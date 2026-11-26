#!/usr/bin/env python3
"""Serialize local heavy checks across projects; inspect FIFO requests and history."""
from contextlib import contextmanager
from datetime import datetime, timezone
import argparse
import fcntl
import json
import os
from pathlib import Path
import re
import secrets
import signal
import subprocess
import sys
import time

DEFAULT_STATE = Path(__file__).resolve().parent / 'state'
PARALLEL_LIMIT = 2


def workload(row):
    value = row.get('workload', 'heavy')
    if value not in ('heavy', 'light', 'network'):
        raise ValueError('Unknown workload class')
    return value


def parallel_admission(rows, ident):
    active = sum(workload(r) != 'heavy' and r['phase'] == 'active' for r in rows)
    pending = [r for r in rows if r['phase'] == 'parallel_queued']
    return active < PARALLEL_LIMIT and bool(pending) and pending[0]['id'] == ident


def live_existing(state):
    path = state / 'drain.json'
    if not path.exists():
        return []
    live = []
    for item in json.loads(path.read_text()):
        r = subprocess.run(['ps', '-p', str(int(item['pid'])), '-o', 'lstart=,stat='],
                           text=True, capture_output=True)
        parts = r.stdout.strip().rsplit(None, 1)
        if len(parts) == 2 and parts[0] == item['start'] and not parts[1].startswith('Z'):
            live.append(item['pid'])
    return live


def atomic_json(path, value):
    temporary = path.with_name(path.name + '.' + secrets.token_hex(8) + '.tmp')
    try:
        with os.fdopen(os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), 'w') as out:
            json.dump(value, out, ensure_ascii=False)
            out.flush()
            os.fsync(out.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


@contextmanager
def metadata_lock(state):
    # Never wait for slot/lease while holding this short metadata lock.
    with os.fdopen(os.open(state / 'metadata.lock', os.O_RDWR | os.O_CREAT, 0o600), 'a+') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        yield


def with_durations(row, monotonic=None):
    monotonic = time.monotonic() if monotonic is None else monotonic
    result = dict(row)
    if 'finished_at' in row:
        return result
    queued = row.get('queued_monotonic')
    started = row.get('started_monotonic')
    result['wait_seconds'] = (max(0, (started if started is not None else monotonic) - queued)
                              if queued is not None else None)
    result['run_seconds'] = max(0, monotonic - started) if started is not None else None
    return result


def safe_text(value):
    return ' '.join(''.join(c if c.isprintable() else ' ' for c in value).split())[:160]


class Queue:
    """Metadata is protected separately from the unchanged, inherited slot lock."""

    def __init__(self, state):
        self.state = state
        self.requests = state / 'requests'
        self.history = state / 'history'
        self.requests.mkdir(mode=0o700, exist_ok=True)
        self.history.mkdir(mode=0o700, exist_ok=True)

    def request_path(self, ident):
        return self.requests / (ident + '.json')

    def lease_path(self, ident):
        return self.requests / (ident + '.lease')

    def finish(self, row, outcome, exit_code, estimated=False):
        # Caller holds metadata.lock. A completed record wins after a crash
        # between the history write and deletion of the live request.
        target = self.history / (row['id'] + '.json')
        if not target.exists():
            result = with_durations(row)
            result.update(finished_at=time.time(), outcome=outcome, exit_code=exit_code,
                          timing_estimated=estimated)
            if estimated:
                result['observed_finished_at'] = result.pop('finished_at')
                result['finished_at'] = None
            for key in ('token', 'queued_monotonic', 'started_monotonic'):
                result.pop(key, None)
            atomic_json(target, result)
        self.request_path(row['id']).unlink(missing_ok=True)
        # Closing, not LOCK_UN, matters for inherited open file descriptions.
        self.lease_path(row['id']).unlink(missing_ok=True)

    def live_requests(self):
        # Caller holds metadata.lock. A lease is acquired before registration
        # and inherited by the child: PID reuse is irrelevant to reclamation.
        rows = []
        for path in self.requests.glob('*.json'):
            row = json.loads(path.read_text())
            workload(row)
            ident = row['id']
            if not re.fullmatch(r'[a-f0-9]{32}', ident) or path != self.request_path(ident):
                raise ValueError('Invalid request identity')
            if (self.history / (ident + '.json')).exists():
                self.finish(row, 'unknown', None, estimated=True)
                continue
            # A missing lease indicates corrupt metadata, not a dead owner.
            with self.lease_path(ident).open('r+') as lease:
                try:
                    fcntl.flock(lease, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError:
                    rows.append(row)
                else:
                    self.finish(row, 'unknown', None, estimated=True)
        return sorted(rows, key=lambda item: item['sequence'])

    def register(self, details):
        ident = secrets.token_hex(16)
        lease = os.fdopen(os.open(self.lease_path(ident), os.O_RDWR | os.O_CREAT | os.O_EXCL, 0o600), 'a+')
        fcntl.flock(lease, fcntl.LOCK_EX)
        try:
            with metadata_lock(self.state):
                existing = self.live_requests()
                counter = self.state / 'counter.json'
                if not counter.exists() and (existing or any(self.history.glob('*.json'))):
                    raise ValueError('Missing queue counter for live requests')
                sequence = json.loads(counter.read_text())['next_sequence'] if counter.exists() else 1
                if type(sequence) is not int or sequence < 1 or any(r['sequence'] >= sequence for r in existing):
                    raise ValueError('Invalid queue counter')
                atomic_json(counter, {'next_sequence': sequence + 1})
                row = dict(details, id=ident, sequence=sequence, pid=os.getpid(),
                           queued_at=time.time(), queued_monotonic=time.monotonic(),
                           phase='queued' if workload(details) == 'heavy' else 'parallel_queued')
                atomic_json(self.request_path(ident), row)
            return row, lease
        except BaseException:
            lease.close()
            self.lease_path(ident).unlink(missing_ok=True)
            raise

    def save_active(self, row, token=None):
        # Caller holds metadata.lock and an admitted place (slot for heavy).
        atomic_json(self.request_path(row['id']), row)
        if workload(row) == 'heavy':
            active = dict(row)
            if token:
                active['token'] = token
            atomic_json(self.state / 'active.json', active)

    def clear_active(self, ident):
        # Caller holds metadata.lock. A legacy successor does not use it, so
        # also hold slot.lock across read+unlink, or leave its snapshot alone.
        with (self.state / 'slot.lock').open('a+') as slot:
            try:
                fcntl.flock(slot, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                return
            path = self.state / 'active.json'
            try:
                active = json.loads(path.read_text())
            except FileNotFoundError:
                return
            if active.get('id') == ident:
                path.unlink(missing_ok=True)

    def snapshot(self):
        with metadata_lock(self.state):
            rows = self.live_requests()
            active = None
            with (self.state / 'slot.lock').open('a+') as slot:
                try:
                    fcntl.flock(slot, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError:
                    try:
                        active = json.loads((self.state / 'active.json').read_text())
                    except FileNotFoundError:
                        active = {'phase': 'starting', 'legacy': True}
                    current = next((r for r in rows if r['id'] == active.get('id')), None)
                    if current:
                        token = active.get('token')
                        active = with_durations(current)
                        if token:
                            active['token'] = token
                    else:
                        active.update(legacy=True, wait_seconds=None, run_seconds=None)
            waiting = [with_durations(r) for r in rows if r['phase'] in ('queued', 'parallel_queued')]
            lane_positions = {'heavy': 0, 'parallel': 0}
            for position, row in enumerate(waiting, 1):
                row['position'] = position
                lane = 'heavy' if workload(row) == 'heavy' else 'parallel'
                lane_positions[lane] += 1
                row.update(lane=lane, lane_position=lane_positions[lane])
            return {'active': active, 'waiting': waiting,
                    'parallel_active': [with_durations(r) for r in rows
                                        if workload(r) != 'heavy' and r['phase'] == 'active'],
                    'limits': {'heavy': 1, 'parallel': PARALLEL_LIMIT},
                    'existing_checks': live_existing(self.state)}

    def past(self, limit):
        with metadata_lock(self.state):
            self.live_requests()
            rows = [json.loads(p.read_text()) for p in self.history.glob('*.json')]
            rows.sort(key=lambda r: r.get('finished_at') or r['observed_finished_at'], reverse=True)
            return rows[:limit]


def human_status(status):
    def seconds(value):
        return 'unknown' if value is None else f'{value:.1f}s'

    def describe(row):
        label = safe_text(str(row.get('label') or row.get('runner') or 'legacy command'))
        owner = safe_text(str(row.get('owner') or 'unspecified'))
        cwd = safe_text(str(row.get('cwd') or 'unknown'))
        return (f'{label} | class={workload(row)} | owner={owner} | {cwd} | wrapper PID={row.get("pid", "unknown")}'
                f' | child PID={row.get("child_pid", "unknown")}')

    lines = []
    active = status['active']
    if active:
        lines.append('Heavy running: ' + describe(active))
        lines.append(f'  phase={active.get("phase", "unknown")} | '
                     f'waited={seconds(active.get("wait_seconds"))} | '
                     f'running={seconds(active.get("run_seconds"))}')
        lines.append(f'  started={timestamp(active.get("started_at"))}')
        if active.get('legacy'):
            lines.append('  Legacy owner: task/timing unavailable; a child may still hold the slot.')
    else:
        lines.append('Heavy running: none')
    parallel = status.get('parallel_active', [])
    lines.append(f'Parallel running ({len(parallel)}/{PARALLEL_LIMIT}):')
    for row in parallel:
        lines.append(f'  {describe(row)} | running={seconds(row.get("run_seconds"))}')
    lines.append(f'Waiting ({len(status["waiting"])}):')
    for row in status['waiting']:
        lines.append(f'  {row["position"]}. {describe(row)} | waiting={seconds(row["wait_seconds"])}'
                     f' | lane={row.get("lane", "heavy")} #{row.get("lane_position", row["position"])}')
    if status['existing_checks']:
        lines.append('Existing checks still draining: ' + ', '.join(map(str, status['existing_checks'])))
    lines.append('Heavy and parallel admission have separate FIFO lanes; remaining time is not estimated.')
    return '\n'.join(lines)


def timestamp(value):
    return datetime.fromtimestamp(value, timezone.utc).isoformat(timespec='seconds') if value else 'unknown'


def run_command(queue, row, lease, command, cancelled, slot=None):
    if cancelled:
        return 'cancelled', 128 + cancelled[0]
    inherited = (lease.fileno(),) + ((slot.fileno(),) if slot is not None else ())
    try:
        child = subprocess.Popen(command, pass_fds=inherited, start_new_session=True)
    except OSError as error:
        print(str(error), file=sys.stderr)
        return 'launch_error', 127
    row['child_pid'] = child.pid
    persistence_error = None
    try:
        with metadata_lock(queue.state):
            queue.save_active(row)
    except (OSError, ValueError, KeyError, TypeError) as error:
        # Do not delete the live request and release capacity while a child
        # still runs just because persisting its PID failed.
        persistence_error = error
    forwarded = False
    while child.poll() is None:
        if cancelled and not forwarded:
            try:
                os.killpg(child.pid, cancelled[0])
            except ProcessLookupError:
                pass
            forwarded = True
        time.sleep(.1)
    if persistence_error:
        row['metadata_warning'] = 'child_pid_update_failed'
        print('Warning: child PID update failed; command completion was observed.',
              file=sys.stderr, flush=True)
    exit_code = child.returncode if child.returncode >= 0 else 128 - child.returncode
    return ('interrupted' if forwarded else 'exited'), exit_code


def execute_parallel(queue, row, lease, command, cancelled):
    outcome, exit_code = 'unknown', None
    try:
        last_notice = 0
        while True:
            if cancelled:
                outcome, exit_code = 'cancelled', 128 + cancelled[0]
                return exit_code
            with metadata_lock(queue.state):
                rows = queue.live_requests()
                if parallel_admission(rows, row['id']):
                    # Publish admission before Popen, while the lease is held.
                    # The child inherits it, preserving the place after SIGKILL.
                    row.update(phase='active', acquired_at=time.time(),
                               started_at=time.time(), started_monotonic=time.monotonic())
                    queue.save_active(row)
                    break
                if time.monotonic() - last_notice >= 30:
                    pending = [r for r in rows if r['phase'] == 'parallel_queued']
                    position = next(i for i, r in enumerate(pending, 1) if r['id'] == row['id'])
                    print(f'Parallel queue position={position}; class={workload(row)}; '
                          f'waited={with_durations(row)["wait_seconds"]:.1f}s; limit={PARALLEL_LIMIT}',
                          file=sys.stderr, flush=True)
                    last_notice = time.monotonic()
            time.sleep(.1)
        print(f'Parallel place acquired; class={workload(row)}; request={row["id"]}; '
              f'waited={with_durations(row)["wait_seconds"]:.1f}s', file=sys.stderr, flush=True)
        outcome, exit_code = run_command(queue, row, lease, command, cancelled)
        return exit_code
    finally:
        with metadata_lock(queue.state):
            queue.finish(row, outcome, exit_code, estimated=exit_code is None)


def execute(queue, row, lease, command, cancelled, screen_label):
    token = secrets.token_hex(16) if screen_label else None
    release_marker = queue.state / ('release-' + token) if token else None
    acquired = False
    outcome, exit_code = 'unknown', None
    try:
        with (queue.state / 'slot.lock').open('a+') as slot:
            print(f'Waiting for the shared heavy-check slot... request={row["id"]} '
                  f'label={row["label"]}', file=sys.stderr, flush=True)
            last_notice = 0
            while not acquired:
                if cancelled:
                    outcome, exit_code = 'cancelled', 128 + cancelled[0]
                    return exit_code
                with metadata_lock(queue.state):
                    rows = queue.live_requests()
                    pending = [r for r in rows if r['phase'] == 'queued' and workload(r) == 'heavy']
                    if pending and pending[0]['id'] == row['id']:
                        try:
                            fcntl.flock(slot, fcntl.LOCK_EX | fcntl.LOCK_NB)
                        except BlockingIOError:
                            pass
                        else:
                            acquired = True
                            row.update(phase='draining', acquired_at=time.time())
                            queue.save_active(row, token)
                    if not acquired and time.monotonic() - last_notice >= 30:
                        position = next(i for i, r in enumerate(pending, 1) if r['id'] == row['id'])
                        try:
                            owner = json.loads((queue.state / 'active.json').read_text())
                        except FileNotFoundError:
                            owner = {}
                        blocker = safe_text(str(owner.get('label') or owner.get('cwd') or 'unknown/starting'))
                        print(f'Queue position={position}; waited={with_durations(row)["wait_seconds"]:.1f}s. '
                              f'Slot owner={blocker}. Use --status --human for details.', file=sys.stderr, flush=True)
                        last_notice = time.monotonic()
                if not acquired:
                    time.sleep(.25)
            while live_existing(queue.state):
                if cancelled:
                    outcome, exit_code = 'cancelled', 128 + cancelled[0]
                    return exit_code
                time.sleep(.5)
            if cancelled:
                outcome, exit_code = 'cancelled', 128 + cancelled[0]
                return exit_code
            row.update(phase='active', started_at=time.time(), started_monotonic=time.monotonic())
            with metadata_lock(queue.state):
                queue.save_active(row, token)
            if token:
                print(json.dumps({'event': 'screens_acquired', 'token': token,
                                  'label': row['label']}), flush=True)
                while not release_marker.exists():
                    if cancelled:
                        outcome, exit_code = 'cancelled', 128 + cancelled[0]
                        return exit_code
                    time.sleep(.1)
                outcome, exit_code = 'released', 0
                return exit_code
            print(f'Heavy-check slot acquired; runner={row["runner"]}, workers={row["workers"]}; '
                  f'waited={with_durations(row)["wait_seconds"]:.1f}s', file=sys.stderr, flush=True)
            outcome, exit_code = run_command(queue, row, lease, command, cancelled, slot)
            return exit_code
    finally:
        # This executes before the lease closes. The slot fd may already be
        # closed, so do not delete an active record belonging to a successor.
        with metadata_lock(queue.state):
            queue.finish(row, outcome, exit_code, estimated=exit_code is None)
            queue.clear_active(row['id'])
        if release_marker:
            release_marker.unlink(missing_ok=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--state-dir', type=Path, default=DEFAULT_STATE,
                        help='Use default shared state; override only for isolated self-tests')
    actions = parser.add_mutually_exclusive_group()
    actions.add_argument('--status', action='store_true')
    actions.add_argument('--history', action='store_true')
    actions.add_argument('--reserve-screens', metavar='LABEL')
    actions.add_argument('--release', metavar='TOKEN')
    parser.add_argument('--human', action='store_true', help='Readable status/history; default is compatible JSON')
    parser.add_argument('--limit', type=int, default=20, help='History entries, newest first (1–1000)')
    parser.add_argument('--label', help='Short task label; never include secrets')
    parser.add_argument('--owner', help='Chat/task identifier; never include secrets')
    parser.add_argument('--class', dest='workload', choices=['heavy', 'light', 'network'], default='heavy',
                        help='Explicit resource class; light/network share two parallel places')
    parser.add_argument('--runner', choices=['generic', 'vitest', 'jest', 'playwright', 'pytest'], default='generic')
    parser.add_argument('--workers', type=int, choices=[1, 2], default=1)
    parser.add_argument('command', nargs=argparse.REMAINDER)
    args = parser.parse_args()
    command = args.command[1:] if args.command[:1] == ['--'] else args.command
    if not 1 <= args.limit <= 1000:
        parser.error('--limit must be between 1 and 1000')
    if args.human and not (args.status or args.history):
        parser.error('--human requires --status or --history')
    if (args.status or args.history or args.release) and command:
        parser.error('Inspection/release takes no command')
    state = args.state_dir.expanduser().resolve()
    state.mkdir(parents=True, exist_ok=True, mode=0o700)
    if args.release:
        if not re.fullmatch(r'[a-f0-9]{32}', args.release):
            parser.error('Supply only a valid screen session token')
        try:
            active = json.loads((state / 'active.json').read_text())
        except (FileNotFoundError, json.JSONDecodeError):
            parser.error('No active screen session')
        if active.get('kind') != 'screens' or active.get('token') != args.release:
            parser.error('Token does not match the active screen session')
        fd = os.open(state / ('release-' + args.release), os.O_WRONLY | os.O_CREAT, 0o600)
        os.close(fd)
        print('Screen session release requested', flush=True)
        return 0
    queue = Queue(state)
    if args.status:
        status = queue.snapshot()
        print(human_status(status) if args.human else json.dumps(status, ensure_ascii=False))
        return 0
    if args.history:
        rows = queue.past(args.limit)
        if args.human:
            print(f'History ({len(rows)}), newest first:')
            for row in rows:
                wait, run = row.get('wait_seconds'), row.get('run_seconds')
                wait = 'unknown' if wait is None else f'{wait:.1f}'
                run = 'unknown' if run is None else f'{run:.1f}'
                print(f'{row["id"][:8]} | {safe_text(row["label"])} | class={workload(row)} | owner={safe_text(row.get("owner") or "unspecified")} '
                      f'| {row["outcome"]} exit={row["exit_code"]} | waited={wait} s | ran={run} s'
                      + (' | timing estimated; completion unobserved' if row['timing_estimated'] else ''))
                print(f'  queued={timestamp(row.get("queued_at"))} | started={timestamp(row.get("started_at"))}'
                      f' | finished={timestamp(row.get("finished_at"))}')
        else:
            print(json.dumps({'history': rows}, ensure_ascii=False))
        return 0
    if args.reserve_screens and (command or args.runner != 'generic' or args.workload != 'heavy'):
        parser.error('Screen reservation is heavy and takes no command or test runner')
    if not command and not args.reserve_screens:
        parser.error('Supply a finite command after --')
    flags = ('--maxWorkers', '--minWorkers', '--workers', '--max-workers', '--numprocesses')
    if any(x.split('=')[0] in flags or x == '-n' or (x.startswith('-n') and x[2:].isdigit()) for x in command):
        parser.error('Set concurrency with wrapper --workers 1 or 2 before --')
    runner = args.runner
    if runner == 'generic':
        names = [Path(x).name for x in command]
        runner = next((name for name in ('vitest', 'jest', 'playwright', 'pytest') if name in names), runner)
    if args.workload != 'heavy' and args.workers != 1:
        parser.error('Light/network commands use exactly one worker')
    if args.workload != 'heavy' and runner == 'playwright':
        parser.error('Browser automation remains heavy')
    if args.workload == 'network' and runner != 'generic':
        parser.error('Network class is for network commands, not test runners')
    if runner in ('vitest', 'jest'):
        command += [f'--maxWorkers={args.workers}']
        if runner == 'vitest':
            command += ['--minWorkers=1']
    elif runner == 'playwright':
        command += [f'--workers={args.workers}']
    elif runner == 'pytest' and args.workers == 2:
        parser.error('pytest defaults to serial; use a project-supported xdist command separately')
    cancelled = []
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda signum, frame: cancelled.append(signum))
    details = {'cwd': os.getcwd(), 'runner': runner, 'workers': args.workers,
               'workload': args.workload,
               'kind': 'screens' if args.reserve_screens else 'command',
               'label': safe_text(args.reserve_screens or args.label or Path(command[0]).name),
               'owner': safe_text(args.owner) if args.owner else None}
    row, lease = queue.register(details)
    with lease:
        if args.workload != 'heavy':
            return execute_parallel(queue, row, lease, command, cancelled)
        return execute(queue, row, lease, command, cancelled, args.reserve_screens)


if __name__ == '__main__':
    try:
        raise SystemExit(main())
    except (OSError, ValueError, KeyError, TypeError) as error:
        print(f'Queue metadata/IO error ({type(error).__name__}); command not confirmed. '
              'Inspect state without deleting lock files.', file=sys.stderr)
        raise SystemExit(2)
