#!/usr/bin/env python3
"""Serialize local heavy checks across projects. Does not manage existing processes."""
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


def live_existing(state):
    path = state / 'drain.json'
    if not path.exists():
        return []
    # Fail closed if the transition file is unreadable or malformed.
    live = []
    for item in json.loads(path.read_text()):
        r = subprocess.run(['ps', '-p', str(int(item['pid'])), '-o', 'lstart=,stat='],
                           text=True, capture_output=True)
        parts = r.stdout.strip().rsplit(None, 1)
        if len(parts) == 2 and parts[0] == item['start'] and not parts[1].startswith('Z'):
            live.append(item['pid'])
    return live


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--state-dir', type=Path, default=DEFAULT_STATE,
                        help='Use default shared state; override only for isolated self-tests')
    actions = parser.add_mutually_exclusive_group()
    actions.add_argument('--status', action='store_true')
    actions.add_argument('--reserve-screens', metavar='LABEL',
                         help='Hold the shared slot for a finite browser/compilation session')
    actions.add_argument('--release', metavar='TOKEN', help='Release only the matching screen session')
    parser.add_argument('--runner', choices=['generic', 'vitest', 'jest', 'playwright', 'pytest'],
                        default='generic')
    parser.add_argument('--workers', type=int, choices=[1, 2], default=1)
    parser.add_argument('command', nargs=argparse.REMAINDER)
    args = parser.parse_args()
    command = args.command[1:] if args.command[:1] == ['--'] else args.command
    state = args.state_dir.expanduser().resolve()
    state.mkdir(parents=True, exist_ok=True, mode=0o700)
    if args.release:
        if command or not re.fullmatch(r'[a-f0-9]{32}', args.release):
            parser.error('Supply only a valid screen session token')
        active = state / 'active.json'
        try:
            owner = json.loads(active.read_text())
        except (FileNotFoundError, json.JSONDecodeError):
            parser.error('No active screen session')
        if owner.get('kind') != 'screens' or owner.get('token') != args.release:
            parser.error('Token does not match the active screen session')
        # A token-specific marker cannot release the next slot owner. No process signals.
        (state / ('release-' + args.release)).touch(mode=0o600)
        print('Screen session release requested', flush=True)
        return 0
    if args.status:
        status = state / 'active.json'
        with (state / 'slot.lock').open('a+') as slot:
            try:
                fcntl.flock(slot, fcntl.LOCK_EX | fcntl.LOCK_NB)
                owner = None
            except BlockingIOError:
                owner = json.loads(status.read_text()) if status.exists() else {'state': 'starting'}
        print(json.dumps({'active': owner, 'existing_checks': live_existing(state)}, ensure_ascii=False))
        return 0
    if args.reserve_screens and (command or args.runner != 'generic'):
        parser.error('Screen reservation takes no command or test runner')
    if not command and not args.reserve_screens:
        parser.error('Supply a finite command after --')
    # Runner flags belong to this wrapper, so duplicates cannot override the cap.
    flags = ('--maxWorkers', '--minWorkers', '--workers', '--max-workers', '--numprocesses')
    if any(x.split('=')[0] in flags or x == '-n' or
           (x.startswith('-n') and x[2:].isdigit()) for x in command):
        parser.error('Set concurrency with wrapper --workers 1 or 2 before --')
    runner = args.runner
    if runner == 'generic':
        names = [Path(x).name for x in command]
        for name in ('vitest', 'jest', 'playwright', 'pytest'):
            if name in names:
                runner = name
                break
    if runner in ('vitest', 'jest'):
        command += [f'--maxWorkers={args.workers}']
        if runner == 'vitest':
            command += ['--minWorkers=1']
    elif runner == 'playwright':
        command += [f'--workers={args.workers}']
    elif runner == 'pytest':
        if args.workers == 2:
            parser.error('pytest defaults to serial; use a project-supported xdist command separately')
        # Plain pytest is serial and does not require pytest-xdist.
    cancelled = []

    def cancel(signum, frame):
        cancelled.append(signum)

    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, cancel)
    child = None
    with (state / 'slot.lock').open('a+') as slot:
        print('Waiting for the shared heavy-check slot...', file=sys.stderr, flush=True)
        while True:
            if cancelled:
                return 128 + cancelled[0]
            try:
                fcntl.flock(slot, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                time.sleep(.25)
        active = state / 'active.json'
        temporary = state / 'active.tmp'
        token = secrets.token_hex(16) if args.reserve_screens else None
        release_marker = state / ('release-' + token) if token else None
        owner = {'pid': os.getpid(), 'cwd': os.getcwd(), 'runner': runner,
                 'workers': args.workers, 'kind': 'screens' if token else 'command',
                 'phase': 'draining'}
        if token:
            owner.update(token=token, label=args.reserve_screens[:160])
        temporary.write_text(json.dumps(owner))
        temporary.replace(active)
        try:
            while live_existing(state):
                if cancelled:
                    return 128 + cancelled[0]
                time.sleep(.5)
            if cancelled:
                return 128 + cancelled[0]
            owner['phase'] = 'active'
            temporary.write_text(json.dumps(owner))
            temporary.replace(active)
            if token:
                print(json.dumps({'event': 'screens_acquired', 'token': token,
                                  'label': args.reserve_screens}), flush=True)
                while not release_marker.exists():
                    if cancelled:
                        return 128 + cancelled[0]
                    time.sleep(.1)
                return 0
            print(f'Heavy-check slot acquired; runner={runner}, workers={args.workers}',
                  file=sys.stderr, flush=True)
            try:
                # Child keeps the lock if this wrapper is killed unexpectedly.
                child = subprocess.Popen(command, pass_fds=(slot.fileno(),), start_new_session=True)
            except OSError as error:
                print(str(error), file=sys.stderr)
                return 127
            forwarded = False
            while child.poll() is None:
                if cancelled and not forwarded:
                    try:
                        os.killpg(child.pid, cancelled[0])
                    except ProcessLookupError:
                        pass
                    forwarded = True
                time.sleep(.1)
            return child.returncode if child.returncode >= 0 else 128 - child.returncode
        finally:
            if release_marker:
                release_marker.unlink(missing_ok=True)
            active.unlink(missing_ok=True)


if __name__ == '__main__':
    raise SystemExit(main())
