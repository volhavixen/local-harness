#!/usr/bin/env python3
"""Capture one local command. Exit success is not a verdict on test coverage."""
import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import time


def now():
    return datetime.now(timezone.utc).isoformat()


def git(cwd, *args):
    try:
        p = subprocess.run(['git', '-C', str(cwd), *args], capture_output=True)
        return p.stdout if p.returncode == 0 else None
    except OSError:
        return None


def identity(cwd):
    head = git(cwd, 'rev-parse', '--verify', 'HEAD')
    status = git(cwd, 'status', '--porcelain', '-z')
    diff = git(cwd, 'diff', '--no-ext-diff', '--binary', 'HEAD', '--')
    return {
        'head': head.decode().strip() if head else None,
        'dirty': bool(status) if status is not None else None,
        'tracked_diff_sha256': hashlib.sha256(diff).hexdigest() if diff is not None else None,
        'identity_limitations': 'Untracked contents and ignored files are not fingerprinted.',
    }


def write_result(path, record):
    temporary = path.with_suffix('.tmp')
    fd = os.open(str(temporary), os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, 'w') as f:
        json.dump(record, f, ensure_ascii=False, indent=2)
        f.write('\n')
        f.flush()
        os.fsync(f.fileno())
    os.replace(temporary, path)


def signal_group(pgid, signum):
    try:
        os.killpg(pgid, signum)
        return True
    except ProcessLookupError:
        return False


def stop_child(child):
    if os.name == 'posix':
        signal_group(child.pid, signal.SIGTERM)
        deadline = time.monotonic() + 2
        # Reap the leader, but give its whole owned group the grace period.
        while time.monotonic() < deadline:
            child.poll()
            if not signal_group(child.pid, 0):
                break
            time.sleep(.05)
        signal_group(child.pid, signal.SIGKILL)
        child.wait()
    else:
        child.terminate()
        try:
            child.wait(timeout=2)
        except subprocess.TimeoutExpired:
            child.kill()
            child.wait()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run', required=True, help='Persistent evidence directory outside the project')
    parser.add_argument('--name', required=True, help='Unique attempt ID; existing records are never replaced')
    parser.add_argument('--cwd', required=True)
    parser.add_argument('command', nargs=argparse.REMAINDER)
    args = parser.parse_args()
    command = args.command[1:] if args.command[:1] == ['--'] else args.command
    if not command or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,79}', args.name):
        parser.error('Supply a command and a safe, unique name (letters, digits, dots, dashes, underscores).')
    cwd, run = Path(args.cwd).expanduser().resolve(), Path(args.run).expanduser().resolve()
    if not cwd.is_dir():
        parser.error('--cwd must be an existing directory')
    root = git(cwd, 'rev-parse', '--show-toplevel')
    project = Path(root.decode().strip()).resolve() if root else cwd
    if run == project or project in run.parents:
        parser.error('--run must be outside the project')
    run.mkdir(parents=True, exist_ok=True, mode=0o700)
    checks = run / 'checks'
    checks.mkdir(exist_ok=True, mode=0o700)
    folder = checks / args.name
    try:
        folder.mkdir(mode=0o700)
    except FileExistsError:
        parser.error('This attempt already exists; use a new --name.')
    result, log = folder / 'result.json', folder / 'output.log'
    record = {
        'schema_version': 1, 'name': args.name, 'source': 'local',
        'cwd': str(cwd), 'command': command, **identity(cwd),
        'status': 'running', 'exit_code': None, 'failure_kind': None,
        'started_at': now(), 'finished_at': None, 'child_pid': None,
        'counts': dict.fromkeys(['passed', 'failed', 'skipped', 'did_not_run']),
        'report_format': None,
        'coverage': {'status': 'not_assessed', 'expected': [], 'observed': [], 'reason': 'Inspect runner evidence.'},
        'log': str(log), 'capture_error': None,
    }
    write_result(result, record)
    print('Evidence: ' + str(result), flush=True)
    cancelled = []

    def cancel(signum, frame):
        if not cancelled:
            cancelled.append(signum)

    previous = {s: signal.signal(s, cancel) for s in [signal.SIGINT, signal.SIGTERM]}
    child = None
    try:
        fd = os.open(str(log), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, 'wb') as output:
            try:
                child = subprocess.Popen(command, cwd=str(cwd), stdout=output, stderr=subprocess.STDOUT,
                                         start_new_session=(os.name == 'posix'))
            except OSError as e:
                record.update(status='blocked', failure_kind='launch_error', capture_error=type(e).__name__)
            if child is not None:
                record['child_pid'] = child.pid
                write_result(result, record)
                while child.poll() is None:
                    if cancelled:
                        stop_child(child)
                        break
                    try:
                        child.wait(timeout=.1)
                    except subprocess.TimeoutExpired:
                        pass
                record['exit_code'] = child.returncode
                record['status'] = 'interrupted' if cancelled else ('passed' if child.returncode == 0 else 'failed')
    finally:
        for s, handler in previous.items():
            signal.signal(s, handler)
        record['finished_at'] = now()
        end = identity(cwd)
        record['end_head'] = end['head']
        record['end_dirty'] = end['dirty']
        record['end_tracked_diff_sha256'] = end['tracked_diff_sha256']
        record['tracked_content_changed'] = record['tracked_diff_sha256'] != end['tracked_diff_sha256']
        record['revision_changed'] = record['head'] != record['end_head']
        write_result(result, record)
    if cancelled:
        return 128 + cancelled[0]
    if record['exit_code'] is None:
        return 126
    return record['exit_code'] if record['exit_code'] >= 0 else 128 - record['exit_code']


if __name__ == '__main__':
    raise SystemExit(main())
