import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest

SCRIPT = Path(__file__).with_name('heavy_check.py')


class QueueTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(dir=Path(__file__).parent)
        self.addCleanup(self.tmp.cleanup)
        self.state = Path(self.tmp.name)

    def command(self, *args):
        return [sys.executable, str(SCRIPT), '--state-dir', str(self.state), *args]

    def run_cli(self, *args):
        return subprocess.run(self.command(*args), capture_output=True, text=True, timeout=10)

    def test_preserves_command_exit_code(self):
        r = self.run_cli('--', sys.executable, '-c', 'raise SystemExit(7)')
        self.assertEqual(r.returncode, 7, r.stderr)

    def test_runner_limits_one_by_default_and_two_when_requested(self):
        for runner, flag in [('vitest', '--maxWorkers=1'), ('jest', '--maxWorkers=1'),
                             ('playwright', '--workers=1')]:
            r = self.run_cli('--runner', runner, '--', sys.executable, '-c',
                             'import sys; print(sys.argv[1:])')
            self.assertEqual(r.returncode, 0, r.stderr)
            self.assertIn(flag, r.stdout)
        r = self.run_cli('--runner', 'vitest', '--workers', '2', '--', sys.executable,
                         '-c', 'import sys; print(sys.argv[1:])')
        self.assertIn('--maxWorkers=2', r.stdout)

    def test_rejects_more_than_two_and_conflicting_runner_flags(self):
        marker = self.state / 'started'
        for opts in [('--workers', '3'), ('--runner', 'vitest')]:
            command = [sys.executable, '-c', f'open({str(marker)!r}, "w").close()']
            if opts == ('--runner', 'vitest'):
                command += ['--maxWorkers=4']
            r = self.run_cli(*opts, '--', *command)
            self.assertEqual(r.returncode, 2)
            self.assertFalse(marker.exists())

    def test_two_processes_cannot_overlap(self):
        marker = self.state / 'exclusive'
        code = ('import os,time; '
                f'fd=os.open({str(marker)!r},os.O_CREAT|os.O_EXCL|os.O_WRONLY); '
                'time.sleep(.35); os.close(fd); '
                f'os.unlink({str(marker)!r})')
        a = subprocess.Popen(self.command('--', sys.executable, '-c', code),
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        b = subprocess.Popen(self.command('--', sys.executable, '-c', code),
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        for p in (a, b):
            _, err = p.communicate(timeout=10)
            self.assertEqual(p.returncode, 0, err)

    def test_waits_for_existing_check_without_signalling_it(self):
        p = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(.8)'])
        self.addCleanup(lambda: p.poll() is None and p.terminate())
        birth = subprocess.check_output(['ps', '-p', str(p.pid), '-o', 'lstart='], text=True).strip()
        (self.state / 'drain.json').write_text(json.dumps([{'pid': p.pid, 'start': birth}]))
        r = self.run_cli('--', sys.executable, '-c', f'import os; print(os.path.exists("/proc/{p.pid}"))')
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertIsNotNone(p.poll(), 'Queued command started before existing check finished')
        self.assertEqual(p.wait(), 0)

    def test_missing_command_releases_slot(self):
        self.assertEqual(self.run_cli('--', '/no/such/program').returncode, 127)
        self.assertEqual(self.run_cli('--', sys.executable, '-c', 'pass').returncode, 0)

    def test_cancel_waiter_does_not_interrupt_owner(self):
        marker = self.state / 'running'
        code = f'import pathlib,time; pathlib.Path({str(marker)!r}).touch(); time.sleep(.8)'
        owner = subprocess.Popen(self.command('--', sys.executable, '-c', code),
                                 stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        deadline = time.monotonic() + 5
        while not marker.exists() and time.monotonic() < deadline:
            time.sleep(.02)
        self.assertTrue(marker.exists())
        waiter = subprocess.Popen(self.command('--', sys.executable, '-c', 'raise SystemExit(9)'),
                                  stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        time.sleep(.15)
        waiter.terminate()
        self.assertEqual(waiter.wait(timeout=5), 143)
        self.assertIsNone(owner.poll())
        self.assertEqual(owner.wait(timeout=5), 0)

    def test_killed_wrapper_keeps_lock_until_its_child_finishes(self):
        marker = self.state / 'running'
        code = (f'import pathlib,time; p=pathlib.Path({str(marker)!r}); '
                'p.touch(); time.sleep(.8); p.unlink()')
        owner = subprocess.Popen(self.command('--', sys.executable, '-c', code),
                                 stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        deadline = time.monotonic() + 5
        while not marker.exists() and time.monotonic() < deadline:
            time.sleep(.02)
        self.assertTrue(marker.exists())
        owner.kill()
        owner.wait(timeout=5)
        r = self.run_cli('--', sys.executable, '-c',
                         f'from pathlib import Path; assert not Path({str(marker)!r}).exists()')
        self.assertEqual(r.returncode, 0, r.stderr)

    def reserve_screens(self):
        p = subprocess.Popen(self.command('--reserve-screens', 'test-screens'),
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        self.addCleanup(lambda: p.poll() is None and p.terminate())
        line = p.stdout.readline()
        self.assertTrue(line, p.stderr.read() if p.poll() is not None else 'No acquired event')
        event = json.loads(line)
        self.assertEqual(event['event'], 'screens_acquired')
        return p, event['token']

    def test_screen_session_blocks_tests_until_explicit_release(self):
        owner, token = self.reserve_screens()
        marker = self.state / 'test-started'
        p = subprocess.Popen(self.command('--', sys.executable, '-c',
                             f'from pathlib import Path; Path({str(marker)!r}).touch()'),
                             stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        time.sleep(.2)
        self.assertFalse(marker.exists())
        self.assertIsNone(p.poll())
        self.assertEqual(self.run_cli('--release', token).returncode, 0)
        self.assertEqual(owner.wait(timeout=5), 0)
        owner.communicate()
        self.assertEqual(p.wait(timeout=5), 0)
        self.assertTrue(marker.exists())

    def test_wrong_token_cannot_release_screen_session(self):
        owner, token = self.reserve_screens()
        self.assertEqual(self.run_cli('--release', '0' * 32).returncode, 2)
        status = json.loads(self.run_cli('--status').stdout)
        self.assertEqual(status['active']['token'], token)
        self.assertIsNone(owner.poll())
        self.assertEqual(self.run_cli('--release', token).returncode, 0)
        self.assertEqual(owner.wait(timeout=5), 0)
        owner.communicate()

    def test_screen_reservation_waits_for_running_command(self):
        marker = self.state / 'command-active'
        code = (f'from pathlib import Path; import time; p=Path({str(marker)!r}); '
                'p.touch(); time.sleep(.5); p.unlink()')
        p = subprocess.Popen(self.command('--', sys.executable, '-c', code),
                             stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        deadline = time.monotonic() + 5
        while not marker.exists() and time.monotonic() < deadline:
            time.sleep(.02)
        self.assertTrue(marker.exists())
        owner, token = self.reserve_screens()
        self.assertFalse(marker.exists(), 'Browser work overlapped command')
        self.assertEqual(p.wait(timeout=5), 0)
        self.assertEqual(self.run_cli('--release', token).returncode, 0)
        self.assertEqual(owner.wait(timeout=5), 0)
        owner.communicate()

    def test_cancel_screen_session_releases_slot_without_touching_other_processes(self):
        unrelated = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(5)'])
        try:
            owner, _ = self.reserve_screens()
            owner.terminate()
            self.assertEqual(owner.wait(timeout=5), 143)
            owner.communicate()
            self.assertIsNone(unrelated.poll())
            self.assertEqual(self.run_cli('--', sys.executable, '-c', 'pass').returncode, 0)
        finally:
            unrelated.terminate()
            unrelated.wait(timeout=5)


if __name__ == '__main__':
    unittest.main()
