import fcntl
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest
from unittest import mock

SCRIPT = Path(__file__).with_name('heavy_check.py')


class StageOneTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(dir=SCRIPT.parent)
        self.addCleanup(self.tmp.cleanup)
        self.state = Path(self.tmp.name)
        self.children = []
        self.addCleanup(self.stop_children)

    def stop_children(self):
        for child in reversed(self.children):
            if child.poll() is None:
                child.terminate()
            try:
                child.wait(timeout=5)
            except subprocess.TimeoutExpired:
                child.kill()
                child.wait(timeout=5)

    def command(self, *args):
        return [sys.executable, str(SCRIPT), '--state-dir', str(self.state), *args]

    def cli(self, *args):
        return subprocess.run(self.command(*args), capture_output=True, text=True, timeout=10)

    def start(self, label, code, *options):
        child = subprocess.Popen(self.command('--label', label, '--owner', 'test-chat',
                                              *options, '--', sys.executable, '-c', code),
                                 stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        self.children.append(child)
        return child

    def eventually(self, predicate):
        deadline = time.monotonic() + 7
        while time.monotonic() < deadline:
            value = predicate()
            if value:
                return value
            time.sleep(.025)
        self.fail('Expected queue event was not observed before the deadline')

    def status(self):
        result = self.cli('--status')
        self.assertEqual(result.returncode, 0, result.stderr)
        return json.loads(result.stdout)

    def history(self, *options):
        result = self.cli('--history', *options)
        self.assertEqual(result.returncode, 0, result.stderr)
        return json.loads(result.stdout)['history']

    def active(self, label):
        return self.eventually(lambda: (self.status()['active'] or {}).get('label') == label)

    def waiting(self, label):
        return self.eventually(lambda: any(x['label'] == label for x in self.status()['waiting']))

    def gate_owner(self):
        gate = self.state / 'continue'
        code = ('from pathlib import Path; import time; '
                f'p=Path({str(gate)!r}); '
                '\nwhile not p.exists(): time.sleep(.02)')
        child = self.start('blocker', code)
        self.active('blocker')
        return child, gate

    def test_status_lists_owner_and_waiters_in_registration_order(self):
        owner, gate = self.gate_owner()
        first = self.start('first', 'pass')
        self.waiting('first')
        second = self.start('second', 'pass')
        self.waiting('second')
        status = self.status()
        self.assertEqual(status['active']['owner'], 'test-chat')
        self.assertIn('started_at', status['active'])
        self.assertGreaterEqual(status['active']['run_seconds'], 0)
        self.assertEqual([x['label'] for x in status['waiting']], ['first', 'second'])
        self.assertEqual([x['position'] for x in status['waiting']], [1, 2])
        self.assertGreaterEqual(status['waiting'][0]['wait_seconds'], 0)
        human = self.cli('--status', '--human')
        self.assertEqual(human.returncode, 0, human.stderr)
        self.assertIn('blocker', human.stdout)
        self.assertIn('first', human.stdout)
        self.assertIn('test-chat', human.stdout)
        gate.touch()
        for p in (owner, first, second):
            self.assertEqual(p.wait(timeout=10), 0)

    def test_fifo_order_includes_screen_reservations(self):
        owner, gate = self.gate_owner()
        order = self.state / 'order'
        append = lambda name: f'from pathlib import Path; p=Path({str(order)!r}); p.open("a").write({name!r})'
        first = self.start('first', append('A'))
        self.waiting('first')
        screen = subprocess.Popen(self.command('--reserve-screens', 'screen'),
                                  stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
        self.children.append(screen)
        self.addCleanup(screen.stdout.close)
        self.waiting('screen')
        third = self.start('third', append('C'))
        self.waiting('third')
        gate.touch()
        self.assertEqual(owner.wait(timeout=10), 0)
        self.assertEqual(first.wait(timeout=10), 0)
        self.active('screen')
        self.assertEqual(order.read_text(), 'A')
        self.assertIsNone(third.poll())
        event = json.loads(screen.stdout.readline())
        self.assertNotIn(event['token'], self.cli('--status', '--human').stdout)
        self.assertEqual(self.cli('--release', event['token']).returncode, 0)
        self.assertEqual(screen.wait(timeout=10), 0)
        self.assertEqual(third.wait(timeout=10), 0)
        self.assertEqual(order.read_text(), 'AC')
        self.assertNotIn(event['token'], json.dumps(self.history()))

    def test_fifo_commands_start_in_registration_order(self):
        owner, gate = self.gate_owner()
        order = self.state / 'order'
        children = []
        for name in ('A', 'B', 'C'):
            code = f'from pathlib import Path; Path({str(order)!r}).open("a").write({name!r})'
            children.append(self.start(name, code))
            self.waiting(name)
        gate.touch()
        self.assertEqual(owner.wait(timeout=10), 0)
        for child in children:
            self.assertEqual(child.wait(timeout=10), 0)
        self.assertEqual(order.read_text(), 'ABC')

    def test_cancelled_waiter_never_runs_and_does_not_block_next(self):
        owner, gate = self.gate_owner()
        marker = self.state / 'must-not-exist'
        first = self.start('cancelled', f'from pathlib import Path; Path({str(marker)!r}).touch()')
        self.waiting('cancelled')
        second = self.start('second', 'pass')
        self.waiting('second')
        first.terminate()
        self.assertEqual(first.wait(timeout=5), 143)
        self.assertIsNone(owner.poll())
        self.assertEqual([x['label'] for x in self.status()['waiting']], ['second'])
        cancelled = next(x for x in self.history() if x['label'] == 'cancelled')
        self.assertEqual(cancelled['outcome'], 'cancelled')
        self.assertIsNone(cancelled['run_seconds'])
        gate.touch()
        self.assertEqual(owner.wait(timeout=10), 0)
        self.assertEqual(second.wait(timeout=10), 0)
        self.assertFalse(marker.exists())

    def test_killed_waiter_is_recovered_without_pid_checks(self):
        owner, gate = self.gate_owner()
        first = self.start('killed', 'raise SystemExit(99)')
        self.waiting('killed')
        second = self.start('second', 'pass')
        self.waiting('second')
        first.kill()
        first.wait(timeout=5)
        self.assertEqual([x['label'] for x in self.status()['waiting']], ['second'])
        row = next(x for x in self.history() if x['label'] == 'killed')
        self.assertEqual(row['outcome'], 'unknown')
        self.assertIsNone(row['exit_code'])
        gate.touch()
        self.assertEqual(owner.wait(timeout=10), 0)
        self.assertEqual(second.wait(timeout=10), 0)

    def test_killed_wrapper_keeps_child_visible_and_never_invents_result(self):
        marker = self.state / 'child-active'
        gate = self.state / 'child-finish'
        code = ('from pathlib import Path; import time; '
                f'p=Path({str(marker)!r}); p.touch(); gate=Path({str(gate)!r}); '
                '\nwhile not gate.exists(): time.sleep(.02)\np.unlink()')
        owner = self.start('orphan-child', code)
        self.addCleanup(gate.touch)
        self.eventually(marker.exists)
        self.eventually(lambda: (self.status()['active'] or {}).get('child_pid'))
        owner.kill()
        owner.wait(timeout=5)
        second = self.start('second', f'from pathlib import Path; assert not Path({str(marker)!r}).exists()')
        self.waiting('second')
        self.assertEqual(self.status()['active']['label'], 'orphan-child')
        self.assertFalse(any(x['label'] == 'orphan-child' for x in self.history()))
        gate.touch()
        self.assertEqual(second.wait(timeout=10), 0)
        row = next(x for x in self.history() if x['label'] == 'orphan-child')
        self.assertEqual(row['outcome'], 'unknown')
        self.assertIsNone(row['exit_code'])
        self.assertTrue(row['timing_estimated'])

    def test_history_exit_codes_durations_limit_and_no_command_arguments(self):
        owner, gate = self.gate_owner()
        child = self.start('safe-label', 'raise SystemExit(7) # PRIVATE_ARG')
        self.waiting('safe-label')
        gate.touch()
        self.assertEqual(owner.wait(timeout=10), 0)
        self.assertEqual(child.wait(timeout=10), 7)
        rows = self.history('--limit', '1')
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row['label'], 'safe-label')
        self.assertEqual(row['exit_code'], 7)
        self.assertEqual(row['outcome'], 'exited')
        self.assertGreater(row['wait_seconds'], 0)
        self.assertGreaterEqual(row['run_seconds'], 0)
        self.assertFalse(row['timing_estimated'])
        persisted = '\n'.join(p.read_text() for p in self.state.rglob('*.json'))
        self.assertNotIn('PRIVATE_ARG', persisted)
        self.assertEqual(self.cli('--history', '--human').returncode, 0)

    def test_launch_failure_is_recorded_and_slot_is_reusable(self):
        result = self.cli('--label', 'missing', '--', '/no/such/queue-command')
        self.assertEqual(result.returncode, 127)
        row = self.history()[0]
        self.assertEqual(row['exit_code'], 127)
        self.assertEqual(row['outcome'], 'launch_error')
        self.assertEqual(self.cli('--', sys.executable, '-c', 'pass').returncode, 0)

    def test_legacy_lock_and_state_survive_new_status(self):
        slot_path = self.state / 'slot.lock'
        with slot_path.open('a+') as slot:
            inode = slot_path.stat().st_ino
            fcntl.flock(slot, fcntl.LOCK_EX)
            legacy = {'pid': os.getpid(), 'cwd': '/legacy', 'kind': 'command', 'phase': 'active'}
            (self.state / 'active.json').write_text(json.dumps(legacy))
            status = self.status()
            self.assertEqual(status['active']['cwd'], '/legacy')
            self.assertTrue(status['active']['legacy'])
            self.assertIsNone(status['active']['run_seconds'])
            child = self.start('new', 'pass')
            self.waiting('new')
            self.assertIsNone(child.poll())
            self.assertEqual(slot_path.stat().st_ino, inode)
        self.assertEqual(child.wait(timeout=10), 0)
        self.assertEqual(slot_path.stat().st_ino, inode)

    def test_malformed_metadata_fails_closed(self):
        (self.state / 'requests').mkdir()
        (self.state / 'requests' / 'corrupt.json').write_text('{broken')
        marker = self.state / 'must-not-start'
        r = self.cli('--', sys.executable, '-c', f'from pathlib import Path; Path({str(marker)!r}).touch()')
        self.assertNotEqual(r.returncode, 0)
        self.assertFalse(marker.exists())
        self.assertNotEqual(self.cli('--status').returncode, 0)

    def test_completed_record_is_not_overwritten_during_recovery(self):
        self.assertEqual(self.cli('--label', 'done', '--', sys.executable, '-c', 'raise SystemExit(7)').returncode, 7)
        completed = self.history()[0]
        request = dict(completed, phase='active')
        request.pop('outcome')
        request.pop('exit_code')
        ident = request['id']
        (self.state / 'requests' / (ident + '.lease')).touch()
        (self.state / 'requests' / (ident + '.json')).write_text(json.dumps(request))
        self.assertEqual(self.status()['waiting'], [])
        row = self.history()[0]
        self.assertEqual(row['exit_code'], 7)
        self.assertEqual(row['outcome'], 'exited')

    def test_screen_event_uses_the_same_sanitized_label_as_status_and_history(self):
        label = 'a\nb\x1b[31m' + 'x' * 200
        expected = ('a b [31m' + 'x' * 200)[:160]
        screen = subprocess.Popen(self.command('--reserve-screens', label),
                                  stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
        self.children.append(screen)
        self.addCleanup(screen.stdout.close)
        self.active(expected)
        event = json.loads(screen.stdout.readline())
        try:
            self.assertEqual(event['label'], expected)
            self.assertEqual(self.status()['active']['label'], expected)
        finally:
            self.assertEqual(self.cli('--release', event['token']).returncode, 0)
            self.assertEqual(screen.wait(timeout=10), 0)
        self.assertEqual(self.history()[0]['label'], expected)

    def test_private_files_and_invalid_options(self):
        self.assertEqual(self.cli('--label', 'a\nb\x1b[31m', '--', sys.executable, '-c', 'pass').returncode, 0)
        for p in self.state.rglob('*.json'):
            self.assertEqual(p.stat().st_mode & 0o077, 0, str(p))
        for args in [('--history', '--limit', '0'), ('--history', '--limit', '1001'),
                     ('--human', '--', sys.executable, '-c', 'pass'),
                     ('--status', '--', 'secret-command')]:
            self.assertEqual(self.cli(*args).returncode, 2)

    def test_cancel_active_command_records_interruption(self):
        child, _ = self.gate_owner()
        child.terminate()
        self.assertEqual(child.wait(timeout=10), 143)
        row = self.history()[0]
        self.assertEqual(row['outcome'], 'interrupted')
        self.assertEqual(row['exit_code'], 143)
        self.assertGreaterEqual(row['run_seconds'], 0)
        self.assertIsNone(self.status()['active'])

    def test_missing_counter_with_live_waiters_fails_closed(self):
        owner, gate = self.gate_owner()
        (self.state / 'counter.json').unlink()
        result = self.cli('--', sys.executable, '-c', 'raise SystemExit(99)')
        self.assertEqual(result.returncode, 2)
        gate.touch()
        self.assertEqual(owner.wait(timeout=10), 0)


class TimingUnitTests(unittest.TestCase):
    def setUp(self):
        spec = importlib.util.spec_from_file_location('queue_under_test', SCRIPT)
        self.module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.module)

    def test_elapsed_uses_monotonic_clock_and_never_negative(self):
        row = {'queued_at': 1000, 'queued_monotonic': 10, 'started_at': 1001,
               'started_monotonic': 12}
        view = self.module.with_durations(row, monotonic=15)
        self.assertEqual(view['wait_seconds'], 2)
        self.assertEqual(view['run_seconds'], 3)
        self.assertNotIn('wait_seconds', row)
        view = self.module.with_durations({'queued_at': 1000, 'queued_monotonic': 10},
                                          monotonic=9)
        self.assertEqual(view['wait_seconds'], 0)
        self.assertIsNone(view['run_seconds'])

    def test_active_cleanup_cannot_erase_a_legacy_successor(self):
        with tempfile.TemporaryDirectory(dir=SCRIPT.parent) as directory:
            state = Path(directory)
            queue = self.module.Queue(state)
            active = state / 'active.json'
            active.write_text(json.dumps({'id': 'previous'}))
            with (state / 'slot.lock').open('a+') as legacy:
                fcntl.flock(legacy, fcntl.LOCK_EX)
                with self.module.metadata_lock(state):
                    queue.clear_active('previous')
                self.assertTrue(active.exists())
            read_text = Path.read_text
            protected_reads = []

            def probe(path, *args, **kwargs):
                if path == active:
                    with (state / 'slot.lock').open('a+') as competing_legacy:
                        with self.assertRaises(BlockingIOError):
                            fcntl.flock(competing_legacy, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    protected_reads.append(True)
                return read_text(path, *args, **kwargs)

            with mock.patch.object(Path, 'read_text', probe):
                with self.module.metadata_lock(state):
                    queue.clear_active('previous')
            self.assertEqual(protected_reads, [True])
            self.assertFalse(active.exists())


if __name__ == '__main__':
    unittest.main()
