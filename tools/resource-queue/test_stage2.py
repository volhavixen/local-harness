import fcntl
from http.server import BaseHTTPRequestHandler, HTTPServer
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import unittest
from unittest import mock

import test_stage1

SCRIPT = Path(__file__).with_name('heavy_check.py')


class ParallelTests(unittest.TestCase):
    # Reuse only fixture helpers, not the stage1 test cases.
    setUp = test_stage1.StageOneTests.setUp
    stop_children = test_stage1.StageOneTests.stop_children
    command = test_stage1.StageOneTests.command
    cli = test_stage1.StageOneTests.cli
    start = test_stage1.StageOneTests.start
    eventually = test_stage1.StageOneTests.eventually
    status = test_stage1.StageOneTests.status
    history = test_stage1.StageOneTests.history
    active = test_stage1.StageOneTests.active
    waiting = test_stage1.StageOneTests.waiting
    gate_owner = test_stage1.StageOneTests.gate_owner

    def gated(self, label, workload):
        gate = self.state / (label + '-finish')
        marker = self.state / (label + '-started')
        code = ('from pathlib import Path; import time; '
                f'Path({str(marker)!r}).touch(); gate=Path({str(gate)!r}); '
                '\nwhile not gate.exists(): time.sleep(.02)')
        child = self.start(label, code, '--class', workload)
        self.addCleanup(gate.touch)
        return child, gate, marker

    def test_parallel_status_empty_is_backward_compatible(self):
        status = self.status()
        self.assertIsNone(status['active'])
        self.assertEqual(status['parallel_active'], [])
        self.assertEqual(status['limits'], {'heavy': 1, 'parallel': 2})
        self.assertEqual(status['waiting'], [])

    def test_light_and_network_run_while_heavy_and_its_waiter_remain_blocked(self):
        owner, heavy_gate = self.gate_owner()
        next_heavy = self.start('next-heavy', 'pass')
        self.waiting('next-heavy')
        light, light_gate, lm = self.gated('light', 'light')
        network, network_gate, nm = self.gated('network', 'network')
        self.eventually(lm.exists)
        self.eventually(nm.exists)
        status = self.status()
        self.assertEqual(status['active']['label'], 'blocker')
        self.assertEqual({r['workload'] for r in status['parallel_active']}, {'light', 'network'})
        self.assertIsNone(next_heavy.poll())
        self.assertIn('Parallel running (2/2)', self.cli('--status', '--human').stdout)
        light_gate.touch()
        network_gate.touch()
        self.assertEqual(light.wait(timeout=10), 0)
        self.assertEqual(network.wait(timeout=10), 0)
        self.assertEqual(self.status()['active']['label'], 'blocker')
        heavy_gate.touch()
        self.assertEqual(owner.wait(timeout=10), 0)
        self.assertEqual(next_heavy.wait(timeout=10), 0)

    def test_parallel_classes_share_two_places_and_pending_order(self):
        a, ga, ma = self.gated('A', 'light')
        b, gb, mb = self.gated('B', 'network')
        self.eventually(ma.exists)
        self.eventually(mb.exists)
        c, gc, mc = self.gated('C', 'network')
        self.waiting('C')
        d, gd, md = self.gated('D', 'light')
        self.waiting('D')
        self.assertFalse(mc.exists())
        self.assertFalse(md.exists())
        status = self.status()
        self.assertEqual([r['lane_position'] for r in status['waiting']], [1, 2])
        self.assertEqual(len(status['parallel_active']), 2)
        gb.touch()
        self.assertEqual(b.wait(timeout=10), 0)
        self.eventually(mc.exists)
        self.assertFalse(md.exists())
        ga.touch()
        self.assertEqual(a.wait(timeout=10), 0)
        self.eventually(md.exists)
        gc.touch()
        gd.touch()
        self.assertEqual(c.wait(timeout=10), 0)
        self.assertEqual(d.wait(timeout=10), 0)

    def test_full_parallel_lane_cannot_block_heavy_or_legacy_scheduler(self):
        a, ga, ma = self.gated('A', 'light')
        b, gb, mb = self.gated('B', 'network')
        self.eventually(ma.exists)
        self.eventually(mb.exists)
        c, gc, mc = self.gated('C', 'light')
        self.waiting('C')
        rows = self.status()['waiting']
        self.assertEqual(rows[0]['phase'], 'parallel_queued')
        # A real subprocess follows the stage1 selection protocol (only
        # phase='queued'), against the same metadata, leases and slot file.
        legacy_code = '''
import importlib.util,sys,time,fcntl
from pathlib import Path
spec=importlib.util.spec_from_file_location('queue',sys.argv[1])
q=importlib.util.module_from_spec(spec); spec.loader.exec_module(q)
state=Path(sys.argv[2]); queue=q.Queue(state)
row,lease=queue.register({'label':'legacy-heavy','kind':'command','cwd':str(state)})
with lease, (state/'slot.lock').open('a+') as slot:
    while True:
        with q.metadata_lock(state):
            pending=[r for r in queue.live_requests() if r['phase']=='queued']
            if pending and pending[0]['id']==row['id']:
                try: fcntl.flock(slot,fcntl.LOCK_EX|fcntl.LOCK_NB)
                except BlockingIOError: pass
                else: break
        time.sleep(.02)
    (state/'legacy-started').touch()
    with q.metadata_lock(state): queue.finish(row,'exited',0)
'''
        legacy = subprocess.Popen([sys.executable, '-c', legacy_code, str(SCRIPT), str(self.state)],
                                  stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        self.children.append(legacy)
        self.eventually(lambda: (self.state / 'legacy-started').exists())
        self.assertEqual(legacy.wait(timeout=10), 0)
        heavy = self.start('heavy', 'pass')
        self.assertEqual(heavy.wait(timeout=10), 0)
        self.assertFalse(mc.exists())
        ga.touch()
        gb.touch()
        self.assertEqual(a.wait(timeout=10), 0)
        self.assertEqual(b.wait(timeout=10), 0)
        self.eventually(mc.exists)
        gc.touch()
        self.assertEqual(c.wait(timeout=10), 0)

    def test_screen_token_and_heavy_owner_survive_parallel_completion(self):
        screen = subprocess.Popen(self.command('--reserve-screens', 'screen'),
                                  stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
        self.children.append(screen)
        self.addCleanup(screen.stdout.close)
        self.active('screen')
        token = json.loads(screen.stdout.readline())['token']
        heavy = self.start('heavy', 'pass')
        self.waiting('heavy')
        light = self.start('light', 'raise SystemExit(7)', '--class', 'light')
        self.assertEqual(light.wait(timeout=10), 7)
        self.assertIsNone(heavy.poll())
        status = self.status()
        self.assertEqual(status['active']['label'], 'screen')
        self.assertEqual(status['active'].get('token'), token)
        row = next(r for r in self.history() if r['label'] == 'light')
        self.assertEqual(row['workload'], 'light')
        self.assertEqual(row['exit_code'], 7)
        self.assertNotIn(token, json.dumps(self.history()))
        self.assertNotIn(token, self.cli('--status', '--human').stdout)
        self.assertEqual(self.cli('--release', token).returncode, 0)
        self.assertEqual(screen.wait(timeout=10), 0)
        self.assertEqual(heavy.wait(timeout=10), 0)

    def test_orphan_parallel_child_retains_capacity_until_it_finishes(self):
        a, ga, ma = self.gated('A', 'light')
        b, gb, mb = self.gated('B', 'network')
        self.eventually(ma.exists)
        self.eventually(mb.exists)
        self.eventually(lambda: all(r.get('child_pid') for r in self.status()['parallel_active']))
        a.kill()
        a.wait(timeout=5)
        c, gc, mc = self.gated('C', 'light')
        self.waiting('C')
        self.assertFalse(mc.exists())
        self.assertEqual(len(self.status()['parallel_active']), 2)
        self.assertFalse(any(r['label'] == 'A' for r in self.history()))
        ga.touch()
        self.eventually(mc.exists)
        row = next(r for r in self.history() if r['label'] == 'A')
        self.assertEqual(row['outcome'], 'unknown')
        self.assertIsNone(row['exit_code'])
        gb.touch()
        gc.touch()
        self.assertEqual(b.wait(timeout=10), 0)
        self.assertEqual(c.wait(timeout=10), 0)

    def test_cancel_parallel_waiter_preserves_other_children(self):
        a, ga, ma = self.gated('A', 'light')
        b, gb, mb = self.gated('B', 'network')
        self.eventually(ma.exists)
        self.eventually(mb.exists)
        c, gc, mc = self.gated('C', 'light')
        self.waiting('C')
        c.terminate()
        self.assertEqual(c.wait(timeout=10), 143)
        self.assertFalse(mc.exists())
        self.assertIsNone(a.poll())
        self.assertIsNone(b.poll())
        row = next(r for r in self.history() if r['label'] == 'C')
        self.assertEqual(row['outcome'], 'cancelled')
        self.assertEqual(row['exit_code'], 143)
        ga.touch()
        gb.touch()
        self.assertEqual(a.wait(timeout=10), 0)
        self.assertEqual(b.wait(timeout=10), 0)

    def test_network_http_request_does_not_wait_for_heavy_slot(self):
        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                self.send_response(200)
                self.end_headers()
                self.wfile.write(b'queue-network-ok')

            def log_message(self, *args):
                pass

        server = HTTPServer(('127.0.0.1', 0), Handler)
        thread = threading.Thread(target=server.serve_forever)
        thread.start()
        try:
            owner, gate = self.gate_owner()
            url = 'http://127.0.0.1:%d/' % server.server_port
            code = f'import urllib.request; assert urllib.request.urlopen({url!r}, timeout=3).read()==b"queue-network-ok"'
            child = self.start('HTTP', code, '--class', 'network')
            self.assertEqual(child.wait(timeout=10), 0)
            self.assertIsNone(owner.poll())
            gate.touch()
            self.assertEqual(owner.wait(timeout=10), 0)
        finally:
            server.shutdown()
            thread.join(timeout=5)
            server.server_close()

    def test_invalid_parallel_options_do_not_register_requests(self):
        cases = [('--class', 'invalid'), ('--class', 'light', '--workers', '2'),
                 ('--class', 'network', '--workers', '2'),
                 ('--class', 'light', '--runner', 'playwright'),
                 ('--class', 'network', '--runner', 'pytest')]
        for options in cases:
            result = self.cli(*options, '--', sys.executable, '-c', 'pass')
            self.assertEqual(result.returncode, 2, result.stderr)
        for options in [('--class', 'light'), ('--class', 'network')]:
            self.assertEqual(self.cli(*options, '--reserve-screens', 'screen').returncode, 2)
        self.assertEqual(self.cli('--class', 'light', '--', 'playwright', 'test').returncode, 2)
        self.assertEqual(self.status()['waiting'], [])
        self.assertEqual(self.history(), [])

    def test_parallel_active_cancel_and_launch_failure_are_recorded(self):
        a, _, marker = self.gated('A', 'light')
        self.eventually(marker.exists)
        a.terminate()
        self.assertEqual(a.wait(timeout=10), 143)
        row = self.history()[0]
        self.assertEqual((row['workload'], row['outcome'], row['exit_code']), ('light', 'interrupted', 143))
        self.assertEqual(self.status()['parallel_active'], [])
        result = self.cli('--class', 'network', '--', '/no/such/queue-network-command')
        self.assertEqual(result.returncode, 127)
        self.assertEqual(self.history()[0]['outcome'], 'launch_error')


class AdmissionUnitTests(unittest.TestCase):
    def setUp(self):
        spec = importlib.util.spec_from_file_location('queue_stage2', SCRIPT)
        self.module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.module)

    def test_admission_counts_both_parallel_classes_and_ignores_heavy(self):
        rows = [dict(id='heavy', sequence=1, phase='active'),
                dict(id='a', sequence=2, workload='light', phase='active'),
                dict(id='b', sequence=3, workload='network', phase='parallel_queued'),
                dict(id='c', sequence=4, workload='light', phase='parallel_queued')]
        self.assertTrue(self.module.parallel_admission(rows, 'b'))
        self.assertFalse(self.module.parallel_admission(rows, 'c'))
        self.assertFalse(self.module.parallel_admission(rows, 'missing'))
        rows[1]['phase'] = 'parallel_queued'
        self.assertTrue(self.module.parallel_admission(rows, 'a'))
        self.assertFalse(self.module.parallel_admission(rows, 'b'))
        self.assertFalse(self.module.parallel_admission(rows, 'c'))
        rows[1]['phase'] = 'active'
        self.assertTrue(self.module.parallel_admission(rows, 'b'))

    def test_missing_workload_is_heavy_and_unknown_workload_fails_closed(self):
        self.assertEqual(self.module.workload({}), 'heavy')
        with self.assertRaises(ValueError):
            self.module.workload({'workload': 'future-class'})

    def test_cancelled_before_spawn_never_starts_command(self):
        with tempfile.TemporaryDirectory(dir=SCRIPT.parent) as directory:
            state = Path(directory)
            queue = self.module.Queue(state)
            row, lease = queue.register({'workload': 'light', 'label': 'cancelled'})
            marker = state / 'must-not-start'
            with lease:
                result = self.module.run_command(queue, row, lease,
                    [sys.executable, '-c', f'from pathlib import Path; Path({str(marker)!r}).touch()'], [15])
                self.assertEqual(result, ('cancelled', 143))
                self.assertFalse(marker.exists())
                with self.module.metadata_lock(state):
                    queue.finish(row, *result)

    def test_pid_write_failure_keeps_capacity_and_known_child_exit(self):
        with tempfile.TemporaryDirectory(dir=SCRIPT.parent) as directory:
            state = Path(directory)
            queue = self.module.Queue(state)
            row, lease = queue.register({'workload': 'light', 'label': 'io-failure'})
            row['phase'] = 'active'
            with self.module.metadata_lock(state):
                queue.save_active(row)
            marker, gate = state / 'started', state / 'finish'
            code = ('from pathlib import Path; import time; '
                    f'Path({str(marker)!r}).touch(); gate=Path({str(gate)!r}); '
                    '\nwhile not gate.exists(): time.sleep(.02)\nraise SystemExit(7)')
            results = {}

            def execute():
                try:
                    results['result'] = self.module.run_command(queue, row, lease,
                        [sys.executable, '-c', code], [])
                except Exception as error:
                    results['error'] = type(error).__name__

            with lease:
                with mock.patch.object(queue, 'save_active', side_effect=OSError('simulated IO failure')):
                    thread = threading.Thread(target=execute)
                    thread.start()
                    try:
                        test_stage1.StageOneTests.eventually(self, marker.exists)
                        self.assertTrue(thread.is_alive())
                        self.assertEqual(len(queue.snapshot()['parallel_active']), 1)
                    finally:
                        gate.touch()
                        thread.join(timeout=10)
                self.assertFalse(thread.is_alive())
                self.assertNotIn('error', results)
                self.assertEqual(results['result'], ('exited', 7))
                with self.module.metadata_lock(state):
                    queue.finish(row, *results['result'])
            self.assertEqual(queue.past(1)[0]['exit_code'], 7)
            self.assertEqual(queue.past(1)[0]['metadata_warning'], 'child_pid_update_failed')


if __name__ == '__main__':
    unittest.main()
