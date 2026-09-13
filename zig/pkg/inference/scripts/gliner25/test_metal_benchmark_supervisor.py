"""Bounded protocol/process tests. No models, Torch, GPU, or network access."""
import json
import os
from pathlib import Path
import signal
import sys
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest import mock

import metal_benchmark_supervisor as supervision


FIXTURE = r'''
import json, os, signal, subprocess, sys, time
from pathlib import Path
mode, directory, arm, argument = sys.argv[1:]
def emit(value):
    print(json.dumps(value, allow_nan=False), flush=True)
def ready(**extra):
    emit(dict(event="ready", arm=arm, **extra))
if mode == "sleep":
    time.sleep(60)
elif mode == "raw":
    os.write(1, bytes.fromhex(argument))
    time.sleep(60)
elif mode == "oversized":
    os.write(1, b"x" * (4 * 1024**2 + 1))
    time.sleep(60)
elif mode == "log_overflow":
    os.write(2, b"x" * 8192)
    ready()
    time.sleep(60)
elif mode in ("descendant_exit", "detached"):
    marker = str(Path(directory) / "child-ready.json")
    child_code = """
import json,os,signal,sys,time
signal.signal(signal.SIGTERM, signal.SIG_IGN)
retained_cache = bytearray(16 * 1024**2)
with open(sys.argv[1], 'w') as f:
    json.dump({'pid':os.getpid()},f)
time.sleep(60)
"""
    child = subprocess.Popen([sys.executable, "-u", "-c", child_code, marker],
        stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        start_new_session=mode == "detached")
    while not Path(marker).exists():
        time.sleep(.005)
    ready(child_pid=child.pid)
    if mode == "detached":
        sys.stdin.readline()
    os._exit(0)
elif mode == "blocked_stdin":
    ready()
    time.sleep(60)
else:
    if mode == "fallback_split":
        os.write(2, b"UserWarning: aten::test is not supported on the MPS backend and will fall ")
    if mode == "term_warning":
        def terminate(*_):
            os.write(2, b"UserWarning: MPS operator falling back to CPU\n")
            os._exit(0)
        signal.signal(signal.SIGTERM, terminate)
    retained_cache = bytearray(2 * 1024**2)
    ready()
    for line in sys.stdin:
        command = json.loads(line)
        if command['op'] == 'stop':
            emit(dict(event='stopped', arm=arm, request_id=command['request_id']))
            break
        result = dict(event='result', arm=arm, request_id=command['request_id'],
            case_id=command['case_id'], duration_ns=100, output={'ok':True})
        if mode == "bad_id":
            result['request_id'] += 1
        elif mode == "bad_arm":
            result['arm'] = 'unowned_arm'
        elif mode == "bad_case":
            result['case_id'] = 'other-case'
        elif mode == "bad_event":
            result['event'] = 'heartbeat'
        elif mode == "invalid_duration":
            result['duration_ns'] = json.loads(argument)
        elif mode in ("error", "error_timing") and command['op'] == 'run':
            result = dict(event='error', arm=arm, request_id=command['request_id'],
                case_id=command['case_id'], category='unsupported', error_type='RuntimeError',
                message='bounded test failure', recoverable=True, device_unsafe=False)
            if mode == "error_timing":
                result['duration_ns'] = 1
        elif mode == "fallback_split":
            os.write(2, b"back to run on the CPU.\n")
        emit(result)
'''


class MetalBenchmarkSupervisorTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.directory = Path(self.temporary.name)
        self.guard = supervision.ResourceGuard(max_rss_bytes=512 * 1024**2)
        self.workers = []

    def tearDown(self):
        try:
            for worker in reversed(self.workers):
                worker.close()
        finally:
            self.temporary.cleanup()

    def worker(self, mode="normal", *, arm="antfly_metal", argument="", guard=None):
        destination = self.directory / str(len(self.workers))
        destination.mkdir()
        command = [sys.executable, "-B", "-u", "-c", FIXTURE, mode, str(destination), arm, argument]
        worker = supervision.Worker(arm, command, os.environ.copy(), destination, guard or self.guard)
        self.workers.append(worker)
        return worker

    def assert_clean(self, worker):
        self.assertTrue(worker.cleanup["complete"], worker.cleanup)
        self.assertTrue(worker.cleanup["direct_child_reaped"])
        self.assertIsNotNone(worker.process.returncode)
        self.assertEqual([], worker.cleanup["survivors"])
        self.assertEqual([], worker.cleanup["inspection_errors"])
        self.assertNotIn(worker, worker.guard.workers)
        json.dumps(worker.cleanup, allow_nan=False)

    def test_ready_result_stop_and_shared_resident_cache_accounting(self):
        for arm in ("antfly_metal", "fastino_mps"):
            worker = self.worker(arm=arm)
            self.assertEqual(arm, worker.receive(3)["arm"])
        for worker in self.workers:
            result = worker.request("validate", "unicode-東京", timeout=2)
            self.assertEqual((1, 100), (result["request_id"], result["duration_ns"]))
            self.assertEqual(2, worker.request("run", "unicode-東京", timeout=2)["request_id"])
            self.assertEqual(3, worker.request("stop", timeout=2)["request_id"])
            self.assertEqual(0, worker.process.wait(timeout=2))
            worker.close()
            self.assert_clean(worker)
        self.assertGreater(self.guard.peak_rss_bytes, 4 * 1024**2)
        self.assertEqual({"antfly_metal", "fastino_mps"}, set(self.guard.peak_rss_by_arm))
        receipt = self.guard.receipt()
        self.assertEqual(2, len(receipt["completed_workers"]))
        json.dumps(receipt, allow_nan=False)

    def test_error_response_keeps_diagnostics_and_can_retry(self):
        worker = self.worker("error", arm="fastino_cpu")
        worker.receive(3)
        error = worker.request("run", "case", timeout=2)
        self.assertEqual("error", error["event"])
        self.assertEqual("unsupported", error["category"])
        self.assertEqual("bounded test failure", error["message"])
        self.assertIs(error["recoverable"], True)
        self.assertNotIn("duration_ns", error)
        self.assertEqual("result", worker.request("validate", "case", timeout=2)["event"])

    def test_timeout_cleans_owner_and_next_worker_recovers(self):
        worker = self.worker("sleep")
        start = time.monotonic()
        with self.assertRaisesRegex(supervision.BenchmarkError, "deadline"):
            worker.receive(.08)
        self.assertLess(time.monotonic() - start, 2)
        self.assert_clean(worker)
        replacement = self.worker()
        self.assertEqual("ready", replacement.receive(3)["event"])

    def test_command_write_uses_same_deadline_when_stdin_is_full(self):
        worker = self.worker("blocked_stdin")
        worker.receive(3)
        written = 0
        while written < 1024**2:
            try:
                written += os.write(worker.process.stdin.fileno(), b"x" * 4096)
            except BlockingIOError:
                break
        self.assertLess(written, 1024**2, "fixture did not fill its bounded pipe")
        start = time.monotonic()
        with self.assertRaisesRegex(supervision.BenchmarkError, "deadline"):
            worker.request("run", "case", timeout=.08)
        self.assertLess(time.monotonic() - start, 2)
        self.assert_clean(worker)

    def test_original_group_descendant_survives_leader_then_is_killed(self):
        worker = self.worker("descendant_exit")
        ready = worker.receive(3)
        child = self.guard.psutil.Process(ready["child_pid"])
        child.create_time()  # Pin the psutil identity before any cleanup/reuse.
        self.assertEqual(0, worker.process.wait(timeout=2))
        self.assertTrue(child.is_running())
        worker.close()
        self.assert_clean(worker)
        tracked = worker.cleanup["tracked_processes"]
        self.assertIn(child.pid, [entry["pid"] for entry in tracked])
        self.assertIn((child.pid, signal.SIGKILL), [(entry["pid"], entry["signal"]) for entry in worker.cleanup["signals"]])
        self.assertGreaterEqual(self.guard.peak_rss_by_arm[worker.arm], 16 * 1024**2)
        self.assertTrue(not child.is_running() or child.status() == self.guard.psutil.STATUS_ZOMBIE)
        self.assertIn("not attempted", worker.cleanup["nonchild_reaping"])
        self.assertLess(worker.cleanup["elapsed_seconds"], 4.6)

    def test_observed_descendant_in_new_session_remains_owned(self):
        worker = self.worker("detached")
        ready = worker.receive(3)
        child = self.guard.psutil.Process(ready["child_pid"])
        self.guard.check()
        os.write(worker.process.stdin.fileno(), b"exit\n")
        self.assertEqual(0, worker.process.wait(timeout=2))
        worker.close()
        self.assert_clean(worker)
        self.assertIn(child.pid, [entry["pid"] for entry in worker.cleanup["observed_exited"]])

    def test_selector_failures_roll_back_launched_process_and_registration(self):
        original_popen = supervision.subprocess.Popen
        launched = []
        def capture(*args, **kwargs):
            process = original_popen(*args, **kwargs)
            launched.append(process)
            return process
        for where in ("create", "register"):
            with self.subTest(where=where):
                selector = mock.Mock()
                selector.register.side_effect = OSError("injected selector registration")
                selected = mock.Mock(side_effect=OSError("injected selector creation")) if where == "create" else mock.Mock(return_value=selector)
                destination = self.directory / where
                destination.mkdir()
                with mock.patch.object(supervision.subprocess, "Popen", side_effect=capture), \
                     mock.patch.object(supervision.selectors, "DefaultSelector", selected), \
                     self.assertRaisesRegex(OSError, "injected selector"):
                    supervision.Worker("antfly_metal", [sys.executable, "-c", "import time; time.sleep(60)"],
                                       os.environ.copy(), destination, self.guard)
                self.assertIsNotNone(launched[-1].returncode)
                self.assertEqual([], self.guard.workers)
                self.assertTrue(self.guard.receipt()["completed_workers"][-1]["cleanup"]["complete"])

    def test_launch_failure_preserves_existing_log_and_has_no_registered_owner(self):
        destination = self.directory / "launch"
        destination.mkdir()
        with self.assertRaises(FileNotFoundError):
            supervision.Worker("antfly_metal", ["/does/not/exist/gliner25-test"], {}, destination, self.guard)
        self.assertEqual([], self.guard.workers)
        log = destination / "antfly_metal.stderr.log"
        log.write_bytes(b"preserve failed startup")
        with self.assertRaises(FileExistsError):
            supervision.Worker("antfly_metal", [sys.executable], {}, destination, self.guard)
        self.assertEqual(b"preserve failed startup", log.read_bytes())

    def test_oversized_stdout_is_rejected_and_cleanup_does_not_retain_it(self):
        worker = self.worker("oversized")
        with self.assertRaisesRegex(supervision.BenchmarkError, "oversized stdout"):
            worker.receive(4)
        self.assertLessEqual(len(worker.buffer), supervision.MAX_RESPONSE_BYTES)
        self.assert_clean(worker)

    def test_stderr_file_is_capped_and_guard_failure_does_not_block_cleanup(self):
        guard = supervision.ResourceGuard(max_log_bytes=1024)
        worker = self.worker("log_overflow", guard=guard)
        with self.assertRaisesRegex(supervision.BenchmarkError, "log byte ceiling"):
            worker.receive(3)
        self.assertEqual(1024, worker.log_path.stat().st_size)
        self.assert_clean(worker)
        self.assertIn("stderr log byte ceiling exceeded", worker.cleanup["guard_violations"])

    def test_fallback_warning_split_across_protocol_waits_is_rejected(self):
        worker = self.worker("fallback_split")
        worker.receive(3)
        with self.assertRaisesRegex(supervision.BenchmarkError, "MPS CPU fallback"):
            worker.request("run", "case", timeout=2)
        self.assert_clean(worker)
        self.assertIn(b"MPS backend", worker.log_path.read_bytes())

    def test_fallback_during_teardown_is_drained_and_reported(self):
        worker = self.worker("term_warning")
        worker.receive(3)
        with self.assertRaisesRegex(supervision.BenchmarkError, "MPS CPU fallback"):
            worker.close()
        self.assert_clean(worker)
        self.assertIn(b"falling back", worker.log_path.read_bytes())

    def test_bad_request_identity_arm_case_event_and_timings_fail_closed(self):
        cases = [("bad_id", ""), ("bad_arm", ""), ("bad_case", ""), ("bad_event", ""), ("error_timing", "")]
        cases += [("invalid_duration", json.dumps(value)) for value in (0, -1, True, None, 1.5, "100")]
        for mode, argument in cases:
            with self.subTest(mode=mode, argument=argument):
                worker = self.worker(mode, argument=argument)
                worker.receive(3)
                with self.assertRaises(supervision.BenchmarkError):
                    worker.request("run", "case", timeout=2)
                self.assert_clean(worker)

    def test_ready_rejects_unknown_identity_event_and_nonfinite_json(self):
        messages = [b'{"event":"ready","arm":"wrong"}\n', b'{"event":"heartbeat"}\n',
                    b'{"event":"ready","arm":"antfly_metal","request_id":0}\n',
                    b'{"event":"ready","arm":"antfly_metal","value":NaN}\n',
                    b'{"event":"ready","arm":"antfly_metal","value":1e999}\n',
                    b'{"event":"ready","event":"ready","arm":"antfly_metal"}\n',
                    b'[]\n', b'null\n', b'\xff\n']
        for raw in messages:
            with self.subTest(raw=raw):
                worker = self.worker("raw", argument=raw.hex())
                with self.assertRaises(supervision.BenchmarkError):
                    worker.receive(3)
                self.assert_clean(worker)

    def test_rss_limit_rejects_real_worker_and_retains_measured_peak(self):
        guard = supervision.ResourceGuard(max_rss_bytes=1)
        worker = self.worker("sleep", guard=guard)
        with self.assertRaisesRegex(supervision.BenchmarkError, "combined worker RSS"):
            worker.receive(3)
        self.assertGreater(guard.peak_rss_bytes, 1)
        self.assertGreater(guard.peak_rss_by_arm[worker.arm], 1)
        self.assert_clean(worker)

    def test_combined_rss_includes_each_arm_and_deduplicates_creation_identity(self):
        guard = supervision.ResourceGuard(max_rss_bytes=599)
        guard.workers = [SimpleNamespace(arm="antfly_metal", _pump=lambda: None,
            _tree=SimpleNamespace(sample=lambda: {(10, 1.0): 100, (11, 1.0): 200})),
            SimpleNamespace(arm="fastino_mps", _pump=lambda: None,
            _tree=SimpleNamespace(sample=lambda: {(11, 1.0): 200, (12, 1.0): 300}))]
        with self.assertRaisesRegex(supervision.BenchmarkError, "RSS 600 exceeds 599"):
            guard.check()
        self.assertEqual({"antfly_metal": 300, "fastino_mps": 500}, guard.peak_rss_by_arm)
        self.assertEqual(600, guard.peak_rss_bytes)

    def test_command_bound_precedes_write_and_is_measured_in_utf8_bytes(self):
        worker = self.worker()
        worker.receive(3)
        with self.assertRaisesRegex(supervision.BenchmarkError, "2048 bytes"):
            worker.request("run", "東" * 1000, timeout=2)
        self.assertEqual(0, worker.sequence)
        self.assert_clean(worker)

    def test_context_exception_cleans_and_close_is_idempotent(self):
        worker = self.worker()
        with self.assertRaisesRegex(ValueError, "parent failure"):
            with worker:
                worker.receive(3)
                raise ValueError("parent failure")
        first = worker.cleanup
        worker.close()
        self.assertIs(first, worker.cleanup)
        self.assert_clean(worker)

    def test_invalid_limits_and_json_are_rejected_without_launch(self):
        for value in (0, -1, True, 1.5):
            with self.subTest(value=value), self.assertRaises(supervision.BenchmarkError):
                supervision.ResourceGuard(max_rss_bytes=value)
        with self.assertRaises(supervision.BenchmarkError):
            supervision.ResourceGuard(max_log_bytes=supervision.MAX_LOG_BYTES + 1)
        for value in (b'{"x":Infinity}', b'{"x":-Infinity}', b'{"x":1e309}', b'{"x":{"y":1,"y":2}}'):
            with self.subTest(value=value), self.assertRaises(supervision.BenchmarkError):
                supervision.strict_json(value)


if __name__ == "__main__":
    unittest.main()
