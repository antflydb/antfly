# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""No-model process coverage for the one-shot command worker and lifeline."""

import os
import select
import signal
import subprocess
import sys
import time
import unittest


FIXTURE = os.path.abspath(sys.argv.pop(1))


class OneShotTests(unittest.TestCase):
    def start(
        self, mode="blocked", *, timeout=1500, grace=300, cancel=False,
        parent_atexit=False,
    ):
        environment = os.environ.copy()
        for key in tuple(environment):
            if key.startswith("ANTFLY_TRAINING_") or key.startswith(
                "ANTFLY_INFERENCE_SUPERVIS"
            ):
                del environment[key]
        environment.update(
            FIXTURE_MODE=mode,
            FIXTURE_TIMEOUT_MS=str(timeout),
            FIXTURE_GRACE_MS=str(grace),
        )
        if cancel:
            environment["FIXTURE_CANCEL_PARENT"] = "1"
        if parent_atexit:
            environment["FIXTURE_PARENT_ATEXIT"] = "1"
        else:
            environment.pop("FIXTURE_PARENT_ATEXIT", None)
        process = subprocess.Popen(
            [FIXTURE, "job.json"],
            env=environment,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            bufsize=0,
            start_new_session=True,
        )
        workers = []
        self.addCleanup(self.stop, process, workers)
        return process, workers

    @staticmethod
    def stop(process, workers):
        # Only Popen owns a live process handle. Historical worker PIDs can
        # already have been reaped/reused, so never signal them in cleanup.
        # Parent loss closes the lifeline; every negative fixture also has an
        # independent hard bound, including the deliberate pre-monitor stall.
        if process.poll() is None:
            process.kill()
        process.communicate(timeout=20)
        workers.clear()

    def worker(self, process, workers):
        self.assertTrue(select.select([process.stdout], [], [], 5)[0])
        line = process.stdout.readline().decode().strip()
        self.assertTrue(line.startswith("worker "), line)
        pid = int(line.split()[1])
        workers.append(pid)
        return pid

    def finish(self, process):
        output, error = process.communicate(timeout=5)
        self.assertNotIn(b"worker ", output, "a command worker must never restart")
        return output, error

    def test_clean_error_and_watchdog_exit_never_restart(self):
        for mode, code in (
            ("clean", 0),
            ("clean_atexit", 0),
            ("fail", 7),
            ("fatal", 86),
            ("restart_atexit", 86),
        ):
            with self.subTest(mode=mode):
                process, workers = self.start(mode)
                pid = self.worker(process, workers)
                self.finish(process)
                self.assertEqual(process.returncode, code)
                with self.assertRaises(ProcessLookupError):
                    os.kill(pid, 0)

    def test_sigint_and_sigterm_pause_at_worker_boundary(self):
        for requested in (signal.SIGINT, signal.SIGTERM):
            with self.subTest(signal=requested):
                process, workers = self.start("pause")
                self.worker(process, workers)
                process.send_signal(requested)
                output, _ = self.finish(process)
                self.assertIn(b"paused\n", output)
                self.assertEqual(process.returncode, 128 + requested)

    def test_parent_death_during_blocked_native_work(self):
        for mode in ("blocked", "blocked_atexit"):
            with self.subTest(mode=mode):
                process, workers = self.start(mode, timeout=10_000)
                self.worker(process, workers)
                process.kill()
                # communicate waits for the inherited worker stdout pipe too.
                # libc exit would hang in the intentionally blocked atexit.
                self.finish(process)
                self.assertEqual(process.returncode, -signal.SIGKILL)

    def test_hard_timeout_covers_work_startup_and_teardown(self):
        for mode in ("blocked", "blocked_atexit", "before_monitor", "teardown"):
            with self.subTest(mode=mode):
                process, workers = self.start(mode, timeout=250, grace=100)
                pid = self.worker(process, workers)
                self.finish(process)
                self.assertEqual(process.returncode, 124)
                with self.assertRaises(ProcessLookupError):
                    os.kill(pid, 0)

    def test_cooperative_timeout_uses_the_same_deadline_status(self):
        process, workers = self.start("cooperative_timeout", timeout=200, grace=1000)
        self.worker(process, workers)
        self.finish(process)
        self.assertEqual(process.returncode, 124)

    def test_second_signal_forces_termination_before_long_grace(self):
        process, workers = self.start(timeout=10_000, grace=3000)
        pid = self.worker(process, workers)
        started = time.monotonic()
        process.send_signal(signal.SIGTERM)
        time.sleep(0.05)
        process.send_signal(signal.SIGTERM)
        self.finish(process)
        self.assertLess(time.monotonic() - started, 2)
        self.assertEqual(process.returncode, 128 + signal.SIGTERM)
        with self.assertRaises(ProcessLookupError):
            os.kill(pid, 0)

    def test_canceling_parent_reaps_worker(self):
        process, workers = self.start(timeout=10_000, cancel=True)
        pid = self.worker(process, workers)
        self.finish(process)
        self.assertEqual(process.returncode, 0)
        with self.assertRaises(ProcessLookupError):
            os.kill(pid, 0)

    def test_worker_rejects_mismatched_configuration_before_work(self):
        process, _ = self.start("wrong_fingerprint")
        output, _ = self.finish(process)
        self.assertEqual(output, b"")
        self.assertEqual(process.returncode, 9)

    def test_completed_parent_bypasses_blocking_exit_handlers_after_cleanup(self):
        for mode, code, requested in (
            ("clean", 0, None),
            ("fail", 7, None),
            ("fatal", 86, None),
            ("blocked", 124, None),
            ("cooperative_timeout", 124, None),
            ("pause", 130, signal.SIGINT),
            ("pause", 143, signal.SIGTERM),
        ):
            with self.subTest(mode=mode, signal=requested):
                process, workers = self.start(
                    mode, timeout=500, grace=200, parent_atexit=True
                )
                pid = self.worker(process, workers)
                if requested is not None:
                    process.send_signal(requested)
                output, _ = self.finish(process)
                self.assertEqual(process.returncode, code)
                self.assertEqual(output.count(b"parent-cleanup\n"), 1)
                if requested is not None:
                    self.assertIn(b"paused\n", output)
                with self.assertRaises(ProcessLookupError):
                    os.kill(pid, 0)

        # The embedding cancellation API returns an error only after its
        # force-reap and caller-owned cleanup. The executable maps that error
        # to a failed outcome and must bypass the parent handler as well.
        process, workers = self.start(
            timeout=10_000, cancel=True, parent_atexit=True
        )
        pid = self.worker(process, workers)
        output, error = self.finish(process)
        self.assertEqual(process.returncode, 1)
        self.assertEqual(output.count(b"parent-cleanup\n"), 1)
        self.assertIn(b"fixture supervisor failed: Canceled", error)
        with self.assertRaises(ProcessLookupError):
            os.kill(pid, 0)


if __name__ == "__main__":
    unittest.main()
