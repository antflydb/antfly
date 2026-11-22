# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Process-level coverage; pass the compiled inference_supervisor_fixture path."""

import os
import select
import signal
import subprocess
import sys
import unittest

FIXTURE = os.path.abspath(sys.argv.pop(1))


class SupervisorTests(unittest.TestCase):
    def start(self, mode="blocked", index=1, cancel=False, capture_errors=False, worker=False):
        env = os.environ.copy()
        for key in tuple(env):
            if key.startswith("ANTFLY_INFERENCE_SUPERVIS"):
                del env[key]
        env.update(FIXTURE_MODE=mode, FIXTURE_COMMAND_INDEX=str(index))
        if worker:
            env.update(ANTFLY_INFERENCE_SUPERVISED_WORKER="1",
                       ANTFLY_INFERENCE_SUPERVISOR_LIFELINE="stdin-v1")
        if cancel:
            env["FIXTURE_CANCEL_PARENT"] = "1"
        args = [FIXTURE] + (["inference"] if index == 2 else []) + ["run"]
        proc = subprocess.Popen(
            args,
            env=env,
            stdin=subprocess.PIPE if worker else None,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE if capture_errors else subprocess.DEVNULL,
            bufsize=0,
            start_new_session=True,
        )
        self.addCleanup(self.stop, proc)
        return proc

    @staticmethod
    def stop(proc):
        # A regression must fail the test without leaving a busy-loop worker
        # behind. Each fixture owns a separate process group for cleanup only.
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        proc.communicate(timeout=5)

    def worker(self, proc):
        self.assertTrue(
            select.select([proc.stdout], [], [], 5)[0], "worker startup timed out"
        )
        line = proc.stdout.readline().decode().strip()
        self.assertTrue(line.startswith("worker "), line)
        return int(line.split()[1])

    def test_parent_death_during_blocked_startup(self):
        for index in (1, 2):
            with self.subTest(index=index):
                proc = self.start(index=index)
                self.worker(proc)
                proc.send_signal(signal.SIGKILL)
                # communicate also waits for worker-owned stdout to close;
                # an orphan in native startup makes this time out.
                proc.communicate(timeout=5)
                self.assertEqual(proc.returncode, -signal.SIGKILL)

    def test_worker_lifeline_eof_is_clean_and_data_is_failure(self):
        for payload, expected in ((b"", 0), (b"x", 1)):
            with self.subTest(payload=payload):
                proc = self.start(worker=True)
                self.worker(proc)
                proc.communicate(input=payload, timeout=5)
                self.assertEqual(proc.returncode, expected)

    def test_operator_stop_during_blocked_startup(self):
        for index in (1, 2):
            for sig in (signal.SIGINT, signal.SIGTERM):
                with self.subTest(index=index, signal=sig):
                    proc = self.start(index=index)
                    pid = self.worker(proc)
                    proc.send_signal(sig)
                    output, _ = proc.communicate(timeout=5)
                    self.assertEqual(proc.returncode, 0)
                    self.assertNotIn(b"worker ", output, "operator stop restarted a worker")
                    with self.assertRaises(ProcessLookupError):
                        os.kill(pid, 0)

    def test_process_group_operator_signal_does_not_restart(self):
        for sig in (signal.SIGINT, signal.SIGTERM):
            with self.subTest(signal=sig):
                proc = self.start()
                pid = self.worker(proc)
                os.killpg(proc.pid, sig)
                output, _ = proc.communicate(timeout=5)
                self.assertEqual(proc.returncode, 0)
                self.assertNotIn(b"worker ", output)
                with self.assertRaises(ProcessLookupError):
                    os.kill(pid, 0)

    def test_repeated_operator_signals_close_lifeline_once(self):
        proc = self.start()
        pid = self.worker(proc)
        proc.send_signal(signal.SIGTERM)
        proc.send_signal(signal.SIGINT)
        output, _ = proc.communicate(timeout=5)
        self.assertEqual(proc.returncode, 0)
        self.assertNotIn(b"worker ", output)
        with self.assertRaises(ProcessLookupError):
            os.kill(pid, 0)

    def test_operator_stop_interrupts_restart_backoff(self):
        proc = self.start("restart", capture_errors=True)
        # Six completed workers reach the existing 3.2s backoff. Observe the
        # actual log boundary rather than guessing with a wall-clock sleep.
        for _ in range(6):
            self.worker(proc)
        while True:
            self.assertTrue(select.select([proc.stderr], [], [], 5)[0])
            if b"delay_ms=3200" in proc.stderr.readline():
                break
        proc.send_signal(signal.SIGTERM)
        output, _ = proc.communicate(timeout=2)
        self.assertEqual(proc.returncode, 0)
        self.assertNotIn(b"worker ", output)

    def test_clean_exit_and_configuration_failure_do_not_restart(self):
        for mode, successful in (("clean", True), ("fail", False)):
            with self.subTest(mode=mode):
                proc = self.start(mode)
                self.worker(proc)
                output, _ = proc.communicate(timeout=5)
                self.assertNotIn(b"worker ", output)
                self.assertEqual(proc.returncode == 0, successful)

    def test_watchdog_and_native_crash_restart(self):
        for mode in ("restart", "blocked"):
            with self.subTest(mode=mode):
                proc = self.start(mode)
                first = self.worker(proc)
                if mode == "blocked":
                    os.kill(first, signal.SIGKILL)
                second = self.worker(proc)
                self.assertNotEqual(first, second)
                self.assertIsNone(proc.poll())

    def test_canceling_supervisor_reaps_worker(self):
        proc = self.start(cancel=True)
        pid = self.worker(proc)
        proc.communicate(timeout=5)
        self.assertEqual(proc.returncode, 0)
        with self.assertRaises(ProcessLookupError):
            os.kill(pid, 0)


if __name__ == "__main__":
    unittest.main()
