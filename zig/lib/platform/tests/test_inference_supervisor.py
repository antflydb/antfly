# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Process-level coverage; pass the compiled inference_supervisor_fixture path."""

import os
import select
import signal
import subprocess
import sys
import unittest

FIXTURE = os.path.abspath(sys.argv.pop(1))


class SupervisorTests(unittest.TestCase):
    def start(self, mode="blocked", index=1, cancel=False):
        env = os.environ.copy()
        for key in tuple(env):
            if key.startswith("ANTFLY_INFERENCE_SUPERVIS"):
                del env[key]
        env.update(FIXTURE_MODE=mode, FIXTURE_COMMAND_INDEX=str(index))
        if cancel:
            env["FIXTURE_CANCEL_PARENT"] = "1"
        args = [FIXTURE] + (["inference"] if index == 2 else []) + ["run"]
        proc = subprocess.Popen(
            args,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
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
            for sig in (signal.SIGTERM, signal.SIGKILL):
                with self.subTest(index=index, signal=sig):
                    proc = self.start(index=index)
                    self.worker(proc)
                    proc.send_signal(sig)
                    # communicate also waits for worker-owned stdout to close;
                    # an orphan in native startup makes this time out.
                    proc.communicate(timeout=5)
                    self.assertEqual(proc.returncode, -sig)

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
