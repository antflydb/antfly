#!/usr/bin/env python3
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

"""Reject skipped, partial and duplicate performance evidence."""

import json
import os
import signal
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest
from check_lake_execution_runtime import observations, run_logged, PHASES, FAMILIES


class ProbeProcessBudgets(unittest.TestCase):
    def test_separate_phase_budgets_retain_phase_logs(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            build_log, run_log = root / "build.log", root / "run.log"
            # A slow build must not spend the executable's execution budget.
            run_logged(
                [sys.executable, "-c", "import time; time.sleep(.1); print('built')"],
                None,
                build_log,
                2,
            )
            run_logged([sys.executable, "-c", "print('ran')"], None, run_log, 2)
            self.assertEqual(build_log.read_text().strip(), "built")
            self.assertEqual(run_log.read_text().strip(), "ran")

    def test_execution_timeout_stops_the_process(self):
        with tempfile.TemporaryDirectory() as temporary:
            log = Path(temporary) / "run.log"
            with self.assertRaises(subprocess.TimeoutExpired):
                run_logged(
                    [
                        sys.executable,
                        "-c",
                        "import time; print('started', flush=True); time.sleep(30)",
                    ],
                    None,
                    log,
                    0.2,
                )
            self.assertIn("started", log.read_text())

    def test_timeout_stops_children_after_wrapper_exits(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            ready = root / "child.pid"
            child = (
                "import os,signal,sys,time; from pathlib import Path; "
                "signal.signal(signal.SIGTERM,signal.SIG_IGN); "
                "Path(sys.argv[1]).write_text(str(os.getpid())); time.sleep(30)"
            )
            wrapper = (
                "import os,signal,subprocess,sys,time; "
                "signal.signal(signal.SIGTERM,lambda *_:os._exit(0)); "
                "subprocess.Popen([sys.executable,'-c',sys.argv[1],sys.argv[2]]); "
                "time.sleep(30)"
            )
            child_terminated = False
            try:
                with self.assertRaises(subprocess.TimeoutExpired):
                    run_logged(
                        [sys.executable, "-c", wrapper, child, str(ready)],
                        None,
                        root / "run.log",
                        2,
                    )
                pid = int(ready.read_text())
                for _ in range(40):
                    # An orphan may briefly remain a zombie until the host's
                    # reaper runs; it is already dead and cannot retain work.
                    status = subprocess.run(
                        ["ps", "-p", str(pid), "-o", "stat="],
                        text=True,
                        capture_output=True,
                        check=False,
                    ).stdout.strip()
                    if not status or status.startswith("Z"):
                        child_terminated = True
                        break
                    time.sleep(0.05)
                else:
                    self.fail("timed-out wrapper left a live child")
            finally:
                if not child_terminated and ready.exists():
                    try:
                        os.kill(int(ready.read_text()), signal.SIGKILL)
                    except ProcessLookupError:
                        pass


class Evidence(unittest.TestCase):
    def log(self):
        return "\n".join(
            "LAKE_RUNTIME "
            + json.dumps(
                dict(
                    parallel_jobs=5,
                    headroom_mode="auto",
                    headroom_queries=1,
                    family_names=list(FAMILIES),
                    family_ns=[1] * len(FAMILIES),
                    family_calls=[1] * len(FAMILIES),
                    family_declaration_root_bytes=[1] * len(FAMILIES),
                    api_decoded_hits=1,
                    provider_decoded_hits=1,
                    io_dispatch="linear",
                    caller_io_dispatch_calls=1,
                    provider_io_dispatch_calls=1,
                    working_quota_layers=2 if mode == "native" else 1,
                    provider_quota_peak_bytes=1,
                    source_optimize="debug",
                    provider_optimize=0,
                    source_is_test=False,
                    provider_is_test=False,
                    mode=mode,
                    phase=phase,
                    round=0,
                    declarations=5,
                    rows=40 if phase == "incremental_append" else 32,
                    api_source_open_ns=0,
                    api_admission_ns=0,
                    provider_store_open_ns=1,
                    provider_source_open_ns=1,
                    provider_physical_ns=1,
                    provider_cache_hits=1,
                    provider_cache_reads=1,
                    provider_cache_bytes=1,
                    control_ns=0,
                    execution_ns=1,
                    total_attempt_ns=1,
                    peak_working_set_bytes=1,
                )
            )
            for mode in ("source", "native")
            for phase in PHASES
        )

    def test_complete(self):
        self.assertEqual(len(observations(self.log(), 1, 8)), 6)

    def test_skip_partial_and_duplicate_fail(self):
        for log in (
            "1 skipped",
            self.log().split("\n", 1)[1],
            self.log() + "\n" + self.log().split("\n")[0],
        ):
            with self.assertRaises(ValueError):
                observations(log, 1, 8)

    def test_test_mode_evidence_is_rejected(self):
        with self.assertRaises(ValueError):
            observations(
                self.log().replace('"source_is_test": false', '"source_is_test": true'),
                1,
                8,
            )

    def test_optimized_evidence_requires_both_compilation_modes(self):
        fast = (
            self.log()
            .replace('"source_optimize": "debug"', '"source_optimize": "fast"')
            .replace('"provider_optimize": 0', '"provider_optimize": 1')
        )
        self.assertEqual(len(observations(fast, 1, 8, optimize="fast")), 6)
        for log in (
            self.log(),
            fast.replace('"provider_optimize": 1', '"provider_optimize": 0'),
        ):
            with self.assertRaisesRegex(ValueError, "optimization mode mismatch"):
                observations(log, 1, 8, optimize="fast")

    def test_incorrect_workload_fails(self):
        with self.assertRaises(ValueError):
            observations(self.log(), 1, 9)


if __name__ == "__main__":
    unittest.main()
