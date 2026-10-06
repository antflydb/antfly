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

import contextlib
import io
import importlib.util
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path

SCRIPT = Path(__file__).with_name("run_test_partitions.py")
SPEC = importlib.util.spec_from_file_location("run_test_partitions", SCRIPT)
assert SPEC and SPEC.loader
partitions = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = partitions
SPEC.loader.exec_module(partitions)


class RunTestPartitionsTest(unittest.TestCase):
    def test_builds_disjoint_partition_and_complement_commands(self):
        partition, complement = partitions.build_commands(
            Path("test-binary"),
            ["db restore", "db dense"],
            ["simulation", "release scale"],
            [
                "--seed=123",
                "--skip-test-filter",
                "caller skip",
                "--test-filter",
                "requested case",
            ],
        )
        self.assertEqual(
            [
                "test-binary",
                "--suite-filter",
                "db restore",
                "--suite-filter",
                "db dense",
                "--skip-test-filter",
                "simulation",
                "--skip-test-filter",
                "release scale",
                "--seed=123",
                "--skip-test-filter",
                "caller skip",
                "--test-filter",
                "requested case",
            ],
            partition,
        )
        self.assertEqual(
            [
                "test-binary",
                "--suite-filter",
                "storage.",
                "--skip-test-filter",
                "simulation",
                "--skip-test-filter",
                "release scale",
                "--skip-test-filter",
                "db restore",
                "--skip-test-filter",
                "db dense",
                "--seed=123",
                "--skip-test-filter",
                "caller skip",
                "--test-filter",
                "requested case",
            ],
            complement,
        )

    def test_preserves_in_flight_output_before_child_completes(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            release = root / "release"
            command = [
                sys.executable,
                "-c",
                (
                    "import pathlib, sys, time; "
                    "sys.stdout.write('test in progress...'); sys.stdout.flush(); "
                    "release = pathlib.Path(sys.argv[1]); "
                    "deadline = time.monotonic() + 5\n"
                    "while not release.exists() and time.monotonic() < deadline: time.sleep(0.01)\n"
                    "sys.exit(7)"
                ),
                str(release),
            ]
            result = []
            runner = threading.Thread(
                target=lambda: result.append(
                    partitions.run_partitions((("blocked", command),), root / "logs")
                )
            )
            runner.start()
            try:
                deadline = time.monotonic() + 5
                while time.monotonic() < deadline:
                    logs = list((root / "logs").glob("*.log"))
                    if logs and logs[0].read_bytes() == b"test in progress...":
                        break
                    time.sleep(0.01)
                else:
                    self.fail("partial test output was buffered until process exit")
                self.assertTrue(runner.is_alive())
            finally:
                release.touch()
                runner.join(timeout=10)
            self.assertFalse(runner.is_alive())
            self.assertEqual(result, [7])

    def test_inventory_preserves_records_across_large_and_partial_writes(self):
        command = [
            sys.executable,
            "-c",
            "import os; os.write(2, b'TEST\\t' + b'x' * 70000); os.write(2, b'\\nTEST\\tlast\\n')",
        ]
        output = io.StringIO()
        with contextlib.redirect_stderr(output):
            self.assertEqual(0, partitions.list_partitions((command, command)))
        self.assertEqual(
            output.getvalue(), ("TEST\t" + "x" * 70000 + "\nTEST\tlast\n") * 2
        )

    def test_runs_both_commands_concurrently(self):
        command = [sys.executable, "-c", "import time; time.sleep(0.5)"]
        started = time.monotonic()
        self.assertEqual(
            0,
            partitions.run_partitions((("first", command), ("second", command))),
        )
        self.assertLess(time.monotonic() - started, 0.85)


if __name__ == "__main__":
    unittest.main()
