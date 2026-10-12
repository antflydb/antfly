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

"""Prevent comparative qualification from accepting inconsistent exact results."""

import json
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest

WORKER = Path(__file__).resolve().parents[1] / "regional_worker.py"


class WorkerTests(unittest.TestCase):
    def test_duplicate_launches_run_once_and_never_persist_the_credential(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            counter = root / "count"
            command = [
                sys.executable,
                "-c",
                "import sys,time; from pathlib import Path; sys.stdin.readline(); p=Path(sys.argv[1]); p.open('a').write('one\\n'); time.sleep(.2)",
                str(counter),
            ]
            job = root / "job"
            token = "short-lived-test-credential"
            for _ in range(4):
                subprocess.run(
                    [sys.executable, str(WORKER), "launch", str(job), *command],
                    input=token + "\n",
                    text=True,
                    check=True,
                    capture_output=True,
                )
            deadline = time.monotonic() + 10
            while not (job / "exit.json").exists() and time.monotonic() < deadline:
                time.sleep(0.02)
            status = json.loads((job / "exit.json").read_text())
            self.assertEqual(status["exit_code"], 0)
            self.assertEqual(counter.read_text(), "one\n")
            for path in job.iterdir():
                self.assertNotIn(token.encode(), path.read_bytes())
            subprocess.run(
                [sys.executable, str(WORKER), "launch", str(job), *command],
                input=token + "\n",
                text=True,
                check=True,
                capture_output=True,
            )
            self.assertEqual(counter.read_text(), "one\n")
            changed = subprocess.run(
                [
                    sys.executable,
                    str(WORKER),
                    "launch",
                    str(job),
                    sys.executable,
                    "-c",
                    "pass",
                ],
                input=token + "\n",
                text=True,
                capture_output=True,
            )
            self.assertNotEqual(changed.returncode, 0)

    def test_failed_job_is_terminal(self):
        with tempfile.TemporaryDirectory() as directory:
            job = Path(directory) / "job"
            command = [
                sys.executable,
                "-c",
                "import sys; sys.stdin.readline(); sys.exit(3)",
            ]
            subprocess.run(
                [sys.executable, str(WORKER), "supervise", str(job), *command],
                input="credential\n",
                text=True,
                check=True,
                capture_output=True,
            )
            self.assertEqual(
                json.loads((job / "exit.json").read_text())["exit_code"], 3
            )
