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

import os
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path

from scripts.packaging.fixture_process import run_fixture


class FixtureProcessTests(unittest.TestCase):
    def test_build_exit_status_and_output_are_preserved(self):
        for status in (0, 7):
            with self.subTest(status=status):
                result = run_fixture(
                    [
                        sys.executable,
                        "-c",
                        f"import sys; print('stdout'); print('stderr', file=sys.stderr); sys.exit({status})",
                    ],
                    cwd=Path.cwd(),
                )
                self.assertEqual(result.returncode, status)
                self.assertEqual(result.stdout, "stdout\n")
                self.assertEqual(result.stderr, "stderr\n")

    @unittest.skipUnless(os.name == "posix", "POSIX compiler process groups")
    def test_timeout_stops_compiler_descendants_before_cleanup(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            child_pid = root / "compiler.pid"
            command = [
                sys.executable,
                "-c",
                """
import os, signal, time
from pathlib import Path
pid = os.fork()
if pid == 0:
    signal.alarm(5)
    Path("compiler.pid").write_text(str(os.getpid()))
    print("compiler child ready", flush=True)
    time.sleep(10)
else:
    time.sleep(10)
""",
            ]
            started = time.monotonic()
            try:
                with self.assertRaises(subprocess.TimeoutExpired) as caught:
                    run_fixture(command, cwd=root, timeout=1)
                self.assertIn("compiler child ready", caught.exception.output.decode())
                self.assertLess(time.monotonic() - started, 4)
                state = subprocess.run(
                    ["ps", "-o", "stat=", "-p", child_pid.read_text()],
                    capture_output=True,
                    text=True,
                    check=False,
                )
                self.assertIn(state.returncode, (0, 1), state.stderr)
                self.assertTrue(
                    not state.stdout.strip() or state.stdout.strip().startswith("Z"),
                    state.stdout,
                )
            finally:
                # Also clean up if this regression catches a broken runner.
                if child_pid.exists():
                    try:
                        os.kill(int(child_pid.read_text()), signal.SIGKILL)
                    except ProcessLookupError:
                        pass


if __name__ == "__main__":
    unittest.main()
