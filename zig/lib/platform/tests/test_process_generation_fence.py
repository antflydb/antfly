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

"""Exercise inherited lock ownership across actual exec and parent SIGKILL."""

import os
from pathlib import Path
import select
import signal
import subprocess
import sys
import tempfile
import time
import unittest

FIXTURE = str(Path(sys.argv.pop(1)).resolve())
CACHE = Path(sys.argv.pop(1)).resolve()
CACHE.mkdir(parents=True, exist_ok=True)


class GenerationFence(unittest.TestCase):
    def probe(self, directory):
        result = subprocess.run(
            [FIXTURE, "probe", directory], timeout=5, capture_output=True
        )
        self.assertIn(result.returncode, (0, 17), result.stderr.decode())
        return result.returncode

    def line(self, process):
        self.assertTrue(
            select.select([process.stdout], [], [], 5)[0], "fixture readiness timeout"
        )
        return process.stdout.readline().decode().strip()

    def exercise(self, close_parent=False, acknowledge=False):
        with tempfile.TemporaryDirectory(dir=CACHE) as directory:
            process = subprocess.Popen(
                [FIXTURE, "parent-close" if close_parent else "parent", directory],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                bufsize=0,
            )
            child_pid = None
            try:
                line = self.line(process)
                self.assertTrue(line.startswith("child "), line)
                child_pid = int(line.split()[1])
                self.assertEqual(self.probe(directory), 17)
                if acknowledge:
                    process.stdin.write(b"x")
                    self.assertEqual(self.line(process), "child-ready")
                if not close_parent:
                    process.kill()
                    self.assertEqual(process.wait(timeout=5), -signal.SIGKILL)
                # Parent is reaped (or explicitly closed its descriptor). The
                # child hasn't opened/acquired anything, yet replacement fails.
                self.assertEqual(self.probe(directory), 17)
                if not acknowledge:
                    process.stdin.write(b"x")
                    self.assertEqual(self.line(process), "child-ready")
                self.assertEqual(self.probe(directory), 17)
                process.stdin.close()
                # Physical descriptor release, not a PID check or elapsed
                # timeout, is the positive criterion for allowing replacement.
                deadline = time.monotonic() + 5
                while self.probe(directory) == 17:
                    self.assertLess(
                        time.monotonic(),
                        deadline,
                        "child failed to release inherited lease",
                    )
                    time.sleep(0.02)
                if close_parent:
                    self.assertEqual(
                        process.wait(timeout=5), 0, process.stderr.read().decode()
                    )
                child_pid = None
            finally:
                if child_pid is not None:
                    try:
                        os.kill(child_pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                if process.poll() is None:
                    process.kill()
                process.wait(timeout=5)
                for stream in (process.stdin, process.stdout, process.stderr):
                    stream.close()

    def test_orphan_before_startup_acknowledgement(self):
        self.exercise()

    def test_orphan_after_startup_acknowledgement(self):
        self.exercise(acknowledge=True)

    def test_parent_close_preserves_child_lease(self):
        self.exercise(close_parent=True)

    def test_close_is_idempotent_and_allows_reacquisition(self):
        with tempfile.TemporaryDirectory(dir=CACHE) as directory:
            result = subprocess.run(
                [FIXTURE, "close-twice", directory], timeout=5, capture_output=True
            )
            self.assertEqual(result.returncode, 0, result.stderr.decode())
            self.assertEqual(self.probe(directory), 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
