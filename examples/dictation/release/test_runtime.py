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

"""Exercise the real app's headless supervisor without starting a real server."""

import os
import socket
import subprocess
import tempfile
import time
import unittest
from pathlib import Path


@unittest.skipUnless(
    os.environ.get("DICTATION_TEST_APP"),
    "Set DICTATION_TEST_APP to a built app executable",
)
class RuntimeSupervisorTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.child = self.root / "antfly"
        self.child.write_text("""#!/bin/bash
trap 'echo stopped > "$ROOT/stopped"; exit 0' TERM
printf '%s' "$$" > "$ROOT/pid"
printf '%s\\n' "$@" > "$ROOT/args"
while true; do sleep 0.1; done
""")
        # The supervisor deliberately clears the environment; embed fixture path.
        self.child.write_text(self.child.read_text().replace("$ROOT", str(self.root)))
        self.child.chmod(0o700)

    def launch(self, mode="--managed-runtime"):
        process = subprocess.Popen(
            [os.environ["DICTATION_TEST_APP"], mode, str(self.child), str(self.root)],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        if mode == "--managed-runtime":
            process.stdin.write(b"synthetic-bootstrap-password\n")
            process.stdin.flush()
        self.addCleanup(self.finish, process)
        return process

    def finish(self, process):
        if process.stdin and not process.stdin.closed:
            process.stdin.close()
        try:
            process.wait(timeout=15)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()
        for stream in (process.stdout, process.stderr):
            stream.close()

    def wait_file(self, name):
        path = self.root / name
        deadline = time.monotonic() + 10
        while not path.exists() and time.monotonic() < deadline:
            time.sleep(0.02)
        self.assertTrue(path.exists(), name)
        return path

    def test_eof_stops_and_reaps_owned_engine(self):
        process = self.launch()
        pid = int(self.wait_file("pid").read_text())
        self.finish(process)
        self.assertTrue((self.root / "stopped").exists())
        with self.assertRaises(ProcessLookupError):
            os.kill(pid, 0)
        args = (self.root / "args").read_text().splitlines()
        self.assertEqual(args[args.index("--host") + 1], "127.0.0.1")
        self.assertEqual(args[args.index("--data-dir") + 1], str(self.root / "data"))

    def test_second_instance_cannot_attach_or_kill_first_engine(self):
        first = self.launch()
        self.wait_file("pid")
        second = self.launch()
        self.assertEqual(second.wait(timeout=10), 75)
        self.assertIsNone(first.poll())
        self.assertFalse((self.root / "stopped").exists())

    def test_busy_port_blocks_launch_instead_of_attaching(self):
        with socket.socket() as listener:
            try:
                listener.bind(("127.0.0.1", 47831))
            except OSError:
                self.skipTest("Another engine already uses the managed port")
            process = self.launch()
            self.assertEqual(process.wait(timeout=10), 78)
            self.assertFalse((self.root / "pid").exists())
            self.assertIn("occupied", (self.root / "runtime.log").read_text())

    def test_model_download_is_also_owned_and_stopped_on_eof(self):
        process = self.launch("--managed-runtime-pull")
        self.wait_file("pid")
        self.finish(process)
        self.assertTrue((self.root / "stopped").exists())
        args = (self.root / "args").read_text().splitlines()
        self.assertEqual(args[:3], ["inference", "pull", "openai/whisper-tiny"])
        self.assertEqual(
            args[args.index("--models-dir") + 1], str(self.root / "models")
        )

    def test_noisy_engine_has_a_bounded_log(self):
        self.child.write_text("#!/bin/bash\nhead -c 4194304 /dev/zero\nsleep 0.2\n")
        process = self.launch("--managed-runtime-pull")
        self.assertEqual(process.wait(timeout=10), 0)
        self.assertLessEqual((self.root / "download.log").stat().st_size, 1048576)
