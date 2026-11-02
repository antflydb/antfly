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

"""Production RPC fatal callbacks must bypass blocked C teardown."""

import os
from pathlib import Path
import select
import signal
import subprocess
import struct
import json
import sys
import tempfile
import time
import unittest

LEASE_FIXTURE = str(Path(sys.argv.pop(1)).resolve())
WORKER_FIXTURE = str(Path(sys.argv.pop(1)).resolve())
CACHE = Path(sys.argv.pop(1)).resolve()
CACHE.mkdir(parents=True, exist_ok=True)


class WorkerLifetime(unittest.TestCase):
    def line(self, stream):
        self.assertTrue(
            select.select([stream], [], [], 5)[0], "fixture readiness timeout"
        )
        return stream.readline().decode().strip()

    def cleanup(self, process):
        if process.poll() is None:
            process.kill()
        process.wait(timeout=5)
        for stream in (process.stdin, process.stdout, process.stderr):
            if stream is not None:
                stream.close()

    def launch(self, mode):
        process = subprocess.Popen(
            [WORKER_FIXTURE, mode],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            bufsize=0,
        )
        self.addCleanup(self.cleanup, process)
        return process

    def test_negative_control_libc_exit_enters_wedged_teardown(self):
        process = self.launch("libc-exit")
        self.assertEqual(self.line(process.stderr), "atexit-entered")
        with self.assertRaises(subprocess.TimeoutExpired):
            process.wait(timeout=0.1)

    def test_rpc_parent_eof_exits_despite_wedged_teardown(self):
        process = self.launch("owner-loss")
        self.assertEqual(self.line(process.stderr), "worker-ready")
        process.stdin.close()
        self.assertEqual(process.wait(timeout=5), 0)
        self.assertNotIn(b"atexit-entered", process.stderr.read())

    def test_rpc_invalid_frame_exits_despite_wedged_teardown(self):
        process = self.launch("owner-loss")
        self.assertEqual(self.line(process.stderr), "worker-ready")
        process.stdin.write(b"invalid-frame!!!!")
        self.assertEqual(process.wait(timeout=5), 0)
        self.assertNotIn(b"atexit-entered", process.stderr.read())

    def test_resource_owner_loss_exits_despite_wedged_teardown(self):
        process = self.launch("resource-failure")
        self.assertEqual(process.wait(timeout=5), 1)
        self.assertNotIn(b"atexit-entered", process.stderr.read())

    def test_invalid_heap_credit_reply_exits_without_libc_teardown(self):
        process = self.launch("heap-invalid-reply")

        def read_exact(count):
            result = b""
            while len(result) < count:
                self.assertTrue(
                    select.select([process.stdout], [], [], 5)[0],
                    "heap request timeout",
                )
                piece = os.read(process.stdout.fileno(), count - len(result))
                self.assertTrue(piece, "worker closed before heap request")
                result += piece
            return result

        magic, kind, request_id, length = struct.unpack("<4sBQI", read_exact(17))
        self.assertEqual((magic, kind), (b"AFW6", 10))
        self.assertLessEqual(length, 1024)
        request = json.loads(read_exact(length))
        self.assertEqual(request["operation"], "transient_host")
        self.assertEqual(json.loads(request["data"])["next"], 1024)
        # This acknowledges success with an impossible nonzero value. A child
        # cannot continue or return credit based on an uncertain owner reply.
        process.stdin.write(
            struct.pack("<4sBQIiiQ", b"AFW6", 11, request_id, 16, 0, 0, 7)
        )
        self.assertEqual(process.wait(timeout=5), 1)
        self.assertNotIn(b"atexit-entered", process.stderr.read())

    def test_denied_prompt_observation_exits_without_libc_teardown(self):
        process = self.launch("prompt-observation-denied")
        marker = self.line(process.stderr).split()
        self.assertEqual(marker[0], "denial")
        code, detail = map(int, marker[1:])
        self.assertNotEqual(code, 0)

        def read_exact(count):
            result = b""
            while len(result) < count:
                self.assertTrue(
                    select.select([process.stdout], [], [], 5)[0],
                    "observation request timeout",
                )
                piece = os.read(process.stdout.fileno(), count - len(result))
                self.assertTrue(piece, "worker closed before observation request")
                result += piece
            return result

        magic, kind, request_id, length = struct.unpack("<4sBQI", read_exact(17))
        self.assertEqual((magic, kind), (b"AFW6", 10))
        self.assertLessEqual(length, 1024)
        request = json.loads(read_exact(length))
        self.assertEqual(request["operation"], "prompt_cache")
        self.assertEqual(
            json.loads(request["data"]), {"key": 1, "previous": 0, "next": 64}
        )
        process.stdin.write(
            struct.pack("<4sBQIiiQ", b"AFW6", 11, request_id, 16, code, detail, 0)
        )
        self.assertEqual(process.wait(timeout=5), 1)
        self.assertNotIn(b"atexit-entered", process.stderr.read())

    def test_parent_sigkill_releases_worker_lease_without_libc_teardown(self):
        with tempfile.TemporaryDirectory(dir=CACHE) as directory:
            process = subprocess.Popen(
                [LEASE_FIXTURE, "parent-lifetime", directory, WORKER_FIXTURE],
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                bufsize=0,
            )
            child_pid = None
            try:
                pid_line = self.line(process.stdout)
                self.assertTrue(pid_line.startswith("child "), pid_line)
                child_pid = int(pid_line.split()[1])
                self.assertEqual(self.line(process.stderr), "worker-ready")

                def probe():
                    result = subprocess.run(
                        [LEASE_FIXTURE, "probe", directory],
                        timeout=5,
                        capture_output=True,
                    )
                    self.assertIn(result.returncode, (0, 17), result.stderr.decode())
                    return result.returncode

                self.assertEqual(probe(), 17)
                process.kill()
                self.assertEqual(process.wait(timeout=5), -signal.SIGKILL)
                deadline = time.monotonic() + 5
                while probe() == 17:
                    self.assertLess(
                        time.monotonic(),
                        deadline,
                        "worker retained lease after parent loss",
                    )
                    time.sleep(0.02)
                # The inherited lock's release establishes the generation fence.
                # No PID-existence or timeout observation grants replacement.
                child_pid = None
                self.assertNotIn(b"atexit-entered", process.stderr.read())
            finally:
                if child_pid is not None:
                    try:
                        os.kill(child_pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                self.cleanup(process)


if __name__ == "__main__":
    unittest.main(verbosity=2)
