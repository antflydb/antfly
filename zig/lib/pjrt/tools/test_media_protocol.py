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

"""Failure-path checks for the native media benchmark's pipe protocol."""

import subprocess
import sys
import unittest

from benchmark_media_pjrt import read_exact


class MediaProtocolTests(unittest.TestCase):
    def worker(self, source):
        process = subprocess.Popen(
            [sys.executable, "-c", source], stdout=subprocess.PIPE, bufsize=0
        )

        def cleanup():
            if process.poll() is None:
                process.kill()
            process.wait()
            process.stdout.close()

        self.addCleanup(cleanup)
        return process

    def test_fragmented_responses_preserve_frame_boundaries(self):
        process = self.worker(
            "import os,time; os.write(1,b'RE'); time.sleep(.02); "
            "os.write(1,b'ADY\\nnext')"
        )
        self.assertEqual(read_exact(process, 6, timeout=2), b"READY\n")
        self.assertEqual(read_exact(process, 4, timeout=2), b"next")

    def test_truncated_response_fails(self):
        process = self.worker("import os; os.write(1,b'bad')")
        with self.assertRaises((EOFError, RuntimeError)):
            read_exact(process, 16, timeout=2)

    def test_unresponsive_worker_times_out(self):
        process = self.worker("import time; time.sleep(30)")
        with self.assertRaises(TimeoutError):
            read_exact(process, 6, timeout=0.05)


if __name__ == "__main__":
    unittest.main()
