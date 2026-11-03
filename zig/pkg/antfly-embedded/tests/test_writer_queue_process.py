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

"""Real kernel ownership, process death, ordering, and legacy flock coverage."""

import fcntl
import os
from pathlib import Path
import select
import subprocess
import sys
import tempfile
import unittest


FIXTURE = os.path.abspath(sys.argv.pop(1))


class WriterQueueTests(unittest.TestCase):
    def setUp(self):
        parent = Path.cwd() / ".zig-cache" / "tmp"
        parent.mkdir(parents=True, exist_ok=True)
        temporary = tempfile.TemporaryDirectory(prefix="writer-queue-", dir=parent)
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name).resolve()
        self.path = self.directory / "database.lock"
        self.path.touch()

    @staticmethod
    def stop(process):
        if process.poll() is None:
            process.kill()
        process.communicate(timeout=10)

    def start(self, kind="write", *, rounds=1, timeout=5000):
        process = subprocess.Popen(
            [FIXTURE, str(self.path), kind, str(timeout), str(rounds)],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            bufsize=0,
        )
        self.addCleanup(self.stop, process)
        self.event(process, "enrolled")
        return process

    def event(self, process, expected, round=0, timeout=5):
        ready, _, _ = select.select([process.stdout], [], [], timeout)
        self.assertTrue(ready, f"missing {expected}; exit={process.poll()}")
        line = process.stdout.readline()
        self.assertEqual(line, f"{expected} {round}\n".encode())

    def waiting(self, process):
        self.assertIsNone(process.poll())
        ready, _, _ = select.select([process.stdout], [], [], 0.05)
        self.assertFalse(ready, "a successor proceeded before its predecessor")

    def release(self, process, round=0):
        process.stdin.write(b"r")
        process.stdin.flush()
        self.event(process, "released", round)

    def finished(self, process):
        self.assertEqual(process.wait(timeout=5), 0)

    def test_fifo_prevents_immediate_holder_reentry(self):
        first = self.start(rounds=2)
        self.event(first, "acquired")
        second = self.start()
        third = self.start()
        self.release(first)
        self.event(first, "enrolled", 1)
        self.event(second, "acquired")
        self.waiting(first)
        self.waiting(third)
        self.release(second)
        self.event(third, "acquired")
        self.waiting(first)
        self.release(third)
        self.event(first, "acquired", 1)
        self.release(first, 1)
        for process in [first, second, third]:
            self.finished(process)

    def test_killed_middle_waiter_does_not_release_the_live_head(self):
        first = self.start()
        self.event(first, "acquired")
        middle = self.start()
        last = self.start()
        middle.kill()
        middle.wait(timeout=5)
        self.waiting(last)
        self.release(first)
        self.event(last, "acquired")
        self.release(last)
        self.finished(first)
        self.finished(last)

    def test_killed_holders_release_each_successor_in_order(self):
        first = self.start()
        self.event(first, "acquired")
        second = self.start()
        third = self.start()
        first.kill()
        first.wait(timeout=5)
        self.event(second, "acquired")
        self.waiting(third)
        second.kill()
        second.wait(timeout=5)
        self.event(third, "acquired")
        self.release(third)
        self.finished(third)

    def test_expired_middle_waiter_cannot_let_its_successor_cut_in(self):
        first = self.start()
        self.event(first, "acquired")
        middle = self.start(timeout=100)
        last = self.start()
        self.event(middle, "busy")
        self.assertEqual(middle.wait(timeout=5), 75)
        self.waiting(last)
        self.release(first)
        self.event(last, "acquired")
        self.release(last)
        self.finished(first)
        self.finished(last)
        queues = list(self.directory.glob(".aflite-writer-queue-v1-*.lock"))
        self.assertEqual(len(queues), 1)
        self.assertEqual(queues[0].stat().st_size, 64 + 1024 * 8)

    def test_reader_cohort_does_not_starve_an_intervening_writer(self):
        first = self.start("read")
        self.event(first, "acquired")
        second = self.start("read")
        self.event(second, "acquired")
        writer = self.start()
        late_reader = self.start("read")
        self.waiting(writer)
        self.waiting(late_reader)
        self.release(second)
        self.waiting(writer)
        self.release(first)
        self.event(writer, "acquired")
        self.waiting(late_reader)
        self.release(writer)
        self.event(late_reader, "acquired")
        self.release(late_reader)
        for process in [first, second, writer, late_reader]:
            self.finished(process)

    def test_legacy_holder_remains_authoritative_after_queued_head_dies(self):
        with self.path.open("r+b") as legacy:
            fcntl.flock(legacy, fcntl.LOCK_EX)
            first = self.start()
            second = self.start()
            first.kill()
            first.wait(timeout=5)
            self.waiting(second)
            fcntl.flock(legacy, fcntl.LOCK_UN)
            self.event(second, "acquired")
            self.release(second)
            self.finished(second)


if __name__ == "__main__":
    unittest.main()
