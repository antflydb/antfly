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
import tempfile
import queue
import threading
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch
from pathlib import Path

import lite_sqlite_comparison as bench


class MatchedDurabilityTests(unittest.TestCase):
    def test_worker_retains_acknowledgements_before_a_later_visibility_failure(self):
        class Database:
            calls = 0
            closed = False

            def batch_json(self, request):
                self.calls += 1
                if self.calls == 3:
                    raise RuntimeError("committed visibility pending")

            def close(self):
                self.closed = True

        db = Database()
        sdk = SimpleNamespace(
            open=lambda *args, **kwargs: db, BusyError=BlockingIOError
        )
        start = threading.Event()
        start.set()
        ready, results = queue.Queue(), queue.Queue()
        stop = threading.Event()
        with patch.object(bench, "sdk", return_value=sdk):
            bench.worker_main(
                {
                    "engine": "antfly",
                    "path": "unused",
                    "library": "unused",
                    "indexed": True,
                    "seconds": 10,
                },
                0,
                ready,
                start,
                SimpleNamespace(value=time.monotonic()),
                stop,
                results,
            )
        result = results.get_nowait()
        self.assertEqual(result["successful_calls"], 2)
        self.assertEqual(len(result["latencies_ns"]), 2)
        self.assertIn("committed visibility pending", result["error"])
        self.assertTrue(stop.is_set())
        self.assertTrue(db.closed)

    def test_sqlite_document_and_index_rollback_together_and_verify_overwrites(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "sqlite.db"
            bench.bootstrap("sqlite", path, True, None, 1)
            with contextlib.closing(bench.sqlite_open(path)) as db:
                for sequence in range(130):
                    db.execute("BEGIN IMMEDIATE")
                    db.execute(
                        bench.UPSERT,
                        (
                            bench.key(0, sequence),
                            bench.encoded(bench.payload(0, sequence)),
                        ),
                    )
                    db.execute("COMMIT")
                db.execute("BEGIN IMMEDIATE")
                db.execute(
                    bench.UPSERT,
                    (bench.key(0, 130), bench.encoded(bench.payload(0, 130))),
                )
                db.execute("ROLLBACK")
                self.assertEqual(
                    db.execute(
                        "SELECT count(*) FROM search WHERE search MATCH 'matchedworker0sequence130'"
                    ).fetchone()[0],
                    0,
                )
            result = bench.verify(
                "sqlite", path, True, None, [{"worker": 0, "successful_calls": 130}]
            )
            self.assertEqual(result["primary_rows_checked"], 128)
            self.assertTrue(result["latest_and_overwritten_tokens_checked"])

    def test_verifier_rejects_missing_latest_values_even_with_valid_integrity(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "sqlite.db"
            bench.bootstrap("sqlite", path, False, None, 1)
            with self.assertRaises(AssertionError):
                bench.verify(
                    "sqlite", path, False, None, [{"worker": 0, "successful_calls": 1}]
                )


if __name__ == "__main__":
    unittest.main()
