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
import unittest
from pathlib import Path

import lite_sqlite_comparison as bench


class MatchedDurabilityTests(unittest.TestCase):
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

    def test_compiler_processes_returns_a_list_without_raising(self):
        # This process's own ancestry (pytest/uv/the test runner) must never
        # itself look like a compiler to this check, and the function must
        # tolerate whatever `ps -axo stat,comm` reports on the host without
        # raising -- on a shared dev host this is routinely nonzero, which
        # is why run_case()/main() record it instead of asserting it is
        # empty (see compiler_contamination_samples / pre_run_compiler_processes).
        result = bench.compiler_processes()
        self.assertIsInstance(result, list)


if __name__ == "__main__":
    unittest.main()
