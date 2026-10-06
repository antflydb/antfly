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

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


SCRIPT = Path(__file__).with_name("verify_search_benchmark.py")


def record(ids, scores, total=10):
    return {
        "schema_version": 1,
        "query_grammar": "V1",
        "total_hits": total,
        "relation": "exact",
        "hits": [{"id": doc_id, "score": score} for doc_id, score in zip(ids, scores)],
    }


class VerifySearchBenchmarkTest(unittest.TestCase):
    def run_verifier(self, left, right):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            left_path = root / "left.jsonl"
            right_path = root / "right.jsonl"
            left_path.write_text(json.dumps(left) + "\n", encoding="utf-8")
            right_path.write_text(json.dumps(right) + "\n", encoding="utf-8")
            return subprocess.run(
                [sys.executable, str(SCRIPT), str(left_path), str(right_path)],
                check=False,
                capture_output=True,
                text=True,
            )

    def test_accepts_strict_match_with_float_tolerance(self):
        result = self.run_verifier(
            record([1, 2], [3.0, 2.0]),
            record([1, 2], [3.000001, 2.000001]),
        )
        self.assertEqual(0, result.returncode, result.stdout + result.stderr)

    def test_accepts_different_ids_only_at_tied_cutoff(self):
        result = self.run_verifier(
            record([1, 2, 3], [3.0, 2.0, 2.0]),
            record([1, 2, 4], [3.0, 2.0, 2.0]),
        )
        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
        self.assertEqual(1, json.loads(result.stdout)["tie_aware_matches"])

    def test_rejects_non_cutoff_id_mismatch(self):
        result = self.run_verifier(
            record([1, 2], [3.0, 2.0]),
            record([9, 2], [3.0, 2.0]),
        )
        self.assertEqual(1, result.returncode)

    def test_rejects_count_mismatch(self):
        result = self.run_verifier(
            record([1], [3.0], total=10),
            record([1], [3.0], total=11),
        )
        self.assertEqual(1, result.returncode)


if __name__ == "__main__":
    unittest.main()
