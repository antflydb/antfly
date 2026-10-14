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

"""Transient recovery keeps the original construction deadline and admission errors."""

import importlib.util
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location(
    "hackernews_poc", Path(__file__).resolve().parents[1] / "poc.py"
)
poc = importlib.util.module_from_spec(spec)
spec.loader.exec_module(poc)


class ConstructionRecoveryTests(unittest.TestCase):
    def run_sequence(self, statuses, deadline=10):
        clock = [0]
        observed = {}
        polls = []

        def read():
            polls.append(clock[0])
            return statuses[min(len(polls) - 1, len(statuses) - 1)]

        def sleep(seconds):
            clock[0] += seconds

        result = poc.wait_for_index(
            read, deadline, lambda: None, observed, now=lambda: clock[0], sleep=sleep
        )
        return result, observed, polls

    def test_flat_source_is_staged_without_mutating_input(self):
        import tempfile

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source, state = root / "input", root / "state"
            (source / "archive").mkdir(parents=True)
            parquet = source / "archive" / "part.parquet"
            parquet.write_bytes(b"unchanged input")
            staged = poc.prepare_filesystem_source(source, state)
            target = (
                staged / "archive" / "buckets" / "antfly" / "objects" / parquet.name
            )
            self.assertEqual(target.read_bytes()[:8], b"AFOBJ001")
            self.assertEqual(target.read_bytes()[84:], parquet.read_bytes())
            self.assertFalse((source / "archive" / "buckets").exists())
            self.assertEqual(poc.prepare_filesystem_source(source, state), staged)
            parquet.write_bytes(b"changed input")
            with self.assertRaisesRegex(ValueError, "Staged source changed"):
                poc.prepare_filesystem_source(source, state)

    def test_timeout_recovers_under_original_deadline(self):
        failed = {"status": {"readiness": {"state": "failed"}, "error": "Timeout"}}
        pending = {"status": {"readiness": {"state": "pending"}}}
        ready = {"status": {"readiness": {"queryable": True}}}
        result, observed, polls = self.run_sequence([failed, failed, pending, ready])
        self.assertEqual(result, ready)
        self.assertEqual(polls, [0, 1, 2, 3])
        self.assertEqual(observed["transient_failures"]["Timeout"]["observations"], 2)

    def test_repeated_timeout_does_not_extend_deadline(self):
        failed = {
            "status": {"readiness": {"state": "failed"}, "error": "DeadlineExceeded"}
        }
        with self.assertRaisesRegex(RuntimeError, "build deadline"):
            self.run_sequence([failed], deadline=3)

    def test_late_readiness_does_not_extend_deadline(self):
        clock = [0]

        def read():
            clock[0] = 4
            return {"status": {"readiness": {"queryable": True}}}

        with self.assertRaisesRegex(RuntimeError, "build deadline"):
            poc.wait_for_index(
                read, 3, lambda: None, {}, now=lambda: clock[0], sleep=lambda _: None
            )

    def test_permanent_admission_fails_immediately(self):
        failed = {
            "status": {
                "readiness": {"state": "failed"},
                "error": "SqlProgramLimitExceeded",
            }
        }
        with self.assertRaisesRegex(RuntimeError, "Index construction failed"):
            self.run_sequence([failed])


if __name__ == "__main__":
    unittest.main()
