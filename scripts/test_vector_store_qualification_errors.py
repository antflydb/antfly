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
import tempfile
import unittest
from pathlib import Path

from vector_store_qualification_errors import inspect_workload_errors
from run_vector_store_ab import verify_50k_gate


class WorkloadErrorTests(unittest.TestCase):
    def test_successful_client_retry_is_not_clean_qualification(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "vdbbench-live.log").write_text(
                "WARNING: Antfly insert error: Server error '500 Internal Server Error'\n"
                "WARNING: Insert failed, try_idx=0\nSuccess to finish task\n"
            )
            (root / "antfly-initial.log").write_text(
                "error: public table batch failed table=vdbbench err=error.OutOfMemory\n"
                "warning: maintenance failed: error.VectorPayloadStorePoisoned\n"
            )
            result = inspect_workload_errors(root)
            self.assertFalse(result["qualified"])
            self.assertEqual(result["counts"]["client_insert_retry"], 1)
            self.assertEqual(result["counts"]["source_store_poisoned"], 1)

    def test_startup_progress_is_not_a_workload_error(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "vdbbench-live.log").write_text(
                'status {"error_count":0}\nSuccess to finish task\n'
            )
            (root / "antfly-initial.log").write_text(
                "info: dense posting checkpoint published\n"
            )
            self.assertTrue(inspect_workload_errors(root)["qualified"])
            (root / "antfly-restart.log").write_text(
                "error: query failed err=OutOfMemory\n"
            )
            self.assertFalse(inspect_workload_errors(root)["qualified"])
            (root / "antfly-restart.log").write_text(
                "warning: source collection cleanup deferred: OutOfMemory\n"
            )
            self.assertFalse(inspect_workload_errors(root)["qualified"])

    def test_framework_query_failures_reject_an_otherwise_successful_run(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "vdbbench-live.log").write_text("Success to finish task\n")
            (root / "antfly-initial.log").write_text("info: ready\n")
            (root / "vdbbench-framework.log").write_text(
                "VectorDB search_embedding error: 500 Internal Server Error\n"
            )
            result = inspect_workload_errors(root)
            self.assertFalse(result["qualified"])
            self.assertEqual(result["counts"]["client_query_error"], 1)
            (root / "vdbbench-framework.log").write_text("")
            (root / "antfly-reopened.log").write_text(
                "warning: public table query read failed table=test err=error.ResourceBudgetExceeded\n"
            )
            self.assertFalse(inspect_workload_errors(root)["qualified"])

    def test_missing_workload_logs_fail_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            result = inspect_workload_errors(Path(directory))
            self.assertFalse(result["qualified"])
            self.assertEqual(len(result["missing_logs"]), 2)

    def test_scale_gate_rechecks_old_successful_receipts(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            arms = [
                {
                    "case": "Performance1536D50K",
                    "pair": pair,
                    "mode": mode,
                    "exit_code": 0,
                    "binary_sha256": "pinned",
                    "refinement": None,
                }
                for pair, mode in [
                    (1, "primary_lsm"),
                    (1, "vector_store"),
                    (2, "vector_store"),
                    (2, "primary_lsm"),
                ]
            ]
            (root / "ab-runs.json").write_text(json.dumps(arms))
            arm = root / "Performance1536D50K-1-primary_lsm"
            arm.mkdir()
            (arm / "vdbbench-live.log").write_text(
                "Insert failed, try_idx=0\nSuccess to finish task\n"
            )
            (arm / "antfly-initial.log").write_text("info: ready\n")
            with self.assertRaisesRegex(ValueError, "logs contain errors/retries"):
                verify_50k_gate(root, "pinned", None, 2, {}, {})


if __name__ == "__main__":
    unittest.main()
