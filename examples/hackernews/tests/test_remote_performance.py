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

import argparse
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location(
    "hn_remote_performance",
    Path(__file__).resolve().parents[1] / "remote_performance.py",
)
matrix = importlib.util.module_from_spec(spec)
spec.loader.exec_module(matrix)


class RemotePerformanceTests(unittest.TestCase):
    def test_matrix_isolates_provider_source_storage_and_publication_identity(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "archive").mkdir()
            (root / "archive" / "sample.parquet").write_bytes(b"fixture")
            binary = root / "binary"
            binary.write_bytes(b"engine")
            args = argparse.Namespace(
                binary=["before=" + str(binary), "after=" + str(binary)],
                filesystem_source=root,
                state=root / "states",
                providers=["local", "gcs"],
                cycles=2,
                port=18977,
                expected_rows=10000,
                repeats=20,
                memory_mb=4096,
                current_rows=128,
                diagnose_warm=False,
                project="dev",
                bucket="bucket",
                gcs_source_prefix="sample/source",
                gcs_artifact_prefix="sample/artifacts",
            )
            cells = matrix.plan(args)
            self.assertEqual(len(cells), 16)
            self.assertEqual(
                set(cells[0]["harness_sha256"]), {"poc.py", "remote_performance.py"}
            )
            self.assertEqual(len({cell["state"] for cell in cells}), 16)
            self.assertEqual(
                [cell["name"] for cell in cells[:2]],
                ["before-local-stored-1-1", "after-local-stored-1-1"],
            )
            self.assertEqual(
                [cell["name"] for cell in cells[8:10]],
                ["after-local-stored-1-2", "before-local-stored-1-2"],
            )
            for cell in cells:
                self.assertIn(
                    "--store-source" if cell["store_source"] else "--no-store-source",
                    cell["command"],
                )
                self.assertEqual(
                    cell["fixture"][0]["sha256"],
                    matrix.fingerprint(root / "archive" / "sample.parquet"),
                )
                self.assertNotIn("--profile", cell["command"])
            args.gcs_artifact_prefix = "sample/source/indices"
            with self.assertRaisesRegex(ValueError, "disjoint"):
                matrix.plan(args)
            args.providers = ["local"]
            args.binary.append(args.binary[0])
            with self.assertRaisesRegex(ValueError, "Duplicate"):
                matrix.plan(args)

    def test_changed_harness_never_launches_a_resume_child(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            cell = {
                "name": "changed",
                "state": str(root / "changed"),
                "harness_sha256": {"poc.py": "wrong-digest"},
            }
            with patch.object(matrix.subprocess, "Popen") as launch:
                with self.assertRaisesRegex(
                    ValueError, "Harness changed after planning"
                ):
                    matrix.execute([cell], root)
                launch.assert_not_called()
            self.assertFalse((root / "matrix.lock").exists())

    def test_passed_cell_rejects_a_binary_changed_after_planning(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binary = root / "binary"
            binary.write_bytes(b"old")
            state = root / "cached"
            state.mkdir()
            (state / "matrix-result.json").write_text(
                json.dumps(
                    {
                        "status": "passed",
                        "measurements": {"top_hits": []},
                    }
                )
            )
            cell = {
                "name": "cached",
                "state": str(state),
                "harness_sha256": {},
                "fixture": [],
                "command": ["poc.py", "--binary", str(binary)],
                "binary_sha256": matrix.fingerprint(binary),
            }
            binary.write_bytes(b"new")
            with patch.object(matrix.subprocess, "Popen") as launch:
                with self.assertRaisesRegex(
                    ValueError, "Binary changed after planning"
                ):
                    matrix.execute([cell], root)
                launch.assert_not_called()
            self.assertFalse((root / "matrix.lock").exists())

    def test_distribution_keeps_a_first_repeat_outlier(self):
        samples = [3926.98, 11.7, 9.59, 7.46, 6.61, 6.36, 6.22, 5.97, 6.88, 6.51]
        result = matrix.distribution(samples)
        self.assertEqual(result["samples_ms"], samples)
        self.assertEqual(result["median_ms"], 6.745)
        self.assertEqual(result["p95_ms"], 3926.98)
        self.assertEqual(result["max_ms"], 3926.98)

    def test_incomplete_cells_cannot_relabel_resumed_build_time(self):
        for status in ("failed", "interrupted", "running", None):
            with (
                self.subTest(status=status),
                tempfile.TemporaryDirectory() as directory,
            ):
                root = Path(directory)
                binary = root / "binary"
                binary.write_bytes(b"engine")
                state = root / "attempt"
                state.mkdir()
                (state / "data").mkdir()
                if status is not None:
                    (state / "matrix-result.json").write_text(
                        json.dumps({"status": status})
                    )
                cell = {
                    "name": "attempt",
                    "state": str(state),
                    "harness_sha256": {},
                    "fixture": [],
                    "command": ["poc.py", "--binary", str(binary)],
                    "binary_sha256": matrix.fingerprint(binary),
                }
                with patch.object(matrix.subprocess, "Popen") as launch:
                    with self.assertRaisesRegex(
                        ValueError, "requires fresh state|Unmeasured existing"
                    ):
                        matrix.execute([cell], root)
                    launch.assert_not_called()
                self.assertTrue((state / "data").exists())
                self.assertFalse((root / "matrix.lock").exists())

    def test_failed_extension_invalidates_summary_and_successful_resume_rebinds_plan(
        self,
    ):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binary = root / "binary"
            binary.write_bytes(b"engine")

            def cell(name):
                return {
                    "name": name,
                    "state": str(root / name),
                    "harness_sha256": {},
                    "fixture": [],
                    "command": ["poc.py", "--binary", str(binary)],
                    "binary_sha256": matrix.fingerprint(binary),
                }

            local, gcs = cell("local"), cell("gcs")

            def passed(recipe):
                state = Path(recipe["state"])
                state.mkdir(exist_ok=True)
                (state / "matrix-cell.json").write_text(json.dumps(recipe))
                (state / "matrix-result.json").write_text(
                    json.dumps({"status": "passed", "measurements": {"top_hits": []}})
                )

            passed(local)
            summary = root / "matrix-summary.json"
            with patch.object(matrix.subprocess, "Popen") as launch:
                matrix.execute([local], root)
                original = json.loads(summary.read_text())
                self.assertTrue(original["complete"])
                launch.assert_not_called()
                launch.return_value.wait.return_value = 1
                with self.assertRaisesRegex(RuntimeError, "Qualification failed"):
                    matrix.execute([local, gcs], root)
                launch.assert_called_once()
                failed = json.loads(summary.read_text())
                self.assertFalse(failed["complete"])
                self.assertEqual(failed["planned_cells"], ["local", "gcs"])
                self.assertNotEqual(failed["plan_sha256"], original["plan_sha256"])
                self.assertEqual(
                    json.loads(
                        (Path(local["state"]) / "matrix-result.json").read_text()
                    )["status"],
                    "passed",
                )
            # A new, fully qualified cell report can finish the same plan;
            # deriving its summary never launches either completed cell again.
            passed(gcs)
            with patch.object(matrix.subprocess, "Popen") as launch:
                matrix.execute([local, gcs], root)
                launch.assert_not_called()
            resumed = json.loads(summary.read_text())
            self.assertTrue(resumed["complete"])
            self.assertEqual(resumed["plan_sha256"], failed["plan_sha256"])
            self.assertEqual([c["cell"] for c in resumed["cells"]], ["local", "gcs"])

    def test_validation_failure_invalidates_previous_summary(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            summary = root / "matrix-summary.json"
            summary.write_text(json.dumps({"complete": True}))
            cell = {
                "name": "changed",
                "state": str(root / "changed"),
                "harness_sha256": {"poc.py": "wrong-digest"},
            }
            with patch.object(matrix.subprocess, "Popen") as launch:
                with self.assertRaisesRegex(
                    ValueError, "Harness changed after planning"
                ):
                    matrix.execute([cell], root)
                launch.assert_not_called()
            self.assertFalse(json.loads(summary.read_text())["complete"])
            self.assertFalse((root / "matrix.lock").exists())

    def test_completed_cells_resume_without_launching_again(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binary = root / "binary"
            binary.write_bytes(b"engine")
            state = root / "passed"
            state.mkdir()
            (state / "data").mkdir()
            (state / "matrix-result.json").write_text(
                json.dumps({"status": "passed", "measurements": {"top_hits": []}})
            )
            cell = {
                "name": "passed",
                "state": str(state),
                "harness_sha256": {},
                "fixture": [],
                "command": ["poc.py", "--binary", str(binary)],
                "binary_sha256": matrix.fingerprint(binary),
            }
            with patch.object(matrix.subprocess, "Popen") as launch:
                matrix.execute([cell], root)
                launch.assert_not_called()
            self.assertTrue(
                json.loads((root / "matrix-summary.json").read_text())["complete"]
            )
