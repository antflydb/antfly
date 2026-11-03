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
