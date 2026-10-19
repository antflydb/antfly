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

from pathlib import Path
import unittest
from inference_e2e_shards import partition, discover_tests, Timings


class ShardTests(unittest.TestCase):
    def test_each_file_runs_exactly_once_and_new_files_are_admitted(self):
        files = discover_tests(
            Path(__file__).resolve().parents[2] / "zig/e2e/inference"
        )
        files.append(Path("test_new_provider.py"))
        groups = partition(files)
        assigned = [p for group in groups.values() for p in group]
        self.assertCountEqual(files, assigned)
        self.assertEqual(len(assigned), len(set(assigned)))
        self.assertIn(Path("test_new_provider.py"), groups["embedding"])
        for group in groups.values():
            self.assertTrue(group)

    def test_shared_decoder_endpoints_remain_together(self):
        files = [
            Path(p)
            for p in (
                "test_chat.py",
                "test_generate.py",
                "test_openai_compat.py",
                "test_rewrite.py",
                "test_read.py",
                "test_dictate.py",
                "test_transcribe.py",
            )
        ]
        groups = partition(files)
        self.assertEqual(len(groups["generation"]), 5)
        self.assertEqual(len(groups["speech"]), 2)

    def test_nested_files_are_included_but_virtualenvs_are_not(self):
        import tempfile

        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            for name in (
                "nested/test_new.py",
                ".venv/test_not_ours.py",
                "__pycache__/test_not_ours.py",
            ):
                path = root / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.touch()
            self.assertEqual(discover_tests(root), [root / "nested/test_new.py"])

    def test_timings_survive_an_incomplete_suite(self):
        import json
        import tempfile
        from types import SimpleNamespace

        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "timings.jsonl"
            Timings(path).pytest_runtest_logreport(
                SimpleNamespace(
                    nodeid="test_model.py::test_first_use",
                    when="setup",
                    duration=12.5,
                    outcome="passed",
                )
            )
            value = json.loads(path.read_text())
            self.assertEqual(value["phase"], "setup")
            self.assertEqual(value["seconds"], 12.5)

    def test_full_lane_artifact_names_cannot_collide(self):
        import re

        workflow = (
            Path(__file__).resolve().parents[2] / ".github/workflows/zig-tests.yml"
        ).read_text()
        lane = workflow.split("  e2e-full-tests:", 1)[1].split("\n  e2e-full:", 1)[0]
        names = []
        for step in lane.split("      - name:"):
            if "uses: actions/upload-artifact@" in step:
                name = re.search(r"^\s+name: (.+)$", step, re.M).group(1)
                self.assertIn("${{ matrix.suite }}", name)
                names.append(name)
        self.assertGreaterEqual(len(names), 3)
