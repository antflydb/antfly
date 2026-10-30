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
from inference_e2e_shards import (
    partition,
    discover_tests,
    Timings,
    ModelAffinity,
    item_affinity,
    configure_shard_environment,
)


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
        self.assertEqual(len(groups["generation"]), 4)
        self.assertIn(Path("test_read.py"), groups["embedding"])
        self.assertEqual(len(groups["speech"]), 2)

    def test_compound_cases_follow_model_and_eviction_runs_last(self):
        from types import SimpleNamespace as S
        from unittest.mock import Mock

        def item(file, name, model=None):
            return S(
                path=Path(file),
                nodeid=f"{file}::{name}",
                get_closest_marker=lambda _: S(args=(model,)) if model else None,
            )

        cases = [
            item("test_dictate.py", "raw"),
            item("test_dictate.py", "cleanup", "generator"),
            item("test_chat.py", "chat"),
            item("test_read.py", "read"),
            item("test_models.py", "evict", "eviction"),
            item("test_new.py", "new_decoder", "generator"),
            item("test_new.py", "default"),
        ]
        assignment = []
        for lane in ("generation", "speech", "embedding"):
            selected = list(cases)
            ModelAffinity(lane).pytest_collection_modifyitems(
                S(hook=S(pytest_deselected=Mock())), selected
            )
            assignment.extend(c.nodeid for c in selected)
            self.assertTrue(all(item_affinity(c)[0] == lane for c in selected))
            if lane == "generation":
                self.assertEqual(len(selected), 3)
            if lane == "embedding":
                self.assertEqual(selected[-1].nodeid, "test_models.py::evict")
        self.assertCountEqual(assignment, [c.nodeid for c in cases])

    def test_residency_is_byte_bounded_and_eviction_is_explicit(self):
        root = Path(__file__).resolve().parents[2]
        workflow = (root / ".github/workflows/zig-tests.yml").read_text()
        lane = workflow.split("- name: Run Antfly inference full E2E", 1)[1].split(
            "- name: Validate standalone", 1
        )[0]
        self.assertIn('ANTFLY_INFERENCE_MAX_LOADED_MODELS: "0"', lane)
        self.assertIn('ANTFLY_INFERENCE_PROCESS_MEMORY_BUDGET_MB: "14000"', lane)
        self.assertIn('ANTFLY_INFERENCE_HOST_BUDGET_MB: "12000"', lane)
        self.assertIn(
            "ANTFLY_INFERENCE_MODELS_DIR=$home_cache_root/inference-models-full-v1",
            workflow,
        )
        regression = (root / "zig/e2e/inference/test_models.py").read_text()
        self.assertIn(
            '@pytest.mark.parametrize("base_url", ["eviction"], indirect=True)',
            regression,
        )

    def test_primary_model_marker_is_explicit_and_unknown_roles_fail(self):
        from types import SimpleNamespace as S

        bad = S(
            path=Path("test_new.py"), get_closest_marker=lambda _: S(args=("typo",))
        )
        with self.assertRaisesRegex(ValueError, "unknown inference model affinity"):
            item_affinity(bad)

    def test_reuse_requires_byte_budgets_and_preserves_explicit_model_directory(self):
        env = {"ANTFLY_INFERENCE_MAX_LOADED_MODELS": "1"}
        configure_shard_environment(env)
        self.assertEqual(env["ANTFLY_INFERENCE_MAX_LOADED_MODELS"], "1")
        env.update(
            CI="true",
            HF_HOME="/cache/hf",
            ANTFLY_INFERENCE_PROCESS_MEMORY_BUDGET_MB="14000",
            ANTFLY_INFERENCE_HOST_BUDGET_MB="12000",
        )
        configure_shard_environment(env)
        self.assertEqual(env["ANTFLY_INFERENCE_MAX_LOADED_MODELS"], "0")
        self.assertEqual(
            env["ANTFLY_INFERENCE_MODELS_DIR"], "/cache/inference-models-full-v1"
        )
        env["ANTFLY_INFERENCE_MODELS_DIR"] = "/explicit/models"
        configure_shard_environment(env)
        self.assertEqual(env["ANTFLY_INFERENCE_MODELS_DIR"], "/explicit/models")

    def test_real_pytest_assigns_every_item_once_and_restarts_only_for_eviction(self):
        import importlib.util
        import os
        import subprocess
        import sys
        import tempfile

        if importlib.util.find_spec("pytest") is None:
            self.skipTest("pytest is required for real collection/fixture validation")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "pytest.ini").write_text(
                "[pytest]\nmarkers =\n    inference_model(name): model affinity\n"
            )
            (root / "conftest.py").write_text("""
import os
from pathlib import Path
import pytest

def record(value):
    with Path(os.environ['AFFINITY_TRACE']).open('a') as output:
        output.write(value+'\\n')

@pytest.fixture(scope='session')
def base_url(request):
    policy = getattr(request, 'param', 'normal')
    record('start:'+policy)
    yield policy
    record('stop:'+policy)

@pytest.fixture(scope='session')
def api(base_url):
    return base_url

@pytest.fixture(autouse=True)
def log_case(request):
    record('case:'+request.node.nodeid)
""")
            sources = {
                "test_chat.py": "def test_chat(api): assert api == 'normal'\n",
                "test_dictate.py": "import pytest\ndef test_raw(api): assert api == 'normal'\n@pytest.mark.inference_model('generator')\ndef test_cleanup(api): assert api == 'normal'\n",
                "test_read.py": "def test_read(api): assert api == 'normal'\n",
                "test_new.py": "import pytest\n@pytest.mark.inference_model('generator')\ndef test_new_decoder(api): assert api == 'normal'\ndef test_default(api): assert api == 'normal'\n",
                "test_models.py": "import pytest\n@pytest.mark.inference_model('eviction')\n@pytest.mark.parametrize('base_url',['eviction'],indirect=True)\ndef test_evict(api): assert api == 'eviction'\n",
            }
            for name, source in sources.items():
                (root / name).write_text(source)
            all_cases = []
            for lane in ("generation", "speech", "embedding"):
                trace = root / (lane + ".trace")
                code = (
                    "import pytest; from inference_e2e_shards import affinity_plugin; raise SystemExit(pytest.main(['-q', '.'], plugins=[affinity_plugin("
                    + repr(lane)
                    + ")]))"
                )
                result = subprocess.run(
                    [sys.executable, "-c", code],
                    cwd=root,
                    env={
                        **os.environ,
                        "PYTHONPATH": str(Path(__file__).parent.resolve()),
                        "AFFINITY_TRACE": str(trace),
                    },
                    capture_output=True,
                    text=True,
                    timeout=30,
                )
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                events = trace.read_text().splitlines()
                selected = [event[5:] for event in events if event.startswith("case:")]
                all_cases.extend(selected)
                self.assertEqual(events.count("start:normal"), 1)
                self.assertEqual(events.count("stop:normal"), 1)
                if lane == "embedding":
                    self.assertIn("test_evict", selected[-1])
                    self.assertLess(
                        events.index("stop:normal"), events.index("start:eviction")
                    )
                    self.assertEqual(events.count("start:eviction"), 1)
                else:
                    self.assertNotIn("start:eviction", events)
            self.assertEqual(len(all_cases), 7)
            self.assertEqual(len(set(all_cases)), 7)

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
