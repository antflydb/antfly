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

"""Partition serial inference tests by model family without omitting new files.

Collected tests follow their primary model affinity, including compound tests
that use a decoder from a speech endpoint. Secondary models can remain resident
under the existing byte budgets. Forced eviction uses a separate server lifetime.
Each lane is serial; CI admits at most two full-suite lanes concurrently.
"""

import json
import os
from pathlib import Path
import sys

GENERATION = {
    "test_chat.py",
    "test_generate.py",
    "test_openai_compat.py",
    "test_rewrite.py",
    "test_cuda_gemma4_l4.py",
}
SPEECH = {"test_dictate.py", "test_transcribe.py", "test_transcription_sessions.py"}

MODEL_LANES = {
    "generator": "generation",
    "reader": "embedding",
    "eviction": "embedding",
}


def item_affinity(item):
    marker = item.get_closest_marker("inference_model")
    if marker is not None:
        model = marker.args[0]
        if model not in MODEL_LANES:
            raise ValueError(f"unknown inference model affinity: {model}")
        return MODEL_LANES[model], model
    name = Path(item.path).name
    if name in GENERATION:
        return "generation", "generator"
    if name in SPEECH:
        return "speech", "speech"
    if name == "test_read.py":
        return "embedding", "reader"
    return "embedding", name


class ModelAffinity:
    def __init__(self, shard):
        self.shard = shard

    # Run after pytest's fixture grouping so the deliberate one-model server
    # executes last, after the ordinary reusable server has been torn down.
    def pytest_collection_modifyitems(self, config, items):
        selected, deselected = [], []
        for item in items:
            (selected if item_affinity(item)[0] == self.shard else deselected).append(
                item
            )
        config.hook.pytest_deselected(items=deselected)
        selected.sort(
            key=lambda item: (
                item_affinity(item)[1] == "eviction",
                item_affinity(item)[1],
                str(item.path),
            )
        )
        items[:] = selected


def partition(paths: list[Path]) -> dict[str, list[Path]]:
    groups: dict[str, list[Path]] = {"generation": [], "speech": [], "embedding": []}
    for path in sorted(paths):
        group = (
            "generation"
            if path.name in GENERATION
            else "speech"
            if path.name in SPEECH
            else "embedding"
        )
        groups[group].append(path)
    return groups


class Timings:
    """Flush every completed phase so timeouts still leave useful evidence."""

    def __init__(self, path: Path):
        self.path = path

    def pytest_runtest_logreport(self, report):
        with self.path.open("a") as output:
            output.write(
                json.dumps(
                    {
                        "nodeid": report.nodeid,
                        "phase": report.when,
                        "seconds": report.duration,
                        "outcome": report.outcome,
                    }
                )
                + "\n"
            )


def affinity_plugin(shard):
    import pytest

    class Plugin(ModelAffinity):
        @pytest.hookimpl(trylast=True)
        def pytest_collection_modifyitems(self, config, items):
            super().pytest_collection_modifyitems(config, items)

    return Plugin(shard)


def discover_tests(root: Path) -> list[Path]:
    result = []
    for directory, dirs, files in os.walk(root):
        dirs[:] = [d for d in dirs if not d.startswith(".") and d != "__pycache__"]
        result.extend(
            Path(directory) / name
            for name in files
            if name.startswith("test_") and name.endswith(".py")
        )
    return result


def configure_shard_environment(environ):
    # PR CI invokes this checked-out command through a trusted main workflow.
    # Keep reuse policy here too, so a PR can qualify it before the workflow is
    # merged. Count-only caps remain available in direct pytest invocations.
    budgets = (
        "ANTFLY_INFERENCE_PROCESS_MEMORY_BUDGET_MB",
        "ANTFLY_INFERENCE_HOST_BUDGET_MB",
    )
    if all(int(environ.get(name, "0")) > 0 for name in budgets):
        environ["ANTFLY_INFERENCE_MAX_LOADED_MODELS"] = "0"
    if environ.get("CI") == "true" and environ.get("HF_HOME"):
        environ.setdefault(
            "ANTFLY_INFERENCE_MODELS_DIR",
            str(Path(environ["HF_HOME"]).parent / "inference-models-full-v1"),
        )


def main() -> None:
    import pytest

    if len(sys.argv) != 2 or sys.argv[1] not in {"generation", "speech", "embedding"}:
        raise SystemExit("usage: inference_e2e_shards.py generation|speech|embedding")
    configure_shard_environment(os.environ)
    root = Path(__file__).resolve().parents[2]
    # Collect all items before assigning them. A newly marked compound test
    # must not disappear because its file's default affinity differs.
    paths = discover_tests(root / "zig/e2e/inference")
    if not paths:
        raise SystemExit("inference shard is empty")
    report = Path(
        os.environ.get(
            "ANTFLY_INFERENCE_REPORT_DIR", root / ".local-test-results/inference"
        )
    )
    report.mkdir(parents=True, exist_ok=True)
    raise SystemExit(
        pytest.main(
            [
                "-v",
                "--durations=0",
                "--durations-min=1",
                "--tb=short",
                f"--junitxml={report / (sys.argv[1] + '.xml')}",
                *[str(path) for path in paths],
            ],
            plugins=[
                affinity_plugin(sys.argv[1]),
                Timings(report / (sys.argv[1] + ".jsonl")),
            ],
        )
    )


if __name__ == "__main__":
    main()
