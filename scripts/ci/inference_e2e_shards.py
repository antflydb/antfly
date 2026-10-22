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

Generation stays together to reuse its expensive decoder/projector. Speech has
its own model lifetime. Lightweight/embedding tests own the remaining files.
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
    "test_read.py",
    "test_cuda_gemma4_l4.py",
}
SPEECH = {"test_dictate.py", "test_transcribe.py", "test_transcription_sessions.py"}


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


def main() -> None:
    import pytest

    if len(sys.argv) != 2 or sys.argv[1] not in {"generation", "speech", "embedding"}:
        raise SystemExit("usage: inference_e2e_shards.py generation|speech|embedding")
    root = Path(__file__).resolve().parents[2]
    paths = partition(discover_tests(root / "zig/e2e/inference"))[sys.argv[1]]
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
            plugins=[Timings(report / (sys.argv[1] + ".jsonl"))],
        )
    )


if __name__ == "__main__":
    main()
