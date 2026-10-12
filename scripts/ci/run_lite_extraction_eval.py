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

"""Run the public extraction evaluator against a pinned real model in CI.

Produces the complete precision/recall report for the CI artifact. This curated
regression set is a quality gate, not a representative production benchmark.
"""

import argparse
import json
from pathlib import Path
import subprocess
import sys


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--models-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[2]
    fixture = root / "scripts/testdata/eval_extract"
    subprocess.run(
        [
            sys.executable,
            str(root / "scripts/eval_extract.py"),
            "--models-dir",
            str(args.models_dir),
            "--corpus",
            str(fixture / "regression.jsonl"),
            "--config",
            str(fixture / "plain.json"),
            "--config",
            str(fixture / "descriptions.json"),
            "--output",
            str(args.output),
        ],
        check=True,
    )
    report = json.loads(args.output.read_text())
    if report["documents"] != 23 or len(report["runs"]) != 2:
        raise RuntimeError("incomplete real-model evaluation report")
    for run in report["runs"]:
        micro = run["metrics"]["micro"]
        for name in ("precision", "recall"):
            if micro[name] is None or micro[name] < 0.5:
                raise RuntimeError(
                    f"real extractor {name} below regression floor: {micro}"
                )
        for label in ("person", "location"):
            if run["metrics"]["per_label"][label]["tp"] == 0:
                raise RuntimeError(f"real extractor returned no correct {label} spans")
        print(json.dumps({"config": run["config_path"], "metrics": run["metrics"]}))


if __name__ == "__main__":
    main()
