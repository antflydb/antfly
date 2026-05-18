#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
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
import sys
from pathlib import Path


def load_json(path: Path):
    with path.open() as f:
        return json.load(f)


def require_keys(obj, keys, label):
    missing = [key for key in keys if key not in obj]
    if missing:
        raise SystemExit(f"{label}: missing keys: {', '.join(missing)}")


def validate_run_status(path: Path):
    data = load_json(path)
    require_keys(
        data,
        ["contract_version", "status", "task", "out_dir", "resume_from", "actions", "derived", "artifacts"],
        "run_status.json",
    )
    require_keys(data["derived"], ["outcome_code", "alerts", "metric_summary"], "run_status.json derived")
    require_keys(data["artifacts"], ["report", "best", "latest", "final"], "run_status.json artifacts")


def validate_training_config(path: Path):
    data = load_json(path)
    require_keys(data, ["contract_version", "artifact_family_version", "task"], "training_config.json")
    if "inputs" not in data:
        raise SystemExit("training_config.json: missing inputs")


def validate_training_report(path: Path):
    data = load_json(path)
    require_keys(data, ["contract_version", "artifact_family_version", "task"], "training_report.json")
    if "report" not in data and "summary" not in data:
        raise SystemExit("training_report.json: missing report/summary payload")


def main() -> int:
    if len(sys.argv) < 2:
        print("usage: validate_run_contracts.py <run_dir> [<run_dir> ...]", file=sys.stderr)
        return 2

    for raw in sys.argv[1:]:
        run_dir = Path(raw)
        if not run_dir.is_dir():
            raise SystemExit(f"{run_dir}: not a directory")

        training_config = run_dir / "training_config.json"
        training_report = run_dir / "training_report.json"
        if not training_config.is_file():
            raise SystemExit(f"{run_dir}: missing training_config.json")
        if not training_report.is_file():
            raise SystemExit(f"{run_dir}: missing training_report.json")

        validate_training_config(training_config)
        validate_training_report(training_report)

        run_status = run_dir / "run_status.json"
        if run_status.is_file():
            validate_run_status(run_status)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
