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

"""Turn generated cases (generate_decision_cases.py) into native Laya records.

python3 scripts/laya/synthetic_to_records.py cases.jsonl --output records.jsonl

One record per question, every question of a case sharing its `group_id` and
state text, labels and descriptions shaped as prepare_laya_finetune.py shapes
typed-decisions (score labels "0".."n-1", yes/no labels "false", "true"). The
cases carry no answers, so targets are uniform placeholders: label them with
prepare_laya_longcontext_teacher.py --score-all --gold-weight 0. Lines that do
not parse (an interrupted write) are skipped and counted.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


def records(case: dict) -> list[dict]:
    text = json.dumps(case["state"], ensure_ascii=False)
    out = []
    for name, q in case["questions"].items():
        kind, criteria = q["type"], q.get("criteria")
        if kind == "choice":
            labels, descriptions = list(criteria), [str(v) for v in criteria.values()]
        elif kind == "score":
            labels, descriptions = (
                [str(i) for i in range(len(criteria))],
                [str(v) for v in criteria],
            )
        else:
            criteria = criteria if isinstance(criteria, dict) else {}
            labels = ["false", "true"]
            descriptions = [
                str(criteria.get("false") or ""),
                str(criteria.get("true") or ""),
            ]
        out.append(
            {
                "id": f"{case['id']}/{name}",
                "group_id": case["id"],
                "text": text,
                "kind": kind,
                "instruction": q["instructions"],
                "labels": labels,
                "descriptions": descriptions,
                "target": [1 / len(labels)] * len(labels),
            }
        )
    return out


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("cases", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        parser.error(f"{args.output} exists")
    cases = skipped = written = 0
    with args.output.open("x") as out:
        for line in args.cases.read_text().splitlines():
            if not line.strip():
                continue
            try:
                case = json.loads(line)
                rows = records(case)
            except (json.JSONDecodeError, KeyError, TypeError, AttributeError):
                skipped += 1
                continue
            cases += 1
            for row in rows:
                out.write(json.dumps(row, ensure_ascii=False) + "\n")
                written += 1
    print(
        json.dumps({"cases": cases, "records": written, "skipped_lines": skipped}),
        file=sys.stderr,
    )


if __name__ == "__main__":
    main()
