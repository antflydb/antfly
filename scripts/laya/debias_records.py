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

"""Remove answer-position and yes/no priors from native Laya records.

python3 scripts/laya/debias_records.py labelled.jsonl --output debiased.jsonl

Generated cases tend to list the right option first and to phrase yes/no
statements that hold; a student trained on them learns those priors instead
of the content (E2 never chose an option past the third and answered "true"
81% of the time). This shuffles every choice question's options, permuting
labels, descriptions and target together, leaves score questions in order
(their levels are ordinal), and subsamples yes/no questions so the target's
argmax is true and false equally often. The input is unchanged.
"""

from __future__ import annotations

import argparse
import json
import random
import sys
from collections import Counter
from pathlib import Path


def shuffled(record: dict, rng: random.Random) -> dict:
    order = list(range(len(record["labels"])))
    rng.shuffle(order)
    out = dict(record)
    for key in ("labels", "descriptions", "target"):
        if key in record and record[key] is not None:
            out[key] = [record[key][i] for i in order]
    return out


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("records", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--seed", type=int, default=20261007)
    args = parser.parse_args()
    if args.output.exists():
        parser.error(f"{args.output} exists")
    rng = random.Random(args.seed)
    rows = [
        json.loads(line)
        for line in args.records.read_text().splitlines()
        if line.strip()
    ]
    yes = [r for r in rows if r["kind"] == "noul" and r["target"][1] > r["target"][0]]
    no = [r for r in rows if r["kind"] == "noul" and r["target"][1] <= r["target"][0]]
    keep = min(len(yes), len(no))
    kept_noul = {id(r) for r in rng.sample(yes, keep) + rng.sample(no, keep)}
    out, counts = [], Counter()
    for record in rows:
        if record["kind"] == "noul":
            if id(record) not in kept_noul:
                counts["noul dropped"] += 1
                continue
            out.append(record)
        elif record["kind"] == "choice":
            out.append(shuffled(record, rng))
        else:
            out.append(record)
        counts[record["kind"]] += 1
    with args.output.open("x") as handle:
        for record in out:
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")
    position = Counter(
        max(range(len(r["target"])), key=r["target"].__getitem__)
        for r in out
        if r["kind"] == "choice"
    )
    print(
        json.dumps(
            {
                "records": len(out),
                **counts,
                "choice answer by position": dict(sorted(position.items())),
            }
        ),
        file=sys.stderr,
    )


if __name__ == "__main__":
    main()
