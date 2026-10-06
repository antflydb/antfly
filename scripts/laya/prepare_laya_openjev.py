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

# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "torch>=2.6,<3",
#     "transformers>=4.51,<5",
# ]
# ///
"""Convert Open-Jev records to native Antfly Laya training records.

Open-Jev (https://huggingface.co/datasets/ZefanCai/Open-Jev) publishes
rule-labelled typed decisions in almost our format: `group_id`, `kind`
(`choice`/`score`/`noul`), `question`, `options`, `state` and a `target`
distribution aligned with `options`. The labels come from each generator's
rules, so no teacher is needed. See zig/pkg/inference/models/laya/LAYA.md,
"Scaling packed training on Open-Jev".

    uv run --script scripts/laya/prepare_laya_openjev.py \\
        .tmp/laya/openjev/raw/release-v2-redistributable/train.jsonl.gz \\
        --laya-model .tmp/laya/laya-released --common .tmp/laya/common.py \\
        --disjoint-from td/s0-eval.jsonl --disjoint-from td/s0-calibration.jsonl \\
        --output openjev-train.jsonl

Mapping, matching prepare_laya_finetune.py's conventions:
- choice: options written `label: description` (every option, unique labels)
  split into label and description; otherwise the option is the label.
- score: labels `0`..`n-1`, the options become their descriptions.
- noul: options must be `no`, `yes` (or `false`, `true`); labels become
  `false`, `true`.

Records are dropped when:
- their source is excluded (`customer-control-v1` by default: Open-Jev notes
  its question descriptions come from TypeSafe documentation without a
  verified source license);
- the whole state does not fit Laya's sequence with `--margin` tokens to spare
  (the packed trainer rejects a row whose state would be truncated);
- their case overlaps a `--disjoint-from` split by group, ID or state text.
A case whose records are all dropped disappears; a partly dropped case keeps
the records that fit.
"""

from __future__ import annotations

import argparse
import collections
import gzip
import json
import math
import os
import tempfile
from pathlib import Path

from prepare_laya_longcontext_teacher import (
    laya_decision_config,
    load_module,
    upstream_question,
)

DEFAULT_EXCLUDED = ("customer-control-v1",)
BOOLEAN_OPTIONS = {
    ("no", "yes"): False,
    ("false", "true"): False,
    ("yes", "no"): True,
    ("true", "false"): True,
}


def open_text(path: Path):
    return gzip.open(path, "rt") if path.suffix == ".gz" else path.open()


def state_text(state) -> str:
    return state if isinstance(state, str) else json.dumps(state, ensure_ascii=False)


def convert(row: dict, max_labels: int = 20) -> dict:
    """One Open-Jev row as a native record. Raises ValueError when it cannot map."""
    kind, options, target = (
        row["kind"],
        list(row["options"]),
        [float(p) for p in row["target"]],
    )
    if len(options) != len(target):
        raise ValueError("options and target differ in length")
    if kind == "choice":
        parts = [option.split(": ", 1) for option in options]
        if all(len(p) == 2 and p[0] for p in parts) and len(
            {p[0] for p in parts}
        ) == len(parts):
            labels, descriptions = [p[0] for p in parts], [p[1] for p in parts]
        else:
            labels, descriptions = options, [""] * len(options)
    elif kind == "score":
        labels, descriptions = [str(i) for i in range(len(options))], options
    elif kind == "noul":
        key = tuple(o.strip().lower() for o in options)
        if key not in BOOLEAN_OPTIONS:
            raise ValueError(f"noul options must be no/yes: {options}")
        if BOOLEAN_OPTIONS[key]:
            target = target[::-1]
        labels, descriptions = ["false", "true"], ["", ""]
    else:
        raise ValueError(f"unknown kind {kind}")
    if (
        not 2 <= len(labels) <= max_labels
        or len(set(labels)) != len(labels)
        or not all(labels)
    ):
        raise ValueError(f"expected 2-{max_labels} unique nonempty labels")
    total = sum(target)
    if any(not math.isfinite(p) or p < 0 for p in target) or abs(total - 1) > 1e-4:
        raise ValueError("invalid target distribution")
    text = state_text(row["state"])
    if not text or not row["question"]:
        raise ValueError("empty state or question")
    return {
        "id": row["id"],
        "group_id": row["group_id"],
        "text": text,
        "kind": kind,
        "instruction": row["question"],
        "labels": labels,
        "descriptions": descriptions,
        "target": [p / total for p in target],
    }


def fits(tok, record: dict, decision: dict, build_sequence, margin: int) -> bool:
    """True when Laya keeps the whole state with `margin` tokens to spare."""
    ids, _ = build_sequence(
        tok,
        record["text"],
        upstream_question(record),
        decision["max_len"] * 4,
        decision["head_max_len"],
    )
    return len(ids) + margin <= decision["max_len"]


def main():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("source", type=Path, help="Open-Jev raw JSONL (optionally .gz)")
    parser.add_argument(
        "--laya-model",
        type=Path,
        required=True,
        help="Prepared unpacked Laya directory",
    )
    parser.add_argument(
        "--common", type=Path, required=True, help="Upstream laya/common.py"
    )
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--exclude-source",
        action="append",
        help=f"Drop a source (default: {', '.join(DEFAULT_EXCLUDED)})",
    )
    parser.add_argument(
        "--disjoint-from",
        type=Path,
        action="append",
        default=[],
        help="Native split to stay disjoint from",
    )
    parser.add_argument(
        "--margin",
        type=int,
        default=4,
        help="Spare tokens required after the whole state",
    )
    args = parser.parse_args()
    if args.output.exists():
        parser.error(f"Output already exists: {args.output}")
    excluded = set(args.exclude_source or DEFAULT_EXCLUDED)

    from transformers import PreTrainedTokenizerFast

    common = load_module(args.common, "laya_common")
    decision = laya_decision_config(args.laya_model)
    tok = PreTrainedTokenizerFast.from_pretrained(args.laya_model)

    avoid_groups, avoid_ids, avoid_texts = set(), set(), set()
    for path in args.disjoint_from:
        for line in path.read_text().splitlines():
            if line.strip():
                r = json.loads(line)
                avoid_groups.add(r["group_id"])
                avoid_ids.add(r["id"])
                avoid_texts.add(r["text"])

    dropped = collections.Counter()
    kept_by_source = collections.Counter()
    fit_cache: dict = {}
    count, ids = 0, set()
    temporary = None
    try:
        with (
            open_text(args.source) as source,
            tempfile.NamedTemporaryFile(
                mode="w", dir=args.output.parent, delete=False
            ) as out,
        ):
            temporary = Path(out.name)
            for line in source:
                if not line.strip():
                    continue
                row = json.loads(line)
                if row["source"] in excluded:
                    dropped["excluded source"] += 1
                    continue
                try:
                    record = convert(row)
                except (ValueError, KeyError, TypeError):
                    dropped["unmappable"] += 1
                    continue
                if (
                    record["group_id"] in avoid_groups
                    or record["id"] in avoid_ids
                    or record["text"] in avoid_texts
                ):
                    dropped["overlaps a disjoint split"] += 1
                    continue
                if record["id"] in ids:
                    dropped["duplicate id"] += 1
                    continue
                key = (
                    record["text"],
                    record["kind"],
                    record["instruction"],
                    tuple(record["labels"]),
                    tuple(record["descriptions"]),
                )
                if key not in fit_cache:
                    fit_cache[key] = fits(
                        tok, record, decision, common.build_sequence, args.margin
                    )
                if not fit_cache[key]:
                    dropped["state does not fit"] += 1
                    continue
                ids.add(record["id"])
                kept_by_source[row["source"]] += 1
                out.write(
                    json.dumps(record, ensure_ascii=False, allow_nan=False) + "\n"
                )
                count += 1
            if not count:
                raise ValueError("Empty dataset")
            out.flush()
            os.fsync(out.fileno())
        os.link(temporary, args.output)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
    print(
        json.dumps(
            {
                "records": count,
                "output": str(args.output),
                "dropped": dict(dropped),
                "kept_by_source": dict(kept_by_source),
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
