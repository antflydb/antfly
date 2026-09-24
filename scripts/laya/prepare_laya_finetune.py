#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Convert a typed-decisions JSONL split to native Antfly Laya training records.

Accepts state/questions/gold objects or their JSON-encoded dataset columns.
Each input case stays in one group across all its questions. Run separately
on the upstream train, evaluation, and optional calibration splits.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import tempfile
from pathlib import Path


def decoded(value):
    return json.loads(value) if isinstance(value, str) else value


def description(value):
    if value is None:
        return ""
    return value if isinstance(value, str) else json.dumps(value, ensure_ascii=False)


def convert(case: dict) -> list[dict]:
    state = case["state"]
    if isinstance(state, str):
        try:
            state = json.loads(state)
        except json.JSONDecodeError:
            pass
    text = state if isinstance(state, str) else json.dumps(state, ensure_ascii=False)
    if not text:
        raise ValueError("Empty state")
    group = str(
        case.get("id", case.get("case_id", hashlib.sha256(text.encode()).hexdigest()))
    )
    questions, gold = decoded(case["questions"]), decoded(case["gold"])
    if not isinstance(questions, dict) or not questions or set(questions) != set(gold):
        raise ValueError("Every question must have exactly one gold target")
    records = []
    for qid, question in questions.items():
        kind, criteria = question["type"], question.get("criteria")
        if kind == "choice" and isinstance(criteria, dict):
            labels = list(criteria)
            descriptions = [description(criteria[key]) for key in labels]
        elif kind == "score" and isinstance(criteria, list):
            labels = [str(i) for i in range(len(criteria))]
            descriptions = [description(value) for value in criteria]
        elif kind == "noul" and (criteria is None or isinstance(criteria, dict)):
            labels = ["false", "true"]
            descriptions = [description((criteria or {}).get(key)) for key in labels]
        else:
            raise ValueError(f"Invalid question type/criteria: {qid}")
        if not 2 <= len(labels) <= 20 or any(
            not isinstance(label, str) or not label for label in labels
        ):
            raise ValueError(f"Expected 2-20 nonempty labels: {qid}")
        probabilities = gold[qid]["probabilities"]
        if not isinstance(probabilities, dict) or set(probabilities) != set(labels):
            raise ValueError(f"Target keys differ from label keys: {qid}")
        target = [probabilities[key] for key in labels]
        if (
            any(
                isinstance(p, bool)
                or not isinstance(p, (float, int))
                or not math.isfinite(p)
                or not 0 <= p <= 1
                for p in target
            )
            or abs(sum(target) - 1) > 1e-5
        ):
            raise ValueError(f"Invalid target distribution: {qid}")
        instruction = question["instructions"]
        if not isinstance(instruction, str) or not instruction:
            raise ValueError(f"Empty instruction: {qid}")
        records.append(
            {
                "id": f"{group}/{qid}",
                "group_id": group,
                "text": text,
                "kind": kind,
                "instruction": instruction,
                "labels": labels,
                "descriptions": descriptions,
                "target": target,
            }
        )
    return records


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        parser.error(f"Output already exists: {args.output}")
    ids = set()
    count = 0
    temporary = None
    try:
        with (
            args.source.open() as source,
            tempfile.NamedTemporaryFile(
                mode="w", dir=args.output.parent, delete=False
            ) as out,
        ):
            temporary = Path(out.name)
            for line_number, line in enumerate(source, 1):
                if not line.strip():
                    continue
                try:
                    records = convert(json.loads(line))
                    for record in records:
                        if record["id"] in ids:
                            raise ValueError(f"Duplicate decision ID: {record['id']}")
                        ids.add(record["id"])
                        out.write(
                            json.dumps(record, ensure_ascii=False, allow_nan=False)
                            + "\n"
                        )
                        count += 1
                except (ValueError, KeyError, TypeError) as error:
                    raise ValueError(f"{args.source}:{line_number}: {error}") from error
            if not count:
                raise ValueError("Empty dataset")
            out.flush()
            os.fsync(out.fileno())
        os.link(temporary, args.output)  # Atomic, and fails if another writer won.
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
    print(json.dumps({"records": count, "output": str(args.output)}))


if __name__ == "__main__":
    main()
