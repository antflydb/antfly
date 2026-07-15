#!/usr/bin/env python3
"""Compare correctness JSONL emitted by search benchmark adapters.

Inputs are line-aligned VERIFY_TOP_N_COUNT responses. The verifier fails closed
on schema, grammar, exact-count, ordering, score, or non-cutoff ID differences.
Different IDs are accepted only inside each engine's tied cutoff score group.
Raw mismatch diagnostics are emitted as JSON for archival with benchmark runs.
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path
from typing import Any


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("left", type=Path)
    parser.add_argument("right", type=Path)
    parser.add_argument("--abs-score-tol", type=float, default=1e-5)
    parser.add_argument("--rel-score-tol", type=float, default=1e-5)
    parser.add_argument("--allow-lower-bound-counts", action="store_true")
    parser.add_argument("--diagnostics", type=Path)
    return parser.parse_args()


def load_jsonl(path: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as source:
        for line_number, raw in enumerate(source, 1):
            line = raw.strip()
            if not line:
                continue
            try:
                value = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"{path}:{line_number}: invalid JSON: {exc}") from exc
            if not isinstance(value, dict):
                raise ValueError(f"{path}:{line_number}: expected JSON object")
            records.append(value)
    return records


def score_close(left: float, right: float, abs_tol: float, rel_tol: float) -> bool:
    return math.isfinite(left) and math.isfinite(right) and math.isclose(
        left, right, abs_tol=abs_tol, rel_tol=rel_tol
    )


def validate_record(record: dict[str, Any], side: str, index: int) -> list[str]:
    errors: list[str] = []
    if record.get("schema_version") != 1:
        errors.append(f"{side}[{index}]: unsupported schema_version")
    if record.get("query_grammar") != "V1":
        errors.append(f"{side}[{index}]: unsupported query_grammar")
    if record.get("relation") not in ("exact", "gte"):
        errors.append(f"{side}[{index}]: invalid total-hit relation")
    if not isinstance(record.get("total_hits"), int) or record["total_hits"] < 0:
        errors.append(f"{side}[{index}]: invalid total_hits")
    hits = record.get("hits")
    if not isinstance(hits, list):
        errors.append(f"{side}[{index}]: hits must be an array")
        return errors
    previous_score = math.inf
    seen: set[int] = set()
    for hit_index, hit in enumerate(hits):
        if not isinstance(hit, dict):
            errors.append(f"{side}[{index}].hits[{hit_index}]: expected object")
            continue
        doc_id = hit.get("id")
        score = hit.get("score")
        if not isinstance(doc_id, int) or doc_id < 0:
            errors.append(f"{side}[{index}].hits[{hit_index}]: invalid id")
        elif doc_id in seen:
            errors.append(f"{side}[{index}].hits[{hit_index}]: duplicate id {doc_id}")
        else:
            seen.add(doc_id)
        if not isinstance(score, (int, float)) or not math.isfinite(float(score)):
            errors.append(f"{side}[{index}].hits[{hit_index}]: invalid score")
        elif float(score) > previous_score:
            errors.append(f"{side}[{index}]: hits are not score-descending")
        else:
            previous_score = float(score)
    return errors


def compare_record(
    left: dict[str, Any],
    right: dict[str, Any],
    index: int,
    abs_tol: float,
    rel_tol: float,
    allow_lower_bound_counts: bool,
) -> tuple[list[str], bool]:
    errors = validate_record(left, "left", index)
    errors.extend(validate_record(right, "right", index))
    if errors:
        return errors, False

    if left.get("query") != right.get("query"):
        if "query" in left or "query" in right:
            errors.append(f"query[{index}]: query identity differs")

    if not allow_lower_bound_counts and (
        left["relation"] != "exact" or right["relation"] != "exact"
    ):
        errors.append(f"query[{index}]: exact totals required")
    if left["relation"] == "exact" and right["relation"] == "exact":
        if left["total_hits"] != right["total_hits"]:
            errors.append(
                f"query[{index}]: total_hits {left['total_hits']} != {right['total_hits']}"
            )

    left_hits = left["hits"]
    right_hits = right["hits"]
    if len(left_hits) != len(right_hits):
        errors.append(
            f"query[{index}]: hit lengths {len(left_hits)} != {len(right_hits)}"
        )
        return errors, False
    if not left_hits:
        return errors, False

    left_cutoff = float(left_hits[-1]["score"])
    right_cutoff = float(right_hits[-1]["score"])
    if not score_close(left_cutoff, right_cutoff, abs_tol, rel_tol):
        errors.append(
            f"query[{index}]: cutoff scores {left_cutoff} != {right_cutoff}"
        )
        return errors, False

    def split_cutoff(hits: list[dict[str, Any]], cutoff: float) -> tuple[dict[int, float], set[int]]:
        definite: dict[int, float] = {}
        tied: set[int] = set()
        for hit in hits:
            score = float(hit["score"])
            if score_close(score, cutoff, abs_tol, rel_tol):
                tied.add(hit["id"])
            else:
                definite[hit["id"]] = score
        return definite, tied

    left_definite, left_tied = split_cutoff(left_hits, left_cutoff)
    right_definite, right_tied = split_cutoff(right_hits, right_cutoff)
    if left_definite.keys() != right_definite.keys():
        errors.append(
            f"query[{index}]: non-cutoff IDs differ: "
            f"{sorted(left_definite)} != {sorted(right_definite)}"
        )
    else:
        for doc_id, left_score in left_definite.items():
            right_score = right_definite[doc_id]
            if not score_close(left_score, right_score, abs_tol, rel_tol):
                errors.append(
                    f"query[{index}]: score for id {doc_id}: "
                    f"{left_score} != {right_score}"
                )

    if len(left_tied) != len(right_tied):
        errors.append(f"query[{index}]: cutoff tie widths differ")
    strict_match = [hit["id"] for hit in left_hits] == [hit["id"] for hit in right_hits]
    return errors, not strict_match and not errors


def main() -> int:
    args = parse_args()
    try:
        left = load_jsonl(args.left)
        right = load_jsonl(args.right)
    except (OSError, ValueError) as exc:
        print(str(exc), file=sys.stderr)
        return 2

    diagnostics: dict[str, Any] = {
        "schema_version": 1,
        "left": str(args.left),
        "right": str(args.right),
        "queries": max(len(left), len(right)),
        "strict_matches": 0,
        "tie_aware_matches": 0,
        "errors": [],
    }
    if len(left) != len(right):
        diagnostics["errors"].append(
            f"record counts differ: {len(left)} != {len(right)}"
        )
    else:
        for index, (left_record, right_record) in enumerate(zip(left, right)):
            errors, tie_aware = compare_record(
                left_record,
                right_record,
                index,
                args.abs_score_tol,
                args.rel_score_tol,
                args.allow_lower_bound_counts,
            )
            diagnostics["errors"].extend(errors)
            if not errors:
                left_ids = [hit["id"] for hit in left_record["hits"]]
                right_ids = [hit["id"] for hit in right_record["hits"]]
                if left_ids == right_ids:
                    diagnostics["strict_matches"] += 1
                elif tie_aware:
                    diagnostics["tie_aware_matches"] += 1

    diagnostics["ok"] = not diagnostics["errors"]
    encoded = json.dumps(diagnostics, indent=2, sort_keys=True) + "\n"
    if args.diagnostics:
        args.diagnostics.write_text(encoded, encoding="utf-8")
    sys.stdout.write(encoded)
    return 0 if diagnostics["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
