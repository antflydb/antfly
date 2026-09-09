"""Attribute qualified dense-delete A/B arms to replay work, not window size.

Usage: python scripts/summarize_dense_delete_experiment.py AB_ROOT
Outputs JSON; refuses incomplete/failed arms and ambiguous replay sequences.
"""

import argparse
import json
import math
from pathlib import Path

from native_preparation_experiments import events
from source_capture_experiment import checkpoint_handoffs


def mixed_apply_work(lines, covered_sequence):
    if covered_sequence <= 0:
        raise ValueError("invalid covered sequence")
    rows = []
    seen = set()
    for row in events(lines, "antfly_bench_derived_worker "):
        if row.get("kind") != "dense_vector" or row.get("index") != "vec":
            continue
        overwritten, documents = int(row["overwritten"]), int(row["documents"])
        if min(overwritten, documents) < 0:
            raise ValueError("negative replay count")
        if not overwritten or not documents:
            continue
        sequence = int(row["sequence"])
        if sequence <= 0:
            raise ValueError("invalid replay sequence")
        if sequence > covered_sequence:
            continue  # Later source churn/enrichment is not the mixed window.
        if sequence in seen:
            raise ValueError("ambiguous/retried dense apply sequence")
        for key in ("total_ms", "dense_delete_ms", "dense_embedding_apply_ms"):
            value = float(row[key])
            if not math.isfinite(value) or value < 0:
                raise ValueError("invalid replay timer")
        seen.add(sequence)
        rows.append(row)
    if not rows:
        raise ValueError("no mixed dense apply observations")
    documents = sum(int(row["documents"]) for row in rows)
    totals = {
        key: sum(float(row[key]) for row in rows)
        for key in ("total_ms", "dense_delete_ms", "dense_embedding_apply_ms")
    }
    return {
        "windows": len(rows),
        "replayed_documents": documents,
        "stage_totals_ms": totals,
        "apply_ms_per_1000_replayed_documents": totals["total_ms"] / documents * 1000,
        "max_apply_window_ms": max(float(row["total_ms"]) for row in rows),
    }


def summarize(root):
    receipts = json.loads((root / "ab-runs.json").read_text())
    if not receipts or any(
        row.get("exit_code") != 0 or row.get("invalid_reason") for row in receipts
    ):
        raise ValueError("all recorded arms must pass qualification first")
    cases = {}
    for row in receipts:
        pairs = cases.setdefault(row["case"], {})
        modes = pairs.setdefault(row["pair"], set())
        if row["mode"] in modes:
            raise ValueError("duplicate qualification arm")
        modes.add(row["mode"])
    if any(
        len(pairs) < 2
        or any(modes != {"control", "candidate"} for modes in pairs.values())
        for pairs in cases.values()
    ):
        raise ValueError("expected complete reversed control/candidate pairs")
    result = []
    for receipt in receipts:
        arm = Path(receipt["command"][1])
        profile = json.loads((arm / "public-mixed-profile.json").read_text())
        shards = profile["final_status"]["shard_status"]
        if len(shards) != 1 or profile["errors"]:
            raise ValueError("expected one error-free benchmark shard")
        status = next(iter(shards.values()))
        covered = int(status["catch_up_applied_sequence"])
        if covered != int(status["catch_up_target_sequence"]):
            raise ValueError("mixed workload still has replay debt")
        lines = (arm / "antfly-initial.log").read_text().splitlines()
        result.append(
            {
                "arm": arm.name,
                "written_rows": profile["written_rows"],
                "mixed_covered_sequence": covered,
                **mixed_apply_work(lines, covered),
                "checkpoint_handoffs": checkpoint_handoffs(lines),
            }
        )
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path)
    args = parser.parse_args()
    print(json.dumps(summarize(args.root), indent=2))
