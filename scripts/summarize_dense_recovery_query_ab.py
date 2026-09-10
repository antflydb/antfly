"""Summarize completed query-only recovery arms; never call these qualification QPS."""

import argparse
import json
import math
import statistics
from pathlib import Path


def recall_checks(rows, budget_percentage_points=1.0):
    controls = {row["pair"]: row for row in rows if row["mode"] == "control"}
    checks = []
    for row in rows:
        if row["mode"] == "control":
            continue
        control = controls.get(row["pair"], {})
        candidate_recall = row.get("fixed_queries_recall")
        control_recall = control.get("fixed_queries_recall")
        check = {
            "mode": row["mode"],
            "pair": row["pair"],
            "budget_percentage_points": budget_percentage_points,
            "status": "not_measured",
        }
        if (
            row.get("fixed_queries_count", 0) > 0
            and row.get("fixed_queries_count") == control.get("fixed_queries_count")
            and all(
                value is not None and math.isfinite(value) and 0 <= value <= 1
                for value in (control_recall, candidate_recall)
            )
        ):
            loss = 100 * (control_recall - candidate_recall)
            check.update(
                control_recall=control_recall,
                candidate_recall=candidate_recall,
                loss_percentage_points=loss,
                status="pass" if loss <= budget_percentage_points + 1e-9 else "fail",
            )
        checks.append(check)
    return checks


def summarize(root):
    rows = []
    receipts = json.loads((root / "runs.json").read_text())
    for receipt in receipts:
        if not receipt.get("passed") or receipt.get("error"):
            continue
        arm = root / f"{receipt['pair']}-{receipt['mode']}"
        row = {"mode": receipt["mode"], "pair": receipt["pair"]}
        for concurrency in (1, 30):
            result = json.loads((arm / f"c{concurrency}.json").read_text())
            prefix = f"c{concurrency}"
            row[f"{prefix}_diagnostic_qps"] = result["qps"]
            row[f"{prefix}_recall"] = result["recall"]
            row[f"{prefix}_http_p95_ms"] = result["all"]["http_ms"]["p95_ms"]
            for key, values in result["all"]["server_stages_ms"].items():
                if key in (
                    "total_ns",
                    "hbc_admission_wait_ns",
                    "hbc_rerank_artifact_read_ns",
                    "hbc_scan_admission_wait_ns",
                    "hbc_rerank_admission_wait_ns",
                    "hbc_leaf_score_ns",
                    "hbc_child_expand_ns",
                    "hbc_subgroup_routing_ns",
                    "hbc_rerank_distance_ns",
                ):
                    row[f"{prefix}_{key}_mean_ms"] = values["mean_ms"]
                    row[f"{prefix}_{key}_p95_ms"] = values["p95_ms"]
            for section in ("admission_work", "physical_io"):
                for key, values in result["all"][section].items():
                    if values["mean"] is not None:
                        row[f"{prefix}_{key}_mean"] = values["mean"]
        if (arm / "warmup.json").exists():
            warmup = json.loads((arm / "warmup.json").read_text())
            for key in (
                "count",
                "recall",
                "approximate_vectors_mean",
                "exact_vectors_mean",
                "leaves_mean",
            ):
                if key in warmup:
                    row[f"fixed_queries_{key}"] = warmup[key]
            for key, values in warmup.get("profile_values", {}).items():
                if key.startswith(("hbc_traversal_", "hbc_subgroup_")):
                    row[f"fixed_queries_{key}_mean"] = values["mean"]
        if (arm / "memory.jsonl").exists():
            memory = [
                json.loads(line)
                for line in (arm / "memory.jsonl").read_text().splitlines()
            ]
            for key in ("rss_bytes", "phys_footprint_bytes"):
                values = [sample[key] for sample in memory if key in sample]
                if values:
                    phases = (
                        "restart_query_and_mixed"
                        if (arm / "mixed.json").exists()
                        else "restart_and_query"
                    )
                    row[f"{phases}_peak_{key}"] = max(values)
        if (arm / "mixed.json").exists():
            mixed = json.loads((arm / "mixed.json").read_text())
            for key in (
                "offered_write_rows_per_second",
                "write_rows_per_second",
                "query_qps",
                "recall",
                "catchup_seconds",
            ):
                value = mixed.get(key)
                if isinstance(value, (int, float)) and math.isfinite(value):
                    row[f"mixed_{key}"] = value
            for phase in (
                "query_latency",
                "server_latency",
                "hbc_latency",
                "write_latency",
                "write_schedule_delay",
                "write_scheduled_latency",
            ):
                for statistic in ("mean_ms", "p95_ms", "p99_ms", "max_ms"):
                    value = (mixed.get(phase) or {}).get(statistic)
                    if isinstance(value, (int, float)) and math.isfinite(value):
                        row[f"mixed_{phase}_{statistic}"] = value
        rows.append(row)
    medians = []
    for mode in sorted({row["mode"] for row in rows}):
        arms = [row for row in rows if row["mode"] == mode]
        fields = set.intersection(*(set(row) for row in arms)) - {"mode", "pair"}
        medians.append(
            {
                "mode": mode,
                "arms": len(arms),
                **{
                    key: statistics.median(row[key] for row in arms)
                    for key in sorted(fields)
                },
            }
        )
    return {
        "diagnostic_only": True,
        "individual_arms": rows,
        "medians": medians,
        "recall_checks": recall_checks(rows),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path)
    args = parser.parse_args()
    output = json.dumps(summarize(args.root), indent=2) + "\n"
    (args.root / "summary.json").write_text(output)
    print(output, end="")


if __name__ == "__main__":
    main()
