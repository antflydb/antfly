"""Summarize completed matched arms without substituting historical winners."""

import argparse
import json
import statistics
from pathlib import Path

from projection_locality_inputs import receipt_passed


def load(path):
    return json.loads(path.read_text())


def summarize(root):
    rows = []
    for receipt in load(root / "ab-runs.json"):
        if not receipt_passed(root, receipt):
            continue
        arm = Path(receipt["command"][1])
        summary = load(arm / "qualification-summary.json")
        disk = load(arm / "disk-after-restart.json")
        table = load(arm / "table-before-restart.json")
        expected_mode = receipt.get("table_mode", receipt["mode"])
        if table.get("storage", {}).get("dense_embeddings") != expected_mode:
            raise ValueError(f"wrong effective table storage: {arm}")
        live = next(r for r in summary["runs"] if r["label"].endswith("online-live"))
        warm = next(r for r in summary["runs"] if r["label"].endswith("reopened-warm"))
        mixed = summary["public_mixed_profiles"]["public-mixed-profile"]
        if mixed["errors"]:
            raise ValueError(f"mixed workload errors: {arm}")
        row = {
            "case": receipt["case"],
            "pair": receipt["pair"],
            "mode": receipt["mode"],
            "encoding": receipt["encoding"],
            "ready_s": live["ready_seconds"],
            "insert_s": live["insert_seconds"],
            "recall": live["recall"],
            "allocated_bytes": disk["total_allocated_bytes"],
            "logical_bytes": disk["total_logical_bytes"],
            "read_only_rss_bytes": summary["phase_rss_profiles"]["read_only"][
                "peak_rss_bytes"
            ],
            "mixed_rss_bytes": summary["phase_rss_profiles"]["mixed"]["peak_rss_bytes"],
            "restart_rss_bytes": summary["rss_profiles"]["rss-restart"][
                "peak_rss_bytes"
            ],
            "mixed_write_rows_s": mixed["write_rows_per_second"],
            "mixed_write_p95_ms": mixed["write_latency"]["p95_ms"],
            "mixed_query_qps": mixed["query_qps"],
            "mixed_query_p95_ms": mixed["query_latency"]["p95_ms"],
            "mixed_server_p95_ms": mixed["server_latency"]["p95_ms"],
            "mixed_catchup_s": mixed["catchup_seconds"],
            "warm_p95_ms": warm["serial_latency_ms"]["p95"],
            "warm_recall": warm["recall"],
        }
        # The ledger high-water mark is captured after each timed phase;
        # unlike sampled RSS it measures physical footprint, not mapped pages.
        for phase, name in (("live", "footprint"), ("restart", "footprint-restart")):
            memory = summary.get("memory_profiles", {}).get(name, {})
            for key in ("demand_peak_bytes", "phys_footprint_ledger_peak_bytes"):
                if memory.get(key) is not None:
                    row[f"{phase}_{key}"] = memory[key]
        for i, concurrency in enumerate(live["concurrency"]):
            row[f"c{concurrency}_qps"] = live["concurrent_qps"][i]
            for percentile in ["p95", "p99"]:
                row[f"c{concurrency}_{percentile}_ms"] = live["concurrent_latency_ms"][
                    percentile
                ][i]
        for group, values in disk["groups"].items():
            row[f"disk_{group}_allocated_bytes"] = values["allocated_bytes"]
        profile_path = arm / "public-query-profile.json"
        if profile_path.exists():
            profile = load(profile_path)
            for key in [
                "hbc_rerank_vector_physical_reads",
                "hbc_rerank_vector_projection_reads",
                "hbc_rerank_vector_projection_bytes",
                "hbc_rerank_vector_residual_bytes",
                "hbc_subgroup_leaves_scored",
                "hbc_subgroup_vectors_skipped",
            ]:
                if key in profile["profile_values"]:
                    row[f"post_restart_profile_{key}_mean"] = profile["profile_values"][
                        key
                    ]["mean"]
            for key in [
                "approximate_vectors_mean",
                "exact_vectors_mean",
                "mean_ms",
                "p95_ms",
            ]:
                row[f"post_restart_profile_{key}"] = profile[key]
            for key in [
                "hbc_leaf_score_ns",
                "hbc_rerank_artifact_read_ns",
                "hbc_projection_completion_ns",
                "hbc_subgroup_routing_ns",
            ]:
                if key in profile["server_timings"]:
                    row[f"post_restart_profile_{key}_mean_ms"] = profile[
                        "server_timings"
                    ][key]["mean_ms"]
        rows.append(row)
    medians = []
    for case, mode in sorted({(r["case"], r["mode"]) for r in rows}):
        group = [r for r in rows if r["case"] == case and r["mode"] == mode]
        common = set.intersection(*(set(r) for r in group))
        medians.append(
            {
                "case": case,
                "mode": mode,
                "arms": len(group),
                **{
                    key: statistics.median(r[key] for r in group)
                    for key in sorted(common - {"case", "mode", "pair", "encoding"})
                },
            }
        )
    return {"individual_arms": rows, "medians": medians}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    rendered = json.dumps(summarize(args.root), indent=2) + "\n"
    if args.output:
        args.output.write_text(rendered)
    else:
        print(rendered, end="")


if __name__ == "__main__":
    main()
