#!/usr/bin/env python3
"""Summarize frozen small A/B receipts without hiding failed qualification gates."""

import argparse
import json
import statistics
from pathlib import Path


def summarize(root):
    summaries = []
    for receipt in json.loads((root / "experiments.json").read_text()):
        path = root / receipt["feature"] / "comparison.json"
        comparison = json.loads(path.read_text()) if path.exists() else {}
        ratios = comparison.get("paired_ratios", [])
        numeric_keys = sorted({key for pair in ratios for key in pair})
        medians = {}
        for key in numeric_keys:
            values = [pair[key] for pair in ratios if pair.get(key) is not None]
            if values:
                medians[key] = statistics.median(values)
        arms = []
        for arm in comparison.get("arms", []):
            enrichment = arm.get("enrichment", {})
            source = (
                enrichment.get("table", {})
                .get("storage_status", {})
                .get("source_vectors")
                or {}
            )
            restarted = (
                arm.get("table_after_restart", {})
                .get("storage_status", {})
                .get("source_vectors")
                or {}
            )
            arms.append(
                {
                    "pair": arm["pair"],
                    "mode": arm["mode"],
                    "semantic_failed_queries": enrichment.get("semantic", {}).get(
                        "failed_queries"
                    ),
                    "full_text_failed_queries": enrichment.get("full_text", {}).get(
                        "failed_queries"
                    ),
                    "source_before_restart": source,
                    "source_after_restart": restarted,
                }
            )
        summaries.append(
            {
                "feature": receipt["feature"],
                "exit_code": receipt.get("exit_code"),
                "qualified": comparison.get("qualified", False)
                and receipt.get("exit_code") == 0,
                "binary_sha256": receipt["binary_sha256"],
                "median_paired_ratios": medians,
                "arms": arms,
            }
        )
    return summaries


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path)
    args = parser.parse_args()
    result = summarize(args.root)
    (args.root / "summary.json").write_text(json.dumps(result, indent=2) + "\n")
    columns = [
        "initial_ready_s",
        "update_ready_s",
        "semantic_qps",
        "semantic_p99_ms",
        "total_logical_bytes",
        "sampled_peak_rss_kib",
    ]
    print("experiment qualified " + " ".join(columns))
    for row in result:
        values = row["median_paired_ratios"]
        print(
            row["feature"],
            row["qualified"],
            " ".join(f"{values[key]:.3f}" if key in values else "-" for key in columns),
        )


if __name__ == "__main__":
    main()
