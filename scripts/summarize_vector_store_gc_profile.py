#!/usr/bin/env python3
"""Extract source GC stage profiles without treating absent counters as zero."""

import argparse
import json
from pathlib import Path

TOTALS = (
    "catalog_metadata_bytes_shared",
    "catalog_metadata_bytes_copied",
    "collection_apply_visits_avoided",
    "inventory_rows_scanned",
    "inventory_updates",
    "inventory_update_ns",
    "collection_locked_ns",
    "collection_setup_ns",
    "collection_plan_ns",
    "collection_mark_ns",
    "collection_mark_merge_ns",
    "collection_copy_ns",
    "collection_publish_ns",
    "preparation_ns",
    "checkpoint_ns",
    "collection_active_scan_turns",
    "collection_active_scan_pause_ns",
    "collection_rescued_payloads",
    "deduplicated_reappend_payloads",
    "deduplicated_reappend_bytes",
)
MAXIMA = (
    "collection_max_locked_ns",
    "collection_max_setup_ns",
    "collection_max_plan_ns",
    "collection_mark_max_step_ns",
    "collection_mark_max_merge_ns",
    "collection_max_copy_ns",
    "collection_max_publish_ns",
)


def source(table):
    return (table or {}).get("storage_status", {}).get("source_vectors") or {}


def summarize(root):
    results = []
    decoder = json.JSONDecoder()
    for arm in json.loads((root / "ab-runs.json").read_text()):
        name = f"{arm['case']}-{arm['pair']}-{arm['mode']}"
        directory = root / name
        observations = []
        log = directory / "vdbbench-framework.log"
        if log.exists():
            for line in log.read_text().splitlines():
                if "antfly_bench_status " not in line:
                    continue
                data, _ = decoder.raw_decode(line.split("antfly_bench_status ", 1)[1])
                stats = (data.get("table") or {}).get("source_vectors") or {}
                if stats:
                    observations.append(
                        {
                            "phase": data["phase"],
                            "log_time": line[:23],
                            "source_vectors": stats,
                        }
                    )
        phase_profiles = []
        churn = directory / "source-churn.json"
        maxima = {}
        if churn.exists():
            for phase in json.loads(churn.read_text()):
                before, after = (
                    source(phase.get("table_before")),
                    source(phase.get("table")),
                )
                count = after.get("prepared_payloads", -1) - before.get(
                    "prepared_payloads", -1
                )
                valid = (
                    "prepared_payloads" in before
                    and "prepared_payloads" in after
                    and 0 <= count <= phase["rows"]
                )
                if phase["phase"].startswith("update-"):
                    valid = valid and count == phase["rows"]
                delta = {
                    key: after[key] - before[key]
                    if valid
                    and key in before
                    and key in after
                    and after[key] >= before[key]
                    else None
                    for key in TOTALS
                }
                for stats in (before, after):
                    for key in MAXIMA:
                        if key in stats:
                            maxima[key] = max(maxima.get(key, 0), stats[key])
                phase_profiles.append(
                    {"phase": phase["phase"], "sample_valid": valid, "delta": delta}
                )
        complete = bool(phase_profiles) and all(
            p["sample_valid"] and all(v is not None for v in p["delta"].values())
            for p in phase_profiles
        )
        totals = {
            key: sum(p["delta"][key] for p in phase_profiles) if complete else None
            for key in TOTALS
        }
        results.append(
            {
                "arm": name,
                "workload_passed": arm.get("exit_code") == 0
                and not arm.get("invalid_reason"),
                "ingest_observations": observations,
                "churn_source_profile_complete": complete,
                "churn_phases": phase_profiles,
                "churn_totals": totals,
                "max_observed_through_churn": maxima,
            }
        )
    (root / "gc-stage-profile.json").write_text(json.dumps(results, indent=2) + "\n")
    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path)
    args = parser.parse_args()
    for result in summarize(args.root):
        print(
            result["arm"],
            "ingest_samples=" + str(len(result["ingest_observations"])),
            "complete_churn_profile=" + str(result["churn_source_profile_complete"]),
        )
