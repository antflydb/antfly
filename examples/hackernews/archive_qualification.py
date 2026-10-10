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

"""Plan and qualify a pinned HN archive with budget reservations and release gates.

Default is a local dry run. Execution reserves its entire allowance permanently,
including failed/interrupted attempts, and invokes the regional temporary pod.
Reservations bound approved work, not GCP's delayed billing meter.
"""

import argparse
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path
import subprocess
import sys
import time
import uuid

REQUIRED_CHECKS = {
    "remote_bm25",
    "projected_highlights",
    "snapshot_pagination",
    "restart_scores",
    "restart_pagination",
    "metadata_filters_match_residual",
    "metadata_filters_after_restart",
    "empty_date_filter_before_and_after_restart",
    "concurrent_search",
    "exact_filter",
}


def percentile(values, fraction=0.95):
    if not values or any(not math.isfinite(v) or v < 0 for v in values):
        raise ValueError("Missing or invalid latency samples")
    return sorted(values)[math.ceil(len(values) * fraction) - 1]


def plan(config, inventory):
    if (
        config["project"] != inventory["project"]
        or config["bucket"] != inventory["bucket"]
    ):
        raise ValueError("Inventory authority differs")
    if inventory["prefix"] != config["source_prefix"] + "/archive":
        raise ValueError("Inventory source differs")
    for expected, actual in (
        ("expected_rows", "rows"),
        ("source_objects", "object_count"),
        ("source_bytes", "bytes"),
    ):
        if config[expected] != inventory[actual]:
            raise ValueError(f"Inventory {actual} differs")
    if len(inventory["objects"]) != inventory["object_count"]:
        raise ValueError("Incomplete inventory")
    if any(
        not o.get("generation") or not o.get("crc32c") for o in inventory["objects"]
    ):
        raise ValueError("Inventory lacks immutable object identities")
    r, b = config["resources"], config["budget"]
    if (
        r["cpu"] <= 0
        or r["memory_gib"] <= 0
        or not 0 < r["disk_gib"] <= 10
        or config["lifetime_seconds"] > 3600
        or config["lifetime_seconds"] < config["build_timeout_seconds"] + 120
        or config["repeats"] < 20
        or config["concurrency"] < 4
        or config["cycles"] < 2
    ):
        raise ValueError("Resource, lifetime or benchmark coverage bounds violated")
    compute = (
        config["lifetime_seconds"]
        / 3600
        * (
            r["cpu"] * b["cpu_hour_usd"]
            + r["memory_gib"] * b["memory_gib_hour_usd"]
            + r["disk_gib"] * b["disk_gib_hour_usd"]
        )
    )
    estimate = compute + b["other_run_allowance_usd"]
    if not 0 < estimate <= b["reservation_per_run_usd"]:
        raise ValueError("Run allowance does not cover estimated resources")
    if (
        b["max_runs"] < 1
        or b["prior_work_reserved_usd"] < 0
        or b["ceiling_usd"] > 150
        or b["ceiling_usd"] <= 0
        or b["prior_work_reserved_usd"] + b["max_runs"] * b["reservation_per_run_usd"]
        > b["ceiling_usd"]
    ):
        raise ValueError("Plan exceeds approved $150 ceiling")
    return {
        "compute_upper_estimate_usd": round(compute, 4),
        "run_estimate_with_allowance_usd": round(estimate, 4),
        "reservation_usd": b["reservation_per_run_usd"],
        "planned_total_reserved_usd": b["prior_work_reserved_usd"]
        + b["max_runs"] * b["reservation_per_run_usd"],
        "note": "Same-region reads; existing export reused. Actual billing and GCS operations must be reconciled separately.",
    }


def reserve(ledger_path, config, run_id):
    """Serialize attempts; never refund failed work or overwrite an existing budget."""
    b = config["budget"]
    with os.fdopen(os.open(ledger_path, os.O_RDWR | os.O_CREAT, 0o600), "r+") as file:
        fcntl.flock(file, fcntl.LOCK_EX)
        file.seek(0)
        raw = file.read()
        ledger = (
            json.loads(raw)
            if raw
            else {
                "project": config["project"],
                "budget": b,
                "runs": [],
            }
        )
        if ledger["project"] != config["project"] or ledger["budget"] != b:
            raise ValueError("Existing ledger budget differs")
        if len(ledger["runs"]) >= b["max_runs"]:
            raise ValueError(
                "Run limit exhausted; review recorded attempts before retrying"
            )
        reserved = b["prior_work_reserved_usd"] + sum(
            r["reserved_usd"] for r in ledger["runs"]
        )
        if reserved + b["reservation_per_run_usd"] > b["ceiling_usd"]:
            raise ValueError("Budget exhausted")
        ledger["runs"].append(
            {
                "id": run_id,
                "reserved_usd": b["reservation_per_run_usd"],
                "reserved_at_unix": time.time(),
            }
        )
        file.seek(0)
        json.dump(ledger, file, indent=2)
        file.write("\n")
        file.truncate()
        file.flush()
        os.fsync(file.fileno())


def assess(regional, config):
    failures = []
    c = config["criteria"]
    for mode, cycles in regional["runs"].items():
        if len(cycles) < config["cycles"]:
            failures.append(f"{mode}: missing cache/restart cycles")
        for i, report in enumerate(cycles):
            label = f"{mode}/{i + 1}"
            if report["row_count"] != config["expected_rows"]:
                failures.append(f"{label}: incorrect row count")
            missing = REQUIRED_CHECKS - set(report["verified"])
            if missing:
                failures.append(
                    f"{label}: missing correctness checks {sorted(missing)}"
                )
            samples = report["warm_samples_ms"]
            if len(samples) < config["repeats"]:
                failures.append(f"{label}: insufficient warm samples")
            metrics = {
                "warm_p95_ms": percentile(samples),
                "concurrent_max_ms": max(report["concurrent_search_ms"]),
                "restart_ms": report["after_restart_ms"],
                "cold_ms": report["first_search_ms"],
                "build_seconds": report["ready_seconds"],
            }
            if len(report["concurrent_search_ms"]) != config["concurrency"]:
                failures.append(f"{label}: insufficient concurrency coverage")
            for metric, value in metrics.items():
                if not math.isfinite(value) or value < 0 or value > c[metric]:
                    failures.append(f"{label}: {metric}={value} exceeds {c[metric]}")
            for name, result in report["metadata_filters"].items():
                if (
                    len(result["samples_ms"]) < config["repeats"]
                    or percentile(result["samples_ms"]) > c["filter_p95_ms"]
                ):
                    failures.append(
                        f"{label}: filter {name} exceeds sample/latency gate"
                    )
            if set(report["metadata_filters"]) != {
                "author",
                "created_at",
                "item_type",
                "points",
                "combined",
            }:
                failures.append(f"{label}: missing metadata filter workloads")
            for term in config["workload_terms"]:
                workload = report.get("workloads", {}).get(term)
                if (
                    not workload
                    or len(workload["samples_ms"]) < config["repeats"]
                    or percentile(workload["samples_ms"]) > c["warm_p95_ms"]
                ):
                    failures.append(
                        f"{label}: workload {term} exceeds sample/latency gate"
                    )
            cursor_depth = report.get("cursor_depth", {})
            if cursor_depth.get("hits", 0) < c[
                "cursor_min_hits"
            ] or not cursor_depth.get("samples_ms"):
                failures.append(f"{label}: archive cursor depth coverage missing")
            elif (
                percentile(cursor_depth["samples_ms"]) > c["cursor_p95_ms"]
                or cursor_depth.get("restart_ms", math.inf) > c["cursor_p95_ms"]
            ):
                failures.append(f"{label}: deep cursor latency/restart gate failed")
            peak = report.get("build_peak_rss_bytes")
            if (
                peak is None
                or peak
                > config["resources"]["memory_gib"] * 1024**3 * c["peak_rss_fraction"]
            ):
                failures.append(
                    f"{label}: peak RSS exceeds memory headroom gate or is missing"
                )
    if "indexed" not in regional["runs"]:
        failures.append("Indexed archive coverage missing")
    return {
        "passed": not failures,
        "failures": failures,
        "scope": "Single-table archive engine qualification. Current/history ingestion and public deployment are separate gates.",
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--config",
        type=Path,
        default=Path(__file__).with_name("archive-qualification.json"),
    )
    parser.add_argument("--inventory", type=Path, required=True)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--revision", required=True)
    parser.add_argument("--ledger", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args()
    config = json.loads(args.config.read_text())
    inventory = json.loads(args.inventory.read_text())
    estimate = plan(config, inventory)
    run_id = "hn-poc/archive-qualification/" + uuid.uuid4().hex
    r = config["resources"]
    command = [
        sys.executable,
        str(Path(__file__).with_name("regional.py")),
        "--binary",
        str(args.binary.resolve()),
        "--revision",
        args.revision,
        "--output",
        str(args.output / "regional"),
        "--run-prefix",
        run_id,
        "--modes",
        "indexed",
        "--cpu",
        str(r["cpu"]),
        "--memory",
        f"{r['memory_gib']}Gi",
        "--disk",
        f"{r['disk_gib']}Gi",
    ]
    for key, flag in (
        ("project", "project"),
        ("cluster", "cluster"),
        ("region", "region"),
        ("bucket", "bucket"),
        ("source_prefix", "source-prefix"),
        ("expected_rows", "expected-rows"),
        ("lifetime_seconds", "lifetime"),
        ("build_timeout_seconds", "build-timeout"),
        ("cycles", "cycles"),
        ("repeats", "repeats"),
        ("concurrency", "concurrency"),
    ):
        command.extend(["--" + flag, str(config[key])])
    command.extend(["--cursor-pages", str(config["cursor_pages"])])
    command.extend(
        [
            "--cursor-retention-ms",
            "3600000",
            "--workload-terms",
            *config["workload_terms"],
        ]
    )
    print(json.dumps({"plan": estimate, "command": command}, indent=2), flush=True)
    if not args.execute:
        return
    if not args.binary.is_file():
        parser.error("Binary must exist before reserving cloud work")
    # Check live object versions before spending a run reservation. The regional
    # engine separately fences source/publication versions during serving.
    from archive_inventory import verify_inventory

    verify_inventory(inventory)
    args.output.mkdir(parents=True, exist_ok=False)
    reserve(args.ledger, config, run_id)
    (args.output / "plan.json").write_text(
        json.dumps(
            {
                "config": config,
                "estimate": estimate,
                "run_prefix": run_id,
                "inventory_sha256": hashlib.sha256(
                    args.inventory.read_bytes()
                ).hexdigest(),
            },
            indent=2,
        )
        + "\n"
    )
    (args.output / "inventory.json").write_bytes(args.inventory.read_bytes())
    completed = subprocess.run(command, check=False)
    if completed.returncode:
        verdict = {
            "passed": False,
            "failures": [
                f"Regional run exited {completed.returncode}; inspect retained logs"
            ],
            "run_prefix": run_id,
        }
    else:
        verdict = assess(
            json.loads((args.output / "regional" / "regional.json").read_text()), config
        )
    if completed.returncode:
        verdict["engine_failures"] = [
            json.loads(path.read_text())
            for path in sorted((args.output / "regional").glob("*-failure.json"))
        ]
    (args.output / "verdict.json").write_text(json.dumps(verdict, indent=2) + "\n")
    print(json.dumps(verdict, indent=2))
    raise SystemExit(0 if verdict["passed"] else 1)


if __name__ == "__main__":
    main()
