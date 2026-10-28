#!/usr/bin/env python3
"""Compare fixed manifests under equal limits; failed gates never select defaults."""

import argparse
import json
import gzip
import copy
import pathlib
from run_local import configuration_for_policy
from load_clients import service_diagnostics


def compare(baseline, candidate):
    failures = []
    for field in ("manifest_sha256", "process_limits", "enforced_limits"):
        if not baseline.get(field) or baseline.get(field) != candidate.get(field):
            failures.append("missing or unequal " + field)
    for label, receipt in (("baseline", baseline), ("candidate", candidate)):
        if not receipt.get("passed") or receipt.get("server_exit_code") != 0:
            failures.append(label + " qualification failed")
        if not receipt.get("binary_sha256"):
            failures.append(label + " binary identity missing")
    if baseline.get("binary_sha256") == candidate.get("binary_sha256"):
        runnable_comparison = candidate.get("policy") == "runnable"
        failfast_comparison = (
            runnable_comparison and baseline.get("policy") == "failfast"
        )
        configurations = []
        for receipt in (baseline, candidate):
            config = copy.deepcopy(receipt.get("configuration"))
            if config is None:
                failures.append("same-binary comparison configuration missing")
                continue
            config.get("pgwire", {}).pop("bind_port", None)
            if runnable_comparison:
                if receipt.get("policy") not in ("fixed", "failfast", "runnable"):
                    failures.append(
                        "runnable comparison requires fixed, failfast or runnable policies"
                    )
                    continue
                expected = configuration_for_policy(
                    receipt["manifest"], receipt["policy"]
                )
                expected.setdefault("pgwire", {})["enabled"] = True
                # The launcher can supply a default connection ceiling.
                expected["pgwire"].setdefault(
                    "max_connections", receipt["manifest"].get("pgwire_connections", 32)
                )
                if config != expected:
                    failures.append(
                        "runnable comparison configuration does not match manifest policy"
                    )
                config.get("admission", {}).pop("runnable", None)
                if failfast_comparison:
                    # Both complete policies were checked against the manifest
                    # before normalizing their intentional waiting difference.
                    config.get("admission", {}).get("query", {}).pop("waiting", None)
            else:
                config.get("admission", {}).get("query", {}).pop("waiting", None)
            configurations.append(config)
        if len(configurations) == 2 and configurations[0] != configurations[1]:
            failures.append(
                "same-binary comparison differs beyond "
                + (
                    "runnable scheduling and query waiting"
                    if failfast_comparison
                    else "runnable scheduling"
                    if runnable_comparison
                    else "query waiting"
                )
            )
    limits = candidate["manifest"]["comparison_thresholds"]
    baseline_phases = {p["name"]: p for p in baseline.get("phases", [])}
    candidate_phases = {p["name"]: p for p in candidate.get("phases", [])}
    expected = {p["name"] for p in candidate["manifest"]["phases"]}
    if set(baseline_phases) != expected or set(candidate_phases) != expected:
        failures.append("phase coverage does not match manifest")
    comparisons = []
    for name in sorted(expected & baseline_phases.keys() & candidate_phases.keys()):
        old, new = baseline_phases[name], candidate_phases[name]
        phase_policy = next(
            p for p in candidate["manifest"]["phases"] if p["name"] == name
        )
        minimum_service = phase_policy.get("thresholds", {}).get(
            "runnable_observed_slot_ns_min", 0
        )
        if type(minimum_service) is not int or minimum_service < 0:
            failures.append(name + ": invalid observed service threshold")
        elif minimum_service:
            for label, receipt, phase in (
                ("baseline", baseline, old),
                ("candidate", candidate, new),
            ):
                if receipt.get("policy") != "runnable":
                    continue
                prefix = label + " " + name + ": "
                try:
                    observed = service_diagnostics(
                        (phase.get("workload_before") or {}).get("runnable") or {},
                        (phase.get("workload_after") or {}).get("runnable") or {},
                    )
                except (RuntimeError, TypeError, AttributeError):
                    failures.append(prefix + "invalid runnable service snapshots")
                    continue
                recorded = phase.get("runnable_diagnostics")
                if (
                    not isinstance(recorded, dict)
                    or recorded.get("service") != observed
                ):
                    failures.append(
                        prefix + "service diagnostics differ from snapshots"
                    )
                if not observed["available"]:
                    failures.append(
                        prefix + "runnable service observations were not measured"
                    )
                elif sum(observed["counters"]["observed_slot_ns"]) < minimum_service:
                    failures.append(
                        prefix
                        + "runnable observed service was below the declared minimum"
                    )
        if (
            old["offered"] != new["offered"]
            or old.get("retries") != 0
            or new.get("retries") != 0
        ):
            failures.append(name + ": offered operations or retries differ")
        if (
            not old.get("success_p95_seconds")
            or old["successful_per_second"] <= 0
            or new.get("success_p95_seconds") is None
        ):
            failures.append(name + ": successful measurements missing")
            continue
        latency_ratio = new["success_p95_seconds"] / old["success_p95_seconds"]
        throughput_ratio = new["successful_per_second"] / old["successful_per_second"]
        by_kind = {}
        for kind in sorted(
            old.get("by_kind", {}).keys() & new.get("by_kind", {}).keys()
        ):
            previous, current = old["by_kind"][kind], new["by_kind"][kind]
            by_kind[kind] = {
                "baseline_successful": previous["successful"],
                "candidate_successful": current["successful"],
                "throughput_ratio": current["successful_per_second"]
                / previous["successful_per_second"]
                if previous["successful_per_second"]
                else None,
                "success_p95_ratio": current["success_p95_seconds"]
                / previous["success_p95_seconds"]
                if previous["success_p95_seconds"]
                and current["success_p95_seconds"] is not None
                else None,
            }
        comparisons.append(
            dict(
                name=name,
                success_p95_ratio=latency_ratio,
                throughput_ratio=throughput_ratio,
                by_kind=by_kind,
            )
        )
        if (
            name in ("low-load", "recovery")
            and latency_ratio > limits["low_load_p95_ratio_max"]
        ):
            failures.append(name + ": p95 regression")
        if throughput_ratio < limits["throughput_ratio_min"]:
            failures.append(name + ": throughput regression")
    return {
        "passed": not failures,
        "failures": failures,
        "comparisons": comparisons,
        "baseline_binary_sha256": baseline.get("binary_sha256"),
        "candidate_binary_sha256": candidate.get("binary_sha256"),
        "automatic_defaults_qualified": False,
        "remaining_default_gates": [
            "Linux and larger profiles",
            "whole-node ownership closure",
            "distributed protected progress",
        ],
    }


def read_receipt(path):
    if path.suffix == ".gz":
        with gzip.open(path, "rt") as source:
            return json.load(source)
    return json.loads(path.read_text())


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", type=pathlib.Path, required=True)
    parser.add_argument("--candidate", type=pathlib.Path, required=True)
    parser.add_argument("--output", type=pathlib.Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        parser.error("output already exists; use a fresh comparison path")
    result = compare(read_receipt(args.baseline), read_receipt(args.candidate))
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    return not result["passed"]


if __name__ == "__main__":
    raise SystemExit(main())
