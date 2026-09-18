"""Generate preselected candidate-policy cells; never attest execution or qualification."""

from __future__ import annotations

import argparse
import copy
from pathlib import Path

import workload_cluster_qualification as cluster
import workload_qualification as q
import workload_scenarios as scenarios

MIB = 1 << 20


def candidate_config(tier):
    cpus, memory, _ = q.TIERS[tier]
    config = copy.deepcopy(q.release_plan(tier)["arms"]["candidate"]["config"])
    config["health_metrics_interval_ms"] = 250
    config["admission"].update(
        ingress={
            "max_requests": 512,
            "max_retained_bytes": memory // 8,
            "control_requests": 2,
            "control_retained_bytes": 256 << 10,
            "recovery_requests": 4,
            "recovery_retained_bytes": 4 * MIB,
        },
        session_max_retained_bytes=64 * MIB,
        transaction_completion_bytes=16 * MIB,
        read_execution={
            "max_runnable_tasks": cpus,
            "max_outstanding_tasks": 160,
            "max_queued_tasks": 160,
            "max_wait_ms": 1000,
            "max_working_bytes": memory // 64,
            "max_suspended_io": cpus * 8,
            "max_scan_state_bytes": 0,
            "max_scan_snapshot_ms": 30000,
            "protected": {
                "max_runnable_tasks": 0,
                "max_outstanding_tasks": 0,
                "max_working_bytes": 0,
                "max_transition_tasks": 0,
                "max_transition_bytes": 0,
            },
        },
        dense_execution={"max_runnable_tasks": 0},
        remote_attempt_worker={"max_attempts": 0, "max_bytes": 0},
        remote_attempt_coordinator={
            "max_attempts": 0,
            "max_bytes": 0,
            "max_destination_attempts": 0,
            "max_destinations": 0,
        },
    )
    return config


def telemetry(config):
    admission = config["admission"]
    spec = {
        "interval_seconds": 0.25,
        "ceilings": {},
        "expected": {},
        "queue_metrics": [],
        "recovery_queue_bound": 0,
    }
    for kind in ("query", "write"):
        scope = f"antfly_admission_{kind}_"
        item = admission[kind]
        waiting = item["waiting"]
        spec["ceilings"].update(
            {
                scope + "in_flight_requests": item["max_concurrent_requests"],
                scope + "queued_requests": waiting["max_queued_requests"],
                scope + "retained_bytes": waiting["max_retained_bytes"],
                scope + "queued_bytes": waiting["max_queued_bytes"],
            }
        )
        spec["expected"].update(
            {
                scope + "queue_capacity_requests": waiting["max_queued_requests"],
                scope + "retained_capacity_bytes": waiting["max_retained_bytes"],
            }
        )
        spec["queue_metrics"].append(scope + "queued_requests")
    if "read_execution" in admission:
        read = admission["read_execution"]
        scope = "antfly_read_execution_"
        for name, field in (
            ("runnable", "max_runnable_tasks"),
            ("outstanding", "max_outstanding_tasks"),
            ("queued", "max_queued_tasks"),
            ("working_bytes", "max_working_bytes"),
            ("suspended_io", "max_suspended_io"),
        ):
            spec["ceilings"][scope + name] = read[field]
        spec["expected"].update(
            {
                scope + "enabled": 1,
                scope + "runnable_limit": read["max_runnable_tasks"],
                scope + "outstanding_limit": read["max_outstanding_tasks"],
                scope + "queue_limit": read["max_queued_tasks"],
                scope + "working_bytes_limit": read["max_working_bytes"],
                scope + "suspended_io_limit": read["max_suspended_io"],
                "antfly_recovery_ingress_enabled": 1,
                "antfly_recovery_ingress_capacity_requests": admission["ingress"][
                    "recovery_requests"
                ],
                "antfly_recovery_ingress_capacity_bytes": admission["ingress"][
                    "recovery_retained_bytes"
                ],
            }
        )
        spec["ceilings"].update(
            {
                "antfly_recovery_ingress_outstanding_requests": admission["ingress"][
                    "recovery_requests"
                ],
                "antfly_recovery_ingress_retained_bytes": admission["ingress"][
                    "recovery_retained_bytes"
                ],
            }
        )
        spec["queue_metrics"].append(scope + "queued")
    return spec


def performance_plan(tier, *, operators=False):
    plan = q.release_plan(tier)
    plan["comparison"] = "candidate_fixed_policy_vs_fixed_foreground_baseline"
    plan["policy_version"] = 1
    plan["arms"]["candidate"]["config"] = candidate_config(tier)
    for arm in plan["arms"].values():
        arm["telemetry"] = telemetry(arm["config"])
    if operators:
        plan["workloads"] = [
            scenarios.mixed_fixture(plan["documents"], ratio, 32) for ratio in (90, 50)
        ]
    plan["note"] = (
        "Preselected candidate fixed policy, distinct from identical-config overhead cells. "
        "Legacy baseline64f retains the same foreground80/queue160 limits; candidate also enables "
        "bounded ingress, reserved recovery, session/completion bytes and general read scheduling. "
        "Default native LSM: protected LMDB probes and snapshot suspension explicitly disabled. "
        "Both arms require pinned ReleaseFast artifacts and unchanged Cloud resource/timing gates. "
        "250ms candidate health collection still requires observed <=1s source age; the older "
        "baseline may lack fresh ownership telemetry, which remains unavailable, never waived. "
        "These lookup/mixed cells do not exercise durable transaction decisions, remote attempts, "
        "sustained compaction, long-operator isolation, slow consumers or the retained vector datasets. "
        "Repeated identical writes are not sustained ingestion. Operator fixtures are separate "
        "deterministic data, not the quoted vector/graph benchmark datasets. No result is claimed."
    )
    return plan


def correctness_plan():
    plan = cluster.template()
    plan["policy_version"] = 1
    plan["request_timeout"] = 3
    plan["max_schedule_lateness_seconds"] = 5
    config = candidate_config("starter")
    admission = config["admission"]
    admission["ingress"].update(
        max_requests=24,
        max_retained_bytes=16 * MIB,
        recovery_requests=2,
        recovery_retained_bytes=MIB,
    )
    for kind in ("query", "write"):
        admission[kind] = {
            "max_concurrent_requests": 4,
            "waiting": {
                "max_queued_requests": 8,
                "max_queued_bytes": MIB,
                "max_retained_bytes": 8 * MIB,
                "max_wait_ms": 1000,
            },
        }
    admission["session_max_retained_bytes"] = 8 * MIB
    admission["transaction_completion_bytes"] = 8 * MIB
    admission["read_execution"].update(
        max_runnable_tasks=2,
        max_outstanding_tasks=8,
        max_queued_tasks=8,
        max_working_bytes=4 * MIB,
        max_suspended_io=2,
    )
    admission["remote_attempt_worker"] = {
        "max_attempts": 8,
        "max_bytes": 2 * MIB,
        "max_run_ms": 5000,
    }
    admission["remote_attempt_coordinator"] = {
        "max_attempts": 4,
        "max_bytes": 2 * MIB,
        "max_destination_attempts": 2,
        "max_destinations": 2,
        "max_run_ms": 5000,
    }
    plan["nodes"][0]["config"] = {"health_metrics_interval_ms": 250}
    for node in plan["nodes"][1:]:
        node["config"] = copy.deepcopy(config)
    plan["nodes"][1]["proxy_api"] = True
    root = "/db/v1/tables/policy_fixture"

    def request(method, path, body, checks, status=200, is_write=False):
        return {
            "action": "request",
            "node": "api",
            "method": method,
            "path": path,
            "body": body,
            "is_write": is_write,
            "expect": {"status": status, "checks": checks},
        }

    def check(path, value):
        return {"path": path, "equals": value}

    plan["setup"] = [
        request(
            "POST",
            root,
            {
                "num_shards": 1,
                "storage": {
                    "transaction_recovery": {
                        "protocol_version": 1,
                        "max_count": 4,
                        "max_bytes": 4 * MIB,
                        "max_transaction_bytes": MIB,
                    }
                },
            },
            [check(["name"], "policy_fixture")],
            is_write=True,
        ),
        request(
            "POST",
            root + "/batch",
            {
                "inserts": {"a": {"marker": "durable-a"}},
                "sync_level": "full_index",
            },
            [check(["inserted"], 1)],
            status=201,
            is_write=True,
        ),
    ]
    lookup = request(
        "GET", root + "/documents/a", None, [check(["marker"], "durable-a")]
    )
    direct = {
        **lookup,
        "node": "data",
        "via_proxy": True,
        "expect": {"transport_error": True},
    }
    plan["actions"] = [
        {
            "at": 0,
            "action": "discover",
            "node": "data",
            "coordinator": 3,
            "id": "worker",
        },
        {"at": 1, "action": "proxy_checkpoint", "node": "data", "id": "routed"},
        {"at": 2, **lookup},
        {
            "at": 3,
            "action": "assert_proxy",
            "node": "data",
            "checkpoint": "routed",
            "minimums": {
                "forwarded_upstream_bytes": 1,
                "forwarded_downstream_bytes": 1,
            },
            "path_prefixes_include": ["/internal/"],
        },
        {"at": 4, "action": "partition", "node": "data"},
        {"at": 5, **direct},
        {"at": 6, "action": "heal", "node": "data"},
        {"at": 7, **lookup},
        {"at": 8, "action": "kill", "node": "api"},
        {"at": 9, "action": "restart", "node": "api"},
        {"at": 14, **lookup},
    ]
    plan["note"] = (
        "Small local correctness cell; no Cloud machine or performance claim. Pin a fresh homogeneous "
        "candidate binary/SHA/revision before running. Tiny finite worker/coordinator journals, reserved "
        "recovery, ingress, sessions, completion and read budgets are opt-in on both stable node IDs. "
        "Worker8 records also bounds closure identities; terminal tombstones need not become zero. "
        "Table opts into immutable recovery policy; 1MiB transaction plus64KiB fits each half of8MiB "
        "completion reserve. This checks signed discovery, advertised routing, API partition/heal and "
        "API restart with existing committed data. It does not prove durable-decision interruption, "
        "coordinator reconciliation, unknown-write resolution or replicated failover; those require "
        "separate semantic fault cells. Policy/config parsing and live candidate behavior remain pending."
    )
    return plan


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--operators",
        action="store_true",
        help="materialize checked4096-row graph/text/aggregation mixes",
    )
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    for tier in q.TIERS:
        q.save(
            args.output / f"{tier}.json",
            performance_plan(tier, operators=args.operators),
        )
    q.save(args.output / "local-correctness.json", correctness_plan())


if __name__ == "__main__":
    main()
