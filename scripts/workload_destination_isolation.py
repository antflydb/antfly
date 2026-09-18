#!/usr/bin/env python3
"""Four owned local processes; prove distinct placement before destination loss."""

from __future__ import annotations

import argparse
import copy
import json
import shutil
import time
from pathlib import Path

import workload_cluster_qualification as runner
import workload_fixed_policy as policies


def make_plan(binary, revision, optimization, setup_only=False):
    plan = policies.correctness_plan()
    first, api = plan["nodes"][1:]
    first["name"] = "worker_a"
    second = copy.deepcopy(first)
    second.update(name="worker_b", node_id=4)
    plan["nodes"] = [plan["nodes"][0], first, second, api]
    for node in plan["nodes"][1:]:
        node["config"]["admission"]["remote_attempt_coordinator"].update(
            max_attempts=2,
            max_destination_attempts=1,
            max_destinations=2,
            max_run_ms=1000,
        )
        node["config"]["admission"]["remote_attempt_worker"]["max_run_ms"] = 1000
        if node.get("proxy_api"):
            node["proxy_capture_response_bytes"] = 16384
    plan["artifacts"]["candidate"].update(
        binary=str(binary),
        revision=revision,
        sha256=runner.q.checksum(binary),
        optimization=optimization,
    )
    plan["setup"], plan["actions"] = [], [{"action": "ready", "node": "api", "at": 0}]
    plan.update(
        driver="workload_destination_isolation.py",
        setup_only=setup_only,
        max_tables=6,
        placement_timeout_seconds=30,
        note="Single-replica tables; distinct live voter and advertised proxy path proof required. No write replay, no performance/replicated availability claim.",
    )
    return plan


def placement(snapshot, table_id, worker_ids):
    """Require one actual live voter and leader, not merely requested placement."""
    tables = [
        row for row in snapshot.get("tables", []) if int(row["table_id"]) == table_id
    ]
    ranges = [
        row for row in snapshot.get("ranges", []) if int(row["table_id"]) == table_id
    ]
    if (
        len(tables) != 1
        or tables[0].get("desired_replica_count") != 1
        or len(ranges) != 1
    ):
        return None
    group = ranges[0]["group_id"]
    voters = [
        (store["node_id"], report)
        for store in snapshot.get("stores", [])
        if store.get("live")
        for report in store.get("group_statuses", [])
        if report["group_id"] == group and report.get("local_voter")
    ]
    if len(voters) != 1:
        return None
    node, report = voters[0]
    if (
        node not in worker_ids
        or not report.get("local_leader")
        or report.get("voter_count") != 1
        or not report.get("voter_set_known")
        or report.get("joint_consensus")
        or report.get("raft_applied_index", 0) <= 0
    ):
        return None
    return {
        "destination": node,
        "group_id": group,
        "table_id": table_id,
        "report": report,
    }


class IsolationCluster(runner.Cluster):
    def call(
        self,
        method,
        path,
        body=None,
        *,
        node="api",
        status=200,
        checks=(),
        timeout=None,
    ):
        action = {
            "method": method,
            "path": path,
            "body": body,
            "is_write": method != "GET",
            "expect": {"status": status, "checks": list(checks)},
        }
        result = runner.request(
            self.ports[node]["api"], action, timeout or self.plan["request_timeout"]
        )
        self.record(
            {
                "event": "isolation_request",
                "node": node,
                "method": method,
                "path": path,
                **result,
            }
        )
        if not result["passed"] or result.get("unknown_write_outcome"):
            raise RuntimeError(
                f"request failed; no mutation replay: {method} {path} status={result.get('status')}"
            )
        return (
            json.loads(result["body"])
            if result.get("body", "").startswith(("{", "["))
            else result["body"]
        )

    def placed(self, table, table_id):
        deadline = time.monotonic() + self.plan["placement_timeout_seconds"]
        ids = {self.nodes[name]["node_id"] for name in ("worker_a", "worker_b")}
        while time.monotonic() < deadline:
            snapshot = self.call(
                "GET",
                "/metadata/v1/admin/snapshot",
                node="metadata",
                timeout=min(2, max(0.01, deadline - time.monotonic())),
            )
            proof = placement(snapshot, table_id, ids)
            if proof:
                proof["table"] = table
                self.record({"event": "verified_single_destination", **proof})
                return proof
            time.sleep(0.2)
        raise TimeoutError("actual single-voter placement unavailable")

    def lookup(self, proof, status=200):
        return self.call(
            "GET",
            f"/db/v1/tables/{proof['table']}/documents/a",
            status=status,
            checks=(
                [{"path": ["marker"], "equals": proof["table"]}]
                if status == 200
                else []
            ),
        )

    def records(self, expected):
        action = copy.deepcopy(policies.reconciliation_plan()["actions"][4])
        action["expected"]["antfly_remote_attempt_coordinator_records"] = expected
        action["ceilings"]["antfly_remote_attempt_coordinator_records"] = 2
        result = runner.poll_metrics(
            self.ports["api"]["health"],
            action,
            time.monotonic(),
            lambda row: self.record({"event": "isolation_metrics_sample", **row}),
        )
        self.record({"event": "isolation_metrics_result", **result})
        if not result["passed"]:
            raise RuntimeError("strict coordinator metrics assertion failed")


def run(binary, revision, optimization, output, setup_only=False):
    plan = make_plan(binary, revision, optimization, setup_only)
    runner.validate(plan)
    output.mkdir(parents=True, exist_ok=False)
    runner.q.save(output / "plan.json", plan)
    for module in (
        runner,
        runner.q,
        runner.scenarios,
        runner.q.vectors,
        runner.q.evidence,
        runner.proxy_module,
        runner.proxy_module.evidence,
        runner.attempt_evidence,
        policies,
    ):
        shutil.copy2(module.__file__, output / Path(module.__file__).name)
    shutil.copy2(__file__, output / Path(__file__).name)
    runner.q.save(
        output / "host.json",
        {
            "platform": runner.q.platform.platform(),
            "purpose": "destination isolation correctness",
            "resource_envelope_enforced": False,
        },
    )
    cluster, failure, cleanup, selected = None, None, [], {}
    try:
        (output / "artifacts").mkdir()
        artifact = output / "artifacts/candidate"
        shutil.copyfile(binary, artifact)
        if runner.q.checksum(artifact) != plan["artifacts"]["candidate"]["sha256"]:
            raise ValueError("copied binary checksum mismatch")
        artifact.chmod(0o500)
        cluster = IsolationCluster(plan, output)
        for node in plan["nodes"]:
            cluster.start(node["name"])
        cluster.call(
            "POST",
            "/db/v1/tablespaces/isolation_single",
            {"placement_policy_json": json.dumps({"desired_replica_count": 1})},
            status=201,
        )
        template = policies.correctness_plan()["setup"][0]["body"]
        for index in range(plan["max_tables"]):
            table = f"isolation_{index}"
            created = cluster.call(
                "POST",
                f"/db/v1/tables/{table}",
                {**template, "tablespace_name": "isolation_single"},
            )
            proof = cluster.placed(table, int(created["table_id"]))
            selected.setdefault(proof["destination"], proof)
            if len(selected) == 2:
                break
        if (
            len(selected) != 2
            or len({row["group_id"] for row in selected.values()}) != 2
        ):
            raise RuntimeError(
                "bounded table set did not produce two distinct actual destinations"
            )
        for proof in selected.values():
            cluster.call(
                "POST",
                f"/db/v1/tables/{proof['table']}/batch",
                {
                    "inserts": {"a": {"marker": proof["table"]}},
                    "sync_level": "full_index",
                },
                status=201,
                checks=[{"path": ["inserted"], "equals": 1}],
            )
            name = next(
                name
                for name in ("worker_a", "worker_b")
                if cluster.nodes[name]["node_id"] == proof["destination"]
            )
            proof["node"] = name
            cluster.fault(
                {"node": name, "action": "proxy_checkpoint", "id": "placement"}
            )
            cluster.lookup(proof)
            cluster.fault(
                {
                    "node": name,
                    "action": "assert_proxy",
                    "checkpoint": "placement",
                    "minimums": {
                        "forwarded_upstream_bytes": 1,
                        "forwarded_downstream_bytes": 1,
                    },
                    "path_prefixes_include": [
                        f"/internal/v1/groups/{proof['group_id']}/"
                    ],
                }
            )
        cluster.record(
            {
                "event": "verified_two_destinations",
                "destinations": list(selected.values()),
            }
        )
        if not setup_only:
            cluster.records(0)
            failed, healthy = list(selected.values())
            for prior in (failed, healthy):
                current = cluster.placed(prior["table"], prior["table_id"])
                if any(
                    current[key] != prior[key] for key in ("destination", "group_id")
                ):
                    raise RuntimeError("placement changed before fault")
            cluster.fault(
                {
                    "node": healthy["node"],
                    "action": "proxy_checkpoint",
                    "id": "healthy_during_loss",
                }
            )
            cluster.fault(
                {"node": failed["node"], "action": "proxy_checkpoint", "id": "loss"}
            )
            cluster.fault({"node": failed["node"], "action": "drop_response"})
            cluster.lookup(failed, status=503)
            cluster.fault(
                {
                    "node": failed["node"],
                    "action": "assert_proxy",
                    "checkpoint": "loss",
                    "minimums": {
                        "forwarded_upstream_bytes": 1,
                        "dropped_response_bytes": 1,
                    },
                }
            )
            cluster.records(1)
            for _ in range(3):
                cluster.lookup(healthy)
            cluster.fault(
                {
                    "node": healthy["node"],
                    "action": "assert_proxy",
                    "checkpoint": "healthy_during_loss",
                    "minimums": {
                        "forwarded_upstream_bytes": 1,
                        "forwarded_downstream_bytes": 1,
                    },
                    "path_prefixes_include": [
                        f"/internal/v1/groups/{healthy['group_id']}/"
                    ],
                }
            )
            cluster.records(1)
            cluster.fault({"node": failed["node"], "action": "heal"})
            cluster.lookup(failed)
            cluster.records(0)
    except BaseException as error:  # noqa: BLE001
        # Retain receipts and stop every owned process, including on interruption.
        failure = f"{type(error).__name__}: {error}"
    finally:
        if cluster:
            cleanup = cluster.close()
        receipt = {
            "passed": failure is None and not cleanup,
            "failure": failure,
            "cleanup_errors": cleanup,
            "setup_only": setup_only,
            "fault_correctness_passed": failure is None
            and not cleanup
            and not setup_only,
            "performance_qualification": False,
            "release_qualified": False,
            "artifact": plan["artifacts"]["candidate"],
            "selected": list(selected.values()),
        }
        runner.q.save(output / "receipt.json", receipt)
        paths = [
            path
            for path in output.rglob("*")
            if path.is_file()
            and path.suffix in (".py", ".json", ".jsonl", ".log")
            and not any(
                part in {"data", "replicas", "snapshots", "artifacts"}
                for part in path.relative_to(output).parts
            )
        ]
        runner.q.save(
            output / "checksums.json",
            {str(path.relative_to(output)): runner.q.checksum(path) for path in paths},
        )
    print(json.dumps(receipt, indent=2))
    return 0 if receipt["passed"] else 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--revision", required=True)
    parser.add_argument(
        "--optimization", choices=("Debug", "ReleaseFast"), default="Debug"
    )
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--setup-only", action="store_true")
    args = parser.parse_args()
    raise SystemExit(
        run(args.binary, args.revision, args.optimization, args.output, args.setup_only)
    )
