#!/usr/bin/env python3
"""Six small native processes: metadata leader loss, quorum loss, and recovery.

Reuses the owned-process runner, receipts, and bounded HTTP implementation.
This is correctness coverage, not a performance or process-memory qualification.
No mutation is retried, including the deliberately ambiguous no-quorum write.
"""

from __future__ import annotations

import argparse
import copy
import json
import re
import shutil
import time
from pathlib import Path

import workload_cluster_qualification as runner

METADATA = ("metadata1", "metadata2", "metadata3")
DATA = ("data1", "data2", "data3")


def make_plan(binary, revision, optimization):
    base = json.loads(
        (
            Path(__file__).parent / "workload-fixed-policy-plans/local-correctness.json"
        ).read_text()
    )
    admission = copy.deepcopy(base["nodes"][1]["config"]["admission"])
    # This cell is independent of remote-attempt coordinator qualification.
    admission.pop("remote_attempt_coordinator")
    admission.pop("remote_attempt_worker")
    admission["transaction_completion_bytes"] = 0
    nodes = [
        {
            "name": name,
            "role": "metadata",
            "metadata_id": index + 1,
            "artifact": "candidate",
            "config": {"health_metrics_interval_ms": 250},
        }
        for index, name in enumerate(METADATA)
    ]
    nodes += [
        {
            "name": name,
            "role": "data",
            "node_id": index + 11,
            "store_role": "data",
            "metadata": METADATA[0],
            "artifact": "candidate",
            "config": {
                "health_metrics_interval_ms": 250,
                "admission": copy.deepcopy(admission),
            },
        }
        for index, name in enumerate(DATA)
    ]
    return {
        "schema": 1,
        "purpose": "fault_correctness",
        "startup_timeout": 90,
        "request_timeout": 4,
        "max_schedule_lateness_seconds": 5,
        "artifacts": {
            "candidate": {
                "binary": str(binary),
                "revision": revision,
                "sha256": runner.q.checksum(binary),
                "optimization": optimization,
            }
        },
        "nodes": nodes,
        "setup": [],
        "actions": [{"action": "ready", "node": METADATA[0], "at": 0}],
        "driver": "workload_metadata_quorum.py",
        "scenario": [
            "verify three voters",
            "seed replicated table and document",
            "kill observed metadata leader",
            "verify new leader and write/read",
            "kill another voter",
            "submit metadata mutation once without quorum",
            "restart both original roots",
            "verify quorum and write/read",
            "observe ambiguous mutation without replay",
        ],
        "limitations": [
            "Scheduling ceilings are not an enforced whole-process memory envelope",
            "No performance qualification, data-replica loss, or durable transaction crash claim",
        ],
    }


class ReplicatedCluster(runner.Cluster):
    def __init__(self, plan, output):
        super().__init__(plan, output)
        self.defer_ready = True
        self.metadata_identity = None
        peer_config = {
            "raft_urls": {
                str(i + 1): f"http://127.0.0.1:{self.ports[n]['raft']}"
                for i, n in enumerate(METADATA)
            },
            "orchestration_urls": {
                str(i + 1): f"http://127.0.0.1:{self.ports[n]['api']}"
                for i, n in enumerate(METADATA)
            },
        }
        for node in self.nodes.values():
            node["config"]["metadata"] = copy.deepcopy(peer_config)
        runner.q.save(output / "rendered-plan.json", plan)

    def ready(self, name):
        if not self.defer_ready:
            super().ready(name)

    def start(self, name, artifact=None):
        # The shared runner deliberately has no arbitrary CLI escape hatch.
        # Extend only the concrete native topology flags in this single-threaded
        # driver; restore its command builder even if process creation fails.
        original = runner.node_command

        def command(node, binary, directory, ports, all_ports):
            argv = original(node, binary, directory, ports, all_ports)
            if node["role"] == "metadata":
                argv += ["--id", str(node["metadata_id"])]
            else:
                for peer in METADATA[1:]:
                    argv += [
                        "--metadata-api",
                        f"http://127.0.0.1:{all_ports[peer]['api']}",
                    ]
            return argv

        runner.node_command = command
        try:
            super().start(name, artifact)
        finally:
            runner.node_command = original

    def check_processes(self):
        for name, process in self.processes.items():
            if process.poll() is not None and name not in self.expected_stopped:
                raise RuntimeError(f"unexpected {name} exit {process.returncode}")

    def call(
        self, node, method, path, body=None, expected=200, checks=(), timeout=None
    ):
        self.check_processes()
        action = {
            "method": method,
            "path": path,
            "body": body,
            "is_write": method != "GET",
            "expect": {"status": expected, "checks": list(checks)},
        }
        result = runner.request(
            self.ports[node]["api"],
            action,
            timeout or self.plan["request_timeout"],
            raw=not checks,
        )
        self.record(
            {
                "event": "semantic_request",
                "node": node,
                "method": method,
                "path": path,
                **result,
            }
        )
        return result

    def checked(self, *args, **kwargs):
        result = self.call(*args, **kwargs)
        if not result["passed"] or result.get("unknown_write_outcome"):
            raise RuntimeError(f"semantic request failed; no mutation replay: {result}")
        return json.loads(result["body"])

    def leader(self, live, previous=None, timeout=60):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            rows = {}
            for node in live:
                result = self.call(
                    node,
                    "GET",
                    "/metadata/v1/status",
                    timeout=min(1, max(0.01, deadline - time.monotonic())),
                )
                if not result["passed"]:
                    continue
                row = json.loads(result["body"])
                if (
                    row.get("metadata_raft_voter_count") == 3
                    and row.get("metadata_raft_local_voter") is True
                    and row.get("metadata_raft_local_node_id")
                    == self.nodes[node]["metadata_id"]
                ):
                    rows[node] = row
            leaders = [
                node
                for node, row in rows.items()
                if row.get("metadata_raft_role") == "leader"
            ]
            if len(rows) == len(live) and len(leaders) == 1:
                leader = leaders[0]
                identity = self.nodes[leader]["metadata_id"]
                fingerprints = {
                    json.dumps(row.get("metadata_raft_voter_set_fingerprint"))
                    for row in rows.values()
                }
                groups = {
                    (row.get("metadata_group_id"), row.get("metadata_incarnation"))
                    for row in rows.values()
                }
                if (
                    leader != previous
                    and len(fingerprints) == 1
                    and "null" not in fingerprints
                    and len(groups) == 1
                    and None not in next(iter(groups))
                    and all(
                        row.get("metadata_raft_leader_id") == identity
                        for row in rows.values()
                    )
                ):
                    observed = (next(iter(groups)), next(iter(fingerprints)))
                    if (
                        self.metadata_identity is not None
                        and observed != self.metadata_identity
                    ):
                        raise AssertionError(
                            "metadata group/incarnation/voter identity changed"
                        )
                    self.metadata_identity = observed
                    self.record(
                        {
                            "event": "verified_metadata_leader",
                            "leader": leader,
                            "statuses": rows,
                        }
                    )
                    return leader
            time.sleep(0.1)
        raise TimeoutError("three-voter metadata leader agreement deadline")

    def replicas(self, metadata_node, timeout=60):
        """Require reported live voter state, not merely requested placement."""
        deadline = time.monotonic() + timeout
        expected = {self.nodes[node]["node_id"] for node in DATA}
        public_table = self.checked(
            DATA[0],
            "GET",
            "/db/v1/tables/quorum_fixture",
            checks=[{"path": ["name"], "equals": "quorum_fixture"}],
        )
        table_id = int(public_table["table_id"])
        while time.monotonic() < deadline:
            result = self.call(
                metadata_node,
                "GET",
                "/metadata/v1/admin/snapshot",
                timeout=min(2, max(0.01, deadline - time.monotonic())),
            )
            if result["passed"]:
                snapshot = json.loads(result["body"])
                tables = [
                    row for row in snapshot["tables"] if row["table_id"] == table_id
                ]
                ranges = [
                    row
                    for row in snapshot["ranges"]
                    if tables and row["table_id"] == tables[0]["table_id"]
                ]
                if (
                    len(tables) == 1
                    and tables[0]["desired_replica_count"] == 3
                    and len(ranges) == 1
                ):
                    group = ranges[0]["group_id"]
                    reports = {
                        store["node_id"]: report
                        for store in snapshot["stores"]
                        if store["node_id"] in expected and store.get("live")
                        for report in store.get("group_statuses", [])
                        if report["group_id"] == group
                    }
                    if (
                        set(reports) == expected
                        and all(
                            row.get("local_voter")
                            and row.get("voter_count") == 3
                            and row.get("voter_set_known")
                            and not row.get("joint_consensus")
                            and row.get("raft_applied_index", 0) > 0
                            for row in reports.values()
                        )
                        and sum(
                            bool(row.get("local_leader")) for row in reports.values()
                        )
                        == 1
                        and len(
                            {
                                json.dumps(row["voter_set_fingerprint"])
                                for row in reports.values()
                            }
                        )
                        == 1
                    ):
                        self.record(
                            {
                                "event": "verified_three_data_voters",
                                "group_id": group,
                                "reports": reports,
                            }
                        )
                        return
            time.sleep(0.2)
        raise TimeoutError("three live data voter reports deadline")

    def read_document(self, key, marker, timeout=60):
        deadline = time.monotonic() + timeout
        pending = set(DATA)
        while pending and time.monotonic() < deadline:
            for node in list(pending):
                result = self.call(
                    node,
                    "GET",
                    f"/db/v1/tables/quorum_fixture/documents/{key}",
                    checks=[{"path": ["marker"], "equals": marker}],
                    timeout=min(2, max(0.01, deadline - time.monotonic())),
                )
                if result["passed"]:
                    pending.remove(node)
            if pending:
                time.sleep(0.2)
        if pending:
            raise TimeoutError(f"sentinel read deadline: {sorted(pending)}")

    def sentinel(self, key):
        marker = f"durable-{key}"
        self.checked(
            DATA[0],
            "POST",
            "/db/v1/tables/quorum_fixture/batch",
            {"inserts": {key: {"marker": marker}}, "sync_level": "full_index"},
            expected=201,
            checks=[{"path": ["inserted"], "equals": 1}],
        )
        self.read_document(key, marker)


def run(binary, revision, optimization, output):
    plan = make_plan(binary, revision, optimization)
    runner.validate(plan)
    output.mkdir(parents=True, exist_ok=False)
    runner.q.save(output / "plan.json", plan)
    for source in (
        __file__,
        runner.__file__,
        runner.q.__file__,
        runner.scenarios.__file__,
        runner.q.vectors.__file__,
        runner.q.evidence.__file__,
        runner.proxy_module.__file__,
        runner.attempt_evidence.__file__,
    ):
        shutil.copy2(source, output / Path(source).name)
    template_dir = output / "workload-fixed-policy-plans"
    template_dir.mkdir()
    shutil.copy2(
        Path(__file__).parent / "workload-fixed-policy-plans/local-correctness.json",
        template_dir / "local-correctness.json",
    )
    runner.q.save(
        output / "host.json",
        {
            "platform": runner.q.platform.platform(),
            "purpose": "small native replicated correctness",
            "resource_envelope_enforced": False,
        },
    )
    cluster, failure, cleanup = None, None, []
    try:
        (output / "artifacts").mkdir()
        artifact = output / "artifacts/candidate"
        shutil.copyfile(binary, artifact)
        if runner.q.checksum(artifact) != plan["artifacts"]["candidate"]["sha256"]:
            raise ValueError("copied binary checksum mismatch")
        artifact.chmod(0o500)
        cluster = ReplicatedCluster(plan, output)
        # All metadata voters must be launched before requiring elected state.
        for node in METADATA:
            cluster.start(node)
        cluster.defer_ready = False
        for node in METADATA:
            cluster.ready(node)
        leader = cluster.leader(METADATA)
        for node in DATA:
            cluster.start(node)
        cluster.checked(
            DATA[0],
            "POST",
            "/db/v1/tables/quorum_fixture",
            {"num_shards": 1},
            checks=[{"path": ["name"], "equals": "quorum_fixture"}],
        )
        # Wait on reads only; never retry a create/batch with an unknown outcome.
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            statuses = [
                cluster.call(node, "GET", "/db/v1/tables/quorum_fixture")
                for node in DATA
            ]
            if all(item["passed"] for item in statuses):
                break
            time.sleep(0.2)
        else:
            raise TimeoutError("data table provisioning deadline")
        cluster.sentinel("before")
        cluster.replicas(leader)
        cluster.fault({"action": "kill", "node": leader})
        survivors = [node for node in METADATA if node != leader]
        successor = cluster.leader(survivors, previous=leader)
        cluster.checked(
            DATA[0],
            "POST",
            "/db/v1/tables/after_leader",
            {"num_shards": 1},
            checks=[{"path": ["name"], "equals": "after_leader"}],
        )
        cluster.sentinel("after_leader")
        second = next(node for node in survivors if node != successor)
        cluster.fault({"action": "kill", "node": second})
        # One surviving voter cannot commit a new metadata mutation. Its
        # request may be rejected or time out, and is never presumed aborted.
        uncertain = cluster.call(
            DATA[0], "POST", "/db/v1/tables/without_quorum", {"num_shards": 1}
        )
        if uncertain.get("status") not in (503, 504) and not (
            uncertain.get("unknown_write_outcome")
            and "status" not in uncertain
            and not uncertain.get("invalid_result")
        ):
            raise AssertionError(
                f"no-quorum request lacked expected unavailable/unknown outcome: {uncertain}"
            )
        cluster.record(
            {
                "event": "quorum_unavailable_write",
                "outcome": "unresolved",
                "receipt": uncertain,
            }
        )
        cluster.defer_ready = True
        for node in (leader, second):
            cluster.start(node)
        cluster.defer_ready = False
        for node in (leader, second):
            cluster.ready(node)
        recovered = cluster.leader(METADATA)
        cluster.checked(
            DATA[0],
            "POST",
            "/db/v1/tables/after_quorum",
            {"num_shards": 1},
            checks=[{"path": ["name"], "equals": "after_quorum"}],
        )
        cluster.sentinel("after_quorum")
        cluster.replicas(recovered)
        for key in ("before", "after_leader"):
            cluster.read_document(key, f"durable-{key}")
        for node in DATA:
            for table in ("quorum_fixture", "after_leader", "after_quorum"):
                cluster.checked(
                    node,
                    "GET",
                    f"/db/v1/tables/{table}",
                    checks=[{"path": ["name"], "equals": table}],
                )
        observation = cluster.call(DATA[0], "GET", "/db/v1/tables/without_quorum")
        cluster.record(
            {
                "event": "ambiguous_write_observation_only",
                "receipt": observation,
                "replayed": False,
                "terminal_outcome_proven": observation.get("status") == 200,
            }
        )
    except Exception as error:  # noqa: BLE001
        failure = f"{type(error).__name__}: {error}"
    finally:
        if cluster is not None:
            cleanup = cluster.close()
        receipt = {
            "passed": failure is None and not cleanup,
            "failure": failure,
            "cleanup_errors": cleanup,
            "purpose": "small native replicated correctness",
            "performance_qualification": False,
            "artifact": plan["artifacts"]["candidate"],
            "limitations": plan["limitations"],
        }
        runner.q.save(output / "receipt.json", receipt)
        evidence_paths = [
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
            {
                str(path.relative_to(output)): runner.q.checksum(path)
                for path in evidence_paths
            },
        )
    print(json.dumps(receipt, indent=2))
    return 0 if receipt["passed"] else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--revision", required=True)
    parser.add_argument(
        "--optimization", choices=("Debug", "ReleaseSafe", "ReleaseFast"), required=True
    )
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if not re.fullmatch(r"[0-9a-f]{40}", args.revision):
        parser.error("--revision requires the full frozen binary revision")
    return run(
        args.binary.resolve(), args.revision, args.optimization, args.output.resolve()
    )


if __name__ == "__main__":
    raise SystemExit(main())
