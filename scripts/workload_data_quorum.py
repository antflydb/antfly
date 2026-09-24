#!/usr/bin/env python3
"""Six native processes: ordinary DATA leader loss and replacement.

Uses the production metadata catalog, DATA Raft HTTP transport, and the
owned-process qualification runner. Replicated physical completion activation
remains disabled; this scenario qualifies ordinary document writes only.
"""

from __future__ import annotations

import argparse
import json
import re
import shutil
import time
from pathlib import Path

import workload_cluster_qualification as runner
import workload_metadata_quorum as metadata_quorum

METADATA = metadata_quorum.METADATA
DATA = metadata_quorum.DATA
TABLE = "quorum_fixture"


def make_plan(binary, revision, optimization):
    plan = metadata_quorum.make_plan(binary, revision, optimization)
    plan["driver"] = "workload_data_quorum.py"
    plan["scenario"] = [
        "verify three metadata and three DATA voters",
        "submit one ordinary full-index document write",
        "verify each DATA replica advances its applied index and reads the document",
        "kill the observed DATA leader after convergence",
        "verify a different DATA leader with the same group and voter fingerprint",
        "submit one ordinary full-index document write through the successor",
        "restart the killed DATA process on its original root",
        "verify all three DATA replicas converge and both documents remain readable",
    ]
    plan["limitations"] = [
        "Ordinary DATA writes only; replicated physical completion activation remains disabled",
        "Public document reads may forward; per-replica application evidence is the reported Raft applied index",
        "No power-loss, no-quorum mutation, process-memory, or performance qualification",
    ]
    return plan


class DataQuorumCluster(metadata_quorum.ReplicatedCluster):
    def __init__(self, plan, output):
        super().__init__(plan, output)
        self.data_identity = None
        self.data_leader_term = None

    def data_view(self, metadata_node, live, previous=None, timeout=60):
        """Require live, agreeing three-voter reports for one public table."""
        expected = {self.nodes[node]["node_id"]: node for node in live}
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self.data_identity is None:
                public = self.call(
                    live[0],
                    "GET",
                    f"/db/v1/tables/{TABLE}",
                    checks=[{"path": ["name"], "equals": TABLE}],
                    timeout=min(2, max(0.01, deadline - time.monotonic())),
                )
                if not public["passed"]:
                    time.sleep(0.2)
                    continue
                table_id = int(json.loads(public["body"])["table_id"])
            else:
                table_id = self.data_identity[0]
            response = self.call(
                metadata_node,
                "GET",
                "/metadata/v1/admin/snapshot",
                timeout=min(2, max(0.01, deadline - time.monotonic())),
            )
            if response["passed"]:
                snapshot = json.loads(response["body"])
                tables = [
                    row
                    for row in snapshot["tables"]
                    if row["table_id"] == table_id
                    and row.get("desired_replica_count") == 3
                ]
                ranges = [
                    row for row in snapshot["ranges"] if row["table_id"] == table_id
                ]
                if len(tables) == 1 and len(ranges) == 1:
                    group_id = ranges[0]["group_id"]
                    reports = {
                        expected[store["node_id"]]: report
                        for store in snapshot["stores"]
                        if store["node_id"] in expected and store.get("live")
                        for report in store.get("group_statuses", [])
                        if report["group_id"] == group_id
                    }
                    if set(reports) == set(live) and all(
                        report.get("local_voter")
                        and report.get("voter_count") == 3
                        and report.get("voter_set_known")
                        and not report.get("joint_consensus")
                        and report.get("raft_applied_index", 0) > 0
                        and report.get("raft_term", 0) > 0
                        for report in reports.values()
                    ):
                        raw_fingerprints = [
                            report.get("voter_set_fingerprint")
                            for report in reports.values()
                        ]
                        fingerprints = {json.dumps(value) for value in raw_fingerprints}
                        leaders = [
                            node
                            for node, report in reports.items()
                            if report.get("local_leader")
                        ]
                        if (
                            len(fingerprints) == 1
                            and all(
                                isinstance(value, list)
                                and len(value) == 32
                                and any(value)
                                and all(
                                    type(byte) is int and 0 <= byte <= 255
                                    for byte in value
                                )
                                for value in raw_fingerprints
                            )
                            and len(leaders) == 1
                            and leaders[0] != previous
                        ):
                            leader_term = reports[leaders[0]]["raft_term"]
                            if any(
                                report["raft_term"] > leader_term
                                for report in reports.values()
                            ):
                                time.sleep(0.2)
                                continue
                            if (
                                previous is not None
                                and self.data_leader_term is not None
                                and leader_term <= self.data_leader_term
                            ):
                                time.sleep(0.2)
                                continue
                            identity = (table_id, group_id, next(iter(fingerprints)))
                            if (
                                self.data_identity is not None
                                and identity != self.data_identity
                            ):
                                raise AssertionError(
                                    "DATA table/group/voter identity changed"
                                )
                            self.data_identity = identity
                            self.data_leader_term = leader_term
                            view = {
                                "table_id": table_id,
                                "group_id": group_id,
                                "fingerprint": identity[2],
                                "leader": leaders[0],
                                "leader_term": leader_term,
                                "reports": reports,
                            }
                            self.record({"event": "verified_data_leader", **view})
                            return view
            time.sleep(0.2)
        raise TimeoutError("live three-voter DATA leader agreement deadline")

    def wait_applied_after(self, metadata_node, live, before, timeout=60):
        """Require each actual DATA owner to advance past its pre-write index."""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                view = self.data_view(
                    metadata_node,
                    live,
                    timeout=min(2, max(0.01, deadline - time.monotonic())),
                )
            except TimeoutError:
                continue
            if all(
                view["reports"][node]["raft_applied_index"] > before[node]
                for node in live
            ):
                self.record(
                    {
                        "event": "verified_data_applied_advance",
                        "group_id": view["group_id"],
                        "before": before,
                        "after": {
                            node: view["reports"][node]["raft_applied_index"]
                            for node in live
                        },
                    }
                )
                return view
            time.sleep(0.2)
        raise TimeoutError("DATA replicas did not advance after accepted write")

    def read_document_on(self, nodes, key, marker, timeout=60):
        deadline = time.monotonic() + timeout
        pending = set(nodes)
        while pending and time.monotonic() < deadline:
            for node in list(pending):
                response = self.call(
                    node,
                    "GET",
                    f"/db/v1/tables/{TABLE}/documents/{key}",
                    checks=[{"path": ["marker"], "equals": marker}],
                    timeout=min(2, max(0.01, deadline - time.monotonic())),
                )
                if response["passed"]:
                    pending.remove(node)
            if pending:
                time.sleep(0.2)
        if pending:
            raise TimeoutError(f"document {key} unreadable through {sorted(pending)}")

    def write_once(self, node, key):
        marker = f"durable-{key}"
        self.checked(
            node,
            "POST",
            f"/db/v1/tables/{TABLE}/batch",
            {"inserts": {key: {"marker": marker}}, "sync_level": "full_index"},
            expected=201,
            checks=[{"path": ["inserted"], "equals": 1}],
        )
        return marker


def run(binary, revision, optimization, output):
    plan = make_plan(binary, revision, optimization)
    runner.validate(plan)
    output.mkdir(parents=True, exist_ok=False)
    runner.q.save(output / "plan.json", plan)
    for source in (
        __file__,
        metadata_quorum.__file__,
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
            "purpose": "small native ordinary DATA quorum correctness",
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
        cluster = DataQuorumCluster(plan, output)
        for node in METADATA:
            cluster.start(node)
        cluster.defer_ready = False
        for node in METADATA:
            cluster.ready(node)
        metadata_leader = cluster.leader(METADATA)
        for node in DATA:
            cluster.start(node)
        cluster.checked(
            DATA[0],
            "POST",
            f"/db/v1/tables/{TABLE}",
            {"num_shards": 1},
            checks=[{"path": ["name"], "equals": TABLE}],
        )
        initial = cluster.data_view(metadata_leader, DATA)
        before = {node: initial["reports"][node]["raft_applied_index"] for node in DATA}
        first_marker = cluster.write_once(initial["leader"], "before")
        cluster.wait_applied_after(metadata_leader, DATA, before)
        cluster.read_document_on(DATA, "before", first_marker)

        killed_leader = initial["leader"]
        cluster.fault({"action": "kill", "node": killed_leader})
        survivors = tuple(node for node in DATA if node != killed_leader)
        successor = cluster.data_view(
            metadata_leader, survivors, previous=killed_leader
        )
        cluster.read_document_on(survivors, "before", first_marker)
        before_successor_write = {
            node: successor["reports"][node]["raft_applied_index"] for node in survivors
        }
        second_marker = cluster.write_once(successor["leader"], "after_failover")
        committed = cluster.wait_applied_after(
            metadata_leader, survivors, before_successor_write
        )
        cluster.read_document_on(survivors, "after_failover", second_marker)

        cluster.fault({"action": "restart", "node": killed_leader})
        cluster.data_view(metadata_leader, DATA)
        # The returned old leader must catch up to the smallest witnessed
        # post-failover applied frontier, not merely report voter membership.
        floor = min(
            committed["reports"][node]["raft_applied_index"] for node in survivors
        )
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            try:
                converged = cluster.data_view(
                    metadata_leader,
                    DATA,
                    timeout=min(2, max(0.01, deadline - time.monotonic())),
                )
            except TimeoutError:
                continue
            if all(
                report["raft_applied_index"] >= floor
                for report in converged["reports"].values()
            ):
                cluster.record(
                    {
                        "event": "verified_restarted_data_convergence",
                        "group_id": converged["group_id"],
                        "minimum_applied_index": floor,
                        "reports": converged["reports"],
                    }
                )
                break
            time.sleep(0.2)
        else:
            raise TimeoutError("restarted DATA replica did not catch up")
        cluster.read_document_on(DATA, "before", first_marker)
        cluster.read_document_on(DATA, "after_failover", second_marker)
    except Exception as error:  # noqa: BLE001
        failure = f"{type(error).__name__}: {error}"
    finally:
        if cluster is not None:
            cleanup = cluster.close()
        receipt = {
            "passed": failure is None and not cleanup,
            "failure": failure,
            "cleanup_errors": cleanup,
            "purpose": "small native ordinary DATA quorum correctness",
            "physical_completion_qualified": False,
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
