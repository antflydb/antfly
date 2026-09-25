"""Fail-closed checks for ordinary DATA leader replacement qualification."""

import copy
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import workload_data_quorum as quorum


def snapshot(leader="data1", index=10):
    return {
        "tables": [
            {
                "table_id": 42,
                "name": "table:internal-name",
                "desired_replica_count": 3,
            }
        ],
        "ranges": [{"table_id": 42, "group_id": 7}],
        "stores": [
            {
                "node_id": number + 11,
                "live": True,
                "group_statuses": [
                    {
                        "group_id": 7,
                        "local_voter": True,
                        "voter_count": 3,
                        "voter_set_known": True,
                        "voter_set_fingerprint": [1] * 32,
                        "raft_applied_index": index,
                        "raft_term": 2 if leader == "data2" else 1,
                        "local_leader": f"data{number + 1}" == leader,
                        "joint_consensus": False,
                    }
                ],
            }
            for number in range(3)
        ],
    }


def fixture(value=None):
    cluster = object.__new__(quorum.DataQuorumCluster)
    cluster.nodes = {
        node: {"node_id": number + 11} for number, node in enumerate(quorum.DATA)
    }
    cluster.data_identity = None
    cluster.data_leader_term = None
    cluster.record = Mock()
    current = value or snapshot()

    def call(_node, _method, path, **_kwargs):
        if path == f"/db/v1/tables/{quorum.TABLE}":
            return {
                "passed": True,
                "body": json.dumps({"table_id": 42, "name": quorum.TABLE}),
            }
        return {"passed": True, "body": json.dumps(current)}

    cluster.call = Mock(side_effect=call)
    return cluster, current


class DataQuorumEvidenceTests(unittest.TestCase):
    def test_plan_keeps_completion_disabled_and_names_actual_scope(self):
        plan = quorum.make_plan(Path("/usr/bin/true"), "a" * 40, "Debug")
        quorum.runner.validate(plan)
        self.assertEqual(plan["driver"], "workload_data_quorum.py")
        self.assertTrue(
            all(
                node["config"]
                .get("admission", {})
                .get("transaction_completion_bytes", 0)
                == 0
                for node in plan["nodes"]
            )
        )
        self.assertTrue(
            any("ordinary" in value.lower() for value in plan["limitations"])
        )

    def test_requires_one_real_leader_and_stable_three_voter_identity(self):
        cluster, rows = fixture()
        initial = cluster.data_view(quorum.METADATA[0], quorum.DATA)
        self.assertEqual(initial["leader"], "data1")
        self.assertEqual(initial["group_id"], 7)
        rows["stores"][0]["group_statuses"][0]["local_leader"] = False
        rows["stores"][1]["group_statuses"][0]["local_leader"] = True
        for store in rows["stores"]:
            store["group_statuses"][0]["raft_term"] = 2
        successor = cluster.data_view(
            quorum.METADATA[0], quorum.DATA[1:], previous="data1"
        )
        self.assertEqual(successor["leader"], "data2")
        for field, bad in (
            ("local_voter", False),
            ("voter_count", 2),
            ("voter_set_known", False),
            ("joint_consensus", True),
            ("raft_applied_index", 0),
        ):
            with self.subTest(field=field):
                broken = copy.deepcopy(rows)
                broken["stores"][1]["group_statuses"][0][field] = bad
                cluster.call = Mock(
                    return_value={"passed": True, "body": json.dumps(broken)}
                )
                with (
                    self.assertRaises(TimeoutError),
                    patch.object(quorum.time, "sleep"),
                ):
                    cluster.data_view(
                        quorum.METADATA[0], quorum.DATA[1:], timeout=0.002
                    )

    def test_rejects_replacement_group_or_fingerprint(self):
        for field, bad in (("group_id", 8), ("voter_set_fingerprint", [9] * 32)):
            with self.subTest(field=field):
                cluster, rows = fixture()
                cluster.data_view(quorum.METADATA[0], quorum.DATA)
                if field == "group_id":
                    rows["ranges"][0]["group_id"] = bad
                    for store in rows["stores"]:
                        store["group_statuses"][0]["group_id"] = bad
                else:
                    for store in rows["stores"]:
                        store["group_statuses"][0][field] = bad
                with self.assertRaisesRegex(AssertionError, "identity changed"):
                    cluster.data_view(quorum.METADATA[0], quorum.DATA)

    def test_rejects_stale_successor_term_and_empty_voter_fingerprint(self):
        cluster, rows = fixture()
        cluster.data_view(quorum.METADATA[0], quorum.DATA)
        rows["stores"][0]["group_statuses"][0]["local_leader"] = False
        rows["stores"][1]["group_statuses"][0]["local_leader"] = True
        with self.assertRaises(TimeoutError), patch.object(quorum.time, "sleep"):
            cluster.data_view(
                quorum.METADATA[0], quorum.DATA[1:], previous="data1", timeout=0.002
            )
        for store in rows["stores"]:
            store["group_statuses"][0]["raft_term"] = 2
        rows["stores"][1]["group_statuses"][0]["voter_set_fingerprint"] = [0] * 32
        with self.assertRaises(TimeoutError), patch.object(quorum.time, "sleep"):
            cluster.data_view(
                quorum.METADATA[0], quorum.DATA[1:], previous="data1", timeout=0.002
            )

    def test_requires_each_replica_to_advance_after_write(self):
        cluster, rows = fixture()
        cluster.data_view(quorum.METADATA[0], quorum.DATA)
        baseline = {node: 10 for node in quorum.DATA}
        rows["stores"][0]["group_statuses"][0]["raft_applied_index"] = 11
        with self.assertRaises(TimeoutError), patch.object(quorum.time, "sleep"):
            cluster.wait_applied_after(
                quorum.METADATA[0], quorum.DATA, baseline, timeout=0.002
            )
        for store in rows["stores"]:
            store["group_statuses"][0]["raft_applied_index"] = 11
        advanced = cluster.wait_applied_after(quorum.METADATA[0], quorum.DATA, baseline)
        self.assertTrue(
            all(
                report["raft_applied_index"] == 11
                for report in advanced["reports"].values()
            )
        )

    def test_write_is_one_shot_and_unknown_outcome_is_not_retried(self):
        cluster, _ = fixture()
        cluster.checked = Mock(side_effect=RuntimeError("unknown write outcome"))
        with self.assertRaisesRegex(RuntimeError, "unknown write outcome"):
            cluster.write_once("data2", "before")
        self.assertEqual(cluster.checked.call_count, 1)

    def test_run_kills_observed_data_leader_then_restarts_original_root(self):
        class FakeCluster:
            instance = None

            def __init__(self, _plan, _output):
                self.events = []
                self.view_count = 0
                FakeCluster.instance = self

            def start(self, node):
                self.events.append(("start", node))

            def ready(self, node):
                self.events.append(("ready", node))

            def leader(self, _live):
                return "metadata1"

            def checked(self, node, method, *_args, **_kwargs):
                self.events.append(("checked", node, method))
                return {"name": quorum.TABLE}

            def data_view(self, _metadata, _live, previous=None, **_kwargs):
                self.view_count += 1
                leader = "data2" if self.view_count == 1 else "data3"
                if previous is not None:
                    assert previous == "data2"
                return {
                    "leader": leader,
                    "group_id": 7,
                    "reports": {
                        node: {"raft_applied_index": 10 + self.view_count}
                        for node in quorum.DATA
                    },
                }

            def write_once(self, node, key):
                self.events.append(("write", node, key))
                return f"durable-{key}"

            def wait_applied_after(self, _metadata, live, _before):
                self.events.append(("wait_applied", tuple(live)))
                return {
                    "group_id": 7,
                    "reports": {node: {"raft_applied_index": 20} for node in live},
                }

            def read_document_on(self, nodes, key, marker):
                self.events.append(("read", tuple(nodes), key, marker))

            def fault(self, action):
                self.events.append((action["action"], action["node"]))

            def record(self, _event):
                pass

            def close(self):
                return []

        with (
            tempfile.TemporaryDirectory() as root,
            patch.object(quorum, "DataQuorumCluster", FakeCluster),
        ):
            result = quorum.run(
                Path("/usr/bin/true"), "a" * 40, "Debug", Path(root) / "receipt"
            )
        self.assertEqual(result, 0)
        events = FakeCluster.instance.events
        self.assertEqual(
            [event for event in events if event[0] == "write"],
            [("write", "data2", "before"), ("write", "data3", "after_failover")],
        )
        self.assertLess(
            events.index(("kill", "data2")), events.index(("restart", "data2"))
        )


if __name__ == "__main__":
    unittest.main()
