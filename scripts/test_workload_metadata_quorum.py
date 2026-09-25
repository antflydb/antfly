"""Fail-closed evidence checks for the small native metadata quorum driver."""

import copy
import json
import unittest
from unittest.mock import Mock, patch

import workload_metadata_quorum as quorum


def cluster_fixture():
    cluster = object.__new__(quorum.ReplicatedCluster)
    cluster.nodes = {
        name: {"metadata_id": index + 1} for index, name in enumerate(quorum.METADATA)
    }
    cluster.nodes.update(
        {name: {"node_id": index + 11} for index, name in enumerate(quorum.DATA)}
    )
    cluster.metadata_identity = None
    cluster.record = Mock()
    rows = {
        node: {
            "metadata_raft_voter_count": 3,
            "metadata_raft_local_voter": True,
            "metadata_raft_local_node_id": index + 1,
            "metadata_raft_role": "leader" if index == 0 else "follower",
            "metadata_raft_leader_id": 1,
            "metadata_raft_voter_set_fingerprint": "same-voters",
            "metadata_group_id": 100,
            "metadata_incarnation": "durable-identity",
        }
        for index, node in enumerate(quorum.METADATA)
    }
    cluster.call = Mock(
        side_effect=lambda node, *_args, **_kwargs: {
            "passed": True,
            "body": json.dumps(rows[node]),
        }
    )
    return cluster, rows


class QuorumEvidenceTests(unittest.TestCase):
    def test_rejects_independent_groups_and_inconsistent_leadership(self):
        for field, value in (
            ("metadata_group_id", 200),
            ("metadata_incarnation", "another-cluster"),
            ("metadata_raft_voter_count", 1),
            ("metadata_raft_leader_id", 2),
            ("metadata_raft_local_voter", False),
            ("metadata_raft_voter_set_fingerprint", "other-voters"),
        ):
            with self.subTest(field=field):
                cluster, rows = cluster_fixture()
                rows[quorum.METADATA[1]][field] = value
                with (
                    self.assertRaises(TimeoutError),
                    patch.object(quorum.time, "sleep"),
                ):
                    cluster.leader(quorum.METADATA, timeout=0.002)

    def test_restart_must_preserve_group_incarnation_and_voters(self):
        cluster, rows = cluster_fixture()
        self.assertEqual(cluster.leader(quorum.METADATA), quorum.METADATA[0])
        for row in rows.values():
            row["metadata_incarnation"] = "replacement-root"
        with self.assertRaisesRegex(AssertionError, "identity changed"):
            cluster.leader(quorum.METADATA)

    def test_live_successor_requires_agreement_from_both_survivors(self):
        cluster, rows = cluster_fixture()
        self.assertEqual(cluster.leader(quorum.METADATA), quorum.METADATA[0])
        rows[quorum.METADATA[1]]["metadata_raft_role"] = "leader"
        for node in quorum.METADATA[1:]:
            rows[node]["metadata_raft_leader_id"] = 2
        self.assertEqual(
            cluster.leader(quorum.METADATA[1:], previous=quorum.METADATA[0]),
            quorum.METADATA[1],
        )

    def test_unknown_mutation_is_never_replayed(self):
        cluster, _ = cluster_fixture()
        cluster.call = Mock(
            return_value={"passed": True, "unknown_write_outcome": True}
        )
        with self.assertRaisesRegex(RuntimeError, "no mutation replay"):
            cluster.checked(quorum.DATA[0], "POST", "/db/v1/tables/example", {})
        self.assertEqual(cluster.call.call_count, 1)

    def test_replica_proof_uses_table_id_and_actual_voter_reports(self):
        cluster, _ = cluster_fixture()
        cluster.checked = Mock(
            return_value={"name": "quorum_fixture", "table_id": "42"}
        )
        snapshot = {
            "tables": [
                {
                    "name": "table:opaque-internal-name",
                    "table_id": 42,
                    "desired_replica_count": 3,
                }
            ],
            "ranges": [{"table_id": 42, "group_id": 7}],
            "stores": [
                {
                    "node_id": index + 11,
                    "live": True,
                    "group_statuses": [
                        {
                            "group_id": 7,
                            "local_voter": True,
                            "voter_count": 3,
                            "voter_set_known": True,
                            "voter_set_fingerprint": [1, 2, 3],
                            "raft_applied_index": 10,
                            "local_leader": index == 0,
                            "joint_consensus": False,
                        }
                    ],
                }
                for index in range(3)
            ],
        }
        cluster.call = Mock(return_value={"passed": True, "body": json.dumps(snapshot)})
        cluster.replicas(quorum.METADATA[0])
        observed = cluster.record.call_args.args[0]
        self.assertEqual(observed["event"], "verified_three_data_voters")
        self.assertEqual(set(observed["reports"]), {11, 12, 13})
        stale = copy.deepcopy(snapshot)
        stale["stores"][2]["group_statuses"][0]["local_voter"] = False
        cluster.call = Mock(return_value={"passed": True, "body": json.dumps(stale)})
        with self.assertRaises(TimeoutError), patch.object(quorum.time, "sleep"):
            cluster.replicas(quorum.METADATA[0], timeout=0.002)


if __name__ == "__main__":
    unittest.main()
