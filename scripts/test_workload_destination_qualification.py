import copy
import unittest
from pathlib import Path
from unittest.mock import patch

import workload_cluster_qualification as runner
import workload_destination_isolation as isolation


class DestinationFixtureTests(unittest.TestCase):
    def snapshot(self):
        return {
            "tables": [{"table_id": 10, "desired_replica_count": 1}],
            "ranges": [{"table_id": 10, "group_id": 100}],
            "stores": [
                {
                    "node_id": 2,
                    "live": True,
                    "group_statuses": [
                        {
                            "group_id": 100,
                            "local_voter": True,
                            "local_leader": True,
                            "voter_count": 1,
                            "voter_set_known": True,
                            "joint_consensus": False,
                            "raft_applied_index": 8,
                        }
                    ],
                }
            ],
        }

    def test_plan_has_headroom_and_distinct_workers(self):
        plan = isolation.make_plan(Path("/usr/bin/true"), "a" * 40, "Debug")
        runner.validate(plan)
        workers = [node for node in plan["nodes"] if node.get("proxy_api")]
        self.assertEqual({node["node_id"] for node in workers}, {2, 4})
        coordinator = plan["nodes"][-1]["config"]["admission"][
            "remote_attempt_coordinator"
        ]
        self.assertEqual(coordinator["max_attempts"], 2)
        self.assertEqual(coordinator["max_destination_attempts"], 1)
        self.assertFalse(plan["setup_only"])

    def test_unknown_mutation_is_never_replayed(self):
        cluster = isolation.IsolationCluster.__new__(isolation.IsolationCluster)
        cluster.ports = {"api": {"api": 123}}
        cluster.plan = {"request_timeout": 1}
        receipts = []
        cluster.record = receipts.append
        with patch.object(
            runner,
            "request",
            return_value={"passed": True, "status": 201, "unknown_write_outcome": True},
        ) as send:
            with self.assertRaisesRegex(RuntimeError, "no mutation replay"):
                cluster.call("POST", "/db/v1/tables/example", {}, status=201)
            send.assert_called_once()
        self.assertTrue(receipts[0]["unknown_write_outcome"])

    def test_placement_needs_actual_exclusive_live_leader(self):
        original = self.snapshot()
        self.assertEqual(isolation.placement(original, 10, {2, 4})["destination"], 2)
        for field, value in [
            ("local_leader", False),
            ("local_voter", False),
            ("voter_count", 2),
            ("voter_set_known", False),
            ("joint_consensus", True),
            ("raft_applied_index", 0),
        ]:
            bad = copy.deepcopy(original)
            bad["stores"][0]["group_statuses"][0][field] = value
            self.assertIsNone(isolation.placement(bad, 10, {2, 4}))
        duplicate = copy.deepcopy(original)
        duplicate["stores"].append(
            {**copy.deepcopy(original["stores"][0]), "node_id": 4}
        )
        self.assertIsNone(isolation.placement(duplicate, 10, {2, 4}))
        self.assertIsNone(isolation.placement(original, 10, {4}))
        original["stores"][0]["live"] = False
        self.assertIsNone(isolation.placement(original, 10, {2, 4}))


if __name__ == "__main__":
    unittest.main()
