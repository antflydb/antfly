import copy
import json
import unittest
from pathlib import Path

import workload_cluster_qualification as runner
import workload_frontend_overload as frontend


class FrontendFixtureTests(unittest.TestCase):
    def test_exact_success_and_conservative_queue_full_contract(self):
        self.assertEqual(
            frontend.classify({"status": 200, "body": '{"marker":"durable-a"}'}),
            "success",
        )
        denied = {
            "status": 429,
            "headers": {"Retry-After": "1"},
            "body": json.dumps(
                {
                    "error": "AdmissionQueueFull",
                    "reason": "instance_busy",
                    "stage": "admission",
                    "execution_started": False,
                }
            ),
        }
        self.assertEqual(frontend.classify(denied), "rejected")
        for key, value in (
            ("reason", "resource_exhausted"),
            ("stage", "execution"),
            ("execution_started", True),
            ("error", "AdmissionWaitTimeout"),
        ):
            invalid = copy.deepcopy(denied)
            invalid["body"] = json.dumps({**json.loads(denied["body"]), key: value})
            with self.assertRaises(AssertionError):
                frontend.classify(invalid)
        for invalid in (
            {"status": 200, "body": "{}"},
            {**denied, "status": 500},
            {**denied, "headers": {}},
            {**denied, "error": "timeout"},
        ):
            with self.assertRaises(AssertionError):
                frontend.classify(invalid)

    def test_frontend_policy_has_finite_headroom_and_no_remote_attempts(self):
        plan = frontend.make_plan(Path("/usr/bin/true"), "a" * 40, "Debug")
        runner.validate(plan)
        self.assertEqual(plan["concurrencies"], [40, 80])
        self.assertEqual(plan["generator_max_workers"], 80)
        for node in plan["nodes"][1:]:
            admission = node["config"]["admission"]
            self.assertGreater(
                admission["ingress"]["max_requests"]
                - admission["ingress"]["control_requests"]
                - admission["ingress"]["recovery_requests"],
                80,
            )
            self.assertEqual(admission["remote_attempt_coordinator"]["max_attempts"], 0)
            self.assertEqual(admission["remote_attempt_worker"]["max_attempts"], 0)
            self.assertEqual(admission["query"]["max_concurrent_requests"], 4)
            self.assertEqual(admission["query"]["waiting"]["max_queued_requests"], 8)
        action = frontend.metric_action(plan["nodes"][-1]["config"], 10, True)
        runner.validate_metrics_action(action)
        self.assertEqual(action["expected"]["antfly_admission_query_retained_bytes"], 0)
        self.assertEqual(
            action["ceilings"]["antfly_admission_query_in_flight_requests"], 4
        )
        self.assertEqual(
            action["ceilings"]["antfly_admission_query_queued_requests"], 8
        )


if __name__ == "__main__":
    unittest.main()
