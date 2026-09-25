import copy
import unittest
from pathlib import Path
from unittest.mock import patch

import workload_cluster_qualification as cluster
import workload_fixed_policy as policy


class MetricAssertionTests(unittest.TestCase):
    def action(self):
        return {
            "action": "metrics",
            "node": "api",
            "at": 0,
            "timeout_seconds": 2,
            "interval_seconds": 0.25,
            "max_age_seconds": 1,
            "stable_seconds": 0.5,
            "expected": {"records": 1, "available": 1},
            "ceilings": {"records": 1},
        }

    def sample(self, *, records=1, age="0", kind="text/plain"):
        return {
            "passed": True,
            "status": 200,
            "body": f"records {records}\navailable 1\n",
            "headers": {"Content-Type": kind, "X-Antfly-Metrics-Age-Ms": age},
        }

    def poll(self, action, sample, *, submitted=100, now=100):
        clock, emitted = [now], []

        def response(*_, **__):
            clock[0] += 0.05
            return copy.deepcopy(sample)

        with (
            patch.object(cluster.time, "monotonic", side_effect=lambda: clock[0]),
            patch.object(
                cluster.time,
                "sleep",
                side_effect=lambda seconds: clock.__setitem__(0, clock[0] + seconds),
            ),
            patch.object(cluster, "request", side_effect=response) as calls,
        ):
            result = cluster.poll_metrics(1, action, submitted, emitted.append)
        return result, emitted, calls.call_count

    def test_fresh_stability_requires_multiple_actual_samples(self):
        result, samples, count = self.poll(self.action(), self.sample())
        self.assertTrue(result["passed"])
        self.assertGreaterEqual(count, 3)
        self.assertGreaterEqual(samples[-1]["stable_seconds"], 0.5)
        self.assertEqual(result["deadline_monotonic"], 102)
        self.assertEqual(samples[0]["body"], "records 1\navailable 1\n")

    def test_stale_html_missing_series_and_overshoot_cannot_pass(self):
        for sample in (
            self.sample(age="1200"),
            self.sample(age="-10"),
            self.sample(kind="text/html"),
            {**self.sample(), "body": "unrelated 1\n"},
        ):
            with self.subTest(sample=sample):
                result, emitted, _ = self.poll(self.action(), sample)
                self.assertFalse(result["passed"])
                self.assertTrue(emitted)
                self.assertEqual(emitted[0]["body"], sample["body"])
        result, _, count = self.poll(self.action(), self.sample(records=2))
        self.assertFalse(result["passed"])
        self.assertEqual(
            count, 1
        )  # Never wait for a hard-ceiling violation to disappear.
        self.assertIn("ceiling", result["failure"])

    def test_action_dispatch_delay_never_restarts_original_timeout(self):
        result, _, count = self.poll(self.action(), self.sample(), now=102.1)
        self.assertEqual(count, 0)
        self.assertFalse(result["passed"])
        result, _, _ = self.poll(self.action(), self.sample(), now=101.9)
        self.assertFalse(result["passed"])
        self.assertEqual(result["deadline_monotonic"], 102)

    def test_discovery_relation_rejects_namespace_loss_and_epoch_rollback(self):
        previous = {
            "coordinator": 3,
            "destination": 2,
            "protocol_version": 3,
            "worker_namespace": 123,
            "worker_incarnation": 4,
        }
        relation = {"previous": "initial", "namespace": "same", "epoch": "increased"}
        cluster.compare_discovery(
            previous, {**previous, "worker_incarnation": 5}, relation
        )
        for changed in (
            {"worker_namespace": 124, "worker_incarnation": 5},
            {"worker_incarnation": 3},
            {"worker_incarnation": 4},
            {"destination": 9, "worker_incarnation": 5},
        ):
            with self.assertRaises(ValueError):
                cluster.compare_discovery(previous, {**previous, **changed}, relation)
        cluster.compare_discovery(previous, previous, {**relation, "epoch": "same"})

    def test_production_plan_and_invalid_poll_contract(self):
        plan = policy.reconciliation_plan()
        binary = Path("/usr/bin/true")
        plan["artifacts"]["candidate"].update(
            binary=str(binary), sha256=cluster.q.checksum(binary), revision="b" * 40
        )
        cluster.validate(plan)
        metrics = [
            action for action in plan["actions"] if action["action"] == "metrics"
        ]
        self.assertGreaterEqual(len(metrics), 6)
        for node in plan["nodes"][1:]:
            self.assertEqual(
                node["config"]["admission"]["remote_attempt_coordinator"][
                    "max_attempts"
                ],
                1,
            )
        for changes in (
            {"timeout_seconds": 0},
            {"max_age_seconds": 5},
            {"stable_seconds": 2},
            {"expected": {}},
        ):
            with self.assertRaises(ValueError):
                cluster.validate_metrics_action({**self.action(), **changes})
        bad = copy.deepcopy(plan)
        relation = next(
            action["compare"] for action in bad["actions"] if "compare" in action
        )
        relation["previous"] = "future-unverified"
        with self.assertRaises(ValueError):
            cluster.validate(bad)


if __name__ == "__main__":
    unittest.main()
