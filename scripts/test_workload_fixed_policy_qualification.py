import copy
import json
import unittest
from pathlib import Path

import workload_cluster_qualification as cluster
import workload_evidence as evidence
import workload_fixed_policy as policy
import workload_qualification as q


class FixedPolicyTests(unittest.TestCase):
    def test_release_cells_preserve_preselected_thresholds_and_baseline(self):
        for tier, (cpus, memory, _) in q.TIERS.items():
            with self.subTest(tier=tier):
                old = q.release_plan(tier)
                plan = policy.performance_plan(tier)
                for field in (
                    "warmup_seconds",
                    "seconds",
                    "runs",
                    "concurrency",
                    "open_factors",
                    "overload_seconds",
                    "recovery_seconds",
                    "request_timeout",
                    "generator_workers",
                    "generator_queue",
                    "documents",
                ):
                    self.assertEqual(plan[field], old[field])
                self.assertEqual(
                    plan["arms"]["baseline"]["config"],
                    old["arms"]["baseline"]["config"],
                )
                admission = plan["arms"]["candidate"]["config"]["admission"]
                self.assertEqual(
                    admission["read_execution"]["max_runnable_tasks"], cpus
                )
                self.assertLess(
                    admission["read_execution"]["max_working_bytes"], memory // 8
                )
                self.assertGreater(admission["ingress"]["recovery_requests"], 0)
                self.assertEqual(admission["read_execution"]["max_scan_state_bytes"], 0)
                self.assertEqual(
                    admission["read_execution"]["protected"]["max_runnable_tasks"], 0
                )
                for arm in plan["arms"].values():
                    arm.update(image="pinned-by-runner", revision="a" * 40)
                q.validate(plan)
        self.assertEqual(
            q.GATES,
            {
                "low_load_p99_ratio": 1.10,
                "low_load_p99_add_ms": 1.0,
                "throughput_ratio": 0.95,
                "cgroup_peak_ratio": 0.90,
            },
        )

    def test_operator_cells_preserve_checked_ground_truth_and_weighted_mix(self):
        plan = policy.performance_plan("starter", operators=True)
        for arm in plan["arms"].values():
            arm.update(image="pinned-by-runner", revision="a" * 40)
        q.validate(plan)
        for workload, writes in zip(plan["workloads"], (10, 50)):
            self.assertEqual(sum(op["weight"] for op in workload["operations"]), 100)
            self.assertEqual(
                sum(op["weight"] for op in workload["operations"] if op["is_write"]),
                writes,
            )
            self.assertEqual(len(workload["operations"]), 5)

    def test_correctness_topology_has_finite_independent_completion_budgets(self):
        plan = policy.correctness_plan()
        binary = Path("/usr/bin/true")
        plan["artifacts"]["candidate"].update(
            binary=str(binary), sha256=q.checksum(binary), revision="b" * 40
        )
        cluster.validate(plan)
        recovery = plan["setup"][0]["body"]["storage"]["transaction_recovery"]
        for node in plan["nodes"][1:]:
            a = node["config"]["admission"]
            ingress = a["ingress"]
            self.assertLess(
                ingress["control_requests"] + ingress["recovery_requests"],
                ingress["max_requests"],
            )
            self.assertLess(
                ingress["control_retained_bytes"] + ingress["recovery_retained_bytes"],
                ingress["max_retained_bytes"],
            )
            self.assertGreaterEqual(
                a["remote_attempt_worker"]["max_attempts"], len(plan["nodes"]) - 1
            )
            self.assertGreater(a["remote_attempt_coordinator"]["max_attempts"], 0)
            self.assertLessEqual(
                recovery["max_transaction_bytes"] + 65536,
                a["transaction_completion_bytes"] // 2,
            )
            read = a["read_execution"]
            self.assertLessEqual(
                read["max_runnable_tasks"], read["max_outstanding_tasks"]
            )
            self.assertLessEqual(
                read["max_suspended_io"], read["max_outstanding_tasks"]
            )
        self.assertTrue(plan["nodes"][1]["proxy_api"])
        self.assertIn("assert_proxy", [action["action"] for action in plan["actions"]])
        with self.assertRaises(ValueError):
            cluster.validate(
                policy.correctness_plan()
            )  # unresolved artifact must fail closed

    def test_disabled_policy_fails_even_when_all_usage_is_below_ceiling(self):
        spec = policy.telemetry(policy.candidate_config("starter"))
        sample = {
            "finished_s": 0.25,
            "metrics_fresh": True,
            "memory_limit": 1000,
            "memory_peak": 100,
            "metrics": {**{key: 0 for key in spec["ceilings"]}, **spec["expected"]},
        }
        self.assertEqual(evidence.periodic_gates([sample], spec, 1)["status"], "passed")
        sample["metrics"]["antfly_read_execution_enabled"] = 0
        self.assertEqual(evidence.periodic_gates([sample], spec, 1)["status"], "failed")
        del sample["metrics"]["antfly_read_execution_enabled"]
        self.assertEqual(
            evidence.periodic_gates([sample], spec, 1)["status"], "unavailable"
        )

    def test_per_arm_series_do_not_waive_source_freshness(self):
        plan = policy.performance_plan("starter")
        baseline = q.arm_telemetry(plan, "baseline")
        candidate = q.arm_telemetry(plan, "candidate")
        self.assertNotIn("antfly_read_execution_enabled", baseline["expected"])
        self.assertIn("antfly_read_execution_enabled", candidate["expected"])
        sample = {
            "finished_s": 0.25,
            "metrics_fresh": False,
            "memory_limit": 1000,
            "memory_peak": 100,
            "metrics": {
                **{key: 0 for key in baseline["ceilings"]},
                **baseline["expected"],
            },
        }
        self.assertEqual(
            evidence.periodic_gates([sample], baseline, 1)["status"], "unavailable"
        )
        del plan["arms"]["baseline"]["telemetry"]
        plan["telemetry"] = baseline
        self.assertIs(q.arm_telemetry(plan, "baseline"), baseline)
        self.assertIs(q.arm_telemetry(plan, "candidate"), candidate)
        bad = copy.deepcopy(plan)
        bad["arms"]["candidate"]["telemetry"]["expected"]["broken"] = float("nan")
        with self.assertRaises(ValueError):
            evidence.validate(bad["arms"]["candidate"]["telemetry"])

    def test_retained_plans_match_generator(self):
        directory = Path(__file__).parent / "workload-fixed-policy-plans"
        for tier in q.TIERS:
            self.assertEqual(
                json.loads((directory / f"{tier}.json").read_text()),
                policy.performance_plan(tier),
            )
        self.assertEqual(
            json.loads((directory / "local-correctness.json").read_text()),
            policy.correctness_plan(),
        )


if __name__ == "__main__":
    unittest.main()
