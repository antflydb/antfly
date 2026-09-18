"""Offline checks for bounded arrival generation and honest release evidence."""

import json
import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import workload_qualification as qualification


class WorkloadQualificationTests(unittest.TestCase):
    def test_plan_rejects_unpinned_or_mismatched_qualification(self):
        plan = qualification.template("process")
        with tempfile.NamedTemporaryFile() as binary:
            for arm in plan["arms"].values():
                arm.update(binary=binary.name, revision="a" * 40)
            qualification.validate(plan)
            plan["arms"]["candidate"]["optimization"] = "ReleaseSafe"
            with self.assertRaises(ValueError):
                qualification.validate(plan)
            plan["arms"]["candidate"]["optimization"] = "Debug"
            plan["purpose"] = "qualification"
            with self.assertRaises(ValueError):
                qualification.validate(plan)

    def test_cloud_limits_are_exact_and_swap_disabled(self):
        plan = qualification.template("docker")
        argv = qualification.docker_command(
            plan, plan["arms"]["baseline"], Path("/tmp/fixture"), 1234, "owned-test"
        )
        self.assertEqual(argv[argv.index("--cpus") + 1], "1")
        self.assertEqual(argv[argv.index("--memory") + 1], str(4 << 30))
        self.assertEqual(argv[argv.index("--memory-swap") + 1], str(4 << 30))
        values = {
            "cpu.max": "100000 100000",
            "memory.max": str(4 << 30),
            "memory.swap.max": "0",
        }
        self.assertTrue(qualification.verify_cgroup(values, "starter"))
        self.assertFalse(qualification.verify_cgroup(values, "standard"))
        self.assertFalse(
            qualification.verify_cgroup({**values, "memory.swap.max": "max"}, "starter")
        )

    def test_offered_operation_mix_is_independent_of_completion(self):
        workload = {"read_percent": 90, "query_percent_of_reads": 50}
        counts = qualification.Counter(
            qualification.operation(i, workload, 128)[0] for i in range(1000)
        )
        self.assertEqual(counts, {"lookup": 450, "query": 450, "write": 100})
        for index in range(100):
            kind, _, _, body, document = qualification.operation(index, workload, 8)
            if kind == "write":
                self.assertEqual(
                    body["inserts"][f"doc{document}"],
                    qualification.fixture_document(document),
                )

    def test_correctness_checks_reject_partial_results_and_wrong_documents(self):
        self.assertEqual(
            qualification.classify("lookup", 1, 200, b'{"marker":"fixture-1"}', 2),
            "completed",
        )
        self.assertEqual(
            qualification.classify("lookup", 1, 200, b'{"marker":"fixture-0"}', 2),
            "invalid_result",
        )
        partial = json.dumps(
            {"responses": [{"hits": {"hits": [{"_id": "doc0"}]}}]}
        ).encode()
        self.assertEqual(
            qualification.classify("query", 0, 200, partial, 2), "invalid_result"
        )
        self.assertEqual(
            qualification.classify("query", 0, 429, b"busy", 2), "rejected"
        )

    def test_open_arrivals_remain_bounded_and_record_generator_drops(self):
        class SlowHTTP:
            def __init__(self, *_):
                self.connection = SimpleNamespace(timeout=1, sock=None)

            def close(self):
                pass

            def request(self, *_):
                time.sleep(0.02)
                return 429, b"busy", {}

        plan = qualification.template("process")
        plan.update(generator_workers=1, generator_queue=1, documents=8)
        with (
            tempfile.TemporaryDirectory() as tmp,
            patch.object(qualification, "HTTP", SlowHTTP),
        ):
            path = Path(tmp) / "samples.jsonl"
            result = qualification.run_load(
                1, plan["workloads"][0], plan, path, seconds=0.06, rate=1000
            )
            samples = [json.loads(line) for line in path.read_text().splitlines()]
        self.assertEqual(result["offered"], 60)
        self.assertEqual(len(samples), 60)
        self.assertGreater(result["counts"].get("generator_dropped", 0), 0)
        self.assertLessEqual(result["generator_peak_outstanding"], 2)
        self.assertIsNone(result["success_latency"]["p99_ms"])
        rejected = [sample for sample in samples if sample["outcome"] == "rejected"]
        self.assertTrue(any(sample["client_wait_ms"] > 1 for sample in rejected))

    def test_better_latency_with_rejected_work_does_not_pass_comparison(self):
        common = {
            "workload": "lookup",
            "phase": "closed",
            "concurrency": 1,
            "completed_qps": 100,
            "success_latency": {"p99_ms": 5},
        }
        baseline = {**common, "arm": "baseline", "counts": {"completed": 100}}
        candidate = {
            **common,
            "arm": "candidate",
            "counts": {"completed": 100, "rejected": 100},
        }
        self.assertFalse(
            qualification.compare([baseline, candidate])[0]["measured_gate_pass"]
        )

    def test_recovery_arrivals_do_not_wait_for_overload_drain(self):
        class SlowHTTP:
            def __init__(self, *_):
                self.connection = SimpleNamespace(timeout=1, sock=None)

            def close(self):
                pass

            def request(self, *_):
                time.sleep(0.03)
                return 429, b"busy", {}

        plan = qualification.template("process")
        plan.update(generator_workers=1, generator_queue=12, documents=8)
        with (
            tempfile.TemporaryDirectory() as tmp,
            patch.object(qualification, "HTTP", SlowHTTP),
        ):
            path = Path(tmp) / "samples.jsonl"
            result = qualification.run_load(
                1,
                plan["workloads"][0],
                plan,
                path,
                seconds=0.05,
                rate_schedule=[("overload", 0.02, 300), ("recovery", 0.03, 100)],
            )
            samples = [json.loads(line) for line in path.read_text().splitlines()]
        first_recovery = min(
            (sample for sample in samples if sample["arrival_phase"] == "recovery"),
            key=lambda sample: sample["sequence"],
        )
        self.assertAlmostEqual(first_recovery["submitted_s"], 0.02, places=5)
        self.assertGreater(first_recovery["client_wait_ms"], 50)
        self.assertEqual(
            [phase["name"] for phase in result["arrival_phases"]],
            ["overload", "recovery"],
        )
        self.assertNotIn("generator_dropped", result["counts"])

    def test_repeated_gate_uses_complete_lifecycle_mean(self):
        common = {
            "workload": "lookup",
            "phase": "closed",
            "concurrency": 1,
            "success_latency": {"p99_ms": 5},
            "counts": {"completed": 100},
        }
        points = [{**common, "arm": "baseline", "completed_qps": 100} for _ in range(3)]
        points += [
            {**common, "arm": "candidate", "completed_qps": qps}
            for qps in (200, 10, 10)
        ]
        result = qualification.compare(points, expected_runs=3)[0]
        self.assertAlmostEqual(result["sustainable_closed_qps"]["candidate"], 220 / 3)
        self.assertFalse(result["measured_gate_pass"])
        self.assertFalse(
            qualification.compare(points[:-1], expected_runs=3)[0]["measured_gate_pass"]
        )


if __name__ == "__main__":
    unittest.main()
