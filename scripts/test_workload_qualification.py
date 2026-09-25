"""Offline checks for bounded arrival generation and honest release evidence."""

import json
import tempfile
import time
import unittest
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import workload_qualification as qualification


class WorkloadQualificationTests(unittest.TestCase):
    def test_outcome_summary_separates_correctness_generator_and_overload(self):
        clean = [{"exit_code": 0, "forced_kill": False, "oom_killed": False}]
        for purpose in ("smoke", "qualification"):
            accepted = qualification.outcome_summary(
                purpose, [{"counts": {"completed": 4, "rejected": 2}}], clean, None
            )
            self.assertTrue(accepted["correctness_passed"])
            self.assertEqual(accepted["exit_code"], 0)
            for kind in (
                "unexpected_http_error",
                "transport_error",
                "unknown_write_outcome",
                "invalid_result",
                "late_response",
            ):
                with self.subTest(purpose=purpose, kind=kind):
                    failed = qualification.outcome_summary(
                        purpose, [{"counts": {kind: 1}}], clean, None
                    )
                    self.assertEqual(failed["status"], "experiment_failed")
                    self.assertEqual(failed["exit_code"], 1)
                    self.assertFalse(failed["correctness_passed"])
            for kind in ("generator_dropped", "client_deadline_before_dispatch"):
                invalid = qualification.outcome_summary(
                    purpose, [{"counts": {kind: 1}}], clean, None
                )
                self.assertTrue(invalid["correctness_passed"])
                self.assertFalse(invalid["generator_valid"])
                self.assertEqual(invalid["status"], "generator_invalid")
                self.assertEqual(invalid["exit_code"], 2)
        recall = qualification.outcome_summary(
            "smoke",
            [{"counts": {"completed": 1}, "vector": {"recall_floor_pass": False}}],
            clean,
            None,
        )
        self.assertFalse(recall["correctness_passed"])
        for runtime in (
            {"exit_code": 1},
            {"exit_code": 0, "oom_killed": True},
            {"exit_code": 0, "forced_kill": True},
            {"exit_code": 0, "exited_before_shutdown": True},
            {},
        ):
            with self.subTest(runtime=runtime):
                failed = qualification.outcome_summary(
                    "smoke", [{"counts": {"completed": 1}}], [runtime], None
                )
                self.assertEqual(failed["exit_code"], 1)
                self.assertFalse(failed["shutdown_clean"])

    def test_run_fails_on_warmup_errors_and_retains_receipts_after_shutdown(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            binary = root / "antfly"
            binary.write_bytes(b"test artifact")
            plan = qualification.template("process")
            plan.update(
                runs=1,
                concurrency=[1],
                open_factors=[0.5],
                workloads=[plan["workloads"][0]],
            )
            for arm in plan["arms"].values():
                arm.update(binary=str(binary), revision="a" * 40)
            output = root / "receipts"

            @contextmanager
            def launch(_plan, _arm, directory):
                directory.mkdir(parents=True)
                runtime = {"port": 9999, "resource_limits_verified": False}
                try:
                    yield runtime
                finally:
                    runtime.update(exit_code=0, forced_kill=False, oom_killed=False)
                    qualification.save(directory / "runtime.json", runtime)

            def load(_port, _workload, _plan, path, **_kwargs):
                # All measurements pass; only candidate warmup is malformed.
                bad = path.parent.name == "candidate" and "warmup" in path.name
                counts = {"invalid_result" if bad else "completed": 1}
                path.write_text(json.dumps({"counts": counts}) + "\n")
                return {"counts": counts, "completed_qps": 10}

            with (
                patch.object(qualification, "launch", launch),
                patch.object(qualification, "seed"),
                patch.object(qualification, "snapshot", return_value={}),
                patch.object(qualification, "run_load", side_effect=load),
            ):
                summary = qualification.run(plan, output)
            self.assertEqual(summary["status"], "experiment_failed")
            self.assertEqual(summary["exit_code"], 1)
            self.assertTrue(summary["shutdown_clean"])
            self.assertTrue(
                all("warmup" in row["phase"] for row in summary["correctness_failures"])
            )
            self.assertEqual(json.loads((output / "summary.json").read_text()), summary)
            hashes = json.loads((output / "checksums.json").read_text())
            for name in (
                "summary.json",
                "warmups.json",
                "trial-0/candidate/runtime.json",
            ):
                self.assertEqual(hashes[name], qualification.checksum(output / name))

            # Missing telemetry fails evidence qualification without relabeling
            # successful product requests as correctness failures.
            with (
                patch.object(qualification, "launch", launch),
                patch.object(qualification, "seed"),
                patch.object(qualification, "snapshot", return_value={}),
                patch.object(
                    qualification,
                    "run_load",
                    side_effect=lambda *_args, **_kwargs: {
                        "counts": {"completed": 1},
                        "completed_qps": 10,
                    },
                ),
            ):
                missing = qualification.run(plan, root / "missing-telemetry")
            self.assertTrue(missing["correctness_passed"])
            self.assertFalse(missing["telemetry_complete"])
            self.assertEqual(missing["status"], "telemetry_unavailable")
            self.assertEqual(missing["exit_code"], 3)
            self.assertIn("Prometheus telemetry snapshots", missing["unmeasured_gates"])

            # Exceptions still finish the lifecycle and checksum final evidence.
            failed_output = root / "exception-receipts"
            with (
                patch.object(qualification, "launch", launch),
                patch.object(
                    qualification, "seed", side_effect=RuntimeError("fixture failed")
                ),
                self.assertRaisesRegex(RuntimeError, "fixture failed"),
            ):
                qualification.run(plan, failed_output)
            failed_summary = json.loads((failed_output / "summary.json").read_text())
            self.assertEqual(failed_summary["exit_code"], 1)
            self.assertTrue(failed_summary["shutdown_clean"])
            self.assertTrue((failed_output / "checksums.json").exists())

    def test_cli_returns_recorded_failure_or_generator_invalid_exit_code(self):
        with tempfile.TemporaryDirectory() as temporary:
            plan = Path(temporary) / "plan.json"
            plan.write_text("{}")
            for code in (0, 1, 2, 3):
                result = {
                    "status": "test",
                    "exit_code": code,
                    "correctness_passed": code != 1,
                    "release_qualified": False,
                    "generator_valid": code != 2,
                    "shutdown_clean": code != 1,
                }
                with (
                    patch(
                        "sys.argv",
                        [
                            "workload_qualification.py",
                            "run",
                            str(plan),
                            "--output",
                            str(plan.parent / "receipts"),
                        ],
                    ),
                    patch.object(qualification, "run", return_value=result),
                    patch("builtins.print"),
                ):
                    self.assertEqual(qualification.main(), code)

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
            plan,
            plan["arms"]["baseline"],
            Path("/tmp/fixture"),
            1234,
            "owned-test",
            5678,
        )
        self.assertEqual(argv[argv.index("--cpus") + 1], "1")
        self.assertIn("--no-healthcheck", argv)
        self.assertIn("127.0.0.1:1234:8080", argv)
        self.assertIn("127.0.0.1:5678:4200", argv)
        self.assertEqual(argv[argv.index("--health") + 1], "true")
        self.assertEqual(argv[argv.index("--health-port") + 1], "4200")
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

    def test_process_command_enables_dedicated_metrics_listener(self):
        argv = qualification.process_command(
            Path("/tmp/antfly"), Path("/tmp/fixture"), 1234, 5678
        )
        self.assertEqual(argv[argv.index("--port") + 1], "1234")
        self.assertEqual(argv[argv.index("--health") + 1], "true")
        self.assertEqual(argv[argv.index("--health-port") + 1], "5678")

    def test_snapshots_reject_dashboard_html_and_use_only_metrics_port(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            for content_type in ("text/html", "text/plain; version=0.0.4"):
                runtime = {"pid": 42, "port": 1234, "metrics_port": 5678}
                with (
                    patch.object(
                        qualification,
                        "command",
                        return_value=SimpleNamespace(stdout="1024", returncode=0),
                    ),
                    patch.object(qualification, "HTTP") as http,
                ):
                    http.return_value.request.return_value = (
                        200,
                        b"<!doctype html><html>Dashboard</html>",
                        {"Content-Type": content_type},
                    )
                    result = qualification.snapshot(runtime, 1234, directory, "invalid")
                http.assert_called_once_with(5678, 3)
                self.assertEqual(result["metrics_status"], 200)
                self.assertFalse(result["metrics_valid"])
                self.assertIn("metrics_error", result)
                self.assertFalse((directory / "invalid.prom").exists())
                self.assertTrue((directory / "invalid.metrics-invalid.body").exists())
                self.assertFalse(
                    qualification.telemetry_summary([runtime])["telemetry_complete"]
                )
            metrics = b'# HELP requests Total requests\n# TYPE requests counter\nrequests{route="/query",status="200"} 7\nlatency_sum 1.2e-3\n'
            runtime = {"pid": 42, "metrics_port": 5678}
            with (
                patch.object(
                    qualification,
                    "command",
                    return_value=SimpleNamespace(stdout="1024", returncode=0),
                ),
                patch.object(qualification, "HTTP") as http,
            ):
                http.return_value.request.return_value = (
                    200,
                    metrics,
                    {"Content-Type": "text/plain; version=0.0.4"},
                )
                result = qualification.snapshot(runtime, 1234, directory, "valid")
            self.assertTrue(result["metrics_valid"])
            self.assertEqual((directory / "valid.prom").read_bytes(), metrics)
            self.assertTrue(
                qualification.telemetry_summary([runtime])["telemetry_complete"]
            )
            self.assertFalse(
                qualification.telemetry_summary([{}])["telemetry_complete"]
            )
            self.assertFalse(qualification.telemetry_summary([])["telemetry_complete"])
            self.assertIsNotNone(
                qualification.prometheus_error(
                    200, b"# comments only", {"content-type": "text/plain"}
                )
            )
            self.assertIsNotNone(
                qualification.prometheus_error(
                    503, metrics, {"content-type": "text/plain"}
                )
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

    def test_release_plans_pin_baseline_and_leave_candidate_unattested(self):
        for tier, (_, memory, _) in qualification.TIERS.items():
            pending = qualification.release_plan(tier)
            self.assertEqual(
                pending["arms"]["baseline"]["revision"],
                "64f1afbb373d5da0a932f08e456116da139e9e9a",
            )
            self.assertIsNone(pending["arms"]["candidate"]["revision"])
            self.assertIsNone(pending["arms"]["candidate"]["image"])
            with self.assertRaises(ValueError):
                qualification.validate(pending)
            ready = qualification.release_plan(
                tier,
                "baseline@sha256:" + "a" * 64,
                "candidate@sha256:" + "b" * 64,
                "c" * 40,
            )
            qualification.validate(ready)
            self.assertEqual(
                ready["arms"]["baseline"]["config"],
                ready["arms"]["candidate"]["config"],
            )
            self.assertEqual(
                ready["arms"]["candidate"]["config"]["admission"]["query"]["waiting"][
                    "max_retained_bytes"
                ],
                memory // 8,
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
