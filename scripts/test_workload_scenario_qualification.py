import json
import tempfile
import time
import subprocess
import unittest
from pathlib import Path
from unittest.mock import patch

import workload_evidence as evidence
import workload_qualification as harness
import workload_scenarios as scenarios


def operation(kind="read", weight=90):
    return {
        "class": kind,
        "weight": weight,
        "is_write": False,
        "method": "POST",
        "path": "/db/v1/tables/fixture/query",
        "body": {"full_text_search": {"match_all": {}}, "limit": 0},
        "expect": {
            "status": 200,
            "checks": [
                {"path": ["responses", 0, "hits", "total", "value"], "equals": 32}
            ],
        },
    }


class ScenarioTests(unittest.TestCase):
    def test_semantics_mix_and_ambiguous_writes(self):
        read = operation()
        write = {
            **operation("write", 10),
            "is_write": True,
            "path": "/db/v1/tables/fixture/batch",
        }
        workload = {"name": "mixed", "kind": "scenario", "operations": [read, write]}
        scenarios.validate(workload)
        self.assertEqual(
            harness.Counter(
                scenarios.select(workload, n)[1]["class"] for n in range(1000)
            ),
            {"read": 900, "write": 100},
        )
        self.assertEqual(harness.operation(0, workload, 1)[0], "read")
        body = {"responses": [{"hits": {"total": {"value": 32}}}]}
        self.assertEqual(scenarios.classify(read, 200, json.dumps(body)), "completed")
        body["responses"][0]["error"] = "partial results"
        self.assertEqual(
            scenarios.classify(read, 200, json.dumps(body)), "invalid_result"
        )
        write["is_write"] = False
        with self.assertRaises(ValueError):
            scenarios.validate(workload)

    def test_real_operator_fixture_declares_exact_ground_truth(self):
        for percent in (90, 50):
            workload = scenarios.mixed_fixture(
                rows=128, read_percent=percent, graph_depth=16
            )
            scenarios.validate(workload)
            self.assertEqual(
                sum(item["weight"] for item in workload["operations"]), 100
            )
            self.assertEqual(workload["operations"][-1]["weight"], 100 - percent)
            aggregate = workload["operations"][2]
            self.assertEqual(aggregate["expect"]["checks"][0]["equals"], 8128)
            self.assertEqual(
                workload["operations"][3]["expect"]["checks"][0]["length"], 16
            )

    def test_fixture_failure_never_replays_write_and_retains_receipt(self):
        class Client:
            requests = 0

            def __init__(self, *_):
                pass

            def request(self, *_):
                self.__class__.requests += 1
                return 503, b"{}", {}

            def close(self):
                pass

        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(RuntimeError):
                scenarios.seed(
                    Client, 1, {"name": "fixture", "setup": [operation()]}, Path(tmp)
                )
            self.assertEqual(Client.requests, 1)
            self.assertEqual(
                json.loads((Path(tmp) / "fixture-fixture.json").read_text())[0][
                    "outcome"
                ],
                "unexpected_http_error",
            )

    def test_stream_absolute_deadline_and_disconnect_not_throughput(self):
        clock = [100.0]

        class Response:
            status = 200

            def read1(self, _):
                return b"x"

        class Connection:
            sock = None
            closed = 0

            def __init__(self, *_):
                pass

            def request(self, *_):
                pass

            def getresponse(self):
                return Response()

            def close(self):
                self.__class__.closed += 1

        op = {
            **operation(),
            "method": "GET",
            "path": "/db/v1/tables/fixture/keys",
            "stream": {
                "mode": "disconnect",
                "chunk_bytes": 1,
                "max_bytes": 100,
                "pause_seconds": 0.8,
            },
        }
        with (
            patch.object(scenarios.http.client, "HTTPConnection", Connection),
            patch.object(scenarios.time, "monotonic", side_effect=lambda: clock[0]),
            patch.object(
                scenarios.time,
                "sleep",
                side_effect=lambda value: clock.__setitem__(0, clock[0] + value),
            ),
        ):
            with self.assertRaises(TimeoutError):
                scenarios.stream_request(1, op, 100.5)
            clock[0] = 100
            result = scenarios.stream_request(1, op, 102)
            self.assertEqual(result["outcome"], "intentional_disconnect")
            self.assertEqual(Connection.closed, 2)
        summary = harness.outcome_summary(
            "smoke",
            [{"counts": {"intentional_disconnect": 1}}],
            [{"exit_code": 0}],
            None,
        )
        self.assertTrue(summary["correctness_passed"])

    def test_unknown_scenario_write_outcome_is_preserved(self):
        class Connection:
            sock = None

        class Client:
            def __init__(self, *_):
                self.connection = Connection()

            def request(self, *_):
                raise OSError("response lost")

            def close(self):
                pass

        workload = {
            "kind": "scenario",
            "name": "write",
            "operations": [
                {
                    **operation("ingestion", 100),
                    "is_write": True,
                    "path": "/db/v1/tables/f/batch",
                }
            ],
        }
        with (
            tempfile.TemporaryDirectory() as tmp,
            patch.object(harness, "HTTP", Client),
        ):
            result = harness.run_load(
                1,
                workload,
                harness.template("process"),
                Path(tmp) / "samples",
                seconds=0.005,
                concurrency=1,
            )
            self.assertGreater(result["counts"]["unknown_write_outcome"], 0)
            self.assertEqual(result["completed_qps"], 0)


class EvidenceTests(unittest.TestCase):
    def sample(self, second=0.5):
        return {
            "started_s": second - 0.1,
            "finished_s": second,
            "metrics": {"queue": 0, "bytes": 12},
            "metrics_fresh": True,
            "memory_limit": 1000,
            "memory_peak": 800,
            "memory_events": {"oom": 0},
        }

    def test_sampler_retains_actual_cgroup_peak_and_raw_metrics(self):
        class Client:
            def __init__(self, *_):
                pass

            def request(self, *_):
                return 200, b"queue 0\nbytes 12\n", {"Content-Type": "text/plain"}

            def close(self):
                pass

        commands = []

        def command(argv, **kwargs):
            commands.append(argv)
            return subprocess.CompletedProcess(
                argv, 0, "100\n800\n1000\noom 0\noom_kill 0\n", ""
            )

        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "metrics.jsonl"
            with evidence.observe(
                Client,
                command,
                {"metrics_port": 1, "container": "owned-fixture"},
                path,
                {"interval_seconds": 0.1},
            ) as observations:
                time.sleep(0.02)
            self.assertEqual(observations[0]["memory_peak"], 800)
            self.assertFalse(observations[0]["metrics_fresh"])
            self.assertIsNone(observations[0]["metrics_age_seconds"])
            self.assertIn("/sys/fs/cgroup/memory.peak", commands[0])
            self.assertNotIn("metrics_raw", observations[0])
            retained = json.loads(path.read_text().splitlines()[0])
            self.assertEqual(retained["metrics_raw"], "queue 0\nbytes 12\n")
            self.assertIn("memory_events", retained)

    def test_cache_age_not_scrape_frequency_controls_freshness(self):
        class Client:
            age = "0"

            def __init__(self, *_):
                pass

            def request(self, *_):
                return (
                    200,
                    b"queue 0\n",
                    {"Content-Type": "text/plain", "X-Antfly-Metrics-Age-Ms": self.age},
                )

            def close(self):
                pass

        with tempfile.TemporaryDirectory() as tmp:
            for age, expected in (("0", True), ("5000", False)):
                Client.age = age
                with evidence.observe(
                    Client,
                    lambda *a, **k: None,
                    {"metrics_port": 1},
                    Path(tmp) / age,
                    {"interval_seconds": 0.1},
                ) as rows:
                    time.sleep(0.01)
                self.assertEqual(rows[0]["metrics_fresh"], expected)
                self.assertIn("metrics_body_sha256", rows[0])

    def test_memory_peaks_and_missing_metrics_fail_closed(self):
        spec = {"ceilings": {"bytes": 15}}
        rows = [self.sample(0.5), self.sample(1.5)]
        stale = [dict(rows[0], metrics_fresh=False)]
        result = evidence.periodic_gates(stale, spec, 1)
        self.assertEqual(result["status"], "unavailable")
        self.assertEqual(result["memory_gate_status"], "passed")
        self.assertEqual(evidence.periodic_gates(rows, spec, 2)["status"], "passed")
        rows[1]["memory_peak"] = 901
        self.assertEqual(evidence.periodic_gates(rows, spec, 2)["status"], "failed")
        rows[1]["memory_peak"] = 800
        del rows[1]["metrics"]["bytes"]
        self.assertEqual(
            evidence.periodic_gates(rows, spec, 2)["status"], "unavailable"
        )
        self.assertEqual(
            evidence.periodic_gates([self.sample(3)], spec, 3)["status"], "unavailable"
        )
        self.assertEqual(
            evidence.metrics(b'metric{class="a"} 1\n'), {'metric{class="a"}': 1}
        )
        for body in (b"<html>ok</html>", b"q NaN", b"q 1\nq 2"):
            with self.assertRaises(ValueError):
                evidence.metrics(body)

    def test_recovery_uses_original_submission_and_all_post10_windows(self):
        samples = [
            {
                "submitted_s": second + 0.1,
                "finished_s": second + 0.2,
                "latency_ms": 1,
                "outcome": "completed",
            }
            for second in range(70, 120)
        ]
        telemetry = [self.sample(second + 0.5) for second in range(70, 120)]
        spec = {"queue_metrics": ["queue"], "recovery_queue_bound": 0}
        self.assertEqual(
            evidence.recovery_gate(samples, telemetry, spec, 60, 120, 1)["status"],
            "passed",
        )
        telemetry[0]["metrics"]["queue"] = 1
        self.assertEqual(
            evidence.recovery_gate(samples, telemetry, spec, 60, 120, 1)["status"],
            "failed",
        )
        telemetry[0]["metrics"]["queue"] = 0
        samples[0]["latency_ms"] = 3
        self.assertEqual(
            evidence.recovery_gate(samples, telemetry, spec, 60, 120, 1)["status"],
            "failed",
        )
        self.assertEqual(
            evidence.recovery_gate([], [], spec, 60, 120, 1)["status"], "unavailable"
        )

    def test_half_rate_latency_requires_all_repetitions_and_honest_success(self):
        points = [
            {
                "workload": "read",
                "phase": "open",
                "factor": 0.5,
                "arm": arm,
                "counts": {"completed": 10},
                "success_latency": {"p99_ms": latency},
            }
            for arm, latency in (("baseline", 1), ("candidate", 2))
        ]
        self.assertEqual(
            harness.compare_open_low_load(points, 1)[0]["status"], "passed"
        )
        points[1]["success_latency"]["p99_ms"] = 2.2
        self.assertEqual(
            harness.compare_open_low_load(points, 1)[0]["status"], "failed"
        )
        self.assertEqual(
            harness.compare_open_low_load(points, 3)[0]["status"], "unavailable"
        )
        points[1]["counts"]["rejected"] = 5
        self.assertEqual(
            harness.compare_open_low_load(points, 1)[0]["status"], "unavailable"
        )

    def test_progress_does_not_invent_continuous_eligibility(self):
        samples = [
            {
                "class": "read",
                "submitted_s": 0,
                "finished_s": 11,
                "outcome": "completed",
            }
        ]
        result = evidence.progress_gates(samples, [operation()], 10)[0]
        self.assertEqual(result["without_completion"], [0, 1])
        samples[0]["submitted_s"] = 1
        self.assertEqual(
            evidence.progress_gates(samples, [operation()], 5)[0]["status"],
            "unavailable",
        )


if __name__ == "__main__":
    unittest.main()
