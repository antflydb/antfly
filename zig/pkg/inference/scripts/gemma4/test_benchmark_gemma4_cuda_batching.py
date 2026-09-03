#!/usr/bin/env python3

from __future__ import annotations

import argparse
import pathlib
import sys
import tempfile
import unittest
from unittest import mock

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

import benchmark_gemma4_cuda_server as cuda_server_benchmark
from benchmark_gemma4_cuda_batching import (
    ROW_TWO_COUNTER,
    SCHEDULER_COUNTERS,
    evaluate_acceptance,
    evaluate_isolation_probe,
    parse_prometheus_counters,
    require_scheduler_counters,
    response_fingerprint,
)


def scheduler_delta(row_two_steps: int = 0) -> dict[str, int]:
    counters = {name: 0 for name in SCHEDULER_COUNTERS}
    counters[ROW_TWO_COUNTER] = row_two_steps
    return counters


def measurement(
    tok_s: float,
    p95_ms: float,
    fingerprints: tuple[str, str] = ("fingerprint-a", "fingerprint-b"),
    row_two_steps: int = 0,
) -> dict:
    return {
        "aggregate_tok_s": {"median": tok_s},
        "request_latency_ms": {"p95": p95_ms},
        "fingerprints": sorted(fingerprints),
        "fingerprints_by_case": {
            "primary_a": [fingerprints[0]],
            "primary_b": [fingerprints[1]],
        },
        "prompt_tokens_by_case": {"primary_a": [12], "primary_b": [12]},
        "scheduler_counter_delta": scheduler_delta(row_two_steps),
    }


def isolation_probe(passed: bool = True) -> dict:
    return {"passed": passed}


class CudaServerBenchmarkTests(unittest.TestCase):
    def test_main_passes_a_named_request_case_to_shared_measurement(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            existing_path = pathlib.Path(__file__)
            args = argparse.Namespace(
                antfly_bin=existing_path,
                model=existing_path,
                models_dir=existing_path.parent,
                output_dir=pathlib.Path(tmp) / "output",
                prompt="ants",
                tokens=8,
                cache_dtype="f32",
                warmups=0,
                repeats=1,
                startup_timeout=1.0,
                server_prefix="",
                max_step_items=1,
                decode_wait_us=0,
            )
            result = {
                "measurements": {
                    "1": {
                        "fingerprints": ["stable"],
                        "aggregate_tok_s": {"median": 1.0},
                        "request_latency_ms": {"p95": 1.0},
                    }
                },
                "metrics": {},
            }

            with (
                mock.patch.object(
                    cuda_server_benchmark, "parse_args", return_value=args
                ),
                mock.patch.object(cuda_server_benchmark, "Server") as server_type,
                mock.patch.object(
                    cuda_server_benchmark, "measure_mode", return_value=result
                ) as measure,
                mock.patch("builtins.print"),
            ):
                cuda_server_benchmark.main()

            measure.assert_called_once_with(
                server_type.return_value.__enter__.return_value,
                [("default", mock.ANY)],
                [1],
                0,
                1,
            )


class PrometheusCounterTests(unittest.TestCase):
    def test_parser_keeps_unlabelled_counters_only(self) -> None:
        text = """
# HELP requests_total Request count
# TYPE requests_total counter
requests_total 12
# TYPE latency gauge
latency 4
# TYPE labelled_total counter
labelled_total{route="generate"} 7
# TYPE fractional_total counter
fractional_total 1.5
"""
        self.assertEqual(
            {"requests_total": 12, "fractional_total": 1.5},
            parse_prometheus_counters(text),
        )

    def test_required_scheduler_counters_reject_missing_samples(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "missing counters"):
            require_scheduler_counters(
                "# TYPE unrelated_total counter\nunrelated_total 1\n"
            )

    def test_required_scheduler_counters_accept_complete_exposition(self) -> None:
        text = "\n".join(
            line
            for name in SCHEDULER_COUNTERS
            for line in (f"# TYPE {name} counter", f"{name} 3")
        )
        self.assertEqual(
            {name: 3 for name in SCHEDULER_COUNTERS}, require_scheduler_counters(text)
        )


class ResponseFingerprintTests(unittest.TestCase):
    def response(self) -> dict:
        return {
            "choices": [{"message": {"content": "ants"}, "finish_reason": "stop"}],
            "usage": {"prompt_tokens": 7, "completion_tokens": 3},
        }

    def test_finish_reason_difference_changes_fingerprint(self) -> None:
        baseline = self.response()
        changed = self.response()
        changed["choices"][0]["finish_reason"] = "length"
        self.assertNotEqual(
            response_fingerprint(baseline), response_fingerprint(changed)
        )

    def test_prompt_usage_difference_changes_fingerprint(self) -> None:
        baseline = self.response()
        changed = self.response()
        changed["usage"]["prompt_tokens"] = 8
        self.assertNotEqual(
            response_fingerprint(baseline), response_fingerprint(changed)
        )


class AcceptanceTests(unittest.TestCase):
    def test_staggered_isolation_does_not_require_homogeneous_row_two_step(
        self,
    ) -> None:
        baseline = {
            "fingerprints_by_case": {"long": ["long-fp"], "short": ["short-fp"]},
        }
        waves = [
            {"case_ids": ["long", "short"], "fingerprints": ["long-fp", "short-fp"]},
            {"case_ids": ["short", "long"], "fingerprints": ["short-fp", "long-fp"]},
        ]
        cases = [
            ("long", {"max_tokens": 32}),
            ("short", {"max_tokens": 24}),
        ]

        probe = evaluate_isolation_probe(
            baseline, waves, scheduler_delta(), cases, 25.0
        )

        self.assertTrue(probe["passed"])
        self.assertFalse(probe["row_two_steps_positive"])

    def test_c2_row_batch_and_throughput_pass_while_c4_is_diagnostic(self) -> None:
        baseline = {"measurements": {"1": measurement(100.0, 100.0)}}
        batched = {
            "measurements": {
                "1": measurement(99.0, 103.0),
                "2": measurement(160.0, 180.0, row_two_steps=12),
                "4": measurement(1.0, 10_000.0, ("different-a", "different-b")),
            }
        }

        acceptance = evaluate_acceptance(
            baseline, batched, 1.5, 1.05, isolation_probe()
        )

        self.assertTrue(acceptance["passed"])
        self.assertEqual(12, acceptance["c2_step_batch_size_2_total"])
        self.assertFalse(
            acceptance["concurrency_4_plus_diagnostics"]["4"][
                "exact_response_fingerprints"
            ]
        )

    def test_missing_row_two_steps_fails(self) -> None:
        baseline = {"measurements": {"1": measurement(100.0, 100.0)}}
        batched = {
            "measurements": {
                "1": measurement(100.0, 100.0),
                "2": measurement(180.0, 180.0, row_two_steps=0),
            }
        }

        acceptance = evaluate_acceptance(
            baseline, batched, 1.5, 1.05, isolation_probe()
        )

        self.assertFalse(acceptance["passed"])
        self.assertFalse(acceptance["c2_row_two_steps_positive"])

    def test_c2_speedup_and_exact_fingerprint_are_gating(self) -> None:
        baseline = {"measurements": {"1": measurement(100.0, 100.0)}}
        batched = {
            "measurements": {
                "1": measurement(100.0, 100.0),
                "2": measurement(
                    140.0, 180.0, ("swapped-b", "swapped-a"), row_two_steps=8
                ),
            }
        }

        acceptance = evaluate_acceptance(
            baseline, batched, 1.5, 1.05, isolation_probe()
        )

        self.assertFalse(acceptance["passed"])
        self.assertFalse(acceptance["c2_exact_response_fingerprints"])
        self.assertLess(acceptance["c2_aggregate_speedup"], 1.5)

    def test_isolation_probe_is_gating(self) -> None:
        baseline = {"measurements": {"1": measurement(100.0, 100.0)}}
        batched = {
            "measurements": {
                "1": measurement(100.0, 100.0),
                "2": measurement(180.0, 180.0, row_two_steps=8),
            }
        }

        acceptance = evaluate_acceptance(
            baseline, batched, 1.5, 1.05, isolation_probe(False)
        )

        self.assertFalse(acceptance["passed"])


if __name__ == "__main__":
    unittest.main()
