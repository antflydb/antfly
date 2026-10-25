# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import io
import json
import struct
from pathlib import Path
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from qualify_attention_match_queries import (
    read_response,
    parse_capture,
    percentile,
    summarize_performance,
    performance_schedule,
    requests,
    compare_performance,
    summarize_memory,
    wait_server_ready,
)
import qualify_attention_match_queries as qualification


def stream(*events):
    return io.BytesIO(
        b"".join(
            b"data: "
            + (event.encode() if isinstance(event, str) else json.dumps(event).encode())
            + b"\n\n"
            for event in events
        )
    )


class ServerReadinessTests(unittest.TestCase):
    def test_listening_announcement_does_not_bypass_health_check(self):
        process, log, response = MagicMock(), MagicMock(), MagicMock()
        process.poll.return_value = None
        log.read_text.return_value = "listening on 127.0.0.1:8100"
        response.__enter__.return_value.status = 200
        with (
            patch(
                "qualify_attention_match_queries.urllib.request.urlopen",
                side_effect=[ConnectionResetError(), response],
            ) as request,
            patch("qualify_attention_match_queries.time.sleep") as sleep,
        ):
            wait_server_ready(process, log, 8100)
        self.assertEqual(request.call_count, 2)
        sleep.assert_called_once()

    def test_startup_failure_and_missing_health_fail_closed(self):
        process, log = MagicMock(), MagicMock()
        process.poll.return_value = None
        for failure in ("AddressInUse", "InferenceWorkerExited"):
            log.read_text.return_value = "listening on 127.0.0.1:8100\n" + failure
            with (
                self.subTest(failure=failure),
                patch(
                    "qualify_attention_match_queries.urllib.request.urlopen"
                ) as request,
                self.assertRaises(RuntimeError),
            ):
                wait_server_ready(process, log, 8100)
            request.assert_not_called()
        log.read_text.return_value = "listening on 127.0.0.1:8100"
        with (
            patch(
                "qualify_attention_match_queries.urllib.request.urlopen",
                side_effect=ConnectionResetError(),
            ),
            self.assertRaises(TimeoutError),
        ):
            wait_server_ready(process, log, 8100, timeout=0)


class StreamingEvidenceTests(unittest.TestCase):
    def test_error_event_preserves_server_diagnostic(self):
        with self.assertRaisesRegex(RuntimeError, "CompactionFitRejected"):
            read_response(
                io.BytesIO(
                    b'event: error\ndata: {"message":"CompactionFitRejected"}\n\n'
                ),
                True,
            )

    def test_assembles_content_and_terminal_usage(self):
        result = read_response(
            stream(
                {
                    "choices": [
                        {
                            "index": 0,
                            "delta": {"role": "assistant", "content": "violet-"},
                        }
                    ]
                },
                {
                    "choices": [
                        {
                            "index": 0,
                            "delta": {"content": "729"},
                            "finish_reason": "stop",
                        }
                    ]
                },
                {
                    "choices": [],
                    "usage": {
                        "prompt_tokens": 27,
                        "completion_tokens": 4,
                        "total_tokens": 31,
                    },
                },
                "[DONE]",
            ),
            True,
        )
        self.assertEqual(
            result["choices"],
            [
                {
                    "index": 0,
                    "message": {"role": "assistant", "content": "violet-729"},
                    "finish_reason": "stop",
                }
            ],
        )
        self.assertEqual(result["usage"]["total_tokens"], 31)

    def test_rejects_truncated_or_unaccounted_completion(self):
        for events in (
            (),
            ("[DONE]",),
            ({"choices": [{"finish_reason": "stop"}]}, "[DONE]"),
            ({"usage": {"total_tokens": 31}}, "[DONE]"),
        ):
            with self.subTest(events=events), self.assertRaises(AssertionError):
                read_response(stream(*events), True)

    def test_rejects_error_and_unexpected_choices(self):
        with self.assertRaises(RuntimeError):
            read_response(stream({"error": "ResourceUnavailable"}), True)
        with self.assertRaises(RuntimeError):
            read_response(io.BytesIO(b"event: error\n"), True)
        with self.assertRaises(AssertionError):
            read_response(
                stream({"choices": [{"index": 1, "delta": {"content": "extra"}}]}), True
            )


class CaptureEvidenceTests(unittest.TestCase):
    def test_fixture_provenance_rejects_stale_geometry_and_truncated_payload(self):
        capture = dict(source_tokens=2050, train=2, heldout=1, query_sha256="b" * 64)
        data = struct.pack("<6f", 1, 2, 1, 2, 1, 1) + bytes(28)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "fixture.f32"
            path.write_bytes(data)
            evidence = qualification.fitting_fixture_evidence(path, capture, False)
            self.assertEqual(evidence["rows"], 2)
            self.assertEqual(evidence["query_sha256"], capture["query_sha256"])
            self.assertEqual(len(evidence["sha256"]), 64)
            with self.assertRaises(AssertionError):
                qualification.fitting_fixture_evidence(path, capture, True)
            path.write_bytes(data[:-1])
            with self.assertRaises(AssertionError):
                qualification.fitting_fixture_evidence(path, capture, False)
            path.write_bytes(struct.pack("<6f", 1, float("nan"), 1, 2, 1, 1))
            with self.assertRaises(AssertionError):
                qualification.fitting_fixture_evidence(path, capture, False)

    def test_fit_evidence_requires_phases_compression_and_no_installation(self):
        row = dict(
            source_tokens=2117,
            snapshot_ns=1,
            fit_ns=2,
            export_ns=3,
            upload_ns=4,
            total_ns=15,
            diagnostic_only=1,
            installed=0,
            completed_chunks=2,
            export_sha256="a" * 64,
            source_host_bytes=100,
            candidate_host_bytes=80,
            candidate_device_bytes=40,
            export_bytes=90,
        )
        requests = [{"response": {"usage": {"prompt_tokens": 2117}}}]
        qualification.validate_fit_evidence([row], requests)
        qualification.validate_fit_evidence(
            [{**row, "export_version": 2}], requests, export_version_required=2
        )
        for version in (None, 0, 1, 3):
            with (
                self.subTest(version=version),
                self.assertRaisesRegex(AssertionError, "wire version"),
            ):
                qualification.validate_fit_evidence(
                    [{**row, "export_version": version}],
                    requests,
                    export_version_required=2,
                )
        for key, value in (
            ("installed", 1),
            ("source_tokens", 27),
            ("completed_chunks", 0),
            ("total_ns", 9),
            ("fit_ns", 0),
            ("export_sha256", ""),
            ("candidate_device_bytes", 0),
        ):
            with self.subTest(key=key), self.assertRaises(AssertionError):
                qualification.validate_fit_evidence([{**row, key: value}], requests)
        with self.assertRaises(AssertionError):
            qualification.validate_fit_evidence([], requests)

    def test_fit_rejection_and_export_digest_parse(self):
        self.assertEqual(
            parse_capture("phase=fit error=CompactionFitRejected completed_chunks=0"),
            dict(phase="fit", error="CompactionFitRejected", completed_chunks=0),
        )
        self.assertEqual(
            parse_capture("export_sha256=" + "a" * 64)["export_sha256"], "a" * 64
        )

    def test_structured_log_final_integer_and_float_exponent(self):
        row = parse_capture(
            'source_tokens=27 repeat_mode=exact query_max_absolute=0 query_max_relative_l2=1e-9 repeat_kernel_launches=42"}'
        )
        self.assertEqual(row["repeat_kernel_launches"], 42)
        self.assertEqual(row["repeat_mode"], "exact")
        self.assertEqual(row["query_max_relative_l2"], 1e-9)

    def test_rejects_nonfinite_numeric_evidence(self):
        for value in ("nan", "inf", "-inf"):
            with self.subTest(value=value), self.assertRaises(AssertionError):
                parse_capture("query_max_absolute=" + value)


class PerformanceEvidenceTests(unittest.TestCase):
    def test_binary_comparison_schedules_abba_and_detects_binary_changes(self):
        for mutate in (False, True):
            with (
                self.subTest(mutate=mutate),
                tempfile.TemporaryDirectory() as temporary,
            ):
                root = Path(temporary)
                candidate, baseline, output = (
                    root / name for name in ("candidate", "baseline", "result.json")
                )
                candidate.write_bytes(b"new binary")
                baseline.write_bytes(b"saved binary")
                model = root / "model"
                model.mkdir()
                (model / "weights").write_bytes(b"same model")
                launches = []

                def run(args, probe, evidence, label):
                    self.assertTrue(probe)
                    launches.append(
                        (args.server, args.repeat_mode, args.port, args.repetitions)
                    )
                    total = args.warmups + args.repetitions
                    evidence[label] = {
                        "requests": [
                            {
                                "warmup": i < args.warmups,
                                "body": {},
                                "elapsed_s": 54 if args.server == candidate else 60,
                                "response": {
                                    "choices": [{"message": {"content": "same"}}],
                                    "usage": {},
                                },
                            }
                            for i in range(total)
                        ],
                        "captures": [
                            {
                                "repeat_mode": "exact_split",
                                "source_tokens": 8189,
                                "elapsed_ns": (16 if args.server == candidate else 22)
                                * 10**9,
                                "query_sha256": "same",
                            }
                            for _ in range(total)
                        ],
                    }
                    if mutate:
                        baseline.write_bytes(b"changed during qualification")

                argv = [
                    "qualify",
                    "--server",
                    str(candidate),
                    "--compare-server",
                    str(baseline),
                    "--models-dir",
                    str(root),
                    "--model",
                    "model",
                    "--output",
                    str(output),
                    "--mode",
                    "performance",
                    "--repeat-mode",
                    "exact_split",
                    "--compare-repeat-mode",
                    "exact_split",
                    "--port",
                    "8100",
                ]
                with (
                    patch("sys.argv", argv),
                    patch.object(qualification, "run_server", side_effect=run),
                    patch.object(
                        qualification,
                        "git_provenance",
                        return_value={"source_snapshot_sha256": "same"},
                    ),
                    patch.object(
                        qualification.subprocess,
                        "check_output",
                        return_value="test GPU",
                    ),
                    patch("builtins.print"),
                ):
                    self.assertEqual(qualification.main(), int(mutate))
                result = json.loads(output.read_text())
                self.assertEqual(
                    [entry[0] for entry in launches],
                    [candidate, baseline, baseline, candidate],
                )
                self.assertEqual(
                    [entry[1:] for entry in launches],
                    [("exact_split", port, 5) for port in (8100, 8102, 8104, 8106)],
                )
                self.assertEqual(result["passed"], not mutate)
                if mutate:
                    self.assertIn("comparison_binary changed", result["error"])
                else:
                    self.assertTrue(result["comparison"][0]["speed_gate_met"])
                    self.assertFalse(result["warm_memory"]["memory_gate_met"])

    def test_request_and_collection_metrics_have_separate_scopes(self):
        records = [
            {
                "variant": "candidate",
                "requests": [
                    {"warmup": i == 0, "elapsed_s": wall}
                    for i, wall in enumerate([999, 50, 52])
                ],
                "captures": [
                    {
                        "repeat_mode": "exact_split",
                        "source_tokens": 8189,
                        "elapsed_ns": 17 * 10**9,
                    }
                    for _ in range(3)
                ],
            }
        ]
        collection = summarize_performance(records)[0]
        request = summarize_performance(records, metric="request")[0]
        self.assertEqual(collection["p50_s"], 17)
        self.assertEqual(request["p50_s"], 51)
        self.assertEqual(request["samples"], 2)
        self.assertEqual(request["metric"], "request")
        for invalid in (0, -1, float("nan"), float("inf"), 16):
            records[0]["requests"][1]["elapsed_s"] = invalid
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                summarize_performance(records, metric="request")
        records[0]["captures"].pop()
        with self.assertRaisesRegex(ValueError, "count mismatch"):
            summarize_performance(records)

    def test_alternates_routes_and_preserves_odd_sample_count(self):
        schedule = performance_schedule("exact_split", "exact", 11)
        self.assertEqual(
            [route for route, _ in schedule],
            ["exact_split", "exact", "exact", "exact_split"],
        )
        for route in ("exact", "exact_split"):
            self.assertEqual(
                sum(count for name, count in schedule if name == route), 11
            )
        for arguments in (("exact", "exact", 10), ("exact_split", "exact", 1)):
            with self.assertRaises(ValueError):
                performance_schedule(*arguments)

    def test_excludes_warmups_and_uses_nearest_rank_p95(self):
        records = [
            {
                "requests": [{"warmup": i == 0} for i in range(21)],
                "captures": [
                    {
                        "repeat_mode": "exact_split",
                        "source_tokens": 8189,
                        "elapsed_ns": value * 10**9,
                    }
                    for value in [999] + list(range(1, 21))
                ],
            }
        ]
        row = summarize_performance(records)[0]
        self.assertEqual(row["samples"], 20)
        self.assertEqual(row["p50_s"], 10.5)
        self.assertEqual(row["p95_s"], 19)
        self.assertEqual(row["max_s"], 20)
        with self.assertRaises(ValueError):
            percentile([], 0.95)

    def test_memory_gate_fails_closed_for_missing_samples_errors_and_excess(self):
        report = {
            "samples": 10,
            "errors": [],
            "peak_host_rss_bytes": 3 * 1024**3,
            "peak_gpu_memory_bytes": 10 * 1024**3,
        }
        self.assertTrue(
            summarize_memory([{"memory_after_first_request": report}])[
                "memory_gate_met"
            ]
        )
        for row in (
            {},
            report | {"errors": ["sampler failure"]},
            report | {"samples": 1},
            report | {"peak_gpu_memory_bytes": None},
            report | {"peak_host_rss_bytes": 9 * 1024**3},
        ):
            with self.subTest(row=row):
                self.assertFalse(
                    summarize_memory([{"memory_after_first_request": row}])[
                        "memory_gate_met"
                    ]
                )
        self.assertFalse(summarize_memory([])["memory_gate_met"])

    def test_same_route_binary_samples_stay_separate(self):
        records = [
            {
                "variant": variant,
                "requests": [{"warmup": False} for _ in range(10)],
                "captures": [
                    {
                        "repeat_mode": "exact_split",
                        "source_tokens": 8189,
                        "elapsed_ns": seconds * 10**9,
                    }
                    for _ in range(10)
                ],
            }
            for variant, seconds in (("candidate", 16), ("baseline", 22))
        ]
        rows = summarize_performance(records)
        self.assertEqual(len(rows), 2)
        self.assertEqual({row["repeat_mode"] for row in rows}, {"exact_split"})
        self.assertEqual({row["samples"] for row in rows}, {10})
        comparison = compare_performance(rows, "candidate", "baseline")[0]
        self.assertTrue(comparison["speed_gate_met"])
        self.assertAlmostEqual(comparison["p50_reduction_fraction"], 1 - 16 / 22)

    def test_speed_gate_requires_both_percentiles_and_enough_samples(self):
        baseline = {
            "repeat_mode": "exact",
            "source_tokens": 8189,
            "fixture": "fact",
            "samples": 10,
            "p50_s": 40,
            "p95_s": 40,
        }
        candidate = baseline | {"repeat_mode": "exact_split", "p50_s": 30, "p95_s": 33}
        self.assertTrue(
            compare_performance([baseline, candidate], "exact_split", "exact")[0][
                "speed_gate_met"
            ]
        )
        self.assertFalse(
            compare_performance(
                [baseline, candidate], "exact_split", "exact", warmups=0
            )[0]["speed_gate_met"]
        )
        for change in ({"samples": 9}, {"p95_s": 35}, {"p50_s": 35}):
            with self.subTest(change=change):
                self.assertFalse(
                    compare_performance(
                        [baseline, candidate | change], "exact_split", "exact"
                    )[0]["speed_gate_met"]
                )

    def test_fixtures_share_answer_but_have_distinct_inputs(self):
        bodies = [
            next(requests("model", "f16", [0], True, fixture))
            for fixture in ("fact", "code", "tool")
        ]
        self.assertEqual(len({body["messages"][0]["content"] for body in bodies}), 3)
        for body in bodies:
            self.assertIn("violet-729", body["messages"][0]["content"])
            self.assertEqual(body["temperature"], 0)

    def test_phase_fields_and_large_counters(self):
        row = parse_capture(
            "phase=repeat repeat_mode=exact_split capture_host_wait_ns=37385130323 download_bytes=939180032"
        )
        self.assertEqual(row["phase"], "repeat")
        self.assertEqual(row["capture_host_wait_ns"], 37385130323)


if __name__ == "__main__":
    unittest.main()


class WholeHeadEvidenceTests(unittest.TestCase):
    def test_quality_metrics_parse_as_floats_and_cannot_bypass_gate(self):
        parsed = parse_capture(
            "max_head_mass_error=0.0363989 max_head_output_rmse=6.2e-3 validated_heads=8"
        )
        self.assertEqual(parsed["validated_heads"], 8)
        self.assertAlmostEqual(parsed["max_head_output_rmse"], 0.0062)
        row = dict(
            source_tokens=8189,
            snapshot_ns=1,
            fit_ns=2,
            export_ns=3,
            upload_ns=4,
            total_ns=15,
            diagnostic_only=1,
            installed=0,
            completed_chunks=48,
            export_sha256="a" * 64,
            source_host_bytes=10,
            candidate_host_bytes=5,
            candidate_device_bytes=4,
            export_bytes=6,
            residual_fit=1,
            **parsed,
        )
        request = {"response": {"usage": {"prompt_tokens": 8189}}}
        qualification.validate_fit_evidence([row], [request])
        for key, value in (
            ("validated_heads", 0),
            ("max_head_mass_error", 0.051),
            ("max_head_output_rmse", float("nan")),
        ):
            with self.subTest(key=key), self.assertRaises(AssertionError):
                qualification.validate_fit_evidence([{**row, key: value}], [request])


class AllHeadFixtureEvidenceTests(unittest.TestCase):
    def test_manifest_requires_completed_unique_heads_matching_files(self):
        capture = dict(source_tokens=2050, train=2, heldout=1, query_sha256="b" * 64)
        data = struct.pack("<6f", 1, 2050, 1, 2, 1, 1) + bytes(4 * (4100 + 3))
        log = (
            "attention_match_fit_fixture donor_layer=5 head=0 rows=2050 train_rows=2 heldout_rows=1\n"
            "attention_match_fit_fixture donor_layer=5 head=1 rows=2050 train_rows=2 heldout_rows=1\n"
            "attention_match_fit_fixture_complete heads=2\n"
        )
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "fixture.f32"
            path.write_bytes(data)
            Path(f"{path}.layer5.head1.f32").write_bytes(data)
            result = qualification.fitting_fixtures_evidence(path, capture, log)
            self.assertEqual([row["head"] for row in result], [0, 1])
            for bad in (
                log.replace("head=1", "head=0"),
                log.replace("heads=2", "heads=3"),
                log.split("attention_match_fit_fixture_complete")[0],
                log.replace("train_rows=2", "train_rows=4"),
            ):
                with self.subTest(bad=bad), self.assertRaises(AssertionError):
                    qualification.fitting_fixtures_evidence(path, capture, bad)


class ActualSavingsEvidenceTests(unittest.TestCase):
    def test_reported_savings_must_use_actual_global_bytes(self):
        row = dict(
            source_tokens=8189,
            snapshot_ns=1,
            fit_ns=2,
            export_ns=3,
            upload_ns=4,
            total_ns=15,
            diagnostic_only=1,
            installed=0,
            completed_chunks=48,
            export_sha256="a" * 64,
            source_host_bytes=100,
            candidate_host_bytes=70,
            candidate_device_bytes=40,
            export_bytes=60,
            source_global_bytes=100,
            candidate_global_bytes=60,
            actual_global_savings_ratio=100 / 60,
            preserved_heads=2,
        )
        request = {"response": {"usage": {"prompt_tokens": 8189}}}
        qualification.validate_fit_evidence([row], [request])
        for value in (4, float("inf"), float("nan")):
            with self.subTest(value=value), self.assertRaises(AssertionError):
                qualification.validate_fit_evidence(
                    [{**row, "actual_global_savings_ratio": value}], [request]
                )
        # Completed private chunks can all be discarded by per-head fallback.
        for candidate_bytes in (100, 110):
            with (
                self.subTest(candidate_bytes=candidate_bytes),
                self.assertRaises(AssertionError),
            ):
                qualification.validate_fit_evidence(
                    [
                        {
                            **row,
                            "candidate_global_bytes": candidate_bytes,
                            "actual_global_savings_ratio": 100 / candidate_bytes,
                        }
                    ],
                    [request],
                )
        qualification.validate_fit_evidence([row], [request], minimum_savings=1.5)
        with self.assertRaisesRegex(AssertionError, "requested minimum"):
            qualification.validate_fit_evidence([row], [request], minimum_savings=2)
        without_bytes = {
            key: value for key, value in row.items() if key != "source_global_bytes"
        }
        with self.assertRaisesRegex(AssertionError, "missing actual"):
            qualification.validate_fit_evidence([without_bytes], [request])


class CudaFitEvidenceTests(unittest.TestCase):
    def test_installed_cache_must_actually_serve_and_advance(self):
        row = dict(
            source_tokens=27,
            logical_tokens=31,
            invalid=0,
            attention_calls=140,
            donor_appends=96,
            allocated_device_bytes=100,
        )
        request = {"response": {"usage": {"prompt_tokens": 27, "completion_tokens": 5}}}
        qualification.validate_serving_evidence([row], [request])
        for change in (
            {"logical_tokens": 27},
            {"logical_tokens": 33},
            {"invalid": 1},
            {"attention_calls": 0},
            {"donor_appends": 0},
            {"source_tokens": 28},
        ):
            with self.subTest(change=change), self.assertRaises(AssertionError):
                qualification.validate_serving_evidence([{**row, **change}], [request])
        with self.assertRaises(AssertionError):
            qualification.validate_serving_evidence([], [request])

    def test_installation_requires_explicit_evidence_and_timing(self):
        row = dict(
            source_tokens=27,
            snapshot_ns=1,
            fit_ns=2,
            export_ns=3,
            upload_ns=4,
            total_ns=15,
            diagnostic_only=1,
            installed=1,
            install_ns=2,
            completed_chunks=0,
            export_sha256="a" * 64,
            source_host_bytes=100,
            candidate_host_bytes=100,
            candidate_device_bytes=40,
            export_bytes=60,
            cuda_fit=1,
        )
        request = {"response": {"usage": {"prompt_tokens": 27}}}
        qualification.validate_fit_evidence(
            [row], [request], cuda_required=True, install_required=True
        )
        with self.assertRaisesRegex(AssertionError, "installation evidence"):
            qualification.validate_fit_evidence([row], [request])
        for change in ({"installed": 0}, {"install_ns": 0}, {"install_ns": 20}):
            with self.subTest(change=change), self.assertRaises(AssertionError):
                qualification.validate_fit_evidence(
                    [{**row, **change}], [request], install_required=True
                )

    def test_cuda_request_cannot_silently_report_cpu_fit(self):
        row = dict(
            source_tokens=27,
            snapshot_ns=1,
            fit_ns=2,
            export_ns=3,
            upload_ns=4,
            total_ns=15,
            diagnostic_only=1,
            installed=0,
            completed_chunks=0,
            export_sha256="a" * 64,
            source_host_bytes=100,
            candidate_host_bytes=100,
            candidate_device_bytes=40,
            export_bytes=60,
            cuda_fit=1,
        )
        request = {"response": {"usage": {"prompt_tokens": 27}}}
        qualification.validate_fit_evidence([row], [request], cuda_required=True)
        for value in (0, None):
            with (
                self.subTest(value=value),
                self.assertRaisesRegex(AssertionError, "native CUDA"),
            ):
                qualification.validate_fit_evidence(
                    [{**row, "cuda_fit": value}], [request], cuda_required=True
                )

    def test_cooperative_fit_requires_actual_execution_evidence(self):
        row = dict(
            source_tokens=2117,
            snapshot_ns=1,
            fit_ns=2,
            export_ns=3,
            upload_ns=4,
            total_ns=15,
            diagnostic_only=1,
            installed=0,
            completed_chunks=1,
            export_sha256="a" * 64,
            source_host_bytes=100,
            candidate_host_bytes=100,
            candidate_device_bytes=40,
            export_bytes=60,
            cuda_fit=1,
            cooperative_fit=1,
            cooperative_launches=256,
            cooperative_restarts=1,
        )
        requests = [{"response": {"usage": {"prompt_tokens": 2117}}}]
        qualification.validate_fit_evidence([row], requests, cooperative_required=True)
        for change in (
            {"cooperative_fit": 0},
            {"cuda_fit": 0},
            {"cooperative_launches": 0},
            {"cooperative_launches": None},
            {"cooperative_restarts": -1},
            {"cooperative_restarts": 257},
        ):
            with self.subTest(change=change), self.assertRaises(AssertionError):
                qualification.validate_fit_evidence(
                    [{**row, **change}], requests, cooperative_required=True
                )

    def test_parallel_fit_requires_worker_count_and_scratch_reuse(self):
        row = dict(
            source_tokens=27,
            snapshot_ns=1,
            fit_ns=2,
            export_ns=3,
            upload_ns=4,
            total_ns=15,
            diagnostic_only=1,
            installed=0,
            completed_chunks=0,
            export_sha256="a" * 64,
            source_host_bytes=100,
            candidate_host_bytes=100,
            candidate_device_bytes=40,
            export_bytes=60,
            fit_workers=4,
            workspace_reuses=20,
        )
        requests = [{"response": {"usage": {"prompt_tokens": 27}}}]
        qualification.validate_fit_evidence([row], requests, workers_required=4)
        row["host_workspace_limit_bytes"] = 3 * 1024**3
        qualification.validate_fit_evidence(
            [row], requests, host_workspace_required=3 * 1024**3
        )
        for value in (None, 2 * 1024**3):
            with (
                self.subTest(host_workspace=value),
                self.assertRaisesRegex(AssertionError, "host workspace limit"),
            ):
                qualification.validate_fit_evidence(
                    [{**row, "host_workspace_limit_bytes": value}],
                    requests,
                    host_workspace_required=3 * 1024**3,
                )
        for change in ({"fit_workers": 1}, {"workspace_reuses": 0}):
            with (
                self.subTest(change=change),
                self.assertRaisesRegex(AssertionError, "parallel worker"),
            ):
                qualification.validate_fit_evidence(
                    [{**row, **change}], requests, workers_required=4
                )


class WorkerSamplingTests(unittest.TestCase):
    def test_worker_resolution_rejects_missing_and_ambiguous_workers(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            server = root / "server"
            server.touch()
            children = root / "10/task/10/children"
            children.parent.mkdir(parents=True)
            for pid in (11, 12):
                (root / str(pid)).mkdir()
                (root / str(pid) / "exe").symlink_to(server)
            children.write_text("11")
            self.assertEqual(qualification.discover_worker_pid(10, server, root), 11)
            for content in ("", "11 12", "99"):
                children.write_text(content)
                with (
                    self.subTest(content=content),
                    self.assertRaisesRegex(RuntimeError, "private inference worker"),
                ):
                    qualification.discover_worker_pid(10, server, root)
