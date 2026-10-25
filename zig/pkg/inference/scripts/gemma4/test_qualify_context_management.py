# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0

import json
import sys
import tempfile
import unittest
from unittest import mock
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parent))
import qualify_context_management as subject


class ContextQualificationTests(unittest.TestCase):
    def test_provenance_includes_untracked_cuda_sources(self):
        def git_output(command):
            arguments = command[3:]
            if arguments[0] == "status":
                return b"?? kernel.cu\n?? template.cuh\n"
            if arguments[0] == "diff":
                return b""
            if arguments[0] == "ls-files":
                return b"kernel.cu\0template.cuh\0"
            if arguments[0] == "rev-parse":
                return b"a" * 40 + b"\n"
            raise AssertionError(command)

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "kernel.cu").write_text("kernel version 1")
            (root / "template.cuh").write_text("template version 1")
            with mock.patch.object(subject.subprocess, "check_output", side_effect=git_output):
                before = subject.git_provenance(root)
                (root / "template.cuh").write_text("template version 2")
                after = subject.git_provenance(root)
        self.assertEqual(before["untracked_source_files"], 2)
        self.assertNotEqual(before["source_snapshot_sha256"], after["source_snapshot_sha256"])

    def test_fixture_keeps_tool_calls_atomic(self):
        messages = subject.fixture_messages(1)
        subject.assert_tool_atomic(messages)
        split = [message for message in messages if message.get("role") != "tool"]
        with self.assertRaisesRegex(AssertionError, "tool call/result units split"):
            subject.assert_tool_atomic(split)

    def test_window_validator_requires_instructions_and_exact_budget(self):
        source = subject.fixture_messages(1)
        response = {
            "messages": source,
            "context": {"effective_input_tokens": 90, "original_input_tokens": 120, "windowed": True, "dropped_messages": 2},
        }
        subject.validate_window(source, response, 100)
        response["messages"] = source[1:]
        with self.assertRaisesRegex(AssertionError, "instructions"):
            subject.validate_window(source, response, 100)

    def test_sse_parser_preserves_metadata_order(self):
        raw = b'data: {"choices":[],"context":{"policy":"reject"}}\n\ndata: {"choices":[{"delta":{"content":"a"}}]}\n\ndata: [DONE]\n\n'
        events = subject.parse_sse(raw)
        self.assertEqual(events[0]["context"]["policy"], "reject")
        self.assertEqual(events[1]["choices"][0]["delta"]["content"], "a")
        self.assertTrue(events[2]["done"])

    def test_sse_parser_preserves_error_events(self):
        events = subject.parse_sse(b"event: error\ndata: backend failed\n\n")
        self.assertEqual(events, [{"error": "backend failed"}])

    def test_checkpoint_digest_validation(self):
        checkpoint = {"version": "v1"}
        checkpoint.update({field: "a" * 64 for field in ("covered_prefix_sha256", "memory_sha256", "model_revision", "tokenizer_revision", "template_revision")})
        subject.validate_checkpoint(checkpoint)
        checkpoint["memory_sha256"] = "bad"
        with self.assertRaisesRegex(AssertionError, "memory_sha256"):
            subject.validate_checkpoint(checkpoint)

    def test_fixture_is_json_round_trip_stable(self):
        fixture = subject.fixture_messages(2)
        self.assertEqual(fixture, json.loads(json.dumps(fixture)))

    def test_memory_evidence_records_peak_and_process_lifetime(self):
        rows = [
            {"wall_time_s": 10.0, "monotonic_ns": 100, "pid": 42, "start_abstime": 7, "rss_bytes": 100, "phys_footprint_bytes": 80, "lifetime_max_phys_footprint_bytes": 90},
            {"wall_time_s": 20.0, "monotonic_ns": 200, "pid": 42, "start_abstime": 7, "rss_bytes": 120, "phys_footprint_bytes": 110, "lifetime_max_phys_footprint_bytes": 130},
        ]
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "memory.jsonl"
            path.write_text("".join(json.dumps(row) + "\n" for row in rows) + '{"wall_time_s":')
            evidence = subject.memory_evidence(path)
        self.assertEqual(evidence["peak_phys_footprint_bytes"], 110)
        self.assertEqual(evidence["conservative_peak_phys_footprint_bytes"], 130)
        self.assertEqual(evidence["samples"], 2)
        self.assertEqual(evidence["process_lifetimes"], [(42, 7)])
        self.assertEqual(len(evidence["sha256"]), 64)

    def test_qualification_gates_require_runtime_and_explicit_thresholds(self):
        args = SimpleNamespace(
            backend="metal", server_pid=None, max_gpu_memory_bytes=None, max_host_rss_bytes=None,
            qualification_mode=True,
            runs=3,
            max_p95_latency_ms=None,
            max_latency_cv=None,
            max_peak_phys_footprint_bytes=None,
        )
        gates, _ = subject.evaluate_qualification([], args, {}, None, 10.0, 20.0, 100, 200)
        failed = {gate["name"] for gate in gates if not gate["passed"]}
        self.assertIn("backend_executed", failed)
        self.assertIn("memory_samples", failed)
        self.assertIn("latency_threshold_supplied", failed)

    def test_complete_synthetic_evidence_passes_all_gates(self):
        args = SimpleNamespace(
            backend="metal", server_pid=None, max_gpu_memory_bytes=None, max_host_rss_bytes=None,
            qualification_mode=True,
            runs=3,
            max_p95_latency_ms=20.0,
            max_latency_cv=0.20,
            max_peak_phys_footprint_bytes=1000,
        )
        runs = []
        for index, duration in enumerate((100.0, 105.0, 110.0)):
            runs.append({
                "evidence_passed": True,
                "duration_ms": duration,
                "request_timings_ms": [{"elapsed_ms": 10.0 + index, "path": "/ai/v1/generate", "inference_backend": "metal", "inference_route": "compiled-whole-model", "inference_pid": "42", "qualification_group": group} for group in ("prepared_generation", "checkpoint_generation", "checkpoint_stream_generation")],
                "model_revision": "a" * 64,
                "tokenizer_revision": "b" * 64,
                "template_revision": "c" * 64,
                "cases": {"compact": {"checkpoint": {"memory_sha256": "d" * 64}}},
            })
        provenance = {
            "source": {key: "e" * 64 for key in ("head", "status_sha256", "tracked_diff_sha256", "untracked_content_sha256", "source_snapshot_sha256")},
            "server_binary": {"sha256": "f" * 64, "size_bytes": 1},
            "machine": {"system": "Darwin", "machine": "arm64"},
            "hardware_evidence": {
                "schema": "antfly.macos_hardware_evidence.v1",
                "server_pid": 42,
                "device": {"name": "Apple test device"},
            },
        }
        memory = {
            "samples": 2,
            "first_wall_time_s": 9.0,
            "last_wall_time_s": 21.0,
            "first_monotonic_ns": 90,
            "last_monotonic_ns": 210,
            "pids": [42],
            "process_lifetimes": [(42, 7)],
            "peak_phys_footprint_bytes": 900,
            "conservative_peak_phys_footprint_bytes": 900,
        }
        gates, aggregate = subject.evaluate_qualification(runs, args, provenance, memory, 10.0, 20.0, 100, 200)
        self.assertTrue(all(gate["passed"] for gate in gates), gates)
        self.assertEqual(aggregate["request_latency_ms"]["p95"], 12.0)

        memory["peak_phys_footprint_bytes"] = 800
        memory["conservative_peak_phys_footprint_bytes"] = 1100
        gates, _ = subject.evaluate_qualification(runs, args, provenance, memory, 10.0, 20.0, 100, 200)
        by_name = {gate["name"]: gate for gate in gates}
        self.assertFalse(by_name["peak_physical_footprint"]["passed"])
        self.assertEqual(by_name["peak_physical_footprint"]["observed"], 1100)

    def test_runtime_claim_without_instrumented_route_fails(self):
        args = SimpleNamespace(backend="metal", server_pid=None, max_gpu_memory_bytes=None, max_host_rss_bytes=None, qualification_mode=True, runs=3, max_p95_latency_ms=20, max_latency_cv=1, max_peak_phys_footprint_bytes=1000)
        provenance = {"hardware_evidence": {"schema": "antfly.macos_hardware_evidence.v1", "server_pid": 42, "device": {"name": "Apple"}}}
        gates, _ = subject.evaluate_qualification([], args, provenance, None, 1, 2, 100, 200)
        by_name = {gate["name"]: gate for gate in gates}
        self.assertFalse(by_name["backend_executed"]["passed"])
        self.assertFalse(by_name["generation_route_instrumented"]["passed"])

    def test_fact_quality_requires_distinct_labels_values_and_attribution(self):
        checkpoint = {"memory": {
            "conversation": "FACT_MARKER_CTX_V1: amber-ant",
            "tool_evidence": [{"tool_call_id": "call_ctx_v1", "tool_name": "lookup_fact", "facts": "TOOL_FACT_MARKER_CTX_V1=amber-ant"}],
        }}
        subject.validate_fixture_memory(checkpoint)
        checkpoint["memory"]["conversation"] = "TOOL_FACT_MARKER_CTX_V1=amber-ant"
        with self.assertRaisesRegex(AssertionError, "conversation lost"):
            subject.validate_fixture_memory(checkpoint)
        checkpoint["memory"]["conversation"] = "FACT_MARKER_CTX_V1: amber-ant"
        checkpoint["memory"]["tool_evidence"][0]["facts"] = "TOOL_FACT_MARKER_CTX_V1=wrong-value"
        with self.assertRaisesRegex(AssertionError, "tool result"):
            subject.validate_fixture_memory(checkpoint)
        checkpoint["memory"]["tool_evidence"][0]["facts"] = "TOOL_FACT_MARKER_CTX_V1=amber-ant"
        checkpoint["memory"]["tool_evidence"][0]["tool_call_id"] = "wrong-call"
        with self.assertRaisesRegex(AssertionError, "attribution"):
            subject.validate_fixture_memory(checkpoint)

    def test_failed_tool_fact_quality_keeps_checkpoint_and_replaces_stale_success(self):
        source = subject.fixture_messages(21)
        checkpoint = {"version": "v1", "memory": {
            "conversation": "FACT_MARKER_CTX_V1: amber-ant",
            "tool_evidence": [{"tool_call_id": "call_ctx_v1", "tool_name": "lookup_fact", "facts": '{"key":"codename"}'}],
            "unresolved_tool_calls": [],
        }}
        checkpoint.update({field: "a" * 64 for field in (
            "covered_prefix_sha256", "memory_sha256", "model_revision", "tokenizer_revision", "template_revision"
        )})
        window = {"messages": source[:2] + source[-3:], "context": {
            "original_input_tokens": 2000, "effective_input_tokens": 900,
            "windowed": True, "dropped_messages": len(source) - 5,
        }}
        generated = {"context": {
            "policy": "reject", "checkpoint_applied": False,
            "kv_cache_mode": "exact_hybrid", "kv_cache_version": "v1",
            "original_input_tokens": 900, "effective_input_tokens": 900,
            "target_input_tokens": 1024, "reserved_output_tokens": 32,
        }, "usage": {"prompt_tokens": 900, "completion_tokens": 10}}
        client = mock.Mock()
        client.timings_ms = [{"elapsed_ms": 10, "path": "/ai/v1/generate/context/prepare"}]
        client.post.side_effect = [(200, window), (200, generated), (200, {"checkpoint": checkpoint, "context": {"compacted": True}})]
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "evidence.json"
            output.write_text('{"qualified":true,"evidence_passed":true}')
            with mock.patch.object(subject, "Client", return_value=client), mock.patch.object(subject, "git_provenance", return_value={}), mock.patch.object(sys, "argv", ["qualify", "--model", "fixture", "--output", str(output)]):
                with self.assertRaisesRegex(AssertionError, "TOOL_FACT_MARKER_CTX_V1"):
                    subject.main()
            evidence = json.loads(output.read_text())
        self.assertFalse(evidence["qualified"])
        self.assertFalse(evidence["evidence_passed"])
        self.assertEqual(len(evidence["runs"]), 1)
        failed = evidence["runs"][0]
        self.assertFalse(failed["evidence_passed"])
        self.assertEqual(failed["cases"]["compact"]["checkpoint"], checkpoint)
        self.assertEqual(failed["request_timings_ms"], client.timings_ms)
        self.assertIn("TOOL_FACT_MARKER_CTX_V1", failed["error"])
        functional_gate = next(gate for gate in evidence["gates"] if gate["name"] == "functional_cases")
        self.assertFalse(functional_gate["passed"])

    def test_cuda_sampler_filters_to_tested_pid(self):
        pid = __import__("os").getpid()
        output = f"GPU-a, {pid + 1}, 999\nGPU-b, {pid}, 123\n"
        with mock.patch.object(subject.subprocess, "check_output", return_value=output):
            row = subject.sample_cuda_process(pid)
        self.assertEqual(row["pid"], pid)
        self.assertGreater(row["host_rss_bytes"], 0)
        self.assertEqual(row["gpu_allocations"], [{"gpu_uuid": "GPU-b", "used_memory_bytes": 123 * 1024 * 1024}])


if __name__ == "__main__":
    unittest.main()
