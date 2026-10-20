# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0

import sys
import unittest
import copy
import json
import tempfile
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent))
import stress_context_cuda as subject


def diagnostics(*, checkpoint=True, effective=500):
    return {"policy": "reject", "checkpoint_applied": checkpoint, "effective_input_tokens": effective,
            "original_input_tokens": effective, "kv_cache_mode": "exact_hybrid", "kv_cache_version": "v1",
            "target_input_tokens": 8192, "reserved_output_tokens": 64}


def grammar_stream(text='{"answer":"amber-ant","source":"checkpoint"}'):
    return [{"choices": [], "context": diagnostics()},
            {"choices": [{"delta": {"content": text[:12]}}]},
            {"choices": [{"delta": {"content": text[12:]}}]},
            {"choices": [{"delta": {}, "finish_reason": "stop"}]}, {"done": True}]


class StressContextCudaTests(unittest.TestCase):
    def test_failed_campaign_replaces_stale_success_evidence(self):
        def fail_after_compaction(client, model, filler_turns, evidence):
            evidence["cases"] = {"compact_checkpoint": {"memory": {"conversation": "wrong fact"}}}
            raise AssertionError("incomplete grammar output")

        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "stress.json"
            output.write_text('{"evidence_passed":true}')
            with mock.patch.object(sys, "argv", ["stress", "--model", "gemma-4-E4B", "--output", str(output)]), \
                    mock.patch.object(subject, "run_stress", side_effect=fail_after_compaction), \
                    mock.patch("builtins.print"):
                with self.assertRaisesRegex(AssertionError, "incomplete grammar"):
                    subject.main()
            evidence = json.loads(output.read_text())
        self.assertFalse(evidence["evidence_passed"])
        self.assertFalse(evidence["qualified"])
        self.assertEqual(evidence["error_type"], "AssertionError")
        self.assertIn("request_timings", evidence)
        self.assertEqual(evidence["cases"]["compact_checkpoint"]["memory"]["conversation"], "wrong fact")

    def test_managed_requires_checkpoint_and_budget(self):
        response = {"context": diagnostics(effective=8192)}
        subject.assert_managed(response, checkpoint=True)
        response["context"]["checkpoint_applied"] = False
        with self.assertRaisesRegex(AssertionError, "checkpoint state"):
            subject.assert_managed(response, checkpoint=True)

    def test_cuda_attestation_is_fail_closed(self):
        good = [{"path": "/ai/v1/generate", "inference_backend": "cuda", "inference_route": "direct-eager", "inference_pid": "42"}]
        subject.assert_cuda_timings(good)
        bad = [{**good[0], "inference_backend": "native"}]
        with self.assertRaisesRegex(AssertionError, "did not attest CUDA"):
            subject.assert_cuda_timings(bad)

    def test_stream_requires_metadata_before_tokens_and_done(self):
        events = grammar_stream()
        self.assertEqual(subject.assert_stream(events)["answer"], "amber-ant")
        with self.assertRaisesRegex(AssertionError, "precede"):
            subject.assert_stream([events[1], events[0], *events[2:]])

    def test_stream_rejects_truncated_invalid_and_incorrect_answers(self):
        for text in ('{', '[]', '{"answer":"wrong","source":"checkpoint"}',
                     '{"answer":"amber-ant","source":"wrong"}'):
            with self.subTest(text=text), self.assertRaises((AssertionError, ValueError)):
                subject.assert_stream(grammar_stream(text))
        events = grammar_stream()
        events[-2]["choices"][0]["finish_reason"] = "length"
        with self.assertRaisesRegex(AssertionError, "complete normally"):
            subject.assert_stream(events)

    def test_managed_rejects_missing_or_wrong_kv_and_reservations(self):
        for field, bad in (("kv_cache_mode", "full"), ("kv_cache_version", "v2"),
                           ("checkpoint_applied", 1), ("effective_input_tokens", -1),
                           ("effective_input_tokens", True), ("effective_input_tokens", 8193),
                           ("target_input_tokens", 16384), ("reserved_output_tokens", 32)):
            with self.subTest(field=field, bad=bad):
                context = diagnostics()
                context[field] = bad
                with self.assertRaises(AssertionError):
                    subject.assert_managed({"context": context}, checkpoint=True)
                del context[field]
                with self.assertRaises(AssertionError):
                    subject.assert_managed({"context": context}, checkpoint=True)

    def test_stream_rejects_empty_missing_finish_and_duplicate_metadata(self):
        events = grammar_stream()
        for invalid in ([events[0], events[-1]], events[:-1], events[:-2] + [events[-1]],
                        [events[0], *events], [*events, events[-1]],
                        [*events[:-1], events[1], events[-1]]):
            with self.subTest(events=invalid), self.assertRaises(AssertionError):
                subject.assert_stream(invalid)

    def test_latency_summary_reports_measurements_without_threshold(self):
        result = subject.latency_summary([{"elapsed_ms": value} for value in (1, 2, 3, 4, 100)])
        self.assertEqual(result["count"], 5)
        self.assertEqual(result["p95_ms"], 100)
        self.assertNotIn("passed", result)

    def test_response_text_rejects_missing_content(self):
        self.assertEqual(subject.response_text({"choices": [{"message": {"content": "{}"}}]}), "{}")
        with self.assertRaisesRegex(AssertionError, "no text"):
            subject.response_text({"choices": [{"message": {"content": None}}]})

    def test_stress_run_preserves_effective_prompt_inputs(self):
        class Client:
            def __init__(self):
                self.timings_ms = []
                self.requests = []

            def post(self, path, body):
                self.requests.append(copy.deepcopy(body))
                if path.endswith("context/prepare"):
                    compact = body["mode"] == "compact"
                    context = {
                        "policy": body["mode"],
                        "effective_input_tokens": 500 if compact else 7600,
                        "original_input_tokens": 9000,
                        "windowed": not compact,
                        "compacted": compact,
                        "dropped_messages": 2,
                    }
                    result = {"messages": copy.deepcopy(body["messages"]), "context": context}
                    if compact:
                        result["checkpoint"] = {
                            "version": "v1",
                            "memory": {"conversation": "FACT_MARKER_CTX_V1: amber-ant", "tool_evidence": [{"tool_call_id": "call_ctx_v1", "tool_name": "lookup_fact", "facts": "TOOL_FACT_MARKER_CTX_V1: amber-ant"}]},
                            **{field: "a" * 64 for field in ("covered_prefix_sha256", "memory_sha256", "model_revision", "tokenizer_revision", "template_revision")},
                        }
                    return 200, result
                checkpoint = body["context_management"].get("checkpoint") is not None
                effective = 500 if checkpoint else 7600
                content = '{"answer":"amber-ant","source":"checkpoint"}' if body.get("response_format") else "amber-ant"
                self.timings_ms.append({"path": path, "elapsed_ms": 10, "inference_backend": "cuda", "inference_route": "direct-eager", "inference_pid": "42"})
                return 200, {"choices": [{"message": {"content": content}}], "usage": {}, "context": diagnostics(checkpoint=checkpoint, effective=effective)}

            def stream(self, path, body):
                self.requests.append(copy.deepcopy(body))
                self.timings_ms.append({"path": path, "elapsed_ms": 10, "inference_backend": "cuda", "inference_route": "direct-eager", "inference_pid": "42"})
                return grammar_stream()

        client = Client()
        evidence = subject.run_stress(client, "gemma-4-E4B", 1)
        generates = [request for request in client.requests if "context_management" in request]
        self.assertGreaterEqual(len(generates), 5)
        for request in generates:
            self.assertIn("tools", request)
            self.assertEqual(request["tool_choice"], "auto")
            self.assertEqual(request["chat_template_kwargs"], {"enable_thinking": False})
            self.assertEqual(request["max_tokens"], 64)
        self.assertEqual(evidence["cases"]["long_context_generation"][0]["context"]["effective_input_tokens"], 7600)


if __name__ == "__main__":
    unittest.main()
