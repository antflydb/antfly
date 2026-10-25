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

import copy
import unittest
from compare_matched_latency import compare
from native_benchmark_fixture import MODEL_IDENTITY
from matched_chat import request_identity


def reports():
    common = {
        "passed": True,
        "sampling_qualified": True,
        "smoke": False,
        "mode": "replay",
        "size": "8k",
        "warmups_requested": 5,
        "samples_requested": 100,
        "fixture": {
            "identity": MODEL_IDENTITY,
            "logical_tokens": 8189,
            "checkpoint_sha256_to_reverify": "a" * 64,
            "token_bytes_sha256": "b" * 64,
        },
        "hardware": [
            {"name": "NVIDIA L4", "uuid": "GPU-test", "driver_version": "test"}
        ],
    }
    native = copy.deepcopy(common)
    llama = copy.deepcopy(common)
    native.update(
        input_sha256={"model.gguf": "c" * 64},
        request={
            "temperature": 0,
            "max_tokens": 16,
            "prompt_cache": False,
            "cache_dtype": "f16",
        },
        worker={"server_kind": "standalone", "stopped": True},
    )
    llama.update(
        model_sha256="c" * 64,
        request_settings={
            "temperature": 0,
            "n_predict": 16,
            "cache_prompt": False,
            "repeat_penalty": 1,
        },
        process_group_stopped=True,
        command=["llama-server", "-ctk", "f16", "-ctv", "f16"],
    )
    for report, seconds in [(native, 1.2), (llama, 1.0)]:
        report["observations"] = []
        for index in range(106):
            row = {
                "index": index,
                "passed": True,
                "cold": index == 0,
                "warmup": 0 < index <= 5,
                "total_seconds": seconds,
                "first_content_seconds": seconds / 2,
            }
            if report is native:
                row["result"] = {
                    "usage": {"prompt_tokens": 8189, "completion_tokens": 5},
                    "choices": [
                        {"finish_reason": "stop", "message": {"content": "violet-729"}}
                    ],
                }
            else:
                row["result"] = {
                    "tokens_evaluated": 8189,
                    "tokens_predicted": 5,
                    "content": "violet-729",
                    "tokens": [68002, 236772, 236832, 236778, 236819],
                    "stop_type": "eos",
                    "truncated": False,
                    "timings": {"prompt_n": 8189, "cache_n": 0, "predicted_n": 5},
                }
            report["observations"].append(row)
    shared = {
        "messages": [{"role": "user", "content": "test prompt"}],
        "max_tokens": 16,
        "temperature": 0,
        "stream": True,
        "stream_options": {"include_usage": True},
        "chat_template_kwargs": {"enable_thinking": False},
    }
    native["request"].update(shared)
    llama["request_settings"].pop("n_predict")
    llama["request_settings"].update(
        {key: value for key, value in shared.items() if key != "messages"}
    )
    llama["request"] = dict(shared, cache_prompt=False, repeat_penalty=1)
    native.update(input_mode="chat", endpoint="/ai/v1/generate")
    llama.update(
        input_mode="chat",
        endpoint="/v1/chat/completions",
        template_preflight={
            "verified": True,
            "logical_tokens": 8189,
            "token_bytes_sha256": "b" * 64,
        },
    )
    for report in (native, llama):
        report["chat_request_sha256"] = request_identity(report["request"])
    llama["chat_preflight"] = copy.deepcopy(llama["observations"][0]["result"])
    llama["chat_preflight"]["timings"].update(prompt_ms=800.0, predicted_ms=100.0)
    for row in llama["observations"]:
        row["result"] = {
            "content": "violet-729",
            "finish_reason": "stop",
            "usage": {
                "prompt_tokens": 8189,
                "completion_tokens": 5,
                "total_tokens": 8194,
                "prompt_tokens_details": {"cached_tokens": 0},
            },
            "timings": dict(llama["chat_preflight"]["timings"]),
        }
    return native, llama


class ComparisonTests(unittest.TestCase):
    def test_rejects_pretokenized_or_unlabeled_historical_reports(self):
        for mode in (None, "token_ids"):
            a, b = reports()
            b["input_mode"] = mode
            with self.assertRaisesRegex(ValueError, "chat input"):
                compare(a, b)

    def test_rejects_changed_messages_and_unverified_template(self):
        a, b = reports()
        b["request"]["messages"] = [{"role": "user", "content": "different"}]
        with self.assertRaisesRegex(ValueError, "messages"):
            compare(a, b)
        a, b = reports()
        b["template_preflight"]["verified"] = False
        with self.assertRaisesRegex(ValueError, "verification"):
            compare(a, b)

    def test_eos_inclusive_llama_count_preserves_full_wall_latency(self):
        a, b = reports()
        b["chat_preflight"]["tokens"].append(106)
        b["chat_preflight"]["tokens_predicted"] = 6
        b["chat_preflight"]["timings"]["predicted_n"] = 6
        for row in b["observations"]:
            row["result"]["usage"].update(completion_tokens=6, total_tokens=8195)
            row["result"]["timings"]["predicted_n"] = 6
        value = compare(a, b)
        self.assertEqual(value["output_tokens"], 5)
        self.assertEqual(value["latency_seconds"]["llama"]["total_seconds"]["p50"], 1.0)
        self.assertAlmostEqual(value["antfly_over_llama"]["total_seconds"]["p50"], 1.2)
        self.assertEqual(
            b["observations"][0]["result"]["usage"]["completion_tokens"], 6
        )

    def test_bounded_profile_requires_explicit_matching_scope_and_complete_samples(
        self,
    ):
        a, b = reports()
        for report in (a, b):
            report.update(sampling_profile="bounded", samples_requested=20)
            report["observations"] = report["observations"][:26]
        result = compare(a, b)
        self.assertEqual(result["samples"], {"antfly": 20, "llama": 20})
        self.assertEqual(result["sampling_profile"], "bounded")
        self.assertTrue(result["latency_thresholds_met"])
        b.pop("sampling_profile")
        with self.assertRaises(ValueError):
            compare(a, b)
        b["sampling_profile"] = "bounded"
        b["observations"].pop()
        with self.assertRaises(ValueError):
            compare(a, b)

    def test_matched_complete_campaign_meets_only_scoped_gate(self):
        a, b = reports()
        result = compare(a, b)
        self.assertTrue(result["latency_thresholds_met"])
        self.assertAlmostEqual(result["antfly_over_llama"]["total_seconds"]["p50"], 1.2)
        self.assertFalse(result["production_ready"])
        self.assertFalse(result["portable_restore_comparison_qualified"])

    def test_recomputes_percentiles_and_preserves_slow_tail_failure(self):
        a, b = reports()
        a["latency_seconds"] = {"total_seconds": {"p50": 0.001, "p95": 0.001}}
        for row in a["observations"][-10:]:
            row["total_seconds"] = 1.6
        value = compare(a, b)
        self.assertFalse(value["latency_thresholds_met"])
        self.assertEqual(value["antfly_over_llama"]["total_seconds"]["p95"], 1.6)
        self.assertEqual(value["antfly_over_llama"]["total_seconds"]["p50"], 1.2)

    def test_rejects_smoke_missing_or_failed_samples(self):
        for mutation in [
            lambda r: r.update(smoke=True),
            lambda r: r["observations"].pop(),
            lambda r: r["observations"][-1].update(passed=False),
            lambda r: r["observations"][-1].update(index=0),
        ]:
            a, b = reports()
            mutation(a)
            with self.assertRaises(ValueError):
                compare(a, b)

    def test_rejects_different_model_tokens_hardware_or_precision(self):
        for mutation in [
            lambda r: r.update(model_sha256="d" * 64),
            lambda r: r["fixture"].update(token_bytes_sha256="d" * 64),
            lambda r: r["hardware"][0].update(driver_version="other"),
            lambda r: r["command"].__setitem__(-1, "q8_0"),
        ]:
            a, b = reports()
            mutation(b)
            with self.assertRaises(ValueError):
                compare(a, b)

    def test_rejects_portable_or_resident_workloads(self):
        for mode in ["portable_restore", "resident"]:
            a, b = reports()
            a["mode"] = mode
            with self.assertRaisesRegex(ValueError, "full prompt replay"):
                compare(a, b)

    def test_rejects_hidden_cache_and_different_output_counts(self):
        a, b = reports()
        b["observations"][20]["result"]["timings"].update(prompt_n=1, cache_n=8188)
        with self.assertRaisesRegex(ValueError, "reused"):
            compare(a, b)
        a, b = reports()
        b["observations"][20]["result"]["usage"].update(
            completion_tokens=6, total_tokens=8195
        )
        b["observations"][20]["result"]["timings"]["predicted_n"] = 6
        with self.assertRaisesRegex(ValueError, "accounting"):
            compare(a, b)
        a, b = reports()
        a["observations"][20]["result"]["usage"]["cached_prompt_tokens"] = 8189
        with self.assertRaisesRegex(ValueError, "reused"):
            compare(a, b)


if __name__ == "__main__":
    unittest.main()
