#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
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

import hashlib
import json
from pathlib import Path
import sys
import tempfile
import unittest


SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))
from benchmark_cuda_gemma4_a4b import (  # noqa: E402
    BenchmarkContractError,
    cuda_replay_kv_capacity,
    llama_command,
    metric_stats,
    parse_antfly_sample,
    parse_llama_sample,
    require_sha256,
)


PROMPT_IDS = "11 12"
OUTPUT_IDS = "21 22 23"


def digest(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def payload(*, decode_calls: int = 60, **generate_overrides: int) -> dict:
    generate = {
        "a4b_route_calls": 90,
        "a4b_decode_calls": decode_calls,
        "a4b_prefill_calls": 30,
        "a4b_compact_down_hits": 60,
        "a4b_compact_down_fallbacks": 0,
        "a4b_exact_lm_head_hits": 3,
        "a4b_exact_lm_head_fallbacks": 0,
        "graph_capture_replays": 2,
        "graph_capture_persistent_replays": 1,
        "graph_capture_capacity_skips": 0,
        "graph_capture_discards": 0,
        "graph_capture_update_failures": 0,
        "cross_backend_copies": 0,
        "cross_backend_sync_fallbacks": 0,
        "decoder_runtime_linear_slot_prepare_misses": 0,
        "decoder_runtime_rms_norm_slot_prepare_misses": 0,
        "decoder_runtime_linear_apply_misses": 0,
        "decoder_runtime_rms_norm_apply_misses": 0,
        "lm_head_argmax_fallbacks": 0,
        "device_kv_attempts": 90,
        "device_kv_successes": 90,
        "device_kv_fail_batch": 0,
        "device_kv_fail_no_cache": 0,
        "device_kv_fail_no_hook": 0,
        "device_kv_fail_no_storage": 0,
        "device_kv_fail_read": 0,
        "device_kv_fail_shape": 0,
        "device_kv_fail_write": 0,
    }
    generate.update(generate_overrides)
    return {
        "backend": "cuda",
        "tokens": 3,
        "finish_reason": "length",
        "token_ids": [21, 22, 23],
        "timing_ms": {"decode_inner": 40},
        "cuda": {
            "a4b_resident_source_bytes": 14_000_000_000,
            "a4b_resident_source_count": 60,
            "a4b_route_calls": 90,
            "a4b_decode_calls": 60,
            "a4b_prefill_calls": 30,
            "graph_capture_replays": 2,
        },
        "cuda_generate": generate,
    }


def log() -> str:
    return (
        f"prompt_token_ids: {PROMPT_IDS}\n"
        f"token_ids: {OUTPUT_IDS}\n"
        "cuda_a4b: resident load complete layers=30 sources=60 "
        "source_mib=13600 workspace_mib=238 budget_mib=16384\n"
    )


class CudaA4bBenchmarkTests(unittest.TestCase):
    def test_cuda_replay_kv_capacity_covers_full_sample_and_headroom(self) -> None:
        self.assertEqual(192, cuda_replay_kv_capacity(29, 128))
        self.assertEqual(192, cuda_replay_kv_capacity(24, 128))
        self.assertEqual(96, cuda_replay_kv_capacity(32, 1))
        with self.assertRaisesRegex(BenchmarkContractError, "must be positive"):
            cuda_replay_kv_capacity(0, 128)

    def test_accepts_resident_device_execution(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            json_path = root / "sample.json"
            log_path = root / "sample.log"
            json_path.write_text(json.dumps(payload()))
            log_path.write_text(log())
            sample = parse_antfly_sample(
                json_path,
                log_path,
                prompt_tokens=2,
                prompt_sha256=digest(PROMPT_IDS),
                output_tokens=3,
                output_sha256=digest(OUTPUT_IDS),
            )
            self.assertEqual(50.0, sample["decode_tok_s"])
            self.assertEqual(60, sample["counters"]["a4b_decode_calls"])

    def test_rejects_missing_decode_route(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            json_path = root / "sample.json"
            log_path = root / "sample.log"
            json_path.write_text(json.dumps(payload(decode_calls=0)))
            log_path.write_text(log())
            with self.assertRaisesRegex(BenchmarkContractError, "incomplete"):
                parse_antfly_sample(
                    json_path,
                    log_path,
                    prompt_tokens=2,
                    prompt_sha256=digest(PROMPT_IDS),
                    output_tokens=3,
                    output_sha256=digest(OUTPUT_IDS),
                )

    def test_rejects_fast_path_fallback_or_missing_device_kv(self) -> None:
        for overrides, message in (
            ({"a4b_compact_down_fallbacks": 1}, "fallback/skip"),
            ({"a4b_exact_lm_head_hits": 0}, "incomplete"),
            ({"cross_backend_copies": 1}, "fallback/skip"),
            ({"device_kv_successes": 89}, "incomplete"),
        ):
            with self.subTest(overrides=overrides), tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                json_path = root / "sample.json"
                log_path = root / "sample.log"
                json_path.write_text(json.dumps(payload(**overrides)))
                log_path.write_text(log())
                with self.assertRaisesRegex(BenchmarkContractError, message):
                    parse_antfly_sample(
                        json_path,
                        log_path,
                        prompt_tokens=2,
                        prompt_sha256=digest(PROMPT_IDS),
                        output_tokens=3,
                        output_sha256=digest(OUTPUT_IDS),
                    )


    def test_parses_llama_decode_and_stability(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "llama.log"
            path.write_text(
                "ggml_cuda_init: found 1 CUDA devices:\n"
                "llama_prepare_model_devices: using device CUDA0 (NVIDIA L4)\n"
                "load_tensors: offloaded 47/47 layers to GPU\n"
                "llama_perf_context_print: eval time = 1800.0 ms / 127 runs "
                "(14.17 ms per token, 70.57 tokens per second)\n"
            )
            sample = parse_llama_sample(path, expected_device="NVIDIA L4")
            self.assertEqual(70.57, sample["decode_tok_s"])
            self.assertEqual(47, sample["offloaded_layers"])
        self.assertEqual(0.0, metric_stats([1.0, 1.0, 1.0])["cv"])

    def test_llama_comparator_requires_cuda_and_complete_offload(self) -> None:
        command = llama_command(Path("llama-cli"), Path("model.gguf"), "hello", 8)
        self.assertEqual("999", command[command.index("-ngl") + 1])
        self.assertIn("--single-turn", command)
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "llama.log"
            base = (
                "ggml_cuda_init: found 1 CUDA devices:\n"
                "llama_prepare_model_devices: using device CUDA0 (NVIDIA L4)\n"
                "load_tensors: offloaded 46/47 layers to GPU\n"
                "llama_perf_context_print: eval time = 1 ms / 1 runs "
                "(1 ms per token, 1 tokens per second)\n"
            )
            path.write_text(base)
            with self.assertRaisesRegex(BenchmarkContractError, "expected complete offload"):
                parse_llama_sample(path, expected_device="NVIDIA L4")
            path.write_text(base.replace("46/47", "47/47").replace("NVIDIA L4", "CPU"))
            with self.assertRaisesRegex(BenchmarkContractError, "CUDA device"):
                parse_llama_sample(path, expected_device="NVIDIA L4")
            path.write_text("llama_perf_context_print: eval time = 1 ms / 1 runs (1 tokens per second)\n")
            with self.assertRaisesRegex(BenchmarkContractError, "exactly one CUDA device"):
                parse_llama_sample(path, expected_device="NVIDIA L4")

    def test_parses_current_llama_device_inventory_and_fit_probe(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "llama.log"
            path.write_text(
                "common_param: device_info:\n"
                "common_param:   - CUDA0   : NVIDIA L4 (22563 MiB, 22369 MiB free)\n"
                "llama_prepare_model_devices: using device CUDA0 (NVIDIA L4) "
                "(0000:00:03.0) - 22369 MiB free\n"
                "load_tensors: offloaded 31/31 layers to GPU\n"
                "load_tensors: offloaded 31/31 layers to GPU\n"
                "slot print_timing: eval time = 1736.75 ms / 128 tokens "
                "(13.68 ms per token, 73.13 tokens per second)\n"
            )
            sample = parse_llama_sample(path, expected_device="NVIDIA L4")
            self.assertEqual(73.13, sample["decode_tok_s"])
            self.assertEqual(31, sample["offloaded_layers"])

    def test_llama_binary_digest_is_pinned(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "llama-cli"
            path.write_bytes(b"pinned comparator")
            expected = hashlib.sha256(path.read_bytes()).hexdigest()
            self.assertEqual(expected, require_sha256(path, expected, "llama.cpp binary"))
            with self.assertRaisesRegex(BenchmarkContractError, "SHA-256"):
                require_sha256(path, "0" * 64, "llama.cpp binary")


if __name__ == "__main__":
    unittest.main()
