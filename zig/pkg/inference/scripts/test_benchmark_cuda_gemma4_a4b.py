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
    metric_stats,
    parse_antfly_sample,
    parse_llama_sample,
    require_sha256,
)


PROMPT_IDS = "11 12"
OUTPUT_IDS = "21 22 23"


def digest(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def payload(*, decode_calls: int = 60) -> dict:
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
        "cuda_generate": {
            "a4b_resident_source_bytes": 0,
            "a4b_resident_source_count": 0,
            "a4b_route_calls": 90,
            "a4b_decode_calls": decode_calls,
            "a4b_prefill_calls": 30,
            "graph_capture_replays": 2,
        },
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


    def test_parses_llama_decode_and_stability(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "llama.log"
            path.write_text(
                "llama_perf_context_print: eval time = 1800.0 ms / 127 runs "
                "(14.17 ms per token, 70.57 tokens per second)\n"
            )
            self.assertEqual(70.57, parse_llama_sample(path)["decode_tok_s"])
        self.assertEqual(0.0, metric_stats([1.0, 1.0, 1.0])["cv"])

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
