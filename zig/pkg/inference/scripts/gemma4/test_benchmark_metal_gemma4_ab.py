#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0

"""Fast fake-binary tests for the Gemma4 Metal A/B benchmark contract."""

import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))

from benchmark_metal_gemma4_ab import (  # noqa: E402
    BenchmarkContractError,
    _route_expectations,
    build_summary,
)


BENCHMARK = SCRIPT_DIR / "benchmark_metal_gemma4_ab.py"
PROMPT_TOKENS = 20
OUTPUT_TOKENS = 128


def token_digest(count: int) -> str:
    value = " ".join(str(index) for index in range(count))
    return hashlib.sha256(value.encode()).hexdigest()


def executable(path: Path, source: str) -> None:
    path.write_text(source)
    path.chmod(0o755)


FAKE_ANTFLY = r'''#!/usr/bin/env python3
import json
import os
from pathlib import Path
import sys

args = sys.argv[1:]
tokens = int(args[args.index("--max-tokens") + 1])
json_path = Path(args[args.index("--json-timing") + 1])
label = json_path.stem
variant = os.environ.get("BENCH_VARIANT", "missing")
if variant not in ("baseline", "candidate"):
    raise SystemExit(f"missing variant: {variant}")
if variant == "baseline" and os.environ.get("CANDIDATE_ONLY") is not None:
    raise SystemExit("candidate environment leaked into baseline")
for inherited_policy in (
    "TERMITE_METAL_QUANT_MODE",
    "TERMITE_METAL_DISABLE_GEMMA4_E4B_FAST_RESIDENCY",
    "TERMITE_GPU_EAGER_DENSE_MAX_MB",
    "ANTFLY_INFERENCE_METAL_DISABLE_GEMMA_FUSED_QKV",
):
    if inherited_policy in os.environ:
        raise SystemExit(f"inherited policy environment leaked: {inherited_policy}")
order_log = os.environ.get("ORDER_LOG")
if order_log:
    with Path(order_log).open("a") as stream:
        stream.write(label + "\n")

stage_enabled = os.environ.get("TERMITE_METAL_STAGE_TIMING") == "1"
pair_decode = os.environ.get("TERMITE_METAL_ENABLE_Q4_0_PAIR_ACTIVATION_FUSION") == "1"
pair_prefill = os.environ.get("TERMITE_METAL_ENABLE_Q4_0_PAIR_ACTIVATION_MM") == "1"
lm_head_repack = os.environ.get("TERMITE_METAL_ENABLE_LM_HEAD_Q4_REPACK") == "q4_k"
concurrent = os.environ.get("TERMITE_METAL_ENABLE_CONCURRENT_PLANNED_DISPATCH") == "1"
if concurrent and os.environ.get("TERMITE_METAL_DISABLE_CONCURRENT_PLANNED_DISPATCH") == "1":
    raise SystemExit("concurrent dispatch simultaneously enabled and disabled")

model_topology = os.environ.get("FAKE_GEMMA4_TOPOLOGY", "e4b")
decode_frames = tokens
prompt_tokens = int(os.environ.get("FAKE_PROMPT_TOKENS", "20"))
split_disabled = os.environ.get("TERMITE_METAL_DISABLE_DECODE_GQA_SPLIT") == "1"
default_split_min_kv = 192 if model_topology == "e2b" else 32
split_min_kv = int(
    os.environ.get("TERMITE_METAL_DECODE_GQA_SPLIT_MIN_KV", str(default_split_min_kv))
)
below_floor_frames = min(decode_frames, max(split_min_kv - (prompt_tokens + 1), 0))
split_frames = decode_frames - below_floor_frames
if split_disabled:
    below_floor_frames = 0
    split_frames = 0
if model_topology == "e2b":
    attention = 35 * decode_frames + 28
    paged_attention = 35 * (decode_frames if split_disabled else below_floor_frames) + 28
    split_attention = 35 * split_frames
    below_floor_calls = 0 if split_disabled else 35 * below_floor_frames
    generated_flash_prefill = 0
    generated_flash_prefill_hd512 = 7
    prefill_paged_kv = 7
    pair_count = 35
    logical_decode_q4 = 175
    logical_prefill_q4 = 275
elif model_topology == "e4b":
    attention = 42 * decode_frames
    paged_attention = 42 * (decode_frames if split_disabled else below_floor_frames)
    split_attention = 42 * split_frames
    below_floor_calls = 0 if split_disabled else 42 * below_floor_frames
    generated_flash_prefill = 35
    generated_flash_prefill_hd512 = 7
    prefill_paged_kv = 42
    pair_count = 42
    logical_decode_q4 = 210
    logical_prefill_q4 = 342
else:
    raise SystemExit(f"invalid fake model topology: {model_topology}")
gqa_split_schedule = os.environ.get("TERMITE_METAL_TRACE_DECODE_GQA_SPLIT_SCHEDULE") == "1"
gqa_swa_calls = 35 * split_frames
gqa_global_calls = 7 * split_frames

def gqa_variant(name):
    value = os.environ.get(name, "auto")
    return "s32" if value == "auto" else value

gqa_swa_variant = gqa_variant("TERMITE_METAL_DECODE_GQA_SPLIT_SWA_VARIANT")
gqa_global_variant = gqa_variant("TERMITE_METAL_DECODE_GQA_SPLIT_GLOBAL_VARIANT")
if gqa_swa_variant not in ("s8", "s16", "s24", "s32"):
    raise SystemExit(f"invalid SWA GQA split variant: {gqa_swa_variant}")
if gqa_global_variant not in ("s8", "s16", "s24", "s32"):
    raise SystemExit(f"invalid global GQA split variant: {gqa_global_variant}")
decode_pairs = pair_count * decode_frames if pair_decode else 0
prefill_pairs = pair_count if pair_prefill else 0
q4_row_one = logical_decode_q4 * decode_frames - 2 * decode_pairs
q4_prefill_dispatches = logical_prefill_q4 - 2 * prefill_pairs
q4_prefill_rows = [0, 0, 0, 0]
q4_prefill_bucket = 0 if prompt_tokens == 1 else 1 if prompt_tokens <= 8 else 2 if prompt_tokens <= 64 else 3
q4_prefill_rows[q4_prefill_bucket] = q4_prefill_dispatches
q4_rows = [q4_row_one + q4_prefill_rows[0], *q4_prefill_rows[1:]]

q4_variant_names = ("nr4-nsg2", "nr8-nsg2", "nr4-nsg4", "nr8-nsg4")
q4_workload_env = {
    "attention": "TERMITE_METAL_Q4_0_MMV_ATTENTION_VARIANT",
    "ffn_gate_up": "TERMITE_METAL_Q4_0_MMV_FFN_GATE_UP_VARIANT",
    "ffn_down": "TERMITE_METAL_Q4_0_MMV_FFN_DOWN_VARIANT",
}
q4_workload_calls = {
    "generic": 18 * decode_frames,
    "attention": 66 * decode_frames,
    "ffn_gate_up": 0 if pair_decode else 84 * decode_frames,
    "ffn_down": 42 * decode_frames,
}
q4_global = os.environ.get("TERMITE_METAL_Q4_0_MMV_VARIANT", "auto")
q4_workload_policy = {}
q4_variant_counts = {name: 0 for name in q4_variant_names}
for workload in ("generic", "attention", "ffn_gate_up", "ffn_down"):
    environment_name = q4_workload_env.get(workload)
    role_override = os.environ.get(environment_name, "auto") if environment_name else "auto"
    override = q4_global if q4_global != "auto" else role_override
    if override in q4_variant_names:
        selected = override
    elif override == "legacy":
        selected = "nr4-nsg2" if workload in ("generic", "attention") else "nr8-nsg2"
    else:
        selected = "nr4-nsg2"
    q4_workload_policy[workload] = (override, selected)
    q4_variant_counts[selected] += q4_workload_calls[workload]
if model_topology == "e2b":
    if not pair_decode or pair_prefill or gqa_split_schedule:
        raise SystemExit("fake E2B only supports the qualified decode-pair topology")
    q4_variant_counts = {
        "nr4-nsg2": 70 * decode_frames,
        "nr8-nsg2": 35 * decode_frames,
        "nr4-nsg4": 0,
        "nr8-nsg4": 0,
    }

if stage_enabled:
    prefill_ms, decode_ms = 20000, 30000
elif tokens <= 4:
    prefill_ms = 100 if variant == "baseline" else 99
    decode_ms = 30 if variant == "baseline" else 29
else:
    prefill_ms = 4000 if variant == "baseline" else 3980
    decode_ms = 6000 if variant == "baseline" else 5820
total_ms = prefill_ms + decode_ms

if stage_enabled:
    decode_start = int(os.environ["TERMITE_METAL_STAGE_TIMING_DECODE_START"])
    decode_stride = int(os.environ["TERMITE_METAL_STAGE_TIMING_DECODE_STRIDE"])
    decode_max = int(os.environ["TERMITE_METAL_STAGE_TIMING_DECODE_MAX"])
    decode_sampled = 0
    if decode_frames > decode_start:
        decode_sampled = min(decode_max, ((decode_frames - 1 - decode_start) // decode_stride) + 1)
    prefill_stage = {
        "frames": 1,
        "gpu": 100,
        "attention": 20,
        "ffn": 40,
        "ple": 10,
        "tail": 10,
        "embedding": 10,
        "other": 10,
    }
    decode_stage = {
        "frames": decode_sampled,
        "gpu": 100 * decode_sampled,
        "attention": 20 * decode_sampled,
        "ffn": 40 * decode_sampled,
        "ple": 10 * decode_sampled,
        "tail": 10 * decode_sampled,
        "embedding": 10 * decode_sampled,
        "other": 10 * decode_sampled,
    }
    stage = {
        "scope": "runtime_frame",
        "enabled": 1,
        "supported": 1,
        "complete": 1,
        "samples": 8 * (1 + decode_sampled),
        "failures": 0,
        "prefill": prefill_stage,
        "decode": decode_stage,
    }
else:
    empty = {
        "frames": 0,
        "gpu": 0,
        "attention": 0,
        "ffn": 0,
        "ple": 0,
        "tail": 0,
        "embedding": 0,
        "other": 0,
    }
    stage = {
        "scope": "runtime_frame",
        "enabled": 0,
        "supported": 0,
        "complete": 0,
        "samples": 0,
        "failures": 0,
        "prefill": empty,
        "decode": empty,
    }

q4_variants = {
    "mmv_nr4_nsg2": q4_variant_counts["nr4-nsg2"],
    "mmv_nr8_nsg2": q4_variant_counts["nr8-nsg2"],
    "mmv_nr4_nsg4": q4_variant_counts["nr4-nsg4"],
    "mmv_nr8_nsg4": q4_variant_counts["nr8-nsg4"],
    "mmv_variant_fallbacks": 0,
    "mm_sg_aligned": 342,
    "mm_sg_aligned_tail": 265,
    "mm_sg_unrolled": 0,
}
payload = {
    "backend": "metal",
    "tokens": tokens,
    "token_ids": list(range(tokens)),
    "finish_reason": "length",
    "timing_ms": {
        "generate": total_ms,
        "prefill_inner": prefill_ms,
        "decode_inner": decode_ms,
    },
    "speculative": None,
    "draft_cuda": None,
    "draft_cuda_generate": None,
    "runtime": {"decode_greedy_calls": max(tokens - 1, 0)},
    "generation_decoder_runtime": {"forward_attempts": max(tokens - 1, 0)},
    "metal": {
        "device": "Apple M4",
        "device_registry_id": 123456,
        "native_quant_null": False,
        "runtime_command_operators": {"fallback": 0},
        "attention_dispatch": {
            "paged_1x": paged_attention,
            "decode_gqa_split": split_attention,
            "generated_flash_prefill": generated_flash_prefill,
            "generated_flash_prefill_hd512": generated_flash_prefill_hd512,
            "prefill_direct_kv": 0,
            "prefill_paged_kv": prefill_paged_kv,
        },
        "decode_gqa_split_policy": {
            "min_kv": split_min_kv,
            "below_min_kv": below_floor_calls,
        },
        "prepared_frame": {"fast_path": decode_frames, "fallback": 0},
        "lm_head_q4_q6_refine": {
            "dispatches": tokens if lm_head_repack else 0,
            "resident_sampling_rejections": 0,
        },
        "stage_timing_ns": stage,
        "q4_0_policy": q4_variants,
        "frame_fallbacks": {
            "decode_fallback": 0,
            "prefill_plan_fail": 0,
            "prefill_execute_fail": 0,
        },
        "quant_kernel_plan": {"fast_path_misses": 0, "unsupported_routes": 0},
    },
}
json_path.write_text(json.dumps(payload))

print("generate-setup: live whole-model executor skipped")
print(f"gen_debug: executePrefill whole-model fast path seq_len={prompt_tokens}")
print("prompt_token_ids:", " ".join(str(index) for index in range(prompt_tokens)))
print("token_ids:", " ".join(str(index) for index in range(tokens)))
print(
    "metal_attention_dispatch: "
    f"paged_1x={paged_attention} "
    f"decode_gqa_split={split_attention} "
    f"generated_decode_1x=0 generated_flash_prefill={generated_flash_prefill} "
    f"generated_flash_prefill_hd512={generated_flash_prefill_hd512} "
    f"prefill_direct_kv=0 prefill_paged_kv={prefill_paged_kv} "
    "generated_rms_norm=0"
)
print(
    "metal_decode_gqa_split_policy: "
    f"min_kv={split_min_kv} "
    f"below_min_kv={below_floor_calls}"
)
if os.environ.get("TERMITE_METAL_TRACE_DECODE_GQA_SPLIT_SCHEDULE") == "1":
    # A partial snapshot proves the parser intentionally consumes the final
    # stable snapshot, just like the existing cumulative route counters.
    print(
        "metal_decode_gqa_split_schedule: legacy_total=0 swa_total=0 global_total=0 "
        "swa_s8=0 swa_s16=0 swa_s24=0 swa_s32=0 "
        "global_s8=0 global_s16=0 global_s24=0 global_s32=0 "
        "fallbacks=0 invalid_overrides=0"
    )
    swa_buckets = {
        variant: gqa_swa_calls if variant == gqa_swa_variant else 0
        for variant in ("s8", "s16", "s24", "s32")
    }
    global_buckets = {
        variant: gqa_global_calls if variant == gqa_global_variant else 0
        for variant in ("s8", "s16", "s24", "s32")
    }
    print(
        f"metal_decode_gqa_split_schedule: legacy_total={split_attention} "
        f"swa_total={gqa_swa_calls} global_total={gqa_global_calls} "
        f"swa_s8={swa_buckets['s8']} swa_s16={swa_buckets['s16']} "
        f"swa_s24={swa_buckets['s24']} swa_s32={swa_buckets['s32']} "
        f"global_s8={global_buckets['s8']} global_s16={global_buckets['s16']} "
        f"global_s24={global_buckets['s24']} global_s32={global_buckets['s32']} "
        "fallbacks=0 invalid_overrides=0"
    )
print(f"metal_prepared_frame: fast_path={decode_frames} fallback=0")
print("metal_runtime_memory: total_mb=5718 frame_retained_mb=209")
print(
    f"metal_q4_0_dispatch: linear_reduce_rows={q4_rows[0]}/{q4_rows[1]}/{q4_rows[2]}/{q4_rows[3]} "
    f"pair_act_reduce={decode_pairs + prefill_pairs}"
)
print(
    f"metal_q4_0_policy: mmv_nr4_nsg2={q4_variant_counts['nr4-nsg2']} "
    f"mmv_nr8_nsg2={q4_variant_counts['nr8-nsg2']} "
    f"mmv_nr4_nsg4={q4_variant_counts['nr4-nsg4']} "
    f"mmv_nr8_nsg4={q4_variant_counts['nr8-nsg4']} mmv_variant_fallbacks=0 "
    "mm_sg_aligned=342 mm_sg_aligned_tail=265 mm_sg_unrolled=0"
)
if os.environ.get("TERMITE_METAL_TRACE_Q4_0_MMV_VARIANT") == "1":
    q4_shapes = {
        "generic": (2560, 2048),
        "attention": (2560, 2048),
        "ffn_gate_up": (2560, 10240),
        "ffn_down": (10240, 2560),
    }
    for workload in ("generic", "attention", "ffn_gate_up", "ffn_down"):
        override, selected = q4_workload_policy[workload]
        in_dim, out_dim = q4_shapes[workload]
        print(
            f"metal-q4-0-mmv apple_family=9 workload={workload} global={q4_global} "
            f"override={override} shape={in_dim}x{out_dim} selected={selected} fallback=0"
        )
print(
    f"metal_q4_0_pair_activation_policy: "
    f"mmv_nr4_nsg2={decode_pairs if model_topology == 'e4b' else 0} "
    f"mmv_nr8_nsg2=0 mmv_nr4_nsg4={decode_pairs if model_topology == 'e2b' else 0} "
    "mmv_nr8_nsg4=0 mmv_variant_fallbacks=0 "
    f"mm_m32_n64_aligned=0 mm_m32_n64_tail={prefill_pairs} "
    "mm_m32_n32_aligned=0 mm_m32_n32_tail=0 mm_variant_fallbacks=0"
)
if lm_head_repack:
    print("info: lm_head Q4_K repack: slot=350 refine_slot=352 rows=262144 cols=2560")
print(
    "metal_q4_q6_k_dispatch: "
    f"q4_linear_reduce_rows={tokens if lm_head_repack else 0}/0/0/0 "
    f"q6_linear_reduce_rows={1 if lm_head_repack else tokens + 1}/0/0/0 "
    f"lm_head_q4_q6_refine_dispatches={tokens if lm_head_repack else 0} "
    "lm_head_q4_resident_sampling_rejections=0"
)
p = stage["prefill"]
d = stage["decode"]
print(
    f"metal_stage_timing_ns: enabled={stage['enabled']} supported={stage['supported']} "
    f"complete={stage['complete']} scope=runtime_frame prefill_frames={p['frames']} prefill_gpu={p['gpu']} "
    f"prefill_attention={p['attention']} prefill_ffn={p['ffn']} prefill_ple={p['ple']} "
    f"prefill_tail={p['tail']} prefill_embedding={p['embedding']} prefill_other={p['other']} "
    f"decode_frames={d['frames']} decode_gpu={d['gpu']} decode_attention={d['attention']} "
    f"decode_ffn={d['ffn']} decode_ple={d['ple']} decode_tail={d['tail']} "
    f"decode_embedding={d['embedding']} decode_other={d['other']} "
    f"samples={stage['samples']} failures={stage['failures']}"
)
'''


class RouteFormulaTests(unittest.TestCase):
    def test_split_and_pair_formulas_at_gate_shapes(self) -> None:
        for tokens in (4, 128, 300):
            with self.subTest(tokens=tokens):
                split = _route_expectations("split_ffn", tokens)
                self.assertEqual(split["q4_rows"], (210 * tokens, 0, 342, 0))
                self.assertEqual(split["decode_pairs"], 0)
                gqa = _route_expectations("gqa_split_schedule", tokens)
                self.assertEqual(gqa["attention"], 42 * tokens)
                self.assertEqual(gqa["decode_pairs"], 0)
                paired = _route_expectations("pair_decode_prefill", tokens)
                self.assertEqual(paired["q4_rows"], (126 * tokens, 0, 258, 0))
                self.assertEqual(paired["decode_pairs"], 42 * tokens)
                self.assertEqual(paired["prefill_pairs"], 42)
                self.assertEqual(
                    paired["q4_decode_row_one"] + 2 * paired["decode_pairs"],
                    paired["logical_decode_q4"],
                )
                self.assertEqual(
                    sum(paired["q4_rows"]) - paired["q4_decode_row_one"]
                    + 2 * paired["prefill_pairs"],
                    paired["logical_prefill_q4"],
                )
                repack = _route_expectations("lm_head_repack", tokens)
                self.assertEqual(repack["q4_decode_row_one"], 126 * tokens)
                self.assertEqual(repack["decode_pairs"], 42 * tokens)
                self.assertEqual(repack["prefill_pairs"], 0)
                e2b = _route_expectations("lm_head_repack", tokens, "e2b")
                below_floor = min(tokens, 192 - 24)
                self.assertEqual(
                    e2b["attention_routes"],
                    (
                        35 * below_floor + 28,
                        35 * (tokens - below_floor),
                        0,
                        7,
                        0,
                        7,
                    ),
                )
                self.assertEqual(e2b["q4_decode_row_one"], 105 * tokens)
                self.assertEqual(e2b["decode_pairs"], 35 * tokens)
                self.assertEqual(
                    e2b["q4_mmv_variants"],
                    (70 * tokens, 35 * tokens, 0, 0),
                )
        with self.assertRaisesRegex(BenchmarkContractError, "not qualified for E2B"):
            _route_expectations("split_ffn", OUTPUT_TOKENS, "e2b")

    def test_prefill_row_buckets_follow_live_prompt_length(self) -> None:
        self.assertEqual(
            (210 * OUTPUT_TOKENS, 342, 0, 0),
            _route_expectations("split_ffn", OUTPUT_TOKENS, prompt_tokens=8)["q4_rows"],
        )
        self.assertEqual(
            (210 * OUTPUT_TOKENS, 0, 0, 342),
            _route_expectations("split_ffn", OUTPUT_TOKENS, prompt_tokens=65)["q4_rows"],
        )

    def test_split_rollback_has_all_paged_decode_and_no_below_floor_hits(self) -> None:
        rollback = _route_expectations("gqa_split_rollback", OUTPUT_TOKENS)
        self.assertEqual(0, rollback["split_frames"])
        self.assertEqual(0, rollback["below_floor_calls"])
        self.assertEqual(
            (42 * OUTPUT_TOKENS, 0, 35, 7, 0, 42),
            rollback["attention_routes"],
        )
        paired_rollback = _route_expectations(
            "gqa_split_rollback_pair_decode", OUTPUT_TOKENS, "e2b"
        )
        self.assertEqual(0, paired_rollback["split_frames"])
        self.assertEqual(0, paired_rollback["below_floor_calls"])
        self.assertEqual(35 * OUTPUT_TOKENS, paired_rollback["decode_pairs"])
        self.assertEqual(
            (35 * OUTPUT_TOKENS + 28, 0, 0, 7, 0, 7),
            paired_rollback["attention_routes"],
        )


class HarnessTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory(prefix="gemma4-metal-ab-test-")
        self.tmp = Path(self.tempdir.name)
        self.model = self.tmp / "model.gguf"
        self.model.write_bytes(b"fake gguf")
        self.binary = self.tmp / "antfly-inference"
        executable(self.binary, FAKE_ANTFLY)
        self.order_log = self.tmp / "order.log"

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def paired_command(self, out: Path) -> list[str]:
        return [
            sys.executable,
            str(BENCHMARK),
            "run",
            "--out-dir",
            str(out),
            "--experiment-id",
            "fixture-paired",
            "--model",
            str(self.model),
            "--antfly-bin",
            str(self.binary),
            "--prompt",
            "fixture prompt",
            "--expected-prompt-tokens",
            str(PROMPT_TOKENS),
            "--expected-prompt-token-ids-sha256",
            token_digest(PROMPT_TOKENS),
            "--output-tokens",
            str(OUTPUT_TOKENS),
            "--expected-token-ids-sha256",
            token_digest(OUTPUT_TOKENS),
            "--runs",
            "2",
            "--warmups",
            "1",
            "--warmup-output-tokens",
            "4",
            "--expected-warmup-token-ids-sha256",
            token_digest(4),
            "--cooldown-seconds",
            "0",
            "--common-env",
            f"ORDER_LOG={self.order_log}",
            "--baseline-env",
            "BENCH_VARIANT=baseline",
            "--candidate-env",
            "BENCH_VARIANT=candidate",
            "--candidate-env",
            "CANDIDATE_ONLY=1",
            "--max-total-latency-ratio",
            "0.99",
            "--max-prefill-latency-ratio",
            "1.0",
            "--max-decode-latency-ratio",
            "0.98",
            "--min-decode-throughput-ratio",
            "1.02",
            "--min-target-wins",
            "2",
            "--max-cv",
            "0.01",
        ]

    def run_paired(self, out: Path) -> subprocess.CompletedProcess[str]:
        environment = os.environ.copy()
        environment["STAGE_TIMING_RUNS"] = "1"
        environment["CANDIDATE_ONLY"] = "inherited-value"
        environment["TERMITE_METAL_DISABLE_SWA_SCAN_CLAMP"] = "1"
        environment["TERMITE_METAL_ENABLE_PIPELINED_DECODE_FRAME"] = "1"
        environment["TERMITE_METAL_ENABLE_A4B_DAG_SCHEDULER"] = "1"
        environment["TERMITE_METAL_STAGE_TIMING_ROOFLINE"] = "1"
        environment["TERMITE_METAL_QUANT_MODE"] = "eager"
        environment["TERMITE_METAL_DISABLE_GEMMA4_E4B_FAST_RESIDENCY"] = "1"
        environment["TERMITE_GPU_EAGER_DENSE_MAX_MB"] = "4096"
        environment["ANTFLY_INFERENCE_METAL_DISABLE_GEMMA_FUSED_QKV"] = "1"
        completed = subprocess.run(
            self.paired_command(out),
            env=environment,
            text=True,
            capture_output=True,
            check=False,
        )
        if completed.returncode != 0:
            logs = "\n".join(
                f"[{path.name}]\n{path.read_text(errors='replace')}"
                for path in sorted(out.glob("*.log"))
            )
            self.fail(
                f"paired harness exited {completed.returncode}:\n"
                f"stdout:\n{completed.stdout}\nstderr:\n{completed.stderr}\nlogs:\n{logs}"
            )
        return completed

    def test_full_paired_contract_order_env_isolation_and_stage_exclusion(self) -> None:
        out = self.tmp / "paired"
        completed = self.run_paired(out)
        self.assertIn("passed=True", completed.stdout)
        summary = json.loads((out / "summary.json").read_text())
        self.assertEqual(summary["schema"], "antfly.gemma4_metal_ab.v8")
        self.assertTrue(summary["passed"])
        self.assertEqual(len(summary["performance_samples"]), 4)
        self.assertEqual(len(summary["stage_timing_samples"]), 2)
        self.assertEqual(len(summary["warmup_samples"]), 2)
        self.assertAlmostEqual(summary["metrics"]["candidate_total_ms"]["median"], 9800)
        self.assertAlmostEqual(
            summary["paired_ratios"]["candidate_decode_latency_ratio"]["median"],
            0.97,
        )
        self.assertEqual(summary["target_wins"], 2)
        self.assertTrue(summary["checks"]["stage_timing_excluded_from_performance"])
        self.assertEqual(
            summary["metadata"]["decode_throughput_metric"],
            "(output_tokens - 1) / decode_inner_seconds",
        )
        self.assertEqual(
            summary["metadata"]["stage_timing_contract"]["sampling"],
            {"prefill_max": 1, "decode_start": 32, "decode_stride": 64, "decode_max": 5},
        )
        self.assertEqual(
            summary["metadata"]["stage_timing_contract"]["scope"], "runtime_frame"
        )
        self.assertEqual(
            summary["metadata"]["environment_isolation_contract"],
            {
                "clear_inherited_prefixes": [
                    "TERMITE_",
                    "ANTFLY_GEMMA4_",
                    "ANTFLY_INFERENCE_",
                ],
                "reapply_only_explicit_maps": True,
                "runner_owned": [
                    "TERMITE_GEN_STAGE_DEBUG",
                    "TERMITE_METAL_STAGE_TIMING",
                    "TERMITE_METAL_TRACE_DECODE_GQA_SPLIT_SCHEDULE",
                    "TERMITE_METAL_TRACE_Q4_0_MMV_VARIANT",
                ],
                "runner_owned_values": {
                    "TERMITE_GEN_STAGE_DEBUG": "1",
                    "TERMITE_METAL_STAGE_TIMING": (
                        "1 when invocation.stage_timing is true, otherwise 0"
                    ),
                    "TERMITE_METAL_TRACE_DECODE_GQA_SPLIT_SCHEDULE": (
                        "1 only for gqa_split_schedule route profile, otherwise 0"
                    ),
                    "TERMITE_METAL_TRACE_Q4_0_MMV_VARIANT": (
                        "1 only for q4_mmv_workload route profile, otherwise 0"
                    ),
                },
            },
        )
        self.assertIsNone(summary["metadata"]["effective_baseline_env"]["CANDIDATE_ONLY"])
        self.assertIsNone(
            summary["metadata"]["effective_baseline_env"][
                "TERMITE_METAL_DISABLE_SWA_SCAN_CLAMP"
            ]
        )
        self.assertIsNone(
            summary["metadata"]["effective_candidate_env"][
                "TERMITE_METAL_ENABLE_PIPELINED_DECODE_FRAME"
            ]
        )
        self.assertIsNone(
            summary["metadata"]["effective_candidate_env"][
                "TERMITE_METAL_ENABLE_A4B_DAG_SCHEDULER"
            ]
        )
        self.assertIsNone(
            summary["metadata"]["effective_candidate_env"][
                "TERMITE_METAL_STAGE_TIMING_ROOFLINE"
            ]
        )
        self.assertEqual(summary["metadata"]["effective_candidate_env"]["CANDIDATE_ONLY"], "1")
        self.assertEqual(
            summary["metadata"]["effective_candidate_env"][
                "TERMITE_METAL_TRACE_DECODE_GQA_SPLIT_SCHEDULE"
            ],
            "0",
        )
        self.assertEqual(
            self.order_log.read_text().splitlines(),
            [
                "warmup-baseline-01",
                "warmup-candidate-01",
                "performance-baseline-01",
                "performance-candidate-01",
                "performance-candidate-02",
                "performance-baseline-02",
                "stage_timing-baseline-01",
                "stage_timing-candidate-01",
            ],
        )

    def test_gqa_split_schedule_profile_reconciles_shape_variants(self) -> None:
        out = self.tmp / "gqa-schedule"
        command = self.paired_command(out)
        command.extend(
            (
                "--baseline-route-profile",
                "gqa_split_schedule",
                "--candidate-route-profile",
                "gqa_split_schedule",
                "--baseline-env",
                "TERMITE_METAL_DECODE_GQA_SPLIT_SWA_VARIANT=auto",
                "--baseline-env",
                "TERMITE_METAL_DECODE_GQA_SPLIT_GLOBAL_VARIANT=auto",
                "--candidate-env",
                "TERMITE_METAL_DECODE_GQA_SPLIT_SWA_VARIANT=s8",
                "--candidate-env",
                "TERMITE_METAL_DECODE_GQA_SPLIT_GLOBAL_VARIANT=s24",
            )
        )
        environment = os.environ.copy()
        environment["STAGE_TIMING_RUNS"] = "0"
        completed = subprocess.run(
            command,
            env=environment,
            text=True,
            capture_output=True,
            check=True,
        )
        self.assertIn("passed=True", completed.stdout)
        summary = json.loads((out / "summary.json").read_text())
        by_variant = {
            sample["variant"]: sample
            for sample in summary["performance_samples"]
            if sample["index"] == 1
        }
        baseline = by_variant["baseline"]["routes"]["gqa_split_schedule"]
        candidate = by_variant["candidate"]["routes"]["gqa_split_schedule"]
        split_frames = OUTPUT_TOKENS - 11
        self.assertEqual(baseline["legacy_total"], 42 * split_frames)
        self.assertEqual(baseline["swa_s32"], 35 * split_frames)
        self.assertEqual(baseline["global_s32"], 7 * split_frames)
        self.assertEqual(baseline["expected_variants"], {"global": "s32", "swa": "s32"})
        self.assertEqual(candidate["swa_s8"], 35 * split_frames)
        self.assertEqual(candidate["global_s24"], 7 * split_frames)
        self.assertEqual(candidate["expected_variants"], {"global": "s24", "swa": "s8"})
        metadata = summary["metadata"]
        self.assertEqual(
            metadata["effective_baseline_env"][
                "TERMITE_METAL_TRACE_DECODE_GQA_SPLIT_SCHEDULE"
            ],
            "1",
        )
        self.assertEqual(
            metadata["effective_candidate_env"][
                "TERMITE_METAL_DECODE_GQA_SPLIT_GLOBAL_VARIANT"
            ],
            "s24",
        )

        log_path = out / "performance-candidate-01.log"
        original_log = log_path.read_text()
        expected_swa = 35 * split_frames
        log_path.write_text(
            original_log.replace(f"swa_s8={expected_swa}", f"swa_s8={expected_swa - 1}")
        )
        with self.assertRaisesRegex(BenchmarkContractError, "GQA split"):
            build_summary(out)
        log_path.write_text(
            original_log.replace(
                "fallbacks=0 invalid_overrides=0",
                "fallbacks=1 invalid_overrides=0",
            )
        )
        with self.assertRaisesRegex(BenchmarkContractError, "fallback/invalid override"):
            build_summary(out)

    def test_gqa_split_rollback_profile_attests_all_paged_route(self) -> None:
        out = self.tmp / "gqa-rollback"
        command = self.paired_command(out)
        command.extend(
            (
                "--candidate-route-profile",
                "gqa_split_rollback",
                "--candidate-env",
                "TERMITE_METAL_DISABLE_DECODE_GQA_SPLIT=1",
            )
        )
        environment = os.environ.copy()
        environment["STAGE_TIMING_RUNS"] = "0"
        completed = subprocess.run(
            command,
            env=environment,
            text=True,
            capture_output=True,
            check=True,
        )
        self.assertIn("passed=True", completed.stdout)
        summary = json.loads((out / "summary.json").read_text())
        candidate = next(
            sample
            for sample in summary["performance_samples"]
            if sample["variant"] == "candidate" and sample["index"] == 1
        )
        self.assertEqual(candidate["routes"]["paged_1x"], 42 * OUTPUT_TOKENS)
        self.assertEqual(candidate["routes"]["decode_gqa_split"], 0)
        self.assertEqual(
            candidate["routes"]["decode_gqa_split_policy"],
            {"min_kv": 32, "below_min_kv": 0},
        )

        e2b_out = self.tmp / "gqa-rollback-pair-e2b"
        e2b = self.paired_command(e2b_out)
        e2b.extend(
            (
                "--model-topology",
                "e2b",
                "--baseline-route-profile",
                "pair_decode",
                "--candidate-route-profile",
                "gqa_split_rollback_pair_decode",
                "--expected-pair-mmv-variant",
                "nr4-nsg4",
                "--common-env",
                "FAKE_GEMMA4_TOPOLOGY=e2b",
                "--baseline-env",
                "TERMITE_METAL_ENABLE_Q4_0_PAIR_ACTIVATION_FUSION=1",
                "--candidate-env",
                "TERMITE_METAL_ENABLE_Q4_0_PAIR_ACTIVATION_FUSION=1",
                "--candidate-env",
                "TERMITE_METAL_DISABLE_DECODE_GQA_SPLIT=1",
            )
        )
        completed = subprocess.run(
            e2b,
            env=environment,
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(0, completed.returncode, completed.stderr)
        self.assertIn("passed=True", completed.stdout)
        e2b_summary = json.loads((e2b_out / "summary.json").read_text())
        e2b_candidate = next(
            sample
            for sample in e2b_summary["performance_samples"]
            if sample["variant"] == "candidate" and sample["index"] == 1
        )
        self.assertEqual(e2b_candidate["routes"]["paged_1x"], 35 * OUTPUT_TOKENS + 28)
        self.assertEqual(e2b_candidate["routes"]["decode_gqa_split"], 0)
        self.assertEqual(
            e2b_candidate["routes"]["decode_gqa_split_policy"],
            {"min_kv": 192, "below_min_kv": 0},
        )

        invalid = self.paired_command(self.tmp / "gqa-rollback-missing-flag")
        invalid.extend(("--candidate-route-profile", "gqa_split_rollback"))
        rejected = subprocess.run(invalid, text=True, capture_output=True, check=False)
        self.assertNotEqual(0, rejected.returncode)
        self.assertIn("decode GQA split rollback disagree", rejected.stderr)

    def test_live_prompt_length_selects_the_prefill_row_bucket(self) -> None:
        out = self.tmp / "prompt-row-bucket"
        command = self.paired_command(out)
        command[command.index("--expected-prompt-tokens") + 1] = "8"
        command[command.index("--expected-prompt-token-ids-sha256") + 1] = token_digest(8)
        command.extend(("--common-env", "FAKE_PROMPT_TOKENS=8"))
        environment = os.environ.copy()
        environment["STAGE_TIMING_RUNS"] = "0"
        completed = subprocess.run(
            command,
            env=environment,
            text=True,
            capture_output=True,
            check=True,
        )
        self.assertIn("passed=True", completed.stdout)
        summary = json.loads((out / "summary.json").read_text())
        candidate = next(
            sample
            for sample in summary["performance_samples"]
            if sample["variant"] == "candidate" and sample["index"] == 1
        )
        self.assertEqual(
            candidate["routes"]["q4_linear_reduce_rows"],
            [210 * OUTPUT_TOKENS, 342, 0, 0],
        )

    def test_paired_mode_requires_balanced_even_run_count(self) -> None:
        out = self.tmp / "odd-paired"
        command = self.paired_command(out)
        runs_index = command.index("--runs") + 1
        command[runs_index] = "3"
        completed = subprocess.run(
            command,
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertNotEqual(completed.returncode, 0)
        self.assertIn("positive even number of pairs", completed.stderr)
        self.assertFalse(out.exists())

    def test_stage_timing_failure_is_fail_closed(self) -> None:
        out = self.tmp / "bad-stage"
        self.run_paired(out)
        log_path = out / "stage_timing-candidate-01.log"
        log_path.write_text(log_path.read_text().replace("complete=1", "complete=0"))
        json_path = out / "stage_timing-candidate-01.json"
        payload = json.loads(json_path.read_text())
        payload["metal"]["stage_timing_ns"]["complete"] = 0
        json_path.write_text(json.dumps(payload))
        with self.assertRaisesRegex(BenchmarkContractError, "complete=0"):
            build_summary(out)

    def test_metal_device_identity_is_fail_closed(self) -> None:
        out = self.tmp / "bad-metal-device"
        self.run_paired(out)
        json_path = out / "performance-candidate-01.json"
        payload = json.loads(json_path.read_text())
        payload["metal"]["device"] = "Unexpected GPU"
        json_path.write_text(json.dumps(payload))
        with self.assertRaisesRegex(BenchmarkContractError, "Metal device"):
            build_summary(out)

        payload["metal"]["device"] = "Apple M4"
        payload["metal"]["device_registry_id"] = 0
        json_path.write_text(json.dumps(payload))
        with self.assertRaisesRegex(BenchmarkContractError, "device_registry_id"):
            build_summary(out)

        payload["metal"]["device_registry_id"] = 123457
        json_path.write_text(json.dumps(payload))
        with self.assertRaisesRegex(BenchmarkContractError, "changed between A/B samples"):
            build_summary(out)

    def test_pair_decode_and_prefill_route_profile_is_exact(self) -> None:
        out = self.tmp / "pair-routes"
        command = self.paired_command(out)
        command.extend(
            (
                "--candidate-route-profile",
                "pair_decode_prefill",
                "--candidate-env",
                "TERMITE_METAL_ENABLE_Q4_0_PAIR_ACTIVATION_FUSION=1",
                "--candidate-env",
                "TERMITE_METAL_ENABLE_Q4_0_PAIR_ACTIVATION_MM=1",
            )
        )
        environment = os.environ.copy()
        environment["STAGE_TIMING_RUNS"] = "0"
        environment["CANDIDATE_ONLY"] = "inherited-value"
        completed = subprocess.run(
            command,
            env=environment,
            text=True,
            capture_output=True,
            check=True,
        )
        self.assertIn("passed=True", completed.stdout)
        summary = json.loads((out / "summary.json").read_text())
        candidate = next(
            sample
            for sample in summary["performance_samples"]
            if sample["variant"] == "candidate"
        )
        routes = candidate["routes"]
        self.assertEqual(routes["q4_linear_reduce_rows"], [16_128, 0, 258, 0])
        self.assertEqual(routes["q4_pair_activation_decode"], 5_376)
        self.assertEqual(
            routes["q4_pair_activation_policy"]["mmv_nr4_nsg2"], 5_376
        )
        self.assertEqual(
            routes["q4_pair_activation_policy"]["mm_m32_n64_tail"], 42
        )
        self.assertEqual(routes["q4_pair_activation_total"], 5_418)

    def test_lm_head_repack_profile_attests_q4_nomination_and_q6_refine(self) -> None:
        out = self.tmp / "lm-head-repack"
        command = self.paired_command(out)
        command.extend(
            (
                "--baseline-route-profile",
                "pair_decode",
                "--candidate-route-profile",
                "lm_head_repack",
                "--baseline-env",
                "TERMITE_METAL_ENABLE_Q4_0_PAIR_ACTIVATION_FUSION=1",
                "--candidate-env",
                "TERMITE_METAL_ENABLE_Q4_0_PAIR_ACTIVATION_FUSION=1",
                "--candidate-env",
                "TERMITE_METAL_ENABLE_LM_HEAD_Q4_REPACK=q4_k",
            )
        )
        completed = subprocess.run(command, text=True, capture_output=True, check=True)
        self.assertIn("passed=True", completed.stdout)
        summary = json.loads((out / "summary.json").read_text())
        by_variant = {
            sample["variant"]: sample
            for sample in summary["performance_samples"]
            if sample["index"] == 1
        }
        self.assertEqual(by_variant["baseline"]["routes"]["q4_k_linear_reduce_rows"], [0, 0, 0, 0])
        self.assertEqual(by_variant["baseline"]["routes"]["q6_linear_reduce_rows"], [129, 0, 0, 0])
        candidate = by_variant["candidate"]["routes"]
        self.assertEqual(candidate["q4_k_linear_reduce_rows"], [128, 0, 0, 0])
        self.assertEqual(candidate["q6_linear_reduce_rows"], [1, 0, 0, 0])
        self.assertEqual(candidate["lm_head_q4_q6_refine_dispatches"], 128)
        self.assertEqual(candidate["lm_head_q4_k_repack_count"], 1)

        log_path = out / "performance-candidate-01.log"
        log_path.write_text(
            log_path.read_text().replace(
                "lm_head_q4_q6_refine_dispatches=128",
                "lm_head_q4_q6_refine_dispatches=127",
            )
        )
        with self.assertRaisesRegex(BenchmarkContractError, "lm-head Q4_K/Q6_K routes"):
            build_summary(out)

    def test_e2b_lm_head_repack_profile_uses_explicit_topology(self) -> None:
        out = self.tmp / "lm-head-repack-e2b"
        command = self.paired_command(out)
        command.extend(
            (
                "--model-topology",
                "e2b",
                "--baseline-route-profile",
                "pair_decode",
                "--candidate-route-profile",
                "lm_head_repack",
                "--expected-pair-mmv-variant",
                "nr4-nsg4",
                "--common-env",
                "FAKE_GEMMA4_TOPOLOGY=e2b",
                "--baseline-env",
                "TERMITE_METAL_ENABLE_Q4_0_PAIR_ACTIVATION_FUSION=1",
                "--candidate-env",
                "TERMITE_METAL_ENABLE_Q4_0_PAIR_ACTIVATION_FUSION=1",
                "--candidate-env",
                "TERMITE_METAL_ENABLE_LM_HEAD_Q4_REPACK=q4_k",
            )
        )
        completed = subprocess.run(command, text=True, capture_output=True, check=True)
        self.assertIn("passed=True", completed.stdout)
        summary = json.loads((out / "summary.json").read_text())
        self.assertEqual(summary["metadata"]["model_topology"], "e2b")
        candidate = next(
            sample
            for sample in summary["performance_samples"]
            if sample["variant"] == "candidate" and sample["index"] == 1
        )
        routes = candidate["routes"]
        self.assertEqual(routes["paged_1x"], 4_508)
        self.assertEqual(routes["decode_gqa_split"], 0)
        self.assertEqual(routes["q4_linear_reduce_rows"], [13_440, 0, 275, 0])
        self.assertEqual(routes["q4_mmv_variants"], [8_960, 4_480, 0, 0])
        self.assertEqual(routes["q4_pair_activation_decode"], 4_480)
        self.assertEqual(
            routes["q4_pair_activation_policy"]["mmv_nr4_nsg4"], 4_480
        )
        self.assertEqual(routes["lm_head_q4_q6_refine_dispatches"], 128)

    def test_q4_mmv_workload_profile_proves_role_specific_override(self) -> None:
        out = self.tmp / "q4-mmv-workload"
        command = self.paired_command(out)
        command.extend(
            (
                "--baseline-route-profile",
                "q4_mmv_workload",
                "--candidate-route-profile",
                "q4_mmv_workload",
                "--candidate-env",
                "TERMITE_METAL_Q4_0_MMV_FFN_DOWN_VARIANT=nr8-nsg4",
            )
        )
        environment = os.environ.copy()
        environment["STAGE_TIMING_RUNS"] = "0"
        completed = subprocess.run(
            command,
            env=environment,
            text=True,
            capture_output=True,
            check=True,
        )
        self.assertIn("passed=True", completed.stdout)
        summary = json.loads((out / "summary.json").read_text())
        candidate = next(
            sample
            for sample in summary["performance_samples"]
            if sample["variant"] == "candidate"
        )
        routes = candidate["routes"]
        self.assertEqual(routes["q4_mmv_variants"], [21_504, 0, 0, 5_376])
        policy = routes["q4_mmv_workload_policy"]
        self.assertEqual(policy["attention"]["selected"], "nr4-nsg2")
        self.assertEqual(policy["ffn_gate_up"]["selected"], "nr4-nsg2")
        self.assertEqual(policy["ffn_down"]["override"], "nr8-nsg4")
        self.assertEqual(policy["ffn_down"]["selected"], "nr8-nsg4")

    def test_q4_mmv_workload_profile_reconciles_attention_and_gate_up(self) -> None:
        out = self.tmp / "q4-mmv-attention-gate-up"
        command = self.paired_command(out)
        command.extend(
            (
                "--baseline-route-profile",
                "q4_mmv_workload",
                "--candidate-route-profile",
                "q4_mmv_workload",
                "--candidate-env",
                "TERMITE_METAL_Q4_0_MMV_ATTENTION_VARIANT=nr8-nsg4",
                "--candidate-env",
                "TERMITE_METAL_Q4_0_MMV_FFN_GATE_UP_VARIANT=nr8-nsg4",
            )
        )
        environment = os.environ.copy()
        environment["STAGE_TIMING_RUNS"] = "0"
        completed = subprocess.run(
            command,
            env=environment,
            text=True,
            capture_output=True,
            check=True,
        )
        self.assertIn("passed=True", completed.stdout)
        summary = json.loads((out / "summary.json").read_text())
        candidate = next(
            sample
            for sample in summary["performance_samples"]
            if sample["variant"] == "candidate"
        )
        routes = candidate["routes"]
        decode_frames = OUTPUT_TOKENS
        self.assertEqual(
            routes["q4_mmv_variants"],
            [60 * decode_frames, 0, 0, 150 * decode_frames],
        )
        policy = routes["q4_mmv_workload_policy"]
        self.assertEqual(policy["generic"]["selected"], "nr4-nsg2")
        self.assertEqual(policy["attention"]["selected"], "nr8-nsg4")
        self.assertEqual(policy["ffn_gate_up"]["selected"], "nr8-nsg4")
        self.assertEqual(policy["ffn_down"]["selected"], "nr4-nsg2")
        self.assertEqual(
            summary["metadata"]["q4_mmv_workload_contract"][
                "dispatches_per_decode_frame"
            ],
            {"generic": 18, "attention": 66, "ffn_gate_up": 84, "ffn_down": 42},
        )

    def test_q4_mmv_workload_override_requires_route_profile(self) -> None:
        out = self.tmp / "q4-mmv-workload-missing-profile"
        command = self.paired_command(out)
        command.extend(
            (
                "--candidate-env",
                "TERMITE_METAL_Q4_0_MMV_ATTENTION_VARIANT=nr8-nsg2",
            )
        )
        completed = subprocess.run(command, text=True, capture_output=True, check=False)
        self.assertEqual(completed.returncode, 1)
        self.assertIn("without route profile q4_mmv_workload", completed.stderr)

    def test_q4_mmv_workload_profile_rejects_unobservable_role(self) -> None:
        out = self.tmp / "q4-mmv-unobservable-role"
        command = self.paired_command(out)
        command.extend(
            (
                "--candidate-route-profile",
                "q4_mmv_workload",
                "--candidate-env",
                "TERMITE_METAL_Q4_0_MMV_PLE_VARIANT=nr8-nsg4",
            )
        )
        completed = subprocess.run(command, text=True, capture_output=True, check=False)
        self.assertEqual(completed.returncode, 1)
        self.assertIn("no observable row-one ple dispatches", completed.stderr)
        self.assertFalse(out.exists())

    def test_pair_prefill_route_profile_counts_the_shared_pair_counter(self) -> None:
        out = self.tmp / "pair-prefill-routes"
        command = self.paired_command(out)
        command.extend(
            (
                "--target-phase",
                "prefill",
                "--candidate-route-profile",
                "pair_prefill",
                "--candidate-env",
                "TERMITE_METAL_ENABLE_Q4_0_PAIR_ACTIVATION_MM=1",
                "--max-prefill-latency-ratio",
                "1.0",
                "--max-decode-latency-ratio",
                "1.0",
                "--min-decode-throughput-ratio",
                "1.0",
            )
        )
        completed = subprocess.run(command, text=True, capture_output=True, check=True)
        self.assertIn("passed=True", completed.stdout)
        summary = json.loads((out / "summary.json").read_text())
        candidate = next(
            sample
            for sample in summary["performance_samples"]
            if sample["variant"] == "candidate"
        )
        routes = candidate["routes"]
        self.assertEqual(routes["q4_pair_activation_decode"], 0)
        self.assertEqual(routes["q4_pair_activation_total"], 42)
        self.assertEqual(routes["q4_pair_activation_policy"]["mm_m32_n64_tail"], 42)

    def test_regression_and_cv_gates_use_only_performance_samples(self) -> None:
        out = self.tmp / "regression"
        self.run_paired(out)
        path = out / "performance-candidate-02.json"
        payload = json.loads(path.read_text())
        payload["timing_ms"].update(
            {"generate": 14000, "prefill_inner": 4000, "decode_inner": 10000}
        )
        path.write_text(json.dumps(payload))
        summary = build_summary(out)
        self.assertFalse(summary["passed"])
        self.assertIn("candidate_decode_ms", summary["cv_violations"])
        self.assertNotEqual(
            summary["paired_ratios"]["candidate_decode_latency_ratio"]["median"],
            30000 / 6000,
        )

    def test_extra_artifact_and_binary_provenance_are_rejected(self) -> None:
        out = self.tmp / "provenance"
        self.run_paired(out)
        (out / "unexpected.json").write_text("{}")
        with self.assertRaisesRegex(BenchmarkContractError, "unexpected A/B JSON"):
            build_summary(out)
        (out / "unexpected.json").unlink()
        self.binary.write_text(self.binary.read_text() + "\n# changed\n")
        with self.assertRaisesRegex(BenchmarkContractError, "antfly_binary_sha256"):
            build_summary(out)

    def test_short_concurrency_determinism_gate(self) -> None:
        out = self.tmp / "determinism"
        command = [
            sys.executable,
            str(BENCHMARK),
            "run",
            "--out-dir",
            str(out),
            "--experiment-id",
            "concurrency-short",
            "--mode",
            "determinism",
            "--model",
            str(self.model),
            "--antfly-bin",
            str(self.binary),
            "--prompt",
            "fixture prompt",
            "--expected-prompt-tokens",
            str(PROMPT_TOKENS),
            "--expected-prompt-token-ids-sha256",
            token_digest(PROMPT_TOKENS),
            "--output-tokens",
            "64",
            "--expected-token-ids-sha256",
            token_digest(64),
            "--runs",
            "3",
            "--warmups",
            "0",
            "--cooldown-seconds",
            "0",
            "--candidate-route-profile",
            "concurrent_split",
            "--candidate-env",
            "BENCH_VARIANT=candidate",
            "--candidate-env",
            "TERMITE_METAL_ENABLE_CONCURRENT_PLANNED_DISPATCH=1",
        ]
        completed = subprocess.run(command, text=True, capture_output=True, check=True)
        self.assertIn("digests=1 passed=True", completed.stdout)
        summary = json.loads((out / "summary.json").read_text())
        self.assertEqual(len(summary["determinism_samples"]), 3)
        self.assertEqual(summary["unique_output_token_digests"], [token_digest(64)])

        log_path = out / "determinism-candidate-02.log"
        log_path.write_text(log_path.read_text().replace("token_ids: 0 1 2", "token_ids: 9 1 2"))
        with self.assertRaisesRegex(BenchmarkContractError, "output token digest"):
            build_summary(out)

    def test_short_gqa_schedule_determinism_is_not_concurrency_specific(self) -> None:
        out = self.tmp / "gqa-determinism"
        command = [
            sys.executable,
            str(BENCHMARK),
            "run",
            "--out-dir",
            str(out),
            "--experiment-id",
            "gqa-schedule-short",
            "--mode",
            "determinism",
            "--model",
            str(self.model),
            "--antfly-bin",
            str(self.binary),
            "--prompt",
            "fixture prompt",
            "--expected-prompt-tokens",
            str(PROMPT_TOKENS),
            "--expected-prompt-token-ids-sha256",
            token_digest(PROMPT_TOKENS),
            "--output-tokens",
            "64",
            "--expected-token-ids-sha256",
            token_digest(64),
            "--runs",
            "3",
            "--warmups",
            "0",
            "--cooldown-seconds",
            "0",
            "--candidate-route-profile",
            "gqa_split_schedule",
            "--candidate-env",
            "BENCH_VARIANT=candidate",
            "--candidate-env",
            "TERMITE_METAL_DECODE_GQA_SPLIT_SWA_VARIANT=s16",
            "--candidate-env",
            "TERMITE_METAL_DECODE_GQA_SPLIT_GLOBAL_VARIANT=s8",
            "--candidate-env",
            "TERMITE_METAL_DECODE_GQA_SPLIT_MIN_KV=1",
        ]
        completed = subprocess.run(command, text=True, capture_output=True, check=True)
        self.assertIn("digests=1 passed=True", completed.stdout)
        summary = json.loads((out / "summary.json").read_text())
        self.assertEqual(
            summary["metadata"]["candidate_gqa_split_variants"],
            {"global": "s8", "swa": "s16"},
        )
        for sample in summary["determinism_samples"]:
            schedule = sample["routes"]["gqa_split_schedule"]
            self.assertEqual(schedule["swa_s16"], 35 * 64)
            self.assertEqual(schedule["global_s8"], 7 * 64)
            self.assertEqual(
                sample["routes"]["decode_gqa_split_policy"],
                {"min_kv": 1, "below_min_kv": 0},
            )

    def test_stage_only_mode_has_no_performance_statistics(self) -> None:
        out = self.tmp / "stage-only"
        command = [
            sys.executable,
            str(BENCHMARK),
            "run",
            "--out-dir",
            str(out),
            "--experiment-id",
            "stage-only",
            "--mode",
            "stage",
            "--model",
            str(self.model),
            "--antfly-bin",
            str(self.binary),
            "--prompt",
            "fixture prompt",
            "--expected-prompt-tokens",
            str(PROMPT_TOKENS),
            "--expected-prompt-token-ids-sha256",
            token_digest(PROMPT_TOKENS),
            "--output-tokens",
            str(OUTPUT_TOKENS),
            "--expected-token-ids-sha256",
            token_digest(OUTPUT_TOKENS),
            "--warmups",
            "0",
            "--stage-timing-runs",
            "1",
            "--cooldown-seconds",
            "0",
            "--candidate-env",
            "BENCH_VARIANT=candidate",
        ]
        completed = subprocess.run(command, text=True, capture_output=True, check=True)
        self.assertIn("runs=1 passed=True", completed.stdout)
        summary = json.loads((out / "summary.json").read_text())
        self.assertEqual(summary["mode"], "stage")
        self.assertEqual(summary["performance_samples"], [])
        self.assertEqual(len(summary["stage_timing_samples"]), 1)
        self.assertTrue(summary["checks"]["no_performance_samples"])


if __name__ == "__main__":
    unittest.main()
