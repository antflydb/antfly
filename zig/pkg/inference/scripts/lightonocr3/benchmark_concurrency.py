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

"""Measure warm native OCR throughput with concurrent requests in one Node.

The test checks each result against a serial reference from the same resident
checkpoint. Its timed region excludes loading and reference generation. A new
process is used for each setting; all concurrency is inside that one process.
"""

import argparse
import json
import os
import re
import signal
import subprocess
from datetime import datetime, timezone
from pathlib import Path

from benchmark_antfly_readers import checkpoint, digest, hardware, plain_text

RESULT = re.compile(
    r"OCR_CONCURRENCY concurrency=(\d+) requests=(\d+) pages_per_request=(\d+) "
    r"completed=(\d+) elapsed_ms=(\d+) pages_per_second=([\d.]+) "
    r"p50_ms=(\d+) p95_ms=(\d+) native_items=(\d+) parity=true"
)


def prefill_stream_observation(text):
    values = re.findall(
        r"LIGHTONOCR3 prefill_stream_layers=(\d+) prefill_stream_splits=(\d+)", text
    )
    return {
        "prefill_stream_layers_observed": sorted({int(k) for k, _ in values}),
        "prefill_stream_splits": sum(int(n) for _, n in values),
    }


def run(args, model, label, mode, concurrency, output):
    env = os.environ.copy()
    for key in list(env):
        if key.startswith(
            ("TERMITE_METAL_DISABLE_LIGHTONOCR3", "TERMITE_METAL_DISABLE_QWEN35")
        ) or key in (
            "TERMITE_METAL_LIGHTONOCR3_BF16_ACTIVATIONS",
            "TERMITE_METAL_ENABLE_LIGHTONOCR3_WEIGHT_RESIDENCY",
            "TERMITE_METAL_ENABLE_LIGHTONOCR3_RECURRENT_PARALLEL",
            "TERMITE_METAL_ENABLE_LIGHTONOCR3_RECURRENT_COALESCING",
            "TERMITE_METAL_TRACE_LIGHTONOCR3_RECURRENT_PARALLEL",
            "TERMITE_METAL_ENABLE_LIGHTONOCR3_PREFILL_ATTENTION_MPP",
            "TERMITE_METAL_TRACE_LIGHTONOCR3_PREFILL_ATTENTION_MPP",
            "TERMITE_METAL_ENABLE_LIGHTONOCR3_PREFILL_CORE_NORM",
            "TERMITE_METAL_TRACE_LIGHTONOCR3_PREFILL_CORE_NORM",
            "ANTFLY_TEST_OCR_POLICY_ROUNDS",
            "TERMITE_METAL_LIGHTONOCR3_BF16_PREFILL_INPUTS",
            "ANTFLY_INFERENCE_FLORENCE_DISABLE_KV_CACHE",
            "ANTFLY_FLORENCE2_BATCH_WAIT_US",
            "ANTFLY_FLORENCE2_DISABLE_NO_REPEAT_ARGMAX",
            "ANTFLY_FLORENCE2_TRACE_NO_REPEAT_ARGMAX",
            "ANTFLY_FLORENCE2_SINGLE_FRAME",
            "ANTFLY_INFERENCE_READ_PROFILE",
            "ANTFLY_LIGHTONOCR3_LOGICAL64",
            "ANTFLY_LIGHTONOCR3_DEVICE_RECURRENT_STATE",
            "ANTFLY_LIGHTONOCR3_SHARED_DECODER_SCRATCH",
            "ANTFLY_OCR_ENCODER_SCOPES",
            "ANTFLY_FLORENCE_CPU_PREPROCESS",
            "ANTFLY_LIGHTONOCR3_TOKEN_ENCODER",
            "ANTFLY_LIGHTONOCR3_GPU_GREEDY_STREAM",
            "ANTFLY_LIGHTONOCR3_CONTINUOUS",
            "ANTFLY_TEST_OCR_STATIC_WORK_ASSIGNMENT",
            "ANTFLY_LIGHTONOCR3_STATIC_PAGE_ASSIGNMENT",
            "ANTFLY_LIGHTONOCR3_CONTENTION_WAIT_US",
            "ANTFLY_LIGHTONOCR3_WAIT_FOR_PREFILL_PEERS",
            "ANTFLY_LIGHTONOCR3_READY_COHORTS",
            "ANTFLY_LIGHTONOCR3_DISABLE_GATE_PARK",
            "TERMITE_METAL_ENABLE_LIGHTONOCR3_FRAME_POLICY",
            "TERMITE_METAL_TRACE_LIGHTONOCR3_COMPUTE_COPIES",
            "TERMITE_METAL_TRACE_LIGHTONOCR3_PACKED_CORE",
            "TERMITE_METAL_TRACE_LIGHTONOCR3_WEIGHT_HANDLES",
            "TERMITE_METAL_ENABLE_LIGHTONOCR3_WEIGHT_HANDLES",
            "ANTFLY_LIGHTONOCR3_VISION_BATCH",
            "ANTFLY_LIGHTONOCR3_DISABLE_SHARED_VISION_PEAK",
            "ANTFLY_LIGHTONOCR3_PREFILL_BATCH",
            "ANTFLY_LIGHTONOCR3_PACKED_MIXED",
            "ANTFLY_LIGHTONOCR3_PACKED_SHARED_FRAME",
            "ANTFLY_LIGHTONOCR3_PACKED_PREFILL_ITEMS",
            "ANTFLY_LIGHTONOCR3_PREFILL_ACTIVATION_POOL",
            "ANTFLY_LIGHTONOCR3_VISION_WAIT_US",
            "ANTFLY_LIGHTONOCR3_PREFILL_WAIT_US",
            "TERMITE_METAL_ENABLE_PREFILL_SG_ATTENTION",
            "TERMITE_METAL_ENABLE_QWEN3VL_PREFILL_SG_ATTENTION",
            "TERMITE_METAL_DISABLE_QWEN3VL_PREFILL_SG_ATTENTION",
            "TERMITE_METAL_DISABLE_PREFILL_SG_ATTENTION",
            "TERMITE_METAL_ENABLE_PREFILL_SG_DIRECT_LOAD",
            "TERMITE_METAL_DISABLE_PREFILL_SG_DIRECT_LOAD",
            "TERMITE_SERVER_GENERATE_TIMING",
            "TERMITE_METAL_ENABLE_ACTIVE_PAGED_SLOT_ATTENTION_DEFERRED",
            "TERMITE_METAL_DISABLE_ACTIVE_PAGED_SLOT_ATTENTION_DEFERRED",
            "TERMITE_METAL_DISABLE_QWEN35_CORE_BATCH",
            "TERMITE_METAL_DISABLE_QWEN35_CORE_ENCODER",
            "TERMITE_METAL_DISABLE_QWEN35_CORE_PARALLEL_NORM",
            "TERMITE_METAL_LIGHTONOCR3_SMALL_MPP",
            "TERMITE_METAL_FLORENCE2_ENCODER_MIRRORS",
            "TERMITE_METAL_DISABLE_FLORENCE2_CHANNEL_REDUCTION",
            "TERMITE_METAL_DISABLE_F16_ENCODER_MPP",
            "TERMITE_METAL_DISABLE_FLORENCE2_WINDOW_SOFTMAX",
            "TERMITE_METAL_DISABLE_BF16_FUSED_GATE_UP",
            "TERMITE_METAL_DISABLE_LIGHTONOCR3_FFN_SMALL_TILE",
        ):
            del env[key]
    env["ANTFLY_LIGHTONOCR3_PACKED_MIXED"] = "1" if args.packed_mixed else "0"
    env["ANTFLY_LIGHTONOCR3_PACKED_SHARED_FRAME"] = (
        "1" if args.packed_shared_frame else "0"
    )
    if args.packed_shared_frame:
        env["ANTFLY_LIGHTONOCR3_SINGLE_FRAME"] = "1"
        env["ANTFLY_LIGHTONOCR3_STREAM_FIRST_LAYERS"] = "0"
    env["ANTFLY_LIGHTONOCR3_PACKED_PREFILL_ITEMS"] = str(args.packed_prefill_items)
    env["ANTFLY_LIGHTONOCR3_PREFILL_QUERY_CHUNK_ROWS"] = str(
        args.prepared_prefill_chunk_rows
    )
    env["ANTFLY_LIGHTONOCR3_PREFILL_ACTIVATION_POOL"] = (
        "1" if args.prefill_activation_pool else "0"
    )
    if args.frame_policy:
        env["TERMITE_METAL_ENABLE_LIGHTONOCR3_FRAME_POLICY"] = "1"
    if args.contention_wait_us is not None:
        env["ANTFLY_LIGHTONOCR3_CONTENTION_WAIT_US"] = str(args.contention_wait_us)
    if args.policy_rounds:
        env["ANTFLY_TEST_OCR_POLICY_ROUNDS"] = json.dumps(args.policy_rounds)
    if args.prefill_core_norm:
        env["TERMITE_METAL_ENABLE_LIGHTONOCR3_PREFILL_CORE_NORM"] = "1"
    if args.disable_prefill_core_norm:
        env["TERMITE_METAL_DISABLE_LIGHTONOCR3_PREFILL_CORE_NORM"] = "1"
    if args.require_prefill_core_norm:
        env["TERMITE_METAL_TRACE_LIGHTONOCR3_PREFILL_CORE_NORM"] = "1"
    if args.static_work_assignment:
        env["ANTFLY_TEST_OCR_STATIC_WORK_ASSIGNMENT"] = "1"
        env["ANTFLY_LIGHTONOCR3_STATIC_PAGE_ASSIGNMENT"] = "1"
    if args.disable_direct_core_output:
        env["TERMITE_METAL_DISABLE_LIGHTONOCR3_DIRECT_CORE_OUTPUT"] = "1"
    if args.disable_compute_copies:
        env["TERMITE_METAL_DISABLE_LIGHTONOCR3_COMPUTE_COPIES"] = "1"
    if args.weight_handles:
        env["TERMITE_METAL_ENABLE_LIGHTONOCR3_WEIGHT_HANDLES"] = "1"
    if args.require_weight_handles:
        env["TERMITE_METAL_TRACE_LIGHTONOCR3_WEIGHT_HANDLES"] = "1"
    if args.disable_packed_core:
        env["TERMITE_METAL_DISABLE_LIGHTONOCR3_PACKED_CORE"] = "1"
    if args.trace_packed_core or args.require_packed_core:
        env["TERMITE_METAL_TRACE_LIGHTONOCR3_PACKED_CORE"] = "1"
    if args.trace_compute_copies:
        env["TERMITE_METAL_TRACE_LIGHTONOCR3_COMPUTE_COPIES"] = "1"
    if args.disable_prefill_direct_kv_mpp:
        env["TERMITE_METAL_DISABLE_LIGHTONOCR3_PREFILL_DIRECT_KV"] = "1"
    if args.prefill_attention_mpp:
        env["TERMITE_METAL_ENABLE_LIGHTONOCR3_PREFILL_ATTENTION_MPP"] = "1"
    if args.disable_prefill_attention_mpp:
        env["TERMITE_METAL_DISABLE_LIGHTONOCR3_PREFILL_ATTENTION_MPP"] = "1"
    if args.require_prefill_attention_mpp:
        env["TERMITE_METAL_TRACE_LIGHTONOCR3_PREFILL_ATTENTION_MPP"] = "1"
    env["TERMITE_METAL_ENABLE_LIGHTONOCR3_WIDE_RECURRENT"] = (
        "1" if args.wide_recurrent or args.require_wide_recurrent else "0"
    )
    if args.recurrent_parallel:
        env["TERMITE_METAL_ENABLE_LIGHTONOCR3_RECURRENT_PARALLEL"] = "1"
    if args.disable_recurrent_parallel:
        env["TERMITE_METAL_DISABLE_LIGHTONOCR3_RECURRENT_PARALLEL"] = "1"
    if args.recurrent_coalescing:
        env["TERMITE_METAL_ENABLE_LIGHTONOCR3_RECURRENT_COALESCING"] = "1"
    if args.require_recurrent_parallel:
        env["TERMITE_METAL_TRACE_LIGHTONOCR3_RECURRENT_PARALLEL"] = "1"
    if args.disable_decode_pack_reuse:
        env["TERMITE_METAL_DISABLE_LIGHTONOCR3_DECODE_PACK_REUSE"] = "1"
    if args.disable_vision_pack_reuse:
        env["TERMITE_METAL_DISABLE_LIGHTONOCR3_VISION_PACK_REUSE"] = "1"
    env["ANTFLY_LIGHTONOCR3_TOKEN_ENCODER"] = (
        "0" if args.disable_lighton_token_encoder else "1"
    )
    env["TERMITE_METAL_DISABLE_LIGHTONOCR3_WIDE_GATE_UP"] = (
        "1" if args.disable_wide_gate_up else "0"
    )
    env["TERMITE_METAL_ENABLE_LIGHTONOCR3_BATCH_ARGMAX"] = (
        "1" if args.batch_argmax else "0"
    )
    env["ANTFLY_LIGHTONOCR3_VISION_LATE_SEAL"] = "1" if args.defer_vision_seal else "0"
    env["ANTFLY_LIGHTONOCR3_PREFILL_LATE_SEAL"] = (
        "1" if args.defer_prefill_seal else "0"
    )
    env["ANTFLY_LIGHTONOCR3_KEEP_VISION_WORKSPACE"] = (
        "1" if args.cache_vision_workspace else "0"
    )
    env["TERMITE_METAL_ENABLE_LIGHTONOCR3_LARGE_VISION_PACK_REUSE"] = (
        "1" if args.large_vision_pack_reuse else "0"
    )
    env["ANTFLY_LIGHTONOCR3_RESIDENT_VISION_BUDGET"] = (
        "1" if args.resident_vision_budget else "0"
    )
    env["ANTFLY_LIGHTONOCR3_PREFILL_STREAM_LAYERS"] = str(args.prefill_stream_layers)
    env["ANTFLY_LIGHTONOCR3_PIPELINE_VISION"] = "1" if args.pipeline_vision else "0"
    if args.pipeline_vision:
        env["ANTFLY_LIGHTONOCR3_CPU_PREPROCESS"] = "1"
        env["TERMITE_METAL_ENABLE_PIPELINED_DECODE_FRAME"] = "1"
    env["ANTFLY_LIGHTONOCR3_GPU_TOKEN_BURST"] = "1" if args.gpu_token_burst else "0"
    env["ANTFLY_LIGHTONOCR3_GPU_TOKEN_BURST_WIDTH"] = str(args.gpu_token_burst_width)
    if args.gpu_token_burst:
        env["ANTFLY_LIGHTONOCR3_SINGLE_FRAME"] = "1" if args.backend == "metal" else "0"
        env["ANTFLY_LIGHTONOCR3_STREAM_FIRST_LAYERS"] = "0"
    env["ANTFLY_LIGHTONOCR3_STREAM_CHUNKS"] = "1" if args.stream_chunk_layers else "0"
    if args.stream_chunk_layers:
        env["ANTFLY_LIGHTONOCR3_STREAM_FIRST_LAYERS"] = str(args.stream_chunk_layers)
    env["ANTFLY_LIGHTONOCR3_GPU_GREEDY_STREAM"] = "1" if args.gpu_greedy_stream else "0"
    env["TERMITE_METAL_ENABLE_LIGHTONOCR3_WIDE64_GATE_UP"] = (
        "1" if args.wide64_gate_up else "0"
    )
    env["TERMITE_METAL_ENABLE_LIGHTONOCR3_FULL64_GATE_UP"] = (
        "1" if args.full64_gate_up else "0"
    )
    if args.disable_64_row_tile:
        env["TERMITE_METAL_DISABLE_LIGHTONOCR3_64_ROW_TILE"] = "1"
    if args.disable_planned_mpp:
        env["TERMITE_METAL_DISABLE_LIGHTONOCR3_PLANNED_MPP"] = "1"
    env["TERMITE_METAL_ENABLE_LIGHTONOCR3_PREFILL_FFN"] = (
        "1" if args.prefill_fused_ffn else "0"
    )
    env.pop("ANTFLY_TEST_OCR_PREFILL_FFN_ROUNDS", None)
    if args.prefill_ffn_rounds:
        env["ANTFLY_TEST_OCR_PREFILL_FFN_ROUNDS"] = json.dumps(
            [value == "on" for value in args.prefill_ffn_rounds]
        )
    env.pop("ANTFLY_TEST_OCR_BF16_PROJECTION_ROUNDS", None)
    if args.bf16_projection_rounds:
        env["ANTFLY_TEST_OCR_BF16_PROJECTION_ROUNDS"] = json.dumps(
            [value == "on" for value in args.bf16_projection_rounds]
        )
    if args.bf16_prefill_inputs:
        env["TERMITE_METAL_LIGHTONOCR3_BF16_PREFILL_INPUTS"] = "1"
    if args.prefill_sg_attention:
        env["TERMITE_METAL_ENABLE_PREFILL_SG_ATTENTION"] = "1"
    if args.prefill_direct_kv:
        env["TERMITE_METAL_ENABLE_PREFILL_SG_DIRECT_LOAD"] = "1"
    env["ANTFLY_LIGHTONOCR3_VISION_WAIT_US"] = str(args.vision_wait_us)
    env["ANTFLY_LIGHTONOCR3_PREFILL_WAIT_US"] = str(args.prefill_wait_us)
    if args.prefill_batch:
        env["ANTFLY_LIGHTONOCR3_PREFILL_BATCH"] = "1"
    env["ANTFLY_LIGHTONOCR3_PREFILL_BATCH_ITEMS"] = str(args.prefill_batch_items)
    if args.disable_shared_vision_peak:
        env["ANTFLY_LIGHTONOCR3_DISABLE_SHARED_VISION_PEAK"] = "1"
    if args.vision_batch:
        env["ANTFLY_LIGHTONOCR3_VISION_BATCH"] = "1"
    env["ANTFLY_LIGHTONOCR3_VISION_BATCH_ITEMS"] = str(args.vision_batch_items)
    env["ANTFLY_LIGHTONOCR3_CONTINUOUS"] = "1" if args.continuous else "0"
    env["ANTFLY_LIGHTONOCR3_WAIT_FOR_PREFILL_PEERS"] = (
        "1" if args.wait_for_prefill_peers else "0"
    )
    env["ANTFLY_LIGHTONOCR3_QUEUED_PHASE_PRIORITY"] = (
        "1" if args.queued_phase_priority else "0"
    )
    env["ANTFLY_LIGHTONOCR3_PREFILL_DECODE_STREAK"] = str(args.prefill_decode_streak)
    env["ANTFLY_LIGHTONOCR3_CUDA_WIDE_MICROBATCHES"] = (
        "1" if args.cuda_wide_microbatches else "0"
    )
    env["ANTFLY_LIGHTONOCR3_EXECUTOR_TAIL"] = "1" if args.executor_tail else "0"
    env["ANTFLY_LIGHTONOCR3_PHASE_FAIR"] = "1" if args.phase_fair else "0"
    env["ANTFLY_LIGHTONOCR3_PHASE_PARK"] = "1" if args.phase_park else "0"
    env["ANTFLY_LIGHTONOCR3_TURN_COALESCING"] = "1" if args.turn_coalescing else "0"
    env["ANTFLY_LIGHTONOCR3_LOGICAL64"] = "1" if args.logical64 else "0"
    env["ANTFLY_LIGHTONOCR3_DEVICE_RECURRENT_STATE"] = (
        "1" if args.device_recurrent_state else "0"
    )
    env["ANTFLY_OCR_ENCODER_SCOPES"] = "1" if args.ocr_encoder_scopes else "0"
    env["ANTFLY_FLORENCE_CPU_PREPROCESS"] = "1" if args.florence_cpu_preprocess else "0"
    env["ANTFLY_LIGHTONOCR3_PHYSICAL32"] = "1" if args.physical32 else "0"
    env["ANTFLY_LIGHTONOCR3_PHYSICAL64"] = "1" if args.physical64 else "0"
    env["ANTFLY_LIGHTONOCR3_PHYSICAL128"] = "1" if args.physical128 else "0"
    if args.compact_kv:
        env["ANTFLY_LIGHTONOCR3_COMPACT_KV"] = "1"
    env["ANTFLY_LIGHTONOCR3_SHARED_DECODER_SCRATCH"] = (
        "1" if args.share_decoder_scratch else "0"
    )
    if args.physical32 or args.physical64 or args.physical128:
        env["ANTFLY_LIGHTONOCR3_WIDE_DECODE"] = "1"
    if (
        args.require_small_tail_fusion
        or args.require_prefill_gates
        or args.require_decode_projections
    ):
        env["TERMITE_METAL_TRACE_DENSE_LINEAR_SHAPES"] = "1"
    if args.require_vision_mpp:
        env["TERMITE_METAL_TRACE_VISION_ATTENTION"] = "1"
    if args.profile_reader:
        env["ANTFLY_INFERENCE_READ_PROFILE"] = "1"
        env["TERMITE_METAL_TRACE_VISION_ATTENTION"] = "1"
    env["ANTFLY_FLORENCE2_BATCH_WAIT_US"] = str(args.florence_batch_wait_us)
    env["ANTFLY_FLORENCE2_SINGLE_FRAME"] = (
        "0" if args.disable_florence_single_frame else "1"
    )
    if args.require_florence_single_frame:
        env["ANTFLY_FLORENCE2_TRACE_NO_REPEAT_ARGMAX"] = "1"
    if args.disable_florence_no_repeat_argmax:
        env["ANTFLY_FLORENCE2_DISABLE_NO_REPEAT_ARGMAX"] = "1"
    if args.require_florence_no_repeat_argmax:
        env["ANTFLY_FLORENCE2_TRACE_NO_REPEAT_ARGMAX"] = "1"
    if args.deferred_attention == "on":
        env["TERMITE_METAL_ENABLE_ACTIVE_PAGED_SLOT_ATTENTION_DEFERRED"] = "1"
    elif args.deferred_attention == "off":
        env["TERMITE_METAL_DISABLE_ACTIVE_PAGED_SLOT_ATTENTION_DEFERRED"] = "1"
    if args.disable_florence_window_softmax:
        env["TERMITE_METAL_DISABLE_FLORENCE2_WINDOW_SOFTMAX"] = "1"
    if args.disable_ocr_ffn_fusion:
        env["TERMITE_METAL_DISABLE_BF16_FUSED_GATE_UP"] = "1"
    if args.disable_ocr_attention:
        env["TERMITE_METAL_DISABLE_LIGHTONOCR3_GQA_SPLIT"] = "1"
    if args.disable_tail_split:
        env["TERMITE_METAL_DISABLE_LIGHTONOCR3_TAIL_SPLIT"] = "1"
    if args.disable_partial_qkv_split:
        env["TERMITE_METAL_DISABLE_LIGHTONOCR3_PARTIAL_QKV_SPLIT"] = "1"
    if args.disable_partial_down_split:
        env["TERMITE_METAL_DISABLE_LIGHTONOCR3_PARTIAL_DOWN_SPLIT"] = "1"
    if args.disable_gate_up_split:
        env["TERMITE_METAL_DISABLE_LIGHTONOCR3_GATE_UP_SPLIT"] = "1"
    if args.disable_batch_gate_up:
        env["TERMITE_METAL_DISABLE_LIGHTONOCR3_BATCH_GATE_UP"] = "1"
    if args.disable_prefill_columns:
        env["TERMITE_METAL_DISABLE_QWEN35_PREFILL_COLUMNS"] = "1"
    if args.disable_single_down_tile:
        env["TERMITE_METAL_DISABLE_LIGHTONOCR3_SINGLE_DOWN_TILE"] = "1"
    if args.disable_four_row_split:
        env["TERMITE_METAL_DISABLE_LIGHTONOCR3_FOUR_ROW_SPLIT"] = "1"
    if args.disable_prefill_norm:
        env["TERMITE_METAL_DISABLE_LIGHTONOCR3_PREFILL_NORM"] = "1"
    if args.disable_prefill_conv_vec4:
        env["TERMITE_METAL_DISABLE_QWEN35_PREFILL_CONV_VEC4"] = "1"
    if args.disable_prefill_projections:
        env["TERMITE_METAL_DISABLE_LIGHTONOCR3_PREFILL_PROJECTIONS"] = "1"
    if args.disable_vision_mpp:
        env["TERMITE_METAL_DISABLE_LIGHTONOCR3_VISION_MPP"] = "1"
    if args.disable_local_kernels:
        env["TERMITE_METAL_DISABLE_LIGHTONOCR3_LOCAL_KERNELS"] = "1"
    if args.weight_residency:
        env["TERMITE_METAL_ENABLE_LIGHTONOCR3_WEIGHT_RESIDENCY"] = "1"
    if args.disable_f16_encoder_mpp:
        env["TERMITE_METAL_DISABLE_F16_ENCODER_MPP"] = "1"
    if args.disable_four_row_tile:
        env["TERMITE_METAL_DISABLE_LIGHTONOCR3_FOUR_ROW_TILE"] = "1"
    if args.disable_down_split:
        env["TERMITE_METAL_DISABLE_LIGHTONOCR3_DOWN_SPLIT"] = "1"
    if args.disable_qkv_small_tile:
        env["TERMITE_METAL_DISABLE_LIGHTONOCR3_QKV_SMALL_TILE"] = "1"
    if args.disable_ffn_small_tile:
        env["TERMITE_METAL_DISABLE_LIGHTONOCR3_FFN_SMALL_TILE"] = "1"
    if args.disable_recurrent_fusion:
        env["TERMITE_METAL_DISABLE_QWEN35_FUSED_DECODE"] = "1"
    if args.disable_florence_channel_reduction:
        env["TERMITE_METAL_DISABLE_FLORENCE2_CHANNEL_REDUCTION"] = "1"
    if args.disable_recurrent_batch:
        env["TERMITE_METAL_DISABLE_QWEN35_CORE_BATCH"] = "1"
    if args.disable_recurrent_encoder:
        env["TERMITE_METAL_DISABLE_QWEN35_CORE_ENCODER"] = "1"
    if args.disable_recurrent_norm:
        env["TERMITE_METAL_DISABLE_QWEN35_CORE_PARALLEL_NORM"] = "1"
    if args.disable_step_pool:
        env["TERMITE_METAL_DISABLE_QWEN35_STEP_POOL"] = "1"
    if args.small_mpp:
        env["TERMITE_METAL_LIGHTONOCR3_SMALL_MPP"] = "1"
    if args.florence_encoder_mirrors:
        env["TERMITE_METAL_FLORENCE2_ENCODER_MIRRORS"] = "1"
    elif args.disable_florence_encoder_mirrors:
        env["TERMITE_METAL_FLORENCE2_ENCODER_MIRRORS"] = "0"
    if args.disable_prefill_pack_reuse:
        env["TERMITE_METAL_DISABLE_LIGHTONOCR3_PREFILL_PACK_REUSE"] = "1"
    if args.disable_wide_mpp:
        env["TERMITE_METAL_DISABLE_LIGHTONOCR3_WIDE_MPP"] = "1"
    if args.disable_wide_projection_tiles:
        env["TERMITE_METAL_DISABLE_LIGHTONOCR3_WIDE_PROJECTION_TILES"] = "1"
    if args.bf16_projection_inputs:
        env["TERMITE_METAL_LIGHTONOCR3_BF16_ACTIVATIONS"] = "1"
    if args.disable_batch_projections:
        env["TERMITE_METAL_DISABLE_LIGHTONOCR3_BATCH_PROJECTIONS"] = "1"
    if args.disable_tiled_projections:
        env["TERMITE_METAL_DISABLE_LIGHTONOCR3_TILED_PROJECTIONS"] = "1"
    env.update(
        ANTFLY_TEST_LIGHTONOCR3_MODEL_DIR=str(model),
        ANTFLY_TEST_LIGHTONOCR3_CONCURRENCY=str(concurrency),
        ANTFLY_TEST_LIGHTONOCR3_REQUESTS=str(args.requests),
        ANTFLY_TEST_LIGHTONOCR3_PAGES_PER_REQUEST=str(args.pages_per_request),
        ANTFLY_TEST_LIGHTONOCR3_MAX_TOKENS=str(args.max_tokens),
        ANTFLY_TEST_LIGHTONOCR3_ADMISSION_UNITS=str(args.admission_units),
        ANTFLY_TEST_LIGHTONOCR3_WARM_RUNS="0",
        ANTFLY_TEST_LIGHTONOCR3_HTTP_BATCH="0",
        ANTFLY_TEST_LIGHTONOCR3_MEMORY_LIMIT_GIB=str(args.memory_gib),
        ANTFLY_TEST_LIGHTONOCR3_SHARED_LIMIT_GIB=str(args.shared_memory_gib),
        ANTFLY_TEST_LIGHTONOCR3_BACKEND_LIMIT_GIB=str(args.backend_memory_gib),
        ANTFLY_TEST_LIGHTONOCR3_KV_LIMIT_GIB=str(args.kv_memory_gib),
        ANTFLY_TEST_LIGHTONOCR3_SCRATCH_LIMIT_GIB=str(args.scratch_memory_gib),
        ANTFLY_LIGHTONOCR3_BATCH="1" if mode == "batch" else "0",
        ANTFLY_INFERENCE_READ_BATCH_SIZE=str(args.native_read_batch_size),
        ANTFLY_LIGHTONOCR3_BATCH_ROWS=str(args.batch_rows),
        ANTFLY_LIGHTONOCR3_BATCH_WINDOW=str(args.batch_window),
        ANTFLY_LIGHTONOCR3_BATCH_WAIT_US=str(args.batch_wait_us),
        ANTFLY_LIGHTONOCR3_DECODE_WAIT_US=str(args.decode_wait_us),
        ANTFLY_BENCH_LIGHTONOCR3="1",
        ANTFLY_INFERENCE_PREFERRED_BACKEND=args.backend,
        ANTFLY_INFERENCE_REQUIRED_BACKEND=args.backend,
        TERMITE_CUDA_ENABLE_QWEN35_RECURRENT_CORE="1"
        if args.cuda_recurrent_core
        else "0",
        ANTFLY_LIGHTONOCR3_CUDA_BATCH="1" if args.cuda_batch else "0",
    )
    if args.extended_epoch:
        env["ANTFLY_LIGHTONOCR3_EXTENDED_EPOCH"] = "1"
    if args.images:
        env["ANTFLY_TEST_LIGHTONOCR3_IMAGE_PATH"] = str(args.images[0].resolve())
        env["ANTFLY_TEST_LIGHTONOCR3_IMAGES_JSON"] = json.dumps(
            [str(image.resolve()) for image in args.images]
        )
    path = output / f"{label}-{mode}-c{concurrency}.txt"
    command = [
        str(args.native_test.resolve()),
        "--test-filter",
        "LightOnOCR3 real checkpoint",
    ]
    timed_out = False
    with path.open("w") as log:
        process = subprocess.Popen(
            command,
            env=env,
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        try:
            process.wait(timeout=args.timeout)
        except subprocess.TimeoutExpired:
            timed_out = True
            os.killpg(process.pid, signal.SIGTERM)
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait()
    text = path.read_text()
    match = RESULT.search(text)
    result = {
        "model": label,
        "cuda_environment": {
            key: env.get(key)
            for key in (
                "TERMITE_CUDA_ENABLE_QWEN35_RECURRENT_CORE",
                "ANTFLY_LIGHTONOCR3_CUDA_BATCH",
                "ANTFLY_LIGHTONOCR3_PREPARED_DECODE",
                "ANTFLY_LIGHTONOCR3_EXTENDED_EPOCH",
                "ANTFLY_INFERENCE_CUDA_GQA_PREFILL_PROFILE",
                "ANTFLY_INFERENCE_CUDA_TURBOQUANT_SPLIT_ATTENTION",
                "ANTFLY_INFERENCE_CUDA_TURBOQUANT_SPLIT_ATTENTION_CHUNK",
                "ANTFLY_CUDA_ENABLE_LIGHTON_BATCHED_ATTENTION",
                "ANTFLY_CUDA_ENABLE_LIGHTON_BATCHED_KV_WRITE",
                "ANTFLY_CUDA_ENABLE_LIGHTON_GEMM_BUCKET_ROWS",
                "ANTFLY_CUDA_ENABLE_LIGHTON_BF16_DECODE_OUTPUTS",
                "ANTFLY_CUDA_ENABLE_LIGHTON_BF16_PROJECTION_OUTPUTS",
                "ANTFLY_CUDA_ENABLE_LIGHTON_BF16_PREFILL_FFN",
                "ANTFLY_CUDA_ENABLE_LIGHTON_FUSED_FFN",
                "ANTFLY_CUDA_ENABLE_LIGHTON_NORM_WEIGHT_CACHE",
                "ANTFLY_CUDA_ENABLE_LIGHTON_PACKED_MIXED",
                "ANTFLY_CUDA_ENABLE_LIGHTON_FUSED_VISION_QKV",
                "ANTFLY_CUDA_ENABLE_LIGHTON_RAGGED_RECURRENT",
                "ANTFLY_CUDA_ENABLE_LIGHTON_FFN_BF16_INPUTS",
                "ANTFLY_CUDA_ENABLE_LIGHTON_FFN_BLOCK",
                "ANTFLY_CUDA_ENABLE_LIGHTON_FUSED_GDN_DECODE",
                "ANTFLY_CUDA_PROFILE_OCR_TEMP_SCANS",
                "ANTFLY_LIGHTONOCR3_PHYSICAL128",
                "ANTFLY_LIGHTONOCR3_COMPACT_KV",
                "ANTFLY_CUDA_ENABLE_FLORENCE_CANONICAL_DECODE_GEMM",
                "ANTFLY_FLORENCE2_CUDA_CHECK_ENCODER_BATCH",
                "ANTFLY_TEST_OCR_CUDA_PROFILE_RANGE",
                "ANTFLY_CUDA_ENABLE_OCR_PINNED_METADATA",
                "ANTFLY_CUDA_ENABLE_OCR_PINNED_INPUTS",
                "ANTFLY_CUDA_ENABLE_FLORENCE_FUSED_CROSS_ATTENTION",
                "ANTFLY_CUDA_ENABLE_LIGHTON_CACHED_MROPE_POSITIONS",
                "ANTFLY_CUDA_ENABLE_LIGHTON_VISION_CUDNN",
                "ANTFLY_LIGHTONOCR3_WAIT_FOR_PREFILL_PEERS",
                "ANTFLY_LIGHTONOCR3_QUEUED_PHASE_PRIORITY",
                "ANTFLY_LIGHTONOCR3_PREFILL_DECODE_STREAK",
                "ANTFLY_LIGHTONOCR3_TURN_COALESCING",
                "ANTFLY_LIGHTONOCR3_PHASE_FAIR",
                "ANTFLY_LIGHTONOCR3_PHASE_PARK",
                "ANTFLY_LIGHTONOCR3_EXECUTOR_TAIL",
                "ANTFLY_LIGHTONOCR3_CUDA_WIDE_MICROBATCHES",
                "ANTFLY_INFERENCE_CUDA_RMS_NORM_BF16_MIRROR",
                "ANTFLY_CUDA_ENABLE_LIGHTON_BATCHED_RECURRENT",
                "ANTFLY_CUDA_ENABLE_LIGHTON_FUSED_BATCH_RECURRENT",
                "ANTFLY_CUDA_ENABLE_LIGHTON_CACHED_RECURRENT_GATES",
                "ANTFLY_CUDA_ENABLE_LIGHTON_ATTENTION_CHUNK128",
                "ANTFLY_CUDA_ENABLE_FLORENCE_F16_WEIGHTS",
                "ANTFLY_CUDA_ENABLE_FLORENCE_CUDNN",
                "ANTFLY_CUDA_ENABLE_FLORENCE_CUDNN_CONV",
                "ANTFLY_CUDA_ENABLE_FLORENCE_CHANNEL_TC",
                "ANTFLY_CUDA_ENABLE_FLORENCE_GROUPED_CHANNEL_TC",
                "ANTFLY_CUDA_ENABLE_FLORENCE_CHANNEL_PLAN_CACHE",
                "ANTFLY_CUDA_ENABLE_FLORENCE_FUSED_BIAS",
                "ANTFLY_CUDA_ENABLE_FLORENCE_FUSED_DEPTHWISE",
                "ANTFLY_CUDA_ENABLE_FLORENCE_FUSED_MLP",
                "ANTFLY_FLORENCE2_CUDA_ENCODER_MICROBATCH",
                "ANTFLY_FLORENCE2_CUDA_VISION_MICROBATCH",
                "ANTFLY_CUDA_ENABLE_LIGHTON_FUSED_PROJECTIONS",
                "ANTFLY_CUDA_ENABLE_FLORENCE_FUSED_ATTENTION_INPUTS",
                "ANTFLY_CUDA_ENABLE_FLORENCE_PACKED_SELF_KV",
                "ANTFLY_CUDA_ENABLE_FLORENCE_BATCH_VISION_TAIL",
                "ANTFLY_INFERENCE_CUDA_CUBLASLT_BF16_TUNING_PROFILE",
                "ANTFLY_INFERENCE_CUDA_CUBLASLT_WORKSPACE_MB",
                "ANTFLY_INFERENCE_CUDA_CUBLASLT_PLAN_CACHE_ENTRIES",
                "ANTFLY_CUDA_ENABLE_LIGHTON_PARALLEL_PREFIX",
                "ANTFLY_CUDA_ENABLE_LIGHTON_BATCHED_PREFIX",
                "ANTFLY_CUDA_LIGHTON_PREFILL_LIBRARY",
                "ANTFLY_CUDA_ENABLE_LIGHTON_PREFIX_SHARED_QK",
                "ANTFLY_CUDA_ENABLE_LIGHTON_STATE_POOL",
                "ANTFLY_CUDA_ENABLE_LIGHTON_SHARED_RECURRENT_WEIGHTS",
                "ANTFLY_CUDA_ENABLE_LIGHTON_FUSED_VISION_MLP",
                "ANTFLY_CUDA_ENABLE_FLORENCE_BART_FFN",
                "ANTFLY_CUDA_ENABLE_FLORENCE_INCREMENTAL_ADMISSION",
                "ANTFLY_CUDA_ENABLE_FLORENCE_FUSED_KV_APPEND",
                "ANTFLY_INFERENCE_CUDA_DEFER_FREE",
                "ANTFLY_INFERENCE_CUDA_DEFER_FREE_BUDGET_MB",
                "ANTFLY_INFERENCE_CUDA_TEMP_CACHE_MB",
                "ANTFLY_INFERENCE_CUDA_TEMP_CACHE_MAX_BUFFERS",
                "ANTFLY_INFERENCE_CUDA_TEMP_STABLE_REUSE",
                "ANTFLY_CUDA_CUBLASLT_WORKSPACE_MB",
                "TERMITE_CUDA_CUBLASLT_WORKSPACE_MB",
                "LD_LIBRARY_PATH",
            )
        }
        if args.backend == "cuda"
        else None,
        "packed_mixed_requested": args.packed_mixed,
        "packed_shared_frame_requested": args.packed_shared_frame,
        "packed_prefill_items": args.packed_prefill_items,
        "prepared_prefill_chunk_rows": args.prepared_prefill_chunk_rows,
        "mode": mode,
        "concurrency": concurrency,
        "log": path.name,
        "exit_code": process.returncode,
        "status": "timeout" if timed_out else "failed",
    }
    if (
        process.returncode == 0
        and match
        and f"LIGHTONOCR3 backend={args.backend}" in text
    ):
        fields = [
            "concurrency",
            "requests",
            "pages_per_request",
            "completed",
            "elapsed_ms",
            "pages_per_second",
            "p50_ms",
            "p95_ms",
            "native_items",
        ]
        result.update(
            {
                key: float(value) if key == "pages_per_second" else int(value)
                for key, value in zip(fields, match.groups())
            }
        )
        result.update(status="ok", serial_reference_parity=True)
        packed_steps = re.findall(
            r"LIGHTONOCR3 packed_step_prefill_items=(\d+) packed_step_decode_items=(\d+) packed_step_query_rows=(\d+)",
            text,
        )
        result["packed_steps_including_warmup"] = len(packed_steps)
        result["mixed_steps_including_warmup"] = sum(
            int(prefill) > 0 and int(decode) > 0 for prefill, decode, _ in packed_steps
        )
        if args.require_packed_mixed and not result["mixed_steps_including_warmup"]:
            result["status"] = "missing_actual_packed_mixed_steps"

        shared_submits = [
            int(submits)
            for enabled, submits in re.findall(
                r"LIGHTONOCR3 packed_shared_frame=(\d+) packed_forward_submissions=(\d+)",
                text,
            )
            if int(enabled)
        ]
        result["packed_shared_frame_steps_including_warmup"] = len(shared_submits)
        result["packed_shared_frame_topology_mismatches_including_warmup"] = sum(
            submits != 1 for submits in shared_submits
        )
        if args.require_packed_shared_frame and (
            not shared_submits or any(submits != 1 for submits in shared_submits)
        ):
            result["status"] = "missing_single_frame_packed_forwards"

        if args.policy_rounds:
            samples = [
                {
                    key: float(value) if key == "pages_per_second" else int(value)
                    for key, value in zip(fields, item.groups())
                }
                for item in RESULT.finditer(text)
            ]
            labels = re.findall(r"OCR_POLICY round=(\d+) name=([a-z0-9]+)", text)
            if [name for _, name in labels] != args.policy_rounds or len(
                samples
            ) != len(args.policy_rounds):
                result["status"] = "incomplete_policy_rounds"
            else:
                result["policy_rounds"] = [
                    dict(round=i, policy=name, **sample)
                    for i, (name, sample) in enumerate(zip(args.policy_rounds, samples))
                ]
                pieces = re.split(r"OCR_POLICY round=\d+ name=[a-z0-9]+\n", text)
                sections = pieces[1:]
                snapshot_pattern = r"LIGHTONOCR3 frame_policy_snapshots=(\d+)"
                prior_snapshots = re.findall(snapshot_pattern, pieces[0])
                previous_snapshots = int(prior_snapshots[-1]) if prior_snapshots else 0
                for sample, section in zip(result["policy_rounds"], sections):
                    sample["vision_batch_sizes"] = sorted(
                        {
                            int(v)
                            for v in re.findall(
                                r"LIGHTONOCR3 (?:prepared_)?vision_batch_items=(\d+)",
                                section,
                            )
                        }
                    )
                    sample["prefill_batch_sizes"] = sorted(
                        {
                            int(v)
                            for v in re.findall(
                                r"LIGHTONOCR3 prefill_batch_items=(\d+)", section
                            )
                        }
                    )
                    if (
                        args.require_wide_vision
                        and max(sample["vision_batch_sizes"], default=0) <= 8
                    ):
                        result["status"] = "missing_timed_wide_vision"
                    if (
                        args.require_wide_prefill
                        and max(sample["prefill_batch_sizes"], default=0) <= 8
                    ):
                        result["status"] = "missing_timed_wide_prefill"
                    cohort_steps = re.findall(
                        r"LIGHTONOCR3 cohort_items=(\d+) decode_steps=(\d+) batched_steps=(\d+) singleton_steps=(\d+) decode_items=(\d+)",
                        section,
                    )
                    sample["cohort_decode_steps"] = sum(int(v[1]) for v in cohort_steps)
                    sample["cohort_decode_items"] = sum(int(v[4]) for v in cohort_steps)
                    for counter in [
                        "coalesce_turn_boundaries",
                        "coalesce_rebased_items",
                        "coalesce_waits",
                        "coalesce_wait_us",
                        "kv_block_skips",
                    ]:
                        sample[counter] = sum(
                            int(v) for v in re.findall(counter + r"=(\d+)", section)
                        )
                    if args.turn_coalescing and not sample["coalesce_turn_boundaries"]:
                        result["status"] = "missing_timed_turn_coalescing"

                    packed_steps = re.findall(
                        r"LIGHTONOCR3 packed_step_prefill_items=(\d+) packed_step_decode_items=(\d+) packed_step_query_rows=(\d+)",
                        section,
                    )
                    sample["packed_steps"] = len(packed_steps)
                    sample["packed_max_items"] = max(
                        (
                            int(prefill) + int(decode)
                            for prefill, decode, _ in packed_steps
                        ),
                        default=0,
                    )
                    sample["packed_max_prefill_items"] = max(
                        (int(prefill) for prefill, _, _ in packed_steps), default=0
                    )
                    sample["packed_max_query_rows"] = max(
                        (int(rows) for _, _, rows in packed_steps), default=0
                    )
                    if args.require_wide_packed_prefill and (
                        sample["packed_max_prefill_items"] <= 8
                        or sample["packed_max_query_rows"] > 4096
                    ):
                        result["status"] = "missing_timed_wide_packed_prefill"
                    if args.require_physical128 and sample["packed_max_items"] <= 64:
                        result["status"] = "missing_timed_physical128_steps"
                    sample["packed_input_frames"] = sum(
                        int(value)
                        for value in re.findall(
                            r"LIGHTONOCR3 packed_input_frames=(\d+)", section
                        )
                    )
                    for counter in [
                        "packed_gpu_decode_rows",
                        "packed_cpu_logit_rows",
                        "packed_standalone_wide_heads",
                    ]:
                        sample[counter] = sum(
                            int(value)
                            for value in re.findall(counter + r"=(\d+)", section)
                        )
                    wide_recurrent = [
                        tuple(map(int, values))
                        for values in re.findall(
                            r"LIGHTONOCR3 wide_recurrent_dispatches=(\d+) wide_recurrent_lifetime_max_rows=(\d+) wide_recurrent_staged_bytes=(\d+)",
                            section,
                        )
                    ]
                    sample["wide_recurrent_dispatches"] = sum(
                        calls for calls, _, _ in wide_recurrent
                    )
                    sample["wide_recurrent_lifetime_max_rows"] = max(
                        (rows for _, rows, _ in wide_recurrent), default=0
                    )
                    sample["wide_recurrent_staged_bytes"] = sum(
                        size for _, _, size in wide_recurrent
                    )
                    if args.require_wide_recurrent and (
                        not sample["wide_recurrent_dispatches"]
                        or not 16 < sample["wide_recurrent_lifetime_max_rows"] <= 64
                        or not sample["wide_recurrent_staged_bytes"]
                    ):
                        result["status"] = "missing_timed_wide_recurrent_steps"
                    short_prefill = re.findall(
                        r"LIGHTONOCR3 short_prefill_core_dispatches=(\d+) short_direct_attention_dispatches=(\d+)",
                        section,
                    )
                    sample["short_prefill_core_dispatches"] = sum(
                        int(core) for core, _ in short_prefill
                    )
                    sample["short_direct_attention_dispatches"] = sum(
                        int(attention) for _, attention in short_prefill
                    )
                    sample["packed_prefill_chunks"] = sum(
                        int(prefill) for prefill, _, _ in packed_steps
                    )
                    chunk_progress = [
                        tuple(map(int, values))
                        for values in re.findall(
                            r"LIGHTONOCR3 packed_step_prefill_starts=(\d+) packed_step_prefill_finishes=(\d+) prepared_prefill_chunk_rows=(\d+)",
                            section,
                        )
                    ]
                    sample["packed_prefill_pages"] = (
                        sum(finishes for _, finishes, _ in chunk_progress)
                        if chunk_progress
                        else sample["packed_prefill_chunks"]
                    )
                    sample["packed_prefill_starts"] = (
                        sum(starts for starts, _, _ in chunk_progress)
                        if chunk_progress
                        else sample["packed_prefill_chunks"]
                    )
                    sample["prepared_prefill_chunk_rows"] = (
                        args.prepared_prefill_chunk_rows
                    )
                    if chunk_progress and (
                        len(chunk_progress) != len(packed_steps)
                        or any(
                            rows != args.prepared_prefill_chunk_rows
                            for _, _, rows in chunk_progress
                        )
                    ):
                        result["status"] = (
                            "inconsistent_prepared_prefill_chunk_progress"
                        )
                    if args.require_prepared_prefill_chunks and (
                        not chunk_progress
                        or sample["packed_prefill_pages"] != args.requests
                        or sample["packed_prefill_starts"] != args.requests
                        or sample["packed_prefill_chunks"]
                        <= sample["packed_prefill_pages"]
                    ):
                        result["status"] = "missing_actual_prepared_prefill_chunks"
                    sample["packed_prefill_max_items"] = max(
                        (int(prefill) for prefill, _, _ in packed_steps), default=0
                    )
                    sample["mixed_steps"] = sum(
                        int(prefill) > 0 and int(decode) > 0
                        for prefill, decode, _ in packed_steps
                    )
                    sample["packed_decode_rows"] = sum(
                        int(decode) for _, decode, _ in packed_steps
                    )
                    sample["packed_query_rows"] = sum(
                        int(rows) for _, _, rows in packed_steps
                    )
                    recurrent_groups = re.findall(
                        r"LIGHTONOCR3 packed_recurrent_groups=(\d+) packed_recurrent_rows=(\d+)",
                        section,
                    )
                    sample["packed_recurrent_groups"] = sum(
                        int(groups) for groups, _ in recurrent_groups
                    )
                    sample["packed_recurrent_rows"] = sum(
                        int(rows) for _, rows in recurrent_groups
                    )
                    attention_groups = re.findall(
                        r"LIGHTONOCR3 packed_attention_groups=(\d+) packed_attention_rows=(\d+)",
                        section,
                    )
                    sample["packed_attention_groups"] = sum(
                        int(groups) for groups, _ in attention_groups
                    )
                    sample["packed_attention_rows"] = sum(
                        int(rows) for _, rows in attention_groups
                    )
                    shared_frames = [
                        (int(enabled), int(submits))
                        for enabled, submits in re.findall(
                            r"LIGHTONOCR3 packed_shared_frame=(\d+) packed_forward_submissions=(\d+)",
                            section,
                        )
                        if int(enabled)
                    ]
                    sample["packed_shared_frame_steps"] = len(shared_frames)
                    # This counter counts decoder-runtime frames. Prefix CPU
                    # readback may still require a separate staging blit.
                    sample["packed_shared_frame_topology_mismatches"] = sum(
                        submits != 1 for _, submits in shared_frames
                    )
                    if args.require_packed_shared_frame and (
                        not shared_frames
                        or sample["packed_shared_frame_topology_mismatches"]
                    ):
                        result["status"] = "missing_single_frame_packed_forwards"
                    if args.require_packed_mixed and (
                        not sample["mixed_steps"]
                        or not sample["packed_recurrent_groups"]
                        or not all(
                            2 * int(groups)
                            <= int(rows)
                            <= (
                                128
                                if args.backend == "cuda" and args.physical128
                                else 64
                                if args.backend == "cuda"
                                or args.wide_recurrent
                                or args.require_wide_recurrent
                                else 16
                            )
                            * int(groups)
                            for groups, rows in recurrent_groups
                        )
                    ):
                        result["status"] = "missing_batched_packed_recurrent_steps"
                    sample["decode_pack_calls"] = sum(
                        int(value)
                        for value in re.findall(
                            r"LIGHTONOCR3 decode_pack_calls=(\d+)", section
                        )
                    )
                    sample["retained_weight_handles_observed"] = (
                        "lightonocr3_weight_handles: provider_owned=1 temporary_host_mirrors=1"
                        in section
                    )
                    sample["execution_gate_wait_ns"] = sorted(
                        {
                            int(value)
                            for value in re.findall(
                                r"LIGHTONOCR3 execution_gate_wait_ns=(\d+)", section
                            )
                        }
                    )
                    # The C runtime can survive several reader epochs. Measure
                    # counter increments; an unchanged lifetime total is zero
                    # activity in an off round. A new runtime can reset it.
                    raw_snapshots = [
                        int(value) for value in re.findall(snapshot_pattern, section)
                    ]
                    sample["frame_policy_snapshot_lifetime_totals"] = raw_snapshots
                    sample["frame_policy_snapshots"] = 0
                    for value in raw_snapshots:
                        sample["frame_policy_snapshots"] += (
                            value - previous_snapshots
                            if value >= previous_snapshots
                            else value
                        )
                        previous_snapshots = value
                    frames = re.findall(
                        r"LIGHTONOCR3 runtime_frames=(\d+) frame_gpu_ns=(\d+) frame_wait_ns=(\d+)",
                        section,
                    )
                    if frames:
                        sample["framed_runtime_totals"] = {
                            key: sum(int(row[index]) for row in frames)
                            for index, key in enumerate(("frames", "gpu_ns", "wait_ns"))
                        }
                    sample.update(prefill_stream_observation(section))
                    for counter in (
                        "prepared_blocks",
                        "wide_projection_tiles",
                        "wide_mpp_projections",
                        "wide_mpp_heads",
                        "wide_gate_up_calls",
                        "full64_gate_up_calls",
                        "prefill_fused_ffn_calls",
                        "vision_workspace_allocations",
                        "vision_workspace_reuses",
                        "vision_workspace_bytes",
                        "large_vision_pack_calls",
                        "large_vision_pack_reuse_hits",
                        "batch_argmax_calls",
                        "batch_argmax_rows",
                        "gpu_greedy_stream_steps",
                        "gpu_greedy_stream_two_frame_steps",
                        "gpu_greedy_stream_multi_frame_steps",
                        "gpu_greedy_stream_frame_submits",
                        "gpu_greedy_stream_topology_mismatches",
                        "phase_priority_yields",
                        "phase_fair",
                        "executor_steps",
                        "executor_tail_steps",
                        "activation_pool_hits",
                        "activation_pool_peak_bytes",
                        "activation_pool_duplicate_retirements",
                        "prefill_activation_pool",
                        "prefill_activation_pool_hits",
                        "prefill_activation_pool_hit_bytes",
                        "phase_gate_wait_ns",
                    ):
                        values = [
                            int(value)
                            for value in re.findall(
                                (
                                    rf"(?m)^LIGHTONOCR3 [^\n]*\b{counter}=(\d+)"
                                    if counter.startswith("prefill_activation_pool")
                                    or counter == "phase_fair"
                                    else rf"LIGHTONOCR3 {counter}=(\d+)"
                                ),
                                section,
                            )
                        ]
                        sample[counter] = (
                            max(values, default=0)
                            if counter.endswith("_peak_bytes")
                            or counter == "phase_gate_wait_ns"
                            else sum(values)
                        )
                    if args.phase_fair and not sample["phase_fair"]:
                        result["status"] = "missing_timed_phase_fairness"
                    if args.phase_park and not sample["phase_gate_wait_ns"]:
                        result["status"] = "missing_timed_phase_parking"
                    if args.executor_tail and not sample["executor_tail_steps"]:
                        result["status"] = "missing_timed_executor_tail"
                    sample["packed_core_rows_observed"] = sorted(
                        {
                            int(row)
                            for row in re.findall(
                                r"lightonocr3_packed_core: rows=(\d+)", section
                            )
                        }
                    )
                    windows = re.findall(
                        r"LIGHTONOCR3 registered_window_peak=(\d+) physical_rows_limit=(\d+)",
                        section,
                    )
                    if windows:
                        sample["registered_window_peak"] = max(
                            int(peak) for peak, _ in windows
                        )
                        sample["physical_rows_limit"] = max(
                            int(rows) for _, rows in windows
                        )
                # Each round is a complete measurement; a single top-level
                # throughput would misrepresent this alternating comparison.
                for key in fields:
                    if key != "concurrency":
                        result.pop(key, None)
        if not args.policy_rounds:
            timed_sections = re.split(r"OCR_POLICY round=\d+ name=[a-z0-9]+\n", text)[
                1:
            ]
            result["prefill_fused_ffn_calls"] = sum(
                int(value)
                for section in timed_sections
                for value in re.findall(
                    r"LIGHTONOCR3 prefill_fused_ffn_calls=(\d+)", section
                )
            )
        if args.prefill_ffn_rounds:
            observed = [
                bool(int(enabled))
                for _, enabled in re.findall(
                    r"OCR_PREFILL_FFN_POLICY round=(\d+) enabled=([01])", text
                )
            ]
            expected = [value == "on" for value in args.prefill_ffn_rounds]
            if observed != expected:
                result["status"] = "missing_prefill_ffn_rounds"
            for sample, enabled in zip(result.get("policy_rounds", []), expected):
                sample["prefill_fused_ffn_enabled"] = enabled
        if args.bf16_projection_rounds:
            observed = [
                (int(index), bool(int(enabled)))
                for index, enabled in re.findall(
                    r"OCR_BF16_POLICY round=(\d+) enabled=([01])", text
                )
            ]
            expected = [
                (index, value == "on")
                for index, value in enumerate(args.bf16_projection_rounds)
            ]
            samples = result.get("policy_rounds", [])
            if observed != expected or len(samples) != len(expected):
                result["status"] = "missing_bf16_projection_rounds"
            for sample, (index, enabled) in zip(samples, expected):
                sample["bf16_projection_enabled"] = enabled
                if sample.get("round") != index or (
                    (sample.get("decode_pack_calls", 0) > 0) != enabled
                ):
                    result["status"] = "missing_timed_bf16_projection_policy"
        memory_samples = {}
        for index, phase, tensor_live, owned_live, alias_live in re.findall(
            r"OCR_MEMORY round=(\d+) phase=(before|after) tensor_live_bytes=(\d+) owned_buffer_live_bytes=(\d+) alias_buffer_live_bytes=(\d+)",
            text,
        ):
            memory_samples.setdefault(int(index), {})[phase] = dict(
                tensor_live_bytes=int(tensor_live),
                owned_buffer_live_bytes=int(owned_live),
                alias_buffer_live_bytes=int(alias_live),
            )
        for sample in result.get("policy_rounds", []):
            sample["buffer_lifetime_gauges"] = memory_samples.get(sample["round"], {})
        if args.require_memory_rounds and (
            not result.get("policy_rounds")
            or any(
                set(sample.get("buffer_lifetime_gauges", {})) != {"before", "after"}
                for sample in result["policy_rounds"]
            )
        ):
            result["status"] = "missing_round_buffer_lifetime_gauges"
        if args.require_prefill_activation_pool:
            samples = result.get("policy_rounds", [])
            if not samples or any(
                sample.get("prefill_activation_pool") != 1
                or sample.get("prefill_activation_pool_hits", 0) == 0
                or sample.get("prefill_activation_pool_hit_bytes", 0) == 0
                or not 8 * 1024**2
                < sample.get("activation_pool_peak_bytes", 0)
                <= 64 * 1024**2
                for sample in samples
            ):
                result["status"] = "missing_bounded_prefill_activation_reuse"
        if args.require_prefill_fused_ffn:
            timed_samples = result.get("policy_rounds", [result])
            if not timed_samples or any(
                (sample.get("prefill_fused_ffn_calls", 0) > 0)
                != sample.get("prefill_fused_ffn_enabled", args.prefill_fused_ffn)
                for sample in timed_samples
            ):
                result["status"] = "missing_timed_prefill_fused_ffn"
        cpu_samples = [
            int(value)
            for _, value in re.findall(
                r"OCR_CPU round=(\d+) process_cpu_ms=(\d+)", text
            )
        ]
        if (
            args.policy_rounds
            and "policy_rounds" in result
            and len(cpu_samples) == len(result["policy_rounds"])
        ):
            for sample, cpu_ms in zip(result["policy_rounds"], cpu_samples):
                sample["process_cpu_ms"] = cpu_ms
        elif not args.policy_rounds and len(cpu_samples) == 1:
            result["process_cpu_ms"] = cpu_samples[0]
        if args.require_prefill_core_norm:
            observed = bool(
                re.search(r"lightonocr3_prefill_core_norm: rows=\d+ heads=\d+", text)
            )
            result["prefill_core_norm_observed_including_warmup"] = observed
            if not observed:
                result["status"] = "missing_prefill_core_norm"
        if args.require_prefill_attention_mpp:
            dispatches = re.findall(
                r"lightonocr3_prefill_attention_mpp: queries=\d+ keys=\d+ heads=\d+",
                text,
            )
            result["prefill_attention_mpp_observed_including_warmup"] = bool(dispatches)
            if not dispatches:
                result["status"] = "missing_prefill_attention_mpp"
        if args.require_frame_policy:
            rounds = result.get("policy_rounds", [])
            if not rounds:
                result["status"] = "frame_policy_requires_rounds"
            for sample in rounds:
                enabled = sample["policy"] == "flags" or (
                    (sample["policy"] == "current" and args.frame_policy)
                    or sample["policy"].startswith("engine")
                )
                if enabled != (sample["frame_policy_snapshots"] > 0):
                    result["status"] = "frame_policy_control_mismatch"
        if args.require_execution_gate_wait:
            rounds = result.get("policy_rounds", [])
            if not rounds:
                result["status"] = "gate_wait_requires_policy_rounds"
            for sample in rounds:
                expected = (
                    0
                    if sample["policy"] in ("gateoff", "pipelineoff", "spin")
                    else min(
                        args.contention_wait_us
                        if args.contention_wait_us is not None
                        else 100,
                        1000,
                    )
                    * 1000
                )
                if sample["execution_gate_wait_ns"] != [expected]:
                    result["status"] = "gate_wait_control_mismatch"
        for required, label, counter in (
            (
                args.require_florence_grouped_channel,
                "florence_grouped_channel",
                "cuda_grouped_channel_calls",
            ),
            (
                args.require_florence_channel_plan_cache,
                "florence_channel_plan_cache",
                "cuda_channel_plan_cache_hits",
            ),
            (
                args.require_florence_vision_microbatch,
                "florence_vision_microbatch",
                "cuda_vision_microbatch_chunks",
            ),
            (
                args.require_lighton_fused_projections,
                "lighton_fused_projections",
                "cuda_fused_projection_calls",
            ),
            (
                args.require_florence_fused_attention_inputs,
                "florence_fused_attention_inputs",
                "cuda_fused_attention_input_calls",
            ),
            (
                args.require_florence_fused_attention_inputs,
                "florence_fused_qkv_split",
                "cuda_fused_qkv_split_calls",
            ),
            (
                args.require_lighton_attention_chunk128,
                "lighton_attention_chunk128",
                "cuda_attention_chunk128_calls",
            ),
            (
                args.require_florence_fused_cross_attention,
                "florence_fused_cross",
                "cuda_fused_cross_calls",
            ),
            (
                args.require_florence_encoder_microbatch,
                "florence_encoder_microbatch",
                "cuda_encoder_microbatch_chunks",
            ),
            (
                args.require_florence_fused_bias,
                "florence_fused_bias",
                "cuda_fused_bias_calls",
            ),
            (
                args.require_florence_fused_depthwise,
                "florence_fused_depthwise",
                "cuda_fused_depthwise_calls",
            ),
            (
                args.require_florence_fused_mlp,
                "florence_fused_mlp",
                "cuda_fused_mlp_calls",
            ),
            (
                args.require_florence_channel_tc,
                "florence_channel_tc",
                "cuda_channel_tc_calls",
            ),
            (
                args.require_florence_cudnn_attention,
                "florence_cudnn_attention",
                "cuda_attention_cudnn_calls",
            ),
            (
                args.require_florence_cudnn_conv,
                "florence_cudnn_conv",
                "cuda_conv_cudnn_calls",
            ),
            (
                args.require_florence_batched_vision_tail,
                "florence_batched_vision_tail",
                "cuda_batched_vision_tail_calls",
            ),
        ):
            counts = [
                int(value)
                for value in re.findall(
                    rf"(?:FLORENCE2|LIGHTONOCR3) {counter}=(\d+)", text
                )
            ]
            result[label + "_calls"] = sum(counts)
            if required and not sum(counts):
                result["status"] = "missing_" + label
        cache_samples = re.findall(
            r"OCR_CUDA temp_hits=(\d+) temp_misses=(\d+) temp_evictions=(\d+) temp_cached_bytes=(\d+) temp_cached_buffers=(\d+) forced_drains=(\d+) physical_live_bytes=(\d+) physical_peak_bytes=(\d+) physical_limit_bytes=(\d+)",
            text,
        )
        result["cuda_temp_cache"] = [
            dict(
                zip(
                    (
                        "hits",
                        "misses",
                        "evictions",
                        "cached_bytes",
                        "cached_buffers",
                        "forced_drains",
                        "physical_live_bytes",
                        "physical_peak_bytes",
                        "physical_limit_bytes",
                    ),
                    map(int, row),
                )
            )
            for row in cache_samples
        ]
        result["cuda_retained_cache"] = [
            dict(
                zip(
                    (
                        "cache_budget_bytes",
                        "deferred_budget_bytes",
                        "retained_budget_bytes",
                    ),
                    map(int, row),
                )
            )
            for row in re.findall(
                r"OCR_CUDA cache_budget_bytes=(\d+) deferred_budget_bytes=(\d+) retained_budget_bytes=(\d+)",
                text,
            )
        ]
        if args.require_cuda_temp_cache and not result["cuda_temp_cache"]:
            result["status"] = "missing_cuda_temp_cache_stats"
        if any(
            row["physical_peak_bytes"] > row["physical_limit_bytes"]
            for row in result["cuda_temp_cache"]
        ):
            result["status"] = "cuda_physical_limit_exceeded"
        for counter, label, required in (
            (
                "gpu_token_burst_steps",
                "cuda_token_burst",
                args.require_cuda_token_burst,
            ),
            (
                "cuda_bf16_recurrent_prefill_calls",
                "lighton_bf16_recurrent_prefill",
                args.require_lighton_bf16_recurrent_prefill,
            ),
            (
                "cuda_bf16_prefill_ffn_calls",
                "lighton_bf16_prefill_ffn",
                args.require_lighton_bf16_prefill_ffn,
            ),
            (
                "cuda_fused_gdn_decode_calls",
                "lighton_fused_gdn_decode",
                args.require_lighton_fused_gdn_decode,
            ),
            (
                "cuda_ffn_block_calls",
                "lighton_ffn_block",
                args.require_lighton_ffn_block,
            ),
            (
                "cuda_ffn_bf16_input_calls",
                "lighton_ffn_bf16_inputs",
                args.require_lighton_ffn_bf16_inputs,
            ),
            (
                "cuda_ragged_batch_calls",
                "lighton_ragged_recurrent",
                args.require_lighton_ragged_recurrent,
            ),
            (
                "cuda_fused_vision_qkv_calls",
                "lighton_fused_vision_qkv",
                args.require_lighton_fused_vision_qkv,
            ),
            (
                "cuda_prefix_shared_qk_calls",
                "lighton_prefix_shared_qk",
                args.require_lighton_prefix_shared_qk,
            ),
            (
                "cuda_state_pool_hits",
                "lighton_state_pool",
                args.require_lighton_state_pool,
            ),
            (
                "cuda_shared_recurrent_weight_borrows",
                "lighton_shared_recurrent_weights",
                args.require_lighton_shared_recurrent_weights,
            ),
            (
                "cuda_incremental_admission_items",
                "florence_incremental_admission",
                args.require_florence_incremental_admission,
            ),
            (
                "cuda_norm_weight_cache_hits",
                "lighton_norm_weight_cache",
                args.require_lighton_norm_weight_cache,
            ),
            (
                "cuda_fused_vision_mlp_calls",
                "lighton_fused_vision_mlp",
                args.require_lighton_fused_vision_mlp,
            ),
            (
                "cuda_bart_ffn_calls",
                "florence_bart_ffn",
                args.require_florence_bart_ffn,
            ),
            (
                "cuda_prefix_batch_calls",
                "lighton_prefix_batch",
                args.require_lighton_prefix_batch,
            ),
            (
                "cuda_fused_kv_append_calls",
                "florence_fused_kv_append",
                args.require_florence_fused_kv_append,
            ),
        ):
            count = sum(
                int(n)
                for n in re.findall(
                    rf"^(?:LIGHTONOCR3|FLORENCE2) [^\n]*\b{counter}=(\d+)\b",
                    text,
                    re.MULTILINE,
                )
            )
            result[label + "_calls"] = count
            if required and not count:
                result["status"] = "missing_" + label
        kv_layers = [
            tuple(map(int, q))
            for q in re.findall(
                r"LIGHTONOCR3 kv_storage_layers=(\d+) model_layers=(\d+)", text
            )
        ]
        result["kv_storage_layers"] = [
            dict(storage=storage, model=model) for storage, model in kv_layers
        ]
        if args.require_compact_kv and (
            not kv_layers
            or any(not (0 < storage < model) for storage, model in kv_layers)
        ):
            result["status"] = "missing_compact_kv_storage"
        canonical = [
            tuple(map(int, q))
            for q in re.findall(
                r"FLORENCE2 cuda_canonical_f16_calls=(\d+) plans=(\d+) declines=(\d+)",
                text,
            )
        ]
        result["florence_canonical_gemm"] = [
            dict(calls=calls, plans=plans, declines=declines)
            for calls, plans, declines in canonical
        ]
        if args.require_florence_canonical_gemm and (
            not canonical
            or not sum(q[0] for q in canonical)
            or any(q[2] for q in canonical)
        ):
            result["status"] = "missing_florence_canonical_gemm"
        if args.require_physical128:
            # These groups count model callbacks, each of which may partition
            # into two already-qualified 64-page CUDA packets. Verify the
            # actual backend packet count as well as the wider dense step.
            claims = re.findall(
                r"LIGHTONOCR3 packed_step_prefill_items=(\d+) packed_step_decode_items=(\d+) packed_step_query_rows=(\d+)",
                text,
            )
            callbacks = re.findall(
                r"LIGHTONOCR3 packed_recurrent_groups=(\d+) packed_recurrent_rows=(\d+)",
                text,
            )
            shape_matches = len(claims) == len(callbacks) and all(
                int(rows) == (int(prefill) + int(decode)) * int(groups)
                for (prefill, decode, _), (groups, rows) in zip(claims, callbacks)
            )
            expected_packets = sum(
                int(groups) * ((int(prefill) + int(decode) + 63) // 64)
                for (prefill, decode, _), (groups, _) in zip(claims, callbacks)
            )
            actual_packets = result["lighton_ragged_recurrent_calls"]
            result["cuda_ragged_expected_packets"] = expected_packets
            result["cuda_ragged_observed_packets"] = actual_packets
            if (
                not shape_matches
                or not expected_packets
                or actual_packets != expected_packets
            ):
                result["status"] = "wide_recurrent_packet_count_mismatch"
        result["lighton_parallel_prefix_calls"] = sum(
            int(n)
            for n in re.findall(r"LIGHTONOCR3 cuda_parallel_prefix_calls=(\d+)", text)
        )
        if (
            args.require_lighton_parallel_prefix
            and not result["lighton_parallel_prefix_calls"]
        ):
            result["status"] = "missing_lighton_parallel_prefix"
        cached_gates = re.findall(
            r"LIGHTONOCR3 cuda_cached_gate_single_calls=(\d+) cuda_cached_gate_batch_calls=(\d+)",
            text,
        )
        result["lighton_cached_gate_single_calls"] = sum(
            int(n) for n, _ in cached_gates
        )
        result["lighton_cached_gate_batch_calls"] = sum(int(n) for _, n in cached_gates)
        if args.require_lighton_cached_gates and (
            not result["lighton_cached_gate_single_calls"]
            or not result["lighton_cached_gate_batch_calls"]
        ):
            result["status"] = "missing_lighton_cached_gates"
        pinned_inputs = re.findall(
            r"OCR_CUDA pinned_input_uploads=(\d+) pinned_input_wraps=(\d+)", text
        )
        result["ocr_pinned_input_uploads"] = sum(int(u) for u, _ in pinned_inputs)
        result["ocr_pinned_input_wraps"] = sum(int(w) for _, w in pinned_inputs)
        if args.require_ocr_pinned_inputs and not result["ocr_pinned_input_uploads"]:
            result["status"] = "missing_ocr_pinned_inputs"
        metadata = re.findall(
            r"OCR_CUDA pinned_metadata_uploads=(\d+) mrope_cache_hits=(\d+) mrope_cache_uploads=(\d+)",
            text,
        )
        result["ocr_pinned_metadata_uploads"] = sum(int(p) for p, _, _ in metadata)
        result["lighton_position_cache_hits"] = sum(int(h) for _, h, _ in metadata)
        result["lighton_position_cache_uploads"] = sum(int(u) for _, _, u in metadata)
        if (
            args.require_ocr_pinned_metadata
            and not result["ocr_pinned_metadata_uploads"]
        ):
            result["status"] = "missing_ocr_pinned_metadata"
        if (
            args.require_lighton_position_cache
            and not result["lighton_position_cache_hits"]
        ):
            result["status"] = "missing_lighton_position_cache"
        fused_ffn = re.findall(
            r"LIGHTONOCR3 cuda_fused_ffn_calls=(\d+) paired_weights=(\d+)", text
        )
        result["lighton_fused_ffn_calls"] = sum(int(calls) for calls, _ in fused_ffn)
        result["lighton_paired_weight_counts"] = [int(n) for _, n in fused_ffn]
        if args.require_lighton_fused_ffn and not result["lighton_fused_ffn_calls"]:
            result["status"] = "missing_lighton_fused_ffn"
        if args.require_florence_no_repeat_argmax:
            timed = "".join(
                re.split(r"OCR_POLICY round=\d+ name=[a-z0-9]+\n", text)[1:]
            )
            traces = re.findall(
                r"FLORENCE2 no_repeat_argmax_enabled=(\d+) device_accepts=(\d+) cpu_fallbacks=(\d+)",
                timed,
            )
            stats = {
                "enabled": sorted({int(row[0]) for row in traces}),
                "device_accepts": sum(int(row[1]) for row in traces),
                "cpu_fallbacks": sum(int(row[2]) for row in traces),
            }
            result["florence_no_repeat_argmax"] = stats
            enabled = not args.disable_florence_no_repeat_argmax
            if (
                stats["enabled"] != [int(enabled)]
                or (enabled and not stats["device_accepts"])
                or (
                    not enabled
                    and (stats["device_accepts"] or not stats["cpu_fallbacks"])
                )
            ):
                result["status"] = "florence_no_repeat_argmax_control_mismatch"
        if args.require_florence_single_frame:
            timed = "".join(
                re.split(r"OCR_POLICY round=\d+ name=[a-z0-9]+\n", text)[1:]
            )
            traces = re.findall(
                r"FLORENCE2 single_frame_enabled=(\d+) single_frame_steps=(\d+) frame_api_fallbacks=(\d+) frame_topology_mismatches=(\d+)",
                timed,
            )
            stats = {
                "enabled": sorted({int(x[0]) for x in traces}),
                "steps": sum(int(x[1]) for x in traces),
                "api_fallbacks": sum(int(x[2]) for x in traces),
                "topology_mismatches": sum(int(x[3]) for x in traces),
            }
            result["florence_single_frame"] = stats
            enabled = not args.disable_florence_single_frame
            if (
                stats["enabled"] != [int(enabled)]
                or (enabled and not stats["steps"])
                or stats["api_fallbacks"]
                or stats["topology_mismatches"]
                or (not enabled and stats["steps"])
            ):
                result["status"] = "florence_single_frame_control_mismatch"
        if args.require_weight_handles:
            result["retained_weight_handles_observed_including_warmup"] = (
                "lightonocr3_weight_handles: provider_owned=1 temporary_host_mirrors=1"
                in text
            )
            if not result["retained_weight_handles_observed_including_warmup"]:
                result["status"] = "missing_weight_handles"
            for sample in result.get("policy_rounds", []):
                enabled = sample["policy"] not in (
                    "pipelineoff",
                    "handlesoff",
                    "default",
                    "gateoff",
                    "flags",
                )
                if enabled != sample["retained_weight_handles_observed"]:
                    result["status"] = "weight_handle_control_mismatch"
        if args.require_packed_core:
            rows = sorted(
                {
                    int(row)
                    for row in re.findall(r"lightonocr3_packed_core: rows=(\d+)", text)
                }
            )
            result["packed_core_rows_observed_including_warmup"] = rows
            if not rows:
                result["status"] = "missing_packed_core"
        if args.require_recurrent_parallel:
            dispatches = re.findall(
                r"lightonocr3_recurrent_parallel: rows=\d+ heads=\d+ threads=512", text
            )
            result["recurrent_parallel_observed_including_warmup"] = bool(dispatches)
            result["recurrent_parallel_rows_observed_including_warmup"] = sorted(
                {
                    int(value)
                    for value in re.findall(
                        r"lightonocr3_recurrent_parallel: rows=(\d+)", text
                    )
                }
            )
            if not dispatches:
                result["status"] = "missing_recurrent_parallel"
        if args.require_small_tail_fusion:
            dispatches = re.findall(
                r"site=bf16_gate_up_device[^\n]*rows=([234])[^\n]*variant=lightonocr3_gate_up_(?:split|local_vector|local_tg)(?:\s|$)",
                text,
            )
            result["small_tail_fusion_dispatches_including_warmup"] = len(dispatches)
            if not dispatches:
                result["status"] = "missing_small_tail_fusion"

        if args.require_vision_mpp:
            dispatches = re.findall(
                r"metal_vision_attention_trace: [^\n]*variant=lightonocr3_vision_mpp",
                text,
            )
            result["vision_mpp_dispatches_including_warmup"] = len(dispatches)
            if not dispatches:
                result["status"] = "missing_vision_mpp"
        if args.require_decode_projections:
            for variant_name in ["recurrent_gate_tg", "decode_projection_chunk"]:
                dispatches = re.findall(
                    r"metal_dense_linear_trace: [^\n]*variant=lightonocr3_"
                    + variant_name,
                    text,
                )
                result[variant_name + "_dispatches_including_warmup"] = len(dispatches)
                if not dispatches:
                    result["status"] = "missing_" + variant_name
        if args.require_prefill_gates:
            dispatches = re.findall(
                r"site=linear_multi_row_device[^\n]*variant=lightonocr3_prefill_gate_tg",
                text,
            )
            result["prefill_gate_dispatches_including_warmup"] = len(dispatches)
            if not dispatches:
                result["status"] = "missing_prefill_gates"

        result["references"] = [
            json.loads(row) for row in re.findall(r"OCR_REFERENCE (.+)", text)
        ]
        if args.reference_report:
            reference = json.loads(args.reference_report.read_text())
            expected = (
                reference.get("references") or reference["results"][0]["references"]
            )
            actual = result["references"]

            def content(value):
                return "".join(plain_text(value).split())

            matches = len(expected) == len(actual) and all(
                content(got["text"]) == content(want["text"])
                for got, want in zip(actual, expected)
            )
            result["cross_runtime_content_parity"] = matches
            result["reference_report_sha256"] = digest(args.reference_report)
            if not matches:
                result["status"] = "reference_mismatch"

        result["registered_windows_including_untimed_warmup"] = [
            {"registered_window_peak": int(peak), "physical_rows_limit": int(rows)}
            for peak, rows in re.findall(
                r"LIGHTONOCR3 registered_window_peak=(\d+) physical_rows_limit=(\d+)",
                text,
            )
        ]
        result["cohorts_including_untimed_warmup"] = [
            dict(
                zip(
                    (
                        "items",
                        "decode_steps",
                        "batched_steps",
                        "singleton_steps",
                        "decode_items",
                    ),
                    map(int, row),
                )
            )
            for row in re.findall(
                r"LIGHTONOCR3 cohort_items=(\d+) decode_steps=(\d+) batched_steps=(\d+) singleton_steps=(\d+) decode_items=(\d+)",
                text,
            )
        ]
        if args.require_extended_epoch:
            timed = "".join(
                re.split(r"OCR_POLICY round=\d+ name=[a-z0-9]+\n", text)[1:]
            )
            sizes = [
                int(n) for n in re.findall(r"LIGHTONOCR3 cohort_items=(\d+)", timed)
            ]
            result["extended_epoch_timed_cohort_sizes"] = sizes
            if not result["registered_windows_including_untimed_warmup"]:
                result["status"] = "missing_extended_epoch_window_telemetry"
            if not any(n > max(64, args.batch_window) for n in sizes):
                result["status"] = "missing_timed_extended_epoch"
            if any(n > 256 for n in sizes) or any(
                row["registered_window_peak"]
                > (args.batch_window if args.physical128 else 64)
                or row["physical_rows_limit"]
                > (args.batch_rows if args.physical128 else 64)
                for row in result["registered_windows_including_untimed_warmup"]
            ):
                result["status"] = "extended_epoch_window_limit_exceeded"
    elif "ResourceLimitExceeded" in text or "MemoryAdmissionDenied" in text:
        result["status"] = "resource_limit"
    if (
        result["status"] == "ok"
        and args.require_packed_shared_frame
        and (
            not result.get("packed_shared_frame_steps_including_warmup")
            or result.get("packed_shared_frame_topology_mismatches_including_warmup")
        )
    ):
        result["status"] = "missing_single_frame_packed_forwards"
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--native-test", type=Path, required=True)
    parser.add_argument("--backend", choices=["metal", "cuda"], default="metal")
    parser.add_argument("--cuda-recurrent-core", action="store_true")
    parser.add_argument("--cuda-batch", action="store_true")
    parser.add_argument(
        "--model", action="append", required=True, help="LABEL=CHECKPOINT_DIRECTORY"
    )
    parser.add_argument("--images", type=Path, nargs="+")
    parser.add_argument("--concurrency", type=int, nargs="+", default=[1, 2, 4, 8])
    parser.add_argument(
        "--modes", nargs="+", choices=["serial", "batch"], default=["serial", "batch"]
    )
    parser.add_argument("--requests", type=int, default=16)
    parser.add_argument("--pages-per-request", type=int, default=1)
    parser.add_argument("--max-tokens", type=int, default=256)
    parser.add_argument("--admission-units", type=int, default=256)
    parser.add_argument("--batch-rows", type=int, default=8)
    parser.add_argument("--physical32", action="store_true")
    parser.add_argument("--physical64", action="store_true")
    parser.add_argument("--physical128", action="store_true")
    parser.add_argument("--require-physical128", action="store_true")
    parser.add_argument("--compact-kv", action="store_true")
    parser.add_argument("--require-compact-kv", action="store_true")
    parser.add_argument("--require-florence-canonical-gemm", action="store_true")
    parser.add_argument("--logical64", action="store_true")
    parser.add_argument("--device-recurrent-state", action="store_true")
    parser.add_argument("--share-decoder-scratch", action="store_true")
    parser.add_argument("--ocr-encoder-scopes", action="store_true")
    parser.add_argument("--batch-window", type=int, default=8)
    parser.add_argument("--batch-wait-us", type=int, default=5000)
    parser.add_argument("--florence-batch-wait-us", type=int, default=5000)
    parser.add_argument("--profile-reader", action="store_true")
    parser.add_argument(
        "--require-small-tail-fusion",
        action="store_true",
        help="Require an actual two-/three-/four-row fused FFN dispatch (includes warmup)",
    )
    parser.add_argument("--prefill-sg-attention", action="store_true")
    parser.add_argument("--bf16-prefill-inputs", action="store_true")
    parser.add_argument("--prefill-fused-ffn", action="store_true")
    parser.add_argument("--require-prefill-fused-ffn", action="store_true")
    parser.add_argument(
        "--prefill-ffn-rounds",
        nargs="+",
        choices=("off", "on"),
        help="Override the FFN flag per policy round within one resident model",
    )
    parser.add_argument("--prefill-direct-kv", action="store_true")
    parser.add_argument("--decode-wait-us", type=int, default=1000)
    parser.add_argument(
        "--deferred-attention", choices=["auto", "on", "off"], default="auto"
    )
    parser.add_argument(
        "--disable-batch-projections",
        action="store_true",
        help="Ablate the specialized small-batch Metal kernel",
    )
    parser.add_argument("--disable-tiled-projections", action="store_true")
    parser.add_argument(
        "--policy-rounds",
        nargs="+",
        choices=[
            "off",
            "on",
            "recurrent",
            "attention",
            "norm",
            "coalesced",
            "static",
            "batched",
            "window8",
            "spin",
            "copyoff",
            "pipelineoff",
            "packedoff",
            "readyoff",
            "visionoff",
            "ready",
            "handlesoff",
            "default",
            "gateoff",
            "flags",
            "direct",
            "copykernel",
            "current",
            "enginebase",
            "engineplan",
            "engineworker",
            "engineall",
            "enginepool",
            "enginecached",
            "enginephase",
            "enginepoolphase",
            "enginefull",
            "enginetail",
            "enginetailfair",
            "enginefullfair",
            "engineepoch",
            "enginetailepoch",
        ],
    )
    parser.add_argument("--prefill-core-norm", action="store_true", default=None)
    parser.add_argument("--disable-prefill-core-norm", action="store_true")
    parser.add_argument("--require-prefill-core-norm", action="store_true")
    parser.add_argument("--disable-direct-core-output", action="store_true")
    parser.add_argument("--disable-compute-copies", action="store_true")
    parser.add_argument("--trace-compute-copies", action="store_true")
    parser.add_argument("--disable-packed-core", action="store_true")
    parser.add_argument("--trace-packed-core", action="store_true")
    parser.add_argument("--require-packed-core", action="store_true")
    parser.add_argument("--require-weight-handles", action="store_true")
    parser.add_argument("--weight-handles", action="store_true")
    parser.add_argument("--require-execution-gate-wait", action="store_true")
    parser.add_argument("--frame-policy", action="store_true")
    parser.add_argument("--require-frame-policy", action="store_true")
    parser.add_argument("--disable-prefill-direct-kv-mpp", action="store_true")
    parser.add_argument("--static-work-assignment", action="store_true")
    parser.add_argument(
        "--contention-wait-us", type=int, choices=range(1001), metavar="0..1000"
    )
    parser.add_argument("--prefill-attention-mpp", action="store_true", default=None)
    parser.add_argument("--disable-prefill-attention-mpp", action="store_true")
    parser.add_argument("--require-prefill-attention-mpp", action="store_true")
    parser.add_argument("--wide-recurrent", action="store_true")
    parser.add_argument("--prefill-activation-pool", action="store_true")
    parser.add_argument("--require-prefill-activation-pool", action="store_true")
    parser.add_argument("--require-wide-recurrent", action="store_true")
    parser.add_argument("--recurrent-parallel", action="store_true", default=None)
    parser.add_argument("--disable-recurrent-parallel", action="store_true")
    parser.add_argument("--recurrent-coalescing", action="store_true")
    parser.add_argument("--require-recurrent-parallel", action="store_true")
    parser.add_argument("--disable-recurrent-batch", action="store_true")
    parser.add_argument("--disable-recurrent-fusion", action="store_true")
    parser.add_argument("--disable-ffn-small-tile", action="store_true")
    parser.add_argument("--disable-qkv-small-tile", action="store_true")
    parser.add_argument("--disable-down-split", action="store_true")
    parser.add_argument("--disable-four-row-tile", action="store_true")
    parser.add_argument("--disable-f16-encoder-mpp", action="store_true")
    parser.add_argument("--weight-residency", action="store_true")
    parser.add_argument("--disable-local-kernels", action="store_true")
    parser.add_argument("--disable-vision-mpp", action="store_true")
    parser.add_argument("--disable-prefill-projections", action="store_true")
    parser.add_argument("--require-vision-mpp", action="store_true")
    parser.add_argument("--require-prefill-gates", action="store_true")
    parser.add_argument("--require-decode-projections", action="store_true")
    parser.add_argument("--disable-prefill-conv-vec4", action="store_true")
    parser.add_argument(
        "--continuous",
        action="store_true",
        help="Use refill; otherwise force fixed waves as the benchmark control",
    )
    parser.add_argument("--wait-for-prefill-peers", action="store_true")
    parser.add_argument("--cuda-wide-microbatches", action="store_true")
    parser.add_argument("--require-wide-vision", action="store_true")
    parser.add_argument("--require-wide-prefill", action="store_true")
    parser.add_argument("--phase-fair", action="store_true")
    parser.add_argument("--phase-park", action="store_true")
    parser.add_argument("--executor-tail", action="store_true")
    parser.add_argument("--turn-coalescing", action="store_true")
    parser.add_argument("--queued-phase-priority", action="store_true")
    parser.add_argument(
        "--prefill-decode-streak", type=int, choices=range(17), default=4
    )
    parser.add_argument("--vision-batch", action="store_true")
    parser.add_argument("--vision-batch-items", type=int, default=4)
    parser.add_argument("--disable-shared-vision-peak", action="store_true")
    parser.add_argument("--prefill-batch", action="store_true")
    parser.add_argument(
        "--prepared-prefill-chunk-rows",
        type=int,
        default=0,
        help="Split scheduled visual prefixes into bounded query chunks; 0 retains whole-prefix prefill",
    )
    parser.add_argument("--require-prepared-prefill-chunks", action="store_true")
    parser.add_argument(
        "--packed-mixed",
        action="store_true",
        help="Opt in to one packed Qwen prefill plus queued decode rows",
    )
    parser.add_argument(
        "--require-packed-mixed",
        action="store_true",
        help="Require observed steps containing both prefill and decode work",
    )
    parser.add_argument(
        "--packed-shared-frame",
        action="store_true",
        help="Borrow one decoder frame across packed input, model and F32 output head",
    )
    parser.add_argument("--require-packed-shared-frame", action="store_true")
    parser.add_argument("--prefill-batch-items", type=int, default=4)
    parser.add_argument("--extended-epoch", action="store_true")
    parser.add_argument("--require-extended-epoch", action="store_true")
    parser.add_argument(
        "--packed-prefill-items",
        type=int,
        default=1,
        help="Maximum independent prefills per packed step (1..8)",
    )
    parser.add_argument("--vision-wait-us", type=int, default=2000)
    parser.add_argument("--prefill-wait-us", type=int, default=5000)
    parser.add_argument("--disable-tail-split", action="store_true")
    parser.add_argument("--disable-partial-qkv-split", action="store_true")
    parser.add_argument("--disable-partial-down-split", action="store_true")
    parser.add_argument("--disable-gate-up-split", action="store_true")
    parser.add_argument("--disable-batch-gate-up", action="store_true")
    parser.add_argument("--disable-prefill-columns", action="store_true")
    parser.add_argument("--disable-single-down-tile", action="store_true")
    parser.add_argument("--disable-four-row-split", action="store_true")
    parser.add_argument("--disable-prefill-norm", action="store_true")
    parser.add_argument("--disable-ocr-attention", action="store_true")
    parser.add_argument("--disable-florence-window-softmax", action="store_true")
    parser.add_argument("--disable-florence-no-repeat-argmax", action="store_true")
    parser.add_argument("--require-florence-no-repeat-argmax", action="store_true")
    parser.add_argument("--require-florence-cudnn-attention", action="store_true")
    parser.add_argument("--require-florence-cudnn-conv", action="store_true")
    parser.add_argument("--require-florence-grouped-channel", action="store_true")
    parser.add_argument("--require-florence-channel-plan-cache", action="store_true")
    parser.add_argument("--require-florence-channel-tc", action="store_true")
    parser.add_argument(
        "--require-florence-fused-attention-inputs", action="store_true"
    )
    parser.add_argument("--require-florence-fused-bias", action="store_true")
    parser.add_argument("--require-florence-fused-depthwise", action="store_true")
    parser.add_argument("--require-ocr-pinned-metadata", action="store_true")
    parser.add_argument("--require-ocr-pinned-inputs", action="store_true")
    parser.add_argument("--require-florence-fused-cross-attention", action="store_true")
    parser.add_argument("--require-lighton-position-cache", action="store_true")
    parser.add_argument("--require-lighton-cached-gates", action="store_true")
    parser.add_argument("--require-lighton-parallel-prefix", action="store_true")
    parser.add_argument("--require-lighton-prefix-batch", action="store_true")
    parser.add_argument("--require-lighton-prefix-shared-qk", action="store_true")
    parser.add_argument("--require-lighton-state-pool", action="store_true")
    parser.add_argument(
        "--require-lighton-shared-recurrent-weights", action="store_true"
    )
    parser.add_argument("--require-lighton-fused-vision-mlp", action="store_true")
    parser.add_argument("--require-lighton-bf16-prefill-ffn", action="store_true")
    parser.add_argument("--require-lighton-bf16-recurrent-prefill", action="store_true")
    parser.add_argument("--require-wide-packed-prefill", action="store_true")
    parser.add_argument("--require-lighton-norm-weight-cache", action="store_true")
    parser.add_argument("--require-florence-bart-ffn", action="store_true")
    parser.add_argument("--require-florence-incremental-admission", action="store_true")
    parser.add_argument("--require-cuda-temp-cache", action="store_true")
    parser.add_argument("--require-lighton-fused-vision-qkv", action="store_true")
    parser.add_argument("--require-lighton-ragged-recurrent", action="store_true")
    parser.add_argument("--require-lighton-ffn-bf16-inputs", action="store_true")
    parser.add_argument("--require-lighton-ffn-block", action="store_true")
    parser.add_argument("--require-lighton-fused-gdn-decode", action="store_true")
    parser.add_argument("--require-florence-fused-kv-append", action="store_true")
    parser.add_argument("--require-lighton-attention-chunk128", action="store_true")
    parser.add_argument("--require-florence-fused-mlp", action="store_true")
    parser.add_argument("--require-florence-vision-microbatch", action="store_true")
    parser.add_argument("--require-florence-encoder-microbatch", action="store_true")
    parser.add_argument("--require-florence-batched-vision-tail", action="store_true")
    parser.add_argument("--require-lighton-fused-projections", action="store_true")
    parser.add_argument("--require-lighton-fused-ffn", action="store_true")
    parser.add_argument("--disable-ocr-ffn-fusion", action="store_true")
    parser.add_argument("--disable-florence-channel-reduction", action="store_true")
    parser.add_argument("--disable-recurrent-encoder", action="store_true")
    parser.add_argument("--disable-recurrent-norm", action="store_true")
    parser.add_argument("--disable-step-pool", action="store_true")
    parser.add_argument("--small-mpp", action="store_true")
    parser.add_argument("--florence-encoder-mirrors", action="store_true")
    parser.add_argument("--disable-florence-encoder-mirrors", action="store_true")
    parser.add_argument("--bf16-projection-inputs", action="store_true")
    parser.add_argument(
        "--bf16-projection-rounds",
        nargs="+",
        choices=("off", "on"),
        help="Override decode projection input precision per resident policy round; requires BF16 prefill",
    )
    parser.add_argument(
        "--require-memory-rounds",
        action="store_true",
        help="Require before/after buffer lifetime gauges outside timed policy rounds",
    )
    parser.add_argument("--disable-wide-mpp", action="store_true")
    parser.add_argument("--disable-florence-single-frame", action="store_true")
    parser.add_argument("--require-florence-single-frame", action="store_true")
    parser.add_argument("--disable-planned-mpp", action="store_true")
    parser.add_argument("--disable-64-row-tile", action="store_true")
    parser.add_argument("--disable-wide-gate-up", action="store_true")
    parser.add_argument("--wide64-gate-up", action="store_true")
    parser.add_argument("--full64-gate-up", action="store_true")
    parser.add_argument("--gpu-greedy-stream", action="store_true")
    parser.add_argument(
        "--stream-chunk-layers", type=int, choices=[0, 1, 2, 4, 8, 16], default=0
    )
    parser.add_argument("--florence-cpu-preprocess", action="store_true")
    parser.add_argument("--disable-lighton-token-encoder", action="store_true")
    parser.add_argument("--disable-prefill-pack-reuse", action="store_true")
    parser.add_argument("--disable-decode-pack-reuse", action="store_true")
    parser.add_argument("--disable-vision-pack-reuse", action="store_true")
    parser.add_argument("--disable-wide-projection-tiles", action="store_true")
    parser.add_argument("--native-read-batch-size", type=int, default=8)
    parser.add_argument("--memory-gib", type=int, default=13)
    parser.add_argument("--shared-memory-gib", type=int, default=24)
    parser.add_argument(
        "--backend-memory-gib",
        type=int,
        default=0,
        help="Explicit backend budget; 0 keeps model defaults",
    )
    parser.add_argument(
        "--kv-memory-gib",
        type=int,
        default=0,
        help="Explicit KV budget; 0 keeps defaults",
    )
    parser.add_argument(
        "--scratch-memory-gib",
        type=int,
        default=0,
        help="Explicit scratch budget; 0 keeps defaults",
    )
    parser.add_argument("--timeout", type=int, default=600)
    parser.add_argument(
        "--reference-report",
        type=Path,
        help="Compare markup-stripped content with saved serial references; use one model per run",
    )
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument(
        "--build-manifest",
        type=Path,
        help="Saved binary/source fingerprints for the tested build",
    )
    parser.add_argument(
        "--batch-argmax",
        action="store_true",
        help="Use two dispatches for qualified M4 LightOnOCR3 batched token selection",
    )
    parser.add_argument(
        "--defer-vision-seal",
        action="store_true",
        help="Keep queued vision groups open until their stage executor is ready",
    )
    parser.add_argument(
        "--defer-prefill-seal",
        action="store_true",
        help="Keep queued prefill groups open until their stage executor is ready",
    )
    parser.add_argument(
        "--cache-vision-workspace",
        action="store_true",
        help="Reuse vision SDPA packing banks under the existing shared epoch lease",
    )
    parser.add_argument(
        "--large-vision-pack-reuse",
        action="store_true",
        help="Reuse frame-local BF16 projection inputs for 4097..16384 vision rows",
    )
    parser.add_argument(
        "--resident-vision-budget",
        action="store_true",
        help="Require resident vision operations and admit their bounded linear workspace",
    )
    parser.add_argument(
        "--gpu-token-burst",
        action="store_true",
        help="Overlap greedy OCR decode steps using GPU token handoff",
    )
    parser.add_argument("--require-cuda-token-burst", action="store_true")
    parser.add_argument(
        "--gpu-token-burst-width",
        type=int,
        choices=tuple(range(2, 9)),
        default=2,
        help="Maximum GPU token burst width; two logits buffers are reused",
    )
    parser.add_argument(
        "--pipeline-vision",
        action="store_true",
        help="Overlap two fully admitted resident vision frame groups",
    )
    parser.add_argument(
        "--prefill-stream-layers",
        type=int,
        choices=(0, 1, 2, 4, 8, 16),
        default=0,
        help="Overlap admitted single-page prefill layer groups after one complete warm prompt",
    )
    args = parser.parse_args()
    if args.cuda_recurrent_core and args.backend != "cuda":
        parser.error("--cuda-recurrent-core requires --backend=cuda")
    if args.cuda_batch and (args.backend != "cuda" or not args.cuda_recurrent_core):
        parser.error("--cuda-batch requires the CUDA recurrent core")
    if args.bf16_projection_rounds and (
        not args.policy_rounds
        or len(args.bf16_projection_rounds) != len(args.policy_rounds)
    ):
        parser.error("--bf16-projection-rounds must match --policy-rounds length")
    if (
        args.bf16_projection_rounds
        and "on" in args.bf16_projection_rounds
        and not args.bf16_prefill_inputs
    ):
        parser.error(
            "decode precision rounds require --bf16-prefill-inputs to hold prefill precision fixed"
        )
    if args.require_memory_rounds and args.backend == "cuda":
        parser.error(
            "--require-memory-rounds uses Metal lifetime gauges; use --require-cuda-temp-cache for CUDA allocation accounting"
        )
    if args.require_cuda_temp_cache and args.backend != "cuda":
        parser.error("--require-cuda-temp-cache requires --backend cuda")
    if args.require_memory_rounds and not args.policy_rounds:
        parser.error("--require-memory-rounds requires --policy-rounds")
    if args.require_prefill_activation_pool and (
        not args.prefill_activation_pool or not args.policy_rounds
    ):
        parser.error(
            "--require-prefill-activation-pool requires --prefill-activation-pool and --policy-rounds"
        )
    prefill_ffn_enabled = args.prefill_fused_ffn or "on" in (
        args.prefill_ffn_rounds or []
    )
    if prefill_ffn_enabled and not args.bf16_prefill_inputs:
        parser.error("packed prefill FFN requires --bf16-prefill-inputs")
    if args.prefill_ffn_rounds and (
        not args.policy_rounds
        or len(args.prefill_ffn_rounds) != len(args.policy_rounds)
    ):
        parser.error("--prefill-ffn-rounds must match --policy-rounds length")
    if args.require_prefill_fused_ffn and not prefill_ffn_enabled:
        parser.error("--require-prefill-fused-ffn requires an enabled FFN round")
    if args.prefill_stream_layers and (
        not args.continuous
        or not args.device_recurrent_state
        or not args.share_decoder_scratch
        or args.prefill_batch
    ):
        parser.error(
            "--prefill-stream-layers requires continuous GPU recurrent state, shared scratch and single-page prefill"
        )
    if args.prepared_prefill_chunk_rows and (
        not args.packed_mixed or not 64 <= args.prepared_prefill_chunk_rows <= 1024
    ):
        parser.error(
            "--prepared-prefill-chunk-rows must be64..1024 and requires --packed-mixed"
        )
    if args.require_prepared_prefill_chunks and not args.prepared_prefill_chunk_rows:
        parser.error(
            "--require-prepared-prefill-chunks requires --prepared-prefill-chunk-rows"
        )
    packed_prefill_limit = (
        16 if args.backend == "cuda" and args.cuda_wide_microbatches else 8
    )
    if not 1 <= args.packed_prefill_items <= packed_prefill_limit or (
        args.packed_prefill_items != 1 and not args.packed_mixed
    ):
        parser.error(
            f"--packed-prefill-items must be 1..{packed_prefill_limit} and requires --packed-mixed"
        )
    if args.require_wide_packed_prefill and not (
        args.policy_rounds
        and args.backend == "cuda"
        and args.cuda_wide_microbatches
        and args.packed_prefill_items > 8
    ):
        parser.error(
            "--require-wide-packed-prefill requires timed wide CUDA packed prefills above eight items"
        )
    if args.packed_shared_frame and (
        not args.packed_mixed
        or args.prefill_activation_pool
        or args.prefill_stream_layers
    ):
        parser.error(
            "--packed-shared-frame requires --packed-mixed and unsplit frames without the retained prefill pool"
        )
    if args.require_packed_shared_frame and not args.packed_shared_frame:
        parser.error("--require-packed-shared-frame requires --packed-shared-frame")
    if args.require_packed_mixed and not args.packed_mixed:
        parser.error("--require-packed-mixed requires --packed-mixed")
    if args.packed_mixed and (
        not args.continuous
        or not args.device_recurrent_state
        or not args.share_decoder_scratch
        or args.prefill_batch
        or args.gpu_greedy_stream
        or args.stream_chunk_layers
    ):
        parser.error(
            "--packed-mixed requires continuous GPU recurrent state, shared decoder scratch and single-page prefill, with decode layer streaming disabled"
        )
    if args.pipeline_vision and (
        not args.continuous or not args.resident_vision_budget or args.vision_batch
    ):
        parser.error(
            "--pipeline-vision requires continuous resident vision with single-image groups"
        )
    if args.require_cuda_token_burst and (
        not args.gpu_token_burst or args.backend != "cuda"
    ):
        parser.error("--require-cuda-token-burst requires CUDA and --gpu-token-burst")
    if args.gpu_token_burst_width != 2 and not args.gpu_token_burst:
        parser.error("--gpu-token-burst-width requires --gpu-token-burst")
    if args.gpu_token_burst and (
        not args.continuous
        or not args.device_recurrent_state
        or not args.share_decoder_scratch
        or args.gpu_greedy_stream
        or args.stream_chunk_layers
    ):
        parser.error(
            "--gpu-token-burst requires continuous GPU recurrent state and shared decoder scratch, with decode layer streaming disabled"
        )
    if args.stream_chunk_layers:
        args.gpu_greedy_stream = True
    if args.gpu_greedy_stream:
        if not args.continuous or not args.device_recurrent_state:
            parser.error(
                "GPU greedy streaming requires --continuous and --device-recurrent-state"
            )
        if os.environ.get("ANTFLY_LIGHTONOCR3_SINGLE_FRAME", "0") == "1":
            parser.error(
                "GPU greedy streaming requires ANTFLY_LIGHTONOCR3_SINGLE_FRAME=0"
            )
        try:
            prefix_layers = int(
                str(args.stream_chunk_layers)
                if args.stream_chunk_layers
                else os.environ.get("ANTFLY_LIGHTONOCR3_STREAM_FIRST_LAYERS", "0")
            )
        except ValueError:
            parser.error("GPU greedy streaming requires a 1..16 layer prefix")
        if not 1 <= prefix_layers <= 16:
            parser.error("GPU greedy streaming requires a 1..16 layer prefix")
    if (
        not 0 <= args.vision_wait_us <= 100000
        or not 0 <= args.prefill_wait_us <= 1000000
    ):
        parser.error(
            "vision waits must be 0..100000 and prefill waits 0..1000000 microseconds"
        )
    if (args.vision_batch or args.prefill_batch) and not args.continuous:
        parser.error("vision/prefill batching requires --continuous")
    if args.prefill_direct_kv and not args.prefill_sg_attention:
        parser.error("direct KV prefill requires --prefill-sg-attention")
    if args.policy_rounds and len(args.policy_rounds) > 16:
        parser.error("at most 16 policy rounds are supported")
    if args.requests < 1 or any(c < 1 or c > 256 for c in args.concurrency):
        parser.error("requests must be positive; client concurrency must be 1..256")
    if args.admission_units < 1:
        parser.error("admission units must be positive")
    if sum([args.physical32, args.physical64, args.physical128]) > 1:
        parser.error("choose only one physical row ceiling")
    if args.cuda_wide_microbatches and args.backend != "cuda":
        parser.error("wide microbatches require CUDA")
    vision_limit = (
        16 if args.cuda_wide_microbatches else (8 if args.backend == "cuda" else 4)
    )
    prefill_limit = 16 if args.cuda_wide_microbatches else 8
    if not 1 <= args.vision_batch_items <= vision_limit:
        parser.error(f"vision batch limit must be 1..{vision_limit}")
    if not 1 <= args.prefill_batch_items <= prefill_limit:
        parser.error(f"prefill batch limit must be 1..{prefill_limit}")
    if args.require_compact_kv and not args.compact_kv:
        parser.error("--require-compact-kv requires --compact-kv")
    if args.require_physical128 and not args.policy_rounds:
        parser.error("--require-physical128 requires timed --policy-rounds")
    if args.require_physical128 and not args.physical128:
        parser.error("--require-physical128 requires --physical128")
    if args.physical128 and not (
        args.backend == "cuda"
        and args.cuda_batch
        and args.packed_mixed
        and args.extended_epoch
    ):
        parser.error(
            "--physical128 requires CUDA batching, packed mixed execution and an extended epoch"
        )
    if not 1 <= args.pages_per_request <= 64 or not 1 <= args.batch_rows <= (
        128 if args.physical128 else 64
    ):
        parser.error(
            "pages per request must be 1..64; batch rows may reach 128 with --physical128"
        )
    if args.batch_rows > 32 and not (args.physical64 or args.physical128):
        parser.error("more than thirty-two physical rows requires --physical64")
    if args.batch_rows > 16 and not (
        args.physical32 or args.physical64 or args.physical128
    ):
        parser.error(
            "more than sixteen physical rows requires --physical32 or --physical64"
        )
    if (args.physical32 or args.physical64 or args.physical128) and not (
        args.continuous or args.cuda_batch
    ):
        parser.error("physical row overrides require --continuous or --cuda-batch")
    if args.share_decoder_scratch and (
        not args.device_recurrent_state or not args.continuous
    ):
        parser.error(
            "--share-decoder-scratch requires --device-recurrent-state and --continuous"
        )
    if args.logical64 and not (args.physical32 or args.physical64):
        parser.error("--logical64 requires --physical32 or --physical64")
    if (
        not 1
        <= args.batch_window
        <= (
            128 if args.physical128 else 64 if args.logical64 or args.physical64 else 32
        )
    ):
        parser.error(
            "batch window must be 1..32, or 1..64 with --logical64 or --physical64"
        )
    if not 1 <= args.native_read_batch_size <= 64:
        parser.error("native read batch size must be 1..64")
    if args.florence_encoder_mirrors and args.disable_florence_encoder_mirrors:
        parser.error("choose only one Florence mirror override")
    if not 0 <= args.decode_wait_us <= 10000:
        parser.error("decode wait must be 0..10000 microseconds")
    florence_wait_limit = 1000000 if args.backend == "cuda" else 20000
    if not 0 <= args.florence_batch_wait_us <= florence_wait_limit:
        parser.error(
            f"Florence batch wait must be 0..{florence_wait_limit} microseconds"
        )
    if not 0 <= args.batch_wait_us <= 20000:
        parser.error("batch wait must be 0..20000 microseconds")
    if any(
        value < 0
        for value in (
            args.memory_gib,
            args.shared_memory_gib,
            args.backend_memory_gib,
            args.kv_memory_gib,
            args.scratch_memory_gib,
        )
    ):
        parser.error("memory budgets must be nonnegative")
    models = {}
    for specification in args.model:
        label, directory = specification.split("=", 1)
        if not re.fullmatch(r"[A-Za-z0-9_-]+", label) or label in models:
            parser.error(
                "model labels must be unique and use letters, numbers, underscores or hyphens"
            )
        models[label] = Path(directory).resolve()
    if args.reference_report:
        if len(models) != 1:
            parser.error("reference report requires exactly one model")
        reference = json.loads(args.reference_report.read_text())
        if [image["sha256"] for image in reference["images"]] != [
            digest(image) for image in (args.images or [])
        ]:
            parser.error("reference report must use the same images in the same order")
    build = None
    if args.build_manifest:
        build = json.loads(args.build_manifest.read_text())
        if build.get("binary_sha256") != digest(args.native_test):
            parser.error("build manifest does not match the native test binary")
        if not re.fullmatch(
            r"[0-9a-f]{40}", build.get("runtime_commit", "")
        ) or not re.fullmatch(
            r"[0-9a-f]{64}", build.get("runtime_source_diff_sha256", "")
        ):
            parser.error(
                "build manifest requires valid commit and runtime diff fingerprints"
            )
    args.output_dir.mkdir(parents=True, exist_ok=True)
    report = {
        "created_at": datetime.now(timezone.utc).isoformat(),  # noqa: UP017 (stock macOS Python 3.9)
        "hardware": hardware(),
        "backend": args.backend,
        "cuda_recurrent_core": args.cuda_recurrent_core,
        "cuda_batch": args.cuda_batch,
        "runtime_commit": subprocess.check_output(
            ["git", "rev-parse", "HEAD"], text=True
        ).strip(),
        "runtime_source_diff_sha256": __import__("hashlib")
        .sha256(
            subprocess.check_output(
                ["git", "diff", "HEAD", "--", "zig/pkg/inference/src"]
            )
        )
        .hexdigest(),
        "binary_sha256": digest(args.native_test),
        "models": {label: checkpoint(path) for label, path in models.items()},
        "images": [
            {"path": str(p.resolve()), "sha256": digest(p)} for p in (args.images or [])
        ],
        "benchmark_script_sha256": digest(Path(__file__)),
        "settings": {
            key: str(value) if isinstance(value, Path) else value
            for key, value in vars(args).items()
            if key not in ("native_test", "images", "output_dir")
        },
        "stream_chunk_layers": args.stream_chunk_layers,
        "batch_argmax": args.batch_argmax,
        "cache_vision_workspace": args.cache_vision_workspace,
        "large_vision_pack_reuse": args.large_vision_pack_reuse,
        "resident_vision_budget": args.resident_vision_budget,
        "pipeline_vision": args.pipeline_vision,
        "prefill_stream_layers": args.prefill_stream_layers,
        "gpu_token_burst": args.gpu_token_burst,
        "gpu_token_burst_width": args.gpu_token_burst_width,
        "defer_vision_seal": args.defer_vision_seal,
        "defer_prefill_seal": args.defer_prefill_seal,
        "submission_policy": "Concurrency bounds submitting client threads; batch_window independently bounds live pages and batch_rows caps physical GPU decode rows. A larger client backlog changes neither serving cap. vLLM offline queues all requests up front and its concurrency option sets max_num_seqs.",
        "scope": "One resident Node; timed native encoded-image API requests including preprocessing, queueing and inference; excludes loading, reference reads, HTTP, PDF rendering and indexing. Other machine workloads uncontrolled. Every timed output must match its same-model serial reference.",
        "results": [],
    }
    prefill_library = os.environ.get("ANTFLY_CUDA_LIGHTON_PREFILL_LIBRARY")
    if prefill_library:
        report["bf16_recurrent_prefill_library"] = {
            "path": str(Path(prefill_library).resolve()),
            "sha256": digest(Path(prefill_library)),
            "precision": "BF16 recurrent prefill operands/output; F32 state and decode",
        }
    if build is not None:
        report["worktree_runtime_commit"] = report["runtime_commit"]
        report["worktree_runtime_source_diff_sha256"] = report[
            "runtime_source_diff_sha256"
        ]
        report["runtime_commit"] = build["runtime_commit"]
        report["runtime_source_diff_sha256"] = build["runtime_source_diff_sha256"]
        report["build_manifest_sha256"] = digest(args.build_manifest)
    report_path = args.output_dir / "report.json"
    for label, model in models.items():
        for concurrency in args.concurrency:
            for mode in args.modes:
                result = run(args, model, label, mode, concurrency, args.output_dir)
                report["results"].append(result)
                report_path.write_text(json.dumps(report, indent=2) + "\n")
                print(json.dumps(result), flush=True)
    report["hardware_after"] = hardware()
    report_path.write_text(json.dumps(report, indent=2) + "\n")
    if any(result["status"] != "ok" for result in report["results"]):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
