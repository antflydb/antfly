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


def run(args, model, label, mode, concurrency, output):
    env = os.environ.copy()
    for key in list(env):
        if key.startswith(
            ("TERMITE_METAL_DISABLE_LIGHTONOCR3", "TERMITE_METAL_DISABLE_QWEN35")
        ) or key in (
            "TERMITE_METAL_LIGHTONOCR3_BF16_ACTIVATIONS",
            "TERMITE_METAL_ENABLE_LIGHTONOCR3_WEIGHT_RESIDENCY",
            "TERMITE_METAL_LIGHTONOCR3_BF16_PREFILL_INPUTS",
            "ANTFLY_INFERENCE_FLORENCE_DISABLE_KV_CACHE",
            "ANTFLY_FLORENCE2_BATCH_WAIT_US",
            "ANTFLY_INFERENCE_READ_PROFILE",
            "ANTFLY_LIGHTONOCR3_CONTINUOUS",
            "ANTFLY_LIGHTONOCR3_VISION_BATCH",
            "ANTFLY_LIGHTONOCR3_DISABLE_SHARED_VISION_PEAK",
            "ANTFLY_LIGHTONOCR3_PREFILL_BATCH",
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
    if args.disable_shared_vision_peak:
        env["ANTFLY_LIGHTONOCR3_DISABLE_SHARED_VISION_PEAK"] = "1"
    if args.vision_batch:
        env["ANTFLY_LIGHTONOCR3_VISION_BATCH"] = "1"
    env["ANTFLY_LIGHTONOCR3_CONTINUOUS"] = "1" if args.continuous else "0"
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
        ANTFLY_INFERENCE_PREFERRED_BACKEND="metal",
        ANTFLY_INFERENCE_REQUIRED_BACKEND="metal",
    )
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
        "mode": mode,
        "concurrency": concurrency,
        "log": path.name,
        "exit_code": process.returncode,
        "status": "timeout" if timed_out else "failed",
    }
    if process.returncode == 0 and match and "LIGHTONOCR3 backend=metal" in text:
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
    elif "ResourceLimitExceeded" in text or "MemoryAdmissionDenied" in text:
        result["status"] = "resource_limit"
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--native-test", type=Path, required=True)
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
    parser.add_argument("--vision-batch", action="store_true")
    parser.add_argument("--disable-shared-vision-peak", action="store_true")
    parser.add_argument("--prefill-batch", action="store_true")
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
    parser.add_argument("--disable-ocr-ffn-fusion", action="store_true")
    parser.add_argument("--disable-florence-channel-reduction", action="store_true")
    parser.add_argument("--disable-recurrent-encoder", action="store_true")
    parser.add_argument("--disable-recurrent-norm", action="store_true")
    parser.add_argument("--disable-step-pool", action="store_true")
    parser.add_argument("--small-mpp", action="store_true")
    parser.add_argument("--florence-encoder-mirrors", action="store_true")
    parser.add_argument("--disable-florence-encoder-mirrors", action="store_true")
    parser.add_argument("--bf16-projection-inputs", action="store_true")
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
    args = parser.parse_args()
    if (
        not 0 <= args.vision_wait_us <= 100000
        or not 0 <= args.prefill_wait_us <= 100000
    ):
        parser.error("vision/prefill collection waits must be 0..100000 microseconds")
    if (args.vision_batch or args.prefill_batch) and not args.continuous:
        parser.error("vision/prefill batching requires --continuous")
    if args.prefill_direct_kv and not args.prefill_sg_attention:
        parser.error("direct KV prefill requires --prefill-sg-attention")
    if args.requests < 1 or any(c < 1 or c > 16 for c in args.concurrency):
        parser.error("requests must be positive; concurrency must be 1..16")
    if args.admission_units < 1:
        parser.error("admission units must be positive")
    if not 1 <= args.pages_per_request <= 8 or not 1 <= args.batch_rows <= 8:
        parser.error("pages per request and batch rows must be 1..8")
    if not 1 <= args.batch_window <= 16:
        parser.error("batch window must be 1..16")
    if not 1 <= args.native_read_batch_size <= 8:
        parser.error("native read batch size must be 1..8")
    if args.florence_encoder_mirrors and args.disable_florence_encoder_mirrors:
        parser.error("choose only one Florence mirror override")
    if not 0 <= args.decode_wait_us <= 10000:
        parser.error("decode wait must be 0..10000 microseconds")
    if not 0 <= args.florence_batch_wait_us <= 20000:
        parser.error("Florence batch wait must be 0..20000 microseconds")
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
        "settings": {
            key: str(value) if isinstance(value, Path) else value
            for key, value in vars(args).items()
            if key not in ("native_test", "images", "output_dir")
        },
        "scope": "One resident Node; timed native encoded-image API requests including preprocessing, queueing and inference; excludes loading, reference reads, HTTP, PDF rendering and indexing. Other machine workloads uncontrolled. Every timed output must match its same-model serial reference.",
        "results": [],
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
    if any(result["status"] != "ok" for result in report["results"]):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
