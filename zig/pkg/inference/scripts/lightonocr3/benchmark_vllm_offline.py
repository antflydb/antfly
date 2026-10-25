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

"""Benchmark uncached OCR through the actual in-process vLLM V1 engine.

Run sequentially with other GPU benchmarks. The worker owns its entire process
and model. Every image has a unique UUID; prefix and processor caches are off.
Explicit cache capacities must fit vLLM's reported automatic capacity.
"""

import argparse
import base64
import gc
import json
import os
import signal
import subprocess
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from benchmark_antfly_readers import checkpoint, digest, hardware
from benchmark_vllm_metal import cache_override_admission


def arguments():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--python", type=Path, default=Path(sys.executable))
    parser.add_argument("--model-dir", type=Path, required=True)
    parser.add_argument("--images", type=Path, nargs="+", required=True)
    parser.add_argument("--concurrency", type=int, default=64)
    parser.add_argument("--requests", type=int, default=128)
    parser.add_argument("--max-tokens", type=int, default=256)
    parser.add_argument("--max-model-len", type=int, default=1024)
    parser.add_argument("--gpu-memory-utilization", type=float, default=0.7)
    parser.add_argument("--kv-cache-blocks", type=int)
    parser.add_argument("--timeout", type=float, default=900)
    parser.add_argument("--oracle-report", type=Path)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args()
    if min(args.concurrency, args.requests, args.max_tokens, args.max_model_len) < 1:
        parser.error("request dimensions must be positive")
    if args.kv_cache_blocks is not None and args.kv_cache_blocks < 1:
        parser.error("kv-cache-blocks must be positive")
    if not 0 < args.gpu_memory_utilization <= 1:
        parser.error("gpu-memory-utilization must be in (0, 1]")
    return args


def worker(args):
    # Set before importing vLLM or creating its engine. Async HTTP serving uses
    # a separate EngineCore; the offline LLMEngine supports InprocClient.
    os.environ["VLLM_ENABLE_V1_MULTIPROCESSING"] = "0"
    import mlx.core as mx
    from vllm import LLM, SamplingParams
    from vllm.platforms import current_platform

    report_path = args.output_dir / "report.json"
    report = json.loads(report_path.read_text())

    def save():
        report_path.write_text(json.dumps(report, indent=2) + "\n")

    model = None
    try:
        assert type(current_platform).__module__.startswith("vllm_metal"), type(
            current_platform
        )
        report.update(status="loading", platform_class=str(type(current_platform)))
        save()
        load_started = time.perf_counter()
        options = dict(
            model=str(args.model_dir.resolve()),
            dtype="bfloat16",
            max_model_len=args.max_model_len,
            max_num_seqs=args.concurrency,
            max_num_batched_tokens=4096,
            gpu_memory_utilization=args.gpu_memory_utilization,
            enable_prefix_caching=False,
            mm_processor_cache_gb=0,
            limit_mm_per_prompt={"image": 1, "video": 0},
        )
        if args.kv_cache_blocks is not None:
            options["num_gpu_blocks_override"] = args.kv_cache_blocks
        model = LLM(**options)
        report["model_load_seconds"] = time.perf_counter() - load_started
        report["engine_client_class"] = type(model.llm_engine.engine_core).__name__
        assert report["engine_client_class"] == "InprocClient", report[
            "engine_client_class"
        ]
        sys.stdout.flush()
        sys.stderr.flush()
        admission = cache_override_admission(
            (args.output_dir / "worker.txt").read_text(), args.kv_cache_blocks
        )
        report["cache_override_admission"] = admission
        save()
        if not admission["admitted"]:
            raise RuntimeError("Cache override not admitted: " + admission["reason"])

        images = [
            "data:image/png;base64," + base64.b64encode(p.read_bytes()).decode()
            for p in args.images
        ]
        media_ids = set()

        def conversation(index):
            media_id = uuid.uuid4().hex
            assert media_id not in media_ids
            media_ids.add(media_id)
            return [
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "image_url",
                            "image_url": {"url": images[index % len(images)]},
                            "uuid": media_id,
                        }
                    ],
                }
            ]

        sampling = SamplingParams(temperature=0, max_tokens=args.max_tokens)

        def generate(messages):
            outputs = model.chat(
                messages,
                sampling_params=sampling,
                use_tqdm=False,
                chat_template_kwargs={"enable_thinking": False},
            )
            # One final drain, never a per-step profiling/synchronization hook.
            mx.synchronize()
            assert len(outputs) == len(messages)
            assert all(o.finished and len(o.outputs) == 1 for o in outputs)
            return outputs

        references = []
        for index in range(len(images)):
            output = generate([conversation(index)])[0]
            assert output.prompt_token_ids
            references.append(
                dict(
                    image_index=index,
                    text=output.outputs[0].text,
                    usage=dict(
                        prompt_tokens=len(output.prompt_token_ids),
                        completion_tokens=len(output.outputs[0].token_ids),
                    ),
                )
            )
        assert (
            max(r["usage"]["prompt_tokens"] for r in references) + args.max_tokens
            <= args.max_model_len
        )
        if args.oracle_report:
            oracle = json.loads(args.oracle_report.read_text())
            assert {r["image_index"]: r["text"] for r in references} == {
                r["image_index"]: r["text"] for r in oracle["references"]
            }
        report.update(status="running", references=references)
        save()
        started = time.perf_counter()
        outputs = generate([conversation(i) for i in range(args.requests)])
        elapsed = time.perf_counter() - started
        parity = all(
            o.outputs[0].text == references[i % len(images)]["text"]
            for i, o in enumerate(outputs)
        )
        result = dict(
            concurrency=args.concurrency,
            completed=len(outputs),
            elapsed_seconds=elapsed,
            pages_per_second=len(outputs) / elapsed,
            serial_reference_parity=parity,
            outputs=[
                dict(
                    request_index=i,
                    image_index=i % len(images),
                    text=o.outputs[0].text,
                    finish_reason=o.outputs[0].finish_reason,
                    completion_tokens=len(o.outputs[0].token_ids),
                )
                for i, o in enumerate(outputs)
            ],
        )
        report["results"].append(result)
        report["unique_media_uuids"] = len(media_ids)
        assert len(media_ids) == len(images) + args.requests
        assert parity, "A concurrent page differs from its fresh serial reference"
        report["status"] = "ok"
        save()
        print(
            json.dumps({k: v for k, v in result.items() if k != "outputs"}), flush=True
        )
    except Exception as error:
        report.update(status="failed", error=str(error))
        save()
        raise
    finally:
        model = None
        gc.collect()
        mx.clear_cache()


def main():
    args = arguments()
    if args.worker:
        worker(args)
        return
    args.output_dir.mkdir(parents=True, exist_ok=False)
    report_path = args.output_dir / "report.json"
    command = [
        str(args.python.absolute()),
        "-u",
        str(Path(__file__).resolve()),
        *sys.argv[1:],
        "--worker",
    ]
    report = dict(
        created_at=datetime.now(timezone.utc).isoformat(),
        hardware=hardware(),
        benchmark_script_sha256=digest(Path(__file__)),
        model=checkpoint(args.model_dir.resolve()),
        images=[dict(path=str(p.resolve()), sha256=digest(p)) for p in args.images],
        command=command,
        reference_engine_mode="offline_v1_inprocess",
        worker_environment={"VLLM_ENABLE_V1_MULTIPROCESSING": "0"},
        gpu_memory_utilization=args.gpu_memory_utilization,
        kv_cache_block_override=args.kv_cache_blocks,
        max_model_len=args.max_model_len,
        versions=json.loads(
            subprocess.check_output(
                [
                    str(args.python.absolute()),
                    "-c",
                    'import importlib.metadata as m,json;print(json.dumps({p:m.version(p)for p in ["vllm","vllm-metal","mlx","mlx-vlm","mlx-lm","transformers","torch"]}))',
                ],
                text=True,
            )
        ),
        scope="Actual vLLM V1 InprocClient, original BF16 checkpoint, image-only greedy OCR with thinking off. All requests queued up front; max_num_seqs bounds active concurrency. Includes chat rendering, image preprocessing, queue, inference, detokenization, and a final GPU drain. Excludes loading, serial references, disk reads and HTTP. Prefix/processor caches disabled; fresh media UUID for every request. Other machine workloads uncontrolled.",
        status="starting",
        results=[],
    )
    report_path.write_text(json.dumps(report, indent=2) + "\n")
    with (args.output_dir / "worker.txt").open("w") as log:
        process = subprocess.Popen(
            command, stdout=log, stderr=subprocess.STDOUT, start_new_session=True
        )
        try:
            process.wait(timeout=args.timeout)
            if process.returncode:
                raise RuntimeError(
                    f"Worker exited with {process.returncode}; see worker.txt"
                )
        except Exception as error:
            report = json.loads(report_path.read_text())
            report.update(status="failed", error=report.get("error", str(error)))
            report_path.write_text(json.dumps(report, indent=2) + "\n")
            raise
        finally:
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGTERM)
                try:
                    process.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait()


if __name__ == "__main__":
    main()
