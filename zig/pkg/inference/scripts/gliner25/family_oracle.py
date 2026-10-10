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

"""Pinned, persistent CUDA oracle for the Multi and Decide-1B checkpoints.

Each JSON-line command supplies distinct texts and either `tasks` (classification)
or `schema` (extraction). `validate` additionally captures every encoder invocation;
`run` times the public upstream batch API without instrumentation or model loading.
Failures are fatal, including unsupported attention implementations. This worker
does not grant production qualification.
"""

import argparse
import hashlib
import importlib.metadata
import json
from pathlib import Path
import sys
import time


FIXTURES = Path(__file__).resolve().parents[2] / "testdata/gliner25/family"
UPSTREAM_COMMIT = "55656fbfa01d3d4a77485e1a1eeeaf682990ccdf"


def verify_model(directory, name, manifest=None):
    manifest = manifest or json.loads((FIXTURES / "manifest.json").read_text())
    pin = manifest["models"][name]
    for filename, expected in pin["files"].items():
        path = directory / filename
        if path.stat().st_size != expected["size_bytes"]:
            raise ValueError(f"size mismatch: {filename}")
        digest = hashlib.sha256()
        with path.open("rb") as stream:
            for block in iter(lambda: stream.read(8 << 20), b""):
                digest.update(block)
        if digest.hexdigest() != expected["sha256"]:
            raise ValueError(f"SHA-256 mismatch: {filename}")
    return pin


def execute(model, command):
    texts = command["texts"]
    if not isinstance(texts, list) or not 1 <= len(texts) <= 64:
        raise ValueError("expected 1..64 distinct request rows")
    if any(not isinstance(text, str) for text in texts):
        raise ValueError("texts must be strings")
    if ("tasks" in command) == ("schema" in command):
        raise ValueError("supply exactly one of tasks and schema")
    if "kind" in command:
        # The existing full-head oracle defines the public Classifier and
        # JointIE entry points as well as extraction. Keep their schema parsing
        # inside the same timed boundary used by the native boundary worker.
        from benchmark_cpu import execute_python

        return [
            execute_python(
                model, dict(command, text=text), json.dumps(command["schema"])
            )
            for text in texts
        ]
    options = dict(batch_size=len(texts), include_confidence=True)
    if "tasks" in command:
        return model.batch_classify_text(texts, command["tasks"], **options)
    from oracle import build_extract_schema

    return model.batch_extract(
        texts,
        build_extract_schema(command["schema"]),
        num_workers=0,
        include_spans=True,
        **options,
    )


def emit(value):
    print(json.dumps(value, ensure_ascii=False, allow_nan=False), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model-dir", type=Path, required=True)
    parser.add_argument(
        "--model", choices=("multi", "multi_decide", "decide_1b"), required=True
    )
    parser.add_argument("--dtype", choices=("fp32", "fp16", "bf16"), default="fp32")
    parser.add_argument(
        "--weight-dtype",
        choices=("fp32", "fp16", "bf16"),
        default="fp32",
        help="Resident weight dtype; --dtype separately selects autocast",
    )
    parser.add_argument(
        "--attention", choices=("eager", "sdpa", "flashdeberta"), default="eager"
    )
    parser.add_argument(
        "--cases",
        type=Path,
        help="Capture every fixture once instead of reading worker commands",
    )
    args = parser.parse_args()
    if args.weight_dtype != "fp32" and args.weight_dtype != args.dtype:
        parser.error("reduced resident weights require the matching autocast dtype")
    pin = verify_model(args.model_dir, args.model)

    import torch
    import gliner2
    from gliner2 import AutoExtractor

    reference = json.loads((FIXTURES / "manifest.json").read_text())["reference"]
    if reference["gliner2_commit"] != UPSTREAM_COMMIT:
        raise RuntimeError("unexpected upstream commit")
    source_root = Path(gliner2.__file__).parent
    for filename, digest in reference["source_files"].items():
        if hashlib.sha256((source_root / filename).read_bytes()).hexdigest() != digest:
            raise RuntimeError(f"upstream source mismatch: {filename}")
    if not torch.cuda.is_available():
        raise RuntimeError("CUDA unavailable; CPU fallback is forbidden")
    # All reference FP32 matrix products use IEEE, including SDPA/Triton.
    torch.backends.cuda.matmul.fp32_precision = "ieee"
    torch.backends.cudnn.conv.fp32_precision = "ieee"
    torch.backends.cuda.matmul.allow_fp16_reduced_precision_reduction = False
    torch.backends.cuda.matmul.allow_bf16_reduced_precision_reduction = False
    torch.set_num_threads(1)
    flash = args.attention == "flashdeberta"
    if flash:
        import flashdeberta

        flash_pin = reference["flashdeberta"]
        if importlib.metadata.version("flashdeberta") != flash_pin["version"]:
            raise RuntimeError("FlashDeBERTa version mismatch")
        flash_root = Path(flashdeberta.__file__).parent
        for filename, digest in flash_pin["source_files"].items():
            if (
                hashlib.sha256((flash_root / filename).read_bytes()).hexdigest()
                != digest
            ):
                raise RuntimeError(f"FlashDeBERTa source mismatch: {filename}")
    dtypes = {"fp32": torch.float32, "fp16": torch.float16, "bf16": torch.bfloat16}
    model = (
        AutoExtractor.from_pretrained(str(args.model_dir), use_flashdeberta=flash)
        .eval()
        .to(device="cuda", dtype=dtypes[args.weight_dtype])
    )
    if flash:
        if "flashdeberta" not in type(model.encoder).__module__.lower():
            raise RuntimeError("FlashDeBERTa silently fell back")
    else:
        model.encoder.set_attn_implementation(args.attention)
        if model.encoder.config._attn_implementation != args.attention:
            raise RuntimeError("attention implementation silently fell back")
    packages = ("gliner2", "torch", "transformers", "tokenizers", "safetensors")
    versions = {name: importlib.metadata.version(name) for name in packages}
    if flash:
        versions.update(
            {
                name: importlib.metadata.version(name)
                for name in ("flashdeberta", "triton")
            }
        )
    if (
        versions["torch"].split("+")[0] != "2.14.0"
        or versions["transformers"] != "5.17.0"
    ):
        raise RuntimeError("reference requires torch 2.14.0 and transformers 5.17.0")
    dtype = dtypes[args.dtype]
    metadata = dict(
        event="ready",
        arm="fastino_cuda",
        model_id=pin["model_id"],
        revision=pin["revision"],
        model_files=pin["files"],
        packages=versions,
        dtype=args.dtype,
        weight_dtype=args.weight_dtype,
        attention=args.attention,
        device=torch.cuda.get_device_name(),
        cuda=torch.version.cuda,
        gliner2_commit=UPSTREAM_COMMIT,
        timing_boundary="public_batch_api_loaded_model",
        qualification=False,
    )
    emit(metadata)
    commands = (
        (
            dict(case, op="validate", request_id=case["id"])
            for case in json.loads(args.cases.read_text())["cases"]
        )
        if args.cases
        else map(json.loads, sys.stdin)
    )
    for command in commands:
        op = command["op"]
        if op == "stop":
            return
        if op not in ("validate", "run"):
            raise ValueError("unknown operation")
        if args.model != "multi" and "schema" in command:
            raise ValueError("Decide checkpoints qualify classification only")
        captures = []
        original = model.encoder.forward

        def capture(*positional, **keywords):
            ids = keywords.get("input_ids", positional[0] if positional else None)
            captures.append(
                dict(
                    input_ids=ids.cpu().tolist(),
                    attention_mask=keywords["attention_mask"].cpu().tolist(),
                )
            )
            return original(*positional, **keywords)

        if op == "validate":
            model.encoder.forward = capture
        try:
            with (
                torch.inference_mode(),
                torch.autocast("cuda", dtype=dtype, enabled=args.dtype != "fp32"),
            ):
                torch.cuda.synchronize()
                start = time.perf_counter_ns()
                output = execute(model, command)
                torch.cuda.synchronize()
                duration = time.perf_counter_ns() - start
        finally:
            model.encoder.forward = original
        if "schema" in command:
            from benchmark_cpu import canonical_python

            output = [
                canonical_python(
                    dict(command, text=text, kind=command.get("kind", "extract")), row
                )
                for text, row in zip(command["texts"], output, strict=True)
            ]
        emit(
            dict(
                request_id=command.get("request_id"),
                outputs=output,
                duration_ns=duration,
                encoder_calls=captures,
                timed=op == "run",
            )
        )


if __name__ == "__main__":
    main()
