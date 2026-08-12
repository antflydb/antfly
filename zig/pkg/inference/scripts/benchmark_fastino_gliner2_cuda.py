#!/usr/bin/env python3
"""Fail-closed Fastino GLiNER2 CUDA reference benchmark.

The benchmark deliberately records both wall-clock API latency and CUDA-event
execution latency.  It refuses to run unless the model and the observed
forward inputs are CUDA-resident FP16 tensors, so a CPU reference cannot be
mistaken for the CUDA baseline.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import statistics
import time
from dataclasses import fields, is_dataclass

import torch
from gliner2 import GLiNER2


TEXT = (
    "On Tuesday, the City of Riverton announced a public transit pilot with "
    "Acme Robotics and Northstar Analytics. Mayor Elena Ortiz said the program "
    "will add electric shuttles between the downtown library, the university "
    "medical center, and Harbor Station. The $4.8 million contract begins in "
    "September and runs for eighteen months. Acme will provide vehicles, while "
    "Northstar will analyze ridership data and publish monthly reports. Residents "
    "can submit comments through the Riverton Mobility Office before August 30. "
    "The project is funded by a state clean transportation grant and local capital "
    "improvements budget. On Tuesday, the City of Riverton announced a public "
    "transit pilot with Acme Robotics and Northstar Analytics. Mayor Elena Ortiz "
    "said the program will add electric shuttles between the downtown library, the "
    "university medical center, and Harbor Station. The $4.8 million contract "
    "begins in September and runs for eighteen months. Acme will provide vehicles, "
    "while Northstar will analyze ridership data and publish monthly reports."
)
LABELS = ["person", "organization", "location", "date", "money"]


def assert_cuda_fp16_model(model: torch.nn.Module, device: torch.device) -> None:
    tensors = list(model.parameters()) + list(model.buffers())
    if not tensors:
        raise RuntimeError("Fastino model has no tensors to validate")
    wrong_device = [t for t in tensors if t.device != device]
    if wrong_device:
        raise RuntimeError("Fastino model contains non-CUDA or wrong-device tensors")
    floating = [t for t in tensors if t.is_floating_point()]
    if not floating or any(t.dtype != torch.float16 for t in floating):
        raise RuntimeError("Fastino quantize=True did not produce an FP16 model")


def tensors_in(value: object) -> list[torch.Tensor]:
    """Collect tensor leaves without assuming Fastino's internal call shape."""
    if isinstance(value, torch.Tensor):
        return [value]
    if is_dataclass(value):
        return [tensor for field in fields(value) for tensor in tensors_in(getattr(value, field.name))]
    if isinstance(value, dict):
        return [tensor for child in value.values() for tensor in tensors_in(child)]
    if isinstance(value, (list, tuple)):
        return [tensor for child in value for tensor in tensors_in(child)]
    return []


def verify_forward_residency(
    model: torch.nn.Module,
    device: torch.device,
    text: str,
    labels: list[str],
    expected_token_count: int | None,
) -> int:
    observed = False
    observed_encoder_tokens = 0

    def hook(_module: torch.nn.Module, args: tuple[object, ...]) -> None:
        nonlocal observed, observed_encoder_tokens
        tensors = tensors_in(args)
        if not tensors:
            return
        # Fastino's public batch API calls an internal module rather than
        # GLiNER2.forward on current releases.  Observe every module so this
        # remains valid across releases, and require an actual CUDA tensor at
        # the model boundary rather than inferring GPU use from parameters.
        if any(t.device == device for t in tensors):
            observed = True
        # GLiNER's encoder input IDs are CUDA integer [batch, sequence]
        # tensors. Capture the widest one at the model boundary rather than
        # approximating tokens from text length; schema labels are part of the
        # actual encoder sequence and must be included in a fair comparison.
        for tensor in tensors:
            if tensor.device == device and not tensor.is_floating_point() and tensor.ndim >= 2:
                observed_encoder_tokens = max(observed_encoder_tokens, int(tensor.shape[-1]))

    handles = [module.register_forward_pre_hook(hook) for module in model.modules()]
    try:
        model.batch_extract_entities([text], labels, batch_size=1)
        torch.cuda.synchronize(device)
    finally:
        for handle in handles:
            handle.remove()
    if not observed:
        raise RuntimeError("Fastino CUDA forward hook was not reached")
    if observed_encoder_tokens <= 0:
        raise RuntimeError("Fastino encoder token-count hook was not reached")
    if expected_token_count is not None and observed_encoder_tokens != expected_token_count:
        raise RuntimeError(
            f"expected {expected_token_count} encoder tokens, got {observed_encoder_tokens}"
        )
    return observed_encoder_tokens


def percentile(samples: list[float], ratio: float) -> float:
    """Linear-interpolated percentile, matching the Zig benchmark harness."""
    ordered = sorted(samples)
    position = (len(ordered) - 1) * ratio
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    fraction = position - lower
    return ordered[lower] * (1.0 - fraction) + ordered[upper] * fraction


def measure(
    model: GLiNER2,
    text: str,
    labels: list[str],
    batch_size: int,
    warmups: int,
    repeats: int,
    device: torch.device,
) -> dict[str, object]:
    texts = [text] * batch_size
    for _ in range(warmups):
        model.batch_extract_entities(texts, labels, batch_size=batch_size)
    torch.cuda.synchronize(device)

    api_ms: list[float] = []
    gpu_ms: list[float] = []
    entity_count = 0
    for _ in range(repeats):
        start_event = torch.cuda.Event(enable_timing=True)
        end_event = torch.cuda.Event(enable_timing=True)
        torch.cuda.synchronize(device)
        start = time.perf_counter_ns()
        start_event.record(torch.cuda.current_stream(device))
        result = model.batch_extract_entities(texts, labels, batch_size=batch_size)
        end_event.record(torch.cuda.current_stream(device))
        torch.cuda.synchronize(device)
        api_ms.append((time.perf_counter_ns() - start) / 1_000_000.0)
        gpu_ms.append(start_event.elapsed_time(end_event))
        entity_count = sum(len(group) for item in result for group in item["entities"].values())

    if max(gpu_ms) <= 0.0:
        raise RuntimeError("CUDA events observed no GPU execution")
    return {
        "batch": batch_size,
        "api_avg_ms": statistics.mean(api_ms),
        "api_p50_ms": percentile(api_ms, 0.50),
        "api_p95_ms": percentile(api_ms, 0.95),
        "gpu_avg_ms": statistics.mean(gpu_ms),
        "gpu_p50_ms": percentile(gpu_ms, 0.50),
        "gpu_p95_ms": percentile(gpu_ms, 0.95),
        "items_per_s": batch_size * 1000.0 / statistics.mean(api_ms),
        "entity_count": entity_count,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--model", default="fastino/gliner2-base-v1")
    parser.add_argument("--device", default="cuda:0")
    parser.add_argument("--warmups", type=int, default=3)
    parser.add_argument("--repeats", type=int, default=10)
    text_source = parser.add_mutually_exclusive_group()
    text_source.add_argument("--text")
    text_source.add_argument("--text-file")
    parser.add_argument("--text-repeat", type=int, default=1)
    parser.add_argument("--label", action="append", dest="labels")
    parser.add_argument("--expect-encoder-seq-len", type=int)
    parser.add_argument("--mode", choices=("eager", "compiled", "both"), default="both")
    parser.add_argument("--require-device-name", default="NVIDIA L4")
    args = parser.parse_args()
    if args.text_repeat < 1:
        raise RuntimeError("--text-repeat must be >= 1")
    if args.text_file:
        with open(args.text_file, "r", encoding="utf-8") as fixture_file:
            source_text = fixture_file.read().strip()
        if not source_text:
            raise RuntimeError("--text-file is empty")
    else:
        source_text = args.text or TEXT
    text = " ".join([source_text] * args.text_repeat)
    labels = args.labels or LABELS

    # Keep the Fastino reference on its normal torch/Inductor CUDA path.
    os.environ.pop("USE_FLASHDEBERTA", None)
    if not torch.cuda.is_available():
        raise RuntimeError("CUDA is required for the Fastino reference benchmark")
    device = torch.device(args.device)
    torch.cuda.set_device(device)
    device_name = torch.cuda.get_device_name(device)
    if args.require_device_name and device_name != args.require_device_name:
        raise RuntimeError(f"expected GPU {args.require_device_name!r}, got {device_name!r}")

    model = GLiNER2.from_pretrained(args.model, map_location=device, quantize=True)
    assert_cuda_fp16_model(model, device)
    encoder_sequence_length = verify_forward_residency(
        model,
        device,
        text,
        labels,
        args.expect_encoder_seq_len,
    )
    torch.cuda.synchronize(device)

    report: dict[str, object] = {
        "runtime": "fastino-gliner2",
        "model": args.model,
        "torch": torch.__version__,
        "torch_cuda": torch.version.cuda,
        "device": device_name,
        "device_index": device.index,
        "model_dtype": "float16",
        "input_device": str(device),
        "flashdeberta": False,
        "encoder_sequence_length": encoder_sequence_length,
        "batches": {},
    }

    if args.mode in ("eager", "both"):
        report["batches"]["eager_fp16"] = [
            measure(model, text, labels, batch, args.warmups, args.repeats, device) for batch in (1, 8)
        ]
    if args.mode in ("compiled", "both"):
        compile_start = time.perf_counter_ns()
        model.compile()
        model.batch_extract_entities([text], labels, batch_size=1)
        torch.cuda.synchronize(device)
        report["compile_first_ms"] = (time.perf_counter_ns() - compile_start) / 1_000_000.0
        report["batches"]["torch_compile_fp16"] = [
            measure(model, text, labels, batch, args.warmups, args.repeats, device) for batch in (1, 8)
        ]
    print(json.dumps(report, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
