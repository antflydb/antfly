#!/usr/bin/env python3
"""Pinned Fastino device worker for the direct-core Metal and CUDA comparisons.

Only this worker imports Torch. The parent owns process/RSS/deadline limits.
The existing CPU helper defines extraction, schema compilation, canonical
outputs, and temporary-lifetime semantics; it and the oracle stay unchanged.
MPS operator fallback is forbidden. Explicit upstream host decoding is part
of the synchronized full-extraction clock, including its device transfers.
"""

from __future__ import annotations

import argparse
import contextlib
import hashlib
import importlib.metadata
import json
import math
import os
from pathlib import Path
import re
import sys
import time
from typing import Any, Callable
import warnings

import benchmark_cpu as bench
import oracle


SCOPE = "gliner25_direct_core_metal_comparison_fp32_v1"
TIMING_BOUNDARY = bench.TIMING_BOUNDARY
MATH_POLICY = "pytorch_fp32_deterministic_no_mps_fallback_no_fast_math_v1"
SYNC_POLICIES = {
    "mps": "torch_mps_synchronize_before_start_and_after_extract_v1",
    "cpu": "synchronous_cpu_v1",
    "cuda": "torch_cuda_synchronize_before_start_and_after_extract_v1",
}
MPS_ENV = ("PYTORCH_ENABLE_MPS_FALLBACK", "PYTORCH_MPS_FAST_MATH")
MAX_COMMAND_BYTES = 2048
MAX_REQUEST_BYTES = 64 * 1024
MAX_ERROR_CHARS = 4096
# v2.9.1 MPSFallback.mm emits this even for its unconditionally registered
# embedding_renorm_/linalg_svd fallbacks. The environment switch alone is not
# sufficient. Match this warning, not ordinary eager-attention/tokenizer logs.
FALLBACK_WARNING = r"(?s).*(?:will fall back to run on the CPU|MPS.*(?:falling back|fallback|fall back).*CPU).*"


class WorkerError(bench.BenchmarkError):
    pass


class DeviceSynchronizationError(WorkerError):
    pass


def configure_environment(environ=None, modules=None) -> None:
    """Establish import-time MPS flags before any Torch registration runs."""
    environ = os.environ if environ is None else environ
    modules = sys.modules if modules is None else modules
    if any(name == "torch" or name.startswith("torch.") for name in modules):
        raise WorkerError(
            "Torch was imported before the worker established its MPS policy"
        )
    for name, expected in [(key, "0") for key in MPS_ENV] + [
        (key, "1") for key in bench.THREAD_ENV
    ]:
        if environ.get(name) not in (None, expected):
            raise WorkerError(
                f"worker environment disagrees with pinned policy: {name}"
            )
        environ[name] = expected


def check_environment() -> None:
    for name in MPS_ENV:
        if os.environ.get(name) != "0":
            raise WorkerError(f"MPS policy changed after import: {name}")
    for name in bench.THREAD_ENV:
        if os.environ.get(name) != "1":
            raise WorkerError(f"thread policy changed after import: {name}")


@contextlib.contextmanager
def reject_mps_fallback():
    with warnings.catch_warnings():
        warnings.filterwarnings("error", message=FALLBACK_WARNING, category=Warning)
        yield


def normalized_device(value: Any) -> str:
    value = str(value)
    if value in ("mps", "mps:0"):
        return "mps"
    if value in ("cuda", "cuda:0"):
        return "cuda"
    if value in ("cpu", "cpu:0"):
        return "cpu"
    raise WorkerError(f"unexpected tensor device: {value}")


def verify_runtime(torch: Any, device: str) -> None:
    check_environment()
    if (
        torch.get_num_threads() != 1
        or torch.get_num_interop_threads() != 1
        or torch.get_default_dtype() != torch.float32
        or torch.are_deterministic_algorithms_enabled() is not True
        or torch.is_deterministic_algorithms_warn_only_enabled() is not False
    ):
        raise WorkerError(
            "Torch thread/dtype/determinism profile differs from the oracle"
        )
    if device == "mps" and (
        not torch.backends.mps.is_built() or not torch.backends.mps.is_available()
    ):
        raise WorkerError("requested MPS backend is not built and available")

    if device == "cuda":
        if torch.version.cuda is None or not torch.cuda.is_available():
            raise WorkerError("requested CUDA runtime is not available")
        if (torch.backends.cuda.matmul.allow_tf32 or torch.backends.cudnn.allow_tf32
                or torch.get_float32_matmul_precision() != "highest"
                or os.environ.get("CUBLAS_WORKSPACE_CONFIG") != ":4096:8"
                or os.environ.get("TRITON_F32_DEFAULT") != "ieee"):
            raise WorkerError("CUDA strict FP32 math policy changed")


def verify_model_tensors(model: Any, torch: Any, device: str) -> dict[str, Any]:
    """Metadata-only validation; do not download or duplicate model weights."""
    if model.architecture != "boundary" or model.training is not False:
        raise WorkerError("worker requires an eval-mode boundary model")
    if model.strict_extraction is not True:
        raise WorkerError("strict_extraction must remain True")
    summary = {
        "parameters": 0,
        "parameter_elements": 0,
        "floating_buffers": 0,
        "other_buffers": 0,
        "parameter_device": device,
        "buffer_devices": [],
        "floating_dtype": "float32",
    }
    buffer_devices = set()
    for kind, tensors in (
        ("parameter", model.named_parameters()),
        ("buffer", model.named_buffers()),
    ):
        for name, tensor in tensors:
            actual = normalized_device(tensor.device)
            if actual != device:
                raise WorkerError(
                    f"{kind} {name}: device {actual} differs from {device}"
                )
            floating = tensor.is_floating_point()
            if tensor.is_complex() or (floating and tensor.dtype != torch.float32):
                raise WorkerError(f"{kind} {name}: expected FP32 floating tensors")
            if kind == "parameter":
                if not floating:
                    raise WorkerError(f"parameter {name}: expected FP32")
                summary["parameters"] += 1
                summary["parameter_elements"] += tensor.numel()
            else:
                buffer_devices.add(actual)
                summary["floating_buffers" if floating else "other_buffers"] += 1
    if summary["parameters"] == 0:
        raise WorkerError("model has no verifiable parameters")
    summary["buffer_devices"] = sorted(buffer_devices)
    return summary


def load_model(auto_extractor: Any, model_dir: Path, device: str, profile: str = "eager_fp32", flashdeberta: bool = False, compile_static: bool = False) -> Any:
    # The pinned loader strictly restores every checkpoint tensor on CPU first.
    # Do not use a global default device: source preprocessing/decoding has
    # deliberate host operations, while batch.to() follows model parameters.
    if flashdeberta and importlib.metadata.version("flashdeberta") != "0.0.7":
        raise WorkerError("FlashDeBERTa candidate requires flashdeberta==0.0.7")
    model = (
        auto_extractor.from_pretrained(
            str(model_dir.resolve()),
            local_files_only=True,
            map_location="cpu",
            quantize=False,
            compile=False,
            use_flashdeberta=flashdeberta,
        )
        .float()
        .eval()
        .to(device)
    )
    # This is a RuntimeMixin attribute, not an accepted extract() keyword.
    model.strict_extraction = True
    if flashdeberta and not type(model.encoder).__module__.startswith("flashdeberta."):
        raise WorkerError("requested FlashDeBERTa silently fell back to another encoder")
    if profile.startswith("compile_"):
        model.compile(dynamic=not compile_static)
    return model


def mps_memory(torch: Any, device: str) -> dict[str, int] | None:
    if device != "mps":
        return None
    result = {
        "current_allocated_bytes": torch.mps.current_allocated_memory(),
        "driver_allocated_bytes": torch.mps.driver_allocated_memory(),
        "recommended_max_bytes": torch.mps.recommended_max_memory(),
    }
    if any(type(value) is not int or value < 0 for value in result.values()):
        raise WorkerError("MPS allocator returned invalid byte counters")
    return result


def cuda_memory(torch: Any, device: str) -> dict[str, int] | None:
    if device != "cuda":
        return None
    return {
        "allocated_bytes": torch.cuda.memory_allocated(),
        "reserved_bytes": torch.cuda.memory_reserved(),
        "peak_allocated_bytes": torch.cuda.max_memory_allocated(),
        "peak_reserved_bytes": torch.cuda.max_memory_reserved(),
    }


def synchronize(torch: Any, device: str) -> None:
    if device == "cuda":
        try:
            torch.cuda.synchronize()
        except Exception as exc:
            raise DeviceSynchronizationError(f"CUDA synchronization failed: {exc}") from exc
    if device == "mps":
        try:
            torch.mps.synchronize()
        except Exception as exc:
            raise DeviceSynchronizationError(
                f"MPS synchronization failed: {exc}"
            ) from exc


def timed_call(
    torch: Any,
    device: str,
    execute: Callable[[], Any],
    *,
    clock: Callable[[], int] = time.perf_counter_ns,
) -> tuple[Any, int]:
    """Fence old work outside the clock and all extraction work before stop.

    Exceptions never become timing samples. Before continuing a recoverable
    case, drain its submitted work; a failed drain makes the device unsafe.
    No cache emptying, GC, canonicalization, or allocator telemetry is timed.
    """
    with torch.inference_mode():
        synchronize(torch, device)
        started = clock()
        try:
            output = execute()
        except Exception as exc:
            if error_details(exc, device)["recoverable"]:
                synchronize(torch, device)
            raise
        synchronize(torch, device)
        duration = clock() - started
    if type(duration) is not int or duration <= 0:
        raise WorkerError("worker clock did not advance")
    return output, duration


@contextlib.contextmanager
def capture_validation_inputs(model: Any, torch: Any, device: str, batch_size: int = 1):
    """Validate real encoder devices before the existing token-ID readback."""
    devices = []
    batch_ids = []

    def capture(_module, positional, keyword):
        ids = keyword.get("input_ids")
        if ids is None and positional:
            ids = positional[0]
        if not torch.is_tensor(ids):
            raise WorkerError("validation encoder has no tensor input IDs")
        if batch_size > 1:
            rows = ids.detach().cpu().tolist()
            if (len(rows) != batch_size or not rows or not isinstance(rows[0], list)
                    or not 1 <= len(rows[0]) <= oracle.MAX_ENCODED_TOKENS
                    or any(not isinstance(row, list) or len(row) != len(rows[0])
                           or any(type(token) is not int for token in row) for row in rows)):
                raise WorkerError("validation encoder batch shape differs")
            batch_ids.append([token for row in rows for token in row])
        observed = {}

        def visit(value, path, depth=0):
            if depth > 8 or len(observed) > 128:
                raise WorkerError("encoder input metadata exceeds validation bounds")
            if torch.is_tensor(value):
                actual = normalized_device(value.device)
                if actual != device:
                    raise WorkerError(
                        f"encoder input {path}: device {actual} differs from {device}"
                    )
                if value.is_complex() or (
                    value.is_floating_point() and value.dtype != torch.float32
                ):
                    raise WorkerError(
                        f"encoder input {path}: floating dtype differs from FP32"
                    )
                observed[path] = actual
            elif isinstance(value, (tuple, list)):
                for index, item in enumerate(value):
                    visit(item, f"{path}[{index}]", depth + 1)
            elif isinstance(value, dict):
                for name, item in value.items():
                    visit(item, f"{path}.{name}", depth + 1)

        visit(positional, "args")
        visit(keyword, "kwargs")
        devices.append(
            {
                "input_device": normalized_device(ids.device),
                "encoder_shape": list(ids.shape),
                "encoder_input_devices": observed,
            }
        )

    handle = model.encoder.register_forward_pre_hook(capture, with_kwargs=True)
    try:
        with (bench.capture_encoder_input_ids(model) if batch_size == 1 else contextlib.nullcontext(batch_ids)) as ids:
            yield ids, devices
    finally:
        handle.remove()


def finite_output(value: Any, path: str = "output") -> None:
    if isinstance(value, dict):
        for key, item in value.items():
            if not isinstance(key, str):
                raise WorkerError(f"{path}: non-string output key")
            finite_output(item, f"{path}.{key}")
    elif isinstance(value, list):
        for index, item in enumerate(value):
            finite_output(item, f"{path}[{index}]")
    elif isinstance(value, float):
        if not math.isfinite(value):
            raise WorkerError(f"{path}: non-finite canonical output")
    elif value is not None and type(value) not in (str, bool, int):
        raise WorkerError(f"{path}: non-JSON canonical output")


def error_details(exc: Exception, device: str) -> dict[str, Any]:
    message = str(exc)
    lower = message.lower()
    unsafe = False
    recoverable = True
    if isinstance(exc, Warning) and re.match(FALLBACK_WARNING, message, re.IGNORECASE):
        category = "mps_cpu_fallback"
        # TORCH_WARN_ONCE can suppress a later warning. Never reuse this worker.
        recoverable = False
    elif isinstance(exc, DeviceSynchronizationError):
        category, unsafe, recoverable = "device_synchronization", True, False
    elif isinstance(exc, MemoryError) or any(
        word in lower for word in ("out of memory", "outofmemory", "bad allocation")
    ):
        category, unsafe, recoverable = "out_of_memory", True, False
    elif "cuda" in lower or any(
        word in lower
        for word in ("command buffer", "device lost", "gpu fault", "gpu hang")
    ):
        category, unsafe, recoverable = "device_failure", True, False
    elif isinstance(exc, NotImplementedError) or any(
        word in lower
        for word in (
            "not implemented for",
            "not currently implemented",
            "not currently supported",
            "does not have a deterministic implementation",
            "doesn't support float64",
        )
    ):
        category = "unsupported_operation"
    elif (
        device == "mps"
        and isinstance(exc, RuntimeError)
        and any(word in lower for word in ("mps", "metal", "gpu"))
    ):
        category, unsafe, recoverable = "device_failure", True, False
    elif isinstance(exc, (WorkerError, bench.BenchmarkError, oracle.ContractError)):
        category, recoverable = "contract", False
    else:
        category = "extraction"
    return {
        "category": category,
        "error_type": type(exc).__name__,
        "message": message[:MAX_ERROR_CHARS],
        "recoverable": recoverable,
        "device_unsafe": unsafe,
    }


def read_requests(path: Path) -> tuple[list[dict[str, Any]], str]:
    with path.open("rb") as source:
        raw = source.read(MAX_REQUEST_BYTES + 1)
    if len(raw) > MAX_REQUEST_BYTES:
        raise WorkerError("fixed requests exceed byte limit")
    parsed = bench.strict_json(raw)
    if (
        not isinstance(parsed, dict)
        or set(parsed) != {"format_version", "requests"}
        or parsed["format_version"] != 1
    ):
        raise WorkerError("unsupported fixed request envelope")
    requests = parsed["requests"]
    if not isinstance(requests, list) or len(requests) != 10:
        raise WorkerError("worker requires the fixed ten task requests")
    seen = set()
    for request in requests:
        if (
            not isinstance(request, dict)
            or set(request) != {"id", "kind", "text", "schema"}
            or not isinstance(request["id"], str)
            or not request["id"]
            or request["id"] in seen
            or request["kind"] not in ("extract", "classification", "joint_ie")
            or not isinstance(request["text"], str)
            or not isinstance(request["schema"], dict)
        ):
            raise WorkerError("invalid fixed task request")
        seen.add(request["id"])
    return requests, hashlib.sha256(raw).hexdigest()


def ready_event(args, model, torch, bundle, provenance, requests_sha256):
    verify_runtime(torch, args.device)
    summary = verify_model_tensors(model, torch, args.device)
    provenance = {
        **provenance,
        "device": summary["parameter_device"],
        "dtype": "float32",
        "threads": 1,
        "interop_threads": 1,
        "deterministic_algorithms": True,
        "mps_environment": {key: os.environ[key] for key in MPS_ENV},
    }
    return {
        "event": "ready",
        "batch_size": getattr(args, "batch_size", 1),
        "arm": f"fastino_{args.device}",
        "scope": "gliner25_direct_core_cuda_comparison_v1" if args.device == "cuda" else SCOPE,
        "timing_boundary": TIMING_BOUNDARY,
        "model": args.model,
        "model_id": bundle["model_id"],
        "revision": bundle["revision"],
        "model_files": bundle["files"],
        "requests_sha256": requests_sha256,
        "dtype": "float32",
        "device": summary["parameter_device"],
        "parameter_device": summary["parameter_device"],
        "floating_dtype": "float32",
        "model_tensor_summary": summary,
        "threads": 1,
        "interop_threads": 1,
        "deterministic_algorithms": True,
        "strict_extraction": True,
        "provenance": provenance,
        "qualification": False,
        "synchronization_policy": SYNC_POLICIES[args.device],
        "math_policy": (f"pytorch_cuda_amp_{args.profile.rsplit('_', 1)[-1]}_no_tf32_v1" if "_amp_" in args.profile else "pytorch_cuda_fp32_deterministic_no_tf32_v1") if args.device == "cuda" else MATH_POLICY,
        **({"profile": args.profile, "activation_dtype": "bfloat16_autocast" if "bf16" in args.profile else "float16_autocast" if "fp16" in args.profile else "float32",
            "compile_static": args.compile_static,
            "encoder_backend": "flashdeberta" if args.flashdeberta else "transformers",
            "flashdeberta_version": importlib.metadata.version("flashdeberta") if args.flashdeberta else None} if args.device == "cuda" else {}),
        "mps_memory": mps_memory(torch, args.device),
        **({"cuda_memory": cuda_memory(torch, args.device), "cuda_runtime": {
            "torch_cuda": torch.version.cuda,
            "device_name": torch.cuda.get_device_name(),
            "compute_capability": list(torch.cuda.get_device_capability()),
            "tf32": False,
            "triton_f32_default": os.environ["TRITON_F32_DEFAULT"],
            "cublas_workspace_config": os.environ["CUBLAS_WORKSPACE_CONFIG"],
        }} if args.device == "cuda" else {}),
    }


def execute_batch(model, request, schema_json, batch_size):
    """One real upstream batch; schema compilation and preprocessing are timed."""
    if request["kind"] != "extract":
        raise WorkerError("CUDA batch benchmark requires an extract fixture")
    return model.batch_extract(
        [request["text"]] * batch_size,
        oracle.build_extract_schema(bench.strict_json(schema_json)),
        batch_size=batch_size, num_workers=0, threshold=0.5,
        include_confidence=True, include_spans=True, max_len=oracle.MAX_WORDS,
    )


def serve_commands(
    args,
    model,
    torch,
    bundle,
    requests,
    requests_sha256,
    *,
    source=None,
    emit=None,
    execute=None,
    canonical=None,
    clock=time.perf_counter_ns,
) -> int:
    source = sys.stdin.buffer if source is None else source
    emit = bench.emit if emit is None else emit
    batch_size = getattr(args, "batch_size", 1)
    execute = (bench.execute_python if batch_size == 1 else
               lambda model, request, schema: execute_batch(model, request, schema, batch_size)) if execute is None else execute
    canonical = bench.canonical_python if canonical is None else canonical
    by_id = {request["id"]: request for request in requests}
    schemas = {
        request["id"]: json.dumps(request["schema"], ensure_ascii=False)
        for request in requests
    }
    validated = set()
    previous_id = count = 0
    arm = f"fastino_{args.device}"
    while line := source.readline(MAX_COMMAND_BYTES + 1):
        if (
            len(line) > MAX_COMMAND_BYTES
            or not line.endswith(b"\n")
            or count >= args.max_commands
        ):
            raise WorkerError("worker command limit exceeded")
        count += 1
        command = bench.strict_json(line)
        if (
            not isinstance(command, dict)
            or not {"op", "request_id"} <= command.keys()
            or not command.keys() <= {"op", "request_id", "case_id"}
            or type(command["request_id"]) is not int
            or command["request_id"] <= previous_id
        ):
            raise WorkerError("invalid worker command identity")
        previous_id = command["request_id"]
        if command["op"] == "stop":
            if command.get("case_id", "") != "":
                raise WorkerError("stop command cannot select a case")
            if oracle.verify_model_dir(args.model, args.model_dir) != bundle:
                raise WorkerError("model bundle changed during benchmark")
            oracle.verify_upstream_checkout(args.upstream)
            if read_requests(oracle.FIXTURES / "requests.json")[1] != requests_sha256:
                raise WorkerError("fixed requests changed during benchmark")
            verify_runtime(torch, args.device)
            verify_model_tensors(model, torch, args.device)
            emit(
                {
                    "event": "stopped",
                    "arm": arm,
                    "request_id": previous_id,
                    "scope": "gliner25_direct_core_cuda_comparison_v1" if args.device == "cuda" else SCOPE,
                    "qualification": False,
                    "requests_sha256": requests_sha256,
                }
            )
            return 0
        case_id = command.get("case_id")
        if (
            command["op"] not in ("validate", "run")
            or not isinstance(case_id, str)
            or case_id not in by_id
        ):
            raise WorkerError("unknown benchmark command")
        request = by_id[case_id]
        try:
            verify_runtime(torch, args.device)
            if model.strict_extraction is not True:
                raise WorkerError("strict_extraction changed during benchmark")
            validating = command["op"] == "validate"
            if validating:
                verify_model_tensors(model, torch, args.device)
                validated.discard(case_id)
            elif case_id not in validated:
                raise WorkerError(
                    "run requires successful input-device validation for this case"
                )
            capture = (
                capture_validation_inputs(model, torch, args.device, batch_size)
                if validating
                else contextlib.nullcontext(([], []))
            )
            autocast = torch.autocast("cuda", dtype=torch.bfloat16 if "bf16" in args.profile else torch.float16) if "_amp_" in args.profile else contextlib.nullcontext()
            with reject_mps_fallback(), capture as (input_ids, inputs), autocast:
                output, duration = timed_call(
                    torch,
                    args.device,
                    lambda: execute(model, request, schemas[case_id]),
                    clock=clock,
                )
            if validating and (len(input_ids) != 1 or len(inputs) != 1):
                raise WorkerError(
                    f"{case_id}: validation requires exactly one encoder execution"
                )
            outputs = None
            if batch_size > 1:
                if not isinstance(output, list) or len(output) != batch_size:
                    raise WorkerError("Python batch output count differs")
                outputs = [canonical(request, item) for item in output]
                finite_output(outputs)
                output = outputs[0]
            else:
                output = canonical(request, output)
            finite_output(output)
            memory = mps_memory(
                torch, args.device
            )  # deliberately outside both clock reads
            if validating:
                validated.add(case_id)
            emit(
                {
                    "event": "result",
                    "arm": arm,
                    "request_id": previous_id,
                    "case_id": case_id,
                    "duration_ns": duration,
                    "batch_size": batch_size,
                    "encoder_shape": inputs[0]["encoder_shape"] if inputs else None,
                    "outputs": outputs,
                    "input_ids": input_ids[0] if input_ids else None,
                    "input_device": inputs[0]["input_device"] if inputs else None,
                    "encoder_input_devices": inputs[0]["encoder_input_devices"]
                    if inputs
                    else None,
                    "output": output,
                    "mps_memory": memory,
                    **({"cuda_memory": cuda_memory(torch, args.device)} if args.device == "cuda" else {}),
                }
            )
        except Exception as exc:
            details = error_details(exc, args.device)
            emit(
                {
                    "event": "error",
                    "arm": arm,
                    "request_id": previous_id,
                    "case_id": case_id,
                    **details,
                }
            )
            if not details["recoverable"]:
                return 1
    raise WorkerError("protocol ended without explicit stop")


def python_worker(args) -> int:
    if args.device not in SYNC_POLICIES or not 1 <= args.max_commands <= 4096:
        raise WorkerError("worker profile or command limit is invalid")
    configure_environment()
    # This guard also catches fallback during loading, before ready is emitted.
    with reject_mps_fallback():
        if args.device == "cuda":
            # PyTorch's flags do not govern FlashDeBERTa's default tl.dot.
            # Establish Triton's IEEE default before its first import/JIT too.
            for name, value in (("CUBLAS_WORKSPACE_CONFIG", ":4096:8"), ("TRITON_F32_DEFAULT", "ieee")):
                if os.environ.get(name) not in (None, value):
                    raise WorkerError(f"{name} differs from strict CUDA policy")
                os.environ[name] = value
        provenance, torch = oracle.prepare_runtime(args.upstream)
        if args.device == "cuda":
            torch.backends.cuda.matmul.allow_tf32 = False
            torch.backends.cudnn.allow_tf32 = False
            torch.set_float32_matmul_precision("highest")
        torch.set_num_interop_threads(1)
        verify_runtime(torch, args.device)
        bundle = oracle.verify_model_dir(args.model, args.model_dir)
        requests, requests_sha256 = read_requests(oracle.FIXTURES / "requests.json")
        from gliner2 import AutoExtractor

        # Reinstall after third-party imports, which may add warning filters.
        with reject_mps_fallback():
            model = load_model(AutoExtractor, args.model_dir, args.device, args.profile, args.flashdeberta, args.compile_static)
            synchronize(torch, args.device)
            bench.emit(
                ready_event(args, model, torch, bundle, provenance, requests_sha256)
            )
        return serve_commands(args, model, torch, bundle, requests, requests_sha256)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", choices=("eager_fp32", "compile_fp32", "eager_amp_bf16", "compile_amp_bf16", "eager_amp_fp16", "compile_amp_fp16"), default="eager_fp32")
    parser.add_argument("--flashdeberta", action="store_true")
    parser.add_argument("--compile-static", action="store_true")
    parser.add_argument("--device", choices=("mps", "cpu", "cuda"), required=True)
    parser.add_argument("--model", choices=("small", "base", "multi"), required=True)
    parser.add_argument("--model-dir", type=Path, required=True)
    parser.add_argument("--upstream", type=Path, required=True)
    parser.add_argument("--batch-size", type=int, default=1)
    parser.add_argument("--max-commands", type=int, default=2048)
    args = parser.parse_args(argv)
    if not 1 <= args.batch_size <= 64 or (args.device != "cuda" and args.batch_size != 1):
        parser.error("batch size must be 1..64 and larger batches require CUDA")
    if args.device != "cuda" and (args.profile != "eager_fp32" or args.flashdeberta or args.compile_static):
        parser.error("additional profiles require CUDA")
    if args.compile_static and not args.profile.startswith("compile_"):
        parser.error("static shapes require a compiled profile")
    return args


def main(argv=None) -> int:
    try:
        return python_worker(parse_args(argv))
    except Exception as exc:
        print(
            f"{type(exc).__name__}: {str(exc)[:MAX_ERROR_CHARS]}",
            file=sys.stderr,
            flush=True,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
