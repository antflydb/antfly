#!/usr/bin/env python3
"""Benchmark pinned upstream GLiNER2.5 family artifacts on CPU or MPS.

The clock covers schema construction/compilation, preprocessing, encoder and
heads, and upstream decoding. Model loading, artifact/source/runtime checks,
prepared-token validation, result validation, and report serialization are
outside the clock. MPS is synchronized immediately before and after each timed
call and PyTorch CPU operator fallback is forbidden.
"""
from __future__ import annotations

import argparse
import contextlib
import datetime as dt
import hashlib
import importlib.metadata
import json
import math
import os
from pathlib import Path
import platform
import statistics
import sys
import tempfile
import time
from typing import Any, Callable
import warnings


HERE = Path(__file__).resolve().parent
TESTDATA = HERE.parents[1] / "testdata" / "gliner25" / "family"
CONTRACT = HERE / "family_contract.json"
RUNTIME_CONTRACT_1B = HERE / "decide_1b_oracle_runtime.json"
DEFAULT_RUNTIME_1B = Path("/private/tmp/antfly-gliner-transformers-5.17-py312")
WARMUPS = 3
SAMPLES = 20
THREADS = 2
MAX_WORDS = 4096
OUTPUT_TOLERANCE = 5e-4
RAW_LOGIT_TOLERANCE = 2e-3
MPS_FALLBACK_WARNING = (
    r"(?s).*(?:will fall back to run on the CPU|"
    r"MPS.*(?:falling back|fallback|fall back).*CPU).*"
)
THREAD_ENV = (
    "OMP_NUM_THREADS",
    "OPENBLAS_NUM_THREADS",
    "MKL_NUM_THREADS",
    "VECLIB_MAXIMUM_THREADS",
    "BLIS_NUM_THREADS",
)
MPS_ENV_UNSET = (
    "PYTORCH_MPS_PREFER_METAL",
    "PYTORCH_MPS_HIGH_WATERMARK_RATIO",
    "PYTORCH_MPS_LOW_WATERMARK_RATIO",
    "PYTORCH_MPS_LOG_PROFILE_INFO",
    "PYTORCH_MPS_TRACE_SIGNPOSTS",
    "PYTORCH_DEBUG_MPS_ALLOCATOR",
)
CAPTURES = {
    ("multi_v1", "extract"): (
        "multi_v1_capture.json",
        "602e6e4470163308de6b139ae829b11321adfdcb8371d64e6a3266dea431733b",
        "requests",
        ("spanish_entities",),
    ),
    ("multi_v1", "decide"): (
        "multi_v1_decide_capture.json",
        "ea52610f882b9607a97e0ac2dbb918a57b22d893eae9a9257ddbd6e3ca7a2957",
        "requests",
        ("described_prompt_choice", "choice_score_noul"),
    ),
    ("multi_decide", "extract"): (
        "multi_decide_capture.json",
        "6b582e356c06d9f37e11b68b35707d7995df431f2af3e6fb0e7265f8a5d3bec0",
        "requests",
        ("spanish_entities",),
    ),
    ("multi_decide", "decide"): (
        "multi_decide_decide_capture.json",
        "b435f0fb3d638b27828b949235e9a9c2b35e1171aa55ef94263cf53edb589900",
        "requests",
        ("described_prompt_choice", "choice_score_noul"),
    ),
    ("decide_1b", "decide"): (
        "decide_1b_capture.json",
        "45828bb5e2d00812299d2a2b778d37a215bef231b821d74791a1d9d40335d34b",
        "public_decide_requests",
        ("described_prompt_choice", "choice_score_noul"),
    ),
}


class BenchmarkError(RuntimeError):
    pass


class UnsupportedBenchmark(BenchmarkError):
    pass


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def strict_json(path: Path) -> dict[str, Any]:
    def reject(value: str) -> None:
        raise BenchmarkError(f"non-finite JSON constant: {value}")

    value = json.loads(path.read_text(encoding="utf-8"), parse_constant=reject)
    if not isinstance(value, dict):
        raise BenchmarkError(f"expected JSON object: {path}")
    return value


def configure_environment() -> None:
    if any(name == "torch" or name.startswith("torch.") for name in sys.modules):
        raise BenchmarkError("Torch was imported before benchmark policy setup")
    for name in THREAD_ENV:
        inherited = os.environ.get(name)
        if inherited not in (None, str(THREADS)):
            raise BenchmarkError(f"thread environment differs: {name}={inherited!r}")
        os.environ[name] = str(THREADS)
    for name, value in {
        "PYTORCH_ENABLE_MPS_FALLBACK": "0",
        "PYTORCH_MPS_FAST_MATH": "0",
        "TOKENIZERS_PARALLELISM": "false",
        "HF_HUB_OFFLINE": "1",
        "TRANSFORMERS_OFFLINE": "1",
    }.items():
        inherited = os.environ.get(name)
        if inherited not in (None, value):
            raise BenchmarkError(f"runtime environment differs: {name}={inherited!r}")
        os.environ[name] = value
    for name in MPS_ENV_UNSET:
        if name in os.environ:
            raise BenchmarkError(f"MPS runtime override must be unset: {name}")
    os.environ.pop("USE_FLASHDEBERTA", None)
    sys.dont_write_bytecode = True


def percentile95(values: list[int]) -> int:
    return sorted(values)[math.ceil(0.95 * len(values)) - 1]


def summary(values: list[int]) -> dict[str, Any]:
    if len(values) != SAMPLES or any(type(value) is not int or value <= 0 for value in values):
        raise BenchmarkError("invalid timing sample inventory")
    return {
        "samples_ns": values,
        "mean_ms": statistics.mean(values) / 1e6,
        "median_ms": statistics.median(values) / 1e6,
        "p95_ms": percentile95(values) / 1e6,
        "serial_rps": 1e9 / statistics.mean(values),
    }


def synchronize(torch: Any, device: str) -> None:
    if device == "mps":
        try:
            torch.mps.synchronize()
        except Exception as exc:
            raise UnsupportedBenchmark(f"MPS synchronization failed: {exc}") from exc


@contextlib.contextmanager
def reject_mps_fallback(device: str):
    if device != "mps":
        yield
        return
    with warnings.catch_warnings():
        warnings.filterwarnings("error", message=MPS_FALLBACK_WARNING, category=Warning)
        yield


def timed(torch: Any, device: str, call: Callable[[], Any]) -> tuple[Any, int]:
    with torch.inference_mode(), reject_mps_fallback(device):
        synchronize(torch, device)
        started = time.perf_counter_ns()
        try:
            result = call()
            synchronize(torch, device)
        except (NotImplementedError, Warning) as exc:
            raise UnsupportedBenchmark(f"{device} operation is unsupported: {exc}") from exc
        except RuntimeError as exc:
            message = str(exc).lower()
            if device == "mps" and any(word in message for word in ("mps", "metal", "not implemented")):
                raise UnsupportedBenchmark(f"MPS execution is unsupported: {exc}") from exc
            raise
        elapsed = time.perf_counter_ns() - started
    if elapsed <= 0:
        raise BenchmarkError("monotonic benchmark clock did not advance")
    return result, elapsed


def compare(
    expected: Any,
    actual: Any,
    path: str = "output",
    tolerance: float = OUTPUT_TOLERANCE,
) -> None:
    if isinstance(expected, dict):
        if not isinstance(actual, dict) or expected.keys() != actual.keys():
            raise BenchmarkError(f"{path}: object keys differ")
        for key in expected:
            compare(expected[key], actual[key], f"{path}.{key}", tolerance)
    elif isinstance(expected, list):
        if not isinstance(actual, list) or len(expected) != len(actual):
            raise BenchmarkError(f"{path}: array length differs")
        for index, (left, right) in enumerate(zip(expected, actual, strict=True)):
            compare(left, right, f"{path}[{index}]", tolerance)
    elif isinstance(expected, float):
        if isinstance(actual, bool) or not isinstance(actual, (int, float)) or not math.isfinite(actual):
            raise BenchmarkError(f"{path}: expected finite number")
        if abs(expected - actual) > tolerance:
            raise BenchmarkError(f"{path}: numeric value differs: {expected!r} != {actual!r}")
    elif type(expected) is not type(actual) or expected != actual:
        raise BenchmarkError(f"{path}: value differs: {expected!r} != {actual!r}")


def verify_model_device(model: Any, torch: Any, device: str) -> dict[str, int]:
    parameters = elements = buffers = 0
    for kind, inventory in (
        ("parameter", model.named_parameters()),
        ("buffer", model.named_buffers()),
    ):
        for name, tensor in inventory:
            if str(tensor.device).split(":", 1)[0] != device:
                raise BenchmarkError(f"{kind} {name} is on {tensor.device}, expected {device}")
            if tensor.is_complex() or (tensor.is_floating_point() and tensor.dtype != torch.float32):
                raise BenchmarkError(f"{kind} {name} has an unsupported dtype")
            if kind == "parameter":
                if not tensor.is_floating_point():
                    raise BenchmarkError(f"parameter {name} is not FP32")
                parameters += 1
                elements += tensor.numel()
            else:
                buffers += 1
    if parameters == 0:
        raise BenchmarkError("model exposes no parameters")
    return {"parameters": parameters, "parameter_elements": elements, "buffers": buffers}


def load_capture(profile: str, task: str) -> tuple[Path, dict[str, Any], list[dict[str, Any]]]:
    try:
        filename, pin, member, ids = CAPTURES[(profile, task)]
    except KeyError as exc:
        raise UnsupportedBenchmark(f"{profile} does not support the {task} benchmark") from exc
    path = TESTDATA / filename
    if sha256(path) != pin:
        raise BenchmarkError(f"pinned capture differs: {path}")
    capture = strict_json(path)
    rows = capture.get(member)
    if not isinstance(rows, list):
        raise BenchmarkError("capture request inventory is absent")
    by_id = {row.get("id"): row for row in rows if isinstance(row, dict)}
    if len(by_id) != len(rows) or any(request_id not in by_id for request_id in ids):
        raise BenchmarkError("capture request identities differ")
    return path, capture, [by_id[request_id] for request_id in ids]


def ordered_mapping(source: Any, names: Any, path: str) -> dict[str, Any]:
    if not isinstance(source, dict) or not isinstance(names, list):
        raise BenchmarkError(f"{path}: ordered mapping evidence is absent")
    if (
        any(not isinstance(name, str) for name in names)
        or len(names) != len(set(names))
        or set(source) != set(names)
    ):
        raise BenchmarkError(f"{path}: ordered mapping inventory differs")
    return {name: source[name] for name in names}


def ordered_extract_schema(row: dict[str, Any]) -> dict[str, Any]:
    source = row.get("schema")
    native = row.get("native_schema")
    if not isinstance(source, dict) or not isinstance(native, dict):
        raise BenchmarkError(f"{row.get('id')}: extraction schema evidence is absent")
    schema = dict(source)
    schema["entities"] = ordered_mapping(
        source.get("entities"), native.get("entities"), f"{row.get('id')}.entities"
    )
    return schema


def ordered_decide_schema(row: dict[str, Any]) -> dict[str, Any]:
    source = row.get("schema")
    classification = row.get("native_classification")
    evidence = classification.get("tasks") if isinstance(classification, dict) else None
    if not isinstance(source, dict) or not isinstance(evidence, list):
        raise BenchmarkError(f"{row.get('id')}: classification schema evidence is absent")
    if any(not isinstance(task, dict) for task in evidence):
        raise BenchmarkError(f"{row.get('id')}: classification task evidence differs")
    task_names = [task.get("name") for task in evidence]
    tasks = ordered_mapping(source.get("tasks"), task_names, f"{row.get('id')}.tasks")
    ordered_tasks: dict[str, Any] = {}
    for task_evidence in evidence:
        name = task_evidence["name"]
        source_task = tasks[name]
        if not isinstance(source_task, dict):
            raise BenchmarkError(f"{row.get('id')}.tasks.{name}: task must be an object")
        task = dict(source_task)
        task["labels"] = ordered_mapping(
            source_task.get("labels"),
            task_evidence.get("labels"),
            f"{row.get('id')}.tasks.{name}.labels",
        )
        ordered_tasks[name] = task
    schema = dict(source)
    schema["tasks"] = ordered_tasks
    return schema


def prepare_extract(model: Any, row: dict[str, Any], common: Any) -> tuple[Callable[[], Any], Callable[[Any], None]]:
    from gliner2 import Schema

    schema = Schema.from_dict(ordered_extract_schema(row))
    encoded = common.encoded_evidence(model, row["text"], schema, boundary=True)
    if encoded["input_ids"] != row["encoded"]["input_ids"]:
        raise BenchmarkError(f"{row['id']}: prepared token IDs differ")

    def execute() -> Any:
        # Rebuild the schema inside every timed request, matching a fresh API call.
        return model.extract(
            row["text"],
            Schema.from_dict(ordered_extract_schema(row)),
            threshold=0.5,
            include_confidence=True,
            include_spans=True,
            max_len=MAX_WORDS,
        )

    def validate(output: Any) -> None:
        canonical = common.canonical_expected(
            row, row["native_schema"], common.jsonable(output)
        )
        compare(row["native_expected"], canonical)

    return execute, validate


def prepare_decide(model: Any, row: dict[str, Any], common: Any) -> tuple[Callable[[], Any], Callable[[Any], None]]:
    from gliner2.classification import Classifier, ClassificationConfig, ClassificationSchema

    initial = Classifier(model).compile_schema(ClassificationSchema.from_dict(ordered_decide_schema(row)))
    encoded = common.encoded_evidence(model, row["text"], initial, boundary=False)
    if encoded["input_ids"] != row["encoded"]["input_ids"]:
        raise BenchmarkError(f"{row['id']}: prepared token IDs differ")

    def execute() -> tuple[Any, Any, list[str]]:
        classifier = Classifier(model)
        compiled = classifier.compile_schema(ClassificationSchema.from_dict(ordered_decide_schema(row)))
        config = ClassificationConfig(
            on_infeasible="raise", max_len=MAX_WORDS, include_confidence=True
        )
        scores = classifier.score(row["text"], compiled, config=config)
        decoded = classifier.decode(scores, compiled, config=config)
        return scores, decoded, list(compiled.task_order)

    def validate(result: tuple[Any, Any, list[str]]) -> None:
        scores, decoded, order = result
        expected_tasks = row["native_classification"]["tasks"]
        if order != [task["name"] for task in expected_tasks]:
            raise BenchmarkError(f"{row['id']}: classification task order differs")
        for task in expected_tasks:
            actual = scores.tasks[task["name"]]
            if list(actual) != task["labels"]:
                raise BenchmarkError(f"{row['id']}: classification labels differ")
            compare(
                task["raw_logits"],
                list(actual.values()),
                f"{row['id']}.raw_logits",
                RAW_LOGIT_TOLERANCE,
            )
        selected = {name: list(decoded.selected(name)) for name in order}
        compare(row["selected"], selected, f"{row['id']}.selected")
        probabilities = {
            name: {label: scores.probability(name, label) for label in scores.tasks[name]}
            for name in order
        }
        compare(row["probabilities"], probabilities, f"{row['id']}.probabilities")

    return execute, validate


def benchmark_case(torch: Any, device: str, row: dict[str, Any], prepare: Callable[[dict[str, Any]], tuple[Callable[[], Any], Callable[[Any], None]]]) -> dict[str, Any]:
    execute, validate = prepare(row)
    for _ in range(WARMUPS):
        output, _ = timed(torch, device, execute)
        validate(output)
        del output
    samples: list[int] = []
    for _ in range(SAMPLES):
        output, elapsed = timed(torch, device, execute)
        validate(output)
        samples.append(elapsed)
        del output
    return {
        "case_id": row["id"],
        "input_bytes": len(row["text"].encode()),
        "prepared_tokens": len(row["encoded"]["input_ids"]),
        **summary(samples),
    }


def select_prepare(
    task: str,
    extract: Callable[[dict[str, Any]], tuple[Callable[[], Any], Callable[[Any], None]]],
    decide: Callable[[dict[str, Any]], tuple[Callable[[], Any], Callable[[Any], None]]],
) -> Callable[[dict[str, Any]], tuple[Callable[[], Any], Callable[[Any], None]]]:
    if task == "extract":
        return extract
    if task == "decide":
        return decide
    raise BenchmarkError(f"unsupported benchmark task: {task}")


def atomic_write(path: Path, report: dict[str, Any]) -> None:
    path = path.expanduser().absolute()
    if path.exists() or path.is_symlink():
        raise BenchmarkError(f"refusing to overwrite output: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary_name = tempfile.mkstemp(prefix=f".{path.name}-", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(report, stream, ensure_ascii=False, allow_nan=False, sort_keys=True, indent=2)
            stream.write("\n")
        temporary.replace(path)
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise


def run(args: argparse.Namespace) -> dict[str, Any]:
    started_utc = dt.datetime.now(dt.timezone.utc).isoformat()
    configure_environment()
    capture_path, capture, rows = load_capture(args.profile, args.task)
    contract = strict_json(args.contract)

    # These helpers perform the same whole-artifact and pinned-source checks as
    # capture generation. Imports occur only after the runtime search path and
    # MPS fallback policy have been fixed.
    import capture_family_references as common
    import verify_family_contract as family

    source = common.verify_source(args.upstream, contract)
    model_before = family.verify_model(
        args.profile, args.model_dir, contract_path=args.contract, verify_model_sha256=True
    )
    if capture.get("model") != model_before:
        raise BenchmarkError("capture model identity differs from the selected artifact")

    runtime: dict[str, Any]
    if args.profile == "decide_1b":
        import capture_decide_1b_references as one_b

        runtime_contract = strict_json(args.runtime_contract)
        runtime = one_b.verify_runtime_dir(args.runtime_dir, runtime_contract)
        one_b.activate(args.runtime_dir, args.upstream)
    else:
        runtime = common.runtime_identity(contract)
        sys.path.insert(0, str(args.upstream.resolve()))

    import torch
    import transformers
    import gliner2
    from gliner2 import AutoExtractor

    if gliner2.__version__ != "2.0.0":
        raise BenchmarkError(f"unexpected GLiNER2 version: {gliner2.__version__}")
    expected_transformers = "5.17.0" if args.profile == "decide_1b" else "4.55.4"
    if transformers.__version__ != expected_transformers:
        raise BenchmarkError(f"unexpected Transformers version: {transformers.__version__}")
    # Match the frozen capture order: Transformers must inspect the real
    # environment before the serving-only PEFT type shim is installed. Span
    # Decide-1B does not need or install this boundary-training import shim.
    if args.profile != "decide_1b":
        common.install_inference_peft_shim()
    if args.device == "mps" and (
        not torch.backends.mps.is_built() or not torch.backends.mps.is_available()
    ):
        raise UnsupportedBenchmark("requested MPS backend is unavailable")
    torch.set_num_threads(THREADS)
    torch.set_num_interop_threads(1)
    torch.use_deterministic_algorithms(True)
    torch.set_default_dtype(torch.float32)
    torch.manual_seed(0)

    with reject_mps_fallback(args.device):
        model = AutoExtractor.from_pretrained(
            str(args.model_dir.resolve()),
            local_files_only=True,
            map_location="cpu",
            use_flashdeberta=False,
        ).float().eval().to(args.device)
    expected_architecture = "span" if args.profile == "decide_1b" else "boundary"
    if getattr(model, "architecture", None) != expected_architecture:
        raise BenchmarkError("loaded model architecture differs")
    if getattr(model, "training", None) is not False:
        raise BenchmarkError("loaded model is not in evaluation mode")
    tensor_summary = verify_model_device(model, torch, args.device)
    if args.profile == "decide_1b":
        rope = one_b.verify_rope(args.model_dir, runtime_contract, model.encoder)
    else:
        rope = None

    prepare = select_prepare(
        args.task,
        lambda row: prepare_extract(model, row, common),
        lambda row: prepare_decide(model, row, common),
    )
    cases = [benchmark_case(torch, args.device, row, prepare) for row in rows]
    synchronize(torch, args.device)

    model_after = family.verify_model(
        args.profile, args.model_dir, contract_path=args.contract, verify_model_sha256=True
    )
    if model_after != model_before:
        raise BenchmarkError("model artifact changed during benchmark")
    if common.verify_source(args.upstream, contract) != source:
        raise BenchmarkError("pinned source changed during benchmark")
    if sha256(capture_path) != CAPTURES[(args.profile, args.task)][1]:
        raise BenchmarkError("capture changed during benchmark")

    imports = {
        name: str(Path(module.__file__).resolve())
        for name, module in sys.modules.items()
        if (name == "gliner2" or name.startswith("gliner2."))
        and getattr(module, "__file__", None)
    }
    checkout = args.upstream.resolve()
    if not imports or any(not Path(path).is_relative_to(checkout) for path in imports.values()):
        raise BenchmarkError("GLiNER2 imported outside the pinned checkout")
    mps_memory = None
    if args.device == "mps":
        mps_memory = {
            "current_allocated_bytes": torch.mps.current_allocated_memory(),
            "driver_allocated_bytes": torch.mps.driver_allocated_memory(),
            "recommended_max_bytes": torch.mps.recommended_max_memory(),
        }
    report = {
        "schema": "antfly.gliner25_upstream_family_benchmark.v1",
        "qualification": False,
        "profile": args.profile,
        "task": args.task,
        "device": args.device,
        "warmups": WARMUPS,
        "measured_samples": SAMPLES,
        "timing_boundary": "schema_build_compile+preprocess+encoder+heads+upstream_decode",
        "timing_caveats": [
            "serial warm-model descriptive latency; no concurrent-load p95 claim",
            "model loading, pin checks, token validation, result validation, and serialization are excluded",
            "MPS is synchronized immediately before start and after decode",
            "Antfly Node handler measurements include additional routing and serialization",
        ],
        "model": model_before,
        "source": {**source, "package_version": gliner2.__version__, "imports": imports},
        "runtime": {
            **runtime,
            "python": platform.python_version(),
            "torch": torch.__version__,
            "transformers": transformers.__version__,
            "device": args.device,
            "dtype": "float32",
            "threads": torch.get_num_threads(),
            "interop_threads": torch.get_num_interop_threads(),
            "deterministic_algorithms": torch.are_deterministic_algorithms_enabled(),
            "mps_fallback": False,
            "mps_fast_math": False,
        },
        "capture": {
            "path": str(capture_path),
            "sha256": CAPTURES[(args.profile, args.task)][1],
        },
        "rope": rope,
        "model_tensors": tensor_summary,
        "mps_memory": mps_memory,
        "started_utc": started_utc,
        "completed_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "cases": cases,
    }
    atomic_write(args.output, report)
    return report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", required=True, choices=("multi_v1", "multi_decide", "decide_1b"))
    parser.add_argument("--device", required=True, choices=("cpu", "mps"))
    parser.add_argument("--task", required=True, choices=("extract", "decide"))
    parser.add_argument("--model-dir", required=True, type=Path)
    parser.add_argument("--upstream", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--threads", type=int, default=THREADS)
    parser.add_argument("--contract", type=Path, default=CONTRACT)
    parser.add_argument("--runtime-contract", type=Path, default=RUNTIME_CONTRACT_1B)
    parser.add_argument("--runtime-dir", type=Path, default=DEFAULT_RUNTIME_1B)
    args = parser.parse_args()
    if args.threads != THREADS:
        parser.error(f"this benchmark contract requires --threads {THREADS}")
    for label in ("model_dir", "upstream"):
        path = getattr(args, label).expanduser().resolve()
        if not path.is_dir():
            parser.error(f"--{label.replace('_', '-')} must be an existing directory")
        setattr(args, label, path)
    args.contract = args.contract.expanduser().resolve()
    args.runtime_contract = args.runtime_contract.expanduser().resolve()
    args.runtime_dir = args.runtime_dir.expanduser().resolve()
    return args


def main() -> int:
    try:
        args = parse_args()
        report = run(args)
        print(json.dumps({
            "status": "ok",
            "profile": report["profile"],
            "device": report["device"],
            "task": report["task"],
            "output": str(args.output.expanduser().absolute()),
        }, sort_keys=True, allow_nan=False))
        return 0
    except UnsupportedBenchmark as exc:
        print(json.dumps({"status": "unsupported", "error": str(exc)}, sort_keys=True), file=sys.stderr)
        return 2
    except (BenchmarkError, OSError, ValueError, KeyError) as exc:
        print(json.dumps({"status": "error", "error_type": type(exc).__name__, "error": str(exc)}, sort_keys=True), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
