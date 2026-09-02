#!/usr/bin/env python3
"""Qualify Gemma4 DPO/GRPO policy training on strict Zig CUDA.

This runner is deliberately dependency-free and does not download or convert
models.  It preflights mounted BF16 SafeTensors checkpoints, executes bounded
preference recipes, validates the resulting CUDA execution evidence and LoRA
movement, samples process GPU memory, and writes an atomic machine-readable
summary.  Benchmark mode compares the same cold-process protocol with a pinned
Python/Unsloth baseline report.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import pathlib
import re
import shutil
import statistics
import subprocess
import sys
import time
from dataclasses import asdict, dataclass
from typing import Any, Iterable


MAX_SAFETENSORS_HEADER_BYTES = 64 * 1024 * 1024
LABEL_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
TEXT_MLP_WEIGHT_RE = re.compile(
    r"(?:^|\.)layers\.(?P<layer>[0-9]+)\.mlp\."
    r"(?P<projection>gate_proj|up_proj|down_proj)\.weight$"
)
SUPPORTED_RANK2_DTYPES = {"BF16", "F32"}
SUMMARY_SCHEMA = "antfly_gemma4_cuda_preference_qualification/v1"
BASELINE_SCHEMA = "antfly_gemma4_unsloth_preference_benchmark/v1"
EXECUTION_EVIDENCE_SCHEMA = "antfly_training_execution_evidence/v1"
DEVICE_EXECUTION_SCOPE = "optimizer-steps-only;excludes-rollout-and-reference-scoring"
SMOKE_UPDATES = 1
BENCHMARK_UPDATES = 25
BENCHMARK_MIN_REPETITIONS = 3
DETERMINISTIC_CUDA_ENVIRONMENT = {
    "ANTFLY_INFERENCE_CUDA_CUBLASLT_BF16_TUNING_PROFILE": "off",
}
DPO_FIXTURE = {
    "prompt": "Answer briefly: what is the capital of France?",
    "chosen": "The capital is Paris.",
    "rejected": "The capital is Berlin.",
}
GRPO_PROMPT = "The"
BENCHMARK_GRPO_TARGET = "qualification-sequence-hash-v1"
DPO_BETA = 0.1
LEARNING_RATE = 1e-4
WEIGHT_DECAY = 0.01
ADAM_BETA1 = 0.9
ADAM_BETA2 = 0.999
ADAM_EPSILON = 1e-8
MAX_GRAD_NORM = 1.0
LORA_TARGET_MODULES = ["q_proj", "v_proj"]
LORA_DROPOUT = 0.0
GRPO_GROUP_SIZE = 4
GRPO_MAX_COMPLETION_TOKENS = 4
GRPO_CLIP_EPSILON = 0.2
GRPO_KL_COEF = 0.04
GRPO_ADVANTAGE_EPSILON = 1e-4


class QualificationError(RuntimeError):
    """A fail-closed qualification or comparison error."""


@dataclass(frozen=True)
class ModelSpec:
    label: str
    path: pathlib.Path


@dataclass(frozen=True)
class ModelPreflight:
    label: str
    path: str
    artifact_kind: str
    shard_count: int
    total_checkpoint_bytes: int
    tensor_count: int
    dtypes: list[str]
    rank2_dtypes: list[str]
    config_sha256: str
    topology: dict[str, bool | int | str | None]


@dataclass(frozen=True)
class ProcessMetrics:
    return_code: int
    wall_seconds: float
    peak_gpu_memory_mib: int
    gpu_samples: int


def finite_number(value: Any, name: str, *, positive: bool = False) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise QualificationError(f"{name} must be a finite number")
    converted = float(value)
    if not math.isfinite(converted) or (positive and converted <= 0.0):
        qualifier = "positive finite" if positive else "finite"
        raise QualificationError(f"{name} must be {qualifier}, found {value!r}")
    return converted


def nonnegative_int(mapping: dict[str, Any], name: str, *, namespace: str) -> int:
    value = mapping.get(name)
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise QualificationError(f"{namespace}.{name} must be a nonnegative integer")
    return value


def sha256_file(path: pathlib.Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def atomic_write_json(path: pathlib.Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp-{os.getpid()}")
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temporary.replace(path)


def parse_assignment(raw: str, option: str) -> tuple[str, str]:
    label, separator, value = raw.partition("=")
    if not separator or not LABEL_RE.fullmatch(label) or not value:
        raise QualificationError(f"{option} must use a safe LABEL=VALUE assignment: {raw!r}")
    return label, value


def parse_unique_assignments(values: Iterable[str], option: str) -> dict[str, str]:
    parsed: dict[str, str] = {}
    for raw in values:
        label, value = parse_assignment(raw, option)
        if label in parsed:
            raise QualificationError(f"duplicate {option} label: {label}")
        parsed[label] = value
    return parsed


def safetensors_header(path: pathlib.Path) -> dict[str, Any]:
    try:
        with path.open("rb") as source:
            size_raw = source.read(8)
            if len(size_raw) != 8:
                raise QualificationError(f"truncated SafeTensors header length: {path}")
            header_size = int.from_bytes(size_raw, "little")
            if header_size <= 0 or header_size > MAX_SAFETENSORS_HEADER_BYTES:
                raise QualificationError(f"invalid SafeTensors header size {header_size}: {path}")
            raw = source.read(header_size)
            if len(raw) != header_size:
                raise QualificationError(f"truncated SafeTensors header: {path}")
        value = json.loads(raw)
    except (OSError, json.JSONDecodeError) as error:
        raise QualificationError(f"cannot inspect SafeTensors header {path}: {error}") from error
    if not isinstance(value, dict):
        raise QualificationError(f"SafeTensors header must be an object: {path}")
    return value


def checkpoint_shards(model_dir: pathlib.Path) -> tuple[str, list[pathlib.Path]]:
    single = model_dir / "model.safetensors"
    index = model_dir / "model.safetensors.index.json"
    if single.is_file():
        return "safetensors", [single]
    if index.is_file():
        try:
            parsed = json.loads(index.read_text(encoding="utf-8"))
            weight_map = parsed["weight_map"]
        except (OSError, json.JSONDecodeError, KeyError, TypeError) as error:
            raise QualificationError(f"invalid sharded SafeTensors index {index}: {error}") from error
        if not isinstance(weight_map, dict) or not weight_map:
            raise QualificationError(f"empty sharded SafeTensors weight_map: {index}")
        resolved_root = model_dir.resolve()
        shards: list[pathlib.Path] = []
        names: set[str] = set()
        for name in weight_map.values():
            if not isinstance(name, str) or not name:
                raise QualificationError(f"invalid shard name in {index}")
            names.add(name)
        for name in sorted(names):
            shard = (model_dir / name).resolve()
            try:
                shard.relative_to(resolved_root)
            except ValueError as error:
                raise QualificationError(f"shard escapes model directory: {name}") from error
            if not shard.is_file():
                raise QualificationError(f"missing SafeTensors shard: {shard}")
            shards.append(shard)
        return "sharded_safetensors", shards
    ggufs = sorted(model_dir.glob("*.gguf"))
    if ggufs:
        raise QualificationError(
            f"{model_dir} contains packed GGUF deployment weights; strict CUDA preference "
            "training requires a BF16 SafeTensors model directory"
        )
    raise QualificationError(f"no model.safetensors or model.safetensors.index.json in {model_dir}")


def text_config(config: dict[str, Any]) -> dict[str, Any]:
    text = config.get("text_config")
    return text if isinstance(text, dict) else config


def config_topology(config: dict[str, Any]) -> dict[str, bool | int | str | None]:
    text = text_config(config)

    def value(name: str) -> int | str | None:
        candidate = text.get(name)
        if isinstance(candidate, (int, str)) and not isinstance(candidate, bool):
            return candidate
        return None

    return {
        "model_type": config.get("model_type") if isinstance(config.get("model_type"), str) else None,
        "hidden_size": value("hidden_size"),
        "num_hidden_layers": value("num_hidden_layers"),
        "num_attention_heads": value("num_attention_heads"),
        "num_key_value_heads": value("num_key_value_heads"),
        "head_dim": value("head_dim") or value("attention_head_dim"),
        "intermediate_size": value("intermediate_size"),
        "num_kv_shared_layers": value("num_kv_shared_layers"),
        "use_double_wide_mlp": (
            text.get("use_double_wide_mlp")
            if isinstance(text.get("use_double_wide_mlp"), bool)
            else None
        ),
        "vocab_size": value("vocab_size"),
    }


def required_positive_int(mapping: dict[str, Any], name: str, config_path: pathlib.Path) -> int:
    value = mapping.get(name)
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise QualificationError(f"{name} must be a positive integer in {config_path}")
    return value


def validate_text_mlp_shapes(
    config: dict[str, Any],
    config_path: pathlib.Path,
    tensor_shapes: dict[str, tuple[int, ...]],
) -> None:
    text = text_config(config)
    hidden_size = required_positive_int(text, "hidden_size", config_path)
    intermediate_size = required_positive_int(text, "intermediate_size", config_path)
    num_hidden_layers = required_positive_int(text, "num_hidden_layers", config_path)

    num_kv_shared_layers = text.get("num_kv_shared_layers", 0)
    if (
        not isinstance(num_kv_shared_layers, int)
        or isinstance(num_kv_shared_layers, bool)
        or num_kv_shared_layers < 0
        or num_kv_shared_layers > num_hidden_layers
    ):
        raise QualificationError(
            f"num_kv_shared_layers must be an integer in [0, {num_hidden_layers}] "
            f"in {config_path}"
        )
    use_double_wide_mlp = text.get("use_double_wide_mlp", False)
    if not isinstance(use_double_wide_mlp, bool):
        raise QualificationError(f"use_double_wide_mlp must be boolean in {config_path}")
    if use_double_wide_mlp and num_kv_shared_layers == num_hidden_layers:
        raise QualificationError(
            f"double-wide MLP requires at least one non-shared KV donor layer in {config_path}"
        )

    found: dict[tuple[int, str], tuple[str, tuple[int, ...]]] = {}
    for name, shape in tensor_shapes.items():
        match = TEXT_MLP_WEIGHT_RE.search(name)
        if match is None:
            continue
        layer = int(match.group("layer"))
        projection = match.group("projection")
        if layer >= num_hidden_layers:
            raise QualificationError(
                f"text MLP tensor references out-of-range layer {layer}: {name}"
            )
        key = (layer, projection)
        if key in found:
            raise QualificationError(
                f"duplicate text MLP {projection} weight for layer {layer}: "
                f"{found[key][0]} and {name}"
            )
        found[key] = (name, shape)

    first_shared_layer = num_hidden_layers - num_kv_shared_layers
    for layer in range(num_hidden_layers):
        layer_intermediate_size = intermediate_size
        if use_double_wide_mlp and layer >= first_shared_layer:
            layer_intermediate_size *= 2
        expected = {
            "gate_proj": (layer_intermediate_size, hidden_size),
            "up_proj": (layer_intermediate_size, hidden_size),
            "down_proj": (hidden_size, layer_intermediate_size),
        }
        for projection, expected_shape in expected.items():
            item = found.get((layer, projection))
            if item is None:
                raise QualificationError(
                    f"missing text MLP {projection} weight for layer {layer} in checkpoint"
                )
            name, actual_shape = item
            if actual_shape != expected_shape:
                raise QualificationError(
                    f"text MLP shape mismatch for {name}: expected {list(expected_shape)}, "
                    f"found {list(actual_shape)}"
                )


def inspect_model(spec: ModelSpec) -> ModelPreflight:
    model_dir = spec.path.resolve()
    if not model_dir.is_dir():
        if model_dir.is_file() and model_dir.suffix.lower() == ".gguf":
            raise QualificationError(f"{model_dir} is packed GGUF, not a BF16 training directory")
        raise QualificationError(f"model path is not a directory: {model_dir}")
    artifact_kind, shards = checkpoint_shards(model_dir)
    config_path = model_dir / "config.json"
    try:
        config_bytes = config_path.read_bytes()
        config = json.loads(config_bytes)
    except (OSError, json.JSONDecodeError) as error:
        raise QualificationError(f"cannot load model config {config_path}: {error}") from error
    if not isinstance(config, dict):
        raise QualificationError(f"model config must be an object: {config_path}")
    model_type = config.get("model_type")
    normalized = model_type.lower().replace("_", "") if isinstance(model_type, str) else ""
    if not normalized.startswith("gemma4") or "assistant" in normalized:
        raise QualificationError(f"expected Gemma4 model_type in {config_path}, found {model_type!r}")
    if not any(
        (model_dir / name).is_file()
        for name in ("tokenizer.json", "tokenizer.model", "spiece.model", "tokenizer_config.json")
    ):
        raise QualificationError(f"no tokenizer artifact found in {model_dir}")

    dtypes: set[str] = set()
    rank2_dtypes: set[str] = set()
    tensor_shapes: dict[str, tuple[int, ...]] = {}
    tensor_count = 0
    for shard in shards:
        for name, metadata in safetensors_header(shard).items():
            if name == "__metadata__":
                continue
            if not isinstance(metadata, dict):
                raise QualificationError(f"invalid tensor metadata {name!r} in {shard}")
            dtype = metadata.get("dtype")
            shape = metadata.get("shape")
            if not isinstance(dtype, str) or not isinstance(shape, list):
                raise QualificationError(f"incomplete tensor metadata {name!r} in {shard}")
            if not all(isinstance(dimension, int) and not isinstance(dimension, bool) and dimension >= 0 for dimension in shape):
                raise QualificationError(f"invalid tensor shape {name!r} in {shard}")
            if name in tensor_shapes:
                raise QualificationError(f"duplicate tensor metadata {name!r} across checkpoint shards")
            tensor_shapes[name] = tuple(shape)
            dtypes.add(dtype)
            tensor_count += 1
            if len(shape) == 2:
                rank2_dtypes.add(dtype)
    unsupported = rank2_dtypes - SUPPORTED_RANK2_DTYPES
    if unsupported:
        raise QualificationError(
            f"unsupported rank-2 stored-weight dtype(s) for strict CUDA backward: "
            f"{', '.join(sorted(unsupported))}"
        )
    if "BF16" not in rank2_dtypes:
        raise QualificationError(f"checkpoint has no rank-2 BF16 weights: {model_dir}")
    validate_text_mlp_shapes(config, config_path, tensor_shapes)
    return ModelPreflight(
        label=spec.label,
        path=str(model_dir),
        artifact_kind=artifact_kind,
        shard_count=len(shards),
        total_checkpoint_bytes=sum(path.stat().st_size for path in shards),
        tensor_count=tensor_count,
        dtypes=sorted(dtypes),
        rank2_dtypes=sorted(rank2_dtypes),
        config_sha256=hashlib.sha256(config_bytes).hexdigest(),
        topology=config_topology(config),
    )


def recipe_for(
    objective: str,
    model: ModelSpec,
    dataset_path: pathlib.Path,
    run_dir: pathlib.Path,
    max_seq_len: int,
    rank: int,
    alpha: float,
    updates: int,
) -> dict[str, Any]:
    recipe: dict[str, Any] = {
        "recipe": objective,
        "model": {"path": str(model.path.resolve()), "family": "gemma4"},
        "dataset": {
            "path": str(dataset_path),
            "format": "text-preference" if objective == "dpo" else "text-grpo",
            "max_examples": 1,
            "max_seq_len": max_seq_len,
        },
        "adapter": {
            "rank": rank,
            "alpha": alpha,
            "dropout": LORA_DROPOUT,
            "target_modules": LORA_TARGET_MODULES,
        },
        "optimizer": {
            "learning_rate": LEARNING_RATE,
            "weight_decay": WEIGHT_DECAY,
            "lr_scheduler": "constant",
            "epochs": updates,
            "gradient_accumulation_steps": 1,
            "max_grad_norm": MAX_GRAD_NORM,
        },
        "backend": "cuda",
        "artifacts": {
            "root": str(run_dir),
            "adapter_dir": str(run_dir / "adapter-bootstrap"),
            "trained_adapter_dir": str(run_dir / "adapter-trained"),
            "report_path": str(run_dir / f"{objective}_report.json"),
        },
    }
    if objective == "dpo":
        recipe["preference"] = {"beta": DPO_BETA}
    else:
        recipe["grpo"] = {
            # Four independently seeded on-policy samples keep this bounded
            # smoke representative of GRPO while making a wholly identical
            # reward group substantially less likely than a two-sample probe.
            "group_size": GRPO_GROUP_SIZE,
            "max_completion_tokens": GRPO_MAX_COMPLETION_TOKENS,
            "clip_epsilon": GRPO_CLIP_EPSILON,
            "kl_coef": GRPO_KL_COEF,
            "advantage_eps": GRPO_ADVANTAGE_EPSILON,
            "normalize_advantage": True,
            "reward_mode": "sequence-hash",
        }
    return recipe


def query_gpu_memory_mib(pid: int) -> int | None:
    tool = shutil.which("nvidia-smi")
    if tool is None:
        return None
    try:
        completed = subprocess.run(
            [tool, "--query-compute-apps=pid,used_gpu_memory", "--format=csv,noheader,nounits"],
            text=True,
            capture_output=True,
            timeout=2,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    total = 0
    found = False
    for line in completed.stdout.splitlines():
        fields = [field.strip() for field in line.split(",")]
        if len(fields) != 2:
            continue
        try:
            row_pid, memory = int(fields[0]), int(fields[1])
        except ValueError:
            continue
        if row_pid == pid:
            found = True
            total += memory
    return total if found else None


def run_command(command: list[str], log_path: pathlib.Path, timeout_seconds: int) -> ProcessMetrics:
    environment = os.environ.copy()
    environment["TERMITE_ENABLE_TRAINING_GRAPH_EXECUTOR"] = "1"
    environment["TERMITE_REQUIRE_TRAINING_GRAPH_EXECUTOR"] = "1"
    environment.update(DETERMINISTIC_CUDA_ENVIRONMENT)
    for name in (
        "TERMITE_DISABLE_TRAINING_GRAPH_EXECUTOR",
        "TERMITE_TRAINING_GRAPH_EXECUTOR_PARITY_CHECK",
        "TERMITE_TRAINING_GRAPH_EXECUTOR_PARITY_NODE_IDS",
    ):
        environment.pop(name, None)
    started = time.monotonic()
    peak_gpu_mib = 0
    gpu_samples = 0
    with log_path.open("w", encoding="utf-8") as log:
        log.write("command: " + json.dumps(command) + "\n")
        log.flush()
        process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT, env=environment, text=True)
        try:
            while True:
                observed = query_gpu_memory_mib(process.pid)
                if observed is not None and observed > 0:
                    peak_gpu_mib = max(peak_gpu_mib, observed)
                    gpu_samples += 1
                return_code = process.poll()
                if return_code is not None:
                    break
                if time.monotonic() - started > timeout_seconds:
                    process.terminate()
                    try:
                        process.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait(timeout=10)
                    raise QualificationError(f"command timed out after {timeout_seconds}s; see {log_path}")
                time.sleep(0.2)
        finally:
            if process.poll() is None:
                process.terminate()
    return ProcessMetrics(process.returncode, time.monotonic() - started, peak_gpu_mib, gpu_samples)


def validate_process_metrics(metrics: ProcessMetrics) -> None:
    finite_number(metrics.wall_seconds, "process wall_seconds", positive=True)
    if metrics.gpu_samples < 1 or metrics.peak_gpu_memory_mib < 1:
        raise QualificationError("strict CUDA qualification captured no positive nvidia-smi process-memory sample")


def validate_trainable_update(report: dict[str, Any], objective: str) -> dict[str, Any]:
    update = report.get("trainable_update")
    if not isinstance(update, dict):
        raise QualificationError(f"{objective} report is missing trainable tensor movement evidence")
    tensor_count = nonnegative_int(update, "tensor_count", namespace="trainable_update")
    changed = nonnegative_int(update, "changed_tensor_count", namespace="trainable_update")
    delta = finite_number(update.get("max_abs_delta"), "trainable_update.max_abs_delta", positive=True)
    if tensor_count < 1 or changed < 1 or changed > tensor_count:
        raise QualificationError(f"{objective} report has an invalid changed-tensor count")
    if delta <= 0.0:
        raise QualificationError(f"{objective} report has no trainable delta")
    return update


def validate_execution_evidence(
    report: dict[str, Any], objective: str, optimizer_steps: int
) -> dict[str, Any]:
    if report.get("device_execution_scope") != DEVICE_EXECUTION_SCOPE:
        raise QualificationError(f"{objective} report does not scope strict CUDA evidence to optimizer steps")
    evidence = report.get("device_execution")
    if not isinstance(evidence, dict) or evidence.get("schema_version") != EXECUTION_EVIDENCE_SCHEMA:
        raise QualificationError(f"{objective} report is missing strict CUDA execution evidence")
    train_steps = nonnegative_int(evidence, "train_steps", namespace="device_execution")
    if train_steps < optimizer_steps:
        raise QualificationError(f"{objective} report has fewer CUDA train steps than optimizer steps")
    for name in ("graph_executor_partitions", "graph_executor_planned_dispatches", "cuda_kernel_launches"):
        if nonnegative_int(evidence, name, namespace="device_execution") < 1:
            raise QualificationError(f"{objective} report has no {name}")
    for name in (
        "graph_executor_fallback_steps",
        "graph_executor_native_partitions",
        "graph_executor_unsupported_ops",
        "graph_executor_interpreter_fallbacks",
        "graph_executor_true_host_outputs",
        "runtime_input_d2h_bytes",
        "compiled_session_setup_d2h_bytes",
        "graph_execution_h2d_bytes",
        "host_gradient_tensors",
    ):
        if nonnegative_int(evidence, name, namespace="device_execution") != 0:
            raise QualificationError(f"{objective} strict CUDA fallback violation: {name} != 0")
    for observed, declared in (
        ("runtime_input_uploads", "declared_runtime_input_uploads"),
        ("runtime_input_upload_bytes", "declared_runtime_input_upload_bytes"),
        ("runtime_input_h2d_bytes", "declared_runtime_input_h2d_bytes"),
    ):
        if nonnegative_int(evidence, observed, namespace="device_execution") != nonnegative_int(
            evidence, declared, namespace="device_execution"
        ):
            raise QualificationError(f"{objective} undeclared CUDA transfer: {observed} != {declared}")
    eval_steps = nonnegative_int(evidence, "eval_steps", namespace="device_execution")
    if nonnegative_int(evidence, "graph_execution_d2h_bytes", namespace="device_execution") > (
        train_steps + eval_steps
    ) * 4:
        raise QualificationError(f"{objective} graph readback exceeds one f32 loss per graph step")
    # A hot training step may read back its scalar loss; an optimizer update
    # may additionally read back one scalar global gradient norm for clipping
    # and report telemetry. No tensor-sized or unaccounted D2H is allowed.
    scalar_readback_budget = (train_steps + eval_steps + optimizer_steps) * 4
    for name in ("training_runtime_d2h_bytes", "cuda_d2h_bytes"):
        if nonnegative_int(evidence, name, namespace="device_execution") > scalar_readback_budget:
            raise QualificationError(f"{objective} CUDA scalar readback budget exceeded: {name}")
    if nonnegative_int(evidence, "cuda_largest_d2h_transfer_bytes", namespace="device_execution") > 4:
        raise QualificationError(f"{objective} CUDA readback exceeds one f32 scalar")
    if nonnegative_int(evidence, "peak_resident_bytes", namespace="device_execution") < 1:
        raise QualificationError(f"{objective} report has no device-resident memory evidence")
    return evidence


def validate_report(path: pathlib.Path, objective: str, expected_updates: int) -> dict[str, Any]:
    try:
        report = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise QualificationError(f"cannot load {objective} report {path}: {error}") from error
    if not isinstance(report, dict):
        raise QualificationError(f"{objective} report must be an object")
    if report.get("policy_backend") != "cuda" or report.get("optimizer_backend") != "cuda":
        raise QualificationError(f"{objective} policy and optimizer must both report CUDA")
    optimizer_steps = report.get("optimizer_steps")
    cuda_optimizer_steps = report.get("cuda_optimizer_steps")
    if optimizer_steps != expected_updates or cuda_optimizer_steps != optimizer_steps:
        raise QualificationError(
            f"{objective} expected {expected_updates} CUDA optimizer steps; "
            f"optimizer_steps={optimizer_steps!r}, cuda_optimizer_steps={cuda_optimizer_steps!r}"
        )
    micro_batches = report.get("micro_batch_steps")
    if not isinstance(micro_batches, int) or isinstance(micro_batches, bool) or micro_batches < optimizer_steps:
        raise QualificationError(f"{objective} report has invalid micro-batch accounting")
    finite_number(report.get("loss"), f"{objective}.loss")
    finite_number(report.get("mean_grad_norm"), f"{objective}.mean_grad_norm", positive=True)
    if objective == "grpo":
        finite_number(report.get("mean_reward"), "grpo.mean_reward")
        finite_number(report.get("reward_std"), "grpo.reward_std", positive=True)
        informative_groups = report.get("informative_groups")
        if not isinstance(informative_groups, int) or isinstance(informative_groups, bool) or informative_groups < 1:
            raise QualificationError("grpo report has no informative reward groups")
        expected_exact_contract = {
            "group_size": GRPO_GROUP_SIZE,
            "advantage_standard_deviation_correction": 1,
            "reward_scaling": "group-sample-std",
            "loss_normalization": "per-completion-token-mean",
        }
        observed_contract = {name: report.get(name) for name in expected_exact_contract}
        if observed_contract != expected_exact_contract:
            raise QualificationError(
                f"grpo report objective contract mismatch: expected={expected_exact_contract!r}, "
                f"observed={observed_contract!r}"
            )
        for name, expected in (
            ("clip_epsilon", GRPO_CLIP_EPSILON),
            ("kl_coef", GRPO_KL_COEF),
            ("advantage_epsilon", GRPO_ADVANTAGE_EPSILON),
        ):
            observed = finite_number(report.get(name), f"grpo.{name}")
            if not math.isclose(observed, expected, rel_tol=1e-6, abs_tol=1e-9):
                raise QualificationError(
                    f"grpo report {name} mismatch: expected={expected!r}, observed={observed!r}"
                )
    else:
        beta = finite_number(report.get("beta"), "dpo.beta")
        if not math.isclose(beta, DPO_BETA, rel_tol=1e-6, abs_tol=1e-9):
            raise QualificationError(f"dpo report beta mismatch: {beta!r}")
    validate_trainable_update(report, objective)
    validate_execution_evidence(report, objective, optimizer_steps)
    return report


def adapter_checkpoint(run_dir: pathlib.Path, which: str) -> pathlib.Path:
    path = run_dir / which / "adapter_model.safetensors"
    if not path.is_file():
        raise QualificationError(f"missing adapter checkpoint: {path}")
    return path


def finetune_command(antfly_bin: pathlib.Path, recipe_path: pathlib.Path) -> list[str]:
    prefix = [str(antfly_bin)]
    if antfly_bin.name == "antfly":
        prefix.append("inference")
    return [*prefix, "finetune", "run", str(recipe_path)]


def run_case(
    antfly_bin: pathlib.Path,
    model: ModelSpec,
    objective: str,
    grpo_target: str | None,
    run_dir: pathlib.Path,
    max_seq_len: int,
    rank: int,
    alpha: float,
    updates: int,
    timeout_seconds: int,
) -> dict[str, Any]:
    run_dir.mkdir(parents=True)
    dataset_path = run_dir / f"{objective}.jsonl"
    if objective == "dpo":
        row = DPO_FIXTURE
    else:
        if not grpo_target:
            raise QualificationError(f"missing --grpo-target {model.label}=TEXT")
        row = {"prompt": GRPO_PROMPT, "target": grpo_target}
    dataset_path.write_text(json.dumps(row, sort_keys=True) + "\n", encoding="utf-8")
    recipe_path = run_dir / "recipe.json"
    atomic_write_json(
        recipe_path,
        recipe_for(objective, model, dataset_path, run_dir, max_seq_len, rank, alpha, updates),
    )
    log_path = run_dir / "run.log"
    metrics = run_command(finetune_command(antfly_bin, recipe_path), log_path, timeout_seconds)
    if metrics.return_code != 0:
        raise QualificationError(f"{model.label} {objective} exited {metrics.return_code}; see {log_path}")
    validate_process_metrics(metrics)
    report_path = run_dir / f"{objective}_report.json"
    report = validate_report(report_path, objective, updates)
    initial = adapter_checkpoint(run_dir, "adapter-bootstrap")
    trained = adapter_checkpoint(run_dir, "adapter-trained")
    initial_sha256 = sha256_file(initial)
    trained_sha256 = sha256_file(trained)
    if initial_sha256 == trained_sha256:
        raise QualificationError(f"{model.label} {objective} persisted adapter did not change")
    throughput = updates / metrics.wall_seconds
    finite_number(throughput, "optimizer_steps_per_second", positive=True)
    evidence = report["device_execution"]
    return {
        "label": model.label,
        "objective": objective,
        "status": "passed",
        "optimizer_steps": updates,
        "cuda_optimizer_steps": report["cuda_optimizer_steps"],
        "micro_batch_steps": report["micro_batch_steps"],
        "loss": report["loss"],
        "mean_grad_norm": report["mean_grad_norm"],
        "trainable_update": report["trainable_update"],
        "wall_seconds": metrics.wall_seconds,
        "optimizer_steps_per_second": throughput,
        "peak_gpu_memory_mib": metrics.peak_gpu_memory_mib,
        "gpu_memory_samples": metrics.gpu_samples,
        "reported_peak_resident_bytes": evidence["peak_resident_bytes"],
        "initial_adapter_sha256": initial_sha256,
        "trained_adapter_sha256": trained_sha256,
        "report_path": str(report_path),
        "log_path": str(log_path),
    }


def benchmark_aggregate(cases: list[dict[str, Any]]) -> dict[str, Any]:
    if not cases:
        raise QualificationError("cannot aggregate an empty benchmark")
    seconds = [finite_number(case["wall_seconds"], "wall_seconds", positive=True) for case in cases]
    throughputs = [
        finite_number(case["optimizer_steps_per_second"], "optimizer_steps_per_second", positive=True)
        for case in cases
    ]
    memory = [
        finite_number(case["peak_gpu_memory_mib"], "peak_gpu_memory_mib", positive=True)
        for case in cases
    ]
    return {
        "repetitions": len(cases),
        "wall_seconds": seconds,
        "median_wall_seconds": statistics.median(seconds),
        "mean_wall_seconds": statistics.fmean(seconds),
        "optimizer_steps_per_second": throughputs,
        "median_optimizer_steps_per_second": statistics.median(throughputs),
        "peak_gpu_memory_mib": memory,
        "median_peak_gpu_memory_mib": statistics.median(memory),
        "max_peak_gpu_memory_mib": max(memory),
    }


def load_unsloth_baseline(
    path: pathlib.Path,
    preflight: list[ModelPreflight],
    objectives: list[str],
    protocol: dict[str, Any],
) -> dict[tuple[str, str], dict[str, Any]]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise QualificationError(f"cannot load Unsloth baseline {path}: {error}") from error
    if not isinstance(payload, dict) or payload.get("schema_version") != BASELINE_SCHEMA:
        raise QualificationError(f"Unsloth baseline must use {BASELINE_SCHEMA}")
    if payload.get("status") != "passed":
        raise QualificationError("Unsloth baseline must have terminal passed status")
    if payload.get("protocol") != protocol:
        raise QualificationError("Unsloth baseline protocol does not exactly match the Zig benchmark protocol")
    expected_configs = {item.label: item.config_sha256 for item in preflight}
    rows = payload.get("cases")
    if not isinstance(rows, list):
        raise QualificationError("Unsloth baseline cases must be an array")
    indexed: dict[tuple[str, str], dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, dict):
            raise QualificationError("Unsloth baseline case must be an object")
        label = row.get("label")
        objective = row.get("objective")
        if label not in expected_configs or objective not in objectives:
            continue
        key = (label, objective)
        if key in indexed:
            raise QualificationError(f"duplicate Unsloth baseline case: {label}/{objective}")
        if row.get("model_config_sha256") != expected_configs[label]:
            raise QualificationError(f"Unsloth baseline model fingerprint mismatch: {label}/{objective}")
        if row.get("optimizer_steps") != protocol["updates"]:
            raise QualificationError(f"Unsloth optimizer-step mismatch: {label}/{objective}")
        for name in (
            "median_wall_seconds",
            "median_optimizer_steps_per_second",
            "median_peak_gpu_memory_mib",
            "loss",
            "mean_grad_norm",
        ):
            finite_number(row.get(name), f"Unsloth {label}/{objective}.{name}", positive=name != "loss")
        indexed[key] = row
    missing = [
        f"{item.label}/{objective}"
        for item in preflight
        for objective in objectives
        if (item.label, objective) not in indexed
    ]
    if missing:
        raise QualificationError("Unsloth baseline is missing cases: " + ", ".join(missing))
    return indexed


def compare_with_unsloth(
    zig: dict[str, Any], baseline: dict[str, Any]
) -> dict[str, Any]:
    zig_wall = finite_number(zig["median_wall_seconds"], "Zig median wall seconds", positive=True)
    base_wall = finite_number(baseline["median_wall_seconds"], "Unsloth median wall seconds", positive=True)
    zig_rate = finite_number(
        zig["median_optimizer_steps_per_second"], "Zig optimizer throughput", positive=True
    )
    base_rate = finite_number(
        baseline["median_optimizer_steps_per_second"], "Unsloth optimizer throughput", positive=True
    )
    zig_memory = finite_number(
        zig["median_peak_gpu_memory_mib"], "Zig peak GPU memory", positive=True
    )
    base_memory = finite_number(
        baseline["median_peak_gpu_memory_mib"], "Unsloth peak GPU memory", positive=True
    )
    return {
        "zig_to_unsloth_wall_time_ratio": zig_wall / base_wall,
        "zig_to_unsloth_optimizer_throughput_ratio": zig_rate / base_rate,
        "zig_to_unsloth_peak_gpu_memory_ratio": zig_memory / base_memory,
        "unsloth": baseline,
    }


def benchmark_protocol(max_seq_len: int, rank: int, alpha: float) -> dict[str, Any]:
    return {
        "process_scope": "fresh-process-per-objective-repetition",
        "duration_scope": "process-wall-including-model-load-and-adapter-publication",
        "updates": BENCHMARK_UPDATES,
        "max_examples": 1,
        "max_seq_len": max_seq_len,
        "rank": rank,
        "alpha": alpha,
        "learning_rate": LEARNING_RATE,
        "optimizer_family": "adamw",
        "weight_decay": WEIGHT_DECAY,
        "adam_beta1": ADAM_BETA1,
        "adam_beta2": ADAM_BETA2,
        "adam_epsilon": ADAM_EPSILON,
        "lr_scheduler": "constant",
        "max_grad_norm": MAX_GRAD_NORM,
        "gradient_accumulation_steps": 1,
        "lora_target_modules": LORA_TARGET_MODULES,
        "lora_dropout": LORA_DROPOUT,
        "dpo_beta": DPO_BETA,
        "dpo_fixture": DPO_FIXTURE,
        "grpo_fixture": {"prompt": GRPO_PROMPT, "target": BENCHMARK_GRPO_TARGET},
        "grpo_reward_mode": "sequence-hash",
        "grpo_group_size": GRPO_GROUP_SIZE,
        "grpo_max_completion_tokens": GRPO_MAX_COMPLETION_TOKENS,
        "grpo_clip_epsilon": GRPO_CLIP_EPSILON,
        "grpo_kl_coef": GRPO_KL_COEF,
        "grpo_advantage_epsilon": GRPO_ADVANTAGE_EPSILON,
        "grpo_advantage_standard_deviation_correction": 1,
        "grpo_reward_scaling": "group-sample-std",
        "grpo_loss_normalization": "per-completion-token-mean",
    }


def validate_grpo_targets(
    models: list[ModelSpec],
    targets: dict[str, str],
    objectives: list[str],
    benchmark: bool,
) -> None:
    if "grpo" not in objectives:
        return
    missing = [model.label for model in models if not targets.get(model.label)]
    if missing:
        raise QualificationError("GRPO requires --grpo-target for: " + ", ".join(missing))
    if benchmark:
        mismatched = [
            model.label for model in models if targets[model.label] != BENCHMARK_GRPO_TARGET
        ]
        if mismatched:
            raise QualificationError(
                f"--benchmark locks --grpo-target LABEL={BENCHMARK_GRPO_TARGET} for: "
                + ", ".join(mismatched)
            )


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--model", action="append", required=True, metavar="LABEL=DIR")
    result.add_argument("--grpo-target", action="append", default=[], metavar="LABEL=TEXT")
    result.add_argument("--objective", action="append", choices=("dpo", "grpo"), default=[])
    result.add_argument("--antfly-bin", type=pathlib.Path)
    result.add_argument("--out", type=pathlib.Path)
    result.add_argument("--preflight-only", action="store_true")
    result.add_argument("--benchmark", action="store_true")
    result.add_argument("--unsloth-baseline", type=pathlib.Path)
    result.add_argument("--repetitions", type=int, default=1)
    result.add_argument("--max-seq-len", type=int, default=128)
    result.add_argument("--rank", type=int)
    result.add_argument("--alpha", type=float, default=32.0)
    result.add_argument("--timeout-seconds", type=int, default=3600)
    return result


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    try:
        rank = args.rank if args.rank is not None else (16 if args.benchmark else 8)
        if (
            args.repetitions < 1
            or args.max_seq_len < 8
            or rank < 1
            or args.alpha <= 0
            or args.timeout_seconds < 1
        ):
            raise QualificationError("repetitions, sequence length, rank, alpha, and timeout must be positive")
        if args.benchmark and args.repetitions < BENCHMARK_MIN_REPETITIONS:
            raise QualificationError(f"--benchmark requires at least {BENCHMARK_MIN_REPETITIONS} repetitions")
        if args.benchmark and args.unsloth_baseline is None:
            raise QualificationError("--benchmark requires --unsloth-baseline for a matched comparison")
        if not args.benchmark and args.unsloth_baseline is not None:
            raise QualificationError("--unsloth-baseline requires --benchmark")
        if args.benchmark and (args.max_seq_len != 128 or rank != 16 or args.alpha != 32.0):
            raise QualificationError(
                "--benchmark locks --max-seq-len=128, --rank=16, and --alpha=32"
            )

        models_raw = parse_unique_assignments(args.model, "--model")
        targets = parse_unique_assignments(args.grpo_target, "--grpo-target")
        unknown_targets = sorted(set(targets) - set(models_raw))
        if unknown_targets:
            raise QualificationError("--grpo-target has no matching --model: " + ", ".join(unknown_targets))
        models = [ModelSpec(label, pathlib.Path(path)) for label, path in models_raw.items()]
        preflight = [inspect_model(model) for model in models]
        preflight_payload = [asdict(item) for item in preflight]
        if args.preflight_only:
            print(json.dumps({"status": "passed", "models": preflight_payload}, indent=2, sort_keys=True))
            return 0

        objectives = list(dict.fromkeys(args.objective or ["dpo", "grpo"]))
        validate_grpo_targets(models, targets, objectives, args.benchmark)
        package_root = pathlib.Path(__file__).resolve().parents[2]
        antfly_bin = (args.antfly_bin or package_root / "zig-out/bin/antfly-inference").resolve()
        if not antfly_bin.is_file() or not os.access(antfly_bin, os.X_OK):
            raise QualificationError(f"inference binary is not executable: {antfly_bin}")
        out_dir = (
            args.out
            or pathlib.Path("/tmp")
            / f"antfly-gemma4-cuda-preference-{time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())}"
        ).resolve()
        if out_dir.exists():
            raise QualificationError(f"refusing to reuse existing output directory: {out_dir}")

        updates = BENCHMARK_UPDATES if args.benchmark else SMOKE_UPDATES
        protocol = benchmark_protocol(args.max_seq_len, rank, args.alpha)
        baseline = (
            load_unsloth_baseline(args.unsloth_baseline.resolve(), preflight, objectives, protocol)
            if args.benchmark
            else {}
        )
        out_dir.mkdir(parents=True)
        summary: dict[str, Any] = {
            "schema_version": SUMMARY_SCHEMA,
            "status": "running",
            "created_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "mode": "benchmark" if args.benchmark else "smoke",
            "protocol": protocol if args.benchmark else {**protocol, "updates": SMOKE_UPDATES},
            "antfly_bin": str(antfly_bin),
            "antfly_bin_sha256": sha256_file(antfly_bin),
            "runner_sha256": sha256_file(pathlib.Path(__file__).resolve()),
            "deterministic_cuda_environment": DETERMINISTIC_CUDA_ENVIRONMENT,
            "models": preflight_payload,
            "objectives": objectives,
            "repetitions": args.repetitions,
            "cases": [],
            "aggregates": [],
        }
        summary_path = out_dir / "summary.json"
        atomic_write_json(summary_path, summary)
        try:
            for model in models:
                for objective in objectives:
                    cases: list[dict[str, Any]] = []
                    for repetition in range(1, args.repetitions + 1):
                        result = run_case(
                            antfly_bin,
                            model,
                            objective,
                            targets.get(model.label),
                            out_dir / model.label / objective / f"run-{repetition:02d}",
                            args.max_seq_len,
                            rank,
                            args.alpha,
                            updates,
                            args.timeout_seconds,
                        )
                        result["repetition"] = repetition
                        cases.append(result)
                        summary["cases"].append(result)
                        atomic_write_json(summary_path, summary)
                    digests = {case["trained_adapter_sha256"] for case in cases}
                    losses = {json.dumps(case["loss"]) for case in cases}
                    if len(digests) != 1 or len(losses) != 1:
                        raise QualificationError(
                            f"{model.label} {objective} repetitions are not deterministic: "
                            f"adapter_digests={sorted(digests)}, losses={sorted(losses)}"
                        )
                    aggregate = {
                        "label": model.label,
                        "objective": objective,
                        **benchmark_aggregate(cases),
                    }
                    if args.benchmark:
                        aggregate["comparison"] = compare_with_unsloth(
                            aggregate, baseline[(model.label, objective)]
                        )
                    summary["aggregates"].append(aggregate)
            summary["status"] = "passed"
        except Exception as error:
            summary["status"] = "failed"
            summary["error"] = str(error)
            atomic_write_json(summary_path, summary)
            raise
        atomic_write_json(summary_path, summary)
        print(f"PASS summary={summary_path}")
        return 0
    except QualificationError as error:
        print(f"error: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
