#!/usr/bin/env python3
"""Run the pinned Unsloth side of the Gemma4 CUDA preference benchmark.

The public process is a standard-library orchestrator.  Every objective and
repetition runs in a fresh child process so wall time includes Python startup,
model load, training, and adapter publication.  Heavy Python dependencies are
imported only by the worker.  The resulting JSON is consumed directly by
``run_gemma4_cuda_preference_smoke.py --benchmark --unsloth-baseline``.

This runner is intentionally local-only: it neither downloads checkpoints nor
publishes artifacts outside the requested output directory.
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
from dataclasses import dataclass
from typing import Any, Iterable


SCHEMA_VERSION = "antfly_gemma4_unsloth_preference_benchmark/v1"
LABEL_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
UPDATES = 25
MIN_REPETITIONS = 3
MAX_SEQ_LEN = 128
LORA_RANK = 16
LORA_ALPHA = 32.0
LEARNING_RATE = 1e-4
WEIGHT_DECAY = 0.01
ADAM_BETA1 = 0.9
ADAM_BETA2 = 0.999
ADAM_EPSILON = 1e-8
GRADIENT_ACCUMULATION_STEPS = 1
MAX_GRAD_NORM = 1.0
LORA_TARGET_MODULES = ["q_proj", "v_proj"]
LORA_DROPOUT = 0.0
SEED = 3407
GRPO_TARGET = "qualification-sequence-hash-v1"
GRPO_GROUP_SIZE = 4
GRPO_MAX_COMPLETION_TOKENS = 4
GRPO_CLIP_EPSILON = 0.2
GRPO_KL_COEF = 0.04
GRPO_ADVANTAGE_EPSILON = 1e-4
DPO_BETA = 0.1
DPO_FIXTURE = {
    "prompt": "Answer briefly: what is the capital of France?",
    "chosen": "The capital is Paris.",
    "rejected": "The capital is Berlin.",
}
GRPO_FIXTURE = {"prompt": "The", "target": GRPO_TARGET}
PROTOCOL = {
    "process_scope": "fresh-process-per-objective-repetition",
    "duration_scope": "process-wall-including-model-load-and-adapter-publication",
    "updates": UPDATES,
    "max_examples": 1,
    "max_seq_len": MAX_SEQ_LEN,
    "rank": LORA_RANK,
    "alpha": LORA_ALPHA,
    "learning_rate": LEARNING_RATE,
    "optimizer_family": "adamw",
    "weight_decay": WEIGHT_DECAY,
    "adam_beta1": ADAM_BETA1,
    "adam_beta2": ADAM_BETA2,
    "adam_epsilon": ADAM_EPSILON,
    "lr_scheduler": "constant",
    "max_grad_norm": MAX_GRAD_NORM,
    "gradient_accumulation_steps": GRADIENT_ACCUMULATION_STEPS,
    "lora_target_modules": LORA_TARGET_MODULES,
    "lora_dropout": LORA_DROPOUT,
    "dpo_beta": DPO_BETA,
    "dpo_fixture": DPO_FIXTURE,
    "grpo_fixture": GRPO_FIXTURE,
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
PINNED_VERSIONS = {
    "torch": "2.12.1+cu130",
    "transformers": "5.9.0",
    "triton": "3.7.1",
    "trl": "0.28.0",
    "unsloth": "2026.8.19",
    "unsloth_zoo": "2026.8.13",
}


class BenchmarkError(RuntimeError):
    pass


@dataclass(frozen=True)
class ModelSpec:
    label: str
    path: pathlib.Path
    config_sha256: str


@dataclass(frozen=True)
class ProcessMetrics:
    wall_seconds: float
    peak_gpu_memory_mib: int
    gpu_memory_samples: int


def atomic_write_json(path: pathlib.Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp-{os.getpid()}")
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temporary.replace(path)


def sha256_file(path: pathlib.Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def parse_assignment(raw: str) -> tuple[str, pathlib.Path]:
    label, separator, value = raw.partition("=")
    if not separator or not LABEL_RE.fullmatch(label) or not value:
        raise BenchmarkError(f"--model must use LABEL=PATH with a safe label: {raw!r}")
    return label, pathlib.Path(value).resolve()


def checkpoint_shards(model_dir: pathlib.Path) -> list[pathlib.Path]:
    single = model_dir / "model.safetensors"
    index = model_dir / "model.safetensors.index.json"
    if single.is_file():
        return [single]
    if not index.is_file():
        raise BenchmarkError(
            f"model must contain model.safetensors or model.safetensors.index.json: {model_dir}"
        )
    try:
        parsed = json.loads(index.read_text(encoding="utf-8"))
        weight_map = parsed["weight_map"]
    except (OSError, json.JSONDecodeError, KeyError, TypeError) as error:
        raise BenchmarkError(f"invalid sharded SafeTensors index {index}: {error}") from error
    if not isinstance(weight_map, dict) or not weight_map:
        raise BenchmarkError(f"empty sharded SafeTensors weight_map: {index}")

    resolved_root = model_dir.resolve()
    names: set[str] = set()
    for name in weight_map.values():
        if not isinstance(name, str) or not name:
            raise BenchmarkError(f"invalid shard name in {index}")
        names.add(name)
    shards: list[pathlib.Path] = []
    for name in sorted(names):
        shard = (model_dir / name).resolve()
        try:
            shard.relative_to(resolved_root)
        except ValueError as error:
            raise BenchmarkError(f"shard escapes model directory: {name}") from error
        if not shard.is_file():
            raise BenchmarkError(f"missing SafeTensors shard: {shard}")
        shards.append(shard)
    return shards


def parse_models(values: Iterable[str]) -> list[ModelSpec]:
    result: list[ModelSpec] = []
    seen: set[str] = set()
    for raw in values:
        label, path = parse_assignment(raw)
        if label in seen:
            raise BenchmarkError(f"duplicate model label: {label}")
        seen.add(label)
        config_path = path / "config.json"
        if not config_path.is_file():
            raise BenchmarkError(f"model must contain config.json: {path}")
        checkpoint_shards(path)
        try:
            config = json.loads(config_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as error:
            raise BenchmarkError(f"cannot read model config {config_path}: {error}") from error
        if not isinstance(config, dict) or config.get("model_type") != "gemma4":
            raise BenchmarkError(f"model is not a Gemma4 checkpoint: {path}")
        result.append(ModelSpec(label, path, sha256_file(config_path)))
    if not result:
        raise BenchmarkError("at least one --model LABEL=PATH is required")
    return result


def expected_qv_lora_trainable_tensors(config: dict[str, Any]) -> int:
    text_config = config.get("text_config")
    if not isinstance(text_config, dict):
        raise BenchmarkError("Gemma4 config has no text_config object")
    layers = text_config.get("num_hidden_layers")
    shared = text_config.get("num_kv_shared_layers")
    if (
        not isinstance(layers, int)
        or isinstance(layers, bool)
        or layers <= 0
        or not isinstance(shared, int)
        or isinstance(shared, bool)
        or shared < 0
        or shared >= layers
    ):
        raise BenchmarkError("Gemma4 config has invalid text/KV-sharing layer counts")
    # Every text layer owns q_proj LoRA A/B tensors.  v_proj A/B tensors are
    # present only on the non-shared KV layers in Gemma4.
    return 2 * layers + 2 * (layers - shared)


def finite_number(value: Any, label: str, *, positive: bool = False) -> float:
    if not isinstance(value, (int, float)) or isinstance(value, bool) or not math.isfinite(value):
        raise BenchmarkError(f"{label} must be finite")
    result = float(value)
    if positive and result <= 0:
        raise BenchmarkError(f"{label} must be positive")
    return result


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
            total += memory
            found = True
    return total if found else None


def run_process(command: list[str], log_path: pathlib.Path, timeout_seconds: int) -> ProcessMetrics:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    started = time.monotonic()
    peak_gpu_memory_mib = 0
    gpu_memory_samples = 0
    with log_path.open("w", encoding="utf-8") as log:
        process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT, text=True)
        while process.poll() is None:
            if time.monotonic() - started > timeout_seconds:
                process.terminate()
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
                raise BenchmarkError(f"worker timed out after {timeout_seconds}s; see {log_path}")
            sample = query_gpu_memory_mib(process.pid)
            if sample is not None:
                peak_gpu_memory_mib = max(peak_gpu_memory_mib, sample)
                gpu_memory_samples += 1
            time.sleep(0.2)
        return_code = process.wait()
    wall_seconds = time.monotonic() - started
    if return_code != 0:
        raise BenchmarkError(f"worker exited with status {return_code}; see {log_path}")
    if gpu_memory_samples == 0 or peak_gpu_memory_mib <= 0:
        raise BenchmarkError(f"worker produced no process GPU-memory evidence; see {log_path}")
    return ProcessMetrics(wall_seconds, peak_gpu_memory_mib, gpu_memory_samples)


def validate_worker_result(path: pathlib.Path, objective: str, model: ModelSpec) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise BenchmarkError(f"cannot read worker result {path}: {error}") from error
    if not isinstance(payload, dict) or payload.get("status") != "passed":
        raise BenchmarkError(f"worker did not publish a passing result: {path}")
    if payload.get("objective") != objective or payload.get("model_config_sha256") != model.config_sha256:
        raise BenchmarkError(f"worker result identity mismatch: {path}")
    if payload.get("optimizer_steps") != UPDATES:
        raise BenchmarkError(f"worker did not execute exactly {UPDATES} optimizer steps: {path}")
    if payload.get("versions") != PINNED_VERSIONS:
        raise BenchmarkError(f"worker package fingerprint drifted: {path}")
    finite_number(payload.get("loss"), "worker loss")
    finite_number(payload.get("mean_grad_norm"), "worker mean grad norm", positive=True)
    finite_number(payload.get("torch_peak_gpu_memory_mib"), "worker torch peak memory", positive=True)
    trainable = payload.get("trainable_parameters")
    if not isinstance(trainable, int) or isinstance(trainable, bool) or trainable <= 0:
        raise BenchmarkError(f"worker has invalid trainable-parameter evidence: {path}")
    adapter_path = pathlib.Path(str(payload.get("adapter_path", "")))
    if not (adapter_path / "adapter_config.json").is_file() or not (
        adapter_path / "adapter_model.safetensors"
    ).is_file():
        raise BenchmarkError(f"worker did not publish a PEFT adapter: {path}")
    return payload


def aggregate_case(model: ModelSpec, objective: str, runs: list[dict[str, Any]]) -> dict[str, Any]:
    if len(runs) < MIN_REPETITIONS:
        raise BenchmarkError(f"{model.label}/{objective} requires at least {MIN_REPETITIONS} repetitions")
    walls = [finite_number(run["wall_seconds"], "wall seconds", positive=True) for run in runs]
    rates = [UPDATES / wall for wall in walls]
    peaks = [finite_number(run["peak_gpu_memory_mib"], "peak GPU memory", positive=True) for run in runs]
    losses = [finite_number(run["loss"], "loss") for run in runs]
    grad_norms = [finite_number(run["mean_grad_norm"], "mean grad norm", positive=True) for run in runs]
    return {
        "label": model.label,
        "objective": objective,
        "model_config_sha256": model.config_sha256,
        "optimizer_steps": UPDATES,
        "median_wall_seconds": statistics.median(walls),
        "median_optimizer_steps_per_second": statistics.median(rates),
        "median_peak_gpu_memory_mib": statistics.median(peaks),
        "loss": statistics.median(losses),
        "mean_grad_norm": statistics.median(grad_norms),
        "repetitions": len(runs),
        "wall_seconds": walls,
        "optimizer_steps_per_second": rates,
        "peak_gpu_memory_mib": peaks,
        "worker_results": runs,
    }


def benchmark_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model", action="append", default=[], metavar="LABEL=PATH")
    parser.add_argument("--objective", action="append", choices=("dpo", "grpo"), default=[])
    parser.add_argument("--repetitions", type=int, default=MIN_REPETITIONS)
    parser.add_argument("--out", type=pathlib.Path, required=True)
    parser.add_argument("--timeout-seconds", type=int, default=1800)
    return parser


def worker_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--worker", action="store_true")
    parser.add_argument("--worker-model", type=pathlib.Path, required=True)
    parser.add_argument("--worker-objective", choices=("dpo", "grpo"), required=True)
    parser.add_argument("--worker-dir", type=pathlib.Path, required=True)
    parser.add_argument("--worker-result", type=pathlib.Path, required=True)
    parser.add_argument("--worker-updates", type=int, default=UPDATES)
    return parser


def run_benchmark(argv: list[str]) -> int:
    args = benchmark_parser().parse_args(argv)
    summary_path: pathlib.Path | None = None
    summary: dict[str, Any] | None = None
    try:
        if args.repetitions < MIN_REPETITIONS:
            raise BenchmarkError(f"--repetitions must be at least {MIN_REPETITIONS}")
        if args.timeout_seconds <= 0:
            raise BenchmarkError("--timeout-seconds must be positive")
        models = parse_models(args.model)
        objectives = args.objective or ["dpo", "grpo"]
        out_dir = args.out.resolve()
        if out_dir.exists():
            raise BenchmarkError(f"output directory must not already exist: {out_dir}")
        out_dir.mkdir(parents=True)
        summary_path = out_dir / "summary.json"
        runner_path = pathlib.Path(__file__).resolve()
        summary = {
            "schema_version": SCHEMA_VERSION,
            "status": "running",
            "protocol": PROTOCOL,
            "cases": [],
            "metadata": {
                "created_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "runner_path": str(runner_path),
                "runner_sha256": sha256_file(runner_path),
                "python_executable": sys.executable,
                "pinned_versions": PINNED_VERSIONS,
                "repetitions": args.repetitions,
                "optimizer": {
                    "name": "adamw_torch_fused",
                    "weight_decay": WEIGHT_DECAY,
                    "betas": [ADAM_BETA1, ADAM_BETA2],
                    "epsilon": ADAM_EPSILON,
                    "lr_scheduler": "constant",
                    "max_grad_norm": MAX_GRAD_NORM,
                },
                "objective_contract": {
                    "dpo_beta": DPO_BETA,
                    "dpo_reference": "same-base-adapter-disabled-precomputed",
                    "grpo_group_size": GRPO_GROUP_SIZE,
                    "grpo_clip_epsilon": GRPO_CLIP_EPSILON,
                    "grpo_kl_coef": GRPO_KL_COEF,
                    "grpo_loss_type": "grpo-sequence-normalized",
                    "grpo_reward_scaling": "stock-trl-group-sample-std",
                    "grpo_advantage_epsilon": GRPO_ADVANTAGE_EPSILON,
                    "grpo_advantage_standard_deviation_correction": 1,
                },
            },
        }
        atomic_write_json(summary_path, summary)
        for model in models:
            for objective in objectives:
                runs: list[dict[str, Any]] = []
                for repetition in range(1, args.repetitions + 1):
                    run_dir = out_dir / model.label / objective / f"run-{repetition:02d}"
                    run_dir.mkdir(parents=True)
                    result_path = run_dir / "worker.json"
                    log_path = run_dir / "worker.log"
                    command = [
                        sys.executable,
                        str(runner_path),
                        "--worker",
                        "--worker-model",
                        str(model.path),
                        "--worker-objective",
                        objective,
                        "--worker-dir",
                        str(run_dir),
                        "--worker-result",
                        str(result_path),
                        "--worker-updates",
                        str(UPDATES),
                    ]
                    metrics = run_process(command, log_path, args.timeout_seconds)
                    worker = validate_worker_result(result_path, objective, model)
                    runs.append(
                        {
                            **worker,
                            "repetition": repetition,
                            "wall_seconds": metrics.wall_seconds,
                            "peak_gpu_memory_mib": metrics.peak_gpu_memory_mib,
                            "gpu_memory_samples": metrics.gpu_memory_samples,
                            "log_path": str(log_path),
                            "worker_result_path": str(result_path),
                        }
                    )
                    summary["metadata"]["last_completed"] = f"{model.label}/{objective}/run-{repetition:02d}"
                    atomic_write_json(summary_path, summary)
                summary["cases"].append(aggregate_case(model, objective, runs))
                atomic_write_json(summary_path, summary)
        summary["status"] = "passed"
        summary["metadata"]["completed_at_utc"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        atomic_write_json(summary_path, summary)
        print(summary_path)
        return 0
    except BenchmarkError as error:
        if summary_path is not None and summary is not None:
            summary["status"] = "failed"
            summary["metadata"]["completed_at_utc"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            summary["metadata"]["failure"] = str(error)
            try:
                atomic_write_json(summary_path, summary)
            except OSError as write_error:
                print(f"Could not publish failed benchmark summary: {write_error}", file=sys.stderr)
        print(f"Gemma4 Unsloth preference benchmark error: {error}", file=sys.stderr)
        return 2


def installed_versions() -> dict[str, str]:
    import importlib.metadata

    return {name: importlib.metadata.version(name) for name in PINNED_VERSIONS}


def configure_worker_environment() -> None:
    """Enforce the worker's local-only, non-reporting execution boundary."""
    os.environ["HF_HUB_OFFLINE"] = "1"
    os.environ["TRANSFORMERS_OFFLINE"] = "1"
    os.environ["WANDB_DISABLED"] = "true"
    os.environ["TOKENIZERS_PARALLELISM"] = "false"


def render_user_prompt(processor: Any, prompt: str) -> str:
    messages = [{"role": "user", "content": [{"type": "text", "text": prompt}]}]
    rendered = processor.apply_chat_template(messages, tokenize=False, add_generation_prompt=True)
    if not isinstance(rendered, str) or not rendered:
        raise BenchmarkError("Gemma4 processor produced an empty prompt")
    return rendered


def sequence_hash_reward(completion_ids: list[list[int]]) -> list[float]:
    rewards: list[float] = []
    for completion in completion_ids:
        value = 0xCBF29CE484222325
        for token in completion:
            value ^= int(token) & 0xFFFFFFFF
            value = (value * 0x100000001B3) & 0xFFFFFFFFFFFFFFFF
        rewards.append((value >> 40) / 16_777_215.0)
    return rewards


def training_evidence(trainer: Any, train_output: Any) -> tuple[float, float, list[float]]:
    loss = finite_number(getattr(train_output, "training_loss", None), "training loss")
    grad_norms: list[float] = []
    for entry in trainer.state.log_history:
        if not isinstance(entry, dict) or "grad_norm" not in entry:
            continue
        value = finite_number(entry["grad_norm"], "logged grad norm")
        if value > 0:
            grad_norms.append(value)
    if not grad_norms:
        raise BenchmarkError("trainer did not log any positive gradient norms")
    return loss, statistics.fmean(grad_norms), grad_norms


def run_worker(argv: list[str]) -> int:
    args = worker_parser().parse_args(argv)
    try:
        if args.worker_updates <= 0:
            raise BenchmarkError("worker updates must be positive")
        worker_dir = args.worker_dir.resolve()
        result_path = args.worker_result.resolve()
        adapter_path = worker_dir / "adapter"
        trainer_path = worker_dir / "trainer"
        worker_dir.mkdir(parents=True, exist_ok=True)
        versions = installed_versions()
        if versions != PINNED_VERSIONS:
            raise BenchmarkError(f"pinned package mismatch: expected {PINNED_VERSIONS}, got {versions}")

        configure_worker_environment()

        from unsloth import FastModel
        import torch
        from datasets import Dataset
        from transformers import set_seed
        from trl import DPOConfig, DPOTrainer, GRPOConfig, GRPOTrainer

        if not torch.cuda.is_available():
            raise BenchmarkError("CUDA is unavailable")
        set_seed(SEED)
        torch.cuda.set_device(0)
        torch.cuda.empty_cache()
        torch.cuda.reset_peak_memory_stats()
        model_path = args.worker_model.resolve()
        config_path = model_path / "config.json"
        config_sha256 = sha256_file(config_path)
        try:
            model_config = json.loads(config_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as error:
            raise BenchmarkError(f"cannot read model config {config_path}: {error}") from error
        if not isinstance(model_config, dict):
            raise BenchmarkError(f"model config is not an object: {config_path}")
        expected_trainable_tensors = expected_qv_lora_trainable_tensors(model_config)
        model, processor = FastModel.from_pretrained(
            model_name=str(model_path),
            max_seq_length=MAX_SEQ_LEN,
            dtype=torch.bfloat16,
            load_in_4bit=False,
            load_in_8bit=False,
            load_in_16bit=True,
            full_finetuning=False,
            device_map="sequential",
            trust_remote_code=False,
            use_gradient_checkpointing="unsloth",
            random_state=SEED,
            fast_inference=False,
            text_only=False,
        )
        model = FastModel.get_peft_model(
            model,
            r=LORA_RANK,
            target_modules=LORA_TARGET_MODULES,
            lora_alpha=LORA_ALPHA,
            lora_dropout=LORA_DROPOUT,
            bias="none",
            finetune_vision_layers=False,
            finetune_language_layers=True,
            finetune_attention_modules=True,
            finetune_mlp_modules=False,
            finetune_audio_layers=False,
            use_gradient_checkpointing="unsloth",
            random_state=SEED,
            use_rslora=False,
            loftq_config=None,
        )
        trainable_inventory = [name for name, parameter in model.named_parameters() if parameter.requires_grad]
        trainable_parameters = sum(parameter.numel() for parameter in model.parameters() if parameter.requires_grad)
        trainable_tensors = len(trainable_inventory)
        if trainable_tensors != expected_trainable_tensors:
            raise BenchmarkError(
                f"resolved {trainable_tensors} trainable tensors, expected {expected_trainable_tensors} "
                "text q/v tensors from the checkpoint topology"
            )
        if any(
            "language_model" not in name or not ("q_proj" in name or "v_proj" in name)
            for name in trainable_inventory
        ):
            raise BenchmarkError("LoRA inventory escaped the Gemma4 language q/v scope")
        trainable_inventory_sha256 = hashlib.sha256("\n".join(trainable_inventory).encode()).hexdigest()
        tokenizer = processor.tokenizer if hasattr(processor, "tokenizer") else processor
        common = {
            "output_dir": str(trainer_path),
            "max_steps": args.worker_updates,
            "learning_rate": LEARNING_RATE,
            "lr_scheduler_type": "constant",
            "warmup_steps": 0,
            "optim": "adamw_torch_fused",
            "weight_decay": WEIGHT_DECAY,
            "adam_beta1": ADAM_BETA1,
            "adam_beta2": ADAM_BETA2,
            "adam_epsilon": ADAM_EPSILON,
            "gradient_accumulation_steps": GRADIENT_ACCUMULATION_STEPS,
            "max_grad_norm": MAX_GRAD_NORM,
            "bf16": True,
            "fp16": False,
            "tf32": False,
            "gradient_checkpointing": True,
            "use_cache": False,
            "logging_strategy": "steps",
            "logging_steps": 1,
            "logging_first_step": True,
            "save_strategy": "no",
            "eval_strategy": "no",
            "report_to": "none",
            "disable_tqdm": True,
            "skip_memory_metrics": True,
            "dataloader_num_workers": 0,
            "dataloader_pin_memory": False,
            "seed": SEED,
            "data_seed": SEED,
        }

        if args.worker_objective == "dpo":
            rendered_prompt = render_user_prompt(processor, DPO_FIXTURE["prompt"])
            dataset = Dataset.from_list(
                [{"prompt": rendered_prompt, "chosen": DPO_FIXTURE["chosen"], "rejected": DPO_FIXTURE["rejected"]}]
            )
            training_args = DPOConfig(
                **common,
                per_device_train_batch_size=1,
                max_length=MAX_SEQ_LEN,
                max_prompt_length=MAX_SEQ_LEN,
                max_completion_length=MAX_SEQ_LEN,
                beta=DPO_BETA,
                loss_type="sigmoid",
                precompute_ref_log_probs=True,
                precompute_ref_batch_size=1,
                remove_unused_columns=True,
            )
            trainer = DPOTrainer(
                model=model,
                ref_model=None,
                args=training_args,
                train_dataset=dataset,
                processing_class=processor,
            )
            input_contract = {
                "raw": DPO_FIXTURE,
                "rendered_prompt": rendered_prompt,
                "prompt_ids": tokenizer(rendered_prompt, add_special_tokens=False)["input_ids"],
                "chosen_ids": tokenizer(DPO_FIXTURE["chosen"], add_special_tokens=False)["input_ids"]
                + [tokenizer.eos_token_id],
                "rejected_ids": tokenizer(DPO_FIXTURE["rejected"], add_special_tokens=False)["input_ids"]
                + [tokenizer.eos_token_id],
            }
            objective_config = {
                "beta": DPO_BETA,
                "loss_type": "sigmoid",
                "reference": "same-base-adapter-disabled-precomputed",
            }
        else:
            rendered_prompt = render_user_prompt(processor, GRPO_FIXTURE["prompt"])
            dataset = Dataset.from_list([{"prompt": rendered_prompt, "target": GRPO_TARGET}])

            def token_hash_reward(completion_ids: list[list[int]], **_: Any) -> list[float]:
                return sequence_hash_reward(completion_ids)

            training_args = GRPOConfig(
                **common,
                per_device_train_batch_size=GRPO_GROUP_SIZE,
                remove_unused_columns=False,
                num_generations=GRPO_GROUP_SIZE,
                generation_batch_size=GRPO_GROUP_SIZE,
                max_completion_length=GRPO_MAX_COMPLETION_TOKENS,
                temperature=1.0,
                top_p=1.0,
                top_k=None,
                repetition_penalty=1.0,
                use_vllm=False,
                use_transformers_paged=False,
                beta=GRPO_KL_COEF,
                epsilon=GRPO_CLIP_EPSILON,
                num_iterations=1,
                scale_rewards="group",
                loss_type="grpo",
                shuffle_dataset=False,
            )
            trainer = GRPOTrainer(
                model=model,
                reward_funcs=token_hash_reward,
                args=training_args,
                train_dataset=dataset,
                processing_class=processor,
            )
            input_contract = {
                "raw": GRPO_FIXTURE,
                "rendered_prompt": rendered_prompt,
                "prompt_ids": tokenizer(rendered_prompt, add_special_tokens=False)["input_ids"],
                "reward_mode": "sequence-hash-fnv1a-token-ids",
            }
            objective_config = {
                "group_size": GRPO_GROUP_SIZE,
                "max_completion_tokens": GRPO_MAX_COMPLETION_TOKENS,
                "clip_epsilon": GRPO_CLIP_EPSILON,
                "kl_coef": GRPO_KL_COEF,
                "loss_type": "grpo-sequence-normalized",
                "advantage_normalization": "stock-trl-group-sample-std",
                "advantage_epsilon": GRPO_ADVANTAGE_EPSILON,
                "advantage_standard_deviation_correction": 1,
            }

        train_started = time.monotonic()
        train_output = trainer.train()
        torch.cuda.synchronize()
        train_seconds = time.monotonic() - train_started
        if trainer.state.global_step != args.worker_updates:
            raise BenchmarkError(
                f"trainer executed {trainer.state.global_step} optimizer steps, expected {args.worker_updates}"
            )
        loss, mean_grad_norm, grad_norms = training_evidence(trainer, train_output)
        model.save_pretrained(adapter_path, safe_serialization=True)
        torch.cuda.synchronize()
        adapter_weights = adapter_path / "adapter_model.safetensors"
        if not adapter_weights.is_file():
            raise BenchmarkError("PEFT adapter publication did not produce adapter_model.safetensors")
        result = {
            "status": "passed",
            "objective": args.worker_objective,
            "model_path": str(model_path),
            "model_config_sha256": config_sha256,
            "optimizer_steps": trainer.state.global_step,
            "loss": loss,
            "mean_grad_norm": mean_grad_norm,
            "grad_norms": grad_norms,
            "train_seconds": train_seconds,
            "torch_peak_gpu_memory_mib": torch.cuda.max_memory_allocated() / (1024 * 1024),
            "torch_peak_reserved_gpu_memory_mib": torch.cuda.max_memory_reserved() / (1024 * 1024),
            "trainable_parameters": trainable_parameters,
            "trainable_tensors": trainable_tensors,
            "expected_trainable_tensors": expected_trainable_tensors,
            "trainable_inventory_sha256": trainable_inventory_sha256,
            "adapter_path": str(adapter_path),
            "adapter_sha256": sha256_file(adapter_weights),
            "adapter_bytes": adapter_weights.stat().st_size,
            "versions": versions,
            "gpu": {
                "name": torch.cuda.get_device_name(0),
                "capability": list(torch.cuda.get_device_capability(0)),
                "cuda_runtime": torch.version.cuda,
            },
            "input_contract": input_contract,
            "objective_config": objective_config,
            "optimizer_config": {
                "name": "adamw_torch_fused",
                "learning_rate": LEARNING_RATE,
                "weight_decay": WEIGHT_DECAY,
                "betas": [ADAM_BETA1, ADAM_BETA2],
                "epsilon": ADAM_EPSILON,
                "lr_scheduler": "constant",
                "gradient_accumulation_steps": GRADIENT_ACCUMULATION_STEPS,
                "max_grad_norm": MAX_GRAD_NORM,
            },
        }
        atomic_write_json(result_path, result)
        return 0
    except Exception as error:
        failure = {"status": "failed", "error_type": type(error).__name__, "error": str(error)}
        try:
            atomic_write_json(args.worker_result.resolve(), failure)
        except Exception:
            pass
        raise


def main(argv: list[str] | None = None) -> int:
    arguments = list(sys.argv[1:] if argv is None else argv)
    if "--worker" in arguments:
        return run_worker(arguments)
    return run_benchmark(arguments)


if __name__ == "__main__":
    raise SystemExit(main())
