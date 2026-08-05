#!/usr/bin/env python3
"""Reject synthetic, repeated, or leaking data before a GLiNER2 release gate."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from gliner2_release_contract import (
    MODEL_FINGERPRINT_ENTRIES,
    FingerprintEntry,
    canonical_text,
    directory_fingerprint as contract_directory_fingerprint,
)


TASK_KINDS = frozenset({"entities", "classifications", "json_structures", "relations"})
MODEL_FINGERPRINT_FILES = (
    *(entry.relative_path for entry in MODEL_FINGERPRINT_ENTRIES),
)
PEFT_ADAPTER_FILES = ("adapter_model.safetensors", "adapter_config.json")
ADAPTER_BUNDLE_FILES = (*PEFT_ADAPTER_FILES, "task_head.safetensors")
FULL_TASK_OBJECTIVE = "gliner2-total-loss"


@dataclass(frozen=True)
class DatasetContent:
    record_count: int
    has_repeated_records: bool
    texts: frozenset[bytes]
    tasks: frozenset[str]


def jsonl_files(path: Path) -> Iterable[Path]:
    if path.is_file():
        yield path
        return
    if path.is_dir():
        files = sorted(path.glob("*.jsonl"))
        if files:
            yield from files
            return
    raise ValueError(f"{path}: expected a JSONL file or directory containing JSONL files")


def digest(value: Any) -> bytes:
    encoded = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).digest()


def record_text(record: dict[str, Any]) -> str:
    text = record.get("input", record.get("text"))
    if not isinstance(text, str) or not text.strip():
        raise ValueError("record has no non-empty input/text")
    return canonical_text(text)


def sha256_file(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            hasher.update(chunk)
    return f"sha256:{hasher.hexdigest()}"


def directory_fingerprint(directory: Path, relative_paths: tuple[str, ...]) -> str:
    """Fingerprint required paths; retained for adapter-bundle compatibility."""
    return contract_directory_fingerprint(
        directory,
        tuple(FingerprintEntry(relative_path) for relative_path in relative_paths),
    )


def base_model_fingerprint(model_dir: Path) -> str:
    return contract_directory_fingerprint(model_dir, MODEL_FINGERPRINT_ENTRIES)


def peft_adapter_fingerprint(adapter_dir: Path) -> str:
    return directory_fingerprint(adapter_dir, PEFT_ADAPTER_FILES)


def adapter_bundle_fingerprint(adapter_dir: Path) -> str:
    return directory_fingerprint(adapter_dir, ADAPTER_BUNDLE_FILES)


def production_accelerator_step_metrics(
    adapter_dir: Path,
    expected_steps: int,
    backend: str,
) -> list[dict[str, Any]]:
    """Load retained step evidence proving the selected accelerator trained the adapter."""
    if backend not in {"metal", "cuda"}:
        raise ValueError(f"unsupported release accelerator backend: {backend}")
    backend_label = backend.upper()
    metrics_path = adapter_dir / "training_metrics.jsonl"
    try:
        lines = metrics_path.read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        raise ValueError(f"release adapter has no retained {backend_label} training metrics: {metrics_path}: {exc}") from exc
    steps: list[dict[str, Any]] = []
    for line_no, raw in enumerate(lines, 1):
        if not raw.strip():
            continue
        try:
            row = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ValueError(f"release adapter has invalid retained {backend_label} training metrics at line {line_no}: {exc}") from exc
        if not isinstance(row, dict):
            raise ValueError(f"release adapter training metric line {line_no} must be an object")
        if row.get("event") == "step":
            steps.append(row)
    if len(steps) != expected_steps:
        raise ValueError(
            f"release adapter retained {backend_label} training metrics have {len(steps)} steps, expected {expected_steps}"
        )
    cuda_upload_synchronization_total = 0
    for index, row in enumerate(steps, 1):
        integer_zero_fields = (
            "device_trainable_transfer_count",
            "device_resident_transfer_count",
            "graph_executor_interpreter_fallbacks",
            "graph_executor_true_host_outputs",
        )
        positive_integer_fields = (
            "device_trainable_bytes",
            "graph_executor_partitions",
        )
        if row.get("step") != index or row.get("optimizer_backend") != backend:
            raise ValueError(f"release adapter step {index} was not executed by the {backend_label} optimizer")
        if any(type(row.get(name)) is not int or row[name] != 0 for name in integer_zero_fields):
            raise ValueError(
                f"release adapter step {index} has device transfers, interpreter fallback, or true host output"
            )
        if any(type(row.get(name)) is not int or row[name] <= 0 for name in positive_integer_fields):
            raise ValueError(f"release adapter step {index} lacks compiled device-resident dispatch evidence")
        command_dispatches = row.get("graph_executor_command_dispatches")
        planned_dispatches = row.get("graph_executor_planned_dispatches")
        if (
            type(command_dispatches) is not int
            or type(planned_dispatches) is not int
            or command_dispatches < 0
            or planned_dispatches < 0
            or command_dispatches + planned_dispatches <= 0
        ):
            raise ValueError(f"release adapter step {index} lacks compiled device-resident dispatch evidence")
        if (
            "graph_executor_fallback_reason" not in row
            or row.get("graph_executor_fallback_reason") not in (None, "")
        ):
            raise ValueError(f"release adapter step {index} reports a graph-executor fallback reason")
        if backend == "cuda":
            cuda_integer_fields = (
                "cuda_h2d_bytes",
                "cuda_training_input_uploads",
                "cuda_training_input_upload_bytes",
                "cuda_d2h_bytes",
                "cuda_largest_d2h_transfer_bytes",
                "cuda_to_float32_calls",
                "cuda_upload_synchronizations",
                "cuda_packed_attention_forward_calls",
                "cuda_packed_attention_backward_calls",
                "cuda_exact_gelu_forward_calls",
                "cuda_exact_gelu_backward_calls",
            )
            if any(type(row.get(name)) is not int or row[name] < 0 for name in cuda_integer_fields):
                raise ValueError(f"release adapter step {index} lacks complete CUDA runtime telemetry")
            if (
                row["cuda_training_input_uploads"] <= 0
                or row["cuda_training_input_upload_bytes"] <= 0
                or row["cuda_h2d_bytes"] < row["cuda_training_input_upload_bytes"]
            ):
                raise ValueError(f"release adapter step {index} does not account for full-step CUDA input H2D")
            if (
                row["cuda_d2h_bytes"] > 8
                or row["cuda_largest_d2h_transfer_bytes"] > 4
                or row["cuda_to_float32_calls"] > 2
                or row["cuda_upload_synchronizations"] > (1 if index == 1 else 0)
            ):
                raise ValueError(f"release adapter step {index} has disallowed CUDA transfer or synchronization telemetry")
            cuda_upload_synchronization_total += row["cuda_upload_synchronizations"]
            if any(
                row[name] <= 0
                for name in (
                    "cuda_packed_attention_forward_calls",
                    "cuda_packed_attention_backward_calls",
                    "cuda_exact_gelu_forward_calls",
                    "cuda_exact_gelu_backward_calls",
                )
            ):
                raise ValueError(f"release adapter step {index} lacks required CUDA fused-kernel coverage")
    if backend == "cuda" and cuda_upload_synchronization_total > 1:
        raise ValueError("release adapter has more than one cold-start CUDA upload synchronization")
    return steps


def json_object(path: Path, label: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"{label} is missing or invalid: {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise ValueError(f"{label} must be a JSON object: {path}")
    return value


def is_sha256_fingerprint(value: Any) -> bool:
    return (
        isinstance(value, str)
        and value.startswith("sha256:")
        and len(value) == len("sha256:") + 64
        and all(char in "0123456789abcdef" for char in value[len("sha256:") :])
    )


def validate_release_artifact_provenance(
    model_dir: Path,
    train_data: Path,
    adapter_dir: Path,
    train_record_count: int | None = None,
    eval_data: Path | None = None,
    backend: str = "metal",
) -> None:
    backend = backend.lower()
    if backend not in {"metal", "cuda"}:
        raise ValueError(f"unsupported release accelerator backend: {backend}")
    if not train_data.is_file():
        raise ValueError("release adapter provenance requires --train to be one JSONL file")
    manifest = json_object(adapter_dir / "training_manifest.json", "Zig training manifest")
    adapter_config = json_object(adapter_dir / "adapter_config.json", "PEFT adapter config")

    if manifest.get("schema_version") != "gliner2_autodiff_training/v3":
        raise ValueError("release adapter has an unsupported Zig training manifest")
    if manifest.get("objective") != FULL_TASK_OBJECTIVE or manifest.get("lora_only_trainables") is not True:
        raise ValueError("release adapter was not produced by LoRA-only full-task training")
    if str(manifest.get("backend", "")).lower() != backend or manifest.get("compiled_required") is not True:
        raise ValueError(f"release adapter provenance requires {backend.upper()} training with compiled_required=true")
    if manifest.get("training_precision") != "fp32" or manifest.get("optimizer_state_precision") != "fp32":
        raise ValueError("release adapter provenance requires FP32 training and optimizer state")
    lora_dropout = manifest.get("lora_dropout")
    span_negative_mask_rate = manifest.get("span_negative_mask_rate")
    if (
        manifest.get("deterministic") is not False
        or manifest.get("sampling_config") != "disabled"
        or manifest.get("schema_conditioning_policy") != "deterministic-eval-form"
        or manifest.get("model_dropout") != "disabled"
        or manifest.get("train_shuffle") is not True
        or type(lora_dropout) not in (int, float)
        or not math.isfinite(lora_dropout)
        or float(lora_dropout) != 0.0
        or type(span_negative_mask_rate) not in (int, float)
        or not math.isfinite(span_negative_mask_rate)
        or float(span_negative_mask_rate) != 0.5
    ):
        raise ValueError("release adapter has invalid production training-policy provenance")
    cache_capacity = manifest.get("graph_cache_capacity")
    cache_stat_names = (
        "graph_cache_build_reserve_bytes",
        "graph_cache_builds",
        "graph_cache_hits",
        "graph_cache_active_reuses",
        "graph_cache_evictions",
        "graph_cache_resident_signatures",
        "graph_cache_peak_resident_signatures",
    )
    cache_stats = {name: manifest.get(name) for name in cache_stat_names}
    if (
        manifest.get("graph_shape_policy") != "batch-local-v2"
        or type(manifest.get("graph_seq_bucket_multiple")) is not int
        or manifest.get("graph_seq_bucket_multiple") != 8
        or manifest.get("graph_span_word_bucket_policy") != "bounded-next-power-of-two"
        or manifest.get("graph_schema_bucket_policy") != "bounded-next-power-of-two"
        or type(cache_capacity) is not int
        or not 1 <= cache_capacity <= 8
        or manifest.get("graph_cache_runtime_input_policy") != "active-entry-only"
        or any(type(value) is not int or value < 0 for value in cache_stats.values())
        or cache_stats["graph_cache_build_reserve_bytes"] == 0
        or cache_stats["graph_cache_builds"] == 0
        or cache_stats["graph_cache_resident_signatures"] == 0
        or cache_stats["graph_cache_resident_signatures"] > cache_stats["graph_cache_peak_resident_signatures"]
        or cache_stats["graph_cache_peak_resident_signatures"] > cache_capacity
        or cache_stats["graph_cache_resident_signatures"] > cache_stats["graph_cache_builds"]
        or cache_stats["graph_cache_evictions"] > cache_stats["graph_cache_builds"]
    ):
        raise ValueError("release adapter has invalid production graph-shape/cache provenance")
    if (
        manifest.get("initial_adapter_checkpoint_used") is not False
        or manifest.get("initial_adapter_checkpoint") is not None
        or manifest.get("initial_adapter_checkpoint_sha256") is not None
    ):
        raise ValueError("release adapter provenance requires clean base-model initialization")
    resume_used = manifest.get("resume_checkpoint_used", False)
    if type(resume_used) is not bool:
        raise ValueError("release adapter has invalid resume provenance")
    resume_path = manifest.get("resume_checkpoint")
    resume_sha256 = manifest.get("resume_checkpoint_sha256")
    state_fingerprint = manifest.get("training_state_fingerprint_sha256")
    restored_micro_batches = manifest.get("resume_restored_micro_batch_steps")
    restored_optimizer_steps = manifest.get("resume_restored_optimizer_steps")
    restored_epochs = manifest.get("resume_restored_epochs")
    if resume_used:
        if (
            not isinstance(resume_path, str)
            or not resume_path.strip()
            or not is_sha256_fingerprint(resume_sha256)
            or not is_sha256_fingerprint(state_fingerprint)
            or not all(
                type(value) is int and value > 0
                for value in (restored_micro_batches, restored_optimizer_steps, restored_epochs)
            )
            or restored_optimizer_steps > restored_micro_batches
        ):
            raise ValueError("release adapter has incomplete resume provenance")
        expected_resume_path = (
            f"checkpoints/resume-source-{resume_sha256.removeprefix('sha256:')}.safetensors"
        )
        if resume_path != expected_resume_path:
            raise ValueError("release adapter has invalid retained resume-checkpoint path")
        retained_resume_checkpoint = adapter_dir / resume_path
        if (
            not retained_resume_checkpoint.is_file()
            or sha256_file(retained_resume_checkpoint) != resume_sha256
        ):
            raise ValueError("release adapter resume-checkpoint fingerprint does not match its manifest")
    elif any(
        value is not None
        for value in (
            resume_path,
            resume_sha256,
            restored_micro_batches,
            restored_optimizer_steps,
            restored_epochs,
        )
    ):
        raise ValueError("release adapter has inconsistent resume provenance")
    if state_fingerprint is not None and not is_sha256_fingerprint(state_fingerprint):
        raise ValueError("release adapter has invalid training-state fingerprint")

    manifest_eval_data = manifest.get("eval_data")
    if not isinstance(manifest_eval_data, str) or not manifest_eval_data.strip():
        raise ValueError("release adapter provenance requires held-out eval data")
    resolved_eval_data = eval_data if eval_data is not None else Path(manifest_eval_data)
    if not resolved_eval_data.is_file():
        raise ValueError("release adapter provenance requires --eval to be one JSONL file")
    if manifest.get("eval_data_sha256") != sha256_file(resolved_eval_data):
        raise ValueError("release adapter eval-data fingerprint does not match --eval")

    eval_every_epochs = manifest.get("eval_every_epochs")
    eval_batch_size = manifest.get("eval_batch_size")
    early_stopping_patience = manifest.get("early_stopping_patience")
    early_stopping_threshold = manifest.get("early_stopping_threshold")
    early_stopped = manifest.get("early_stopped")
    eval_count = manifest.get("eval_count")
    best_eval_loss = manifest.get("best_eval_loss")
    best_epoch = manifest.get("best_epoch")
    bad_epochs = manifest.get("early_stopping_bad_epochs")
    if (
        type(eval_every_epochs) is not int
        or eval_every_epochs <= 0
        or type(eval_batch_size) is not int
        or eval_batch_size <= 0
        or type(early_stopping_patience) is not int
        or early_stopping_patience < 0
        or not isinstance(early_stopping_threshold, (int, float))
        or isinstance(early_stopping_threshold, bool)
        or not math.isfinite(early_stopping_threshold)
        or early_stopping_threshold < 0
        or type(early_stopped) is not bool
        or type(eval_count) is not int
        or eval_count <= 0
        or not isinstance(best_eval_loss, (int, float))
        or isinstance(best_eval_loss, bool)
        or not math.isfinite(best_eval_loss)
        or best_eval_loss < 0
        or type(best_epoch) is not int
        or best_epoch <= 0
        or type(bad_epochs) is not int
        or bad_epochs < 0
    ):
        raise ValueError("release adapter has invalid held-out model-selection provenance")
    selected_checkpoint = manifest.get("selected_checkpoint")
    best_checkpoint_sha256 = manifest.get("best_checkpoint_sha256")
    expected_selected_checkpoint = f"checkpoints/best-epoch-{best_epoch}.safetensors"
    if (
        selected_checkpoint != expected_selected_checkpoint
        or not is_sha256_fingerprint(best_checkpoint_sha256)
        or not is_sha256_fingerprint(state_fingerprint)
    ):
        raise ValueError("release adapter has incomplete best-checkpoint provenance")
    best_checkpoint = adapter_dir / selected_checkpoint
    if not best_checkpoint.is_file() or sha256_file(best_checkpoint) != best_checkpoint_sha256:
        raise ValueError("release adapter best-checkpoint fingerprint does not match its manifest")
    max_examples = manifest.get("max_examples")
    if type(max_examples) is not int or max_examples != 0:
        raise ValueError("release adapter provenance requires training over the complete dataset")
    max_steps = manifest.get("max_steps")
    requested_max_steps = manifest.get("requested_max_steps")
    if (
        type(max_steps) is not int
        or type(requested_max_steps) is not int
        or max_steps != 0
        or requested_max_steps != 0
    ):
        raise ValueError("release adapter provenance requires uncapped training with max_steps=0")
    steps_per_epoch = manifest.get("steps_per_epoch")
    grad_accum = manifest.get("grad_accum")
    optimizer_steps = manifest.get("optimizer_steps")
    epochs = manifest.get("epochs")
    requested_epochs = manifest.get("requested_epochs")
    effective_steps = manifest.get("effective_steps")
    micro_batch_steps = manifest.get("micro_batch_steps")
    total_steps = manifest.get("total_steps")
    example_count = manifest.get("example_count")
    examples_per_epoch = manifest.get("examples_per_epoch")
    batch_size = manifest.get("batch_size")
    effective_batch_size = manifest.get("effective_batch_size")
    if not all(
        type(value) is int and value > 0
        for value in (
            steps_per_epoch,
            grad_accum,
            optimizer_steps,
            epochs,
            requested_epochs,
            effective_steps,
            micro_batch_steps,
            total_steps,
            example_count,
            examples_per_epoch,
            batch_size,
            effective_batch_size,
        )
    ):
        raise ValueError("release adapter manifest has invalid epoch or optimizer-step accounting")
    actual_record_count = train_record_count if train_record_count is not None else load_dataset(train_data).record_count
    if example_count != actual_record_count:
        raise ValueError("release adapter manifest example count does not match the training JSONL")
    if examples_per_epoch != example_count:
        raise ValueError("release adapter training does not consume every training record")
    if (
        effective_batch_size != min(example_count, batch_size)
        or steps_per_epoch * effective_batch_size != examples_per_epoch
    ):
        raise ValueError("release adapter manifest has inconsistent batch accounting")
    optimizer_steps_per_epoch = max((steps_per_epoch + grad_accum - 1) // grad_accum, 1)
    expected_micro_batch_steps = epochs * steps_per_epoch
    expected_optimizer_steps = epochs * optimizer_steps_per_epoch
    if (
        optimizer_steps != expected_optimizer_steps
        or effective_steps != optimizer_steps
        or micro_batch_steps != expected_micro_batch_steps
        or total_steps != expected_micro_batch_steps
    ):
        raise ValueError("release adapter manifest has inconsistent completed-run accounting")
    if (
        epochs > requested_epochs
        or eval_count != epochs // eval_every_epochs
        or best_epoch > epochs
        or best_epoch % eval_every_epochs != 0
    ):
        raise ValueError("release adapter has inconsistent held-out model-selection accounting")
    if early_stopped:
        if (
            early_stopping_patience <= 0
            or bad_epochs != early_stopping_patience
            or epochs % eval_every_epochs != 0
        ):
            raise ValueError("release adapter has inconsistent early-stopping provenance")
    elif epochs != requested_epochs or (
        early_stopping_patience > 0 and bad_epochs >= early_stopping_patience
    ):
        raise ValueError("release adapter provenance requires completion or a valid early stop")
    if resume_used and (
        restored_micro_batches != restored_epochs * steps_per_epoch
        or restored_optimizer_steps != restored_epochs * optimizer_steps_per_epoch
        or restored_micro_batches > total_steps
        or restored_optimizer_steps > optimizer_steps
        or restored_epochs > epochs
    ):
        raise ValueError("release adapter has inconsistent resumed-run accounting")
    production_accelerator_step_metrics(adapter_dir, micro_batch_steps, backend)
    expected_train = sha256_file(train_data)
    if manifest.get("train_data_sha256") != expected_train:
        raise ValueError("release adapter training-data fingerprint does not match --train")
    expected_model = base_model_fingerprint(model_dir)
    if manifest.get("base_model_fingerprint_sha256") != expected_model:
        raise ValueError("release adapter base-model fingerprint does not match --model-dir")
    expected_adapter = adapter_bundle_fingerprint(adapter_dir)
    if manifest.get("adapter_bundle_fingerprint_sha256") != expected_adapter:
        raise ValueError("release adapter bundle fingerprint does not match its Zig training manifest")
    manifest_model = manifest.get("model_dir")
    if not isinstance(manifest_model, str) or adapter_config.get("base_model_name_or_path") != manifest_model:
        raise ValueError("release adapter config and Zig training manifest disagree on the base model")
    config_rank, manifest_rank = adapter_config.get("r"), manifest.get("lora_rank")
    config_alpha, manifest_alpha = adapter_config.get("lora_alpha"), manifest.get("lora_alpha")
    config_dropout, manifest_dropout = adapter_config.get("lora_dropout"), manifest.get("lora_dropout")
    config_targets, manifest_targets = adapter_config.get("target_modules"), manifest.get("resolved_lora_targets")
    numeric_metadata = (config_alpha, manifest_alpha, config_dropout, manifest_dropout)
    if (
        type(config_rank) is not int
        or type(manifest_rank) is not int
        or config_rank <= 0
        or config_rank != manifest_rank
        or not all(isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value) for value in numeric_metadata)
        or float(config_alpha) != float(manifest_alpha)
        or float(config_dropout) != float(manifest_dropout)
        or not isinstance(config_targets, list)
        or not isinstance(manifest_targets, list)
        or not config_targets
        or any(not isinstance(value, str) or not value for value in config_targets + manifest_targets)
        or config_targets != manifest_targets
    ):
        raise ValueError("release adapter config and Zig training manifest disagree on LoRA metadata")


def has_value(value: Any) -> bool:
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, list):
        return any(has_value(item) for item in value)
    if isinstance(value, dict):
        if "value" in value:
            return has_value(value["value"])
        return any(has_value(item) for item in value.values())
    return value is not None


def record_tasks(record: dict[str, Any]) -> set[str]:
    tasks: set[str] = set()
    legacy_entities = record.get("entities")
    if legacy_entities is not None:
        if not isinstance(legacy_entities, list):
            raise ValueError("legacy entities must be an array")
        if any(isinstance(item, dict) and has_value(item.get("text")) for item in legacy_entities):
            tasks.add("entities")
    output = record.get("output")
    if isinstance(output, dict):
        entities = output.get("entities")
        if entities is not None:
            if not isinstance(entities, dict):
                raise ValueError("output.entities must be an object")
            if any(has_value(value) for value in entities.values()):
                tasks.add("entities")
        classifications = output.get("classifications")
        if classifications is not None:
            if not isinstance(classifications, list):
                raise ValueError("output.classifications must be an array")
            if any(
                isinstance(item, dict)
                and isinstance(item.get("task"), str)
                and bool(item["task"].strip())
                and isinstance(item.get("labels"), list)
                and bool(item["labels"])
                and isinstance(item.get("true_label"), list)
                and any(has_value(label) for label in item["true_label"])
                for item in classifications
            ):
                tasks.add("classifications")
        rows = output.get("json_structures")
        if rows is not None:
            if not isinstance(rows, list):
                raise ValueError("output.json_structures must be an array")
            if any(isinstance(row, dict) and any(has_value(value) for value in row.values()) for row in rows):
                tasks.add("json_structures")
        rows = output.get("relations")
        if rows is not None:
            if not isinstance(rows, list):
                raise ValueError("output.relations must be an array")
            if any(
                isinstance(row, dict)
                and any(
                    isinstance(relation, dict)
                    and has_value(relation.get("head"))
                    and has_value(relation.get("tail"))
                    for relation in row.values()
                )
                for row in rows
            ):
                tasks.add("relations")
    return tasks


def load_dataset(path: Path, max_records: int | None = None) -> DatasetContent:
    if max_records is not None and max_records < 1:
        raise ValueError("max_records must be >= 1")
    records: set[bytes] = set()
    record_count = 0
    has_repeated_records = False
    texts: set[bytes] = set()
    tasks: set[str] = set()
    for source in jsonl_files(path):
        with source.open(encoding="utf-8") as lines:
            for line_no, raw in enumerate(lines, 1):
                if max_records is not None and record_count >= max_records:
                    break
                if not raw.strip():
                    continue
                try:
                    record = json.loads(raw)
                    if not isinstance(record, dict):
                        raise ValueError("record must be a JSON object")
                    record_hash = digest(record)
                    has_repeated_records |= record_hash in records
                    records.add(record_hash)
                    record_count += 1
                    texts.add(digest(record_text(record)))
                    tasks.update(record_tasks(record))
                except (json.JSONDecodeError, ValueError) as exc:
                    raise ValueError(f"{source}:{line_no}: {exc}") from exc
        if max_records is not None and record_count >= max_records:
            break
    if record_count == 0:
        raise ValueError(f"{path}: no JSONL records")
    return DatasetContent(record_count, has_repeated_records, frozenset(texts), frozenset(tasks))


def validate_release_data(
    train: DatasetContent,
    eval_: DatasetContent | None,
    smoke_texts: frozenset[bytes],
    require_full_task: bool,
    *,
    min_train_records: int = 1,
    min_eval_records: int = 0,
    comparison_train: DatasetContent | None = None,
) -> None:
    if train.record_count < min_train_records:
        raise ValueError(f"training data has {train.record_count} records; require at least {min_train_records}")
    if min_eval_records and eval_ is None:
        raise ValueError("eval data is required")
    if eval_ is not None and eval_.record_count < min_eval_records:
        raise ValueError(f"eval data has {eval_.record_count} records; require at least {min_eval_records}")
    if train.has_repeated_records:
        raise ValueError("training data contains repeated/cycled records")
    if len(train.texts) != train.record_count:
        raise ValueError("training data contains repeated/cycled normalized input texts")
    if eval_ is not None and eval_.has_repeated_records:
        raise ValueError("eval data contains repeated/cycled records")
    if eval_ is not None and len(eval_.texts) != eval_.record_count:
        raise ValueError("eval data contains repeated/cycled normalized input texts")
    if eval_ is not None and train.texts & eval_.texts:
        raise ValueError("training and eval data overlap by input text")
    if train.texts & smoke_texts or (eval_ is not None and eval_.texts & smoke_texts):
        raise ValueError("training or eval data contains checked-in smoke fixture content")
    if require_full_task:
        compared = comparison_train or train
        missing = TASK_KINDS - compared.tasks
        if missing:
            raise ValueError(f"compared training slice is missing non-empty full-task coverage: {','.join(sorted(missing))}")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--train", type=Path, required=True)
    parser.add_argument("--eval", type=Path)
    parser.add_argument("--require-full-task", action="store_true")
    parser.add_argument("--min-train-records", type=int, default=1)
    parser.add_argument("--min-eval-records", type=int, default=0)
    parser.add_argument("--comparison-records", type=int)
    parser.add_argument("--model-dir", type=Path)
    parser.add_argument("--release-adapter-dir", type=Path)
    parser.add_argument("--backend", choices=("metal", "cuda"), default="metal")
    parser.add_argument(
        "--smoke-dir",
        type=Path,
        default=Path(__file__).resolve().parents[1] / "testdata",
    )
    args = parser.parse_args()
    try:
        train = load_dataset(args.train)
        eval_ = load_dataset(args.eval) if args.eval else None
        comparison_train = load_dataset(args.train, args.comparison_records) if args.comparison_records else None
        smoke_texts: set[bytes] = set()
        smoke_paths = sorted(args.smoke_dir.glob("gliner2*smoke*.jsonl"))
        if not smoke_paths:
            raise ValueError(f"{args.smoke_dir}: no GLiNER2 smoke fixtures found")
        for path in smoke_paths:
            smoke_texts.update(load_dataset(path).texts)
        validate_release_data(
            train,
            eval_,
            frozenset(smoke_texts),
            args.require_full_task,
            min_train_records=args.min_train_records,
            min_eval_records=args.min_eval_records,
            comparison_train=comparison_train,
        )
        if (args.model_dir is None) != (args.release_adapter_dir is None):
            raise ValueError("--model-dir and --release-adapter-dir must be supplied together")
        if args.model_dir is not None:
            if args.eval is None:
                raise ValueError("--eval is required for release adapter provenance")
            validate_release_artifact_provenance(
                args.model_dir,
                args.train,
                args.release_adapter_dir,
                train.record_count,
                args.eval,
                args.backend,
            )
    except ValueError as exc:
        parser.error(str(exc))
    print(
        f"release_data_ok train_records={train.record_count} eval_records={eval_.record_count if eval_ else 0} "
        f"tasks={','.join(sorted(train.tasks))} provenance={'ok' if args.model_dir else 'not_requested'}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
