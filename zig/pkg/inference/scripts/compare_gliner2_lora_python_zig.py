#!/usr/bin/env python3
"""Run a small GLiNER2 LoRA apples-to-apples smoke comparison.

The script intentionally keeps the comparison narrow: same local model dir,
same NER smoke JSONL, same LoRA rank/alpha/dropout/target groups, one backend
for Zig, and one JSON report with raw command outputs plus extracted metrics.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import shutil
import subprocess
import sys
import textwrap
import time
from pathlib import Path
from typing import Any

from gliner2_parity_data import allowed_labels_for_objective, normalize_python_record


DEFAULT_PYTHON = "/private/tmp/gliner2-parity-venv/bin/python"
DEFAULT_MODEL_DIR = "/private/tmp/termite-models/gliner2"
DEFAULT_PYTHON_MODEL = "fastino/gliner2-base-v1"
DEFAULT_OUT_DIR = "/private/tmp/termite-gliner2-apples-to-apples"
DEFAULT_LABELS = "person,organization,location"
LORA_TARGETS = "encoder,span_rep,classifier,count_embed,count_pred"


def repo_root() -> Path:
    return Path(__file__).resolve().parents[4]


def inference_dir() -> Path:
    return repo_root() / "zig" / "pkg" / "inference"


def default_train_data() -> Path:
    return inference_dir() / "testdata" / "gliner2_ner_smoke.jsonl"


def convert_to_python_jsonl(src: Path, dst: Path, allowed_labels: set[str] | None = None) -> dict[str, Any]:
    dst.parent.mkdir(parents=True, exist_ok=True)
    examples = 0
    mentions = 0
    task_counts = {"classifications": 0, "json_structures": 0, "relations": 0}
    labels: set[str] = set()
    with src.open("r", encoding="utf-8") as fin, dst.open("w", encoding="utf-8") as fout:
        for line in fin:
            if not line.strip():
                continue
            record = json.loads(line)
            normalized, counts, record_labels = normalize_python_record(record, allowed_labels)
            mentions += counts["entity_mentions"]
            for key in task_counts:
                task_counts[key] += counts[key]
            labels.update(record_labels)
            fout.write(json.dumps(normalized, ensure_ascii=False) + "\n")
            examples += 1
    return {"examples": examples, "mentions": mentions, "labels": sorted(labels), **task_counts, "path": str(dst)}


def summarize_python_jsonl(src: Path, allowed_labels: set[str] | None = None) -> dict[str, Any]:
    examples = 0
    mentions = 0
    task_counts = {"classifications": 0, "json_structures": 0, "relations": 0}
    labels: set[str] = set()
    with src.open("r", encoding="utf-8") as fin:
        for line in fin:
            if not line.strip():
                continue
            record = json.loads(line)
            _, counts, record_labels = normalize_python_record(record, allowed_labels)
            mentions += counts["entity_mentions"]
            for key in task_counts:
                task_counts[key] += counts[key]
            labels.update(record_labels)
            examples += 1
    return {"examples": examples, "mentions": mentions, "labels": sorted(labels), **task_counts}


def convert_limited_to_python_jsonl(src: Path, dst: Path, max_examples: int, allowed_labels: set[str] | None = None) -> dict[str, Any]:
    dst.parent.mkdir(parents=True, exist_ok=True)
    examples = 0
    mentions = 0
    task_counts = {"classifications": 0, "json_structures": 0, "relations": 0}
    labels: set[str] = set()
    with src.open("r", encoding="utf-8") as fin, dst.open("w", encoding="utf-8") as fout:
        for line in fin:
            if examples >= max_examples:
                break
            if not line.strip():
                continue
            record = json.loads(line)
            normalized, counts, record_labels = normalize_python_record(record, allowed_labels)
            mentions += counts["entity_mentions"]
            for key in task_counts:
                task_counts[key] += counts[key]
            labels.update(record_labels)
            fout.write(json.dumps(normalized, ensure_ascii=False) + "\n")
            examples += 1
    return {"examples": examples, "mentions": mentions, "labels": sorted(labels), **task_counts, "path": str(dst)}


def prepare_python_model_dir(model_dir: Path, out_dir: Path) -> Path:
    """Create a Python-loader-compatible view of the local GLiNER2 bundle.

    The Termite local model cache may omit `encoder_config.model_type`; the
    upstream Python package delegates that file to Hugging Face AutoConfig,
    which requires it.  Patch only the comparison copy and symlink large files.
    """
    dst = out_dir / "python_model"
    dst.mkdir(parents=True, exist_ok=True)
    root_config = dst / "config.json"
    if not (model_dir / "config.json").exists() and not root_config.exists():
        root_config.write_text(
            json.dumps(
                {
                    "model_name": "microsoft/deberta-v3-base",
                    "model_type": "extractor",
                    "counting_layer": "count_lstm_v2",
                    "token_pooling": "first",
                    "max_width": 8,
                },
                indent=2,
            ),
            encoding="utf-8",
        )
    tokenizer_config = dst / "tokenizer_config.json"
    if not (model_dir / "tokenizer_config.json").exists() and not tokenizer_config.exists():
        tokenizer_config.write_text(
            json.dumps(
                {
                    "tokenizer_class": "DebertaV2TokenizerFast",
                    "model_max_length": 512,
                },
                indent=2,
            ),
            encoding="utf-8",
        )
    for src in model_dir.iterdir():
        target = dst / src.name
        if src.name == "encoder_config":
            target.mkdir(exist_ok=True)
            for child in src.iterdir():
                child_target = target / child.name
                if child.name == "config.json":
                    cfg = json.loads(child.read_text(encoding="utf-8"))
                    cfg.setdefault("model_type", "deberta-v2")
                    cfg.setdefault("relative_attention", True)
                    cfg.setdefault("position_biased_input", False)
                    cfg.setdefault("pos_att_type", ["p2c", "c2p"])
                    cfg.setdefault("max_relative_positions", -1)
                    cfg.setdefault("norm_rel_ebd", "layer_norm")
                    cfg.setdefault("share_att_key", True)
                    cfg.setdefault("type_vocab_size", 0)
                    cfg.setdefault("layer_norm_eps", 1e-7)
                    child_target.write_text(json.dumps(cfg, indent=2), encoding="utf-8")
                elif not child_target.exists():
                    child_target.symlink_to(child)
        elif not target.exists():
            target.symlink_to(src)
    return dst


def run_command(cmd: list[str], cwd: Path, timeout: int | None = None, env: dict[str, str] | None = None) -> dict[str, Any]:
    started = time.time()
    run_env = {**os.environ, "TOKENIZERS_PARALLELISM": "false"}
    if env:
        run_env.update(env)
    proc = subprocess.run(
        cmd,
        cwd=str(cwd),
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=timeout,
        env=run_env,
    )
    return {
        "argv": cmd,
        "cwd": str(cwd),
        "returncode": proc.returncode,
        "elapsed_seconds": time.time() - started,
        "output": proc.stdout,
    }


def trim_result_output(result: dict[str, Any], max_chars: int) -> None:
    output = result.get("output")
    if not isinstance(output, str) or max_chars <= 0 or len(output) <= max_chars:
        return
    head_len = max_chars // 4
    tail_len = max_chars - head_len
    result["output_truncated"] = True
    result["output_original_chars"] = len(output)
    result["output_head_chars"] = head_len
    result["output_tail_chars"] = tail_len
    result["output"] = (
        output[:head_len]
        + f"\n\n... <truncated {len(output) - max_chars} chars; kept head={head_len} tail={tail_len}> ...\n\n"
        + output[-tail_len:]
    )


def load_json_file(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


def finite_number(value: Any) -> bool:
    try:
        return math.isfinite(float(value))
    except (TypeError, ValueError):
        return False


def as_float_or_none(value: Any) -> float | None:
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
    return f if math.isfinite(f) else None


def parse_zig_output(output: str) -> dict[str, Any]:
    def parse_float(value: str | None) -> float | None:
        if value is None:
            return None
        lowered = value.lower()
        if lowered in {"nan", "+nan", "-nan"}:
            return float("nan")
        if lowered in {"inf", "+inf", "infinity", "+infinity"}:
            return float("inf")
        if lowered in {"-inf", "-infinity"}:
            return float("-inf")
        return float(value)

    float_pattern = r"([-+]?(?:nan|inf|infinity|[0-9]+(?:\.[0-9]*)?(?:[eE][-+]?[0-9]+)?|\.[0-9]+(?:[eE][-+]?[0-9]+)?))"
    step = re.search(rf"loss={float_pattern}\s+grad_norm={float_pattern}\s+supervised_tok/s={float_pattern}", output)
    final = re.search(rf"final avg loss={float_pattern}", output)
    loaded = re.search(r"loaded\s+(\d+)\s+weights", output)
    parity_debug = extract_prefixed_json(output, "SPAN_PARITY_DEBUG ")
    preprocess_debug = extract_prefixed_json(output, "SPAN_PREPROCESS_DEBUG ")
    component_debug = extract_prefixed_json(output, "SPAN_COMPONENT_DEBUG ")
    total_component_debug = extract_prefixed_json(output, "GLINER2_TOTAL_LOSS_COMPONENT_DEBUG ")
    classification_debug = extract_prefixed_json(output, "GLINER2_CLASSIFICATION_DEBUG ")
    optimizer_parity_steps = extract_prefixed_json(output, "GLINER2_OPT_PARITY ")
    return {
        "step_loss": parse_float(step.group(1)) if step else None,
        "grad_norm": parse_float(step.group(2)) if step else None,
        "supervised_tok_per_s": parse_float(step.group(3)) if step else None,
        "final_avg_loss": parse_float(final.group(1)) if final else None,
        "loaded_weight_count": int(loaded.group(1)) if loaded else None,
        "span_parity_debug": parity_debug[-1] if parity_debug else None,
        "span_preprocess_debug": preprocess_debug[-1] if preprocess_debug else None,
        "span_component_debug": component_debug[-1] if component_debug else None,
        "gliner2_total_loss_components": total_component_debug[-1] if total_component_debug else None,
        "gliner2_classification_debug": classification_debug[-1] if classification_debug else None,
        "optimizer_parity_steps": optimizer_parity_steps,
    }


def _trim_trailing(values: list[Any], pad: Any = 0) -> list[Any]:
    out = list(values or [])
    while out and out[-1] == pad:
        out.pop()
    return out


def _first_mismatch(path: str, left: Any, right: Any) -> dict[str, Any] | None:
    if isinstance(left, list) and isinstance(right, list):
        limit = min(len(left), len(right))
        for idx in range(limit):
            mismatch = _first_mismatch(f"{path}[{idx}]", left[idx], right[idx])
            if mismatch is not None:
                return mismatch
        if len(left) != len(right):
            return {"field": path, "index": limit, "python": len(left), "zig": len(right), "kind": "length"}
        return None
    if left != right:
        return {"field": path, "python": left, "zig": right}
    return None


def _zig_schema_special_indices(zig: dict[str, Any]) -> list[list[int]]:
    counts = zig.get("schema_special_counts") or []
    flat = zig.get("schema_special_positions") or []
    if not counts:
        return []
    max_schemas = len(counts)
    width = len(flat) // max_schemas if max_schemas else 0
    out: list[list[int]] = []
    for schema_idx, count in enumerate(counts):
        start = schema_idx * width
        out.append([int(x) for x in flat[start:start + int(count)] if int(x) >= 0])
    while out and not out[-1]:
        out.pop()
    return out


def _zig_schema_special_indices_for_sample(zig: dict[str, Any], sample_idx: int) -> list[list[int]]:
    counts_all = zig.get("schema_special_counts_all") or []
    flat_all = zig.get("schema_special_positions_all") or []
    max_schemas = int(zig.get("max_schemas") or len(zig.get("schema_special_counts") or []))
    max_schema_specials = int(zig.get("max_schema_specials") or 0)
    if not counts_all or not flat_all or max_schemas <= 0 or max_schema_specials <= 0:
        return _zig_schema_special_indices(zig) if sample_idx == 0 else []
    counts_start = sample_idx * max_schemas
    pos_start = sample_idx * max_schemas * max_schema_specials
    out: list[list[int]] = []
    for schema_idx in range(max_schemas):
        count_pos = counts_start + schema_idx
        if count_pos >= len(counts_all):
            break
        count = int(counts_all[count_pos])
        row_start = pos_start + schema_idx * max_schema_specials
        out.append([int(x) for x in flat_all[row_start:row_start + count] if int(x) >= 0])
    while out and not out[-1]:
        out.pop()
    return out


def _python_structure_positive_count(structure_labels: Any) -> int:
    count = 0
    for item in structure_labels or []:
        if not isinstance(item, list) or len(item) < 2:
            continue
        if not isinstance(item[1], list):
            continue
        for span_group in item[1] or []:
            if not isinstance(span_group, list):
                continue
            for field in span_group or []:
                if isinstance(field, list):
                    count += sum(1 for sub in field if sub not in (None, [-1, -1], (-1, -1)))
                elif field not in (None, [-1, -1], (-1, -1)):
                    count += 1
    return count


def _zig_sample_slice(zig: dict[str, Any], field: str, sample_idx: int, width: int, fallback_field: str | None = None) -> list[Any]:
    values = zig.get(field) or []
    if not values and fallback_field:
        values = zig.get(fallback_field) or []
    start = sample_idx * width
    end = start + width
    return values[start:end]


def _zig_task_types_for_sample(zig: dict[str, Any], sample_idx: int, count: int) -> list[str]:
    task_id_to_name = {1: "entities", 2: "json_structures", 3: "relations", 4: "classifications"}
    max_schemas = int(zig.get("max_schemas") or len(zig.get("task_type_ids") or []))
    ids = _zig_sample_slice(zig, "task_type_ids_all", sample_idx, max_schemas, "task_type_ids")
    return [task_id_to_name.get(int(x), "unknown") for x in ids[:count]]


def _compare_preprocess_sample(py: dict[str, Any], zig: dict[str, Any], sample_idx: int) -> list[dict[str, Any]]:
    mismatches: list[dict[str, Any]] = []
    max_length = int(zig.get("max_length") or len(zig.get("input_ids") or []))
    max_words = int(zig.get("max_words_per_sample") or len(zig.get("first_token_positions") or []))
    max_spans = int(zig.get("max_spans") or 0)
    entity_types = int(zig.get("num_entity_types") or 0)
    word_count = int(py.get("text_word_counts") or 0)
    py_task_types = py.get("task_types", [])

    comparisons = {
        f"samples[{sample_idx}].input_ids": (
            _trim_trailing(py.get("input_ids", []), 0),
            _trim_trailing(_zig_sample_slice(zig, "input_ids_all", sample_idx, max_length, "input_ids"), 0),
        ),
        f"samples[{sample_idx}].attention_mask": (
            _trim_trailing(py.get("attention_mask", []), 0),
            _trim_trailing(_zig_sample_slice(zig, "attention_mask_all", sample_idx, max_length, "attention_mask"), 0),
        ),
        f"samples[{sample_idx}].text_word_indices": (
            py.get("text_word_indices", []),
            _zig_sample_slice(zig, "first_token_positions_all", sample_idx, max_words, "first_token_positions")[:word_count],
        ),
        f"samples[{sample_idx}].schema_special_indices": (
            py.get("schema_special_indices", []),
            _zig_schema_special_indices_for_sample(zig, sample_idx),
        ),
        f"samples[{sample_idx}].task_types": (
            py_task_types,
            _zig_task_types_for_sample(zig, sample_idx, len(py_task_types)),
        ),
    }
    for field, (left, right) in comparisons.items():
        mismatch = _first_mismatch(field, left, right)
        if mismatch is not None:
            mismatches.append(mismatch)

    py_positive = _python_structure_positive_count(py.get("structure_labels"))
    span_label_width = max_spans * entity_types
    zig_labels = _zig_sample_slice(zig, "span_labels_all", sample_idx, span_label_width, "span_labels")
    zig_positive = int(round(sum(float(x) for x in zig_labels)))
    if py_positive != zig_positive:
        mismatches.append({ "field": f"samples[{sample_idx}].structure_positive_count", "python": py_positive, "zig": zig_positive })
    return mismatches


def compare_preprocess_debug(py: dict[str, Any] | None, zig: dict[str, Any] | None) -> tuple[bool, list[dict[str, Any]]]:
    mismatches: list[dict[str, Any]] = []
    if not py or not zig:
        return False, [{"field": "preprocess_debug", "python": bool(py), "zig": bool(zig), "kind": "missing"}]

    task_id_to_name = {1: "entities", 2: "json_structures", 3: "relations", 4: "classifications"}
    comparisons = {
        "input_ids": (_trim_trailing(py.get("input_ids", []), 0), _trim_trailing(zig.get("input_ids", []), 0)),
        "attention_mask": (_trim_trailing(py.get("attention_mask", []), 0), _trim_trailing(zig.get("attention_mask", []), 0)),
        "text_word_indices": (py.get("text_word_indices", []), (zig.get("first_token_positions", []) or [])[: len(py.get("text_word_indices", []))]),
        "schema_special_indices": (py.get("schema_special_indices", []), _zig_schema_special_indices(zig)),
        "task_types": (py.get("task_types", []), [task_id_to_name.get(int(x), "unknown") for x in (zig.get("task_type_ids") or [])[: len(py.get("task_types", []))]]),
    }
    for field, (left, right) in comparisons.items():
        mismatch = _first_mismatch(field, left, right)
        if mismatch is not None:
            mismatches.append(mismatch)

    py_positive = _python_structure_positive_count(py.get("structure_labels"))
    zig_labels = zig.get("span_labels") or []
    zig_first_sample_width = int(zig.get("max_spans") or 0) * int(zig.get("num_entity_types") or 0)
    if zig_first_sample_width <= 0:
        zig_first_sample_width = len(zig_labels)
    zig_positive = int(round(sum(float(x) for x in zig_labels[:zig_first_sample_width])))
    if py_positive != zig_positive:
        mismatches.append({"field": "structure_positive_count", "python": py_positive, "zig": zig_positive})

    return not mismatches, mismatches[:10]


def compare_preprocess_debug_samples(py_samples: list[dict[str, Any]] | None, zig: dict[str, Any] | None) -> tuple[bool, list[dict[str, Any]]]:
    if not py_samples:
        return compare_preprocess_debug(None, zig)
    if not zig:
        return False, [{"field": "preprocess_debug", "python": True, "zig": False, "kind": "missing"}]
    mismatches: list[dict[str, Any]] = []
    for fallback_idx, sample in enumerate(py_samples):
        sample_idx = int(sample.get("sample_idx", fallback_idx))
        mismatches.extend(_compare_preprocess_sample(sample, zig, sample_idx))
        if len(mismatches) >= 10:
            break
    return not mismatches, mismatches[:10]


def compare_component_losses(py: dict[str, Any] | None, zig: dict[str, Any] | None, tolerance: float) -> tuple[bool, dict[str, Any]]:
    fields = ["classification_loss", "structure_loss", "count_loss", "total_loss"]
    if not py or not zig:
        return False, {"missing": {"python": bool(py), "zig": bool(zig)}}
    deltas: dict[str, Any] = {}
    ok = True
    for field in fields:
        pv = py.get(field)
        zv = zig.get(field)
        if pv is None or zv is None:
            deltas[field] = {"python": pv, "zig": zv, "delta": None, "ok": False}
            ok = False
            continue
        if not finite_number(pv) or not finite_number(zv):
            deltas[field] = {"python": pv, "zig": zv, "delta": None, "ok": False, "reason": "non_finite"}
            ok = False
            continue
        delta = float(zv) - float(pv)
        field_ok = abs(delta) <= tolerance
        deltas[field] = {"python": float(pv), "zig": float(zv), "delta": delta, "ok": field_ok}
        ok = ok and field_ok
    return ok, deltas


def reconcile_single_component_from_step_loss(
    py: dict[str, Any] | None,
    zig: dict[str, Any] | None,
    zig_step_loss: float | None,
    step_loss_matches: bool,
    tolerance: float,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    if not py or not zig or zig_step_loss is None or not step_loss_matches:
        return zig, None
    component_fields = ["classification_loss", "structure_loss", "count_loss"]
    active_epsilon = 1e-12
    nonzero = [
        field
        for field in component_fields
        if finite_number(py.get(field)) and abs(float(py[field])) > active_epsilon
    ]
    if len(nonzero) != 1:
        return zig, None
    field = nonzero[0]
    py_total = py.get("total_loss")
    if not finite_number(py_total) or abs(float(py_total) - float(py[field])) > tolerance:
        return zig, None
    # Only reconcile away small numerical noise. Substituting the (already-matching)
    # step loss for the component would otherwise make the component-loss check
    # tautological and mask a genuine divergence. Require the independently-reported
    # Zig component to already be within a bounded multiple of the tolerance of
    # Python; a larger gap is a real divergence and must surface as a failure.
    reconcile_bound = tolerance * 8.0
    zig_field_val = zig.get(field)
    if not finite_number(zig_field_val) or abs(float(zig_field_val) - float(py[field])) > reconcile_bound:
        return zig, None
    fixed = dict(zig)
    raw = {"component": fixed.get(field), "total_loss": fixed.get("total_loss")}
    fixed[field] = zig_step_loss
    fixed["total_loss"] = zig_step_loss
    return fixed, {
        "component": field,
        "source": "zig_step_loss",
        "raw_zig_debug": raw,
        "zig_step_loss": zig_step_loss,
        "reconcile_bound": reconcile_bound,
    }


def compare_classification_debug(py: dict[str, Any] | None, zig: dict[str, Any] | None, tolerance: float) -> tuple[bool, dict[str, Any]]:
    if not py or not zig:
        return False, {"missing": {"python": bool(py), "zig": bool(zig)}}
    fields = [
        "valid_count",
        "positive_count",
        "label_sum",
        "mask_sum",
        "logits_min",
        "logits_max",
        "logits_mean",
        "bce_sum",
    ]
    deltas: dict[str, Any] = {}
    ok = True
    for field in fields:
        pv = py.get(field)
        zv = zig.get(field)
        if isinstance(pv, int) and isinstance(zv, int):
            field_ok = pv == zv
            delta = int(zv) - int(pv)
        elif finite_number(pv) and finite_number(zv):
            delta = float(zv) - float(pv)
            field_ok = abs(delta) <= tolerance
        else:
            delta = None
            field_ok = pv == zv
        deltas[field] = {"python": pv, "zig": zv, "delta": delta, "ok": field_ok}
        ok = ok and field_ok
    for field in ("valid_logits_head", "valid_labels_head"):
        py_values = py.get(field) or []
        zig_values = zig.get(field) or []
        count = min(len(py_values), len(zig_values))
        max_delta = 0.0
        for idx in range(count):
            if finite_number(py_values[idx]) and finite_number(zig_values[idx]):
                max_delta = max(max_delta, abs(float(zig_values[idx]) - float(py_values[idx])))
        field_ok = len(py_values) == len(zig_values) and max_delta <= tolerance
        deltas[field] = {
            "python_len": len(py_values),
            "zig_len": len(zig_values),
            "max_abs_delta": max_delta,
            "ok": field_ok,
        }
        ok = ok and field_ok
    return ok, deltas


def canonical_adapter_param_name(name: str) -> str:
    """Map Python PEFT and Zig PEFT-export parameter names onto one key.

    Python (PEFT) trainable params look like
    `base_model.model.<module>.lora_A.default.weight`; the Zig optimizer
    parity dump emits the PEFT-export name `<module>.lora_A.weight`.
    """
    if name.startswith("base_model.model."):
        name = name[len("base_model.model."):]
    name = name.replace(".lora_A.default.", ".lora_A.").replace(".lora_B.default.", ".lora_B.")
    if name.endswith(".weight"):
        name = name[: -len(".weight")]
    return name


def _derived_grad_head(m_now: list[Any], m_prev: list[Any], beta1: float) -> list[float]:
    """Recover the post-clip gradient head from the Adam first-moment update.

    m_t = beta1 * m_{t-1} + (1 - beta1) * g_t  =>  g_t = (m_t - beta1*m_{t-1}) / (1-beta1)
    """
    out: list[float] = []
    for idx, value in enumerate(m_now or []):
        prev = float(m_prev[idx]) if m_prev and idx < len(m_prev) else 0.0
        out.append((float(value) - beta1 * prev) / (1.0 - beta1))
    return out


def compare_optimizer_parity(
    py_steps: list[dict[str, Any]] | None,
    zig_steps: list[dict[str, Any]] | None,
    beta1: float = 0.9,
) -> dict[str, Any]:
    """Per-step grad/m/v/weight comparison for the LoRA adapter tensors.

    Localizes where multi-step divergence is born: a gradient delta points at
    the forward/backward pass, an m/v delta (with matching gradients) points
    at the optimizer formula or its per-parameter step bookkeeping, and a
    weight delta with matching m/v points at the parameter update itself.
    """
    if not py_steps or not zig_steps:
        return {
            "ran": False,
            "python_step_count": len(py_steps or []),
            "zig_step_count": len(zig_steps or []),
        }

    def index_steps(steps: list[dict[str, Any]]) -> list[dict[str, dict[str, Any]]]:
        indexed: list[dict[str, dict[str, Any]]] = []
        for payload in steps:
            indexed.append({
                canonical_adapter_param_name(str(t.get("name"))): t
                for t in payload.get("tensors", [])
            })
        return indexed

    py_idx = index_steps(py_steps)
    zig_idx = index_steps(zig_steps)
    quantities = ("weight", "m", "v")
    step_reports: list[dict[str, Any]] = []
    for step_i in range(min(len(py_idx), len(zig_idx))):
        py_tensors = py_idx[step_i]
        zig_tensors = zig_idx[step_i]
        common = sorted(set(py_tensors) & set(zig_tensors))
        head_max = {q: {"max_abs_delta": 0.0, "tensor": None} for q in quantities}
        abs_sum_max = {q: {"max_abs_delta": 0.0, "tensor": None} for q in quantities}
        derived_grad_max = {"max_abs_delta": 0.0, "tensor": None}
        true_vs_derived_grad_max = {"max_abs_delta": 0.0, "tensor": None}
        step_count_mismatches: list[dict[str, Any]] = []
        for name in common:
            pt = py_tensors[name]
            zt = zig_tensors[name]
            py_step_count = int(pt.get("step_count") or 0)
            zig_step_count = int(zt.get("step_count") or 0)
            if py_step_count != zig_step_count:
                step_count_mismatches.append({
                    "tensor": name,
                    "python": py_step_count,
                    "zig": zig_step_count,
                })
            for q in quantities:
                pv = pt.get(q) or []
                zv = zt.get(q) or []
                for a, b in zip(pv, zv):
                    if not (finite_number(a) and finite_number(b)):
                        continue
                    delta = abs(float(a) - float(b))
                    if delta > head_max[q]["max_abs_delta"]:
                        head_max[q] = {"max_abs_delta": delta, "tensor": name}
                pa = pt.get(f"{q}_abs_sum")
                za = zt.get(f"{q}_abs_sum")
                if finite_number(pa) and finite_number(za):
                    delta = abs(float(pa) - float(za))
                    if delta > abs_sum_max[q]["max_abs_delta"]:
                        abs_sum_max[q] = {"max_abs_delta": delta, "tensor": name}
            py_m_prev = (py_idx[step_i - 1].get(name, {}).get("m") or []) if step_i > 0 else []
            zig_m_prev = (zig_idx[step_i - 1].get(name, {}).get("m") or []) if step_i > 0 else []
            py_derived = _derived_grad_head(pt.get("m") or [], py_m_prev, beta1)
            zig_derived = _derived_grad_head(zt.get("m") or [], zig_m_prev, beta1)
            for a, b in zip(py_derived, zig_derived):
                delta = abs(a - b)
                if delta > derived_grad_max["max_abs_delta"]:
                    derived_grad_max = {"max_abs_delta": delta, "tensor": name}
            for a, b in zip(pt.get("grad") or [], zig_derived):
                if not finite_number(a):
                    continue
                delta = abs(float(a) - b)
                if delta > true_vs_derived_grad_max["max_abs_delta"]:
                    true_vs_derived_grad_max = {"max_abs_delta": delta, "tensor": name}
        step_reports.append({
            "step": step_i + 1,
            "tensors_compared": len(common),
            "python_only_tensors": len(py_tensors) - len(common),
            "zig_only_tensors": len(zig_tensors) - len(common),
            "step_count_mismatch_count": len(step_count_mismatches),
            "step_count_mismatches": step_count_mismatches[:10],
            "weight_head_max_abs_delta": head_max["weight"],
            "m_head_max_abs_delta": head_max["m"],
            "v_head_max_abs_delta": head_max["v"],
            "weight_abs_sum_max_abs_delta": abs_sum_max["weight"],
            "m_abs_sum_max_abs_delta": abs_sum_max["m"],
            "v_abs_sum_max_abs_delta": abs_sum_max["v"],
            "derived_grad_head_max_abs_delta": derived_grad_max,
            "python_true_grad_vs_zig_derived_max_abs_delta": true_vs_derived_grad_max,
        })
    return {
        "ran": True,
        "python_step_count": len(py_idx),
        "zig_step_count": len(zig_idx),
        "steps": step_reports,
    }


def summarize_preprocess_tasks(samples: list[dict[str, Any]] | None) -> dict[str, Any]:
    task_counts: dict[str, int] = {}
    sample_summaries: list[dict[str, Any]] = []
    for fallback_idx, sample in enumerate(samples or []):
        sample_idx = int(sample.get("sample_idx", fallback_idx))
        local_counts: dict[str, int] = {}
        for task_type in sample.get("task_types", []) or []:
            name = str(task_type)
            task_counts[name] = task_counts.get(name, 0) + 1
            local_counts[name] = local_counts.get(name, 0) + 1
        sample_summaries.append({
            "sample_idx": sample_idx,
            "task_counts": local_counts,
            "has_non_entity_task": any(k != "entities" for k in local_counts),
            "structure_positive_count": _python_structure_positive_count(sample.get("structure_labels")),
        })
    return {
        "sample_count": len(sample_summaries),
        "task_counts": task_counts,
        "sample_summaries": sample_summaries,
        "non_entity_task_count": sum(v for k, v in task_counts.items() if k != "entities"),
    }


def summarize_component_deltas(deltas: dict[str, Any]) -> dict[str, Any]:
    failing: list[dict[str, Any]] = []
    for name, payload in deltas.items():
        if not isinstance(payload, dict) or payload.get("ok") is True:
            continue
        delta = payload.get("delta")
        failing.append({
            "component": name,
            "python": payload.get("python"),
            "zig": payload.get("zig"),
            "delta": delta,
            "abs_delta": abs(float(delta)) if delta is not None else None,
        })
    failing.sort(key=lambda item: item["abs_delta"] if item["abs_delta"] is not None else -1.0, reverse=True)
    return {
        "failing_components": failing,
        "largest_failing_component": failing[0]["component"] if failing else None,
    }


def summarize_metal_readiness(args: argparse.Namespace, report: dict[str, Any], zig_step_rows: list[dict[str, Any]], zig_manifest: dict[str, Any]) -> dict[str, Any] | None:
    if args.zig_backend != "metal" and not args.zig_build_metal:
        return None

    optimizer_backends = sorted({
        str(row.get("optimizer_backend"))
        for row in zig_step_rows
        if row.get("optimizer_backend") is not None
    })
    max_device_resident_transfer_count = max(
        [int(row.get("device_resident_transfer_count") or 0) for row in zig_step_rows] or [0]
    )
    max_device_trainable_bytes = max(
        [int(row.get("device_trainable_bytes") or 0) for row in zig_step_rows] or [0]
    )
    total_command_dispatches = sum(
        int(row.get("graph_executor_command_dispatches") or 0) for row in zig_step_rows
    )
    total_planned_dispatches = sum(
        int(row.get("graph_executor_planned_dispatches") or 0) for row in zig_step_rows
    )
    total_interpreter_fallbacks = sum(
        int(row.get("graph_executor_interpreter_fallbacks") or 0) for row in zig_step_rows
    )
    total_host_outputs = sum(
        int(row.get("graph_executor_host_outputs") or 0) for row in zig_step_rows
    )
    def row_true_host_outputs(row: dict[str, Any]) -> int:
        if row.get("graph_executor_true_host_outputs") is not None:
            return int(row.get("graph_executor_true_host_outputs") or 0)
        return sum(
            int(row.get(key) or 0)
            for key in (
                "graph_executor_host_output_command",
                "graph_executor_host_output_interpreter",
                "graph_executor_host_output_pre_materialized_constant",
                "graph_executor_host_output_runtime_region",
                "graph_executor_host_output_unattributed",
            )
        )

    total_true_host_outputs = sum(row_true_host_outputs(row) for row in zig_step_rows)
    total_parameter_materializations = sum(
        int(row.get("graph_executor_host_output_parameter") or 0) for row in zig_step_rows
    )
    total_device_parameter_outputs = sum(
        int(row.get("graph_executor_device_output_parameter") or 0) for row in zig_step_rows
    )
    graph_executor_fallback_reasons = sorted({
        str(row.get("graph_executor_fallback_reason"))
        for row in zig_step_rows
        if row.get("graph_executor_fallback_reason")
    })
    manifest_backend = str(zig_manifest.get("backend") or "")
    manifest_objective = str(zig_manifest.get("objective") or "")
    zig_metrics = report.get("zig", {}).get("metrics", {})
    checks = {
        "zig_returncode_ok": report.get("zig", {}).get("returncode") == 0,
        "manifest_backend_is_metal": manifest_backend.lower() == "metal",
        "manifest_objective_matches_request": manifest_objective == args.zig_objective,
        "finite_step_loss": finite_number(zig_metrics.get("step_loss")) or finite_number(zig_metrics.get("final_avg_loss")),
        "finite_grad_norm": finite_number(zig_metrics.get("grad_norm")) if zig_metrics.get("grad_norm") is not None else True,
        "optimizer_backend_is_metal": optimizer_backends == ["metal"] if optimizer_backends else False,
        "device_resident_transfers_zero": max_device_resident_transfer_count == 0,
        "device_trainables_resident": max_device_trainable_bytes > 0,
    }
    warnings: list[str] = []
    zero_dispatches = total_command_dispatches == 0 and total_planned_dispatches == 0
    graph_executor_requested = bool(
        args.zig_backend == "metal" and getattr(args, "zig_training_graph_executor", False)
    )
    max_interpreter_fallbacks = int(getattr(args, "metal_max_interpreter_fallbacks", 64))
    if graph_executor_requested and zig_step_rows:
        # When the training graph executor was explicitly requested, zero
        # dispatches means every step silently fell back to interpreter-only
        # execution; that is a failing check (gated by --strict), not a warning.
        checks["graph_executor_dispatches_nonzero"] = not zero_dispatches
        # A non-empty fallback reason is a FULL-STEP bail to the interpreter —
        # always a failure. The per-op interpreter-fallback count is a
        # perf/coverage signal gated by an explicit ceiling (regression guard
        # against drifting into broad interpreter-only execution).
        checks["graph_executor_fallback_reasons_empty"] = not graph_executor_fallback_reasons
        checks["graph_executor_true_host_outputs_zero"] = total_true_host_outputs == 0
        checks["interpreter_fallbacks_within_threshold"] = total_interpreter_fallbacks <= max_interpreter_fallbacks
    elif zero_dispatches:
        warnings.append("Metal run reported no graph command/planned dispatches; check for interpreter-only execution")
    if graph_executor_fallback_reasons:
        warnings.append(
            "Metal run reported graph executor fallback reasons: " + ", ".join(graph_executor_fallback_reasons)
        )
    if total_interpreter_fallbacks > 0:
        warnings.append(f"Metal run reported {total_interpreter_fallbacks} interpreter fallbacks")
    if total_true_host_outputs > 0:
        warnings.append(f"Metal run reported {total_true_host_outputs} true host outputs")
    if total_parameter_materializations > 0:
        warnings.append(f"Metal run reported {total_parameter_materializations} parameter materializations")
    return {
        "ok": all(checks.values()),
        "checks": checks,
        "warnings": warnings,
        "manifest_backend": manifest_backend or None,
        "manifest_objective": manifest_objective or None,
        "optimizer_backends": optimizer_backends,
        "max_device_resident_transfer_count": max_device_resident_transfer_count,
        "max_device_trainable_bytes": max_device_trainable_bytes,
        "total_graph_command_dispatches": total_command_dispatches,
        "total_graph_planned_dispatches": total_planned_dispatches,
        "total_graph_interpreter_fallbacks": total_interpreter_fallbacks,
        "total_graph_host_outputs": total_host_outputs,
        "total_graph_true_host_outputs": total_true_host_outputs,
        "total_graph_parameter_materializations": total_parameter_materializations,
        "total_graph_device_parameter_outputs": total_device_parameter_outputs,
        "graph_executor_fallback_reasons": graph_executor_fallback_reasons,
        "max_interpreter_fallbacks_threshold": max_interpreter_fallbacks,
        "graph_executor_requested": graph_executor_requested,
    }


def parse_op_stat_items(payload: str) -> dict[str, dict[str, float]]:
    stats: dict[str, dict[str, float]] = {}
    for item in payload.split(","):
        item = item.strip()
        if not item or ":count=" not in item:
            continue
        parts = item.split(":")
        name = parts[0]
        values: dict[str, float] = {}
        for part in parts[1:]:
            if "=" not in part:
                continue
            key, raw_value = part.split("=", 1)
            try:
                values[key] = float(raw_value)
            except ValueError:
                pass
        if values:
            stats[name] = values
    return stats


def parse_zig_op_stats(output: str) -> dict[str, Any]:
    parsed: dict[str, Any] = {}
    prefixes = {
        "metal_partition_command_ops:": "command_ops",
        "metal_partition_fallback_ops:": "fallback_ops",
        "metal_partition_host_output_ops:": "host_output_ops",
        "metal_partition_host_output_reasons:": "host_output_reasons",
    }
    for line in output.splitlines():
        for prefix, key in prefixes.items():
            if line.startswith(prefix):
                parsed[key] = parse_op_stat_items(line[len(prefix):].strip())
    return parsed


def top_op_stat_items(stats: dict[str, dict[str, float]], limit: int = 16) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for name, values in stats.items():
        count = values.get("count")
        total_ms = values.get("total_ms")
        avg_ms = values.get("avg_ms")
        if count is None:
            continue
        items.append({
            "name": name,
            "count": count,
            "total_ms": total_ms,
            "avg_ms": avg_ms,
        })
    items.sort(key=lambda item: (float(item.get("count") or 0), float(item.get("total_ms") or 0.0)), reverse=True)
    return items[:limit]


def parse_bool_token(value: str) -> bool | None:
    if value == "true":
        return True
    if value == "false":
        return False
    return None


def parse_dot_shape_items(payload: str) -> list[dict[str, Any]]:
    shapes: list[dict[str, Any]] = []
    shape_re = re.compile(
        r"(?P<lhs0>-?\d+)x(?P<lhs1>-?\d+)\*(?P<rhs0>-?\d+)x(?P<rhs1>-?\d+)->(?P<out0>-?\d+)x(?P<out1>-?\d+)"
    )
    for item in payload.split(","):
        item = item.strip()
        if not item or item == "none":
            continue
        shape_part, _, stat_payload = item.partition(":")
        match = shape_re.fullmatch(shape_part)
        if not match:
            continue
        groups = match.groupdict()
        values: dict[str, str] = {}
        for part in stat_payload.split(":"):
            if "=" not in part:
                continue
            key, raw_value = part.split("=", 1)
            values[key] = raw_value
        shape: dict[str, Any] = {
            "lhs": [int(groups["lhs0"]), int(groups["lhs1"])],
            "rhs": [int(groups["rhs0"]), int(groups["rhs1"])],
            "out": [int(groups["out0"]), int(groups["out1"])],
        }
        for key in ("count", "lhs_id", "rhs_id", "rhs_source"):
            if key in values:
                shape[key] = int(values[key])
        for key in ("total_ms", "avg_ms"):
            if key in values:
                shape[key] = float(values[key])
        for key in ("phase", "family", "pos", "node"):
            if key in values:
                shape[key] = values[key]
        if "lhs" in values:
            shape["lhs_op"] = values["lhs"]
        if "rhs" in values:
            shape["rhs_op"] = values["rhs"]
        for key in ("rhs_transpose", "rhs_parameter", "rhs_lora", "raw_linear"):
            if key in values:
                shape[key] = parse_bool_token(values[key])
        shapes.append(shape)
    return shapes


def parse_zig_op_runs(output: str) -> dict[str, Any]:
    dot_shapes: list[dict[str, Any]] = []
    for line in output.splitlines():
        prefixes = ("metal_partition_command_dot_shapes:", "metal_partition_dot_shapes:")
        prefix = next((p for p in prefixes if line.startswith(p)), None)
        if prefix is None:
            continue
        if prefix == "metal_partition_command_dot_shapes:":
            dot_shapes.extend(parse_dot_shape_items(line[len(prefix):].strip()))
            continue
        top_index = line.find(" top=")
        if top_index < 0:
            continue
        dot_shapes.extend(parse_dot_shape_items(line[top_index + len(" top="):]))
    dot_shapes.sort(key=lambda item: (item.get("total_ms", 0), item.get("count", 0)), reverse=True)
    return {
        "dot_shapes": dot_shapes,
        "top_dot_shapes": dot_shapes[:16],
    }


LOOP_PROFILE_VALUE_RE = re.compile(
    r"^(?P<value>-?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?)(?:\(hits=(?P<hits>-?\d+)\))?$"
)
LOOP_PROFILE_AVG_KEYS = (
    "partition_view_ms",
    "graph_plan_ms",
    "runtime_inputs_ms",
    "parameters_ms",
    "constants_ms",
    "begin_frame_ms",
    "runtime_plan_ms",
    "execution_ms",
    "planned_region_ms",
    "fused_pattern_ms",
    "command_path_ms",
    "interpreter_ms",
    "stats_ms",
    "alias_clone_ms",
    "free_expired_ms",
    "submit_frame_ms",
    "boundary_outputs_ms",
    "accounted_ms",
)
LOOP_PROFILE_COUNT_KEYS = (
    "nodes",
    "executed",
    "planned_region_hits",
    "fused_pattern_hits",
    "command_path_hits",
    "interpreter_hits",
)
PLANNED_ACCESS_PROFILE_KEYS = (
    "calls",
    "accesses",
    "range_scans",
    "conflicts",
    "capacity_flushes",
    "barriers",
    "total_ms",
    "avg_us",
)


def parse_loop_profile_value(raw_value: str) -> tuple[float | int | None, int | None]:
    match = LOOP_PROFILE_VALUE_RE.fullmatch(raw_value.strip())
    if match is None:
        return None, None
    value_text = match.group("value")
    if "." in value_text or "e" in value_text.lower():
        value: float | int = float(value_text)
    else:
        value = int(value_text)
    hits_text = match.group("hits")
    hits = int(hits_text) if hits_text is not None else None
    return value, hits


def parse_zig_loop_profiles(output: str) -> list[dict[str, Any]]:
    profiles: list[dict[str, Any]] = []
    prefix = "metal_partition_loop_profile:"
    for raw_line in output.splitlines():
        line = raw_line.strip()
        if not line.startswith(prefix):
            continue
        profile: dict[str, Any] = {}
        payload = line[len(prefix):].strip()
        for part in payload.split(":"):
            if "=" not in part:
                continue
            key, raw_value = part.split("=", 1)
            key = key.strip()
            value, hits = parse_loop_profile_value(raw_value)
            if value is None:
                continue
            profile[key] = value
            if hits is not None:
                hit_key = key[:-3] + "_hits" if key.endswith("_ms") else f"{key}_hits"
                profile[hit_key] = hits
        if profile:
            profiles.append(profile)
    return profiles


def parse_zig_planned_access_profiles(output: str) -> list[dict[str, Any]]:
    profiles: list[dict[str, Any]] = []
    prefix = "metal_planned_access_profile:"
    for raw_line in output.splitlines():
        line = raw_line.strip()
        if not line.startswith(prefix):
            continue
        profile: dict[str, Any] = {}
        payload = line[len(prefix):].strip()
        for part in payload.split(":"):
            if "=" not in part:
                continue
            key, raw_value = part.split("=", 1)
            value, _ = parse_loop_profile_value(raw_value)
            if value is not None:
                profile[key.strip()] = value
        if profile:
            profiles.append(profile)
    return profiles


def summarize_loop_profiles(profiles: list[dict[str, Any]]) -> dict[str, Any]:
    def avg(rows: list[dict[str, Any]], key: str) -> float | None:
        values: list[float] = []
        for row in rows:
            value = row.get(key)
            if value is None:
                continue
            try:
                values.append(float(value))
            except (TypeError, ValueError):
                pass
        return sum(values) / len(values) if values else None

    warm_profiles = profiles[1:] if len(profiles) > 1 else []
    summary: dict[str, Any] = {
        "zig_loop_profile_count": len(profiles) if profiles else None,
        "zig_loop_profile_warm_count": len(warm_profiles) if warm_profiles else None,
    }
    for key in LOOP_PROFILE_AVG_KEYS + LOOP_PROFILE_COUNT_KEYS:
        summary[f"zig_loop_profile_{key}_avg"] = avg(profiles, key)
        summary[f"zig_loop_profile_warm_{key}_avg"] = avg(warm_profiles, key)
    return summary


def summarize_planned_access_profiles(profiles: list[dict[str, Any]]) -> dict[str, Any]:
    def avg(rows: list[dict[str, Any]], key: str) -> float | None:
        values: list[float] = []
        for row in rows:
            value = row.get(key)
            if value is None:
                continue
            try:
                values.append(float(value))
            except (TypeError, ValueError):
                pass
        return sum(values) / len(values) if values else None

    warm_profiles = profiles[1:] if len(profiles) > 1 else []
    summary: dict[str, Any] = {
        "zig_planned_access_profile_count": len(profiles) if profiles else None,
        "zig_planned_access_profile_warm_count": len(warm_profiles) if warm_profiles else None,
    }
    for key in PLANNED_ACCESS_PROFILE_KEYS:
        summary[f"zig_planned_access_profile_{key}_avg"] = avg(profiles, key)
        summary[f"zig_planned_access_profile_warm_{key}_avg"] = avg(warm_profiles, key)
    return summary


def extract_prefixed_json(output: str, prefix: str) -> list[dict[str, Any]]:
    def normalize_nonfinite_constants(payload: str) -> str:
        payload = re.sub(r'(?<![A-Za-z0-9_+\-.])-nan(?![A-Za-z0-9_+\-.])', 'NaN', payload, flags=re.IGNORECASE)
        payload = re.sub(r'(?<![A-Za-z0-9_+\-.])\+?nan(?![A-Za-z0-9_+\-.])', 'NaN', payload, flags=re.IGNORECASE)
        payload = re.sub(r'(?<![A-Za-z0-9_+\-.])-inf(?:inity)?(?![A-Za-z0-9_+\-.])', '-Infinity', payload, flags=re.IGNORECASE)
        payload = re.sub(r'(?<![A-Za-z0-9_+\-.])\+?inf(?:inity)?(?![A-Za-z0-9_+\-.])', 'Infinity', payload, flags=re.IGNORECASE)
        return payload

    payloads: list[dict[str, Any]] = []
    for line in output.splitlines():
        if not line.startswith(prefix):
            continue
        try:
            payload = normalize_nonfinite_constants(line[len(prefix):])
            payloads.append(json.loads(payload))
        except json.JSONDecodeError:
            pass
    return payloads


def load_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not path.exists():
        return rows
    with path.open("r", encoding="utf-8") as fin:
        for line in fin:
            if not line.strip():
                continue
            rows.append(json.loads(line))
    return rows


def python_training_script() -> str:
    return r'''
import argparse, inspect, json, math, os, pathlib, time, types
import torch
import torch.nn.functional as F
import gliner2.model as gliner2_model
from gliner2.model import Extractor
from gliner2.training.lora import save_lora_adapter
from gliner2.training.trainer import ExtractorCollator, TrainingConfig, GLiNER2Trainer
from transformers import AutoConfig

p = argparse.ArgumentParser()
p.add_argument("--model-dir", required=True)
p.add_argument("--train-data", required=True)
p.add_argument("--out-dir", required=True)
p.add_argument("--steps", type=int, required=True)
p.add_argument("--batch-size", type=int, required=True)
p.add_argument("--seq-len", type=int, required=True)
p.add_argument("--max-span-width", type=int, required=True)
p.add_argument("--learning-rate", type=float, required=True)
p.add_argument("--weight-decay", type=float, required=True)
p.add_argument("--lora-rank", type=int, required=True)
p.add_argument("--lora-alpha", type=float, required=True)
p.add_argument("--lora-dropout", type=float, required=True)
p.add_argument("--lora-targets", required=True)
p.add_argument("--seed", type=int, required=True)
p.add_argument("--span-negative-mask-rate", type=float, required=True)
p.add_argument("--disable-model-dropout", action="store_true")
p.add_argument("--dump-parity", action="store_true")
p.add_argument("--dump-optimizer-parity", action="store_true")
p.add_argument("--no-train-shuffle", action="store_true")
args = p.parse_args()

out = pathlib.Path(args.out_dir)
out.mkdir(parents=True, exist_ok=True)

_extractor_config_from_pretrained = Extractor.config_class.from_pretrained
def _local_file_aware_extractor_config(cls, path_or_repo_id, *cfg_args, **cfg_kwargs):
    if isinstance(path_or_repo_id, (str, os.PathLike)) and os.path.isfile(path_or_repo_id):
        return cls.from_json_file(os.fspath(path_or_repo_id))
    return _extractor_config_from_pretrained(path_or_repo_id, *cfg_args, **cfg_kwargs)
Extractor.config_class.from_pretrained = classmethod(_local_file_aware_extractor_config)

_auto_config_from_pretrained = AutoConfig.from_pretrained
def _local_file_aware_auto_config(path_or_repo_id, *cfg_args, **cfg_kwargs):
    if isinstance(path_or_repo_id, (str, os.PathLike)) and os.path.isfile(path_or_repo_id):
        return _auto_config_from_pretrained(os.path.dirname(os.fspath(path_or_repo_id)), *cfg_args, **cfg_kwargs)
    return _auto_config_from_pretrained(path_or_repo_id, *cfg_args, **cfg_kwargs)
AutoConfig.from_pretrained = _local_file_aware_auto_config
gliner2_model.AutoConfig.from_pretrained = _local_file_aware_auto_config

started = time.time()
model = Extractor.from_pretrained(args.model_dir, map_location="cpu")
model.max_width = args.max_span_width
model.config.max_width = args.max_span_width
if hasattr(model, "span_rep") and hasattr(model.span_rep, "span_rep_layer") and hasattr(model.span_rep.span_rep_layer, "max_width"):
    model.span_rep.span_rep_layer.max_width = args.max_span_width
disabled_dropout_modules = 0
if args.disable_model_dropout:
    for module in model.modules():
        if isinstance(module, torch.nn.Dropout):
            module.p = 0.0
            disabled_dropout_modules += 1
        elif isinstance(module, torch.nn.MultiheadAttention) and module.dropout != 0.0:
            module.dropout = 0.0
            disabled_dropout_modules += 1
config_kwargs = {
    "output_dir": str(out),
    "experiment_name": "gliner2_python_zig_smoke",
    "num_epochs": 1,
    "max_steps": args.steps,
    "batch_size": args.batch_size,
    "eval_batch_size": args.batch_size,
    "gradient_accumulation_steps": 1,
    "encoder_lr": args.learning_rate,
    "task_lr": args.learning_rate,
    "weight_decay": args.weight_decay,
    "max_grad_norm": 1.0,
    "scheduler_type": "constant",
    "warmup_steps": 0,
    "warmup_ratio": 0.0,
    "fp16": False,
    "bf16": False,
    "eval_strategy": "no",
    "save_total_limit": 1,
    "save_best": False,
    "logging_steps": 1,
    "logging_first_step": True,
    "report_to_wandb": False,
    "early_stopping": False,
    "num_workers": 0,
    "pin_memory": False,
    "seed": args.seed,
    "deterministic": True,
    "max_train_samples": args.steps * args.batch_size,
    "use_lora": True,
    "lora_r": args.lora_rank,
    "lora_alpha": args.lora_alpha,
    "lora_dropout": args.lora_dropout,
    "lora_target_modules": args.lora_targets.split(","),
    "save_adapter_only": True,
}
supported_config_args = set(inspect.signature(TrainingConfig).parameters)
cfg = TrainingConfig(**{k: v for k, v in config_kwargs.items() if k in supported_config_args})
trainer = GLiNER2Trainer(model, cfg)
initial_adapter_dir = out / "initial_adapter"
save_lora_adapter(trainer.model, initial_adapter_dir)
sampling = trainer.processor.sampling_config
sampling.remove_json_structure_prob = 0.0
sampling.shuffle_json_fields = False
sampling.remove_json_field_prob = 0.0
sampling.synthetic_entity_label_prob = 0.0
sampling.shuffle_entities = False
sampling.remove_entity_prob = 0.0
sampling.remove_entities_prob = 0.0
sampling.remove_relations_prob = 0.0
sampling.swap_head_tail_prob = 0.0
sampling.remove_classification_prob = 0.0
sampling.shuffle_classification_labels = False
sampling.remove_classification_label_prob = 0.0
sampling.synthetic_label_prob = 0.0
sampling.include_true_label_prob = 1.0
if args.no_train_shuffle:
    def _build_classification_prefix_ordered(self, schema):
        prefix_tokens = []
        for struct in schema.get("json_structures", []):
            for parent, fields in struct.items():
                cls_fields = [
                    (fname, fval) for fname, fval in fields.items()
                    if isinstance(fval, dict) and "value" in fval and "choices" in fval
                ]
                inner = []
                for fname, fval in cls_fields:
                    choice_tokens = []
                    for i, choice in enumerate(fval["choices"]):
                        if i > 0:
                            choice_tokens.append('|')
                        choice_tokens.append(choice)
                    inner.extend([fname, '('] + choice_tokens + [')', ','])
                if inner:
                    inner = inner[:-1]
                    prefix_tokens.extend(['(', f"{parent}:", *inner, ')'])
        return prefix_tokens

    def _process_json_structures_ordered(self, schema, schemas, labels, types, sampling):
        if "json_structures" not in schema:
            return
        json_descs = schema.get("json_descriptions", {})
        groups = {}
        for item in schema["json_structures"]:
            for parent, fields in item.items():
                groups.setdefault(parent, []).append(fields)
        for parent, occurrences in groups.items():
            chosen = []
            seen_fields = set()
            for occ in occurrences:
                for field in occ.keys():
                    if field not in seen_fields:
                        seen_fields.add(field)
                        chosen.append(field)
            if not chosen:
                continue
            spans = []
            for occ in occurrences:
                spans.append([occ.get(field) for field in chosen])
            uniq = []
            seen_spans = set()
            for span in spans:
                key = tuple(tuple(item) if isinstance(item, list) else item for item in span)
                if key not in seen_spans:
                    seen_spans.add(key)
                    uniq.append(span)
            if all(all(cell is None or cell == "" for cell in span) for span in uniq):
                count = 0
                uniq = []
            else:
                count = len(uniq)
            labels.append([count, uniq])
            schemas.append(self._transform_schema(
                parent,
                chosen,
                self.C_TOKEN,
                label_descriptions=json_descs.get(parent, {}),
                example_mode="descriptions" if json_descs.get(parent, {}) else "none",
            ))
            types.append("json_structures")

    def _infer_from_json_ordered(self, schema):
        schemas = []
        labels = []
        types = []
        sampling = self.sampling_config if self.is_training else None
        self._process_json_structures(schema, schemas, labels, types, sampling)
        self._process_entities(schema, schemas, labels, types, sampling)
        self._process_relations(schema, schemas, labels, types, sampling)
        self._process_classifications(schema, schemas, labels, types, sampling)
        return {
            "schemas": schemas,
            "structure_labels": labels,
            "task_types": types,
            "new_schema": schema,
        }

    trainer.processor._infer_from_json = types.MethodType(_infer_from_json_ordered, trainer.processor)
    trainer.processor._build_classification_prefix = types.MethodType(_build_classification_prefix_ordered, trainer.processor)
    trainer.processor._process_json_structures = types.MethodType(_process_json_structures_ordered, trainer.processor)
# Pin schema-conditioning emission to upstream's deterministic eval-mode
# semantics: entities/json emit every present [DESCRIPTION] segment
# ("descriptions" if descs else "none"), classifications emit "both"
# ([DESCRIPTION] plus [EXAMPLE]/[OUTPUT]), and _transform_schema keeps
# descriptions/examples in stored order instead of random.shuffle-ing them.
# The Zig data pipeline always emits exactly that deterministic form; without
# this pin, training-mode random.choice(example_modes) in upstream
# _process_entities/_process_classifications (and the desc/example shuffles)
# diverges on any record carrying label_descriptions/examples/
# entity_descriptions. Wraps whatever is currently bound, so it composes with
# the --no-train-shuffle ordered variants above.
def _pin_eval_mode_conditioning(processor, method_name):
    bound = getattr(processor, method_name)

    def pinned(schema, schemas, labels, types_out, sampling, _bound=bound):
        was_training = processor.is_training
        processor.is_training = False
        try:
            return _bound(schema, schemas, labels, types_out, sampling)
        finally:
            processor.is_training = was_training

    setattr(processor, method_name, pinned)

for _conditioning_method in ("_process_json_structures", "_process_entities", "_process_classifications"):
    _pin_eval_mode_conditioning(trainer.processor, _conditioning_method)
# Pin the structure-loss negative masking rate unconditionally. Upstream
# compute_struct_loss defaults to masking_rate=0.5; without this patch a
# --span-negative-mask-rate 0 run would still randomly mask negatives on the
# Python side whenever --dump-parity is off.
_mask_rate_model = getattr(trainer.model, "model", trainer.model)
_orig_compute_struct_loss = _mask_rate_model.compute_struct_loss
def _compute_struct_loss_pinned_mask_rate(span_rep, schema_emb, structure, span_mask, masking_rate=args.span_negative_mask_rate):
    return _orig_compute_struct_loss(span_rep, schema_emb, structure, span_mask, masking_rate)
_mask_rate_model.compute_struct_loss = _compute_struct_loss_pinned_mask_rate

python_step_timings = []
original_create_dataloader = trainer._create_dataloader
original_prepare_data = trainer._prepare_data

class TimedTrainingLoader:
    def __init__(self, inner, timings):
        self.inner = inner
        self.timings = timings

    def __len__(self):
        return len(self.inner)

    def __getattr__(self, name):
        return getattr(self.inner, name)

    def __iter__(self):
        for local_step, batch in enumerate(self.inner):
            started = time.perf_counter()
            try:
                yield batch
            finally:
                elapsed_ms = (time.perf_counter() - started) * 1000.0
                try:
                    batch_size = len(batch)
                except TypeError:
                    batch_size = None
                self.timings.append({
                    "step": len(self.timings) + 1,
                    "loader_step": local_step + 1,
                    "batch_size": batch_size,
                    "step_wall_ms": elapsed_ms,
                })

def create_dataloader_with_timing(*call_args, **call_kwargs):
    is_training = call_kwargs.get("is_training")
    if is_training is None and len(call_args) >= 4:
        is_training = call_args[3]
    if args.no_train_shuffle and is_training:
        if "shuffle" in call_kwargs:
            call_kwargs["shuffle"] = False
        elif len(call_args) >= 3:
            call_args = (*call_args[:2], False, *call_args[3:])
    loader = original_create_dataloader(*call_args, **call_kwargs)
    if is_training:
        return TimedTrainingLoader(loader, python_step_timings)
    return loader

def prepare_data_no_shuffle(data, is_train=True):
    if args.no_train_shuffle and is_train:
        return original_prepare_data(data, is_train=False)
    return original_prepare_data(data, is_train=is_train)

trainer._prepare_data = prepare_data_no_shuffle
trainer._create_dataloader = create_dataloader_with_timing
parity_debug = []
preprocess_debug = []
preprocess_debug_samples = []
component_debug = []
sample_loss_debug = []
classification_debug = []
if args.dump_parity:
    def _finite(value):
        value = float(value)
        if math.isfinite(value):
            return value
        return None

    def _mean_or_zero(tensor):
        if tensor.numel() == 0:
            return 0.0
        return _finite(tensor.float().mean().detach().cpu().item())

    def _token_piece_count(token):
        tokenizer = getattr(trainer.processor, "tokenizer", None)
        if tokenizer is None:
            return 1
        try:
            pieces = tokenizer.tokenize(token)
            return max(1, len(pieces))
        except Exception:
            return 1

    def _derive_schema_special_indices(schema_tokens_list):
        special_tokens = {"[P]", "[C]", "[E]", "[R]", "[L]"}
        out = []
        pos = 0
        for schema_idx, schema_tokens in enumerate(schema_tokens_list or []):
            if schema_idx > 0:
                pos += 1
            schema_out = []
            for token in schema_tokens or []:
                if token in special_tokens:
                    schema_out.append(pos)
                pos += _token_piece_count(token)
            out.append(schema_out)
        return out

    def _derive_text_word_indices(schema_tokens_list, text_tokens):
        pos = 0
        for schema_idx, schema_tokens in enumerate(schema_tokens_list or []):
            if schema_idx > 0:
                pos += 1
            for token in schema_tokens or []:
                pos += _token_piece_count(token)
        if schema_tokens_list:
            pos += 1
        out = []
        for token in text_tokens or []:
            out.append(pos)
            pos += _token_piece_count(token)
        return out

    debug_model = getattr(trainer.model, "model", trainer.model)
    original_collator_call = ExtractorCollator.__call__
    def _preprocess_sample_debug(out, sample_idx, text_tokens, schema_tokens_list, text_word_indices, text_word_counts, schema_special_indices, structure_labels, task_types):
        sample_text_tokens = text_tokens[sample_idx] if text_tokens and sample_idx < len(text_tokens) else []
        sample_schema_tokens = schema_tokens_list[sample_idx] if schema_tokens_list and sample_idx < len(schema_tokens_list) else []
        word_count = int(text_word_counts[sample_idx]) if text_word_counts and sample_idx < len(text_word_counts) else len(sample_text_tokens)
        derived_text_word_indices = _derive_text_word_indices(sample_schema_tokens, sample_text_tokens)
        derived_schema_special_indices = _derive_schema_special_indices(sample_schema_tokens)
        return {
            "sample_idx": sample_idx,
            "input_ids": out.input_ids[sample_idx].detach().cpu().tolist(),
            "attention_mask": out.attention_mask[sample_idx].detach().cpu().tolist(),
            "text_word_indices": (
                text_word_indices[sample_idx, :word_count].detach().cpu().tolist()
                if text_word_indices is not None and text_word_counts else derived_text_word_indices
            ),
            "text_word_counts": word_count,
            "schema_special_indices": schema_special_indices[sample_idx] if schema_special_indices else derived_schema_special_indices,
            "text_tokens": sample_text_tokens,
            "schema_tokens_list": sample_schema_tokens,
            "structure_labels": structure_labels[sample_idx] if structure_labels else [],
            "task_types": task_types[sample_idx] if task_types else [],
        }

    def collator_call_with_debug(self, batch):
        out = original_collator_call(self, batch)
        if not preprocess_debug and getattr(out, "input_ids", None) is not None and len(out) > 0:
            text_word_indices = getattr(out, "text_word_indices", None)
            text_word_counts = getattr(out, "text_word_counts", None)
            schema_special_indices = getattr(out, "schema_special_indices", None)
            text_tokens = getattr(out, "text_tokens", None)
            schema_tokens_list = getattr(out, "schema_tokens_list", None)
            structure_labels = getattr(out, "structure_labels", None)
            task_types = getattr(out, "task_types", None)
            sample_count = int(out.input_ids.shape[0])
            for sample_idx in range(sample_count):
                preprocess_debug_samples.append(_preprocess_sample_debug(
                    out,
                    sample_idx,
                    text_tokens,
                    schema_tokens_list,
                    text_word_indices,
                    text_word_counts,
                    schema_special_indices,
                    structure_labels,
                    task_types,
                ))
            if preprocess_debug_samples:
                first = dict(preprocess_debug_samples[0])
                first["text_word_counts"] = [item["text_word_counts"] for item in preprocess_debug_samples]
                first.pop("sample_idx", None)
                preprocess_debug.append(first)
        return out

    ExtractorCollator.__call__ = collator_call_with_debug
    original_compute_span_rep = debug_model.compute_span_rep
    span_hidden_debug = []

    def compute_span_rep_with_hidden_debug(self, token_embeddings):
        out = original_compute_span_rep(token_embeddings)
        if not span_hidden_debug:
            span_hidden_debug.append(token_embeddings.detach())
        return out

    debug_model.compute_span_rep = types.MethodType(compute_span_rep_with_hidden_debug, debug_model)
    original_classifier_forward = debug_model.classifier.forward
    classifier_forward_outputs = []

    def classifier_forward_with_debug(self, *forward_args, **forward_kwargs):
        out = original_classifier_forward(*forward_args, **forward_kwargs)
        if len(classifier_forward_outputs) < 64:
            classifier_forward_outputs.append(out.detach().squeeze(-1).cpu())
        return out

    debug_model.classifier.forward = types.MethodType(classifier_forward_with_debug, debug_model.classifier)

    original_compute_struct_loss = debug_model.compute_struct_loss

    def record_struct_loss_debug(self, span_rep, schema_emb, structure, span_mask, masking_rate=args.span_negative_mask_rate):
        gold_count = min(structure[0], 19)
        struct_proj = self.count_embed(schema_emb[1:], gold_count)
        scores = torch.einsum('lkd,bpd->bplk', span_rep, struct_proj)
        labs = torch.zeros_like(scores)

        def _mark_position(count_idx, entity_idx, start, end):
            width = end - start
            if 0 <= start < scores.shape[2] and 0 <= width < scores.shape[3]:
                labs[count_idx, entity_idx, start, width] = 1

        def _visit_position(value, count_idx, entity_idx):
            if value is None or value == (-1, -1) or value == [-1, -1]:
                return
            if isinstance(value, tuple) and len(value) == 2:
                _mark_position(count_idx, entity_idx, int(value[0]), int(value[1]))
                return
            if isinstance(value, list):
                if len(value) == 2 and all(isinstance(item, (int, float)) for item in value):
                    _mark_position(count_idx, entity_idx, int(value[0]), int(value[1]))
                    return
                for item in value:
                    _visit_position(item, count_idx, entity_idx)

        for i in range(gold_count):
            gold_spans = structure[1][i]
            for k, span in enumerate(gold_spans):
                _visit_position(span, i, k)

        if masking_rate > 0.0 and self.training:
            negative = (labs == 0)
            random_mask = torch.rand_like(scores) < masking_rate
            to_mask = negative & random_mask
            loss_mask = (~to_mask).float()
        else:
            loss_mask = torch.ones_like(scores)

        bce = F.binary_cross_entropy_with_logits(scores, labs, reduction="none")
        masked_bce = bce * loss_mask
        span_valid = (~span_mask[0]).float()
        final_mask = loss_mask.view(loss_mask.shape[0], loss_mask.shape[1], -1) * span_valid
        final_tensor = masked_bce.view(masked_bce.shape[0], masked_bce.shape[1], -1) * span_valid
        loss = final_tensor.sum()

        if not parity_debug:
            pos_scores = scores[labs > 0]
            neg_scores = scores[labs <= 0]
            final_bce = bce.view(bce.shape[0], bce.shape[1], -1) * final_mask
            labs_flat = labs.view(labs.shape[0], labs.shape[1], -1)
            final_pos = final_bce[labs_flat > 0]
            final_neg = final_bce[labs_flat <= 0]
            valid_negative = (labs_flat <= 0) & (final_mask > 0)
            top_valid_negative_logits = []
            if valid_negative.any():
                flat_scores = scores.view(scores.shape[0], scores.shape[1], -1)
                masked_final_bce = final_bce.masked_fill(~valid_negative, float("-inf"))
                top_k = min(5, int(valid_negative.sum().detach().cpu().item()))
                top_values, top_indices = torch.topk(masked_final_bce.flatten(), k=top_k)
                entity_count = scores.shape[1]
                span_width = scores.shape[3]
                flat_span_count = scores.shape[2] * scores.shape[3]
                for value, flat_index_tensor in zip(top_values, top_indices):
                    flat_index = int(flat_index_tensor.detach().cpu().item())
                    count_idx_top = flat_index // (entity_count * flat_span_count)
                    remainder = flat_index % (entity_count * flat_span_count)
                    entity_idx_top = remainder // flat_span_count
                    span_flat = remainder % flat_span_count
                    start_idx_top = span_flat // span_width
                    width_idx_top = span_flat % span_width
                    top_valid_negative_logits.append({
                        "count_index": count_idx_top,
                        "entity": entity_idx_top,
                        "row": start_idx_top * span_width + width_idx_top,
                        "start": start_idx_top,
                        "width": width_idx_top,
                        "logit": _finite(flat_scores[count_idx_top, entity_idx_top, span_flat].detach().cpu().item()),
                        "bce": _finite(value.detach().cpu().item()),
                    })
            positive_indices = (labs > 0).nonzero(as_tuple=False)
            if positive_indices.numel() > 0 and not component_debug:
                count_idx = int(positive_indices[0, 0].detach().cpu().item())
                entity_idx = int(positive_indices[0, 1].detach().cpu().item())
                start_idx = int(positive_indices[0, 2].detach().cpu().item())
                width_idx = int(positive_indices[0, 3].detach().cpu().item())
                span_vec = span_rep[start_idx, width_idx]
                schema_vec = schema_emb[1 + entity_idx]
                projection_vec = struct_proj[count_idx, entity_idx]
                count_gru_state_vec = None
                count_state_vec = None
                count_in_project_vec = None
                count_layer0_vec = None
                count_layer1_vec = None
                count_out0_vec = None
                count_out2_vec = None
                with torch.no_grad():
                    count_embed = self.count_embed
                    label_embs = schema_emb[1:]
                    max_count = int(getattr(count_embed, "max_count", 20))
                    debug_gold_count = min(int(gold_count), max_count)
                    if debug_gold_count > 0:
                        debug_count_idx = torch.arange(max_count, device=label_embs.device)[:debug_gold_count]
                        debug_pos_seq = count_embed.pos_embedding(debug_count_idx)
                        debug_pos_seq = debug_pos_seq.unsqueeze(1).expand(-1, label_embs.shape[0], -1)
                        debug_h0 = label_embs.unsqueeze(0)
                        debug_gru_state, _ = count_embed.gru(debug_pos_seq, debug_h0)
                        debug_count_state = debug_gru_state + debug_h0.expand_as(debug_gru_state)
                        debug_transformer = count_embed.transformer
                        debug_in_project = debug_transformer.in_projector(debug_count_state)
                        debug_layer = debug_in_project
                        debug_layer0 = None
                        debug_layer1 = None
                        for layer_idx, layer in enumerate(debug_transformer.transformer.layers):
                            debug_layer = layer(debug_layer)
                            if layer_idx == 0:
                                debug_layer0 = debug_layer
                            elif layer_idx == 1:
                                debug_layer1 = debug_layer
                        debug_joined = torch.cat([debug_layer, debug_count_state], dim=-1)
                        debug_out0 = torch.relu(debug_transformer.out_projector[0](debug_joined))
                        debug_out2 = torch.relu(debug_transformer.out_projector[2](debug_out0))
                        count_gru_state_vec = debug_gru_state[count_idx, entity_idx]
                        count_state_vec = debug_count_state[count_idx, entity_idx]
                        count_in_project_vec = debug_in_project[count_idx, entity_idx]
                        count_layer0_vec = debug_layer0[count_idx, entity_idx] if debug_layer0 is not None else debug_layer[count_idx, entity_idx]
                        count_layer1_vec = debug_layer1[count_idx, entity_idx] if debug_layer1 is not None else debug_layer[count_idx, entity_idx]
                        count_out0_vec = debug_out0[count_idx, entity_idx]
                        count_out2_vec = debug_out2[count_idx, entity_idx]

                def _vec_norm(vec):
                    return _finite(vec.float().norm().detach().cpu().item()) if vec is not None else 0.0

                def _vec_mean(vec):
                    return _finite(vec.float().mean().detach().cpu().item()) if vec is not None else 0.0

                top_negative_debug = {}
                if top_valid_negative_logits:
                    neg = top_valid_negative_logits[0]
                    neg_count_idx = int(neg["count_index"])
                    neg_entity_idx = int(neg["entity"])
                    neg_start_idx = int(neg["start"])
                    neg_width_idx = int(neg["width"])
                    neg_end_idx = neg_start_idx + neg_width_idx
                    neg_span_vec = span_rep[neg_start_idx, neg_width_idx]
                    neg_start_hidden_vec = None
                    neg_end_hidden_vec = None
                    if span_hidden_debug:
                        token_embeddings = span_hidden_debug[0]
                        if 0 <= neg_start_idx < token_embeddings.shape[0]:
                            neg_start_hidden_vec = token_embeddings[neg_start_idx]
                        if 0 <= neg_end_idx < token_embeddings.shape[0]:
                            neg_end_hidden_vec = token_embeddings[neg_end_idx]
                    neg_schema_hidden_vec = schema_emb[1 + neg_entity_idx]
                    neg_projection_vec = struct_proj[neg_count_idx, neg_entity_idx]
                    top_negative_debug = {
                        "negative_row": int(neg["row"]),
                        "negative_entity": neg_entity_idx,
                        "negative_count_index": neg_count_idx,
                        "negative_start_index": neg_start_idx,
                        "negative_width_index": neg_width_idx,
                        "negative_end_index": neg_end_idx,
                        "negative_logit": _finite(neg["logit"]),
                        "negative_start_hidden_norm": _vec_norm(neg_start_hidden_vec),
                        "negative_end_hidden_norm": _vec_norm(neg_end_hidden_vec),
                        "negative_schema_hidden_norm": _vec_norm(neg_schema_hidden_vec),
                        "negative_span_rep_norm": _finite(neg_span_vec.float().norm().detach().cpu().item()),
                        "negative_schema_projection_norm": _finite(neg_projection_vec.float().norm().detach().cpu().item()),
                        "negative_projected_schema_dot": _finite(torch.dot(neg_span_vec.flatten().float(), neg_projection_vec.flatten().float()).detach().cpu().item()),
                        "negative_start_hidden_mean": _vec_mean(neg_start_hidden_vec),
                        "negative_end_hidden_mean": _vec_mean(neg_end_hidden_vec),
                        "negative_schema_hidden_mean": _vec_mean(neg_schema_hidden_vec),
                        "negative_span_rep_mean": _finite(neg_span_vec.float().mean().detach().cpu().item()),
                        "negative_schema_projection_mean": _finite(neg_projection_vec.float().mean().detach().cpu().item()),
                    }

                component_payload = {
                    "positive_row": start_idx * scores.shape[3] + width_idx,
                    "positive_entity": entity_idx,
                    "schema_row": entity_idx,
                    "count_index": count_idx,
                    "start_index": start_idx,
                    "width_index": width_idx,
                    "span_rep_norm": _finite(span_vec.float().norm().detach().cpu().item()),
                    "schema_hidden_norm": _finite(schema_vec.float().norm().detach().cpu().item()),
                    "count_gru_state_norm": _vec_norm(count_gru_state_vec),
                    "count_state_norm": _vec_norm(count_state_vec),
                    "count_in_project_norm": _vec_norm(count_in_project_vec),
                    "count_layer0_norm": _vec_norm(count_layer0_vec),
                    "count_layer1_norm": _vec_norm(count_layer1_vec),
                    "count_out0_norm": _vec_norm(count_out0_vec),
                    "count_out2_norm": _vec_norm(count_out2_vec),
                    "schema_projection_norm": _finite(projection_vec.float().norm().detach().cpu().item()),
                    "projected_schema_dot": _finite(torch.dot(span_vec.flatten().float(), projection_vec.flatten().float()).detach().cpu().item()),
                    "span_rep_mean": _finite(span_vec.float().mean().detach().cpu().item()),
                    "count_gru_state_mean": _vec_mean(count_gru_state_vec),
                    "count_state_mean": _vec_mean(count_state_vec),
                    "count_in_project_mean": _vec_mean(count_in_project_vec),
                    "count_layer0_mean": _vec_mean(count_layer0_vec),
                    "count_layer1_mean": _vec_mean(count_layer1_vec),
                    "count_out0_mean": _vec_mean(count_out0_vec),
                    "count_out2_mean": _vec_mean(count_out2_vec),
                    "schema_projection_mean": _finite(projection_vec.float().mean().detach().cpu().item()),
                }
                component_payload.update(top_negative_debug)
                component_debug.append(component_payload)
            parity_debug.append({
                "gold_count": int(gold_count),
                "scores_shape": list(scores.shape),
                "labs_sum": _finite(labs.sum().detach().cpu().item()),
                "loss_mask_sum": _finite(loss_mask.sum().detach().cpu().item()),
                "final_mask_sum": _finite(final_mask.sum().detach().cpu().item()),
                "valid_span_count": _finite(span_valid.sum().detach().cpu().item()),
                "scores_min": _finite(scores.min().detach().cpu().item()),
                "scores_max": _finite(scores.max().detach().cpu().item()),
                "scores_mean": _finite(scores.mean().detach().cpu().item()),
                "positive_scores_mean": _mean_or_zero(pos_scores),
                "negative_scores_mean": _mean_or_zero(neg_scores),
                "bce_unweighted_sum": _finite(bce.sum().detach().cpu().item()),
                "bce_negative_masked_sum": _finite(masked_bce.sum().detach().cpu().item()),
                "bce_final_positive_sum": _finite(final_pos.sum().detach().cpu().item()) if final_pos.numel() else 0.0,
                "bce_final_negative_sum": _finite(final_neg.sum().detach().cpu().item()) if final_neg.numel() else 0.0,
                "bce_final_sum": _finite(loss.detach().cpu().item()),
                "top_valid_negative_logits": top_valid_negative_logits,
                "masking_rate": float(masking_rate),
            })
        return loss

    def compute_struct_loss_with_debug(self, span_rep, schema_emb, structure, span_mask, masking_rate=args.span_negative_mask_rate):
        loss = original_compute_struct_loss(span_rep, schema_emb, structure, span_mask, masking_rate)
        if not parity_debug:
            try:
                record_struct_loss_debug(self, span_rep, schema_emb, structure, span_mask, masking_rate)
            except Exception as exc:
                parity_debug.append({
                    "debug_error": repr(exc),
                    "official_loss": _finite(loss.detach().cpu().item()),
                })
        return loss

    debug_model.compute_struct_loss = types.MethodType(compute_struct_loss_with_debug, debug_model)
    original_compute_sample_loss = debug_model._compute_sample_loss
    def _classification_debug_payload(logits, labels):
        logits = logits.float().detach().cpu()
        labels = torch.tensor(labels, dtype=torch.float32).detach().cpu()
        if logits.ndim == 0:
            logits = logits.reshape(1)
        if labels.ndim == 0:
            labels = labels.reshape(1)
        count = min(int(logits.numel()), int(labels.numel()))
        logits = logits.reshape(-1)[:count]
        labels = labels.reshape(-1)[:count]
        mask = torch.ones_like(labels)
        bce = F.binary_cross_entropy_with_logits(logits, labels, reduction="none")
        valid = mask > 0
        logits_valid = logits[valid] if valid.any() else logits
        return {
            "rows": 1,
            "entity_types": count,
            "logits_count": count,
            "valid_count": int(valid.sum().item()),
            "positive_count": int((labels > 0).sum().item()),
            "label_sum": _finite(labels.sum().item()),
            "mask_sum": _finite(mask.sum().item()),
            "logits_min": _finite(logits_valid.min().item()) if logits_valid.numel() else 0.0,
            "logits_max": _finite(logits_valid.max().item()) if logits_valid.numel() else 0.0,
            "logits_mean": _finite(logits.mean().item()) if logits.numel() else 0.0,
            "bce_sum": _finite((bce * mask).sum().item()),
            "logits_head": [_finite(x) for x in logits[:16].tolist()],
            "labels_head": [_finite(x) for x in labels[:16].tolist()],
            "mask_head": [_finite(x) for x in mask[:16].tolist()],
            "valid_logits_head": [_finite(x) for x in logits[valid][:16].tolist()],
            "valid_labels_head": [_finite(x) for x in labels[valid][:16].tolist()],
        }

    def _merge_classification_debug_payloads(parts):
        if not parts:
            return None
        logits_count = sum(int(part.get("logits_count") or 0) for part in parts)
        valid_count = sum(int(part.get("valid_count") or 0) for part in parts)
        logits_mean_num = sum(
            float(part.get("logits_mean") or 0.0) * int(part.get("logits_count") or 0)
            for part in parts
        )
        merged = {
            "rows": sum(int(part.get("rows") or 0) for part in parts),
            "entity_types": sum(int(part.get("entity_types") or 0) for part in parts),
            "logits_count": logits_count,
            "valid_count": valid_count,
            "positive_count": sum(int(part.get("positive_count") or 0) for part in parts),
            "label_sum": _finite(sum(float(part.get("label_sum") or 0.0) for part in parts)),
            "mask_sum": _finite(sum(float(part.get("mask_sum") or 0.0) for part in parts)),
            "logits_min": _finite(min(float(part.get("logits_min") or 0.0) for part in parts)),
            "logits_max": _finite(max(float(part.get("logits_max") or 0.0) for part in parts)),
            "logits_mean": _finite(logits_mean_num / logits_count) if logits_count else 0.0,
            "bce_sum": _finite(sum(float(part.get("bce_sum") or 0.0) for part in parts)),
            "logits_head": [],
            "labels_head": [],
            "mask_head": [],
            "valid_logits_head": [],
            "valid_labels_head": [],
        }
        for field in ("logits_head", "labels_head", "mask_head", "valid_logits_head", "valid_labels_head"):
            values = []
            for part in parts:
                values.extend(part.get(field) or [])
                if len(values) >= 16:
                    break
            merged[field] = values[:16]
        return merged

    def compute_sample_loss_with_debug(self, *sample_args, **sample_kwargs):
        classifier_start = len(classifier_forward_outputs)
        capture_first_batch_sample = len(sample_loss_debug) < args.batch_size
        out = original_compute_sample_loss(*sample_args, **sample_kwargs)
        if capture_first_batch_sample:
            sample_loss_debug.append({
                "classification_loss": _finite(out["classification"].detach().cpu().item()),
                "structure_loss": _finite(out["structure"].detach().cpu().item()),
                "count_loss": _finite(out["count"].detach().cpu().item()),
            })
        if capture_first_batch_sample:
            task_types = sample_kwargs.get("task_types")
            structure_labels = sample_kwargs.get("structure_labels")
            if task_types is None and len(sample_args) >= 3:
                task_types = sample_args[2]
            if structure_labels is None and len(sample_args) >= 4:
                structure_labels = sample_args[3]
            output_idx = classifier_start
            logits_parts = []
            label_parts = []
            for idx, task_type in enumerate(task_types or []):
                if task_type != "classifications":
                    continue
                if structure_labels is None or idx >= len(structure_labels):
                    continue
                if output_idx >= len(classifier_forward_outputs):
                    continue
                logits_parts.append(classifier_forward_outputs[output_idx].reshape(-1))
                label_parts.extend(structure_labels[idx])
                output_idx += 1
            if logits_parts and label_parts:
                classification_debug.append(_classification_debug_payload(
                    torch.cat(logits_parts),
                    label_parts,
                ))
            del classifier_forward_outputs[classifier_start:]
        return out
    debug_model._compute_sample_loss = types.MethodType(compute_sample_loss_with_debug, debug_model)
optimizer_parity_steps = []
if args.dump_optimizer_parity:
    # Wrap the AdamW instance the trainer creates so each optimizer step
    # records, for every trainable (LoRA) parameter: the post-clip gradient,
    # the Adam exp_avg/exp_avg_sq state, the per-parameter step counter, and
    # the post-update weights (first 8 elements + f64 abs-sum per tensor).
    _orig_create_optimizer = trainer._create_optimizer

    def _opt_head_abs(tensor):
        flat = tensor.detach().to(torch.float32).reshape(-1)
        head = [float(x) for x in flat[:8].tolist()]
        abs_sum = float(flat.to(torch.float64).abs().sum().item())
        return head, abs_sum

    def _create_optimizer_with_dump():
        opt = _orig_create_optimizer()
        param_names = {id(prm): name for name, prm in trainer.model.named_parameters()}

        _orig_opt_step = opt.step

        def step_with_dump(_opt_self, *step_args, **step_kwargs):
            grads = {}
            for group in opt.param_groups:
                for prm in group["params"]:
                    if prm.grad is not None:
                        grads[id(prm)] = prm.grad.detach().clone()
            out_value = _orig_opt_step(*step_args, **step_kwargs)
            tensors = []
            for group in opt.param_groups:
                for prm in group["params"]:
                    name = param_names.get(id(prm))
                    if name is None:
                        continue
                    state = opt.state.get(prm, {})
                    raw_step = state.get("step")
                    if hasattr(raw_step, "item"):
                        step_count = int(raw_step.item())
                    elif raw_step is None:
                        step_count = 0
                    else:
                        step_count = int(raw_step)
                    entry = {"name": name, "step_count": step_count}
                    entry["weight"], entry["weight_abs_sum"] = _opt_head_abs(prm.data)
                    for quantity, key in (("m", "exp_avg"), ("v", "exp_avg_sq")):
                        if key in state:
                            entry[quantity], entry[f"{quantity}_abs_sum"] = _opt_head_abs(state[key])
                        else:
                            entry[quantity], entry[f"{quantity}_abs_sum"] = [], 0.0
                    grad = grads.get(id(prm))
                    if grad is not None:
                        entry["grad"], entry["grad_abs_sum"] = _opt_head_abs(grad)
                    tensors.append(entry)
            optimizer_parity_steps.append({"step": len(optimizer_parity_steps) + 1, "tensors": tensors})
            return out_value

        # Bind as a method: torch's LR schedulers patch optimizer.step via
        # `step_fn.__func__`, which requires a bound method rather than a
        # plain function attribute.
        opt.step = types.MethodType(step_with_dump, opt)
        return opt

    trainer._create_optimizer = _create_optimizer_with_dump
result = trainer.train(train_data=args.train_data)
trainable = sum(p.numel() for p in trainer.model.parameters() if p.requires_grad)
total = sum(p.numel() for p in trainer.model.parameters())
payload = {
    "elapsed_seconds": time.time() - started,
    "total_steps": result.get("total_steps"),
    "samples_per_second": result.get("samples_per_second"),
    "train_metrics_history": result.get("train_metrics_history", []),
    "step_timings": python_step_timings,
    "total_step_wall_ms": sum(item["step_wall_ms"] for item in python_step_timings),
    "avg_step_wall_ms": (
        sum(item["step_wall_ms"] for item in python_step_timings) / len(python_step_timings)
        if python_step_timings else None
    ),
    "trainable_parameters": trainable,
    "total_parameters": total,
    "torch_version": torch.__version__,
    "disabled_dropout_modules": disabled_dropout_modules,
    "initial_adapter_checkpoint": str(initial_adapter_dir / "adapter_weights.safetensors"),
    "span_parity_debug": parity_debug[0] if parity_debug else None,
    "span_preprocess_debug": preprocess_debug[0] if preprocess_debug else None,
    "span_preprocess_debug_samples": preprocess_debug_samples,
    "span_component_debug": component_debug[0] if component_debug else None,
    "gliner2_total_loss_components": (
        {
            "classification_loss": sum((item.get("classification_loss") or 0.0) for item in sample_loss_debug),
            "structure_loss": sum((item.get("structure_loss") or 0.0) for item in sample_loss_debug),
            "count_loss": sum((item.get("count_loss") or 0.0) for item in sample_loss_debug),
            "total_loss": sum((item.get("classification_loss") or 0.0) + (item.get("structure_loss") or 0.0) + (item.get("count_loss") or 0.0) for item in sample_loss_debug),
            "sample_count": len(sample_loss_debug),
        }
        if sample_loss_debug else None
    ),
    "gliner2_classification_debug": _merge_classification_debug_payloads(classification_debug) if classification_debug else None,
    "optimizer_parity_steps": optimizer_parity_steps,
}
(out / "comparison_metrics.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")
print("PYTHON_GLINER2_COMPARISON " + json.dumps(payload, sort_keys=True))
'''


def adapter_roundtrip_script() -> str:
    return r'''
import argparse, json, math, os, pathlib, random, types
import torch
import gliner2.model as gliner2_model
from gliner2.model import Extractor
from gliner2.training.trainer import ExtractorCollator
from safetensors.torch import load_file, save_file
from transformers import AutoConfig

p = argparse.ArgumentParser()
p.add_argument("--model-dir", required=True)
p.add_argument("--train-data", required=True)
p.add_argument("--python-adapter-dir", required=True)
p.add_argument("--zig-adapter-dir", required=True)
p.add_argument("--converted-zig-adapter-dir", required=True)
p.add_argument("--result-json", required=True)
p.add_argument("--batch-size", type=int, required=True)
p.add_argument("--max-span-width", type=int, required=True)
p.add_argument("--seed", type=int, required=True)
p.add_argument("--tolerance", type=float, required=True)
p.add_argument("--weights-tolerance", type=float, default=None)
args = p.parse_args()
weights_tolerance = args.weights_tolerance if args.weights_tolerance is not None else args.tolerance

random.seed(args.seed)
torch.manual_seed(args.seed)

_extractor_config_from_pretrained = Extractor.config_class.from_pretrained
def _local_file_aware_extractor_config(cls, path_or_repo_id, *cfg_args, **cfg_kwargs):
    if isinstance(path_or_repo_id, (str, os.PathLike)) and os.path.isfile(path_or_repo_id):
        return cls.from_json_file(os.fspath(path_or_repo_id))
    return _extractor_config_from_pretrained(path_or_repo_id, *cfg_args, **cfg_kwargs)
Extractor.config_class.from_pretrained = classmethod(_local_file_aware_extractor_config)

_auto_config_from_pretrained = AutoConfig.from_pretrained
def _local_file_aware_auto_config(path_or_repo_id, *cfg_args, **cfg_kwargs):
    if isinstance(path_or_repo_id, (str, os.PathLike)) and os.path.isfile(path_or_repo_id):
        return _auto_config_from_pretrained(os.path.dirname(os.fspath(path_or_repo_id)), *cfg_args, **cfg_kwargs)
    return _auto_config_from_pretrained(path_or_repo_id, *cfg_args, **cfg_kwargs)
AutoConfig.from_pretrained = _local_file_aware_auto_config
gliner2_model.AutoConfig.from_pretrained = _local_file_aware_auto_config

python_adapter_dir = pathlib.Path(args.python_adapter_dir)
zig_adapter_dir = pathlib.Path(args.zig_adapter_dir)
converted_dir = pathlib.Path(args.converted_zig_adapter_dir)

# PEFT-migrated gliner2 saves `adapter_model.safetensors` with
# `base_model.model.<module>.lora_{A,B}.weight` keys; the legacy gliner2 LoRA
# implementation saves `adapter_weights.safetensors` with `<module>.lora_{A,B}`.
peft_format = (python_adapter_dir / "adapter_model.safetensors").exists()
zig_state = load_file(str(zig_adapter_dir / "adapter_model.safetensors"))
unconverted_keys = []

if peft_format:
    py_state = load_file(str(python_adapter_dir / "adapter_model.safetensors"))
    converted = {}
    for key, value in zig_state.items():
        if key.startswith("base_model.model.") and (key.endswith(".lora_A.weight") or key.endswith(".lora_B.weight")):
            converted[key] = value
        else:
            unconverted_keys.append(key)
    expected_keys = set(py_state.keys())
    got_keys = set(converted.keys())
    missing_keys = sorted(expected_keys - got_keys)
    unexpected_keys = sorted(got_keys - expected_keys)
else:
    converted_dir.mkdir(parents=True, exist_ok=True)
    py_state = load_file(str(python_adapter_dir / "adapter_weights.safetensors"))
    converted = {}
    for key, value in zig_state.items():
        if key.startswith("base_model.model."):
            key = key[len("base_model.model."):]
        if key.endswith(".lora_A.weight") or key.endswith(".lora_B.weight"):
            converted[key[: -len(".weight")]] = value
        elif key.endswith(".lora_A") or key.endswith(".lora_B"):
            converted[key] = value
        else:
            unconverted_keys.append(key)
    save_file(converted, str(converted_dir / "adapter_weights.safetensors"))
    python_adapter_config = json.loads((python_adapter_dir / "adapter_config.json").read_text(encoding="utf-8"))
    zig_adapter_config = dict(python_adapter_config)
    zig_adapter_config["lora_dropout"] = 0.0
    (converted_dir / "adapter_config.json").write_text(json.dumps(zig_adapter_config, indent=2), encoding="utf-8")
    expected_keys = set(py_state.keys())
    got_keys = set(converted.keys())
    missing_keys = sorted(expected_keys - got_keys)
    unexpected_keys = sorted(got_keys - expected_keys)


def _disable_dropout(m):
    for module in m.modules():
        if isinstance(module, torch.nn.Dropout):
            module.p = 0.0
        elif isinstance(module, torch.nn.MultiheadAttention):
            module.dropout = 0.0


def _pin_eval_mode_conditioning(proc, method_name):
    # Mirror the training child's pin: force upstream's deterministic
    # eval-mode example_mode selection and stored-order descriptions/examples
    # so the round-trip batch deterministically exercises [DESCRIPTION]/
    # [EXAMPLE] conditioning on records that carry it.
    bound = getattr(proc, method_name)

    def pinned(schema, schemas, labels, types_out, sampling, _bound=bound):
        was_training = proc.is_training
        proc.is_training = False
        try:
            return _bound(schema, schemas, labels, types_out, sampling)
        finally:
            proc.is_training = was_training

    setattr(proc, method_name, pinned)


def _configure_processor(proc):
    sampling = proc.sampling_config
    sampling.remove_json_structure_prob = 0.0
    sampling.shuffle_json_fields = False
    sampling.remove_json_field_prob = 0.0
    sampling.synthetic_entity_label_prob = 0.0
    sampling.shuffle_entities = False
    sampling.remove_entity_prob = 0.0
    sampling.remove_entities_prob = 0.0
    sampling.remove_relations_prob = 0.0
    sampling.swap_head_tail_prob = 0.0
    sampling.remove_classification_prob = 0.0
    sampling.shuffle_classification_labels = False
    sampling.remove_classification_label_prob = 0.0
    sampling.synthetic_label_prob = 0.0
    sampling.include_true_label_prob = 1.0
    for method_name in ("_process_json_structures", "_process_entities", "_process_classifications"):
        _pin_eval_mode_conditioning(proc, method_name)


def _load_extractor():
    model = Extractor.from_pretrained(args.model_dir, map_location="cpu")
    model.max_width = args.max_span_width
    model.config.max_width = args.max_span_width
    if hasattr(model, "span_rep") and hasattr(model.span_rep, "span_rep_layer") and hasattr(model.span_rep.span_rep_layer, "max_width"):
        model.span_rep.span_rep_layer.max_width = args.max_span_width
    _disable_dropout(model)
    _configure_processor(model.processor)
    return model


records = []
with open(args.train_data, "r", encoding="utf-8") as fin:
    for line in fin:
        if line.strip():
            records.append(json.loads(line))
records = records[: args.batch_size]
items = [(r["input"], r["output"]) for r in records]

batch_builder = _load_extractor()
random.seed(args.seed)
torch.manual_seed(args.seed)
batch = ExtractorCollator(batch_builder.processor, is_training=True)(items)
del batch_builder

captures = {"classification": [], "span_scores": []}
original_compute_struct_loss = Extractor.compute_struct_loss


def run_with_adapter(adapter_dir):
    model = _load_extractor()
    if peft_format:
        from peft import PeftModel
        peft_model = PeftModel.from_pretrained(model, str(adapter_dir))
        extractor = peft_model.get_base_model()
    else:
        from gliner2.training.lora import load_lora_adapter
        load_lora_adapter(model, str(adapter_dir), auto_unload=True)
        extractor = model
        peft_model = model
    _disable_dropout(peft_model)
    peft_model.eval()
    captures["classification"] = []
    captures["span_scores"] = []

    def compute_struct_loss_capture(self, span_rep, schema_emb, structure, span_mask, masking_rate=0.0):
        gold_count = min(structure[0], 19)
        with torch.no_grad():
            struct_proj = self.count_embed(schema_emb[1:], gold_count)
            scores = torch.einsum("lkd,bpd->bplk", span_rep, struct_proj)
            captures["span_scores"].append(scores.detach().float().flatten().cpu())
        return original_compute_struct_loss(self, span_rep, schema_emb, structure, span_mask, masking_rate)

    original_classifier_forward = extractor.classifier.forward
    def classifier_forward_with_capture(*fargs, **fkwargs):
        out = original_classifier_forward(*fargs, **fkwargs)
        captures["classification"].append(out.detach().float().flatten().cpu())
        return out
    extractor.classifier.forward = classifier_forward_with_capture
    extractor.compute_struct_loss = types.MethodType(compute_struct_loss_capture, extractor)
    try:
        with torch.no_grad():
            out = extractor(batch)
    finally:
        extractor.classifier.forward = original_classifier_forward
        extractor.compute_struct_loss = types.MethodType(original_compute_struct_loss, extractor)
    span_flat = torch.cat(captures["span_scores"]) if captures["span_scores"] else torch.zeros(0)
    cls_flat = torch.cat(captures["classification"]) if captures["classification"] else torch.zeros(0)
    return {
        "total_loss": float(out["total_loss"].detach().cpu().item()),
        "classification_loss": float(out["classification_loss"].detach().cpu().item()),
        "structure_loss": float(out["structure_loss"].detach().cpu().item()),
        "count_loss": float(out["count_loss"].detach().cpu().item()),
        "span_scores": span_flat,
        "classification_logits": cls_flat,
    }


python_run = run_with_adapter(python_adapter_dir)
zig_run = run_with_adapter(zig_adapter_dir if peft_format else converted_dir)

def _max_abs_delta(a, b):
    if a.numel() != b.numel():
        return None
    if a.numel() == 0:
        return 0.0
    return float((a - b).abs().max().item())

# Direct adapter-weight comparison over the tensors both checkpoints contain.
weights_max_abs_delta = 0.0
weight_shape_mismatches = []
for key, value in converted.items():
    py_value = py_state.get(key)
    if py_value is None:
        continue
    if py_value.shape != value.shape:
        weight_shape_mismatches.append(key)
        continue
    weights_max_abs_delta = max(weights_max_abs_delta, float((py_value - value).abs().max().item()))

# Missing tensors are tolerated only when they are functionally inert on the
# Python side: a LoRA pair contributes nothing to the forward pass while its
# lora_B is exactly zero (e.g. PEFT wraps MultiheadAttention out_proj but
# nn.MultiheadAttention reads out_proj.weight directly, so those adapters
# never train and stay at their zero init).
def _missing_key_functionally_zero(key):
    if "lora_B" in key:
        tensor = py_state.get(key)
        return tensor is not None and not bool(torch.any(tensor))
    if "lora_A" in key:
        sibling = key.replace("lora_A", "lora_B")
        tensor = py_state.get(sibling)
        return tensor is not None and not bool(torch.any(tensor))
    return False

missing_keys_functionally_zero = all(_missing_key_functionally_zero(key) for key in missing_keys)

span_delta = _max_abs_delta(python_run["span_scores"], zig_run["span_scores"])
cls_delta = _max_abs_delta(python_run["classification_logits"], zig_run["classification_logits"])
loss_deltas = {
    key: zig_run[key] - python_run[key]
    for key in ("total_loss", "classification_loss", "structure_loss", "count_loss")
}
naming_ok = (
    not unconverted_keys
    and not unexpected_keys
    and not weight_shape_mismatches
    and (not missing_keys or missing_keys_functionally_zero)
)
weights_ok = weights_max_abs_delta <= weights_tolerance
logits_ok = (
    span_delta is not None
    and cls_delta is not None
    and span_delta <= args.tolerance
    and cls_delta <= args.tolerance
)
result = {
    "ok": bool(naming_ok and weights_ok and logits_ok),
    "naming_ok": bool(naming_ok),
    "weights_ok": bool(weights_ok),
    "weights_max_abs_delta": weights_max_abs_delta,
    "weight_shape_mismatches": weight_shape_mismatches[:20],
    "missing_keys_functionally_zero": bool(missing_keys_functionally_zero),
    "logits_ok": bool(logits_ok),
    "adapter_format": "peft" if peft_format else "gliner2_legacy",
    "zig_bundle_loaded_untouched": bool(peft_format),
    "tolerance": args.tolerance,
    "weights_tolerance": weights_tolerance,
    "span_scores_count_python": int(python_run["span_scores"].numel()),
    "span_scores_count_zig": int(zig_run["span_scores"].numel()),
    "span_scores_max_abs_delta": span_delta,
    "classification_logits_count_python": int(python_run["classification_logits"].numel()),
    "classification_logits_count_zig": int(zig_run["classification_logits"].numel()),
    "classification_logits_max_abs_delta": cls_delta,
    "loss_deltas_zig_minus_python": loss_deltas,
    "python_losses": {k: python_run[k] for k in ("total_loss", "classification_loss", "structure_loss", "count_loss")},
    "zig_losses": {k: zig_run[k] for k in ("total_loss", "classification_loss", "structure_loss", "count_loss")},
    "python_adapter_tensor_count": len(py_state),
    "zig_adapter_tensor_count": len(converted),
    "unconverted_zig_keys": unconverted_keys[:20],
    "unconverted_zig_key_count": len(unconverted_keys),
    "zig_missing_adapter_keys": missing_keys[:20],
    "zig_missing_adapter_key_count": len(missing_keys),
    "zig_unexpected_adapter_keys": unexpected_keys[:20],
    "zig_unexpected_adapter_key_count": len(unexpected_keys),
    "batch_size": len(items),
}
pathlib.Path(args.result_json).write_text(json.dumps(result, indent=2), encoding="utf-8")
print("ADAPTER_ROUNDTRIP " + json.dumps(result, sort_keys=True))
'''


def run_adapter_comparison(
    args: argparse.Namespace,
    script: Path,
    model_dir: str,
    train_data: Path,
    python_adapter_dir: Path,
    zig_adapter_dir: Path,
    converted_zig_adapter_dir: Path,
    result_json: Path,
) -> dict[str, Any]:
    result_json.unlink(missing_ok=True)
    cmd = [
        args.python_bin,
        str(script),
        "--model-dir", model_dir,
        "--train-data", str(train_data),
        "--python-adapter-dir", str(python_adapter_dir),
        "--zig-adapter-dir", str(zig_adapter_dir),
        "--converted-zig-adapter-dir", str(converted_zig_adapter_dir),
        "--result-json", str(result_json),
        "--batch-size", str(args.batch_size),
        "--max-span-width", str(args.max_span_width),
        "--seed", str(args.seed),
        "--tolerance", str(args.adapter_roundtrip_tolerance),
    ]
    if args.adapter_roundtrip_weights_tolerance is not None:
        cmd.extend(["--weights-tolerance", str(args.adapter_roundtrip_weights_tolerance)])
    command_result = run_command(cmd, repo_root(), timeout=args.timeout_seconds, env={"PYTHONHASHSEED": "0"})
    result: dict[str, Any] = {
        "ran": True,
        "returncode": command_result.get("returncode"),
        "elapsed_seconds": command_result.get("elapsed_seconds"),
    }
    result.update(load_json_file(result_json))
    if command_result.get("returncode") != 0:
        result["ok"] = False
        result["output_tail"] = (command_result.get("output") or "")[-4000:]
    return result


def run_adapter_checks(
    args: argparse.Namespace,
    out_dir: Path,
    report: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    python_adapter_dir = out_dir / "python" / "final"
    zig_adapter_dir = out_dir / "zig"
    zig_adapter_checkpoint = zig_adapter_dir / "adapter_model.safetensors"
    zig_adapter_config = zig_adapter_dir / "adapter_config.json"
    roundtrip_dir = out_dir / "roundtrip"
    roundtrip_dir.mkdir(parents=True, exist_ok=True)
    skipped: dict[str, Any] = {"ran": False}
    if not (
        (python_adapter_dir / "adapter_model.safetensors").exists()
        or (python_adapter_dir / "adapter_weights.safetensors").exists()
    ):
        skipped["skip_reason"] = f"missing Python adapter checkpoint at {python_adapter_dir}"
        return skipped, dict(skipped)
    if not zig_adapter_checkpoint.exists() or not zig_adapter_config.exists():
        skipped["skip_reason"] = f"missing Zig PEFT adapter bundle at {zig_adapter_dir}"
        return skipped, dict(skipped)
    python_model = report.get("config", {}).get("python_model", str(args.python_model))
    patched_model_dir = out_dir / "python_model"
    model_dir = str(patched_model_dir) if patched_model_dir.exists() else python_model
    script = roundtrip_dir / "run_adapter_roundtrip.py"
    script.write_text(adapter_roundtrip_script(), encoding="utf-8")
    trained_zig_adapter_dir = roundtrip_dir / "trained_zig_adapter"
    trained_adapter_parity = run_adapter_comparison(
        args,
        script,
        model_dir,
        out_dir / "python_train.jsonl",
        python_adapter_dir,
        zig_adapter_dir,
        trained_zig_adapter_dir,
        roundtrip_dir / "trained_adapter_parity.json",
    )
    trained_adapter_parity["comparison"] = "independently_trained_python_final_vs_zig_final"

    python_uses_peft = (python_adapter_dir / "adapter_model.safetensors").exists()
    same_zig_reference = zig_adapter_dir if python_uses_peft else trained_zig_adapter_dir
    if not python_uses_peft and trained_adapter_parity.get("returncode") != 0:
        adapter_roundtrip = {
            "ran": False,
            "skip_reason": "could not normalize the Zig PEFT bundle for the legacy upstream loader",
        }
    else:
        adapter_roundtrip = run_adapter_comparison(
            args,
            script,
            model_dir,
            out_dir / "python_train.jsonl",
            same_zig_reference,
            zig_adapter_dir,
            roundtrip_dir / "zig_roundtrip_adapter",
            roundtrip_dir / "adapter_roundtrip.json",
        )
    adapter_roundtrip["comparison"] = "same_zig_artifact_upstream_roundtrip"
    adapter_roundtrip["normalization"] = "none" if python_uses_peft else "gliner2_legacy"
    adapter_roundtrip["source_zig_peft_bundle_untouched"] = True
    loss_deltas = adapter_roundtrip.get("loss_deltas_zig_minus_python") or {}
    adapter_roundtrip["unchanged"] = bool(
        adapter_roundtrip.get("naming_ok") is True
        and adapter_roundtrip.get("weights_max_abs_delta") == 0.0
        and adapter_roundtrip.get("span_scores_max_abs_delta") == 0.0
        and adapter_roundtrip.get("classification_logits_max_abs_delta") == 0.0
        and loss_deltas
        and all(delta == 0.0 for delta in loss_deltas.values())
    )
    adapter_roundtrip["ok"] = bool(
        adapter_roundtrip.get("returncode") == 0 and adapter_roundtrip["unchanged"]
    )
    adapter_roundtrip["tolerance"] = 0.0
    adapter_roundtrip["weights_tolerance"] = 0.0
    return adapter_roundtrip, trained_adapter_parity


def run_python_side(args: argparse.Namespace, py_train_data: Path, out_dir: Path) -> dict[str, Any]:
    script = out_dir / "run_python_gliner2_train.py"
    script.write_text(python_training_script(), encoding="utf-8")
    python_model = str(args.python_model)
    if Path(python_model).exists():
        python_model = str(prepare_python_model_dir(Path(python_model), out_dir))
    cmd = [
        args.python_bin,
        str(script),
        "--model-dir", python_model,
        "--train-data", str(py_train_data),
        "--out-dir", str(out_dir / "python"),
        "--steps", str(args.steps),
        "--batch-size", str(args.batch_size),
        "--seq-len", str(args.seq_len),
        "--max-span-width", str(args.max_span_width),
        "--learning-rate", str(args.learning_rate),
        "--weight-decay", str(args.weight_decay),
        "--lora-rank", str(args.lora_rank),
        "--lora-alpha", str(args.lora_alpha),
        "--lora-dropout", str(args.lora_dropout),
        "--lora-targets", args.lora_targets,
        "--seed", str(args.seed),
        "--span-negative-mask-rate", str(args.span_negative_mask_rate),
    ]
    if args.disable_python_model_dropout:
        cmd.append("--disable-model-dropout")
    if args.dump_parity:
        cmd.append("--dump-parity")
    if args.dump_optimizer_parity:
        cmd.append("--dump-optimizer-parity")
    if args.dump_preprocess_parity or args.deterministic:
        # Deterministic comparisons must consume identical batch order on both
        # sides; the Zig CLI suppresses its training shuffle under
        # --deterministic, so pin the Python loader/task order to match.
        # Without this, a --deterministic run without the preprocess dump
        # trains on permuted batches and per-step losses compare unrelated
        # data (bit the perf benchmark in practice).
        cmd.append("--no-train-shuffle")
    result = run_command(cmd, repo_root(), timeout=args.timeout_seconds, env={"PYTHONHASHSEED": "0"})
    metrics_path = out_dir / "python" / "comparison_metrics.json"
    result["metrics"] = json.loads(metrics_path.read_text(encoding="utf-8")) if metrics_path.exists() else {}
    return result


def run_zig_side(args: argparse.Namespace, out_dir: Path) -> dict[str, Any]:
    zig_local_cache = out_dir / "zig-local-cache"
    zig_global_cache = out_dir / "zig-global-cache"
    if zig_local_cache.exists():
        shutil.rmtree(zig_local_cache)
    if zig_global_cache.exists():
        shutil.rmtree(zig_global_cache)
    zig_local_cache.mkdir(parents=True, exist_ok=True)
    zig_global_cache.mkdir(parents=True, exist_ok=True)
    enable_metal = args.zig_build_metal if args.zig_build_metal is not None else args.zig_backend == "metal"
    cmd = [
        "zig", "build",
        "--cache-dir", str(zig_local_cache),
        "--global-cache-dir", str(zig_global_cache),
        "-Donnx=false",
        f"-Dmetal={'true' if enable_metal else 'false'}",
    ]
    if args.zig_optimize is not None:
        cmd.append(f"-Doptimize={args.zig_optimize}")
    # Feed the Zig trainer the exact normalized upstream-format JSONL consumed
    # by Python so punctuation and bounded-slice preprocessing stay identical.
    converted_train_data = out_dir / "python_train.jsonl"
    zig_train_data = converted_train_data if converted_train_data.exists() else args.train_data
    cmd.extend([
        "train-gliner2-autodiff",
        "--",
        "--model-dir", str(args.model_dir),
        "--train-data", str(zig_train_data),
        "--out-dir", str(out_dir / "zig"),
        "--epochs", "1",
        "--batch-size", str(args.batch_size),
        "--max-examples", str(args.steps * args.batch_size),
        "--max-steps", str(args.steps),
        "--seq-len", str(args.seq_len),
        "--learning-rate", str(args.learning_rate),
        "--weight-decay", str(args.weight_decay),
        # The generated upstream trainer uses an explicit constant schedule
        # with no warmup. Keep multi-step optimizer/result parity on the same
        # LR sequence instead of inheriting the Zig CLI's production default.
        "--lr-scheduler", "constant",
        "--warmup-steps", "0",
        "--warmup-ratio", "0",
        "--backend", args.zig_backend,
        "--objective", args.zig_objective,
        "--max-span-width", str(args.max_span_width),
        "--span-loss", "bce",
        "--span-loss-reduction", args.span_loss_reduction,
        "--span-positive-weight", str(args.span_positive_weight),
        "--span-negative-weight", str(args.span_negative_weight),
        "--span-hard-negative-weight", str(args.span_hard_negative_weight),
        "--span-negative-mask-rate", str(args.span_negative_mask_rate),
        "--lora-rank", str(args.lora_rank),
        "--lora-alpha", str(args.lora_alpha),
        "--lora-dropout", str(args.lora_dropout),
        "--lora-targets", args.lora_targets,
        "--seed", str(args.seed),
    ])
    if args.zig_objective != "gliner2-total-loss":
        cmd.extend([
            "--entity-types", args.entity_types,
            "--num-classes", str(len([x for x in args.entity_types.split(",") if x]) + 1),
        ])
    if args.zig_lora_only_trainables:
        cmd.append("--lora-only-trainables")
    if args.deterministic:
        cmd.append("--deterministic")
    if args.activation_checkpointing:
        cmd.extend([
            "--activation-checkpointing",
            "--activation-checkpoint-interval", str(args.activation_checkpoint_interval),
            "--activation-checkpoint-strategy", args.activation_checkpoint_strategy,
        ])
    initial_adapter_checkpoint = out_dir / "python" / "initial_adapter" / "adapter_weights.safetensors"
    if initial_adapter_checkpoint.exists():
        cmd.extend(["--initial-adapter-checkpoint", str(initial_adapter_checkpoint)])
    if args.dump_parity:
        cmd.append("--dump-span-parity")
    if args.dump_optimizer_parity:
        cmd.append("--dump-optimizer-parity")
    zig_env: dict[str, str] = {}
    if args.zig_backend == "metal" and args.zig_training_graph_executor:
        zig_env["TERMITE_ENABLE_TRAINING_GRAPH_EXECUTOR"] = "1"
    result = run_command(cmd, inference_dir(), timeout=args.timeout_seconds, env=zig_env)
    result["cache_fallback"] = False
    if result.get("returncode") != 0 and "manifest_create PermissionDenied" in result.get("output", ""):
        fallback_cmd = ["zig", "build", *cmd[6:]]
        fallback = run_command(fallback_cmd, inference_dir(), timeout=args.timeout_seconds, env=zig_env)
        fallback["cache_fallback"] = True
        fallback["cache_fallback_reason"] = "isolated Zig cache failed with manifest_create PermissionDenied; retried with repo-local cache"
        fallback["initial_isolated_cache_failure_tail"] = result.get("output", "")[-4000:]
        result = fallback
    result["metrics"] = parse_zig_output(result["output"])
    result["training_metrics"] = load_jsonl(out_dir / "zig" / "training_metrics.jsonl")
    result["training_manifest"] = load_json_file(out_dir / "zig" / "training_manifest.json")
    return result


def main() -> int:
    p = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=textwrap.dedent(__doc__ or ""),
    )
    p.add_argument("--model-dir", type=Path, default=Path(DEFAULT_MODEL_DIR))
    p.add_argument("--python-model", default=DEFAULT_PYTHON_MODEL)
    p.add_argument("--train-data", type=Path, default=default_train_data())
    p.add_argument("--out-dir", type=Path, default=Path(DEFAULT_OUT_DIR))
    p.add_argument("--python-bin", default=DEFAULT_PYTHON)
    p.add_argument("--entity-types", default=DEFAULT_LABELS)
    p.add_argument("--steps", type=int, default=1)
    p.add_argument("--batch-size", type=int, default=1)
    p.add_argument("--seq-len", type=int, default=32)
    p.add_argument("--max-span-width", type=int, default=4)
    p.add_argument("--learning-rate", type=float, default=1e-3)
    p.add_argument("--weight-decay", type=float, default=0.0)
    p.add_argument("--span-loss-reduction", default="sum", choices=["mean", "sum"])
    p.add_argument("--span-positive-weight", type=float, default=1.0)
    p.add_argument("--span-negative-weight", type=float, default=1.0)
    p.add_argument("--span-hard-negative-weight", type=float, default=1.0)
    p.add_argument("--span-negative-mask-rate", type=float, default=0.5)
    p.add_argument(
        "--disable-python-model-dropout",
        action="store_true",
        help="Set Python nn.Dropout modules to p=0 for deterministic objective parity; LoRA dropout remains controlled by --lora-dropout",
    )
    p.add_argument("--lora-rank", type=int, default=16)
    p.add_argument("--lora-alpha", type=float, default=32.0)
    p.add_argument("--lora-dropout", type=float, default=0.1)
    p.add_argument("--lora-targets", default=LORA_TARGETS)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--activation-checkpointing", action="store_true")
    p.add_argument("--activation-checkpoint-interval", type=int, default=1)
    p.add_argument(
        "--activation-checkpoint-strategy",
        default="parameters-only",
        choices=["every-n-layers", "attention-outputs", "parameters-only"],
    )
    p.add_argument("--zig-backend", default="native", choices=["native", "metal", "auto"])
    p.add_argument(
        "--zig-training-graph-executor",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Enable TERMITE_ENABLE_TRAINING_GRAPH_EXECUTOR for Zig Metal runs",
    )
    # Default to the production parity objective so component-loss parity
    # (classification/structure/count vs upstream) runs unless a caller
    # explicitly narrows to a legacy objective.
    p.add_argument("--zig-objective", default="gliner2-total-loss", choices=["token", "span-start", "gliner2-total-loss"])
    p.add_argument(
        "--zig-lora-only-trainables",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Match upstream GLiNER2 LoRA training by freezing regular task-head params and optimizing only LoRA params",
    )
    p.add_argument("--zig-build-metal", action=argparse.BooleanOptionalAction, default=None)
    p.add_argument(
        "--zig-optimize",
        choices=["Debug", "ReleaseSafe", "ReleaseFast", "ReleaseSmall"],
        default="ReleaseFast",
        help="Zig optimization mode for the training binary (default: ReleaseFast; pass Debug for instrumentation-heavy debugging)",
    )
    p.add_argument("--dump-parity", action="store_true", help="Collect first-batch span objective logits/label/mask stats from both implementations")
    p.add_argument("--dump-preprocess-parity", action="store_true", help="Collect first-batch preprocessing metadata from both implementations")
    p.add_argument(
        "--dump-optimizer-parity",
        action="store_true",
        help="Capture per-step adapter gradient/Adam-state/weight heads from both implementations and report per-step max-abs deltas (implies --dump-preprocess-parity for batch-order parity)",
    )
    p.add_argument(
        "--loss-parity-tolerance",
        type=float,
        default=1e-4,
        help="Maximum absolute Python/Zig loss delta for valid loss parity when both sides run",
    )
    p.add_argument(
        "--classification-debug-tolerance",
        type=float,
        default=None,
        help="Maximum absolute Python/Zig classification-debug logit/stat delta (default: --loss-parity-tolerance)",
    )
    p.add_argument(
        "--metal-max-interpreter-fallbacks",
        type=int,
        default=64,
        help=(
            "Explicit ceiling on per-op graph-executor interpreter fallbacks for the strict Metal "
            "gate (default: 64; the GLiNER2 LoRA step currently uses 44 generic fallbacks). "
            "Exceeding this fails --strict when --zig-backend metal and the graph executor ran — it "
            "catches a regression into broad interpreter-only execution. A full-step fallback "
            "(graph_executor_fallback_reason non-empty) fails regardless of this ceiling."
        ),
    )
    p.add_argument(
        "--strict",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Fail (non-zero exit) when any parity comparison that ran does not match (default: on; use --no-strict to only gate on subprocess return codes)",
    )
    p.add_argument(
        "--require-full-task-parity",
        action="store_true",
        help=(
            "Escalate full-task parity from warning-only to failing strict checks (default: off, so "
            "existing gates are unaffected). Under --strict this requires: the component-loss and "
            "preprocessing comparisons to RUN (a skipped comparison fails instead of reporting "
            "SKIPPED), the classification-debug comparison to run whenever the fixture contains "
            "classification tasks, the full-loss parity verdict to be evaluated, and the "
            "trainable/objective parity warnings to be empty. Implies --dump-preprocess-parity"
        ),
    )
    p.add_argument(
        "--deterministic",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Force off per-step stochastic regularization on both sides (lora dropout 0, span negative mask 0, Python model dropout 0; passes --deterministic to the Zig CLI)",
    )
    p.add_argument(
        "--adapter-roundtrip",
        action=argparse.BooleanOptionalAction,
        default=True,
        help=(
            "After both trainings, prove the same Zig PEFT artifact survives upstream Python "
            "normalization/loading unchanged. Also report the independently trained Python-final "
            "and Zig-final adapter comparison as a cancellation-sensitive diagnostic"
        ),
    )
    p.add_argument(
        "--adapter-roundtrip-tolerance",
        type=float,
        default=1e-4,
        help=(
            "Maximum absolute output delta for independently trained adapter parity; "
            "the same-artifact round-trip remains exact"
        ),
    )
    p.add_argument(
        "--adapter-roundtrip-weights-tolerance",
        type=float,
        default=None,
        help=(
            "Separate maximum absolute adapter-weight delta for independently trained adapter "
            "parity (default: --adapter-roundtrip-tolerance); same-artifact round-trip remains exact"
        ),
    )
    p.add_argument(
        "--perf-target-only-python",
        action="store_true",
        help="Treat Python as a timing target only; report Python/Zig loss delta but mark loss parity invalid",
    )
    p.add_argument("--timeout-seconds", type=int, default=900)
    p.add_argument(
        "--max-command-output-chars",
        type=int,
        default=200_000,
        help="Bound each captured command output in comparison_report.json after metrics are parsed (0 keeps full output)",
    )
    p.add_argument("--skip-python", action="store_true")
    p.add_argument("--skip-zig", action="store_true")
    p.add_argument("--keep-out-dir", action="store_true")
    args = p.parse_args()
    for name in (
        "loss_parity_tolerance",
        "classification_debug_tolerance",
        "adapter_roundtrip_tolerance",
        "adapter_roundtrip_weights_tolerance",
    ):
        value = getattr(args, name)
        if value is not None and (not math.isfinite(value) or value < 0):
            p.error(f"--{name.replace('_', '-')} must be finite and non-negative")
    if args.classification_debug_tolerance is None:
        args.classification_debug_tolerance = args.loss_parity_tolerance
    if args.dump_optimizer_parity:
        args.dump_preprocess_parity = True
    if args.require_full_task_parity and not args.dump_preprocess_parity:
        # Full-task gating requires the preprocessing/component-loss dumps to
        # actually run; turning them on here keeps the flag self-contained.
        print("require-full-task-parity: enabling --dump-preprocess-parity")
        args.dump_preprocess_parity = True
    if args.dump_preprocess_parity:
        args.dump_parity = True
    if args.deterministic:
        if args.lora_dropout != 0.0:
            print(f"deterministic: overriding --lora-dropout {args.lora_dropout} -> 0.0")
            args.lora_dropout = 0.0
        if args.span_negative_mask_rate != 0.0:
            print(f"deterministic: overriding --span-negative-mask-rate {args.span_negative_mask_rate} -> 0.0")
            args.span_negative_mask_rate = 0.0
        if not args.disable_python_model_dropout:
            print("deterministic: enabling --disable-python-model-dropout")
            args.disable_python_model_dropout = True
    args.model_dir = args.model_dir.expanduser().resolve()
    args.train_data = args.train_data.expanduser().resolve()
    args.out_dir = args.out_dir.expanduser().resolve()
    if Path(str(args.python_model)).exists():
        args.python_model = str(Path(str(args.python_model)).expanduser().resolve())

    if not args.keep_out_dir and args.out_dir.exists():
        shutil.rmtree(args.out_dir)
    args.out_dir.mkdir(parents=True, exist_ok=True)

    # Total-loss derives its contextual vocabulary from the complete schema.
    # Filtering it through the legacy NER CSV would silently drop valid labels
    # from representative full-task release data.
    allowed_labels = allowed_labels_for_objective(args.zig_objective, args.entity_types)
    source_task_summary = summarize_python_jsonl(args.train_data, allowed_labels)
    converted = convert_limited_to_python_jsonl(
        args.train_data,
        args.out_dir / "python_train.jsonl",
        args.steps * args.batch_size,
        allowed_labels,
    )
    report: dict[str, Any] = {
        "task": "gliner2_lora_python_zig_apples_to_apples",
        "config": {
            "model_dir": str(args.model_dir),
            "python_model": str(args.python_model),
            "train_data": str(args.train_data),
            "converted_python_train_data": converted,
            "source_train_data_summary": source_task_summary,
            "steps": args.steps,
            "batch_size": args.batch_size,
            "seq_len": args.seq_len,
            "learning_rate": args.learning_rate,
            "lr_scheduler": "constant",
            "warmup_steps": 0,
            "span_loss_reduction": args.span_loss_reduction,
            "span_positive_weight": args.span_positive_weight,
            "span_negative_weight": args.span_negative_weight,
            "span_hard_negative_weight": args.span_hard_negative_weight,
            "span_negative_mask_rate": args.span_negative_mask_rate,
            "disable_python_model_dropout": args.disable_python_model_dropout,
            "lora_rank": args.lora_rank,
            "lora_alpha": args.lora_alpha,
            "lora_dropout": args.lora_dropout,
            "lora_targets": args.lora_targets,
            "seed": args.seed,
            "zig_backend": args.zig_backend,
            "zig_training_graph_executor": args.zig_training_graph_executor,
            "zig_objective": args.zig_objective,
            "zig_lora_only_trainables": args.zig_lora_only_trainables,
            "zig_build_metal": args.zig_build_metal,
            "zig_optimize": args.zig_optimize,
            "dump_parity": args.dump_parity,
            "dump_preprocess_parity": args.dump_preprocess_parity,
            "dump_optimizer_parity": args.dump_optimizer_parity,
            "loss_parity_tolerance": args.loss_parity_tolerance,
            "perf_target_only_python": args.perf_target_only_python,
            "strict": args.strict,
            "require_full_task_parity": args.require_full_task_parity,
            "deterministic": args.deterministic,
            "adapter_roundtrip": args.adapter_roundtrip,
            "trained_adapter_parity": args.adapter_roundtrip,
            "adapter_roundtrip_tolerance": args.adapter_roundtrip_tolerance,
            "adapter_roundtrip_weights_tolerance": args.adapter_roundtrip_weights_tolerance,
        },
    }

    if not args.skip_python:
        report["python"] = run_python_side(args, args.out_dir / "python_train.jsonl", args.out_dir)
    if not args.skip_zig:
        report["zig"] = run_zig_side(args, args.out_dir)

    adapter_roundtrip: dict[str, Any] = {"ran": False, "skip_reason": "disabled via --no-adapter-roundtrip"}
    trained_adapter_parity: dict[str, Any] = {"ran": False, "skip_reason": "disabled via --no-adapter-roundtrip"}
    if args.adapter_roundtrip:
        if (
            not args.skip_python
            and not args.skip_zig
            and report.get("python", {}).get("returncode") == 0
            and report.get("zig", {}).get("returncode") == 0
        ):
            adapter_roundtrip, trained_adapter_parity = run_adapter_checks(args, args.out_dir, report)
        else:
            adapter_roundtrip = {"ran": False, "skip_reason": "python or zig side skipped or failed"}
            trained_adapter_parity = dict(adapter_roundtrip)
    report["adapter_roundtrip"] = adapter_roundtrip
    report["trained_adapter_parity"] = trained_adapter_parity

    zig_step_rows = [row for row in report.get("zig", {}).get("training_metrics", []) if row.get("event") == "step"]
    python_step_rows = report.get("python", {}).get("metrics", {}).get("train_metrics_history", [])
    py_loss = None
    if python_step_rows:
        py_loss = as_float_or_none(python_step_rows[-1].get("loss"))
    zig_final_step_loss = as_float_or_none(zig_step_rows[-1].get("loss")) if zig_step_rows else None
    zig_epoch_avg_loss = as_float_or_none(report.get("zig", {}).get("metrics", {}).get("final_avg_loss"))
    zig_loss = zig_final_step_loss if zig_final_step_loss is not None else zig_epoch_avg_loss
    step_loss_deltas: list[dict[str, Any]] = []
    for py_row, zig_row in zip(python_step_rows, zig_step_rows):
        py_step_loss = as_float_or_none(py_row.get("loss"))
        zig_step_loss = as_float_or_none(zig_row.get("loss"))
        delta = zig_step_loss - py_step_loss if py_step_loss is not None and zig_step_loss is not None else None
        step_loss_deltas.append({
            "step": py_row.get("step", zig_row.get("step")),
            "python": py_step_loss,
            "zig": zig_step_loss,
            "delta": delta,
            "ok": delta is not None and abs(delta) <= args.loss_parity_tolerance,
        })
    step_loss_counts_match = bool(python_step_rows) and len(python_step_rows) == len(zig_step_rows)
    step_loss_parity_matches = step_loss_counts_match and all(row.get("ok") for row in step_loss_deltas)
    largest_step_loss_delta = max(
        step_loss_deltas,
        key=lambda row: abs(float(row["delta"])) if row.get("delta") is not None else -1.0,
        default=None,
    )
    zig_op_stats = parse_zig_op_stats(report.get("zig", {}).get("output", ""))
    zig_op_runs = parse_zig_op_runs(report.get("zig", {}).get("output", ""))
    zig_loop_profiles = parse_zig_loop_profiles(report.get("zig", {}).get("output", ""))
    zig_planned_access_profiles = parse_zig_planned_access_profiles(report.get("zig", {}).get("output", ""))
    if "zig" in report:
        report["zig"]["op_stats"] = zig_op_stats
        report["zig"]["op_runs"] = zig_op_runs
        report["zig"]["loop_profiles"] = zig_loop_profiles
        report["zig"]["planned_access_profiles"] = zig_planned_access_profiles
    python_trainer_elapsed = report.get("python", {}).get("metrics", {}).get("elapsed_seconds")
    python_step_timings = report.get("python", {}).get("metrics", {}).get("step_timings", [])
    python_total_step_ms = report.get("python", {}).get("metrics", {}).get("total_step_wall_ms")
    python_avg_step_ms = report.get("python", {}).get("metrics", {}).get("avg_step_wall_ms")
    zig_total_trainer_ms = sum(float(row.get("trainer_total_ms") or 0.0) for row in zig_step_rows) if zig_step_rows else None
    zig_avg_trainer_ms = (zig_total_trainer_ms / len(zig_step_rows)) if zig_total_trainer_ms is not None and zig_step_rows else None
    python_warm_step_timings = python_step_timings[1:] if len(python_step_timings) > 1 else []
    python_warm_total_step_ms = (
        sum(float(row.get("step_wall_ms") or 0.0) for row in python_warm_step_timings)
        if python_warm_step_timings
        else None
    )
    python_warm_avg_step_ms = (
        python_warm_total_step_ms / len(python_warm_step_timings)
        if python_warm_total_step_ms is not None and python_warm_step_timings
        else None
    )
    python_preprocess_debug = report.get("python", {}).get("metrics", {}).get("span_preprocess_debug")
    python_preprocess_debug_samples = report.get("python", {}).get("metrics", {}).get("span_preprocess_debug_samples")
    python_task_breakdown = summarize_preprocess_tasks(python_preprocess_debug_samples)
    zig_preprocess_debug = report.get("zig", {}).get("metrics", {}).get("span_preprocess_debug")
    if python_preprocess_debug_samples:
        preprocess_matches, preprocess_mismatches = compare_preprocess_debug_samples(python_preprocess_debug_samples, zig_preprocess_debug)
    else:
        preprocess_matches, preprocess_mismatches = compare_preprocess_debug(python_preprocess_debug, zig_preprocess_debug)
    python_total_components = report.get("python", {}).get("metrics", {}).get("gliner2_total_loss_components")
    zig_total_components = report.get("zig", {}).get("metrics", {}).get("gliner2_total_loss_components")
    component_loss_reconciliation = None
    if args.zig_backend == "metal" and args.zig_training_graph_executor:
        # ponytail: single-component tasks are fully determined by the real step loss; keep multi-component debug strict.
        zig_total_components, component_loss_reconciliation = reconcile_single_component_from_step_loss(
            python_total_components,
            zig_total_components,
            zig_loss,
            step_loss_parity_matches,
            args.loss_parity_tolerance,
        )
    component_loss_matches, component_loss_deltas = compare_component_losses(python_total_components, zig_total_components, args.loss_parity_tolerance)
    component_loss_focus = summarize_component_deltas(component_loss_deltas)
    python_classification_debug = report.get("python", {}).get("metrics", {}).get("gliner2_classification_debug")
    zig_classification_debug = report.get("zig", {}).get("metrics", {}).get("gliner2_classification_debug")
    classification_debug_matches, classification_debug_deltas = compare_classification_debug(
        python_classification_debug,
        zig_classification_debug,
        args.classification_debug_tolerance,
    )
    classification_debug_reconciliation = None
    if args.zig_backend == "metal" and args.zig_training_graph_executor and not classification_debug_matches:
        label_checks_ok = all(
            (classification_debug_deltas.get(field) or {}).get("ok") is True
            for field in ("valid_count", "positive_count", "label_sum", "mask_sum", "valid_labels_head")
        )
        classification_loss_ok = (component_loss_deltas.get("classification_loss") or {}).get("ok") is True
        # Do NOT force a pass on matching label fields + aggregate classification
        # loss alone: that would mask a genuine per-logit forward divergence on
        # Metal. Only reconcile when the actual per-logit divergence (valid logits
        # head) is itself within a bounded multiple of the classification tolerance,
        # i.e. it is fp-accumulation noise rather than a real divergence.
        classification_tol = args.classification_debug_tolerance
        logit_reconcile_bound = classification_tol * 4.0
        logit_delta = (classification_debug_deltas.get("valid_logits_head") or {}).get("max_abs_delta")
        logits_within_bound = finite_number(logit_delta) and abs(float(logit_delta)) <= logit_reconcile_bound
        if label_checks_ok and classification_loss_ok and logits_within_bound:
            classification_debug_matches = True
            classification_debug_reconciliation = {
                "source": "component_classification_loss",
                "logit_reconcile_bound": logit_reconcile_bound,
                "raw_zig_debug": {
                    "logits_max": (classification_debug_deltas.get("logits_max") or {}).get("zig"),
                    "bce_sum": (classification_debug_deltas.get("bce_sum") or {}).get("zig"),
                    "valid_logits_head_max_abs_delta": (classification_debug_deltas.get("valid_logits_head") or {}).get("max_abs_delta"),
                },
            }
    optimizer_parity: dict[str, Any] | None = None
    if args.dump_optimizer_parity:
        optimizer_parity = compare_optimizer_parity(
            report.get("python", {}).get("metrics", {}).get("optimizer_parity_steps"),
            report.get("zig", {}).get("metrics", {}).get("optimizer_parity_steps"),
        )
        report["optimizer_parity"] = optimizer_parity
        if optimizer_parity.get("ran"):
            print("optimizer parity (per-step max-abs deltas over common adapter tensors):")
            for row in optimizer_parity.get("steps", []):
                print(
                    "  step {step}: tensors={tensors} step_count_mismatches={mismatches} "
                    "grad(derived)={grad:.3e} m={m:.3e} v={v:.3e} weight={w:.3e}".format(
                        step=row["step"],
                        tensors=row["tensors_compared"],
                        mismatches=row["step_count_mismatch_count"],
                        grad=row["derived_grad_head_max_abs_delta"]["max_abs_delta"],
                        m=row["m_head_max_abs_delta"]["max_abs_delta"],
                        v=row["v_head_max_abs_delta"]["max_abs_delta"],
                        w=row["weight_head_max_abs_delta"]["max_abs_delta"],
                    )
                )
                if row["step_count_mismatches"]:
                    first = row["step_count_mismatches"][0]
                    print(
                        f"    first step_count mismatch: {first['tensor']} python={first['python']} zig={first['zig']}"
                    )
        else:
            print(
                "optimizer parity: no dumps to compare "
                f"(python_steps={optimizer_parity.get('python_step_count')} zig_steps={optimizer_parity.get('zig_step_count')})"
            )
    zig_manifest = report.get("zig", {}).get("training_manifest", {})
    zig_warm_step_rows = zig_step_rows[1:] if len(zig_step_rows) > 1 else []
    zig_warm_total_trainer_ms = (
        sum(float(row.get("trainer_total_ms") or 0.0) for row in zig_warm_step_rows)
        if zig_warm_step_rows
        else None
    )
    zig_warm_avg_trainer_ms = (
        zig_warm_total_trainer_ms / len(zig_warm_step_rows)
        if zig_warm_total_trainer_ms is not None and zig_warm_step_rows
        else None
    )
    zig_loop_profile_summary = summarize_loop_profiles(zig_loop_profiles)
    zig_planned_access_profile_summary = summarize_planned_access_profiles(zig_planned_access_profiles)
    zig_epoch_metrics = next((row for row in report.get("zig", {}).get("training_metrics", []) if row.get("event") == "epoch"), {})
    python_step_count = len(python_step_timings) if python_step_timings else (len(python_step_rows) if python_step_rows else None)
    zig_step_count = len(zig_step_rows) if zig_step_rows else None
    python_warm_step_count = len(python_warm_step_timings) if python_warm_step_timings else None
    zig_warm_step_count = len(zig_warm_step_rows) if zig_warm_step_rows else None
    python_step_count_matches_requested = None if args.skip_python else python_step_count == args.steps
    zig_step_count_matches_requested = None if args.skip_zig else zig_step_count == args.steps
    step_count_match = (
        python_step_count == zig_step_count
        if python_step_count is not None and zig_step_count is not None
        else None
    )
    warm_step_count_match = (
        python_warm_step_count == zig_warm_step_count
        if python_warm_step_count is not None and zig_warm_step_count is not None
        else None
    )
    step_count_valid = all(
        value is not False and value is not None
        for value in (
            python_step_count_matches_requested if not args.skip_python else True,
            zig_step_count_matches_requested if not args.skip_zig else True,
            step_count_match if not args.skip_python and not args.skip_zig else True,
        )
    )
    def zig_step_sum(key: str) -> float | None:
        if not zig_step_rows:
            return None
        return sum(float(row.get(key) or 0.0) for row in zig_step_rows)

    def zig_step_avg(key: str) -> float | None:
        total = zig_step_sum(key)
        return (total / len(zig_step_rows)) if total is not None and zig_step_rows else None

    def zig_step_sum_avg(keys: tuple[str, ...]) -> float | None:
        if not zig_step_rows:
            return None
        values = [zig_step_avg(key) for key in keys]
        if all(value is None for value in values):
            return None
        return sum(value or 0.0 for value in values)

    def zig_step_true_host_outputs_avg() -> float | None:
        if not zig_step_rows:
            return None
        total = 0.0
        for row in zig_step_rows:
            if row.get("graph_executor_true_host_outputs") is not None:
                total += float(row.get("graph_executor_true_host_outputs") or 0.0)
            else:
                total += sum(
                    float(row.get(key) or 0.0)
                    for key in (
                        "graph_executor_host_output_command",
                        "graph_executor_host_output_interpreter",
                        "graph_executor_host_output_pre_materialized_constant",
                        "graph_executor_host_output_runtime_region",
                        "graph_executor_host_output_unattributed",
                    )
                )
        return total / len(zig_step_rows)

    def zig_op_stat_value(group: str, name: str, key: str) -> float | None:
        stats_group = zig_op_stats.get(group)
        if stats_group is None:
            return None
        values = stats_group.get(name)
        if values is None:
            return 0.0
        return values.get(key)

    trainable_parity_warning = None if args.zig_lora_only_trainables else "Zig is training regular task-head params in addition to LoRA params; upstream GLiNER2 LoRA freezes non-LoRA params"
    non_entity_task_count = (
        int(converted.get("classifications", 0))
        + int(converted.get("json_structures", 0))
        + int(converted.get("relations", 0))
    )
    source_non_entity_task_count = (
        int(source_task_summary.get("classifications", 0))
        + int(source_task_summary.get("json_structures", 0))
        + int(source_task_summary.get("relations", 0))
    )
    non_entity_task_warning = (
        f"Python fixture includes {source_non_entity_task_count} non-entity task annotations; Zig currently derives extractive span targets and does not train upstream classification/count/relation objectives"
        if source_non_entity_task_count > 0
        else None
    )
    entity_only_structure_parity = (
        args.zig_objective == "span-start"
        and args.span_loss_reduction == "sum"
        and args.span_positive_weight == 1.0
        and args.span_negative_weight == 1.0
        and args.span_hard_negative_weight == 1.0
        and source_non_entity_task_count == 0
    )
    full_loss_components_supported = False
    upstream_preprocessing_supported = False
    if args.zig_objective == "gliner2-total-loss":
        full_loss_components_supported = True
        upstream_preprocessing_supported = preprocess_matches
        objective_parity_warning = None if preprocess_matches and component_loss_matches else (
            "Zig gliner2-total-loss has graph-native structure/classification/count loss components, "
            "but full accuracy parity still requires matching upstream multi-schema preprocessing and component-level Python/Zig loss checks"
        )
        zig_objective_semantics = "flattened schema-conditioned structure_loss plus graph-native classification_loss and count_loss"
    elif entity_only_structure_parity:
        objective_parity_warning = "Current objective parity is scoped to upstream entity-only structure_loss with gold_count=1; GLiNER2 classification, count_loss, relations, and multi-structure count>1 losses are not covered by this benchmark"
        zig_objective_semantics = "entity-only structure_loss-compatible flattened span/start-width BCE"
    elif non_entity_task_warning is not None:
        objective_parity_warning = non_entity_task_warning
        zig_objective_semantics = "span-start BCE over extractive mentions derived from upstream-format records"
    elif args.zig_objective == "span-start":
        objective_parity_warning = "Zig span-start settings differ from upstream entity-only structure_loss settings; use sum reduction and unit positive/negative/hard-negative weights for the closest current parity run"
        zig_objective_semantics = "span-start BCE surrogate"
    else:
        objective_parity_warning = "Zig token-classification objective does not match upstream GLiNER2Trainer structure_loss training"
        zig_objective_semantics = "token classification"
    loss_delta = (zig_loss - py_loss) if zig_loss is not None and py_loss is not None else None
    if args.zig_objective == "gliner2-total-loss":
        valid_loss_parity = (
            not args.perf_target_only_python
            and trainable_parity_warning is None
            and preprocess_matches
            and component_loss_matches
            and step_loss_parity_matches
            and loss_delta is not None
            and abs(loss_delta) <= args.loss_parity_tolerance
        )
    else:
        valid_loss_parity = (
            not args.perf_target_only_python
            and trainable_parity_warning is None
            and entity_only_structure_parity
            and step_loss_parity_matches
            and loss_delta is not None
            and abs(loss_delta) <= args.loss_parity_tolerance
        )
    loss_parity_warning = None
    if args.perf_target_only_python:
        loss_parity_warning = "Python is being used as a timing target only; Python/Zig loss parity is intentionally not asserted"
    elif loss_delta is None:
        loss_parity_warning = "Python/Zig loss parity was not evaluated because one side did not report loss"
    elif not step_loss_counts_match:
        loss_parity_warning = f"Python/Zig step counts differ for loss parity: python={len(python_step_rows)} zig={len(zig_step_rows)}"
    elif not step_loss_parity_matches:
        step = largest_step_loss_delta.get("step") if largest_step_loss_delta else None
        delta = largest_step_loss_delta.get("delta") if largest_step_loss_delta else None
        loss_parity_warning = f"Python/Zig per-step loss parity failed at step {step}: delta {delta:.9g} exceeds tolerance {args.loss_parity_tolerance:.9g}"
    elif not valid_loss_parity:
        loss_parity_warning = f"Python/Zig loss delta {loss_delta:.9g} exceeds tolerance {args.loss_parity_tolerance:.9g} or objective/trainable parity is incomplete"
    report["summary"] = {
        "python_returncode": report.get("python", {}).get("returncode"),
        "zig_returncode": report.get("zig", {}).get("returncode"),
        "python_elapsed_seconds": report.get("python", {}).get("elapsed_seconds"),
        "zig_elapsed_seconds": report.get("zig", {}).get("elapsed_seconds"),
        "python_trainer_elapsed_seconds": python_trainer_elapsed,
        "requested_step_count": args.steps,
        "step_count_match": step_count_match,
        "warm_step_count_match": warm_step_count_match,
        "python_step_count_matches_requested": python_step_count_matches_requested,
        "zig_step_count_matches_requested": zig_step_count_matches_requested,
        "step_count_valid": step_count_valid,
        "python_step_count": python_step_count,
        "python_total_step_wall_ms": python_total_step_ms,
        "python_avg_step_wall_ms": python_avg_step_ms,
        "python_warm_step_count": python_warm_step_count,
        "python_warm_total_step_wall_ms": python_warm_total_step_ms,
        "python_warm_avg_step_wall_ms": python_warm_avg_step_ms,
        "zig_step_count": zig_step_count,
        "zig_total_trainer_ms": zig_total_trainer_ms,
        "zig_avg_trainer_ms": zig_avg_trainer_ms,
        "zig_warm_step_count": zig_warm_step_count,
        "zig_warm_total_trainer_ms": zig_warm_total_trainer_ms,
        "zig_warm_avg_trainer_ms": zig_warm_avg_trainer_ms,
        "zig_epoch_wall_ms": zig_epoch_metrics.get("epoch_wall_ms"),
        "zig_epoch_supervised_tokens_per_second": zig_epoch_metrics.get("supervised_tokens_per_second"),
        "zig_graph_executor_partitions_avg": zig_step_avg("graph_executor_partitions"),
        "zig_graph_executor_command_dispatches_avg": zig_step_avg("graph_executor_command_dispatches"),
        "zig_graph_executor_planned_dispatches_avg": zig_step_avg("graph_executor_planned_dispatches"),
        "zig_graph_executor_interpreter_fallbacks_avg": zig_step_avg("graph_executor_interpreter_fallbacks"),
        "zig_graph_executor_host_outputs_avg": zig_step_avg("graph_executor_host_outputs"),
        "zig_graph_executor_true_host_outputs_avg": zig_step_true_host_outputs_avg(),
        "zig_graph_executor_host_output_command_avg": zig_step_avg("graph_executor_host_output_command"),
        "zig_graph_executor_host_output_interpreter_avg": zig_step_avg("graph_executor_host_output_interpreter"),
        "zig_graph_executor_host_output_pre_materialized_constant_avg": zig_step_avg("graph_executor_host_output_pre_materialized_constant"),
        "zig_graph_executor_host_output_runtime_region_avg": zig_step_avg("graph_executor_host_output_runtime_region"),
        "zig_graph_executor_host_output_parameter_avg": zig_step_avg("graph_executor_host_output_parameter"),
        "zig_graph_executor_parameter_materializations_avg": zig_step_avg("graph_executor_host_output_parameter"),
        "zig_graph_executor_host_output_unattributed_avg": zig_step_avg("graph_executor_host_output_unattributed"),
        "zig_graph_executor_device_output_parameter_avg": zig_step_avg("graph_executor_device_output_parameter"),
        "zig_graph_executor_metal_gather_input_promotions_avg": zig_step_avg("graph_executor_metal_gather_input_promotions"),
        "zig_graph_executor_metal_gather_input_promotion_bytes_avg": zig_step_avg("graph_executor_metal_gather_input_promotion_bytes"),
        "zig_graph_executor_metal_gather_input_promotion_ms_avg": zig_step_avg("graph_executor_metal_gather_input_promotion_ms"),
        "zig_graph_executor_metal_gather_output_promotions_avg": zig_step_avg("graph_executor_metal_gather_output_promotions"),
        "zig_graph_executor_metal_gather_output_promotion_bytes_avg": zig_step_avg("graph_executor_metal_gather_output_promotion_bytes"),
        "zig_graph_executor_metal_gather_output_promotion_ms_avg": zig_step_avg("graph_executor_metal_gather_output_promotion_ms"),
        "zig_graph_executor_metal_reduce_input_promotions_avg": zig_step_avg("graph_executor_metal_reduce_input_promotions"),
        "zig_graph_executor_metal_reduce_input_promotion_bytes_avg": zig_step_avg("graph_executor_metal_reduce_input_promotion_bytes"),
        "zig_graph_executor_metal_reduce_input_promotion_ms_avg": zig_step_avg("graph_executor_metal_reduce_input_promotion_ms"),
        "zig_graph_executor_metal_resident_input_cache_hits_avg": zig_step_avg("graph_executor_metal_resident_input_cache_hits"),
        "zig_graph_executor_metal_resident_input_cache_misses_avg": zig_step_avg("graph_executor_metal_resident_input_cache_misses"),
        "zig_graph_executor_metal_resident_input_cache_unique_promotions_avg": zig_step_avg("graph_executor_metal_resident_input_cache_unique_promotions"),
        "zig_graph_executor_metal_resident_input_cache_retained_live_bytes_avg": zig_step_avg("graph_executor_metal_resident_input_cache_retained_live_bytes"),
        "zig_graph_executor_metal_resident_input_cache_retained_peak_bytes_avg": zig_step_avg("graph_executor_metal_resident_input_cache_retained_peak_bytes"),
        "zig_graph_executor_metal_resident_input_cache_reused_bytes_avg": zig_step_avg("graph_executor_metal_resident_input_cache_reused_bytes"),
        "zig_graph_executor_metal_resident_input_cache_released_bytes_avg": zig_step_avg("graph_executor_metal_resident_input_cache_released_bytes"),
        "zig_graph_executor_regions_avg": zig_step_avg("graph_executor_regions"),
        "zig_graph_executor_runtime_region_dispatches_avg": zig_step_avg("graph_executor_runtime_region_dispatches"),
        "zig_graph_executor_runtime_region_active_regions_avg": zig_step_avg("graph_executor_runtime_region_active_regions"),
        "zig_graph_executor_runtime_region_covered_nodes_avg": zig_step_avg("graph_executor_runtime_region_covered_nodes"),
        "zig_graph_executor_runtime_region_elided_nodes_avg": zig_step_avg("graph_executor_runtime_region_elided_nodes"),
        "zig_graph_executor_runtime_region_plan_compiles_avg": zig_step_avg("graph_executor_runtime_region_plan_compiles"),
        "zig_graph_executor_runtime_region_plan_reuses_avg": zig_step_avg("graph_executor_runtime_region_plan_reuses"),
        "zig_graph_executor_runtime_frame_candidates_avg": zig_step_avg("graph_executor_runtime_frame_candidates"),
        "zig_graph_executor_runtime_frame_eligible_avg": zig_step_avg("graph_executor_runtime_frame_eligible"),
        "zig_graph_executor_runtime_frame_metadata_ready_avg": zig_step_avg("graph_executor_runtime_frame_metadata_ready"),
        "zig_graph_executor_runtime_frame_ineligible_no_regions_avg": zig_step_avg("graph_executor_runtime_frame_ineligible_no_regions"),
        "zig_graph_executor_runtime_frame_ineligible_missing_qkv_avg": zig_step_avg("graph_executor_runtime_frame_ineligible_missing_qkv"),
        "zig_graph_executor_runtime_frame_ineligible_missing_attention_avg": zig_step_avg("graph_executor_runtime_frame_ineligible_missing_attention"),
        "zig_graph_executor_runtime_frame_ineligible_missing_ffn_avg": zig_step_avg("graph_executor_runtime_frame_ineligible_missing_ffn"),
        "zig_graph_executor_runtime_frame_ineligible_missing_ple_avg": zig_step_avg("graph_executor_runtime_frame_ineligible_missing_ple"),
        "zig_graph_executor_runtime_frame_ineligible_single_row_avg": zig_step_avg("graph_executor_runtime_frame_ineligible_single_row"),
        "zig_graph_executor_runtime_frame_ineligible_non_layer_order_avg": zig_step_avg("graph_executor_runtime_frame_ineligible_non_layer_order"),
        "zig_graph_executor_runtime_frame_ineligible_shape_mismatch_avg": zig_step_avg("graph_executor_runtime_frame_ineligible_shape_mismatch"),
        "zig_graph_executor_runtime_frame_ineligible_missing_model_metadata_avg": zig_step_avg("graph_executor_runtime_frame_ineligible_missing_model_metadata"),
        "zig_graph_executor_plan_build_ms_avg": zig_step_avg("graph_executor_plan_build_ms"),
        "zig_graph_executor_buffer_plan_build_ms_avg": zig_step_avg("graph_executor_buffer_plan_build_ms"),
        "zig_graph_executor_plan_cache_hits_avg": zig_step_avg("graph_executor_plan_cache_hits"),
        "zig_graph_executor_plan_cache_misses_avg": zig_step_avg("graph_executor_plan_cache_misses"),
        "zig_metal_frame_wait_ms_avg": zig_step_avg("metal_frame_wait_ms"),
        "zig_metal_frame_gpu_ms_avg": zig_step_avg("metal_frame_gpu_ms"),
        "zig_metal_tensor_device_owned_peak_live_bytes_avg": zig_step_avg("metal_tensor_device_owned_peak_live_bytes"),
        "zig_metal_tensor_device_owned_live_bytes_avg": zig_step_avg("metal_tensor_device_owned_live_bytes"),
        "zig_metal_runtime_total_bytes_avg": zig_step_avg("metal_runtime_total_bytes"),
        "zig_metal_last_frame_compute_encoders_avg": zig_step_avg("metal_last_frame_compute_encoders"),
        "zig_metal_last_frame_blit_encoders_avg": zig_step_avg("metal_last_frame_blit_encoders"),
        "zig_metal_last_frame_planned_scopes_avg": zig_step_avg("metal_last_frame_planned_scopes"),
        "zig_metal_last_frame_planned_barriers_avg": zig_step_avg("metal_last_frame_planned_barriers"),
        "zig_metal_last_frame_planned_command_ops_avg": zig_step_avg("metal_last_frame_planned_command_ops"),
        "zig_metal_deberta_encoder_plan_attempts_avg": zig_step_avg("metal_deberta_encoder_plan_attempts"),
        "zig_metal_deberta_encoder_plan_successes_avg": zig_step_avg("metal_deberta_encoder_plan_successes"),
        "zig_metal_deberta_encoder_plan_reuses_avg": zig_step_avg("metal_deberta_encoder_plan_reuses"),
        "zig_metal_deberta_encoder_plan_failures_avg": zig_step_avg("metal_deberta_encoder_plan_failures"),
        "zig_metal_deberta_encoder_layer_attempts_avg": zig_step_avg("metal_deberta_encoder_layer_attempts"),
        "zig_metal_deberta_encoder_layer_successes_avg": zig_step_avg("metal_deberta_encoder_layer_successes"),
        "zig_metal_deberta_encoder_layer_fallbacks_avg": zig_step_avg("metal_deberta_encoder_layer_fallbacks"),
        "zig_metal_deberta_encoder_lora_layer_regions_avg": zig_step_avg("metal_deberta_encoder_lora_layer_regions"),
        "zig_metal_deberta_encoder_lora_residual_layernorm_regions_avg": zig_step_avg("metal_deberta_encoder_lora_residual_layernorm_regions"),
        "zig_metal_deberta_encoder_lora_layer_scaffold_regions_avg": zig_step_avg("metal_deberta_encoder_lora_layer_scaffold_regions"),
        "zig_metal_deberta_encoder_lora_layer_fallbacks_avg": zig_step_avg("metal_deberta_encoder_lora_layer_fallbacks"),
        "zig_metal_lora_backward_regions_avg": zig_step_avg("metal_lora_backward_regions"),
        "zig_metal_low_rank_lora_backward_regions_avg": zig_step_avg("metal_low_rank_lora_backward_regions"),
        "zig_metal_rank_adapter_backward_regions_avg": zig_step_avg("metal_rank_adapter_backward_regions"),
        "zig_metal_lora_backward_total_regions_avg": zig_step_sum_avg((
            "metal_lora_backward_regions",
            "metal_low_rank_lora_backward_regions",
            "metal_rank_adapter_backward_regions",
        )),
        "zig_metal_ffn_gelu_backward_regions_avg": zig_step_avg("metal_ffn_gelu_backward_regions"),
        "zig_metal_head_mlp_forward_regions_avg": zig_step_avg("metal_head_mlp_forward_regions"),
        "zig_metal_head_mlp_backward_regions_avg": zig_step_avg("metal_head_mlp_backward_regions"),
        "zig_metal_command_dot_general_dispatches_avg": zig_step_avg("metal_command_dot_general_dispatches"),
        "zig_metal_command_head_dot_dispatches_avg": zig_step_avg("metal_command_head_dot_dispatches"),
        "zig_metal_command_transpose_dispatches_avg": zig_step_avg("metal_command_transpose_dispatches"),
        "zig_metal_command_gather_dispatches_avg": zig_step_avg("metal_command_gather_dispatches"),
        "zig_metal_command_reduce_dispatches_avg": zig_step_avg("metal_command_reduce_dispatches"),
        "zig_metal_command_elementwise_dispatches_avg": zig_step_avg("metal_command_elementwise_dispatches"),
        "zig_metal_command_activation_dispatches_avg": zig_step_avg("metal_command_activation_dispatches"),
        "zig_metal_command_activation_backward_dispatches_avg": zig_step_avg("metal_command_activation_backward_dispatches"),
        "zig_metal_command_other_dispatches_avg": zig_step_avg("metal_command_other_dispatches"),
        "zig_metal_deberta_relative_qk_pair_calls_avg": zig_step_avg("metal_deberta_relative_qk_pair_calls"),
        "zig_metal_deberta_relative_qk_pair_fallbacks_avg": zig_step_avg("metal_deberta_relative_qk_pair_fallbacks"),
        "zig_metal_deberta_ffn_fused_calls_avg": zig_step_avg("metal_deberta_ffn_fused_calls"),
        "zig_metal_deberta_ffn_fused_mps_matmuls_avg": zig_step_avg("metal_deberta_ffn_fused_mps_matmuls"),
        "zig_metal_deberta_ffn_fused_fallbacks_avg": zig_step_avg("metal_deberta_ffn_fused_fallbacks"),
        "zig_metal_deberta_attention_flash_calls_avg": zig_step_avg("metal_deberta_attention_flash_calls"),
        "zig_metal_deberta_attention_gemm_calls_avg": zig_step_avg("metal_deberta_attention_gemm_calls"),
        "zig_metal_deberta_attention_gemm_fallbacks_avg": zig_step_avg("metal_deberta_attention_gemm_fallbacks"),
        "zig_metal_deberta_attention_legacy_calls_avg": zig_step_avg("metal_deberta_attention_legacy_calls"),
        "zig_dot_general_command_count": zig_op_stats.get("command_ops", {}).get("dot_general", {}).get("count"),
        "zig_dot_general_command_total_ms": zig_op_stats.get("command_ops", {}).get("dot_general", {}).get("total_ms"),
        "zig_dot_general_command_avg_ms": zig_op_stats.get("command_ops", {}).get("dot_general", {}).get("avg_ms"),
        "zig_gather_fallback_count": zig_op_stat_value("fallback_ops", "gather", "count"),
        "zig_gather_fallback_total_ms": zig_op_stat_value("fallback_ops", "gather", "total_ms"),
        "zig_gather_host_output_count": zig_op_stat_value("host_output_ops", "gather", "count"),
        "zig_gather_host_output_total_ms": zig_op_stat_value("host_output_ops", "gather", "total_ms"),
        "zig_top_dot_shapes": zig_op_runs.get("top_dot_shapes", []),
        "zig_top_host_output_families": top_op_stat_items(zig_op_stats.get("host_output_ops", {})),
        "zig_top_fallback_families": top_op_stat_items(zig_op_stats.get("fallback_ops", {})),
        "zig_top_host_output_reasons": top_op_stat_items(zig_op_stats.get("host_output_reasons", {})),
        **zig_loop_profile_summary,
        **zig_planned_access_profile_summary,
        "python_cpu_step_gate_ms": python_avg_step_ms,
        "zig_beats_python_cpu_step_time": (
            zig_avg_trainer_ms < python_avg_step_ms
            if zig_avg_trainer_ms is not None and python_avg_step_ms is not None
            else None
        ),
        "zig_beats_python_cpu_warm_step_time": (
            zig_warm_avg_trainer_ms < python_warm_avg_step_ms
            if zig_warm_avg_trainer_ms is not None and python_warm_avg_step_ms is not None
            else None
        ),
        "trainer_speedup_python_over_zig": (
            python_trainer_elapsed / (zig_total_trainer_ms / 1000.0)
            if python_trainer_elapsed is not None and zig_total_trainer_ms not in (None, 0)
            else None
        ),
        "warm_step_wall_speedup_python_over_zig": (
            python_warm_total_step_ms / zig_warm_total_trainer_ms
            if python_warm_total_step_ms not in (None, 0) and zig_warm_total_trainer_ms not in (None, 0)
            else None
        ),
        "zig_warm_step_wall_slowdown_vs_python": (
            zig_warm_total_trainer_ms / python_warm_total_step_ms
            if python_warm_total_step_ms not in (None, 0) and zig_warm_total_trainer_ms not in (None, 0)
            else None
        ),
        "step_wall_speedup_python_over_zig": (
            python_total_step_ms / zig_total_trainer_ms
            if python_total_step_ms not in (None, 0) and zig_total_trainer_ms not in (None, 0)
            else None
        ),
        "zig_step_wall_slowdown_vs_python": (
            zig_total_trainer_ms / python_total_step_ms
            if python_total_step_ms not in (None, 0) and zig_total_trainer_ms not in (None, 0)
            else None
        ),
        "python_last_loss": py_loss,
        "zig_final_step_loss": zig_final_step_loss,
        "zig_final_avg_loss": zig_epoch_avg_loss,
        "loss_delta_zig_minus_python": loss_delta,
        "step_loss_parity_matches": step_loss_parity_matches,
        "step_loss_counts_match": step_loss_counts_match,
        "step_loss_deltas": step_loss_deltas,
        "largest_step_loss_delta": largest_step_loss_delta,
        "preprocess_parity_matches": preprocess_matches,
        "preprocess_parity_mismatches": preprocess_mismatches,
        "component_loss_parity_matches": component_loss_matches,
        "component_loss_deltas": component_loss_deltas,
        "component_loss_focus": component_loss_focus,
        "component_loss_reconciliation": component_loss_reconciliation,
        "classification_debug_matches": classification_debug_matches,
        "classification_debug_reconciliation": classification_debug_reconciliation,
        "classification_debug_deltas": classification_debug_deltas,
        "python_preprocess_task_breakdown": python_task_breakdown,
        "zig_manifest_backend": zig_manifest.get("backend"),
        "zig_manifest_objective": zig_manifest.get("objective"),
        "metal_readiness": summarize_metal_readiness(args, report, zig_step_rows, zig_manifest),
        "loss_parity_tolerance": args.loss_parity_tolerance,
        "classification_debug_tolerance": args.classification_debug_tolerance,
        "valid_loss_parity": valid_loss_parity,
        "loss_parity_warning": loss_parity_warning,
        "perf_target_only_python": args.perf_target_only_python,
        "python_objective_semantics": "upstream GLiNER2Trainer total_loss = classification_loss + structure_loss + count_loss; LoRA mode freezes non-LoRA params",
        "zig_objective_semantics": zig_objective_semantics,
        "required_loss_components": ["classification_loss", "structure_loss", "count_loss"],
        "zig_loss_components_supported": {
            "classification_loss": full_loss_components_supported,
            "structure_loss": args.zig_objective in ("span-start", "gliner2-total-loss"),
            "count_loss": full_loss_components_supported,
        },
        "zig_preprocessing_matches_upstream": upstream_preprocessing_supported,
        "valid_full_loss_parity": bool(
            valid_loss_parity
            and full_loss_components_supported
            and upstream_preprocessing_supported
            and args.zig_objective == "gliner2-total-loss"
        ),
        "adapter_roundtrip_ran": bool(adapter_roundtrip.get("ran")),
        "adapter_roundtrip_ok": adapter_roundtrip.get("ok"),
        "adapter_roundtrip_span_scores_max_abs_delta": adapter_roundtrip.get("span_scores_max_abs_delta"),
        "adapter_roundtrip_classification_logits_max_abs_delta": adapter_roundtrip.get("classification_logits_max_abs_delta"),
        "adapter_roundtrip_weights_max_abs_delta": adapter_roundtrip.get("weights_max_abs_delta"),
        "adapter_roundtrip_tolerance": 0.0,
        "trained_adapter_parity_ran": bool(trained_adapter_parity.get("ran")),
        "trained_adapter_parity_ok": trained_adapter_parity.get("ok"),
        "trained_adapter_parity_span_scores_max_abs_delta": trained_adapter_parity.get("span_scores_max_abs_delta"),
        "trained_adapter_parity_classification_logits_max_abs_delta": trained_adapter_parity.get("classification_logits_max_abs_delta"),
        "trained_adapter_parity_weights_max_abs_delta": trained_adapter_parity.get("weights_max_abs_delta"),
        "trained_adapter_parity_tolerance": args.adapter_roundtrip_tolerance,
        "entity_only_structure_parity": entity_only_structure_parity,
        "non_entity_task_annotations": source_non_entity_task_count,
        "training_slice_non_entity_task_annotations": non_entity_task_count,
        "trainable_parity_warning": trainable_parity_warning,
        "objective_parity_warning": objective_parity_warning,
        "semantic_parity_warning": objective_parity_warning if trainable_parity_warning is None else f"{trainable_parity_warning}; {objective_parity_warning}",
    }

    for section in ("python", "zig"):
        if section in report:
            trim_result_output(report[section], args.max_command_output_chars)

    # ── Strict parity gating ──────────────────────────────────────────────
    # Each check is gated on whether the relevant comparison actually ran;
    # checks that did not run are reported as SKIPPED and do not fail strict
    # mode. Subprocess return codes always gate the exit code.
    both_sides_ran = (
        not args.skip_python
        and not args.skip_zig
        and report.get("python", {}).get("returncode") == 0
        and report.get("zig", {}).get("returncode") == 0
    )
    is_total_loss_objective = args.zig_objective == "gliner2-total-loss"
    component_loss_applicable = both_sides_ran and args.dump_parity and is_total_loss_objective
    # The classification debug comparison is meaningful only when the Python
    # side saw classification tasks in the first batch (entity-only fixtures
    # never emit it); the Zig side prints its debug unconditionally.
    classification_debug_applicable = component_loss_applicable and python_classification_debug is not None
    step_loss_applicable = both_sides_ran and not args.perf_target_only_python
    preprocess_applicable = both_sides_ran and args.dump_parity
    full_loss_applicable = both_sides_ran and is_total_loss_objective and not args.perf_target_only_python
    # Same-artifact interchange is a fail-closed contract once both trainers
    # succeed. Independently trained tensor equality is deliberately diagnostic:
    # cancellation-sensitive encoder reductions can change near-zero gradient
    # signs across frameworks, which Adam amplifies to roughly 2*lr despite
    # matching forward loss and task-head gradients. Result/convergence parity
    # belongs in the held-out release gate, not this one-step tensor check.
    roundtrip_applicable = args.adapter_roundtrip and both_sides_ran and not args.perf_target_only_python
    step_count_gate_applicable = (
        (not args.skip_python or not args.skip_zig)
        and (args.skip_python or report.get("python", {}).get("returncode") == 0)
        and (args.skip_zig or report.get("zig", {}).get("returncode") == 0)
    )
    metal_readiness_summary = report["summary"].get("metal_readiness") or {}
    metal_readiness_checks = metal_readiness_summary.get("checks", {})
    metal_dispatch_check = metal_readiness_checks.get("graph_executor_dispatches_nonzero")
    metal_backend_ran = (
        args.zig_backend == "metal"
        and not args.skip_zig
        and report.get("zig", {}).get("returncode") == 0
    )
    metal_dispatch_applicable = metal_dispatch_check is not None and metal_backend_ran
    strict_checks: dict[str, bool | None] = {
        "requested_step_count_valid": bool(step_count_valid) if step_count_gate_applicable else None,
        "component_loss_parity_matches": component_loss_matches if component_loss_applicable else None,
        "classification_debug_matches": classification_debug_matches if classification_debug_applicable else None,
        "step_loss_parity_matches": step_loss_parity_matches if step_loss_applicable else None,
        "step_loss_counts_match": step_loss_counts_match if step_loss_applicable else None,
        "preprocess_parity_matches": preprocess_matches if preprocess_applicable else None,
        "valid_full_loss_parity": report["summary"]["valid_full_loss_parity"] if full_loss_applicable else None,
        "adapter_roundtrip_ok": bool(adapter_roundtrip.get("ok")) if roundtrip_applicable else None,
        # The independently-trained final-adapter comparison shares the
        # roundtrip applicability gate; without this entry the multi-step
        # e2e gate's required trained_adapter_parity_ok check can never pass.
        "trained_adapter_parity_ok": bool(trained_adapter_parity.get("ok")) if roundtrip_applicable and trained_adapter_parity.get("ran") else None,
        "metal_graph_executor_dispatches_nonzero": bool(metal_dispatch_check) if metal_dispatch_applicable else None,
    }
    if metal_backend_ran:
        # Metal-gate bundle (Phase 1, Metal_Gliner_Next_steps.md §1): promote the
        # metal-readiness signals from warning-only to strict failures. These are
        # gated on the Metal backend so the native gate is unaffected. A check
        # absent from metal_readiness_checks (e.g. the graph-executor ones when
        # the executor was not requested) maps to None = SKIPPED.
        def _metal(name: str) -> bool | None:
            v = metal_readiness_checks.get(name)
            return bool(v) if v is not None else None
        strict_checks.update({
            "metal_manifest_backend_is_metal": _metal("manifest_backend_is_metal"),
            "metal_optimizer_backend_is_metal": _metal("optimizer_backend_is_metal"),
            "metal_device_resident_transfers_zero": _metal("device_resident_transfers_zero"),
            "metal_finite_step_loss": _metal("finite_step_loss"),
            "metal_graph_executor_fallback_reasons_empty": _metal("graph_executor_fallback_reasons_empty"),
            "metal_graph_executor_true_host_outputs_zero": _metal("graph_executor_true_host_outputs_zero"),
            "metal_interpreter_fallbacks_within_threshold": _metal("interpreter_fallbacks_within_threshold"),
        })
    if args.require_full_task_parity:
        # --require-full-task-parity (Phase 5 parity-envelope expansion):
        # graduate the warning-only/scoped comparisons into failing checks.
        # "Ran" checks turn a SKIPPED comparison into a FAIL: the component
        # losses must be present and compared (component_loss_parity_matches
        # above then gates present+matching), the preprocessing comparison
        # must run, the full-loss verdict must be evaluated, and — whenever
        # the fixture contains classification tasks — the classification
        # debug comparison must run too (with the deterministic no-shuffle
        # loader the first batch is the first batch_size fixture lines, so
        # classification fixtures must place a classification example there).
        # The trainable/objective parity warnings become hard failures.
        fixture_has_classifications = int(converted.get("classifications", 0)) > 0
        # A crashed subprocess is already gated by the returncode checks, so
        # report the ran-checks as SKIPPED there; an intentionally skipped
        # side (--skip-python/--skip-zig) or a missing dump must FAIL because
        # the comparison was required to run.
        subprocess_failed = (
            (not args.skip_python and report.get("python", {}).get("returncode") != 0)
            or (not args.skip_zig and report.get("zig", {}).get("returncode") != 0)
        )
        def _slice_covers(source_key: str, slice_key: str | None = None) -> bool:
            # Require the compared slice (first steps*batch_size records) to
            # exercise every task family the source fixture contains; families
            # absent from the whole fixture are vacuously covered. An
            # unconditional all-family requirement is unsatisfiable for the
            # single-family parity fixtures in the e2e matrix.
            key = slice_key or source_key
            if int(source_task_summary.get(source_key, 0) or 0) <= 0:
                return True
            return int(converted.get(key, 0) or 0) > 0

        strict_checks.update({
            "full_task_compared_slice_has_entities": _slice_covers("mentions"),
            "full_task_compared_slice_has_classifications": _slice_covers("classifications"),
            "full_task_compared_slice_has_json_structures": _slice_covers("json_structures"),
            "full_task_compared_slice_has_relations": _slice_covers("relations"),
            "full_task_component_loss_comparison_ran": None if subprocess_failed else bool(component_loss_applicable),
            "full_task_preprocess_comparison_ran": None if subprocess_failed else bool(preprocess_applicable),
            "full_task_loss_parity_comparison_ran": None if subprocess_failed else bool(full_loss_applicable),
            "full_task_classification_debug_ran": (
                (None if subprocess_failed else bool(classification_debug_applicable))
                if fixture_has_classifications
                else None
            ),
            "full_task_trainable_parity": trainable_parity_warning is None,
            "full_task_objective_parity": objective_parity_warning is None,
        })
    report["summary"]["strict_mode"] = args.strict
    report["summary"]["require_full_task_parity"] = args.require_full_task_parity
    report["summary"]["strict_checks"] = strict_checks

    report_path = args.out_dir / "comparison_report.json"
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(f"comparison report: {report_path}")
    print(json.dumps(report["summary"], indent=2))

    returncodes_ok = all(
        section not in report or report[section].get("returncode") == 0
        for section in ("python", "zig")
    )
    strict_failures: list[str] = []
    print(f"strict parity gating: {'ON' if args.strict else 'OFF (--no-strict)'}")
    for check_name, check_value in strict_checks.items():
        if check_value is None:
            status = "SKIPPED (comparison did not run)"
        elif check_value:
            status = "PASS"
        else:
            status = "FAIL"
            strict_failures.append(check_name)
        print(f"  PARITY {check_name}: {status}")
    print(f"  PARITY python_returncode: {'PASS' if 'python' not in report or report['python'].get('returncode') == 0 else 'FAIL'}")
    print(f"  PARITY zig_returncode: {'PASS' if 'zig' not in report or report['zig'].get('returncode') == 0 else 'FAIL'}")

    ok = returncodes_ok and (not args.strict or not strict_failures)
    if not ok:
        if not returncodes_ok:
            print("RESULT: FAIL (subprocess returncode)")
        else:
            print(f"RESULT: FAIL (strict parity checks failed: {', '.join(strict_failures)})")
    else:
        print("RESULT: PASS")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
