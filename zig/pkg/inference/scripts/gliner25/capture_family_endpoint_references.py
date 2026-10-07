#!/usr/bin/env python3
"""Capture additive mixed-feature endpoint bounds for multilingual GLiNER2.5."""
from __future__ import annotations

import argparse
import json
import os
import platform
import shutil
import sys
import tempfile
from pathlib import Path
from typing import Any

import capture_family_references as common
import verify_family_contract as family


HERE = Path(__file__).resolve().parent
DEFAULT_REQUESTS = HERE / "family_endpoint_reference_requests.json"
FROZEN_COMMON_SHA256 = "4e9fee278e8e4757f1ee03aec87208dfc7cbb58eb7a009b30e8f525c8eb9cde5"


def validate_requests(path: Path) -> dict[str, Any]:
    document = family.strict_json(path)
    if document.get("format_version") != 1 or document.get("scope") != "mixed_feature_endpoint_bounds_reference":
        raise common.CaptureError("invalid endpoint-bound request fixture")
    extract = document.get("extract_schema")
    classification = document.get("classification_schema")
    native = document.get("native_schema")
    if not all(isinstance(value, dict) for value in (extract, classification, native)):
        raise common.CaptureError("endpoint schemas must be objects")
    if not all(key in extract for key in ("entities", "relations", "structures")):
        raise common.CaptureError("extract schema must exercise entities, relations, and structures")
    tasks = classification.get("tasks")
    if not isinstance(tasks, dict) or len(tasks) != 1:
        raise common.CaptureError("endpoint classification schema must contain one task")
    task = next(iter(tasks.values()))
    if not isinstance(task.get("labels"), dict) or not task.get("instruction"):
        raise common.CaptureError("endpoint classification must combine descriptions and prompt")
    if task.get("min_labels") != 1 or task.get("max_labels") != 1:
        raise common.CaptureError("endpoint classification must exercise structured selection")
    native_task = native.get("classifications", [{}])[0]
    if native_task.get("prompt") != task["instruction"] or native_task.get("min_labels") != 1 or native_task.get("max_labels") != 1:
        raise common.CaptureError("native structured classification differs")
    requests = document.get("requests")
    if not isinstance(requests, list) or [row.get("id") for row in requests] != ["one_character", "realistic_long"]:
        raise common.CaptureError("endpoint fixture must contain the two ordered bound requests")
    short, long = requests
    if len(short.get("text", "").encode()) != 1:
        raise common.CaptureError("lower-bound text must be exactly one UTF-8 byte")
    if not 550 <= len(long.get("text", "").encode()) <= 750 or not 90 <= len(long["text"].split()) <= 110:
        raise common.CaptureError("upper-bound text must stay near 600 bytes and 100 words")
    return document


def build_joint_schema(document: dict[str, Any], classifier: Any) -> tuple[Any, Any]:
    from gliner2 import Schema
    from gliner2.classification import ClassificationSchema

    schema = Schema.from_dict(document["extract_schema"])
    compiled = classifier.compile_schema(ClassificationSchema.from_dict(document["classification_schema"]))
    # Current upstream Schema.from_dict exposes only the legacy joint
    # classification fields.  Compose its supported internal model-schema
    # contract with the dedicated classification compiler so descriptions,
    # prompt, activation and exclusivity are measured in the same encoder pass
    # as every extraction head.
    schema.schema["classifications"] = compiled.build()["classifications"]
    return schema, compiled


def mixed_classification(
    model: Any,
    classifier: Any,
    compiled: Any,
    text: str,
    schema: Any,
    encoded_ids: list[int],
    torch: Any,
) -> tuple[list[dict[str, Any]], Any]:
    from gliner2.classification import ClassificationConfig, ClassificationScores

    raw = schema.build()
    batch = model.processor.collate_fn_inference(
        [(text, raw)], max_len=common.MAX_WORDS, error_policy="raise",
        architecture="boundary", build_targets=False, on_capacity_exceeded="raise",
    )
    ids = batch.input_ids.detach().cpu().tolist()[0]
    if ids != encoded_ids:
        raise common.CaptureError("mixed raw-logit preprocessing differs from captured input IDs")
    with torch.inference_mode():
        core = model._encode_core(batch)
        results = []
        for spec in core["cls_specs"][0]:
            prompt = spec["schema_tokens"][2]
            config = model._resolve_classification_config(prompt, raw["classifications"])
            if config is None:
                raise common.CaptureError("mixed classification config was not resolved")
            # Preserve the head logits.  The dedicated structured classifier
            # applies its per-task temperature during probability/utility
            # presentation; the legacy joint decoder separately applies the
            # checkpoint's boundary temperature.
            logits = model.classifier(spec["group_embs"][1:]).squeeze(-1)
            labels = config["labels"]
            if logits.numel() != len(labels):
                raise common.CaptureError("mixed classification logit geometry differs")
            results.append({
                "name": config["task"],
                "labels": labels,
                "raw_logits": common.jsonable(logits),
            })
    raw_by_task = {
        task["name"]: dict(zip(task["labels"], task["raw_logits"]))
        for task in results
    }
    scores = ClassificationScores(
        text=text,
        tasks=raw_by_task,
        fingerprint=compiled.fingerprint,
        specs={spec.name: spec for spec in compiled.task_specs},
    )
    config = ClassificationConfig(
        on_infeasible="raise", max_len=common.MAX_WORDS, include_confidence=True
    )
    structured = classifier.decode(scores, compiled, config=config)
    return results, structured


def capture_rows(model: Any, document: dict[str, Any], torch: Any) -> list[dict[str, Any]]:
    from gliner2.classification import Classifier

    classifier = Classifier(model)
    schema, compiled = build_joint_schema(document, classifier)
    native = document["native_schema"]
    native_json = json.dumps(native, ensure_ascii=False, separators=(",", ":"), sort_keys=False)
    schema_bytes = len(native_json.encode())
    rows = []
    for request in document["requests"]:
        text = request["text"]
        encoded = common.encoded_evidence(model, text, schema, boundary=True)
        raw_tasks, structured = mixed_classification(
            model, classifier, compiled, text, schema, encoded["input_ids"], torch
        )
        with torch.inference_mode():
            output = model.extract(text, schema, threshold=0.5, include_confidence=True,
                                   include_spans=True, max_len=common.MAX_WORDS)
        output = common.jsonable(output)
        structured_output = common.jsonable(structured.to_dict(include_confidence=True))
        native_output = dict(output)
        native_output.update({task: structured_output[task] for task in compiled.task_order})
        processor_words = list(model.processor.word_splitter(text, lower=False))
        rows.append({
            "id": request["id"],
            "text": text,
            "native_schema": native,
            "native_schema_json": native_json,
            "encoded": encoded,
            "native_classification": {"input_ids": encoded["input_ids"], "tasks": raw_tasks},
            "decoded_output": {
                "joint_extract": output,
                "structured_classification": structured_output,
            },
            "native_expected": common.canonical_expected(
                {"kind": "extract"}, native, native_output
            ),
            "measured_bounds": {
                "text_bytes": len(text.encode()),
                "whitespace_words": len(text.split()),
                "processor_words": len(processor_words),
                "input_tokens": len(encoded["input_ids"]),
                "schema_bytes": schema_bytes,
            },
            "features_exercised": [
                "described_entities", "relations", "legacy_structures",
                "classification_label_descriptions", "classification_prompt",
                "classification_min_max_selection",
            ],
            "capture_pass": (
                len(raw_tasks) == len(compiled.task_order)
                and list(structured.tasks) == list(compiled.task_order)
                and structured.feasible
            ),
        })
    return rows


def capture(args: argparse.Namespace) -> dict[str, Any]:
    if os.environ.get("PYTHONHASHSEED") != "0":
        raise common.CaptureError("capture requires PYTHONHASHSEED=0")
    if common.sha256_file(Path(common.__file__)) != FROZEN_COMMON_SHA256:
        raise common.CaptureError("shared capture helper differs from its pinned identity")
    contract = family.strict_json(args.contract)
    source = common.verify_source(args.upstream, contract)
    runtime = common.runtime_identity(contract)
    document = validate_requests(args.requests)
    before = family.verify_model(args.profile, args.model_dir, contract_path=args.contract, verify_model_sha256=True)
    sys.dont_write_bytecode = True
    os.environ.update({"HF_HUB_OFFLINE": "1", "TRANSFORMERS_OFFLINE": "1",
                       "TOKENIZERS_PARALLELISM": "false", "OMP_NUM_THREADS": str(args.threads),
                       "MKL_NUM_THREADS": str(args.threads)})
    sys.path.insert(0, str(args.upstream.resolve()))
    import torch
    import gliner2
    from gliner2 import AutoExtractor

    common.install_inference_peft_shim()
    torch.set_num_threads(args.threads)
    torch.set_num_interop_threads(1)
    torch.use_deterministic_algorithms(True)
    torch.set_default_dtype(torch.float32)
    torch.manual_seed(0)
    model = AutoExtractor.from_pretrained(str(args.model_dir.resolve()), local_files_only=True,
                                          map_location="cpu", use_flashdeberta=False).float().cpu().eval()
    if getattr(model, "architecture", None) != "boundary":
        raise common.CaptureError("endpoint-bound oracle requires a boundary checkpoint")
    rows = capture_rows(model, document, torch)
    after = family.verify_model(args.profile, args.model_dir, contract_path=args.contract, verify_model_sha256=True)
    if after != before:
        raise common.CaptureError("model identity changed during capture")
    report = {
        "format_version": 1, "status": "captured", "scope": "upstream_mixed_feature_endpoint_bounds",
        "qualification": False, "native_runtime_qualified": False, "production_qualified": False,
        "capture_pass": all(row["capture_pass"] for row in rows), "model": before,
        "source": {
            **source,
            "package_version": gliner2.__version__,
            "advanced_classification_composition": {
                "joint_schema_parser_limit": (
                    "Schema.from_dict cannot represent min_labels, max_labels, or the "
                    "dedicated structured-classification constraint model"
                ),
                "model_wire": (
                    "ClassificationSchema is compiled and its supported prompt, label "
                    "description, activation, threshold, and exclusivity wire is injected "
                    "into the same joint Schema request as every extraction head"
                ),
                "structured_decode": (
                    "The dedicated min/max solver decodes the exact unscaled classification "
                    "head logits captured from that mixed encoder pass"
                ),
                "joint_decode": (
                    "The upstream joint extraction decoder output is retained separately "
                    "because its legacy wire does not carry structured solver constraints"
                ),
            },
        },
        "runtime": {**runtime, "platform": {"system": platform.system(), "machine": platform.machine()},
                    "device": "cpu", "dtype": "float32", "threads": args.threads},
        "artifacts": {"generator_sha256": common.sha256_file(Path(__file__)),
                      "shared_helper_sha256": FROZEN_COMMON_SHA256,
                      "contract_sha256": common.sha256_file(args.contract),
                      "requests_sha256": common.sha256_file(args.requests)},
        "requests": rows,
    }
    destination = args.output.expanduser().absolute()
    if destination.exists() or destination.is_symlink():
        raise common.CaptureError(f"refusing to overwrite output: {destination}")
    destination.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{destination.name}-", dir=destination.parent))
    try:
        (staging / "capture.json").write_text(json.dumps(report, ensure_ascii=False, sort_keys=True,
                                                          indent=2, allow_nan=False) + "\n", encoding="utf-8")
        staging.rename(destination)
    finally:
        if staging.exists(): shutil.rmtree(staging)
    return {"status": "captured", "profile": args.profile, "output": str(destination),
            "requests": len(rows), "capture_pass": report["capture_pass"], "qualification": False}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", choices=("multi_v1", "multi_decide"), required=True)
    parser.add_argument("--model-dir", type=Path, required=True)
    parser.add_argument("--upstream", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--requests", type=Path, default=DEFAULT_REQUESTS)
    parser.add_argument("--contract", type=Path, default=family.CONTRACT_PATH)
    parser.add_argument("--threads", type=int, default=2)
    args = parser.parse_args()
    try:
        if not 1 <= args.threads <= 8: raise common.CaptureError("threads must be in 1..8")
        print(json.dumps(capture(args), sort_keys=True, allow_nan=False))
        return 0
    except (common.CaptureError, family.ContractError, OSError, ImportError, RuntimeError,
            ValueError, KeyError, TypeError, IndexError) as exc:
        print(json.dumps({"status": "error", "error_type": type(exc).__name__, "error": str(exc),
                          "qualification": False}, sort_keys=True), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
