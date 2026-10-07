#!/usr/bin/env python3
"""Capture exact public /decide distributions for multilingual boundary models."""
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
DEFAULT_REQUESTS = HERE / "family_decide_reference_requests.json"
FROZEN_COMMON_SHA256 = "4e9fee278e8e4757f1ee03aec87208dfc7cbb58eb7a009b30e8f525c8eb9cde5"


def labels_for(question: dict[str, Any]) -> tuple[list[str], list[str]]:
    kind = question["type"]
    criteria = question.get("criteria")
    if kind == "choice":
        if not isinstance(criteria, dict) or len(criteria) < 2:
            raise common.CaptureError("choice criteria must be a description map")
        return list(criteria), list(criteria.values())
    if kind == "score":
        if not isinstance(criteria, list) or len(criteria) < 2:
            raise common.CaptureError("score criteria must be a description list")
        return [str(index) for index in range(len(criteria))], criteria
    if kind == "noul":
        if criteria is not None:
            raise common.CaptureError("noul cannot declare criteria")
        return ["false", "true"], ["False", "True"]
    raise common.CaptureError(f"unsupported /decide question type: {kind!r}")


def native_schema(decide_request: dict[str, Any]) -> dict[str, Any]:
    classifications = []
    for name, question in decide_request["questions"].items():
        labels, descriptions = labels_for(question)
        classifications.append({
            "name": name,
            "prompt": question["instructions"],
            "top_k": len(labels),
            "labels": labels,
            "label_definitions": {
                label: {"description": description}
                for label, description in zip(labels, descriptions, strict=True)
            },
        })
    return {"classifications": classifications}


def validate_requests(path: Path) -> dict[str, Any]:
    document = family.strict_json(path)
    if document.get("format_version") != 1 or document.get("scope") != "public_decide_classification_reference":
        raise common.CaptureError("invalid public /decide request fixture")
    requests = document.get("requests")
    if not isinstance(requests, list) or len(requests) != 2:
        raise common.CaptureError("public /decide fixture must contain exactly two requests")
    seen: set[str] = set()
    for row in requests:
        if row.get("kind") != "classification" or row.get("text") != row.get("decide_request", {}).get("state"):
            raise common.CaptureError("/decide row must be a matching classification request")
        if row.get("id") in seen or not isinstance(row.get("id"), str):
            raise common.CaptureError("/decide request IDs must be unique")
        seen.add(row["id"])
        derived = native_schema(row["decide_request"])
        if not family._same_json(row.get("native_schema"), derived):
            raise common.CaptureError("native schema differs from exact /decide translation")
        tasks = row.get("schema", {}).get("tasks")
        if not isinstance(tasks, dict) or list(tasks) != list(row["decide_request"]["questions"]):
            raise common.CaptureError("upstream tasks differ from /decide question order")
        for name, question in row["decide_request"]["questions"].items():
            labels, descriptions = labels_for(question)
            task = tasks[name]
            if (
                task.get("labels") != dict(zip(labels, descriptions, strict=True))
                or task.get("instruction") != question["instructions"]
                or task.get("min_labels") != 1
                or task.get("max_labels") != 1
                or bool(task.get("ordered", False)) != (question["type"] == "score")
            ):
                raise common.CaptureError("upstream task differs from /decide semantics")
        if row.get("expect", {}).get("tasks") != list(tasks):
            raise common.CaptureError("/decide task-order expectation differs")
    return document


def decide_answers(decide_request: dict[str, Any], probabilities: dict[str, dict[str, float]]) -> dict[str, Any]:
    answers: dict[str, Any] = {}
    for name, question in decide_request["questions"].items():
        labels, descriptions = labels_for(question)
        values = probabilities[name]
        if list(values) != labels:
            raise common.CaptureError("probability label order differs from /decide request")
        if abs(sum(values.values()) - 1.0) > 1e-6:
            raise common.CaptureError("/decide exclusive distribution does not sum to one")
        kind = question["type"]
        answer: dict[str, Any] = {"type": kind}
        if kind == "choice":
            answer["choice"] = max(labels, key=values.__getitem__)
        elif kind == "score":
            answer["score"] = sum(index * values[label] for index, label in enumerate(labels))
            answer["legend"] = dict(zip(labels, descriptions, strict=True))
        else:
            answer["noul"] = values["true"]
        if kind != "noul":
            answer["probabilities"] = values
        answers[name] = answer
    return answers


def capture_rows(model: Any, requests: list[dict[str, Any]], torch: Any, wire_model: str) -> list[dict[str, Any]]:
    from gliner2.classification import Classifier, ClassificationConfig, ClassificationSchema

    classifier = Classifier(model)
    rows = []
    for request in requests:
        schema = ClassificationSchema.from_dict(request["schema"])
        compiled = classifier.compile_schema(schema)
        encoded = common.encoded_evidence(model, request["text"], compiled, boundary=False)
        config = ClassificationConfig(on_infeasible="raise", max_len=common.MAX_WORDS, include_confidence=True)
        with torch.inference_mode():
            scores = classifier.score(request["text"], compiled, config=config)
            result = classifier.decode(scores, compiled, config=config)
        probabilities = {
            task: {label: scores.probability(task, label) for label in scores.tasks[task]}
            for task in compiled.task_order
        }
        decide_request = common.jsonable(request["decide_request"])
        decide_request["model"] = wire_model
        native = request["native_schema"]
        rows.append({
            "id": request["id"],
            "text": request["text"],
            "schema": request["schema"],
            "native_schema": native,
            "native_schema_json": json.dumps(native, ensure_ascii=False, separators=(",", ":"), sort_keys=False),
            "decide_request": decide_request,
            "decide_request_json": json.dumps(decide_request, ensure_ascii=False, separators=(",", ":"), sort_keys=False),
            "encoded": encoded,
            "native_classification": {
                "input_ids": encoded["input_ids"],
                "tasks": [
                    {
                        "name": task,
                        "labels": list(scores.tasks[task]),
                        "raw_logits": [
                            common.jsonable(scores.tasks[task][label])
                            for label in scores.tasks[task]
                        ],
                    }
                    for task in compiled.task_order
                ],
            },
            "raw_logits": {
                task: {label: common.jsonable(logit) for label, logit in scores.tasks[task].items()}
                for task in compiled.task_order
            },
            "probabilities": probabilities,
            "selected": {task: list(result.selected(task)) for task in compiled.task_order},
            "decide_expected": {"model": wire_model, "answers": decide_answers(decide_request, probabilities)},
            "semantic": {"pass": list(compiled.task_order) == request["expect"]["tasks"],
                         "expected_tasks": request["expect"]["tasks"], "actual_tasks": list(compiled.task_order)},
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
    requests = validate_requests(args.requests)
    before = family.verify_model(args.profile, args.model_dir, contract_path=args.contract, verify_model_sha256=True)
    sys.dont_write_bytecode = True
    os.environ.update({"HF_HUB_OFFLINE": "1", "TRANSFORMERS_OFFLINE": "1",
                       "TOKENIZERS_PARALLELISM": "false", "OMP_NUM_THREADS": str(args.threads),
                       "MKL_NUM_THREADS": str(args.threads)})
    sys.path.insert(0, str(args.upstream.resolve()))
    import torch
    import gliner2
    from gliner2 import AutoExtractor

    torch.set_num_threads(args.threads)
    torch.set_num_interop_threads(1)
    torch.use_deterministic_algorithms(True)
    torch.set_default_dtype(torch.float32)
    torch.manual_seed(0)
    model = AutoExtractor.from_pretrained(str(args.model_dir.resolve()), local_files_only=True,
                                          map_location="cpu", use_flashdeberta=False).float().cpu().eval()
    if getattr(model, "architecture", None) != "boundary":
        raise common.CaptureError("multilingual /decide oracle requires a boundary checkpoint")
    rows = capture_rows(model, requests["requests"], torch, contract["models"][args.profile]["repo"])
    after = family.verify_model(args.profile, args.model_dir, contract_path=args.contract, verify_model_sha256=True)
    if after != before:
        raise common.CaptureError("model identity changed during capture")
    report = {
        "format_version": 1, "status": "captured", "scope": "upstream_public_decide_reference",
        "qualification": False, "native_runtime_qualified": False, "production_qualified": False,
        "semantic_pass": all(row["semantic"]["pass"] for row in rows), "model": before,
        "source": {**source, "package_version": gliner2.__version__},
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
        if staging.exists():
            shutil.rmtree(staging)
    return {"status": "captured", "profile": args.profile, "output": str(destination),
            "requests": len(rows), "semantic_pass": report["semantic_pass"], "qualification": False}


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
        if not 1 <= args.threads <= 8:
            raise common.CaptureError("threads must be in 1..8")
        print(json.dumps(capture(args), sort_keys=True, allow_nan=False))
        return 0
    except (common.CaptureError, family.ContractError, OSError, ImportError, RuntimeError,
            ValueError, KeyError, TypeError) as exc:
        print(json.dumps({"status": "error", "error_type": type(exc).__name__, "error": str(exc),
                          "qualification": False}, sort_keys=True), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
