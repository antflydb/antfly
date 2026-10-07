#!/usr/bin/env python3
"""Capture bounded multilingual GLiNER2.5 Python reference outputs.

This produces source-model oracle evidence only.  It does not exercise an
Antfly runtime or qualify a backend, precision, model, or task for production.
"""
from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import importlib.util
import json
import os
import platform
import re
import shutil
import subprocess
import sys
import tempfile
import types
import unicodedata
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import verify_family_contract as family


HERE = Path(__file__).resolve().parent
DEFAULT_REQUESTS = HERE / "family_reference_requests.json"
MAX_REQUESTS = 16
MAX_TEXT_BYTES = 4096
MAX_SCHEMA_BYTES = 16384
MAX_ENCODED_TOKENS = 512
MAX_WORDS = 128


class CaptureError(ValueError):
    pass


def sha256_file(path: Path) -> str:
    return family.sha256_file(path)


def git(source: Path, *args: str) -> str:
    result = subprocess.run(
        ["git", "-C", str(source), *args],
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
        env={**os.environ, "GIT_OPTIONAL_LOCKS": "0"},
    )
    if result.returncode:
        raise CaptureError(f"cannot verify upstream checkout: {result.stderr.strip()}")
    return result.stdout.strip()


def verify_source(source: Path, contract: dict[str, Any]) -> dict[str, str]:
    source = source.expanduser().resolve()
    if not (source / "gliner2" / "__init__.py").is_file():
        raise CaptureError(f"not a GLiNER2 checkout: {source}")
    if Path(git(source, "rev-parse", "--show-toplevel")).resolve() != source:
        raise CaptureError("upstream path must be the checkout root")
    expected = contract["upstream_python"]
    commit = git(source, "rev-parse", "HEAD")
    if commit != expected["revision"]:
        raise CaptureError(f"upstream commit {commit} != pinned {expected['revision']}")
    if git(source, "status", "--porcelain=v1", "--untracked-files=all"):
        raise CaptureError("upstream checkout is dirty")
    if git(source, "ls-files", "--others", "--ignored", "--exclude-standard"):
        raise CaptureError("upstream checkout contains ignored files")
    return {"repo": expected["repo"], "revision": commit, "checkout": str(source)}


def runtime_identity(contract: dict[str, Any]) -> dict[str, Any]:
    expected = contract["oracle_runtime"]
    actual = {
        "python": platform.python_version(),
        "unicode": unicodedata.unidata_version,
        "packages": {
            name: importlib.metadata.version(name) for name in expected["packages"]
        },
        "absent_packages": expected.get("absent_packages", []),
    }
    present = [name for name in actual["absent_packages"] if importlib.util.find_spec(name)]
    if present:
        raise CaptureError(f"oracle runtime requires absent packages: {present!r}")
    if not family._same_json(actual, expected):
        raise CaptureError(f"oracle runtime differs: actual={actual!r}")
    return actual


def install_inference_peft_shim() -> dict[str, Any]:
    """Satisfy GLiNER2's serving-only eager trainer type imports.

    The pinned runtime deliberately has no PEFT installation.  Public boundary
    extraction lazily imports ``ExtractorCollator`` from the training module,
    whose two top-level PEFT imports are used only by trainer/LoRA paths.  A
    minimal type-only module keeps that unrelated optional dependency out of
    the oracle while leaving every executed inference object untouched.
    """
    if importlib.util.find_spec("peft") is not None:
        raise CaptureError("PEFT compatibility shim requires peft to be absent")
    peft = types.ModuleType("peft")
    tuners = types.ModuleType("peft.tuners")
    lora = types.ModuleType("peft.tuners.lora")
    layer = types.ModuleType("peft.tuners.lora.layer")

    class InactivePeftModel:
        pass

    class InactiveLoraLayer:
        pass

    peft.PeftModel = InactivePeftModel
    layer.LoraLayer = InactiveLoraLayer
    sys.modules.update({
        "peft": peft,
        "peft.tuners": tuners,
        "peft.tuners.lora": lora,
        "peft.tuners.lora.layer": layer,
    })
    return {
        "name": "inference_only_peft_type_import_shim",
        "reason": "pinned GLiNER2 ExtractorCollator eagerly imports trainer-only PEFT types",
        "classes": ["PeftModel", "LoraLayer"],
        "executed_peft_code": False,
    }


def validate_requests(path: Path) -> dict[str, Any]:
    document = family.strict_json(path)
    if not isinstance(document, dict) or document.get("format_version") != 1:
        raise CaptureError("request fixture must have format_version 1")
    requests = document.get("requests")
    if not isinstance(requests, list) or not 1 <= len(requests) <= MAX_REQUESTS:
        raise CaptureError(f"request fixture must contain 1..{MAX_REQUESTS} requests")
    seen: set[str] = set()
    for request in requests:
        if not isinstance(request, dict):
            raise CaptureError("every request must be an object")
        request_id = request.get("id")
        if (
            not isinstance(request_id, str)
            or not re.fullmatch(r"[a-z][a-z0-9_]{0,63}", request_id)
            or request_id in seen
        ):
            raise CaptureError("request IDs must be unique safe identifiers")
        seen.add(request_id)
        if request.get("kind") not in ("extract", "classification"):
            raise CaptureError(f"unsupported request kind: {request.get('kind')!r}")
        text = request.get("text")
        schema = request.get("schema")
        if not isinstance(text, str) or not 0 < len(text.encode()) <= MAX_TEXT_BYTES:
            raise CaptureError(f"{request_id}: invalid or oversized text")
        if not isinstance(schema, dict):
            raise CaptureError(f"{request_id}: schema must be an object")
        encoded_schema = json.dumps(
            schema, ensure_ascii=False, allow_nan=False, separators=(",", ":")
        ).encode()
        if len(encoded_schema) > MAX_SCHEMA_BYTES:
            raise CaptureError(f"{request_id}: schema is oversized")
        if not isinstance(request.get("expect"), dict):
            raise CaptureError(f"{request_id}: semantic expectation is required")
    return document


def jsonable(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [jsonable(item) for item in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if hasattr(value, "tolist"):
        return jsonable(value.tolist())
    raise CaptureError(f"cannot serialize oracle value {type(value).__name__}")


def encoded_evidence(model: Any, text: str, schema: Any, *, boundary: bool) -> dict[str, Any]:
    words = list(model.processor.word_splitter(text, lower=False))
    if len(words) > MAX_WORDS:
        raise CaptureError(f"request exceeds {MAX_WORDS} words")
    raw = schema.build() if hasattr(schema, "build") else schema
    batch = model.processor.collate_fn_inference(
        [(text, raw)],
        max_len=MAX_WORDS,
        error_policy="raise",
        architecture="boundary" if boundary else "span",
        build_targets=False,
        on_capacity_exceeded="raise",
    )
    input_ids = batch.input_ids.detach().cpu().tolist()
    if len(input_ids) != 1 or not 0 < len(input_ids[0]) <= MAX_ENCODED_TOKENS:
        raise CaptureError("encoded request is outside the bounded token contract")
    return {
        "input_ids": input_ids[0],
        "attention_mask": batch.attention_mask.detach().cpu().tolist()[0],
        "text_tokens": jsonable(batch.text_tokens[0]),
        "schema_tokens": jsonable(batch.schema_tokens_list[0]),
        "start_mappings": jsonable(batch.start_mappings[0]),
        "end_mappings": jsonable(batch.end_mappings[0]),
    }


def all_strings(value: Any) -> set[str]:
    if isinstance(value, str):
        return {value}
    if isinstance(value, Mapping):
        result: set[str] = set()
        for item in value.values():
            result.update(all_strings(item))
        return result
    if isinstance(value, (list, tuple)):
        result = set()
        for item in value:
            result.update(all_strings(item))
        return result
    return set()


def native_schema_for(request: dict[str, Any]) -> dict[str, Any]:
    """Translate upstream schema JSON into Antfly extraction-v2 schema JSON."""
    original = request["schema"]
    if request["kind"] == "classification":
        classifications = []
        for name, raw in original["tasks"].items():
            spec = dict(raw)
            labels = spec.pop("labels")
            item: dict[str, Any] = {"name": name}
            if isinstance(labels, dict):
                item["labels"] = list(labels)
                item["label_definitions"] = {
                    label: {"description": description}
                    for label, description in labels.items()
                }
            else:
                item["labels"] = labels
            # Upstream's `instruction` is the same prompt semantic accepted by
            # extraction-v2. Preserve all solver and presentation controls.
            item.update(spec)
            classifications.append(item)
        return {"classifications": classifications, "classification_constraints": []}

    schema = dict(original)
    if isinstance(schema.get("entities"), dict):
        schema["entity_definitions"] = {
            name: {"description": description}
            for name, description in schema["entities"].items()
        }
        schema["entities"] = list(schema["entities"])
    if "classifications" in schema:
        schema["classifications"] = [
            {"name": item["task"], **{key: value for key, value in item.items() if key != "task"}}
            for item in schema["classifications"]
        ]
    if "relations" in schema:
        schema["relations"] = [{"type": name} for name in schema["relations"]]
    if "structures" in schema:
        schema["structures"] = {
            name: {
                **{key: value for key, value in spec.items() if key != "fields"},
                "fields": {
                    field["name"]: {
                        ("type" if key == "dtype" else key): value
                        for key, value in field.items()
                        if key != "name"
                    }
                    for field in spec["fields"]
                },
            }
            for name, spec in schema["structures"].items()
        }
    return schema


def canonical_labels(value: Any) -> list[dict[str, Any]]:
    if value is None:
        return []
    if isinstance(value, dict) and "value" in value:
        chosen = value["value"] if isinstance(value["value"], list) else [value["value"]]
        probabilities = value.get("probabilities", {})
        return [
            {"label": label, "confidence": probabilities.get(label, value.get("confidence"))}
            for label in chosen
        ]
    items = value if isinstance(value, list) else [value]
    return [
        {"label": item.get("label", item.get("value")), "confidence": item["confidence"]}
        for item in items
    ]


def canonical_values(value: Any, attributes: tuple[str, ...] = ()) -> list[dict[str, Any]]:
    if value is None:
        return []
    items = value if isinstance(value, list) else [value]
    return [
        {
            "text": item["text"],
            "confidence": item["confidence"],
            "source": (
                {"start": item["start"], "end": item["end"]}
                if "start" in item
                else None
            ),
            "attributes": [
                {"name": name, "labels": canonical_labels(item[name])}
                for name in attributes
                if name in item
            ],
        }
        for item in items
    ]


def canonical_expected(
    request: dict[str, Any], native_schema: dict[str, Any], output: dict[str, Any]
) -> dict[str, Any]:
    expected: dict[str, list[Any]] = {
        "entities": [],
        "classifications": [],
        "structures": [],
        "relations": [],
    }
    for entity in native_schema.get("entities", []):
        expected["entities"].append({
            "name": entity,
            "values": canonical_values(
                output.get("entities", {}).get(entity),
                tuple(native_schema.get("entity_attributes", {})),
            ),
        })
    for task in native_schema.get("classifications", []):
        expected["classifications"].append({
            "name": task["name"],
            "labels": canonical_labels(output.get(task["name"])),
        })
    for name, structure in native_schema.get("structures", {}).items():
        instances = [
            {
                "fields": [
                    {"name": field, "values": canonical_values(record.get(field))}
                    for field in structure["fields"]
                ]
            }
            for record in output.get(name, [])
        ]
        if instances:
            expected["structures"].append({"name": name, "instances": instances})
    for relation, edges in output.get("relation_extraction", {}).items():
        for edge in edges:
            head = canonical_values(edge["head"])[0]
            tail = canonical_values(edge["tail"])[0]
            expected["relations"].append({
                "name": relation,
                "head": head,
                "tail": tail,
                "confidence": edge.get("confidence", edge["head"]["confidence"]),
            })
    return expected


def semantic_result(expect: dict[str, Any], output: Any, selected: dict[str, list[str]] | None) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []
    if "contains_text" in expect:
        found = all_strings(output)
        for text in expect["contains_text"]:
            checks.append({"kind": "contains_text", "value": text, "pass": text in found})
    if "selected" in expect:
        for task, labels in expect["selected"].items():
            checks.append({
                "kind": "selected",
                "task": task,
                "expected": labels,
                "actual": None if selected is None else selected.get(task),
                "pass": selected is not None and selected.get(task) == labels,
            })
    if "selected_contains" in expect:
        for task, labels in expect["selected_contains"].items():
            actual = [] if selected is None else selected.get(task, [])
            checks.append({
                "kind": "selected_contains",
                "task": task,
                "expected": labels,
                "actual": actual,
                "pass": all(label in actual for label in labels),
            })
    if not checks:
        raise CaptureError("semantic expectation did not define a known check")
    return {"pass": all(check["pass"] for check in checks), "checks": checks}


def capture_requests(model: Any, requests: list[dict[str, Any]], torch: Any) -> list[dict[str, Any]]:
    from gliner2 import Schema
    from gliner2.classification import Classifier, ClassificationConfig, ClassificationSchema

    rows: list[dict[str, Any]] = []
    classifier = Classifier(model)
    for request in requests:
        text = request["text"]
        native_schema = native_schema_for(request)
        if request["kind"] == "extract":
            schema = Schema.from_dict(request["schema"])
            encoded = encoded_evidence(model, text, schema, boundary=True)
            with torch.inference_mode():
                output = model.extract(
                    text,
                    schema,
                    threshold=0.5,
                    include_confidence=True,
                    include_spans=True,
                    max_len=MAX_WORDS,
                )
            output = jsonable(output)
            selected = None
            detail: dict[str, Any] = {}
        else:
            schema = ClassificationSchema.from_dict(request["schema"])
            compiled = classifier.compile_schema(schema)
            encoded = encoded_evidence(model, text, compiled, boundary=False)
            config = ClassificationConfig(
                on_infeasible="raise", max_len=MAX_WORDS, include_confidence=True
            )
            with torch.inference_mode():
                scores = classifier.score(text, compiled, config=config)
                result = classifier.decode(scores, compiled, config=config)
            output = jsonable(result.to_dict(include_confidence=True))
            selected = {
                task: list(result.selected(task)) for task in compiled.task_order
            }
            detail = {
                "logits": jsonable(scores.tasks),
                "probabilities": {
                    task: {
                        label: scores.probability(task, label)
                        for label in scores.tasks[task]
                    }
                    for task in scores.tasks
                },
                "selected": selected,
                "schema_fingerprint": compiled.fingerprint,
                "native_classification": {
                    "input_ids": encoded["input_ids"],
                    "tasks": [
                        {
                            "name": task,
                            "labels": list(scores.tasks[task]),
                            "raw_logits": list(scores.tasks[task].values()),
                        }
                        for task in compiled.task_order
                    ],
                },
            }
        rows.append({
            "id": request["id"],
            "kind": request["kind"],
            "text": text,
            "schema": request["schema"],
            "native_schema": native_schema,
            # Prompt construction is order-sensitive for maps such as
            # structure fields.  The report itself is key-sorted for stable
            # provenance, so retain the exact native request bytes separately.
            "native_schema_json": json.dumps(
                native_schema,
                ensure_ascii=False,
                allow_nan=False,
                separators=(",", ":"),
                sort_keys=False,
            ),
            "native_options": {
                "offset_unit": "unicode_codepoints",
                "include_confidence": True,
                "include_spans": True,
            },
            "encoded": encoded,
            "output": output,
            "native_expected": canonical_expected(request, native_schema, output),
            **detail,
            "semantic": semantic_result(request["expect"], output, selected),
        })
    return rows


def capture(args: argparse.Namespace) -> dict[str, Any]:
    if os.environ.get("PYTHONHASHSEED") != "0":
        raise CaptureError("capture requires PYTHONHASHSEED=0")
    contract = family.strict_json(args.contract)
    source = verify_source(args.upstream, contract)
    runtime = runtime_identity(contract)
    requests = validate_requests(args.requests)
    model_before = family.verify_model(
        args.profile,
        args.model_dir,
        contract_path=args.contract,
        verify_model_sha256=True,
    )

    sys.dont_write_bytecode = True
    os.environ.update({
        "HF_HUB_OFFLINE": "1",
        "TRANSFORMERS_OFFLINE": "1",
        "TOKENIZERS_PARALLELISM": "false",
        "OMP_NUM_THREADS": str(args.threads),
        "MKL_NUM_THREADS": str(args.threads),
    })
    os.environ.pop("USE_FLASHDEBERTA", None)
    sys.path.insert(0, str(args.upstream.resolve()))
    import torch
    import gliner2
    from gliner2 import AutoExtractor

    compatibility = install_inference_peft_shim()

    if gliner2.__version__ != "2.0.0":
        raise CaptureError(f"unexpected upstream package version {gliner2.__version__}")
    torch.set_num_threads(args.threads)
    torch.set_num_interop_threads(1)
    torch.use_deterministic_algorithms(True)
    torch.set_default_dtype(torch.float32)
    torch.manual_seed(0)

    model = AutoExtractor.from_pretrained(
        str(args.model_dir.resolve()),
        local_files_only=True,
        map_location="cpu",
        use_flashdeberta=False,
    ).float().cpu().eval()
    if getattr(model, "architecture", None) != "boundary":
        raise CaptureError("family reference capture requires a boundary checkpoint")
    rows = capture_requests(model, requests["requests"], torch)
    imports = {
        name: str(Path(module.__file__).resolve())
        for name, module in sys.modules.items()
        if (name == "gliner2" or name.startswith("gliner2."))
        and getattr(module, "__file__", None)
    }
    checkout = args.upstream.resolve()
    if not imports or any(not Path(path).is_relative_to(checkout) for path in imports.values()):
        raise CaptureError("GLiNER2 imported outside the pinned checkout")

    # Re-hash after inference so a concurrent artifact change cannot be hidden
    # behind an unchanged sidecar/header identity.
    model_after = family.verify_model(
        args.profile,
        args.model_dir,
        contract_path=args.contract,
        verify_model_sha256=True,
    )
    if model_after != model_before:
        raise CaptureError("model identity changed during capture")
    verify_source(args.upstream, contract)

    report = {
        "format_version": 1,
        "status": "captured",
        "scope": "upstream_multilingual_family_reference",
        "qualification": False,
        "native_runtime_qualified": False,
        "production_qualified": False,
        "semantic_pass": all(row["semantic"]["pass"] for row in rows),
        "model": model_before,
        "source": {**source, "package_version": gliner2.__version__, "imports": imports},
        "runtime": {
            **runtime,
            "platform": {"system": platform.system(), "machine": platform.machine()},
            "device": "cpu",
            "dtype": "float32",
            "threads": args.threads,
            "pythonhashseed": 0,
            "compatibility": [compatibility],
        },
        "artifacts": {
            "generator_sha256": sha256_file(Path(__file__)),
            "contract_sha256": sha256_file(args.contract),
            "requests_sha256": sha256_file(args.requests),
        },
        "requests": rows,
    }
    destination = args.output.expanduser().absolute()
    if destination.exists() or destination.is_symlink():
        raise CaptureError(f"refusing to overwrite output: {destination}")
    destination.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{destination.name}-", dir=destination.parent))
    try:
        (staging / "capture.json").write_text(
            json.dumps(report, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False)
            + "\n",
            encoding="utf-8",
        )
        staging.rename(destination)
    finally:
        if staging.exists():
            shutil.rmtree(staging)
    return {
        "status": "captured",
        "profile": args.profile,
        "output": str(destination),
        "requests": len(rows),
        "semantic_pass": report["semantic_pass"],
        "qualification": False,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", choices=("multi_v1", "multi_decide"), required=True)
    parser.add_argument("--model-dir", type=Path, required=True)
    parser.add_argument("--upstream", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--requests", type=Path, default=DEFAULT_REQUESTS)
    parser.add_argument("--contract", type=Path, default=family.CONTRACT_PATH)
    parser.add_argument("--threads", type=int, default=1)
    args = parser.parse_args()
    try:
        if not 1 <= args.threads <= 8:
            raise CaptureError("threads must be in 1..8")
        result = capture(args)
        print(json.dumps(result, sort_keys=True, allow_nan=False))
        return 0
    except (
        CaptureError,
        family.ContractError,
        OSError,
        ImportError,
        RuntimeError,
        ValueError,
        KeyError,
        TypeError,
        subprocess.TimeoutExpired,
    ) as exc:
        print(json.dumps({
            "status": "error",
            "error_type": type(exc).__name__,
            "error": str(exc),
            "qualification": False,
        }, sort_keys=True), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
