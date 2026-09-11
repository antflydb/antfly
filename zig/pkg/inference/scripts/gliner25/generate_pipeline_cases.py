#!/usr/bin/env python3
"""Adapt a verified, pinned variant capture to canonical native schema cases.

This command imports no ML runtime and performs no model download or inference.
It verifies the capture's provenance and all captured tensor hashes before
publishing expectations. The native consumer rehashes the actual model bundle.
"""
from __future__ import annotations
import argparse
import hashlib
import json
from pathlib import Path

import oracle


ROOT = Path(__file__).resolve().parents[2] / "testdata" / "gliner25"


def schema_for(request):
    original = request["schema"]
    if request["kind"] == "joint_ie":
        return {"joint_ie": {"entities": {name: {} for name in original["entities"]}, "relations": original["relations"]}}
    if request["kind"] == "classification":
        return {"classifications": [{"name": name, **config} for name, config in original["tasks"].items()],
                "classification_constraints": original["constraints"]}
    schema = dict(original)
    if isinstance(schema.get("entities"), dict):
        schema["entity_definitions"] = {name: {"description": desc} for name, desc in schema["entities"].items()}
        schema["entities"] = list(schema["entities"])
    if "classifications" in schema:
        schema["classifications"] = [{"name": c["task"], **{k: v for k, v in c.items() if k != "task"}} for c in schema["classifications"]]
    if "relations" in schema:
        schema["relations"] = [{"type": name} for name in schema["relations"]]
    if "structures" in schema:
        schema["structures"] = {name: {**{k: v for k, v in spec.items() if k != "fields"},
                                              "fields": {f["name"]: {("type" if k == "dtype" else k): v for k, v in f.items() if k != "name"} for f in spec["fields"]}}
                                for name, spec in schema["structures"].items()}
    return schema


def labels(value):
    if value is None:
        return []
    if isinstance(value, dict) and isinstance(value.get("value"), list):
        return [{"label": label, "confidence": value["probabilities"][label]} for label in value["value"]]
    return [{"label": item.get("label", item.get("value")), "confidence": item["confidence"]}
            for item in (value if isinstance(value, list) else [value])]


def values(value, attributes=()):
    if value is None:
        return []
    return [{"text": item["text"], "confidence": item["confidence"],
             "source": {"start": item["start"], "end": item["end"]} if "start" in item else None,
             "attributes": [{"name": name, "labels": labels(item[name])} for name in attributes if name in item]}
            for item in (value if isinstance(value, list) else [value])]


def verified_capture(model: str, capture_dir: Path, requests_path: Path):
    manifest = oracle.load_manifest()
    pinned = manifest["models"][model]
    reference = oracle.read_json(capture_dir / "capture.json")
    if reference.get("format_version") != 1 or reference.get("status") != "captured" or reference.get("scope") != "pretrained_reference":
        raise oracle.ContractError("pipeline expectations require a completed pretrained capture")
    if any(reference.get(key) is not False for key in ("native_runtime_qualified", "real_model_qualified", "training_qualified")):
        raise oracle.ContractError("oracle capture must not claim runtime qualification")
    if reference.get("generator_sha256") != oracle.sha256_file(oracle.HERE / "oracle.py") or reference.get("manifest_sha256") != oracle.sha256_file(oracle.HERE / "oracle_manifest.json"):
        raise oracle.ContractError("capture generator or model manifest is not pinned")
    provenance = reference.get("provenance", {})
    if provenance.get("commit") != manifest["upstream"]["commit"] or provenance.get("runtime") != manifest["runtime"] or provenance.get("device") != "cpu" or provenance.get("dtype") != "float32" or provenance.get("threads") != 1:
        raise oracle.ContractError("capture source/runtime profile is not pinned")
    actual = reference.get("model", {})
    if actual.get("model_id") != pinned["model_id"] or actual.get("revision") != pinned["revision"]:
        raise oracle.ContractError("capture belongs to a different model variant or revision")
    expected_files = {name: {key: spec[key] for key in ("sha256", "size_bytes")} for name, spec in pinned["files"].items()}
    if actual.get("files") != expected_files:
        raise oracle.ContractError("capture model bundle identity is not pinned")
    if reference.get("requests_sha256") != oracle.sha256_file(requests_path):
        raise oracle.ContractError("capture requests do not match the requested fixture")
    for row in reference["requests"]:
        if tensor := row.get("tensor_capture"):
            filename = tensor["file"]
            if Path(filename).name != filename or not filename.endswith(".safetensors"):
                raise oracle.ContractError("capture tensor filename must be a safe basename")
            oracle.verify_file(capture_dir / filename, tensor)
    return reference, pinned


def generate(model: str, capture_dir: Path, requests_path: Path):
    requests = oracle.read_json(requests_path)["requests"]
    reference, pinned = verified_capture(model, capture_dir, requests_path)
    by_id = {row["id"]: row["output"] for row in reference["requests"]}
    if len(by_id) != len(reference["requests"]) or len({row["id"] for row in requests}) != len(requests) or set(by_id) != {row["id"] for row in requests}:
        raise oracle.ContractError("capture must contain exactly one output for every request")
    if {row["id"]: row["kind"] for row in reference["requests"]} != {row["id"]: row["kind"] for row in requests}:
        raise oracle.ContractError("capture request kinds do not match")
    cases = []
    for request in requests:
        schema = schema_for(request)
        output = by_id[request["id"]]
        expected = {"entities": [], "classifications": [], "structures": [], "relations": []}
        if request["kind"] == "joint_ie":
            for entity in request["schema"]["entities"]:
                expected["entities"].append({"name": entity, "values": values([v for v in output["entities"] if v["type"] == entity])})
            entities = {entity["id"]: entity for entity in output["entities"]}
            if len(entities) != len(output["entities"]):
                raise oracle.ContractError("JointIE capture contains duplicate entity IDs")
            entity_types = list(request["schema"]["entities"])
            for edge in output["relations"]:
                head, tail = entities[edge["head"]], entities[edge["tail"]]
                expected["relations"].append({"name": edge["type"], "head": values(head)[0], "tail": values(tail)[0],
                                               "confidence": edge["confidence"], "derived": edge.get("derived", False),
                                               "head_entity_type": entity_types.index(head["type"]),
                                               "tail_entity_type": entity_types.index(tail["type"])})
        else:
            for entity in schema.get("entities", []):
                expected["entities"].append({"name": entity, "values": values(output["entities"][entity], schema.get("entity_attributes", {}))})
            for task in schema.get("classifications", []):
                expected["classifications"].append({"name": task["name"], "labels": labels(output[task["name"]])})
            for name, structure in schema.get("structures", {}).items():
                instances = [{"fields": [{"name": f, "values": values(record[f])} for f in structure["fields"]]} for record in output.get(name, [])]
                if instances:
                    expected["structures"].append({"name": name, "instances": instances})
            for relation, edges in output.get("relation_extraction", {}).items():
                for edge in edges:
                    expected["relations"].append({"name": relation, "head": values(edge["head"])[0], "tail": values(edge["tail"])[0], "confidence": edge["head"]["confidence"]})
        cases.append({"id": request["id"], "text": request["text"], "schema": schema, "expected": expected})
    return {"format_version": 2, "source_commit": reference["provenance"]["commit"],
              "model": model, "model_id": pinned["model_id"], "revision": pinned["revision"],
              "model_files": reference["model"]["files"],
              "model_sha256": reference["model"]["files"]["model.safetensors"]["sha256"],
              "tokenizer_sha256": reference["model"]["files"]["tokenizer.json"]["sha256"],
              "reference_sha256": hashlib.sha256((capture_dir / "capture.json").read_bytes()).hexdigest(),
              "requests_sha256": reference["requests_sha256"],
              "offset_unit": "unicode_codepoints", "cases": cases}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model", choices=("base", "small", "multi"), default="small")
    parser.add_argument("--capture-dir", type=Path)
    parser.add_argument("--requests", type=Path, default=ROOT / "requests.json")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    capture_dir = args.capture_dir or ROOT / f"{args.model}_reference"
    output = args.output or ROOT / ("pipeline_cases.json" if args.model == "small" else f"pipeline_cases_{args.model}.json")
    result = generate(args.model, capture_dir, args.requests)
    # Schema-map insertion order influences prompt construction (especially
    # structure fields). Provenance JSON's generic sort_keys writer is not
    # suitable for this canonical request artifact.
    output.write_text(json.dumps(result, ensure_ascii=False, indent=2, allow_nan=False) + "\n", encoding="utf-8")
    print(json.dumps({"model": args.model, "cases": len(result["cases"]), "output": str(output)}))


if __name__ == "__main__":
    main()
