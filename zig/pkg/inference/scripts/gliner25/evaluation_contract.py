#!/usr/bin/env python3
"""Offline held-out data admission, blinded requests and exact-fact scoring.

This contract runs no model and downloads no data. Dataset-specific adapters,
official scorers, calibration and release thresholds remain separate work.
"""
from __future__ import annotations

import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path
import unicodedata
from typing import Any, Iterator

import oracle

SCOPE = "gliner25_evaluation_lock/v1"
SPLITS = ("train", "calibration", "test")
MAX_FILE_BYTES = 512 * 1024 * 1024
MAX_LINE_BYTES = 2 * 1024 * 1024
MAX_DOCUMENTS = 100000
MAX_FACTS = 4096


class EvaluationError(ValueError):
    pass


def encoded(value: Any, *, sorted_keys: bool = False) -> bytes:
    return json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(",", ":"), sort_keys=sorted_keys).encode("utf-8")


def digest(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def checked(condition: bool, message: str) -> None:
    if not condition:
        raise EvaluationError(message)


def is_digest(value: Any) -> bool:
    return isinstance(value, str) and len(value) == 64 and all(char in "0123456789abcdef" for char in value)


def pinned_path(root: Path, pin: dict[str, Any]) -> Path:
    checked(isinstance(pin, dict) and set(pin) == {"path", "size_bytes", "sha256"}, "invalid file pin")
    relative = Path(pin["path"])
    checked(not relative.is_absolute() and ".." not in relative.parts, "file escapes lock directory")
    path = (root / relative).resolve(strict=True)
    checked(path.is_relative_to(root.resolve()) and path.is_file(), "file escapes lock directory")
    checked(type(pin["size_bytes"]) is int and 0 < pin["size_bytes"] <= MAX_FILE_BYTES and is_digest(pin["sha256"]), "invalid file identity")
    checked(path.stat().st_size == pin["size_bytes"] and oracle.sha256_file(path) == pin["sha256"], "file content differs from lock")
    return path


def rows(path: Path) -> Iterator[dict[str, Any]]:
    with path.open("rb") as source:
        while line := source.readline(MAX_LINE_BYTES + 1):
            checked(len(line) <= MAX_LINE_BYTES and line.endswith(b"\n"), "oversized or unterminated JSONL record")
            try:
                value = json.loads(line, object_pairs_hook=oracle._unique_object,
                                   parse_constant=lambda _: (_ for _ in ()).throw(EvaluationError("nonfinite JSON")))
            except (UnicodeError, json.JSONDecodeError) as error:
                raise EvaluationError("invalid JSONL record") from error
            checked(isinstance(value, dict), "JSONL record must be an object")
            yield value


def metric_facts(metrics: Any, specs: dict[str, Any], text: str) -> None:
    checked(isinstance(metrics, dict) and set(metrics) == set(specs), "missing or undeclared metric family")
    byte_text = text.encode("utf-8")
    boundaries = {0}
    end = 0
    for character in text:
        end += len(character.encode("utf-8"))
        boundaries.add(end)
    count = 0

    def visit(value: Any, container: str | None = None) -> None:
        if isinstance(value, dict):
            if container in ("span", "anchor"):
                checked(type(value.get("start")) is int and type(value.get("end")) is int and
                        value["start"] in boundaries and value["end"] in boundaries and value["start"] < value["end"], "invalid UTF-8 gold/prediction span")
                if "text" in value:
                    checked(value["text"] == byte_text[value["start"]:value["end"]].decode(), "span text differs from immutable document")
            for key, item in value.items():
                visit(item, key)
        elif isinstance(value, list):
            for item in value:
                visit(item)
        else:
            checked(value is None or type(value) in (str, int, bool), "exact facts cannot contain floating scores")

    for name, facts in metrics.items():
        checked(isinstance(facts, list), "metric facts must be lists")
        count += len(facts)
        checked(count <= MAX_FACTS, "fact budget exceeded")
        for fact in facts:
            checked(isinstance(fact, dict), "metric facts must be objects")
            visit(fact)
        if specs[name]["counting"] == "set":
            checked(len({encoded(fact, sorted_keys=True) for fact in facts}) == len(facts), "duplicate set fact")


def audit(lock_path: Path) -> dict[str, Any]:
    lock = oracle.read_json(lock_path)
    checked(lock.get("scope") == SCOPE and lock.get("status") == "locked" and lock.get("qualification") is False,
            "evaluation requires a locked manifest, not the metadata catalog")
    checked(lock.get("upstream_commit") == oracle.UPSTREAM_COMMIT and lock.get("unicode_version") == unicodedata.unidata_version,
            "source or Unicode audit profile differs")
    checked(lock.get("harness_sha256") == oracle.sha256_file(Path(__file__)), "evaluation harness differs from lock")
    checked(lock.get("schema_selection") == "fixed_before_test" and lock.get("test_used_for_tuning") is False,
            "schema and tuning policy is not held out")
    checked(is_digest(lock.get("adapter_sha256")) and is_digest(lock.get("metric_contract_sha256")), "adapter or metric contract is not pinned")
    metrics = lock.get("metrics")
    checked(isinstance(metrics, dict) and bool(metrics) and len(metrics) <= 64, "invalid metric inventory")
    for spec in metrics.values():
        checked(isinstance(spec, dict) and spec.get("counting") in ("set", "multiset") and isinstance(spec.get("definition"), str) and bool(spec["definition"]),
                "metric semantics must be explicit")
    root = lock_path.parent
    checked(pinned_path(root, lock.get("adapter_file", {})).is_file() and lock["adapter_file"]["sha256"] == lock["adapter_sha256"], "adapter identity differs")
    metric_path = pinned_path(root, lock.get("metric_contract_file", {}))
    checked(lock["metric_contract_file"]["sha256"] == lock["metric_contract_sha256"] and oracle.read_json(metric_path) == metrics, "metric contract identity differs")
    schemas: dict[str, Any] = {}
    schema_specs = lock.get("schemas", [])
    checked(isinstance(schema_specs, list) and 0 < len(schema_specs) <= 1024, "schema count exceeds admission")
    for spec in schema_specs:
        checked(isinstance(spec.get("id"), str) and bool(spec["id"]) and spec["id"] not in schemas, "duplicate or invalid schema identity")
        checked(spec.get("origin") in ("public_ontology", "training_only", "external_business_contract"), "schema cannot originate from test gold")
        path = pinned_path(root, spec["file"])
        checked(spec["file"]["size_bytes"] <= MAX_LINE_BYTES, "schema exceeds byte limit")
        body = oracle.read_json(path)
        checked(isinstance(body, dict) and bool(body), "empty schema")
        schemas[spec["id"]] = body
    checked(bool(schemas), "no fixed schema")
    checked(isinstance(lock.get("request_options"), dict), "request options must be frozen")
    checked(lock.get("offset_unit") == "utf8_bytes", "canonical evaluation coordinates must be UTF-8 bytes")
    checked(isinstance(lock.get("source_files"), list) and bool(lock["source_files"]), "raw corpus evidence is missing")
    for pin in lock["source_files"]:
        pinned_path(root, pin)
    checked(isinstance(lock.get("supporting_files", []), list), "invalid supporting file inventory")
    for pin in lock.get("supporting_files", []):
        pinned_path(root, pin)
    split_counts = Counter()
    families: dict[str, str] = {}
    contents: dict[str, str] = {}
    identifiers = set()
    files = []
    documents = 0
    for spec in lock.get("splits", []):
        split = spec.get("split")
        checked(split in SPLITS and type(spec.get("records")) is int and spec["records"] > 0, "invalid split")
        path = pinned_path(root, spec["file"])
        files.append((spec, path))
        seen = 0
        for row in rows(path):
            documents += 1
            seen += 1
            checked(documents <= MAX_DOCUMENTS, "document admission exceeded")
            checked(set(row) == {"id", "family_id", "language", "schema_id", "text", "gold"}, "unexpected normalized record fields")
            checked(all(isinstance(row[key], str) and bool(row[key]) for key in ("id", "family_id", "language", "schema_id", "text")), "invalid record identity")
            checked(row["schema_id"] in schemas and row["id"] not in identifiers, "unknown schema or duplicate record ID")
            identifiers.add(row["id"])
            checked(families.setdefault(row["family_id"], split) == split, "document/translation family crosses held-out splits")
            # Normalization is for duplicate auditing only. Model text and
            # source coordinates always retain the original bytes.
            normalized = " ".join(unicodedata.normalize("NFC", row["text"]).casefold().split())
            checked(contents.setdefault(digest(normalized.encode()), split) == split, "normalized document crosses held-out splits")
            metric_facts(row["gold"], metrics, row["text"])
            split_counts[split] += 1
        checked(seen == spec["records"], "declared split coverage differs")
        pinned_path(root, spec["file"])
    checked(split_counts["test"] > 0, "test split is missing")
    return {"lock": lock, "schemas": schemas, "files": files,
            "summary": {"scope": "gliner25_evaluation_admission/v1", "qualification": False,
                        "lock_sha256": oracle.sha256_file(lock_path), "documents": documents,
                        "split_counts": dict(split_counts), "families": len(families),
                        "contamination_free_claim": False}}


def prepare(lock_path: Path, destination: Path) -> dict[str, Any]:
    admitted = audit(lock_path)
    lock = admitted["lock"]
    destination.mkdir(mode=0o700, parents=False, exist_ok=False)
    count = 0
    with (destination / "requests.jsonl").open("xb") as requests, (destination / "gold.jsonl").open("xb") as gold:
        for spec, path in admitted["files"]:
            if spec["split"] != "test":
                continue
            for row in rows(path):
                # No gold, source-dataset label fields, per-example candidate
                # types or learned instance identities enter inference input.
                request = {"text": row["text"], "schema": admitted["schemas"][row["schema_id"]],
                           "options": lock["request_options"], "offset_unit": "utf8_bytes"}
                request_id = digest(encoded({"lock": admitted["summary"]["lock_sha256"], "id": row["id"]}))
                common = {"request_id": request_id, "request_sha256": digest(encoded(request))}
                requests.write(encoded({**common, "request": request}) + b"\n")
                gold.write(encoded({**common, "family_id": row["family_id"], "language": row["language"], "metrics": row["gold"]}) + b"\n")
                count += 1
    for spec, _ in admitted["files"]:
        pinned_path(lock_path.parent, spec["file"])
    for spec in lock["schemas"]:
        pinned_path(lock_path.parent, spec["file"])
    for pin in [lock["adapter_file"], lock["metric_contract_file"], *lock["source_files"], *lock.get("supporting_files", [])]:
        pinned_path(lock_path.parent, pin)
    checked(oracle.sha256_file(lock_path) == admitted["summary"]["lock_sha256"], "evaluation lock changed during preparation")
    receipt = {"scope": "gliner25_blinded_evaluation/v1", "status": "complete", "qualification": False,
               "harness_sha256": lock["harness_sha256"],
               "lock_sha256": admitted["summary"]["lock_sha256"], "adapter_sha256": lock["adapter_sha256"],
               "metric_contract_sha256": lock["metric_contract_sha256"], "metrics": lock["metrics"], "records": count,
               "requests_sha256": oracle.sha256_file(destination / "requests.jsonl"),
               "gold_sha256": oracle.sha256_file(destination / "gold.jsonl")}
    oracle.write_json(destination / "prepared.json", receipt)
    return receipt


def score(prepared: Path, predictions: Path) -> dict[str, Any]:
    receipt = oracle.read_json(prepared / "prepared.json")
    checked(receipt.get("scope") == "gliner25_blinded_evaluation/v1" and receipt.get("status") == "complete", "incomplete evaluation input")
    checked(receipt.get("harness_sha256") == oracle.sha256_file(Path(__file__)), "evaluation harness differs from prepared evidence")
    checked(oracle.sha256_file(prepared / "requests.jsonl") == receipt["requests_sha256"] and
            oracle.sha256_file(prepared / "gold.jsonl") == receipt["gold_sha256"], "prepared evidence was modified")
    prediction_hash = oracle.sha256_file(predictions)
    totals = {name: Counter() for name in receipt["metrics"]}
    count = 0
    request_iter, gold_iter, prediction_iter = rows(prepared / "requests.jsonl"), rows(prepared / "gold.jsonl"), rows(predictions)
    while True:
        request, gold, prediction = next(request_iter, None), next(gold_iter, None), next(prediction_iter, None)
        if request is None and gold is None and prediction is None:
            break
        checked(request is not None and gold is not None and prediction is not None, "prediction coverage differs; failures cannot be dropped")
        checked(set(prediction) == {"request_id", "request_sha256", "metrics"}, "prediction protocol differs")
        checked(all(request[key] == gold[key] == prediction[key] for key in ("request_id", "request_sha256")), "prediction request identity or ordering differs")
        checked(request["request_sha256"] == digest(encoded(request["request"])), "prepared request content differs")
        metric_facts(prediction["metrics"], receipt["metrics"], request["request"]["text"])
        for name in totals:
            wanted = Counter(encoded(fact, sorted_keys=True) for fact in gold["metrics"][name])
            found = Counter(encoded(fact, sorted_keys=True) for fact in prediction["metrics"][name])
            totals[name].update(tp=sum((wanted & found).values()), fp=sum((found - wanted).values()),
                                fn=sum((wanted - found).values()), documents=1, exact_documents=int(wanted == found),
                                zero_gold_documents=int(not wanted), zero_gold_predicted_documents=int(not wanted and bool(found)))
        count += 1
        checked(count <= MAX_DOCUMENTS, "prediction admission exceeded")
    checked(count == receipt["records"] and count > 0, "empty or incomplete prediction set")
    checked(prediction_hash == oracle.sha256_file(predictions), "predictions changed while scoring")
    checked(oracle.sha256_file(prepared / "requests.jsonl") == receipt["requests_sha256"] and
            oracle.sha256_file(prepared / "gold.jsonl") == receipt["gold_sha256"], "prepared evidence changed while scoring")
    for name, counts in totals.items():
        tp, fp, fn = counts["tp"], counts["fp"], counts["fn"]
        counts["precision"] = tp / (tp + fp) if tp + fp else 0.0
        counts["recall"] = tp / (tp + fn) if tp + fn else 0.0
        counts["micro_f1"] = 2 * tp / (2 * tp + fp + fn) if 2 * tp + fp + fn else 0.0
        counts["document_exact_match"] = counts["exact_documents"] / count
        counts["support"] = tp + fn
        counts["absent_query_false_positive_rate"] = counts["zero_gold_predicted_documents"] / counts["zero_gold_documents"] if counts["zero_gold_documents"] else None
    return {"scope": "gliner25_heldout_exact_fact_metrics/v1", "qualification": False, "records": count,
            "lock_sha256": receipt["lock_sha256"], "prediction_sha256": prediction_hash,
            "prepared_sha256": oracle.sha256_file(prepared / "prepared.json"),
            "metrics": {name: dict(counts) for name, counts in totals.items()}}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    audit_parser = commands.add_parser("audit")
    audit_parser.add_argument("--lock", type=Path, required=True)
    prepare_parser = commands.add_parser("prepare")
    prepare_parser.add_argument("--lock", type=Path, required=True)
    prepare_parser.add_argument("--output-dir", type=Path, required=True)
    score_parser = commands.add_parser("score")
    score_parser.add_argument("--prepared-dir", type=Path, required=True)
    score_parser.add_argument("--predictions", type=Path, required=True)
    score_parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.command == "audit":
        print(json.dumps(audit(args.lock)["summary"], indent=2))
    elif args.command == "prepare":
        print(json.dumps(prepare(args.lock, args.output_dir), indent=2))
    else:
        result = score(args.prepared_dir, args.predictions)
        with args.output.open("xb") as output:
            output.write(encoded(result) + b"\n")


if __name__ == "__main__":
    main()
