#!/usr/bin/env python3
"""Prepare the immutable CrossNER AI holdout without executing source code.

All model queries use the entire author-declared ontology, including absent
types and miscellaneous. Raw BIO tokens are joined with one ASCII space.
"""
from __future__ import annotations

import argparse
import ast
from collections import defaultdict
import hashlib
import math
from pathlib import Path
import unicodedata
from typing import Any

import evaluation_contract as evaluation
import oracle

MANIFEST = Path(__file__).with_name("crossner_ai_manifest.json")
SCHEMA_ID = "crossner_ai_full_verbatim_ontology_v1"
SPLIT_POLICY = "preserve_official_test_remove_lower_priority_normalized_overlaps/v1"


def ontology(source: str) -> list[str]:
    """Read a literal declaration; importing dataloader would execute code."""
    candidates = [node.value for node in ast.parse(source).body if isinstance(node, ast.Assign) and
                  any(isinstance(target, ast.Name) and target.id == "ai_labels" for target in node.targets)]
    evaluation.checked(len(candidates) == 1, "author ontology declaration differs")
    labels = ast.literal_eval(candidates[0])
    evaluation.checked(isinstance(labels, list) and labels[0] == "O", "invalid author ontology")
    names = []
    for label in labels[1:]:
        evaluation.checked(isinstance(label, str) and label.startswith(("B-", "I-")), "invalid BIO ontology")
        if label.startswith("B-"):
            evaluation.checked(label[2:] not in names, "duplicate author ontology type")
            names.append(label[2:])
    evaluation.checked(set(labels) == {"O", *[prefix + name for name in names for prefix in ("B-", "I-")]}, "incomplete BIO ontology")
    return names


def parse_bio(raw: str, names: list[str]) -> list[tuple[str, list[dict[str, Any]], int]]:
    documents = []
    tokens, labels = [], []

    def finish():
        if not tokens:
            return
        text = " ".join(tokens)
        offsets = []
        byte_end = 0
        for token in tokens:
            offsets.append((byte_end, byte_end + len(token.encode("utf-8"))))
            byte_end += len(token.encode("utf-8")) + 1
        facts = []
        active = None
        start = 0

        def close(end):
            if active is not None:
                source_start, source_end = offsets[start][0], offsets[end - 1][1]
                facts.append({"type": active, "span": {"start": source_start, "end": source_end,
                              "text": text.encode("utf-8")[source_start:source_end].decode("utf-8")}})

        for index, label in enumerate([*labels, "O"]):
            if label == "O":
                close(index)
                active = None
            elif label.startswith("B-"):
                close(index)
                active, start = label[2:], index
            else:
                evaluation.checked(active == label[2:], "orphan or type-changing BIO continuation")
        documents.append((text, facts, len(tokens)))
        tokens.clear()
        labels.clear()

    allowed = {"O", *[prefix + name for name in names for prefix in ("B-", "I-")]}
    for line in raw.splitlines():
        if not line.strip():
            finish()
            continue
        columns = line.split("\t")
        evaluation.checked(len(columns) == 2 and bool(columns[0]) and not columns[0].isspace() and columns[1] in allowed,
                           "malformed BIO source; labels may not be silently discarded")
        tokens.append(columns[0])
        labels.append(columns[1])
    finish()
    evaluation.checked(bool(documents), "empty source split")
    return documents


def metric_definitions(names):
    return {"entity_exact": {"counting": "set", "definition": "Exact author type and span in single-space reconstructed UTF-8 source"},
            **{"entity_type/" + name: {"counting": "set", "definition": "Exact occurrence for fixed ontology type " + name} for name in names}}


def facts_by_metric(facts, names):
    return {"entity_exact": facts, **{"entity_type/" + name: [fact for fact in facts if fact["type"] == name] for name in names}}


def separate_splits(parsed):
    """Use text identity only; never use labels to retain or exclude a row.

    Keep the entire official test split, including its duplicate occurrences.
    The lower-priority calibration/train exclusions are a declared derivation.
    Annotation differences are diagnostics and never affect this decision.
    """
    priority = {"train": 0, "dev": 1, "test": 2}
    groups = defaultdict(list)
    for split in priority:
        for index, (text, facts, _) in enumerate(parsed[split]):
            normalized = " ".join(unicodedata.normalize("NFC", text).casefold().split())
            groups[evaluation.digest(normalized.encode())].append({
                "id": f"crossner/ai/{split}/{index:06d}", "source_split": split, "index": index,
                "text_sha256": evaluation.digest(text.encode()),
                "gold_sha256": evaluation.digest(evaluation.encoded(facts, sorted_keys=True))})
    excluded = set()
    ledger = []
    for identity, members in sorted(groups.items()):
        if len(members) < 2:
            continue
        kept_split = max((row["source_split"] for row in members), key=priority.__getitem__)
        excluded_ids = []
        for row in members:
            if row["source_split"] != kept_split:
                excluded.add((row["source_split"], row["index"]))
                excluded_ids.append(row["id"])
        ledger.append({"normalized_text_sha256": identity, "members": members,
                       "cross_split": len({row["source_split"] for row in members}) > 1,
                       "exact_text_variants": len({row["text_sha256"] for row in members}),
                       "gold_variants": len({row["gold_sha256"] for row in members}),
                       "retained_source_split": kept_split, "excluded_ids": excluded_ids})
    retained = {split: [(index, row) for index, row in enumerate(rows) if (split, index) not in excluded]
                for split, rows in parsed.items()}
    audit = {"scope": "gliner25_crossner_split_audit/v1", "policy": SPLIT_POLICY,
             "unicode_version": unicodedata.unidata_version, "decision_inputs": "normalized_text_and_original_split_only",
             "official_test_preserved": True, "within_test_multiplicity_preserved": True,
             "source_counts": {split: len(rows) for split, rows in parsed.items()},
             "retained_counts": {split: len(rows) for split, rows in retained.items()},
             "cross_split_groups": sum(group["cross_split"] for group in ledger),
             "excluded_ids": sorted(f"crossner/ai/{split}/{index:06d}" for split, index in excluded),
             "duplicate_groups": ledger, "article_family_ids_available": False,
             "pretraining_contamination_status": "unknown"}
    return retained, audit


def prediction_facts(request, output, backend):
    """Convert actual native/Python entities without access to a gold row."""
    names = oracle.read_json(MANIFEST)["entity_types"]
    evaluation.checked(request["schema"] == {"entities": names}, "inference did not use the fixed complete ontology")
    text = request["text"]
    byte_text = text.encode("utf-8")
    positions = [0]
    for character in text:
        positions.append(positions[-1] + len(character.encode("utf-8")))
    boundaries = set(positions)
    facts = []
    if backend == "python":
        groups = output["entities"]
        evaluation.checked(isinstance(groups, dict) and set(groups) == set(names), "Python query coverage differs")
    elif backend in ("native", "metal"):
        groups = {group["name"]: group["values"] for group in output["entities"]}
        evaluation.checked(len(groups) == len(output["entities"]) and set(groups) == set(names), "native query coverage differs")
    else:
        raise evaluation.EvaluationError("unknown prediction backend")
    for name in names:
        evaluation.checked(isinstance(groups[name], list), "entity output must be a list")
        for value in groups[name]:
            confidence = value["confidence"]
            evaluation.checked(type(confidence) in (float, int) and math.isfinite(confidence) and 0 <= confidence <= 1, "invalid confidence")
            if backend == "python":
                start, end = value["start"], value["end"]
                evaluation.checked(type(start) is int and type(end) is int and 0 <= start < end < len(positions), "invalid Python codepoint span")
                start, end = positions[start], positions[end]
            else:
                source = value["source"]
                evaluation.checked(isinstance(source, dict) and source.get("unit") == "utf8_bytes", "native evaluation must return UTF-8 spans")
                start, end = source["start"], source["end"]
            evaluation.checked(type(start) is int and type(end) is int and start in boundaries and end in boundaries and start < end,
                               "invalid exact entity span")
            evaluation.checked(value["text"] == byte_text[start:end].decode(), "predicted surface differs from original bytes")
            facts.append({"type": name, "span": {"start": start, "end": end, "text": value["text"]}})
    result = facts_by_metric(facts, names)
    evaluation.metric_facts(result, metric_definitions(names), text)
    return result


def prepare(corpus: Path, output: Path) -> dict[str, Any]:
    manifest = oracle.read_json(MANIFEST)
    source_files = {}
    for pin in manifest["files"]:
        path = corpus / pin["path"]
        evaluation.checked(path.is_file() and path.stat().st_size == pin["size_bytes"], "source file size differs")
        data = path.read_bytes()
        git_id = hashlib.sha1(b"blob " + str(len(data)).encode() + b"\0" + data).hexdigest()
        evaluation.checked(evaluation.digest(data) == pin["sha256"] and git_id == pin["git_blob_sha1"], "source Git blob or digest differs")
        source_files[pin["path"]] = data
    names = ontology(source_files["src/dataloader.py"].decode())
    evaluation.checked(names == manifest["entity_types"], "full fixed author ontology differs")
    parsed_splits = {split: parse_bio(source_files[f"ner_data/ai/{split}.txt"].decode("utf-8"), names)
                     for split in ("train", "dev", "test")}
    retained, split_audit = separate_splits(parsed_splits)
    evaluation.checked(manifest["split_policy"] == SPLIT_POLICY and
                       split_audit["source_counts"] == manifest["source_documents"] and
                       split_audit["excluded_ids"] == manifest["excluded_ids"], "declared corpus split audit differs")
    output.mkdir(mode=0o700, exist_ok=False)

    def put(name, data):
        path = output / name
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("xb") as target:
            target.write(data)
        return {"path": name, "size_bytes": len(data), "sha256": evaluation.digest(data)}

    raw_pins = [put("source/" + name, data) for name, data in source_files.items()]
    schema_file = put("schema.json", evaluation.encoded({"entities": names}) + b"\n")
    definitions = metric_definitions(names)
    metric_file = put("metrics.json", evaluation.encoded(definitions) + b"\n")
    adapter_file = put("adapter.py", Path(__file__).read_bytes())
    manifest_pin = put("corpus_manifest.json", MANIFEST.read_bytes())
    audit_pin = put("split_audit.json", evaluation.encoded(split_audit) + b"\n")
    split_files = []
    statistics = {}
    for source_split, split in (("train", "train"), ("dev", "calibration"), ("test", "test")):
        normalized = []
        parsed = [row for _, row in retained[source_split]]
        evaluation.checked(bool(parsed), "split became empty after duplicate audit")
        for index, (text, facts, _) in retained[source_split]:
            # No article IDs exist in source BIO. Exact text identity is the
            # strongest available family key, never an article-level claim.
            normalized.append({"id": f"crossner/ai/{source_split}/{index:06d}",
                               "family_id": "crossner/text/" + evaluation.digest(text.encode()),
                               "language": "en", "schema_id": SCHEMA_ID, "text": text,
                               "gold": facts_by_metric(facts, names)})
        file_pin = put(split + ".jsonl", b"".join(evaluation.encoded(row) + b"\n" for row in normalized))
        split_files.append({"split": split, "records": len(parsed), "file": file_pin})
        statistics[split] = {"source_documents": len(parsed_splits[source_split]), "documents": len(parsed),
                             "excluded_documents": len(parsed_splits[source_split]) - len(parsed),
                             "entities": sum(len(facts) for _, facts, _ in parsed),
                             "maximum_original_tokens": max(tokens for _, _, tokens in parsed),
                             "maximum_utf8_bytes": max(len(text.encode()) for text, _, _ in parsed)}
    lock = {"scope": evaluation.SCOPE, "status": "locked", "qualification": False,
            "upstream_commit": oracle.UPSTREAM_COMMIT, "unicode_version": unicodedata.unidata_version,
            "harness_sha256": oracle.sha256_file(Path(evaluation.__file__)),
            "schema_selection": "fixed_before_test", "test_used_for_tuning": False,
            "adapter_file": adapter_file, "adapter_sha256": adapter_file["sha256"],
            "metric_contract_file": metric_file, "metric_contract_sha256": metric_file["sha256"],
            "schemas": [{"id": SCHEMA_ID, "origin": "public_ontology", "file": schema_file}],
            "request_options": {"threshold": 0.5, "overlap": "flat", "best_effort": False},
            "offset_unit": "utf8_bytes", "source_files": raw_pins, "supporting_files": [manifest_pin, audit_pin],
            "metrics": definitions, "splits": split_files,
            "dataset": {"id": "crossner_ai", "repository": manifest["repository"], "revision": manifest["revision"],
                        "text_profile": manifest["text_profile"], "schema_profile": manifest["schema_profile"],
                        "source_manifest_sha256": manifest_pin["sha256"], "split_policy": SPLIT_POLICY,
                        "split_audit_sha256": audit_pin["sha256"], "exclusions": split_audit["excluded_ids"],
                        "article_family_ids_available": False, "pretraining_contamination_status": "unknown"}}
    oracle.write_json(output / "lock.json", lock)
    admission = evaluation.audit(output / "lock.json")["summary"]
    blinded = evaluation.prepare(output / "lock.json", output / "prepared")
    summary = {"scope": "gliner25_crossner_ai_preparation/v1", "qualification": False,
               "no_model_execution": True, "statistics": statistics, "split_audit": split_audit,
               "admission": admission, "prepared": blinded}
    oracle.write_json(output / "preparation.json", summary)
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--corpus-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    result = prepare(args.corpus_dir, args.output_dir)
    print(evaluation.encoded(result).decode())


if __name__ == "__main__":
    main()
