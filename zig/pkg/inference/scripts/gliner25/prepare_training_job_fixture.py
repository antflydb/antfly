#!/usr/bin/env python3
"""Create four synthetic named-JSONL rows for a real small-model job smoke.

No tokenizer, model or training runtime is loaded. The full schema is fixed
before annotations; the pinned upstream word regex checks source boundaries.
Actual Source/Dataset preflight remains a native execution check.
"""
from __future__ import annotations

import argparse
import ast
import copy
import hashlib
import json
import re
import unicodedata
from pathlib import Path

import oracle

SOURCE_WORD_SPLITTER = {
    "char_is_explicit_opt_in": True,
    "commit": oracle.UPSTREAM_COMMIT,
    "file": "gliner2/processing/word_splitter.py",
    "published_default": "whitespace",
    "sha256": "8c533d4defec1cc469578bdb87244d57a82684ecff57431d0556a54e9a6eefc8",
}


def encoded(value):
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode() + b"\n"


def check(condition, message):
    if not condition:
        raise oracle.ContractError(message)


def word_pattern(source, expected_sha):
    """Load the pinned upstream whitespace splitter without its ML package."""
    check(oracle.sha256_file(source) == expected_sha, "pinned word splitter source differs")
    classes = {
        node.name: node
        for node in ast.parse(source.read_text()).body
        if isinstance(node, ast.ClassDef)
    }
    assignments = [
        node.value
        for node in classes["WhitespaceTokenSplitter"].body
        if isinstance(node, ast.Assign)
        and any(
            isinstance(target, ast.Name) and target.id == "_PATTERN"
            for target in node.targets
        )
    ]
    check(
        len(assignments) == 1 and isinstance(assignments[0], ast.Call),
        "word pattern declaration differs",
    )
    call = assignments[0]
    check(
        isinstance(call.func, ast.Attribute)
        and isinstance(call.func.value, ast.Name)
        and call.func.value.id == "re"
        and call.func.attr == "compile"
        and not call.keywords,
        "unexpected source pattern expression",
    )
    pattern = ast.literal_eval(call.args[0])
    flags = 0
    if len(call.args) == 2:
        flags_node = call.args[1]
        check(
            isinstance(flags_node, ast.BinOp) and isinstance(flags_node.op, ast.BitOr),
            "pattern flags differ",
        )
        for flag in (flags_node.left, flags_node.right):
            check(
                isinstance(flag, ast.Attribute)
                and isinstance(flag.value, ast.Name)
                and flag.value.id == "re"
                and flag.attr in ("VERBOSE", "IGNORECASE"),
                "unexpected regex flag",
            )
            flags |= getattr(re, flag.attr)
    else:
        check(len(call.args) == 1, "pattern arity differs")
    return re.compile(pattern, flags)


def schema():
    return {"entities": ["person", "company", "location"],
        "classifications": [{"name": "outcome", "labels": ["accepted", "rejected"], "mode": "single"}],
        "structures": {"employment": {"mode": "natural", "anchor": "employee", "fields": {
            "employee": {"dtype": "str", "cardinality": "required_one"},
            "employer": {"dtype": "str", "cardinality": "required_one"}}}},
        "relations": [{"type": "works_for", "source": "person", "target": "company"}]}


def row(identifier, text, employee, employer, outcome):
    def span(value):
        check(text.count(value) == 1, "synthetic value must have one declared source occurrence")
        start = text.index(value)
        return {"start": len(text[:start].encode()), "end": len(text[:start + len(value)].encode()), "unit": "utf8_bytes"}
    employee_span, employer_span = span(employee), span(employer)
    return {"version": 1, "id": identifier, "text": text, "schema": schema(),
        "entities": [{"id": "employee", "type": "person", "span": employee_span},
                     {"id": "employer", "type": "company", "span": employer_span}],
        "classifications": [{"task": "outcome", "labels": [outcome]}],
        "records": [{"type": "employment", "id": identifier + "/employment/0", "fields": [
            {"name": "employee", "values": [{"occurrences": [copy.deepcopy(employee_span)]}]},
            {"name": "employer", "values": [{"occurrences": [copy.deepcopy(employer_span)]}]}]}],
        "relations": [{"type": "works_for", "head": {"entity": "employee"}, "tail": {"entity": "employer"}}]}


def build_rows():
    return {
        "train": [row("job-smoke/train/001", "Ada works for Acme. The request was accepted.", "Ada", "Acme", "accepted"),
                  row("job-smoke/train/002", "Beta employs Ben. The request was rejected.", "Ben", "Beta", "rejected")],
        "validation": [row("job-smoke/validation/001", "Cora is employed by Delta. An accepted request is on file.", "Cora", "Delta", "accepted"),
                       row("job-smoke/validation/002", "Davi has a job at Echo. The latest request was rejected.", "Davi", "Echo", "rejected")],
    }


def prepare(output, source):
    model_manifest = oracle.read_json(Path(__file__).with_name("oracle_manifest.json"))
    splitter_manifest = SOURCE_WORD_SPLITTER
    pattern = word_pattern(
        source / splitter_manifest["file"], splitter_manifest["sha256"]
    )
    datasets = build_rows()
    fixed_schema = schema()
    identifiers, normalized, surfaces = set(), set(), {}
    evidence = {}
    for split, rows in datasets.items():
        split_surfaces = set()
        records = []
        for example in rows:
            check(example["schema"] == fixed_schema, "synthetic row schema differs from complete fixed schema")
            text, identifier = example["text"], example["id"]
            normalized_text = " ".join(unicodedata.normalize("NFC", text).casefold().split())
            check(identifier not in identifiers and normalized_text not in normalized, "synthetic split identity/text overlap")
            identifiers.add(identifier); normalized.add(normalized_text)
            words = list(pattern.finditer(text))
            starts = {len(text[:match.start()].encode()) for match in words}
            ends = {len(text[:match.end()].encode()) for match in words}
            spans = [entity["span"] for entity in example["entities"]]
            spans += [span for record in example["records"] for field in record["fields"] for value in field["values"] for span in value["occurrences"]]
            for span in spans:
                check(span["unit"] == "utf8_bytes" and span["start"] in starts and span["end"] in ends and span["start"] < span["end"],
                      "synthetic annotation is not representable by pinned source word boundaries")
                split_surfaces.add(text.encode()[span["start"]:span["end"]].decode())
            records.append({"id": identifier, "text_sha256": hashlib.sha256(text.encode()).hexdigest(),
                "normalized_text_sha256": hashlib.sha256(normalized_text.encode()).hexdigest(),
                "words": len(words), "utf8_bytes": len(text.encode()), "entities": len(example["entities"]),
                "records": len(example["records"]), "relations": len(example["relations"]),
                "supervised_classification_labels": example["classifications"][0]["labels"]})
        evidence[split] = records
        surfaces[split] = split_surfaces
    check(not surfaces["train"] & surfaces["validation"], "synthetic entity/record values cross splits")
    with oracle.atomic_output_directory(output) as staging:
        files = {}
        for split, rows in datasets.items():
            path = staging / (split + ".jsonl")
            path.write_bytes(b"".join(encoded(example) for example in rows))
            files[split] = {"path": path.name, "size_bytes": path.stat().st_size, "sha256": oracle.sha256_file(path), "records": len(rows)}
        manifest = {"scope": "gliner25_training_job_fixture/v1", "version": 1, "qualification": False,
            "dataset_format": "gliner_boundary_dataset.Row/version=1", "source_kind": "authored_synthetic_integration_examples",
            "no_model_execution": True, "heldout_quality_claim": False, "unicode_version": unicodedata.unidata_version,
            "generator_sha256": oracle.sha256_file(Path(__file__)), "oracle_sha256": oracle.sha256_file(Path(oracle.__file__)),
            "word_pattern_helper_sha256": oracle.sha256_file(Path(__file__)),
            "upstream_commit": oracle.UPSTREAM_COMMIT, "source_word_splitter": splitter_manifest,
            "model_reference": model_manifest["models"]["small"], "files": files, "rows": evidence,
            "split_policy": "distinct_ids_exact_and_normalized_text_and_positive_entity_values/v1",
            "schema_policy": "one_complete_fixed_schema_in_every_row_including_absent_location_type",
            "static_checks": {"all_annotations_on_pinned_word_boundaries": True, "balanced_outcome_labels_per_split": True,
                              "native_compiled_schema_and_targets": "pending_actual_Source_Dataset_preflight"},
            "suggested_run": {"mode": "heads", "epochs": 2, "batch_size": 1, "accumulation": 2,
                              "scheduler": "constant", "warmup_steps": 0, "shuffle": False, "seed": 42},
            "resume_probe": {"checkpoint_after_microbatches": 1, "restore_into_new_output_path": True,
                             "semantic_run_settings_must_match": True, "expected_completed_optimizer_updates": 2},
            "limitations": ["Four authored examples test job plumbing only; validation is not a production or benchmark holdout.",
                "Shared task templates and labels are intentional; normalized text/ID separation is not a near-duplicate or pretraining audit.",
                "Model files are referenced by exact pins but are neither opened nor executed by this generator.",
                "Actual tokenizer/compiled target admission, optimizer execution, export and validation inference are separate native checks.",
                "No threshold, schema or model choice may be presented as calibrated by these examples."]}
        oracle.write_json(staging / "manifest.json", manifest)
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--upstream", type=Path, default=Path("/private/tmp/antfly-gliner25-upstream"))
    args = parser.parse_args()
    result = prepare(args.output_dir, args.upstream)
    print("prepared", sum(pin.get("records", 0) for pin in result["files"].values()), "synthetic rows; actual native preflight pending")


if __name__ == "__main__":
    main()
