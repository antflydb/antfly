#!/usr/bin/env python3
"""Replay bounded saved MASSIVE intent outputs; never load a numerical runtime.

The fixed English 60-label profile and original small FP32 artifact are the
only admitted scope. Partial Metal shards remain an explicit partial proof.
"""
from __future__ import annotations

import argparse
from collections import Counter
import hashlib
import math
import os
from pathlib import Path
import stat
import sys

import benchmark_cpu as bench
import check_bundles as comparison
import evaluate_massive11 as driver
import evaluation_contract as contract
import massive_eval_contract as execution
import oracle

HERE = Path(__file__).resolve().parent
SCOPE = "gliner25_massive_english_intent_evidence/v1"
PROFILE = "intent_en-US"
WORKER = {"size_bytes": 14600280, "sha256": "a43fcdee29931bcb67972ff002f3c1457e25f547cda24a60759cfed4a0f6880d"}
PYTHON_SHA = "80ee2dd97bc26259d4e30853336f72ad38aa4aa0531bb196cc444d899422689d"
RESOURCE_POLICY = {"max_worker_rss_bytes": 6 * 1024**3, "startup_timeout_seconds": 180,
                   "response_timeout_seconds": 125, "threads": 1}
MAX_FILE = 64 * 1024**2
MAX_LEDGER = 256 * 1024
check = execution.check


def digest(raw):
    return {"size_bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest()}


def read(path, expected=None, maximum=MAX_FILE):
    """Parse only the exact bytes hashed through a bounded regular descriptor."""
    with os.fdopen(os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK), "rb") as source:
        before = os.fstat(source.fileno())
        check(stat.S_ISREG(before.st_mode) and 0 <= before.st_size <= maximum, "unbounded or nonregular evidence")
        raw = source.read(maximum + 1)
        after = os.fstat(source.fileno())
    check(len(raw) == before.st_size and all(getattr(before, key) == getattr(after, key)
        for key in ("st_dev", "st_ino", "st_size", "st_mtime_ns", "st_ctime_ns")), "evidence changed while reading")
    actual = digest(raw)
    check(expected is None or all(actual[key] == expected[key] for key in actual), "evidence digest differs: " + str(path))
    return raw


def stream_pin(path, expected):
    with os.fdopen(os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK), "rb") as source:
        before = os.fstat(source.fileno())
        check(stat.S_ISREG(before.st_mode) and before.st_size == expected["size_bytes"] <= 2 * 1024**3, "artifact admission differs")
        size, hashed = 0, hashlib.sha256()
        while part := source.read(1024**2):
            size += len(part)
            check(size <= before.st_size, "artifact grew")
            hashed.update(part)
        after = os.fstat(source.fileno())
    check(all(getattr(before, key) == getattr(after, key) for key in
        ("st_dev", "st_ino", "st_size", "st_mtime_ns", "st_ctime_ns")), "artifact changed while hashing")
    check(size == expected["size_bytes"] and hashed.hexdigest() == expected["sha256"], "artifact digest differs")


def decode(raw):
    result = bench.strict_json(raw)
    check(isinstance(result, dict), "evidence must be an object")
    return result


def rows(raw, count):
    lines = raw.splitlines(keepends=True)
    check(len(lines) == count and all(line.endswith(b"\n") and len(line) <= execution.MAX_EVENT_BYTES for line in lines),
          "missing, extra or unbounded evidence row")
    return [decode(line) for line in lines]


def selected(response, backend, case, labels):
    check(set(response) == {"event", "case_id", "request_sha256", "input_ids", "output"} and
          response["event"] == "result" and response["case_id"] == case["id"] and
          response["request_sha256"] == case["request_sha256"], "substituted, reordered or failed response")
    tokens = response["input_ids"]
    check(isinstance(tokens, list) and 0 < len(tokens) <= 512 and
          all(type(token) is int and 0 <= token <= 0xFFFFFFFF for token in tokens), "invalid or oversized token sequence")
    output = response["output"]
    if backend == "python":
        check(set(output) == {"intent"}, "source output is not one ordinary intent")
        value = output["intent"]
    else:
        check(set(output) == {"entities", "classifications", "relations", "structures", "classification_solver",
                             "joint_solver", "record_solver", "long_document"}, "native task surface differs")
        check(all(output[key] == [] for key in ("entities", "relations", "structures")) and
              all(output[key] is None for key in ("classification_solver", "joint_solver", "record_solver", "long_document")),
              "unrequested native output or solver scope")
        groups = output["classifications"]
        check(isinstance(groups, list) and len(groups) == 1 and set(groups[0]) == {"name", "multi_label", "labels"} and
              groups[0]["name"] == "intent" and groups[0]["multi_label"] is False and len(groups[0]["labels"]) == 1,
              "native intent must contain exactly one selected label")
        value = groups[0]["labels"][0]
    check(set(value) == {"label", "confidence"} and value["label"] in labels and
          type(value["confidence"]) in (int, float) and math.isfinite(value["confidence"]) and
          0 <= value["confidence"] <= 1, "invalid selected label or probability")
    return value


def facts(label, labels):
    return {"intent_exact": [{"label": label}], **{"intent_type/" + name: [{"label": label}] if label == name else [] for name in labels}}


def metrics(gold, predicted, labels):
    """Independent multiclass counts, including every absent ontology member."""
    check(len(gold) == len(predicted) > 0 and all(label in labels for label in gold + predicted), "invalid metric denominator")
    n = len(gold)
    result = {}
    for name, target in [("intent_exact", None)] + [("intent_type/" + label, label) for label in labels]:
        if target is None:
            tp = sum(wanted == actual for wanted, actual in zip(gold, predicted, strict=True))
            fp = fn = n - tp
            exact, absent, absent_predicted = tp, 0, 0
        else:
            tp = sum(wanted == actual == target for wanted, actual in zip(gold, predicted, strict=True))
            fp = sum(wanted != target and actual == target for wanted, actual in zip(gold, predicted, strict=True))
            fn = sum(wanted == target and actual != target for wanted, actual in zip(gold, predicted, strict=True))
            exact, absent, absent_predicted = n - fp - fn, n - tp - fn, fp
        result[name] = {"tp": tp, "fp": fp, "fn": fn, "documents": n, "exact_documents": exact,
            "zero_gold_documents": absent, "zero_gold_predicted_documents": absent_predicted,
            "precision": tp / (tp + fp) if tp + fp else 0.0, "recall": tp / (tp + fn) if tp + fn else 0.0,
            "micro_f1": 2 * tp / (2 * tp + fp + fn) if 2 * tp + fp + fn else 0.0,
            "document_exact_match": exact / n, "support": tp + fn,
            "absent_query_false_positive_rate": absent_predicted / absent if absent else None}
    return result


def compare(left, right, tokens_equal):
    result = comparison.compare_backends({"classifications": [{"name": "intent", **left}]},
                                         {"classifications": [{"name": "intent", **right}]})
    result["token_ids_equal"] = tokens_equal
    result["parity_pass"] = tokens_equal and result["parity_pass"]
    return result


def summarize(comparisons):
    check(comparisons and all(row["parity_pass"] and row["decisions_equal"] and row["token_ids_equal"] and
          row["aligned_confidence_count"] == 1 and row["confidence_absolute_tolerance"] == 5e-4 for row in comparisons),
          "selected intent numerical parity failed")
    return {"requests": len(comparisons), "exact_token_sequences": len(comparisons), "exact_selected_labels": len(comparisons),
            "aligned_selected_confidences": len(comparisons), "confidence_absolute_tolerance": 5e-4,
            "maximum_absolute_confidence_error": max(row["max_aligned_confidence_absolute_error"] for row in comparisons), "pass": True}


class Audit:
    def __init__(self, prepared, source, binary):
        self.inputs = {}
        self.helpers = execution.contract_files()
        registry = execution.load_registry()
        self.entry = execution.profile_entry(PROFILE, registry)
        self.artifact = {"kind": "source_fp32", "precision": "fp32", "receipt": None, "receipt_sha256": None,
                         **driver.evaluate.source_identity("small")}
        self.source, self.binary = source, binary
        self.registry = registry
        self.profile_dir = prepared / PROFILE
        self.prepared = self.load(self.profile_dir / "prepared/prepared.json", sha=self.entry["prepared_sha256"])
        requests_raw = self.consume(self.profile_dir / "prepared/requests.jsonl", sha=self.entry["requests_sha256"])
        gold_raw = self.consume(self.profile_dir / "prepared/gold.jsonl", sha=self.entry["gold_sha256"])
        self.requests = rows(requests_raw, execution.RECORDS)
        self.gold = rows(gold_raw, execution.RECORDS)
        self.load(self.profile_dir / "lock.json", sha=self.entry["lock_sha256"])
        self.load(self.profile_dir / "transport.json", sha=self.entry["transport_sha256"])
        manifest = execution.read_json(execution.massive.MANIFEST)
        self.schema, metric_names = execution.massive.schema_and_metrics(self.entry["profile"], manifest)
        self.labels = self.schema["classifications"][0]["labels"]
        check(self.labels == manifest["intents"] and len(self.labels) == len(set(self.labels)) == 60 and
              list(self.prepared["metrics"]) == list(metric_names), "full intent ontology or metric family differs")
        check(contract.digest(contract.encoded(self.schema)) == self.entry["schema_sha256"], "schema order differs")
        self.cases, self.wanted = [], []
        for request, gold in zip(self.requests, self.gold, strict=True):
            value = request["request"]
            check(value["schema"] == self.schema and value["options"] == {"best_effort": False, "overlap": "flat",
                  "threshold": .5, "word_splitter": "whitespace"} and value["offset_unit"] == "utf8_bytes" and
                  contract.digest(contract.encoded(value)) == request["request_sha256"] and all(
                  request[key] == gold[key] for key in ("request_id", "request_sha256")), "prepared request/gold identity differs")
            label = gold["metrics"]["intent_exact"][0]["label"]
            check(label in self.labels and gold["metrics"] == facts(label, self.labels), "gold does not preserve full-label facts")
            self.wanted.append(label)
            self.cases.append({"id": request["request_id"], "request_sha256": request["request_sha256"], "text": value["text"]})
        check(len({case["id"] for case in self.cases}) == execution.RECORDS and
              contract.digest(contract.encoded([case["id"] for case in self.cases])) == self.entry["request_ids_sha256"] and
              contract.digest(contract.encoded([case["request_sha256"] for case in self.cases])) == self.entry["request_sha256s_sha256"],
              "global ordered request identities differ")
        self.check_artifacts()

    def consume(self, path, expected=None, sha=None):
        raw = read(path, expected)
        actual = digest(raw)
        check(sha is None or actual["sha256"] == sha, "accepted evidence SHA differs")
        previous = self.inputs.setdefault(str(path), actual)
        check(previous == actual, "same evidence path changed during audit")
        return raw

    def load(self, path, expected=None, sha=None):
        return decode(self.consume(path, expected, sha))

    def check_artifacts(self):
        stream_pin(self.binary, WORKER)
        for expected in self.artifact["source_files"]:
            stream_pin(self.source / expected["path"], expected)

    def report_file(self, directory, value):
        check(set(value) == {"path", "size_bytes", "sha256"} and Path(value["path"]).name == value["path"], "unsafe report evidence path")
        return self.consume(directory / value["path"], value)

    def identity(self, report, backend):
        check(report["status"] == "complete" and report["qualification"] is False and report["backend"] == backend and
              report["model"] == "small" and report["artifact"] == self.artifact and report["contract_files"] == self.helpers and
              report["profile"] == PROFILE and report["registry_sha256"] == oracle.sha256_file(execution.REGISTRY) and
              report["lock_sha256"] == self.entry["lock_sha256"] and report["prepared_sha256"] == self.entry["prepared_sha256"] and
              report["policy"] == execution.POLICY and report["limits"] == execution.LIMITS and
              report["resource_policy"] == RESOURCE_POLICY and report["errors"] == 0,
              "report artifact, profile, limits or completion differs")
        check(report["binary_sha256"] == (PYTHON_SHA if backend == "python" else WORKER["sha256"]), "unapproved executable")

    def output_rows(self, path, report, backend, start, end):
        raw_responses = self.report_file(path.parent, report["files"]["responses"])
        raw_predictions = self.report_file(path.parent, report["files"]["predictions"])
        responses, predictions = rows(raw_responses, end - start), rows(raw_predictions, end - start)
        selections = []
        for case, response, prediction in zip(self.cases[start:end], responses, predictions, strict=True):
            value = selected(response, backend, case, self.labels)
            check(prediction == {"request_id": case["id"], "request_sha256": case["request_sha256"],
                                 "metrics": facts(value["label"], self.labels)}, "saved facts differ from raw selected label")
            selections.append(value)
        return responses, selections

    def comparisons(self, report, responses, selections, start, references):
        recorded, values = [], {name: [] for name in references}
        for index, (response, selection) in enumerate(zip(responses, selections, strict=True)):
            row = {"case_id": self.cases[start + index]["id"], "input_ids_u32_le_sha256":
                   contract.digest(b"".join(token.to_bytes(4, "little") for token in response["input_ids"]))}
            for name, (reference_responses, reference_values) in references.items():
                item = compare(reference_values[start + index], selection,
                               reference_responses[start + index]["input_ids"] == response["input_ids"])
                row[name] = item
                values[name].append(item)
            recorded.append(row)
        check(recorded == report["comparisons"], "recorded parity differs from consumed raw outputs")
        return {name: summarize(value) for name, value in values.items()}

    def shard(self, path, backend, index, references, expected=None):
        report = self.load(path, sha=expected)
        self.identity(report, backend)
        start, end = execution.RANGES[index]
        check(report["scope"] == execution.SHARD_SCOPE and report["transport"] == {"global_records": execution.RECORDS,
              "transport_sha256": self.entry["transport_sha256"], **self.entry["shards"][index]} and
              report["processed"] == report["denominator"] == end - start and report["unprocessed"] == 0 and
              0 < report["peak_worker_rss_bytes"] <= RESOURCE_POLICY["max_worker_rss_bytes"], "shard range or resource receipt differs")
        fixture_raw = self.report_file(path.parent, report["files"]["fixture"])
        fixture = decode(fixture_raw)
        execution.validate_fixture(fixture, self.registry)
        check(report["fixture_sha256"] == digest(fixture_raw)["sha256"] and fixture["transport"] == report["transport"] and
              all({key: case[key] for key in ("id", "request_sha256", "text")} == expected
                  for case, expected in zip(fixture["cases"], self.cases[start:end], strict=True)), "actual fixture differs from prepared inputs")
        driver.validate_ready(report["ready"], backend, self.artifact, fixture, report["fixture_sha256"], report["python_runtime"])
        responses, selections = self.output_rows(path, report, backend, start, end)
        compared = self.comparisons(report, responses, selections, start, references)
        stderr = path.parent / (backend + ".stderr.log")
        self.consume(stderr)
        result = {"index": index, "range": [start, end], "report": digest(self.consume(path)), "files": report["files"],
                  "peak_worker_rss_bytes": report["peak_worker_rss_bytes"], "comparisons": compared,
                  "maximum_encoded_tokens": max(len(row["input_ids"]) for row in responses),
                  "source_reference_report_sha256": report["source_reference_report_sha256"],
                  "native_reference_report_sha256": report["native_reference_report_sha256"]}
        return report, result, responses, selections

    def aggregate(self, path, backend, source_reference=None):
        report = self.load(path)
        self.identity(report, backend)
        check(report["scope"] == execution.AGGREGATE_SCOPE and report["denominator"] == report["successful_results"] == execution.RECORDS and
              report["error_codes"] == {} and len(report["shards"]) == 3, "incomplete aggregate")
        references = {} if source_reference is None else {"source_fp32": source_reference[1:]}
        check(report["source_reference_report_sha256"] == (source_reference[0] if source_reference else None) and
              report["native_reference_report_sha256"] is None, "aggregate reference identity differs")
        reports, summaries, all_responses, all_selections = [], [], [], []
        for index, item in enumerate(report["shards"]):
            check(item["index"] == index, "reordered or duplicated shards")
            shard_path = path.parent.with_name(path.parent.name + "-" + str(index)) / "report.json"
            original, summary, responses, selections = self.shard(shard_path, backend, index, references, item["report_sha256"])
            check(item["transport"] == original["transport"] and driver.shared_identity(original) == driver.shared_identity(report), "aggregate shard identity differs")
            reports.append(original); summaries.append(summary); all_responses.extend(responses); all_selections.extend(selections)
        driver.validate_shard_set(reports, self.entry)
        responses, selections = self.output_rows(path, report, backend, 0, execution.RECORDS)
        check(responses == all_responses and selections == all_selections, "aggregate reordered or changed shard outputs")
        recomputed = metrics(self.wanted, [value["label"] for value in selections], self.labels)
        check(recomputed == report["metrics"] and report["direct_document_rates"] == {"intent_exact": recomputed["intent_exact"]["document_exact_match"]}, "aggregate metric counters differ")
        compared = self.comparisons(report, responses, selections, 0, references)
        check(report["required_token_parity"] == (None if source_reference is None else {"pass": True, "exact": True}) and
              report["required_parity"] == (None if source_reference is None else {"reference": "source_fp32", "pass": True,
              "confidence_absolute_tolerance": 5e-4}), "aggregate parity status differs")
        return {"report": digest(self.consume(path)), "files": report["files"], "shards": summaries,
                "comparisons": compared, "metrics": recomputed, "source_reference_report_sha256": report["source_reference_report_sha256"]}, responses, selections

    def finish(self):
        for path, expected in self.inputs.items():
            read(Path(path), expected)
        self.check_artifacts()
        check(execution.contract_files() == self.helpers, "frozen helper closure changed")


def run(args):
    audit = Audit(args.prepared_root, args.source_dir, args.binary)
    python, python_responses, python_values = audit.aggregate(args.python_report, "python")
    native, native_responses, native_values = audit.aggregate(args.native_report, "native",
        (python["report"]["sha256"], python_responses, python_values))
    check(native["metrics"] == python["metrics"], "source and native quality differ")
    metal = []
    metal_originals = {}
    metal_outputs = {}
    seen = set()
    for path in args.metal_shard:
        report = audit.load(path)
        index = report["transport"]["index"]
        check(type(index) is int and 0 <= index < 3 and index not in seen, "duplicate or invalid Metal shard")
        seen.add(index)
        check(report["source_reference_report_sha256"] == python["report"]["sha256"] and
              report["native_reference_report_sha256"] == native["report"]["sha256"], "Metal references different source/native aggregate")
        original, summary, responses, values = audit.shard(path, "metal", index,
            {"source_fp32": (python_responses, python_values), "same_artifact_native": (native_responses, native_values)})
        metal_originals[index] = original
        metal_outputs[index] = (responses, values)
        start, end = execution.RANGES[index]
        summary["metrics"] = metrics(audit.wanted[start:end], [value["label"] for value in values], audit.labels)
        check(summary["metrics"] == metrics(audit.wanted[start:end], [value["label"] for value in native_values[start:end]], audit.labels), "Metal shard quality differs")
        metal.append(summary)
    metal.sort(key=lambda row: row["index"])
    metal_count = sum(row["range"][1] - row["range"][0] for row in metal)
    metal_aggregate = None
    if args.metal_report is not None:
        check(seen == {0, 1, 2}, "complete Metal aggregate needs all three audited shards")
        report = audit.load(args.metal_report)
        audit.identity(report, "metal")
        check(report["scope"] == execution.AGGREGATE_SCOPE and
              report["denominator"] == report["successful_results"] == execution.RECORDS and report["error_codes"] == {} and
              report["source_reference_report_sha256"] == python["report"]["sha256"] and
              report["native_reference_report_sha256"] == native["report"]["sha256"] and report["shards"] == [
                  {"index": index, "report_sha256": metal[index]["report"]["sha256"], "transport": metal_originals[index]["transport"]}
                  for index in range(3)], "Metal aggregate coverage or recorded reference differs")
        driver.validate_shard_set([metal_originals[index] for index in range(3)], audit.entry)
        check(all(driver.shared_identity(metal_originals[index]) == driver.shared_identity(report) for index in range(3)),
              "Metal aggregate shard identity differs")
        responses, values = audit.output_rows(args.metal_report, report, "metal", 0, execution.RECORDS)
        check(responses == [row for index in range(3) for row in metal_outputs[index][0]] and
              values == [row for index in range(3) for row in metal_outputs[index][1]], "Metal aggregate changed shard outputs")
        recomputed = metrics(audit.wanted, [value["label"] for value in values], audit.labels)
        check(recomputed == report["metrics"] == native["metrics"] and
              report["direct_document_rates"] == {"intent_exact": recomputed["intent_exact"]["document_exact_match"]} and
              report["required_token_parity"] == {"pass": True, "exact": True} and report["required_parity"] == {
                  "reference": "same_artifact_native", "pass": True, "confidence_absolute_tolerance": 5e-4},
              "Metal aggregate quality or parity differs")
        compared = audit.comparisons(report, responses, values, 0,
            {"source_fp32": (python_responses, python_values), "same_artifact_native": (native_responses, native_values)})
        metal_aggregate = {"report": digest(audit.consume(args.metal_report)), "files": report["files"], "comparisons": compared,
                           "source_reference_report_sha256": python["report"]["sha256"],
                           "native_reference_report_sha256": native["report"]["sha256"]}
    audit.finish()
    # Keep all 61 metrics once; Metal selections already match the same rows.
    metric_rows = native.pop("metrics")
    check(python.pop("metrics") == metric_rows, "source metric snapshot differs")
    for row in metal:
        row["metrics_sha256"] = contract.digest(contract.encoded(row.pop("metrics"), sorted_keys=True))
    return {"format_version": 1, "scope": SCOPE, "status": "audited", "qualification": False, "model_execution": False,
        "source_commit": oracle.UPSTREAM_COMMIT, "model": "small", "artifact": audit.artifact, "worker": WORKER,
        "profile": audit.entry, "fixed_schema": audit.schema, "policy": execution.POLICY, "limits": execution.LIMITS,
        "resource_policy": RESOURCE_POLICY, "contract_files": audit.helpers,
        "reports": {"python": python, "native": native, "metal_shards": metal, "metal_aggregate": metal_aggregate}, "metrics": metric_rows,
        "coverage": {"python": 2974, "native": 2974, "metal": metal_count, "metal_complete": seen == {0, 1, 2},
                     "metal_missing_shards": sorted({0, 1, 2} - seen), "errors": 0},
        "probability_scope": {"declared_labels": 60, "selected_labels_per_request": 1, "selected_confidence_parity": True,
                              "full_probability_vector_observed": False, "unselected_probability_parity": False,
                              "probability_calibration_qualified": False},
        "quality": {"correct_intents": metric_rows["intent_exact"]["tp"], "documents": 2974,
                    "accuracy": metric_rows["intent_exact"]["document_exact_match"], "metric_families": len(metric_rows),
                    "absent_gold_labels": [name for name in audit.labels if metric_rows["intent_type/" + name]["support"] == 0],
                    "fixed_ontology_macro_f1": sum(metric_rows["intent_type/" + name]["micro_f1"] for name in audit.labels) / 60,
                    "source_native_metrics_equal": True, "no_holdout_tuning": True, "absolute_quality_floor_qualified": False},
        "raw_evidence": [{"path": path, **value} for path, value in sorted(audit.inputs.items())],
        "auditor": {"path": "scripts/gliner25/" + Path(__file__).name, **digest(read(Path(__file__)))},
        "claims": {"heldout_english_intent_only": True, "same_executable_cpu_metal_on_recorded_shards": bool(metal),
                   "all_other_massive_profiles_qualified": False, "benchmark": False, "release_qualification": False}}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--prepared-root", type=Path, required=True)
    parser.add_argument("--source-dir", type=Path, required=True)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--python-report", type=Path, required=True)
    parser.add_argument("--native-report", type=Path, required=True)
    parser.add_argument("--metal-shard", type=Path, action="append", default=[])
    parser.add_argument("--metal-report", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    check(not args.output.exists() and args.output.parent.is_dir(), "audit output must be fresh")
    try:
        result = run(args)
        check(not any(name in sys.modules for name in ("torch", "peft", "gliner2")), "audit imported a numerical runtime")
        raw = contract.encoded(result, sorted_keys=True) + b"\n"
        check(len(raw) <= MAX_LEDGER, "ledger byte cap exceeded")
        with args.output.open("xb") as output:
            output.write(raw)
        print(contract.encoded({"status": "audited", "output": digest(raw), "coverage": result["coverage"]}).decode())
    except BaseException as error:
        failure = args.output.with_name(args.output.name + ".failure.json")
        with failure.open("xb") as output:
            output.write(contract.encoded({"scope": SCOPE, "status": "failed", "qualification": False,
                "model_execution": False, "error": {"type": type(error).__name__, "message": str(error)}}) + b"\n")
        raise


if __name__ == "__main__":
    main()
