#!/usr/bin/env python3
"""Check locked byte-span scores using CrossNER's unmodified official scorer.

The common refinement of gold and predicted UTF-8 boundaries in each type is a
lossless BIO representation of per-type flat spans. Independent type planes
preserve cross-type overlap. It preserves partial-word errors and never snaps,
drops, trims or rescales an entity. This checks the declared exact-span metric;
it does not claim the original trainer's tokenization or flattened batching.
No model executes and all input evidence remains read-only.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import stat
import tempfile
import types

import evaluation_contract as evaluation

HERE = Path(__file__).resolve().parent
REFERENCE = HERE.parent.parent / "testdata/gliner25/crossner_metric_reference"
SCORER_SHA256 = "45e1b96603d09e7abe9949092789592de37b9a7744631bb8fb36e81bf0baa2e4"
MAX_FILE_BYTES = 32 * 1024 * 1024
MAX_ROWS = 1024
MAX_FACTS = 4096


def checked(condition, message):
    if not condition:
        raise evaluation.EvaluationError(message)


def read(path, maximum=MAX_FILE_BYTES):
    with os.fdopen(os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK), "rb") as source:
        before = os.fstat(source.fileno())
        checked(stat.S_ISREG(before.st_mode) and before.st_size <= maximum, "input size or kind differs")
        raw = source.read(maximum + 1)
        after = os.fstat(source.fileno())
        checked(len(raw) == before.st_size and
                (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns) ==
                (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns),
                "input changed while reading")
    return raw


def pin(raw):
    return {"size_bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest()}


def decode(raw):
    def pairs(items):
        result = {}
        for key, value in items:
            checked(key not in result, "duplicate JSON key")
            result[key] = value
        return result

    def invalid_constant(_):
        raise evaluation.EvaluationError("non-finite JSON constant")

    return json.loads(raw, object_pairs_hook=pairs, parse_constant=invalid_constant)


def load_scorer():
    raw = read(REFERENCE / "conll2002_metrics.py", 16384)
    checked(pin(raw) == {"size_bytes": 13425, "sha256": SCORER_SHA256}, "official scorer pin differs")
    module = types.ModuleType("_pinned_crossner_conll2002")
    # Compile the exact bytes just hashed, without a second path read or pycache.
    exec(compile(raw, str(REFERENCE / "conll2002_metrics.py"), "exec"), module.__dict__)
    return module


def spans(text, facts, ontology):
    checked(isinstance(text, str) and len(text.encode()) <= 1024 * 1024, "text exceeds audit geometry")
    checked(isinstance(facts, list) and len(facts) <= MAX_FACTS, "entity count exceeds audit geometry")
    raw = text.encode()
    boundaries = {0}
    position = 0
    for char in text:
        position += len(char.encode())
        boundaries.add(position)
    unique = set()
    for fact in facts:
        checked(isinstance(fact, dict) and set(fact) == {"type", "span"} and fact["type"] in ontology,
                "entity type or fields differ")
        span = fact["span"]
        checked(isinstance(span, dict) and set(span) == {"start", "end", "text"}, "span fields differ")
        start, end = span["start"], span["end"]
        checked(type(start) is int and type(end) is int and start in boundaries and end in boundaries and start < end,
                "invalid UTF-8 span")
        checked(span["text"] == raw[start:end].decode(), "source span text differs")
        unique.add((start, end, fact["type"]))
    result = sorted(unique)
    for name in ontology:
        typed = [row for row in result if row[2] == name]
        checked(all(left[1] <= right[0] for left, right in zip(typed, typed[1:])),
                "overlapping spans within one type cannot be represented in flat BIO")
    return result


def plane_lines(text, wanted, found):
    """Emit one already validated type plane, retaining every source boundary."""
    cuts = sorted({0, len(text.encode()), *[value for rows in (wanted, found) for row in rows for value in row[:2]]})

    def tags(rows):
        index = 0
        for start, end in zip(cuts, cuts[1:]):
            while index < len(rows) and rows[index][1] <= start:
                index += 1
            if index < len(rows) and rows[index][0] <= start and end <= rows[index][1]:
                yield ("B-" if rows[index][0] == start else "I-") + rows[index][2]
            else:
                yield "O"

    # The official parser consumes gold then prediction. Its explicit boundary
    # sentinel prevents a chunk crossing distinct documents, including empty ones.
    yield "-X- O O"
    for gold_tag, predicted_tag in zip(tags(wanted), tags(found)):
        yield "w " + gold_tag + " " + predicted_tag
    yield "-X- O O"


def score_documents(documents, ontology, scorer=None):
    checked(1 <= len(documents) <= MAX_ROWS and isinstance(ontology, list) and
            1 <= len(ontology) <= 128 and
            all(isinstance(name, str) and 0 < len(name) <= 128 and not any(c.isspace() for c in name) for name in ontology) and
            len(set(ontology)) == len(ontology),
            "invalid audit document or ontology geometry")
    scorer = scorer or load_scorer()

    def lines():
        yield "-X- O O"
        for text, gold, predicted in documents:
            wanted, found = spans(text, gold, ontology), spans(text, predicted, ontology)
            for name in ontology:
                left = [row for row in wanted if row[2] == name]
                right = [row for row in found if row[2] == name]
                if left or right:
                    yield from plane_lines(text, left, right)

    counts = scorer.evaluate(lines())
    overall, by_type = scorer.metrics(counts)

    def result(metric):
        return {"tp": metric.tp, "fp": metric.fp, "fn": metric.fn, "precision": metric.prec,
                "recall": metric.rec, "micro_f1": metric.fscore}

    zero = scorer.calculate_metrics(0, 0, 0)
    return {"entity_exact": result(overall),
            **{"entity_type/" + name: result(by_type.get(name, zero)) for name in ontology}}


def check_report_identity(report, receipt, stored=None):
    checked(report.get("scope") == "gliner25_heldout_execution/v1" and report.get("qualification") is False and
            report.get("status") == "complete" and report.get("lock_sha256") == receipt["lock_sha256"] and
            report.get("denominator") == report.get("completed_results") == receipt["records"] == 431 and
            report.get("errors") == report.get("unprocessed") == 0, "report identity or completion differs")
    if stored is not None:
        checked(stored.get("lock_sha256") == receipt["lock_sha256"] and stored.get("records") == receipt["records"] and
                stored.get("scope") == "gliner25_heldout_exact_fact_metrics/v1" and stored.get("qualification") is False and
                report.get("metrics") == stored.get("metrics"), "embedded report metrics differ")


def audit(prepared, reports):
    checked(1 <= len(reports) <= 16 and len(set(reports)) == len(reports), "invalid report inventory")
    inputs = {}

    def remember(path, expected=None, maximum=MAX_FILE_BYTES):
        raw = read(path, maximum)
        digest = pin(raw)
        checked(expected is None or digest == expected, "input digest differs")
        old = inputs.setdefault(str(path), digest)
        checked(old == digest, "input changed between reads")
        return raw

    receipt_raw = remember(prepared / "prepared.json", maximum=1024 * 1024)
    receipt = decode(receipt_raw)
    request_raw = remember(prepared / "requests.jsonl")
    gold_raw = remember(prepared / "gold.jsonl")
    checked(pin(request_raw)["sha256"] == receipt["requests_sha256"] and
            pin(gold_raw)["sha256"] == receipt["gold_sha256"], "prepared identity differs")

    def rows(raw):
        lines = raw.splitlines()
        checked(1 <= len(lines) <= MAX_ROWS and all(lines), "missing or excess rows")
        return [decode(line) for line in lines]

    requests, gold = rows(request_raw), rows(gold_raw)
    checked(len(requests) == len(gold) == 431, "locked CrossNER denominator differs")
    ontology = requests[0]["request"]["schema"]["entities"]
    scorer = load_scorer()
    remember(REFERENCE / "conll2002_metrics.py", {"size_bytes": 13425, "sha256": SCORER_SHA256})
    results = []
    for path in reports:
        report = decode(remember(path, maximum=8 * 1024 * 1024))
        check_report_identity(report, receipt)
        files = report["files"]
        for name in ("predictions", "metrics"):
            checked(files[name]["path"] == name + (".jsonl" if name == "predictions" else ".json"), "non-canonical report path")
        prediction_raw = remember(path.parent / "predictions.jsonl", {k: files["predictions"][k] for k in ("size_bytes", "sha256")})
        predictions = rows(prediction_raw)
        stored = decode(remember(path.parent / "metrics.json", {k: files["metrics"][k] for k in ("size_bytes", "sha256")}))
        check_report_identity(report, receipt, stored)
        checked(len(predictions) == len(requests), "prediction denominator differs")
        documents = []
        for request, expected, actual in zip(requests, gold, predictions):
            checked(all(request[key] == expected[key] == actual[key] for key in ("request_id", "request_sha256")),
                    "request identity or ordering differs")
            checked(request["request"]["schema"]["entities"] == ontology and
                    request["request"]["offset_unit"] == "utf8_bytes" and
                    request["request"]["options"]["overlap"] == "flat", "metric schema differs")
            documents.append((request["request"]["text"], expected["metrics"]["entity_exact"], actual["metrics"]["entity_exact"]))
        official = score_documents(documents, ontology, scorer)
        # The legacy scorer reads paths. Give it only already bounded, hashed
        # bytes in a private owner so an external path replacement cannot turn
        # its ordinary blocking reads into a FIFO or unbounded input.
        with tempfile.TemporaryDirectory(prefix="gliner25-bio-metric-") as temporary:
            owner = Path(temporary)
            for name, raw in (("prepared.json", receipt_raw), ("requests.jsonl", request_raw),
                              ("gold.jsonl", gold_raw), ("predictions.jsonl", prediction_raw)):
                (owner / name).write_bytes(raw)
            current = evaluation.score(owner, owner / "predictions.jsonl")
        checked(current == stored, "stored metric evidence differs")
        for name, value in official.items():
            for key, expected in value.items():
                actual = stored["metrics"][name][key]
                checked(actual == expected if key in ("tp", "fp", "fn") else abs(actual - expected) <= 1e-15,
                        "official BIO metric differs: " + name + "/" + key)
        results.append({"report": str(path), "report_pin": inputs[str(path)], "model": report["model"],
                        "backend": report["backend"], "precision": report["artifact"]["precision"],
                        "documents": len(documents), "metric_families": len(official), "official_metrics": official})
    for path, digest in inputs.items():
        checked(pin(read(Path(path))) == digest, "audit input changed")
    return {"scope": "gliner25_crossner_official_bio_metric_audit/v1", "status": "pass", "qualification": False,
            "model_execution": False, "source_revision": "2e7ba2a7798c961e3f29fbc51252c5a8d40224bf",
            "representation": "per_type_common_UTF8_boundary_refinement_with_explicit_type_and_document_sentinels",
            "limitations": ["Exact per-type flat span counts only; original BIO-token representability is not asserted.",
                            "Does not reproduce the original trainer's flattened batches or reverse tag-column call.",
                            "No task quality, calibration, latency or release threshold changes."],
            "inputs": inputs, "reports": results}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--prepared-dir", type=Path, required=True)
    parser.add_argument("--report", type=Path, action="append", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = audit(args.prepared_dir, args.report)
    result["audit_tool"] = pin(read(Path(__file__)))
    raw = (json.dumps(result, sort_keys=True, indent=2, allow_nan=False) + "\n").encode()
    with args.output.open("xb") as output:
        output.write(raw)
    print(json.dumps({"status": result["status"], "reports": len(result["reports"]), "output": pin(raw), "qualification": False}))


if __name__ == "__main__":
    main()
