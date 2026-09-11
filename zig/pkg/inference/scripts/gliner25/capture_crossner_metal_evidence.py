#!/usr/bin/env python3
"""Capture or validate the additive original-FP32 CrossNER Metal evidence.

Capture replays bounded saved outputs without loading a model. Validation uses
only repository fixtures and independently pinned source/historical contracts.
The compact ledger is not a replacement for archiving the pinned raw captures.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import stat
import tempfile

import audit_crossner_bio_scoring as bio
import evaluate as execution
import evaluation_contract as contract
import oracle

HERE = Path(__file__).resolve().parent
LEDGER = HERE.parent.parent / "testdata/gliner25/crossner_fp32_metal_execution_v1.json"
HISTORY = HERE / "crossner_ai_execution.json"
SCOPE = "gliner25_crossner_ai_fp32_metal_evidence/v1"
WORKER_SHA = "a43fcdee29931bcb67972ff002f3c1457e25f547cda24a60759cfed4a0f6880d"
HISTORICAL_WORKER_SHA = "0069cdad6b8d3b1a97f00779c3de73d81125766ad832f5fc3cd9d9d076b77334"
HISTORY_PIN = {"size_bytes": 205694, "sha256": "1a663fe9a3d6b9c240285104f7a89ff310c29341bb67569b2e5b3231e2938506"}
AUDITS = {
    "historical_metal": {"path": "/private/tmp/gliner25-crossner-metal-all-independent-audit-v3.json", "size_bytes": 25044, "sha256": "b99c6d62525434d1c2f7ccf8509a0559a294bdd76dcad2427f6dab4502788fd3"},
    "same_executable": {"path": "/private/tmp/gliner25-crossner-same-executable-all-audit-v2.json", "size_bytes": 761528, "sha256": "13fafb8c9436d409b6daf12b8542969a7d5244fdea17f045012b7b935db45a0a"},
    "official_bio": {"path": "/private/tmp/gliner25-crossner-official-bio-audit-v4.json", "size_bytes": 66728, "sha256": "67959d10aec3007ba3e3c915419d08a49d67d25da068bea6958a18ad83501b1e"},
}
AUDIT_TOOLS = {
    "official_bio": {"path": "scripts/gliner25/audit_crossner_bio_scoring.py", "size_bytes": 13121,
                     "sha256": "1597e430e846acaa5f45ec7b355bc1f309e350a75c5ecf7f135f0f3b47327b7f"},
    "historical_metal": {"path": "/private/tmp/gliner25-audit-crossner-metal-all-v3.py", "size_bytes": 12580,
                         "sha256": "3d2e0194fd9f40567d7f2c32a13fcf297d521a324141120bd41245f52774f997"},
    "same_executable": {"path": "/private/tmp/gliner25-crossner-same-executable-audit-v2.py", "size_bytes": 11845,
                        "sha256": "1694771f014fd2fdf5af5d073906546b3ca8d7b32e903f863f1afafae159e713"},
}
VARIANTS = ("small", "base", "multi")
PHASES = ("python", "historical_native", "native", "metal")
COMPARISONS = {"native_python": ("python", "native"), "metal_python": ("python", "metal"),
               "metal_historical_native": ("historical_native", "metal"), "metal_native_same_executable": ("native", "metal")}
MAX_LEDGER_BYTES = 256 * 1024
check = bio.checked


def digest(value):
    # Recomputed Counter rows and stored JSON use different key insertion orders.
    return hashlib.sha256(contract.encoded(value, sorted_keys=True)).hexdigest()


def pinned(path, expected=None, maximum=8 * 1024**2):
    raw = bio.read(path, maximum)
    pin = bio.pin(raw)
    check(expected is None or pin == {key: expected[key] for key in ("size_bytes", "sha256")}, "evidence pin differs: " + str(path))
    return raw, pin


def stream_pin(path, expected):
    with os.fdopen(os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK), "rb") as source:
        before = os.fstat(source.fileno())
        check(stat.S_ISREG(before.st_mode) and before.st_size == expected["size_bytes"] <= 2 * 1024**3, "source geometry differs")
        size, value = 0, hashlib.sha256()
        while chunk := source.read(1024**2):
            size += len(chunk)
            check(size <= before.st_size, "source grew")
            value.update(chunk)
        after = os.fstat(source.fileno())
    check(all(getattr(before, key) == getattr(after, key) for key in ("st_dev", "st_ino", "st_size", "st_mtime_ns", "st_ctime_ns")), "source changed while hashing")
    check(size == expected["size_bytes"] and value.hexdigest() == expected["sha256"], "source content differs")


def historical():
    raw, _ = pinned(HISTORY, HISTORY_PIN)
    return bio.decode(raw)


def metric_rows(row):
    return {"entity_exact": row["entity_exact"], **{"entity_type/" + name: value for name, value in row["per_type"].items()}}


def summary(rows):
    check(len(rows) == 431 and all(row["decisions_equal"] and row["parity_pass"] and
        row["confidence_absolute_tolerance"] == 5e-4 for row in rows), "numerical comparison differs")
    return {"requests": 431, "exact_token_sequences": 431, "exact_ordered_decisions": 431,
            "aligned_confidences": sum(row["aligned_confidence_count"] for row in rows),
            "max_absolute_confidence_error": max(row["max_aligned_confidence_absolute_error"] for row in rows),
            "confidence_absolute_tolerance": 5e-4, "pass": True}


def validate(value):
    """Check independent model, corpus, historical report and approved-build pins."""
    history = historical()
    old = {row["model"]: row for row in history["models"]}
    preparation = bio.decode(bio.read(HERE / "crossner_ai_preparation.json"))
    check(value.get("format_version") == 1 and value.get("scope") == SCOPE and value.get("status") == "audited" and
          value.get("qualification") is False and value.get("model_execution") is False, "ledger status differs")
    check(value["historical_ledger"] == {"path": "scripts/gliner25/crossner_ai_execution.json", **HISTORY_PIN}, "historical ledger binding differs")
    check(value["source_commit"] == oracle.UPSTREAM_COMMIT == history["source_commit"] and
          value["corpus_revision"] == history["corpus_revision"], "source revision differs")
    dataset = value["dataset"]
    check(dataset == {"id": "crossner_ai", "documents": 431, "gold_entities": 1809,
        "lock_sha256": history["lock_sha256"], "prepared_receipt_sha256": history["prepared_receipt_sha256"],
        "requests_sha256": preparation["evidence_files"]["prepared/requests.jsonl"]["sha256"],
        "gold_sha256": preparation["evidence_files"]["prepared/gold.jsonl"]["sha256"],
        "ontology": history["ontology"], "offset_unit": history["offset_unit"], "request_options": history["request_options"],
        "policy": history["evaluation_policy"]}, "locked corpus/request contract differs")
    check(value["worker"] == {"size_bytes": 14600280, "sha256": WORKER_SHA, "build_mode": "ReleaseFast",
          "math_policy": "strict_f32_activations_v1", "weight_precision": "fp32", "activation_precision": "f32",
          "accumulation_precision": "f32", "head_precision": "f32"}, "approved worker identity differs")
    check(value["contract_files"] == execution.contract_files() == history["contract_files"], "frozen helper closure differs")
    check(value["audit_receipts"] == AUDITS, "accepted audit receipt differs")
    check(value["audit_tools"] == AUDIT_TOOLS, "audit source identity differs")
    pinned(Path(bio.__file__), AUDIT_TOOLS["official_bio"])
    check(value["official_bio"] == {"reports": 15, "cpu_reports": 12, "metal_reports": 3, "scored_document_instances": 6465,
          "metric_family_comparisons": 225, "scorer_sha256": bio.SCORER_SHA256,
          "representation": "per_type_common_UTF8_boundary_refinement_with_explicit_type_and_document_sentinels"}, "official metric proof differs")
    check(value["capture_tool"] == {"path": "scripts/gliner25/" + Path(__file__).name,
                                  "sha256": oracle.sha256_file(Path(__file__))}, "ledger capture tool differs")
    check([row["model"] for row in value["models"]] == list(VARIANTS), "model inventory differs")
    for row in value["models"]:
        variant = row["model"]
        check(row["source"] == execution.source_identity(variant), "published model/sidecar identity differs")
        check(set(row["reports"]) == set(PHASES), "phase inventory differs")
        for phase, report in row["reports"].items():
            check(report["model"] == variant and report["backend"] == ("native" if phase == "historical_native" else phase), "foreign report identity")
            check(report["completed_results"] == report["denominator"] == 431 and report["errors"] == report["unprocessed"] == 0, "report denominator differs")
            check(type(report["pin"]["size_bytes"]) is int and 0 < report["pin"]["size_bytes"] <= 8 * 1024**2 and
                  execution.evaluation.is_digest(report["pin"]["sha256"]), "invalid report pin")
            check(report["max_worker_rss_bytes"] == 6 * 1024**3 and 0 < report["peak_worker_rss_bytes"] <= 6 * 1024**3, "recorded worker guard differs")
            check(set(report["files"]) == {"fixture", "responses", "predictions", "metrics"} and
                  report["files"]["fixture"]["sha256"] == old[variant]["fixture_sha256"] and
                  report["files"]["metrics"] == old[variant]["reports"]["python"]["files"]["metrics"],
                  "report fixture/metric identity differs")
            if phase in ("python", "historical_native"):
                prior = old[variant]["reports"]["python" if phase == "python" else "native"]
                check(report["pin"]["sha256"] == prior["sha256"] and report["binary_sha256"] == prior["binary_sha256"] and
                      report["files"] == prior["files"], "historical source report was substituted")
            else:
                check(report["binary_sha256"] == WORKER_SHA, "CPU/Metal executable differs")
            expected_source = None if phase == "python" else row["reports"]["python"]["pin"]["sha256"]
            expected_native = row["reports"]["historical_native"]["pin"]["sha256"] if phase == "metal" else None
            check(report["source_reference_report_sha256"] == expected_source and
                  report["native_reference_report_sha256"] == expected_native, "historical reference was rewritten")
        check(row["reports"]["historical_native"]["binary_sha256"] == HISTORICAL_WORKER_SHA, "historical build differs")
        check(set(row["comparisons"]) == set(COMPARISONS), "comparison inventory differs")
        predictions = old[variant]["entity_exact"]["tp"] + old[variant]["entity_exact"]["fp"]
        for compared in row["comparisons"].values():
            check(compared["requests"] == compared["exact_token_sequences"] == compared["exact_ordered_decisions"] == 431 and
                  compared["aligned_confidences"] == predictions and compared["pass"] is True and
                  compared["confidence_absolute_tolerance"] == 5e-4 and
                  type(compared["max_absolute_confidence_error"]) in (int, float) and
                  math.isfinite(compared["max_absolute_confidence_error"]) and 0 <= compared["max_absolute_confidence_error"] <= 5e-4,
                  "numerical proof incomplete or tolerance changed")
        check(row["quality"] == {"metric_families": 15, "all_four_phases_identical": True,
              "metrics_sha256": digest(metric_rows(old[variant])), "entity_exact": old[variant]["entity_exact"],
              "fixed_ontology_macro_f1": old[variant]["fixed_ontology_macro_f1"],
              "fresh_native_micro_f1_delta": 0, "metal_micro_f1_delta": 0}, "source/backend quality differs")
    check(value["totals"] == {"fresh_native_results": 1293, "metal_results": 1293,
          "same_executable_comparisons": 1293, "all_recorded_reference_comparisons": 5172, "errors": 0, "unprocessed": 0}, "proof totals differ")
    check(value["claims"] == {"original_fp32_short_english_entity_parity": True, "same_executable_cpu_metal_parity": True,
          "new_quality_floor": False, "benchmark": False, "release_qualification": False,
          "reduced_metal_holdout_qualification": False}, "unsupported qualification claim")
    return value


def capture(args):
    history = historical()
    helpers = execution.contract_files()
    audits = {}
    for name, expected in AUDITS.items():
        path = args.captures / Path(expected["path"]).name
        raw, _ = pinned(path, expected)
        audits[name] = bio.decode(raw)
    old = {row["model"]: row for row in history["models"]}
    historical_rows = {row["model"]: row for row in audits["historical_metal"]["results"]}
    same_rows = {row["model"]: row for row in audits["same_executable"]["results"]}
    check(audits["same_executable"]["same_executable"] and audits["same_executable"]["private_owner_cleaned"] and
          audits["same_executable"]["total_requests"] == 1293 and audits["same_executable"]["binary_sha256"] == WORKER_SHA,
          "same-executable audit differs")
    official = audits["official_bio"]
    check(official["status"] == "pass" and len(official["reports"]) == 15 and
          sum(row["documents"] for row in official["reports"]) == 6465 and
          sum(row["metric_families"] for row in official["reports"]) == 225, "BIO audit differs")
    check(official["audit_tool"] == {key: AUDIT_TOOLS["official_bio"][key] for key in ("size_bytes", "sha256")}, "BIO tool pin differs")
    for name, key in (("historical_metal", "read_evidence_pins"), ("same_executable", "input_pins")):
        tool = AUDIT_TOOLS[name]
        check(audits[name][key][tool["path"]] == {field: tool[field] for field in ("size_bytes", "sha256")}, "independent audit tool differs")
    preparation = bio.decode(bio.read(HERE / "crossner_ai_preparation.json"))
    prepared_raw = {name: pinned(args.prepared / name, preparation["evidence_files"]["prepared/" + name])[0]
                    for name in ("prepared.json", "requests.jsonl", "gold.jsonl")}
    receipt = bio.decode(prepared_raw["prepared.json"])
    requests = [bio.decode(line) for line in prepared_raw["requests.jsonl"].splitlines()]
    models, consumed = [], {}
    with tempfile.TemporaryDirectory(prefix="gliner25-metal-ledger-") as temporary:
        owner = Path(temporary)
        check(stat.S_IMODE(owner.stat().st_mode) == 0o700, "private owner permission differs")
        prepared = owner / "prepared"
        prepared.mkdir()
        for name, raw in prepared_raw.items(): (prepared / name).write_bytes(raw)
        for variant in VARIANTS:
            expected_source = execution.source_identity(variant)
            artifact = {"kind": "source_fp32", "precision": "fp32", "receipt": None, "receipt_sha256": None, **expected_source}
            check(artifact == historical_rows[variant]["artifact"], "independent audit source differs")
            phases, report_rows = {}, {}
            for phase in PHASES:
                expected = (same_rows[variant]["reports"][phase] if phase in ("native", "metal") else
                            historical_rows[variant]["report_pins"]["python" if phase == "python" else "native"])
                original_path = Path(expected["path"])
                path = args.captures / original_path.parent.name / original_path.name
                raw, report_pin = pinned(path, expected)
                consumed[path] = report_pin
                report = bio.decode(raw)
                bio.check_report_identity(report, receipt)
                check(report["artifact"] == artifact and report["contract_files"] == helpers, "raw report artifact/helper differs")
                private = owner / (variant + "-" + phase)
                private.mkdir()
                (private / "report.json").write_bytes(raw)
                for key, filename in (("fixture", "requests.fixture.json"), ("responses", "responses.jsonl"), ("predictions", "predictions.jsonl"), ("metrics", "metrics.json")):
                    expected_file = report["files"][key]
                    check(expected_file["path"] == filename, "noncanonical capture filename")
                    data, file_pin = pinned(path.parent / filename, expected_file)
                    consumed[path.parent / filename] = file_pin
                    (private / filename).write_bytes(data)
                fixture = bio.decode((private / "requests.fixture.json").read_bytes())
                execution.validate_fixture(fixture)
                check(fixture["cases"] == [{"id": row["request_id"], "request_sha256": row["request_sha256"], **row["request"]} for row in requests], "request content/order differs")
                backend = "native" if phase == "historical_native" else phase
                reference = execution.load_reference(private / "report.json", fixture, prepared, backend, artifact)
                bio.check_report_identity(report, receipt, reference["metrics"])
                reference["fixture"] = fixture
                phases[phase] = reference
                report_rows[phase] = {"pin": {"path": str(original_path), **report_pin}, "files": report["files"],
                    **{key: report[key] for key in ("model", "backend", "binary_sha256", "denominator", "completed_results", "errors", "unprocessed", "max_worker_rss_bytes", "peak_worker_rss_bytes", "source_reference_report_sha256", "native_reference_report_sha256")}}
            metrics = phases["python"]["metrics"]["metrics"]
            check(metrics == metric_rows(old[variant]) and all(ref["metrics"]["metrics"] == metrics for ref in phases.values()), "all-phase metrics differ")
            collected = {name: [] for name in COMPARISONS}
            for index, case in enumerate(phases["python"]["fixture"]["cases"]):
                ids = phases["python"]["responses"][index]["input_ids"]
                request = {key: case[key] for key in ("text", "schema", "options", "offset_unit")}
                canonical = {}
                for phase, reference in phases.items():
                    response = reference["responses"][index]
                    check(execution.validate_tokens(response["input_ids"]) == ids, "encoder token IDs differ")
                    _, canonical[phase] = execution.canonical_output(request, response["output"], "native" if phase == "historical_native" else phase)
                for name, (left, right) in COMPARISONS.items():
                    collected[name].append(execution.comparison.compare_backends(canonical[left], canonical[right]))
                check(collected["native_python"][-1] == phases["native"]["report"]["comparisons"][index]["source_fp32"] and
                      collected["metal_python"][-1] == phases["metal"]["report"]["comparisons"][index]["source_fp32"] and
                      collected["metal_historical_native"][-1] == phases["metal"]["report"]["comparisons"][index]["same_artifact_native"], "raw stored comparison differs")
                same_expected = {"case_id": case["id"], "input_ids_u32_le_sha256": hashlib.sha256(b"".join(token.to_bytes(4, "little") for token in ids)).hexdigest(), **collected["metal_native_same_executable"][-1]}
                check(same_expected == same_rows[variant]["comparisons"][index], "retrospective comparison differs")
            models.append({"model": variant, "source": expected_source, "reports": report_rows,
                "comparisons": {name: summary(rows) for name, rows in collected.items()},
                "quality": {"metric_families": len(metrics), "all_four_phases_identical": True, "metrics_sha256": digest(metrics),
                    "entity_exact": metrics["entity_exact"], "fixed_ontology_macro_f1": old[variant]["fixed_ontology_macro_f1"],
                    "fresh_native_micro_f1_delta": 0, "metal_micro_f1_delta": 0}})
            for pin in expected_source["source_files"]: stream_pin(args.models / variant / pin["path"], pin)
    check(not owner.exists(), "private replay owner not removed")
    stream_pin(args.binary, {"size_bytes": 14600280, "sha256": WORKER_SHA})
    for path, expected in consumed.items(): pinned(path, expected)
    check(execution.contract_files() == helpers, "helpers changed during replay")
    value = {"format_version": 1, "scope": SCOPE, "status": "audited", "qualification": False, "model_execution": False,
        "source_commit": history["source_commit"], "corpus_revision": history["corpus_revision"],
        "historical_ledger": {"path": "scripts/gliner25/crossner_ai_execution.json", **HISTORY_PIN},
        "dataset": {"id": "crossner_ai", "documents": 431, "gold_entities": 1809, "lock_sha256": history["lock_sha256"],
            "prepared_receipt_sha256": history["prepared_receipt_sha256"], "requests_sha256": receipt["requests_sha256"], "gold_sha256": receipt["gold_sha256"],
            "ontology": history["ontology"], "offset_unit": history["offset_unit"], "request_options": history["request_options"], "policy": history["evaluation_policy"]},
        "worker": {"size_bytes": 14600280, "sha256": WORKER_SHA, "build_mode": "ReleaseFast", "math_policy": "strict_f32_activations_v1",
            "weight_precision": "fp32", "activation_precision": "f32", "accumulation_precision": "f32", "head_precision": "f32"},
        "contract_files": helpers, "audit_receipts": AUDITS, "audit_tools": AUDIT_TOOLS,
        "official_bio": {"reports": 15, "cpu_reports": 12, "metal_reports": 3, "scored_document_instances": 6465, "metric_family_comparisons": 225,
            "scorer_sha256": bio.SCORER_SHA256, "representation": official["representation"]},
        "capture_tool": {"path": "scripts/gliner25/" + Path(__file__).name, "sha256": oracle.sha256_file(Path(__file__))},
        "models": models, "totals": {"fresh_native_results": 1293, "metal_results": 1293, "same_executable_comparisons": 1293,
            "all_recorded_reference_comparisons": 5172, "errors": 0, "unprocessed": 0},
        "claims": {"original_fp32_short_english_entity_parity": True, "same_executable_cpu_metal_parity": True,
            "new_quality_floor": False, "benchmark": False, "release_qualification": False, "reduced_metal_holdout_qualification": False},
        "evidence_limits": ["One reconstructed short English entity corpus, fixed full fourteen-label ontology, 431 official rows including duplicates.",
            "The compact ledger retains provenance and summaries; full raw-output replay still requires the pinned capture files or a new documented model campaign.",
            "The historical CPU ledger and Metal report reference hashes remain unchanged; same-executable comparison is retrospective.",
            "Corpus-specific source quality is unchanged by these backends. No absolute quality floor, performance, other-task/language, reduced-Metal or release claim."]}
    return validate(value)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--validate", type=Path)
    parser.add_argument("--captures", type=Path, default=Path("/private/tmp"))
    parser.add_argument("--models", type=Path, default=Path("/private/tmp/antfly-gliner25-models"))
    parser.add_argument("--prepared", type=Path, default=Path("/private/tmp/antfly-gliner25-eval-data/crossner-ai-locked-v2/prepared"))
    parser.add_argument("--binary", type=Path, default=Path("/private/tmp/antfly-gliner25-frozen-workers/bundle-check-" + WORKER_SHA))
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.validate:
        check(args.output is None, "validation does not write a replacement ledger")
        value = validate(bio.decode(bio.read(args.validate, MAX_LEDGER_BYTES)))
    else:
        check(args.output is not None, "capture requires an explicit new output")
        value = capture(args)
        raw = (json.dumps(value, sort_keys=True, indent=2, allow_nan=False) + "\n").encode()
        check(len(raw) <= MAX_LEDGER_BYTES, "ledger byte budget exceeded")
        name = None
        try:
            with tempfile.NamedTemporaryFile(dir=args.output.parent, prefix=".crossner-metal-ledger-", delete=False) as staging:
                name = Path(staging.name)
                staging.write(raw)
                staging.flush()
                os.fsync(staging.fileno())
            os.link(name, args.output)
        finally:
            if name is not None:
                name.unlink()
    print(json.dumps({"status": value["status"], "models": len(value["models"]), "same_executable_comparisons": value["totals"]["same_executable_comparisons"], "qualification": False}))


if __name__ == "__main__":
    main()
