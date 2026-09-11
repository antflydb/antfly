#!/usr/bin/env python3
"""Reconstruct every completed native CrossNER result without loading a model.

This is evidence review, not another model execution or an independent BIO
metric implementation. All prepared data, raw outputs and reports are read-only.
Only the explicitly named evidence ledger is written after every check passes.
"""
from __future__ import annotations

import argparse
from pathlib import Path

import check_bundles as comparison
import evaluate as runner
import evaluation_contract as evaluation
import oracle


def review(ledger_path: Path, lock: Path, prepared: Path, captures: Path):
    ledger = oracle.read_json(ledger_path)
    evaluation.checked(ledger["qualification"] is False and ledger["contract_files"] == runner.contract_files(),
                       "ledger execution contract differs")
    original = {(row["model"], row["precision"]): row for row in ledger["native_reduced_precision"]}
    rows = []
    for model_row in ledger["models"]:
        model = model_row["model"]
        fixture = runner.prepare_fixture(lock, prepared, model)
        source_pin = model_row["reports"]["python"]
        evaluation.checked(oracle.sha256_file(Path(source_pin["path"])) == source_pin["sha256"], "source report pin differs")
        source = runner.load_reference(Path(source_pin["path"]), fixture, prepared, "python")
        evaluation.checked(source["metrics"]["metrics"]["entity_exact"] == model_row["entity_exact"], "source ledger metrics differ")
        for precision in ("fp32", "fp16_encoder", "q8_0", "q4_0" if model == "small" else "q4_k"):
            path = captures / f"gliner25-crossner-native-{model}-{precision}" / "report.json"
            report = oracle.read_json(path)
            artifact = report["artifact"]
            expected_source = runner.source_identity(model)
            evaluation.checked(artifact["model_id"] == expected_source["model_id"] and
                               artifact["revision"] == expected_source["revision"] and artifact["precision"] == precision and
                               artifact["kind"] == ("source_fp32" if precision == "fp32" else "bundle") and
                               report["binary_sha256"] == model_row["reports"]["native"]["binary_sha256"] and
                               report["source_reference_report_sha256"] == source["sha256"] and
                               report["completed_results"] == 431 and report["unprocessed"] == 0, "native report identity/count differs")
            actual = runner.load_reference(path, fixture, prepared, "native", artifact)
            comparisons = []
            for case, expected_response, response in zip(fixture["cases"], source["responses"], actual["responses"]):
                ids = response["input_ids"]
                evaluation.checked(ids == expected_response["input_ids"], "encoder token IDs differ from original FP32")
                request = {key: case[key] for key in ("text", "schema", "options", "offset_unit")}
                _, expected = runner.canonical_output(request, expected_response["output"], "python")
                _, observed = runner.canonical_output(request, response["output"], "native")
                comparisons.append({"case_id": case["id"], "input_ids_u32_le_sha256": evaluation.digest(
                    b"".join(token.to_bytes(4, "little") for token in ids)),
                    "source_fp32": comparison.compare_backends(expected, observed)})
            evaluation.checked(comparisons == report["comparisons"], "stored raw-output comparison differs")
            delta = runner.quality_delta(source["metrics"], actual["metrics"])
            evaluation.checked(delta == report["source_fp32_quality_delta"], "stored source quality delta differs")
            if precision == "fp32":
                evaluation.checked(actual["sha256"] == model_row["reports"]["native"]["sha256"] and
                                   all(row["source_fp32"]["parity_pass"] for row in comparisons) and
                                   actual["metrics"] == source["metrics"], "original FP32 parity differs")
                continue
            metrics = actual["metrics"]["metrics"]
            maximum_loss = 1.0 if precision.startswith("q4_") else 0.5
            violations = [{"scope": name, "support": row["support"], "loss_percentage_points": row["micro_f1_loss"] * 100}
                          for name, row in sorted(delta.items()) if row["micro_f1_loss"] * 100 > maximum_loss]
            compared = [row["source_fp32"] for row in comparisons]
            row = {"model": model, "precision": precision, "backend": "native", "artifact": artifact,
                "report": {"path": str(path), "sha256": actual["sha256"], "binary_sha256": report["binary_sha256"],
                           "source_reference_report_sha256": source["sha256"], "files": report["files"]},
                "completed_results": report["completed_results"], "errors": report["errors"], "unprocessed": report["unprocessed"],
                "all_token_sequences_equal": True, "all_raw_comparisons_recomputed": True,
                "entity_exact": metrics["entity_exact"],
                "per_type": {name: metrics["entity_type/" + name] for name in ledger["ontology"]},
                "fixed_ontology_macro_f1": sum(metrics["entity_type/" + name]["micro_f1"] for name in ledger["ontology"]) / len(ledger["ontology"]),
                "source_fp32_comparison": {"cases": len(compared), "confidence_absolute_tolerance": comparison.CONFIDENCE_TOLERANCE,
                    "ordered_decisions_equal_cases": sum(row["decisions_equal"] for row in compared),
                    "confidence_and_decision_tolerance_pass_cases": sum(row["parity_pass"] for row in compared),
                    "maximum_aligned_confidence_absolute_error": max(row["max_aligned_confidence_absolute_error"] for row in compared)},
                "source_fp32_quality_delta": delta,
                "relative_quality_gate": {"maximum_loss_percentage_points": maximum_loss, "evaluated_scopes": len(delta),
                    "status": "fail" if violations else "pass_for_this_corpus_only", "violations": violations,
                    "absolute_quality_floor": None, "qualification": False}, "qualification": False}
            if (model, precision) in original:
                evaluation.checked(row == original[model, precision], "previously reviewed reduced evidence changed")
            rows.append(row)
    evaluation.checked(len(rows) == 9, "all nine native reduced profiles are required")
    ledger["native_reduced_precision"] = rows
    ledger["status"] = "all_three_source_fp32_and_nine_native_reduced_profiles_reviewed"
    ledger["evidence_limits"][-1] = "Metal held-out runs are not included in this evidence version."
    ledger["review_method"][-1] = "Recompute every stored source_fp32 comparison and every reduced quality delta; original FP32 metric families must be identical across Python and native."
    ledger["review_tool"] = {"path": Path(__file__).name, "sha256": oracle.sha256_file(Path(__file__)),
                           "model_execution": False, "native_reports_reviewed": 12, "native_results_reviewed": 12 * 431}
    return ledger


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ledger", type=Path, default=oracle.HERE / "crossner_ai_execution.json")
    parser.add_argument("--lock", type=Path, default=Path("/private/tmp/antfly-gliner25-eval-data/crossner-ai-locked-v2/lock.json"))
    parser.add_argument("--prepared-dir", type=Path, default=Path("/private/tmp/antfly-gliner25-eval-data/crossner-ai-locked-v2/prepared"))
    parser.add_argument("--captures-dir", type=Path, default=Path("/private/tmp"))
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = review(args.ledger, args.lock, args.prepared_dir, args.captures_dir)
    oracle.write_json(args.output, result)
    for row in result["native_reduced_precision"]:
        print(row["model"], row["precision"], f"F1={row['entity_exact']['micro_f1'] * 100:.6f}%",
              f"delta={row['source_fp32_quality_delta']['entity_exact']['micro_f1_delta'] * 100:+.6f}pp",
              row["relative_quality_gate"])
    print("ledger", oracle.sha256_file(args.output), "qualification=false")


if __name__ == "__main__":
    main()
