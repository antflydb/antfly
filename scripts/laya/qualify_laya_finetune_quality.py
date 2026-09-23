#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Score a completed native Laya campaign against its independent Torch replay.

This is the held-out quality gate only. Gradient, export, lifecycle, and resource
evidence are separate prerequisites for a production qualification report.
"""

import argparse
import hashlib
import json
import math
import random
from pathlib import Path


def load(path):
    def invalid(value):
        raise ValueError(f"Non-finite JSON number: {value}")

    def finite(value):
        result = float(value)
        if not math.isfinite(result):
            invalid(value)
        return result

    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError(f"Duplicate JSON key: {key}")
            result[key] = value
        return result

    return json.loads(
        path.read_text(),
        parse_constant=invalid,
        parse_float=finite,
        object_pairs_hook=unique,
    )


def digest(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def native_run_digest(job_path):
    """Reproduce v1 native identity without rounding its serialized FP32 recipe.

    job.json and the hashed identity use the same Zig serializer. Retain its
    float lexemes while removing whitespace and clearing relocation fields.

    This consumes the trainer-written job.json, whose key order is the field
    declaration order of finetune/laya/job.zig Config, not arbitrary JSON.
    Preserve that order: sorting or hand-reordering keys changes the v1 hash
    and validate_evidence rejects the edited artifact as an identity mismatch.
    """

    class FloatLexeme(str):
        pass

    def encode(value):
        if isinstance(value, FloatLexeme):
            return str(value)
        if isinstance(value, dict):
            return (
                "{"
                + ",".join(
                    json.dumps(key, ensure_ascii=False) + ":" + encode(item)
                    for key, item in value.items()
                )
                + "}"
            )
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"))

    load(job_path)  # Validate duplicates and finite numbers first.
    job = json.loads(job_path.read_text(), parse_float=FloatLexeme)
    hasher = hashlib.sha256(b"antfly-laya-training/v1")
    for name in ("config.json", "tokenizer.json", "model.safetensors"):
        with (Path(job["model_dir"]) / name).open("rb") as stream:
            for block in iter(lambda: stream.read(1024 * 1024), b""):
                hasher.update(block)
    for field in ("train_file", "eval_file", "calibration_file"):
        if job.get(field):
            hasher.update(bytes.fromhex(digest(job[field])))
    job.update(output_dir="", resume_from=None, stop_after_microbatches=None)
    hasher.update(encode(job).encode())
    return hasher.hexdigest()


def validate_evidence(
    native_path, reference_path, reference_mode, replay_run=None, legacy_manifest=None
):
    """Bind quality comparisons to source assets, all splits, recipe and order.

    Old v1 reports did not capture source metadata digests. Re-scoring them
    requires their retained immutable evidence manifest; never infer identity
    merely from equal aggregate losses.
    """
    native, reference = load(native_path), load(reference_path)
    directory = native_path.parent
    job = load(directory / "job.json")
    sealed = load(legacy_manifest)["files"] if legacy_manifest else None

    def require_sealed(path):
        if sealed is None:
            raise ValueError("Legacy reports require --legacy-manifest")
        key = str(path.resolve().relative_to(legacy_manifest.parent.resolve()))
        if key not in sealed or digest(path) != sealed[key]["sha256"]:
            raise ValueError(f"Retained evidence changed: {path}")

    if job.get("resume_from") is not None or native.get("resumed_microbatches", 0) != 0:
        raise ValueError("Quality baselines require a fresh training run")
    if job["objective"] != native["objective"] or job["backend"] != native["backend"]:
        raise ValueError("Report and job differ")
    if native_run_digest(directory / "job.json") != native["run_sha256"]:
        raise ValueError(
            "Native run identity differs from its recipe or admitted assets"
        )
    assets = {
        key: Path(job["model_dir"]) / name
        for key, name in (
            ("weights", "model.safetensors"),
            ("config", "config.json"),
            ("tokenizer", "tokenizer.json"),
        )
    }
    actual = {key: digest(path) for key, path in assets.items()}
    if actual["weights"] != reference["weights_sha256"]:
        raise ValueError("Reference source weights differ")
    for report, report_path in ((native, native_path), (reference, reference_path)):
        if "source_sha256" in report:
            if report["source_sha256"] != actual:
                raise ValueError("Source assets differ from admitted training inputs")
        else:
            for path in (report_path, directory / "job.json", *assets.values()):
                require_sealed(path)
    for split in ("train", "eval", "calibration"):
        path = job.get(f"{split}_file")
        observed = digest(path) if path else None
        if observed != reference["data_sha256"].get(split):
            raise ValueError(f"Reference {split} data differs")
        key = f"{split}_sha256"
        if key in native:
            if observed != native[key]:
                raise ValueError(f"Admitted {split} data changed")
        elif path:
            require_sealed(Path(path))
    if "source_sha256" not in native:
        for name in (
            "metrics.jsonl",
            "initial_eval_predictions.json",
            "eval_predictions.json",
        ):
            require_sealed(directory / name)
    if reference_mode == "matched-replay":
        replay_run = replay_run or directory
        replay_job = load(replay_run / "job.json")
        if digest(replay_run / "job.json") != reference["replay"]["native_job_sha256"]:
            raise ValueError(
                "Reference is bound to a different replay job; provide --replay-run"
            )
        ignored = {"backend", "output_dir", "max_host_bytes"}
        if {k: v for k, v in job.items() if k not in ignored} != {
            k: v for k, v in replay_job.items() if k not in ignored
        }:
            raise ValueError("Native and reference training recipes differ")

        def order(root):
            events = [
                json.loads(line)
                for line in (root / "metrics.jsonl").read_text().splitlines()
            ]
            return [
                (row["epoch"], row["batch"], row["record_ids"])
                for row in events
                if row["event"] == "step"
            ]

        steps = order(directory)
        if (
            steps != order(replay_run)
            or len(steps) != reference["replay"]["microbatches"]
            or len(steps) != native["optimizer"]["microbatch_step"]
        ):
            raise ValueError("Training order or microbatch count differs")
        if "source_sha256" not in reference:
            require_sealed(replay_run / "metrics.jsonl")
    return {
        "passed": True,
        "source_sha256": actual,
        "legacy_manifest_sha256": digest(legacy_manifest) if legacy_manifest else None,
    }


def valid_metrics(row):
    if (
        not isinstance(row, dict)
        or type(row.get("examples")) is not int
        or row["examples"] <= 0
    ):
        return False

    def number(v):
        return type(v) in (int, float) and math.isfinite(v)

    return (
        number(row.get("soft_ce"))
        and row["soft_ce"] >= 0
        and number(row.get("accuracy"))
        and 0 <= row["accuracy"] <= 1
        and (
            row.get("ordinal_mae") is None
            or number(row["ordinal_mae"])
            and row["ordinal_mae"] >= 0
        )
    )


def score(native, reference, gates, reference_mode="matched-replay"):
    if reference_mode not in ("matched-replay", "source-baseline"):
        raise ValueError("Unknown reference mode")
    matched = reference_mode == "matched-replay"
    checks = []

    def check(name, passed, **evidence):
        checks.append({"name": name, "passed": bool(passed), **evidence})

    def difference(name, actual, expected, tolerance):
        delta = abs(actual - expected)
        check(
            name,
            math.isfinite(delta) and delta <= tolerance,
            actual=actual,
            expected=expected,
            absolute_difference=delta,
            tolerance=tolerance,
        )

    rows = [
        native[key] for key in ("initial_eval", "final_eval", "final_uncalibrated_eval")
    ]
    rows += native["initial_by_kind"] + native["final_by_kind"]
    rows += [
        reference[key] for key in ("initial", "initial_serving_calibrated", "final")
    ]
    rows += list(reference["by_domain"].values())
    check("valid_metrics", all(valid_metrics(row) for row in rows))
    check(
        "valid_thresholds",
        all(
            type(value) in (int, float) and math.isfinite(value) and value >= 0
            for value in (
                gates[key]
                for key in (
                    "max_overall_accuracy_regression",
                    "max_per_type_accuracy_regression",
                    "max_per_type_soft_ce_regression",
                    "max_ordinal_mae_regression",
                    "matched_torch_final_ce_max_abs_difference",
                    "matched_torch_accuracy_max_abs_difference",
                )
            )
        ),
    )
    check(
        "valid_optimizer_count",
        type(native["optimizer"]["optimizer_step"]) is int
        and native["optimizer"]["optimizer_step"] > 0,
    )
    check("completed_native_run", native["status"] == "complete")
    if matched:
        check(
            "matched_objective", native["objective"] == reference["replay"]["objective"]
        )
    check(
        "matched_training_data",
        native["train_sha256"] == reference["data_sha256"]["train"],
    )
    check(
        "matched_heldout_data",
        native["eval_sha256"] == reference["data_sha256"]["eval"],
    )
    if matched:
        check(
            "matched_optimizer_updates",
            native["optimizer"]["optimizer_step"] == reference["replay"]["updates"],
        )
    check(
        "heldout_coverage",
        native["final_eval"]["examples"] == reference["final"]["examples"]
        and native["final_eval"]["examples"] >= 160,
    )
    check(
        "typed_coverage",
        len(native["initial_by_kind"]) == 3
        and len(native["final_by_kind"]) == 3
        and all(
            row is not None and row["examples"] >= 20 for row in native["final_by_kind"]
        ),
    )
    check(
        "workflow_coverage",
        len(reference["by_domain"]) >= 4
        and all(row["examples"] >= 20 for row in reference["by_domain"].values()),
    )
    difference(
        "matched_initial_ce",
        native["initial_eval"]["soft_ce"],
        reference["initial"]["soft_ce"],
        0.001,
    )
    for name, trained, baseline in (
        (
            "heldout_uncalibrated_ce_improves",
            native["final_uncalibrated_eval"]["soft_ce"],
            native["initial_eval"]["soft_ce"],
        ),
        (
            "heldout_calibrated_ce_improves",
            native["final_eval"]["soft_ce"],
            reference["initial_serving_calibrated"]["soft_ce"],
        ),
    ):
        check(
            name,
            math.isfinite(trained) and trained < baseline,
            trained=trained,
            baseline=baseline,
        )
    check(
        "overall_accuracy",
        native["final_eval"]["accuracy"]
        >= native["initial_eval"]["accuracy"]
        - gates["max_overall_accuracy_regression"],
        initial=native["initial_eval"]["accuracy"],
        final=native["final_eval"]["accuracy"],
    )
    initial_mae = native["initial_eval"]["ordinal_mae"]
    final_mae = native["final_eval"]["ordinal_mae"]
    check(
        "ordinal_mae",
        initial_mae is not None
        and final_mae is not None
        and final_mae <= initial_mae + gates["max_ordinal_mae_regression"],
        initial=initial_mae,
        final=final_mae,
    )
    for kind, before, after in zip(
        ("choice", "score", "noul"), native["initial_by_kind"], native["final_by_kind"]
    ):
        if before is None or after is None:
            check(f"{kind}_coverage", False)
            continue
        check(
            f"{kind}_accuracy",
            after["accuracy"]
            >= before["accuracy"] - gates["max_per_type_accuracy_regression"],
            initial=before["accuracy"],
            final=after["accuracy"],
        )
        check(
            f"{kind}_ce",
            after["soft_ce"]
            <= before["soft_ce"] + gates["max_per_type_soft_ce_regression"],
            initial=before["soft_ce"],
            final=after["soft_ce"],
        )
    if matched:
        difference(
            "matched_final_ce",
            native["final_eval"]["soft_ce"],
            reference["final"]["soft_ce"],
            gates["matched_torch_final_ce_max_abs_difference"],
        )
        difference(
            "matched_final_accuracy",
            native["final_eval"]["accuracy"],
            reference["final"]["accuracy"],
            gates["matched_torch_accuracy_max_abs_difference"],
        )
    return {
        "format": "antfly-laya-finetune-quality/v1",
        "reference_mode": reference_mode,
        "independent_training_replay": matched,
        "passed": all(c["passed"] for c in checks),
        "checks": checks,
    }


def paired_case_intervals(native_path, repetitions=10000):
    """Resample whole held-out cases, preserving their correlated decisions."""
    directory = native_path.parent
    job = load(directory / "job.json")
    report = load(native_path)
    config = load(Path(job["model_dir"]) / "config.json")["laya"]
    record_bytes = Path(job["eval_file"]).read_bytes()
    if hashlib.sha256(record_bytes).hexdigest() != report["eval_sha256"]:
        raise ValueError("Held-out records changed after training")
    records = [json.loads(line) for line in record_bytes.decode().splitlines()]
    before = load(directory / "initial_eval_predictions.json")
    after = load(directory / "eval_predictions.json")
    if (
        not len(records)
        == len(before)
        == len(after)
        == report["final_eval"]["examples"]
    ):
        raise ValueError("Prediction and held-out record counts differ")
    kinds = {"choice": 0, "score": 1, "noul": 2}

    def ce(row, temperature):
        values = [z / temperature for z in row["logits"]]
        peak = max(values)
        logsum = peak + math.log(sum(math.exp(z - peak) for z in values))
        return sum(t * (logsum - z) for t, z in zip(row["target"], values))

    def correct(row):
        return int(
            max(range(len(row["logits"])), key=row["logits"].__getitem__)
            == max(range(len(row["target"])), key=row["target"].__getitem__)
        )

    groups = {}
    raw_before, raw_after, calibrated_after, source_before = [], [], [], []
    for record, initial, final in zip(records, before, after):
        kind = record["kind"]
        if (
            initial["kind"] != kind
            or final["kind"] != kind
            or len(initial["target"]) != len(record["target"])
            or len(final["target"]) != len(record["target"])
        ):
            raise ValueError("Held-out prediction ordering differs")
        for row in (initial, final):
            if ("source_sha256" in report or row.get("id") is not None) and row.get(
                "id"
            ) != record["id"]:
                raise ValueError("Held-out prediction IDs differ")
            if not row["logits"] or any(
                type(z) not in (int, float) or not math.isfinite(z)
                for z in row["logits"]
            ):
                raise ValueError("Invalid prediction logits")
            if (
                len(row["logits"]) != len(row["target"])
                or max(abs(a - b) for a, b in zip(row["target"], record["target"]))
                > 1e-6
            ):
                raise ValueError("Held-out target mismatch")
        count = len(record["target"])
        bucket = (
            "2"
            if count <= 2
            else "3-5"
            if count <= 5
            else "6-10"
            if count <= 10
            else "11+"
        )
        source_temperature = config.get("temperature_by_options", {}).get(
            f"{kind}:{bucket}", config.get("temperature", [1, 1, 1])[kinds[kind]]
        )
        source_temperature = max(0.001, source_temperature)
        final_temperature = report["temperature"][kinds[kind]]
        if not math.isfinite(final_temperature) or final_temperature <= 0:
            raise ValueError("Invalid final temperature")

        def observation(row, temperature, kind=kind):
            z = [v / temperature for v in row["logits"]]
            probabilities = [math.exp(v - max(z)) for v in z]
            total = sum(probabilities)
            mae = (
                abs(
                    sum(
                        i * (p / total - t)
                        for i, (p, t) in enumerate(zip(probabilities, row["target"]))
                    )
                )
                if kind == "score"
                else None
            )
            return {
                "kind": kind,
                "ce": ce(row, temperature),
                "correct": correct(row),
                "mae": mae,
            }

        raw_before.append(observation(initial, 1))
        raw_after.append(observation(final, 1))
        calibrated_after.append(observation(final, final_temperature))
        source_before.append(observation(initial, source_temperature))
        delta_ce = calibrated_after[-1]["ce"] - source_before[-1]["ce"]
        groups.setdefault(record["group_id"], []).append(
            (delta_ce, correct(final) - correct(initial))
        )

    def aggregate(rows):
        ordinal = [r["mae"] for r in rows if r["mae"] is not None]
        return {
            "examples": len(rows),
            "soft_ce": sum(r["ce"] for r in rows) / len(rows),
            "accuracy": sum(r["correct"] for r in rows) / len(rows),
            "ordinal_mae": sum(ordinal) / len(ordinal) if ordinal else None,
        }

    def verify(actual, rows):
        expected = aggregate(rows)
        for key, value in expected.items():
            if value is None:
                if actual[key] is not None:
                    raise ValueError(f"Reported {key} differs from predictions")
            elif (
                type(actual.get(key)) not in (int, float)
                or not math.isfinite(actual[key])
                or abs(actual[key] - value) > 1e-5
            ):
                raise ValueError(f"Reported {key} differs from predictions")

    for key, rows in (
        ("initial_eval", raw_before),
        ("final_eval", calibrated_after),
        ("final_uncalibrated_eval", raw_after),
    ):
        verify(report[key], rows)
    for i, kind in enumerate(kinds):
        for key, rows in (
            ("initial_by_kind", raw_before),
            ("final_by_kind", calibrated_after),
        ):
            subset = [r for r in rows if r["kind"] == kind]
            if subset:
                verify(report[key][i], subset)
            elif report[key][i] is not None:
                raise ValueError("Reported typed coverage differs")
    if len(groups) < 32:
        raise ValueError("Qualification requires at least 32 held-out cases")
    cases = list(groups.values())
    rng = random.Random(20260921)
    differences = [[], []]
    for _ in range(repetitions):
        selected = rng.choices(cases, k=len(cases))
        count = sum(map(len, selected))
        for metric in range(2):
            differences[metric].append(
                sum(row[metric] for case in selected for row in case) / count
            )
    result = {
        "cases": len(cases),
        "decisions": len(records),
        "repetitions": repetitions,
        "seed": 20260921,
        "baseline": "released serving calibration",
        "difference": "trained minus source",
    }
    for metric, name in enumerate(("soft_ce", "accuracy")):
        samples = sorted(differences[metric])
        result[name] = {
            "mean_difference": sum(row[metric] for case in cases for row in case)
            / len(records),
            "paired_case_95_percent_interval": [
                samples[int(repetitions * 0.025)],
                samples[int(repetitions * 0.975)],
            ],
        }
    result["source_calibrated_by_kind"] = {
        kind: aggregate([r for r in source_before if r["kind"] == kind])
        for kind in kinds
        if any(r["kind"] == kind for r in source_before)
    }
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("native", "reference", "gates", "output"):
        parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument(
        "--reference-mode",
        choices=("matched-replay", "source-baseline"),
        default="matched-replay",
        help="Source-baseline mode checks quality only and never claims an independent training replay",
    )
    parser.add_argument(
        "--replay-run",
        type=Path,
        help="Original native run bound by the Torch replay job hash",
    )
    parser.add_argument(
        "--legacy-manifest",
        type=Path,
        help="Sealed evidence manifest required for older reports lacking asset digests",
    )
    args = parser.parse_args()
    binding = validate_evidence(
        args.native,
        args.reference,
        args.reference_mode,
        args.replay_run,
        args.legacy_manifest,
    )
    result = score(
        load(args.native),
        load(args.reference),
        load(args.gates)["quality"],
        args.reference_mode,
    )
    result["evidence_binding"] = binding
    result["case_bootstrap"] = paired_case_intervals(args.native)
    native, gates = load(args.native), load(args.gates)["quality"]
    for kind, after in zip(("choice", "score", "noul"), native["final_by_kind"]):
        before = result["case_bootstrap"]["source_calibrated_by_kind"][kind]
        result["checks"].append(
            {
                "name": f"{kind}_serving_calibrated_ce",
                "passed": after["soft_ce"]
                <= before["soft_ce"] + gates["max_per_type_soft_ce_regression"],
                "initial": before["soft_ce"],
                "final": after["soft_ce"],
            }
        )
    result["passed"] = all(check["passed"] for check in result["checks"])
    result["input_sha256"] = {
        str(path): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in (args.native, args.reference, args.gates)
    }
    with args.output.open("x") as stream:
        json.dump(result, stream, indent=2, allow_nan=False)
        stream.write("\n")
    print(json.dumps(result))
    raise SystemExit(0 if result["passed"] else 1)


if __name__ == "__main__":
    main()
