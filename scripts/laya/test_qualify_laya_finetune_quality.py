# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
import copy
import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from qualify_laya_finetune_quality import (
    load,
    native_run_digest,
    paired_case_intervals,
    score,
    validate_evidence,
)


class QualityGateTest(unittest.TestCase):
    def setUp(self):
        before = {"examples": 160, "soft_ce": 1.1, "accuracy": 0.7, "ordinal_mae": 0.4}
        after = {"examples": 160, "soft_ce": 0.8, "accuracy": 0.8, "ordinal_mae": 0.3}
        self.native = {
            "status": "complete",
            "objective": "soft_ce",
            "train_sha256": "train",
            "eval_sha256": "eval",
            "optimizer": {"optimizer_step": 80},
            "initial_eval": before,
            "final_eval": after,
            "final_uncalibrated_eval": after,
            "initial_by_kind": [dict(before, examples=40) for _ in range(3)],
            "final_by_kind": [dict(after, examples=40) for _ in range(3)],
        }
        self.reference = {
            "data_sha256": {"train": "train", "eval": "eval"},
            "replay": {"objective": "soft_ce", "updates": 80},
            "initial": before,
            "initial_serving_calibrated": dict(before, soft_ce=0.95),
            "final": after,
            "by_domain": {str(i): dict(after, examples=40) for i in range(4)},
        }
        self.gates = {
            "max_overall_accuracy_regression": 0.025,
            "max_per_type_accuracy_regression": 0.05,
            "max_per_type_soft_ce_regression": 0.05,
            "max_ordinal_mae_regression": 0.05,
            "matched_torch_final_ce_max_abs_difference": 0.05,
            "matched_torch_accuracy_max_abs_difference": 0.05,
        }

    def test_evidence_binds_recipe_source_calibration_and_order(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            native, replay = root / "native", root / "replay"
            native.mkdir()
            replay.mkdir()

            def write(path, value):
                path.write_text(json.dumps(value))

            def sha(path):
                return hashlib.sha256(path.read_bytes()).hexdigest()

            source = {}
            for key, name in (
                ("weights", "model.safetensors"),
                ("config", "config.json"),
                ("tokenizer", "tokenizer.json"),
            ):
                (root / name).write_text(name)
                source[key] = sha(root / name)
            job = {
                "model_dir": str(root),
                "output_dir": str(native),
                "backend": "cpu",
                "objective": "soft_ce",
                "seed": 42,
                "resume_from": None,
                "epochs": 2,
                "batch_size": 1,
                "gradient_accumulation": 4,
            }
            hashes = {}
            for split in ("train", "eval", "calibration"):
                path = root / (split + ".jsonl")
                path.write_text(split)
                hashes[split] = sha(path)
                job[split + "_file"] = str(path)
            event = {"event": "step", "epoch": 1, "batch": 1, "record_ids": ["first"]}
            for target in (native, replay):
                write(target / "job.json", dict(job, output_dir=str(target)))
                write(target / "metrics.jsonl", event)
            report = dict(
                backend="cpu",
                objective="soft_ce",
                source_sha256=source,
                optimizer={"microbatch_step": 1},
                **{key + "_sha256": value for key, value in hashes.items()},
            )
            reference = {
                "weights_sha256": source["weights"],
                "source_sha256": source,
                "data_sha256": hashes,
                "replay": {
                    "native_job_sha256": sha(replay / "job.json"),
                    "microbatches": 1,
                },
            }
            report["run_sha256"] = native_run_digest(native / "job.json")
            write(native / "report.json", report)
            write(root / "reference.json", reference)

            def check():
                return validate_evidence(
                    native / "report.json",
                    root / "reference.json",
                    "matched-replay",
                    replay,
                )

            self.assertTrue(check()["passed"])
            write(native / "job.json", dict(job, seed=43))
            with self.assertRaisesRegex(ValueError, "run identity differs"):
                check()
            write(native / "job.json", job)
            write(native / "metrics.jsonl", dict(event, record_ids=["wrong"]))
            with self.assertRaisesRegex(ValueError, "order"):
                check()
            write(native / "metrics.jsonl", event)
            (root / "calibration.jsonl").write_text("changed")
            with self.assertRaisesRegex(ValueError, "run identity differs"):
                check()
            (root / "calibration.jsonl").write_text("calibration")
            (root / "tokenizer.json").write_text("changed")
            with self.assertRaisesRegex(ValueError, "run identity differs"):
                check()

    def test_strict_json_rejects_overflow_duplicates_and_nan(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "report.json"
            for payload in ('{"x":1e999}', '{"x":1,"x":2}', '{"x":NaN}'):
                path.write_text(payload)
                with self.assertRaises(ValueError):
                    load(path)

    def test_invalid_accuracy_and_bool_counter_fail(self):
        self.native["final_eval"]["accuracy"] = 1.2
        self.assertFalse(score(self.native, self.reference, self.gates)["passed"])
        self.native["final_eval"]["accuracy"] = 0.8
        self.native["optimizer"]["optimizer_step"] = True
        self.assertFalse(score(self.native, self.reference, self.gates)["passed"])

    def test_matched_improvement_passes(self):
        self.assertTrue(score(self.native, self.reference, self.gates)["passed"])

    def test_calibration_cannot_hide_a_worse_training_result(self):
        self.native["final_uncalibrated_eval"] = dict(
            self.native["final_eval"], soft_ce=1.2
        )
        self.assertFalse(score(self.native, self.reference, self.gates)["passed"])

    def test_improvement_over_raw_source_is_not_enough(self):
        self.reference["initial_serving_calibrated"]["soft_ce"] = 0.7
        self.assertFalse(score(self.native, self.reference, self.gates)["passed"])

    def test_aggregate_improvement_cannot_hide_type_regression(self):
        self.native["final_by_kind"][1]["accuracy"] = 0.3
        self.assertFalse(score(self.native, self.reference, self.gates)["passed"])

    def test_baseline_mode_never_claims_an_independent_training_replay(self):
        self.native["objective"] = "rlcd"
        result = score(self.native, self.reference, self.gates, "source-baseline")
        self.assertTrue(result["passed"])
        self.assertFalse(result["independent_training_replay"])
        self.assertNotIn(
            "matched_final_ce", {check["name"] for check in result["checks"]}
        )
        self.assertFalse(score(self.native, self.reference, self.gates)["passed"])

    def test_wrong_data_incomplete_missing_type_and_nonfinite_fail(self):
        for mutation in (
            lambda n: n.update(eval_sha256="wrong"),
            lambda n: n.update(status="paused"),
            lambda n: n.update(objective="rlcd"),
            lambda n: n.update(final_by_kind=[]),
            lambda n: n["final_eval"].update(soft_ce=float("nan")),
        ):
            with self.subTest(mutation=mutation):
                native = copy.deepcopy(self.native)
                mutation(native)
                self.assertFalse(score(native, self.reference, self.gates)["passed"])

    def test_intervals_resample_cases_not_individual_decisions(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            records, before, after = [], [], []
            for case in range(32):
                for _ in range(5):
                    records.append(
                        {"group_id": str(case), "kind": "choice", "target": [1, 0]}
                    )
                    logits = [0, 1] if case < 16 else [1, 0]
                    before.append(
                        {"kind": "choice", "target": [1, 0], "logits": logits}
                    )
                    after.append(
                        {"kind": "choice", "target": [1, 0], "logits": logits[::-1]}
                    )
            data = root / "eval.jsonl"
            data.write_text("".join(json.dumps(row) + "\n" for row in records))
            import math

            baseline = {
                "examples": 160,
                "soft_ce": math.log(1 + math.exp(1)) - 0.5,
                "accuracy": 0.5,
                "ordinal_mae": None,
            }
            objects = {
                "config.json": {"laya": {"temperature": [1, 1, 1]}},
                "job.json": {"model_dir": directory, "eval_file": str(data)},
                "report.json": {
                    "temperature": [1, 1, 1],
                    "initial_eval": baseline,
                    "final_eval": baseline,
                    "final_uncalibrated_eval": baseline,
                    "initial_by_kind": [baseline, None, None],
                    "final_by_kind": [baseline, None, None],
                    "eval_sha256": hashlib.sha256(data.read_bytes()).hexdigest(),
                },
                "initial_eval_predictions.json": before,
                "eval_predictions.json": after,
            }
            for name, value in objects.items():
                (root / name).write_text(json.dumps(value))
            result = paired_case_intervals(root / "report.json", repetitions=2000)
            self.assertEqual(result["cases"], 32)
            self.assertAlmostEqual(result["soft_ce"]["mean_difference"], 0)
            low, high = result["soft_ce"]["paired_case_95_percent_interval"]
            self.assertLess(low, -0.3)
            self.assertGreater(high, 0.3)
            report = root / "report.json"
            corrupt = json.loads(report.read_text())
            corrupt["final_eval"] = dict(baseline, accuracy=0.9)
            report.write_text(json.dumps(corrupt))
            with self.assertRaisesRegex(ValueError, "differs from predictions"):
                paired_case_intervals(report, repetitions=10)
            report.write_text(json.dumps(objects["report.json"]))
            data.write_text(
                data.read_text().replace('"group_id": "0"', '"group_id": "changed"')
            )
            with self.assertRaisesRegex(ValueError, "changed after training"):
                paired_case_intervals(root / "report.json", repetitions=10)


if __name__ == "__main__":
    unittest.main()
