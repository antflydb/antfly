from __future__ import annotations

import copy
import json
import tempfile
import unittest
from pathlib import Path

from compare_gliner2_lora_python_zig import COMPARISON_CONTRACT, UPSTREAM_SAMPLING_DEFAULTS
from evaluate_gliner2_full_task import EVALUATION_CONTRACT, REQUIRED_MINIMA
from gliner2_release_contract import (
    CANONICAL_GLINER2_VERSION,
    CANONICAL_NORMALIZATION,
    CANONICAL_ORACLE_PACKAGE_VERSIONS,
    CANONICAL_UNICODE_VERSION,
    UPSTREAM_COMMIT,
)
from summarize_gliner2_convergence import (
    DERIVED_CONTRACT,
    EVIDENCE_CONTRACT,
    INPUT_CONTRACT,
    STOCK_STOCHASTIC_TRAINING_POLICY,
    materialize_study,
    stochastic_comparison_errors,
    summarize,
)
from validate_gliner2_release_data import adapter_bundle_fingerprint, peft_adapter_fingerprint


SHA_MODEL = "sha256:" + "1" * 64
SHA_TRAIN = "sha256:" + "2" * 64
SHA_EVAL = "sha256:" + "3" * 64
SHA_REPORT = "sha256:" + "4" * 64


def metrics(value: float) -> dict:
    result: dict = {}
    for key in REQUIRED_MINIMA:
        section, name = key.split(".", 1)
        result.setdefault(section, {})[name] = value
    return result


def oracle() -> dict:
    return {
        "commit": UPSTREAM_COMMIT,
        "checkout": "/oracle",
        "imported_module": "/oracle/gliner2/model.py",
        "python_version": "3.12.10",
        "unicode_version": CANONICAL_UNICODE_VERSION,
        "gliner2_version": CANONICAL_GLINER2_VERSION,
        "package_versions": CANONICAL_ORACLE_PACKAGE_VERSIONS,
    }


def passing_payload() -> dict:
    return {
        "contract": DERIVED_CONTRACT,
        "evidence_contract": EVIDENCE_CONTRACT,
        "oracle": {
            "commit": UPSTREAM_COMMIT,
            "checkout": "/oracle",
            "python_version": "3.12",
            "unicode_version": CANONICAL_UNICODE_VERSION,
            "gliner2_version": CANONICAL_GLINER2_VERSION,
            "package_versions": CANONICAL_ORACLE_PACKAGE_VERSIONS,
        },
        "normalization": CANONICAL_NORMALIZATION,
        "unicode_version": CANONICAL_UNICODE_VERSION,
        "training_policy": STOCK_STOCHASTIC_TRAINING_POLICY,
        "fingerprints": {"base_model": SHA_MODEL, "train_data": SHA_TRAIN, "eval_data": SHA_EVAL},
        "artifact_paths": {"base_model": "/model", "train_data": "/train", "eval_data": "/eval"},
        "minimums": {key: 0.7 for key in REQUIRED_MINIMA},
        "runs": [
            {
                "seed": seed,
                "comparison_report": {"path": f"/run-{seed}/comparison_report.json", "sha256": SHA_REPORT},
                "python": {
                    "metrics": metrics(0.82),
                    "optimizer_steps": 2,
                    "losses": [2.0, 1.0],
                    "oracle": oracle(),
                    "adapter": {"path": f"/run-{seed}/python/final", "fingerprint": "sha256:" + "5" * 64},
                    "evaluation_report": {"path": f"/run-{seed}/python-eval.json", "sha256": SHA_REPORT},
                },
                "zig": {
                    "metrics": metrics(0.81),
                    "optimizer_steps": 2,
                    "losses": [2.0, 1.1],
                    "adapter": {"path": f"/run-{seed}/zig", "fingerprint": "sha256:" + "6" * 64},
                    "evaluation_report": {"path": f"/run-{seed}/zig-eval.json", "sha256": SHA_REPORT},
                },
            }
            for seed in range(5)
        ],
    }


class ConvergenceSummaryTest(unittest.TestCase):
    def test_stock_stochastic_gate_rejects_disabled_sampling_or_runtime_checks(self) -> None:
        config = {
            "deterministic": False,
            "python_sampling_policy": "upstream-default",
            "python_schema_conditioning_policy": "upstream-training-default",
            "python_train_shuffle": True,
            "disable_python_model_dropout": False,
            "lora_dropout": 0.0,
            "span_negative_mask_rate": 0.5,
        }
        strict_checks = {
            "requested_step_count_valid": True,
            "adapter_roundtrip_ok": True,
            "metal_manifest_backend_is_metal": True,
            "metal_optimizer_backend_is_metal": True,
            "metal_device_resident_transfers_zero": True,
            "metal_finite_step_loss": True,
            "metal_training_precision_fp32": True,
            "metal_graph_executor_fallback_reasons_empty": True,
            "metal_graph_executor_true_host_outputs_within_threshold": True,
            "metal_interpreter_fallbacks_within_threshold": True,
        }
        summary = {"strict_mode": True, "strict_checks": strict_checks}
        python_result = {"metrics": {
            "sampling_policy": "upstream-default",
            "sampling_config": UPSTREAM_SAMPLING_DEFAULTS,
            "schema_conditioning_policy": "upstream-training-default",
            "training_deterministic": False,
            "train_shuffle": True,
            "configured_dropout_modules": 3,
            "disabled_dropout_modules": 0,
        }}
        zig_result = {"training_manifest": {
            "deterministic": False,
            "sampling_config": "disabled",
            "schema_conditioning_policy": "deterministic-eval-form",
            "model_dropout": "disabled",
            "train_shuffle": True,
            "lora_dropout": 0.0,
            "span_negative_mask_rate": 0.5,
        }}
        self.assertEqual([], stochastic_comparison_errors(config, summary, python_result, zig_result))

        disabled = copy.deepcopy(python_result)
        disabled["metrics"]["sampling_policy"] = "disabled"
        self.assertIn("sampling policy", " ".join(
            stochastic_comparison_errors(config, summary, disabled, zig_result)
        ))
        mistyped_sampling = copy.deepcopy(python_result)
        mistyped_sampling["metrics"]["sampling_config"]["remove_entities_prob"] = False
        self.assertIn("SamplingConfig differs", " ".join(
            stochastic_comparison_errors(config, summary, mistyped_sampling, zig_result)
        ))
        mistyped_config = copy.deepcopy(config)
        mistyped_config["lora_dropout"] = False
        self.assertIn("LoRA dropout", " ".join(
            stochastic_comparison_errors(mistyped_config, summary, python_result, zig_result)
        ))
        pinned_conditioning = copy.deepcopy(python_result)
        pinned_conditioning["metrics"]["schema_conditioning_policy"] = "deterministic-eval-form"
        self.assertIn("schema conditioning", " ".join(
            stochastic_comparison_errors(config, summary, pinned_conditioning, zig_result)
        ))
        weakened_zig = copy.deepcopy(zig_result)
        weakened_zig["training_manifest"]["span_negative_mask_rate"] = 0.0
        self.assertIn("span_negative_mask_rate", " ".join(
            stochastic_comparison_errors(config, summary, python_result, weakened_zig)
        ))
        failed_runtime = copy.deepcopy(summary)
        failed_runtime["strict_checks"]["metal_device_resident_transfers_zero"] = False
        self.assertIn("metal_device_resident_transfers_zero", " ".join(
            stochastic_comparison_errors(config, failed_runtime, python_result, zig_result)
        ))

        cuda_summary = copy.deepcopy(summary)
        cuda_summary["strict_checks"] = {
            "requested_step_count_valid": True,
            "adapter_roundtrip_ok": True,
            "cuda_manifest_backend_is_cuda": True,
            "cuda_optimizer_backend_is_cuda": True,
            "cuda_device_resident_transfers_zero": True,
            "cuda_device_trainables_resident": True,
            "cuda_finite_step_loss": True,
            "cuda_finite_grad_norm": True,
            "cuda_training_precision_fp32": True,
            "cuda_runtime_telemetry_present": True,
            "cuda_full_step_h2d_accounted": True,
            "cuda_parameter_state_h2d_zero": True,
            "cuda_graph_executor_dispatches_nonzero": True,
            "cuda_graph_executor_fallback_reasons_empty": True,
            "cuda_graph_executor_true_host_outputs_zero": True,
            "cuda_interpreter_fallbacks_within_threshold": True,
            "cuda_bulk_d2h_eliminated": True,
            "cuda_d2h_within_threshold": True,
            "cuda_only_scalar_metrics_downloaded": True,
            "cuda_upload_synchronizations_bounded": True,
            "cuda_packed_attention_exercised": True,
            "cuda_exact_gelu_exercised": True,
        }
        self.assertEqual(
            [],
            stochastic_comparison_errors(config, cuda_summary, python_result, zig_result, "cuda"),
        )
        del cuda_summary["strict_checks"]["cuda_graph_executor_dispatches_nonzero"]
        self.assertIn(
            "cuda_graph_executor_dispatches_nonzero",
            " ".join(stochastic_comparison_errors(config, cuda_summary, python_result, zig_result, "cuda")),
        )
        cuda_summary["strict_checks"]["cuda_graph_executor_dispatches_nonzero"] = True
        cuda_summary["strict_checks"]["cuda_optimizer_backend_is_cuda"] = False
        self.assertIn(
            "cuda_optimizer_backend_is_cuda",
            " ".join(stochastic_comparison_errors(config, cuda_summary, python_result, zig_result, "cuda")),
        )

    def test_five_seed_quality_and_optimizer_contract_passes(self) -> None:
        result = summarize(passing_payload())
        self.assertTrue(result["pass"], result["failures"])
        self.assertTrue(result["evidence_bound"])
        self.assertEqual(5, result["seed_count"])
        self.assertTrue(all(row["pass"] for row in result["metrics"].values()))

    def test_independent_tensor_equality_is_not_part_of_contract(self) -> None:
        payload = passing_payload()
        for run in payload["runs"]:
            run["python"]["tensor_digest"] = "different-python"
            run["zig"]["tensor_digest"] = "different-zig"
        self.assertTrue(summarize(payload)["pass"])

    def test_rejects_pair_deficit_step_mismatch_and_divergent_loss(self) -> None:
        payload = passing_payload()
        payload["runs"][0]["zig"]["metrics"] = metrics(0.70)
        payload["runs"][1]["zig"]["optimizer_steps"] = 1
        payload["runs"][2]["zig"]["losses"] = [1.0, 3.0]
        result = summarize(payload)
        self.assertFalse(result["pass"])
        combined = " ".join(result["failures"])
        self.assertIn("paired", combined)
        self.assertIn("optimizer-step mismatch", combined)
        self.assertIn("diverged", combined)

    def test_rejects_wrong_oracle_normalization_and_duplicate_seeds(self) -> None:
        payload = copy.deepcopy(passing_payload())
        payload["oracle"]["commit"] = "wrong"
        payload["normalization"] = "legacy"
        payload["runs"][1]["seed"] = payload["runs"][0]["seed"]
        result = summarize(payload)
        self.assertFalse(result["pass"])
        combined = " ".join(result["failures"])
        self.assertIn("oracle commit", combined)
        self.assertIn("normalization", combined)
        self.assertIn("unique", combined)

    def test_invalid_minimum_reports_failure_instead_of_crashing(self) -> None:
        payload = passing_payload()
        payload["minimums"][next(iter(REQUIRED_MINIMA))] = "not-a-number"
        result = summarize(payload)
        self.assertFalse(result["pass"])
        self.assertIn("invalid minimum", " ".join(result["failures"]))

    def test_manifest_derives_all_values_from_generated_reports_and_adapters(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            model_dir = root / "model"
            model_dir.mkdir()
            train_data = root / "train.jsonl"
            eval_data = root / "eval.jsonl"
            train_data.write_text("{}\n", encoding="utf-8")
            eval_data.write_text("{}\n", encoding="utf-8")
            minimums = {key: 0.7 for key in REQUIRED_MINIMA}
            fingerprints = {"base_model": SHA_MODEL, "train_data": SHA_TRAIN, "eval_data": SHA_EVAL}
            verified = {"commit": UPSTREAM_COMMIT, "checkout": "/oracle"}
            manifest_runs: list[dict] = []
            first_python_adapter: Path | None = None
            for seed in range(5):
                run_dir = root / f"seed-{seed}"
                python_adapter = run_dir / "python" / "final"
                zig_adapter = run_dir / "zig"
                for adapter, prefix in ((python_adapter, b"python"), (zig_adapter, b"zig")):
                    adapter.mkdir(parents=True)
                    (adapter / "adapter_model.safetensors").write_bytes(prefix + b"-weights")
                    (adapter / "adapter_config.json").write_bytes(prefix + b"-config")
                    (adapter / "task_head.safetensors").write_bytes(prefix + b"-head")
                if first_python_adapter is None:
                    first_python_adapter = python_adapter
                comparison_path = run_dir / "comparison_report.json"
                comparison = {
                    "contract": COMPARISON_CONTRACT,
                    "config": {
                        "seed": seed,
                        "model_dir": str(model_dir),
                        "train_data": str(train_data),
                        "model_fingerprint_sha256": SHA_MODEL,
                        "training_data_fingerprint_sha256": SHA_TRAIN,
                        "scoring_normalization": CANONICAL_NORMALIZATION,
                        "zig_backend": "metal",
                        "zig_objective": "gliner2-total-loss",
                        "zig_lora_only_trainables": True,
                        "deterministic": False,
                        "python_sampling_policy": "upstream-default",
                        "python_schema_conditioning_policy": "upstream-training-default",
                        "python_train_shuffle": True,
                        "disable_python_model_dropout": False,
                        "lora_dropout": 0.0,
                        "span_negative_mask_rate": 0.5,
                        "oracle": verified,
                        "steps": 2,
                    },
                    "summary": {
                        "requested_step_count": 2,
                        "step_count_valid": True,
                        "strict_mode": True,
                        "strict_checks": {
                            "requested_step_count_valid": True,
                            "adapter_roundtrip_ok": True,
                            "metal_manifest_backend_is_metal": True,
                            "metal_optimizer_backend_is_metal": True,
                            "metal_device_resident_transfers_zero": True,
                            "metal_finite_step_loss": True,
                            "metal_training_precision_fp32": True,
                            "metal_graph_executor_fallback_reasons_empty": True,
                            "metal_graph_executor_true_host_outputs_within_threshold": True,
                            "metal_interpreter_fallbacks_within_threshold": True,
                        },
                    },
                    "python": {
                        "returncode": 0,
                        "metrics": {
                            "total_steps": 2,
                            "train_metrics_history": [{"loss": 2.0}, {"loss": 1.0}],
                            "oracle": oracle(),
                            "sampling_policy": "upstream-default",
                            "sampling_config": UPSTREAM_SAMPLING_DEFAULTS,
                            "schema_conditioning_policy": "upstream-training-default",
                            "training_deterministic": False,
                            "train_shuffle": True,
                            "configured_dropout_modules": 3,
                            "disabled_dropout_modules": 0,
                        },
                    },
                    "zig": {
                        "returncode": 0,
                        "training_manifest": {
                            "backend": "metal",
                            "training_precision": "fp32",
                            "optimizer_state_precision": "fp32",
                            "objective": "gliner2-total-loss",
                            "lora_only_trainables": True,
                            "deterministic": False,
                            "sampling_config": "disabled",
                            "schema_conditioning_policy": "deterministic-eval-form",
                            "model_dropout": "disabled",
                            "train_shuffle": True,
                            "lora_dropout": 0.0,
                            "span_negative_mask_rate": 0.5,
                            "optimizer_steps": 2,
                        },
                        "training_metrics": [
                            {"event": "step", "loss": 2.0},
                            {"event": "step", "loss": 1.1},
                        ],
                    },
                }
                comparison_path.write_text(json.dumps(comparison), encoding="utf-8")
                eval_paths: dict[str, Path] = {}
                for side, adapter, value in (
                    ("python", python_adapter, 0.82),
                    ("zig", zig_adapter, 0.81),
                ):
                    eval_path = run_dir / f"{side}-evaluation.json"
                    eval_path.write_text(json.dumps({
                        "contract": EVALUATION_CONTRACT,
                        "pass": True,
                        "minimums": minimums,
                        "model_dir": str(model_dir),
                        "adapter_dir": str(adapter),
                        "eval_data": str(eval_data),
                        "inference": {
                            "normalization": CANONICAL_NORMALIZATION,
                            "unicode_version": CANONICAL_UNICODE_VERSION,
                        },
                        "oracle": oracle(),
                        "artifacts": {
                            "base_model_fingerprint_sha256": SHA_MODEL,
                            "adapter_bundle_fingerprint_sha256": adapter_bundle_fingerprint(adapter),
                            "peft_adapter_fingerprint_sha256": peft_adapter_fingerprint(adapter),
                            "eval_data_fingerprint_sha256": SHA_EVAL,
                        },
                        "metrics": metrics(value),
                    }), encoding="utf-8")
                    eval_paths[side] = eval_path
                manifest_runs.append({
                    "seed": seed,
                    "comparison_report": str(comparison_path),
                    "python_evaluation_report": str(eval_paths["python"]),
                    "zig_evaluation_report": str(eval_paths["zig"]),
                })
            manifest = {"contract": INPUT_CONTRACT, "minimums": minimums, "runs": manifest_runs}
            derived = materialize_study(
                manifest,
                manifest_dir=root,
                model_dir=model_dir,
                train_data=train_data,
                eval_data=eval_data,
                fingerprints=fingerprints,
                verified_oracle=verified,
            )
            result = summarize(derived)
            self.assertTrue(result["pass"], result["failures"])
            self.assertEqual(0.82, result["runs"][0]["python"]["metrics"]["entities.micro_f1"])

            inline = copy.deepcopy(manifest)
            inline["metrics"] = metrics(1.0)
            with self.assertRaisesRegex(ValueError, "may contain only"):
                materialize_study(
                    inline,
                    manifest_dir=root,
                    model_dir=model_dir,
                    train_data=train_data,
                    eval_data=eval_data,
                    fingerprints=fingerprints,
                    verified_oracle=verified,
                )

            assert first_python_adapter is not None
            (first_python_adapter / "adapter_model.safetensors").write_bytes(b"changed")
            with self.assertRaisesRegex(ValueError, "artifact fingerprints"):
                materialize_study(
                    manifest,
                    manifest_dir=root,
                    model_dir=model_dir,
                    train_data=train_data,
                    eval_data=eval_data,
                    fingerprints=fingerprints,
                    verified_oracle=verified,
                )


if __name__ == "__main__":
    unittest.main()
