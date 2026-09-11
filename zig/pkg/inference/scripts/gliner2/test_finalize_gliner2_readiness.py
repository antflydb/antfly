from __future__ import annotations

import copy
import unittest

from evaluate_gliner2_full_task import EVALUATION_CONTRACT, REQUIRED_MINIMA
from evaluate_gliner2_native_release_smoke import NATIVE_EVALUATION_CONTRACT
from finalize_gliner2_readiness import build_summary
from gliner2_release_contract import (
    CANONICAL_GLINER2_VERSION,
    CANONICAL_NORMALIZATION,
    CANONICAL_ORACLE_PACKAGE_VERSIONS,
    CANONICAL_UNICODE_VERSION,
    UPSTREAM_COMMIT,
)
from summarize_gliner2_convergence import (
    EVIDENCE_CONTRACT,
    OUTPUT_CONTRACT,
    STOCK_STOCHASTIC_TRAINING_POLICY,
)


SHA_MODEL = "sha256:" + "1" * 64
SHA_TRAIN = "sha256:" + "2" * 64
SHA_EVAL = "sha256:" + "3" * 64
SHA_ADAPTER = "sha256:" + "4" * 64


def evidence() -> dict:
    oracle = {
        "commit": UPSTREAM_COMMIT,
        "checkout": "/oracle",
        "imported_module": "/oracle/gliner2/model.py",
        "gliner2_version": CANONICAL_GLINER2_VERSION,
        "package_versions": CANONICAL_ORACLE_PACKAGE_VERSIONS,
    }
    return {
        "default_summary": {
            "pass": True,
            "accelerator_backend": "metal",
            "training_precision": "fp32",
            "optimizer_state_precision": "fp32",
            "precision_consistent": True,
            "oracle": oracle,
            "oracle_valid_in_every_run": True,
            "model_fingerprint_sha256": SHA_MODEL,
            "training_data_fingerprint_sha256": SHA_TRAIN,
        },
        "quality_report": {
            "contract": EVALUATION_CONTRACT,
            "pass": True,
            "oracle": oracle,
            "inference": {
                "normalization": CANONICAL_NORMALIZATION,
                "unicode_version": CANONICAL_UNICODE_VERSION,
            },
            "artifacts": {
                "base_model_fingerprint_sha256": SHA_MODEL,
                "adapter_bundle_fingerprint_sha256": SHA_ADAPTER,
                "eval_data_fingerprint_sha256": SHA_EVAL,
            },
        },
        "native_report": {
            "contract": NATIVE_EVALUATION_CONTRACT,
            "pass": True,
            "normalization": CANONICAL_NORMALIZATION,
            "artifacts": {
                "base_model_fingerprint_sha256": SHA_MODEL,
                "adapter_bundle_fingerprint_sha256": SHA_ADAPTER,
                "eval_data_fingerprint_sha256": SHA_EVAL,
            },
        },
        "convergence_report": {
            "contract": OUTPUT_CONTRACT,
            "evidence_contract": EVIDENCE_CONTRACT,
            "evidence_bound": True,
            "pass": True,
            "seed_count": 5,
            "oracle": {
                "commit": UPSTREAM_COMMIT,
                "checkout": "/oracle",
                "gliner2_version": CANONICAL_GLINER2_VERSION,
                "package_versions": CANONICAL_ORACLE_PACKAGE_VERSIONS,
            },
            "normalization": CANONICAL_NORMALIZATION,
            "unicode_version": CANONICAL_UNICODE_VERSION,
            "training_policy": STOCK_STOCHASTIC_TRAINING_POLICY,
            "accelerator_backend": "metal",
            "training_precision": "fp32",
            "thresholds": {"max_mean_deficit": 0.02, "max_paired_deficit": 0.05},
            "fingerprints": {
                "base_model": SHA_MODEL,
                "train_data": SHA_TRAIN,
                "eval_data": SHA_EVAL,
            },
            "runs": [{"seed": seed, "pass": True} for seed in range(5)],
            "metrics": {key: {"pass": True} for key in REQUIRED_MINIMA},
        },
    }


class ReadinessFinalizerTest(unittest.TestCase):
    def build(self, **overrides):
        values = evidence()
        values.update(overrides)
        convergence_evidence_errors = values.pop("convergence_evidence_errors", [])
        hardware_qualification = values.pop("hardware_qualification", None)
        hardware_qualification_errors = values.pop("hardware_qualification_errors", [])
        release_adapter_fingerprint = values.pop(
            "release_adapter_fingerprint", SHA_ADAPTER
        )
        release_manifest = values.pop(
            "release_manifest",
            {
                "backend": "Metal",
                "compiled_required": True,
                "training_precision": "fp32",
                "optimizer_state_precision": "fp32",
            },
        )
        backend = values.pop("backend", "metal")
        return build_summary(
            backend=backend,
            default_rc=0,
            head_rc=0,
            quality_rc=0,
            native_rc=0,
            skip_head_opt_in=True,
            require_head_opt_in=False,
            head_summary=None,
            convergence_evidence_errors=convergence_evidence_errors,
            hardware_qualification=hardware_qualification,
            hardware_qualification_errors=hardware_qualification_errors,
            release_adapter_fingerprint=release_adapter_fingerprint,
            release_manifest=release_manifest,
            paths={},
            **values,
        )

    def test_all_measured_gates_compute_production_ready_true(self) -> None:
        result = self.build()
        self.assertTrue(
            result["production_ready"], result["production_readiness_blockers"]
        )
        self.assertEqual([], result["production_readiness_blockers"])
        self.assertEqual("metal", result["accelerator_backend"])

    def test_backend_and_precision_are_bound_end_to_end(self) -> None:
        cuda_values = evidence()
        cuda_values["default_summary"].update(accelerator_backend="cuda")
        cuda_values["convergence_report"].update(accelerator_backend="cuda")
        result = self.build(
            backend="cuda",
            hardware_qualification={
                "contract": "gliner2_cuda_hardware_qualification_matrix/v1",
                "pass": True,
                "training_precision": "fp32",
                "optimizer_state_precision": "fp32",
                "cuda_artifacts": "fatbin",
            },
            release_manifest={
                "backend": "CUDA",
                "compiled_required": True,
                "training_precision": "fp32",
                "optimizer_state_precision": "fp32",
            },
            **cuda_values,
        )
        self.assertTrue(
            result["production_ready"], result["production_readiness_blockers"]
        )
        mislabeled = self.build(backend="cuda")
        self.assertFalse(mislabeled["production_ready"])
        self.assertFalse(mislabeled["checks"]["release_adapter_backend_and_precision"])

    def test_stochastic_and_fail_closed_unicode_are_policies_not_blockers(self) -> None:
        result = self.build()
        self.assertIn(
            "exact Python RNG-stream equality is not required",
            result["policies"]["stochastic_training"],
        )
        self.assertIn(
            "not a readiness blocker",
            result["policies"]["unsupported_training_unicode"],
        )

    def test_missing_convergence_or_normalization_mismatch_blocks(self) -> None:
        missing = self.build(convergence_report=None)
        self.assertFalse(missing["production_ready"])
        mismatch = evidence()["native_report"]
        mismatch["normalization"] = "legacy"
        result = self.build(native_report=mismatch)
        self.assertFalse(result["production_ready"])
        self.assertIn(
            "normalization", " ".join(result["production_readiness_blockers"])
        )

    def test_mismatched_fingerprints_block(self) -> None:
        convergence = evidence()["convergence_report"]
        convergence["fingerprints"]["base_model"] = "sha256:" + "9" * 64
        result = self.build(convergence_report=convergence)
        self.assertFalse(result["production_ready"])
        self.assertFalse(result["checks"]["artifact_fingerprints_consistent"])

    def test_release_adapter_or_stale_convergence_evidence_blocks(self) -> None:
        native = evidence()["native_report"]
        native["artifacts"]["adapter_bundle_fingerprint_sha256"] = "sha256:" + "9" * 64
        mismatch = self.build(native_report=native)
        self.assertFalse(mismatch["production_ready"])
        stale = self.build(convergence_evidence_errors=["comparison report changed"])
        self.assertFalse(stale["production_ready"])
        self.assertFalse(stale["checks"]["convergence_evidence_current"])

    def test_oracle_dependency_drift_blocks_release(self) -> None:
        default = copy.deepcopy(evidence()["default_summary"])
        default["oracle"]["package_versions"]["torch"] = "future"
        result = self.build(default_summary=default)
        self.assertFalse(result["production_ready"])
        self.assertFalse(result["checks"]["pinned_python_oracle"])


if __name__ == "__main__":
    unittest.main()
