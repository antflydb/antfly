#!/usr/bin/env python3

from __future__ import annotations

import contextlib
import hashlib
import io
import json
from pathlib import Path
import sys
import tempfile
import unittest


sys.path.insert(0, str(Path(__file__).resolve().parent))

import validate_qwen3vl_promotion as promotion


def write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, sort_keys=True), encoding="utf-8")


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


class PromotionCampaignTests(unittest.TestCase):
    git_head = "a" * 40
    device_family = "Apple M4"

    def metrics(self, scenario: str) -> dict[str, object]:
        values: dict[str, object] = {}
        if scenario == "transformers_parity":
            values["fixture_count"] = 6
        elif scenario == "single_image_2mp":
            values.update(source_pixels=2_899_968, metal_repeat_count=2)
        elif scenario == "multi_image":
            values["image_count"] = 2
        elif scenario == "long_decode":
            values["generated_tokens"] = 256
        elif scenario == "long_context":
            values.update(active_tokens=8_192, fixture_count=8)
        elif scenario == "concurrency":
            values.update(max_concurrency=4, completed_requests=20)
        elif scenario == "cancellation":
            values.update(cancelled_requests=10, post_cancel_requests=10)
        elif scenario == "cache_lifecycle":
            values["cycles"] = 10
        elif scenario == "performance":
            values.update(warmups=1, paired_trials=5, performance_regression_percent=5.0)
        elif scenario == "soak":
            values.update(duration_seconds=3_600, completed_requests=100)
        elif scenario == "text_parity":
            values.update(
                fixture_count=32,
                max_score_abs=0.03,
                max_logit_abs=0.10,
                ranking_match_rate=1.0,
            )
        elif scenario == "multimodal_parity":
            values.update(
                fixture_count=12,
                max_source_pixels=2_899_968,
                metal_repeat_count=2,
                max_score_abs=0.03,
                max_logit_abs=0.10,
            )
        elif scenario == "quantized_conversion":
            values.update(
                quantization="decoder_q8_0_projector_q8_0_classifier_f16",
                independent_conversion_passes=2,
            )
        elif scenario == "retrieval_quality":
            values.update(
                query_count=100,
                candidate_count=1_000,
                mean_top_10_overlap=0.95,
                ndcg_at_10_abs_delta=0.01,
                mrr_abs_delta=0.01,
                brier_score_abs_delta=0.02,
            )
        elif scenario == "http_api":
            values.update(successful_requests=20, invalid_requests=20)
        return values

    def make_campaign(self, root: Path) -> tuple[Path, dict[str, object]]:
        reports = root / "reports"
        artifacts = root / "artifacts"
        reports.mkdir()
        artifacts.mkdir()
        binary_path = artifacts / "antfly-inference"
        binary_path.write_bytes(b"release-safe-metal-binary")
        binary_path.chmod(0o755)
        binary_sha = sha256(binary_path)
        model_targets: dict[str, dict[str, object]] = {}
        for model, contract in promotion.MODEL_CONTRACTS.items():
            receipt_path = artifacts / f"{model}-receipt.json"
            write_json(
                receipt_path,
                {
                    "version": 2,
                    "source": {"owner": "Qwen", "name": model, "variant": "test"},
                    "artifacts": [{"path": "model.gguf", "size": 1, "sha256": "c" * 64}],
                },
            )
            model_targets[model] = {
                "managed_receipt": str(receipt_path.relative_to(root)),
                "managed_receipt_sha256": sha256(receipt_path),
                "quantization": contract["quantization"],
            }
        entries: list[dict[str, str]] = []
        for model, contract in promotion.MODEL_CONTRACTS.items():
            for scenario, required_checks in contract["scenarios"].items():
                stem = f"{model}-{scenario}"
                artifact_path = artifacts / f"{stem}.jsonl"
                artifact_path.write_text('{"event":"measured"}\n', encoding="utf-8")
                report_path = reports / f"{stem}.json"
                write_json(
                    report_path,
                    {
                        "schema": promotion.EVIDENCE_SCHEMA,
                        "pass": True,
                        "release_ready": False,
                        "model": model,
                        "scenario": scenario,
                        "runtime_build": {
                            "git_head": self.git_head,
                            "git_dirty": False,
                            "binary_sha256": binary_sha,
                            "backend": "metal",
                            "device_family": self.device_family,
                        },
                        "model_artifact": model_targets[model],
                        "checks": {name: True for name in required_checks},
                        "metrics": self.metrics(scenario),
                        "gates": {"scenario_contract": {"pass": True}},
                        "resources": {
                            "sample_count": 2,
                            "swapout_growth_mib": 0.0,
                            "threshold_violations": [],
                        },
                        "artifacts": [
                            {
                                "path": str(artifact_path.relative_to(root)),
                                "sha256": sha256(artifact_path),
                            }
                        ],
                    },
                )
                entries.append(
                    {
                        "model": model,
                        "scenario": scenario,
                        "report": str(report_path.relative_to(root)),
                        "sha256": sha256(report_path),
                    }
                )
        manifest = {
            "schema": promotion.MANIFEST_SCHEMA,
            "target": {
                "git_head": self.git_head,
                "binary": str(binary_path.relative_to(root)),
                "binary_sha256": binary_sha,
                "backend": "metal",
                "device_family": self.device_family,
            },
            "models": model_targets,
            "evidence": entries,
        }
        path = root / "promotion-manifest.json"
        write_json(path, manifest)
        return path, manifest

    def rewrite_entry_digest(self, root: Path, manifest: dict[str, object], index: int) -> None:
        entry = manifest["evidence"][index]
        entry["sha256"] = sha256(root / entry["report"])
        write_json(root / "promotion-manifest.json", manifest)

    def test_complete_hash_pinned_matrix_promotes(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            path, _ = self.make_campaign(Path(raw))
            report = promotion.validate_manifest(path)
            self.assertTrue(report["pass"])
            self.assertTrue(report["release_ready"])
            self.assertEqual(len(promotion.required_matrix()), report["validated_lane_count"])

    def test_missing_lane_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            path, manifest = self.make_campaign(root)
            manifest["evidence"].pop()
            write_json(path, manifest)
            with self.assertRaisesRegex(promotion.PromotionError, "incomplete promotion matrix"):
                promotion.validate_manifest(path)

    def test_dirty_runtime_fails_even_with_updated_report_hash(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            path, manifest = self.make_campaign(root)
            entry = manifest["evidence"][0]
            report_path = root / entry["report"]
            report = json.loads(report_path.read_text(encoding="utf-8"))
            report["runtime_build"]["git_dirty"] = True
            write_json(report_path, report)
            self.rewrite_entry_digest(root, manifest, 0)
            with self.assertRaisesRegex(promotion.PromotionError, "dirty or unknown"):
                promotion.validate_manifest(path)

    def test_tampered_raw_artifact_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            path, manifest = self.make_campaign(root)
            entry = manifest["evidence"][0]
            report = json.loads((root / entry["report"]).read_text(encoding="utf-8"))
            (root / report["artifacts"][0]["path"]).write_text("tampered", encoding="utf-8")
            with self.assertRaisesRegex(promotion.PromotionError, "artifact SHA-256 mismatch"):
                promotion.validate_manifest(path)

    def test_tampered_runtime_binary_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            path, manifest = self.make_campaign(root)
            (root / manifest["target"]["binary"]).write_bytes(b"tampered")
            with self.assertRaisesRegex(promotion.PromotionError, "runtime binary SHA-256 mismatch"):
                promotion.validate_manifest(path)

    def test_tampered_managed_receipt_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            path, manifest = self.make_campaign(root)
            model = "qwen3-vl-reranker-2b"
            (root / manifest["models"][model]["managed_receipt"]).write_text(
                "{}", encoding="utf-8"
            )
            with self.assertRaisesRegex(promotion.PromotionError, "managed receipt SHA-256 mismatch"):
                promotion.validate_manifest(path)

    def test_performance_regression_above_five_percent_fails(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            path, manifest = self.make_campaign(root)
            index = next(
                index
                for index, entry in enumerate(manifest["evidence"])
                if entry["scenario"] == "performance"
            )
            entry = manifest["evidence"][index]
            report_path = root / entry["report"]
            report = json.loads(report_path.read_text(encoding="utf-8"))
            report["metrics"]["performance_regression_percent"] = 5.01
            write_json(report_path, report)
            self.rewrite_entry_digest(root, manifest, index)
            with self.assertRaisesRegex(promotion.PromotionError, "must be <= 5.0"):
                promotion.validate_manifest(path)

    def test_reranker_calibrated_score_drift_above_limit_fails(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            path, manifest = self.make_campaign(root)
            index = next(
                index
                for index, entry in enumerate(manifest["evidence"])
                if entry["model"] == "qwen3-vl-reranker-2b"
                and entry["scenario"] == "multimodal_parity"
            )
            entry = manifest["evidence"][index]
            report_path = root / entry["report"]
            report = json.loads(report_path.read_text(encoding="utf-8"))
            report["metrics"]["max_score_abs"] = 0.030001
            write_json(report_path, report)
            self.rewrite_entry_digest(root, manifest, index)
            with self.assertRaisesRegex(promotion.PromotionError, "max_score_abs"):
                promotion.validate_manifest(path)

    def test_non_finite_metric_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            path, manifest = self.make_campaign(root)
            index = next(
                index
                for index, entry in enumerate(manifest["evidence"])
                if entry["scenario"] == "soak"
            )
            entry = manifest["evidence"][index]
            report_path = root / entry["report"]
            report = json.loads(report_path.read_text(encoding="utf-8"))
            report["metrics"]["duration_seconds"] = float("nan")
            write_json(report_path, report)
            self.rewrite_entry_digest(root, manifest, index)
            with self.assertRaisesRegex(promotion.PromotionError, "duration_seconds"):
                promotion.validate_manifest(path)

    def test_duplicate_json_keys_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            path, _ = self.make_campaign(root)
            encoded = path.read_text(encoding="utf-8")
            path.write_text(
                encoded.replace(
                    '"schema": "antfly.qwen3vl.promotion_manifest.v1",',
                    '"schema": "antfly.qwen3vl.promotion_manifest.v1", '
                    '"schema": "antfly.qwen3vl.promotion_manifest.v0",',
                    1,
                ),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(promotion.PromotionError, "duplicate JSON key 'schema'"):
                promotion.validate_manifest(path)

    def test_cli_failure_report_never_sets_release_ready(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            path, manifest = self.make_campaign(root)
            manifest["evidence"].pop()
            write_json(path, manifest)
            output = root / "promotion.json"
            with contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(2, promotion.main(["--manifest", str(path), "--output", str(output)]))
            report = json.loads(output.read_text(encoding="utf-8"))
            self.assertFalse(report["pass"])
            self.assertFalse(report["release_ready"])


if __name__ == "__main__":
    unittest.main()
