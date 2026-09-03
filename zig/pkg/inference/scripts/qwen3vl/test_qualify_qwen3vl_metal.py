#!/usr/bin/env python3

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
import struct
import sys
import tempfile
import unittest
from unittest import mock


sys.path.insert(0, str(Path(__file__).resolve().parent))

import qualify_qwen3vl_metal as qualify


def write_json(path: Path, value: object) -> None:
    path.write_text(json.dumps(value), encoding="utf-8")


class ManagedBundleTests(unittest.TestCase):
    def make_bundle(self, root: Path) -> None:
        files = {
            "decoder.gguf": b"decoder",
            "projector.gguf": b"projector",
            "antfly_inference_bundle.json": json.dumps(
                {
                    "family": "qwen3_vl_gguf_bundle/v1",
                    "decoder": "decoder.gguf",
                    "projector": "projector.gguf",
                }
            ).encode(),
        }
        artifacts = []
        for name, data in files.items():
            (root / name).write_bytes(data)
            artifacts.append(
                {
                    "path": name,
                    "size": len(data),
                    "sha256": hashlib.sha256(data).hexdigest(),
                }
            )
        write_json(
            root / qualify.RECEIPT_NAME,
            {"version": 2, "source": qualify.EXPECTED_SOURCE, "artifacts": artifacts},
        )

    def test_validates_every_artifact_and_resolves_bundle(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            self.make_bundle(root)
            evidence = qualify.validate_managed_bundle(root)
            self.assertEqual(str(root.resolve() / "decoder.gguf"), evidence["decoder_path"])
            self.assertEqual(3, len(evidence["artifacts"]))

    def test_tamper_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            self.make_bundle(root)
            (root / "decoder.gguf").write_bytes(b"tampered")
            with self.assertRaisesRegex(qualify.QualificationError, "size mismatch"):
                qualify.validate_managed_bundle(root)

    def test_unreceipted_file_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            self.make_bundle(root)
            (root / "stale.json").write_text("{}", encoding="utf-8")
            with self.assertRaisesRegex(qualify.QualificationError, "file set mismatch"):
                qualify.validate_managed_bundle(root)

    def test_reranker_requires_exact_nested_contract_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            files = {
                "model.safetensors": b"weights",
                "additional_chat_templates/reranker.jinja": b"template",
                "1_LogitScore/config.json": b"score",
                "antfly_inference_bundle.json": json.dumps(
                    {
                        "family": "qwen3_vl_reranker_safetensors_bundle/v1",
                        "model": "model.safetensors",
                    }
                ).encode(),
                "model_manifest.json": b'{"type":"reranker"}',
            }
            expected = {
                path: (len(data), hashlib.sha256(data).hexdigest())
                for path, data in files.items()
                if path not in ("antfly_inference_bundle.json", "model_manifest.json")
            }
            artifacts = []
            for name, data in files.items():
                path = root / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(data)
                artifact = {"path": name, "size": len(data)}
                if name not in ("antfly_inference_bundle.json", "model_manifest.json"):
                    artifact["sha256"] = hashlib.sha256(data).hexdigest()
                artifacts.append(artifact)
            write_json(
                root / qualify.RECEIPT_NAME,
                {
                    "version": 2,
                    "source": qualify.RERANKER_EXPECTED_SOURCE,
                    "artifacts": artifacts,
                },
            )
            original = qualify.RERANKER_EXPECTED_ARTIFACTS
            original_generated = qualify.RERANKER_GENERATED_ARTIFACTS
            qualify.RERANKER_EXPECTED_ARTIFACTS = expected
            qualify.RERANKER_GENERATED_ARTIFACTS = {
                path: (len(data), hashlib.sha256(data).hexdigest())
                for path, data in files.items()
                if path in ("antfly_inference_bundle.json", "model_manifest.json")
            }
            try:
                evidence = qualify.validate_managed_reranker_bundle(root)
                self.assertEqual(str(root.resolve() / "model.safetensors"), evidence["model_path"])
                (root / "1_LogitScore/config.json").write_bytes(b"wrong")
                with self.assertRaises(qualify.QualificationError):
                    qualify.validate_managed_reranker_bundle(root)
            finally:
                qualify.RERANKER_EXPECTED_ARTIFACTS = original
                qualify.RERANKER_GENERATED_ARTIFACTS = original_generated


class ParserAndMetricTests(unittest.TestCase):
    def test_metal_command_preserves_multi_image_order(self) -> None:
        args = SimpleNamespace(
            antfly_bin=Path("/bin/antfly-inference"),
            model_dir=Path("/models/qwen"),
            prompt="Read the text.",
            image=Path("/images/fallback.jpg"),
            images=[Path("/images/first.jpg"), Path("/images/second.jpg")],
            max_tokens=1,
            host_budget_mb=2048,
            backend_budget_mb=3072,
            combined_budget_mb=4096,
            kv_budget_mb=256,
            scratch_budget_mb=768,
        )
        command = qualify.metal_command(args, Path("/artifacts"))
        self.assertEqual(
            ["--image", "/images/first.jpg", "--image", "/images/second.jpg"],
            command[4:8],
        )
        self.assertEqual("--backend", command[8])

    def test_parses_prompt_and_generation_ids(self) -> None:
        rendered, prompt_ids, generated_ids = qualify.parse_prompt_output(
            "prompt:\nhello\nprompt_token_ids: 1 2 3\nworld\ntoken_ids: 4\n"
        )
        self.assertEqual("hello", rendered)
        self.assertEqual([1, 2, 3], prompt_ids)
        self.assertEqual([4], generated_ids)

    def test_patch_metrics_are_deterministic(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            reference = root / "reference.f32"
            actual = root / "actual.f32"
            reference.write_bytes(struct.pack("<4f", 0.0, 1.0, -1.0, 2.0))
            actual.write_bytes(struct.pack("<4f", 0.0, 1.1, -0.9, 2.0))
            metrics = qualify.patch_metrics(reference, actual)
            self.assertTrue(metrics["size_match"])
            self.assertEqual(4, metrics["value_count"])
            self.assertAlmostEqual(0.05, metrics["mean_abs"], places=6)
            self.assertAlmostEqual(0.1, metrics["max_abs"], places=6)

    def test_logit_metrics_require_shape_and_report_argmax(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            reference = root / "reference.f32"
            actual = root / "actual.f32"
            reference.write_bytes(struct.pack("<4f", 0.0, 1.0, 4.0, 2.0))
            actual.write_bytes(struct.pack("<4f", 0.1, 0.9, 3.5, 2.1))
            metrics = qualify.logit_metrics(reference, actual, top_k=3)
            self.assertTrue(metrics["size_match"])
            self.assertTrue(metrics["finite"])
            self.assertEqual(2, metrics["reference_argmax"])
            self.assertEqual(2, metrics["actual_argmax"])
            self.assertEqual(3, metrics["top_k_overlap"])

            actual.write_bytes(struct.pack("<2f", 1.0, 2.0))
            mismatch = qualify.logit_metrics(reference, actual)
            self.assertFalse(mismatch["size_match"])

    def test_logit_quality_limits_are_versioned_and_fail_closed(self) -> None:
        passing = {
            "size_match": True,
            "finite": True,
            "cosine_similarity": 0.97,
            "pearson_correlation": 0.96,
            "mean_abs": 0.8,
            "rmse": 1.1,
            "max_abs": 5.0,
            "top_k_overlap": 8,
        }
        self.assertTrue(qualify.logit_quality_pass(passing))
        self.assertFalse(qualify.logit_quality_pass({**passing, "rmse": 1.3}))
        self.assertFalse(qualify.logit_quality_pass({**passing, "finite": False}))

    def test_vm_stat_uses_reported_page_size(self) -> None:
        output = (
            "Mach Virtual Memory Statistics: (page size of 16384 bytes)\n"
            "Pages free: 20.\nSwapouts: 7.\n"
        )
        self.assertEqual(7 * 16384, qualify.parse_vm_stat_swapout_bytes(output))

    def test_resource_violation_retains_measured_execution(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            with (
                mock.patch.object(qualify, "swapout_bytes", return_value=0),
                mock.patch.object(qualify, "memory_free_percent", return_value=90),
                mock.patch.object(qualify, "process_rss_mib", return_value=1.0),
            ):
                with self.assertRaises(qualify.ResourceViolation) as raised:
                    qualify.run_resource_monitored(
                        ["/bin/sleep", "1"],
                        root / "stdout.log",
                        root / "stderr.log",
                        timeout_seconds=0.01,
                        max_rss_mib=10.0,
                        min_free_percent=10,
                        max_swap_growth_mib=0.0,
                        sample_interval_seconds=0.01,
                        label="test process",
                    )
            self.assertIn("timeout", str(raised.exception))
            self.assertGreaterEqual(raised.exception.execution["resources"]["sample_count"], 1)

    def test_final_swap_snapshot_is_a_hard_gate(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            with (
                mock.patch.object(qualify, "swapout_bytes", side_effect=[0, 0, 1048576]),
                mock.patch.object(qualify, "memory_free_percent", return_value=90),
                mock.patch.object(qualify, "process_rss_mib", return_value=1.0),
            ):
                with self.assertRaises(qualify.ResourceViolation) as raised:
                    qualify.run_resource_monitored(
                        ["/usr/bin/true"],
                        root / "stdout.log",
                        root / "stderr.log",
                        timeout_seconds=1.0,
                        max_rss_mib=10.0,
                        min_free_percent=10,
                        max_swap_growth_mib=0.0,
                        sample_interval_seconds=0.01,
                        label="test process",
                    )
            self.assertIn("swapout growth 1.0 MiB", str(raised.exception))
            self.assertEqual(
                1.0,
                raised.exception.execution["resources"]["swapout_growth_mib"],
            )


class GateTests(unittest.TestCase):
    def fixtures(self) -> tuple[SimpleNamespace, dict, dict, dict, dict, dict]:
        args = SimpleNamespace(
            max_rss_mib=4096.0,
            max_swap_growth_mib=0.0,
            min_free_percent=10,
            max_tokens=1,
            expected_token_id=None,
        )
        oracle = {
            "rendered_prompt": "hello",
            "placeholder_token_ids": [1, 2],
            "input_ids": [1, 9, 9, 2],
            "mrope": {"position_ids": list(range(12)), "position_deltas": [-2]},
            "image": {"grid_thw": [[1, 4, 4]], "resized_sizes": [[64, 64]]},
            "architecture": {
                "visual_token_count": 2,
                "text_hidden_size": 2,
                "deepstack_visual_indexes": [5, 11, 17],
            },
        }
        parity = {
            "placeholder_token_ids": [1, 2],
            "expanded_token_ids": [1, 9, 9, 2],
            "mrope_position_ids": list(range(12)),
            "mrope_position_delta": -2,
            "images": [
                {"grid_thw": [1, 4, 4], "resized_width": 64, "resized_height": 64}
            ],
            "visual_token_count": 2,
            "deepstack_layer_count": 3,
            "projected_embedding_value_count": 4,
            "deepstack_embedding_value_count": 12,
        }
        timing = {
            "backend": "metal",
            "tokens": 1,
            "token_ids": [7],
            "metal": {
                "device": "Apple Test",
                "runtime_command_operators": {"fallback": 0},
                "quant_kernel_plan": {
                    "unsupported_routes": 0,
                    "fast_path_misses": 0,
                    "generated_artifact_missing": 0,
                    "generated_runtime_not_wired": 0,
                    "unsupported": 0,
                    "top_fallback_count": 0,
                },
                "frame_fallbacks": {"decode_fallback": 0, "prefill_plan_fail": 0},
            },
        }
        run = {
            "stdout": "prompt:\nhello\nprompt_token_ids: 1 2\nanswer\ntoken_ids: 7\n",
            "stderr": "",
            "resources": {
                "max_rss_mib": 1024.0,
                "swapout_growth_mib": 0.0,
                "min_free_percent": 50,
            },
        }
        metrics = {
            "size_match": True,
            "value_count": 10,
            "mean_abs": 0.0001,
            "rmse": 0.001,
            "p99_abs": 0.002,
            "max_abs": 0.02,
        }
        return args, oracle, parity, timing, run, metrics

    def test_all_preprocess_and_metal_gates_pass_exact_fixture(self) -> None:
        gates = qualify.parity_gates(*self.fixtures())
        self.assertTrue(all(gate["pass"] for gate in gates.values()))

    def test_mrope_mismatch_is_a_hard_gate(self) -> None:
        args, oracle, parity, timing, run, metrics = self.fixtures()
        parity["mrope_position_ids"][5] = 999
        gates = qualify.parity_gates(args, oracle, parity, timing, run, metrics)
        self.assertFalse(gates["mrope_position_ids_exact"]["pass"])

    def test_nonzero_runtime_fallback_is_a_hard_gate(self) -> None:
        args, oracle, parity, timing, run, metrics = self.fixtures()
        timing["metal"]["runtime_command_operators"]["fallback"] = 1
        gates = qualify.parity_gates(args, oracle, parity, timing, run, metrics)
        self.assertFalse(gates["metal_fallback_counters_zero"]["pass"])


class DeterminismTests(unittest.TestCase):
    def test_boundary_hashes_and_logits_must_be_bitwise_exact(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            logits_a = root / "a.f32"
            logits_b = root / "b.f32"
            logits_a.write_bytes(struct.pack("<2f", 1.0, 2.0))
            logits_b.write_bytes(struct.pack("<2f", 1.0, 2.0))
            digest = "a" * 64
            parity = {
                "placeholder_token_ids": [1],
                "expanded_token_ids": [1, 2],
                "mrope_position_ids": [0, 0, 0, 1, 1, 1],
                "images": [
                    {
                        "grid_thw": [1, 2, 2],
                        "positioned_embedding_f32le_sha256": digest,
                    }
                ],
                "visual_token_count": 1,
                "deepstack_layer_count": 3,
                "mrope_position_delta": -1,
                "projected_embedding_value_count": 2,
                "projected_embedding_f32le_sha256": digest,
                "deepstack_embedding_value_count": 6,
                "deepstack_embedding_f32le_sha256": digest,
                "deepstack_taps": [
                    {"value_count": 2, "f32le_sha256": digest},
                    {"value_count": 2, "f32le_sha256": digest},
                    {"value_count": 2, "f32le_sha256": digest},
                ],
            }
            timing = {"token_ids": [7]}
            run_a = {
                "stdout": "prompt:\nhello\nprompt_token_ids: 1\nanswer\ntoken_ids: 7\n",
                "stderr": "",
                "logits": str(logits_a),
            }
            run_b = {**run_a, "logits": str(logits_b)}
            metrics = qualify.metal_determinism_metrics(
                [(parity, timing, run_a), (parity, timing, run_b)]
            )
            self.assertTrue(metrics["structure_exact"])
            self.assertTrue(metrics["positioned_embeddings_exact"])
            self.assertTrue(metrics["projector_exact"])
            self.assertTrue(metrics["deepstack_exact"])
            self.assertTrue(metrics["prefill_logits_exact"])
            self.assertTrue(metrics["generated_tokens_exact"])

            logits_b.write_bytes(struct.pack("<2f", 2.0, 1.0))
            mismatch = qualify.metal_determinism_metrics(
                [(parity, timing, run_a), (parity, timing, run_b)]
            )
            self.assertFalse(mismatch["prefill_logits_exact"])


if __name__ == "__main__":
    unittest.main()
