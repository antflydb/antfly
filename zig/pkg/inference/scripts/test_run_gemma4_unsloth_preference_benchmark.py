#!/usr/bin/env python3

from __future__ import annotations

import importlib.util
import json
import os
import pathlib
import sys
import tempfile
import unittest
from unittest import mock


SCRIPTS = pathlib.Path(__file__).resolve().parent


def load_script(module_name: str, filename: str):
    spec = importlib.util.spec_from_file_location(module_name, SCRIPTS / filename)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


baseline = load_script(
    "gemma4_unsloth_preference_benchmark",
    "run_gemma4_unsloth_preference_benchmark.py",
)
smoke = load_script(
    "gemma4_cuda_preference_smoke_for_unsloth_test",
    "run_gemma4_cuda_preference_smoke.py",
)


class Gemma4UnslothPreferenceBenchmarkTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = pathlib.Path(self.temporary.name)

    def write_model(self, name: str = "model") -> pathlib.Path:
        model = self.root / name
        model.mkdir()
        (model / "config.json").write_text(
            json.dumps({"model_type": "gemma4"}) + "\n", encoding="utf-8"
        )
        (model / "model.safetensors").write_bytes(b"test weights")
        return model

    def test_protocol_exactly_matches_zig_consumer(self) -> None:
        self.assertEqual(
            smoke.benchmark_protocol(
                baseline.MAX_SEQ_LEN,
                baseline.LORA_RANK,
                baseline.LORA_ALPHA,
            ),
            baseline.PROTOCOL,
        )

    def test_sequence_hash_is_deterministic_bounded_and_order_sensitive(self) -> None:
        rewards = baseline.sequence_hash_reward([[1, 2, 3], [1, 3, 2], [1, 2, 3], []])
        self.assertEqual(rewards[0], rewards[2])
        self.assertNotEqual(rewards[0], rewards[1])
        self.assertTrue(all(0.0 <= reward <= 1.0 for reward in rewards))
        self.assertAlmostEqual(0.815099883979552, rewards[0])

    def test_qv_inventory_expectation_tracks_gemma4_kv_sharing(self) -> None:
        self.assertEqual(
            100,
            baseline.expected_qv_lora_trainable_tensors(
                {"text_config": {"num_hidden_layers": 35, "num_kv_shared_layers": 20}}
            ),
        )
        self.assertEqual(
            132,
            baseline.expected_qv_lora_trainable_tensors(
                {"text_config": {"num_hidden_layers": 42, "num_kv_shared_layers": 18}}
            ),
        )
        with self.assertRaisesRegex(baseline.BenchmarkError, "invalid text/KV-sharing"):
            baseline.expected_qv_lora_trainable_tensors(
                {"text_config": {"num_hidden_layers": 42, "num_kv_shared_layers": 42}}
            )

    def test_model_parser_fingerprints_local_gemma4_checkpoint(self) -> None:
        model = self.write_model()
        parsed = baseline.parse_models([f"e2b={model}"])
        self.assertEqual("e2b", parsed[0].label)
        self.assertEqual(model.resolve(), parsed[0].path)
        self.assertEqual(baseline.sha256_file(model / "config.json"), parsed[0].config_sha256)

    def test_model_parser_accepts_shards_and_rejects_path_escape(self) -> None:
        model = self.write_model()
        (model / "model.safetensors").unlink()
        (model / "model-00001-of-00002.safetensors").write_bytes(b"first")
        (model / "model-00002-of-00002.safetensors").write_bytes(b"second")
        index = model / "model.safetensors.index.json"
        index.write_text(
            json.dumps(
                {
                    "weight_map": {
                        "a": "model-00001-of-00002.safetensors",
                        "b": "model-00002-of-00002.safetensors",
                    }
                }
            ),
            encoding="utf-8",
        )
        self.assertEqual(2, len(baseline.checkpoint_shards(model)))
        self.assertEqual("e2b", baseline.parse_models([f"e2b={model}"])[0].label)

        outside = self.root / "outside.safetensors"
        outside.write_bytes(b"outside")
        index.write_text(json.dumps({"weight_map": {"a": "../outside.safetensors"}}), encoding="utf-8")
        with self.assertRaisesRegex(baseline.BenchmarkError, "escapes model directory"):
            baseline.parse_models([f"e2b={model}"])

    def test_model_parser_rejects_duplicate_labels_and_non_gemma4(self) -> None:
        model = self.write_model()
        with self.assertRaisesRegex(baseline.BenchmarkError, "duplicate model label"):
            baseline.parse_models([f"same={model}", f"same={model}"])

        other = self.write_model("other")
        (other / "config.json").write_text('{"model_type":"gemma3"}\n', encoding="utf-8")
        with self.assertRaisesRegex(baseline.BenchmarkError, "not a Gemma4"):
            baseline.parse_models([f"other={other}"])

    def test_worker_environment_forces_local_only_non_reporting_mode(self) -> None:
        inherited = {
            "HF_HUB_OFFLINE": "0",
            "TRANSFORMERS_OFFLINE": "0",
            "WANDB_DISABLED": "false",
            "TOKENIZERS_PARALLELISM": "true",
        }
        expected = {
            "HF_HUB_OFFLINE": "1",
            "TRANSFORMERS_OFFLINE": "1",
            "WANDB_DISABLED": "true",
            "TOKENIZERS_PARALLELISM": "false",
        }
        with mock.patch.dict(os.environ, inherited, clear=False):
            baseline.configure_worker_environment()
            self.assertEqual(expected, {name: os.environ[name] for name in expected})

    def test_aggregate_uses_medians_and_exact_step_rate(self) -> None:
        model_path = self.write_model()
        model = baseline.ModelSpec("e2b", model_path, "config-sha")
        runs = [
            {
                "wall_seconds": seconds,
                "peak_gpu_memory_mib": memory,
                "loss": loss,
                "mean_grad_norm": grad,
            }
            for seconds, memory, loss, grad in (
                (100.0, 10_000.0, 0.3, 0.6),
                (50.0, 9_000.0, 0.1, 0.2),
                (75.0, 9_500.0, 0.2, 0.4),
            )
        ]
        aggregate = baseline.aggregate_case(model, "dpo", runs)
        self.assertEqual(75.0, aggregate["median_wall_seconds"])
        self.assertEqual(9_500.0, aggregate["median_peak_gpu_memory_mib"])
        self.assertAlmostEqual(baseline.UPDATES / 75.0, aggregate["median_optimizer_steps_per_second"])
        self.assertEqual(0.2, aggregate["loss"])
        self.assertEqual(0.4, aggregate["mean_grad_norm"])

    def test_aggregate_requires_three_independent_repetitions(self) -> None:
        model = baseline.ModelSpec("e2b", self.write_model(), "config-sha")
        with self.assertRaisesRegex(baseline.BenchmarkError, "at least 3 repetitions"):
            baseline.aggregate_case(model, "grpo", [])

    def test_failed_worker_publishes_terminal_summary(self) -> None:
        model = self.write_model()
        output = self.root / "benchmark"
        with mock.patch.object(
            baseline,
            "run_process",
            side_effect=baseline.BenchmarkError("synthetic worker failure"),
        ):
            return_code = baseline.run_benchmark(
                [
                    "--model",
                    f"e2b={model}",
                    "--objective",
                    "dpo",
                    "--out",
                    str(output),
                ]
            )
        self.assertEqual(2, return_code)
        summary = json.loads((output / "summary.json").read_text(encoding="utf-8"))
        self.assertEqual("failed", summary["status"])
        self.assertEqual("synthetic worker failure", summary["metadata"]["failure"])
        self.assertIn("completed_at_utc", summary["metadata"])


if __name__ == "__main__":
    unittest.main()
