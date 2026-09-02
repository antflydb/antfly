#!/usr/bin/env python3

from __future__ import annotations

import importlib.util
import json
import pathlib
import sys
import tempfile
import unittest
from unittest import mock


SCRIPT = pathlib.Path(__file__).resolve().with_name("run_gemma4_cuda_preference_smoke.py")
SPEC = importlib.util.spec_from_file_location("gemma4_cuda_preference_smoke", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
smoke = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = smoke
SPEC.loader.exec_module(smoke)


def write_safetensors(
    path: pathlib.Path,
    dtype: str = "BF16",
    *,
    hidden_size: int = 1536,
    intermediate_size: int = 6144,
    num_hidden_layers: int = 35,
    num_kv_shared_layers: int = 0,
    use_double_wide_mlp: bool = False,
    stored_double_wide_mlp: bool | None = None,
) -> None:
    data_size = 32 if dtype == "BF16" else 64
    metadata = {
        "model.embed_tokens.weight": {
            "dtype": dtype,
            "shape": [4, 4],
            "data_offsets": [0, data_size],
        }
    }
    stored_double = (
        use_double_wide_mlp if stored_double_wide_mlp is None else stored_double_wide_mlp
    )
    first_shared_layer = num_hidden_layers - num_kv_shared_layers
    for layer in range(num_hidden_layers):
        layer_intermediate_size = intermediate_size
        if stored_double and num_kv_shared_layers > 0 and layer >= first_shared_layer:
            layer_intermediate_size *= 2
        prefix = f"model.language_model.layers.{layer}.mlp"
        for projection, shape in {
            "gate_proj": [layer_intermediate_size, hidden_size],
            "up_proj": [layer_intermediate_size, hidden_size],
            "down_proj": [hidden_size, layer_intermediate_size],
        }.items():
            metadata[f"{prefix}.{projection}.weight"] = {
                "dtype": dtype,
                "shape": shape,
                "data_offsets": [0, data_size],
            }
    header = json.dumps(metadata, separators=(",", ":")).encode()
    path.write_bytes(len(header).to_bytes(8, "little") + header + bytes(data_size))


def write_model(
    root: pathlib.Path,
    dtype: str = "BF16",
    *,
    hidden_size: int = 1536,
    intermediate_size: int = 6144,
    num_hidden_layers: int = 35,
    num_kv_shared_layers: int = 0,
    use_double_wide_mlp: bool = False,
    stored_double_wide_mlp: bool | None = None,
) -> pathlib.Path:
    model = root / "model"
    model.mkdir(parents=True)
    (model / "config.json").write_text(
        json.dumps(
            {
                "model_type": "gemma4",
                "text_config": {
                    "hidden_size": hidden_size,
                    "num_hidden_layers": num_hidden_layers,
                    "num_attention_heads": 8,
                    "num_key_value_heads": 1,
                    "head_dim": 256,
                    "intermediate_size": intermediate_size,
                    "num_kv_shared_layers": num_kv_shared_layers,
                    "use_double_wide_mlp": use_double_wide_mlp,
                    "vocab_size": 262144,
                },
            }
        ),
        encoding="utf-8",
    )
    (model / "tokenizer_config.json").write_text("{}\n", encoding="utf-8")
    write_safetensors(
        model / "model.safetensors",
        dtype,
        hidden_size=hidden_size,
        intermediate_size=intermediate_size,
        num_hidden_layers=num_hidden_layers,
        num_kv_shared_layers=num_kv_shared_layers,
        use_double_wide_mlp=use_double_wide_mlp,
        stored_double_wide_mlp=stored_double_wide_mlp,
    )
    return model


def valid_evidence(train_steps: int = 2) -> dict[str, object]:
    return {
        "schema_version": smoke.EXECUTION_EVIDENCE_SCHEMA,
        "train_steps": train_steps,
        "eval_steps": 0,
        "graph_executor_partitions": train_steps,
        "graph_executor_planned_dispatches": train_steps * 10,
        "graph_executor_fallback_steps": 0,
        "graph_executor_native_partitions": 0,
        "graph_executor_unsupported_ops": 0,
        "graph_executor_interpreter_fallbacks": 0,
        "graph_executor_true_host_outputs": 0,
        "runtime_input_uploads": train_steps * 3,
        "runtime_input_upload_bytes": train_steps * 24,
        "runtime_input_h2d_bytes": train_steps * 24,
        "runtime_input_d2h_bytes": 0,
        "declared_runtime_input_uploads": train_steps * 3,
        "declared_runtime_input_upload_bytes": train_steps * 24,
        "declared_runtime_input_h2d_bytes": train_steps * 24,
        "compiled_session_setup_d2h_bytes": 0,
        "graph_execution_h2d_bytes": 0,
        "graph_execution_d2h_bytes": train_steps * 4,
        "training_runtime_d2h_bytes": train_steps * 4,
        "host_gradient_tensors": 0,
        "cuda_kernel_launches": train_steps * 10,
        "cuda_d2h_bytes": train_steps * 4,
        "cuda_largest_d2h_transfer_bytes": 4,
        "peak_resident_bytes": 512 * 1024 * 1024,
    }


def valid_report(updates: int = 1) -> dict[str, object]:
    return {
        "policy_backend": "cuda",
        "optimizer_backend": "cuda",
        "optimizer_steps": updates,
        "cuda_optimizer_steps": updates,
        "micro_batch_steps": updates * 2,
        "loss": 0.5,
        "beta": smoke.DPO_BETA,
        "mean_grad_norm": 0.25,
        "mean_reward": 0.5,
        "reward_std": 0.25,
        "informative_groups": 1,
        "group_size": smoke.GRPO_GROUP_SIZE,
        "clip_epsilon": smoke.GRPO_CLIP_EPSILON,
        "kl_coef": smoke.GRPO_KL_COEF,
        "advantage_epsilon": smoke.GRPO_ADVANTAGE_EPSILON,
        "advantage_standard_deviation_correction": 1,
        "reward_scaling": "group-sample-std",
        "loss_normalization": "per-completion-token-mean",
        "trainable_update": {
            "tensor_count": 4,
            "changed_tensor_count": 2,
            "max_abs_delta": 0.001,
        },
        "device_execution_scope": smoke.DEVICE_EXECUTION_SCOPE,
        "device_execution": valid_evidence(updates * 2),
    }


class Gemma4CudaPreferenceSmokeTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = pathlib.Path(self.temporary.name)

    def test_preflight_accepts_bf16_checkpoint(self) -> None:
        result = smoke.inspect_model(smoke.ModelSpec("e2b", write_model(self.root)))
        self.assertEqual("safetensors", result.artifact_kind)
        self.assertEqual(["BF16"], result.rank2_dtypes)
        self.assertEqual(1536, result.topology["hidden_size"])
        self.assertEqual(106, result.tensor_count)

    def test_preflight_accepts_e2b_double_and_e4b_uniform_mlp_topologies(self) -> None:
        e2b = write_model(
            self.root / "e2b",
            num_kv_shared_layers=20,
            use_double_wide_mlp=True,
        )
        e2b_result = smoke.inspect_model(smoke.ModelSpec("e2b", e2b))
        self.assertEqual(20, e2b_result.topology["num_kv_shared_layers"])
        self.assertIs(True, e2b_result.topology["use_double_wide_mlp"])

        e4b = write_model(
            self.root / "e4b",
            hidden_size=2560,
            intermediate_size=10240,
            num_hidden_layers=42,
            num_kv_shared_layers=18,
            use_double_wide_mlp=False,
        )
        e4b_result = smoke.inspect_model(smoke.ModelSpec("e4b", e4b))
        self.assertEqual(18, e4b_result.topology["num_kv_shared_layers"])
        self.assertIs(False, e4b_result.topology["use_double_wide_mlp"])

    def test_preflight_rejects_config_checkpoint_mlp_shape_mismatch(self) -> None:
        model = write_model(
            self.root,
            num_kv_shared_layers=20,
            use_double_wide_mlp=False,
            stored_double_wide_mlp=True,
        )
        with self.assertRaisesRegex(smoke.QualificationError, "text MLP shape mismatch"):
            smoke.inspect_model(smoke.ModelSpec("e2b", model))

    def test_preflight_rejects_packed_gguf_and_rank2_f16(self) -> None:
        packed = self.root / "packed"
        packed.mkdir()
        (packed / "model.gguf").write_bytes(b"GGUF")
        with self.assertRaisesRegex(smoke.QualificationError, "packed GGUF"):
            smoke.inspect_model(smoke.ModelSpec("a4b", packed))

        other_root = self.root / "other"
        other_root.mkdir()
        model = write_model(other_root, "F16")
        with self.assertRaisesRegex(smoke.QualificationError, "unsupported rank-2"):
            smoke.inspect_model(smoke.ModelSpec("e2b", model))

    def test_preflight_rejects_shard_path_escape(self) -> None:
        model = write_model(self.root)
        (model / "model.safetensors").unlink()
        (model / "model.safetensors.index.json").write_text(
            json.dumps({"weight_map": {"weight": "../outside.safetensors"}}), encoding="utf-8"
        )
        (self.root / "outside.safetensors").write_bytes(b"outside")
        with self.assertRaisesRegex(smoke.QualificationError, "escapes model directory"):
            smoke.inspect_model(smoke.ModelSpec("e2b", model))

    def test_recipe_is_bounded_and_strictly_selects_cuda(self) -> None:
        model = smoke.ModelSpec("e2b", self.root / "model")
        recipe = smoke.recipe_for(
            "grpo", model, self.root / "data.jsonl", self.root / "run", 128, 8, 32, 25
        )
        self.assertEqual("cuda", recipe["backend"])
        self.assertEqual(["q_proj", "v_proj"], recipe["adapter"]["target_modules"])
        self.assertEqual(1, recipe["dataset"]["max_examples"])
        self.assertEqual(25, recipe["optimizer"]["epochs"])
        self.assertEqual(4, recipe["grpo"]["group_size"])
        self.assertEqual(4, recipe["grpo"]["max_completion_tokens"])
        self.assertEqual(1e-4, recipe["grpo"]["advantage_eps"])
        self.assertEqual("sequence-hash", recipe["grpo"]["reward_mode"])

    def test_benchmark_locks_grpo_target(self) -> None:
        models = [smoke.ModelSpec("e2b", self.root / "model")]
        smoke.validate_grpo_targets(
            models, {"e2b": smoke.BENCHMARK_GRPO_TARGET}, ["dpo", "grpo"], True
        )
        with self.assertRaisesRegex(smoke.QualificationError, "locks --grpo-target"):
            smoke.validate_grpo_targets(models, {"e2b": "Berlin"}, ["grpo"], True)

    def test_report_accepts_actual_cuda_optimizer_and_finite_health(self) -> None:
        path = self.root / "dpo_report.json"
        path.write_text(json.dumps(valid_report()), encoding="utf-8")
        parsed = smoke.validate_report(path, "dpo", 1)
        self.assertEqual(1, parsed["cuda_optimizer_steps"])
        self.assertGreater(parsed["mean_grad_norm"], 0)

    def test_report_accepts_f32_objective_parameters_and_rejects_real_drift(self) -> None:
        report = valid_report()
        report["beta"] = 0.10000000149011612
        path = self.root / "dpo_f32_report.json"
        path.write_text(json.dumps(report), encoding="utf-8")
        smoke.validate_report(path, "dpo", 1)

        report["beta"] = 0.11
        path.write_text(json.dumps(report), encoding="utf-8")
        with self.assertRaisesRegex(smoke.QualificationError, "beta mismatch"):
            smoke.validate_report(path, "dpo", 1)

        report = valid_report()
        report["clip_epsilon"] = 0.20000000298023224
        report["kl_coef"] = 0.03999999910593033
        report["advantage_epsilon"] = 0.00009999999747378752
        path = self.root / "grpo_f32_report.json"
        path.write_text(json.dumps(report), encoding="utf-8")
        smoke.validate_report(path, "grpo", 1)

        report["clip_epsilon"] = 0.21
        path.write_text(json.dumps(report), encoding="utf-8")
        with self.assertRaisesRegex(smoke.QualificationError, "clip_epsilon mismatch"):
            smoke.validate_report(path, "grpo", 1)

    def test_report_rejects_optimizer_fallback_and_step_mismatch(self) -> None:
        report = valid_report()
        report["optimizer_backend"] = "host"
        path = self.root / "grpo_report.json"
        path.write_text(json.dumps(report), encoding="utf-8")
        with self.assertRaisesRegex(smoke.QualificationError, "both report CUDA"):
            smoke.validate_report(path, "grpo", 1)

        report = valid_report()
        report["cuda_optimizer_steps"] = 0
        path.write_text(json.dumps(report), encoding="utf-8")
        with self.assertRaisesRegex(smoke.QualificationError, "CUDA optimizer steps"):
            smoke.validate_report(path, "grpo", 1)

    def test_report_rejects_graph_fallback_and_host_gradients(self) -> None:
        for field in ("graph_executor_fallback_steps", "host_gradient_tensors"):
            report = valid_report()
            report["device_execution"][field] = 1
            path = self.root / f"{field}.json"
            path.write_text(json.dumps(report), encoding="utf-8")
            with self.assertRaisesRegex(smoke.QualificationError, field):
                smoke.validate_report(path, "dpo", 1)

    def test_report_rejects_excess_scalar_readback(self) -> None:
        for field in ("training_runtime_d2h_bytes", "cuda_d2h_bytes"):
            report = valid_report()
            report["device_execution"][field] = 13
            path = self.root / f"{field}.json"
            path.write_text(json.dumps(report), encoding="utf-8")
            with self.assertRaisesRegex(smoke.QualificationError, "scalar readback budget"):
                smoke.validate_report(path, "dpo", 1)

    def test_report_rejects_nonfinite_loss_gradient_and_delta(self) -> None:
        cases = (
            ("loss", float("nan"), "dpo.loss"),
            ("mean_grad_norm", float("inf"), "mean_grad_norm"),
        )
        for field, value, message in cases:
            report = valid_report()
            report[field] = value
            path = self.root / f"{field}.json"
            path.write_text(json.dumps(report), encoding="utf-8")
            with self.assertRaisesRegex(smoke.QualificationError, message):
                smoke.validate_report(path, "dpo", 1)

        report = valid_report()
        report["trainable_update"]["max_abs_delta"] = 0.0
        path = self.root / "delta.json"
        path.write_text(json.dumps(report), encoding="utf-8")
        with self.assertRaisesRegex(smoke.QualificationError, "max_abs_delta"):
            smoke.validate_report(path, "dpo", 1)

    def test_run_case_gates_adapter_delta_gpu_memory_and_throughput(self) -> None:
        executable = self.root / "fake-antfly-inference"
        executable.write_text(
            """#!/usr/bin/env python3
import json
import pathlib
import sys

assert sys.argv[1:3] == ["finetune", "run"]
recipe = json.loads(pathlib.Path(sys.argv[3]).read_text(encoding="utf-8"))
artifacts = recipe["artifacts"]
updates = recipe["optimizer"]["epochs"]
bootstrap = pathlib.Path(artifacts["adapter_dir"])
trained = pathlib.Path(artifacts["trained_adapter_dir"])
bootstrap.mkdir(parents=True)
trained.mkdir(parents=True)
(bootstrap / "adapter_model.safetensors").write_bytes(b"initial")
(trained / "adapter_model.safetensors").write_bytes(b"trained")
evidence = {
    "schema_version": "antfly_training_execution_evidence/v1",
    "train_steps": updates * 2,
    "eval_steps": 0,
    "graph_executor_partitions": updates * 2,
    "graph_executor_planned_dispatches": updates * 20,
    "graph_executor_fallback_steps": 0,
    "graph_executor_native_partitions": 0,
    "graph_executor_unsupported_ops": 0,
    "graph_executor_interpreter_fallbacks": 0,
    "graph_executor_true_host_outputs": 0,
    "runtime_input_uploads": updates * 6,
    "runtime_input_upload_bytes": updates * 48,
    "runtime_input_h2d_bytes": updates * 48,
    "runtime_input_d2h_bytes": 0,
    "declared_runtime_input_uploads": updates * 6,
    "declared_runtime_input_upload_bytes": updates * 48,
    "declared_runtime_input_h2d_bytes": updates * 48,
    "compiled_session_setup_d2h_bytes": 0,
    "graph_execution_h2d_bytes": 0,
    "graph_execution_d2h_bytes": updates * 8,
    "training_runtime_d2h_bytes": updates * 12,
    "host_gradient_tensors": 0,
    "cuda_kernel_launches": updates * 20,
    "cuda_d2h_bytes": updates * 12,
    "cuda_largest_d2h_transfer_bytes": 4,
    "peak_resident_bytes": 536870912,
}
report = {
    "policy_backend": "cuda",
    "optimizer_backend": "cuda",
    "optimizer_steps": updates,
    "cuda_optimizer_steps": updates,
    "micro_batch_steps": updates * 2,
    "loss": 0.5,
    "beta": 0.1,
    "mean_grad_norm": 0.25,
    "trainable_update": {"tensor_count": 4, "changed_tensor_count": 2, "max_abs_delta": 0.001},
    "device_execution_scope": "optimizer-steps-only;excludes-rollout-and-reference-scoring",
    "device_execution": evidence,
}
pathlib.Path(artifacts["report_path"]).write_text(json.dumps(report), encoding="utf-8")
""",
            encoding="utf-8",
        )
        executable.chmod(0o755)
        with mock.patch.object(smoke, "query_gpu_memory_mib", return_value=512):
            result = smoke.run_case(
                executable,
                smoke.ModelSpec("e2b", self.root / "model"),
                "dpo",
                None,
                self.root / "run",
                128,
                8,
                32,
                1,
                30,
            )
        self.assertEqual("passed", result["status"])
        self.assertEqual(512, result["peak_gpu_memory_mib"])
        self.assertGreater(result["optimizer_steps_per_second"], 0)
        self.assertNotEqual(result["initial_adapter_sha256"], result["trained_adapter_sha256"])

    def test_successful_process_metrics_reject_missing_gpu_memory_samples(self) -> None:
        executable = self.root / "success"
        executable.write_text(
            "#!/bin/sh\n"
            'test "$ANTFLY_INFERENCE_CUDA_CUBLASLT_BF16_TUNING_PROFILE" = off\n',
            encoding="utf-8",
        )
        executable.chmod(0o755)
        with mock.patch.object(smoke, "query_gpu_memory_mib", return_value=None):
            metrics = smoke.run_command([str(executable)], self.root / "run.log", 5)
        self.assertEqual(0, metrics.return_code)
        with self.assertRaisesRegex(smoke.QualificationError, "no positive nvidia-smi"):
            smoke.validate_process_metrics(metrics)

    def test_unsloth_baseline_is_fingerprint_and_protocol_bound(self) -> None:
        model = write_model(self.root)
        inspected = smoke.inspect_model(smoke.ModelSpec("e2b", model))
        protocol = smoke.benchmark_protocol(128, 16, 32)
        row = {
            "label": "e2b",
            "objective": "dpo",
            "model_config_sha256": inspected.config_sha256,
            "optimizer_steps": smoke.BENCHMARK_UPDATES,
            "median_wall_seconds": 10.0,
            "median_optimizer_steps_per_second": 2.5,
            "median_peak_gpu_memory_mib": 9000,
            "loss": 0.5,
            "mean_grad_norm": 0.2,
        }
        path = self.root / "unsloth.json"
        path.write_text(
            json.dumps(
                {
                    "schema_version": smoke.BASELINE_SCHEMA,
                    "status": "passed",
                    "protocol": protocol,
                    "cases": [row],
                }
            ),
            encoding="utf-8",
        )
        indexed = smoke.load_unsloth_baseline(path, [inspected], ["dpo"], protocol)
        self.assertEqual(10.0, indexed[("e2b", "dpo")]["median_wall_seconds"])
        path.write_text(
            json.dumps(
                {
                    "schema_version": smoke.BASELINE_SCHEMA,
                    "status": "running",
                    "protocol": protocol,
                    "cases": [row],
                }
            ),
            encoding="utf-8",
        )
        with self.assertRaisesRegex(smoke.QualificationError, "terminal passed status"):
            smoke.load_unsloth_baseline(path, [inspected], ["dpo"], protocol)

        row["model_config_sha256"] = "wrong"
        path.write_text(
            json.dumps(
                {
                    "schema_version": smoke.BASELINE_SCHEMA,
                    "status": "passed",
                    "protocol": protocol,
                    "cases": [row],
                }
            ),
            encoding="utf-8",
        )
        with self.assertRaisesRegex(smoke.QualificationError, "fingerprint mismatch"):
            smoke.load_unsloth_baseline(path, [inspected], ["dpo"], protocol)

    def test_comparison_reports_time_throughput_and_memory_ratios(self) -> None:
        comparison = smoke.compare_with_unsloth(
            {
                "median_wall_seconds": 8.0,
                "median_optimizer_steps_per_second": 3.0,
                "median_peak_gpu_memory_mib": 8000,
            },
            {
                "median_wall_seconds": 10.0,
                "median_optimizer_steps_per_second": 2.5,
                "median_peak_gpu_memory_mib": 10000,
            },
        )
        self.assertEqual(0.8, comparison["zig_to_unsloth_wall_time_ratio"])
        self.assertEqual(1.2, comparison["zig_to_unsloth_optimizer_throughput_ratio"])
        self.assertEqual(0.8, comparison["zig_to_unsloth_peak_gpu_memory_ratio"])

    def test_benchmark_requires_three_repetitions_and_baseline(self) -> None:
        with mock.patch("sys.stderr"):
            self.assertEqual(
                2,
                smoke.main(["--model", f"e2b={self.root / 'missing'}", "--benchmark"]),
            )
            self.assertEqual(
                2,
                smoke.main(
                    [
                        "--model",
                        f"e2b={self.root / 'missing'}",
                        "--benchmark",
                        "--repetitions",
                        "3",
                    ]
                ),
            )

    def test_monolith_binary_command_includes_inference_namespace(self) -> None:
        command = smoke.finetune_command(self.root / "antfly", self.root / "recipe.json")
        self.assertEqual(["inference", "finetune", "run"], command[1:4])


if __name__ == "__main__":
    unittest.main()
