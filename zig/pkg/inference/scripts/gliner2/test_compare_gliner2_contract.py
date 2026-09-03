from __future__ import annotations

import argparse
import unittest

from compare_gliner2_lora_python_zig import (
    UPSTREAM_SAMPLING_DEFAULTS,
    compare_component_losses,
    compare_optimizer_parity,
    format_finite_number,
    optimizer_parity_gate,
    python_training_script,
    resolve_python_sampling_policy,
    resolve_python_schema_conditioning_policy,
    summarize_cuda_readiness,
    summarize_metal_readiness,
    within_loss_tolerance,
    zig_training_environment,
)


class ComparisonContractTest(unittest.TestCase):
    @staticmethod
    def _optimizer_tensor(
        name: str, *, step_count: int = 1, weight_abs_sum: float = 1.0
    ) -> dict:
        return {
            "name": name,
            "step_count": step_count,
            "weight": [0.25],
            "weight_abs_sum": weight_abs_sum,
            "m": [0.1] if step_count else [],
            "m_abs_sum": 0.1 if step_count else 0.0,
            "v": [0.01] if step_count else [],
            "v_abs_sum": 0.01 if step_count else 0.0,
            "grad": [1.0] if step_count else [],
        }

    def test_sampling_policy_auto_tracks_comparison_mode(self) -> None:
        self.assertEqual("disabled", resolve_python_sampling_policy(True, "auto"))
        self.assertEqual(
            "upstream-default", resolve_python_sampling_policy(False, "auto")
        )
        self.assertEqual("disabled", resolve_python_sampling_policy(False, "disabled"))
        with self.assertRaisesRegex(ValueError, "requires"):
            resolve_python_sampling_policy(True, "upstream-default")

    def test_generated_fastino_trainer_records_and_conditionally_applies_sampling(
        self,
    ) -> None:
        script = python_training_script()
        self.assertIn('if args.sampling_policy == "disabled":', script)
        self.assertIn('"sampling_config": applied_sampling_config', script)
        self.assertIn(
            'schema_conditioning_policy = "ordered-training-form" if args.no_train_shuffle else "upstream-training-default"',
            script,
        )
        self.assertIn(
            "if args.training_deterministic:\n    for _conditioning_method", script
        )
        self.assertIn(
            '"schema_conditioning_policy": schema_conditioning_policy', script
        )
        self.assertEqual(0.2, UPSTREAM_SAMPLING_DEFAULTS["remove_json_structure_prob"])
        self.assertEqual(0.5, UPSTREAM_SAMPLING_DEFAULTS["synthetic_label_prob"])
        self.assertTrue(UPSTREAM_SAMPLING_DEFAULTS["shuffle_classification_labels"])

    def test_generated_fastino_trainer_supports_synchronized_cuda_measurement(
        self,
    ) -> None:
        script = python_training_script()
        self.assertIn(
            'p.add_argument("--device", choices=("auto", "cpu", "cuda")', script
        )
        self.assertIn("model.to(training_device)", script)
        self.assertIn("torch.cuda.synchronize(training_device)", script)
        self.assertIn(
            '"cuda_device_name": torch.cuda.get_device_name(training_device)', script
        )

    def test_schema_conditioning_policy_records_every_harness_mode(self) -> None:
        self.assertEqual(
            "upstream-training-default",
            resolve_python_schema_conditioning_policy(False, False),
        )
        self.assertEqual(
            "ordered-training-form",
            resolve_python_schema_conditioning_policy(False, True),
        )
        self.assertEqual(
            "deterministic-eval-form",
            resolve_python_schema_conditioning_policy(True, True),
        )

    def test_accelerator_graph_executor_environment_is_explicit(self) -> None:
        metal = zig_training_environment(
            argparse.Namespace(
                zig_backend="metal",
                zig_training_graph_executor=True,
            )
        )
        self.assertEqual({"TERMITE_ENABLE_TRAINING_GRAPH_EXECUTOR": "1"}, metal)

        cuda = zig_training_environment(
            argparse.Namespace(
                zig_backend="cuda",
                zig_training_graph_executor=True,
            )
        )
        self.assertEqual({"TERMITE_ENABLE_TRAINING_GRAPH_EXECUTOR": "1"}, cuda)

    def test_loss_tolerance_scales_summed_full_task_losses(self) -> None:
        ok, bound = within_loss_tolerance(55.20604348015485, 55.205818176, 1e-4, 5e-6)
        self.assertTrue(ok)
        self.assertGreater(bound, 0.00022530415484567357)
        self.assertFalse(within_loss_tolerance(55.206, 55.196, 1e-4, 5e-6)[0])

        components = {
            "classification_loss": 9.643522356,
            "structure_loss": 55.206043480,
            "count_loss": 0.004974797,
            "total_loss": 64.854540634,
        }
        zig_components = {
            "classification_loss": 9.643534660,
            "structure_loss": 55.205818176,
            "count_loss": 0.004974916,
            "total_loss": 64.854327752,
        }
        matches, deltas = compare_component_losses(
            components, zig_components, 1e-4, 5e-6
        )
        self.assertTrue(matches)
        self.assertTrue(all(row["ok"] for row in deltas.values()))

    def test_failure_metric_formatting_is_fail_closed(self) -> None:
        self.assertEqual("0.000175476074", format_finite_number(0.00017547607421875))
        self.assertEqual("unavailable", format_finite_number(None))
        self.assertEqual("unavailable", format_finite_number(float("nan")))

    def test_optimizer_parity_ignores_only_proven_inert_python_mha_out_proj(
        self,
    ) -> None:
        common = self._optimizer_tensor(
            "encoder.encoder.layer.0.attention.self.query_proj.lora_A"
        )
        inert_a = self._optimizer_tensor(
            "count_embed.transformer.transformer.layers.0.self_attn.out_proj.lora_A",
            step_count=0,
            weight_abs_sum=2.0,
        )
        inert_b = self._optimizer_tensor(
            "count_embed.transformer.transformer.layers.0.self_attn.out_proj.lora_B",
            step_count=0,
            weight_abs_sum=0.0,
        )
        comparison = compare_optimizer_parity(
            [{"step": 1, "tensors": [common, inert_a, inert_b]}],
            [{"step": 1, "tensors": [dict(common)]}],
        )
        row = comparison["steps"][0]
        self.assertEqual(0, row["python_only_tensors"])
        self.assertEqual(2, row["python_only_inert_tensors_ignored"])
        self.assertEqual(2, row["raw_python_only_tensors"])
        self.assertTrue(optimizer_parity_gate(comparison, 1e-6)[0])

        active_b = dict(
            inert_b,
            step_count=1,
            m=[0.1],
            m_abs_sum=0.1,
            v=[0.01],
            v_abs_sum=0.01,
            grad=[1.0],
            weight_abs_sum=0.5,
        )
        active_comparison = compare_optimizer_parity(
            [{"step": 1, "tensors": [common, inert_a, active_b]}],
            [{"step": 1, "tensors": [dict(common)]}],
        )
        active_row = active_comparison["steps"][0]
        self.assertEqual(2, active_row["python_only_tensors"])
        self.assertEqual(0, active_row["python_only_inert_tensors_ignored"])
        self.assertFalse(optimizer_parity_gate(active_comparison, 1e-6)[0])

        future_layer_a = dict(
            inert_a,
            name="count_embed.transformer.transformer.layers.2.self_attn.out_proj.lora_A",
        )
        future_layer_b = dict(
            inert_b,
            name="count_embed.transformer.transformer.layers.2.self_attn.out_proj.lora_B",
        )
        future_layer_comparison = compare_optimizer_parity(
            [{"step": 1, "tensors": [common, future_layer_a, future_layer_b]}],
            [{"step": 1, "tensors": [dict(common)]}],
        )
        future_layer_row = future_layer_comparison["steps"][0]
        self.assertEqual(2, future_layer_row["python_only_tensors"])
        self.assertEqual(0, future_layer_row["python_only_inert_tensors_ignored"])
        self.assertFalse(optimizer_parity_gate(future_layer_comparison, 1e-6)[0])

    def test_cuda_readiness_requires_cuda_optimizer_and_resident_trainables(
        self,
    ) -> None:
        args = argparse.Namespace(
            zig_backend="cuda",
            zig_build_cuda=True,
            zig_objective="gliner2-total-loss",
            zig_training_graph_executor=True,
            metal_max_interpreter_fallbacks=64,
        )
        report = {
            "zig": {
                "returncode": 0,
                "metrics": {"step_loss": 1.25, "grad_norm": 0.5},
            }
        }
        rows = [
            {
                "optimizer_backend": "cuda",
                "device_resident_transfer_count": 0,
                "device_trainable_bytes": 4096,
                "graph_executor_planned_dispatches": 8,
                "graph_executor_interpreter_fallbacks": 0,
                "graph_executor_true_host_outputs": 0,
                "cuda_kernel_launches": 8,
                "cuda_h2d_bytes": 4096,
                "cuda_training_input_uploads": 4,
                "cuda_training_input_upload_bytes": 4096,
                "cuda_d2h_bytes": 8,
                "cuda_largest_d2h_transfer_bytes": 4,
                "cuda_to_float32_calls": 2,
                "cuda_upload_synchronizations": 0,
                "cuda_packed_attention_forward_calls": 1,
                "cuda_packed_attention_backward_calls": 1,
                "cuda_exact_gelu_forward_calls": 1,
                "cuda_exact_gelu_backward_calls": 1,
            }
        ]
        summary = summarize_cuda_readiness(
            args,
            report,
            rows,
            {
                "backend": "CUDA",
                "objective": "gliner2-total-loss",
                "training_precision": "fp32",
                "optimizer_state_precision": "fp32",
            },
        )
        self.assertIsNotNone(summary)
        self.assertTrue(summary["ok"])
        self.assertTrue(summary["checks"]["manifest_backend_is_cuda"])
        self.assertTrue(summary["checks"]["optimizer_backend_is_cuda"])
        self.assertTrue(summary["checks"]["cuda_runtime_telemetry_present"])
        self.assertTrue(summary["checks"]["cuda_full_step_h2d_accounted"])
        self.assertTrue(summary["checks"]["cuda_only_scalar_metrics_downloaded"])

        bulk_download = [dict(rows[0])]
        bulk_download[0]["cuda_largest_d2h_transfer_bytes"] = 8
        bulk_summary = summarize_cuda_readiness(
            args,
            report,
            bulk_download,
            {
                "backend": "CUDA",
                "objective": "gliner2-total-loss",
                "training_precision": "fp32",
                "optimizer_state_precision": "fp32",
            },
        )
        self.assertFalse(bulk_summary["checks"]["cuda_bulk_d2h_eliminated"])

        extra_metric_download = [dict(rows[0])]
        extra_metric_download[0]["cuda_to_float32_calls"] = 3
        extra_metric_summary = summarize_cuda_readiness(
            args,
            report,
            extra_metric_download,
            {
                "backend": "CUDA",
                "objective": "gliner2-total-loss",
                "training_precision": "fp32",
                "optimizer_state_precision": "fp32",
            },
        )
        self.assertFalse(
            extra_metric_summary["checks"]["cuda_only_scalar_metrics_downloaded"]
        )

        missing_telemetry_row = {
            key: value for key, value in rows[0].items() if not key.startswith("cuda_")
        }
        missing_telemetry = summarize_cuda_readiness(
            args,
            report,
            [missing_telemetry_row],
            {
                "backend": "CUDA",
                "objective": "gliner2-total-loss",
                "training_precision": "fp32",
                "optimizer_state_precision": "fp32",
            },
        )
        self.assertFalse(missing_telemetry["ok"])
        self.assertFalse(missing_telemetry["checks"]["cuda_runtime_telemetry_present"])
        self.assertFalse(missing_telemetry["checks"]["cuda_full_step_h2d_accounted"])

        partial_telemetry_row = dict(rows[0])
        del partial_telemetry_row["cuda_largest_d2h_transfer_bytes"]
        partial_telemetry = summarize_cuda_readiness(
            args,
            report,
            [partial_telemetry_row],
            {
                "backend": "CUDA",
                "objective": "gliner2-total-loss",
                "training_precision": "fp32",
                "optimizer_state_precision": "fp32",
            },
        )
        self.assertFalse(partial_telemetry["ok"])
        self.assertFalse(partial_telemetry["checks"]["cuda_runtime_telemetry_present"])

        cold_then_hot = [dict(rows[0]), dict(rows[0])]
        cold_then_hot[0]["cuda_upload_synchronizations"] = 1
        bounded = summarize_cuda_readiness(
            args,
            report,
            cold_then_hot,
            {
                "backend": "CUDA",
                "objective": "gliner2-total-loss",
                "training_precision": "fp32",
                "optimizer_state_precision": "fp32",
            },
        )
        self.assertTrue(bounded["checks"]["cuda_upload_synchronizations_bounded"])
        cold_then_hot[1]["cuda_upload_synchronizations"] = 1
        repeated = summarize_cuda_readiness(
            args,
            report,
            cold_then_hot,
            {
                "backend": "CUDA",
                "objective": "gliner2-total-loss",
                "training_precision": "fp32",
                "optimizer_state_precision": "fp32",
            },
        )
        self.assertFalse(repeated["checks"]["cuda_upload_synchronizations_bounded"])

        rows[0]["optimizer_backend"] = "host"
        failed = summarize_cuda_readiness(
            args,
            report,
            rows,
            {
                "backend": "CUDA",
                "objective": "gliner2-total-loss",
                "training_precision": "fp32",
                "optimizer_state_precision": "fp32",
            },
        )
        self.assertFalse(failed["ok"])
        self.assertFalse(failed["checks"]["optimizer_backend_is_cuda"])

    def test_metal_readiness_allows_only_bounded_host_index_metadata(self) -> None:
        args = argparse.Namespace(
            zig_backend="metal",
            zig_build_metal=True,
            zig_objective="gliner2-total-loss",
            zig_training_graph_executor=True,
            metal_max_interpreter_fallbacks=64,
            metal_max_true_host_outputs_per_step=6,
        )
        row = {
            "optimizer_backend": "metal",
            "device_resident_transfer_count": 0,
            "device_trainable_bytes": 4096,
            "graph_executor_command_dispatches": 8,
            "graph_executor_interpreter_fallbacks": 6,
            "graph_executor_true_host_outputs": 6,
            "graph_executor_host_output_command": 0,
            "graph_executor_host_output_interpreter": 6,
            "graph_executor_host_output_pre_materialized_constant": 0,
            "graph_executor_host_output_runtime_region": 0,
            "graph_executor_host_output_unattributed": 0,
        }
        report = {
            "zig": {"returncode": 0, "metrics": {"step_loss": 1.0, "grad_norm": 0.5}}
        }
        manifest = {
            "backend": "Metal",
            "objective": "gliner2-total-loss",
            "training_precision": "fp32",
            "optimizer_state_precision": "fp32",
        }
        summary = summarize_metal_readiness(args, report, [row], manifest)
        self.assertIsNotNone(summary)
        self.assertTrue(
            summary["checks"]["graph_executor_true_host_outputs_within_threshold"]
        )

        excessive = dict(row, graph_executor_true_host_outputs=7)
        failed = summarize_metal_readiness(args, report, [excessive], manifest)
        self.assertFalse(
            failed["checks"]["graph_executor_true_host_outputs_within_threshold"]
        )

        wrong_category = dict(
            row,
            graph_executor_host_output_command=1,
            graph_executor_host_output_interpreter=5,
        )
        failed = summarize_metal_readiness(args, report, [wrong_category], manifest)
        self.assertFalse(
            failed["checks"]["graph_executor_true_host_outputs_within_threshold"]
        )


if __name__ == "__main__":
    unittest.main()
