from __future__ import annotations

import unittest

from compare_gliner2_lora_python_zig import (
    UPSTREAM_SAMPLING_DEFAULTS,
    compare_component_losses,
    format_finite_number,
    python_training_script,
    resolve_python_sampling_policy,
    resolve_python_schema_conditioning_policy,
    within_loss_tolerance,
)


class ComparisonContractTest(unittest.TestCase):
    def test_sampling_policy_auto_tracks_comparison_mode(self) -> None:
        self.assertEqual("disabled", resolve_python_sampling_policy(True, "auto"))
        self.assertEqual("upstream-default", resolve_python_sampling_policy(False, "auto"))
        self.assertEqual("disabled", resolve_python_sampling_policy(False, "disabled"))
        with self.assertRaisesRegex(ValueError, "requires"):
            resolve_python_sampling_policy(True, "upstream-default")

    def test_generated_fastino_trainer_records_and_conditionally_applies_sampling(self) -> None:
        script = python_training_script()
        self.assertIn('if args.sampling_policy == "disabled":', script)
        self.assertIn('"sampling_config": applied_sampling_config', script)
        self.assertIn(
            'schema_conditioning_policy = "ordered-training-form" if args.no_train_shuffle else "upstream-training-default"',
            script,
        )
        self.assertIn("if args.training_deterministic:\n    for _conditioning_method", script)
        self.assertIn('"schema_conditioning_policy": schema_conditioning_policy', script)
        self.assertEqual(0.2, UPSTREAM_SAMPLING_DEFAULTS["remove_json_structure_prob"])
        self.assertEqual(0.5, UPSTREAM_SAMPLING_DEFAULTS["synthetic_label_prob"])
        self.assertTrue(UPSTREAM_SAMPLING_DEFAULTS["shuffle_classification_labels"])

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
        matches, deltas = compare_component_losses(components, zig_components, 1e-4, 5e-6)
        self.assertTrue(matches)
        self.assertTrue(all(row["ok"] for row in deltas.values()))

    def test_failure_metric_formatting_is_fail_closed(self) -> None:
        self.assertEqual("0.000175476074", format_finite_number(0.00017547607421875))
        self.assertEqual("unavailable", format_finite_number(None))
        self.assertEqual("unavailable", format_finite_number(float("nan")))


if __name__ == "__main__":
    unittest.main()
