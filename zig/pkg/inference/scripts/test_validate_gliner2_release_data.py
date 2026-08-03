#!/usr/bin/env python3

import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from validate_gliner2_release_data import (
    MODEL_FINGERPRINT_FILES,
    DatasetContent,
    adapter_bundle_fingerprint,
    base_model_fingerprint,
    digest,
    load_dataset,
    record_tasks,
    sha256_file,
    validate_release_artifact_provenance,
    validate_release_data,
)


ALL_TASKS = frozenset({"entities", "classifications", "json_structures", "relations"})


def dataset(records: list[str], texts: list[str], tasks: frozenset[str] = ALL_TASKS) -> DatasetContent:
    hashes = [digest(row) for row in records]
    return DatasetContent(len(hashes), len(set(hashes)) != len(hashes), frozenset(digest(text) for text in texts), tasks)


class ReleaseDataValidationTest(unittest.TestCase):
    def test_base_model_fingerprint_accepts_absent_optional_spm(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            model_dir = Path(tmp)
            for relative_path in MODEL_FINGERPRINT_FILES:
                if relative_path == "spm.model":
                    continue
                path = model_dir / relative_path
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(relative_path.encode())
            self.assertEqual(
                "sha256:9317c6a7c2d586358da84851ecfe259a2075724436fbc26736b9f15d7fdaa638",
                base_model_fingerprint(model_dir),
            )

    def test_accepts_disjoint_full_task_data_and_rejects_leaks(self) -> None:
        train = dataset(["train-a", "train-b"], ["alpha", "beta"])
        eval_ = dataset(["eval-a"], ["gamma"])
        validate_release_data(train, eval_, frozenset(), True)

        bad_inputs = (
            (dataset(["same", "same"], ["alpha"]), eval_, frozenset(), True),
            (train, dataset(["eval-a", "eval-a"], ["gamma"]), frozenset(), True),
            (train, dataset(["eval-a"], ["alpha"]), frozenset(), True),
            (train, eval_, frozenset({digest("beta")}), True),
            (dataset(["train-a"], ["alpha"], frozenset({"entities"})), eval_, frozenset(), True),
        )
        for args in bad_inputs:
            with self.subTest(args=args), self.assertRaises(ValueError):
                validate_release_data(*args)

    def test_enforces_release_size_and_compared_slice_coverage(self) -> None:
        train = dataset(["train-a", "train-b"], ["alpha", "beta"])
        eval_ = dataset(["eval-a"], ["gamma"])
        prefix = dataset(["train-a"], ["alpha"], frozenset({"entities"}))
        with self.assertRaises(ValueError):
            validate_release_data(train, eval_, frozenset(), True, min_train_records=3)
        with self.assertRaises(ValueError):
            validate_release_data(train, eval_, frozenset(), True, min_eval_records=2)
        with self.assertRaises(ValueError):
            validate_release_data(train, eval_, frozenset(), True, comparison_train=prefix)

    def test_empty_task_placeholders_do_not_count_as_coverage(self) -> None:
        empty = {
            "input": "fine",
            "output": {"entities": {}, "classifications": [], "json_structures": [], "relations": []},
        }
        self.assertEqual(set(), record_tasks(empty))
        negative_or_incomplete = {
            "input": "fine",
            "output": {
                "classifications": [
                    {"task": "priority", "labels": ["normal", "urgent"], "true_label": []}
                ],
                "relations": [{"works_for": {"head": "Alice", "tail": ""}}],
            },
        }
        self.assertEqual(set(), record_tasks(negative_or_incomplete))

    def test_leak_detection_normalizes_case_whitespace_and_unicode(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            train_path = root / "train.jsonl"
            eval_path = root / "eval.jsonl"
            train_path.write_text(json.dumps({"input": "  CAFÉ   worker "}) + "\n", encoding="utf-8")
            eval_path.write_text(json.dumps({"input": "cafe\u0301 worker"}) + "\n", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "overlap"):
                validate_release_data(load_dataset(train_path), load_dataset(eval_path), frozenset(), False)

    def test_rejects_recycled_normalized_inputs_with_different_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "train.jsonl"
            rows = [
                {"input": "CAFÉ worker", "metadata": 1},
                {"input": " cafe\u0301   WORKER ", "metadata": 2},
            ]
            path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "normalized input texts"):
                validate_release_data(load_dataset(path), None, frozenset(), False)

    def test_release_adapter_provenance_matches_exact_training_data_and_model(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            model_dir = root / "model"
            adapter_dir = root / "adapter"
            model_dir.mkdir()
            adapter_dir.mkdir()
            for relative_path in MODEL_FINGERPRINT_FILES:
                model_path = model_dir / relative_path
                model_path.parent.mkdir(parents=True, exist_ok=True)
                model_path.write_bytes(relative_path.encode())
            train_path = root / "train.jsonl"
            train_path.write_text('{"input":"training row"}\n', encoding="utf-8")
            eval_path = root / "eval.jsonl"
            eval_path.write_text('{"input":"held-out row"}\n', encoding="utf-8")
            (adapter_dir / "adapter_model.safetensors").write_bytes(b"adapter weights")
            (adapter_dir / "task_head.safetensors").write_bytes(b"compatibility head")
            best_checkpoint = adapter_dir / "checkpoints" / "best-epoch-1.safetensors"
            best_checkpoint.parent.mkdir()
            best_checkpoint.write_bytes(b"best training state")
            adapter_config = {
                "base_model_name_or_path": str(model_dir),
                "r": 16,
                "lora_alpha": 32,
                "lora_dropout": 0,
                "target_modules": ["encoder.layer.0.query_proj"],
            }
            (adapter_dir / "adapter_config.json").write_text(json.dumps(adapter_config), encoding="utf-8")
            manifest = {
                "schema_version": "gliner2_autodiff_training/v3",
                "backend": "Metal",
                "compiled_required": True,
                "objective": "gliner2-total-loss",
                "lora_only_trainables": True,
                "deterministic": False,
                "sampling_config": "disabled",
                "schema_conditioning_policy": "deterministic-eval-form",
                "model_dropout": "disabled",
                "train_shuffle": True,
                "span_negative_mask_rate": 0.5,
                "graph_shape_policy": "batch-local-v2",
                "graph_seq_bucket_multiple": 8,
                "graph_span_word_bucket_policy": "bounded-next-power-of-two",
                "graph_schema_bucket_policy": "bounded-next-power-of-two",
                "graph_cache_capacity": 2,
                "graph_cache_build_reserve_bytes": 1024,
                "graph_cache_builds": 2,
                "graph_cache_hits": 1,
                "graph_cache_active_reuses": 1,
                "graph_cache_evictions": 0,
                "graph_cache_resident_signatures": 2,
                "graph_cache_peak_resident_signatures": 2,
                "graph_cache_runtime_input_policy": "active-entry-only",
                "initial_adapter_checkpoint_used": False,
                "lora_rank": 16,
                "lora_alpha": 32,
                "lora_dropout": 0,
                "resolved_lora_targets": ["encoder.layer.0.query_proj"],
                "max_examples": 0,
                "max_steps": 0,
                "requested_max_steps": 0,
                "steps_per_epoch": 1,
                "grad_accum": 1,
                "optimizer_steps": 1,
                "epochs": 1,
                "requested_epochs": 1,
                "effective_steps": 1,
                "micro_batch_steps": 1,
                "total_steps": 1,
                "example_count": 1,
                "examples_per_epoch": 1,
                "batch_size": 1,
                "effective_batch_size": 1,
                "model_dir": str(model_dir),
                "train_data_sha256": sha256_file(train_path),
                "eval_data": str(eval_path),
                "eval_data_sha256": sha256_file(eval_path),
                "eval_every_epochs": 1,
                "eval_batch_size": 8,
                "early_stopping_patience": 0,
                "early_stopping_threshold": 0,
                "early_stopped": False,
                "eval_count": 1,
                "best_eval_loss": 1.0,
                "best_epoch": 1,
                "early_stopping_bad_epochs": 0,
                "selected_checkpoint": "checkpoints/best-epoch-1.safetensors",
                "best_checkpoint_sha256": sha256_file(best_checkpoint),
                "training_state_fingerprint_sha256": "sha256:" + "2" * 64,
                "base_model_fingerprint_sha256": base_model_fingerprint(model_dir),
                "adapter_bundle_fingerprint_sha256": adapter_bundle_fingerprint(adapter_dir),
            }
            metal_step = {
                "event": "step",
                "step": 1,
                "loss": 1.0,
                "optimizer_backend": "metal",
                "device_trainable_transfer_count": 0,
                "device_resident_transfer_count": 0,
                "device_trainable_bytes": 128,
                "graph_executor_fallback_reason": None,
                "graph_executor_partitions": 1,
                "graph_executor_command_dispatches": 1,
                "graph_executor_planned_dispatches": 1,
                "graph_executor_interpreter_fallbacks": 0,
                "graph_executor_true_host_outputs": 0,
            }
            def write_metal_steps(count: int) -> None:
                (adapter_dir / "training_metrics.jsonl").write_text(
                    "".join(json.dumps({**metal_step, "step": step}) + "\n" for step in range(1, count + 1)),
                    encoding="utf-8",
                )

            write_metal_steps(1)
            (adapter_dir / "training_manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
            validate_release_artifact_provenance(model_dir, train_path, adapter_dir)

            for key, bad_value in (("backend", "native"), ("compiled_required", False)):
                with self.subTest(production_manifest_field=key, bad_value=bad_value):
                    original = manifest[key]
                    manifest[key] = bad_value
                    (adapter_dir / "training_manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
                    with self.assertRaisesRegex(ValueError, "Metal training with compiled_required"):
                        validate_release_artifact_provenance(model_dir, train_path, adapter_dir)
                    manifest[key] = original

            bad_training_policy_values = (
                ("deterministic", True),
                ("sampling_config", "upstream-default"),
                ("schema_conditioning_policy", "upstream-training-default"),
                ("model_dropout", "upstream-default"),
                ("train_shuffle", False),
                ("lora_dropout", 0.1),
                ("span_negative_mask_rate", 0.0),
            )
            for key, bad_value in bad_training_policy_values:
                with self.subTest(training_policy_field=key, bad_value=bad_value):
                    original = manifest[key]
                    manifest[key] = bad_value
                    (adapter_dir / "training_manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
                    with self.assertRaisesRegex(ValueError, "training-policy provenance"):
                        validate_release_artifact_provenance(model_dir, train_path, adapter_dir)
                    manifest[key] = original

            bad_step_values = (
                ("optimizer_backend", "host"),
                ("device_resident_transfer_count", 1),
                ("device_trainable_bytes", 0),
                ("graph_executor_fallback_reason", "unsupported op"),
                ("graph_executor_partitions", 0),
                ("graph_executor_interpreter_fallbacks", 1),
                ("graph_executor_true_host_outputs", 1),
            )
            for key, bad_value in bad_step_values:
                with self.subTest(production_metric_field=key, bad_value=bad_value):
                    bad_step = dict(metal_step)
                    bad_step[key] = bad_value
                    (adapter_dir / "training_metrics.jsonl").write_text(
                        json.dumps(bad_step) + "\n", encoding="utf-8"
                    )
                    (adapter_dir / "training_manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
                    with self.assertRaisesRegex(ValueError, "release adapter step"):
                        validate_release_artifact_provenance(model_dir, train_path, adapter_dir)
            write_metal_steps(1)

            bad_shape_cache_values = (
                ("graph_shape_policy", "dataset-global"),
                ("graph_seq_bucket_multiple", 7),
                ("graph_span_word_bucket_policy", "fixed"),
                ("graph_schema_bucket_policy", "fixed"),
                ("graph_cache_capacity", 0),
                ("graph_cache_capacity", 9),
                ("graph_cache_capacity", True),
                ("graph_cache_runtime_input_policy", "per-signature"),
                ("graph_cache_build_reserve_bytes", 0),
                ("graph_cache_builds", -1),
                ("graph_cache_hits", False),
                ("graph_cache_peak_resident_signatures", 3),
                ("graph_cache_resident_signatures", 0),
            )
            for key, bad_value in bad_shape_cache_values:
                with self.subTest(shape_cache_field=key, bad_value=bad_value):
                    original = manifest[key]
                    manifest[key] = bad_value
                    (adapter_dir / "training_manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
                    with self.assertRaisesRegex(ValueError, "graph-shape/cache provenance"):
                        validate_release_artifact_provenance(model_dir, train_path, adapter_dir)
                    manifest[key] = original
            missing_shape_policy = manifest.pop("graph_shape_policy")
            (adapter_dir / "training_manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "graph-shape/cache provenance"):
                validate_release_artifact_provenance(model_dir, train_path, adapter_dir)
            manifest["graph_shape_policy"] = missing_shape_policy
            (adapter_dir / "training_manifest.json").write_text(json.dumps(manifest), encoding="utf-8")

            original_eval_digest = manifest["eval_data_sha256"]
            manifest["eval_data_sha256"] = "sha256:" + "0" * 64
            (adapter_dir / "training_manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "eval-data fingerprint"):
                validate_release_artifact_provenance(model_dir, train_path, adapter_dir, eval_data=eval_path)
            manifest["eval_data_sha256"] = original_eval_digest

            original_best_digest = manifest["best_checkpoint_sha256"]
            manifest["best_checkpoint_sha256"] = "sha256:" + "0" * 64
            (adapter_dir / "training_manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "best-checkpoint fingerprint"):
                validate_release_artifact_provenance(model_dir, train_path, adapter_dir)
            manifest["best_checkpoint_sha256"] = original_best_digest
            (adapter_dir / "training_manifest.json").write_text(json.dumps(manifest), encoding="utf-8")

            resume_bytes = b"resume training state"
            resume_digest = "sha256:" + hashlib.sha256(resume_bytes).hexdigest()
            resume_relative_path = (
                f"checkpoints/resume-source-{resume_digest.removeprefix('sha256:')}.safetensors"
            )
            resume_checkpoint = adapter_dir / resume_relative_path
            resume_checkpoint.write_bytes(resume_bytes)
            manifest.update(
                resume_checkpoint_used=True,
                resume_checkpoint=resume_relative_path,
                resume_checkpoint_sha256=resume_digest,
                training_state_fingerprint_sha256="sha256:" + "2" * 64,
                resume_restored_micro_batch_steps=1,
                resume_restored_optimizer_steps=1,
                resume_restored_epochs=1,
            )
            (adapter_dir / "training_manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
            validate_release_artifact_provenance(model_dir, train_path, adapter_dir)
            manifest.update(
                epochs=2,
                requested_epochs=2,
                optimizer_steps=2,
                effective_steps=2,
                micro_batch_steps=2,
                total_steps=2,
                eval_count=2,
                resume_restored_micro_batch_steps=2,
            )
            (adapter_dir / "training_manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "resumed-run accounting"):
                validate_release_artifact_provenance(model_dir, train_path, adapter_dir)
            manifest.update(
                epochs=1,
                requested_epochs=1,
                optimizer_steps=1,
                effective_steps=1,
                micro_batch_steps=1,
                total_steps=1,
                eval_count=1,
                resume_restored_micro_batch_steps=1,
            )
            manifest["resume_checkpoint"] = "checkpoints/epoch-1.safetensors"
            (adapter_dir / "training_manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "retained resume-checkpoint path"):
                validate_release_artifact_provenance(model_dir, train_path, adapter_dir)
            manifest["resume_checkpoint"] = resume_relative_path
            resume_checkpoint.write_bytes(b"corrupt")
            (adapter_dir / "training_manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "resume-checkpoint fingerprint"):
                validate_release_artifact_provenance(model_dir, train_path, adapter_dir)
            resume_checkpoint.write_bytes(resume_bytes)
            manifest["resume_checkpoint_sha256"] = "not-a-digest"
            (adapter_dir / "training_manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "incomplete resume provenance"):
                validate_release_artifact_provenance(model_dir, train_path, adapter_dir)
            for key in (
                "resume_checkpoint_used",
                "resume_checkpoint",
                "resume_checkpoint_sha256",
                "resume_restored_micro_batch_steps",
                "resume_restored_optimizer_steps",
                "resume_restored_epochs",
            ):
                manifest.pop(key)

            manifest["max_examples"] = False
            (adapter_dir / "training_manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "complete dataset"):
                validate_release_artifact_provenance(model_dir, train_path, adapter_dir)
            manifest["max_examples"] = 0

            manifest["max_steps"] = manifest["requested_max_steps"] = False
            (adapter_dir / "training_manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "uncapped training"):
                validate_release_artifact_provenance(model_dir, train_path, adapter_dir)
            manifest["max_steps"] = manifest["requested_max_steps"] = 0

            manifest["initial_adapter_checkpoint_used"] = True
            manifest["initial_adapter_checkpoint"] = "/untracked/ancestor.safetensors"
            manifest["initial_adapter_checkpoint_sha256"] = "sha256:" + "0" * 64
            (adapter_dir / "training_manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "clean base-model initialization"):
                validate_release_artifact_provenance(model_dir, train_path, adapter_dir)
            manifest["initial_adapter_checkpoint_used"] = False
            del manifest["initial_adapter_checkpoint"]
            del manifest["initial_adapter_checkpoint_sha256"]

            manifest["max_steps"] = manifest["requested_max_steps"] = 1
            (adapter_dir / "training_manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "uncapped training"):
                validate_release_artifact_provenance(model_dir, train_path, adapter_dir)
            manifest["max_steps"] = manifest["requested_max_steps"] = 0
            manifest["optimizer_steps"] = 0
            (adapter_dir / "training_manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "epoch or optimizer-step accounting"):
                validate_release_artifact_provenance(model_dir, train_path, adapter_dir)
            manifest["optimizer_steps"] = 1

            manifest["optimizer_steps"] = manifest["effective_steps"] = 2
            (adapter_dir / "training_manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "completed-run accounting"):
                validate_release_artifact_provenance(model_dir, train_path, adapter_dir)
            manifest["optimizer_steps"] = manifest["effective_steps"] = 1
            manifest["micro_batch_steps"] = 2
            (adapter_dir / "training_manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "completed-run accounting"):
                validate_release_artifact_provenance(model_dir, train_path, adapter_dir)
            manifest["micro_batch_steps"] = 1
            manifest["requested_epochs"] = 2
            (adapter_dir / "training_manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "completion or a valid early stop"):
                validate_release_artifact_provenance(model_dir, train_path, adapter_dir)
            manifest["requested_epochs"] = 1

            manifest.update(
                epochs=2,
                requested_epochs=5,
                optimizer_steps=2,
                effective_steps=2,
                micro_batch_steps=2,
                total_steps=2,
                eval_count=2,
                early_stopped=True,
                early_stopping_patience=1,
                early_stopping_bad_epochs=1,
            )
            write_metal_steps(2)
            (adapter_dir / "training_manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
            validate_release_artifact_provenance(model_dir, train_path, adapter_dir)
            manifest["early_stopping_bad_epochs"] = 0
            (adapter_dir / "training_manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "early-stopping provenance"):
                validate_release_artifact_provenance(model_dir, train_path, adapter_dir)
            manifest["early_stopping_bad_epochs"] = 2
            (adapter_dir / "training_manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "early-stopping provenance"):
                validate_release_artifact_provenance(model_dir, train_path, adapter_dir)
            manifest.update(
                epochs=1,
                requested_epochs=1,
                optimizer_steps=1,
                effective_steps=1,
                micro_batch_steps=1,
                total_steps=1,
                eval_count=1,
                early_stopped=False,
                early_stopping_patience=0,
                early_stopping_bad_epochs=0,
            )
            write_metal_steps(1)

            manifest.update(
                example_count=101,
                examples_per_epoch=96,
                batch_size=32,
                effective_batch_size=32,
                steps_per_epoch=3,
                optimizer_steps=3,
                effective_steps=3,
                micro_batch_steps=3,
                total_steps=3,
            )
            (adapter_dir / "training_manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "does not consume every training record"):
                validate_release_artifact_provenance(model_dir, train_path, adapter_dir, 101)

            manifest.update(
                example_count=3,
                examples_per_epoch=3,
                batch_size=1,
                effective_batch_size=1,
                steps_per_epoch=3,
                grad_accum=2,
                optimizer_steps=2,
                effective_steps=2,
                micro_batch_steps=3,
                total_steps=3,
            )
            write_metal_steps(3)
            (adapter_dir / "training_manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
            validate_release_artifact_provenance(model_dir, train_path, adapter_dir, 3)

            manifest.update(
                example_count=1,
                examples_per_epoch=1,
                batch_size=1,
                effective_batch_size=1,
                steps_per_epoch=1,
                grad_accum=2,
                optimizer_steps=1,
                effective_steps=1,
                micro_batch_steps=1,
                total_steps=1,
            )
            write_metal_steps(1)
            (adapter_dir / "training_manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
            validate_release_artifact_provenance(model_dir, train_path, adapter_dir)
            manifest["grad_accum"] = 1

            manifest["effective_batch_size"] = 2
            (adapter_dir / "training_manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "inconsistent batch accounting"):
                validate_release_artifact_provenance(model_dir, train_path, adapter_dir)
            manifest["effective_batch_size"] = 1
            (adapter_dir / "training_manifest.json").write_text(json.dumps(manifest), encoding="utf-8")

            adapter_config["lora_alpha"] = 64
            (adapter_dir / "adapter_config.json").write_text(json.dumps(adapter_config), encoding="utf-8")
            manifest["adapter_bundle_fingerprint_sha256"] = adapter_bundle_fingerprint(adapter_dir)
            (adapter_dir / "training_manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "LoRA metadata"):
                validate_release_artifact_provenance(model_dir, train_path, adapter_dir)
            adapter_config["lora_alpha"] = 32
            (adapter_dir / "adapter_config.json").write_text(json.dumps(adapter_config), encoding="utf-8")
            manifest["adapter_bundle_fingerprint_sha256"] = adapter_bundle_fingerprint(adapter_dir)
            (adapter_dir / "training_manifest.json").write_text(json.dumps(manifest), encoding="utf-8")

            missing_model_file = model_dir / "tokenizer_config.json"
            missing_model_file.unlink()
            with self.assertRaisesRegex(ValueError, "required fingerprint file is missing"):
                validate_release_artifact_provenance(model_dir, train_path, adapter_dir)
            missing_model_file.write_bytes(b"tokenizer_config.json")

            missing_adapter_file = adapter_dir / "task_head.safetensors"
            missing_adapter_file.unlink()
            with self.assertRaisesRegex(ValueError, "required fingerprint file is missing"):
                validate_release_artifact_provenance(model_dir, train_path, adapter_dir)
            missing_adapter_file.write_bytes(b"compatibility head")

            train_path.write_text('{"input":"different row"}\n', encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "training-data fingerprint"):
                validate_release_artifact_provenance(model_dir, train_path, adapter_dir)
            train_path.write_text('{"input":"training row"}\n', encoding="utf-8")
            (adapter_dir / "adapter_model.safetensors").write_bytes(b"swapped adapter")
            with self.assertRaisesRegex(ValueError, "bundle fingerprint"):
                validate_release_artifact_provenance(model_dir, train_path, adapter_dir)


if __name__ == "__main__":
    unittest.main()
