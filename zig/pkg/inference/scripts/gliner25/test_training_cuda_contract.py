"""Contract tests independent of a CUDA device or downloaded model."""
import copy
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch
from contextlib import nullcontext
from dataclasses import dataclass

import numpy as np
import benchmark_training_cuda as bench
from training_cuda_worker import adapt_row, disable_query_sampling, cublas_version
import training_cuda_quality as quality


class TrainingContractTests(unittest.TestCase):
    def test_reference_cublas_identity_checks_vendor_status_and_version(self):
        import ctypes
        torch = SimpleNamespace(version=SimpleNamespace(cuda="12.8"),
                                cuda=SimpleNamespace(current_blas_handle=lambda: 12345))
        for status, value in ((0, 120800), (1, 120800), (0, 0)):
            def query(handle, output):
                self.assertEqual(handle.value, 12345)
                ctypes.cast(output, ctypes.POINTER(ctypes.c_int))[0] = value
                return status
            with patch.object(ctypes, "CDLL", return_value=SimpleNamespace(cublasGetVersion_v2=query)) as loader:
                if status == 0 and value > 0:
                    self.assertEqual(cublas_version(torch), value)
                else:
                    with self.assertRaisesRegex(ValueError, "cuBLAS runtime identity"):
                        cublas_version(torch)
                loader.assert_called_once_with("libcublas.so.12")

    def test_snapshot_reports_signed_zero_bits_without_changing_numeric_acceptance(self):
        with tempfile.TemporaryDirectory() as directory:
            left = self.snapshot(directory, "left", [0.0, 1, 0, 0, 0, 0, 0, 0])
            right = self.snapshot(directory, "right", [-0.0, 1, 0, 0, 0, 0, 0, 0])
            report = bench.compare_snapshots(left, right)
            self.assertTrue(report["passed"])
            weight = report["tensors"]["classifier.weight"]["weight"]
            self.assertEqual(weight["max_absolute_error"], 0)
            self.assertFalse(weight["bitwise_equal"])
            self.assertEqual(weight["bitwise_mismatches"], 1)
            self.assertTrue(report["tensors"]["classifier.weight"]["m"]["bitwise_equal"])

    def test_compiled_snapshot_names_follow_parameter_objects_and_reject_inventory_changes(self):
        left, right = object(), object()
        inventory = (("encoder.weight", left), ("encoder._orig_mod.weight", right))
        compiled = SimpleNamespace(named_parameters=lambda: [
            ("encoder._orig_mod.weight", left),
            ("encoder._orig_mod._orig_mod.weight", right),
        ])
        self.assertEqual(quality.canonical_parameters(compiled, inventory), inventory)
        for changed in (
            [("encoder.weight", object()), ("encoder.other", right)],
            [("encoder.other", right), ("encoder.weight", left)],
            [("encoder.weight", left)],
            [("encoder.weight", left), ("encoder.other", right), ("extra", object())],
        ):
            compiled.named_parameters = lambda: changed
            with self.assertRaisesRegex(ValueError, "identity or registration order"):
                quality.canonical_parameters(compiled, inventory)
        plain = SimpleNamespace(named_parameters=lambda: inventory)
        with self.assertRaisesRegex(ValueError, "identity or registration order"):
            quality.canonical_parameters(plain, (("same", left), ("same", right)))

    def test_negative_query_sampling_is_disabled_without_changing_other_settings(self):
        @dataclass(frozen=True)
        class Settings:
            negative_query_ratio: float = 1.0
            max_negative_queries_per_batch: int = 64
        original = Settings()
        model = SimpleNamespace(boundary_head=SimpleNamespace(settings=original), boundary_settings=original)
        disable_query_sampling(model)
        self.assertEqual(original.negative_query_ratio, 1.0)
        self.assertEqual(model.boundary_head.settings.negative_query_ratio, 0.0)
        self.assertEqual(model.boundary_head.settings.max_negative_queries_per_batch, 64)
        self.assertIs(model.boundary_settings, model.boundary_head.settings)

    def test_qualification_rejects_normalized_train_validation_overlap(self):
        left = [{"id": "train", "text": "Alice  works at Acme."}]
        right = [{"id": "test", "text": " ALICE works at ACME. "}]
        with self.assertRaisesRegex(ValueError, "overlap"):
            quality.require_disjoint(left, right)
        right[0]["text"] = "Bob works at Beta."
        quality.require_disjoint(left, right)
        right[0]["id"] = "train"
        with self.assertRaisesRegex(ValueError, "overlap"):
            quality.require_disjoint(left, right)

    def test_adapter_rejects_negative_and_out_of_range_offsets(self):
        original = json.loads((bench.oracle.FIXTURES / "training_job_small_v1" / "train.jsonl").read_text().splitlines()[0])
        for start, end in ((-1, 3), (0, 100000), (3, 3), (False, 3)):
            row = copy.deepcopy(original)
            row["entities"][0]["span"].update(start=start, end=end)
            with self.assertRaisesRegex(ValueError, "surface offsets"):
                adapt_row(row)

    def test_qualification_bounds_reject_before_launch(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binary = root / "worker"
            binary.touch()
            for updates, validation_only in ((-1, False), (513, False), (1, True)):
                args = SimpleNamespace(output=root / "report", native_bin=binary,
                                       batch_size=2, pairs=2, warmup=0, adam_epsilon=1e-8,
                                       qualification_updates=updates, validate_only=validation_only)
                with self.assertRaisesRegex(bench.common.BenchmarkError, "qualification update"):
                    bench.run(args)
            self.assertFalse((root / "report").exists())

    def test_quality_adapter_preserves_record_schema_and_all_gold_tasks(self):
        row = json.loads((bench.oracle.FIXTURES / "training_job_small_v1" / "validation.jsonl").read_text().splitlines()[0])
        request = quality.evaluation_request(row)
        self.assertEqual(bench.common.adaptation.schema_for(request)["structures"],
                         {name: {**spec, "fields": {key: {("type" if k == "dtype" else k): v for k, v in field.items()} for key, field in spec["fields"].items()}}
                          for name, spec in row["schema"]["structures"].items()})
        facts = quality.gold_facts(row)
        self.assertEqual({name: sum(values.values()) for name, values in facts.items()},
                         {"entities": 2, "classifications": 1, "records": 1, "relations": 1})

    def test_encoder_work_limit_rejects_invalid_values_before_loading_or_launching(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binary = root / "worker"
            binary.touch()
            for limit in (0, -1, True, 1.5, "8589934592", 1024 * 1024**3 + 1):
                args = SimpleNamespace(output=root / "report", native_bin=binary,
                                       batch_size=8, pairs=2, warmup=0, adam_epsilon=1e-8,
                                       qualification_updates=0, validate_only=True,
                                       encoder_forward_bytes=limit)
                with patch.object(bench.oracle, "verify_dependencies") as dependencies:
                    with self.assertRaisesRegex(bench.common.BenchmarkError, "logical encoder"):
                        bench.run(args)
                    dependencies.assert_not_called()
            self.assertFalse((root / "report").exists())

    def test_cublas_selection_rejects_missing_or_relative_library_before_launch(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binary = root / "worker"
            binary.touch()
            for library in (Path("libcublas.so.12"), root / "missing.so"):
                args = SimpleNamespace(output=root / "report", native_bin=binary,
                                       batch_size=8, pairs=2, warmup=0, adam_epsilon=1e-8,
                                       qualification_updates=0, validate_only=True,
                                       cublas_library=library)
                with patch.object(bench.oracle, "verify_dependencies") as dependencies:
                    with self.assertRaisesRegex(bench.common.BenchmarkError, "existing absolute"):
                        bench.run(args)
                    dependencies.assert_not_called()
            self.assertFalse((root / "report").exists())

    def test_evaluation_snapshot_validation_precedes_weight_mutation(self):
        class Parameter:
            requires_grad = True
            shape = (2,)
            def __init__(self):
                self.values = np.array([9, 9], dtype=np.float32)
            def numel(self):
                return 2
            def copy_(self, values):
                self.values[:] = values
        parameter = Parameter()
        model = SimpleNamespace(named_parameters=lambda: [("classifier.weight", parameter)])
        torch = SimpleNamespace(no_grad=nullcontext, from_numpy=lambda value: value)
        with tempfile.TemporaryDirectory() as directory:
            receipt = self.snapshot(directory, "native", [1, 2, 0, 0, 0, 0, 0, 0])
            for key, value in (("offset", 4), ("elements", 3), ("shape", [1, 2])):
                invalid = copy.deepcopy(receipt)
                invalid["slots"][0][key] = value
                with self.assertRaisesRegex(ValueError, "snapshot layout"):
                    quality.load_weights(model, invalid, torch)
                np.testing.assert_array_equal(parameter.values, [9, 9])
            inventory = quality.canonical_parameters(model)
            model.named_parameters = lambda: [("classifier._orig_mod.weight", parameter)]
            quality.load_weights(model, receipt, torch, inventory)
            np.testing.assert_array_equal(parameter.values, [1, 2])

    def test_quality_evaluation_restores_model_mode_on_failure(self):
        modes = []
        model = SimpleNamespace(training=True, eval=lambda: modes.append(False), train=lambda value: modes.append(value))
        row = json.loads((bench.oracle.FIXTURES / "training_job_small_v1" / "validation.jsonl").read_text().splitlines()[0])
        with patch.object(quality.common, "execute_python", side_effect=RuntimeError("device failed")):
            with self.assertRaisesRegex(RuntimeError, "device failed"):
                quality.evaluate(model, [row], SimpleNamespace(inference_mode=nullcontext))
        self.assertEqual(modes, [False, True])

    def test_invalid_adam_epsilon_is_rejected_before_loading_or_launch(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binary = root / "worker"
            binary.touch()
            for epsilon in (0, -1, float("nan"), float("inf"), 1.01):
                args = SimpleNamespace(output=root / "report", native_bin=binary,
                                       batch_size=2, pairs=2, warmup=0, adam_epsilon=epsilon)
                with self.assertRaisesRegex(bench.common.BenchmarkError, "Adam epsilon"):
                    bench.run(args)
            self.assertFalse((root / "report").exists())

    def test_adapter_preserves_all_fixture_tasks_and_absent_labels(self):
        path = bench.oracle.FIXTURES / "training_job_small_v1" / "train.jsonl"
        original = json.loads(path.read_text().splitlines()[0])
        before = copy.deepcopy(original)
        text, schema = adapt_row(original)
        self.assertEqual(original, before)
        self.assertEqual(text, original["text"])
        self.assertEqual(schema["entities"]["location"], [])
        self.assertEqual(schema["classifications"][0]["true_label"], ["accepted"])
        self.assertEqual(schema["relations"], [{"works_for": {"head": "Ada", "tail": "Acme"}}])
        self.assertEqual(schema["json_structures"], [{"employment": {"employee": "Ada", "employer": "Acme"}}])
        self.assertEqual(schema["record_metadata"]["employment"]["fields"]["employer"]["cardinality"], "required_one")

    def test_adapter_rejects_ambiguous_surface_conversion(self):
        row = json.loads((bench.oracle.FIXTURES / "training_job_small_v1" / "train.jsonl").read_text().splitlines()[0])
        row["text"] += " Ada"
        with self.assertRaisesRegex(ValueError, "ambiguous"):
            adapt_row(row)

    def snapshot(self, directory, arm, values, **changes):
        path = Path(directory) / (arm + ".bin")
        np.asarray(values, dtype="<f4").tofile(path)
        slot = {"canonical_name": "classifier.weight", "shape": [2], "elements": 2,
                "offset": 0, "present": True, "adam_step": 1, "group": 1, **changes}
        return {"snapshot": str(path), "size_bytes": path.stat().st_size, "slots": [slot],
                "identity": {"optimizer_step": 1, "microbatch_step": 2}, "accumulated_microbatches": 0}

    def test_snapshot_checks_gradients_and_moments_even_when_weights_match(self):
        with tempfile.TemporaryDirectory() as directory:
            values = [1, 2, 0.1, 0.2, 0.01, 0.02, 0.001, 0.002]
            left = self.snapshot(directory, "native", values)
            right = self.snapshot(directory, "python", values)
            self.assertTrue(bench.compare_snapshots(left, right)["passed"])
            for index, field in ((2, "gradient"), (4, "m"), (6, "v")):
                changed = list(values); changed[index] += 0.1
                right = self.snapshot(directory, "python", changed)
                result = bench.compare_snapshots(left, right)
                self.assertFalse(result["passed"])
                self.assertTrue(any("." + field + ":" in item for item in result["failures"]))

    def test_missing_and_zero_gradients_are_different(self):
        with tempfile.TemporaryDirectory() as directory:
            left = self.snapshot(directory, "native", [0] * 8)
            right = self.snapshot(directory, "python", [0] * 8, present=False)
            self.assertFalse(bench.compare_snapshots(left, right)["passed"])

    def test_snapshot_reports_worst_coordinate_across_chunks(self):
        with tempfile.TemporaryDirectory() as directory:
            n = 262145
            values = np.zeros(4 * n, dtype="<f4")
            left = self.snapshot(directory, "native", values, shape=[n], elements=n)
            values[0] = 0.25
            values[n - 1] = -0.5
            right = self.snapshot(directory, "python", values, shape=[n], elements=n)
            result = bench.compare_snapshots(left, right)
            weight = result["tensors"]["classifier.weight"]["weight"]
            self.assertFalse(weight["passed"])
            self.assertEqual(weight["worst_element"],
                             {"flat_index": n - 1, "native": 0.0, "python": -0.5})
            self.assertEqual(weight["state_at_worst"]["python"],
                             {"weight": -0.5, "gradient": 0.0, "m": 0.0, "v": 0.0})

    def test_nonfinite_and_truncated_snapshots_are_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            left = self.snapshot(directory, "native", [0] * 8)
            right = self.snapshot(directory, "python", [float("nan")] + [0] * 7)
            with self.assertRaisesRegex(bench.common.BenchmarkError, "non-finite"):
                bench.compare_snapshots(left, right)
            right["size_bytes"] += 4
            with self.assertRaisesRegex(bench.common.BenchmarkError, "snapshot size"):
                bench.compare_snapshots(left, right)

    def test_dropped_auxiliary_loss_fails_even_if_total_is_close(self):
        native = {"report": {"zero_loss_fallback": False, "examples": 2, "terms": {"total": 1, "relation": 0},
                             "optimizer": {"optimizer_stepped": True, "identity": {"optimizer_step": 1, "microbatch_step": 2}}},
                  "cuda_transfers": {"kernel_launches": 10, "host_fallback_calls": 0}}
        python = {"examples": 2, "optimizer_stepped": True, "optimizer_step": 1, "microbatch_step": 2, "terms": {"total": 1}}
        with self.assertRaisesRegex(bench.common.BenchmarkError, "components differ"):
            bench.compare_steps(native, python)


class LossTraceContract(unittest.TestCase):
    def test_module_trace_compares_retained_input_without_inventing_an_output(self):
        from training_cuda_trace import ModuleTrace

        class Linear:
            def __init__(self):
                self.hook = None

            def register_forward_hook(self, hook):
                self.hook = hook
                return SimpleNamespace(remove=lambda: setattr(self, "hook", None))

        module = Linear()
        model = SimpleNamespace(named_modules=lambda: [("projection", module)])
        torch = SimpleNamespace(nn=SimpleNamespace(Linear=Linear, LayerNorm=type("LayerNorm", (), {})))
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "native.bin"
            values = np.asarray([1, 2, 3], dtype="<f4")
            path.write_bytes(values.tobytes())
            metadata = {"path": str(path), "size_bytes": 12, "tensors": [
                {"name": "projection", "kind": "input", "offset": 0, "elements": 3}]}
            observed = []

            def compare(native, python):
                np.testing.assert_array_equal(native, values)
                observed.append(python)
                return {"max_absolute_error": 0}

            with patch.object(ModuleTrace, "compare", staticmethod(compare)):
                trace = ModuleTrace(model, torch, metadata)
                with trace:
                    self.assertIsNotNone(module.hook)
                    sentinel = object()
                    module.hook(module, (sentinel,), object())
                self.assertIsNone(module.hook)
                self.assertEqual(observed, [sentinel])
                self.assertEqual(trace.results[0]["input"]["max_absolute_error"], 0)
                self.assertNotIn("output", trace.results[0])
                self.assertNotIn("same_input_output", trace.results[0])

    def test_trace_restores_original_method_after_failure(self):
        from types import SimpleNamespace
        from training_cuda_trace import BoundaryTrace
        class Head:
            settings = SimpleNamespace(negative_query_ratio=0)
            def _compute_losses(self):
                return "original"
        head = Head()
        with self.assertRaisesRegex(RuntimeError, "diagnostic failure"):
            with BoundaryTrace(SimpleNamespace(boundary_head=head), None):
                raise RuntimeError("diagnostic failure")
        self.assertNotIn("_compute_losses", head.__dict__)
        self.assertEqual(head._compute_losses(), "original")

    def test_trace_rejects_oversized_native_payload(self):
        from types import SimpleNamespace
        from training_cuda_trace import BoundaryTrace
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "trace.json"
            path.write_bytes(b" " * (2 * 1024**2 + 1))
            with self.assertRaisesRegex(ValueError, "exceeds limit"):
                BoundaryTrace(SimpleNamespace(boundary_head=None), None, path)


if __name__ == "__main__":
    unittest.main()
