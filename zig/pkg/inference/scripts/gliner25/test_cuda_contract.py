"""CPU-only checks for the CUDA benchmark's timing and provenance contract."""
import contextlib
from types import SimpleNamespace
from pathlib import Path
import unittest
from unittest.mock import patch

import benchmark_cuda as bench
import metal_python_worker as worker
import oracle


class CudaContractTests(unittest.TestCase):
    def test_batch_executor_uses_one_upstream_batch(self):
        calls = []
        model = SimpleNamespace(batch_extract=lambda *a, **kw: calls.append((a, kw)) or [{}, {}])
        with patch.object(oracle, "build_extract_schema", return_value="compiled"):
            result = worker.execute_batch(model, {"kind": "extract", "text": "Alice"}, "{}", 2)
        self.assertEqual(len(result), 2)
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0][0], (["Alice", "Alice"], "compiled"))
        self.assertEqual(calls[0][1]["batch_size"], 2)
        self.assertEqual(calls[0][1]["num_workers"], 0)
        with self.assertRaises(worker.WorkerError):
            worker.execute_batch(model, {"kind": "classification"}, "{}", 2)

    def test_batch_token_check_requires_actual_full_batch_shape(self):
        receipt = {"input_ids": [1, 2, 1, 2], "encoder_shape": [2, 2]}
        responses = {"native": receipt, "python": dict(receipt)}
        self.assertEqual(bench.checked_batch_tokens(responses, 2), ([1, 2, 1, 2], [2, 2]))
        responses["python"]["encoder_shape"] = [1, 4]
        with self.assertRaisesRegex(bench.BenchmarkError, "batch shape"):
            bench.checked_batch_tokens(responses, 2)
        responses["python"] = {**receipt, "input_ids": [1, 2, 1, 3]}
        with self.assertRaisesRegex(bench.BenchmarkError, "token identity"):
            bench.checked_batch_tokens(responses, 2)

    def test_batch_output_checks_every_document_and_count(self):
        with patch.object(bench.cpu, "canonical_result", side_effect=lambda x: x):
            response = {"batch_size": 2, "output": {}, "outputs": [{"label": "a"}, {"label": "b"}]}
            actual = bench.checked_output(response, 2)
            with self.assertRaises(bench.BenchmarkError):
                bench.require_equal([{"label": "a"}] * 2, actual)
            response["outputs"].pop()
            with self.assertRaisesRegex(bench.BenchmarkError, "output count"):
                bench.checked_output(response, 2)

    def test_timing_fences_surround_the_measured_extraction(self):
        events = []
        torch = SimpleNamespace(
            cuda=SimpleNamespace(synchronize=lambda: events.append("sync")),
            inference_mode=contextlib.nullcontext,
        )
        clocks = iter([100, 140])
        def clock():
            events.append("clock")
            return next(clocks)
        output, duration = worker.timed_call(
            torch, "cuda", lambda: events.append("extract") or "result", clock=clock
        )
        self.assertEqual((output, duration), ("result", 40))
        self.assertEqual(events, ["sync", "clock", "extract", "sync", "clock"])

    def test_failed_cuda_fence_never_becomes_a_sample(self):
        def fail():
            raise RuntimeError("lost device")
        torch = SimpleNamespace(cuda=SimpleNamespace(synchronize=fail), inference_mode=contextlib.nullcontext)
        with self.assertRaises(worker.DeviceSynchronizationError):
            worker.timed_call(torch, "cuda", lambda: self.fail("extraction ran"))

    def test_device_index_is_explicit(self):
        self.assertEqual(worker.normalized_device("cuda:0"), "cuda")
        with self.assertRaises(worker.WorkerError):
            worker.normalized_device("cuda:1")

    def test_frozen_reference_hashes_still_match(self):
        oracle.verify_config_fixtures()
        oracle.verify_reference_fixtures()

    def test_mixed_precision_relaxes_only_confidence(self):
        with self.assertRaises(bench.BenchmarkError):
            bench.require_equal({"confidence": 0.9}, {"confidence": 0.902})
        bench.require_equal({"confidence": 0.9}, {"confidence": 0.902}, confidence_tolerance=5e-3)
        with self.assertRaises(bench.BenchmarkError):
            bench.require_equal({"label": "person", "confidence": 0.9},
                                {"label": "company", "confidence": 0.902},
                                confidence_tolerance=5e-3)

    def test_cpu_readiness_cannot_pass_as_cuda(self):
        bundle = {"model_id": "model", "revision": "commit", "files": {}}
        ready = {"event": "ready", "arm": "native", "scope": bench.CUDA_SCOPE,
                 "timing_boundary": bench.TIMING_BOUNDARY, "model_id": "model",
                 "revision": "commit", "model_files": {}, "dtype": "float32",
                 "threads": 1, "qualification": False, "build_mode": "ReleaseFast",
                 "scheduler": "serial_io", "cases_sha256": "hash", "backend": "native"}
        with patch.object(oracle, "sha256_file", return_value="hash"):
            with self.assertRaises(bench.BenchmarkError):
                bench.checked_ready("native", ready, bundle, None, 1)

    def test_flash_candidate_cannot_silently_fall_back(self):
        class Model:
            encoder = SimpleNamespace()
            def float(self): return self
            def eval(self): return self
            def to(self, device): return self
        extractor = SimpleNamespace(from_pretrained=lambda *args, **kwargs: Model())
        with patch.object(worker.importlib.metadata, "version", return_value="0.0.7"):
            with self.assertRaisesRegex(worker.WorkerError, "silently fell back"):
                worker.load_model(extractor, Path("/unused"), "cuda", flashdeberta=True)

    def test_mixed_confidence_does_not_mutate_evidence_or_accept_invalid_values(self):
        actual = {"confidence": 0.902, "source": {"start": 2}}
        expected = {"confidence": 0.9, "source": {"start": 2}}
        bench.require_equal(expected, actual, confidence_tolerance=5e-3)
        self.assertEqual(actual["confidence"], 0.902)
        for value in (True, float("nan"), float("inf"), 0.906):
            with self.assertRaises(bench.BenchmarkError):
                bench.require_equal({"confidence": 0.9}, {"confidence": value}, confidence_tolerance=5e-3)
        with self.assertRaises(bench.BenchmarkError):
            bench.require_equal(expected, {"confidence": 0.9, "source": {"start": 2.0}}, confidence_tolerance=5e-3)

    def test_existing_trained_export_contracts_remain_valid(self):
        import check_training_merge
        import check_trained_execution
        check_training_merge.load_contract()
        check_trained_execution.load_contract()

    def test_strict_cuda_readiness_requires_triton_ieee_too(self):
        bundle = {"model_id": "model", "revision": "commit", "files": {}}
        ready = {"event": "ready", "arm": "fastino_cuda", "scope": bench.CUDA_SCOPE,
                 "timing_boundary": bench.TIMING_BOUNDARY, "model_id": "model", "revision": "commit",
                 "model_files": {}, "dtype": "float32", "threads": 1, "interop_threads": 1,
                 "qualification": False, "device": "cuda", "parameter_device": "cuda",
                 "profile": "eager_fp32", "encoder_backend": "transformers", "compile_static": False,
                 "math_policy": "pytorch_cuda_fp32_deterministic_no_tf32_v1",
                 "synchronization_policy": "torch_cuda_synchronize_before_start_and_after_extract_v1",
                 "cuda_runtime": {"triton_f32_default": "tf32"}}
        with self.assertRaises(bench.BenchmarkError):
            bench.checked_ready("python", ready, bundle, None, 1)
        ready["cuda_runtime"]["triton_f32_default"] = "ieee"
        bench.checked_ready("python", ready, bundle, None, 1)

    def test_native_sample_requires_gpu_work_without_host_fallback(self):
        valid = {"h2d_bytes": 8, "d2h_bytes": 4, "kernel_launches": 3, "host_fallback_calls": 0}
        self.assertEqual(bench.checked_transfers({"cuda_transfers": valid}), valid)
        for counts in (None, {}, {**valid, "kernel_launches": 0}, {**valid, "host_fallback_calls": 1},
                       {**valid, "d2h_bytes": -4}, {**valid, "h2d_bytes": True}):
            with self.assertRaises(bench.BenchmarkError):
                bench.checked_transfers({"cuda_transfers": counts})

    def test_candidate_failure_preserves_the_framework_error(self):
        with self.assertRaisesRegex(bench.BenchmarkError, "InternalTorchDynamoError: unsupported kernel"):
            bench.checked_output({"event": "error", "arm": "fastino_cuda",
                                  "error_type": "InternalTorchDynamoError", "message": "unsupported kernel"})
        with self.assertRaisesRegex(bench.BenchmarkError, "missing extraction output"):
            bench.checked_output({"event": "result"})

    def test_compiler_process_inventory_is_still_bounded(self):
        import metal_benchmark_supervisor as supervisor
        class MissingProcess(Exception): pass
        def process(pid):
            return SimpleNamespace(pid=pid, create_time=lambda: float(pid))
        fake = SimpleNamespace(Process=process, NoSuchProcess=MissingProcess)
        for limit in (supervisor.MAX_PROCESSES, 128):
            tree = supervisor._ProcessTree(SimpleNamespace(pid=1), fake, limit)
            for pid in range(2, limit + 1):
                tree._register(process(pid), "observed_descendant")
            # Re-observation does not spend an additional identity slot.
            tree._register(process(1), "observed_descendant")
            with self.assertRaisesRegex(supervisor.BenchmarkError, "identity ceiling"):
                tree._register(process(limit + 1), "observed_descendant")
        for invalid in (0, 4097, True):
            with self.assertRaises(supervisor.BenchmarkError):
                supervisor.ResourceGuard(max_processes=invalid)


if __name__ == "__main__":
    unittest.main()
