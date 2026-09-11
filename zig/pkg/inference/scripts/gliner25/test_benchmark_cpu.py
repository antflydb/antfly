from __future__ import annotations

import copy
import json
import os
from pathlib import Path
import sys
import tempfile
import types
import unittest
from unittest import mock

import benchmark_cpu as bench
import oracle


class BenchmarkContractTest(unittest.TestCase):
    def test_encoder_capture_covers_direct_classifier_and_boundary_core_routes_and_restores_hooks(self):
        class Tensor:
            def __init__(self, values):
                self.values = values
            def detach(self):
                return self
            def cpu(self):
                return self
            def tolist(self):
                return self.values

        class Encoder:
            def __init__(self):
                self.hooks = []
            def register_forward_pre_hook(self, hook, *, with_kwargs):
                self.assert_with_kwargs = with_kwargs
                self.hooks.append(hook)
                return types.SimpleNamespace(remove=lambda: self.hooks.remove(hook))
            def __call__(self, *args, **kwargs):
                for hook in self.hooks:
                    hook(self, args, kwargs)
                return "encoded"

        encoder = Encoder()
        model = types.SimpleNamespace(encoder=encoder)
        model._encode_core = lambda batch: encoder(input_ids=batch.input_ids)
        batch = types.SimpleNamespace(input_ids=Tensor([[1, 2, 3]]))
        for execute in (lambda: model._encode_core(batch), lambda: encoder(input_ids=batch.input_ids), lambda: encoder(batch.input_ids)):
            with bench.capture_encoder_input_ids(model) as captured:
                self.assertEqual("encoded", execute())
            self.assertEqual([[1, 2, 3]], captured)
            self.assertEqual([], encoder.hooks)
        with self.assertRaisesRegex(RuntimeError, "execution failed"):
            with bench.capture_encoder_input_ids(model):
                raise RuntimeError("execution failed")
        self.assertEqual([], encoder.hooks)
        for invalid in ([], [[]], [[1], [2]], [[True]], [[1.0]], [[1] * (oracle.MAX_ENCODED_TOKENS + 1)]):
            with self.subTest(invalid=invalid), self.assertRaises(bench.BenchmarkError):
                with bench.capture_encoder_input_ids(model):
                    encoder(input_ids=Tensor(invalid))
            self.assertEqual([], encoder.hooks)

    def test_live_python_adapter_matches_all_thirty_captured_task_outputs(self):
        requests = {row["id"]: row for row in oracle.read_json(oracle.FIXTURES / "requests.json")["requests"]}
        for variant in ("small", "base", "multi"):
            captured = oracle.read_json(oracle.FIXTURES / f"{variant}_reference/capture.json")
            expected = {row["id"]: row["expected"] for row in oracle.read_json(bench.case_fixture(variant))["cases"]}
            for row in captured["requests"]:
                with self.subTest(model=variant, case=row["id"]):
                    bench.require_equal(bench.canonical_result(expected[row["id"]]), bench.canonical_python(requests[row["id"]], row["output"]))

    def test_only_confidence_is_approximate(self):
        bench.require_equal({"confidence": 0.5}, {"confidence": 0.5004})
        for expected, actual in (({"confidence": 0.5}, {"confidence": 0.501}),
                                 ({"confidence": 0.5}, {"confidence": float("nan")}),
                                 ({"start": 1}, {"start": 1.00001}),
                                 ({"start": 1}, {"start": True}),
                                 ({"text": "İ"}, {"text": "I"}),
                                 ([1, 2], [2, 1]), ([1], [1, 2]),
                                 ({"head_entity_type": 0}, {"head_entity_type": 1})):
            with self.subTest(expected=expected, actual=actual), self.assertRaises(bench.BenchmarkError):
                bench.require_equal(expected, actual)

    def test_protocol_rejects_duplicates_and_nonfinite_numbers(self):
        for value in ('{"id":1,"id":2}', '{"duration_ns":NaN}'):
            with self.assertRaises((bench.BenchmarkError, oracle.ContractError)):
                bench.strict_json(value)

    def test_native_readiness_rejects_debug_mixed_model_and_thread_drift(self):
        model = oracle.load_manifest()["models"]["small"]
        bundle = {**model, "files": {name: {key: item[key] for key in ("size_bytes", "sha256")} for name, item in model["files"].items()}}
        ready = {"event": "ready", "arm": "native", "scope": bench.SCOPE, "timing_boundary": bench.TIMING_BOUNDARY,
                 "model_id": bundle["model_id"], "revision": bundle["revision"], "model_files": bundle["files"],
                 "dtype": "float32", "threads": 1, "qualification": False, "build_mode": "ReleaseFast", "scheduler": "serial_io",
                 "cases_sha256": oracle.sha256_file(bench.case_fixture("small"))}
        bench.checked_ready("native", ready, bundle, bench.case_fixture("small"), 1)
        for key, value in (("build_mode", "Debug"), ("threads", 2), ("revision", "wrong"), ("cases_sha256", "0" * 64), ("timing_boundary", "encoder_only"), ("qualification", True)):
            with self.subTest(key=key), self.assertRaises(bench.BenchmarkError):
                bench.checked_ready("native", {**ready, key: value}, bundle, bench.case_fixture("small"), 1)

    def test_pairing_is_balanced_and_validation_precedes_warmup_and_measurement(self):
        model = oracle.load_manifest()["models"]["small"]
        bundle = {"model_id": model["model_id"], "revision": model["revision"], "files": {name: {key: item[key] for key in ("size_bytes", "sha256")} for name, item in model["files"].items()}}
        case_path = bench.case_fixture("small")
        fixture = oracle.read_json(case_path)
        case = fixture["cases"][0]
        expected = bench.canonical_result(case["expected"])
        events = []

        class Guard:
            def __init__(self, limit):
                self.max_rss_bytes = limit
                self.peak_rss_bytes = 4096

        class Worker:
            def __init__(self, arm, command, env, directory, guard):
                self.arm = arm
                self.process = types.SimpleNamespace(wait=lambda **kwargs: 0)
                events.append((arm, "start"))
                for name in bench.THREAD_ENV:
                    if env[name] != "1":
                        raise AssertionError("thread budget drift")

            def receive(self, timeout):
                return {"event": "ready", "arm": self.arm, "scope": bench.SCOPE, "timing_boundary": bench.TIMING_BOUNDARY,
                        "model_id": bundle["model_id"], "revision": bundle["revision"], "model_files": bundle["files"],
                        "dtype": "float32", "threads": 1, "interop_threads": 1, "qualification": False,
                        "build_mode": "ReleaseFast", "scheduler": "serial_io", "cases_sha256": oracle.sha256_file(case_path)}

            def request(self, op, case_id="", timeout=60):
                events.append((self.arm, op))
                return {"output": copy.deepcopy(expected), "input_ids": [1, 2, 3], "duration_ns": 100 if self.arm == "native" else 200}

            def close(self):
                events.append((self.arm, "close"))

        with tempfile.TemporaryDirectory() as temporary:
            args = types.SimpleNamespace(model_root=Path(temporary), cases=[case["id"]], warmup=1, pairs=2, threads=1,
                                         native_bin=Path("/unused/native"), timeout_ms=1000, upstream=Path("/unused/source"), max_rss_mib=512, startup_timeout=10)
            with mock.patch.object(bench, "Worker", Worker), mock.patch.object(bench, "ResourceGuard", Guard), \
                 mock.patch.object(oracle, "verify_model_dir", return_value=bundle), mock.patch.object(oracle, "verify_upstream_checkout"):
                result = bench.run_variant(args, "small", Path(temporary) / "output")
        self.assertEqual([("python", "start"), ("native", "start"), ("python", "validate"), ("native", "validate")], events[:4])
        self.assertEqual([("native", "run"), ("python", "run"), ("native", "run"), ("python", "run"), ("python", "run"), ("native", "run")], events[4:10])
        self.assertEqual(0.5, result["comparisons"][case["id"]]["native_over_python_latency"]["median"])
        self.assertEqual([("python", "close"), ("native", "close")], events[-2:])

    def test_worker_protocol_identity_and_timeout_cleanup(self):
        class Guard:
            def __init__(self):
                self.workers = []
            def check(self):
                pass

        script = "import json,sys; print(json.dumps({'event':'ready'}),flush=True); command=json.loads(sys.stdin.readline()); print(json.dumps({'event':'result','request_id':999}),flush=True)"
        with tempfile.TemporaryDirectory() as temporary:
            worker = bench.Worker("native", [sys.executable, "-u", "-c", script], os.environ.copy(), Path(temporary), Guard())
            try:
                self.assertEqual("ready", worker.receive(2)["event"])
                with self.assertRaisesRegex(bench.BenchmarkError, "identity"):
                    worker.request("run", "case", 2)
            finally:
                worker.close()
            self.assertIsNotNone(worker.process.poll())
            worker = bench.Worker("native", [sys.executable, "-u", "-c", "import sys; sys.stdin.read()"], os.environ.copy(), Path(temporary), Guard())
            try:
                with self.assertRaisesRegex(bench.BenchmarkError, "deadline"):
                    worker.receive(0.02)
            finally:
                worker.close()
            self.assertIsNotNone(worker.process.poll())


if __name__ == "__main__":
    unittest.main()
