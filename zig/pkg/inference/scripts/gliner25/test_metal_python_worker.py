from __future__ import annotations

import contextlib
import copy
import io
import json
import os
from pathlib import Path
import sys
import tempfile
import types
import unittest
from unittest import mock
import warnings

import benchmark_cpu as bench
import metal_python_worker as worker
import oracle


class Tensor:
    def __init__(self, values=None, *, device="mps:0", dtype="float32", events=None):
        self.values = [[1, 2, 3]] if values is None else values
        self.device = device
        self.dtype = dtype
        self.events = events

    def is_floating_point(self):
        return self.dtype in ("float32", "float64", "float16")

    def is_complex(self):
        return self.dtype == "complex64"

    def numel(self):
        return 3

    def detach(self):
        return self

    def cpu(self):
        if self.events is not None:
            self.events.append("input_ids_readback")
        return self

    def tolist(self):
        return self.values


class Encoder:
    def __init__(self):
        self.hooks = []

    def register_forward_pre_hook(self, hook, *, with_kwargs):
        if with_kwargs is not True:
            raise AssertionError("keyword input observation is required")
        self.hooks.append(hook)
        return types.SimpleNamespace(remove=lambda: self.hooks.remove(hook))

    def __call__(self, *args, **kwargs):
        for hook in tuple(self.hooks):
            hook(self, args, kwargs)


class Model:
    def __init__(self, device="mps", events=None):
        self.encoder = Encoder()
        self.architecture = "boundary"
        self.training = False
        self.strict_extraction = True
        self.parameters = [("encoder.weight", Tensor(device=device, events=events))]
        self.buffers = [("positions", Tensor(device=device, dtype="int64")),
                        ("floating", Tensor(device=device))]
        self.load_actions = []

    def named_parameters(self):
        return iter(self.parameters)

    def named_buffers(self):
        return iter(self.buffers)

    def float(self):
        self.load_actions.append("float")
        return self

    def eval(self):
        self.load_actions.append("eval")
        self.training = False
        return self

    def to(self, device):
        self.load_actions.append(("to", device))
        for _, tensor in self.parameters + self.buffers:
            tensor.device = device
        return self


class Torch:
    float32 = "float32"

    def __init__(self, events=None):
        self.events = [] if events is None else events
        self.inference_active = False
        self.threads = self.interop = 1
        self.deterministic = True
        self.warn_only = False
        self.default_dtype = self.float32
        self.available = self.built = True
        self.backends = types.SimpleNamespace(mps=types.SimpleNamespace(
            is_built=lambda: self.built, is_available=lambda: self.available))
        self.mps = types.SimpleNamespace(
            synchronize=lambda: self.events.append("synchronize"),
            current_allocated_memory=lambda: self.memory("current", 32),
            driver_allocated_memory=lambda: self.memory("driver", 64),
            recommended_max_memory=lambda: self.memory("recommended", 128))

    def memory(self, name, value):
        self.events.append(f"memory_{name}")
        return value

    @contextlib.contextmanager
    def inference_mode(self):
        self.events.append("inference_enter")
        self.inference_active = True
        try:
            yield
        finally:
            self.inference_active = False
            self.events.append("inference_exit")

    def get_num_threads(self):
        return self.threads

    def get_num_interop_threads(self):
        return self.interop

    def get_default_dtype(self):
        return self.default_dtype

    def are_deterministic_algorithms_enabled(self):
        return self.deterministic

    def is_deterministic_algorithms_warn_only_enabled(self):
        return self.warn_only

    def is_tensor(self, value):
        return isinstance(value, Tensor)


class MetalPythonWorkerTest(unittest.TestCase):
    def setUp(self):
        explicit = {name: "0" for name in worker.MPS_ENV}
        explicit.update({name: "1" for name in bench.THREAD_ENV})
        self.environment = mock.patch.dict(os.environ, explicit)
        self.environment.start()
        self.addCleanup(self.environment.stop)
        self.args = types.SimpleNamespace(device="mps", model="small", model_dir=Path("/unused/model"),
                                          upstream=Path("/unused/source"), max_commands=100)
        self.bundle = {"model_id": "pinned/small", "revision": "abc", "files": {"model": {"sha256": "1" * 64}}}
        self.requests, self.requests_sha256 = worker.read_requests(oracle.FIXTURES / "requests.json")

    def test_module_and_help_remain_model_free_and_cli_selects_device_explicitly(self):
        self.assertNotIn("torch", sys.modules)
        args = worker.parse_args(["--device", "mps", "--model", "small", "--model-dir", "/model", "--upstream", "/source"])
        self.assertEqual("mps", args.device)
        self.assertEqual(2048, args.max_commands)
        with contextlib.redirect_stdout(io.StringIO()), self.assertRaises(SystemExit) as stop:
            worker.parse_args(["--help"])
        self.assertEqual(0, stop.exception.code)
        self.assertNotIn("torch", sys.modules)

    def test_import_time_flags_reject_late_torch_and_inherited_relaxations(self):
        env = {}
        worker.configure_environment(env, {})
        self.assertEqual({**{key: "0" for key in worker.MPS_ENV}, **{key: "1" for key in bench.THREAD_ENV}}, env)
        for flag in worker.MPS_ENV + bench.THREAD_ENV:
            with self.subTest(flag=flag), self.assertRaises(worker.WorkerError):
                worker.configure_environment({flag: "1" if flag in worker.MPS_ENV else "8"}, {})
        for modules in ({"torch": object()}, {"torch.mps": object()}):
            with self.assertRaisesRegex(worker.WorkerError, "before"):
                worker.configure_environment(env, modules)
        with mock.patch.dict(os.environ, {"PYTORCH_ENABLE_MPS_FALLBACK": "1"}), self.assertRaises(worker.WorkerError):
            worker.check_environment()

    def test_load_is_local_strict_fp32_eval_explicit_device_without_quantization_or_compile(self):
        for device in ("cpu", "mps"):
            model = Model("cpu")
            model.strict_extraction = False
            loader = mock.Mock(return_value=model)
            loaded = worker.load_model(types.SimpleNamespace(from_pretrained=loader), Path("/unused/model"), device)
            self.assertIs(loaded, model)
            loader.assert_called_once_with("/unused/model", local_files_only=True, map_location="cpu",
                                           quantize=False, compile=False, use_flashdeberta=False)
            self.assertEqual(["float", "eval", ("to", device)], model.load_actions)
            self.assertIs(True, model.strict_extraction)

    def test_ready_corrects_cpu_provenance_and_reports_actual_policy_and_allocator(self):
        provenance = {"device": "cpu", "runtime": {"torch": "pinned"}}
        ready = worker.ready_event(self.args, Model(), Torch(), self.bundle, provenance, self.requests_sha256)
        self.assertEqual("cpu", provenance["device"])
        self.assertEqual("mps", ready["device"])
        self.assertEqual("mps", ready["provenance"]["device"])
        self.assertEqual("fastino_mps", ready["arm"])
        self.assertEqual(worker.SCOPE, ready["scope"])
        self.assertEqual(bench.TIMING_BOUNDARY, ready["timing_boundary"])
        self.assertEqual(worker.MATH_POLICY, ready["math_policy"])
        self.assertEqual(worker.SYNC_POLICIES["mps"], ready["synchronization_policy"])
        self.assertEqual(self.requests_sha256, ready["requests_sha256"])
        self.assertEqual(self.bundle["files"], ready["model_files"])
        self.assertIs(False, ready["qualification"])
        self.assertIs(True, ready["deterministic_algorithms"])
        self.assertEqual(1, ready["model_tensor_summary"]["parameters"])
        self.assertEqual(1, ready["model_tensor_summary"]["floating_buffers"])
        self.assertEqual(1, ready["model_tensor_summary"]["other_buffers"])
        self.assertEqual({"current_allocated_bytes": 32, "driver_allocated_bytes": 64, "recommended_max_bytes": 128}, ready["mps_memory"])

    def test_readiness_rejects_wrong_devices_dtypes_training_and_missing_parameters(self):
        for tensor_set, tensor in (("parameters", Tensor(device="cpu")), ("buffers", Tensor(device="cpu", dtype="int64")),
                                   ("buffers", Tensor(dtype="float64")), ("parameters", Tensor(dtype="float16")),
                                   ("parameters", Tensor(dtype="int64")), ("buffers", Tensor(dtype="complex64"))):
            model = Model()
            setattr(model, tensor_set, [("wrong", tensor)])
            with self.subTest(tensors=tensor_set, dtype=tensor.dtype, device=tensor.device), self.assertRaises(worker.WorkerError):
                worker.verify_model_tensors(model, Torch(), "mps")
        for attr, value in (("training", True), ("strict_extraction", False), ("architecture", "span"), ("parameters", [])):
            model = Model()
            setattr(model, attr, value)
            with self.subTest(attr=attr), self.assertRaises(worker.WorkerError):
                worker.verify_model_tensors(model, Torch(), "mps")

    def test_readiness_rejects_unavailable_mps_threads_or_relaxed_determinism(self):
        for key, value in (("available", False), ("built", False), ("threads", 2), ("interop", 2),
                           ("deterministic", False), ("warn_only", True), ("default_dtype", "float16")):
            torch = Torch()
            setattr(torch, key, value)
            with self.subTest(key=key), self.assertRaises(worker.WorkerError):
                worker.verify_runtime(torch, "mps")
        torch.available = torch.built = False
        torch.default_dtype = "float32"
        worker.verify_runtime(torch, "cpu")

    def test_mps_clock_includes_decode_and_last_fence_but_excludes_first_fence_and_memory(self):
        events = []
        torch = Torch(events)
        ticks = iter((100, 350))
        def clock():
            events.append("clock")
            return next(ticks)
        def extract():
            self.assertTrue(torch.inference_active)
            events.extend(("schema", "encoder", "heads", "host_decode", "temporaries_released"))
            return {"value": 7}
        output, duration = worker.timed_call(torch, "mps", extract, clock=clock)
        worker.mps_memory(torch, "mps")
        self.assertEqual({"value": 7}, output)
        self.assertEqual(250, duration)
        self.assertEqual(["inference_enter", "synchronize", "clock", "schema", "encoder", "heads", "host_decode",
                          "temporaries_released", "synchronize", "clock", "inference_exit", "memory_current",
                          "memory_driver", "memory_recommended"], events)

    def test_cpu_clock_never_touches_mps(self):
        events = []
        torch = Torch(events)
        torch.mps = None
        ticks = iter((1, 4))
        output, duration = worker.timed_call(torch, "cpu", lambda: "host result", clock=lambda: next(ticks))
        self.assertEqual(("host result", 3), (output, duration))
        self.assertIsNone(worker.mps_memory(torch, "cpu"))
        self.assertEqual(["inference_enter", "inference_exit"], events)

    def test_failed_case_is_drained_before_reuse_but_has_no_timing_sample(self):
        events = []
        torch = Torch(events)
        def unsupported():
            events.append("unsupported")
            raise NotImplementedError("operator not implemented for MPS")
        with self.assertRaises(NotImplementedError):
            worker.timed_call(torch, "mps", unsupported, clock=lambda: events.append("clock") or 1)
        self.assertEqual(["inference_enter", "synchronize", "clock", "unsupported", "synchronize", "inference_exit"], events)
        torch.mps.synchronize = mock.Mock(side_effect=RuntimeError("command buffer failed"))
        with self.assertRaises(worker.DeviceSynchronizationError):
            worker.timed_call(torch, "mps", lambda: None, clock=lambda: 1)

    def test_fallback_warning_including_unconditional_svd_is_fatal_but_host_decode_is_allowed(self):
        for operator in ("aten::embedding_renorm_", "aten::linalg_svd", "aten::other"):
            text = f"The operator '{operator}' is not currently supported on the MPS backend and will fall back to run on the CPU."
            with self.subTest(operator=operator), self.assertRaises(UserWarning) as caught:
                with worker.reject_mps_fallback():
                    warnings.warn(text, UserWarning)
            error = worker.error_details(caught.exception, "mps")
            self.assertEqual("mps_cpu_fallback", error["category"])
            self.assertFalse(error["recoverable"])
        with warnings.catch_warnings(record=True) as captured:
            with worker.reject_mps_fallback():
                warnings.warn("Using eager attention because SDPA is unavailable", UserWarning)
        self.assertEqual(1, len(captured))
        torch = Torch()
        ticks = iter((1, 2))
        self.assertEqual(([1, 2, 3], 1), worker.timed_call(torch, "mps", lambda: Tensor().cpu().tolist()[0], clock=lambda: next(ticks)))

    def test_validation_captures_direct_encoder_ids_devices_and_removes_all_hooks(self):
        for positional in (False, True):
            model, torch = Model(), Torch()
            ids = Tensor(dtype="int64")
            with worker.capture_validation_inputs(model, torch, "mps") as (captured, devices):
                if positional:
                    model.encoder(ids, attention_mask=Tensor(dtype="bool"))
                else:
                    model.encoder(input_ids=ids, attention_mask=Tensor(dtype="bool"))
            self.assertEqual([[1, 2, 3]], captured)
            self.assertEqual("mps", devices[0]["input_device"])
            self.assertEqual({"mps"}, set(devices[0]["encoder_input_devices"].values()))
            self.assertEqual([], model.encoder.hooks)
        with self.assertRaisesRegex(ValueError, "source failure"):
            with worker.capture_validation_inputs(model, torch, "mps"):
                raise ValueError("source failure")
        self.assertEqual([], model.encoder.hooks)

    def test_input_device_rejection_precedes_validation_readback_and_bad_tokens_do_not_pass(self):
        for tensor in (Tensor(device="cpu", dtype="int64"), Tensor(device="mps:1", dtype="int64"), Tensor(dtype="float16")):
            events = []
            tensor.events = events
            model = Model()
            with self.subTest(device=tensor.device, dtype=tensor.dtype), self.assertRaises(worker.WorkerError):
                with worker.capture_validation_inputs(model, Torch(), "mps"):
                    model.encoder(input_ids=tensor)
            self.assertEqual([], events)
            self.assertEqual([], model.encoder.hooks)
        for ids in ([], [[]], [[1], [2]], [[True]], [[1.5]], [[1] * 513]):
            model = Model()
            with self.subTest(ids=ids), self.assertRaises(bench.BenchmarkError):
                with worker.capture_validation_inputs(model, Torch(), "mps"):
                    model.encoder(input_ids=Tensor(ids, dtype="int64"))
            self.assertEqual([], model.encoder.hooks)
        model = Model()
        with self.assertRaises(worker.WorkerError):
            with worker.capture_validation_inputs(model, Torch(), "mps"):
                model.encoder(input_ids=Tensor(dtype="int64"), attention_mask=Tensor(device="cpu", dtype="bool"))

    def serve(self, commands, *, execute=None, model=None, torch=None, canonical=None):
        events = []
        model, torch = model or Model(self.args.device), torch or Torch()
        ticks = iter(range(1, 1000))
        if execute is None:
            def execute(model, request, schema_json):
                self.assertTrue(model.strict_extraction)
                self.assertTrue(torch.inference_active)
                self.assertEqual(request["schema"], bench.strict_json(schema_json))
                model.encoder(input_ids=Tensor(dtype="int64", device=self.args.device))
                return {"confidence": 0.5}
        raw = b"".join(json.dumps(command).encode() + b"\n" for command in commands)
        with mock.patch.object(oracle, "verify_model_dir", return_value=self.bundle) as model_pin, \
             mock.patch.object(oracle, "verify_upstream_checkout") as source_pin:
            code = worker.serve_commands(self.args, model, torch, self.bundle, self.requests, self.requests_sha256,
                                         source=io.BytesIO(raw), emit=events.append, execute=execute,
                                         canonical=canonical or (lambda request, output: output), clock=lambda: next(ticks))
        return code, events, model_pin, source_pin

    def test_validate_run_stop_protocol_reuses_source_execution_and_omits_timed_hooks(self):
        case = self.requests[0]["id"]
        hook_counts = []
        def execute(model, request, schema):
            hook_counts.append(len(model.encoder.hooks))
            model.encoder(input_ids=Tensor(dtype="int64"))
            return {"confidence": 0.5}
        code, events, model_pin, source_pin = self.serve([
            {"op": "validate", "request_id": 1, "case_id": case},
            {"op": "run", "request_id": 2, "case_id": case},
            {"op": "stop", "request_id": 3, "case_id": ""},
        ], execute=execute)
        self.assertEqual(0, code)
        self.assertEqual([2, 0], hook_counts)
        self.assertEqual(["result", "result", "stopped"], [event["event"] for event in events])
        self.assertEqual([1, 2, 3], [event["request_id"] for event in events])
        self.assertEqual([1, 2, 3], events[0]["input_ids"])
        self.assertEqual("mps", events[0]["input_device"])
        self.assertIsNone(events[1]["input_ids"])
        self.assertIsNone(events[1]["input_device"])
        self.assertTrue(all(event["arm"] == "fastino_mps" for event in events))
        model_pin.assert_called_once()
        source_pin.assert_called_once()

    def test_recoverable_unsupported_case_retains_identity_and_does_not_drop_later_cases(self):
        bad, good = [case["id"] for case in self.requests[:2]]
        def execute(model, request, schema):
            if request["id"] == bad:
                raise NotImplementedError("operator not implemented for MPS")
            model.encoder(input_ids=Tensor(dtype="int64"))
            return {"confidence": 0.25}
        code, events, _, _ = self.serve([
            {"op": "validate", "request_id": 1, "case_id": bad},
            {"op": "validate", "request_id": 2, "case_id": good},
            {"op": "stop", "request_id": 3},
        ], execute=execute)
        self.assertEqual(0, code)
        self.assertEqual(["error", "result", "stopped"], [row["event"] for row in events])
        self.assertEqual((1, bad, "unsupported_operation", True),
                         tuple(events[0][key] for key in ("request_id", "case_id", "category", "recoverable")))
        self.assertNotIn("duration_ns", events[0])

    def test_device_oom_fallback_and_nonfinite_output_never_become_successful_samples(self):
        case = self.requests[0]["id"]
        for failure, expected in ((RuntimeError("MPS backend out of memory"), "out_of_memory"),
                                  (RuntimeError("CUDA error: device-side assert"), "device_failure"),
                                  (UserWarning("MPS will fall back to run on the CPU"), "mps_cpu_fallback")):
            def execute(*args):
                raise failure
            code, events, model_pin, _ = self.serve([
                {"op": "validate", "request_id": 1, "case_id": case},
                {"op": "stop", "request_id": 2},
            ], execute=execute)
            self.assertEqual(1, code)
            self.assertEqual(1, len(events))
            self.assertEqual(expected, events[0]["category"])
            self.assertFalse(events[0]["recoverable"])
            self.assertNotIn("duration_ns", events[0])
            model_pin.assert_not_called()
        for invalid in (float("nan"), float("inf"), -float("inf")):
            code, events, _, _ = self.serve([
                {"op": "validate", "request_id": 1, "case_id": case},
            ], canonical=lambda *_: {"confidence": invalid})
            self.assertEqual(1, code)
            self.assertEqual("error", events[0]["event"])
            self.assertIn("non-finite", events[0]["message"])

    def test_case_cannot_run_before_input_validation_or_when_strict_extraction_was_disabled(self):
        case = self.requests[0]["id"]
        code, events, _, _ = self.serve([{"op": "run", "request_id": 1, "case_id": case}])
        self.assertEqual(1, code)
        self.assertIn("requires successful", events[0]["message"])
        model = Model()
        model.strict_extraction = False
        code, events, _, _ = self.serve([{"op": "validate", "request_id": 1, "case_id": case}], model=model)
        self.assertEqual(1, code)
        self.assertIn("strict_extraction", events[0]["message"])

    def test_protocol_invalid_identity_duplicate_keys_overflow_or_eof_fail_closed(self):
        for raw in (b'{"op":"stop","request_id":true}\n', b'{"op":"stop","request_id":0}\n',
                    b'{"op":"stop","request_id":1,"request_id":2}\n', b'{"op":"stop","request_id":NaN}\n',
                    b'{"op":"stop","request_id":1}', b"x" * 2049 + b"\n", b"", b"[]\n",
                    b'{"op":"stop","request_id":1,"unexpected":0}\n'):
            with self.subTest(raw=raw[:100]), self.assertRaises((bench.BenchmarkError, oracle.ContractError)):
                worker.serve_commands(self.args, Model(), Torch(), self.bundle, self.requests, self.requests_sha256,
                                      source=io.BytesIO(raw), emit=lambda _: None)
        case = self.requests[0]["id"]
        with self.assertRaises(worker.WorkerError):
            self.serve([{"op": "validate", "request_id": 2, "case_id": case},
                        {"op": "run", "request_id": 2, "case_id": case}])
        self.args.max_commands = 1
        with self.assertRaisesRegex(worker.WorkerError, "limit"):
            self.serve([{"op": "validate", "request_id": 1, "case_id": case}, {"op": "stop", "request_id": 2}])

    def test_stop_rehashes_model_source_and_exact_requests_without_reporting_stopped_on_tamper(self):
        for changed in ("model", "source", "requests"):
            output = []
            with mock.patch.object(oracle, "verify_model_dir", return_value={} if changed == "model" else self.bundle), \
                 mock.patch.object(oracle, "verify_upstream_checkout", side_effect=oracle.ContractError("dirty source") if changed == "source" else None), \
                 mock.patch.object(worker, "read_requests", return_value=(self.requests, "0" * 64 if changed == "requests" else self.requests_sha256)):
                with self.subTest(changed=changed), self.assertRaises((worker.WorkerError, oracle.ContractError)):
                    worker.serve_commands(self.args, Model(), Torch(), self.bundle, self.requests, self.requests_sha256,
                                          source=io.BytesIO(b'{"op":"stop","request_id":1}\n'), emit=output.append)
            self.assertEqual([], output)

    def test_fixed_requests_are_bounded_and_do_not_allow_duplicate_case_identity(self):
        self.assertEqual(10, len(self.requests))
        self.assertEqual(oracle.sha256_file(oracle.FIXTURES / "requests.json"), self.requests_sha256)
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "requests.json"
            duplicate = copy.deepcopy(self.requests)
            duplicate[1]["id"] = duplicate[0]["id"]
            for requests in (duplicate, self.requests[:9]):
                path.write_text(json.dumps({"format_version": 1, "requests": requests}))
                with self.assertRaises(worker.WorkerError):
                    worker.read_requests(path)
            path.write_bytes(b" " * (worker.MAX_REQUEST_BYTES + 1))
            with self.assertRaises(worker.WorkerError):
                worker.read_requests(path)

    def test_unchanged_canonical_adapter_covers_all_thirty_pinned_task_outputs(self):
        requests = {row["id"]: row for row in self.requests}
        for variant in ("small", "base", "multi"):
            capture = oracle.read_json(oracle.FIXTURES / f"{variant}_reference/capture.json")
            expected = {row["id"]: row["expected"] for row in oracle.read_json(bench.case_fixture(variant))["cases"]}
            for row in capture["requests"]:
                with self.subTest(model=variant, case=row["id"]):
                    output = bench.canonical_python(requests[row["id"]], row["output"])
                    worker.finite_output(output)
                    bench.require_equal(bench.canonical_result(expected[row["id"]]), output)


if __name__ == "__main__":
    unittest.main()
