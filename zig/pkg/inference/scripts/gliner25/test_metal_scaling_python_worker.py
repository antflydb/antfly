from __future__ import annotations

import copy
import io
import json
import os
from pathlib import Path
import types
import unittest
from unittest import mock

import metal_scaling_python_worker as worker
import scaling_contract_v1 as contract
from test_metal_python_worker import Model, Tensor, Torch


class BatchTensor(Tensor):
    @property
    def shape(self):
        return (len(self.values), len(self.values[0]))

    def reshape(self, dimension):
        assert dimension == -1
        result = Tensor([x for row in self.values for x in row], device=self.device, dtype=self.dtype)
        return result


def case():
    return {"id": "mixed", "kind": "extract", "schema": {"entities": ["person"]},
            "items": [{"id": "a", "text": "Alice."}, {"id": "b", "text": "Bob."}],
            "expected_input_ids": [[1, 2, 3], [4]]}


def encode(model, ids=None, mask=None):
    model.encoder(input_ids=BatchTensor(ids or [[1, 2, 3], [4, 0, 0]], dtype="int64"),
                  attention_mask=BatchTensor(mask or [[1, 1, 1], [1, 0, 0]], dtype="int64"))


class ScalingWorkerTests(unittest.TestCase):
    def setUp(self):
        values = {key: "0" for key in worker.reference.MPS_ENV}
        values.update({key: "1" for key in worker.bench.THREAD_ENV})
        self.env = mock.patch.dict(os.environ, values)
        self.env.start()
        self.addCleanup(self.env.stop)

    def test_actual_batch_hook_is_removed_on_replay_split_padding_and_device_error(self):
        for mode in ("pass", "split", "padding", "device"):
            model, torch = Model(), Torch()
            def exercise():
                with worker.capture_batch(model, torch, "mps", case()) as captured:
                    if mode == "split":
                        encode(model, ids=[[1, 2, 3]], mask=[[1, 1, 1]])
                    elif mode == "padding":
                        encode(model, mask=[[1, 1, 1], [1, 1, 1]])
                    elif mode == "device":
                        model.encoder(input_ids=BatchTensor(device="cpu", dtype="int64"),
                                      attention_mask=BatchTensor(dtype="int64"))
                    else:
                        encode(model)
                        self.assertEqual([2, 3], captured[0]["input_shape"])
            if mode == "pass":
                exercise()
            else:
                with self.subTest(mode=mode), self.assertRaises(worker.bench.BenchmarkError):
                    exercise()
            self.assertEqual([], model.encoder.hooks)
        model = Model()
        with self.assertRaisesRegex(worker.bench.BenchmarkError, "multiple encoder"):
            with worker.capture_batch(model, Torch(), "mps", case()):
                encode(model)
                encode(model)
        self.assertEqual([], model.encoder.hooks)

    def test_extract_uses_batch_api_once_with_complete_list_and_same_fixed_schema(self):
        calls = []
        model = types.SimpleNamespace(batch_extract=lambda *args, **kwargs: calls.append((args, kwargs)) or [{}, {}])
        with mock.patch.object(worker.oracle, "build_extract_schema", side_effect=lambda spec: ("compiled", spec)):
            worker.execute_batch(model, case(), json.dumps(case()["schema"]))
        self.assertEqual(1, len(calls))
        positional, options = calls[0]
        self.assertEqual(["Alice.", "Bob."], positional[0])
        self.assertEqual(2, options["batch_size"])
        self.assertEqual(0, options["num_workers"])
        self.assertEqual(512, options["max_len"])
        self.assertEqual(0.5, options["threshold"])
        self.assertTrue(options["include_confidence"])
        self.assertTrue(options["include_spans"])

    def test_serve_validation_then_run_then_stop_with_no_hook_in_timed_run(self):
        model, torch = Model(), Torch()
        args = types.SimpleNamespace(device="mps", model="small", model_dir=Path("/unused/model"),
            upstream=Path("/unused/source"), prepared=Path("/unused/prepared"), max_commands=3)
        inputs = {"source_cases": {"mixed": case()}, "requests_sha256": "a" * 64, "pins": {"pin": "value"}}
        events, calls = [], []
        def execute(model, task, schema):
            calls.append(len(model.encoder.hooks))
            encode(model)
            return [{"entities": {"person": []}}, {"entities": {"person": []}}]
        raw = b"".join(json.dumps({"request_id": index, "op": op, **({"case_id": "mixed"} if op != "stop" else {})}).encode() + b"\n"
                       for index, op in enumerate(("validate", "run", "stop"), 1))
        clock = iter((10, 20, 30, 40))
        with mock.patch.object(worker.oracle, "verify_model_dir", return_value={}), \
             mock.patch.object(worker.oracle, "verify_upstream_checkout"), \
             mock.patch.object(contract, "load", return_value=inputs):
            result = worker.serve(args, model, torch, {}, inputs, source=io.BytesIO(raw),
                                  emit=events.append, execute=execute, clock=lambda: next(clock))
        self.assertEqual(0, result)
        self.assertEqual([1, 0], calls)
        self.assertEqual(["result", "result", "stopped"], [row["event"] for row in events])
        self.assertEqual([2, 3], events[0]["input_shape"])
        self.assertEqual(2, len(events[1]["outputs"]))
        self.assertEqual(None, events[1]["input_ids"])
        self.assertEqual([], model.encoder.hooks)

    def test_missing_batch_output_is_contract_failure(self):
        with self.assertRaisesRegex(worker.bench.BenchmarkError, "incomplete output"):
            worker.canonical_batch(case(), [{"entities": {"person": []}}])


if __name__ == "__main__":
    unittest.main()
