from __future__ import annotations

import contextlib
import copy
import io
import json
import math
import os
from pathlib import Path
import subprocess
import struct
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock

import check_training_export as exports
import check_training_merge as check
import test_training_export as fixtures


def entries(path):
    with exports.Opened(path, exports.MAX_WEIGHT) as file:
        return {name: (item["shape"], list(struct.unpack("<" + "f" * (item["size_bytes"] // 4),
            b"".join(file.chunks(item["offset"], item["size_bytes"]))))) for name, item in exports.tensor_header(file).items()}


def scaffold(root, mode="lora"):
    source, adapter, training, pins, inventory = fixtures.scaffold(root, mode)
    original = entries(source / "model.safetensors")
    # Keep the real full-inventory admission active in these tiny byte fixtures.
    for index in range(330):
        original[f"frozen.constant.{index:03}"] = ([1], [float(index)])
    inventory.update({name: {"shape": value[0], "dtype": "F32"} for name, value in original.items()})
    pins["model.safetensors"] = fixtures.write_tensors(source / "model.safetensors", original)
    identity = exports.source_identity("small", pins)
    adapter_receipt = json.loads((adapter / exports.ADAPTER_RECEIPT).read_text())
    adapter_receipt.update(source=identity, frozen_weight_sha256=pins["model.safetensors"]["sha256"])
    adapter_pin = fixtures.write_json(adapter / exports.ADAPTER_RECEIPT, adapter_receipt)
    snapshot = json.loads((adapter / exports.RECEIPT).read_text())
    snapshot.update(source=identity, adapter_receipt=adapter_pin)
    snapshot_pin = fixtures.write_json(adapter / exports.RECEIPT, snapshot)
    run = json.loads((training / "run.json").read_text())
    run["source"] = identity
    fixtures.write_json(training / "run.json", run)
    result = json.loads((training / "result.json").read_text())
    result["portable_model"]["provenance"] = snapshot_pin
    fixtures.write_json(training / "result.json", result)
    adapter_files = {"config": snapshot["adapter_config"], "weights": snapshot["weights"], "receipt": adapter_pin}
    configuration = {"version": 1, "source_dir": str(source.resolve()), "adapter_dir": str(adapter.resolve()),
                     "output_dir": str((root / "merged").resolve()), "expected_source": identity, "expected_adapter": adapter_files,
                     "schema_sha256": snapshot["provenance"]["schemas_sha256"]}
    config_path = root / "materialize.json"
    config_pin = fixtures.write_json(config_path, configuration)
    merged = root / "merged"
    merged.mkdir()
    values = copy.deepcopy(original)
    values["encoder.encoder.layer.0.weight"] = ([2, 2], [5., 6., 7., 8.])
    merged_pins = {"model.safetensors": fixtures.write_tensors(merged / "model.safetensors", values)}
    for name in exports.SIDECARS:
        target = merged / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes((source / name).read_bytes())
        merged_pins[name] = exports.digest_bytes(target.read_bytes())
    contract, _ = check.load_contract()
    receipt = {**contract["native_receipt"], "source": identity,
        "provenance": {"configuration": config_pin, "adapter_files": adapter_files,
                       "schema_sha256": snapshot["provenance"]["schemas_sha256"]},
        "target_sha256": adapter_receipt["target_sha256"], "parameter_sha256": adapter_receipt["parameter_sha256"],
        "merged": exports.source_identity("small", merged_pins), "tensor_count": 334, "merged_tensor_count": 1}
    fixtures.write_json(merged / check.RECEIPT, receipt)
    return SimpleNamespace(root=root, source=source, adapter=adapter, training=training, merged=merged,
                           config=config_path, pins=pins, inventory=inventory, contract=contract)


@contextlib.contextmanager
def prepared(mode="lora"):
    with tempfile.TemporaryDirectory() as temporary:
        value = scaffold(Path(temporary), mode)
        with mock.patch.object(exports, "expected_source", return_value=value.pins), \
             mock.patch.object(exports, "published_inventory", return_value=value.inventory):
            yield value


def audit(value):
    return check.audit_merge("small", value.source, value.adapter, value.training, value.merged, value.config, value.contract)


def rewrite_merged(value, change):
    tensors = entries(value.merged / "model.safetensors")
    change(tensors)
    actual = fixtures.write_tensors(value.merged / "model.safetensors", tensors)
    receipt = json.loads((value.merged / check.RECEIPT).read_text())
    receipt["merged"]["weight"] = actual
    fixtures.write_json(value.merged / check.RECEIPT, receipt)


def args(value):
    return check.parser().parse_args(["--variant", "small", "--source-dir", str(value.source),
        "--adapter-dir", str(value.adapter), "--run-dir", str(value.training), "--merged-dir", str(value.merged),
        "--job-config", str(value.config), "--output-dir", str(value.root / "report"),
        "--runtime", "--peft-wheel", str(value.root / "unused-wheel.whl")])


class MergeIntegrityTests(unittest.TestCase):
    def test_both_modes_keep_full334_source_bias_sidecars_and_final_training_state(self):
        for mode in ("lora", "dora"):
            with self.subTest(mode=mode), prepared(mode) as value:
                result = audit(value)
                self.assertEqual((334, 333, 1), (len(result["merged_tensors"]), len(result["untouched_names"]), len(result["adapted_names"])))
                self.assertTrue(result["untouched_and_bias_bytes_equal"])
                self.assertTrue(result["training_export"]["job"]["state_digest_recomputed"])
                self.assertIn("no_merge_math_claim", result["static_scope"])

    def test_rehashed_untouched_weight_and_bias_mutations_are_rejected(self):
        for name in ("frozen.constant.000", "encoder.encoder.layer.0.bias", "classifier.weight"):
            with self.subTest(name=name), prepared() as value:
                rewrite_merged(value, lambda tensors: tensors[name][1].__setitem__(0, .25))
                with self.assertRaisesRegex(ValueError, "untouched tensor or bias"):
                    audit(value)

    def test_complete_inventory_shape_nonfinite_and_sidecar_cannot_be_rehashed_away(self):
        mutations = (
            lambda tensors: tensors.pop("frozen.constant.000"),
            lambda tensors: tensors.__setitem__("frozen.extra", ([1], [0.])),
            lambda tensors: tensors.__setitem__("encoder.encoder.layer.0.weight", ([1, 4], [1., 2., 3., 4.])),
            lambda tensors: tensors["encoder.encoder.layer.0.weight"][1].__setitem__(0, float("inf")),
        )
        for mutation in mutations:
            with self.subTest(mutation=mutation), prepared() as value:
                rewrite_merged(value, mutation)
                with self.assertRaises(ValueError): audit(value)
        with prepared() as value:
            (value.merged / "tokenizer_config.json").write_text('{}\n')
            with self.assertRaisesRegex(ValueError, "sidecar"): audit(value)

    def test_exact_job_bytes_schema_adapter_counts_and_math_policy_are_bound(self):
        for key, replacement in (("target_sha256", "1" * 64), ("parameter_sha256", "2" * 64),
                                  ("merged_tensor_count", 2), ("tensor_count", 333), ("version", True),
                                  ("math_policy", "other_math")):
            with self.subTest(key=key), prepared() as value:
                receipt = json.loads((value.merged / check.RECEIPT).read_text())
                receipt[key] = replacement
                fixtures.write_json(value.merged / check.RECEIPT, receipt)
                with self.assertRaises(ValueError): audit(value)
        with prepared() as value:
            value.config.write_bytes(value.config.read_bytes() + b" ")
            with self.assertRaisesRegex(ValueError, "job bytes"): audit(value)
        with prepared() as value:
            configuration = json.loads(value.config.read_text())
            configuration["schema_sha256"] = [4] * 32
            receipt = json.loads((value.merged / check.RECEIPT).read_text())
            receipt["provenance"]["configuration"] = fixtures.write_json(value.config, configuration)
            fixtures.write_json(value.merged / check.RECEIPT, receipt)
            with self.assertRaisesRegex(ValueError, "job schema"): audit(value)
        for field, content in (("version", True), ("source_dir", "/source/../other"),
                               ("memory", {"job_bytes": 1.5}), ("merge_limits", {"adapter": {"max_rank": True}})):
            with self.subTest(field=field), prepared() as value:
                configuration = json.loads(value.config.read_text())
                configuration[field] = content
                receipt = json.loads((value.merged / check.RECEIPT).read_text())
                receipt["provenance"]["configuration"] = fixtures.write_json(value.config, configuration)
                fixtures.write_json(value.merged / check.RECEIPT, receipt)
                with self.assertRaises(ValueError): audit(value)

    def test_static_run_publishes_narrow_scope_failure_is_retained_and_never_overwrites(self):
        with prepared() as value:
            selected = args(value)
            selected.runtime, selected.peft_wheel = False, None
            report = check.run(selected)
            self.assertEqual("static_verified", report["status"])
            self.assertFalse(report["numerical_runtime_executed"])
            before = (selected.output_dir / "report.json").read_bytes()
            with self.assertRaises(FileExistsError): check.run(selected)
            self.assertEqual(before, (selected.output_dir / "report.json").read_bytes())
        with prepared() as value:
            selected = args(value)
            selected.runtime, selected.peft_wheel = False, None
            rewrite_merged(value, lambda tensors: tensors["classifier.bias"][1].__setitem__(0, 2.))
            with self.assertRaises(ValueError): check.run(selected)
            failure = json.loads((selected.output_dir / "failure.json").read_text())
            self.assertEqual("incomplete", failure["status"])
            self.assertFalse(failure["qualification"])
            self.assertFalse((selected.output_dir / "report.json").exists())

    def test_immutable_tolerances_helpers_and_request_fixture(self):
        contract, _ = check.load_contract()
        requests, _ = check.request_fixture(contract)
        self.assertEqual(10, len(requests))
        self.assertEqual((1e-6, 1e-5, 5e-4), tuple(contract["tolerances"][name] for name in
                         ("adapted_absolute", "adapted_relative", "confidence_absolute")))
        changed = copy.deepcopy(contract)
        changed["tolerances"]["adapted_absolute"] = 1e-3
        with mock.patch.object(check, "read_json", return_value=(changed, {})):
            with self.assertRaisesRegex(ValueError, "contract"): check.load_contract()
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            check.parser().parse_args(["--adapted-absolute", ".1"])


class MergeNumericalTests(unittest.TestCase):
    def test_absolute_plus_relative_rule_nearzero_large_negative_nonfinite_and_full_count(self):
        row = check.numeric_comparison([0., -10., 1000.], [1e-6, -10.0001, 1000.01])
        self.assertEqual((3, 0), (row["elements"], row["violations"]))
        row = check.numeric_comparison([0., -10., 1000.], [math.nextafter(1e-6, math.inf), -10.001, 1000.1])
        self.assertEqual((3, [0, 1, 2]), (row["violations"], row["first_violation_indices"]))
        for actual in ([float("nan")], [float("inf")], [True], []):
            with self.assertRaises(ValueError): check.numeric_comparison([0.], actual)

    def test_threeway_complete_token_decision_metadata_probability_and_confidence_gates(self):
        request = {"id": "one"}
        output = {"intent": {"value": "keep", "confidence": .75, "probabilities": {"keep": .75, "delete": .25}},
                  "_meta": {"objective": 1., "feasible": True, "exact": True, "violations": []},
                  "entities": [{"id": "e1", "type": "person", "text": "A", "start": 0, "end": 1, "confidence": .9}]}
        row = {"id": "one", "request_sha256": check.object_digest(request), "input_ids": [1, 2], "output": output}
        captures = {phase: [copy.deepcopy(row)] for phase in check.PHASES}
        captures["peft_merged"][0]["output"]["_meta"]["objective"] += 2e-5
        self.assertTrue(all(result["pass"] for result in check.compare_phases(captures, [request])))
        mutations = (
            lambda item: item["input_ids"].__setitem__(1, 3),
            lambda item: item["output"]["intent"].__setitem__("confidence", .751),
            lambda item: item["output"]["intent"]["probabilities"].__setitem__("delete", .251),
            lambda item: item["output"]["intent"].__setitem__("value", "delete"),
            lambda item: item["output"]["_meta"].__setitem__("exact", False),
            lambda item: item["output"]["entities"][0].__setitem__("end", 2),
        )
        for mutation in mutations:
            changed = copy.deepcopy(captures)
            mutation(changed["native_merged"][0])
            self.assertFalse(all(result["pass"] for result in check.compare_phases(changed, [request])))
        del captures["native_merged"]
        with self.assertRaisesRegex(ValueError, "incomplete"): check.compare_phases(captures, [request])

    def test_comparison_summary_cannot_omit_untouched_or_change_denominators(self):
        audit = {"merged_tensors": {"x.weight": {"size_bytes": 8}, "x.bias": {"size_bytes": 4}}, "adapted_names": ["x.weight"]}
        rows = [{"name": "x.weight", "kind": "numerical", **check.numeric_comparison([0., 1.], [0., 1.])},
                {"name": "x.bias", "kind": "exact", "elements": 1, "bytes_equal": True}]
        self.assertTrue(check.validate_tensor_results(rows, audit))
        for changed in (rows[:1], [rows[0], rows[0]], [dict(rows[0], elements=1), rows[1]],
                        [rows[0], dict(rows[1], bytes_equal=False)]):
            with self.assertRaises(ValueError): check.validate_tensor_results(changed, audit)


class MergeLifecycleTests(unittest.TestCase):
    def exercise(self, value, failure=None):
        selected = args(value)
        audited = audit(value)
        selected.output_dir.mkdir()
        requests, _ = check.request_fixture(value.contract)
        owners = []

        class FakeGuard:
            def __init__(self, *_): self.peak_rss_bytes = 12345
            def check(self): pass

        class FakeWorker:
            def __init__(self, arm, command, env, directory, guard):
                self.closed = False
                self.calls = 0
                self.buffer = bytearray()
                self.process = SimpleNamespace(poll=lambda: 0, returncode=0, stdout=io.BytesIO())
                envelope = json.loads((directory / "worker.json").read_text())
                (Path(envelope["scratch_dir"]) / "partial-copy").write_bytes(b"owned worker bytes")
                self.messages = [{"event": "ready", "scope": check.SCOPE, "qualification": False,
                    "audit_sha256": envelope["audit_sha256"], "python": envelope["python"], "helpers": envelope["helpers"]}]
                captures = {phase: [] for phase in check.PHASES}
                by_id = {request["id"]: request for request in requests}
                for kind, phase, identifier in check.event_plan(requests):
                    event = {"event": kind, "phase": phase}
                    if kind == "case":
                        request = by_id[identifier]
                        record = {"id": identifier, "request_sha256": check.object_digest(request), "input_ids": [1, 2],
                                  "output": {"value": "same", "confidence": .9}}
                        if failure == "comparison" and phase == "native_merged": record["output"]["confidence"] = .901
                        event["case"] = record
                        captures[phase].append(record)
                    self.messages.append(event)
                tensor_results = []
                for name, item in sorted(audited["merged_tensors"].items()):
                    count = item["size_bytes"] // 4
                    if name in audited["adapted_names"]:
                        tensor_results.append({"name": name, "kind": "numerical", **check.numeric_comparison([0.] * count, [0.] * count)})
                    else:
                        tensor_results.append({"name": name, "kind": "exact", "elements": count, "bytes_equal": True})
                result = {"qualification": False, "tolerances": check.TOLERANCES, "loader_profile": "peft-0.18.0-export-v1",
                    "one_resident_model_owner": True, "owners_released_before_next_load": True,
                    "network_allowed": False, "missing_weight_fallback": False, "quality_evaluation": False,
                    "requests": check.REQUEST_PIN, "private_copy_bytes": 10,
                    "captures": captures, "comparisons": check.compare_phases(captures, requests),
                    "tensor_comparisons": tensor_results, "tensor_parity_pass": True,
                    "status": "comparison_failed" if failure == "comparison" else "verified"}
                if failure == "omitted": del self.messages[-2]
                if failure == "substitution": result["comparisons"][0]["pass"] = False
                self.messages.append({"event": "complete", "runtime": result})
                owners.append(self)
            def receive(self, _timeout):
                self.calls += 1
                if failure == "cancel" and self.calls == 3: raise KeyboardInterrupt()
                if failure == "deadline" and self.calls == 3: raise TimeoutError("runtime deadline")
                return self.messages[self.calls - 1]
            def close(self): self.closed = True

        with mock.patch.object(check, "MergeGuard", FakeGuard), mock.patch.object(check.bench, "Worker", FakeWorker):
            if failure in ("cancel", "deadline", "omitted", "substitution"):
                with self.assertRaises((KeyboardInterrupt, TimeoutError, ValueError)):
                    check.supervise_runtime(audited, selected, selected.output_dir, value.contract)
            else:
                result = check.supervise_runtime(audited, selected, selected.output_dir, value.contract)
                self.assertEqual("comparison_failed" if failure else "verified", result["status"])
        self.assertTrue(owners[0].closed)
        receipt = json.loads((selected.output_dir / "runtime.process.json").read_text())
        self.assertEqual(failure in (None, "comparison"), receipt["complete_protocol"])
        self.assertTrue(receipt["scratch_cleaned"])
        self.assertFalse((selected.output_dir / ".runtime-scratch").exists())

    def test_complete_threephase_protocol_and_numerical_failure_are_distinct(self):
        for failure in (None, "comparison"):
            with self.subTest(failure=failure), prepared() as value: self.exercise(value, failure)

    def test_cancel_deadline_missing_case_and_substituted_summary_close_worker(self):
        for failure in ("cancel", "deadline", "omitted", "substitution"):
            with self.subTest(failure=failure), prepared() as value: self.exercise(value, failure)

    def test_runtime_owner_views_must_release_before_next_model(self):
        class Model:
            def __init__(self): self.base = Base()
            def merge_and_unload(self, **_): return self.base
        class Base:
            def state_dict(self): return {}
            def eval(self): return self
        # Plain functions avoid mock.call_args retaining the very owners whose
        # release this test checks.
        with mock.patch.object(check, "load_unmerged", new=lambda *_: Model()), \
             mock.patch.object(check, "capture_outputs", new=lambda *_: []), \
             mock.patch.object(check, "compare_merged_tensors", new=lambda *_: []):
            result = check.source_forms({}, Path('/unused'), [], None, lambda _: None)
            self.assertEqual(({"unmerged": [], "peft_merged": []}, []), result)
        retained = []
        def leaked(*_):
            model = Model()
            retained.append(model)
            return model
        with mock.patch.object(check, "load_unmerged", new=leaked), \
             mock.patch.object(check, "capture_outputs", new=lambda *_: []):
            with self.assertRaisesRegex(ValueError, "still retained"):
                check.source_forms({}, Path('/unused'), [], None, lambda _: None)

    def test_real_worker_rejects_altered_envelope_before_loading_any_runtime(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "worker.json"
            path.write_text('{"scope":"wrong"}\n')
            command = [check.python_identity()["invocation"], str(Path(check.__file__)), "worker", str(path), "0" * 64]
            result = subprocess.run(command, env=dict(os.environ, PYTHONDONTWRITEBYTECODE="1"),
                                    text=True, capture_output=True, timeout=15)
            self.assertEqual(1, result.returncode)
            event = json.loads(result.stdout)
            self.assertEqual("error", event["event"])
            self.assertIn("invalid merge worker envelope", event["error"]["message"])
            self.assertEqual([path], list(path.parent.iterdir()))


if __name__ == "__main__":
    unittest.main()
