"""Native-epoch input/profile isolation checks; no numerical runtime needed."""
from __future__ import annotations

import copy
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock

import capture_training_inactive_adapters as control
import capture_training_inactive_native_epoch as epoch
from test_training_inactive_adapters import tensor_header


class InactiveNativeEpochContract(unittest.TestCase):
    def assert_tensor_bindings_equal(self, actual, expected, actual_tensors, expected_tensors, path=""):
        """Compare logical metadata and every referenced tensor's exact stored value."""
        self.assertIs(type(actual),type(expected),path)
        if isinstance(expected,str) and expected in expected_tensors:
            self.assertIn(actual,actual_tensors,path)
            self.assertEqual(expected_tensors[expected],actual_tensors[actual],path)
            return 1
        if isinstance(expected,dict):
            self.assertEqual(expected.keys(),actual.keys(),path)
            return sum(self.assert_tensor_bindings_equal(actual[key],expected[key],actual_tensors,expected_tensors,
                       path+"."+key) for key in expected)
        if isinstance(expected,list):
            self.assertEqual(len(expected),len(actual),path)
            return sum(self.assert_tensor_bindings_equal(value,expected[index],actual_tensors,expected_tensors,
                       path+"["+str(index)+"]") for index,value in enumerate(actual))
        self.assertEqual(expected,actual,path)
        return 0

    def test_only_declared_epoch_changes_and_private_helper_preserves_control(self):
        original_profiles = copy.deepcopy(control.PROFILES)
        original_contract = control.CONTRACT
        original_scope = control.SCOPE
        helper = epoch.load_control()
        self.assertEqual([2, 4, 5], helper.PROFILES["classifier_only"]["flush_after"])
        actual = copy.deepcopy(helper.PROFILES)
        actual["classifier_only"]["flush_after"] = [2, 3, 5]
        self.assertEqual(original_profiles, actual)
        self.assertEqual(original_profiles, control.PROFILES)
        self.assertEqual(original_contract, control.CONTRACT)
        self.assertEqual(original_scope, control.SCOPE)
        self.assertIsNot(helper.capture, control.capture)
        self.assertEqual(epoch.expected_contract(), epoch.oracle.read_json(epoch.CONTRACT))
        self.assertFalse(any(name in sys.modules for name in ("torch", "gliner2", "peft")))

    def test_jsonl_is_exact_ordered_case_schema_and_annotation_mapping(self):
        helper = epoch.load_control()
        cases = helper.examples()
        files = epoch.input_files(helper)
        self.assertEqual(4, len(files))
        for family, profile in helper.PROFILES.items():
            raw = files[family + ".jsonl"]
            self.assertTrue(raw.endswith(b"\n"))
            rows = [json.loads(line) for line in raw.splitlines()]
            self.assertEqual(len(profile["sequence"]), len(rows))
            self.assertEqual(len(rows), len({row["id"] for row in rows}))
            for index, (row, case_id) in enumerate(zip(rows, profile["sequence"], strict=True)):
                case = cases[case_id]
                self.assertEqual(1, row["version"])
                self.assertEqual(family + "-" + str(index), row["id"])
                self.assertEqual(case["text"], row["text"])
                self.assertEqual(case["schema_json"], json.dumps(row["schema"], separators=(",", ":")))
                self.assertEqual("Ada", row["text"].encode()[0:3].decode())
                self.assertEqual(case["annotations"]["entities"][0]["source"], row["entities"][0]["span"])
                labels = [] if case_id == "inactive" else ["good" if case_id == "active_good" else "bad"]
                self.assertEqual(labels, [] if "classifications" not in row else row["classifications"][0]["labels"])
                self.assertEqual(case_id != "inactive", "classifications" in row)
            # One immutable unshuffled epoch, with a flush at each full window
            # and at the epoch end. There is no cursor or post-update injection.
            flushes = [i for i in range(1, len(rows) + 1) if i % 2 == 0 or i == len(rows)]
            self.assertEqual(profile["flush_after"], flushes)

    def test_contract_or_helper_tamper_fails_without_loading_numerical_runtime(self):
        value = epoch.expected_contract()
        for change in ("flush", "lr", "dropout", "source"):
            altered = copy.deepcopy(value)
            if change == "flush": altered["profiles"]["classifier_only"]["flush_after"] = [2, 3, 5]
            elif change == "lr": altered["task_lr"] = .02
            elif change == "dropout": altered["dropout"] = .1
            else: altered["source_commit"] = "0" * 40
            with tempfile.TemporaryDirectory() as temporary:
                path = Path(temporary) / "contract.json"
                path.write_text(json.dumps(altered))
                with mock.patch.object(epoch, "CONTRACT", path):
                    with self.assertRaisesRegex(epoch.oracle.ContractError, "native epoch contract differs"):
                        epoch.load_control()
        with mock.patch.object(epoch, "digest", return_value={"size_bytes": 1, "sha256": "0" * 64}):
            with self.assertRaisesRegex(epoch.oracle.ContractError, "frozen control generator changed"):
                epoch.load_control()

    def test_input_publication_is_atomic_bounded_and_does_not_overwrite(self):
        helper = epoch.load_control()
        with tempfile.TemporaryDirectory() as temporary:
            destination = Path(temporary) / "inputs"
            with mock.patch.object(epoch, "preflight", return_value=helper):
                result = epoch.prepare_inputs(Path("unused"), destination)
                self.assertFalse(result["qualification"])
                self.assertEqual(5, len(result["files"]))
                self.assertLess(sum(pin["size_bytes"] for pin in result["files"].values()), 64 * 1024)
                for name, pin in result["files"].items():
                    self.assertEqual(pin, epoch.digest(destination / name))
                before = {path.name: path.read_bytes() for path in destination.iterdir()}
                with self.assertRaisesRegex(epoch.oracle.ContractError, "refusing to overwrite"):
                    epoch.prepare_inputs(Path("unused"), destination)
                self.assertEqual(before, {path.name: path.read_bytes() for path in destination.iterdir()})
                failed = Path(temporary) / "failure"
                with mock.patch.object(epoch, "write_inputs", side_effect=RuntimeError("injected write failure")):
                    with self.assertRaisesRegex(RuntimeError, "injected write failure"):
                        epoch.prepare_inputs(Path("unused"), failed)
                self.assertFalse(failed.exists())
                self.assertEqual([destination], list(Path(temporary).iterdir()))

    def test_captured_fixture_pins_native_rows_and_source_generator_provenance(self):
        directory = epoch.oracle.FIXTURES / "training_inactive_native_epoch"
        expected = {
            "capture.json": {"size_bytes": 423554, "sha256": "24cefafa4dde1f7067b3ba81e7b9209495a58a0cb31620c5805b00bec9abdc24"},
            "tensors.safetensors": {"size_bytes": 207343, "sha256": "eb8d7939308642c9587c73789070516e632078850473feecd3416f57d665246c"},
        }
        report = epoch.oracle.read_json(directory / "capture.json")
        expected.update(report["native_inputs"])
        self.assertEqual(set(expected), {path.name for path in directory.iterdir()})
        for name, pin in expected.items():
            self.assertEqual(pin, epoch.digest(directory / name))
        self.assertEqual(epoch.SCOPE, report["scope"])
        self.assertFalse(report["qualification"])
        self.assertEqual(epoch.digest(epoch.CONTRACT), report["contract"])
        self.assertEqual(epoch.digest(Path(epoch.__file__)), report["capture_wrapper"])
        self.assertEqual(epoch.CONTROL_PINS["generator"], report["generator"])
        self.assertEqual(control.digest(Path(control.step_capture.__file__)), report["tensor_storage_helper"])
        self.assertEqual(epoch.expected_contract()["tensor_storage_helper"], report["tensor_storage_helper"])
        self.assertEqual(epoch.NATIVE_EPOCH, report["profile_adapter"])
        self.assertIn(epoch.NEW_EPOCH_NOTE, report["notes"])
        self.assertNotIn(epoch.OLD_EPOCH_NOTE, report["notes"])
        helper = epoch.load_control()
        self.assertEqual(epoch.native_settings(helper), report["native_settings"])
        self.assertEqual(report["native_settings"], epoch.oracle.read_json(directory / "native_settings.json"))
        for name, raw in epoch.input_files(helper).items():
            self.assertEqual(raw, (directory / name).read_bytes())

    def test_captured_epoch_changes_only_classifier_schedule_and_preserves_initial_state(self):
        directory = epoch.oracle.FIXTURES / "training_inactive_native_epoch"
        original = epoch.oracle.FIXTURES / "training_inactive_adapters"
        report = control.step_capture.expand_metadata(epoch.oracle.read_json(directory / "capture.json"))
        prior = control.step_capture.expand_metadata(epoch.oracle.read_json(original / "capture.json"))

        def tensors(path):
            with path.open('rb') as stream:
                raw=stream.read(8 * 1024**2 + 1)
            header,start=tensor_header(raw)
            return {name: (item["dtype"],tuple(item["shape"]),raw[start+item["data_offsets"][0]:start+item["data_offsets"][1]])
                    for name,item in header.items()}

        actual,expected=tensors(directory/"tensors.safetensors"),tensors(original/"tensors.safetensors")
        profiles = {profile["id"]: profile for profile in prior["profiles"]}
        self.assertEqual(set(profiles),{profile["id"] for profile in report["profiles"]})
        self.assertEqual(8,len(profiles))
        self.assertEqual(8,len(report["profiles"]))
        for key in ("config", "encoder_config", "base_parameters", "optimizer", "cases", "tokenizer_fragments"):
            self.assert_tensor_bindings_equal(report[key],prior[key],actual,expected,key)
        unchanged=0
        initial_states=0
        for profile in report["profiles"]:
            profile_id=profile["id"]
            # Canonical physical names are chosen across each entire capture.
            # A changed classifier schedule can change which name holds an
            # unchanged value; dtype, shape, payload and None remain exact.
            self.assertGreater(self.assert_tensor_bindings_equal(profile["initial"],profiles[profile_id]["initial"],
                               actual,expected,profile_id+".initial"),0)
            initial_states+=1
            if not profile["id"].endswith(".classifier_only"):
                self.assertGreater(self.assert_tensor_bindings_equal(profile,profiles[profile_id],
                                   actual,expected,profile_id),0)
                unchanged+=1
                continue
            self.assertEqual([2, 4, 5], profile["flush_after"])
            self.assertEqual([2, 2, 1], [f["microbatches"] for f in profile["flushes"]])
            self.assertEqual([1, 1, 2], [f["partial_renormalization"] for f in profile["flushes"]])
            self.assertEqual([0, 0, 1, 1, 2], [m["global_step_before"] for m in profile["microbatches"]])
            self.assertTrue(profile["fresh_owner_mid_window_resume_exact"])
            self.assertTrue(profile["frozen_parameters_unchanged"])
            for micro in profile["microbatches"]:
                self.assertEqual(micro["case"] == "inactive", micro["fallback"])
                self.assertGreater(micro["model_loss"], 0)
                if micro["fallback"]:
                    self.assertEqual(0, micro["reported_loss"])
                    self.assertFalse(micro["model_loss_requires_grad"])
                    self.assertTrue(all(key is not None for key in micro["gradients_unscaled"].values()))
            self.assertEqual(0, profile["flushes"][-1]["grad_norm"])
            for index, flush in enumerate(profile["flushes"], 1):
                self.assertEqual(index, flush["global_step"])
                self.assertEqual(index, flush["scheduler_last_epoch"])
                self.assertTrue(all(slot["state_present"] and slot["step"] == index for slot in flush["parameters"].values()))

        self.assertEqual(6,unchanged)
        self.assertEqual(8,initial_states)


if __name__ == "__main__":
    unittest.main()
