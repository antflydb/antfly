"""Native-epoch input/profile isolation checks; no numerical runtime needed."""
from __future__ import annotations

import copy
import json
from pathlib import Path
import struct
import sys
import tempfile
import unittest
from unittest import mock

import capture_training_inactive_adapters as control
import capture_training_inactive_native_epoch as epoch


class InactiveNativeEpochContract(unittest.TestCase):
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
            "capture.json": {"size_bytes": 867431, "sha256": "9947cc37d6adc8b209c7769b2cd61f239738c3583646747444e655bb1afa44f1"},
            "tensors.safetensors": {"size_bytes": 591162, "sha256": "374658ee67127b2f81ec597a65eabe72ee52263d88a718ebf4ef7e9b246741e9"},
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
        report = epoch.oracle.read_json(directory / "capture.json")
        prior = epoch.oracle.read_json(original / "capture.json")
        profiles = {profile["id"]: profile for profile in prior["profiles"]}
        for key in ("config", "encoder_config", "base_parameters", "optimizer", "cases", "tokenizer_fragments"):
            self.assertEqual(prior[key], report[key])
        for profile in report["profiles"]:
            if not profile["id"].endswith(".classifier_only"):
                self.assertEqual(profiles[profile["id"]], profile)
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

        def tensors(path):
            raw = path.read_bytes()
            length = struct.unpack("<Q", raw[:8])[0]
            self.assertLess(length, 1024**2)
            header = json.loads(raw[8:8 + length])
            start = 8 + length
            return {name: (item["dtype"], item["shape"], raw[start + item["data_offsets"][0]:start + item["data_offsets"][1]])
                    for name, item in header.items()}

        actual, expected = tensors(directory / "tensors.safetensors"), tensors(original / "tensors.safetensors")
        unchanged = 0
        for key, value in actual.items():
            if ".classifier_only." not in key or ".classifier_only.initial." in key:
                self.assertEqual(expected[key], value, key)
                unchanged += 1
        self.assertEqual(1552, unchanged)


if __name__ == "__main__":
    unittest.main()
