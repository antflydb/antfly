from __future__ import annotations

import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

import run


class PreparedExportTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.plan = run.load_plan()
        cls.entry = cls.plan["profiles"]["cpu-lora"]
        cls.static = run.read_json(cls.entry["static_audit"]["path"])
        prior = run.read_json(cls.plan["prior_all_target_export_proofs"]["lora"]["path"])
        cls.report = copy.deepcopy(cls.static)
        cls.report["numerical_runtime_executed"] = True
        cls.report["runtime"] = prior["runtime"]
        cls.report["runtime"]["private_copy_bytes"] = cls.entry["runtime_copy_bytes"]

    def test_four_exact_commands_preserve_virtualenv_and_fixed_profile(self):
        for name, entry in self.plan["profiles"].items():
            command = entry["command"]
            self.assertEqual(command[:2], [self.plan["python_invocation"], "-B"])
            self.assertNotEqual(command[0], self.plan["python_executable"]["path"])
            self.assertEqual(command[command.index("--runtime-profile") + 1], "peft-0.18.0-export-v1")
            self.assertEqual(command[command.index("--requests") + 1], self.plan["requests"]["path"])
            self.assertEqual(entry["expected_modules"], ["classifier.0", "classifier.3"])
            self.assertEqual(entry["expected_adapter_tensor_count"], 4 if name.endswith("lora") else 6)
            self.assertLess(entry["runtime_copy_bytes"] + entry["additional_request_copy_bytes"], self.plan["max_runtime_copy_bytes"])

    def test_same_size_foreign_bytes_and_symlink_are_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            original = root / "original"
            original.write_bytes(b"same")
            expected = run.file_pin(original)
            original.write_bytes(b"evil")
            with self.assertRaisesRegex(ValueError, "frozen input differs"):
                run.exact(original, expected)
            (root / "link").symlink_to(original)
            with self.assertRaises(OSError):
                run.file_pin(root / "link")

    def test_foreign_slot_bytes_source_model_or_checkpoint_cannot_be_accepted(self):
        for field in ("export_tensors", "source_tensors", "job"):
            report = copy.deepcopy(self.report)
            report[field] = {}
            with self.subTest(field=field), self.assertRaisesRegex(ValueError, "proof differs"):
                run.validate_report_content(report, self.static, self.entry, self.plan)

    def test_missing_fallback_or_wrong_loader_request_is_rejected(self):
        mutations = [("missing_weight_fallback", True), ("all_loaded_tensor_bytes_equal", False),
                     ("network_allowed", True), ("loader_profile", "oracle-0.17.1"),
                     ("private_copy_bytes", 1), ("requests", {"size_bytes": 1, "sha256": "0" * 64})]
        for field, value in mutations:
            with self.subTest(field=field), self.assertRaises(ValueError):
                report = copy.deepcopy(self.report)
                report["runtime"][field] = value
                run.validate_report_content(report, self.static, self.entry, self.plan)

    def test_all_ten_requests_and_exact_runtime_identity_are_required(self):
        expected, captures = run.validate_report_content(self.report, self.static, self.entry, self.plan)
        self.assertEqual(len(expected), 9)
        self.assertEqual(len(captures), 8)
        for change in ("omit", "duplicate", "wrong_dependency", "wrong_wheel"):
            with self.subTest(change=change), self.assertRaises(ValueError):
                report = copy.deepcopy(self.report)
                if change == "omit":
                    report["runtime"]["outputs"].pop()
                elif change == "duplicate":
                    report["runtime"]["outputs"][-1] = report["runtime"]["outputs"][0]
                elif change == "wrong_dependency":
                    report["runtime"]["provenance"]["runtime"]["packages"]["peft"] = "0.17.1"
                else:
                    report["runtime"]["provenance"]["peft_wheel"]["sha256"] = "0" * 64
                run.validate_report_content(report, self.static, self.entry, self.plan)

    def test_artifact_tree_never_follows_symlink_or_allows_byte_overflow(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "file").write_bytes(b"1234")
            self.assertEqual(run.tree_sizes(root, 4), {"file": 4})
            with self.assertRaisesRegex(ValueError, "byte ceiling"):
                run.tree_sizes(root, 3)
            (root / "link").symlink_to(root / "file")
            with self.assertRaisesRegex(ValueError, "nonregular"):
                run.tree_sizes(root, 100)

    def test_cleanup_requires_reaping_and_preserves_published_and_partial_evidence(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            case = root / "execution/cpu-lora"
            stage = case / ".report-owned"
            temporary_model = stage / ".training-export-runtime-owned/source"
            temporary_model.mkdir(parents=True)
            (temporary_model / "model.safetensors").write_bytes(b"weights")
            (stage / "retained.safetensors").write_bytes(b"partial-capture")
            (case / "report").mkdir()
            (case / "report/report.json").write_bytes(b"published")
            (case / "stderr.log").write_bytes(b"failure")
            with mock.patch.object(run, "ROOT", root), mock.patch.object(run, "load_plan", return_value=self.plan):
                with self.assertRaisesRegex(ValueError, "before complete"):
                    run.cleanup_unpublished_copies(case, {"cleanup": {"complete": False}}, 1000)
                self.assertTrue(temporary_model.exists())
                removed = run.cleanup_unpublished_copies(case, {"cleanup": {"complete": True}}, 1000)
                self.assertEqual(len(removed), 1)
                self.assertFalse(temporary_model.exists())
                self.assertEqual((stage / "retained.safetensors").read_bytes(), b"partial-capture")
                self.assertEqual((case / "report/report.json").read_bytes(), b"published")
                self.assertEqual((case / "stderr.log").read_bytes(), b"failure")
                outside = root / "outside"
                outside.mkdir()
                with self.assertRaisesRegex(ValueError, "not a helper-owned"):
                    run.cleanup_unpublished_copies(outside, {"cleanup": {"complete": True}}, 1000)

    def test_bad_artifact_guard_does_not_prevent_process_cleanup(self):
        class Tracker:
            def __init__(self, *_):
                self.sampled = 0
            def sample(self):
                self.sampled += 1
                return 4
            def cleanup_sample(self):
                return self.sample()
            def receipt(self):
                return {}
        supervisor = type("Supervisor", (), {"ProcessTree": Tracker})
        tracked = run.artifact_tracker(supervisor, Path("/unused"), 16)(None, None)
        with mock.patch.object(run, "tree_sizes", side_effect=ValueError("artifact guard")) as inspect:
            with self.assertRaisesRegex(ValueError, "artifact guard"):
                tracked.sample()
            self.assertEqual(tracked.cleanup_sample(), 4)
            self.assertEqual(inspect.call_count, 1)
        self.assertEqual(tracked.sampled, 2)


if __name__ == "__main__":
    unittest.main()
