# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Qualification must retain failures and reject incomplete engine coverage."""

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

import numpy as np
from validate_gemma4 import validate_campaign


class ValidationTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.cpu = self.root / "cpu"
        self.cpu.mkdir()
        self.campaign = self.root / "campaign"
        self.campaign.mkdir()
        self.reference = np.linspace(-1, 1, 262144, dtype="<f4")
        self.reference.tofile(self.cpu / "text.f32")
        (self.campaign / "provenance.json").write_text(
            json.dumps({"validation_only": True, "rounds": 1, "iterations": 1})
        )
        (self.campaign / "summary.json").write_text(
            json.dumps({"results": [{"id": "text", "ms": {"native": 1, "pjrt": 2}}]})
        )
        for arm in ("native", "pjrt"):
            directory = self.campaign / f"r0-{arm}"
            directory.mkdir()
            self.reference.tofile(
                directory / ("text-1.f32" if arm == "pjrt" else "text.f32")
            )

    def validate(self):
        return validate_campaign(self.campaign, [self.cpu])

    def test_complete_identical_logits_pass(self):
        report = self.validate()
        self.assertTrue(report["passed"])
        self.assertEqual(len(report["rows"]), 2)

    def test_later_step_reference_cannot_validate_prefill(self):
        (self.cpu / "reference.json").write_text(
            json.dumps({"logit_position": "teacher_forced_last_before_eos"})
        )
        with self.assertRaisesRegex(ValueError, "first-step"):
            self.validate()

    def test_missing_engine_cannot_pass(self):
        directory = self.campaign / "r0-pjrt"
        (directory / "text-1.f32").unlink()
        directory.rmdir()
        with self.assertRaisesRegex(ValueError, "rounds/arms"):
            self.validate()

    def test_missing_vector_cannot_pass(self):
        (self.campaign / "r0-native/text.f32").unlink()
        with self.assertRaisesRegex(ValueError, "logit coverage"):
            self.validate()

    def test_close_logits_with_different_argmax_fail_qualification(self):
        values = self.reference.copy()
        values[-2], values[-1] = values[-1], values[-2]
        values.tofile(self.campaign / "r0-native/text.f32")
        report = self.validate()
        self.assertTrue(report["logit_tolerance_pass"])
        self.assertFalse(report["argmax_all_match"])
        self.assertFalse(report["passed"])

    def test_duplicate_pjrt_measurement_cannot_replace_missing_vector(self):
        path = self.campaign / "r0-pjrt/text-1.f32"
        path.rename(path.with_name("text-2.f32"))
        with self.assertRaisesRegex(ValueError, "logit coverage"):
            self.validate()

    def test_timing_campaign_is_not_logit_validation(self):
        (self.campaign / "provenance.json").write_text(
            json.dumps({"validation_only": False, "rounds": 1, "iterations": 1})
        )
        with self.assertRaisesRegex(ValueError, "validation campaign"):
            self.validate()

    def test_failing_gate_exits_nonzero_after_saving_diagnostics(self):
        (-self.reference).tofile(self.campaign / "r0-native/text.f32")
        output = self.root / "report.json"
        result = subprocess.run(
            [
                sys.executable,
                str(Path(__file__).with_name("validate_gemma4.py")),
                "--campaign",
                str(self.campaign),
                "--cpu",
                str(self.cpu),
                "--out",
                str(output),
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(result.returncode, 1, result.stderr)
        report = json.loads(output.read_text())
        self.assertFalse(report["passed"])
        self.assertFalse(report["logit_tolerance_pass"])
        self.assertEqual(len(report["rows"]), 2)


if __name__ == "__main__":
    unittest.main()
