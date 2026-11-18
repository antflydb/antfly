# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Regression checks for failed and incomplete CPU media oracle artifacts."""

import copy
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import media_campaign
import report_media
from media_validation import validate_oracle


def oracle():
    vector = [1.0] + [0.0] * 767
    return {
        "cases": [
            {
                "name": name,
                "max_abs": 0.0,
                "cosine": 1.0,
                "upstream_vector": vector.copy(),
                "wrapper_vector": vector.copy(),
            }
            for name in ("first", "last")
        ]
    }


class OracleValidationTests(unittest.TestCase):
    def test_valid_exact_and_tolerated_vectors(self):
        value = oracle()
        value["cases"][-1]["wrapper_vector"][1] = 5e-7
        value["cases"][-1]["max_abs"] = 5e-7
        expected = validate_oracle(value, ["last", "first"], 1e-6)
        self.assertEqual(set(expected), {"first", "last"})

    def test_nonfinite_and_negative_error_metadata_are_rejected(self):
        for error in (float("nan"), float("inf"), -float("inf"), -1.0):
            with self.subTest(error=error):
                value = oracle()
                value["cases"][-1]["max_abs"] = error
                # Reproduce the artifact left by a failed final CPU case.
                value = json.loads(json.dumps(value))
                with self.assertRaises(ValueError):
                    validate_oracle(value, ["first", "last"], 1e-6)

    def test_nonfinite_cosine_is_rejected(self):
        value = oracle()
        value["cases"][-1]["cosine"] = float("nan")
        with self.assertRaises(ValueError):
            validate_oracle(value, ["first", "last"], 1e-6)

    def test_invalid_vectors_are_rejected_despite_success_metadata(self):
        for field in ("wrapper_vector", "upstream_vector"):
            for kind in ("nan", "inf", "shape", "norm"):
                with self.subTest(field=field, kind=kind):
                    value = oracle()
                    vector = value["cases"][-1][field]
                    if kind == "shape":
                        vector.pop()
                    else:
                        vector[0] = {
                            "nan": float("nan"),
                            "inf": float("inf"),
                            "norm": 0.0,
                        }[kind]
                    with self.assertRaises(ValueError):
                        validate_oracle(value, ["first", "last"], 1e-6)

    def test_wrapper_difference_is_recomputed(self):
        value = oracle()
        value["cases"][-1]["wrapper_vector"] = [0.0, 1.0] + [0.0] * 766
        with self.assertRaisesRegex(ValueError, "exceeds tolerance"):
            validate_oracle(value, ["first", "last"], 1e-6)

    def test_missing_duplicate_and_extra_cases_are_rejected(self):
        for names in (["first"], ["first", "first"], ["first", "last", "extra"]):
            with self.subTest(names=names):
                value = oracle()
                row = value["cases"][0]
                value["cases"] = [
                    {**copy.deepcopy(row), "name": name} for name in names
                ]
                with self.assertRaises(ValueError):
                    validate_oracle(value, ["first", "last"], 1e-6)

    def test_campaign_rejects_failed_final_cpu_case_before_launch(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            cases = [{"name": name} for name in ("first", "last")]
            (base / "suite.json").write_text(json.dumps({"cases": cases}))
            (base / "manifest.json").write_text("{}")
            value = oracle()
            value["cases"][-1]["max_abs"] = float("nan")
            (base / "oracle.json").write_text(json.dumps(value))
            (base / "acceptance.json").write_text(
                json.dumps(
                    {
                        "timing": {
                            "rounds": 3,
                            "warmups": 3,
                            "samples_per_case_per_arm_per_round": 8,
                        },
                        "static_wrapper_vs_unmodified_upstream_cpu": {"max_abs": 1e-6},
                    }
                )
            )
            args = ["media_campaign.py"]
            for flag, path in {
                "binary": "unused-binary",
                "runner": "unused-runner",
                "plugin": "unused-plugin",
                "model": "unused-model",
                "suite": "suite.json",
                "manifest": "manifest.json",
                "oracle": "oracle.json",
                "acceptance": "acceptance.json",
                "out": "campaign",
            }.items():
                args += ["--" + flag, str(base / path)]
            with (
                patch("sys.argv", args),
                patch.object(media_campaign.subprocess, "Popen") as launch,
            ):
                with self.assertRaisesRegex(ValueError, "CPU oracle wrapper error"):
                    media_campaign.main()
                launch.assert_not_called()
            self.assertFalse((base / "campaign/summary.json").exists())

    def test_report_rejects_failed_fp32_oracle_before_publishing(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            (base / "summary.json").write_text(
                json.dumps({"cases": [{"name": "first"}, {"name": "last"}]})
            )
            value = oracle()
            value["cases"][-1]["max_abs"] = float("nan")
            (base / "oracle.json").write_text(json.dumps(value))
            (base / "gate.json").write_text(
                json.dumps({"wrapper_upstream_max_abs": 1e-6})
            )
            output = base / "report.json"
            with (
                patch(
                    "sys.argv",
                    [
                        "report_media.py",
                        "--run",
                        str(base),
                        "--fp32-oracle",
                        str(base / "oracle.json"),
                        "--fp32-acceptance",
                        str(base / "gate.json"),
                        "--out",
                        str(output),
                    ],
                ),
                self.assertRaisesRegex(ValueError, "CPU oracle wrapper error"),
            ):
                report_media.main()
            self.assertFalse(output.exists())


if __name__ == "__main__":
    unittest.main()
