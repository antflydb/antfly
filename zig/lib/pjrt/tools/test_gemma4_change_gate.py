# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""An improved candidate must not silently pass a failed absolute oracle gate."""

import unittest

from qualify_native_gemma4 import accuracy_gate


class ChangeGateTests(unittest.TestCase):
    def test_improvement_does_not_clear_absolute_failure(self):
        result = accuracy_gate(
            {"relative_l2": 0.05},
            {
                "relative_l2": 0.015,
                "argmax_matches": True,
                "logit_tolerance_pass": False,
            },
        )
        self.assertTrue(result["change_accuracy_pass"])
        self.assertFalse(result["absolute_reference_pass"])

    def test_absolute_pass_does_not_hide_regression(self):
        result = accuracy_gate(
            {"relative_l2": 0.005},
            {"relative_l2": 0.01, "argmax_matches": True, "logit_tolerance_pass": True},
        )
        self.assertFalse(result["change_accuracy_pass"])
        self.assertTrue(result["absolute_reference_pass"])


if __name__ == "__main__":
    unittest.main()
