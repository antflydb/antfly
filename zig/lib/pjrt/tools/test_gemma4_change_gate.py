# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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
