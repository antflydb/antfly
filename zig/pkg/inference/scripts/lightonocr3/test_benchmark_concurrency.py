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

"""Serving qualification must distinguish a CUDA model from CUDA recurrence."""

import unittest

from benchmark_concurrency import recurrent_placement_observation


class RecurrentPlacementTests(unittest.TestCase):
    def test_cuda_backend_banner_is_insufficient(self):
        self.assertFalse(
            recurrent_placement_observation("LIGHTONOCR3 backend=cuda", None)["matched"]
        )

    def test_default_requires_every_loaded_session_on_device(self):
        gpu = "inference recurrent placement backend=cuda placement=cuda reason=supported_geometry compute=8.0"
        host = "inference recurrent placement backend=cuda placement=host reason=unsupported_geometry compute=8.0"
        result = recurrent_placement_observation(gpu, None)
        self.assertTrue(result["matched"])
        self.assertEqual(result["sessions"][0]["compute"], "8.0")
        self.assertFalse(
            recurrent_placement_observation(gpu + "\n" + host, None)["matched"]
        )

    def test_explicit_cpu_baseline_requires_operator_override(self):
        host = "inference recurrent placement backend=cuda placement=host reason=operator_disabled compute=12.1"
        self.assertTrue(recurrent_placement_observation(host, False)["matched"])
        self.assertFalse(recurrent_placement_observation(host, None)["matched"])
        self.assertFalse(
            recurrent_placement_observation(
                host.replace("operator_disabled", "unsupported_device"), False
            )["matched"]
        )


if __name__ == "__main__":
    unittest.main()
