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

import math
import io
import os
import unittest
from unittest.mock import patch

from cuda_video_qualification import cleanup_snapshot, validate_vector


class VectorValidationTests(unittest.TestCase):
    def test_normalized_vector(self):
        self.assertAlmostEqual(validate_vector([1 / math.sqrt(768)] * 768), 1)

    def test_invalid_outputs(self):
        for vector in (
            [1.0] * 767,
            [0.0] * 768,
            [1.0] * 768,
            [float("nan")] + [0.0] * 767,
            [float("inf")] + [0.0] * 767,
            [True] + [0.0] * 767,
            ["1"] + [0.0] * 767,
        ):
            with self.subTest(first=vector[0], length=len(vector)):
                with self.assertRaises(ValueError):
                    validate_vector(vector)

    def test_cleanup_requires_quiescent_metrics(self):
        names = ("antfly_inference_embedding_groups_active",
                 "antfly_admission_inference_in_flight_requests")
        for values, accepted in (((0, 0), True), ((1, 0), False), ((0, 1), False)):
            raw = "\n".join(f"{name} {value}" for name, value in zip(names, values)).encode()
            with self.subTest(values=values), patch(
                "cuda_video_qualification.urllib.request.urlopen", return_value=io.BytesIO(raw)
            ):
                if accepted:
                    self.assertIn("VmRSS", cleanup_snapshot("http://unused", os.getpid())["process"])
                else:
                    with self.assertRaises(ValueError):
                        cleanup_snapshot("http://unused", os.getpid())


if __name__ == "__main__":
    unittest.main()
