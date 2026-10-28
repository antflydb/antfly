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
        names = (
            "antfly_inference_embedding_groups_active",
            "antfly_admission_inference_in_flight_requests",
        )
        for values, accepted in (((0, 0), True), ((1, 0), False), ((0, 1), False)):
            raw = "\n".join(
                f"{name} {value}" for name, value in zip(names, values)
            ).encode()
            with (
                self.subTest(values=values),
                patch(
                    "cuda_video_qualification.urllib.request.urlopen",
                    return_value=io.BytesIO(raw),
                ),
                patch(
                    "cuda_video_qualification.Path.read_text",
                    autospec=True,
                    side_effect=lambda path: {
                        "/proc/101/status": "Name:\tsupervisor\nVmRSS:\t10 kB\nVmHWM:\t12 kB\n",
                        "/proc/101/task/101/children": "202",
                        "/proc/202/status": "Name:\tworker\nVmRSS:\t100 kB\nVmHWM:\t120 kB\n",
                        "/proc/202/task/202/children": "",
                    }[str(path)],
                ) as read_status,
            ):
                if accepted:
                    snapshot = cleanup_snapshot("http://unused", 101)
                    self.assertEqual(
                        snapshot["process"], {"VmRSS": "10 kB", "VmHWM": "12 kB"}
                    )
                    self.assertEqual(snapshot["tree_rss_kib"], 110)
                    self.assertEqual(
                        [row["pid"] for row in snapshot["process_tree"]], [101, 202]
                    )
                    self.assertEqual(snapshot["process_tree"][1]["hwm_kib"], 120)
                else:
                    with self.assertRaises(ValueError):
                        cleanup_snapshot("http://unused", 101)
                    read_status.assert_not_called()


if __name__ == "__main__":
    unittest.main()
