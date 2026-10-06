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

import json
import tempfile
import unittest
from pathlib import Path

from run_posting_locality_ab import validate_pair_recall


class PairRecallTest(unittest.TestCase):
    def check(self, after=0.98, count=1000, *, invalid_live=None):
        with tempfile.TemporaryDirectory() as directory:
            roots = [Path(directory) / name for name in ("control", "candidate")]
            for root, recall in zip(roots, (0.99, after), strict=True):
                root.mkdir()
                live = (
                    invalid_live
                    if root == roots[1] and invalid_live is not None
                    else recall
                )
                (root / "qualification-summary.json").write_text(
                    json.dumps(
                        {
                            "runs": [
                                {"label": "online-live", "recall": live},
                                {"label": "reopened-warm", "recall": recall},
                            ]
                        }
                    )
                )
                (root / "public-query-profile.json").write_text(
                    json.dumps({"count": count, "recall": recall})
                )
            return validate_pair_recall(*roots, 1000)

    def test_one_percentage_point_boundary_passes(self):
        self.assertEqual(self.check()["fixed-profile"], [0.99, 0.98])

    def test_loss_and_missing_work_fail_closed(self):
        with self.assertRaises(RuntimeError):
            self.check(0.9799)
        with self.assertRaises(RuntimeError):
            self.check(count=10)
        for value in (float("nan"), -1, 0, 1.1, True):
            with self.assertRaises(RuntimeError):
                self.check(invalid_live=value)


if __name__ == "__main__":
    unittest.main()
