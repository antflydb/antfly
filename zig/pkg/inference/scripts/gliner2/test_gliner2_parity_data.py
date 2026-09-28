#!/usr/bin/env python3
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

import unittest

from gliner2_parity_data import allowed_labels_for_objective, normalize_python_record


class ParityDataTest(unittest.TestCase):
    def test_total_loss_preserves_non_default_entity_labels(self) -> None:
        record = {
            "input": "Rex runs",
            "output": {"entities": {"animal": ["Rex"], "person": []}},
        }
        total_labels = allowed_labels_for_objective(
            "gliner2-total-loss", "person,organization,location"
        )
        normalized, counts, labels = normalize_python_record(record, total_labels)
        self.assertIn("animal", normalized["output"]["entities"])
        self.assertEqual(1, counts["entity_mentions"])
        self.assertEqual({"animal", "person"}, labels)

        span_labels = allowed_labels_for_objective(
            "span-start", "person,organization,location"
        )
        normalized, _, _ = normalize_python_record(record, span_labels)
        self.assertNotIn("animal", normalized["output"]["entities"])


if __name__ == "__main__":
    unittest.main()
