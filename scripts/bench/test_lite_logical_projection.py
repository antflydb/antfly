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
import unittest

from lite_logical_projection import LogicalFieldHashes


def report(documents, columns=("id",)):
    hashes = LogicalFieldHashes(columns)
    for document in documents:
        hashes.add(document)
    return hashes.report()


class LogicalProjectionTests(unittest.TestCase):
    def test_iteration_and_json_object_order_do_not_change_identity(self):
        first = [{"id": "a", "body": {"x": 1, "y": 2}}, {"id": "b", "body": "other"}]
        second = [{"body": "other", "id": "b"}, {"body": {"y": 2, "x": 1}, "id": "a"}]
        self.assertEqual(report(first), report(second))

    def test_changed_nested_value_is_detected(self):
        first = report([{"id": "a", "body": {"x": [1, 2]}}])
        changed = report([{"id": "a", "body": {"x": [2, 1]}}])
        self.assertEqual(first["logical_row_sha256"], changed["logical_row_sha256"])
        self.assertNotEqual(
            first["field_sha256"]["body"], changed["field_sha256"]["body"]
        )

    def test_missing_and_replaced_rows_are_detected(self):
        first = report([{"id": "a"}, {"id": "b"}])
        self.assertNotEqual(first["rows"], report([{"id": "a"}])["rows"])
        replaced = report([{"id": "a"}, {"id": "c"}])
        self.assertEqual(first["rows"], replaced["rows"])
        self.assertNotEqual(first["logical_row_sha256"], replaced["logical_row_sha256"])

    def test_compound_key_boundaries_are_preserved(self):
        first = report([{"a": "a", "b": "bc"}], ("a", "b"))
        second = report([{"a": "ab", "b": "c"}], ("a", "b"))
        self.assertNotEqual(first["logical_row_sha256"], second["logical_row_sha256"])

    def test_absent_field_and_explicit_null_differ(self):
        first = report([{"id": "a", "body": None}, {"id": "b"}])
        second = report([{"id": "a"}, {"id": "b", "body": None}])
        self.assertNotEqual(
            first["field_sha256"]["body"], second["field_sha256"]["body"]
        )

    def test_invalid_or_duplicate_keys_fail_closed(self):
        for columns in [(), ("id", "id")]:
            with self.assertRaises(ValueError):
                LogicalFieldHashes(columns)
        for documents in [[{}], [{"id": None}], [{"id": "a"}, {"id": "a"}]]:
            with self.assertRaises(ValueError):
                report(documents)

    def test_report_does_not_expose_fixture_values(self):
        result = report(
            [{"id": "private-id-sentinel", "body": "private-payload-sentinel"}]
        )
        encoded = json.dumps(result)
        self.assertNotIn("private-id-sentinel", encoded)
        self.assertNotIn("private-payload-sentinel", encoded)


if __name__ == "__main__":
    unittest.main()
