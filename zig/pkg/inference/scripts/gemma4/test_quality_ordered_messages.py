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
import quality_ordered_messages as ordered


class OrderedMessagesTests(unittest.TestCase):
    def setUp(self):
        self.value = {"schema_version": 1, "messages": [
            {"role": "user", "parts": [{"text": "document", "selected": True}]},
            {"role": "assistant", "parts": [{"text": "stored"}]},
            {"role": "user", "parts": [{"text": "question"}]},
        ]}

    def test_golden_and_every_semantic_field(self):
        self.assertEqual(ordered.semantic_digest(self.value), "7c3eaf2f4e5bb244a3ec01c5e2b068e29454582dcd963b2a2b4d040cb8606179")
        changed = {"schema_version": 1, "messages": [dict(message) for message in self.value["messages"]]}
        changed["messages"][1] = {"role": "user", "parts": [{"text": "stored"}]}
        self.assertNotEqual(ordered.semantic_digest(self.value), ordered.semantic_digest(changed))
        source = ordered.selected_source_digest(self.value)
        self.assertEqual(ordered.selected_part_digests(self.value), [__import__("hashlib").sha256(b"document").hexdigest()])
        changed["messages"][2] = {"role": "user", "parts": [{"text": "different question"}]}
        self.assertEqual(source, ordered.selected_source_digest(changed))
        changed["messages"][0] = {"role": "user", "parts": [{"text": "different source", "selected": True}]}
        self.assertNotEqual(source, ordered.selected_source_digest(changed))

    def test_strict_json_and_bounds(self):
        self.assertEqual(ordered.loads(b'{"schema_version":1,"messages":[{"role":"user","parts":[{"text":"x","selected":true}]}]}')["schema_version"], 1)
        for raw in (
            b'{"schema_version":1,"schema_version":1,"messages":[]}',
            b'{"schema_version":1,"messages":[],"unknown":0}',
            b'{"schema_version":NaN,"messages":[]}',
        ):
            with self.assertRaises(ValueError):
                ordered.loads(raw)
        forged = {"schema_version": 1, "messages": [
            {"role": "assistant", "parts": [{"text": "generated", "selected": True}]},
        ]}
        with self.assertRaises(ValueError):
            ordered.validate(forged)


if __name__ == "__main__":
    unittest.main()
