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

import copy
import unittest

import quality_ordered_messages as ordered


def artifact(call_id="call-1", name="read_source_ranges", arguments='{"end":9,"start":1}'):
    return {"schema_version": 2, "messages": [
        {"role": "user", "parts": [{"text": "read the source"}]},
        {"role": "assistant", "parts": [], "content_is_null": True, "tool_calls": [{
            "id": call_id, "type": "function",
            "function": {"name": name, "arguments_json": arguments},
        }]},
        {"role": "tool", "parts": [{"text": "authoritative tool output", "selected": True}],
         "tool_call_id": call_id},
        {"role": "user", "parts": [{"text": "return the result"}]},
    ]}


class ToolHistoryV2Tests(unittest.TestCase):
    def test_schema1_golden_is_unchanged(self):
        value = {"schema_version": 1, "messages": [
            {"role": "user", "parts": [{"text": "document", "selected": True}]},
            {"role": "assistant", "parts": [{"text": "stored"}]},
            {"role": "user", "parts": [{"text": "question"}]},
        ]}
        self.assertEqual(ordered.semantic_digest(value),
                         "7c3eaf2f4e5bb244a3ec01c5e2b068e29454582dcd963b2a2b4d040cb8606179")
        forged = copy.deepcopy(value)
        forged["messages"][1]["content_is_null"] = False
        with self.assertRaises(ValueError):
            ordered.validate(forged)

    def test_call_metadata_is_domain_bound_but_never_selected_source(self):
        base = artifact()
        digest = ordered.semantic_digest(base)
        self.assertEqual(digest, "aca13549c4e9eb3fac3c8d6d64d67d8eef55aab7f79063425db350009111d436")
        source = ordered.selected_source_digest(base)
        for changed in (artifact(call_id="call-2"), artifact(name="read_other"),
                        artifact(arguments='{"end":10,"start":1}')):
            self.assertNotEqual(digest, ordered.semantic_digest(changed))
            self.assertEqual(source, ordered.selected_source_digest(changed))
        self.assertEqual(ordered.selected_part_digests(base), [
            __import__("hashlib").sha256(b"authoritative tool output").hexdigest()
        ])

    def test_orphan_duplicate_mismatch_and_nonobject_arguments_rejected(self):
        orphan = {"schema_version": 2, "messages": [
            {"role": "tool", "parts": [{"text": "source", "selected": True}], "tool_call_id": "missing"}
        ]}
        duplicate = artifact()
        duplicate["messages"][1]["tool_calls"].append(copy.deepcopy(duplicate["messages"][1]["tool_calls"][0]))
        duplicate["messages"].insert(3, {"role": "tool", "parts": [{"text": "second"}], "tool_call_id": "call-1"})
        mismatch = artifact()
        mismatch["messages"][2]["tool_call_id"] = "different"
        nonobject = artifact(arguments="[]")
        oversized = artifact(arguments='{"value":"' + "x" * ordered.MAX_TOOL_ARGUMENTS_BYTES + '"}')
        for value in (orphan, duplicate, mismatch, nonobject, oversized):
            with self.assertRaises(ValueError):
                ordered.validate(value)

    def test_owned_json_shape_rejects_unknown_duplicate_and_selected_assistant_metadata(self):
        valid = artifact()
        raw = __import__("json").dumps(valid, separators=(",", ":")).encode()
        self.assertEqual(ordered.loads(raw), valid)
        selected = artifact()
        selected["messages"][1]["parts"] = [{"text": "metadata", "selected": True}]
        with self.assertRaises(ValueError):
            ordered.validate(selected)
        with self.assertRaises(ValueError):
            ordered.loads(b'{"schema_version":2,"schema_version":2,"messages":[]}')
        unknown = artifact()
        unknown["messages"][1]["unknown"] = 1
        with self.assertRaises(ValueError):
            ordered.validate(unknown)

    def test_raw_presence_numeric_and_depth_contract(self):
        base = artifact()
        encoded = __import__("json").dumps(base, separators=(",", ":"))
        with self.assertRaises(ValueError):
            ordered.loads(encoded.replace('"type":"function",', "", 1).encode())
        legacy = b'{"schema_version":1,"messages":[{"role":"user","parts":[{"text":"source","selected":true}],"content_is_null":false}]}'
        with self.assertRaises(ValueError):
            ordered.loads(legacy)
        aliases = (
            b'{"schema_version":2,"messages":[{"role":"user","parts":[{"text":"source","selected":true}]},{"role":"assistant","parts":[{"text":"ordinary"}],"content_is_null":false,"tool_calls":null}]}',
            b'{"schema_version":2,"messages":[{"role":"user","parts":[{"text":"source","selected":true}],"content_is_null":false}]}',
            b'{"schema_version":2,"messages":[{"role":"assistant","parts":[],"content_is_null":true,"tool_calls":[{"id":"call","type":"function","function":{"name":"read","arguments_json":"{}"}}]},{"role":"tool","parts":[{"text":"source","selected":true}],"tool_call_id":null}]}',
        )
        for raw in aliases:
            with self.assertRaises(ValueError):
                ordered.loads(raw)
        ordered.validate(artifact(arguments='{"min":-9223372036854775808,"max":9223372036854775807,"finite":1.25e3}'))
        for arguments in (
            '{"value":9223372036854775808}',
            '{"value":-9223372036854775809}',
            '{"value":1e999}',
        ):
            with self.assertRaises(ValueError):
                ordered.validate(artifact(arguments=arguments))
        depth64 = '{"value":' + "[" * 63 + "0" + "]" * 63 + "}"
        ordered.validate(artifact(arguments=depth64))
        depth65 = '{"value":' + "[" * 64 + "0" + "]" * 64 + "}"
        with self.assertRaises(ValueError):
            ordered.validate(artifact(arguments=depth65))
        prefix, suffix = '{"values":["', '"]}'
        wide = prefix + "a" * (ordered.MAX_TOOL_ARGUMENTS_BYTES - 1 - len(prefix) - len(suffix)) + suffix
        self.assertEqual(len(wide.encode()), ordered.MAX_TOOL_ARGUMENTS_BYTES - 1)
        ordered.validate(artifact(arguments=wide))
        with self.assertRaises(ValueError):
            ordered.validate(artifact(arguments='{"values":[' + "0," * ordered.MAX_TOOL_ARGUMENT_VALUES + "0]}"))


if __name__ == "__main__":
    unittest.main()
