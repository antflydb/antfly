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

import hashlib
import json
from pathlib import Path
import tempfile
import unittest

import quality_corpus_tool_history_v4 as tool
import quality_corpus_pack_v8_tool_history as pack
import quality_ordered_messages as ordered


class HistoricalToolCorpusTests(unittest.TestCase):
    def blueprint(self):
        cases = []
        for index in range(100):
            cases.append({
                "id": f"structured-{index:03d}", "bucket": "structured_tool",
                "question_utf8": "Emit canonical compact JSON with keys path and value for the cited scalar.",
                "reference_utf8": '{"path":"answer","value":"7"}',
                "distractor_order_seed": "1" * 64,
                "intended_compactable_quartile": index % 4,
                "primary_source_family": f"family-{index:03d}",
                "source": {"path": f"source-{index:03d}.txt", "sha256": "2" * 64,
                           "byte_start": 9, "byte_end": 10},
            })
        return {"schema_version": 2, "stage": "quality_source_task_blueprints",
                "model_outputs_observed": False, "model": "E2B", "cases": cases}

    def test_derivation_is_deterministic_and_binds_range_metadata(self):
        raw = json.dumps(self.blueprint(), sort_keys=True, separators=(",", ":")).encode()
        first = tool.derive(raw)
        second = tool.derive(raw)
        self.assertEqual(first, second)
        self.assertEqual(first["parent_blueprints_sha256"], hashlib.sha256(raw).hexdigest())
        history = first["cases"][0]["tool_history"]
        self.assertEqual(history["assistant_call"]["function"]["name"], "read_source_ranges")
        self.assertEqual(history["assistant_call"]["function"]["arguments_contract"],
                         "packed_selected_source_ranges_v1")
        self.assertEqual(history["assistant_call"]["id"], history["tool_call_id"])

    def test_packer_places_all_selected_source_only_in_adjacent_tool_response(self):
        blueprints = self.blueprint()
        raw_blueprints = json.dumps(blueprints, sort_keys=True, separators=(",", ":")).encode()
        case = tool.derive(raw_blueprints)["cases"][0]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = b"answer = 7\n" + b"neutral source text\n" * 64
            path = root / case["source"]["path"]
            path.write_bytes(source)
            source_hash = hashlib.sha256(source).hexdigest()
            case["source"]["sha256"] = source_hash
            # Derive again so the sealed tool arguments bind the final source identity.
            blueprints["cases"][0]["source"]["sha256"] = source_hash
            case = tool.derive(json.dumps(blueprints, sort_keys=True, separators=(",", ":")).encode())["cases"][0]
            row = {"authorship": "generated", "clone_cluster_id": "c", "content_type": "text",
                   "family_id": case["primary_source_family"], "family_sha256": source_hash,
                   "license": "test", "normalized_sha256": source_hash,
                   "partition": "quality_evaluation", "path": path.name, "repository": "fixture",
                   "revision": "1", "sha256": source_hash}
            distractor = b"distractor source\n" * 32
            distractor_path = root / "distractor.txt"
            distractor_path.write_bytes(distractor)
            distractor_hash = hashlib.sha256(distractor).hexdigest()
            distractor_family = "distractor-family"
            distractor_row = dict(row, family_id=distractor_family, path=distractor_path.name,
                                  sha256=distractor_hash, family_sha256=distractor_hash,
                                  normalized_sha256=distractor_hash)
            work = {"case": case, "primary_family": case["primary_source_family"],
                    "distractor_families": [distractor_family],
                    "available_source_bytes": len(source) + len(distractor)}
            candidate = pack.compose_candidate(work, {
                case["primary_source_family"]: row, distractor_family: distractor_row,
            }, root, len(source) + len(distractor), insertion_index=0, padding_units=2)
        messages = candidate["messages"]
        self.assertEqual(messages["schema_version"], 2)
        self.assertEqual([m["role"] for m in messages["messages"]],
                         ["user", "assistant", "tool", "user"])
        self.assertTrue(messages["messages"][1]["content_is_null"])
        self.assertEqual(messages["messages"][1]["parts"], [])
        self.assertEqual(messages["messages"][1]["tool_calls"][0]["id"],
                         messages["messages"][2]["tool_call_id"])
        arguments = json.loads(messages["messages"][1]["tool_calls"][0]["function"]["arguments_json"])
        self.assertEqual(arguments, {"ranges": [
            {"byte_end": len(source), "byte_start": 0,
             "path": path.name, "sha256": source_hash},
            {"byte_end": len(distractor), "byte_start": 0,
             "path": distractor_path.name, "sha256": distractor_hash},
        ]})
        self.assertTrue(messages["messages"][2]["parts"])
        self.assertEqual(len(messages["messages"][2]["parts"]), 2)
        self.assertEqual([segment["part_index"] for segment in candidate["segments"]], [0, 1])
        for segment in candidate["segments"]:
            part = messages["messages"][segment["message_index"]]["parts"][segment["part_index"]]
            self.assertTrue(part["selected"])
            self.assertIn(f"path={segment['source_path']}", part["text"])
        self.assertTrue(all(part.get("selected") for part in messages["messages"][2]["parts"]))
        self.assertFalse(any(part.get("selected") for index, message in enumerate(messages["messages"])
                             if index != 2 for part in message["parts"]))
        self.assertEqual(messages["messages"][0]["parts"][-1],
                         {"text": " neutral neutral\n", "selected": False})
        self.assertEqual(len(messages["messages"][3]["parts"]), 1)
        self.assertFalse(messages["messages"][3]["parts"][0]["selected"])
        self.assertEqual(len(candidate["segments"]), 2)
        self.assertTrue(all(segment["message_index"] == 2 for segment in candidate["segments"]))
        primary = pack.primary_selected_part(messages, candidate["segments"], candidate["primary_label"])
        self.assertIn("answer = 7", primary["text"])
        forged = json.loads(json.dumps(candidate["segments"]))
        primary_segment = next(row for row in forged if row["label"] == candidate["primary_label"])
        primary_segment["message_index"] = 3
        primary_segment["part_index"] = 0
        with self.assertRaises(ValueError):
            pack.primary_selected_part(messages, forged, candidate["primary_label"])
        ordered.validate(messages)


if __name__ == "__main__":
    unittest.main()
