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

import quality_ordered_messages as ordered
import quality_context_qualify as quality


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


class OrderedQualityPlanTests(unittest.TestCase):
    def test_schema4_sealed_ordered_plan_and_requests(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            reference = b"result=7"
            (root / "reference.txt").write_bytes(reference)
            tokens = b"[7,9]"
            (root / "tokens.json").write_bytes(tokens)
            cases = []
            buckets = tuple(sorted(quality.release.BUCKETS))
            for bucket in buckets:
                for index in range(quality.release.MIN_QUALITY_CASES):
                    case_id = f"{bucket}-{index}"
                    artifact = {"schema_version": 1, "messages": [
                        {"role": "user", "parts": [{"text": f"source {case_id}", "selected": True}]},
                        {"role": "assistant", "parts": [{"text": "stored"}]},
                        {"role": "user", "parts": [{"text": "return the sealed result"}]},
                    ]}
                    messages = json.dumps(artifact, sort_keys=True, separators=(",", ":")).encode()
                    prompt = ("rendered " + case_id).encode()
                    message_path = f"messages-{case_id}.json"
                    prompt_path = f"prompt-{case_id}.txt"
                    (root / message_path).write_bytes(messages)
                    (root / prompt_path).write_bytes(prompt)
                    cases.append({
                        "id": case_id, "bucket": bucket,
                        "messages_file": message_path, "messages_sha256": sha(messages),
                        "request_content_sha256": ordered.semantic_digest(artifact),
                        "source_sha256": ordered.selected_source_digest(artifact),
                        "selected_part_sha256s": ordered.selected_part_digests(artifact),
                        "prompt_file": prompt_path, "prompt_sha256": sha(prompt),
                        "prompt_tokens": 2016, "reserved_output_tokens": 32,
                        "reference_file": "reference.txt", "reference_sha256": sha(reference),
                        "reference_tokens_file": "tokens.json", "reference_tokens_sha256": sha(tokens),
                        "scorer": quality.SCORER, "route": "native", "settings_sha256": "1" * 64,
                        "variant_settings_sha256": {"exact": "2" * 64, "attention_matching": "3" * 64},
                    })
            plan = {"schema_version": 4, "qualified_context_tokens": [2048], "cases": cases}
            self.assertIs(quality.plan_contract(plan, root), plan)
            requests = quality.requests(plan)
            self.assertEqual(len(requests), len(cases) * 2)
            self.assertIn("messages_file", requests[0])
            self.assertEqual(requests[0]["prompt_file"], cases[0]["prompt_file"])
            self.assertNotIn("document_file", requests[0])
            forged = json.loads(json.dumps(plan))
            forged["cases"][0]["request_content_sha256"] = "0" * 64
            with self.assertRaises(ValueError):
                quality.plan_contract(forged, root)
            changed_question = json.loads(json.dumps(plan))
            first = changed_question["cases"][0]
            original_first_raw = (root / first["messages_file"]).read_bytes()
            artifact2 = json.loads(original_first_raw)
            artifact2["messages"][-1]["parts"][0]["text"] = "changed final question"
            raw2 = json.dumps(artifact2, sort_keys=True, separators=(",", ":")).encode()
            (root / first["messages_file"]).write_bytes(raw2)
            first["messages_sha256"] = sha(raw2)
            first["request_content_sha256"] = ordered.semantic_digest(artifact2)
            self.assertEqual(first["source_sha256"], ordered.selected_source_digest(artifact2))
            quality.plan_contract(changed_question, root)
            (root / first["messages_file"]).write_bytes(original_first_raw)
            duplicate_source = json.loads(json.dumps(plan))
            first_case, second_case = duplicate_source["cases"][:2]
            first_artifact = ordered.loads((root / first_case["messages_file"]).read_bytes())
            second_artifact = ordered.loads((root / second_case["messages_file"]).read_bytes())
            second_artifact["messages"][0]["parts"][0]["text"] = first_artifact["messages"][0]["parts"][0]["text"]
            second_artifact["messages"][0]["parts"].append({"text": "selected distractor", "selected": True})
            second_artifact["messages"][-1]["parts"][0]["text"] = "another final question"
            second_raw = json.dumps(second_artifact, sort_keys=True, separators=(",", ":")).encode()
            (root / second_case["messages_file"]).write_bytes(second_raw)
            second_case["messages_sha256"] = sha(second_raw)
            second_case["request_content_sha256"] = ordered.semantic_digest(second_artifact)
            second_case["source_sha256"] = ordered.selected_source_digest(second_artifact)
            second_case["selected_part_sha256s"] = ordered.selected_part_digests(second_artifact)
            with self.assertRaisesRegex(ValueError, "selected sources"):
                quality.plan_contract(duplicate_source, root)
            reused = ordered.selected_part_digests(first_artifact)[0]
            with self.assertRaisesRegex(ValueError, "overlaps calibration"):
                quality.release.require_evaluation_calibration_disjoint(
                    quality.release.quality_evaluation_source_identities(second_case),
                    {reused},
                )


if __name__ == "__main__":
    unittest.main()
