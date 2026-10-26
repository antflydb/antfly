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

"""Release coverage integrity tests; synthetic data never grants qualification."""
import copy
import hashlib
import json
import struct
from pathlib import Path
import tempfile
import unittest

import release_attention_matching as release


class ReleaseBlockCoverageTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        digest = lambda text: hashlib.sha256(text.encode()).hexdigest()
        self.identity = {key: digest(key) for key in release.SEMANTIC_BINDINGS}
        self.origin = {key: digest(key) for key in release.PROVENANCE_BINDINGS}
        loaded = {key + "_sha256": self.identity[key] for key in
                  ("artifact", "tokenizer", "template", "layout", "implementation")}
        loaded["load_generation"] = 1
        document = {"id": "evaluation", "sha256": digest("document"), "bucket": "document_qa"}
        prompt = self.root / "prompt.i64"
        prompt.write_bytes(bytes(8160 * 8))
        question_tokens = self.root / "question.i64"
        question_tokens.write_bytes(bytes(32))
        rendered_prompt = self.root / "rendered.txt"
        rendered_prompt.write_text("renderer-owned prompt")
        offsets = self.root / "prompt-offsets.u32le"
        offsets.write_bytes(struct.pack("<II", 0, 1) * 8160)
        receipt = {
            "schema_version": 2, "stage": "context_payload_block_coverage", "document": document,
            "request": {"session_id": "1" * 64, "request_index": 0,
                        "question_sha256": digest("question"),
                        "request_content_sha256": digest("request-content")},
            "question": {"message_index": 0, "part_index": 1, "source_start": 2000,
                         "source_end": 2004, "token_count": 4,
                         "tokens_sha256": hashlib.sha256(bytes(32)).hexdigest()},
            "qualified_context_tokens": 8192, "prompt_tokens": 8160, "reserved_output_tokens": 32,
            "prompt_tokens_sha256": release.sha(prompt),
            "prompt_token_offsets_sha256": release.sha(offsets),
            "rendered_prompt_sha256": release.sha(rendered_prompt),
            "admitted_prefill_chunk_rows": 64,
            "policy": {"keep_prefix_tokens": 256, "keep_recent_tokens": 512, "block_tokens": 512,
                       "compaction_targets": [{"message_index": 0, "part_index": 0}]},
            "payload_parts": [{"message_index": 0, "part_index": 0, "label": 1,
                               "token_start": 256, "token_end": 1280}],
            "eligible_token_labels": [0] * 256 + [1] * 1024 + [0] * (8160 - 1280),
            "eligible_ranges": [{"message_index": 0, "part_index": 0, "label": 1,
                                 "start": 256, "end": 1280}],
            "eligible_blocks": [{"message_index": 0, "part_index": 0, "label": 1,
                                 "start": start, "end": start + 512} for start in (256, 768)],
            "loaded_identity": loaded,
            "provenance": {key: self.origin[key] for key in ("binary_sha256", "source_build_sha256")},
            "completed": True, "cancelled": False, "elapsed_ns": 100,
            "status_readback_bytes": 4, "kv_readback_bytes": 0,
        }
        path = self.root / "coverage.json"
        path.write_text(json.dumps(receipt))
        contract = self.root / "generation.json"; contract.write_text('{"mode":"greedy_fixed_length_ignore_eos"}')
        fit_template = self.root / "fit.txt"; fit_template.write_text("fit")
        gate_template = self.root / "gate.txt"; gate_template.write_text("gate")
        fit_tokens = self.root / "fit.i64"; fit_tokens.write_bytes(bytes(64))
        gate_tokens = self.root / "gate.i64"; gate_tokens.write_bytes(bytes(64))
        ref = lambda value: {"path": value.name, "sha256": release.sha(value)}
        study = lambda tokens, template: {"tokens": {**ref(tokens), "count": 8},
            "prompt_tokens_sha256": release.sha(prompt), "rendered_suffix_sha256": digest(template.name),
            "template": ref(template), "generation_contract": ref(contract), "response_tokens": 8,
            "generation_mode": "greedy_fixed_length_ignore_eos"}
        capture_inputs = self.root / "capture-inputs.json"
        capture_inputs.write_text(json.dumps({"schema_version": 1, "stage": "context_numerical_capture_inputs",
            "request": receipt["request"], "coverage_receipt": ref(path),
            "rendered_prompt": ref(rendered_prompt),
            "prompt": {**ref(prompt), "count": 8160},
            "prompt_token_offsets": {**ref(offsets), "count": 8160},
            "question": {"tokens": {**ref(question_tokens), "count": 4},
                         "source_start": 2000, "source_end": 2004},
            "fit": study(fit_tokens, fit_template), "gate": study(gate_tokens, gate_template),
            "loaded_identity": loaded, "provenance": receipt["provenance"],
            "completed": True, "diagnostic_only": False, "admitted_prefill_chunk_rows": 64}))
        self.entries = [{"receipt": {"path": path.name, "sha256": release.sha(path)},
                         "prompt_tokens": {"path": prompt.name, "sha256": release.sha(prompt)},
                         "prompt_token_offsets": ref(offsets),
                         "question_tokens": {"path": question_tokens.name,
                                             "sha256": release.sha(question_tokens)},
                         "rendered_prompt": {"path": rendered_prompt.name,
                                             "sha256": release.sha(rendered_prompt)},
                         "capture_inputs": {"path": capture_inputs.name,
                                            "sha256": release.sha(capture_inputs)}}]
        binding = {"receipt_sha256": release.sha(path), "prompt_tokens_sha256": release.sha(prompt),
                   "prompt_token_offsets_sha256": release.sha(offsets),
                   "rendered_prompt_sha256": release.sha(rendered_prompt),
                   "admitted_prefill_chunk_rows": 64,
                   "qualified_context_tokens": 8192, "prompt_tokens": 8160, "reserved_output_tokens": 32}
        self.captures = {f"{owner}-{start}": {"schema_version": 2, "document": copy.deepcopy(document),
            "capture": {"runtime_block_coverage": dict(binding), "evaluation_rows": 1, "rows": 512,
                        "owner_layer": owner, "block_start": start,
                        "query_sources": [{"loaded_identity": dict(loaded)}]}}
            for owner in (4, 9, 14) for start in (256, 768)}

    def audit(self, lengths=(8192,)):
        return release.raw_block_coverage_contract(self.entries, self.root, self.captures,
                                                    self.identity, self.origin, "gemma4-e2b", lengths)

    def test_complete_runtime_cartesian_product_is_recomputed(self):
        self.assertEqual(self.audit(), {"receipts": 1, "blocks": 6, "qualified_context_tokens": [8192]})

    def test_offsets_are_bound_to_runtime_and_every_capture(self):
        self.entries[0]["prompt_token_offsets"]["sha256"] = "0" * 64
        with self.assertRaisesRegex(ValueError, "coverage prompt offsets file digest mismatch"):
            self.audit()
        self.entries[0]["prompt_token_offsets"]["sha256"] = release.sha(self.root / "prompt-offsets.u32le")
        next(iter(self.captures.values()))["capture"]["runtime_block_coverage"]["prompt_token_offsets_sha256"] = "0" * 64
        with self.assertRaisesRegex(ValueError, "runtime coverage binding mismatch"):
            self.audit()

    def test_missing_block_rejected_even_when_every_owner_remains(self):
        self.captures.pop("9-768")
        with self.assertRaisesRegex(ValueError, "incomplete document/owner/runtime-block"):
            self.audit()

    def test_extra_and_duplicate_blocks_rejected(self):
        self.captures["4-768"]["capture"]["block_start"] = 1536
        with self.assertRaisesRegex(ValueError, "incomplete document/owner/runtime-block"):
            self.audit()
        self.captures["4-768"]["capture"]["block_start"] = 256
        with self.assertRaisesRegex(ValueError, "duplicate numerical runtime block"):
            self.audit()

    def test_prompt_bytes_loaded_generation_and_context_length_are_bound(self):
        with self.assertRaisesRegex(ValueError, "misses qualified context length"):
            self.audit((8192, 32768))
        self.captures["4-256"]["capture"]["query_sources"][0]["loaded_identity"]["load_generation"] = 2
        with self.assertRaisesRegex(ValueError, "runtime coverage binding mismatch"):
            self.audit()
        self.captures["4-256"]["capture"]["query_sources"][0]["loaded_identity"]["load_generation"] = 1
        path = self.root / "prompt.i64"
        path.write_bytes(bytes([1]) + path.read_bytes()[1:])
        self.entries[0]["prompt_tokens"]["sha256"] = release.sha(path)
        with self.assertRaisesRegex(ValueError, "prompt token identity mismatch"):
            self.audit()

    def test_legacy_or_teacherless_capture_is_not_runtime_coverage(self):
        for field, value in (("schema_version", 1), ("evaluation_rows", 0)):
            with self.subTest(field=field):
                meta = self.captures["4-256"]
                target = meta if field == "schema_version" else meta["capture"]
                previous = target[field]
                target[field] = value
                with self.assertRaisesRegex(ValueError, "lacks runtime block and teacher coverage"):
                    self.audit()
                target[field] = previous

    def test_same_total_cannot_hide_changed_prompt_and_output_reservation(self):
        binding = self.captures["4-256"]["capture"]["runtime_block_coverage"]
        binding["prompt_tokens"] -= 1
        binding["reserved_output_tokens"] += 1
        with self.assertRaisesRegex(ValueError, "runtime coverage binding mismatch"):
            self.audit()


if __name__ == "__main__":
    unittest.main()
