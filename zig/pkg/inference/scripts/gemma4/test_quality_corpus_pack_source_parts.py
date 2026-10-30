# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Packing must preserve the actual ordered source corpus and its labels."""
import hashlib
from pathlib import Path
import tempfile
import unittest

import quality_corpus_pack_v8_tool_history as pack


class SourcePartsTests(unittest.TestCase):
    def test_tool_primary_excerpt_can_start_at_cited_bytes_without_fabricating_context(self):
        raw = ("before " * 300 + "quoted-evidence" + " after" * 300).encode()
        start = raw.index(b"quoted-evidence"); end = start + len(b"quoted-evidence")
        left, right, excerpt = pack._utf8_evidence_excerpt(raw, start, end, 1024, 0)
        self.assertEqual(left, start)
        self.assertEqual(right - left, 1024)
        self.assertEqual(excerpt, raw[left:right])
        self.assertTrue(excerpt.startswith(b"quoted-evidence"))
        ending = b"prefix " * 300 + b"quoted-evidence"
        left, right, excerpt = pack._utf8_evidence_excerpt(ending, len(ending)-15, len(ending), 1024, 0)
        self.assertEqual(excerpt, b"quoted-evidence")
        self.assertEqual(right, len(ending))
        with self.assertRaisesRegex(ValueError, "prefix limit"):
            pack._utf8_evidence_excerpt(raw, start, end, 1024, -1)

    def test_single_message_retains_sources_on_both_sides_of_evidence(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            sources = {}
            payloads = {"primary": b"answer-evidence\n" + b"p" * 8192,
                        "distractor-a": b"a" * 8192,
                        "distractor-b": b"b" * 8192}
            for family, raw in payloads.items():
                path = family + ".txt"
                (root / path).write_bytes(raw)
                digest = hashlib.sha256(raw).hexdigest()
                sources[family] = {
                    "authorship": "human", "clone_cluster_id": family,
                    "content_type": "markdown", "family_id": family,
                    "family_sha256": digest, "license": "Apache-2.0",
                    "normalized_sha256": digest, "partition": "quality_evaluation",
                    "path": path, "repository": "fixture", "revision": "fixture",
                    "sha256": digest,
                }
            for bucket in ("document_qa", "exact_retrieval", "code"):
                case = {"id": bucket, "bucket": bucket,
                        "source": {"path": "primary.txt", "sha256": sources["primary"]["sha256"],
                                   "byte_start": 0, "byte_end": 15},
                        "intended_compactable_quartile": 1,
                        "distractor_order_seed": "fixed-source-order",
                        "question_utf8": "Return the cited source evidence.",
                        "reference_utf8": "answer-evidence"}
                work = {"case": case, "primary_family": "primary",
                        "distractor_families": ["distractor-a", "distractor-b"],
                        "available_source_bytes": sum(map(len, payloads.values()))}
                for insertion in range(3):
                    with self.subTest(bucket=bucket, insertion=insertion):
                        value = pack.compose_candidate(work, sources, root,
                            work["available_source_bytes"], insertion_index=insertion)
                        messages = value["messages"]["messages"]
                        self.assertEqual(len(messages), 1)
                        parts = messages[0]["parts"]
                        self.assertEqual(sum(part["selected"] for part in parts), 3)
                        self.assertEqual({row["family_id"] for row in value["segments"]}, set(sources))
                        self.assertEqual({row["part_index"] for row in value["segments"]}, {0, 1, 2})
                        for segment in value["segments"]:
                            self.assertEqual(segment["message_index"], 0)
                            part = parts[segment["part_index"]]
                            self.assertTrue(part["selected"])
                            self.assertEqual(part["text"], part["text"].strip())
                            self.assertIn("family=" + segment["family_id"] + " ", part["text"])
                        self.assertEqual(parts[-1], {"text": case["question_utf8"], "selected": False})
                        primary = next(row for row in value["segments"] if row["family_id"] == "primary")
                        self.assertEqual(primary["part_index"], insertion)
                        self.assertEqual(primary["label"], value["primary_label"])
                        rendered = "".join(part["text"].strip() for part in parts).encode()
                        self.assertEqual(rendered.count(value["evidence_part_utf8"]), 1)
                        evidence = value["evidence_part_utf8"][value["evidence_part_byte_start"]:
                                                                value["evidence_part_byte_end"]]
                        self.assertEqual(evidence, b"answer-evidence")
                # Reordering for a target quartile must leave the source
                # excerpts themselves unchanged at a partial byte budget.
                variants = [pack.compose_candidate(work, sources, root, 12288,
                            insertion_index=index) for index in range(3)]
                ranges = [{row["family_id"]: (row["source_byte_start"], row["source_byte_end"])
                           for row in value["segments"]} for value in variants]
                self.assertEqual(ranges[0], ranges[1])
                self.assertEqual(ranges[1], ranges[2])
                divided = pack.compose_candidate(work, sources, root, 12288,
                    insertion_index=0, insertion_subpart_units=32768)
                unsplit = variants[0]
                self.assertEqual({row["family_id"] for row in divided["segments"]}, set(sources))
                self.assertEqual(len(divided["segments"]), len(unsplit["segments"]) + 1)
                for family in sources:
                    pieces = sorted((row for row in divided["segments"] if row["family_id"] == family),
                                    key=lambda row: row["source_byte_start"])
                    original = next(row for row in unsplit["segments"] if row["family_id"] == family)
                    self.assertEqual(pieces[0]["source_byte_start"], original["source_byte_start"])
                    self.assertEqual(pieces[-1]["source_byte_end"], original["source_byte_end"])
                    for left, right in zip(pieces, pieces[1:]):
                        self.assertEqual(left["source_byte_end"], right["source_byte_start"])
                    for segment in pieces:
                        part = divided["messages"]["messages"][segment["message_index"]]["parts"][segment["part_index"]]
                        self.assertEqual(part["text"].encode()[segment["part_byte_start"]:segment["part_byte_end"]],
                            payloads[family][segment["source_byte_start"]:segment["source_byte_end"]])
                for insertion in range(3):
                    guarded = pack.compose_candidate(work, sources, root, 12288,
                        insertion_index=insertion, prefix_padding_units=64, suffix_padding_units=64)
                    guarded_messages = guarded["messages"]["messages"]
                    self.assertEqual(len(guarded_messages[0]["parts"]), 4)
                    for segment in guarded["segments"]:
                        part = guarded_messages[segment["message_index"]]["parts"][segment["part_index"]]
                        raw = part["text"].encode()
                        body = raw[segment["part_byte_start"]:segment["part_byte_end"]]
                        source = payloads[segment["family_id"]][segment["source_byte_start"]:
                                                                   segment["source_byte_end"]]
                        self.assertEqual(body, source)
                    part = guarded["evidence_part_utf8"]
                    self.assertEqual(part[guarded["evidence_part_byte_start"]:
                                          guarded["evidence_part_byte_end"]], b"answer-evidence")
                    self.assertFalse(guarded_messages[-1]["parts"][-1]["selected"])

                for family in ("distractor-a", "distractor-b"):
                    self.assertGreater(ranges[0][family][1] - ranges[0][family][0], 1024)


    def test_native_quartiles_cover_the_eligible_interval_before_the_2048_row_tail(self):
        for quartile, start in enumerate((128, 1568, 3008, 4448)):
            self.assertEqual(pack.evidence_quartile(start + 32, start + 48, 7936, 128, 2048), quartile)
            self.assertLess(start + 48, 7936 - 2048)
        self.assertEqual(pack.evidence_quartile(5000, 5010, 7936, 128, 2048), 3)
        self.assertEqual(pack.evidence_quartile(5000, 5010, 7936), 2)
        with self.assertRaisesRegex(ValueError, "quartile interval"):
            pack.evidence_quartile(100, 110, 2048, 128, 2048)

    def test_native_placement_splits_a_distractor_when_whole_parts_skip_a_quartile(self):
        base = {"target_prompt_tokens": 32512, "intended_quartile": 2,
                "byte_budget": 89366, "maximum_byte_budget": 607514,
                "insertion_index": 7, "maximum_insertion_index": 11,
                "phase": "padding", "padding_units": 12, "samples": []}
        moved = pack.next_candidate_control(base, 32512, 1,
            evidence_token_start=14664, evidence_token_end=14681,
            protected_prefix=128, protected_recent=2048)
        self.assertEqual(moved["insertion_index"], 8)
        middle = pack.next_candidate_control(dict(moved, phase="padding", padding_units=12),
            32512, 3, evidence_token_start=23869, evidence_token_end=23886,
            protected_prefix=128, protected_recent=2048)
        self.assertEqual(middle["insertion_index"], 7)
        self.assertGreater(middle["insertion_subpart_units"], 0)
        self.assertLess(middle["insertion_subpart_units"], 65536)

    def test_protected_evidence_adds_bounded_guards_without_accepting_ineligible_tokens(self):
        base = {"target_prompt_tokens": 7936, "intended_quartile": 0,
                "byte_budget": 12000, "maximum_byte_budget": 30000,
                "insertion_index": 1, "maximum_insertion_index": 2,
                "phase": "source", "padding_units": 0, "samples": []}
        prefix = pack.next_candidate_control(base, 8132, 0,
            evidence_eligible=False, evidence_token_start=473, evidence_token_end=492)
        self.assertEqual(prefix["prefix_padding_units"], 55)
        self.assertEqual(prefix["suffix_padding_units"], 0)
        self.assertEqual(prefix["samples"], [])
        recent = pack.next_candidate_control(base, 7936, 3,
            evidence_eligible=False, evidence_token_start=7510, evidence_token_end=7550)
        self.assertEqual(recent["suffix_padding_units"], 142)
        native = pack.next_candidate_control(base, 7936, 3,
            evidence_eligible=False, evidence_token_start=6000, evidence_token_end=6020,
            protected_prefix=128, protected_recent=2048)
        self.assertEqual(native["insertion_index"], 0)
        self.assertNotIn("suffix_padding_units", native)
        # First normalize an oversized source. Reserving against its old
        # evidence position would consume a guard for bytes that will move.
        native_long = pack.next_candidate_control(base, 8000, 3,
            evidence_eligible=False, evidence_token_start=6090, evidence_token_end=6100,
            protected_prefix=128, protected_recent=2048)
        self.assertNotIn("suffix_padding_units", native_long)
        self.assertLess(native_long["byte_budget"], base["byte_budget"])
        tail = dict(base, target_prompt_tokens=32512, intended_quartile=3,
                    byte_budget=97449, maximum_byte_budget=607537,
                    insertion_index=9, maximum_insertion_index=11)
        earlier = pack.next_candidate_control(tail, 32512, 3, evidence_eligible=False,
            evidence_token_start=31694, evidence_token_end=31707,
            protected_prefix=128, protected_recent=2048)
        self.assertEqual(earlier["insertion_index"], 8)
        self.assertNotIn("suffix_padding_units", earlier)
        self.assertFalse(earlier.get("completed", False))
        divided = pack.next_candidate_control(earlier, 32512, 1,
            evidence_token_start=14664, evidence_token_end=14681,
            protected_prefix=128, protected_recent=2048)
        self.assertEqual(divided["insertion_index"], 8)
        self.assertGreater(divided["insertion_subpart_units"], 0)
        with self.assertRaisesRegex(ValueError, "outside protected windows"):
            pack.next_candidate_control(base, 7936, 0, evidence_eligible=False,
                evidence_token_start=1000, evidence_token_end=1020)
        with self.assertRaisesRegex(ValueError, "exhausted"):
            pack.next_candidate_control(dict(base, prefix_padding_units=760), 7936, 0,
                evidence_eligible=False, evidence_token_start=473, evidence_token_end=492)


if __name__ == "__main__":
    unittest.main()
