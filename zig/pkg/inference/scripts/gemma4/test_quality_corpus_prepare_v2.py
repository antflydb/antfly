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
import contextlib
import hashlib
import io
import json
import struct
import tempfile
import unittest
from pathlib import Path

import quality_corpus_prepare_v2 as corpus


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


class QualityCorpusPreparationTests(unittest.TestCase):
    def setUp(self):
        self.old_buckets = corpus.BUCKETS
        self.old_cases = corpus.CASES_PER_CELL
        corpus.BUCKETS = ("document_qa",)
        corpus.CASES_PER_CELL = 4
        self.addCleanup(setattr, corpus, "BUCKETS", self.old_buckets)
        self.addCleanup(setattr, corpus, "CASES_PER_CELL", self.old_cases)
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.manifest = {"schema_version": 1, "stage": "quality_corpus_campaign",
                         "frozen_before_execution": True, "models": []}
        for model_index, model in enumerate(corpus.MODELS):
            self.manifest["models"].append(self.make_model(model, model_index))

    def write(self, name, raw):
        path = self.root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(raw)
        return {"path": name, "sha256": sha(raw)}

    def write_json(self, name, value):
        return self.write(name, json.dumps(value, sort_keys=True, separators=(",", ":")).encode())

    def make_model(self, model, model_index):
        tokenizer = sha((model + " tokenizer").encode())
        template = sha((model + " template").encode())
        sources, source_rows = {}, []
        for index in range(4):
            family = f"{model}-family-{index}"
            fact = f"UNIQUE-{model_index}-{index}-ANSWER".encode()
            ref = self.write(f"{model}/source-{index}.txt", fact)
            sources[family] = (fact, ref)
            source_rows.append({"family_id": family, "family_sha256": sha(fact + b" family"),
                                "repository": "local://fixture", "revision": "%040x" % (model_index + 1),
                                "path": ref["path"], "sha256": ref["sha256"], "license": "Apache-2.0",
                                "content_type": "text", "authorship": "human",
                                "normalized_sha256": sha(fact), "clone_cluster_id": family,
                                "partition": "quality_evaluation"})
        source_manifest = {"schema_version": 1, "stage": "quality_source_manifest", "model": model,
                           "selection_seed": sha((model + " seed").encode()),
                           "snapshot_sha256": sha((model + " snapshot").encode()),
                           "selection_source_families": [], "fitting_source_families": [],
                           "calibration_source_families": [], "sources": source_rows}
        source_ref = self.write_json(f"{model}/sources.json", source_manifest)
        exclusions = {}
        for partition in ("selection", "fitting", "calibration"):
            exclusions[partition] = self.write_json(f"{model}/{partition}-families.json", {
                "schema_version": 1, "stage": "context_source_families", "model": model,
                "partition": partition, "source_family_ids": [],
            })
        cases = []
        for index, (family, (fact, source_ref_row)) in enumerate(sources.items()):
            token_count, evidence_token = 1024, (128, 384, 640, 896)[index]
            document = fact + b" context"
            question = f"question {model} {index}".encode()
            document_ref = self.write(f"{model}/document-{index}.txt", document)
            question_ref = self.write(f"{model}/question-{index}.txt", question)
            request_content_sha256 = corpus.release._coverage_structured_request_identity(document, question)
            pieces = [b"x"] * token_count
            pieces[evidence_token] = fact
            pieces[-1] = question
            rendered = b"".join(pieces)
            offsets, cursor = bytearray(), 0
            for piece in pieces:
                offsets.extend(struct.pack("<II", cursor, cursor + len(piece)))
                cursor += len(piece)
            prompt_raw = b"".join(struct.pack("<q", value) for value in range(token_count))
            prompt_ref = self.write(f"{model}/prompt-{index}.i64", prompt_raw)
            rendered_ref = self.write(f"{model}/rendered-{index}.txt", rendered)
            offsets_ref = self.write(f"{model}/offsets-{index}.u32le", bytes(offsets))
            reference = fact + b"\n"
            reference_ref = self.write(f"{model}/reference-{index}.txt", reference)
            reference_ids = struct.pack("<II", index + 1, index + 101)
            reference_tokens_ref = self.write(f"{model}/reference-{index}.u32le", reference_ids)
            decoded_ref = self.write(f"{model}/decoded-{index}.txt", reference)
            labels = [1] * (token_count - 1) + [0]
            coverage = {
                "schema_version": 2, "stage": "context_payload_block_coverage",
                "request": {"session_id": sha(f"{model}-{index}-session".encode()),
                            "request_index": index, "question_sha256": sha(question),
                            "request_content_sha256": request_content_sha256},
                "document": {"id": f"{model}-{index}-document", "sha256": sha(document),
                             "bucket": "document_qa"},
                "question": {"message_index": 0, "part_index": 1,
                             "source_start": token_count - 1, "source_end": token_count,
                             "token_count": 1, "tokens_sha256": sha(prompt_raw[-8:])},
                "qualified_context_tokens": token_count + 2, "prompt_tokens": token_count,
                "reserved_output_tokens": 2, "admitted_prefill_chunk_rows": 64,
                "prompt_tokens_sha256": sha(prompt_raw),
                "prompt_token_offsets_sha256": sha(bytes(offsets)),
                "rendered_prompt_sha256": sha(rendered),
                "policy": {"keep_prefix_tokens": 0, "keep_recent_tokens": 1,
                           "block_tokens": 512,
                           "compaction_targets": [{"message_index": 0, "part_index": 0}]},
                "payload_parts": [{"message_index": 0, "part_index": 0, "label": 1,
                                   "token_start": 0, "token_end": token_count - 1}],
                "eligible_token_labels": labels,
                "eligible_ranges": [{"message_index": 0, "part_index": 0, "label": 1,
                                     "start": 0, "end": token_count - 1}],
                "eligible_blocks": [{"message_index": 0, "part_index": 0, "label": 1,
                                     "start": 0, "end": 512}],
                "loaded_identity": {"load_generation": 1,
                                    "artifact_sha256": sha((model + " artifact").encode()),
                                    "tokenizer_sha256": tokenizer, "template_sha256": template,
                                    "layout_sha256": sha((model + " layout").encode()),
                                    "implementation_sha256": sha((model + " implementation").encode())},
                "provenance": {"binary_sha256": sha(b"fixture binary"),
                               "source_build_sha256": sha(b"fixture source")},
                "completed": True, "cancelled": False, "elapsed_ns": 1,
                "status_readback_bytes": 4, "kv_readback_bytes": 0,
            }
            coverage_ref = self.write_json(f"{model}/coverage-{index}.json", coverage)
            rendered_start = sum(len(piece) for piece in pieces[:evidence_token])
            receipt = {"schema_version": 1, "stage": "quality_case_construction", "completed": True,
                       "diagnostic_only": False, "case_id": f"{model}-{index}", "model": model,
                       "source_manifest_sha256": source_ref["sha256"],
                       "request_content_sha256": request_content_sha256,
                       "tokenizer_sha256": tokenizer, "template_sha256": template,
                       "rendered_prompt": rendered_ref, "prompt_tokens": prompt_ref,
                       "token_offsets": offsets_ref, "coverage_receipt": coverage_ref,
                       "reference_roundtrip": {"reference": reference_ref, "tokens": reference_tokens_ref,
                                               "decoded": decoded_ref, "token_count": 2,
                                               "tokenizer_sha256": tokenizer},
                       "evidence": [{"source_family_id": family, "source_byte_start": 0,
                                     "source_byte_end": len(fact), "rendered_byte_start": rendered_start,
                                     "rendered_byte_end": rendered_start + len(fact),
                                     "token_start": evidence_token, "token_end": evidence_token + 1,
                                     "fact_sha256": sha(fact)}]}
            receipt_ref = self.write_json(f"{model}/receipt-{index}.json", receipt)
            cases.append({"id": f"{model}-{index}", "bucket": "document_qa",
                          "route": corpus.ROUTES[index % 3], "context_tokens": 1026,
                          "prompt_tokens": token_count, "reserved_output_tokens": 2,
                          "primary_source_family": family, "distractor_source_families": [],
                          "document": document_ref, "question": question_ref,
                          "request_content_sha256": receipt["request_content_sha256"],
                          "construction_receipt": receipt_ref})
        return {"model": model, "tokenizer_sha256": tokenizer, "template_sha256": template,
                "qualified_context_tokens": [1026], "source_manifest": source_ref, "cases": cases,
                "exclusion_manifests": exclusions,
                "baseline_reporting": "report_only_predeclared_no_filtering_v1"}

    def rewrite_receipt(self, model_index, case_index, mutation):
        case = self.manifest["models"][model_index]["cases"][case_index]
        path = self.root / case["construction_receipt"]["path"]
        value = json.loads(path.read_text())
        mutation(value)
        raw = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
        path.write_bytes(raw)
        case["construction_receipt"]["sha256"] = sha(raw)

    def rewrite_coverage(self, model_index, case_index, mutation):
        case = self.manifest["models"][model_index]["cases"][case_index]
        receipt_path = self.root / case["construction_receipt"]["path"]
        receipt = json.loads(receipt_path.read_text())
        coverage_path = self.root / receipt["coverage_receipt"]["path"]
        coverage = json.loads(coverage_path.read_text())
        mutation(coverage)
        coverage_raw = json.dumps(coverage, sort_keys=True, separators=(",", ":")).encode()
        coverage_path.write_bytes(coverage_raw)
        receipt["coverage_receipt"]["sha256"] = sha(coverage_raw)
        receipt_raw = json.dumps(receipt, sort_keys=True, separators=(",", ":")).encode()
        receipt_path.write_bytes(receipt_raw)
        case["construction_receipt"]["sha256"] = sha(receipt_raw)

    def test_validates_independent_source_backed_balanced_campaign(self):
        result = corpus.validate_campaign(self.manifest, self.root)
        self.assertEqual(set(result), set(corpus.MODELS))
        self.assertTrue(all(value["cases"] == 4 for value in result.values()))

    def test_token_offsets_allow_repeated_normalizer_source_span(self):
        raw = struct.pack("<IIIIII", 4, 5, 4, 5, 5, 6)
        self.assertEqual(corpus.u32_pairs(raw, 3, "offset fixture"), [(4, 5), (4, 5), (5, 6)])
        with self.assertRaisesRegex(ValueError, "non-monotonic"):
            corpus.u32_pairs(struct.pack("<IIII", 4, 5, 3, 6), 2, "offset fixture")

    def test_strict_json_rejects_nonfinite_numbers(self):
        with self.assertRaisesRegex(ValueError, "invalid fixture"):
            corpus.decode_json(b'{"value":NaN}', "fixture")

    def test_rejects_evidence_outside_compactable_interval(self):
        def mutation(receipt):
            receipt["evidence"][0]["token_start"] = 0
            receipt["evidence"][0]["token_end"] = 1
        self.rewrite_receipt(0, 0, mutation)
        with self.assertRaisesRegex(ValueError, "source-backed|evidence token"):
            corpus.validate_campaign(self.manifest, self.root)

    def test_rejects_widened_evidence_token_interval_that_moves_quartile(self):
        def mutation(receipt):
            receipt["evidence"][0]["token_start"] -= 1
            receipt["evidence"][0]["token_end"] += 1
        self.rewrite_receipt(0, 1, mutation)
        with self.assertRaisesRegex(ValueError, "minimal runtime-offset enclosure"):
            corpus.validate_campaign(self.manifest, self.root)

    def test_rejects_reference_decode_mismatch(self):
        model = self.manifest["models"][0]
        case = model["cases"][0]
        decoded = self.write("gemma4-e2b/wrong-decoded.txt", b"wrong")
        self.rewrite_receipt(0, 0, lambda receipt: receipt["reference_roundtrip"].update(decoded=decoded))
        with self.assertRaisesRegex(ValueError, "round-trip"):
            corpus.validate_campaign(self.manifest, self.root)

    def test_allows_same_reference_for_independent_cases(self):
        first_case = self.manifest["models"][0]["cases"][0]
        first_receipt = json.loads((self.root / first_case["construction_receipt"]["path"]).read_text())
        shared = copy.deepcopy(first_receipt["reference_roundtrip"])
        self.rewrite_receipt(0, 1, lambda receipt: receipt.update(reference_roundtrip=shared))
        result = corpus.validate_campaign(self.manifest, self.root)
        self.assertEqual(result[corpus.MODELS[0]]["cases"], 4)

    def test_declared_generated_source_is_reported_not_rejected(self):
        model = self.manifest["models"][0]
        path = self.root / model["source_manifest"]["path"]
        value = json.loads(path.read_text())
        value["sources"][0]["authorship"] = "generated"
        raw = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
        path.write_bytes(raw)
        model["source_manifest"]["sha256"] = sha(raw)
        for index in range(len(model["cases"])):
            self.rewrite_receipt(0, index, lambda receipt: receipt.update(source_manifest_sha256=sha(raw)))
        self.assertEqual(corpus.validate_campaign(self.manifest, self.root)[corpus.MODELS[0]]["cases"], 4)

    def test_rejects_unknown_authorship_category(self):
        model = self.manifest["models"][0]
        path = self.root / model["source_manifest"]["path"]
        value = json.loads(path.read_text())
        value["sources"][0]["authorship"] = "unknown"
        raw = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
        path.write_bytes(raw)
        model["source_manifest"]["sha256"] = sha(raw)
        with self.assertRaisesRegex(ValueError, "clone cluster"):
            corpus.validate_campaign(self.manifest, self.root)

    def test_rejects_declared_normalized_digest_not_derived_from_source_bytes(self):
        model = self.manifest["models"][0]
        path = self.root / model["source_manifest"]["path"]
        value = json.loads(path.read_text())
        value["sources"][0]["normalized_sha256"] = sha(b"attacker supplied normalization")
        raw = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
        path.write_bytes(raw)
        model["source_manifest"]["sha256"] = sha(raw)
        for index in range(len(model["cases"])):
            self.rewrite_receipt(0, index, lambda receipt: receipt.update(source_manifest_sha256=sha(raw)))
        with self.assertRaisesRegex(ValueError, "normalized digest mismatch"):
            corpus.validate_campaign(self.manifest, self.root)

    def test_allows_paired_cross_model_declared_clone_cluster(self):
        model = self.manifest["models"][1]
        path = self.root / model["source_manifest"]["path"]
        value = json.loads(path.read_text())
        value["sources"][0]["clone_cluster_id"] = self.manifest["models"][0]["model"] + "-family-0"
        raw = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
        path.write_bytes(raw)
        model["source_manifest"]["sha256"] = sha(raw)
        for index in range(len(model["cases"])):
            self.rewrite_receipt(1, index, lambda receipt: receipt.update(source_manifest_sha256=sha(raw)))
        result = corpus.validate_campaign(self.manifest, self.root)
        self.assertEqual(result[corpus.MODELS[1]]["cases"], 4)

    def test_rejects_selection_family_overlap(self):
        model = self.manifest["models"][0]
        path = self.root / model["source_manifest"]["path"]
        value = json.loads(path.read_text())
        value["selection_source_families"] = [value["sources"][0]["family_id"]]
        raw = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
        path.write_bytes(raw)
        model["source_manifest"]["sha256"] = sha(raw)
        with self.assertRaisesRegex(ValueError, "overlapping source family"):
            corpus.validate_campaign(self.manifest, self.root)

    def test_rejects_mixed_loaded_artifact_provenance_within_model(self):
        self.rewrite_coverage(0, 1, lambda coverage: coverage["loaded_identity"].update(
            artifact_sha256=sha(b"different loaded artifact")))
        with self.assertRaisesRegex(ValueError, "mix loaded artifact or build provenance"):
            corpus.validate_campaign(self.manifest, self.root)

    def test_rejects_source_family_reuse_within_cell(self):
        cases = self.manifest["models"][0]["cases"]
        cases[1]["bucket"] = cases[0]["bucket"]
        cases[1]["primary_source_family"] = cases[0]["primary_source_family"]
        with self.assertRaisesRegex(ValueError, "reused within quality cell"):
            corpus.validate_campaign(self.manifest, self.root)

    def test_baseline_report_is_complete_and_has_no_invented_threshold(self):
        cases = self.manifest["models"][0]["cases"]
        reports = [{"case_id": case["id"], "variant": "exact", "correct": index != 0}
                   for index, case in enumerate(cases)]
        report = corpus.baseline_accuracy_report(cases, reports)
        self.assertIsNone(report["acceptance_threshold"])
        self.assertFalse(report["case_filtering_permitted"])
        self.assertEqual(report["cells"][0]["correct"], 3)
        self.assertEqual(report["cells"][0]["failed_case_ids"], [cases[0]["id"]])

    def test_cli_validates_and_reports_baseline_without_acceptance_threshold(self):
        campaign_path = self.root / "campaign.json"
        campaign_path.write_text(json.dumps(self.manifest, sort_keys=True, separators=(",", ":")))
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            self.assertEqual(0, corpus.main(["validate", "--campaign", str(campaign_path),
                                             "--root", str(self.root)]))
        validation = json.loads(output.getvalue())
        self.assertTrue(validation["completed"])
        cases = self.manifest["models"][0]["cases"]
        reports = [{"case_id": case["id"], "variant": "exact", "correct": True} for case in cases]
        reports_path = self.root / "baseline.json"
        reports_path.write_text(json.dumps(reports, sort_keys=True, separators=(",", ":")))
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            self.assertEqual(0, corpus.main(["baseline-report", "--campaign", str(campaign_path),
                                             "--root", str(self.root), "--model", corpus.MODELS[0],
                                             "--reports", str(reports_path)]))
        baseline = json.loads(output.getvalue())
        self.assertIsNone(baseline["acceptance_threshold"])
        self.assertFalse(baseline["case_filtering_permitted"])


if __name__ == "__main__":
    unittest.main()
