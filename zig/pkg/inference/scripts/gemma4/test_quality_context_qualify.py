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

"""Synthetic protocol tests; these fixtures are not model-quality evidence."""
import copy
import hashlib
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

import quality_context_qualify as quality
import release_attention_matching as release


def sha_bytes(value):
    return hashlib.sha256(value).hexdigest()


class QualityContextTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.identity = {key: sha_bytes(key.encode()) for key in release.SEMANTIC_BINDINGS}
        self.identity.update(dtype="f16_kv_f32_bias", kernel_route="metal_context_optimized")
        self.origin = {key: sha_bytes(("origin-" + key).encode()) for key in release.PROVENANCE_BINDINGS}
        self.origin["binary_sha256"] = release.sha(Path(sys.executable).resolve())
        self.loaded = {key + "_sha256": self.identity[key] for key in release.SEMANTIC_BINDINGS}
        self.loaded["load_generation"] = 7
        self.cases = []
        for index, bucket in enumerate(sorted(release.BUCKETS)):
            directory = self.root / f"case-{index}"
            directory.mkdir()
            document = f"independent document {index}".encode()
            question = f"sealed question {index}".encode()
            files = {
                "document": document,
                "question": question,
                "prompt": document + b"\n" + question,
                "reference": "café\n".encode(),
                "reference_tokens": json.dumps([1, 200 + index]).encode(),
            }
            paths = {}
            for name, contents in files.items():
                suffix = ".json" if name == "reference_tokens" else ".txt"
                path = directory / (name + suffix)
                path.write_bytes(contents)
                paths[name] = path
            self.cases.append({
                "id": f"case-{index}", "bucket": bucket,
                "document_file": str(paths["document"].relative_to(self.root)),
                "document_sha256": sha_bytes(files["document"]),
                "question_file": str(paths["question"].relative_to(self.root)),
                "question_sha256": sha_bytes(files["question"]),
                "request_content_sha256": release._coverage_structured_request_identity(
                    files["document"], files["question"]),
                "prompt_file": str(paths["prompt"].relative_to(self.root)),
                "prompt_sha256": sha_bytes(files["prompt"]),
                "prompt_tokens": 4092, "reserved_output_tokens": 4,
                "reference_file": str(paths["reference"].relative_to(self.root)),
                "reference_sha256": sha_bytes(files["reference"]),
                "reference_tokens_file": str(paths["reference_tokens"].relative_to(self.root)),
                "reference_tokens_sha256": sha_bytes(files["reference_tokens"]),
                "scorer": quality.SCORER, "route": "native",
                "settings_sha256": sha_bytes(b"settings"),
                "variant_settings_sha256": {
                    variant: sha_bytes(variant.encode()) for variant in quality.VARIANTS},
            })
        self.plan = {"schema_version": 3, "qualified_context_tokens": [4096], "cases": self.cases}
        self.min_cases = patch.object(release, "MIN_QUALITY_CASES", 1)
        self.min_cases.start()
        self.addCleanup(self.min_cases.stop)

    def test_unsupported_quality_routes_rejected_before_dispatch(self):
        for route in ("cli", "serial_mixed_batch"):
            with self.subTest(route=route):
                plan = copy.deepcopy(self.plan)
                plan["cases"][0]["route"] = route
                with self.assertRaisesRegex(ValueError, "invalid quality bucket/scorer/route"):
                    quality.flatten_plan(plan, self.root)

    def test_single_reference_token_cannot_qualify_compacted_continuation(self):
        plan = copy.deepcopy(self.plan)
        case = plan["cases"][0]
        path = self.root / case["reference_tokens_file"]
        raw = json.dumps([1]).encode()
        path.write_bytes(raw)
        case["reference_tokens_sha256"] = sha_bytes(raw)
        with self.assertRaisesRegex(ValueError, "invalid reference token ids"):
            quality.flatten_plan(plan, self.root)

    def test_quality_routes_preserve_separate_serving_route_gates(self):
        self.assertTrue({"cli", "serial_mixed_batch"}.issubset(release.ROUTES))
        for route in quality.QUALITY_ROUTES:
            with self.subTest(route=route):
                plan = copy.deepcopy(self.plan)
                plan["cases"][0]["route"] = route
                commands = quality.flatten_plan(plan, self.root)
                self.assertEqual(commands[0]["route"], route)

    def report(self, case, variant):
        token_ids = [1, 2]
        output = "cafe\u0301"
        reference_tokens = json.loads((self.root / case["reference_tokens_file"]).read_text())
        committed = 700 if variant == "attention_matching" else 1000
        return {
            "schema_version": 2, "semantic_identity": self.identity, "provenance": self.origin,
            "loaded_identity": copy.deepcopy(self.loaded), "completed": True,
            "case_id": case["id"], "variant": variant, "bucket": case["bucket"],
            "route": case["route"], "document_sha256": case["document_sha256"],
            "question_sha256": case["question_sha256"],
            "request_content_sha256": case["request_content_sha256"],
            "prompt_sha256": case["prompt_sha256"], "reference_sha256": case["reference_sha256"],
            "reference_tokens_sha256": case["reference_tokens_sha256"],
            "settings_sha256": case["settings_sha256"],
            "variant_settings_sha256": case["variant_settings_sha256"][variant],
            "prompt_tokens": case["prompt_tokens"],
            "reserved_output_tokens": case["reserved_output_tokens"], "scorer": quality.SCORER,
            "exact_fallback": False, "published": variant == "attention_matching",
            "elapsed_ns": 1000, "committed_before_bytes": 1000,
            "committed_after_bytes": committed, "committed_context_bytes": committed,
            "context_peak_bytes": 1200, "live_bytes_before": 10, "live_bytes_after_release": 10,
            "context_allocation_scope": release.CONTEXT_ALLOCATION_SCOPE,
            "context_allocation_scope_version": release.CONTEXT_ALLOCATION_SCOPE_VERSION,
            "context_allocation_scope_complete": True, "full_owner_physical_available": True,
            "model_execution_pool_admission_version": release.CONTEXT_EXECUTION_POOL_ADMISSION_VERSION,
            "model_execution_pool_admitted_backend_bytes": release.CONTEXT_EXECUTION_POOL_ADMITTED_BACKEND_BYTES,
            "model_execution_pool_shared_residency": True, "owner_released": True,
            "owner_post_release_live_bytes": 0,
            "owner_measurement": {"available": True, "released_baseline_bytes": 0},
            "backend_kv_readback_bytes": 0,
            "output_token_ids": token_ids, "output_sha256": quality.token_digest(token_ids),
            "output_utf8": output, "output_utf8_sha256": sha_bytes(output.encode()),
            "reference_sequence_sha256": quality.token_digest(reference_tokens),
            "continuation_forward_steps": len(reference_tokens) - 1,
            "published_compaction_forward_steps": len(reference_tokens) - 1 if variant == "attention_matching" else 0,
            "teacher_forced": [{"sequence_position": index, "token_id": token, "log_probability": probability,
                                "conditioned_on_token_id": reference_tokens[index - 1] if index else None,
                                "source": ("prefill_final_row" if index == 0 else
                                           "published_compacted_context" if variant == "attention_matching" else
                                           "exact_context_continuation")}
                               for index, (token, probability) in enumerate(zip(reference_tokens, (-.1, -.2)))],
        }

    def reports(self):
        return [self.report(case, variant) for case in self.cases for variant in quality.VARIANTS]

    def test_reconstructs_normalized_accuracy_and_raw_token_nll(self):
        result = quality.reconstruct(self.plan, self.reports(), self.identity, self.origin, self.root)
        self.assertTrue(result["diagnostic_only"])
        self.assertFalse(result["serving_qualified"])
        self.assertEqual(len(result["cases"]), len(release.BUCKETS))
        for case in result["cases"]:
            self.assertTrue(case["exact_correct"] and case["candidate_correct"])
            self.assertAlmostEqual(case["exact_nll_sum"], .3)
            self.assertEqual(case["scored_tokens"], 2)
            self.assertNotIn("swapout_growth_bytes", case)

    def test_raw_output_and_teacher_evidence_are_recomputed(self):
        mutations = (
            (lambda row: row["output_token_ids"].__setitem__(0, 9), "output token digest mismatch"),
            (lambda row: row.update(output_utf8="wrong"), "decoded output digest mismatch"),
            (lambda row: row["teacher_forced"][0].update(token_id=9), "teacher token probability"),
            (lambda row: row["teacher_forced"][0].update(log_probability=float("nan")), "teacher token probability"),
            (lambda row: row["teacher_forced"].pop(), "teacher continuation length"),
            (lambda row: row.update(output_token_ids=[], output_sha256=quality.token_digest([])), "output tokens"),
        )
        for mutate, message in mutations:
            reports = self.reports()
            mutate(reports[0])
            with self.subTest(message=message), self.assertRaisesRegex(ValueError, message):
                quality.reconstruct(self.plan, reports, self.identity, self.origin, self.root)

    def test_generation_publication_fallback_allocation_and_resources_fail_closed(self):
        mutations = (
            (lambda rows: rows[1]["loaded_identity"].update(load_generation=8), "changed loaded generation"),
            (lambda rows: rows[1].update(published=False), "requested variant"),
            (lambda rows: rows[1].update(exact_fallback=True), "requested variant"),
            (lambda rows: rows[1].update(backend_kv_readback_bytes=1), "allocation invariant"),
            (lambda rows: rows[1].update(live_bytes_after_release=11), "allocation invariant"),
            (lambda rows: rows[1].update(elapsed_ns=0), "allocation invariant"),
            (lambda rows: rows[1].update(committed_after_bytes=1000), "publication allocation"),
        )
        for mutate, message in mutations:
            reports = self.reports()
            mutate(reports)
            with self.subTest(message=message), self.assertRaisesRegex(ValueError, message):
                quality.reconstruct(self.plan, reports, self.identity, self.origin, self.root)

    def test_teacher_conditional_chain_and_actual_compacted_steps_are_required(self):
        mutations = (
            (lambda row: row.update(reference_sequence_sha256="0" * 64), "reference sequence digest"),
            (lambda row: row.update(continuation_forward_steps=0), "continuation step"),
            (lambda row: row.update(continuation_forward_steps=True), "continuation step"),
            (lambda row: row.update(published_compaction_forward_steps=0), "continuation step"),
            (lambda row: row["teacher_forced"][0].update(conditioned_on_token_id=1), "conditional chain"),
            (lambda row: row["teacher_forced"][1].update(conditioned_on_token_id=999), "conditional chain"),
            (lambda row: row["teacher_forced"][1].update(sequence_position=True), "conditional chain"),
            (lambda row: row["teacher_forced"][1].update(source="prefill_final_row"), "conditional chain"),
            (lambda row: row["teacher_forced"][1].update(source="exact_context_continuation"), "conditional chain"),
        )
        for mutate, message in mutations:
            reports = self.reports()
            mutate(reports[1])
            with self.subTest(message=message), self.assertRaisesRegex(ValueError, message):
                quality.reconstruct(self.plan, reports, self.identity, self.origin, self.root)

    def test_plan_rehashes_inputs_and_requires_balanced_independent_cells(self):
        quality.plan_contract(self.plan, self.root)
        (self.root / self.cases[0]["prompt_file"]).write_text("changed")
        with self.assertRaisesRegex(ValueError, "prompt_file digest mismatch"):
            quality.plan_contract(self.plan, self.root)
        prompt = (self.root / self.cases[0]["document_file"]).read_text() + "\nsealed question 0"
        (self.root / self.cases[0]["prompt_file"]).write_text(prompt)
        plan = copy.deepcopy(self.plan)
        plan["cases"].pop()
        with self.assertRaisesRegex(ValueError, "missing, imbalanced, or underpowered"):
            quality.plan_contract(plan, self.root)
        plan = copy.deepcopy(self.plan)
        plan["cases"][1]["document_sha256"] = plan["cases"][0]["document_sha256"]
        with self.assertRaisesRegex(ValueError, "not independent"):
            quality.plan_contract(plan, self.root)
        plan = copy.deepcopy(self.plan)
        plan["cases"][1]["prompt_sha256"] = plan["cases"][0]["prompt_sha256"]
        with self.assertRaisesRegex(ValueError, "prompts are not independent"):
            quality.plan_contract(plan, self.root)
        plan = copy.deepcopy(self.plan)
        plan["cases"][0]["request_content_sha256"] = "f" * 64
        with self.assertRaisesRegex(ValueError, "structured request identity mismatch"):
            quality.plan_contract(plan, self.root)
        question_path = self.root / self.cases[0]["question_file"]
        question_path.write_text("changed sealed question")
        plan = copy.deepcopy(self.plan)
        plan["cases"][0]["question_sha256"] = release.sha(question_path)
        with self.assertRaisesRegex(ValueError, "structured request identity mismatch"):
            quality.plan_contract(plan, self.root)

    def test_plan_flattens_case_order_then_exact_and_attention_matching(self):
        requests = quality.flatten_plan(self.plan, self.root)
        self.assertEqual(requests, quality.requests(self.plan))
        self.assertEqual([(row["case_id"], row["variant"]) for row in requests],
                         [(case["id"], variant) for case in self.cases for variant in quality.VARIANTS])
        self.assertTrue(all(row["command"] == "quality" and "prompt_file" in row for row in requests))
        observed = quality.driver_report_contract(self.reports()[0], self.identity, self.origin,
                                                  requests[0], self.root)
        self.assertAlmostEqual(observed["nll"], .3)

    def test_paths_and_boolean_teacher_token_ids_fail_closed(self):
        plan = copy.deepcopy(self.plan)
        plan["cases"][0]["prompt_file"] = str(self.root / self.cases[0]["prompt_file"])
        with self.assertRaisesRegex(ValueError, "absolute or traversing"):
            quality.plan_contract(plan, self.root)
        link = self.root / "prompt-link.txt"
        link.symlink_to(self.root / self.cases[0]["prompt_file"])
        plan = copy.deepcopy(self.plan)
        plan["cases"][0]["prompt_file"] = link.name
        with self.assertRaisesRegex(ValueError, "symlinked"):
            quality.plan_contract(plan, self.root)
        reports = self.reports()
        reports[0]["teacher_forced"][0]["token_id"] = True
        reports[0]["teacher_forced"][0]["log_probability"] = -.1
        with self.assertRaisesRegex(ValueError, "teacher token probability"):
            quality.reconstruct(self.plan, reports, self.identity, self.origin, self.root)

    def test_missing_and_duplicate_reports_are_rejected(self):
        reports = self.reports()
        with self.assertRaisesRegex(ValueError, "count mismatch"):
            quality.reconstruct(self.plan, reports[:-1], self.identity, self.origin, self.root)
        reports[-1] = copy.deepcopy(reports[0])
        with self.assertRaisesRegex(ValueError, "duplicate raw quality report"):
            quality.reconstruct(self.plan, reports, self.identity, self.origin, self.root)


if __name__ == "__main__":
    unittest.main()
