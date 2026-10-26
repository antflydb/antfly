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

"""Fail-closed context allocation scope and mixed-batch receipt tests."""
import copy
import hashlib
import struct
import unittest

import release_attention_matching as release


def scoped_report():
    return {
        "context_allocation_scope": release.CONTEXT_ALLOCATION_SCOPE,
        "context_allocation_scope_version": release.CONTEXT_ALLOCATION_SCOPE_VERSION,
        "context_allocation_scope_complete": True,
        "full_owner_physical_available": True,
        "model_execution_pool_admission_version": release.CONTEXT_EXECUTION_POOL_ADMISSION_VERSION,
        "model_execution_pool_admitted_backend_bytes": release.CONTEXT_EXECUTION_POOL_ADMITTED_BACKEND_BYTES,
        "model_execution_pool_shared_residency": True,
        "committed_context_bytes": 700,
        "context_peak_bytes": 1000,
        "owner_released": True,
        "owner_post_release_live_bytes": 0,
        "live_bytes_before": 10,
        "live_bytes_after_release": 10,
    }


def allocation(kind, members):
    return {"member_request_indices": members, "receipt": {
        "released": True,
        "context_allocation_scope_version": release.CONTEXT_ALLOCATION_SCOPE_VERSION,
        "context_batch_scope_version": 1,
        "context_allocation_scope_complete": True,
        "full_owner_physical_available": True,
        "context_batch_kind": kind,
        "context_batch_expected_items": len(members),
        "context_batch_completed_items": len(members),
        "model_execution_pool_admission_version": release.CONTEXT_EXECUTION_POOL_ADMISSION_VERSION,
        "model_execution_pool_admitted_backend_bytes": release.CONTEXT_EXECUTION_POOL_ADMITTED_BACKEND_BYTES,
        "measurement": {"available": True, "committed_live_bytes": 700,
                        "request_peak_bytes": 1000, "released_baseline_bytes": 0},
        "post_release_live_bytes": 0,
    }}


def mixed_report():
    return {
        "schema_version": 2,
        "completed": True,
        "operation": "mixed_batch_scope",
        "scope_only": True,
        "serving_qualified": False,
        "model_execution_pool": {
            "admission_version": release.CONTEXT_EXECUTION_POOL_ADMISSION_VERSION,
            "admitted_backend_bytes": release.CONTEXT_EXECUTION_POOL_ADMITTED_BACKEND_BYTES,
            "shared_once": True,
        },
        "allocations": [
            allocation("exact_siblings", [0, 2]),
            allocation("attention_matching_singleton", [1]),
        ],
    }


def mixed_serving_report():
    siblings = []
    items = []
    loaded = {key + "_sha256": hashlib.sha256(key.encode()).hexdigest()
              for key in release.SEMANTIC_BINDINGS}
    loaded["load_generation"] = 9
    for index, variant in enumerate(("exact", "attention_matching", "exact")):
        tokens = [index + 1, index + 2]
        expected = {
            "variant": variant,
            "document_file": f"document-{index}.txt", "document_sha256": f"{index + 1:064x}",
            "question_file": f"question-{index}.txt", "question_sha256": f"{index + 2:064x}",
            "request_content_sha256": f"{index + 3:064x}", "prompt_sha256": f"{index + 4:064x}",
            "prompt_tokens": 1024 + index, "reserved_output_tokens": len(tokens),
            "settings_sha256": f"{index + 5:064x}", "variant_settings_sha256": f"{index + 6:064x}",
        }
        siblings.append(expected)
        items.append({"request_index": index, **expected, "loaded_identity": copy.deepcopy(loaded),
            "elapsed_ns": 100, "ttft_ns": 60, "decode_ns": 40, "backend_kv_readback_bytes": 0,
            "published": variant == "attention_matching", "exact_fallback": False,
            "output_token_ids": tokens,
            "output_sha256": hashlib.sha256(b"".join(struct.pack("<I", value) for value in tokens)).hexdigest(),
            "allocation_group_index": 1 if variant == "attention_matching" else 0})
    report = mixed_report()
    report.update(operation="mixed_batch", scope_only=False, serving_qualified=True,
                  session_id="1" * 64, request_index=7, items=items)
    return report, siblings


class ContextAllocationScopeTests(unittest.TestCase):
    def test_single_request_scope_requires_every_versioned_binding(self):
        report = scoped_report()
        self.assertEqual(release.context_allocation_scope_contract(report)["committed_context_bytes"], 700)
        mutations = (
            ("scope label", lambda row: row.update(context_allocation_scope="process_rss"), "versioned"),
            ("scope version", lambda row: row.update(context_allocation_scope_version=0), "versioned"),
            ("scope complete", lambda row: row.update(context_allocation_scope_complete=False), "versioned"),
            ("pool version", lambda row: row.update(model_execution_pool_admission_version=0), "pool"),
            ("pool bytes", lambda row: row.update(model_execution_pool_admitted_backend_bytes=0), "pool"),
            ("pool shared", lambda row: row.update(model_execution_pool_shared_residency=False), "pool"),
            ("release", lambda row: row.update(owner_post_release_live_bytes=None), "baseline"),
            ("peak", lambda row: row.update(context_peak_bytes=699), "context peak"),
        )
        for name, mutation, error in mutations:
            with self.subTest(name=name):
                invalid = copy.deepcopy(report)
                mutation(invalid)
                with self.assertRaisesRegex(ValueError, error):
                    release.context_allocation_scope_contract(invalid)

    def test_mixed_scope_binds_exact_group_and_attention_singleton(self):
        report = mixed_report()
        result = release.mixed_batch_scope_contract(report, ["exact", "attention_matching", "exact"])
        self.assertEqual(len(result), 2)

    def test_mixed_scope_rejects_membership_failure_and_duplicate_shared_residency(self):
        cases = (
            ("exact members", lambda row: row["allocations"][0].update(member_request_indices=[0]), "incomplete|membership"),
            ("AM singleton", lambda row: row["allocations"][1].update(member_request_indices=[1, 2]), "receipt|membership"),
            ("failed item", lambda row: row["allocations"][0]["receipt"].update(context_batch_completed_items=1), "incomplete"),
            ("stale scope", lambda row: row["allocations"][1]["receipt"].update(context_allocation_scope_version=0), "incomplete"),
            ("different pool", lambda row: row["allocations"][1]["receipt"].update(model_execution_pool_admitted_backend_bytes=1), "execution pool"),
            ("top-level duplicate", lambda row: row["model_execution_pool"].update(shared_once=False), "duplicates"),
        )
        for name, mutation, error in cases:
            with self.subTest(name=name):
                report = mixed_report()
                mutation(report)
                with self.assertRaisesRegex(ValueError, error):
                    release.mixed_batch_scope_contract(report, ["exact", "attention_matching", "exact"])

    def test_mixed_serving_binds_every_item_to_one_group_without_duplicating_bytes(self):
        report, siblings = mixed_serving_report()
        result = release.mixed_batch_serving_contract(report, siblings)
        self.assertEqual(len(result["items"]), 3)
        self.assertEqual(len(result["allocations"]), 2)

    def test_mixed_serving_rejects_missing_forged_or_misjoined_items(self):
        cases = (
            ("missing item", lambda row: row["items"].pop(), "incomplete"),
            ("duplicate item", lambda row: row["items"][2].update(request_index=0), "request binding"),
            ("wrong group", lambda row: row["items"][1].update(allocation_group_index=0), "membership"),
            ("missing measurement", lambda row: row["allocations"][0]["receipt"]["measurement"].update(
                available=False), "allocation measurement"),
            ("forged output", lambda row: row["items"][1]["output_token_ids"].append(99), "completion"),
            ("identity drift", lambda row: row["items"][2]["loaded_identity"].update(load_generation=10),
             "identity changed"),
            ("publication drift", lambda row: row["items"][1].update(published=False), "completion"),
        )
        for name, mutation, error in cases:
            with self.subTest(name=name):
                report, siblings = mixed_serving_report()
                mutation(report)
                with self.assertRaisesRegex(ValueError, error):
                    release.mixed_batch_serving_contract(report, siblings)


if __name__ == "__main__":
    unittest.main()
