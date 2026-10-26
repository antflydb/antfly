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

"""Adversarial raw fault protocol tests. Synthetic reports never qualify models."""
import copy
import hashlib
import json
import struct
import sys
import unittest

import release_attention_matching as release
import test_release_serving_collector as fixtures


class FailureCollectorTests(unittest.TestCase):
    def setUp(self):
        fixtures.ServingCollectorTests.setUp(self)
        tokens = [11, 29, 313]
        digest = hashlib.sha256(b"".join(struct.pack("<I", token) for token in tokens)).hexdigest()
        snapshot = {"epoch": 3, "logical_tokens": 8160,
                    "content_sha256": "c" * 64, "descriptors_sha256": "d" * 64,
                    "kv_sha256": "e" * 64, "diagnostic_kv_readback_bytes": 4096}
        self.report.update(route="native", fault="cancel_fit", loaded_generation_before=7,
                           loaded_generation_after=7, request_generation=7,
                           injection={"kind": "cancel_fit", "stage": "fit",
                                      "mechanism": "cooperative_checkpoint_after_partial_work",
                                      "completed_units": 1, "total_units": 2},
                           context_disposition="original_context_unchanged", partial_context=None,
                           original_context={"before": snapshot, "after": dict(snapshot)},
                           maintenance_kv_readback_bytes=0,
                           exact_output_tokens=tokens, observer_output_tokens=list(tokens),
                           exact_output_sha256=digest, observer_output_sha256=digest,
                           events=[{"name": name, "monotonic_ns": i, "live_bytes": 100,
                                    **({"fault": "cancel_fit"} if name == "fault_injected" else {})}
                                   for i, name in enumerate(("request_started", "fault_injected",
                                                            "original_context_verified", "observer_verified", "request_released"))])
        (self.root / "document.txt").write_bytes(b"sealed document")
        (self.root / "question.txt").write_bytes(b"sealed question")
        document = (self.root / "document.txt").read_bytes()
        question = (self.root / "question.txt").read_bytes()
        self.plan = {
            "schema_version": 1,
            "policy": {"max_context_tokens": 8192, "max_kv_tokens": 4096,
                       "keep_prefix_tokens": 0, "keep_recent_tokens": 256},
            "runtime": {"optimize_decode": True, "optimize_prefill": True,
                        "admitted_prefill_chunk_rows": 64,
                        "profile_semantic_sha256": "9" * 64},
            "request": {"document_file": "document.txt", "document_sha256": hashlib.sha256(document).hexdigest(),
                        "question_file": "question.txt", "question_sha256": hashlib.sha256(question).hexdigest(),
                        "request_content_sha256": release._coverage_structured_request_identity(document, question),
                        "prompt_sha256": "a" * 64, "prompt_tokens": 8160,
                        "reserved_output_tokens": 32},
            "faults": list(release.FAILURE_ORDER),
        }

    def collect(self, directory="run", mutation=""):
        code = ("import sys,json,time\nr=json.loads(" + repr(json.dumps(self.report)) + ")\ngeneration=7\noriginal=r['original_context']\n"
                "for line in sys.stdin:\n c=json.loads(line)\n if c['command']=='stop': break\n"
                " r.update(session_id=c['session_id'],request_index=c['request_index'],route=c['route'],fault=c['fault'])\n"
                " r.update({k:c[k] for k in ('variant','document_file','document_sha256','question_file','question_sha256','request_content_sha256','prompt_sha256','prompt_tokens','reserved_output_tokens','settings_sha256','variant_settings_sha256')})\n"
                " r['injection'].update(kind=c['fault'],stage={\"cancel_prefill\":\"prefill\",\"cancel_replay\":\"replay\",\"cancel_fit\":\"fit\",\"cancel_publish\":\"publish\",\"oom\":\"admission\",\"nonfinite\":\"fit\",\"gpu_failure\":\"gpu\",\"reload\":\"reload\"}[c['fault']],mechanism={\"cancel_prefill\":\"cooperative_checkpoint_after_partial_work\",\"cancel_replay\":\"cooperative_checkpoint_after_partial_work\",\"cancel_fit\":\"cooperative_checkpoint_after_partial_work\",\"cancel_publish\":\"transaction_boundary_cancellation\",\"oom\":\"request_local_allocator_failure\",\"nonfinite\":\"device_fitter_nonfinite_status\",\"gpu_failure\":\"simulated_gpu_completion_error\",\"reload\":\"model_manager_generation_transition\"}[c['fault']],completed_units=1,total_units=1 if c['fault'] in (\"cancel_publish\",\"oom\",\"nonfinite\",\"gpu_failure\",\"reload\") else 2)\n"
                " r.update(context_disposition='partial_prefill_destroyed',original_context=None,partial_context={**original['before'],'logical_tokens':1}) if c['fault']=='cancel_prefill' else r.update(context_disposition='preparation_allocation_rolled_back',original_context=None,partial_context=None) if c['fault']=='oom' else r.update(context_disposition='model_generation_reloaded',original_context=None,partial_context=None) if c['fault']=='reload' else r.update(context_disposition='original_context_unchanged',original_context=original,partial_context=None)\n"
                " r['events'][2]['name']='partial_context_destroyed' if c['fault']=='cancel_prefill' else 'allocation_rollback_verified' if c['fault']=='oom' else 'model_generation_reloaded' if c['fault']=='reload' else 'original_context_verified'\n"
                " r['events'][1]['fault']=c['fault']\n"
                " r['loaded_generation_before']=generation\n"
                " generation+=int(c['fault']=='reload')\n r['loaded_generation_after']=generation\n"
                " r['request_generation']=generation\n r['loaded_identity']['load_generation']=generation\n"
                " if c['fault']=='reload':\n"
                "  r.update(stale_selection_rejected=True,pre_reload_exact_generation=generation-1,attention_matching_generation=generation,exact_after_generation=generation,pre_reload_exact_output_tokens=list(r['exact_output_tokens']),pre_reload_exact_output_sha256=r['exact_output_sha256'],attention_matching_output_tokens=list(r['observer_output_tokens']),attention_matching_output_sha256=r['observer_output_sha256'],exact_after_output_tokens=list(r['exact_output_tokens']),exact_after_output_sha256=r['exact_output_sha256'])\n"
                "  r['events']=[{'name':n,'monotonic_ns':i,'live_bytes':100,**({'fault':'reload'} if n=='fault_injected' else {})} for i,n in enumerate(('request_started','fault_injected','model_generation_reloaded','stale_selection_rejected','fresh_attention_matching_published','observer_verified','request_released'))]\n"
                + mutation + " time.sleep(.01)\n print(json.dumps(r),flush=True)\n")
        return release.collect_failures_v2([sys.executable, "-c", code], self.identity, self.origin,
                                           self.plan, self.root, self.root / directory, timeout=5)

    def audit(self, directory="run"):
        path = self.root / directory / "session.json"
        return release.raw_failure_contract({"session": {"path": str(path.relative_to(self.root)),
                                                          "sha256": release.sha(path)}},
                                            self.root, self.identity, self.origin, [8192])

    def test_raw_fault_collection_covers_sites_and_reload_generation(self):
        receipt = self.collect()
        self.assertTrue(receipt["completed"])
        self.assertFalse(receipt["serving_qualified"])
        result = self.audit()
        self.assertEqual({trial["report"]["fault"] for trial in result["trials"]}, release.FAILURES)
        self.assertEqual(result["load_generation"], 8)

    def test_failure_plan_binds_inputs_policy_and_canonical_order(self):
        requests = release.failure_plan_contract(self.plan, self.root, self.identity)
        self.assertEqual([request["fault"] for request in requests], list(release.FAILURE_ORDER))
        changed = copy.deepcopy(self.plan)
        changed["faults"][0], changed["faults"][1] = changed["faults"][1], changed["faults"][0]
        with self.assertRaisesRegex(ValueError, "canonical complete fault order"):
            release.failure_plan_contract(changed, self.root, self.identity)
        changed = copy.deepcopy(self.plan)
        changed["request"]["document_sha256"] = "0" * 64
        with self.assertRaisesRegex(ValueError, "input file identity"):
            release.failure_plan_contract(changed, self.root, self.identity)
        changed = copy.deepcopy(self.plan)
        changed["policy"]["keep_recent_tokens"] += 1
        self.assertNotEqual(requests[0]["variant_settings_sha256"],
                            release.failure_plan_contract(changed, self.root, self.identity)[0]["variant_settings_sha256"])

    def test_raw_tokens_context_and_order_reject_forged_success(self):
        attacks = (
            ("tokens", lambda row: row["observer_output_tokens"].append(1), "digest does not match"),
            ("context", lambda row: row["original_context"]["after"].update(epoch=4), "context changed"),
            ("kv", lambda row: row["original_context"]["after"].update(kv_sha256="f" * 64), "context changed"),
            ("maintenance readback", lambda row: row.update(maintenance_kv_readback_bytes=1), "maintenance readback"),
            ("clock", lambda row: row["events"][0].update(monotonic_ns=-1), "raw failure event"),
            ("order", lambda row: row["events"].reverse(), "raw failure event"),
            ("injection", lambda row: row["events"][1].update(fault="oom"), "matching injection"),
            ("boundary", lambda row: row["injection"].update(completed_units=2), "injection boundary"),
            ("commit", lambda row: row["events"].insert(2, {"name": "publication_commit", "monotonic_ns": 1, "live_bytes": 100}), "published after"),
            ("leak", lambda row: row.update(live_bytes_after_release=101), "leaked"),
            ("generation", lambda row: row.update(loaded_generation_after=8), "generation transition"),
        )
        for name, mutate, error in attacks:
            with self.subTest(name=name):
                report = copy.deepcopy(self.report)
                mutate(report)
                with self.assertRaisesRegex(ValueError, error):
                    release.failure_report_contract(report, self.identity, self.origin)

    def test_cancel_prefill_requires_destroyed_partial_context(self):
        report = copy.deepcopy(self.report)
        report.update(fault="cancel_prefill",
                      injection={"kind": "cancel_prefill", "stage": "prefill",
                                 "mechanism": "cooperative_checkpoint_after_partial_work",
                                 "completed_units": 64, "total_units": 8160},
                      context_disposition="partial_prefill_destroyed",
                      partial_context={**report["original_context"]["before"], "logical_tokens": 64},
                      original_context=None)
        report["events"][1]["fault"] = "cancel_prefill"
        report["events"][2]["name"] = "partial_context_destroyed"
        release.failure_report_contract(report, self.identity, self.origin)
        report["original_context"] = {"before": report["partial_context"], "after": report["partial_context"]}
        with self.assertRaisesRegex(ValueError, "partial prefill"):
            release.failure_report_contract(report, self.identity, self.origin)

    def test_oom_requires_preparation_allocation_rollback(self):
        report = copy.deepcopy(self.report)
        report.update(fault="oom", injection={"kind": "oom", "stage": "admission",
                                               "mechanism": "request_local_allocator_failure",
                                               "completed_units": 1, "total_units": 1},
                      context_disposition="preparation_allocation_rolled_back",
                      original_context=None, partial_context=None)
        report["events"][1]["fault"] = "oom"
        report["events"][2]["name"] = "allocation_rollback_verified"
        release.failure_report_contract(report, self.identity, self.origin)
        report["partial_context"] = copy.deepcopy(self.report["original_context"]["before"])
        with self.assertRaisesRegex(ValueError, "allocation did not roll back"):
            release.failure_report_contract(report, self.identity, self.origin)

    def test_collector_rejects_injection_mismatch_and_retains_failure_receipt(self):
        with self.assertRaisesRegex(ValueError, "matching injection"):
            self.collect(mutation=" r['events'][1]['fault']='oom'\n")
        receipt = release.read_json(self.root / "run/session.json")
        self.assertFalse(receipt["completed"])
        self.assertEqual(receipt["trial_count"], 0)

    def test_collector_rejects_child_request_binding_mismatch(self):
        with self.assertRaisesRegex(ValueError, "report/request binding"):
            self.collect(mutation=" r['question_sha256']='0'*64\n")

    def test_raw_fault_command_tampering_rejects_after_rehash(self):
        self.collect()
        fixtures.ServingCollectorTests.rewrite_journal(self, lambda rows: next(
            row for row in rows if row["event"] == "command").update(fault="oom"))
        with self.assertRaisesRegex(ValueError, "failure command sequence"):
            self.audit()

    def test_raw_fault_samples_reject_swap_growth(self):
        self.collect()
        fixtures.ServingCollectorTests.rewrite_journal(self, lambda rows: next(
            row for row in rows if row["event"] == "resource").update(swapout_bytes=1))
        with self.assertRaisesRegex(ValueError, "resource guard"):
            self.audit()


if __name__ == "__main__":
    unittest.main()
