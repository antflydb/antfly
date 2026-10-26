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

"""Synthetic protocol tests; none of these observations qualifies a model."""
import copy
import hashlib
import json
from pathlib import Path
import struct
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import paired_context_qualify as paired
import release_attention_matching as release
import attention_matching_calibrate as calibrate
import native_repeat_profile as native_profile


class PairedContextTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.identity = {key: hashlib.sha256(key.encode()).hexdigest() for key in release.SEMANTIC_BINDINGS}
        self.identity.update(dtype="f16_kv_f32_bias", kernel_route="metal_context_optimized")
        self.origin = {key: hashlib.sha256(key.encode()).hexdigest() for key in release.PROVENANCE_BINDINGS}
        self.origin["binary_sha256"] = release.sha(Path(sys.executable).resolve())
        prompt = self.root / "prompt.txt"
        prompt.write_text("A synthetic document for collector protocol testing only.")
        document = self.root / "document.txt"
        document.write_text("A synthetic document for collector protocol testing only.")
        question = self.root / "question.txt"
        question.write_text("What does the document say?")
        profile_semantic = hashlib.sha256(b"profile-semantic").hexdigest()
        profile = self.root / "profile.json"
        profile.write_text(json.dumps({"runtime_candidate": {
            "identity": {"implementation_sha256": self.identity["implementation"]},
            "implementation": {"split_decode": True, "flash_prefill": True},
            "profile_semantic_sha256": profile_semantic,
            "max_prompt_tokens": 8188, "max_output_tokens": 4}}))
        self.identity["profile"] = release.sha(profile)
        policy = {"max_context_tokens": 8192, "max_kv_tokens": 4096,
                  "keep_prefix_tokens": 0, "keep_recent_tokens": 1}
        runtime = {"policy": policy, "kernel_flags": {"split_decode": True, "flash_prefill": True},
                   "implementation_sha256": self.identity["implementation"],
                   "profile_file": str(profile), "profile_sha256": self.identity["profile"],
                   "profile_semantic_sha256": profile_semantic}
        cell = {"context_tokens": 8192, "reserved_output_tokens": 4,
                "generation_settings_sha256": release._coverage_generation_settings(4),
                "variant_settings_sha256": {key: release._variant_settings_v2(
                    key, policy, runtime["implementation_sha256"], runtime["profile_sha256"],
                    runtime["profile_semantic_sha256"], True, True) for key in release.PERFORMANCE_VARIANTS}}
        self.plan = {"schema_version": 4, "runtime": runtime, "cells": [cell], "requests": [
            {"pair_id": "pair-0", "warmup": False, "route": "native", "variant": variant,
             "document_file": str(document), "document_sha256": release.sha(document),
             "question_file": str(question), "question_sha256": release.sha(question),
             "request_content_sha256": release._coverage_structured_request_identity(
                 document.read_bytes(), question.read_bytes()),
             "prompt_file": str(prompt), "prompt_sha256": release.sha(prompt),
             "prompt_tokens": 8188, "reserved_output_tokens": 4,
             "settings_sha256": cell["generation_settings_sha256"],
             "variant_settings_sha256": cell["variant_settings_sha256"][variant]}
            for variant in release.PERFORMANCE_VARIANTS]}
        loaded = {key + "_sha256": self.identity[key] for key in release.SEMANTIC_BINDINGS}
        loaded["load_generation"] = 7
        self.report = {"schema_version": 2, "semantic_identity": self.identity, "provenance": self.origin,
                       "loaded_identity": loaded, "completed": True, "exact_fallback": False,
                       "ttft_ns": 1000, "decode_ns": 1000, "elapsed_ns": 3000,
                       "committed_context_bytes": 1000, "backend_kv_readback_bytes": 0,
                       "context_peak_bytes": 1200,
                       "context_allocation_scope": release.CONTEXT_ALLOCATION_SCOPE,
                       "context_allocation_scope_version": release.CONTEXT_ALLOCATION_SCOPE_VERSION,
                       "context_allocation_scope_complete": True, "full_owner_physical_available": True,
                       "model_execution_pool_admission_version": release.CONTEXT_EXECUTION_POOL_ADMISSION_VERSION,
                       "model_execution_pool_admitted_backend_bytes": release.CONTEXT_EXECUTION_POOL_ADMITTED_BACKEND_BYTES,
                       "model_execution_pool_shared_residency": True, "owner_released": True,
                       "owner_post_release_live_bytes": 0,
                       "owner_measurement": {"available": True, "released_baseline_bytes": 0},
                       "swapout_growth_bytes": 0, "live_bytes_before": 100, "live_bytes_after_release": 100,
                       "output_token_ids": [1, 2, 3, 4],
                       "output_sha256": hashlib.sha256(struct.pack("<4I", 1, 2, 3, 4)).hexdigest()}
        for target, value in (("group_rss_mib", 0), ("memory_free_percent", 50), ("swapout_bytes", 0)):
            mock = patch.object(release, target, return_value=value)
            mock.start()
            self.addCleanup(mock.stop)
        mock = patch.object(release.shutil, "disk_usage", return_value=SimpleNamespace(free=20 * 1024**3))
        mock.start()
        self.addCleanup(mock.stop)

    def reports(self):
        return [{**copy.deepcopy(self.report), **request, "published": request["variant"] == "attention_matching"}
                for request in self.plan["requests"]]

    def native_profile(self):
        protocols = []
        consumers = {5: [5], 11: [11], 17: [17], 23: [23, 29, 35, 41]}
        sources = [
            {"kind": "request_question_replay", "partition": "training", "physical_branch": 0},
            {"kind": "fixed_study_prefill", "partition": "training", "physical_branch": 1},
            {"kind": "fixed_study_generated_response", "partition": "training", "physical_branch": 1},
            {"kind": "fixed_study_prefill", "partition": "validation", "physical_branch": 2},
            {"kind": "fixed_study_generated_response", "partition": "validation", "physical_branch": 2},
        ]
        for owner in native_profile.E4B_OWNERS:
            protocol = {"schema_version": 2, "layer": owner["layer"],
                "query_sampling": "source_uniform_partition_v3",
                "accounting": "serial_physical_branch_upper_bound_v2",
                "max_training_rows": 3072, "max_validation_rows": 1024,
                "sources": copy.deepcopy(sources),
                "owner": {"kv_heads": 2, "query_heads": 8, "head_dim": 512},
                "strata": [{"consumer_layer": layer, "query_head": head,
                            "kv_head": head // 4}
                           for layer in consumers[owner["layer"]] for head in range(8)],
                "fit": {"template_sha256": hashlib.sha256(b"fit-template").hexdigest(),
                        "generation_contract_sha256": hashlib.sha256(b"fit-generation").hexdigest(),
                        "response_tokens": 256,
                        "generation_mode": "greedy_fixed_length_ignore_eos"},
                "gate": {"template_sha256": hashlib.sha256(b"gate-template").hexdigest(),
                         "generation_contract_sha256": hashlib.sha256(b"gate-generation").hexdigest(),
                         "response_tokens": 256,
                         "generation_mode": "greedy_fixed_length_ignore_eos"}}
            protocol["sha256"] = calibrate.runtime_protocol_identity(protocol)
            protocols.append(protocol)
        owners = [{"layer": owner["layer"], "retained": [256, 128]}
                  for owner in native_profile.E4B_OWNERS]
        semantic = calibrate.profile_semantic_identity(
            owners, protocols, 8188, 4, 16384, engine="native_whole_head_v2")
        digest = lambda name: hashlib.sha256(name.encode()).hexdigest()
        return {"schema": native_profile.PROFILE_SCHEMA,
            "stage": native_profile.PROFILE_STAGE, "diagnostic_only": True,
            "candidate_qualification": True, "serving_qualified": False,
            "release_authority": False,
            "training_role": "internal_development_calibration",
            "evaluation_opened": False, "frozen_before_unseen_evaluation": True,
            "identity": {"artifact_sha256": self.identity["artifact"],
                         "tokenizer_sha256": self.identity["tokenizer"],
                         "template_sha256": self.identity["template"],
                         "layout_sha256": self.identity["layout"],
                         "implementation_sha256": self.identity["implementation"]},
            "implementation": copy.deepcopy(native_profile.IMPLEMENTATION),
            "allocation_page_bytes": 16384, "max_prompt_tokens": 8188,
            "max_output_tokens": 4, "minimum_saved_bytes": 16384,
            "profile_semantic_sha256": semantic, "owners": owners,
            "query_protocols": protocols,
            "evidence": {name: digest(name) for name in (
                "campaign_spec_sha256", "repeat_results_sha256",
                "training_authority_sha256", "target_bootstrap_sha256",
                "target_bootstrap_resource_sha256", "source_build_sha256",
                "binary_sha256", "freezer_sha256")}}

    def plan_with_native_profile(self, profile):
        path = self.root / "native-profile.json"
        path.write_text(json.dumps(profile))
        plan = copy.deepcopy(self.plan)
        runtime = plan["runtime"]
        runtime["profile_file"] = path.name
        runtime["profile_sha256"] = release.sha(path)
        runtime["profile_semantic_sha256"] = profile.get("profile_semantic_sha256", "f" * 64)
        flags = runtime["kernel_flags"]
        for cell in plan["cells"]:
            cell["variant_settings_sha256"] = {variant: release._variant_settings_v2(
                variant, runtime["policy"], runtime["implementation_sha256"],
                runtime["profile_sha256"], runtime["profile_semantic_sha256"],
                flags["split_decode"], flags["flash_prefill"])
                for variant in release.PERFORMANCE_VARIANTS}
        for request in plan["requests"]:
            request["variant_settings_sha256"] = plan["cells"][0]["variant_settings_sha256"][request["variant"]]
            for field in ("document", "question", "prompt"):
                request[field + "_file"] = Path(request[field + "_file"]).name
        identity = dict(self.identity)
        identity["profile"] = runtime["profile_sha256"]
        return plan, identity

    def collect(self, mutation=""):
        code = ("import sys,json,time\nbase=json.loads(" + repr(json.dumps(self.report)) + ")\n"
                "for line in sys.stdin:\n c=json.loads(line)\n if c['command']=='stop': break\n"
                " r={**base,**c,'published':c['variant']=='attention_matching'}\n"
                " r['committed_context_bytes']=700 if r['published'] else 1000\n"
                + mutation + " time.sleep(.01)\n print(json.dumps(r),flush=True)\n")
        return release.collect_performance_v2([sys.executable, "-c", code], self.identity, self.origin,
                                               self.plan, self.root / "run", timeout=5)

    def audit(self):
        path = self.root / "run/session.json"
        return release.raw_performance_contract({"session": {"path": "run/session.json", "sha256": release.sha(path)}},
                                                self.root, self.identity, self.origin, [8192])

    def test_raw_collection_reconstructs_pair_and_preserves_prompt_bytes(self):
        session = self.collect()
        self.assertTrue(session["completed"])
        self.assertFalse(session["serving_qualified"])
        result = self.audit()
        self.assertEqual(result["samples"][0]["order"], list(release.PERFORMANCE_VARIANTS))
        self.assertEqual(result["samples"][0]["exact"]["decode_tokens_per_second"], 3e6)
        self.assertEqual(result["samples"][0]["attention_matching"]["committed_context_bytes"], 700)
        Path(self.plan["requests"][0]["prompt_file"]).write_text("external input changed after collection")
        self.assertEqual(self.audit(), result)
        with self.assertRaisesRegex(ValueError, "too few performance warmups"):
            release.performance_contract(result, self.identity, self.origin, [8192])

    def test_reply_variant_forgery_is_rejected_during_collection(self):
        with self.assertRaisesRegex(ValueError, "requested variant"):
            self.collect(" r['published']=True\n")
        self.assertFalse(release.read_json(self.root / "run/session.json")["completed"])

    def test_parent_binds_child_session_and_relative_sealed_prompt(self):
        with patch.dict(release.os.environ, {"ANTFLY_CONTEXT_SESSION_ID": "stale",
                                           "ANTFLY_CONTEXT_CAMPAIGN_DIR": "/not/the/campaign"}):
            session = self.collect(
                " import os,pathlib,hashlib\n"
                " assert c['session_id']==os.environ['ANTFLY_CONTEXT_SESSION_ID']\n"
                " assert 'prompt_file' not in c\n"
                " root=pathlib.Path(os.environ['ANTFLY_CONTEXT_CAMPAIGN_DIR'])\n"
                " assert not pathlib.Path(c['document_file']).is_absolute()\n"
                " assert not pathlib.Path(c['question_file']).is_absolute()\n"
                " assert hashlib.sha256((root/c['document_file']).read_bytes()).hexdigest()==c['document_sha256']\n"
                " assert hashlib.sha256((root/c['question_file']).read_bytes()).hexdigest()==c['question_sha256']\n")
        self.assertTrue(session["completed"])
        self.assertEqual(len(self.audit()["samples"]), 1)

    def test_plan_rejects_repeated_variant_or_cross_variant_prompt_drift(self):
        plan = copy.deepcopy(self.plan)
        plan["requests"].append(copy.deepcopy(plan["requests"][0]))
        with self.assertRaisesRegex(ValueError, "variant repeated"):
            paired.plan_contract(plan)
        plan = copy.deepcopy(self.plan)
        plan["requests"][1]["prompt_sha256"] = "f" * 64
        with self.assertRaisesRegex(ValueError, "paired prompt changed"):
            paired.plan_contract(plan)
        sealed = copy.deepcopy(self.plan)
        sealed["runtime"]["profile_file"] = "profile.json"
        for request in sealed["requests"]:
            for field in ("document", "question", "prompt"):
                request[field + "_file"] = Path(request[field + "_file"]).name
        sealed["requests"][0]["request_content_sha256"] = "f" * 64
        with self.assertRaisesRegex(ValueError, "structured request identity mismatch"):
            paired.plan_contract(sealed, self.root, self.identity)

    def test_plan_derives_settings_and_rejects_legacy_or_runtime_drift(self):
        legacy = copy.deepcopy(self.plan); legacy["schema_version"] = 3; legacy.pop("runtime")
        with self.assertRaisesRegex(ValueError, "invalid paired performance plan"):
            paired.plan_contract(legacy)

        def rehash(plan):
            runtime = plan["runtime"]
            flags = runtime["kernel_flags"]
            for cell in plan["cells"]:
                cell["variant_settings_sha256"] = {variant: release._variant_settings_v2(
                    variant, runtime["policy"], runtime["implementation_sha256"],
                    runtime["profile_sha256"], runtime["profile_semantic_sha256"],
                    flags["split_decode"], flags["flash_prefill"])
                    for variant in release.PERFORMANCE_VARIANTS}
            for request in plan["requests"]:
                request["variant_settings_sha256"] = plan["cells"][0]["variant_settings_sha256"][request["variant"]]

        for field in ("max_context_tokens", "max_kv_tokens", "keep_prefix_tokens", "keep_recent_tokens"):
            changed = copy.deepcopy(self.plan); changed["runtime"]["policy"][field] += 1
            with self.subTest(field=field), self.assertRaisesRegex(
                    ValueError, "variant settings are not derived"):
                paired.plan_contract(changed)

        sealed = copy.deepcopy(self.plan)
        sealed["runtime"]["profile_file"] = "profile.json"
        for field in ("split_decode", "flash_prefill"):
            changed = copy.deepcopy(sealed)
            changed["runtime"]["kernel_flags"][field] = not changed["runtime"]["kernel_flags"][field]
            rehash(changed)
            with self.subTest(field=field), self.assertRaisesRegex(
                    ValueError, "frozen profile runtime authority mismatch"):
                paired.plan_contract(changed, self.root, self.identity)
        changed = copy.deepcopy(sealed); changed["runtime"]["profile_semantic_sha256"] = "f" * 64
        rehash(changed)
        with self.assertRaisesRegex(ValueError, "frozen profile runtime authority mismatch"):
            paired.plan_contract(changed, self.root, self.identity)
        changed = copy.deepcopy(sealed); changed["runtime"]["implementation_sha256"] = "f" * 64
        rehash(changed)
        with self.assertRaisesRegex(ValueError, "semantic identity mismatch"):
            paired.plan_contract(changed, self.root, self.identity)

    def test_plan_accepts_strict_native_candidate_and_preserves_legacy_dispatch(self):
        profile = self.native_profile()
        plan, identity = self.plan_with_native_profile(profile)
        self.assertIs(paired.plan_contract(plan, self.root, identity), plan)
        legacy = copy.deepcopy(self.plan)
        legacy["runtime"]["profile_file"] = "profile.json"
        for request in legacy["requests"]:
            for field in ("document", "question", "prompt"):
                request[field + "_file"] = Path(request[field + "_file"]).name
        self.assertIs(paired.plan_contract(legacy, self.root, self.identity), legacy)

    def test_plan_rejects_invalid_native_semantic_flags_and_protocol(self):
        cases = []
        changed = self.native_profile()
        changed["profile_semantic_sha256"] = "f" * 64
        cases.append((changed, "semantic digest mismatch"))
        changed = self.native_profile()
        changed["serving_qualified"] = True
        cases.append((changed, "candidate flags"))
        changed = self.native_profile()
        changed["query_protocols"][0]["sources"].reverse()
        cases.append((changed, "source protocol changed"))
        for profile, message in cases:
            plan, identity = self.plan_with_native_profile(profile)
            with self.subTest(message=message), self.assertRaisesRegex(ValueError, message):
                paired.plan_contract(plan, self.root, identity)

    def test_raw_output_reservation_and_generation_identity_are_checked(self):
        for mutate, message in (
            (lambda report: report["output_token_ids"].pop(), "full output tokens"),
            (lambda report: report["output_token_ids"].__setitem__(0, 99), "output digest mismatch"),
            (lambda report: report["loaded_identity"].update(profile_sha256="f" * 64), "loaded serving identity mismatch"),
            (lambda report: report.update(live_bytes_after_release=101), "leaked allocations"),
        ):
            reports = self.reports()
            mutate(reports[0])
            with self.subTest(message=message), self.assertRaisesRegex(ValueError, message):
                paired.reconstruct(self.plan, reports, self.identity, self.origin)

    def test_rehashed_raw_command_cannot_change_declared_pair_order(self):
        self.collect()
        session_path = self.root / "run/session.json"
        session = release.read_json(session_path)
        journal = self.root / "run" / session["files"]["journal"]["path"]
        rows = list(release.journal_records(journal))
        next(row for row in rows if row["event"] == "command")["variant"] = "attention_matching"
        journal.write_text("".join(json.dumps(row) + "\n" for row in rows))
        session["files"]["journal"]["sha256"] = release.sha(journal)
        session_path.write_text(json.dumps(session))
        with self.assertRaisesRegex(ValueError, "command/plan mismatch"):
            self.audit()


if __name__ == "__main__":
    unittest.main()
