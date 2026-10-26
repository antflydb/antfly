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
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

import attention_matching_calibrate as calibrate
import native_repeat_campaign as campaign
import native_repeat_profile as profile
import native_repeat_campaign_resources as resources


class NativeRepeatProfileTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.base = Path(self.tmp.name)
        self.output = self.base / "campaign"
        self.coverage = self.output / "coverage"
        self.coverage.mkdir(parents=True)
        models = self.base / "models"
        model = models / "google/e4b"
        model.mkdir(parents=True)
        self.binary = self.base / "context-tests"
        self.binary.write_bytes(b"source-bound test binary")
        self.binary.chmod(0o755)
        self.build_receipt = self.coverage / "source-build-receipt.json"
        self.build_receipt.write_text("sealed build")
        self.build = {"binary_path": self.binary,
                      "binary_sha256": campaign.sha(self.binary),
                      "receipt_sha256": campaign.sha(self.build_receipt)}
        self.spec = {"schema": campaign.SPEC_SCHEMA,
            "source_build_receipt": {"path": str(self.build_receipt),
                                     "sha256": campaign.sha(self.build_receipt)},
            "model": {"directory": str(model), "models_root": str(models),
                      "model_id": "google/e4b", "artifact_sha256": "1" * 64},
            "coverage_plan": self.external("coverage-plan.json", b"plan"),
            "training_document": self.external("document.txt", b"document"),
            "training_question": self.external("question.txt", b"question"),
            "expected_owners": [{"owner_layer": owner["layer"],
                "source_heads": owner["kv_heads"], "head_dim": owner["head_dim"]}
                for owner in profile.E4B_OWNERS], "capture_stem": "e4b-training"}
        self.spec_path = self.base / "campaign-spec.json"
        campaign.write_new(self.spec_path, self.spec)
        self.spec_sha = campaign.sha(self.spec_path)
        self.authority_path = self.coverage / "training-authority.json"
        campaign.write_new(self.authority_path, {"sealed": "authority"})
        self.source_index_path = self.coverage / "e4b-training-native-head-index.json"
        campaign.write_new(self.source_index_path, {"eligible_block_starts": [1, 513]})
        self.capture_phase_path = self.output / "capture-phase.json"
        campaign._phase(self.output, "capture", self.spec_sha, {
            "training_authority": campaign._phase_ref(self.authority_path)})
        self.binding = self.make_binding()
        self.results_path = self.make_results()
        self.bootstrap_path = self.output / "native-qualification-bootstrap.json"
        campaign.write_new(self.bootstrap_path, self.make_bootstrap())
        self.bootstrap_resource = self.output / "native-qualification-bootstrap-resource.json"
        command = ["/usr/bin/env", "MTL_DEBUG_LAYER=1",
            "ANTFLY_CONTEXT_MODEL_DIR=" + self.spec["model"]["directory"],
            "ANTFLY_CONTEXT_NATIVE_QUALIFICATION_BOOTSTRAP_OUTPUT=" + str(self.bootstrap_path),
            str(self.binary), "--test-filter", profile.BOOTSTRAP_TEST]
        campaign.write_new(self.bootstrap_resource,
                           self.resource(command, profile.BOOTSTRAP_LIMITS, 0))
        self.bootstrap_resource.with_suffix(".stdout").write_bytes(b"")
        self.bootstrap_resource.with_suffix(".stderr").write_text(
            "context_native_qualification_bootstrap_artifact: sha256=" +
            campaign.sha(self.bootstrap_path) + "\n" +
            "context_native_qualification_bootstrap: owners=4 model_forward_steps=0 "
            "candidate_qualification=true serving_qualified=false release_authority=false\n")

    def external(self, name, data):
        path = self.base / name
        path.write_bytes(data)
        return {"path": str(path), "sha256": campaign.sha(path)}

    def ref(self, path, size=False):
        result = {"path": str(path), "sha256": campaign.sha(path)}
        if size:
            result["bytes"] = path.stat().st_size
        return result

    def rewrite_json(self, path, value):
        path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")

    def reseal_first_row(self, results):
        row = results["results"][0]
        seal_path = Path(row["seal"]["path"])
        sealed_row = dict(row)
        sealed_row.pop("seal")
        seal = {"schema": "antfly.context.native.resumable-head-fit-seal.v1",
            "fit_layout": profile.FIT_LAYOUT, "attempt_id": row["attempt_id"],
            "diagnostic_only": True, "serving_qualified": False,
            "fresh_unseen_qualified": False, "campaign_spec_sha256": self.spec_sha,
            "capture_phase": results["capture_phase"],
            "training_authority": results["training_authority"], **sealed_row}
        self.rewrite_json(seal_path, seal)
        row["seal"] = self.ref(seal_path)
        self.rewrite_json(self.results_path, results)

    def resource(self, command, limits, returncode):
        return {"schema": resources.SCHEMA, "rss_scope": resources.RSS_SCOPE,
            "command": command, "limits": limits, "violation": None,
            "returncode": returncode, "pass": returncode == 0,
            "campaign_parent_pid": 2, "child_pgid": 3, "samples": 2,
            "remaining_child_group_rss_mib": 0,
            "remaining_child_group_live_processes": 0,
            "peak_parent_rss_mib": 20, "peak_child_group_rss_mib": 30,
            "peak_combined_rss_mib": 50, "swapout_growth_mib": 0,
            "min_disk_free_mib": 5000, "min_free_percent": 70,
            "initial_free_percent": 80, "elapsed_seconds": 1}

    def make_binding(self):
        coordinates = [{"owner_layer": owner["layer"], "kv_head": head}
                       for owner in profile.E4B_OWNERS for head in range(2)]
        cells, descriptors = [], {}
        for owner in profile.E4B_OWNERS:
            consumers = {5: [5], 11: [11], 17: [17], 23: [23, 29, 35, 41]}[owner["layer"]]
            for head in range(2):
                strata = [{"consumer_layer": layer, "query_head": query,
                           "kv_head": head}
                          for layer in consumers for query in range(head * 4, head * 4 + 4)]
                cells.append({"owner_layer": owner["layer"], "kv_head": head,
                              "source_heads": 2, "head_dim": 512})
                descriptor_path = self.coverage / f"owner{owner['layer']}-descriptor.json"
                if not descriptor_path.exists():
                    descriptor_path.write_text("sealed descriptor")
                descriptors[(owner["layer"], head)] = {
                    "source_descriptor": {"rows": 3073, "heads": 2, "dim": 512,
                                          "strata": strata},
                    "source_descriptor_path": descriptor_path,
                    "source_descriptor_ref": {"sha256": campaign.sha(descriptor_path)},
                    "repeat_owner": {"training_rows": len(strata) * 6,
                        "validation_rows": len(strata) * 2, "strata": strata}}
        capture_identity = {"load_generation": 7, "artifact_sha256": "1" * 64,
            "tokenizer_sha256": "2" * 64, "template_sha256": "3" * 64,
            "layout_sha256": "4" * 64, "implementation_sha256": "5" * 64}
        return {"plan": {"coordinates": coordinates},
            "authority": {"loaded_identity": capture_identity, "cells": cells,
                          "sealer_sha256": "a" * 64,
                          "document": {"sha256": "d" * 64}},
            "repeat": {"query_sampling": "representative_repeat_partition_v1"},
            "source_index": {"eligible_block_starts": [1, 513]},
            "paths": {"source_build_receipt": self.build_receipt,
                      "authority": self.authority_path,
                      "source_index": self.source_index_path},
            "digests": {"source_build_receipt": campaign.sha(self.build_receipt),
                        "authority": campaign.sha(self.authority_path),
                        "source_index": campaign.sha(self.source_index_path)},
            "descriptors": descriptors}

    def make_results(self):
        rows = []
        for owner in profile.E4B_OWNERS:
            groups = 4 if owner["layer"] != 23 else 16
            for head in range(2):
                stem = f"owner{owner['layer']}-head{head}"
                attempt_id = "000001"
                attempt_root = self.coverage / "fit-attempts" / stem / ("attempt-" + attempt_id)
                attempt_root.mkdir(parents=True)
                input_path = attempt_root / "input.bin"
                input_path.write_bytes(b"AMHEAD02" + bytes(128))
                manifest_path = attempt_root / "input.bin.json"
                manifest = {"schema": profile.native.SCHEMA,
                    "recipe": profile.native.RECIPE, "diagnostic_only": True,
                    "evaluation_role": "internal_development",
                    "fresh_unseen_qualified": False, "serving_qualified": False,
                    "full_context_memory_qualified": False,
                    "old_validation_queries_used_for_fit": False,
                    "held_numerics_used_for_selection": False,
                    "training_authority_mode": "native_repeat_v1",
                    "query_sampling": "representative_repeat_partition_v1",
                    "preparer_sha256": campaign.sha(campaign.preparer.__file__),
                    "source_preparer_sha256": campaign.sha(profile.native.__file__),
                    "authority_sealer_sha256": "a" * 64,
                    "source_tensor_order": "logical_row_kv_head_head_dim",
                    "source_kv_heads": 2, "kv_head": head,
                    "owner_layer": owner["layer"], "head_dim": 512,
                    "source_rows": 3073, "training_rows": groups * 6,
                    "validation_rows": groups * 2, "consumer_groups": groups,
                    "prefix_rows": 1, "recent_rows": 2048,
                    "minimum_recent_rows": 2048, "continuation_rows": 32,
                    "retained_row_cap": 6144, "staging_mib": 512,
                    "retention_divisors": [4, 3, 2],
                    "limits": {"max_mass_error": .05, "max_output_rmse": .10,
                               "max_consumer_output_rmse": .10},
                    "input": {"path": str(input_path), "bytes": input_path.stat().st_size,
                              "sha256": campaign.sha(input_path)},
                    "sources": {
                        "authority": {"path": str(self.authority_path),
                            "sha256": campaign.sha(self.authority_path)},
                        "source_build_receipt": {"path": str(self.build_receipt),
                            "sha256": campaign.sha(self.build_receipt)},
                        "source_index": {"path": str(self.source_index_path),
                            "sha256": campaign.sha(self.source_index_path)},
                        "source_descriptor": {
                            "path": str(self.binding["descriptors"][(owner["layer"], head)]["source_descriptor_path"]),
                            "sha256": self.binding["descriptors"][(owner["layer"], head)]["source_descriptor_ref"]["sha256"]}}}
                campaign.write_new(manifest_path, manifest)
                metrics = {"version": 2, "queries": groups * 2,
                    "consumer_groups": groups, "max_mass_error": .01,
                    "output_rmse": .02, "max_consumer_output_rmse": .03}
                candidate = attempt_root / "result.candidate"
                candidate_bytes = 2305 * (4 * 512 + 4)
                with candidate.open("xb") as stream:
                    stream.truncate(candidate_bytes)
                report_path = attempt_root / "result.json"
                report = {"schema": "antfly.context.native.head-qualification.v2",
                    "recipe": profile.native.RECIPE, "accepted_head": True,
                    "outcome": "accepted_head", "terminal_failure": None,
                    "candidate_sha256": campaign.sha(candidate),
                    "candidate_bytes": candidate_bytes, "serving_qualified": False,
                    "full_context_memory_qualified": False,
                    "input_sha256": campaign.sha(input_path), "source_rows": 3073,
                    "head_dim": 512, "training_rows": groups * 6,
                    "validation_rows": groups * 2, "consumer_groups": groups,
                    "exact_prefix_rows": 1, "exact_recent_rows": 2048,
                    "continuation_rows": 32, "retained_row_cap": 6144,
                    "attempts": [{"divisor": 4, "retained_rows": 2305,
                                  "outcome": "accepted_head", "elapsed_ns": 20,
                                  "blocks": [{"block": block, "logical_start": 1 + block * 512,
                                    "retained_rows": 128, "outcome": "fitted", "elapsed_ns": 5,
                                    "solver": {"quality_evaluated": False, "nnls_sweeps": 2,
                                      "selection_steps": 128, "selection_active_solves": 1}}
                                    for block in range(2)], "metrics": metrics,
                                  "reference": dict(metrics)}],
                    "derived_attention_readback_bytes": 1,
                    "validation_readback_bytes": 1,
                    "diagnostic_candidate_readback_bytes": 1}
                campaign.write_new(report_path, report)
                resource_path = attempt_root / "resource.json"
                command = ["/usr/bin/env", "MTL_DEBUG_LAYER=1",
                    "ANTFLY_CONTEXT_NATIVE_HEAD_INPUT=" + str(input_path),
                    "ANTFLY_CONTEXT_NATIVE_HEAD_SHA256=" + campaign.sha(input_path),
                    "ANTFLY_CONTEXT_NATIVE_HEAD_OUTPUT=" + str(attempt_root / "result"),
                    str(self.binary), "--test-filter",
                    "context captured native whole-head Gemma qualification"]
                campaign.write_new(resource_path,
                                   self.resource(command, campaign.FIT_LIMITS, 0))
                stdout, stderr = attempt_root / "resource.stdout", attempt_root / "resource.stderr"
                stdout.write_bytes(b"")
                stderr.write_text("complete\ncontext_native_head_artifacts: report_sha256=" +
                    campaign.sha(report_path) + " candidate_sha256=" + campaign.sha(candidate) +
                    " candidate_bytes=" + str(candidate_bytes) + "\n")
                row = {"owner_layer": owner["layer"], "kv_head": head,
                    "attempt_id": attempt_id, "accepted_head": True, "input": self.ref(input_path),
                    "input_manifest": self.ref(manifest_path), "report": self.ref(report_path),
                    "resource": self.ref(resource_path), "stdout": self.ref(stdout),
                    "stderr": self.ref(stderr), "candidate": self.ref(candidate, True)}
                seal_path = attempt_root / "seal.json"
                campaign.write_new(seal_path, {"schema": "antfly.context.native.resumable-head-fit-seal.v1",
                    "fit_layout": profile.FIT_LAYOUT, "attempt_id": attempt_id,
                    "diagnostic_only": True, "serving_qualified": False,
                    "fresh_unseen_qualified": False, "campaign_spec_sha256": self.spec_sha,
                    "capture_phase": self.ref(self.capture_phase_path),
                    "training_authority": self.ref(self.authority_path), **row})
                row["seal"] = self.ref(seal_path)
                rows.append(row)
        value = {"schema": profile.RESULT_SCHEMA, "fit_layout": profile.FIT_LAYOUT, "completed": True,
            "outcome": "accepted", "evaluation_role": "internal_development",
            "diagnostic_only": True, "serving_qualified": False,
            "release_authority": False, "full_context_memory_qualified": False,
            "fresh_unseen_qualified": False, "campaign_spec_sha256": self.spec_sha,
            "capture_phase": self.ref(self.capture_phase_path),
            "training_authority": self.ref(self.authority_path),
            "accepted_head_count": 8, "head_count": 8,
            "all_heads_accepted": True, "results": rows}
        path = self.coverage / "results.json"
        campaign.write_new(path, value)
        return path

    def make_protocol(self, owner):
        sources = [
            {"kind": "request_question_replay", "partition": "training", "physical_branch": 0},
            {"kind": "fixed_study_prefill", "partition": "training", "physical_branch": 1},
            {"kind": "fixed_study_generated_response", "partition": "training", "physical_branch": 1},
            {"kind": "fixed_study_prefill", "partition": "validation", "physical_branch": 2},
            {"kind": "fixed_study_generated_response", "partition": "validation", "physical_branch": 2}]
        consumers = {5: [5], 11: [11], 17: [17], 23: [23, 29, 35, 41]}[owner["layer"]]
        strata = [{"consumer_layer": layer, "query_head": head, "kv_head": head // 4}
                  for layer in consumers for head in range(8)]
        study = {"template_sha256": "7" * 64,
                 "generation_contract_sha256": "8" * 64,
                 "response_tokens": 256,
                 "generation_mode": "greedy_fixed_length_ignore_eos"}
        value = {"schema_version": 2, "layer": owner["layer"],
            "query_sampling": "source_uniform_partition_v3",
            "accounting": "serial_physical_branch_upper_bound_v2",
            "max_training_rows": 3072, "max_validation_rows": 1024,
            "sources": sources, "owner": {"kv_heads": 2, "query_heads": 8,
                                           "head_dim": 512},
            "strata": strata, "fit": dict(study), "gate": dict(study)}
        return {**value, "sha256": calibrate.runtime_protocol_identity(value)}

    def make_bootstrap(self):
        return {"schema": profile.BOOTSTRAP_SCHEMA, "purpose": profile.PROFILE_STAGE,
            "diagnostic_only": True, "candidate_qualification": True,
            "serving_qualified": False, "release_authority": False,
            "model_forward_steps": 0,
            "loaded_identity": {"load_generation": 11, "artifact_sha256": "1" * 64,
                "tokenizer_sha256": "2" * 64, "template_sha256": "3" * 64,
                "layout_sha256": "4" * 64, "implementation_sha256": "6" * 64},
            "implementation": dict(profile.IMPLEMENTATION), "allocation_page_bytes": 4096,
            "owners": list(profile.E4B_OWNERS),
            "query_protocols": [self.make_protocol(owner) for owner in profile.E4B_OWNERS]}

    def invoke(self, output=True):
        kwargs = {"campaign_spec": self.spec_path, "campaign_spec_sha256": self.spec_sha,
            "results": self.results_path, "results_sha256": campaign.sha(self.results_path),
            "target_bootstrap": self.bootstrap_path,
            "target_bootstrap_sha256": campaign.sha(self.bootstrap_path),
            "target_bootstrap_resource": self.bootstrap_resource,
            "target_bootstrap_resource_sha256": campaign.sha(self.bootstrap_resource),
            "max_prompt_tokens": 3073, "max_output_tokens": 32,
            "minimum_saved_bytes": 4096}
        if output:
            kwargs["output"] = self.output / "native-qualification-profile.json"
        with mock.patch.object(profile.authority, "validate_authority", return_value=self.binding), \
             mock.patch.object(profile.campaign, "validate_build", return_value=self.build), \
             mock.patch.object(profile, "_input_contract"):
            return profile.freeze(**kwargs)

    def test_freezes_complete_eight_head_native_profile_before_unseen(self):
        value = self.invoke()
        self.assertEqual(value["owners"], [{"layer": owner["layer"],
                                            "retained": [128, 128]}
                                           for owner in profile.E4B_OWNERS])
        self.assertEqual(value["implementation"]["engine"], "native_whole_head_v2")
        self.assertTrue(value["frozen_before_unseen_evaluation"])
        self.assertFalse(value["evaluation_opened"])
        self.assertFalse(value["serving_qualified"])
        self.assertEqual(value["profile_semantic_sha256"],
            calibrate.profile_semantic_identity(value["owners"], value["query_protocols"],
                3073, 32, 4096, engine="native_whole_head_v2"))
        self.assertEqual(value["evidence"]["repeat_results_sha256"],
                         campaign.sha(self.results_path))

    def test_empty_head_and_bootstrap_stdout_are_hash_bound_and_accepted(self):
        value = self.invoke()
        self.assertEqual(value["schema"], profile.PROFILE_SCHEMA)
        empty = campaign.sha(self.coverage /
            "fit-attempts/owner5-head0/attempt-000001/resource.stdout")
        self.assertEqual(empty, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
        self.assertEqual(campaign.sha(self.bootstrap_resource.with_suffix(".stdout")), empty)

    def test_validate_freeze_rewalks_source_graph(self):
        self.invoke()
        path = self.output / "native-qualification-profile.json"
        with mock.patch.object(profile.authority, "validate_authority", return_value=self.binding), \
             mock.patch.object(profile.campaign, "validate_build", return_value=self.build), \
             mock.patch.object(profile, "_input_contract"):
            value = profile.validate_freeze(path, campaign.sha(path),
                campaign_spec=self.spec_path, campaign_spec_sha256=self.spec_sha,
                results=self.results_path, results_sha256=campaign.sha(self.results_path),
                target_bootstrap=self.bootstrap_path,
                target_bootstrap_sha256=campaign.sha(self.bootstrap_path),
                target_bootstrap_resource=self.bootstrap_resource,
                target_bootstrap_resource_sha256=campaign.sha(self.bootstrap_resource))
        self.assertEqual(value["profile"]["schema"], profile.PROFILE_SCHEMA)
        self.assertTrue(value["calibration_only"])
        self.assertFalse(value["fresh_unseen_numerical_qualified"])
        self.assertEqual(value["selected_head_count"], 8)
        self.assertEqual(value["calibration_context_tokens"], [3105])
        self.assertEqual(len(value["per_head_numerics"]), 8)
        self.assertEqual(value["per_head_numerics"][0]["selected_divisor"], 4)

    def test_rejects_altered_candidate_even_when_report_still_passes(self):
        candidate = self.coverage / "fit-attempts/owner5-head0/attempt-000001/result.candidate"
        candidate.write_bytes(b"substituted candidate")
        with self.assertRaisesRegex(ValueError, "SHA-256"):
            self.invoke(False)

    def test_rejects_altered_metrics_resource_and_seal(self):
        attempt = self.coverage / "fit-attempts/owner5-head0/attempt-000001"
        targets = [attempt / "result.json", attempt / "resource.json", attempt / "seal.json"]
        for target in targets:
            with self.subTest(target=target.name):
                original = target.read_bytes()
                target.write_bytes(original + b" ")
                with self.assertRaisesRegex(ValueError, "SHA-256"):
                    self.invoke(False)
                target.write_bytes(original)

    def test_rejects_pass_metrics_after_coherent_outer_reseal(self):
        results = json.loads(self.results_path.read_text())
        row = results["results"][0]
        report_path = Path(row["report"]["path"])
        report = json.loads(report_path.read_text())
        report["attempts"][-1]["metrics"]["max_mass_error"] = .2
        report["attempts"][-1]["reference"]["max_mass_error"] = .2
        self.rewrite_json(report_path, report)
        stderr_path = Path(row["stderr"]["path"])
        stderr_path.write_text("complete\ncontext_native_head_artifacts: report_sha256=" +
            campaign.sha(report_path) + " candidate_sha256=" + report["candidate_sha256"] +
            " candidate_bytes=" + str(report["candidate_bytes"]) + "\n")
        row["report"] = self.ref(report_path)
        row["stderr"] = self.ref(stderr_path)
        self.reseal_first_row(results)
        with self.assertRaisesRegex(ValueError, "outcome differs"):
            self.invoke(False)

    def test_rejects_old_shape_report_after_coherent_outer_reseal(self):
        results = json.loads(self.results_path.read_text())
        row = results["results"][0]
        report_path = Path(row["report"]["path"])
        report = json.loads(report_path.read_text())
        for name in ("terminal_failure", "candidate_sha256", "candidate_bytes"):
            report.pop(name)
        for attempt in report["attempts"]:
            attempt.pop("elapsed_ns")
            attempt.pop("blocks")
        self.rewrite_json(report_path, report)
        row["report"] = self.ref(report_path)
        self.reseal_first_row(results)
        with self.assertRaisesRegex(ValueError, "lacks bound candidate"):
            self.invoke(False)

    def test_rejects_candidate_size_corruption_after_coherent_outer_reseal(self):
        results = json.loads(self.results_path.read_text())
        row = results["results"][0]
        candidate_path = Path(row["candidate"]["path"])
        with candidate_path.open("r+b") as stream:
            stream.truncate(candidate_path.stat().st_size - 1)
        report_path = Path(row["report"]["path"])
        report = json.loads(report_path.read_text())
        report["candidate_sha256"] = campaign.sha(candidate_path)
        report["candidate_bytes"] = candidate_path.stat().st_size
        self.rewrite_json(report_path, report)
        stderr_path = Path(row["stderr"]["path"])
        stderr_path.write_text("complete\ncontext_native_head_artifacts: report_sha256=" +
            campaign.sha(report_path) + " candidate_sha256=" + report["candidate_sha256"] +
            " candidate_bytes=" + str(report["candidate_bytes"]) + "\n")
        row["candidate"] = self.ref(candidate_path, True)
        row["report"] = self.ref(report_path)
        row["stderr"] = self.ref(stderr_path)
        self.reseal_first_row(results)
        with self.assertRaisesRegex(ValueError, "size differs"):
            self.invoke(False)

    def test_rejects_changed_terminal_resource_state_after_coherent_reseal(self):
        results = json.loads(self.results_path.read_text())
        row = results["results"][0]
        resource_path = Path(row["resource"]["path"])
        receipt = json.loads(resource_path.read_text())
        receipt["remaining_child_group_live_processes"] = 1
        self.rewrite_json(resource_path, receipt)
        row["resource"] = self.ref(resource_path)
        self.reseal_first_row(results)
        with self.assertRaisesRegex(ValueError, "terminate cleanly"):
            self.invoke(False)

    def test_rejects_altered_authority_and_build_receipt(self):
        for target in (self.authority_path, self.build_receipt):
            with self.subTest(target=target.name):
                original = target.read_bytes()
                target.write_bytes(original + b"stale")
                with self.assertRaisesRegex(ValueError, "SHA-256"):
                    self.invoke(False)
                target.write_bytes(original)

    def test_rejects_missing_or_extra_physical_head(self):
        value = json.loads(self.results_path.read_text())
        value["results"].pop()
        self.results_path.unlink()
        campaign.write_new(self.results_path, value)
        with self.assertRaisesRegex(ValueError, "missing, extra|invalid or incomplete"):
            self.invoke(False)

    def test_rejects_foreign_or_legacy_bootstrap(self):
        value = json.loads(self.bootstrap_path.read_text())
        value["implementation"]["engine"] = "block_v1"
        self.bootstrap_path.unlink()
        campaign.write_new(self.bootstrap_path, value)
        with self.assertRaisesRegex(ValueError, "bootstrap"):
            self.invoke(False)

    def test_rejects_capture_target_identity_substitution(self):
        value = json.loads(self.bootstrap_path.read_text())
        value["loaded_identity"]["layout_sha256"] = "9" * 64
        self.bootstrap_path.unlink()
        campaign.write_new(self.bootstrap_path, value)
        self.bootstrap_resource.with_suffix(".stderr").write_text(
            "context_native_qualification_bootstrap_artifact: sha256=" +
            campaign.sha(self.bootstrap_path) + "\n" +
            "context_native_qualification_bootstrap: owners=4 model_forward_steps=0 "
            "candidate_qualification=true serving_qualified=false release_authority=false\n")
        with self.assertRaisesRegex(ValueError, "capture/target"):
            self.invoke(False)

    def test_bootstrap_rejects_zero_loaded_identity_digests(self):
        original = self.bootstrap_path.read_bytes()
        fields = ("artifact_sha256", "tokenizer_sha256", "template_sha256",
                  "layout_sha256", "implementation_sha256")
        for field in fields:
            with self.subTest(field=field):
                value = json.loads(original)
                value["loaded_identity"][field] = "0" * 64
                self.bootstrap_path.write_text(json.dumps(value))
                with self.assertRaisesRegex(ValueError, "invalid target-native loaded identity"):
                    profile.validate_bootstrap(self.bootstrap_path,
                                               campaign.sha(self.bootstrap_path))
        self.bootstrap_path.write_bytes(original)

    def test_candidate_projection_rejects_missing_malformed_and_zero_evidence(self):
        frozen = self.invoke(False)
        evidence_field = "campaign_spec_sha256"
        cases = []
        missing = copy.deepcopy(frozen)
        missing["evidence"].pop(evidence_field)
        cases.append(("missing", missing))
        malformed = copy.deepcopy(frozen)
        malformed["evidence"][evidence_field] = "not-a-digest"
        cases.append(("malformed", malformed))
        zero = copy.deepcopy(frozen)
        zero["evidence"][evidence_field] = "0" * 64
        cases.append(("zero", zero))
        for name, value in cases:
            with self.subTest(case=name), self.assertRaisesRegex(
                    ValueError, "invalid native qualification evidence"):
                profile.qualification_candidate_view(value)

    def test_rejects_wrong_or_reordered_runtime_protocol(self):
        value = json.loads(self.bootstrap_path.read_text())
        value["query_protocols"][0]["sources"].reverse()
        self.bootstrap_path.unlink()
        campaign.write_new(self.bootstrap_path, value)
        with self.assertRaisesRegex(ValueError, "source protocol"):
            self.invoke(False)

    def test_rejects_missing_bootstrap_terminal_artifact_marker(self):
        self.bootstrap_resource.with_suffix(".stderr").write_text(
            "context_native_qualification_bootstrap: owners=4 model_forward_steps=0 "
            "candidate_qualification=true serving_qualified=false release_authority=false\n")
        with self.assertRaisesRegex(ValueError, "completion evidence"):
            self.invoke(False)

    def test_rejects_row_budget_and_non_page_savings(self):
        with mock.patch.object(profile.authority, "validate_authority", return_value=self.binding), \
             mock.patch.object(profile.campaign, "validate_build", return_value=self.build), \
             mock.patch.object(profile, "_input_contract"):
            kwargs = {"campaign_spec": self.spec_path, "campaign_spec_sha256": self.spec_sha,
                "results": self.results_path, "results_sha256": campaign.sha(self.results_path),
                "target_bootstrap": self.bootstrap_path,
                "target_bootstrap_sha256": campaign.sha(self.bootstrap_path),
                "target_bootstrap_resource": self.bootstrap_resource,
                "target_bootstrap_resource_sha256": campaign.sha(self.bootstrap_resource),
                "max_prompt_tokens": 3073, "max_output_tokens": 32,
                "minimum_saved_bytes": 4097}
            with self.assertRaisesRegex(ValueError, "page-granular"):
                profile.freeze(**kwargs)

    def test_input_contract_reconstructs_full_interleaved_authority_bytes(self):
        source = self.base / "raw-source"
        source.mkdir()
        keys = bytes(range(24))
        values = bytes(range(24, 48))
        training = bytes(range(16))
        validation = bytes(range(16, 32))
        refs = {}
        for name, data in (("keys.f16", keys), ("values.f16", values),
                           ("training.f32", training), ("validation.f32", validation)):
            path = source / name
            path.write_bytes(data)
            refs[name] = {"path": name, "sha256": campaign.sha(path)}
        descriptor = {"rows": 3, "heads": 2, "dim": 2, "scale": .5,
                      "keys": refs["keys.f16"], "values": refs["values.f16"]}
        captured = {"training": refs["training.f32"], "validation": refs["validation.f32"],
                    "validation_group_rows": 1}
        manifest = {"training_rows": 2, "validation_rows": 2, "prefix_rows": 0,
                    "recent_rows": 1, "continuation_rows": 1, "retained_row_cap": 8,
                    "staging_mib": 1}
        selected_keys = profile.native.select_head_rows(keys, 3, 2, 2, 1, 2)
        selected_values = profile.native.select_head_rows(values, 3, 2, 2, 1, 2)
        header = profile.native.HEADER.pack(b"AMHEAD02", 3, 2, 2, 2, 1, 0, 1,
                                            4, 2, 1, 8, 1, .5, 0)
        path = source / "input.bin"
        path.write_bytes(header + selected_keys + selected_values + training + validation)
        profile._input_contract(path, campaign.sha(path), manifest,
                                {"source_descriptor": descriptor, "repeat_owner": captured},
                                source, 1)
        path.write_bytes(path.read_bytes()[:-1] + b"\xff")
        with self.assertRaisesRegex(ValueError, "authority bytes|SHA-256"):
            profile._input_contract(path, campaign.sha(path), manifest,
                                    {"source_descriptor": descriptor, "repeat_owner": captured},
                                    source, 1)


if __name__ == "__main__":
    unittest.main()
