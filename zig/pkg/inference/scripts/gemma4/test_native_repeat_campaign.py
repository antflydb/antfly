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

import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import types
import unittest
from unittest import mock

import native_repeat_campaign as campaign
import native_repeat_campaign_resources as resources


class CampaignTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.output = self.root / "output"
        self.output.mkdir()
        models = self.root / "models"
        model = models / "google/e4b"
        model.mkdir(parents=True)
        self.binary = self.root / "context-tests"
        self.binary.write_bytes(b"binary")
        self.binary.chmod(0o755)
        self.build = self.root / "build.json"
        self.build.write_bytes(b"build")
        self.document = self.root / "source-document.txt"
        self.question = self.root / "source-question.txt"
        self.document.write_text("document")
        self.question.write_text("question")
        self.plan = self.root / "source-plan.json"
        self.plan_value = {"schema_version": 2, "policy": {
            "keep_prefix_tokens": 512, "keep_recent_tokens": 1024,
            "max_context_tokens": 8192, "max_kv_tokens": 6144},
            "cases": [{"case_id": "explicit-training-case", "bucket": "document_qa",
                "route": "native", "prompt_tokens": 8160,
                "reserved_output_tokens": 32, "request_content_sha256": "9" * 64,
                "document_file": "ignored-document-name", "document_sha256": campaign.sha(self.document),
                "question_file": "ignored-question-name", "question_sha256": campaign.sha(self.question)}]}
        campaign.write_new(self.plan, self.plan_value)
        self.spec = {"schema": campaign.SPEC_SCHEMA,
            "source_build_receipt": {"path": str(self.build), "sha256": campaign.sha(self.build)},
            "model": {"directory": str(model), "artifact_sha256": "1" * 64,
                      "models_root": str(models), "model_id": "google/e4b"},
            "coverage_plan": {"path": str(self.plan), "sha256": campaign.sha(self.plan)},
            "training_document": {"path": str(self.document), "sha256": campaign.sha(self.document)},
            "training_question": {"path": str(self.question), "sha256": campaign.sha(self.question)},
            "expected_owners": [{"owner_layer": 5, "source_heads": 2, "head_dim": 512}],
            "capture_stem": "e4b-training"}
        self.spec_path = self.root / "spec.json"
        campaign.write_new(self.spec_path, self.spec)
        self.spec_sha = campaign.sha(self.spec_path)

    def make_identity_phase(self):
        value = {"schema": "antfly.context.native.identity-bootstrap.v1",
            "diagnostic_only": True, "training_source_authority": False,
            "release_authority": False, "serving_qualified": False,
            "load_generation": 7, "model_forward_steps": 0,
            "identity": {"artifact": "1" * 64, "tokenizer": "2" * 64,
                "template": "3" * 64, "layout": "4" * 64,
                "implementation": "5" * 64, "profile": "0" * 64,
                "dtype": "f16_kv_f32_bias", "kernel_route": "metal_context_optimized"},
            "owners": json.loads(json.dumps(self.spec["expected_owners"]))}
        bootstrap = self.output / "identity-bootstrap.json"
        campaign.write_new(bootstrap, value)
        campaign._phase(self.output, "identity", self.spec_sha,
                        {"bootstrap": campaign._phase_ref(bootstrap)})

    def test_spec_and_local_plan_bind_explicit_inputs_without_rewriting_policy(self):
        value = campaign.load_spec(self.spec_path, self.spec_sha)
        normalized, root, path, source, document, question = \
            campaign._local_coverage_plan(value, self.output)
        self.assertEqual(normalized["policy"], self.plan_value["policy"])
        self.assertEqual(normalized["cases"][0]["case_id"], "explicit-training-case")
        self.assertEqual(normalized["cases"][0]["request_content_sha256"], "9" * 64)
        self.assertEqual(normalized["cases"][0]["document_file"], "document.txt")
        self.assertEqual(normalized["cases"][0]["question_file"], "question.txt")
        self.assertEqual(document.read_bytes(), self.document.read_bytes())
        self.assertEqual(question.read_bytes(), self.question.read_bytes())
        self.assertEqual(source, self.plan)
        self.assertEqual(json.loads(path.read_text()), normalized)
        with self.assertRaises(FileExistsError):
            campaign._local_coverage_plan(value, self.output)

    def test_plan_input_hash_mismatch_fails_before_copy(self):
        self.spec["training_question"]["sha256"] = "0" * 64
        path = self.root / "bad-spec.json"
        campaign.write_new(path, self.spec)
        value = campaign.load_spec(path, campaign.sha(path))
        with self.assertRaisesRegex(ValueError, "differ from coverage plan"):
            campaign._local_coverage_plan(value, self.output)
        self.assertFalse((self.output / "coverage-inputs").exists())

    def test_identity_contract_requires_explicit_geometry_and_artifact(self):
        value = {"schema": "antfly.context.native.identity-bootstrap.v1",
            "diagnostic_only": True, "training_source_authority": False,
            "release_authority": False, "serving_qualified": False,
            "load_generation": 1, "model_forward_steps": 0,
            "identity": {"artifact": "1" * 64, "tokenizer": "2" * 64,
                "template": "3" * 64, "layout": "4" * 64,
                "implementation": "5" * 64, "profile": "0" * 64,
                "dtype": "f16_kv_f32_bias", "kernel_route": "metal_context_optimized"},
            "owners": json.loads(json.dumps(self.spec["expected_owners"]))}
        self.assertIs(campaign._identity_contract(value, self.spec), value)
        value["owners"][0]["source_heads"] = 1
        with self.assertRaisesRegex(ValueError, "owner geometry"):
            campaign._identity_contract(value, self.spec)

    def test_headroom_profile_is_fixed_and_failure_is_sealed(self):
        self.assertEqual(campaign.HEADROOM_PROFILE, {
            "profile": "loader_64pct_two_sample_v1", "required_free_percent": 64,
            "samples": 2, "interval_seconds": 10})
        with mock.patch.object(campaign.subprocess, "check_output",
                               return_value="System-wide memory free percentage: 63%"):
            with self.assertRaisesRegex(ValueError, "64%"):
                campaign._headroom(self.output, "identity")
        receipt = json.loads((self.output / "identity-headroom.json").read_text())
        self.assertFalse(receipt["pass"])
        self.assertEqual(receipt["required_free_percent"], 64)

    def test_plan_phase_freezes_explicit_owner_geometry_before_capture(self):
        coverage = self.output / "coverage"
        coverage.mkdir()
        session = coverage / "session.json"
        campaign.write_new(session, {"completed": True, "coverage_receipts": [{}]})
        inputs = coverage / "capture-inputs-000000.json"
        inputs.write_text("inputs")
        build = coverage / "source-build-receipt.json"
        build.write_text("build")
        self.make_identity_phase()
        plan_root = self.output / "coverage-inputs"
        plan_root.mkdir()
        normalized = plan_root / "coverage-plan.json"
        document = plan_root / "document.txt"
        question = plan_root / "question.txt"
        for path, data in ((normalized, "plan"), (document, "document"), (question, "question")):
            path.write_text(data)
        campaign._phase(self.output, "coverage", self.spec_sha,
            {"identity_phase": campaign._phase_ref(self.output / "identity-phase.json"),
             "normalized_plan": campaign._phase_ref(normalized),
             "training_document": campaign._phase_ref(document),
             "training_question": campaign._phase_ref(question),
             "coverage_session": campaign._phase_ref(session)},
            coverage_directory=str(coverage))
        observed = {}
        def create_plan(**kwargs):
            observed.update(kwargs)
            campaign.write_new(kwargs["output"], {"coordinates": [
                {"owner_layer": 5, "kv_head": 0},
                {"owner_layer": 5, "kv_head": 1}]})
        with mock.patch.object(campaign.authority, "create_plan", side_effect=create_plan):
            campaign.phase_plan(self.spec, self.spec_sha, self.output)
        self.assertEqual(observed["owners"], [{"owner_layer": 5, "source_heads": 2}])
        self.assertEqual(observed["output_stem"], "e4b-training")
        self.assertEqual(observed["capture_inputs"], inputs)
        self.assertTrue((coverage / "training-plan.json").is_file())

    def test_plan_phase_rejects_substitute_absolute_coverage_directory(self):
        substitute = self.root / "substitute-coverage"
        substitute.mkdir()
        session = substitute / "session.json"
        campaign.write_new(session, {"completed": True, "coverage_receipts": [{}]})
        campaign._phase(self.output, "coverage", self.spec_sha,
            {"coverage_session": campaign._phase_ref(session)},
            coverage_directory=str(substitute))
        with self.assertRaisesRegex(ValueError, "canonical"):
            campaign.phase_plan(self.spec, self.spec_sha, self.output)

    def test_capture_command_binds_frozen_plan_inputs_and_exact_limits(self):
        coverage = self.output / "coverage"
        coverage.mkdir()
        plan = coverage / "training-plan.json"
        campaign.write_new(plan, {"frozen": True})
        inputs = coverage / "capture-inputs-000000.json"
        inputs.write_text("inputs")
        build_receipt = coverage / "source-build-receipt.json"
        build_receipt.write_text("build")
        coverage_phase = self.output / "coverage-phase.json"
        coverage_phase.write_text("coverage")
        campaign._phase(self.output, "plan", self.spec_sha, {
            "coverage_phase": campaign._phase_ref(coverage_phase),
            "training_plan": campaign._phase_ref(plan),
            "capture_inputs": campaign._phase_ref(inputs),
            "coverage_build_receipt": campaign._phase_ref(build_receipt)})
        seen = {}
        def run(command, resource, limits, numerical=False):
            seen.update(command=command, resource=resource, limits=limits,
                        numerical=numerical)
            Path(resource).write_text("resource")
            Path(resource).with_suffix(".stdout").write_text("")
            Path(resource).with_suffix(".stderr").write_text("capture")
            (coverage / "e4b-training-native-head-index.json").write_text("source")
            (coverage / "e4b-training-native-repeat-index.json").write_text("repeat")
            return {"returncode": 0}
        def seal(**kwargs):
            seen["seal"] = kwargs
            Path(kwargs["output"]).write_text("authority")
        def headroom(output, phase):
            campaign.write_new(Path(output) / (phase + "-headroom.json"), {"pass": True})
        with mock.patch.object(campaign, "_headroom", side_effect=headroom), \
             mock.patch.object(campaign, "validate_build", return_value={"binary_path": self.binary}), \
             mock.patch.object(campaign, "_run", side_effect=run), \
             mock.patch.object(campaign.authority, "seal_authority", side_effect=seal), \
             mock.patch.object(campaign.authority, "validate_authority", return_value={}):
            campaign.phase_capture(self.spec, self.spec_sha, self.output)
        command = seen["command"]
        self.assertIn("ANTFLY_CONTEXT_NATIVE_REPEAT_TRAINING_PLAN_SHA256=" + campaign.sha(plan), command)
        self.assertIn("ANTFLY_CONTEXT_CAPTURE_INPUT_MANIFEST_SHA256=" + campaign.sha(inputs), command)
        self.assertIn("ANTFLY_CONTEXT_CAMPAIGN_PEAK_BYTES=8589934592", command)
        self.assertEqual(seen["limits"], campaign.CAPTURE_LIMITS)
        self.assertFalse(seen["numerical"])
        self.assertEqual(seen["seal"]["plan_sha256"], campaign.sha(plan))

    def test_resource_contract_distinguishes_numerical_one_from_failure(self):
        command = ["/usr/bin/env", "binary"]
        receipt = self.output / "resource.json"
        value = {"schema": resources.SCHEMA, "rss_scope": resources.RSS_SCOPE,
            "command": command, "limits": campaign.FIT_LIMITS, "violation": None,
            "returncode": 1, "pass": False, "campaign_parent_pid": 2,
            "child_pgid": 3, "samples": 2, "remaining_child_group_rss_mib": 0,
            "remaining_child_group_live_processes": 0, "peak_parent_rss_mib": 20,
            "peak_child_group_rss_mib": 30, "peak_combined_rss_mib": 50,
            "swapout_growth_mib": 0, "min_disk_free_mib": 5000,
            "min_free_percent": 70, "initial_free_percent": 80,
            "elapsed_seconds": 1}
        campaign.write_new(receipt, value)
        self.assertEqual(campaign._resource_contract(
            receipt, command, campaign.FIT_LIMITS, numerical=True)["returncode"], 1)
        with self.assertRaisesRegex(ValueError, "terminate cleanly"):
            campaign._resource_contract(receipt, command, campaign.FIT_LIMITS)

    def _new_raw_report(self, input_path, attempts, terminal):
        return {"schema": "antfly.context.native.head-qualification.v2",
            "recipe": campaign.native.RECIPE, "accepted_head": False,
            "outcome": "preserved_original", "terminal_failure": terminal,
            "serving_qualified": False, "full_context_memory_qualified": False,
            "input_sha256": campaign.sha(input_path), "source_rows": 1025,
            "head_dim": 512, "training_rows": 24, "validation_rows": 8,
            "consumer_groups": 4, "exact_prefix_rows": 1,
            "exact_recent_rows": 512, "continuation_rows": 32,
            "retained_row_cap": 6144, "attempts": attempts,
            "derived_attention_readback_bytes": 1,
            "validation_readback_bytes": 1,
            "diagnostic_candidate_readback_bytes": 1}

    def test_new_raw_singular_rejection_follows_and_seals_frozen_ladder(self):
        input_path = self.output / "singular-input.bin"
        input_path.write_bytes(b"input")
        attempts = []
        for divisor in (4, 3, 2):
            retained = (512 + divisor - 1) // divisor
            attempts.append({"divisor": divisor, "retained_rows": 513 + retained,
                "outcome": "SingularValueFit", "elapsed_ns": 11,
                "blocks": [{"block": 0, "logical_start": 1,
                    "retained_rows": retained, "outcome": "SingularValueFit",
                    "elapsed_ns": 7, "solver": None}],
                "metrics": None, "reference": None})
        report_path = self.output / "singular.json"
        campaign.write_new(report_path,
            self._new_raw_report(input_path, attempts, "SingularValueFit"))
        result = campaign._qualification_report(report_path, input_path, 1, 5, 1,
            {"source_rows": 1025, "head_dim": 512, "training_rows": 24,
             "validation_rows": 8, "consumer_groups": 4, "prefix_rows": 1,
             "recent_rows": 512, "continuation_rows": 32, "retained_row_cap": 6144})
        self.assertEqual(result["terminal_failure"], "SingularValueFit")

    def test_new_terminal_parity_failure_is_operational_not_recipe_rejection(self):
        input_path = self.output / "parity-input.bin"
        input_path.write_bytes(b"input")
        retained = 128
        actual = {"version": 2, "queries": 8, "consumer_groups": 4,
            "max_mass_error": 0.001, "output_rmse": 0.001,
            "max_consumer_output_rmse": 0.001}
        reference = dict(actual)
        reference["max_mass_error"] = 0.002
        attempts = [{"divisor": 4, "retained_rows": 641,
            "outcome": "numerical_parity_rejected", "elapsed_ns": 19,
            "blocks": [{"block": 0, "logical_start": 1,
                "retained_rows": retained, "outcome": "fitted", "elapsed_ns": 17,
                "solver": {"quality_evaluated": False, "nnls_sweeps": 7,
                    "selection_steps": 128, "selection_active_solves": 2}}],
            "metrics": actual, "reference": reference}]
        report_path = self.output / "parity.json"
        campaign.write_new(report_path, self._new_raw_report(
            input_path, attempts, "NativeHeadNumericalParityFailed"))
        with self.assertRaisesRegex(RuntimeError, "operational"):
            campaign._qualification_report(report_path, input_path, 1, 5, 1,
                {"source_rows": 1025, "head_dim": 512, "training_rows": 24,
                 "validation_rows": 8, "consumer_groups": 4, "prefix_rows": 1,
                 "recent_rows": 512, "continuation_rows": 32, "retained_row_cap": 6144})

    def _bound_artifact_fixture(self):
        report_path = self.output / "bound-report.json"
        candidate_path = self.output / "bound-result.candidate"
        stderr_path = self.output / "bound-resource.stderr"
        candidate_path.write_bytes(b"x" * 32)
        report = {"accepted_head": True, "head_dim": 3,
                  "attempts": [{"retained_rows": 2}],
                  "candidate_sha256": campaign.sha(candidate_path),
                  "candidate_bytes": 32}
        campaign.write_new(report_path, report)
        stderr_path.write_text("context_native_head_artifacts: report_sha256=" +
            campaign.sha(report_path) + " candidate_sha256=" +
            report["candidate_sha256"] + " candidate_bytes=32\n")
        return report, report_path, candidate_path, stderr_path

    def test_bound_head_artifacts_validate_exact_candidate_and_marker(self):
        report, report_path, candidate_path, stderr_path = self._bound_artifact_fixture()
        result = campaign.validate_head_artifacts(
            report, report_path, candidate_path, stderr_path)
        self.assertEqual(result["candidate_bytes"], 32)
        self.assertEqual(result["report_sha256"], campaign.sha(report_path))

    def test_bound_rejected_head_requires_null_zero_and_no_candidate(self):
        report_path = self.output / "rejected-report.json"
        candidate_path = self.output / "rejected-result.candidate"
        stderr_path = self.output / "rejected-resource.stderr"
        report = {"accepted_head": False, "head_dim": 512,
                  "attempts": [{"retained_rows": 769}],
                  "candidate_sha256": None, "candidate_bytes": 0}
        campaign.write_new(report_path, report)
        stderr_path.write_text("context_native_head_artifacts: report_sha256=" +
            campaign.sha(report_path) +
            " candidate_sha256=none candidate_bytes=0\n")
        result = campaign.validate_head_artifacts(
            report, report_path, candidate_path, stderr_path)
        self.assertIsNone(result["candidate_sha256"])

    def test_bound_head_artifacts_reject_tampered_digest_or_size(self):
        report, report_path, candidate_path, stderr_path = self._bound_artifact_fixture()
        candidate_path.write_bytes(b"y" * 32)
        with self.assertRaisesRegex(ValueError, "digest or size"):
            campaign.validate_head_artifacts(
                report, report_path, candidate_path, stderr_path)
        candidate_path.write_bytes(b"x" * 31)
        with self.assertRaisesRegex(ValueError, "digest or size"):
            campaign.validate_head_artifacts(
                report, report_path, candidate_path, stderr_path)

    def test_bound_head_artifacts_reject_tampered_or_duplicate_marker(self):
        report, report_path, candidate_path, stderr_path = self._bound_artifact_fixture()
        marker = stderr_path.read_text()
        stderr_path.write_text(marker.replace("candidate_bytes=32", "candidate_bytes=31"))
        with self.assertRaisesRegex(ValueError, "marker"):
            campaign.validate_head_artifacts(
                report, report_path, candidate_path, stderr_path)
        stderr_path.write_text(marker + marker)
        with self.assertRaisesRegex(ValueError, "marker"):
            campaign.validate_head_artifacts(
                report, report_path, candidate_path, stderr_path)

    def _capture_phase(self):
        coverage = self.output / "coverage"
        coverage.mkdir()
        authority_path = coverage / "training-authority.json"
        authority_path.write_text("authority")
        receipt = coverage / "source-build.json"
        receipt.write_text("build")
        plan_phase = self.output / "plan-phase.json"
        plan_phase.write_text("plan")
        resource = coverage / "capture-resource.json"
        source = coverage / "e4b-training-native-head-index.json"
        repeat = coverage / "e4b-training-native-repeat-index.json"
        for path, data in ((resource, "resource"), (source, "source"), (repeat, "repeat")):
            path.write_text(data)
        campaign._phase(self.output, "capture", self.spec_sha, {
            "plan_phase": campaign._phase_ref(plan_phase),
            "resource": campaign._phase_ref(resource),
            "source_index": campaign._phase_ref(source),
            "repeat_source": campaign._phase_ref(repeat),
            "training_authority": campaign._phase_ref(authority_path)})
        self.make_identity_phase()
        return authority_path, receipt

    def _prepare_fit(self, accepted):
        authority_path, receipt = self._capture_phase()
        coordinates = [{"owner_layer": 5, "kv_head": 0},
                       {"owner_layer": 5, "kv_head": 1}]
        cells = [{"owner_layer": 5, "kv_head": head, "source_heads": 2,
                  "head_dim": 512} for head in range(2)]
        binding = {"plan": {"coordinates": coordinates},
            "authority": {"loaded_identity": {"load_generation": 7,
                "artifact_sha256": "1" * 64, "tokenizer_sha256": "2" * 64,
                "template_sha256": "3" * 64, "layout_sha256": "4" * 64,
                "implementation_sha256": "5" * 64}, "cells": cells},
            "paths": {"source_build_receipt": receipt},
            "digests": {"source_build_receipt": campaign.sha(self.build)}}
        build = {"binary_path": self.binary,
                 "receipt_sha256": campaign.sha(self.build),
                 "binary_sha256": campaign.sha(self.binary)}

        def prepare(**kwargs):
            path = Path(kwargs["output"])
            path.write_bytes(b"input")
            Path(str(path) + ".json").write_text("{}")
            return {"source_rows": 1025, "head_dim": 512,
                    "training_rows": 24, "validation_rows": 8,
                    "consumer_groups": 4, "prefix_rows": 1, "recent_rows": 512,
                    "continuation_rows": 32, "retained_row_cap": 6144}

        calls = []
        def run(command, resource, limits, numerical=False):
            head = len(calls)
            calls.append(command)
            Path(resource).write_text("resource")
            Path(resource).with_suffix(".stdout").write_text("")
            output_prefix = Path(next(item.split("=", 1)[1] for item in command
                                      if item.startswith("ANTFLY_CONTEXT_NATIVE_HEAD_OUTPUT=")))
            input_path = Path(next(item.split("=", 1)[1] for item in command
                                   if item.startswith("ANTFLY_CONTEXT_NATIVE_HEAD_INPUT=")))
            is_accepted = accepted[head]
            metrics = {"version": 2, "queries": 8, "consumer_groups": 4,
                "max_mass_error": .01 if is_accepted else .2,
                "output_rmse": .01, "max_consumer_output_rmse": .01}
            attempts = [{"divisor": divisor,
                "retained_rows": 1 + ((512 + divisor - 1) // divisor) + 512,
                "outcome": "accepted_head" if is_accepted else "whole_head_quality_rejected",
                "elapsed_ns": 19,
                "blocks": [{"block": 0, "logical_start": 1,
                    "retained_rows": (512 + divisor - 1) // divisor,
                    "outcome": "fitted", "elapsed_ns": 17,
                    "solver": {"quality_evaluated": False, "nnls_sweeps": 7,
                        "selection_steps": 128, "selection_active_solves": 2}}],
                "metrics": metrics, "reference": dict(metrics)}
                for divisor in ([4] if is_accepted else [4, 3, 2])]
            candidate = Path(str(output_prefix) + ".candidate")
            if is_accepted:
                candidate.write_bytes(b"c" * (attempts[-1]["retained_rows"] * (4 * 512 + 4)))
            report = {"schema": "antfly.context.native.head-qualification.v2",
                "recipe": campaign.native.RECIPE, "accepted_head": is_accepted,
                "outcome": "accepted_head" if is_accepted else "preserved_original",
                "terminal_failure": None if is_accepted else "whole_head_quality_rejected",
                "serving_qualified": False, "full_context_memory_qualified": False,
                "input_sha256": campaign.sha(input_path), "source_rows": 1025,
                "head_dim": 512, "training_rows": 24, "validation_rows": 8,
                "consumer_groups": 4, "exact_prefix_rows": 1, "exact_recent_rows": 512,
                "continuation_rows": 32, "retained_row_cap": 6144,
                "attempts": attempts,
                "derived_attention_readback_bytes": 1,
                "validation_readback_bytes": 1,
                "diagnostic_candidate_readback_bytes": 1,
                "candidate_sha256": campaign.sha(candidate) if is_accepted else None,
                "candidate_bytes": candidate.stat().st_size if is_accepted else 0}
            report_path = Path(str(output_prefix) + ".json")
            campaign.write_new(report_path, report)
            marker = ("context_native_head_artifacts: report_sha256=" +
                campaign.sha(report_path) + " candidate_sha256=" +
                (report["candidate_sha256"] or "none") + " candidate_bytes=" +
                str(report["candidate_bytes"]) + "\n")
            Path(resource).with_suffix(".stderr").write_text(marker)
            return {"returncode": 0 if is_accepted else 1}
        return binding, build, prepare, run

    def test_fit_seals_numerical_rejection_and_continues_all_heads(self):
        binding, build, prepare, run = self._prepare_fit([True, False])
        with mock.patch.object(campaign.authority, "validate_authority", return_value=binding), \
             mock.patch.object(campaign, "validate_build", return_value=build), \
             mock.patch.object(campaign.preparer, "prepare", side_effect=prepare), \
             mock.patch.object(campaign, "_run", side_effect=run):
            result = campaign.phase_fit_legacy_diagnostic(
                self.spec, self.spec_sha, self.output)
        self.assertEqual(result["outcome"], "numerical_rejection")
        self.assertEqual(result["accepted_head_count"], 1)
        self.assertEqual(len(result["results"]), 2)
        self.assertTrue((self.output / "coverage/owner5-head0-seal.json").is_file())
        self.assertTrue((self.output / "coverage/owner5-head1-seal.json").is_file())
        self.assertTrue((self.output / "coverage/results.json").is_file())

    def test_fit_operational_failure_does_not_seal_campaign_results(self):
        binding, build, prepare, _ = self._prepare_fit([False, False])
        def run_without_report(command, resource, limits, numerical=False):
            Path(resource).write_text("resource")
            return {"returncode": 1}
        with mock.patch.object(campaign.authority, "validate_authority", return_value=binding), \
             mock.patch.object(campaign, "validate_build", return_value=build), \
             mock.patch.object(campaign.preparer, "prepare", side_effect=prepare), \
             mock.patch.object(campaign, "_run", side_effect=run_without_report):
            with self.assertRaises(FileNotFoundError):
                campaign.phase_fit_legacy_diagnostic(
                    self.spec, self.spec_sha, self.output)
        self.assertFalse((self.output / "coverage/results.json").exists())

    def test_fit_rejoins_authority_head_dim_to_bootstrap_and_spec(self):
        binding, build, prepare, run = self._prepare_fit([True, True])
        binding["authority"]["cells"][1]["head_dim"] = 256
        with mock.patch.object(campaign.authority, "validate_authority", return_value=binding), \
             mock.patch.object(campaign, "validate_build", return_value=build), \
             mock.patch.object(campaign.preparer, "prepare", side_effect=prepare), \
             mock.patch.object(campaign, "_run", side_effect=run):
            with self.assertRaisesRegex(ValueError, "cell geometry"):
                campaign.phase_fit_legacy_diagnostic(
                    self.spec, self.spec_sha, self.output)
        self.assertFalse((self.output / "coverage/results.json").exists())

    def test_public_fit_reuses_exact_default_seal_and_dispatches_resumable_format(self):
        spec = dict(self.spec)
        spec["expected_owners"] = [{"owner_layer": layer,
            "source_heads": 2, "head_dim": 512} for layer in (5, 11, 17, 23)]
        attempt = (self.output /
            "coverage/fit-attempts/owner5-head0/attempt-000000")
        attempt.mkdir(parents=True)
        seal = attempt / "seal.json"
        seal.write_text("accepted seal\n")
        validate_head = mock.Mock(return_value={"accepted_head": True})
        run_head = mock.Mock(side_effect=lambda _spec, _sha, _output,
            owner, head, attempt_id: {"accepted_head": True,
                "owner_layer": owner, "kv_head": head,
                "attempt_id": attempt_id,
                "seal": {"path": "seal",
                         "sha256": f"{owner:02d}{head}".ljust(64, "a")}})
        assembled = {"schema": "antfly.context.native.repeat-resumable-results.v1",
                     "fit_layout": "immutable_attempt_v1"}
        assemble = mock.Mock(return_value=assembled)
        module = types.SimpleNamespace(validate_head=validate_head,
            run_head=run_head, assemble=assemble)
        with mock.patch.dict(sys.modules, {"native_repeat_fit_resume": module}), \
             mock.patch.object(campaign, "phase_fit_legacy_diagnostic") as legacy:
            result = campaign.phase_fit(spec, self.spec_sha, self.output)
        self.assertEqual(result, assembled)
        legacy.assert_not_called()
        validate_head.assert_called_once_with(spec, self.spec_sha, self.output,
            5, 0, "000000", campaign.sha(seal))
        self.assertEqual(run_head.call_count, 7)
        selections = assemble.call_args.args[3]
        self.assertEqual([(row["owner_layer"], row["kv_head"])
                          for row in selections],
                         [(layer, head) for layer in (5, 11, 17, 23)
                          for head in range(2)])
        self.assertTrue(all(row["attempt_id"] == "000000"
                            for row in selections))

    def test_main_uses_distinct_exit_for_sealed_numerical_rejection(self):
        with mock.patch.object(campaign, "load_spec", return_value=self.spec), \
             mock.patch.object(campaign, "run_phase", return_value={"all_heads_accepted": False}):
            self.assertEqual(campaign.main(["fit", "--spec", str(self.spec_path),
                "--spec-sha256", self.spec_sha, "--output", str(self.output)]), 2)
        with mock.patch.object(campaign, "load_spec", side_effect=ValueError("bad")):
            self.assertEqual(campaign.main(["fit", "--spec", str(self.spec_path),
                "--spec-sha256", self.spec_sha, "--output", str(self.output)]), 1)


class CombinedResourceTests(unittest.TestCase):
    def test_process_rss_counts_parent_and_child_once_and_ignores_zombies(self):
        listing = "10 4 1024 S\n11 20 2048 S\n12 20 4096 Z\n11 20 2048 S\n"
        self.assertEqual(resources.process_rss(10, 20, listing), (1, 2, 3, 1))

    def test_combined_guard_claims_outputs_and_preserves_witness_contract(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            output = root / "resource.json"
            observed = {}
            class Process:
                pid = 123
                returncode = 0
                def __init__(self): self.polls = 0
                def poll(self):
                    self.polls += 1
                    return None if self.polls == 1 else 0
            def popen(command, **kwargs):
                observed.update(kwargs)
                return Process()
            disk = mock.Mock(); disk.sample_mib.return_value = 5000
            probe = lambda parent, child=None: (10, 20 if child else 0,
                                                 30 if child else 10,
                                                 1 if child else 0)
            with mock.patch.object(resources.base, "DiskSpaceGuard", return_value=disk), \
                 mock.patch.object(resources.base, "memory_free_percent", return_value=80), \
                 mock.patch.object(resources.base, "swapout_bytes", return_value=0), \
                 mock.patch.object(resources.subprocess, "Popen", side_effect=popen), \
                 mock.patch.object(resources.base, "terminate_group"), \
                 mock.patch.object(resources.base, "wait_group_drained", return_value=(0, 0)), \
                 mock.patch.object(resources.time, "sleep"):
                result = resources.run_guarded_combined(["binary"], output,
                    timeout=2, max_rss_mib=64, min_free_percent=15,
                    max_swap_growth_mib=0, min_disk_free_mib=4096,
                    _rss_probe=probe)
            self.assertTrue(result["pass"])
            self.assertEqual(result["rss_scope"], resources.RSS_SCOPE)
            self.assertEqual(result["remaining_child_group_live_processes"], 0)
            self.assertTrue(observed["start_new_session"])
            self.assertIn(resources.base.GUARD_FD_ENV, observed["env"])
            self.assertEqual(json.loads(output.read_text())["command"], ["binary"])
            with self.assertRaisesRegex(ValueError, "already exists"):
                resources.run_guarded_combined(["binary"], output,
                    timeout=2, max_rss_mib=64, min_free_percent=15,
                    max_swap_growth_mib=0, min_disk_free_mib=4096)

    def test_sigterm_unwinds_guard_and_drains_real_child_group(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            child_pid = root / "child.pid"
            child = root / "child.py"
            child.write_text("import os,pathlib,time\npathlib.Path(" +
                repr(str(child_pid)) + ").write_text(str(os.getpid()))\ntime.sleep(30)\n")
            receipt = root / "resource.json"
            scripts = Path(campaign.__file__).resolve().parent.parent
            gemma = Path(campaign.__file__).resolve().parent
            wrapper = ("import sys\n" +
                f"sys.path[:0]=[{str(gemma)!r},{str(scripts)!r}]\n" +
                "import native_repeat_campaign as campaign\n" +
                "import native_repeat_campaign_resources as resources\n" +
                "with campaign.controlled_termination():\n" +
                f" result=resources.run_guarded_combined([{sys.executable!r},{str(child)!r}],"
                f"{str(receipt)!r},timeout=20,max_rss_mib=512,min_free_percent=1,"
                "max_swap_growth_mib=0,min_disk_free_mib=0,interval=.05)\n" +
                "raise SystemExit(0 if result['pass'] else 1)\n")
            parent = subprocess.Popen([sys.executable, "-c", wrapper],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            deadline = time.monotonic() + 8
            while not child_pid.exists() and parent.poll() is None and time.monotonic() < deadline:
                time.sleep(.02)
            self.assertTrue(child_pid.is_file(), parent.stderr.read().decode() if parent.poll() is not None else "")
            pid = int(child_pid.read_text())
            os.kill(parent.pid, signal.SIGTERM)
            stdout, stderr = parent.communicate(timeout=10)
            self.assertEqual(parent.returncode, 1, (stdout, stderr))
            report = json.loads(receipt.read_text())
            self.assertIn("InterruptedError", report["violation"])
            self.assertEqual(report["remaining_child_group_rss_mib"], 0)
            self.assertEqual(report["remaining_child_group_live_processes"], 0)
            with self.assertRaises(ProcessLookupError):
                os.kill(pid, 0)


if __name__ == "__main__":
    unittest.main()
