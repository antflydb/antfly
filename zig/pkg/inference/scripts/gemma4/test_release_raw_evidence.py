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

"""Adversarial schema-2 tests. Synthetic fixtures are never qualification."""
import copy
import hashlib
import itertools
import json
from pathlib import Path
import shutil
import tempfile
import unittest
from unittest.mock import patch

import attention_matching_calibrate as am
import metal_attention_matching_qualify as metal
import numpy as np
import release_attention_matching as release
import release_build_fixture
import test_attention_matching_calibrate as calibration_fixtures


class RawEvidenceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.addCleanup(self.temp.cleanup)
        self.raw_numerical_counter = 0
        self.manifest = self.make_manifest()

    @staticmethod
    def digest(value):
        return hashlib.sha256(value.encode()).hexdigest()

    def runtime_profile(self):
        fixture = calibration_fixtures.CalibrationTests()
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        _, meta, _ = fixture.capture("runtime", schema=2, values=[1, 1, 1, 1])
        key = f"{am.model_key(meta)}:4:0"
        contract = {**am.capture_contract(meta), "source_rows": 512, "head_dim": 1}
        profile = {"profiles": {key: {"retained_rows": 256, "recipe": am.RUNTIME_PROFILE_RECIPE, **contract}}}
        captures = {"capture": meta}
        profile["runtime_candidate"] = am.derive_runtime_candidate(captures, profile["profiles"],
                                                                   16384, 32768, True, True)
        identity = {key.removesuffix("_sha256"): value
                    for key, value in profile["runtime_candidate"]["identity"].items()}
        return profile, captures, identity

    def test_runtime_candidate_matches_frozen_choices_with_shorter_evaluation(self):
        profile, captures, identity = self.runtime_profile()
        candidate = profile["runtime_candidate"]
        candidate["max_prompt_tokens"] = 8160
        candidate["max_output_tokens"] = 32
        candidate["profile_semantic_sha256"] = am.profile_semantic_identity(candidate["owners"], candidate["query_protocols"], 8160, 32, 32768)
        result = release.runtime_candidate_contract(profile, captures, identity)
        self.assertEqual(result["max_prompt_tokens"], 8160)
        self.assertEqual(result["query_protocols"], candidate["query_protocols"])
        self.assertIsNone(release.runtime_candidate_contract({"profiles": profile["profiles"]}, captures, identity))

    def test_runtime_candidate_rejects_rehashed_semantic_substitution(self):
        profile, captures, identity = self.runtime_profile()
        for mutate in (
            lambda value: value["owners"][0]["retained"].__setitem__(0, 320),
            lambda value: value["query_protocols"][0].__setitem__("sha256", "1" * 64),
            lambda value: value["identity"].__setitem__("implementation_sha256", "1" * 64),
            lambda value: value["implementation"].__setitem__("recipe", "relative_qr"),
            lambda value: value.__setitem__("max_prompt_tokens", 1024),
            lambda value: value.__setitem__("max_output_tokens", 131072),
            lambda value: value.__setitem__("unexpected", True),
        ):
            changed = copy.deepcopy(profile)
            candidate = changed["runtime_candidate"]
            mutate(candidate)
            candidate["profile_semantic_sha256"] = am.profile_semantic_identity(
                candidate["owners"], candidate["query_protocols"], candidate["max_prompt_tokens"], candidate["max_output_tokens"],
                candidate["minimum_saved_bytes"])
            with self.subTest(candidate=candidate), self.assertRaises(ValueError):
                release.runtime_candidate_contract(changed, captures, identity)

    def write_record(self, name, value):
        path = self.root / name
        release.write_json(path, value)
        return {"path": name, "sha256": release.sha(path)}

    def report(self, identity, origin, **overrides):
        value = {"schema_version": 2, "semantic_identity": identity, "provenance": origin,
                 "published": True, "exact_fallback": False,
                 "committed_before_bytes": 10000, "committed_after_bytes": 7000,
                 "backend_kv_readback_bytes": 0, "swapout_growth_bytes": 0,
                 "live_bytes_before": 100, "live_bytes_after_release": 100,
                 "context_allocation_scope": release.CONTEXT_ALLOCATION_SCOPE,
                 "context_allocation_scope_version": release.CONTEXT_ALLOCATION_SCOPE_VERSION,
                 "context_allocation_scope_complete": True, "full_owner_physical_available": True,
                 "model_execution_pool_admission_version": release.CONTEXT_EXECUTION_POOL_ADMISSION_VERSION,
                 "model_execution_pool_admitted_backend_bytes": release.CONTEXT_EXECUTION_POOL_ADMITTED_BACKEND_BYTES,
                 "model_execution_pool_shared_residency": True, "committed_context_bytes": 7000,
                 "context_peak_bytes": 10000, "owner_released": True, "owner_post_release_live_bytes": 0,
                 "route": "native", "prompt_tokens": 8160, "reserved_output_tokens": 32}
        value.update(overrides)
        return value

    def make_manifest(self):
        artifact = self.root / "model.gguf"
        profile = self.root / "profile.json"
        binary = self.root / "inference"
        source = self.root / "source-build.json"
        artifact.write_bytes(b"model artifact")
        profile.write_bytes(b"frozen profile")
        binary.write_bytes(b"native executable")
        binary_sha = release.sha(binary)
        release_build_fixture.write(self.root, source, binary)

        identity = {"artifact": release.sha(artifact), "profile": release.sha(profile),
                    "tokenizer": self.digest("tokenizer"), "template": self.digest("template"),
                    "layout": self.digest("layout"), "implementation": self.digest("implementation"),
                    "dtype": "f16_kv_f32_bias", "kernel_route": "metal_context_optimized"}
        origin = {"binary_sha256": binary_sha, "source_build_sha256": release.sha(source),
                  "device_sha256": self.digest("device"), "os_sha256": self.digest("os")}
        length, reserved = 8192, 32
        loaded = {"load_generation": 1, "artifact_sha256": identity["artifact"],
                  "tokenizer_sha256": identity["tokenizer"], "template_sha256": identity["template"],
                  "layout_sha256": identity["layout"], "implementation_sha256": identity["implementation"]}

        exact_observations = []
        for index in range(-release.MIN_PERFORMANCE_WARMUPS, release.MIN_PERFORMANCE_PAIRS):
            for storage in (("automatic", "packed_exact") if index % 2 == 0 else ("packed_exact", "automatic")):
                exact_observations.append({"id": f"exact-{index}-{storage}", "pair_id": f"exact-{index}",
                                           "warmup": index < 0, "min_free_percent": 50,
                                           "swapout_growth_bytes": 0,
                                           "observation": {"schema_version": 2, "route": "native_pipeline",
                                                           "workload": "independent_document",
                                                           "prompt_sha256": self.digest(f"exact-prompt-{index}"),
                                                           "storage": storage, "prompt_tokens": length - reserved,
                                                           "reserved_output_tokens": reserved,
                                                           "produced_output_tokens": reserved, "total_positions": length,
                                                           "prefill_chunk_rows": 64, "callback_count": 1,
                                                           "first_callback_ns": 100, "phases_ms": {"prefill": .8, "decode": .2},
                                                           "elapsed_ns": 1_040_000 if storage == "packed_exact" else 1_000_000,
                                                           "output_sha256": hashlib.sha256(bytes(reserved * 4)).hexdigest(),
                                                           "output_token_ids": [0] * reserved, "context_live_bytes": 100,
                                                           "context_peak_bytes": 200, "backend_kv_readback_bytes": 0,
                                                           "loaded_identity": dict(loaded)}})

        cases = []
        for bucket in sorted(release.BUCKETS):
            for index in range(release.MIN_QUALITY_CASES):
                cases.append({**self.report(identity, origin), "id": f"{bucket}-{index}",
                              "bucket": bucket,
                              "document_sha256": self.digest(f"document-{bucket}-{index}"),
                              "exact_correct": True, "candidate_correct": True,
                              "scored_tokens": 20, "exact_nll_sum": 10, "candidate_nll_sum": 10})

        settings = {variant: self.digest("settings-" + variant) for variant in release.PERFORMANCE_VARIANTS}
        orders = list(itertools.permutations(release.PERFORMANCE_VARIANTS))

        def perf_row(index, warmup=False):
            prefix = "warmup" if warmup else "sample"
            variants = {}
            for variant in release.PERFORMANCE_VARIANTS:
                variants[variant] = {"completed": True, "attempt_id": f"{prefix}-{index}-{variant}",
                                     "settings_sha256": settings[variant], "ttft_ms": 100.0,
                                     "decode_tokens_per_second": 100.0,
                                     "committed_context_bytes": 10000,
                                     "swapout_growth_bytes": 0, "output_sha256": "e" * 64}
            variants["packed_exact"]["ttft_ms"] = 104.0
            variants["attention_matching"].update(ttft_ms=120.0, decode_tokens_per_second=95.0,
                                                   committed_context_bytes=7000, published=True,
                                                   exact_fallback=False)
            return {"pair_id": f"{prefix}-{index}",
                    "prompt_sha256": self.digest(f"prompt-{prefix}-{index}"),
                    "settings_sha256": self.digest("shared-generation-settings"),
                    "context_tokens": length, "reserved_output_tokens": reserved,
                    "prompt_tokens": length - reserved, "output_tokens": reserved,
                    "order": list(orders[index % len(orders)]), **variants}

        trials = [{"start_seconds": index * 2880, "end_seconds": (index + 1) * 2880,
                   "report": self.report(identity, origin, route=route, request_index=index),
                   "resources": {"swapout_growth_bytes": 0, "min_free_percent": 20,
                                 "min_disk_free_mib": 8192, "peak_group_rss_mib": 1024}}
                  for index, route in enumerate(sorted(release.ROUTES))]
        records = {
            "exact_context": {"schema_version": 1, "completed": True,
                              "file_provenance": {"artifact_sha256": identity["artifact"],
                                                  "binary_sha256": origin["binary_sha256"],
                                                  "source_build_receipt_sha256": origin["source_build_sha256"]}, "semantic_identity": identity, "provenance": origin,
                              "evidence_passed": True, "loaded_identity_verified": True,
                              "qualified_context_tokens": [length],
                              "results": [{"context_tokens": length, "pairs": release.MIN_PERFORMANCE_PAIRS,
                                           "packed_exact_elapsed_ratio_upper_95": 1.04, "packed_exact_decode_ratio_lower_95": 1.0}],
                              "observations": exact_observations},
            "numerical": {"semantic_identity": identity, "provenance": origin,
                          "frozen_before_evaluation": True, "all_owner_heads_covered": True,
                          "metal_fp64_parity_passed": True,
                          "consumers": [{"owner_layer": owner, "kv_head": kv, "consumer_layer": consumer,
                                         "query_head": head, "mass_max": .04,
                                         "output_relative_rmse": .09, "after_f16": True}
                                        for owner, kv, consumer, head in sorted(release.expected_strata("gemma4-e2b"))]},
            "quality": {"semantic_identity": identity, "provenance": origin, "cases": cases},
            "soak": {"semantic_identity": identity, "provenance": origin, "persistent_driver": True,
                     "completed": True, "duration_seconds": 14400, "trials": trials, "max_rss_mib": 8192},
            "failures": {"semantic_identity": identity, "provenance": origin,
                         "cases": [{"fault": fault, "live_bytes_before": 100,
                                    "exact_output_sha256": "e" * 64, "observer_output_sha256": "e" * 64,
                                    "loaded_generation_before": 1, "request_generation": 1,
                                    "loaded_generation_after": 2 if fault == "reload" else 1,
                                    "events": [{"name": "fault_injected", "monotonic_ns": 1, "live_bytes": 200},
                                               {"name": ("partial_context_destroyed" if fault == "cancel_prefill" else
                                                         "allocation_rollback_verified" if fault == "oom" else
                                                         "model_generation_reloaded" if fault == "reload" else
                                                         "original_context_verified"),
                                                "monotonic_ns": 2, "live_bytes": 200},
                                               {"name": "request_released", "monotonic_ns": 3, "live_bytes": 100}]}
                                   for fault in sorted(release.FAILURES)]},
            "performance": {"schema_version": 2, "semantic_identity": identity, "provenance": origin,
                            "cells": [{"context_tokens": length, "reserved_output_tokens": reserved,
                                       "generation_settings_sha256": self.digest("shared-generation-settings"),
                                       "variant_settings_sha256": settings}],
                            "warmups": [perf_row(index, True) for index in range(release.MIN_PERFORMANCE_WARMUPS)],
                            "samples": [perf_row(index) for index in range(release.MIN_PERFORMANCE_PAIRS)]},
        }
        reload_case = next(case for case in records["failures"]["cases"] if case["fault"] == "reload")
        reload_case.update(
            request_generation=reload_case["loaded_generation_after"],
            stale_selection_rejected=True,
        )
        reload_case["events"] = [
            {"name": name, "monotonic_ns": index, "live_bytes": 100}
            for index, name in enumerate((
                "fault_injected",
                "model_generation_reloaded",
                "stale_selection_rejected",
                "fresh_attention_matching_published",
                "request_released",
            ))
        ]
        model = {"model": "gemma4-e2b", "semantic_identity": identity, "provenance": origin,
                 "qualified_context_tokens": [length], "calibration_documents": [self.digest("calibration")],
                 "provenance_files": {
                     "artifact": {"path": artifact.name, "sha256": release.sha(artifact)},
                     "profile": {"path": profile.name, "sha256": release.sha(profile)},
                     "source_build": {"path": source.name, "sha256": release.sha(source)},
                     "binary": {"path": binary.name, "sha256": release.sha(binary)}}}
        for kind, value in records.items():
            model[kind] = self.write_record(kind + ".json", value)
        return {"schema_version": 2, "models": [model]}

    def evidence(self, kind):
        entry = self.manifest["models"][0][kind]
        return release.read_json(self.root / entry["path"])

    def rewrite(self, kind, mutate):
        entry = self.manifest["models"][0][kind]
        path = self.root / entry["path"]
        value = release.read_json(path)
        mutate(value)
        path.write_text(json.dumps(value, sort_keys=True))
        entry["sha256"] = release.sha(path)

    def attach_raw_numerical(self):
        self.raw_numerical_counter += 1
        suffix = str(self.raw_numerical_counter)
        model = self.manifest["models"][0]
        identity, origin = model["semantic_identity"], model["provenance"]
        fixture = calibration_fixtures.CalibrationTests()
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        source_manifest, _, _ = fixture.capture(values=[1, 1, 1, 1])
        capture_dir = self.root / ("evaluation-capture-" + suffix)
        shutil.copytree(source_manifest.parent, capture_dir)
        capture_path = capture_dir / "manifest.json"
        meta = json.loads(capture_path.read_text())
        meta["purpose"] = "evaluation"
        meta["model"].update(sha256=identity["artifact"], tokenizer_sha256=identity["tokenizer"],
                             template_sha256=identity["template"], layout_sha256=identity["layout"],
                             implementation_sha256=identity["implementation"])
        meta["document"].update(id="held-out", sha256=self.digest("held-out-numerical-document"))
        capture_path.write_text(json.dumps(meta, sort_keys=True))
        meta, arrays = am.load_capture(capture_path)
        profile = {"schema_version": am.SCHEMA, "stage": "offline_numerical", "serving_qualified": False,
                   "script_sha256": am.script_identity(), "retention_policy": "fixed",
                   "calibration_document_hashes": model["calibration_documents"],
                   "calibration_document_ids": ["calibration"], "calibration_query_hashes": [],
                   "profiles": {f"{am.model_key(meta)}:4:0": {
                       "retained_rows": 2, "recipe": "relative_cd_ridge_rms", **am.capture_contract(meta)}}}
        profile_path = self.root / model["provenance_files"]["profile"]["path"]
        profile_path.write_text(json.dumps(profile, sort_keys=True))
        previous_profile_sha = identity["profile"]
        identity["profile"] = model["provenance_files"]["profile"]["sha256"] = release.sha(profile_path)
        for kind in ("numerical", "quality", "soak", "failures", "performance", "exact_context"):
            entry = model[kind]
            path = self.root / entry["path"]
            path.write_text(path.read_text().replace(previous_profile_sha, identity["profile"]))
            entry["sha256"] = release.sha(path)
        capture_sha = release.sha(capture_path)
        result = am.fit_head(meta, arrays, 0, 2, "relative_cd_ridge_rms")
        self.assertTrue(result["accepted"])
        result.update(capture_sha256=capture_sha, selected_attempt=True)
        offline = {"schema_version": am.SCHEMA, "stage": "offline_numerical", "serving_qualified": False,
                   "script_sha256": am.script_identity(), "profile_sha256": identity["profile"],
                   "captures": [{"sha256": capture_sha, "path": "/collector/path/is/not/trusted",
                                 "query_sha256": meta["tensors"]["queries"]["sha256"],
                                 "metadata": {key: meta[key] for key in
                                              ("schema_version", "purpose", "model", "document", "source", "capture")}}],
                   "results": [result]}
        offline_ref = self.write_record("offline-numerical-" + suffix + ".json", offline)

        metal_dir = self.root / ("metal-numerical-" + suffix)
        case_dir = metal_dir / "case-000"
        case_dir.mkdir(parents=True)
        prefix = case_dir / "candidate"
        arrays["keys"][result["selected_source_rows"], 0].astype("<f2").tofile(str(prefix) + "-keys.f16")
        np.ones((2, 1), dtype="<f2").tofile(str(prefix) + "-values.f16")
        np.full(2, np.log(2), dtype="<f4").tofile(str(prefix) + "-bias.f32")
        report_bytes = metal.REPORT.pack(0, 128, 0., 0., 0., 7, 0., 1)
        Path(str(prefix) + "-report.bin").write_bytes(report_bytes)
        report_fields = ("failure_flags", "iterations", "mass_relative_max_error", "output_relative_rmse",
                         "training_mass_relative_max_error", "evaluated", "max_consumer_output_relative_rmse",
                         "validation_groups")
        report = dict(zip(report_fields, metal.REPORT.unpack(report_bytes)))
        oracle = metal.evaluate_candidate(
            meta, arrays, result,
            am.raw_array(str(prefix) + "-keys.f16", "<f2", (2, 1)),
            am.raw_array(str(prefix) + "-values.f16", "<f2", (2, 1)),
            am.raw_array(str(prefix) + "-bias.f32", "<f4", (2,)), report)
        input_path, resource_path = case_dir / "input.bin", case_dir / "resources.json"
        metal.export_input(input_path, meta, arrays, result)
        resource_path.write_text(json.dumps({
            "schema": "antfly.benchmark_resources.v1",
            "command": ["/campaign/inference", "--attention-matching-fit-metal"],
            "pass": True, "returncode": 0, "violation": None,
            "limits": {"timeout_seconds": 300, "max_rss_mib": 2048,
                       "min_free_percent": 15, "max_swap_growth_mib": 0,
                       "min_disk_free_mib": 4096},
            "samples": 2, "peak_group_rss_mib": 100, "initial_free_percent": 30,
            "min_free_percent": 20, "swapout_growth_mib": 0,
            "min_disk_free_mib": 8192, "pid": 123, "elapsed_seconds": 1,
        }, sort_keys=True))
        outputs = {suffix: release.sha(str(prefix) + "-" + suffix)
                   for suffix in ("keys.f16", "values.f16", "bias.f32", "report.bin")}
        case = {"capture_sha256": capture_sha, "owner_layer": 4, "head": 0, "retained_rows": 2,
                "passed": True, "input_sha256": release.sha(input_path),
                "resource_receipt_sha256": release.sha(resource_path), "gpu_report": report,
                "oracle": oracle, "outputs": outputs}
        terminal = {"schema_version": 1, "stage": "metal_fitter_parity", "serving_qualified": False,
                    "binary_sha256": origin["binary_sha256"],
                    "build_receipt_sha256": origin["source_build_sha256"],
                    "numerical_receipt_sha256": offline_ref["sha256"],
                    "driver_sha256": release.sha(metal.__file__),
                    "metric_parity_tolerance": metal.METRIC_PARITY_TOLERANCE,
                    "report_parity_tolerance": metal.REPORT_PARITY_TOLERANCE,
                    "passed": True, "candidates": [case]}
        metal_ref = {"path": "metal-numerical-" + suffix + "/receipt.json", "sha256": ""}
        release.write_json(self.root / metal_ref["path"], terminal)
        metal_ref["sha256"] = release.sha(self.root / metal_ref["path"])
        raw = {"offline_numerical": offline_ref, "metal_qualification": metal_ref,
               "profile": dict(model["provenance_files"]["profile"]),
               "captures": [{"path": str(capture_path.relative_to(self.root)), "sha256": capture_sha}]}
        self.rewrite("numerical", lambda numerical: numerical.update(raw_evidence=raw))
        return raw, case_dir

    def test_summary_only_soak_numerical_and_failures_never_pass_v2(self):
        result = release.audit(self.manifest, self.root)
        self.assertFalse(result["evidence_passed"])
        self.assertTrue(result["diagnostic_only"])
        self.assertEqual(set(result["incomplete_gates"]), {
            "raw_numerical_observations", "raw_failure_injection_observations",
            "v2_raw_soak_collector", "loaded_tokenizer_template_profile_provenance",
            "raw_quality_observations", "raw_performance_observations", "raw_exact_context_observations"})
        self.assertFalse(result["models"][0]["evidence_passed"])

    def test_inflated_packed_storage_cannot_hide_ordinary_exact_memory_regression(self):
        perf = self.evidence("performance")
        for row in perf["samples"] + perf["warmups"]:
            row["packed_exact"]["committed_context_bytes"] = 20000
            row["attention_matching"]["committed_context_bytes"] = 9000
        model = self.manifest["models"][0]
        with self.assertRaisesRegex(ValueError, "committed savings below"):
            release.performance_contract(perf, model["semantic_identity"], model["provenance"], [8192])

    def test_wrong_context_and_incomplete_pair_reject(self):
        perf = self.evidence("performance")
        perf["samples"][0]["context_tokens"] = 16384
        with self.assertRaisesRegex(ValueError, "outside declared context cell"):
            release.performance_contract(perf, self.manifest["models"][0]["semantic_identity"],
                                         self.manifest["models"][0]["provenance"], [8192])
        perf = self.evidence("performance")
        perf["samples"][0]["attention_matching"]["completed"] = False
        with self.assertRaisesRegex(ValueError, "incomplete performance attempt"):
            release.performance_contract(perf, self.manifest["models"][0]["semantic_identity"],
                                         self.manifest["models"][0]["provenance"], [8192])
        perf = self.evidence("performance")
        del perf["samples"][0]["packed_exact"]
        with self.assertRaisesRegex(ValueError, "incomplete paired performance sample"):
            release.performance_contract(perf, self.manifest["models"][0]["semantic_identity"],
                                         self.manifest["models"][0]["provenance"], [8192])

    def test_variant_settings_and_order_balance_are_derived(self):
        perf = self.evidence("performance")
        perf["samples"][0]["exact"]["settings_sha256"] = "0" * 64
        with self.assertRaisesRegex(ValueError, "variant settings mismatch"):
            release.performance_contract(perf, self.manifest["models"][0]["semantic_identity"],
                                         self.manifest["models"][0]["provenance"], [8192])
        perf = self.evidence("performance")
        for row in perf["samples"]:
            row["order"] = list(release.PERFORMANCE_VARIANTS)
        with self.assertRaisesRegex(ValueError, "execution order is imbalanced"):
            release.performance_contract(perf, self.manifest["models"][0]["semantic_identity"],
                                         self.manifest["models"][0]["provenance"], [8192])

    def test_performance_gates_use_ordinary_exact_baselines(self):
        model = self.manifest["models"][0]
        args = (model["semantic_identity"], model["provenance"], [8192])

        perf = self.evidence("performance")
        for row in perf["samples"]:
            row["packed_exact"]["ttft_ms"] = 104.9
            row["attention_matching"]["ttft_ms"] = 129.9
        with self.assertRaisesRegex(ValueError, "attention-matching TTFT regression"):
            release.performance_contract(perf, *args)

        perf = self.evidence("performance")
        for row in perf["samples"]:
            row["packed_exact"]["decode_tokens_per_second"] = 80
            row["attention_matching"]["decode_tokens_per_second"] = 80
        with self.assertRaisesRegex(ValueError, "packed-exact decode regression"):
            release.performance_contract(perf, *args)

        perf = self.evidence("performance")
        for row in perf["samples"]:
            row["packed_exact"]["decode_tokens_per_second"] = 96
            row["attention_matching"]["decode_tokens_per_second"] = 89
        with self.assertRaisesRegex(ValueError, "attention-matching decode regression"):
            release.performance_contract(perf, *args)

    def test_forged_duration_without_raw_reports_is_explicitly_incomplete(self):
        self.rewrite("soak", lambda soak: soak.update(duration_seconds=14400, raw_reports=[]))
        result = release.audit(self.manifest, self.root)
        self.assertFalse(result["evidence_passed"])
        self.assertIn("v2_raw_soak_collector", result["incomplete_gates"])

    def test_release_uses_canonical_exact_pair_and_warmup_validation(self):
        self.rewrite("exact_context", lambda record: record["observations"].append({
            **record["observations"][-2], "id": "orphan", "pair_id": "orphan"}))
        with self.assertRaisesRegex(ValueError, "incomplete measured pair"):
            release.audit(self.manifest, self.root)
        self.rewrite("exact_context", lambda record: record["observations"].pop())
        self.rewrite("exact_context", lambda record: record.update(
            observations=[entry for entry in record["observations"] if not entry["warmup"]]))
        with self.assertRaisesRegex(ValueError, "too few exact-context warmups"):
            release.audit(self.manifest, self.root)

    def test_quality_context_claim_includes_reserved_continuation(self):
        self.rewrite("quality", lambda record: record["cases"][0].update(prompt_tokens=8192))
        with self.assertRaisesRegex(ValueError, "quality case outside release claim"):
            release.audit(self.manifest, self.root)
        self.rewrite("quality", lambda record: record["cases"][0].update(prompt_tokens=8160, reserved_output_tokens=0))
        with self.assertRaisesRegex(ValueError, "reserved output positions"):
            release.audit(self.manifest, self.root)

    def test_partial_decode_cannot_qualify_performance_reservation(self):
        perf = self.evidence("performance")
        perf["samples"][0]["output_tokens"] = 1
        model = self.manifest["models"][0]
        with self.assertRaisesRegex(ValueError, "outside declared context cell"):
            release.performance_contract(perf, model["semantic_identity"], model["provenance"], [8192])

    def test_unverifiable_binary_profile_and_source_paths_reject(self):
        model = self.manifest["models"][0]
        binary_path = model["provenance_files"]["binary"]["path"]
        model["provenance_files"]["binary"]["path"] = "missing-binary"
        with self.assertRaisesRegex(ValueError, "unverifiable binary file"):
            release.audit(self.manifest, self.root)

        model["provenance_files"]["binary"]["path"] = binary_path
        profile_sha = model["provenance_files"]["profile"]["sha256"]
        model["provenance_files"]["profile"]["sha256"] = "0" * 64
        with self.assertRaisesRegex(ValueError, "profile file digest mismatch"):
            release.audit(self.manifest, self.root)

        model["provenance_files"]["profile"]["sha256"] = profile_sha
        source = self.root / model["provenance_files"]["source_build"]["path"]
        receipt = release.read_json(source)
        receipt["binary"]["sha256"] = "0" * 64
        source.write_text(json.dumps(receipt, sort_keys=True))
        digest = release.sha(source)
        model["provenance_files"]["source_build"]["sha256"] = digest
        model["provenance"]["source_build_sha256"] = digest
        with self.assertRaisesRegex(ValueError, "does not bind executable"):
            release.audit(self.manifest, self.root)

    @patch.object(release, "quality_upper_bound", return_value=0)
    def test_misbalanced_quality_cells_reject(self, _):
        def add_case(quality):
            extra = dict(quality["cases"][0])
            extra["id"] = "extra-independent-case"
            extra["document_sha256"] = self.digest("extra-independent-document")
            quality["cases"].append(extra)
        self.rewrite("quality", add_case)
        with self.assertRaisesRegex(ValueError, "quality cells are imbalanced"):
            release.audit(self.manifest, self.root)

    def test_raw_numerical_recomputes_sealed_capture_and_metal_outputs(self):
        raw, _ = self.attach_raw_numerical()
        model = self.manifest["models"][0]
        with patch.object(release, "expected_strata", return_value={(4, 0, 4, 0)}):
            result = release.raw_numerical_contract(
                raw, self.root, model["semantic_identity"], model["provenance"],
                model["model"], set(model["calibration_documents"]))
        self.assertEqual(result["consumer_observations"], 1)
        self.assertEqual(result["metal_candidates"], 1)
        with patch.object(release, "expected_strata", return_value={(4, 0, 4, 0)}):
            audited = release.audit(self.manifest, self.root)
        self.assertNotIn("raw_numerical_observations", audited["incomplete_gates"])
        self.assertEqual(audited["models"][0]["raw_numerical"]["metal_candidates"], 1)
        self.assertFalse(audited["evidence_passed"])

    def test_raw_numerical_rejects_forged_summary_and_bad_raw_candidate(self):
        raw, case_dir = self.attach_raw_numerical()
        model = self.manifest["models"][0]
        offline_path = self.root / raw["offline_numerical"]["path"]
        offline = release.read_json(offline_path)
        offline["results"][0]["validation"]["mass_max"] = -1
        offline_path.write_text(json.dumps(offline, sort_keys=True))
        raw["offline_numerical"]["sha256"] = release.sha(offline_path)
        metal_path = self.root / raw["metal_qualification"]["path"]
        terminal = release.read_json(metal_path)
        terminal["numerical_receipt_sha256"] = raw["offline_numerical"]["sha256"]
        metal_path.write_text(json.dumps(terminal, sort_keys=True))
        raw["metal_qualification"]["sha256"] = release.sha(metal_path)
        with patch.object(release, "expected_strata", return_value={(4, 0, 4, 0)}), \
                self.assertRaisesRegex(ValueError, "differs from sealed capture"):
            release.raw_numerical_contract(raw, self.root, model["semantic_identity"], model["provenance"],
                                           model["model"], set(model["calibration_documents"]))

        raw, case_dir = self.attach_raw_numerical()
        bias = case_dir / "candidate-bias.f32"
        np.full(2, np.log(2) + .2, dtype="<f4").tofile(bias)
        metal_path = self.root / raw["metal_qualification"]["path"]
        terminal = release.read_json(metal_path)
        terminal["candidates"][0]["outputs"]["bias.f32"] = release.sha(bias)
        metal_path.write_text(json.dumps(terminal, sort_keys=True))
        raw["metal_qualification"]["sha256"] = release.sha(metal_path)
        with patch.object(release, "expected_strata", return_value={(4, 0, 4, 0)}), \
                self.assertRaisesRegex(ValueError, "consumer gate"):
            release.raw_numerical_contract(raw, self.root, model["semantic_identity"], model["provenance"],
                                           model["model"], set(model["calibration_documents"]))

    def test_resource_guard_is_derived_from_strict_primitive_fields(self):
        valid = {"schema": "antfly.benchmark_resources.v1", "command": ["/bin/collector"],
                 "pass": True, "returncode": 0, "violation": None,
                 "limits": {"timeout_seconds": 300, "max_rss_mib": 2048,
                            "min_free_percent": 15, "max_swap_growth_mib": 0,
                            "min_disk_free_mib": 4096},
                 "samples": 2, "peak_group_rss_mib": 100, "initial_free_percent": 30,
                 "min_free_percent": 20, "swapout_growth_mib": 0,
                 "min_disk_free_mib": 8192, "pid": 123, "elapsed_seconds": 1}
        self.assertEqual(release.resource_guard_contract(valid)["samples"], 2)
        attacks = (
            ("nonzero return", lambda value: value.update(returncode=1)),
            ("hidden violation", lambda value: value.update(violation="OOM")),
            ("swap growth", lambda value: value.update(swapout_growth_mib=1)),
            ("low free memory", lambda value: value.update(min_free_percent=14)),
            ("low free disk", lambda value: value.update(min_disk_free_mib=4095)),
            ("zero samples", lambda value: value.update(samples=0)),
            ("boolean samples", lambda value: value.update(samples=True)),
            ("RSS over cap", lambda value: value.update(peak_group_rss_mib=2049)),
            ("boolean RSS", lambda value: value.update(peak_group_rss_mib=True)),
            ("weak configured floor", lambda value: value["limits"].update(min_free_percent=10)),
            ("nonzero configured swap", lambda value: value["limits"].update(max_swap_growth_mib=1)),
        )
        for name, mutate in attacks:
            with self.subTest(name=name):
                receipt = json.loads(json.dumps(valid))
                mutate(receipt)
                with self.assertRaises(ValueError):
                    release.resource_guard_contract(receipt)

    def test_raw_numerical_rejects_pass_true_with_failed_resource_measurements(self):
        raw, case_dir = self.attach_raw_numerical()
        resource_path = case_dir / "resources.json"
        resources = release.read_json(resource_path)
        resources["returncode"] = 1
        resource_path.write_text(json.dumps(resources, sort_keys=True))
        metal_path = self.root / raw["metal_qualification"]["path"]
        terminal = release.read_json(metal_path)
        terminal["candidates"][0]["resource_receipt_sha256"] = release.sha(resource_path)
        metal_path.write_text(json.dumps(terminal, sort_keys=True))
        raw["metal_qualification"]["sha256"] = release.sha(metal_path)
        model = self.manifest["models"][0]
        with patch.object(release, "expected_strata", return_value={(4, 0, 4, 0)}), \
                self.assertRaisesRegex(ValueError, "did not exit successfully"):
            release.raw_numerical_contract(raw, self.root, model["semantic_identity"], model["provenance"],
                                           model["model"], set(model["calibration_documents"]))

    def test_raw_numerical_rejects_unbound_profile_and_offline_only_solver(self):
        raw, _ = self.attach_raw_numerical()
        model = self.manifest["models"][0]
        args = (raw, self.root, model["semantic_identity"], model["provenance"],
                model["model"], set(model["calibration_documents"]))
        path = self.root / raw["offline_numerical"]["path"]
        offline = release.read_json(path)
        original = json.loads(json.dumps(offline))
        for update, reason in (({"profile_sha256": "0" * 64}, "does not bind the frozen profile"),
                               ({}, "not implemented by serving")):
            offline = json.loads(json.dumps(original))
            offline.update(update)
            if not update:
                offline["results"][0]["recipe"] = "relative_cd_ridge_cpqr"
                offline["results"][0]["solver_identity"] = am.RECIPE_IDENTITIES["relative_cd_ridge_cpqr"]
            path.write_text(json.dumps(offline, sort_keys=True))
            raw["offline_numerical"]["sha256"] = release.sha(path)
            with patch.object(release, "expected_strata", return_value={(4, 0, 4, 0)}), \
                    patch.object(am, "fit_head") as fit, self.assertRaisesRegex(ValueError, reason):
                release.raw_numerical_contract(*args)
            fit.assert_not_called()

    def test_raw_numerical_rejects_rehashed_unrelated_fitter_input(self):
        raw, case_dir = self.attach_raw_numerical()
        model = self.manifest["models"][0]
        input_path = case_dir / "input.bin"
        data = bytearray(input_path.read_bytes())
        data[-1] ^= 1
        input_path.write_bytes(data)
        metal_path = self.root / raw["metal_qualification"]["path"]
        terminal = release.read_json(metal_path)
        terminal["candidates"][0]["input_sha256"] = release.sha(input_path)
        metal_path.write_text(json.dumps(terminal, sort_keys=True))
        raw["metal_qualification"]["sha256"] = release.sha(metal_path)
        with patch.object(release, "expected_strata", return_value={(4, 0, 4, 0)}), \
                self.assertRaisesRegex(ValueError, "differs from sealed fit/validation queries"):
            release.raw_numerical_contract(raw, self.root, model["semantic_identity"], model["provenance"],
                                           model["model"], set(model["calibration_documents"]))

    def test_raw_numerical_rejects_loaded_layout_or_implementation_drift(self):
        raw, _ = self.attach_raw_numerical()
        model = self.manifest["models"][0]
        for field in ("layout", "implementation"):
            with self.subTest(field=field), patch.object(am, "load_capture") as load, \
                    self.assertRaisesRegex(ValueError, "model identity mismatch"):
                identity = {**model["semantic_identity"], field: "0" * 64}
                release.raw_numerical_contract(raw, self.root, identity, model["provenance"],
                                               model["model"], set(model["calibration_documents"]))
            load.assert_not_called()

    def test_raw_numerical_cannot_substitute_a_different_frozen_retention(self):
        raw, _ = self.attach_raw_numerical()
        model = self.manifest["models"][0]
        path = self.root / raw["profile"]["path"]
        profile = release.read_json(path)
        next(iter(profile["profiles"].values()))["retained_rows"] = 3
        path.write_text(json.dumps(profile, sort_keys=True))
        raw["profile"]["sha256"] = model["semantic_identity"]["profile"] = release.sha(path)
        offline_path = self.root / raw["offline_numerical"]["path"]
        offline = release.read_json(offline_path)
        offline["profile_sha256"] = raw["profile"]["sha256"]
        offline_path.write_text(json.dumps(offline, sort_keys=True))
        raw["offline_numerical"]["sha256"] = release.sha(offline_path)
        with patch.object(release, "expected_strata", return_value={(4, 0, 4, 0)}), \
                patch.object(am, "fit_head") as fit, \
                self.assertRaisesRegex(ValueError, "differs from frozen profile"):
            release.raw_numerical_contract(raw, self.root, model["semantic_identity"], model["provenance"],
                                           model["model"], set(model["calibration_documents"]))
        fit.assert_not_called()

    def test_raw_numerical_cannot_pick_multiple_attempts_for_one_head(self):
        raw, _ = self.attach_raw_numerical()
        model = self.manifest["models"][0]
        path = self.root / raw["offline_numerical"]["path"]
        offline = release.read_json(path)
        extra = dict(offline["results"][0], retained_rows=3)
        offline["results"].append(extra)
        path.write_text(json.dumps(offline, sort_keys=True))
        raw["offline_numerical"]["sha256"] = release.sha(path)
        with patch.object(release, "expected_strata", return_value={(4, 0, 4, 0)}), \
                patch.object(am, "fit_head") as fit, \
                self.assertRaisesRegex(ValueError, "exactly one candidate per capture/head"):
            release.raw_numerical_contract(raw, self.root, model["semantic_identity"], model["provenance"],
                                           model["model"], set(model["calibration_documents"]))
        fit.assert_not_called()

    def test_raw_numerical_preflight_limits_before_loading_or_fitting(self):
        raw, _ = self.attach_raw_numerical()
        model = self.manifest["models"][0]
        args = (raw, self.root, model["semantic_identity"], model["provenance"],
                model["model"], set(model["calibration_documents"]))
        with patch.object(release, "MAX_RAW_NUMERICAL_CAPTURES", 0), \
                patch.object(am, "load_capture") as load, \
                self.assertRaisesRegex(ValueError, "capture count exceeds audit limit"):
            release.raw_numerical_contract(*args)
        load.assert_not_called()
        with patch.object(release, "MAX_RAW_NUMERICAL_INPUT_BYTES", 0), \
                patch.object(am, "load_capture") as load, \
                self.assertRaisesRegex(ValueError, "capture bytes exceed audit limit"):
            release.raw_numerical_contract(*args)
        load.assert_not_called()
        with patch.object(release, "MAX_RAW_NUMERICAL_METADATA_BYTES", 0), \
                patch.object(am, "load_capture") as load, \
                self.assertRaisesRegex(ValueError, "metadata bytes exceed audit limit"):
            release.raw_numerical_contract(*args)
        load.assert_not_called()
        with patch.object(release, "MAX_RAW_NUMERICAL_FITS", 0), \
                patch.object(release, "expected_strata", return_value={(4, 0, 4, 0)}), \
                patch.object(am, "load_capture") as load, \
                self.assertRaisesRegex(ValueError, "fit count exceeds audit limit"):
            release.raw_numerical_contract(*args)
        load.assert_not_called()
        with patch.object(release, "MAX_RAW_NUMERICAL_FIT_WORK_BYTES", 0), \
                patch.object(release, "expected_strata", return_value={(4, 0, 4, 0)}), \
                patch.object(am, "load_capture") as load, \
                self.assertRaisesRegex(ValueError, "aggregate fit work exceeds audit limit"):
            release.raw_numerical_contract(*args)
        load.assert_not_called()


if __name__ == "__main__":
    unittest.main()
