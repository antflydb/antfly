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

"""Synthetic evidence tests. These fixtures are never release qualification."""
import json
import hashlib
import itertools
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch
from types import SimpleNamespace

import release_attention_matching as release
import release_build_fixture
import exact_context_qualify as exact_context


class ReleaseTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.addCleanup(self.temp.cleanup)

    def identity(self, model=0):
        return {**{key: str(model + 1) * 64 for key in release.BINDINGS},
                "dtype": "f16_kv_f32_bias", "kernel_route": "metal_context_optimized"}

    def report(self, identity=None):
        return {"schema_version": 1, "identity": identity or self.identity(),
                "published": True, "exact_fallback": False, "committed_before_bytes": 8192,
                "committed_after_bytes": 4096, "backend_kv_readback_bytes": 0,
                "swapout_growth_bytes": 0, "memory_guard_passed": True,
                "isolation_passed": True, "rollback_passed": True,
                "live_bytes_before": 1024, "live_bytes_after_release": 1024,
                "route": "native", "prompt_tokens": 4096}

    def manifest(self):
        models = []
        for index, name in enumerate(("gemma4-e2b", "gemma4-e4b")):
            identity = self.identity(index)
            cases = []
            for bucket in sorted(release.BUCKETS):
                for length in (4096, 8192):
                    cases.append({**self.report(identity), "id": f"{bucket}-{length}", "bucket": bucket,
                                  "prompt_tokens": length, "document_sha256": "b" * 64,
                                  "exact_correct": True, "candidate_correct": True, "scored_tokens": 20,
                                  "exact_nll_sum": 10, "candidate_nll_sum": 10})
            trials = [{"start_seconds": i * 2880, "end_seconds": (i + 1) * 2880,
                       "report": {**self.report(identity), "route": route, "request_index": i}, "resources": {"pass": True, "swapout_growth_bytes": 0, "min_free_percent": 20, "min_disk_free_mib": 8192, "peak_group_rss_mib": 1024}}
                      for i, route in enumerate(sorted(release.ROUTES))]
            records = {"numerical": {"frozen_before_evaluation": True, "all_owner_heads_covered": True,
                                     "metal_fp64_parity_passed": True,
                                     "consumers": [{"owner_layer": owner, "kv_head": kv, "consumer_layer": consumer, "query_head": head, "mass_max": .04, "output_relative_rmse": .09, "after_f16": True} for owner, kv, consumer, head in sorted(release.expected_strata(name))]},
                       "quality": {"cases": cases},
                       "soak": {"persistent_driver": True, "completed": True, "duration_seconds": 14400, "trials": trials, "max_rss_mib": 8192},
                       "failures": {"cases": [{"fault": fault, "rollback_passed": True, "isolation_passed": True,
                                                "released_all_allocations": True} for fault in release.FAILURES]},
                       "performance": {"clean_memory": True, "matched_output_identity": True, "exact_path_latency_ratio": 1.04}}
            model = {"model": name, "identity": identity, "calibration_documents": ["a" * 64]}
            for kind, record in records.items():
                path = self.root / f"{name}-{kind}.json"
                release.write_json(path, {"identity": identity, **record})
                model[kind] = {"path": path.name, "sha256": release.sha(path)}
            models.append(model)
        return {"schema_version": 1, "models": models}

    def rewrite(self, manifest, kind, mutate):
        entry = manifest["models"][0][kind]
        path = self.root / entry["path"]
        record = release.read_json(path)
        mutate(record)
        path.write_text(json.dumps(record))
        entry["sha256"] = release.sha(path)

    def test_complete_synthetic_evidence_cannot_enable_serving(self):
        result = release.audit(self.manifest(), self.root)
        self.assertTrue(result["evidence_passed"])
        self.assertTrue(result["diagnostic_only"])
        self.assertFalse(result["serving_qualified"])
        self.assertEqual(len(result["models"]), 2)

    def test_numerical_failure_and_missing_identity_reject(self):
        manifest = self.manifest()
        self.rewrite(manifest, "numerical", lambda record: record["consumers"][0].update(mass_max=.051))
        with self.assertRaisesRegex(ValueError, "numerical gate"):
            release.audit(manifest, self.root)
        manifest["models"][0]["identity"]["profile"] = "d" * 64
        with self.assertRaisesRegex(ValueError, "mismatched"):
            release.audit(manifest, self.root)

    def test_fallback_leak_unclean_memory_and_no_allocation_savings_reject(self):
        for key, value in (("published", False), ("exact_fallback", True), ("committed_after_bytes", 8192),
                           ("swapout_growth_bytes", 1), ("backend_kv_readback_bytes", 4),
                           ("live_bytes_after_release", 2048), ("isolation_passed", False)):
            with self.subTest(key=key), self.assertRaises(ValueError):
                release.report_contract({**self.report(), key: value}, self.identity())

    def test_quality_regression_and_calibration_overlap_reject(self):
        manifest = self.manifest()
        self.rewrite(manifest, "quality", lambda record: record["cases"][0].update(candidate_correct=False))
        with self.assertRaisesRegex(ValueError, "quality regression"):
            release.audit(manifest, self.root)
        self.rewrite(manifest, "quality", lambda record: record["cases"][0].update(candidate_correct=True, document_sha256="a" * 64))
        with self.assertRaisesRegex(ValueError, "overlaps calibration"):
            release.audit(manifest, self.root)

    def test_short_or_idle_soak_cannot_qualify(self):
        manifest = self.manifest()
        self.rewrite(manifest, "soak", lambda record: record.update(duration_seconds=14399))
        with self.assertRaisesRegex(ValueError, "four-hour"):
            release.audit(manifest, self.root)
        self.rewrite(manifest, "soak", lambda record: record.update(duration_seconds=14400, trials=[]))
        with self.assertRaisesRegex(ValueError, "four-hour"):
            release.audit(manifest, self.root)

    def test_digest_duplicate_json_and_nonfinite_reject(self):
        manifest = self.manifest()
        manifest["models"][0]["quality"]["sha256"] = "0" * 64
        with self.assertRaisesRegex(ValueError, "digest mismatch"):
            release.audit(manifest, self.root)
        for content in ('{"x":1,"x":2}', '{"x":NaN}'):
            path = self.root / "bad.json"
            path.write_text(content)
            with self.assertRaises(ValueError):
                release.read_json(path)

    @patch.object(release, "group_rss_mib", return_value=64)
    @patch.object(release, "memory_free_percent", return_value=50)
    @patch.object(release, "swapout_bytes", return_value=0)
    @patch.object(release.shutil, "disk_usage", return_value=SimpleNamespace(free=20 * 1024**3))
    def test_serial_smoke_and_timeout_leave_terminal_receipts(self, *_):
        report = self.report()
        command = [sys.executable, "-c", "import sys,json\nr=json.loads(" + repr(json.dumps(report)) + ")\nfor line in sys.stdin:\n c=json.loads(line)\n if c['command']=='stop': break\n r['request_index']=c['request_index']\n print(json.dumps(r),flush=True)"]
        result = release.soak(command, self.identity(), self.root / "smoke", duration=.05, timeout=5)
        self.assertTrue(result["completed"])
        self.assertFalse(result["serving_qualified"])
        self.assertGreater(len(result["trials"]), 0)
        self.assertTrue(result["persistent_driver"])
        self.assertTrue((self.root / "smoke/trials.jsonl").exists())
        with self.assertRaisesRegex(ValueError, "timed out"):
            release.soak([sys.executable, "-c", "import time; time.sleep(20)"], self.identity(), self.root / "timeout", duration=.1, timeout=.05)
        self.assertFalse(release.read_json(self.root / "timeout/soak.json")["completed"])

    def test_missing_shared_consumer_and_resource_failures_reject(self):
        manifest = self.manifest()
        self.rewrite(manifest, "numerical", lambda record: record["consumers"].pop())
        with self.assertRaisesRegex(ValueError, "consumer/head"):
            release.audit(manifest, self.root)

    @patch.object(release, "group_rss_mib", return_value=64)
    @patch.object(release, "memory_free_percent", return_value=5)
    @patch.object(release, "swapout_bytes", return_value=0)
    @patch.object(release.shutil, "disk_usage", return_value=SimpleNamespace(free=20 * 1024**3))
    def test_soak_resource_guard_refuses_before_launch(self, *_):
        with patch.object(release.subprocess, "Popen") as launch:
            with self.assertRaisesRegex(ValueError, "resource guard"):
                release.soak(["unused"], self.identity(), self.root / "refused", duration=.01)
            launch.assert_not_called()
        self.assertFalse(release.read_json(self.root / "refused/soak.json")["completed"])


class ReleaseV2Tests(ReleaseTests):
    def setUp(self):
        super().setUp()
        self.v2_counter = 0

    def semantic(self, model=0):
        return {**{key: format(model + index + 1, "x")[-1] * 64 for index, key in enumerate(sorted(release.SEMANTIC_BINDINGS))},
                "dtype": "f16_kv_f32_bias", "kernel_route": "metal_context_optimized"}

    def origin(self, model=0):
        return {key: format(model + index + 9, "x")[-1] * 64 for index, key in enumerate(sorted(release.PROVENANCE_BINDINGS))}

    def v2_report(self, identity, origin, **overrides):
        value = {"schema_version": 2, "semantic_identity": identity, "provenance": origin,
                 "published": True, "exact_fallback": False, "committed_before_bytes": 10000,
                 "committed_after_bytes": 7000, "backend_kv_readback_bytes": 0,
                 "swapout_growth_bytes": 0, "live_bytes_before": 100, "live_bytes_after_release": 100,
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

    def v2_manifest(self):
        self.v2_counter += 1
        prefix = f"v2-{self.v2_counter}"
        name, length, reserved = "gemma4-e2b", 8192, 32
        artifact, profile = self.root / f"{prefix}-model.gguf", self.root / f"{prefix}-profile.json"
        binary, source = self.root / f"{prefix}-binary", self.root / f"{prefix}-source.json"
        artifact.write_bytes(b"model")
        profile.write_bytes(b"profile")
        binary.write_bytes(b"binary")
        release_build_fixture.write(self.root, source, binary)
        identity, origin = self.semantic(), self.origin()
        identity.update(artifact=release.sha(artifact), profile=release.sha(profile))
        origin.update(binary_sha256=release.sha(binary), source_build_sha256=release.sha(source))
        cases = []
        for bucket in sorted(release.BUCKETS):
            for index in range(release.MIN_QUALITY_CASES):
                cases.append({**self.v2_report(identity, origin), "id": f"{bucket}-{index}", "bucket": bucket,
                              "document_sha256": hashlib.sha256(f"{bucket}-{index}".encode()).hexdigest(),
                              "exact_correct": True, "candidate_correct": True, "scored_tokens": 20,
                              "exact_nll_sum": 10, "candidate_nll_sum": 10})
        settings = {variant: hashlib.sha256(variant.encode()).hexdigest()
                    for variant in release.PERFORMANCE_VARIANTS}
        orders = list(itertools.permutations(release.PERFORMANCE_VARIANTS))
        def perf_row(index, warmup=False):
            prefix_name = "warmup" if warmup else "pair"
            variants = {}
            for variant in release.PERFORMANCE_VARIANTS:
                variants[variant] = {"completed": True, "attempt_id": f"{prefix_name}-{index}-{variant}",
                                     "settings_sha256": settings[variant], "ttft_ms": 100.0,
                                     "decode_tokens_per_second": 100.0, "committed_context_bytes": 10000,
                                     "swapout_growth_bytes": 0, "output_sha256": "e" * 64}
            variants["packed_exact"]["ttft_ms"] = 104.0
            variants["attention_matching"].update(ttft_ms=125.0, decode_tokens_per_second=94.0,
                                                   committed_context_bytes=7000, published=True,
                                                   exact_fallback=False)
            return {"pair_id": f"{prefix_name}-{index}",
                    "prompt_sha256": hashlib.sha256(f"{prefix_name}-prompt-{index}".encode()).hexdigest(),
                    "settings_sha256": "f" * 64, "context_tokens": length,
                    "reserved_output_tokens": reserved, "prompt_tokens": length - reserved,
                    "output_tokens": reserved, "order": list(orders[index % len(orders)]), **variants}
        perf = [perf_row(index) for index in range(release.MIN_PERFORMANCE_PAIRS)]
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
                                                           "prompt_sha256": hashlib.sha256(f"exact-prompt-{index}".encode()).hexdigest(),
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

        trials = []
        for index, route in enumerate(sorted(release.ROUTES)):
            trials.append({"start_seconds": index * 2880, "end_seconds": (index + 1) * 2880,
                           "report": self.v2_report(identity, origin, route=route, request_index=index),
                           "resources": {"swapout_growth_bytes": 0, "min_free_percent": 20,
                                         "min_disk_free_mib": 8192, "peak_group_rss_mib": 1024}})
        records = {
            "exact_context": {"schema_version": 1, "completed": True,
                              "file_provenance": {"artifact_sha256": identity["artifact"],
                                                  "binary_sha256": origin["binary_sha256"],
                                                  "source_build_receipt_sha256": origin["source_build_sha256"]}, "evidence_passed": True,
                              "loaded_identity_verified": True, "qualified_context_tokens": [length],
                              "results": [{"context_tokens": length, "pairs": release.MIN_PERFORMANCE_PAIRS,
                                           "packed_exact_elapsed_ratio_upper_95": 1.04, "packed_exact_decode_ratio_lower_95": 1.0}],
                              "observations": exact_observations},
            "numerical": {"frozen_before_evaluation": True, "all_owner_heads_covered": True,
                          "metal_fp64_parity_passed": True,
                          "consumers": [{"owner_layer": owner, "kv_head": kv, "consumer_layer": consumer,
                                         "query_head": head, "mass_max": .04, "output_relative_rmse": .09,
                                         "after_f16": True} for owner, kv, consumer, head in sorted(release.expected_strata(name))]},
            "quality": {"cases": cases},
            "soak": {"persistent_driver": True, "completed": True, "duration_seconds": 14400,
                     "trials": trials, "max_rss_mib": 8192},
            "failures": {"cases": [{"fault": fault, "live_bytes_before": 100,
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
                                     for fault in release.FAILURES]},
            "performance": {"schema_version": 2,
                            "cells": [{"context_tokens": length, "reserved_output_tokens": reserved,
                                       "generation_settings_sha256": "f" * 64,
                                       "variant_settings_sha256": settings}],
                            "warmups": [perf_row(index, True) for index in range(release.MIN_PERFORMANCE_WARMUPS)],
                            "samples": perf},
        }
        reload_case = next(case for case in records["failures"]["cases"] if case["fault"] == "reload")
        reload_case.update(
            request_generation=reload_case["loaded_generation_after"],
            stale_selection_rejected=True,
        )
        reload_case["events"] = [
            {"name": event, "monotonic_ns": index, "live_bytes": 100}
            for index, event in enumerate((
                "fault_injected",
                "model_generation_reloaded",
                "stale_selection_rejected",
                "fresh_attention_matching_published",
                "request_released",
            ))
        ]
        model = {"model": name, "semantic_identity": identity, "provenance": origin,
                 "qualified_context_tokens": [length], "calibration_documents": ["a" * 64],
                 "provenance_files": {
                     "artifact": {"path": artifact.name, "sha256": release.sha(artifact)},
                     "profile": {"path": profile.name, "sha256": release.sha(profile)},
                     "source_build": {"path": source.name, "sha256": release.sha(source)},
                     "binary": {"path": binary.name, "sha256": release.sha(binary)}}}
        for kind, record in records.items():
            path = self.root / f"{prefix}-{kind}.json"
            release.write_json(path, {"semantic_identity": identity, "provenance": origin, **record})
            model[kind] = {"path": path.name, "sha256": release.sha(path)}
        return {"schema_version": 2, "models": [model]}

    def v2_rewrite(self, manifest, kind, mutate):
        entry = manifest["models"][0][kind]
        path = self.root / entry["path"]
        record = release.read_json(path)
        mutate(record)
        path.write_text(json.dumps(record))
        entry["sha256"] = release.sha(path)

    def test_v2_independent_artifact_passes_with_derived_bounds(self):
        result = release.audit(self.v2_manifest(), self.root)
        self.assertEqual([row["model"] for row in result["models"]], ["gemma4-e2b"])
        self.assertLessEqual(result["models"][0]["performance_bounds"][0]["attention_matching_ttft_ratio_upper_95"], 1.25)
        self.assertFalse(result["evidence_passed"])
        self.assertTrue(result["diagnostic_only"])
        self.assertFalse(result["serving_qualified"])

    def test_v2_rejects_sparse_samples_claimed_savings_and_binary_mismatch(self):
        manifest = self.v2_manifest()
        self.v2_rewrite(manifest, "performance", lambda row: row.update(samples=row["samples"][:-1]))
        with self.assertRaisesRegex(ValueError, "too few paired"):
            release.audit(manifest, self.root)
        manifest = self.v2_manifest()
        self.v2_rewrite(manifest, "performance", lambda row: [sample["attention_matching"].update(committed_context_bytes=7600)
                                                                for sample in row["samples"]])
        with self.assertRaisesRegex(ValueError, "savings below"):
            release.audit(manifest, self.root)
        manifest = self.v2_manifest()
        manifest["models"][0]["provenance"]["binary_sha256"] = "0" * 64
        with self.assertRaisesRegex(ValueError, "provenance mismatch"):
            release.audit(manifest, self.root)

    def test_v2_rejects_underpowered_quality_and_uncertain_regression(self):
        manifest = self.v2_manifest()
        self.v2_rewrite(manifest, "quality", lambda row: row["cases"].pop())
        with self.assertRaisesRegex(ValueError, "too few quality"):
            release.audit(manifest, self.root)
        manifest = self.v2_manifest()
        self.v2_rewrite(manifest, "quality", lambda row: [case.update(candidate_correct=False) for case in row["cases"][:20]])
        with self.assertRaisesRegex(ValueError, "uncertainty gate"):
            release.audit(manifest, self.root)

    def test_v2_derives_failure_rollback_from_ordered_events(self):
        manifest = self.v2_manifest()
        def forge(record):
            case = next(case for case in record["cases"] if case["fault"] == "oom")
            case["events"].insert(2, {"name": "publication_commit", "monotonic_ns": 2,
                                      "live_bytes": 200})
        self.v2_rewrite(manifest, "failures", forge)
        with self.assertRaisesRegex(ValueError, "published after injection"):
            release.audit(manifest, self.root)


class ExactContextTests(unittest.TestCase):
    def loaded_identity(self):
        return {"load_generation": 1, "artifact_sha256": "a" * 64,
                "tokenizer_sha256": "a" * 64, "template_sha256": "a" * 64,
                "layout_sha256": "a" * 64, "implementation_sha256": "a" * 64}

    def observation(self, storage="automatic"):
        return {"schema_version": 2, "route": "native_pipeline", "storage": storage,
                "prompt_tokens": 7680, "reserved_output_tokens": 512, "produced_output_tokens": 32,
                "total_positions": 7712, "prefill_chunk_rows": 512, "output_sha256": "a" * 64,
                "elapsed_ns": 1_000_000, "first_callback_ns": 900_000, "callback_count": 32,
                "phases_ms": {"prefill": .8, "decode": .2}, "context_live_bytes": None,
                "context_peak_bytes": None, "backend_kv_readback_bytes": None,
                "loaded_identity": self.loaded_identity()}

    def test_parser_preserves_unknown_counters_and_rejects_fabricated_shape(self):
        rows = exact_context.parse_observations(exact_context.PREFIX + json.dumps(self.observation()) + "\n")
        self.assertIsNone(exact_context.observation_contract(rows[0])["context_peak_bytes"])
        broken = self.observation()
        broken["total_positions"] += 1
        with self.assertRaisesRegex(ValueError, "total positions"):
            exact_context.observation_contract(broken)

    def test_diagnostic_receipt_cannot_promote_without_loaded_identity(self):
        receipt = {"schema_version": 1, "completed": True, "loaded_identity_verified": False,
                   "file_provenance": {"artifact_sha256": "a" * 64, "binary_sha256": "b" * 64,
                                       "source_build_receipt_sha256": "b" * 64},
                   "observations": [{"observation": self.observation()}]}
        del receipt["observations"][0]["observation"]["loaded_identity"]
        identity = {**{key: "a" * 64 for key in release.SEMANTIC_BINDINGS},
                    "dtype": "f16_kv_f32_bias", "kernel_route": "metal_context_optimized"}
        origin = {key: "b" * 64 for key in release.PROVENANCE_BINDINGS}
        with self.assertRaisesRegex(ValueError, "loaded identity"):
            exact_context.audit(receipt, identity, origin, [8192])


if __name__ == "__main__":
    unittest.main()
