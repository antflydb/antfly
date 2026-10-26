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

"""Numerical, provenance, and qualification isolation tests for the offline tool."""
import copy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest

import attention_matching_calibrate as am
import numpy as np


class CalibrationTests(unittest.TestCase):
    def test_profile_engine_identity_matches_native_golden_vectors(self):
        self.assertEqual(am.profile_semantic_identity([], [], 8192, 32, 1),
                         "2c46e69233a79a1f400df9fdeeaff474b1a77903240d22df6830841067314540")
        self.assertEqual(am.profile_semantic_identity([], [], 8192, 32, 1, "native_whole_head_v2"),
                         "f513982df6299aa54157a75f483e82afff075684dfd39c4795f5cbc525612b85")
        with self.assertRaisesRegex(ValueError, "unknown attention matching engine"):
            am.profile_semantic_identity([], [], 8192, 32, 1, "unknown")

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.addCleanup(self.temp.cleanup)

    def capture(self, name="calibration", purpose="calibration", keys=None, values=None, train=None, held=None,
                schema=1, evaluation=None):
        directory = self.root / name
        directory.mkdir()
        keys = np.asarray(keys if keys is not None else [0, 0, 0, 0], dtype="<f2").reshape(-1, 1, 1)
        values = np.asarray(values if values is not None else [-2, -1, 1, 2], dtype="<f2").reshape(keys.shape)
        train = np.asarray(train if train is not None else [-1, 0, 1], dtype="<f4")
        held = np.asarray(held if held is not None else [-2, 2], dtype="<f4")
        evaluation = np.asarray(evaluation if evaluation is not None else [], dtype="<f4")
        queries = np.concatenate([train, held, evaluation]).reshape(1, -1, 1)
        if schema == 1:
            positions = np.concatenate([am.replay_positions(100, 20, len(train), False),
                                        am.replay_positions(100, 20, len(held), True)]).reshape(1, -1)
        else:
            self.assertGreaterEqual(len(train), 3)
            self.assertGreaterEqual(len(held), 2)
            fit_question = am.replay_positions(100, 20, 1, False)
            fit_study = am.replay_positions(100, 20, 1, False)
            fit_response = am.replay_positions(120, 20, len(train) - 2, False)
            gate = am.replay_positions(100, 20, 1, True)
            gate_response = am.replay_positions(120, 20, len(held) - 1, True)
            teacher = am.replay_positions(400, 20, len(evaluation), True) if len(evaluation) else np.asarray([], dtype="<u4")
            positions = np.concatenate([fit_question, fit_study, fit_response, gate, gate_response, teacher]).reshape(1, -1)
        for key, array in {"keys": keys, "values": values, "queries": queries, "positions": positions}.items():
            array.tofile(directory / (key + ".raw"))
        spec = {"schema_version": schema, "purpose": purpose,
                "model": {"id": "fixture", "sha256": "a" * 64, "tokenizer_sha256": "b" * 64, "template_sha256": "c" * 64,
                          "layout_sha256": "8" * 64, "implementation_sha256": "9" * 64,
                          "global_owner_shapes": [{"layer": 4, "heads": 1, "dim": 1}]},
                "document": {"id": name, "sha256": hashlib.sha256(name.encode()).hexdigest(), "bucket": "document_qa"},
                "source": {"git_head": "d" * 40, "files": {"capture.zig": "e" * 64}},
                "capture": {"rows": len(keys), "heads": 1, "dim": 1, "training_rows": len(train), "validation_rows": len(held),
                            "block_start": 0, "owner_layer": 4, "scale": 1., "query_origin": "repeat_prefill", "replay_start": 100, "replay_tokens": 20,
                            "query_sampling": "uniform_partition_v1",
                            "consumers": [{"layer": 4, "query_heads": 1}],
                            "strata": [{"consumer_layer": 4, "query_head": 0, "kv_head": 0, "training_start": 0, "training_count": len(train), "validation_start": 0, "validation_count": len(held)}]},
                "tensors": {k: k + ".raw" for k in ("keys", "values", "queries", "positions")}}
        if schema == 2:
            c = spec["capture"]
            c.update(evaluation_rows=len(evaluation), query_origin="multi_branch_v2",
                     query_sampling="source_uniform_partition_v3")
            c.pop("replay_start")
            c.pop("replay_tokens")
            sources = [
                {"id": "request-question", "partition": "training", "kind": "request_question_replay",
                 "capture_route": "private_model_replay", "conditioning": "completed_prompt_private_replay",
                 "logical_start": 100, "logical_count": 20, "tokens_sha256": hashlib.sha256(b"question").hexdigest(),
                 "template_sha256": "c" * 64, "sampling_partition": "training", "selection_visible": True, "private": True, "usage_visible": False},
                {"id": "fit-study", "partition": "training", "kind": "fixed_study_prefill",
                 "capture_route": "private_model_replay", "conditioning": "completed_prompt_private_replay",
                 "logical_start": 100, "logical_count": 20, "tokens_sha256": hashlib.sha256(b"fit-study").hexdigest(),
                 "template_sha256": "1" * 64, "sampling_partition": "training", "selection_visible": True, "private": True, "usage_visible": False, "predeclared": True, "study_pair_id": "fit-pair"},
                {"id": "fit-response", "partition": "training", "kind": "fixed_study_generated_response",
                 "capture_route": "private_model_replay", "conditioning": "completed_prompt_study_generated_response",
                 "logical_start": 120, "logical_count": 20, "tokens_sha256": hashlib.sha256(b"fit-response").hexdigest(),
                 "template_sha256": "1" * 64, "sampling_partition": "training", "selection_visible": True, "private": True, "usage_visible": False, "predeclared": True, "study_pair_id": "fit-pair", "generation_contract_sha256": "6" * 64,
                 "generation_mode": "greedy_fixed_length_ignore_eos"},
                {"id": "gate-study", "partition": "validation", "kind": "fixed_study_prefill",
                 "capture_route": "private_model_replay", "conditioning": "completed_prompt_private_replay",
                 "logical_start": 100, "logical_count": 20, "tokens_sha256": hashlib.sha256(b"gate-study").hexdigest(),
                 "template_sha256": "2" * 64, "sampling_partition": "validation", "selection_visible": False, "private": True, "usage_visible": False, "predeclared": True, "study_pair_id": "gate-pair"},
                {"id": "gate-response", "partition": "validation", "kind": "fixed_study_generated_response",
                 "capture_route": "private_model_replay", "conditioning": "completed_prompt_study_generated_response",
                 "logical_start": 120, "logical_count": 20, "tokens_sha256": hashlib.sha256(b"gate-response").hexdigest(),
                 "template_sha256": "2" * 64, "sampling_partition": "validation", "selection_visible": False, "private": True, "usage_visible": False, "predeclared": True, "study_pair_id": "gate-pair", "generation_contract_sha256": "7" * 64,
                 "generation_mode": "greedy_fixed_length_ignore_eos"},
            ]
            if len(evaluation):
                sources.append({"id": "teacher", "partition": "evaluation", "kind": "exact_teacher_continuation",
                                "capture_route": "private_model_replay", "conditioning": "completed_prompt_private_replay",
                                "logical_start": 400, "logical_count": 20, "tokens_sha256": hashlib.sha256((name + "-teacher").encode()).hexdigest(),
                                "template_sha256": "3" * 64, "sampling_partition": "validation", "selection_visible": False, "private": True, "usage_visible": False})
            loaded_identity = {"load_generation": 1, "artifact_sha256": "a" * 64,
                               "tokenizer_sha256": "b" * 64, "template_sha256": "c" * 64,
                               "layout_sha256": "8" * 64, "implementation_sha256": "9" * 64}
            for source in sources:
                source["loaded_identity"] = loaded_identity
                source["physical_branch_id"] = ("fit-pair" if source["id"] in {"fit-study", "fit-response"} else
                                                "gate-pair" if source["id"] in {"gate-study", "gate-response"} else
                                                source["id"])
                branch = ({"request-question": (100, 20, 1),
                           "fit-pair": (100, 40, len(train) - 1),
                           "gate-pair": (100, 40, len(held)),
                           "teacher": (400, 20, len(evaluation))})[source["physical_branch_id"]]
                source.update(branch_logical_start=branch[0], branch_logical_count=branch[1],
                              branch_rows_per_stratum=branch[2])
            c["query_sources"] = sources
            stratum = c["strata"][0]
            stratum["query_source_ranges"] = {
                "training": [{"source_id": "request-question", "start": 0, "count": 1},
                             {"source_id": "fit-study", "start": 1, "count": len(fit_study)},
                             {"source_id": "fit-response", "start": 1 + len(fit_study), "count": len(fit_response)}],
                "validation": [{"source_id": "gate-study", "start": 0, "count": len(gate)},
                               {"source_id": "gate-response", "start": len(gate), "count": len(gate_response)}],
                "evaluation": ([{"source_id": "teacher", "start": 0, "count": len(evaluation)}] if len(evaluation) else []),
            }
            if len(evaluation):
                stratum.update(evaluation_start=0, evaluation_count=len(evaluation))
            branch_runs = []
            for index, branch_id in enumerate(dict.fromkeys(source["physical_branch_id"] for source in sources)):
                branch_runs.append({"physical_branch_id": branch_id,
                                    "source_ids": [source["id"] for source in sources if source["physical_branch_id"] == branch_id],
                                    "completed": True, "cancelled": False,
                                    "elapsed_ns": 100 + index, "physical_bytes": 10_000 + 100 * index,
                                    "query_bytes": 1_000 + 10 * index, "status_readback_bytes": 4,
                                    "kv_readback_bytes": 0})
            by_id = {source["id"]: source for source in sources}
            serving_runs = [run for run in branch_runs if all(by_id[x]["partition"] != "evaluation" for x in run["source_ids"])]
            evaluation_runs = [run for run in branch_runs if all(by_id[x]["partition"] == "evaluation" for x in run["source_ids"])]
            c["query_campaign"] = {
                "accounting": "serial_physical_branch_upper_bound_v2", "serial_branches": True,
                "branch_runs": branch_runs,
                "serving_elapsed_ns": sum(x["elapsed_ns"] for x in serving_runs),
                "evaluation_elapsed_ns": sum(x["elapsed_ns"] for x in evaluation_runs),
                "serving_query_bytes": sum(x["query_bytes"] for x in serving_runs),
                "projected_serving_peak_bytes": max(x["physical_bytes"] - x["query_bytes"] for x in serving_runs) +
                                                sum(x["query_bytes"] for x in serving_runs),
                "observed_peak_bytes": max(x["physical_bytes"] for x in branch_runs),
                "evaluation_peak_bytes": max((x["physical_bytes"] for x in evaluation_runs), default=0),
            }
            c["runtime_block_coverage"] = {"receipt_sha256": "d" * 64,
                                            "prompt_tokens_sha256": "e" * 64,
                                            "qualified_context_tokens": 4096,
                                            "prompt_tokens": 4088,
                                            "reserved_output_tokens": 8}
            c["runtime_query_pool"] = am.runtime_query_pool_observation(c)
            c["query_protocol_sha256"] = am.query_protocol_identity(c)
        am.write_json(directory / "spec.json", spec)
        manifest = am.seal(directory / "spec.json", directory / "sealed")
        return manifest, *am.load_capture(manifest)

    def test_unequal_mass_value_fit_and_held_query_independence(self):
        _, meta, arrays = self.capture(keys=[-1, -1, -1, 1], values=[-1, -2, -3, 4], train=[-.5, .5, 1], held=[-1.5, 0, 2])
        for recipe in am.RECIPES:
            report = am.fit_head(meta, arrays, 0, 2, recipe)
            self.assertTrue(report["accepted"], report)
            self.assertEqual(report["selected_source_rows"], [0, 3])
            self.assertLess(report["validation"]["mass_max"], .0001)
            self.assertLess(report["validation"]["output_relative_rmse"], .002)
        _, other_meta, other_arrays = self.capture("new-held", keys=[-1, -1, -1, 1], values=[-1, -2, -3, 4], train=[-.5, .5, 1], held=[-3, 3, 4])
        for recipe in am.RECIPES:
            first = am.fit_head(meta, arrays, 0, 2, recipe)
            second = am.fit_head(other_meta, other_arrays, 0, 2, recipe)
            self.assertEqual(first["selected_source_rows"], second["selected_source_rows"])
            self.assertEqual(first["training"], second["training"])

    def test_pinned_omp_is_deterministic_and_distinct_from_batched_pursuit(self):
        rng = np.random.default_rng(1)
        for _ in range(3):
            features = rng.uniform(.01, 1, (8, 6))
        target = features.sum(axis=1)
        selected, weights = am.upstream_omp(features, target, 3)
        repeated, repeated_weights = am.upstream_omp(features, target, 3)
        pursuit, _ = am.pursuit(features / target[:, None], 3)
        self.assertEqual(selected.tolist(), [0, 2, 3])
        self.assertTrue(np.array_equal(selected, repeated))
        self.assertTrue(np.array_equal(weights, repeated_weights))
        self.assertEqual(pursuit.tolist(), [0, 3, 5])
        identity = am.RECIPE_IDENTITIES["max_shift_omp_lstsq"]
        self.assertEqual(identity["upstream_revision"], am.AUTHOR_REVISION)
        self.assertIn("clamp", identity["mass_solver"])

    def test_pinned_omp_degenerate_ties_and_fit_diagnostics(self):
        features = np.ones((5, 4), dtype=np.float64)
        selected, weights = am.upstream_omp(features, np.full(5, 4.), 3)
        self.assertEqual(selected.tolist(), [0, 1, 2])
        self.assertTrue(np.isfinite(weights).all())
        _, meta, arrays = self.capture(values=[-4, -2, 3, 7])
        report = am.fit_head(meta, arrays, 0, 2, "max_shift_omp_lstsq")
        self.assertTrue(report["accepted"], report)
        self.assertEqual(report["mass_conditioning"]["rank"], 1)
        self.assertEqual(report["solver_identity"]["selection"], "upstream_simple_omp_single_key_raw_correlation")
        self.assertIn("squared_l2_objective", report["fit_diagnostics"]["mass"])
        conversion = report["fit_diagnostics"]["storage_conversion"]
        self.assertEqual((conversion["bias_dtype"], conversion["value_dtype"]), ("float32", "float16"))
        self.assertGreaterEqual(conversion["mass_weight_relative_l2"], 0)

    def test_cpqr_selection_is_training_only_deterministic_and_diversity_aware(self):
        features = np.asarray([[1., 1., 0.], [0., 0., .8], [0., 0., .2]])
        selected = am.cpqr_selection(features, 2)
        self.assertEqual(selected.tolist(), [0, 2])
        self.assertTrue(np.array_equal(selected, am.cpqr_selection(features, 2)))
        identity = am.RECIPE_IDENTITIES["relative_cd_ridge_cpqr"]
        self.assertEqual(identity["selection"], "training_probability_column_pivoted_qr_low_index_ties")
        self.assertIn("oracle", identity["precision"])

        _, first_meta, first_arrays = self.capture(
            keys=[-1, -1, -1, 1], values=[-1, -2, -3, 4], train=[-.5, .5, 1], held=[-1.5, 0, 2])
        _, second_meta, second_arrays = self.capture(
            "cpqr-new-held", keys=[-1, -1, -1, 1], values=[-1, -2, -3, 4],
            train=[-.5, .5, 1], held=[-3, 3, 4])
        first = am.fit_head(first_meta, first_arrays, 0, 2, "relative_cd_ridge_cpqr")
        second = am.fit_head(second_meta, second_arrays, 0, 2, "relative_cd_ridge_cpqr")
        self.assertEqual(first["selected_source_rows"], [0, 3])
        self.assertEqual(first["selected_source_rows"], second["selected_source_rows"])
        self.assertEqual(first["training"], second["training"])

    def test_all_budgets_and_exact_rank_deficient_storage(self):
        _, meta, arrays = self.capture(values=[-4, -2, 3, 7])
        for recipe in am.RECIPES:
            for retained in (1, 2, 3, 4):
                if recipe == "relative_minimax_ridge_peak" and retained in (2, 3):
                    # This fixture has only one unique positive-support key
                    # column. The bounded selector must reject padding the
                    # requested budget with an exact duplicate.
                    with self.assertRaisesRegex(am.NumericalFailure, "unique columns exhausted"):
                        am.fit_head(meta, arrays, 0, retained, recipe)
                    continue
                report = am.fit_head(meta, arrays, 0, retained, recipe)
                self.assertTrue(report["accepted"], report)
                self.assertEqual(report["exact"], retained == 4)
                self.assertEqual(len(report["selected_source_rows"]), retained)
                if retained == 4:
                    self.assertEqual(report["f16_value_relative_rmse"], 0)
                    self.assertEqual(report["retained_kv_bias_bytes"], report["source_kv_bytes"])
                else:
                    self.assertEqual(report["value_conditioning"]["rank"], 1)

    def test_coverage_diagnostic_exposes_rare_training_keys_without_using_held_for_selection(self):
        probabilities = np.asarray([[.99, .01], [.98, .02], [.01, .99]])
        report = am.query_coverage(probabilities, np.asarray([0]), 2)
        self.assertTrue(report["diagnostic_only"])
        self.assertEqual(report["omitted_held_dominant_training_rare_keys"], [1])
        self.assertEqual(report["omitted_mass_training_max"], .02)
        self.assertEqual(report["omitted_mass_validation_max"], .99)
        exact = am.query_coverage(probabilities, np.asarray([0, 1]), 2)
        self.assertEqual(exact["omitted_mass_validation_max"], 0)

    def test_disjoint_query_distribution_rejects(self):
        _, meta, arrays = self.capture(keys=[-5, 5], values=[-1, 1], train=[-1, -2], held=[1, 2])
        for recipe in am.RECIPES:
            report = am.fit_head(meta, arrays, 0, 1, recipe)
            self.assertFalse(report["accepted"])
            self.assertGreater(report["validation"]["mass_max"], .9)

    def test_per_consumer_gate_catches_error_hidden_by_aggregate(self):
        _, meta, arrays = self.capture(keys=[-1, -1, -1, 1], values=[0, 0, 1, -1], train=[-.5, .5, 1, -.5], held=[-1, 0])
        meta["capture"]["consumers"].append({"layer": 9, "query_heads": 1})
        meta["capture"]["strata"] = [
            {"consumer_layer": layer, "query_head": 0, "kv_head": 0, "training_start": index * 2,
             "training_count": 2, "validation_start": index, "validation_count": 1}
            for index, layer in enumerate((4, 9))]
        am.shape_contract(meta)
        report = am.fit_head(meta, arrays, 0, 2, "relative_qr")
        self.assertTrue(am.passes(report["validation"]))
        self.assertFalse(report["accepted"])
        self.assertGreater(report["consumers"][1]["validation"]["output_relative_rmse"], .1)

    def test_integrity_and_reference_mismatch_fail_closed(self):
        path, meta, arrays = self.capture()
        arrays["exact_outputs"][0, 0, 0] += 1
        with self.assertRaisesRegex(ValueError, "reference mismatch"):
            am.fit_head(meta, arrays, 0, 2, am.RECIPES[0])
        tensor = path.parent / "values.bin"
        raw = bytearray(tensor.read_bytes())
        raw[0] ^= 1
        tensor.write_bytes(raw)
        with self.assertRaisesRegex(ValueError, "integrity failed"):
            am.load_capture(path)

    def test_underflow_rejects_candidate_without_losing_other_results(self):
        path, _, _ = self.capture(keys=[-65504, 65504], values=[-1, 1], train=[-1, -2], held=[1, 2])
        receipt = am.run_captures([path], (1, 2), (am.RECIPES[0],))
        self.assertFalse(receipt["results"][0]["accepted"])
        self.assertIn("zero or nonfinite", receipt["results"][0]["reason"])
        self.assertTrue(receipt["results"][1]["accepted"])

    def test_campaign_limits_fail_before_unbounded_fit_expansion(self):
        path, _, _ = self.capture(values=[1, 1, 1, 1])
        limits = am.campaign_limits(max_fits=1, max_seconds=10, max_working_set_bytes=256 * 1024 * 1024)
        with self.assertRaisesRegex(ValueError, "fit limit"):
            am.run_captures([path], (1, 2), ("max_shift_omp_lstsq",), limits=limits)
        with self.assertRaisesRegex(ValueError, "time limit"):
            am.campaign_limits(max_seconds=0)
        receipt = am.run_captures([path], (1,), ("max_shift_omp_lstsq",), limits=limits)
        self.assertEqual(receipt["campaign_usage"]["fits_started"], 1)
        self.assertEqual(receipt["numerics"]["recipe_identities"]["max_shift_omp_lstsq"],
                         am.RECIPE_IDENTITIES["max_shift_omp_lstsq"])
        self.assertLessEqual(receipt["campaign_usage"]["peak_estimated_working_set_bytes"],
                             receipt["campaign_limits"]["max_estimated_working_set_bytes"])

    def test_consumer_coverage_gqa_and_query_leakage_are_rejected(self):
        _, meta, arrays = self.capture()
        arrays["positions"][0, -1] = arrays["positions"][0, 0]
        with self.assertRaisesRegex(ValueError, "overlap"):
            am.validate_queries(meta, arrays)
        for mutate in (lambda m: m["capture"]["consumers"].append({"layer": 9, "query_heads": 1}),
                       lambda m: m["capture"]["strata"][0].update(kv_head=1),
                       lambda m: m["capture"]["strata"][0].update(training_count=1)):
            changed = copy.deepcopy(meta)
            mutate(changed)
            with self.assertRaises(ValueError):
                am.shape_contract(changed)

    def test_replay_origin_cannot_include_prompt_queries(self):
        _, meta, arrays = self.capture()
        arrays["positions"][0, 0] = 99
        with self.assertRaisesRegex(ValueError, "outside isolated replay"):
            am.validate_queries(meta, arrays)

    def test_tail_only_queries_cannot_claim_full_replay_sampling(self):
        _, meta, arrays = self.capture()
        arrays["positions"][0, :3] = am.replay_positions(110, 10, 3, False)
        with self.assertRaisesRegex(ValueError, "full replay partition"):
            am.validate_queries(meta, arrays)
        del meta["capture"]["query_sampling"]
        with self.assertRaisesRegex(ValueError, "whole-replay sampling"):
            am.shape_contract(meta)

    def test_manifest_memory_limits_and_nonfinite_inputs(self):
        path, meta, _ = self.capture()
        meta["capture"].update(heads=64, dim=512, training_rows=3072, validation_rows=1024)
        meta["model"]["global_owner_shapes"] = [{"layer": 4, "heads": 64, "dim": 512}]
        with self.assertRaisesRegex(ValueError, "memory envelope"):
            am.shape_contract(meta)
        np.array([np.nan], dtype="<f2").tofile(self.root / "nan.bin")
        with self.assertRaisesRegex(ValueError, "nonfinite"):
            am.raw_array(self.root / "nan.bin", "<f2", (1,))
        with self.assertRaises(ValueError):
            am.raw_array(path.parent / "keys.bin", "<f2", (999,))

    def test_freeze_selects_smallest_passing_budget_and_rejects_exact_only(self):
        path, _, _ = self.capture(values=[1, 1, 1, 1])
        receipt = am.run_captures([path], (1, 2, 3, 4), (am.RECIPES[0],))
        profile = am.freeze(receipt, "f" * 64)
        self.assertFalse(profile["serving_qualified"])
        self.assertEqual(next(iter(profile["profiles"].values()))["retained_rows"], 1)
        exact = am.run_captures([path], (4,), (am.RECIPES[0],))
        self.assertTrue(exact["all_passed"])
        self.assertFalse(exact["all_nonidentity"])
        with self.assertRaisesRegex(ValueError, "no nonidentity"):
            am.freeze(exact, "f" * 64)

    def test_runtime_candidate_is_one_exact_frozen_artifact_projection(self):
        _, meta, _ = self.capture("runtime", schema=2, values=[1, 1, 1, 1])
        key = f"{am.model_key(meta)}:4:0"
        contract = am.capture_contract(meta)
        contract.update(source_rows=512, head_dim=1)
        profiles = {key: {"retained_rows": 256, "recipe": am.RUNTIME_PROFILE_RECIPE,
                          "calibration_median_fit_seconds": 1.0, **contract}}
        candidate = am.derive_runtime_candidate({"capture": meta}, profiles, 16384, 32768, True, True)
        self.assertEqual(candidate["owners"], [{"layer": 4, "retained": [256]}])
        self.assertEqual((candidate["max_prompt_tokens"], candidate["max_output_tokens"]), (4088, 8))
        self.assertEqual(candidate["allocation_page_bytes"], 16384)
        self.assertEqual(candidate["minimum_saved_bytes"], 32768)
        self.assertEqual(candidate["implementation"]["recipe"], am.RUNTIME_PROFILE_RECIPE)
        self.assertEqual(candidate["implementation"]["solver"], "bounded_coordinate_descent")
        self.assertTrue(candidate["implementation"]["split_decode"])
        self.assertTrue(candidate["implementation"]["flash_prefill"])
        self.assertEqual(candidate["schema_version"], 2)
        self.assertEqual(candidate["query_protocols"], [am.runtime_protocol_authority(meta)])
        self.assertRegex(candidate["profile_semantic_sha256"], r"^[0-9a-f]{64}$")
        changed = copy.deepcopy(profiles)
        changed[key]["recipe"] = "relative_qr"
        with self.assertRaisesRegex(ValueError, "supported RMS"):
            am.derive_runtime_candidate({"capture": meta}, changed, 16384, 32768, True, True)
        with self.assertRaisesRegex(ValueError, "page granular"):
            am.derive_runtime_candidate({"capture": meta}, profiles, 16384, 4096, True, True)
        with self.assertRaisesRegex(ValueError, "optimized Metal"):
            am.derive_runtime_candidate({"capture": meta}, profiles, 16384, 16384, False, False)

    def test_runtime_candidate_rejects_identity_protocol_and_owner_head_drift(self):
        _, meta, _ = self.capture("runtime-drift", schema=2, values=[1, 1, 1, 1])
        key = f"{am.model_key(meta)}:4:0"
        profiles = {key: {"retained_rows": 256, "recipe": am.RUNTIME_PROFILE_RECIPE,
                          "calibration_median_fit_seconds": 1.0,
                          **dict(am.capture_contract(meta), source_rows=512, head_dim=1)}}
        other = copy.deepcopy(meta)
        other["model"]["implementation_sha256"] = "7" * 64
        with self.assertRaisesRegex(ValueError, "one semantic identity"):
            am.derive_runtime_candidate({"first": meta, "other": other}, profiles, 16384, 16384, True, True)
        other = copy.deepcopy(meta)
        other["capture"]["query_sources"][2]["generation_contract_sha256"] = "7" * 64
        with self.assertRaisesRegex(ValueError, "owner query protocol drift"):
            am.derive_runtime_candidate({"first": meta, "other": other}, profiles, 16384, 16384, True, True)
        with self.assertRaisesRegex(ValueError, "empty runtime candidate"):
            am.derive_runtime_candidate({"capture": meta}, {}, 16384, 16384, True, True)

    def test_runtime_candidate_binds_distinct_protocol_per_global_owner(self):
        _, base, _ = self.capture("runtime-three-owner", schema=2, values=[1, 1, 1, 1])
        shapes = [{"layer": 4, "heads": 1, "dim": 1},
                  {"layer": 9, "heads": 2, "dim": 1},
                  {"layer": 14, "heads": 1, "dim": 1}]
        captures, profiles = {}, {}
        for ordinal, shape in enumerate(shapes):
            meta = copy.deepcopy(base)
            meta["model"]["global_owner_shapes"] = shapes
            meta["capture"]["owner_layer"] = shape["layer"]
            meta["capture"]["heads"] = shape["heads"]
            meta["capture"]["dim"] = shape["dim"]
            meta["capture"]["consumers"] = [{"layer": shape["layer"], "query_heads": shape["heads"]}]
            meta["capture"]["strata"] = [
                {"consumer_layer": shape["layer"], "query_head": head, "kv_head": head}
                for head in range(shape["heads"])
            ]
            captures[str(shape["layer"])] = meta
            for head in range(shape["heads"]):
                key = f"{am.model_key(meta)}:{shape['layer']}:{head}"
                profiles[key] = {"retained_rows": 256, "recipe": am.RUNTIME_PROFILE_RECIPE,
                                 "source_rows": 512, "head_dim": 1}
        candidate = am.derive_runtime_candidate(captures, profiles, 16384, 16384, True, False)
        self.assertEqual(candidate["query_protocols"],
                         [am.runtime_protocol_authority(captures[str(layer)]) for layer in (4, 9, 14)])
        self.assertEqual(candidate["owners"], [
            {"layer": 4, "retained": [256]},
            {"layer": 9, "retained": [256, 256]},
            {"layer": 14, "retained": [256]},
        ])

    def test_runtime_protocol_v2_cross_language_golden_and_adversarial_drift(self):
        authority = {
            "schema_version": 2,
            "layer": 0,
            "query_sampling": "source_uniform_partition_v3",
            "accounting": "serial_physical_branch_upper_bound_v2",
            "max_training_rows": 3072,
            "max_validation_rows": 1024,
            "sources": [{"kind": kind, "partition": partition, "physical_branch": branch}
                        for _, kind, partition, branch in am.RUNTIME_PROTOCOL_SOURCES],
            "owner": {"kv_heads": 2, "query_heads": 8, "head_dim": 64},
            "strata": ([{"consumer_layer": 0, "query_head": q, "kv_head": q // 4} for q in range(8)] +
                       [{"consumer_layer": 1, "query_head": q, "kv_head": q // 2} for q in range(4)]),
            "fit": {"template_sha256": "11" * 32, "generation_contract_sha256": "22" * 32,
                    "response_tokens": 128, "generation_mode": "greedy_fixed_length_ignore_eos"},
            "gate": {"template_sha256": "33" * 32, "generation_contract_sha256": "44" * 32,
                     "response_tokens": 64, "generation_mode": "greedy_fixed_length_ignore_eos"},
        }
        self.assertEqual(am.runtime_protocol_identity(authority),
                         "b32e3916dca3e579d9aef264c6b1cecad71d42b87731bf1fcd94c75acffb3316")
        for mutate, rejected in (
                (lambda row: row["fit"].update(response_tokens=129), False),
                (lambda row: row["gate"].update(generation_contract_sha256="45" * 32), False),
                (lambda row: row["strata"].__setitem__(0, {"consumer_layer": 0, "query_head": 0, "kv_head": 1}), True),
                (lambda row: row["owner"].update(query_heads=10), True)):
            changed = copy.deepcopy(authority)
            mutate(changed)
            if rejected:
                with self.assertRaisesRegex(ValueError, "GQA"):
                    am.runtime_protocol_identity(changed)
            else:
                self.assertNotEqual(am.runtime_protocol_identity(changed), am.runtime_protocol_identity(authority))

    def test_runtime_protocol_authority_rejects_missing_reordered_and_unpaired_sources(self):
        _, meta, _ = self.capture("runtime-authority", schema=2, values=[1, 1, 1, 1])
        authority = am.runtime_protocol_authority(meta)
        self.assertEqual(authority["fit"]["response_tokens"], 20)
        self.assertEqual(authority["gate"]["response_tokens"], 20)
        cases = []
        changed = copy.deepcopy(meta)
        changed["capture"]["query_sources"].pop(0)
        cases.append((changed, "exactly five"))
        changed = copy.deepcopy(meta)
        changed["capture"]["query_sources"][0], changed["capture"]["query_sources"][1] = (
            changed["capture"]["query_sources"][1], changed["capture"]["query_sources"][0])
        cases.append((changed, "source order"))
        changed = copy.deepcopy(meta)
        changed["capture"]["query_sources"][2]["physical_branch_id"] = "different-fit-branch"
        cases.append((changed, "branch grouping"))
        changed = copy.deepcopy(meta)
        changed["capture"]["query_sources"][2]["template_sha256"] = "f" * 64
        cases.append((changed, "template drift"))
        changed = copy.deepcopy(meta)
        changed["capture"]["query_sources"][2]["logical_start"] += 1
        cases.append((changed, "adjacency"))
        changed = copy.deepcopy(meta)
        changed["capture"]["query_sources"][2]["branch_logical_count"] += 1
        cases.append((changed, "branch bounds"))
        for invalid, error in cases:
            with self.subTest(error=error), self.assertRaisesRegex(ValueError, error):
                am.runtime_protocol_authority(invalid)

    def test_no_tuning_on_evaluation_or_relabeling_calibration_document(self):
        path, _, _ = self.capture(values=[1, 1, 1, 1])
        profile = am.freeze(am.run_captures([path], (1,), (am.RECIPES[0],)), "f" * 64)
        evaluation, _, _ = self.capture("evaluation", "evaluation", values=[1, 1, 1, 1], train=[-.3, .4, .8], held=[-.9, 1.2])
        report = am.run_captures([evaluation], (), (), profile)
        self.assertTrue(report["all_passed"] and report["all_nonidentity"])
        with self.assertRaisesRegex(ValueError, "wrong corpus split"):
            am.run_captures([evaluation], (2,), (am.RECIPES[0],))
        data = am.read_json(evaluation)
        data["document"]["sha256"] = profile["calibration_document_hashes"][0]
        evaluation.write_text(json.dumps(data))
        with self.assertRaisesRegex(ValueError, "overlaps calibration"):
            am.run_captures([evaluation], (), (), profile)

    def test_v2_query_sources_keep_teacher_evaluation_out_of_adaptive_retry(self):
        keys = [-5, -5, 5, 5]
        calibration, _, _ = self.capture("v2-calibration", schema=2, keys=keys, values=[1, 1, 1, 1], train=[0, 0, 0], held=[0, 0])
        profile = am.freeze(am.run_captures([calibration], (1, 2, 4), ("relative_qr",)), "f" * 64, adaptive=True)
        evaluation, _, _ = self.capture("v2-evaluation", "evaluation", schema=2,
                                        keys=keys, values=[-1, -1, 1, 1], train=[0, 0, 0], held=[0, 0], evaluation=[2, 3])
        report = am.run_captures([evaluation], (), (), profile)
        self.assertEqual(len(report["results"]), 1, report)
        result = report["results"][0]
        self.assertTrue(result["runtime_accepted"])
        self.assertFalse(result["independent_evaluation_passed"])
        self.assertFalse(result["accepted"])
        self.assertFalse(report["all_passed"])

    def test_v2_query_source_provenance_and_uniform_ranges_fail_closed(self):
        path, meta, arrays = self.capture("v2-contract", schema=2)
        self.assertEqual(meta["schema_version"], 2)
        self.assertTrue(am.fit_head(meta, arrays, 0, 2, am.RECIPES[0])["runtime_accepted"])
        changed = copy.deepcopy(meta)
        changed["capture"]["query_sources"][3]["selection_visible"] = True
        with self.assertRaisesRegex(ValueError, "visibility"):
            am.shape_contract(changed)
        changed = copy.deepcopy(meta)
        changed["capture"]["query_sources"][0]["capture_route"] = "native_prefill"
        changed["capture"]["query_protocol_sha256"] = am.query_protocol_identity(changed["capture"])
        with self.assertRaisesRegex(ValueError, "capture route"):
            am.shape_contract(changed)
        changed = copy.deepcopy(meta)
        changed["capture"]["query_sources"][3]["tokens_sha256"] = changed["capture"]["query_sources"][0]["tokens_sha256"]
        changed["capture"]["query_protocol_sha256"] = am.query_protocol_identity(changed["capture"])
        with self.assertRaisesRegex(ValueError, "overlap"):
            am.shape_contract(changed)
        changed = copy.deepcopy(meta)
        changed["capture"]["strata"][0]["query_source_ranges"]["training"].pop()
        changed["capture"]["query_protocol_sha256"] = am.query_protocol_identity(changed["capture"])
        with self.assertRaisesRegex(ValueError, "coverage|query pool contract"):
            am.shape_contract(changed)
        changed = copy.deepcopy(meta)
        changed["capture"]["query_protocol_sha256"] = "0" * 64
        with self.assertRaisesRegex(ValueError, "digest mismatch"):
            am.shape_contract(changed)
        changed = copy.deepcopy(meta)
        changed["capture"]["query_sources"][2]["generation_mode"] = "greedy_until_eos"
        changed["capture"]["query_protocol_sha256"] = am.query_protocol_identity(changed["capture"])
        with self.assertRaisesRegex(ValueError, "generation mode"):
            am.shape_contract(changed)
        changed = copy.deepcopy(meta)
        changed["capture"]["query_sources"][2]["loaded_identity"]["artifact_sha256"] = "0" * 64
        changed["capture"]["query_protocol_sha256"] = am.query_protocol_identity(changed["capture"])
        with self.assertRaisesRegex(ValueError, "disagree on loaded identity"):
            am.shape_contract(changed)
        changed = copy.deepcopy(meta)
        changed["capture"]["query_campaign"]["branch_runs"][0]["kv_readback_bytes"] = 1
        with self.assertRaisesRegex(ValueError, "resource observation"):
            am.shape_contract(changed)
        changed = copy.deepcopy(meta)
        changed["capture"]["query_campaign"]["projected_serving_peak_bytes"] -= 1
        with self.assertRaisesRegex(ValueError, "aggregate mismatch"):
            am.shape_contract(changed)
        changed = copy.deepcopy(meta)
        changed["capture"]["query_campaign"]["branch_runs"].pop()
        with self.assertRaisesRegex(ValueError, "source coverage"):
            am.shape_contract(changed)
        arrays["positions"][0, 0] += 1
        with self.assertRaisesRegex(ValueError, "uniformly cover"):
            am.validate_queries(meta, arrays)

    def test_query_protocol_is_document_invariant_but_binds_compiled_sources(self):
        _, first, _ = self.capture("protocol-first", schema=2, train=[-1, 0, 1], held=[-2, 2])
        _, second, _ = self.capture("protocol-second", schema=2, train=[-1, -.5, .5, 1], held=[-2, 0, 2])
        sources = second["capture"]["query_sources"]
        sources[0]["tokens_sha256"] = "0" * 64
        sources[0]["logical_count"] = 37
        for source in sources:
            if source["kind"] == "fixed_study_generated_response":
                source["tokens_sha256"] = hashlib.sha256(source["id"].encode() + b"-other-document").hexdigest()
                source["logical_count"] += 11
        stratum = second["capture"]["strata"][0]
        stratum["training_count"] += 7
        stratum["validation_count"] += 3
        for ranges in stratum["query_source_ranges"].values():
            for index, entry in enumerate(ranges):
                entry["start"] += index * 13
                entry["count"] += index + 1
        self.assertEqual(am.query_protocol_identity(first["capture"]),
                         am.query_protocol_identity(second["capture"]))
        self.assertEqual(am.query_protocol_description(first["capture"]),
                         am.query_protocol_description(second["capture"]))
        # Profile compatibility is stable across request-dependent sampler
        # caps and offsets, not merely across changed token digests.
        first_contract = am.capture_contract(first)
        second_contract = am.capture_contract(second)
        self.assertEqual(first_contract, second_contract)
        for mutate in (
            lambda c: c["query_sources"][1].update(template_sha256="0" * 64),
            lambda c: c["query_sources"][2].update(generation_contract_sha256="0" * 64),
            lambda c: c["query_sources"].reverse(),
            lambda c: c["query_sources"][0]["loaded_identity"].update(implementation_sha256="0" * 64),
        ):
            changed = copy.deepcopy(first["capture"])
            mutate(changed)
            self.assertNotEqual(am.query_protocol_identity(first["capture"]),
                                am.query_protocol_identity(changed))

    def test_fit_memory_estimate_charges_independent_evaluation_rows(self):
        _, meta, _ = self.capture("memory-eval", "evaluation", schema=2,
                                  train=[-1, 0, 1], held=[-2, 2], evaluation=[-.5, .5])
        without = copy.deepcopy(meta)
        without["capture"]["evaluation_rows"] = 0
        self.assertGreater(am.estimated_fit_bytes(meta, 2), am.estimated_fit_bytes(without, 2))

    def test_runtime_query_pool_is_predeclared_per_source_and_fails_closed(self):
        _, meta, _ = self.capture("runtime-pool", schema=2)
        pool = meta["capture"]["runtime_query_pool"]
        self.assertEqual(pool["description"]["max_training_rows"], 3072)
        self.assertEqual(pool["description"]["max_validation_rows"], 1024)
        self.assertTrue(pool["runtime_supported"])
        changed = copy.deepcopy(meta["capture"])
        quota = changed["runtime_query_pool"]["source_counts"]["fit-study"]["max_rows_per_stratum"]
        entry = next(x for x in changed["strata"][0]["query_source_ranges"]["training"]
                     if x["source_id"] == "fit-study")
        entry["count"] = quota + 1
        observed = am.runtime_query_pool_observation(changed)
        self.assertFalse(observed["runtime_supported"])
        changed["runtime_query_pool"] = observed
        with self.assertRaisesRegex(ValueError, "exceed runtime shared query pool"):
            am.validate_runtime_query_pool(changed)

    def test_profile_binding_rejects_changed_source(self):
        path, _, _ = self.capture(values=[1, 1, 1, 1])
        profile = am.freeze(am.run_captures([path], (1,), (am.RECIPES[0],)), "f" * 64)
        evaluation, _, _ = self.capture("evaluation", "evaluation", values=[1, 1, 1, 1], train=[-.3, .4, .8], held=[-.9, 1.2])
        data = am.read_json(evaluation)
        data["source"]["files"]["capture.zig"] = "0" * 64
        evaluation.write_text(json.dumps(data))
        with self.assertRaisesRegex(ValueError, "contract mismatch"):
            am.run_captures([evaluation], (), (), profile)

    def test_diagnostic_capture_cannot_freeze_or_claim_replay(self):
        path, meta, _ = self.capture(purpose="diagnostic", values=[1, 1, 1, 1])
        receipt = am.run_captures([path], (2,), (am.RECIPES[0],))
        with self.assertRaisesRegex(ValueError, "only calibration"):
            am.freeze(receipt, "f" * 64)
        meta["purpose"] = "calibration"
        meta["capture"]["query_origin"] = "request_prefill"
        with self.assertRaisesRegex(ValueError, "isolated replay"):
            am.shape_contract(meta)

    def test_adaptive_profile_keeps_sensitive_owner_exact_and_requires_coverage(self):
        first, _, _ = self.capture("owner4", values=[1, 1, 1, 1])
        second, _, _ = self.capture("owner9", keys=[-5, -5, 5, 5], values=[-1, -1, 1, 1], train=[-1, -2], held=[1, 2])
        for index, path in enumerate((first, second)):
            meta = am.read_json(path)
            meta["model"]["global_owner_shapes"] = [{"layer": layer, "heads": 1, "dim": 1} for layer in (4, 9)]
            meta["document"] = {"id": "shared-document", "sha256": "7" * 64, "bucket": "document_qa"}
            if index:
                meta["capture"]["owner_layer"] = 9
                meta["capture"]["consumers"][0]["layer"] = 9
                meta["capture"]["strata"][0]["consumer_layer"] = 9
            path.write_text(json.dumps(meta))
        receipt = am.run_captures([first, second], (1, 2, 4), (am.RECIPES[0],))
        profile = am.freeze(receipt, "f" * 64)
        self.assertEqual(sorted(x["retained_rows"] for x in profile["profiles"].values()), [1, 4])
        incomplete = am.run_captures([first], (1,), (am.RECIPES[0],))
        with self.assertRaisesRegex(ValueError, "incomplete global owner"):
            am.freeze(incomplete, "f" * 64)

    def test_adaptive_retry_is_frozen_and_exact_only_document_fails_memory_gate(self):
        options = {"keys": [-5, -5, 5, 5], "values": [1, 1, 1, 1]}
        path, _, _ = self.capture(train=[0, 0, 0], held=[0, 0], **options)
        profile = am.freeze(am.run_captures([path], (1, 2, 4), (am.RECIPES[0],)), "f" * 64, adaptive=True)
        chosen = next(iter(profile["profiles"].values()))
        self.assertEqual(chosen["retention_schedule"], [1, 2, 4])
        evaluation, _, _ = self.capture("difficult", "evaluation", train=[-1, -2, -3], held=[1, 2], **options)
        report = am.run_captures([evaluation], (), (), profile)
        self.assertEqual([x["retained_rows"] for x in report["results"]], [1, 2, 4])
        self.assertEqual([x["selected_attempt"] for x in report["results"]], [False, False, True])
        self.assertTrue(report["all_passed"])
        self.assertFalse(report["all_candidates_passed"])
        self.assertFalse(report["all_models_reduce_kv_bytes"])
        easy, _, _ = self.capture("easy", "evaluation", train=[-.001, 0, .001], held=[-.002, .002], **options)
        combined = am.run_captures([easy, evaluation], (), (), profile)
        self.assertTrue(combined["all_passed"])
        self.assertFalse(combined["all_models_reduce_kv_bytes"])
        self.assertEqual(len(combined["raw_bytes_by_model_document"]), 2)
        self.assertEqual(chosen["retention_schedule"], [1, 2, 4])

    def test_adaptive_profile_requires_calibrated_exact_reference(self):
        path, _, _ = self.capture(values=[1, 1, 1, 1])
        receipt = am.run_captures([path], (1, 2), (am.RECIPES[0],))
        with self.assertRaisesRegex(ValueError, "exact calibration reference"):
            am.freeze(receipt, "f" * 64, adaptive=True)


if __name__ == "__main__":
    unittest.main()
