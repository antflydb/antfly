# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0

import copy
import math
import unittest

from metal_checkpoint_cycle_evidence import validate_report


def fixture():
    identity = {key: bytes([index + 1] * 32).hex() for index, key in
                enumerate(("weights", "tokenizer", "template", "layout"))}
    source = {"model": "google/gemma-4-E2B-it-qat-q4_0-gguf", "expected_prompt_tokens": 8128,
              "request": {"encoding": "f16", "cycles": 2, "append_tokens": 512,
                          "answer_tokens": 4, "initial_minimum_savings_ratio": 1.5,
                          "incremental_minimum_savings_ratio": 1.000001, "reference": []}}
    rows = []
    for index in range(2):
        rows.append({"cycle": index + 1, "logical_tokens": 8132 + 516 * index,
                     "owner_id": 2, "source_revision": 1 + 106 * index,
                     "installed_revision": 2 + 106 * index, "query_owner_id": 2,
                     "query_owner_peak_bytes": 500000000 + index,
                     "query_scope_released": True, "query_scope_delta_bytes": 0,
                     "query_uses_source_owner": True, "completed_chunks": 24,
                     "validated_heads": 3, "attention_calls": 140, "donor_appends": 60,
                     "fp64_accelerated_value_solves": 24,
                     "ragged_repeat_calls": 70 * index, "admitted_peak_bytes": 8 * 1024**3,
                     "max_head_mass_error": .03, "max_head_output_rmse": .002,
                     "source_global_bytes": 49938432 if index == 0 else 27066368,
                     "candidate_global_bytes": 23822336 if index == 0 else 24674304,
                     "fp8_heads": 0, "rotated4_heads": 0,
                     "immutable_rows_sha256": [3 + index] * 32,
                     "checkpoint_sha256": [5 + index] * 32, "comparisons": 4,
                     "greedy_matches": 4, "exact_nll": .134,
                     "compacted_nll": .135, "max_logit_relative_l2": .001})
    rows += [{} for _ in range(18)]
    value = {"schema": "antfly.metal-checkpoint-cycles.report.v3", "model": source["model"],
             "model_directory": "/models/e2b", "input_sha256": "a" * 64,
             "encoding": "f16", "load_generation": 1,
             "model_identity": {key: list(bytes.fromhex(raw)) for key, raw in identity.items()},
             "rendered_prompt_sha256": "b" * 64, "prompt_tokens_sha256": "c" * 64,
             "implementation_sha256": "d" * 64,
             "report": {"passed": True, "diagnostic_only": True, "production_ready": False,
                        "frozen_reference": False, "completed": 2, "initial_tokens": 8128,
                        "final_tokens": 8648, "rows": rows,
                        "exact_release": {"released": True, "post_release_live_bytes": 0},
                        "compacted_release": {"released": True, "post_release_live_bytes": 0}}}
    return value, source, {"input_sha256": "a" * 64, "model_directory": "/models/e2b",
                           "loaded_identity": identity}


class Evidence(unittest.TestCase):
    def test_two_cycles_keep_diagnostic_authority(self):
        value, source, binding = fixture()
        result = validate_report(value, source, **binding)
        self.assertEqual(result["completed_cycles"], 2)
        self.assertEqual(result["comparisons"], 8)
        self.assertEqual(result["greedy_matches"], 8)
        self.assertFalse(result["production_ready"])
        self.assertFalse(result["frozen_reference"])

    def test_owner_scope_and_trajectory_tampering(self):
        mutations = {"owner_id": 3, "source_revision": 1, "installed_revision": 110,
                     "logical_tokens": 8647, "query_owner_id": 3,
                     "query_scope_released": False, "query_scope_delta_bytes": None,
                     "query_uses_source_owner": False, "query_owner_peak_bytes": 10,
                     "ragged_repeat_calls": 0, "attention_calls": 0, "donor_appends": 0,
                     "candidate_global_bytes": 27066368, "completed_chunks": 0,
                     "validated_heads": 0, "fp64_accelerated_value_solves": 0, "comparisons": 3, "greedy_matches": 5,
                     "admitted_peak_bytes": 8 * 1024**3 + 1,
                     "immutable_rows_sha256": [0] * 32, "checkpoint_sha256": [5] * 32}
        for key, invalid in mutations.items():
            with self.subTest(key=key):
                value, source, binding = fixture()
                value["report"]["rows"][1][key] = invalid
                with self.assertRaises(ValueError):
                    validate_report(value, source, **binding)

    def test_numerical_poisoning_and_unchanged_limits(self):
        for key in ("exact_nll", "compacted_nll", "max_head_mass_error",
                    "max_head_output_rmse", "max_logit_relative_l2"):
            for invalid in (float("nan"), float("inf"), -1, True):
                with self.subTest(key=key, invalid=invalid):
                    value, source, binding = fixture()
                    value["report"]["rows"][0][key] = invalid
                    with self.assertRaises(ValueError):
                        validate_report(value, source, **binding)
        for key, invalid in (("max_head_mass_error", .050001),
                             ("max_head_output_rmse", .100001),
                             ("compacted_nll", 8 * math.log(1.05) + 1)):
            with self.subTest(key=key):
                value, source, binding = fixture()
                value["report"]["rows"][0][key] = invalid
                with self.assertRaises(ValueError):
                    validate_report(value, source, **binding)

    def test_wrong_model_input_or_authority(self):
        for key, invalid in (("model", "other"), ("model_directory", "/other"),
                             ("input_sha256", "e" * 64), ("implementation_sha256", "0" * 64),
                             ("schema", "antfly.metal-checkpoint-cycles.report.v1"),
                             ("encoding", "fp8"), ("load_generation", 0)):
            with self.subTest(key=key):
                value, source, binding = fixture(); value[key] = invalid
                with self.assertRaises(ValueError): validate_report(value, source, **binding)
        for key, invalid in (("production_ready", True), ("diagnostic_only", False),
                             ("frozen_reference", True), ("completed", 1), ("passed", False)):
            with self.subTest(key=key):
                value, source, binding = fixture(); value["report"][key] = invalid
                with self.assertRaises(ValueError): validate_report(value, source, **binding)
        value, source, binding = fixture()
        value["model_identity"]["weights"][0] ^= 1
        with self.assertRaises(ValueError): validate_report(value, source, **binding)

    def test_requested_codec_must_be_resident_each_cycle(self):
        for encoding, key in (("fp8", "fp8_heads"), ("rotated4", "rotated4_heads")):
            value, source, binding = fixture()
            source["request"]["encoding"] = value["encoding"] = encoding
            with self.assertRaises(ValueError): validate_report(value, source, **binding)
            for row in value["report"]["rows"][:2]: row[key] = 3
            validate_report(value, source, **binding)
            value["report"]["rows"][1][key] = 0
            with self.assertRaises(ValueError): validate_report(value, source, **binding)

    def test_full_release_and_boolean_byte_counts(self):
        for label in ("exact_release", "compacted_release"):
            for invalid in (None, 1, False):
                with self.subTest(label=label, invalid=invalid):
                    value, source, binding = fixture()
                    value["report"][label]["post_release_live_bytes"] = invalid
                    with self.assertRaises(ValueError): validate_report(value, source, **binding)


if __name__ == "__main__":
    unittest.main()
