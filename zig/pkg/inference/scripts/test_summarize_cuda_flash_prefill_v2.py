#!/usr/bin/env python3

import importlib.util
import unittest
from pathlib import Path


SCRIPT = Path(__file__).with_name("summarize_cuda_flash_prefill_v2.py")
SPEC = importlib.util.spec_from_file_location("flash_v2_summary", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def fixture(candidate_scale: float = 0.5):
    results = []
    for head_dim in (256, 512):
        for q_len, prefixes in ((512, (0, 512, 1024, 1536)), (3, (2048,))):
            for prefix in prefixes:
                baseline = float(prefix + q_len + head_dim)
                results.append(
                    {
                        "head_dim": head_dim,
                        "q_len": q_len,
                        "prefix": prefix,
                        "layout": "identity-null",
                        "pattern": "random",
                        "pass": True,
                        "candidate_vs_canonical": {"bitwise_mismatches": 0},
                        "timing": {
                            "baseline_mean_us": baseline,
                            "candidate_mean_us": baseline * candidate_scale,
                        },
                    }
                )
    return {
        "schema": MODULE.INPUT_SCHEMA,
        "runtime_integrated": False,
        "pass": True,
        "gates": {"require_bitwise_candidate": True},
        "artifacts": {"candidate_sha256": "candidate", "baseline_sha256": "baseline"},
        "candidate_query_tile": 32,
        "candidate_key_tile": 32,
        "candidate_head_group": 1,
        "results": results,
    }


class SummaryTests(unittest.TestCase):
    def test_projects_exact_locked_fixture_and_layer_mix(self):
        summary = MODULE.summarize(fixture(), 1.2)
        self.assertEqual(MODULE.OUTPUT_SCHEMA, summary["schema"])
        self.assertEqual([512, 512, 512, 512, 3], summary["locked_fixture"]["chunks"])
        self.assertAlmostEqual(2.0, summary["projected_35_layer_attention"]["speedup"])
        self.assertTrue(summary["projected_35_layer_attention"]["material_pass"])
        self.assertEqual([28, 7], [item["layers"] for item in summary["per_head_dimension"]])

    def test_rejects_non_bitwise_or_incomplete_evidence(self):
        payload = fixture()
        payload["results"][0]["candidate_vs_canonical"]["bitwise_mismatches"] = 1
        with self.assertRaises(MODULE.EvidenceError):
            MODULE.summarize(payload, 1.2)
        payload = fixture()
        payload["results"].pop()
        with self.assertRaises(MODULE.EvidenceError):
            MODULE.summarize(payload, 1.2)

    def test_material_threshold_is_explicit(self):
        summary = MODULE.summarize(fixture(candidate_scale=0.9), 1.2)
        self.assertFalse(summary["projected_35_layer_attention"]["material_pass"])


if __name__ == "__main__":
    unittest.main()
