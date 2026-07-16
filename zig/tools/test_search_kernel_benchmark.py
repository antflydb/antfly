#!/usr/bin/env python3

import importlib.util
import sys
import unittest
from pathlib import Path


SCRIPT = Path(__file__).with_name("run_search_kernel_benchmark.py")
SPEC = importlib.util.spec_from_file_location("kernel_benchmark", SCRIPT)
assert SPEC and SPEC.loader
benchmark = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = benchmark
SPEC.loader.exec_module(benchmark)


def manifest(segment_mode: str = "single") -> dict:
    return {
        "query_grammar": "V1",
        "segment_mode": segment_mode,
        "corpus": {"sha256": "abc", "indexed_documents": 3},
        "analysis": {"tokenizer": "unicode_words"},
        "bm25": {"k1": 1.2, "b": 0.75},
    }


def antfly_manifest(segment_mode: str = "production", segment_count: int = 2) -> dict:
    value = manifest(segment_mode)
    value["layout"] = {
        "segments": [{} for _ in range(segment_count)],
        "merge_stats": {
            "in_flight_merges": 0,
            "in_flight_segments": 0,
            "pending_indexes": 0,
            "pending_segments": 0,
            "pending_bytes": 0,
            "pending_mmap_bytes": 0,
            "pending_heap_bytes": 0,
            "quarantined_merges": 0,
            "quarantined_segments": 0,
            "failed_merges": 0,
        },
    }
    return value


class KernelBenchmarkTest(unittest.TestCase):
    def test_parse_ps_cpu_time(self):
        self.assertEqual(benchmark.parse_ps_cpu_time("0:01.25"), 1_250_000_000)
        self.assertEqual(benchmark.parse_ps_cpu_time("2:03:04.50"), 7_384_500_000_000)
        self.assertEqual(benchmark.parse_ps_cpu_time("1-02:03:04.50"), 93_784_500_000_000)

    def test_positive_manifest_metric(self):
        self.assertEqual(benchmark.positive_manifest_metric({"elapsed": 42}, "elapsed"), 42)
        self.assertIsNone(benchmark.positive_manifest_metric({"elapsed": 0}, "elapsed"))
        self.assertIsNone(benchmark.positive_manifest_metric({"elapsed": True}, "elapsed"))
        self.assertIsNone(benchmark.positive_manifest_metric({"elapsed": "42"}, "elapsed"))
        self.assertIsNone(benchmark.positive_manifest_metric({}, "elapsed"))

    def test_compatible_manifest_preflight_accepts_equal_contract(self):
        benchmark.assert_compatible_manifests(manifest(), manifest())

    def test_compatible_manifest_preflight_rejects_segment_mismatch(self):
        with self.assertRaisesRegex(RuntimeError, "segment_mode"):
            benchmark.assert_compatible_manifests(manifest(), manifest("production"))

    def test_settled_layout_accepts_debt_free_production_index(self):
        benchmark.assert_settled_antfly_layout(antfly_manifest())

    def test_settled_layout_rejects_pending_merge_work(self):
        value = antfly_manifest()
        value["layout"]["merge_stats"]["pending_indexes"] = 1
        with self.assertRaisesRegex(RuntimeError, "merge debt"):
            benchmark.assert_settled_antfly_layout(value)

    def test_settled_layout_rejects_single_mode_with_multiple_segments(self):
        with self.assertRaisesRegex(RuntimeError, "exactly one segment"):
            benchmark.assert_settled_antfly_layout(antfly_manifest("single", 2))


if __name__ == "__main__":
    unittest.main()
