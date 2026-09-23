import copy
import json
import unittest

import laya_cuda_performance as performance


class LayaPerformanceTest(unittest.TestCase):
    def setUp(self):
        self.rows = [
            {
                "profile": profile,
                "scope": scope,
                "batch": batch,
                "p50_ms": 10,
                "p95_ms": 12,
                "samples": 100,
                "warmups": 10,
                "warp_attention": int(batch == 8),
                "packed_geglu": 1,
                "chunks": 3,
                "padded_tokens": 820,
            }
            for profile in ("fixed", "mixed")
            for scope in ("pipeline", "prepared")
            for batch in (1, 8)
        ]

    def output(self, rows):
        return (
            "Laya qualification backend=cuda load_ms=1\n"
            "Laya benchmark build mode=ReleaseSafe cpu=x86_64_v3 artifacts=fatbin\n"
            + "\n".join("Laya benchmark " + json.dumps(row) for row in rows)
            + "\n1 selected; 1 passed; 0 skipped\n"
        )

    def test_incomplete_duplicate_and_nonfinite_measurements_fail(self):
        for rows in (self.rows[:-1], self.rows + self.rows[:1]):
            with self.assertRaises(ValueError):
                performance.index_measurements(rows)
        for value in (0, -1, float("inf"), float("nan")):
            rows = copy.deepcopy(self.rows)
            rows[0]["p95_ms"] = value
            with self.assertRaises(ValueError):
                performance.index_measurements(rows)

    def test_gate_compares_pipeline_and_checks_both_percentiles(self):
        rows = copy.deepcopy(self.rows)
        rows[1]["p95_ms"] = 1000  # Batch eight, pipeline.
        checks = performance.compare(self.rows, rows, self.rows)
        self.assertEqual(8, len(checks))
        self.assertEqual(1, sum(not check["passed"] for check in checks))
        rows = copy.deepcopy(self.rows)
        rows[2]["p95_ms"] = 1000  # Diagnostic prepared scope.
        self.assertTrue(
            all(c["passed"] for c in performance.compare(self.rows, rows, self.rows))
        )

    def test_native_requires_execution_counts_and_routes(self):
        performance.parse_native(self.output(self.rows), "candidate", 100, 10)
        with self.assertRaises(ValueError):
            performance.parse_native(self.output(self.rows), "baseline", 100, 10)
        for field, value in (("samples", 99), ("warmups", 9), ("packed_geglu", 0)):
            rows = copy.deepcopy(self.rows)
            rows[0][field] = value
            with self.assertRaises(ValueError):
                performance.parse_native(self.output(rows), "candidate", 100, 10)
        rows = copy.deepcopy(self.rows)
        rows[1]["warp_attention"] = 0
        with self.assertRaises(ValueError):
            performance.parse_native(self.output(rows), "candidate", 100, 10)
        for field, value in (("chunks", 1), ("padded_tokens", 1192)):
            rows = copy.deepcopy(self.rows)
            rows[5][field] = value
            with self.assertRaises(ValueError):
                performance.parse_native(self.output(rows), "candidate", 100, 10)
        with self.assertRaises(ValueError):
            performance.parse_native(
                self.output(self.rows).replace("1 passed", "0 passed"),
                "candidate",
                100,
                10,
            )

    def test_pytorch_requires_matched_precision_versions_and_iterations(self):
        report = {
            "dtype": "float32",
            "tf32": False,
            "torch_version": "2.6.0+cu124",
            "transformers_version": "4.57.6",
            "samples": 100,
            "warmups": 10,
            "measurements": self.rows,
        }
        performance.validate_pytorch(report, 100, 10)
        for field, value in (
            ("dtype", "float16"),
            ("tf32", True),
            ("samples", 99),
            ("torch_version", "2.7.0"),
            ("transformers_version", "4.58.0"),
        ):
            with self.assertRaises(ValueError):
                performance.validate_pytorch({**report, field: value}, 100, 10)

    def test_bucketing_requires_five_percent_improvement(self):
        self.assertFalse(
            all(
                c["passed"] for c in performance.compare_bucketing(self.rows, self.rows)
            )
        )
        faster = copy.deepcopy(self.rows)
        for row in faster:
            row["p50_ms"] *= 0.94
            row["p95_ms"] *= 0.94
        self.assertTrue(
            all(c["passed"] for c in performance.compare_bucketing(self.rows, faster))
        )


if __name__ == "__main__":
    unittest.main()
