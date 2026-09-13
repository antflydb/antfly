"""Keep failed arms out of timing and fixed-query routing summaries."""

import json
import tempfile
import unittest
from pathlib import Path

from summarize_dense_recovery_query_ab import recall_checks, summarize


class RecoverySummaryTest(unittest.TestCase):
    def test_recall_budget_is_separate_from_successful_execution(self):
        control = {
            "mode": "control",
            "pair": 1,
            "fixed_queries_count": 1000,
            "fixed_queries_recall": 0.98405,
        }
        candidate = {
            "mode": "subgroup_routing",
            "pair": 1,
            "fixed_queries_count": 1000,
            "fixed_queries_recall": 0.90753,
        }
        check = recall_checks([control, candidate])[0]
        self.assertEqual(check["status"], "fail")
        self.assertAlmostEqual(check["loss_percentage_points"], 7.652)
        candidate["fixed_queries_recall"] = 0.97405
        self.assertEqual(recall_checks([control, candidate])[0]["status"], "pass")
        candidate["fixed_queries_count"] = 100
        self.assertEqual(
            recall_checks([control, candidate])[0]["status"], "not_measured"
        )
        self.assertEqual(recall_checks([candidate])[0]["status"], "not_measured")

    def test_fixed_queries_and_failed_arm_exclusion(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "runs.json").write_text(
                json.dumps(
                    [
                        {"mode": "angular", "pair": 1, "passed": True},
                        {"mode": "angular", "pair": 2, "passed": False},
                    ]
                )
            )
            arm = root / "1-angular"
            arm.mkdir()
            result = {
                "qps": 100,
                "recall": 0.99,
                "all": {
                    "http_ms": {"p95_ms": 20},
                    "server_stages_ms": {
                        "hbc_leaf_score_ns": {"mean_ms": 2, "p95_ms": 3}
                    },
                    "admission_work": {},
                    "physical_io": {},
                },
            }
            for concurrency in (1, 30):
                (arm / f"c{concurrency}.json").write_text(json.dumps(result))
            (arm / "warmup.json").write_text(
                json.dumps(
                    {
                        "count": 1000,
                        "recall": 0.991,
                        "approximate_vectors_mean": 1234,
                        "profile_values": {"hbc_traversal_bound_stops": {"mean": 0.5}},
                    }
                )
            )
            (arm / "mixed.json").write_text(
                json.dumps(
                    {
                        "offered_write_rows_per_second": 2000,
                        "write_rows_per_second": 1800,
                        "query_qps": 300,
                        "catchup_seconds": 2,
                        "write_schedule_delay": {"p95_ms": 100},
                        "write_latency": {"p95_ms": 50},
                        "server_latency": {"p95_ms": 8},
                        "write_scheduled_latency": None,
                    }
                )
            )
            (arm / "memory.jsonl").write_text('{"rss_bytes": 1234}\n')
            summary = summarize(root)
            self.assertTrue(summary["diagnostic_only"])
            self.assertEqual(len(summary["individual_arms"]), 1)
            median = summary["medians"][0]
            self.assertEqual(median["arms"], 1)
            self.assertEqual(median["fixed_queries_count"], 1000)
            self.assertEqual(median["fixed_queries_recall"], 0.991)
            self.assertEqual(
                median["fixed_queries_hbc_traversal_bound_stops_mean"], 0.5
            )
            self.assertEqual(median["c30_hbc_leaf_score_ns_mean_ms"], 2)
            self.assertEqual(median["mixed_offered_write_rows_per_second"], 2000)
            self.assertEqual(median["mixed_write_rows_per_second"], 1800)
            self.assertEqual(median["mixed_write_schedule_delay_p95_ms"], 100)
            self.assertEqual(median["mixed_write_latency_p95_ms"], 50)
            self.assertEqual(median["mixed_server_latency_p95_ms"], 8)
            self.assertEqual(median["restart_query_and_mixed_peak_rss_bytes"], 1234)
            self.assertNotIn("restart_and_query_peak_rss_bytes", median)


if __name__ == "__main__":
    unittest.main()
