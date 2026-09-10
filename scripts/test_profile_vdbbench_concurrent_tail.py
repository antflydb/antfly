"""Paired tail-attribution tests; run with VectorDBBench's virtualenv."""

import unittest
from argparse import Namespace

from profile_vdbbench_concurrent_tail import PROFILE_FIELDS, summarize, worker_arguments


class TailSummaryTest(unittest.TestCase):
    def test_independent_processes_start_at_different_queries(self):
        args = Namespace(processes=6, count=1000)
        workers = worker_arguments(args)
        self.assertEqual(
            [a.query_offset for a in workers], [0, 166, 333, 500, 666, 833]
        )
        self.assertFalse(hasattr(args, "query_offset"))

    def test_tail_cohort_preserves_paired_stage_values(self):
        rows = []
        for index in range(20):
            row = dict.fromkeys(PROFILE_FIELDS, 0.0)
            row.update(index=index, http_ms=10.0, recall=1.0, approximate=20, exact=2)
            row["total_ns"] = 9.0
            rows.append(row)
        rows[0]["http_ms"] = 100.0
        rows[0]["total_ns"] = 2.0
        rows[0]["admission_work"] = {"hbc_admission_selected_scan_bytes": 4096}
        rows[0]["physical_io"] = {"hbc_rerank_vector_physical_reads": 7}
        rows[1]["total_ns"] = 9.5
        result = summarize(rows, 2.0, 3)
        self.assertEqual(result["completed"], 20)
        self.assertEqual(result["qps"], 10)
        self.assertEqual(result["recall"], 1)
        tail = result["http_slowest_5_percent"]
        self.assertEqual(tail["count"], 1)
        self.assertEqual(
            tail["physical_io"]["hbc_rerank_vector_physical_reads"],
            {"samples": 1, "mean": 7},
        )
        self.assertEqual(tail["outside_server_timer_ms"]["mean_ms"], 98)
        self.assertEqual(tail["server_stages_ms"]["total_ns"]["mean_ms"], 2)
        self.assertEqual(
            tail["admission_work"]["hbc_admission_selected_scan_bytes"],
            {"samples": 1, "mean": 4096},
        )
        self.assertEqual(
            result["all"]["admission_work"]["hbc_admission_selected_scan_bytes"][
                "samples"
            ],
            1,
        )
        self.assertIsNone(
            result["all"]["admission_work"]["hbc_admission_peak_reserved_bytes"]["mean"]
        )
        self.assertEqual(
            result["server_slowest_5_percent"]["server_stages_ms"]["total_ns"][
                "mean_ms"
            ],
            9.5,
        )


if __name__ == "__main__":
    unittest.main()
