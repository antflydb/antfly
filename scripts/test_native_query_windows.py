import unittest

from summarize_native_query_windows import snapshots, summarize, windows


class QueryWindowTests(unittest.TestCase):
    def test_only_inner_snapshots_and_monotonic_counters(self):
        result = summarize(
            [{"start": 10, "end": 20}],
            snapshots(
                [
                    "# snapshot_wall_time_s 9",
                    "antfly_dense_checkpoint_completion_rounds_total 100",
                    "# snapshot_wall_time_s 11",
                    "antfly_dense_checkpoint_completion_rounds_total 110",
                    "# snapshot_wall_time_s 19",
                    "antfly_dense_checkpoint_completion_rounds_total 115",
                    "# snapshot_wall_time_s 21",
                    "antfly_dense_checkpoint_completion_rounds_total 190",
                ]
            ),
        )[0]
        self.assertEqual(result["inner_sample_seconds"], 8)
        self.assertEqual(
            result["metrics"]["antfly_dense_checkpoint_completion_rounds_total"][
                "delta"
            ],
            5,
        )

    def test_window_requires_matching_start_and_end(self):
        lines = [
            "2026-09-08 10:00:00,000 Syncing all process and start concurrency search, concurrency=30",
            "2026-09-08 10:00:30,000 End search in concurrency 30: dur=30s, total_count=20000",
        ]
        wave = windows(lines, "UTC")[0]
        self.assertEqual(wave["reported_count"], 20000)
        with self.assertRaises(ValueError):
            windows(lines[:1], "UTC")
        with self.assertRaises(ValueError):
            windows(lines[1:], "UTC")


if __name__ == "__main__":
    unittest.main()
