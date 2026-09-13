import unittest

from summarize_dense_publication import summarize_lines


class PublicationTest(unittest.TestCase):
    def test_collection_and_capture_finish_remain_separate(self):
        result = summarize_lines(
            [
                "dense replay collection sequence=8 records=4 applied_windows=1 deferred_capture=true capture_before_collection=false collect_ns=30000000 apply_ns=80000000",
                "dense capture stages batch=9 sequence=8 completed=true patch_ns=1000000 wal_ns=2000000 total_ns=3000000",
            ]
        )
        groups = result["groups"]
        self.assertEqual(
            groups["replay_collection"]["timings"]["collect_ns"]["sum_ms"], 30
        )
        self.assertEqual(groups["capture_finish"]["timings"]["total_ns"]["sum_ms"], 3)
        self.assertNotIn("completed_wait_ns", groups["replay_collection"]["timings"])

    def test_blocker_overlap_is_not_additive(self):
        result = summarize_lines(
            [
                "dense checkpoint rebase worker generation=2 sequence=3 rebase_stage_ns=200 success=true",
                "dense checkpoint completion blockers generation=2 source_capture_overlap_ns=150 maintenance_capture_overlap_ns=10 rebase_stage_ns=200 lock_deferrals=4",
            ]
        )
        self.assertEqual(result["groups"]["rebase_worker"]["count"], 1)
        self.assertEqual(
            result["groups"]["completion_blockers"]["timings"][
                "source_capture_overlap_ns"
            ]["sum_ms"],
            0.00015,
        )
        self.assertTrue(any("overlap each other" in note for note in result["notes"]))

    def test_nested_timers_and_unknown_cpu_are_not_combined(self):
        result = summarize_lines(
            [
                "dense checkpoint worker generation=2 sequence=10 kind=full build_wall_ns=4000000 build_thread_cpu_ns=2000000 success=true",
                "dense checkpoint worker generation=3 sequence=11 kind=delta build_wall_ns=5000000 build_thread_cpu_ns=null success=false",
                "dense checkpoint handoff generation=2 sequence=10 install_ns=3000000 completed_wait_ns=9000000",
                "dense checkpoint install generation=2 sequence=10 readers_ns=2000000 durable_ns=1000000",
            ]
        )
        groups = result["groups"]
        self.assertEqual(groups["worker"]["timings"]["build_wall_ns"]["sum_ms"], 9)
        cpu = groups["worker"]["timings"]["build_thread_cpu_ns"]
        self.assertEqual(cpu["sum_ms"], 2)
        self.assertEqual(cpu["missing_count"], 1)
        self.assertEqual(groups["handoff"]["timings"]["completed_wait_ns"]["max_ms"], 9)
        self.assertEqual(groups["install"]["timings"]["durable_ns"]["sum_ms"], 1)

    def test_old_logs_keep_limits_and_invalid_data_visible(self):
        result = summarize_lines(
            [
                "dense posting checkpoint staging sequence=10001 bytes=200 total_ns=2684370000",
                "shared vector-block maintenance needed reason=boundary_mismatch stable_tip_finalizing=true sequence_ready=true count_ready=false",
                "dense checkpoint worker generation=2 sequence=11 build_wall_ns=-5",
            ]
        )
        self.assertNotIn("worker", result["groups"])
        self.assertEqual(result["invalid_lines"], [3])
        self.assertEqual(
            result["groups"]["staging"]["timings"]["total_ns"]["sum_ms"], 2684.37
        )
        self.assertEqual(result["readiness_observations"]["sequence_ready=true"], 1)
        self.assertEqual(result["readiness_observations"]["count_ready=false"], 1)


if __name__ == "__main__":
    unittest.main()
