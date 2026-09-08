import unittest

from summarize_dense_publication import summarize_lines


class PublicationTest(unittest.TestCase):
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
