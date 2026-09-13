import unittest

from summarize_process_phases import summarize


def row(t, *, pid=1, incarnation=1, cpu=None):
    return {
        "wall_time_s": t,
        "monotonic_ns": t * 1000000000,
        "pid": pid,
        "start_abstime": incarnation,
        "rss_bytes": 10,
        "phys_footprint_bytes": 5,
        "user_cpu_ns": t * 2000000000 if cpu is None else cpu,
        "system_cpu_ns": t * 1000000000,
        "cpu_timebase_numer": 125,
        "cpu_timebase_denom": 3,
    }


class ProcessPhasesTest(unittest.TestCase):
    def test_phase_and_restart_boundaries_are_not_charged(self):
        result = summarize(
            [row(1), row(2), row(3), row(4, incarnation=2), row(5, incarnation=2)],
            [{"phase": "load", "wall_time": 1}, {"phase": "query", "wall_time": 3}],
        )
        self.assertEqual([p["average_cpu_cores"] for p in result["phases"]], [3, 3])
        self.assertEqual(result["excluded_intervals"]["process_lifetime_changed"], 1)
        self.assertEqual(result["excluded_intervals"]["phase_boundary"], 1)

    def test_legacy_units_and_clock_jumps_are_explicit(self):
        first, second = row(1), row(2)
        del first["cpu_timebase_numer"]
        result = summarize([first, second], [{"phase": "query", "wall_time": 0}])
        self.assertIsNone(result["phases"][0]["average_cpu_cores"])
        second["monotonic_ns"] = first["monotonic_ns"]
        result = summarize([first, second], [{"phase": "query", "wall_time": 0}])
        self.assertEqual(result["excluded_intervals"]["clock_discontinuity"], 1)
        with self.assertRaises(ValueError):
            summarize(
                [], [{"phase": "a", "wall_time": 2}, {"phase": "b", "wall_time": 1}]
            )

    def test_equal_duration_disjoint_cpu_samples_are_not_combined(self):
        samples = [row(t) for t in (1, 2, 3, 4)]
        # One second of user CPU and one second of system CPU, but never
        # together: adding their rates would fabricate a process CPU reading.
        for sample in samples[:2]:
            del sample["system_cpu_ns"]
        for sample in samples[2:]:
            del sample["user_cpu_ns"]
        phase = summarize(samples, [{"phase": "query", "wall_time": 0}])["phases"][0]
        self.assertEqual(phase["counters"]["user_cpu_ns"]["observed_seconds"], 1)
        self.assertEqual(phase["counters"]["system_cpu_ns"]["observed_seconds"], 1)
        self.assertIsNone(phase["average_cpu_cores"])
        self.assertEqual(phase["combined_cpu"]["intervals"], 0)


if __name__ == "__main__":
    unittest.main()
