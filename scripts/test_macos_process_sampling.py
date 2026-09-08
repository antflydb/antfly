import unittest

from sample_macos_process_memory import cpu_nanoseconds


class CpuUnitsTest(unittest.TestCase):
    def test_intel_and_arm_timebases(self):
        self.assertEqual(cpu_nanoseconds(24000000, (125, 3)), 1000000000)
        self.assertEqual(cpu_nanoseconds(1000000000, (1, 1)), 1000000000)
        self.assertEqual(cpu_nanoseconds(1, (125, 3)), 41)
        self.assertEqual(cpu_nanoseconds(2**63, (125, 3)), (2**63 * 125) // 3)

    def test_invalid_counters_are_not_zero_measurements(self):
        for ticks, timebase in ((-1, (1, 1)), (1, (1, 0)), (1, (0, 1))):
            with self.assertRaises(ValueError):
                cpu_nanoseconds(ticks, timebase)


if __name__ == "__main__":
    unittest.main()
