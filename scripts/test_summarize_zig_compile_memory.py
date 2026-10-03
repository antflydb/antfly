import unittest
from summarize_zig_compile_memory import GIB, summarize


class SummaryTests(unittest.TestCase):
    def test_units_and_concurrent_peak_keep_polls_distinct(self):
        trace = "header\n"
        trace += f"now\t0\t10\t{8 * GIB // 1024}\t8192\tzig build-lib --name antfly-storage-kernel -OReleaseFast\t0\n"
        trace += f"now\t0\t11\t{4 * GIB // 1024}\t4096\tzig build-lib --name antfly-runtime-inference\t0\n"
        trace += f"now\t0\t10\t{9 * GIB // 1024}\t9216\tzig build-lib --name antfly-storage-kernel\t1\n"
        report = summarize(trace)
        self.assertEqual(report["sampled_compiler_aggregate_peak_bytes"], 12 * GIB)
        self.assertEqual(
            report["units"]["storage_kernel"]["minimum_reservation_gib"], 12
        )
        self.assertEqual(report["units"]["inference"]["minimum_reservation_gib"], 5)
        self.assertTrue(report["aggregate_is_same_poll"])
        self.assertFalse(report["qualification"])

    def test_legacy_trace_marks_aggregate_as_approximate(self):
        report = summarize(
            "header\nnow\t0\t10\t1024\t1\tzig build-lib --name antfly-runtime-cli\n"
        )
        self.assertFalse(report["aggregate_is_same_poll"])
        self.assertEqual(report["units"]["cli"]["minimum_reservation_gib"], 1)


if __name__ == "__main__":
    unittest.main()
