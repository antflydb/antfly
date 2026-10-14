import unittest

from qualify_sequence_fuzz import parse_report


REPORT = """======= FUZZING REPORT =======
Fuzz test: "src.codecs.fuzz_test.test.video fuzz sequence AV1 interframes layers retained outputs and cleanup" (0123456789abcdef)
Runs: 0 -> 10651
Unique runs: 0 -> 240
Coverage: 0/48147 -> 7349/48147 (15.26%)
Build Summary: 3/3 steps succeeded; 3/3 tests passed
"""


class QualificationReportTests(unittest.TestCase):
    def test_complete_target_report(self):
        self.assertEqual(parse_report(REPORT, "av1")["runs"], 10651)

    def test_zero_exit_panic_is_not_success(self):
        with self.assertRaises(ValueError):
            parse_report("thread 123 panic: integer overflow\n" + REPORT, "av1")

    def test_wrong_target_is_not_qualified(self):
        with self.assertRaises(ValueError):
            parse_report(REPORT, "vp9")

    def test_cumulative_counts_are_rejected(self):
        with self.assertRaises(ValueError):
            parse_report(REPORT.replace("Runs: 0", "Runs: 300"), "av1")

    def test_missing_or_ambiguous_report_is_rejected(self):
        for log in ("Build Summary: 3/3 steps succeeded", REPORT + REPORT):
            with self.subTest(log=log), self.assertRaises(ValueError):
                parse_report(log, "av1")


if __name__ == "__main__":
    unittest.main()
