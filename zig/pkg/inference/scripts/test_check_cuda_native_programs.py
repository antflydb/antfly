# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
import unittest

from check_cuda_native_programs import records, select_programs, retired_programs


class NativeRecordsTest(unittest.TestCase):
    def test_resource_format_and_multiple_architectures(self):
        report = "arch = sm_75\n Function kernel:\n REG:32 STACK:0\narch = sm_120\n Function kernel:\n REG:40 STACK:0\n"
        parsed = records(report, True)
        self.assertEqual(len(parsed), 2)
        self.assertEqual(parsed[("sm_120", "kernel")], ["REG:40 STACK:0"])

    def test_native_cubin_resources_inherit_the_verified_single_code_arch(self):
        report = "Resource usage:\n Function kernel:\n REG:32 STACK:0\n"
        self.assertEqual(
            records(report, True, "sm_121"), {("sm_121", "kernel"): ["REG:32 STACK:0"]}
        )

    def test_sass_code_arch_and_function_format(self):
        report = "code for sm_121\n Function : kernel\n /*0000*/ MOV R1, R2;\n /* 0x1234 */\n"
        self.assertEqual(
            records(report, False),
            {("sm_121", "kernel"): ["/*0000*/ MOV R1, R2;", "/* 0x1234 */"]},
        )

    def test_requested_programs_must_exist_in_both_record_sets(self):
        rows = {( "sm_89", "qualified"): ["record"], ("sm_89", "unrelated"): ["other"]}
        selected = select_programs([rows, rows], ["qualified"])
        self.assertEqual(list(selected[0]), [("sm_89", "qualified")])
        with self.assertRaisesRegex(AssertionError, "missing requested"):
            select_programs([rows, {}], ["qualified"])
        with self.assertRaisesRegex(AssertionError, "missing requested"):
            select_programs([rows, rows], ["absent"])

    def test_retirement_requires_exact_removal_across_every_architecture(self):
        original = {(arch, name): [] for arch in ("sm_75", "sm_120") for name in ("live", "retired")}
        candidate = {key: value for key, value in original.items() if key[1] == "live"}
        self.assertEqual(retired_programs(original, candidate, ["retired"]), {("sm_75", "retired"), ("sm_120", "retired")})
        for rows, names in (({}, ["retired"]), (candidate, []), (original, ["retired"]), ({**candidate, ("sm_75", "retired"): []}, ["retired"]), (candidate, ["absent"])):
            with self.subTest(rows=rows, names=names), self.assertRaises(AssertionError):
                retired_programs(original, rows, names)

    def test_empty_malformed_and_duplicate_reports_are_errors(self):
        for text in [
            "",
            "arch = sm_75\n REG:32 STACK:0",
            "Function kernel:\n REG:32 STACK:0",
            "arch = sm_75\n Function kernel:\n Function kernel:\n REG:32",
        ]:
            with self.subTest(text=text), self.assertRaises(AssertionError):
                records(text, True)


if __name__ == "__main__":
    unittest.main()
