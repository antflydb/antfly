# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
import unittest

from check_cuda_native_programs import records


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
