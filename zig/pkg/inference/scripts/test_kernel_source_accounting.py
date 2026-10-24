# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
import importlib.util
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location(
    "kernel_source_accounting", Path(__file__).with_name("kernel_source_accounting.py"))
accounting = importlib.util.module_from_spec(spec)
spec.loader.exec_module(accounting)


class SourceAccountingTest(unittest.TestCase):
    def test_zig_test_ownership_ignores_literals_comments_and_nested_braces(self):
        production = '''/*
/* nested comment */
test "comment only" { ignored }
*/
const text = "test \\"fake\\" { }";
const multiline =
    \\\\test "inside literal" { }
;
'''
        block = '''test "real test" {
    const close = '}';
    const text = "}";
    // }
    if (true) { _ = close; }
}
'''
        after = 'pub fn retained() void {}\n'
        authored, tests, count = accounting.split_zig_tests(production + block + after)
        self.assertEqual(authored, production + after)
        self.assertEqual(tests, block)
        self.assertEqual(count, 1)
        self.assertEqual(len(authored) + len(tests), len(production + block + after))

    def test_generated_region_keeps_both_boundary_markers(self):
        generated = '// quant-kernel-codegen:begin fixture\nbody\n// quant-kernel-codegen:end fixture\n'
        authored, emitted = accounting.managed('before\n' + generated + 'after\n')
        self.assertEqual(authored, 'before\nafter\n')
        self.assertEqual(emitted, generated)


if __name__ == '__main__':
    unittest.main()
