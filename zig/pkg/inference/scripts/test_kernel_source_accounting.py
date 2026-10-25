# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
import importlib.util
from pathlib import Path
import unittest
from unittest import mock
import tempfile
from types import SimpleNamespace

spec = importlib.util.spec_from_file_location(
    "kernel_source_accounting", Path(__file__).with_name("kernel_source_accounting.py"))
accounting = importlib.util.module_from_spec(spec)
spec.loader.exec_module(accounting)


class SourceAccountingTest(unittest.TestCase):
    def test_shared_test_helpers_do_not_count_as_production_source(self):
        helper = "zig/pkg/inference/src/ops/testing/reference.zig"
        production = "zig/pkg/inference/src/ops/kernel_math.zig"
        before = b"pub fn baseline() void {}\n"
        after = "pub fn replacement() void {}\npub fn extra() void {}\n"
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for name in (helper, production):
                path = root / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(after)
            with mock.patch.object(accounting.subprocess, "check_output", side_effect=["0" * 40, helper + "\n" + production + "\n", "", "1" * 40]), mock.patch.object(accounting.subprocess, "run", return_value=SimpleNamespace(stdout=before)):
                receipt = accounting.account(root, "baseline")
        expected_bytes = len(before) - len(after.encode())
        self.assertEqual(receipt["authored_production"]["bytes_removed"], expected_bytes)
        self.assertEqual(receipt["groups"]["tests_and_benchmark_tooling"]["bytes_removed"], expected_bytes)
        self.assertEqual(sum(row["bytes_removed"] for row in receipt["groups"].values()), 2 * expected_bytes)

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

    def test_metal_fragment_owners_are_device_source_and_conserve_all_bytes(self):
        owner = '// Native phase contract.\n#define TERMITE_MSL_PHASE \\\n    "first\\n" \\\n    "second\\n"\n'
        function = 'static NSString *termite_metal_shader_source(void) {\n    return @"prefix\\n" TERMITE_MSL_PHASE;\n}'
        prefix, middle, suffix = 'host before\n', '\nhost middle\n', '\nhost after\n'
        source = prefix + owner + middle + function + suffix
        device, host = accounting.shader(source)
        self.assertEqual(device, owner + function)
        self.assertEqual(host, prefix + middle + suffix)
        self.assertEqual(len(device) + len(host), len(source))

    def test_generated_region_keeps_both_boundary_markers(self):
        generated = '// quant-kernel-codegen:begin fixture\nbody\n// quant-kernel-codegen:end fixture\n'
        authored, emitted = accounting.managed('before\n' + generated + 'after\n')
        self.assertEqual(authored, 'before\nafter\n')
        self.assertEqual(emitted, generated)


if __name__ == '__main__':
    unittest.main()
