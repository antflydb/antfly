# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Compile/link and runner contracts for independently compiled consumer tests."""

import platform
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

import importlib.util

_SPEC = importlib.util.spec_from_file_location(
    "test_runtime_cache", Path(__file__).with_name("test_runtime_cache.py")
)
assert _SPEC and _SPEC.loader
_cache = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_cache)
ZIG_ROOT = _cache.ZIG_ROOT
link_children = _cache.link_children


class LinkedTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="antfly-linked-tests-")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.build_dir = self.root / "zig"
        link_children(ZIG_ROOT, self.build_dir)
        (self.build_dir / "build.zig").unlink()
        shutil.copyfile(
            ZIG_ROOT / "tools/fixtures/linked_tests.zig", self.build_dir / "build.zig"
        )
        self.write("consumer.c", "int fixtureCValue(void) { return 7; }\n")
        self.write("contract.zig", "pub const minimum: u32 = 42;\n")
        self.write(
            "provider.zig",
            'const c = @import("contract.zig");\nexport fn fixtureValue() u32 { return c.minimum; }\n',
        )
        self.write(
            "consumer.zig",
            """const std = @import("std");
const contract = @import("contract.zig");
extern fn fixtureValue() u32;
extern fn fixtureCValue() c_int;
test "fixture consumer" {
    try std.testing.expectEqual(@as(c_int, 7), fixtureCValue());
    const value = fixtureValue();
    try std.testing.expect(value >= contract.minimum);
    std.debug.print("provider-value={d}\\n", .{value});
}
""",
        )
        self.write("implementation.zig", 'test "fixture implementation" {}\n')

    def write(self, name, text):
        (self.build_dir / name).write_text(text)

    def build(self, *args, succeeds=True):
        result = subprocess.run(
            [
                "zig",
                "build",
                "--cache-dir",
                str(self.root / "cache"),
                "--summary",
                "all",
                "--color",
                "off",
                "-j2",
                *args,
            ],
            cwd=self.build_dir,
            text=True,
            capture_output=True,
            timeout=240,
        )
        output = result.stdout + result.stderr
        self.assertEqual(result.returncode == 0, succeeds, output)
        return output

    def assert_compile(self, output, kind, name, state):
        self.assertRegex(output, rf"compile {kind} fixture-{name} Debug \S+ {state}")

    def test_cache_and_linked_behavior(self):
        self.assertIn("provider-value=42", self.build("test"))
        output = self.build("test")
        self.assert_compile(output, "test_obj", "consumer", "cached")
        self.assert_compile(output, "lib", "provider", "cached")
        self.write(
            "provider.zig",
            'const c = @import("contract.zig");\nexport fn fixtureValue() u32 { return c.minimum + 1; }\n',
        )
        output = self.build("test")
        self.assertIn("provider-value=43", output)
        self.assert_compile(output, "test_obj", "consumer", "cached")
        self.assert_compile(output, "lib", "provider", "success")
        self.assert_compile(output, "exe", "consumer", "success")
        path = self.build_dir / "consumer.zig"
        path.write_text(path.read_text() + "\n// consumer-only edit\n")
        output = self.build("test")
        self.assert_compile(output, "test_obj", "consumer", "success")
        self.assert_compile(output, "lib", "provider", "cached")
        self.write("consumer.c", "int fixtureCValue(void) { return 2 + 5; }\n")
        output = self.build("test")
        self.assert_compile(output, "test_obj", "consumer", "cached")
        self.assert_compile(output, "exe", "consumer", "success")
        self.assert_compile(output, "lib", "provider", "cached")
        self.write("contract.zig", "pub const minimum: u32 = 43;\n")
        output = self.build("test")
        self.assertIn("provider-value=44", output)
        self.assert_compile(output, "test_obj", "consumer", "success")
        self.assert_compile(output, "lib", "provider", "success")

    def test_selection_and_failure_diagnostics(self):
        output = self.build("test", "--", "consumer")
        self.assertIn("1 passed", output)
        self.assertNotIn("fixture implementation...", output)
        output = self.build("test", "--", "missing", succeeds=False)
        self.assertIn("test filter matched no declared tests: missing", output)
        self.build("test", "--", "missing", "--allow-empty-test-filter")
        output = self.build(
            "test", "--", "fixture", "--skip-test-filter", "fixture", succeeds=False
        )
        self.assertIn("test selection matched no runnable tests", output)
        self.build(
            "test",
            "--",
            "fixture",
            "--skip-test-filter",
            "fixture",
            "--allow-empty-test-filter",
        )
        path = self.build_dir / "consumer.zig"
        path.write_text(
            path.read_text().replace(
                "value >= contract.minimum", "value < contract.minimum"
            )
        )
        output = self.build("test", succeeds=False)
        self.assertIn("consumer.zig:", output)
        self.assertIn("TestUnexpectedResult", output)

    def test_foreign_link(self):
        target = (
            "x86_64-linux-musl"
            if platform.system() != "Linux" or platform.machine() != "x86_64"
            else "aarch64-linux-musl"
        )
        self.build("compile", f"-Dtarget={target}")

    def test_concurrent_execution_retains_diagnostics_and_runs_again(self):
        self.write(
            "barrier.py",
            """import pathlib, sys, time
root = pathlib.Path(__file__).parent
name = sys.argv[1]
own = root / (name + ".count")
other = root / (("second" if name == "first" else "first") + ".count")
generation = int(own.read_text()) + 1 if own.exists() else 1
temporary = own.with_suffix(".tmp")
temporary.write_text(str(generation))
temporary.replace(own)
deadline = time.monotonic() + 10
while not other.exists() or int(other.read_text()) != generation:
    if time.monotonic() >= deadline:
        raise RuntimeError("independent test runs were serialized")
    time.sleep(.01)
print("stdout-" + name)
print("diagnostic-" + name, file=sys.stderr)
""",
        )
        for generation in (1, 2):
            output = self.build("concurrency")
            for name in ("first", "second"):
                self.assertIn("diagnostic-" + name, output)
                self.assertEqual(
                    (self.build_dir / (name + ".count")).read_text(), str(generation)
                )
        captured = list((self.root / "cache").rglob("stdout"))
        self.assertTrue(any(p.read_text() == "stdout-first\n" for p in captured))
        self.assertTrue(any(p.read_text() == "stdout-second\n" for p in captured))


if __name__ == "__main__":
    unittest.main()
