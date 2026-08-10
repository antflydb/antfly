#!/usr/bin/env python3

import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path


SCRIPT = Path(__file__).with_name("patch_zig_0_16_build_runner_maxrss.py")
SPEC = importlib.util.spec_from_file_location("patch_zig_0_16_build_runner_maxrss", SCRIPT)
assert SPEC and SPEC.loader
patcher = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = patcher
SPEC.loader.exec_module(patcher)


class BuildRunnerPatchTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)

    def tearDown(self):
        self.temporary.cleanup()

    def test_patches_exactly_one_zig_0_16_wake_loop(self):
        source = self.root / "build_runner.zig"
        destination = self.root / "patched" / "build_runner.zig"
        source.write_text("before\n" + patcher.OLD_WAKE_LOOP + "after\n", encoding="utf-8")

        patcher.patch_build_runner(source, destination)

        self.assertEqual(
            "before\n" + patcher.NEW_WAKE_LOOP + "after\n",
            destination.read_text(encoding="utf-8"),
        )

    def test_rejects_unknown_or_already_fixed_runner(self):
        source = self.root / "build_runner.zig"
        destination = self.root / "patched.zig"
        source.write_text(patcher.NEW_WAKE_LOOP, encoding="utf-8")

        with self.assertRaisesRegex(RuntimeError, "found 0"):
            patcher.patch_build_runner(source, destination)

        self.assertFalse(destination.exists())


if __name__ == "__main__":
    unittest.main()
