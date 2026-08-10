#!/usr/bin/env python3

import importlib.util
import os
import sys
import unittest
from pathlib import Path
from unittest import mock


SCRIPT = Path(__file__).with_name("run_bounded_zig_build.py")
SPEC = importlib.util.spec_from_file_location("run_bounded_zig_build", SCRIPT)
assert SPEC and SPEC.loader
launcher = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = launcher
sys.path.insert(0, str(SCRIPT.parent))
SPEC.loader.exec_module(launcher)


class BoundedZigBuildTest(unittest.TestCase):
    def test_environment_override_is_used_as_exact_budget(self):
        with mock.patch.dict(os.environ, {launcher.MAX_RSS_ENV: "123456"}):
            self.assertEqual(123456, launcher.detect_max_rss())

    def test_invalid_environment_override_is_rejected(self):
        with mock.patch.dict(os.environ, {launcher.MAX_RSS_ENV: "not-bytes"}):
            with self.assertRaisesRegex(RuntimeError, "positive byte count"):
                launcher.detect_max_rss()

    def test_detected_memory_limit_reserves_twenty_percent_headroom(self):
        with mock.patch.dict(os.environ, {}, clear=True):
            with mock.patch.object(
                launcher,
                "detect_memory_limit",
                return_value=10_000,
            ):
                self.assertEqual(8_000, launcher.detect_max_rss())

    def test_command_adds_missing_scheduler_options(self):
        command = launcher.build_command(
            "zig",
            ["build", "unit-test", "-Doptimize=Debug"],
            Path("/tmp/patched-runner.zig"),
            10_000,
        )

        self.assertEqual(
            [
                "zig",
                "build",
                "unit-test",
                "-Doptimize=Debug",
                "--build-runner",
                "/tmp/patched-runner.zig",
                "--maxrss",
                "10000",
            ],
            command,
        )

    def test_command_preserves_explicit_scheduler_options(self):
        arguments = [
            "build",
            "unit-test",
            "--build-runner=/tmp/ci-runner.zig",
            "--maxrss=20000",
        ]

        self.assertEqual(
            ["zig", *arguments],
            launcher.build_command("zig", arguments, Path("unused"), 10_000),
        )

    def test_command_adds_scheduler_options_before_runtime_arguments(self):
        command = launcher.build_command(
            "zig",
            ["build", "unit-metadata-test", "--", "reconciler test"],
            Path("/tmp/patched-runner.zig"),
            10_000,
        )

        self.assertEqual(
            [
                "zig",
                "build",
                "unit-metadata-test",
                "--build-runner",
                "/tmp/patched-runner.zig",
                "--maxrss",
                "10000",
                "--",
                "reconciler test",
            ],
            command,
        )


if __name__ == "__main__":
    unittest.main()
