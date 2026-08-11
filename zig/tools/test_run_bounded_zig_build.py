#!/usr/bin/env python3

import importlib.util
import io
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

    def test_large_host_cannot_admit_every_runtime_unit_together(self):
        with mock.patch.dict(os.environ, {}, clear=True):
            with mock.patch.object(
                launcher,
                "detect_memory_limit",
                return_value=64 * 1024 * 1024 * 1024,
            ):
                self.assertEqual(
                    launcher.DEFAULT_SCHEDULER_CEILING,
                    launcher.detect_max_rss(),
                )

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

    def test_newer_unrecognized_zig_uses_stock_runner(self):
        with mock.patch.object(launcher, "zig_lib_dir", return_value=Path("/zig/lib")):
            with mock.patch.object(
                launcher,
                "patch_build_runner",
                side_effect=RuntimeError("unknown runner"),
            ):
                with mock.patch.object(launcher, "zig_version", return_value=(0, 17, 0)):
                    self.assertIsNone(
                        launcher.prepare_build_runner("zig", Path("/tmp/patched.zig"))
                    )

    def test_unrecognized_zig_0_16_runner_fails_closed(self):
        with mock.patch.object(launcher, "zig_lib_dir", return_value=Path("/zig/lib")):
            with mock.patch.object(
                launcher,
                "patch_build_runner",
                side_effect=RuntimeError("unknown runner"),
            ):
                with mock.patch.object(launcher, "zig_version", return_value=(0, 16, 0)):
                    with self.assertRaisesRegex(RuntimeError, "unknown runner"):
                        launcher.prepare_build_runner("zig", Path("/tmp/patched.zig"))

    def test_print_max_rss_uses_shared_host_aware_detection(self):
        with mock.patch.object(sys, "argv", [str(SCRIPT), "--print-max-rss"]):
            with mock.patch.object(launcher, "detect_max_rss", return_value=123_456):
                output = io.StringIO()
                with mock.patch("sys.stdout", output):
                    self.assertEqual(0, launcher.main())
        self.assertEqual("123456\n", output.getvalue())


if __name__ == "__main__":
    unittest.main()
