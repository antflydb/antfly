#!/usr/bin/env python3

import importlib.util
import io
import os
import re
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
    def test_ci_unit_watchdog_leaves_runtime_after_cold_compilation(self):
        workflow = (SCRIPT.parents[2] / ".github/workflows/zig-tests.yml").read_text(
            encoding="utf-8"
        )
        job = re.search(
            r"^  zig-base-tests:\n(.*?)(?=^  [a-z][a-z0-9-]*:|\Z)",
            workflow,
            re.MULTILINE | re.DOTALL,
        )
        self.assertIsNotNone(job)
        defaults = {}
        for kind in ("IDLE", "HARD"):
            match = re.search(rf"ANTFLY_UNIT_WATCHDOG_{kind}_SECS:-(\d+)", job.group(1))
            self.assertIsNotNone(match)
            defaults[kind] = int(match.group(1))
        outer = re.search(r"^    timeout-minutes: (\d+)$", job.group(1), re.MULTILINE)
        self.assertIsNotNone(outer)
        # The observed cold-build critical path consumed 52 minutes before
        # DB-core execution. Extend the total budget, not the idle threshold.
        self.assertEqual(defaults["IDLE"], 30 * 60)
        self.assertEqual(defaults["HARD"], 90 * 60)
        self.assertEqual(int(outer.group(1)), 120)
        self.assertLess(defaults["IDLE"], defaults["HARD"])
        self.assertLess(defaults["HARD"], int(outer.group(1)) * 60)

    def test_ci_scheduler_caps_admit_the_storage_compile_claim(self):
        # Production runtime construction owns the reservation. The root build
        # only composes owners; testing it would couple this contract to file
        # layout instead of the scheduler claim used by the storage artifact.
        runtime_build = SCRIPT.parents[1] / "pkg/antfly/build/runtime.zig"
        build = runtime_build.read_text(encoding="utf-8")
        workflow = (SCRIPT.parents[2] / ".github/workflows/zig-tests.yml").read_text(
            encoding="utf-8"
        )
        claim = re.search(
            r"\.distributed => @as\(usize, if \(target.result.os.tag == .macos\) "
            r"(?P<macos>\d+) else (?P<linux>\d+)\) \* 1024 \* 1024 \* 1024",
            build,
        )
        self.assertIsNotNone(
            claim, "update this contract when storage claims change shape"
        )
        # ARC is Linux. Keep macOS's independently measured reservation out
        # of this Linux scheduler contract, without losing the target branch.
        required = int(claim.group("linux")) * 1024**3
        caps = re.findall(r"--max-rss-cap (\d+)", workflow)
        self.assertTrue(caps)
        for cap in caps:
            self.assertGreaterEqual(int(cap), required)

        # A sufficient CLI cap is not enough: ARC injects a smaller default
        # ANTFLY_ZIG_MAX_RSS, which detect_max_rss respects before applying the
        # cap. Check each compiling job's explicit override, including the E2E
        # builders that do not pass --max-rss-cap at all.
        jobs = dict(
            re.findall(
                r"^  ([a-z][a-z0-9-]*):\n(.*?)(?=^  [a-z][a-z0-9-]*:|\Z)",
                workflow,
                re.MULTILINE | re.DOTALL,
            )
        )
        for job in (
            "zig-base-tests",
            "zig-full-tests",
            "zig-build-cache-tests",
            "e2e-base-build",
            "e2e-full-build",
        ):
            with self.subTest(job=job):
                self.assertIn("    runs-on: arc-antfly-heavy\n", jobs[job])
                budget = re.search(
                    r'^      ANTFLY_ZIG_MAX_RSS: "(\d+)"$',
                    jobs[job],
                    re.MULTILINE,
                )
                self.assertIsNotNone(budget, "override ARC's inherited 20 GiB budget")
                configured = budget.group(1)
                # Heavy pods have a 24 GiB limit; reserve at least 2 GiB for
                # the driver/runner rather than admitting all pod memory.
                self.assertLessEqual(int(configured), 22 * 1024**3)
                with mock.patch.dict(
                    os.environ, {launcher.MAX_RSS_ENV: configured}, clear=True
                ):
                    self.assertGreaterEqual(launcher.detect_max_rss(), required)
                    for cap in caps:
                        self.assertGreaterEqual(
                            launcher.detect_max_rss(int(cap)), required
                        )

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

    def test_workload_cap_limits_a_large_host(self):
        with mock.patch.dict(os.environ, {}, clear=True):
            with mock.patch.object(
                launcher,
                "detect_memory_limit",
                return_value=64 * 1024 * 1024 * 1024,
            ):
                self.assertEqual(
                    16 * 1024 * 1024 * 1024,
                    launcher.detect_max_rss(16 * 1024 * 1024 * 1024),
                )

    def test_uncapped_build_uses_detected_host_budget(self):
        with mock.patch.dict(os.environ, {}, clear=True):
            with mock.patch.object(
                launcher, "detect_memory_limit", return_value=40_000
            ):
                self.assertEqual(32_000, launcher.detect_max_rss())

    def test_ci_cap_admits_storage_kernel_without_overriding_small_cgroup(self):
        cap = 22 * 1024 * 1024 * 1024
        with mock.patch.dict(os.environ, {}, clear=True):
            with mock.patch.object(
                launcher, "detect_memory_limit", return_value=64 * 1024**3
            ):
                self.assertEqual(cap, launcher.detect_max_rss(cap))
            with mock.patch.object(
                launcher, "detect_memory_limit", return_value=16 * 1024**3
            ):
                self.assertEqual(int(16 * 1024**3 * 0.8), launcher.detect_max_rss(cap))

    def test_command_adds_missing_scheduler_options(self):
        command = launcher.build_command(
            "zig",
            ["build", "antfly-unit-test", "-Doptimize=Debug"],
            Path("/tmp/patched-runner.zig"),
            10_000,
        )

        self.assertEqual(
            [
                "zig",
                "build",
                "antfly-unit-test",
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
            "antfly-unit-test",
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
            ["build", "antfly-metadata-test", "--", "reconciler test"],
            Path("/tmp/patched-runner.zig"),
            10_000,
        )

        self.assertEqual(
            [
                "zig",
                "build",
                "antfly-metadata-test",
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
                with mock.patch.object(
                    launcher, "zig_version", return_value=(0, 17, 0)
                ):
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
                with mock.patch.object(
                    launcher, "zig_version", return_value=(0, 16, 0)
                ):
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
