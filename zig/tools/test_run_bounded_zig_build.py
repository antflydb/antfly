#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import importlib.util
import io
import os
import re
import subprocess
import sys
import tempfile
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
    def test_unit_inventory_failure_never_launches_the_unit_build(self):
        monitor = SCRIPT.parents[2] / "scripts/ci/run-zig-unit-tests.sh"
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "zig").mkdir()
            binaries = root / "bin"
            binaries.mkdir()
            trace = root / "commands"
            taskset = binaries / "taskset"
            taskset.write_text(
                '#!/bin/sh\nprintf "%s\\n" "$*" >> "$PREFLIGHT_TRACE"\n'
                'case "$*" in *unit-storage-test-audit*) exit 42;; *) exit 99;; esac\n'
            )
            taskset.chmod(0o755)
            # The production watchdog enables ptrace on Linux. This test must
            # never change the host's settings, even if sudo is available.
            sudo = binaries / "sudo"
            sudo.write_text("#!/bin/sh\nexit 0\n")
            sudo.chmod(0o755)
            result = subprocess.run(
                ["bash", str(monitor), "0-1", "21474836480"],
                cwd=root,
                env={
                    **os.environ,
                    "PATH": f"{binaries}{os.pathsep}{os.environ['PATH']}",
                    "RUNNER_TEMP": str(root),
                    "PREFLIGHT_TRACE": str(trace),
                },
                capture_output=True,
                text=True,
                timeout=15,
            )
            self.assertEqual(result.returncode, 42, result.stdout + result.stderr)
            commands = trace.read_text().splitlines()
            self.assertEqual(len(commands), 1)
            self.assertIn("unit-storage-test-audit", commands[0])
            self.assertNotIn("make zig-unit-test", commands[0])

    def test_runtime_memory_profiles(self):
        # Exercise the admission policy itself without constructing or compiling
        # the product graph. In particular, host/target and backend fallbacks
        # must not inherit claims measured only for native Linux CPU releases.
        subprocess.run(
            [
                os.environ.get("ZIG", "zig"),
                "test",
                "pkg/antfly/build/runtime_memory.zig",
            ],
            cwd=SCRIPT.parents[1],
            check=True,
        )

    def test_ci_unit_watchdog_leaves_runtime_after_cold_compilation(self):
        workflow = (SCRIPT.parents[2] / ".github/workflows/zig-tests.yml").read_text(
            encoding="utf-8"
        )
        monitor = (SCRIPT.parents[2] / "scripts/ci/run-zig-unit-tests.sh").read_text(
            encoding="utf-8"
        )
        defaults = {}
        for kind in ("IDLE", "HARD"):
            match = re.search(rf"ANTFLY_UNIT_WATCHDOG_{kind}_SECS:-(\d+)", monitor)
            self.assertIsNotNone(match)
            defaults[kind] = int(match.group(1))
        self.assertEqual(defaults["IDLE"], 30 * 60)
        self.assertEqual(defaults["HARD"], 90 * 60)
        for name in ("zig-base-tests", "zig-full-tests"):
            job = re.search(
                rf"^  {name}:\n(.*?)(?=^  [a-z][a-z0-9-]*:|\Z)",
                workflow,
                re.MULTILINE | re.DOTALL,
            )
            self.assertIsNotNone(job)
            self.assertIn("bash scripts/ci/run-zig-unit-tests.sh", job.group(1))
            self.assertIn("name: Retain unit test progress", job.group(1))
            outer = re.search(
                r"^    timeout-minutes: (\d+)$", job.group(1), re.MULTILINE
            )
            self.assertIsNotNone(outer)
            self.assertEqual(int(outer.group(1)), 120)
            self.assertLess(defaults["IDLE"], defaults["HARD"])
            self.assertLess(defaults["HARD"], int(outer.group(1)) * 60)

    def test_ci_scheduler_caps_admit_the_storage_compile_claim(self):
        # Production runtime construction owns the reservation. The root build
        # only composes owners; testing it would couple this contract to file
        # layout instead of the scheduler claim used by the storage artifact.
        runtime_build = SCRIPT.parents[1] / "pkg/antfly/build/runtime_memory.zig"
        workflow = (SCRIPT.parents[2] / ".github/workflows/zig-tests.yml").read_text(
            encoding="utf-8"
        )
        claim = subprocess.run(
            [
                os.environ.get("ZIG", "zig"),
                "run",
                "--dep",
                "runtime_memory",
                "-Mroot=tools/runtime_storage_compile_claim.zig",
                f"-Mruntime_memory={runtime_build}",
            ],
            cwd=SCRIPT.parents[1],
            check=True,
            capture_output=True,
            text=True,
        )
        # These CI caps apply to Linux; the separate macOS reservation is
        # checked by the runtime profile tests above.
        # Physical storage is its own archive. The distributed reservation no
        # longer includes DB codegen and cannot establish this admission check.
        required = int(claim.stderr.strip())
        self.assertGreater(required, 0)
        caps = re.findall(r"--max-rss-cap (\d+)", workflow)
        self.assertTrue(caps)
        for cap in caps:
            self.assertGreaterEqual(int(cap), required)

        # Check the effective job budget, not just the command-line cap. Match
        # ARC's 20 GiB budget and retain 4 GiB inside the 24 GiB pod. The former
        # 22 GiB override allowed the unit build to OOM-kill the whole runner.
        # Include E2E builders that do not pass --max-rss-cap at all.
        jobs = dict(
            re.findall(
                r"^  ([a-z][a-z0-9-]*):\n(.*?)(?=^  [a-z][a-z0-9-]*:|\Z)",
                workflow,
                re.MULTILINE | re.DOTALL,
            )
        )
        expected_runners = {
            "zig-base-tests": "arc-antfly-heavy",
            "zig-full-tests": "arc-antfly-heavy",
            "zig-build-cache-tests": "arc-antfly-heavy",
            "e2e-base-build": "arc-antfly-heavy",
            "e2e-full-build": "arc-antfly-heavy",
        }
        for job, runner in expected_runners.items():
            with self.subTest(job=job):
                self.assertIn(f"    runs-on: {runner}\n", jobs[job])
                if job in ("e2e-base-build", "e2e-full-build"):
                    # Compilation needs job-owned scratch on the heavy PVC,
                    # not the runtime runner's 10 GiB ephemeral filesystem.
                    self.assertIn("mountpoint -q /mnt/cache", jobs[job])
                    self.assertIn("$cache_root/zig-local", jobs[job])
                    self.assertIn("$cache_root/zig-global", jobs[job])
                    self.assertIn("$cache_root/tmp", jobs[job])
                    self.assertIn("Release completed E2E build scratch", jobs[job])
                budget = re.search(
                    r'^      ANTFLY_ZIG_MAX_RSS: "(\d+)"$',
                    jobs[job],
                    re.MULTILINE,
                )
                self.assertIsNotNone(budget, "declare ARC's 20 GiB compiler budget")
                configured = budget.group(1)
                # Keep driver, fixture pages and runtime tests inside the pod
                # envelope while admitting two 8 GiB compiler steps together.
                self.assertEqual(int(configured), 20 * 1024**3)
                self.assertGreaterEqual(int(configured), 2 * 8 * 1024**3)
                # The observed OOM overlapped three default 7 GiB compiles,
                # whose combined physical RSS exceeded the 24 GiB pod limit.
                self.assertLess(int(configured), 3 * 7 * 1024**3)
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
            ["build", "antfly-unit-test", "-Doptimize=debug"],
            Path("/tmp/patched-runner.zig"),
            10_000,
        )

        self.assertEqual(
            [
                "zig",
                "build",
                "antfly-unit-test",
                "-Doptimize=debug",
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
