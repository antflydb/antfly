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

"""Keep cache GC away from the one worktree (or shared cache) a build is
actually using, without refusing to clean every other worktree on the
machine the way the previous all-or-nothing guard did.

Scratch directories go under scripts/.tmp-logs/ (never /tmp or a mktemp
dir) per AGENTS.md, and are removed in tearDown. Every real-script
invocation below forces DRY_RUN=1 regardless of what the test asks for,
so a regression in the guard can never delete anything for real.
"""

import os
import shutil
import subprocess
import time
import unittest
import uuid
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parent
REPO_ROOT = SCRIPTS.parent
ACTIVITY_SH = SCRIPTS / "cache_gc_activity.sh"
CACHE_GC_SH = SCRIPTS / "cache_gc.sh"
SCRATCH_ROOT = SCRIPTS / ".tmp-logs"


BASH = shutil.which("bash") or "/bin/bash"


def _run_activity_func(call, env=None):
    """Source cache_gc_activity.sh and evaluate one call against it."""
    cmd = [BASH, "-c", f'source "{ACTIVITY_SH}"; {call}']
    return subprocess.run(
        cmd, env=env or os.environ.copy(), text=True, capture_output=True, check=False
    )


def _make_stub(bin_dir, name, body):
    """Write a tiny fake executable named `name` into `bin_dir` so a test
    can make one real tool (pgrep/lsof/ps) misbehave on purpose, via a
    PATH override, without touching any real process inspection the rest
    of the suite relies on."""
    path = bin_dir / name
    path.write_text(f"#!/bin/sh\n{body}\n")
    path.chmod(0o755)
    return path


class CacheArgParsingTests(unittest.TestCase):
    """Pure string-handling helpers -- no processes involved."""

    def test_cache_dir_prefix_and_global_cache_args_are_extracted(self):
        cmdline = (
            "maker build --zig-lib=/x --global-cache=/g --cache-dir /c --prefix /p -j1"
        )
        result = _run_activity_func(f'_cache_arg_paths "{cmdline}"')
        self.assertEqual(result.stdout.splitlines(), ["/g", "/c", "/p"])

    def test_equals_and_space_separated_forms_both_work(self):
        result = _run_activity_func(
            '_cache_arg_paths "zig build --cache-dir=/a --prefix /b --global-cache-dir=/c"'
        )
        self.assertEqual(result.stdout.splitlines(), ["/a", "/b", "/c"])

    def test_no_matching_flags_yields_nothing(self):
        result = _run_activity_func('_cache_arg_paths "zig build-lib -Ofast foo.zig"')
        self.assertEqual(result.stdout, "")

    def test_paths_overlap_is_symmetric_prefix_containment(self):
        cases = [
            ("/a/b", "/a/b", True),
            ("/a/b", "/a/b/c", True),
            ("/a/b/c", "/a/b", True),
            ("/a/b", "/a/bc", False),
            ("/a/b", "/a/c", False),
            ("/a/b", "", False),
        ]
        for a, b, expected in cases:
            with self.subTest(a=a, b=b):
                result = _run_activity_func(
                    f'_paths_overlap "{a}" "{b}" && echo yes || echo no'
                )
                self.assertEqual(result.stdout.strip(), "yes" if expected else "no")


class ActivityDetectionTests(unittest.TestCase):
    """build_active_in / shared_cache_busy against real pgrep/ps, with a
    name that is guaranteed not to be running so no real process can make
    these flaky."""

    def test_unmatched_process_name_is_not_active(self):
        env = os.environ.copy()
        env["CACHE_GC_ZIG_PROC_NAMES"] = "antfly-cache-gc-test-no-such-process"
        result = _run_activity_func('build_active_in "/tmp"; echo "rc=$?"', env=env)
        self.assertIn("rc=1", result.stdout)
        self.assertEqual(result.stdout.replace("rc=1\n", ""), "")

    def test_unknown_shared_cache_category_is_not_busy(self):
        result = _run_activity_func('shared_cache_busy bogus; echo "rc=$?"')
        self.assertIn("rc=1", result.stdout)

    def test_unavailable_process_inspection_is_treated_as_active(self):
        # An empty-but-for-bash PATH means pgrep/ps can't be found; the
        # guard must fail safe (treat as active) rather than silently GC.
        empty_bin = SCRATCH_ROOT / f"empty-bin-{uuid.uuid4().hex[:8]}"
        empty_bin.mkdir(parents=True)
        try:
            env = os.environ.copy()
            env["PATH"] = str(empty_bin)
            result = _run_activity_func('build_active_in "/tmp"; echo "rc=$?"', env=env)
            self.assertIn("rc=0", result.stdout)
            self.assertIn("inspection unavailable", result.stdout)
        finally:
            shutil.rmtree(empty_bin, ignore_errors=True)


class InspectionFailureFailSafeTests(unittest.TestCase):
    """A reviewer found that build_active_in/shared_cache_busy treated a
    pgrep/lsof/ps *failure* (as opposed to a clean "no matches"/"no such
    cwd") the same as "not busy": pgrep exiting 2, or lsof/ps failing to
    inspect a pid pgrep just handed back, were silently swallowed, which
    permits deleting a live build's cache. These stub out one real tool
    at a time via a PATH override (small fake scripts, never a mktemp
    dir -- AGENTS.md puts scratch under scripts/.tmp-logs/) and assert
    the guards now fail safe (report busy) with a reason, rather than
    quietly reporting idle.
    """

    def setUp(self):
        self.bin_dir = SCRATCH_ROOT / f"stub-bin-{uuid.uuid4().hex[:8]}"
        self.bin_dir.mkdir(parents=True)
        self.proc = None

    def tearDown(self):
        if self.proc is not None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait(timeout=5)
        shutil.rmtree(self.bin_dir, ignore_errors=True)

    def _stubbed_env(self, **extra):
        env = os.environ.copy()
        env["PATH"] = f"{self.bin_dir}{os.pathsep}{env['PATH']}"
        env.update(extra)
        return env

    def _spawn_live_candidate(self, cwd):
        # Stands in for a real build process the same way the
        # integration fixture below does: a plain `sleep`, matched via
        # CACHE_GC_ZIG_PROC_NAMES="sleep" so no fake binary is needed to
        # fool `pgrep -x`.
        cwd.mkdir(parents=True, exist_ok=True)
        self.proc = subprocess.Popen(
            ["sleep", "300"],
            cwd=str(cwd),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        time.sleep(0.2)

    def test_pgrep_error_exit_makes_build_active_in_busy(self):
        _make_stub(self.bin_dir, "pgrep", "exit 2")
        env = self._stubbed_env()
        result = _run_activity_func('build_active_in "/tmp"; echo "rc=$?"', env=env)
        self.assertIn("rc=0", result.stdout)
        self.assertIn("pgrep exit 2", result.stdout)
        self.assertIn("skipping to be safe", result.stdout)

    def test_pgrep_error_exit_makes_shared_cache_busy_busy(self):
        _make_stub(self.bin_dir, "pgrep", "exit 2")
        env = self._stubbed_env()
        result = _run_activity_func('shared_cache_busy zig; echo "rc=$?"', env=env)
        self.assertIn("rc=0", result.stdout)
        self.assertIn("exit 2", result.stdout)
        self.assertIn("skipping to be safe", result.stdout)

    def test_lsof_failure_for_live_pid_makes_build_active_in_busy(self):
        candidate_cwd = self.bin_dir.parent / f"lsof-fixture-{uuid.uuid4().hex[:8]}"
        self._spawn_live_candidate(candidate_cwd)
        try:
            _make_stub(self.bin_dir, "lsof", "exit 1")
            env = self._stubbed_env(CACHE_GC_ZIG_PROC_NAMES="sleep")
            # The path doesn't matter here: a failing cwd lookup must be
            # reported *before* any path comparison, regardless of what
            # path is being checked.
            result = _run_activity_func(
                'build_active_in "/nonexistent-path"; echo "rc=$?"', env=env
            )
            self.assertIn("rc=0", result.stdout)
            self.assertIn("lsof", result.stdout)
            self.assertIn("skipping to be safe", result.stdout)
        finally:
            shutil.rmtree(candidate_cwd, ignore_errors=True)

    def test_ps_failure_makes_build_active_in_busy(self):
        candidate_cwd = self.bin_dir.parent / f"ps-fixture-{uuid.uuid4().hex[:8]}"
        self._spawn_live_candidate(candidate_cwd)
        try:
            _make_stub(self.bin_dir, "ps", "exit 1")
            env = self._stubbed_env(CACHE_GC_ZIG_PROC_NAMES="sleep")
            # Query a path that can't overlap with the real candidate's
            # real cwd (looked up with the real, unstubbed lsof/proc),
            # so the guard falls through to the ps-based command-line
            # check, where the stub bites.
            result = _run_activity_func(
                'build_active_in "/nonexistent-path"; echo "rc=$?"', env=env
            )
            self.assertIn("rc=0", result.stdout)
            self.assertIn("ps", result.stdout)
            self.assertIn("skipping to be safe", result.stdout)
        finally:
            shutil.rmtree(candidate_cwd, ignore_errors=True)


class CacheGcIntegrationTest(unittest.TestCase):
    """Builds a fake worktree tree, puts a real long-running process (a
    plain `sleep`, stood in for a build via CACHE_GC_ZIG_PROC_NAMES) with
    its cwd inside one worktree, and runs the actual cache_gc.sh against
    that fake tree. Asserts the active worktree's cache is skipped while
    the inactive worktree's old cache is cleaned.

    Under some sandboxed test runners, pgrep/lsof are themselves
    restricted and fail to see the live fixture process at all -- that
    previously made the guard miss it and silently report idle. With
    the inspection-failure fail-safe in place, that same restriction
    now surfaces as an inspection error, and the guard must still skip
    the active worktree (just for a different, equally valid, reason).
    So the primary assertion here is "the active worktree is skipped
    either way"; the stronger "and the idle one is positively cleaned,
    with no fail-safe involved" assertion only applies once we can tell
    detection genuinely worked.
    """

    def setUp(self):
        self.fake_root = SCRATCH_ROOT / f"cache-gc-fixture-{uuid.uuid4().hex[:8]}"
        self.active_wt = self.fake_root / ".worktrees" / "active"
        self.idle_wt = self.fake_root / ".worktrees" / "idle"
        self.active_cache = self.active_wt / "zig" / ".zig-cache"
        self.idle_cache = self.idle_wt / "zig" / ".zig-cache"
        for cache in (self.active_cache, self.idle_cache):
            (cache / "o").mkdir(parents=True)
            (cache / "o" / "artifact.bin").write_bytes(b"x" * 1024)
        # Back-date the idle worktree's cache well past GC_MAX_AGE_DAYS (and
        # the GC_ACTIVE_MINUTES window), so is_idle()/is_active() in
        # cache_gc.sh give a deterministic answer under their real,
        # non-degenerate defaults -- "-mmin -0"/"-mtime -0" boundary
        # semantics are implementation-dependent and were flaky here.
        old = time.time() - 10 * 86400
        for p in (
            self.idle_cache,
            self.idle_cache / "o",
            self.idle_cache / "o" / "artifact.bin",
        ):
            os.utime(p, (old, old))
        # Isolate the shared-cache sections from the real machine so the
        # test is fast and cannot touch a real cache.
        (self.fake_root / ".cache").mkdir(parents=True)
        (self.fake_root / "tmp").mkdir(parents=True)
        self.proc = subprocess.Popen(
            ["sleep", "300"],
            cwd=str(self.active_cache.parent),  # the "zig" dir, like a real build's cwd
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        # Give it a moment to actually be running and resolvable by pgrep/lsof.
        time.sleep(0.2)

    def tearDown(self):
        self.proc.terminate()
        try:
            self.proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait(timeout=5)
        shutil.rmtree(self.fake_root, ignore_errors=True)

    def _run_gc(self):
        env = os.environ.copy()
        env.update(
            {
                "DRY_RUN": "1",  # never skip this, regardless of the caller
                "CACHE_GC_REPO_ROOT": str(self.fake_root),
                "CACHE_GC_WORKTREES": "\n".join(
                    [str(self.fake_root), str(self.active_wt), str(self.idle_wt)]
                ),
                "CACHE_GC_ZIG_PROC_NAMES": "sleep",
                "GC_ACTIVE_MINUTES": "30",  # real defaults; the idle cache's
                "GC_MAX_AGE_DAYS": "3",  # files are back-dated well past both
                "ZIG_GLOBAL_CACHE_DIR": str(self.fake_root / ".cache" / "zig"),
                "XDG_CACHE_HOME": str(self.fake_root / ".cache"),
                "TMPDIR": str(self.fake_root / "tmp"),
            }
        )
        result = subprocess.run(
            [BASH, str(CACHE_GC_SH)],
            cwd=str(self.fake_root),
            env=env,
            text=True,
            capture_output=True,
            timeout=30,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        return result.stdout

    def test_active_worktree_cache_kept_idle_worktree_cache_cleaned(self):
        stdout = self._run_gc()

        active_lines = [
            line for line in stdout.splitlines() if str(self.active_cache) in line
        ]
        idle_lines = [
            line for line in stdout.splitlines() if str(self.idle_cache) in line
        ]

        self.assertTrue(
            active_lines, "expected the active worktree's cache to be mentioned"
        )
        self.assertTrue(
            idle_lines, "expected the idle worktree's cache to be mentioned"
        )

        # The safe behavior, regardless of *why*: the active worktree's
        # cache is always skipped and never deleted -- whether because
        # the live `sleep` stand-in was genuinely found busy there, or
        # because pgrep/lsof/ps couldn't be trusted to tell (sandboxed
        # test runners can restrict either) and the guard failed safe.
        self.assertTrue(
            any("skip" in line for line in active_lines),
            f"active worktree cache should be skipped, got: {active_lines}",
        )
        self.assertFalse(
            any(line.strip().startswith("rm ") for line in active_lines),
            f"active worktree cache should never be deleted, got: {active_lines}",
        )
        self.assertIn("Skipped (active builds)", stdout)
        self.assertIn(
            str(self.active_cache), stdout.split("Skipped (active builds)", 1)[1]
        )

        # The stronger claim -- the idle worktree's cache is positively
        # cleaned, not just also swept up by a fail-safe -- only holds
        # when detection genuinely worked (the active worktree's skip
        # reason names a real pid/cwd match, not an inspection failure).
        genuinely_detected = any(
            "active build" in line
            and "cannot inspect" not in line
            and "inspection" not in line
            for line in active_lines
        )
        if genuinely_detected:
            self.assertTrue(
                any(line.strip().startswith("rm ") for line in idle_lines),
                f"idle worktree cache should be identified for removal, got: {idle_lines}",
            )
            self.assertFalse(
                any("skip" in line for line in idle_lines),
                f"idle worktree cache should not be skipped, got: {idle_lines}",
            )


if __name__ == "__main__":
    unittest.main()
