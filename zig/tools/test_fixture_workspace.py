# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
import os
import platform
import subprocess
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from tools import fixture_workspace as workspace


class FixtureWorkspaceTests(unittest.TestCase):
    def test_zig_fixtures_survive_workspace_cleanup_and_empty_environment(self):
        zig_root = Path(__file__).resolve().parents[1]
        # Match the production build's explicit Linux target: native libc
        # discovery can select host CRT objects unsupported by Zig's linker.
        target_flags = (
            ["-target", f"{platform.machine()}-linux-gnu"]
            if platform.system() == "Linux"
            else []
        )
        with tempfile.TemporaryDirectory() as temporary:
            temporary = Path(temporary)
            binary = temporary / "fixture-tests"
            build = subprocess.run(
                [
                    "zig",
                    "test",
                    "-lc",
                    *target_flags,
                    "--test-no-exec",
                    "--test-filter",
                    "test directory",
                    f"-femit-bin={binary}",
                    "--dep",
                    "antfly_platform",
                    f"-Mroot={zig_root / 'pkg/antfly/src/common/test_directory.zig'}",
                    f"-Mantfly_platform={zig_root / 'lib/platform/src/root.zig'}",
                ],
                cwd=temporary,
                capture_output=True,
                check=False,
                text=True,
                timeout=120,
            )
            self.assertEqual(build.returncode, 0, build.stdout + build.stderr)
            root = temporary / "workspace"
            root.mkdir()
            # Exercise actual Zig fixtures before and after CI removes its
            # private workspace and exports an empty value for later phases.
            for value in (None, str(root), ""):
                with self.subTest(workspace=value):
                    env = dict(os.environ)
                    env.pop("ANTFLY_TEST_WORKSPACE", None)
                    if value is not None:
                        env["ANTFLY_TEST_WORKSPACE"] = value
                    result = subprocess.run(
                        [str(binary)],
                        cwd=temporary,
                        env=env,
                        capture_output=True,
                        check=False,
                        text=True,
                        timeout=30,
                    )
                    self.assertEqual(
                        result.returncode, 0, result.stdout + result.stderr
                    )
                    if value == str(root):
                        root.rmdir()

    def test_rejects_disk_small_full_and_unbounded_filesystems(self):
        with tempfile.TemporaryDirectory() as root:
            for kind, total, free in [
                ("ext2/ext3", workspace.BUDGET, workspace.BUDGET),
                ("tmpfs", 64 << 20, 64 << 20),
                ("tmpfs", workspace.BUDGET, 32 << 20),
                ("tmpfs", 1 << 30, 1 << 30),
            ]:
                with (
                    self.subTest(kind=kind, total=total, free=free),
                    patch.object(
                        workspace.subprocess, "check_output", return_value=kind
                    ),
                    patch.object(
                        workspace.os,
                        "statvfs",
                        return_value=SimpleNamespace(
                            f_blocks=total, f_bavail=free, f_frsize=1
                        ),
                    ),
                    self.assertRaises(ValueError),
                ):
                    workspace.validate_root(root)

    def test_preprovisioned_volume_is_private_and_never_unmounted(self):
        with (
            tempfile.TemporaryDirectory() as root,
            patch.object(workspace.subprocess, "check_output", return_value="tmpfs\n"),
            patch.object(
                workspace.os,
                "statvfs",
                return_value=SimpleNamespace(
                    f_blocks=workspace.BUDGET, f_bavail=workspace.BUDGET, f_frsize=1
                ),
            ),
            patch.object(workspace.subprocess, "run") as run,
        ):
            first, mounted = workspace.prepare({"ANTFLY_CI_FIXTURE_ROOT": root})
            second, _ = workspace.prepare({"ANTFLY_CI_FIXTURE_ROOT": root})
            self.assertFalse(mounted)
            self.assertNotEqual(first, second)
            self.assertEqual(first.stat().st_mode & 0o777, 0o700)
            (first / "fixture").write_text("data")
            workspace.cleanup(first, mounted)
            self.assertTrue(second.exists())
            self.assertTrue(Path(root).exists())
            workspace.cleanup(second, False)
            run.assert_not_called()

    def test_bad_explicit_root_fails_instead_of_falling_back(self):
        with patch.object(workspace.subprocess, "run") as run:
            with self.assertRaises(ValueError):
                workspace.prepare({"ANTFLY_CI_FIXTURE_ROOT": "relative"})
            run.assert_not_called()

    def test_denied_mount_removes_only_empty_directory(self):
        with (
            tempfile.TemporaryDirectory() as root,
            patch.object(Path, "exists", return_value=False),
            patch.object(
                workspace.subprocess, "run", return_value=SimpleNamespace(returncode=1)
            ),
        ):
            self.assertIsNone(workspace.prepare({"RUNNER_TEMP": root}))
            self.assertEqual(os.listdir(root), [])

    def test_cleanup_rejects_shared_volume_root(self):
        with self.assertRaises(ValueError):
            workspace.cleanup("/mnt/antfly-fixtures", False)


if __name__ == "__main__":
    unittest.main()
