# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
import os
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from tools import fixture_workspace as workspace


class FixtureWorkspaceTests(unittest.TestCase):
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
                ):
                    with self.assertRaises(ValueError):
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
