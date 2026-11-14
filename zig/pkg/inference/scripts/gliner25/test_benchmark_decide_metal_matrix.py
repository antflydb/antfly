#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Exercise benchmark provenance across real Git staging transitions."""
import pathlib
import subprocess
import tempfile
import unittest

from benchmark_decide_metal_matrix import source_diff


class SourceIdentityTests(unittest.TestCase):
    def test_staging_preserves_identity_and_edits_change_it(self):
        scratch = pathlib.Path(".benchmark-results")
        scratch.mkdir(exist_ok=True)
        with tempfile.TemporaryDirectory(dir=scratch) as directory:
            root = pathlib.Path(directory)

            def git(*args):
                subprocess.run(["git", *args], cwd=root, check=True,
                               stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)

            git("init", "--initial-branch=fixture")
            path = root / "source.zig"
            path.write_text("baseline\n")
            git("add", "source.zig")
            git("-c", "user.name=Benchmark Fixture", "-c", "user.email=fixture@invalid",
                "-c", "commit.gpgsign=false", "-c", "core.hooksPath=/dev/null",
                "commit", "-m", "fixture")
            path.write_text("measured source\n")
            measured = source_diff(root)
            self.assertTrue(measured)
            git("add", "source.zig")
            self.assertEqual(measured, source_diff(root))
            git("restore", "--staged", "source.zig")
            self.assertEqual(measured, source_diff(root))
            path.write_text("changed source\n")
            self.assertNotEqual(measured, source_diff(root))


if __name__ == "__main__":
    unittest.main()
