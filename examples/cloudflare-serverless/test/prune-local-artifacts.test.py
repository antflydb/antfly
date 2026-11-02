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

import contextlib
import importlib.util
import io
from pathlib import Path
import subprocess
import sys
import tarfile
import tempfile
import unittest
from unittest.mock import patch

EXAMPLE = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "prune", EXAMPLE / "qualification/prune-local-artifacts.py"
)
PRUNE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(PRUNE)


class PruneSafety(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(
            prefix="prune-safety-", dir=EXAMPLE / "dist/qualification"
        )
        self.base = Path(self.temp.name)
        self.dist = self.base / "dist"
        self.dist.mkdir()
        self.original = (PRUNE.BASE, PRUNE.DIST, PRUNE.QUAL)
        PRUNE.BASE, PRUNE.DIST, PRUNE.QUAL = (
            self.base,
            self.dist,
            self.dist / "qualification",
        )
        PRUNE.QUAL.mkdir()

    def tearDown(self):
        PRUNE.BASE, PRUNE.DIST, PRUNE.QUAL = self.original
        self.temp.cleanup()

    def file(self, path, magic=b"\x00asm"):
        target = self.dist / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(magic + b"0" * (1024 * 1024))
        return target

    def test_credentials_symlinks_active_and_retained_modules_are_never_candidates(
        self,
    ):
        self.file("active.wasm")
        credential = self.file("archive/observability-token")
        self.file("qualification/local-retention-test/approved-control/module.wasm")
        outside = self.base / "outside.wasm"
        outside.write_bytes(b"\x00asm" + b"0" * (1024 * 1024))
        (self.dist / "archive/link.wasm").symlink_to(outside)
        original = Path.open

        def guarded(path, *args, **kwargs):
            if path == credential:
                raise AssertionError("Protected content was opened")
            return original(path, *args, **kwargs)

        with patch.object(Path, "open", guarded):
            self.assertEqual(PRUNE.inventory(), [])
        self.assertTrue(outside.exists())

    def test_only_known_binary_magic_is_selected_and_dry_run_does_not_delete(self):
        wasm = self.file("archive/module.wasm")
        native = self.file("archive/test", b"\x7fELF")
        self.file("archive/results.json", b'{"x"')
        self.assertEqual(
            {r["kind"] for r in PRUNE.inventory()},
            {"archived-wasm", "archived-native-executable"},
        )
        with (
            patch.object(sys, "argv", ["prune"]),
            contextlib.redirect_stdout(io.StringIO()),
        ):
            PRUNE.main()
        self.assertTrue(wasm.exists() and native.exists())

    def test_git_backed_archives_are_selected_but_uncommitted_snapshots_remain(self):
        folder = self.dist / "frozen"
        folder.mkdir()
        committed = folder / "source.tar"
        with committed.open("wb") as out:
            subprocess.run(
                ["git", "archive", "HEAD", "AGENTS.md"],
                cwd=PRUNE.REPO,
                stdout=out,
                check=True,
            )
        dirty = folder / "dirty.tar"
        with tarfile.open(dirty, "w"):
            pass
        rows = PRUNE.inventory()
        self.assertEqual([r["path"] for r in rows], ["dist/frozen/source.tar"])
        self.assertEqual(rows[0]["commit"], PRUNE.git("rev-parse", "HEAD"))
        self.assertTrue(dirty.exists())

    def test_empty_apply_is_an_idempotent_no_op(self):
        with (
            patch.object(sys, "argv", ["prune", "--apply"]),
            contextlib.redirect_stdout(io.StringIO()),
        ):
            PRUNE.main()
        self.assertEqual(list(PRUNE.QUAL.iterdir()), [])


if __name__ == "__main__":
    unittest.main()
