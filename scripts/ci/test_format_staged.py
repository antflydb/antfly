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

"""Verify the hook never stages unrelated or overlapping working-tree edits."""

import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import format_staged


class StagedFormattingTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.git("init", "-q")
        self.git("config", "user.email", "test@example.invalid")
        self.git("config", "user.name", "Test")
        self.git("config", "core.autocrlf", "false")

    def git(self, *args):
        return subprocess.check_output(
            ["git", "--literal-pathspecs", *args], cwd=self.root
        )

    def write(self, path, content):
        target = self.root / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(content)

    def base(self, path="example.py"):
        content = b"value = 0\n" + b"# context\n" * 12 + b"other = 0\n"
        self.write(path, content)
        self.git("add", "--", path)
        self.git("commit", "-qm", "base")
        return content

    def format(self, path, data):
        return data.replace(b"value=1", b"value = 1")

    def test_partial_staging_preserves_unstaged_hunk(self):
        original = self.base()
        staged = original.replace(b"value = 0", b"value=1")
        self.write("example.py", staged)
        self.git("add", "example.py")
        working = staged.replace(b"other = 0", b"other = 2")
        self.write("example.py", working)
        format_staged.format_staged(self.root, self.format)
        self.assertEqual(self.git("show", ":example.py"), self.format("", staged))
        self.assertEqual(
            (self.root / "example.py").read_bytes(), self.format("", working)
        )
        self.assertIn(b"other = 2", self.git("diff"))
        self.assertNotIn(b"other = 2", self.git("diff", "--cached"))

    def test_overlap_rejects_without_changing_index_or_files(self):
        original = self.base()
        staged = original.replace(b"value = 0", b"value=1")
        working = staged.replace(b"value=1", b"value=2")
        self.write("example.py", staged)
        self.git("add", "example.py")
        self.write("example.py", working)
        with self.assertRaisesRegex(format_staged.HookError, "No edits were applied"):
            format_staged.format_staged(self.root, self.format)
        self.assertEqual(self.git("show", ":example.py"), staged)
        self.assertEqual((self.root / "example.py").read_bytes(), working)

    def test_formatter_failure_does_not_apply_earlier_formatting(self):
        for name in ("a.py", "b.py"):
            self.write(name, b"value=1\n")
        self.git("add", ".")

        def failing(path, data):
            if path == "b.py":
                raise format_staged.HookError("formatter unavailable")
            return self.format(path, data)

        with self.assertRaisesRegex(format_staged.HookError, "unavailable"):
            format_staged.format_staged(self.root, failing)
        for name in ("a.py", "b.py"):
            self.assertEqual(self.git("show", f":{name}"), b"value=1\n")
            self.assertEqual((self.root / name).read_bytes(), b"value=1\n")

    def test_new_file_with_space_and_newline_preserves_mode(self):
        name = "a space\nand [bracket].py"
        self.write(name, b"value=1\n")
        (self.root / name).chmod(0o755)
        self.git("add", "--", name)
        format_staged.format_staged(self.root, self.format)
        self.assertEqual(self.git("show", f":{name}"), b"value = 1\n")
        self.assertTrue(os.access(self.root / name, os.X_OK))
        self.assertTrue(
            self.git("ls-files", "--stage", "--", name).startswith(b"100755")
        )

    def test_renamed_file_is_formatted_without_staging_untracked_files(self):
        self.base()
        self.git("mv", "example.py", "renamed.py")
        self.write("renamed.py", b"value=1\n")
        self.git("add", "renamed.py")
        self.write("untracked.py", b"value=1\n")
        format_staged.format_staged(self.root, self.format)
        self.assertEqual(self.git("show", ":renamed.py"), b"value = 1\n")
        self.assertNotIn(b"untracked.py", self.git("ls-files"))
        self.assertEqual((self.root / "untracked.py").read_bytes(), b"value=1\n")

    def test_deleted_files_and_unsupported_paths_are_not_formatted(self):
        self.base()
        self.git("rm", "example.py")
        self.write("document.md", b"unchanged\n")
        self.git("add", "document.md")
        with patch.object(format_staged, "run", wraps=format_staged.run):
            format_staged.format_staged(
                self.root, lambda *_: self.fail("unexpected formatter")
            )
        self.assertEqual(self.git("show", ":document.md"), b"unchanged\n")

    def test_missing_tool_reports_actionable_error(self):
        with self.assertRaisesRegex(
            format_staged.HookError, "Required formatting tool"
        ):
            format_staged.run(["missing-antfly-formatter-test"], cwd=self.root)

    def test_alternate_index_is_preserved(self):
        self.base()
        index = self.root / "alternate-index"
        shutil.copyfile(self.root / ".git/index", index)
        with patch.dict(os.environ, {"GIT_INDEX_FILE": str(index)}):
            self.write("example.py", b"value=1\n")
            self.git("add", "example.py")
            format_staged.format_staged(self.root, self.format)
            self.assertEqual(self.git("show", ":example.py"), b"value = 1\n")
        self.assertIn(b"value = 0", self.git("show", ":example.py"))

    def test_split_index_and_custom_diff_settings(self):
        self.base()
        self.git("config", "color.ui", "always")
        self.git("config", "diff.noprefix", "true")
        self.git("config", "diff.context", "0")
        self.write("example.py", b"value=1\n")
        self.git("add", "example.py")
        self.git("update-index", "--split-index")
        format_staged.format_staged(self.root, self.format)
        self.assertEqual(self.git("show", ":example.py"), b"value = 1\n")

    def test_installed_hook_formats_commit_and_leaves_unstaged_file_alone(self):
        if not shutil.which("gofmt"):
            self.skipTest("gofmt is required for the installed hook smoke test")
        repository = Path(__file__).resolve().parents[2]
        for path in (".githooks/pre-commit", "scripts/format_staged.py"):
            self.write(path, (repository / path).read_bytes())
        (self.root / ".githooks/pre-commit").chmod(0o755)
        self.git("config", "core.hooksPath", ".githooks")
        self.write("example.go", b"package main\nfunc main(){}\n")
        self.write("unstaged.go", b"package main\nfunc other(){}\n")
        self.git("add", "example.go")
        self.git("commit", "-qm", "formatted")
        self.assertEqual(
            self.git("show", "HEAD:example.go"), b"package main\n\nfunc main() {}\n"
        )
        self.assertEqual(
            (self.root / "unstaged.go").read_bytes(), b"package main\nfunc other(){}\n"
        )


if __name__ == "__main__":
    unittest.main()
