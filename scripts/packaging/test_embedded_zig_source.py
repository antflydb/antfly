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

"""Source archive ownership, immutable inputs, and reproducibility checks."""

import hashlib
import json
import subprocess
import tarfile
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import package_embedded_zig_source as package


class SourcePackageTests(unittest.TestCase):
    def fixture(self, root):
        files = {
            package.PACKAGE + "/build.zig": 'const std = @import("std");\n',
            package.PACKAGE
            + "/build.zig.zon": '.{ .name = .antfly_embedded, .version = "0.1.0", .dependencies = .{ .composition = .{ .path = "../.." } }, .paths = .{ "src" }, }\n',
            package.PACKAGE + "/README.md": "Apache source package\n",
            package.PACKAGE
            + "/src/root.zig": "// SPDX-License-Identifier: Apache-2.0\n",
            "zig/embedded.build.zig": '// SPDX-License-Identifier: Apache-2.0\nconst std = @import("std");\n',
            "zig/build.zig": "old composition",
            "zig/build.zig.zon": "pinned third party manifest",
            "zig/pkg/inference/src/root.zig": "Apache inference",
            "zig/pkg/antfly/src/root.zig": "// SPDX-License-Identifier: Elastic-2.0\n",
            "zig/e2e/antfly/test.zig": "server fixture",
            "LICENSES/Apache-2.0.txt": "Apache license",
            "LICENSES/Elastic-2.0.txt": "Elastic license",
            "LICENSES/MIT.txt": "MIT dependency license",
            "THIRD_PARTY_NOTICES.md": "dependency notices",
            "specs/openapi/query.yaml": "query contract",
            "scripts/tool.py": "# SPDX-License-Identifier: Apache-2.0\n",
        }
        for name, content in files.items():
            path = root / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(content)

        def git(*args):
            return subprocess.check_output(["git", *args], cwd=root).decode().strip()

        git("init", "-q")
        git("add", ".")
        git(
            "-c",
            "user.name=Test",
            "-c",
            "user.email=test@example.com",
            "-c",
            "commit.gpgsign=false",
            "commit",
            "-qm",
            "fixture",
        )
        return git("rev-parse", "HEAD")

    def test_selection_excludes_server_owners_and_preserves_notices(self):
        for path in (
            "zig/pkg/antfly/src/root.zig",
            "zig/e2e/antfly/test.zig",
            "LICENSES/Elastic-2.0.txt",
            "py/project.py",
        ):
            self.assertFalse(package.selected(path), path)
        for path in (
            "zig/pkg/antfly-embedded/src/root.zig",
            "zig/pkg/inference/src/root.zig",
            "zig/lib/raft/root.zig",
            "specs/openapi/query.yaml",
            "THIRD_PARTY_NOTICES.md",
            "LICENSES/MIT.txt",
        ):
            self.assertTrue(package.selected(path), path)

    def test_immutable_staging_does_not_take_dirty_working_tree(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw) / "repo"
            root.mkdir()
            commit = self.fixture(root)
            (root / "scripts/tool.py").write_text("dirty")
            staged = Path(raw) / "stage"
            package.stage(root, commit, staged)
            self.assertIn("Apache-2.0", (staged / "scripts/tool.py").read_text())
            self.assertFalse((staged / "zig/pkg/antfly").exists())
            self.assertFalse((staged / "LICENSES/Elastic-2.0.txt").exists())

    def test_unmapped_elastic_source_fails_closed_but_strings_are_allowed(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw) / "repo"
            root.mkdir()
            commit = self.fixture(root)
            path = root / "zig/new_owner.zig"
            path.write_text(
                '// SPDX-License-Identifier: Apache-2.0\nconst fixture = "SPDX-License-Identifier: Elastic-2.0";\n'
            )
            package.stage(root, commit, Path(raw) / "safe", True)
            path.write_text("// SPDX-License-Identifier: Elastic-2.0\n")
            with self.assertRaisesRegex(ValueError, "ELv2 source"):
                package.stage(root, commit, Path(raw) / "unsafe", True)

    def test_source_archive_is_reproducible_and_has_complete_manifest(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw) / "repo"
            root.mkdir()
            commit = self.fixture(root)
            check_output = subprocess.check_output

            def command(args, **kwargs):
                if args[:2] == ["zig-fixture", "fetch"]:
                    return "antfly_embedded-0.2.1-fixtureHash\n"
                return check_output(args, **kwargs)

            with mock.patch.object(
                package.subprocess, "check_output", side_effect=command
            ):
                first = package.build(
                    root, commit, "0.2.1", Path(raw) / "first", "zig-fixture"
                )
                second = package.build(
                    root, commit, "0.2.1", Path(raw) / "second", "zig-fixture"
                )
            self.assertEqual(first, second)
            self.assertFalse(first["working_tree"])
            with tarfile.open(Path(raw) / "first" / first["archive"]) as archive:
                members = {
                    item.name.removeprefix("./"): item
                    for item in archive
                    if item.isfile()
                }
                contents = {
                    name: archive.extractfile(member).read()
                    for name, member in members.items()
                }
                manifest = json.loads(contents["SOURCE-MANIFEST.json"])
                self.assertEqual(manifest["commit"], commit)
                self.assertEqual(manifest["version"], "0.2.1")
                self.assertEqual(
                    {entry["path"] for entry in manifest["files"]},
                    set(contents) - {"SOURCE-MANIFEST.json"},
                )
                for entry in manifest["files"]:
                    self.assertEqual(
                        entry["sha256"],
                        hashlib.sha256(contents[entry["path"]]).hexdigest(),
                    )
                self.assertIn(b'.path = "zig"', contents["build.zig.zon"])
                self.assertIn(b"buildDependency(b, @This())", contents["zig/build.zig"])
                self.assertEqual(contents["LICENSE"], b"Apache license")
                self.assertNotIn(package.PACKAGE + "/build.zig.zon", contents)

    def test_source_links_and_stale_outputs_are_rejected(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw) / "repo"
            root.mkdir()
            commit = self.fixture(root)
            (root / "zig/broken.zig").symlink_to("missing.zig")
            with self.assertRaisesRegex(ValueError, "symlink"):
                package.stage(root, commit, Path(raw) / "stage", True)
            output = Path(raw) / "output"
            output.mkdir()
            (output / "stale").write_text("stale")
            with self.assertRaisesRegex(ValueError, "must be empty"):
                package.build(root, commit, "0.2.1", output, "zig")

    def test_invalid_commit_and_version_are_rejected(self):
        with tempfile.TemporaryDirectory() as raw:
            for commit, version in (
                ("HEAD", "0.2.1"),
                ("a" * 40, "../../bad"),
                ("a" * 40, "v0.2.1"),
            ):
                with self.assertRaises(ValueError):
                    package.build(Path(raw), commit, version, Path(raw), "zig")


if __name__ == "__main__":
    unittest.main()
