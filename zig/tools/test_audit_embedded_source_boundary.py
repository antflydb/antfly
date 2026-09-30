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

from pathlib import Path
import tempfile
import unittest

from audit_embedded_source_boundary import audit, production_imports


class EmbeddedBoundaryTest(unittest.TestCase):
    def test_dynamic_imports_fail_closed(self):
        with self.assertRaisesRegex(ValueError, "literal source owner"):
            production_imports("const server = @import(source_path);")
        self.assertEqual(production_imports('const std = @import("std");'), [])

    def test_ignores_comments_strings_and_test_bodies(self):
        source = r"""
// @import("raft/server.zig")
const note = "@import(\"raft/server.zig\")";
const text =
    \\@import("raft/server.zig")
;
test "nested { and escaped quotes" {
    const helper = struct { const server = @import("raft/server.zig"); };
}
test { _ = @import("data/runtime.zig"); }
const local = @import("db.zig");
fn lazy() void { _ = @import("local.zig"); }
"""
        self.assertEqual(production_imports(source), ["db.zig", "local.zig"])

    def test_cycle_and_transitive_server_import(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "root.zig").write_text('const db = @import("db.zig");')
            (root / "db.zig").write_text('const root = @import("root.zig");')
            self.assertEqual(len(audit(root, ["root.zig"])), 2)
            (root / "db.zig").write_text(
                'fn lazy() void { _ = @import("raft/server.zig"); }'
            )
            (root / "raft").mkdir()
            (root / "raft/server.zig").write_text("")
            with self.assertRaisesRegex(
                ValueError, "root.zig -> db.zig -> raft/server.zig"
            ):
                audit(root, ["root.zig"])

    def test_missing_and_escaping_imports_fail(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            entry = root / "root.zig"
            for dependency, message in [
                ("missing.zig", "missing source"),
                ("../server.zig", "outside its source owner"),
            ]:
                entry.write_text(f'const db = @import("{dependency}");')
                with self.assertRaisesRegex(ValueError, message):
                    audit(root, ["root.zig"])


if __name__ == "__main__":
    unittest.main()
