# Copyright 2026 Antfly, Inc.
#
# Licensed under the Elastic License 2.0 (ELv2); you may not use this file
# except in compliance with the Elastic License 2.0. You may obtain a copy of
# the Elastic License 2.0 at
#
#     https://www.antfly.io/licensing/ELv2-license
#
# Unless required by applicable law or agreed to in writing, software distributed
# under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# Elastic License 2.0 for the specific language governing permissions and
# limitations.


from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from check_embedded_isolated_build import stage_sources

from audit_embedded_source_boundary import audit, production_imports, audit_modules


class EmbeddedBoundaryTest(unittest.TestCase):
    def test_isolated_stage_keeps_working_sources_and_omits_server_owners(self):
        with tempfile.TemporaryDirectory() as directory:
            repository = Path(directory) / "repo"
            stage = Path(directory) / "stage"
            files = {
                "zig/pkg/antfly/src/storage/db/db.zig": "local working change",
                "zig/pkg/antfly/src/storage/server_db_adapter.zig": "server",
                "zig/pkg/antfly/src/capi/server_owner.zig": "private server",
                "zig/pkg/antfly/src/tracing/server_raft_writer.zig": "raft trace",
                "specs/openapi/public.yaml": "contract",
                "scripts/codegen.py": "generator",
                "docs/plan.md": "unrelated",
            }
            for name, content in files.items():
                path = repository / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(content)
            listing = b"\0".join(name.encode() for name in files) + b"\0"
            with patch("check_embedded_isolated_build.subprocess.check_output", return_value=listing):
                self.assertEqual(stage_sources(repository, stage), 3)
            self.assertEqual((stage / "zig/pkg/antfly/src/storage/db/db.zig").read_text(), "local working change")
            self.assertTrue((stage / "specs/openapi/public.yaml").is_file())
            self.assertTrue((stage / "scripts/codegen.py").is_file())
            self.assertIn('@compileError("server implementation unavailable', (stage / "zig/pkg/antfly/src/capi/server_owner.zig").read_text())
            self.assertFalse((stage / "docs/plan.md").exists())

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

    def test_test_only_owner_and_nonempty_fallback(self):
        source = '\n'.join([
            'const fixture = if (builtin.is_test) @import("raft/fixture.zig") else struct {};',
            'const oracle = if (builtin.is_test) @import("lmdb_engine") else struct { const local = @import("fallback.zig"); };',
            'const Harness = if (builtin.is_test) struct { const runtime = @import("vopr"); } else struct {};',
        ])
        self.assertEqual(production_imports(source, include_named=True), ["fallback.zig"])

    def test_target_alternatives_keep_unknown_options(self):
        source = ('const remote = if (builtin.os.tag == .freestanding or build_options.minimal) '
                  '@import("stub.zig") else @import("remote");')
        self.assertEqual(production_imports(source, include_named=True, target_os="freestanding"), ["stub.zig"])
        self.assertEqual(production_imports(source, include_named=True, target_os="linux"), ["stub.zig", "remote"])

    def test_disabled_backend_guard_excludes_only_its_function(self):
        source = ('fn disabled() void { if (comptime !build_options.enable_pjrt) return error.Unavailable; '
                  '_ = @import("pjrt"); } fn other() void { _ = @import("local.zig"); }')
        self.assertEqual(production_imports(source, include_named=True, options={"enable_pjrt": False}), ["local.zig"])
        self.assertEqual(production_imports(source, include_named=True, options={"enable_pjrt": True}), ["pjrt", "local.zig"])
        self.assertEqual(production_imports(source, include_named=True, options={}), ["pjrt", "local.zig"])

    def test_runtime_control_flow_cannot_make_a_feature_guard_unconditional(self):
        for control in ("if (dynamic)", "while (dynamic)", "for (items) |_|", "if (dynamic) {} else"):
            with self.subTest(control=control):
                source = ('fn execute() void { ' + control +
                          ' if (!build_options.enable_pjrt) return; '
                          '_ = @import("storage/server_db_adapter.zig"); }')
                self.assertEqual(production_imports(source, options={"enable_pjrt": False}),
                                 ["storage/server_db_adapter.zig"])
        source = ('fn execute() void { if (dynamic) { if (!build_options.enable_pjrt) return; '
                  '_ = @import("disabled.zig"); } _ = @import("outside.zig"); }')
        self.assertEqual(production_imports(source, options={"enable_pjrt": False}), ["outside.zig"])

    def test_external_cache_does_not_exempt_antfly_generated_owners(self):
        with tempfile.TemporaryDirectory() as directory:
            project = Path(directory) / "project"
            cache = Path(directory) / "cache"
            project.mkdir()
            cache.mkdir()
            entry = project / "entry.zig"
            entry.write_text('const generated = @import("generated");')
            generated = cache / "generated.zig"
            generated.write_text('const sibling = @import("sibling.zig");')
            sibling = cache / "sibling.zig"
            sibling.write_text('const server = @import("server");')
            server = project / "pkg/antfly/src/storage/server_db_adapter.zig"
            server.parent.mkdir(parents=True)
            server.write_text("")
            modules = {"entry": entry, "generated": generated, "server": server}
            edges = {("entry", "generated"): "generated", ("generated", "server"): "server"}
            with self.assertRaisesRegex(ValueError, "server coordination"):
                audit_modules(project, modules, edges, "entry")
            sibling.write_text("")
            self.assertEqual(audit_modules(project, modules, edges, "entry"), 3)
            self.assertEqual(audit_modules(project, modules, {("entry", "generated"): "generated"},
                                          "entry", external_modules={"generated"}), 2)
            # Explicit dependencies cannot hide a declared Antfly import.
            with self.assertRaisesRegex(ValueError, "server coordination"):
                audit_modules(project, modules, edges, "entry", external_modules={"generated"})
            modules["dependency"] = cache / "dependency.zig"
            indirect = {("entry", "generated"): "generated",
                        ("generated", "dependency"): "dependency",
                        ("dependency", "server"): "server"}
            with self.assertRaisesRegex(ValueError, "server coordination"):
                audit_modules(project, modules, indirect, "entry",
                              external_modules={"generated", "dependency"})

    def test_target_struct_branch_preserves_local_imports(self):
        source = ('const backend = if (@import("builtin").os.tag == .freestanding) '
                  'struct { const local = @import("portable.zig"); } else @import("native");')
        self.assertEqual(production_imports(source, include_named=True, target_os="freestanding"), ["builtin", "portable.zig"])
        self.assertEqual(production_imports(source, include_named=True, target_os="linux"), ["builtin", "native"])

    def test_named_modules_use_their_own_import_tables(self):
        with tempfile.TemporaryDirectory() as directory:
            project = Path(directory)
            first = project / "first.zig"
            second = project / "second.zig"
            local = project / "local.zig"
            first.write_text('const child = @import("shared");')
            second.write_text('const local = @import("shared");')
            local.write_text("")
            self.assertEqual(audit_modules(project, {"first": first, "second": second, "local": local},
                {("first", "shared"): "second", ("second", "shared"): "local"}, "first"), 3)
            with self.assertRaisesRegex(ValueError, "unresolved module"):
                audit_modules(project, {"first": first}, {}, "first")

    def test_named_module_cannot_hide_a_server_owner(self):
        with tempfile.TemporaryDirectory() as directory:
            project = Path(directory)
            entry = project / "entry.zig"
            entry.write_text('const innocent_name = @import("contracts");')
            for name in ("server_db_adapter.zig", "metadata_hot_standby_port.zig"):
                with self.subTest(owner=name):
                    owner = project / "pkg/antfly/src/storage" / name
                    owner.parent.mkdir(parents=True, exist_ok=True)
                    owner.write_text("")
                    with self.assertRaisesRegex(ValueError, "server coordination"):
                        audit_modules(project, {"entry": entry, "server": owner}, {("entry", "contracts"): "server"}, "entry")


if __name__ == "__main__":
    unittest.main()
