from __future__ import annotations

import dataclasses
import pathlib
import tempfile
import unittest
from unittest import mock

from scripts.merge_audit import audit_split_declarations as audit


class SplitDeclarationAuditTest(unittest.TestCase):
    def test_companion_policy_scope_is_path_scoped(self) -> None:
        policy = {
            "split_migrations": {
                "db": {
                    "declaration_mixins": [
                        {"path": "platform/sync.zig", "factory": "Impl", "owner": "DB"},
                        {"path": "outside.zig", "factory": "Impl", "owner": "DB"},
                    ],
                    "declaration_companion_policies": {
                        "db/core.zig": {
                            "destinations": ["db/core.zig", "platform/sync.zig"],
                            "reviewed_resolutions": {"function:open": {"reason": "reviewed"}},
                        },
                    },
                },
            },
        }

        scoped, scope_name = audit.companion_policy_scope(
            policy,
            "db",
            "db/core.zig",
        )

        self.assertEqual("db::companion::db/core.zig", scope_name)
        self.assertEqual("db/core.zig", scoped["split_migrations"][scope_name]["source"])
        self.assertEqual(
            ["db/core.zig", "platform/sync.zig"],
            scoped["split_migrations"][scope_name]["destinations"],
        )
        self.assertIn(
            "function:open",
            scoped["split_migrations"][scope_name]["reviewed_resolutions"],
        )
        self.assertEqual(
            [{"path": "platform/sync.zig", "factory": "Impl", "owner": "DB"}],
            scoped["split_migrations"][scope_name]["declaration_mixins"],
        )
        self.assertNotIn(scope_name, policy["split_migrations"])

    def test_companion_policy_scope_rejects_malformed_path_entry(self) -> None:
        policy = {
            "split_migrations": {
                "db": {
                    "declaration_companion_policies": {"db/core.zig": []},
                },
            },
        }

        with self.assertRaisesRegex(ValueError, "must be an object"):
            audit.companion_policy_scope(policy, "db", "db/core.zig")

    def test_historical_audit_skips_fully_absent_placement_range(self) -> None:
        body = "fn existing() void {}\n"

        def ref_text(ref: str, path: str) -> str:
            if ref in {"base", "incoming"} and path == "monolith.zig":
                return body
            self.fail(f"unexpected ref lookup: {ref}:{path}")

        with tempfile.TemporaryDirectory(
            prefix="merge-audit-historical-range-",
            dir=audit.ROOT,
        ) as raw_dir:
            destination = pathlib.Path(raw_dir) / "split.zig"
            destination.write_text(body)
            with mock.patch.object(audit, "ref_text", side_effect=ref_text):
                obligations, _ = audit.analyze(
                    "monolith.zig",
                    "base",
                    "incoming",
                    [destination],
                    0.7,
                    0.05,
                    include_unchanged=True,
                    declaration_placement_ranges=[{
                        "start": "function:futureStart",
                        "end": "function:futureEnd",
                        "path": str(destination.relative_to(audit.ROOT)),
                        "reason": "Introduced after this historical baseline.",
                    }],
                )

        self.assertEqual(["exact"], [item.status for item in obligations])

    def test_historical_audit_rejects_partially_absent_placement_range(self) -> None:
        body = "fn futureStart() void {}\n"

        def ref_text(ref: str, path: str) -> str:
            if ref in {"base", "incoming"} and path == "monolith.zig":
                return body
            self.fail(f"unexpected ref lookup: {ref}:{path}")

        with tempfile.TemporaryDirectory(
            prefix="merge-audit-partial-range-",
            dir=audit.ROOT,
        ) as raw_dir:
            destination = pathlib.Path(raw_dir) / "split.zig"
            destination.write_text(body)
            with mock.patch.object(audit, "ref_text", side_effect=ref_text):
                with self.assertRaisesRegex(ValueError, "range boundaries"):
                    audit.analyze(
                        "monolith.zig",
                        "base",
                        "incoming",
                        [destination],
                        0.7,
                        0.05,
                        include_unchanged=True,
                        declaration_placement_ranges=[{
                            "start": "function:futureStart",
                            "end": "function:futureEnd",
                            "path": str(destination.relative_to(audit.ROOT)),
                            "reason": "Malformed partial historical range.",
                        }],
                    )

    def test_similarity_bounds_large_repetitive_declarations(self) -> None:
        repeated = "    }\n" * (audit.LARGE_DECLARATION_LINE_THRESHOLD + 1)
        with mock.patch.object(audit.difflib, "SequenceMatcher") as matcher:
            matcher.return_value.ratio.return_value = 0.75

            self.assertEqual(0.75, audit.similarity(repeated, repeated))

        matcher.assert_called_once()
        self.assertTrue(matcher.call_args.kwargs["autojunk"])

    def test_similarity_keeps_exact_matching_for_normal_declarations(self) -> None:
        with mock.patch.object(audit.difflib, "SequenceMatcher") as matcher:
            matcher.return_value.ratio.return_value = 1.0

            self.assertEqual(1.0, audit.similarity("fn a() void {}", "fn a() void {}"))

        self.assertFalse(matcher.call_args.kwargs["autojunk"])

    def test_split_visibility_normalization_ignores_layout(self) -> None:
        incoming = "fn helper(a: usize, b: usize) usize { return a + b; }\n"
        split = """pub fn helper(
    a: usize,
    b: usize,
) usize {
    return a + b;
}
"""
        self.assertEqual(
            audit.normalized_split_visibility(incoming),
            audit.normalized_split_visibility(split),
        )

    def test_token_normalization_preserves_string_whitespace(self) -> None:
        self.assertNotEqual(
            audit.normalized_zig_tokens('const value = "a b";'),
            audit.normalized_zig_tokens('const value = "ab";'),
        )

    def test_expand_destinations_can_inventory_pinned_ref(self) -> None:
        with mock.patch.object(
            audit,
            "run_git",
            return_value=(
                "zig/pkg/antfly/build/tests.zig\n"
                "zig/pkg/antfly/build/tools.zig\n"
            ),
        ) as run_git:
            paths = audit.expand_destinations(
                ["zig/pkg/antfly/build"],
                "branch-sha",
            )

        self.assertEqual(
            [
                audit.ROOT / "zig/pkg/antfly/build/tests.zig",
                audit.ROOT / "zig/pkg/antfly/build/tools.zig",
            ],
            paths,
        )
        run_git.assert_called_once_with(
            [
                "ls-tree",
                "-r",
                "--name-only",
                "branch-sha",
                "--",
                "zig/pkg/antfly/build",
            ]
        )

    def test_expand_destinations_skips_paths_added_after_pinned_ref(self) -> None:
        def ls_tree(args: list[str]) -> str:
            return (
                "zig/pkg/antfly/build/tests.zig\n"
                if args[-1] == "zig/pkg/antfly/build"
                else ""
            )

        with mock.patch.object(audit, "run_git", side_effect=ls_tree):
            paths = audit.expand_destinations(
                ["zig/pkg/antfly/build", "zig/build_test_filters.zig"],
                "branch-sha",
            )

        self.assertEqual(
            [audit.ROOT / "zig/pkg/antfly/build/tests.zig"],
            paths,
        )

    def test_expand_destinations_rejects_entirely_missing_pinned_tree(self) -> None:
        with mock.patch.object(audit, "run_git", return_value=""):
            with self.assertRaisesRegex(ValueError, "no destinations exist"):
                audit.expand_destinations(
                    ["zig/pkg/antfly/missing"],
                    "branch-sha",
                )

    def test_analyze_reads_destinations_from_pinned_ref(self) -> None:
        body = "fn value() u8 { return 1; }\n"

        def ref_text(ref: str, path: str) -> str:
            if ref in {"base", "incoming"} and path == "monolith.zig":
                return body
            if ref == "branch" and path == "virtual/split.zig":
                return body
            self.fail(f"unexpected ref lookup: {ref}:{path}")

        with mock.patch.object(audit, "ref_text", side_effect=ref_text):
            obligations, _ = audit.analyze(
                "monolith.zig",
                "base",
                "incoming",
                [audit.ROOT / "virtual/split.zig"],
                0.7,
                0.05,
                include_unchanged=True,
                destination_ref="branch",
            )

        self.assertEqual(["exact"], [item.status for item in obligations])

    def test_analyze_matches_unique_renamed_test_by_exact_body(self) -> None:
        incoming = 'test "original contract" { try verify(); }\n'
        current = 'test "split owner original contract" { try verify(); }\n'

        def ref_text(ref: str, path: str) -> str:
            if ref in {"base", "incoming"} and path == "monolith.zig":
                return incoming
            self.fail(f"unexpected ref lookup: {ref}:{path}")

        with tempfile.TemporaryDirectory(
            prefix="merge-audit-renamed-test-body-",
            dir=audit.ROOT,
        ) as raw_dir:
            destination = pathlib.Path(raw_dir) / "split.zig"
            destination.write_text(current)
            with mock.patch.object(audit, "ref_text", side_effect=ref_text):
                obligations, _ = audit.analyze(
                    "monolith.zig",
                    "base",
                    "incoming",
                    [destination],
                    0.7,
                    0.05,
                    include_unchanged=True,
                )

        self.assertEqual(["split_test_name_adapted"], [item.status for item in obligations])

    def test_analyze_does_not_guess_between_duplicate_renamed_test_bodies(self) -> None:
        incoming = 'test "original contract" { try verify(); }\n'
        current = (
            'test "first split contract" { try verify(); }\n'
            'test "second split contract" { try verify(); }\n'
        )

        def ref_text(ref: str, path: str) -> str:
            if ref in {"base", "incoming"} and path == "monolith.zig":
                return incoming
            self.fail(f"unexpected ref lookup: {ref}:{path}")

        with tempfile.TemporaryDirectory(
            prefix="merge-audit-duplicate-test-body-",
            dir=audit.ROOT,
        ) as raw_dir:
            destination = pathlib.Path(raw_dir) / "split.zig"
            destination.write_text(current)
            with mock.patch.object(audit, "ref_text", side_effect=ref_text):
                obligations, _ = audit.analyze(
                    "monolith.zig",
                    "base",
                    "incoming",
                    [destination],
                    0.7,
                    0.05,
                    include_unchanged=True,
                )

        self.assertEqual(["missing_carried"], [item.status for item in obligations])

    def test_analyze_accepts_only_hash_locked_absent_intentional_deletions(
        self,
    ) -> None:
        body = "fn unused() void {}\n"
        digest = audit.sha256_text(body)

        def ref_text(ref: str, path: str) -> str:
            if ref in {"base", "incoming"} and path == "monolith.zig":
                return body
            self.fail(f"unexpected ref lookup: {ref}:{path}")

        with tempfile.TemporaryDirectory(
            prefix="merge-audit-intentional-deletion-",
            dir=audit.ROOT,
        ) as raw_dir:
            destination = pathlib.Path(raw_dir) / "split.zig"
            destination.write_text("")
            review = {
                "function:unused": {
                    "incoming_sha256": digest,
                    "reason": "The split refactor removed an unused wrapper.",
                }
            }
            with mock.patch.object(audit, "ref_text", side_effect=ref_text):
                obligations, _ = audit.analyze(
                    "monolith.zig",
                    "base",
                    "incoming",
                    [destination],
                    0.7,
                    0.05,
                    include_unchanged=True,
                    intentional_declaration_deletions=review,
                )
            self.assertEqual(
                ["intentional_deletion"],
                [item.status for item in obligations],
            )

            stale = {
                "function:unused": {
                    "incoming_sha256": "0" * 64,
                    "reason": "Stale review.",
                }
            }
            with mock.patch.object(audit, "ref_text", side_effect=ref_text):
                with self.assertRaisesRegex(ValueError, "stale incoming_sha256"):
                    audit.analyze(
                        "monolith.zig",
                        "base",
                        "incoming",
                        [destination],
                        0.7,
                        0.05,
                        include_unchanged=True,
                        intentional_declaration_deletions=stale,
                    )

            destination.write_text(body)
            with mock.patch.object(audit, "ref_text", side_effect=ref_text):
                with self.assertRaisesRegex(ValueError, "still has a split"):
                    audit.analyze(
                        "monolith.zig",
                        "base",
                        "incoming",
                        [destination],
                        0.7,
                        0.05,
                        include_unchanged=True,
                        intentional_declaration_deletions=review,
                    )

    def test_intentional_declaration_deletions_are_manifest_driven(self) -> None:
        policy = {
            "split_migrations": {
                "db": {
                    "intentional_declaration_deletions": {
                        "function:unused": {
                            "incoming_sha256": "a" * 64,
                            "reason": "Unused after the split refactor.",
                        }
                    }
                }
            }
        }
        self.assertEqual(
            policy["split_migrations"]["db"][
                "intentional_declaration_deletions"
            ],
            audit.split_intentional_declaration_deletions(policy, "db"),
        )

    def test_analyze_supports_source_path_renames_across_revisions(self) -> None:
        base = "fn value() u8 { return 1; }\n"
        incoming = "fn value() u8 { return 2; }\n"
        with tempfile.TemporaryDirectory(
            prefix="merge-audit-path-rename-",
            dir=audit.ROOT,
        ) as raw_dir:
            path = pathlib.Path(raw_dir) / "runtime.zig"
            path.write_text(incoming)

            def ref_text(ref: str, source: str) -> str:
                if (ref, source) == ("base", "old/runtime.zig"):
                    return base
                if (ref, source) == ("incoming", "new/runtime.zig"):
                    return incoming
                self.fail(f"unexpected ref lookup: {ref}:{source}")

            with mock.patch.object(audit, "ref_text", side_effect=ref_text):
                obligations, _ = audit.analyze(
                    "new/runtime.zig",
                    "base",
                    "incoming",
                    [path],
                    0.7,
                    0.05,
                    base_source="old/runtime.zig",
                    incoming_source="new/runtime.zig",
                )

        self.assertEqual(["exact"], [item.status for item in obligations])

    def test_analyze_supports_container_and_owner_renames(self) -> None:
        base = """const OldRuntime = struct {
    fn value(self: *OldRuntime) u8 { _ = self; return 1; }
};
"""
        incoming = """const OldRuntime = struct {
    fn value(self: *OldRuntime) u8 { _ = self; return 2; }
};
"""
        current = """const NewRuntime = struct {
    fn value(self: *NewRuntime) u8 { _ = self; return 2; }
};
"""
        with tempfile.TemporaryDirectory(
            prefix="merge-audit-declaration-rename-",
            dir=audit.ROOT,
        ) as raw_dir:
            path = pathlib.Path(raw_dir) / "runtime.zig"
            path.write_text(current)

            def ref_text(ref: str, source: str) -> str:
                self.assertEqual("runtime.zig", source)
                return {"base": base, "incoming": incoming}[ref]

            with mock.patch.object(audit, "ref_text", side_effect=ref_text):
                obligations, _ = audit.analyze(
                    "runtime.zig",
                    "base",
                    "incoming",
                    [path],
                    0.7,
                    0.05,
                    declaration_name_aliases={
                        "OldRuntime": ["NewRuntime"],
                    },
                )

        self.assertEqual(
            ["split_module_reference_adapted", "split_module_reference_adapted"],
            sorted(item.status for item in obligations),
        )

    def test_container_field_lines_canonicalize_declared_aliases(self) -> None:
        old = audit.all_declarations(
            "const Config = struct {\n    tick_ms: u64 = 100,\n};\n",
            "old.zig",
        )[0]
        new = audit.all_declarations(
            "const Config = struct {\n    control_tick_ms: u64 = 100,\n};\n",
            "new.zig",
        )[0]
        aliases = {"tick_ms": ["control_tick_ms"]}

        self.assertEqual(
            audit.direct_container_field_lines(
                old,
                symbol_reference_migrations=aliases,
            ),
            audit.direct_container_field_lines(
                new,
                symbol_reference_migrations=aliases,
            ),
        )

    def test_added_container_compares_fields_before_name_collision(self) -> None:
        incoming = """const State = struct {
    value: u64 = 0,
    fn incomingOnly() void {}
};
"""
        current = """const State = struct {
    value: u64 = 0,
    fn branchOnly() void {}
};
"""
        with tempfile.TemporaryDirectory(
            prefix="merge-audit-added-container-",
            dir=audit.ROOT,
        ) as raw_dir:
            path = pathlib.Path(raw_dir) / "split.zig"
            path.write_text(current)

            def ref_text(ref: str, _: str) -> str:
                return {"base": "", "incoming": incoming}[ref]

            with mock.patch.object(audit, "ref_text", side_effect=ref_text):
                obligations, _ = audit.analyze(
                    "monolith.zig",
                    "base",
                    "incoming",
                    [path],
                    0.7,
                    0.05,
                )

        state = next(item for item in obligations if item.key == "container:State")
        self.assertEqual("container_review_fields_present", state.status)

    def test_container_field_migrations_require_live_source_and_target(self) -> None:
        incoming = """const Options = struct {
    repair_ctx: ?*anyopaque = null,
    sequence: u64 = 0,
};
"""
        current = """const Options = struct {
    repair: RepairOptions = .{},
};
"""
        migrations = {
            "container:Options": {
                "repair_ctx": {
                    "target": "repair",
                    "reason": "repair activation moved into options",
                },
                "sequence": {
                    "target": "repair",
                    "reason": "sequence moved into options",
                },
            }
        }
        with tempfile.TemporaryDirectory(
            prefix="merge-audit-container-fields-",
            dir=audit.ROOT,
        ) as raw_dir:
            path = pathlib.Path(raw_dir) / "split.zig"
            path.write_text(current)

            with mock.patch.object(audit, "ref_text", return_value=incoming):
                obligations, _ = audit.analyze(
                    "monolith.zig",
                    "base",
                    "incoming",
                    [path],
                    0.7,
                    0.05,
                    include_unchanged=True,
                    container_field_migrations=migrations,
                )
                with self.assertRaisesRegex(
                    ValueError,
                    "missing current target field",
                ):
                    audit.analyze(
                        "monolith.zig",
                        "base",
                        "incoming",
                        [path],
                        0.7,
                        0.05,
                        include_unchanged=True,
                        container_field_migrations={
                            "container:Options": {
                                "sequence": {
                                    "target": "missing",
                                    "reason": "invalid target",
                                }
                            }
                        },
                    )

        self.assertEqual(
            ["container_review_fields_present"],
            [item.status for item in obligations],
        )
        self.assertIn("repair_ctx->repair", obligations[0].detail)

    def test_relative_imports_are_compared_by_resolved_repository_path(self) -> None:
        incoming = audit.Declaration(
            "binding",
            "db_mod",
            'const db_mod = @import("../storage/db/mod.zig");\n',
            0,
            "zig/pkg/antfly/src/api/table_writes.zig",
            0,
            52,
        )
        current = audit.Declaration(
            "binding",
            "db_mod",
            'const db_mod = @import("../../storage/db/mod.zig");\n',
            0,
            "zig/pkg/antfly/src/api/table_writes/cache.zig",
            0,
            55,
        )
        self.assertEqual(
            audit.normalized_import_binding(incoming),
            audit.normalized_import_binding(current),
        )
        facade_import = dataclasses.replace(
            incoming,
            name="backups_api",
            body='const backups_api = @import("backups.zig");\n',
        )
        split_import = dataclasses.replace(
            current,
            name="backups_api",
            body='const backups_api = @import("../backups.zig");\n',
        )
        self.assertEqual(
            audit.normalized_import_binding(facade_import),
            audit.normalized_import_binding(split_import),
        )

    def test_qualified_module_references_resolve_across_split_aliases(self) -> None:
        incoming = audit.Declaration(
            "function",
            "value",
            'fn value() u8 { return @import("tables.zig").default_value; }\n',
            0,
            "api/table_reads.zig",
            0,
            67,
        )
        current = audit.Declaration(
            "function",
            "value",
            "fn value() u8 { return tables_api.default_value; }\n",
            0,
            "api/table_reads/cache.zig",
            0,
            52,
        )
        self.assertEqual(
            audit.normalized_module_references(
                incoming,
                {},
            ),
            audit.normalized_module_references(
                current,
                {
                    (
                        "api/table_reads/cache.zig",
                        "tables_api",
                    ): "api/tables.zig",
                    (
                        "api/table_reads/cache.zig",
                        "tables",
                    ): "api/tables.zig",
                },
            ),
        )

    def test_module_reference_placeholders_do_not_overlap_at_ten_imports(self) -> None:
        declaration = audit.Declaration(
            "function",
            "value",
            "fn value() type { return alias_10.Value; }\n",
            0,
            "api/table_reads/cache.zig",
            0,
            43,
        )
        import_paths = {
            (declaration.path, f"alias_{index}"): f"api/module_{index}.zig"
            for index in range(12)
        }
        expected = dataclasses.replace(
            declaration,
            body='fn value() type { return @import("../module_10.zig").Value; }\n',
        )

        self.assertEqual(
            audit.normalized_module_references(expected, {}),
            audit.normalized_module_references(declaration, import_paths),
        )

    def test_split_visibility_required_for_unique_cross_file_method_call(self) -> None:
        private = audit.Declaration(
            "function",
            "reserve",
            "    fn reserve(self: *Cache) !void {}\n",
            4,
            "api/writes/cache.zig",
            0,
            39,
            owner="Cache",
        )
        texts = {
            private.path: private.body,
            "api/writes/sources.zig": "fn clear(cache: *Cache) !void { try cache.reserve(); }\n",
        }
        self.assertTrue(
            audit.split_visibility_required(private, [private], texts)
        )
        public = dataclasses.replace(
            private,
            body="    pub fn reserve(self: *Cache) !void {}\n",
        )
        self.assertFalse(
            audit.split_visibility_required(public, [public], texts)
        )
        duplicate = dataclasses.replace(
            private,
            path="api/writes/other.zig",
        )
        self.assertFalse(
            audit.split_visibility_required(
                private,
                [private, duplicate],
                texts,
            )
        )
        unrelated = {
            private.path: private.body,
            "api/writes/sources.zig": "fn release(mutex: *Mutex) void { mutex.reserve(); }\n",
        }
        self.assertFalse(
            audit.split_visibility_required(private, [private], unrelated)
        )

    def test_split_visibility_required_for_imported_top_level_function(self) -> None:
        private = audit.Declaration(
            "function",
            "existingPrimaryBackend",
            "fn existingPrimaryBackend() Backend { return .lsm; }\n",
            0,
            "api/table_writes/managed_db.zig",
            0,
            58,
        )
        texts = {
            private.path: private.body,
            "api/table_writes/sources.zig": (
                'const managed_db = @import("managed_db.zig");\n'
                "const existingPrimaryBackend = "
                "managed_db.existingPrimaryBackend;\n"
            ),
        }
        self.assertTrue(
            audit.split_visibility_required(private, [private], texts)
        )

        public = dataclasses.replace(
            private,
            body="pub fn existingPrimaryBackend() Backend { return .lsm; }\n",
        )
        self.assertFalse(
            audit.split_visibility_required(public, [public], texts)
        )

        unrelated = {
            private.path: private.body,
            "api/table_writes/sources.zig": (
                'const managed_db = @import("../other/managed_db.zig");\n'
                "const existingPrimaryBackend = "
                "managed_db.existingPrimaryBackend;\n"
            ),
        }
        self.assertFalse(
            audit.split_visibility_required(private, [private], unrelated)
        )

    def test_symbol_call_migrations_canonicalize_only_calls(self) -> None:
        incoming = audit.Declaration(
            "function",
            "value",
            "fn value() void { parseValue(input); }\n",
            0,
            "api/table_reads.zig",
            0,
            39,
        )
        current = dataclasses.replace(
            incoming,
            body="fn value() void { json_helpers.parseValue(input); }\n",
            path="api/table_reads/fanout.zig",
        )
        migrations = {"parseValue": ["json_helpers.parseValue"]}
        self.assertEqual(
            audit.normalized_module_references(
                incoming,
                {},
                symbol_call_migrations=migrations,
            ),
            audit.normalized_module_references(
                current,
                {},
                symbol_call_migrations=migrations,
            ),
        )
        self.assertEqual(
            audit.normalized_module_references(
                incoming,
                {},
                symbol_call_migrations=migrations,
            ),
            audit.normalized_module_references(
                current,
                {
                    (
                        "api/table_reads/fanout.zig",
                        "json_helpers",
                    ): "api/json_helpers.zig"
                },
                symbol_call_migrations=migrations,
            ),
        )
        changed_non_call = dataclasses.replace(
            current,
            body="fn value() void { consume(json_helpers.parseValue); }\n",
        )
        self.assertNotEqual(
            audit.normalized_module_references(
                incoming,
                {},
                symbol_call_migrations=migrations,
            ),
            audit.normalized_module_references(
                changed_non_call,
                {},
                symbol_call_migrations=migrations,
            ),
        )

    def test_module_references_canonicalize_generic_self_dispatch(self) -> None:
        monolith = audit.Declaration(
            "function",
            "run",
            "fn run(self: *DB) void { self.method(1); helper(2); }\n",
            0,
            "monolith.zig",
            0,
            0,
        )
        split = dataclasses.replace(
            monolith,
            body=(
                "fn run(self: *DB) void { "
                "Self.method(self, 1); Self.helper(2); }\n"
            ),
            path="split.zig",
        )

        self.assertEqual(
            audit.normalized_module_references(monolith, {}),
            audit.normalized_module_references(split, {}),
        )

    def test_symbol_reference_migrations_canonicalize_exact_tokens(self) -> None:
        incoming = audit.Declaration(
            "binding",
            "options",
            "const options = .{ .limit = legacy_limit };\n",
            0,
            "api/table_writes.zig",
            0,
            45,
        )
        current = dataclasses.replace(
            incoming,
            body="const options = .{ .limit = owner_limit };\n",
            path="api/table_writes/bulk.zig",
        )
        migrations = {"legacy_limit": ["owner_limit"]}
        self.assertEqual(
            audit.normalized_module_references(
                incoming,
                {},
                symbol_reference_migrations=migrations,
            ),
            audit.normalized_module_references(
                current,
                {},
                symbol_reference_migrations=migrations,
            ),
        )
        embedded = dataclasses.replace(current, body="const options = owner_limits;\n")
        self.assertNotEqual(
            audit.normalized_module_references(
                incoming,
                {},
                symbol_reference_migrations=migrations,
            ),
            audit.normalized_module_references(
                embedded,
                {},
                symbol_reference_migrations=migrations,
            ),
        )

    def test_container_fields_normalize_split_module_owners(self) -> None:
        incoming = audit.Declaration(
            "container",
            "Source",
            "pub const Source = struct {\n    cache: ?*reads.Cache = null,\n};\n",
            0,
            "api/table_writes.zig",
            0,
            64,
        )
        current = dataclasses.replace(
            incoming,
            body=(
                "pub const Source = struct {\n"
                "    cache: ?*read_cache.Cache = null,\n"
                "};\n"
            ),
            path="api/table_writes/sources.zig",
        )
        migrations = {"api/table_reads.zig": ["api/table_reads/cache.zig"]}
        self.assertEqual(
            audit.direct_container_field_lines(
                incoming,
                {("api/table_writes.zig", "reads"): "api/table_reads.zig"},
                migrations,
            ),
            audit.direct_container_field_lines(
                current,
                {
                    (
                        "api/table_writes/sources.zig",
                        "read_cache",
                    ): "api/table_reads/cache.zig"
                },
                migrations,
            ),
        )

    def test_split_binding_alias_resolves_to_equivalent_owner_definition(self) -> None:
        incoming = audit.Declaration(
            "binding",
            "auto_bulk_ingest_max_window_ops",
            "const auto_bulk_ingest_max_window_ops: usize = 25_000;\n",
            0,
            "api/table_writes.zig",
            0,
            55,
        )
        target = audit.Declaration(
            "binding",
            "max_window_ops",
            "pub const max_window_ops: usize = 25_000;\n",
            0,
            "api/table_writes/bulk_ingest.zig",
            0,
            44,
        )
        alias = audit.Declaration(
            "binding",
            "auto_bulk_ingest_max_window_ops",
            "const auto_bulk_ingest_max_window_ops = "
            "table_write_bulk_ingest.max_window_ops;\n",
            0,
            "api/table_writes/cache.zig",
            0,
            86,
        )
        self.assertEqual(
            ("table_write_bulk_ingest", "max_window_ops"),
            audit.qualified_binding_alias(alias),
        )
        self.assertEqual(
            audit.normalized_binding_definition(incoming),
            audit.normalized_binding_definition(target),
        )

    def test_review_bodies_are_opt_in_obligation_fields(self) -> None:
        obligation = audit.Obligation(
            key="function:value",
            kind="function",
            name="value",
            change="modified",
            status="three_way_conflict",
            base_sha256="base",
            incoming_sha256="incoming",
            base_body="fn value() u8 { return 1; }",
            incoming_body="fn value() u8 { return 2; }",
            current_body="fn value() u8 { return 3; }",
        )
        serialized = dataclasses.asdict(obligation)
        self.assertEqual(
            "fn value() u8 { return 2; }",
            serialized["incoming_body"],
        )

    def test_zig_multiline_strings_do_not_change_brace_depth(self) -> None:
        text = r'''
test "multiline json remains one declaration" {
    const schema =
        \\{"outer":{"inner":true}}
        \\}
    ;
    try consume(schema);
}

test "following test is still discovered" {
    /* nested /* } */ block comment */
    try consume("{");
}
'''
        tests = [
            item
            for item in audit.all_declarations(text, "sample.zig")
            if item.kind == "test"
        ]
        self.assertEqual(
            [
                "multiline json remains one declaration",
                "following test is still discovered",
            ],
            [item.name for item in tests],
        )
        self.assertIn("try consume(schema);", tests[0].body)
        self.assertIn('try consume("{");', tests[1].body)

    def test_nested_declarations_are_not_double_counted(self) -> None:
        text = """
const Owner = struct {
    fn method() void {
        const MethodLocal = struct {
            fn nested() void {}
        };
    }
};

fn production() void {
    const Local = struct {
        fn nested() void {}
    };
}

test "owns nested declarations" {
    const Local = struct {
        fn nested() void {}
    };
}
"""
        declarations = audit.all_declarations(text, "sample.zig")
        self.assertEqual(
            [
                ("container", "Owner"),
                ("function", "method"),
                ("function", "production"),
                ("test", "owns nested declarations"),
            ],
            sorted((item.kind, item.name) for item in declarations),
        )
        owners = {(item.kind, item.name): item.owner for item in declarations}
        self.assertEqual("Owner", owners[("function", "method")])
        self.assertIsNone(owners[("container", "Owner")])
        self.assertIsNone(owners[("function", "production")])

    def test_owned_methods_do_not_fall_back_to_same_name_in_another_owner(self) -> None:
        incoming = """
const Cache = struct {
    fn lease() void {}
};
"""
        current = """
const Source = struct {
    fn lease() void {}
};
"""
        incoming_declarations = audit.all_declarations(incoming, "incoming.zig")
        current_declarations = audit.all_declarations(current, "current.zig")
        incoming_method = next(
            item for item in incoming_declarations if item.kind == "function"
        )
        same_name = [
            item
            for item in current_declarations
            if item.kind == incoming_method.kind
            and item.name == incoming_method.name
        ]
        self.assertEqual(
            [],
            audit.matching_owner_candidates(incoming_method, same_name),
        )
        self.assertEqual(
            same_name,
            audit.matching_owner_candidates(
                incoming_method,
                same_name,
                {
                    "owner": "Source",
                    "path": "current.zig",
                },
            ),
        )

    def test_three_way_merge_ignores_move_only_indentation(self) -> None:
        base = audit.Declaration(
            "function",
            "value",
            "    fn value() u8 {\n        return 1;\n    }\n",
            4,
            "base.zig",
            0,
            46,
        )
        incoming = audit.Declaration(
            "function",
            "value",
            "    fn value() u8 {\n        const result: u8 = 2;\n        return result;\n    }\n",
            4,
            "incoming.zig",
            0,
            80,
        )
        current = audit.Declaration(
            "function",
            "value",
            "        fn value() u8 {\n            return 1;\n        }\n",
            8,
            "split.zig",
            0,
            54,
        )
        status, merged = audit.merge_declarations(current, base, incoming)
        self.assertEqual(0, status)
        self.assertIn("        fn value()", merged)
        self.assertIn("            const result: u8 = 2;", merged)

    def test_three_way_merge_canonicalizes_declared_split_calls(self) -> None:
        base = """fn value() u8 {
    legacy();
    return 1;
}
"""
        incoming = """fn value() u8 {
    incomingFix();
    legacy();
    return 1;
}
"""
        current = """pub fn value() u8 {
    incomingFix();
    branchFeature();
    replacement.legacy();
    return 1;
}
"""
        with tempfile.TemporaryDirectory(
            prefix="merge-audit-canonical-merge-",
            dir=audit.ROOT,
        ) as raw_dir:
            path = pathlib.Path(raw_dir) / "split.zig"
            path.write_text(current)

            def ref_text(ref: str, _: str) -> str:
                return base if ref == "base" else incoming

            with mock.patch.object(audit, "ref_text", side_effect=ref_text):
                obligations, _ = audit.analyze(
                    "monolith.zig",
                    "base",
                    "incoming",
                    [path],
                    0.7,
                    0.05,
                    symbol_call_migrations={
                        "legacy": ["replacement.legacy"],
                    },
                )

        self.assertEqual(["integrated"], [item.status for item in obligations])
        self.assertIn("manifest-declared", obligations[0].detail)

    def test_canonicalized_merge_rejects_retained_replaced_tokens(self) -> None:
        base = audit.Declaration(
            "function",
            "value",
            "fn\nvalue\n(\n)\nu8\n{\nreturn\noldBehavior\n(\n)\n;\n}\n",
            0,
            "base.zig",
            0,
            0,
        )
        incoming = dataclasses.replace(
            base,
            body="fn\nvalue\n(\n)\nu8\n{\nreturn\nnewBehavior\n(\n)\n;\n}\n",
            path="incoming.zig",
        )
        current = dataclasses.replace(
            base,
            body=(
                "fn\nvalue\n(\n)\nu8\n{\noldBehavior\n(\n)\n;\n"
                "return\nnewBehavior\n(\n)\n;\n}\n"
            ),
            path="current.zig",
        )

        self.assertFalse(
            audit.canonicalized_incoming_is_preserved(current, base, incoming)
        )

    def test_missing_candidate_reindent_preserves_relative_layout(self) -> None:
        body = "fn inserted() void {\n    run();\n}\n"
        self.assertEqual(
            "    fn inserted() void {\n        run();\n    }\n",
            audit.reindent_declaration(body, 4),
        )

    def test_candidate_edits_can_be_selected_by_exact_key(self) -> None:
        edits = {
            "one.zig": [
                audit.CandidateEdit("test:one", "a", "b", 1),
                audit.CandidateEdit("test:two", "c", "d", 2),
            ],
            "two.zig": [
                audit.CandidateEdit("test:three", "e", "f", 3),
            ],
        }
        self.assertEqual(
            {
                "one.zig": [
                    audit.CandidateEdit("test:two", "c", "d", 2),
                ]
            },
            audit.select_candidate_edits(edits, ["test:two"]),
        )
        with self.assertRaises(ValueError):
            audit.select_candidate_edits(edits, ["test:missing"])
        self.assertEqual(
            {
                "one.zig": [
                    audit.CandidateEdit("test:one", "a", "b", 1),
                ],
                "two.zig": [
                    audit.CandidateEdit("test:three", "e", "f", 3),
                ],
            },
            audit.select_candidate_edits(edits, [], ["test:two"]),
        )
        with self.assertRaisesRegex(ValueError, "unknown.*excluded"):
            audit.select_candidate_edits(edits, [], ["test:missing"])
        with self.assertRaisesRegex(ValueError, "both selected and excluded"):
            audit.select_candidate_edits(edits, ["test:two"], ["test:two"])

    def test_reviewed_incoming_replacement_reindents_whole_declaration(self) -> None:
        incoming = "    fn value() void {\n        changed();\n    }\n"
        self.assertEqual(
            "        fn value() void {\n            changed();\n        }\n",
            audit.reindent_declaration(incoming, 8),
        )

    def test_split_migration_is_resolved_from_policy(self) -> None:
        source, destinations = audit.split_migration(
            {
                "split_migrations": {
                    "writes": {
                        "source": "api/table_writes.zig",
                        "destinations": [
                            "api/table_writes.zig",
                            "api/table_writes",
                        ],
                    }
                }
            },
            "writes",
        )
        self.assertEqual("api/table_writes.zig", source)
        self.assertEqual(
            ["api/table_writes.zig", "api/table_writes"],
            destinations,
        )

    def test_split_migration_rejects_unknown_or_invalid_entries(self) -> None:
        with self.assertRaisesRegex(ValueError, "unknown split migration"):
            audit.split_migration({"split_migrations": {}}, "missing")
        with self.assertRaisesRegex(ValueError, "requires a source string"):
            audit.split_migration(
                {"split_migrations": {"writes": {"destinations": []}}},
                "writes",
            )

    def test_split_declaration_placements_are_manifest_driven(self) -> None:
        policy = {
            "split_migrations": {
                "writes": {
                    "source": "api/table_writes.zig",
                    "destinations": ["api/table_writes"],
                    "declaration_placements": {
                        "test:writer lifecycle": "api/table_writes/sources.zig",
                    },
                }
            }
        }
        self.assertEqual(
            {
                "test:writer lifecycle": "api/table_writes/sources.zig",
            },
            audit.split_declaration_placements(policy, "writes"),
        )
        self.assertEqual({}, audit.split_declaration_placements(policy, None))

    def test_split_declaration_placement_ranges_are_manifest_driven(self) -> None:
        placement_range = {
            "start": "container:DB.RepairStart",
            "end": "function:DB.finishRepair",
            "path": "storage/db/artifact_repair.zig",
            "reason": "The durable repair lineage moved into the repair owner.",
        }
        policy = {
            "split_migrations": {
                "db": {
                    "source": "storage/db/db.zig",
                    "destinations": ["storage/db"],
                    "declaration_placement_ranges": [placement_range],
                }
            }
        }
        self.assertEqual(
            [placement_range],
            audit.split_declaration_placement_ranges(policy, "db"),
        )
        self.assertEqual([], audit.split_declaration_placement_ranges(policy, None))

        invalid = {
            "split_migrations": {
                "db": {
                    "declaration_placement_ranges": [{"start": "x"}],
                }
            }
        }
        with self.assertRaisesRegex(ValueError, "exact start, end, path"):
            audit.split_declaration_placement_ranges(invalid, "db")

    def test_split_declaration_owner_migrations_are_manifest_driven(self) -> None:
        policy = {
            "split_migrations": {
                "writes": {
                    "source": "api/table_writes.zig",
                    "destinations": ["api/table_writes"],
                    "declaration_owner_migrations": {
                        "function:Source.helper": {
                            "owner": None,
                            "path": "api/table_writes/helpers.zig",
                        },
                    },
                },
            },
        }
        self.assertEqual(
            {
                "function:Source.helper": {
                    "owner": None,
                    "path": "api/table_writes/helpers.zig",
                },
            },
            audit.split_declaration_owner_migrations(policy, "writes"),
        )

    def test_renamed_owner_migration_seeds_candidate_lookup(self) -> None:
        base = "fn oldName() u8 {\n    return 1;\n}\n"
        incoming = "fn oldName() u8 {\n    return 2;\n}\n"
        current = "fn splitName() u8 {\n    return 1;\n}\n"
        with tempfile.TemporaryDirectory(
            prefix="merge-audit-owner-rename-",
            dir=audit.ROOT,
        ) as raw_dir:
            path = pathlib.Path(raw_dir) / "split.zig"
            path.write_text(current)
            relative = str(path.relative_to(audit.ROOT))

            def ref_text(ref: str, _: str) -> str:
                return base if ref == "base" else incoming

            with mock.patch.object(audit, "ref_text", side_effect=ref_text):
                obligations, _ = audit.analyze(
                    "db.zig",
                    "base",
                    "incoming",
                    [path],
                    0.7,
                    0.05,
                    declaration_owner_migrations={
                        "function:oldName": {
                            "owner": None,
                            "path": relative,
                            "name": "splitName",
                        },
                    },
                )
        self.assertEqual(1, len(obligations))
        self.assertEqual("clean_candidate", obligations[0].status)
        self.assertEqual(relative, obligations[0].current_path)

    def test_generic_container_candidate_is_not_duplicated(self) -> None:
        base = "const State = struct {\n    value: u8 = 1,\n};\n"
        incoming = "const State = struct {\n    value: u8 = 2,\n};\n"
        current = (
            "fn State(comptime T: type) type {\n"
            "    _ = T;\n"
            "    return struct {\n"
            "        value: u8 = 2,\n"
            "    };\n"
            "}\n"
        )
        with tempfile.TemporaryDirectory(
            prefix="merge-audit-generic-container-",
            dir=audit.ROOT,
        ) as raw_dir:
            path = pathlib.Path(raw_dir) / "split.zig"
            path.write_text(current)

            def ref_text(ref: str, _: str) -> str:
                return base if ref == "base" else incoming

            with mock.patch.object(audit, "ref_text", side_effect=ref_text):
                obligations, _ = audit.analyze(
                    "db.zig",
                    "base",
                    "incoming",
                    [path],
                    0.7,
                    0.05,
                )

        self.assertEqual(1, len(obligations))
        self.assertNotEqual("ambiguous_destination", obligations[0].status)
        self.assertIsNone(obligations[0].second_similarity)

    def test_manifest_mixins_restore_only_absent_owner_declarations(self) -> None:
        text = """
pub fn Impl(comptime DB: type) type {
    return struct {
        const marker = 1;

        fn privateWork(self: *DB) void {
            _ = self;
        }

        pub fn publicWork(self: *DB) void {
            _ = self;
        }
    };
}
"""
        extracted = audit.mixin_declarations(
            text,
            "split.zig",
            "Impl",
            "DB",
        )
        self.assertEqual(
            {
                ("binding", "marker", "DB"),
                ("function", "privateWork", "DB"),
                ("function", "publicWork", "DB"),
            },
            {(item.kind, item.name, item.owner) for item in extracted},
        )
        wrapper = audit.Declaration(
            "function",
            "publicWork",
            "pub fn publicWork(self: *DB) void {}\n",
            4,
            "db.zig",
            0,
            42,
            "DB",
        )
        augmented = audit.augment_with_manifest_mixins(
            [wrapper],
            {"split.zig": text},
            [{"path": "split.zig", "factory": "Impl", "owner": "DB"}],
        )
        self.assertEqual(
            1,
            sum(
                item.name == "publicWork" and item.owner == "DB"
                for item in augmented
            ),
        )
        self.assertEqual(
            1,
            sum(
                item.name == "privateWork" and item.owner == "DB"
                for item in augmented
            ),
        )
        forced = audit.augment_with_manifest_mixins(
            [wrapper],
            {"split.zig": text},
            [{"path": "split.zig", "factory": "Impl", "owner": "DB"}],
            {("split.zig", "function", "publicWork", "DB")},
        )
        self.assertEqual(
            2,
            sum(
                item.name == "publicWork" and item.owner == "DB"
                for item in forced
            ),
        )
        self.assertEqual(
            {"db.zig", "split.zig"},
            {
                item.path
                for item in forced
                if item.name == "publicWork" and item.owner == "DB"
            },
        )

    def test_split_declaration_mixins_are_manifest_driven(self) -> None:
        policy = {
            "split_migrations": {
                "db": {
                    "source": "db.zig",
                    "destinations": ["db"],
                    "declaration_mixins": [
                        {
                            "path": "db/lifecycle.zig",
                            "factory": "Impl",
                            "owner": "DB",
                        },
                    ],
                },
            },
        }
        self.assertEqual(
            policy["split_migrations"]["db"]["declaration_mixins"],
            audit.split_declaration_mixins(policy, "db"),
        )

    def test_candidate_writer_uses_spans_for_duplicate_declaration_text(self) -> None:
        relative = "duplicate-candidate-test.zig"
        source = audit.ROOT / relative
        source.write_text("fn same() void {}\nfn same() void {}\n")
        self.addCleanup(source.unlink)
        with tempfile.TemporaryDirectory() as raw:
            output = pathlib.Path(raw) / "candidates"
            edits = {
                relative: [
                    audit.CandidateEdit(
                        "function:Owner.same",
                        "fn same() void {}",
                        "fn same() void { changed(); }",
                        1,
                        start=18,
                        end=35,
                    )
                ]
            }
            audit.write_candidates(output, edits, [], {})
            self.assertEqual(
                "fn same() void {}\nfn same() void { changed(); }\n",
                (output / relative).read_text(),
            )

    def test_candidate_writer_composes_insertions_at_shared_anchor(self) -> None:
        relative = "shared-anchor-candidate-test.zig"
        source = audit.ROOT / relative
        source.write_text("fn anchor() void {}\n")
        self.addCleanup(source.unlink)
        with tempfile.TemporaryDirectory() as raw:
            output = pathlib.Path(raw) / "candidates"
            edits = {
                relative: [
                    audit.CandidateEdit(
                        "function:first",
                        "fn anchor() void {}",
                        "fn first() void {}",
                        1,
                        start=0,
                        end=19,
                        insert_before=True,
                    ),
                    audit.CandidateEdit(
                        "function:second",
                        "fn anchor() void {}",
                        "fn second() void {}",
                        2,
                        start=0,
                        end=19,
                        insert_before=True,
                    ),
                ]
            }
            audit.write_candidates(output, edits, [], {})
            self.assertEqual(
                "fn first() void {}\nfn second() void {}\nfn anchor() void {}\n",
                (output / relative).read_text(),
            )

    def test_container_declaration_span_includes_semicolon(self) -> None:
        declarations = audit.extract_declarations(
            "const Mode = enum {\n    one,\n};\n",
            "mode.zig",
            "container",
            audit.CONTAINER_RE,
            None,
        )
        self.assertEqual(1, len(declarations))
        self.assertEqual(
            "const Mode = enum {\n    one,\n};\n",
            declarations[0].body,
        )

    def test_bindings_include_imports_scalars_and_container_constants(self) -> None:
        declarations = audit.all_declarations(
            'const std = @import("std");\n'
            "const timeout_ns: u64 = 5;\n"
            "const Owner = struct {\n"
            "    const State = enum { idle };\n"
            "    var hook: ?*anyopaque = null;\n"
            "    field: u64,\n"
            "};\n",
            "bindings.zig",
            max_indent=4,
        )
        bindings = {
            (item.name, item.owner): item.body
            for item in declarations
            if item.kind == "binding"
        }
        self.assertIn(("std", None), bindings)
        self.assertIn(("timeout_ns", None), bindings)
        self.assertIn(("hook", "Owner"), bindings)
        self.assertNotIn(("field", "Owner"), bindings)

    def test_binding_semicolon_scanner_ignores_literals_and_initializers(self) -> None:
        text = (
            'const value = .{ .message = ";", .nested = .{ 1, 2 } };\n'
            "const next = 3;\n"
        )
        bindings = audit.extract_bindings(text, "bindings.zig", None)
        self.assertEqual(2, len(bindings))
        self.assertEqual(
            'const value = .{ .message = ";", .nested = .{ 1, 2 } };\n',
            bindings[0].body,
        )

    def test_function_body_scanner_skips_struct_literal_in_return_type(self) -> None:
        text = (
            "fn existing() @TypeOf((Options{}).backend) {\n"
            "    return .{};\n"
            "}\n"
            "fn next() void {}\n"
        )
        functions = audit.extract_declarations(
            text,
            "functions.zig",
            "function",
            audit.FUNCTION_RE,
            None,
        )
        self.assertEqual(2, len(functions))
        self.assertEqual(
            "fn existing() @TypeOf((Options{}).backend) {\n"
            "    return .{};\n"
            "}\n",
            functions[0].body,
        )

    def test_function_body_scanner_skips_anonymous_return_container(self) -> None:
        text = (
            "fn load() !?struct { indexes: ?[]u8, schema: ?[]u8 } {\n"
            "    const indexes = try readIndexes();\n"
            "    return .{ .indexes = indexes, .schema = null };\n"
            "}\n"
            "const module_binding = 1;\n"
        )
        declarations = audit.all_declarations(text, "functions.zig")
        functions = [item for item in declarations if item.kind == "function"]
        bindings = [item for item in declarations if item.kind == "binding"]
        self.assertEqual(["load"], [item.name for item in functions])
        self.assertEqual(["module_binding"], [item.name for item in bindings])
        self.assertIn("const indexes = try readIndexes();", functions[0].body)

    def test_full_baseline_pairing_keeps_unchanged_declarations(self) -> None:
        declaration = audit.Declaration(
            "function",
            "carried",
            "fn carried() void {}\n",
            0,
            "sample.zig",
            0,
            21,
        )
        self.assertEqual(
            [],
            audit.pair_declarations(
                [declaration],
                [declaration],
                include_unchanged=False,
            ),
        )
        self.assertEqual(
            [(declaration, declaration, 1)],
            audit.pair_declarations(
                [declaration],
                [declaration],
                include_unchanged=True,
            ),
        )

    def test_removed_declarations_preserve_duplicate_ordinals(self) -> None:
        first = audit.Declaration(
            "test",
            "same",
            'test "same" { first(); }\n',
            0,
            "sample.zig",
            0,
            25,
        )
        second = dataclasses.replace(
            first,
            body='test "same" { second(); }\n',
            start=26,
            end=52,
        )
        incoming = dataclasses.replace(first, path="incoming.zig")

        self.assertEqual(
            [(second, 2)],
            audit.removed_declarations([first, second], [incoming]),
        )

    def test_deleted_declarations_are_strict_split_obligations(self) -> None:
        base = (
            "fn removed() void { baseline(); }\n"
            "fn kept() void {}\n"
        )
        incoming = "fn kept() void {}\n"
        cases = {
            "absent": ("fn kept() void {}\n", "deleted_absent"),
            "baseline": (
                "fn removed() void { baseline(); }\nfn kept() void {}\n",
                "deleted_still_present",
            ),
            "branch": (
                "fn removed() void { branch(); }\nfn kept() void {}\n",
                "deleted_branch_changed",
            ),
        }
        for name, (current, expected_status) in cases.items():
            with self.subTest(name=name), tempfile.TemporaryDirectory(
                prefix="merge-audit-deletion-",
                dir=audit.ROOT,
            ) as raw_dir:
                path = pathlib.Path(raw_dir) / "split.zig"
                path.write_text(current)

                def ref_text(ref: str, _: str) -> str:
                    return base if ref == "base" else incoming

                with mock.patch.object(audit, "ref_text", side_effect=ref_text):
                    obligations, replacements = audit.analyze(
                        "monolith.zig",
                        "base",
                        "incoming",
                        [path],
                        0.7,
                        0.05,
                    )

                removed = next(
                    item
                    for item in obligations
                    if item.key == "function:removed"
                )
                self.assertEqual("deleted", removed.change)
                self.assertEqual(expected_status, removed.status)
                self.assertIsNone(removed.incoming_sha256)
                self.assertEqual(1, removed.base_line)
                self.assertEqual({}, replacements)

    def test_retained_deletion_requires_exact_live_obligation(self) -> None:
        base = "fn removed() void { baseline(); }\n"
        incoming = ""
        current = "fn removed() void { branch(); }\n"
        with tempfile.TemporaryDirectory(
            prefix="merge-audit-retained-deletion-",
            dir=audit.ROOT,
        ) as raw_dir:
            path = pathlib.Path(raw_dir) / "split.zig"
            path.write_text(current)
            relative = str(path.relative_to(audit.ROOT))
            retention = {
                "base_sha256": audit.sha256_text(base),
                "current_sha256": audit.sha256_text(current),
                "path": relative,
                "reason": "branch API still depends on it",
            }

            def ref_text(ref: str, _: str) -> str:
                return base if ref == "base" else incoming

            with mock.patch.object(audit, "ref_text", side_effect=ref_text):
                obligations, _ = audit.analyze(
                    "monolith.zig",
                    "base",
                    "incoming",
                    [path],
                    0.7,
                    0.05,
                    retained_deletions={
                        "function:removed": retention,
                    },
                )
                with self.assertRaisesRegex(
                    ValueError,
                    "do not identify uniquely retained",
                ):
                    audit.analyze(
                        "monolith.zig",
                        "base",
                        "incoming",
                        [path],
                        0.7,
                        0.05,
                        retained_deletions={
                            "function:missing": retention,
                        },
                    )

                with self.assertRaisesRegex(ValueError, "stale current_sha256"):
                    audit.analyze(
                        "monolith.zig",
                        "base",
                        "incoming",
                        [path],
                        0.7,
                        0.05,
                        retained_deletions={
                            "function:removed": {
                                **retention,
                                "current_sha256": "0" * 64,
                            },
                        },
                    )

        removed = next(
            item for item in obligations if item.key == "function:removed"
        )
        self.assertEqual("deleted_retained_reviewed", removed.status)
        self.assertIn("branch API still depends on it", removed.detail)

    def test_reviewed_composition_requires_exact_live_three_way_conflict(self) -> None:
        base = "fn value() u8 { return 1; }\n"
        incoming = "fn value() u8 { return 2; }\n"
        current = "fn value() u8 { return 3; }\n"
        with tempfile.TemporaryDirectory(
            prefix="merge-audit-reviewed-composition-",
            dir=audit.ROOT,
        ) as raw_dir:
            path = pathlib.Path(raw_dir) / "split.zig"
            path.write_text(current)
            relative = str(path.relative_to(audit.ROOT))
            review = {
                "base_sha256": audit.sha256_text(base),
                "incoming_sha256": audit.sha256_text(incoming),
                "current_sha256": audit.sha256_text(current),
                "path": relative,
                "reason": "split graph composes both sides",
            }

            def ref_text(ref: str, _: str) -> str:
                return base if ref == "base" else incoming

            with mock.patch.object(audit, "ref_text", side_effect=ref_text):
                obligations, _ = audit.analyze(
                    "monolith.zig",
                    "base",
                    "incoming",
                    [path],
                    0.7,
                    0.05,
                    reviewed_compositions={"function:value": review},
                )
                with self.assertRaisesRegex(
                    ValueError,
                    "stale current_sha256",
                ):
                    audit.analyze(
                        "monolith.zig",
                        "base",
                        "incoming",
                        [path],
                        0.7,
                        0.05,
                        reviewed_compositions={
                            "function:value": {
                                **review,
                                "current_sha256": "0" * 64,
                            }
                        },
                    )
                with self.assertRaisesRegex(
                    ValueError,
                    "do not identify live three-way conflicts",
                ):
                    audit.analyze(
                        "monolith.zig",
                        "base",
                        "incoming",
                        [path],
                        0.7,
                        0.05,
                        reviewed_compositions={"function:missing": review},
                    )

        obligation = next(
            item for item in obligations if item.key == "function:value"
        )
        self.assertEqual("three_way_composition_reviewed", obligation.status)
        self.assertIn("split graph composes both sides", obligation.detail)

    def test_reviewed_resolution_requires_exact_live_semantic_conflict(self) -> None:
        base = "fn value() u8 { return baseline(); }\n"
        incoming = "fn value() u8 { return incoming(); }\n"
        current = "fn value() u8 { return composed(); }\n"
        with tempfile.TemporaryDirectory(
            prefix="merge-audit-reviewed-resolution-",
            dir=audit.ROOT,
        ) as raw_dir:
            path = pathlib.Path(raw_dir) / "split.zig"
            path.write_text(current)
            relative = str(path.relative_to(audit.ROOT))
            review = {
                "status": "three_way_conflict",
                "base_sha256": audit.sha256_text(base),
                "incoming_sha256": audit.sha256_text(incoming),
                "current_sha256": audit.sha256_text(current),
                "path": relative,
                "reason": "incoming durability and branch routing are composed",
            }

            def ref_text(ref: str, _: str) -> str:
                return base if ref == "base" else incoming

            with mock.patch.object(audit, "ref_text", side_effect=ref_text):
                obligations, _ = audit.analyze(
                    "monolith.zig",
                    "base",
                    "incoming",
                    [path],
                    0.7,
                    0.05,
                    reviewed_resolutions={"function:value": review},
                )
                with self.assertRaisesRegex(ValueError, "stale status"):
                    audit.analyze(
                        "monolith.zig",
                        "base",
                        "incoming",
                        [path],
                        0.7,
                        0.05,
                        reviewed_resolutions={
                            "function:value": {
                                **review,
                                "status": "added_name_collision",
                            }
                        },
                    )
                with self.assertRaisesRegex(
                    ValueError,
                    "do not identify live reviewable semantic conflicts",
                ):
                    audit.analyze(
                        "monolith.zig",
                        "base",
                        "incoming",
                        [path],
                        0.7,
                        0.05,
                        reviewed_resolutions={"function:missing": review},
                    )

        obligation = next(
            item for item in obligations if item.key == "function:value"
        )
        self.assertEqual("semantic_resolution_reviewed", obligation.status)
        self.assertIn("incoming durability", obligation.detail)

    def test_reviewed_resolution_accepts_null_base_for_added_collision(self) -> None:
        base = ""
        incoming = "fn value() u8 { return incoming(); }\n"
        current = "fn value() u8 { return branch(); }\n"
        with tempfile.TemporaryDirectory(
            prefix="merge-audit-reviewed-added-",
            dir=audit.ROOT,
        ) as raw_dir:
            path = pathlib.Path(raw_dir) / "split.zig"
            path.write_text(current)
            relative = str(path.relative_to(audit.ROOT))
            review = {
                "status": "added_name_collision",
                "base_sha256": None,
                "incoming_sha256": audit.sha256_text(incoming),
                "current_sha256": audit.sha256_text(current),
                "path": relative,
                "reason": "branch helper is an incoming superset",
            }

            def ref_text(ref: str, _: str) -> str:
                return base if ref == "base" else incoming

            with mock.patch.object(audit, "ref_text", side_effect=ref_text):
                obligations, _ = audit.analyze(
                    "monolith.zig",
                    "base",
                    "incoming",
                    [path],
                    0.7,
                    0.05,
                    reviewed_resolutions={"function:value": review},
                )

        obligation = next(
            item for item in obligations if item.key == "function:value"
        )
        self.assertEqual("semantic_resolution_reviewed", obligation.status)

    def test_reviewed_resolution_accepts_clean_candidate_without_emitting_edit(self) -> None:
        base = "fn value() u8 {\n    const first = 1;\n    const padding_a = 0;\n    const padding_b = 0;\n    const padding_c = 0;\n    const second = 2;\n    return first + second + padding_a + padding_b + padding_c;\n}\n"
        incoming = "fn value() u8 {\n    const first = 3;\n    const padding_a = 0;\n    const padding_b = 0;\n    const padding_c = 0;\n    const second = 2;\n    return first + second + padding_a + padding_b + padding_c;\n}\n"
        current = "fn value() u8 {\n    const first = 1;\n    const padding_a = 0;\n    const padding_b = 0;\n    const padding_c = 0;\n    const second = 4;\n    return first + second + padding_a + padding_b + padding_c;\n}\n"
        with tempfile.TemporaryDirectory(
            prefix="merge-audit-reviewed-clean-",
            dir=audit.ROOT,
        ) as raw_dir:
            path = pathlib.Path(raw_dir) / "split.zig"
            path.write_text(current)
            relative = str(path.relative_to(audit.ROOT))
            review = {
                "status": "clean_candidate",
                "base_sha256": audit.sha256_text(base),
                "incoming_sha256": audit.sha256_text(incoming),
                "current_sha256": audit.sha256_text(current),
                "path": relative,
                "reason": "branch intentionally retains its allocator API",
            }

            def ref_text(ref: str, _: str) -> str:
                return base if ref == "base" else incoming

            with mock.patch.object(audit, "ref_text", side_effect=ref_text):
                obligations, replacements = audit.analyze(
                    "monolith.zig",
                    "base",
                    "incoming",
                    [path],
                    0.7,
                    0.05,
                    reviewed_resolutions={"function:value": review},
                )

        obligation = next(
            item for item in obligations if item.key == "function:value"
        )
        self.assertEqual("semantic_resolution_reviewed", obligation.status)
        self.assertEqual({}, replacements)

    def test_reviewed_resolution_accepts_hash_locked_container_divergence(self) -> None:
        base = "const VTable = struct {\n    callback: *const fn (u8) void,\n};\n"
        incoming = "const VTable = struct {\n    callback: *const fn (u16) void,\n};\n"
        current = "const VTable = struct {\n    callback: *const fn (u32) void,\n    extra: bool,\n};\n"
        with tempfile.TemporaryDirectory(
            prefix="merge-audit-reviewed-container-",
            dir=audit.ROOT,
        ) as raw_dir:
            path = pathlib.Path(raw_dir) / "split.zig"
            path.write_text(current)
            review = {
                "status": "container_fields_diverged",
                "base_sha256": audit.sha256_text(base),
                "incoming_sha256": audit.sha256_text(incoming),
                "current_sha256": audit.sha256_text(current),
                "path": str(path.relative_to(audit.ROOT)),
                "reason": "branch callback widens the incoming contract",
            }

            def ref_text(ref: str, _: str) -> str:
                return base if ref == "base" else incoming

            with mock.patch.object(audit, "ref_text", side_effect=ref_text):
                obligations, _ = audit.analyze(
                    "monolith.zig",
                    "base",
                    "incoming",
                    [path],
                    0.7,
                    0.05,
                    reviewed_resolutions={"container:VTable": review},
                )

        obligation = next(
            item for item in obligations if item.key == "container:VTable"
        )
        self.assertEqual("semantic_resolution_reviewed", obligation.status)

    def test_test_name_aliases_do_not_apply_to_production_declarations(self) -> None:
        test_declaration = audit.Declaration(
            "test",
            "incoming name",
            'test "incoming name" {}\n',
            0,
            "sample.zig",
            0,
            24,
        )
        function_declaration = dataclasses.replace(
            test_declaration,
            kind="function",
            body="fn incoming_name() void {}\n",
        )
        aliases = {"incoming name": ["branch-prefixed incoming name"]}
        self.assertEqual(
            ["incoming name", "branch-prefixed incoming name"],
            audit.candidate_names(test_declaration, aliases),
        )
        self.assertEqual(
            ["incoming name"],
            audit.candidate_names(function_declaration, aliases),
        )

    def test_split_test_name_rewrites_are_path_scoped(self) -> None:
        declaration = audit.Declaration(
            "test",
            "db basic batch/get works",
            'test "db basic batch/get works" {}\n',
            0,
            "db.zig",
            0,
            40,
        )
        rules = [
            {
                "path": "db/lifecycle.zig",
                "source_prefix": "db ",
                "destination_prefix": "db lifecycle ",
            },
        ]
        self.assertEqual(
            [
                (
                    "db lifecycle basic batch/get works",
                    "db/lifecycle.zig",
                ),
            ],
            audit.rewritten_test_names(declaration, rules),
        )
        self.assertEqual(
            [],
            audit.rewritten_test_names(
                dataclasses.replace(declaration, kind="function"),
                rules,
            ),
        )

    def test_split_test_name_adaptation_only_changes_header(self) -> None:
        incoming = audit.Declaration(
            "test",
            "db behavior",
            'test "db behavior" {\n    try run("db behavior");\n}\n',
            0,
            "db.zig",
            0,
            52,
        )
        current = audit.Declaration(
            "test",
            "db lifecycle behavior",
            'test "db lifecycle behavior" {\n'
            '    try run("db behavior");\n'
            "}\n",
            0,
            "db/lifecycle.zig",
            0,
            62,
        )
        canonical = audit.declaration_for_test_match(current, incoming)
        self.assertEqual(incoming.name, canonical.name)
        self.assertEqual(
            audit.normalized(incoming.body),
            audit.normalized(canonical.body),
        )
        self.assertEqual(
            current.body,
            audit.restore_split_test_name(
                canonical.body,
                incoming,
                current,
            ),
        )

    def test_split_test_name_rewrites_are_manifest_driven(self) -> None:
        policy = {
            "split_migrations": {
                "db": {
                    "source": "db.zig",
                    "destinations": ["db"],
                    "test_name_rewrites": [
                        {
                            "path": "db/lifecycle.zig",
                            "source_prefix": "db ",
                            "destination_prefix": "db lifecycle ",
                        },
                    ],
                },
            },
        }
        self.assertEqual(
            policy["split_migrations"]["db"]["test_name_rewrites"],
            audit.split_test_name_rewrites(policy, "db"),
        )
        with self.assertRaisesRegex(ValueError, "require exactly"):
            audit.split_test_name_rewrites(
                {
                    "split_migrations": {
                        "db": {
                            "test_name_rewrites": [
                                {
                                    "path": "db/lifecycle.zig",
                                    "source_prefix": "db ",
                                },
                            ],
                        },
                    },
                },
                "db",
            )
        with self.assertRaisesRegex(ValueError, "source_prefix"):
            audit.split_test_name_rewrites(
                {
                    "split_migrations": {
                        "db": {
                            "test_name_rewrites": [
                                {
                                    "path": "db/query/result_shape.zig",
                                    "source_prefix": "",
                                    "destination_prefix": "db query result shape ",
                                },
                            ],
                        },
                    },
                },
                "db",
            )

    def test_split_retained_deletions_are_manifest_driven(self) -> None:
        policy = {
            "split_migrations": {
                "writes": {
                    "retained_deletions": {
                        "function:legacy": {
                            "base_sha256": "a" * 64,
                            "current_sha256": "b" * 64,
                            "path": "api/table_writes/legacy.zig",
                            "reason": "branch compatibility surface",
                        },
                    },
                },
            },
        }
        self.assertEqual(
            policy["split_migrations"]["writes"]["retained_deletions"],
            audit.split_retained_deletions(policy, "writes"),
        )
        with self.assertRaisesRegex(ValueError, "lowercase SHA-256 hashes"):
            audit.split_retained_deletions(
                {
                    "split_migrations": {
                        "writes": {
                            "retained_deletions": {
                                "function:legacy": {
                                    "base_sha256": "short",
                                    "current_sha256": "b" * 64,
                                    "path": "legacy.zig",
                                    "reason": "reviewed",
                                },
                            },
                        },
                    },
                },
                "writes",
            )

    def test_merge_preserves_current_ignores_move_indentation(self) -> None:
        current = audit.Declaration(
            "function",
            "value",
            "    fn value() void {\n        incoming();\n        branchExtra();\n        stable();\n    }\n",
            4,
            "split.zig",
            0,
            86,
        )
        merged = (
            "        fn value() void {\n"
            "            incoming();\n"
            "            branchExtra();\n"
            "            stable();\n"
            "        }\n"
        )
        self.assertTrue(audit.merge_preserves_current(current, merged))
        self.assertFalse(
            audit.merge_preserves_current(
                current,
                "fn value() void {\n    incoming();\n    stable();\n}\n",
            )
        )

    def test_split_visibility_normalization_only_ignores_pub(self) -> None:
        incoming = "fn helper() void {\n    run();\n}\n"
        current = "pub fn helper() void {\n    run();\n}\n"
        changed = "pub fn helper() void {\n    branchRun();\n}\n"
        self.assertEqual(
            audit.normalized_split_visibility(incoming),
            audit.normalized_split_visibility(current),
        )
        self.assertNotEqual(
            audit.normalized_split_visibility(incoming),
            audit.normalized_split_visibility(changed),
        )
        self.assertEqual(
            audit.normalized_split_visibility(
                "var test_hook: ?Hook = null;\n"
            ),
            audit.normalized_split_visibility(
                "pub var test_hook: ?Hook = null;\n"
            ),
        )

    def test_multiple_merge_conflicts_are_classified_not_crashed(self) -> None:
        def declaration(body: str, path: str) -> audit.Declaration:
            return audit.Declaration(
                "function", "value", body, 0, path, 0, len(body)
            )

        base = declaration(
            "fn value() void {\n    one();\n    stable();\n    two();\n}\n",
            "base.zig",
        )
        current = declaration(
            "fn value() void {\n    branch_one();\n    stable();\n    branch_two();\n}\n",
            "current.zig",
        )
        incoming = declaration(
            "fn value() void {\n    main_one();\n    stable();\n    main_two();\n}\n",
            "incoming.zig",
        )
        status, merged = audit.merge_declarations(current, base, incoming)
        self.assertGreater(status, 0)
        self.assertIn("<<<<<<<", merged)

    def test_direct_container_fields_only(self) -> None:
        text = """
pub const DB = struct {
    first: u64,
    second: bool = false,

    pub fn method(self: *DB) void {
        const local: u8 = 1;
        _ = self;
        _ = local;
    }
};
"""
        declaration = next(
            item
            for item in audit.all_declarations(text, "sample.zig")
            if item.kind == "container"
        )
        self.assertEqual(
            {"first", "second"},
            audit.direct_container_fields(declaration),
        )
        self.assertEqual(
            {
                "first": "first: u64,",
                "second": "second: bool = false,",
            },
            audit.direct_container_field_lines(declaration),
        )

    def test_direct_container_variants_only(self) -> None:
        text = """
pub const State = enum(u8) {
    ready,
    running = 3,
    stopped, // terminal

    pub fn terminal(self: State) bool {
        return self == .stopped;
    }
};

pub const Payload = union(enum) {
    empty,
    bytes: []const u8,
};
"""
        declarations = {
            item.name: item
            for item in audit.all_declarations(text, "sample.zig")
            if item.kind == "container"
        }
        self.assertEqual(
            {"ready", "running", "stopped"},
            audit.direct_container_variants(declarations["State"]),
        )
        self.assertEqual(
            {"empty"},
            audit.direct_container_variants(declarations["Payload"]),
        )
        self.assertEqual(
            {"field:bytes", "variant:empty"},
            audit.direct_container_members(declarations["Payload"]),
        )

    def test_qualified_type_aliases_are_discovered(self) -> None:
        aliases = audit.extract_aliases(
            "pub const Response = runtime.Response;\n"
            "const scalar = 1;\n",
            "sample.zig",
        )
        self.assertEqual(
            [("alias", "Response", "pub const Response = runtime.Response;\n")],
            [(item.kind, item.name, item.body) for item in aliases],
        )

    def test_container_factory_fields_are_audited(self) -> None:
        text = """
pub fn AsyncContext(comptime DB: type) type {
    return struct {
        alloc: Allocator,
        pending: bool = false,

        pub fn deinit(self: *@This()) void {
            _ = self;
            _ = DB;
        }
    };
}
"""
        declaration = next(
            item
            for item in audit.all_declarations(text, "internal.zig")
            if item.kind == "function"
        )
        self.assertEqual(
            {
                "alloc": "alloc: Allocator,",
                "pending": "pending: bool = false,",
            },
            audit.direct_container_field_lines(declaration),
        )

    def test_neighbor_placement_requires_agreeing_anchors(self) -> None:
        declarations = [
            audit.Declaration("function", name, "", 0, "source.zig", i, i + 1)
            for i, name in enumerate(("before", "added", "after"))
        ]
        self.assertEqual(
            ("split.zig", "function:before", "function:after"),
            audit.neighbor_placement(
                1,
                declarations,
                ["split.zig", None, "split.zig"],
            ),
        )
        self.assertIsNone(
            audit.neighbor_placement(
                1,
                declarations,
                ["left.zig", None, "right.zig"],
            )
        )
        nested = dataclasses.replace(declarations[1], owner="Owner")
        self.assertIsNone(
            audit.neighbor_placement(
                1,
                [declarations[0], nested, declarations[2]],
                ["split.zig", None, "split.zig"],
            )
        )

    def test_candidate_directory_must_be_outside_repository(self) -> None:
        with self.assertRaises(ValueError):
            audit.prepare_candidate_root(audit.ROOT / "candidate-output")
        with tempfile.TemporaryDirectory() as raw:
            path = pathlib.Path(raw) / "candidate-output"
            self.assertEqual(path.resolve(), audit.prepare_candidate_root(path))


if __name__ == "__main__":
    unittest.main()
