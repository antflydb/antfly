import pathlib
import unittest
from unittest import mock

from scripts.merge_audit import audit_zig_build_surface as audit


class ZigBuildSurfaceAuditTest(unittest.TestCase):
    def test_parse_name_status_audits_both_sides_of_renames(self) -> None:
        self.assertEqual(
            {
                "build/modified.zig",
                "build/old.zig",
                "build/new.zig",
                "build/copied.zig",
            },
            audit.parse_name_status(
                b"M\0build/modified.zig\0"
                b"R100\0build/old.zig\0build/new.zig\0"
                b"C100\0build/source.zig\0build/copied.zig\0"
            ),
        )

    def test_parse_name_status_rejects_truncated_records(self) -> None:
        with self.assertRaisesRegex(ValueError, "truncated rename"):
            audit.parse_name_status(b"R100\0build/old.zig\0")

    def test_classify_companion_requires_an_owner_for_composed_files(self) -> None:
        exact = audit.classify_companion("build/exact.zig", b"same", b"same", None)
        delegated = audit.classify_companion(
            "build/composed.zig",
            b"incoming",
            b"composed",
            "build_composed",
        )
        unresolved = audit.classify_companion(
            "build/unowned.zig",
            b"incoming",
            b"composed",
            None,
        )

        self.assertEqual("exact", exact["status"])
        self.assertEqual("declaration_audit_required", delegated["status"])
        self.assertEqual("divergent_unowned", unresolved["status"])
        self.assertEqual("build_composed", delegated["owner_migration"])

    def test_companion_report_discovers_changed_owned_paths(self) -> None:
        policy = {
            "required_split_migrations": ["build_composed"],
            "split_migrations": {
                "build_composed": {"source": "build/composed.zig"},
            },
        }
        incoming = {
            "build/exact.zig": b"exact",
            "build/composed.zig": b"incoming",
            "outside.zig": b"outside",
        }
        current = {
            "build/exact.zig": b"exact",
            "build/composed.zig": b"branch plus incoming",
            "outside.zig": b"different",
        }
        with mock.patch.object(
            audit,
            "changed_paths",
            return_value={"build.zig", *incoming},
        ), mock.patch.object(
            audit,
            "ref_blob",
            side_effect=lambda _ref, path: incoming[path],
        ), mock.patch.object(
            audit,
            "current_blob",
            side_effect=lambda path: current[path],
        ):
            report = audit.companion_report(
                "base",
                "incoming",
                "build.zig",
                ["build/**"],
                policy,
            )

        self.assertEqual(2, report["summary"]["changed"])
        self.assertEqual(1, report["summary"]["exact"])
        self.assertEqual(1, report["summary"]["delegated"])
        self.assertEqual(0, report["summary"]["unresolved"])

    def test_companion_report_treats_split_destinations_as_owned(self) -> None:
        policy = {
            "required_split_migrations": ["build_split"],
            "split_migrations": {
                "build_split": {
                    "source": "build.zig",
                    "destinations": ["build/split"],
                },
            },
        }
        with mock.patch.object(
            audit,
            "changed_paths",
            return_value={"build/split/helper.zig"},
        ), mock.patch.object(
            audit.split_audit,
            "expand_destinations",
            return_value=[audit.ROOT / "build/split/helper.zig"],
        ), mock.patch.object(
            audit,
            "ref_blob",
            return_value=b"premerge",
        ), mock.patch.object(
            audit,
            "current_blob",
            return_value=b"premerge plus main",
        ):
            report = audit.companion_report(
                "base",
                "premerge",
                "build.zig",
                ["build/**"],
                policy,
            )

        self.assertEqual(1, report["summary"]["delegated"])
        self.assertEqual(0, report["summary"]["unresolved"])
        self.assertEqual(
            "build_split", report["files"][0]["owner_migration"]
        )

    def test_added_surface_covers_build_registration_kinds(self) -> None:
        base = 'const old = b.step("old", "old");\n'
        incoming = base + """
const mode = b.option(bool, "ha-tests", "ha");
const step = b.step("new-test", "new");
const mod = b.createModule(.{ .root_source_file = b.path("new_root.zig") });
const exe = b.addExecutable(.{ .name = "new_exe", .root_module = mod });
"""
        added = audit.added_surface(base, incoming)
        self.assertEqual({"ha-tests"}, added["options"])
        self.assertEqual({"new-test"}, added["steps"])
        self.assertEqual({"new_root.zig"}, added["root_sources"])
        self.assertEqual({"new_exe"}, added["artifacts"])

    def test_artifact_surface_ignores_unrelated_name_fields(self) -> None:
        text = '''
const imports = .{ .name = "not-an-artifact", .field = "platform" };
const exe = b.addExecutable(.{
    .name = "real-artifact",
    .root_module = mod,
});
'''
        self.assertEqual({"real-artifact"}, audit.artifact_names(text))

    def test_semantic_surface_keeps_build_graph_endpoints(self) -> None:
        text = '''
module.addImport(
    "dep",
    dependency.module("dep"),
);
suite.step.dependOn(&run.step);
module.linkSystemLibrary("archive", .{ .needed = true });
module.addIncludePath(b.path("include/native"));
module.addCSourceFiles(.{
    .files = &.{ "src/one.c", "src/two.cc" },
    .flags = &.{"-O2"},
});
'''
        surface = audit.semantic_surface(text)
        self.assertEqual(
            {'addImport("dep",dependency.module("dep"))'},
            surface["module_imports"],
        )
        self.assertEqual(
            {"suite->run"},
            surface["step_dependencies"],
        )
        self.assertEqual(
            {'linkSystemLibrary("archive",.{.needed=true})'},
            surface["system_libraries"],
        )
        self.assertEqual(
            {'addIncludePath(b.path("include/native"))'},
            surface["include_paths"],
        )
        self.assertEqual(
            {
                "module.native_source(src/one.c)",
                "module.native_source(src/two.cc)",
            },
            surface["native_sources"],
        )

    def test_step_dependency_bindings_do_not_leak_across_functions(self) -> None:
        text = '''
fn dependOnAll(step: *std.Build.Step, dependency: *std.Build.Step) void {
    step.dependOn(dependency);
}

fn build(b: *std.Build) void {
    const step = b.step("named-test", "test");
    step.dependOn(&run_tests.step);
}
'''
        self.assertEqual(
            {"step->dependency", "named-test->tests"},
            audit.step_dependency_facts(text),
        )

    def test_semantic_surface_normalizes_context_qualified_dependencies(self) -> None:
        surface = audit.semantic_surface(
            'first.addImport("shared", ctx.dep); second.addImport("shared", dep);'
        )
        self.assertEqual(
            {'addImport("shared",dep)'},
            surface["module_imports"],
        )

    def test_dependency_helper_maps_public_step_to_returned_run(self) -> None:
        text = '''
const run_cmd_tests = addNamedFilteredTest(
    b,
    module,
    &.{},
    "cmd-test",
    "commands",
);
_ = addNamedFilteredTest(b, module, &.{}, "search-test", "search");
'''
        self.assertEqual(
            {"cmd-test->cmd", "search-test->search"},
            audit.helper_dependency_facts(
                text,
                [{"function": "addNamedFilteredTest", "step_argument": 3}],
            ),
        )

    def test_dependency_helper_reads_descriptor_step_name(self) -> None:
        text = '''
_ = addDBFilteredTestStep(b, root, .{
    .name = "db-restore-test",
    .filters = &.{"restore"},
});
'''
        self.assertEqual(
            {"db-restore-test->db_restore"},
            audit.helper_dependency_facts(
                text,
                [
                    {
                        "function": "addDBFilteredTestStep",
                        "step_argument": 2,
                        "step_field": "name",
                    }
                ],
            ),
        )

    def test_current_steps_include_split_helper_registrations(self) -> None:
        text = """
const direct = b.step("direct-test", "direct");
addFocusedAPITestStep(b, "focused-test", "focused", run);
const run = addAPIFocusedTestRun(b, root, "api-test", "api", &filters, false, false, null);
const module = addModuleTestStep(b, root, "module-test", "module", .{});
const spec = .{ .name = "declared-test", .description = "declared" };
"""
        self.assertEqual(
            {"direct-test", "focused-test", "api-test", "module-test", "declared-test"},
            audit.current_surface(text)["steps"],
        )

    def test_current_surface_uses_manifest_configured_helpers(self) -> None:
        text = '''
const run = addNamedFilteredTest(
    b,
    makeConfiguredTestModule(ctx, "pkg/root.zig"),
    &.{"one", "two"},
    "focused-test",
    "description",
);
'''
        surface = audit.current_surface(
            text,
            {
                "steps": [{"function": "addNamedFilteredTest", "argument": 3}],
                "root_sources": [
                    {"function": "makeConfiguredTestModule", "argument": 1}
                ],
            },
        )
        self.assertIn("focused-test", surface["steps"])
        self.assertIn("pkg/root.zig", surface["root_sources"])

    def test_current_surface_finds_descriptor_and_bound_root_sources(self) -> None:
        text = '''
const descriptor = .{ .path = "pkg/descriptor_root.zig" };
const bound_root = "pkg/bound_root.zig";
const module = b.createModule(.{ .root_source_file = b.path(bound_root) });
'''
        self.assertEqual(
            {"pkg/descriptor_root.zig", "pkg/bound_root.zig"},
            audit.current_surface(text)["root_sources"],
        )

    def test_call_string_arguments_ignores_nested_commas(self) -> None:
        text = 'helper(b, make(.{ .x = "not-it" }), &.{"a", "b"}, "wanted");'
        self.assertEqual(
            {"wanted"},
            audit.call_string_arguments(text, "helper", 3),
        )

    def test_build_report_tracks_carried_surface_separately(self) -> None:
        with mock.patch.object(
            audit.split_audit,
            "ref_text",
            side_effect=[
                'const old = b.step("carried", "old");\n',
                'const old = b.step("carried", "old");\nconst new = b.step("added", "new");\n',
            ],
        ), mock.patch.object(
            audit.split_audit,
            "resolve_ref",
            side_effect=lambda ref: ref,
        ):
            with mock.patch.object(
                pathlib.Path,
                "read_text",
                return_value='const new = b.step("added", "new");\n',
            ):
                report = audit.build_report(
                    "base",
                    "incoming",
                    "zig/build.zig",
                    [audit.ROOT / "zig/build.zig"],
                )
        self.assertEqual([], report["missing"]["steps"])
        self.assertEqual(["carried"], report["missing_incoming"]["steps"])
        self.assertEqual("incoming", report["incoming_ref"])

    def test_build_report_accepts_verified_aliases_and_omissions(self) -> None:
        incoming = '''
const renamed = b.step("old-step", "old");
const removed = b.createModule(.{ .root_source_file = b.path("old_root.zig") });
'''
        current = 'const renamed = b.step("new-step", "new");\n'
        with mock.patch.object(
            audit.split_audit,
            "ref_text",
            side_effect=["", incoming],
        ), mock.patch.object(
            audit.split_audit,
            "resolve_ref",
            side_effect=lambda ref: ref,
        ), mock.patch.object(pathlib.Path, "read_text", return_value=current):
            report = audit.build_report(
                "base",
                "incoming",
                "zig/build.zig",
                [audit.ROOT / "zig/build.zig"],
                surface_aliases={"steps": {"old-step": "new-step"}},
                surface_omissions={
                    "root_sources": {"old_root.zig": "replaced by generated module"}
                },
            )
        self.assertEqual(0, report["summary"]["missing_incoming"])

    def test_build_report_rejects_alias_with_missing_target(self) -> None:
        with mock.patch.object(
            audit.split_audit,
            "ref_text",
            side_effect=["", 'const old = b.step("old-step", "old");\n'],
        ), mock.patch.object(
            audit.split_audit,
            "resolve_ref",
            side_effect=lambda ref: ref,
        ), mock.patch.object(pathlib.Path, "read_text", return_value=""):
            with self.assertRaisesRegex(ValueError, "missing targets: new-step"):
                audit.build_report(
                    "base",
                    "incoming",
                    "zig/build.zig",
                    [audit.ROOT / "zig/build.zig"],
                    surface_aliases={"steps": {"old-step": "new-step"}},
                )

    def test_retention_report_skips_policy_absent_from_historical_ref(self) -> None:
        incoming = 'const kept = b.step("kept", "kept");\n'
        with mock.patch.object(
            audit.split_audit,
            "ref_text",
            side_effect=["", incoming],
        ), mock.patch.object(
            audit.split_audit,
            "resolve_ref",
            side_effect=lambda ref: ref,
        ), mock.patch.object(pathlib.Path, "read_text", return_value=incoming):
            report = audit.build_report(
                "base",
                "premerge",
                "zig/build.zig",
                [audit.ROOT / "zig/build.zig"],
                surface_aliases={"steps": {"main-only": "missing-target"}},
                surface_omissions={
                    "root_sources": {"main-only.zig": "main-only replacement"}
                },
                conditional_steps={"main-only": "main-only conditional step"},
                allow_inapplicable_policy=True,
            )

        self.assertEqual(0, report["summary"]["missing_incoming"])
        self.assertEqual(
            ["steps.main-only"], report["inapplicable_policy"]["aliases"]
        )
        self.assertEqual(
            ["main-only"], report["inapplicable_policy"]["conditional_steps"]
        )
        self.assertEqual(
            ["root_sources.main-only.zig"],
            report["inapplicable_policy"]["omissions"],
        )

    def test_retention_aliases_extend_main_aliases_without_overwriting(self) -> None:
        self.assertEqual(
            {
                "steps": {"main-old": "main-new"},
                "root_sources": {"branch-old.zig": "branch-new.zig"},
            },
            audit.merge_surface_aliases(
                {"steps": {"main-old": "main-new"}},
                {"root_sources": {"branch-old.zig": "branch-new.zig"}},
            ),
        )

    def test_runtime_inventory_is_authoritative_for_step_names(self) -> None:
        incoming = 'const wanted = b.step("wanted", "wanted");\n'
        static_false_positive = 'const artifact = .{ .name = "wanted" };\n'
        with mock.patch.object(
            audit.split_audit,
            "ref_text",
            side_effect=["", incoming],
        ), mock.patch.object(
            audit.split_audit,
            "resolve_ref",
            side_effect=lambda ref: ref,
        ), mock.patch.object(
            pathlib.Path,
            "read_text",
            return_value=static_false_positive,
        ):
            report = audit.build_report(
                "base",
                "incoming",
                "zig/build.zig",
                [audit.ROOT / "zig/build.zig"],
                runtime_steps={"install"},
            )
        self.assertEqual(["wanted"], report["missing"]["steps"])

    def test_conditional_step_requires_direct_registration(self) -> None:
        incoming = 'const wanted = b.step("wanted", "wanted");\n'
        with mock.patch.object(
            audit.split_audit,
            "ref_text",
            side_effect=["", incoming],
        ), mock.patch.object(
            audit.split_audit,
            "resolve_ref",
            side_effect=lambda ref: ref,
        ), mock.patch.object(
            pathlib.Path,
            "read_text",
            return_value=incoming,
        ):
            report = audit.build_report(
                "base",
                "incoming",
                "zig/build.zig",
                [audit.ROOT / "zig/build.zig"],
                runtime_steps={"install"},
                conditional_steps={"wanted": "registered only with -Dwanted=true"},
            )
        self.assertEqual([], report["missing"]["steps"])

    def test_conditional_step_rejects_static_name_false_positive(self) -> None:
        incoming = 'const wanted = b.step("wanted", "wanted");\n'
        with mock.patch.object(
            audit.split_audit,
            "ref_text",
            side_effect=["", incoming],
        ), mock.patch.object(
            audit.split_audit,
            "resolve_ref",
            side_effect=lambda ref: ref,
        ), mock.patch.object(
            pathlib.Path,
            "read_text",
            return_value='const artifact = .{ .name = "wanted" };\n',
        ):
            with self.assertRaisesRegex(ValueError, "no direct b.step registration"):
                audit.build_report(
                    "base",
                    "incoming",
                    "zig/build.zig",
                    [audit.ROOT / "zig/build.zig"],
                    runtime_steps={"install"},
                    conditional_steps={"wanted": "registered conditionally"},
                )

    def test_build_report_rejects_stale_omission(self) -> None:
        with mock.patch.object(
            audit.split_audit,
            "ref_text",
            side_effect=["", ""],
        ), mock.patch.object(
            audit.split_audit,
            "resolve_ref",
            side_effect=lambda ref: ref,
        ), mock.patch.object(pathlib.Path, "read_text", return_value=""):
            with self.assertRaisesRegex(ValueError, "stale build surface omission"):
                audit.build_report(
                    "base",
                    "incoming",
                    "zig/build.zig",
                    [audit.ROOT / "zig/build.zig"],
                    surface_omissions={
                        "root_sources": {"old_root.zig": "no longer applicable"}
                    },
                )

    def test_runtime_step_inventory_parses_compiled_build_list(self) -> None:
        completed = mock.Mock(
            returncode=0,
            stdout="  install (default) Copy artifacts\n  focused-test Run tests\n",
            stderr="",
        )
        with mock.patch.object(audit.subprocess, "run", return_value=completed):
            self.assertEqual(
                {"install", "focused-test"},
                audit.runtime_step_inventory(pathlib.Path("zig")),
            )


if __name__ == "__main__":
    unittest.main()
