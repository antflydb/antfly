#!/usr/bin/env python3

import importlib.util
import io
import json
import sys
import tempfile
import unittest
from contextlib import redirect_stderr
from pathlib import Path


SCRIPT = Path(__file__).with_name("analyze_zig_import_graph.py")
SPEC = importlib.util.spec_from_file_location("analyze_zig_import_graph", SCRIPT)
assert SPEC and SPEC.loader
analyzer = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = analyzer
SPEC.loader.exec_module(analyzer)


class ImportGraphTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)

    def tearDown(self):
        self.temporary.cleanup()

    def write(self, name: str, contents: str) -> Path:
        path = self.root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(contents, encoding="utf-8")
        return path.resolve()

    def write_codegen_boundaries(self):
        for name in {
            source for source, _ in analyzer.CODEGEN_BOUNDARIES
        } | {
            target for _, target in analyzer.CODEGEN_BOUNDARIES
        }:
            path = self.root / name
            if not path.exists():
                self.write(name, "pub const value = 1;\n")

    def test_follows_only_existing_relative_zig_imports_inside_root(self):
        entry = self.write(
            "entry.zig",
            'const child = @import("nested/child.zig");\n'
            'const package = @import("package_name");\n'
            'const missing = @import("missing.zig");\n'
            'const outside = @import("../outside.zig");\n',
        )
        child = self.write("nested/child.zig", 'const leaf = @import("leaf.zig");\n')
        leaf = self.write("nested/leaf.zig", "pub const value = 1;\n")

        graph = analyzer.ImportGraph(self.root)

        self.assertEqual({entry, child, leaf}, graph.closure([entry]))

    def test_shortest_path_reports_import_chain(self):
        entry = self.write('entry.zig', 'const left = @import("left.zig");\nconst right = @import("right.zig");\n')
        left = self.write("left.zig", 'const target = @import("target.zig");\n')
        self.write("right.zig", 'const detour = @import("detour.zig");\n')
        self.write("detour.zig", 'const target = @import("target.zig");\n')
        target = self.write("target.zig", "pub const value = 1;\n")

        graph = analyzer.ImportGraph(self.root)

        self.assertEqual([entry, left, target], graph.shortest_path(entry, target))

    def test_resolve_source_rejects_escape(self):
        self.write("entry.zig", "pub const value = 1;\n")
        graph = analyzer.ImportGraph(self.root)

        with self.assertRaisesRegex(ValueError, "escapes"):
            graph.resolve_source("../outside.zig")

    def test_codegen_boundary_accepts_focused_cli(self):
        self.write("cli_runtime.zig", 'const client = @import("client.zig");\n')
        self.write("client.zig", "pub const value = 1;\n")
        self.write_codegen_boundaries()
        graph = analyzer.ImportGraph(self.root)

        self.assertTrue(analyzer.check_codegen_boundary(graph))

    def test_codegen_boundary_rejects_transitive_runtime_import(self):
        self.write("cli_runtime.zig", 'const command = @import("cmd/lite.zig");\n')
        self.write("cmd/lite.zig", 'const runtime = @import("../standalone/runtime.zig");\n')
        self.write_codegen_boundaries()
        graph = analyzer.ImportGraph(self.root)

        diagnostics = io.StringIO()
        with redirect_stderr(diagnostics):
            self.assertFalse(analyzer.check_codegen_boundary(graph))
        self.assertIn(
            "cli_runtime.zig -> cmd/lite.zig -> standalone/runtime.zig",
            diagnostics.getvalue(),
        )

    def test_codegen_boundary_rejects_inference_host_runtime_import(self):
        self.write_codegen_boundaries()
        self.write(
            "standalone/inference_host.zig",
            'const runtime = @import("runtime.zig");\n',
        )
        graph = analyzer.ImportGraph(self.root)

        diagnostics = io.StringIO()
        with redirect_stderr(diagnostics):
            self.assertFalse(analyzer.check_codegen_boundary(graph))
        self.assertIn(
            "standalone/inference_host.zig -> standalone/runtime.zig",
            diagnostics.getvalue(),
        )

    def test_time_report_resolves_paths_relative_to_zig_directory(self):
        source = self.write("zig/pkg/antfly/src/example.zig", "pub const value = 1;\n")
        report_path = self.root / "report.json"
        report_path.write_text(
            json.dumps(
                {
                    "total_ns": 2_000_000_000,
                    "stats": {
                        "imported_files": 4,
                        "real_ns_llvm_emit": 1_500_000_000,
                    },
                    "all_files": ["pkg/antfly/src/example.zig"],
                }
            ),
            encoding="utf-8",
        )

        report = analyzer.load_time_report("example", report_path, self.root)

        self.assertEqual(frozenset({source}), report.repo_files)
        self.assertTrue(report.has_file_list)
        self.assertEqual(2.0, analyzer.report_stats(report)["total_seconds"])

    def test_time_report_does_not_treat_missing_file_list_as_empty_graph(self):
        report_path = self.root / "report.json"
        report_path.write_text(json.dumps({"total_ns": 1}), encoding="utf-8")

        report = analyzer.load_time_report("old", report_path, self.root)

        self.assertFalse(report.has_file_list)
        self.assertIsNone(analyzer.report_stats(report)["repo_zig_files"])


if __name__ == "__main__":
    unittest.main()
