#!/usr/bin/env python3

import importlib.util
import io
import json
import struct
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

    def write_elf_object(self, name: str, sections: list[tuple[str, int, int]]) -> Path:
        names = bytearray(b"\0.shstrtab\0")
        name_offsets = {"": 0, ".shstrtab": 1}
        for section_name, _, _ in sections:
            name_offsets[section_name] = len(names)
            names.extend(section_name.encode())
            names.append(0)
        string_offset = 64
        section_offset = (string_offset + len(names) + 7) & ~7
        section_count = 2 + len(sections)
        header = struct.pack(
            "<16sHHIQQQIHHHHHH",
            b"\x7fELF" + bytes((2, 1, 1)) + bytes(9),
            1,
            183,
            1,
            0,
            0,
            section_offset,
            0,
            64,
            0,
            0,
            64,
            section_count,
            1,
        )
        section_headers = [bytes(64)]
        section_headers.append(
            struct.pack(
                "<IIQQQQIIQQ",
                name_offsets[".shstrtab"],
                3,
                0,
                0,
                string_offset,
                len(names),
                0,
                0,
                1,
                0,
            )
        )
        for section_name, size, flags in sections:
            section_headers.append(
                struct.pack(
                    "<IIQQQQIIQQ",
                    name_offsets[section_name],
                    1,
                    flags,
                    0,
                    0,
                    size,
                    0,
                    0,
                    1,
                    0,
                )
            )
        payload = header + names + bytes(section_offset - string_offset - len(names)) + b"".join(section_headers)
        path = self.root / name
        path.write_bytes(payload)
        return path

    def write_codegen_boundaries(self):
        for name in {
            source for source, _ in analyzer.CODEGEN_BOUNDARIES
        } | {
            target for _, target in analyzer.CODEGEN_BOUNDARIES
        } | set(analyzer.CONTROL_WAL_CONSUMERS) | {
            analyzer.NATIVE_WAL_IMPLEMENTATION,
        } | {source for source, _ in analyzer.INFERENCE_ABI_FORBIDDEN_TOKENS}:
            path = self.root / name
            if not path.exists():
                self.write(name, "pub const value = 1;\n")

    def write_api_kernel_boundaries(self):
        for name in set(analyzer.API_KERNEL_CONTRACTS) | set(analyzer.API_KERNEL_IMPLEMENTATIONS):
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

    def test_codegen_boundary_rejects_raw_inference_provider_export(self):
        self.write_codegen_boundaries()
        self.write(
            "standalone/inference_bridge.zig",
            "pub const ProviderContext = extern struct {};\n",
        )
        graph = analyzer.ImportGraph(self.root)

        diagnostics = io.StringIO()
        with redirect_stderr(diagnostics):
            self.assertFalse(analyzer.check_codegen_boundary(graph))
        self.assertIn("removed raw inference ABI token ProviderContext", diagnostics.getvalue())

    def test_codegen_boundary_accepts_control_wal_runtime_selector(self):
        self.write_codegen_boundaries()
        self.write(
            "storage/ha/replication_log.zig",
            'const wal = @import("../wal_runtime.zig");\n',
        )
        self.write(
            "storage/wal_runtime.zig",
            'const native = @import("wal.zig");\n',
        )
        graph = analyzer.ImportGraph(self.root)

        self.assertTrue(analyzer.check_codegen_boundary(graph))

    def test_codegen_boundary_rejects_direct_native_wal_import(self):
        self.write_codegen_boundaries()
        self.write(
            "storage/ha/replication_log.zig",
            'const wal = @import("../wal.zig");\n',
        )
        graph = analyzer.ImportGraph(self.root)

        diagnostics = io.StringIO()
        with redirect_stderr(diagnostics):
            self.assertFalse(analyzer.check_codegen_boundary(graph))
        self.assertIn(
            "storage/ha/replication_log.zig directly imports storage/wal.zig",
            diagnostics.getvalue(),
        )

    def test_api_kernel_boundary_accepts_data_only_contract_imports(self):
        self.write_api_kernel_boundaries()
        self.write("api/table_write_source.zig", 'const wire = @import("backup_contract.zig");\n')
        self.write("api/backup_contract.zig", "pub const value = 1;\n")
        graph = analyzer.ImportGraph(self.root)

        self.assertTrue(analyzer.check_api_kernel_boundary(graph))

    def test_api_kernel_boundary_rejects_direct_implementation_import(self):
        self.write_api_kernel_boundaries()
        self.write("api/table_write_source.zig", 'const impl = @import("table_writes.zig");\n')
        graph = analyzer.ImportGraph(self.root)

        diagnostics = io.StringIO()
        with redirect_stderr(diagnostics):
            self.assertFalse(analyzer.check_api_kernel_boundary(graph))
        self.assertIn(
            "api/table_write_source.zig directly imports api/table_writes.zig",
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

    def test_time_report_comparison_reports_shared_fraction_of_smaller_graph(self):
        shared = self.write("zig/pkg/antfly/src/shared.zig", "pub const shared = true;\n")
        base_only = self.write("zig/pkg/antfly/src/base.zig", "pub const base = true;\n")
        candidate_only = self.write("zig/pkg/antfly/src/candidate.zig", "pub const candidate = true;\n")
        base = analyzer.TimeReport("base", self.root / "base.json", {}, frozenset({shared, base_only}), True)
        candidate = analyzer.TimeReport(
            "candidate",
            self.root / "candidate.json",
            {},
            frozenset({shared, candidate_only}),
            True,
        )

        stats = analyzer.comparison_stats(base, candidate)

        self.assertEqual(1, stats["shared_files"])
        self.assertEqual(1, stats["shared_lines"])
        self.assertEqual(0.5, stats["shared_fraction_of_smaller_graph"])

    def test_aggregate_overlap_counts_duplicate_instances_and_groups(self):
        storage = self.write("zig/pkg/antfly/src/storage/db.zig", "one\ntwo\n")
        httpx = self.write("zig/lib/httpx/src/httpx.zig", "one\n")
        unique = self.write("zig/pkg/antfly/src/unique.zig", "one\n")
        reports = [
            analyzer.TimeReport("one", self.root / "one.json", {}, frozenset({storage, httpx}), True),
            analyzer.TimeReport("two", self.root / "two.json", {}, frozenset({storage, httpx, unique}), True),
            analyzer.TimeReport("three", self.root / "three.json", {}, frozenset({storage}), True),
        ]

        stats = analyzer.aggregate_overlap_stats(reports, self.root)

        self.assertTrue(stats["available"])
        self.assertEqual(6, stats["file_instances"])
        self.assertEqual(3, stats["unique_files"])
        self.assertEqual(3, stats["duplicate_instances"])
        groups = {row["name"]: row for row in stats["groups"]}
        self.assertEqual(2, groups["zig/pkg/antfly/src/storage"]["duplicate_instances"])
        self.assertEqual(4, groups["zig/pkg/antfly/src/storage"]["duplicate_lines"])
        self.assertEqual(1, groups["zig/lib/httpx"]["duplicate_instances"])

    def test_aggregate_overlap_requires_file_lists_from_every_report(self):
        report = analyzer.TimeReport("old", self.root / "old.json", {}, frozenset(), False)

        self.assertFalse(analyzer.aggregate_overlap_stats([report])["available"])

    def test_object_report_attributes_alloc_sections_to_longest_source_module(self):
        source_root = self.root / "src"
        (source_root / "storage/db").mkdir(parents=True)
        (source_root / "storage/db.zig").write_text("pub const root = true;\n")
        (source_root / "storage/db/db.zig").write_text("pub const implementation = true;\n")
        object_path = self.write_elf_object(
            "candidate.o",
            [
                (".text..Lstorage.db.db.DB.open.412", 64, 0x6),
                (".rodata..Lstorage.db.db.DB.open", 16, 0x2),
                (".debug_info", 4096, 0),
            ],
        )

        report = analyzer.load_object_report("candidate", object_path, source_root)

        self.assertEqual(80, report.alloc_bytes)
        self.assertEqual(64, report.text_bytes)
        self.assertEqual(0, report.unassigned_bytes)
        self.assertEqual(
            analyzer.ModuleEmission(bytes=80, text_bytes=64, sections=2),
            report.modules["storage.db.db"],
        )
        self.assertEqual(
            analyzer.SectionEmission(module="storage.db.db", bytes=64, text_bytes=64),
            report.named_sections[".text..Lstorage.db.db.DB.open"],
        )

    def test_object_overlap_separates_same_module_from_repeated_named_sections(self):
        one = analyzer.ObjectReport(
            "one",
            self.root / "one.o",
            {"storage.db.db": analyzer.ModuleEmission(100, 80, 2)},
            100,
            80,
            0,
            {
                ".text..Lstorage.db.db.DB.open": analyzer.SectionEmission("storage.db.db", 80, 80),
                ".rodata..Lstorage.db.db.onlyOne": analyzer.SectionEmission("storage.db.db", 20, 0),
            },
        )
        two = analyzer.ObjectReport(
            "two",
            self.root / "two.o",
            {
                "storage.db.db": analyzer.ModuleEmission(90, 70, 2),
                "api.query": analyzer.ModuleEmission(20, 15, 1),
            },
            110,
            85,
            0,
            {
                ".text..Lstorage.db.db.DB.open": analyzer.SectionEmission("storage.db.db", 70, 70),
                ".text..Lstorage.db.db.onlyTwo": analyzer.SectionEmission("storage.db.db", 20, 0),
                ".text..Lapi.query.parse": analyzer.SectionEmission("api.query", 20, 15),
            },
        )

        stats = analyzer.aggregate_object_overlap_stats([one, two])

        self.assertEqual(1, stats["coemitted_modules"])
        self.assertEqual(90, stats["coemitted_module_bytes"])
        self.assertEqual(1, stats["duplicated_sections"])
        self.assertEqual(70, stats["duplicate_bytes"])
        self.assertEqual(70, stats["duplicate_text_bytes"])
        self.assertEqual("storage.db.db", stats["modules"][0]["name"])
        self.assertEqual(1, stats["modules"][0]["duplicated_sections"])


if __name__ == "__main__":
    unittest.main()
