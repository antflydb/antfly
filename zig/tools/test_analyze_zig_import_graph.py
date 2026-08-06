#!/usr/bin/env python3

import importlib.util
import sys
import tempfile
import unittest
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


if __name__ == "__main__":
    unittest.main()
