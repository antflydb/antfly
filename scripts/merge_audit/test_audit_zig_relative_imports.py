import pathlib
import tempfile
import unittest

from scripts.merge_audit import audit_zig_relative_imports as audit


class ZigRelativeImportAuditTest(unittest.TestCase):
    def test_exception_list_requires_exact_reasoned_records(self) -> None:
        with self.assertRaisesRegex(ValueError, "exactly path, import, and reason"):
            audit.load_exception_list(
                "repository",
                [{"path": "owner.zig", "import": "generated.zig"}],
            )

    def test_scanner_ignores_comments_and_unrelated_strings(self) -> None:
        text = '''
const real = @import(
    "nested/real.zig"
);
// const stale = @import("removed.zig");
const literal = "@import(\\\"also_removed.zig\\\")";
\\\\const embedded = @import("multiline_removed.zig");
const package = @import("antfly_platform");
const second = @import("second.zig");
'''
        self.assertEqual(
            [
                audit.ImportReference("nested/real.zig", 2),
                audit.ImportReference("second.zig", 9),
            ],
            audit.scan_file_imports(text),
        )

    def test_audit_reports_missing_escape_and_present_imports(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            owner = root / "owners"
            owner.mkdir()
            (owner / "present.zig").write_text("const value = 1;\n")
            source = owner / "source.zig"
            source.write_text(
                'const present = @import("present.zig");\n'
                'const missing = @import("missing.zig");\n'
                'const escape = @import("../../escape.zig");\n'
            )

            findings, unused = audit.audit_paths("db", [source], {}, root=root)

        self.assertEqual([], unused)
        self.assertEqual(
            ["present", "missing", "escapes_repository"],
            [finding.status for finding in findings],
        )

    def test_reasoned_exception_is_consumed_and_stale_exception_fails(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            owner = root / "owners"
            owner.mkdir()
            source = owner / "source.zig"
            source.write_text('const missing = @import("generated.zig");\n')
            exceptions = {
                ("owners/source.zig", "generated.zig"): "created by build graph",
                ("owners/source.zig", "stale.zig"): "must not linger",
            }

            findings, unused = audit.audit_paths(
                "table_reads", [source], exceptions, root=root
            )

        self.assertEqual("excepted", findings[0].status)
        self.assertEqual(
            [{
                "path": "owners/source.zig",
                "import": "stale.zig",
                "reason": "must not linger",
            }],
            unused,
        )


if __name__ == "__main__":
    unittest.main()
