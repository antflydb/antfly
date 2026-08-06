import json
import pathlib
import tempfile
import unittest
from unittest import mock

from scripts.merge_audit import audit_zig_split_merge as audit


class ConflictMarkerTests(unittest.TestCase):
    def test_separator_lines_are_not_conflict_markers(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            (root / "separator.txt").write_text(
                "==============================\n"
                "===========                          ================\n"
            )
            with mock.patch.object(audit, "ROOT", root):
                self.assertFalse(
                    audit.text_file_has_conflict_markers("separator.txt")
                )

    def test_exact_git_marker_lines_are_detected(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            (root / "conflict.txt").write_text(
                "<<<<<<< HEAD\nours\n=======\ntheirs\n>>>>>>> origin/main\n"
            )
            with mock.patch.object(audit, "ROOT", root):
                self.assertTrue(
                    audit.text_file_has_conflict_markers("conflict.txt")
                )


class ManifestPolicyTests(unittest.TestCase):
    def test_same_path_function_false_positives_are_loaded(self):
        original = {
            key: set(values)
            for key, values in audit.SAME_PATH_FUNCTION_FALSE_POSITIVES.items()
        }
        try:
            with tempfile.TemporaryDirectory() as tmp:
                manifest = pathlib.Path(tmp) / "policy.json"
                manifest.write_text(json.dumps({
                    "same_path_function_false_positives": {
                        "src/example.zig": ["removedHelper"],
                    },
                }))
                audit.load_manifest_policy(manifest)
            self.assertEqual(
                {"removedHelper"},
                audit.SAME_PATH_FUNCTION_FALSE_POSITIVES["src/example.zig"],
            )
        finally:
            audit.SAME_PATH_FUNCTION_FALSE_POSITIVES.clear()
            audit.SAME_PATH_FUNCTION_FALSE_POSITIVES.update(original)

    def test_path_qualified_symbol_alias_requires_live_destination(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            destination = root / "src/destination.zig"
            destination.parent.mkdir(parents=True)
            destination.write_text(
                "pub const CurrentType = struct {};\n"
                "fn currentHelper() void {}\n"
            )
            with mock.patch.object(audit, "ROOT", root):
                self.assertTrue(audit.current_symbol_alias_exists(
                    "src/destination.zig::CurrentType", "", "const"
                ))
                self.assertTrue(audit.current_symbol_alias_exists(
                    "src/destination.zig::currentHelper", "", "fn"
                ))
                self.assertFalse(audit.current_symbol_alias_exists(
                    "src/destination.zig::Missing", "", "const"
                ))


class ImportedFunctionReferenceTests(unittest.TestCase):
    def test_comments_and_literals_do_not_create_member_calls(self):
        text = (
            'const imported = @import("imported.zig");\n'
            '// imported.missing()\n'
            'const message = "imported.alsoMissing()";\n'
            '/* imported.blockMissing() */\n'
            '\\\\imported.multilineMissing()\n'
            'pub fn run() void { imported.present(); }\n'
        )
        masked = audit.mask_zig_comments_and_strings(text)
        self.assertIn("imported.present()", masked)
        self.assertNotIn("imported.missing()", masked)
        self.assertNotIn("imported.alsoMissing()", masked)
        self.assertNotIn("imported.blockMissing()", masked)
        self.assertNotIn("imported.multilineMissing()", masked)

    def test_real_missing_imported_member_fails(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp).resolve()
            src = root / "src"
            src.mkdir()
            (src / "imported.zig").write_text("pub fn present() void {}\n")
            (src / "caller.zig").write_text(
                'const imported = @import("imported.zig");\n'
                '// imported.commentOnly()\n'
                'pub fn run() void { imported.missing(); }\n'
            )
            with mock.patch.object(audit, "ROOT", root):
                result = audit.check_current_imported_function_refs([
                    audit.ChangedFile("src/caller.zig", "staged")
                ])[0]
            self.assertFalse(result.ok)
            self.assertIn("imported.missing", result.detail)
            self.assertNotIn("commentOnly", result.detail)


if __name__ == "__main__":
    unittest.main()
