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


if __name__ == "__main__":
    unittest.main()
