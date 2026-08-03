from __future__ import annotations

import hashlib
import json
import pathlib
import tempfile
import unittest
import uuid

from scripts.merge_audit import apply_split_candidates as apply


class ApplySplitCandidatesTest(unittest.TestCase):
    def setUp(self) -> None:
        self.relative = f".merge-audit-test-{uuid.uuid4().hex}.zig"
        self.current = apply.ROOT / self.relative
        self.current.write_text("const value = 1;\n")
        self.addCleanup(self.current.unlink, missing_ok=True)

    def candidate_manifest(
        self,
        root: pathlib.Path,
        candidate_body: bytes = b"const value = 2;\n",
    ) -> pathlib.Path:
        candidate = root / self.relative
        candidate.write_bytes(candidate_body)
        manifest = {
            "candidate_files": [self.relative],
            "current_file_sha256": {
                self.relative: hashlib.sha256(self.current.read_bytes()).hexdigest(),
            },
            "candidate_file_sha256": {
                self.relative: hashlib.sha256(candidate_body).hexdigest(),
            },
        }
        path = root / "split-declaration-candidates.json"
        path.write_text(json.dumps(manifest))
        return path

    def test_preflight_and_apply_are_hash_locked(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            manifest = self.candidate_manifest(pathlib.Path(raw))
            prepared = apply.preflight(manifest, [])
            apply.apply_preflighted(prepared)
            self.assertEqual("const value = 2;\n", self.current.read_text())

    def test_preflight_rejects_changed_current_file(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            manifest = self.candidate_manifest(pathlib.Path(raw))
            self.current.write_text("const value = 3;\n")
            with self.assertRaisesRegex(ValueError, "current file hash changed"):
                apply.preflight(manifest, [])

    def test_preflight_rejects_conflict_markers(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            manifest = self.candidate_manifest(
                pathlib.Path(raw),
                b"<<<<<<< current\n=======\n>>>>>>> incoming\n",
            )
            with self.assertRaisesRegex(ValueError, "conflict markers"):
                apply.preflight(manifest, [])


if __name__ == "__main__":
    unittest.main()
