from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import gliner2_release_contract as contract
from gliner2_release_contract import (
    CANONICAL_NORMALIZATION,
    MODEL_FINGERPRINT_ENTRIES,
    canonical_text,
    directory_fingerprint,
    verify_upstream_checkout,
)


class ReleaseContractTest(unittest.TestCase):
    def test_canonical_normalization_is_versioned_and_unicode_aware(self) -> None:
        self.assertEqual("unicode_nfc_collapsed_whitespace_casefold/v1", CANONICAL_NORMALIZATION)
        with self.subTest(rule="NFC"):
            self.assertEqual("café", canonical_text("cafe\u0301"))
        with self.subTest(rule="collapsed whitespace"):
            self.assertEqual("one two", canonical_text("  one\t\n two  "))
        with self.subTest(rule="casefold"):
            self.assertEqual("strasse", canonical_text("Straße"))

    def test_model_fingerprint_matches_zig_present_and_absent_goldens(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            model_dir = Path(tmp)
            for entry in MODEL_FINGERPRINT_ENTRIES:
                path = model_dir / entry.relative_path
                path.parent.mkdir(parents=True, exist_ok=True)
                if not entry.optional:
                    path.write_bytes(entry.relative_path.encode())

            self.assertEqual(
                "sha256:9317c6a7c2d586358da84851ecfe259a2075724436fbc26736b9f15d7fdaa638",
                directory_fingerprint(model_dir, MODEL_FINGERPRINT_ENTRIES),
            )
            (model_dir / "spm.model").write_bytes(b"spm.model")
            self.assertEqual(
                "sha256:87543f2c1d708003977125e91fd1b0f2217c3500db7f480a503e87be38cce833",
                directory_fingerprint(model_dir, MODEL_FINGERPRINT_ENTRIES),
            )

    def test_optional_path_that_is_not_a_file_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            model_dir = Path(tmp)
            for entry in MODEL_FINGERPRINT_ENTRIES:
                path = model_dir / entry.relative_path
                path.parent.mkdir(parents=True, exist_ok=True)
                if not entry.optional:
                    path.write_bytes(b"required")
            (model_dir / "spm.model").mkdir()
            with self.assertRaisesRegex(ValueError, "optional fingerprint path is not a regular file"):
                directory_fingerprint(model_dir, MODEL_FINGERPRINT_ENTRIES)

    def test_oracle_checkout_accepts_only_the_fixed_clean_commit(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            checkout = Path(tmp)
            package = checkout / "gliner2"
            package.mkdir()
            (package / "__init__.py").write_text("", encoding="utf-8")
            clean = [
                SimpleNamespace(returncode=0, stdout="8f3fc399bcc5a00749a62a1565e5c6529f04b574\n"),
                SimpleNamespace(returncode=0, stdout=""),
            ]
            with mock.patch("gliner2_release_contract.subprocess.run", side_effect=clean):
                result = verify_upstream_checkout(checkout)
            self.assertEqual("8f3fc399bcc5a00749a62a1565e5c6529f04b574", result["commit"])

            wrong = SimpleNamespace(returncode=0, stdout="wrong\n")
            with (
                mock.patch("gliner2_release_contract.subprocess.run", return_value=wrong),
                self.assertRaisesRegex(ValueError, "does not match frozen"),
            ):
                verify_upstream_checkout(checkout)

    def test_normalization_runtime_is_pinned_to_python312_unicode15(self) -> None:
        with (
            mock.patch.object(contract.sys, "version_info", (3, 12, 0)),
            mock.patch.object(contract.unicodedata, "unidata_version", "15.0.0"),
        ):
            self.assertEqual(
                {"python": "3.12", "unicode": "15.0.0"},
                contract.verify_canonical_python_runtime(),
            )
        with (
            mock.patch.object(contract.sys, "version_info", (3, 14, 0)),
            mock.patch.object(contract.unicodedata, "unidata_version", "16.0.0"),
            self.assertRaisesRegex(ValueError, "requires Python 3.12 / Unicode 15.0.0"),
        ):
            contract.verify_canonical_python_runtime()


if __name__ == "__main__":
    unittest.main()
