#!/usr/bin/env python3
"""Tests for digest-checked PyPI promotion."""

from __future__ import annotations

import importlib.util
import sys
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest import mock

SCRIPT = Path(__file__).with_name("prepare_pypi_promotion.py")


def load_module():
    spec = importlib.util.spec_from_file_location("prepare_pypi_promotion", SCRIPT)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"failed to load {SCRIPT}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def write_wheel(path: Path) -> None:
    with zipfile.ZipFile(path, "w") as wheel:
        wheel.writestr(
            "antfly_cli-1.2.3.dist-info/METADATA", "Name: antfly-cli\nVersion: 1.2.3\n"
        )


class PyPIPromotionTests(unittest.TestCase):
    def run_promotion(
        self,
        module,
        snapshot: Path,
        output: Path | None = None,
        *,
        verify_complete: bool = False,
        attempts: int = 1,
        expected_version: str | None = None,
    ) -> int:
        argv = [
            str(SCRIPT),
            "--project",
            "antfly-cli",
            "--snapshot-dir",
            str(snapshot),
        ]
        if output is not None:
            argv.extend(("--out-dir", str(output)))
        if expected_version is not None:
            argv.extend(("--expected-version", expected_version))
        if verify_complete:
            argv.extend(
                (
                    "--verify-complete",
                    "--attempts",
                    str(attempts),
                    "--retry-seconds",
                    "0",
                )
            )
        with mock.patch.object(sys, "argv", argv):
            return module.main()

    def test_exact_registry_artifact_is_skipped(self) -> None:
        module = load_module()
        with tempfile.TemporaryDirectory() as raw_tmp:
            root = Path(raw_tmp)
            snapshot, output = root / "snapshot", root / "output"
            snapshot.mkdir()
            wheel = snapshot / "antfly_cli-1.2.3-py3-none-any.whl"
            write_wheel(wheel)
            with mock.patch.object(
                module,
                "pypi_release_files",
                return_value={wheel.name: module.sha256(wheel)},
            ):
                self.assertEqual(self.run_promotion(module, snapshot, output), 0)
            self.assertEqual(list(output.iterdir()), [])

    def test_registry_content_drift_fails(self) -> None:
        module = load_module()
        with tempfile.TemporaryDirectory() as raw_tmp:
            root = Path(raw_tmp)
            snapshot, output = root / "snapshot", root / "output"
            snapshot.mkdir()
            wheel = snapshot / "antfly_cli-1.2.3-py3-none-any.whl"
            write_wheel(wheel)
            with (
                mock.patch.object(
                    module, "pypi_release_files", return_value={wheel.name: "0" * 64}
                ),
                self.assertRaisesRegex(SystemExit, "different contents"),
            ):
                self.run_promotion(module, snapshot, output)

    def test_expected_python_registry_version_is_enforced(self) -> None:
        module = load_module()
        with tempfile.TemporaryDirectory() as raw_tmp:
            root = Path(raw_tmp)
            snapshot, output = root / "snapshot", root / "output"
            snapshot.mkdir()
            write_wheel(snapshot / "antfly_cli-1.2.3-py3-none-any.whl")
            with self.assertRaisesRegex(SystemExit, "Python package version differs"):
                self.run_promotion(module, snapshot, output, expected_version="1.2.4")

    def test_untracked_registry_file_fails(self) -> None:
        module = load_module()
        with tempfile.TemporaryDirectory() as raw_tmp:
            root = Path(raw_tmp)
            snapshot, output = root / "snapshot", root / "output"
            snapshot.mkdir()
            wheel = snapshot / "antfly_cli-1.2.3-py3-none-any.whl"
            write_wheel(wheel)
            registry_files = {
                wheel.name: module.sha256(wheel),
                "antfly_cli-1.2.3.tar.gz": "a" * 64,
            }
            with (
                mock.patch.object(
                    module, "pypi_release_files", return_value=registry_files
                ),
                self.assertRaisesRegex(SystemExit, "absent from the release ledger"),
            ):
                self.run_promotion(module, snapshot, output)

    def test_partial_registry_release_prepares_only_missing_files(self) -> None:
        module = load_module()
        with tempfile.TemporaryDirectory() as raw_tmp:
            root = Path(raw_tmp)
            snapshot, output = root / "snapshot", root / "output"
            snapshot.mkdir()
            existing = snapshot / "antfly_cli-1.2.3-py3-none-first.whl"
            missing = snapshot / "antfly_cli-1.2.3-py3-none-second.whl"
            write_wheel(existing)
            write_wheel(missing)
            with mock.patch.object(
                module,
                "pypi_release_files",
                return_value={existing.name: module.sha256(existing)},
            ):
                self.assertEqual(self.run_promotion(module, snapshot, output), 0)
            self.assertEqual([path.name for path in output.iterdir()], [missing.name])

    def test_complete_registry_release_is_verified(self) -> None:
        module = load_module()
        with tempfile.TemporaryDirectory() as raw_tmp:
            snapshot = Path(raw_tmp) / "snapshot"
            snapshot.mkdir()
            wheel = snapshot / "antfly_cli-1.2.3-py3-none-any.whl"
            write_wheel(wheel)
            with mock.patch.object(
                module,
                "pypi_release_files",
                return_value={wheel.name: module.sha256(wheel)},
            ):
                self.assertEqual(
                    self.run_promotion(module, snapshot, verify_complete=True), 0
                )

    def test_complete_verification_waits_for_registry_visibility(self) -> None:
        module = load_module()
        with tempfile.TemporaryDirectory() as raw_tmp:
            snapshot = Path(raw_tmp) / "snapshot"
            snapshot.mkdir()
            wheel = snapshot / "antfly_cli-1.2.3-py3-none-any.whl"
            write_wheel(wheel)
            with (
                mock.patch.object(
                    module,
                    "pypi_release_files",
                    side_effect=[{}, {wheel.name: module.sha256(wheel)}],
                ) as lookup,
                mock.patch.object(module.time, "sleep") as sleep,
            ):
                self.assertEqual(
                    self.run_promotion(
                        module, snapshot, verify_complete=True, attempts=2
                    ),
                    0,
                )
            self.assertEqual(lookup.call_count, 2)
            sleep.assert_called_once_with(0.0)

    def test_complete_verification_rejects_missing_file(self) -> None:
        module = load_module()
        with tempfile.TemporaryDirectory() as raw_tmp:
            snapshot = Path(raw_tmp) / "snapshot"
            snapshot.mkdir()
            wheel = snapshot / "antfly_cli-1.2.3-py3-none-any.whl"
            write_wheel(wheel)
            with (
                mock.patch.object(module, "pypi_release_files", return_value={}),
                self.assertRaisesRegex(SystemExit, "missing release-ledger files"),
            ):
                self.run_promotion(module, snapshot, verify_complete=True)

    def test_malformed_registry_file_metadata_fails_closed(self) -> None:
        module = load_module()
        malformed_payloads = (
            {},
            {"urls": [None]},
            {"urls": [{"filename": "artifact.whl", "digests": {}}]},
            {"urls": [{"filename": "artifact.whl", "digests": {"sha256": "bad"}}]},
        )
        for payload in malformed_payloads:
            with (
                self.subTest(payload=payload),
                self.assertRaisesRegex(SystemExit, "malformed"),
            ):
                module.parse_pypi_release_files(payload)

    def test_duplicate_registry_filename_fails_closed(self) -> None:
        module = load_module()
        entry = {"filename": "artifact.whl", "digests": {"sha256": "a" * 64}}
        with self.assertRaisesRegex(SystemExit, "duplicate release file"):
            module.parse_pypi_release_files({"urls": [entry, entry]})


if __name__ == "__main__":
    unittest.main()
