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
    def run_promotion(self, module, snapshot: Path, output: Path) -> int:
        argv = [
            str(SCRIPT),
            "--project",
            "antfly-cli",
            "--snapshot-dir",
            str(snapshot),
            "--out-dir",
            str(output),
        ]
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


if __name__ == "__main__":
    unittest.main()
