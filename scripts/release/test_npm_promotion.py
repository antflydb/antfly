#!/usr/bin/env python3
"""Tests for read-only npm promotion preflight."""

from __future__ import annotations

import base64
import hashlib
import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

SCRIPT = Path(__file__).with_name("prepare_npm_promotion.py")


def load_module():
    spec = importlib.util.spec_from_file_location("prepare_npm_promotion", SCRIPT)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"failed to load {SCRIPT}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class NpmPromotionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.tarball = Path(self.tmp.name) / "package.tgz"
        self.tarball.write_bytes(b"immutable npm package")
        digest = hashlib.sha512(self.tarball.read_bytes()).digest()
        self.integrity = "sha512-" + base64.b64encode(digest).decode("ascii")

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def write_snapshot(self, module, version: str) -> Path:
        root = Path(self.tmp.name)
        artifacts = []
        for package, stem in module.NPM_PACKAGES.items():
            name = f"{stem}-{version}.tgz"
            (root / name).write_bytes(package.encode())
            artifacts.append({"kind": "npm", "package": package, "name": name})
        manifest = {
            "schema_version": 1,
            "version": version,
            "registry_versions": {"npm": version, "python": "unused"},
            "artifacts": artifacts,
        }
        (root / "cli-snapshot.json").write_text(json.dumps(manifest))
        return root

    def test_missing_version_passes_without_reading_mutable_tag(self) -> None:
        module = load_module()

        def unexpected_tag(_package: str, _tag: str) -> str | None:
            raise AssertionError("a missing immutable version has no tag constraint")

        module.preflight_package(
            "@antfly/cli",
            "1.2.3",
            "latest",
            self.tarball,
            lambda _package, _version: None,
            unexpected_tag,
        )

    def test_identical_version_and_tag_are_resumable(self) -> None:
        module = load_module()
        module.preflight_package(
            "@antfly/cli",
            "1.2.3",
            "latest",
            self.tarball,
            lambda _package, _version: self.integrity,
            lambda _package, _tag: "1.2.3",
        )

    def test_existing_version_content_drift_fails(self) -> None:
        module = load_module()
        with self.assertRaisesRegex(SystemExit, "different contents"):
            module.preflight_package(
                "@antfly/cli",
                "1.2.3",
                "latest",
                self.tarball,
                lambda _package, _version: "sha512-different",
                lambda _package, _tag: "1.2.3",
            )

    def test_existing_version_tag_drift_fails(self) -> None:
        module = load_module()
        with self.assertRaisesRegex(SystemExit, "dist-tag latest points to 1.2.2"):
            module.preflight_package(
                "@antfly/cli",
                "1.2.3",
                "latest",
                self.tarball,
                lambda _package, _version: self.integrity,
                lambda _package, _tag: "1.2.2",
            )

    def test_snapshot_requires_the_exact_npm_package_set(self) -> None:
        module = load_module()
        root = self.write_snapshot(module, "1.2.3")
        self.assertEqual(
            set(module.snapshot_packages(root, "1.2.3")), set(module.NPM_PACKAGES)
        )

        manifest = json.loads((root / "cli-snapshot.json").read_text())
        manifest["artifacts"].pop()
        (root / "cli-snapshot.json").write_text(json.dumps(manifest))
        with self.assertRaisesRegex(SystemExit, "package set differs"):
            module.snapshot_packages(root, "1.2.3")

    def test_snapshot_accepts_an_authenticated_legacy_registry_version(self) -> None:
        module = load_module()
        root = self.write_snapshot(module, "1.2.3-rc2")
        self.assertEqual(
            set(module.snapshot_packages(root, "1.2.3-rc2")),
            set(module.NPM_PACKAGES),
        )
        with (
            mock.patch.object(
                sys,
                "argv",
                [
                    str(SCRIPT),
                    "--snapshot-dir",
                    str(root),
                    "--version",
                    "1.2.3-rc2",
                    "--tag",
                    "next",
                ],
            ),
            mock.patch.object(module, "preflight_package") as preflight,
        ):
            self.assertEqual(module.main(), 0)
        self.assertEqual(preflight.call_count, len(module.NPM_PACKAGES))


if __name__ == "__main__":
    unittest.main()
