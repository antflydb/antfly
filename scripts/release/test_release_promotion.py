#!/usr/bin/env python3
"""Tests for immutable release storage and the unified release ledger."""

from __future__ import annotations

import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

RELEASE_DIR = Path(__file__).resolve().parent
COMMIT = "0123456789abcdef0123456789abcdef01234567"


def load_module(name: str, filename: str):
    path = RELEASE_DIR / filename
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"failed to load {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class ReleasePromotionTests(unittest.TestCase):
    def test_github_asset_is_compare_or_fail(self) -> None:
        github = load_module("create_github_release_test", "create_github_release.py")
        with tempfile.TemporaryDirectory() as raw_tmp:
            artifact = Path(raw_tmp) / "artifact.bin"
            artifact.write_bytes(b"immutable")
            release = {
                "id": 1,
                "assets": [
                    {
                        "name": artifact.name,
                        "digest": f"sha256:{github.sha256(artifact)}",
                    }
                ],
            }
            with mock.patch.object(github, "request_bytes") as upload:
                github.upload_asset(
                    "antflydb/antfly", release, artifact, "token", False, True
                )
                upload.assert_not_called()

            release["assets"][0]["digest"] = f"sha256:{'0' * 64}"
            with self.assertRaisesRegex(SystemExit, "immutable release asset differs"):
                github.upload_asset(
                    "antflydb/antfly", release, artifact, "token", False, True
                )

    def test_local_versioned_object_is_compare_or_fail(self) -> None:
        storage = load_module("publish_objectstorage_test", "publish_objectstorage.py")
        with tempfile.TemporaryDirectory() as raw_tmp:
            root = Path(raw_tmp)
            artifact = root / "artifact.bin"
            artifact.write_bytes(b"immutable")
            publisher = storage.LocalPublisher(root / "objects", "releases")
            publisher.upload(artifact, "v1/artifact.bin", False, immutable=True)
            publisher.upload(artifact, "v1/artifact.bin", False, immutable=True)
            artifact.write_bytes(b"different")
            with self.assertRaisesRegex(SystemExit, "immutable object differs"):
                publisher.upload(artifact, "v1/artifact.bin", False, immutable=True)

    def test_mutable_aliases_wait_for_every_immutable_object(self) -> None:
        storage = load_module(
            "publish_objectstorage_order_test", "publish_objectstorage.py"
        )

        class RecordingPublisher:
            def __init__(self) -> None:
                self.calls: list[tuple[str, bool]] = []

            def upload(
                self, path: Path, key: str, dry_run: bool, immutable: bool = False
            ) -> None:
                self.calls.append((key, immutable))
                if key == "versions/v1/artifact-b.bin":
                    raise SystemExit("immutable object differs")

        with tempfile.TemporaryDirectory() as raw_tmp:
            root = Path(raw_tmp)
            first, second = root / "artifact-a.bin", root / "artifact-b.bin"
            first.write_bytes(b"a")
            second.write_bytes(b"b")
            publisher = RecordingPublisher()
            argv = [
                "publish_objectstorage.py",
                "--provider",
                "local",
                "--bucket",
                "release",
                "--prefix",
                "versions/v1",
                "--content-addressed-prefix",
                "artifacts",
                "--latest-prefix",
                "channels/latest",
                "--publish-latest",
                str(first),
                str(second),
            ]
            with (
                mock.patch.object(sys, "argv", argv),
                mock.patch.object(storage, "build_publisher", return_value=publisher),
                self.assertRaisesRegex(SystemExit, "immutable object differs"),
            ):
                storage.main()

            self.assertFalse(
                any(key.startswith("channels/latest/") for key, _ in publisher.calls)
            )
            self.assertTrue(all(immutable for _, immutable in publisher.calls))

    def test_release_ledger_is_deterministic_and_includes_registry_artifacts(
        self,
    ) -> None:
        payload = load_module("build_release_payload_test", "build_release_payload.py")
        with tempfile.TemporaryDirectory() as raw_tmp:
            root = Path(raw_tmp)
            archives, extras, output = (
                root / "archives",
                root / "extras",
                root / "output",
            )
            archives.mkdir()
            extras.mkdir()
            (archives / "antfly_1.2.3_Linux_x86_64_gnu.tar.gz").write_bytes(b"native")
            (extras / "antfly-cli-1.2.3.tgz").write_bytes(b"npm")
            (extras / "cli-snapshot.json").write_text(
                json.dumps(
                    {
                        "version": "1.2.3",
                        "commit": COMMIT,
                        "registry_versions": {"npm": "1.2.3", "python": "1.2.3"},
                    }
                )
            )
            argv = [
                "build_release_payload.py",
                "--tag",
                "v1.2.3",
                "--commit",
                COMMIT,
                "--archive-dir",
                str(archives),
                "--extra-dir",
                str(extras),
                "--out-dir",
                str(output),
            ]
            with (
                mock.patch.object(sys, "argv", argv),
                mock.patch.dict("os.environ", {"SOURCE_DATE_EPOCH": "0"}),
            ):
                self.assertEqual(payload.main(), 0)
            ledger = json.loads((output / "artifacts.json").read_text())
            self.assertEqual(ledger["schema_version"], 1)
            self.assertEqual(ledger["generated_at"], "1970-01-01T00:00:00Z")
            self.assertEqual(
                ledger["registry_versions"], {"npm": "1.2.3", "python": "1.2.3"}
            )
            kinds = {artifact["kind"] for artifact in ledger["artifacts"]}
            self.assertIn("runtime-archive", kinds)
            self.assertIn("npm-package", kinds)
            self.assertIn("cli-manifest", kinds)


if __name__ == "__main__":
    unittest.main()
