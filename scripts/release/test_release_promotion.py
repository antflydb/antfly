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

    def test_s3_immutable_object_uses_atomic_create_and_compare(self) -> None:
        storage = load_module(
            "publish_objectstorage_s3_test", "publish_objectstorage.py"
        )

        class ConditionalConflict(Exception):
            def __init__(self) -> None:
                self.response = {
                    "Error": {"Code": "PreconditionFailed"},
                    "ResponseMetadata": {"HTTPStatusCode": 412},
                }

        class FakeS3:
            def __init__(self, digest: str) -> None:
                self.digest = digest
                self.put_args = None

            def put_object(self, **kwargs):
                self.put_args = kwargs
                raise ConditionalConflict()

            def head_object(self, **_kwargs):
                return {"Metadata": {"sha256": self.digest}}

        with tempfile.TemporaryDirectory() as raw_tmp:
            artifact = Path(raw_tmp) / "artifact.bin"
            artifact.write_bytes(b"immutable")
            client = FakeS3(storage.sha256(artifact))
            publisher = storage.S3Publisher.__new__(storage.S3Publisher)
            publisher.bucket = "releases"
            publisher.client = client
            publisher.client_error = ConditionalConflict

            publisher.upload(artifact, "v1/artifact.bin", False, immutable=True)
            self.assertEqual(client.put_args["IfNoneMatch"], "*")

            client.digest = "0" * 64
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
            first, second, metadata = (
                root / "artifact-a.bin",
                root / "artifact-b.bin",
                root / "metadata.json",
            )
            first.write_bytes(b"a")
            second.write_bytes(b"b")
            metadata.write_text('{"tag":"v1"}\n')
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
                str(metadata),
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

    def test_latest_metadata_pointer_is_published_last(self) -> None:
        storage = load_module(
            "publish_objectstorage_pointer_test", "publish_objectstorage.py"
        )

        class RecordingPublisher:
            def __init__(self) -> None:
                self.calls: list[tuple[str, bool]] = []

            def upload(
                self, path: Path, key: str, dry_run: bool, immutable: bool = False
            ) -> None:
                self.calls.append((key, immutable))

        with tempfile.TemporaryDirectory() as raw_tmp:
            root = Path(raw_tmp)
            artifact, metadata = root / "artifact.bin", root / "metadata.json"
            artifact.write_bytes(b"artifact")
            metadata.write_text('{"tag":"v1"}\n')
            publisher = RecordingPublisher()
            argv = [
                "publish_objectstorage.py",
                "--provider",
                "local",
                "--bucket",
                "release",
                "--prefix",
                "versions/v1",
                "--latest-prefix",
                "channels/latest",
                "--publish-latest",
                str(artifact),
                str(metadata),
            ]
            with (
                mock.patch.object(sys, "argv", argv),
                mock.patch.object(storage, "build_publisher", return_value=publisher),
            ):
                self.assertEqual(storage.main(), 0)

            self.assertEqual(
                publisher.calls[-1], ("channels/latest/metadata.json", False)
            )
            immutable_calls = [call for call in publisher.calls if call[1]]
            self.assertEqual(len(immutable_calls), 2)

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
            self.assertEqual(ledger["schema_version"], 2)
            self.assertEqual(ledger["generated_at"], "1970-01-01T00:00:00Z")
            self.assertEqual(
                ledger["registry_versions"], {"npm": "1.2.3", "python": "1.2.3"}
            )
            kinds = {artifact["kind"] for artifact in ledger["artifacts"]}
            self.assertIn("runtime-archive", kinds)
            self.assertIn("npm-package", kinds)
            self.assertIn("cli-manifest", kinds)
            scopes = {artifact["scope"] for artifact in ledger["artifacts"]}
            self.assertEqual(scopes, {"runtime", "cli", "support"})

            verifier = load_module(
                "verify_release_ledger_test", "verify_release_ledger.py"
            )
            promotion = root / "promotion"
            promotion.mkdir()
            promoted_package = promotion / "antfly-cli-1.2.3.tgz"
            promoted_package.write_bytes((output / promoted_package.name).read_bytes())
            promoted_manifest = promotion / "cli-snapshot.json"
            promoted_manifest.write_bytes(
                (output / promoted_manifest.name).read_bytes()
            )
            verify_argv = [
                "verify_release_ledger.py",
                "--ledger",
                str(output / "artifacts.json"),
                "--payload-dir",
                str(promotion),
                "--scope",
                "cli",
                "--tag",
                "v1.2.3",
                "--commit",
                COMMIT,
                "--ledger-sha256",
                verifier.sha256(output / "artifacts.json"),
            ]
            with mock.patch.object(sys, "argv", verify_argv):
                self.assertEqual(verifier.main(), 0)

            # Control-plane files live in the ledger scope, never beside CLI
            # payload bytes. This covers the production recovery layout.
            misplaced_ledger = promotion / "artifacts.json"
            misplaced_ledger.write_bytes((output / "artifacts.json").read_bytes())
            with (
                mock.patch.object(sys, "argv", verify_argv),
                self.assertRaisesRegex(SystemExit, "release cli scope mismatch"),
            ):
                verifier.main()
            misplaced_ledger.unlink()

            promoted_package.write_bytes(b"drift")
            with (
                mock.patch.object(sys, "argv", verify_argv),
                self.assertRaisesRegex(
                    SystemExit, "promoted artifact differs from release ledger"
                ),
            ):
                verifier.main()


if __name__ == "__main__":
    unittest.main()
