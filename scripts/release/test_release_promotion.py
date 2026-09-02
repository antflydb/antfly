#!/usr/bin/env python3
"""Tests for immutable release storage and the unified release ledger."""

from __future__ import annotations

import importlib.util
import io
import json
import sys
import tempfile
import unittest
import urllib.error
from pathlib import Path
from unittest import mock

RELEASE_DIR = Path(__file__).resolve().parent
COMMIT = "0123456789abcdef0123456789abcdef01234567"
sys.path.insert(0, str(RELEASE_DIR))


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
    def test_source_commit_must_declare_a_supported_build_contract(self) -> None:
        contract = load_module(
            "validate_source_contract_test", "validate_source_contract.py"
        )
        document = {
            "schema_version": 1,
            "required_source_paths": sorted(contract.REQUIRED_PATHS),
        }

        def read_object(_root: Path, _commit: str, path: str) -> bytes:
            if path == contract.CONTRACT_PATH:
                return json.dumps(document).encode()
            return b"present"

        with mock.patch.object(contract, "git_object", side_effect=read_object):
            self.assertEqual(contract.validate(RELEASE_DIR, COMMIT), 1)

        document["schema_version"] = 999
        with (
            mock.patch.object(contract, "git_object", side_effect=read_object),
            self.assertRaisesRegex(SystemExit, "unsupported release build contract"),
        ):
            contract.validate(RELEASE_DIR, COMMIT)

    def test_release_source_snapshot_is_extracted_from_the_exact_commit(self) -> None:
        stage = load_module("stage_release_source_test", "stage_release_source.py")
        with tempfile.TemporaryDirectory() as raw_tmp:
            output = Path(raw_tmp) / "source"
            objects = {
                "scripts/install.sh": b"#!/bin/sh\n",
                "openapi.yaml": b"openapi: 3.1.0\n",
            }
            with mock.patch.object(
                stage,
                "git_object",
                side_effect=lambda _root, commit, source: (
                    objects[source] if commit == COMMIT else b"wrong commit"
                ),
            ):
                manifest_path = stage.stage_source(RELEASE_DIR, COMMIT, output)

            manifest = json.loads(manifest_path.read_text())
            self.assertEqual(manifest["commit"], COMMIT)
            self.assertEqual(
                {entry["name"] for entry in manifest["artifacts"]},
                {"install.sh", "openapi.yaml"},
            )
            self.assertEqual(
                (output / "install.sh").read_bytes(), objects["scripts/install.sh"]
            )

    def test_release_channel_policy_resolves_all_supported_channels(self) -> None:
        channels = load_module("release_channels_test", "release_channels.py")
        policy = channels.load_policy()

        stable_name, stable = channels.resolve_channel("v1.2.3", "auto", policy)
        next_name, next_channel = channels.resolve_channel(
            "v1.3.0-rc.2", "auto", policy
        )
        nightly_name, nightly = channels.resolve_channel(
            "v0.0.0-dev.123", "nightly", policy
        )

        self.assertEqual((stable_name, stable["npm_tag"]), ("stable", "latest"))
        self.assertEqual((next_name, next_channel["npm_tag"]), ("next", "next"))
        self.assertEqual(
            (nightly_name, nightly["npm_tag"], nightly["github_release"]),
            ("nightly", "nightly", "none"),
        )
        self.assertGreater(
            channels.compare_channel_tags(
                "v0.0.0-dev.10", "v0.0.0-dev.2", "nightly", policy
            ),
            0,
        )
        identity = channels.github_outputs(next_name, next_channel, "v1.3.0-rc.2")
        self.assertEqual(
            {
                key: identity[key]
                for key in ("npm_version", "python_version", "container_tag")
            },
            {
                "npm_version": "1.3.0-rc.2",
                "python_version": "1.3.0rc2",
                "container_tag": "v1.3.0-rc.2",
            },
        )
        with self.assertRaisesRegex(SystemExit, "requires the nightly channel"):
            channels.resolve_channel("v0.0.0-dev.123", "auto", policy)
        for legacy in ("v1.2.3-rc2", "v1.2.3-pre.2", "v1.2.3-preview2"):
            with (
                self.subTest(legacy=legacy),
                self.assertRaisesRegex(SystemExit, "canonical tag syntax"),
            ):
                channels.resolve_channel(legacy, "next", policy)
            self.assertEqual(
                channels.resolve_channel(legacy, "next", policy, allow_legacy=True)[0],
                "next",
            )
        for unsupported in (
            "v1.2",
            "v1.2.3+build.1",
            "v1.2.3-experimental.1",
        ):
            with self.subTest(unsupported=unsupported), self.assertRaises(SystemExit):
                channels.resolve_channel(unsupported, "auto", policy)

    def test_channel_discovery_only_treats_missing_aliases_as_empty(self) -> None:
        discovery = load_module("discover_channel_tag_test", "discover_channel_tag.py")

        class Response(io.BytesIO):
            def __enter__(self):
                return self

            def __exit__(self, *_args):
                self.close()

        def response(document: dict):
            return Response(json.dumps(document).encode())

        self.assertEqual(
            discovery.discover_github_latest(
                "antflydb/antfly",
                "token",
                lambda *_args, **_kwargs: response({"tag_name": "v1.2.3"}),
            ),
            "v1.2.3",
        )
        self.assertEqual(
            discovery.discover_npm_tag(
                "@antfly/cli",
                "next",
                lambda *_args, **_kwargs: response(
                    {"dist-tags": {"next": "1.3.0-rc.2"}}
                ),
            ),
            "v1.3.0-rc.2",
        )
        self.assertEqual(
            discovery.discover_npm_integrity(
                "@antfly/cli",
                "1.3.0-rc.2",
                lambda *_args, **_kwargs: response(
                    {
                        "versions": {
                            "1.3.0-rc.2": {"dist": {"integrity": "sha512-exact"}}
                        }
                    }
                ),
            ),
            "sha512-exact",
        )

        def http_error(code: int):
            raise urllib.error.HTTPError("https://registry", code, "error", {}, None)

        self.assertEqual(
            discovery.discover_npm_tag(
                "@antfly/cli", "nightly", lambda *_args, **_kwargs: http_error(404)
            ),
            "",
        )
        with self.assertRaisesRegex(SystemExit, "HTTP 500"):
            discovery.discover_npm_tag(
                "@antfly/cli", "next", lambda *_args, **_kwargs: http_error(500)
            )

    def test_object_storage_recovery_restores_exact_ledger_members(self) -> None:
        download = load_module(
            "download_objectstorage_test", "download_objectstorage.py"
        )
        with tempfile.TemporaryDirectory() as raw_tmp:
            root = Path(raw_tmp)
            prefix = root / "release" / "antfly" / "v0.0.0-dev.123"
            prefix.mkdir(parents=True)
            ledger = {
                "schema_version": 2,
                "artifacts": [
                    {"name": "metadata.json"},
                    {"name": "artifact.tgz"},
                ],
            }
            (prefix / "artifacts.json").write_text(json.dumps(ledger))
            (prefix / "metadata.json").write_text('{"tag":"v0.0.0-dev.123"}')
            (prefix / "artifact.tgz").write_bytes(b"snapshot")
            out_dir = root / "restored"

            download.restore_payload(
                download.LocalReader(root, "release"),
                "antfly/v0.0.0-dev.123",
                out_dir,
            )

            self.assertEqual(
                {path.name for path in out_dir.iterdir()},
                {"artifacts.json", "metadata.json", "artifact.tgz"},
            )
            self.assertEqual((out_dir / "artifact.tgz").read_bytes(), b"snapshot")

    def test_release_channel_store_uses_atomic_create_and_update(self) -> None:
        channel = load_module("release_channel_store_test", "release_channel_state.py")

        class FakeClient:
            def __init__(self) -> None:
                self.requests: list[dict] = []

            def put_object(self, **request) -> None:
                self.requests.append(request)

        store = channel.S3ChannelStore.__new__(channel.S3ChannelStore)
        store.bucket = "releases"
        store.key = "antfly/channels/stable.json"
        store.client = FakeClient()
        store.client_error = Exception
        document = {"schema_version": 1, "current": None, "pending": None}

        store.compare_and_swap(channel.StoredState(document, None), document)
        self.assertEqual(store.client.requests[-1]["IfNoneMatch"], "*")
        self.assertNotIn("IfMatch", store.client.requests[-1])

        store.compare_and_swap(channel.StoredState(document, '"etag"'), document)
        self.assertEqual(store.client.requests[-1]["IfMatch"], '"etag"')
        self.assertNotIn("IfNoneMatch", store.client.requests[-1])

    def test_published_github_release_never_returns_to_draft(self) -> None:
        github = load_module(
            "create_github_release_state_test", "create_github_release.py"
        )
        published = {"id": 1, "draft": False, "assets": []}
        with (
            mock.patch.object(github, "get_release_by_tag", return_value=published),
            mock.patch.object(github, "github_api") as api,
        ):
            actual = github.create_or_update_release(
                "antflydb/antfly", "v1.2.3", "token", {"draft": True}
            )

        self.assertIs(actual, published)
        api.assert_not_called()

    def test_stable_channel_transaction_is_monotonic_and_resumable(self) -> None:
        channel = load_module("release_channel_state_test", "release_channel_state.py")

        class MemoryStore:
            def __init__(self, document: dict) -> None:
                self.document = document
                self.etag = "1"
                self.writes = 0

            def load(self):
                return channel.StoredState(
                    json.loads(json.dumps(self.document)), self.etag
                )

            def compare_and_swap(self, previous, document: dict) -> None:
                self.assert_etag(previous.etag)
                self.document = json.loads(json.dumps(document))
                self.writes += 1
                self.etag = str(self.writes + 1)

            def assert_etag(self, etag: str) -> None:
                if etag != self.etag:
                    raise AssertionError("stale channel state")

        previous = {
            "tag": "v1.2.2",
            "commit": "1" * 40,
            "ledger_sha256": "2" * 64,
        }
        candidate = channel.release_identity(
            "v1.2.3",
            COMMIT,
            "3" * 64,
            container_digest=f"sha256:{'a' * 64}",
        )
        store = MemoryStore({"schema_version": 1, "current": previous, "pending": None})

        channel.begin_promotion(store, candidate, None)
        self.assertEqual(store.document["pending"], candidate)
        self.assertEqual(store.writes, 1)
        channel.begin_promotion(store, candidate, None)
        self.assertEqual(store.writes, 1, "same transaction should resume")
        channel.finish_promotion(store, candidate)
        self.assertEqual(
            store.document,
            {
                "schema_version": 1,
                "channel": "stable",
                "current": candidate,
                "pending": None,
            },
        )
        channel.finish_promotion(store, candidate)
        self.assertEqual(store.writes, 2, "committed transaction should replay")
        self.assertEqual(
            channel.journaled_container_digest(store, candidate),
            f"sha256:{'a' * 64}",
        )

        older = channel.release_identity("v1.2.1", "4" * 40, "5" * 64)
        with self.assertRaisesRegex(SystemExit, "cannot move backward"):
            channel.begin_promotion(store, older, None)

    def test_stable_channel_blocks_a_different_incomplete_promotion(self) -> None:
        channel = load_module(
            "release_channel_pending_test", "release_channel_state.py"
        )

        class MemoryStore:
            def __init__(self, document: dict) -> None:
                self.document = document

            def load(self):
                return channel.StoredState(self.document, "1")

            def compare_and_swap(self, _previous, document: dict) -> None:
                self.document = document

        pending = channel.release_identity("v1.2.3", COMMIT, "3" * 64)
        store = MemoryStore({"schema_version": 1, "current": None, "pending": pending})
        different = channel.release_identity("v1.2.4", "4" * 40, "5" * 64)
        with self.assertRaisesRegex(SystemExit, "is incomplete"):
            channel.begin_promotion(store, different, None)

    def test_stable_channel_upgrades_a_legacy_pending_container_identity(self) -> None:
        channel = load_module(
            "release_channel_pending_upgrade_test", "release_channel_state.py"
        )

        class MemoryStore:
            def __init__(self, document: dict) -> None:
                self.document = document

            def load(self):
                return channel.StoredState(self.document, "1")

            def compare_and_swap(self, _previous, document: dict) -> None:
                self.document = document

        legacy = channel.release_identity("v1.2.3", COMMIT, "3" * 64)
        candidate = channel.release_identity(
            "v1.2.3",
            COMMIT,
            "3" * 64,
            container_digest=f"sha256:{'a' * 64}",
        )
        store = MemoryStore({"schema_version": 1, "current": None, "pending": legacy})
        channel.begin_promotion(store, candidate, None)
        self.assertEqual(store.document["pending"], candidate)

    def test_stable_channel_rejects_observed_alias_drift(self) -> None:
        channel = load_module("release_channel_alias_test", "release_channel_state.py")

        class MemoryStore:
            def load(self):
                return channel.StoredState(
                    {
                        "schema_version": 1,
                        "current": {"tag": "v1.2.3"},
                        "pending": None,
                    },
                    "1",
                )

            def compare_and_swap(self, _previous, _document: dict) -> None:
                raise AssertionError("drifted aliases must not be promoted")

        candidate = channel.release_identity("v1.2.5", COMMIT, "3" * 64)
        with self.assertRaisesRegex(SystemExit, "observed alias is v1.2.4"):
            channel.begin_promotion(MemoryStore(), candidate, "v1.2.4")

    def test_stable_channel_rejects_identity_drift_for_current_tag(self) -> None:
        channel = load_module(
            "release_channel_identity_test", "release_channel_state.py"
        )

        class MemoryStore:
            def load(self):
                return channel.StoredState(
                    {
                        "schema_version": 1,
                        "current": {
                            "tag": "v1.2.3",
                            "commit": COMMIT,
                            "ledger_sha256": "3" * 64,
                        },
                        "pending": None,
                    },
                    "1",
                )

            def compare_and_swap(self, _previous, _document: dict) -> None:
                raise AssertionError("identity drift must not be written")

        drifted = channel.release_identity("v1.2.3", COMMIT, "4" * 64)
        with self.assertRaisesRegex(SystemExit, "different ledger_sha256"):
            channel.begin_promotion(MemoryStore(), drifted, None)

    def test_stable_channel_rejects_container_digest_drift(self) -> None:
        channel = load_module(
            "release_channel_container_test", "release_channel_state.py"
        )
        current = channel.release_identity(
            "v1.2.3",
            COMMIT,
            "3" * 64,
            container_digest=f"sha256:{'a' * 64}",
        )

        class MemoryStore:
            def load(self):
                return channel.StoredState(
                    {"schema_version": 1, "current": current, "pending": None},
                    "1",
                )

            def compare_and_swap(self, _previous, _document: dict) -> None:
                raise AssertionError("container drift must not be written")

        drifted = channel.release_identity(
            "v1.2.3",
            COMMIT,
            "3" * 64,
            container_digest=f"sha256:{'b' * 64}",
        )
        with self.assertRaisesRegex(SystemExit, "different container_digest"):
            channel.begin_promotion(MemoryStore(), drifted, None)

    def test_next_channel_uses_semver_prerelease_precedence(self) -> None:
        channel = load_module("release_channel_next_test", "release_channel_state.py")

        class MemoryStore:
            def __init__(self) -> None:
                self.document = {
                    "schema_version": 1,
                    "current": {"tag": "v1.3.0-rc.2"},
                    "pending": None,
                }

            def load(self):
                return channel.StoredState(self.document, "1")

            def compare_and_swap(self, _previous, document: dict) -> None:
                self.document = document

        candidate = channel.release_identity("v1.3.0-rc.10", COMMIT, "3" * 64, "next")
        store = MemoryStore()
        channel.begin_promotion(store, candidate, None, "next")
        self.assertEqual(store.document["pending"], candidate)

        store.document = {
            "schema_version": 1,
            "current": {"tag": "v1.3.0-rc.10"},
            "pending": None,
        }
        older = channel.release_identity("v1.3.0-rc.3", "4" * 40, "5" * 64, "next")
        with self.assertRaisesRegex(SystemExit, "cannot move backward"):
            channel.begin_promotion(store, older, None, "next")

        store.document = {
            "schema_version": 1,
            "current": {"tag": "v1.3.0-rc10"},
            "pending": None,
        }
        with self.assertRaisesRegex(SystemExit, "precedence collision"):
            channel.begin_promotion(store, candidate, None, "next")

    def test_nightly_channel_uses_numeric_run_sequence(self) -> None:
        channel = load_module(
            "release_channel_nightly_test", "release_channel_state.py"
        )

        class MemoryStore:
            def __init__(self) -> None:
                self.document = {
                    "schema_version": 1,
                    "channel": "nightly",
                    "current": {"tag": "v0.0.0-dev.9"},
                    "pending": None,
                }

            def load(self):
                return channel.StoredState(self.document, "1")

            def compare_and_swap(self, _previous, document: dict) -> None:
                self.document = document

        candidate = channel.release_identity(
            "v0.0.0-dev.10", COMMIT, "3" * 64, "nightly"
        )
        store = MemoryStore()
        channel.begin_promotion(store, candidate, None, "nightly")
        self.assertEqual(store.document["pending"], candidate)

        store.document = {
            "schema_version": 1,
            "channel": "nightly",
            "current": {"tag": "v0.0.0-dev.10"},
            "pending": None,
        }
        older = channel.release_identity("v0.0.0-dev.9", "4" * 40, "5" * 64, "nightly")
        with self.assertRaisesRegex(SystemExit, "cannot move backward"):
            channel.begin_promotion(store, older, None, "nightly")

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
            archives, extras, source, output = (
                root / "archives",
                root / "extras",
                root / "source",
                root / "output",
            )
            archives.mkdir()
            extras.mkdir()
            source.mkdir()
            (archives / "antfly_1.2.3_Linux_x86_64_gnu.tar.gz").write_bytes(b"native")
            (extras / "antfly-cli-1.2.3.tgz").write_bytes(b"npm")
            (extras / "cli-snapshot.json").write_text(
                json.dumps(
                    {
                        "version": "1.2.3",
                        "commit": COMMIT,
                        "registry_versions": {
                            "npm": "1.2.3",
                            "python": "1.2.3",
                            "container": "v1.2.3",
                        },
                    }
                )
            )
            source_artifacts = []
            for name, content in (
                ("install.sh", b"#!/bin/sh\n"),
                ("openapi.yaml", b"openapi: 3.1.0\n"),
            ):
                path = source / name
                path.write_bytes(content)
                source_artifacts.append(
                    {
                        "name": name,
                        "size": path.stat().st_size,
                        "sha256": payload.sha256(path),
                    }
                )
            (source / "source-snapshot.json").write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "commit": COMMIT,
                        "artifacts": source_artifacts,
                    }
                )
            )
            with self.assertRaisesRegex(SystemExit, "does not match"):
                payload.verify_source_snapshot(source, "f" * 40)
            release_spec = root / "release-request.json"
            release_spec.write_text(
                json.dumps(
                    {
                        "schema_version": 4,
                        "tag": "v1.2.3",
                        "version": "1.2.3",
                        "channel": "stable",
                        "source_commit": COMMIT,
                        "build_controller_commit": "f" * 40,
                        "build_contract_schema": 1,
                        "registry_versions": {
                            "npm": "1.2.3",
                            "python": "1.2.3",
                            "container": "v1.2.3",
                        },
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
                "--source-dir",
                str(source),
                "--out-dir",
                str(output),
                "--release-spec",
                str(release_spec),
                "--promotion-controller-commit",
                "e" * 40,
            ]
            with (
                mock.patch.object(sys, "argv", argv),
                mock.patch.dict("os.environ", {"SOURCE_DATE_EPOCH": "0"}),
            ):
                self.assertEqual(payload.main(), 0)
            ledger = json.loads((output / "artifacts.json").read_text())
            self.assertEqual(ledger["schema_version"], 4)
            self.assertEqual(ledger["build_controller_commit"], "f" * 40)
            self.assertEqual(ledger["promotion_controller_commit"], "e" * 40)
            self.assertEqual(ledger["generated_at"], "1970-01-01T00:00:00Z")
            self.assertEqual(
                ledger["registry_versions"],
                {"npm": "1.2.3", "python": "1.2.3", "container": "v1.2.3"},
            )
            kinds = {artifact["kind"] for artifact in ledger["artifacts"]}
            self.assertIn("runtime-archive", kinds)
            self.assertIn("npm-package", kinds)
            self.assertIn("cli-manifest", kinds)
            self.assertIn("source-manifest", kinds)
            self.assertIn("release-spec", kinds)
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
