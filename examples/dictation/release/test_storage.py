# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import base64
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import release as r
from storage import ObjectStorage
from objectstore_channel import ConditionalWriteConflict, LocalConditionalStore

KEY = base64.b64encode(bytes(range(32))).decode()
SIGNATURE = base64.b64encode(bytes(range(64))).decode()


class R2PublicationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.storage = ObjectStorage.local(self.root / "objects", r.POLICY["bucket"])

    def payload(self, tag="dictation/v1.0.0", key=KEY):
        identity = r.version(tag)
        identity.pop("order")
        identity.update(tag=tag, source_commit="a" * 40, controller_commit="b" * 40)
        directory = self.root / (
            identity["version"] + "-" + str(len(list(self.root.iterdir())))
        )
        directory.mkdir()
        archive = directory / (
            "Antfly-Dictation-" + identity["version"] + "-universal.zip"
        )
        archive.write_bytes(b"signed and notarized app fixture")
        (directory / "appcast.xml").write_bytes(r.appcast(identity, archive, SIGNATURE))
        (directory / "SHA256SUMS").write_text(
            "".join(
                r.digest(p) + "  " + p.name + "\n"
                for p in [archive, directory / "appcast.xml"]
            )
        )
        r.write_manifest(identity, directory, key)
        return identity, directory

    def publish(self, identity, directory):
        return r.publish(
            identity, directory, self.storage, source_verifier=lambda _: None
        )

    def feed(self, channel):
        return self.storage.reader.read_optional(
            f"antfly-dictation/channels/{channel}/appcast.xml"
        )

    def test_publish_seals_exact_versions_and_content_objects_before_both_feeds(self):
        identity, directory = self.payload()
        original = self.storage.channel_store

        def channel(key):
            self.storage.exact(
                r.release_prefix(identity), {p.name for p in directory.iterdir()}
            )
            return original(key)

        self.storage.channel_store = channel
        self.assertEqual(
            self.publish(identity, directory),
            {"stable": "committed", "beta": "committed"},
        )
        self.assertEqual(self.feed("stable"), (directory / "appcast.xml").read_bytes())
        self.assertEqual(self.feed("stable"), self.feed("beta"))
        for path in directory.iterdir():
            self.assertEqual(
                self.storage.reader.read(
                    f"antfly-dictation/artifacts/sha256/{r.digest(path)}/{path.name}"
                ),
                path.read_bytes(),
            )
        self.assertFalse(
            (self.root / "objects" / r.POLICY["bucket"] / "antfly").exists()
        )

    def test_retry_skips_all_completed_uploads(self):
        identity, directory = self.payload()
        self.publish(identity, directory)
        with patch.object(
            self.storage.publisher, "upload", side_effect=AssertionError("re-uploaded")
        ):
            self.publish(identity, directory)

    def test_beta_does_not_touch_stable_and_old_versions_cannot_downgrade(self):
        stable, first = self.payload("dictation/v2.0.0")
        self.publish(stable, first)
        beta, second = self.payload("dictation/v2.1.0-beta.1")
        self.publish(beta, second)
        self.assertEqual(self.feed("stable"), (first / "appcast.xml").read_bytes())
        old, third = self.payload()
        self.assertEqual(
            self.publish(old, third), {"stable": "superseded", "beta": "superseded"}
        )
        self.assertEqual(self.feed("beta"), (second / "appcast.xml").read_bytes())

    def test_same_version_different_bytes_cannot_replace_anything(self):
        identity, first = self.payload()
        self.publish(identity, first)
        _, second = self.payload()
        (second / "appcast.xml").write_bytes(
            (second / "appcast.xml").read_bytes() + b"\n"
        )
        (second / "artifacts.json").unlink()
        r.write_manifest(identity, second, KEY)
        with self.assertRaisesRegex(ValueError, "Immutable release object differs"):
            self.publish(identity, second)
        self.assertEqual(self.feed("stable"), (first / "appcast.xml").read_bytes())

    def test_new_version_cannot_rotate_established_signing_key(self):
        identity, directory = self.payload()
        self.publish(identity, directory)
        next_id, second = self.payload(
            "dictation/v1.1.0", base64.b64encode(bytes([9]) * 32).decode()
        )
        with self.assertRaisesRegex(ValueError, "Immutable"):
            self.publish(next_id, second)
        self.assertIsNone(
            self.storage.reader.read_optional(
                r.release_prefix(next_id) + "/artifacts.json"
            )
        )

    def test_interrupted_upload_resumes_without_advancing_partial_feed(self):
        identity, directory = self.payload()
        original = self.storage.upload_immutable

        def upload(path, key):
            if key.endswith("/appcast.xml"):
                raise ConnectionError("interrupted")
            original(path, key)

        with patch.object(self.storage, "upload_immutable", side_effect=upload):
            with self.assertRaises(ConnectionError):
                self.publish(identity, directory)
        self.assertIsNone(self.feed("stable"))
        self.assertIsNone(
            self.storage.reader.read_optional(
                r.release_prefix(identity) + "/artifacts.json"
            )
        )
        self.publish(identity, directory)
        self.assertIsNotNone(self.feed("stable"))

    def test_unexpected_remote_member_prevents_sealing(self):
        identity, directory = self.payload()
        extra = self.root / "extra"
        extra.write_bytes(b"extra")
        self.storage.upload_immutable(extra, r.release_prefix(identity) + "/extra")
        with self.assertRaisesRegex(ValueError, "Unexpected objects"):
            self.publish(identity, directory)
        self.assertIsNone(self.feed("stable"))
        self.assertIsNone(
            self.storage.reader.read_optional(
                r.release_prefix(identity) + "/artifacts.json"
            )
        )

    def test_cas_retries_concurrent_change_and_never_replaces_newer_version(self):
        identity, directory = self.payload()
        newer, next_dir = self.payload("dictation/v2.0.0")
        store = self.storage.channel_store(
            "antfly-dictation/channels/stable/appcast.xml"
        )
        original = store.compare_and_swap_bytes

        def concurrent(previous, body, *args, **kwargs):
            original(previous, (next_dir / "appcast.xml").read_bytes(), *args, **kwargs)
            raise ConditionalWriteConflict("concurrent publication")

        with (
            patch.object(store, "compare_and_swap_bytes", side_effect=concurrent),
            patch.object(self.storage, "channel_store", return_value=store),
        ):
            self.assertEqual(
                r.promote_feed(
                    self.storage,
                    "stable",
                    identity,
                    (directory / "appcast.xml").read_bytes(),
                ),
                "superseded",
            )
        self.assertEqual(self.feed("stable"), (next_dir / "appcast.xml").read_bytes())

    def test_lost_channel_response_is_recovered_by_exact_bytes_on_retry(self):
        identity, directory = self.payload()
        store = self.storage.channel_store(
            "antfly-dictation/channels/stable/appcast.xml"
        )
        original = store.compare_and_swap_bytes

        def lost(*args, **kwargs):
            original(*args, **kwargs)
            raise ConnectionError("lost committed response")

        with (
            patch.object(store, "compare_and_swap_bytes", side_effect=lost),
            patch.object(self.storage, "channel_store", return_value=store),
        ):
            with self.assertRaises(ConnectionError):
                self.publish(identity, directory)
        self.publish(identity, directory)
        self.assertEqual(self.feed("stable"), self.feed("beta"))

    def test_recovery_preserves_exact_signed_bytes_and_original_identity(self):
        identity, directory = self.payload()
        self.publish(identity, directory)
        destination = self.root / "recovered"
        result = r.recover(
            self.storage,
            identity["tag"],
            identity["source_commit"],
            r.digest(directory / "artifacts.json"),
            destination,
        )
        self.assertEqual(result, identity)
        self.assertEqual(
            {p.name: p.read_bytes() for p in destination.iterdir()},
            {p.name: p.read_bytes() for p in directory.iterdir()},
        )
        self.publish(result, destination)

    def test_recovery_rejects_wrong_digest_source_extra_members_or_unsealed_payload(
        self,
    ):
        for change in ("digest", "source", "extra", "unsealed", "bytes"):
            with self.subTest(change=change):
                identity, directory = self.payload(
                    "dictation/v1.0."
                    + str(
                        ["digest", "source", "extra", "unsealed", "bytes"].index(change)
                    )
                )
                self.publish(identity, directory)
                prefix = (
                    self.root
                    / "objects"
                    / r.POLICY["bucket"]
                    / r.release_prefix(identity)
                )
                if change == "extra":
                    (prefix / "unexpected").write_bytes(b"extra")
                if change == "unsealed":
                    (prefix / "artifacts.json").unlink()
                if change == "bytes":
                    (prefix / "appcast.xml").write_bytes(b"tampered")
                out = self.root / ("restored-" + change)
                with self.assertRaises((SystemExit, ValueError, FileNotFoundError)):
                    r.recover(
                        self.storage,
                        identity["tag"],
                        "c" * 40 if change == "source" else identity["source_commit"],
                        "0" * 64
                        if change == "digest"
                        else r.digest(directory / "artifacts.json"),
                        out,
                    )
                self.assertFalse(out.exists())

    def test_local_cas_rejects_stale_snapshot_and_bounds_metadata(self):
        store = LocalConditionalStore(self.root, "bucket", "channel")
        absent = store.load_bytes()
        store.compare_and_swap_bytes(absent, b"first", "application/xml")
        with self.assertRaises(ConditionalWriteConflict):
            store.compare_and_swap_bytes(absent, b"stale", "application/xml")
        with self.assertRaisesRegex(ValueError, "size limit"):
            store.load_bytes(max_bytes=2)
        self.assertEqual(store.load_bytes().body, b"first")

    def test_modified_local_bytes_are_rejected_before_any_storage_write(self):
        identity, directory = self.payload()
        (directory / "appcast.xml").write_bytes(b"tampered")
        with patch.object(self.storage, "upload_immutable") as upload:
            with self.assertRaises((SystemExit, ValueError)):
                self.publish(identity, directory)
            upload.assert_not_called()
