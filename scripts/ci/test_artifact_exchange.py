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

"""Exercise CI receipt trust boundaries and exact content reuse."""

import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch
from urllib.error import HTTPError

import artifact_exchange as exchange


class Store:
    def __init__(self):
        self.objects = {}
        self.writes = 0

    def put(self, key, path):
        data = path.read_bytes()
        if key in self.objects:
            assert self.objects[key] == data
        else:
            self.objects[key] = data
            self.writes += 1

    def get(self, key, path):
        path.write_bytes(self.objects[key])


class ArtifactExchangeTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.source = self.root / "source"
        self.source.mkdir()
        (self.source / "nested").mkdir()
        (self.source / "a.tar.gz").write_bytes(b"canonical archive")
        (self.source / "nested/b.json").write_text("{}")
        self.store = Store()

    def upload(self, destination="receipts", **overrides):
        values = dict(
            patterns=str(self.source),
            name="native",
            repository="antflydb/antfly",
            run_id="42",
            destination=self.root / destination,
            now=100,
        )
        values.update(overrides)
        exchange.upload(self.store, **values)
        return values["destination"]

    def restore(self, staging, **overrides):
        values = dict(
            staging=staging,
            destination=self.root / "restored",
            repository="antflydb/antfly",
            run_id="42",
            name="native",
        )
        values.update(overrides)
        exchange.restore(self.store, **values)
        return values["destination"]

    def test_identical_bytes_are_stored_once_across_runs_and_restore_exact_layout(self):
        first = self.upload()
        self.upload("second", run_id="43")
        self.assertEqual(self.store.writes, 2)
        destination = self.restore(first)
        self.assertEqual((destination / "a.tar.gz").read_bytes(), b"canonical archive")
        self.assertEqual((destination / "nested/b.json").read_text(), "{}")

    def test_namespaces_and_age_windows_do_not_share_objects(self):
        self.upload()
        self.upload("later", now=exchange.WINDOW_SECONDS + 1)
        self.upload("vopr", namespace="vopr", retention_days=90)
        self.assertEqual(self.store.writes, 6)
        self.assertTrue(
            any(key.startswith("vopr/replay/") for key in self.store.objects)
        )

    def test_wrong_producer_name_namespace_or_run_is_rejected(self):
        first = self.upload()
        for field, value in [
            ("repository", "evil/repo"),
            ("run_id", "43"),
            ("name", "other"),
            ("namespace", "vopr"),
        ]:
            with (
                self.subTest(field=field),
                self.assertRaisesRegex(ValueError, "identity differs"),
            ):
                self.restore(first, **{field: value})

    def test_corruption_is_rejected_before_target_file_is_published(self):
        first = self.upload()
        self.store.objects[next(iter(self.store.objects))] = b"corrupt"
        with self.assertRaisesRegex(ValueError, "content differs"):
            self.restore(first)
        self.assertEqual(list((self.root / "restored").rglob("*")), [])

    def test_traversal_bad_digest_key_and_duplicate_paths_are_rejected(self):
        first = self.upload()
        path = next(first.iterdir())
        original = json.loads(path.read_text())
        for change in ["path", "key", "sha256", "duplicate"]:
            body = json.loads(json.dumps(original))
            if change == "duplicate":
                body["files"].append(body["files"][0])
            else:
                body["files"][0][change] = {
                    "path": "../escape",
                    "key": "release/short/1/not-the-digest",
                    "sha256": "x" * 64,
                }[change]
            path.write_text(json.dumps(body))
            with self.subTest(change=change), self.assertRaises(ValueError):
                self.restore(first)
        self.assertFalse((self.root / "escape").exists())

    def test_pattern_download_rejects_colliding_members_and_mixed_formats(self):
        first = self.upload(name="native-a")
        self.upload(name="native-b")
        with self.assertRaisesRegex(ValueError, "duplicate"):
            self.restore(first, name="", pattern="native-*")
        next(p for p in first.iterdir() if "native-b" in p.name).unlink()
        (first / "unexpected.tar.gz").write_bytes(b"legacy")
        with self.assertRaisesRegex(ValueError, "mixed"):
            self.restore(first, name="", pattern="native-*")

    def test_legacy_artifacts_need_no_cloud_credentials_and_existing_files_are_preserved(
        self,
    ):
        destination = self.restore(self.source)
        self.assertEqual((destination / "a.tar.gz").read_bytes(), b"canonical archive")
        with self.assertRaisesRegex(ValueError, "must be empty"):
            self.restore(self.source)

    def test_symlinks_cannot_escape_inventory_or_legacy_download(self):
        (self.source / "escape").symlink_to(self.root)
        with self.assertRaisesRegex(ValueError, "symlinks"):
            self.upload()
        with self.assertRaisesRegex(ValueError, "symlinks"):
            self.restore(self.source)

    def test_gcs_conditional_conflict_requires_actual_remote_digest(self):
        store = exchange.GCS("fake-token")
        path = self.source / "a.tar.gz"
        for code in (403, 412):
            error = HTTPError(
                "https://storage.googleapis.com", code, "exists", {}, None
            )
            for remote, valid in [(path.read_bytes(), True), (b"poisoned", False)]:
                with patch.object(
                    store, "request", side_effect=[error, io.BytesIO(remote)]
                ):
                    if valid:
                        store.put("key", path)
                    else:
                        with self.assertRaisesRegex(
                            ValueError, "existing CI object differs"
                        ):
                            store.put("key", path)

    def test_permission_failure_without_a_verified_object_is_not_ignored(self):
        store = exchange.GCS("fake-token")
        denied = HTTPError("https://storage.googleapis.com", 403, "denied", {}, None)
        missing = HTTPError("https://storage.googleapis.com", 404, "missing", {}, None)
        with patch.object(store, "request", side_effect=[denied, missing]):
            with self.assertRaises(HTTPError) as result:
                store.put("key", self.source / "a.tar.gz")
            self.assertEqual(result.exception.code, 403)

    def test_upload_uses_create_only_precondition_and_streamed_file_length(self):
        store = exchange.GCS("fake-token")
        opener = Mock()
        with patch.object(exchange, "build_opener", return_value=opener):
            with (self.source / "a.tar.gz").open("rb") as stream:
                store.request("release/short/1/hash", data=stream, method="POST")
                request = opener.open.call_args.args[0]
                self.assertIn("ifGenerationMatch=0", request.full_url)
                self.assertEqual(
                    request.get_header("Content-length"),
                    str((self.source / "a.tar.gz").stat().st_size),
                )
                self.assertIs(request.data, stream)

    def test_network_retry_restarts_the_download_without_partial_old_bytes(self):
        store = exchange.GCS("fake-token")
        out = self.root / "download"
        with (
            patch.object(
                store,
                "request",
                side_effect=[OSError("connection reset"), io.BytesIO(b"complete")],
            ),
            patch.object(exchange.time, "sleep"),
        ):
            store.get("key", out)
        self.assertEqual(out.read_bytes(), b"complete")


if __name__ == "__main__":
    unittest.main()
