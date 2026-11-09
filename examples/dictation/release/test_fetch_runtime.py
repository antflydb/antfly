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

import hashlib
import importlib.util
import io
import json
import tarfile
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

spec = importlib.util.spec_from_file_location(
    "released_runtime", Path(__file__).with_name("fetch-runtime.py")
)
f = importlib.util.module_from_spec(spec)
spec.loader.exec_module(f)


class ReleasedRuntimeTests(unittest.TestCase):
    def fixture(self, root):
        archive = root / "runtime.tar.gz"
        with tarfile.open(archive, "w:gz") as tar:
            for name, body in [
                ("antfly", b"released binary"),
                ("LICENSE", b"Apache license"),
            ]:
                member = tarfile.TarInfo(name)
                member.size = len(body)
                tar.addfile(member, io.BytesIO(body))
        stage = root / "payload"
        stage.mkdir()
        f.extract(archive, stage)
        pin = {
            "tag": "v1.0.0",
            "source_commit": "a" * 40,
            "archive": archive.name,
            "sha256": f.sha256(archive),
            "size": archive.stat().st_size,
            "payload_sha256": f.payload_digest(stage),
            "architecture": "arm64",
            "apple_providers": False,
        }
        ledger = json.dumps(
            {
                "tag": pin["tag"],
                "commit": pin["source_commit"],
                "artifacts": [
                    {"name": archive.name, "sha256": pin["sha256"], "size": pin["size"]}
                ],
            }
        ).encode()
        pin["ledger_sha256"] = hashlib.sha256(ledger).hexdigest()
        return archive, pin, ledger

    def test_staging_uses_released_bytes_and_cached_payload_needs_no_network(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            archive, pin, ledger = self.fixture(root)

            def download(url, target, sha, maximum):
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(
                    ledger if url.endswith("artifacts.json") else archive.read_bytes()
                )

            with (
                patch.object(f, "PIN", pin),
                patch.object(f, "download", side_effect=download),
            ):
                staged = f.stage(root / "cache")
                self.assertEqual((staged / "antfly").read_bytes(), b"released binary")
                f.verify_staged(staged)
            with (
                patch.object(f, "PIN", pin),
                patch.object(f, "download", side_effect=AssertionError("network")),
            ):
                self.assertEqual(f.stage(root / "cache"), staged)

    def test_modified_binary_metadata_or_extra_file_is_rejected(self):
        for damage in ("binary", "metadata", "extra"):
            with (
                self.subTest(damage=damage),
                tempfile.TemporaryDirectory() as directory,
            ):
                root = Path(directory)
                _, pin, _ = self.fixture(root)
                stage = root / "payload"
                (stage / "release.json").write_text(json.dumps(pin))
                if damage == "binary":
                    (stage / "antfly").write_bytes(b"tampered")
                elif damage == "metadata":
                    (stage / "release.json").write_text("{}")
                else:
                    (stage / "unexpected").write_bytes(b"extra")
                with patch.object(f, "PIN", pin), self.assertRaises(ValueError):
                    f.verify_staged(stage)

    def test_unsafe_archive_links_and_duplicates_are_rejected(self):
        for name, link in [("../antfly", False), ("/antfly", False), ("antfly", True)]:
            with (
                self.subTest(name=name, link=link),
                tempfile.TemporaryDirectory() as directory,
            ):
                root = Path(directory)
                archive = root / "archive.tar.gz"
                stage = root / "stage"
                stage.mkdir()
                with tarfile.open(archive, "w:gz") as tar:
                    member = tarfile.TarInfo(name)
                    if link:
                        member.type = tarfile.SYMTYPE
                        member.linkname = "/usr/bin/false"
                    tar.addfile(member)
                with self.assertRaises(ValueError):
                    f.extract(archive, stage)

    def test_download_checks_hash_and_bounds_and_cleans_partial_bytes(self):
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / "download"
            for body, maximum, expected in [
                (b"bad", 100, "0" * 64),
                (b"large", 2, hashlib.sha256(b"large").hexdigest()),
            ]:
                with (
                    patch.object(f, "urlopen", return_value=io.BytesIO(body)),
                    self.assertRaises(ValueError),
                ):
                    f.download(
                        "https://releases.antfly.io/file", target, expected, maximum
                    )
                self.assertFalse(target.exists())
                self.assertFalse(target.with_suffix(".partial").exists())

    def test_cached_download_is_verified_before_reuse(self):
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / "file"
            target.write_bytes(b"valid")
            with patch.object(f, "urlopen", side_effect=AssertionError("network")):
                f.download(
                    "https://releases.antfly.io/file",
                    target,
                    hashlib.sha256(b"valid").hexdigest(),
                    5,
                )
