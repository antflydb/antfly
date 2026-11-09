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
import json
import plistlib
import tempfile
import unittest
import zipfile
import os
import subprocess
import sys
from pathlib import Path
from unittest.mock import patch

import release as r

KEY = base64.b64encode(bytes(range(32))).decode()
SIGNATURE = base64.b64encode(bytes(range(64))).decode()


class ReleaseTests(unittest.TestCase):
    def test_version_order_and_apple_bundle_versions(self):
        tags = [
            "dictation/v0.1.0-beta.1",
            "dictation/v0.1.0-beta.2",
            "dictation/v0.1.0-rc.1",
            "dictation/v0.1.0",
            "dictation/v0.1.1-beta.1",
            "dictation/v1.0.0",
        ]
        versions = [r.version(t) for t in tags]
        self.assertEqual(
            [v["order"] for v in versions], sorted(v["order"] for v in versions)
        )
        self.assertEqual(versions[0]["bundle_version"], "0.1.0b1")
        self.assertEqual(versions[2]["bundle_version"], "0.1.0fc1")
        self.assertEqual(versions[3]["channel"], "stable")

    def test_invalid_tags(self):
        for t in [
            "v1.0.0",
            "dictation/v01.0.0",
            "dictation/v1.100.0",
            "dictation/v1.0.0-beta.0",
            "dictation/v1.0.0-rc.256",
            "dictation/v1.0.0\n",
            "dictation/v1.0.0;echo bad",
            "dictation/v1.0.0+build",
            "dictation/v1.0.0-alpha.1",
        ]:
            with self.subTest(tag=t), self.assertRaises(ValueError):
                r.version(t)

    def test_stamp_and_dev_reset(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "Info.plist"
            p.write_bytes(
                plistlib.dumps({"CFBundleIdentifier": "dev.antfly.dictation"})
            )
            r.stamp(p, "dictation/v1.2.3-rc.4", KEY)
            info = plistlib.loads(p.read_bytes())
            self.assertEqual(info["CFBundleVersion"], "1.2.3fc4")
            self.assertTrue(
                info["AntflyUpdatesEnabled"] and info["SURequireSignedFeed"]
            )
            self.assertFalse(info["SUAutomaticallyUpdate"])
            self.assertEqual(info["AntflyReleaseChannel"], "beta")
            r.stamp(p)
            info = plistlib.loads(p.read_bytes())
            self.assertFalse(info["AntflyUpdatesEnabled"])
            self.assertNotIn("SUPublicEDKey", info)
            self.assertNotIn("SUFeedURL", info)

    def test_missing_or_placeholder_key_fails(self):
        for key in ["", "placeholder", base64.b64encode(bytes(32)).decode()]:
            with self.subTest(key=key), self.assertRaises(ValueError):
                r.public_key(key)

    def test_payload_contains_signed_enclosure_and_exact_identity(self):
        with tempfile.TemporaryDirectory() as d:
            archive = Path(d) / "Antfly-Dictation-1.0.0-arm64.zip"
            archive.write_bytes(b"archive")
            identity = r.version("dictation/v1.0.0") | {"tag": "dictation/v1.0.0"}
            root = r.ET.fromstring(r.appcast(identity, archive, SIGNATURE))
            enclosure = root.find("channel/item/enclosure")
            self.assertEqual(enclosure.attrib["length"], "7")
            self.assertEqual(
                enclosure.attrib["{" + r.SPARKLE + "}edSignature"], SIGNATURE
            )
            self.assertIn("/antfly-dictation/releases/1.0.0/", enclosure.attrib["url"])

    def feed(self, tag):
        with tempfile.TemporaryDirectory() as d:
            archive = Path(d) / "app.zip"
            archive.write_bytes(b"archive")
            return r.appcast(r.version(tag) | {"tag": tag}, archive, SIGNATURE)

    def test_rejects_archive_traversal_and_external_symlinks(self):
        for name, target in [
            ("../bad", None),
            ("/bad", None),
            ("Other.app/file", None),
            ("Antfly Dictation.app/link", "../../outside"),
            ("Antfly Dictation.app/link", "/absolute"),
        ]:
            with (
                self.subTest(name=name, target=target),
                tempfile.TemporaryDirectory() as d,
            ):
                archive = Path(d) / "app.zip"
                with zipfile.ZipFile(archive, "w") as z:
                    item = zipfile.ZipInfo(name)
                    if target:
                        item.external_attr = 0o120777 << 16
                    z.writestr(item, target or "data")
                with self.assertRaises(ValueError):
                    r.verify_unsigned_zip(archive)

    def test_allows_versioned_framework_symlinks(self):
        with tempfile.TemporaryDirectory() as d:
            archive = Path(d) / "app.zip"
            with zipfile.ZipFile(archive, "w") as z:
                item = zipfile.ZipInfo(
                    "Antfly Dictation.app/Contents/Frameworks/Sparkle.framework/Versions/Current"
                )
                item.external_attr = 0o120777 << 16
                z.writestr(item, "B")
            r.verify_unsigned_zip(archive)

    def request(self):
        request = {
            "schema_version": 1,
            "tag": "dictation/v1.0.0",
            "source_commit": "a" * 40,
        }
        event = {
            "repository": {"full_name": "antflydb/antfly"},
            "workflow_run": {
                "name": "Dictation release request",
                "path": ".github/workflows/dictation-release-request.yml",
                "event": "push",
                "conclusion": "success",
                "head_repository": {"full_name": "antflydb/antfly"},
                "head_sha": "a" * 40,
                "head_branch": request["tag"],
            },
        }
        return request, event

    def test_rejects_fork_manual_and_mismatched_tag_requests(self):
        for field, value in [
            ("event", "workflow_dispatch"),
            ("head_branch", "dictation/v9.0.0"),
            ("head_sha", "b" * 40),
            ("path", ".github/workflows/other.yml"),
            ("head_repository", {"full_name": "attacker/fork"}),
        ]:
            request, event = self.request()
            event["workflow_run"][field] = value
            with self.subTest(field=field), self.assertRaises(ValueError):
                r.resolve(request, event, "c" * 40, "main")

    def test_requires_source_ancestry_and_unmoved_tag(self):
        request, event = self.request()
        with patch.object(r, "run", side_effect=["c" * 40, "", "", "b" * 40]):
            with self.assertRaisesRegex(ValueError, "tag moved"):
                r.resolve(request, event, "c" * 40, "main")

    def test_pinned_sparkle_matches_release_policy(self):
        package = Path(__file__).resolve().parents[1]
        lock = json.loads((package / "Package.resolved").read_text())
        self.assertEqual(
            lock["pins"][0]["state"]["version"], r.POLICY["sparkle_version"]
        )
        self.assertIn(
            'exact: "' + r.POLICY["sparkle_version"] + '"',
            (package / "Package.swift").read_text(),
        )


class PublicationTests(unittest.TestCase):
    def identity(self):
        identity = r.version("dictation/v1.0.0")
        identity.pop("order")
        return identity | {
            "tag": "dictation/v1.0.0",
            "source_commit": "a" * 40,
            "controller_commit": "c" * 40,
        }

    def test_feed_download_identity_is_verified(self):
        with tempfile.TemporaryDirectory() as d:
            directory = Path(d)
            identity = self.identity()
            archive = directory / "Antfly-Dictation-1.0.0-arm64.zip"
            archive.write_bytes(b"archive")
            feed = r.appcast(identity, archive, SIGNATURE)
            (directory / "appcast.xml").write_bytes(feed)
            self.assertEqual(r.verify_feed(identity, directory), feed)
            (directory / "appcast.xml").write_bytes(
                feed.replace(b'length="7"', b'length="8"')
            )
            with self.assertRaisesRegex(ValueError, "download identity"):
                r.verify_feed(identity, directory)


class NativeSignatureTests(unittest.TestCase):
    @unittest.skipUnless(sys.platform == "darwin", "CryptoKit requires macOS")
    def test_private_key_consistency_helper_matches_rfc8032(self):
        package = Path(__file__).resolve().parents[1]
        seed = bytes.fromhex(
            "9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60"
        )
        expected = bytes.fromhex(
            "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a"
        )
        public = subprocess.check_output(
            [
                "swift",
                "-module-cache-path",
                str(package / ".build/modules"),
                str(package / "release/public-key.swift"),
            ],
            input=base64.b64encode(seed) + b"\n",
        )
        self.assertEqual(base64.b64decode(public.strip()), expected)

    @unittest.skipUnless(
        sys.platform == "darwin", "Sparkle signing tools require macOS"
    )
    def test_real_sparkle_archive_and_feed_signatures_reject_tampering(self):
        tool = (
            Path(__file__).resolve().parents[1] / ".build/updater-tools/bin/sign_update"
        )
        if not tool.exists():
            self.skipTest("Run release/fetch-tools.sh first")
        seed = base64.b64encode(os.urandom(32))

        def sign(*args, check=True):
            return subprocess.run(
                [str(tool), "--ed-key-file", "-", *args],
                input=seed,
                capture_output=True,
                check=check,
            )

        with tempfile.TemporaryDirectory() as d:
            archive = Path(d) / "app.zip"
            archive.write_bytes(b"update archive fixture")
            signature = sign("-p", str(archive)).stdout.decode().strip()
            sign("--verify", str(archive), signature)
            identity = r.version("dictation/v1.0.0") | {"tag": "dictation/v1.0.0"}
            feed = Path(d) / "appcast.xml"
            feed.write_bytes(r.appcast(identity, archive, signature))
            sign(str(feed))
            sign("--verify", str(feed))
            feed.write_bytes(feed.read_bytes().replace(b"1.0.0", b"9.0.0"))
            self.assertNotEqual(sign("--verify", str(feed), check=False).returncode, 0)
            archive.write_bytes(b"tampered archive")
            self.assertNotEqual(
                sign("--verify", str(archive), signature, check=False).returncode, 0
            )


if __name__ == "__main__":
    unittest.main()
