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
import io
import json
import subprocess
import tarfile
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import rust_sdk_release as sdk

VERSION = "0.3.0"
COMMIT = "a" * 40
TAG = "rs/antfly/sdk/v0.3.0"


class RustSdkReleaseTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.crate = self.root / "antfly-sdk-0.3.0.crate"
        text = (
            '[package]\nname = "antfly-sdk"\nversion = "0.3.0"\n'
            'license = "Apache-2.0"\ndescription = "Rust SDK"\n'
            f'[package.metadata.antfly-release]\nsource-commit = "{COMMIT}"\n'
        )
        with tarfile.open(self.crate, "w:gz") as archive:
            member = tarfile.TarInfo("antfly-sdk-0.3.0/Cargo.toml")
            data = text.encode()
            member.size = len(data)
            archive.addfile(member, io.BytesIO(data))
        self.checksum = hashlib.sha256(self.crate.read_bytes()).hexdigest()
        self.metadata = {
            "schema_version": 1,
            "version": VERSION,
            "commit": COMMIT,
            "name": self.crate.name,
            "sha256": self.checksum,
            "size": self.crate.stat().st_size,
        }
        self.write_metadata()

    def write_metadata(self):
        (self.root / "sdk-snapshot.json").write_text(json.dumps(self.metadata))

    def test_valid_snapshot(self):
        self.assertEqual(sdk.verify(self.root, VERSION, COMMIT), self.crate)

    def test_tampered_crate(self):
        self.crate.write_bytes(self.crate.read_bytes() + b"tampered")
        with self.assertRaisesRegex(ValueError, "identity or digest"):
            sdk.verify(self.root, VERSION, COMMIT)

    def test_wrong_source_or_version(self):
        for key, value in (("commit", "b" * 40), ("version", "0.3.1")):
            with self.subTest(field=key):
                original = self.metadata[key]
                self.metadata[key] = value
                self.write_metadata()
                with self.assertRaisesRegex(ValueError, "identity or digest"):
                    sdk.verify(self.root, VERSION, COMMIT)
                self.metadata[key] = original

    def test_extra_files_and_symlinks(self):
        extra = self.root / "extra"
        extra.write_text("unexpected")
        with self.assertRaisesRegex(ValueError, "file set"):
            sdk.verify(self.root, VERSION, COMMIT)
        extra.unlink()
        saved = self.crate.read_bytes()
        self.crate.unlink()
        self.crate.symlink_to("/dev/null")
        with self.assertRaisesRegex(ValueError, "symlinks"):
            sdk.verify(self.root, VERSION, COMMIT)
        self.crate.unlink()
        self.crate.write_bytes(saved)

    def test_same_registry_bytes_skip_publication(self):
        with (
            patch.object(sdk, "preflight", return_value=[]),
            patch.object(sdk, "upload") as upload,
        ):
            sdk.publish(self.root, VERSION, COMMIT)
            upload.assert_not_called()

    def test_conflicting_registry_bytes_stop_before_upload(self):
        with (
            patch.object(
                sdk, "preflight", side_effect=ValueError("immutable version differs")
            ),
            patch.object(sdk, "upload") as upload,
        ):
            with self.assertRaisesRegex(ValueError, "immutable version"):
                sdk.publish(self.root, VERSION, COMMIT)
            upload.assert_not_called()

    def test_upload_exact_sealed_crate_and_verify_registry(self):
        with (
            patch.object(sdk, "preflight", return_value=[sdk.NAME]),
            patch.object(sdk, "upload") as upload,
            patch.object(sdk, "registry_checksum", return_value=self.checksum),
            patch.dict("os.environ", {"CARGO_REGISTRY_TOKEN": "test-token"}),
        ):
            sdk.publish(self.root, VERSION, COMMIT)
            self.assertEqual(upload.call_args.args[0], self.crate)
            self.assertEqual(upload.call_args.args[1]["vers"], VERSION)

    def test_invalid_namespace_is_rejected_before_git(self):
        for tag in ("v0.3.0", "rs/antfly/sdk/v0.3", "rs/antfly/sdk/v0.3.0/evil"):
            with (
                self.subTest(tag=tag),
                patch.object(sdk.subprocess, "check_output") as git,
            ):
                with self.assertRaisesRegex(ValueError, "release tag"):
                    sdk.identity(self.root, tag)
                git.assert_not_called()

    def test_tag_ancestry_version_and_source(self):
        manifest = '[package]\nname="antfly-sdk"\nversion="0.3.0"\nlicense="Apache-2.0"'
        with (
            patch.object(
                sdk.subprocess, "check_output", side_effect=[COMMIT, manifest]
            ),
            patch.object(sdk.subprocess, "run") as git,
        ):
            self.assertEqual(sdk.identity(self.root, TAG), (VERSION, COMMIT))
            self.assertEqual(
                git.call_args.args[0],
                ["git", "merge-base", "--is-ancestor", COMMIT, "origin/main"],
            )
        with (
            patch.object(sdk.subprocess, "check_output", return_value=COMMIT),
            patch.object(
                sdk.subprocess,
                "run",
                side_effect=subprocess.CalledProcessError(1, "git"),
            ),
        ):
            with self.assertRaises(subprocess.CalledProcessError):
                sdk.identity(self.root, TAG)
        with (
            patch.object(
                sdk.subprocess,
                "check_output",
                side_effect=[COMMIT, manifest.replace("0.3.0", "0.2.0")],
            ),
            patch.object(sdk.subprocess, "run"),
        ):
            with self.assertRaisesRegex(ValueError, "package identity"):
                sdk.identity(self.root, TAG)

    def test_publisher_does_not_execute_source_or_build_tools(self):
        workflow = (
            Path(__file__).resolve().parents[2]
            / ".github/workflows/rust-sdk-release.yml"
        ).read_text()
        publisher = workflow.split("\n  publish:\n", 1)[1]
        self.assertIn("environment: crates-io", publisher)
        self.assertIn("github.ref == 'refs/heads/main'", publisher)
        self.assertNotIn("rust-toolchain", publisher)
        self.assertNotIn("cargo publish", publisher)
        self.assertNotIn("cargo test", publisher)
        self.assertIn("--commit", publisher)


if __name__ == "__main__":
    unittest.main()
