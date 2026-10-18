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

import io
import json
import struct
import tarfile
import tempfile
import unittest
import urllib.error
from pathlib import Path
from unittest.mock import patch

import embedded_cargo as cargo

VERSION = "0.3.0-rc.1"
COMMIT = "a" * 40
SYS, BINDING = cargo.CRATES


def manifest(name, extra=""):
    dependency = (
        f'[dependencies.antfly-embedded-sys]\nversion = "={VERSION}"\n'
        if name == BINDING
        else ""
    )
    return (
        f'[package]\nname = "{name}"\nversion = "{VERSION}"\n'
        'license = "Apache-2.0"\nreadme = "README.md"\n'
        f'[package.metadata.antfly-release]\nsource-commit = "{COMMIT}"\n'
        + dependency
        + extra
    )


def archive(path, text):
    with tarfile.open(path, "w:gz") as output:
        for name, body in {
            "Cargo.toml": text,
            "README.md": "Embedded Antfly\n",
        }.items():
            data = body.encode()
            member = tarfile.TarInfo(f"{path.name.removesuffix('.crate')}/{name}")
            member.size = len(data)
            output.addfile(member, io.BytesIO(data))


class CargoReleaseTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.target = self.root / "target"
        (self.target / "package").mkdir(parents=True)
        self.snapshot = self.root / "snapshot"
        for name in cargo.CRATES:
            archive(self.target / "package" / f"{name}-{VERSION}.crate", manifest(name))
        cargo.seal(self.target, self.snapshot, VERSION, COMMIT)
        self.checksums = cargo.verify(self.snapshot, VERSION, COMMIT)

    def test_rc_stamping_pins_sys_and_binds_source_without_mutating_input(self):
        original = (
            '[package]\nname = "antfly-embedded"\nversion = "0.0.1"\n'
            'license = "Apache-2.0"\n[dependencies]\n'
            'antfly-embedded-sys = { path = "../embedded-sys", version = "0.0.1" }\n'
        )
        stamped = cargo.stamp_manifest(original, BINDING, VERSION, COMMIT)
        self.assertIn('version = "=0.3.0-rc.1"', stamped)
        self.assertIn(f'source-commit = "{COMMIT}"', stamped)
        self.assertNotIn(COMMIT, original)
        with self.assertRaisesRegex(ValueError, "unexpected crate identity"):
            cargo.stamp_manifest(original, SYS, VERSION, COMMIT)

    def test_snapshot_requires_exact_identity_files_and_bytes(self):
        with self.assertRaisesRegex(ValueError, "identity"):
            cargo.verify(self.snapshot, VERSION, "b" * 40)
        extra = self.snapshot / "unexpected"
        extra.touch()
        with self.assertRaisesRegex(ValueError, "file set"):
            cargo.verify(self.snapshot, VERSION, COMMIT)
        extra.unlink()
        with (self.snapshot / f"{SYS}-{VERSION}.crate").open("ab") as output:
            output.write(b"tampered")
        with self.assertRaisesRegex(ValueError, "digest"):
            cargo.verify(self.snapshot, VERSION, COMMIT)

    def test_snapshot_cannot_relabel_one_crate_as_the_other(self):
        sys = self.snapshot / f"{SYS}-{VERSION}.crate"
        binding = self.snapshot / f"{BINDING}-{VERSION}.crate"
        sys.write_bytes(binding.read_bytes())
        manifest_path = self.snapshot / "cargo-snapshot.json"
        document = json.loads(manifest_path.read_text())
        for item in document["artifacts"]:
            if item["name"] == sys.name:
                item.update(sha256=cargo.digest(sys), size=sys.stat().st_size)
        manifest_path.write_text(json.dumps(document))
        with self.assertRaisesRegex(ValueError, "archive identity"):
            cargo.verify(self.snapshot, VERSION, COMMIT)

    def test_rejects_loose_sys_dependency(self):
        path = self.target / "package" / f"{BINDING}-{VERSION}.crate"
        archive(path, manifest(BINDING).replace(f'"={VERSION}"', f'"{VERSION}"'))
        with self.assertRaisesRegex(ValueError, "pin its sys"):
            cargo.crate_identity(path)

    def test_metadata_preserves_renames_features_targets_and_dependency_kinds(self):
        path = self.target / "package" / f"{BINDING}-{VERSION}.crate"
        archive(
            path,
            manifest(
                BINDING,
                """
[features]
default = ["dep:renamed"]
[dependencies.renamed]
package = "serde"
version = "1"
optional = true
default-features = false
features = ["derive"]
[build-dependencies]
pkg-config = "0.3"
[target.'cfg(unix)'.dev-dependencies]
tokio = { version = "1", features = ["rt"] }
""",
            ),
        )
        document = cargo.upload_metadata(path)
        self.assertEqual(document["name"], BINDING)
        self.assertEqual(document["vers"], VERSION)
        self.assertEqual(document["readme"], "Embedded Antfly\n")
        self.assertEqual(document["features"], {"default": ["dep:renamed"]})
        deps = document["deps"]
        renamed = next(dep for dep in deps if dep["name"] == "serde")
        self.assertEqual(
            renamed,
            {
                "name": "serde",
                "version_req": "1",
                "features": ["derive"],
                "optional": True,
                "default_features": False,
                "kind": "normal",
                "target": None,
                "registry": None,
                "explicit_name_in_toml": "renamed",
            },
        )
        self.assertEqual(
            next(dep for dep in deps if dep["name"] == "tokio")["target"], "cfg(unix)"
        )
        self.assertEqual(
            next(dep for dep in deps if dep["name"] == "pkg-config")["kind"], "build"
        )
        self.assertEqual(
            next(dep for dep in deps if dep["name"] == SYS)["version_req"],
            "=" + VERSION,
        )

    def test_unsupported_dependencies_and_external_readme_fail_closed(self):
        path = self.target / "package" / f"{SYS}-{VERSION}.crate"
        archive(
            path,
            manifest(SYS, '[dependencies.unsafe]\nversion="1"\npath="../outside"\n'),
        )
        with self.assertRaisesRegex(ValueError, "unsupported"):
            cargo.upload_metadata(path)
        archive(
            path,
            manifest(SYS).replace('readme = "README.md"', 'readme = "../README.md"'),
        )
        with self.assertRaisesRegex(ValueError, "inside the archive"):
            cargo.upload_metadata(path)

    @patch.object(cargo.urllib.request, "urlopen")
    def test_upload_frames_exact_sealed_bytes_using_cargo_api(self, urlopen):
        urlopen.return_value.__enter__.return_value = io.BytesIO(b'{"warnings": {}}')
        path = self.snapshot / f"{SYS}-{VERSION}.crate"
        metadata = cargo.upload_metadata(path)
        cargo.upload(path, metadata, "test-short-lived-token")
        request = urlopen.call_args.args[0]
        self.assertEqual(request.method, "PUT")
        self.assertEqual(request.full_url, "https://crates.io/api/v1/crates/new")
        self.assertEqual(request.get_header("Authorization"), "test-short-lived-token")
        data = request.data
        size = struct.unpack("<I", data[:4])[0]
        self.assertEqual(json.loads(data[4 : 4 + size]), metadata)
        offset = 4 + size
        self.assertEqual(
            struct.unpack("<I", data[offset : offset + 4])[0], path.stat().st_size
        )
        self.assertEqual(data[offset + 4 :], path.read_bytes())

    @patch.object(cargo.urllib.request, "urlopen")
    def test_http_success_with_registry_errors_is_failure(self, urlopen):
        urlopen.return_value.__enter__.return_value = io.BytesIO(
            b'{"errors": [{"detail": "rejected"}]}'
        )
        with self.assertRaisesRegex(ValueError, "rejected"):
            cargo.upload(self.snapshot / f"{SYS}-{VERSION}.crate", {}, "token")

    @patch.object(cargo.urllib.request, "urlopen")
    def test_only_http_404_is_an_absent_version(self, urlopen):
        for status in (401, 403, 429, 500, 503):
            with self.subTest(status=status):
                urlopen.side_effect = urllib.error.HTTPError(
                    "https://crates.io", status, "error", {}, io.BytesIO()
                )
                with self.assertRaises(urllib.error.HTTPError):
                    cargo.registry_checksum(SYS, VERSION)
        urlopen.side_effect = urllib.error.HTTPError(
            "https://crates.io", 404, "missing", {}, io.BytesIO()
        )
        self.assertIsNone(cargo.registry_checksum(SYS, VERSION))

    @patch.object(
        cargo.subprocess,
        "run",
        side_effect=AssertionError("credentialed source execution"),
    )
    @patch.object(cargo.time, "sleep")
    @patch.object(cargo, "upload")
    @patch.object(cargo, "registry_checksum")
    def test_publish_waits_for_sys_visibility_and_never_executes_source(
        self, checksum, upload, sleep, run
    ):
        # Both crates are preflighted before the sys upload; sys becomes visible
        # on the second poll, and only then can bindings be uploaded.
        checksum.side_effect = [
            None,
            None,
            None,
            self.checksums[SYS],
            self.checksums[BINDING],
        ]
        events = []
        upload.side_effect = lambda path, metadata, token: events.append(
            ("upload", metadata["name"])
        )
        sleep.side_effect = lambda seconds: events.append(("wait", seconds))
        cargo.publish(self.snapshot, VERSION, COMMIT, "token")
        self.assertEqual(events, [("upload", SYS), ("wait", 2), ("upload", BINDING)])
        run.assert_not_called()

    @patch.object(cargo, "upload")
    @patch.object(cargo, "registry_checksum")
    def test_partial_retry_skips_matching_sys(self, checksum, upload):
        checksum.side_effect = [self.checksums[SYS], None, self.checksums[BINDING]]
        cargo.publish(self.snapshot, VERSION, COMMIT, "token")
        self.assertEqual(
            [call.args[1]["name"] for call in upload.call_args_list], [BINDING]
        )

    @patch.object(cargo, "upload")
    @patch.object(cargo, "registry_checksum")
    def test_binding_collision_prevents_every_upload(self, checksum, upload):
        checksum.side_effect = [None, "b" * 64]
        with self.assertRaisesRegex(ValueError, "immutable"):
            cargo.publish(self.snapshot, VERSION, COMMIT, "token")
        upload.assert_not_called()

    @patch.object(cargo, "upload")
    @patch.object(cargo, "registry_checksum")
    def test_missing_credentials_prevents_every_upload(self, checksum, upload):
        checksum.return_value = None
        with self.assertRaisesRegex(ValueError, "token"):
            cargo.publish(self.snapshot, VERSION, COMMIT, "")
        upload.assert_not_called()

    @patch.object(cargo, "upload")
    @patch.object(cargo, "registry_checksum")
    def test_exact_retry_requires_no_credentials_or_writes(self, checksum, upload):
        checksum.side_effect = [self.checksums[SYS], self.checksums[BINDING]]
        cargo.publish(self.snapshot, VERSION, COMMIT, "")
        upload.assert_not_called()
