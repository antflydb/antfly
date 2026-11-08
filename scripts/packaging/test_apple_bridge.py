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

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import apple_bridge as bridge


class AppleBridgeArtifactTests(unittest.TestCase):
    def test_committed_bridge_matches_its_source(self):
        bridge.verify(bridge.SOURCE.parent.parent / "prebuilt/arm64", "arm64")

    def fixture(self, root):
        binary = root / bridge.LIBRARY
        binary.write_bytes(
            b"\xcf\xfa\xed\xfe" + (0x100000C).to_bytes(4, "little") + b"fixture"
        )
        document = {
            "schema_version": 1,
            "abi_version": 2,
            "source_commit": "a" * 40,
            "architecture": "arm64",
            "source_sha256": bridge.digest(bridge.SOURCE),
            "sha256": bridge.digest(binary),
            "sdk_version": "27.0",
        }
        (root / "apple-bridge.json").write_text(json.dumps(document))
        return document

    def test_source_bound_artifact_verifies_without_executing_mac_code(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.fixture(root)
            with patch.object(
                bridge.subprocess, "run", side_effect=AssertionError("execution")
            ):
                self.assertEqual(bridge.verify(root, "arm64"), root / bridge.LIBRARY)

    def test_mismatched_source_commit_hash_architecture_abi_or_sdk_is_rejected(self):
        for field, value in [
            ("source_commit", "invalid"),
            ("source_sha256", "0" * 64),
            ("sha256", "0" * 64),
            ("architecture", "x86_64"),
            ("abi_version", 1),
            ("sdk_version", "26.1"),
        ]:
            with self.subTest(field=field), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                document = self.fixture(root)
                document[field] = value
                (root / "apple-bridge.json").write_text(json.dumps(document))
                with self.assertRaises(ValueError):
                    bridge.verify(root, "arm64")

    def test_tampered_binary_extra_member_and_symlink_are_rejected(self):
        for damage in ("bytes", "extra", "link"):
            with (
                self.subTest(damage=damage),
                tempfile.TemporaryDirectory() as directory,
            ):
                root = Path(directory)
                self.fixture(root)
                if damage == "bytes":
                    (root / bridge.LIBRARY).write_bytes(b"tampered")
                elif damage == "extra":
                    (root / "unexpected").write_bytes(b"payload")
                else:
                    binary = root / bridge.LIBRARY
                    binary.unlink()
                    binary.symlink_to(bridge.SOURCE)
                with self.assertRaises(ValueError):
                    bridge.verify(root, "arm64")

    def test_unrelated_release_commit_does_not_require_a_rebuild(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            document = self.fixture(root)
            document["source_commit"] = "b" * 40
            (root / "apple-bridge.json").write_text(json.dumps(document))
            self.assertEqual(bridge.verify(root, "arm64"), root / bridge.LIBRARY)

    def test_macho_architecture_is_checked_independently_of_metadata(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            document = self.fixture(root)
            binary = root / bridge.LIBRARY
            binary.write_bytes(b"\xcf\xfa\xed\xfe" + (0x1000007).to_bytes(4, "little"))
            document["sha256"] = bridge.digest(binary)
            (root / "apple-bridge.json").write_text(json.dumps(document))
            with self.assertRaisesRegex(ValueError, "Mach-O"):
                bridge.verify(root, "arm64")
