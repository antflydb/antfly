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
import re
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import qualify_write_pool_fixtures as fixtures


class FixtureAcquisitionTests(unittest.TestCase):
    def test_lighton_import_preserves_cache_and_installs_canonical_reader_profile(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            cache = root / "cache"
            cache.mkdir()
            (cache / "config.json").write_text("{}")
            (cache / "model.safetensors").write_bytes(b"pinned weights")
            (cache / "chat_template.jinja").write_text("raw hub template")
            models = root / "models"
            alias = "lightonai/LightOnOCR-3-0.8B"
            fixtures.install_alias(models, "readers", alias, cache)
            fixtures.install_alias(models, "readers", alias, cache)
            installed = models / "readers" / alias
            self.assertFalse(installed.is_symlink())
            self.assertFalse((installed / "model.safetensors").is_symlink())
            self.assertEqual(
                (installed / "model.safetensors").read_bytes(), b"pinned weights"
            )
            self.assertEqual(
                (cache / "chat_template.jinja").read_text(), "raw hub template"
            )
            self.assertIn(
                "lightonocr3",
                json.loads((installed / "model_manifest.json").read_text())[
                    "capabilities"
                ],
            )
            self.assertNotEqual(
                (installed / "chat_template.jinja").read_text(), "raw hub template"
            )

    def test_alias_materializes_existing_external_symlinks(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            cache = root / "cache"
            cache.mkdir()
            (cache / "config.json").write_text("{}")
            (cache / "nested").mkdir()
            (cache / "nested/config.json").write_text("{}")
            models = root / "models"
            link = models / "transcribers/openai/whisper-small"
            link.parent.mkdir(parents=True)
            link.symlink_to(cache, target_is_directory=True)
            with patch.object(
                fixtures.os,
                "link",
                side_effect=OSError(fixtures.errno.EXDEV, "cross-device"),
            ):
                fixtures.install_alias(
                    models, "transcribers", "openai/whisper-small", cache
                )
            self.assertFalse(link.is_symlink())
            self.assertFalse((link / "config.json").is_symlink())
            self.assertEqual((link / "config.json").read_text(), "{}")
            fixtures.install_alias(
                models, "transcribers", "openai/whisper-small", cache
            )

    def test_reader_import_fixtures_match_the_runtime_contract(self):
        source = (
            fixtures.ROOT / "zig/pkg/inference/src/readers/lightonocr3.zig"
        ).read_text()
        declared = re.search(r"pub const manifest_json =\s*\\\\([^\n]+)", source).group(
            1
        )
        assets = fixtures.ROOT / "zig/pkg/inference/scripts/lightonocr3"
        self.assertEqual(
            json.loads(declared),
            json.loads((assets / "runtime_model_manifest.json").read_text()),
        )
        section = source.split("pub const chat_template =", 1)[1].split(";", 1)[0]
        strings = re.findall(r'"(?:[^"\\]|\\.)*"', section)
        self.assertEqual(
            "".join(json.loads(item) for item in strings),
            (assets / "runtime_chat_template.jinja").read_text(),
        )

    def test_verified_download_is_reused_without_network(self):
        data = b"pinned artifact"
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "model.bin"
            digest = hashlib.sha256(data).hexdigest()
            with patch.object(
                fixtures.urllib.request, "urlopen", return_value=io.BytesIO(data)
            ) as request:
                receipt = fixtures.acquire(
                    "https://example.invalid/model", path, sha256=digest, size=len(data)
                )
                again = fixtures.acquire(
                    "https://example.invalid/model", path, sha256=digest, size=len(data)
                )
            self.assertEqual(receipt, again)
            self.assertEqual(request.call_count, 1)

    def test_corrupt_download_is_not_published(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "model.bin"
            with patch.object(
                fixtures.urllib.request, "urlopen", return_value=io.BytesIO(b"wrong")
            ):
                with self.assertRaisesRegex(ValueError, "SHA-256 mismatch"):
                    fixtures.acquire(
                        "https://example.invalid/model", path, sha256="0" * 64
                    )
            self.assertFalse(path.exists())

    def test_corrupt_cache_is_not_overwritten(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "model.bin"
            path.write_bytes(b"wrong")
            with patch.object(fixtures.urllib.request, "urlopen") as request:
                with self.assertRaises(ValueError):
                    fixtures.acquire(
                        "https://example.invalid/model", path, sha256="0" * 64
                    )
            request.assert_not_called()
            self.assertEqual(path.read_bytes(), b"wrong")

    def test_small_artifacts_verify_git_blob_identity(self):
        data = b"{}\n"
        blob = hashlib.sha1(f"blob {len(data)}\0".encode() + data).hexdigest()
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "config.json"
            path.write_bytes(data)
            fixtures.verify(path, blob_id=blob)
            with self.assertRaisesRegex(ValueError, "Git blob mismatch"):
                fixtures.verify(path, blob_id="0" * 40)

    def test_snapshot_rejects_revision_drift_and_unsafe_paths(self):
        for metadata in (
            {"sha": "other", "siblings": []},
            {"sha": "pinned", "siblings": [{"rfilename": "../config.json"}]},
        ):
            with (
                self.subTest(metadata=metadata),
                tempfile.TemporaryDirectory() as temporary,
            ):
                with patch.object(
                    fixtures.urllib.request,
                    "urlopen",
                    return_value=io.BytesIO(json.dumps(metadata).encode()),
                ):
                    with self.assertRaises(ValueError):
                        fixtures.snapshot(
                            Path(temporary),
                            ("extractors", "alias", "owner/model", "pinned"),
                        )


if __name__ == "__main__":
    unittest.main()
