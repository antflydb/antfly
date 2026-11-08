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
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import qualify_write_pool_fixtures as fixtures


class FixtureAcquisitionTests(unittest.TestCase):
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
