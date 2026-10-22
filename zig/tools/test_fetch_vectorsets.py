#!/usr/bin/env python3
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
import json
import sys
import tempfile
import unittest
from pathlib import Path


SCRIPT = Path(__file__).with_name("fetch_vectorsets.py")
SPEC = importlib.util.spec_from_file_location("fetch_vectorsets", SCRIPT)
assert SPEC and SPEC.loader
fetch = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = fetch
SPEC.loader.exec_module(fetch)

MANIFEST = Path(__file__).resolve().parents[1] / "testdata" / "vectorsets.manifest.json"


class FetchVectorsetsTest(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        root = Path(temp.name)
        self.mirror = root / "mirror"
        self.dest = root / "dest"
        self.mirror.mkdir()
        self.payloads = {"a.pbvec": b"alpha" * 100, "b.pbvec": b"beta" * 7}
        for name, payload in self.payloads.items():
            (self.mirror / name).write_bytes(payload)
        self.manifest_path = root / "manifest.json"
        self.write_manifest(self.payloads)

    def write_manifest(self, payloads):
        self.manifest_path.write_text(
            json.dumps(
                {
                    "base_url": self.mirror.as_uri() + "/",
                    "files": [
                        {
                            "name": name,
                            "size": len(payload),
                            "sha256": hashlib.sha256(payload).hexdigest(),
                        }
                        for name, payload in payloads.items()
                    ],
                }
            )
        )

    def run_main(self, *extra):
        return fetch.main(
            ["--manifest", str(self.manifest_path), "--dest", str(self.dest), *extra]
        )

    def test_fetches_and_verifies_missing_files(self):
        self.assertEqual(0, self.run_main())
        for name, payload in self.payloads.items():
            self.assertEqual(payload, (self.dest / name).read_bytes())

    def test_reuses_verified_files_without_fetching(self):
        self.assertEqual(0, self.run_main())
        for path in self.mirror.iterdir():
            path.unlink()
        manifest = fetch.load_manifest(self.manifest_path)
        self.assertEqual([], fetch.ensure(manifest, self.dest, allow_fetch=False))

    def test_no_fetch_fails_on_missing_file(self):
        self.assertEqual(1, self.run_main("--no-fetch"))
        self.assertFalse(self.dest.exists() and any(self.dest.iterdir()))

    def test_refetches_corrupt_file(self):
        self.assertEqual(0, self.run_main())
        (self.dest / "a.pbvec").write_bytes(b"tampered")
        self.assertEqual(1, self.run_main("--no-fetch"))
        self.assertEqual(0, self.run_main())
        self.assertEqual(self.payloads["a.pbvec"], (self.dest / "a.pbvec").read_bytes())

    def test_rejects_hash_mismatch_without_leaving_partial_file(self):
        (self.mirror / "b.pbvec").write_bytes(b"beta" * 7 + b"!")
        self.write_manifest({"b.pbvec": self.payloads["b.pbvec"]})
        self.assertEqual(1, self.run_main())
        self.assertEqual([], list(self.dest.iterdir()))

    def test_rejects_path_traversal_names(self):
        self.manifest_path.write_text(
            json.dumps(
                {
                    "base_url": "file:///",
                    "files": [{"name": "../x", "size": 1, "sha256": "0"}],
                }
            )
        )
        with self.assertRaises(fetch.FixtureError):
            fetch.load_manifest(self.manifest_path)

    def test_checked_in_manifest_is_well_formed(self):
        manifest = fetch.load_manifest(MANIFEST)
        self.assertTrue(
            manifest.base_url.startswith(
                "https://raw.githubusercontent.com/antflydb/antfly/"
            )
        )
        names = [fixture.name for fixture in manifest.files]
        self.assertEqual(sorted(names), names)
        self.assertEqual(len(names), len(set(names)))
        for fixture in manifest.files:
            self.assertTrue(fixture.name.endswith(".pbvec"))
            self.assertEqual(64, len(fixture.sha256))
            self.assertGreater(fixture.size, 0)


if __name__ == "__main__":
    unittest.main()
