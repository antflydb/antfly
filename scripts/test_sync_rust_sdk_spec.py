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

import tempfile
import unittest
from pathlib import Path

from sync_rust_sdk_spec import synchronize


class RustSdkSpecTests(unittest.TestCase):
    def test_stale_and_missing_package_inputs_are_checked_without_mutation(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "rs/crates/sdk").mkdir(parents=True)
            source = root / "openapi.yaml"
            target = root / "rs/crates/sdk/openapi.yaml"
            source.write_bytes(b"openapi: 3.0.3\n")
            self.assertFalse(synchronize(root, True))
            self.assertFalse(target.exists())
            self.assertTrue(synchronize(root, False))
            self.assertTrue(synchronize(root, True))
            source.write_bytes(b"openapi: 3.0.3\ninfo: {}\n")
            self.assertFalse(synchronize(root, True))
            self.assertNotEqual(source.read_bytes(), target.read_bytes())
            self.assertTrue(synchronize(root, False))
            self.assertEqual(source.read_bytes(), target.read_bytes())
