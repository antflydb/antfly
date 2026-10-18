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
import json
from pathlib import Path
import re
import unittest


class LiteEmbeddingBundleTests(unittest.TestCase):
    def test_ci_bundle_matches_runtime_registry_pins_and_profile(self):
        root = Path(__file__).resolve().parents[2]
        receipt = json.loads(
            (
                root / "scripts/testdata/eval_extract/qwen_embedding_bundle.json"
            ).read_text()
        )
        catalog = (root / receipt["source_catalog"]).read_text()
        for name, pin in receipt["model_files"].items():
            match = re.search(
                r'\.path = "'
                + re.escape(name)
                + r'",\s*\.size = ([0-9_]+),\s*\.sha256 = "([a-f0-9]+)"',
                catalog,
            )
            self.assertIsNotNone(match, name)
            self.assertEqual(int(match[1].replace("_", "")), pin["size_bytes"])
            self.assertEqual(match[2], pin["sha256"])
            self.assertIn('"' + pin.get("model_id", receipt["model_id"]) + '"', catalog)
            self.assertIn('"' + pin.get("revision", receipt["revision"]) + '"', catalog)
        digest = hashlib.sha256(receipt["model_manifest"].encode()).hexdigest()
        self.assertEqual(receipt["model_manifest_sha256"], digest)
        self.assertIn('"' + digest + '"', catalog)


if __name__ == "__main__":
    unittest.main()
