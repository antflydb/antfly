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
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import Mock

import migrate_hub_bundles as migration


class MigrationTests(unittest.TestCase):
    def test_cards_preserve_evidence_and_publish_precision_specific_contracts(self):
        card = "---\nlicense: apache-2.0\n---\nOriginal measured evidence."
        for _, name in migration.MIGRATIONS:
            result = migration.metadata(
                name, card, {"family": "gliner2_split_bundle/v1", "wrapper": "gliner2"}
            )
            self.assertTrue(
                result["README.md"].decode().startswith("---\nlicense: apache-2.0\n---")
            )
            self.assertIn(b"Original measured evidence.", result["README.md"])
            self.assertIn(b"FP32", result["README.md"])
            descriptor = json.loads(result["antfly_inference_bundle.json"])
            variant = json.loads(result["antfly_inference_variants.json"])["variants"][
                0
            ]
            self.assertEqual(
                (descriptor["encoder"], descriptor["head"]),
                (variant["encoder"], variant["head"]),
            )
            self.assertNotIn("tasks", descriptor)
            if name.endswith("1B"):
                self.assertIn(b"1024 prepared tokens per sequence", result["README.md"])
                self.assertIn(b"empty results", result["README.md"])
            else:
                self.assertIn(b"exporter strips", result["README.md"])

    def test_publish_rejects_modified_reviewed_metadata_before_network_mutation(self):
        with TemporaryDirectory() as temporary:
            directory = Path(temporary)
            folder = directory / "GLiNER2.5-Decide-1B"
            folder.mkdir()
            (folder / "README.md").write_bytes(b"changed")
            (directory / "plan.json").write_text(
                json.dumps(
                    [
                        {
                            "source": "antflydb/old",
                            "destination": "antflydb/GLiNER2.5-Decide-1B",
                            "revision": "pin",
                            "updates": {"README.md": migration.pin(b"reviewed")},
                        }
                    ]
                )
            )
            api = Mock()
            with self.assertRaisesRegex(ValueError, "metadata changed"):
                migration.apply(api, directory)
            self.assertEqual(api.mock_calls, [])

    def test_publish_rejects_concurrent_hub_edits(self):
        with TemporaryDirectory() as temporary:
            directory = Path(temporary)
            (directory / "plan.json").write_text(
                json.dumps(
                    [
                        {
                            "source": "antflydb/old",
                            "destination": "antflydb/GLiNER2.5-Decide-1B",
                            "revision": "reviewed",
                            "updates": {},
                        }
                    ]
                )
            )
            api = Mock()
            api.model_info.return_value = SimpleNamespace(sha="new-commit")
            with self.assertRaisesRegex(ValueError, "source changed"):
                migration.apply(api, directory)
            api.create_commit.assert_not_called()
            api.move_repo.assert_not_called()


if __name__ == "__main__":
    unittest.main()
