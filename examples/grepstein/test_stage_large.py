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

import argparse
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import zipfile

import stage_large


class StageLargeTests(unittest.TestCase):
    def test_inventory_and_budgets_before_upload_and_completed_catalogs(self):
        for mode in ("inventory", "wrong-plan", "raw-budget", "complete"):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                archives = root / "archives"
                archives.mkdir()
                source = archives / "release.zip"
                with zipfile.ZipFile(source, "w") as archive:
                    archive.writestr("DATA/document.pdf", b"pdf")
                    archive.writestr("DATA/video.mp4", b"video")
                plan = {"sha256": "a" * 64, "archives": [{"destination": str(source)}]}
                (archives / "acquisition-plan.json").write_text(json.dumps(plan))
                Path(str(source) + ".json").write_text(
                    json.dumps({"dataset": "doj-08", "sha256": "b" * 64})
                )
                args = argparse.Namespace(
                    state=root,
                    grepstein=root / "grepstein",
                    bucket="private-originals",
                    max_compressed_bytes=100,
                    max_raw_bytes=1 if mode == "raw-budget" else 100,
                    max_object_bytes=100,
                    execute=mode != "inventory",
                    expected_plan_sha256="wrong"
                    if mode == "wrong-plan"
                    else plan["sha256"],
                )
                calls = []

                def execute(command, **kwargs):
                    calls.append(command)
                    if command[1] == "stage-originals":
                        state = Path(command[command.index("--state") + 1])
                        state.mkdir(parents=True)
                        (state / "originals.json").write_text(
                            json.dumps({"uploaded": True, "antfly_loaded": False})
                        )

                with patch.object(stage_large.subprocess, "run", side_effect=execute):
                    if mode in ("wrong-plan", "raw-budget"):
                        with self.assertRaises(ValueError):
                            stage_large.run(args)
                    else:
                        stage_large.run(args)
                self.assertTrue(
                    all(
                        command[1] in ("fetch-archive", "stage-originals")
                        for command in calls
                    )
                )
                uploads = [
                    command for command in calls if command[1] == "stage-originals"
                ]
                self.assertEqual(len(uploads), 1 if mode == "complete" else 0)
                self.assertEqual(
                    (root / "staging-complete.json").exists(), mode == "complete"
                )
                if mode == "wrong-plan":
                    self.assertEqual(len(calls), 1)
                if mode == "complete":
                    receipt = json.loads((root / "staging-complete.json").read_text())
                    self.assertFalse(receipt["antfly_loaded"])
                    self.assertEqual(receipt["raw_bytes"], 8)


if __name__ == "__main__":
    unittest.main()
