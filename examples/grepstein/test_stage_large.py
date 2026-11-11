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
import fcntl
import json
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest.mock import patch

import stage_large


class StageLargeTests(unittest.TestCase):
    def test_inventory_and_budgets_before_upload_and_completed_catalogs(self):
        for mode in (
            "inventory",
            "wrong-plan",
            "raw-budget",
            "changed-plan",
            "wrong-receipt",
            "locked",
            "complete",
        ):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                archives = root / "archives"
                archives.mkdir()
                source = archives / "release.zip"
                with zipfile.ZipFile(source, "w") as archive:
                    archive.writestr("DATA/document.pdf", b"pdf")
                    archive.writestr("DATA/video.mp4", b"video")
                identity = {
                    "dataset": "doj-08",
                    "item": "release",
                    "url": "https://archive.org/release.zip",
                    "file": {"name": "release.zip", "size": str(source.stat().st_size)},
                }
                plan = {
                    "sha256": "a" * 64,
                    "archives": [
                        {
                            "destination": str(source),
                            "bytes": source.stat().st_size,
                            "receipt": identity,
                        }
                    ],
                }
                (archives / "acquisition-plan.json").write_text(json.dumps(plan))
                Path(str(source) + ".json").write_text(
                    json.dumps(
                        {
                            **identity,
                            "sha256": "b" * 64,
                            "item": "wrong" if mode == "wrong-receipt" else "release",
                        }
                    )
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

                def execute(
                    command,
                    *,
                    calls=calls,
                    mode=mode,
                    plan=plan,
                    archives=archives,
                    **kwargs,
                ):
                    calls.append(command)
                    if mode == "changed-plan" and "--expected-plan-sha256" in command:
                        changed = json.loads(json.dumps(plan))
                        changed["archives"][0]["receipt"]["item"] = "other-release"
                        (archives / "acquisition-plan.json").write_text(
                            json.dumps(changed)
                        )
                    if command[1] == "stage-originals":
                        self.assertTrue((archives / ".lock").is_dir())
                        state = Path(command[command.index("--state") + 1])
                        state.mkdir(parents=True)
                        (state / "originals.json").write_text(
                            json.dumps({"uploaded": True, "antfly_loaded": False})
                        )

                lock = (root / ".stage-large.lock").open("a+")
                self.addCleanup(lock.close)
                if mode == "locked":
                    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                with patch.object(stage_large.subprocess, "run", side_effect=execute):
                    if mode in (
                        "wrong-plan",
                        "raw-budget",
                        "changed-plan",
                        "wrong-receipt",
                        "locked",
                    ):
                        with self.assertRaises(ValueError):
                            stage_large.run(args)
                    else:
                        stage_large.run(args)
                self.assertFalse((archives / ".lock").exists())
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
