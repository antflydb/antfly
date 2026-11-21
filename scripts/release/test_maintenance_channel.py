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

"""Maintenance releases advance independently of the global stable channel."""

import copy
import json
import os
import subprocess
import sys
import tempfile
import textwrap
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent))
import release_channel_state as state
import release_channels as channels
from build_release_payload import verify_release_spec
from publish_objectstorage import main as publish_objects

ROOT = Path(__file__).resolve().parents[2]


class MemoryStore:
    def __init__(self, channel, tag):
        self.document = {
            "schema_version": 1,
            "channel": channel,
            "current": state.release_identity(tag, "a" * 40, "b" * 64, channel),
            "pending": None,
        }
        self.etag = 0
        self.receipts = []

    def load(self):
        return state.StoredState(copy.deepcopy(self.document), str(self.etag))

    def compare_and_swap(self, previous, document):
        assert previous.etag == str(self.etag)
        self.document = copy.deepcopy(document)
        self.etag += 1

    def create_completion(self, receipt):
        self.receipts.append(receipt)


class MaintenanceChannelTests(unittest.TestCase):
    def test_routing_preserves_historical_specs(self):
        for tag, expected in (
            ("v0.3.3", "v0-3"),
            ("v0.4.0", "stable"),
            ("v0.4.0-rc.1", "next"),
        ):
            with self.subTest(tag=tag):
                self.assertEqual(channels.resolve_channel(tag)[0], expected)
        spec = channels.build_release_spec("v0.3.2", "stable", "a" * 40, "b" * 40)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "spec.json"
            path.write_text(json.dumps(spec.document()))
            self.assertEqual(
                verify_release_spec(path, "v0.3.2", "a" * 40)["channel"], "stable"
            )

    def test_patch_after_v04_leaves_global_stable_untouched(self):
        stable = MemoryStore("stable", "v0.4.0")
        maintenance = MemoryStore("v0-3", "v0.3.3")
        original = copy.deepcopy(stable.document)
        candidate = state.release_identity(
            "v0.3.4",
            "c" * 40,
            "d" * 64,
            "v0-3",
            container_digest="sha256:" + "e" * 64,
        )
        state.begin_promotion(maintenance, candidate, None, "v0-3")
        state.begin_promotion(maintenance, candidate, None, "v0-3")
        state.finish_promotion(maintenance, candidate, "v0-3")
        self.assertEqual(maintenance.document["current"]["tag"], "v0.3.4")
        self.assertEqual(maintenance.receipts[0]["channel"], "v0-3")
        self.assertEqual(stable.document, original)
        with self.assertRaisesRegex(SystemExit, "cannot move backward"):
            state.preflight_promotion(stable, candidate, None, "stable")
        older = state.release_identity("v0.3.3", "f" * 40, "a" * 64, "v0-3")
        with self.assertRaisesRegex(SystemExit, "cannot move backward"):
            state.begin_promotion(maintenance, older, None, "v0-3")

    def test_other_lines_and_prereleases_cannot_use_series_channel(self):
        for tag in ("v0.2.6", "v0.4.0", "v0.3.4-rc.1"):
            with self.subTest(tag=tag), self.assertRaises(SystemExit):
                channels.resolve_channel(tag, "v0-3")

    def test_policy_rejects_maintenance_channels_with_global_destinations(self):
        policy = channels.load_policy()
        for key, value in (
            ("npm_tag", "latest"),
            ("container_alias", "latest"),
            ("object_alias", "latest"),
            ("github_release", "latest"),
            ("publish_homebrew", True),
            ("release_line", "invalid"),
        ):
            with self.subTest(key=key), tempfile.TemporaryDirectory() as directory:
                changed = copy.deepcopy(policy)
                changed["channels"]["v0-3"][key] = value
                path = Path(directory) / "policy.json"
                path.write_text(json.dumps(changed))
                with self.assertRaises(SystemExit):
                    channels.load_policy(path)

    def test_object_publication_does_not_touch_latest(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            metadata = root / "metadata.json"
            metadata.write_text('{"tag":"v0.3.3"}\n')
            latest = root / "bucket/antfly/latest/metadata.json"
            latest.parent.mkdir(parents=True)
            latest.write_text('{"tag":"v0.4.0"}\n')
            args = [
                "publish",
                "--provider",
                "local",
                "--local-root",
                str(root),
                "--bucket",
                "bucket",
                "--prefix",
                "antfly/v0.3.3",
                "--channel",
                "v0-3",
                str(metadata),
            ]
            with mock.patch.object(sys, "argv", args):
                self.assertEqual(publish_objects(), 0)
            self.assertEqual(latest.read_text(), '{"tag":"v0.4.0"}\n')
            self.assertEqual(
                (root / "bucket/antfly/v0-3/metadata.json").read_bytes(),
                metadata.read_bytes(),
            )

    def test_actual_github_publication_disables_latest(self):
        workflow = (ROOT / ".github/workflows/antfly-release.yml").read_text()
        step = workflow.split("      - name: Publish the verified GitHub release\n", 1)[
            1
        ].split("\n  complete-release-channel:", 1)[0]
        script = textwrap.dedent(step.split("        run: |\n", 1)[1])
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            captured = root / "args"
            gh = root / "gh"
            gh.write_text('#!/bin/sh\nprintf "%s\\n" "$@" > "$CAPTURED"\n')
            gh.chmod(0o700)
            subprocess.run(
                ["bash", "-c", script],
                check=True,
                env={
                    **os.environ,
                    "PATH": str(root) + os.pathsep + os.environ["PATH"],
                    "CAPTURED": str(captured),
                    "RELEASE_TAG": "v0.3.3",
                    "GITHUB_RELEASE_MODE": "release",
                },
            )
            self.assertEqual(
                captured.read_text().splitlines(),
                [
                    "release",
                    "edit",
                    "v0.3.3",
                    "--draft=false",
                    "--prerelease=false",
                    "--latest=false",
                ],
            )


if __name__ == "__main__":
    unittest.main()
