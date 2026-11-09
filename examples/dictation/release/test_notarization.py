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
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import notarization as n
import release


class NotarizationTests(unittest.TestCase):
    def identity(self):
        identity = release.version("dictation/v0.1.0-beta.1")
        identity.pop("order")
        return dict(
            identity,
            tag="dictation/v0.1.0-beta.1",
            source_commit="a" * 40,
            controller_commit="b" * 40,
        )

    def test_exact_signed_bytes_and_identity_are_required_for_resume(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            (directory / "signed.zip").write_bytes(b"signed archive")
            submission = "b0b6c2a9-ea11-4a26-af8f-9a82251d8005"
            (directory / "submission.json").write_text(json.dumps({"id": submission}))
            identity = self.identity()
            n.record(directory, identity)
            self.assertEqual(n.validate(directory, identity), submission)
            with self.assertRaises(ValueError):
                n.validate(directory, dict(identity, source_commit="c" * 40))
            (directory / "signed.zip").write_bytes(b"different archive")
            with self.assertRaises(ValueError):
                n.validate(directory, identity)

    def producer(self):
        return {
            "path": ".github/workflows/dictation-release.yml",
            "event": "workflow_run",
            "head_branch": "main",
            "status": "completed",
            "head_sha": "b" * 40,
        }

    def jobs(self):
        return {
            "jobs": [
                {
                    "name": "sign",
                    "steps": [
                        {"name": name, "conclusion": "success"}
                        for name in (
                            "Developer ID sign and submit to Apple",
                            "Preserve signed notarization submission",
                        )
                    ],
                }
            ]
        }

    def test_only_completed_trusted_producers_with_saved_signing_checkpoint_can_resume(
        self,
    ):
        for field, value in (
            ("path", ".github/workflows/pr-ci.yml"),
            ("event", "pull_request"),
            ("event", "workflow_dispatch"),
            ("head_branch", "topic"),
            ("status", "in_progress"),
            ("head_sha", "invalid"),
        ):
            with (
                self.subTest(field=field, value=value),
                patch.object(
                    release,
                    "gh_api",
                    return_value=dict(self.producer(), **{field: value}),
                ),
            ):
                with self.assertRaises(ValueError):
                    n.producer("123")
        with patch.object(
            release, "gh_api", side_effect=[self.producer(), {"jobs": []}]
        ):
            with self.assertRaises(ValueError):
                n.producer("123")
        with patch.object(
            release, "gh_api", side_effect=[self.producer(), self.jobs()]
        ):
            self.assertEqual(n.producer("123")["head_sha"], "b" * 40)

    def test_resume_checks_controller_and_current_policy_before_emitting_outputs(self):
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "outputs"
            with (
                patch.dict(os.environ, GITHUB_OUTPUT=str(output)),
                patch.object(n, "producer", return_value=self.producer()),
                patch.object(release, "verify_source") as verify,
            ):
                n.resume("123", self.identity())
                verify.assert_called_once_with(self.identity())
                self.assertIn("tag=dictation/v0.1.0-beta.1", output.read_text())
                with self.assertRaises(ValueError):
                    n.resume("123", dict(self.identity(), controller_commit="c" * 40))

    def test_workflow_preserves_checkpoint_before_wait_and_never_rebuilds_on_resume(
        self,
    ):
        workflow = (
            Path(__file__).resolve().parents[3]
            / ".github/workflows/dictation-release.yml"
        ).read_text()
        self.assertLess(
            workflow.index("name: Preserve signed notarization submission"),
            workflow.index(
                "name: Wait for notarization, staple, and sign Sparkle payloads"
            ),
        )
        shell = (Path(__file__).parent / "sign-release.sh").read_text()
        finish = shell.split("# Resume the exact signed bytes;")[1]
        self.assertNotIn("notarytool submit", finish)
        self.assertNotIn("sign-app.sh", finish)
        self.assertIn("notarytool wait", finish)
