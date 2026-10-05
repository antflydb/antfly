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

import copy
import json
import subprocess
import tempfile
import unittest
from pathlib import Path

from release_channels import build_release_spec
from verify_embedded_publish_inputs import (
    CONTROLLER_PATH,
    verify_identity,
    verify_producer,
)

REPOSITORY = "antflydb/antfly"
WORKFLOW = {"id": 123, "path": CONTROLLER_PATH}


def producer(commit="1" * 40):
    return {
        "name": "Release build",
        "workflow_id": WORKFLOW["id"],
        "path": CONTROLLER_PATH,
        "event": "workflow_run",
        "status": "completed",
        "conclusion": "success",
        "head_branch": "main",
        "head_sha": commit,
        "repository": {"full_name": REPOSITORY},
        "head_repository": {"full_name": REPOSITORY},
    }


class EmbeddedPublishTests(unittest.TestCase):
    def test_display_name_does_not_authenticate_the_producer(self):
        run = producer()
        run.update(
            event="pull_request", workflow_id=456, path=".github/workflows/pr.yml"
        )
        with self.assertRaisesRegex(ValueError, "artifact producer"):
            verify_producer(run, WORKFLOW, REPOSITORY, "main")

    def test_producer_requires_controller_identity_and_trusted_execution(self):
        for field, value in {
            "workflow_id": 456,
            "path": ".github/workflows/pr.yml",
            "event": "pull_request",
            "status": "in_progress",
            "conclusion": "failure",
            "head_branch": "feature",
            "repository": {"full_name": "other/repo"},
            "head_repository": {"full_name": "other/fork"},
            "head_sha": "invalid",
        }.items():
            with self.subTest(field=field):
                run = producer()
                run[field] = value
                with self.assertRaises(ValueError):
                    verify_producer(run, WORKFLOW, REPOSITORY, "main")
        self.assertEqual(
            verify_producer(producer(), WORKFLOW, REPOSITORY, "main"), "1" * 40
        )

    def test_workflow_lookup_must_resolve_the_expected_controller(self):
        workflow = dict(WORKFLOW, path=".github/workflows/other.yml")
        with self.assertRaises(ValueError):
            verify_producer(producer(), workflow, REPOSITORY, "main")

    def test_identity_binds_request_tag_controller_products_and_git_history(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)

            def git(*args):
                return subprocess.check_output(
                    ["git", "-C", str(root), *args], text=True
                ).strip()

            git("init", "-q", "--initial-branch=main")
            git("config", "user.name", "Release Tests")
            git("config", "user.email", "release-tests@example.invalid")
            git("commit", "-q", "--allow-empty", "-m", "source")
            source = git("rev-parse", "HEAD")
            git("commit", "-q", "--allow-empty", "-m", "controller")
            controller = git("rev-parse", "HEAD")
            git("tag", "v0.3.0", source)
            git("remote", "add", "origin", str(root))
            document = build_release_spec(
                "v0.3.0",
                "stable",
                source,
                controller,
                release_line="0.3",
                source_ref="refs/heads/main",
                source_ref_head=source,
                build_contract_schema=2,
            ).document()
            request = root / "release-request.json"

            def verify(doc):
                request.write_text(json.dumps(doc))
                return verify_identity(
                    producer(controller),
                    WORKFLOW,
                    REPOSITORY,
                    "main",
                    request,
                    "v0.3.0",
                    root,
                )

            self.assertEqual(
                verify(document),
                {
                    "commit": source,
                    "version": "0.3.0",
                    "python_version": "0.3.0",
                    "npm_tag": "latest",
                },
            )
            for field, value in {
                "build_controller_commit": "4" * 40,
                "source_ref": "refs/heads/feature",
                "release_line": "0.2",
                "build_contract_schema": 1,
                "source_commit": controller,
                "tag": "v0.3.1",
            }.items():
                with self.subTest(field=field):
                    changed = copy.deepcopy(document)
                    changed[field] = value
                    with self.assertRaises((ValueError, SystemExit)):
                        verify(changed)

            git("checkout", "-q", "--orphan", "untrusted")
            git("commit", "-q", "--allow-empty", "-m", "untrusted source")
            untrusted = git("rev-parse", "HEAD")
            git("tag", "-f", "v0.3.0", untrusted)
            git("checkout", "-q", "main")
            changed = copy.deepcopy(document)
            changed["source_commit"] = untrusted
            with self.assertRaises(subprocess.CalledProcessError):
                verify(changed)
