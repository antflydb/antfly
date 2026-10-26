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
import os
import re
import subprocess
import sys
import tempfile
import textwrap
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
    def test_automatic_workflow_preserves_credential_separation(self):
        workflow = (
            Path(__file__).resolve().parents[2]
            / ".github/workflows/embedded-release-publish.yml"
        ).read_text()
        self.assertIn('workflows: ["Release build"]', workflow)
        self.assertIn("github.event.workflow_run.conclusion == 'success'", workflow)
        self.assertIn('echo "publish=false"', workflow)
        prepare = workflow.split("  prepare-cargo:")[1].split("  publish-cargo:")[0]
        publish = workflow.split("  publish-cargo:")[1].split("  publish-pypi:")[0]
        self.assertNotIn("id-token: write", prepare)
        self.assertIn("embedded_cargo.py build", prepare)
        self.assertIn("github_environment.py check", prepare)
        self.assertIn("environment: crates-io", publish)
        self.assertIn("id-token: write", publish)
        self.assertIn("embedded_cargo.py publish", publish)
        self.assertNotIn("embedded_cargo.py build", publish)
        self.assertNotIn("rust-toolchain@", publish)
        self.assertNotIn("--stage", publish)

    def test_oidc_publishers_require_dispatch_and_relay_cannot_publish(self):
        workflow = (
            Path(__file__).resolve().parents[2]
            / ".github/workflows/embedded-release-publish.yml"
        ).read_text()
        jobs = dict(
            re.findall(
                r"^  ([\w-]+):\n(.*?)(?=^  [\w-]+:|\Z)",
                workflow.split("\njobs:\n", 1)[1],
                re.MULTILINE | re.DOTALL,
            )
        )
        for name in ("prepare-cargo", "publish-cargo", "publish-pypi", "publish-npm"):
            with self.subTest(job=name):
                guard = re.search(r"^    if: (.+)$", jobs[name], re.MULTILINE)[1]
                self.assertIn("github.event_name == 'workflow_dispatch'", guard)
                self.assertIn("needs.prepare.outputs.publish == 'true'", guard)
        prepare = jobs["prepare"]
        self.assertNotIn("actions: write", prepare)
        self.assertNotIn("id-token: write", prepare)
        self.assertIn("verify_embedded_publish_inputs.py producer", prepare)
        self.assertIn("verify_embedded_publish_inputs.py identity", prepare)
        relay = jobs["dispatch-publication"]
        self.assertIn("needs: prepare", relay)
        self.assertIn("github.event_name == 'workflow_run'", relay)
        self.assertIn("needs.prepare.outputs.publish == 'true'", relay)
        self.assertIn("actions: write", relay)
        self.assertNotIn("id-token: write", relay)
        self.assertNotIn("environment:", relay)

    def test_handoff_dispatches_exact_inputs_only_from_main(self):
        workflow = (
            Path(__file__).resolve().parents[2]
            / ".github/workflows/embedded-release-publish.yml"
        ).read_text()
        relay = workflow.split("  dispatch-publication:\n", 1)[1].split(
            "  prepare-cargo:", 1
        )[0]
        script = textwrap.dedent(relay.split("        run: |\n", 1)[1])
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            captured = root / "arguments.json"
            stub = root / "gh"
            stub.write_text(
                "#!" + sys.executable + "\n"
                "import json,os,sys\n"
                "from pathlib import Path\n"
                "Path(os.environ['CAPTURED_ARGS']).write_text(json.dumps(sys.argv[1:]))\n"
            )
            stub.chmod(0o700)
            env = {
                **os.environ,
                "PATH": str(root) + os.pathsep + os.environ["PATH"],
                "GITHUB_REF": "refs/heads/main",
                "GITHUB_REPOSITORY": REPOSITORY,
                "RELEASE_TAG": "v0.3.0-rc.2",
                "BUILD_RUN_ID": "39688758152",
                "CAPTURED_ARGS": str(captured),
            }
            subprocess.run(["bash", "-c", script], env=env, check=True)
            self.assertEqual(
                json.loads(captured.read_text()),
                [
                    "workflow",
                    "run",
                    "embedded-release-publish.yml",
                    "--repo",
                    REPOSITORY,
                    "--ref",
                    "main",
                    "-f",
                    "tag_name=v0.3.0-rc.2",
                    "-f",
                    "build_run_id=39688758152",
                ],
            )
            captured.unlink()
            result = subprocess.run(
                ["bash", "-c", script],
                env={**env, "GITHUB_REF": "refs/heads/feature"},
                check=False,
            )
            self.assertNotEqual(result.returncode, 0)
            self.assertFalse(captured.exists())

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
                    "build_contract_schema": "2",
                    "commit": source,
                    "version": "0.3.0",
                    "python_version": "0.3.0",
                    "npm_tag": "latest",
                    "tag": "v0.3.0",
                },
            )
            combined = copy.deepcopy(document)
            combined["build_contract_schema"] = 3
            self.assertEqual(verify(combined)["build_contract_schema"], "3")
            combined["build_contract_schema"] = 4
            self.assertEqual(verify(combined)["build_contract_schema"], "4")
            combined["build_contract_schema"] = 5
            self.assertEqual(verify(combined)["build_contract_schema"], "5")
            combined["build_contract_schema"] = 6
            self.assertEqual(verify(combined)["build_contract_schema"], "6")
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

            # Automatic execution derives its immutable tag from the authenticated
            # controller artifact, including prerelease registry versions.
            git("tag", "v0.3.0-rc.1", source)
            rc = build_release_spec(
                "v0.3.0-rc.1",
                "next",
                source,
                controller,
                release_line="0.3",
                source_ref="refs/heads/main",
                source_ref_head=source,
                build_contract_schema=5,
            ).document()
            request.write_text(json.dumps(rc))
            outputs = verify_identity(
                producer(controller),
                WORKFLOW,
                REPOSITORY,
                "main",
                request,
                None,
                root,
            )
            self.assertEqual(outputs["tag"], "v0.3.0-rc.1")
            self.assertEqual(outputs["version"], "0.3.0-rc.1")
            self.assertEqual(outputs["python_version"], "0.3.0rc1")
            self.assertEqual(outputs["npm_tag"], "next")
            with self.assertRaises((ValueError, SystemExit)):
                verify_identity(
                    producer(controller),
                    WORKFLOW,
                    REPOSITORY,
                    "main",
                    request,
                    "v0.3.0",
                    root,
                )

            git("checkout", "-q", "--orphan", "untrusted")
            git("commit", "-q", "--allow-empty", "-m", "untrusted source")
            untrusted = git("rev-parse", "HEAD")
            git("tag", "-f", "v0.3.0", untrusted)
            git("checkout", "-q", "main")
            changed = copy.deepcopy(document)
            changed["source_commit"] = untrusted
            with self.assertRaises(subprocess.CalledProcessError):
                verify(changed)
