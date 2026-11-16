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

"""Exercise independent grep releases, installed archives and immutable retries."""

from __future__ import annotations

import copy
import io
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile
import tempfile
import unittest
from unittest.mock import patch

import grep_release as release

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts/packaging"))
import package_grep_release as packaging  # noqa: E402
from create_reproducible_tar import create_archive  # noqa: E402


def identity():
    return {
        "schema_version": 1,
        "product": "antfly-grep",
        "tag": "antfly-grep/v0.1.0-rc.1",
        "version": "0.1.0-rc.1",
        "source_commit": "a" * 40,
        "source_ref_head": "a" * 40,
        "build_controller_commit": "b" * 40,
        "build_run_id": 123,
        "request_run_id": 122,
        "source_date_epoch": 42,
    }


def package_fixture(root, target, doc=None):
    doc = doc or identity()
    suffix = next(p[3] for p in release.PLATFORMS if p[2] == target)
    binary = root / "fixture-bin/antfly-grep"
    binary.parent.mkdir(parents=True, exist_ok=True)
    binary.write_text(
        f"#!{sys.executable}\n"
        "import pathlib,sys\n"
        f"version = {doc['version']!r}\n"
        "if '--version' in sys.argv: print('antfly-grep '+version)\n"
        "elif '--help' in sys.argv: print('fixture help')\n"
        "else:\n"
        "    pattern=sys.argv[-2]\n"
        "    rows=pathlib.Path(sys.argv[-1]).read_text().splitlines()\n"
        "    matches=[f'{i}:{line}' for i,line in enumerate(rows,1) if pattern in line]\n"
        "    if matches: print('\\n'.join(matches))\n"
        "    sys.exit(0 if matches else 1)\n"
    )
    stage = root / "staging"
    if stage.exists():
        shutil.rmtree(stage)
    packaging.stage(
        binary,
        stage,
        version=doc["version"],
        target=target,
        commit=doc["source_commit"],
    )
    shutil.copy2(ROOT / "zig/pkg/antfly-grep/install.sh", stage / "install.sh")
    (stage / "install.sh").chmod(0o755)
    shutil.copy2(ROOT / "LICENSES/Apache-2.0.txt", stage / "LICENSE")
    shutil.copy2(ROOT / "THIRD_PARTY_NOTICES.md", stage / "THIRD_PARTY_NOTICES.md")
    shutil.copy2(ROOT / "zig/pkg/antfly-grep/README.md", stage / "README.md")
    (stage / "LICENSES").mkdir()
    shutil.copy2(ROOT / "LICENSES/Apache-2.0.txt", stage / "LICENSES/Apache-2.0.txt")
    output = root / "payload" / release.filename(doc["version"], suffix)
    create_archive(stage, output, doc["source_date_epoch"])
    return output, stage


def complete_payload(root, doc=None):
    doc = doc or identity()
    for _, _, target, _ in release.PLATFORMS:
        package_fixture(root, target, doc)
    directory = root / "payload"
    (directory / "grep-release-identity.json").write_text(json.dumps(doc))
    release.seal(directory, doc)
    return directory


class ReleaseServer:
    """Stateful GitHub fixture, including interrupted and uncertain uploads."""

    def __init__(self, directory):
        self.directory = directory
        self.doc = identity()
        self.release = {
            "id": 17,
            "tag_name": self.doc["tag"],
            "target_commitish": self.doc["source_commit"],
            "prerelease": True,
            "draft": True,
            "assets": [],
        }
        self.calls = []
        self.uploads = []
        self.deleted = []
        self.upload = None
        self.next_id = 100

    def accept(self, names):
        for name in names:
            path = self.directory / name
            self.release["assets"].append(
                {
                    "id": self.next_id,
                    "name": name,
                    "state": "uploaded",
                    "size": path.stat().st_size,
                    "digest": "sha256:" + release.digest(path),
                }
            )
            self.next_id += 1

    def starter(self, name):
        asset = {
            "id": self.next_id,
            "name": name,
            "state": "starter",
            "size": 0,
            "digest": None,
        }
        self.next_id += 1
        self.release["assets"].append(asset)
        return asset

    def execute(self, command, **kwargs):
        self.calls.append(command)
        if "--slurp" in command:
            return json.dumps([[self.release]])
        if command[1:3] == ["release", "upload"]:
            names = [Path(path).name for path in command[6:]]
            self.uploads.append(names)
            if self.upload is not None:
                self.upload(self, names)
            else:
                self.accept(names)
            return ""
        method = command[3]
        if method == "DELETE":
            asset_id = int(command[4].rsplit("/", 1)[1])
            asset = next(a for a in self.release["assets"] if a["id"] == asset_id)
            if not (
                self.release["draft"]
                and asset["state"] == "starter"
                and asset["size"] == 0
            ):
                raise AssertionError("attempted to delete immutable release bytes")
            self.deleted.append(asset_id)
            self.release["assets"].remove(asset)
            return ""  # The actual DELETE endpoint returns 204 with no JSON.
        if method == "PATCH":
            self.release.update(json.loads(kwargs["input"]))
        elif method != "GET":
            raise AssertionError(f"unexpected API mutation: {command}")
        return json.dumps(self.release)


class GrepReleaseTests(unittest.TestCase):
    def test_version_namespace_is_independent_and_canonical(self):
        for tag, expected in (
            ("antfly-grep/v1.2.3", "1.2.3"),
            ("antfly-grep/v0.1.0-rc.5", "0.1.0-rc.5"),
            ("antfly-grep/v1.0.0-beta.1", "1.0.0-beta.1"),
        ):
            self.assertEqual(release.version(tag), expected)
        for tag in (
            "v1.2.3",
            "antfly-grep/v01.2.3",
            "antfly-grep/v1.0.0-rc01",
            "antfly-grep/v1.0.0-rc.01",
            "antfly-grep/v1.0.0/evil",
            "other/v1.0.0",
        ):
            with self.subTest(tag=tag), self.assertRaises(ValueError):
                release.version(tag)

    def test_archive_installs_and_executes_without_antfly_or_libantfly(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            target = release.PLATFORMS[0][2]
            archive, _ = package_fixture(root, target)
            release.smoke(archive, identity(), target, root / "smoke")
            with tarfile.open(archive) as tar:
                names = {m.name.removeprefix("./") for m in tar}
            self.assertFalse(
                any(n == "antfly" or n.startswith(("lib/", "include/")) for n in names)
            )

    def test_archive_rejects_wrong_identity_missing_assets_and_binary_tampering(self):
        for damage in (
            "version",
            "target",
            "source",
            "binary",
            "installer",
            "libantfly",
            "traversal",
            "symlink",
        ):
            with self.subTest(damage=damage), tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                target = release.PLATFORMS[0][2]
                archive, stage = package_fixture(root, target)
                meta_path = stage / "share/antfly-grep/build.json"
                meta = json.loads(meta_path.read_text())
                if damage in {"version", "target", "source"}:
                    key = "source_commit" if damage == "source" else damage
                    meta[key] = "different"
                    meta_path.write_text(json.dumps(meta))
                elif damage == "binary":
                    (stage / "antfly-grep").write_text("changed")
                elif damage == "installer":
                    (stage / "install.sh").unlink()
                elif damage == "libantfly":
                    (stage / "lib").mkdir()
                    (stage / "lib/libantfly.so").write_bytes(b"not a grep release")
                if damage in {"traversal", "symlink"}:
                    with tarfile.open(archive, "w:gz") as tar:
                        info = tarfile.TarInfo(
                            "../outside" if damage == "traversal" else "link"
                        )
                        if damage == "symlink":
                            info.type = tarfile.SYMTYPE
                            info.linkname = "/outside"
                        tar.addfile(info, io.BytesIO())
                else:
                    create_archive(stage, archive, 42)
                with self.assertRaises(ValueError):
                    release.verify_archive(archive, identity(), target)

    def test_complete_platform_set_and_checksum_ledger_are_required(self):
        with tempfile.TemporaryDirectory() as temp:
            directory = complete_payload(Path(temp))
            self.assertEqual(release.verify_bundle(directory), identity())
            self.assertEqual(len(list(directory.iterdir())), 7)
            (directory / "SHA256SUMS").write_text("wrong ledger")
            with self.assertRaisesRegex(ValueError, "ledger"):
                release.verify_bundle(directory)
        for damage in ("missing", "unexpected", "tampered"):
            with self.subTest(damage=damage), tempfile.TemporaryDirectory() as temp:
                directory = complete_payload(Path(temp))
                archive = next(directory.glob("*.tar.gz"))
                if damage == "missing":
                    archive.unlink()
                elif damage == "unexpected":
                    (directory / "foreign.txt").write_text("foreign")
                else:
                    archive.write_bytes(b"tampered")
                with self.assertRaises(ValueError):
                    release.verify_bundle(directory)

    def test_seal_refuses_a_partial_matrix(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            package_fixture(root, release.PLATFORMS[0][2])
            directory = root / "payload"
            (directory / "grep-release-identity.json").write_text(
                json.dumps(identity())
            )
            with self.assertRaisesRegex(ValueError, "platform"):
                release.seal(directory, identity())

    def test_published_assets_are_create_or_verify_and_never_latest(self):
        for doc in (
            identity(),
            {**identity(), "tag": "antfly-grep/v0.1.0", "version": "0.1.0"},
        ):
            with tempfile.TemporaryDirectory() as temp:
                directory = complete_payload(Path(temp), doc)
                state = {"release": None}
                calls = []

                def execute(command, **kwargs):
                    calls.append(command)
                    if "--slurp" in command:
                        return json.dumps(
                            [[state["release"]] if state["release"] else []]
                        )
                    if command[1:3] == ["release", "upload"]:
                        for path in command[6:]:
                            file = Path(path)
                            state["release"]["assets"].append(
                                {
                                    "name": file.name,
                                    "state": "uploaded",
                                    "digest": "sha256:" + release.digest(file),
                                }
                            )
                        return ""
                    method = command[command.index("--method") + 1]
                    if method == "POST":
                        body = json.loads(kwargs["input"])
                        self.assertTrue(body["draft"])
                        self.assertEqual(body["prerelease"], "-" in doc["version"])
                        self.assertEqual(body["make_latest"], "false")
                        state["release"] = {**body, "id": 17, "assets": []}
                    elif method == "PATCH":
                        body = json.loads(kwargs["input"])
                        self.assertEqual(body["make_latest"], "false")
                        state["release"].update(body)
                    return json.dumps(state["release"])

                with patch.object(release, "run", side_effect=execute):
                    release.publish(directory, "antflydb/antfly", doc)
                    self.assertFalse(state["release"]["draft"])
                    count = len(calls)
                    release.publish(directory, "antflydb/antfly", doc)
                    self.assertFalse(
                        any(
                            "POST" in c or "PATCH" in c or "upload" in c
                            for c in calls[count:]
                        )
                    )
                    state["release"]["assets"][0]["digest"] = "sha256:" + "0" * 64
                    with self.assertRaisesRegex(ValueError, "never overwrite"):
                        release.publish(directory, "antflydb/antfly", doc)

    def test_draft_retry_only_uploads_missing_assets(self):
        with tempfile.TemporaryDirectory() as temp:
            directory = complete_payload(Path(temp))
            names = sorted(p.name for p in directory.iterdir())
            existing = {
                "id": 17,
                "tag_name": identity()["tag"],
                "target_commitish": identity()["source_commit"],
                "prerelease": True,
                "draft": True,
                "assets": [
                    {
                        "name": name,
                        "state": "uploaded",
                        "digest": "sha256:" + release.digest(directory / name),
                    }
                    for name in names[:-1]
                ],
            }

            def execute(command, **kwargs):
                if "--slurp" in command:
                    return json.dumps([[existing]])
                if command[1:3] == ["release", "upload"]:
                    self.assertEqual(command[6:], [str(directory / names[-1])])
                    existing["assets"].append(
                        {
                            "name": names[-1],
                            "state": "uploaded",
                            "digest": "sha256:" + release.digest(directory / names[-1]),
                        }
                    )
                    return ""
                if "PATCH" in command:
                    existing.update(json.loads(kwargs["input"]))
                return json.dumps(existing)

            with patch.object(release, "run", side_effect=execute):
                release.publish(directory, "antflydb/antfly", identity())
            self.assertFalse(existing["draft"])

    def test_asset_delete_accepts_an_empty_204_response(self):
        with patch.object(release, "run", return_value=""):
            self.assertIsNone(
                release.api("antflydb/antfly", "releases/assets/100", "DELETE")
            )
            with self.assertRaisesRegex(ValueError, "empty release metadata"):
                release.api("antflydb/antfly", "releases/17")

    def test_retry_reclaims_only_empty_starters_in_a_validated_draft(self):
        for empty_digest in (None, "sha256:" + release.hashlib.sha256(b"").hexdigest()):
            with (
                self.subTest(digest=empty_digest),
                tempfile.TemporaryDirectory() as temp,
            ):
                directory = complete_payload(Path(temp))
                names = sorted(p.name for p in directory.iterdir())
                server = ReleaseServer(directory)
                server.accept(names[:-1])
                accepted = copy.deepcopy(server.release["assets"])
                starter = server.starter(names[-1])
                starter["digest"] = empty_digest
                with patch.object(release, "run", side_effect=server.execute):
                    release.publish(directory, "antflydb/antfly", identity())
                self.assertEqual(server.deleted, [starter["id"]])
                self.assertEqual(server.uploads, [[names[-1]]])
                self.assertEqual(server.release["assets"][:-1], accepted)
                self.assertFalse(server.release["draft"])

    def test_unfinished_asset_cleanup_rejects_conflicts_before_mutating(self):
        for damage in (
            "published",
            "uploaded-empty",
            "nonempty",
            "digest",
            "bad-id",
            "unexpected",
            "duplicate",
            "completed-mismatch",
        ):
            with self.subTest(damage=damage), tempfile.TemporaryDirectory() as temp:
                directory = complete_payload(Path(temp))
                names = sorted(p.name for p in directory.iterdir())
                server = ReleaseServer(directory)
                starter = server.starter(names[0])
                if damage == "published":
                    server.release["draft"] = False
                elif damage == "uploaded-empty":
                    starter["state"] = "uploaded"
                elif damage == "nonempty":
                    starter["size"] = 1
                elif damage == "digest":
                    starter["digest"] = "sha256:" + "f" * 64
                elif damage == "bad-id":
                    starter["id"] = True
                elif damage == "unexpected":
                    starter["name"] = "foreign-file"
                elif damage == "duplicate":
                    server.accept([names[0]])
                elif damage == "completed-mismatch":
                    server.accept([names[1]])
                    server.release["assets"][-1]["digest"] = "sha256:" + "f" * 64
                with patch.object(release, "run", side_effect=server.execute):
                    with self.assertRaises(ValueError):
                        release.publish(directory, "antflydb/antfly", identity())
                self.assertEqual(server.deleted, [])
                self.assertEqual(server.uploads, [])

    def test_partial_upload_retries_only_unaccepted_bytes(self):
        with tempfile.TemporaryDirectory() as temp:
            directory = complete_payload(Path(temp))
            names = sorted(p.name for p in directory.iterdir())
            server = ReleaseServer(directory)

            def fail_once(server, missing):
                if len(server.uploads) == 1:
                    server.accept(missing[:1])
                    server.starter(missing[1])
                    raise subprocess.CalledProcessError(
                        1, ["gh", "release", "upload"], stderr="HTTP 502: Bad Gateway"
                    )
                server.accept(missing)

            server.upload = fail_once
            with (
                patch.object(release, "run", side_effect=server.execute),
                patch.object(release.time, "sleep") as sleep,
            ):
                release.publish(directory, "antflydb/antfly", identity())
            self.assertEqual(server.uploads, [names, names[1:]])
            self.assertEqual(len(server.deleted), 1)
            self.assertEqual(
                [a["name"] for a in server.release["assets"]].count(names[0]), 1
            )
            sleep.assert_called_once_with(1)
            self.assertFalse(server.release["draft"])

    def test_lost_upload_response_reconciles_without_reuploading(self):
        with tempfile.TemporaryDirectory() as temp:
            directory = complete_payload(Path(temp))
            server = ReleaseServer(directory)

            def accepted_but_response_lost(server, names):
                server.accept(names)
                raise subprocess.CalledProcessError(
                    1, ["gh", "release", "upload"], stderr="connection reset by peer"
                )

            server.upload = accepted_but_response_lost
            with (
                patch.object(release, "run", side_effect=server.execute),
                patch.object(release.time, "sleep") as sleep,
            ):
                release.publish(directory, "antflydb/antfly", identity())
            self.assertEqual(len(server.uploads), 1)
            self.assertEqual(server.deleted, [])
            sleep.assert_not_called()
            self.assertFalse(server.release["draft"])

    def test_transient_retries_are_bounded_and_later_run_can_recover(self):
        with tempfile.TemporaryDirectory() as temp:
            directory = complete_payload(Path(temp))
            server = ReleaseServer(directory)

            def unavailable(server, names):
                server.starter(names[0])
                raise subprocess.CalledProcessError(
                    1,
                    ["gh", "release", "upload"],
                    stderr="HTTP 503: Service Unavailable",
                )

            server.upload = unavailable
            with (
                patch.object(release, "run", side_effect=server.execute),
                patch.object(release.time, "sleep") as sleep,
            ):
                with self.assertRaises(subprocess.CalledProcessError):
                    release.publish(directory, "antflydb/antfly", identity())
                self.assertEqual(len(server.uploads), 3)
                self.assertEqual([c.args for c in sleep.call_args_list], [(1,), (2,)])
                self.assertTrue(server.release["draft"])
                # A normal workflow retry uses the retained server state.
                server.upload = None
                release.publish(directory, "antflydb/antfly", identity())
                self.assertFalse(server.release["draft"])
                self.assertEqual(len(server.uploads), 4)

    def test_permission_errors_are_not_retried(self):
        with tempfile.TemporaryDirectory() as temp:
            directory = complete_payload(Path(temp))
            server = ReleaseServer(directory)

            def denied(server, names):
                raise subprocess.CalledProcessError(
                    1,
                    ["gh", "release", "upload"],
                    stderr="HTTP 403: Credential renewal timeout; resource not accessible",
                )

            server.upload = denied
            with (
                patch.object(release, "run", side_effect=server.execute),
                patch.object(release.time, "sleep") as sleep,
            ):
                with self.assertRaises(subprocess.CalledProcessError):
                    release.publish(directory, "antflydb/antfly", identity())
            self.assertEqual(len(server.uploads), 1)
            sleep.assert_not_called()
            self.assertTrue(server.release["draft"])

    def test_workflows_keep_tag_code_read_only_and_publish_after_qualification(self):
        request = (ROOT / ".github/workflows/grep-release-request.yml").read_text()
        self.assertIn('- "antfly-grep/v*"', request)
        self.assertNotIn("contents: write", request)
        self.assertNotIn("actions/checkout", request)
        workflow = (ROOT / ".github/workflows/grep-release.yml").read_text()
        build = workflow.split("\n  build:\n", 1)[1].split("\n  seal:\n", 1)[0]
        publish = workflow.split("\n  publish:\n", 1)[1]
        self.assertNotIn("contents: write", build)
        self.assertIn("ref: ${{ github.sha }}", build)
        self.assertIn(
            "uses: ./.grep-controller/.github/actions/prepare-zig-dependencies", build
        )
        self.assertIn("mirror-only: 'true'", build)
        self.assertIn("Install and run the exact release archive", build)
        self.assertIn('-Dgrep-version="$VERSION"', build)
        self.assertNotIn("-Dantfly-version", build)
        self.assertIn("needs: [resolve, seal]", publish)
        self.assertIn("environment: release-promotion", publish)
        self.assertIn("contents: write", publish)
        self.assertNotIn("inputs.source_commit", publish)
        self.assertIn("ref: ${{ github.sha }}", publish)
        self.assertEqual(len(release.matrix()["include"]), 5)
        docs = (ROOT / ".github/workflows/trigger-docs-build.yml").read_text()
        self.assertIn(
            "!startsWith(github.event.release.tag_name, 'antfly-grep/')", docs
        )


class GrepReleaseGitTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.repo = self.root / "repo"
        subprocess.run(["git", "init", "-q", "-b", "main", str(self.repo)], check=True)
        release.git(self.repo, "config", "user.name", "Test")
        release.git(self.repo, "config", "user.email", "test@example.com")
        for path in release.CONTROL_PATHS:
            file = self.repo / path
            file.parent.mkdir(parents=True, exist_ok=True)
            file.write_text("trusted controller\n")
        release.git(self.repo, "add", ".")
        release.git(self.repo, "commit", "-qm", "trusted main")
        self.commit = release.git(self.repo, "rev-parse", "HEAD")
        self.tag = identity()["tag"]
        release.git(self.repo, "tag", "-a", self.tag, "-m", self.tag)
        self.origin = self.root / "origin.git"
        subprocess.run(
            ["git", "clone", "-q", "--bare", str(self.repo), str(self.origin)],
            check=True,
        )
        release.git(self.repo, "remote", "add", "origin", str(self.origin))
        self.request = {
            "schema_version": 1,
            "tag": self.tag,
            "source_commit": self.commit,
        }
        self.event = {
            "repository": {"full_name": "antflydb/antfly"},
            "workflow_run": {
                "name": "Grep release request",
                "path": release.REQUEST_WORKFLOW,
                "event": "push",
                "conclusion": "success",
                "head_branch": self.tag,
                "head_sha": self.commit,
                "id": 122,
                "head_repository": {"full_name": "antflydb/antfly"},
            },
        }

    def resolve(self, request=None, event=None):
        return release.resolve(
            self.repo,
            request or self.request,
            event or self.event,
            "antflydb/antfly",
            self.commit,
            123,
            "main",
        )

    def test_request_is_bound_to_main_and_actual_tag_event(self):
        doc = self.resolve()
        self.assertEqual(doc["source_commit"], self.commit)
        self.assertEqual(doc["build_controller_commit"], self.commit)
        self.assertEqual(doc["version"], "0.1.0-rc.1")
        self.assertEqual(doc["source_ref_head"], self.commit)
        for key, value in (
            ("path", ".github/workflows/other.yml"),
            ("event", "workflow_dispatch"),
            ("head_sha", "c" * 40),
            ("head_branch", "other-tag"),
            ("head_repository", {"full_name": "foreign/repo"}),
        ):
            event = copy.deepcopy(self.event)
            event["workflow_run"][key] = value
            with self.subTest(key=key), self.assertRaises(ValueError):
                self.resolve(event=event)

    def test_foreign_branch_commit_cannot_be_released(self):
        release.git(self.repo, "checkout", "-qb", "foreign")
        (self.repo / "foreign.txt").write_text("not on main")
        release.git(self.repo, "add", ".")
        release.git(self.repo, "commit", "-qm", "foreign")
        foreign = release.git(self.repo, "rev-parse", "HEAD")
        release.git(self.repo, "tag", "-f", self.tag)
        release.git(
            self.repo, "push", "-q", "--force", "origin", f"refs/tags/{self.tag}"
        )
        release.git(self.repo, "checkout", "-q", "main")
        request = {**self.request, "source_commit": foreign}
        event = copy.deepcopy(self.event)
        event["workflow_run"]["head_sha"] = foreign
        with self.assertRaises(subprocess.CalledProcessError):
            self.resolve(request, event)

    def test_moved_tag_and_changed_release_policy_fail_closed(self):
        doc = self.resolve()
        (self.repo / release.CONTROL_PATHS[1]).write_text("changed controller\n")
        release.git(self.repo, "add", ".")
        release.git(self.repo, "commit", "-qm", "change policy")
        release.git(self.repo, "push", "-q", "origin", "main")
        with self.assertRaisesRegex(ValueError, "policy changed"):
            release.verify_source(self.repo, doc, "main")
        release.git(self.repo, "tag", "-f", self.tag)
        release.git(
            self.repo, "push", "-q", "--force", "origin", f"refs/tags/{self.tag}"
        )
        with self.assertRaisesRegex(ValueError, "tag moved"):
            release.verify_source(self.repo, doc, "main")


if __name__ == "__main__":
    unittest.main()
