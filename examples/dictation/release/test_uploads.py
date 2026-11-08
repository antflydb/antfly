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

"""Exercise GitHub upload recovery against a stateful, in-memory service."""

import copy
import hashlib
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import release as r


class UploadRecoveryTests(unittest.TestCase):
    def setUp(self):
        assets = patch.object(
            r, "with_complete_assets", side_effect=lambda repo, release, token: release
        )
        assets.start()
        self.addCleanup(assets.stop)
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.directory = Path(self.temporary.name)
        self.identity = {
            "tag": "dictation/v1.0.0",
            "source_commit": "a" * 40,
            "channel": "stable",
            "version": "1.0.0",
            "bundle_version": "1.0.0",
            "controller_commit": "b" * 40,
        }
        for name in ("app.zip", "appcast.xml", "SHA256SUMS", "artifacts.json"):
            (self.directory / name).write_bytes(("synthetic " + name).encode())
        self.expected = {p.name: r.digest(p) for p in self.directory.iterdir()}
        self.remote = {
            "id": 7,
            "tag_name": self.identity["tag"],
            "target_commitish": self.identity["source_commit"],
            "prerelease": False,
            "draft": True,
            "assets": [],
        }
        self.requests = []
        self.uploads = []
        self.sleeps = []
        self.behaviors = []
        self.next_id = 100
        self.delete_failure = None

    def asset(self, name, *, state="uploaded", size=None, digest=None):
        self.next_id += 1
        result = {
            "id": self.next_id,
            "name": name,
            "state": state,
            "size": (self.directory / name).stat().st_size if size is None else size,
            "digest": "sha256:" + self.expected[name] if digest is None else digest,
        }
        if state == "starter":
            result.update(
                size=0 if size is None else size,
                digest=None if digest is None else digest,
            )
        self.remote["assets"].append(result)
        return result

    def api(self, path, method="GET", payload=None):
        self.requests.append((path, method))
        if method == "DELETE":
            if self.delete_failure:
                failure = self.delete_failure
                self.delete_failure = None
                raise failure
            asset_id = int(path.rsplit("/", 1)[1])
            self.remote["assets"] = [
                a for a in self.remote["assets"] if a["id"] != asset_id
            ]
            return None
        if method == "GET":
            return copy.deepcopy(self.remote)
        raise AssertionError((path, method))

    def command(self, *args, **kwargs):
        if args[:3] == ("gh", "release", "upload"):
            names = [
                Path(arg).name for arg in args if arg.startswith(str(self.directory))
            ]
            self.uploads.append(names)
            if self.behaviors:
                return self.behaviors.pop(0)(names)
            for name in names:
                self.asset(name)
        elif args[:3] == ("gh", "release", "edit"):
            self.remote["draft"] = False
        else:
            raise AssertionError("Unexpected CLI operation: " + repr(args))
        return ""

    def invoke(self):
        with (
            patch.object(r, "gh_api", side_effect=self.api),
            patch.object(r, "run", side_effect=self.command),
            patch.object(r.time, "sleep", side_effect=self.sleeps.append),
        ):
            return r.upload_release_assets(
                self.identity, self.directory, copy.deepcopy(self.remote), self.expected
            )

    def failure(self, status):
        return subprocess.CalledProcessError(
            1, ["gh", "release", "upload"], stderr=f"HTTP {status}"
        )

    def test_missing_files_upload_once_and_are_verified_by_digest(self):
        result = self.invoke()
        self.assertEqual(len(self.uploads), 1)
        self.assertEqual(set(self.uploads[0]), set(self.expected))
        self.assertEqual(len(result["assets"]), len(self.expected))
        self.assertEqual(self.sleeps, [])

    def test_complete_published_release_needs_no_download_or_mutation(self):
        for name in self.expected:
            self.asset(name)
        self.remote["draft"] = False
        self.invoke()
        self.assertEqual(self.requests, [])
        self.assertEqual(self.uploads, [])

    def test_empty_draft_placeholder_is_removed_but_completed_asset_is_preserved(self):
        complete = self.asset("app.zip")
        placeholder = self.asset("appcast.xml", state="starter")
        self.invoke()
        self.assertEqual(
            [p for p, m in self.requests if m == "DELETE"],
            [f"repos/{r.POLICY['repository']}/releases/assets/{placeholder['id']}"],
        )
        self.assertNotIn("app.zip", self.uploads[0])
        self.assertIn(complete, self.remote["assets"])

    def test_conflict_anywhere_blocks_all_deletion_and_upload(self):
        self.asset("appcast.xml", state="starter")
        self.asset("app.zip", digest="sha256:" + "0" * 64)
        with self.assertRaisesRegex(ValueError, "never overwrite"):
            self.invoke()
        self.assertEqual(self.requests, [])
        self.assertEqual(self.uploads, [])

    def test_invalid_placeholders_and_missing_uploaded_digest_are_never_deleted(self):
        for change in (
            {"size": 1},
            {"size": False},
            {"digest": "sha256:" + "f" * 64},
            {"id": 0},
            {"state": "uploaded", "digest": None},
        ):
            with self.subTest(change=change):
                self.remote["assets"] = []
                asset = self.asset("app.zip", state="starter")
                asset.update(change)
                with self.assertRaises(ValueError):
                    self.invoke()
                self.assertEqual(self.requests, [])
                self.assertEqual(self.uploads, [])

    def test_duplicate_or_unexpected_assets_block_all_mutation(self):
        for duplicate in (True, False):
            with self.subTest(duplicate=duplicate):
                self.remote["assets"] = []
                self.asset("app.zip", state="starter")
                asset = self.asset("app.zip")
                if not duplicate:
                    asset["name"] = "unexpected.zip"
                with self.assertRaisesRegex(ValueError, "Duplicate or unexpected"):
                    self.invoke()
                self.assertEqual(self.requests, [])
                self.assertEqual(self.uploads, [])

    def test_zero_byte_starter_digest_is_a_safe_placeholder(self):
        self.asset(
            "app.zip",
            state="starter",
            digest="sha256:" + hashlib.sha256(b"").hexdigest(),
        )
        self.invoke()
        self.assertEqual(sum(method == "DELETE" for _, method in self.requests), 1)

    def test_published_incomplete_release_is_not_repaired_by_mutation(self):
        self.remote["draft"] = False
        with self.assertRaisesRegex(ValueError, "Published release is incomplete"):
            self.invoke()
        self.assertEqual(self.requests, [])
        self.assertEqual(self.uploads, [])

    def test_accepted_upload_with_lost_response_is_not_repeated(self):
        def accepted(names):
            for name in names:
                self.asset(name)
            raise subprocess.CalledProcessError(
                1, ["gh"], stderr="connection reset; EOF"
            )

        self.behaviors = [accepted]
        self.invoke()
        self.assertEqual(len(self.uploads), 1)
        self.assertEqual(self.sleeps, [])

    def test_partial_upload_then_502_recovers_without_reuploading_completed_bytes(self):
        completed = []

        def partial(names):
            completed.append(self.asset(names[0]))
            self.asset(names[1], state="starter")
            raise self.failure(502)

        self.behaviors = [partial]
        self.invoke()
        self.assertEqual(len(self.uploads), 2)
        self.assertNotIn(completed[0]["name"], self.uploads[1])
        self.assertIn(completed[0], self.remote["assets"])
        self.assertEqual(self.sleeps, [1])

    def test_transient_failures_are_bounded_and_a_later_job_retry_recovers(self):
        def unavailable(names):
            self.asset(names[0], state="starter")
            raise self.failure(503)

        self.behaviors = [unavailable] * 3
        with self.assertRaises(subprocess.CalledProcessError):
            self.invoke()
        self.assertEqual(len(self.uploads), 3)
        self.assertEqual(self.sleeps, [1, 2])
        self.invoke()
        self.assertEqual(len(self.uploads), 4)
        self.assertEqual(len(self.remote["assets"]), len(self.expected))

    def test_permanent_permission_failure_is_not_retried(self):
        def forbidden(names):
            raise self.failure(403)

        self.behaviors = [forbidden]
        with self.assertRaises(subprocess.CalledProcessError):
            self.invoke()
        self.assertEqual(len(self.uploads), 1)
        self.assertEqual(len(self.requests), 1)  # Reconcile even a failed response.
        self.assertEqual(self.sleeps, [])

    def test_new_conflicting_bytes_after_upload_failure_stop_recovery(self):
        def corrupt(names):
            self.asset(names[0], digest="sha256:" + "f" * 64)
            self.asset(names[1], state="starter")
            raise self.failure(502)

        self.behaviors = [corrupt]
        with self.assertRaisesRegex(ValueError, "never overwrite"):
            self.invoke()
        self.assertFalse(any(method == "DELETE" for _, method in self.requests))
        self.assertEqual(len(self.uploads), 1)

    def test_identity_mismatch_fails_before_any_mutation(self):
        for key, value in (
            ("target_commitish", "main"),
            ("tag_name", "dictation/v9.0.0"),
            ("prerelease", True),
        ):
            original = self.remote[key]
            self.remote[key] = value
            with (
                self.subTest(key=key),
                self.assertRaisesRegex(ValueError, "identity mismatch"),
            ):
                self.invoke()
            self.remote[key] = original
        self.assertEqual(self.requests, [])
        self.assertEqual(self.uploads, [])

    def test_placeholder_deletion_failure_is_reconciled_and_retried(self):
        self.asset("app.zip", state="starter")
        self.delete_failure = r.GitHubError("fixture", "HTTP 503")
        self.invoke()
        self.assertEqual(sum(method == "DELETE" for _, method in self.requests), 2)
        self.assertEqual(len(self.uploads), 1)
        self.assertEqual(self.sleeps, [1])

    def test_deleted_placeholder_404_is_idempotent(self):
        placeholder = self.asset("app.zip", state="starter")
        original = self.api

        def api(path, method="GET", payload=None):
            if method == "DELETE":
                self.remote["assets"].remove(placeholder)
                raise r.GitHubError(path, "HTTP 404")
            return original(path, method, payload)

        with patch.object(self, "api", side_effect=api):
            self.invoke()
        self.assertEqual(len(self.uploads), 1)

    def test_github_delete_204_needs_no_json_body(self):
        result = subprocess.CompletedProcess(["gh"], 0, stdout="", stderr="")
        with patch.object(r.subprocess, "run", return_value=result):
            self.assertIsNone(r.gh_api("fixture", "DELETE"))

    def test_publication_repairs_feed_without_mutating_complete_downloads(self):
        archive = self.directory / "Antfly-Dictation-1.0.0-universal.zip"
        archive.write_bytes(b"fixture")
        import base64

        (self.directory / "appcast.xml").write_bytes(
            r.appcast(self.identity, archive, base64.b64encode(bytes(64)).decode())
        )
        (self.directory / "app.zip").unlink()
        (self.directory / "artifacts.json").unlink()
        r.write_manifest(
            self.identity, self.directory, base64.b64encode(bytes(range(32))).decode()
        )
        self.expected = {p.name: r.digest(p) for p in self.directory.iterdir()}
        for name in self.expected:
            self.asset(name)
        self.remote["draft"] = False
        with (
            patch.object(r, "verify_source"),
            patch.object(r, "gh_api", side_effect=self.api),
            patch.object(r, "run", side_effect=self.command),
            patch.object(r, "get_release_by_tag", return_value=self.remote),
            patch.dict(r.os.environ, {"GH_TOKEN": "fixture"}),
        ):
            r.publish_github_mirror(self.identity, self.directory)
        self.assertEqual(self.uploads, [])
        self.assertFalse(any(method != "GET" for _, method in self.requests))


class DraftLookupContractTests(unittest.TestCase):
    def test_draft_tag_404_falls_back_to_listing_and_numeric_paginated_assets(self):
        import create_github_release as github

        calls = []
        draft = {"id": 42, "tag_name": "dictation/v1.0.0", "draft": True}

        def api(method, repo, path, token, body=None):
            calls.append(path)
            if path.startswith("/releases/tags/"):
                raise github.GitHubError("GET fixture failed with 404")
            if path == "/releases?per_page=100&page=1":
                return [{"tag_name": "other/v1"}] * 100
            if path == "/releases?per_page=100&page=2":
                return [draft]
            if path == "/releases/42/assets?per_page=100&page=1":
                return [{"id": n} for n in range(100)]
            if path == "/releases/42/assets?per_page=100&page=2":
                return [{"id": 100}]
            raise AssertionError(path)

        with patch.object(github, "github_api", side_effect=api):
            found = r.get_release_by_tag(
                "antflydb/antfly", draft["tag_name"], "fixture"
            )
            complete = github.with_complete_assets("antflydb/antfly", found, "fixture")
        self.assertTrue(complete["draft"])
        self.assertEqual(len(complete["assets"]), 101)
        self.assertEqual(calls[-1], "/releases/42/assets?per_page=100&page=2")


if __name__ == "__main__":
    unittest.main()
