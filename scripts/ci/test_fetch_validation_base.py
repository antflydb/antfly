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

"""Exercise exact-base fetching against real shallow repositories."""

import base64
from contextlib import chdir
import importlib.util
import os
import re
from pathlib import Path
import subprocess
import tempfile
import textwrap
import unittest
from unittest.mock import patch


from fetch_validation_base import fetch_base, fetch_environment
from classify_validation import changed_paths, classify

ROOT = Path(__file__).resolve().parents[2]


def git(root, *args):
    return subprocess.check_output(
        ["git", "-C", str(root), *args], text=True, stderr=subprocess.DEVNULL
    ).strip()


class ValidationBaseTests(unittest.TestCase):
    def test_shallow_diff_preserves_multi_commit_and_diverged_changes(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            source, clone = root / "source", root / "clone"
            source.mkdir()
            git(source, "init", "-b", "main")
            git(source, "config", "user.name", "CI test")
            git(source, "config", "user.email", "ci@example.invalid")
            (source / "old.txt").write_text("old")
            git(source, "add", ".")
            git(source, "commit", "-m", "base")
            base = git(source, "rev-parse", "HEAD")
            for filename in ("graph.zig", "policy.py", "README.md"):
                (source / filename).write_text(filename)
                git(source, "add", ".")
                git(source, "commit", "-m", filename)
            main = git(source, "rev-parse", "HEAD")
            git(source, "checkout", "-b", "diverged", base)
            (source / "old.txt").unlink()
            (source / "branch.rs").write_text("branch")
            git(source, "add", "-A")
            git(source, "commit", "-m", "diverged")
            diverged = git(source, "rev-parse", "HEAD")
            git(source, "tag", "unneeded-tag")
            expected = {}
            for revision in (base, diverged):
                expected[revision] = set(
                    git(source, "diff", "--name-only", revision, main).splitlines()
                )
            subprocess.run(
                [
                    "git",
                    "clone",
                    "--no-tags",
                    "--depth=1",
                    "--single-branch",
                    "--branch",
                    "main",
                    source.as_uri(),
                    str(clone),
                ],
                check=True,
                capture_output=True,
            )
            refs_before = git(clone, "show-ref")
            head_before = git(clone, "rev-parse", "HEAD")
            for revision in (base, diverged):
                with self.subTest(revision=revision):
                    self.assertNotEqual(
                        subprocess.run(
                            ["git", "cat-file", "-e", revision],
                            cwd=clone,
                            stdout=subprocess.DEVNULL,
                            stderr=subprocess.DEVNULL,
                        ).returncode,
                        0,
                    )
                    fetch_base(revision, cwd=clone)
                    self.assertEqual(git(clone, "rev-list", "--count", revision), "1")
                    result = subprocess.check_output(
                        ["git", "diff", "--name-only", revision, "HEAD"],
                        cwd=clone,
                        text=True,
                    )
                    self.assertEqual(set(result.splitlines()), expected[revision])
                    # Exercise the SDK classifier's real two-endpoint diff too.
                    with chdir(clone):
                        paths = changed_paths(revision, "HEAD")
                    self.assertEqual(set(paths), expected[revision])
                    self.assertTrue(classify(paths)["format_zig"])
                    with patch("fetch_validation_base.subprocess.run") as run:
                        run.return_value.returncode = 0
                        fetch_base(revision, cwd=clone)
                        self.assertEqual(run.call_count, 1)
            self.assertEqual(git(clone, "show-ref"), refs_before)
            self.assertEqual(git(clone, "rev-parse", "HEAD"), head_before)
            self.assertEqual(git(clone, "rev-list", "--count", "HEAD"), "1")
            self.assertEqual(git(clone, "status", "--porcelain"), "")
            self.assertEqual(git(clone, "tag"), "")

    def test_empty_and_initial_push_bases_leave_full_selection_to_callers(self):
        with patch("fetch_validation_base.subprocess.run") as run:
            for base in ("", "0" * 40):
                fetch_base(base)
            run.assert_not_called()

    def test_non_commit_inputs_are_rejected(self):
        for base in ("main", "HEAD~1", "--upload-pack=evil", "a" * 39, "x" * 40):
            with self.subTest(base=base), self.assertRaises(ValueError):
                fetch_base(base)

    def test_fetch_failure_is_not_treated_as_a_clean_diff(self):
        with patch("fetch_validation_base.subprocess.run") as run:
            run.side_effect = [
                subprocess.CompletedProcess([], 1),
                subprocess.CalledProcessError(1, "git"),
            ]
            with self.assertRaises(subprocess.CalledProcessError):
                fetch_base("a" * 40)
            self.assertEqual(run.call_args.kwargs["timeout"], 120)

    def test_token_is_ephemeral_scoped_and_preserves_existing_git_config(self):
        before = {
            "GH_TOKEN": "test-token",
            "GITHUB_SERVER_URL": "https://github.example",
            "GIT_CONFIG_COUNT": "1",
            "GIT_CONFIG_KEY_0": "protocol.version",
            "GIT_CONFIG_VALUE_0": "2",
        }
        with patch.dict(os.environ, before, clear=True):
            env = fetch_environment()
            self.assertEqual(dict(os.environ), before)
        self.assertNotIn("GH_TOKEN", env)
        self.assertEqual(env["GIT_CONFIG_COUNT"], "2")
        self.assertEqual(env["GIT_CONFIG_KEY_0"], "protocol.version")
        self.assertEqual(
            env["GIT_CONFIG_KEY_1"], "http.https://github.example/.extraheader"
        )
        encoded = env["GIT_CONFIG_VALUE_1"].removeprefix("AUTHORIZATION: basic ")
        self.assertEqual(base64.b64decode(encoded), b"x-access-token:test-token")

    def test_workflows_use_shallow_head_and_explicit_base_for_diff_gates(self):
        for name, job in (("zig-tests.yml", "changes"), ("sdks-ci.yml", "sdks")):
            workflow = (ROOT / ".github/workflows" / name).read_text()
            self.assertNotIn("fetch-depth: 0", workflow)
            jobs = dict(
                re.findall(
                    r"\n  ([\w-]+):\n(.*?)(?=\n  [\w-]+:\n|\Z)", workflow, re.DOTALL
                )
            )
            fetch = re.search(
                r"      - name: Fetch validation diff base\n(.*?)(?=      - |\Z)",
                jobs[job],
                re.DOTALL,
            )[1]
            self.assertIn('fetch_validation_base.py "$BASE_SHA"', fetch)
            self.assertIn(
                "BASE_SHA: ${{ inputs.base_sha || github.event.merge_group.base_sha || github.event.before }}",
                fetch,
            )

    def test_older_approved_heads_select_all_scopes_without_missing_helper(self):
        for name, job in (("zig-tests.yml", "changes"), ("sdks-ci.yml", "sdks")):
            workflow = (ROOT / ".github/workflows" / name).read_text()
            jobs = dict(
                re.findall(
                    r"\n  ([\w-]+):\n(.*?)(?=\n  [\w-]+:\n|\Z)", workflow, re.DOTALL
                )
            )
            step = re.search(
                r"      - name: Fetch validation diff base\n(.*?)(?=      - |\Z)",
                jobs[job],
                re.DOTALL,
            )[1]
            script = textwrap.dedent(step.split("        run: |\n", 1)[1])
            with self.subTest(workflow=name), tempfile.TemporaryDirectory() as temp:
                env_file = Path(temp) / "env"
                subprocess.run(
                    ["bash", "-euo", "pipefail", "-c", script],
                    cwd=temp,
                    env={**os.environ, "GITHUB_ENV": str(env_file)},
                    check=True,
                )
                self.assertEqual(env_file.read_text(), "VALIDATION_FORCE_ALL=true\n")
                self.assertIn(
                    '"${VALIDATION_FORCE_ALL:-false}" == "true" ||', jobs[job]
                )

    def test_previous_reader_fetches_only_pinned_snapshot(self):
        spec = importlib.util.spec_from_file_location(
            "previous_reader", ROOT / "scripts/ci/prepare_lite_previous_reader.py"
        )
        previous = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(previous)
        with (
            patch.object(previous.subprocess, "run") as run,
            patch.object(previous, "prepare_previous_reader_build"),
            patch.object(previous.Path, "exists", return_value=False),
            patch.object(previous.Path, "is_file", return_value=True),
        ):
            run.return_value.returncode = 1
            previous.main()
            commands = [call.args[0] for call in run.call_args_list]
            self.assertIn(
                ["git", "fetch", "--no-tags", "--depth=1", "origin", previous.REVISION],
                commands,
            )


if __name__ == "__main__":
    unittest.main()
