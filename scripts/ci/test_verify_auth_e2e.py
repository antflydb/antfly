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

import copy
import unittest
from unittest import mock

from scripts.ci import verify_auth_e2e as gate


class AuthE2eGateTests(unittest.TestCase):
    commit = "a" * 40
    repository = "antflydb/antfly"

    def run_record(self):
        return {
            "id": 42,
            "run_attempt": 3,
            "html_url": "https://example.test/run/42",
            "head_sha": self.commit,
            "head_branch": "main",
            "event": "push",
            "path": ".github/workflows/zig-tests.yml",
            "status": "completed",
            "conclusion": "success",
            "repository": {"full_name": self.repository},
            "head_repository": {"full_name": self.repository},
        }

    def jobs(self):
        return [
            {"name": name, "status": "completed", "conclusion": "success"}
            for name in ("e2e-full", "better-auth / compatibility (supported)")
        ]

    def test_rejects_untrusted_incomplete_and_foreign_commit_runs(self):
        for key, value in (
            ("head_sha", "b" * 40),
            ("head_branch", "feature"),
            ("event", "pull_request"),
            ("path", ".github/workflows/other.yml"),
            ("status", "in_progress"),
            ("conclusion", "failure"),
            ("head_repository", {"full_name": "someone/fork"}),
            ("repository", {"full_name": "someone/fork"}),
        ):
            run = {**self.run_record(), key: value}
            with self.subTest(key=key):
                self.assertEqual(
                    gate.qualified_runs([run], self.commit, self.repository), []
                )
        self.assertEqual(
            len(gate.qualified_runs([self.run_record()], self.commit, self.repository)),
            1,
        )

    def test_skipped_or_missing_supported_auth_cannot_qualify(self):
        self.assertTrue(gate.has_auth_e2e(self.jobs()))
        for name in ("e2e-full", "better-auth / compatibility (supported)"):
            for conclusion in ("skipped", "failure", "cancelled"):
                jobs = copy.deepcopy(self.jobs())
                next(job for job in jobs if job["name"] == name)["conclusion"] = (
                    conclusion
                )
                self.assertFalse(gate.has_auth_e2e(jobs))
        self.assertFalse(gate.has_auth_e2e(self.jobs()[:1]))

    def test_main_uses_only_latest_attempt_at_exact_commit(self):
        with (
            mock.patch(
                "sys.argv",
                ["gate", "--repository", self.repository, "--commit", self.commit],
            ),
            mock.patch.object(
                gate, "api_pages", side_effect=[[self.run_record()], self.jobs()]
            ) as api,
        ):
            gate.main()
        self.assertIn("head_sha=" + self.commit, api.call_args_list[0].args[0])
        self.assertIn("/runs/42/attempts/3/jobs?", api.call_args_list[1].args[0])

    def test_successful_old_attempt_does_not_mask_missing_new_coverage(self):
        with (
            mock.patch(
                "sys.argv",
                ["gate", "--repository", self.repository, "--commit", self.commit],
            ),
            mock.patch.object(gate, "api_pages", side_effect=[[self.run_record()], []]),
            self.assertRaisesRegex(SystemExit, "No successful main Zig E2E run"),
        ):
            gate.main()

    def test_all_api_pages_are_considered(self):
        import json

        with mock.patch.object(
            gate.subprocess,
            "check_output",
            return_value=json.dumps(
                [{"jobs": [{"name": "first"}]}, {"jobs": [{"name": "second"}]}]
            ),
        ) as gh:
            self.assertEqual(
                gate.api_pages("endpoint", "jobs"),
                [{"name": "first"}, {"name": "second"}],
            )
        self.assertEqual(
            gh.call_args.args[0], ["gh", "api", "--paginate", "--slurp", "endpoint"]
        )


class AuthArtifactInstallationTests(unittest.TestCase):
    def test_real_install_command_accepts_both_shared_archive_families(self):
        import io
        import os
        from pathlib import Path
        import subprocess
        import tarfile
        import tempfile
        import textwrap

        workflow = (
            Path(__file__).resolve().parents[2]
            / ".github/workflows/better-auth-compatibility.yml"
        ).read_text()
        step = workflow.split("      - name: Install shared E2E artifacts\n", 1)[1]
        command = textwrap.dedent(
            step.split("        run: |\n", 1)[1].split("      - uses:", 1)[0]
        )
        for family in ("base", "full"):
            with (
                self.subTest(family=family),
                tempfile.TemporaryDirectory() as directory,
            ):
                root = Path(directory)
                for folder, name, members in (
                    (
                        "auth-library",
                        "libantfly",
                        {"lib/libantfly.so": b"library", "include/antfly.h": b"header"},
                    ),
                    (
                        "auth-server",
                        "antfly",
                        {
                            "zig-out/bin/antfly": b"server",
                            "zig-out/bin/antfly-grep": b"grep",
                        },
                    ),
                ):
                    dest = root / folder
                    dest.mkdir()
                    with tarfile.open(
                        dest / f"{name}-e2e-{family}.tar.gz", "w:gz"
                    ) as archive:
                        for path, data in members.items():
                            info = tarfile.TarInfo(path)
                            info.size = len(data)
                            info.mode = 0o644
                            archive.addfile(info, io.BytesIO(data))
                subprocess.run(
                    ["bash", "-euo", "pipefail", "-c", command],
                    cwd=root,
                    env={**os.environ, "RUNNER_TEMP": str(root)},
                    check=True,
                )
                self.assertEqual(
                    (root / "zig/zig-out/lib/libantfly.so").read_bytes(), b"library"
                )
                self.assertEqual(
                    (root / "zig/zig-out/bin/antfly").read_bytes(), b"server"
                )
                self.assertTrue(os.access(root / "zig/zig-out/bin/antfly", os.X_OK))


if __name__ == "__main__":
    unittest.main()
