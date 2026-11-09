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

"""Restore only ledger-selected R2 bytes while preserving release identity."""

import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import textwrap
import unittest

from download_objectstorage import (
    LocalReader,
    restore_payload,
    restore_verified_payload,
)
from recover_release_payload import recover_payload
from test_release_scope_materialization import fixture, TAG, COMMIT, ROOT


class Reader:
    def __init__(self, root):
        self.root = root
        self.reads = []

    def read(self, name):
        self.reads.append(name)
        return (self.root / name).read_bytes()

    def list_names(self):
        return {p.name for p in self.root.iterdir()}


class R2ScopeTests(unittest.TestCase):
    def test_github_recovery_repairs_missing_r2_before_downstream_scope_restore(self):
        for conflict in (False, True):
            with (
                self.subTest(conflict=conflict),
                tempfile.TemporaryDirectory() as directory,
            ):
                root = Path(directory)
                source, ledger = fixture(root / "github")
                checksum = hashlib.sha256(ledger.read_bytes()).hexdigest()
                recovered = root / "dist/release-payload"
                self.assertEqual(
                    recover_payload(
                        [("github-release", lambda: Reader(source))],
                        recovered,
                        TAG,
                        COMMIT,
                        checksum,
                    ),
                    "github-release",
                )
                store = root / "r2"
                prefix = f"antfly/{TAG}"
                remote = store / "antfly-releases" / prefix
                if conflict:
                    remote.mkdir(parents=True)
                    (remote / "antfly_test.tar.gz").write_bytes(
                        b"conflicting immutable bytes"
                    )
                (root / "scripts").symlink_to(
                    ROOT / "scripts", target_is_directory=True
                )
                (root / "bin").mkdir()
                (root / "bin/python").symlink_to(sys.executable)
                workflow = (ROOT / ".github/workflows/antfly-release.yml").read_text()
                step = workflow.split(
                    "      - name: Repair the R2 mirror from verified GitHub recovery\n",
                    1,
                )[1].split("\n      - name:", 1)[0]
                self.assertIn("steps.recovery.outputs.source == 'github-release'", step)
                # Execute the production handoff, substituting only its storage adapter.
                shell = textwrap.dedent(step.split("        run: |\n", 1)[1]).replace(
                    "--provider s3", '--provider local --local-root "$LOCAL_ROOT"'
                )
                result = subprocess.run(
                    ["bash", "-c", shell],
                    cwd=root,
                    env={
                        **os.environ,
                        "PATH": str(root / "bin") + os.pathsep + os.environ["PATH"],
                        "RELEASE_TAG": TAG,
                        "LOCAL_ROOT": str(store),
                        "AWS_ENDPOINT_URL": "unused",
                    },
                    capture_output=True,
                    text=True,
                    check=False,
                )
                if conflict:
                    self.assertNotEqual(result.returncode, 0)
                    self.assertIn("immutable object differs", result.stderr)
                    self.assertEqual(
                        (remote / "antfly_test.tar.gz").read_bytes(),
                        b"conflicting immutable bytes",
                    )
                else:
                    self.assertEqual(
                        result.returncode, 0, result.stdout + result.stderr
                    )
                    restore_payload(
                        LocalReader(store, "antfly-releases"),
                        prefix,
                        root / "runtime",
                        TAG,
                        COMMIT,
                        checksum,
                        "runtime",
                    )
                    self.assertEqual(
                        (root / "runtime/antfly_test.tar.gz").read_bytes(),
                        b"server runtime",
                    )
                # Repair must never advance mutable channel pointers.
                self.assertFalse((store / "antfly-releases/antfly/latest").exists())
                self.assertFalse((store / "antfly-releases/antfly/next").exists())

    def test_only_requested_scope_is_downloaded_from_current_and_legacy_releases(self):
        for legacy in (False, True):
            for scope in ("runtime", "cli", None):
                with (
                    self.subTest(legacy=legacy, scope=scope),
                    tempfile.TemporaryDirectory() as directory,
                ):
                    root = Path(directory)
                    source, ledger = fixture(root, legacy)
                    checksum = hashlib.sha256(ledger.read_bytes()).hexdigest()
                    reader = Reader(source)
                    restore_verified_payload(
                        reader, root / "output", TAG, COMMIT, checksum, scope
                    )
                    expected = {
                        entry["name"]
                        for entry in json.loads(ledger.read_text())["artifacts"]
                        if scope is None or entry["scope"] == scope
                    }
                    self.assertEqual(set(reader.reads), expected | {"artifacts.json"})
                    self.assertEqual(
                        {p.name for p in (root / "output").iterdir()},
                        expected | {"artifacts.json"},
                    )

    def test_wrong_ledger_digest_or_release_identity_downloads_no_payload(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source, ledger = fixture(root)
            for tag, commit, checksum in [
                (TAG, COMMIT, "0" * 64),
                ("v9.9.9", COMMIT, hashlib.sha256(ledger.read_bytes()).hexdigest()),
            ]:
                reader = Reader(source)
                with self.assertRaises(SystemExit):
                    restore_verified_payload(
                        reader, root / "output", tag, commit, checksum, "runtime"
                    )
                self.assertEqual(reader.reads, ["artifacts.json"])

    def test_corrupt_selected_bytes_and_extra_mirror_members_are_rejected(self):
        for corruption in ("content", "extra"):
            with (
                self.subTest(corruption=corruption),
                tempfile.TemporaryDirectory() as directory,
            ):
                root = Path(directory)
                source, ledger = fixture(root)
                checksum = hashlib.sha256(ledger.read_bytes()).hexdigest()
                (
                    source
                    / ("antfly_test.tar.gz" if corruption == "content" else "extra")
                ).write_bytes(b"bad")
                with self.assertRaises(SystemExit):
                    restore_verified_payload(
                        Reader(source),
                        root / "output",
                        TAG,
                        COMMIT,
                        checksum,
                        "runtime",
                    )


if __name__ == "__main__":
    unittest.main()
