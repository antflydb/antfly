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

"""Exercise promotion's real scope step against current and legacy snapshots."""

import hashlib
import json
import os
import subprocess
import sys
import tempfile
import textwrap
import unittest
from pathlib import Path

from verify_release_ledger import materialize_scope

ROOT = Path(__file__).resolve().parents[2]
TAG = "v0.3.0-rc.2"
VERSION = "0.3.0-rc.2"
COMMIT = "a" * 40


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def fixture(root, legacy=False):
    source = root / "dist/release-payload"
    source.mkdir(parents=True)
    artifacts = []
    cli = []

    def write(name, data, kind, scope):
        path = source / name
        path.write_bytes(data)
        entry = {
            "name": name,
            "size": len(data),
            "sha256": digest(path),
            "kind": kind,
            "scope": scope,
        }
        artifacts.append(entry)
        return entry

    for package in ("cli", "cli-darwin-arm64", "cli-linux-arm64", "cli-linux-x64"):
        entry = write(
            f"antfly-{package}-{VERSION}.tgz", package.encode(), "npm-package", "cli"
        )
        cli.append({**entry, "kind": "npm", "package": "@antfly/" + package})
    if legacy:
        for platform in (
            "manylinux_2_28_x86_64",
            "manylinux_2_28_aarch64",
            "macosx_15_0_arm64",
        ):
            entry = write(
                f"antfly_cli-0.3.0rc2-py3-none-{platform}.whl",
                platform.encode(),
                "python-wheel",
                "cli",
            )
            cli.append(
                {
                    **entry,
                    "kind": "python",
                    "package": "antfly-cli",
                    "package_version": "0.3.0rc2",
                }
            )
    manifest = {
        "schema_version": 1 if legacy else 2,
        "version": VERSION,
        "commit": COMMIT,
        "registry_versions": {
            "npm": VERSION,
            **({"python": "0.3.0rc2"} if legacy else {}),
        },
        "artifacts": cli,
    }
    write("cli-snapshot.json", json.dumps(manifest).encode(), "cli-manifest", "cli")
    write("antfly_test.tar.gz", b"server runtime", "runtime-archive", "runtime")
    # The ledger, rather than an extension glob, defines the runtime scope.
    write("runtime-metadata.json", b"runtime metadata", "runtime-metadata", "runtime")
    write("release-spec.json", b"support metadata", "release-spec", "support")
    ledger = source / "artifacts.json"
    ledger.write_text(
        json.dumps(
            {"schema_version": 2, "tag": TAG, "commit": COMMIT, "artifacts": artifacts}
        )
    )
    return source, ledger


def workflow_step():
    workflow = (ROOT / ".github/workflows/antfly-release.yml").read_text()
    step = workflow.split(
        "      - name: Materialize and verify typed bundle scopes\n", 1
    )[1].split("      - uses:", 1)[0]
    return textwrap.dedent(step.split("        run: |\n", 1)[1])


class ReleaseScopeTests(unittest.TestCase):
    def test_real_workflow_step_supports_npm_only_and_legacy_wheel_snapshots(self):
        for legacy in (False, True):
            with (
                self.subTest(legacy=legacy),
                tempfile.TemporaryDirectory() as directory,
            ):
                root = Path(directory)
                source, ledger = fixture(root, legacy)
                (root / "scripts").symlink_to(
                    ROOT / "scripts", target_is_directory=True
                )
                (root / "bin").mkdir()
                (root / "bin/python").symlink_to(sys.executable)
                result = subprocess.run(
                    ["bash", "-c", workflow_step()],
                    cwd=root,
                    env={
                        **os.environ,
                        "PATH": str(root / "bin") + os.pathsep + os.environ["PATH"],
                        "RELEASE_TAG": TAG,
                        "RELEASE_VERSION": VERSION,
                        "RELEASE_COMMIT": COMMIT,
                        "LEDGER_SHA256": digest(ledger),
                    },
                    text=True,
                    capture_output=True,
                    check=False,
                )
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                snapshot = root / "dist/verified-cli-snapshot"
                self.assertEqual(len(list(snapshot.glob("*.whl"))), 3 if legacy else 0)
                for scope, destination in (
                    ("cli", snapshot),
                    ("runtime", root / "dist/release-runtime"),
                ):
                    expected = {
                        entry["name"]
                        for entry in json.loads(ledger.read_text())["artifacts"]
                        if entry["scope"] == scope
                    }
                    self.assertEqual(
                        {path.name for path in destination.iterdir()}, expected
                    )
                    for name in expected:
                        self.assertEqual(
                            (destination / name).read_bytes(),
                            (source / name).read_bytes(),
                        )

    def test_missing_declared_wheel_fails_before_materializing(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source, ledger = fixture(root, True)
            next(source.glob("*.whl")).unlink()
            destination = root / "output"
            with self.assertRaisesRegex(SystemExit, "scope mismatch"):
                materialize_scope(
                    ledger, source, destination, TAG, COMMIT, digest(ledger), "cli"
                )
            self.assertFalse(destination.exists())

    def test_unledgered_files_and_corrupt_other_scopes_fail_closed(self):
        for corruption in ("unledgered", "runtime"):
            with (
                self.subTest(corruption=corruption),
                tempfile.TemporaryDirectory() as directory,
            ):
                root = Path(directory)
                source, ledger = fixture(root)
                if corruption == "unledgered":
                    (source / "unexpected.whl").write_bytes(b"untracked")
                else:
                    (source / "antfly_test.tar.gz").write_bytes(b"corrupt")
                destination = root / "output"
                with self.assertRaises(SystemExit):
                    materialize_scope(
                        ledger, source, destination, TAG, COMMIT, digest(ledger), "cli"
                    )
                self.assertFalse(destination.exists())

    def test_ledger_hash_and_identity_are_required_before_copying(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source, ledger = fixture(root)
            for tag, commit, checksum in (
                (TAG, COMMIT, "0" * 64),
                ("v0.3.0-rc.3", COMMIT, digest(ledger)),
                (TAG, "b" * 40, digest(ledger)),
            ):
                with (
                    self.subTest(tag=tag, commit=commit, checksum=checksum),
                    self.assertRaises(SystemExit),
                ):
                    materialize_scope(
                        ledger, source, root / "output", tag, commit, checksum, "cli"
                    )
            self.assertFalse((root / "output").exists())

    def test_nonempty_destination_is_preserved(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source, ledger = fixture(root)
            destination = root / "output"
            destination.mkdir()
            (destination / "existing").write_bytes(b"preserve")
            with self.assertRaisesRegex(SystemExit, "destination must be empty"):
                materialize_scope(
                    ledger, source, destination, TAG, COMMIT, digest(ledger), "cli"
                )
            self.assertEqual(
                {path.name for path in destination.iterdir()}, {"existing"}
            )
            self.assertEqual((destination / "existing").read_bytes(), b"preserve")
