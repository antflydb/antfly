#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
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

"""Exercise conformance setup using real Zig runners and a local git stand-in."""

import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

ZIG_ROOT = Path(__file__).resolve().parents[1] / "zig"


class ConformanceFixturesTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory()
        cls.addClassCleanup(cls.temp.cleanup)
        root = Path(cls.temp.name)
        cls.openjpeg = root / "openjpeg-fixtures"
        cls.cache_args = [
            "--cache-dir",
            os.environ.get("ZIG_LOCAL_CACHE_DIR", "/tmp/zig-local-cache"),
            "--global-cache-dir",
            os.environ.get("ZIG_GLOBAL_CACHE_DIR", "/tmp/zig-global-cache"),
        ]
        subprocess.run(
            [
                "zig",
                "build-exe",
                "lib/image/src/jpeg2000_conformance_fixtures.zig",
                f"-femit-bin={cls.openjpeg}",
                *cls.cache_args,
            ],
            cwd=ZIG_ROOT,
            check=True,
        )

    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        self.fixtures = self.root / "fixture cache"
        self.log = self.root / "git.log"
        self.seed = self.root / "seed"
        fixture = self.seed / "tests/fixtures/basic.json"
        fixture.parent.mkdir(parents=True)
        fixture.write_text(
            json.dumps(
                {
                    "category": "decode",
                    "tests": [{"name": "null", "input": "null", "expected": None}],
                }
            )
        )
        (self.seed / "input/conformance").mkdir(parents=True)
        stub = self.root / "git"
        stub.write_text(
            "#!/bin/sh\n"
            'printf "%s\\n" "$*" >> "$GIT_LOG"\n'
            '[ "$1" = clone ] || exit 99\n'
            '[ "$FETCH_FAIL" != 1 ] || exit 42\n'
            'mkdir -p "$4"\n'
            'cp -R "$FIXTURE_SEED/." "$4"\n'
        )
        stub.chmod(0o755)
        self.env = {
            **os.environ,
            "PATH": str(self.root) + os.pathsep + os.environ["PATH"],
            "GIT_LOG": str(self.log),
            "FIXTURE_SEED": str(self.seed),
            "FETCH_FAIL": "0",
        }

    def run_suite(self, suite, offline=False):
        if suite == "toon":
            command = [
                "zig",
                "build",
                "lib-toon-conformance",
                "-j2",
                f"-Dconformance-fixtures={self.fixtures}",
                *self.cache_args,
            ]
            if offline:
                command.append("-Dconformance-fetch=false")
        else:
            command = [
                str(self.openjpeg),
                "fetch",
                str(self.fixtures / "openjpeg-data"),
            ]
            if offline:
                command.append("--no-fetch")
        return subprocess.run(
            command, cwd=ZIG_ROOT, env=self.env, text=True, capture_output=True
        )

    def test_fetch_missing_then_reuse_cache_online_and_offline(self):
        for suite in ("toon", "openjpeg"):
            with self.subTest(suite=suite):
                result = self.run_suite(suite)
                self.assertEqual(result.returncode, 0, result.stderr)
                if suite == "toon":
                    self.assertIn("tests=1 passed=1", result.stderr)
                calls = self.log.read_text()
                self.env["FETCH_FAIL"] = "1"
                for offline in (False, True):
                    result = self.run_suite(suite, offline=offline)
                    self.assertEqual(result.returncode, 0, result.stderr)
                    self.assertEqual(self.log.read_text(), calls)
                self.env["FETCH_FAIL"] = "0"

    def test_offline_missing_fails_without_fetching(self):
        for suite, error in (
            ("toon", "ToonFixturesUnavailable"),
            ("openjpeg", "FixtureDirMissing"),
        ):
            with self.subTest(suite=suite):
                result = self.run_suite(suite, offline=True)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn(error, result.stderr)
                self.assertFalse(self.log.exists())

    def test_fetch_failure_propagates_and_can_be_retried(self):
        for suite in ("toon", "openjpeg"):
            with self.subTest(suite=suite):
                self.env["FETCH_FAIL"] = "1"
                result = self.run_suite(suite)
                self.assertNotEqual(result.returncode, 0)
                self.assertTrue(self.log.exists())
                self.env["FETCH_FAIL"] = "0"
                result = self.run_suite(suite)
                self.assertEqual(result.returncode, 0, result.stderr)

    def test_openjpeg_suite_honors_fixture_directory(self):
        result = subprocess.run(
            [
                "zig",
                "test",
                "lib/image/src/mod.zig",
                "--test-filter",
                "external jpeg2000 iso conformance corpus",
                *self.cache_args,
            ],
            cwd=ZIG_ROOT,
            env={**self.env, "OPENJPEG_DATA_DIR": str(self.fixtures)},
            text=True,
            capture_output=True,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn(
            f"fixtures not present at {self.fixtures}/input/conformance", result.stderr
        )
        self.assertIn("1 skipped", result.stderr)
        self.assertFalse(self.log.exists())

    def test_cached_fixtures_do_not_hide_test_failure(self):
        result = self.run_suite("toon")
        self.assertEqual(result.returncode, 0, result.stderr)
        fixture = self.fixtures / "toon-format-spec/tests/fixtures/basic.json"
        case = json.loads(fixture.read_text())
        case["tests"][0]["expected"] = "wrong"
        fixture.write_text(json.dumps(case))
        result = self.run_suite("toon")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("ToonConformanceFailed", result.stderr)


if __name__ == "__main__":
    unittest.main()
