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

import unittest

from classify_validation import classify


class ClassifyValidationTests(unittest.TestCase):
    def test_python_script_only_formats_python(self) -> None:
        scopes = classify(["scripts/example.py"])
        self.assertTrue(scopes["format_python"])
        self.assertFalse(scopes["sdk"])
        self.assertFalse(scopes["release"])

    def test_any_sdk_change_runs_the_complete_sdk_suite(self) -> None:
        for path in (
            "py/packages/sdk/src/antfly/client.py",
            "ts/packages/sdk/src/client.ts",
            "go/pkg/sdk/client.go",
            "rs/crates/sdk/src/lib.rs",
            "openapi.yaml",
        ):
            with self.subTest(path=path):
                self.assertTrue(classify([path])["sdk"])

    def test_openapi_tool_environment_runs_the_sdk_suite(self) -> None:
        for path in ("scripts/pyproject.toml", "scripts/uv.lock"):
            with self.subTest(path=path):
                scopes = classify([path])
                self.assertTrue(scopes["sdk"])

    def test_typescript_changes_check_consumers_and_antfarm(self) -> None:
        for path in (
            "ts/packages/sdk/src/client.ts",
            "ts/packages/components/src/QueryBox.tsx",
            "ts/apps/antfarm/src/api.ts",
            "openapi.yaml",
            "zig/pkg/antfly/antfarm/index.html",
        ):
            with self.subTest(path=path):
                scopes = classify([path])
                self.assertTrue(scopes["typescript"])
                self.assertTrue(scopes["antfarm_e2e"])

    def test_non_typescript_sdk_change_does_not_require_browser_e2e(self) -> None:
        scopes = classify(["go/pkg/sdk/client.go"])
        self.assertTrue(scopes["sdk"])
        self.assertFalse(scopes["antfarm_e2e"])

    def test_formatter_infrastructure_checks_every_language(self) -> None:
        scopes = classify(["scripts/format.sh"])
        for language in ("zig", "go", "python", "typescript", "rust"):
            self.assertTrue(scopes[f"format_{language}"])

    def test_shared_validation_infrastructure_runs_every_contract(self) -> None:
        for path in (
            ".github/workflows/sdks-ci.yml",
            ".github/actions/load-toolchain-policy/action.yml",
            "scripts/ci/check.sh",
            "scripts/ci/check_toolchain_policy.py",
            "scripts/ci/classify_validation.py",
            "scripts/ci/toolchain-policy.json",
            "Makefile",
        ):
            with self.subTest(path=path):
                scopes = classify([path])
                for scope in (
                    "sdk",
                    "typescript",
                    "memoryaf",
                    "release",
                    "antfarm_e2e",
                ):
                    self.assertTrue(scopes[scope])
                for language in ("zig", "go", "python", "typescript", "rust"):
                    self.assertTrue(scopes[f"format_{language}"])

    def test_release_change_runs_release_and_its_language_formatter(self) -> None:
        scopes = classify(["scripts/packaging/package_cli_release.py"])
        self.assertTrue(scopes["release"])
        self.assertTrue(scopes["format_python"])

    def test_cli_change_runs_sdk_and_release_contracts(self) -> None:
        scopes = classify(["py/packages/cli/src/antfly_cli/__main__.py"])
        self.assertTrue(scopes["sdk"])
        self.assertTrue(scopes["release"])


if __name__ == "__main__":
    unittest.main()
