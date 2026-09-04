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

    def test_formatter_infrastructure_checks_every_language(self) -> None:
        scopes = classify(["scripts/format.sh"])
        for language in ("zig", "go", "python", "typescript", "rust"):
            self.assertTrue(scopes[f"format_{language}"])

    def test_workflow_changes_run_all_contracts(self) -> None:
        scopes = classify([".github/workflows/repository-validation.yml"])
        self.assertTrue(scopes["sdk"])
        self.assertTrue(scopes["release"])
        for language in ("zig", "go", "python", "typescript", "rust"):
            self.assertTrue(scopes[f"format_{language}"])

    def test_release_change_runs_release_and_its_language_formatter(self) -> None:
        scopes = classify(["scripts/packaging/package_cli_release.py"])
        self.assertTrue(scopes["release"])
        self.assertTrue(scopes["format_python"])


if __name__ == "__main__":
    unittest.main()
