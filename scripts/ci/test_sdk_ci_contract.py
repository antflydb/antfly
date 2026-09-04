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

import json
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]


class SdkCiContractTests(unittest.TestCase):
    def test_typescript_phases_have_bounded_concurrency(self) -> None:
        check_script = (REPO_ROOT / "scripts/ci/check.sh").read_text()

        self.assertIn("turbo run lint typecheck build --concurrency=2", check_script)
        self.assertIn("turbo run test --concurrency=1", check_script)
        self.assertNotIn("turbo run lint typecheck build test", check_script)

    def test_vitest_worker_counts_are_bounded(self) -> None:
        packages = (
            "ts/packages/sdk/package.json",
            "ts/packages/components/package.json",
            "ts/apps/antfarm/package.json",
        )

        for package in packages:
            with self.subTest(package=package):
                package_json = json.loads((REPO_ROOT / package).read_text())
                self.assertIn("--maxWorkers=2", package_json["scripts"]["test"])


if __name__ == "__main__":
    unittest.main()
