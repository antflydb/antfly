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

import itertools
import json
import re
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]


class SdkCiContractTests(unittest.TestCase):
    def test_zig_consumers_have_one_matching_compiler_for_every_scope(self) -> None:
        workflow = (REPO_ROOT / ".github/workflows/sdks-ci.yml").read_text()
        steps = dict(
            re.findall(
                r"^      - name: ([^\n]+)\n(.*?)(?=^      - name: |\Z)",
                workflow,
                re.MULTILINE | re.DOTALL,
            )
        )

        def enabled(name, active):
            condition = re.search(r"^        if: (.+)$", steps[name], re.MULTILINE)
            self.assertIsNotNone(condition, name)

            def compare(term):
                match = re.fullmatch(
                    r"steps\.scopes\.outputs\.(\w+) (==|!=) 'true'", term.strip()
                )
                self.assertIsNotNone(match, term)
                present = match[1] in active
                return present if match[2] == "==" else not present

            return any(
                all(compare(term) for term in alternative.split("&&"))
                for alternative in condition[1].split("||")
            )

        scopes = ("sdk", "memory", "release", "format_zig")
        for flags in itertools.product((False, True), repeat=len(scopes)):
            active = {scope for scope, flag in zip(scopes, flags) if flag}
            with self.subTest(scopes=sorted(active)):
                compiling = bool(active & {"sdk", "memory", "release"})
                nix = enabled("Install Nix for native checks", active)
                native = enabled("Install glibc-linked Zig for native checks", active)
                formatting = enabled("Set up Zig for formatting", active)
                self.assertEqual(nix, compiling)
                self.assertEqual(native, compiling)
                self.assertEqual(formatting, "format_zig" in active and not compiling)
                for consumer in (
                    "Check SDKs",
                    "Check memory",
                    "Check release packaging and scripts",
                    "Check Zig formatting",
                ):
                    if enabled(consumer, active):
                        self.assertEqual(int(native) + int(formatting), 1, consumer)

        order = list(steps)
        self.assertLess(
            order.index("Install Nix for native checks"),
            order.index("Install glibc-linked Zig for native checks"),
        )
        for consumer in (
            "Check SDKs",
            "Check memory",
            "Check release packaging and scripts",
        ):
            self.assertLess(
                order.index("Install glibc-linked Zig for native checks"),
                order.index(consumer),
            )

    def test_typescript_phases_have_bounded_concurrency(self) -> None:
        check_script = (REPO_ROOT / "scripts/ci/check.sh").read_text()

        self.assertIn("turbo run lint typecheck build --concurrency=2", check_script)
        self.assertIn("turbo run test --concurrency=1", check_script)
        self.assertNotIn("turbo run lint typecheck build test", check_script)

    def test_vitest_worker_counts_are_bounded(self) -> None:
        packages = (
            "ts/packages/sdk/package.json",
            "ts/packages/embedded/package.json",
            "ts/packages/components/package.json",
            "ts/apps/antfarm/package.json",
        )

        for package in packages:
            with self.subTest(package=package):
                package_json = json.loads((REPO_ROOT / package).read_text())
                self.assertIn("--maxWorkers=2", package_json["scripts"]["test"])


if __name__ == "__main__":
    unittest.main()
