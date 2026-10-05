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

"""Require author sign-offs once the contribution policy is on the base branch."""

from __future__ import annotations

import argparse
import subprocess
import sys

POLICY_MARKER = "## Certificate of Origin and inbound licenses"


def git(*args: str) -> str:
    return subprocess.check_output(["git", *args], text=True)


def check(base: str, head: str) -> list[str]:
    # This PR introduces the policy. Historical commits already on a PR when
    # it is introduced cannot retroactively carry an author sign-off.
    base_policy = git("show", f"{base}:CONTRIBUTING.md")
    if POLICY_MARKER not in base_policy:
        print("Contributor sign-offs begin with PRs based on this policy")
        return []
    errors = []
    for commit in git("rev-list", "--no-merges", f"{base}..{head}").splitlines():
        author, email, message = git(
            "show", "-s", "--format=%an%n%ae%n%B", commit
        ).split("\n", 2)
        expected = f"Signed-off-by: {author} <{email}>"
        if expected not in message.splitlines():
            errors.append(f"{commit[:12]}: missing author sign-off ({expected})")
    return errors


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", required=True)
    parser.add_argument("--head", default="HEAD")
    args = parser.parse_args()
    errors = check(args.base, args.head)
    for error in errors:
        print(error, file=sys.stderr)
    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
