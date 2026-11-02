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

"""Fetch only the exact diff base needed by a shallow CI checkout."""

from __future__ import annotations

import argparse
import base64
import os
from pathlib import Path
import re
import subprocess


def fetch_environment():
    env = dict(os.environ)
    token = env.pop("GH_TOKEN", "")
    if token:
        # Match checkout's HTTPS credential scope without writing credentials
        # into .git/config or putting the token on a command line.
        server = env.get("GITHUB_SERVER_URL", "https://github.com").rstrip("/")
        index = int(env.get("GIT_CONFIG_COUNT", "0"))
        env["GIT_CONFIG_COUNT"] = str(index + 1)
        env[f"GIT_CONFIG_KEY_{index}"] = f"http.{server}/.extraheader"
        credential = base64.b64encode(f"x-access-token:{token}".encode()).decode()
        env[f"GIT_CONFIG_VALUE_{index}"] = f"AUTHORIZATION: basic {credential}"
    return env


def fetch_base(base: str, *, cwd: Path | None = None):
    # Dispatches and newly created branches can legitimately have no diff base.
    # Their existing callers select all scopes rather than skipping validation.
    if not base or set(base) == {"0"}:
        return
    if not re.fullmatch(r"[0-9a-fA-F]{40}", base):
        raise ValueError("validation base must be a full commit SHA")
    probe = ["git", "cat-file", "-e", f"{base}^{{commit}}"]
    if (
        subprocess.run(
            probe, cwd=cwd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        ).returncode
        == 0
    ):
        return
    subprocess.run(
        [
            "git",
            "fetch",
            "--no-tags",
            "--no-recurse-submodules",
            "--depth=1",
            "--no-write-fetch-head",
            "origin",
            base,
        ],
        cwd=cwd,
        env=fetch_environment(),
        check=True,
        timeout=120,
    )
    subprocess.run(probe, cwd=cwd, check=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("base", nargs="?", default="")
    args = parser.parse_args()
    fetch_base(args.base)


if __name__ == "__main__":
    main()
