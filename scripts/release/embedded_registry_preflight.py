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

"""Check every embedded registry identity before publication can begin."""

from __future__ import annotations

import argparse
from pathlib import Path

from lite_snapshot import NPM_PACKAGES, verify
from prepare_npm_promotion import preflight_package
from prepare_pypi_promotion import (
    expected_release_files,
    missing_registry_files,
    pypi_release_files,
)
from registry.npm import dist_tag
from release_channels import (
    compare_channel_tags,
    load_policy,
    python_version_from_release,
)


def preflight(snapshot: Path, version: str, commit: str, npm_tag: str) -> None:
    verify(snapshot, version, commit)
    if npm_tag not in {"latest", "next"}:
        raise ValueError("embedded publication requires latest or next")
    channel = "stable" if npm_tag == "latest" else "next"
    for name, stem in NPM_PACKAGES.items():
        observed = dist_tag(name, npm_tag)
        # The initial name-claim version is not a functional release.
        if (
            observed
            and observed != "0.0.0"
            and compare_channel_tags(
                "v" + version, "v" + observed, channel, load_policy()
            )
            < 0
        ):
            raise ValueError(
                f"refusing to move {name} {npm_tag} backward from {observed}"
            )
        preflight_package(name, version, npm_tag, snapshot / f"{stem}-{version}.tgz")
    wheels = sorted(snapshot.glob("*.whl"))
    missing_registry_files(
        expected_release_files(wheels),
        pypi_release_files("antfly-embedded", python_version_from_release(version)),
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot", type=Path, required=True)
    parser.add_argument("--version", required=True)
    parser.add_argument("--commit", required=True)
    parser.add_argument("--npm-tag", required=True)
    args = parser.parse_args()
    preflight(args.snapshot, args.version, args.commit, args.npm_tag)
