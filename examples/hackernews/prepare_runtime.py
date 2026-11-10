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

"""Download the pinned producer libraries; verify release metadata, size and SHA-256."""

import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import tarfile

PIN = json.loads(Path(__file__).with_name("runtime.json").read_text())


def verify_metadata(metadata):
    if (
        metadata.get("tag") != PIN["version"]
        or metadata.get("commit") != PIN["source_commit"]
    ):
        raise ValueError("Release source/version differs from runtime.json")
    assets = {asset["name"]: asset for asset in metadata.get("artifacts", [])}
    for asset in PIN["libraries"].values():
        if any(
            assets.get(asset["name"], {}).get(key) != asset[key]
            for key in ("size", "sha256")
        ):
            raise ValueError(
                "Release library differs from runtime.json: " + asset["name"]
            )


def verify_archive(path, asset):
    with path.open("rb") as source:
        digest = hashlib.file_digest(source, "sha256").hexdigest()
    if path.stat().st_size != asset["size"] or digest != asset["sha256"]:
        raise ValueError("Library size/checksum mismatch: " + asset["name"])


def prepare(root, architectures):
    metadata = json.loads(
        subprocess.check_output(
            [
                "gh",
                "release",
                "download",
                PIN["version"],
                "--repo",
                "antflydb/antfly",
                "--pattern",
                "metadata.json",
                "--output",
                "-",
            ],
            text=True,
            timeout=60,
        )
    )
    verify_metadata(metadata)
    downloads = root / "qualification-assets"
    downloads.mkdir(exist_ok=True)
    for arch in architectures:
        asset = PIN["libraries"][arch]
        archive = downloads / asset["name"]
        destination = root / "qualification-linux" / arch
        if destination.exists():
            raise ValueError(
                f"Refusing to overwrite {destination}; use a fresh worktree or review its removal"
            )
        if not archive.exists():
            subprocess.run(
                [
                    "gh",
                    "release",
                    "download",
                    PIN["version"],
                    "--repo",
                    "antflydb/antfly",
                    "--pattern",
                    asset["name"],
                    "--output",
                    str(archive),
                ],
                check=True,
                timeout=180,
            )
        verify_archive(archive, asset)
        # Reject traversal and escaping links even for a checksum-verified archive.
        with tarfile.open(archive) as bundle:
            bundle.extractall(destination, filter="data")
        if not (destination / "lib/libantfly.so").is_file():
            raise ValueError("Unexpected embedded library archive layout")
        (destination / "hn-runtime.json").write_text(
            json.dumps(
                {
                    "version": PIN["version"],
                    "source_commit": PIN["source_commit"],
                    "architecture": arch,
                    "asset": asset,
                },
                indent=2,
            )
            + "\n"
        )
        print(f"Verified {arch}: {asset['name']}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--architecture", choices=("amd64", "arm64"), action="append", required=True
    )
    args = parser.parse_args()
    try:
        prepare(
            Path(__file__).resolve().parents[2], list(dict.fromkeys(args.architecture))
        )
    except (OSError, ValueError, subprocess.SubprocessError, tarfile.TarError) as error:
        parser.exit(1, str(error) + "\n")


if __name__ == "__main__":
    main()
