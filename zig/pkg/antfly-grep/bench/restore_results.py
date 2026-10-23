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

"""Restore checksummed historical benchmark evidence from reachable Git blobs."""

import argparse
import hashlib
import json
import subprocess
import tempfile
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--file", action="append", default=[])
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    manifest = json.loads(
        Path(__file__)
        .with_name("results")
        .joinpath("archive-manifest.json")
        .read_text()
    )
    entries = {entry["name"]: entry for entry in manifest["files"]}
    unknown = set(args.file) - entries.keys()
    if unknown:
        parser.error("Unknown archived file: " + ", ".join(sorted(unknown)))
    args.output.mkdir(parents=True, exist_ok=True)
    root = Path(__file__).resolve().parents[4]
    restored = 0
    for name in args.file or entries:
        entry = entries[name]
        if Path(name).name != name or len(entry["git_blob"]) != 40:
            raise ValueError("Invalid archive entry")
        destination = args.output / name
        if destination.exists():
            if hashlib.sha256(destination.read_bytes()).hexdigest() != entry["sha256"]:
                raise ValueError(
                    "Refusing to overwrite different bytes: " + str(destination)
                )
            continue
        temporary = None
        try:
            with tempfile.NamedTemporaryFile(dir=args.output, delete=False) as stream:
                temporary = Path(stream.name)
                subprocess.run(
                    ["git", "cat-file", "blob", entry["git_blob"]],
                    cwd=root,
                    stdout=stream,
                    check=True,
                )
            data = temporary.read_bytes()
            if (
                len(data) != entry["bytes"]
                or hashlib.sha256(data).hexdigest() != entry["sha256"]
            ):
                raise ValueError("Archive checksum mismatch: " + name)
            temporary.replace(destination)
            restored += 1
        finally:
            if temporary is not None:
                temporary.unlink(missing_ok=True)
    print("Restored", restored, "verified files to", args.output)


if __name__ == "__main__":
    main()
