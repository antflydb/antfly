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

"""Reproduce licensed retrieval clips from a hash-verified original source.

The checked-in clips are authoritative inputs. Encoder revisions can change
container bytes; this tool fails if regeneration differs from the manifest.
"""

import argparse
import json
from pathlib import Path
import subprocess
import tempfile

import reference


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--source", type=Path, required=True)
    p.add_argument(
        "--manifest",
        type=Path,
        default=Path(__file__).parent / "testdata/retrieval/corpus.json",
    )
    p.add_argument("--output-dir", type=Path, required=True)
    args = p.parse_args()
    data = json.loads(args.manifest.read_text())
    source_hash = reference.sha256(args.source)
    args.output_dir.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="antfly-video-corpus-") as temporary:
        for row in data["documents"]:
            if source_hash != row["provenance"]["source_sha256"]:
                raise ValueError("original source hash mismatch")
            command = list(row["provenance"]["encoding_command"])
            command[command.index("-i") + 1] = str(args.source.resolve())
            # Manifest output paths are restricted to one filename.
            target = Path(row["path"])
            if target.name != row["path"]:
                raise ValueError("unsafe corpus output path")
            command[-1] = str(Path(temporary) / target)
            subprocess.run(["ffmpeg", *command], check=True, timeout=120)
            encoded = Path(command[-1])
            if reference.sha256(encoded) != row["sha256"]:
                raise ValueError("encoder output differs; preserve the pinned corpus")
            encoded.replace(args.output_dir / target)
    (args.output_dir / "corpus.json").write_bytes(args.manifest.read_bytes())


if __name__ == "__main__":
    main()
