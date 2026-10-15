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

"""Prepare a qualification-only Docker context from verified reference sources.

Run in the pinned PyTorch reference environment. No weights, credentials or
native production decoder sources enter this context. Docker installation and
execution are separate explicit commands.
"""

import argparse
import importlib.metadata
import json
from pathlib import Path
import shutil
import tarfile

if __package__:
    from . import reference
else:
    import reference


FFMPEG_SHA256 = "8c3850283eb25fa026482078a04051e0be17347b09ef81a0849bec15a96e002e"


def prepare(output, ffmpeg_source):
    import transformers

    source = Path(transformers.__file__).parent
    for name, expected in reference.SOURCE_HASHES.items():
        if reference.sha256(source / name) != expected:
            raise ValueError(f"reference implementation differs: {name}")
    distribution = importlib.metadata.distribution("transformers")
    if distribution.version != "5.19.0.dev0":
        raise ValueError("reference package metadata differs from pinned environment")
    metadata = [
        distribution.locate_file(file).parent
        for file in distribution.files or ()
        if file.name == "METADATA" and str(file.parent).endswith(".dist-info")
    ]
    if len(metadata) != 1:
        raise ValueError("reference distribution metadata is ambiguous")
    if reference.sha256(ffmpeg_source) != FFMPEG_SHA256:
        raise ValueError("FFmpeg source archive differs from pinned 9.0.2 release")
    with tarfile.open(ffmpeg_source, "r:xz") as archive:
        release = archive.extractfile("ffmpeg-9.0.2/RELEASE")
        if release is None or release.read(128).strip() != b"9.0.2":
            raise ValueError("FFmpeg archive release marker differs")
    # Refuse to merge stale packages or replace caller files.
    output.mkdir(parents=True, exist_ok=False)
    shutil.copytree(
        source,
        output / "transformers",
        ignore=shutil.ignore_patterns("__pycache__", "*.pyc"),
    )
    shutil.copytree(
        metadata[0],
        output / "transformers.dist-info",
        ignore=shutil.ignore_patterns("direct_url.json"),
    )
    shutil.copyfile(
        Path(__file__).with_name("linux-reference.Dockerfile"), output / "Dockerfile"
    )
    shutil.copyfile(ffmpeg_source, output / "ffmpeg-9.0.2.tar.xz")
    receipt = {
        "ffmpeg_source_url": "https://ffmpeg.org/releases/ffmpeg-9.0.2.tar.xz",
        "ffmpeg_source_sha256": FFMPEG_SHA256,
        "purpose": "Linux reference qualification only",
        "transformers_revision": reference.TRANSFORMERS_REVISION,
        "required_source_sha256": reference.SOURCE_HASHES,
        "context_sha256": {
            str(file.relative_to(output)): reference.sha256(file)
            for file in sorted(output.rglob("*"))
            if file.is_file()
        },
    }
    (output / "context-receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output", type=Path, required=True, help="New context directory"
    )
    parser.add_argument(
        "--ffmpeg-source",
        type=Path,
        required=True,
        help="Official ffmpeg-9.0.2.tar.xz archive; SHA-256 is verified before copying",
    )
    args = parser.parse_args()
    prepare(args.output, args.ffmpeg_source)
    print(args.output / "context-receipt.json")


if __name__ == "__main__":
    main()
