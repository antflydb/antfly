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

"""Install the existing pinned FP32 base fixture for the Lite public-path test."""

import argparse
import hashlib
import json
import shutil
import urllib.request
from pathlib import Path


def prepare(destination: Path, source: Path | None = None) -> None:
    root = Path(__file__).resolve().parents[2]
    receipt = json.loads(
        (
            root / "zig/pkg/inference/testdata/gliner25/pipeline_cases_base.json"
        ).read_text()
    )
    for name, pin in receipt["model_files"].items():
        target = destination / name
        target.parent.mkdir(parents=True, exist_ok=True)
        temporary = target.with_suffix(target.suffix + ".partial")
        try:
            if source:
                shutil.copyfile(source / name, temporary)
            else:
                url = f"https://huggingface.co/{receipt['model_id']}/resolve/{receipt['revision']}/{name}"
                with (
                    urllib.request.urlopen(url, timeout=120) as response,
                    temporary.open("wb") as output,
                ):
                    shutil.copyfileobj(response, output)
            with temporary.open("rb") as data:
                digest = hashlib.file_digest(data, "sha256").hexdigest()
            if temporary.stat().st_size != pin["size_bytes"] or digest != pin["sha256"]:
                raise ValueError(f"pinned model file mismatch: {name}")
            temporary.replace(target)
        finally:
            temporary.unlink(missing_ok=True)
    (destination / "model_manifest.json").write_text(
        json.dumps(
            {
                "type": "recognizer",
                "tasks": ["extract"],
                "inputs": ["text"],
                "capabilities": ["named_entity_recognition", "extraction", "relations"],
            }
        )
    )


def prepare_embedding(destination: Path, source: Path | None = None) -> None:
    """Install the registry's pinned Q8 bundle for the public embedder gate."""
    root = Path(__file__).resolve().parents[2]
    receipt = json.loads(
        (root / "scripts/testdata/eval_extract/qwen_embedding_bundle.json").read_text()
    )
    for name, pin in receipt["model_files"].items():
        target = destination / name
        target.parent.mkdir(parents=True, exist_ok=True)
        temporary = target.with_suffix(target.suffix + ".partial")
        try:
            if source and (source / name).is_file():
                shutil.copyfile(source / name, temporary)
            else:
                model_id = pin.get("model_id", receipt["model_id"])
                revision = pin.get("revision", receipt["revision"])
                url = f"https://huggingface.co/{model_id}/resolve/{revision}/{name}"
                with (
                    urllib.request.urlopen(url, timeout=120) as response,
                    temporary.open("wb") as output,
                ):
                    shutil.copyfileobj(response, output)
            with temporary.open("rb") as data:
                digest = hashlib.file_digest(data, "sha256").hexdigest()
            if temporary.stat().st_size != pin["size_bytes"] or digest != pin["sha256"]:
                raise ValueError(f"pinned embedding model file mismatch: {name}")
            temporary.replace(target)
        finally:
            temporary.unlink(missing_ok=True)
    manifest = receipt["model_manifest"].encode()
    if hashlib.sha256(manifest).hexdigest() != receipt["model_manifest_sha256"]:
        raise ValueError("pinned embedding manifest mismatch")
    (destination / "model_manifest.json").write_bytes(manifest)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("destination", type=Path)
    parser.add_argument(
        "--source", type=Path, help="Verify/copy an already downloaded pinned bundle"
    )
    parser.add_argument(
        "--embedding-destination",
        type=Path,
        help="Also install the pinned Qwen Q8 bundle",
    )
    args = parser.parse_args()
    prepare(args.destination, args.source)
    if args.embedding_destination:
        prepare_embedding(args.embedding_destination)
