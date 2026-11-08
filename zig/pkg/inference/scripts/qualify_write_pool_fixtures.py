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

"""Acquire public, pinned write-pool artifacts without a private model directory."""

import argparse
from concurrent.futures import ThreadPoolExecutor
import fcntl
import hashlib
import json
import os
import shutil
import errno
from pathlib import Path, PurePosixPath
import urllib.request

from qualify_write_pool_production import CORPUS_SHA

ROOT = Path(__file__).resolve().parents[4]
MODELS = (
    (
        "embedders",
        "google/embeddinggemma-2",
        "google/embeddinggemma-2",
        "f9972ac22e696a105c482202646979e33e4b5a1e",
    ),
    (
        "extractors",
        "antflydb/GLiNER2.5",
        "fastino/gliner2.5-base-v1",
        "07d99c0ba5d15cb3b24e0a236fa99bdd4ef87a5c",
    ),
    (
        "transcribers",
        "openai/whisper-small",
        "openai/whisper-small",
        "973afd24965f72e36ca33b3055d56a652f456b4d",
    ),
    (
        "readers",
        "lightonai/LightOnOCR-3-0.8B",
        "lightonai/LightOnOCR-3-0.8B",
        "36d8636925327c671bd8e479c517a004f7566bd9",
    ),
)
SPEECH_URL = "https://huggingface.co/datasets/hf-internal-testing/librispeech_asr_dummy/resolve/main/clean/validation-00000-of-00001.parquet"


def verify(path, *, sha256=None, blob_id=None, size=None):
    if size is not None and path.stat().st_size != size:
        raise ValueError(f"size mismatch: {path}")
    with path.open("rb") as source:
        digest = hashlib.file_digest(source, "sha256").hexdigest()
    if sha256 is not None and digest != sha256:
        raise ValueError(f"SHA-256 mismatch: {path}")
    if blob_id is not None:
        git_hash = hashlib.sha1(f"blob {path.stat().st_size}\0".encode())
        with path.open("rb") as source:
            while chunk := source.read(1024 * 1024):
                git_hash.update(chunk)
        if git_hash.hexdigest() != blob_id:
            raise ValueError(f"Git blob mismatch: {path}")
    return {"path": str(path), "size_bytes": path.stat().st_size, "sha256": digest}


def acquire(url, path, **identity):
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        return verify(path, **identity)
    temporary = path.with_name(path.name + ".part")
    with (
        urllib.request.urlopen(url, timeout=60) as source,
        temporary.open("wb") as target,
    ):
        while chunk := source.read(1024 * 1024):
            target.write(chunk)
    receipt = verify(temporary, **identity)
    temporary.replace(path)
    receipt["path"] = str(path)
    return receipt


def snapshot(cache, model):
    kind, alias, repository, revision = model
    directory = cache / "huggingface" / repository / revision
    directory.parent.mkdir(parents=True, exist_ok=True)
    with directory.with_suffix(".acquisition.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        with urllib.request.urlopen(
            f"https://huggingface.co/api/models/{repository}/revision/{revision}?blobs=true",
            timeout=30,
        ) as response:
            metadata = json.load(response)
        if metadata["sha"] != revision:
            raise ValueError(f"revision mismatch for {repository}")
        files = []
        for entry in metadata["siblings"]:
            name = PurePosixPath(entry["rfilename"])
            if name.is_absolute() or ".." in name.parts:
                raise ValueError("unsafe model artifact path")
            if name.suffix not in (".json", ".safetensors", ".model", ".txt", ".jinja"):
                continue
            lfs = entry.get("lfs")
            identity = (
                {"sha256": lfs["sha256"]} if lfs else {"blob_id": entry["blobId"]}
            )
            receipt = acquire(
                f"https://huggingface.co/{repository}/resolve/{revision}/{name}",
                directory / str(name),
                size=entry["size"],
                **identity,
            )
            files.append(receipt)
    # Shared verified checkpoints may be read-only. Receipts belong to the
    # requesting campaign, never inside an immutable artifact directory.
    return (
        kind,
        alias,
        directory,
        {"repository": repository, "revision": revision, "files": files},
    )


def install_alias(models_dir, kind, alias, directory):
    directory = directory.resolve()
    link = models_dir.resolve() / kind / alias
    link.parent.mkdir(parents=True, exist_ok=True)
    if link.is_symlink():
        if link.resolve() != directory:
            raise ValueError(f"model alias points elsewhere: {link}")
        link.unlink()
    link.mkdir(exist_ok=True)
    overrides = (
        {
            "model_manifest.json": "runtime_model_manifest.json",
            "chat_template.jinja": "runtime_chat_template.jinja",
        }
        if alias == "lightonai/LightOnOCR-3-0.8B"
        else {}
    )

    def materialize(source, destination):
        for artifact in source.iterdir():
            if source == directory and artifact.name in overrides:
                continue
            target = destination / artifact.name
            if target.is_symlink():
                if target.resolve() != artifact:
                    raise ValueError(f"imported artifact points elsewhere: {target}")
                target.unlink()
            if artifact.is_dir():
                target.mkdir(exist_ok=True)
                materialize(artifact, target)
                continue
            if target.exists():
                if target.samefile(artifact):
                    continue
                if (
                    target.is_dir()
                    or verify(target)["sha256"] != verify(artifact)["sha256"]
                ):
                    raise ValueError(f"imported artifact differs: {target}")
                continue
            # Artifact confinement requires real files under this root.
            try:
                os.link(artifact, target)
            except OSError as error:
                if error.errno not in (errno.EXDEV, errno.EPERM, errno.EACCES):
                    raise
                shutil.copyfile(artifact, target)

    materialize(directory, link)
    for name, fixture in overrides.items():
        content = (
            ROOT / "zig/pkg/inference/scripts/lightonocr3" / fixture
        ).read_bytes()
        target = link / name
        if target.is_symlink() or (target.exists() and target.read_bytes() != content):
            raise ValueError(f"reader import metadata differs: {target}")
        if not target.exists():
            target.write_bytes(content)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--cache", type=Path, default=Path.home() / ".cache/antfly/conformance-fixtures"
    )
    parser.add_argument("--models-dir", type=Path, required=True)
    parser.add_argument("--config", type=Path, required=True)
    args = parser.parse_args()
    with ThreadPoolExecutor(max_workers=4) as pool:
        for kind, alias, directory, receipt in pool.map(
            lambda model: snapshot(args.cache.resolve(), model), MODELS
        ):
            install_alias(args.models_dir, kind, alias, directory)
            receipt_path = (
                args.models_dir.resolve().parent
                / "acquisition"
                / kind
                / (alias + ".json")
            )
            receipt_path.parent.mkdir(parents=True, exist_ok=True)
            receipt_path.write_text(json.dumps(receipt, indent=2) + "\n")
    speech = args.cache.resolve() / "gemma4-transcription" / f"{CORPUS_SHA}.parquet"
    acquire(SPEECH_URL, speech, sha256=CORPUS_SHA)
    config = json.loads((ROOT / "configs/inference-write-pool-40g.json").read_text())
    config["inference"]["models_dir"] = str(args.models_dir.resolve())
    config["inference"]["ml_dir"] = str(args.models_dir.resolve().parent / "ml")
    args.config.parent.mkdir(parents=True, exist_ok=True)
    args.config.write_text(json.dumps(config, indent=2) + "\n")
    print(
        json.dumps(
            {"config": str(args.config.resolve()), "speech_corpus": str(speech)},
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
