#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Rebuild the SM89 GLiNER accurate Tensor Core artifact with pinned inputs."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tarfile
import tempfile
import urllib.request

COMMIT = "ad7b2f5e84fcfa124cb02b91d5bd26d238c0459e"  # NVIDIA CUTLASS v3.9.2
ARCHIVE_SHA256 = "36429479df2ab1cd1c7981c2d34e786106326ccfefc8846f902f76151a65e112"
TREE_SHA256 = "992cc63a5ec8e51e391e638e88f1fe213a19eecedc23bc75d5cb5f5b8ddbb49c"
SYMBOLS = ["antfly_gliner_tf32x3_small_v1", "antfly_gliner_bf16x3_small_v1", "antfly_gliner_bf16x3_medium_v1", "antfly_gliner_bf16x3_large_v1"]
NVCC_VERSION = "release 13.2, V13.2.78"
FLAGS = ["-cubin", "-O3", "-DNDEBUG", "-std=c++17", "--expt-relaxed-constexpr", "-arch=sm_89"]


def sha(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def tree_sha(root):
    result = hashlib.sha256()
    for path in sorted(root.rglob("*")):
        if path.is_symlink():
            raise RuntimeError("CUTLASS cache must not contain symlinks")
        if path.is_file():
            result.update(path.relative_to(root).as_posix().encode() + b"\0" + path.read_bytes() + b"\0")
    return result.hexdigest()


def dependency(cache):
    target = cache / ("cutlass-" + COMMIT)
    if target.exists():
        if tree_sha(target) != TREE_SHA256:
            raise RuntimeError("CUTLASS cache checksum mismatch: " + str(target))
        return target
    with tempfile.TemporaryDirectory(prefix=".cutlass-", dir=cache) as temporary:
        temporary = Path(temporary)
        archive = temporary / "source.tar.gz"
        request = urllib.request.Request("https://codeload.github.com/NVIDIA/cutlass/tar.gz/" + COMMIT, headers={"User-Agent": "Antfly-CUDA-artifact-builder"})
        with urllib.request.urlopen(request, timeout=120) as response, archive.open("wb") as output:
            shutil.copyfileobj(response, output)
        if sha(archive) != ARCHIVE_SHA256:
            raise RuntimeError("CUTLASS archive checksum mismatch")
        headers = temporary / "headers"
        headers.mkdir()
        with tarfile.open(archive) as source:
            for member in source:
                parts = Path(member.name).parts[1:]
                if not parts or ".." in parts or not (parts[0] == "include" or parts == ("LICENSE.txt",)):
                    continue
                output = headers.joinpath(*parts)
                if member.isdir():
                    output.mkdir(parents=True, exist_ok=True)
                elif member.isfile():
                    output.parent.mkdir(parents=True, exist_ok=True)
                    output.write_bytes(source.extractfile(member).read())
        if tree_sha(headers) != TREE_SHA256:
            raise RuntimeError("CUTLASS extracted-header checksum mismatch")
        headers.rename(target)
    return target


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    choice = parser.add_mutually_exclusive_group()
    choice.add_argument("--write", action="store_true")
    choice.add_argument("--check", action="store_true")
    choice.add_argument("--check-source-policy", action="store_true")
    args = parser.parse_args()
    package = Path(__file__).resolve().parents[1]
    repository = package.parents[2]
    kernel_dir = package / "src/ops/cuda/kernels"
    sources = [kernel_dir / "gliner25_fast_f32.cu", kernel_dir / "gliner25_fast_f32_mma.cuh"]
    artifact_dir = package / "src/ops/cuda/artifacts"
    output = artifact_dir / "gliner25_fast_f32_sm89.cubin"
    metadata = artifact_dir / "gliner25_fast_f32.json"
    license_file = artifact_dir / "gliner25_fast_f32_LICENSE.txt"
    if not all(path.is_file() for path in sources + [license_file]):
        raise RuntimeError("Missing GLiNER kernel source or CUTLASS license")
    if args.check_source_policy:
        record = json.loads(metadata.read_text())
        expected = {
            "abi_version": 1, "architecture": "sm_89", "global_scratch_bytes": 0,
            "cutlass_commit": COMMIT, "cutlass_archive_sha256": ARCHIVE_SHA256,
            "cutlass_tree_sha256": TREE_SHA256, "flags": FLAGS, "symbols": SYMBOLS,
            "source_sha256": {path.name: sha(path) for path in sources},
            "artifact_sha256": sha(output),
        }
        if NVCC_VERSION not in record.get("nvcc_version", ""):
            raise RuntimeError("GLiNER metadata does not identify the pinned NVCC version")
        if any(record.get(key) != value for key, value in expected.items()):
            raise RuntimeError("GLiNER source, artifact or metadata policy is stale")
        if sha(license_file) != "42fec630f410aa308f70a51a89fadcd19586fa620f9831a32bee528a9a10000e":
            raise RuntimeError("CUTLASS artifact license differs from the pinned source")
        print("GLiNER accurate Tensor Core source policy passes")
        return
    cache = repository / ".zig-cache/cuda-deps"
    cache.mkdir(parents=True, exist_ok=True)
    headers = dependency(cache)
    if license_file.read_bytes() != (headers / "LICENSE.txt").read_bytes():
        raise RuntimeError("CUTLASS artifact license differs from the pinned source")
    nvcc = os.environ.get("NVCC") or str(Path(os.environ.get("CUDA_HOME", "/usr/local/cuda")) / "bin/nvcc")
    version = subprocess.check_output([nvcc, "--version"], text=True).strip()
    if NVCC_VERSION not in version:
        raise RuntimeError("CUDA 13.2.78 is required for deterministic artifact regeneration")
    with tempfile.TemporaryDirectory(prefix=".gliner25-", dir=cache) as temporary:
        generated = Path(temporary) / "gliner25_fast_f32_sm89.cubin"
        subprocess.run([nvcc, *FLAGS, "-I" + str(headers / "include"), str(sources[0]), "-o", str(generated)], check=True)
        cuobjdump = str(Path(nvcc).parent / "cuobjdump")
        symbols = subprocess.check_output([cuobjdump, "--dump-elf-symbols", str(generated)], text=True)
        if any(symbol not in symbols for symbol in SYMBOLS):
            raise RuntimeError("Missing GLiNER CUDA entry point")
        record = {"abi_version": 1, "architecture": "sm_89", "precision": "FP32 storage and I/O; BF16x3 or TF32x3 arithmetic with FP32 accumulation", "global_scratch_bytes": 0, "cutlass_commit": COMMIT, "cutlass_archive_sha256": ARCHIVE_SHA256, "cutlass_tree_sha256": TREE_SHA256, "nvcc_version": version, "flags": FLAGS, "source_sha256": {path.name: sha(path) for path in sources}, "artifact_sha256": sha(generated), "symbols": SYMBOLS}
        record_bytes = (json.dumps(record, indent=2, sort_keys=True) + "\n").encode()
        if args.write:
            for path, data in ((output, generated.read_bytes()), (metadata, record_bytes)):
                staged = path.with_name(path.name + ".tmp")
                staged.write_bytes(data)
                staged.replace(path)
            print("wrote", output)
        elif not output.is_file() or output.read_bytes() != generated.read_bytes() or not metadata.is_file() or metadata.read_bytes() != record_bytes:
            raise RuntimeError("GLiNER CUDA artifact is stale; rerun with --write")
        else:
            print("GLiNER accurate Tensor Core artifact is current")


if __name__ == "__main__":
    main()
