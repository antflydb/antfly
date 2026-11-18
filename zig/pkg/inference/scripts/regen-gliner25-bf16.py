#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Reproduce the GLiNER BF16 boundary kernels with the pinned CUDA toolkit."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile

VERSION = "release 13.2, V13.2.78"
FLAGS = ["-cubin", "-arch=sm_89"]
SYMBOLS = ["antfly_gliner_bf16_full_" + name + "_v1" for name in
           ("round", "norm", "scale_key", "views", "softmax", "gelu", "norm_dot_bias", "scale_views")]


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--write", action="store_true")
    mode.add_argument("--check-source-policy", action="store_true")
    args = parser.parse_args()
    package = Path(__file__).resolve().parents[1]
    source = package / "src/ops/cuda/kernels/gliner25_bf16_full.cu"
    artifact = package / "src/ops/cuda/artifacts/gliner25_bf16_full_sm89.cubin"
    metadata = artifact.with_name("gliner25_bf16_full.json")
    expected = {"abi_version": 1, "architecture": "sm_89", "flags": FLAGS,
                "symbols": SYMBOLS, "source_sha256": sha(source),
                "artifact_sha256": sha(artifact) if artifact.exists() else None}
    if args.check_source_policy:
        record = json.loads(metadata.read_text())
        if VERSION not in record.get("nvcc_version", "") or any(
                record.get(key) != value for key, value in expected.items()):
            raise RuntimeError("GLiNER BF16 source/artifact metadata is stale")
        print("GLiNER BF16 source policy passes")
        return
    nvcc = os.environ.get("NVCC", "/usr/local/cuda/bin/nvcc")
    version = subprocess.check_output([nvcc, "--version"], text=True).strip()
    if VERSION not in version:
        raise RuntimeError("Requires the pinned CUDA 13.2.78 toolkit")
    cache = package.parents[2] / ".zig-cache/cuda-artifacts"
    cache.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=cache) as temporary:
        rebuilt = Path(temporary) / artifact.name
        subprocess.run([nvcc, *FLAGS, str(source), "-o", str(rebuilt)], check=True)
        symbols = subprocess.check_output(
            [str(Path(nvcc).parent / "cuobjdump"), "--dump-elf-symbols", str(rebuilt)], text=True)
        if any(symbol not in symbols for symbol in SYMBOLS):
            raise RuntimeError("GLiNER BF16 artifact is missing an ABI symbol")
        if args.write:
            artifact.write_bytes(rebuilt.read_bytes())
            expected.update(artifact_sha256=sha(artifact), nvcc_version=version)
            metadata.write_text(json.dumps(expected, indent=2) + "\n")
        elif rebuilt.read_bytes() != artifact.read_bytes():
            raise RuntimeError("GLiNER BF16 artifact is not reproducible")
        else:
            record = json.loads(metadata.read_text())
            if any(record.get(key) != value for key, value in expected.items()):
                raise RuntimeError("GLiNER BF16 metadata is stale")
    print("GLiNER BF16 artifact check passes")


if __name__ == "__main__":
    main()
