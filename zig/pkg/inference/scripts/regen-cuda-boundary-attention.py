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

"""Regenerate self-contained GLiNER2.5 boundary-attention cubins.

Requires only a CUDA toolkit with nvcc. No PyTorch, ATen, CUTLASS, or Python
runtime headers are used.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile

ARTIFACTS = Path(__file__).resolve().parents[1] / "src/ops/cuda/artifacts"
DIRECTIONS = ("forward", "backward")
ARCHITECTURES = {"sm_89": "cubin", "sm_80": "sm80.cubin", "sm_120": "sm120.cubin"}
OPTIONS = ("-std=c++17", "-O3", "-DNDEBUG", "--fmad=false")


def source_name(direction, arch):
    target = "_sm120" if arch == "sm_120" else ""
    return f"gliner25_boundary_attention_{direction}{target}.cu"


def source_files(arch):
    files = [source_name(direction, arch) for direction in DIRECTIONS]
    if arch == "sm_120":
        files.append("gliner25_boundary_attention_sm120.cuh")
    return files


def validate_retained_sources(prior, sources, selected):
    for arch in ARCHITECTURES:
        if arch in selected:
            continue
        for name in source_files(arch):
            if prior.get("source_sha256", {}).get(name) != sources[name]:
                raise RuntimeError(f"source changed for retained {arch}: {name}")



def sha(data):
    return hashlib.sha256(data).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--write", action="store_true")
    mode.add_argument("--check", action="store_true")
    parser.add_argument("--output-dir", type=Path, default=ARTIFACTS)
    parser.add_argument(
        "--cuda",
        type=Path,
        default=Path(os.environ.get("CUDA_HOME", "/usr/local/cuda")),
    )
    parser.add_argument("--architectures", nargs="+", choices=tuple(ARCHITECTURES), default=tuple(ARCHITECTURES))
    args = parser.parse_args()
    nvcc = args.cuda / "bin/nvcc"
    version = subprocess.check_output([nvcc, "--version"], text=True)
    # Partial regeneration preserves legacy images and their trainer identities.
    # A closed manifest still verifies every shipped byte and source hash.
    prior_path = args.output_dir / "gliner25_boundary_attention.json"
    if not prior_path.exists():
        prior_path = ARTIFACTS / "gliner25_boundary_attention.json"
    prior = json.loads(prior_path.read_text()) if prior_path.exists() else {}
    outputs = {}
    for name, expected in prior.get("artifacts", {}).items():
        path = prior_path.parent / name
        data = path.read_bytes()
        if sha(data) != expected:
            raise RuntimeError(f"invalid prior artifact: {path}")
        outputs[name] = data
    compiler_by_arch = dict(prior.get("compiler_by_arch", {}))
    for arch, suffix in ARCHITECTURES.items():
        if any(f"gliner25_boundary_attention_{d}.{suffix}" in outputs for d in DIRECTIONS):
            compiler_by_arch.setdefault(arch, prior.get("compiler", version.strip()))
    sources = {
        name: sha((ARTIFACTS / name).read_bytes())
        for arch in ARCHITECTURES for name in source_files(arch)
    }
    if prior:
        validate_retained_sources(prior, sources, args.architectures)
    with tempfile.TemporaryDirectory(prefix="gliner25-boundary-") as temp:
        for name in sources:
            (Path(temp) / name).write_bytes((ARTIFACTS / name).read_bytes())
        for arch in args.architectures:
            for direction in DIRECTIONS:
                source = Path(temp) / source_name(direction, arch)
                name = f"gliner25_boundary_attention_{direction}.{ARCHITECTURES[arch]}"
                out = Path(temp) / name
                subprocess.run(
                    [str(nvcc), "-cubin", f"-arch={arch}", *OPTIONS, str(source), "-o", str(out)],
                    check=True,
                )
                outputs[name] = out.read_bytes()
    for arch in args.architectures:
        compiler_by_arch[arch] = version.strip()
    metadata = {
        "compiler_by_arch": compiler_by_arch,
        "source_files_by_arch": {arch: source_files(arch) for arch in ARCHITECTURES},
        "profile_by_arch": {
            arch: "gliner25_boundary_attention_warp_d32_v2" if arch == "sm_120" else "gliner25_boundary_attention_native_d32_v1"
            for arch in ARCHITECTURES
        },
        "profile": "gliner25_boundary_attention_native_d32_v1",
        "compiler": version.strip() if set(args.architectures) == set(ARCHITECTURES) else prior.get("compiler", version.strip()),
        "options": list(OPTIONS),
        "source_sha256": sources,
        "artifacts": {name: sha(data) for name, data in outputs.items()},
    }
    outputs["gliner25_boundary_attention.json"] = (
        json.dumps(metadata, indent=2, sort_keys=True) + "\n"
    ).encode()
    for name, data in outputs.items():
        destination = args.output_dir / name
        if args.check:
            if not destination.exists() or destination.read_bytes() != data:
                raise RuntimeError(f"stale boundary-attention artifact: {destination}")
        else:
            args.output_dir.mkdir(parents=True, exist_ok=True)
            with tempfile.NamedTemporaryFile(
                dir=args.output_dir, prefix=".boundary-", delete=False
            ) as f:
                temporary = Path(f.name)
                f.write(data)
            temporary.chmod(0o644)
            temporary.replace(destination)
    print("Boundary attention artifacts " + ("match" if args.check else "written"))


if __name__ == "__main__":
    main()
