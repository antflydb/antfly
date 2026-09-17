#!/usr/bin/env python3
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
OPTIONS = ("-std=c++17", "-O3", "-DNDEBUG", "--fmad=false")


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
    args = parser.parse_args()
    nvcc = args.cuda / "bin/nvcc"
    version = subprocess.check_output([nvcc, "--version"], text=True)
    outputs = {}
    sources = {}
    for direction in DIRECTIONS:
        source_name = f"gliner25_boundary_attention_{direction}.cu"
        source = (ARTIFACTS / source_name).read_bytes()
        sources[source_name] = sha(source)
        with tempfile.TemporaryDirectory(prefix="gliner25-boundary-") as temp:
            src = Path(temp) / source_name
            src.write_bytes(source)
            for suffix, arch in (("cubin", "sm_89"), ("sm80.cubin", "sm_80")):
                name = f"gliner25_boundary_attention_{direction}.{suffix}"
                out = Path(temp) / name
                subprocess.run(
                    [
                        str(nvcc),
                        "-cubin",
                        f"-arch={arch}",
                        *OPTIONS,
                        str(src),
                        "-o",
                        str(out),
                    ],
                    check=True,
                )
                outputs[name] = out.read_bytes()
    metadata = {
        "profile": "gliner25_boundary_attention_native_d32_v1",
        "compiler": version.strip(),
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
