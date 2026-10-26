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

"""Generate the static LightOnOCR-3 CUDA head images using CUDA 13.0 and Triton.

Generation requires an SM12.1 device. The deployed runtime needs only the CUDA
driver; Python, PyTorch and Triton are build and qualification dependencies.
"""

import argparse
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import re
import subprocess
import sys


def main():
    root = Path(__file__).resolve().parents[5]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, default=root / ".cache/lighton-head")
    parser.add_argument("--ptxas", default="/usr/local/cuda/bin/ptxas")
    args = parser.parse_args()
    output = args.output_dir.resolve()
    if not output.is_relative_to(root):
        parser.error("output directory must be inside the worktree")
    version = subprocess.check_output([args.ptxas, "--version"], text=True)
    if "release 13.0" not in version:
        parser.error("CUDA 13.0 ptxas is required for driver-compatible PTX 9.0")
    for variable, directory in {
        "TRITON_CACHE_DIR": "triton",
        "CUDA_CACHE_PATH": "cuda",
        "TORCHINDUCTOR_CACHE_DIR": "inductor",
        "PYTHONPYCACHEPREFIX": "pycache",
        "TMPDIR": "tmp",
    }.items():
        path = output / directory
        path.mkdir(parents=True, exist_ok=True)
        os.environ[variable] = str(path)
    os.environ.update(
        TRITON_PTXAS_PATH=args.ptxas,
        TRITON_PTXAS_BLACKWELL_PATH=args.ptxas,
        TRITON_DISABLE_LINE_INFO="1",
    )
    if importlib.metadata.version("triton") != "3.7.1":
        parser.error("Triton 3.7.1 is required to reproduce these static images")
    import torch

    if torch.cuda.get_device_capability(0) != (12, 1):
        parser.error("generation is qualified on SM12.1 only")
    sys.path.insert(0, str(root / "zig/pkg/inference/src/ops/cuda/prototypes"))
    from lighton_head import lighton_head, lighton_head_finish

    dummy_bf16 = torch.empty(1, dtype=torch.bfloat16, device="cuda")
    dummy_f32 = torch.empty(1, dtype=torch.float32, device="cuda")
    dummy_i32 = torch.empty(1, dtype=torch.int32, device="cuda")
    manifest = {
        "torch": torch.__version__,
        "triton": importlib.metadata.version("triton"),
        "ptxas": version.strip(),
        "ptx_version": "9.0",
        "device_capability": [12, 1],
        "kernels": {},
    }

    def write(kernel, filename, argument_types):
        metadata = kernel.metadata
        assert metadata.global_scratch_size == metadata.profile_scratch_size == 0
        text = kernel.asm["ptx"].rstrip("\n") + "\n"
        assert ".version 9.0" in text and ".target sm_121a" in text
        entry = re.search(r"\.visible \.entry \w+\((.*?)\)\s*", text, re.S)
        assert entry is not None
        params = re.findall(r"\.param \.(u\d+)", entry[1])
        assert params == argument_types, (params, argument_types)
        (output / filename).write_text(text)
        manifest["kernels"][filename] = {
            "sha256": hashlib.sha256(text.encode()).hexdigest(),
            "entrypoint": metadata.name,
            "shared_bytes": metadata.shared,
            "threads": metadata.num_warps * 32,
            "num_ctas": metadata.num_ctas,
            "arguments": params,
            "global_scratch_bytes": metadata.global_scratch_size,
            "profile_scratch_bytes": metadata.profile_scratch_size,
        }

    for label, hidden, tile_rows in [("08", 1024, 64), ("4b", 2560, 32)]:
        # warmup compiles without launching or reading the one-element dummies.
        compiled = lighton_head.warmup(
            dummy_bf16,
            dummy_bf16,
            dummy_f32,
            dummy_f32,
            dummy_i32,
            32,
            248320,
            hidden,
            tile_rows,
            128,
            64,
            False,
            (),
            grid=(1, 1940),
            num_warps=4,
            num_stages=2,
            ptx_version=90,
        )
        write(
            compiled, f"lighton_head_{label}.ptx", ["u64"] * 5 + ["u32"] + ["u64"] * 2
        )
    compiled = lighton_head_finish.warmup(
        dummy_f32,
        dummy_i32,
        dummy_i32,
        1940,
        2048,
        grid=(1,),
        num_warps=4,
        ptx_version=90,
    )
    write(compiled, "lighton_head_finish.ptx", ["u64"] * 5)
    (output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    print(json.dumps(manifest, indent=2))


if __name__ == "__main__":
    main()
