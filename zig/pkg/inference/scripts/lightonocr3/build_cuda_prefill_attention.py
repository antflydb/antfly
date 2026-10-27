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

"""Build optional SM12.1 paged prefill attention beside a recurrent library.

Python is a build dependency. The native library uses F16 Q/KV operands and
F32 output; recurrent state and decode retain their existing precision.
Requires FlashInfer 0.7.0.post1, CUDA 13, TVM FFI and the existing native
recurrent-prefill library. Header changes apply only to an output-local copy.
"""

import argparse
import ctypes
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys

HEADER_HASHES = {
    "flashinfer/attention/cascade.cuh": "66df7beae6efd81198cdd6be5d301e06022b41898ed822b1d314c76b5cb137c6",
    "flashinfer/utils.cuh": "d14edcb4d39503603b3347bdb20fdd6b96ff3467b97ba3f539af5f8fdd86adec",
}


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--recurrent-library", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--nvcc", default="/usr/local/cuda/bin/nvcc")
    parser.add_argument("--cxx", default="g++")
    parser.add_argument(
        "--source",
        type=Path,
        default=Path(__file__).resolve().parents[2]
        / "src/ops/cuda/prototypes/lighton_prefill_attention.cpp",
    )
    args = parser.parse_args()
    out = args.output_dir.resolve()
    out.mkdir(parents=True, exist_ok=True)
    os.environ["FLASHINFER_WORKSPACE_BASE"] = str(out / "jit")
    os.environ["FLASHINFER_CUDA_ARCH_LIST"] = "12.1"
    os.environ.setdefault("MAX_JOBS", "4")
    os.environ["PATH"] = os.pathsep.join(
        [
            str(Path(sys.executable).parent),
            str(Path(args.nvcc).parent),
            os.environ.get("PATH", ""),
        ]
    )
    if importlib.metadata.version("flashinfer-python") != "0.7.0.post1":
        raise RuntimeError("This specialization requires pinned FlashInfer 0.7.0.post1")
    import torch
    import tvm_ffi
    from flashinfer.jit import env as jit_env
    from flashinfer.jit import utils as jit_utils
    from flashinfer.jit.attention.modules import _gen_batch_prefill_primary_module

    if torch.cuda.get_device_capability() != (12, 1):
        raise RuntimeError("This optional specialization supports SM12.1 only")
    original = jit_env.FLASHINFER_INCLUDE_DIR
    private = out / "include"
    for name, expected in HEADER_HASHES.items():
        if digest(original / name) != expected:
            raise RuntimeError("Pinned FlashInfer header changed: " + name)
    shutil.copytree(original, private, dirs_exist_ok=True)
    cascade = private / "flashinfer/attention/cascade.cuh"
    text = cascade.read_text()
    start = text.index("cudaError_t VariableLengthMergeStates(")
    needle = (
        "constexpr uint32_t vec_size = std::max(16U / sizeof(DTypeIn), HEAD_DIM / 32U);"
    )
    position = text.index(needle, start)
    text = text[:position] + text[position:].replace(
        needle,
        "constexpr uint32_t vec_size = std::min(32U / sizeof(DTypeIn), std::max(16U / sizeof(DTypeIn), HEAD_DIM / 32U));",
        1,
    )
    cascade.write_text(text)
    # The HD256 kernel generator instantiates 16/64/128. Remove the generic
    # unreachable 32 branch so all native AOT symbols resolve eagerly. The
    # default still throws; the bridge also validates the plan's tile size.
    utils = private / "flashinfer/utils.cuh"
    text = utils.read_text()
    start = text.index("#define DISPATCH_CTA_TILE_Q(")
    stop = text.index("#define DISPATCH_GQA_GROUP_SIZE", start)
    macro = text[start:stop]
    first = macro.index("    case 32: {")
    last = macro.index("    case 16: {", first)
    utils.write_text(text[:start] + macro[:first] + macro[last:] + text[stop:])
    jit_env.FLASHINFER_INCLUDE_DIR = private
    jit_utils.dtype_map[torch.float32] = "float"
    jit_utils.filename_safe_dtype_map[torch.float32] = "f32"
    spec = _gen_batch_prefill_primary_module(
        "fa2",
        torch.float16,
        torch.float16,
        torch.float32,
        torch.int32,
        256,
        256,
        0,
        False,
        False,
        False,
    )
    spec.build()
    kernels = Path(spec.get_library_path())
    recurrent = out / "libantfly_lighton_recurrent.so"
    native_kernels = out / "libantfly_lighton_attention_kernels.so"
    if args.recurrent_library.resolve() != recurrent:
        shutil.copy2(args.recurrent_library, recurrent)
    shutil.copy2(kernels, native_kernels)
    ffi = Path(tvm_ffi.__file__).resolve().parent
    library = out / "libantfly_lighton_prefill.so"
    command = [
        args.cxx,
        "-std=c++17",
        "-O3",
        "-fPIC",
        "-shared",
        str(args.source.resolve()),
        "-L" + str(out),
        "-Wl,--no-as-needed",
        "-l:libantfly_lighton_attention_kernels.so",
        "-l:libantfly_lighton_recurrent.so",
        "-Wl,--as-needed",
        "-I" + str(ffi / "include"),
        "-L" + str(ffi / "lib"),
        "-ltvm_ffi",
        "-Wl,-z,defs",
        "-Wl,-rpath,$ORIGIN",
        "-Wl,-rpath," + str(ffi / "lib"),
        "-o",
        str(library),
    ]
    with (out / "bridge-build.log").open("w") as log:
        subprocess.run(command, check=True, stdout=log, stderr=subprocess.STDOUT)
    dependencies = subprocess.check_output(["ldd", str(library)], text=True)
    (out / "dependencies.txt").write_text(dependencies)
    if any(name in dependencies for name in ("libtorch", "libpython", "not found")):
        raise RuntimeError("Unexpected or unresolved native library dependency")
    loaded = ctypes.CDLL(str(library))
    for name, expected in (
        ("antfly_lighton_prefill_abi", (1 << 32) | 2408),
        ("antfly_attention_abi", (1 << 32) | (104 << 16) | 88),
    ):
        function = getattr(loaded, name)
        function.restype = ctypes.c_uint64
        if function() != expected:
            raise RuntimeError("Native library ABI mismatch: " + name)
    manifest = {
        "architecture": "sm_121",
        "recurrent_library_sha256": digest(recurrent),
        "library_sha256": digest(library),
        "kernel_library_sha256": digest(native_kernels),
        "bridge_sha256": digest(args.source),
        "bridge_command": command,
        "headers": {
            name: {"original_sha256": value, "patched_sha256": digest(private / name)}
            for name, value in HEADER_HASHES.items()
        },
        "generated_sources": {str(p): digest(p) for p in spec.sources},
        "packages": {
            name: importlib.metadata.version(name)
            for name in ("torch", "flashinfer-python", "apache-tvm-ffi")
        },
        "precision": "F16 prefill Q/KV; F32 prefill output. Recurrent/decode precision inherited from the selected recurrent library.",
        "runtime": "Keep all three output libraries together. CUDA, CUTLASS DSL runtime and TVM FFI shared libraries required; no Python/Torch runtime.",
        "qualification": "Build and ABI checks only; run independent kernel checks and full OCR output/throughput qualification before selecting.",
    }
    (out / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    print(library)


if __name__ == "__main__":
    main()
