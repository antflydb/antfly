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

"""Build the optional SM12.1 BF16 recurrent prefill library.

Requires the pinned FlashInfer SM120 source, CUTLASS DSL, TVM FFI, PyTorch,
and CUDA 13 on the target GPU. Python is used at build time only. The native
runtime loads the resulting shared library and supplies all device scratch.
"""

import argparse
import hashlib
import importlib.metadata
import importlib.util
import sys
import json
import subprocess
from pathlib import Path

SOURCE_SHA256 = "94fad8696bed061c577603f82670f038f55497231a6775717f8bef65facddb51"


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument(
        "--source",
        type=Path,
        default=Path(__file__).resolve().parents[2]
        / "src/ops/cuda/prototypes/lighton_bf16_prefill.cu",
    )
    parser.add_argument("--nvcc", default="/usr/local/cuda/bin/nvcc")
    args = parser.parse_args()
    import torch
    import tvm_ffi
    import flashinfer.gdn_kernels.delta_rule_dsl.delta_rule_sm120 as sm

    if torch.cuda.get_device_capability() != (12, 1):
        raise RuntimeError("This build is qualified only for SM12.1")
    if digest(sm.__file__) != SOURCE_SHA256:
        raise RuntimeError(
            "FlashInfer SM120 source does not match the qualified revision"
        )
    out = args.output_dir.resolve()
    out.mkdir(parents=True, exist_ok=True)
    # Interpret the index tensor as a table of native per-page pointers.
    # The F32 arithmetic and K,V layout remain unchanged; all slot addressing
    # uses pointers validated by the native caller before state mutation.
    patched_source = out / "delta_rule_sm120_pointers.py"
    original = Path(sm.__file__).read_text()
    replacements = {
        "state_idx = cutlass.Int32(g_state_indices[work_desc.seq_idx])": "state_idx = cutlass.Int32(0)",
        "        mState = cute.make_tensor(g_state.iterator, state_layout)": """        state_iterator = g_state.iterator
        if cutlass.const_expr(self.use_state_indices):
            state_iterator = cute.make_ptr(
                cutlass.Float32, cutlass.Int64(g_state_indices[work_desc.seq_idx]),
                cute.AddressSpace.gmem, assumed_align=16)
        mState = cute.make_tensor(state_iterator, state_layout)""",
        "            mInitState = cute.make_tensor(g_init_state.iterator, init_state_layout)": """            initial_iterator = g_init_state.iterator
            if cutlass.const_expr(self.use_state_indices):
                initial_iterator = state_iterator
            mInitState = cute.make_tensor(initial_iterator, init_state_layout)""",
    }
    patched = original
    for old, new in replacements.items():
        if patched.count(old) != 1:
            raise RuntimeError("Pinned state-pointer patch no longer applies: " + old)
        patched = patched.replace(old, new)
    patched_source.write_text(patched)
    name = (
        "flashinfer.gdn_kernels.delta_rule_dsl.delta_rule_sm120_antfly_"
        + digest(patched_source)[:16]
    )
    spec = importlib.util.spec_from_file_location(name, patched_source)
    native = importlib.util.module_from_spec(spec)
    sys.modules[name] = native
    spec.loader.exec_module(native)
    sm = native
    obj = out / "lighton_prefill.o"
    compile_original = sm.cached_compile
    exports = []

    def export_compile(func, *a, compile_options=None, **kw):
        fn = compile_original(func, *a, compile_options=compile_options, **kw)
        fn.export_to_c(str(obj), function_name="antfly_lighton_prefill")
        exports.append(digest(obj))
        return fn

    sm.cached_compile = export_compile
    torch.manual_seed(126)
    try:
        for heads in (16, 32):
            rows = 320
            q = torch.nn.functional.normalize(
                torch.randn(rows, 16, 128, device="cuda"), dim=-1
            ).bfloat16()
            k = torch.nn.functional.normalize(
                torch.randn_like(q.float()), dim=-1
            ).bfloat16()
            v = (torch.randn(rows, heads, 128, device="cuda") * 0.1).bfloat16()
            alpha = torch.exp(-torch.rand(rows, heads, device="cuda") * 0.1)
            beta = torch.sigmoid(torch.randn(rows, heads, device="cuda"))
            output = torch.empty_like(v)
            # Native state has K,V storage, viewed as V,K by the CuTe core.
            # Initial and final F32 state alias; selected page pointers carry
            # sequence ownership rather than an artificial contiguous pool.
            state = (torch.randn(2, heads, 128, 128, device="cuda") * 0.01).transpose(
                -1, -2
            )
            initial = state
            indices = torch.tensor(
                [state[i].data_ptr() for i in range(2)],
                dtype=torch.int64,
                device="cuda",
            )
            cu = torch.tensor([0, 64, rows], dtype=torch.int64, device="cuda")
            sm.delta_rule_prefill_dsl(
                output,
                state,
                q,
                k,
                v,
                initial,
                alpha,
                beta,
                cu,
                1.0,
                state_indices=indices,
            )
            torch.cuda.synchronize()
            if not torch.isfinite(output).all() or not torch.isfinite(state).all():
                raise RuntimeError("Export smoke test produced nonfinite values")
    finally:
        sm.cached_compile = compile_original
    if not exports or len(set(exports)) != 1:
        raise RuntimeError("Expected one shared dynamic-head specialization")
    ffi = Path(tvm_ffi.__file__).resolve().parent
    site = ffi.parent
    dsl = site / "nvidia_cutlass_dsl/cu13/lib"
    library = out / "libantfly_lighton_prefill.so"
    command = [
        args.nvcc,
        "-shared",
        "-Xcompiler=-fPIC",
        "-arch=sm_121",
        "-O3",
        "--fmad=false",
        "-std=c++17",
        str(args.source.resolve()),
        str(obj),
        "-I" + str(ffi / "include"),
        "-I" + str(ffi / "3rdparty/dlpack/include"),
        "-L" + str(ffi / "lib"),
        "-ltvm_ffi",
        "-L" + str(dsl),
        "-lcute_dsl_runtime",
        "-Xlinker=-rpath=" + str(ffi / "lib"),
        "-Xlinker=-rpath=" + str(dsl),
        "-o",
        str(library),
    ]
    with (out / "build.log").open("w") as log:
        subprocess.run(command, check=True, stdout=log, stderr=subprocess.STDOUT)
    manifest = {
        "abi_version": 1,
        "packet_bytes": 2408,
        "architecture": "sm_121",
        "flashinfer_source_sha256": SOURCE_SHA256,
        "patched_flashinfer_source_sha256": digest(patched_source),
        "state_index_patch": "Native page-pointer table, in-place F32 state; state arithmetic unchanged",
        "shim_sha256": digest(args.source),
        "object_sha256": digest(obj),
        "library_sha256": digest(library),
        "build_command": command,
        "compiler": subprocess.check_output([args.nvcc, "--version"], text=True),
        "packages": {
            name: importlib.metadata.version(name)
            for name in (
                "torch",
                "flashinfer-python",
                "apache-tvm-ffi",
                "nvidia-cutlass-dsl",
            )
        },
        "runtime": "No Python runtime; CUDA runtime, CUTLASS DSL runtime and TVM FFI shared libraries required",
        "precision": "BF16 recurrent prefill operands/output; F32 convolution, normalization, recurrent state and decode",
    }
    (out / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    print(library)


if __name__ == "__main__":
    main()
