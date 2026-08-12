// Copyright 2026 Antfly, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

const std = @import("std");
const build_options = @import("build_options");

pub const mode = build_options.cuda_artifacts;
pub const is_sm89 = std.mem.eql(u8, mode, "sm89");
pub const is_fatbin = std.mem.eql(u8, mode, "fatbin");
pub const format = if (is_sm89) "cubin" else if (is_fatbin) "fatbin" else "ptx";
pub const target = if (is_sm89) "sm_89" else if (is_fatbin) "fatbin" else "portable";
pub const uses_jit = !is_sm89 and !is_fatbin;

/// Whether the selected device can load a bundled SASS image containing the
/// BF16 WMMA kernels. Portable PTX targets compute_75, where those entry bodies
/// are intentionally empty. The fatbin currently carries SASS for every major
/// architecture from SM80 through SM120; a later major would fall back to the
/// same compute_75 PTX and must therefore use the F16 kernel instead.
pub fn hasBf16WmmaCodeFor(compute_major: i32, compute_minor: i32) bool {
    if (is_sm89) return compute_major == 8 and compute_minor == 9;
    if (is_fatbin) return compute_major >= 8 and compute_major <= 12;
    return false;
}

const artifact_bytes = if (is_sm89)
    @embedFile("artifacts/inference_cuda_kernels_sm89.cubin")
else if (is_fatbin)
    @embedFile("artifacts/inference_cuda_kernels.fatbin")
else
    @embedFile("artifacts/inference_cuda_kernels.ptx");

pub const image = artifact_bytes ++ if (uses_jit) "\x00" else "";
