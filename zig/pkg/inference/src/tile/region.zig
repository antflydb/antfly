// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
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

//! A generated region inside a hand-maintained source file: the text
//! between the begin and end marker lines is owned by the tile generator.

pub const Region = struct { path: []const u8, begin: []const u8, end: []const u8, data: []const u8 };

pub const metal_kernels_m = "src/backends/metal_kernels.m";
pub const cuda_bundle = "src/ops/cuda/artifacts/inference_cuda_kernels.cu";
/// Indent of the C-string lines inside metal_kernels.m.
pub const metal_indent = "           ";

pub fn region(comptime path: []const u8, comptime indent: []const u8, comptime family: []const u8, comptime data: []const u8) Region {
    return .{
        .path = path,
        .begin = indent ++ "// tile-codegen:begin " ++ family ++ " family (do not edit; run: zig build tile-kernel-codegen -- --write)\n",
        .end = indent ++ "// tile-codegen:end " ++ family ++ " family\n",
        .data = data,
    };
}

/// A Metal region in metal_kernels.m.
pub fn metal(comptime family: []const u8, comptime data: []const u8) Region {
    return region(metal_kernels_m, metal_indent, family, data);
}

/// A CUDA region in inference_cuda_kernels.cu.
pub fn cuda(comptime family: []const u8, comptime data: []const u8) Region {
    return region(cuda_bundle, "", family, data);
}

/// Renders MSL source as the C-string lines metal_kernels.m embeds
/// (`<indent>"...\\n"`), escaping quotes and backslashes.
pub fn metalLines(comptime indent: []const u8, comptime src: []const u8) []const u8 {
    comptime {
        @setEvalBranchQuota(100_000_000);
        var out: []const u8 = "";
        var start: usize = 0;
        for (src, 0..) |ch, i| {
            if (ch != '\n') continue;
            out = out ++ indent ++ "\"" ++ escape(src[start..i]) ++ "\\n\"\n";
            start = i + 1;
        }
        return out;
    }
}

fn escape(comptime s: []const u8) []const u8 {
    var out: []const u8 = "";
    for (s) |ch| out = out ++ switch (ch) {
        '\\' => "\\\\",
        '"' => "\\\"",
        else => &[_]u8{ch},
    };
    return out;
}
