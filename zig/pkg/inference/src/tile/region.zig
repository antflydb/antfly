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

/// Encode shader lines once at the source boundary. Native owners keep readable
/// MSL; the checked-in Objective-C translation unit retains its string ABI.
pub fn metalLines(comptime indent: []const u8, comptime src: []const u8) []const u8 {
    comptime {
        @setEvalBranchQuota(100_000_000);
        const encoding = @import("source_boundary.zig");
        const size = encoding.metalEncodedSize(src, indent, .{});
        var buffer: [size]u8 = undefined;
        encoding.encodeMetal(&buffer, src, indent, .{});
        const result = buffer;
        return &result;
    }
}

const std = @import("std");

/// A native owner exports source_0, source_1, ... in lexical program order.
/// One region has the family name; multiple regions retain numbered suffixes.
fn sourceCount(comptime source: type) usize {
    comptime {
        var count: usize = 0;
        for (std.meta.declarations(source)) |decl| {
            if (std.mem.startsWith(u8, decl, "source_")) count += 1;
        }
        if (count == 0) @compileError("native family has no source regions");
        for (0..count) |i| {
            if (!@hasDecl(source, std.fmt.comptimePrint("source_{d}", .{i})))
                @compileError("native family source indices must be contiguous from zero");
        }
        return count;
    }
}

/// Compile-time ownership metadata, separate from emitted device programs.
pub const Family = struct {
    path: []const u8,
    indent: []const u8,
    name: []const u8,
    source: type,
};

pub fn nativeFamily(comptime path: []const u8, comptime indent: []const u8, comptime name: []const u8, comptime source: type) Family {
    return .{ .path = path, .indent = indent, .name = name, .source = source };
}

pub fn metalFamily(comptime name: []const u8, comptime source: type) Family {
    return nativeFamily(metal_kernels_m, metal_indent, name, source);
}

pub fn cudaFamily(comptime name: []const u8, comptime source: type) Family {
    return nativeFamily(cuda_bundle, "", name, source);
}

fn regionCount(comptime families: []const Family) usize {
    comptime {
        @setEvalBranchQuota(1_000_000);
        var count: usize = 0;
        for (families) |item| count += sourceCount(item.source);
        return count;
    }
}

/// Flatten family descriptions once; do not repeatedly concatenate arrays.
pub fn regions(comptime families: []const Family) [regionCount(families)]Region {
    comptime {
        @setEvalBranchQuota(1_000_000);
        var result: [regionCount(families)]Region = undefined;
        var cursor: usize = 0;
        for (families) |item| {
            const count = sourceCount(item.source);
            for (0..count) |i| {
                const key = if (count == 1) item.name else std.fmt.comptimePrint("{s}.{d}", .{ item.name, i });
                result[cursor] = region(item.path, item.indent, key, @field(item.source, std.fmt.comptimePrint("source_{d}", .{i})));
                cursor += 1;
            }
        }
        return result;
    }
}

test "Metal source encoding preserves quotes backslashes blanks and line boundaries" {
    const text = "kernel void k() {\n  // \"quoted\" \\n\n}\n";
    try std.testing.expectEqualStrings("  \"kernel void k() {\\n\"\n  \"  // \\\"quoted\\\" \\\\n\\n\"\n  \"}\\n\"\n", comptime metalLines("  ", text));
    try std.testing.expectEqualStrings("", comptime metalLines("", "unterminated"));
    try std.testing.expectEqualStrings("\"a\\n\"\n", comptime metalLines("", "a\nunterminated"));
}

test "native family descriptors retain numbered keys and custom source paths" {
    const sources = struct {
        pub const source_0 = "first";
        pub const source_1 = "second";
    };
    const entries = comptime regions(&.{nativeFamily("custom.cuh", "", "row_program", sources)});
    try std.testing.expectEqual(@as(usize, 2), entries.len);
    try std.testing.expectEqualStrings("custom.cuh", entries[0].path);
    try std.testing.expectEqualStrings("first", entries[0].data);
    try std.testing.expectEqualStrings("second", entries[1].data);
    try std.testing.expect(std.mem.indexOf(u8, entries[1].begin, "row_program.1 family") != null);
    const single = comptime regions(&.{metalFamily("single", struct {
        pub const source_0 = "kernel";
    })});
    try std.testing.expectEqualStrings(metal_indent, single[0].begin[0..metal_indent.len]);
    try std.testing.expect(std.mem.indexOf(u8, single[0].begin, "single family") != null);
}
