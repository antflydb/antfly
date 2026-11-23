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

//! Native sources are split before backend compilation, never at serving time.
const std = @import("std");
const boundary = @import("region.zig");
pub const Encoding = enum { cuda, metal, encoded_metal };
const marker = "// native-program:entry ";
const indent_marker = "// native-program:indent ";

fn count(comptime source: []const u8) usize {
    @setEvalBranchQuota(100_000_000);
    const n = std.mem.count(u8, source, marker);
    if (n == 0) @compileError("native source has no export descriptions");
    return n;
}

pub fn sources(comptime source: []const u8, comptime encoding: Encoding) [count(source)][]const u8 {
    comptime {
        @setEvalBranchQuota(100_000_000);
        var result: [count(source)][]const u8 = undefined;
        var names: [count(source)][]const u8 = undefined;
        var cursor: usize = 0;
        for (0..result.len) |i| {
            const begin = std.mem.indexOfPos(u8, source, cursor, marker).?;
            if (begin != 0 and source[begin - 1] != '\n')
                @compileError("native export marker must begin a line");
            const name_start = begin + marker.len;
            const name_end = std.mem.indexOfScalarPos(u8, source, name_start, '\n') orelse
                @compileError("native export description must end with a newline");
            const name = source[name_start..name_end];
            if (name.len == 0) @compileError("native export name is empty");
            for (name) |c| if (!std.ascii.isAlphanumeric(c) and c != '_')
                @compileError("invalid native export name");
            for (names[0..i]) |previous| if (std.mem.eql(u8, previous, name))
                @compileError("duplicate native export description");
            names[i] = name;
            var body_start = name_end + 1;
            var indent: []const u8 = boundary.metal_indent;
            if (std.mem.startsWith(u8, source[body_start..], indent_marker)) {
                if (encoding != .metal) @compileError("indent metadata requires readable Metal source");
                const end = std.mem.indexOfScalarPos(u8, source, body_start, '\n').?;
                const n = std.fmt.parseInt(u8, source[body_start + indent_marker.len .. end], 10) catch
                    @compileError("invalid native Metal indentation");
                const spaces: [n]u8 = @splat(' ');
                indent = &spaces;
                body_start = end + 1;
            }
            const end = std.mem.indexOfPos(u8, source, body_start, marker) orelse source.len;
            const body = source[body_start..end];
            if (std.mem.indexOf(u8, body, name ++ "(") == null and
                !(encoding == .encoded_metal and std.mem.indexOf(u8, body, name ++ ")") != null))
                @compileError("native export description does not match its body: " ++ name);
            if (body.len == 0 or body[body.len - 1] != '\n')
                @compileError("native export body must end with a newline");
            result[i] = if (encoding == .metal) boundary.metalLines(indent, body) else body;
            cursor = end;
        }
        return result;
    }
}

test "native source descriptions preserve body bytes order and Metal boundaries" {
    const text = "// license\n// native-program:entry first\nextern void first() {}\n" ++
        "// native-program:entry second\nextern void second() {\n}\n";
    const cuda = comptime sources(text, .cuda);
    try std.testing.expectEqual(@as(usize, 2), cuda.len);
    try std.testing.expectEqualStrings("extern void first() {}\n", cuda[0]);
    try std.testing.expectEqualStrings("extern void second() {\n}\n", cuda[1]);
    const metal = comptime sources("// native-program:entry k\n// native-program:indent 2\nkernel void k() {}\n", .metal);
    try std.testing.expectEqualStrings("  \"kernel void k() {}\\n\"\n", metal[0]);
}

/// These ABI shells surround regions already owned by the quant generator.
/// They cannot acquire a second owner. All other direct exports must be in a
/// generated family; adding an exception requires changing this explicit policy.
const quant_shells = [_][]const u8{
    "termite_gliner_boundary_f32",
    "dt_validate_f32",
    "dt_validate_control",
    "dt_zero",
    "dt_forward",
    "dt_rows",
    "dt_dq",
    "dt_dkdv",
    "dt_relative",
    "termite_linear_q4_0_pair_activation_q8_1_q8_1_tile32_w5_e4b_ffn",
    "termite_linear_q4_0_pair_activation_q8_1_q8_1_tile32_w5_e4b_ffn_rows2",
    "termite_linear_q4_0_pair_activation_q8_1_q8_1_tile32_w5_e4b_ffn_rows4",
    "termite_linear_q4_0_pair_activation_q8_1_q8_1_tile32_w5_e4b_ffn_rows8_c2",
    "termite_linear_q4_0_pair_activation_q8_1_q8_1_tile32_w5_e4b_ffn_rows16_c1",
};

pub fn audit(contents: []const u8) !void {
    var depth: usize = 0;
    var lines = std.mem.splitScalar(u8, contents, '\n');
    while (lines.next()) |raw| {
        const line = std.mem.trimStart(u8, raw, " \t");
        if (std.mem.indexOf(u8, line, "-codegen:begin") != null) depth += 1;
        if (std.mem.indexOf(u8, line, "-codegen:end") != null) {
            if (depth == 0) return error.UnbalancedNativeOwnership;
            depth -= 1;
        }
        if (depth != 0) continue;
        var candidate = std.mem.startsWith(u8, line, "extern \"C\" __global__") or
            std.mem.startsWith(u8, line, "TERMITE_MSL_ABI_");
        const prefix = "\"kernel void ";
        if (std.mem.startsWith(u8, line, prefix)) {
            var end = prefix.len;
            while (end < line.len and (std.ascii.isAlphanumeric(line[end]) or line[end] == '_')) : (end += 1) {}
            candidate = end < line.len and line[end] == '(';
        }
        if (!candidate) continue;
        var compatibility = false;
        if (std.mem.indexOfScalar(u8, line, '(')) |open| {
            var begin = open;
            while (begin > 0 and (std.ascii.isAlphanumeric(line[begin - 1]) or line[begin - 1] == '_')) : (begin -= 1) {}
            inline for (quant_shells) |name| {
                if (std.mem.eql(u8, line[begin..open], name)) compatibility = true;
            }
        }
        if (!compatibility) {
            std.debug.print("native kernel requires generator ownership: {s}\n", .{line});
            return error.NativeKernelOutsideGenerator;
        }
    }
    if (depth != 0) return error.UnbalancedNativeOwnership;
}

test "native ownership rejects new direct and ABI exports but permits generated programs" {
    const cuda = "extern \"C\" __global__ void extra() {}\n";
    const metal = "\"kernel void extra() {}\\n\"\n";
    const abi = "TERMITE_MSL_ABI_TEST(extra)\n";
    try std.testing.expectError(error.NativeKernelOutsideGenerator, audit(cuda));
    try std.testing.expectError(error.NativeKernelOutsideGenerator, audit("extern \"C\" __global__ void extra() { dt_zero(); }\n"));
    try std.testing.expectError(error.NativeKernelOutsideGenerator, audit(metal));
    try std.testing.expectError(error.NativeKernelOutsideGenerator, audit(abi));
    try audit("// tile-codegen:begin f\n" ++ cuda ++ metal ++ abi ++ "// tile-codegen:end f\n");
    try audit("#define ABI \"kernel void \" #Name\n");
    try std.testing.expectError(error.UnbalancedNativeOwnership, audit("// tile-codegen:end f\n"));
}
