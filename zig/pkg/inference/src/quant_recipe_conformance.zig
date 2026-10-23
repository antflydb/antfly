// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Emit CUDA recipe expressions plus independent scalar fixtures for a host C++
//! compiler. This checks generated syntax/arithmetic; GPU qualification is separate.
const std = @import("std");
const recipe = @import("graph/quant_recipe.zig");
const reference = @import("gguf/testing/quant_reference.zig");
pub fn main(init: std.process.Init) !void {
    var args = std.process.Args.Iterator.init(init.minimal.args);
    _ = args.next();
    const output = args.next() orelse return error.MissingOutputPath;
    var out: std.Io.Writer.Allocating = .init(init.gpa);
    defer out.deinit();
    const w = &out.writer;
    try w.writeAll(
        \\#include <stdint.h>
        \\#include <stdio.h>
        \\#include <string.h>
        \\#include <math.h>
        \\#define __device__
        \\#define __forceinline__ inline
        \\static _Float16 __ushort_as_half(uint16_t bits) { _Float16 value; memcpy(&value, &bits, 2); return value; }
        \\static float __half2float(_Float16 value) { return float(value); }
        \\static float __uint_as_float(uint32_t bits) { float value; memcpy(&value, &bits, 4); return value; }
        \\
    );
    try w.print("{s}\n{s}\n{s}\n", .{ recipe.cuda_half_source, recipe.cuda_word_source, recipe.cuda_float_source });
    inline for (recipe.formats) |format| {
        const source = try recipe.render(init.gpa, comptime recipe.forFormat(format), .cuda, "decode_" ++ @tagName(format));
        defer init.gpa.free(source);
        try w.print("{s}\n", .{source});
    }
    try w.writeAll("int main() { unsigned checked = 0;\n");
    inline for (recipe.formats) |format| {
        const r = comptime recipe.forFormat(format);
        for (0..64) |trial| {
            var raw: [r.blockBytes()]u8 = undefined;
            var expected: [r.blockValues()]f32 = undefined;
            @import("graph/testing/quant_blocks.zig").fill(format, trial, &raw);
            try reference.decode(format, &raw, &expected);
            try w.writeAll("{ const unsigned char raw[] = {");
            for (raw) |byte| try w.print("{d},", .{byte});
            try w.writeAll("}; const uint32_t expected[] = {");
            for (expected) |value| try w.print("0x{x}u,", .{@as(u32, @bitCast(value))});
            try w.print("}}; for (int lane = 0; lane < {d}; ++lane) {{ float want = __uint_as_float(expected[lane]); float got = decode_{s}(raw, lane); if (want != got) {{ fprintf(stderr, \"{s} trial {d} lane %d: %.9g != %.9g\\n\", lane, double(got), double(want)); return 1; }} ++checked; }} }}\n", .{ r.blockValues(), @tagName(format), @tagName(format), trial });
        }
    }
    try w.writeAll("printf(\"CUDA recipe host conformance: %u values passed\\n\", checked); return 0; }\n");
    try std.Io.Dir.cwd().writeFile(init.io, .{ .sub_path = output, .data = out.written() });
}
