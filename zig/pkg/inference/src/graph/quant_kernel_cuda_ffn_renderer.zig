// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! AOT source schedules for the Q4 E4B gated FFN. Geometry selects native
//! acquisition order and register spelling; no runtime abstraction is emitted.
const std = @import("std");
const Emitter = @import("quant_kernel_native_source.zig").Emitter;
const projections = [_][]const u8{ "gate", "up" };
const words = [_][]const u8{ "low0", "high0", "low1", "high1" };
pub const Geometry = enum(u8) {
    single = 1,
    pair = 2,
    columns_first = 4,
    rows_first = 8,
    packed_weights_first = 16,
    fn rows(comptime g: Geometry) usize {
        return @backingInt(g);
    }
    fn cols(comptime g: Geometry) usize {
        return switch (g) {
            .rows_first => 2,
            .packed_weights_first => 1,
            else => 4,
        };
    }
};
const bases = "unsigned int q8_base0 = iqs * 4u;\nunsigned int q8_base1 = q8_base0 + 4u;\n";
const row_loop = "#pragma unroll\nfor (unsigned int r = 0u; r < rows_per_block; ++r) {\n";
const column_loop = "#pragma unroll\nfor (unsigned int c = 0u; c < group_cols; ++c) {\n";
const row_guard = "unsigned int row = row_base + r;\nif (row < rows) {\n";

// Scalar pair state deliberately uses separate names, not a two-row array.
fn q8Acquire(e: *Emitter, comptime g: Geometry, second: bool) !void {
    const pair = g == .pair;
    const suffix: []const u8 = if (pair) (if (second) "1" else "0") else "";
    const row: []const u8 = if (pair) (if (second) "row1" else "row0") else "row";
    const scale_type: []const u8 = if (second or g == .columns_first) "" else "float ";
    const index: []const u8 = if (g == .columns_first) "[r]" else "";
    try e.add("const unsigned char* q8_bp{s} = q8_input + ({s} * row_blocks + block) * 36u;\nunsigned short q8_d{s}_h = (unsigned short)q8_bp{s}[0] | ((unsigned short)q8_bp{s}[1] << 8);\n{s}q8_d{s}{s} = termite_half_to_float(q8_d{s}_h);\nconst signed char* q8_values{s} = (const signed char*)(q8_bp{s} + 4u);\n", .{ suffix, row, suffix, suffix, suffix, scale_type, suffix, index, suffix, suffix, suffix });
    if (g == .single or (pair and !second)) try e.raw(bases);
    for (words, 0..) |word, i| try e.add("{s}q8_{s}{s}{s}{s} = termite_load_i8x4_aligned(q8_values{s} + q8_base{d}{s});\n", .{
        if (second or g == .columns_first) "" else "int ", word,  if (pair) "_" else "",            suffix, index,
        suffix,                                            i / 2, if (i % 2 == 1) " + 16u" else "",
    });
}
fn weights(e: *Emitter) !void {
    for (projections) |p| try e.add("const unsigned char* {s}_bp = weight_{s} + (col * row_blocks + block) * 18u;\n", .{ p, p });
}
fn partialDot(e: *Emitter, acc: []const u8, q: []const u8, word_suffix: []const u8, array: []const u8) !void {
    for (projections) |p| try e.add("{s}_acc{s} += termite_q4_0_q8_1_partial_mmvq2({s}_bp, q8_d{s}{s}, iqs, q8_low0{s}{s}, q8_high0{s}{s}, q8_low1{s}{s}, q8_high1{s}{s});\n", .{ p, acc, p, q, array, word_suffix, array, word_suffix, array, word_suffix, array, word_suffix, array });
}
fn accumulation(e: *Emitter, comptime g: Geometry) !void {
    try e.raw("unsigned int iqs = (local_tid & 1u) * 2u;\n");
    if (g == .rows_first or g == .packed_weights_first) try e.raw(bases);
    try e.add("for (unsigned int block = local_tid >> 1u; block < row_blocks; block += {d}u) {{\n", .{if (g == .packed_weights_first) @as(usize, 64) else 80});
    switch (g) {
        .single, .pair => {
            try q8Acquire(e, g, false);
            if (g == .pair) {
                try e.raw("float q8_d1 = 0.0f;\n");
                for (words) |w| try e.add("int q8_{s}_1 = 0;\n", .{w});
                try e.raw("if (has_row1) {\n");
                try q8Acquire(e, g, true);
                try e.raw("}\n");
            }
            try e.raw(column_loop);
            try e.raw("unsigned int col = col_tile + c;\n");
            try weights(e);
            try partialDot(e, if (g == .pair) "0[c]" else "[c]", if (g == .pair) "0" else "", if (g == .pair) "_0" else "", "");
            if (g == .pair) {
                try e.raw("if (has_row1) {\n");
                try partialDot(e, "1[c]", "1", "_1", "");
                try e.raw("}\n");
            }
            try e.raw("}\n");
        },
        .columns_first => {
            try e.raw("float q8_d[4];\n");
            for (words) |w| try e.add("int q8_{s}[4];\n", .{w});
            try e.raw(bases);
            try e.raw(row_loop);
            try e.raw(row_guard);
            try q8Acquire(e, g, false);
            try e.raw("} else {\nq8_d[r] = 0.0f;\n");
            for (words) |w| try e.add("q8_{s}[r] = 0;\n", .{w});
            try e.raw("}\n}\n");
            try e.raw(column_loop);
            try e.raw("unsigned int col = col_tile + c;\n");
            try weights(e);
            try e.raw(row_loop);
            try e.raw("if (row_base + r < rows) {\n");
            try partialDot(e, "[r][c]", "", "", "[r]");
            try e.raw("}\n}\n}\n");
        },
        .rows_first, .packed_weights_first => {
            if (g == .packed_weights_first) {
                try weights(e);
                for (projections) |p| try e.add("unsigned short {s}_d_h = (unsigned short){s}_bp[0] | ((unsigned short){s}_bp[1] << 8);\n", .{ p, p, p });
                for (projections) |p| try e.add("float {s}_d = termite_half_to_float({s}_d_h);\n", .{ p, p });
                for (projections) |p| for (0..2) |i| try e.add("termite_q4_0_centered_i8x4_pair {s}_packed{d} = termite_pack_q4_0_low_high_centered_i8x4({s}_bp, q8_base{d});\n", .{ p, i, p, i });
            }
            try e.raw(row_loop);
            try e.raw(row_guard);
            try q8Acquire(e, g, false);
            if (g == .rows_first) {
                try e.raw(column_loop);
                try e.raw("unsigned int col = col_tile + c;\n");
                try weights(e);
                try partialDot(e, "[r][c]", "", "", "");
                try e.raw("}\n");
            } else {
                for (projections) |p| {
                    for (words, 0..) |w, i| try e.add("{s}{s}_sumi = __dp4a({s}_packed{d}.{s}, q8_{s}, {s}{s});\n", .{
                        if (i == 0) "int " else "", p,                           p, i / 2, if (i % 2 == 0) "low" else "high", w,
                        if (i == 0) "0" else p,     if (i == 0) "" else "_sumi",
                    });
                }
                for (projections) |p| try e.add("{s}_acc[r] += {s}_d * q8_d * (float){s}_sumi;\n", .{ p, p, p });
            }
            try e.raw("}\n}\n");
        },
    }
    try e.raw("}\n");
}

pub fn renderBody(allocator: std.mem.Allocator, comptime g: Geometry) ![]u8 {
    var e = Emitter{ .allocator = allocator };
    errdefer e.bytes.deinit(allocator);
    const n = g.rows();
    const cols = g.cols();
    const packed_first = g == .packed_weights_first;
    if (!packed_first) try e.add("const unsigned int group_cols = {d}u;\n", .{cols});
    try e.raw("const unsigned int groups_per_wave = 4u;\n");
    if (packed_first) try e.raw("const unsigned int warps_per_group = 4u;\n");
    try e.add("const unsigned int waves = {d}u;\n", .{8 / cols});
    if (n > 1) try e.add("const unsigned int rows_per_block = {d}u;\n", .{n});
    try e.raw("const unsigned int row_blocks = 80u;\nconst unsigned int q8_block_cols = 32u;\nconst unsigned int out_row_blocks = 320u;\n");
    if (g == .single) {
        try e.raw("unsigned int global_q8_out_block = blockIdx.x;\nunsigned int row = global_q8_out_block / out_row_blocks;\nunsigned int q8_out_block = global_q8_out_block - row * out_row_blocks;\n");
    } else if (g == .pair) {
        try e.raw("unsigned int global_pair_block = blockIdx.x;\nunsigned int row_pair = global_pair_block / out_row_blocks;\nunsigned int row0 = row_pair * rows_per_block;\nif (row0 >= rows) return;\nunsigned int row1 = row0 + 1u;\nbool has_row1 = row1 < rows;\nunsigned int q8_out_block = global_pair_block - row_pair * out_row_blocks;\n");
    } else {
        try e.raw("unsigned int global_group_block = blockIdx.x;\nunsigned int row_group = global_group_block / out_row_blocks;\nunsigned int row_base = row_group * rows_per_block;\nif (row_base >= rows) return;\nunsigned int q8_out_block = global_group_block - row_group * out_row_blocks;\n");
    }
    try e.raw("unsigned int col_block = q8_out_block * q8_block_cols;\nunsigned int tid = threadIdx.x;\nunsigned int lane = tid & 31u;\nunsigned int warp = tid >> 5;\n");
    const warps: []const u8 = if (packed_first) "warps_per_group" else "5u";
    try e.add("unsigned int group = warp / {s};\nunsigned int group_warp = warp - group * {s};\n", .{ warps, warps });
    for (projections) |p| {
        try e.add("__shared__ float {s}_partial", .{p});
        if (n > 1) try e.add("[{d}]", .{n});
        try e.raw("[4]");
        if (!packed_first) try e.add("[{d}]", .{cols});
        try e.add("[{d}];\n", .{if (packed_first) @as(usize, 4) else 5});
    }
    try e.raw("__shared__ float activated");
    if (n > 1) try e.add("[{d}]", .{n});
    try e.raw("[32];\n#pragma unroll\nfor (unsigned int wave = 0u; wave < waves; ++wave) {\nif (group < groups_per_wave) {\nunsigned int local_tid = group_warp * 32u + lane;\n");
    try e.raw(if (packed_first) "unsigned int col = col_block + wave * groups_per_wave + group;\n" else "unsigned int col_tile = col_block + (wave * groups_per_wave + group) * group_cols;\n");
    const row_registers: usize = if (g == .pair) 2 else 1;
    for (0..row_registers) |r| for (projections) |p| {
        try e.add("float {s}_acc", .{p});
        if (g == .pair) try e.add("{d}", .{r});
        if (n >= 4) try e.add("[{d}]", .{n});
        if (!packed_first) try e.add("[{d}]", .{cols});
        try e.raw(";\n");
    };
    if (n >= 4) try e.raw(row_loop);
    if (!packed_first) try e.raw(column_loop);
    for (0..row_registers) |r| for (projections) |p| {
        try e.add("{s}_acc", .{p});
        if (g == .pair) try e.add("{d}", .{r});
        if (n >= 4) try e.raw("[r]");
        if (!packed_first) try e.raw("[c]");
        try e.raw(" = 0.0f;\n");
    };
    if (!packed_first) try e.raw("}\n");
    if (n >= 4) try e.raw("}\n");
    try accumulation(&e, g);
    if (n >= 4) try e.raw(row_loop);
    if (!packed_first) try e.raw(column_loop);
    for (0..row_registers) |r| for (projections) |p| {
        try e.add("float {s}_sum", .{p});
        if (g == .pair) try e.add("{d}", .{r});
        try e.add(" = termite_warp_reduce_sum({s}_acc", .{p});
        if (g == .pair) try e.add("{d}", .{r});
        if (n >= 4) try e.raw("[r]");
        if (!packed_first) try e.raw("[c]");
        try e.raw(");\n");
    };
    try e.raw("if (lane == 0u) {\n");
    for (0..row_registers) |r| for (projections) |p| {
        try e.add("{s}_partial", .{p});
        if (g == .pair) try e.add("[{d}]", .{r});
        if (n >= 4) try e.raw("[r]");
        try e.raw("[group]");
        if (!packed_first) try e.raw("[c]");
        try e.add("[group_warp] = {s}_sum", .{p});
        if (g == .pair) try e.add("{d}", .{r});
        try e.raw(";\n");
    };
    try e.raw("}\n");
    if (!packed_first) try e.raw("}\n");
    if (n >= 4) try e.raw("}\n");
    try e.add("}}\n__syncthreads();\nif (tid < {d}u) {{\n", .{if (g == .single) @as(usize, 16) else if (g == .pair) 32 else 64});
    if (g == .single) {
        try e.raw("unsigned int out_group = tid >> 2u;\nunsigned int c = tid & 3u;\n");
    } else {
        const shift: usize = if (packed_first) 2 else if (cols == 2) 3 else 4;
        try e.add("unsigned int out_row = tid >> {d}u;\n", .{shift});
        if (packed_first) {
            try e.raw("unsigned int out_group = tid & 3u;\n");
        } else try e.add("unsigned int local = tid & {d}u;\n", .{if (cols == 2) @as(usize, 7) else 15});
        try e.raw(if (g == .pair) "if (out_row == 0u || has_row1) {\n" else "if (row_base + out_row < rows) {\n");
        if (!packed_first) try e.add("unsigned int out_group = local >> {d}u;\nunsigned int c = local & {d}u;\n", .{ if (cols == 2) @as(usize, 1) else 2, cols - 1 });
    }
    try e.add("float gate_y = 0.0f;\nfloat up_y = 0.0f;\n#pragma unroll\nfor (unsigned int w = 0u; w < {s}; ++w) {{\n", .{warps});
    for (projections) |p| try e.add("{s}_y += {s}_partial{s}[out_group]{s}[w];\n", .{ p, p, if (n > 1) "[out_row]" else "", if (packed_first) "" else "[c]" });
    try e.raw("}\nactivated");
    if (n > 1) try e.raw("[out_row]");
    try e.add("[wave * {d}u + out_group{s}] = termite_decoder_activation_f32(gate_y, activation) * up_y;\n", .{ 4 * cols, if (packed_first) "" else " * group_cols + c" });
    if (n > 1) try e.raw("}\n");
    try e.raw("}\n__syncthreads();\n}\n");
    try e.raw(switch (g) {
        .single => "if (warp == 0u) {\n",
        .pair => "if (warp < rows_per_block && (warp == 0u || has_row1)) {\nunsigned int row = row0 + warp;\n",
        else => "if (warp < rows_per_block && row_base + warp < rows) {\nunsigned int row = row_base + warp;\n",
    });
    try e.add("float x = activated{s}[lane];\n", .{if (n > 1) "[warp]" else ""});
    if (g == .columns_first or g == .rows_first) {
        try e.raw("termite_store_q8_1_zero_sum_warp(dst_q8, row, out_row_blocks, q8_out_block, lane, x);\n");
    } else {
        try e.add("#define TERMITE_Q8_OUTPUT_BLOCK {s}\n#include \"../kernels/q8_1_zero_sum_store.cuh\"\n", .{if (g == .single) "global_q8_out_block" else "(row * out_row_blocks + q8_out_block)"});
    }
    try e.raw("}\n");
    return e.bytes.toOwnedSlice(allocator);
}

fn Owner(comptime geometry: Geometry) type {
    return struct {
        pub const begin = "// quant-kernel-codegen:begin cuda e4b ffn " ++ @tagName(geometry) ++ " (do not edit; run: zig build quant-kernel-codegen -- --write)";
        pub const end = "// quant-kernel-codegen:end cuda e4b ffn " ++ @tagName(geometry);
        pub fn render(allocator: std.mem.Allocator) ![]u8 {
            return renderBody(allocator, geometry);
        }
    };
}
pub const owners = .{ Owner(.single), Owner(.pair), Owner(.columns_first), Owner(.rows_first), Owner(.packed_weights_first) };
