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

//! Grouped Q8 projection geometry with native scalar/vector schedules.
//! Input values and scales remain outside projection loops, so sharing the
//! source retains the qualified reuse, accumulation and publication order.
const std = @import("std");
const native = @import("quant_kernel_cpu_source.zig");
const Emitter = native.Emitter;
const Rows = native.Rows;
const suffix = native.rowSuffix;
const Kind = enum { grouped, panel8 };
const Entry = struct { name: []const u8, parameters: []const native.Parameter };
const entries = [_]Entry{
    .{ .name = "linearQ8_0QuantizedInputGroupedRange", .parameters = &.{ .prepared_comptime, .projection_count_comptime, .q_input, .input_scales, .weight_blocks_grouped, .panel_blocks_grouped, .biases, .outputs, .rows, .in_dim, .out_dim, .out_start, .out_end } },
    .{ .name = "linearQ8_0QuantizedInputGroupedPanel8Range", .parameters = &.{ .projection_count_comptime, .q_input, .input_scales, .weight_blocks_grouped, .panel_blocks_grouped, .panel8_blocks_grouped, .biases, .outputs, .rows, .in_dim, .out_dim, .out_start, .out_end } },
};
pub const owners = .{ Owner(.grouped), Owner(.panel8) };
fn Owner(comptime kind: Kind) type {
    return struct {
        pub const name = entries[@backingInt(kind)].name;
        pub const begin = "// quant-kernel-codegen:begin CPU Q8 grouped projection " ++ name;
        pub const end = "// quant-kernel-codegen:end CPU Q8 grouped projection " ++ name;
        pub fn render(allocator: std.mem.Allocator) ![]u8 {
            var out = Emitter{ .allocator = allocator };
            defer out.bytes.deinit(allocator);
            try out.add("fn {s}(\n", .{name});
            for (entries[@backingInt(kind)].parameters) |parameter| try out.add("{s},\n", .{parameter.declaration()});
            try out.raw(") void {\n");
            if (kind == .grouped) try out.raw("const block_size: usize = q8_0BlockSize(prepared);\n");
            try out.raw("const values_per_block: usize = 32;\nstd.debug.assert(in_dim % values_per_block == 0);\nconst row_blocks = in_dim / values_per_block;\n");
            if (kind == .grouped) try out.raw("const row_stride = row_blocks * block_size;\nconst use_nr8 = q8_0GroupedNR8DirectEnabled();\n") else try panel8Bounds(&out);
            try out.raw("var r: usize = 0;\n");
            const row_tiles: []const usize = if (kind == .panel8) &.{ 4, 2, 1 } else &.{ 2, 1 };
            for (row_tiles) |mr| {
                if (mr == 4) try out.raw("while (q8_0Panel8MR4DirectEnabled() and r + 4 <= rows) : (r += 4) {\n") else if (mr == 2) try out.raw("while (r + 2 <= rows) : (r += 2) {\n") else try out.raw("while (r < rows) : (r += 1) {\n");
                try inputRows(&out, mr);
                if (kind == .panel8) {
                    try panel8Column(&out, mr);
                } else {
                    try out.raw("var o: usize = out_start;\nwhile (o < out_end and (o % prepared_q8_0_panel_nr) != 0) : (o += 1) {\n");
                    try groupedColumn(&out, mr, 1);
                    try out.raw("}\nwhile (use_nr8 and (o % 8) == 0 and o + 8 <= out_end) : (o += 8) {\n");
                    try groupedColumn(&out, mr, 8);
                    try out.raw("}\nwhile (o + prepared_q8_0_panel_nr <= out_end) : (o += prepared_q8_0_panel_nr) {\n");
                    try groupedColumn(&out, mr, 4);
                    try out.raw("}\nwhile (o < out_end) : (o += 1) {\n");
                    try groupedColumn(&out, mr, 1);
                    try out.raw("}\n");
                }
                try out.raw("}\n");
            }
            if (kind == .panel8) {
                try out.raw("if (body_end < out_end) {\n");
                try residualCall(&out, "body_end", "out_end");
                try out.raw("}\n");
            }
            try out.raw("}\n");
            return native.formatZig(allocator, out.bytes.items);
        }
    };
}
fn inputRows(out: *Emitter, mr: usize) !void {
    var rows: Rows = .{};
    for (0..mr) |r| try out.add("const input_q_row{s} = q_input[{s} * row_blocks * values_per_block ..][0 .. row_blocks * values_per_block];\n", .{ suffix(mr, r), rows.position(r) });
    for (0..mr) |r| try out.add("const input_scale_row{s} = input_scales[{s} * row_blocks ..][0..row_blocks];\n", .{ suffix(mr, r), rows.position(r) });
}
fn blockInputs(out: *Emitter, mr: usize) !void {
    for (0..mr) |r| try out.add("const qx{s}: *const [32]u8 = input_q_row{s}[block_idx * values_per_block ..][0..32];\n", .{ suffix(mr, r), suffix(mr, r) });
    for (0..mr) |r| try out.add("const input_scale{s} = input_scale_row{s}[block_idx];\n", .{ suffix(mr, r), suffix(mr, r) });
}
fn groupedColumn(out: *Emitter, mr: usize, nr: usize) !void {
    for (0..mr) |r| {
        if (nr == 1) try out.add("var acc{s}: [projection_count]f32 = undefined;\n", .{suffix(mr, r)}) else try out.add("var acc{s}: [projection_count][{d}]f32 = undefined;\n", .{ suffix(mr, r), nr });
    }
    try out.raw("inline for (0..projection_count) |projection_idx| {\n");
    if (nr == 1) try out.add("acc{s}[projection_idx] = optionalLinearBiasAt(biases[projection_idx], o);\n", .{suffix(mr, 0)}) else {
        try out.add("acc{s}[projection_idx] = .{{\n", .{suffix(mr, 0)});
        for (0..nr) |c| {
            try out.raw("optionalLinearBiasAt(biases[projection_idx], o");
            if (c != 0) try out.add(" + {d}", .{c});
            try out.raw("),\n");
        }
        try out.raw("};\n");
    }
    if (mr == 2) try out.raw("acc1[projection_idx] = acc0[projection_idx];\n");
    try out.raw("}\nfor (0..row_blocks) |block_idx| {\n");
    try blockInputs(out, mr);
    try out.raw("inline for (0..projection_count) |projection_idx| {\n");
    if (nr == 1) try out.raw("const weight_block = q8_0QuantizedWeightBlock(prepared, weight_blocks[projection_idx], panel_blocks[projection_idx], row_blocks, row_stride, o, block_idx, false);\n") else try out.add("const weights = q8_0QuantizedWeightBlock{d}(prepared, weight_blocks[projection_idx], panel_blocks[projection_idx], row_blocks, row_stride, o, block_idx, true);\n", .{nr});
    try out.raw("q8_0AddDot");
    if (nr != 1) try out.add("{d}", .{nr});
    try out.add("MR{d}(prepared", .{mr});
    for (0..mr) |r| try out.add(", qx{s}", .{suffix(mr, r)});
    for (0..mr) |r| try out.add(", input_scale{s}", .{suffix(mr, r)});
    if (nr == 1) try out.raw(", weight_block") else if (nr == 8) try out.raw(", weights") else for (0..nr) |c| try out.add(", weights[{d}]", .{c});
    for (0..mr) |r| try out.add(", &acc{s}[projection_idx]", .{suffix(mr, r)});
    try out.raw(");\n}\n}\n");
    try publish(out, mr, if (nr == 1) null else nr, false);
}
fn publish(out: *Emitter, mr: usize, nr: ?usize, vector: bool) !void {
    try out.raw("inline for (0..projection_count) |projection_idx| {\n");
    var rows: Rows = .{};
    if (nr) |width| {
        for (0..mr) |r| try out.add("const out_row{s} = outputs[projection_idx][{s} * out_dim ..][0..out_dim];\n", .{ suffix(mr, r), rows.position(r) });
        if (vector) for (0..mr) |r| try out.add("const acc{s}_values = f32x8ToArray(acc{s}[projection_idx]);\n", .{ suffix(mr, r), suffix(mr, r) });
        if (vector) try out.raw("inline for (0..prepared_q8_0_panel8_nr) |lane| {\n") else try out.add("inline for (0..{d}) |lane| {{\n", .{width});
        for (0..mr) |r| {
            if (vector) try out.add("out_row{s}[o + lane] = acc{s}_values[lane];\n", .{ suffix(mr, r), suffix(mr, r) }) else try out.add("out_row{s}[o + lane] = acc{s}[projection_idx][lane];\n", .{ suffix(mr, r), suffix(mr, r) });
        }
        try out.raw("}\n");
    } else for (0..mr) |r| try out.add("outputs[projection_idx][{s} * out_dim + o] = acc{s}[projection_idx];\n", .{ rows.position(r), suffix(mr, r) });
    try out.raw("}\n");
}
fn residualCall(out: *Emitter, first: []const u8, last: []const u8) !void {
    try out.add("linearQ8_0QuantizedInputGroupedRange(true, projection_count, q_input, input_scales, weight_blocks, panel_blocks, biases, outputs, rows, in_dim, out_dim, {s}, {s});\n", .{ first, last });
}
fn panel8Bounds(out: *Emitter) !void {
    try out.raw("var body_start = out_start;\nwhile (body_start < out_end and (body_start % prepared_q8_0_panel8_nr) != 0) : (body_start += 1) {}\nconst body_end = if (body_start < out_end)\nbody_start + ((out_end - body_start) / prepared_q8_0_panel8_nr) * prepared_q8_0_panel8_nr\nelse\nbody_start;\nif (out_start < body_start) {\n");
    try residualCall(out, "out_start", "body_start");
    try out.raw("}\nif (body_start == body_end) {\nif (body_end < out_end) {\n");
    try residualCall(out, "body_end", "out_end");
    try out.raw("}\nreturn;\n}\n");
}
fn panel8Column(out: *Emitter, mr: usize) !void {
    try out.raw("var o = body_start;\nwhile (o < body_end) : (o += prepared_q8_0_panel8_nr) {\n");
    for (0..mr) |r| try out.add("var acc{s}: [projection_count]F32x8 = undefined;\n", .{suffix(mr, r)});
    try out.add("inline for (0..projection_count) |projection_idx| {{\nacc{s}[projection_idx] = q8_0BiasVec8(biases[projection_idx], o);\n", .{suffix(mr, 0)});
    for (1..mr) |r| try out.add("acc{d}[projection_idx] = acc0[projection_idx];\n", .{r});
    try out.raw("}\nfor (0..row_blocks) |block_idx| {\n");
    try blockInputs(out, mr);
    try out.raw("const panel_off = ((o / prepared_q8_0_panel8_nr) * row_blocks + block_idx) * prepared_q8_0_panel8_block_bytes;\ninline for (0..projection_count) |projection_idx| {\n");
    if (mr == 1) try out.raw("acc[projection_idx] += ") else try out.raw("const sums = ");
    try out.add("dotQ8_0Panel8MR{d}Vec(", .{mr});
    for (0..mr) |r| try out.add("qx{s}, ", .{suffix(mr, r)});
    for (0..mr) |r| try out.add("input_scale{s}, ", .{suffix(mr, r)});
    try out.raw("panel8_blocks[projection_idx][panel_off..][0..prepared_q8_0_panel8_block_bytes]);\n");
    if (mr != 1) for (0..mr) |r| try out.add("acc{d}[projection_idx] += sums.r{d};\n", .{ r, r });
    try out.raw("}\n}\n");
    try publish(out, mr, 8, true);
    try out.raw("}\n");
}

test "grouped Q8 native row and panel schedules parse without leaking" {
    inline for (owners) |OwnerType| {
        const source = try OwnerType.render(std.testing.allocator);
        defer std.testing.allocator.free(source);
        try std.testing.expect(source.len != 0);
    }
}
