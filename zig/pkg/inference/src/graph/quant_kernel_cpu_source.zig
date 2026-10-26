// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Native CPU source emission and row-register naming shared by tile owners.
const std = @import("std");

pub const Emitter = @import("quant_kernel_native_source.zig").Emitter;
pub const Rows = struct {
    buffers: [4][64]u8 = undefined,
    pub fn row(self: *Rows, mr: usize, r: usize) []const u8 {
        return if (mr == 1) "q_input_row" else std.fmt.bufPrint(&self.buffers[0], "q_input_row{d}", .{r}) catch unreachable;
    }
    pub fn acc(self: *Rows, mr: usize, r: usize) []const u8 {
        return if (mr == 1) "acc" else std.fmt.bufPrint(&self.buffers[1], "acc{d}", .{r}) catch unreachable;
    }
    pub fn position(self: *Rows, r: usize) []const u8 {
        return if (r == 0) "r" else std.fmt.bufPrint(&self.buffers[2], "(r + {d})", .{r}) catch unreachable;
    }
    pub fn block(self: *Rows, mr: usize, r: usize) []const u8 {
        return if (mr == 1) "q" else std.fmt.bufPrint(&self.buffers[3], "q{d}", .{r}) catch unreachable;
    }
};

pub fn rowName(buffer: *[32]u8, prefix: []const u8, mr: usize, row: usize) []const u8 {
    return if (mr == 1) prefix else std.fmt.bufPrint(buffer, "{s}{d}", .{ prefix, row }) catch unreachable;
}

pub fn formatZig(allocator: std.mem.Allocator, bytes: []const u8) ![]u8 {
    const source = try allocator.dupeSentinel(u8, bytes, 0);
    defer allocator.free(source);
    var tree = try std.zig.Ast.parse(allocator, source, .{});
    defer tree.deinit(allocator);
    if (tree.errors.len != 0) return error.InvalidCpuKernelSource;
    var formatted = std.Io.Writer.Allocating.init(allocator);
    defer formatted.deinit();
    try tree.render(allocator, &formatted.writer, .{});
    return formatted.toOwnedSlice();
}

/// Format a managed statement region in its existing native function scope.
/// The wrapper participates only in syntax parsing and is never emitted.
pub fn formatStatements(allocator: std.mem.Allocator, statements: []const u8, indent: usize) ![]u8 {
    const prefix = "fn fragment() void {\n";
    const wrapped = try std.mem.concat(allocator, u8, &.{ prefix, statements, "\n}\n" });
    defer allocator.free(wrapped);
    const formatted = try formatZig(allocator, wrapped);
    defer allocator.free(formatted);
    if (!std.mem.startsWith(u8, formatted, prefix) or !std.mem.endsWith(u8, formatted, "}\n")) return error.InvalidNativeStatementRegion;
    var out = Emitter{ .allocator = allocator };
    errdefer out.bytes.deinit(allocator);
    var lines = std.mem.splitScalar(u8, formatted[prefix.len .. formatted.len - 2], '\n');
    while (lines.next()) |line| {
        if (line.len == 0) {
            if (lines.peek() != null) try out.raw("\n");
            continue;
        }
        if (!std.mem.startsWith(u8, line, "    ")) return error.InvalidNativeStatementIndentation;
        try out.bytes.appendNTimes(allocator, ' ', indent);
        try out.raw(line[4..]);
        try out.raw("\n");
    }
    return out.bytes.toOwnedSlice(allocator);
}

test "statement formatting retains nested scope and anonymous struct declarations" {
    const formatted = try formatStatements(std.testing.allocator, "if (ready) { const x: struct { value: u32 } = .{ .value = 7 }; consume(x); }\npublish();", 8);
    defer std.testing.allocator.free(formatted);
    try std.testing.expect(std.mem.startsWith(u8, formatted, "        if (ready) {\n"));
    try std.testing.expect(std.mem.endsWith(u8, formatted, "        publish();\n"));
    try std.testing.expect(std.mem.indexOf(u8, formatted, "fragment") == null);
}

// Native parameter declarations are shared by entry and worker schedule owners.
pub const Parameter = enum {
    kind_comptime,
    io,
    input,
    prepared_blocks_required,
    output,
    rows,
    in_dim,
    out_dim,
    prepared_blocks_a_required,
    prepared_blocks_b_required,
    prepared_blocks_c_required,
    bias_a,
    bias_b,
    bias_c,
    output_a,
    output_b,
    output_c,
    weight_raw,
    row_blocks,
    block_size,
    values_per_block,
    row_stride,
    dot_fn,
    weight_raw_a,
    dot_fn_a,
    weight_raw_b,
    dot_fn_b,
    prepared_blocks_optional,
    prepared_panel_blocks,
    q_input,
    input_scales,
    prepared_panel8_blocks,
    bias,
    q_values,
    prepared_blocks_a_optional,
    prepared_panel_blocks_a,
    prepared_panel8_blocks_a,
    prepared_blocks_b_optional,
    prepared_panel_blocks_b,
    prepared_panel8_blocks_b,
    weight_raw_c,
    prepared_blocks_c_optional,
    prepared_panel_blocks_c,
    prepared_panel8_blocks_c,
    panel_blocks,
    panel8_blocks,
    panel16_blocks_optional,
    panel_blocks_a,
    panel8_blocks_a,
    panel16_blocks_a,
    panel_blocks_b,
    panel8_blocks_b,
    panel16_blocks_b,
    panel_blocks_c,
    panel8_blocks_c,
    panel16_blocks_c,
    known_required,
    q6,
    known_optional,
    packed_qkv_panel16_blocks,
    packed_qkv_panel16_mr2,
    input_sums,
    use_mr4,
    has_min_comptime,
    q3_panel_blocks,
    q3_panel_blocks_a,
    q3_panel_blocks_b,
    q3_panel_blocks_c,
    panel16_blocks_required,
    prepared_comptime,
    out_start,
    out_end,
    projection_count_comptime,
    weight_blocks_grouped,
    panel_blocks_grouped,
    biases,
    outputs,
    panel8_blocks_grouped,
    pub fn declaration(self: Parameter) []const u8 {
        return switch (self) {
            .kind_comptime => "comptime kind: gguf_tensor_types.KnownTensorType",
            .io => "io: ?std.Io",
            .input => "input: []const f32",
            .prepared_blocks_required => "prepared_blocks: []const u8",
            .output => "output: []f32",
            .rows => "rows: usize",
            .in_dim => "in_dim: usize",
            .out_dim => "out_dim: usize",
            .prepared_blocks_a_required => "prepared_blocks_a: []const u8",
            .prepared_blocks_b_required => "prepared_blocks_b: []const u8",
            .prepared_blocks_c_required => "prepared_blocks_c: []const u8",
            .bias_a => "bias_a: ?[]const f32",
            .bias_b => "bias_b: ?[]const f32",
            .bias_c => "bias_c: ?[]const f32",
            .output_a => "output_a: []f32",
            .output_b => "output_b: []f32",
            .output_c => "output_c: []f32",
            .weight_raw => "weight_raw: []const u8",
            .row_blocks => "row_blocks: usize",
            .block_size => "block_size: usize",
            .values_per_block => "values_per_block: usize",
            .row_stride => "row_stride: usize",
            .dot_fn => "dot_fn: *const fn ([]const f32, []const u8) f32",
            .weight_raw_a => "weight_raw_a: []const u8",
            .dot_fn_a => "dot_fn_a: *const fn ([]const f32, []const u8) f32",
            .weight_raw_b => "weight_raw_b: []const u8",
            .dot_fn_b => "dot_fn_b: *const fn ([]const f32, []const u8) f32",
            .prepared_blocks_optional => "prepared_blocks: ?[]const u8",
            .prepared_panel_blocks => "prepared_panel_blocks: ?[]const u8",
            .q_input => "q_input: []const u8",
            .input_scales => "input_scales: []const f32",
            .prepared_panel8_blocks => "prepared_panel8_blocks: ?[]const u8",
            .bias => "bias: ?[]const f32",
            .q_values => "q_values: []const u8",
            .prepared_blocks_a_optional => "prepared_blocks_a: ?[]const u8",
            .prepared_panel_blocks_a => "prepared_panel_blocks_a: ?[]const u8",
            .prepared_panel8_blocks_a => "prepared_panel8_blocks_a: ?[]const u8",
            .prepared_blocks_b_optional => "prepared_blocks_b: ?[]const u8",
            .prepared_panel_blocks_b => "prepared_panel_blocks_b: ?[]const u8",
            .prepared_panel8_blocks_b => "prepared_panel8_blocks_b: ?[]const u8",
            .weight_raw_c => "weight_raw_c: []const u8",
            .prepared_blocks_c_optional => "prepared_blocks_c: ?[]const u8",
            .prepared_panel_blocks_c => "prepared_panel_blocks_c: ?[]const u8",
            .prepared_panel8_blocks_c => "prepared_panel8_blocks_c: ?[]const u8",
            .panel_blocks => "panel_blocks: ?[]const u8",
            .panel8_blocks => "panel8_blocks: ?[]const u8",
            .panel16_blocks_optional => "panel16_blocks: ?[]const u8",
            .panel_blocks_a => "panel_blocks_a: ?[]const u8",
            .panel8_blocks_a => "panel8_blocks_a: ?[]const u8",
            .panel16_blocks_a => "panel16_blocks_a: ?[]const u8",
            .panel_blocks_b => "panel_blocks_b: ?[]const u8",
            .panel8_blocks_b => "panel8_blocks_b: ?[]const u8",
            .panel16_blocks_b => "panel16_blocks_b: ?[]const u8",
            .panel_blocks_c => "panel_blocks_c: ?[]const u8",
            .panel8_blocks_c => "panel8_blocks_c: ?[]const u8",
            .panel16_blocks_c => "panel16_blocks_c: ?[]const u8",
            .known_required => "known: gguf_tensor_types.KnownTensorType",
            .q6 => "q6: bool",
            .known_optional => "known: ?gguf_tensor_types.KnownTensorType",
            .packed_qkv_panel16_blocks => "packed_qkv_panel16_blocks: ?[]const u8",
            .packed_qkv_panel16_mr2 => "packed_qkv_panel16_mr2: bool",
            .input_sums => "input_sums: []const i32",
            .use_mr4 => "use_mr4: bool",
            .has_min_comptime => "comptime has_min: bool",
            .q3_panel_blocks => "q3_panel_blocks: []const u8",
            .q3_panel_blocks_a => "q3_panel_blocks_a: []const u8",
            .q3_panel_blocks_b => "q3_panel_blocks_b: []const u8",
            .q3_panel_blocks_c => "q3_panel_blocks_c: []const u8",
            .panel16_blocks_required => "panel16_blocks: []const u8",
            .prepared_comptime => "comptime prepared: bool",
            .out_start => "out_start: usize",
            .out_end => "out_end: usize",
            .projection_count_comptime => "comptime projection_count: usize",
            .weight_blocks_grouped => "weight_blocks: [projection_count][]const u8",
            .panel_blocks_grouped => "panel_blocks: [projection_count]?[]const u8",
            .biases => "biases: [projection_count]?[]const f32",
            .outputs => "outputs: [projection_count][]f32",
            .panel8_blocks_grouped => "panel8_blocks: [projection_count][]const u8",
        };
    }
};

pub fn rowSuffix(rows: usize, row: usize) []const u8 {
    return if (rows == 1) "" else ([_][]const u8{ "0", "1", "2", "3" })[row];
}
