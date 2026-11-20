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

//! Ordered native dot phases. The caller owns storage conversion, accumulator
//! initialization, reduction/MMA schedule and epilogue publication.
const render = @import("source_template.zig").render;

pub const Product = struct { left: []const u8, right: []const u8 };
pub const Association = enum { combined, termwise };

/// Relative-score schedules differ numerically: scalar schedules accumulate a
/// combined expression, while warp-striped CUDA schedules publish each product
/// into their accumulator in order.
/// The schedule selects that association explicitly; neither is reassociated.
pub fn accumulate(comptime terms: []const Product, comptime association: Association, comptime accumulator: []const u8) []const u8 {
    if (terms.len == 0) @compileError("ordered product phase needs a term");
    var result: []const u8 = "";
    for (terms, 0..) |term, i| {
        if (association == .termwise or i == 0) result = result ++ accumulator ++ " += ";
        result = result ++ term.left ++ " * " ++ term.right;
        result = result ++ if (association == .termwise or i + 1 == terms.len) ";\n" else " + ";
    }
    return result;
}

pub const Dot = struct {
    integer: []const u8,
    index: []const u8,
    begin: []const u8 = "0",
    end: []const u8,
    step: ?[]const u8 = null,
    left: []const u8 = "",
    right: []const u8 = "",
    terms: []const Product = &.{},
    association: Association = .combined,
    accumulator: []const u8 = "acc",
    indent: []const u8 = "    ",
    pub fn native(comptime d: Dot) []const u8 {
        if (d.terms.len != 0 and (d.left.len != 0 or d.right.len != 0)) @compileError("dot selects either a product list or one operand pair");
        if (d.terms.len == 0 and (d.left.len == 0 or d.right.len == 0)) @compileError("dot needs both operands");
        return render(
            \\{{indent}}for ({{u}} {{i}} = {{begin}}; {{i}} < {{end}}; {{increment}}) {
            \\{{indent}}    {{products}}
            \\{{indent}}}
            \\
        , .{ .indent = d.indent, .u = d.integer, .i = d.index, .begin = d.begin, .end = d.end, .increment = if (d.step) |step| d.index ++ " += " ++ step else "++" ++ d.index, .products = accumulate(if (d.terms.len == 0) &.{.{ .left = d.left, .right = d.right }} else d.terms, d.association, d.accumulator) });
    }
};

/// Ordered 256-value blocks, 32-value subblocks and eight-value staging for
/// the qualified Metal row schedule. Decoder symbols belong to ABI bindings.
/// Packed-value decoding remains owned by the format recipe; this schedule
/// stages each input vector once and visits output columns in their old order.
/// Register naming is an ABI adapter, not another traversal implementation.
pub const MetalBlock256Sub32Rows = struct {
    pub const Accumulators = union(enum) {
        pair: struct { first: [2][]const u8, second: [2][]const u8 },
        pair_array: struct { first: []const u8, second: []const u8 },
        scalar: [2][]const u8,
    };
    weight: []const u8,
    other_weight: ?[]const u8 = null,
    accumulators: Accumulators,
    dot: []const u8,
    pub fn native(comptime p: MetalBlock256Sub32Rows) []const u8 {
        const paired = p.other_weight != null;
        const update = switch (p.accumulators) {
            .pair => |a| if (paired) "if (row == 0u) { " ++ a.first[0] ++ " += block.x; " ++ a.second[0] ++ " += block.y; } else { " ++ a.first[1] ++ " += block.x; " ++ a.second[1] ++ " += block.y; }" else @compileError("paired accumulators need two weights"),
            .pair_array => |a| if (paired) a.first ++ "[row] += block.x; " ++ a.second ++ "[row] += block.y;" else @compileError("paired accumulators need two weights"),
            .scalar => |a| if (!paired) "if (row == 0u) " ++ a[0] ++ " += block; else " ++ a[1] ++ " += block;" else @compileError("scalar accumulators need one weight"),
        };
        return render(
            \\    for (uint sb = sb0; sb < total_subblocks; sb += NSG * NQ) { uint b = sb >> 3; uint sub = sb & 7u; uint chunk = sub >> 1; bool high = (sub & 1u) != 0u; uint j0 = il * NQ; uint input_off = in_row + b * 256u + sub * 32u + j0; float x[8]; for (uint i = 0; i < NQ; ++i) { x[i] = input[input_off + i]; } for (uint row = 0; row < NR0; ++row) { uint o = first_o + row; if (o >= p.out_dim) continue; {{type}} block = {{dot}}(x, {{weights}}, p.row_blocks, o, b, sub, chunk, high, j0); {{update}} } }
            \\
        , .{ .type = if (paired) "float2" else "float", .dot = p.dot, .weights = p.weight ++ (if (p.other_weight) |other| ", " ++ other else ""), .update = update });
    }
};
