// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//! Exact reducer interchange. Integer sums remain i128 until the final SQL
//! result, including when worker-local states spill or merge in stages.
const std = @import("std");
const Datum = @import("scalar.zig").Datum;
pub fn encode(a: std.mem.Allocator, states: []const @import("operators.zig").Aggregate) ![]const Datum {
    const values = try a.alloc(Datum, states.len * 7);
    for (states, 0..) |state, i| {
        const cells = values[i * 7 ..][0..7];
        for (cells, 0..) |*cell_, part| cell_.* = try cell(a, state, part);
    }
    return values;
}

/// Column access to exact partial interchange without a seven-cell row slice.
pub fn cell(a: std.mem.Allocator, state: @import("operators.zig").Aggregate, part: usize) !Datum {
    return switch (part) {
        0 => Datum.json(.{ .integer = if (state.distinct) 0 else @intCast(state.count) }),
        1 => Datum.json(.{ .number_string = try std.fmt.allocPrint(a, "{d}", .{state.integer_sum}) }),
        2 => Datum.json(.{ .float = state.number_sum }),
        3 => Datum.json(.{ .float = state.compensation }),
        4 => Datum.json(.{ .float = state.mean }),
        5 => Datum.json(.{ .bool = state.boolean }),
        6 => if (state.selected) |selected| selected.row.values[0] else .{},
        else => error.InvalidSqlBackendResponse,
    };
}
