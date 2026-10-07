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

//! External window sorts, partition rows, peer directories and segment trees
//! share the statement's spill owner. Each pass preserves original ordinals.
const std = @import("std");
const disk = @import("disk_rows.zig");
const scalar = @import("scalar.zig");
const operators = @import("operators.zig");
const spill = @import("spill.zig");
const window = @import("window_runtime.zig");
const Datum = scalar.Datum;
fn same(left: []const Datum, right: []const Datum, count: usize) !bool {
    for (left[0..count], right[0..count]) |a, b| {
        if (a.sql_null != b.sql_null) return false;
        if (!a.sql_null and (try scalar.compareDatums(a, b)) != .eq) return false;
    }
    return true;
}
/// Retain one bounded typed page for the small-input fast path, then admit
/// directly into the window store. No public JSON stream or second spool.
fn Input(comptime Context: type) type {
    return struct {
        context: Context,
        width: usize,
        input_width: usize,
        arena: std.heap.ArenaAllocator,
        scratch: std.heap.ArenaAllocator,
        pending: std.ArrayList([]Datum) = .empty,
        bytes: usize = 0,
        rows: ?disk.Rows = null,

        fn deinit(self: *@This()) void {
            if (self.rows) |*rows| rows.deinit();
            self.pending.deinit(self.context.alloc);
            self.arena.deinit();
            self.scratch.deinit();
        }
        fn append(raw: *anyopaque, input: []const Datum) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (input.len != self.input_width) return error.InvalidSqlBackendResponse;
            var bytes: usize = self.width * @sizeOf(Datum) + @sizeOf([]Datum);
            for (input) |value| bytes +|= try operators.datumBytes(value);
            if (self.rows == null and (self.pending.items.len >= self.context.limits.page_rows or bytes > self.context.limits.page_bytes -| self.bytes)) {
                self.rows = try disk.Rows.init(self.context.alloc, self.context.spill.?, self.width);
                try self.rows.?.enableColumns();
                for (self.pending.items) |values| try self.rows.?.append(.{ .values = values, .keys = &.{}, .ordinal = self.rows.?.len });
                self.pending.clearAndFree(self.context.alloc);
                _ = self.arena.reset(.free_all);
            }
            if (self.rows) |*rows| {
                _ = self.scratch.reset(.retain_capacity);
                const values = try self.scratch.allocator().alloc(Datum, self.width);
                @memset(values, .{});
                @memcpy(values[0..input.len], input);
                try rows.append(.{ .values = values, .keys = &.{}, .ordinal = rows.len });
            } else {
                const values = try self.arena.allocator().alloc(Datum, self.width);
                @memset(values, .{});
                for (input, values[0..input.len]) |value, *out| out.* = try operators.cloneDatum(self.arena.allocator(), value);
                try self.pending.append(self.context.alloc, values);
                self.bytes +|= bytes;
            }
        }
    };
}
pub fn execute(context: anytype, statement: @import("ast.zig").Select) !?@import("runtime.zig").Output {
    const bound = context.binding.window.?;
    const manager = context.spill orelse return null;
    const width = bound.input.columns.len + bound.specs.len;
    const Collector = Input(@TypeOf(context));
    var input: Collector = .{ .context = context, .width = width, .input_width = bound.input.columns.len, .arena = .init(context.alloc), .scratch = .init(context.alloc) };
    defer input.deinit();
    var input_context = context;
    input_context.binding = bound.input.*;
    input_context.limits.result_rows = context.limits.scan_rows;
    try input_context.selectInto(bound.statement, .{ .ptr = &input, .append = Collector.append });
    if (input.rows == null) return try window.evaluateCells(context, statement, input.pending.items);
    var rows = input.rows.?;
    input.rows = null;
    defer rows.deinit();
    const roots = try @import("ordering_reuse.zig").plan(context.arena, bound);
    try rows.enableColumnUpdates(bound.input.columns.len);
    var final_indices = try disk.Integers.init(manager);
    defer final_indices.deinit();
    var physical_order: ?@import("window_binding.zig").Sort = null;
    for (bound.sorts, 0..) |specification, root| {
        if (roots[root] != root) continue;
        var arena = std.heap.ArenaAllocator.init(context.alloc);
        defer arena.deinit();
        const a = arena.allocator();
        const orders = try a.alloc(operators.Order, specification.partition.len + specification.order.len);
        @memset(orders[0..specification.partition.len], .{});
        @memcpy(orders[specification.partition.len..], specification.directions);
        var sort = spill.Sort.init(context.alloc, manager, orders, context.limits.retained_bytes / 4);
        defer sort.deinit();
        var scratch = std.heap.ArenaAllocator.init(context.alloc);
        defer scratch.deinit();
        for (0..rows.len) |index| {
            try context.checkpoint();
            _ = scratch.reset(.free_all);
            const ordinal = index;
            const keys = try scratch.allocator().alloc(Datum, orders.len);
            for (specification.partition, keys[0..specification.partition.len]) |column, *key| key.* = try operators.cloneDatum(scratch.allocator(), try rows.cell(index, column));
            for (specification.order, keys[specification.partition.len..]) |column, *key| key.* = try operators.cloneDatum(scratch.allocator(), try rows.cell(index, column));
            // Sort compact row references; wide payloads stay in their
            // existing row store instead of being copied into every run.
            try sort.add(.{ .values = &.{Datum.json(.{ .integer = @intCast(index) })}, .keys = keys, .ordinal = ordinal });
        }
        var layout = try disk.Integers.init(manager);
        defer layout.deinit();
        var carry: ?operators.Row = null;
        while (true) {
            const first_row = carry orelse (try sort.next(scratch.allocator())) orelse break;
            carry = null;
            var keys_arena = std.heap.ArenaAllocator.init(context.alloc);
            defer keys_arena.deinit();
            const keys = try keys_arena.allocator().alloc(Datum, specification.partition.len);
            for (first_row.keys[0..keys.len], keys) |value, *key| key.* = try operators.cloneDatum(keys_arena.allocator(), value);
            const begin = layout.len;
            try layout.append(std.math.cast(usize, first_row.values[0].value.integer) orelse return error.InvalidSqlSpill);
            while (true) {
                _ = scratch.reset(.free_all);
                const candidate = (try sort.next(scratch.allocator())) orelse break;
                if (!try same(keys, candidate.keys, keys.len)) {
                    carry = candidate;
                    break;
                }
                try layout.append(std.math.cast(usize, candidate.values[0].value.integer) orelse return error.InvalidSqlSpill);
            }
            var partition: disk.View = .{ .source = &rows, .indices = &layout, .begin = begin, .len = layout.len - begin };
            for (bound.sorts, 0..) |requirement, sort_index| {
                if (roots[sort_index] != root) continue;
                var starts = try disk.Integers.init(manager);
                defer starts.deinit();
                var ends = try disk.Integers.init(manager);
                defer ends.deinit();
                var groups = try disk.Integers.init(manager);
                defer groups.deinit();
                var position: usize = 0;
                while (position < partition.len) {
                    var end = position + 1;
                    while (end < partition.len and try window.equal(&partition, position, end, requirement.order)) : (end += 1) try context.checkpoint();
                    try groups.append(position);
                    for (position..end) |_| {
                        try starts.append(position);
                        try ends.append(end);
                    }
                    position = end;
                }
                try groups.append(partition.len);
                const indices = disk.Identity{ .len = partition.len };
                for (bound.specs, 0..) |spec, column| if (spec.sort == sort_index) try window.evaluate(context, &partition, indices, requirement, spec, bound.input.columns.len + column, &starts, &ends, &groups);
            }
        }
        std.mem.swap(disk.Integers, &final_indices, &layout);
        physical_order = specification;
    }
    var result: disk.View = .{ .source = &rows, .indices = &final_indices, .len = rows.len };
    return try window.finishOrderedCells(context, statement, &result, physical_order);
}
