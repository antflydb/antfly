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

//! PostgreSQL ordering of pinned canonical array cells, without decoding a
//! flat element vector. Primitive/NUMERIC comparison allocates nothing; JSONB
//! owns at most one element pair. Only a codec-authenticated view may enter.
const std = @import("std");
const arrays = @import("array_value.zig");
const layout = @import("../common/sql_array_layout.zig");
const numbers = @import("../common/sql_numeric_layout.zig");
const exact = @import("numeric_value.zig");
const json_order = @import("json_order.zig");
const MemoryBudget = @import("memory_budget.zig");
const uuid = @import("../common/uuid.zig");

/// Inputs must have passed strict canonical ingestion, or remain pinned under
/// authenticated row/page ownership. This function does not confer that trust.
/// Scratch is separately bounded; all work/cancellation belongs to context.
pub fn order(left: layout.View, right: layout.View, context: *exact.Context, scratch_limit: usize) !std.math.Order {
    const native = @import("array_native.zig");
    // This boundary already requires strict ingress or pinned authenticated
    // ownership. Such views need not carry an optional validation summary.
    if (left.kind == right.kind and native.primitive(left.kind)) {
        try context.charge(1);
        var work: arrays.Budget = .{ .remaining = std.math.maxInt(usize), .shared = context };
        const compared = try native.orderPrefix(left, right, &work, true);
        if (compared != .eq) return compared;
        var ad: [layout.max_rank]arrays.Dimension = undefined;
        var bd: [layout.max_rank]arrays.Dimension = undefined;
        for (ad[0..left.rank], 0..) |*axis, i| axis.* = try left.dimension(i);
        for (bd[0..right.rank], 0..) |*axis, i| axis.* = try right.dimension(i);
        return arrays.compareShape(left.count, right.count, ad[0..left.rank], bd[0..right.rank], &work);
    }
    return orderScalar(left, right, context, scratch_limit);
}

fn orderScalar(left: layout.View, right: layout.View, context: *exact.Context, scratch_limit: usize) !std.math.Order {
    try context.charge(1);
    if (left.kind != right.kind) return error.SqlTypeMismatch;
    var work: arrays.Budget = .{ .remaining = std.math.maxInt(usize), .shared = context };
    var memory: MemoryBudget = .{ .backing = context.alloc, .limit = scratch_limit };
    var arena = std.heap.ArenaAllocator.init(memory.allocator());
    defer arena.deinit();
    for (0..@min(left.count, right.count)) |i| {
        const a = try left.cell(i);
        const b = try right.cell(i);
        try work.consume(1);
        const compared: std.math.Order = if (a.sql_null or b.sql_null)
            (if (a.sql_null == b.sql_null) .eq else if (a.sql_null) .gt else .lt)
        else switch (left.kind) {
            .numeric => numeric: {
                const av = try numbers.View.openWithBudget(a.bytes, .{ .bytes = context.max_input_bytes }, context);
                const bv = try numbers.View.openWithBudget(b.bytes, .{ .bytes = context.max_input_bytes }, context);
                break :numeric try av.order(bv, context);
            },
            .jsonb => jsonb: {
                if (!arena.reset(.{ .retain_with_limit = @min(64 * 1024, scratch_limit / 2) }))
                    return if (memory.isExhausted()) context.limit() else error.OutOfMemory;
                const av = json_order.parsePinnedTextLeaky(arena.allocator(), a.bytes, &work) catch |err|
                    return if (err == error.OutOfMemory and memory.isExhausted()) context.limit() else err;
                const bv = json_order.parsePinnedTextLeaky(arena.allocator(), b.bytes, &work) catch |err|
                    return if (err == error.OutOfMemory and memory.isExhausted()) context.limit() else err;
                break :jsonb try json_order.compare(av, bv, &work, 0);
            },
            else => primitive: {
                var au: [36]u8 = undefined;
                var bu: [36]u8 = undefined;
                break :primitive try arrays.compareElement(left.kind, element(left.kind, a.bytes, &au), element(left.kind, b.bytes, &bu), &work);
            },
        };
        if (compared != .eq) return compared;
    }
    var ad: [layout.max_rank]arrays.Dimension = undefined;
    var bd: [layout.max_rank]arrays.Dimension = undefined;
    for (ad[0..left.rank], 0..) |*axis, i| axis.* = try left.dimension(i);
    for (bd[0..right.rank], 0..) |*axis, i| axis.* = try right.dimension(i);
    return arrays.compareShape(left.count, right.count, ad[0..left.rank], bd[0..right.rank], &work);
}

fn element(kind: arrays.ElementType, bytes: []const u8, uuid_bytes: *[36]u8) arrays.Element {
    return arrays.Element.json(switch (kind) {
        .int16 => .{ .integer = std.mem.readInt(i16, bytes[0..2], .little) },
        .int32 => .{ .integer = std.mem.readInt(i32, bytes[0..4], .little) },
        .int64 => .{ .integer = std.mem.readInt(i64, bytes[0..8], .little) },
        .float32 => .{ .float = @as(f32, @bitCast(std.mem.readInt(u32, bytes[0..4], .little))) },
        .float64 => .{ .float = @bitCast(std.mem.readInt(u64, bytes[0..8], .little)) },
        .boolean => .{ .bool = bytes[0] == 1 },
        .text => .{ .string = bytes },
        .uuid => value: {
            uuid_bytes.* = uuid.format(bytes[0..16].*);
            break :value .{ .string = uuid_bytes };
        },
        .numeric, .jsonb => unreachable,
    });
}

fn storedFixture(a: std.mem.Allocator, kind: arrays.ElementType, hex: []const u8) ![]u8 {
    const bytes = try a.alloc(u8, hex.len / 2);
    defer a.free(bytes);
    _ = try std.fmt.hexToBytes(bytes, hex);
    var decoded = try @import("array_binary.zig").decode(a, kind, bytes, .{});
    defer decoded.deinit();
    return @import("array_storage.zig").encodeAlloc(a, decoded.value, .{});
}

test "SQL borrowed canonical array ordering matches PostgreSQL without primitive allocations" {
    const a = std.testing.allocator;
    const Entry = struct { name: []const u8, element_type: arrays.ElementType, left_binary: []const u8, right_binary: []const u8, order: std.math.Order };
    const fixture = try std.json.parseFromSlice(struct { entries: []const Entry }, a, @embedFile("fixtures/sql_array_order_reference.json"), .{ .ignore_unknown_fields = true });
    defer fixture.deinit();
    try std.testing.expectEqual(@as(usize, 36), fixture.value.entries.len);
    for (fixture.value.entries) |entry| {
        const left = try storedFixture(a, entry.element_type, entry.left_binary);
        defer a.free(left);
        const right = try storedFixture(a, entry.element_type, entry.right_binary);
        defer a.free(right);
        const av = try @import("array_storage.zig").validateCanonical(a, entry.element_type, left, .{});
        const bv = try @import("array_storage.zig").validateCanonical(a, entry.element_type, right, .{});
        var denied = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
        var context: exact.Context = .{ .alloc = if (entry.element_type == .jsonb) a else denied.allocator() };
        const actual = try order(av, bv, &context, 1024 * 1024);
        if (actual != entry.order) std.debug.print("array oracle mismatch: {s}\n", .{entry.name});
        try std.testing.expectEqual(entry.order, actual);
        try std.testing.expectEqual(entry.order.invert(), try order(bv, av, &context, 1024 * 1024));
        try std.testing.expectEqual(std.math.Order.eq, try order(av, av, &context, 1024 * 1024));
        try std.testing.expectEqual(@as(usize, 0), denied.alloc_index);
        var decoded_a = try @import("array_storage.zig").decode(a, entry.element_type, left, .{});
        defer decoded_a.deinit();
        var decoded_b = try @import("array_storage.zig").decode(a, entry.element_type, right, .{});
        defer decoded_b.deinit();
        var work: arrays.Budget = .{};
        try std.testing.expectEqual(entry.order, try decoded_a.value.compare(decoded_b.value, &work));
    }
}

test "SQL borrowed canonical array comparison shares sticky cancellation and work admission" {
    const a = std.testing.allocator;
    var payload: [2048]u8 = @splat('x');
    const value = try arrays.Value.init(.text, &.{.{ .length = 2 }}, &.{ arrays.Element.json(.{ .string = &payload }), .{} }, .{});
    const bytes = try @import("array_storage.zig").encodeAlloc(a, value, .{});
    defer a.free(bytes);
    const view = try @import("array_storage.zig").validateCanonical(a, .text, bytes, .{});
    // Distinct pinned owners exercise the text walk. Identical storage now
    // has a constant-time identity path and does not traverse these bytes.
    const other_bytes = try a.dupe(u8, bytes);
    defer a.free(other_bytes);
    const other = try @import("array_storage.zig").validateCanonical(a, .text, other_bytes, .{});
    const Cancel = struct {
        calls: usize = 0,
        fn poll(raw: ?*anyopaque) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.calls += 1;
            if (self.calls == 3) return error.Canceled;
        }
    };
    var cancel: Cancel = .{};
    var context: exact.Context = .{ .alloc = a, .checkpoint = Cancel.poll, .ptr = &cancel };
    try std.testing.expectError(error.Canceled, order(view, other, &context, 0));
    try std.testing.expectEqual(@as(usize, 3), cancel.calls);
    context.checkpoint = null;
    try std.testing.expectError(error.Canceled, order(view, other, &context, 0));
    context = .{ .alloc = a, .remaining = 512 };
    try std.testing.expectError(error.SqlProgramLimitExceeded, order(view, other, &context, 0));
    context.remaining = 8 * 1024 * 1024;
    try std.testing.expectError(error.SqlProgramLimitExceeded, order(view, other, &context, 0));
}

test "SQL borrowed canonical array JSONB comparison reuses bounded scratch and cleans allocation faults" {
    const a = std.testing.allocator;
    var document = try std.json.parseFromSlice(std.json.Value, a, "{\"a\":[1,null,true],\"b\":\"text\"}", .{ .parse_numbers = false });
    defer document.deinit();
    const cells = try a.alloc(arrays.Element, 1000);
    defer a.free(cells);
    @memset(cells, arrays.Element.json(document.value));
    const value = try arrays.Value.init(.jsonb, &.{.{ .length = @intCast(cells.len) }}, cells, .{});
    const bytes = try @import("array_storage.zig").encodeAlloc(a, value, .{});
    defer a.free(bytes);
    const view = try @import("array_storage.zig").validateCanonical(a, .jsonb, bytes, .{});
    var memory: MemoryBudget = .{ .backing = a, .limit = 64 * 1024 };
    var context: exact.Context = .{ .alloc = memory.allocator() };
    try std.testing.expectEqual(std.math.Order.eq, try order(view, view, &context, memory.limit));
    try std.testing.expectEqual(@as(usize, 0), memory.live);
    try std.testing.expect(memory.peak < 16 * 1024);
    std.debug.print("borrowed JSONB array: cells={d} scratch_peak={d} work={d}\n", .{ cells.len, memory.peak, 8 * 1024 * 1024 - context.remaining });
    const Run = struct {
        fn run(alloc: std.mem.Allocator, input: layout.View) !void {
            var work: exact.Context = .{ .alloc = alloc };
            try std.testing.expectEqual(std.math.Order.eq, try order(input, input, &work, 64 * 1024));
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(a, Run.run, .{view});
    context = .{ .alloc = a };
    try std.testing.expectError(error.SqlProgramLimitExceeded, order(view, view, &context, 64));
    context.remaining = 8 * 1024 * 1024;
    try std.testing.expectError(error.SqlProgramLimitExceeded, order(view, view, &context, 64 * 1024));
}

test "SQL borrowed canonical array JSONB comparison borrows large pinned string tokens" {
    const a = std.testing.allocator;
    const text = try a.alloc(u8, 128 * 1024);
    defer a.free(text);
    @memset(text, 'x');
    const value = try arrays.Value.init(.jsonb, &.{.{ .length = 1 }}, &.{arrays.Element.json(.{ .string = text })}, .{});
    const bytes = try @import("array_storage.zig").encodeAlloc(a, value, .{});
    defer a.free(bytes);
    const view = try @import("array_storage.zig").validateCanonical(a, .jsonb, bytes, .{});
    var memory: MemoryBudget = .{ .backing = a, .limit = 64 * 1024 };
    var context: exact.Context = .{ .alloc = memory.allocator() };
    try std.testing.expectEqual(std.math.Order.eq, try order(view, view, &context, memory.limit));
    try std.testing.expect(memory.peak < 16 * 1024);
    try std.testing.expectEqual(@as(usize, 0), memory.live);
}

test "SQL borrowed canonical array primitive comparison scales without decoded cell vectors" {
    const a = std.testing.allocator;
    const cells = try a.alloc(arrays.Element, 10000);
    defer a.free(cells);
    for (cells, 0..) |*cell, i| cell.* = if (i % 3 == 0) .{} else arrays.Element.json(.{ .integer = @intCast(i) });
    const value = try arrays.Value.init(.int64, &.{.{ .length = @intCast(cells.len), .lower = -100 }}, cells, .{});
    const bytes = try @import("array_storage.zig").encodeAlloc(a, value, .{});
    defer a.free(bytes);
    const view = try @import("array_storage.zig").validateCanonical(a, .int64, bytes, .{});
    var denied = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
    var context: exact.Context = .{ .alloc = denied.allocator() };
    const before = std.Io.Clock.now(.awake, std.testing.io).nanoseconds;
    for (0..100) |_| try std.testing.expectEqual(std.math.Order.eq, try order(view, view, &context, 0));
    const elapsed = std.Io.Clock.now(.awake, std.testing.io).nanoseconds - before;
    try std.testing.expectEqual(@as(usize, 0), denied.alloc_index);
    var decoded_memory: MemoryBudget = .{ .backing = a, .limit = 16 * 1024 * 1024 };
    const decoded_before = std.Io.Clock.now(.awake, std.testing.io).nanoseconds;
    for (0..100) |_| {
        var decoded = try @import("array_storage.zig").decode(decoded_memory.allocator(), .int64, bytes, .{});
        defer decoded.deinit();
        var work: arrays.Budget = .{};
        try std.testing.expectEqual(std.math.Order.eq, try decoded.value.compare(decoded.value, &work));
    }
    const decoded_elapsed = std.Io.Clock.now(.awake, std.testing.io).nanoseconds - decoded_before;
    try std.testing.expectEqual(@as(usize, 0), decoded_memory.live);
    try std.testing.expect(decoded_memory.peak >= cells.len * @sizeOf(arrays.Element));
    std.debug.print("borrowed primitive array: comparisons=100 cells={d} decoded_allocations=0 elapsed_ns={d}; one-vector decode baseline peak={d} elapsed_ns={d}\n", .{ cells.len, elapsed, decoded_memory.peak, decoded_elapsed });
}

test "SQL native canonical ordering preserves authenticated views and work" {
    const a = std.testing.allocator;
    for ([_]arrays.ElementType{ .boolean, .int16, .int32, .int64, .float32, .float64, .text }) |kind| {
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const region = arena.allocator();
        const count = 129;
        const cells = try region.alloc(arrays.Element, count);
        for (0..3) |pattern| {
            for (cells, 0..) |*cell, i| cell.* = if (pattern == 2 or (pattern == 1 and i % 7 != 0)) .{} else .json(switch (kind) {
                .boolean => .{ .bool = i % 2 == 0 },
                .int16, .int32, .int64 => .{ .integer = @intCast(i) },
                .float32, .float64 => .{ .float = if (i % 17 == 0) std.math.nan(f64) else if (i % 13 == 0) -0.0 else @floatFromInt(i) },
                .text => .{ .string = if (i % 3 == 0) "é☺" else "other" },
                else => unreachable,
            });
            for (0..4) |variant| {
                const left = try arrays.Value.init(kind, &.{.{ .length = count, .lower = -7 }}, cells, .{});
                const lb = try @import("array_storage.zig").encodeAlloc(region, left, .{});
                const copy = try region.dupe(arrays.Element, cells);
                if (variant == 1) copy[128] = .{};
                if (variant == 2) copy[128] = .json(switch (kind) {
                    .boolean => .{ .bool = false },
                    .float32, .float64 => .{ .float = -99 },
                    .text => .{ .string = "different" },
                    else => .{ .integer = -99 },
                });
                const right = try arrays.Value.init(kind, &.{.{ .length = count, .lower = if (variant == 3) 1 else -7 }}, copy, .{});
                const rb = try @import("array_storage.zig").encodeAlloc(region, right, .{});
                const lv = try layout.View.openAuthenticated(kind, lb, .{});
                const rv = try layout.View.openAuthenticated(kind, rb, .{});
                try std.testing.expect(lv.validation == null and rv.validation == null);
                var fast: exact.Context = .{ .alloc = std.testing.failing_allocator };
                var slow = fast;
                const expected = try orderScalar(lv, rv, &slow, 0);
                try std.testing.expectEqual(expected, try order(lv, rv, &fast, 0));
                try std.testing.expectEqual(slow.remaining, fast.remaining);
                var denied: exact.Context = .{ .alloc = std.testing.failing_allocator, .remaining = (exact.Context{ .alloc = a }).remaining - fast.remaining - 1 };
                try std.testing.expectError(error.SqlProgramLimitExceeded, order(lv, rv, &denied, 0));
            }
        }
    }
}

test "SQL native canonical ordering measurement uses distinct authenticated owners" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const region = arena.allocator();
    const count = 16384;
    const cells = try region.alloc(arrays.Element, count);
    for (cells, 0..) |*cell, i| cell.* = if (i % 7 != 0) .{} else .json(.{ .integer = @intCast(i) });
    const value = try arrays.Value.init(.int64, &.{.{ .length = count, .lower = -7 }}, cells, .{});
    const lb = try @import("array_storage.zig").encodeAlloc(region, value, .{});
    const rb = try region.dupe(u8, lb);
    const left = try layout.View.openAuthenticated(.int64, lb, .{});
    const right = try layout.View.openAuthenticated(.int64, rb, .{});
    try std.testing.expect(left.bytes.ptr != right.bytes.ptr);
    for (0..2) |mode| {
        var elapsed: i96 = 0;
        var total_work: u64 = 0;
        for (0..20) |_| {
            var context: exact.Context = .{ .alloc = std.testing.failing_allocator };
            const start = std.Io.Clock.now(.awake, std.testing.io).nanoseconds;
            const result = if (mode == 0) try orderScalar(left, right, &context, 0) else try order(left, right, &context, 0);
            elapsed += @intCast(std.Io.Clock.now(.awake, std.testing.io).nanoseconds - start);
            total_work += (exact.Context{ .alloc = a }).remaining - context.remaining;
            try std.testing.expectEqual(std.math.Order.eq, result);
        }
        std.debug.print("SQL_NATIVE_CANONICAL native={} repetitions=20 cells={} elapsed_ns={} work={} allocations=0 distinct_owners=true\n", .{ mode == 1, count, elapsed, total_work });
    }
}
