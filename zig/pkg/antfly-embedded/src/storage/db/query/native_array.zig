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

//! Borrowed projection of an admitted flat SQL-array payload. No element vector
//! or JSON envelope is materialized; only the selected scalar may own scratch.
const std = @import("std");
const storage = @import("../../../sql/array_storage.zig");
const arrays = @import("../../../sql/array_value.zig");
const A = std.mem.Allocator;
pub const Part = enum { whole, dimensions, values, sql_nulls };
const Counter = struct {
    writer: std.Io.Writer = undefined,
    limit: usize,
    count: usize = 0,
    fn initWriter(self: *@This()) *std.Io.Writer {
        self.writer = .{ .vtable = &.{ .drain = drain }, .buffer = &.{} };
        return &self.writer;
    }
    fn drain(writer: *std.Io.Writer, data: []const []const u8, splat: usize) std.Io.Writer.Error!usize {
        const self: *@This() = @alignCast(@fieldParentPtr("writer", writer));
        var n: usize = 0;
        for (data[0 .. data.len - 1]) |bytes| n = std.math.add(usize, n, bytes.len) catch return error.WriteFailed;
        n = std.math.add(usize, n, std.math.mul(usize, data[data.len - 1].len, splat) catch return error.WriteFailed) catch return error.WriteFailed;
        if (n > self.limit - self.count) return error.WriteFailed;
        self.count += n;
        return n;
    }
};
pub const Prepared = struct {
    view: storage.layout.View,
    sizes: [4]?usize = @splat(null),

    pub fn init(a: A, kind: arrays.ElementType, bytes: []const u8, authenticated: bool) !Prepared {
        return .{ .view = if (authenticated) try storage.layout.View.openAuthenticated(kind, bytes, .{}) else try storage.validateCanonical(a, kind, bytes, .{}) };
    }
    pub fn length(self: *const Prepared, part: Part) usize {
        return if (part == .dimensions) self.view.rank else self.view.count;
    }
    pub fn size(self: *Prepared, a: A, part: Part) !usize {
        const slot = &self.sizes[@backingInt(part)];
        if (slot.*) |n| return n;
        const limit = @min(8 * 1024 * 1024, ((arrays.Limits{}).work -| self.view.count) / 2);
        const n = try self.measure(a, part, limit);
        slot.* = n;
        return n;
    }
    fn add(total: *usize, n: usize, limit: usize) !void {
        if (n > limit - total.*) return error.SqlProgramLimitExceeded;
        total.* += n;
    }
    fn integerSize(integer: i64) usize {
        const negative = integer < 0;
        var digits: usize = if (negative) 2 else 1;
        var n: u64 = @abs(integer);
        while (n >= 10) : (n /= 10) digits += 1;
        return digits;
    }
    fn stringSize(bytes: []const u8, limit: usize) !usize {
        var n: usize = 2;
        if (n > limit) return error.SqlProgramLimitExceeded;
        for (bytes) |byte| try add(&n, switch (byte) {
            '"', '\\', '\n', '\r', '\t', 8, 12 => 2,
            0...7, 11, 14...31 => 6,
            else => 1,
        }, limit);
        return n;
    }
    /// Size primitive domains from admitted cell descriptors instead of running
    /// the serializer into an unbuffered counter. Float/NUMERIC formatting stays
    /// with the standard codec; all domains retain the same bounded admission.
    fn measure(self: *const Prepared, a: A, part: Part, limit: usize) anyerror!usize {
        if (part == .whole) {
            var n: usize = "{\"dimensions\":,\"values\":,\"sql_nulls\":}".len;
            if (n > limit) return error.SqlProgramLimitExceeded;
            for ([_]Part{ .dimensions, .values, .sql_nulls }) |child| try add(&n, try self.measure(a, child, limit - n), limit);
            return n;
        }
        if (part == .values and (self.view.kind == .float32 or self.view.kind == .float64 or self.view.kind == .numeric)) {
            var counter: Counter = .{ .limit = limit };
            self.write(a, counter.initWriter(), part) catch |err| return if (err == error.WriteFailed) error.SqlProgramLimitExceeded else err;
            return counter.count;
        }
        var n: usize = 2;
        if (n > limit) return error.SqlProgramLimitExceeded;
        for (0..self.length(part)) |i| {
            if (i != 0) try add(&n, 1, limit);
            const wire_size: usize = switch (part) {
                .whole => unreachable,
                .dimensions => blk: {
                    const d = try self.view.dimension(i);
                    break :blk "{\"length\":,\"lower_bound\":}".len + integerSize(d.length) + integerSize(d.lower);
                },
                .sql_nulls => if ((try self.view.cell(i)).sql_null) 4 else 5,
                .values => blk: {
                    const cell = try self.view.cell(i);
                    if (cell.sql_null) break :blk 4;
                    break :blk switch (self.view.kind) {
                        .int16 => 2 + integerSize(std.mem.readInt(i16, cell.bytes[0..2], .little)),
                        .int32 => 2 + integerSize(std.mem.readInt(i32, cell.bytes[0..4], .little)),
                        .int64 => 2 + integerSize(std.mem.readInt(i64, cell.bytes[0..8], .little)),
                        .text => try stringSize(cell.bytes, limit - n),
                        .boolean => if (cell.bytes[0] == 1) 4 else 5,
                        .uuid => 38,
                        .jsonb => cell.bytes.len,
                        else => unreachable,
                    };
                },
            };
            try add(&n, wire_size, limit);
        }
        return n;
    }
    pub fn value(self: *const Prepared, a: A, index: usize) !std.json.Value {
        var ctx: @import("../../../sql/numeric_value.zig").Context = .{ .alloc = a, .remaining = (arrays.Limits{}).work };
        return self.valueWithContext(a, index, &ctx);
    }
    fn valueWithContext(self: *const Prepared, a: A, index: usize, ctx: *@import("../../../sql/numeric_value.zig").Context) !std.json.Value {
        const cell = try self.view.cell(index);
        if (cell.sql_null) return .null;
        return switch (self.view.kind) {
            .int16 => .{ .string = try std.fmt.allocPrint(a, "{d}", .{std.mem.readInt(i16, cell.bytes[0..2], .little)}) },
            .int32 => .{ .string = try std.fmt.allocPrint(a, "{d}", .{std.mem.readInt(i32, cell.bytes[0..4], .little)}) },
            .int64 => .{ .string = try std.fmt.allocPrint(a, "{d}", .{std.mem.readInt(i64, cell.bytes[0..8], .little)}) },
            .text => .{ .string = cell.bytes },
            .boolean => .{ .bool = cell.bytes[0] == 1 },
            .uuid => .{ .string = try a.dupe(u8, &@import("../../../common/uuid.zig").format(cell.bytes[0..16].*)) },
            .float32 => float(@as(f32, @bitCast(std.mem.readInt(u32, cell.bytes[0..4], .little)))),
            .float64 => float(@bitCast(std.mem.readInt(u64, cell.bytes[0..8], .little))),
            .numeric => blk: {
                const numeric = @import("../../../sql/numeric_value.zig");
                var decoded = try @import("../../../sql/numeric_binary.zig").decodeCanonical(ctx, cell.bytes);
                defer decoded.deinit();
                break :blk .{ .string = try numeric.format(ctx, decoded.value) };
            },
            // Navigation uses a borrowed JsonView instead. This fallback is for
            // callers explicitly requesting a logical scalar/container.
            .jsonb => try std.json.parseFromSliceLeaky(std.json.Value, a, cell.bytes, .{ .allocate = .alloc_if_needed, .parse_numbers = false }),
        };
    }
    fn float(n: f64) std.json.Value {
        return if (std.math.isFinite(n)) .{ .float = n } else .{ .string = if (std.math.isNan(n)) "NaN" else if (n < 0) "-Infinity" else "Infinity" };
    }
    pub fn write(self: *const Prepared, a: A, writer: *std.Io.Writer, part: Part) anyerror!void {
        if (part == .whole) {
            try writer.writeAll("{\"dimensions\":");
            try self.write(a, writer, .dimensions);
            try writer.writeAll(",\"values\":");
            try self.write(a, writer, .values);
            try writer.writeAll(",\"sql_nulls\":");
            try self.write(a, writer, .sql_nulls);
            return writer.writeByte('}');
        }
        try writer.writeByte('[');
        var scratch = std.heap.ArenaAllocator.init(@import("antfly_platform").allocator.reclaimingScratch(a));
        defer scratch.deinit();
        var ctx: @import("../../../sql/numeric_value.zig").Context = .{ .alloc = scratch.allocator(), .remaining = (arrays.Limits{}).work };
        for (0..self.length(part)) |i| {
            if (i != 0) try writer.writeByte(',');
            switch (part) {
                .whole => unreachable,
                .dimensions => {
                    const d = try self.view.dimension(i);
                    try std.json.Stringify.value(.{ .length = d.length, .lower_bound = d.lower }, .{}, writer);
                },
                .sql_nulls => try writer.writeAll(if ((try self.view.cell(i)).sql_null) "true" else "false"),
                .values => {
                    const cell = try self.view.cell(i);
                    if (cell.sql_null) {
                        try writer.writeAll("null");
                    } else if (self.view.kind == .jsonb) {
                        try writer.writeAll(cell.bytes);
                    } else if (self.view.kind == .int16 or self.view.kind == .int32 or self.view.kind == .int64) {
                        const n: i64 = switch (self.view.kind) {
                            .int16 => std.mem.readInt(i16, cell.bytes[0..2], .little),
                            .int32 => std.mem.readInt(i32, cell.bytes[0..4], .little),
                            .int64 => std.mem.readInt(i64, cell.bytes[0..8], .little),
                            else => unreachable,
                        };
                        try writer.print("\"{d}\"", .{n});
                    } else {
                        if (!scratch.reset(.{ .retain_with_limit = 4096 })) return error.OutOfMemory;
                        try std.json.Stringify.value(try self.valueWithContext(scratch.allocator(), i, &ctx), .{}, writer);
                    }
                },
            }
        }
        try writer.writeByte(']');
    }
};

test "public graph metric borrowed wire borrowed SQL arrays preserve PostgreSQL wire goldens" {
    const a = std.testing.allocator;
    const Fixture = struct { entries: []const struct { element_type: arrays.ElementType, binary: []const u8 } };
    const fixture = try std.json.parseFromSlice(Fixture, a, @embedFile("../../../sql/fixtures/sql_array_binary_reference.json"), .{ .ignore_unknown_fields = true });
    defer fixture.deinit();
    for (fixture.value.entries) |entry| {
        const bytes = try a.alloc(u8, entry.binary.len / 2);
        defer a.free(bytes);
        _ = try std.fmt.hexToBytes(bytes, entry.binary);
        var decoded = try @import("../../../sql/array_binary.zig").decode(a, entry.element_type, bytes, .{});
        defer decoded.deinit();
        const stored = try storage.encodeAlloc(a, decoded.value, .{});
        defer a.free(stored);
        var admitted = try storage.decode(a, entry.element_type, stored, .{});
        defer admitted.deinit();
        const expected = try @import("../../../sql/array_wire.zig").encodeAlloc(a, admitted.value, .{});
        defer a.free(expected);
        const Check = struct {
            fn run(scratch: A, kind: arrays.ElementType, raw: []const u8, want: []const u8) !void {
                var prepared = try Prepared.init(scratch, kind, raw, false);
                try std.testing.expectEqual(want.len, try prepared.size(scratch, .whole));
                var writer = std.Io.Writer.Allocating.init(std.testing.allocator);
                defer writer.deinit();
                try prepared.write(scratch, &writer.writer, .whole);
                try std.testing.expectEqualStrings(want, writer.written());
            }
        };
        try Check.run(a, entry.element_type, stored, expected);
        try @import("antfly_platform").allocator.checkAllAllocationFailures(a, Check.run, .{ entry.element_type, stored, expected });
    }
}

test "public graph metric borrowed wire native SQL array preparation enforces bounded wire admission" {
    const a = std.testing.allocator;
    const cells = try a.alloc(arrays.Element, 65536);
    defer a.free(cells);
    for (cells) |*cell| cell.* = arrays.Element.json(.{ .integer = std.math.maxInt(i64) });
    const array = try arrays.Value.init(.int64, &.{.{ .length = 65536 }}, cells, .{});
    const bytes = try storage.encodeAlloc(a, array, .{});
    defer a.free(bytes);
    var prepared = try Prepared.init(a, .int64, bytes, true);
    var budget: @import("../../lite/test_allocator.zig").BudgetAllocator = .{ .backing = a, .limit = 0 };
    try std.testing.expectError(error.SqlProgramLimitExceeded, prepared.size(budget.allocator(), .whole));
    try std.testing.expect(prepared.sizes[@backingInt(Part.whole)] == null);
    try std.testing.expectEqual(@as(usize, 0), budget.alloc_attempts);
    // Tiny selected metadata remains usable even when the whole wire envelope
    // cannot be admitted. Failed sizing never poisons another part's cache.
    try std.testing.expect((try prepared.size(budget.allocator(), .dimensions)) < 100);
}

test "public graph metric borrowed wire primitive array lengths match every escape and integer boundary" {
    const a = std.testing.allocator;
    var all: [127]u8 = undefined;
    for (0..127) |i| all[i] = @intCast(i + 1);
    const text = "quote\" slash\\ newline\n return\r tab\t back\x08 form\x0c ctrl\x01 é中";
    for ([_][]const u8{ text, all[0..], "" }) |value| {
        const array = try arrays.Value.init(.text, &.{.{ .length = 2, .lower = -3 }}, &.{ arrays.Element.json(.{ .string = value }), .{ .value = .null, .sql_null = true } }, .{});
        const bytes = try storage.encodeAlloc(a, array, .{});
        defer a.free(bytes);
        var prepared = try Prepared.init(a, .text, bytes, true);
        for ([_]Part{ .whole, .dimensions, .values, .sql_nulls }) |part| {
            var output = std.Io.Writer.Allocating.init(a);
            defer output.deinit();
            try prepared.write(a, &output.writer, part);
            try std.testing.expectEqual(output.written().len, try prepared.size(a, part));
        }
    }
    for ([_]i64{ 0, 9, 10, -1, -9, -10, std.math.minInt(i64), std.math.maxInt(i64) }) |value| {
        var buffer: [32]u8 = undefined;
        const written = try std.fmt.bufPrint(&buffer, "{d}", .{value});
        try std.testing.expectEqual(written.len, Prepared.integerSize(value));
    }
}
