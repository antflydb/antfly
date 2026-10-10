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

//! PostgreSQL JSONB text output, independent of API JSON serialization.
//! Keys use JSONB's length/byte order; numeric scale survives exponent expansion.
const std = @import("std");
const Json = std.json.Value;
const Budget = @import("json_order.zig").Budget;

/// Canonical SQL text is distinct from API JSON serialization. Preflight
/// through the same writer, then allocate the exact output once. Object-key
/// sorting scratch is reclaimed independently of the retained result owner.
pub fn format(a: std.mem.Allocator, value: Json, limit: usize, work: *Budget) ![]u8 {
    var scratch = std.heap.ArenaAllocator.init(a);
    defer scratch.deinit();
    var counter: std.Io.Writer.Discarding = .init(&.{});
    try write(scratch.allocator(), value, &counter.writer, limit, work, 0);
    if (counter.count > limit) return error.SqlProgramLimitExceeded;
    const output = try a.alloc(u8, @intCast(counter.count));
    errdefer a.free(output);
    _ = scratch.reset(.retain_capacity);
    var writer: std.Io.Writer = .fixed(output);
    try write(scratch.allocator(), value, &writer, limit, work, 0);
    std.debug.assert(writer.end == output.len);
    return output;
}

fn number(raw: []const u8, writer: *std.Io.Writer, limit: usize, work: *Budget) !void {
    _ = @import("../common/json_number.zig").Number.parse(raw) orelse return error.SqlTypeMismatch;
    try work.consume(raw.len);
    const start: usize = @intFromBool(raw[0] == '-');
    const end = std.mem.indexOfAny(u8, raw, "eE") orelse raw.len;
    const body = raw[start..end];
    const point = std.mem.indexOfScalar(u8, body, '.') orelse body.len;
    const digits = body.len - @intFromBool(point != body.len);
    const exponent: i64 = if (end == raw.len) 0 else std.fmt.parseInt(i64, raw[end + 1 ..], 10) catch return error.SqlProgramLimitExceeded;
    const places = std.math.add(i64, @intCast(point), exponent) catch return error.SqlProgramLimitExceeded;
    const scale = if (places < digits) std.math.sub(i64, @intCast(digits), places) catch return error.SqlProgramLimitExceeded else 0;
    if (places > 131072 or scale > 16383) return error.SqlProgramLimitExceeded;
    const size: usize = @intCast(@max(places, 1) + scale + @intFromBool(scale != 0) + @as(i64, @intCast(start)));
    if (size > limit) return error.SqlProgramLimitExceeded;
    try work.consume(size);
    const zero = for (body) |byte| {
        if (byte != '0' and byte != '.') break false;
    } else true;
    if (zero) {
        try writer.writeByte('0');
        if (scale != 0) {
            try writer.writeByte('.');
            try writer.splatByteAll('0', @intCast(scale));
        }
        return;
    }
    if (start != 0) try writer.writeByte('-');
    var ordinal: i64 = 0;
    var wrote_integer = false;
    for (body) |byte| {
        if (byte == '.') continue;
        if (ordinal < places and (wrote_integer or byte != '0')) {
            try writer.writeByte(byte);
            wrote_integer = true;
        }
        ordinal += 1;
    }
    if (places > digits) {
        try writer.splatByteAll('0', @intCast(places - @as(i64, @intCast(digits))));
        wrote_integer = true;
    }
    if (!wrote_integer) try writer.writeByte('0');
    if (scale == 0) return;
    try writer.writeByte('.');
    if (places < 0) try writer.splatByteAll('0', @intCast(-places));
    ordinal = 0;
    for (body) |byte| {
        if (byte == '.') continue;
        if (ordinal >= places) try writer.writeByte(byte);
        ordinal += 1;
    }
}

test "SQL JSONB text formatting owns exact output and cleans allocation and cancellation failures" {
    const a = std.testing.allocator;
    var object: std.json.ObjectMap = .empty;
    defer object.deinit(a);
    const wide: [512]u8 = @splat('a');
    try object.put(a, "zz", .{ .integer = 2 });
    try object.put(a, "a", .{ .string = &wide });
    const value: Json = .{ .object = object };
    const Probe = struct {
        fn run(alloc: std.mem.Allocator, input: Json) !void {
            var work: Budget = .{};
            const text = try format(alloc, input, 4096, &work);
            defer alloc.free(text);
            try std.testing.expect(std.mem.startsWith(u8, text, "{\"a\": \""));
            try std.testing.expect(std.mem.endsWith(u8, text, "\", \"zz\": 2}"));
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(a, Probe.run, .{value});
    var tiny: Budget = .{};
    try std.testing.expectError(error.SqlProgramLimitExceeded, format(a, value, 1, &tiny));
    const Cancel = struct {
        polls: usize = 0,
        fn check(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.polls += 1;
            if (self.polls == 3) return error.QueryCanceled;
        }
    };
    var cancel: Cancel = .{};
    var context: @import("numeric_value.zig").Context = .{ .alloc = a, .checkpoint = Cancel.check, .ptr = &cancel };
    var work: Budget = .{ .shared = &context };
    // The third poll is in the second pass, after output allocation. Both
    // the retained output and canonical-key scratch must unwind on error.
    try std.testing.expectError(error.QueryCanceled, format(a, value, 4096, &work));
    try std.testing.expectEqual(@as(usize, 3), cancel.polls);
}

pub fn write(a: std.mem.Allocator, value: Json, writer: *std.Io.Writer, limit: usize, work: *Budget, depth: usize) anyerror!void {
    if (depth >= 64) return error.SqlProgramLimitExceeded;
    try work.consume(1);
    switch (value) {
        .number_string => |raw| try number(raw, writer, limit, work),
        .float => |v| {
            if (!std.math.isFinite(v)) return error.SqlTypeMismatch;
            var buffer: [64]u8 = undefined;
            try number(try @import("builtin_cast.zig").floatText(f64, v, &buffer), writer, limit, work);
        },
        .string => |bytes| {
            try work.consume(bytes.len);
            if (!std.unicode.utf8ValidateSlice(bytes) or std.mem.indexOfScalar(u8, bytes, 0) != null) return error.SqlTypeMismatch;
            try std.json.Stringify.value(value, .{}, writer);
        },
        .array => |items| {
            try writer.writeByte('[');
            for (items.items, 0..) |item, index| {
                if (index != 0) try writer.writeAll(", ");
                try write(a, item, writer, limit, work, depth + 1);
            }
            try writer.writeByte(']');
        },
        .object => |object| {
            var maximum: usize = 0;
            for (object.keys()) |key| maximum = @max(maximum, key.len);
            const levels = if (object.count() == 0) 0 else std.math.log2_int_ceil(usize, object.count()) + 1;
            const sort_work = std.math.mul(usize, object.count(), levels *| (maximum +| 1)) catch return error.SqlProgramLimitExceeded;
            try work.consume(sort_work);
            const indices = try a.alloc(usize, object.count());
            defer a.free(indices);
            for (indices, 0..) |*index, ordinal| index.* = ordinal;
            std.mem.sort(usize, indices, object.keys(), struct {
                fn less(keys: []const []const u8, left: usize, right: usize) bool {
                    return if (keys[left].len != keys[right].len) keys[left].len < keys[right].len else std.mem.lessThan(u8, keys[left], keys[right]);
                }
            }.less);
            try writer.writeByte('{');
            for (indices, 0..) |index, ordinal| {
                if (ordinal != 0) try writer.writeAll(", ");
                try write(a, .{ .string = object.keys()[index] }, writer, limit, work, depth + 1);
                try writer.writeAll(": ");
                try write(a, object.values()[index], writer, limit, work, depth + 1);
            }
            try writer.writeByte('}');
        },
        else => try std.json.Stringify.value(value, .{}, writer),
    }
}
