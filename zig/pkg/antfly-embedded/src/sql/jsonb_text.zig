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

/// PostgreSQL's observable numeric display, shared with physical JSONB cells.
/// Unlike semantic hashing, this retains the result's decimal display scale.
pub fn number(raw: []const u8, writer: *std.Io.Writer, limit: usize, work: *Budget) !void {
    const layout = try @import("jsonb_number.zig").Layout.init(raw, work);
    try layout.write(writer, limit, work);
}

test "SQL JSONB numeric text bounds normalized weight and preserves zero display scale" {
    const a = std.testing.allocator;
    for ([_][]const u8{ "0e131072", "-0e999999", "0e1073741823" }) |raw| {
        var work: Budget = .{};
        const text = try format(a, .{ .number_string = raw }, 1, &work);
        defer a.free(text);
        try std.testing.expectEqualStrings("0", text);
    }
    for ([_][]const u8{ "0.1e131072", "0.0001e131075" }) |raw| {
        var work: Budget = .{};
        const text = try format(a, .{ .number_string = raw }, 131072, &work);
        defer a.free(text);
        try std.testing.expectEqual(@as(usize, 131072), text.len);
        try std.testing.expectEqual(@as(u8, '1'), text[0]);
        for (text[1..]) |byte| try std.testing.expectEqual(@as(u8, '0'), byte);
    }
    for ([_][]const u8{ "1e131072", "0e-16384", "0e1073741824" }) |raw| {
        var work: Budget = .{};
        try std.testing.expectError(error.SqlNumericOutOfRange, format(a, .{ .number_string = raw }, 1024 * 1024, &work));
    }
    var work: Budget = .{};
    const scaled = try format(a, .{ .number_string = "-0.00" }, 4, &work);
    defer a.free(scaled);
    try std.testing.expectEqualStrings("0.00", scaled);
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
            // An existing JSONB datum has exact logical numeric identity.
            // SQL float-to-JSON conversion chooses its spelling in sqlJson(),
            // before this boundary; rendering must not round a retained DOM.
            const parts = try @import("../common/json_float_decimal.zig").Parts.init(v);
            try work.consume(parts.work());
            var coefficient: [768]u8 = undefined;
            const digits = try parts.coefficientText(&coefficient);
            var buffer: [800]u8 = undefined;
            const raw = std.fmt.bufPrint(&buffer, "{s}{s}e{d}", .{ if (parts.negative) "-" else "", digits, parts.decimalExponent() }) catch unreachable;
            try number(raw, writer, limit, work);
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
