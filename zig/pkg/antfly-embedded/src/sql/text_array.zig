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

//! Owned PostgreSQL string_to_array with linear-time, budgeted delimiter
//! matching. Two passes allocate the exact flat shape and owned token strings.
const std = @import("std");
const arrays = @import("array_value.zig");
const A = std.mem.Allocator;

fn writeJoined(a: A, value: *const arrays.Value, delimiter: []const u8, null_text: ?[]const u8, writer: *std.Io.Writer, byte_limit: usize, work: *arrays.Budget) !void {
    var emitted = false;
    for (value.elements) |cell| {
        try work.consume(1);
        if (cell.sql_null and null_text == null) continue;
        if (emitted) {
            try work.consume(delimiter.len);
            try writer.writeAll(delimiter);
        }
        emitted = true;
        if (cell.sql_null) {
            try work.consume(null_text.?.len);
            try writer.writeAll(null_text.?);
            continue;
        }
        switch (value.element_type) {
            .text, .uuid => {
                try work.consume(cell.value.string.len);
                try writer.writeAll(cell.value.string);
            },
            .int16, .int32, .int64 => {
                try work.consume(20);
                try writer.print("{d}", .{cell.value.integer});
            },
            .boolean => try writer.writeAll(if (cell.value.bool) "t" else "f"),
            .float32, .float64 => {
                try work.consume(64);
                var buffer: [64]u8 = undefined;
                const casts = @import("builtin_cast.zig");
                const text = if (value.element_type == .float32) try casts.floatText(f32, @floatCast(cell.value.float), &buffer) else try casts.floatText(f64, cell.value.float, &buffer);
                try writer.writeAll(text);
            },
            .jsonb => try @import("jsonb_text.zig").write(a, cell.value, writer, byte_limit, work, 0),
            .numeric => {
                const numeric = @import("numeric_value.zig");
                var context: numeric.Context = .{ .alloc = a, .remaining = work.remaining, .max_output_bytes = byte_limit };
                defer work.remaining = context.remaining;
                try numeric.write(&context, cell.numeric.?.*, writer);
            },
        }
    }
}

/// Flatten row-major cells without changing bounds or materializing an
/// intermediate string per element. Two passes allocate exactly the output;
/// scratch key directories for JSONB never enter the retained result owner.
pub fn join(a: A, value: *const arrays.Value, delimiter: []const u8, null_text: ?[]const u8, byte_limit: usize, work: *arrays.Budget) ![]const u8 {
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    var counter: std.Io.Writer.Discarding = .init(&.{});
    try writeJoined(arena.allocator(), value, delimiter, null_text, &counter.writer, byte_limit, work);
    if (counter.count > byte_limit) return error.SqlProgramLimitExceeded;
    const bytes = try a.alloc(u8, @intCast(counter.count));
    errdefer a.free(bytes);
    _ = arena.reset(.retain_capacity);
    var writer: std.Io.Writer = .fixed(bytes);
    try writeJoined(arena.allocator(), value, delimiter, null_text, &writer, byte_limit, work);
    std.debug.assert(writer.end == bytes.len);
    return bytes;
}

test "SQL array string output uses one exact allocation for wide primitive arrays" {
    const a = std.testing.allocator;
    const elements = try a.alloc(arrays.Element, 32768);
    defer a.free(elements);
    @memset(elements, arrays.Element.json(.{ .string = "é" }));
    const value = try arrays.Value.init(.text, &.{.{ .length = 32768, .lower = -7 }}, elements, .{ .bytes = 8 * 1024 * 1024 });
    var counted = std.testing.FailingAllocator.init(a, .{ .fail_index = 1 });
    var work: arrays.Budget = .{};
    const started = std.Io.Clock.now(.awake, std.testing.io).nanoseconds;
    const result = try join(counted.allocator(), &value, ",", null, 32768 * 3 - 1, &work);
    defer counted.allocator().free(result);
    try std.testing.expectEqual(@as(usize, 32768 * 3 - 1), result.len);
    try std.testing.expect(std.mem.startsWith(u8, result, "é,é,"));
    try std.testing.expectEqual(@as(usize, 1), counted.allocations);
    std.debug.print("SQL array string: cells=32768 output_bytes={} output_allocations=1 elapsed_ns={}\n", .{ result.len, std.Io.Clock.now(.awake, std.testing.io).nanoseconds - started });
}

const Split = struct {
    text: []const u8,
    delimiter: ?[]const u8,
    prefix: []const usize,
    cursor: usize = 0,
    start: usize = 0,
    matched: usize = 0,
    done: bool = false,
    fn next(self: *Split, work: *arrays.Budget) !?[]const u8 {
        if (self.done or self.text.len == 0) return null;
        const delimiter = self.delimiter orelse {
            if (self.cursor == self.text.len) return null;
            const width = std.unicode.utf8ByteSequenceLength(self.text[self.cursor]) catch return error.SqlInvalidTextRepresentation;
            if (width > self.text.len - self.cursor) return error.SqlInvalidTextRepresentation;
            const result = self.text[self.cursor..][0..width];
            _ = std.unicode.utf8Decode(result) catch return error.SqlInvalidTextRepresentation;
            try work.consume(width);
            self.cursor += width;
            return result;
        };
        if (delimiter.len == 0) {
            self.done = true;
            try work.consume(self.text.len);
            return self.text;
        }
        while (self.cursor < self.text.len) {
            try work.consume(1);
            const byte = self.text[self.cursor];
            while (self.matched != 0 and byte != delimiter[self.matched]) {
                try work.consume(1);
                self.matched = self.prefix[self.matched - 1];
            }
            if (byte == delimiter[self.matched]) self.matched += 1;
            self.cursor += 1;
            if (self.matched == delimiter.len) {
                const result = self.text[self.start .. self.cursor - delimiter.len];
                self.start = self.cursor;
                self.matched = 0;
                return result;
            }
        }
        self.done = true;
        return self.text[self.start..];
    }
};

pub const Result = struct { value: *const arrays.Value, bytes: usize };
pub fn split(a: A, text: []const u8, delimiter: ?[]const u8, null_text: ?[]const u8, byte_limit: usize, work: *arrays.Budget) !Result {
    const prefix_count = if (delimiter) |d| d.len else 0;
    const prefix_bytes = std.math.mul(usize, prefix_count, @sizeOf(usize)) catch return error.SqlProgramLimitExceeded;
    if (prefix_bytes > byte_limit) return error.SqlProgramLimitExceeded;
    const prefix = try a.alloc(usize, prefix_count);
    defer a.free(prefix);
    if (delimiter) |d| if (d.len != 0) {
        prefix[0] = 0;
        var matched: usize = 0;
        for (d[1..], 1..) |byte, i| {
            try work.consume(1);
            while (matched != 0 and byte != d[matched]) {
                try work.consume(1);
                matched = prefix[matched - 1];
            }
            if (byte == d[matched]) matched += 1;
            prefix[i] = matched;
        }
    };
    var probe: Split = .{ .text = text, .delimiter = delimiter, .prefix = prefix };
    var count: usize = 0;
    while (try probe.next(work) != null) {
        count += 1;
        if (count > 65536) return error.SqlProgramLimitExceeded;
    }
    const cells_bytes = std.math.mul(usize, count, @sizeOf(arrays.Element)) catch return error.SqlProgramLimitExceeded;
    const bytes = std.math.add(usize, cells_bytes, text.len + prefix_bytes + @sizeOf(arrays.Value) + @sizeOf(arrays.Dimension)) catch return error.SqlProgramLimitExceeded;
    if (bytes > byte_limit) return error.SqlProgramLimitExceeded;
    const elements = try a.alloc(arrays.Element, count);
    errdefer a.free(elements);
    var initialized: usize = 0;
    errdefer for (elements[0..initialized]) |element| if (!element.sql_null) a.free(element.value.string);
    probe = .{ .text = text, .delimiter = delimiter, .prefix = prefix };
    for (elements) |*element| {
        const token = (try probe.next(work)) orelse return error.InvalidSqlProgram;
        try work.consume(token.len);
        element.* = if (null_text != null and std.mem.eql(u8, token, null_text.?)) .{} else arrays.Element.json(.{ .string = try a.dupe(u8, token) });
        initialized += 1;
    }
    const dimensions = try a.alloc(arrays.Dimension, @intFromBool(count != 0));
    errdefer a.free(dimensions);
    if (count != 0) dimensions[0] = .{ .length = @intCast(count) };
    const value = try a.create(arrays.Value);
    errdefer a.destroy(value);
    value.* = try arrays.Value.initWithBudget(.text, dimensions, elements, .{ .bytes = byte_limit }, work);
    return .{ .value = value, .bytes = bytes };
}
