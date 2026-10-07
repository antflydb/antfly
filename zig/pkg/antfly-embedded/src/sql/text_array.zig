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
