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

//! Borrowed PostgreSQL JSONB numeric admission and display layout. Admission
//! validates the numeric domain without expanding exponents or allocating.
//! Emission alone admits and charges the resulting display bytes.
const std = @import("std");
const Number = @import("../common/json_number.zig").Number;

pub const Layout = struct {
    body: []const u8,
    places: i128,
    digits: usize,
    scale: u16,
    size: usize,
    negative: bool,
    zero: bool,

    pub fn init(raw: []const u8, work: anytype) !Layout {
        var offset: usize = 0;
        while (offset < raw.len) {
            const end = offset + @min(raw.len - offset, 256);
            try work.consume(end - offset);
            offset = end;
        }
        try work.consume(0);
        const parsed = Number.parse(raw) orelse return error.SqlInvalidTextRepresentation;
        const magnitude = std.fmt.parseInt(u32, parsed.exponent, 10) catch return error.SqlNumericOutOfRange;
        if (magnitude > std.math.maxInt(i32) / 2) return error.SqlNumericOutOfRange;
        const exponent: i128 = if (parsed.exponent_negative) -@as(i128, magnitude) else magnitude;
        const start: usize = @intFromBool(parsed.negative);
        const end = std.mem.indexOfAny(u8, raw, "eE") orelse raw.len;
        const body = raw[start..end];
        const point = std.mem.indexOfScalar(u8, body, '.') orelse body.len;
        const digits = body.len - @intFromBool(point != body.len);
        const places = @as(i128, point) + exponent;
        const scale = @max(@as(i128, digits) - places, 0);
        const zero = parsed.first == parsed.end;
        const integer_places: i128 = if (zero) 0 else @max(parsed.adjustment + exponent, 0);
        if (integer_places > 131072 or scale > 16383) return error.SqlNumericOutOfRange;
        const size = @max(integer_places, 1) + scale + @intFromBool(scale != 0) + @intFromBool(parsed.negative and !zero);
        return .{ .body = body, .places = places, .digits = digits, .scale = @intCast(scale), .size = @intCast(size), .negative = parsed.negative and !zero, .zero = zero };
    }

    pub fn write(self: Layout, writer: *std.Io.Writer, limit: usize, work: anytype) !void {
        if (self.size > limit) return error.SqlProgramLimitExceeded;
        try work.consume(self.size);
        if (self.zero) {
            try writer.writeByte('0');
            if (self.scale != 0) {
                try writer.writeByte('.');
                try writer.splatByteAll('0', self.scale);
            }
            return;
        }
        if (self.negative) try writer.writeByte('-');
        var ordinal: i128 = 0;
        var wrote_integer = false;
        for (self.body) |byte| {
            if (byte == '.') continue;
            if (ordinal < self.places and (wrote_integer or byte != '0')) {
                try writer.writeByte(byte);
                wrote_integer = true;
            }
            ordinal += 1;
        }
        if (self.places > self.digits) {
            try writer.splatByteAll('0', @intCast(self.places - @as(i128, self.digits)));
            wrote_integer = true;
        }
        if (!wrote_integer) try writer.writeByte('0');
        if (self.scale == 0) return;
        try writer.writeByte('.');
        if (self.places < 0) try writer.splatByteAll('0', @intCast(-self.places));
        ordinal = 0;
        for (self.body) |byte| {
            if (byte == '.') continue;
            if (ordinal >= self.places) try writer.writeByte(byte);
            ordinal += 1;
        }
    }
};

test "SQL JSONB numeric admission is independent of physical display expansion" {
    const Budget = @import("json_order.zig").Budget;
    var work: Budget = .{ .remaining = 64 };
    const layout = try Layout.init("1e131071", &work);
    try std.testing.expectEqual(@as(usize, 131072), layout.size);
    try std.testing.expectEqual(@as(usize, 56), work.remaining);
    var buffer: [16]u8 = undefined;
    var writer = std.Io.Writer.fixed(&buffer);
    try std.testing.expectError(error.SqlProgramLimitExceeded, layout.write(&writer, 16, &work));
    try std.testing.expectEqual(@as(usize, 0), writer.end);
    work = .{ .remaining = 64 };
    const zero = try Layout.init("-0e1073741823", &work);
    try std.testing.expectEqual(@as(usize, 1), zero.size);
    try zero.write(&writer, 1, &work);
    try std.testing.expectEqualStrings("0", writer.buffered());
    for ([_][]const u8{ "1e131072", "0e-16384", "0e1073741824", "1e-999999999999999999999" }) |raw| {
        work = .{};
        try std.testing.expectError(error.SqlNumericOutOfRange, Layout.init(raw, &work));
    }
}
