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

const std = @import("std");

/// PostgreSQL accepts optional braces and a dash after each four hex digits.
/// Persist and return only the canonical lower-case 8-4-4-4-12 spelling.
pub fn parse(input: []const u8) error{InvalidUuid}![16]u8 {
    const wrapped = input.len >= 2 and input[0] == '{';
    const text = if (wrapped) blk: {
        if (input[input.len - 1] != '}') return error.InvalidUuid;
        break :blk input[1 .. input.len - 1];
    } else input;
    var bytes: [16]u8 = undefined;
    var offset: usize = 0;
    for (&bytes, 0..) |*byte, index| {
        if (offset + 2 > text.len) return error.InvalidUuid;
        const high = std.fmt.charToDigit(text[offset], 16) catch return error.InvalidUuid;
        const low = std.fmt.charToDigit(text[offset + 1], 16) catch return error.InvalidUuid;
        byte.* = (high << 4) | low;
        offset += 2;
        if (index % 2 == 1 and index != 15 and offset < text.len and text[offset] == '-') offset += 1;
    }
    if (offset != text.len) return error.InvalidUuid;
    return bytes;
}

pub fn format(bytes: [16]u8) [36]u8 {
    const hex = "0123456789abcdef";
    var result: [36]u8 = undefined;
    var offset: usize = 0;
    for (bytes, 0..) |byte, index| {
        if (index == 4 or index == 6 or index == 8 or index == 10) {
            result[offset] = '-';
            offset += 1;
        }
        result[offset] = hex[byte >> 4];
        result[offset + 1] = hex[byte & 15];
        offset += 2;
    }
    return result;
}

/// Validate the normalized spelling without decoding or formatting it.
/// This also allows evaluators to borrow canonical inputs with no heap work.
pub fn isCanonical(input: []const u8) bool {
    if (input.len != 36) return false;
    for (input, 0..) |byte, index| {
        if (index == 8 or index == 13 or index == 18 or index == 23) {
            if (byte != '-') return false;
        } else if (!std.ascii.isDigit(byte) and !(byte >= 'a' and byte <= 'f')) return false;
    }
    return true;
}

pub fn canonicalAlloc(alloc: std.mem.Allocator, input: []const u8) ![]u8 {
    const value = format(try parse(input));
    return alloc.dupe(u8, &value);
}

/// RFC 9562 variant only; the version nibble alone is not sufficient.
pub fn version(bytes: [16]u8) ?u4 {
    if (bytes[8] >> 6 != 2) return null;
    return @intCast(bytes[6] >> 4);
}

/// PostgreSQL extracts v7 milliseconds, NOT its implementation-specific
/// sub-millisecond ordering bits. V1 uses 100 ns Gregorian-epoch ticks.
pub fn timestampUnixMicros(bytes: [16]u8) ?i64 {
    return switch (version(bytes) orelse return null) {
        7 => @as(i64, std.mem.readInt(u48, bytes[0..6], .big)) * 1000,
        1 => value: {
            const ticks = @as(u64, std.mem.readInt(u32, bytes[0..4], .big)) |
                (@as(u64, std.mem.readInt(u16, bytes[4..6], .big)) << 32) |
                (@as(u64, std.mem.readInt(u16, bytes[6..8], .big) & 0x0fff) << 48);
            break :value @as(i64, @intCast(ticks / 10)) - 12_219_292_800_000_000;
        },
        else => null,
    };
}

/// One logical clock belongs to the generator owner, never an OS thread.
/// Reserve ascending 12-bit sub-ms instants even if wall time regresses.
/// Processes/restarts have independent clocks and secure random tails: this
/// does not promise globally ordered allocation across nodes or restarts.
pub const Version7Clock = struct {
    last: std.atomic.Value(u64) = .init(0),
    pub const max_millis: u64 = std.math.maxInt(u48);
    pub const max_ticks: u64 = std.math.maxInt(u60);

    pub fn reserve(self: *@This(), unix_ns: i128) error{UuidTimestampOutOfRange}!u60 {
        const ticks: u64 = try ticksForUnixNs(unix_ns);
        var previous = self.last.load(.monotonic);
        while (true) {
            if (previous == max_ticks) return error.UuidTimestampOutOfRange;
            const next = @max(ticks, previous + 1);
            if (self.last.cmpxchgWeak(previous, next, .monotonic, .monotonic)) |actual| {
                previous = actual;
            } else return @intCast(next);
        }
    }

    pub fn ticksForUnixNs(unix_ns: i128) error{UuidTimestampOutOfRange}!u60 {
        if (unix_ns < 0 or @divFloor(unix_ns, 1_000_000) > max_millis) return error.UuidTimestampOutOfRange;
        return @intCast(@divFloor(unix_ns, 1_000_000) * 4096 + @divFloor(@mod(unix_ns, 1_000_000) * 4096, 1_000_000));
    }
};

/// Entropy must come from the invocation's secure I/O source. This pure codec
/// neither invents randomness nor borrows a clock/allocator from global state.
pub fn version7(ticks: u60, entropy: [8]u8) [16]u8 {
    var bytes: [16]u8 = undefined;
    std.mem.writeInt(u48, bytes[0..6], @intCast(ticks >> 12), .big);
    std.mem.writeInt(u16, bytes[6..8], @as(u16, @truncate(ticks & 0xfff)) | 0x7000, .big);
    @memcpy(bytes[8..16], &entropy);
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    return bytes;
}

test "UUID PostgreSQL extraction binds RFC variants and exact epoch precision" {
    const examples = .{
        .{ "019535d9-3df7-79fb-b466-fa907fa17f9e", @as(?u4, 7), @as(?i64, 1_740_365_184_503_000) },
        .{ "c232ab00-9414-11ec-b3c8-9f6bdeced846", @as(?u4, 1), @as(?i64, 1_645_557_742_000_000) },
        .{ "41db1265-8bc1-4ab3-992f-885799a4af1d", @as(?u4, 4), @as(?i64, null) },
        .{ "00000000-0000-7000-8000-000000000000", @as(?u4, 7), @as(?i64, 0) },
        .{ "ffffffff-ffff-7fff-bfff-ffffffffffff", @as(?u4, 7), @as(?i64, 281_474_976_710_655_000) },
        .{ "019535d9-3df7-79fb-7466-fa907fa17f9e", @as(?u4, null), @as(?i64, null) },
    };
    inline for (examples) |example| {
        const bytes = try parse(example[0]);
        try std.testing.expectEqual(example[1], version(bytes));
        try std.testing.expectEqual(example[2], timestampUnixMicros(bytes));
    }
}

test "UUIDv7 logical clocks retain byte ordering across repeated and regressing clocks without allocation" {
    var clock: Version7Clock = .{};
    var prior: [16]u8 = @splat(0);
    for (0..10_000) |i| {
        const ticks = try clock.reserve(1_740_372_384_759_123_456 - @as(i128, @intCast(i)));
        const bytes = version7(ticks, @splat(@truncate(i)));
        try std.testing.expectEqual(@as(?u4, 7), version(bytes));
        try std.testing.expect(std.mem.order(u8, &prior, &bytes) == .lt);
        prior = bytes;
    }
    try std.testing.expectError(error.UuidTimestampOutOfRange, clock.reserve(-1));
    try std.testing.expectError(error.UuidTimestampOutOfRange, clock.reserve((@as(i128, Version7Clock.max_millis) + 1) * 1_000_000));
    clock.last.store(Version7Clock.max_ticks, .monotonic);
    try std.testing.expectError(error.UuidTimestampOutOfRange, clock.reserve(0));
}

test "UUIDv7 shared logical clock reserves unique ordered ticks under std Io concurrency" {
    const workers = 8;
    const count = 4096;
    const State = struct {
        clock: *Version7Clock,
        output: []u60,
        failed: bool = false,
        fn run(self: *@This()) std.Io.Cancelable!void {
            for (self.output) |*value| value.* = self.clock.reserve(1_740_000_000_000_000_000) catch {
                self.failed = true;
                return;
            };
        }
    };
    var clock: Version7Clock = .{};
    const first = try clock.reserve(1_740_000_000_001_000_000);
    const values = try std.testing.allocator.alloc(u60, workers * count);
    defer std.testing.allocator.free(values);
    var states: [workers]State = undefined;
    var group = std.Io.Group.init;
    defer group.cancel(std.testing.io);
    for (&states, 0..) |*state, index| {
        state.* = .{ .clock = &clock, .output = values[index * count ..][0..count] };
        try group.concurrent(std.testing.io, State.run, .{state});
    }
    try group.await(std.testing.io);
    for (states) |state| {
        try std.testing.expect(!state.failed);
        for (state.output[1..], state.output[0 .. count - 1]) |next, prior| try std.testing.expect(next > prior);
    }
    std.mem.sort(u60, values, {}, std.sort.asc(u60));
    for (values, 1..) |value, offset| try std.testing.expectEqual(first + offset, value);
}

test "UUID input variants share one canonical value" {
    const canonical = "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11";
    for ([_][]const u8{
        canonical,
        "A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11",
        "{a0eebc999c0b4ef8bb6d6bb9bd380a11}",
        "a0ee-bc99-9c0b-4ef8-bb6d-6bb9-bd38-0a11",
    }) |input| {
        const value = format(try parse(input));
        try std.testing.expectEqualStrings(canonical, &value);
        try std.testing.expect(isCanonical(&value));
        try std.testing.expectEqual(std.mem.eql(u8, canonical, input), isCanonical(input));
    }
    for ([_][]const u8{ "", "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a1z", "a0eebc99--9c0b-4ef8-bb6d-6bb9bd380a11", "{a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11" }) |input| {
        try std.testing.expectError(error.InvalidUuid, parse(input));
        try std.testing.expect(!isCanonical(input));
    }
}
