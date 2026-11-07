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

//! Prefix-free semantic range keys. NUMERIC display scale is deliberately
//! absent from identity; source range bytes stay in the index entry payload.
//! The lower-only form supports predecessor/successor exclusion checks without
//! scanning every historical interval belonging to an equality-key group.
const std = @import("std");
const ranges = @import("range_value.zig");
const binary = @import("range_binary.zig");

/// Write the same lower-bound ordering component as lowerAlloc directly from
/// canonical physical bytes. Admission/recovery can verify an ordered interval
/// key without allocating NUMERIC limbs or a second copy of an unbounded key.
pub fn writeLowerView(ctx: *ranges.Context, value: binary.View, writer: *std.Io.Writer) !void {
    if (value.empty) return error.InvalidRange;
    try ctx.charge(1);
    if (value.lower) |point| {
        try writer.writeByte(1);
        var escaped: EscapeWriter = .{ .destination = writer, .context = ctx };
        switch (point) {
            .number => |number| try @import("numeric_key.zig").layout.Source.fromCanonicalRow(number).write(&escaped.writer, false, ctx),
            .date => |date| try escaped.writer.writeInt(u32, @as(u32, @bitCast(date.days)) ^ (@as(u32, 1) << 31), .big),
            .timestamp => |stamp| try escaped.writer.writeInt(u64, @as(u64, @bitCast(stamp.micros)) ^ (@as(u64, 1) << 63), .big),
        }
        try writer.writeAll(&.{ 0, 0, if (value.lower_inc) 0 else 1 });
    } else try writer.writeByte(0);
}

const EscapeWriter = struct {
    writer: std.Io.Writer = .{ .vtable = &.{ .drain = drain }, .buffer = &.{} },
    destination: *std.Io.Writer,
    context: *ranges.Context,
    count: usize = 0,
    fn drain(raw: *std.Io.Writer, data: []const []const u8, splat: usize) std.Io.Writer.Error!usize {
        const self: *@This() = @alignCast(@fieldParentPtr("writer", raw));
        var consumed: usize = 0;
        for (data, 0..) |part, index| {
            const repetitions = if (index + 1 == data.len) splat else 1;
            for (0..repetitions) |_| for (part) |byte| {
                const bytes: usize = if (byte == 0) 2 else 1;
                if (bytes > self.context.max_output_bytes -| self.count) {
                    self.context.failure = self.context.limit();
                    return error.WriteFailed;
                }
                self.count += bytes;
                try self.destination.writeByte(byte);
                if (byte == 0) try self.destination.writeByte(255);
                consumed += 1;
            };
        }
        return consumed;
    }
};

const Output = struct {
    context: *ranges.Context,
    bytes: std.ArrayList(u8) = .empty,
    fn put(self: *@This(), input: []const u8) !void {
        if (input.len > self.context.max_output_bytes -| self.bytes.items.len) return self.context.limit();
        try self.context.charge(input.len);
        try self.bytes.appendSlice(self.context.alloc, input);
    }
    fn escaped(self: *@This(), input: []const u8) !void {
        for (input) |byte| {
            try self.put(&.{byte});
            if (byte == 0) try self.put(&.{255});
        }
        try self.put(&.{ 0, 0 });
    }
    fn endpoint(self: *@This(), point: ranges.Endpoint) !void {
        var fixed: [8]u8 = undefined;
        const bytes: []const u8 = switch (point) {
            .number => |value| try @import("numeric_key.zig").encodeAlloc(self.context, value),
            .date => |value| block: {
                std.mem.writeInt(u32, fixed[0..4], @as(u32, @bitCast(value.days)) ^ (@as(u32, 1) << 31), .big);
                break :block fixed[0..4];
            },
            .timestamp => |value| block: {
                std.mem.writeInt(u64, &fixed, @as(u64, @bitCast(value.micros)) ^ (@as(u64, 1) << 63), .big);
                break :block &fixed;
            },
        };
        defer if (point == .number) self.context.alloc.free(bytes);
        try self.escaped(bytes);
    }
    fn lower(self: *@This(), value: ranges.Value) !void {
        if (value.lower) |point| {
            try self.put(&.{1});
            try self.endpoint(point);
            try self.put(&.{if (value.lower_inc) 0 else 1});
        } else try self.put(&.{0});
    }
    fn upper(self: *@This(), value: ranges.Value) !void {
        if (value.upper) |point| {
            try self.put(&.{0});
            try self.endpoint(point);
            try self.put(&.{if (value.upper_inc) 1 else 0});
        } else try self.put(&.{1});
    }
};
pub fn lowerAlloc(ctx: *ranges.Context, input: ranges.Value) ![]u8 {
    const value = try ranges.canonical(ctx, input);
    if (value.empty) return error.InvalidRange;
    var out: Output = .{ .context = ctx };
    errdefer out.bytes.deinit(ctx.alloc);
    try out.lower(value);
    return out.bytes.toOwnedSlice(ctx.alloc);
}
pub fn encodeAlloc(ctx: *ranges.Context, input: ranges.Value) ![]u8 {
    const value = try ranges.canonical(ctx, input);
    var out: Output = .{ .context = ctx };
    errdefer out.bytes.deinit(ctx.alloc);
    try out.put(&.{ @backingInt(value.kind), if (value.empty) 0 else 1 });
    if (!value.empty) {
        try out.lower(value);
        try out.upper(value);
    }
    return out.bytes.toOwnedSlice(ctx.alloc);
}

const Reader = struct {
    context: *ranges.Context,
    bytes: []const u8,
    pos: usize = 0,
    mask: u8 = 0,
    fn byte(self: *@This()) !u8 {
        if (self.pos == self.bytes.len) return error.InvalidRelationalIndexKey;
        try self.context.charge(1);
        const value = self.bytes[self.pos] ^ self.mask;
        self.pos += 1;
        return value;
    }
    fn endpoint(self: *@This(), kind: ranges.Kind) !ranges.Endpoint {
        var bytes: std.ArrayList(u8) = .empty;
        defer bytes.deinit(self.context.alloc);
        while (true) {
            const value = try self.byte();
            if (value == 0) {
                const next = try self.byte();
                if (next == 0) break;
                if (next != 255) return error.InvalidRelationalIndexKey;
            }
            if (bytes.items.len >= self.context.max_output_bytes) return self.context.limit();
            try bytes.append(self.context.alloc, value);
        }
        return switch (kind) {
            .numrange => .{ .number = (try @import("numeric_key.zig").decode(self.context, bytes.items)).value },
            .daterange => block: {
                if (bytes.items.len != 4) return error.InvalidRelationalIndexKey;
                const signed: i32 = @bitCast(std.mem.readInt(u32, bytes.items[0..4], .big) ^ (@as(u32, 1) << 31));
                break :block .{ .date = try @import("temporal_value.zig").Date.init(signed) };
            },
            .tsrange, .tstzrange => block: {
                if (bytes.items.len != 8) return error.InvalidRelationalIndexKey;
                const signed: i64 = @bitCast(std.mem.readInt(u64, bytes.items[0..8], .big) ^ (@as(u64, 1) << 63));
                break :block .{ .timestamp = try @import("temporal_value.zig").Timestamp.init(signed) };
            },
        };
    }
};
pub const Decoded = struct { value: ranges.Value, consumed: usize };
pub fn decodePrefixLeaky(ctx: *ranges.Context, kind: ranges.Kind, bytes: []const u8, descending: bool) !Decoded {
    if (bytes.len > ctx.max_input_bytes) return ctx.limit();
    var reader: Reader = .{ .context = ctx, .bytes = bytes, .mask = if (descending) 255 else 0 };
    if (try reader.byte() != @backingInt(kind)) return error.InvalidRelationalIndexKey;
    const nonempty = try reader.byte();
    if (nonempty == 0) return .{ .value = .{ .kind = kind, .empty = true }, .consumed = reader.pos };
    if (nonempty != 1) return error.InvalidRelationalIndexKey;
    var value: ranges.Value = .{ .kind = kind };
    const lower = try reader.byte();
    if (lower > 1) return error.InvalidRelationalIndexKey;
    if (lower == 1) {
        value.lower = try reader.endpoint(kind);
        const inclusive = try reader.byte();
        if (inclusive > 1) return error.InvalidRelationalIndexKey;
        value.lower_inc = inclusive == 0;
    }
    const upper = try reader.byte();
    if (upper > 1) return error.InvalidRelationalIndexKey;
    if (upper == 0) {
        value.upper = try reader.endpoint(kind);
        const inclusive = try reader.byte();
        if (inclusive > 1) return error.InvalidRelationalIndexKey;
        value.upper_inc = inclusive == 1;
    }
    const canonical = try ranges.canonical(ctx, value);
    if (canonical.flags() != value.flags()) return error.InvalidRelationalIndexKey;
    return .{ .value = canonical, .consumed = reader.pos };
}

/// Structural skip used by an index tuple directory. No endpoint allocation
/// or numeric formatting is needed to locate the next key component.
pub fn prefixLen(kind: ranges.Kind, bytes: []const u8, descending: bool) !usize {
    const mask: u8 = if (descending) 255 else 0;
    if (bytes.len < 2 or bytes[0] ^ mask != @backingInt(kind)) return error.InvalidRelationalIndexKey;
    if (bytes[1] ^ mask == 0) return 2;
    if (bytes[1] ^ mask != 1) return error.InvalidRelationalIndexKey;
    var pos: usize = 2;
    for (0..2) |side| {
        if (pos == bytes.len) return error.InvalidRelationalIndexKey;
        const present = bytes[pos] ^ mask;
        pos += 1;
        if (present > 1) return error.InvalidRelationalIndexKey;
        if (present == side) continue;
        var logical_bytes: usize = 0;
        while (true) {
            if (bytes.len - pos < 2) return error.InvalidRelationalIndexKey;
            const byte = bytes[pos] ^ mask;
            pos += 1;
            if (byte == 0) {
                const next = bytes[pos] ^ mask;
                pos += 1;
                if (next == 0) break;
                if (next != 255) return error.InvalidRelationalIndexKey;
            }
            logical_bytes += 1;
        }
        if (logical_bytes == 0 or (kind == .daterange and logical_bytes != 4) or ((kind == .tsrange or kind == .tstzrange) and logical_bytes != 8)) return error.InvalidRelationalIndexKey;
        if (pos == bytes.len or bytes[pos] ^ mask > 1) return error.InvalidRelationalIndexKey;
        pos += 1;
    }
    return pos;
}

test "SQL range semantic keys preserve PostgreSQL ordering across all bound identities" {
    const a = std.testing.allocator;
    const fixture = try std.json.parseFromSlice(struct { relations: []const struct { kind: ranges.Kind, left: []const u8, right: []const u8, equal: bool, less: bool } }, a, @embedFile("fixtures/sql_range_reference.json"), .{ .ignore_unknown_fields = true });
    defer fixture.deinit();
    for (fixture.value.relations) |entry| {
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        var ctx: ranges.Context = .{ .alloc = arena.allocator() };
        const left = try ranges.parseLeaky(&ctx, entry.kind, entry.left);
        const right = try ranges.parseLeaky(&ctx, entry.kind, entry.right);
        const lk = try encodeAlloc(&ctx, left);
        const rk = try encodeAlloc(&ctx, right);
        const decoded = try decodePrefixLeaky(&ctx, entry.kind, lk, false);
        try std.testing.expectEqual(lk.len, decoded.consumed);
        try std.testing.expectEqual(lk.len, try prefixLen(entry.kind, lk, false));
        try std.testing.expectEqualSlices(u8, lk, try encodeAlloc(&ctx, decoded.value));
        const descending = try ctx.alloc.dupe(u8, lk);
        for (descending) |*byte| byte.* = ~byte.*;
        const decoded_descending = try decodePrefixLeaky(&ctx, entry.kind, descending, true);
        try std.testing.expectEqual(lk.len, try prefixLen(entry.kind, descending, true));
        try std.testing.expectEqualSlices(u8, lk, try encodeAlloc(&ctx, decoded_descending.value));
        const compared = std.mem.order(u8, lk, rk);
        errdefer std.debug.print("range key {s}: {s} / {s}\n", .{ @tagName(entry.kind), entry.left, entry.right });
        try std.testing.expectEqual(entry.equal, compared == .eq);
        try std.testing.expectEqual(entry.less, compared == .lt);
    }
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    var ctx: ranges.Context = .{ .alloc = arena.allocator() };
    const l = try ranges.parseLeaky(&ctx, .numrange, "[1.00,2.000)");
    const r = try ranges.parseLeaky(&ctx, .numrange, "[1,2)");
    try std.testing.expectEqualSlices(u8, try encodeAlloc(&ctx, l), try encodeAlloc(&ctx, r));
}
