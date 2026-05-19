// Copyright 2026 Antfly, Inc.
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

//! Protobuf wire-format primitives (encode + decode).
//!
//! Shared by lib/pjrt and
//! potentially lib/vector and lib/tokenizer in the future.
//! Only implements raw wire-level operations — message-specific
//! field dispatch stays in each consumer.

const std = @import("std");
const builtin = @import("builtin");
const Allocator = std.mem.Allocator;

/// Protobuf wire types.
pub const WireType = enum(u3) {
    varint = 0,
    fixed64 = 1,
    length_delimited = 2,
    fixed32 = 5,
};

/// Growable byte buffer for encoding.
pub const Buf = std.ArrayListUnmanaged(u8);

// ---------------------------------------------------------------------------
// Encode — low-level
// ---------------------------------------------------------------------------

pub fn writeVarint(alloc: Allocator, buf: *Buf, value: u64) !void {
    var v = value;
    while (v >= 0x80) {
        try buf.append(alloc, @as(u8, @truncate(v)) | 0x80);
        v >>= 7;
    }
    try buf.append(alloc, @truncate(v));
}

pub fn writeTag(alloc: Allocator, buf: *Buf, field: u32, wt: WireType) !void {
    try writeVarint(alloc, buf, @as(u64, field) << 3 | @intFromEnum(wt));
}

// ---------------------------------------------------------------------------
// Encode — field writers (tag + value)
// ---------------------------------------------------------------------------

pub fn writeUint64(alloc: Allocator, buf: *Buf, field: u32, value: u64) !void {
    try writeTag(alloc, buf, field, .varint);
    try writeVarint(alloc, buf, value);
}

pub fn writeInt32(alloc: Allocator, buf: *Buf, field: u32, value: i32) !void {
    try writeTag(alloc, buf, field, .varint);
    try writeVarint(alloc, buf, @bitCast(@as(i64, value)));
}

pub fn writeFixed32(alloc: Allocator, buf: *Buf, field: u32, value: u32) !void {
    try writeTag(alloc, buf, field, .fixed32);
    try writeFixed32Raw(alloc, buf, value);
}

pub fn writeFixed64(alloc: Allocator, buf: *Buf, field: u32, value: u64) !void {
    try writeTag(alloc, buf, field, .fixed64);
    try writeFixed64Raw(alloc, buf, value);
}

pub fn writeString(alloc: Allocator, buf: *Buf, field: u32, value: []const u8) !void {
    try writeTag(alloc, buf, field, .length_delimited);
    try writeVarint(alloc, buf, value.len);
    try buf.appendSlice(alloc, value);
}

pub fn writeBytes(alloc: Allocator, buf: *Buf, field: u32, value: []const u8) !void {
    try writeString(alloc, buf, field, value);
}

pub fn writeMessage(alloc: Allocator, buf: *Buf, field: u32, content: []const u8) !void {
    try writeTag(alloc, buf, field, .length_delimited);
    try writeVarint(alloc, buf, content.len);
    try buf.appendSlice(alloc, content);
}

pub fn writeMapEntry(alloc: Allocator, buf: *Buf, field: u32, key: []const u8, val: []const u8) !void {
    try writeTag(alloc, buf, field, .length_delimited);
    try writeVarint(alloc, buf, key.len + val.len);
    try buf.appendSlice(alloc, key);
    try buf.appendSlice(alloc, val);
}

pub fn writePackedFloats(alloc: Allocator, buf: *Buf, field: u32, values: []const f32) !void {
    try writeTag(alloc, buf, field, .length_delimited);
    try writeVarint(alloc, buf, values.len * 4);
    if (builtin.target.cpu.arch.endian() == .little) {
        try buf.appendSlice(alloc, std.mem.sliceAsBytes(values));
    } else {
        for (values) |v| try writeFixed32Raw(alloc, buf, @bitCast(v));
    }
}

pub fn writePackedInt64s(alloc: Allocator, buf: *Buf, field: u32, values: []const i64) !void {
    var payload_len: usize = 0;
    for (values) |v| payload_len += varintSize(@bitCast(v));

    try writeTag(alloc, buf, field, .length_delimited);
    try writeVarint(alloc, buf, payload_len);
    for (values) |v| try writeVarint(alloc, buf, @bitCast(v));
}

// ---------------------------------------------------------------------------
// Encode — helpers
// ---------------------------------------------------------------------------

pub fn varintSize(value: u64) usize {
    if (value == 0) return 1;
    var v = value;
    var size: usize = 0;
    while (v > 0) {
        size += 1;
        v >>= 7;
    }
    return size;
}

fn writeFixed32Raw(alloc: Allocator, buf: *Buf, value: u32) !void {
    const bytes: [4]u8 = @bitCast(std.mem.nativeToLittle(u32, value));
    try buf.appendSlice(alloc, &bytes);
}

fn writeFixed64Raw(alloc: Allocator, buf: *Buf, value: u64) !void {
    const bytes: [8]u8 = @bitCast(std.mem.nativeToLittle(u64, value));
    try buf.appendSlice(alloc, &bytes);
}

// ---------------------------------------------------------------------------
// Decode
// ---------------------------------------------------------------------------

pub const DecodeError = error{ Overflow, EndOfStream, InvalidWireType };

pub const Tag = struct {
    field: u32,
    wire_type: WireType,
};

pub fn readVarint(bytes: []const u8, pos: *usize) DecodeError!u64 {
    var result: u64 = 0;
    var shift: u6 = 0;
    while (pos.* < bytes.len) {
        const b = bytes[pos.*];
        pos.* += 1;
        result |= @as(u64, b & 0x7F) << shift;
        if (b & 0x80 == 0) return result;
        shift = std.math.add(u6, shift, 7) catch return error.Overflow;
    }
    return error.EndOfStream;
}

pub fn readTag(bytes: []const u8, pos: *usize) DecodeError!Tag {
    const v = try readVarint(bytes, pos);
    const raw_wire_type: u3 = @intCast(v & 0x7);
    return .{
        .field = @intCast(v >> 3),
        .wire_type = switch (raw_wire_type) {
            @intFromEnum(WireType.varint) => .varint,
            @intFromEnum(WireType.fixed64) => .fixed64,
            @intFromEnum(WireType.length_delimited) => .length_delimited,
            @intFromEnum(WireType.fixed32) => .fixed32,
            else => return error.InvalidWireType,
        },
    };
}

pub fn readFixed32(bytes: []const u8, pos: *usize) DecodeError!u32 {
    if (pos.* + 4 > bytes.len) return error.EndOfStream;
    const val = std.mem.readInt(u32, bytes[pos.*..][0..4], .little);
    pos.* += 4;
    return val;
}

pub fn readFixed64(bytes: []const u8, pos: *usize) DecodeError!u64 {
    if (pos.* + 8 > bytes.len) return error.EndOfStream;
    const val = std.mem.readInt(u64, bytes[pos.*..][0..8], .little);
    pos.* += 8;
    return val;
}

pub fn readLengthDelimited(bytes: []const u8, pos: *usize) DecodeError![]const u8 {
    const len: usize = @intCast(try readVarint(bytes, pos));
    if (pos.* + len > bytes.len) return error.EndOfStream;
    const slice = bytes[pos.* .. pos.* + len];
    pos.* += len;
    return slice;
}

pub fn skipField(bytes: []const u8, pos: *usize, wt: WireType) DecodeError!void {
    switch (wt) {
        .varint => _ = try readVarint(bytes, pos),
        .fixed64 => {
            if (pos.* + 8 > bytes.len) return error.EndOfStream;
            pos.* += 8;
        },
        .length_delimited => {
            const len: usize = @intCast(try readVarint(bytes, pos));
            if (pos.* + len > bytes.len) return error.EndOfStream;
            pos.* += len;
        },
        .fixed32 => {
            if (pos.* + 4 > bytes.len) return error.EndOfStream;
            pos.* += 4;
        },
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "varint roundtrip" {
    const alloc = std.testing.allocator;
    const cases = [_]u64{ 0, 1, 127, 128, 300, 0x7FFFFFFFFFFFFFFF, std.math.maxInt(u64) };

    for (cases) |expected| {
        var buf: Buf = .empty;
        defer buf.deinit(alloc);

        try writeVarint(alloc, &buf, expected);
        var pos: usize = 0;
        const got = try readVarint(buf.items, &pos);
        try std.testing.expectEqual(expected, got);
        try std.testing.expectEqual(buf.items.len, pos);
    }
}

test "string field roundtrip" {
    const alloc = std.testing.allocator;
    var buf: Buf = .empty;
    defer buf.deinit(alloc);

    try writeString(alloc, &buf, 5, "hello");

    var pos: usize = 0;
    const tag = try readTag(buf.items, &pos);
    try std.testing.expectEqual(@as(u32, 5), tag.field);
    try std.testing.expectEqual(WireType.length_delimited, tag.wire_type);

    const data = try readLengthDelimited(buf.items, &pos);
    try std.testing.expectEqualStrings("hello", data);
    try std.testing.expectEqual(buf.items.len, pos);
}

test "nested message roundtrip" {
    const alloc = std.testing.allocator;

    // Encode inner message.
    var inner: Buf = .empty;
    defer inner.deinit(alloc);
    try writeUint64(alloc, &inner, 1, 42);
    try writeString(alloc, &inner, 2, "nested");

    // Wrap in outer message field 3.
    var outer: Buf = .empty;
    defer outer.deinit(alloc);
    try writeMessage(alloc, &outer, 3, inner.items);

    // Decode outer.
    var pos: usize = 0;
    const outer_tag = try readTag(outer.items, &pos);
    try std.testing.expectEqual(@as(u32, 3), outer_tag.field);
    try std.testing.expectEqual(WireType.length_delimited, outer_tag.wire_type);

    const msg_bytes = try readLengthDelimited(outer.items, &pos);
    try std.testing.expectEqual(outer.items.len, pos);

    // Decode inner fields.
    var ipos: usize = 0;
    const tag1 = try readTag(msg_bytes, &ipos);
    try std.testing.expectEqual(@as(u32, 1), tag1.field);
    try std.testing.expectEqual(@as(u64, 42), try readVarint(msg_bytes, &ipos));

    const tag2 = try readTag(msg_bytes, &ipos);
    try std.testing.expectEqual(@as(u32, 2), tag2.field);
    try std.testing.expectEqualStrings("nested", try readLengthDelimited(msg_bytes, &ipos));
}

test "packed floats roundtrip" {
    const alloc = std.testing.allocator;
    const expected = [_]f32{ 1.0, -2.5, 3.14, 0.0 };

    var buf: Buf = .empty;
    defer buf.deinit(alloc);
    try writePackedFloats(alloc, &buf, 7, &expected);

    var pos: usize = 0;
    const tag = try readTag(buf.items, &pos);
    try std.testing.expectEqual(@as(u32, 7), tag.field);
    try std.testing.expectEqual(WireType.length_delimited, tag.wire_type);

    const payload = try readLengthDelimited(buf.items, &pos);
    try std.testing.expectEqual(expected.len * 4, payload.len);

    var fpos: usize = 0;
    for (expected) |exp| {
        const bits = std.mem.readInt(u32, payload[fpos..][0..4], .little);
        fpos += 4;
        const got: f32 = @bitCast(bits);
        try std.testing.expectEqual(exp, got);
    }
}

test "fixed32 and fixed64 roundtrip" {
    const alloc = std.testing.allocator;
    var buf: Buf = .empty;
    defer buf.deinit(alloc);

    try writeFixed32(alloc, &buf, 1, 0xDEADBEEF);
    try writeFixed64(alloc, &buf, 2, 0xCAFEBABE12345678);

    var pos: usize = 0;
    const tag1 = try readTag(buf.items, &pos);
    try std.testing.expectEqual(@as(u32, 1), tag1.field);
    try std.testing.expectEqual(WireType.fixed32, tag1.wire_type);
    try std.testing.expectEqual(@as(u32, 0xDEADBEEF), try readFixed32(buf.items, &pos));

    const tag2 = try readTag(buf.items, &pos);
    try std.testing.expectEqual(@as(u32, 2), tag2.field);
    try std.testing.expectEqual(WireType.fixed64, tag2.wire_type);
    try std.testing.expectEqual(@as(u64, 0xCAFEBABE12345678), try readFixed64(buf.items, &pos));
}

test "skipField skips all wire types" {
    const alloc = std.testing.allocator;
    var buf: Buf = .empty;
    defer buf.deinit(alloc);

    // Write fields: varint(1), fixed64(2), string(3), fixed32(4), uint64(5).
    try writeUint64(alloc, &buf, 1, 999);
    try writeFixed64(alloc, &buf, 2, 0x1111);
    try writeString(alloc, &buf, 3, "skip me");
    try writeFixed32(alloc, &buf, 4, 0x2222);
    try writeUint64(alloc, &buf, 5, 42);

    // Skip fields 1-4, read field 5.
    var pos: usize = 0;
    for (0..4) |_| {
        const tag = try readTag(buf.items, &pos);
        try skipField(buf.items, &pos, tag.wire_type);
    }

    const final_tag = try readTag(buf.items, &pos);
    try std.testing.expectEqual(@as(u32, 5), final_tag.field);
    try std.testing.expectEqual(@as(u64, 42), try readVarint(buf.items, &pos));
    try std.testing.expectEqual(buf.items.len, pos);
}

test "readTag rejects invalid wire type" {
    var pos: usize = 0;
    try std.testing.expectError(error.InvalidWireType, readTag(&.{0x03}, &pos));
}

test "packed int64s roundtrip" {
    const alloc = std.testing.allocator;
    const expected = [_]i64{ 0, 1, -1, 127, -128, 999999 };

    var buf: Buf = .empty;
    defer buf.deinit(alloc);
    try writePackedInt64s(alloc, &buf, 4, &expected);

    var pos: usize = 0;
    const tag = try readTag(buf.items, &pos);
    try std.testing.expectEqual(@as(u32, 4), tag.field);

    const payload = try readLengthDelimited(buf.items, &pos);

    var ipos: usize = 0;
    for (expected) |exp| {
        const raw = try readVarint(payload, &ipos);
        const got: i64 = @bitCast(raw);
        try std.testing.expectEqual(exp, got);
    }
    try std.testing.expectEqual(payload.len, ipos);
}
