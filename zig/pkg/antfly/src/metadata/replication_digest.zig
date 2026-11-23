// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! The existing Source JSON identity, streamed without retaining an encoded
//! copy. The byte ceiling applies to JSON, excluding the domain prefix.
const std = @import("std");

pub fn digest(domain: []const u8, value: anytype, max_bytes: usize) ![32]u8 {
    var state = std.crypto.hash.Blake3.init(.{});
    state.update(domain);
    var buffer: [64 * 1024]u8 = undefined;
    var count = std.Io.Writer.Discarding.init(&.{});
    var hashing = std.Io.Writer.Hashed(std.crypto.hash.Blake3).initHasher(&count.writer, state, &buffer);
    try std.json.Stringify.value(value, .{}, &hashing.writer);
    try hashing.writer.flush();
    if (count.fullCount() > max_bytes) return error.CatalogCommandTooLarge;
    var result: [32]u8 = undefined;
    hashing.hasher.final(&result);
    return result;
}

fn reference(domain: []const u8, value: anytype) ![32]u8 {
    const bytes = try std.json.Stringify.valueAlloc(std.testing.allocator, value, .{});
    defer std.testing.allocator.free(bytes);
    var state = std.crypto.hash.Blake3.init(.{});
    state.update(domain);
    state.update(bytes);
    var result: [32]u8 = undefined;
    state.final(&result);
    return result;
}

test "native Source streamed digests preserve exact JSON encoding and domain separation" {
    const a = std.testing.allocator;
    var input = try std.json.parseFromSlice(std.json.Value, a,
        \\{"quoted\"key":"line\n\t\\/é🐜","exact":9007199254740993,"decimal":1.2300e-19,"null":null,"array":[true,false,{},[]]}
    , .{ .parse_numbers = false });
    defer input.deinit();
    const tag = "antfly/source-digest-test/v1\x00";
    try std.testing.expectEqualDeep(try reference(tag, input.value), try digest(tag, input.value, 4096));
    try std.testing.expect(!std.mem.eql(u8, &try digest(tag, input.value, 4096), &try digest("another-domain", input.value, 4096)));
    const Custom = struct {
        pub fn jsonStringify(_: @This(), writer: anytype) !void {
            try writer.beginObject();
            try writer.objectField("included");
            try writer.write(@as(u64, std.math.maxInt(u64)));
            try writer.endObject();
        }
    };
    try std.testing.expectEqualDeep(try reference(tag, Custom{}), try digest(tag, Custom{}, 4096));
    const tuple = .{ @as(?u8, null), @as(i64, std.math.minInt(i64)), @as(f64, -0.0), @as([]const u8, "text") };
    try std.testing.expectEqualDeep(try reference(tag, tuple), try digest(tag, tuple, 4096));
}

test "native Source streamed digests preserve exact byte ceilings across buffers and escapes" {
    const a = std.testing.allocator;
    const text = try a.alloc(u8, 128 * 1024);
    defer a.free(text);
    for (text, 0..) |*byte, i| byte.* = if (i % 3 == 0) '\n' else 'x';
    const value = .{ .body = text };
    const bytes = try std.json.Stringify.valueAlloc(a, value, .{});
    defer a.free(bytes);
    try std.testing.expectEqualDeep(try reference("tag", value), try digest("tag", value, bytes.len));
    try std.testing.expectError(error.CatalogCommandTooLarge, digest("tag", value, bytes.len - 1));
    try std.testing.expectError(error.CatalogCommandTooLarge, digest("tag", value, 0));
    try std.testing.expectEqualDeep(try reference("prefix-excluded", @as(u8, 0)), try digest("prefix-excluded", @as(u8, 0), 1));
}
