// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
const std = @import("std");
const webm = @import("webm.zig");
const source = @import("source.zig");
const a = std.testing.allocator;
const clip = @embedFile("../testdata/video.webm");
fn input() source.Source {
    return .{ .allocator = a, .identity = "vp9-webm", .storage = .{ .borrowed = clip } };
}
fn allocations(allocator: std.mem.Allocator) !void {
    var src = input();
    src.allocator = allocator;
    var reader = try webm.Reader.init(allocator, &src, .{});
    reader.deinit();
    try std.testing.expectEqual(@as(usize, 0), src.retained_bytes);
}
test "webm VP9 index agrees with ffprobe with bounded block probes" {
    const Oracle = struct { sha256: []const u8, packets: []const struct { pts: i64, duration: u64, size: []const u8, pos: []const u8, flags: []const u8 } };
    const oracle = try std.json.parseFromSlice(Oracle, a, @embedFile("../testdata/webm-oracle.json"), .{ .ignore_unknown_fields = true });
    defer oracle.deinit();
    var hash: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(clip, &hash, .{});
    try std.testing.expectEqualStrings(oracle.value.sha256, &std.fmt.bytesToHex(hash, .lower));
    var src = input();
    var reader = try webm.Reader.init(a, &src, .{});
    defer reader.deinit();
    try std.testing.expectEqual(webm.Codec.vp9, reader.track.codec);
    try std.testing.expectEqual(oracle.value.packets.len, reader.packets.len);
    for (reader.packets, oracle.value.packets) |packet, expected| {
        try std.testing.expectEqual(expected.pts * 1_000_000, packet.pts);
        try std.testing.expectEqual(expected.duration * 1_000_000, packet.duration_ns.?);
        try std.testing.expectEqual(try std.fmt.parseInt(usize, expected.size, 10), packet.size);
        // FFprobe reports the Block header, including track/time/flags; our
        // offset begins at the codec payload for this one-byte track fixture.
        try std.testing.expectEqual(try std.fmt.parseInt(u64, expected.pos, 10) + 4, packet.offset);
        try std.testing.expectEqual(expected.flags[0] == 'K', packet.sync);
    }
    try std.testing.expect(src.total_bytes < clip.len / 4);
    var first = try reader.readPacket(0);
    defer first.deinit();
    var last = try reader.readPacket(reader.packets.len - 1);
    defer last.deinit();
    try std.testing.expectEqual(reader.packets[0].size, first.bytes.len);
    // Force resizing failures so every growth follows the same allocation
    // sequence regardless of heap layout in Debug versus ReleaseSafe.
    var baseline = std.testing.FailingAllocator.init(a, .{ .resize_fail_index = 0 });
    try allocations(baseline.allocator());
    for (0..baseline.alloc_index) |index| {
        var failing = std.testing.FailingAllocator.init(a, .{ .resize_fail_index = 0, .fail_index = index });
        try std.testing.expectError(error.OutOfMemory, allocations(failing.allocator()));
        try std.testing.expectEqual(failing.allocated_bytes, failing.freed_bytes);
    }
}
test "webm malformed ranges limits and truncation release source leases" {
    var src = input();
    try std.testing.expectError(error.ResourceLimitExceeded, webm.Reader.init(a, &src, .{ .max_packets = 1 }));
    try std.testing.expectEqual(@as(usize, 0), src.retained_bytes);
    try std.testing.expectError(error.ResourceLimitExceeded, webm.Reader.init(a, &src, .{ .max_metadata_bytes = 1 }));
    try std.testing.expectEqual(@as(usize, 0), src.retained_bytes);
    src.storage = .{ .borrowed = clip[0 .. clip.len - 1] };
    try std.testing.expectError(error.MalformedMedia, webm.Reader.init(a, &src, .{}));
    try std.testing.expectEqual(@as(usize, 0), src.retained_bytes);
}

test "webm rejects video lacing instead of returning the lace as one picture" {
    const changed = try a.dupe(u8, clip);
    defer a.free(changed);
    var original = input();
    var reader = try webm.Reader.init(a, &original, .{});
    const flags_offset: usize = @intCast(reader.packets[0].offset - 1);
    reader.deinit();
    changed[flags_offset] |= 2;
    var bad = source.Source{ .allocator = a, .identity = "laced", .storage = .{ .borrowed = changed } };
    try std.testing.expectError(error.UnsupportedVideoLacing, webm.Reader.init(a, &bad, .{}));
    try std.testing.expectEqual(@as(usize, 0), bad.retained_bytes);
}
