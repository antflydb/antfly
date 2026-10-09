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

fn unknownClusters(allocator: std.mem.Allocator) ![]u8 {
    const changed = try allocator.dupe(u8, clip);
    errdefer allocator.free(changed);
    var src = input();
    var reader = try webm.Reader.init(allocator, &src, .{});
    defer reader.deinit();
    var previous: ?u64 = null;
    for (reader.packets) |packet| {
        if (previous == packet.cluster_offset) continue;
        previous = packet.cluster_offset;
        const offset: usize = @intCast(packet.cluster_offset);
        const ebml = @import("ebml.zig");
        const id = try ebml.readElementId(changed, offset);
        const size = try ebml.readVint(changed, offset + id.len);
        @memset(changed[offset + id.len ..][0..size.len], 255);
        changed[offset + id.len] = @as(u8, 255) >> @as(u3, @intCast(size.len - 1));
    }
    return changed;
}
test "webm unknown-size Clusters preserve packets and validated Cues seek hints" {
    const changed = try unknownClusters(a);
    defer a.free(changed);
    var src = source.Source{ .allocator = a, .identity = "unknown-clusters", .storage = .{ .borrowed = changed } };
    var reader = try webm.Reader.init(a, &src, .{});
    defer reader.deinit();
    var original = input();
    var reference = try webm.Reader.init(a, &original, .{});
    defer reference.deinit();
    try std.testing.expectEqualSlices(webm.Packet, reference.packets, reader.packets);
    try std.testing.expect(reader.cues.len != 0);
    for (reader.cues) |cue| {
        try std.testing.expectEqual(cue.packet_index, try reader.seek(cue.pts));
        try std.testing.expectEqual(cue.packet_index, try reader.seek(cue.pts + 1));
        try std.testing.expect(reader.packets[cue.packet_index].sync);
    }
    try std.testing.expectError(error.MissingVideoReference, reader.seek(-1));
    const Harness = struct {
        fn run(allocator: std.mem.Allocator) !void {
            const bytes = try unknownClusters(allocator);
            defer allocator.free(bytes);
            var s = source.Source{ .allocator = allocator, .identity = "unknown-failures", .storage = .{ .borrowed = bytes } };
            var r = try webm.Reader.init(allocator, &s, .{});
            r.deinit();
            try std.testing.expectEqual(@as(usize, 0), s.retained_bytes);
        }
    };
    try std.testing.checkAllAllocationFailures(a, Harness.run, .{});
}
test "webm Cues budgets and fallback without Cues" {
    var src = input();
    try std.testing.expectError(error.ResourceLimitExceeded, webm.Reader.init(a, &src, .{ .max_cues = 0 }));
    try std.testing.expectEqual(@as(usize, 0), src.retained_bytes);
    const changed = try a.dupe(u8, clip);
    defer a.free(changed);
    const ebml = @import("ebml.zig");
    const header = try ebml.readElementHeader(changed, 0);
    const segment = try ebml.readElementHeader(changed, header.data_end.?);
    var cursor = segment.data_start;
    while (cursor < changed.len) {
        const child = try ebml.readElementHeader(changed, cursor);
        if (child.id == 0x1c53bb6b) changed[cursor + 3] = 0x6c;
        cursor = child.data_end.?;
    }
    src.storage = .{ .borrowed = changed };
    var reader = try webm.Reader.init(a, &src, .{});
    defer reader.deinit();
    try std.testing.expectEqual(@as(usize, 0), reader.cues.len);
    try std.testing.expectEqual(@as(usize, 0), try reader.seek(1));
}

test "webm unknown-size consecutive Clusters terminate at sibling headers" {
    const ebml = @import("ebml.zig");
    var original = input();
    var baseline = try webm.Reader.init(a, &original, .{});
    defer baseline.deinit();
    const offset: usize = @intCast(baseline.packets[0].cluster_offset);
    const cluster = try ebml.readElementHeader(clip, offset);
    const end = cluster.data_end.?;
    const unknown = try unknownClusters(a);
    defer a.free(unknown);
    // Insertion changes later Cluster offsets. Remove stale Cues from this
    // constructed boundary case; Cues seeking is independently qualified.
    const first_known = try ebml.readElementHeader(clip, 0);
    const segment_known = try ebml.readElementHeader(clip, first_known.data_end.?);
    var child_offset = segment_known.data_start;
    while (child_offset < clip.len) {
        const child = try ebml.readElementHeader(clip, child_offset);
        if (child.id == 0x1c53bb6b) unknown[child_offset + 3] = 0x6c;
        child_offset = child.data_end.?;
    }
    var first_packets: usize = 0;
    for (baseline.packets) |packet| if (packet.cluster_offset == offset) {
        first_packets += 1;
    };
    const repeated = try a.alloc(u8, unknown.len + end - offset);
    defer a.free(repeated);
    @memcpy(repeated[0..end], unknown[0..end]);
    @memcpy(repeated[end..][0 .. end - offset], unknown[offset..end]);
    @memcpy(repeated[end + end - offset ..], unknown[end..]);
    const first = try ebml.readElementHeader(repeated, 0);
    const id = try ebml.readElementId(repeated, first.data_end.?);
    const size_offset = first.data_end.? + id.len;
    const size = try ebml.readVint(repeated, size_offset);
    @memset(repeated[size_offset..][0..size.len], 255);
    repeated[size_offset] = @as(u8, 255) >> @as(u3, @intCast(size.len - 1));
    var src = source.Source{ .allocator = a, .identity = "two-live-clusters", .storage = .{ .borrowed = repeated } };
    var reader = try webm.Reader.init(a, &src, .{});
    defer reader.deinit();
    try std.testing.expectEqual(baseline.packets.len + first_packets, reader.packets.len);
    try std.testing.expectEqual(@as(u64, end), reader.packets[first_packets].cluster_offset);
    try std.testing.expectEqual(@as(usize, 0), reader.cues.len);
    for (reader.cues) |cue| try std.testing.expectEqual(cue.packet_index, try reader.seek(cue.pts));
}
