// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
const std = @import("std");
const mp4 = @import("mp4.zig");
const source = @import("source.zig");
const iso = @import("isobmff.zig");
const a = std.testing.allocator;
const clips = .{
    @embedFile("../testdata/bframes-tail.mp4"),
    @embedFile("../testdata/vfr-front.mp4"),
    @embedFile("../testdata/rotated.mp4"),
    @embedFile("../testdata/video-audio.mp4"),
    @embedFile("../testdata/signed-cts.mp4"),
    @embedFile("../testdata/compact-co64.mp4"),
};
const Fixture = struct {
    sha256: []const u8,
    timescale: u32,
    track_id: u32,
    width: u16,
    height: u16,
    packets: []const struct { dts: i64, pts: i64, duration: u32, offset: u64, size: u32, sync: bool },
};
const Receipt = struct { fixtures: []const Fixture };
fn input(bytes: []const u8) source.Source {
    return .{ .allocator = a, .identity = "original-synthetic-fixture-v1", .storage = .{ .borrowed = bytes } };
}

test "mp4 video packet index matches independent ffprobe B-frame VFR rotation and multitrack receipts" {
    const receipt = try std.json.parseFromSlice(Receipt, a, @embedFile("../testdata/mp4-oracle.json"), .{ .ignore_unknown_fields = true });
    defer receipt.deinit();
    inline for (clips, 0..) |bytes, fixture_index| {
        const expected = receipt.value.fixtures[fixture_index];
        var hash: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(bytes, &hash, .{});
        try std.testing.expectEqualStrings(expected.sha256, &std.fmt.bytesToHex(hash, .lower));
        var src = input(bytes);
        var reader = try mp4.Reader.init(a, &src, .{});
        defer reader.deinit();
        try std.testing.expectEqual(expected.track_id, reader.track.id);
        try std.testing.expectEqual(expected.timescale, reader.track.timescale);
        try std.testing.expectEqual(expected.width, reader.track.width);
        try std.testing.expectEqual(expected.height, reader.track.height);
        try std.testing.expectEqual(expected.packets.len, reader.packets.len);
        for (expected.packets, reader.packets, 0..) |want, got, i| {
            try std.testing.expectEqual(want.dts, got.dts);
            try std.testing.expectEqual(want.pts, got.pts);
            try std.testing.expectEqual(want.duration, got.duration);
            try std.testing.expectEqual(want.offset, got.offset);
            try std.testing.expectEqual(want.size, got.size);
            try std.testing.expectEqual(want.sync, got.sync);
            var packet = try reader.readPacket(i);
            defer packet.deinit();
            try std.testing.expectEqualSlices(u8, bytes[@intCast(want.offset)..][0..want.size], packet.bytes);
            const start = try reader.syncBefore(i);
            try std.testing.expect(reader.packets[start].sync and start <= i);
        }
        if (fixture_index == 2) {
            try std.testing.expectEqual(@as(i32, 0), reader.track.display_matrix[0]);
            try std.testing.expectEqual(@as(i32, -65536), reader.track.display_matrix[1]);
        }
    }
}

test "mp4 tail metadata range reads skip mdat and retain packets across later reads" {
    const Provider = struct {
        bytes: []const u8,
        payload_bytes: usize = 0,
        fn readAt(ctx: *anyopaque, offset: u64, out: []u8, _: source.Control) !usize {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            const n = @min(out.len, 13); // force short reads
            @memcpy(out[0..n], self.bytes[@intCast(offset)..][0..n]);
            const mdat_end: usize = findMoov(self.bytes);
            if (offset >= 48 and offset < mdat_end) self.payload_bytes += n;
            return n;
        }
    };
    var provider = Provider{ .bytes = clips[0] };
    var src = source.Source{ .allocator = a, .identity = "immutable-version-v1", .storage = .{ .range = .{ .context = &provider, .read_at = Provider.readAt, .length = clips[0].len } } };
    var reader = try mp4.Reader.init(a, &src, .{});
    try std.testing.expectEqual(@as(usize, 0), provider.payload_bytes);
    var first = try reader.readPacket(0);
    var second = try reader.readPacket(1);
    const first_copy = try a.dupe(u8, first.bytes);
    defer a.free(first_copy);
    second.deinit();
    reader.deinit(); // independently leased packet does not borrow metadata
    try std.testing.expectEqualSlices(u8, first_copy, first.bytes);
    first.deinit();
    try std.testing.expectEqual(@as(usize, 0), src.retained_bytes);
}
fn findMoov(bytes: []const u8) usize {
    var cursor: usize = 0;
    while (cursor < bytes.len) {
        const b = iso.readBox(bytes, cursor) catch unreachable;
        if (b.typ == iso.fourcc("moov")) return cursor;
        cursor = b.end;
    }
    unreachable;
}

test "mp4 video malformed tables, metadata/index limits and fragment rejection release leases" {
    var src = input(clips[0]);
    try std.testing.expectError(error.ResourceLimitExceeded, mp4.Reader.init(a, &src, .{ .max_metadata_bytes = 1 }));
    try std.testing.expectEqual(@as(usize, 0), src.retained_bytes);
    try std.testing.expectError(error.ResourceLimitExceeded, mp4.Reader.init(a, &src, .{ .max_samples = 1 }));
    try std.testing.expectEqual(@as(usize, 0), src.retained_bytes);
    try std.testing.expectError(error.ResourceLimitExceeded, mp4.Reader.init(a, &src, .{ .max_index_bytes = 1 }));
    try std.testing.expectEqual(@as(usize, 0), src.retained_bytes);
    const fragment = [_]u8{ 0, 0, 0, 8, 'm', 'o', 'o', 'f' };
    var fragmented = input(&fragment);
    try std.testing.expectError(error.MissingMovieMetadata, mp4.Reader.init(a, &fragmented, .{}));
    const changed = try a.dupe(u8, clips[0]);
    defer a.free(changed);
    const stts = std.mem.indexOf(u8, changed, "stts").?;
    // Corrupt run count so it exceeds the actual table payload.
    @memset(changed[stts + 8 ..][0..4], 0xff);
    var bad = input(changed);
    try std.testing.expectError(error.MalformedMedia, mp4.Reader.init(a, &bad, .{}));
    try std.testing.expectEqual(@as(usize, 0), bad.retained_bytes);
    var truncated = input(clips[0][0 .. clips[0].len - 1]);
    try std.testing.expectError(error.MalformedMedia, mp4.Reader.init(a, &truncated, .{}));
}
fn allocationCampaign(allocator: std.mem.Allocator) !void {
    var src = input(clips[0]);
    src.allocator = allocator;
    const result = mp4.Reader.init(allocator, &src, .{}) catch |err| {
        try std.testing.expectEqual(@as(usize, 0), src.retained_bytes);
        return err;
    };
    var reader = result;
    reader.deinit();
    try std.testing.expectEqual(@as(usize, 0), src.retained_bytes);
}
test "mp4 rejects out-of-payload offsets and external data references" {
    const changed = try a.dupe(u8, clips[0]);
    defer a.free(changed);
    const stco = std.mem.indexOf(u8, changed, "stco").?;
    @memset(changed[stco + 12 ..][0..4], 0);
    var invalid_offset = input(changed);
    try std.testing.expectError(error.MalformedMedia, mp4.Reader.init(a, &invalid_offset, .{}));
    try std.testing.expectEqual(@as(usize, 0), invalid_offset.retained_bytes);
    @memcpy(changed, clips[0]);
    const url = std.mem.indexOf(u8, changed, "url ").?;
    changed[url + 7] = 0; // Remove the self-contained data-reference flag.
    var external = input(changed);
    try std.testing.expectError(error.UnsupportedDataReference, mp4.Reader.init(a, &external, .{}));
    try std.testing.expectEqual(@as(usize, 0), external.retained_bytes);
}
test "mp4 video allocation failure campaign cleans metadata and index" {
    try std.testing.checkAllAllocationFailures(a, allocationCampaign, .{});
}

test "mp4 cancellation during table walk drains retained metadata and permits retry" {
    const Cancel = struct {
        checks: usize = 0,
        fn check(ctx: ?*const anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(@constCast(ctx.?)));
            self.checks += 1;
            if (self.checks == 12) return error.Cancelled;
        }
    };
    var cancel = Cancel{};
    var src = input(clips[0]);
    src.control = .{ .context = &cancel, .check_fn = Cancel.check };
    try std.testing.expectError(error.Cancelled, mp4.Reader.init(a, &src, .{}));
    try std.testing.expectEqual(@as(usize, 0), src.retained_bytes);
    src.control = .{};
    var reader = try mp4.Reader.init(a, &src, .{});
    reader.deinit();
    try std.testing.expectEqual(@as(usize, 0), src.retained_bytes);
}

const fragmented_clip = @embedFile("../testdata/fragmented.mp4");
fn fragmentedAllocation(allocator: std.mem.Allocator) !void {
    var src = input(fragmented_clip);
    src.allocator = allocator;
    var reader = try mp4.Reader.init(allocator, &src, .{});
    reader.deinit();
    try std.testing.expectEqual(@as(usize, 0), src.retained_bytes);
}
test "fragmented mp4 packet offsets DTS PTS durations and sync match ffprobe" {
    const Oracle = struct {
        sha256: []const u8,
        ffprobe_shift_ticks: i64 = 0,
        packets: []const struct { pts: i64, dts: i64, duration: u32, size: []const u8, pos: []const u8, flags: []const u8 },
    };
    inline for (.{ .{ fragmented_clip, @embedFile("../testdata/fragmented-oracle.json") }, .{ @embedFile("../testdata/fragmented-bframes.mp4"), @embedFile("../testdata/fragmented-bframes-oracle.json") } }, 0..) |pair, case_index| {
        const oracle = try std.json.parseFromSlice(Oracle, a, pair[1], .{ .ignore_unknown_fields = true });
        defer oracle.deinit();
        var hash: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(pair[0], &hash, .{});
        try std.testing.expectEqualStrings(oracle.value.sha256, &std.fmt.bytesToHex(hash, .lower));
        var src = input(pair[0]);
        var reader = try mp4.Reader.init(a, &src, .{});
        defer reader.deinit();
        try std.testing.expectEqual(oracle.value.packets.len, reader.packets.len);
        for (reader.packets, oracle.value.packets) |packet, expected| {
            try std.testing.expectEqual(expected.dts, packet.media_dts);
            try std.testing.expectEqual(expected.pts - oracle.value.ffprobe_shift_ticks, packet.media_pts);
            try std.testing.expectEqual(expected.pts - oracle.value.ffprobe_shift_ticks, packet.pts);
            try std.testing.expectEqual(expected.duration, packet.duration);
            try std.testing.expectEqual(try std.fmt.parseInt(u64, expected.pos, 10), packet.offset);
            try std.testing.expectEqual(try std.fmt.parseInt(u32, expected.size, 10), packet.size);
            try std.testing.expectEqual(expected.flags[0] == 'K', packet.sync);
        }
        if (case_index == 1) {
            // FFprobe translates signed-composition presentation and decode
            // clocks by one frame; the generated first IDR's source PTS is zero.
            const shift = oracle.value.ffprobe_shift_ticks;
            try std.testing.expect(shift > 0);
            try std.testing.expectEqual(@as(u32, @intCast(shift)), reader.track.decode_preroll_ticks);
            for (reader.packets, oracle.value.packets) |packet, expected| try std.testing.expectEqual(expected.dts - shift, packet.dts);
        }
    }
    try std.testing.checkAllAllocationFailures(a, fragmentedAllocation, .{});
}
test "fragmented mp4 bad offsets missing tfdt and admission failures release metadata" {
    const bytes = try a.dupe(u8, fragmented_clip);
    defer a.free(bytes);
    const trun = std.mem.indexOf(u8, bytes, "trun").?;
    @memset(bytes[trun + 12 ..][0..4], 0xff);
    var bad = input(bytes);
    try std.testing.expectError(error.MalformedMedia, mp4.Reader.init(a, &bad, .{}));
    try std.testing.expectEqual(@as(usize, 0), bad.retained_bytes);
    @memcpy(bytes, fragmented_clip);
    const tfdt = std.mem.indexOf(u8, bytes, "tfdt").?;
    @memcpy(bytes[tfdt..][0..4], "free");
    try std.testing.expectError(error.UnsupportedTimeline, mp4.Reader.init(a, &bad, .{}));
    try std.testing.expectEqual(@as(usize, 0), bad.retained_bytes);
    var src = input(fragmented_clip);
    try std.testing.expectError(error.ResourceLimitExceeded, mp4.Reader.init(a, &src, .{ .max_samples = 1 }));
    try std.testing.expectEqual(@as(usize, 0), src.retained_bytes);
}
