// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
const std = @import("std");
const media = @import("antfly_media");
const video = @import("mod.zig");
const a = std.testing.allocator;
const clip = @embedFile("../testdata/mjpeg.mov");
const rgba = @embedFile("../testdata/mjpeg.rgba");
fn source() media.source.Source {
    return .{ .allocator = a, .identity = "mjpeg-v1", .storage = .{ .borrowed = clip } };
}
const requested = [_]video.windows.Window{ .{ .interval = .{ .start = 0, .end = 16384 }, .step = 4096 }, .{ .interval = .{ .start = 8192, .end = 32768 }, .step = 4096 } };
const options = video.mjpeg.JobOptions{ .preparation = .{ .width = 48, .height = 48, .matrix = .bt709 } };
test "video portable MJPEG decode matches independent RGBA oracle and source clocks" {
    var src = source();
    var reader = try media.mp4.Reader.init(a, &src, .{});
    defer reader.deinit();
    try std.testing.expectEqual(media.mp4.Codec.mjpeg, reader.track.codec);
    try std.testing.expectEqual(@as(usize, 8), reader.packets.len);
    try std.testing.expectError(error.UnsupportedVideoCodec, video.decode_plan.create(a, &reader, &.{0}, .{}));
    for (0..8) |index| {
        const before = src.reads;
        var frame = try video.mjpeg.decodeFrame(a, &reader, index, .{});
        defer frame.deinit();
        try std.testing.expectEqual(before + 1, src.reads);
        try std.testing.expectEqual(reader.packets[index].pts, frame.pts);
        try std.testing.expectEqual(@as(u32, 4096), frame.duration);
        try std.testing.expect(frame.decode_high_water >= frame.rgba.len);
        var maximum: u16 = 0;
        for (frame.rgba, rgba[index * 64 * 48 * 4 ..][0 .. 64 * 48 * 4]) |actual, expected| {
            const difference = @abs(@as(i16, actual) - @as(i16, expected));
            maximum = @max(maximum, difference);
        }
        try std.testing.expect(maximum <= 3);
    }
}
test "video portable MJPEG overlap preparation owns patches and decodes only unique frames" {
    var src = source();
    var result = blk: {
        var reader = try media.mp4.Reader.init(a, &src, .{});
        defer reader.deinit();
        const reads = src.reads;
        var result = try video.mjpeg.prepareWindows(a, &reader, &requested, options);
        errdefer result.deinit();
        try std.testing.expectEqual(@as(usize, 8), result.decoded_packets);
        try std.testing.expectEqual(reads + 8, src.reads);
        try std.testing.expectEqual(@as(usize, 10), result.plan.references.len);
        for (result.plan.unique_indexes, 0..) |index, i| {
            var frame = try video.mjpeg.decodeFrame(a, &reader, index, .{});
            defer frame.deinit();
            const expected = try video.preparation.referenceRgba(a, frame.rgba, frame.width, frame.height, options.preparation, .{});
            defer a.free(expected);
            try std.testing.expectEqualSlices(f32, expected, try result.frame(i));
        }
        break :blk result;
    };
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 0), src.retained_bytes);
    try std.testing.expectEqual(@as(usize, 48 * 48 * 3), (try result.frame(0)).len);
    try std.testing.expectError(error.InvalidPacketIndex, result.frame(8));
}
test "video MJPEG budgets malformed packets and cancellation allow retry" {
    const bytes = try a.dupe(u8, clip);
    defer a.free(bytes);
    var src = source();
    src.storage = .{ .borrowed = bytes };
    var reader = try media.mp4.Reader.init(a, &src, .{});
    defer reader.deinit();
    const retained = src.retained_bytes;
    try std.testing.expectError(error.ResourceLimitExceeded, video.mjpeg.decodeFrame(a, &reader, 0, .{ .max_pixels = 1 }));
    try std.testing.expectError(error.ResourceLimitExceeded, video.mjpeg.decodeFrame(a, &reader, 0, .{ .max_packet_bytes = 1 }));
    try std.testing.expectError(error.ResourceLimitExceeded, video.mjpeg.decodeFrame(a, &reader, 0, .{ .max_decode_bytes = 64 * 48 * 4 }));
    try std.testing.expectError(error.InvalidPacketIndex, video.mjpeg.decodeFrame(a, &reader, 8, .{}));
    var limited = options;
    limited.max_output_bytes = 1;
    try std.testing.expectError(error.ResourceLimitExceeded, video.mjpeg.prepareWindows(a, &reader, &requested, limited));
    reader.track.width += 1;
    try std.testing.expectError(error.UnsupportedDynamicGeometry, video.mjpeg.decodeFrame(a, &reader, 0, .{}));
    reader.track.width -= 1;
    const end: usize = @intCast(reader.packets[0].offset + reader.packets[0].size - 1);
    const saved = bytes[end];
    bytes[end] = 0;
    try std.testing.expectError(error.JpegDecodeFailed, video.mjpeg.decodeFrame(a, &reader, 0, .{}));
    bytes[end] = saved;
    const Cancel = struct {
        calls: usize = 0,
        fn check(ctx: ?*const anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(@constCast(ctx.?)));
            self.calls += 1;
            if (self.calls >= 5) return error.Cancelled;
        }
    };
    var cancel = Cancel{};
    src.control = .{ .context = &cancel, .check_fn = Cancel.check };
    try std.testing.expectError(error.Cancelled, video.mjpeg.decodeFrame(a, &reader, 0, .{}));
    try std.testing.expectEqual(retained, src.retained_bytes);
    src.control = .{};
    var frame = try video.mjpeg.decodeFrame(a, &reader, 0, .{});
    frame.deinit();
}
fn allocationCase(allocator: std.mem.Allocator) !void {
    var src = source();
    var reader = try media.mp4.Reader.init(a, &src, .{});
    defer reader.deinit();
    const retained = src.retained_bytes;
    defer std.debug.assert(retained == src.retained_bytes);
    var result = try video.mjpeg.prepareWindows(allocator, &reader, &requested, options);
    defer result.deinit();
}
test "video MJPEG allocation failures release pixels patches and packet leases" {
    // Disallow in-place resizing so every failure index is deterministic.
    var baseline = std.testing.FailingAllocator.init(a, .{ .resize_fail_index = 0 });
    try allocationCase(baseline.allocator());
    try std.testing.expectEqual(baseline.allocated_bytes, baseline.freed_bytes);
    for (0..baseline.alloc_index) |index| {
        var failing = std.testing.FailingAllocator.init(a, .{ .fail_index = index, .resize_fail_index = 0 });
        try std.testing.expectError(error.OutOfMemory, allocationCase(failing.allocator()));
        try std.testing.expectEqual(failing.allocated_bytes, failing.freed_bytes);
    }
}
test "video MJPEG fixture hashes and packet receipts pin the oracle" {
    const receipt = try std.json.parseFromSlice(struct { files: []const struct { sha256: []const u8, bytes: usize }, packets: []const struct { pts: i64, dts: i64, size: []const u8, pos: []const u8 } }, a, @embedFile("../testdata/mjpeg-oracle.json"), .{ .ignore_unknown_fields = true });
    defer receipt.deinit();
    inline for (.{ clip, rgba }, 0..) |bytes, i| {
        var hash: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(bytes, &hash, .{});
        try std.testing.expectEqualStrings(receipt.value.files[i].sha256, &std.fmt.bytesToHex(hash, .lower));
        try std.testing.expectEqual(receipt.value.files[i].bytes, bytes.len);
    }
    var src = source();
    var reader = try media.mp4.Reader.init(a, &src, .{});
    defer reader.deinit();
    for (reader.packets, receipt.value.packets) |packet, expected| {
        try std.testing.expectEqual(packet.pts, expected.pts);
        try std.testing.expectEqual(packet.dts, expected.dts);
        try std.testing.expectEqual(packet.size, try std.fmt.parseInt(u32, expected.size, 10));
        try std.testing.expectEqual(packet.offset, try std.fmt.parseInt(u64, expected.pos, 10));
    }
}

test "video MJPEG rejects interlaced container entries and progressive JPEG samples" {
    const bytes = try a.dupe(u8, clip);
    defer a.free(bytes);
    var src = source();
    src.storage = .{ .borrowed = bytes };
    const field = std.mem.indexOf(u8, bytes, "fiel").? + 4;
    bytes[field] = 2;
    try std.testing.expectError(error.UnsupportedInterlacedVideo, media.mp4.Reader.init(a, &src, .{}));
    try std.testing.expectEqual(@as(usize, 0), src.retained_bytes);
    bytes[field] = 1;
    var reader = try media.mp4.Reader.init(a, &src, .{});
    defer reader.deinit();
    const packet = reader.packets[0];
    const offset: usize = @intCast(packet.offset);
    const frame_header = std.mem.indexOf(u8, bytes[offset..][0..packet.size], &.{ 0xff, 0xc0 }).? + offset;
    bytes[frame_header + 1] = 0xc2;
    try std.testing.expectError(error.UnsupportedVideoProfile, video.mjpeg.decodeFrame(a, &reader, 0, .{}));
    bytes[frame_header + 1] = 0xc0;
    // An extra marker after entropy is excluded by the single-scan EOI contract.
    reader.packets[0].size -= 2;
    try std.testing.expectError(error.JpegDecodeFailed, video.mjpeg.decodeFrame(a, &reader, 0, .{}));
}
test "video sparse MJPEG range frames read only selected payloads and outlive the reader" {
    const Provider = struct {
        fn read(_: *anyopaque, offset: u64, out: []u8, control: media.source.Control) !usize {
            try control.check();
            const start: usize = @intCast(offset);
            const n = @min(out.len, clip.len - start);
            @memcpy(out[0..n], clip[start..][0..n]);
            return n;
        }
    };
    var dummy: u8 = 0;
    var src = source();
    src.storage = .{ .range = .{ .context = &dummy, .read_at = Provider.read, .length = clip.len } };
    var frame = blk: {
        var reader = try media.mp4.Reader.init(a, &src, .{});
        defer reader.deinit();
        const reads = src.reads;
        const bytes = src.total_bytes;
        const result = try video.mjpeg.decodeFrame(a, &reader, 7, .{});
        try std.testing.expectEqual(reads + 1, src.reads);
        try std.testing.expectEqual(@as(u64, reader.packets[7].size), src.total_bytes - bytes);
        break :blk result;
    };
    defer frame.deinit();
    try std.testing.expectEqual(@as(usize, 0), src.retained_bytes);
    try std.testing.expectEqual(@as(i64, 28672), frame.pts);
    for (frame.rgba, rgba[7 * 64 * 48 * 4 ..]) |actual, expected| try std.testing.expect(@abs(@as(i16, actual) - @as(i16, expected)) <= 3);
}

test "video MJPEG window cancellation releases partial preparation and permits retry" {
    var src = source();
    var reader = try media.mp4.Reader.init(a, &src, .{});
    defer reader.deinit();
    const retained = src.retained_bytes;
    const Cancel = struct {
        calls: usize = 0,
        fn check(ctx: ?*const anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(@constCast(ctx.?)));
            self.calls += 1;
            if (self.calls >= 300) return error.Cancelled;
        }
    };
    var cancel = Cancel{};
    src.control = .{ .context = &cancel, .check_fn = Cancel.check };
    try std.testing.expectError(error.Cancelled, video.mjpeg.prepareWindows(a, &reader, &requested, options));
    try std.testing.expectEqual(retained, src.retained_bytes);
    src.control = .{};
    var result = try video.mjpeg.prepareWindows(a, &reader, &requested, options);
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 2), result.reusedSelections());
}
