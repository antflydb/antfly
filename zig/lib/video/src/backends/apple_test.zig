// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
const std = @import("std");
const media = @import("antfly_media");
const apple = @import("apple.zig");
const clip = @embedFile("../../testdata/decode-bframes.mp4");
const oracle = @embedFile("../../testdata/decode-bframes.nv12");
fn input() media.source.Source {
    return .{ .allocator = std.testing.allocator, .identity = "original-decode-bframes-v1", .storage = .{ .borrowed = clip } };
}
test "video Apple selected H264 pictures match independent FFmpeg NV12 planes" {
    if (@import("builtin").os.tag != .macos) return error.SkipZigTest;
    var src = input();
    var reader = try media.mp4.Reader.init(std.testing.allocator, &src, .{});
    defer reader.deinit();
    const indexes = [_]usize{ 0, 3, 2, 19 };
    var batch = try apple.decodeSelected(std.testing.allocator, &reader, &indexes, .{ .require_hardware = false });
    defer batch.deinit();
    try std.testing.expectEqual(@as(usize, 20), batch.submitted_packets);
    for (batch.frames, indexes) |*frame, index| {
        try std.testing.expectEqual(index, frame.decode_index);
        try std.testing.expectEqual(reader.packets[index].pts, frame.pts);
        // FFmpeg's raw output is in presentation order.
        var ordinal: usize = 0;
        for (reader.packets) |packet| if (packet.pts < frame.pts) {
            ordinal += 1;
        };
        const expected = oracle[ordinal * 32 * 24 * 3 / 2 ..][0 .. 32 * 24 * 3 / 2];
        var mapping = try frame.surface.map();
        defer mapping.deinit();
        for (mapping.planes, 0..) |plane, i| {
            const width: usize = if (i == 0) 32 else 32;
            const height: usize = if (i == 0) 24 else 12;
            const base: usize = if (i == 0) 0 else 32 * 24;
            for (0..height) |y| for (0..width) |x| {
                const difference = @abs(@as(i16, plane.bytes[y * plane.stride + x]) - @as(i16, expected[base + y * width + x]));
                try std.testing.expect(difference <= 2);
            };
        }
    }
}
test "video Apple limits invalid selection and cancelled session allow retry" {
    if (@import("builtin").os.tag != .macos) return error.SkipZigTest;
    var src = input();
    var reader = try media.mp4.Reader.init(std.testing.allocator, &src, .{});
    defer reader.deinit();
    const a = std.testing.allocator;
    try std.testing.expectError(error.InvalidPacketIndex, apple.decodeSelected(a, &reader, &.{20}, .{}));
    try std.testing.expectError(error.DuplicateFrameSelection, apple.decodeSelected(a, &reader, &.{ 0, 0 }, .{}));
    try std.testing.expectError(error.ResourceLimitExceeded, apple.decodeSelected(a, &reader, &.{19}, .{ .max_decode_packets = 5 }));
    try std.testing.expectError(error.ResourceLimitExceeded, apple.decodeSelected(a, &reader, &.{0}, .{ .max_surface_bytes = 1 }));
    const Cancel = struct {
        calls: usize = 0,
        fn check(context: ?*const anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(@constCast(context.?)));
            self.calls += 1;
            if (self.calls >= 12) return error.Cancelled;
        }
    };
    const retained = src.retained_bytes;
    try std.testing.expectError(error.ResourceLimitExceeded, apple.decodeSelected(a, &reader, &.{0}, .{ .require_hardware = false, .max_retained_surface_bytes = 1 }));
    try std.testing.expectEqual(retained, src.retained_bytes);
    var cancel = Cancel{};
    src.control = .{ .context = &cancel, .check_fn = Cancel.check };
    try std.testing.expectError(error.Cancelled, apple.decodeSelected(a, &reader, &.{ 0, 19 }, .{ .require_hardware = false }));
    try std.testing.expectEqual(retained, src.retained_bytes);
    src.control = .{};
    var batch = try apple.decodeSelected(a, &reader, &.{0}, .{ .require_hardware = false });
    batch.deinit();
    try std.testing.expectEqual(retained, src.retained_bytes);
}
test "video portable builds fail closed on unavailable Apple decode" {
    if (@import("builtin").os.tag == .macos) return error.SkipZigTest;
    var reader: media.mp4.Reader = undefined;
    try std.testing.expectError(error.UnsupportedVideoBackend, apple.decodeSelected(std.testing.allocator, &reader, &.{0}, .{}));
}

test "video Apple hardware-only request verifies reported hardware route" {
    if (@import("builtin").os.tag != .macos) return error.SkipZigTest;
    var src = input();
    src.identity = "original-hardware-640x360-v1";
    src.storage = .{ .borrowed = @embedFile("../../testdata/decode-hardware.mp4") };
    var reader = try media.mp4.Reader.init(std.testing.allocator, &src, .{});
    defer reader.deinit();
    var batch = apple.decodeSelected(std.testing.allocator, &reader, &.{ 0, 3 }, .{}) catch |err| switch (err) {
        error.VideoDecoderUnavailable => return error.SkipZigTest,
        else => return err,
    };
    defer batch.deinit();
    try std.testing.expect(batch.hardware);
}
fn allocationCampaign(allocator: std.mem.Allocator) !void {
    var src = input();
    var reader = try media.mp4.Reader.init(std.testing.allocator, &src, .{});
    defer reader.deinit();
    const retained = src.retained_bytes;
    var batch = apple.decodeSelected(allocator, &reader, &.{0}, .{ .require_hardware = false }) catch |err| {
        try std.testing.expectEqual(retained, src.retained_bytes);
        return err;
    };
    batch.deinit();
    try std.testing.expectEqual(retained, src.retained_bytes);
}
test "video Apple allocation failure after decode releases retained pictures" {
    if (@import("builtin").os.tag != .macos) return error.SkipZigTest;
    try std.testing.checkAllAllocationFailures(std.testing.allocator, allocationCampaign, .{});
}

test "video decode fixture receipts pin original clips and independent plane oracle" {
    const receipt = try std.json.parseFromSlice(struct { files: []const struct { file: []const u8, sha256: []const u8, bytes: usize } }, std.testing.allocator, @embedFile("../../testdata/decode-oracle.json"), .{ .ignore_unknown_fields = true });
    defer receipt.deinit();
    const fixtures = .{ clip, oracle, @embedFile("../../testdata/prepare-sdr.mp4"), @embedFile("../../testdata/decode-hardware.mp4") };
    inline for (fixtures, 0..) |bytes, index| {
        var digest: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(bytes, &digest, .{});
        try std.testing.expectEqualStrings(receipt.value.files[index].sha256, &std.fmt.bytesToHex(digest, .lower));
        try std.testing.expectEqual(bytes.len, receipt.value.files[index].bytes);
    }
}

test "video Apple batch metadata and mapped planes outlive both reader and batch" {
    if (@import("builtin").os.tag != .macos) return error.SkipZigTest;
    var src = input();
    var matrix: [9]i32 = undefined;
    var batch = blk: {
        var reader = try media.mp4.Reader.init(std.testing.allocator, &src, .{});
        defer reader.deinit();
        matrix = reader.track.display_matrix;
        break :blk try apple.decodeSelected(std.testing.allocator, &reader, &.{0}, .{ .require_hardware = false });
    };
    var mapped = blk: {
        defer batch.deinit();
        try std.testing.expectEqual(@as(usize, 0), src.retained_bytes);
        try std.testing.expectEqualSlices(i32, &matrix, &batch.display_matrix);
        break :blk try batch.frames[0].surface.map();
    };
    defer mapped.deinit();
    try std.testing.expectEqual(@as(usize, 32), mapped.planes[0].width);
    try std.testing.expect(mapped.planes[0].bytes.len >= 32 * 24);
}
