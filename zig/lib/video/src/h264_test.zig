// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
const std = @import("std");
const media = @import("antfly_media");
const video = @import("mod.zig");
const a = std.testing.allocator;
const clip = @embedFile("../testdata/h264-intra.mp4");
const pixels = @embedFile("../testdata/h264-intra.nv12");
fn source() media.source.Source {
    return .{ .allocator = a, .identity = "native-h264-intra", .storage = .{ .borrowed = clip } };
}
test "video portable H264 IDR CAVLC Intra16x16 matches independent FFmpeg NV12" {
    var src = source();
    var reader = try media.mp4.Reader.init(a, &src, .{});
    defer reader.deinit();
    try std.testing.expectEqual(@as(usize, 8), reader.packets.len);
    for (reader.packets, 0..) |packet, index| {
        var frame = try video.h264.decodeFrame(a, &reader, index, .{});
        defer frame.deinit();
        try std.testing.expectEqual(packet.pts, frame.pts);
        try std.testing.expectEqualSlices(u8, pixels[index * 64 * 48 * 3 / 2 ..][0 .. 64 * 48 * 3 / 2], frame.nv12);
        const patches = try video.preparation.referenceHost(a, frame.host(), .{ .width = 48, .height = 48, .matrix = .bt709 }, .{});
        defer a.free(patches);
    }
}

const cases = .{
    .{ @embedFile("../testdata/h264-intra.mp4"), @embedFile("../testdata/h264-intra.nv12") },
    .{ @embedFile("../testdata/h264-intra-qp3.mp4"), @embedFile("../testdata/h264-intra-qp3.nv12") },
    .{ @embedFile("../testdata/h264-intra-crop.mp4"), @embedFile("../testdata/h264-intra-crop.nv12") },
    .{ @embedFile("../testdata/h264-intra-full.mp4"), @embedFile("../testdata/h264-intra-full.nv12") },
    .{ @embedFile("../testdata/h264-intra-plane.mp4"), @embedFile("../testdata/h264-intra-plane.nv12") },
};
test "video portable H264 quantizer cropping full range and oracle hashes" {
    const Receipt = struct { cases: []const struct { mp4_sha256: []const u8, nv12_sha256: []const u8, full_range: bool } };
    const receipt = try std.json.parseFromSlice(Receipt, a, @embedFile("../testdata/h264-intra-oracle.json"), .{ .ignore_unknown_fields = true });
    defer receipt.deinit();
    inline for (cases, 0..) |pair, case_index| {
        inline for (pair, 0..) |bytes, file_index| {
            var hash: [32]u8 = undefined;
            std.crypto.hash.sha2.Sha256.hash(bytes, &hash, .{});
            try std.testing.expectEqualStrings(if (file_index == 0) receipt.value.cases[case_index].mp4_sha256 else receipt.value.cases[case_index].nv12_sha256, &std.fmt.bytesToHex(hash, .lower));
        }
        var src = media.source.Source{ .allocator = a, .identity = "h264-qualified", .storage = .{ .borrowed = pair[0] } };
        var reader = try media.mp4.Reader.init(a, &src, .{});
        defer reader.deinit();
        const size = @as(usize, reader.track.width) * reader.track.height * 3 / 2;
        for (reader.packets, 0..) |_, i| {
            var frame = try video.h264.decodeFrame(a, &reader, i, .{});
            defer frame.deinit();
            try std.testing.expectEqualSlices(u8, pair[1][i * size ..][0..size], frame.nv12);
            try std.testing.expectEqual(receipt.value.cases[case_index].full_range, frame.full_range);
        }
    }
}
fn allocationCase(allocator: std.mem.Allocator) !void {
    var src = source();
    var reader = try media.mp4.Reader.init(a, &src, .{});
    defer reader.deinit();
    const retained = src.retained_bytes;
    defer std.debug.assert(retained == src.retained_bytes);
    var frame = try video.h264.decodeFrame(allocator, &reader, 0, .{});
    defer frame.deinit();
}
test "video H264 allocation failure campaign and work limits clean up" {
    var baseline = std.testing.FailingAllocator.init(a, .{ .resize_fail_index = 0 });
    try allocationCase(baseline.allocator());
    try std.testing.expectEqual(baseline.allocated_bytes, baseline.freed_bytes);
    for (0..baseline.alloc_index) |index| {
        var failing = std.testing.FailingAllocator.init(a, .{ .resize_fail_index = 0, .fail_index = index });
        try std.testing.expectError(error.OutOfMemory, allocationCase(failing.allocator()));
        try std.testing.expectEqual(failing.allocated_bytes, failing.freed_bytes);
    }
    var src = source();
    var reader = try media.mp4.Reader.init(a, &src, .{});
    defer reader.deinit();
    const retained = src.retained_bytes;
    try std.testing.expectError(error.ResourceLimitExceeded, video.h264.decodeFrame(a, &reader, 0, .{ .max_pixels = 1 }));
    try std.testing.expectError(error.ResourceLimitExceeded, video.h264.decodeFrame(a, &reader, 0, .{ .max_packet_bytes = 1 }));
    try std.testing.expectError(error.ResourceLimitExceeded, video.h264.decodeFrame(a, &reader, 0, .{ .max_decode_bytes = 1 }));
    try std.testing.expectEqual(retained, src.retained_bytes);
}
test "video H264 portable overlapping windows own patches and reject filtering and unsupported profiles" {
    var src = source();
    var reader = try media.mp4.Reader.init(a, &src, .{});
    const time: i64 = reader.track.timescale;
    const requests = [_]video.windows.Window{ .{ .interval = .{ .start = 0, .end = 2 * time }, .step = @intCast(@divExact(time, 4)) }, .{ .interval = .{ .start = time, .end = 2 * time }, .step = @intCast(@divExact(time, 4)) } };
    var jobs = try video.software.prepareWindows(a, &reader, &requests, .{ .preparation = .{ .width = 48, .height = 48, .matrix = .bt709 } });
    defer jobs.deinit();
    try std.testing.expectEqual(@as(usize, 8), jobs.decoded_packets);
    try std.testing.expectEqual(@as(usize, 4), jobs.reusedSelections());
    reader.deinit();
    try std.testing.expectEqual(@as(usize, 0), src.retained_bytes);
    _ = try jobs.frame(7);
    inline for (.{ @embedFile("../testdata/h264-intra-filtered.mp4"), @embedFile("../testdata/decode-hardware.mp4") }) |unsupported| {
        var other = media.source.Source{ .allocator = a, .identity = "unsupported", .storage = .{ .borrowed = unsupported } };
        var r = try media.mp4.Reader.init(a, &other, .{});
        defer r.deinit();
        try std.testing.expectError(error.UnsupportedVideoProfile, video.h264.decodeFrame(a, &r, 0, .{}));
    }
}
test "video H264 cancellation during macroblocks releases source and shared reservations" {
    var src = source();
    var reader = try media.mp4.Reader.init(a, &src, .{});
    defer reader.deinit();
    var pool = media.admission.Pool{ .limits = .{ .host_bytes = 1024 * 1024 } };
    src.admission_pool = &pool;
    const Cancel = struct {
        calls: usize = 0,
        fn check(ctx: ?*const anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(@constCast(ctx.?)));
            self.calls += 1;
            if (self.calls >= 8) return error.Cancelled;
        }
    };
    var cancel = Cancel{};
    src.control = .{ .context = &cancel, .check_fn = Cancel.check };
    const retained = src.retained_bytes;
    try std.testing.expectError(error.Cancelled, video.h264.decodeFrame(a, &reader, 0, .{}));
    try std.testing.expectEqual(retained, src.retained_bytes);
    try std.testing.expectEqual(media.admission.Resources{}, pool.snapshot());
    src.control = .{};
    var frame = try video.h264.decodeFrame(a, &reader, 0, .{});
    try std.testing.expectEqual(@as(u64, frame.nv12.len), pool.snapshot().host_bytes);
    frame.deinit();
    try std.testing.expectEqual(media.admission.Resources{}, pool.snapshot());
}

test "video portable H264 PCM byte alignment and emulation prevention match FFmpeg" {
    var src = media.source.Source{ .allocator = a, .identity = "pcm", .storage = .{ .borrowed = @embedFile("../testdata/h264-pcm.mp4") } };
    var reader = try media.mp4.Reader.init(a, &src, .{});
    defer reader.deinit();
    var frame = try video.h264.decodeFrame(a, &reader, 0, .{});
    defer frame.deinit();
    try std.testing.expectEqualSlices(u8, @embedFile("../testdata/h264-pcm.nv12"), frame.nv12);
}

test "video H264 bounded packet mutation corpus releases all allocations" {
    const changed = try a.dupe(u8, clip);
    defer a.free(changed);
    var src = media.source.Source{ .allocator = a, .identity = "mutation-corpus", .storage = .{ .borrowed = changed } };
    var reader = try media.mp4.Reader.init(a, &src, .{});
    defer reader.deinit();
    const packet = reader.packets[0];
    const retained = src.retained_bytes;
    for (0..512) |case| {
        const offset = @as(usize, @intCast(packet.offset)) + (case * 37) % packet.size;
        changed[offset] ^= @as(u8, 1) << @as(u3, @intCast(case % 8));
        if (video.h264.decodeFrame(a, &reader, 0, .{})) |owned| {
            var frame = owned;
            frame.deinit();
        } else |_| {}
        changed[offset] = clip[offset];
        try std.testing.expectEqual(retained, src.retained_bytes);
    }
}
