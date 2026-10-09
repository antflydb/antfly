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
test "video H264 portable overlapping windows own patches and reject unsupported profiles" {
    var src = source();
    var reader = try media.mp4.Reader.init(a, &src, .{});
    const time: i64 = reader.track.timescale;
    const requests = [_]video.windows.Window{ .{ .interval = .{ .start = 0, .end = 2 * time }, .step = @intCast(@divExact(time, 4)) }, .{ .interval = .{ .start = time, .end = 2 * time }, .step = @intCast(@divExact(time, 4)) } };
    var jobs = try video.software.prepareWindows(a, &reader, &requests, .{ .preparation = .{ .width = 48, .height = 48, .matrix = .bt709 } });
    defer jobs.deinit();
    try std.testing.expectEqual(@as(usize, 8), jobs.decoded_packets);
    try std.testing.expectEqual(@as(usize, 4), jobs.reusedSelections());
    _ = try jobs.frame(7);
    const original = reader.track.avcc;
    const unsupported = try a.dupe(u8, original);
    defer a.free(unsupported);
    unsupported[1] = 110;
    reader.track.avcc = unsupported;
    try std.testing.expectError(error.UnsupportedVideoProfile, video.h264.decodeFrame(a, &reader, 0, .{}));
    reader.track.avcc = original;
    reader.deinit();
    try std.testing.expectEqual(@as(usize, 0), src.retained_bytes);
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

test "video H264 Intra4x4 CAVLC matches independent FFmpeg NV12" {
    var src = media.source.Source{ .allocator = a, .identity = "intra4", .storage = .{ .borrowed = @embedFile("../testdata/h264-intra4.mp4") } };
    var reader = try media.mp4.Reader.init(a, &src, .{});
    defer reader.deinit();
    const oracle = @embedFile("../testdata/h264-intra4.nv12");
    const size = @as(usize, reader.track.width) * reader.track.height * 3 / 2;
    for (reader.packets, 0..) |_, i| {
        var frame = try video.h264.decodeFrame(a, &reader, i, .{});
        defer frame.deinit();
        try std.testing.expectEqualSlices(u8, oracle[i * size ..][0..size], frame.nv12);
    }
}

test "video H264 intra deblocking quantization and cropping match independent FFmpeg NV12" {
    inline for (.{
        .{ @embedFile("../testdata/h264-intra-filtered.mp4"), @embedFile("../testdata/h264-intra-filtered.nv12") },
        .{ @embedFile("../testdata/h264-intra4-filtered.mp4"), @embedFile("../testdata/h264-intra4-filtered.nv12") },
        .{ @embedFile("../testdata/h264-intra4-crop.mp4"), @embedFile("../testdata/h264-intra4-crop.nv12") },
        .{ @embedFile("../testdata/h264-intra4-low.mp4"), @embedFile("../testdata/h264-intra4-low.nv12") },
    }) |pair| {
        var src = media.source.Source{ .allocator = a, .identity = "intra-filtered", .storage = .{ .borrowed = pair[0] } };
        var reader = try media.mp4.Reader.init(a, &src, .{});
        defer reader.deinit();
        const size = @as(usize, reader.track.width) * reader.track.height * 3 / 2;
        for (reader.packets, 0..) |_, i| {
            var frame = try video.h264.decodeFrame(a, &reader, i, .{});
            defer frame.deinit();
            try std.testing.expectEqualSlices(u8, pair[1][i * size ..][0..size], frame.nv12);
        }
    }
}

test "video H264 Main CABAC intra matches independent FFmpeg NV12" {
    var src = media.source.Source{ .allocator = a, .identity = "main-cabac", .storage = .{ .borrowed = @embedFile("../testdata/h264-main-intra.mp4") } };
    var reader = try media.mp4.Reader.init(a, &src, .{});
    defer reader.deinit();
    const oracle = @embedFile("../testdata/h264-main-intra.nv12");
    const size = @as(usize, reader.track.width) * reader.track.height * 3 / 2;
    for (reader.packets, 0..) |_, i| {
        var frame = try video.h264.decodeFrame(a, &reader, i, .{});
        defer frame.deinit();
        try std.testing.expectEqualSlices(u8, oracle[i * size ..][0..size], frame.nv12);
    }
}

test "video H264 High CABAC 8x8 intra matches independent FFmpeg NV12" {
    var src = media.source.Source{ .allocator = a, .identity = "high-cabac", .storage = .{ .borrowed = @embedFile("../testdata/h264-high-intra.mp4") } };
    var reader = try media.mp4.Reader.init(a, &src, .{});
    defer reader.deinit();
    const oracle = @embedFile("../testdata/h264-high-intra.nv12");
    const size = @as(usize, reader.track.width) * reader.track.height * 3 / 2;
    for (reader.packets, 0..) |_, i| {
        var frame = try video.h264.decodeFrame(a, &reader, i, .{});
        defer frame.deinit();
        try std.testing.expectEqualSlices(u8, oracle[i * size ..][0..size], frame.nv12);
    }
}

test "video H264 Baseline and High P-frame motion prediction match independent FFmpeg NV12" {
    inline for (.{
        .{ @embedFile("../testdata/h264-baseline-p.mp4"), @embedFile("../testdata/h264-baseline-p.nv12") },
        .{ @embedFile("../testdata/h264-high-p.mp4"), @embedFile("../testdata/h264-high-p.nv12") },
    }) |pair| {
        var src = media.source.Source{ .allocator = a, .identity = "inter-p", .storage = .{ .borrowed = pair[0] } };
        var reader = try media.mp4.Reader.init(a, &src, .{});
        defer reader.deinit();
        const size = @as(usize, reader.track.width) * reader.track.height * 3 / 2;
        for (reader.packets, 0..) |_, i| {
            var frame = try video.h264.decodeFrame(a, &reader, i, .{});
            defer frame.deinit();
            try std.testing.expectEqualSlices(u8, pair[1][i * size ..][0..size], frame.nv12);
        }
    }
}

test "video H264 B-frame spatial temporal direct and CABAC prediction match FFmpeg" {
    inline for (.{
        .{ @embedFile("../testdata/h264-main-b.mp4"), @embedFile("../testdata/h264-main-b.nv12") },
        .{ @embedFile("../testdata/h264-high-b.mp4"), @embedFile("../testdata/h264-high-b.nv12") },
        .{ @embedFile("../testdata/h264-cavlc-b.mp4"), @embedFile("../testdata/h264-cavlc-b.nv12") },
        .{ @embedFile("../testdata/h264-high-weighted.mp4"), @embedFile("../testdata/h264-high-weighted.nv12") },
        .{ @embedFile("../testdata/h264-high-fade.mp4"), @embedFile("../testdata/h264-high-fade.nv12") },
        .{ @embedFile("../testdata/h264-high-pyramid.mp4"), @embedFile("../testdata/h264-high-pyramid.nv12") },
        .{ @embedFile("../testdata/h264-high-wrap.mp4"), @embedFile("../testdata/h264-high-wrap.nv12") },
        .{ @embedFile("../testdata/h264-high-cavlc8.mp4"), @embedFile("../testdata/h264-high-cavlc8.nv12") },
    }) |pair| {
        var src = media.source.Source{ .allocator = a, .identity = "inter-b", .storage = .{ .borrowed = pair[0] } };
        var reader = try media.mp4.Reader.init(a, &src, .{});
        defer reader.deinit();
        const size = @as(usize, reader.track.width) * reader.track.height * 3 / 2;
        for (reader.packets, 0..) |packet, i| {
            var frame = try video.h264.decodeFrame(a, &reader, i, .{});
            defer frame.deinit();
            var order: usize = 0;
            for (reader.packets) |other| if (other.pts < packet.pts) {
                order += 1;
            };
            if (!std.mem.eql(u8, pair[1][order * size ..][0..size], frame.nv12)) {
                var differences: [3]usize = @splat(0);
                for (frame.nv12, pair[1][order * size ..][0..size], 0..) |actual, expected, pos| if (actual != expected) {
                    differences[if (pos < size * 2 / 3) 0 else 1 + pos % 2] += 1;
                    if (differences[0] + differences[1] + differences[2] <= 12) std.debug.print("diff {d} actual {d} expected {d}\n", .{ pos, actual, expected });
                };
                std.debug.print("B fixture {s} packet {d} presentation {d} diffs {any}\n", .{ src.identity, i, order, differences });
                return error.TestUnexpectedResult;
            }
        }
    }
}

fn interAllocationCase(allocator: std.mem.Allocator) !void {
    var src = media.source.Source{ .allocator = a, .identity = "inter-allocation", .storage = .{ .borrowed = @embedFile("../testdata/h264-high-pyramid.mp4") } };
    var reader = try media.mp4.Reader.init(a, &src, .{});
    defer reader.deinit();
    const retained = src.retained_bytes;
    defer std.debug.assert(retained == src.retained_bytes);
    var frame = try video.h264.decodeFrame(allocator, &reader, 6, .{});
    defer frame.deinit();
}
test "video H264 reference picture allocation failures release DPB and packet leases" {
    var baseline = std.testing.FailingAllocator.init(a, .{ .resize_fail_index = 0 });
    try interAllocationCase(baseline.allocator());
    try std.testing.expectEqual(baseline.allocated_bytes, baseline.freed_bytes);
    for (0..baseline.alloc_index) |index| {
        var failing = std.testing.FailingAllocator.init(a, .{ .resize_fail_index = 0, .fail_index = index });
        try std.testing.expectError(error.OutOfMemory, interAllocationCase(failing.allocator()));
        try std.testing.expectEqual(failing.allocated_bytes, failing.freed_bytes);
    }
}
test "video H264 batch decodes shared references once and preserves selection order" {
    var src = media.source.Source{ .allocator = a, .identity = "inter-batch", .storage = .{ .borrowed = @embedFile("../testdata/h264-high-pyramid.mp4") } };
    var reader = try media.mp4.Reader.init(a, &src, .{});
    defer reader.deinit();
    const indexes = [_]usize{ 7, 2, 6, 4 };
    const Capture = struct {
        reader: *media.mp4.Reader,
        calls: usize = 0,
        fail: bool = false,
        fn publish(raw: *anyopaque, slot: usize, frame: *const video.h264.Frame) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.fail) return error.Cancelled;
            const packet = self.reader.packets[indexes[slot]];
            try std.testing.expectEqual(packet.pts, frame.pts);
            var order: usize = 0;
            for (self.reader.packets) |other| if (other.pts < packet.pts) {
                order += 1;
            };
            const size = @as(usize, frame.width) * frame.height * 3 / 2;
            try std.testing.expectEqualSlices(u8, @embedFile("../testdata/h264-high-pyramid.nv12")[order * size ..][0..size], frame.nv12);
            self.calls += 1;
        }
    };
    var capture = Capture{ .reader = &reader };
    var pool = media.admission.Pool{ .limits = .{ .host_bytes = 16 * 1024 * 1024 } };
    src.admission_pool = &pool;
    const retained = src.retained_bytes;
    try std.testing.expectError(error.ResourceLimitExceeded, video.h264.decodeSelected(a, &reader, &indexes, .{ .max_dependency_packets = 7 }, &capture, Capture.publish));
    const stats = try video.h264.decodeSelected(a, &reader, &indexes, .{}, &capture, Capture.publish);
    try std.testing.expectEqual(@as(usize, 8), stats.decoded_packets);
    try std.testing.expectEqual(indexes.len, capture.calls);
    var payload: u64 = 0;
    for (reader.packets[0..8]) |packet| payload += packet.size;
    try std.testing.expectEqual(payload, stats.payload_bytes);
    try std.testing.expectEqual(retained, src.retained_bytes);
    try std.testing.expectEqual(media.admission.Resources{}, pool.snapshot());
    capture.fail = true;
    try std.testing.expectError(error.Cancelled, video.h264.decodeSelected(a, &reader, &indexes, .{}, &capture, Capture.publish));
    try std.testing.expectEqual(retained, src.retained_bytes);
    try std.testing.expectEqual(media.admission.Resources{}, pool.snapshot());
}
test "video H264 CABAC inter mutation corpus fails safely" {
    const original = @embedFile("../testdata/h264-high-b.mp4");
    const changed = try a.dupe(u8, original);
    defer a.free(changed);
    var src = media.source.Source{ .allocator = a, .identity = "cabac-mutations", .storage = .{ .borrowed = changed } };
    var reader = try media.mp4.Reader.init(a, &src, .{});
    defer reader.deinit();
    const retained = src.retained_bytes;
    for (0..192) |case| {
        const packet = reader.packets[case % 3];
        const offset = @as(usize, @intCast(packet.offset)) + (case * 37) % packet.size;
        changed[offset] ^= @as(u8, 1) << @as(u3, @intCast(case % 8));
        if (video.h264.decodeFrame(a, &reader, 2, .{})) |owned| {
            var frame = owned;
            frame.deinit();
        } else |_| {}
        changed[offset] = original[offset];
        try std.testing.expectEqual(retained, src.retained_bytes);
    }
}
test "video H264 weighted prediction rounds clips and handles coincident POC" {
    const weights = @import("h264_weights.zig");
    try std.testing.expectEqual(@as(u8, 115), weights.single(200, .{ .denominator = 1, .weight = 1, .offset = 15 }));
    try std.testing.expectEqual(@as(u8, 0), weights.single(200, .{ .weight = -1 }));
    try std.testing.expectEqual(@as(u8, 255), weights.single(200, .{ .weight = 2 }));
    try std.testing.expectEqual(@as(u8, 76), weights.pair(100, 50, .{ .denominator = 1, .weight = 2, .offset = 1 }, .{ .denominator = 1, .weight = 2, .offset = 0 }));
    try std.testing.expectEqual(@as(u8, 75), weights.implicit(100, 50, 2, 0, 4));
    try std.testing.expectEqual(@as(u8, 75), weights.implicit(100, 50, 2, 0, 0));
}

test "video H264 CABAC zero words are bounded and CAVLC padding stays strict" {
    const Bits = @import("h264_bits.zig").Bits;
    var bits = try Bits.initSlice(a, &.{ 0x65, 0x80, 0, 0 }, .{}, true);
    defer bits.deinit();
    try bits.finish();
    try std.testing.expectEqual(@as(usize, 3), bits.bytes.len);
    try std.testing.expectError(error.MalformedVideoPacket, Bits.init(a, &.{ 0x65, 0x80, 0, 0 }));
    try std.testing.expectError(error.MalformedVideoPacket, Bits.initSlice(a, &.{ 0x65, 0x80, 0 }, .{}, true));
}

test "video H264 broader tool oracle hashes pin independent encoded and decoded fixtures" {
    const Receipt = struct { cases: []const struct { name: []const u8, mp4_sha256: []const u8, nv12_sha256: []const u8 } };
    const receipt = try std.json.parseFromSlice(Receipt, a, @embedFile("../testdata/h264-tools-oracle.json"), .{ .ignore_unknown_fields = true });
    defer receipt.deinit();
    inline for (.{
        .{ "h264-intra4", @embedFile("../testdata/h264-intra4.mp4"), @embedFile("../testdata/h264-intra4.nv12") },
        .{ "h264-intra4-filtered", @embedFile("../testdata/h264-intra4-filtered.mp4"), @embedFile("../testdata/h264-intra4-filtered.nv12") },
        .{ "h264-intra4-crop", @embedFile("../testdata/h264-intra4-crop.mp4"), @embedFile("../testdata/h264-intra4-crop.nv12") },
        .{ "h264-intra4-low", @embedFile("../testdata/h264-intra4-low.mp4"), @embedFile("../testdata/h264-intra4-low.nv12") },
        .{ "h264-main-intra", @embedFile("../testdata/h264-main-intra.mp4"), @embedFile("../testdata/h264-main-intra.nv12") },
        .{ "h264-high-intra", @embedFile("../testdata/h264-high-intra.mp4"), @embedFile("../testdata/h264-high-intra.nv12") },
        .{ "h264-high-cavlc8", @embedFile("../testdata/h264-high-cavlc8.mp4"), @embedFile("../testdata/h264-high-cavlc8.nv12") },
        .{ "h264-baseline-p", @embedFile("../testdata/h264-baseline-p.mp4"), @embedFile("../testdata/h264-baseline-p.nv12") },
        .{ "h264-high-p", @embedFile("../testdata/h264-high-p.mp4"), @embedFile("../testdata/h264-high-p.nv12") },
        .{ "h264-main-b", @embedFile("../testdata/h264-main-b.mp4"), @embedFile("../testdata/h264-main-b.nv12") },
        .{ "h264-high-b", @embedFile("../testdata/h264-high-b.mp4"), @embedFile("../testdata/h264-high-b.nv12") },
        .{ "h264-cavlc-b", @embedFile("../testdata/h264-cavlc-b.mp4"), @embedFile("../testdata/h264-cavlc-b.nv12") },
        .{ "h264-high-weighted", @embedFile("../testdata/h264-high-weighted.mp4"), @embedFile("../testdata/h264-high-weighted.nv12") },
        .{ "h264-high-fade", @embedFile("../testdata/h264-high-fade.mp4"), @embedFile("../testdata/h264-high-fade.nv12") },
        .{ "h264-high-pyramid", @embedFile("../testdata/h264-high-pyramid.mp4"), @embedFile("../testdata/h264-high-pyramid.nv12") },
        .{ "h264-high-wrap", @embedFile("../testdata/h264-high-wrap.mp4"), @embedFile("../testdata/h264-high-wrap.nv12") },
    }) |fixture| {
        var found = false;
        for (receipt.value.cases) |entry| if (std.mem.eql(u8, entry.name, fixture[0])) {
            var hash: [32]u8 = undefined;
            std.crypto.hash.sha2.Sha256.hash(fixture[1], &hash, .{});
            try std.testing.expectEqualStrings(entry.mp4_sha256, &std.fmt.bytesToHex(hash, .lower));
            std.crypto.hash.sha2.Sha256.hash(fixture[2], &hash, .{});
            try std.testing.expectEqualStrings(entry.nv12_sha256, &std.fmt.bytesToHex(hash, .lower));
            found = true;
        };
        try std.testing.expect(found);
    }
}

fn referenceMarking(state: *@import("h264_references.zig").State, commands: []const @import("h264_references.zig").State.Command) !void {
    const Writer = struct {
        bytes: [64]u8 = @splat(0),
        position: usize = 8,
        fn bit(self: *@This(), value: u1) void {
            self.bytes[self.position / 8] |= @as(u8, value) << @as(u3, @intCast(7 - self.position % 8));
            self.position += 1;
        }
        fn ue(self: *@This(), value: u32) void {
            const code = value + 1;
            const length: usize = 32 - @clz(code);
            for (1..length) |_| self.bit(0);
            var i = length;
            while (i > 0) {
                i -= 1;
                self.bit(@intCast(code >> @as(u5, @intCast(i)) & 1));
            }
        }
    };
    var writer = Writer{};
    writer.bytes[0] = 0x61;
    writer.bit(@intFromBool(commands.len != 0));
    if (commands.len != 0) {
        for (commands) |command| {
            writer.ue(command.operation);
            if (command.operation == 1 or command.operation == 2 or command.operation == 3 or command.operation == 4 or command.operation == 6) writer.ue(command.first);
            if (command.operation == 3) writer.ue(command.second);
        }
        writer.ue(0);
    }
    writer.bit(1);
    var bits = try @import("h264_bits.zig").Bits.init(a, writer.bytes[0 .. (writer.position + 7) / 8]);
    defer bits.deinit();
    try state.marking(&bits, false);
    try bits.finish();
}
test "video H264 short and long reference marking preserves identities and sliding order" {
    const State = @import("h264_references.zig").State;
    const Motion = @import("h264_motion.zig").Motion;
    var state = State{ .reference = true };
    defer state.deinit(a);
    const planar: [24]u8 = @splat(0);
    const motions = [_]Motion{.{ .decoded = true }};
    const pair = [2][]const Motion{ &motions, &motions };
    state.order(4);
    try referenceMarking(&state, &.{});
    try state.commit(a, &planar, pair, 3);
    const first_id = state.pictures[0].id;
    state.current_num = 1;
    state.current_poc = 2;
    state.order(4);
    try referenceMarking(&state, &.{ .{ .operation = 3, .first = 0, .second = 2 }, .{ .operation = 6, .first = 1 } });
    try state.commit(a, &planar, pair, 3);
    try std.testing.expectEqual(@as(usize, 2), state.count);
    try std.testing.expectEqual(first_id, state.pictures[0].id);
    try std.testing.expectEqual(@as(?u32, 2), state.pictures[0].long_term);
    try std.testing.expectEqual(@as(?u32, 1), state.pictures[1].long_term);
    state.current_num = 2;
    state.current_poc = 4;
    state.order(4);
    try std.testing.expectEqual(@as(usize, 1), state.list0[0]);
    try referenceMarking(&state, &.{ .{ .operation = 2, .first = 2 }, .{ .operation = 4, .first = 1 } });
    try state.commit(a, &planar, pair, 3);
    try std.testing.expectEqual(@as(usize, 1), state.count);
    try std.testing.expectEqual(@as(u32, 2), state.pictures[0].frame_num);
    state.current_num = 3;
    state.order(4);
    try referenceMarking(&state, &.{.{ .operation = 1, .first = 0 }});
    try state.commit(a, &planar, pair, 3);
    try std.testing.expectEqual(@as(usize, 1), state.count);
    state.current_num = 4;
    state.order(4);
    try referenceMarking(&state, &.{ .{ .operation = 5 }, .{ .operation = 6, .first = 0 } });
    try state.commit(a, &planar, pair, 2);
    try std.testing.expectEqual(@as(u32, 0), state.pictures[0].frame_num);
    try std.testing.expectEqual(@as(i32, 0), state.pictures[0].poc);
    try std.testing.expectEqual(@as(?u32, 0), state.pictures[0].long_term);
    for (1..3) |number| {
        state.current_num = @intCast(number);
        state.order(4);
        try referenceMarking(&state, &.{});
        try state.commit(a, &planar, pair, 2);
    }
    try std.testing.expectEqual(@as(usize, 2), state.count);
    try std.testing.expectEqual(@as(?u32, 0), state.pictures[0].long_term);
    try std.testing.expectEqual(@as(u32, 2), state.pictures[1].frame_num);
    state.current_num = 3;
    state.order(4);
    try std.testing.expectEqual(@as(usize, 1), state.list0[0]);
    try std.testing.expectEqual(@as(usize, 0), state.list0[1]);
}
