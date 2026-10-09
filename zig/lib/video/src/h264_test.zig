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
    unsupported[1] = 255;
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

test "video H264 all seven FMO maps and ASO reconstruct known PCM samples" {
    inline for (.{
        .{ @embedFile("../testdata/h264-groups-0-0.mp4"), @embedFile("../testdata/h264-groups-0-0.nv12") },
        .{ @embedFile("../testdata/h264-groups-1-0.mp4"), @embedFile("../testdata/h264-groups-1-0.nv12") },
        .{ @embedFile("../testdata/h264-groups-2-0.mp4"), @embedFile("../testdata/h264-groups-2-0.nv12") },
        .{ @embedFile("../testdata/h264-groups-3-0.mp4"), @embedFile("../testdata/h264-groups-3-0.nv12") },
        .{ @embedFile("../testdata/h264-groups-3-1.mp4"), @embedFile("../testdata/h264-groups-3-1.nv12") },
        .{ @embedFile("../testdata/h264-groups-4-0.mp4"), @embedFile("../testdata/h264-groups-4-0.nv12") },
        .{ @embedFile("../testdata/h264-groups-4-1.mp4"), @embedFile("../testdata/h264-groups-4-1.nv12") },
        .{ @embedFile("../testdata/h264-groups-5-0.mp4"), @embedFile("../testdata/h264-groups-5-0.nv12") },
        .{ @embedFile("../testdata/h264-groups-5-1.mp4"), @embedFile("../testdata/h264-groups-5-1.nv12") },
        .{ @embedFile("../testdata/h264-groups-6-0.mp4"), @embedFile("../testdata/h264-groups-6-0.nv12") },
    }) |pair| {
        var src = media.source.Source{ .allocator = a, .identity = "fmo-aso", .storage = .{ .borrowed = pair[0] } };
        var reader = try media.mp4.Reader.init(a, &src, .{});
        defer reader.deinit();
        var frame = try video.h264.decodeFrame(a, &reader, 0, .{});
        defer frame.deinit();
        try std.testing.expectEqualSlices(u8, pair[1], frame.nv12);
        try std.testing.expectError(error.ResourceLimitExceeded, video.h264.decodeFrame(a, &reader, 0, .{ .max_slices = 1 }));
    }
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
        .{ @embedFile("../testdata/h264-high-jvt.mp4"), @embedFile("../testdata/h264-high-jvt.nv12") },
        .{ @embedFile("../testdata/h264-high-custom.mp4"), @embedFile("../testdata/h264-high-custom.nv12") },
        .{ @embedFile("../testdata/h264-cabac-pcm.mp4"), @embedFile("../testdata/h264-cabac-pcm.nv12") },
        .{ @embedFile("../testdata/h264-poc1-pcm.mp4"), @embedFile("../testdata/h264-poc1-pcm.nv12") },
        .{ @embedFile("../testdata/h264-baseline-slices.mp4"), @embedFile("../testdata/h264-baseline-slices.nv12") },
        .{ @embedFile("../testdata/h264-high-slices.mp4"), @embedFile("../testdata/h264-high-slices.nv12") },
        .{ @embedFile("../testdata/h264-high-constrained.mp4"), @embedFile("../testdata/h264-high-constrained.nv12") },
        .{ @embedFile("../testdata/h264-high-slice-threads.mp4"), @embedFile("../testdata/h264-high-slice-threads.nv12") },
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
        .{ "h264-high-jvt", @embedFile("../testdata/h264-high-jvt.mp4"), @embedFile("../testdata/h264-high-jvt.nv12") },
        .{ "h264-high-custom", @embedFile("../testdata/h264-high-custom.mp4"), @embedFile("../testdata/h264-high-custom.nv12") },
        .{ "h264-baseline-slices", @embedFile("../testdata/h264-baseline-slices.mp4"), @embedFile("../testdata/h264-baseline-slices.nv12") },
        .{ "h264-high-slices", @embedFile("../testdata/h264-high-slices.mp4"), @embedFile("../testdata/h264-high-slices.nv12") },
        .{ "h264-high-constrained", @embedFile("../testdata/h264-high-constrained.mp4"), @embedFile("../testdata/h264-high-constrained.nv12") },
        .{ "h264-high-slice-threads", @embedFile("../testdata/h264-high-slice-threads.mp4"), @embedFile("../testdata/h264-high-slice-threads.nv12") },
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

test "video H264 High10 CAVLC CABAC IPB slices preserve native 10-bit samples" {
    inline for (.{
        .{ @embedFile("../testdata/h264-high10-cavlc.mp4"), @embedFile("../testdata/h264-high10-cavlc.nv12") },
        .{ @embedFile("../testdata/h264-high10-cabac.mp4"), @embedFile("../testdata/h264-high10-cabac.nv12") },
        .{ @embedFile("../testdata/h264-high8-422-cavlc.mp4"), @embedFile("../testdata/h264-high8-422-cavlc.nv12") },
        .{ @embedFile("../testdata/h264-high8-422-cabac.mp4"), @embedFile("../testdata/h264-high8-422-cabac.nv12") },
        .{ @embedFile("../testdata/h264-high10-422-cavlc.mp4"), @embedFile("../testdata/h264-high10-422-cavlc.nv12") },
        .{ @embedFile("../testdata/h264-high10-422-cabac.mp4"), @embedFile("../testdata/h264-high10-422-cabac.nv12") },
        .{ @embedFile("../testdata/h264-high8-444-cavlc.mp4"), @embedFile("../testdata/h264-high8-444-cavlc.nv12") },
        .{ @embedFile("../testdata/h264-high8-444-cabac.mp4"), @embedFile("../testdata/h264-high8-444-cabac.nv12") },
        .{ @embedFile("../testdata/h264-high10-444-cavlc.mp4"), @embedFile("../testdata/h264-high10-444-cavlc.nv12") },
        .{ @embedFile("../testdata/h264-high10-444-cabac.mp4"), @embedFile("../testdata/h264-high10-444-cabac.nv12") },
        .{ @embedFile("../testdata/h264-high8-mbaff-unfiltered-cavlc.mp4"), @embedFile("../testdata/h264-high8-mbaff-unfiltered-cavlc.nv12") },
        .{ @embedFile("../testdata/h264-high8-mbaff-unfiltered-cabac.mp4"), @embedFile("../testdata/h264-high8-mbaff-unfiltered-cabac.nv12") },
        .{ @embedFile("../testdata/h264-high10-mbaff-unfiltered-cavlc.mp4"), @embedFile("../testdata/h264-high10-mbaff-unfiltered-cavlc.nv12") },
        .{ @embedFile("../testdata/h264-high10-mbaff-unfiltered-cabac.mp4"), @embedFile("../testdata/h264-high10-mbaff-unfiltered-cabac.nv12") },
        .{ @embedFile("../testdata/h264-high8-mbaff-cavlc.mp4"), @embedFile("../testdata/h264-high8-mbaff-cavlc.nv12") },
        .{ @embedFile("../testdata/h264-high8-mbaff-cabac.mp4"), @embedFile("../testdata/h264-high8-mbaff-cabac.nv12") },
        .{ @embedFile("../testdata/h264-high10-mbaff-cavlc.mp4"), @embedFile("../testdata/h264-high10-mbaff-cavlc.nv12") },
        .{ @embedFile("../testdata/h264-high10-mbaff-cabac.mp4"), @embedFile("../testdata/h264-high10-mbaff-cabac.nv12") },
        .{ @embedFile("../testdata/h264-high8-lossless-cavlc.mp4"), @embedFile("../testdata/h264-high8-lossless-cavlc.nv12") },
        .{ @embedFile("../testdata/h264-high8-lossless-cabac.mp4"), @embedFile("../testdata/h264-high8-lossless-cabac.nv12") },
        .{ @embedFile("../testdata/h264-high10-lossless-422-cavlc.mp4"), @embedFile("../testdata/h264-high10-lossless-422-cavlc.nv12") },
        .{ @embedFile("../testdata/h264-high10-lossless-422-cabac.mp4"), @embedFile("../testdata/h264-high10-lossless-422-cabac.nv12") },
        .{ @embedFile("../testdata/h264-high8-lossless-444-cavlc.mp4"), @embedFile("../testdata/h264-high8-lossless-444-cavlc.nv12") },
        .{ @embedFile("../testdata/h264-high8-lossless-444-cabac.mp4"), @embedFile("../testdata/h264-high8-lossless-444-cabac.nv12") },
        .{ @embedFile("../testdata/h264-high10-lossless-444-cavlc.mp4"), @embedFile("../testdata/h264-high10-lossless-444-cavlc.nv12") },
        .{ @embedFile("../testdata/h264-high10-lossless-444-cabac.mp4"), @embedFile("../testdata/h264-high10-lossless-444-cabac.nv12") },
    }, 0..) |pair, case_index| {
        var src = media.source.Source{ .allocator = a, .identity = "high10", .storage = .{ .borrowed = pair[0] } };
        var reader = try media.mp4.Reader.init(a, &src, .{});
        defer reader.deinit();
        const size = pair[1].len / reader.packets.len;
        var field_mbs: usize = 0;
        for (reader.packets, 0..) |_, i| {
            var frame = video.h264.decodeFrame(a, &reader, i, .{}) catch |err| {
                std.debug.print("case={d} packet={d} error={s}\n", .{ case_index, i, @errorName(err) });
                return err;
            };
            defer frame.deinit();
            field_mbs += frame.field_macroblocks;
            try std.testing.expect(frame.bit_depth == 8 or frame.bit_depth == 10);
            const ordinal: usize = @intCast(@divExact(reader.packets[i].pts, reader.packets[i].duration));
            if (!std.mem.eql(u8, pair[1][ordinal * size ..][0..size], frame.nv12)) std.debug.print("case={d} packet={d} ordinal={d} fields={d}\n", .{ case_index, i, ordinal, frame.field_macroblocks });
            try std.testing.expectEqualSlices(u8, pair[1][ordinal * size ..][0..size], frame.nv12);
            const patches = try video.preparation.referenceHost(a, frame.host(), .{ .width = 48, .height = 48, .matrix = .bt709 }, .{});
            defer a.free(patches);
        }
        if (case_index >= 10 and case_index < 18) try std.testing.expect(field_mbs > 0);
    }
}
test "video H264 PAFF and mixed MBAFF PCM preserve woven native samples" {
    inline for (.{
        .{ @embedFile("../testdata/h264-mbaff-pcm-8-1.mp4"), @embedFile("../testdata/h264-mbaff-pcm-8-1.nv12") },
        .{ @embedFile("../testdata/h264-mbaff-pcm-10-2.mp4"), @embedFile("../testdata/h264-mbaff-pcm-10-2.nv12") },
        .{ @embedFile("../testdata/h264-mbaff-pcm-10-3.mp4"), @embedFile("../testdata/h264-mbaff-pcm-10-3.nv12") },
        .{ @embedFile("../testdata/h264-paff-pcm-8-1.mp4"), @embedFile("../testdata/h264-paff-pcm-8-1.nv12") },
        .{ @embedFile("../testdata/h264-paff-pcm-12-2.mp4"), @embedFile("../testdata/h264-paff-pcm-12-2.nv12") },
        .{ @embedFile("../testdata/h264-paff-pcm-14-3.mp4"), @embedFile("../testdata/h264-paff-pcm-14-3.nv12") },
    }) |pair| {
        var src = media.source.Source{ .allocator = a, .identity = "interlaced-pcm", .storage = .{ .borrowed = pair[0] } };
        var reader = try media.mp4.Reader.init(a, &src, .{});
        defer reader.deinit();
        var frame = try video.h264.decodeFrame(a, &reader, 0, .{});
        defer frame.deinit();
        try std.testing.expectEqualSlices(u8, pair[1], frame.nv12);
    }
}

test "video H264 advanced tool oracle hashes pin native-depth and field fixtures" {
    const Receipt = struct { cases: []const struct { name: []const u8, mp4_sha256: []const u8, nv12_sha256: []const u8 } };
    const receipt = try std.json.parseFromSlice(Receipt, a, @embedFile("../testdata/h264-advanced-oracle.json"), .{ .ignore_unknown_fields = true });
    defer receipt.deinit();
    const fixtures = .{
        .{ "h264-cabac-pcm", @embedFile("../testdata/h264-cabac-pcm.mp4"), @embedFile("../testdata/h264-cabac-pcm.nv12") },
        .{ "h264-poc1-pcm", @embedFile("../testdata/h264-poc1-pcm.mp4"), @embedFile("../testdata/h264-poc1-pcm.nv12") },
        .{ "h264-groups-0-0", @embedFile("../testdata/h264-groups-0-0.mp4"), @embedFile("../testdata/h264-groups-0-0.nv12") },
        .{ "h264-groups-1-0", @embedFile("../testdata/h264-groups-1-0.mp4"), @embedFile("../testdata/h264-groups-1-0.nv12") },
        .{ "h264-groups-2-0", @embedFile("../testdata/h264-groups-2-0.mp4"), @embedFile("../testdata/h264-groups-2-0.nv12") },
        .{ "h264-groups-3-0", @embedFile("../testdata/h264-groups-3-0.mp4"), @embedFile("../testdata/h264-groups-3-0.nv12") },
        .{ "h264-groups-3-1", @embedFile("../testdata/h264-groups-3-1.mp4"), @embedFile("../testdata/h264-groups-3-1.nv12") },
        .{ "h264-groups-4-0", @embedFile("../testdata/h264-groups-4-0.mp4"), @embedFile("../testdata/h264-groups-4-0.nv12") },
        .{ "h264-groups-4-1", @embedFile("../testdata/h264-groups-4-1.mp4"), @embedFile("../testdata/h264-groups-4-1.nv12") },
        .{ "h264-groups-5-0", @embedFile("../testdata/h264-groups-5-0.mp4"), @embedFile("../testdata/h264-groups-5-0.nv12") },
        .{ "h264-groups-5-1", @embedFile("../testdata/h264-groups-5-1.mp4"), @embedFile("../testdata/h264-groups-5-1.nv12") },
        .{ "h264-groups-6-0", @embedFile("../testdata/h264-groups-6-0.mp4"), @embedFile("../testdata/h264-groups-6-0.nv12") },
        .{ "h264-redundant", @embedFile("../testdata/h264-redundant.mp4"), @embedFile("../testdata/h264-redundant.nv12") },
        .{ "h264-redundant-missing-primary", @embedFile("../testdata/h264-redundant-missing-primary.mp4"), @embedFile("../testdata/h264-redundant-missing-primary.nv12") },
        .{ "h264-mbaff-pcm-8-1", @embedFile("../testdata/h264-mbaff-pcm-8-1.mp4"), @embedFile("../testdata/h264-mbaff-pcm-8-1.nv12") },
        .{ "h264-mbaff-pcm-10-2", @embedFile("../testdata/h264-mbaff-pcm-10-2.mp4"), @embedFile("../testdata/h264-mbaff-pcm-10-2.nv12") },
        .{ "h264-mbaff-pcm-10-3", @embedFile("../testdata/h264-mbaff-pcm-10-3.mp4"), @embedFile("../testdata/h264-mbaff-pcm-10-3.nv12") },
        .{ "h264-paff-pcm-8-1", @embedFile("../testdata/h264-paff-pcm-8-1.mp4"), @embedFile("../testdata/h264-paff-pcm-8-1.nv12") },
        .{ "h264-paff-pcm-12-2", @embedFile("../testdata/h264-paff-pcm-12-2.mp4"), @embedFile("../testdata/h264-paff-pcm-12-2.nv12") },
        .{ "h264-paff-pcm-14-3", @embedFile("../testdata/h264-paff-pcm-14-3.mp4"), @embedFile("../testdata/h264-paff-pcm-14-3.nv12") },
        .{ "h264-paff-prediction-8-1", @embedFile("../testdata/h264-paff-prediction-8-1.mp4"), @embedFile("../testdata/h264-paff-prediction-8-1.nv12") },
        .{ "h264-paff-prediction-10-2", @embedFile("../testdata/h264-paff-prediction-10-2.mp4"), @embedFile("../testdata/h264-paff-prediction-10-2.nv12") },
        .{ "h264-paff-prediction-14-3", @embedFile("../testdata/h264-paff-prediction-14-3.mp4"), @embedFile("../testdata/h264-paff-prediction-14-3.nv12") },
        .{ "h264-paff-b-8-1-temporal", @embedFile("../testdata/h264-paff-b-8-1-temporal.mp4"), @embedFile("../testdata/h264-paff-b-8-1-temporal.nv12") },
        .{ "h264-paff-b-8-1-spatial", @embedFile("../testdata/h264-paff-b-8-1-spatial.mp4"), @embedFile("../testdata/h264-paff-b-8-1-spatial.nv12") },
        .{ "h264-paff-b-10-2-temporal", @embedFile("../testdata/h264-paff-b-10-2-temporal.mp4"), @embedFile("../testdata/h264-paff-b-10-2-temporal.nv12") },
        .{ "h264-paff-b-10-2-spatial", @embedFile("../testdata/h264-paff-b-10-2-spatial.mp4"), @embedFile("../testdata/h264-paff-b-10-2-spatial.nv12") },
        .{ "h264-paff-b-14-3-temporal", @embedFile("../testdata/h264-paff-b-14-3-temporal.mp4"), @embedFile("../testdata/h264-paff-b-14-3-temporal.nv12") },
        .{ "h264-paff-b-14-3-spatial", @embedFile("../testdata/h264-paff-b-14-3-spatial.mp4"), @embedFile("../testdata/h264-paff-b-14-3-spatial.nv12") },
        .{ "h264-intra-dc-9-1", @embedFile("../testdata/h264-intra-dc-9-1.mp4"), @embedFile("../testdata/h264-intra-dc-9-1.nv12") },
        .{ "h264-intra-dc-9-2", @embedFile("../testdata/h264-intra-dc-9-2.mp4"), @embedFile("../testdata/h264-intra-dc-9-2.nv12") },
        .{ "h264-intra-dc-9-3", @embedFile("../testdata/h264-intra-dc-9-3.mp4"), @embedFile("../testdata/h264-intra-dc-9-3.nv12") },
        .{ "h264-intra-dc-11-1", @embedFile("../testdata/h264-intra-dc-11-1.mp4"), @embedFile("../testdata/h264-intra-dc-11-1.nv12") },
        .{ "h264-intra-dc-11-2", @embedFile("../testdata/h264-intra-dc-11-2.mp4"), @embedFile("../testdata/h264-intra-dc-11-2.nv12") },
        .{ "h264-intra-dc-11-3", @embedFile("../testdata/h264-intra-dc-11-3.mp4"), @embedFile("../testdata/h264-intra-dc-11-3.nv12") },
        .{ "h264-intra-dc-12-1", @embedFile("../testdata/h264-intra-dc-12-1.mp4"), @embedFile("../testdata/h264-intra-dc-12-1.nv12") },
        .{ "h264-intra-dc-12-2", @embedFile("../testdata/h264-intra-dc-12-2.mp4"), @embedFile("../testdata/h264-intra-dc-12-2.nv12") },
        .{ "h264-intra-dc-12-3", @embedFile("../testdata/h264-intra-dc-12-3.mp4"), @embedFile("../testdata/h264-intra-dc-12-3.nv12") },
        .{ "h264-intra-dc-13-1", @embedFile("../testdata/h264-intra-dc-13-1.mp4"), @embedFile("../testdata/h264-intra-dc-13-1.nv12") },
        .{ "h264-intra-dc-13-2", @embedFile("../testdata/h264-intra-dc-13-2.mp4"), @embedFile("../testdata/h264-intra-dc-13-2.nv12") },
        .{ "h264-intra-dc-13-3", @embedFile("../testdata/h264-intra-dc-13-3.mp4"), @embedFile("../testdata/h264-intra-dc-13-3.nv12") },
        .{ "h264-intra-dc-14-1", @embedFile("../testdata/h264-intra-dc-14-1.mp4"), @embedFile("../testdata/h264-intra-dc-14-1.nv12") },
        .{ "h264-intra-dc-14-2", @embedFile("../testdata/h264-intra-dc-14-2.mp4"), @embedFile("../testdata/h264-intra-dc-14-2.nv12") },
        .{ "h264-intra-dc-14-3", @embedFile("../testdata/h264-intra-dc-14-3.mp4"), @embedFile("../testdata/h264-intra-dc-14-3.nv12") },
        .{ "h264-intra-dc-offsets-14-3", @embedFile("../testdata/h264-intra-dc-offsets-14-3.mp4"), @embedFile("../testdata/h264-intra-dc-offsets-14-3.nv12") },
        .{ "h264-high10-cavlc", @embedFile("../testdata/h264-high10-cavlc.mp4"), @embedFile("../testdata/h264-high10-cavlc.nv12") },
        .{ "h264-high10-cabac", @embedFile("../testdata/h264-high10-cabac.mp4"), @embedFile("../testdata/h264-high10-cabac.nv12") },
        .{ "h264-high8-422-cavlc", @embedFile("../testdata/h264-high8-422-cavlc.mp4"), @embedFile("../testdata/h264-high8-422-cavlc.nv12") },
        .{ "h264-high8-422-cabac", @embedFile("../testdata/h264-high8-422-cabac.mp4"), @embedFile("../testdata/h264-high8-422-cabac.nv12") },
        .{ "h264-high10-422-cavlc", @embedFile("../testdata/h264-high10-422-cavlc.mp4"), @embedFile("../testdata/h264-high10-422-cavlc.nv12") },
        .{ "h264-high10-422-cabac", @embedFile("../testdata/h264-high10-422-cabac.mp4"), @embedFile("../testdata/h264-high10-422-cabac.nv12") },
        .{ "h264-high8-444-cavlc", @embedFile("../testdata/h264-high8-444-cavlc.mp4"), @embedFile("../testdata/h264-high8-444-cavlc.nv12") },
        .{ "h264-high8-444-cabac", @embedFile("../testdata/h264-high8-444-cabac.mp4"), @embedFile("../testdata/h264-high8-444-cabac.nv12") },
        .{ "h264-high10-444-cavlc", @embedFile("../testdata/h264-high10-444-cavlc.mp4"), @embedFile("../testdata/h264-high10-444-cavlc.nv12") },
        .{ "h264-high10-444-cabac", @embedFile("../testdata/h264-high10-444-cabac.mp4"), @embedFile("../testdata/h264-high10-444-cabac.nv12") },
        .{ "h264-high8-mbaff-cavlc", @embedFile("../testdata/h264-high8-mbaff-cavlc.mp4"), @embedFile("../testdata/h264-high8-mbaff-cavlc.nv12") },
        .{ "h264-high8-mbaff-cabac", @embedFile("../testdata/h264-high8-mbaff-cabac.mp4"), @embedFile("../testdata/h264-high8-mbaff-cabac.nv12") },
        .{ "h264-high10-mbaff-cavlc", @embedFile("../testdata/h264-high10-mbaff-cavlc.mp4"), @embedFile("../testdata/h264-high10-mbaff-cavlc.nv12") },
        .{ "h264-high10-mbaff-cabac", @embedFile("../testdata/h264-high10-mbaff-cabac.mp4"), @embedFile("../testdata/h264-high10-mbaff-cabac.nv12") },
        .{ "h264-high8-mbaff-unfiltered-cavlc", @embedFile("../testdata/h264-high8-mbaff-unfiltered-cavlc.mp4"), @embedFile("../testdata/h264-high8-mbaff-unfiltered-cavlc.nv12") },
        .{ "h264-high8-mbaff-unfiltered-cabac", @embedFile("../testdata/h264-high8-mbaff-unfiltered-cabac.mp4"), @embedFile("../testdata/h264-high8-mbaff-unfiltered-cabac.nv12") },
        .{ "h264-high10-mbaff-unfiltered-cavlc", @embedFile("../testdata/h264-high10-mbaff-unfiltered-cavlc.mp4"), @embedFile("../testdata/h264-high10-mbaff-unfiltered-cavlc.nv12") },
        .{ "h264-high10-mbaff-unfiltered-cabac", @embedFile("../testdata/h264-high10-mbaff-unfiltered-cabac.mp4"), @embedFile("../testdata/h264-high10-mbaff-unfiltered-cabac.nv12") },
        .{ "h264-high8-lossless-cavlc", @embedFile("../testdata/h264-high8-lossless-cavlc.mp4"), @embedFile("../testdata/h264-high8-lossless-cavlc.nv12") },
        .{ "h264-high8-lossless-cabac", @embedFile("../testdata/h264-high8-lossless-cabac.mp4"), @embedFile("../testdata/h264-high8-lossless-cabac.nv12") },
        .{ "h264-high10-lossless-422-cavlc", @embedFile("../testdata/h264-high10-lossless-422-cavlc.mp4"), @embedFile("../testdata/h264-high10-lossless-422-cavlc.nv12") },
        .{ "h264-high10-lossless-422-cabac", @embedFile("../testdata/h264-high10-lossless-422-cabac.mp4"), @embedFile("../testdata/h264-high10-lossless-422-cabac.nv12") },
        .{ "h264-high8-lossless-444-cavlc", @embedFile("../testdata/h264-high8-lossless-444-cavlc.mp4"), @embedFile("../testdata/h264-high8-lossless-444-cavlc.nv12") },
        .{ "h264-high8-lossless-444-cabac", @embedFile("../testdata/h264-high8-lossless-444-cabac.mp4"), @embedFile("../testdata/h264-high8-lossless-444-cabac.nv12") },
        .{ "h264-high10-lossless-444-cavlc", @embedFile("../testdata/h264-high10-lossless-444-cavlc.mp4"), @embedFile("../testdata/h264-high10-lossless-444-cavlc.nv12") },
        .{ "h264-high10-lossless-444-cabac", @embedFile("../testdata/h264-high10-lossless-444-cabac.mp4"), @embedFile("../testdata/h264-high10-lossless-444-cabac.nv12") },
    };
    try std.testing.expectEqual(@as(usize, fixtures.len), receipt.value.cases.len);
    inline for (fixtures) |fixture| {
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

test "video H264 9 through 14 bit compressed Intra16 DC and distinct chroma QP retain native samples" {
    inline for (.{
        .{ @embedFile("../testdata/h264-intra-dc-12-1.mp4"), @embedFile("../testdata/h264-intra-dc-12-1.nv12") },
        .{ @embedFile("../testdata/h264-intra-dc-12-2.mp4"), @embedFile("../testdata/h264-intra-dc-12-2.nv12") },
        .{ @embedFile("../testdata/h264-intra-dc-12-3.mp4"), @embedFile("../testdata/h264-intra-dc-12-3.nv12") },
        .{ @embedFile("../testdata/h264-intra-dc-14-1.mp4"), @embedFile("../testdata/h264-intra-dc-14-1.nv12") },
        .{ @embedFile("../testdata/h264-intra-dc-14-2.mp4"), @embedFile("../testdata/h264-intra-dc-14-2.nv12") },
        .{ @embedFile("../testdata/h264-intra-dc-14-3.mp4"), @embedFile("../testdata/h264-intra-dc-14-3.nv12") },
        .{ @embedFile("../testdata/h264-intra-dc-9-1.mp4"), @embedFile("../testdata/h264-intra-dc-9-1.nv12") },
        .{ @embedFile("../testdata/h264-intra-dc-9-2.mp4"), @embedFile("../testdata/h264-intra-dc-9-2.nv12") },
        .{ @embedFile("../testdata/h264-intra-dc-9-3.mp4"), @embedFile("../testdata/h264-intra-dc-9-3.nv12") },
        .{ @embedFile("../testdata/h264-intra-dc-11-1.mp4"), @embedFile("../testdata/h264-intra-dc-11-1.nv12") },
        .{ @embedFile("../testdata/h264-intra-dc-11-2.mp4"), @embedFile("../testdata/h264-intra-dc-11-2.nv12") },
        .{ @embedFile("../testdata/h264-intra-dc-11-3.mp4"), @embedFile("../testdata/h264-intra-dc-11-3.nv12") },
        .{ @embedFile("../testdata/h264-intra-dc-13-1.mp4"), @embedFile("../testdata/h264-intra-dc-13-1.nv12") },
        .{ @embedFile("../testdata/h264-intra-dc-13-2.mp4"), @embedFile("../testdata/h264-intra-dc-13-2.nv12") },
        .{ @embedFile("../testdata/h264-intra-dc-13-3.mp4"), @embedFile("../testdata/h264-intra-dc-13-3.nv12") },
        .{ @embedFile("../testdata/h264-intra-dc-offsets-14-3.mp4"), @embedFile("../testdata/h264-intra-dc-offsets-14-3.nv12") },
    }) |pair| {
        var src = media.source.Source{ .allocator = a, .identity = "high-depth-dc", .storage = .{ .borrowed = pair[0] } };
        var reader = try media.mp4.Reader.init(a, &src, .{});
        defer reader.deinit();
        var frame = try video.h264.decodeFrame(a, &reader, 0, .{});
        defer frame.deinit();
        try std.testing.expectEqualSlices(u8, pair[1], frame.nv12);
    }
}

test "video H264 multi slice holes overlap and admission release fail closed" {
    var src = media.source.Source{ .allocator = a, .identity = "slice-negative", .storage = .{ .borrowed = @embedFile("../testdata/h264-groups-6-0.mp4") } };
    var reader = try media.mp4.Reader.init(a, &src, .{});
    defer reader.deinit();
    var input = try reader.readPacket(0);
    const original = try a.dupe(u8, input.bytes);
    defer a.free(original);
    input.deinit();
    const first_size = 4 + std.mem.readInt(u32, original[0..4], .big);
    const doubled = try std.mem.concat(a, u8, &.{ original, original[0..first_size] });
    defer a.free(doubled);
    reader.packets[0].offset = 0;
    var pool = media.admission.Pool{ .limits = .{ .host_bytes = 1024 * 1024 } };
    src.admission_pool = &pool;
    src.storage = .{ .borrowed = original[0..first_size] };
    reader.packets[0].size = first_size;
    try std.testing.expectError(error.IncompleteVideoPicture, video.h264.decodeFrame(a, &reader, 0, .{}));
    try std.testing.expectEqual(media.admission.Resources{}, pool.snapshot());
    src.storage = .{ .borrowed = doubled };
    reader.packets[0].size = @intCast(doubled.len);
    try std.testing.expectError(error.OverlappingVideoSlices, video.h264.decodeFrame(a, &reader, 0, .{}));
    try std.testing.expectEqual(media.admission.Resources{}, pool.snapshot());
    src.storage = .{ .borrowed = original };
    reader.packets[0].size = @intCast(original.len);
    try std.testing.expectError(error.ResourceLimitExceeded, video.h264.decodeFrame(a, &reader, 0, .{ .max_slices = 1 }));
    try std.testing.expectEqual(media.admission.Resources{}, pool.snapshot());
    var frame = try video.h264.decodeFrame(a, &reader, 0, .{});
    frame.deinit();
    try std.testing.expectEqual(media.admission.Resources{}, pool.snapshot());
}

test "video H264 field chroma high depth and explicit group allocation failures unwind" {
    const Harness = struct {
        fn run(allocator: std.mem.Allocator, bytes: []const u8) !void {
            var src = media.source.Source{ .allocator = allocator, .identity = "advanced-allocation", .storage = .{ .borrowed = bytes } };
            var reader = try media.mp4.Reader.init(allocator, &src, .{});
            defer reader.deinit();
            var frame = try video.h264.decodeFrame(allocator, &reader, 0, .{});
            frame.deinit();
        }
    };
    inline for (.{ @embedFile("../testdata/h264-groups-6-0.mp4"), @embedFile("../testdata/h264-paff-pcm-14-3.mp4"), @embedFile("../testdata/h264-high10-444-cabac.mp4"), @embedFile("../testdata/h264-high10-mbaff-cabac.mp4") }) |bytes| {
        try std.testing.checkAllAllocationFailures(a, Harness.run, .{bytes});
    }
}

test "video H264 scaling fallback A B include chroma eight by eight matrices" {
    const Bits = @import("h264_bits.zig").Bits;
    const Matrices = @import("h264_scaling.zig").Matrices;
    var sequence = try Bits.init(a, &.{ 0x67, 0x80, 0x04 });
    defer sequence.deinit();
    var matrices = Matrices{};
    try matrices.parseSequence(&sequence, 3);
    try sequence.finish();
    try std.testing.expectEqualSlices(u8, &matrices.four[0], &matrices.four[1]);
    try std.testing.expectEqualSlices(u8, &matrices.four[1], &matrices.four[2]);
    try std.testing.expectEqualSlices(u8, &matrices.four[3], &matrices.four[4]);
    try std.testing.expect(matrices.four[0][0] != matrices.four[3][0]);
    for (2..6) |i| try std.testing.expectEqualSlices(u8, &matrices.eight[i - 2], &matrices.eight[i]);
    @memset(&matrices.four[0], 17);
    @memset(&matrices.four[3], 19);
    @memset(&matrices.eight[0], 23);
    @memset(&matrices.eight[1], 29);
    var picture = try Bits.init(a, &.{ 0x68, 0x80, 0x04 });
    defer picture.deinit();
    try matrices.parsePicture(&picture, true, 3);
    try picture.finish();
    try std.testing.expectEqual(@as(u8, 17), matrices.four[2][15]);
    try std.testing.expectEqual(@as(u8, 19), matrices.four[5][15]);
    try std.testing.expectEqual(@as(u8, 23), matrices.eight[4][63]);
    try std.testing.expectEqual(@as(u8, 29), matrices.eight[5][63]);
    var malformed = try Bits.init(a, &.{ 0x67, 0xc0, 0x20, 0x50 });
    defer malformed.deinit();
    try std.testing.expectError(error.MalformedVideoConfig, matrices.parseSequence(&malformed, 3));
}

test "video H264 PAFF prediction includes current first field references" {
    inline for (.{
        .{ @embedFile("../testdata/h264-paff-prediction-8-1.mp4"), @embedFile("../testdata/h264-paff-prediction-8-1.nv12") },
        .{ @embedFile("../testdata/h264-paff-prediction-10-2.mp4"), @embedFile("../testdata/h264-paff-prediction-10-2.nv12") },
        .{ @embedFile("../testdata/h264-paff-prediction-14-3.mp4"), @embedFile("../testdata/h264-paff-prediction-14-3.nv12") },
    }) |pair| {
        var src = media.source.Source{ .allocator = a, .identity = "paff-reference", .storage = .{ .borrowed = pair[0] } };
        var reader = try media.mp4.Reader.init(a, &src, .{});
        defer reader.deinit();
        const size = pair[1].len / reader.packets.len;
        for (reader.packets, 0..) |_, i| {
            var frame = try video.h264.decodeFrame(a, &reader, i, .{});
            defer frame.deinit();
            try std.testing.expectEqualSlices(u8, pair[1][i * size ..][0..size], frame.nv12);
        }
    }
}

test "video H264 redundant copies preserve complete primary picture" {
    var src = media.source.Source{ .allocator = a, .identity = "redundant", .storage = .{ .borrowed = @embedFile("../testdata/h264-redundant.mp4") } };
    var reader = try media.mp4.Reader.init(a, &src, .{});
    defer reader.deinit();
    var frame = try video.h264.decodeFrame(a, &reader, 0, .{});
    defer frame.deinit();
    try std.testing.expectEqualSlices(u8, @embedFile("../testdata/h264-redundant.nv12"), frame.nv12);
    var missing = media.source.Source{ .allocator = a, .identity = "missing-primary", .storage = .{ .borrowed = @embedFile("../testdata/h264-redundant-missing-primary.mp4") } };
    var invalid = try media.mp4.Reader.init(a, &missing, .{});
    defer invalid.deinit();
    try std.testing.expectError(error.MissingPrimaryVideoSlice, video.h264.decodeFrame(a, &invalid, 0, .{}));
}

test "video H264 native high depth host preparation honors range and plane geometry" {
    var luma: [512]u8 = undefined;
    var chroma: [1024]u8 = undefined;
    for (0..256) |i| std.mem.writeInt(u16, luma[i * 2 ..][0..2], 32, .little);
    for (0..512) |i| std.mem.writeInt(u16, chroma[i * 2 ..][0..2], 8192, .little);
    var host = video.preparation.HostSurface{ .bit_depth = 14, .chroma_format = 3, .width = 16, .height = 16, .format = .nv12_full, .planes = .{ .{ .bytes = &luma, .width = 16, .height = 16, .stride = 32 }, .{ .bytes = &chroma, .width = 16, .height = 16, .stride = 64 } } };
    const patches = try video.preparation.referenceHost(a, host, .{ .width = 48, .height = 48, .matrix = .bt709, .centered = false }, .{});
    defer a.free(patches);
    // 32/16383 * 255 is below the RGB8 half-step, so neutral full-range
    // pixels round to black. Scaling by 2^(depth-8) would round them to one.
    for (patches) |value| try std.testing.expectEqual(@as(f32, 0), value);
    host.planes[1].stride = 32;
    try std.testing.expectError(error.UnsupportedSurfaceFormat, host.validate());
    host.planes[1].stride = 64;
    host.bit_depth = 15;
    try std.testing.expectError(error.UnsupportedSurfaceFormat, host.validate());
}

test "video H264 PAFF spatial and temporal B skip preserve field presentation samples" {
    inline for (.{
        .{ @embedFile("../testdata/h264-paff-b-8-1-spatial.mp4"), @embedFile("../testdata/h264-paff-b-8-1-spatial.nv12") },
        .{ @embedFile("../testdata/h264-paff-b-8-1-temporal.mp4"), @embedFile("../testdata/h264-paff-b-8-1-temporal.nv12") },
        .{ @embedFile("../testdata/h264-paff-b-10-2-spatial.mp4"), @embedFile("../testdata/h264-paff-b-10-2-spatial.nv12") },
        .{ @embedFile("../testdata/h264-paff-b-10-2-temporal.mp4"), @embedFile("../testdata/h264-paff-b-10-2-temporal.nv12") },
        .{ @embedFile("../testdata/h264-paff-b-14-3-spatial.mp4"), @embedFile("../testdata/h264-paff-b-14-3-spatial.nv12") },
        .{ @embedFile("../testdata/h264-paff-b-14-3-temporal.mp4"), @embedFile("../testdata/h264-paff-b-14-3-temporal.nv12") },
    }) |pair| {
        var src = media.source.Source{ .allocator = a, .identity = "paff-b", .storage = .{ .borrowed = pair[0] } };
        var reader = try media.mp4.Reader.init(a, &src, .{});
        defer reader.deinit();
        const size = pair[1].len / reader.packets.len;
        for (reader.packets, 0..) |packet, i| {
            var frame = try video.h264.decodeFrame(a, &reader, i, .{});
            defer frame.deinit();
            const ordinal: usize = @intCast(@divExact(packet.pts, packet.duration));
            try std.testing.expectEqualSlices(u8, pair[1][ordinal * size ..][0..size], frame.nv12);
        }
    }
}

test "video H264 High422 rejects 444 profile mismatch before allocation" {
    var src = media.source.Source{ .allocator = a, .identity = "invalid-high422", .storage = .{ .borrowed = @embedFile("../testdata/h264-high10-444-cavlc.mp4") } };
    var reader = try media.mp4.Reader.init(a, &src, .{});
    defer reader.deinit();
    const original = reader.track.avcc;
    const invalid = try a.dupe(u8, original);
    defer a.free(invalid);
    invalid[1] = 122;
    invalid[9] = 122; // SPS profile_idc; chroma_format_idc remains 3.
    reader.track.avcc = invalid;
    defer reader.track.avcc = original;
    try std.testing.expectError(error.UnsupportedVideoProfile, video.h264.decodeFrame(a, &reader, 0, .{}));
}

const paff_packet_cases = .{
    .{ "h264-paff-split-packets-8-1", @embedFile("../testdata/h264-paff-split-packets-8-1.mp4"), @embedFile("../testdata/h264-paff-split-packets-8-1.nv12"), true },
    .{ "h264-paff-long-paired-8-1", @embedFile("../testdata/h264-paff-long-paired-8-1.mp4"), @embedFile("../testdata/h264-paff-long-paired-8-1.nv12"), false },
    .{ "h264-paff-long-packets-8-1", @embedFile("../testdata/h264-paff-long-packets-8-1.mp4"), @embedFile("../testdata/h264-paff-long-packets-8-1.nv12"), true },
    .{ "h264-paff-adaptive-paired-8-1", @embedFile("../testdata/h264-paff-adaptive-paired-8-1.mp4"), @embedFile("../testdata/h264-paff-adaptive-paired-8-1.nv12"), false },
    .{ "h264-paff-adaptive-packets-8-1", @embedFile("../testdata/h264-paff-adaptive-packets-8-1.mp4"), @embedFile("../testdata/h264-paff-adaptive-packets-8-1.nv12"), true },
    .{ "h264-paff-cabac-packets-8-1", @embedFile("../testdata/h264-paff-cabac-packets-8-1.mp4"), @embedFile("../testdata/h264-paff-cabac-packets-8-1.nv12"), true },
    .{ "h264-paff-bottom-packets-8-1", @embedFile("../testdata/h264-paff-bottom-packets-8-1.mp4"), @embedFile("../testdata/h264-paff-bottom-packets-8-1.nv12"), true },
    .{ "h264-paff-idr-long-packets-8-1", @embedFile("../testdata/h264-paff-idr-long-packets-8-1.mp4"), @embedFile("../testdata/h264-paff-idr-long-packets-8-1.nv12"), true },
    .{ "h264-paff-b-spatial-packets-8-1", @embedFile("../testdata/h264-paff-b-spatial-packets-8-1.mp4"), @embedFile("../testdata/h264-paff-b-spatial-packets-8-1.nv12"), true },
    .{ "h264-paff-b-temporal-packets-8-1", @embedFile("../testdata/h264-paff-b-temporal-packets-8-1.mp4"), @embedFile("../testdata/h264-paff-b-temporal-packets-8-1.nv12"), true },
    .{ "h264-paff-split-packets-10-2", @embedFile("../testdata/h264-paff-split-packets-10-2.mp4"), @embedFile("../testdata/h264-paff-split-packets-10-2.nv12"), true },
    .{ "h264-paff-long-paired-10-2", @embedFile("../testdata/h264-paff-long-paired-10-2.mp4"), @embedFile("../testdata/h264-paff-long-paired-10-2.nv12"), false },
    .{ "h264-paff-long-packets-10-2", @embedFile("../testdata/h264-paff-long-packets-10-2.mp4"), @embedFile("../testdata/h264-paff-long-packets-10-2.nv12"), true },
    .{ "h264-paff-adaptive-paired-10-2", @embedFile("../testdata/h264-paff-adaptive-paired-10-2.mp4"), @embedFile("../testdata/h264-paff-adaptive-paired-10-2.nv12"), false },
    .{ "h264-paff-adaptive-packets-10-2", @embedFile("../testdata/h264-paff-adaptive-packets-10-2.mp4"), @embedFile("../testdata/h264-paff-adaptive-packets-10-2.nv12"), true },
    .{ "h264-paff-cabac-packets-10-2", @embedFile("../testdata/h264-paff-cabac-packets-10-2.mp4"), @embedFile("../testdata/h264-paff-cabac-packets-10-2.nv12"), true },
    .{ "h264-paff-bottom-packets-10-2", @embedFile("../testdata/h264-paff-bottom-packets-10-2.mp4"), @embedFile("../testdata/h264-paff-bottom-packets-10-2.nv12"), true },
    .{ "h264-paff-idr-long-packets-10-2", @embedFile("../testdata/h264-paff-idr-long-packets-10-2.mp4"), @embedFile("../testdata/h264-paff-idr-long-packets-10-2.nv12"), true },
    .{ "h264-paff-b-spatial-packets-10-2", @embedFile("../testdata/h264-paff-b-spatial-packets-10-2.mp4"), @embedFile("../testdata/h264-paff-b-spatial-packets-10-2.nv12"), true },
    .{ "h264-paff-b-temporal-packets-10-2", @embedFile("../testdata/h264-paff-b-temporal-packets-10-2.mp4"), @embedFile("../testdata/h264-paff-b-temporal-packets-10-2.nv12"), true },
    .{ "h264-paff-split-packets-14-3", @embedFile("../testdata/h264-paff-split-packets-14-3.mp4"), @embedFile("../testdata/h264-paff-split-packets-14-3.nv12"), true },
    .{ "h264-paff-long-paired-14-3", @embedFile("../testdata/h264-paff-long-paired-14-3.mp4"), @embedFile("../testdata/h264-paff-long-paired-14-3.nv12"), false },
    .{ "h264-paff-long-packets-14-3", @embedFile("../testdata/h264-paff-long-packets-14-3.mp4"), @embedFile("../testdata/h264-paff-long-packets-14-3.nv12"), true },
    .{ "h264-paff-adaptive-paired-14-3", @embedFile("../testdata/h264-paff-adaptive-paired-14-3.mp4"), @embedFile("../testdata/h264-paff-adaptive-paired-14-3.nv12"), false },
    .{ "h264-paff-adaptive-packets-14-3", @embedFile("../testdata/h264-paff-adaptive-packets-14-3.mp4"), @embedFile("../testdata/h264-paff-adaptive-packets-14-3.nv12"), true },
    .{ "h264-paff-cabac-packets-14-3", @embedFile("../testdata/h264-paff-cabac-packets-14-3.mp4"), @embedFile("../testdata/h264-paff-cabac-packets-14-3.nv12"), true },
    .{ "h264-paff-bottom-packets-14-3", @embedFile("../testdata/h264-paff-bottom-packets-14-3.mp4"), @embedFile("../testdata/h264-paff-bottom-packets-14-3.nv12"), true },
    .{ "h264-paff-idr-long-packets-14-3", @embedFile("../testdata/h264-paff-idr-long-packets-14-3.mp4"), @embedFile("../testdata/h264-paff-idr-long-packets-14-3.nv12"), true },
    .{ "h264-paff-b-spatial-packets-14-3", @embedFile("../testdata/h264-paff-b-spatial-packets-14-3.mp4"), @embedFile("../testdata/h264-paff-b-spatial-packets-14-3.nv12"), true },
    .{ "h264-paff-b-temporal-packets-14-3", @embedFile("../testdata/h264-paff-b-temporal-packets-14-3.mp4"), @embedFile("../testdata/h264-paff-b-temporal-packets-14-3.nv12"), true },
};

test "video H264 PAFF standalone packets and field MMCO match independent native samples" {
    const Receipt = struct { cases: []const struct { name: []const u8, mp4_sha256: []const u8, nv12_sha256: []const u8 } };
    const receipt = try std.json.parseFromSlice(Receipt, a, @embedFile("../testdata/h264-paff-packets-oracle.json"), .{ .ignore_unknown_fields = true });
    defer receipt.deinit();
    try std.testing.expectEqual(paff_packet_cases.len, receipt.value.cases.len);
    inline for (paff_packet_cases, 0..) |pair, case_index| {
        try std.testing.expectEqualStrings(pair[0], receipt.value.cases[case_index].name);
        inline for (.{ pair[1], pair[2] }, 0..) |bytes, file_index| {
            var hash: [32]u8 = undefined;
            std.crypto.hash.sha2.Sha256.hash(bytes, &hash, .{});
            try std.testing.expectEqualStrings(if (file_index == 0) receipt.value.cases[case_index].mp4_sha256 else receipt.value.cases[case_index].nv12_sha256, &std.fmt.bytesToHex(hash, .lower));
        }
        var pool = media.admission.Pool{ .limits = .{ .host_bytes = 32 * 1024 * 1024 } };
        var src = media.source.Source{ .allocator = a, .admission_pool = &pool, .identity = pair[0], .storage = .{ .borrowed = pair[1] } };
        var reader = try media.mp4.Reader.init(a, &src, .{ .max_index_bytes = 65536 });
        defer reader.deinit();
        const per_picture: usize = if (pair[3]) 2 else 1;
        const size = pair[2].len / (reader.packets.len / per_picture);
        const retained = pool.snapshot();
        for (reader.packets, 0..) |_, i| {
            var frame = video.h264.decodeFrame(a, &reader, i, .{}) catch |err| {
                std.debug.print("PAFF case {s}, packet {d}: {s}\n", .{ pair[0], i, @errorName(err) });
                return err;
            };
            const ordinal: usize = @intCast(@divExact(reader.packets[i / per_picture * per_picture].pts, @as(i64, @intCast(per_picture))));
            try std.testing.expectEqualSlices(u8, pair[2][ordinal * size ..][0..size], frame.nv12);
            try std.testing.expectEqual(@as(i64, @intCast(ordinal * per_picture)), frame.pts);
            try std.testing.expectEqual(@as(u32, @intCast(per_picture)), frame.duration);
            try std.testing.expectEqual((i / per_picture + 1) * per_picture, frame.decoded_packets);
            frame.deinit();
            try std.testing.expectEqual(retained, pool.snapshot());
        }
    }
}

test "video H264 PAFF standalone selection completes once and preserves callback slots" {
    const bytes = @embedFile("../testdata/h264-paff-split-packets-14-3.mp4");
    const known = @embedFile("../testdata/h264-paff-split-packets-14-3.nv12");
    var src = media.source.Source{ .allocator = a, .identity = "paff-selection", .storage = .{ .borrowed = bytes } };
    var reader = try media.mp4.Reader.init(a, &src, .{});
    defer reader.deinit();
    const Collector = struct {
        visited: [4]bool = @splat(false),
        fn publish(context: *anyopaque, slot: usize, frame: *const video.h264.Frame) !void {
            const self: *@This() = @ptrCast(@alignCast(context));
            const ordinal: usize = if (slot % 2 == 0) 2 else 0;
            const size = known.len / 3;
            try std.testing.expectEqualSlices(u8, known[ordinal * size ..][0..size], frame.nv12);
            try std.testing.expectEqual(@as(i64, @intCast(ordinal * 2)), frame.pts);
            try std.testing.expect(!self.visited[slot]);
            self.visited[slot] = true;
        }
    };
    var collector = Collector{};
    const stats = try video.h264.decodeSelected(a, &reader, &.{ 4, 0, 5, 1 }, .{}, &collector, Collector.publish);
    try std.testing.expectEqual(@as(usize, 6), stats.decoded_packets);
    var payload: u64 = 0;
    for (reader.packets) |packet| payload += packet.size;
    try std.testing.expectEqual(payload, stats.payload_bytes);
    for (collector.visited) |visited| try std.testing.expect(visited);
    try std.testing.expectError(error.ResourceLimitExceeded, video.h264.decodeFrame(a, &reader, 0, .{ .max_dependency_packets = 1 }));
    try std.testing.expectError(error.ResourceLimitExceeded, video.h264.decodeFrame(a, &reader, 0, .{ .max_slices = 1 }));
    const packets = reader.packets;
    reader.packets = packets[0..1];
    defer reader.packets = packets;
    try std.testing.expectError(error.IncompleteVideoPicture, video.h264.decodeFrame(a, &reader, 0, .{}));
}

test "video H264 PAFF standalone allocation failures release partial field references" {
    const Harness = struct {
        fn run(allocator: std.mem.Allocator) !void {
            var src = media.source.Source{ .allocator = allocator, .identity = "paff-failures", .storage = .{ .borrowed = @embedFile("../testdata/h264-paff-long-packets-14-3.mp4") } };
            var reader = try media.mp4.Reader.init(allocator, &src, .{});
            defer reader.deinit();
            var frame = try video.h264.decodeFrame(allocator, &reader, reader.packets.len - 1, .{});
            defer frame.deinit();
        }
    };
    try std.testing.checkAllAllocationFailures(a, Harness.run, .{});
}

test "video H264 PAFF field marking preserves complementary identities and long PicNum 31" {
    const State = @import("h264_references.zig").State;
    const Motion = @import("h264_motion.zig").Motion;
    const planar: [24]u8 = @splat(0);
    const motions = [_]Motion{.{ .decoded = true }};
    const pair = [2][]const Motion{ &motions, &motions };
    var state = State{ .reference = true, .field_picture = true, .paired = true };
    defer state.deinit(a);
    for (0..2) |parity| {
        state.field_parity = parity;
        state.current_fields = .{ parity == 0, parity == 1 };
        state.current_field_poc[parity] = @intCast(parity);
        state.order(4);
        try referenceMarking(&state, &.{});
        try state.commit(a, &planar, pair, 16);
    }
    const original_id = state.pictures[0].id;
    state.current_num = 1;
    state.field_parity = 0;
    state.current_fields = .{ true, false };
    state.order(4);
    try referenceMarking(&state, &.{ .{ .operation = 4, .first = 16 }, .{ .operation = 3, .first = 1, .second = 15 }, .{ .operation = 6, .first = 14 } });
    try state.commit(a, &planar, pair, 16);
    try std.testing.expectEqual(original_id, state.pictures[0].id);
    try std.testing.expectEqual(@as(?u32, 15), state.pictures[0].field_long[0]);
    try std.testing.expectEqual(@as(?u32, null), state.pictures[0].field_long[1]);
    try std.testing.expect(state.pictures[0].fields[1]);
    state.order(4);
    state.orderFields(0, false);
    try std.testing.expectEqual(@as(u8, 1), state.field_lists[0][0]); // old bottom remains short-term
    try std.testing.expectEqual(@as(u8, 2), state.field_lists[0][1]); // long index 14 before 15
    try std.testing.expectEqual(@as(u8, 0), state.field_lists[0][2]);
    state.field_parity = 1;
    state.current_fields = .{ false, true };
    try referenceMarking(&state, &.{ .{ .operation = 3, .first = 1, .second = 15 }, .{ .operation = 6, .first = 14 } });
    try state.commit(a, &planar, pair, 16);
    try std.testing.expectEqual(@as(?u32, 15), state.pictures[0].field_long[1]);
    try std.testing.expectEqual(@as(?u32, 14), state.pictures[1].long_term);
    const complementary_id = state.pictures[1].id;
    state.current_num = 2;
    state.field_parity = 0;
    state.current_fields = .{ true, false };
    state.order(4);
    try referenceMarking(&state, &.{ .{ .operation = 2, .first = 31 }, .{ .operation = 6, .first = 14 } });
    try state.commit(a, &planar, pair, 16);
    try std.testing.expectEqual(@as(usize, 2), state.count);
    try std.testing.expectEqual(original_id, state.pictures[0].id);
    try std.testing.expect(!state.pictures[0].fields[0] and state.pictures[0].fields[1]);
    for (state.pictures[0..state.count]) |pic| try std.testing.expect(pic.id != complementary_id);
    const current_id = state.pictures[1].id;
    state.field_parity = 1;
    state.current_fields = .{ false, true };
    state.order(4);
    try referenceMarking(&state, &.{ .{ .operation = 2, .first = 31 }, .{ .operation = 6, .first = 14 } });
    try state.commit(a, &planar, pair, 16);
    try std.testing.expectEqual(@as(usize, 1), state.count);
    try std.testing.expectEqual(current_id, state.pictures[0].id);
    try std.testing.expect(state.pictures[0].fields[0] and state.pictures[0].fields[1]);
    state.current_num = 3;
    try referenceMarking(&state, &.{ .{ .operation = 4, .first = 0 }, .{ .operation = 6, .first = 0 } });
    try std.testing.expectError(error.MalformedVideoPacket, state.commit(a, &planar, pair, 16));
    try referenceMarking(&state, &.{.{ .operation = 4, .first = 2 }});
    try std.testing.expectError(error.MalformedVideoPacket, state.commit(a, &planar, pair, 1));
}

test "video H264 PAFF standalone mismatched complement and cancellation release admission" {
    const bytes = @embedFile("../testdata/h264-paff-split-packets-14-3.mp4");
    const known = @embedFile("../testdata/h264-paff-split-packets-14-3.nv12");
    var src = media.source.Source{ .allocator = a, .identity = "paff-cancellation", .storage = .{ .borrowed = bytes } };
    var reader = try media.mp4.Reader.init(a, &src, .{});
    defer reader.deinit();
    var pool = media.admission.Pool{ .limits = .{ .host_bytes = 1024 * 1024 } };
    src.admission_pool = &pool;
    const retained = src.retained_bytes;
    const original = reader.packets[1];
    reader.packets[1] = reader.packets[3];
    try std.testing.expectError(error.MixedVideoPictures, video.h264.decodeFrame(a, &reader, 0, .{}));
    try std.testing.expectEqual(media.admission.Resources{}, pool.snapshot());
    try std.testing.expectEqual(retained, src.retained_bytes);
    reader.packets[1] = original;
    const Cancel = struct {
        input: *media.source.Source,
        after_reads: usize,
        fn check(ctx: ?*const anyopaque) !void {
            const self: *const @This() = @ptrCast(@alignCast(ctx.?));
            if (self.input.reads >= self.after_reads) return error.Cancelled;
        }
    };
    const cancel = Cancel{ .input = &src, .after_reads = src.reads + 3 };
    src.control = .{ .context = &cancel, .check_fn = Cancel.check };
    try std.testing.expectError(error.Cancelled, video.h264.decodeFrame(a, &reader, 0, .{}));
    try std.testing.expectEqual(media.admission.Resources{}, pool.snapshot());
    try std.testing.expectEqual(retained, src.retained_bytes);
    src.control = .{};
    reader.packets[0].pts = -4;
    reader.packets[1].pts = -3;
    var frame = try video.h264.decodeFrame(a, &reader, 1, .{});
    try std.testing.expectEqual(@as(i64, -4), frame.pts);
    try std.testing.expectEqual(@as(u32, 2), frame.duration);
    try std.testing.expectEqualSlices(u8, known[0 .. known.len / 3], frame.nv12);
    frame.deinit();
    try std.testing.expectEqual(media.admission.Resources{}, pool.snapshot());
}
