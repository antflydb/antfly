// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Native Zig coverage-guided targets plus checked-in corpus replay.
const std = @import("std");
const media = @import("antfly_media");
const h264 = @import("h264.zig");
const Guard = struct {
    checks: usize = 0,
    fn check(context: ?*const anyopaque) !void {
        const self: *Guard = @ptrCast(@alignCast(@constCast(context.?)));
        self.checks += 1;
        if (self.checks > 10_000) return error.DeadlineExceeded;
    }
};
fn decode(bytes: []const u8) !void {
    if (bytes.len > 1024 * 1024) return;
    const a = std.testing.allocator;
    var guard = Guard{};
    var src = media.source.Source{ .allocator = a, .identity = "fuzz-corpus", .storage = .{ .borrowed = bytes }, .control = .{ .context = &guard, .check_fn = Guard.check }, .limits = .{ .max_total_bytes = 16 * 1024 * 1024 } };
    {
        var reader = media.mp4.Reader.init(a, &src, .{ .max_samples = 128, .max_boxes = 2048, .max_index_bytes = 1024 * 1024, .max_metadata_bytes = 1024 * 1024 }) catch return;
        defer reader.deinit();
        if (reader.packets.len == 0) return;
        var frame = h264.decodeFrame(a, &reader, reader.packets.len - 1, .{ .max_pixels = 1024 * 1024, .max_packet_bytes = 1024 * 1024, .max_decode_bytes = 64 * 1024 * 1024, .max_dependency_packets = 128, .max_slices = 64, .max_parameter_packets = 128, .max_parameter_bytes = 1024 * 1024 }) catch return;
        frame.deinit();
    }
    try std.testing.expectEqual(@as(usize, 0), src.retained_bytes);
}
fn corpus(comptime bytes: []const u8) []const u8 {
    return comptime blk: {
        var encoded: [4 + bytes.len]u8 = undefined;
        std.mem.writeInt(u32, encoded[0..4], @intCast(bytes.len), .little);
        @memcpy(encoded[4..], bytes);
        const frozen = encoded;
        break :blk &frozen;
    };
}
test "video fuzz H264 containers reconstruction and allocation cleanup" {
    const Harness = struct {
        fn run(_: void, smith: *std.testing.Smith) !void {
            var buffer: [128 * 1024]u8 = undefined;
            const length = smith.sliceWithHash(&buffer, 0xA17F1001);
            try decode(buffer[0..length]);
        }
    };
    try std.testing.fuzz({}, Harness.run, .{ .corpus = &.{ corpus(@embedFile("../testdata/h264-sintel-original.mp4")), corpus(@embedFile("../testdata/h264-dynamic-geometry.mp4")), corpus(@embedFile("../testdata/h264-mono-14-cabac.mp4")), corpus(@embedFile("../testdata/h264-gaps-poc1-cavlc.mp4")), corpus(@embedFile("../testdata/h264-jm-partitions.mp4")), corpus(@embedFile("../testdata/h264-jm-separate-14.mp4")), corpus(@embedFile("../testdata/h264-jm-secondary-sp.mp4")) } });
}
fn configCorpus(comptime bytes: []const u8) []const u8 {
    return comptime blk: {
        @setEvalBranchQuota(1_000_000);
        const offset = std.mem.indexOf(u8, bytes, "avcC").?;
        const size = std.mem.readInt(u32, bytes[offset - 4 ..][0..4], .big) - 8;
        break :blk corpus(bytes[offset + 4 ..][0..size]);
    };
}
test "video fuzz H264 parameter sets reject arbitrary syntax without leaks" {
    const Harness = struct {
        fn run(_: void, smith: *std.testing.Smith) !void {
            {
                var buffer: [4096]u8 = undefined;
                const length = smith.sliceWithHash(&buffer, 0xA17F1002);
                const bytes = buffer[0..length];
                const cfg = h264.configParse(std.testing.allocator, bytes) catch return;
                cfg.groups.deinit(std.testing.allocator);
            }
        }
    };
    try std.testing.fuzz({}, Harness.run, .{ .corpus = &.{ configCorpus(@embedFile("../testdata/h264-sintel-original.mp4")), configCorpus(@embedFile("../testdata/h264-jm-partitions.mp4")), configCorpus(@embedFile("../testdata/h264-jm-separate-14.mp4")), configCorpus(@embedFile("../testdata/h264-mono-10-cavlc.mp4")) } });
}
