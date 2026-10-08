// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
const std = @import("std");
const sampling = @import("sampling.zig");
const media = @import("antfly_media");
const Case = struct {
    total_frames: usize,
    source_fps: ?f64,
    duration_seconds: ?f64,
    fps: ?f64,
    max_frames: ?usize,
    overflow: ?sampling.Overflow,
    indexes: []const usize,
};
test "sampling matches pinned Python reference snapshot at FPS and cap boundaries" {
    const manifest = try std.json.parseFromSlice(struct { cases: []const Case, reference_snapshot_sha256: []const u8, upstream_snapshot_executed: bool }, std.testing.allocator, @embedFile("../testdata/sampling-oracle.json"), .{ .ignore_unknown_fields = true });
    defer manifest.deinit();
    try std.testing.expect(manifest.value.upstream_snapshot_executed);
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(@embedFile("../testdata/reference/hf_sample_frames.py"), &digest, .{});
    try std.testing.expectEqualStrings(manifest.value.reference_snapshot_sha256, &std.fmt.bytesToHex(digest, .lower));
    for (manifest.value.cases) |case| {
        const indexes = try sampling.embeddingGemma2(std.testing.allocator, .{ .total_frames = case.total_frames, .fps = case.source_fps, .duration_seconds = case.duration_seconds }, .{ .fps = case.fps, .max_frames = case.max_frames, .overflow = case.overflow });
        defer std.testing.allocator.free(indexes);
        try std.testing.expectEqualSlices(usize, case.indexes, indexes);
    }
}
fn allocations(allocator: std.mem.Allocator) !void {
    const indexes = try sampling.embeddingGemma2(allocator, .{ .total_frames = 1800, .fps = 30, .duration_seconds = 60 }, .{});
    allocator.free(indexes);
}
test "sampling allocation failure is bounded to selected output" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, allocations, .{});
    // Fifty million candidates still allocate only the configured 32 outputs.
    const indexes = try sampling.embeddingGemma2(std.testing.allocator, .{ .total_frames = 500_000, .fps = 10, .duration_seconds = 50_000 }, .{ .fps = 1000 });
    defer std.testing.allocator.free(indexes);
    try std.testing.expectEqual(@as(usize, 32), indexes.len);
}
test "sampling invalid metadata, native timestamp limits and cancellation" {
    const a = std.testing.allocator;
    try std.testing.expectError(error.InvalidSamplingMetadata, sampling.embeddingGemma2(a, .{ .total_frames = 2, .fps = std.math.nan(f64) }, .{}));
    try std.testing.expectError(error.InvalidSamplingPolicy, sampling.embeddingGemma2(a, .{ .total_frames = 2 }, .{ .max_frames = null }));
    try std.testing.expectError(error.InvalidSamplingPolicy, sampling.embeddingGemma2(a, .{ .total_frames = 2 }, .{ .max_frames = 0 }));
    try std.testing.expectError(error.SamplingOverflow, sampling.embeddingGemma2(a, .{ .total_frames = 2, .fps = 1, .duration_seconds = 1e30 }, .{}));
    try std.testing.expectError(error.ResourceLimitExceeded, sampling.timestamps(a, &.{}, .{ .start = 0, .end = 1000 }, 1, 32, .{}));
    const Cancel = struct {
        fn check(_: ?*const anyopaque) !void {
            return error.DeadlineExceeded;
        }
    };
    try std.testing.expectError(error.DeadlineExceeded, sampling.timestamps(a, &.{}, .{ .start = 0, .end = 1 }, 1, 32, .{ .check_fn = Cancel.check }));
    _ = media;
}
