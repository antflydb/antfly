// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
const std = @import("std");

pub const names = [_][]const u8{
    "buffer_upload_bytes",       "buffer_upload_calls",    "buffer_download_bytes", "buffer_download_calls",
    "buffer_copy_bytes",         "buffer_copy_calls",      "commands_completed",    "commands_failed",
    "completed_gpu_nanoseconds", "gpu_timestamps_missing",
};
pub const Snapshot = [names.len]u64;
extern fn termite_metal_telemetry_snapshot(values: [*]u64) void;

/// Worker-process counters: no reset, per-request attribution or driver wait.
/// Explicit buffer copies count logical bytes accepted, including Shared memcpy;
/// runtime command factory timestamps count only successfully completed buffers.
pub fn snapshot(comptime enabled: bool) Snapshot {
    var result: Snapshot = @splat(0);
    if (enabled) termite_metal_telemetry_snapshot(&result);
    return result;
}

pub fn render(writer: *std.Io.Writer, values: Snapshot) !void {
    inline for (names, 0..) |name, index| {
        const metric = "antfly_inference_metal_" ++ name ++ "_total";
        try writer.print("# HELP {s} Worker-process Metal {s}; explicit buffer copies are logical accepted bytes/calls, GPU times cover completed runtime-factory command buffers\n# TYPE {s} counter\n{s} {d}\n", .{ metric, name, metric, metric, values[index] });
    }
}

test "disabled Metal telemetry is zero without platform linkage" {
    try std.testing.expectEqual(@as(Snapshot, @splat(0)), snapshot(false));
}
test "Metal telemetry renders unsigned counters without losing integer precision" {
    var writer: std.Io.Writer.Allocating = .init(std.testing.allocator);
    defer writer.deinit();
    var values: Snapshot = @splat(0);
    values[0] = std.math.maxInt(u64);
    values[8] = 17;
    try render(&writer.writer, values);
    try std.testing.expect(std.mem.indexOf(u8, writer.writer.buffered(), "antfly_inference_metal_buffer_upload_bytes_total 18446744073709551615\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, writer.writer.buffered(), "antfly_inference_metal_completed_gpu_nanoseconds_total 17\n") != null);
}
