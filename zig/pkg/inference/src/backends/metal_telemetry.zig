// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

const std = @import("std");

pub const names = [_][]const u8{
    "buffer_upload_bytes",              "buffer_upload_calls",         "buffer_download_bytes",       "buffer_download_calls",
    "buffer_copy_bytes",                "buffer_copy_calls",           "commands_completed",          "commands_failed",
    "completed_gpu_nanoseconds",        "gpu_timestamps_missing",      "buffer_allocations",          "buffer_releases",
    "buffer_owned_bytes_allocated",     "buffer_owned_bytes_released", "buffer_aliases_created",      "buffer_aliases_released",
    "buffer_alias_bytes_registered",    "buffer_alias_bytes_released", "buffer_initialization_bytes", "buffer_initialization_calls",
    "vision_full_f32_flash_dispatches",
};
pub const Snapshot = [names.len]u64;
extern fn termite_metal_telemetry_snapshot(values: [*]u64) void;
pub const memory_names = [_][]const u8{
    "owned_buffer_live_bytes",        "owned_buffer_peak_bytes",
    "private_buffer_live_bytes",      "private_buffer_peak_bytes",
    "host_visible_buffer_live_bytes", "host_visible_buffer_peak_bytes",
    "alias_buffer_live_bytes",        "alias_buffer_peak_bytes",
    "owned_buffer_live_count",        "owned_buffer_peak_count",
    "alias_buffer_live_count",        "alias_buffer_peak_count",
};
pub const MemorySnapshot = [memory_names.len]u64;
extern fn termite_metal_buffer_memory_snapshot(values: [*]u64) void;

/// Tracked runtime MTLBuffer lifetimes, including temporary and command-retained
/// buffers. Owned bytes use allocatedSize; aliases use logical mapped length.
/// These gauges are neither physical residency nor a transactional snapshot.
pub fn memorySnapshot(comptime enabled: bool) MemorySnapshot {
    var result: MemorySnapshot = @splat(0);
    if (enabled) termite_metal_buffer_memory_snapshot(&result);
    return result;
}

pub fn renderMemory(writer: *std.Io.Writer, values: MemorySnapshot) !void {
    inline for (memory_names, 0..) |name, index| {
        const metric = "antfly_inference_metal_" ++ name;
        try writer.print("# HELP {s} Worker-process tracked runtime MTLBuffer {s}; owned allocatedSize and logical aliases are separate, not physical residency\n# TYPE {s} gauge\n{s} {d}\n", .{ metric, name, metric, metric, values[index] });
    }
}

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
    try std.testing.expectEqual(@as(MemorySnapshot, @splat(0)), memorySnapshot(false));
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

test "Metal buffer memory renders unsigned live and peak gauges without counter suffixes" {
    var writer: std.Io.Writer.Allocating = .init(std.testing.allocator);
    defer writer.deinit();
    var values: MemorySnapshot = @splat(0);
    values[1] = std.math.maxInt(u64);
    try renderMemory(&writer.writer, values);
    const text = writer.writer.buffered();
    try std.testing.expect(std.mem.indexOf(u8, text, "# TYPE antfly_inference_metal_owned_buffer_live_bytes gauge\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, text, "antfly_inference_metal_owned_buffer_peak_bytes 18446744073709551615\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, text, "_total") == null);
}
