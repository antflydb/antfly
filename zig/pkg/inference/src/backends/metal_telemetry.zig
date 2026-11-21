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
/// These synchronized gauges are not physical residency.
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

/// Uniform execution diagnostics keep these measurements explicitly scoped to
/// all tracked buffers/contexts in this worker, never this request alone.
pub fn runtimeProcessSnapshot() @import("../execution/runtime_telemetry.zig").Process {
    const values = snapshot(true);
    const memory = memorySnapshot(true);
    return .{
        .counters = .{
            .upload_bytes = values[0],
            .upload_calls = values[1],
            .download_bytes = values[2],
            .download_calls = values[3],
            .copy_bytes = values[4],
            .copy_calls = values[5],
            .completed_commands = values[6],
            .failed_commands = values[7],
            .physical_allocations = values[10],
            .physical_frees = values[11],
            .allocated_bytes = values[12],
            .freed_bytes = values[13],
            .initialized_bytes = values[18],
            .initialization_calls = values[19],
        },
        .owned_live_bytes = memory[0],
        .owned_lifetime_peak_bytes = memory[1],
        .private_live_bytes = memory[2],
        .private_lifetime_peak_bytes = memory[3],
        .host_visible_live_bytes = memory[4],
        .host_visible_lifetime_peak_bytes = memory[5],
        .alias_live_bytes = memory[6],
        .alias_lifetime_peak_bytes = memory[7],
    };
}

const purpose_count = @import("../execution/storage_accounting.zig").owned_purpose_count;
extern fn termite_metal_buffer_purpose_count() usize;
extern fn termite_metal_buffer_interval_begin(storage: *anyopaque, capacity: usize) usize;
extern fn termite_metal_buffer_interval_snapshot(storage: *anyopaque, values: [*]u64) void;
extern fn termite_metal_buffer_interval_finish(storage: *anyopaque, values: [*]u64) void;
pub const BufferIntervalReport = struct {
    scope: enum { worker_process_tracked_mtlbuffer_interval_including_other_contexts } = .worker_process_tracked_mtlbuffer_interval_including_other_contexts,
    backing: enum { owned_allocatedSize_and_separate_logical_alias_length_not_residency } = .owned_allocatedSize_and_separate_logical_alias_length_not_residency,
    root_identity: usize,
    semantic_lifetime_classes: enum { exact_tagged_birth_owners_with_unclassified_remainder_storageMode_separate } = .exact_tagged_birth_owners_with_unclassified_remainder_storageMode_separate,
    /// OwnedPurpose order. Live purpose bytes partition actual owned bytes;
    /// independently measured purpose peaks are never added to get total peak.
    purpose_live_bytes: [purpose_count]u64 = @splat(0),
    purpose_interval_peak_bytes: [purpose_count]u64 = @splat(0),
    /// Order: all owned, private owned, host-visible owned, logical alias.
    /// Owned peak is measured directly, never the sum of class peaks. Aliases
    /// are excluded from owned bytes; private/visible are subsets of owned.
    live_bytes: [4]u64,
    interval_peak_bytes: [4]u64,
    owned_allocations: u64,
    owned_frees: u64,
    aliases_created: u64,
    aliases_released: u64,
};
pub const BufferInterval = struct {
    storage: [128]u8 align(16) = undefined,
    root_identity: usize = 0,
    /// Like Registry.begin, call only at the final address until finish.
    pub fn begin(self: *BufferInterval, comptime enabled: bool) void {
        self.root_identity = if (enabled and termite_metal_buffer_purpose_count() == purpose_count) termite_metal_buffer_interval_begin(&self.storage, self.storage.len) else 0;
    }
    pub fn snapshot(self: *BufferInterval, comptime enabled: bool) ?BufferIntervalReport {
        if (!enabled or self.root_identity == 0) return null;
        var values: [12 + 2 * purpose_count]u64 = undefined;
        termite_metal_buffer_interval_snapshot(&self.storage, &values);
        return report(self.root_identity, values);
    }
    fn report(identity: usize, values: [12 + 2 * purpose_count]u64) BufferIntervalReport {
        return .{ .root_identity = identity, .purpose_live_bytes = values[12..][0..purpose_count].*, .purpose_interval_peak_bytes = values[12 + purpose_count ..][0..purpose_count].*, .live_bytes = values[0..4].*, .interval_peak_bytes = values[4..8].*, .owned_allocations = values[8], .owned_frees = values[9], .aliases_created = values[10], .aliases_released = values[11] };
    }
    pub fn finish(self: *BufferInterval, comptime enabled: bool) ?BufferIntervalReport {
        if (!enabled or self.root_identity == 0) return null;
        var values: [12 + 2 * purpose_count]u64 = undefined;
        termite_metal_buffer_interval_finish(&self.storage, &values);
        const identity = self.root_identity;
        self.root_identity = 0;
        return report(identity, values);
    }
};
test "disabled Metal buffer intervals neither link SDK nor allocate" {
    var interval: BufferInterval = .{};
    interval.begin(false);
    try std.testing.expect(interval.finish(false) == null);
}

test "Metal purpose interval ABI agrees with typed classes and preserves fixed caller storage" {
    const build_options = @import("build_options");
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    try std.testing.expectEqual(purpose_count, termite_metal_buffer_purpose_count());
    try std.testing.expectEqual(@as(usize, 128), @sizeOf(@FieldType(BufferInterval, "storage")));
}
