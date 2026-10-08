// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Process-level timings, not overlapping-request cache counter deltas.
//! A phase includes failed/canceled work and may overlap other phases (index
//! acquisition during highlighting). Names never include tables or paths.
const std = @import("std");
const local = @import("antfly_local_sources");
const prom = @import("../common/health_server.zig");
pub const Phase = enum { source, publication, index, ranking, hydration, highlight, total };
const count = @typeInfo(Phase).@"enum".field_names.len;
pub const Stats = struct {
    calls: [count]u64 = @splat(0),
    nanoseconds: [count]u64 = @splat(0),
};
pub const Metrics = struct {
    calls: [count]std.atomic.Value(u64) = @splat(.init(0)),
    nanoseconds: [count]std.atomic.Value(u64) = @splat(.init(0)),
    pub fn record(self: *Metrics, phase: Phase, start: u64) void {
        const index = @backingInt(phase);
        _ = self.calls[index].fetchAdd(1, .monotonic);
        _ = self.nanoseconds[index].fetchAdd(@import("antfly_platform").time.monotonicNs() -| start, .monotonic);
    }
    pub fn snapshot(self: *const Metrics) Stats {
        var result: Stats = .{};
        for (0..count) |i| {
            result.calls[i] = self.calls[i].load(.monotonic);
            result.nanoseconds[i] = self.nanoseconds[i].load(.monotonic);
        }
        return result;
    }
};

pub fn append(writer: *std.Io.Writer, memory: anytype, disk: anytype, query: anytype) !void {
    try prom.appendPromMetric(writer, "antfly_lake_cache_disk_ready", "gauge", "Whether persistent lake cache initialization succeeded", @intFromBool(disk != null));
    if (memory.disk_unavailable) |reason| try prom.appendPromMetricLabeled(writer, "antfly_lake_cache_disk_unavailable", "gauge", "Persistent cache initialization failure; reads continue through RAM and source", &.{.{ .name = "reason", .value = reason }}, 1);
    inline for (.{ "hits", "misses", "disk_hits", "mapping_hits", "disk_bytes", "provider_reads", "provider_bytes" }) |field| {
        try prom.appendPromMetric(writer, "antfly_lake_cache_" ++ field ++ "_total", "counter", "Lake serving cache " ++ field, @field(memory, field));
    }
    if (disk) |stats| {
        inline for (.{ "writes_completed", "write_errors", "writes_coalesced", "writes_dropped", "dropped_bytes", "read_errors", "corrupt_entries_removed", "drops_policy", "drops_queue", "drops_memory", "drops_capacity", "drops_allocation", "drops_closing" }) |field| {
            try prom.appendPromMetric(writer, "antfly_lake_disk_cache_" ++ field ++ "_total", "counter", "Persistent lake cache " ++ field, @field(stats, field));
        }
        inline for (.{ "stored_bytes", "entries", "queued_bytes", "queued_entries" }) |field| {
            try prom.appendPromMetric(writer, "antfly_lake_disk_cache_" ++ field, "gauge", "Persistent lake cache " ++ field, @field(stats, field));
        }
        if (stats.last_write_error) |reason| try prom.appendPromMetricLabeled(writer, "antfly_lake_disk_cache_last_write_error", "gauge", "Most recent asynchronous lake cache write failure", &.{.{ .name = "reason", .value = reason }}, 1);
    }
    inline for (@typeInfo(Phase).@"enum".field_names, 0..) |phase, i| {
        try prom.appendPromMetric(writer, "antfly_lake_query_" ++ phase ++ "_calls_total", "counter", "Lake query phase invocations including failed work", query.calls[i]);
        try prom.appendPromMetric(writer, "antfly_lake_query_" ++ phase ++ "_nanoseconds_total", "counter", "Lake query phase elapsed nanoseconds including failed work", query.nanoseconds[i]);
    }
}

test "external lake metrics expose fallback and disk write reasons without paths" {
    var buffer: [16384]u8 = undefined;
    var writer: std.Io.Writer = .fixed(&buffer);
    var metrics: Metrics = .{};
    metrics.record(.hydration, @import("antfly_platform").time.monotonicNs());
    try append(&writer, local.serverless_query_lake_serving_cache.Cache.Stats{ .disk_unavailable = "ConcurrencyUnavailable", .disk_hits = 7 }, @as(?local.serverless_query_lake_parquet_rowgroup.PersistentObjectRangeCacheStats, .{ .write_errors = 2, .last_write_error = "NoSpaceLeft", .drops_queue = 3 }), metrics.snapshot());
    const text = writer.buffered();
    try std.testing.expect(std.mem.indexOf(u8, text, "reason=\"ConcurrencyUnavailable\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, text, "reason=\"NoSpaceLeft\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, text, "antfly_lake_disk_cache_drops_queue_total 3") != null);
    try std.testing.expect(std.mem.indexOf(u8, text, "antfly_lake_query_hydration_calls_total 1") != null);
}
