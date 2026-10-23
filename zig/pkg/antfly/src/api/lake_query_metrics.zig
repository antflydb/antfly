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

//! Process-level timings, not overlapping-request cache counter deltas.
//! A phase includes failed/canceled work and may overlap other phases (index
//! acquisition during highlighting). Names never include tables or paths.
const std = @import("std");
const local = @import("antfly_local_sources");
const prom = @import("../common/health_server.zig");
pub const Phase = enum {
    source,
    publication,
    index,
    ranking,
    // Nested text phases; opening occurs only on immutable corpus cache misses.
    text_directory,
    text_inventory,
    text_opening,
    text_term_plan,
    text_navigation,
    text_postings,
    text_cursor,
    cursor_retention,
    result_delivery,
    hydration,
    highlight,
    total,
    // Composed timings enclose leaf timings; native leaves include durable
    // snapshot capture/recovery, which the lake executor does not measure.
    composed_total,
    composed_definition,
    composed_leaf,
    composed_native_leaf,
    composed_cursor,
};
const count = @typeInfo(Phase).@"enum".field_names.len;
pub const Navigation = struct {
    queries: u64 = 0,
    admitted_segments: u64 = 0,
    admission_windows: u64 = 0,
    summary_cache_hits: u64 = 0,
    subtree_reductions: u64 = 0,
    pruned_ranges: u64 = 0,
    overflow_subtrees: u64 = 0,
    metadata_read_bytes: u64 = 0,
};
/// Owned by one composed request, including its serial leaf callbacks. Global
/// deltas cannot attribute one slow request inside a concurrent/warm burst.
pub const ComposedProfile = struct {
    nanoseconds: [count]u64 = @splat(0),
    pub fn record(self: *ComposedProfile, metrics: *Metrics, phase: Phase, start: u64) void {
        self.add(metrics, phase, @import("antfly_platform").time.monotonicNs() -| start);
    }
    fn add(self: *ComposedProfile, metrics: *Metrics, phase: Phase, elapsed: u64) void {
        self.nanoseconds[@backingInt(phase)] +|= elapsed;
        metrics.recordElapsed(phase, elapsed);
    }
    pub fn finish(self: *ComposedProfile, metrics: *Metrics, start: u64) void {
        self.record(metrics, .composed_total, start);
        metrics.retainSlow(self.*);
    }
    fn duration(self: ComposedProfile, phase: Phase) u64 {
        return self.nanoseconds[@backingInt(phase)];
    }
    fn composition(self: ComposedProfile) u64 {
        return self.duration(.composed_total) -| self.duration(.composed_definition) -| self.duration(.composed_leaf) -| self.duration(.composed_cursor);
    }
};
pub const Stats = struct {
    navigation: Navigation = .{},
    best_first_calls: u64 = 0,
    best_first_segments: u64 = 0,
    best_first_pruned_nodes: u64 = 0,
    calls: [count]u64 = @splat(0),
    nanoseconds: [count]u64 = @splat(0),
    slowest_composed: ?ComposedProfile = null,
    slow_profile_drops: u64 = 0,
};
pub const Metrics = struct {
    navigation: [@typeInfo(Navigation).@"struct".field_names.len]std.atomic.Value(u64) = @splat(.init(0)),
    best_first_calls: std.atomic.Value(u64) = .init(0),
    best_first_segments: std.atomic.Value(u64) = .init(0),
    best_first_pruned_nodes: std.atomic.Value(u64) = .init(0),
    calls: [count]std.atomic.Value(u64) = @splat(.init(0)),
    nanoseconds: [count]std.atomic.Value(u64) = @splat(.init(0)),
    slow_mutex: std.atomic.Mutex = .unlocked,
    slowest_composed: ?ComposedProfile = null,
    slow_profile_drops: std.atomic.Value(u64) = .init(0),
    pub fn recordBestFirst(self: *Metrics, segments: u64, pruned: u64) void {
        _ = self.best_first_calls.fetchAdd(1, .monotonic);
        _ = self.best_first_segments.fetchAdd(segments, .monotonic);
        _ = self.best_first_pruned_nodes.fetchAdd(pruned, .monotonic);
    }
    pub fn recordNavigation(self: *Metrics, value: Navigation) void {
        inline for (@typeInfo(Navigation).@"struct".field_names, 0..) |field, i| _ = self.navigation[i].fetchAdd(@field(value, field), .monotonic);
    }
    pub fn record(self: *Metrics, phase: Phase, start: u64) void {
        self.recordElapsed(phase, @import("antfly_platform").time.monotonicNs() -| start);
    }
    fn recordElapsed(self: *Metrics, phase: Phase, elapsed: u64) void {
        const index = @backingInt(phase);
        _ = self.calls[index].fetchAdd(1, .monotonic);
        _ = self.nanoseconds[index].fetchAdd(elapsed, .monotonic);
    }
    fn retainSlow(self: *Metrics, profile: ComposedProfile) void {
        if (profile.duration(.composed_total) <= 750 * std.time.ns_per_ms) return;
        // Optional diagnostics must never queue queries or write logs in the
        // request path. Copy one fixed-size profile with no allocation or I/O.
        if (!self.slow_mutex.tryLock()) {
            _ = self.slow_profile_drops.fetchAdd(1, .monotonic);
            return;
        }
        defer self.slow_mutex.unlock();
        if (self.slowest_composed == null or profile.duration(.composed_total) > self.slowest_composed.?.duration(.composed_total)) self.slowest_composed = profile;
    }
    pub fn snapshot(self: *Metrics) Stats {
        var result: Stats = .{ .best_first_calls = self.best_first_calls.load(.monotonic), .best_first_segments = self.best_first_segments.load(.monotonic), .best_first_pruned_nodes = self.best_first_pruned_nodes.load(.monotonic) };
        inline for (@typeInfo(Navigation).@"struct".field_names, 0..) |field, index| @field(result.navigation, field) = self.navigation[index].load(.monotonic);
        for (0..count) |i| {
            result.calls[i] = self.calls[i].load(.monotonic);
            result.nanoseconds[i] = self.nanoseconds[i].load(.monotonic);
        }
        result.slow_profile_drops = self.slow_profile_drops.load(.monotonic);
        if (self.slow_mutex.tryLock()) {
            defer self.slow_mutex.unlock();
            result.slowest_composed = self.slowest_composed;
        }
        return result;
    }
};

pub fn append(writer: *std.Io.Writer, memory: anytype, disk: anytype, query: anytype) !void {
    try prom.appendPromMetric(writer, "antfly_lake_cache_disk_ready", "gauge", "Whether persistent lake cache initialization succeeded", @intFromBool(disk != null));
    if (memory.disk_unavailable) |reason| try prom.appendPromMetricLabeled(writer, "antfly_lake_cache_disk_unavailable", "gauge", "Persistent cache initialization failure; reads continue through RAM and source", &.{.{ .name = "reason", .value = reason }}, 1);
    inline for (.{ "hits", "misses", "disk_hits", "mapping_hits", "disk_bytes", "provider_reads", "provider_bytes", "disk_init_attempts", "disk_init_failures" }) |field| {
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
    inline for (@typeInfo(Navigation).@"struct".field_names) |field| {
        try prom.appendPromMetric(writer, "antfly_lake_text_frontier_" ++ field ++ "_total", "counter", "Lake native text navigation work including failed and canceled queries", @field(query.navigation, field));
    }
    inline for (@typeInfo(Phase).@"enum".field_names, 0..) |phase, i| {
        try prom.appendPromMetric(writer, "antfly_lake_query_" ++ phase ++ "_calls_total", "counter", "Lake query phase invocations including failed work", query.calls[i]);
        try prom.appendPromMetric(writer, "antfly_lake_query_" ++ phase ++ "_nanoseconds_total", "counter", "Lake query phase elapsed nanoseconds including failed work", query.nanoseconds[i]);
    }
    try prom.appendPromMetric(writer, "antfly_lake_query_slow_profile_drops_total", "counter", "Slow composed profiles skipped due to diagnostic contention", query.slow_profile_drops);
    if (query.slowest_composed) |profile| {
        inline for (.{ "total", "definition", "leaf", "native_leaf", "cursor" }) |phase| {
            try prom.appendPromMetric(writer, "antfly_lake_query_slowest_composed_" ++ phase ++ "_nanoseconds", "gauge", "Request-local phase of the slowest composed request above 750 ms since startup; nested phases overlap", profile.duration(@field(Phase, "composed_" ++ phase)));
        }
        try prom.appendPromMetric(writer, "antfly_lake_query_slowest_composed_composition_nanoseconds", "gauge", "Slowest composed request remainder after definition, leaves and cursor; native overlaps leaves", profile.composition());
    }
}

test "external lake slow composed profiles isolate interleaved requests and never wait" {
    var metrics: Metrics = .{};
    var first: ComposedProfile = .{};
    var second: ComposedProfile = .{};
    first.add(&metrics, .composed_leaf, 200 * std.time.ns_per_ms);
    second.add(&metrics, .composed_leaf, 900 * std.time.ns_per_ms);
    first.add(&metrics, .composed_native_leaf, 100 * std.time.ns_per_ms);
    second.add(&metrics, .composed_native_leaf, 800 * std.time.ns_per_ms);
    first.add(&metrics, .composed_total, 300 * std.time.ns_per_ms);
    second.add(&metrics, .composed_total, 1000 * std.time.ns_per_ms);
    metrics.retainSlow(first);
    try std.testing.expect(metrics.snapshot().slowest_composed == null);
    metrics.retainSlow(second);
    const retained = metrics.snapshot().slowest_composed.?;
    try std.testing.expectEqual(@as(u64, 800 * std.time.ns_per_ms), retained.duration(.composed_native_leaf));
    try std.testing.expectEqual(@as(u64, 100 * std.time.ns_per_ms), retained.composition());
    try std.testing.expectEqual(@as(u64, 1300 * std.time.ns_per_ms), metrics.snapshot().nanoseconds[@backingInt(Phase.composed_total)]);
    try std.testing.expect(metrics.slow_mutex.tryLock());
    var slower = second;
    slower.nanoseconds[@backingInt(Phase.composed_total)] = 2000 * std.time.ns_per_ms;
    metrics.retainSlow(slower);
    try std.testing.expect(metrics.snapshot().slowest_composed == null);
    metrics.slow_mutex.unlock();
    try std.testing.expectEqual(@as(u64, 1), metrics.snapshot().slow_profile_drops);
    try std.testing.expectEqual(@as(u64, 1000 * std.time.ns_per_ms), metrics.snapshot().slowest_composed.?.duration(.composed_total));
    metrics.retainSlow(slower);
    metrics.retainSlow(second);
    try std.testing.expectEqual(@as(u64, 2000 * std.time.ns_per_ms), metrics.snapshot().slowest_composed.?.duration(.composed_total));
    var output: std.Io.Writer.Allocating = .init(std.testing.allocator);
    defer output.deinit();
    const writer = &output.writer;
    try append(writer, local.serverless_query_lake_serving_cache.Cache.Stats{}, @as(?local.serverless_query_lake_parquet_rowgroup.PersistentObjectRangeCacheStats, null), metrics.snapshot());
    const text = output.written();
    try std.testing.expect(std.mem.indexOf(u8, text, "antfly_lake_query_slowest_composed_total_nanoseconds 2000000000") != null);
    try std.testing.expect(std.mem.indexOf(u8, text, "antfly_lake_query_slowest_composed_native_leaf_nanoseconds 800000000") != null);
    try std.testing.expect(std.mem.indexOf(u8, text, "antfly_lake_query_slowest_composed_composition_nanoseconds 1100000000") != null);
}

test "external lake metrics expose fallback and disk write reasons without paths" {
    var output: std.Io.Writer.Allocating = .init(std.testing.allocator);
    defer output.deinit();
    const writer = &output.writer;
    var metrics: Metrics = .{};
    metrics.recordNavigation(.{ .queries = 1, .admitted_segments = 2, .pruned_ranges = 7 });
    metrics.record(.hydration, @import("antfly_platform").time.monotonicNs());
    inline for (.{ Phase.composed_total, .composed_definition, .composed_leaf, .composed_native_leaf, .composed_cursor }) |phase|
        metrics.record(phase, @import("antfly_platform").time.monotonicNs());
    try append(writer, local.serverless_query_lake_serving_cache.Cache.Stats{ .disk_unavailable = "ConcurrencyUnavailable", .disk_hits = 7 }, @as(?local.serverless_query_lake_parquet_rowgroup.PersistentObjectRangeCacheStats, .{ .write_errors = 2, .last_write_error = "NoSpaceLeft", .drops_queue = 3 }), metrics.snapshot());
    const text = output.written();
    inline for (.{ "text_directory", "text_inventory", "text_opening", "text_term_plan", "text_navigation", "text_postings", "text_cursor", "cursor_retention", "result_delivery" }) |phase| {
        try std.testing.expect(std.mem.indexOf(u8, text, "antfly_lake_query_" ++ phase ++ "_calls_total 0") != null);
    }
    try std.testing.expect(std.mem.indexOf(u8, text, "antfly_lake_cache_disk_init_attempts_total 0") != null);
    try std.testing.expect(std.mem.indexOf(u8, text, "antfly_lake_cache_disk_init_failures_total 0") != null);
    try std.testing.expect(std.mem.indexOf(u8, text, "reason=\"ConcurrencyUnavailable\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, text, "reason=\"NoSpaceLeft\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, text, "antfly_lake_disk_cache_drops_queue_total 3") != null);
    try std.testing.expect(std.mem.indexOf(u8, text, "antfly_lake_query_hydration_calls_total 1") != null);
    try std.testing.expect(std.mem.indexOf(u8, text, "antfly_lake_text_frontier_admitted_segments_total 2") != null);
    try std.testing.expect(std.mem.indexOf(u8, text, "antfly_lake_text_frontier_pruned_ranges_total 7") != null);
    inline for (.{ "total", "definition", "leaf", "native_leaf", "cursor" }) |phase| {
        try std.testing.expect(std.mem.indexOf(u8, text, "antfly_lake_query_composed_" ++ phase ++ "_calls_total 1") != null);
        try std.testing.expect(std.mem.indexOf(u8, text, "antfly_lake_query_composed_" ++ phase ++ "_nanoseconds_total ") != null);
    }
}
