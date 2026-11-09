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
    // Nested delivery batches distinguish physical reads from covered native fields.
    hydration_source,
    hydration_native,
    highlight,
    total,
    // Composed timings enclose leaf timings; native leaves include durable
    // snapshot capture/recovery, which the lake executor does not measure.
    composed_total,
    composed_definition,
    composed_leaf,
    composed_native_leaf,
    composed_cursor,
    admission,
    owner_admission,
    wal_cut,
    public_total,
    public_binding,
    public_dispatch,
};
const count = @typeInfo(Phase).@"enum".field_names.len;
pub const Unavailable = enum { reader_lease_expired, text_cache_busy, runtime_cache_busy, snapshot_lease_expired, snapshot_retired, overlay_coverage_unavailable };
const unavailable_count = @typeInfo(Unavailable).@"enum".field_names.len;
pub const PublicStage = enum(u8) { binding, dispatch, cleanup };
const public_stages = @typeInfo(PublicStage).@"enum".field_names.len;
const ActivePublic = struct {
    id: std.atomic.Value(u64) = .init(0),
    ready: std.atomic.Value(u64) = .init(0),
    start: std.atomic.Value(u64) = .init(0),
    stage: std.atomic.Value(PublicStage) = .init(.binding),
};
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
    io: local.serverless_query_lake_read_context.IoTrace.Snapshot = .{},
    leaves: [16]Leaf = @splat(.{}),
    leaf_count: usize = 0,
    leaf_overflow: u64 = 0,
    pub const Leaf = struct {
        source: u64 = 0, // Request-local hash only; never exported.
        source_slot: usize = 0,
        repeated: bool = false,
        kind: enum { unknown, native, object, read_only_lake, writable_lake } = .unknown,
        total_ns: u64 = 0,
        binding_ns: u64 = 0,
        dispatch_ns: u64 = 0,
        preparation_ns: u64 = 0,
        preparations: u64 = 0,
        ranking_ns: u64 = 0,
        hydration_ns: u64 = 0,
        io: local.serverless_query_lake_read_context.IoTrace.Snapshot = .{},
    };
    pub fn beginLeaf(self: *ComposedProfile, name: []const u8) ?*Leaf {
        if (self.leaf_count == self.leaves.len) {
            self.leaf_overflow +|= 1;
            return null;
        }
        const hash = std.hash.Wyhash.hash(0, name);
        var value: Leaf = .{ .source = hash, .source_slot = self.leaf_count };
        for (self.leaves[0..self.leaf_count]) |prior| if (prior.source == hash) {
            value.source_slot = prior.source_slot;
            value.repeated = true;
            break;
        };
        const result = &self.leaves[self.leaf_count];
        result.* = value;
        self.leaf_count += 1;
        return result;
    }
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
    pub fn finishPublic(self: *ComposedProfile, metrics: *Metrics, start: u64) void {
        self.record(metrics, .public_total, start);
        metrics.retainSlowPublic(self.*);
    }
    fn duration(self: ComposedProfile, phase: Phase) u64 {
        return self.nanoseconds[@backingInt(phase)];
    }
    fn composition(self: ComposedProfile) u64 {
        return self.duration(.composed_total) -| self.duration(.composed_definition) -| self.duration(.composed_leaf) -| self.duration(.composed_cursor);
    }
};
pub const Stats = struct {
    // Snapshot at the API owner. Health exposition may run in a separately
    // compiled runtime whose object-store module has independent atomics.
    object_operations: @import("objectstore").operation_metrics.Snapshot = .{ .calls = @splat(0), .errors = @splat(0), .nanoseconds = @splat(0), .max_nanoseconds = @splat(0), .retries = 0 },
    navigation: Navigation = .{},
    cursor_publications: u64 = 0,
    best_first_calls: u64 = 0,
    best_first_segments: u64 = 0,
    best_first_pruned_nodes: u64 = 0,
    calls: [count]u64 = @splat(0),
    nanoseconds: [count]u64 = @splat(0),
    slowest_composed: ?ComposedProfile = null,
    slowest_lake: ?ComposedProfile = null,
    slowest_public: ?ComposedProfile = null,
    slow_profile_drops: u64 = 0,
    unavailable: [unavailable_count]u64 = @splat(0),
    public_active: [public_stages]u64 = @splat(0),
    public_oldest_ns: [public_stages]u64 = @splat(0),
    public_tracking_drops: u64 = 0,
};
pub const Metrics = struct {
    cursor_publications: std.atomic.Value(u64) = .init(0),
    navigation: [@typeInfo(Navigation).@"struct".field_names.len]std.atomic.Value(u64) = @splat(.init(0)),
    best_first_calls: std.atomic.Value(u64) = .init(0),
    best_first_segments: std.atomic.Value(u64) = .init(0),
    best_first_pruned_nodes: std.atomic.Value(u64) = .init(0),
    calls: [count]std.atomic.Value(u64) = @splat(.init(0)),
    nanoseconds: [count]std.atomic.Value(u64) = @splat(.init(0)),
    slow_mutex: std.atomic.Mutex = .unlocked,
    slowest_composed: ?ComposedProfile = null,
    slowest_lake: ?ComposedProfile = null,
    slowest_public: ?ComposedProfile = null,
    slow_profile_drops: std.atomic.Value(u64) = .init(0),
    unavailable: [unavailable_count]std.atomic.Value(u64) = @splat(.init(0)),
    active_public: [64]ActivePublic = @splat(.{}),
    next_public_id: std.atomic.Value(u64) = .init(1),
    public_tracking_drops: std.atomic.Value(u64) = .init(0),
    pub const PublicLease = struct {
        slot: ?*ActivePublic,
        id: u64 = 0,
        pub fn enter(self: PublicLease, stage: PublicStage) void {
            if (self.slot) |slot| slot.stage.store(stage, .release);
        }
        pub fn deinit(self: PublicLease) void {
            if (self.slot) |slot| _ = slot.id.cmpxchgStrong(self.id, 0, .release, .monotonic);
        }
    };
    /// Optional, fixed-capacity live diagnostics also cover requests which
    /// never finish a profile. No allocation, request labels, locks or I/O.
    pub fn beginPublic(self: *Metrics, start: u64) PublicLease {
        var id = self.next_public_id.fetchAdd(1, .monotonic);
        if (id == 0) id = self.next_public_id.fetchAdd(1, .monotonic);
        for (&self.active_public) |*slot| {
            if (slot.id.cmpxchgStrong(0, id, .acq_rel, .monotonic) != null) continue;
            slot.start.store(start, .monotonic);
            slot.stage.store(.binding, .monotonic);
            slot.ready.store(id, .release);
            return .{ .slot = slot, .id = id };
        }
        _ = self.public_tracking_drops.fetchAdd(1, .monotonic);
        return .{ .slot = null };
    }
    pub fn recordUnavailable(self: *Metrics, err: anyerror) void {
        const reason: Unavailable = switch (err) {
            error.LakeIndexReaderLeaseExpired => .reader_lease_expired,
            error.NativeLakeTextCacheBusy => .text_cache_busy,
            error.NativeLakeRuntimeCacheBusy => .runtime_cache_busy,
            error.LakeSnapshotReadLeaseExpired => .snapshot_lease_expired,
            error.LakeSnapshotRetired => .snapshot_retired,
            error.LakeOverlayCoverageUnavailable => .overlay_coverage_unavailable,
            else => return,
        };
        _ = self.unavailable[@backingInt(reason)].fetchAdd(1, .monotonic);
    }
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
    pub fn retainSlowLake(self: *Metrics, profile: ComposedProfile) void {
        if (profile.duration(.total) < 750 * std.time.ns_per_ms) return;
        if (!self.slow_mutex.tryLock()) {
            _ = self.slow_profile_drops.fetchAdd(1, .monotonic);
            return;
        }
        defer self.slow_mutex.unlock();
        if (self.slowest_lake == null or profile.duration(.total) > self.slowest_lake.?.duration(.total)) self.slowest_lake = profile;
    }
    fn retainSlowPublic(self: *Metrics, profile: ComposedProfile) void {
        if (profile.duration(.public_total) < 750 * std.time.ns_per_ms) return;
        if (!self.slow_mutex.tryLock()) {
            _ = self.slow_profile_drops.fetchAdd(1, .monotonic);
            return;
        }
        defer self.slow_mutex.unlock();
        if (self.slowest_public == null or profile.duration(.public_total) > self.slowest_public.?.duration(.public_total)) self.slowest_public = profile;
    }
    pub fn snapshot(self: *Metrics) Stats {
        var result: Stats = .{ .cursor_publications = self.cursor_publications.load(.monotonic), .object_operations = @import("objectstore").operation_metrics.runtime.snapshot(), .best_first_calls = self.best_first_calls.load(.monotonic), .best_first_segments = self.best_first_segments.load(.monotonic), .best_first_pruned_nodes = self.best_first_pruned_nodes.load(.monotonic) };
        inline for (@typeInfo(Navigation).@"struct".field_names, 0..) |field, index| @field(result.navigation, field) = self.navigation[index].load(.monotonic);
        for (0..count) |i| {
            result.calls[i] = self.calls[i].load(.monotonic);
            result.nanoseconds[i] = self.nanoseconds[i].load(.monotonic);
        }
        result.slow_profile_drops = self.slow_profile_drops.load(.monotonic);
        for (0..unavailable_count) |i| result.unavailable[i] = self.unavailable[i].load(.monotonic);
        const now = @import("antfly_platform").time.monotonicNs();
        for (&self.active_public) |*slot| {
            const id = slot.id.load(.acquire);
            if (id == 0 or slot.ready.load(.acquire) != id) continue;
            const start = slot.start.load(.monotonic);
            const stage = @backingInt(slot.stage.load(.acquire));
            // A released/reused slot is never attributed to its previous owner.
            if (slot.id.load(.acquire) != id) continue;
            result.public_active[stage] += 1;
            result.public_oldest_ns[stage] = @max(result.public_oldest_ns[stage], now -| start);
        }
        result.public_tracking_drops = self.public_tracking_drops.load(.monotonic);
        if (self.slow_mutex.tryLock()) {
            defer self.slow_mutex.unlock();
            result.slowest_composed = self.slowest_composed;
            result.slowest_lake = self.slowest_lake;
            result.slowest_public = self.slowest_public;
        }
        return result;
    }
};

pub fn append(writer: *std.Io.Writer, memory: anytype, disk: anytype, query: anytype) !void {
    try prom.appendPromMetric(writer, "antfly_lake_query_cursor_publications_total", "counter", "Query-time lake leaf or composed cursor publication requests, including memo hits; excludes native table captures and background serving-cut construction", query.cursor_publications);
    const operations = @import("objectstore").operation_metrics;
    const object_stats = query.object_operations;
    inline for (@typeInfo(operations.Operation).@"enum".field_names, 0..) |name, i| {
        try prom.appendPromMetric(writer, "antfly_objectstore_" ++ name ++ "_calls_total", "counter", "Linked API runtime logical client calls including local and nested fallback calls; excludes separately owned runtime counters", object_stats.calls[i]);
        try prom.appendPromMetric(writer, "antfly_objectstore_" ++ name ++ "_errors_total", "counter", "Logical client errors including absence and conditional conflicts", object_stats.errors[i]);
        try prom.appendPromMetric(writer, "antfly_objectstore_" ++ name ++ "_nanoseconds_total", "counter", "Logical client elapsed time; nested and concurrent calls overlap", object_stats.nanoseconds[i]);
        try prom.appendPromMetric(writer, "antfly_objectstore_" ++ name ++ "_input_bytes_total", "counter", "Recorded logical request body bytes, including failed writes and nested fallback calls; excludes metadata and transport overhead", object_stats.input_bytes[i]);
        try prom.appendPromMetric(writer, "antfly_objectstore_" ++ name ++ "_output_bytes_total", "counter", "Recorded logical returned body bytes; excludes metadata, transport overhead and separately owned runtimes", object_stats.output_bytes[i]);
        try prom.appendPromMetric(writer, "antfly_objectstore_" ++ name ++ "_max_nanoseconds", "gauge", "Maximum logical client elapsed time since process start", object_stats.max_nanoseconds[i]);
    }
    try prom.appendPromMetric(writer, "antfly_objectstore_get_retries_total", "counter", "Client retries after mutable metadata precondition races", object_stats.retries);
    try prom.appendPromMetric(writer, "antfly_lake_cache_disk_ready", "gauge", "Whether persistent lake cache initialization succeeded", @intFromBool(disk != null));
    if (memory.disk_unavailable) |reason| try prom.appendPromMetricLabeled(writer, "antfly_lake_cache_disk_unavailable", "gauge", "Persistent cache initialization failure; reads continue through RAM and source", &.{.{ .name = "reason", .value = reason }}, 1);
    inline for (.{ "hits", "misses", "disk_hits", "mapping_hits", "disk_bytes", "provider_reads", "provider_bytes", "disk_init_attempts", "disk_init_failures" }) |field| {
        try prom.appendPromMetric(writer, "antfly_lake_cache_" ++ field ++ "_total", "counter", "Lake serving cache " ++ field, @field(memory, field));
    }
    if (disk) |stats| {
        inline for (.{ "writes_completed", "write_errors", "writes_coalesced", "writes_dropped", "dropped_bytes", "read_errors", "corrupt_entries_removed", "inventory_checkpoint_hits", "inventory_scan_entries", "drops_policy", "drops_queue", "drops_memory", "drops_capacity", "drops_allocation", "drops_closing" }) |field| {
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
    inline for (@typeInfo(Unavailable).@"enum".field_names, 0..) |reason, i| {
        try prom.appendPromMetric(writer, "antfly_lake_query_unavailable_" ++ reason ++ "_total", "counter", "Internal cause mapped to a retryable lake query 503", query.unavailable[i]);
    }
    try prom.appendPromMetric(writer, "antfly_lake_query_public_tracking_drops_total", "counter", "Public queries exceeding optional live diagnostic capacity", query.public_tracking_drops);
    inline for (@typeInfo(PublicStage).@"enum".field_names, 0..) |stage, i| {
        try prom.appendPromMetric(writer, "antfly_lake_query_public_active_" ++ stage, "gauge", "Single-table public queries currently in this stage; bounded optional diagnostics", query.public_active[i]);
        try prom.appendPromMetric(writer, "antfly_lake_query_public_oldest_" ++ stage ++ "_nanoseconds", "gauge", "Admission age of oldest live single-table public query currently in this stage", query.public_oldest_ns[i]);
    }
    if (query.slowest_lake) |profile| {
        inline for (@typeInfo(local.serverless_query_lake_read_context.IoTrace.Snapshot).@"struct".field_names) |field| {
            try prom.appendPromMetric(writer, "antfly_lake_query_slowest_lake_io_" ++ field, "gauge", "Request-owned immutable I/O profile; parallel elapsed sums overlap", @field(profile.io, field));
        }

        inline for (.{ "total", "admission", "wal_cut", "source", "publication", "ranking", "cursor_retention", "result_delivery" }) |phase| {
            try prom.appendPromMetric(writer, "antfly_lake_query_slowest_lake_" ++ phase ++ "_nanoseconds", "gauge", "Request-local phase of slowest lake request above 750 ms since startup; unlisted setup is part of total", profile.duration(@field(Phase, phase)));
        }
    }
    if (query.slowest_public) |profile| {
        inline for (.{ "total", "binding", "dispatch" }) |phase| {
            try prom.appendPromMetric(writer, "antfly_lake_query_slowest_public_" ++ phase ++ "_nanoseconds", "gauge", "Request-local phase of slowest single-table public query above 750 ms; total includes cleanup and nested dispatch", profile.duration(@field(Phase, "public_" ++ phase)));
        }
    }
    if (query.slowest_composed) |profile| {
        inline for (@typeInfo(local.serverless_query_lake_read_context.IoTrace.Snapshot).@"struct".field_names) |field| {
            try prom.appendPromMetric(writer, "antfly_lake_query_slowest_composed_io_" ++ field, "gauge", "Request-owned covered lake immutable I/O across serial composed leaves; parallel elapsed sums overlap", @field(profile.io, field));
        }

        inline for (.{ "total", "definition", "leaf", "native_leaf", "cursor" }) |phase| {
            try prom.appendPromMetric(writer, "antfly_lake_query_slowest_composed_" ++ phase ++ "_nanoseconds", "gauge", "Request-local phase of the slowest composed request above 750 ms since startup; nested phases overlap", profile.duration(@field(Phase, "composed_" ++ phase)));
        }
        try prom.appendPromMetric(writer, "antfly_lake_query_slowest_composed_leaf_overflow", "gauge", "Leaf calls exceeding the bounded request-local diagnostic slots", profile.leaf_overflow);
        for (profile.leaves[0..profile.leaf_count], 0..) |leaf, i| {
            var name_buffer: [192]u8 = undefined;
            inline for (.{ "total_ns", "binding_ns", "dispatch_ns", "preparation_ns", "preparations", "ranking_ns", "hydration_ns", "source_slot", "repeated" }) |field| {
                const metric = try std.fmt.bufPrint(&name_buffer, "antfly_lake_query_slowest_composed_call_{d}_{s}", .{ i, field });
                const value: u64 = if (comptime std.mem.eql(u8, field, "repeated")) @intFromBool(leaf.repeated) else @intCast(@field(leaf, field));
                try prom.appendPromMetric(writer, metric, "gauge", "Bounded request-local leaf call; source slots contain no names", value);
            }
            const kind_metric = try std.fmt.bufPrint(&name_buffer, "antfly_lake_query_slowest_composed_call_{d}_kind_{s}", .{ i, @tagName(leaf.kind) });
            try prom.appendPromMetric(writer, kind_metric, "gauge", "Bounded leaf source kind", 1);
            inline for (@typeInfo(local.serverless_query_lake_read_context.IoTrace.Snapshot).@"struct".field_names) |field| {
                const metric = try std.fmt.bufPrint(&name_buffer, "antfly_lake_query_slowest_composed_call_{d}_io_{s}", .{ i, field });
                try prom.appendPromMetric(writer, metric, "gauge", "Leaf-owned lake I/O; parallel elapsed sums overlap", @field(leaf.io, field));
            }
        }
        try prom.appendPromMetric(writer, "antfly_lake_query_slowest_composed_composition_nanoseconds", "gauge", "Slowest composed request remainder after definition, leaves and cursor; native overlaps leaves", profile.composition());
    }
}

test "external lake unavailable reason counters preserve internal causes without request labels" {
    var metrics: Metrics = .{};
    metrics.recordUnavailable(error.LakeIndexReaderLeaseExpired);
    metrics.recordUnavailable(error.NativeLakeTextCacheBusy);
    metrics.recordUnavailable(error.NativeLakeRuntimeCacheBusy);
    metrics.recordUnavailable(error.LakeSnapshotReadLeaseExpired);
    metrics.recordUnavailable(error.LakeSnapshotRetired);
    metrics.recordUnavailable(error.LakeOverlayCoverageUnavailable);
    metrics.recordUnavailable(error.DeadlineExceeded);
    metrics.recordUnavailable(error.Canceled);
    const stats = metrics.snapshot();
    for (stats.unavailable) |value| try std.testing.expectEqual(@as(u64, 1), value);
    var output: std.Io.Writer.Allocating = .init(std.testing.allocator);
    defer output.deinit();
    try append(&output.writer, local.serverless_query_lake_serving_cache.Cache.Stats{}, @as(?local.serverless_query_lake_parquet_rowgroup.PersistentObjectRangeCacheStats, null), stats);
    try std.testing.expect(std.mem.indexOf(u8, output.written(), "antfly_lake_query_unavailable_overlay_coverage_unavailable_total 1") != null);
}

test "external lake public request profiles retain binding dispatch and cleanup independently" {
    var metrics: Metrics = .{};
    var queued: ComposedProfile = .{};
    var sibling: ComposedProfile = .{};
    queued.add(&metrics, .public_binding, 60 * std.time.ns_per_s);
    sibling.add(&metrics, .public_dispatch, 5 * std.time.ns_per_s);
    sibling.add(&metrics, .public_total, 5 * std.time.ns_per_s);
    metrics.retainSlowPublic(sibling);
    queued.add(&metrics, .public_total, 61 * std.time.ns_per_s);
    metrics.retainSlowPublic(queued);
    const profile = metrics.snapshot().slowest_public.?;
    try std.testing.expectEqual(@as(u64, 60 * std.time.ns_per_s), profile.duration(.public_binding));
    try std.testing.expectEqual(@as(u64, 0), profile.duration(.public_dispatch));
    try std.testing.expectEqual(@as(u64, 61 * std.time.ns_per_s), profile.duration(.public_total));
    try std.testing.expect(metrics.snapshot().slowest_lake == null);
}

test "external lake public request profiles expose unfinished stages with bounded admission" {
    var metrics: Metrics = .{};
    var held: [64]Metrics.PublicLease = undefined;
    for (&held) |*lease| lease.* = metrics.beginPublic(1);
    defer for (held) |lease| lease.deinit();
    const overflow = metrics.beginPublic(1);
    defer overflow.deinit();
    try std.testing.expect(overflow.slot == null);
    var snapshot = metrics.snapshot();
    try std.testing.expectEqual(@as(u64, 64), snapshot.public_active[@backingInt(PublicStage.binding)]);
    try std.testing.expectEqual(@as(u64, 1), snapshot.public_tracking_drops);
    held[0].enter(.dispatch);
    snapshot = metrics.snapshot();
    try std.testing.expectEqual(@as(u64, 63), snapshot.public_active[@backingInt(PublicStage.binding)]);
    try std.testing.expectEqual(@as(u64, 1), snapshot.public_active[@backingInt(PublicStage.dispatch)]);
    try std.testing.expect(snapshot.public_oldest_ns[@backingInt(PublicStage.dispatch)] > 0);
    held[0].enter(.cleanup);
    try std.testing.expectEqual(@as(u64, 1), metrics.snapshot().public_active[@backingInt(PublicStage.cleanup)]);
    const old = held[0];
    held[0].deinit();
    held[0] = metrics.beginPublic(@import("antfly_platform").time.monotonicNs());
    old.deinit(); // A stale diagnostic lease cannot clear its replacement.
    snapshot = metrics.snapshot();
    try std.testing.expectEqual(@as(u64, 64), snapshot.public_active[@backingInt(PublicStage.binding)]);
    try std.testing.expectEqual(@as(u64, 0), snapshot.public_active[@backingInt(PublicStage.cleanup)]);
}

test "external lake single request owner operation snapshot survives transport and exposition" {
    var snapshot: Stats = .{};
    snapshot.object_operations.calls[@backingInt(@import("objectstore").operation_metrics.Operation.get)] = 12345;
    snapshot.object_operations.retries = 17;
    snapshot.object_operations.input_bytes[@backingInt(@import("objectstore").operation_metrics.Operation.put)] = 7654;
    snapshot.object_operations.output_bytes[@backingInt(@import("objectstore").operation_metrics.Operation.get)] = 9876;
    const a = std.testing.allocator;
    const encoded = try std.json.Stringify.valueAlloc(a, snapshot, .{});
    defer a.free(encoded);
    var parsed = try std.json.parseFromSlice(Stats, a, encoded, .{});
    defer parsed.deinit();
    var output: std.Io.Writer.Allocating = .init(a);
    defer output.deinit();
    try append(&output.writer, local.serverless_query_lake_serving_cache.Cache.Stats{}, @as(?local.serverless_query_lake_parquet_rowgroup.PersistentObjectRangeCacheStats, null), parsed.value);
    try std.testing.expect(std.mem.indexOf(u8, output.written(), "antfly_objectstore_get_calls_total 12345\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, output.written(), "antfly_objectstore_get_retries_total 17\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, output.written(), "antfly_objectstore_put_input_bytes_total 7654\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, output.written(), "antfly_objectstore_get_output_bytes_total 9876\n") != null);
}

test "external lake single request outlier profiles isolate interleaved phases and never wait" {
    var metrics: Metrics = .{};
    var fast: ComposedProfile = .{};
    var slow: ComposedProfile = .{};
    fast.add(&metrics, .ranking, 100 * std.time.ns_per_ms);
    slow.add(&metrics, .publication, 3000 * std.time.ns_per_ms);
    slow.add(&metrics, .admission, 500 * std.time.ns_per_ms);
    fast.add(&metrics, .wal_cut, 10 * std.time.ns_per_ms);
    fast.add(&metrics, .total, 150 * std.time.ns_per_ms);
    slow.add(&metrics, .total, 4000 * std.time.ns_per_ms);
    metrics.retainSlowLake(fast);
    try std.testing.expect(metrics.snapshot().slowest_lake == null);
    metrics.retainSlowLake(slow);
    const profile = metrics.snapshot().slowest_lake.?;
    try std.testing.expectEqual(@as(u64, 3000 * std.time.ns_per_ms), profile.duration(.publication));
    try std.testing.expectEqual(@as(u64, 500 * std.time.ns_per_ms), profile.duration(.admission));
    try std.testing.expectEqual(@as(u64, 0), profile.duration(.wal_cut));
    try std.testing.expectEqual(@as(u64, 0), profile.duration(.ranking));
    try std.testing.expect(metrics.slow_mutex.tryLock());
    metrics.retainSlowLake(slow);
    metrics.slow_mutex.unlock();
    try std.testing.expectEqual(@as(u64, 1), metrics.snapshot().slow_profile_drops);
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
    second.io = .{ .provider_reads = 3, .provider_bytes = 128, .peak_inflight = 2 };
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
    try std.testing.expect(std.mem.indexOf(u8, text, "antfly_lake_query_slowest_composed_io_provider_reads 3") != null);
    try std.testing.expect(std.mem.indexOf(u8, text, "antfly_lake_query_slowest_composed_io_peak_inflight 2") != null);
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

test "external lake bounded window profiles retain each refill without cross-request attribution" {
    var profile: ComposedProfile = .{};
    var other: ComposedProfile = .{};
    const first = profile.beginLeaf("archive").?;
    first.total_ns = 123;
    _ = profile.beginLeaf("recent");
    const refill = profile.beginLeaf("archive").?;
    refill.dispatch_ns = 456;
    try std.testing.expect(refill.repeated);
    try std.testing.expectEqual(@as(usize, 0), refill.source_slot);
    try std.testing.expect(!other.beginLeaf("archive").?.repeated);
    while (profile.leaf_count < profile.leaves.len) _ = profile.beginLeaf("recent");
    try std.testing.expect(profile.beginLeaf("overflow") == null);
    try std.testing.expectEqual(@as(u64, 1), profile.leaf_overflow);
    try std.testing.expectEqual(@as(u64, 123), profile.leaves[0].total_ns);
    try std.testing.expectEqual(@as(u64, 456), profile.leaves[2].dispatch_ns);
    var metrics: Metrics = .{};
    profile.add(&metrics, .composed_total, std.time.ns_per_s);
    metrics.retainSlow(profile);
    var output: std.Io.Writer.Allocating = .init(std.testing.allocator);
    defer output.deinit();
    try append(&output.writer, local.serverless_query_lake_serving_cache.Cache.Stats{}, @as(?local.serverless_query_lake_parquet_rowgroup.PersistentObjectRangeCacheStats, null), metrics.snapshot());
    try std.testing.expect(std.mem.indexOf(u8, output.written(), "composed_call_2_repeated 1") != null);
    try std.testing.expect(std.mem.indexOf(u8, output.written(), "archive") == null);
}
