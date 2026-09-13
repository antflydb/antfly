// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

const std = @import("std");
const metadata = @import("table_manager.zig");
const observer = @import("store_observer.zig");

pub const Cursor = struct {
    reporter_incarnation: u64,
    sequence: u64,
    digest: [32]u8,
};

/// Bounded HTTP telemetry carries identity and counters, never durable index
/// payloads. A collected observation may be delivered in multiple batches.
pub const max_activity_samples = 512;
pub const ActivitySample = struct {
    group_id: u64,
    index_name: []const u8,
    index_kind: []const u8,
    coverage_generation: u64 = 0,
    coverage_config_hash: u64 = 0,
    activity: metadata.RuntimeEmbeddingActivityStatusReport,
};

/// The report contains complete replacements for changed groups, including all
/// duplicate observations in their original order. Absence is never deletion.
/// A null base establishes a full inventory. Cursors acknowledge applied state.
pub const Update = struct {
    version: u16 = 1,
    sequence: u64,
    base: ?Cursor = null,
    report: metadata.StoreStatusReport,
    removed_groups: []const u64 = &.{},
    /// HTTP-only owner telemetry; never replicated in the durable command.
    activity: []const ActivitySample = &.{},

    pub fn validate(self: Update, alloc: std.mem.Allocator) !void {
        if (self.version != 1 or self.sequence == 0 or self.report.reporter_incarnation == 0 or self.report.runtime_reference) return error.InvalidStoreReporterFence;
        if (self.report.store_id == 0) return error.InvalidNodeID;
        if (!metadata.reporterFenceValid(self.report.reporter_incarnation, self.report.status_generation) or
            !metadata.embeddingActivityReportValid(self.report.reporter_incarnation, self.report.embedding_activity_protocol_version, self.report.embedding_activity_sequence) or
            !metadata.embeddingActivitySamplesValid(self.report.embedding_activity_protocol_version, self.report.runtime_statuses) or
            !metadata.artifactSourcesProtocolValid(self.report.reporter_incarnation, self.report.artifact_sources_protocol_version) or
            !metadata.denseNativeStorageProtocolValid(self.report.reporter_incarnation, self.report.dense_native_storage_protocol_version)) return error.InvalidStoreReporterFence;
        if (self.activity.len > max_activity_samples) return error.InvalidStoreReporterFence;
        for (self.activity) |sample| {
            if (sample.group_id == 0 or sample.index_name.len > 1024 or sample.index_kind.len > 1024) return error.InvalidStoreReporterFence;
            var indexes = [_]metadata.RuntimeIndexStatusReport{.{ .embedding_activity_observed = true, .embedding_activity = sample.activity }};
            const runtime = [_]metadata.RuntimeGroupStatusReport{.{ .indexes = &indexes }};
            if (!metadata.embeddingActivitySamplesValid(self.report.embedding_activity_protocol_version, &runtime)) return error.InvalidStoreReporterFence;
        }
        if (self.base) |base| {
            if (base.reporter_incarnation != self.report.reporter_incarnation or base.sequence >= self.sequence) return error.StoreReportBaseMismatch;
        } else if (self.removed_groups.len != 0) return error.InvalidStoreReporterFence;
        var present: std.AutoHashMapUnmanaged(u64, void) = .empty;
        defer present.deinit(alloc);
        for (self.report.group_statuses) |item| try present.put(alloc, item.group_id, {});
        for (self.report.runtime_statuses) |item| try present.put(alloc, item.group_id, {});
        for (self.removed_groups) |id| {
            const entry = try present.getOrPut(alloc, id);
            if (entry.found_existing) return error.InvalidStoreReporterFence;
        }
    }
};

pub const Command = struct {
    update: Update,
    request_digest: [32]u8,
    // Admission observes a header and cursor; apply atomically compares both.
    expected_header: [32]u8,
    admission_cursor: ?Cursor = null,
};

const Positions = struct {
    groups: std.ArrayListUnmanaged(usize) = .empty,
    runtimes: std.ArrayListUnmanaged(usize) = .empty,
};
fn index(a: std.mem.Allocator, report: metadata.StoreStatusReport) !std.AutoHashMapUnmanaged(u64, Positions) {
    var out: std.AutoHashMapUnmanaged(u64, Positions) = .empty;
    for (report.group_statuses, 0..) |item, i| {
        const entry = try out.getOrPut(a, item.group_id);
        if (!entry.found_existing) entry.value_ptr.* = .{};
        try entry.value_ptr.groups.append(a, i);
    }
    for (report.runtime_statuses, 0..) |item, i| {
        const entry = try out.getOrPut(a, item.group_id);
        if (!entry.found_existing) entry.value_ptr.* = .{};
        try entry.value_ptr.runtimes.append(a, i);
    }
    return out;
}

/// Arena-owned patch; unchanged runtime leaves (including volatile telemetry)
/// never enter the HTTP or Raft command. Full reports remain the repair path.
fn diff(a: std.mem.Allocator, previous: *const Publisher, next: metadata.StoreStatusReport, base: Cursor, sequence: u64) !Update {
    var current = try index(a, next);
    var groups: std.ArrayListUnmanaged(metadata.GroupStatusReport) = .empty;
    var runtimes: std.ArrayListUnmanaged(metadata.RuntimeGroupStatusReport) = .empty;
    var removed: std.ArrayListUnmanaged(u64) = .empty;
    var entries = current.iterator();
    while (entries.next()) |entry| {
        const positions = entry.value_ptr.*;
        const same = blk: {
            const prior = previous.groups.get(entry.key_ptr.*) orelse break :blk false;
            if (prior.groups.len != positions.groups.items.len or prior.runtimes.len != positions.runtimes.items.len) break :blk false;
            for (prior.groups, positions.groups.items) |item, j| if (!observer.groupStatusEqual(item, next.group_statuses[j])) break :blk false;
            for (prior.runtimes, positions.runtimes.items) |item, j| if (!observer.runtimeStatusEqual(item, next.runtime_statuses[j], true)) break :blk false;
            break :blk true;
        };
        if (same) continue;
        for (positions.groups.items) |i| try groups.append(a, next.group_statuses[i]);
        for (positions.runtimes.items) |i| try runtimes.append(a, next.runtime_statuses[i]);
    }
    var ids = previous.groups.keyIterator();
    while (ids.next()) |id| if (!current.contains(id.*)) try removed.append(a, id.*);
    var report = next;
    report.group_statuses = groups.items;
    report.runtime_statuses = runtimes.items;
    return .{ .sequence = sequence, .base = base, .report = report, .removed_groups = removed.items };
}

pub fn asReport(record: metadata.StoreRecord) metadata.StoreStatusReport {
    var report: metadata.StoreStatusReport = .{ .store_id = record.store_id };
    inline for (std.meta.fields(metadata.StoreStatusReport)) |field| {
        if (comptime @hasField(metadata.StoreRecord, field.name)) @field(report, field.name) = @field(record, field.name);
    }
    return report;
}

/// Serialized by the reporter owner. Only acknowledged replacements become
/// the next diff baseline; unchanged clocks retain their last transmitted age.
pub const Publisher = struct {
    const Group = struct {
        arena: std.heap.ArenaAllocator,
        groups: []metadata.GroupStatusReport,
        runtimes: []metadata.RuntimeGroupStatusReport,
        fn destroy(self: *Group, alloc: std.mem.Allocator) void {
            self.arena.deinit();
            alloc.destroy(self);
        }
    };
    const Pending = struct { id: u64, group: *Group };
    pub const Prepared = struct {
        arena: std.heap.ArenaAllocator,
        update: Update,
        replacements: []Pending,
        full: bool,
        pub fn deinit(self: *Prepared, alloc: std.mem.Allocator) void {
            for (self.replacements) |item| item.group.destroy(alloc);
            self.arena.deinit();
        }
    };
    groups: std.AutoHashMapUnmanaged(u64, *Group) = .empty,
    cursor: ?Cursor = null,
    sequence: u64 = 0,

    pub fn deinit(self: *Publisher, alloc: std.mem.Allocator) void {
        var it = self.groups.valueIterator();
        while (it.next()) |group| group.*.destroy(alloc);
        self.groups.deinit(alloc);
        self.* = .{};
    }

    pub fn prepare(self: *Publisher, alloc: std.mem.Allocator, report: metadata.StoreStatusReport, force_full: bool, retain_runtime: bool) !Prepared {
        var arena = std.heap.ArenaAllocator.init(alloc);
        errdefer arena.deinit();
        const a = arena.allocator();
        var next = report;
        next.runtime_reference = false;
        if (retain_runtime) {
            if (self.cursor == null or self.cursor.?.reporter_incarnation != report.reporter_incarnation) return error.StoreReportBaseMismatch;
            var prior_runtime: std.ArrayListUnmanaged(metadata.RuntimeGroupStatusReport) = .empty;
            var it = self.groups.valueIterator();
            while (it.next()) |group| try prior_runtime.appendSlice(a, group.*.runtimes);
            next.runtime_statuses = prior_runtime.items;
        }
        const full = force_full or self.cursor == null or self.cursor.?.reporter_incarnation != report.reporter_incarnation;
        self.sequence = try std.math.add(u64, self.sequence, 1);
        var update = if (full) Update{ .sequence = self.sequence, .report = next } else try diff(a, self, next, self.cursor.?, self.sequence);
        var activity: std.ArrayListUnmanaged(ActivitySample) = .empty;
        if (!retain_runtime) for (report.runtime_statuses) |runtime| {
            for (runtime.indexes) |item| if (item.embedding_activity_observed) {
                try activity.append(a, .{ .group_id = runtime.group_id, .index_name = item.name, .index_kind = item.kind, .coverage_generation = item.coverage_generation, .coverage_config_hash = item.coverage_config_hash, .activity = item.embedding_activity });
            };
        };
        update.activity = activity.items;
        var grouped = try index(a, update.report);
        var replacements: std.ArrayListUnmanaged(Pending) = .empty;
        errdefer for (replacements.items) |item| item.group.destroy(alloc);
        var entries = grouped.iterator();
        while (entries.next()) |entry| {
            const owned = try alloc.create(Group);
            errdefer alloc.destroy(owned);
            var leaf = std.heap.ArenaAllocator.init(alloc);
            errdefer leaf.deinit();
            const la = leaf.allocator();
            const selected_groups = try a.alloc(metadata.GroupStatusReport, entry.value_ptr.groups.items.len);
            const selected_runtime = try a.alloc(metadata.RuntimeGroupStatusReport, entry.value_ptr.runtimes.items.len);
            for (selected_groups, entry.value_ptr.groups.items) |*item, i| item.* = update.report.group_statuses[i];
            for (selected_runtime, entry.value_ptr.runtimes.items) |*item, i| item.* = update.report.runtime_statuses[i];
            const groups = try metadata.cloneGroupStatuses(la, selected_groups);
            const runtimes = try metadata.cloneRuntimeGroupStatusReports(la, selected_runtime);
            owned.* = .{ .arena = leaf, .groups = groups, .runtimes = runtimes };
            try replacements.append(a, .{ .id = entry.key_ptr.*, .group = owned });
        }
        // Commit after acknowledgement cannot allocate or fail.
        try self.groups.ensureUnusedCapacity(alloc, @intCast(replacements.items.len));
        return .{ .arena = arena, .update = update, .replacements = replacements.items, .full = full };
    }

    pub fn commit(self: *Publisher, alloc: std.mem.Allocator, prepared: *Prepared, cursor: Cursor) void {
        if (prepared.full) {
            var it = self.groups.valueIterator();
            while (it.next()) |group| group.*.destroy(alloc);
            self.groups.clearRetainingCapacity();
        } else for (prepared.update.removed_groups) |id| {
            if (self.groups.fetchRemove(id)) |entry| entry.value.destroy(alloc);
        }
        for (prepared.replacements) |item| {
            if (self.groups.fetchRemove(item.id)) |entry| entry.value.destroy(alloc);
            self.groups.putAssumeCapacity(item.id, item.group);
        }
        prepared.replacements = &.{};
        self.cursor = cursor;
    }
};

fn testCursor(update: Update) Cursor {
    return .{ .reporter_incarnation = update.report.reporter_incarnation, .sequence = update.sequence, .digest = @splat(@intCast(update.sequence)) };
}

test "system catalog sparse reports validate capabilities telemetry and removal fences" {
    const alloc = std.testing.allocator;
    var update: Update = .{ .sequence = 1, .report = .{ .store_id = 20, .reporter_incarnation = 77 } };
    try update.validate(alloc);
    update.report.dense_native_storage_protocol_version = std.math.maxInt(u16);
    try std.testing.expectError(error.InvalidStoreReporterFence, update.validate(alloc));
    update.report.dense_native_storage_protocol_version = 0;
    update.report.artifact_sources_protocol_version = std.math.maxInt(u16);
    try std.testing.expectError(error.InvalidStoreReporterFence, update.validate(alloc));
    update.report.artifact_sources_protocol_version = 0;
    var indexes = [_]metadata.RuntimeIndexStatusReport{.{ .name = "dense", .kind = "embeddings", .embedding_activity_observed = true }};
    var activity = [_]ActivitySample{.{ .group_id = 101, .index_name = "dense", .index_kind = "embeddings", .activity = .{} }};
    update.activity = &activity;
    try std.testing.expectError(error.InvalidStoreReporterFence, update.validate(alloc));
    update.report.embedding_activity_protocol_version = metadata.embedding_activity_protocol_version;
    update.report.embedding_activity_sequence = 1;
    indexes[0].embedding_activity.epoch = 1;
    indexes[0].embedding_activity.sample_sequence = 1;
    activity[0].activity = indexes[0].embedding_activity;
    try update.validate(alloc);
    update.activity = &.{};
    update.removed_groups = &.{101};
    try std.testing.expectError(error.InvalidStoreReporterFence, update.validate(alloc));
    update.base = .{ .reporter_incarnation = 77, .sequence = 1, .digest = @splat(0) };
    update.sequence = 2;
    try update.validate(alloc);
    update.removed_groups = &.{ 101, 101 };
    try std.testing.expectError(error.InvalidStoreReporterFence, update.validate(alloc));
    update.removed_groups = &.{101};
    var runtimes = [_]metadata.RuntimeGroupStatusReport{.{ .group_id = 101, .indexes = &indexes }};
    update.report.runtime_statuses = &runtimes;
    try std.testing.expectError(error.InvalidStoreReporterFence, update.validate(alloc));
}

test "system catalog sparse publisher preserves duplicates removals and acknowledged clocks" {
    const alloc = std.testing.allocator;
    var publisher: Publisher = .{};
    defer publisher.deinit(alloc);
    var groups = [_]metadata.GroupStatusReport{ .{ .group_id = 101, .raft_term = 1, .updated_at_millis = 1 }, .{ .group_id = 101, .raft_term = 2, .updated_at_millis = 1 }, .{ .group_id = 102 } };
    var report: metadata.StoreStatusReport = .{ .store_id = 20, .reporter_incarnation = 77, .status_generation = 1, .group_statuses = &groups };
    {
        var first = try publisher.prepare(alloc, report, false, false);
        defer first.deinit(alloc);
        try std.testing.expect(first.full);
        publisher.commit(alloc, &first, testCursor(first.update));
    }
    const leaf = publisher.groups.get(101).?;
    groups[0].updated_at_millis = 10000;
    groups[1].updated_at_millis = 10000;
    {
        var coalesced = try publisher.prepare(alloc, report, false, false);
        defer coalesced.deinit(alloc);
        try std.testing.expectEqual(@as(usize, 0), coalesced.update.report.group_statuses.len);
        publisher.commit(alloc, &coalesced, testCursor(coalesced.update));
    }
    try std.testing.expectEqual(leaf, publisher.groups.get(101).?);
    try std.testing.expectEqual(@as(u64, 1), leaf.groups[0].updated_at_millis);
    groups[0].updated_at_millis = 31000;
    report.group_statuses = groups[0..2];
    {
        var changed = try publisher.prepare(alloc, report, false, false);
        defer changed.deinit(alloc);
        try changed.update.validate(alloc);
        try std.testing.expectEqual(@as(usize, 2), changed.update.report.group_statuses.len);
        try std.testing.expectEqual(@as(u64, 2), changed.update.report.group_statuses[1].raft_term);
        try std.testing.expectEqualSlices(u64, &.{102}, changed.update.removed_groups);
        // An unacknowledged request leaves every baseline object intact.
    }
    try std.testing.expectEqual(leaf, publisher.groups.get(101).?);
    try std.testing.expect(publisher.groups.contains(102));
    var retried = try publisher.prepare(alloc, report, false, false);
    defer retried.deinit(alloc);
    publisher.commit(alloc, &retried, testCursor(retried.update));
    try std.testing.expect(!publisher.groups.contains(102));
    try std.testing.expectEqual(@as(u64, 31000), publisher.groups.get(101).?.groups[0].updated_at_millis);
    report.reporter_incarnation = 88;
    var restarted = try publisher.prepare(alloc, report, false, false);
    defer restarted.deinit(alloc);
    try std.testing.expect(restarted.full and restarted.update.base == null);
}

test "system catalog sparse publisher allocation failures preserve acknowledged ownership" {
    const Case = struct {
        fn run(alloc: std.mem.Allocator) !void {
            var publisher: Publisher = .{};
            defer publisher.deinit(alloc);
            var groups = [_]metadata.GroupStatusReport{.{ .group_id = 101, .raft_term = 1 }};
            const report: metadata.StoreStatusReport = .{ .store_id = 20, .reporter_incarnation = 77, .group_statuses = &groups };
            var full = try publisher.prepare(alloc, report, false, false);
            defer full.deinit(alloc);
            publisher.commit(alloc, &full, testCursor(full.update));
            const leaf = publisher.groups.get(101).?;
            groups[0].raft_term = 2;
            var patch = publisher.prepare(alloc, report, false, false) catch |err| {
                try std.testing.expectEqual(@as(u64, 1), leaf.groups[0].raft_term);
                return err;
            };
            defer patch.deinit(alloc);
            publisher.commit(alloc, &patch, testCursor(patch.update));
            try std.testing.expectEqual(@as(u64, 2), publisher.groups.get(101).?.groups[0].raft_term);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Case.run, .{});
}

test "store report workload benchmark publisher sparse encoding" {
    if (std.c.getenv("ANTFLY_CATALOG_REPORT_BENCH") == null) return;
    const alloc = std.heap.c_allocator;
    for ([_]usize{ 100, 1000, 10000 }) |count| {
        const groups = try alloc.alloc(metadata.GroupStatusReport, count);
        defer alloc.free(groups);
        const runtimes = try alloc.alloc(metadata.RuntimeGroupStatusReport, count);
        defer alloc.free(runtimes);
        for (groups, runtimes, 0..) |*group, *runtime, i| {
            group.* = .{ .group_id = i + 100, .raft_term = 1 };
            runtime.* = .{ .group_id = i + 100, .table_name = "tenant_events", .table_id = i + 1, .store_id = 20, .node_id = 30 };
        }
        const report: metadata.StoreStatusReport = .{ .store_id = 20, .reporter_incarnation = 77, .status_generation = 1, .group_statuses = groups, .runtime_statuses = runtimes };
        var publisher: Publisher = .{};
        defer publisher.deinit(alloc);
        var initial = try publisher.prepare(alloc, report, false, false);
        defer initial.deinit(alloc);
        publisher.commit(alloc, &initial, testCursor(initial.update));
        for ([_]bool{ false, true }) |sparse| {
            var samples: [9]u64 = undefined;
            var body_size: usize = 0;
            for (&samples) |*sample| {
                groups[0].raft_term += 1;
                const start = @import("antfly_platform").time.monotonicNs();
                if (sparse) {
                    var prepared = try publisher.prepare(alloc, report, false, false);
                    defer prepared.deinit(alloc);
                    const body = try std.json.Stringify.valueAlloc(prepared.arena.allocator(), prepared.update, .{});
                    body_size = body.len;
                    publisher.commit(alloc, &prepared, testCursor(prepared.update));
                } else {
                    const body = try std.json.Stringify.valueAlloc(alloc, report, .{});
                    defer alloc.free(body);
                    body_size = body.len;
                }
                sample.* = @import("antfly_platform").time.monotonicNs() - start;
            }
            std.mem.sort(u64, &samples, {}, std.sort.asc(u64));
            std.debug.print("PUBLISHER_BENCH groups={d} sparse={} p50_ms={d:.3} http_bytes={d}\n", .{ count, sparse, @as(f64, @floatFromInt(samples[4])) / 1e6, body_size });
        }
    }
}

test "store report workload benchmark compact activity batches" {
    if (std.c.getenv("ANTFLY_CATALOG_REPORT_BENCH") == null) return;
    const alloc = std.heap.c_allocator;
    for ([_]usize{ 1000, 10000 }) |count| {
        const runtimes = try alloc.alloc(metadata.RuntimeGroupStatusReport, count);
        defer alloc.free(runtimes);
        var indexes = [_]metadata.RuntimeIndexStatusReport{.{ .name = "dense", .kind = "embeddings", .embedding_activity_observed = true, .embedding_activity = .{ .epoch = 1, .sample_sequence = 1 } }};
        for (runtimes, 0..) |*runtime, i| runtime.* = .{ .group_id = i + 1, .table_name = "tenant_events", .indexes = &indexes };
        const report: metadata.StoreStatusReport = .{ .store_id = 20, .reporter_incarnation = 77, .runtime_statuses = runtimes, .embedding_activity_protocol_version = metadata.embedding_activity_protocol_version, .embedding_activity_sequence = 1 };
        var publisher: Publisher = .{};
        defer publisher.deinit(alloc);
        var initial = try publisher.prepare(alloc, report, false, false);
        defer initial.deinit(alloc);
        publisher.commit(alloc, &initial, testCursor(initial.update));
        for ([_]bool{ false, true }) |compact| {
            var elapsed: [9]u64 = undefined;
            var total_bytes: usize = 0;
            var max_bytes: usize = 0;
            var requests: usize = 0;
            for (&elapsed) |*sample| {
                total_bytes = 0;
                max_bytes = 0;
                requests = 0;
                const start = @import("antfly_platform").time.monotonicNs();
                if (compact) {
                    var prepared = try publisher.prepare(alloc, report, false, false);
                    defer prepared.deinit(alloc);
                    const activity = prepared.update.activity;
                    var offset: usize = 0;
                    while (offset < activity.len) {
                        const end = @min(activity.len, offset + max_activity_samples);
                        prepared.update.activity = activity[offset..end];
                        try prepared.update.validate(alloc);
                        const body = try std.json.Stringify.valueAlloc(alloc, prepared.update, .{});
                        defer alloc.free(body);
                        total_bytes += body.len;
                        max_bytes = @max(max_bytes, body.len);
                        requests += 1;
                        offset = end;
                    }
                } else {
                    const body = try std.json.Stringify.valueAlloc(alloc, .{ .activity = runtimes }, .{});
                    defer alloc.free(body);
                    total_bytes = body.len;
                    max_bytes = body.len;
                    requests = 1;
                }
                sample.* = @import("antfly_platform").time.monotonicNs() - start;
            }
            std.mem.sort(u64, &elapsed, {}, std.sort.asc(u64));
            std.debug.print("ACTIVITY_BENCH groups={d} compact={} p50_ms={d:.3} total_http_bytes={d} max_request_bytes={d} requests={d}\n", .{ count, compact, @as(f64, @floatFromInt(elapsed[4])) / 1e6, total_bytes, max_bytes, requests });
        }
    }
}
