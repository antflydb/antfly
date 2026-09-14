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
    telemetry_only: bool = false,
    sequence: u64,
    base: ?Cursor = null,
    report: metadata.StoreStatusReport,
    removed_groups: []const u64 = &.{},
    /// HTTP-only owner telemetry; never replicated in the durable command.
    activity: []const ActivitySample = &.{},

    pub fn validate(self: Update, alloc: std.mem.Allocator) !void {
        if (self.telemetry_only and (self.base == null or self.report.group_statuses.len != 0 or self.report.runtime_statuses.len != 0 or self.removed_groups.len != 0)) return error.InvalidStoreReporterFence;
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
fn diff(a: std.mem.Allocator, previous: *const Publisher, next: metadata.StoreStatusReport, base: Cursor, sequence: u64, retain_runtime: bool) !Update {
    var current = try index(a, next);
    var groups: std.ArrayListUnmanaged(metadata.GroupStatusReport) = .empty;
    var runtimes: std.ArrayListUnmanaged(metadata.RuntimeGroupStatusReport) = .empty;
    var removed: std.ArrayListUnmanaged(u64) = .empty;
    var entries = current.iterator();
    while (entries.next()) |entry| {
        const positions = entry.value_ptr.*;
        const same = blk: {
            const prior = previous.groups.get(entry.key_ptr.*) orelse break :blk false;
            if (prior.groups.len != positions.groups.items.len or (!retain_runtime and prior.runtimes.len != positions.runtimes.items.len)) break :blk false;
            for (prior.groups, positions.groups.items) |item, j| if (!observer.groupStatusEqual(item, next.group_statuses[j])) break :blk false;
            if (!retain_runtime) for (prior.runtimes, positions.runtimes.items) |item, j| {
                if (!observer.runtimeStatusEqual(item, next.runtime_statuses[j], true)) break :blk false;
            };
            break :blk true;
        };
        if (same) continue;
        for (positions.groups.items) |i| try groups.append(a, next.group_statuses[i]);
        if (retain_runtime) {
            const prior = previous.groups.get(entry.key_ptr.*) orelse return error.StoreReportBaseMismatch;
            try runtimes.appendSlice(a, prior.runtimes);
        } else for (positions.runtimes.items) |i| try runtimes.append(a, next.runtime_statuses[i]);
    }
    var ids = previous.groups.keyIterator();
    while (ids.next()) |id| if (!current.contains(id.*)) {
        if (retain_runtime) {
            if (previous.groups.get(id.*).?.groups.len != 0) return error.StoreReportBaseMismatch;
        } else try removed.append(a, id.*);
    };
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
    // The publisher serializes mutations; independently owned snapshots may
    // release references on the control owner. A prepared
    // heartbeat can share the acknowledged immutable index inventory safely
    // across commit, abandonment, and replacement of the structural group row.
    const Runtime = struct {
        arena: std.heap.ArenaAllocator,
        refs: std.atomic.Value(usize) = .init(1),
        items: []metadata.RuntimeGroupStatusReport,
        fn release(self: *Runtime, alloc: std.mem.Allocator) void {
            if (self.refs.fetchSub(1, .acq_rel) == 1) {
                self.arena.deinit();
                alloc.destroy(self);
            }
        }
    };
    pub const RuntimeSnapshot = struct {
        items: []metadata.RuntimeGroupStatusReport,
        leases: []*Runtime,
        pub fn deinit(self: *RuntimeSnapshot, alloc: std.mem.Allocator) void {
            for (self.leases) |runtime| runtime.release(alloc);
            alloc.free(self.leases);
            alloc.free(self.items);
            self.* = undefined;
        }
    };

    /// Preserve caller order (including duplicate runtime rows) while sharing
    /// the acknowledged immutable index arrays. No index inventory is walked.
    pub fn retainRuntimeSnapshot(self: *const Publisher, alloc: std.mem.Allocator, report: metadata.StoreStatusReport) !RuntimeSnapshot {
        const cursor = self.cursor orelse return error.StoreReportBaseMismatch;
        if (cursor.reporter_incarnation != report.reporter_incarnation) return error.StoreReportBaseMismatch;
        const items = try alloc.alloc(metadata.RuntimeGroupStatusReport, report.runtime_statuses.len);
        errdefer alloc.free(items);
        const leases = try alloc.alloc(*Runtime, items.len);
        errdefer alloc.free(leases);
        var initialized: usize = 0;
        errdefer for (leases[0..initialized]) |runtime| runtime.release(alloc);
        var positions: std.AutoHashMapUnmanaged(u64, usize) = .empty;
        defer positions.deinit(alloc);
        for (report.runtime_statuses, items, leases) |record, *item, *lease| {
            const group = self.groups.get(record.group_id) orelse return error.StoreReportBaseMismatch;
            const position = try positions.getOrPut(alloc, record.group_id);
            if (!position.found_existing) position.value_ptr.* = 0;
            if (position.value_ptr.* >= group.runtimes.len) return error.StoreReportBaseMismatch;
            item.* = group.runtimes[position.value_ptr.*];
            position.value_ptr.* += 1;
            _ = group.runtime.refs.fetchAdd(1, .monotonic);
            lease.* = group.runtime;
            initialized += 1;
        }
        return .{ .items = items, .leases = leases };
    }

    const Group = struct {
        arena: std.heap.ArenaAllocator,
        groups: []metadata.GroupStatusReport,
        runtimes: []metadata.RuntimeGroupStatusReport,
        runtime: *Runtime,
        fn destroy(self: *Group, alloc: std.mem.Allocator) void {
            self.runtime.release(alloc);
            self.arena.deinit();
            alloc.destroy(self);
        }
    };
    pub const Pending = struct { id: u64, group: *Group };
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
            // Retained records are already the acknowledged immutable base.
            // A heartbeat supplies structural groups only, never replacement indexes.
            if (report.runtime_statuses.len != 0 or force_full) return error.StoreReportBaseMismatch;
        }
        const full = force_full or self.cursor == null or self.cursor.?.reporter_incarnation != report.reporter_incarnation;
        self.sequence = try std.math.add(u64, self.sequence, 1);
        var update = if (full) Update{ .sequence = self.sequence, .report = next } else try diff(a, self, next, self.cursor.?, self.sequence, retain_runtime);
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
            const runtime = if (retain_runtime) blk: {
                const prior = self.groups.get(entry.key_ptr.*) orelse return error.StoreReportBaseMismatch;
                _ = prior.runtime.refs.fetchAdd(1, .monotonic);
                break :blk prior.runtime;
            } else blk: {
                const value = try alloc.create(Runtime);
                errdefer alloc.destroy(value);
                var runtime_arena = std.heap.ArenaAllocator.init(alloc);
                errdefer runtime_arena.deinit();
                const items = try metadata.cloneRuntimeGroupStatusReports(runtime_arena.allocator(), selected_runtime);
                value.* = .{ .arena = runtime_arena, .items = items };
                break :blk value;
            };
            errdefer runtime.release(alloc);
            owned.* = .{ .arena = leaf, .groups = groups, .runtimes = runtime.items, .runtime = runtime };
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

/// Bounded owned outbox generation. Only the pending generation is coalesced;
/// a worker finishes its in-flight generation so busy producers cannot starve
/// the tail of a large inventory. Payload ownership is independent of reports.
pub const ActivityCollection = struct {
    pub const max_samples = 16384;
    pub const max_payload_bytes = 4 * 1024 * 1024;
    arena: std.heap.ArenaAllocator,
    update: Update,
    selected: std.ArrayListUnmanaged(ActivitySample) = .empty,
    payload_bytes: usize = 0,

    pub fn create(alloc: std.mem.Allocator, report: metadata.StoreStatusReport, samples: []const ActivitySample, cursor: Cursor, rotation: *usize) !*@This() {
        const sequence = try std.math.add(u64, cursor.sequence, 1);
        const self = try alloc.create(@This());
        self.* = .{ .arena = std.heap.ArenaAllocator.init(alloc), .update = .{
            .telemetry_only = true,
            .sequence = sequence,
            .base = cursor,
            .report = .{
                .store_id = report.store_id,
                .reporter_incarnation = report.reporter_incarnation,
                .status_generation = report.status_generation,
                .embedding_activity_protocol_version = report.embedding_activity_protocol_version,
                .embedding_activity_sequence = report.embedding_activity_sequence,
            },
        } };
        errdefer self.destroy(alloc);
        if (samples.len != 0) {
            const window_start = rotation.* % samples.len;
            for (0..@min(samples.len, max_samples)) |i| {
                if (!try self.append(samples[(window_start + i) % samples.len])) break;
            }
            rotation.* = (window_start + self.selected.items.len) % samples.len;
        }
        return self;
    }
    pub fn destroy(self: *@This(), alloc: std.mem.Allocator) void {
        self.arena.deinit();
        alloc.destroy(self);
    }
    fn append(self: *@This(), item: ActivitySample) !bool {
        const size = @sizeOf(ActivitySample) + item.index_name.len + item.index_kind.len;
        if (self.selected.items.len == max_samples or self.payload_bytes + size > max_payload_bytes) return false;
        const a = self.arena.allocator();
        var owned = item;
        owned.index_name = try a.dupe(u8, item.index_name);
        owned.index_kind = try a.dupe(u8, item.index_kind);
        try self.selected.append(a, owned);
        self.payload_bytes += size;
        self.update.activity = self.selected.items;
        return true;
    }
    pub fn mergeOlder(self: *@This(), old: *const @This()) !void {
        if (old.update.report.reporter_incarnation != self.update.report.reporter_incarnation or
            old.update.report.status_generation != self.update.report.status_generation) return;
        const a = self.arena.allocator();
        var seen: std.AutoHashMapUnmanaged([32]u8, void) = .empty;
        for (self.selected.items) |item| try seen.put(a, identity(item), {});
        for (old.update.activity) |item| {
            if (seen.contains(identity(item))) continue;
            if (!try self.append(item)) break;
        }
    }
    fn identity(sample: ActivitySample) [32]u8 {
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        var integers: [40]u8 = undefined;
        std.mem.writeInt(u64, integers[0..8], sample.group_id, .little);
        std.mem.writeInt(u64, integers[8..16], sample.coverage_generation, .little);
        std.mem.writeInt(u64, integers[16..24], sample.coverage_config_hash, .little);
        std.mem.writeInt(u64, integers[24..32], sample.index_name.len, .little);
        std.mem.writeInt(u64, integers[32..40], sample.index_kind.len, .little);
        hash.update(&integers);
        hash.update(sample.index_name);
        hash.update(sample.index_kind);
        return hash.finalResult();
    }
};

test "system catalog telemetry outbox coalesces quiet indexes and owns payloads across failures" {
    const Case = struct {
        fn run(a: std.mem.Allocator) !void {
            var rotation: usize = 0;
            var report: metadata.StoreStatusReport = .{ .store_id = 1, .reporter_incarnation = 7, .status_generation = 1, .embedding_activity_protocol_version = 2, .embedding_activity_sequence = 1 };
            const cursor: Cursor = .{ .reporter_incarnation = 7, .sequence = 1, .digest = @splat(0) };
            var samples = [_]ActivitySample{
                .{ .group_id = 10, .index_name = "dense", .index_kind = "embeddings", .activity = .{ .epoch = 1, .sample_sequence = 1 } },
                .{ .group_id = 20, .index_name = "quiet", .index_kind = "embeddings", .activity = .{ .epoch = 1, .sample_sequence = 1 } },
            };
            const old = try ActivityCollection.create(a, report, &samples, cursor, &rotation);
            defer old.destroy(a);
            report.embedding_activity_sequence = 2;
            samples[0].activity.sample_sequence = 2;
            const next = try ActivityCollection.create(a, report, samples[0..1], cursor, &rotation);
            defer next.destroy(a);
            try next.mergeOlder(old);
            try std.testing.expectEqual(@as(usize, 2), next.update.activity.len);
            try std.testing.expectEqual(@as(u64, 2), next.update.activity[0].activity.sample_sequence);
            try std.testing.expectEqualStrings("quiet", next.update.activity[1].index_name);
            try std.testing.expect(next.update.activity[1].index_name.ptr != old.update.activity[1].index_name.ptr);
            try next.update.validate(a);
            report.status_generation = 2;
            const replacement = try ActivityCollection.create(a, report, samples[0..1], cursor, &rotation);
            defer replacement.destroy(a);
            try replacement.mergeOlder(old);
            try std.testing.expectEqual(@as(usize, 1), replacement.update.activity.len);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Case.run, .{});
}

test "system catalog telemetry outbox rotates beyond its bounded window" {
    const a = std.testing.allocator;
    const samples = try a.alloc(ActivitySample, ActivityCollection.max_samples + 1);
    defer a.free(samples);
    for (samples, 0..) |*sample, i| sample.* = .{ .group_id = i + 1, .index_name = "dense", .index_kind = "embeddings", .activity = .{} };
    var rotation: usize = 0;
    const cursor: Cursor = .{ .reporter_incarnation = 7, .sequence = 1, .digest = @splat(0) };
    const first = try ActivityCollection.create(a, .{ .store_id = 1 }, samples, cursor, &rotation);
    defer first.destroy(a);
    try std.testing.expect(first.update.activity.len <= ActivityCollection.max_samples);
    try std.testing.expect(first.payload_bytes <= ActivityCollection.max_payload_bytes);
    const second = try ActivityCollection.create(a, .{ .store_id = 1 }, samples, cursor, &rotation);
    defer second.destroy(a);
    try std.testing.expectEqual(first.update.activity.len + 1, second.update.activity[0].group_id);
}

test "system catalog retained heartbeat shares immutable inventory through abandonment commit and allocation failure" {
    const Case = struct {
        fn run(a: std.mem.Allocator) !void {
            var publisher: Publisher = .{};
            defer publisher.deinit(a);
            var groups = [_]metadata.GroupStatusReport{.{ .group_id = 101, .raft_term = 1 }};
            var indexes = [_]metadata.RuntimeIndexStatusReport{.{ .name = "search", .kind = "full_text" }};
            var runtimes = [_]metadata.RuntimeGroupStatusReport{
                .{ .group_id = 101, .indexes = &indexes },
                .{ .group_id = 102, .indexes = &indexes },
            };
            var report: metadata.StoreStatusReport = .{ .store_id = 20, .reporter_incarnation = 77, .group_statuses = &groups, .runtime_statuses = &runtimes };
            var full = try publisher.prepare(a, report, false, false);
            defer full.deinit(a);
            publisher.commit(a, &full, testCursor(full.update));
            const runtime = publisher.groups.get(101).?.runtime;
            report.runtime_statuses = &.{};
            {
                var unchanged = try publisher.prepare(a, report, false, true);
                defer unchanged.deinit(a);
                try std.testing.expectEqual(@as(usize, 0), unchanged.replacements.len);
                try std.testing.expectEqual(@as(usize, 0), unchanged.update.removed_groups.len);
            }
            groups[0].raft_term = 2;
            {
                var abandoned = try publisher.prepare(a, report, false, true);
                defer abandoned.deinit(a);
                try std.testing.expectEqual(runtime, abandoned.replacements[0].group.runtime);
                try std.testing.expectEqual(@as(usize, 2), runtime.refs.load(.acquire));
            }
            try std.testing.expectEqual(@as(usize, 1), runtime.refs.load(.acquire));
            var changed = try publisher.prepare(a, report, false, true);
            defer changed.deinit(a);
            try std.testing.expectEqualDeep(runtimes[0], changed.update.report.runtime_statuses[0]);
            publisher.commit(a, &changed, testCursor(changed.update));
            try std.testing.expectEqual(runtime, publisher.groups.get(101).?.runtime);
            try std.testing.expectEqual(@as(usize, 1), runtime.refs.load(.acquire));
            try std.testing.expect(publisher.groups.contains(102));
            report.group_statuses = &.{};
            try std.testing.expectError(error.StoreReportBaseMismatch, publisher.prepare(a, report, false, true));
        }
    };
    // Error-path expectations allocate too; exercise ownership separately from
    // the intentional base-mismatch validation below.
    try Case.run(std.testing.allocator);
    const Failures = struct {
        fn run(a: std.mem.Allocator) !void {
            var publisher: Publisher = .{};
            defer publisher.deinit(a);
            var groups = [_]metadata.GroupStatusReport{.{ .group_id = 101 }};
            var indexes = [_]metadata.RuntimeIndexStatusReport{.{ .name = "search", .kind = "full_text" }};
            var runtimes = [_]metadata.RuntimeGroupStatusReport{.{ .group_id = 101, .indexes = &indexes }};
            var report: metadata.StoreStatusReport = .{ .store_id = 20, .reporter_incarnation = 77, .group_statuses = &groups, .runtime_statuses = &runtimes };
            var full = try publisher.prepare(a, report, false, false);
            defer full.deinit(a);
            publisher.commit(a, &full, testCursor(full.update));
            groups[0].raft_term = 2;
            report.runtime_statuses = &.{};
            var heartbeat = try publisher.prepare(a, report, false, true);
            defer heartbeat.deinit(a);
            publisher.commit(a, &heartbeat, testCursor(heartbeat.update));
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Failures.run, .{});
}

test "store report workload benchmark retained heartbeat" {
    if (std.c.getenv("ANTFLY_CATALOG_REPORT_BENCH") == null) return;
    const a = std.heap.c_allocator;
    for ([_]usize{ 1000, 10000 }) |count| {
        const groups = try a.alloc(metadata.GroupStatusReport, count);
        defer a.free(groups);
        const runtimes = try a.alloc(metadata.RuntimeGroupStatusReport, count);
        defer a.free(runtimes);
        var indexes = [_]metadata.RuntimeIndexStatusReport{.{ .name = "tenant_search", .kind = "full_text" }} ** 32;
        for (groups, runtimes, 0..) |*group, *runtime, i| {
            group.* = .{ .group_id = i + 100, .raft_term = 1 };
            runtime.* = .{ .group_id = i + 100, .table_id = i + 1, .indexes = &indexes };
        }
        var report: metadata.StoreStatusReport = .{ .store_id = 20, .reporter_incarnation = 77, .group_statuses = groups, .runtime_statuses = runtimes };
        var publisher: Publisher = .{};
        defer publisher.deinit(a);
        var initial = try publisher.prepare(a, report, false, false);
        defer initial.deinit(a);
        publisher.commit(a, &initial, testCursor(initial.update));
        for ([_]bool{ false, true }) |changed| {
            groups[0].raft_term = if (changed) 2 else 1;
            for ([_]bool{ false, true }) |retain| {
                report.runtime_statuses = if (retain) &.{} else runtimes;
                var samples: [9]u64 = undefined;
                for (0..10) |sample| {
                    const start = @import("antfly_platform").time.monotonicNs();
                    var prepared = try publisher.prepare(a, report, false, retain);
                    defer prepared.deinit(a);
                    const end = @import("antfly_platform").time.monotonicNs();
                    try std.testing.expectEqual(@as(usize, if (changed) 1 else 0), prepared.replacements.len);
                    if (sample != 0) samples[sample - 1] = end - start;
                }
                std.mem.sort(u64, &samples, {}, std.sort.asc(u64));
                std.debug.print("RETAINED_HEARTBEAT_BENCH groups={d} indexes_per_group=32 changed={} retained={} prepare_p50_ns={d} samples_ns={any}\n", .{ count, changed, retain, samples[4], samples });
            }
        }
    }
}

test "system catalog runtime snapshot retains duplicate ordered leaves across replacement and publisher teardown" {
    const Case = struct {
        fn run(a: std.mem.Allocator) !void {
            var publisher: Publisher = .{};
            defer publisher.deinit(a);
            var indexes = [_]metadata.RuntimeIndexStatusReport{.{ .name = "search", .kind = "full_text", .doc_count = 1 }};
            var runtimes = [_]metadata.RuntimeGroupStatusReport{
                .{ .group_id = 102, .indexes = &indexes },
                .{ .group_id = 101, .indexes = &indexes },
                .{ .group_id = 102, .indexes = &indexes },
            };
            const report: metadata.StoreStatusReport = .{ .store_id = 20, .reporter_incarnation = 77, .runtime_statuses = &runtimes };
            var initial = try publisher.prepare(a, report, false, false);
            defer initial.deinit(a);
            publisher.commit(a, &initial, testCursor(initial.update));
            var snapshot = try publisher.retainRuntimeSnapshot(a, report);
            defer snapshot.deinit(a);
            try std.testing.expectEqualDeep(&runtimes, snapshot.items);
            try std.testing.expectEqual(publisher.groups.get(102).?.runtimes[0].indexes.ptr, snapshot.items[0].indexes.ptr);
            indexes[0].doc_count = 2;
            var changed = try publisher.prepare(a, report, false, false);
            defer changed.deinit(a);
            publisher.commit(a, &changed, testCursor(changed.update));
            try std.testing.expectEqual(@as(u64, 1), snapshot.items[0].indexes[0].doc_count);
            publisher.deinit(a);
            try std.testing.expectEqual(@as(u64, 1), snapshot.items[2].indexes[0].doc_count);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Case.run, .{});
}

test "store report workload benchmark acknowledged runtime cache leases" {
    if (std.c.getenv("ANTFLY_CATALOG_REPORT_BENCH") == null) return;
    const a = std.heap.c_allocator;
    for ([_]usize{ 1000, 10000 }) |count| {
        const runtimes = try a.alloc(metadata.RuntimeGroupStatusReport, count);
        defer a.free(runtimes);
        var indexes = [_]metadata.RuntimeIndexStatusReport{.{ .name = "tenant_search", .kind = "full_text" }} ** 32;
        for (runtimes, 0..) |*runtime, i| runtime.* = .{ .group_id = i + 100, .indexes = &indexes };
        const report: metadata.StoreStatusReport = .{ .store_id = 20, .reporter_incarnation = 77, .runtime_statuses = runtimes };
        var publisher: Publisher = .{};
        defer publisher.deinit(a);
        var initial = try publisher.prepare(a, report, false, false);
        defer initial.deinit(a);
        publisher.commit(a, &initial, testCursor(initial.update));
        for ([_]bool{ false, true }) |shared| {
            var samples: [9]u64 = undefined;
            for (0..10) |sample| {
                const start = @import("antfly_platform").time.monotonicNs();
                if (shared) {
                    var snapshot = try publisher.retainRuntimeSnapshot(a, report);
                    snapshot.deinit(a);
                } else {
                    const copy = try metadata.cloneRuntimeGroupStatusReports(a, runtimes);
                    metadata.freeRuntimeGroupStatusReports(a, copy);
                }
                const elapsed = @import("antfly_platform").time.monotonicNs() - start;
                if (sample != 0) samples[sample - 1] = elapsed;
            }
            std.mem.sort(u64, &samples, {}, std.sort.asc(u64));
            std.debug.print("RUNTIME_CACHE_BENCH groups={d} indexes_per_group=32 shared={} p50_ns={d} samples_ns={any}\n", .{ count, shared, samples[4], samples });
        }
    }
}
