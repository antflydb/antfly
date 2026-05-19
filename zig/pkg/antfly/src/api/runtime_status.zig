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
const platform_time = @import("../platform/time.zig");
const db_mod = @import("../storage/db/mod.zig");

pub const RuntimeStatusSource = enum {
    unknown,
    synthetic_config,
    cached_snapshot,
    live_writer_publish,
    background_refresh,
    startup_catch_up,
    remote_store,
};

pub const RuntimeStatusFreshness = enum {
    unknown,
    fresh,
    stale,
    missing,
    remote_unknown,
    opening,
    catching_up,
    failed,
};

pub const RuntimeStatusMetadata = struct {
    updated_at_ns: u64 = 0,
    source: RuntimeStatusSource = .unknown,
    freshness: RuntimeStatusFreshness = .unknown,
    topology_generation: u64 = 0,
    lsm_root_generation: u64 = 0,
    status_generation: u64 = 0,
    store_id: u64 = 0,
    node_id: u64 = 0,

    pub fn withDefaults(self: @This(), source: RuntimeStatusSource, now_ns: u64) @This() {
        var out = self;
        if (out.updated_at_ns == 0) out.updated_at_ns = now_ns;
        if (out.source == .unknown) out.source = source;
        if (out.freshness == .unknown) out.freshness = .fresh;
        return out;
    }
};

pub const LocalTableRuntimeStatus = struct {
    group_id: u64 = 0,
    metadata: RuntimeStatusMetadata = .{},
    disk_bytes: u64 = 0,
    created_at_millis: u64 = 0,
    stats: db_mod.types.DBStats,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        db_mod.types.freeDBStats(alloc, self.stats);
        self.* = undefined;
    }

    pub fn clone(self: *const @This(), alloc: std.mem.Allocator) !@This() {
        return .{
            .group_id = self.group_id,
            .metadata = self.metadata,
            .disk_bytes = self.disk_bytes,
            .created_at_millis = self.created_at_millis,
            .stats = try cloneDBStats(alloc, self.stats),
        };
    }

    pub fn withMetadataDefaults(self: *@This(), source: RuntimeStatusSource, now_ns: u64) void {
        self.metadata = self.metadata.withDefaults(source, now_ns);
    }
};

pub fn statusHasRuntimeFacts(status: LocalTableRuntimeStatus) bool {
    return switch (status.metadata.source) {
        .live_writer_publish, .background_refresh, .startup_catch_up, .remote_store => true,
        .cached_snapshot, .unknown, .synthetic_config => statusStatsHaveRuntimeFacts(status.stats),
    };
}

pub fn statusRuntimeFresh(status: LocalTableRuntimeStatus) bool {
    return statusHasRuntimeFacts(status) and status.metadata.freshness == .fresh;
}

pub const LocalTableRuntimeStatuses = struct {
    items: []LocalTableRuntimeStatus = &.{},

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        for (self.items) |*item| item.deinit(alloc);
        if (self.items.len > 0) alloc.free(self.items);
        self.* = undefined;
    }

    pub fn clone(self: *const @This(), alloc: std.mem.Allocator) !@This() {
        const items = try alloc.alloc(LocalTableRuntimeStatus, self.items.len);
        var initialized: usize = 0;
        errdefer {
            for (items[0..initialized]) |*item| item.deinit(alloc);
            alloc.free(items);
        }

        for (self.items, 0..) |item, i| {
            items[i] = try item.clone(alloc);
            initialized += 1;
        }
        return .{ .items = items };
    }
};

pub const TableRuntimeSnapshot = struct {
    table_name: []u8,
    statuses: LocalTableRuntimeStatuses,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.table_name);
        self.statuses.deinit(alloc);
        self.* = undefined;
    }
};

pub const TableRuntimeSummary = struct {
    table_count: usize = 0,
    group_count: usize = 0,
    index_count: usize = 0,
    tables_with_replay_debt: usize = 0,
    groups_with_replay_debt: usize = 0,
    indexes_with_replay_debt: usize = 0,
    outstanding_replay_sequences: u64 = 0,
    max_index_replay_backlog: u64 = 0,
    async_indexing: db_mod.types.AsyncIndexingStats = .{},
};

pub const TableRuntimeSnapshotCache = struct {
    alloc: std.mem.Allocator,
    mutex: std.atomic.Mutex = .unlocked,
    entries: std.ArrayListUnmanaged(TableRuntimeSnapshot) = .empty,

    pub fn init(alloc: std.mem.Allocator) @This() {
        return .{ .alloc = alloc };
    }

    pub fn deinit(self: *@This()) void {
        self.clear();
        self.entries.deinit(self.alloc);
        self.* = undefined;
    }

    pub fn clear(self: *@This()) void {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        self.clearLocked();
    }

    pub fn invalidateTable(self: *@This(), table_name: []const u8) void {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        self.removeTableLocked(table_name);
    }

    pub fn replaceOwned(self: *@This(), snapshots: []TableRuntimeSnapshot) void {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();

        var new_entries = std.ArrayListUnmanaged(TableRuntimeSnapshot).empty;
        new_entries.appendSlice(self.alloc, snapshots) catch {
            for (snapshots) |*entry| entry.deinit(self.alloc);
            return;
        };

        const now_ns = platform_time.monotonicNs();
        for (new_entries.items) |*entry| {
            for (entry.statuses.items) |*status| {
                status.withMetadataDefaults(.background_refresh, now_ns);
                self.preserveCachedStatusForSyntheticPlaceholderLocked(entry.table_name, status, now_ns) catch {};
            }
        }

        var old_entries = self.entries;
        self.entries = new_entries;
        for (old_entries.items) |*entry| entry.deinit(self.alloc);
        old_entries.deinit(self.alloc);
    }

    pub fn replaceOwnedPreservingGroupStatus(
        self: *@This(),
        snapshots: []TableRuntimeSnapshot,
        table_name: []const u8,
        group_id: u64,
    ) !void {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();

        var new_entries = std.ArrayListUnmanaged(TableRuntimeSnapshot).empty;
        errdefer {
            for (new_entries.items) |*entry| entry.deinit(self.alloc);
            new_entries.deinit(self.alloc);
        }

        new_entries.appendSlice(self.alloc, snapshots) catch {
            for (snapshots) |*entry| entry.deinit(self.alloc);
            return;
        };
        const now_ns = platform_time.monotonicNs();
        for (new_entries.items) |*entry| {
            for (entry.statuses.items) |*status| {
                status.withMetadataDefaults(.background_refresh, now_ns);
                self.preserveCachedStatusForSyntheticPlaceholderLocked(entry.table_name, status, now_ns) catch {};
            }
        }

        var preserved: ?LocalTableRuntimeStatus = null;
        errdefer if (preserved) |*status| status.deinit(self.alloc);

        for (self.entries.items) |*entry| {
            if (!std.mem.eql(u8, entry.table_name, table_name)) continue;
            for (entry.statuses.items) |status| {
                if (status.group_id != group_id) continue;
                if (!runtimeStatusWorthPreserving(status)) break;
                preserved = try status.clone(self.alloc);
                break;
            }
            if (preserved != null) break;
        }

        if (preserved) |status| {
            defer {
                var owned = status;
                owned.deinit(self.alloc);
            }
            try self.upsertGroupStatusInEntries(&new_entries, table_name, status);
        }

        var old_entries = self.entries;
        self.entries = new_entries;
        for (old_entries.items) |*entry| entry.deinit(self.alloc);
        old_entries.deinit(self.alloc);
    }

    pub fn snapshot(self: *@This(), alloc: std.mem.Allocator, table_name: []const u8) !?LocalTableRuntimeStatuses {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        for (self.entries.items) |*entry| {
            if (!std.mem.eql(u8, entry.table_name, table_name)) continue;
            return try entry.statuses.clone(alloc);
        }
        return null;
    }

    pub fn snapshotGroupStatus(
        self: *@This(),
        alloc: std.mem.Allocator,
        table_name: []const u8,
        group_id: u64,
    ) !?LocalTableRuntimeStatus {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        for (self.entries.items) |*entry| {
            if (!std.mem.eql(u8, entry.table_name, table_name)) continue;
            for (entry.statuses.items) |status| {
                if (status.group_id != group_id) continue;
                return try status.clone(alloc);
            }
            return null;
        }
        return null;
    }

    pub fn upsertGroupStatus(self: *@This(), table_name: []const u8, status: LocalTableRuntimeStatus) !void {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        var owned = status;
        owned.withMetadataDefaults(.live_writer_publish, platform_time.monotonicNs());
        try self.upsertGroupStatusInEntries(&self.entries, table_name, owned);
    }

    fn upsertGroupStatusLocked(self: *@This(), table_name: []const u8, status: LocalTableRuntimeStatus) !void {
        try self.upsertGroupStatusInEntries(&self.entries, table_name, status);
    }

    fn upsertGroupStatusInEntries(
        self: *@This(),
        entries: *std.ArrayListUnmanaged(TableRuntimeSnapshot),
        table_name: []const u8,
        status: LocalTableRuntimeStatus,
    ) !void {
        for (entries.items) |*entry| {
            if (!std.mem.eql(u8, entry.table_name, table_name)) continue;
            for (entry.statuses.items) |*existing| {
                if (existing.group_id != status.group_id) continue;
                var cloned = try status.clone(self.alloc);
                errdefer cloned.deinit(self.alloc);
                preserveArtifactVisibilityOnReplayRegression(existing.*, &cloned);
                existing.deinit(self.alloc);
                existing.* = cloned;
                return;
            }

            var cloned = try status.clone(self.alloc);
            errdefer cloned.deinit(self.alloc);
            const grown = try self.alloc.realloc(entry.statuses.items, entry.statuses.items.len + 1);
            entry.statuses.items = grown;
            entry.statuses.items[entry.statuses.items.len - 1] = cloned;
            return;
        }

        const owned_table_name = try self.alloc.dupe(u8, table_name);
        errdefer self.alloc.free(owned_table_name);
        const items = try self.alloc.alloc(LocalTableRuntimeStatus, 1);
        errdefer self.alloc.free(items);
        items[0] = try status.clone(self.alloc);
        errdefer items[0].deinit(self.alloc);
        try entries.append(self.alloc, .{
            .table_name = owned_table_name,
            .statuses = .{ .items = items },
        });
    }

    fn preserveCachedStatusForSyntheticPlaceholderLocked(
        self: *@This(),
        table_name: []const u8,
        status: *LocalTableRuntimeStatus,
        now_ns: u64,
    ) !void {
        if (status.metadata.source != .synthetic_config) return;

        const previous = self.findGroupStatusLocked(table_name, status.group_id) orelse return;
        if (!runtimeStatusWorthPreserving(previous)) return;

        var merged = try mergeCachedStatusWithSyntheticPlaceholder(self.alloc, previous, status.*, now_ns);
        errdefer merged.deinit(self.alloc);
        status.deinit(self.alloc);
        status.* = merged;
    }

    fn findGroupStatusLocked(
        self: *@This(),
        table_name: []const u8,
        group_id: u64,
    ) ?LocalTableRuntimeStatus {
        for (self.entries.items) |*entry| {
            if (!std.mem.eql(u8, entry.table_name, table_name)) continue;
            for (entry.statuses.items) |status| {
                if (status.group_id == group_id) return status;
            }
            return null;
        }
        return null;
    }

    pub fn summary(self: *@This()) TableRuntimeSummary {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();

        var result: TableRuntimeSummary = .{
            .table_count = self.entries.items.len,
        };
        for (self.entries.items) |*entry| {
            var table_has_replay_debt = false;
            for (entry.statuses.items) |status| {
                result.group_count += 1;
                db_mod.types.accumulateAsyncIndexingStats(&result.async_indexing, status.stats.async_indexing);
                var group_has_replay_debt = false;
                result.index_count += status.stats.indexes.len;
                for (status.stats.indexes) |index| {
                    const backlog = if (index.replay_target_sequence > index.replay_applied_sequence)
                        index.replay_target_sequence - index.replay_applied_sequence
                    else
                        0;
                    const has_replay_debt = index.replay_catch_up_required or backlog > 0;
                    if (!has_replay_debt) continue;
                    group_has_replay_debt = true;
                    table_has_replay_debt = true;
                    result.indexes_with_replay_debt += 1;
                    result.outstanding_replay_sequences += backlog;
                    result.max_index_replay_backlog = @max(result.max_index_replay_backlog, backlog);
                }
                if (group_has_replay_debt) result.groups_with_replay_debt += 1;
            }
            if (table_has_replay_debt) result.tables_with_replay_debt += 1;
        }
        return result;
    }

    fn clearLocked(self: *@This()) void {
        for (self.entries.items) |*entry| entry.deinit(self.alloc);
        self.entries.clearRetainingCapacity();
    }

    fn removeTableLocked(self: *@This(), table_name: []const u8) void {
        var i: usize = 0;
        while (i < self.entries.items.len) {
            if (!std.mem.eql(u8, self.entries.items[i].table_name, table_name)) {
                i += 1;
                continue;
            }
            var removed = self.entries.orderedRemove(i);
            removed.deinit(self.alloc);
        }
    }
};

fn preserveArtifactVisibilityOnReplayRegression(previous: LocalTableRuntimeStatus, incoming: *LocalTableRuntimeStatus) void {
    var preserved_visibility = false;
    for (incoming.stats.indexes) |*dst| {
        const cached = findMatchingIndexStatus(previous.stats.indexes, dst.name, dst.kind) orelse continue;
        const applied_regressed = dst.replay_applied_sequence < cached.replay_applied_sequence;
        const target_not_older = dst.replay_target_sequence >= cached.replay_target_sequence;
        const cached_has_visibility = indexHasArtifactVisibilityFacts(cached);
        const dst_has_visibility = indexHasArtifactVisibilityFacts(dst.*);
        const visibility_regressed_without_newer_replay = target_not_older and
            cached_has_visibility and
            !dst_has_visibility and
            dst.replay_applied_sequence <= cached.replay_applied_sequence;
        if (!applied_regressed and !visibility_regressed_without_newer_replay) continue;

        preserveIndexArtifactVisibility(dst, cached);
        dst.replay_applied_sequence = @max(dst.replay_applied_sequence, cached.replay_applied_sequence);
        dst.replay_target_sequence = @max(dst.replay_target_sequence, cached.replay_target_sequence);
        dst.catch_up_applied_sequence = @max(dst.catch_up_applied_sequence, cached.catch_up_applied_sequence);
        dst.catch_up_target_sequence = @max(dst.catch_up_target_sequence, cached.catch_up_target_sequence);
        dst.replay_catch_up_required = dst.replay_applied_sequence < dst.replay_target_sequence;
        dst.backfill_active = dst.backfill_active or dst.replay_catch_up_required;
        if (dst.replay_target_sequence > 0 and dst.replay_applied_sequence < dst.replay_target_sequence) {
            dst.backfill_progress = @min(
                1.0,
                @as(f64, @floatFromInt(dst.replay_applied_sequence)) /
                    @as(f64, @floatFromInt(dst.replay_target_sequence)),
            );
        }
        preserved_visibility = true;
    }
    if (preserved_visibility and incoming.stats.doc_count < previous.stats.doc_count) {
        incoming.stats.doc_count = previous.stats.doc_count;
    }
}

fn runtimeStatusWorthPreserving(status: LocalTableRuntimeStatus) bool {
    if (statusHasRuntimeFacts(status)) return true;
    return false;
}

fn statusStatsHaveRuntimeFacts(stats: db_mod.types.DBStats) bool {
    if (stats.doc_count > 0) return true;
    if (stats.async_indexing.startup.active or stats.async_indexing.dense_catch_up.active) return true;
    if (stats.enrichment.enabled and (stats.enrichment.processed_requests > 0 or stats.enrichment.applied_sequence > 0 or stats.enrichment.target_sequence > 0 or stats.enrichment.retrying or stats.enrichment.worker_failed)) return true;
    if (stats.text_merge.pending_segments > 0 or stats.text_merge.in_flight_merges > 0 or stats.text_merge.completed_merges > 0 or stats.text_merge.failed_merges > 0) return true;
    for (stats.indexes) |index| {
        if (indexHasArtifactVisibilityFacts(index)) return true;
        if (index.backfill_active or index.catch_up_active or index.replay_catch_up_required) return true;
        // A target-only replay/catch-up marker can be synthesized from topology
        // and accepted sequence. It is not enough to prove that a live runtime
        // has ever published concrete index state.
    }
    return false;
}

fn indexHasArtifactVisibilityFacts(index: db_mod.types.DBIndexStats) bool {
    return index.doc_count > 0 or
        index.term_count > 0 or
        index.edge_count > 0 or
        index.node_count > 0 or
        index.root_node > 0;
}

fn preserveIndexArtifactVisibility(dst: *db_mod.types.DBIndexStats, cached: db_mod.types.DBIndexStats) void {
    dst.doc_count = cached.doc_count;
    dst.term_count = cached.term_count;
    dst.edge_count = cached.edge_count;
    dst.node_count = cached.node_count;
    dst.root_node = cached.root_node;
    dst.text_merge = cached.text_merge;
    dst.hbc_cache = cached.hbc_cache;
    dst.hbc_posting = cached.hbc_posting;
}

fn mergeCachedStatusWithSyntheticPlaceholder(
    alloc: std.mem.Allocator,
    previous: LocalTableRuntimeStatus,
    placeholder: LocalTableRuntimeStatus,
    now_ns: u64,
) !LocalTableRuntimeStatus {
    if (placeholder.stats.indexes.len == 0) {
        var cloned = try previous.clone(alloc);
        cloned.metadata = cachedSnapshotMetadata(previous.metadata, placeholder.metadata, now_ns);
        return cloned;
    }

    var merged = try placeholder.clone(alloc);
    errdefer merged.deinit(alloc);

    merged.stats.doc_count = previous.stats.doc_count;
    merged.stats.enrichment = previous.stats.enrichment;
    merged.stats.ttl_cleanup = previous.stats.ttl_cleanup;
    merged.stats.transaction_recovery = previous.stats.transaction_recovery;
    merged.stats.text_merge = previous.stats.text_merge;
    merged.stats.term_doc_freq_cache_hits = previous.stats.term_doc_freq_cache_hits;
    merged.stats.term_doc_freq_cache_misses = previous.stats.term_doc_freq_cache_misses;
    merged.stats.async_indexing = previous.stats.async_indexing;
    merged.stats.index_count = @intCast(merged.stats.indexes.len);

    for (merged.stats.indexes) |*dst| {
        const cached = findMatchingIndexStatus(previous.stats.indexes, dst.name, dst.kind) orelse continue;
        const owned_name = dst.name;
        dst.* = cached;
        dst.name = owned_name;
    }
    merged.metadata = cachedSnapshotMetadata(previous.metadata, placeholder.metadata, now_ns);
    return merged;
}

fn cachedSnapshotMetadata(
    previous: RuntimeStatusMetadata,
    placeholder: RuntimeStatusMetadata,
    now_ns: u64,
) RuntimeStatusMetadata {
    var metadata = previous;
    metadata.source = .cached_snapshot;
    metadata.freshness = switch (placeholder.freshness) {
        .unknown, .missing => .stale,
        else => placeholder.freshness,
    };
    metadata.updated_at_ns = now_ns;
    return metadata;
}

fn findMatchingIndexStatus(
    indexes: []const db_mod.types.DBIndexStats,
    name: []const u8,
    kind: db_mod.types.IndexKind,
) ?db_mod.types.DBIndexStats {
    for (indexes) |index| {
        if (index.kind != kind) continue;
        if (std.mem.eql(u8, index.name, name)) return index;
    }
    return null;
}

fn lockAtomic(mutex: *std.atomic.Mutex) void {
    while (!mutex.tryLock()) {
        std.Thread.yield() catch {};
    }
}

fn freeAlgebraicCandidateStatuses(alloc: std.mem.Allocator, candidates: []const db_mod.types.AlgebraicCandidateStatus) void {
    for (candidates) |candidate| {
        alloc.free(candidate.recommendation);
        alloc.free(candidate.materialization_id);
        alloc.free(candidate.lifecycle);
        alloc.free(candidate.decision);
    }
    if (candidates.len > 0) alloc.free(candidates);
}

fn cloneAlgebraicCandidateStatuses(
    alloc: std.mem.Allocator,
    candidates: []const db_mod.types.AlgebraicCandidateStatus,
) ![]const db_mod.types.AlgebraicCandidateStatus {
    if (candidates.len == 0) return &.{};
    const out = try alloc.alloc(db_mod.types.AlgebraicCandidateStatus, candidates.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |candidate| {
            alloc.free(candidate.recommendation);
            alloc.free(candidate.materialization_id);
            alloc.free(candidate.lifecycle);
            alloc.free(candidate.decision);
        }
        alloc.free(out);
    }
    for (candidates, 0..) |candidate, i| {
        const recommendation = try alloc.dupe(u8, candidate.recommendation);
        errdefer alloc.free(recommendation);
        const materialization_id = try alloc.dupe(u8, candidate.materialization_id);
        errdefer alloc.free(materialization_id);
        const lifecycle = try alloc.dupe(u8, candidate.lifecycle);
        errdefer alloc.free(lifecycle);
        const decision = try alloc.dupe(u8, candidate.decision);
        errdefer alloc.free(decision);
        out[i] = .{
            .recommendation = recommendation,
            .materialization_id = materialization_id,
            .lifecycle = lifecycle,
            .decision = decision,
            .observation_count = candidate.observation_count,
            .estimated_scan_rows_saved = candidate.estimated_scan_rows_saved,
            .estimated_write_cost = candidate.estimated_write_cost,
            .estimated_tensor_rows = candidate.estimated_tensor_rows,
            .estimated_storage_bytes = candidate.estimated_storage_bytes,
            .estimated_write_amplification = candidate.estimated_write_amplification,
            .score = candidate.score,
            .idle_miss_count = candidate.idle_miss_count,
            .generation = candidate.generation,
        };
        initialized += 1;
    }
    return out;
}

fn freeAlgebraicCandidateDecisionStatuses(alloc: std.mem.Allocator, decisions: []const db_mod.types.AlgebraicCandidateDecisionStatus) void {
    for (decisions) |decision| {
        alloc.free(decision.recommendation);
        alloc.free(decision.materialization_id);
        alloc.free(decision.lifecycle);
        alloc.free(decision.previous_decision);
        alloc.free(decision.decision);
    }
    if (decisions.len > 0) alloc.free(decisions);
}

fn cloneAlgebraicCandidateDecisionStatuses(
    alloc: std.mem.Allocator,
    decisions: []const db_mod.types.AlgebraicCandidateDecisionStatus,
) ![]const db_mod.types.AlgebraicCandidateDecisionStatus {
    if (decisions.len == 0) return &.{};
    const out = try alloc.alloc(db_mod.types.AlgebraicCandidateDecisionStatus, decisions.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |decision| {
            alloc.free(decision.recommendation);
            alloc.free(decision.materialization_id);
            alloc.free(decision.lifecycle);
            alloc.free(decision.previous_decision);
            alloc.free(decision.decision);
        }
        alloc.free(out);
    }
    for (decisions, 0..) |decision, i| {
        const recommendation = try alloc.dupe(u8, decision.recommendation);
        errdefer alloc.free(recommendation);
        const materialization_id = try alloc.dupe(u8, decision.materialization_id);
        errdefer alloc.free(materialization_id);
        const lifecycle = try alloc.dupe(u8, decision.lifecycle);
        errdefer alloc.free(lifecycle);
        const previous_decision = try alloc.dupe(u8, decision.previous_decision);
        errdefer alloc.free(previous_decision);
        const decision_text = try alloc.dupe(u8, decision.decision);
        errdefer alloc.free(decision_text);
        out[i] = .{
            .recommendation = recommendation,
            .materialization_id = materialization_id,
            .lifecycle = lifecycle,
            .previous_decision = previous_decision,
            .decision = decision_text,
            .observation_count = decision.observation_count,
            .estimated_scan_rows_saved = decision.estimated_scan_rows_saved,
            .estimated_write_cost = decision.estimated_write_cost,
            .score = decision.score,
            .score_delta = decision.score_delta,
            .idle_miss_count = decision.idle_miss_count,
            .generation = decision.generation,
        };
        initialized += 1;
    }
    return out;
}

fn freeAlgebraicProgressStatuses(alloc: std.mem.Allocator, progress_items: []const db_mod.types.AlgebraicProgressStatus) void {
    for (progress_items) |progress| {
        alloc.free(progress.recommendation);
        alloc.free(progress.materialization_id);
        alloc.free(progress.lifecycle);
    }
    if (progress_items.len > 0) alloc.free(progress_items);
}

fn cloneAlgebraicProgressStatuses(
    alloc: std.mem.Allocator,
    progress_items: []const db_mod.types.AlgebraicProgressStatus,
) ![]const db_mod.types.AlgebraicProgressStatus {
    if (progress_items.len == 0) return &.{};
    const out = try alloc.alloc(db_mod.types.AlgebraicProgressStatus, progress_items.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |progress| {
            alloc.free(progress.recommendation);
            alloc.free(progress.materialization_id);
            alloc.free(progress.lifecycle);
        }
        alloc.free(out);
    }
    for (progress_items, 0..) |progress, i| {
        const recommendation = try alloc.dupe(u8, progress.recommendation);
        errdefer alloc.free(recommendation);
        const materialization_id = try alloc.dupe(u8, progress.materialization_id);
        errdefer alloc.free(materialization_id);
        const lifecycle = try alloc.dupe(u8, progress.lifecycle);
        errdefer alloc.free(lifecycle);
        out[i] = .{
            .recommendation = recommendation,
            .materialization_id = materialization_id,
            .lifecycle = lifecycle,
            .target_sequence = progress.target_sequence,
            .applied_sequence = progress.applied_sequence,
            .rows_processed = progress.rows_processed,
            .target_rows = progress.target_rows,
        };
        initialized += 1;
    }
    return out;
}

pub fn cloneDBStats(alloc: std.mem.Allocator, stats: db_mod.types.DBStats) !db_mod.types.DBStats {
    const indexes = try alloc.alloc(db_mod.types.DBIndexStats, stats.indexes.len);
    var initialized: usize = 0;
    errdefer {
        for (indexes[0..initialized]) |item| {
            alloc.free(item.name);
            if (item.algebraic_last_error_doc_key) |value| alloc.free(value);
            if (item.algebraic_last_error_reason) |value| alloc.free(value);
            if (item.algebraic_capability_fingerprint) |value| alloc.free(value);
            if (item.algebraic_capability_lifecycle_status) |value| alloc.free(value);
            if (item.algebraic_planner_last_decision) |value| alloc.free(value);
            if (item.algebraic_planner_last_fallback_reason) |value| alloc.free(value);
            if (item.algebraic_planner_lifecycle_blocking_reason) |value| alloc.free(value);
            if (item.algebraic_last_observed_query_shape) |value| alloc.free(value);
            if (item.algebraic_last_recommended_materialization) |value| alloc.free(value);
            if (item.algebraic_top_candidate) |candidate| {
                alloc.free(candidate.recommendation);
                alloc.free(candidate.materialization_id);
                alloc.free(candidate.lifecycle);
                alloc.free(candidate.decision);
            }
            if (item.algebraic_active_progress) |progress| {
                alloc.free(progress.recommendation);
                alloc.free(progress.materialization_id);
                alloc.free(progress.lifecycle);
            }
            for (item.algebraic_candidates) |candidate| {
                alloc.free(candidate.recommendation);
                alloc.free(candidate.materialization_id);
                alloc.free(candidate.lifecycle);
                alloc.free(candidate.decision);
            }
            if (item.algebraic_candidates.len > 0) alloc.free(item.algebraic_candidates);
            for (item.algebraic_progress) |progress| {
                alloc.free(progress.recommendation);
                alloc.free(progress.materialization_id);
                alloc.free(progress.lifecycle);
            }
            if (item.algebraic_progress.len > 0) alloc.free(item.algebraic_progress);
        }
        alloc.free(indexes);
    }

    for (stats.indexes, 0..) |item, i| {
        const algebraic_last_error_doc_key = if (item.algebraic_last_error_doc_key) |value|
            try alloc.dupe(u8, value)
        else
            null;
        errdefer if (algebraic_last_error_doc_key) |value| alloc.free(value);
        const algebraic_last_error_reason = if (item.algebraic_last_error_reason) |value|
            try alloc.dupe(u8, value)
        else
            null;
        errdefer if (algebraic_last_error_reason) |value| alloc.free(value);
        const algebraic_capability_fingerprint = if (item.algebraic_capability_fingerprint) |value|
            try alloc.dupe(u8, value)
        else
            null;
        errdefer if (algebraic_capability_fingerprint) |value| alloc.free(value);
        const algebraic_capability_lifecycle_status = if (item.algebraic_capability_lifecycle_status) |value|
            try alloc.dupe(u8, value)
        else
            null;
        errdefer if (algebraic_capability_lifecycle_status) |value| alloc.free(value);
        const algebraic_planner_last_decision = if (item.algebraic_planner_last_decision) |value|
            try alloc.dupe(u8, value)
        else
            null;
        errdefer if (algebraic_planner_last_decision) |value| alloc.free(value);
        const algebraic_planner_last_fallback_reason = if (item.algebraic_planner_last_fallback_reason) |value|
            try alloc.dupe(u8, value)
        else
            null;
        errdefer if (algebraic_planner_last_fallback_reason) |value| alloc.free(value);
        const algebraic_planner_lifecycle_blocking_reason = if (item.algebraic_planner_lifecycle_blocking_reason) |value|
            try alloc.dupe(u8, value)
        else
            null;
        errdefer if (algebraic_planner_lifecycle_blocking_reason) |value| alloc.free(value);
        const algebraic_last_observed_query_shape = if (item.algebraic_last_observed_query_shape) |value|
            try alloc.dupe(u8, value)
        else
            null;
        errdefer if (algebraic_last_observed_query_shape) |value| alloc.free(value);
        const algebraic_last_recommended_materialization = if (item.algebraic_last_recommended_materialization) |value|
            try alloc.dupe(u8, value)
        else
            null;
        errdefer if (algebraic_last_recommended_materialization) |value| alloc.free(value);
        const algebraic_top_candidate: ?db_mod.types.AlgebraicCandidateStatus = if (item.algebraic_top_candidate) |candidate| .{
            .recommendation = try alloc.dupe(u8, candidate.recommendation),
            .materialization_id = try alloc.dupe(u8, candidate.materialization_id),
            .lifecycle = try alloc.dupe(u8, candidate.lifecycle),
            .decision = try alloc.dupe(u8, candidate.decision),
            .observation_count = candidate.observation_count,
            .estimated_scan_rows_saved = candidate.estimated_scan_rows_saved,
            .estimated_write_cost = candidate.estimated_write_cost,
            .estimated_tensor_rows = candidate.estimated_tensor_rows,
            .estimated_storage_bytes = candidate.estimated_storage_bytes,
            .estimated_write_amplification = candidate.estimated_write_amplification,
            .score = candidate.score,
            .idle_miss_count = candidate.idle_miss_count,
            .generation = candidate.generation,
        } else null;
        errdefer if (algebraic_top_candidate) |candidate| {
            alloc.free(candidate.recommendation);
            alloc.free(candidate.materialization_id);
            alloc.free(candidate.lifecycle);
            alloc.free(candidate.decision);
        };
        const algebraic_active_progress: ?db_mod.types.AlgebraicProgressStatus = if (item.algebraic_active_progress) |progress| .{
            .recommendation = try alloc.dupe(u8, progress.recommendation),
            .materialization_id = try alloc.dupe(u8, progress.materialization_id),
            .lifecycle = try alloc.dupe(u8, progress.lifecycle),
            .target_sequence = progress.target_sequence,
            .applied_sequence = progress.applied_sequence,
            .rows_processed = progress.rows_processed,
            .target_rows = progress.target_rows,
        } else null;
        errdefer if (algebraic_active_progress) |progress| {
            alloc.free(progress.recommendation);
            alloc.free(progress.materialization_id);
            alloc.free(progress.lifecycle);
        };
        const algebraic_candidates = try cloneAlgebraicCandidateStatuses(alloc, item.algebraic_candidates);
        errdefer freeAlgebraicCandidateStatuses(alloc, algebraic_candidates);
        const algebraic_candidate_decision_history = try cloneAlgebraicCandidateDecisionStatuses(alloc, item.algebraic_candidate_decision_history);
        errdefer freeAlgebraicCandidateDecisionStatuses(alloc, algebraic_candidate_decision_history);
        const algebraic_progress = try cloneAlgebraicProgressStatuses(alloc, item.algebraic_progress);
        errdefer freeAlgebraicProgressStatuses(alloc, algebraic_progress);
        indexes[i] = .{
            .name = try alloc.dupe(u8, item.name),
            .kind = item.kind,
            .doc_count = item.doc_count,
            .term_count = item.term_count,
            .edge_count = item.edge_count,
            .node_count = item.node_count,
            .root_node = item.root_node,
            .backfill_active = item.backfill_active,
            .backfill_progress = item.backfill_progress,
            .replay_applied_sequence = item.replay_applied_sequence,
            .replay_target_sequence = item.replay_target_sequence,
            .replay_catch_up_required = item.replay_catch_up_required,
            .catch_up_active = item.catch_up_active,
            .catch_up_phase = item.catch_up_phase,
            .catch_up_applied_sequence = item.catch_up_applied_sequence,
            .catch_up_target_sequence = item.catch_up_target_sequence,
            .text_merge = item.text_merge,
            .hbc_cache = item.hbc_cache,
            .hbc_posting = item.hbc_posting,
            .algebraic_parse_error_count = item.algebraic_parse_error_count,
            .algebraic_last_error_doc_key = algebraic_last_error_doc_key,
            .algebraic_last_error_reason = algebraic_last_error_reason,
            .algebraic_schema_version = item.algebraic_schema_version,
            .algebraic_capability_fingerprint = algebraic_capability_fingerprint,
            .algebraic_capability_lifecycle_status = algebraic_capability_lifecycle_status,
            .algebraic_capability_change_added_fields = item.algebraic_capability_change_added_fields,
            .algebraic_capability_change_removed_fields = item.algebraic_capability_change_removed_fields,
            .algebraic_capability_change_changed_type_fields = item.algebraic_capability_change_changed_type_fields,
            .algebraic_skipped_dynamic_fields = item.algebraic_skipped_dynamic_fields,
            .algebraic_skipped_complex_fields = item.algebraic_skipped_complex_fields,
            .algebraic_skipped_unbounded_fields = item.algebraic_skipped_unbounded_fields,
            .algebraic_minmax_cache_hits = item.algebraic_minmax_cache_hits,
            .algebraic_minmax_cache_misses = item.algebraic_minmax_cache_misses,
            .algebraic_minmax_support_scans = item.algebraic_minmax_support_scans,
            .algebraic_planner_selected = item.algebraic_planner_selected,
            .algebraic_planner_fallback_count = item.algebraic_planner_fallback_count,
            .algebraic_planner_last_decision = algebraic_planner_last_decision,
            .algebraic_planner_last_fallback_reason = algebraic_planner_last_fallback_reason,
            .algebraic_planner_last_estimated_scan_rows = item.algebraic_planner_last_estimated_scan_rows,
            .algebraic_planner_last_estimated_result_buckets = item.algebraic_planner_last_estimated_result_buckets,
            .algebraic_planner_lifecycle_ready = item.algebraic_planner_lifecycle_ready,
            .algebraic_planner_lifecycle_blocking_reason = algebraic_planner_lifecycle_blocking_reason,
            .algebraic_dictionary_registry_claimed_count = item.algebraic_dictionary_registry_claimed_count,
            .algebraic_dictionary_registry_already_owned_count = item.algebraic_dictionary_registry_already_owned_count,
            .algebraic_dictionary_registry_owned_by_other_count = item.algebraic_dictionary_registry_owned_by_other_count,
            .algebraic_dictionary_registry_ready_hit_count = item.algebraic_dictionary_registry_ready_hit_count,
            .algebraic_dictionary_registry_ready_miss_count = item.algebraic_dictionary_registry_ready_miss_count,
            .algebraic_distributed_partial_validation_proven_count = item.algebraic_distributed_partial_validation_proven_count,
            .algebraic_distributed_partial_validation_rejected_count = item.algebraic_distributed_partial_validation_rejected_count,
            .algebraic_distributed_partial_rows_exported_count = item.algebraic_distributed_partial_rows_exported_count,
            .algebraic_vector_filter_attempt_count = item.algebraic_vector_filter_attempt_count,
            .algebraic_vector_filter_resolved_count = item.algebraic_vector_filter_resolved_count,
            .algebraic_vector_filter_unsupported_count = item.algebraic_vector_filter_unsupported_count,
            .algebraic_vector_filter_fail_closed_count = item.algebraic_vector_filter_fail_closed_count,
            .algebraic_vector_filter_include_doc_id_count = item.algebraic_vector_filter_include_doc_id_count,
            .algebraic_vector_filter_exclude_doc_id_count = item.algebraic_vector_filter_exclude_doc_id_count,
            .algebraic_graph_traversal_attempt_count = item.algebraic_graph_traversal_attempt_count,
            .algebraic_graph_traversal_proven_count = item.algebraic_graph_traversal_proven_count,
            .algebraic_graph_traversal_rejected_count = item.algebraic_graph_traversal_rejected_count,
            .algebraic_graph_traversal_fallback_count = item.algebraic_graph_traversal_fallback_count,
            .algebraic_graph_traversal_result_node_count = item.algebraic_graph_traversal_result_node_count,
            .algebraic_observed_query_shape_count = item.algebraic_observed_query_shape_count,
            .algebraic_recommendation_count = item.algebraic_recommendation_count,
            .algebraic_adaptive_candidate_count = item.algebraic_adaptive_candidate_count,
            .algebraic_adaptive_progress_count = item.algebraic_adaptive_progress_count,
            .algebraic_adaptive_backfilling_count = item.algebraic_adaptive_backfilling_count,
            .algebraic_adaptive_ready_count = item.algebraic_adaptive_ready_count,
            .algebraic_adaptive_stale_count = item.algebraic_adaptive_stale_count,
            .algebraic_adaptive_dematerialize_recommended_count = item.algebraic_adaptive_dematerialize_recommended_count,
            .algebraic_adaptive_decision_history_count = item.algebraic_adaptive_decision_history_count,
            .algebraic_adaptive_policy_drift_count = item.algebraic_adaptive_policy_drift_count,
            .algebraic_last_observed_query_shape = algebraic_last_observed_query_shape,
            .algebraic_last_recommended_materialization = algebraic_last_recommended_materialization,
            .algebraic_top_candidate = algebraic_top_candidate,
            .algebraic_active_progress = algebraic_active_progress,
            .algebraic_candidates = algebraic_candidates,
            .algebraic_candidate_decision_history = algebraic_candidate_decision_history,
            .algebraic_progress = algebraic_progress,
        };
        initialized += 1;
    }

    return .{
        .doc_count = stats.doc_count,
        .index_count = stats.index_count,
        .indexes = indexes,
        .enrichment = stats.enrichment,
        .ttl_cleanup = stats.ttl_cleanup,
        .transaction_recovery = stats.transaction_recovery,
        .text_merge = stats.text_merge,
        .term_doc_freq_cache_hits = stats.term_doc_freq_cache_hits,
        .term_doc_freq_cache_misses = stats.term_doc_freq_cache_misses,
        .async_indexing = stats.async_indexing,
    };
}

test "table runtime snapshot cache clones stored status" {
    var cache = TableRuntimeSnapshotCache.init(std.testing.allocator);
    defer cache.deinit();

    const items = try std.testing.allocator.alloc(LocalTableRuntimeStatus, 1);
    items[0] = .{
        .group_id = 7,
        .stats = .{
            .doc_count = 11,
            .index_count = 2,
            .indexes = try std.testing.allocator.alloc(db_mod.types.DBIndexStats, 2),
        },
    };
    items[0].stats.indexes[0] = .{
        .name = try std.testing.allocator.dupe(u8, "vec"),
        .kind = .dense_vector,
        .doc_count = 11,
        .node_count = 5,
    };
    items[0].stats.indexes[1] = .{
        .name = try std.testing.allocator.dupe(u8, "alg"),
        .kind = .algebraic,
        .doc_count = 11,
        .algebraic_parse_error_count = 1,
        .algebraic_schema_version = 42,
        .algebraic_capability_fingerprint = try std.testing.allocator.dupe(u8, "cap:v1"),
        .algebraic_capability_lifecycle_status = try std.testing.allocator.dupe(u8, "stale"),
        .algebraic_capability_change_added_fields = 15,
        .algebraic_capability_change_removed_fields = 16,
        .algebraic_capability_change_changed_type_fields = 17,
        .algebraic_skipped_dynamic_fields = 18,
        .algebraic_skipped_complex_fields = 19,
        .algebraic_skipped_unbounded_fields = 20,
        .algebraic_minmax_cache_hits = 2,
        .algebraic_minmax_cache_misses = 3,
        .algebraic_minmax_support_scans = 4,
        .algebraic_planner_selected = 5,
        .algebraic_planner_fallback_count = 6,
        .algebraic_planner_last_decision = try std.testing.allocator.dupe(u8, "fallback"),
        .algebraic_planner_last_fallback_reason = try std.testing.allocator.dupe(u8, "no_materialization"),
        .algebraic_planner_last_estimated_scan_rows = 61,
        .algebraic_planner_last_estimated_result_buckets = 62,
        .algebraic_planner_lifecycle_ready = false,
        .algebraic_planner_lifecycle_blocking_reason = try std.testing.allocator.dupe(u8, "capability_lifecycle_not_ready"),
        .algebraic_dictionary_registry_claimed_count = 63,
        .algebraic_dictionary_registry_already_owned_count = 64,
        .algebraic_dictionary_registry_owned_by_other_count = 65,
        .algebraic_dictionary_registry_ready_hit_count = 66,
        .algebraic_dictionary_registry_ready_miss_count = 67,
        .algebraic_distributed_partial_validation_proven_count = 68,
        .algebraic_distributed_partial_validation_rejected_count = 69,
        .algebraic_distributed_partial_rows_exported_count = 70,
        .algebraic_vector_filter_attempt_count = 71,
        .algebraic_vector_filter_resolved_count = 72,
        .algebraic_vector_filter_unsupported_count = 73,
        .algebraic_vector_filter_fail_closed_count = 74,
        .algebraic_vector_filter_include_doc_id_count = 75,
        .algebraic_vector_filter_exclude_doc_id_count = 76,
        .algebraic_graph_traversal_attempt_count = 77,
        .algebraic_graph_traversal_proven_count = 78,
        .algebraic_graph_traversal_rejected_count = 79,
        .algebraic_graph_traversal_fallback_count = 80,
        .algebraic_graph_traversal_result_node_count = 81,
        .algebraic_observed_query_shape_count = 7,
        .algebraic_recommendation_count = 8,
        .algebraic_adaptive_candidate_count = 9,
        .algebraic_adaptive_progress_count = 10,
        .algebraic_adaptive_backfilling_count = 11,
        .algebraic_adaptive_ready_count = 12,
        .algebraic_adaptive_stale_count = 13,
        .algebraic_adaptive_dematerialize_recommended_count = 14,
        .algebraic_adaptive_decision_history_count = 15,
        .algebraic_adaptive_policy_drift_count = 16,
        .algebraic_last_error_doc_key = try std.testing.allocator.dupe(u8, "bad-doc"),
        .algebraic_last_error_reason = try std.testing.allocator.dupe(u8, "invalid_json"),
        .algebraic_last_observed_query_shape = try std.testing.allocator.dupe(u8, "shape:v1"),
        .algebraic_last_recommended_materialization = try std.testing.allocator.dupe(u8, "recommendation:v1"),
        .algebraic_top_candidate = .{
            .recommendation = try std.testing.allocator.dupe(u8, "recommendation:v2"),
            .materialization_id = try std.testing.allocator.dupe(u8, "adaptive:v2"),
            .lifecycle = try std.testing.allocator.dupe(u8, "recommended"),
            .decision = try std.testing.allocator.dupe(u8, "materialize"),
            .observation_count = 15,
            .estimated_scan_rows_saved = 16,
            .estimated_write_cost = 17,
            .estimated_tensor_rows = 18,
            .estimated_storage_bytes = 19,
            .estimated_write_amplification = 20,
            .score = 21,
            .idle_miss_count = 22,
            .generation = 23,
        },
        .algebraic_active_progress = .{
            .recommendation = try std.testing.allocator.dupe(u8, "recommendation:v2"),
            .materialization_id = try std.testing.allocator.dupe(u8, "adaptive:v2"),
            .lifecycle = try std.testing.allocator.dupe(u8, "backfilling"),
            .target_sequence = 23,
            .applied_sequence = 24,
            .rows_processed = 25,
            .target_rows = 26,
        },
    };

    const snapshots = try std.testing.allocator.alloc(TableRuntimeSnapshot, 1);
    defer std.testing.allocator.free(snapshots);
    snapshots[0] = .{
        .table_name = try std.testing.allocator.dupe(u8, "docs"),
        .statuses = .{ .items = items },
    };
    cache.replaceOwned(snapshots);

    var cloned = (try cache.snapshot(std.testing.allocator, "docs")).?;
    defer cloned.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 1), cloned.items.len);
    try std.testing.expectEqual(@as(u64, 7), cloned.items[0].group_id);
    try std.testing.expectEqual(@as(u64, 11), cloned.items[0].stats.doc_count);
    try std.testing.expectEqualStrings("vec", cloned.items[0].stats.indexes[0].name);
    try std.testing.expectEqualStrings("alg", cloned.items[0].stats.indexes[1].name);
    try std.testing.expectEqual(@as(u64, 1), cloned.items[0].stats.indexes[1].algebraic_parse_error_count);
    try std.testing.expectEqual(@as(u32, 42), cloned.items[0].stats.indexes[1].algebraic_schema_version);
    try std.testing.expectEqualStrings("cap:v1", cloned.items[0].stats.indexes[1].algebraic_capability_fingerprint.?);
    try std.testing.expectEqualStrings("stale", cloned.items[0].stats.indexes[1].algebraic_capability_lifecycle_status.?);
    try std.testing.expectEqual(@as(u32, 15), cloned.items[0].stats.indexes[1].algebraic_capability_change_added_fields);
    try std.testing.expectEqual(@as(u32, 16), cloned.items[0].stats.indexes[1].algebraic_capability_change_removed_fields);
    try std.testing.expectEqual(@as(u32, 17), cloned.items[0].stats.indexes[1].algebraic_capability_change_changed_type_fields);
    try std.testing.expectEqual(@as(u32, 18), cloned.items[0].stats.indexes[1].algebraic_skipped_dynamic_fields);
    try std.testing.expectEqual(@as(u32, 19), cloned.items[0].stats.indexes[1].algebraic_skipped_complex_fields);
    try std.testing.expectEqual(@as(u32, 20), cloned.items[0].stats.indexes[1].algebraic_skipped_unbounded_fields);
    try std.testing.expectEqual(@as(u64, 2), cloned.items[0].stats.indexes[1].algebraic_minmax_cache_hits);
    try std.testing.expectEqual(@as(u64, 3), cloned.items[0].stats.indexes[1].algebraic_minmax_cache_misses);
    try std.testing.expectEqual(@as(u64, 4), cloned.items[0].stats.indexes[1].algebraic_minmax_support_scans);
    try std.testing.expectEqual(@as(u64, 5), cloned.items[0].stats.indexes[1].algebraic_planner_selected);
    try std.testing.expectEqual(@as(u64, 6), cloned.items[0].stats.indexes[1].algebraic_planner_fallback_count);
    try std.testing.expectEqualStrings("fallback", cloned.items[0].stats.indexes[1].algebraic_planner_last_decision.?);
    try std.testing.expectEqualStrings("no_materialization", cloned.items[0].stats.indexes[1].algebraic_planner_last_fallback_reason.?);
    try std.testing.expectEqual(@as(u64, 61), cloned.items[0].stats.indexes[1].algebraic_planner_last_estimated_scan_rows.?);
    try std.testing.expectEqual(@as(u64, 62), cloned.items[0].stats.indexes[1].algebraic_planner_last_estimated_result_buckets.?);
    try std.testing.expect(!cloned.items[0].stats.indexes[1].algebraic_planner_lifecycle_ready);
    try std.testing.expectEqualStrings("capability_lifecycle_not_ready", cloned.items[0].stats.indexes[1].algebraic_planner_lifecycle_blocking_reason.?);
    try std.testing.expectEqual(@as(u64, 63), cloned.items[0].stats.indexes[1].algebraic_dictionary_registry_claimed_count);
    try std.testing.expectEqual(@as(u64, 64), cloned.items[0].stats.indexes[1].algebraic_dictionary_registry_already_owned_count);
    try std.testing.expectEqual(@as(u64, 65), cloned.items[0].stats.indexes[1].algebraic_dictionary_registry_owned_by_other_count);
    try std.testing.expectEqual(@as(u64, 66), cloned.items[0].stats.indexes[1].algebraic_dictionary_registry_ready_hit_count);
    try std.testing.expectEqual(@as(u64, 67), cloned.items[0].stats.indexes[1].algebraic_dictionary_registry_ready_miss_count);
    try std.testing.expectEqual(@as(u64, 68), cloned.items[0].stats.indexes[1].algebraic_distributed_partial_validation_proven_count);
    try std.testing.expectEqual(@as(u64, 69), cloned.items[0].stats.indexes[1].algebraic_distributed_partial_validation_rejected_count);
    try std.testing.expectEqual(@as(u64, 70), cloned.items[0].stats.indexes[1].algebraic_distributed_partial_rows_exported_count);
    try std.testing.expectEqual(@as(u64, 71), cloned.items[0].stats.indexes[1].algebraic_vector_filter_attempt_count);
    try std.testing.expectEqual(@as(u64, 72), cloned.items[0].stats.indexes[1].algebraic_vector_filter_resolved_count);
    try std.testing.expectEqual(@as(u64, 73), cloned.items[0].stats.indexes[1].algebraic_vector_filter_unsupported_count);
    try std.testing.expectEqual(@as(u64, 74), cloned.items[0].stats.indexes[1].algebraic_vector_filter_fail_closed_count);
    try std.testing.expectEqual(@as(u64, 75), cloned.items[0].stats.indexes[1].algebraic_vector_filter_include_doc_id_count);
    try std.testing.expectEqual(@as(u64, 76), cloned.items[0].stats.indexes[1].algebraic_vector_filter_exclude_doc_id_count);
    try std.testing.expectEqual(@as(u64, 77), cloned.items[0].stats.indexes[1].algebraic_graph_traversal_attempt_count);
    try std.testing.expectEqual(@as(u64, 78), cloned.items[0].stats.indexes[1].algebraic_graph_traversal_proven_count);
    try std.testing.expectEqual(@as(u64, 79), cloned.items[0].stats.indexes[1].algebraic_graph_traversal_rejected_count);
    try std.testing.expectEqual(@as(u64, 80), cloned.items[0].stats.indexes[1].algebraic_graph_traversal_fallback_count);
    try std.testing.expectEqual(@as(u64, 81), cloned.items[0].stats.indexes[1].algebraic_graph_traversal_result_node_count);
    try std.testing.expectEqual(@as(u64, 7), cloned.items[0].stats.indexes[1].algebraic_observed_query_shape_count);
    try std.testing.expectEqual(@as(u64, 8), cloned.items[0].stats.indexes[1].algebraic_recommendation_count);
    try std.testing.expectEqual(@as(u64, 9), cloned.items[0].stats.indexes[1].algebraic_adaptive_candidate_count);
    try std.testing.expectEqual(@as(u64, 10), cloned.items[0].stats.indexes[1].algebraic_adaptive_progress_count);
    try std.testing.expectEqual(@as(u64, 11), cloned.items[0].stats.indexes[1].algebraic_adaptive_backfilling_count);
    try std.testing.expectEqual(@as(u64, 12), cloned.items[0].stats.indexes[1].algebraic_adaptive_ready_count);
    try std.testing.expectEqual(@as(u64, 13), cloned.items[0].stats.indexes[1].algebraic_adaptive_stale_count);
    try std.testing.expectEqual(@as(u64, 14), cloned.items[0].stats.indexes[1].algebraic_adaptive_dematerialize_recommended_count);
    try std.testing.expectEqual(@as(u64, 15), cloned.items[0].stats.indexes[1].algebraic_adaptive_decision_history_count);
    try std.testing.expectEqual(@as(u64, 16), cloned.items[0].stats.indexes[1].algebraic_adaptive_policy_drift_count);
    try std.testing.expectEqualStrings("bad-doc", cloned.items[0].stats.indexes[1].algebraic_last_error_doc_key.?);
    try std.testing.expectEqualStrings("invalid_json", cloned.items[0].stats.indexes[1].algebraic_last_error_reason.?);
    try std.testing.expectEqualStrings("shape:v1", cloned.items[0].stats.indexes[1].algebraic_last_observed_query_shape.?);
    try std.testing.expectEqualStrings("recommendation:v1", cloned.items[0].stats.indexes[1].algebraic_last_recommended_materialization.?);
    const top_candidate = cloned.items[0].stats.indexes[1].algebraic_top_candidate.?;
    try std.testing.expectEqualStrings("recommendation:v2", top_candidate.recommendation);
    try std.testing.expectEqualStrings("adaptive:v2", top_candidate.materialization_id);
    try std.testing.expectEqualStrings("recommended", top_candidate.lifecycle);
    try std.testing.expectEqualStrings("materialize", top_candidate.decision);
    try std.testing.expectEqual(@as(u64, 15), top_candidate.observation_count);
    try std.testing.expectEqual(@as(i128, 21), top_candidate.score);
    const active_progress = cloned.items[0].stats.indexes[1].algebraic_active_progress.?;
    try std.testing.expectEqualStrings("recommendation:v2", active_progress.recommendation);
    try std.testing.expectEqualStrings("adaptive:v2", active_progress.materialization_id);
    try std.testing.expectEqualStrings("backfilling", active_progress.lifecycle);
    try std.testing.expectEqual(@as(u64, 23), active_progress.target_sequence);
    try std.testing.expectEqual(@as(u64, 25), active_progress.rows_processed);
}

test "table runtime snapshot cache replaces snapshots while preserving one group status" {
    var cache = TableRuntimeSnapshotCache.init(std.testing.allocator);
    defer cache.deinit();

    const docs_items = try std.testing.allocator.alloc(LocalTableRuntimeStatus, 1);
    docs_items[0] = .{
        .group_id = 7,
        .stats = .{
            .doc_count = 11,
            .index_count = 1,
            .indexes = try std.testing.allocator.alloc(db_mod.types.DBIndexStats, 1),
        },
    };
    docs_items[0].stats.indexes[0] = .{
        .name = try std.testing.allocator.dupe(u8, "vec"),
        .kind = .dense_vector,
        .doc_count = 11,
        .replay_applied_sequence = 5,
        .replay_target_sequence = 10,
        .replay_catch_up_required = true,
    };
    const initial = try std.testing.allocator.alloc(TableRuntimeSnapshot, 1);
    defer std.testing.allocator.free(initial);
    initial[0] = .{
        .table_name = try std.testing.allocator.dupe(u8, "docs"),
        .statuses = .{ .items = docs_items },
    };
    cache.replaceOwned(initial);

    const refresh_docs_items = try std.testing.allocator.alloc(LocalTableRuntimeStatus, 1);
    refresh_docs_items[0] = .{
        .group_id = 7,
        .stats = .{
            .doc_count = 99,
            .index_count = 1,
            .indexes = try std.testing.allocator.alloc(db_mod.types.DBIndexStats, 1),
        },
    };
    refresh_docs_items[0].stats.indexes[0] = .{
        .name = try std.testing.allocator.dupe(u8, "vec"),
        .kind = .dense_vector,
        .doc_count = 99,
    };
    const refresh_logs_items = try std.testing.allocator.alloc(LocalTableRuntimeStatus, 1);
    refresh_logs_items[0] = .{
        .group_id = 8,
        .stats = .{
            .doc_count = 3,
            .index_count = 1,
            .indexes = try std.testing.allocator.alloc(db_mod.types.DBIndexStats, 1),
        },
    };
    refresh_logs_items[0].stats.indexes[0] = .{
        .name = try std.testing.allocator.dupe(u8, "kw"),
        .kind = .full_text,
        .doc_count = 3,
    };
    const refresh = try std.testing.allocator.alloc(TableRuntimeSnapshot, 2);
    refresh[0] = .{
        .table_name = try std.testing.allocator.dupe(u8, "docs"),
        .statuses = .{ .items = refresh_docs_items },
    };
    refresh[1] = .{
        .table_name = try std.testing.allocator.dupe(u8, "logs"),
        .statuses = .{ .items = refresh_logs_items },
    };

    try cache.replaceOwnedPreservingGroupStatus(refresh, "docs", 7);

    var docs = (try cache.snapshot(std.testing.allocator, "docs")).?;
    defer docs.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u64, 11), docs.items[0].stats.doc_count);
    try std.testing.expectEqual(@as(u64, 5), docs.items[0].stats.indexes[0].replay_applied_sequence);
    try std.testing.expectEqual(@as(u64, 10), docs.items[0].stats.indexes[0].replay_target_sequence);
    try std.testing.expect(docs.items[0].stats.indexes[0].replay_catch_up_required);

    var logs = (try cache.snapshot(std.testing.allocator, "logs")).?;
    defer logs.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u64, 3), logs.items[0].stats.doc_count);
    try std.testing.expectEqualStrings("kw", logs.items[0].stats.indexes[0].name);
}

test "table runtime snapshot cache does not replace published live status with synthetic zero" {
    var cache = TableRuntimeSnapshotCache.init(std.testing.allocator);
    defer cache.deinit();

    const live_items = try std.testing.allocator.alloc(LocalTableRuntimeStatus, 1);
    live_items[0] = .{
        .group_id = 7,
        .metadata = .{
            .source = .live_writer_publish,
            .freshness = .fresh,
            .status_generation = 12,
        },
        .stats = .{
            .doc_count = 1_000_000,
            .index_count = 2,
            .indexes = try std.testing.allocator.alloc(db_mod.types.DBIndexStats, 2),
        },
    };
    live_items[0].stats.indexes[0] = .{
        .name = try std.testing.allocator.dupe(u8, "dense_idx"),
        .kind = .dense_vector,
        .doc_count = 1_000_000,
        .node_count = 44_321,
        .root_node = 3,
        .replay_applied_sequence = 4000,
        .replay_target_sequence = 4000,
        .catch_up_applied_sequence = 4000,
        .catch_up_target_sequence = 4000,
    };
    live_items[0].stats.indexes[1] = .{
        .name = try std.testing.allocator.dupe(u8, "text_idx"),
        .kind = .full_text,
        .doc_count = 1_000_000,
        .term_count = 83,
    };
    const initial = try std.testing.allocator.alloc(TableRuntimeSnapshot, 1);
    defer std.testing.allocator.free(initial);
    initial[0] = .{
        .table_name = try std.testing.allocator.dupe(u8, "docs"),
        .statuses = .{ .items = live_items },
    };
    cache.replaceOwned(initial);

    const synthetic_items = try std.testing.allocator.alloc(LocalTableRuntimeStatus, 1);
    synthetic_items[0] = .{
        .group_id = 7,
        .metadata = .{
            .source = .synthetic_config,
            .freshness = .stale,
        },
        .stats = .{
            .doc_count = 0,
            .index_count = 1,
            .indexes = try std.testing.allocator.alloc(db_mod.types.DBIndexStats, 1),
        },
    };
    synthetic_items[0].stats.indexes[0] = .{
        .name = try std.testing.allocator.dupe(u8, "dense_idx"),
        .kind = .dense_vector,
    };
    const refresh = try std.testing.allocator.alloc(TableRuntimeSnapshot, 1);
    defer std.testing.allocator.free(refresh);
    refresh[0] = .{
        .table_name = try std.testing.allocator.dupe(u8, "docs"),
        .statuses = .{ .items = synthetic_items },
    };
    cache.replaceOwned(refresh);

    var docs = (try cache.snapshot(std.testing.allocator, "docs")).?;
    defer docs.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 1), docs.items.len);
    try std.testing.expectEqual(RuntimeStatusSource.cached_snapshot, docs.items[0].metadata.source);
    try std.testing.expectEqual(RuntimeStatusFreshness.stale, docs.items[0].metadata.freshness);
    try std.testing.expectEqual(@as(u64, 1_000_000), docs.items[0].stats.doc_count);
    try std.testing.expectEqual(@as(u32, 1), docs.items[0].stats.index_count);
    try std.testing.expectEqual(@as(usize, 1), docs.items[0].stats.indexes.len);
    try std.testing.expectEqualStrings("dense_idx", docs.items[0].stats.indexes[0].name);
    try std.testing.expectEqual(@as(u64, 1_000_000), docs.items[0].stats.indexes[0].doc_count);
    try std.testing.expectEqual(@as(u64, 44_321), docs.items[0].stats.indexes[0].node_count);
    try std.testing.expectEqual(@as(u64, 3), docs.items[0].stats.indexes[0].root_node);
    try std.testing.expectEqual(@as(u64, 4000), docs.items[0].stats.indexes[0].replay_applied_sequence);
    try std.testing.expectEqual(@as(u64, 4000), docs.items[0].stats.indexes[0].replay_target_sequence);
    try std.testing.expectEqual(@as(u64, 4000), docs.items[0].stats.indexes[0].catch_up_applied_sequence);
    try std.testing.expectEqual(@as(u64, 4000), docs.items[0].stats.indexes[0].catch_up_target_sequence);
}

test "table runtime snapshot cache preserving replacement does not replace live status with synthetic zero" {
    var cache = TableRuntimeSnapshotCache.init(std.testing.allocator);
    defer cache.deinit();

    const live_items = try std.testing.allocator.alloc(LocalTableRuntimeStatus, 1);
    live_items[0] = .{
        .group_id = 7,
        .metadata = .{
            .source = .live_writer_publish,
            .freshness = .fresh,
        },
        .stats = .{
            .doc_count = 250_000,
            .index_count = 1,
            .indexes = try std.testing.allocator.alloc(db_mod.types.DBIndexStats, 1),
        },
    };
    live_items[0].stats.indexes[0] = .{
        .name = try std.testing.allocator.dupe(u8, "dense_idx"),
        .kind = .dense_vector,
        .doc_count = 250_000,
        .node_count = 2048,
        .root_node = 1,
        .replay_applied_sequence = 1000,
        .replay_target_sequence = 4000,
        .replay_catch_up_required = true,
        .catch_up_active = true,
        .catch_up_applied_sequence = 1000,
        .catch_up_target_sequence = 4000,
    };
    const initial = try std.testing.allocator.alloc(TableRuntimeSnapshot, 1);
    defer std.testing.allocator.free(initial);
    initial[0] = .{
        .table_name = try std.testing.allocator.dupe(u8, "docs"),
        .statuses = .{ .items = live_items },
    };
    cache.replaceOwned(initial);

    const synthetic_items = try std.testing.allocator.alloc(LocalTableRuntimeStatus, 1);
    synthetic_items[0] = .{
        .group_id = 7,
        .metadata = .{
            .source = .synthetic_config,
            .freshness = .stale,
        },
        .stats = .{
            .doc_count = 0,
            .index_count = 1,
            .indexes = try std.testing.allocator.alloc(db_mod.types.DBIndexStats, 1),
        },
    };
    synthetic_items[0].stats.indexes[0] = .{
        .name = try std.testing.allocator.dupe(u8, "dense_idx"),
        .kind = .dense_vector,
    };
    const refresh = try std.testing.allocator.alloc(TableRuntimeSnapshot, 1);
    defer std.testing.allocator.free(refresh);
    refresh[0] = .{
        .table_name = try std.testing.allocator.dupe(u8, "docs"),
        .statuses = .{ .items = synthetic_items },
    };

    try cache.replaceOwnedPreservingGroupStatus(refresh, "docs", 99);

    var docs = (try cache.snapshot(std.testing.allocator, "docs")).?;
    defer docs.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 1), docs.items.len);
    try std.testing.expectEqual(RuntimeStatusSource.cached_snapshot, docs.items[0].metadata.source);
    try std.testing.expectEqual(RuntimeStatusFreshness.stale, docs.items[0].metadata.freshness);
    try std.testing.expectEqual(@as(u64, 250_000), docs.items[0].stats.doc_count);
    try std.testing.expectEqual(@as(u64, 250_000), docs.items[0].stats.indexes[0].doc_count);
    try std.testing.expectEqual(@as(u64, 2048), docs.items[0].stats.indexes[0].node_count);
    try std.testing.expectEqual(@as(u64, 1000), docs.items[0].stats.indexes[0].replay_applied_sequence);
    try std.testing.expectEqual(@as(u64, 4000), docs.items[0].stats.indexes[0].replay_target_sequence);
    try std.testing.expect(docs.items[0].stats.indexes[0].replay_catch_up_required);
    try std.testing.expect(docs.items[0].stats.indexes[0].catch_up_active);
}

test "table runtime snapshot cache can clone a single group status" {
    var cache = TableRuntimeSnapshotCache.init(std.testing.allocator);
    defer cache.deinit();

    const statuses = try std.testing.allocator.alloc(LocalTableRuntimeStatus, 2);
    defer std.testing.allocator.free(statuses);
    statuses[0] = .{
        .group_id = 7,
        .stats = .{ .doc_count = 1, .indexes = &.{} },
    };
    statuses[1] = .{
        .group_id = 9,
        .stats = .{ .doc_count = 2, .indexes = &.{} },
    };
    const snapshots = try std.testing.allocator.alloc(TableRuntimeSnapshot, 1);
    defer std.testing.allocator.free(snapshots);
    snapshots[0] = .{
        .table_name = try std.testing.allocator.dupe(u8, "docs"),
        .statuses = .{ .items = statuses },
    };
    cache.replaceOwned(snapshots);

    var status = (try cache.snapshotGroupStatus(std.testing.allocator, "docs", 9)).?;
    defer status.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u64, 9), status.group_id);
    try std.testing.expectEqual(@as(u64, 2), status.stats.doc_count);
    try std.testing.expect((try cache.snapshotGroupStatus(std.testing.allocator, "docs", 8)) == null);
}

test "table runtime snapshot cache annotates publisher metadata defaults" {
    var cache = TableRuntimeSnapshotCache.init(std.testing.allocator);
    defer cache.deinit();

    const status = LocalTableRuntimeStatus{
        .group_id = 7,
        .stats = .{ .doc_count = 1, .indexes = &.{} },
    };
    try cache.upsertGroupStatus("docs", status);

    var cloned = (try cache.snapshot(std.testing.allocator, "docs")).?;
    defer cloned.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 1), cloned.items.len);
    try std.testing.expectEqual(RuntimeStatusSource.live_writer_publish, cloned.items[0].metadata.source);
    try std.testing.expectEqual(RuntimeStatusFreshness.fresh, cloned.items[0].metadata.freshness);
    try std.testing.expect(cloned.items[0].metadata.updated_at_ns > 0);
}

test "table runtime snapshot cache preserves dense visibility when live publish status regresses replay" {
    var cache = TableRuntimeSnapshotCache.init(std.testing.allocator);
    defer cache.deinit();

    const cached_indexes = try std.testing.allocator.alloc(db_mod.types.DBIndexStats, 1);
    cached_indexes[0] = .{
        .name = try std.testing.allocator.dupe(u8, "dense_idx"),
        .kind = .dense_vector,
        .doc_count = 25_000,
        .node_count = 469,
        .root_node = 1,
        .replay_applied_sequence = 100,
        .replay_target_sequence = 200,
        .replay_catch_up_required = true,
        .catch_up_applied_sequence = 100,
        .catch_up_target_sequence = 200,
        .hbc_cache = .{ .total_bytes = 1234, .accounted_bytes = 1234 },
    };
    var cached_status = LocalTableRuntimeStatus{
        .group_id = 7,
        .stats = .{
            .doc_count = 25_000,
            .index_count = 1,
            .indexes = cached_indexes,
        },
    };
    defer cached_status.deinit(std.testing.allocator);
    try cache.upsertGroupStatus("docs", cached_status);

    const regressed_indexes = try std.testing.allocator.alloc(db_mod.types.DBIndexStats, 1);
    regressed_indexes[0] = .{
        .name = try std.testing.allocator.dupe(u8, "dense_idx"),
        .kind = .dense_vector,
        .doc_count = 0,
        .node_count = 1,
        .root_node = 1,
        .replay_applied_sequence = 0,
        .replay_target_sequence = 200,
        .replay_catch_up_required = true,
        .catch_up_phase = .bulk_finish,
        .catch_up_applied_sequence = 0,
        .catch_up_target_sequence = 200,
    };
    var regressed_status = LocalTableRuntimeStatus{
        .group_id = 7,
        .stats = .{
            .doc_count = 0,
            .index_count = 1,
            .indexes = regressed_indexes,
            .async_indexing = .{
                .dense_catch_up = .{
                    .begin_calls = 2,
                    .finish_calls = 1,
                    .active = true,
                    .phase = .bulk_finish,
                },
            },
        },
    };
    defer regressed_status.deinit(std.testing.allocator);
    try cache.upsertGroupStatus("docs", regressed_status);

    var docs = (try cache.snapshot(std.testing.allocator, "docs")).?;
    defer docs.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(u64, 25_000), docs.items[0].stats.doc_count);
    try std.testing.expectEqual(@as(u64, 25_000), docs.items[0].stats.indexes[0].doc_count);
    try std.testing.expectEqual(@as(u64, 469), docs.items[0].stats.indexes[0].node_count);
    try std.testing.expectEqual(@as(u64, 100), docs.items[0].stats.indexes[0].replay_applied_sequence);
    try std.testing.expectEqual(@as(u64, 200), docs.items[0].stats.indexes[0].replay_target_sequence);
    try std.testing.expect(docs.items[0].stats.indexes[0].replay_catch_up_required);
    try std.testing.expect(docs.items[0].stats.indexes[0].backfill_active);
    try std.testing.expectEqual(db_mod.types.DenseCatchUpStats.Phase.bulk_finish, docs.items[0].stats.indexes[0].catch_up_phase);
    try std.testing.expectEqual(db_mod.types.DenseCatchUpStats.Phase.bulk_finish, docs.items[0].stats.async_indexing.dense_catch_up.phase);
}

test "table runtime snapshot cache allows dense visibility decrease with newer applied replay" {
    var cache = TableRuntimeSnapshotCache.init(std.testing.allocator);
    defer cache.deinit();

    const cached_indexes = try std.testing.allocator.alloc(db_mod.types.DBIndexStats, 1);
    cached_indexes[0] = .{
        .name = try std.testing.allocator.dupe(u8, "dense_idx"),
        .kind = .dense_vector,
        .doc_count = 25_000,
        .replay_applied_sequence = 100,
        .replay_target_sequence = 100,
        .catch_up_applied_sequence = 100,
        .catch_up_target_sequence = 100,
    };
    var cached_status = LocalTableRuntimeStatus{
        .group_id = 7,
        .stats = .{
            .doc_count = 25_000,
            .index_count = 1,
            .indexes = cached_indexes,
        },
    };
    defer cached_status.deinit(std.testing.allocator);
    try cache.upsertGroupStatus("docs", cached_status);

    const newer_indexes = try std.testing.allocator.alloc(db_mod.types.DBIndexStats, 1);
    newer_indexes[0] = .{
        .name = try std.testing.allocator.dupe(u8, "dense_idx"),
        .kind = .dense_vector,
        .doc_count = 24_999,
        .replay_applied_sequence = 101,
        .replay_target_sequence = 101,
        .catch_up_applied_sequence = 101,
        .catch_up_target_sequence = 101,
    };
    var newer_status = LocalTableRuntimeStatus{
        .group_id = 7,
        .stats = .{
            .doc_count = 24_999,
            .index_count = 1,
            .indexes = newer_indexes,
        },
    };
    defer newer_status.deinit(std.testing.allocator);
    try cache.upsertGroupStatus("docs", newer_status);

    var docs = (try cache.snapshot(std.testing.allocator, "docs")).?;
    defer docs.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(u64, 24_999), docs.items[0].stats.doc_count);
    try std.testing.expectEqual(@as(u64, 24_999), docs.items[0].stats.indexes[0].doc_count);
    try std.testing.expectEqual(@as(u64, 101), docs.items[0].stats.indexes[0].replay_applied_sequence);
    try std.testing.expectEqual(@as(u64, 101), docs.items[0].stats.indexes[0].replay_target_sequence);
}

test "cached replay sequence alone is not a runtime fact" {
    const indexes = try std.testing.allocator.alloc(db_mod.types.DBIndexStats, 1);
    defer std.testing.allocator.free(indexes);
    indexes[0] = .{
        .name = try std.testing.allocator.dupe(u8, "dense_idx"),
        .kind = .dense_vector,
        .replay_applied_sequence = 4000,
        .replay_target_sequence = 4000,
        .catch_up_applied_sequence = 4000,
        .catch_up_target_sequence = 4000,
    };
    defer std.testing.allocator.free(indexes[0].name);

    const status = LocalTableRuntimeStatus{
        .group_id = 7,
        .metadata = .{
            .source = .cached_snapshot,
            .freshness = .fresh,
        },
        .stats = .{
            .index_count = 1,
            .indexes = indexes,
        },
    };

    try std.testing.expect(!statusHasRuntimeFacts(status));
}

test "synthetic status with preserved visibility counters is a runtime fact" {
    const indexes = try std.testing.allocator.alloc(db_mod.types.DBIndexStats, 1);
    defer std.testing.allocator.free(indexes);
    indexes[0] = .{
        .name = try std.testing.allocator.dupe(u8, "dense_idx"),
        .kind = .dense_vector,
        .doc_count = 1_000_000,
        .node_count = 8_837,
        .replay_applied_sequence = 10_002,
        .replay_target_sequence = 10_002,
    };
    defer std.testing.allocator.free(indexes[0].name);

    const status = LocalTableRuntimeStatus{
        .group_id = 7,
        .metadata = .{
            .source = .synthetic_config,
            .freshness = .stale,
        },
        .stats = .{
            .doc_count = 1_000_000,
            .index_count = 1,
            .indexes = indexes,
        },
    };

    try std.testing.expect(statusHasRuntimeFacts(status));
}

test "table runtime snapshot cache preserves generic artifact visibility on sequence-only refresh" {
    var cache = TableRuntimeSnapshotCache.init(std.testing.allocator);
    defer cache.deinit();

    const cached_indexes = try std.testing.allocator.alloc(db_mod.types.DBIndexStats, 1);
    cached_indexes[0] = .{
        .name = try std.testing.allocator.dupe(u8, "text_idx"),
        .kind = .full_text,
        .doc_count = 10_000,
        .term_count = 321,
        .replay_applied_sequence = 400,
        .replay_target_sequence = 400,
    };
    var cached_status = LocalTableRuntimeStatus{
        .group_id = 7,
        .stats = .{
            .doc_count = 10_000,
            .index_count = 1,
            .indexes = cached_indexes,
        },
    };
    defer cached_status.deinit(std.testing.allocator);
    try cache.upsertGroupStatus("docs", cached_status);

    const incoming_indexes = try std.testing.allocator.alloc(db_mod.types.DBIndexStats, 1);
    incoming_indexes[0] = .{
        .name = try std.testing.allocator.dupe(u8, "text_idx"),
        .kind = .full_text,
        .replay_applied_sequence = 400,
        .replay_target_sequence = 400,
    };
    var incoming_status = LocalTableRuntimeStatus{
        .group_id = 7,
        .metadata = .{
            .source = .background_refresh,
            .freshness = .fresh,
        },
        .stats = .{
            .index_count = 1,
            .indexes = incoming_indexes,
        },
    };
    defer incoming_status.deinit(std.testing.allocator);
    try cache.upsertGroupStatus("docs", incoming_status);

    var docs = (try cache.snapshot(std.testing.allocator, "docs")).?;
    defer docs.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(u64, 10_000), docs.items[0].stats.doc_count);
    try std.testing.expectEqual(@as(u64, 10_000), docs.items[0].stats.indexes[0].doc_count);
    try std.testing.expectEqual(@as(u64, 321), docs.items[0].stats.indexes[0].term_count);
    try std.testing.expectEqual(@as(u64, 400), docs.items[0].stats.indexes[0].replay_applied_sequence);
    try std.testing.expectEqual(@as(u64, 400), docs.items[0].stats.indexes[0].replay_target_sequence);
}

test "table runtime snapshot cache preserves existing status on replacement allocation failure" {
    const Runner = struct {
        fn run(alloc: std.mem.Allocator) !void {
            var cache = TableRuntimeSnapshotCache.init(alloc);
            defer cache.deinit();

            const initial_items = try alloc.alloc(LocalTableRuntimeStatus, 1);
            initial_items[0] = .{
                .group_id = 7,
                .stats = .{
                    .doc_count = 11,
                    .index_count = 1,
                    .indexes = try alloc.alloc(db_mod.types.DBIndexStats, 1),
                },
            };
            initial_items[0].stats.indexes[0] = .{
                .name = try alloc.dupe(u8, "vec"),
                .kind = .dense_vector,
                .doc_count = 11,
                .replay_applied_sequence = 5,
                .replay_target_sequence = 10,
                .replay_catch_up_required = true,
            };
            const snapshots = try alloc.alloc(TableRuntimeSnapshot, 1);
            snapshots[0] = .{
                .table_name = try alloc.dupe(u8, "docs"),
                .statuses = .{ .items = initial_items },
            };
            cache.replaceOwned(snapshots);

            var replacement = LocalTableRuntimeStatus{
                .group_id = 7,
                .stats = .{
                    .doc_count = 99,
                    .index_count = 1,
                    .indexes = try alloc.alloc(db_mod.types.DBIndexStats, 1),
                },
            };
            defer replacement.deinit(alloc);
            replacement.stats.indexes[0] = .{
                .name = try alloc.dupe(u8, "vec-replacement"),
                .kind = .dense_vector,
                .doc_count = 99,
            };

            cache.upsertGroupStatus("docs", replacement) catch |err| switch (err) {
                error.OutOfMemory => {},
            };

            var docs = (try cache.snapshot(alloc, "docs")).?;
            defer docs.deinit(alloc);
            try std.testing.expectEqual(@as(usize, 1), docs.items.len);
            try std.testing.expectEqual(@as(u64, 11), docs.items[0].stats.doc_count);
            try std.testing.expectEqualStrings("vec", docs.items[0].stats.indexes[0].name);
            try std.testing.expectEqual(@as(u64, 5), docs.items[0].stats.indexes[0].replay_applied_sequence);
        }
    };

    try std.testing.checkAllAllocationFailures(std.testing.allocator, Runner.run, .{});
}

test "table runtime snapshot cache preserves previous snapshots when replace preserve install fails" {
    const Runner = struct {
        fn run(alloc: std.mem.Allocator) !void {
            var cache = TableRuntimeSnapshotCache.init(alloc);
            defer cache.deinit();

            const initial_docs_items = try alloc.alloc(LocalTableRuntimeStatus, 1);
            initial_docs_items[0] = .{
                .group_id = 7,
                .stats = .{
                    .doc_count = 11,
                    .index_count = 1,
                    .indexes = try alloc.alloc(db_mod.types.DBIndexStats, 1),
                },
            };
            initial_docs_items[0].stats.indexes[0] = .{
                .name = try alloc.dupe(u8, "vec"),
                .kind = .dense_vector,
                .doc_count = 11,
                .replay_applied_sequence = 5,
                .replay_target_sequence = 10,
                .replay_catch_up_required = true,
            };
            const initial_logs_items = try alloc.alloc(LocalTableRuntimeStatus, 1);
            initial_logs_items[0] = .{
                .group_id = 8,
                .stats = .{
                    .doc_count = 2,
                    .index_count = 1,
                    .indexes = try alloc.alloc(db_mod.types.DBIndexStats, 1),
                },
            };
            initial_logs_items[0].stats.indexes[0] = .{
                .name = try alloc.dupe(u8, "kw"),
                .kind = .full_text,
                .doc_count = 2,
            };
            const initial = try alloc.alloc(TableRuntimeSnapshot, 2);
            initial[0] = .{
                .table_name = try alloc.dupe(u8, "docs"),
                .statuses = .{ .items = initial_docs_items },
            };
            initial[1] = .{
                .table_name = try alloc.dupe(u8, "logs"),
                .statuses = .{ .items = initial_logs_items },
            };
            cache.replaceOwned(initial);

            const refresh_docs_items = try alloc.alloc(LocalTableRuntimeStatus, 1);
            refresh_docs_items[0] = .{
                .group_id = 7,
                .stats = .{
                    .doc_count = 99,
                    .index_count = 1,
                    .indexes = try alloc.alloc(db_mod.types.DBIndexStats, 1),
                },
            };
            refresh_docs_items[0].stats.indexes[0] = .{
                .name = try alloc.dupe(u8, "vec-new"),
                .kind = .dense_vector,
                .doc_count = 99,
            };
            const refresh = try alloc.alloc(TableRuntimeSnapshot, 1);
            refresh[0] = .{
                .table_name = try alloc.dupe(u8, "docs"),
                .statuses = .{ .items = refresh_docs_items },
            };

            cache.replaceOwnedPreservingGroupStatus(refresh, "docs", 7) catch |err| switch (err) {
                error.OutOfMemory => {},
            };

            var docs = (try cache.snapshot(alloc, "docs")).?;
            defer docs.deinit(alloc);
            try std.testing.expectEqual(@as(u64, 11), docs.items[0].stats.doc_count);
            try std.testing.expectEqualStrings("vec", docs.items[0].stats.indexes[0].name);
            try std.testing.expectEqual(@as(u64, 5), docs.items[0].stats.indexes[0].replay_applied_sequence);

            var logs = (try cache.snapshot(alloc, "logs")).?;
            defer logs.deinit(alloc);
            try std.testing.expectEqual(@as(u64, 2), logs.items[0].stats.doc_count);
            try std.testing.expectEqualStrings("kw", logs.items[0].stats.indexes[0].name);
        }
    };

    try std.testing.checkAllAllocationFailures(std.testing.allocator, Runner.run, .{});
}

test "table runtime snapshot cache summarizes replay debt" {
    var cache = TableRuntimeSnapshotCache.init(std.testing.allocator);
    defer cache.deinit();

    const docs_items = try std.testing.allocator.alloc(LocalTableRuntimeStatus, 2);
    docs_items[0] = .{
        .group_id = 7,
        .stats = .{
            .doc_count = 11,
            .index_count = 2,
            .indexes = try std.testing.allocator.alloc(db_mod.types.DBIndexStats, 2),
        },
    };
    docs_items[0].stats.indexes[0] = .{
        .name = try std.testing.allocator.dupe(u8, "vec"),
        .kind = .dense_vector,
        .replay_applied_sequence = 5,
        .replay_target_sequence = 8,
        .replay_catch_up_required = true,
    };
    docs_items[0].stats.indexes[1] = .{
        .name = try std.testing.allocator.dupe(u8, "text"),
        .kind = .full_text,
        .replay_applied_sequence = 3,
        .replay_target_sequence = 3,
    };
    docs_items[1] = .{
        .group_id = 8,
        .stats = .{
            .doc_count = 6,
            .index_count = 1,
            .indexes = try std.testing.allocator.alloc(db_mod.types.DBIndexStats, 1),
        },
    };
    docs_items[1].stats.indexes[0] = .{
        .name = try std.testing.allocator.dupe(u8, "graph"),
        .kind = .graph,
        .replay_applied_sequence = 1,
        .replay_target_sequence = 4,
    };

    const logs_items = try std.testing.allocator.alloc(LocalTableRuntimeStatus, 1);
    logs_items[0] = .{
        .group_id = 9,
        .stats = .{
            .doc_count = 2,
            .index_count = 1,
            .indexes = try std.testing.allocator.alloc(db_mod.types.DBIndexStats, 1),
        },
    };
    logs_items[0].stats.indexes[0] = .{
        .name = try std.testing.allocator.dupe(u8, "search"),
        .kind = .full_text,
        .replay_applied_sequence = 9,
        .replay_target_sequence = 9,
    };

    const snapshots = try std.testing.allocator.alloc(TableRuntimeSnapshot, 2);
    snapshots[0] = .{
        .table_name = try std.testing.allocator.dupe(u8, "docs"),
        .statuses = .{ .items = docs_items },
    };
    snapshots[1] = .{
        .table_name = try std.testing.allocator.dupe(u8, "logs"),
        .statuses = .{ .items = logs_items },
    };
    cache.replaceOwned(snapshots);

    const summary = cache.summary();
    try std.testing.expectEqual(@as(usize, 2), summary.table_count);
    try std.testing.expectEqual(@as(usize, 3), summary.group_count);
    try std.testing.expectEqual(@as(usize, 4), summary.index_count);
    try std.testing.expectEqual(@as(usize, 1), summary.tables_with_replay_debt);
    try std.testing.expectEqual(@as(usize, 2), summary.groups_with_replay_debt);
    try std.testing.expectEqual(@as(usize, 2), summary.indexes_with_replay_debt);
    try std.testing.expectEqual(@as(u64, 6), summary.outstanding_replay_sequences);
    try std.testing.expectEqual(@as(u64, 3), summary.max_index_replay_backlog);
}
