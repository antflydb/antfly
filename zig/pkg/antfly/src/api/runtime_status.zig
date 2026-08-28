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
const platform_time = @import("antfly_platform").time;
const db_mod = struct {
    pub const types = @import("../storage/db/types.zig");
};
const lsm_backend = @import("../storage/lsm_backend/mod.zig");

pub const RuntimeStatusSource = enum {
    unknown,
    synthetic_config,
    cached_snapshot,
    live_writer_publish,
    background_refresh,
    startup_catch_up,
    remote_store,
    rebuild_state_quarantine,
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
    // Internal ordering for concurrent observations of one live DB. This is
    // deliberately separate from metadata.status_generation, which identifies
    // externally published store snapshots.
    cache_observation_generation: u64 = 0,
    // Identifies the filesystem observation that produced disk_bytes. Disk
    // usage has a separate causal lifetime from DB/runtime facts: a cached or
    // startup status can still carry a freshly scanned, authoritative size.
    disk_observation_generation: u64 = 0,
    metadata: RuntimeStatusMetadata = .{},
    disk_bytes: u64 = 0,
    disk_bytes_known: bool = false,
    created_at_millis: u64 = 0,
    stats: db_mod.types.DBStats,
    lsm_storage_stats: ?LsmStorageStats = null,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        db_mod.types.freeDBStats(alloc, self.stats);
        self.* = undefined;
    }

    pub fn clone(self: *const @This(), alloc: std.mem.Allocator) !@This() {
        return .{
            .group_id = self.group_id,
            .cache_observation_generation = self.cache_observation_generation,
            .disk_observation_generation = self.disk_observation_generation,
            .metadata = self.metadata,
            .disk_bytes = self.disk_bytes,
            .disk_bytes_known = self.disk_bytes_known,
            .created_at_millis = self.created_at_millis,
            .stats = try cloneDBStats(alloc, self.stats),
            .lsm_storage_stats = self.lsm_storage_stats,
        };
    }

    pub fn withMetadataDefaults(self: *@This(), source: RuntimeStatusSource, now_ns: u64) void {
        self.metadata = self.metadata.withDefaults(source, now_ns);
    }
};

pub const LsmStorageStats = struct {
    maintenance: lsm_backend.Backend.MaintenanceStats = .{},
    write: lsm_backend.Backend.WriteStats = .{},
    maintenance_score: u64 = 0,
    maintenance_debt_hint: u64 = 0,
};

pub fn statusHasRuntimeFacts(status: LocalTableRuntimeStatus) bool {
    return switch (status.metadata.source) {
        .live_writer_publish, .background_refresh, .startup_catch_up, .remote_store, .rebuild_state_quarantine => true,
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
    text_merge: db_mod.types.TextMergeStats = .{},
    async_indexing: db_mod.types.AsyncIndexingStats = .{},
};

pub const TableRuntimeSnapshotCache = struct {
    pub const TableEpoch = struct {
        invalidation_epoch: u64,
        root_generation: u64,
    };

    pub const PublicationToken = struct {
        table_epoch: TableEpoch,
        observation_generation: u64,
    };

    pub const PublishResult = enum {
        published,
        stale_table,
        stale_observation,
    };

    pub const CatalogToken = struct {
        alloc: std.mem.Allocator,
        topology_revision: u64,
        complete_catalog: bool,
        observation_generation: u64,
        table_epochs: std.StringHashMapUnmanaged(TableEpoch) = .empty,

        pub fn deinit(self: *@This()) void {
            var it = self.table_epochs.keyIterator();
            while (it.next()) |name| self.alloc.free(@constCast(name.*));
            self.table_epochs.deinit(self.alloc);
            self.* = undefined;
        }
    };

    pub const RefreshResult = struct {
        alloc: std.mem.Allocator,
        published_tables: usize = 0,
        removed_tables: usize = 0,
        removals_deferred: bool = false,
        rejected_tables: std.ArrayListUnmanaged([]u8) = .empty,

        pub fn deinit(self: *@This()) void {
            for (self.rejected_tables.items) |name| self.alloc.free(name);
            self.rejected_tables.deinit(self.alloc);
            self.* = undefined;
        }

        pub fn hasRejectedTables(self: *const @This()) bool {
            return self.rejected_tables.items.len != 0;
        }
    };

    const TableState = struct {
        epoch: TableEpoch,
        groups: std.AutoHashMapUnmanaged(u64, LocalTableRuntimeStatus) = .empty,

        fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
            var it = self.groups.valueIterator();
            while (it.next()) |status| status.deinit(alloc);
            self.groups.deinit(alloc);
            self.* = undefined;
        }
    };

    alloc: std.mem.Allocator,
    mutex: std.atomic.Mutex = .unlocked,
    topology_revision: u64 = 1,
    next_invalidation_epoch: u64 = 1,
    next_observation_generation: u64 = 1,
    tables: std.StringHashMapUnmanaged(TableState) = .empty,

    pub fn init(alloc: std.mem.Allocator) @This() {
        return .{ .alloc = alloc };
    }

    pub fn deinit(self: *@This()) void {
        lockAtomic(&self.mutex);
        self.clearTablesLocked();
        self.tables.deinit(self.alloc);
        self.mutex.unlock();
        self.* = undefined;
    }

    pub fn clear(self: *@This()) void {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        self.advanceInvalidationEpochLocked();
        self.advanceTopologyRevisionLocked();
        self.clearTablesLocked();
    }

    pub fn invalidateTable(self: *@This(), table_name: []const u8) void {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        self.advanceInvalidationEpochLocked();
        self.advanceTopologyRevisionLocked();

        const state = self.ensureTableLocked(table_name) catch {
            // Invalidation is a correctness boundary. If recording its table
            // tombstone fails, clear all states so no old token can match.
            self.clearTablesLocked();
            self.advanceInvalidationEpochLocked();
            return;
        };
        self.clearGroupsLocked(state);
        state.epoch.invalidation_epoch = self.next_invalidation_epoch;
        state.epoch.root_generation +%= 1;
        if (state.epoch.root_generation == 0) state.epoch.root_generation = 1;
    }

    /// Captures the table lifecycle before a DB is opened or inspected.
    pub fn capturePublicationToken(self: *@This(), table_name: []const u8) !PublicationToken {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        const state = try self.ensureTableLocked(table_name);
        return .{
            .table_epoch = state.epoch,
            .observation_generation = self.takeObservationGenerationLocked(),
        };
    }

    /// Captures all catalog tables in one lock acquisition before refresh DB
    /// inspection begins. `table_names` need only live for this call.
    pub fn captureCatalogToken(
        self: *@This(),
        alloc: std.mem.Allocator,
        table_names: []const []const u8,
        complete_catalog: bool,
    ) !CatalogToken {
        var token: CatalogToken = .{
            .alloc = alloc,
            .topology_revision = 0,
            .complete_catalog = complete_catalog,
            .observation_generation = 0,
        };
        errdefer token.deinit();

        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        token.topology_revision = self.topology_revision;
        token.observation_generation = self.takeObservationGenerationLocked();
        // A complete refresh is also authoritative for tables absent from the
        // catalog. Capture cached epochs as removal candidates so publication
        // can prove that an unseen table was not invalidated or recreated
        // while the catalog snapshot was being inspected.
        const cached_table_count = if (complete_catalog) self.tables.count() else 0;
        try token.table_epochs.ensureTotalCapacity(alloc, @intCast(cached_table_count + table_names.len));
        if (complete_catalog) {
            var cached_it = self.tables.iterator();
            while (cached_it.next()) |entry| {
                const owned_name = try alloc.dupe(u8, entry.key_ptr.*);
                token.table_epochs.putAssumeCapacityNoClobber(owned_name, entry.value_ptr.epoch);
            }
        }
        for (table_names) |table_name| {
            const state = try self.ensureTableLocked(table_name);
            if (token.table_epochs.contains(table_name)) continue;
            const owned_name = try alloc.dupe(u8, table_name);
            token.table_epochs.putAssumeCapacityNoClobber(owned_name, state.epoch);
        }
        return token;
    }

    /// Publishes one owned observation in O(1). The status is cloned before
    /// locking so DBStats ownership never crosses the caller/cache boundary.
    pub fn publishGroup(
        self: *@This(),
        token: PublicationToken,
        table_name: []const u8,
        status: LocalTableRuntimeStatus,
    ) !PublishResult {
        var owned = try status.clone(self.alloc);
        errdefer owned.deinit(self.alloc);
        owned.cache_observation_generation = token.observation_generation;
        owned.withMetadataDefaults(.live_writer_publish, platform_time.monotonicNs());

        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        const state = self.tables.getPtr(table_name) orelse {
            owned.deinit(self.alloc);
            return .stale_table;
        };
        if (!std.meta.eql(state.epoch, token.table_epoch)) {
            owned.deinit(self.alloc);
            return .stale_table;
        }
        if (state.groups.getPtr(status.group_id)) |previous| {
            if (previous.cache_observation_generation > token.observation_generation) {
                owned.deinit(self.alloc);
                return .stale_observation;
            }
            preserveArtifactVisibilityOnReplayRegression(previous.*, &owned);
            previous.deinit(self.alloc);
            previous.* = owned;
        } else {
            try state.groups.put(self.alloc, status.group_id, owned);
        }
        return .published;
    }

    /// Publishes a bounded set of owned observations under one table-epoch
    /// decision and one cache lock. Statuses are cloned before locking so
    /// allocation and DBStats ownership do not lengthen the critical section.
    /// A newer observation for one group is preserved without rejecting valid
    /// observations for the other groups.
    pub fn publishGroups(
        self: *@This(),
        token: PublicationToken,
        table_name: []const u8,
        statuses: []const LocalTableRuntimeStatus,
    ) !PublishResult {
        for (statuses, 0..) |status, index| {
            for (statuses[0..index]) |previous| {
                if (previous.group_id == status.group_id) return error.DuplicateRuntimeStatusGroup;
            }
        }

        const owned = try self.alloc.alloc(LocalTableRuntimeStatus, statuses.len);
        var initialized: usize = 0;
        var clean_all = true;
        defer {
            if (clean_all) {
                for (owned[0..initialized]) |*status| status.deinit(self.alloc);
            }
            self.alloc.free(owned);
        }
        for (statuses, 0..) |status, index| {
            owned[index] = try status.clone(self.alloc);
            initialized += 1;
        }

        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        const state = self.tables.getPtr(table_name) orelse return .stale_table;
        if (!std.meta.eql(state.epoch, token.table_epoch)) return .stale_table;

        var new_groups: usize = 0;
        for (statuses) |status| {
            if (!state.groups.contains(status.group_id)) new_groups += 1;
        }
        try state.groups.ensureUnusedCapacity(self.alloc, @intCast(new_groups));

        const now_ns = platform_time.monotonicNs();
        var published = false;
        clean_all = false;
        for (owned, statuses) |*next, status| {
            next.cache_observation_generation = token.observation_generation;
            next.withMetadataDefaults(.live_writer_publish, now_ns);
            if (state.groups.getPtr(status.group_id)) |previous| {
                if (previous.cache_observation_generation > token.observation_generation) {
                    next.deinit(self.alloc);
                    continue;
                }
                preserveArtifactVisibilityOnReplayRegression(previous.*, next);
                previous.deinit(self.alloc);
                previous.* = next.*;
            } else {
                state.groups.putAssumeCapacity(status.group_id, next.*);
            }
            next.* = undefined;
            published = true;
        }
        return if (published or statuses.len == 0) .published else .stale_observation;
    }

    /// Atomically replaces a table's visible observations while advancing its
    /// lifecycle epoch. Use this when a durable structural transition makes
    /// every observation from the preceding epoch unsafe to republish.
    pub fn publishLifecycleTransition(
        self: *@This(),
        token: PublicationToken,
        table_name: []const u8,
        statuses: []const LocalTableRuntimeStatus,
    ) !PublishResult {
        for (statuses, 0..) |status, index| {
            for (statuses[0..index]) |previous| {
                if (previous.group_id == status.group_id) return error.DuplicateRuntimeStatusGroup;
            }
        }

        var replacement = std.AutoHashMapUnmanaged(u64, LocalTableRuntimeStatus).empty;
        var replacement_owned = true;
        defer if (replacement_owned) {
            var it = replacement.valueIterator();
            while (it.next()) |status| status.deinit(self.alloc);
            replacement.deinit(self.alloc);
        };
        try replacement.ensureTotalCapacity(self.alloc, @intCast(statuses.len));
        for (statuses) |status| {
            const owned = try status.clone(self.alloc);
            replacement.putAssumeCapacityNoClobber(status.group_id, owned);
        }

        lockAtomic(&self.mutex);
        const state = self.tables.getPtr(table_name) orelse {
            self.mutex.unlock();
            return .stale_table;
        };
        if (!std.meta.eql(state.epoch, token.table_epoch)) {
            self.mutex.unlock();
            return .stale_table;
        }

        const observation_generation = self.takeObservationGenerationLocked();
        const now_ns = platform_time.monotonicNs();
        var replacement_it = replacement.valueIterator();
        while (replacement_it.next()) |status| {
            status.cache_observation_generation = observation_generation;
            status.withMetadataDefaults(.live_writer_publish, now_ns);
        }

        self.advanceInvalidationEpochLocked();
        self.advanceTopologyRevisionLocked();
        state.epoch.invalidation_epoch = self.next_invalidation_epoch;
        var retired = state.groups;
        state.groups = replacement;
        replacement = .empty;
        replacement_owned = false;
        self.mutex.unlock();

        var retired_it = retired.valueIterator();
        while (retired_it.next()) |status| status.deinit(self.alloc);
        retired.deinit(self.alloc);
        return .published;
    }

    /// Consumes every snapshot. Epoch-valid tables publish independently;
    /// catalog-wide absence removals occur only when topology stayed stable.
    pub fn publishRefresh(
        self: *@This(),
        catalog_token: *const CatalogToken,
        snapshots: []TableRuntimeSnapshot,
    ) !RefreshResult {
        var result: RefreshResult = .{ .alloc = self.alloc };
        errdefer result.deinit();
        var next_unconsumed: usize = 0;
        defer {
            for (snapshots[next_unconsumed..]) |*snapshot_entry| snapshot_entry.deinit(self.alloc);
        }

        var seen_tables = std.StringHashMapUnmanaged(void).empty;
        defer {
            var seen_it = seen_tables.keyIterator();
            while (seen_it.next()) |name| self.alloc.free(@constCast(name.*));
            seen_tables.deinit(self.alloc);
        }
        try seen_tables.ensureTotalCapacity(self.alloc, @intCast(snapshots.len));
        for (snapshots) |snapshot_entry| {
            if (seen_tables.contains(snapshot_entry.table_name)) return error.DuplicateRuntimeStatusTable;
            const owned_name = try self.alloc.dupe(u8, snapshot_entry.table_name);
            seen_tables.putAssumeCapacityNoClobber(owned_name, {});
        }

        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        const now_ns = platform_time.monotonicNs();

        for (snapshots) |*snapshot_entry| {
            const expected_epoch = catalog_token.table_epochs.get(snapshot_entry.table_name);
            const state = self.tables.getPtr(snapshot_entry.table_name);
            if (expected_epoch == null or state == null or !std.meta.eql(expected_epoch.?, state.?.epoch)) {
                const rejected_name = try self.alloc.dupe(u8, snapshot_entry.table_name);
                errdefer self.alloc.free(rejected_name);
                try result.rejected_tables.append(self.alloc, rejected_name);
                snapshot_entry.deinit(self.alloc);
                next_unconsumed += 1;
                continue;
            }

            try self.publishTableRefreshLocked(
                state.?,
                &snapshot_entry.statuses,
                catalog_token.observation_generation,
                now_ns,
            );
            snapshot_entry.deinit(self.alloc);
            next_unconsumed += 1;
            result.published_tables += 1;
        }

        if (!catalog_token.complete_catalog) return result;
        if (catalog_token.topology_revision != self.topology_revision) {
            result.removals_deferred = true;
            return result;
        }

        var advanced_invalidation_epoch = false;
        var it = self.tables.iterator();
        while (it.next()) |entry| {
            if (seen_tables.contains(entry.key_ptr.*)) continue;
            const expected_epoch = catalog_token.table_epochs.get(entry.key_ptr.*) orelse continue;
            if (!std.meta.eql(expected_epoch, entry.value_ptr.epoch)) continue;
            if (!advanced_invalidation_epoch) {
                self.advanceInvalidationEpochLocked();
                advanced_invalidation_epoch = true;
            }
            self.alloc.free(@constCast(entry.key_ptr.*));
            entry.value_ptr.deinit(self.alloc);
            self.tables.removeByPtr(entry.key_ptr);
            result.removed_tables += 1;
        }
        return result;
    }

    pub fn snapshot(self: *@This(), alloc: std.mem.Allocator, table_name: []const u8) !?LocalTableRuntimeStatuses {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        const state = self.tables.getPtr(table_name) orelse return null;
        if (state.groups.count() == 0) return null;
        const items = try alloc.alloc(LocalTableRuntimeStatus, state.groups.count());
        var initialized: usize = 0;
        errdefer {
            for (items[0..initialized]) |*status| status.deinit(alloc);
            alloc.free(items);
        }
        var it = state.groups.valueIterator();
        while (it.next()) |status| : (initialized += 1) items[initialized] = try status.clone(alloc);
        // Runtime status embeds the full DB/LSM statistics payload and can be
        // wider than std.mem.sort's SIMD element-size limit. Group counts are
        // small, so insertion sort avoids that compile-time size constraint
        // without allocating or changing the stable group-id ordering.
        std.sort.insertion(LocalTableRuntimeStatus, items, {}, lessThanGroupId);
        return .{ .items = items };
    }

    pub fn snapshotGroupStatus(
        self: *@This(),
        alloc: std.mem.Allocator,
        table_name: []const u8,
        group_id: u64,
    ) !?LocalTableRuntimeStatus {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        const state = self.tables.getPtr(table_name) orelse return null;
        const status = state.groups.getPtr(group_id) orelse return null;
        return try status.clone(alloc);
    }

    fn mergeRefreshStatusLocked(
        self: *@This(),
        previous: ?*LocalTableRuntimeStatus,
        status: *LocalTableRuntimeStatus,
        now_ns: u64,
    ) !void {
        const cached = previous orelse return;
        if (cached.cache_observation_generation > status.cache_observation_generation) {
            const cloned = try cached.clone(self.alloc);
            status.deinit(self.alloc);
            status.* = cloned;
            return;
        }
        if (status.metadata.source == .synthetic_config and runtimeStatusWorthPreserving(cached.*)) {
            const merged = try mergeCachedStatusWithSyntheticPlaceholder(self.alloc, cached.*, status.*, now_ns);
            status.deinit(self.alloc);
            status.* = merged;
            return;
        }
        preserveArtifactVisibilityOnReplayRegression(cached.*, status);
    }

    pub fn summary(self: *@This()) TableRuntimeSummary {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();

        var result: TableRuntimeSummary = .{};
        var table_it = self.tables.valueIterator();
        while (table_it.next()) |entry| {
            if (entry.groups.count() == 0) continue;
            result.table_count += 1;
            var table_has_replay_debt = false;
            var group_it = entry.groups.valueIterator();
            while (group_it.next()) |status| {
                result.group_count += 1;
                db_mod.types.accumulateTextMergeStats(&result.text_merge, status.stats.text_merge);
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

    fn publishTableRefreshLocked(
        self: *@This(),
        state: *TableState,
        statuses: *LocalTableRuntimeStatuses,
        observation_generation: u64,
        now_ns: u64,
    ) !void {
        var replacement = std.AutoHashMapUnmanaged(u64, LocalTableRuntimeStatus).empty;
        errdefer {
            var it = replacement.valueIterator();
            while (it.next()) |status| status.deinit(self.alloc);
            replacement.deinit(self.alloc);
        }
        try replacement.ensureTotalCapacity(self.alloc, @intCast(statuses.items.len));

        const source_items = statuses.items;
        var moved: usize = 0;
        defer {
            for (source_items[moved..]) |*status| status.deinit(self.alloc);
            if (source_items.len > 0) self.alloc.free(source_items);
            statuses.items = &.{};
        }
        for (source_items) |*source_status| {
            var owned = source_status.*;
            source_status.* = undefined;
            moved += 1;
            owned.cache_observation_generation = observation_generation;
            owned.withMetadataDefaults(.background_refresh, now_ns);
            owned = try self.prepareRefreshStatusLocked(state.groups.getPtr(owned.group_id), owned, now_ns);
            if (replacement.getPtr(owned.group_id)) |duplicate| {
                duplicate.deinit(self.alloc);
                duplicate.* = owned;
            } else {
                replacement.putAssumeCapacity(owned.group_id, owned);
            }
        }

        var old_groups = state.groups;
        state.groups = replacement;
        var old_it = old_groups.valueIterator();
        while (old_it.next()) |status| status.deinit(self.alloc);
        old_groups.deinit(self.alloc);
    }

    fn prepareRefreshStatusLocked(
        self: *@This(),
        previous: ?*LocalTableRuntimeStatus,
        incoming: LocalTableRuntimeStatus,
        now_ns: u64,
    ) !LocalTableRuntimeStatus {
        var owned = incoming;
        errdefer owned.deinit(self.alloc);
        try self.mergeRefreshStatusLocked(previous, &owned, now_ns);
        return owned;
    }

    fn ensureTableLocked(self: *@This(), table_name: []const u8) !*TableState {
        if (self.tables.getPtr(table_name)) |state| return state;
        const owned_name = try self.alloc.dupe(u8, table_name);
        errdefer self.alloc.free(owned_name);
        try self.tables.put(self.alloc, owned_name, .{
            .epoch = .{
                .invalidation_epoch = self.next_invalidation_epoch,
                .root_generation = 0,
            },
        });
        return self.tables.getPtr(owned_name).?;
    }

    fn clearGroupsLocked(self: *@This(), state: *TableState) void {
        var it = state.groups.valueIterator();
        while (it.next()) |status| status.deinit(self.alloc);
        state.groups.clearRetainingCapacity();
    }

    fn clearTablesLocked(self: *@This()) void {
        var it = self.tables.iterator();
        while (it.next()) |entry| {
            self.alloc.free(@constCast(entry.key_ptr.*));
            entry.value_ptr.deinit(self.alloc);
        }
        self.tables.clearRetainingCapacity();
    }

    fn advanceInvalidationEpochLocked(self: *@This()) void {
        self.next_invalidation_epoch +%= 1;
        if (self.next_invalidation_epoch == 0) self.next_invalidation_epoch = 1;
    }

    fn advanceTopologyRevisionLocked(self: *@This()) void {
        self.topology_revision +%= 1;
        if (self.topology_revision == 0) self.topology_revision = 1;
    }

    fn takeObservationGenerationLocked(self: *@This()) u64 {
        const generation = self.next_observation_generation;
        self.next_observation_generation +%= 1;
        if (self.next_observation_generation == 0) self.next_observation_generation = 1;
        return generation;
    }
};

fn lessThanGroupId(_: void, lhs: LocalTableRuntimeStatus, rhs: LocalTableRuntimeStatus) bool {
    return lhs.group_id < rhs.group_id;
}

fn preserveArtifactVisibilityOnReplayRegression(previous: LocalTableRuntimeStatus, incoming: *LocalTableRuntimeStatus) void {
    var preserved_visibility = false;
    for (incoming.stats.indexes) |*dst| {
        const cached = findMatchingIndexStatus(previous.stats.indexes, dst.name, dst.kind) orelse continue;
        const applied_regressed = dst.replay_applied_sequence < cached.replay_applied_sequence;
        const target_not_older = dst.replay_target_sequence >= cached.replay_target_sequence;
        const same_projection_config = if (dst.projection_checkpoint_config_hash != 0 and
            cached.projection_checkpoint_config_hash != 0)
            dst.projection_checkpoint_config_hash == cached.projection_checkpoint_config_hash
        else
            dst.coverage_config_hash != 0 and
                dst.coverage_config_hash == cached.coverage_config_hash;
        const same_projection = same_projection_config and
            dst.projection_checkpoint_generation <= cached.projection_checkpoint_generation;
        const projection_regressed = same_projection and
            dst.projection_checkpoint_applied_sequence < cached.projection_checkpoint_applied_sequence;
        const cached_has_visibility = indexHasArtifactVisibilityFacts(cached);
        const dst_has_visibility = indexHasArtifactVisibilityFacts(dst.*);
        const visibility_regressed_without_newer_replay = target_not_older and
            cached_has_visibility and
            !dst_has_visibility and
            dst.replay_applied_sequence <= cached.replay_applied_sequence;
        if (!applied_regressed and !projection_regressed and !visibility_regressed_without_newer_replay) continue;

        preserveIndexArtifactVisibility(dst, cached);
        if (projection_regressed) preserveIndexProjectionLifecycle(dst, cached);
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
    // A live writer's source cardinality is authoritative and can legitimately
    // fall after deletes (including TTL cleanup). Only background projections
    // need the anti-regression guard for source visibility.
    if (preserved_visibility and
        incoming.metadata.source != .live_writer_publish and
        incoming.stats.source_doc_count < previous.stats.source_doc_count)
    {
        incoming.stats.source_doc_count = previous.stats.source_doc_count;
    }
}

fn preserveIndexProjectionLifecycle(dst: *db_mod.types.DBIndexStats, cached: db_mod.types.DBIndexStats) void {
    dst.coverage_produced_count = cached.coverage_produced_count;
    dst.coverage_skipped_count = cached.coverage_skipped_count;
    dst.coverage_terminal_failed_count = cached.coverage_terminal_failed_count;
    dst.coverage_config_hash = cached.coverage_config_hash;
    dst.coverage_summary_ready = cached.coverage_summary_ready;
    dst.coverage_generation = cached.coverage_generation;
    dst.coverage_identity_ready = cached.coverage_identity_ready;
    dst.backfill_active = cached.backfill_active;
    dst.backfill_progress = cached.backfill_progress;
    dst.enrichment_failed = cached.enrichment_failed;
    dst.projection_checkpoint_status = cached.projection_checkpoint_status;
    dst.projection_checkpoint_applied_sequence = cached.projection_checkpoint_applied_sequence;
    dst.projection_checkpoint_generation = cached.projection_checkpoint_generation;
    dst.projection_checkpoint_config_hash = cached.projection_checkpoint_config_hash;
    dst.checkpoint_replay_tail_sequence_count = cached.checkpoint_replay_tail_sequence_count;
    dst.dense_vector_projection_pending = cached.dense_vector_projection_pending;
}

fn runtimeStatusWorthPreserving(status: LocalTableRuntimeStatus) bool {
    if (statusHasRuntimeFacts(status)) return true;
    return false;
}

fn statusStatsHaveRuntimeFacts(stats: db_mod.types.DBStats) bool {
    if (stats.doc_count > 0) return true;
    if (stats.repair_degraded or stats.repair_issue_count != 0) return true;
    if (docIdentityStatsHaveRuntimeFacts(stats.doc_identity)) return true;
    if (docSetPlanningStatsHaveRuntimeFacts(stats.doc_set_planning)) return true;
    if (stats.async_indexing.startup.active or stats.async_indexing.dense_catch_up.active) return true;
    if (stats.enrichment.enabled and (stats.enrichment.processed_requests > 0 or stats.enrichment.applied_sequence > 0 or stats.enrichment.target_sequence > 0 or stats.enrichment.retrying or stats.enrichment.worker_failed)) return true;
    if (stats.text_merge.pending_segments > 0 or stats.text_merge.in_flight_merges > 0 or stats.text_merge.completed_merges > 0 or stats.text_merge.failed_merges > 0) return true;
    for (stats.indexes) |index| {
        if (indexHasArtifactVisibilityFacts(index)) return true;
        if (index.repair_degraded or index.repair_issue_count != 0) return true;
        if (index.dense_vector_projection_pending) return true;
        if (index.backfill_active or index.catch_up_active or index.replay_catch_up_required) return true;
        // A target-only replay/catch-up marker can be synthesized from topology
        // and accepted sequence. It is not enough to prove that a live runtime
        // has ever published concrete index state.
    }
    return false;
}

fn docIdentityStatsHaveRuntimeFacts(stats: db_mod.types.DocIdentityStats) bool {
    return stats.namespace_table_id != 0 or
        stats.namespace_shard_id != 0 or
        stats.namespace_range_id != 0 or
        stats.next_ordinal != 1 or
        stats.allocated_ordinals != 0 or
        stats.ordinal_capacity_remaining != 0 or
        stats.ordinal_capacity_exhausted or
        stats.rebuild_required or
        stats.state_rows != 0 or
        stats.live_ordinals != 0 or
        stats.tombstone_ordinals != 0 or
        stats.min_created_generation != 0 or
        stats.max_created_generation != 0 or
        stats.min_deleted_generation != 0 or
        stats.max_deleted_generation != 0 or
        stats.scanned_primary_docs != 0 or
        stats.primary_docs_missing_ordinals != 0 or
        stats.primary_docs_missing_identity_state != 0 or
        stats.primary_docs_with_tombstone_ordinals != 0;
}

fn docSetPlanningStatsHaveRuntimeFacts(stats: db_mod.types.DocSetPlanningStats) bool {
    return stats.resolved_set_count != 0 or
        stats.all_set_count != 0 or
        stats.none_set_count != 0 or
        stats.doc_key_list_count != 0 or
        stats.ordinal_list_count != 0 or
        stats.ordinal_bitmap_count != 0 or
        stats.doc_key_list_docs != 0 or
        stats.ordinal_list_docs != 0 or
        stats.ordinal_bitmap_docs != 0 or
        stats.missing_ordinal_coverage_count != 0 or
        stats.bitmap_promotion_count != 0 or
        stats.unsupported_filter_shape_count != 0 or
        stats.stale_identity_generation_rejection_count != 0;
}

fn indexHasArtifactVisibilityFacts(index: db_mod.types.DBIndexStats) bool {
    return index.doc_count > 0 or
        index.term_count > 0 or
        index.edge_count > 0 or
        index.node_count > 0 or
        index.root_node > 0 or
        index.coverage_config_hash != 0;
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

    merged.stats.storage_change_token = previous.stats.storage_change_token;
    merged.stats.source_doc_count = previous.stats.source_doc_count;
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

fn cloneResolverReplayDiagnostics(alloc: std.mem.Allocator, stats: db_mod.types.ResolverReplayDiagnostics) !db_mod.types.ResolverReplayDiagnostics {
    var resolvers = try alloc.alloc(db_mod.types.ResolverReplayDiagnostic, stats.resolvers.len);
    var initialized: usize = 0;
    errdefer {
        for (resolvers[0..initialized]) |resolver| {
            alloc.free(resolver.name);
            alloc.free(resolver.table);
            alloc.free(resolver.source_artifact);
            alloc.free(resolver.resolution_artifact);
        }
        if (resolvers.len > 0) alloc.free(resolvers);
    }

    for (stats.resolvers, 0..) |resolver, i| {
        const name = try alloc.dupe(u8, resolver.name);
        errdefer alloc.free(name);
        const table = try alloc.dupe(u8, resolver.table);
        errdefer alloc.free(table);
        const source_artifact = try alloc.dupe(u8, resolver.source_artifact);
        errdefer alloc.free(source_artifact);
        const resolution_artifact = try alloc.dupe(u8, resolver.resolution_artifact);
        errdefer alloc.free(resolution_artifact);
        resolvers[i] = .{
            .name = name,
            .table = table,
            .source_artifact = source_artifact,
            .resolution_artifact = resolution_artifact,
        };
        initialized += 1;
    }

    return .{
        .resolver_count = stats.resolver_count,
        .resolution_runtime_present = stats.resolution_runtime_present,
        .resolution_worker_started = stats.resolution_worker_started,
        .promotion_runtime_present = stats.promotion_runtime_present,
        .promotion_worker_started = stats.promotion_worker_started,
        .resolvers = resolvers,
    };
}

pub fn cloneDBStats(alloc: std.mem.Allocator, stats: db_mod.types.DBStats) !db_mod.types.DBStats {
    const resolver_replay = try cloneResolverReplayDiagnostics(alloc, stats.resolver_replay);
    errdefer db_mod.types.freeResolverReplayDiagnostics(alloc, resolver_replay);
    const indexes = try alloc.alloc(db_mod.types.DBIndexStats, stats.indexes.len);
    var initialized: usize = 0;
    errdefer {
        for (indexes[0..initialized]) |item| {
            alloc.free(item.name);
            if (item.load_error) |value| alloc.free(value);
            if (item.index_repair_last_error) |value| alloc.free(value);
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
            for (item.algebraic_candidate_decision_history) |entry| {
                alloc.free(entry.recommendation);
                alloc.free(entry.materialization_id);
                alloc.free(entry.lifecycle);
                alloc.free(entry.previous_decision);
                alloc.free(entry.decision);
            }
            if (item.algebraic_candidate_decision_history.len > 0) alloc.free(item.algebraic_candidate_decision_history);
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
        const load_error = if (item.load_error) |value|
            try alloc.dupe(u8, value)
        else
            null;
        errdefer if (load_error) |value| alloc.free(value);
        const index_repair_last_error = if (item.index_repair_last_error) |value|
            try alloc.dupe(u8, value)
        else
            null;
        errdefer if (index_repair_last_error) |value| alloc.free(value);
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
            .load_error = load_error,
            .doc_count = item.doc_count,
            .term_count = item.term_count,
            .edge_count = item.edge_count,
            .node_count = item.node_count,
            .root_node = item.root_node,
            .coverage_produced_count = item.coverage_produced_count,
            .coverage_skipped_count = item.coverage_skipped_count,
            .coverage_terminal_failed_count = item.coverage_terminal_failed_count,
            .coverage_config_hash = item.coverage_config_hash,
            .coverage_summary_ready = item.coverage_summary_ready,
            .coverage_generation = item.coverage_generation,
            .coverage_identity_ready = item.coverage_identity_ready,
            .backfill_active = item.backfill_active,
            .backfill_progress = item.backfill_progress,
            .enrichment_failed = item.enrichment_failed,
            .repair_degraded = item.repair_degraded,
            .repair_issue_count = item.repair_issue_count,
            .repair_summary_ready = item.repair_summary_ready,
            .repair_issue_count_estimated = item.repair_issue_count_estimated,
            .repair_scan_issue_count = item.repair_scan_issue_count,
            .index_repair_id = item.index_repair_id,
            .index_repair_trigger = item.index_repair_trigger,
            .index_repair_phase = item.index_repair_phase,
            .index_repair_automation = item.index_repair_automation,
            .index_repair_attempts = item.index_repair_attempts,
            .index_repair_started_at_ms = item.index_repair_started_at_ms,
            .index_repair_updated_at_ms = item.index_repair_updated_at_ms,
            .index_repair_build_floor_sequence = item.index_repair_build_floor_sequence,
            .index_repair_applied_sequence = item.index_repair_applied_sequence,
            .index_repair_target_sequence = item.index_repair_target_sequence,
            .index_repair_next_retry_at_ms = item.index_repair_next_retry_at_ms,
            .index_repair_last_error = index_repair_last_error,
            .index_repair_wait_reason = item.index_repair_wait_reason,
            .index_repair_status = item.index_repair_status,
            .index_repair_active_generation_serviceable = item.index_repair_active_generation_serviceable,
            .projection_checkpoint_status = item.projection_checkpoint_status,
            .projection_checkpoint_applied_sequence = item.projection_checkpoint_applied_sequence,
            .projection_checkpoint_generation = item.projection_checkpoint_generation,
            .projection_checkpoint_config_hash = item.projection_checkpoint_config_hash,
            .replay_applied_sequence = item.replay_applied_sequence,
            .replay_target_sequence = item.replay_target_sequence,
            .checkpoint_replay_tail_sequence_count = item.checkpoint_replay_tail_sequence_count,
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
        .storage_change_token = stats.storage_change_token,
        .source_doc_count = stats.source_doc_count,
        .doc_count = stats.doc_count,
        .index_count = stats.index_count,
        .indexes = indexes,
        .repair_degraded = stats.repair_degraded,
        .repair_issue_count = stats.repair_issue_count,
        .repair_summary_ready = stats.repair_summary_ready,
        .repair_issue_count_estimated = stats.repair_issue_count_estimated,
        .doc_identity = stats.doc_identity,
        .doc_set_planning = stats.doc_set_planning,
        .enrichment = stats.enrichment,
        .resolution = stats.resolution,
        .promotion = stats.promotion,
        .resolver_replay = resolver_replay,
        .ttl_cleanup = stats.ttl_cleanup,
        .transaction_recovery = stats.transaction_recovery,
        .text_merge = stats.text_merge,
        .term_doc_freq_cache_hits = stats.term_doc_freq_cache_hits,
        .term_doc_freq_cache_misses = stats.term_doc_freq_cache_misses,
        .async_indexing = stats.async_indexing,
    };
}

fn publishGroupForTest(
    cache: *TableRuntimeSnapshotCache,
    table_name: []const u8,
    status: LocalTableRuntimeStatus,
) !TableRuntimeSnapshotCache.PublishResult {
    const token = try cache.capturePublicationToken(table_name);
    return try cache.publishGroup(token, table_name, status);
}

fn publishRefreshForTest(
    cache: *TableRuntimeSnapshotCache,
    snapshots: []TableRuntimeSnapshot,
) !void {
    var ownership_transferred = false;
    errdefer if (!ownership_transferred) {
        for (snapshots) |*snapshot_entry| snapshot_entry.deinit(cache.alloc);
    };
    const names = try cache.alloc.alloc([]const u8, snapshots.len);
    defer cache.alloc.free(names);
    for (snapshots, 0..) |snapshot_entry, i| names[i] = snapshot_entry.table_name;
    var token = try cache.captureCatalogToken(cache.alloc, names, true);
    defer token.deinit();
    ownership_transferred = true;
    var result = try cache.publishRefresh(&token, snapshots);
    defer result.deinit();
    try std.testing.expect(!result.hasRejectedTables());
}

test "runtime status cache rejects refresh captured before invalidation" {
    const alloc = std.testing.allocator;
    var cache = TableRuntimeSnapshotCache.init(alloc);
    defer cache.deinit();

    const stale_names = [_][]const u8{"docs"};
    var stale_token = try cache.captureCatalogToken(alloc, &stale_names, true);
    defer stale_token.deinit();
    cache.invalidateTable("docs");

    const stale_statuses = try alloc.alloc(LocalTableRuntimeStatus, 1);
    stale_statuses[0] = .{
        .group_id = 7,
        .stats = .{ .repair_degraded = true },
    };
    const stale_snapshots = try alloc.alloc(TableRuntimeSnapshot, 1);
    defer alloc.free(stale_snapshots);
    stale_snapshots[0] = .{
        .table_name = try alloc.dupe(u8, "docs"),
        .statuses = .{ .items = stale_statuses },
    };
    var stale_result = try cache.publishRefresh(&stale_token, stale_snapshots);
    defer stale_result.deinit();
    try std.testing.expectEqual(@as(usize, 1), stale_result.rejected_tables.items.len);
    try std.testing.expect((try cache.snapshot(alloc, "docs")) == null);

    const clean_statuses = try alloc.alloc(LocalTableRuntimeStatus, 1);
    clean_statuses[0] = .{ .group_id = 7, .stats = .{} };
    const clean_snapshots = try alloc.alloc(TableRuntimeSnapshot, 1);
    defer alloc.free(clean_snapshots);
    clean_snapshots[0] = .{
        .table_name = try alloc.dupe(u8, "docs"),
        .statuses = .{ .items = clean_statuses },
    };
    try publishRefreshForTest(&cache, clean_snapshots);

    var published = (try cache.snapshot(alloc, "docs")).?;
    defer published.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), published.items.len);
    try std.testing.expect(!published.items[0].stats.repair_degraded);
}

test "runtime status cache publishes unaffected tables and retries only invalidated tables" {
    const alloc = std.testing.allocator;
    var cache = TableRuntimeSnapshotCache.init(alloc);
    defer cache.deinit();

    const table_names = [_][]const u8{ "docs", "logs" };
    var token = try cache.captureCatalogToken(alloc, &table_names, true);
    defer token.deinit();
    cache.invalidateTable("docs");

    const snapshots = try alloc.alloc(TableRuntimeSnapshot, 2);
    defer alloc.free(snapshots);
    snapshots[0] = .{
        .table_name = try alloc.dupe(u8, "docs"),
        .statuses = .{ .items = try alloc.dupe(LocalTableRuntimeStatus, &.{.{ .group_id = 7, .stats = .{ .doc_count = 7 } }}) },
    };
    snapshots[1] = .{
        .table_name = try alloc.dupe(u8, "logs"),
        .statuses = .{ .items = try alloc.dupe(LocalTableRuntimeStatus, &.{.{ .group_id = 9, .stats = .{ .doc_count = 9 } }}) },
    };

    var result = try cache.publishRefresh(&token, snapshots);
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 1), result.published_tables);
    try std.testing.expectEqual(@as(usize, 1), result.rejected_tables.items.len);
    try std.testing.expectEqualStrings("docs", result.rejected_tables.items[0]);
    try std.testing.expect(result.removals_deferred);
    try std.testing.expect((try cache.snapshot(alloc, "docs")) == null);
    var logs = (try cache.snapshot(alloc, "logs")).?;
    defer logs.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 9), logs.items[0].stats.doc_count);
}

test "runtime status cache stable absence removal retires the old table epoch" {
    const alloc = std.testing.allocator;
    var cache = TableRuntimeSnapshotCache.init(alloc);
    defer cache.deinit();

    const stale_logs_token = try cache.capturePublicationToken("logs");
    try std.testing.expectEqual(TableRuntimeSnapshotCache.PublishResult.published, try cache.publishGroup(
        stale_logs_token,
        "logs",
        .{ .group_id = 9, .stats = .{ .doc_count = 9 } },
    ));

    // Production passes only tables present in the current catalog. A complete
    // token must still capture the cached epoch for the now-absent table.
    const table_names = [_][]const u8{"docs"};
    var token = try cache.captureCatalogToken(alloc, &table_names, true);
    defer token.deinit();
    const snapshots = try alloc.alloc(TableRuntimeSnapshot, 1);
    defer alloc.free(snapshots);
    snapshots[0] = .{
        .table_name = try alloc.dupe(u8, "docs"),
        .statuses = .{ .items = try alloc.dupe(LocalTableRuntimeStatus, &.{.{ .group_id = 7, .stats = .{ .doc_count = 7 } }}) },
    };

    var result = try cache.publishRefresh(&token, snapshots);
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 1), result.removed_tables);
    try std.testing.expect((try cache.snapshot(alloc, "logs")) == null);
    try std.testing.expectEqual(TableRuntimeSnapshotCache.PublishResult.stale_table, try cache.publishGroup(
        stale_logs_token,
        "logs",
        .{ .group_id = 9, .stats = .{ .doc_count = 10 } },
    ));
    const recreated = try cache.capturePublicationToken("logs");
    try std.testing.expect(!std.meta.eql(stale_logs_token.table_epoch, recreated.table_epoch));
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
            .doc_identity = .{
                .namespace_table_id = 101,
                .namespace_shard_id = 202,
                .namespace_range_id = 303,
                .next_ordinal = 44,
                .allocated_ordinals = 43,
                .rebuild_required = true,
                .state_rows = 41,
                .live_ordinals = 40,
                .min_created_generation = 12,
                .max_created_generation = 18,
                .min_deleted_generation = 15,
                .max_deleted_generation = 19,
            },
            .doc_set_planning = .{
                .resolved_set_count = 9,
                .ordinal_list_count = 8,
                .ordinal_list_docs = 7,
                .missing_ordinal_coverage_count = 6,
                .stale_identity_generation_rejection_count = 5,
            },
        },
    };
    items[0].stats.indexes[0] = .{
        .name = try std.testing.allocator.dupe(u8, "vec"),
        .kind = .dense_vector,
        .doc_count = 11,
        .node_count = 5,
        .coverage_produced_count = 5,
        .coverage_skipped_count = 6,
        .coverage_terminal_failed_count = 7,
        .coverage_config_hash = 0x1234,
        .coverage_summary_ready = false,
        .coverage_generation = 0x5678,
        .coverage_identity_ready = true,
        .backfill_active = true,
        .backfill_progress = 0.5,
        .enrichment_failed = true,
        .repair_scan_issue_count = 8,
        .projection_checkpoint_status = "rebuilding",
        .projection_checkpoint_applied_sequence = 9,
        .projection_checkpoint_generation = 10,
        .projection_checkpoint_config_hash = 11,
        .checkpoint_replay_tail_sequence_count = 12,
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
    try publishRefreshForTest(&cache, snapshots);

    var cloned = (try cache.snapshot(std.testing.allocator, "docs")).?;
    defer cloned.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 1), cloned.items.len);
    try std.testing.expectEqual(@as(u64, 7), cloned.items[0].group_id);
    try std.testing.expectEqual(@as(u64, 11), cloned.items[0].stats.doc_count);
    try std.testing.expectEqual(@as(u64, 101), cloned.items[0].stats.doc_identity.namespace_table_id);
    try std.testing.expectEqual(@as(u64, 202), cloned.items[0].stats.doc_identity.namespace_shard_id);
    try std.testing.expectEqual(@as(u64, 303), cloned.items[0].stats.doc_identity.namespace_range_id);
    try std.testing.expectEqual(@as(u32, 44), cloned.items[0].stats.doc_identity.next_ordinal);
    try std.testing.expect(cloned.items[0].stats.doc_identity.rebuild_required);
    try std.testing.expectEqual(@as(u64, 12), cloned.items[0].stats.doc_identity.min_created_generation);
    try std.testing.expectEqual(@as(u64, 18), cloned.items[0].stats.doc_identity.max_created_generation);
    try std.testing.expectEqual(@as(u64, 15), cloned.items[0].stats.doc_identity.min_deleted_generation);
    try std.testing.expectEqual(@as(u64, 19), cloned.items[0].stats.doc_identity.max_deleted_generation);
    try std.testing.expectEqual(@as(u64, 9), cloned.items[0].stats.doc_set_planning.resolved_set_count);
    try std.testing.expectEqual(@as(u64, 8), cloned.items[0].stats.doc_set_planning.ordinal_list_count);
    try std.testing.expectEqual(@as(u64, 5), cloned.items[0].stats.doc_set_planning.stale_identity_generation_rejection_count);
    try std.testing.expectEqualStrings("vec", cloned.items[0].stats.indexes[0].name);
    try std.testing.expectEqual(@as(u64, 5), cloned.items[0].stats.indexes[0].coverage_produced_count);
    try std.testing.expectEqual(@as(u64, 6), cloned.items[0].stats.indexes[0].coverage_skipped_count);
    try std.testing.expectEqual(@as(u64, 7), cloned.items[0].stats.indexes[0].coverage_terminal_failed_count);
    try std.testing.expectEqual(@as(u64, 0x1234), cloned.items[0].stats.indexes[0].coverage_config_hash);
    try std.testing.expect(!cloned.items[0].stats.indexes[0].coverage_summary_ready);
    try std.testing.expectEqual(@as(u64, 0x5678), cloned.items[0].stats.indexes[0].coverage_generation);
    try std.testing.expect(cloned.items[0].stats.indexes[0].coverage_identity_ready);
    try std.testing.expect(cloned.items[0].stats.indexes[0].backfill_active);
    try std.testing.expectEqual(@as(f64, 0.5), cloned.items[0].stats.indexes[0].backfill_progress);
    try std.testing.expect(cloned.items[0].stats.indexes[0].enrichment_failed);
    try std.testing.expectEqual(@as(u64, 8), cloned.items[0].stats.indexes[0].repair_scan_issue_count);
    try std.testing.expectEqualStrings("rebuilding", cloned.items[0].stats.indexes[0].projection_checkpoint_status);
    try std.testing.expectEqual(@as(u64, 9), cloned.items[0].stats.indexes[0].projection_checkpoint_applied_sequence);
    try std.testing.expectEqual(@as(u64, 10), cloned.items[0].stats.indexes[0].projection_checkpoint_generation);
    try std.testing.expectEqual(@as(u64, 11), cloned.items[0].stats.indexes[0].projection_checkpoint_config_hash);
    try std.testing.expectEqual(@as(u64, 12), cloned.items[0].stats.indexes[0].checkpoint_replay_tail_sequence_count);
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
    try publishRefreshForTest(&cache, initial);

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

    try publishRefreshForTest(&cache, refresh);

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
    try publishRefreshForTest(&cache, initial);

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
    try publishRefreshForTest(&cache, refresh);

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
    try publishRefreshForTest(&cache, initial);

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

    try publishRefreshForTest(&cache, refresh);

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
    try publishRefreshForTest(&cache, snapshots);

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
    try std.testing.expectEqual(TableRuntimeSnapshotCache.PublishResult.published, try publishGroupForTest(&cache, "docs", status));

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
    try std.testing.expectEqual(TableRuntimeSnapshotCache.PublishResult.published, try publishGroupForTest(&cache, "docs", cached_status));

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
    try std.testing.expectEqual(TableRuntimeSnapshotCache.PublishResult.published, try publishGroupForTest(&cache, "docs", regressed_status));

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
    try std.testing.expectEqual(TableRuntimeSnapshotCache.PublishResult.published, try publishGroupForTest(&cache, "docs", cached_status));

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
    try std.testing.expectEqual(TableRuntimeSnapshotCache.PublishResult.published, try publishGroupForTest(&cache, "docs", newer_status));

    var docs = (try cache.snapshot(std.testing.allocator, "docs")).?;
    defer docs.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(u64, 24_999), docs.items[0].stats.doc_count);
    try std.testing.expectEqual(@as(u64, 24_999), docs.items[0].stats.indexes[0].doc_count);
    try std.testing.expectEqual(@as(u64, 101), docs.items[0].stats.indexes[0].replay_applied_sequence);
    try std.testing.expectEqual(@as(u64, 101), docs.items[0].stats.indexes[0].replay_target_sequence);
}

test "table runtime snapshot cache rejects a late stale live observation" {
    var cache = TableRuntimeSnapshotCache.init(std.testing.allocator);
    defer cache.deinit();

    const stale_token = try cache.capturePublicationToken("docs");
    const current_token = try cache.capturePublicationToken("docs");

    const current = LocalTableRuntimeStatus{
        .group_id = 7,
        .stats = .{ .doc_count = 12 },
    };
    try std.testing.expectEqual(TableRuntimeSnapshotCache.PublishResult.published, try cache.publishGroup(current_token, "docs", current));

    const stale = LocalTableRuntimeStatus{
        .group_id = 7,
        .stats = .{ .doc_count = 10 },
    };
    try std.testing.expectEqual(TableRuntimeSnapshotCache.PublishResult.stale_observation, try cache.publishGroup(stale_token, "docs", stale));

    var docs = (try cache.snapshot(std.testing.allocator, "docs")).?;
    defer docs.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u64, 12), docs.items[0].stats.doc_count);
    try std.testing.expectEqual(current_token.observation_generation, docs.items[0].cache_observation_generation);
}

test "table runtime snapshot cache preserves active managed admission proof" {
    const alloc = std.testing.allocator;
    var cache = TableRuntimeSnapshotCache.init(alloc);
    defer cache.deinit();

    var indexes = [_]db_mod.types.DBIndexStats{.{
        .name = "thumbnail",
        .kind = .dense_vector,
        .index_repair_id = 1,
        .index_repair_trigger = "projection_generation_invalid",
        .index_repair_phase = "detected",
        .index_repair_active_generation_serviceable = true,
    }};
    const status = LocalTableRuntimeStatus{
        .group_id = 7,
        .stats = .{
            .index_count = 1,
            .indexes = &indexes,
        },
    };
    try std.testing.expectEqual(
        TableRuntimeSnapshotCache.PublishResult.published,
        try publishGroupForTest(&cache, "docs", status),
    );

    var snapshot = (try cache.snapshot(alloc, "docs")).?;
    defer snapshot.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), snapshot.items.len);
    try std.testing.expectEqual(@as(usize, 1), snapshot.items[0].stats.indexes.len);
    try std.testing.expect(snapshot.items[0].stats.indexes[0].index_repair_active_generation_serviceable);
}

test "table runtime snapshot cache invalidation fences a stale observed publisher" {
    const alloc = std.testing.allocator;
    var cache = TableRuntimeSnapshotCache.init(alloc);
    defer cache.deinit();

    const stale_token = try cache.capturePublicationToken("docs");
    cache.invalidateTable("docs");

    const stale = LocalTableRuntimeStatus{
        .group_id = 7,
        .stats = .{ .doc_count = 10 },
    };
    try std.testing.expectEqual(TableRuntimeSnapshotCache.PublishResult.stale_table, try cache.publishGroup(stale_token, "docs", stale));
    try std.testing.expect((try cache.snapshot(alloc, "docs")) == null);

    const current_token = try cache.capturePublicationToken("docs");
    const current = LocalTableRuntimeStatus{
        .group_id = 7,
        .stats = .{ .doc_count = 12 },
    };
    try std.testing.expectEqual(TableRuntimeSnapshotCache.PublishResult.published, try cache.publishGroup(current_token, "docs", current));

    var docs = (try cache.snapshot(alloc, "docs")).?;
    defer docs.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 12), docs.items[0].stats.doc_count);
    try std.testing.expectEqual(current_token.observation_generation, docs.items[0].cache_observation_generation);
}

test "table runtime snapshot cache batch publication is table epoch atomic" {
    const alloc = std.testing.allocator;
    var cache = TableRuntimeSnapshotCache.init(alloc);
    defer cache.deinit();

    const stale_token = try cache.capturePublicationToken("docs");
    cache.invalidateTable("docs");
    const statuses = [_]LocalTableRuntimeStatus{
        .{ .group_id = 7, .stats = .{ .doc_count = 10 } },
        .{ .group_id = 8, .stats = .{ .doc_count = 20 } },
    };
    try std.testing.expectEqual(
        TableRuntimeSnapshotCache.PublishResult.stale_table,
        try cache.publishGroups(stale_token, "docs", &statuses),
    );
    try std.testing.expect((try cache.snapshot(alloc, "docs")) == null);
}

test "table runtime snapshot cache lifecycle transition replaces and fences observations" {
    const alloc = std.testing.allocator;
    var cache = TableRuntimeSnapshotCache.init(alloc);
    defer cache.deinit();

    const initial_token = try cache.capturePublicationToken("docs");
    try std.testing.expectEqual(
        TableRuntimeSnapshotCache.PublishResult.published,
        try cache.publishGroups(initial_token, "docs", &.{
            .{ .group_id = 7, .stats = .{ .doc_count = 10 } },
            .{ .group_id = 8, .stats = .{ .doc_count = 20 } },
        }),
    );

    const in_flight_token = try cache.capturePublicationToken("docs");
    const transition_token = try cache.capturePublicationToken("docs");
    try std.testing.expectEqual(
        TableRuntimeSnapshotCache.PublishResult.published,
        try cache.publishLifecycleTransition(transition_token, "docs", &.{
            .{
                .group_id = 7,
                .metadata = .{
                    .source = .startup_catch_up,
                    .freshness = .catching_up,
                },
                .stats = .{ .doc_count = 10 },
            },
        }),
    );

    const current_token = try cache.capturePublicationToken("docs");
    try std.testing.expectEqual(
        transition_token.table_epoch.root_generation,
        current_token.table_epoch.root_generation,
    );
    try std.testing.expect(
        transition_token.table_epoch.invalidation_epoch != current_token.table_epoch.invalidation_epoch,
    );
    try std.testing.expectEqual(
        TableRuntimeSnapshotCache.PublishResult.stale_table,
        try cache.publishGroup(in_flight_token, "docs", .{
            .group_id = 8,
            .stats = .{ .doc_count = 21 },
        }),
    );

    var docs = (try cache.snapshot(alloc, "docs")).?;
    defer docs.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), docs.items.len);
    try std.testing.expectEqual(@as(u64, 7), docs.items[0].group_id);
    try std.testing.expectEqual(RuntimeStatusFreshness.catching_up, docs.items[0].metadata.freshness);
}

test "table runtime snapshot cache batch preserves newer group observations" {
    const alloc = std.testing.allocator;
    var cache = TableRuntimeSnapshotCache.init(alloc);
    defer cache.deinit();

    const batch_token = try cache.capturePublicationToken("docs");
    const newer_token = try cache.capturePublicationToken("docs");
    try std.testing.expectEqual(
        TableRuntimeSnapshotCache.PublishResult.published,
        try cache.publishGroup(newer_token, "docs", .{ .group_id = 7, .stats = .{ .doc_count = 12 } }),
    );
    const statuses = [_]LocalTableRuntimeStatus{
        .{ .group_id = 7, .stats = .{ .doc_count = 10 } },
        .{ .group_id = 8, .stats = .{ .doc_count = 20 } },
    };
    try std.testing.expectEqual(
        TableRuntimeSnapshotCache.PublishResult.published,
        try cache.publishGroups(batch_token, "docs", &statuses),
    );

    var docs = (try cache.snapshot(alloc, "docs")).?;
    defer docs.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 2), docs.items.len);
    for (docs.items) |status| switch (status.group_id) {
        7 => {
            try std.testing.expectEqual(@as(u64, 12), status.stats.doc_count);
            try std.testing.expectEqual(newer_token.observation_generation, status.cache_observation_generation);
        },
        8 => {
            try std.testing.expectEqual(@as(u64, 20), status.stats.doc_count);
            try std.testing.expectEqual(batch_token.observation_generation, status.cache_observation_generation);
        },
        else => return error.UnexpectedRuntimeStatusGroup,
    };
}

test "table runtime snapshot cache live publication does not starve structural refresh" {
    const alloc = std.testing.allocator;
    var cache = TableRuntimeSnapshotCache.init(alloc);
    defer cache.deinit();

    const table_names = [_][]const u8{"docs"};
    var refresh_token = try cache.captureCatalogToken(alloc, &table_names, true);
    defer refresh_token.deinit();
    const live_token = try cache.capturePublicationToken("docs");
    try std.testing.expectEqual(TableRuntimeSnapshotCache.PublishResult.published, try cache.publishGroup(live_token, "docs", .{
        .group_id = 7,
        .stats = .{ .doc_count = 12 },
    }));

    const statuses = try alloc.alloc(LocalTableRuntimeStatus, 1);
    statuses[0] = .{
        .group_id = 7,
        .stats = .{ .doc_count = 10 },
    };
    const snapshots = try alloc.alloc(TableRuntimeSnapshot, 1);
    defer alloc.free(snapshots);
    snapshots[0] = .{
        .table_name = try alloc.dupe(u8, "docs"),
        .statuses = .{ .items = statuses },
    };

    var refresh_result = try cache.publishRefresh(&refresh_token, snapshots);
    defer refresh_result.deinit();
    try std.testing.expect(!refresh_result.hasRejectedTables());
    var docs = (try cache.snapshot(alloc, "docs")).?;
    defer docs.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 12), docs.items[0].stats.doc_count);
    try std.testing.expectEqual(live_token.observation_generation, docs.items[0].cache_observation_generation);
}

test "live writer artifact regression keeps authoritative source deletions" {
    var previous_indexes = [_]db_mod.types.DBIndexStats{.{
        .name = @constCast("semantic_idx"),
        .kind = .dense_vector,
        .doc_count = 1,
        .coverage_config_hash = 77,
        .projection_checkpoint_applied_sequence = 2,
        .projection_checkpoint_config_hash = 77,
        .replay_applied_sequence = 2,
        .replay_target_sequence = 2,
    }};
    const previous = LocalTableRuntimeStatus{
        .group_id = 7,
        .stats = .{
            .source_doc_count = 1,
            .doc_count = 1,
            .index_count = 1,
            .indexes = previous_indexes[0..],
        },
    };

    var incoming_indexes = [_]db_mod.types.DBIndexStats{.{
        .name = @constCast("semantic_idx"),
        .kind = .dense_vector,
        .coverage_config_hash = 77,
        .projection_checkpoint_applied_sequence = 0,
        .projection_checkpoint_config_hash = 77,
        .replay_applied_sequence = 2,
        .replay_target_sequence = 2,
    }};
    var incoming = LocalTableRuntimeStatus{
        .group_id = 7,
        .metadata = .{ .source = .live_writer_publish, .freshness = .fresh },
        .stats = .{
            .source_doc_count = 0,
            .doc_count = 0,
            .index_count = 1,
            .indexes = incoming_indexes[0..],
        },
    };

    preserveArtifactVisibilityOnReplayRegression(previous, &incoming);
    try std.testing.expectEqual(@as(u64, 0), incoming.stats.source_doc_count);
    try std.testing.expectEqual(@as(u64, 1), incoming.stats.doc_count);
}

test "table runtime snapshot cache preserves live completion over regressing persisted projection" {
    const alloc = std.testing.allocator;
    var cache = TableRuntimeSnapshotCache.init(alloc);
    defer cache.deinit();

    var live_indexes = [_]db_mod.types.DBIndexStats{.{
        .name = @constCast("semantic_idx"),
        .kind = .dense_vector,
        .doc_count = 1,
        .coverage_produced_count = 1,
        .coverage_config_hash = 77,
        .coverage_generation = 1,
        .coverage_identity_ready = true,
        .backfill_progress = 1.0,
        .projection_checkpoint_status = "clean",
        .projection_checkpoint_applied_sequence = 2,
        .projection_checkpoint_generation = 0,
        .projection_checkpoint_config_hash = 0,
        .replay_applied_sequence = 2,
        .replay_target_sequence = 2,
    }};
    const live_token = try cache.capturePublicationToken("docs");
    try std.testing.expectEqual(TableRuntimeSnapshotCache.PublishResult.published, try cache.publishGroup(live_token, "docs", .{
        .group_id = 7001,
        .stats = .{
            .source_doc_count = 1,
            .doc_count = 1,
            .index_count = 1,
            .indexes = live_indexes[0..],
        },
    }));

    const table_names = [_][]const u8{"docs"};
    var refresh_token = try cache.captureCatalogToken(alloc, &table_names, true);
    defer refresh_token.deinit();
    const refresh_indexes = try alloc.alloc(db_mod.types.DBIndexStats, 1);
    refresh_indexes[0] = .{
        .name = try alloc.dupe(u8, "semantic_idx"),
        .kind = .dense_vector,
        .doc_count = 1,
        .coverage_produced_count = 1,
        .coverage_config_hash = 77,
        .coverage_generation = 1,
        .coverage_identity_ready = true,
        .backfill_active = true,
        .backfill_progress = 0.5,
        .projection_checkpoint_status = "rebuilding",
        .projection_checkpoint_applied_sequence = 0,
        .projection_checkpoint_generation = 0,
        .projection_checkpoint_config_hash = 0,
        .replay_applied_sequence = 2,
        .replay_target_sequence = 2,
    };
    const refresh_statuses = try alloc.alloc(LocalTableRuntimeStatus, 1);
    refresh_statuses[0] = .{
        .group_id = 7001,
        .stats = .{
            .source_doc_count = 0,
            .doc_count = 1,
            .index_count = 1,
            .indexes = refresh_indexes,
        },
    };
    const refresh = try alloc.alloc(TableRuntimeSnapshot, 1);
    defer alloc.free(refresh);
    refresh[0] = .{
        .table_name = try alloc.dupe(u8, "docs"),
        .statuses = .{ .items = refresh_statuses },
    };
    var refresh_result = try cache.publishRefresh(&refresh_token, refresh);
    defer refresh_result.deinit();

    var published = (try cache.snapshot(alloc, "docs")).?;
    defer published.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 1), published.items[0].stats.source_doc_count);
    try std.testing.expectEqualStrings("clean", published.items[0].stats.indexes[0].projection_checkpoint_status);
    try std.testing.expectEqual(@as(u64, 2), published.items[0].stats.indexes[0].projection_checkpoint_applied_sequence);
    try std.testing.expect(!published.items[0].stats.indexes[0].backfill_active);
    try std.testing.expectEqual(@as(f64, 1.0), published.items[0].stats.indexes[0].backfill_progress);
}

test "table runtime snapshot cache table fences isolate unrelated invalidations" {
    const alloc = std.testing.allocator;
    var cache = TableRuntimeSnapshotCache.init(alloc);
    defer cache.deinit();

    const docs_token = try cache.capturePublicationToken("docs");
    cache.invalidateTable("other");
    try std.testing.expectEqual(TableRuntimeSnapshotCache.PublishResult.published, try cache.publishGroup(
        docs_token,
        "docs",
        .{ .group_id = 7, .stats = .{ .doc_count = 1 } },
    ));
}

test "table runtime snapshot cache replacement preserves a newer live observation" {
    const alloc = std.testing.allocator;
    var cache = TableRuntimeSnapshotCache.init(alloc);
    defer cache.deinit();

    const stale_token = try cache.capturePublicationToken("docs");
    const stale = LocalTableRuntimeStatus{
        .group_id = 7,
        .stats = .{ .doc_count = 10 },
    };
    try std.testing.expectEqual(TableRuntimeSnapshotCache.PublishResult.published, try cache.publishGroup(stale_token, "docs", stale));

    const table_names = [_][]const u8{"docs"};
    var refresh_token = try cache.captureCatalogToken(alloc, &table_names, true);
    defer refresh_token.deinit();

    // Model a refresh that cloned generation 1, then released the cache lock.
    const replacement = try alloc.alloc(TableRuntimeSnapshot, 1);
    defer alloc.free(replacement);
    replacement[0] = .{
        .table_name = try alloc.dupe(u8, "docs"),
        .statuses = .{ .items = try alloc.alloc(LocalTableRuntimeStatus, 1) },
    };
    replacement[0].statuses.items[0] = (try cache.snapshotGroupStatus(alloc, "docs", 7)).?;

    const current_token = try cache.capturePublicationToken("docs");
    const current = LocalTableRuntimeStatus{
        .group_id = 7,
        .stats = .{ .doc_count = 12 },
    };
    try std.testing.expectEqual(TableRuntimeSnapshotCache.PublishResult.published, try cache.publishGroup(current_token, "docs", current));

    var refresh_result = try cache.publishRefresh(&refresh_token, replacement);
    defer refresh_result.deinit();
    var docs = (try cache.snapshot(alloc, "docs")).?;
    defer docs.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 12), docs.items[0].stats.doc_count);
    try std.testing.expectEqual(current_token.observation_generation, docs.items[0].cache_observation_generation);
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

test "cached all-skipped coverage observation is a runtime fact" {
    var indexes = [_]db_mod.types.DBIndexStats{.{
        .name = "visual",
        .kind = .dense_vector,
        .coverage_skipped_count = 2,
        .coverage_config_hash = 0x1234,
        .coverage_summary_ready = true,
    }};
    const status = LocalTableRuntimeStatus{
        .group_id = 7,
        .metadata = .{ .source = .cached_snapshot, .freshness = .fresh },
        .stats = .{ .source_doc_count = 2, .index_count = 1, .indexes = indexes[0..] },
    };
    try std.testing.expect(statusHasRuntimeFacts(status));
}

test "cached identity and doc set telemetry are runtime facts" {
    const identity_status = LocalTableRuntimeStatus{
        .group_id = 7,
        .metadata = .{
            .source = .cached_snapshot,
            .freshness = .stale,
        },
        .stats = .{
            .doc_identity = .{
                .rebuild_required = true,
            },
        },
    };

    const planning_status = LocalTableRuntimeStatus{
        .group_id = 8,
        .metadata = .{
            .source = .synthetic_config,
            .freshness = .stale,
        },
        .stats = .{
            .doc_set_planning = .{
                .stale_identity_generation_rejection_count = 1,
            },
        },
    };

    try std.testing.expect(statusHasRuntimeFacts(identity_status));
    try std.testing.expect(statusHasRuntimeFacts(planning_status));
}

test "cached repair telemetry is runtime facts" {
    const status = LocalTableRuntimeStatus{
        .group_id = 9,
        .metadata = .{
            .source = .cached_snapshot,
            .freshness = .stale,
        },
        .stats = .{
            .repair_degraded = true,
            .repair_issue_count = 1,
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
    try std.testing.expectEqual(TableRuntimeSnapshotCache.PublishResult.published, try publishGroupForTest(&cache, "docs", cached_status));

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
    try std.testing.expectEqual(TableRuntimeSnapshotCache.PublishResult.published, try publishGroupForTest(&cache, "docs", incoming_status));

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
            defer alloc.free(snapshots);
            snapshots[0] = .{
                .table_name = try alloc.dupe(u8, "docs"),
                .statuses = .{ .items = initial_items },
            };
            try publishRefreshForTest(&cache, snapshots);

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

            _ = publishGroupForTest(&cache, "docs", replacement) catch |err| switch (err) {
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
            defer alloc.free(initial);
            initial[0] = .{
                .table_name = try alloc.dupe(u8, "docs"),
                .statuses = .{ .items = initial_docs_items },
            };
            initial[1] = .{
                .table_name = try alloc.dupe(u8, "logs"),
                .statuses = .{ .items = initial_logs_items },
            };
            try publishRefreshForTest(&cache, initial);

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
            defer alloc.free(refresh);
            refresh[0] = .{
                .table_name = try alloc.dupe(u8, "docs"),
                .statuses = .{ .items = refresh_docs_items },
            };

            publishRefreshForTest(&cache, refresh) catch |err| switch (err) {
                error.OutOfMemory => {},
                else => return err,
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
    try publishRefreshForTest(&cache, snapshots);

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
