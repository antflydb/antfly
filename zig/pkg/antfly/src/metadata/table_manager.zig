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
const group_ids = @import("../common/group_ids.zig");
const transition_state = @import("transition_state.zig");

pub const PlacementClass = enum {
    data,
    hot,
    cold,
    serving,
    bulk,
    archive,
};

pub const TableRecord = struct {
    table_id: u64,
    name: []const u8,
    description: []const u8 = "",
    schema_json: []const u8 = "",
    read_schema_json: []const u8 = "",
    indexes_json: []const u8 = "{}",
    replication_sources_json: []const u8 = "[]",
    placement_role: []const u8 = "data",
    restore_backup_id: []const u8 = "",
    restore_location: []const u8 = "",
    desired_replica_count: u16 = 3,
    min_ranges: u32 = 1,

    pub fn migrationState(self: *const TableRecord) TableMigrationState {
        return .{
            .schema_json = self.schema_json,
            .read_schema_json = self.read_schema_json,
        };
    }

    pub fn indexCatalog(self: *const TableRecord) TableIndexCatalog {
        return .{
            .indexes_json = self.indexes_json,
        };
    }
};

// TableDefinition is the preferred product/control-plane name. TableRecord
// remains as the current storage/runtime name during the migration.
pub const TableDefinition = TableRecord;

pub const TableMigrationState = struct {
    schema_json: []const u8,
    read_schema_json: []const u8,

    pub fn migrating(self: TableMigrationState) bool {
        return self.read_schema_json.len > 0;
    }
};

pub const TableIndexCatalog = struct {
    indexes_json: []const u8,
};

pub const RangeRecord = struct {
    group_id: u64,
    range_id: u64 = 0,
    table_id: u64,
    start_key: []const u8,
    end_key: ?[]const u8 = null,
    doc_identity_shard_id: u64 = 0,
    doc_identity_range_id: u64 = 0,
    restore_backup_id: []const u8 = "",
    restore_location: []const u8 = "",
    restore_snapshot_path: []const u8 = "",
};

pub const node_lifecycle_active = "active";
pub const node_lifecycle_draining = "draining";

pub fn nodeLifecycleActive(lifecycle: []const u8) bool {
    return std.mem.eql(u8, lifecycle, node_lifecycle_active);
}

pub const NodeRecord = struct {
    node_id: u64,
    role: []const u8 = "data",
    lifecycle: []const u8 = node_lifecycle_active,
};

pub const StoreRecord = struct {
    store_id: u64,
    node_id: u64,
    api_url: []const u8 = "",
    raft_url: []const u8 = "",
    role: []const u8 = "data",
    health_class: []const u8 = "healthy",
    failure_domain: []const u8 = "",
    live: bool = true,
    drain_requested: bool = false,
    capacity_bytes: u64 = 0,
    available_bytes: u64 = 0,
    lease_pressure: u32 = 0,
    read_load: u32 = 0,
    write_load: u32 = 0,
    active_backfills: u32 = 0,
    backfill_progress_millis: u16 = 1000,
    group_statuses: []GroupStatusReport = &.{},
    runtime_statuses: []RuntimeGroupStatusReport = &.{},
};

pub const GroupStatusReport = struct {
    group_id: u64,
    doc_count: u64 = 0,
    disk_bytes: u64 = 0,
    empty: bool = true,
    created_at_millis: u64 = 0,
    updated_at_millis: u64 = 0,
    local_leader: bool = false,
    local_voter: bool = false,
    voter_count: u16 = 0,
    joint_consensus: bool = false,
    transition_pending: bool = false,
    replay_required: bool = false,
    replay_caught_up: bool = false,
    cutover_ready: bool = false,
    reads_ready_after_cutover: bool = false,
};

pub const StoreStatusReport = struct {
    store_id: u64,
    live: bool = true,
    health_class: []const u8 = "healthy",
    capacity_bytes: u64 = 0,
    available_bytes: u64 = 0,
    lease_pressure: u32 = 0,
    read_load: u32 = 0,
    write_load: u32 = 0,
    active_backfills: u32 = 0,
    backfill_progress_millis: u16 = 1000,
    group_statuses: []GroupStatusReport = &.{},
    runtime_statuses: []RuntimeGroupStatusReport = &.{},
};

pub const RuntimeGroupStatusReport = struct {
    table_id: u64 = 0,
    table_name: []const u8 = "",
    group_id: u64 = 0,
    store_id: u64 = 0,
    node_id: u64 = 0,
    updated_at_ns: u64 = 0,
    source: []const u8 = "unknown",
    freshness: []const u8 = "unknown",
    topology_generation: u64 = 0,
    lsm_root_generation: u64 = 0,
    status_generation: u64 = 0,
    doc_count: u64 = 0,
    disk_bytes: u64 = 0,
    created_at_millis: u64 = 0,
    index_count: u32 = 0,
    enrichment_enabled: bool = false,
    enrichment_target_sequence: u64 = 0,
    enrichment_applied_sequence: u64 = 0,
    enrichment_retrying: bool = false,
    enrichment_worker_failed: bool = false,
    async_indexing_active: bool = false,
    async_startup_active: bool = false,
    async_dense_catch_up_active: bool = false,
    async_bulk_coalescing_active: bool = false,
    doc_identity: RuntimeDocIdentityStatusReport = .{},
    doc_set_planning: RuntimeDocSetPlanningStatusReport = .{},
    indexes: []RuntimeIndexStatusReport = &.{},
};

pub const RuntimeDocIdentityStatusReport = struct {
    namespace_table_id: u64 = 0,
    namespace_shard_id: u64 = 0,
    namespace_range_id: u64 = 0,
    next_ordinal: u32 = 1,
    allocated_ordinals: u64 = 0,
    ordinal_capacity_remaining: u64 = 0,
    ordinal_capacity_exhausted: bool = false,
    rebuild_required: bool = false,
    state_rows: u64 = 0,
    live_ordinals: u64 = 0,
    tombstone_ordinals: u64 = 0,
    min_created_generation: u64 = 0,
    max_created_generation: u64 = 0,
    min_deleted_generation: u64 = 0,
    max_deleted_generation: u64 = 0,
    scanned_primary_docs: u64 = 0,
    primary_docs_missing_ordinals: u64 = 0,
    primary_docs_missing_identity_state: u64 = 0,
    primary_docs_with_tombstone_ordinals: u64 = 0,
    complete: bool = false,
};

pub const RuntimeDocSetPlanningStatusReport = struct {
    resolved_set_count: u64 = 0,
    all_set_count: u64 = 0,
    none_set_count: u64 = 0,
    doc_key_list_count: u64 = 0,
    ordinal_list_count: u64 = 0,
    ordinal_bitmap_count: u64 = 0,
    doc_key_list_docs: u64 = 0,
    ordinal_list_docs: u64 = 0,
    ordinal_bitmap_docs: u64 = 0,
    missing_ordinal_coverage_count: u64 = 0,
    bitmap_promotion_count: u64 = 0,
    unsupported_filter_shape_count: u64 = 0,
    stale_identity_generation_rejection_count: u64 = 0,
};

pub const RuntimeIndexStatusReport = struct {
    name: []const u8 = "",
    kind: []const u8 = "",
    doc_count: u64 = 0,
    term_count: u64 = 0,
    edge_count: u64 = 0,
    node_count: u64 = 0,
    root_node: u64 = 0,
    backfill_active: bool = false,
    backfill_progress_millis: u16 = 0,
    replay_applied_sequence: u64 = 0,
    replay_target_sequence: u64 = 0,
    replay_catch_up_required: bool = false,
};

pub const SchemaProgressRecord = struct {
    table_id: u64,
    node_id: u64,
    schema_version: u32 = 0,
};

pub const RestoreProgressRecord = struct {
    table_id: u64,
    node_id: u64,
    group_id: u64,
    backup_id: []const u8,
    snapshot_path: []const u8 = "",
    primary_restored: bool = false,
    runtime_repair_complete: bool = false,
    phase: []const u8 = "",
    last_error: []const u8 = "",
    updated_at_ms: u64 = 0,
};

pub const ReplicationSourceStatusRecord = struct {
    table_id: u64,
    source_ordinal: u32,
    source_kind: []const u8,
    external_table: []const u8 = "",
    cutover_mode: []const u8 = "",
    slot_name: []const u8 = "",
    publication_name: []const u8 = "",
    phase: []const u8 = "configured",
    checkpoint: []const u8 = "",
    snapshot_offset: u64 = 0,
    prepared_checkpoint: []const u8 = "",
    stream_checkpoint: []const u8 = "",
    last_error: []const u8 = "",
    failure_class: []const u8 = "",
    lag_records: u64 = 0,
    lag_millis: u64 = 0,
    consecutive_failures: u64 = 0,
    last_source_commit_at_ms: u64 = 0,
    last_success_at_ms: u64 = 0,
    last_change_applied_at_ms: u64 = 0,
    updated_at_ms: u64 = 0,
};

pub const ShuffleJoinLeaseRecord = struct {
    job_id: u64,
    owner_group_id: u64,
    expires_at_ms: u64,
};

pub const SplitIntent = struct {
    transition_id: u64,
    table_id: u64,
    source_group_id: u64,
    destination_group_id: u64,
    split_key: []const u8,
    rollback_reason: ?[]const u8 = null,
    automatic: bool = false,
};

pub const MergeIntent = struct {
    transition_id: u64,
    table_id: u64,
    donor_group_id: u64,
    receiver_group_id: u64,
    rollback_reason: ?[]const u8 = null,
    automatic: bool = false,
    allow_doc_identity_reassignment: bool = false,
};

pub const TableManager = struct {
    alloc: std.mem.Allocator,
    tables: std.AutoHashMapUnmanaged(u64, TableRecord) = .empty,
    ranges: std.AutoHashMapUnmanaged(u64, RangeRecord) = .empty,
    split_intents: std.AutoHashMapUnmanaged(u64, SplitIntent) = .empty,
    merge_intents: std.AutoHashMapUnmanaged(u64, MergeIntent) = .empty,

    pub fn init(alloc: std.mem.Allocator) TableManager {
        return .{ .alloc = alloc };
    }

    pub fn deinit(self: *TableManager) void {
        var table_it = self.tables.valueIterator();
        while (table_it.next()) |table| freeTable(self.alloc, table.*);
        self.tables.deinit(self.alloc);

        var range_it = self.ranges.valueIterator();
        while (range_it.next()) |range| freeRange(self.alloc, range.*);
        self.ranges.deinit(self.alloc);

        var split_it = self.split_intents.valueIterator();
        while (split_it.next()) |intent| freeSplitIntent(self.alloc, intent.*);
        self.split_intents.deinit(self.alloc);

        var merge_it = self.merge_intents.valueIterator();
        while (merge_it.next()) |intent| freeMergeIntent(self.alloc, intent.*);
        self.merge_intents.deinit(self.alloc);

        self.* = undefined;
    }

    pub fn upsertTable(self: *TableManager, record: TableRecord) !void {
        const owned = try cloneTable(self.alloc, record);
        errdefer freeTable(self.alloc, owned);
        if (self.tables.getPtr(record.table_id)) |existing| {
            freeTable(self.alloc, existing.*);
            existing.* = owned;
            return;
        }
        try self.tables.put(self.alloc, record.table_id, owned);
    }

    pub fn upsertRange(self: *TableManager, record: RangeRecord) !void {
        try group_ids.requireDataGroupId(record.group_id);
        const table = self.tables.get(record.table_id) orelse return error.UnknownTable;
        _ = table;

        var normalized = record;
        if (normalized.range_id == 0) normalized.range_id = normalized.group_id;
        const owned = try cloneRange(self.alloc, normalized);
        errdefer freeRange(self.alloc, owned);
        if (self.ranges.getPtr(record.group_id)) |existing| {
            freeRange(self.alloc, existing.*);
            existing.* = owned;
            return;
        }
        try self.ranges.put(self.alloc, record.group_id, owned);
    }

    pub fn clearTopology(self: *TableManager) void {
        var table_it = self.tables.valueIterator();
        while (table_it.next()) |table| freeTable(self.alloc, table.*);
        self.tables.clearRetainingCapacity();

        var range_it = self.ranges.valueIterator();
        while (range_it.next()) |range| freeRange(self.alloc, range.*);
        self.ranges.clearRetainingCapacity();
    }

    pub fn replaceTopology(self: *TableManager, tables: []const TableRecord, ranges: []const RangeRecord) !void {
        self.clearTopology();
        for (tables) |record| try self.upsertTable(record);
        for (ranges) |record| try self.upsertRange(record);
    }

    pub const ProjectedTopologyLoadResult = struct {
        skipped_orphan_ranges: usize = 0,
    };

    pub fn replaceProjectedTopology(self: *TableManager, tables: []const TableRecord, ranges: []const RangeRecord) !ProjectedTopologyLoadResult {
        self.clearTopology();
        for (tables) |record| try self.upsertTable(record);

        var result: ProjectedTopologyLoadResult = .{};
        for (ranges) |record| {
            if (!self.tables.contains(record.table_id)) {
                result.skipped_orphan_ranges += 1;
                continue;
            }
            try self.upsertRange(record);
        }
        return result;
    }

    pub fn removeTable(self: *TableManager, table_id: u64) bool {
        const removed = self.tables.fetchRemove(table_id);
        if (removed) |entry| {
            freeTable(self.alloc, entry.value);
            return true;
        }
        return false;
    }

    pub fn removeTableTopology(self: *TableManager, table_id: u64) usize {
        var removed_ranges: usize = 0;
        var to_remove = std.ArrayListUnmanaged(u64).empty;
        defer to_remove.deinit(self.alloc);

        var it = self.ranges.iterator();
        while (it.next()) |entry| {
            if (entry.value_ptr.table_id != table_id) continue;
            to_remove.append(self.alloc, entry.key_ptr.*) catch continue;
        }
        for (to_remove.items) |group_id| {
            if (self.removeRange(group_id)) removed_ranges += 1;
        }
        _ = self.removeTable(table_id);
        return removed_ranges;
    }

    pub fn removeRange(self: *TableManager, group_id: u64) bool {
        const removed = self.ranges.fetchRemove(group_id);
        if (removed) |entry| {
            freeRange(self.alloc, entry.value);
            return true;
        }
        return false;
    }

    pub fn listTables(self: *TableManager, alloc: std.mem.Allocator) ![]TableRecord {
        var out = std.ArrayListUnmanaged(TableRecord).empty;
        errdefer {
            for (out.items) |record| freeTable(alloc, record);
            out.deinit(alloc);
        }
        var it = self.tables.valueIterator();
        while (it.next()) |record| try out.append(alloc, try cloneTable(alloc, record.*));
        return try out.toOwnedSlice(alloc);
    }

    pub fn freeTables(_: *TableManager, alloc: std.mem.Allocator, records: []TableRecord) void {
        for (records) |record| freeTable(alloc, record);
        alloc.free(records);
    }

    pub fn listRanges(self: *TableManager, alloc: std.mem.Allocator) ![]RangeRecord {
        var out = std.ArrayListUnmanaged(RangeRecord).empty;
        errdefer {
            for (out.items) |record| freeRange(alloc, record);
            out.deinit(alloc);
        }
        var it = self.ranges.valueIterator();
        while (it.next()) |record| try out.append(alloc, try cloneRange(alloc, record.*));
        return try out.toOwnedSlice(alloc);
    }

    pub fn freeRanges(_: *TableManager, alloc: std.mem.Allocator, records: []RangeRecord) void {
        for (records) |record| freeRange(alloc, record);
        alloc.free(records);
    }

    pub fn requestSplit(self: *TableManager, intent: SplitIntent) !void {
        try group_ids.requireDataGroupId(intent.source_group_id);
        try group_ids.requireDataGroupId(intent.destination_group_id);
        const source = self.ranges.get(intent.source_group_id) orelse return error.UnknownSourceRange;
        if (source.table_id != intent.table_id) return error.TableRangeMismatch;
        if (!keyStrictlyInsideRange(intent.split_key, source.start_key, source.end_key)) return error.InvalidSplitKey;
        if (self.ranges.contains(intent.destination_group_id)) return error.DestinationRangeAlreadyExists;

        const owned = try cloneSplitIntent(self.alloc, intent);
        errdefer freeSplitIntent(self.alloc, owned);
        if (self.split_intents.getPtr(intent.transition_id)) |existing| {
            freeSplitIntent(self.alloc, existing.*);
            existing.* = owned;
            return;
        }
        try self.split_intents.put(self.alloc, intent.transition_id, owned);
    }

    pub fn requestMerge(self: *TableManager, intent: MergeIntent) !void {
        try group_ids.requireDataGroupId(intent.donor_group_id);
        try group_ids.requireDataGroupId(intent.receiver_group_id);
        const donor = self.ranges.get(intent.donor_group_id) orelse return error.UnknownDonorRange;
        const receiver = self.ranges.get(intent.receiver_group_id) orelse return error.UnknownReceiverRange;
        if (donor.table_id != intent.table_id or receiver.table_id != intent.table_id) return error.TableRangeMismatch;
        if (!rangesAdjacent(donor, receiver)) return error.RangesNotAdjacent;

        const owned = try cloneMergeIntent(self.alloc, intent);
        errdefer freeMergeIntent(self.alloc, owned);
        if (self.merge_intents.getPtr(intent.transition_id)) |existing| {
            freeMergeIntent(self.alloc, existing.*);
            existing.* = owned;
            return;
        }
        try self.merge_intents.put(self.alloc, intent.transition_id, owned);
    }

    pub fn removeSplitIntent(self: *TableManager, transition_id: u64) bool {
        if (self.split_intents.fetchRemove(transition_id)) |entry| {
            freeSplitIntent(self.alloc, entry.value);
            return true;
        }
        return false;
    }

    pub fn removeMergeIntent(self: *TableManager, transition_id: u64) bool {
        if (self.merge_intents.fetchRemove(transition_id)) |entry| {
            freeMergeIntent(self.alloc, entry.value);
            return true;
        }
        return false;
    }

    pub fn applyFinalizedSplit(self: *TableManager, record: transition_state.SplitTransitionRecord) !void {
        const split_key = record.split_key orelse return error.MissingSplitKey;
        const source = self.ranges.get(record.source_group_id) orelse return error.UnknownSourceRange;
        const identity_shard_id = rangeDocIdentityShardId(source);
        const identity_range_id = rangeDocIdentityRangeId(source);

        try self.upsertRange(.{
            .group_id = source.group_id,
            .range_id = source.range_id,
            .table_id = source.table_id,
            .start_key = source.start_key,
            .end_key = split_key,
            .doc_identity_shard_id = source.doc_identity_shard_id,
            .doc_identity_range_id = source.doc_identity_range_id,
        });
        try self.upsertRange(.{
            .group_id = record.destination_group_id,
            .range_id = record.destination_group_id,
            .table_id = source.table_id,
            .start_key = split_key,
            .end_key = record.source_range_end,
            .doc_identity_shard_id = identity_shard_id,
            .doc_identity_range_id = identity_range_id,
        });
        _ = self.removeSplitIntent(record.transition_id);
    }

    pub fn applyRolledBackSplit(self: *TableManager, transition_id: u64) void {
        _ = self.removeSplitIntent(transition_id);
    }

    pub fn applyFinalizedMerge(self: *TableManager, record: transition_state.MergeTransitionRecord) !void {
        const donor = self.ranges.get(record.donor_group_id) orelse return error.UnknownDonorRange;
        const receiver = self.ranges.get(record.receiver_group_id) orelse return error.UnknownReceiverRange;
        const merged_start = if (std.mem.order(u8, donor.start_key, receiver.start_key) == .lt) donor.start_key else receiver.start_key;
        const merged_end = switch (optionalBytesOrder(donor.end_key, receiver.end_key)) {
            .lt => receiver.end_key,
            .eq => receiver.end_key,
            .gt => donor.end_key,
        };

        try self.upsertRange(.{
            .group_id = receiver.group_id,
            .range_id = receiver.range_id,
            .table_id = receiver.table_id,
            .start_key = merged_start,
            .end_key = merged_end,
            .doc_identity_shard_id = receiver.doc_identity_shard_id,
            .doc_identity_range_id = receiver.doc_identity_range_id,
        });
        _ = self.removeRange(donor.group_id);
        _ = self.removeMergeIntent(record.transition_id);
    }

    pub fn applyRolledBackMerge(self: *TableManager, transition_id: u64) void {
        _ = self.removeMergeIntent(transition_id);
    }

    pub fn listDesiredSplitTransitions(self: *TableManager, alloc: std.mem.Allocator) ![]transition_state.SplitTransitionRecord {
        var out = std.ArrayListUnmanaged(transition_state.SplitTransitionRecord).empty;
        errdefer {
            for (out.items) |record| freeSplitTransitionRecord(alloc, record);
            out.deinit(alloc);
        }

        var it = self.split_intents.valueIterator();
        while (it.next()) |intent| {
            try out.append(alloc, try self.buildSplitTransition(intent.*));
        }
        return try out.toOwnedSlice(alloc);
    }

    pub fn listDesiredMergeTransitions(self: *TableManager, alloc: std.mem.Allocator) ![]transition_state.MergeTransitionRecord {
        var out = std.ArrayListUnmanaged(transition_state.MergeTransitionRecord).empty;
        errdefer {
            for (out.items) |record| freeMergeTransitionRecord(alloc, record);
            out.deinit(alloc);
        }

        var it = self.merge_intents.valueIterator();
        while (it.next()) |intent| {
            try out.append(alloc, try cloneMergeTransitionRecord(alloc, .{
                .transition_id = intent.transition_id,
                .donor_group_id = intent.donor_group_id,
                .receiver_group_id = intent.receiver_group_id,
                .phase = .prepare,
                .rollback_reason = intent.rollback_reason,
                .allow_doc_identity_reassignment = intent.allow_doc_identity_reassignment,
            }));
        }
        return try out.toOwnedSlice(alloc);
    }

    pub fn freeSplitTransitions(_: *TableManager, alloc: std.mem.Allocator, records: []transition_state.SplitTransitionRecord) void {
        for (records) |record| freeSplitTransitionRecord(alloc, record);
        alloc.free(records);
    }

    pub fn freeMergeTransitions(_: *TableManager, alloc: std.mem.Allocator, records: []transition_state.MergeTransitionRecord) void {
        for (records) |record| freeMergeTransitionRecord(alloc, record);
        alloc.free(records);
    }

    fn buildSplitTransition(self: *TableManager, intent: SplitIntent) !transition_state.SplitTransitionRecord {
        const source = self.ranges.get(intent.source_group_id) orelse return error.UnknownSourceRange;
        return try cloneSplitTransitionRecord(self.alloc, .{
            .transition_id = intent.transition_id,
            .source_group_id = intent.source_group_id,
            .destination_group_id = intent.destination_group_id,
            .phase = .prepare,
            .split_key = intent.split_key,
            .source_range_end = source.end_key,
            .rollback_reason = intent.rollback_reason,
        });
    }
};

pub fn parsePlacementClass(role: []const u8) ?PlacementClass {
    inline for (comptime std.meta.fields(PlacementClass)) |field| {
        if (std.mem.eql(u8, role, field.name)) return @enumFromInt(field.value);
    }
    return null;
}

pub fn placementRoleCompatible(table_role: []const u8, store_role: []const u8) bool {
    if (table_role.len == 0) return true;
    const table_class = parsePlacementClass(table_role) orelse return std.mem.eql(u8, table_role, store_role);
    const store_class = parsePlacementClass(store_role) orelse return std.mem.eql(u8, table_role, store_role);
    return table_class == store_class;
}

fn keyStrictlyInsideRange(key: []const u8, start_key: []const u8, end_key: ?[]const u8) bool {
    if (std.mem.order(u8, key, start_key) != .gt) return false;
    if (end_key) |end| {
        if (std.mem.order(u8, key, end) != .lt) return false;
    }
    return true;
}

fn rangesAdjacent(a: RangeRecord, b: RangeRecord) bool {
    if (a.end_key) |a_end| {
        if (std.mem.eql(u8, a_end, b.start_key)) return true;
    }
    if (b.end_key) |b_end| {
        if (std.mem.eql(u8, b_end, a.start_key)) return true;
    }
    return false;
}

fn optionalBytesOrder(a: ?[]const u8, b: ?[]const u8) std.math.Order {
    const a_bytes = a orelse return if (b == null) .eq else .gt;
    const b_bytes = b orelse return .lt;
    return std.mem.order(u8, a_bytes, b_bytes);
}

fn cloneOwnedOptional(alloc: std.mem.Allocator, value: ?[]const u8) !?[]const u8 {
    return if (value) |bytes| try alloc.dupe(u8, bytes) else null;
}

fn freeOwnedOptional(alloc: std.mem.Allocator, value: ?[]const u8) void {
    if (value) |bytes| alloc.free(bytes);
}

pub fn cloneTable(alloc: std.mem.Allocator, record: TableRecord) !TableRecord {
    const name = try alloc.dupe(u8, record.name);
    errdefer alloc.free(name);
    const description = try alloc.dupe(u8, record.description);
    errdefer alloc.free(description);
    const schema_json = try alloc.dupe(u8, record.schema_json);
    errdefer alloc.free(schema_json);
    const read_schema_json = try alloc.dupe(u8, record.read_schema_json);
    errdefer alloc.free(read_schema_json);
    const indexes_json = try alloc.dupe(u8, record.indexes_json);
    errdefer alloc.free(indexes_json);
    const replication_sources_json = try alloc.dupe(u8, record.replication_sources_json);
    errdefer alloc.free(replication_sources_json);
    const placement_role = try alloc.dupe(u8, record.placement_role);
    errdefer alloc.free(placement_role);
    const restore_backup_id = try alloc.dupe(u8, record.restore_backup_id);
    errdefer alloc.free(restore_backup_id);
    const restore_location = try alloc.dupe(u8, record.restore_location);
    errdefer alloc.free(restore_location);
    return .{
        .table_id = record.table_id,
        .name = name,
        .description = description,
        .schema_json = schema_json,
        .read_schema_json = read_schema_json,
        .indexes_json = indexes_json,
        .replication_sources_json = replication_sources_json,
        .placement_role = placement_role,
        .restore_backup_id = restore_backup_id,
        .restore_location = restore_location,
        .desired_replica_count = record.desired_replica_count,
        .min_ranges = record.min_ranges,
    };
}

pub fn freeTable(alloc: std.mem.Allocator, record: TableRecord) void {
    alloc.free(record.name);
    alloc.free(record.description);
    alloc.free(record.schema_json);
    alloc.free(record.read_schema_json);
    alloc.free(record.indexes_json);
    alloc.free(record.replication_sources_json);
    alloc.free(record.placement_role);
    alloc.free(record.restore_backup_id);
    alloc.free(record.restore_location);
}

pub fn cloneRange(alloc: std.mem.Allocator, record: RangeRecord) !RangeRecord {
    const start_key = try alloc.dupe(u8, record.start_key);
    errdefer alloc.free(start_key);
    const end_key = try cloneOwnedOptional(alloc, record.end_key);
    errdefer freeOwnedOptional(alloc, end_key);
    const restore_backup_id = try alloc.dupe(u8, record.restore_backup_id);
    errdefer alloc.free(restore_backup_id);
    const restore_location = try alloc.dupe(u8, record.restore_location);
    errdefer alloc.free(restore_location);
    const restore_snapshot_path = try alloc.dupe(u8, record.restore_snapshot_path);
    errdefer alloc.free(restore_snapshot_path);
    return .{
        .group_id = record.group_id,
        .range_id = if (record.range_id == 0) record.group_id else record.range_id,
        .table_id = record.table_id,
        .start_key = start_key,
        .end_key = end_key,
        .doc_identity_shard_id = record.doc_identity_shard_id,
        .doc_identity_range_id = record.doc_identity_range_id,
        .restore_backup_id = restore_backup_id,
        .restore_location = restore_location,
        .restore_snapshot_path = restore_snapshot_path,
    };
}

pub fn rangeDocIdentityShardId(record: RangeRecord) u64 {
    return if (record.doc_identity_shard_id == 0) record.group_id else record.doc_identity_shard_id;
}

pub fn rangeDocIdentityRangeId(record: RangeRecord) u64 {
    if (record.doc_identity_range_id != 0) return record.doc_identity_range_id;
    return if (record.range_id == 0) record.group_id else record.range_id;
}

pub fn freeRange(alloc: std.mem.Allocator, record: RangeRecord) void {
    alloc.free(record.start_key);
    freeOwnedOptional(alloc, record.end_key);
    alloc.free(record.restore_backup_id);
    alloc.free(record.restore_location);
    alloc.free(record.restore_snapshot_path);
}

pub fn cloneRestoreProgress(alloc: std.mem.Allocator, record: RestoreProgressRecord) !RestoreProgressRecord {
    const backup_id = try alloc.dupe(u8, record.backup_id);
    errdefer alloc.free(backup_id);
    const snapshot_path = try alloc.dupe(u8, record.snapshot_path);
    errdefer alloc.free(snapshot_path);
    const phase = try alloc.dupe(u8, record.phase);
    errdefer alloc.free(phase);
    const last_error = try alloc.dupe(u8, record.last_error);
    errdefer alloc.free(last_error);
    return .{
        .table_id = record.table_id,
        .node_id = record.node_id,
        .group_id = record.group_id,
        .backup_id = backup_id,
        .snapshot_path = snapshot_path,
        .primary_restored = record.primary_restored,
        .runtime_repair_complete = record.runtime_repair_complete,
        .phase = phase,
        .last_error = last_error,
        .updated_at_ms = record.updated_at_ms,
    };
}

pub fn freeRestoreProgress(alloc: std.mem.Allocator, record: RestoreProgressRecord) void {
    alloc.free(record.backup_id);
    alloc.free(record.snapshot_path);
    alloc.free(record.phase);
    alloc.free(record.last_error);
}

pub fn cloneReplicationSourceStatus(alloc: std.mem.Allocator, record: ReplicationSourceStatusRecord) !ReplicationSourceStatusRecord {
    const source_kind = try alloc.dupe(u8, record.source_kind);
    errdefer alloc.free(source_kind);
    const external_table = try alloc.dupe(u8, record.external_table);
    errdefer alloc.free(external_table);
    const cutover_mode = try alloc.dupe(u8, record.cutover_mode);
    errdefer alloc.free(cutover_mode);
    const slot_name = try alloc.dupe(u8, record.slot_name);
    errdefer alloc.free(slot_name);
    const publication_name = try alloc.dupe(u8, record.publication_name);
    errdefer alloc.free(publication_name);
    const phase = try alloc.dupe(u8, record.phase);
    errdefer alloc.free(phase);
    const checkpoint = try alloc.dupe(u8, record.checkpoint);
    errdefer alloc.free(checkpoint);
    const prepared_checkpoint = try alloc.dupe(u8, record.prepared_checkpoint);
    errdefer alloc.free(prepared_checkpoint);
    const stream_checkpoint = try alloc.dupe(u8, record.stream_checkpoint);
    errdefer alloc.free(stream_checkpoint);
    const last_error = try alloc.dupe(u8, record.last_error);
    errdefer alloc.free(last_error);
    const failure_class = try alloc.dupe(u8, record.failure_class);
    errdefer alloc.free(failure_class);
    return .{
        .table_id = record.table_id,
        .source_ordinal = record.source_ordinal,
        .source_kind = source_kind,
        .external_table = external_table,
        .cutover_mode = cutover_mode,
        .slot_name = slot_name,
        .publication_name = publication_name,
        .phase = phase,
        .checkpoint = checkpoint,
        .snapshot_offset = record.snapshot_offset,
        .prepared_checkpoint = prepared_checkpoint,
        .stream_checkpoint = stream_checkpoint,
        .last_error = last_error,
        .failure_class = failure_class,
        .lag_records = record.lag_records,
        .lag_millis = record.lag_millis,
        .consecutive_failures = record.consecutive_failures,
        .last_source_commit_at_ms = record.last_source_commit_at_ms,
        .last_success_at_ms = record.last_success_at_ms,
        .last_change_applied_at_ms = record.last_change_applied_at_ms,
        .updated_at_ms = record.updated_at_ms,
    };
}

pub fn freeReplicationSourceStatus(alloc: std.mem.Allocator, record: ReplicationSourceStatusRecord) void {
    alloc.free(record.source_kind);
    alloc.free(record.external_table);
    alloc.free(record.cutover_mode);
    alloc.free(record.slot_name);
    alloc.free(record.publication_name);
    alloc.free(record.phase);
    alloc.free(record.checkpoint);
    alloc.free(record.prepared_checkpoint);
    alloc.free(record.stream_checkpoint);
    alloc.free(record.last_error);
    alloc.free(record.failure_class);
}

pub fn cloneShuffleJoinLease(_: std.mem.Allocator, record: ShuffleJoinLeaseRecord) !ShuffleJoinLeaseRecord {
    return record;
}

pub fn freeShuffleJoinLease(_: std.mem.Allocator, _: ShuffleJoinLeaseRecord) void {}

pub fn cloneNode(alloc: std.mem.Allocator, record: NodeRecord) !NodeRecord {
    const role = try alloc.dupe(u8, record.role);
    errdefer alloc.free(role);
    return .{
        .node_id = record.node_id,
        .role = role,
        .lifecycle = try alloc.dupe(u8, record.lifecycle),
    };
}

pub fn freeNode(alloc: std.mem.Allocator, record: NodeRecord) void {
    alloc.free(record.role);
    alloc.free(record.lifecycle);
}

pub fn cloneStore(alloc: std.mem.Allocator, record: StoreRecord) !StoreRecord {
    const api_url = try alloc.dupe(u8, record.api_url);
    errdefer alloc.free(api_url);
    const raft_url = try alloc.dupe(u8, record.raft_url);
    errdefer alloc.free(raft_url);
    const role = try alloc.dupe(u8, record.role);
    errdefer alloc.free(role);
    const health_class = try alloc.dupe(u8, record.health_class);
    errdefer alloc.free(health_class);
    const failure_domain = try alloc.dupe(u8, record.failure_domain);
    errdefer alloc.free(failure_domain);
    const group_statuses = try cloneGroupStatuses(alloc, record.group_statuses);
    errdefer freeGroupStatuses(alloc, group_statuses);
    const runtime_statuses = try cloneRuntimeGroupStatusReports(alloc, record.runtime_statuses);
    errdefer freeRuntimeGroupStatusReports(alloc, runtime_statuses);
    return .{
        .store_id = record.store_id,
        .node_id = record.node_id,
        .api_url = api_url,
        .raft_url = raft_url,
        .role = role,
        .health_class = health_class,
        .failure_domain = failure_domain,
        .live = record.live,
        .drain_requested = record.drain_requested,
        .capacity_bytes = record.capacity_bytes,
        .available_bytes = record.available_bytes,
        .lease_pressure = record.lease_pressure,
        .read_load = record.read_load,
        .write_load = record.write_load,
        .active_backfills = record.active_backfills,
        .backfill_progress_millis = record.backfill_progress_millis,
        .group_statuses = group_statuses,
        .runtime_statuses = runtime_statuses,
    };
}

pub fn freeStore(alloc: std.mem.Allocator, record: StoreRecord) void {
    alloc.free(record.api_url);
    alloc.free(record.raft_url);
    alloc.free(record.role);
    alloc.free(record.health_class);
    alloc.free(record.failure_domain);
    freeGroupStatuses(alloc, record.group_statuses);
    freeRuntimeGroupStatusReports(alloc, record.runtime_statuses);
}

pub fn cloneGroupStatus(alloc: std.mem.Allocator, record: GroupStatusReport) !GroupStatusReport {
    _ = alloc;
    return .{
        .group_id = record.group_id,
        .doc_count = record.doc_count,
        .disk_bytes = record.disk_bytes,
        .empty = record.empty,
        .created_at_millis = record.created_at_millis,
        .updated_at_millis = record.updated_at_millis,
        .local_leader = record.local_leader,
        .local_voter = record.local_voter,
        .voter_count = record.voter_count,
        .joint_consensus = record.joint_consensus,
        .transition_pending = record.transition_pending,
        .replay_required = record.replay_required,
        .replay_caught_up = record.replay_caught_up,
        .cutover_ready = record.cutover_ready,
        .reads_ready_after_cutover = record.reads_ready_after_cutover,
    };
}

pub fn freeGroupStatus(alloc: std.mem.Allocator, record: GroupStatusReport) void {
    _ = alloc;
    _ = record;
}

pub fn cloneGroupStatuses(alloc: std.mem.Allocator, records: []const GroupStatusReport) ![]GroupStatusReport {
    const out = try alloc.alloc(GroupStatusReport, records.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |record| freeGroupStatus(alloc, record);
        if (out.len > 0) alloc.free(out);
    }
    for (records, 0..) |record, i| {
        out[i] = try cloneGroupStatus(alloc, record);
        initialized += 1;
    }
    return out;
}

pub fn freeGroupStatuses(alloc: std.mem.Allocator, records: []const GroupStatusReport) void {
    for (records) |record| freeGroupStatus(alloc, record);
    if (records.len > 0) alloc.free(records);
}

pub fn cloneRuntimeGroupStatusReport(alloc: std.mem.Allocator, record: RuntimeGroupStatusReport) !RuntimeGroupStatusReport {
    const table_name = try alloc.dupe(u8, record.table_name);
    errdefer alloc.free(table_name);
    const source = try alloc.dupe(u8, record.source);
    errdefer alloc.free(source);
    const freshness = try alloc.dupe(u8, record.freshness);
    errdefer alloc.free(freshness);
    const indexes = try cloneRuntimeIndexStatusReports(alloc, record.indexes);
    errdefer freeRuntimeIndexStatusReports(alloc, indexes);
    return .{
        .table_id = record.table_id,
        .table_name = table_name,
        .group_id = record.group_id,
        .store_id = record.store_id,
        .node_id = record.node_id,
        .updated_at_ns = record.updated_at_ns,
        .source = source,
        .freshness = freshness,
        .topology_generation = record.topology_generation,
        .lsm_root_generation = record.lsm_root_generation,
        .status_generation = record.status_generation,
        .doc_count = record.doc_count,
        .disk_bytes = record.disk_bytes,
        .created_at_millis = record.created_at_millis,
        .index_count = record.index_count,
        .enrichment_enabled = record.enrichment_enabled,
        .enrichment_target_sequence = record.enrichment_target_sequence,
        .enrichment_applied_sequence = record.enrichment_applied_sequence,
        .enrichment_retrying = record.enrichment_retrying,
        .enrichment_worker_failed = record.enrichment_worker_failed,
        .async_indexing_active = record.async_indexing_active,
        .async_startup_active = record.async_startup_active,
        .async_dense_catch_up_active = record.async_dense_catch_up_active,
        .async_bulk_coalescing_active = record.async_bulk_coalescing_active,
        .doc_identity = record.doc_identity,
        .doc_set_planning = record.doc_set_planning,
        .indexes = indexes,
    };
}

pub fn freeRuntimeGroupStatusReport(alloc: std.mem.Allocator, record: RuntimeGroupStatusReport) void {
    alloc.free(record.table_name);
    alloc.free(record.source);
    alloc.free(record.freshness);
    freeRuntimeIndexStatusReports(alloc, record.indexes);
}

pub fn cloneRuntimeGroupStatusReports(alloc: std.mem.Allocator, records: []const RuntimeGroupStatusReport) ![]RuntimeGroupStatusReport {
    const out = try alloc.alloc(RuntimeGroupStatusReport, records.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |record| freeRuntimeGroupStatusReport(alloc, record);
        if (out.len > 0) alloc.free(out);
    }
    for (records, 0..) |record, i| {
        out[i] = try cloneRuntimeGroupStatusReport(alloc, record);
        initialized += 1;
    }
    return out;
}

pub fn freeRuntimeGroupStatusReports(alloc: std.mem.Allocator, records: []const RuntimeGroupStatusReport) void {
    for (records) |record| freeRuntimeGroupStatusReport(alloc, record);
    if (records.len > 0) alloc.free(records);
}

pub fn cloneRuntimeIndexStatusReport(alloc: std.mem.Allocator, record: RuntimeIndexStatusReport) !RuntimeIndexStatusReport {
    const name = try alloc.dupe(u8, record.name);
    errdefer alloc.free(name);
    const kind = try alloc.dupe(u8, record.kind);
    errdefer alloc.free(kind);
    return .{
        .name = name,
        .kind = kind,
        .doc_count = record.doc_count,
        .term_count = record.term_count,
        .edge_count = record.edge_count,
        .node_count = record.node_count,
        .root_node = record.root_node,
        .backfill_active = record.backfill_active,
        .backfill_progress_millis = record.backfill_progress_millis,
        .replay_applied_sequence = record.replay_applied_sequence,
        .replay_target_sequence = record.replay_target_sequence,
        .replay_catch_up_required = record.replay_catch_up_required,
    };
}

pub fn freeRuntimeIndexStatusReport(alloc: std.mem.Allocator, record: RuntimeIndexStatusReport) void {
    alloc.free(record.name);
    alloc.free(record.kind);
}

pub fn cloneRuntimeIndexStatusReports(alloc: std.mem.Allocator, records: []const RuntimeIndexStatusReport) ![]RuntimeIndexStatusReport {
    const out = try alloc.alloc(RuntimeIndexStatusReport, records.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |record| freeRuntimeIndexStatusReport(alloc, record);
        if (out.len > 0) alloc.free(out);
    }
    for (records, 0..) |record, i| {
        out[i] = try cloneRuntimeIndexStatusReport(alloc, record);
        initialized += 1;
    }
    return out;
}

pub fn freeRuntimeIndexStatusReports(alloc: std.mem.Allocator, records: []const RuntimeIndexStatusReport) void {
    for (records) |record| freeRuntimeIndexStatusReport(alloc, record);
    if (records.len > 0) alloc.free(records);
}

fn cloneSplitIntent(alloc: std.mem.Allocator, intent: SplitIntent) !SplitIntent {
    return .{
        .transition_id = intent.transition_id,
        .table_id = intent.table_id,
        .source_group_id = intent.source_group_id,
        .destination_group_id = intent.destination_group_id,
        .split_key = try alloc.dupe(u8, intent.split_key),
        .rollback_reason = try cloneOwnedOptional(alloc, intent.rollback_reason),
        .automatic = intent.automatic,
    };
}

fn freeSplitIntent(alloc: std.mem.Allocator, intent: SplitIntent) void {
    alloc.free(intent.split_key);
    freeOwnedOptional(alloc, intent.rollback_reason);
}

fn cloneMergeIntent(alloc: std.mem.Allocator, intent: MergeIntent) !MergeIntent {
    return .{
        .transition_id = intent.transition_id,
        .table_id = intent.table_id,
        .donor_group_id = intent.donor_group_id,
        .receiver_group_id = intent.receiver_group_id,
        .rollback_reason = try cloneOwnedOptional(alloc, intent.rollback_reason),
        .automatic = intent.automatic,
        .allow_doc_identity_reassignment = intent.allow_doc_identity_reassignment,
    };
}

fn freeMergeIntent(alloc: std.mem.Allocator, intent: MergeIntent) void {
    freeOwnedOptional(alloc, intent.rollback_reason);
}

fn cloneSplitTransitionRecord(alloc: std.mem.Allocator, record: transition_state.SplitTransitionRecord) !transition_state.SplitTransitionRecord {
    const split_key = try cloneOwnedOptional(alloc, record.split_key);
    errdefer freeOwnedOptional(alloc, split_key);
    const source_range_end = try cloneOwnedOptional(alloc, record.source_range_end);
    errdefer freeOwnedOptional(alloc, source_range_end);
    const rollback_reason = try cloneOwnedOptional(alloc, record.rollback_reason);
    errdefer freeOwnedOptional(alloc, rollback_reason);
    return .{
        .transition_id = record.transition_id,
        .source_group_id = record.source_group_id,
        .destination_group_id = record.destination_group_id,
        .phase = record.phase,
        .split_key = split_key,
        .source_range_end = source_range_end,
        .rollback_reason = rollback_reason,
    };
}

pub fn freeSplitTransitionRecord(alloc: std.mem.Allocator, record: transition_state.SplitTransitionRecord) void {
    freeOwnedOptional(alloc, record.split_key);
    freeOwnedOptional(alloc, record.source_range_end);
    freeOwnedOptional(alloc, record.rollback_reason);
}

fn cloneMergeTransitionRecord(alloc: std.mem.Allocator, record: transition_state.MergeTransitionRecord) !transition_state.MergeTransitionRecord {
    const rollback_reason = try cloneOwnedOptional(alloc, record.rollback_reason);
    errdefer freeOwnedOptional(alloc, rollback_reason);
    return .{
        .transition_id = record.transition_id,
        .donor_group_id = record.donor_group_id,
        .receiver_group_id = record.receiver_group_id,
        .phase = record.phase,
        .rollback_reason = rollback_reason,
        .allow_doc_identity_reassignment = record.allow_doc_identity_reassignment,
    };
}

pub fn freeMergeTransitionRecord(alloc: std.mem.Allocator, record: transition_state.MergeTransitionRecord) void {
    freeOwnedOptional(alloc, record.rollback_reason);
}

test "table manager validates split and merge intents" {
    var manager = TableManager.init(std.testing.allocator);
    defer manager.deinit();

    try manager.upsertTable(.{
        .table_id = 10,
        .name = "docs",
    });
    try manager.upsertRange(.{
        .group_id = 101,
        .table_id = 10,
        .start_key = "doc:a",
        .end_key = "doc:m",
    });
    try manager.upsertRange(.{
        .group_id = 102,
        .table_id = 10,
        .start_key = "doc:m",
        .end_key = "doc:z",
    });

    try manager.requestSplit(.{
        .transition_id = 5001,
        .table_id = 10,
        .source_group_id = 101,
        .destination_group_id = 103,
        .split_key = "doc:h",
    });

    const splits = try manager.listDesiredSplitTransitions(std.testing.allocator);
    defer manager.freeSplitTransitions(std.testing.allocator, splits);
    try std.testing.expectEqual(@as(usize, 1), splits.len);
    try std.testing.expectEqualStrings("doc:h", splits[0].split_key.?);
    try std.testing.expectEqualStrings("doc:m", splits[0].source_range_end.?);

    try manager.requestMerge(.{
        .transition_id = 6001,
        .table_id = 10,
        .donor_group_id = 102,
        .receiver_group_id = 101,
        .allow_doc_identity_reassignment = true,
    });

    const merges = try manager.listDesiredMergeTransitions(std.testing.allocator);
    defer manager.freeMergeTransitions(std.testing.allocator, merges);
    try std.testing.expectEqual(@as(usize, 1), merges.len);
    try std.testing.expectEqual(@as(u64, 102), merges[0].donor_group_id);
    try std.testing.expectEqual(@as(u64, 101), merges[0].receiver_group_id);
    try std.testing.expect(merges[0].allow_doc_identity_reassignment);
}

test "table manager applies finalized split to desired topology" {
    var manager = TableManager.init(std.testing.allocator);
    defer manager.deinit();

    try manager.upsertTable(.{
        .table_id = 10,
        .name = "docs",
    });
    try manager.upsertRange(.{
        .group_id = 101,
        .table_id = 10,
        .start_key = "doc:a",
        .end_key = "doc:z",
    });
    try manager.requestSplit(.{
        .transition_id = 5003,
        .table_id = 10,
        .source_group_id = 101,
        .destination_group_id = 102,
        .split_key = "doc:m",
    });

    try manager.applyFinalizedSplit(.{
        .transition_id = 5003,
        .source_group_id = 101,
        .destination_group_id = 102,
        .phase = .finalized,
        .split_key = "doc:m",
        .source_range_end = "doc:z",
    });

    const ranges = try manager.listRanges(std.testing.allocator);
    defer manager.freeRanges(std.testing.allocator, ranges);
    try std.testing.expectEqual(@as(usize, 2), ranges.len);
    for (ranges) |range| {
        if (range.group_id == 101) try std.testing.expectEqual(@as(u64, 101), range.range_id);
        if (range.group_id == 102) {
            try std.testing.expectEqual(@as(u64, 102), range.range_id);
            try std.testing.expectEqual(@as(u64, 101), range.doc_identity_shard_id);
            try std.testing.expectEqual(@as(u64, 101), range.doc_identity_range_id);
        }
    }
    try std.testing.expect(manager.split_intents.count() == 0);
}

test "table manager applies finalized merge preserving receiver range id" {
    var manager = TableManager.init(std.testing.allocator);
    defer manager.deinit();

    try manager.upsertTable(.{
        .table_id = 10,
        .name = "docs",
    });
    try manager.upsertRange(.{
        .group_id = 101,
        .range_id = 1001,
        .table_id = 10,
        .start_key = "doc:a",
        .end_key = "doc:m",
    });
    try manager.upsertRange(.{
        .group_id = 102,
        .range_id = 1002,
        .table_id = 10,
        .start_key = "doc:m",
        .end_key = "doc:z",
    });
    try manager.requestMerge(.{
        .transition_id = 6003,
        .table_id = 10,
        .donor_group_id = 102,
        .receiver_group_id = 101,
    });

    try manager.applyFinalizedMerge(.{
        .transition_id = 6003,
        .donor_group_id = 102,
        .receiver_group_id = 101,
        .phase = .finalized,
    });

    const ranges = try manager.listRanges(std.testing.allocator);
    defer manager.freeRanges(std.testing.allocator, ranges);
    try std.testing.expectEqual(@as(usize, 1), ranges.len);
    try std.testing.expectEqual(@as(u64, 101), ranges[0].group_id);
    try std.testing.expectEqual(@as(u64, 1001), ranges[0].range_id);
    try std.testing.expectEqual(@as(u64, 0), ranges[0].doc_identity_shard_id);
    try std.testing.expectEqual(@as(u64, 0), ranges[0].doc_identity_range_id);
    try std.testing.expectEqualStrings("doc:a", ranges[0].start_key);
    try std.testing.expectEqualStrings("doc:z", ranges[0].end_key.?);
    try std.testing.expect(manager.merge_intents.count() == 0);
}

test "table manager rejects invalid split key" {
    var manager = TableManager.init(std.testing.allocator);
    defer manager.deinit();

    try manager.upsertTable(.{
        .table_id = 10,
        .name = "docs",
    });
    try manager.upsertRange(.{
        .group_id = 101,
        .table_id = 10,
        .start_key = "doc:a",
        .end_key = "doc:m",
    });

    try std.testing.expectError(error.InvalidSplitKey, manager.requestSplit(.{
        .transition_id = 5002,
        .table_id = 10,
        .source_group_id = 101,
        .destination_group_id = 102,
        .split_key = "doc:m",
    }));
}

test "table manager can replace topology from projected state" {
    var manager = TableManager.init(std.testing.allocator);
    defer manager.deinit();

    try manager.upsertTable(.{ .table_id = 1, .name = "old" });
    try manager.upsertRange(.{
        .group_id = 11,
        .table_id = 1,
        .start_key = "a",
        .end_key = "m",
    });

    const tables = [_]TableRecord{
        .{ .table_id = 2, .name = "new" },
    };
    const ranges = [_]RangeRecord{
        .{ .group_id = 21, .table_id = 2, .start_key = "doc:a", .end_key = "doc:z" },
    };
    try manager.replaceTopology(&tables, &ranges);

    const listed_tables = try manager.listTables(std.testing.allocator);
    defer manager.freeTables(std.testing.allocator, listed_tables);
    const listed_ranges = try manager.listRanges(std.testing.allocator);
    defer manager.freeRanges(std.testing.allocator, listed_ranges);

    try std.testing.expectEqual(@as(usize, 1), listed_tables.len);
    try std.testing.expectEqual(@as(u64, 2), listed_tables[0].table_id);
    try std.testing.expectEqualStrings("new", listed_tables[0].name);
    try std.testing.expectEqual(@as(usize, 1), listed_ranges.len);
    try std.testing.expectEqual(@as(u64, 21), listed_ranges[0].group_id);
}

test "table manager parses placement classes and checks compatibility" {
    try std.testing.expectEqual(PlacementClass.serving, parsePlacementClass("serving").?);
    try std.testing.expectEqual(PlacementClass.bulk, parsePlacementClass("bulk").?);
    try std.testing.expect(parsePlacementClass("custom") == null);

    try std.testing.expect(placementRoleCompatible("serving", "serving"));
    try std.testing.expect(!placementRoleCompatible("serving", "bulk"));
    try std.testing.expect(placementRoleCompatible("custom", "custom"));
    try std.testing.expect(!placementRoleCompatible("custom", "archive"));
}
