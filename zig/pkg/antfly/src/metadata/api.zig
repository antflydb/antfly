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
const metadata_state = @import("state.zig");
const metadata_reconciler = @import("reconciler.zig");
const table_manager = @import("table_manager.zig");
const raft_host = @import("../raft/host.zig");
const raft_reconciler = @import("../raft/reconciler.zig");
const raft_service = @import("../raft/service.zig");
const transition_state = @import("transition_state.zig");

pub const MetadataStatus = struct {
    metadata_group_id: u64,
    metadata_epoch: u64 = 0,
    metadata_raft_local_node_id: u64 = 0,
    metadata_raft_role: []const u8 = "absent",
    metadata_raft_leader_id: ?u64 = null,
    metadata_raft_term: u64 = 0,
    metadata_raft_commit_index: u64 = 0,
    metadata_raft_local_voter: bool = false,
    metadata_raft_voter_count: usize = 0,
    metadata_raft_election_elapsed: u32 = 0,
    metadata_raft_randomized_election_timeout: u32 = 0,
    metadata_raft_votes_granted: usize = 0,
    metadata_raft_votes_rejected: usize = 0,
    metadata_raft_votes_unknown: usize = 0,
    metadata_raft_inbound_message_enqueues: usize = 0,
    metadata_raft_inbound_message_drains: usize = 0,
    metadata_raft_pending_inbound_messages: usize = 0,
    metadata_raft_transport_sent_frames: usize = 0,
    metadata_raft_transport_send_failures: usize = 0,
    metadata_raft_transport_retries_scheduled: usize = 0,
    metadata_raft_transport_retries_exhausted: usize = 0,
    metadata_raft_transport_retried_successes: usize = 0,
    metadata_raft_transport_peer_refreshes: usize = 0,
    metadata_raft_transport_peer_routes: usize = 0,
    metadata_raft_transport_served_groups: usize = 0,
    metadata_raft_transport_pending_retries: usize = 0,
    metrics: raft_service.ManagedServiceMetrics,
    reconcile_lease_enabled: bool = false,
    reconcile_lease_owner_node_id: u64 = 0,
    reconcile_lease_expires_at_ms: u64 = 0,
    reconcile_lease_held_by_local: bool = false,
    reconcile_lease_acquisition_count: u64 = 0,
    reconcile_lease_acquire_failures: u64 = 0,
    reconcile_lease_lost_leases: u64 = 0,
    reconcile_lease_last_acquired_ms: u64 = 0,
    projected_tables: usize = 0,
    projected_tables_with_replication_sources: usize = 0,
    projected_replication_sources: usize = 0,
    projected_replication_source_statuses: usize = 0,
    projected_replication_source_statuses_exact_cutover: usize = 0,
    projected_replication_source_statuses_non_exact_cutover: usize = 0,
    projected_replication_source_statuses_reseed_recommended: usize = 0,
    projected_replication_source_statuses_exported_snapshot: usize = 0,
    projected_replication_source_statuses_slot_first: usize = 0,
    projected_replication_source_statuses_slot_resumed: usize = 0,
    projected_replication_source_statuses_snapshot: usize = 0,
    projected_replication_source_statuses_cutover_prepared: usize = 0,
    projected_replication_source_statuses_streaming: usize = 0,
    projected_replication_source_statuses_failed: usize = 0,
    projected_replication_source_statuses_with_last_error: usize = 0,
    projected_replication_source_statuses_slot_missing_failed: usize = 0,
    projected_replication_source_statuses_retryable_failed: usize = 0,
    projected_replication_source_statuses_terminal_failed: usize = 0,
    projected_replication_source_statuses_with_consecutive_failures: usize = 0,
    projected_replication_source_statuses_with_success_timestamp: usize = 0,
    projected_replication_source_statuses_with_change_timestamp: usize = 0,
    projected_replication_source_consecutive_failures_total: u64 = 0,
    projected_replication_source_consecutive_failures_max: u64 = 0,
    projected_replication_source_lag_records_total: u64 = 0,
    projected_replication_source_lag_records_max: u64 = 0,
    projected_replication_source_lag_millis_total: u64 = 0,
    projected_replication_source_lag_millis_max: u64 = 0,
    projected_replication_source_observed_lag_millis_total: u64 = 0,
    projected_replication_source_observed_lag_millis_max: u64 = 0,
    projected_replication_source_statuses_with_source_commit_timestamp: usize = 0,
    projected_replication_source_last_success_at_ms_max: u64 = 0,
    projected_replication_source_last_source_commit_at_ms_max: u64 = 0,
    projected_replication_source_last_change_applied_at_ms_max: u64 = 0,
    projected_ranges: usize = 0,
    projected_stores: usize = 0,
    projected_placement_intents: usize = 0,
    projected_snapshot_bootstrap_intents: usize = 0,
    projected_backup_restore_bootstrap_intents: usize = 0,
    projected_shuffle_join_leases: usize = 0,
    projected_restore_progress: usize = 0,
    projected_split_transitions: usize = 0,
    projected_merge_transitions: usize = 0,
    projected_doc_identity_lifecycle_unknown: usize = 0,
    projected_doc_identity_lifecycle_preserving: usize = 0,
    projected_doc_identity_lifecycle_reassigning: usize = 0,
    projected_doc_identity_lifecycle_rebuild_required: usize = 0,
    projected_doc_identity_lifecycle_ready: usize = 0,
    preferred_stores: usize = 0,
    constrained_stores: usize = 0,
    overloaded_stores: usize = 0,
    excluded_stores: usize = 0,
    backfill_stores: usize = 0,
    active_backfills: usize = 0,
    placement_upserts: usize = 0,
    placement_removals: usize = 0,
    repair_placement_groups: usize = 0,
    rebalance_placement_groups: usize = 0,
};

pub const MetadataHead = struct {
    metadata_group_id: u64,
    metadata_epoch: u64 = 0,
};

pub const ReplicationSourceActionHint = struct {
    table_id: u64,
    table_name: []u8,
    source_ordinal: u32,
    action: []const u8,
    reason: []const u8,
    reseed_exact_cutover_path: []u8,
};

pub const AdminSnapshot = struct {
    status: MetadataStatus,
    tables: []table_manager.TableRecord,
    ranges: []table_manager.RangeRecord,
    nodes: []table_manager.NodeRecord = &.{},
    stores: []table_manager.StoreRecord,
    placement_intents: []raft_reconciler.PlacementIntent,
    shuffle_join_leases: []table_manager.ShuffleJoinLeaseRecord = &.{},
    local_bootstrap_statuses: []raft_host.BootstrapStatus = &.{},
    restore_progresses: []table_manager.RestoreProgressRecord = &.{},
    replication_source_statuses: []table_manager.ReplicationSourceStatusRecord = &.{},
    replication_source_action_hints: []ReplicationSourceActionHint = &.{},
    split_transitions: []transition_state.SplitTransitionRecord,
    merge_transitions: []transition_state.MergeTransitionRecord,
    split_observations: []transition_state.SplitObservationRecord = &.{},
    merge_observations: []transition_state.MergeObservationRecord = &.{},
    merged_group_statuses: []metadata_reconciler.MergedGroupStatus = &.{},
};

pub fn captureSnapshot(alloc: std.mem.Allocator, source: anytype) !AdminSnapshot {
    const SourceType = @TypeOf(source);
    const SourceDeclType = switch (@typeInfo(SourceType)) {
        .pointer => |pointer| pointer.child,
        else => SourceType,
    };
    var snapshot: AdminSnapshot = .{
        .status = if (@hasDecl(SourceDeclType, "metadataStatus")) try source.metadataStatus() else try source.status(),
        .tables = &.{},
        .ranges = &.{},
        .stores = &.{},
        .placement_intents = &.{},
        .split_transitions = &.{},
        .merge_transitions = &.{},
    };
    errdefer freeSnapshot(alloc, source, &snapshot);
    snapshot.tables = try source.listProjectedTables(alloc);
    snapshot.ranges = try source.listProjectedRanges(alloc);
    if (@hasDecl(SourceDeclType, "listProjectedNodes")) {
        snapshot.nodes = try source.listProjectedNodes(alloc);
    }
    snapshot.stores = try source.listProjectedStores(alloc);
    snapshot.placement_intents = try source.listProjectedPlacementIntents(alloc);
    if (@hasDecl(SourceDeclType, "listProjectedShuffleJoinLeases")) {
        snapshot.shuffle_join_leases = try source.listProjectedShuffleJoinLeases(alloc);
    }
    if (@hasDecl(SourceDeclType, "listLocalBootstrapStatuses")) {
        snapshot.local_bootstrap_statuses = try source.listLocalBootstrapStatuses(alloc);
    }
    if (@hasDecl(SourceDeclType, "listProjectedRestoreProgress")) {
        snapshot.restore_progresses = try source.listProjectedRestoreProgress(alloc);
    }
    if (@hasDecl(SourceDeclType, "listProjectedReplicationSourceStatuses")) {
        snapshot.replication_source_statuses = try source.listProjectedReplicationSourceStatuses(alloc);
    }
    snapshot.replication_source_action_hints = try deriveReplicationSourceActionHints(alloc, snapshot.tables, snapshot.replication_source_statuses);
    snapshot.split_transitions = try source.listProjectedSplitTransitions(alloc);
    snapshot.merge_transitions = try source.listProjectedMergeTransitions(alloc);
    if (@hasDecl(SourceDeclType, "observeSplitTransition")) {
        snapshot.split_observations = try captureSplitObservations(alloc, source, snapshot.split_transitions);
    }
    if (@hasDecl(SourceDeclType, "observeMergeTransition")) {
        snapshot.merge_observations = try captureMergeObservations(alloc, source, snapshot.merge_transitions);
    }
    const merged_split_observations = try cloneSplitRuntimeObservations(alloc, snapshot.split_observations);
    defer if (merged_split_observations.len > 0) alloc.free(merged_split_observations);
    const merged_merge_observations = try cloneMergeRuntimeObservations(alloc, snapshot.merge_observations);
    defer if (merged_merge_observations.len > 0) alloc.free(merged_merge_observations);
    snapshot.merged_group_statuses = try metadata_state.mergeHealthyGroupStatuses(
        alloc,
        snapshot.tables,
        snapshot.ranges,
        snapshot.placement_intents,
        snapshot.restore_progresses,
        snapshot.stores,
        snapshot.split_transitions,
        snapshot.merge_transitions,
        merged_split_observations,
        merged_merge_observations,
    );
    return snapshot;
}

pub fn freeSnapshot(alloc: std.mem.Allocator, source: anytype, snapshot: *AdminSnapshot) void {
    const SourceType = @TypeOf(source);
    const SourceDeclType = switch (@typeInfo(SourceType)) {
        .pointer => |pointer| pointer.child,
        else => SourceType,
    };
    source.freeProjectedTables(alloc, snapshot.tables);
    source.freeProjectedRanges(alloc, snapshot.ranges);
    if (@hasDecl(SourceDeclType, "freeProjectedNodes") and snapshot.nodes.len > 0) {
        source.freeProjectedNodes(alloc, snapshot.nodes);
    }
    source.freeProjectedStores(alloc, snapshot.stores);
    source.freeProjectedPlacementIntents(alloc, snapshot.placement_intents);
    if (@hasDecl(SourceDeclType, "freeProjectedShuffleJoinLeases") and snapshot.shuffle_join_leases.len > 0) {
        source.freeProjectedShuffleJoinLeases(alloc, snapshot.shuffle_join_leases);
    }
    if (@hasDecl(SourceDeclType, "freeLocalBootstrapStatuses") and snapshot.local_bootstrap_statuses.len > 0) {
        source.freeLocalBootstrapStatuses(alloc, snapshot.local_bootstrap_statuses);
    }
    if (@hasDecl(SourceDeclType, "freeProjectedRestoreProgress") and snapshot.restore_progresses.len > 0) {
        source.freeProjectedRestoreProgress(alloc, snapshot.restore_progresses);
    }
    if (@hasDecl(SourceDeclType, "freeProjectedReplicationSourceStatuses") and snapshot.replication_source_statuses.len > 0) {
        source.freeProjectedReplicationSourceStatuses(alloc, snapshot.replication_source_statuses);
    }
    freeReplicationSourceActionHints(alloc, snapshot.replication_source_action_hints);
    source.freeProjectedSplitTransitions(alloc, snapshot.split_transitions);
    source.freeProjectedMergeTransitions(alloc, snapshot.merge_transitions);
    if (snapshot.split_observations.len > 0) alloc.free(snapshot.split_observations);
    if (snapshot.merge_observations.len > 0) alloc.free(snapshot.merge_observations);
    if (snapshot.merged_group_statuses.len > 0) metadata_state.freeMergedGroupStatuses(alloc, snapshot.merged_group_statuses);
    snapshot.* = undefined;
}

pub fn deriveReplicationSourceActionHints(
    alloc: std.mem.Allocator,
    tables: []const table_manager.TableRecord,
    statuses: []const table_manager.ReplicationSourceStatusRecord,
) ![]ReplicationSourceActionHint {
    var out = std.ArrayListUnmanaged(ReplicationSourceActionHint).empty;
    errdefer freeReplicationSourceActionHints(alloc, out.items);
    for (statuses) |status| {
        const reason = replicationSourceReseedReason(status) orelse continue;
        const table_name = findTableNameById(tables, status.table_id) orelse continue;
        const path = try std.fmt.allocPrint(
            alloc,
            "/internal/v1/tables/{s}/replication-sources/{d}/reseed-exact-cutover",
            .{ table_name, status.source_ordinal },
        );
        errdefer alloc.free(path);
        try out.append(alloc, .{
            .table_id = status.table_id,
            .table_name = try alloc.dupe(u8, table_name),
            .source_ordinal = status.source_ordinal,
            .action = "reseed_exact_cutover",
            .reason = reason,
            .reseed_exact_cutover_path = path,
        });
    }
    return try out.toOwnedSlice(alloc);
}

fn freeReplicationSourceActionHints(alloc: std.mem.Allocator, hints: []ReplicationSourceActionHint) void {
    for (hints) |hint| {
        alloc.free(hint.table_name);
        alloc.free(hint.reseed_exact_cutover_path);
    }
    if (hints.len > 0) alloc.free(hints);
}

fn replicationSourceReseedReason(status: table_manager.ReplicationSourceStatusRecord) ?[]const u8 {
    if (!std.mem.eql(u8, status.source_kind, "postgres")) return null;
    if (std.mem.eql(u8, status.cutover_mode, "slot_resumed")) return "existing_slot_non_exact_cutover";
    if (std.mem.eql(u8, status.last_error, "ReplicationExactCutoverRequired")) return "exact_cutover_required_rejected";
    return null;
}

fn findTableNameById(tables: []const table_manager.TableRecord, table_id: u64) ?[]const u8 {
    for (tables) |table| {
        if (table.table_id == table_id) return table.name;
    }
    return null;
}

pub fn captureSplitObservations(
    alloc: std.mem.Allocator,
    source: anytype,
    split_transitions: []const transition_state.SplitTransitionRecord,
) ![]transition_state.SplitObservationRecord {
    var out = std.ArrayListUnmanaged(transition_state.SplitObservationRecord).empty;
    errdefer out.deinit(alloc);
    for (split_transitions) |record| {
        const observation = (source.observeSplitTransition(record.transition_id) catch |err| {
            std.log.warn("split transition snapshot observation failed transition_id={d} err={s}", .{ record.transition_id, @errorName(err) });
            continue;
        }) orelse continue;
        try out.append(alloc, .{
            .transition_id = record.transition_id,
            .observation = observation,
        });
    }
    return try out.toOwnedSlice(alloc);
}

pub fn captureMergeObservations(
    alloc: std.mem.Allocator,
    source: anytype,
    merge_transitions: []const transition_state.MergeTransitionRecord,
) ![]transition_state.MergeObservationRecord {
    var out = std.ArrayListUnmanaged(transition_state.MergeObservationRecord).empty;
    errdefer out.deinit(alloc);
    for (merge_transitions) |record| {
        const observation = (source.observeMergeTransition(record.transition_id) catch |err| {
            std.log.warn("merge transition snapshot observation failed transition_id={d} err={s}", .{ record.transition_id, @errorName(err) });
            continue;
        }) orelse continue;
        try out.append(alloc, .{
            .transition_id = record.transition_id,
            .observation = observation,
        });
    }
    return try out.toOwnedSlice(alloc);
}

fn cloneSplitRuntimeObservations(
    alloc: std.mem.Allocator,
    records: []const transition_state.SplitObservationRecord,
) ![]metadata_reconciler.SplitRuntimeObservation {
    const out = try alloc.alloc(metadata_reconciler.SplitRuntimeObservation, records.len);
    errdefer alloc.free(out);
    for (records, 0..) |record, i| {
        out[i] = .{
            .transition_id = record.transition_id,
            .observation = record.observation,
        };
    }
    return out;
}

fn cloneMergeRuntimeObservations(
    alloc: std.mem.Allocator,
    records: []const transition_state.MergeObservationRecord,
) ![]metadata_reconciler.MergeRuntimeObservation {
    const out = try alloc.alloc(metadata_reconciler.MergeRuntimeObservation, records.len);
    errdefer alloc.free(out);
    for (records, 0..) |record, i| {
        out[i] = .{
            .transition_id = record.transition_id,
            .observation = record.observation,
        };
    }
    return out;
}

test "metadata admin snapshot captures projected metadata state" {
    const FakeSource = struct {
        fn status(_: @This()) !MetadataStatus {
            return .{
                .metadata_group_id = 77,
                .metrics = .{},
                .projected_tables = 1,
                .projected_tables_with_replication_sources = 1,
                .projected_replication_sources = 2,
                .projected_replication_source_statuses = 1,
                .projected_replication_source_statuses_exact_cutover = 1,
                .projected_replication_source_statuses_non_exact_cutover = 0,
                .projected_replication_source_statuses_reseed_recommended = 0,
                .projected_replication_source_statuses_exported_snapshot = 1,
                .projected_replication_source_statuses_slot_first = 0,
                .projected_replication_source_statuses_slot_resumed = 0,
                .projected_replication_source_statuses_snapshot = 1,
                .projected_replication_source_statuses_cutover_prepared = 0,
                .projected_replication_source_statuses_streaming = 0,
                .projected_replication_source_statuses_failed = 0,
                .projected_replication_source_statuses_with_last_error = 0,
                .projected_replication_source_statuses_slot_missing_failed = 0,
                .projected_replication_source_statuses_retryable_failed = 0,
                .projected_replication_source_statuses_terminal_failed = 0,
                .projected_replication_source_statuses_with_consecutive_failures = 1,
                .projected_replication_source_statuses_with_success_timestamp = 1,
                .projected_replication_source_statuses_with_change_timestamp = 1,
                .projected_replication_source_consecutive_failures_total = 2,
                .projected_replication_source_consecutive_failures_max = 2,
                .projected_replication_source_lag_records_total = 12,
                .projected_replication_source_lag_records_max = 12,
                .projected_replication_source_lag_millis_total = 34,
                .projected_replication_source_lag_millis_max = 34,
                .projected_replication_source_observed_lag_millis_total = 56,
                .projected_replication_source_observed_lag_millis_max = 56,
                .projected_replication_source_statuses_with_source_commit_timestamp = 1,
                .projected_replication_source_last_success_at_ms_max = 1234,
                .projected_replication_source_last_source_commit_at_ms_max = 1200,
                .projected_replication_source_last_change_applied_at_ms_max = 1235,
                .projected_ranges = 1,
                .projected_stores = 1,
                .projected_placement_intents = 1,
                .projected_restore_progress = 1,
                .projected_split_transitions = 1,
                .projected_merge_transitions = 1,
            };
        }

        fn listProjectedTables(_: @This(), alloc: std.mem.Allocator) ![]table_manager.TableRecord {
            const records = try alloc.alloc(table_manager.TableRecord, 1);
            records[0] = .{
                .table_id = 1,
                .name = try alloc.dupe(u8, "docs"),
                .replication_sources_json = try alloc.dupe(u8, "[{\"type\":\"postgres\"},{\"type\":\"postgres\"}]"),
                .placement_role = try alloc.dupe(u8, "data"),
            };
            return records;
        }

        fn freeProjectedTables(_: @This(), alloc: std.mem.Allocator, records: []table_manager.TableRecord) void {
            for (records) |record| {
                alloc.free(record.name);
                alloc.free(record.replication_sources_json);
                alloc.free(record.placement_role);
            }
            alloc.free(records);
        }

        fn listProjectedRanges(_: @This(), alloc: std.mem.Allocator) ![]table_manager.RangeRecord {
            const records = try alloc.alloc(table_manager.RangeRecord, 1);
            records[0] = .{ .group_id = 10, .table_id = 1, .start_key = try alloc.dupe(u8, "doc:a"), .end_key = try alloc.dupe(u8, "doc:z") };
            return records;
        }

        fn freeProjectedRanges(_: @This(), alloc: std.mem.Allocator, records: []table_manager.RangeRecord) void {
            for (records) |record| {
                alloc.free(record.start_key);
                if (record.end_key) |end_key| alloc.free(end_key);
            }
            alloc.free(records);
        }

        fn listProjectedStores(_: @This(), alloc: std.mem.Allocator) ![]table_manager.StoreRecord {
            const records = try alloc.alloc(table_manager.StoreRecord, 1);
            const group_statuses = try alloc.alloc(table_manager.GroupStatusReport, 1);
            group_statuses[0] = .{
                .group_id = 10,
                .doc_count = 5,
                .disk_bytes = 128,
                .empty = false,
                .created_at_millis = 7,
                .updated_at_millis = 11,
                .local_leader = true,
                .transition_pending = true,
                .replay_required = true,
                .replay_caught_up = true,
                .cutover_ready = true,
                .reads_ready_after_cutover = true,
            };
            records[0] = .{
                .store_id = 11,
                .node_id = 1,
                .role = try alloc.dupe(u8, "data"),
                .health_class = try alloc.dupe(u8, "healthy"),
                .failure_domain = try alloc.dupe(u8, "rack-a"),
                .group_statuses = group_statuses,
            };
            return records;
        }

        fn freeProjectedStores(_: @This(), alloc: std.mem.Allocator, records: []table_manager.StoreRecord) void {
            for (records) |record| table_manager.freeStore(alloc, record);
            alloc.free(records);
        }

        fn listProjectedPlacementIntents(_: @This(), alloc: std.mem.Allocator) ![]raft_reconciler.PlacementIntent {
            const intents = try alloc.alloc(raft_reconciler.PlacementIntent, 1);
            intents[0] = .{
                .record = .{ .group_id = 10, .replica_id = 1, .local_node_id = 1, .bootstrap_mode = .persisted },
                .peer_node_ids = try alloc.dupe(u64, &.{2}),
            };
            return intents;
        }

        fn freeProjectedPlacementIntents(_: @This(), alloc: std.mem.Allocator, intents: []raft_reconciler.PlacementIntent) void {
            for (intents) |intent| alloc.free(intent.peer_node_ids);
            alloc.free(intents);
        }

        fn listLocalBootstrapStatuses(_: @This(), alloc: std.mem.Allocator) ![]raft_host.BootstrapStatus {
            const statuses = try alloc.alloc(raft_host.BootstrapStatus, 1);
            statuses[0] = .{
                .group_id = 10,
                .kind = .backup_db_snapshot_restore,
                .phase = .failed,
                .attempts = 2,
                .last_updated_at_millis = 1234,
                .last_error = try alloc.dupe(u8, "InvalidBackupLocation"),
                .backup_id = try alloc.dupe(u8, "snap1"),
                .snapshot_path = try alloc.dupe(u8, "snap1/groups/10"),
            };
            return statuses;
        }

        fn freeLocalBootstrapStatuses(_: @This(), alloc: std.mem.Allocator, statuses: []raft_host.BootstrapStatus) void {
            for (statuses) |bootstrap_status| {
                if (bootstrap_status.last_error) |msg| alloc.free(msg);
                if (bootstrap_status.backup_id) |value| alloc.free(value);
                if (bootstrap_status.snapshot_path) |value| alloc.free(value);
            }
            alloc.free(statuses);
        }

        fn listProjectedRestoreProgress(_: @This(), alloc: std.mem.Allocator) ![]table_manager.RestoreProgressRecord {
            const records = try alloc.alloc(table_manager.RestoreProgressRecord, 1);
            records[0] = .{
                .table_id = 1,
                .node_id = 1,
                .group_id = 10,
                .backup_id = try alloc.dupe(u8, "snap1"),
            };
            return records;
        }

        fn freeProjectedRestoreProgress(_: @This(), alloc: std.mem.Allocator, records: []table_manager.RestoreProgressRecord) void {
            for (records) |record| table_manager.freeRestoreProgress(alloc, record);
            alloc.free(records);
        }

        fn listProjectedReplicationSourceStatuses(_: @This(), alloc: std.mem.Allocator) ![]table_manager.ReplicationSourceStatusRecord {
            const records = try alloc.alloc(table_manager.ReplicationSourceStatusRecord, 1);
            records[0] = .{
                .table_id = 1,
                .source_ordinal = 0,
                .source_kind = try alloc.dupe(u8, "postgres"),
                .external_table = try alloc.dupe(u8, "users"),
                .cutover_mode = try alloc.dupe(u8, "exported_snapshot"),
                .slot_name = try alloc.dupe(u8, "antfly_postgres_users_docs"),
                .publication_name = try alloc.dupe(u8, "antfly_pub_postgres_users_docs"),
                .phase = try alloc.dupe(u8, "snapshot"),
                .checkpoint = try alloc.dupe(u8, "lsn:0/16B6A50"),
                .snapshot_offset = 2,
                .prepared_checkpoint = try alloc.dupe(u8, "lsn:0/16B6A50"),
                .stream_checkpoint = try alloc.dupe(u8, "lsn:0/16B6A50"),
                .last_error = try alloc.dupe(u8, ""),
                .failure_class = try alloc.dupe(u8, ""),
                .lag_records = 12,
                .lag_millis = 34,
                .consecutive_failures = 2,
                .last_source_commit_at_ms = 1200,
                .last_success_at_ms = 1234,
                .last_change_applied_at_ms = 1235,
                .updated_at_ms = 555,
            };
            return records;
        }

        fn freeProjectedReplicationSourceStatuses(_: @This(), alloc: std.mem.Allocator, records: []table_manager.ReplicationSourceStatusRecord) void {
            for (records) |record| table_manager.freeReplicationSourceStatus(alloc, record);
            alloc.free(records);
        }

        fn listProjectedSplitTransitions(_: @This(), alloc: std.mem.Allocator) ![]transition_state.SplitTransitionRecord {
            const records = try alloc.alloc(transition_state.SplitTransitionRecord, 1);
            records[0] = .{
                .transition_id = 9001,
                .source_group_id = 10,
                .destination_group_id = 11,
                .split_key = try alloc.dupe(u8, "doc:m"),
            };
            return records;
        }

        fn freeProjectedSplitTransitions(_: @This(), alloc: std.mem.Allocator, records: []transition_state.SplitTransitionRecord) void {
            for (records) |record| {
                if (record.split_key) |split_key| alloc.free(split_key);
                if (record.rollback_reason) |reason| alloc.free(reason);
            }
            alloc.free(records);
        }

        fn listProjectedMergeTransitions(_: @This(), alloc: std.mem.Allocator) ![]transition_state.MergeTransitionRecord {
            const records = try alloc.alloc(transition_state.MergeTransitionRecord, 1);
            records[0] = .{ .transition_id = 9002, .donor_group_id = 11, .receiver_group_id = 10 };
            return records;
        }

        fn freeProjectedMergeTransitions(_: @This(), alloc: std.mem.Allocator, records: []transition_state.MergeTransitionRecord) void {
            for (records) |record| if (record.rollback_reason) |reason| alloc.free(reason);
            alloc.free(records);
        }

        fn observeSplitTransition(_: @This(), transition_id: u64) !?transition_state.SplitObservation {
            if (transition_id != 9001) return null;
            return .{
                .status = .{
                    .phase = .cutover_ready,
                    .source_split_phase = .splitting,
                    .bootstrapped = true,
                    .replay_required = true,
                    .replay_caught_up = true,
                    .cutover_ready = true,
                    .destination_ready_for_reads = true,
                    .source_delta_sequence = 3,
                    .dest_delta_sequence = 3,
                },
            };
        }

        fn observeMergeTransition(_: @This(), transition_id: u64) !?transition_state.MergeObservation {
            if (transition_id != 9002) return null;
            return .{
                .donor = .{
                    .phase = .cutover_ready,
                    .donor_group_id = 11,
                    .receiver_group_id = 10,
                    .receiver_accepts_donor_range = true,
                    .bootstrapped = true,
                    .replay_required = true,
                    .replay_caught_up = true,
                    .cutover_ready = true,
                    .receiver_ready_for_reads = true,
                    .donor_delta_sequence = 4,
                    .receiver_delta_sequence = 4,
                    .allow_doc_identity_reassignment = true,
                },
                .receiver = .{
                    .phase = .cutover_ready,
                    .donor_group_id = 11,
                    .receiver_group_id = 10,
                    .receiver_accepts_donor_range = true,
                    .bootstrapped = true,
                    .replay_required = true,
                    .replay_caught_up = true,
                    .cutover_ready = true,
                    .receiver_ready_for_reads = true,
                    .donor_delta_sequence = 4,
                    .receiver_delta_sequence = 4,
                    .allow_doc_identity_reassignment = true,
                },
                .receiver_local_leader = true,
            };
        }
    };

    const source = FakeSource{};
    var snapshot = try captureSnapshot(std.testing.allocator, source);
    defer freeSnapshot(std.testing.allocator, source, &snapshot);

    try std.testing.expectEqual(@as(u64, 77), snapshot.status.metadata_group_id);
    try std.testing.expectEqual(@as(usize, 1), snapshot.status.projected_tables_with_replication_sources);
    try std.testing.expectEqual(@as(usize, 2), snapshot.status.projected_replication_sources);
    try std.testing.expectEqual(@as(usize, 1), snapshot.status.projected_replication_source_statuses);
    try std.testing.expectEqual(@as(usize, 1), snapshot.status.projected_replication_source_statuses_exact_cutover);
    try std.testing.expectEqual(@as(usize, 0), snapshot.status.projected_replication_source_statuses_non_exact_cutover);
    try std.testing.expectEqual(@as(usize, 0), snapshot.status.projected_replication_source_statuses_reseed_recommended);
    try std.testing.expectEqual(@as(usize, 1), snapshot.status.projected_replication_source_statuses_exported_snapshot);
    try std.testing.expectEqual(@as(usize, 0), snapshot.status.projected_replication_source_statuses_slot_first);
    try std.testing.expectEqual(@as(usize, 0), snapshot.status.projected_replication_source_statuses_slot_resumed);
    try std.testing.expectEqual(@as(usize, 1), snapshot.status.projected_replication_source_statuses_snapshot);
    try std.testing.expectEqual(@as(usize, 0), snapshot.status.projected_replication_source_statuses_cutover_prepared);
    try std.testing.expectEqual(@as(usize, 0), snapshot.status.projected_replication_source_statuses_streaming);
    try std.testing.expectEqual(@as(usize, 0), snapshot.status.projected_replication_source_statuses_failed);
    try std.testing.expectEqual(@as(usize, 0), snapshot.status.projected_replication_source_statuses_with_last_error);
    try std.testing.expectEqual(@as(usize, 0), snapshot.status.projected_replication_source_statuses_slot_missing_failed);
    try std.testing.expectEqual(@as(usize, 0), snapshot.status.projected_replication_source_statuses_retryable_failed);
    try std.testing.expectEqual(@as(usize, 0), snapshot.status.projected_replication_source_statuses_terminal_failed);
    try std.testing.expectEqual(@as(usize, 1), snapshot.status.projected_replication_source_statuses_with_consecutive_failures);
    try std.testing.expectEqual(@as(usize, 1), snapshot.status.projected_replication_source_statuses_with_success_timestamp);
    try std.testing.expectEqual(@as(usize, 1), snapshot.status.projected_replication_source_statuses_with_change_timestamp);
    try std.testing.expectEqual(@as(u64, 2), snapshot.status.projected_replication_source_consecutive_failures_total);
    try std.testing.expectEqual(@as(u64, 2), snapshot.status.projected_replication_source_consecutive_failures_max);
    try std.testing.expectEqual(@as(u64, 12), snapshot.status.projected_replication_source_lag_records_total);
    try std.testing.expectEqual(@as(u64, 12), snapshot.status.projected_replication_source_lag_records_max);
    try std.testing.expectEqual(@as(u64, 34), snapshot.status.projected_replication_source_lag_millis_total);
    try std.testing.expectEqual(@as(u64, 34), snapshot.status.projected_replication_source_lag_millis_max);
    try std.testing.expectEqual(@as(u64, 56), snapshot.status.projected_replication_source_observed_lag_millis_total);
    try std.testing.expectEqual(@as(u64, 56), snapshot.status.projected_replication_source_observed_lag_millis_max);
    try std.testing.expectEqual(@as(usize, 1), snapshot.status.projected_replication_source_statuses_with_source_commit_timestamp);
    try std.testing.expectEqual(@as(u64, 1234), snapshot.status.projected_replication_source_last_success_at_ms_max);
    try std.testing.expectEqual(@as(u64, 1200), snapshot.status.projected_replication_source_last_source_commit_at_ms_max);
    try std.testing.expectEqual(@as(u64, 1235), snapshot.status.projected_replication_source_last_change_applied_at_ms_max);
    try std.testing.expectEqual(@as(usize, 1), snapshot.tables.len);
    try std.testing.expectEqual(@as(usize, 1), snapshot.ranges.len);
    try std.testing.expectEqual(@as(usize, 1), snapshot.stores.len);
    try std.testing.expectEqual(@as(usize, 1), snapshot.placement_intents.len);
    try std.testing.expectEqual(@as(usize, 1), snapshot.local_bootstrap_statuses.len);
    try std.testing.expectEqual(@as(u64, 10), snapshot.local_bootstrap_statuses[0].group_id);
    try std.testing.expectEqual(.failed, snapshot.local_bootstrap_statuses[0].phase);
    try std.testing.expectEqual(@as(u64, 1234), snapshot.local_bootstrap_statuses[0].last_updated_at_millis);
    try std.testing.expectEqualStrings("snap1", snapshot.local_bootstrap_statuses[0].backup_id orelse return error.TestExpectedEqual);
    try std.testing.expectEqualStrings("snap1/groups/10", snapshot.local_bootstrap_statuses[0].snapshot_path orelse return error.TestExpectedEqual);
    try std.testing.expectEqual(@as(usize, 1), snapshot.restore_progresses.len);
    try std.testing.expectEqual(@as(usize, 1), snapshot.replication_source_statuses.len);
    try std.testing.expectEqual(@as(usize, 0), snapshot.replication_source_action_hints.len);
    try std.testing.expectEqualStrings("postgres", snapshot.replication_source_statuses[0].source_kind);
    try std.testing.expectEqualStrings("users", snapshot.replication_source_statuses[0].external_table);
    try std.testing.expectEqualStrings("exported_snapshot", snapshot.replication_source_statuses[0].cutover_mode);
    try std.testing.expectEqualStrings("antfly_postgres_users_docs", snapshot.replication_source_statuses[0].slot_name);
    try std.testing.expectEqualStrings("antfly_pub_postgres_users_docs", snapshot.replication_source_statuses[0].publication_name);
    try std.testing.expectEqualStrings("snapshot", snapshot.replication_source_statuses[0].phase);
    try std.testing.expectEqual(@as(u64, 2), snapshot.replication_source_statuses[0].snapshot_offset);
    try std.testing.expectEqualStrings("lsn:0/16B6A50", snapshot.replication_source_statuses[0].prepared_checkpoint);
    try std.testing.expectEqualStrings("lsn:0/16B6A50", snapshot.replication_source_statuses[0].stream_checkpoint);
    try std.testing.expectEqualStrings("", snapshot.replication_source_statuses[0].failure_class);
    try std.testing.expectEqual(@as(u64, 34), snapshot.replication_source_statuses[0].lag_millis);
    try std.testing.expectEqual(@as(u64, 2), snapshot.replication_source_statuses[0].consecutive_failures);
    try std.testing.expectEqual(@as(u64, 1200), snapshot.replication_source_statuses[0].last_source_commit_at_ms);
    try std.testing.expectEqual(@as(u64, 1234), snapshot.replication_source_statuses[0].last_success_at_ms);
    try std.testing.expectEqual(@as(u64, 1235), snapshot.replication_source_statuses[0].last_change_applied_at_ms);
    try std.testing.expectEqual(@as(usize, 1), snapshot.split_transitions.len);
    try std.testing.expectEqual(@as(usize, 1), snapshot.merge_transitions.len);
    try std.testing.expectEqual(@as(usize, 1), snapshot.split_observations.len);
    try std.testing.expectEqual(@as(usize, 1), snapshot.merge_observations.len);
    try std.testing.expectEqual(@as(usize, 1), snapshot.merged_group_statuses.len);
    try std.testing.expectEqual(@as(u64, 10), snapshot.merged_group_statuses[0].group_id);
    try std.testing.expect(snapshot.merged_group_statuses[0].leader_known);
    try std.testing.expectEqual(@as(u64, 11), snapshot.merged_group_statuses[0].leader_store_id);
    try std.testing.expect(snapshot.merged_group_statuses[0].doc_identity_reassignment_active);
}

test "metadata admin snapshot derives replication source action hints for reseed" {
    const FakeSource = struct {
        fn status(_: @This()) !MetadataStatus {
            return .{
                .metadata_group_id = 77,
                .metrics = .{},
                .projected_tables = 1,
                .projected_replication_source_statuses = 1,
                .projected_replication_source_statuses_exact_cutover = 0,
                .projected_replication_source_statuses_non_exact_cutover = 1,
                .projected_replication_source_statuses_reseed_recommended = 1,
            };
        }

        fn listProjectedTables(_: @This(), alloc: std.mem.Allocator) ![]table_manager.TableRecord {
            const records = try alloc.alloc(table_manager.TableRecord, 1);
            records[0] = .{
                .table_id = 9,
                .name = try alloc.dupe(u8, "docs"),
                .replication_sources_json = try alloc.dupe(u8, "[{\"type\":\"postgres\"}]"),
                .placement_role = try alloc.dupe(u8, "data"),
            };
            return records;
        }

        fn freeProjectedTables(_: @This(), alloc: std.mem.Allocator, records: []table_manager.TableRecord) void {
            for (records) |record| {
                alloc.free(record.name);
                alloc.free(record.replication_sources_json);
                alloc.free(record.placement_role);
            }
            alloc.free(records);
        }

        fn listProjectedRanges(_: @This(), alloc: std.mem.Allocator) ![]table_manager.RangeRecord {
            const records = try alloc.alloc(table_manager.RangeRecord, 0);
            return records;
        }

        fn freeProjectedRanges(_: @This(), alloc: std.mem.Allocator, records: []table_manager.RangeRecord) void {
            alloc.free(records);
        }

        fn listProjectedStores(_: @This(), alloc: std.mem.Allocator) ![]table_manager.StoreRecord {
            const records = try alloc.alloc(table_manager.StoreRecord, 0);
            return records;
        }

        fn freeProjectedStores(_: @This(), alloc: std.mem.Allocator, records: []table_manager.StoreRecord) void {
            alloc.free(records);
        }

        fn listProjectedPlacementIntents(_: @This(), alloc: std.mem.Allocator) ![]raft_reconciler.PlacementIntent {
            const records = try alloc.alloc(raft_reconciler.PlacementIntent, 0);
            return records;
        }

        fn freeProjectedPlacementIntents(_: @This(), alloc: std.mem.Allocator, records: []raft_reconciler.PlacementIntent) void {
            alloc.free(records);
        }

        fn listProjectedReplicationSourceStatuses(_: @This(), alloc: std.mem.Allocator) ![]table_manager.ReplicationSourceStatusRecord {
            const records = try alloc.alloc(table_manager.ReplicationSourceStatusRecord, 1);
            records[0] = .{
                .table_id = 9,
                .source_ordinal = 0,
                .source_kind = try alloc.dupe(u8, "postgres"),
                .external_table = try alloc.dupe(u8, "users"),
                .cutover_mode = try alloc.dupe(u8, "slot_resumed"),
                .slot_name = try alloc.dupe(u8, "slot_old"),
                .publication_name = try alloc.dupe(u8, "pub_old"),
                .phase = try alloc.dupe(u8, "streaming"),
            };
            return records;
        }

        fn freeProjectedReplicationSourceStatuses(_: @This(), alloc: std.mem.Allocator, records: []table_manager.ReplicationSourceStatusRecord) void {
            for (records) |record| table_manager.freeReplicationSourceStatus(alloc, record);
            alloc.free(records);
        }

        fn listProjectedSplitTransitions(_: @This(), alloc: std.mem.Allocator) ![]transition_state.SplitTransitionRecord {
            const records = try alloc.alloc(transition_state.SplitTransitionRecord, 0);
            return records;
        }

        fn freeProjectedSplitTransitions(_: @This(), alloc: std.mem.Allocator, records: []transition_state.SplitTransitionRecord) void {
            alloc.free(records);
        }

        fn listProjectedMergeTransitions(_: @This(), alloc: std.mem.Allocator) ![]transition_state.MergeTransitionRecord {
            const records = try alloc.alloc(transition_state.MergeTransitionRecord, 0);
            return records;
        }

        fn freeProjectedMergeTransitions(_: @This(), alloc: std.mem.Allocator, records: []transition_state.MergeTransitionRecord) void {
            alloc.free(records);
        }
    };

    var snapshot = try captureSnapshot(std.testing.allocator, FakeSource{});
    defer freeSnapshot(std.testing.allocator, FakeSource{}, &snapshot);

    try std.testing.expectEqual(@as(usize, 1), snapshot.replication_source_action_hints.len);
    try std.testing.expectEqualStrings("reseed_exact_cutover", snapshot.replication_source_action_hints[0].action);
    try std.testing.expectEqualStrings("existing_slot_non_exact_cutover", snapshot.replication_source_action_hints[0].reason);
    try std.testing.expectEqualStrings("docs", snapshot.replication_source_action_hints[0].table_name);
    try std.testing.expect(std.mem.indexOf(u8, snapshot.replication_source_action_hints[0].reseed_exact_cutover_path, "/internal/v1/tables/docs/replication-sources/0/reseed-exact-cutover") != null);
}
