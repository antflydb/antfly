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
const control_loop = @import("control_loop.zig");
const metadata_reconciler = @import("reconciler.zig");
const placement_planner = @import("placement_planner.zig");
const raft_reconciler = @import("../raft/reconciler.zig");
const table_manager = @import("table_manager.zig");

pub const PlacementReconcileSummary = struct {
    upserts: usize = 0,
    removals: usize = 0,
};

pub const TableWorkflow = struct {
    loop: control_loop.MetadataControlLoop,

    pub fn init(alloc: std.mem.Allocator) TableWorkflow {
        return .{
            .loop = control_loop.MetadataControlLoop.init(alloc),
        };
    }

    pub fn deinit(self: *TableWorkflow) void {
        self.loop.deinit();
        self.* = undefined;
    }

    pub fn controlLoop(self: *TableWorkflow) *control_loop.MetadataControlLoop {
        return &self.loop;
    }

    pub fn setPlacementCandidates(self: *TableWorkflow, candidate_node_ids: []const u64) !void {
        try self.loop.stateRef().setPlacementCandidates(candidate_node_ids);
    }

    pub fn bootstrapDesiredFromCommitted(self: *TableWorkflow, service: anytype) !void {
        try self.loop.stateRef().syncProjected(service);
        try self.loop.stateRef().seedDesiredFromProjected();
    }

    pub fn createTable(
        self: *TableWorkflow,
        service: anytype,
        table: table_manager.TableRecord,
        initial_range: table_manager.RangeRecord,
    ) !control_loop.ReconcileSummary {
        return try self.createTableWithRanges(service, table, &[_]table_manager.RangeRecord{initial_range});
    }

    pub fn createTableWithRanges(
        self: *TableWorkflow,
        service: anytype,
        table: table_manager.TableRecord,
        initial_ranges: []const table_manager.RangeRecord,
    ) !control_loop.ReconcileSummary {
        try self.bootstrapDesiredFromCommitted(service);
        try self.loop.stateRef().tableManager().upsertTable(table);
        for (initial_ranges) |initial_range| {
            try self.loop.stateRef().tableManager().upsertRange(initial_range);
        }
        return try reconcileForService(&self.loop, service);
    }

    pub fn requestSplit(
        self: *TableWorkflow,
        service: anytype,
        intent: table_manager.SplitIntent,
    ) !control_loop.ReconcileSummary {
        try self.bootstrapDesiredFromCommitted(service);
        var current = try self.loop.stateRef().captureCurrent(service);
        defer current.deinit(self.loop.alloc);
        try validateSplitIntentDocIdentity(current.current, intent);
        try self.loop.stateRef().tableManager().requestSplit(intent);
        return try reconcileForService(&self.loop, service);
    }

    pub fn requestMerge(
        self: *TableWorkflow,
        service: anytype,
        intent: table_manager.MergeIntent,
    ) !control_loop.ReconcileSummary {
        try self.bootstrapDesiredFromCommitted(service);
        var current = try self.loop.stateRef().captureCurrent(service);
        defer current.deinit(self.loop.alloc);
        const requires_reassignment = try self.loop.stateRef().tableManager().mergeRequiresDocIdentityReassignment(
            intent.donor_group_id,
            intent.receiver_group_id,
        );
        const normalized_intent = try normalizeMergeIntentDocIdentity(
            current.current,
            intent,
            requires_reassignment,
        );
        try self.loop.stateRef().tableManager().requestMerge(normalized_intent);
        return try reconcileForService(&self.loop, service);
    }

    pub fn addRange(
        self: *TableWorkflow,
        service: anytype,
        record: table_manager.RangeRecord,
    ) !control_loop.ReconcileSummary {
        try self.bootstrapDesiredFromCommitted(service);
        try self.loop.stateRef().tableManager().upsertRange(record);
        return try reconcileForService(&self.loop, service);
    }

    pub fn upsertForeignKeyReferenceRange(
        self: *TableWorkflow,
        service: anytype,
        record: table_manager.ForeignKeyReferenceRangeRecord,
    ) !control_loop.ReconcileSummary {
        try self.bootstrapDesiredFromCommitted(service);
        try self.loop.stateRef().tableManager().upsertForeignKeyReferenceRange(record);
        return try reconcileForService(&self.loop, service);
    }

    pub fn removeForeignKeyReferenceRange(
        self: *TableWorkflow,
        service: anytype,
        child_table_id: u64,
        constraint_name: []const u8,
        parent_table_id: u64,
        start_parent_key: []const u8,
    ) !control_loop.ReconcileSummary {
        try self.bootstrapDesiredFromCommitted(service);
        _ = self.loop.stateRef().tableManager().removeForeignKeyReferenceRange(child_table_id, constraint_name, parent_table_id, start_parent_key);
        return try reconcileForService(&self.loop, service);
    }

    pub fn upsertUniqueConstraintRange(
        self: *TableWorkflow,
        service: anytype,
        record: table_manager.UniqueConstraintRangeRecord,
    ) !control_loop.ReconcileSummary {
        try self.bootstrapDesiredFromCommitted(service);
        try self.loop.stateRef().tableManager().upsertUniqueConstraintRange(record);
        return try reconcileForService(&self.loop, service);
    }

    pub fn removeUniqueConstraintRange(
        self: *TableWorkflow,
        service: anytype,
        table_id: u64,
        constraint_name: []const u8,
        start_encoded_value: []const u8,
    ) !control_loop.ReconcileSummary {
        try self.bootstrapDesiredFromCommitted(service);
        _ = self.loop.stateRef().tableManager().removeUniqueConstraintRange(table_id, constraint_name, start_encoded_value);
        return try reconcileForService(&self.loop, service);
    }

    pub fn beginUniqueConstraintRangeSplit(
        self: *TableWorkflow,
        service: anytype,
        request: table_manager.UniqueConstraintRangeSplitRequest,
    ) !void {
        try self.bootstrapDesiredFromCommitted(service);
        try self.loop.stateRef().tableManager().beginUniqueConstraintRangeSplit(request);
        try service.beginUniqueConstraintRangeSplit(request);
    }

    pub fn finishUniqueConstraintRangeSplit(
        self: *TableWorkflow,
        service: anytype,
        request: table_manager.UniqueConstraintRangeSplitRequest,
    ) !void {
        try self.bootstrapDesiredFromCommitted(service);
        try self.loop.stateRef().tableManager().finishUniqueConstraintRangeSplit(request);
        try service.finishUniqueConstraintRangeSplit(request);
    }

    pub fn beginUniqueConstraintRangeMerge(
        self: *TableWorkflow,
        service: anytype,
        request: table_manager.UniqueConstraintRangeMergeRequest,
    ) !void {
        try self.bootstrapDesiredFromCommitted(service);
        try self.loop.stateRef().tableManager().beginUniqueConstraintRangeMerge(request);
        try service.beginUniqueConstraintRangeMerge(request);
    }

    pub fn finishUniqueConstraintRangeMerge(
        self: *TableWorkflow,
        service: anytype,
        request: table_manager.UniqueConstraintRangeMergeRequest,
    ) !void {
        try self.bootstrapDesiredFromCommitted(service);
        try self.loop.stateRef().tableManager().finishUniqueConstraintRangeMerge(request);
        try service.finishUniqueConstraintRangeMerge(request);
    }

    pub fn beginUniqueConstraintRangeRebuild(
        self: *TableWorkflow,
        service: anytype,
        selector: table_manager.UniqueConstraintRangeSelector,
    ) !void {
        try self.bootstrapDesiredFromCommitted(service);
        try self.loop.stateRef().tableManager().beginUniqueConstraintRangeRebuild(selector);
        try service.beginUniqueConstraintRangeRebuild(selector);
    }

    pub fn finishUniqueConstraintRangeRebuild(
        self: *TableWorkflow,
        service: anytype,
        selector: table_manager.UniqueConstraintRangeSelector,
    ) !void {
        try self.bootstrapDesiredFromCommitted(service);
        try self.loop.stateRef().tableManager().finishUniqueConstraintRangeRebuild(selector);
        try service.finishUniqueConstraintRangeRebuild(selector);
    }

    pub fn beginForeignKeyReferenceRangeSplit(
        self: *TableWorkflow,
        service: anytype,
        request: table_manager.ForeignKeyReferenceRangeSplitRequest,
    ) !void {
        try self.bootstrapDesiredFromCommitted(service);
        try self.loop.stateRef().tableManager().beginForeignKeyReferenceRangeSplit(request);
        try service.beginForeignKeyReferenceRangeSplit(request);
    }

    pub fn finishForeignKeyReferenceRangeSplit(
        self: *TableWorkflow,
        service: anytype,
        request: table_manager.ForeignKeyReferenceRangeSplitRequest,
    ) !void {
        try self.bootstrapDesiredFromCommitted(service);
        try self.loop.stateRef().tableManager().finishForeignKeyReferenceRangeSplit(request);
        try service.finishForeignKeyReferenceRangeSplit(request);
    }

    pub fn beginForeignKeyReferenceRangeMerge(
        self: *TableWorkflow,
        service: anytype,
        request: table_manager.ForeignKeyReferenceRangeMergeRequest,
    ) !void {
        try self.bootstrapDesiredFromCommitted(service);
        try self.loop.stateRef().tableManager().beginForeignKeyReferenceRangeMerge(request);
        try service.beginForeignKeyReferenceRangeMerge(request);
    }

    pub fn finishForeignKeyReferenceRangeMerge(
        self: *TableWorkflow,
        service: anytype,
        request: table_manager.ForeignKeyReferenceRangeMergeRequest,
    ) !void {
        try self.bootstrapDesiredFromCommitted(service);
        try self.loop.stateRef().tableManager().finishForeignKeyReferenceRangeMerge(request);
        try service.finishForeignKeyReferenceRangeMerge(request);
    }

    pub fn beginForeignKeyReferenceRangeRebuild(
        self: *TableWorkflow,
        service: anytype,
        selector: table_manager.ForeignKeyReferenceRangeSelector,
    ) !void {
        try self.bootstrapDesiredFromCommitted(service);
        try self.loop.stateRef().tableManager().beginForeignKeyReferenceRangeRebuild(selector);
        try service.beginForeignKeyReferenceRangeRebuild(selector);
    }

    pub fn finishForeignKeyReferenceRangeRebuild(
        self: *TableWorkflow,
        service: anytype,
        selector: table_manager.ForeignKeyReferenceRangeSelector,
    ) !void {
        try self.bootstrapDesiredFromCommitted(service);
        try self.loop.stateRef().tableManager().finishForeignKeyReferenceRangeRebuild(selector);
        try service.finishForeignKeyReferenceRangeRebuild(selector);
    }

    pub fn dropTable(
        self: *TableWorkflow,
        service: anytype,
        table_id: u64,
    ) !control_loop.ReconcileSummary {
        try self.bootstrapDesiredFromCommitted(service);
        _ = self.loop.stateRef().tableManager().removeTableTopology(table_id);
        return try reconcileForService(&self.loop, service);
    }

    pub fn planLocalPlacementIntents(
        self: *TableWorkflow,
        alloc: std.mem.Allocator,
        local_node_id: u64,
        candidate_node_ids: []const u64,
    ) ![]raft_reconciler.PlacementIntent {
        try self.loop.reconciler.syncAutomaticForeignKeyReferenceOwnerRanges(self.loop.stateRef().tableManager());
        var planner = placement_planner.PlacementPlanner.init(alloc);
        return try planner.planLocalIntents(self.loop.stateRef().tableManager(), local_node_id, candidate_node_ids);
    }

    pub fn reconcileLocalPlacementIntents(
        self: *TableWorkflow,
        service: anytype,
        alloc: std.mem.Allocator,
        local_node_id: u64,
        candidate_node_ids: []const u64,
    ) !PlacementReconcileSummary {
        const desired = try self.planLocalPlacementIntents(alloc, local_node_id, candidate_node_ids);
        defer {
            for (desired) |intent| if (intent.peer_node_ids.len > 0) alloc.free(intent.peer_node_ids);
            alloc.free(desired);
        }

        const current = try service.listProjectedPlacementIntents(alloc);
        defer service.freeProjectedPlacementIntents(alloc, current);

        var summary: PlacementReconcileSummary = .{};
        for (desired) |intent| {
            const existing = findPlacementIntent(current, intent.record.group_id, intent.record.local_node_id);
            if (existing == null or !placementIntentsEqual(existing.?, intent)) {
                try service.upsertReplicaIntent(intent);
                summary.upserts += 1;
            }
        }
        for (current) |intent| {
            if (intent.record.local_node_id != local_node_id) continue;
            if (findPlacementIntent(desired, intent.record.group_id, local_node_id) == null) {
                try service.removeReplicaIntent(intent.record.group_id, local_node_id);
                summary.removals += 1;
            }
        }
        return summary;
    }
};

fn reconcileForService(loop: *control_loop.MetadataControlLoop, service: anytype) !control_loop.ReconcileSummary {
    const ServiceType = @TypeOf(service);
    const ServiceDeclType = switch (@typeInfo(ServiceType)) {
        .pointer => |pointer| pointer.child,
        else => ServiceType,
    };
    if (@hasDecl(ServiceDeclType, "reconcileOnceEnsuringLease")) {
        return try service.reconcileOnceEnsuringLease(loop);
    }
    if (@hasDecl(ServiceDeclType, "reconcileOnceIfLeaseHeld")) {
        return (try service.reconcileOnceIfLeaseHeld(loop)) orelse error.ReconcileLeaseNotHeld;
    }
    return try loop.reconcileOnce(service);
}

fn validateSplitIntentDocIdentity(current: metadata_reconciler.CurrentMetadataState, intent: table_manager.SplitIntent) !void {
    const source = findMergedGroupStatus(current.merged_group_statuses, intent.source_group_id) orelse {
        if (!currentHasDocIdentityTelemetry(current)) return;
        return error.DocIdentityNamespaceMismatch;
    };
    if (source.doc_identity_reassignment_active) return error.DocIdentityNamespaceMismatch;
    if (source.doc_identity_namespace_conflict) return error.DocIdentityNamespaceMismatch;
    if (source.doc_identity.rebuild_required) return error.DocIdentityNamespaceMismatch;
    if (source.doc_identity.ordinal_capacity_exhausted) return error.DocIdentityNamespaceMismatch;
    if (!runtimeDocIdentityHasFacts(source.doc_identity)) return;
}

fn validateMergeIntentDocIdentity(current: metadata_reconciler.CurrentMetadataState, intent: table_manager.MergeIntent) !void {
    const donor = findMergedGroupStatus(current.merged_group_statuses, intent.donor_group_id) orelse return error.DocIdentityNamespaceMismatch;
    const receiver = findMergedGroupStatus(current.merged_group_statuses, intent.receiver_group_id) orelse return error.DocIdentityNamespaceMismatch;
    if (donor.doc_identity_reassignment_active or receiver.doc_identity_reassignment_active) return error.DocIdentityNamespaceMismatch;
    if (donor.doc_identity_namespace_conflict or receiver.doc_identity_namespace_conflict) return error.DocIdentityNamespaceMismatch;
    if (donor.doc_identity.rebuild_required or receiver.doc_identity.rebuild_required) return error.DocIdentityNamespaceMismatch;
    if (donor.doc_identity.ordinal_capacity_exhausted or receiver.doc_identity.ordinal_capacity_exhausted) return error.DocIdentityNamespaceMismatch;
    if (!runtimeDocIdentityHasOrdinalRows(donor.doc_identity) or !runtimeDocIdentityHasOrdinalRows(receiver.doc_identity)) return;
    if (intent.allow_doc_identity_reassignment) return;
    if (!runtimeDocIdentitySameNamespace(donor.doc_identity, receiver.doc_identity)) return error.DocIdentityNamespaceMismatch;
}

fn normalizeMergeIntentDocIdentity(
    current: metadata_reconciler.CurrentMetadataState,
    intent: table_manager.MergeIntent,
    requires_reassignment: bool,
) !table_manager.MergeIntent {
    try validateMergeIntentDocIdentity(current, intent);
    if (intent.allow_doc_identity_reassignment or !requires_reassignment) return intent;

    // A merge approved by the runtime guard may still join distinct committed
    // range identities. Record that reassignment so the durable contract
    // explicitly identifies which namespace wins.
    var normalized = intent;
    normalized.allow_doc_identity_reassignment = true;
    return normalized;
}

fn findMergedGroupStatus(
    statuses: []const metadata_reconciler.MergedGroupStatus,
    group_id: u64,
) ?metadata_reconciler.MergedGroupStatus {
    for (statuses) |status| {
        if (status.group_id == group_id) return status;
    }
    return null;
}

fn currentHasDocIdentityTelemetry(current: metadata_reconciler.CurrentMetadataState) bool {
    for (current.merged_group_statuses) |status| {
        if (runtimeDocIdentityHasFacts(status.doc_identity)) return true;
    }
    for (current.stores) |store| {
        for (store.runtime_statuses) |status| {
            if (runtimeDocIdentityHasFacts(status.doc_identity)) return true;
        }
    }
    return false;
}

fn runtimeDocIdentityHasFacts(stats: table_manager.RuntimeDocIdentityStatusReport) bool {
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
        stats.primary_docs_with_tombstone_ordinals != 0 or
        stats.complete;
}

fn runtimeDocIdentityHasOrdinalRows(stats: table_manager.RuntimeDocIdentityStatusReport) bool {
    return stats.next_ordinal != 1 or
        stats.allocated_ordinals != 0 or
        stats.state_rows != 0 or
        stats.live_ordinals != 0 or
        stats.tombstone_ordinals != 0;
}

fn runtimeDocIdentitySameNamespace(
    left: table_manager.RuntimeDocIdentityStatusReport,
    right: table_manager.RuntimeDocIdentityStatusReport,
) bool {
    return left.namespace_table_id == right.namespace_table_id and
        left.namespace_shard_id == right.namespace_shard_id and
        left.namespace_range_id == right.namespace_range_id;
}

test "table workflow doc identity guards reject active transition intents" {
    var left = metadata_reconciler.MergedGroupStatus{
        .group_id = 91,
        .doc_identity_reassignment_active = true,
        .doc_identity = .{
            .namespace_table_id = 9,
            .namespace_shard_id = 91,
            .namespace_range_id = 9001,
            .next_ordinal = 12,
            .allocated_ordinals = 11,
        },
    };
    const right = metadata_reconciler.MergedGroupStatus{
        .group_id = 92,
        .doc_identity = .{
            .namespace_table_id = 9,
            .namespace_shard_id = 92,
            .namespace_range_id = 9002,
            .next_ordinal = 7,
            .allocated_ordinals = 6,
        },
    };
    var statuses = [_]metadata_reconciler.MergedGroupStatus{ left, right };
    var current = metadata_reconciler.CurrentMetadataState{ .merged_group_statuses = &statuses };

    try std.testing.expectError(error.DocIdentityNamespaceMismatch, validateSplitIntentDocIdentity(current, .{
        .transition_id = 0,
        .table_id = 9,
        .source_group_id = 93,
        .destination_group_id = 94,
        .split_key = "doc:m",
    }));
    try std.testing.expectError(error.DocIdentityNamespaceMismatch, validateSplitIntentDocIdentity(current, .{
        .transition_id = 1,
        .table_id = 9,
        .source_group_id = 91,
        .destination_group_id = 93,
        .split_key = "doc:m",
    }));
    try std.testing.expectError(error.DocIdentityNamespaceMismatch, validateMergeIntentDocIdentity(current, .{
        .transition_id = 2,
        .table_id = 9,
        .donor_group_id = 92,
        .receiver_group_id = 91,
        .allow_doc_identity_reassignment = true,
    }));

    left.doc_identity_reassignment_active = false;
    statuses = [_]metadata_reconciler.MergedGroupStatus{ left, right };
    current = .{ .merged_group_statuses = &statuses };
    try validateMergeIntentDocIdentity(current, .{
        .transition_id = 3,
        .table_id = 9,
        .donor_group_id = 92,
        .receiver_group_id = 91,
        .allow_doc_identity_reassignment = true,
    });
    try std.testing.expectError(error.DocIdentityNamespaceMismatch, validateMergeIntentDocIdentity(current, .{
        .transition_id = 4,
        .table_id = 9,
        .donor_group_id = 92,
        .receiver_group_id = 91,
    }));
    try std.testing.expectError(error.DocIdentityNamespaceMismatch, validateMergeIntentDocIdentity(current, .{
        .transition_id = 40,
        .table_id = 9,
        .donor_group_id = 92,
        .receiver_group_id = 93,
    }));
    try std.testing.expectError(error.DocIdentityNamespaceMismatch, validateMergeIntentDocIdentity(current, .{
        .transition_id = 41,
        .table_id = 9,
        .donor_group_id = 92,
        .receiver_group_id = 93,
        .allow_doc_identity_reassignment = true,
    }));

    statuses[0].doc_identity.ordinal_capacity_exhausted = true;
    try std.testing.expectError(error.DocIdentityNamespaceMismatch, validateMergeIntentDocIdentity(current, .{
        .transition_id = 42,
        .table_id = 9,
        .donor_group_id = 92,
        .receiver_group_id = 91,
        .allow_doc_identity_reassignment = true,
    }));
    statuses[0].doc_identity.ordinal_capacity_exhausted = false;

    statuses[0].doc_identity.rebuild_required = true;
    try std.testing.expectError(error.DocIdentityNamespaceMismatch, validateSplitIntentDocIdentity(current, .{
        .transition_id = 5,
        .table_id = 9,
        .source_group_id = 91,
        .destination_group_id = 93,
        .split_key = "doc:m",
    }));
    statuses[0].doc_identity.rebuild_required = false;
    statuses[0].doc_identity.ordinal_capacity_exhausted = true;
    try std.testing.expectError(error.DocIdentityNamespaceMismatch, validateSplitIntentDocIdentity(current, .{
        .transition_id = 6,
        .table_id = 9,
        .source_group_id = 91,
        .destination_group_id = 93,
        .split_key = "doc:m",
    }));
}

test "table workflow doc identity lifecycle handles mixed-version transition status" {
    const old_left = metadata_reconciler.MergedGroupStatus{
        .group_id = 101,
        .doc_identity = .{
            .namespace_table_id = 10,
            .namespace_shard_id = 101,
            .namespace_range_id = 1001,
        },
    };
    const old_right = metadata_reconciler.MergedGroupStatus{
        .group_id = 102,
        .doc_identity = .{
            .namespace_table_id = 10,
            .namespace_shard_id = 102,
            .namespace_range_id = 1002,
        },
    };
    var statuses = [_]metadata_reconciler.MergedGroupStatus{ old_left, old_right };
    var current = metadata_reconciler.CurrentMetadataState{ .merged_group_statuses = &statuses };

    try validateSplitIntentDocIdentity(current, .{
        .transition_id = 10,
        .table_id = 10,
        .source_group_id = 101,
        .destination_group_id = 103,
        .split_key = "doc:m",
    });
    try validateMergeIntentDocIdentity(current, .{
        .transition_id = 11,
        .table_id = 10,
        .donor_group_id = 102,
        .receiver_group_id = 101,
    });
    const normalized = try normalizeMergeIntentDocIdentity(current, .{
        .transition_id = 11,
        .table_id = 10,
        .donor_group_id = 102,
        .receiver_group_id = 101,
    }, true);
    try std.testing.expect(normalized.allow_doc_identity_reassignment);

    try validateSplitIntentDocIdentity(.{ .merged_group_statuses = &.{} }, .{
        .transition_id = 12,
        .table_id = 10,
        .source_group_id = 101,
        .destination_group_id = 103,
        .split_key = "doc:m",
    });
    try std.testing.expectError(error.DocIdentityNamespaceMismatch, validateSplitIntentDocIdentity(.{ .merged_group_statuses = &.{old_left} }, .{
        .transition_id = 12,
        .table_id = 10,
        .source_group_id = 102,
        .destination_group_id = 103,
        .split_key = "doc:m",
    }));
    try std.testing.expectError(error.DocIdentityNamespaceMismatch, validateMergeIntentDocIdentity(.{ .merged_group_statuses = &.{old_left} }, .{
        .transition_id = 13,
        .table_id = 10,
        .donor_group_id = 102,
        .receiver_group_id = 101,
        .allow_doc_identity_reassignment = true,
    }));

    statuses = [_]metadata_reconciler.MergedGroupStatus{
        .{
            .group_id = 101,
            .doc_identity = .{
                .namespace_table_id = 10,
                .namespace_shard_id = 101,
                .namespace_range_id = 1001,
                .allocated_ordinals = 1,
            },
        },
        .{
            .group_id = 102,
            .doc_identity = .{
                .namespace_table_id = 10,
                .namespace_shard_id = 102,
                .namespace_range_id = 1002,
                .allocated_ordinals = 1,
            },
        },
    };
    current = .{ .merged_group_statuses = &statuses };
    try std.testing.expectError(error.DocIdentityNamespaceMismatch, validateMergeIntentDocIdentity(current, .{
        .transition_id = 14,
        .table_id = 10,
        .donor_group_id = 102,
        .receiver_group_id = 101,
    }));
    try validateMergeIntentDocIdentity(current, .{
        .transition_id = 15,
        .table_id = 10,
        .donor_group_id = 102,
        .receiver_group_id = 101,
        .allow_doc_identity_reassignment = true,
    });

    statuses[1].doc_identity.rebuild_required = true;
    try std.testing.expectError(error.DocIdentityNamespaceMismatch, validateMergeIntentDocIdentity(current, .{
        .transition_id = 16,
        .table_id = 10,
        .donor_group_id = 102,
        .receiver_group_id = 101,
        .allow_doc_identity_reassignment = true,
    }));
}

fn findPlacementIntent(
    intents: []const raft_reconciler.PlacementIntent,
    group_id: u64,
    local_node_id: u64,
) ?raft_reconciler.PlacementIntent {
    for (intents) |intent| {
        if (intent.record.group_id == group_id and intent.record.local_node_id == local_node_id) return intent;
    }
    return null;
}

fn placementIntentsEqual(a: raft_reconciler.PlacementIntent, b: raft_reconciler.PlacementIntent) bool {
    return a.record.group_id == b.record.group_id and
        a.record.replica_id == b.record.replica_id and
        a.record.local_node_id == b.record.local_node_id and
        a.record.metadata_version == b.record.metadata_version and
        a.record.bootstrap_mode == b.record.bootstrap_mode and
        optionalBytesEqual(
            if (a.record.snapshot_bootstrap) |record| record.snapshot_id else null,
            if (b.record.snapshot_bootstrap) |record| record.snapshot_id else null,
        ) and
        optionalBytesEqual(
            if (a.record.backup_restore_bootstrap) |record| record.snapshot_path else null,
            if (b.record.backup_restore_bootstrap) |record| record.snapshot_path else null,
        ) and
        std.mem.eql(u64, a.peer_node_ids, b.peer_node_ids);
}

fn optionalBytesEqual(a: ?[]const u8, b: ?[]const u8) bool {
    if (a == null and b == null) return true;
    if (a == null or b == null) return false;
    return std.mem.eql(u8, a.?, b.?);
}

test "table workflow can build desired topology through the control loop seam" {
    const FakeService = struct {
        table_upserts: usize = 0,
        range_upserts: usize = 0,

        pub fn listProjectedTables(_: *@This(), alloc: std.mem.Allocator) ![]table_manager.TableRecord {
            return try alloc.alloc(table_manager.TableRecord, 0);
        }

        pub fn freeProjectedTables(_: *@This(), alloc: std.mem.Allocator, records: []table_manager.TableRecord) void {
            alloc.free(records);
        }

        pub fn listProjectedRanges(_: *@This(), alloc: std.mem.Allocator) ![]table_manager.RangeRecord {
            return try alloc.alloc(table_manager.RangeRecord, 0);
        }

        pub fn freeProjectedRanges(_: *@This(), alloc: std.mem.Allocator, records: []table_manager.RangeRecord) void {
            alloc.free(records);
        }

        pub fn listProjectedPlacementIntents(_: *@This(), alloc: std.mem.Allocator) ![]raft_reconciler.PlacementIntent {
            return try alloc.alloc(raft_reconciler.PlacementIntent, 0);
        }

        pub fn freeProjectedPlacementIntents(_: *@This(), alloc: std.mem.Allocator, intents: []raft_reconciler.PlacementIntent) void {
            alloc.free(intents);
        }

        pub fn listProjectedSplitTransitions(_: *@This(), alloc: std.mem.Allocator) ![]@import("transition_state.zig").SplitTransitionRecord {
            return try alloc.alloc(@import("transition_state.zig").SplitTransitionRecord, 0);
        }

        pub fn freeProjectedSplitTransitions(_: *@This(), alloc: std.mem.Allocator, records: []@import("transition_state.zig").SplitTransitionRecord) void {
            alloc.free(records);
        }

        pub fn listProjectedMergeTransitions(_: *@This(), alloc: std.mem.Allocator) ![]@import("transition_state.zig").MergeTransitionRecord {
            return try alloc.alloc(@import("transition_state.zig").MergeTransitionRecord, 0);
        }

        pub fn freeProjectedMergeTransitions(_: *@This(), alloc: std.mem.Allocator, records: []@import("transition_state.zig").MergeTransitionRecord) void {
            alloc.free(records);
        }

        pub fn observeSplitTransition(_: *@This(), _: u64) !?@import("transition_state.zig").SplitObservation {
            return null;
        }

        pub fn observeMergeTransition(_: *@This(), _: u64) !?@import("transition_state.zig").MergeObservation {
            return null;
        }

        pub fn applyReconciliationPlan(self: *@This(), plan: *const @import("reconciler.zig").ReconciliationPlan) !void {
            self.table_upserts += plan.table_upserts.len;
            self.range_upserts += plan.range_upserts.len;
        }
    };

    var workflow = TableWorkflow.init(std.testing.allocator);
    defer workflow.deinit();
    var fake = FakeService{};

    const summary = try workflow.createTable(&fake, .{
        .table_id = 55,
        .name = "docs",
        .desired_replica_count = 3,
        .min_ranges = 1,
    }, .{
        .group_id = 5501,
        .table_id = 55,
        .start_key = "doc:a",
        .end_key = "doc:z",
    });

    try std.testing.expectEqual(@as(usize, 1), summary.table_upserts);
    try std.testing.expectEqual(@as(usize, 1), summary.range_upserts);
    try std.testing.expectEqual(@as(usize, 1), fake.table_upserts);
    try std.testing.expectEqual(@as(usize, 1), fake.range_upserts);
}

test "table workflow can reconcile foreign key reference owner ranges" {
    const parent_schema =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}}}
    ;
    const child_schema =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"customer_id":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"foreign_keys":[{"name":"orders_customer_id_fkey","columns":["customer_id"],"references":{"table":"customers","columns":["_id"]},"on_delete":"restrict"}]}
    ;
    const FakeService = struct {
        foreign_key_ref_range_upserts: usize = 0,

        pub fn listProjectedTables(_: *@This(), alloc: std.mem.Allocator) ![]table_manager.TableRecord {
            const records = try alloc.alloc(table_manager.TableRecord, 2);
            errdefer alloc.free(records);
            records[0] = try table_manager.cloneTable(alloc, .{
                .table_id = 10,
                .name = "orders",
                .description = "",
                .schema_json = child_schema,
                .read_schema_json = "",
                .foreign_key_validation_json = "{}",
                .indexes_json = "",
                .replication_sources_json = "",
                .placement_role = "data",
            });
            errdefer table_manager.freeTable(alloc, records[0]);
            records[1] = try table_manager.cloneTable(alloc, .{
                .table_id = 20,
                .name = "customers",
                .description = "",
                .schema_json = parent_schema,
                .read_schema_json = "",
                .foreign_key_validation_json = "{}",
                .indexes_json = "",
                .replication_sources_json = "",
                .placement_role = "data",
            });
            return records;
        }

        pub fn freeProjectedTables(_: *@This(), alloc: std.mem.Allocator, records: []table_manager.TableRecord) void {
            for (records) |record| table_manager.freeTable(alloc, record);
            alloc.free(records);
        }

        pub fn listProjectedRanges(_: *@This(), alloc: std.mem.Allocator) ![]table_manager.RangeRecord {
            return try alloc.alloc(table_manager.RangeRecord, 0);
        }

        pub fn freeProjectedRanges(_: *@This(), alloc: std.mem.Allocator, records: []table_manager.RangeRecord) void {
            alloc.free(records);
        }

        pub fn listProjectedPlacementIntents(_: *@This(), alloc: std.mem.Allocator) ![]raft_reconciler.PlacementIntent {
            return try alloc.alloc(raft_reconciler.PlacementIntent, 0);
        }

        pub fn freeProjectedPlacementIntents(_: *@This(), alloc: std.mem.Allocator, intents: []raft_reconciler.PlacementIntent) void {
            alloc.free(intents);
        }

        pub fn listProjectedSplitTransitions(_: *@This(), alloc: std.mem.Allocator) ![]@import("transition_state.zig").SplitTransitionRecord {
            return try alloc.alloc(@import("transition_state.zig").SplitTransitionRecord, 0);
        }

        pub fn freeProjectedSplitTransitions(_: *@This(), alloc: std.mem.Allocator, records: []@import("transition_state.zig").SplitTransitionRecord) void {
            alloc.free(records);
        }

        pub fn listProjectedMergeTransitions(_: *@This(), alloc: std.mem.Allocator) ![]@import("transition_state.zig").MergeTransitionRecord {
            return try alloc.alloc(@import("transition_state.zig").MergeTransitionRecord, 0);
        }

        pub fn freeProjectedMergeTransitions(_: *@This(), alloc: std.mem.Allocator, records: []@import("transition_state.zig").MergeTransitionRecord) void {
            alloc.free(records);
        }

        pub fn observeSplitTransition(_: *@This(), _: u64) !?@import("transition_state.zig").SplitObservation {
            return null;
        }

        pub fn observeMergeTransition(_: *@This(), _: u64) !?@import("transition_state.zig").MergeObservation {
            return null;
        }

        pub fn applyReconciliationPlan(self: *@This(), plan: *const @import("reconciler.zig").ReconciliationPlan) !void {
            self.foreign_key_ref_range_upserts += plan.foreign_key_ref_range_upserts.len;
        }
    };

    var workflow = TableWorkflow.init(std.testing.allocator);
    defer workflow.deinit();
    var fake = FakeService{};

    const summary = try workflow.upsertForeignKeyReferenceRange(&fake, .{
        .group_id = 3001,
        .child_table_id = 10,
        .constraint_name = "orders_customer_id_fkey",
        .parent_table_id = 20,
        .start_parent_key = "",
        .end_parent_key = null,
        .state = table_manager.foreign_key_ref_range_active,
        .topology_epoch = 1,
    });

    try std.testing.expectEqual(@as(usize, 1), summary.foreign_key_ref_range_upserts);
    try std.testing.expectEqual(@as(usize, 1), fake.foreign_key_ref_range_upserts);
}

test "table workflow drives foreign key reference range lifecycle commands" {
    const FakeService = struct {
        manager: table_manager.TableManager,
        begin_rebuilds: usize = 0,
        finish_rebuilds: usize = 0,
        begin_splits: usize = 0,
        finish_splits: usize = 0,
        begin_merges: usize = 0,
        finish_merges: usize = 0,

        fn init() @This() {
            return .{ .manager = table_manager.TableManager.init(std.testing.allocator) };
        }

        fn deinit(self: *@This()) void {
            self.manager.deinit();
        }

        pub fn listProjectedTables(self: *@This(), alloc: std.mem.Allocator) ![]table_manager.TableRecord {
            return try self.manager.listTables(alloc);
        }

        pub fn freeProjectedTables(self: *@This(), alloc: std.mem.Allocator, records: []table_manager.TableRecord) void {
            self.manager.freeTables(alloc, records);
        }

        pub fn listProjectedRanges(self: *@This(), alloc: std.mem.Allocator) ![]table_manager.RangeRecord {
            return try self.manager.listRanges(alloc);
        }

        pub fn freeProjectedRanges(self: *@This(), alloc: std.mem.Allocator, records: []table_manager.RangeRecord) void {
            self.manager.freeRanges(alloc, records);
        }

        pub fn listProjectedForeignKeyReferenceRanges(self: *@This(), alloc: std.mem.Allocator) ![]table_manager.ForeignKeyReferenceRangeRecord {
            return try self.manager.listForeignKeyReferenceRanges(alloc);
        }

        pub fn freeProjectedForeignKeyReferenceRanges(self: *@This(), alloc: std.mem.Allocator, records: []table_manager.ForeignKeyReferenceRangeRecord) void {
            self.manager.freeForeignKeyReferenceRanges(alloc, records);
        }

        pub fn listProjectedPlacementIntents(_: *@This(), alloc: std.mem.Allocator) ![]raft_reconciler.PlacementIntent {
            return try alloc.alloc(raft_reconciler.PlacementIntent, 0);
        }

        pub fn freeProjectedPlacementIntents(_: *@This(), alloc: std.mem.Allocator, intents: []raft_reconciler.PlacementIntent) void {
            alloc.free(intents);
        }

        pub fn listProjectedSplitTransitions(_: *@This(), alloc: std.mem.Allocator) ![]@import("transition_state.zig").SplitTransitionRecord {
            return try alloc.alloc(@import("transition_state.zig").SplitTransitionRecord, 0);
        }

        pub fn freeProjectedSplitTransitions(_: *@This(), alloc: std.mem.Allocator, records: []@import("transition_state.zig").SplitTransitionRecord) void {
            alloc.free(records);
        }

        pub fn listProjectedMergeTransitions(_: *@This(), alloc: std.mem.Allocator) ![]@import("transition_state.zig").MergeTransitionRecord {
            return try alloc.alloc(@import("transition_state.zig").MergeTransitionRecord, 0);
        }

        pub fn freeProjectedMergeTransitions(_: *@This(), alloc: std.mem.Allocator, records: []@import("transition_state.zig").MergeTransitionRecord) void {
            alloc.free(records);
        }

        pub fn observeSplitTransition(_: *@This(), _: u64) !?@import("transition_state.zig").SplitObservation {
            return null;
        }

        pub fn observeMergeTransition(_: *@This(), _: u64) !?@import("transition_state.zig").MergeObservation {
            return null;
        }

        pub fn beginForeignKeyReferenceRangeRebuild(self: *@This(), selector: table_manager.ForeignKeyReferenceRangeSelector) !void {
            self.begin_rebuilds += 1;
            try self.manager.beginForeignKeyReferenceRangeRebuild(selector);
        }

        pub fn finishForeignKeyReferenceRangeRebuild(self: *@This(), selector: table_manager.ForeignKeyReferenceRangeSelector) !void {
            self.finish_rebuilds += 1;
            try self.manager.finishForeignKeyReferenceRangeRebuild(selector);
        }

        pub fn beginForeignKeyReferenceRangeSplit(self: *@This(), request: table_manager.ForeignKeyReferenceRangeSplitRequest) !void {
            self.begin_splits += 1;
            try self.manager.beginForeignKeyReferenceRangeSplit(request);
        }

        pub fn finishForeignKeyReferenceRangeSplit(self: *@This(), request: table_manager.ForeignKeyReferenceRangeSplitRequest) !void {
            self.finish_splits += 1;
            try self.manager.finishForeignKeyReferenceRangeSplit(request);
        }

        pub fn beginForeignKeyReferenceRangeMerge(self: *@This(), request: table_manager.ForeignKeyReferenceRangeMergeRequest) !void {
            self.begin_merges += 1;
            try self.manager.beginForeignKeyReferenceRangeMerge(request);
        }

        pub fn finishForeignKeyReferenceRangeMerge(self: *@This(), request: table_manager.ForeignKeyReferenceRangeMergeRequest) !void {
            self.finish_merges += 1;
            try self.manager.finishForeignKeyReferenceRangeMerge(request);
        }
    };

    var workflow = TableWorkflow.init(std.testing.allocator);
    defer workflow.deinit();
    var fake = FakeService.init();
    defer fake.deinit();

    try fake.manager.upsertTable(.{ .table_id = 10, .name = "orders" });
    try fake.manager.upsertTable(.{ .table_id = 20, .name = "customers" });
    try fake.manager.upsertForeignKeyReferenceRange(.{
        .child_table_id = 10,
        .constraint_name = "orders_customer_id_fkey",
        .parent_table_id = 20,
        .start_parent_key = "",
        .end_parent_key = null,
        .group_id = 3001,
    });
    const selector: table_manager.ForeignKeyReferenceRangeSelector = .{
        .child_table_id = 10,
        .constraint_name = "orders_customer_id_fkey",
        .parent_table_id = 20,
        .start_parent_key = "",
    };

    try workflow.beginForeignKeyReferenceRangeRebuild(&fake, selector);
    try workflow.finishForeignKeyReferenceRangeRebuild(&fake, selector);

    const split_request: table_manager.ForeignKeyReferenceRangeSplitRequest = .{
        .selector = selector,
        .split_parent_key = "customer:m",
        .left_group_id = 3001,
        .right_group_id = 3002,
    };
    try workflow.beginForeignKeyReferenceRangeSplit(&fake, split_request);
    try workflow.finishForeignKeyReferenceRangeSplit(&fake, split_request);

    const merge_request: table_manager.ForeignKeyReferenceRangeMergeRequest = .{
        .left_selector = selector,
        .right_start_parent_key = "customer:m",
        .merged_group_id = 3001,
    };
    try workflow.beginForeignKeyReferenceRangeMerge(&fake, merge_request);
    try workflow.finishForeignKeyReferenceRangeMerge(&fake, merge_request);

    try std.testing.expectEqual(@as(usize, 1), fake.begin_rebuilds);
    try std.testing.expectEqual(@as(usize, 1), fake.finish_rebuilds);
    try std.testing.expectEqual(@as(usize, 1), fake.begin_splits);
    try std.testing.expectEqual(@as(usize, 1), fake.finish_splits);
    try std.testing.expectEqual(@as(usize, 1), fake.begin_merges);
    try std.testing.expectEqual(@as(usize, 1), fake.finish_merges);

    const ranges = try fake.manager.listForeignKeyReferenceRanges(std.testing.allocator);
    defer fake.manager.freeForeignKeyReferenceRanges(std.testing.allocator, ranges);
    try std.testing.expectEqual(@as(usize, 1), ranges.len);
    try std.testing.expectEqualStrings("", ranges[0].start_parent_key);
    try std.testing.expect(ranges[0].end_parent_key == null);
    try std.testing.expectEqual(@as(u64, 3001), ranges[0].group_id);
    try std.testing.expectEqualStrings(table_manager.foreign_key_ref_range_active, ranges[0].state);
}

test "table workflow create preserves existing projected topology" {
    const FakeService = struct {
        table_upserts: usize = 0,
        range_upserts: usize = 0,
        table_removals: usize = 0,
        range_removals: usize = 0,

        pub fn listProjectedTables(_: *@This(), alloc: std.mem.Allocator) ![]table_manager.TableRecord {
            const records = try alloc.alloc(table_manager.TableRecord, 1);
            errdefer alloc.free(records);
            records[0] = try table_manager.cloneTable(alloc, .{
                .table_id = 7,
                .name = "docs",
                .placement_role = "data",
            });
            return records;
        }

        pub fn freeProjectedTables(_: *@This(), alloc: std.mem.Allocator, records: []table_manager.TableRecord) void {
            for (records) |record| table_manager.freeTable(alloc, record);
            alloc.free(records);
        }

        pub fn listProjectedRanges(_: *@This(), alloc: std.mem.Allocator) ![]table_manager.RangeRecord {
            const records = try alloc.alloc(table_manager.RangeRecord, 1);
            records[0] = .{
                .group_id = 7001,
                .table_id = 7,
                .start_key = try alloc.dupe(u8, ""),
                .end_key = null,
            };
            return records;
        }

        pub fn freeProjectedRanges(_: *@This(), alloc: std.mem.Allocator, records: []table_manager.RangeRecord) void {
            for (records) |record| table_manager.freeRange(alloc, record);
            alloc.free(records);
        }

        pub fn listProjectedPlacementIntents(_: *@This(), alloc: std.mem.Allocator) ![]raft_reconciler.PlacementIntent {
            return try alloc.alloc(raft_reconciler.PlacementIntent, 0);
        }

        pub fn freeProjectedPlacementIntents(_: *@This(), alloc: std.mem.Allocator, intents: []raft_reconciler.PlacementIntent) void {
            alloc.free(intents);
        }

        pub fn listProjectedSplitTransitions(_: *@This(), alloc: std.mem.Allocator) ![]@import("transition_state.zig").SplitTransitionRecord {
            return try alloc.alloc(@import("transition_state.zig").SplitTransitionRecord, 0);
        }

        pub fn freeProjectedSplitTransitions(_: *@This(), alloc: std.mem.Allocator, records: []@import("transition_state.zig").SplitTransitionRecord) void {
            alloc.free(records);
        }

        pub fn listProjectedMergeTransitions(_: *@This(), alloc: std.mem.Allocator) ![]@import("transition_state.zig").MergeTransitionRecord {
            return try alloc.alloc(@import("transition_state.zig").MergeTransitionRecord, 0);
        }

        pub fn freeProjectedMergeTransitions(_: *@This(), alloc: std.mem.Allocator, records: []@import("transition_state.zig").MergeTransitionRecord) void {
            alloc.free(records);
        }

        pub fn observeSplitTransition(_: *@This(), _: u64) !?@import("transition_state.zig").SplitObservation {
            return null;
        }

        pub fn observeMergeTransition(_: *@This(), _: u64) !?@import("transition_state.zig").MergeObservation {
            return null;
        }

        pub fn applyReconciliationPlan(self: *@This(), plan: *const @import("reconciler.zig").ReconciliationPlan) !void {
            self.table_upserts += plan.table_upserts.len;
            self.range_upserts += plan.range_upserts.len;
            self.table_removals += plan.table_removals.len;
            self.range_removals += plan.range_removals.len;
        }
    };

    var workflow = TableWorkflow.init(std.testing.allocator);
    defer workflow.deinit();
    var fake = FakeService{};

    _ = try workflow.createTable(&fake, .{
        .table_id = 8,
        .name = "logs",
        .placement_role = "data",
    }, .{
        .group_id = 8001,
        .table_id = 8,
        .start_key = "",
        .end_key = null,
    });

    try std.testing.expectEqual(@as(usize, 1), fake.table_upserts);
    try std.testing.expectEqual(@as(usize, 1), fake.range_upserts);
    try std.testing.expectEqual(@as(usize, 0), fake.table_removals);
    try std.testing.expectEqual(@as(usize, 0), fake.range_removals);
}

test "table workflow can remove a table topology from desired state" {
    const FakeService = struct {
        table_removals: usize = 0,
        range_removals: usize = 0,

        pub fn listProjectedTables(_: *@This(), alloc: std.mem.Allocator) ![]table_manager.TableRecord {
            const out = try alloc.alloc(table_manager.TableRecord, 1);
            out[0] = .{
                .table_id = 9,
                .name = try alloc.dupe(u8, "docs"),
                .placement_role = try alloc.dupe(u8, "data"),
            };
            return out;
        }

        pub fn freeProjectedTables(_: *@This(), alloc: std.mem.Allocator, records: []table_manager.TableRecord) void {
            for (records) |record| {
                alloc.free(record.name);
                alloc.free(record.placement_role);
            }
            alloc.free(records);
        }

        pub fn listProjectedRanges(_: *@This(), alloc: std.mem.Allocator) ![]table_manager.RangeRecord {
            const out = try alloc.alloc(table_manager.RangeRecord, 2);
            out[0] = .{ .group_id = 901, .table_id = 9, .start_key = try alloc.dupe(u8, "doc:a"), .end_key = try alloc.dupe(u8, "doc:m") };
            out[1] = .{ .group_id = 902, .table_id = 9, .start_key = try alloc.dupe(u8, "doc:m"), .end_key = try alloc.dupe(u8, "doc:z") };
            return out;
        }

        pub fn freeProjectedRanges(_: *@This(), alloc: std.mem.Allocator, records: []table_manager.RangeRecord) void {
            for (records) |record| {
                alloc.free(record.start_key);
                if (record.end_key) |end| alloc.free(end);
            }
            alloc.free(records);
        }

        pub fn listProjectedPlacementIntents(_: *@This(), alloc: std.mem.Allocator) ![]raft_reconciler.PlacementIntent {
            return try alloc.alloc(raft_reconciler.PlacementIntent, 0);
        }

        pub fn freeProjectedPlacementIntents(_: *@This(), alloc: std.mem.Allocator, intents: []raft_reconciler.PlacementIntent) void {
            alloc.free(intents);
        }

        pub fn listProjectedSplitTransitions(_: *@This(), alloc: std.mem.Allocator) ![]@import("transition_state.zig").SplitTransitionRecord {
            return try alloc.alloc(@import("transition_state.zig").SplitTransitionRecord, 0);
        }

        pub fn freeProjectedSplitTransitions(_: *@This(), alloc: std.mem.Allocator, records: []@import("transition_state.zig").SplitTransitionRecord) void {
            alloc.free(records);
        }

        pub fn listProjectedMergeTransitions(_: *@This(), alloc: std.mem.Allocator) ![]@import("transition_state.zig").MergeTransitionRecord {
            return try alloc.alloc(@import("transition_state.zig").MergeTransitionRecord, 0);
        }

        pub fn freeProjectedMergeTransitions(_: *@This(), alloc: std.mem.Allocator, records: []@import("transition_state.zig").MergeTransitionRecord) void {
            alloc.free(records);
        }

        pub fn observeSplitTransition(_: *@This(), _: u64) !?@import("transition_state.zig").SplitObservation {
            return null;
        }

        pub fn observeMergeTransition(_: *@This(), _: u64) !?@import("transition_state.zig").MergeObservation {
            return null;
        }

        pub fn applyReconciliationPlan(self: *@This(), plan: *const @import("reconciler.zig").ReconciliationPlan) !void {
            self.table_removals += plan.table_removals.len;
            self.range_removals += plan.range_removals.len;
        }
    };

    var workflow = TableWorkflow.init(std.testing.allocator);
    defer workflow.deinit();
    var fake = FakeService{};
    try workflow.bootstrapDesiredFromCommitted(&fake);
    const summary = try workflow.dropTable(&fake, 9);
    try std.testing.expectEqual(@as(usize, 1), summary.table_removals);
    try std.testing.expectEqual(@as(usize, 2), summary.range_removals);
    try std.testing.expectEqual(@as(usize, 1), fake.table_removals);
    try std.testing.expectEqual(@as(usize, 2), fake.range_removals);
}

test "table workflow can reconcile projected local placement intents" {
    const FakeService = struct {
        alloc: std.mem.Allocator,
        intents: std.ArrayListUnmanaged(raft_reconciler.PlacementIntent) = .empty,

        fn deinit(self: *@This()) void {
            for (self.intents.items) |intent| if (intent.peer_node_ids.len > 0) self.alloc.free(intent.peer_node_ids);
            self.intents.deinit(self.alloc);
        }

        pub fn listProjectedPlacementIntents(self: *@This(), alloc: std.mem.Allocator) ![]raft_reconciler.PlacementIntent {
            const out = try alloc.alloc(raft_reconciler.PlacementIntent, self.intents.items.len);
            errdefer alloc.free(out);
            for (self.intents.items, 0..) |intent, i| {
                out[i] = .{
                    .record = intent.record,
                    .peer_node_ids = if (intent.peer_node_ids.len == 0) &.{} else try alloc.dupe(u64, intent.peer_node_ids),
                };
            }
            return out;
        }

        pub fn freeProjectedPlacementIntents(_: *@This(), alloc: std.mem.Allocator, intents: []raft_reconciler.PlacementIntent) void {
            for (intents) |intent| if (intent.peer_node_ids.len > 0) alloc.free(intent.peer_node_ids);
            alloc.free(intents);
        }

        fn upsertReplicaIntent(self: *@This(), intent: raft_reconciler.PlacementIntent) !void {
            for (self.intents.items) |*existing| {
                if (existing.record.group_id != intent.record.group_id or existing.record.local_node_id != intent.record.local_node_id) continue;
                if (existing.peer_node_ids.len > 0) self.alloc.free(existing.peer_node_ids);
                existing.* = .{
                    .record = intent.record,
                    .peer_node_ids = if (intent.peer_node_ids.len == 0) &.{} else try self.alloc.dupe(u64, intent.peer_node_ids),
                };
                return;
            }
            try self.intents.append(self.alloc, .{
                .record = intent.record,
                .peer_node_ids = if (intent.peer_node_ids.len == 0) &.{} else try self.alloc.dupe(u64, intent.peer_node_ids),
            });
        }

        fn removeReplicaIntent(self: *@This(), group_id: u64, local_node_id: u64) !void {
            var i: usize = 0;
            while (i < self.intents.items.len) : (i += 1) {
                const intent = self.intents.items[i];
                if (intent.record.group_id != group_id or intent.record.local_node_id != local_node_id) continue;
                if (intent.peer_node_ids.len > 0) self.alloc.free(intent.peer_node_ids);
                _ = self.intents.orderedRemove(i);
                return;
            }
        }
    };

    var workflow = TableWorkflow.init(std.testing.allocator);
    defer workflow.deinit();
    try workflow.controlLoop().stateRef().tableManager().upsertTable(.{
        .table_id = 12,
        .name = "docs",
        .desired_replica_count = 3,
    });
    try workflow.controlLoop().stateRef().tableManager().upsertRange(.{
        .group_id = 1201,
        .table_id = 12,
        .start_key = "doc:a",
        .end_key = "doc:z",
    });

    var fake = FakeService{ .alloc = std.testing.allocator };
    defer fake.deinit();

    const first = try workflow.reconcileLocalPlacementIntents(&fake, std.testing.allocator, 2, &.{ 1, 2, 3 });
    try std.testing.expectEqual(@as(usize, 1), first.upserts);
    try std.testing.expectEqual(@as(usize, 0), first.removals);
    try std.testing.expectEqual(@as(usize, 1), fake.intents.items.len);

    const second = try workflow.reconcileLocalPlacementIntents(&fake, std.testing.allocator, 2, &.{ 1, 3 });
    try std.testing.expectEqual(@as(usize, 0), second.upserts);
    try std.testing.expectEqual(@as(usize, 1), second.removals);
    try std.testing.expectEqual(@as(usize, 0), fake.intents.items.len);
}
