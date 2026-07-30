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
const placement_planner = @import("placement_planner.zig");
const raft_reconciler = @import("../raft/reconciler.zig");
const table_manager = @import("table_manager.zig");
const platform_clock = @import("antfly_platform").clock;
const platform_time = @import("antfly_platform").time;
const reallocation_request = @import("reallocation_request.zig");
const transition_controller = @import("transition_controller.zig");
const transition_state = @import("transition_state.zig");

const doc_identity_transition_rollback_reason = "doc_identity_namespace_mismatch";
const doc_identity_merge_rollback_reason = doc_identity_transition_rollback_reason;

pub const SplitRuntimeObservation = struct {
    transition_id: u64,
    observation: transition_state.SplitObservation,
};

pub const MergeRuntimeObservation = struct {
    transition_id: u64,
    observation: transition_state.MergeObservation,
};

pub const MedianKeyLookup = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        fetch_median_key: *const fn (ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64) anyerror!?[]u8,
    };

    pub fn fetchMedianKey(self: MedianKeyLookup, alloc: std.mem.Allocator, group_id: u64) !?[]u8 {
        return try self.vtable.fetch_median_key(self.ptr, alloc, group_id);
    }
};

pub const CurrentMetadataState = struct {
    placement_intents: []const raft_reconciler.PlacementIntent = &.{},
    tables: []const table_manager.TableRecord = &.{},
    ranges: []const table_manager.RangeRecord = &.{},
    stores: []const table_manager.StoreRecord = &.{},
    merged_group_statuses: []const MergedGroupStatus = &.{},
    restore_progresses: []const table_manager.RestoreProgressRecord = &.{},
    reallocation_request: ?reallocation_request.ReallocationRequestRecord = null,
    schema_progresses: []const table_manager.SchemaProgressRecord = &.{},
    split_transitions: []const transition_state.SplitTransitionRecord = &.{},
    merge_transitions: []const transition_state.MergeTransitionRecord = &.{},
    split_observations: []const SplitRuntimeObservation = &.{},
    merge_observations: []const MergeRuntimeObservation = &.{},
};

pub const MergedGroupStatus = struct {
    group_id: u64,
    doc_count: u64 = 0,
    disk_bytes: u64 = 0,
    disk_bytes_known: bool = false,
    empty: bool = true,
    created_at_millis: u64 = 0,
    updated_at_millis: u64 = 0,
    leader_known: bool = false,
    leader_store_id: u64 = 0,
    voter_count_known: bool = false,
    voter_count: u16 = 0,
    voter_set_known: bool = false,
    voter_set_fingerprint: table_manager.VoterSetFingerprint = [_]u8{0} ** table_manager.voter_set_fingerprint_len,
    healthy_voter_reports: u16 = 0,
    joint_consensus: bool = false,
    readiness_from_leader: bool = false,
    transition_pending: bool = false,
    replay_required: bool = false,
    replay_caught_up: bool = false,
    cutover_ready: bool = false,
    reads_ready_after_cutover: bool = false,
    doc_identity_reassignment_active: bool = false,
    restore_pending: bool = false,
    doc_identity_lifecycle: []const u8 = doc_identity_lifecycle_unknown,
    doc_identity: table_manager.RuntimeDocIdentityStatusReport = .{},
    doc_identity_namespace_conflict: bool = false,
};

pub const doc_identity_lifecycle_unknown = "unknown";
pub const doc_identity_lifecycle_preserving = "preserving";
pub const doc_identity_lifecycle_reassigning = "reassigning";
pub const doc_identity_lifecycle_rebuild_required = "rebuild_required";
pub const doc_identity_lifecycle_ready = "ready";

pub const PlannedSplitStep = struct {
    record: transition_state.SplitTransitionRecord,
    execution: transition_controller.SplitExecutionState,
};

pub const PlannedMergeStep = struct {
    record: transition_state.MergeTransitionRecord,
    execution: transition_controller.MergeExecutionState,
};

pub const PlacementRemoval = struct {
    group_id: u64,
    local_node_id: u64,
};

pub const SplitAdmission = struct {
    expected_source_epoch: u64,
    record: transition_state.SplitTransitionRecord,
};

pub const PlacementChangeKind = enum {
    stable,
    repair_required,
    rebalance,
};

pub const ReconciliationPlan = struct {
    placement_upserts: []raft_reconciler.PlacementIntent,
    table_upserts: []table_manager.TableRecord,
    range_upserts: []table_manager.RangeRecord,
    split_admissions: []SplitAdmission,
    split_upserts: []transition_state.SplitTransitionRecord,
    merge_upserts: []transition_state.MergeTransitionRecord,
    placement_removals: []PlacementRemoval,
    table_removals: []u64,
    range_removals: []u64,
    split_removals: []u64,
    merge_removals: []u64,
    split_steps: []PlannedSplitStep,
    merge_steps: []PlannedMergeStep,
    repair_placement_groups: usize = 0,
    rebalance_placement_groups: usize = 0,
    forced_reallocation: bool = false,
    clear_reallocation_request: ?u128 = null,

    pub fn empty() ReconciliationPlan {
        return .{
            .placement_upserts = &.{},
            .table_upserts = &.{},
            .range_upserts = &.{},
            .split_admissions = &.{},
            .split_upserts = &.{},
            .merge_upserts = &.{},
            .placement_removals = &.{},
            .table_removals = &.{},
            .range_removals = &.{},
            .split_removals = &.{},
            .merge_removals = &.{},
            .split_steps = &.{},
            .merge_steps = &.{},
            .repair_placement_groups = 0,
            .rebalance_placement_groups = 0,
            .forced_reallocation = false,
            .clear_reallocation_request = null,
        };
    }

    pub fn deinit(self: *ReconciliationPlan, alloc: std.mem.Allocator) void {
        for (self.placement_upserts) |intent| raft_reconciler.freeIntentOwned(alloc, intent);
        alloc.free(self.placement_upserts);
        for (self.table_upserts) |record| table_manager.freeTable(alloc, record);
        alloc.free(self.table_upserts);
        for (self.range_upserts) |record| table_manager.freeRange(alloc, record);
        alloc.free(self.range_upserts);
        for (self.split_admissions) |admission| table_manager.freeSplitTransitionRecord(alloc, admission.record);
        alloc.free(self.split_admissions);
        for (self.split_upserts) |record| table_manager.freeSplitTransitionRecord(alloc, record);
        alloc.free(self.split_upserts);
        for (self.merge_upserts) |record| table_manager.freeMergeTransitionRecord(alloc, record);
        alloc.free(self.merge_upserts);
        alloc.free(self.placement_removals);
        alloc.free(self.table_removals);
        alloc.free(self.range_removals);
        alloc.free(self.split_removals);
        alloc.free(self.merge_removals);
        for (self.split_steps) |step| table_manager.freeSplitTransitionRecord(alloc, step.record);
        alloc.free(self.split_steps);
        for (self.merge_steps) |step| table_manager.freeMergeTransitionRecord(alloc, step.record);
        alloc.free(self.merge_steps);
        self.* = undefined;
    }
};

pub const Reconciler = struct {
    alloc: std.mem.Allocator,
    config: Config,
    shard_cooldowns: std.AutoHashMapUnmanaged(u64, u64) = .empty,

    pub const Config = struct {
        max_shard_size_bytes: u64 = 0,
        min_shard_size_bytes: u64 = 0,
        min_shards_per_table: u32 = 1,
        max_shards_per_table: u32 = 0,
        disable_shard_alloc: bool = false,
        auto_range_transition_per_table_limit: u32 = 1,
        auto_range_transition_cluster_limit: u32 = 1,
        stats_stale_after_millis: u64 = 60 * std.time.ms_per_s,
        stats_clock_skew_millis: u64 = 30 * std.time.ms_per_s,
        shard_cooldown_millis: u64 = 60 * std.time.ms_per_s,
        min_shard_merge_age_millis: u64 = 5 * 60 * std.time.ms_per_s,
        median_key_lookup: ?MedianKeyLookup = null,
        clock: platform_clock.Clock = platform_clock.Clock.real(),
    };

    pub fn init(alloc: std.mem.Allocator) Reconciler {
        return initWithConfig(alloc, .{});
    }

    pub fn initWithConfig(alloc: std.mem.Allocator, config: Config) Reconciler {
        return .{
            .alloc = alloc,
            .config = config,
        };
    }

    pub fn deinit(self: *Reconciler) void {
        self.shard_cooldowns.deinit(self.alloc);
        self.* = undefined;
    }

    pub fn setMedianKeyLookup(self: *Reconciler, lookup: ?MedianKeyLookup) void {
        self.config.median_key_lookup = lookup;
    }

    pub fn computePlan(
        self: *Reconciler,
        manager: *table_manager.TableManager,
        placement_candidate_node_ids: []const u64,
        placement_candidate_info: []const @import("state.zig").CandidatePlacementInfo,
        current: CurrentMetadataState,
    ) !ReconciliationPlan {
        const now_monotonic_ms = monotonicMillis();
        const now_realtime_ms = self.config.clock.nowRealtimeMs();
        self.cleanupExpiredShardCooldowns(now_monotonic_ms);
        try self.recordCompletedTransitionCooldowns(current, now_monotonic_ms);
        var planner = placement_planner.PlacementPlanner.init(self.alloc);
        const desired_tables = try manager.listTables(self.alloc);
        defer manager.freeTables(self.alloc, desired_tables);
        var evidence = try StoreEvidenceIndex.init(self.alloc, current);
        defer evidence.deinit();
        try self.syncAutomaticShardIntents(
            manager,
            current,
            &evidence,
            now_monotonic_ms,
            now_realtime_ms,
        );
        const desired_ranges = try manager.listRanges(self.alloc);
        defer manager.freeRanges(self.alloc, desired_ranges);
        const desired_splits = try manager.listDesiredSplitTransitions(self.alloc);
        defer manager.freeSplitTransitions(self.alloc, desired_splits);
        const desired_merges = try manager.listDesiredMergeTransitions(self.alloc);
        defer manager.freeMergeTransitions(self.alloc, desired_merges);
        const split_provisioning_ranges = try allocSplitProvisioningRanges(self.alloc, desired_ranges, desired_splits);
        defer self.alloc.free(split_provisioning_ranges);

        // A forced rebalance and a range transition both change Raft
        // membership. Planning them together can expand the source voter set
        // while the transition driver is trying to prepare that same leader.
        // Serialize forced placement movement behind split/merge work. Health-
        // driven exclusion still flows through candidate.retain_current and
        // therefore remains able to repair failed or draining stores.
        const force_placement_reallocation = current.reallocation_request != null and
            desired_splits.len == 0 and
            desired_merges.len == 0 and
            current.split_transitions.len == 0 and
            current.merge_transitions.len == 0;
        const candidate_domains = try self.alloc.alloc(placement_planner.CandidateDomain, placement_candidate_info.len);
        defer self.alloc.free(candidate_domains);
        for (placement_candidate_info, 0..) |candidate, i| {
            candidate_domains[i] = .{
                .node_id = candidate.node_id,
                .store_id = candidate.store_id,
                .role = candidate.role,
                .failure_domain = candidate.failure_domain,
                .priority = candidate.priority,
                .status_tag = candidate.status_tag,
                .available_bytes = candidate.available_bytes,
                .lease_pressure = candidate.lease_pressure,
                .read_load = candidate.read_load,
                .write_load = candidate.write_load,
                .retain_current = if (force_placement_reallocation) false else candidate.retain_current,
                .force_reallocate = force_placement_reallocation,
            };
        }
        const protected_placement_groups = try allocUnconvergedPlacementGroups(
            self.alloc,
            current,
            desired_splits,
            desired_merges,
            &evidence,
        );
        defer self.alloc.free(protected_placement_groups);
        const desired_placements = if (placement_candidate_node_ids.len > 0)
            try planner.planAllIntentsWithConstraints(
                manager,
                placement_candidate_node_ids,
                current.placement_intents,
                candidate_domains,
                split_provisioning_ranges,
                protected_placement_groups,
            )
        else
            try self.alloc.alloc(raft_reconciler.PlacementIntent, 0);
        defer planner.freeIntents(self.alloc, desired_placements);
        var membership_index = try MembershipTransitionIndex.init(self.alloc, current, desired_placements, &evidence);
        defer membership_index.deinit();
        var active_transition_contracts = try ActiveTransitionContractIndex.init(
            self.alloc,
            current,
        );
        defer active_transition_contracts.deinit();
        try active_transition_contracts.fenceAdjacentRangeMutations(
            current.ranges,
        );
        try active_transition_contracts.fenceAdjacentRangeMutations(
            desired_ranges,
        );
        var table_upserts = std.ArrayListUnmanaged(table_manager.TableRecord).empty;
        var placement_upserts = std.ArrayListUnmanaged(raft_reconciler.PlacementIntent).empty;
        errdefer {
            for (placement_upserts.items) |intent| raft_reconciler.freeIntentOwned(self.alloc, intent);
            placement_upserts.deinit(self.alloc);
        }
        errdefer {
            for (table_upserts.items) |record| table_manager.freeTable(self.alloc, record);
            table_upserts.deinit(self.alloc);
        }
        var range_upserts = std.ArrayListUnmanaged(table_manager.RangeRecord).empty;
        errdefer {
            for (range_upserts.items) |record| table_manager.freeRange(self.alloc, record);
            range_upserts.deinit(self.alloc);
        }
        var split_upserts = std.ArrayListUnmanaged(transition_state.SplitTransitionRecord).empty;
        errdefer {
            for (split_upserts.items) |record| table_manager.freeSplitTransitionRecord(self.alloc, record);
            split_upserts.deinit(self.alloc);
        }
        var split_admissions = std.ArrayListUnmanaged(SplitAdmission).empty;
        errdefer {
            for (split_admissions.items) |admission| table_manager.freeSplitTransitionRecord(self.alloc, admission.record);
            split_admissions.deinit(self.alloc);
        }
        var merge_upserts = std.ArrayListUnmanaged(transition_state.MergeTransitionRecord).empty;
        errdefer {
            for (merge_upserts.items) |record| table_manager.freeMergeTransitionRecord(self.alloc, record);
            merge_upserts.deinit(self.alloc);
        }
        var table_removals = std.ArrayListUnmanaged(u64).empty;
        var placement_removals = std.ArrayListUnmanaged(PlacementRemoval).empty;
        errdefer placement_removals.deinit(self.alloc);
        errdefer table_removals.deinit(self.alloc);
        var range_removals = std.ArrayListUnmanaged(u64).empty;
        errdefer range_removals.deinit(self.alloc);
        var split_removals = std.ArrayListUnmanaged(u64).empty;
        errdefer split_removals.deinit(self.alloc);
        var merge_removals = std.ArrayListUnmanaged(u64).empty;
        errdefer merge_removals.deinit(self.alloc);
        var split_steps = std.ArrayListUnmanaged(PlannedSplitStep).empty;
        errdefer {
            for (split_steps.items) |step| table_manager.freeSplitTransitionRecord(self.alloc, step.record);
            split_steps.deinit(self.alloc);
        }
        var merge_steps = std.ArrayListUnmanaged(PlannedMergeStep).empty;
        errdefer {
            for (merge_steps.items) |step| table_manager.freeMergeTransitionRecord(self.alloc, step.record);
            merge_steps.deinit(self.alloc);
        }

        for (desired_placements) |desired| {
            {
                if (membership_index.deferDesiredPlacement(desired.record.group_id, desired.record.local_node_id)) continue;
                const contracting = membership_index.contracting(desired.record.group_id);
                const latched_peers = membership_index.latchedFinalPeers(desired.record.group_id);
                const transition_peers = if (contracting or latched_peers != null)
                    null
                else if (!membership_index.needsExpansion(desired.record.group_id))
                    null
                else
                    try allocExpandedTransitionPeerNodeIds(self.alloc, current.placement_intents, desired);
                defer if (transition_peers) |peers| self.alloc.free(peers);
                var transition_intent = desired;
                transition_intent.peer_node_ids = latched_peers orelse transition_peers orelse desired.peer_node_ids;

                const effective = effectivePlacementIntent(current, desired_tables, desired_ranges, &evidence, transition_intent);
                const existing = membership_index.currentIntent(effective.record.group_id, effective.record.local_node_id);
                if (existing == null or !placementIntentsEqual(existing.?, effective)) {
                    try placement_upserts.ensureUnusedCapacity(self.alloc, 1);
                    placement_upserts.appendAssumeCapacity(
                        try clonePlacementIntent(self.alloc, effective),
                    );
                }
            }
        }
        for (desired_tables) |*desired| {
            try maybeFinalizeSchemaMigration(self.alloc, current, desired);
            const existing = findTableRecord(current.tables, desired.table_id);
            if (existing == null or !tableRecordsEqual(existing.?, desired.*)) {
                if (active_transition_contracts.get(desired.table_id)) |contract| {
                    const current_table = existing orelse
                        return error.TransitionTableContractViolated;
                    if (!contract.matches(current_table))
                        return error.TransitionTableContractViolated;
                    // Schema and index mutation is serialized behind the
                    // active range generation. Non-structural table metadata
                    // can still advance without changing how transition DBs
                    // are opened.
                    if (!contract.matches(desired.*))
                        continue;
                }
                try table_upserts.ensureUnusedCapacity(self.alloc, 1);
                table_upserts.appendAssumeCapacity(
                    try table_manager.cloneTable(self.alloc, desired.*),
                );
            }
        }
        for (desired_ranges) |desired| {
            const existing = findRangeRecord(current.ranges, desired.group_id);
            if (active_transition_contracts.rangeMutationFenced(desired.group_id) or
                (existing != null and
                    active_transition_contracts.rangeMutationFenced(existing.?.group_id)))
            {
                continue;
            }

            switch (splitRangePublication(current, desired_splits, desired)) {
                .publish_epoch => |published_epoch| {
                    var publishable = desired;
                    publishable.split_attempt_epoch = published_epoch;
                    if (existing == null or !rangeRecordsEqual(existing.?, publishable)) {
                        try range_upserts.ensureUnusedCapacity(self.alloc, 1);
                        range_upserts.appendAssumeCapacity(
                            try table_manager.cloneRange(self.alloc, publishable),
                        );
                    }
                    continue;
                },
                .blocked => continue,
                .none => {},
            }

            if (existing == null or !rangeRecordsEqual(existing.?, desired)) {
                try range_upserts.ensureUnusedCapacity(self.alloc, 1);
                range_upserts.appendAssumeCapacity(
                    try table_manager.cloneRange(self.alloc, desired),
                );
            }
        }
        for (desired_splits) |desired| {
            const existing = findSplitRecord(current.split_transitions, desired.transition_id);
            if (existing == null) {
                try desired.table_contract.validateForSplit();
                const current_table = findTableRecord(
                    current.tables,
                    desired.table_contract.table_id,
                ) orelse continue;
                if (!tableMatchesTransitionContract(
                    current_table,
                    desired.table_contract,
                )) continue;
                if (!splitTransitionDocIdentityCompatibleIndexed(current, &evidence, desired)) continue;
                if (splitAdmissionExpectedEpoch(current, desired_ranges, desired)) |expected_source_epoch| {
                    try split_admissions.ensureUnusedCapacity(self.alloc, 1);
                    split_admissions.appendAssumeCapacity(.{
                        .expected_source_epoch = expected_source_epoch,
                        .record = try cloneSplitRecord(self.alloc, desired),
                    });
                }
                continue;
            }

            // Admission fixes the transition identity. Desired state may add a
            // rollback request, but it must never rewrite the epoch or routing
            // coordinates underneath already-applied data-group commands.
            var effective_record = try cloneSplitRecord(self.alloc, existing.?);
            var effective_record_owned = true;
            errdefer if (effective_record_owned) table_manager.freeSplitTransitionRecord(self.alloc, effective_record);
            if (effective_record.rollback_reason == null and splitTransitionCanRollback(existing.?)) {
                if (desired.rollback_reason) |reason| {
                    effective_record.rollback_reason = try self.alloc.dupe(u8, reason);
                } else if (!splitTransitionDocIdentityCompatibleIndexed(current, &evidence, existing.?)) {
                    effective_record.rollback_reason = try self.alloc.dupe(u8, doc_identity_transition_rollback_reason);
                }
            }

            if (!splitRecordsEqual(existing.?, effective_record)) {
                try split_upserts.append(self.alloc, effective_record);
                effective_record_owned = false;
                continue;
            }
            table_manager.freeSplitTransitionRecord(self.alloc, effective_record);
            effective_record_owned = false;

            const observation = findSplitObservation(current.split_observations, desired.transition_id) orelse defaultSplitObservation();
            try split_steps.ensureUnusedCapacity(self.alloc, 1);
            const planned_record = try cloneSplitRecord(self.alloc, existing.?);
            split_steps.appendAssumeCapacity(.{
                .record = planned_record,
                .execution = transition_controller.TransitionController.describeSplit(planned_record, observation),
            });
        }

        for (desired_merges) |desired| {
            const existing = findMergeRecord(current.merge_transitions, desired.transition_id);
            if (existing == null) {
                desired.table_contract.validateForMerge(
                    desired.allow_doc_identity_reassignment,
                ) catch continue;
                const current_table = findTableRecord(
                    current.tables,
                    desired.table_contract.table_id,
                ) orelse continue;
                if (!tableMatchesTransitionContract(
                    current_table,
                    desired.table_contract,
                )) continue;
                if (!mergeTransitionDocIdentityCompatibleIndexed(current, &evidence, desired, .disallow_active)) continue;
                try merge_upserts.ensureUnusedCapacity(self.alloc, 1);
                merge_upserts.appendAssumeCapacity(
                    try cloneMergeRecord(self.alloc, desired),
                );
                continue;
            }

            // As with splits, only rollback intent is mutable after admission.
            var effective_record = try cloneMergeRecord(self.alloc, existing.?);
            var effective_record_owned = true;
            errdefer if (effective_record_owned) table_manager.freeMergeTransitionRecord(self.alloc, effective_record);
            if (effective_record.rollback_reason == null and mergeTransitionCanRollback(existing.?)) {
                if (desired.rollback_reason) |reason| {
                    effective_record.rollback_reason = try self.alloc.dupe(u8, reason);
                } else if (!mergeTransitionDocIdentityCompatibleIndexed(current, &evidence, existing.?, .allow_existing_active)) {
                    effective_record.rollback_reason = try self.alloc.dupe(u8, doc_identity_merge_rollback_reason);
                }
            }

            if (!mergeRecordsEqual(existing.?, effective_record)) {
                try merge_upserts.append(self.alloc, effective_record);
                effective_record_owned = false;
                continue;
            }
            table_manager.freeMergeTransitionRecord(self.alloc, effective_record);
            effective_record_owned = false;

            const observation = findMergeObservation(current.merge_observations, desired.transition_id) orelse defaultMergeObservation(existing.?);
            try merge_steps.ensureUnusedCapacity(self.alloc, 1);
            const planned_record = try cloneMergeRecord(self.alloc, existing.?);
            merge_steps.appendAssumeCapacity(.{
                .record = planned_record,
                .execution = transition_controller.TransitionController.describeMerge(planned_record, observation),
            });
        }

        for (current.placement_intents) |intent| {
            if (!membership_index.hasDesiredMember(intent.record.group_id, intent.record.local_node_id)) {
                const current_range = findRangeRecord(
                    current.ranges,
                    intent.record.group_id,
                );
                const transition_group =
                    active_transition_contracts.tableIdForGroup(
                        intent.record.group_id,
                    ) != null;
                const dropping_active_table = if (current_range) |range|
                    active_transition_contracts.get(range.table_id) != null and
                        findTableRecord(desired_tables, range.table_id) == null
                else
                    false;
                if (transition_group or dropping_active_table) {
                    // Keep unpublished transition peers and groups removed
                    // from the desired range topology alive until transition
                    // termination. A table drop also retains unrelated ranges
                    // so the table cannot disappear beneath a live contract.
                    // Existing desired ranges may still heal or rebalance.
                    if (current_range == null or
                        findRangeRecord(desired_ranges, intent.record.group_id) == null)
                    {
                        continue;
                    }
                }
                if (membership_index.preserveCurrentPlacement(intent.record.group_id, intent.record.local_node_id)) continue;
                if (membership_index.placementSafeToRemove(intent)) {
                    try placement_removals.append(self.alloc, .{
                        .group_id = intent.record.group_id,
                        .local_node_id = intent.record.local_node_id,
                    });
                } else {
                    var draining = intent;
                    const contracting = membership_index.contracting(intent.record.group_id);
                    draining.serving_state = if (contracting) .retiring else .draining;
                    const desired = membership_index.representativeDesired(intent.record.group_id);
                    const latched_peers = membership_index.latchedFinalPeers(intent.record.group_id);
                    const transition_peers = if (!contracting and latched_peers == null and membership_index.needsExpansion(intent.record.group_id))
                        if (desired) |value|
                            try allocExpandedTransitionPeerNodeIds(self.alloc, current.placement_intents, value)
                        else
                            null
                    else
                        null;
                    defer if (transition_peers) |peers| self.alloc.free(peers);
                    if (latched_peers) |peers| {
                        draining.peer_node_ids = peers;
                    } else if (desired) |value| {
                        draining.peer_node_ids = transition_peers orelse value.peer_node_ids;
                    }
                    const watermark = evidence.relocationWatermark(intent.record.group_id);
                    if (draining.relocation_generation == 0) draining.relocation_generation = intent.record.metadata_version + 1;
                    applyRelocationWatermark(&draining, watermark);
                    if (!placementIntentsEqual(intent, draining)) {
                        try placement_upserts.ensureUnusedCapacity(self.alloc, 1);
                        placement_upserts.appendAssumeCapacity(
                            try clonePlacementIntent(self.alloc, draining),
                        );
                    }
                }
            }
        }
        for (current.tables) |record| {
            if (findTableRecord(desired_tables, record.table_id) == null and
                active_transition_contracts.get(record.table_id) == null)
            {
                try table_removals.append(self.alloc, record.table_id);
            }
        }
        for (current.ranges) |record| {
            if (findRangeRecord(desired_ranges, record.group_id) != null) continue;
            if (active_transition_contracts.rangeMutationFenced(record.group_id))
                continue;
            if (active_transition_contracts.get(record.table_id) != null and
                findTableRecord(desired_tables, record.table_id) == null)
            {
                continue;
            }
            try range_removals.append(self.alloc, record.group_id);
        }
        for (current.split_transitions) |record| {
            if (findSplitRecord(desired_splits, record.transition_id) != null) continue;
            const observation = findSplitObservation(current.split_observations, record.transition_id) orelse defaultSplitObservation();
            if (!splitTransitionCanRollback(record)) {
                try split_removals.append(self.alloc, record.transition_id);
            } else if (terminalSplitObservationPhase(observation)) |phase| {
                try split_upserts.ensureUnusedCapacity(self.alloc, 1);
                var terminal_record = try cloneSplitRecord(self.alloc, record);
                terminal_record.phase = phase;
                split_upserts.appendAssumeCapacity(terminal_record);
            } else {
                try split_steps.ensureUnusedCapacity(self.alloc, 1);
                const planned_record = try cloneSplitRecord(self.alloc, record);
                split_steps.appendAssumeCapacity(.{
                    .record = planned_record,
                    .execution = transition_controller.TransitionController.describeSplit(planned_record, observation),
                });
            }
        }
        for (current.merge_transitions) |record| {
            if (findMergeRecord(desired_merges, record.transition_id) != null) continue;
            const observation = findMergeObservation(current.merge_observations, record.transition_id) orelse defaultMergeObservation(record);
            if (!mergeTransitionCanRollback(record)) {
                try merge_removals.append(self.alloc, record.transition_id);
            } else if (terminalMergeObservationPhase(observation)) |phase| {
                try merge_upserts.ensureUnusedCapacity(self.alloc, 1);
                var terminal_record = try cloneMergeRecord(self.alloc, record);
                terminal_record.phase = phase;
                merge_upserts.appendAssumeCapacity(terminal_record);
            } else {
                try merge_steps.ensureUnusedCapacity(self.alloc, 1);
                const planned_record = try cloneMergeRecord(self.alloc, record);
                merge_steps.appendAssumeCapacity(.{
                    .record = planned_record,
                    .execution = transition_controller.TransitionController.describeMerge(planned_record, observation),
                });
            }
        }

        var repair_placement_groups: usize = 0;
        var rebalance_placement_groups: usize = 0;
        for (desired_ranges) |range| {
            switch (classifyPlacementChange(range.group_id, desired_placements, current.placement_intents, candidate_domains)) {
                .repair_required => repair_placement_groups += 1,
                .rebalance => rebalance_placement_groups += 1,
                .stable => {},
            }
        }

        // Transfer each list independently so an allocation failure while
        // shrinking a later list can release every slice already transferred.
        // `toOwnedSlice` empties its source, keeping the list errdefers from
        // double-freeing records now owned by the partially built plan.
        var plan = ReconciliationPlan.empty();
        errdefer plan.deinit(self.alloc);
        plan.placement_upserts = try placement_upserts.toOwnedSlice(self.alloc);
        plan.table_upserts = try table_upserts.toOwnedSlice(self.alloc);
        plan.range_upserts = try range_upserts.toOwnedSlice(self.alloc);
        plan.split_admissions = try split_admissions.toOwnedSlice(self.alloc);
        plan.split_upserts = try split_upserts.toOwnedSlice(self.alloc);
        plan.merge_upserts = try merge_upserts.toOwnedSlice(self.alloc);
        plan.placement_removals = try placement_removals.toOwnedSlice(self.alloc);
        plan.table_removals = try table_removals.toOwnedSlice(self.alloc);
        plan.range_removals = try range_removals.toOwnedSlice(self.alloc);
        plan.split_removals = try split_removals.toOwnedSlice(self.alloc);
        plan.merge_removals = try merge_removals.toOwnedSlice(self.alloc);
        plan.split_steps = try split_steps.toOwnedSlice(self.alloc);
        plan.merge_steps = try merge_steps.toOwnedSlice(self.alloc);
        plan.repair_placement_groups = repair_placement_groups;
        plan.rebalance_placement_groups = rebalance_placement_groups;
        plan.forced_reallocation = current.reallocation_request != null;
        // Reallocation is a level-triggered replicated intent. Do not consume
        // it unless this round was allowed to run forced placement planning
        // with fully converged membership. Splits, merges, and unconverged
        // groups defer the intent so a single unlucky reconcile round cannot
        // permanently lose the operator's request.
        if (force_placement_reallocation and
            protected_placement_groups.len == 0 and
            placement_candidate_info.len != 0)
        {
            plan.clear_reallocation_request = current.reallocation_request.?.request_id;
        }
        return plan;
    }

    fn syncAutomaticShardIntents(
        self: *Reconciler,
        manager: *table_manager.TableManager,
        current: CurrentMetadataState,
        evidence: *const StoreEvidenceIndex,
        now_monotonic_ms: u64,
        now_realtime_ms: u64,
    ) !void {
        var auto_transitions = try self.computeAutomaticShardTransitions(
            current,
            evidence,
            now_monotonic_ms,
            now_realtime_ms,
        );
        defer auto_transitions.deinit(self.alloc);

        var desired_split_ids = std.ArrayListUnmanaged(u64).empty;
        defer desired_split_ids.deinit(self.alloc);
        for (auto_transitions.splits) |intent| {
            if (managerGroupBusy(manager, intent.source_group_id, intent.destination_group_id, intent.transition_id)) continue;
            try manager.requestSplit(intent);
            try desired_split_ids.append(self.alloc, intent.transition_id);
        }

        var desired_merge_ids = std.ArrayListUnmanaged(u64).empty;
        defer desired_merge_ids.deinit(self.alloc);
        for (auto_transitions.merges) |intent| {
            if (managerGroupBusy(manager, intent.donor_group_id, intent.receiver_group_id, intent.transition_id)) continue;
            try manager.requestMerge(intent);
            try desired_merge_ids.append(self.alloc, intent.transition_id);
        }

        try pruneAutomaticIntents(self.alloc, manager, current, desired_split_ids.items, desired_merge_ids.items);
    }

    fn computeAutomaticShardTransitions(
        self: *Reconciler,
        current: CurrentMetadataState,
        evidence: *const StoreEvidenceIndex,
        now_monotonic_ms: u64,
        now_realtime_ms: u64,
    ) !AutomaticTransitions {
        if ((self.config.disable_shard_alloc and current.reallocation_request == null) or self.config.max_shard_size_bytes == 0) {
            return .{ .splits = &.{}, .merges = &.{} };
        }

        const min_shard_size_bytes = self.effectiveMinShardSizeBytes();
        const min_shards_per_table = @max(self.config.min_shards_per_table, 1);
        const max_shards_per_table = if (self.config.max_shards_per_table == 0)
            std.math.maxInt(u32)
        else
            self.config.max_shards_per_table;
        const per_table_limit = @max(self.config.auto_range_transition_per_table_limit, 1);
        const cluster_limit = @max(self.config.auto_range_transition_cluster_limit, 1);
        var remaining_cluster_budget = cluster_limit;
        const active_cluster = activeRangeTransitionCount(current);
        if (active_cluster >= remaining_cluster_budget) {
            remaining_cluster_budget = 0;
        } else {
            remaining_cluster_budget -= active_cluster;
        }
        if (remaining_cluster_budget == 0) return .{ .splits = &.{}, .merges = &.{} };

        const sorted_ranges = try self.alloc.dupe(table_manager.RangeRecord, current.ranges);
        defer self.alloc.free(sorted_ranges);
        std.mem.sort(table_manager.RangeRecord, sorted_ranges, {}, struct {
            fn lessThan(_: void, a: table_manager.RangeRecord, b: table_manager.RangeRecord) bool {
                if (a.table_id != b.table_id) return a.table_id < b.table_id;
                return std.mem.order(u8, a.start_key, b.start_key) == .lt;
            }
        }.lessThan);
        var planning_index = try AutomaticPlanningIndex.init(
            self.alloc,
            current,
            sorted_ranges,
        );
        defer planning_index.deinit();

        var split_intents = std.ArrayListUnmanaged(table_manager.SplitIntent).empty;
        errdefer {
            for (split_intents.items) |intent| freeSplitIntentOwned(self.alloc, intent);
            split_intents.deinit(self.alloc);
        }
        var merge_intents = std.ArrayListUnmanaged(table_manager.MergeIntent).empty;
        errdefer {
            for (merge_intents.items) |intent| freeMergeIntentOwned(self.alloc, intent);
            merge_intents.deinit(self.alloc);
        }

        for (current.tables) |table| {
            if (remaining_cluster_budget == 0) break;

            var table_budget = per_table_limit;
            const active_table = planning_index.activeTransitionCount(table.table_id);
            if (active_table >= table_budget) continue;
            table_budget -= active_table;

            const table_ranges = planning_index.rangesForTable(table.table_id);
            var planned_shards = std.math.cast(u32, table_ranges.len) orelse
                continue;

            if (planned_shards > min_shards_per_table and table_budget > 0 and remaining_cluster_budget > 0) {
                var i: usize = 0;
                while (i + 1 < table_ranges.len and table_budget > 0 and remaining_cluster_budget > 0) : (i += 1) {
                    const left = table_ranges[i];
                    const right = table_ranges[i + 1];
                    if (!rangesAdjacent(left, right)) continue;
                    if (planning_index.groupBusy(left.group_id) or planning_index.groupBusy(right.group_id)) continue;
                    if (self.isShardInCooldown(left.group_id, now_monotonic_ms) or self.isShardInCooldown(right.group_id, now_monotonic_ms)) continue;
                    const left_status = evidence.mergedStatus(current, left.group_id) orelse continue;
                    const right_status = evidence.mergedStatus(current, right.group_id) orelse continue;
                    if (!evidence.hasFullHealthyPlacement(left.group_id, left_status) or
                        !evidence.hasFullHealthyPlacement(right.group_id, right_status))
                    {
                        continue;
                    }
                    if (!groupStatusFresh(self.config, left_status, now_realtime_ms) or !groupStatusFresh(self.config, right_status, now_realtime_ms)) continue;
                    if (!groupStatusReadyForAutomaticPlanning(left_status) or !groupStatusReadyForAutomaticPlanning(right_status)) continue;
                    if (!self.groupOldEnoughForMerge(left_status, now_realtime_ms) or !self.groupOldEnoughForMerge(right_status, now_realtime_ms)) continue;
                    if (!docIdentityNamespacesCompatibleForAutomaticMerge(left_status, right_status)) continue;

                    const left_size = left_status.disk_bytes;
                    const right_size = right_status.disk_bytes;
                    if (!(left_size < min_shard_size_bytes or right_size < min_shard_size_bytes)) continue;
                    const combined = std.math.add(u64, left_size, right_size) catch
                        continue;
                    if (combined >= self.config.max_shard_size_bytes) continue;

                    const transition_id = deriveAutomaticTransitionId("merge", left.group_id, right.group_id, null);
                    const intent: table_manager.MergeIntent = .{
                        .transition_id = transition_id,
                        .table_id = table.table_id,
                        .donor_group_id = right.group_id,
                        .receiver_group_id = left.group_id,
                        // Independently-created adjacent ranges own distinct
                        // doc-identity namespaces and require reassignment.
                        // Split siblings retain one namespace and avoid that
                        // extra transition. Planning reaches this point only
                        // when reassignment is safe; non-empty incompatible
                        // namespaces remain fenced above.
                        .allow_doc_identity_reassignment = table_manager.rangeDocIdentityShardId(left) != table_manager.rangeDocIdentityShardId(right) or
                            table_manager.rangeDocIdentityRangeId(left) != table_manager.rangeDocIdentityRangeId(right),
                        .automatic = true,
                    };
                    try merge_intents.append(self.alloc, intent);
                    try planning_index.markBusy(left.group_id, right.group_id);
                    planned_shards -= 1;
                    table_budget -= 1;
                    remaining_cluster_budget -= 1;
                }
            }

            if (planned_shards >= max_shards_per_table or table_budget == 0 or remaining_cluster_budget == 0) continue;

            for (table_ranges) |range| {
                if (planned_shards >= max_shards_per_table or table_budget == 0 or remaining_cluster_budget == 0) break;
                if (planning_index.groupBusy(range.group_id)) continue;
                if (self.isShardInCooldown(range.group_id, now_monotonic_ms)) continue;
                const status = evidence.mergedStatus(current, range.group_id) orelse continue;
                if (!evidence.hasFullHealthyPlacement(range.group_id, status)) continue;
                if (!groupStatusFresh(self.config, status, now_realtime_ms)) continue;
                if (!groupStatusReadyForAutomaticPlanning(status)) continue;
                if (!docIdentityNamespaceReadyForAutomaticSplit(status)) continue;
                if (status.disk_bytes <= self.config.max_shard_size_bytes) continue;
                const lookup = self.config.median_key_lookup orelse continue;
                const owned_split_key = (lookup.fetchMedianKey(
                    self.alloc,
                    range.group_id,
                ) catch |err| switch (err) {
                    error.OutOfMemory => return err,
                    else => continue,
                }) orelse continue;
                var split_key_consumed = false;
                defer if (!split_key_consumed) self.alloc.free(owned_split_key);
                if (status.doc_count > 0 and status.doc_count < 2) continue;
                if (owned_split_key.len == 0) continue;
                if (!keyStrictlyInsideRange(owned_split_key, range.start_key, range.end_key)) continue;

                const destination_group_id = deriveAutomaticSplitDestinationId(
                    &planning_index,
                    range.group_id,
                    owned_split_key,
                );
                if (destination_group_id == 0) continue;
                const transition_id = deriveAutomaticTransitionId("split", range.group_id, destination_group_id, owned_split_key);
                const intent: table_manager.SplitIntent = .{
                    .transition_id = transition_id,
                    .table_id = table.table_id,
                    .source_group_id = range.group_id,
                    .destination_group_id = destination_group_id,
                    .split_key = owned_split_key,
                    .automatic = true,
                };
                try planning_index.reserveGroup(destination_group_id);
                try planning_index.markBusy(range.group_id, destination_group_id);
                try split_intents.append(self.alloc, intent);
                split_key_consumed = true;
                planned_shards += 1;
                table_budget -= 1;
                remaining_cluster_budget -= 1;
            }
        }

        var transitions = AutomaticTransitions.empty();
        errdefer transitions.deinit(self.alloc);
        transitions.splits = try split_intents.toOwnedSlice(self.alloc);
        transitions.merges = try merge_intents.toOwnedSlice(self.alloc);
        return transitions;
    }

    fn effectiveMinShardSizeBytes(self: *const Reconciler) u64 {
        if (self.config.min_shard_size_bytes > 0) return self.config.min_shard_size_bytes;
        if (self.config.max_shard_size_bytes == 0) return 0;
        return @max(@divTrunc(self.config.max_shard_size_bytes, 4), 1);
    }

    fn groupOldEnoughForMerge(
        self: *const Reconciler,
        status: MergedGroupStatus,
        now_realtime_ms: u64,
    ) bool {
        if (self.config.min_shard_merge_age_millis == 0) return true;
        if (status.created_at_millis == 0) return true;
        if (now_realtime_ms < status.created_at_millis) return false;
        return now_realtime_ms - status.created_at_millis >= self.config.min_shard_merge_age_millis;
    }

    fn isShardInCooldown(self: *Reconciler, group_id: u64, now_ms: u64) bool {
        if (self.shard_cooldowns.get(group_id)) |cooldown_end_ms| {
            if (now_ms < cooldown_end_ms) return true;
            _ = self.shard_cooldowns.remove(group_id);
        }
        return false;
    }

    fn cleanupExpiredShardCooldowns(self: *Reconciler, now_ms: u64) void {
        var expired = std.ArrayListUnmanaged(u64).empty;
        defer expired.deinit(self.alloc);

        var it = self.shard_cooldowns.iterator();
        while (it.next()) |entry| {
            if (entry.value_ptr.* <= now_ms) expired.append(self.alloc, entry.key_ptr.*) catch continue;
        }
        for (expired.items) |group_id| _ = self.shard_cooldowns.remove(group_id);
    }

    fn recordCompletedTransitionCooldowns(self: *Reconciler, current: CurrentMetadataState, now_ms: u64) !void {
        const cooldown_end_ms = now_ms + self.cooldownDurationMillis();

        for (current.split_transitions) |record| {
            const observation = findSplitObservation(current.split_observations, record.transition_id) orelse continue;
            switch (observation.status.phase) {
                .finalized, .rolled_back => {
                    try self.shard_cooldowns.put(self.alloc, record.source_group_id, cooldown_end_ms);
                    try self.shard_cooldowns.put(self.alloc, record.destination_group_id, cooldown_end_ms);
                },
                else => {},
            }
        }
        for (current.merge_transitions) |record| {
            const observation = findMergeObservation(current.merge_observations, record.transition_id) orelse continue;
            switch (observation.receiver.phase) {
                .finalized, .rolled_back => {
                    try self.shard_cooldowns.put(self.alloc, record.donor_group_id, cooldown_end_ms);
                    try self.shard_cooldowns.put(self.alloc, record.receiver_group_id, cooldown_end_ms);
                },
                else => {},
            }
        }
    }

    fn cooldownDurationMillis(self: *const Reconciler) u64 {
        return if (self.config.shard_cooldown_millis > 0)
            self.config.shard_cooldown_millis
        else
            60 * std.time.ms_per_s;
    }
};

const AutomaticTransitions = struct {
    splits: []table_manager.SplitIntent,
    merges: []table_manager.MergeIntent,

    fn empty() AutomaticTransitions {
        return .{
            .splits = &.{},
            .merges = &.{},
        };
    }

    fn deinit(self: *AutomaticTransitions, alloc: std.mem.Allocator) void {
        for (self.splits) |intent| freeSplitIntentOwned(alloc, intent);
        if (self.splits.len > 0) alloc.free(self.splits);
        for (self.merges) |intent| freeMergeIntentOwned(alloc, intent);
        if (self.merges.len > 0) alloc.free(self.merges);
        self.* = undefined;
    }
};

fn clonePlacementIntent(alloc: std.mem.Allocator, intent: raft_reconciler.PlacementIntent) !raft_reconciler.PlacementIntent {
    return try raft_reconciler.cloneIntentOwned(alloc, intent);
}

fn placementIntentsEqual(a: raft_reconciler.PlacementIntent, b: raft_reconciler.PlacementIntent) bool {
    return a.record.group_id == b.record.group_id and
        a.record.replica_id == b.record.replica_id and
        a.record.local_node_id == b.record.local_node_id and
        a.record.metadata_version == b.record.metadata_version and
        a.record.bootstrap_mode == b.record.bootstrap_mode and
        snapshotBootstrapEqual(a.record.snapshot_bootstrap, b.record.snapshot_bootstrap) and
        backupRestoreBootstrapEqual(a.record.backup_restore_bootstrap, b.record.backup_restore_bootstrap) and
        a.store_id == b.store_id and
        a.serving_state == b.serving_state and
        a.relocation_generation == b.relocation_generation and
        a.relocation_source_node_id == b.relocation_source_node_id and
        a.relocation_source_store_id == b.relocation_source_store_id and
        a.relocation_doc_count_watermark == b.relocation_doc_count_watermark and
        a.relocation_disk_bytes_watermark == b.relocation_disk_bytes_watermark and
        a.relocation_target_sequence == b.relocation_target_sequence and
        a.relocation_applied_sequence == b.relocation_applied_sequence and
        std.mem.eql(u64, a.peer_node_ids, b.peer_node_ids) and
        std.mem.eql(u64, a.learner_node_ids, b.learner_node_ids);
}

test "metadata reconciler detects learner membership changes" {
    const current: raft_reconciler.PlacementIntent = .{
        .record = .{ .group_id = 31, .replica_id = 1, .local_node_id = 1 },
        .peer_node_ids = &.{ 1, 2 },
        .learner_node_ids = &.{3},
    };
    var desired = current;
    desired.learner_node_ids = &.{4};

    try std.testing.expect(!placementIntentsEqual(current, desired));
}

fn snapshotBootstrapEqual(
    a: ?@import("../raft/catalog.zig").SnapshotBootstrapRecord,
    b: ?@import("../raft/catalog.zig").SnapshotBootstrapRecord,
) bool {
    if (a == null and b == null) return true;
    if (a == null or b == null) return false;
    return a.?.from_node_id == b.?.from_node_id and
        a.?.term == b.?.term and
        std.mem.eql(u8, a.?.snapshot_id, b.?.snapshot_id) and
        std.mem.eql(u8, a.?.uri, b.?.uri);
}

fn backupRestoreBootstrapEqual(
    a: ?@import("../raft/catalog.zig").BackupRestoreBootstrapRecord,
    b: ?@import("../raft/catalog.zig").BackupRestoreBootstrapRecord,
) bool {
    if (a == null and b == null) return true;
    if (a == null or b == null) return false;
    return std.mem.eql(u8, a.?.backup_id, b.?.backup_id) and
        std.mem.eql(u8, a.?.artifact_backup_id, b.?.artifact_backup_id) and
        std.mem.eql(u8, a.?.location, b.?.location) and
        std.mem.eql(u8, a.?.snapshot_path, b.?.snapshot_path) and
        std.mem.eql(u8, a.?.connection, b.?.connection) and
        a.?.artifact_size_bytes == b.?.artifact_size_bytes and
        std.mem.eql(u8, a.?.artifact_sha256, b.?.artifact_sha256);
}

fn findPlacementIntent(intents: []const raft_reconciler.PlacementIntent, group_id: u64, local_node_id: u64) ?raft_reconciler.PlacementIntent {
    for (intents) |intent| {
        if (intent.record.group_id == group_id and intent.record.local_node_id == local_node_id) return intent;
    }
    return null;
}

fn normalizeRestoreBootstrapIntent(
    current: CurrentMetadataState,
    tables: []const table_manager.TableRecord,
    ranges: []const table_manager.RangeRecord,
    intent: raft_reconciler.PlacementIntent,
) raft_reconciler.PlacementIntent {
    var effective = intent;
    const range = findRangeRecord(ranges, intent.record.group_id) orelse return effective;
    const table = findTableRecord(tables, range.table_id) orelse return effective;
    if (range.restore_backup_id.len == 0 or range.restore_location.len == 0) return effective;
    if (findRestoreProgress(
        current.restore_progresses,
        table.table_id,
        intent.record.local_node_id,
        intent.record.group_id,
        range.restore_backup_id,
        range.restore_location,
        range.restore_snapshot_path,
        range.restore_artifact_sha256,
    )) |progress| {
        if (!progress.primary_restored) return effective;
        effective.record.bootstrap_mode = .persisted;
        effective.record.snapshot_bootstrap = null;
        effective.record.backup_restore_bootstrap = null;
    } else {
        effective.record.bootstrap_mode = .fetch_snapshot;
        effective.record.snapshot_bootstrap = null;
        effective.record.backup_restore_bootstrap = .{
            .backup_id = range.restore_backup_id,
            .artifact_backup_id = range.restore_artifact_backup_id,
            .location = range.restore_location,
            .snapshot_path = range.restore_snapshot_path,
            .connection = range.restore_connection,
            .artifact_size_bytes = range.restore_artifact_size_bytes,
            .artifact_sha256 = range.restore_artifact_sha256,
        };
    }
    return effective;
}

fn effectivePlacementIntent(
    current: CurrentMetadataState,
    tables: []const table_manager.TableRecord,
    ranges: []const table_manager.RangeRecord,
    evidence: *const StoreEvidenceIndex,
    intent: raft_reconciler.PlacementIntent,
) raft_reconciler.PlacementIntent {
    var effective = normalizeRestoreBootstrapIntent(current, tables, ranges, intent);
    const existing = evidence.placementForMember(effective.record.group_id, effective.record.local_node_id);
    const has_current_group = evidence.placementCount(effective.record.group_id) > 0;
    if (!has_current_group) {
        effective.serving_state = .serving;
        return effective;
    }

    const watermark = evidence.relocationWatermark(effective.record.group_id);
    applyRelocationWatermark(&effective, watermark);

    if (existing) |current_intent| {
        effective.relocation_generation = current_intent.relocation_generation;
        if (effective.relocation_generation == 0 and current_intent.serving_state != .serving) {
            effective.relocation_generation = current_intent.record.metadata_version + 1;
        }
        if (effective.relocation_source_node_id == 0) effective.relocation_source_node_id = current_intent.relocation_source_node_id;
        if (effective.relocation_source_store_id == 0) effective.relocation_source_store_id = current_intent.relocation_source_store_id;
        if (current_intent.relocation_doc_count_watermark > effective.relocation_doc_count_watermark) effective.relocation_doc_count_watermark = current_intent.relocation_doc_count_watermark;
        if (current_intent.relocation_disk_bytes_watermark > effective.relocation_disk_bytes_watermark) effective.relocation_disk_bytes_watermark = current_intent.relocation_disk_bytes_watermark;
        effective.relocation_target_sequence = current_intent.relocation_target_sequence;
        effective.relocation_applied_sequence = current_intent.relocation_applied_sequence;
        applyRelocationWatermark(&effective, watermark);

        effective.serving_state = switch (current_intent.serving_state) {
            // Serving is monotonic for one placement generation. Once the
            // target has passed data and expanded-membership cutover, final
            // voter contraction must not temporarily withdraw read service.
            .serving => .serving,
            .draining => .serving,
            // Promotion is monotonic as well. A cutover-ready replica may
            // already be a committed voter by the time a transiently missing
            // or stale status sample is reconciled. Reclassifying it as a
            // learner would request an invalid voter demotion and can stall
            // the relocation indefinitely. Keep it non-serving until stable
            // membership is proved, but never move it backwards.
            .cutover_ready => if (relocationTargetReady(evidence, effective)) .serving else .cutover_ready,
            .planned, .bootstrapping, .replaying, .retiring => relocationTargetServingState(evidence, effective),
        };
        return effective;
    }

    effective.serving_state = relocationTargetServingState(evidence, effective);
    if (effective.serving_state == .serving) return effective;
    if (effective.relocation_generation == 0) effective.relocation_generation = watermark.generation_hint;
    if (effective.relocation_source_node_id == 0) {
        if (evidence.relocationSource(effective.record.group_id)) |source| {
            effective.relocation_source_node_id = source.record.local_node_id;
            effective.relocation_source_store_id = source.store_id;
        }
    }
    return effective;
}

const RelocationWatermark = struct {
    doc_count: u64 = 0,
    disk_bytes: u64 = 0,
    raft_target_index: u64 = 0,
    generation_hint: u64 = 1,
    has_visible_data: bool = false,
};

fn applyRelocationWatermark(intent: *raft_reconciler.PlacementIntent, watermark: RelocationWatermark) void {
    if (!watermark.has_visible_data) {
        intent.relocation_doc_count_watermark = 0;
        intent.relocation_disk_bytes_watermark = 0;
        return;
    }
    if (intent.relocation_doc_count_watermark < watermark.doc_count) intent.relocation_doc_count_watermark = watermark.doc_count;
    if (intent.relocation_disk_bytes_watermark < watermark.disk_bytes) intent.relocation_disk_bytes_watermark = watermark.disk_bytes;
    // Capture the source's committed apply boundary once. Chasing a moving
    // leader on every reconciliation can starve relocation under sustained
    // writes. Document count remains a logical cross-check; disk bytes are
    // retained for capacity telemetry only because compaction can make an
    // equivalent target generation physically smaller than its source.
    if (intent.relocation_target_sequence == 0) intent.relocation_target_sequence = watermark.raft_target_index;
}

fn relocationStatusHasVisibleDocuments(doc_count: u64) bool {
    return doc_count > 0;
}

fn relocationTargetServingState(
    evidence: *const StoreEvidenceIndex,
    intent: raft_reconciler.PlacementIntent,
) raft_reconciler.PlacementServingState {
    if (relocationTargetReady(evidence, intent)) return .serving;
    if (relocationTargetDataReady(evidence, intent)) return .cutover_ready;
    if (relocationTargetHasDataReport(evidence, intent)) return .replaying;
    return .bootstrapping;
}

fn relocationTargetReady(evidence: *const StoreEvidenceIndex, intent: raft_reconciler.PlacementIntent) bool {
    return relocationTargetCutoverReady(evidence, intent);
}

fn relocationTargetDataReady(evidence: *const StoreEvidenceIndex, intent: raft_reconciler.PlacementIntent) bool {
    if (evidence.placementMemberAmbiguous(intent.record.group_id, intent.record.local_node_id)) return false;
    const store = evidence.storeForIntent(intent) orelse return false;
    if (!store.live or !std.mem.eql(u8, store.health_class, "healthy")) return false;
    const status = evidence.statusForStore(intent.record.group_id, store.store_id) orelse return false;
    if (status.relocation_generation != intent.relocation_generation) return false;
    if (status.transition_pending) return false;
    if (status.replay_required and !status.replay_caught_up) return false;
    if (status.doc_count < intent.relocation_doc_count_watermark) return false;
    if (intent.relocation_target_sequence != 0 and status.raft_applied_index < intent.relocation_target_sequence)
        return false;
    return true;
}

fn relocationTargetCutoverReady(evidence: *const StoreEvidenceIndex, intent: raft_reconciler.PlacementIntent) bool {
    if (!relocationTargetDataReady(evidence, intent)) return false;
    const status = evidence.statusForIntent(intent) orelse return false;
    // Ordinary replica relocation is complete when Raft has promoted the
    // target into a stable voter set and its document generation satisfies
    // the source watermark. Membership alone only proves Raft-log
    // convergence; document storage is installed through a separate
    // generation transition and may still be staging.
    return status.local_voter and
        !status.joint_consensus and
        voterSetMatchesIntent(status.*, intent);
}

fn relocationTargetHasDataReport(evidence: *const StoreEvidenceIndex, intent: raft_reconciler.PlacementIntent) bool {
    const store = evidence.storeForIntent(intent) orelse return false;
    return evidence.hasDataReport(intent.record.group_id, store.store_id);
}

const StoreEvidenceIndex = struct {
    const StoreGroupEvidence = struct {
        status: ?*const table_manager.GroupStatusReport = null,
        status_ambiguous: bool = false,
        runtime_reported: bool = false,
    };

    const PlacementTopology = struct {
        member_count: usize = 0,
        voter_count: usize = 0,
        voter_set_fingerprint: table_manager.VoterSetFingerprint = [_]u8{0} ** table_manager.voter_set_fingerprint_len,
        initialized: bool = false,
        ambiguous: bool = false,
    };

    alloc: std.mem.Allocator,
    stores_by_id: std.AutoHashMapUnmanaged(u64, ?*const table_manager.StoreRecord) = .empty,
    stores_by_node: std.AutoHashMapUnmanaged(u64, ?*const table_manager.StoreRecord) = .empty,
    reports_by_store_group: std.AutoHashMapUnmanaged(u128, StoreGroupEvidence) = .empty,
    merged_status_by_group: std.AutoHashMapUnmanaged(u64, *const MergedGroupStatus) = .empty,
    placement_by_member: std.AutoHashMapUnmanaged(u128, ?*const raft_reconciler.PlacementIntent) = .empty,
    placement_count_by_group: std.AutoHashMapUnmanaged(u64, usize) = .empty,
    placement_topology_by_group: std.AutoHashMapUnmanaged(u64, PlacementTopology) = .empty,
    relocation_source_by_group: std.AutoHashMapUnmanaged(u64, *const raft_reconciler.PlacementIntent) = .empty,
    relocation_watermark_by_group: std.AutoHashMapUnmanaged(u64, RelocationWatermark) = .empty,

    fn init(alloc: std.mem.Allocator, current: CurrentMetadataState) !StoreEvidenceIndex {
        var self = StoreEvidenceIndex{ .alloc = alloc };
        errdefer self.deinit();
        try self.merged_status_by_group.ensureTotalCapacity(
            alloc,
            @intCast(current.merged_group_statuses.len),
        );
        for (current.merged_group_statuses) |*status| {
            const entry = self.merged_status_by_group.getOrPutAssumeCapacity(status.group_id);
            if (entry.found_existing) return error.DuplicateMergedGroupStatus;
            entry.value_ptr.* = status;
        }
        for (current.stores) |*store| {
            const by_id = try self.stores_by_id.getOrPut(alloc, store.store_id);
            by_id.value_ptr.* = if (by_id.found_existing) null else store;
            const by_node = try self.stores_by_node.getOrPut(alloc, store.node_id);
            by_node.value_ptr.* = if (by_node.found_existing) null else store;
            for (store.group_statuses) |*status| {
                const entry = try self.reports_by_store_group.getOrPut(alloc, placementStoreKey(status.group_id, store.store_id));
                if (!entry.found_existing) entry.value_ptr.* = .{};
                if (entry.value_ptr.status != null or entry.value_ptr.status_ambiguous) {
                    entry.value_ptr.status = null;
                    entry.value_ptr.status_ambiguous = true;
                } else {
                    entry.value_ptr.status = status;
                }
            }
            for (store.runtime_statuses) |status| {
                const entry = try self.reports_by_store_group.getOrPut(alloc, placementStoreKey(status.group_id, store.store_id));
                if (!entry.found_existing) entry.value_ptr.* = .{};
                entry.value_ptr.runtime_reported = true;
            }
        }
        for (current.placement_intents) |*intent| {
            const member = try self.placement_by_member.getOrPut(alloc, placementNodeKey(intent.record.group_id, intent.record.local_node_id));
            member.value_ptr.* = if (member.found_existing) null else intent;
            const count = try self.placement_count_by_group.getOrPut(alloc, intent.record.group_id);
            if (!count.found_existing) count.value_ptr.* = 0;
            count.value_ptr.* += 1;

            const topology_entry = try self.placement_topology_by_group.getOrPut(
                alloc,
                intent.record.group_id,
            );
            if (!topology_entry.found_existing) topology_entry.value_ptr.* = .{};
            const topology = topology_entry.value_ptr;
            topology.member_count += 1;
            if (member.found_existing) topology.ambiguous = true;
            const voter_count = table_manager.normalizedVoterCount(
                intent.peer_node_ids,
                intent.record.local_node_id,
            );
            const fingerprint = table_manager.voterSetFingerprint(
                intent.peer_node_ids,
                intent.record.local_node_id,
            );
            if (!topology.initialized) {
                topology.initialized = true;
                topology.voter_count = voter_count;
                topology.voter_set_fingerprint = fingerprint;
            } else if (topology.voter_count != voter_count or
                !std.mem.eql(
                    u8,
                    &topology.voter_set_fingerprint,
                    &fingerprint,
                ))
            {
                topology.ambiguous = true;
            }

            const source = try self.relocation_source_by_group.getOrPut(alloc, intent.record.group_id);
            if (!source.found_existing or relocationSourceRank(intent.serving_state) > relocationSourceRank(source.value_ptr.*.serving_state)) {
                source.value_ptr.* = intent;
            }
            const watermark = try self.relocation_watermark_by_group.getOrPut(alloc, intent.record.group_id);
            if (!watermark.found_existing) watermark.value_ptr.* = .{};
            mergePlacementWatermark(watermark.value_ptr, intent.*);
        }
        try self.buildRelocationWatermarks(current);
        return self;
    }

    fn deinit(self: *StoreEvidenceIndex) void {
        self.stores_by_id.deinit(self.alloc);
        self.stores_by_node.deinit(self.alloc);
        self.reports_by_store_group.deinit(self.alloc);
        self.merged_status_by_group.deinit(self.alloc);
        self.placement_by_member.deinit(self.alloc);
        self.placement_count_by_group.deinit(self.alloc);
        self.placement_topology_by_group.deinit(self.alloc);
        self.relocation_source_by_group.deinit(self.alloc);
        self.relocation_watermark_by_group.deinit(self.alloc);
        self.* = undefined;
    }

    fn storeForIntent(self: *const StoreEvidenceIndex, intent: raft_reconciler.PlacementIntent) ?*const table_manager.StoreRecord {
        const resolved = if (intent.store_id != 0)
            self.stores_by_id.get(intent.store_id) orelse return null
        else
            self.stores_by_node.get(intent.record.local_node_id) orelse return null;
        const store = resolved orelse return null;
        return if (store.node_id == intent.record.local_node_id) store else null;
    }

    fn statusForStore(self: *const StoreEvidenceIndex, group_id: u64, store_id: u64) ?*const table_manager.GroupStatusReport {
        const evidence = self.reports_by_store_group.get(placementStoreKey(group_id, store_id)) orelse return null;
        if (evidence.status_ambiguous) return null;
        return evidence.status;
    }

    fn mergedStatus(self: *const StoreEvidenceIndex, current: CurrentMetadataState, group_id: u64) ?MergedGroupStatus {
        if (current.merged_group_statuses.len == 0) {
            return mergedGroupStatus(current, group_id);
        }
        return (self.merged_status_by_group.get(group_id) orelse return null).*;
    }

    fn statusForIntent(self: *const StoreEvidenceIndex, intent: raft_reconciler.PlacementIntent) ?*const table_manager.GroupStatusReport {
        const store = self.storeForIntent(intent) orelse return null;
        return self.statusForStore(intent.record.group_id, store.store_id);
    }

    fn placementForMember(self: *const StoreEvidenceIndex, group_id: u64, node_id: u64) ?raft_reconciler.PlacementIntent {
        const intent = (self.placement_by_member.get(placementNodeKey(group_id, node_id)) orelse return null) orelse return null;
        return intent.*;
    }

    fn placementMemberAmbiguous(self: *const StoreEvidenceIndex, group_id: u64, node_id: u64) bool {
        const intent = self.placement_by_member.get(placementNodeKey(group_id, node_id)) orelse return false;
        return intent == null;
    }

    fn placementCount(self: *const StoreEvidenceIndex, group_id: u64) usize {
        return self.placement_count_by_group.get(group_id) orelse 0;
    }

    fn hasFullHealthyPlacement(
        self: *const StoreEvidenceIndex,
        group_id: u64,
        status: MergedGroupStatus,
    ) bool {
        const topology = self.placement_topology_by_group.get(group_id) orelse return false;
        if (!topology.initialized or topology.ambiguous or topology.member_count == 0) return false;
        if (topology.voter_count != topology.member_count or
            topology.voter_count > std.math.maxInt(u16))
        {
            return false;
        }
        const expected_count: u16 = @intCast(topology.voter_count);
        return status.leader_known and
            status.readiness_from_leader and
            status.voter_count_known and
            status.voter_set_known and
            status.voter_count == expected_count and
            status.healthy_voter_reports == expected_count and
            std.mem.eql(
                u8,
                &status.voter_set_fingerprint,
                &topology.voter_set_fingerprint,
            );
    }

    fn relocationSource(self: *const StoreEvidenceIndex, group_id: u64) ?raft_reconciler.PlacementIntent {
        const intent = self.relocation_source_by_group.get(group_id) orelse return null;
        return intent.*;
    }

    fn relocationWatermark(self: *const StoreEvidenceIndex, group_id: u64) RelocationWatermark {
        return self.relocation_watermark_by_group.get(group_id) orelse .{};
    }

    fn hasDataReport(self: *const StoreEvidenceIndex, group_id: u64, store_id: u64) bool {
        const evidence = self.reports_by_store_group.get(placementStoreKey(group_id, store_id)) orelse return false;
        return evidence.status != null or evidence.status_ambiguous or evidence.runtime_reported;
    }

    fn buildRelocationWatermarks(self: *StoreEvidenceIndex, current: CurrentMetadataState) !void {
        var base_groups = std.AutoHashMapUnmanaged(u64, void).empty;
        defer base_groups.deinit(self.alloc);
        var healthy_runtime = std.AutoHashMapUnmanaged(u64, RelocationWatermark).empty;
        defer healthy_runtime.deinit(self.alloc);

        if (current.merged_group_statuses.len > 0) {
            for (current.merged_group_statuses) |status| {
                try base_groups.put(self.alloc, status.group_id, {});
                const watermark = try self.watermarkPtr(status.group_id);
                mergeObservedWatermark(watermark, status.doc_count, status.disk_bytes, status.updated_at_millis, true);
            }
        } else {
            var latest_by_group = std.AutoHashMapUnmanaged(u64, *const table_manager.GroupStatusReport).empty;
            defer latest_by_group.deinit(self.alloc);
            for (current.stores) |*store| {
                if (!healthyStore(store.*)) continue;
                for (store.group_statuses) |*status| {
                    const entry = try latest_by_group.getOrPut(self.alloc, status.group_id);
                    if (!entry.found_existing or status.updated_at_millis >= entry.value_ptr.*.updated_at_millis) {
                        entry.value_ptr.* = status;
                    }
                }
            }
            var latest_it = latest_by_group.iterator();
            while (latest_it.next()) |entry| {
                const status = entry.value_ptr.*;
                try base_groups.put(self.alloc, status.group_id, {});
                const watermark = try self.watermarkPtr(status.group_id);
                mergeObservedWatermark(watermark, status.doc_count, status.disk_bytes, status.updated_at_millis, true);
            }
        }

        for (current.stores) |*store| {
            if (!healthyStore(store.*)) continue;
            for (store.runtime_statuses) |status| {
                const entry = try healthy_runtime.getOrPut(self.alloc, status.group_id);
                if (!entry.found_existing) entry.value_ptr.* = .{};
                mergeObservedWatermark(
                    entry.value_ptr,
                    status.doc_count,
                    status.disk_bytes,
                    @divTrunc(status.updated_at_ns, std.time.ns_per_ms),
                    true,
                );
            }
        }
        var base_it = base_groups.iterator();
        while (base_it.next()) |entry| {
            const runtime = healthy_runtime.get(entry.key_ptr.*) orelse continue;
            const watermark = try self.watermarkPtr(entry.key_ptr.*);
            mergeRelocationWatermark(watermark, runtime);
        }

        // Exact placement-owned reports can advance relocation. Reports from a
        // sibling store on the same node, duplicate store identity, or duplicate
        // placement member remain visible for diagnostics but cannot authorize
        // a generation transition.
        for (current.stores) |*store| {
            if (!healthyStore(store.*)) continue;
            for (store.group_statuses) |status| {
                const intent = self.placementForMember(status.group_id, store.node_id) orelse continue;
                const placement_store = self.storeForIntent(intent) orelse continue;
                if (placement_store != store or status.relocation_generation != intent.relocation_generation) continue;
                const watermark = try self.watermarkPtr(status.group_id);
                mergeObservedWatermark(watermark, status.doc_count, status.disk_bytes, status.updated_at_millis, true);
                if ((intent.serving_state == .serving or intent.serving_state == .draining) and
                    status.raft_applied_index > watermark.raft_target_index)
                {
                    watermark.raft_target_index = status.raft_applied_index;
                }
            }
            for (store.runtime_statuses) |status| {
                const intent = self.placementForMember(status.group_id, store.node_id) orelse continue;
                const placement_store = self.storeForIntent(intent) orelse continue;
                if (placement_store != store) continue;
                const watermark = try self.watermarkPtr(status.group_id);
                mergeObservedWatermark(watermark, status.doc_count, status.disk_bytes, 0, false);
            }
        }
    }

    fn watermarkPtr(self: *StoreEvidenceIndex, group_id: u64) !*RelocationWatermark {
        const entry = try self.relocation_watermark_by_group.getOrPut(self.alloc, group_id);
        if (!entry.found_existing) entry.value_ptr.* = .{};
        return entry.value_ptr;
    }
};

fn healthyStore(store: table_manager.StoreRecord) bool {
    return store.live and std.mem.eql(u8, store.health_class, "healthy");
}

fn relocationSourceRank(state: raft_reconciler.PlacementServingState) u8 {
    return switch (state) {
        .serving => 4,
        .draining => 3,
        .retiring => 2,
        else => 1,
    };
}

fn mergePlacementWatermark(watermark: *RelocationWatermark, intent: raft_reconciler.PlacementIntent) void {
    if (intent.relocation_doc_count_watermark > 0) watermark.has_visible_data = true;
    if (intent.relocation_doc_count_watermark > watermark.doc_count) watermark.doc_count = intent.relocation_doc_count_watermark;
    if (intent.relocation_doc_count_watermark > 0 and intent.relocation_disk_bytes_watermark > watermark.disk_bytes)
        watermark.disk_bytes = intent.relocation_disk_bytes_watermark;
    if (intent.relocation_generation >= watermark.generation_hint) watermark.generation_hint = intent.relocation_generation +| 1;
}

fn mergeObservedWatermark(
    watermark: *RelocationWatermark,
    doc_count: u64,
    disk_bytes: u64,
    updated_at_millis: u64,
    include_generation_hint: bool,
) void {
    if (relocationStatusHasVisibleDocuments(doc_count)) watermark.has_visible_data = true;
    if (doc_count > watermark.doc_count) watermark.doc_count = doc_count;
    if (relocationStatusHasVisibleDocuments(doc_count) and disk_bytes > watermark.disk_bytes) watermark.disk_bytes = disk_bytes;
    if (include_generation_hint and updated_at_millis >= watermark.generation_hint) watermark.generation_hint = updated_at_millis +| 1;
}

fn mergeRelocationWatermark(target: *RelocationWatermark, source: RelocationWatermark) void {
    target.has_visible_data = target.has_visible_data or source.has_visible_data;
    if (source.doc_count > target.doc_count) target.doc_count = source.doc_count;
    if (source.disk_bytes > target.disk_bytes) target.disk_bytes = source.disk_bytes;
    if (source.raft_target_index > target.raft_target_index) target.raft_target_index = source.raft_target_index;
    if (source.generation_hint > target.generation_hint) target.generation_hint = source.generation_hint;
}

const MembershipTransitionIndex = struct {
    const GroupState = struct {
        current_count: usize = 0,
        desired_count: usize = 0,
        desired_fingerprint: ?table_manager.VoterSetFingerprint = null,
        desired_voter_count: usize = 0,
        desired_consistent: bool = true,
        desired_ready: bool = true,
        expanded_leader: bool = false,
        representative_desired: ?*const raft_reconciler.PlacementIntent = null,
        needs_expansion: bool = false,
        latched_final_peers: ?[]const u64 = null,
        latched_fingerprint: ?table_manager.VoterSetFingerprint = null,
        latched_voter_count: usize = 0,
        latched_valid: bool = true,
    };

    alloc: std.mem.Allocator,
    groups: std.AutoHashMapUnmanaged(u64, GroupState) = .empty,
    desired_by_member: std.AutoHashMapUnmanaged(u128, *const raft_reconciler.PlacementIntent) = .empty,
    evidence: *const StoreEvidenceIndex,

    fn init(
        alloc: std.mem.Allocator,
        current: CurrentMetadataState,
        desired: []const raft_reconciler.PlacementIntent,
        evidence: *const StoreEvidenceIndex,
    ) !MembershipTransitionIndex {
        var self = MembershipTransitionIndex{
            .alloc = alloc,
            .evidence = evidence,
        };
        errdefer self.deinit();

        for (current.placement_intents) |*intent| {
            const state = try self.groupState(intent.record.group_id);
            state.current_count += 1;
        }
        for (desired) |*intent| {
            try self.desired_by_member.put(alloc, placementNodeKey(intent.record.group_id, intent.record.local_node_id), intent);
            const state = try self.groupState(intent.record.group_id);
            state.desired_count += 1;
            if (state.representative_desired == null) state.representative_desired = intent;
            const fingerprint = table_manager.voterSetFingerprint(intent.peer_node_ids, intent.record.local_node_id);
            const voter_count = table_manager.normalizedVoterCount(intent.peer_node_ids, intent.record.local_node_id);
            if (state.desired_fingerprint) |expected| {
                if (!std.mem.eql(u8, &expected, &fingerprint) or state.desired_voter_count != voter_count)
                    state.desired_consistent = false;
            } else {
                state.desired_fingerprint = fingerprint;
                state.desired_voter_count = voter_count;
            }
        }
        for (current.placement_intents) |*intent| {
            if (!self.desired_by_member.contains(placementNodeKey(intent.record.group_id, intent.record.local_node_id)))
                self.groups.getPtr(intent.record.group_id).?.needs_expansion = true;
            if (intent.serving_state != .retiring or
                self.desired_by_member.contains(placementNodeKey(intent.record.group_id, intent.record.local_node_id))) continue;
            const state = self.groups.getPtr(intent.record.group_id).?;
            const fingerprint = table_manager.voterSetFingerprint(intent.peer_node_ids, null);
            const voter_count = table_manager.normalizedVoterCount(intent.peer_node_ids, null);
            if (intent.peer_node_ids.len == 0 or containsU64(intent.peer_node_ids, intent.record.local_node_id))
                state.latched_valid = false;
            if (state.latched_fingerprint) |expected| {
                if (!std.mem.eql(u8, &expected, &fingerprint) or state.latched_voter_count != voter_count)
                    state.latched_valid = false;
            } else {
                state.latched_final_peers = intent.peer_node_ids;
                state.latched_fingerprint = fingerprint;
                state.latched_voter_count = voter_count;
            }
        }

        for (desired) |*intent| {
            const state = self.groups.getPtr(intent.record.group_id).?;
            const current_intent = self.evidence.placementForMember(intent.record.group_id, intent.record.local_node_id) orelse {
                state.desired_ready = false;
                continue;
            };
            // The final set is published only after the expanded configuration
            // is stable. Validate the currently committed survivor intent here;
            // requiring the desired final intent would wait for a configuration
            // that metadata has not authorized yet.
            const member_ready = current_intent.serving_state == .serving and
                self.memberReadyForContraction(current_intent);
            if (!member_ready)
                state.desired_ready = false;
        }
        // The leader that commits contraction may be the member being
        // removed. Requiring a final-set member to lead before contraction
        // deadlocks whenever the expanded configuration's leader is the
        // source. Final-set leadership is proved separately before metadata
        // removes the retired placement.
        for (current.placement_intents) |*intent| {
            if (intent.serving_state != .serving and intent.serving_state != .draining) continue;
            if (!self.memberReadyForContraction(intent.*)) continue;
            const status = self.statusForIntent(intent.*).?;
            if (status.local_leader) self.groups.getPtr(intent.record.group_id).?.expanded_leader = true;
        }
        return self;
    }

    fn deinit(self: *MembershipTransitionIndex) void {
        self.groups.deinit(self.alloc);
        self.desired_by_member.deinit(self.alloc);
        self.* = undefined;
    }

    fn groupState(self: *MembershipTransitionIndex, group_id: u64) !*GroupState {
        const entry = try self.groups.getOrPut(self.alloc, group_id);
        if (!entry.found_existing) entry.value_ptr.* = .{};
        return entry.value_ptr;
    }

    fn memberReadyForContraction(self: *const MembershipTransitionIndex, intent: raft_reconciler.PlacementIntent) bool {
        const store = self.storeForIntent(intent) orelse return false;
        if (!store.live or !std.mem.eql(u8, store.health_class, "healthy")) return false;
        const status = self.statusForStore(intent.record.group_id, store.store_id) orelse return false;
        if (!status.local_voter or status.joint_consensus or status.transition_pending) return false;
        if (status.replay_required and !status.replay_caught_up) return false;
        return voterSetMatchesIntent(status.*, intent);
    }

    fn storeForIntent(self: *const MembershipTransitionIndex, intent: raft_reconciler.PlacementIntent) ?*const table_manager.StoreRecord {
        return self.evidence.storeForIntent(intent);
    }

    fn statusForStore(self: *const MembershipTransitionIndex, group_id: u64, store_id: u64) ?*const table_manager.GroupStatusReport {
        return self.evidence.statusForStore(group_id, store_id);
    }

    fn statusForIntent(self: *const MembershipTransitionIndex, intent: raft_reconciler.PlacementIntent) ?*const table_manager.GroupStatusReport {
        const store = self.storeForIntent(intent) orelse return null;
        return self.statusForStore(intent.record.group_id, store.store_id);
    }

    fn currentIntent(self: *const MembershipTransitionIndex, group_id: u64, node_id: u64) ?raft_reconciler.PlacementIntent {
        return self.evidence.placementForMember(group_id, node_id);
    }

    fn hasDesiredMember(self: *const MembershipTransitionIndex, group_id: u64, node_id: u64) bool {
        return self.desired_by_member.contains(placementNodeKey(group_id, node_id));
    }

    fn representativeDesired(self: *const MembershipTransitionIndex, group_id: u64) ?raft_reconciler.PlacementIntent {
        const state = self.groups.get(group_id) orelse return null;
        return if (state.representative_desired) |intent| intent.* else null;
    }

    fn needsExpansion(self: *const MembershipTransitionIndex, group_id: u64) bool {
        const state = self.groups.get(group_id) orelse return false;
        return state.needs_expansion;
    }

    fn contracting(self: *const MembershipTransitionIndex, group_id: u64) bool {
        const state = self.groups.get(group_id) orelse return false;
        if (state.latched_final_peers != null) return state.latched_valid;
        return state.desired_count > 0 and
            state.desired_count < state.current_count and
            state.desired_consistent and
            state.desired_ready and
            state.expanded_leader;
    }

    fn latchedFinalPeers(self: *const MembershipTransitionIndex, group_id: u64) ?[]const u64 {
        const state = self.groups.get(group_id) orelse return null;
        if (!state.latched_valid) return null;
        return state.latched_final_peers;
    }

    fn deferDesiredPlacement(self: *const MembershipTransitionIndex, group_id: u64, node_id: u64) bool {
        const state = self.groups.get(group_id) orelse return false;
        if (state.latched_final_peers == null) return false;
        if (!state.latched_valid) return true;
        return !containsU64(state.latched_final_peers.?, node_id);
    }

    fn preserveCurrentPlacement(self: *const MembershipTransitionIndex, group_id: u64, node_id: u64) bool {
        const state = self.groups.get(group_id) orelse return false;
        if (state.desired_count == 0) return false;
        if (state.latched_final_peers == null) return false;
        if (!state.latched_valid) return true;
        return containsU64(state.latched_final_peers.?, node_id);
    }

    fn placementSafeToRemove(self: *const MembershipTransitionIndex, source: raft_reconciler.PlacementIntent) bool {
        const state = self.groups.get(source.record.group_id) orelse return false;
        if (state.desired_count == 0) return true;
        if (source.serving_state != .retiring or !state.latched_valid) return false;
        const final_peers = state.latched_final_peers orelse return false;
        if (containsU64(final_peers, source.record.local_node_id)) return false;

        // A removed member is not guaranteed to apply the configuration entry
        // that removes it: the surviving quorum may commit and stop replication
        // first. A surviving follower can likewise lag the committed entry.
        // Neither stale report can veto finalization: applying the exact stable
        // final set on the live leader proves that the removal committed.
        for (final_peers) |node_id| {
            const intent = self.currentIntent(source.record.group_id, node_id) orelse return false;
            const store = self.storeForIntent(intent) orelse continue;
            if (!store.live or !std.mem.eql(u8, store.health_class, "healthy")) continue;
            const status = self.statusForStore(source.record.group_id, store.store_id) orelse continue;
            if (!status.local_leader) continue;
            if (!status.local_voter or status.joint_consensus or status.transition_pending or
                !status.voter_set_known or @as(usize, status.voter_count) != state.latched_voter_count or
                !std.mem.eql(u8, &status.voter_set_fingerprint, &state.latched_fingerprint.?))
            {
                return false;
            }
            return true;
        }
        return false;
    }
};

fn voterSetMatchesIntent(
    status: table_manager.GroupStatusReport,
    intent: raft_reconciler.PlacementIntent,
) bool {
    if (!status.voter_set_known) return false;
    const expected_count = table_manager.normalizedVoterCount(intent.peer_node_ids, intent.record.local_node_id);
    if (@as(usize, status.voter_count) != expected_count) return false;
    const expected = table_manager.voterSetFingerprint(intent.peer_node_ids, intent.record.local_node_id);
    return std.mem.eql(u8, &status.voter_set_fingerprint, &expected);
}

fn findRestoreProgress(
    records: []const table_manager.RestoreProgressRecord,
    table_id: u64,
    node_id: u64,
    group_id: u64,
    backup_id: []const u8,
    location: []const u8,
    snapshot_path: []const u8,
    artifact_sha256: []const u8,
) ?table_manager.RestoreProgressRecord {
    for (records) |record| {
        if (record.table_id != table_id) continue;
        if (record.node_id != node_id) continue;
        if (record.group_id != group_id) continue;
        if (!std.mem.eql(u8, record.backup_id, backup_id)) continue;
        if (!std.mem.eql(u8, record.location, location)) continue;
        if (!std.mem.eql(u8, record.snapshot_path, snapshot_path)) continue;
        if (!std.mem.eql(u8, record.artifact_sha256, artifact_sha256)) continue;
        return record;
    }
    return null;
}

fn classifyPlacementChange(
    group_id: u64,
    desired_intents: []const raft_reconciler.PlacementIntent,
    current_intents: []const raft_reconciler.PlacementIntent,
    candidate_domains: []const placement_planner.CandidateDomain,
) PlacementChangeKind {
    const desired_count = countPlacementIntents(desired_intents, group_id);
    const current_count = countPlacementIntents(current_intents, group_id);
    if (desired_count == 0 and current_count == 0) return .stable;
    if (placementSetsEqual(group_id, desired_intents, current_intents)) return .stable;
    if (current_count < desired_count) return .repair_required;
    if (hasExcludedCurrentPeer(group_id, current_intents, candidate_domains)) return .repair_required;
    return .rebalance;
}

fn countPlacementIntents(intents: []const raft_reconciler.PlacementIntent, group_id: u64) usize {
    var count: usize = 0;
    for (intents) |intent| {
        if (intent.record.group_id == group_id) count += 1;
    }
    return count;
}

fn allocUnconvergedPlacementGroups(
    alloc: std.mem.Allocator,
    current: CurrentMetadataState,
    desired_splits: []const transition_state.SplitTransitionRecord,
    desired_merges: []const transition_state.MergeTransitionRecord,
    evidence: *const StoreEvidenceIndex,
) ![]u64 {
    const Convergence = struct {
        expected: usize = 0,
        matched: usize = 0,
        invalid_lifecycle: bool = false,
        leader_known: bool = false,
    };

    var convergence = std.AutoHashMapUnmanaged(u64, Convergence).empty;
    defer convergence.deinit(alloc);
    var seen_members = std.AutoHashMapUnmanaged(u128, void).empty;
    defer seen_members.deinit(alloc);
    var busy_groups = std.AutoHashMapUnmanaged(u64, void).empty;
    defer busy_groups.deinit(alloc);
    for (current.split_transitions) |transition| {
        try busy_groups.put(alloc, transition.source_group_id, {});
        try busy_groups.put(alloc, transition.destination_group_id, {});
    }
    for (current.merge_transitions) |transition| {
        try busy_groups.put(alloc, transition.donor_group_id, {});
        try busy_groups.put(alloc, transition.receiver_group_id, {});
    }
    // Desired transitions are admitted later in this same reconciliation
    // plan. Protect their current quorums now so placement planning cannot
    // begin a membership transition in the projection gap before admission.
    for (desired_splits) |transition| {
        try busy_groups.put(alloc, transition.source_group_id, {});
        try busy_groups.put(alloc, transition.destination_group_id, {});
    }
    for (desired_merges) |transition| {
        try busy_groups.put(alloc, transition.donor_group_id, {});
        try busy_groups.put(alloc, transition.receiver_group_id, {});
    }

    for (current.placement_intents) |*intent| {
        const entry = try convergence.getOrPut(alloc, intent.record.group_id);
        if (!entry.found_existing) entry.value_ptr.* = .{};
        entry.value_ptr.expected += 1;
        entry.value_ptr.invalid_lifecycle = entry.value_ptr.invalid_lifecycle or
            intent.serving_state != .serving or
            busy_groups.contains(intent.record.group_id);
        const member = try seen_members.getOrPut(alloc, placementNodeKey(intent.record.group_id, intent.record.local_node_id));
        if (member.found_existing) {
            entry.value_ptr.invalid_lifecycle = true;
            continue;
        }
        member.value_ptr.* = {};
        const store = evidence.storeForIntent(intent.*) orelse continue;
        if (!store.live or !std.mem.eql(u8, store.health_class, "healthy")) continue;
        const status = evidence.statusForStore(intent.record.group_id, store.store_id) orelse continue;
        if (status.relocation_generation != intent.relocation_generation) continue;
        if (!status.local_voter or status.joint_consensus or status.transition_pending) continue;
        if (!voterSetMatchesIntent(status.*, intent.*)) continue;
        entry.value_ptr.matched += 1;
        entry.value_ptr.leader_known = entry.value_ptr.leader_known or status.local_leader;
    }

    var groups = std.ArrayListUnmanaged(u64).empty;
    errdefer groups.deinit(alloc);
    var it = convergence.iterator();
    while (it.next()) |entry| {
        const state = entry.value_ptr.*;
        if (state.invalid_lifecycle or state.matched != state.expected or !state.leader_known) {
            try groups.append(alloc, entry.key_ptr.*);
        }
    }
    std.mem.sort(u64, groups.items, {}, comptime std.sort.asc(u64));
    return try groups.toOwnedSlice(alloc);
}

test "metadata reconciler protects desired split quorum before admission projection" {
    const placements = [_]raft_reconciler.PlacementIntent{.{
        .record = .{ .group_id = 15001, .replica_id = 1, .local_node_id = 101 },
        .peer_node_ids = &.{101},
        .serving_state = .serving,
    }};
    const desired_splits = [_]transition_state.SplitTransitionRecord{.{
        .transition_id = 15003,
        .attempt_epoch = 1,
        .source_group_id = 15001,
        .destination_group_id = 15002,
    }};
    const current: CurrentMetadataState = .{ .placement_intents = &placements };
    var evidence = try StoreEvidenceIndex.init(std.testing.allocator, current);
    defer evidence.deinit();
    const protected = try allocUnconvergedPlacementGroups(
        std.testing.allocator,
        current,
        &desired_splits,
        &.{},
        &evidence,
    );
    defer std.testing.allocator.free(protected);

    try std.testing.expectEqualSlices(u64, &.{15001}, protected);
}

test "metadata reconciler placement convergence requires the exact placement store" {
    const intent: raft_reconciler.PlacementIntent = .{
        .record = .{ .group_id = 15011, .replica_id = 1, .local_node_id = 101 },
        .store_id = 1010,
        .peer_node_ids = &.{101},
        .serving_state = .serving,
        .relocation_generation = 7,
    };
    const status: table_manager.GroupStatusReport = .{
        .group_id = 15011,
        .relocation_generation = 7,
        .local_leader = true,
        .local_voter = true,
        .voter_count = 1,
        .voter_set_known = true,
        .voter_set_fingerprint = table_manager.voterSetFingerprint(&.{101}, 101),
    };
    var stores = [_]table_manager.StoreRecord{
        .{ .store_id = 1010, .node_id = 101 },
        .{
            .store_id = 1011,
            .node_id = 101,
            .group_statuses = @constCast((&[_]table_manager.GroupStatusReport{status})[0..]),
        },
    };

    var current: CurrentMetadataState = .{ .placement_intents = &.{intent}, .stores = &stores };
    var evidence = try StoreEvidenceIndex.init(std.testing.allocator, current);
    const protected = try allocUnconvergedPlacementGroups(
        std.testing.allocator,
        current,
        &.{},
        &.{},
        &evidence,
    );
    defer std.testing.allocator.free(protected);
    try std.testing.expectEqualSlices(u64, &.{15011}, protected);
    evidence.deinit();

    stores[0].group_statuses = @constCast((&[_]table_manager.GroupStatusReport{status})[0..]);
    current.stores = &stores;
    evidence = try StoreEvidenceIndex.init(std.testing.allocator, current);
    defer evidence.deinit();
    const converged = try allocUnconvergedPlacementGroups(
        std.testing.allocator,
        current,
        &.{},
        &.{},
        &evidence,
    );
    defer std.testing.allocator.free(converged);
    try std.testing.expectEqual(@as(usize, 0), converged.len);
}

test "metadata reconciler rejects duplicate exact store identities" {
    const intent: raft_reconciler.PlacementIntent = .{
        .record = .{ .group_id = 15012, .replica_id = 1, .local_node_id = 101 },
        .store_id = 1010,
    };
    const stores = [_]table_manager.StoreRecord{
        .{ .store_id = 1010, .node_id = 101 },
        .{ .store_id = 1010, .node_id = 101 },
    };
    var evidence = try StoreEvidenceIndex.init(std.testing.allocator, .{ .placement_intents = &.{intent}, .stores = &stores });
    defer evidence.deinit();
    try std.testing.expect(evidence.storeForIntent(intent) == null);
}

test "metadata reconciler rejects duplicate placement member evidence" {
    const intent: raft_reconciler.PlacementIntent = .{
        .record = .{ .group_id = 15013, .replica_id = 1, .local_node_id = 101 },
        .store_id = 1010,
        .peer_node_ids = &.{101},
        .serving_state = .replaying,
        .relocation_generation = 7,
    };
    var duplicate = intent;
    duplicate.record.replica_id = 2;
    const placements = [_]raft_reconciler.PlacementIntent{ intent, duplicate };
    const stores = [_]table_manager.StoreRecord{.{
        .store_id = 1010,
        .node_id = 101,
        .group_statuses = @constCast((&[_]table_manager.GroupStatusReport{.{
            .group_id = 15013,
            .relocation_generation = 7,
            .local_voter = true,
            .voter_count = 1,
            .voter_set_known = true,
            .voter_set_fingerprint = table_manager.voterSetFingerprint(&.{101}, 101),
        }})[0..]),
    }};

    var evidence = try StoreEvidenceIndex.init(std.testing.allocator, .{
        .placement_intents = &placements,
        .stores = &stores,
    });
    defer evidence.deinit();
    try std.testing.expect(evidence.placementForMember(15013, 101) == null);
    try std.testing.expect(!relocationTargetCutoverReady(&evidence, intent));
}

fn placementNodeKey(group_id: u64, node_id: u64) u128 {
    return (@as(u128, group_id) << 64) | @as(u128, node_id);
}

fn placementStoreKey(group_id: u64, store_id: u64) u128 {
    return (@as(u128, group_id) << 64) | @as(u128, store_id);
}

fn findPlacementIntentForGroup(
    intents: []const raft_reconciler.PlacementIntent,
    group_id: u64,
) ?raft_reconciler.PlacementIntent {
    for (intents) |intent| {
        if (intent.record.group_id == group_id) return intent;
    }
    return null;
}

fn allocExpandedTransitionPeerNodeIds(
    alloc: std.mem.Allocator,
    current_intents: []const raft_reconciler.PlacementIntent,
    desired: raft_reconciler.PlacementIntent,
) !?[]u64 {
    var needs_expansion = false;
    for (current_intents) |current_intent| {
        if (current_intent.record.group_id != desired.record.group_id) continue;
        if (!containsU64(desired.peer_node_ids, current_intent.record.local_node_id)) {
            needs_expansion = true;
            break;
        }
    }
    if (!needs_expansion) return null;

    var peers = std.ArrayListUnmanaged(u64).empty;
    errdefer peers.deinit(alloc);
    for (desired.peer_node_ids) |node_id| {
        if (!containsU64(peers.items, node_id)) try peers.append(alloc, node_id);
    }
    if (!containsU64(peers.items, desired.record.local_node_id)) {
        try peers.append(alloc, desired.record.local_node_id);
    }
    for (current_intents) |current_intent| {
        if (current_intent.record.group_id != desired.record.group_id) continue;
        if (!containsU64(peers.items, current_intent.record.local_node_id)) {
            try peers.append(alloc, current_intent.record.local_node_id);
        }
    }
    std.mem.sort(u64, peers.items, {}, std.sort.asc(u64));
    return try peers.toOwnedSlice(alloc);
}

fn placementSetsEqual(group_id: u64, desired_intents: []const raft_reconciler.PlacementIntent, current_intents: []const raft_reconciler.PlacementIntent) bool {
    const desired_count = countPlacementIntents(desired_intents, group_id);
    const current_count = countPlacementIntents(current_intents, group_id);
    if (desired_count != current_count) return false;
    for (desired_intents) |desired| {
        if (desired.record.group_id != group_id) continue;
        if (findPlacementIntent(current_intents, group_id, desired.record.local_node_id) == null) return false;
    }
    return true;
}

fn pruneAutomaticIntents(
    alloc: std.mem.Allocator,
    manager: *table_manager.TableManager,
    current: CurrentMetadataState,
    desired_split_ids: []const u64,
    desired_merge_ids: []const u64,
) !void {
    var split_ids = std.ArrayListUnmanaged(u64).empty;
    defer split_ids.deinit(alloc);
    var split_it = manager.split_intents.iterator();
    while (split_it.next()) |entry| {
        if (!entry.value_ptr.automatic) continue;
        if (containsU64(desired_split_ids, entry.key_ptr.*)) continue;
        if (findSplitRecord(current.split_transitions, entry.key_ptr.*) != null) continue;
        if (automaticSplitIntentStillValid(manager, entry.value_ptr.*)) continue;
        try split_ids.append(alloc, entry.key_ptr.*);
    }
    for (split_ids.items) |transition_id| _ = manager.removeSplitIntent(transition_id);

    var merge_ids = std.ArrayListUnmanaged(u64).empty;
    defer merge_ids.deinit(alloc);
    var merge_it = manager.merge_intents.iterator();
    while (merge_it.next()) |entry| {
        if (!entry.value_ptr.automatic) continue;
        if (containsU64(desired_merge_ids, entry.key_ptr.*)) continue;
        if (findMergeRecord(current.merge_transitions, entry.key_ptr.*) != null) continue;
        if (automaticMergeIntentStillValid(manager, entry.value_ptr.*)) continue;
        try merge_ids.append(alloc, entry.key_ptr.*);
    }
    for (merge_ids.items) |transition_id| _ = manager.removeMergeIntent(transition_id);
}

fn automaticSplitIntentStillValid(manager: *table_manager.TableManager, intent: table_manager.SplitIntent) bool {
    const source = manager.ranges.get(intent.source_group_id) orelse return false;
    if (source.table_id != intent.table_id) return false;
    if (manager.ranges.contains(intent.destination_group_id)) return false;
    return keyStrictlyInsideRange(intent.split_key, source.start_key, source.end_key);
}

fn automaticMergeIntentStillValid(manager: *table_manager.TableManager, intent: table_manager.MergeIntent) bool {
    const donor = manager.ranges.get(intent.donor_group_id) orelse return false;
    const receiver = manager.ranges.get(intent.receiver_group_id) orelse return false;
    if (donor.table_id != intent.table_id or receiver.table_id != intent.table_id) return false;
    return rangesAdjacent(donor, receiver);
}

fn activeRangeTransitionCount(current: CurrentMetadataState) u32 {
    const total = current.split_transitions.len +| current.merge_transitions.len;
    return std.math.cast(u32, total) orelse std.math.maxInt(u32);
}

const AutomaticPlanningIndex = struct {
    const RangeSpan = struct {
        start: usize,
        len: usize,
    };

    alloc: std.mem.Allocator,
    sorted_ranges: []const table_manager.RangeRecord,
    range_spans_by_table: std.AutoHashMapUnmanaged(u64, RangeSpan) = .empty,
    table_by_group: std.AutoHashMapUnmanaged(u64, u64) = .empty,
    active_transitions_by_table: std.AutoHashMapUnmanaged(u64, u32) = .empty,
    busy_groups: std.AutoHashMapUnmanaged(u64, void) = .empty,
    occupied_group_ids: std.AutoHashMapUnmanaged(u64, void) = .empty,

    fn init(
        alloc: std.mem.Allocator,
        current: CurrentMetadataState,
        sorted_ranges: []const table_manager.RangeRecord,
    ) !AutomaticPlanningIndex {
        var self: AutomaticPlanningIndex = .{
            .alloc = alloc,
            .sorted_ranges = sorted_ranges,
        };
        errdefer self.deinit();

        try self.table_by_group.ensureTotalCapacity(alloc, @intCast(sorted_ranges.len));
        try self.occupied_group_ids.ensureTotalCapacity(
            alloc,
            @intCast(sorted_ranges.len +
                current.split_transitions.len * 2 +
                current.merge_transitions.len * 2),
        );
        var start: usize = 0;
        while (start < sorted_ranges.len) {
            const table_id = sorted_ranges[start].table_id;
            var end = start;
            while (end < sorted_ranges.len and sorted_ranges[end].table_id == table_id) : (end += 1) {
                const range = sorted_ranges[end];
                const table_entry = self.table_by_group.getOrPutAssumeCapacity(range.group_id);
                if (table_entry.found_existing) return error.DuplicateRangeGroup;
                table_entry.value_ptr.* = table_id;
                try self.occupied_group_ids.put(alloc, range.group_id, {});
            }
            try self.range_spans_by_table.put(alloc, table_id, .{
                .start = start,
                .len = end - start,
            });
            start = end;
        }

        for (current.split_transitions) |record| {
            try self.markExistingTransition(
                record.source_group_id,
                record.destination_group_id,
            );
        }
        for (current.merge_transitions) |record| {
            try self.markExistingTransition(
                record.donor_group_id,
                record.receiver_group_id,
            );
        }
        return self;
    }

    fn deinit(self: *AutomaticPlanningIndex) void {
        self.range_spans_by_table.deinit(self.alloc);
        self.table_by_group.deinit(self.alloc);
        self.active_transitions_by_table.deinit(self.alloc);
        self.busy_groups.deinit(self.alloc);
        self.occupied_group_ids.deinit(self.alloc);
        self.* = undefined;
    }

    fn rangesForTable(
        self: *const AutomaticPlanningIndex,
        table_id: u64,
    ) []const table_manager.RangeRecord {
        const span = self.range_spans_by_table.get(table_id) orelse return &.{};
        return self.sorted_ranges[span.start..][0..span.len];
    }

    fn activeTransitionCount(self: *const AutomaticPlanningIndex, table_id: u64) u32 {
        return self.active_transitions_by_table.get(table_id) orelse 0;
    }

    fn groupBusy(self: *const AutomaticPlanningIndex, group_id: u64) bool {
        return self.busy_groups.contains(group_id);
    }

    fn groupExists(self: *const AutomaticPlanningIndex, group_id: u64) bool {
        return !group_ids.isDataGroupId(group_id) or
            self.occupied_group_ids.contains(group_id);
    }

    fn reserveGroup(self: *AutomaticPlanningIndex, group_id: u64) !void {
        if (self.groupExists(group_id)) return error.AutomaticGroupIdCollision;
        try self.occupied_group_ids.put(self.alloc, group_id, {});
    }

    fn markBusy(
        self: *AutomaticPlanningIndex,
        first_group_id: u64,
        second_group_id: u64,
    ) !void {
        try self.busy_groups.put(self.alloc, first_group_id, {});
        try self.busy_groups.put(self.alloc, second_group_id, {});
    }

    fn markExistingTransition(
        self: *AutomaticPlanningIndex,
        first_group_id: u64,
        second_group_id: u64,
    ) !void {
        try self.markBusy(first_group_id, second_group_id);
        try self.occupied_group_ids.put(self.alloc, first_group_id, {});
        try self.occupied_group_ids.put(self.alloc, second_group_id, {});
        const table_id = self.table_by_group.get(first_group_id) orelse
            self.table_by_group.get(second_group_id) orelse
            return;
        const entry = try self.active_transitions_by_table.getOrPut(self.alloc, table_id);
        if (!entry.found_existing) entry.value_ptr.* = 0;
        entry.value_ptr.* = std.math.add(u32, entry.value_ptr.*, 1) catch
            return error.TooManyActiveRangeTransitions;
    }
};

fn mergedGroupStatus(current: CurrentMetadataState, group_id: u64) ?MergedGroupStatus {
    if (current.merged_group_statuses.len > 0) {
        // Captured state already merges placement-fenced storage and runtime
        // facts. Re-reading stores here would mix snapshots and rescan the
        // entire topology once per planned shard.
        for (current.merged_group_statuses) |status| {
            if (status.group_id == group_id) return status;
        }
        return null;
    }
    const fallback = mergeHealthyGroupStatusFallback(
        current.stores,
        current.placement_intents,
        group_id,
    ) orelse return null;
    return mergeRuntimeGroupFacts(fallback, current.stores, current.placement_intents, current.ranges, group_id);
}

test "metadata reconciler trusts captured merged group status" {
    const captured = MergedGroupStatus{
        .group_id = 701,
        .doc_count = 17,
        .disk_bytes = 170,
        .disk_bytes_known = true,
        .updated_at_millis = 100,
    };
    const stores = [_]table_manager.StoreRecord{.{
        .store_id = 1,
        .node_id = 1,
        .live = true,
        .health_class = "healthy",
        .runtime_statuses = @constCast((&[_]table_manager.RuntimeGroupStatusReport{.{
            .group_id = 701,
            .doc_count = 99,
            .disk_bytes = 990,
            .disk_bytes_known = true,
            .updated_at_ns = 200 * std.time.ns_per_ms,
        }})[0..]),
    }};

    const resolved = mergedGroupStatus(.{
        .stores = &stores,
        .merged_group_statuses = &.{captured},
    }, captured.group_id).?;
    try std.testing.expectEqual(captured.doc_count, resolved.doc_count);
    try std.testing.expectEqual(captured.disk_bytes, resolved.disk_bytes);
    try std.testing.expectEqual(captured.updated_at_millis, resolved.updated_at_millis);
}

fn mergeRuntimeGroupFacts(
    base: MergedGroupStatus,
    stores: []const table_manager.StoreRecord,
    placements: []const raft_reconciler.PlacementIntent,
    ranges: []const table_manager.RangeRecord,
    group_id: u64,
) MergedGroupStatus {
    var merged = base;
    var healthy_voter_reports: u16 = 0;
    for (stores) |store| {
        if (!store.live) continue;
        if (!std.mem.eql(u8, store.health_class, "healthy")) continue;
        const placement = currentPlacementForStore(
            placements,
            group_id,
            store,
        );
        if (placements.len > 0 and placement == null) continue;
        var store_reports_voter = false;
        for (store.group_statuses) |status| {
            if (status.group_id != group_id) continue;
            if (placement) |intent| {
                if (status.relocation_generation != intent.relocation_generation)
                    continue;
            }
            store_reports_voter = store_reports_voter or status.local_voter;
        }
        for (store.runtime_statuses) |status| {
            if (status.group_id != group_id) continue;
            if (status.doc_count > merged.doc_count) merged.doc_count = status.doc_count;
            if (status.disk_bytes_known and (!merged.disk_bytes_known or status.disk_bytes > merged.disk_bytes)) {
                merged.disk_bytes = status.disk_bytes;
                merged.disk_bytes_known = true;
            }
            if (status.doc_count > 0) merged.empty = false;
            const updated_at_millis = @divTrunc(status.updated_at_ns, std.time.ns_per_ms);
            if (updated_at_millis > merged.updated_at_millis) merged.updated_at_millis = updated_at_millis;
            mergeRuntimeDocIdentity(&merged, status.doc_identity);
        }
        if (store_reports_voter) healthy_voter_reports +|= 1;
    }
    if (healthy_voter_reports > merged.healthy_voter_reports) merged.healthy_voter_reports = healthy_voter_reports;
    markDocIdentityRebuildRequiredOnNamespaceMismatch(&merged, ranges, group_id);
    refreshDocIdentityLifecycle(&merged);
    return merged;
}

fn mergeRuntimeDocIdentity(
    merged: *MergedGroupStatus,
    incoming: table_manager.RuntimeDocIdentityStatusReport,
) void {
    if (!runtimeDocIdentityHasFacts(incoming)) return;
    if (!runtimeDocIdentityHasFacts(merged.doc_identity)) {
        merged.doc_identity = incoming;
        return;
    }

    if (runtimeDocIdentityHasOrdinalRows(merged.doc_identity) and runtimeDocIdentityHasOrdinalRows(incoming) and
        !runtimeDocIdentitySameNamespace(merged.doc_identity, incoming))
    {
        merged.doc_identity_namespace_conflict = true;
    }

    merged.doc_identity.rebuild_required = merged.doc_identity.rebuild_required or incoming.rebuild_required;
    merged.doc_identity.ordinal_capacity_exhausted = merged.doc_identity.ordinal_capacity_exhausted or incoming.ordinal_capacity_exhausted;
    merged.doc_identity.complete = merged.doc_identity.complete and incoming.complete;
    merged.doc_identity.allocated_ordinals = @max(merged.doc_identity.allocated_ordinals, incoming.allocated_ordinals);
    merged.doc_identity.state_rows = @max(merged.doc_identity.state_rows, incoming.state_rows);
    merged.doc_identity.live_ordinals = @max(merged.doc_identity.live_ordinals, incoming.live_ordinals);
    merged.doc_identity.tombstone_ordinals = @max(merged.doc_identity.tombstone_ordinals, incoming.tombstone_ordinals);
    merged.doc_identity.primary_docs_missing_ordinals = @max(merged.doc_identity.primary_docs_missing_ordinals, incoming.primary_docs_missing_ordinals);
    merged.doc_identity.primary_docs_missing_identity_state = @max(merged.doc_identity.primary_docs_missing_identity_state, incoming.primary_docs_missing_identity_state);
    merged.doc_identity.primary_docs_with_tombstone_ordinals = @max(merged.doc_identity.primary_docs_with_tombstone_ordinals, incoming.primary_docs_with_tombstone_ordinals);
}

fn markDocIdentityRebuildRequiredOnNamespaceMismatch(
    status: *MergedGroupStatus,
    ranges: []const table_manager.RangeRecord,
    group_id: u64,
) void {
    if (!runtimeDocIdentityHasNamespace(status.doc_identity)) return;
    const range = findRangeRecord(ranges, group_id) orelse return;
    if (status.doc_identity.namespace_table_id == range.table_id and
        status.doc_identity.namespace_shard_id == table_manager.rangeDocIdentityShardId(range) and
        status.doc_identity.namespace_range_id == table_manager.rangeDocIdentityRangeId(range)) return;
    status.doc_identity.rebuild_required = true;
}

fn docIdentityNamespacesCompatibleForAutomaticMerge(left: MergedGroupStatus, right: MergedGroupStatus) bool {
    if (left.doc_identity_reassignment_active or right.doc_identity_reassignment_active) return false;
    if (left.doc_identity_namespace_conflict or right.doc_identity_namespace_conflict) return false;
    if (left.doc_identity.rebuild_required or right.doc_identity.rebuild_required) return false;
    if (left.doc_identity.ordinal_capacity_exhausted or right.doc_identity.ordinal_capacity_exhausted) return false;
    if (!runtimeDocIdentityHasOrdinalRows(left.doc_identity) or !runtimeDocIdentityHasOrdinalRows(right.doc_identity)) return true;
    return runtimeDocIdentitySameNamespace(left.doc_identity, right.doc_identity);
}

fn docIdentityNamespaceReadyForAutomaticSplit(status: MergedGroupStatus) bool {
    if (status.doc_identity_reassignment_active) return false;
    if (status.doc_identity_namespace_conflict) return false;
    if (status.doc_identity.rebuild_required) return false;
    if (status.doc_identity.ordinal_capacity_exhausted) return false;
    return true;
}

const ReassignmentActivityPolicy = enum { disallow_active, allow_existing_active };

fn mergeTransitionDocIdentityCompatible(
    current: CurrentMetadataState,
    record: transition_state.MergeTransitionRecord,
    activity_policy: ReassignmentActivityPolicy,
) bool {
    const donor = mergedGroupStatus(current, record.donor_group_id) orelse return mergeTransitionMissingDocIdentityStatusCompatible(current, record);
    const receiver = mergedGroupStatus(current, record.receiver_group_id) orelse return mergeTransitionMissingDocIdentityStatusCompatible(current, record);
    if (record.allow_doc_identity_reassignment) return docIdentityNamespacesCanReassign(donor, receiver, activity_policy);
    return docIdentityNamespacesCompatibleForAutomaticMerge(donor, receiver);
}

fn mergeTransitionDocIdentityCompatibleIndexed(
    current: CurrentMetadataState,
    evidence: *const StoreEvidenceIndex,
    record: transition_state.MergeTransitionRecord,
    activity_policy: ReassignmentActivityPolicy,
) bool {
    const donor = evidence.mergedStatus(current, record.donor_group_id) orelse return mergeTransitionMissingDocIdentityStatusCompatible(current, record);
    const receiver = evidence.mergedStatus(current, record.receiver_group_id) orelse return mergeTransitionMissingDocIdentityStatusCompatible(current, record);
    if (record.allow_doc_identity_reassignment) return docIdentityNamespacesCanReassign(donor, receiver, activity_policy);
    return docIdentityNamespacesCompatibleForAutomaticMerge(donor, receiver);
}

fn mergeTransitionMissingDocIdentityStatusCompatible(
    current: CurrentMetadataState,
    record: transition_state.MergeTransitionRecord,
) bool {
    if (record.allow_doc_identity_reassignment) return false;
    return !currentHasDocIdentityTelemetry(current);
}

fn splitTransitionDocIdentityCompatible(current: CurrentMetadataState, record: transition_state.SplitTransitionRecord) bool {
    const source = mergedGroupStatus(current, record.source_group_id) orelse return !currentHasDocIdentityTelemetry(current);
    return docIdentityNamespaceReadyForAutomaticSplit(source);
}

fn splitTransitionDocIdentityCompatibleIndexed(
    current: CurrentMetadataState,
    evidence: *const StoreEvidenceIndex,
    record: transition_state.SplitTransitionRecord,
) bool {
    const source = evidence.mergedStatus(current, record.source_group_id) orelse return !currentHasDocIdentityTelemetry(current);
    return docIdentityNamespaceReadyForAutomaticSplit(source);
}

fn currentHasDocIdentityTelemetry(current: CurrentMetadataState) bool {
    if (current.merged_group_statuses.len > 0) return true;
    for (current.stores) |store| {
        for (store.runtime_statuses) |status| {
            if (runtimeDocIdentityHasFacts(status.doc_identity)) return true;
        }
    }
    return false;
}

fn docIdentityNamespacesCanReassign(left: MergedGroupStatus, right: MergedGroupStatus, activity_policy: ReassignmentActivityPolicy) bool {
    if (activity_policy == .disallow_active and
        (left.doc_identity_reassignment_active or right.doc_identity_reassignment_active)) return false;
    if (left.doc_identity_namespace_conflict or right.doc_identity_namespace_conflict) return false;
    if (left.doc_identity.rebuild_required or right.doc_identity.rebuild_required) return false;
    if (left.doc_identity.ordinal_capacity_exhausted or right.doc_identity.ordinal_capacity_exhausted) return false;
    return true;
}

pub fn refreshDocIdentityLifecycle(status: *MergedGroupStatus) void {
    status.doc_identity_lifecycle = deriveDocIdentityLifecycle(status.*);
}

pub fn deriveDocIdentityLifecycle(status: MergedGroupStatus) []const u8 {
    if (status.doc_identity_namespace_conflict or
        status.doc_identity.rebuild_required or
        status.doc_identity.ordinal_capacity_exhausted)
    {
        return doc_identity_lifecycle_rebuild_required;
    }
    if (status.doc_identity_reassignment_active) return doc_identity_lifecycle_reassigning;
    if (!runtimeDocIdentityHasFacts(status.doc_identity)) return doc_identity_lifecycle_unknown;
    if (status.doc_identity.complete and runtimeDocIdentityRepairCountersClear(status.doc_identity)) {
        return doc_identity_lifecycle_ready;
    }
    return doc_identity_lifecycle_preserving;
}

fn runtimeDocIdentityRepairCountersClear(stats: table_manager.RuntimeDocIdentityStatusReport) bool {
    return stats.primary_docs_missing_ordinals == 0 and
        stats.primary_docs_missing_identity_state == 0 and
        stats.primary_docs_with_tombstone_ordinals == 0;
}

test "metadata reconciler doc identity guards block new planning during active reassignment" {
    var receiver = MergedGroupStatus{
        .group_id = 9001,
        .doc_identity_reassignment_active = true,
        .doc_identity = .{
            .namespace_table_id = 90,
            .namespace_shard_id = 9001,
            .namespace_range_id = 1,
            .next_ordinal = 11,
            .allocated_ordinals = 10,
            .live_ordinals = 10,
        },
    };
    const donor = MergedGroupStatus{
        .group_id = 9002,
        .doc_identity = .{
            .namespace_table_id = 90,
            .namespace_shard_id = 9002,
            .namespace_range_id = 2,
            .next_ordinal = 9,
            .allocated_ordinals = 8,
            .live_ordinals = 8,
        },
    };
    const statuses = [_]MergedGroupStatus{ receiver, donor };
    const current = CurrentMetadataState{ .merged_group_statuses = &statuses };
    const merge = transition_state.MergeTransitionRecord{
        .transition_id = 90001,
        .donor_group_id = 9002,
        .receiver_group_id = 9001,
        .allow_doc_identity_reassignment = true,
    };

    try std.testing.expect(!docIdentityNamespacesCompatibleForAutomaticMerge(receiver, donor));
    try std.testing.expect(!docIdentityNamespaceReadyForAutomaticSplit(receiver));
    try std.testing.expect(!mergeTransitionDocIdentityCompatible(current, merge, .disallow_active));
    try std.testing.expect(mergeTransitionDocIdentityCompatible(current, merge, .allow_existing_active));

    receiver.doc_identity_reassignment_active = false;
    receiver.doc_identity_namespace_conflict = true;
    try std.testing.expect(!docIdentityNamespacesCanReassign(receiver, donor, .disallow_active));
}

fn splitTransitionCanRollback(record: transition_state.SplitTransitionRecord) bool {
    return switch (record.phase) {
        .finalized, .rolled_back => false,
        else => true,
    };
}

fn terminalSplitObservationPhase(observation: transition_state.SplitObservation) ?transition_state.TransitionPhase {
    return switch (observation.status.phase) {
        .finalized => .finalized,
        .rolled_back => .rolled_back,
        else => null,
    };
}

fn mergeTransitionCanRollback(record: transition_state.MergeTransitionRecord) bool {
    return switch (record.phase) {
        .finalized, .rolled_back => false,
        else => true,
    };
}

fn terminalMergeObservationPhase(observation: transition_state.MergeObservation) ?transition_state.TransitionPhase {
    return switch (observation.receiver.phase) {
        .finalized => .finalized,
        .rolled_back => .rolled_back,
        else => null,
    };
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

fn runtimeDocIdentityHasNamespace(stats: table_manager.RuntimeDocIdentityStatusReport) bool {
    return stats.namespace_table_id != 0 or
        stats.namespace_shard_id != 0 or
        stats.namespace_range_id != 0;
}

fn runtimeDocIdentitySameNamespace(
    left: table_manager.RuntimeDocIdentityStatusReport,
    right: table_manager.RuntimeDocIdentityStatusReport,
) bool {
    return left.namespace_table_id == right.namespace_table_id and
        left.namespace_shard_id == right.namespace_shard_id and
        left.namespace_range_id == right.namespace_range_id;
}

fn currentPlacementForStore(
    placements: []const raft_reconciler.PlacementIntent,
    group_id: u64,
    store: table_manager.StoreRecord,
) ?raft_reconciler.PlacementIntent {
    var found: ?raft_reconciler.PlacementIntent = null;
    for (placements) |intent| {
        if (intent.record.group_id != group_id or
            intent.record.local_node_id != store.node_id)
        {
            continue;
        }
        if (found != null) return null;
        found = intent;
    }
    const intent = found orelse return null;
    if (intent.store_id != 0 and intent.store_id != store.store_id) return null;
    return intent;
}

fn mergeHealthyGroupStatusFallback(
    stores: []const table_manager.StoreRecord,
    placements: []const raft_reconciler.PlacementIntent,
    group_id: u64,
) ?MergedGroupStatus {
    var latest: ?table_manager.GroupStatusReport = null;
    var leader_evidence: table_manager.GroupLeaderEvidence = .{};
    var voter_set_evidence: table_manager.VoterSetEvidence = .{};
    var healthy_voter_reports: u16 = 0;
    var transition_pending = false;
    var replay_required = false;
    var replay_caught_up = false;
    var cutover_ready = false;
    var reads_ready_after_cutover = false;
    var joint_consensus = false;

    for (stores) |store| {
        if (!store.live) continue;
        if (!std.mem.eql(u8, store.health_class, "healthy")) continue;
        const placement = currentPlacementForStore(
            placements,
            group_id,
            store,
        );
        if (placements.len > 0 and placement == null) continue;

        var counted_voter_for_store = false;
        for (store.group_statuses) |status| {
            if (status.group_id != group_id) continue;
            if (placement) |intent| {
                if (status.relocation_generation != intent.relocation_generation)
                    continue;
            }
            if (latest == null or status.updated_at_millis >= latest.?.updated_at_millis) {
                latest = status;
            }
            if (status.local_voter and !counted_voter_for_store) {
                healthy_voter_reports +|= 1;
                counted_voter_for_store = true;
            }
            leader_evidence.observe(store.store_id, status);
            voter_set_evidence.observe(status);
            transition_pending = transition_pending or status.transition_pending;
            replay_required = replay_required or status.replay_required;
            replay_caught_up = replay_caught_up or status.replay_caught_up;
            cutover_ready = cutover_ready or status.cutover_ready;
            reads_ready_after_cutover = reads_ready_after_cutover or status.reads_ready_after_cutover;
            joint_consensus = joint_consensus or status.joint_consensus;
        }
    }

    const base = latest orelse return null;
    const authoritative_leader = leader_evidence.resolve();
    const voter_set = voter_set_evidence.resolve(
        if (authoritative_leader) |candidate| candidate.report else null,
    );
    var merged: MergedGroupStatus = .{
        .group_id = base.group_id,
        .doc_count = base.doc_count,
        .disk_bytes = base.disk_bytes,
        .disk_bytes_known = base.disk_bytes_known,
        .empty = base.empty,
        .created_at_millis = base.created_at_millis,
        .updated_at_millis = base.updated_at_millis,
        .leader_known = false,
        .leader_store_id = 0,
        .voter_count_known = voter_set.voter_count_known,
        .voter_count = voter_set.voter_count,
        .voter_set_known = voter_set.voter_set_known,
        .voter_set_fingerprint = voter_set.voter_set_fingerprint,
        .healthy_voter_reports = healthy_voter_reports,
        .joint_consensus = joint_consensus,
        .readiness_from_leader = false,
        .transition_pending = transition_pending,
        .replay_required = replay_required,
        .replay_caught_up = replay_caught_up,
        .cutover_ready = cutover_ready,
        .reads_ready_after_cutover = reads_ready_after_cutover,
    };
    if (authoritative_leader) |leader| {
        merged.leader_known = true;
        merged.leader_store_id = leader.store_id;
        merged.readiness_from_leader = true;
        if (voter_set.from_leader) {
            merged.joint_consensus = leader.report.joint_consensus;
        }
        merged.transition_pending = leader.report.transition_pending;
        merged.replay_required = leader.report.replay_required;
        merged.replay_caught_up = leader.report.replay_caught_up;
        merged.cutover_ready = leader.report.cutover_ready;
        merged.reads_ready_after_cutover = leader.report.reads_ready_after_cutover;
    }
    return merged;
}

test "metadata reconciler requires exact leader voter evidence for placement readiness" {
    const placements = [_]raft_reconciler.PlacementIntent{
        .{
            .record = .{ .group_id = 7001, .replica_id = 1, .local_node_id = 1 },
            .peer_node_ids = &.{ 1, 2 },
        },
        .{
            .record = .{ .group_id = 7001, .replica_id = 2, .local_node_id = 2 },
            .peer_node_ids = &.{ 1, 2 },
        },
    };
    const current = CurrentMetadataState{ .placement_intents = &placements };
    var evidence = try StoreEvidenceIndex.init(std.testing.allocator, current);
    defer evidence.deinit();
    const fingerprint = table_manager.voterSetFingerprint(&.{ 1, 2 }, null);

    try std.testing.expect(!evidence.hasFullHealthyPlacement(
        7001,
        .{
            .group_id = 7001,
            .leader_known = true,
            .readiness_from_leader = true,
            .voter_count_known = true,
            .voter_set_known = true,
            .voter_set_fingerprint = fingerprint,
            .voter_count = 2,
            .healthy_voter_reports = 1,
        },
    ));
    try std.testing.expect(evidence.hasFullHealthyPlacement(
        7001,
        .{
            .group_id = 7001,
            .leader_known = true,
            .readiness_from_leader = true,
            .voter_count_known = true,
            .voter_set_known = true,
            .voter_set_fingerprint = fingerprint,
            .voter_count = 2,
            .healthy_voter_reports = 2,
        },
    ));
    try std.testing.expect(!evidence.hasFullHealthyPlacement(
        7001,
        .{
            .group_id = 7001,
            .leader_known = true,
            .readiness_from_leader = true,
            .voter_count_known = true,
            .voter_set_known = true,
            .voter_set_fingerprint = table_manager.voterSetFingerprint(&.{ 1, 3 }, null),
            .voter_count = 2,
            .healthy_voter_reports = 2,
        },
    ));
}

fn monotonicMillis() u64 {
    return @intCast(@divTrunc(platform_time.monotonicNs(), std.time.ns_per_ms));
}

const TestMedianKeyLookup = struct {
    median_key: []const u8,

    fn iface(self: *@This()) MedianKeyLookup {
        return .{
            .ptr = self,
            .vtable = &.{
                .fetch_median_key = fetchMedianKey,
            },
        };
    }

    fn fetchMedianKey(ptr: *anyopaque, alloc: std.mem.Allocator, _: u64) !?[]u8 {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return try alloc.dupe(u8, self.median_key);
    }
};

fn groupStatusFresh(
    config: Reconciler.Config,
    status: MergedGroupStatus,
    now_ms: u64,
) bool {
    if (status.updated_at_millis == 0) return false;
    if (now_ms < status.updated_at_millis) {
        return status.updated_at_millis - now_ms <= config.stats_clock_skew_millis;
    }
    return now_ms - status.updated_at_millis <= config.stats_stale_after_millis;
}

test "metadata reconciler bounds future-skewed group status freshness" {
    const config: Reconciler.Config = .{
        .stats_stale_after_millis = 60_000,
        .stats_clock_skew_millis = 5_000,
    };
    try std.testing.expect(groupStatusFresh(
        config,
        .{ .group_id = 1, .updated_at_millis = 104_999 },
        100_000,
    ));
    try std.testing.expect(!groupStatusFresh(
        config,
        .{ .group_id = 1, .updated_at_millis = 105_001 },
        100_000,
    ));
    try std.testing.expect(!groupStatusFresh(
        config,
        .{ .group_id = 1, .updated_at_millis = 39_999 },
        100_000,
    ));
}

fn groupStatusReadyForAutomaticPlanning(status: MergedGroupStatus) bool {
    return status.leader_known and
        status.readiness_from_leader and
        status.voter_count_known and
        status.voter_set_known and
        !status.joint_consensus and
        !status.transition_pending and
        !status.replay_required and
        !status.replay_caught_up and
        !status.cutover_ready and
        !status.reads_ready_after_cutover and
        !status.restore_pending;
}

fn managerGroupBusy(
    manager: *table_manager.TableManager,
    first_group_id: u64,
    second_group_id: u64,
    transition_id: u64,
) bool {
    var split_it = manager.split_intents.iterator();
    while (split_it.next()) |entry| {
        if (entry.key_ptr.* == transition_id) continue;
        const intent = entry.value_ptr.*;
        if (intent.source_group_id == first_group_id or intent.source_group_id == second_group_id) return true;
        if (intent.destination_group_id == first_group_id or intent.destination_group_id == second_group_id) return true;
    }
    var merge_it = manager.merge_intents.iterator();
    while (merge_it.next()) |entry| {
        if (entry.key_ptr.* == transition_id) continue;
        const intent = entry.value_ptr.*;
        if (intent.donor_group_id == first_group_id or intent.donor_group_id == second_group_id) return true;
        if (intent.receiver_group_id == first_group_id or intent.receiver_group_id == second_group_id) return true;
    }
    return false;
}

fn deriveAutomaticTransitionId(prefix: []const u8, first: u64, second: u64, key: ?[]const u8) u64 {
    var hasher = std.hash.Wyhash.init(0x5a1d_2026_7a11);
    hasher.update(prefix);
    hasher.update(std.mem.asBytes(&first));
    hasher.update(std.mem.asBytes(&second));
    if (key) |bytes| hasher.update(bytes);
    const id = hasher.final();
    return if (id == 0) 1 else id;
}

fn deriveAutomaticSplitDestinationId(
    planning_index: *const AutomaticPlanningIndex,
    source_group_id: u64,
    split_key: []const u8,
) u64 {
    var attempt: u64 = 0;
    while (attempt < 8) : (attempt += 1) {
        var hasher = std.hash.Wyhash.init(0x5a1d_2026_d35a +% attempt);
        hasher.update(std.mem.asBytes(&source_group_id));
        hasher.update(split_key);
        const candidate = group_ids.dataGroupIdFromHash(hasher.final());
        if (candidate == 0) continue;
        if (!planning_index.groupExists(candidate)) return candidate;
    }
    return 0;
}

fn containsU64(values: []const u64, needle: u64) bool {
    for (values) |value| {
        if (value == needle) return true;
    }
    return false;
}

fn rangesAdjacent(a: table_manager.RangeRecord, b: table_manager.RangeRecord) bool {
    if (a.end_key) |a_end| {
        if (std.mem.eql(u8, a_end, b.start_key)) return true;
    }
    if (b.end_key) |b_end| {
        if (std.mem.eql(u8, b_end, a.start_key)) return true;
    }
    return false;
}

fn keyStrictlyInsideRange(key: []const u8, start_key: []const u8, end_key: ?[]const u8) bool {
    if (std.mem.order(u8, key, start_key) != .gt) return false;
    if (end_key) |end| {
        if (std.mem.order(u8, key, end) != .lt) return false;
    }
    return true;
}

fn freeSplitIntentOwned(alloc: std.mem.Allocator, intent: table_manager.SplitIntent) void {
    alloc.free(intent.split_key);
    if (intent.rollback_reason) |reason| alloc.free(reason);
}

fn freeMergeIntentOwned(alloc: std.mem.Allocator, intent: table_manager.MergeIntent) void {
    if (intent.rollback_reason) |reason| alloc.free(reason);
}

const ActiveTransitionContractIndex = struct {
    const BoundaryKey = struct {
        table_id: u64,
        key: []const u8,
    };

    const BoundaryKeyContext = struct {
        pub fn hash(_: @This(), boundary: BoundaryKey) u64 {
            var hasher = std.hash.Wyhash.init(0x7472_616e_7369_746e);
            hasher.update(std.mem.asBytes(&boundary.table_id));
            hasher.update(boundary.key);
            return hasher.final();
        }

        pub fn eql(
            _: @This(),
            lhs: BoundaryKey,
            rhs: BoundaryKey,
        ) bool {
            return lhs.table_id == rhs.table_id and
                std.mem.eql(u8, lhs.key, rhs.key);
        }
    };

    const BoundaryMap = std.HashMapUnmanaged(
        BoundaryKey,
        u64,
        BoundaryKeyContext,
        80,
    );

    const TableConfiguration = struct {
        table_id: u64,
        table_name: []const u8,
        schema_json: []const u8,
        indexes_json: []const u8,

        fn fromContract(
            contract: transition_state.TransitionTableContract,
        ) TableConfiguration {
            return .{
                .table_id = contract.table_id,
                .table_name = contract.table_name,
                .schema_json = contract.schema_json,
                .indexes_json = contract.indexes_json,
            };
        }

        fn eql(self: TableConfiguration, other: TableConfiguration) bool {
            return self.table_id == other.table_id and
                std.mem.eql(u8, self.table_name, other.table_name) and
                std.mem.eql(u8, self.schema_json, other.schema_json) and
                std.mem.eql(u8, self.indexes_json, other.indexes_json);
        }

        fn matches(
            self: TableConfiguration,
            table: table_manager.TableRecord,
        ) bool {
            return table.table_id == self.table_id and
                std.mem.eql(u8, table.name, self.table_name) and
                std.mem.eql(u8, table.schema_json, self.schema_json) and
                std.mem.eql(u8, table.indexes_json, self.indexes_json);
        }
    };

    alloc: std.mem.Allocator,
    by_table: std.AutoHashMapUnmanaged(
        u64,
        TableConfiguration,
    ) = .empty,
    table_by_group: std.AutoHashMapUnmanaged(u64, u64) = .empty,
    range_mutation_fences: std.AutoHashMapUnmanaged(u64, void) = .empty,

    fn init(
        alloc: std.mem.Allocator,
        current: CurrentMetadataState,
    ) !ActiveTransitionContractIndex {
        var self: ActiveTransitionContractIndex = .{ .alloc = alloc };
        errdefer self.deinit();
        var active_transition_count: usize = 0;
        for (current.split_transitions) |record| {
            if (!transitionPhaseTerminal(record.phase))
                active_transition_count = try std.math.add(
                    usize,
                    active_transition_count,
                    1,
                );
        }
        for (current.merge_transitions) |record| {
            if (!transitionPhaseTerminal(record.phase))
                active_transition_count = try std.math.add(
                    usize,
                    active_transition_count,
                    1,
                );
        }
        try self.by_table.ensureTotalCapacity(
            alloc,
            @intCast(active_transition_count),
        );
        for (current.split_transitions) |record| {
            if (transitionPhaseTerminal(record.phase)) continue;
            try record.table_contract.validateForSplit();
            try self.addContract(record.table_contract);
        }
        for (current.merge_transitions) |record| {
            if (transitionPhaseTerminal(record.phase)) continue;
            try record.table_contract.validateForMerge(
                record.allow_doc_identity_reassignment,
            );
            try self.addContract(record.table_contract);
        }
        var contract_it = self.by_table.iterator();
        while (contract_it.next()) |entry| {
            const current_table = findTableRecord(
                current.tables,
                entry.key_ptr.*,
            ) orelse return error.TransitionTableContractViolated;
            if (!entry.value_ptr.matches(current_table))
                return error.TransitionTableContractViolated;
        }

        const group_capacity = try std.math.mul(
            usize,
            active_transition_count,
            2,
        );
        try self.table_by_group.ensureTotalCapacity(alloc, @intCast(group_capacity));
        try self.range_mutation_fences.ensureTotalCapacity(
            alloc,
            @intCast(group_capacity),
        );
        for (current.split_transitions) |record| {
            if (transitionPhaseTerminal(record.phase)) continue;
            try self.addGroup(record.source_group_id, record.table_contract.table_id);
            try self.addGroup(record.destination_group_id, record.table_contract.table_id);
        }
        for (current.merge_transitions) |record| {
            if (transitionPhaseTerminal(record.phase)) continue;
            try self.addGroup(record.donor_group_id, record.table_contract.table_id);
            try self.addGroup(record.receiver_group_id, record.table_contract.table_id);
        }
        return self;
    }

    fn deinit(self: *ActiveTransitionContractIndex) void {
        self.by_table.deinit(self.alloc);
        self.table_by_group.deinit(self.alloc);
        self.range_mutation_fences.deinit(self.alloc);
        self.* = undefined;
    }

    fn addContract(
        self: *ActiveTransitionContractIndex,
        contract: transition_state.TransitionTableContract,
    ) !void {
        try contract.validate();
        const configuration = TableConfiguration.fromContract(contract);
        const entry = self.by_table.getOrPutAssumeCapacity(contract.table_id);
        if (entry.found_existing) {
            if (!entry.value_ptr.eql(configuration))
                return error.ConflictingTableTransitionContract;
            return;
        }
        entry.value_ptr.* = configuration;
    }

    fn addGroup(
        self: *ActiveTransitionContractIndex,
        group_id: u64,
        table_id: u64,
    ) !void {
        if (group_id == 0) return error.InvalidTransitionGroup;
        const entry = self.table_by_group.getOrPutAssumeCapacity(group_id);
        if (entry.found_existing) {
            if (entry.value_ptr.* != table_id)
                return error.ConflictingGroupTransitionContract;
        } else {
            entry.value_ptr.* = table_id;
        }
        self.range_mutation_fences.putAssumeCapacity(group_id, {});
    }

    fn fenceAdjacentRangeMutations(
        self: *ActiveTransitionContractIndex,
        ranges: []const table_manager.RangeRecord,
    ) !void {
        if (self.table_by_group.count() == 0 or ranges.len == 0) return;

        var by_start: BoundaryMap = .empty;
        defer by_start.deinit(self.alloc);
        var by_end: BoundaryMap = .empty;
        defer by_end.deinit(self.alloc);
        try by_start.ensureTotalCapacity(self.alloc, @intCast(ranges.len));
        try by_end.ensureTotalCapacity(self.alloc, @intCast(ranges.len));

        for (ranges) |range| {
            try putRangeBoundary(
                &by_start,
                self.alloc,
                .{ .table_id = range.table_id, .key = range.start_key },
                range.group_id,
            );
            if (range.end_key) |end_key| {
                try putRangeBoundary(
                    &by_end,
                    self.alloc,
                    .{ .table_id = range.table_id, .key = end_key },
                    range.group_id,
                );
            }
        }

        for (ranges) |range| {
            if (!self.table_by_group.contains(range.group_id)) continue;
            if (by_end.get(.{
                .table_id = range.table_id,
                .key = range.start_key,
            })) |predecessor_group_id| {
                try self.range_mutation_fences.put(
                    self.alloc,
                    predecessor_group_id,
                    {},
                );
            }
            if (range.end_key) |end_key| {
                if (by_start.get(.{
                    .table_id = range.table_id,
                    .key = end_key,
                })) |successor_group_id| {
                    try self.range_mutation_fences.put(
                        self.alloc,
                        successor_group_id,
                        {},
                    );
                }
            }
        }
    }

    fn putRangeBoundary(
        map: *BoundaryMap,
        alloc: std.mem.Allocator,
        boundary: BoundaryKey,
        group_id: u64,
    ) !void {
        const entry = try map.getOrPut(alloc, boundary);
        if (entry.found_existing and entry.value_ptr.* != group_id)
            return error.DuplicateRangeBoundary;
        entry.value_ptr.* = group_id;
    }

    fn get(
        self: *const ActiveTransitionContractIndex,
        table_id: u64,
    ) ?TableConfiguration {
        return self.by_table.get(table_id);
    }

    fn tableIdForGroup(
        self: *const ActiveTransitionContractIndex,
        group_id: u64,
    ) ?u64 {
        return self.table_by_group.get(group_id);
    }

    fn rangeMutationFenced(
        self: *const ActiveTransitionContractIndex,
        group_id: u64,
    ) bool {
        return self.range_mutation_fences.contains(group_id);
    }
};

fn transitionPhaseTerminal(phase: transition_state.TransitionPhase) bool {
    return phase == .finalized or phase == .rolled_back;
}

fn tableMatchesTransitionContract(
    table: table_manager.TableRecord,
    contract: transition_state.TransitionTableContract,
) bool {
    return table.table_id == contract.table_id and
        std.mem.eql(u8, table.name, contract.table_name) and
        std.mem.eql(u8, table.schema_json, contract.schema_json) and
        std.mem.eql(u8, table.indexes_json, contract.indexes_json);
}

fn hasExcludedCurrentPeer(
    group_id: u64,
    current_intents: []const raft_reconciler.PlacementIntent,
    candidate_domains: []const placement_planner.CandidateDomain,
) bool {
    for (current_intents) |intent| {
        if (intent.record.group_id != group_id) continue;
        for (candidate_domains) |candidate| {
            if (candidate.node_id != intent.record.local_node_id) continue;
            return candidate.status_tag == .excluded;
        }
    }
    return false;
}

fn cloneSplitRecord(alloc: std.mem.Allocator, record: transition_state.SplitTransitionRecord) !transition_state.SplitTransitionRecord {
    const split_key = if (record.split_key) |value|
        try alloc.dupe(u8, value)
    else
        null;
    const source_range_end = if (record.source_range_end) |value|
        alloc.dupe(u8, value) catch |err| {
            if (split_key) |owned| alloc.free(owned);
            return err;
        }
    else
        null;
    const rollback_reason = if (record.rollback_reason) |value|
        alloc.dupe(u8, value) catch |err| {
            if (source_range_end) |owned| alloc.free(owned);
            if (split_key) |owned| alloc.free(owned);
            return err;
        }
    else
        null;
    const table_contract = record.table_contract.clone(alloc) catch |err| {
        if (rollback_reason) |owned| alloc.free(owned);
        if (source_range_end) |owned| alloc.free(owned);
        if (split_key) |owned| alloc.free(owned);
        return err;
    };
    return .{
        .transition_id = record.transition_id,
        .attempt_epoch = record.attempt_epoch,
        .source_group_id = record.source_group_id,
        .destination_group_id = record.destination_group_id,
        .phase = record.phase,
        .split_key = split_key,
        .source_range_end = source_range_end,
        .rollback_reason = rollback_reason,
        .table_contract = table_contract,
    };
}

fn tableRecordsEqual(a: table_manager.TableRecord, b: table_manager.TableRecord) bool {
    return a.table_id == b.table_id and
        a.desired_replica_count == b.desired_replica_count and
        a.min_ranges == b.min_ranges and
        std.mem.eql(u8, a.description, b.description) and
        std.mem.eql(u8, a.schema_json, b.schema_json) and
        std.mem.eql(u8, a.read_schema_json, b.read_schema_json) and
        std.mem.eql(u8, a.indexes_json, b.indexes_json) and
        std.mem.eql(u8, a.replication_sources_json, b.replication_sources_json) and
        std.mem.eql(u8, a.placement_role, b.placement_role) and
        std.mem.eql(u8, a.name, b.name);
}

fn maybeFinalizeSchemaMigration(
    alloc: std.mem.Allocator,
    current: CurrentMetadataState,
    desired: *table_manager.TableRecord,
) !void {
    if (desired.read_schema_json.len == 0) return;

    const target_version = try schemaVersion(alloc, desired.schema_json);
    if (!try schemaMigrationReady(alloc, current, desired.table_id, target_version)) return;

    const read_version = try schemaVersion(alloc, desired.read_schema_json);
    if (read_version != target_version) {
        const next_indexes_json = try dropFullTextIndexForVersion(alloc, desired.indexes_json, read_version);
        alloc.free(desired.indexes_json);
        desired.indexes_json = next_indexes_json;
    }
    alloc.free(desired.read_schema_json);
    desired.read_schema_json = try alloc.dupe(u8, "");
}

fn schemaMigrationReady(
    alloc: std.mem.Allocator,
    current: CurrentMetadataState,
    table_id: u64,
    target_version: u32,
) !bool {
    var hosting_node_ids = std.ArrayListUnmanaged(u64).empty;
    defer hosting_node_ids.deinit(alloc);

    for (current.placement_intents) |intent| {
        if (!rangeBelongsToTable(current.ranges, intent.record.group_id, table_id)) continue;
        if (containsU64(hosting_node_ids.items, intent.record.local_node_id)) continue;
        try hosting_node_ids.append(alloc, intent.record.local_node_id);
    }

    if (hosting_node_ids.items.len == 0) return false;
    for (hosting_node_ids.items) |node_id| {
        if (findSchemaProgress(current.schema_progresses, table_id, node_id, target_version) == null) return false;
    }
    return true;
}

fn dropFullTextIndexForVersion(
    alloc: std.mem.Allocator,
    indexes_json: []const u8,
    version: u32,
) ![]u8 {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, indexes_json, .{});
    defer parsed.deinit();
    const object = switch (parsed.value) {
        .object => |*object| object,
        else => return error.InvalidTableIndexMetadata,
    };

    var versioned_name_buf: [64]u8 = undefined;
    const stale_name = if (version == 0)
        @import("../api/tables.zig").default_full_text_index_name
    else
        try std.fmt.bufPrint(&versioned_name_buf, "full_text_index_v{d}", .{version});
    _ = object.swapRemove(stale_name);
    return try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(parsed.value, .{})});
}

fn schemaVersion(alloc: std.mem.Allocator, schema_json: []const u8) !u32 {
    if (schema_json.len == 0) return 0;
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, schema_json, .{});
    defer parsed.deinit();
    const object = switch (parsed.value) {
        .object => |object| object,
        else => return error.InvalidTableSchema,
    };
    const version_value = object.get("version") orelse return 0;
    return switch (version_value) {
        .integer => |value| blk: {
            if (value < 0) return error.InvalidTableSchema;
            break :blk std.math.cast(u32, value) orelse return error.InvalidTableSchema;
        },
        else => return error.InvalidTableSchema,
    };
}

fn findSchemaProgress(
    records: []const table_manager.SchemaProgressRecord,
    table_id: u64,
    node_id: u64,
    schema_version: u32,
) ?table_manager.SchemaProgressRecord {
    for (records) |record| {
        if (record.table_id == table_id and record.node_id == node_id and record.schema_version == schema_version) return record;
    }
    return null;
}

fn rangeBelongsToTable(records: []const table_manager.RangeRecord, group_id: u64, table_id: u64) bool {
    for (records) |record| {
        if (record.group_id == group_id and record.table_id == table_id) return true;
    }
    return false;
}

fn rangeRecordsEqual(a: table_manager.RangeRecord, b: table_manager.RangeRecord) bool {
    return a.group_id == b.group_id and
        a.table_id == b.table_id and
        std.mem.eql(u8, a.start_key, b.start_key) and
        optionalBytesEqual(a.end_key, b.end_key) and
        std.mem.eql(u8, a.restore_backup_id, b.restore_backup_id) and
        std.mem.eql(u8, a.restore_artifact_backup_id, b.restore_artifact_backup_id) and
        std.mem.eql(u8, a.restore_location, b.restore_location) and
        std.mem.eql(u8, a.restore_snapshot_path, b.restore_snapshot_path) and
        std.mem.eql(u8, a.restore_connection, b.restore_connection) and
        a.restore_artifact_size_bytes == b.restore_artifact_size_bytes and
        std.mem.eql(u8, a.restore_artifact_sha256, b.restore_artifact_sha256) and
        std.mem.eql(
            u8,
            &a.completed_restore_fingerprint,
            &b.completed_restore_fingerprint,
        ) and
        a.split_attempt_epoch == b.split_attempt_epoch;
}

fn findTableRecord(records: []const table_manager.TableRecord, table_id: u64) ?table_manager.TableRecord {
    for (records) |record| {
        if (record.table_id == table_id) return record;
    }
    return null;
}

fn findRangeRecord(records: []const table_manager.RangeRecord, group_id: u64) ?table_manager.RangeRecord {
    for (records) |record| {
        if (record.group_id == group_id) return record;
    }
    return null;
}

fn splitAdmissionExpectedEpoch(
    current: CurrentMetadataState,
    desired_ranges: []const table_manager.RangeRecord,
    split: transition_state.SplitTransitionRecord,
) ?u64 {
    if (split.phase != .prepare or split.attempt_epoch == 0 or split.split_key == null) return null;
    const source = findRangeRecord(current.ranges, split.source_group_id) orelse return null;
    const desired_source = findRangeRecord(desired_ranges, split.source_group_id) orelse return null;
    if (source.split_attempt_epoch == std.math.maxInt(u64) or
        split.attempt_epoch != source.split_attempt_epoch + 1 or
        !optionalBytesEqual(source.end_key, split.source_range_end))
    {
        return null;
    }
    var publishable_source = desired_source;
    publishable_source.split_attempt_epoch = source.split_attempt_epoch;
    if (!rangeRecordsEqual(source, publishable_source)) return null;
    return source.split_attempt_epoch;
}

const SplitRangePublication = union(enum) {
    none,
    blocked,
    publish_epoch: u64,
};

fn splitRangePublication(
    current: CurrentMetadataState,
    desired_splits: []const transition_state.SplitTransitionRecord,
    desired_range: table_manager.RangeRecord,
) SplitRangePublication {
    for (desired_splits) |split| {
        if (split.source_group_id != desired_range.group_id or
            split.attempt_epoch != desired_range.split_attempt_epoch or
            split.phase != .prepare or
            split.attempt_epoch == 0 or
            findSplitRecord(current.split_transitions, split.transition_id) != null)
        {
            continue;
        }
        const previous_epoch = split.attempt_epoch - 1;
        const current_source = findRangeRecord(current.ranges, split.source_group_id);
        if (current_source == null or current_source.?.split_attempt_epoch == previous_epoch)
            return .{ .publish_epoch = previous_epoch };
        // A stale or consumed epoch must never fall through to the ordinary
        // range upsert path. Only the atomic admission command may advance the
        // durable source epoch for a new split.
        return .blocked;
    }
    return .none;
}

fn allocSplitProvisioningRanges(
    alloc: std.mem.Allocator,
    ranges: []const table_manager.RangeRecord,
    splits: []const transition_state.SplitTransitionRecord,
) ![]table_manager.RangeRecord {
    var out = std.ArrayListUnmanaged(table_manager.RangeRecord).empty;
    errdefer out.deinit(alloc);
    for (splits) |split| {
        if (findRangeRecord(ranges, split.destination_group_id) != null) continue;
        const source = findRangeRecord(ranges, split.source_group_id) orelse continue;
        const split_key = split.split_key orelse continue;
        try out.append(alloc, .{
            .group_id = split.destination_group_id,
            .range_id = split.destination_group_id,
            .table_id = source.table_id,
            .start_key = split_key,
            .end_key = if (split.source_range_end) |end_key| end_key else source.end_key,
            .doc_identity_shard_id = source.doc_identity_shard_id,
            .doc_identity_range_id = source.doc_identity_range_id,
        });
    }
    return try out.toOwnedSlice(alloc);
}

fn cloneMergeRecord(alloc: std.mem.Allocator, record: transition_state.MergeTransitionRecord) !transition_state.MergeTransitionRecord {
    const table_contract = try record.table_contract.clone(alloc);
    errdefer {
        var owned_contract = table_contract;
        owned_contract.deinitOwned(alloc);
    }
    return .{
        .transition_id = record.transition_id,
        .donor_group_id = record.donor_group_id,
        .receiver_group_id = record.receiver_group_id,
        .phase = record.phase,
        .rollback_reason = if (record.rollback_reason) |value| try alloc.dupe(u8, value) else null,
        .allow_doc_identity_reassignment = record.allow_doc_identity_reassignment,
        .table_contract = table_contract,
    };
}

fn splitRecordsEqual(a: transition_state.SplitTransitionRecord, b: transition_state.SplitTransitionRecord) bool {
    return a.transition_id == b.transition_id and
        a.attempt_epoch == b.attempt_epoch and
        a.source_group_id == b.source_group_id and
        a.destination_group_id == b.destination_group_id and
        a.phase == b.phase and
        optionalBytesEqual(a.split_key, b.split_key) and
        optionalBytesEqual(a.source_range_end, b.source_range_end) and
        optionalBytesEqual(a.rollback_reason, b.rollback_reason) and
        a.table_contract.eql(b.table_contract);
}

fn mergeRecordsEqual(a: transition_state.MergeTransitionRecord, b: transition_state.MergeTransitionRecord) bool {
    return a.transition_id == b.transition_id and
        a.donor_group_id == b.donor_group_id and
        a.receiver_group_id == b.receiver_group_id and
        a.phase == b.phase and
        a.allow_doc_identity_reassignment == b.allow_doc_identity_reassignment and
        optionalBytesEqual(a.rollback_reason, b.rollback_reason) and
        a.table_contract.eql(b.table_contract);
}

fn optionalBytesEqual(a: ?[]const u8, b: ?[]const u8) bool {
    if (a == null and b == null) return true;
    if (a == null or b == null) return false;
    return std.mem.eql(u8, a.?, b.?);
}

fn findSplitRecord(records: []const transition_state.SplitTransitionRecord, transition_id: u64) ?transition_state.SplitTransitionRecord {
    for (records) |record| {
        if (record.transition_id == transition_id) return record;
    }
    return null;
}

fn findMergeRecord(records: []const transition_state.MergeTransitionRecord, transition_id: u64) ?transition_state.MergeTransitionRecord {
    for (records) |record| {
        if (record.transition_id == transition_id) return record;
    }
    return null;
}

fn findSplitObservation(records: []const SplitRuntimeObservation, transition_id: u64) ?transition_state.SplitObservation {
    for (records) |record| {
        if (record.transition_id == transition_id) return record.observation;
    }
    return null;
}

fn findMergeObservation(records: []const MergeRuntimeObservation, transition_id: u64) ?transition_state.MergeObservation {
    for (records) |record| {
        if (record.transition_id == transition_id) return record.observation;
    }
    return null;
}

fn defaultSplitObservation() transition_state.SplitObservation {
    return transition_state.unpreparedSplitObservation();
}

fn defaultMergeObservation(record: transition_state.MergeTransitionRecord) transition_state.MergeObservation {
    return .{
        .donor = .{
            .phase = .prepare,
            .donor_group_id = record.donor_group_id,
            .receiver_group_id = record.receiver_group_id,
            .receiver_accepts_donor_range = false,
            .bootstrapped = false,
            .replay_required = false,
            .replay_caught_up = false,
            .cutover_ready = false,
            .receiver_ready_for_reads = false,
            .donor_delta_sequence = 0,
            .receiver_delta_sequence = 0,
        },
        .receiver = .{
            .phase = .prepare,
            .donor_group_id = record.donor_group_id,
            .receiver_group_id = record.receiver_group_id,
            .receiver_accepts_donor_range = false,
            .bootstrapped = false,
            .replay_required = false,
            .replay_caught_up = false,
            .cutover_ready = false,
            .receiver_ready_for_reads = false,
            .donor_delta_sequence = 0,
            .receiver_delta_sequence = 0,
        },
    };
}

fn transitionTableContractForTest(
    table_id: u64,
    table_name: []const u8,
    shard_id: u64,
    range_id: u64,
) transition_state.TransitionTableContract {
    return .{
        .table_id = table_id,
        .table_name = table_name,
        .schema_json = "",
        .indexes_json = "{}",
        .source_identity = .{ .shard_id = shard_id, .range_id = range_id },
        .target_identity = .{ .shard_id = shard_id, .range_id = range_id },
    };
}

test "metadata reconciler publishes table contracts before admitting transitions" {
    var manager = table_manager.TableManager.init(std.testing.allocator);
    defer manager.deinit();

    try manager.upsertTable(.{ .table_id = 10, .name = "docs" });
    try manager.upsertRange(.{
        .group_id = 101,
        .table_id = 10,
        .start_key = "doc:a",
        .end_key = "doc:m",
        .doc_identity_shard_id = 101,
        .doc_identity_range_id = 101,
    });
    try manager.upsertRange(.{
        .group_id = 102,
        .table_id = 10,
        .start_key = "doc:m",
        .end_key = "doc:z",
        .doc_identity_shard_id = 101,
        .doc_identity_range_id = 101,
    });
    try manager.requestSplit(.{
        .transition_id = 7001,
        .table_id = 10,
        .source_group_id = 101,
        .destination_group_id = 103,
        .split_key = "doc:h",
    });
    try manager.requestMerge(.{
        .transition_id = 7002,
        .table_id = 10,
        .donor_group_id = 102,
        .receiver_group_id = 101,
    });

    var reconciler = Reconciler.init(std.testing.allocator);
    var plan = try reconciler.computePlan(&manager, &.{}, &.{}, .{});
    defer plan.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 1), plan.table_upserts.len);
    try std.testing.expectEqual(@as(usize, 2), plan.range_upserts.len);
    const published_source = findRangeRecord(plan.range_upserts, 101) orelse
        return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(u64, 0), published_source.split_attempt_epoch);
    try std.testing.expectEqual(@as(usize, 0), plan.split_admissions.len);
    try std.testing.expectEqual(@as(usize, 0), plan.split_upserts.len);
    try std.testing.expectEqual(@as(usize, 0), plan.merge_upserts.len);
    try std.testing.expectEqual(@as(usize, 0), plan.split_steps.len);
    try std.testing.expectEqual(@as(usize, 0), plan.merge_steps.len);

    var admission_plan = try reconciler.computePlan(&manager, &.{}, &.{}, .{
        .tables = plan.table_upserts,
        .ranges = plan.range_upserts,
    });
    defer admission_plan.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 0), admission_plan.table_upserts.len);
    try std.testing.expectEqual(@as(usize, 0), admission_plan.range_upserts.len);
    try std.testing.expectEqual(@as(usize, 1), admission_plan.split_admissions.len);
    try std.testing.expectEqual(@as(u64, 0), admission_plan.split_admissions[0].expected_source_epoch);
    try std.testing.expectEqual(@as(usize, 0), admission_plan.split_upserts.len);
    try std.testing.expectEqual(@as(usize, 1), admission_plan.merge_upserts.len);
}

test "metadata reconciler provisions split destination without publishing overlapping range" {
    var manager = table_manager.TableManager.init(std.testing.allocator);
    defer manager.deinit();

    try manager.upsertTable(.{ .table_id = 10, .name = "docs", .desired_replica_count = 1 });
    try manager.upsertRange(.{
        .group_id = 101,
        .table_id = 10,
        .start_key = "doc:a",
        .end_key = "doc:z",
    });
    try manager.requestSplit(.{
        .transition_id = 7003,
        .table_id = 10,
        .source_group_id = 101,
        .destination_group_id = 103,
        .split_key = "doc:m",
    });

    const tables = try manager.listTables(std.testing.allocator);
    defer manager.freeTables(std.testing.allocator, tables);
    const ranges = try manager.listRanges(std.testing.allocator);
    defer manager.freeRanges(std.testing.allocator, ranges);
    const source_range = findRangeRecord(ranges, 101) orelse
        return error.TestExpectedEqual;
    var projected_source = source_range;
    projected_source.split_attempt_epoch = 0;
    const projected_ranges = [_]table_manager.RangeRecord{projected_source};
    const current_placements = [_]raft_reconciler.PlacementIntent{.{
        .record = .{ .group_id = 101, .replica_id = 1, .local_node_id = 1 },
        .peer_node_ids = &.{1},
    }};
    const candidates = [_]@import("state.zig").CandidatePlacementInfo{.{
        .node_id = 1,
        .role = "data",
        .failure_domain = "",
        .priority = 0,
        .status_tag = .preferred,
        .retain_current = true,
    }};

    var reconciler = Reconciler.init(std.testing.allocator);
    var plan = try reconciler.computePlan(&manager, &.{1}, &candidates, .{
        .tables = tables,
        .ranges = &projected_ranges,
        .placement_intents = &current_placements,
    });
    defer plan.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 0), plan.range_upserts.len);
    try std.testing.expect(findPlacementIntent(plan.placement_upserts, 103, 1) != null);
    try std.testing.expectEqual(@as(usize, 1), plan.split_admissions.len);
    try std.testing.expectEqual(@as(usize, 0), plan.split_upserts.len);
}

test "metadata reconciler never publishes a split epoch without atomic admission" {
    var manager = table_manager.TableManager.init(std.testing.allocator);
    defer manager.deinit();

    const desired_table: table_manager.TableRecord = .{
        .table_id = 10,
        .name = "docs",
        .desired_replica_count = 1,
    };
    try manager.upsertTable(desired_table);
    try manager.upsertRange(.{
        .group_id = 101,
        .range_id = 101,
        .table_id = 10,
        .start_key = "doc:a",
        .end_key = "doc:z",
        .doc_identity_shard_id = 101,
        .doc_identity_range_id = 101,
        .split_attempt_epoch = 2,
    });
    try manager.requestSplit(.{
        .transition_id = 7004,
        .table_id = 10,
        .source_group_id = 101,
        .destination_group_id = 102,
        .split_key = "doc:m",
    });

    const stale_source: table_manager.RangeRecord = .{
        .group_id = 101,
        .range_id = 101,
        .table_id = 10,
        .start_key = "doc:a",
        .end_key = "doc:z",
        .doc_identity_shard_id = 101,
        .doc_identity_range_id = 101,
        .split_attempt_epoch = 1,
    };
    var reconciler = Reconciler.init(std.testing.allocator);
    var plan = try reconciler.computePlan(&manager, &.{}, &.{}, .{
        .tables = &.{desired_table},
        .ranges = &.{stale_source},
    });
    defer plan.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 0), plan.range_upserts.len);
    try std.testing.expectEqual(@as(usize, 0), plan.split_admissions.len);
    try std.testing.expectEqual(@as(usize, 0), plan.split_upserts.len);
}

test "metadata reconciler resumes projected transition after authority handoff" {
    var manager = table_manager.TableManager.init(std.testing.allocator);
    defer manager.deinit();

    try manager.upsertTable(.{ .table_id = 10, .name = "docs" });
    try manager.upsertRange(.{
        .group_id = 101,
        .table_id = 10,
        .start_key = "doc:a",
        .end_key = "doc:m",
        .split_attempt_epoch = 1,
    });
    try manager.upsertRange(.{
        .group_id = 102,
        .table_id = 10,
        .start_key = "doc:m",
        .end_key = "doc:z",
    });
    const projected = [_]transition_state.SplitTransitionRecord{.{
        .transition_id = 7101,
        .attempt_epoch = 1,
        .source_group_id = 101,
        .destination_group_id = 103,
        .split_key = "doc:h",
        .source_range_end = "doc:m",
        .table_contract = transitionTableContractForTest(10, "docs", 101, 101),
    }};
    try manager.syncProjectedSplitTransitions(&projected);

    const desired = try manager.listDesiredSplitTransitions(std.testing.allocator);
    defer manager.freeSplitTransitions(std.testing.allocator, desired);
    const tables = try manager.listTables(std.testing.allocator);
    defer manager.freeTables(std.testing.allocator, tables);
    const ranges = try manager.listRanges(std.testing.allocator);
    defer manager.freeRanges(std.testing.allocator, ranges);

    var reconciler = Reconciler.init(std.testing.allocator);
    var plan = try reconciler.computePlan(&manager, &.{}, &.{}, .{
        .tables = tables,
        .ranges = ranges,
        .split_transitions = desired,
        .split_observations = &.{
            .{
                .transition_id = 7101,
                .observation = defaultSplitObservation(),
            },
        },
    });
    defer plan.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 0), plan.table_upserts.len);
    try std.testing.expectEqual(@as(usize, 0), plan.range_upserts.len);
    try std.testing.expectEqual(@as(usize, 0), plan.split_admissions.len);
    try std.testing.expectEqual(@as(usize, 0), plan.split_upserts.len);
    try std.testing.expectEqual(@as(usize, 0), plan.split_removals.len);
    try std.testing.expectEqual(@as(usize, 1), plan.split_steps.len);
    try std.testing.expectEqual(transition_controller.SplitExecutionStateTag.awaiting_source_start, plan.split_steps[0].execution.tag);
    try std.testing.expect(plan.split_steps[0].execution.action == .prepare_split_source);
}

test "metadata reconciler never removes an unterminated durable transition" {
    var manager = table_manager.TableManager.init(std.testing.allocator);
    defer manager.deinit();
    try manager.upsertTable(.{ .table_id = 10, .name = "docs" });
    try manager.upsertRange(.{
        .group_id = 101,
        .table_id = 10,
        .start_key = "doc:a",
        .end_key = "doc:m",
        .split_attempt_epoch = 1,
    });

    const tables = try manager.listTables(std.testing.allocator);
    defer manager.freeTables(std.testing.allocator, tables);
    const ranges = try manager.listRanges(std.testing.allocator);
    defer manager.freeRanges(std.testing.allocator, ranges);
    const durable = [_]transition_state.SplitTransitionRecord{.{
        .transition_id = 7102,
        .attempt_epoch = 1,
        .source_group_id = 101,
        .destination_group_id = 103,
        .split_key = "doc:h",
        .source_range_end = "doc:m",
        .table_contract = transitionTableContractForTest(10, "docs", 101, 101),
    }};

    var reconciler = Reconciler.init(std.testing.allocator);
    defer reconciler.deinit();
    var plan = try reconciler.computePlan(&manager, &.{}, &.{}, .{
        .tables = tables,
        .ranges = ranges,
        .split_transitions = &durable,
        .split_observations = &.{.{
            .transition_id = 7102,
            .observation = defaultSplitObservation(),
        }},
    });
    defer plan.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 0), plan.split_removals.len);
    try std.testing.expectEqual(@as(usize, 0), plan.split_admissions.len);
    try std.testing.expectEqual(@as(usize, 1), plan.split_steps.len);
    try std.testing.expectEqual(@as(u64, 1), plan.split_steps[0].record.attempt_epoch);

    var finalized_observation = defaultSplitObservation();
    finalized_observation.status.phase = .finalized;
    var terminal_plan = try reconciler.computePlan(&manager, &.{}, &.{}, .{
        .tables = tables,
        .ranges = ranges,
        .split_transitions = &durable,
        .split_observations = &.{.{
            .transition_id = 7102,
            .observation = finalized_observation,
        }},
    });
    defer terminal_plan.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 0), terminal_plan.split_removals.len);
    try std.testing.expectEqual(@as(usize, 1), terminal_plan.split_upserts.len);
    try std.testing.expectEqual(transition_state.TransitionPhase.finalized, terminal_plan.split_upserts[0].phase);
}

test "metadata reconciler preserves dropped table topology until transitions terminate" {
    var manager = table_manager.TableManager.init(std.testing.allocator);
    defer manager.deinit();

    const tables = [_]table_manager.TableRecord{.{
        .table_id = 10,
        .name = "docs",
    }};
    const ranges = [_]table_manager.RangeRecord{
        .{
            .group_id = 101,
            .table_id = 10,
            .start_key = "doc:a",
            .end_key = "doc:m",
        },
        .{
            .group_id = 102,
            .table_id = 10,
            .start_key = "doc:m",
            .end_key = "doc:z",
        },
    };
    const placements = [_]raft_reconciler.PlacementIntent{
        .{
            .record = .{ .group_id = 101, .replica_id = 1, .local_node_id = 1 },
            .peer_node_ids = &.{1},
        },
        .{
            .record = .{ .group_id = 102, .replica_id = 1, .local_node_id = 1 },
            .peer_node_ids = &.{1},
        },
        .{
            .record = .{ .group_id = 103, .replica_id = 1, .local_node_id = 1 },
            .peer_node_ids = &.{1},
        },
    };
    const transitions = [_]transition_state.SplitTransitionRecord{.{
        .transition_id = 7104,
        .attempt_epoch = 1,
        .source_group_id = 101,
        .destination_group_id = 103,
        .split_key = "doc:h",
        .source_range_end = "doc:m",
        .table_contract = transitionTableContractForTest(10, "docs", 101, 101),
    }};

    var reconciler = Reconciler.init(std.testing.allocator);
    defer reconciler.deinit();
    var plan = try reconciler.computePlan(&manager, &.{}, &.{}, .{
        .tables = &tables,
        .ranges = &ranges,
        .placement_intents = &placements,
        .split_transitions = &transitions,
    });
    defer plan.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 0), plan.table_removals.len);
    try std.testing.expectEqual(@as(usize, 0), plan.range_removals.len);
    try std.testing.expectEqual(@as(usize, 0), plan.placement_removals.len);
    try std.testing.expectEqual(@as(usize, 0), plan.placement_upserts.len);
}

test "metadata reconciler fences transition groups without serializing unrelated ranges" {
    var manager = table_manager.TableManager.init(std.testing.allocator);
    defer manager.deinit();
    try manager.upsertTable(.{ .table_id = 10, .name = "docs" });
    try manager.upsertRange(.{
        .group_id = 101,
        .table_id = 10,
        .start_key = "doc:a",
        .end_key = "doc:g",
        .split_attempt_epoch = 2,
    });
    try manager.upsertRange(.{
        .group_id = 102,
        .table_id = 10,
        .start_key = "doc:g",
        .end_key = "doc:m",
        .split_attempt_epoch = 1,
    });
    try manager.upsertRange(.{
        .group_id = 103,
        .table_id = 10,
        .start_key = "doc:m",
        .end_key = "doc:u",
    });
    try manager.upsertRange(.{
        .group_id = 104,
        .table_id = 10,
        .start_key = "doc:u",
        .end_key = null,
    });

    const tables = [_]table_manager.TableRecord{.{
        .table_id = 10,
        .name = "docs",
    }};
    const ranges = [_]table_manager.RangeRecord{
        .{
            .group_id = 101,
            .table_id = 10,
            .start_key = "doc:a",
            .end_key = "doc:g",
            .split_attempt_epoch = 1,
        },
        .{
            .group_id = 102,
            .table_id = 10,
            .start_key = "doc:g",
            .end_key = "doc:m",
        },
        .{
            .group_id = 103,
            .table_id = 10,
            .start_key = "doc:m",
            .end_key = "doc:t",
        },
        .{
            .group_id = 104,
            .table_id = 10,
            .start_key = "doc:t",
            .end_key = null,
        },
    };
    const transitions = [_]transition_state.SplitTransitionRecord{.{
        .transition_id = 7108,
        .attempt_epoch = 1,
        .source_group_id = 101,
        .destination_group_id = 105,
        .split_key = "doc:d",
        .source_range_end = "doc:g",
        .table_contract = transitionTableContractForTest(10, "docs", 101, 101),
    }};

    var reconciler = Reconciler.init(std.testing.allocator);
    defer reconciler.deinit();
    var plan = try reconciler.computePlan(&manager, &.{}, &.{}, .{
        .tables = &tables,
        .ranges = &ranges,
        .split_transitions = &transitions,
    });
    defer plan.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 2), plan.range_upserts.len);
    try std.testing.expectEqual(@as(usize, 0), plan.range_removals.len);
    try std.testing.expect(findRangeRecord(plan.range_upserts, 101) == null);
    try std.testing.expect(findRangeRecord(plan.range_upserts, 102) == null);
    try std.testing.expect(findRangeRecord(plan.range_upserts, 103) != null);
    try std.testing.expect(findRangeRecord(plan.range_upserts, 104) != null);
}

test "metadata reconciler serializes structural table mutation behind transitions" {
    var manager = table_manager.TableManager.init(std.testing.allocator);
    defer manager.deinit();
    try manager.upsertTable(.{
        .table_id = 10,
        .name = "docs",
        .description = "new description",
        .schema_json = "{\"version\":2}",
    });
    try manager.upsertRange(.{
        .group_id = 101,
        .table_id = 10,
        .start_key = "doc:a",
        .end_key = "doc:z",
    });

    const current_tables = [_]table_manager.TableRecord{.{
        .table_id = 10,
        .name = "docs",
        .description = "old description",
    }};
    const current_ranges = [_]table_manager.RangeRecord{.{
        .group_id = 101,
        .table_id = 10,
        .start_key = "doc:a",
        .end_key = "doc:z",
    }};
    const transitions = [_]transition_state.SplitTransitionRecord{.{
        .transition_id = 7105,
        .attempt_epoch = 1,
        .source_group_id = 101,
        .destination_group_id = 103,
        .split_key = "doc:m",
        .source_range_end = "doc:z",
        .table_contract = transitionTableContractForTest(10, "docs", 101, 101),
    }};

    var reconciler = Reconciler.init(std.testing.allocator);
    defer reconciler.deinit();
    var structural_plan = try reconciler.computePlan(&manager, &.{}, &.{}, .{
        .tables = &current_tables,
        .ranges = &current_ranges,
        .split_transitions = &transitions,
    });
    defer structural_plan.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 0), structural_plan.table_upserts.len);

    try manager.upsertTable(.{
        .table_id = 10,
        .name = "docs",
        .description = "new description",
    });
    var descriptive_plan = try reconciler.computePlan(&manager, &.{}, &.{}, .{
        .tables = &current_tables,
        .ranges = &current_ranges,
        .split_transitions = &transitions,
    });
    defer descriptive_plan.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 1), descriptive_plan.table_upserts.len);
    try std.testing.expectEqualStrings(
        "new description",
        descriptive_plan.table_upserts[0].description,
    );
}

test "metadata reconciler rejects conflicting active table contracts" {
    const contracts = [_]transition_state.SplitTransitionRecord{
        .{
            .transition_id = 7106,
            .attempt_epoch = 1,
            .source_group_id = 101,
            .destination_group_id = 103,
            .table_contract = transitionTableContractForTest(10, "docs", 101, 101),
        },
        .{
            .transition_id = 7107,
            .attempt_epoch = 1,
            .source_group_id = 102,
            .destination_group_id = 104,
            .table_contract = .{
                .table_id = 10,
                .table_name = "docs",
                .schema_json = "{\"version\":2}",
                .indexes_json = "{}",
                .source_identity = .{ .shard_id = 101, .range_id = 101 },
                .target_identity = .{ .shard_id = 101, .range_id = 101 },
            },
        },
    };
    try std.testing.expectError(
        error.ConflictingTableTransitionContract,
        ActiveTransitionContractIndex.init(std.testing.allocator, .{
            .split_transitions = &contracts,
        }),
    );
}

test "metadata reconciler allows concurrent range contracts for one table" {
    const tables = [_]table_manager.TableRecord{.{
        .table_id = 10,
        .name = "docs",
    }};
    const transitions = [_]transition_state.SplitTransitionRecord{
        .{
            .transition_id = 7110,
            .attempt_epoch = 1,
            .source_group_id = 101,
            .destination_group_id = 103,
            .table_contract = transitionTableContractForTest(
                10,
                "docs",
                101,
                1001,
            ),
        },
        .{
            .transition_id = 7111,
            .attempt_epoch = 1,
            .source_group_id = 102,
            .destination_group_id = 104,
            .table_contract = transitionTableContractForTest(
                10,
                "docs",
                102,
                1002,
            ),
        },
    };
    var index = try ActiveTransitionContractIndex.init(
        std.testing.allocator,
        .{
            .tables = &tables,
            .split_transitions = &transitions,
        },
    );
    defer index.deinit();

    try std.testing.expectEqual(@as(usize, 1), index.by_table.count());
    try std.testing.expectEqual(@as(?u64, 10), index.tableIdForGroup(101));
    try std.testing.expectEqual(@as(?u64, 10), index.tableIdForGroup(104));
}

test "metadata reconciler rejects catalog drift from active table contract" {
    const tables = [_]table_manager.TableRecord{.{
        .table_id = 10,
        .name = "docs",
        .schema_json = "{\"version\":2}",
    }};
    const transitions = [_]transition_state.SplitTransitionRecord{.{
        .transition_id = 7109,
        .attempt_epoch = 1,
        .source_group_id = 101,
        .destination_group_id = 103,
        .table_contract = transitionTableContractForTest(10, "docs", 101, 101),
    }};
    try std.testing.expectError(
        error.TransitionTableContractViolated,
        ActiveTransitionContractIndex.init(std.testing.allocator, .{
            .tables = &tables,
            .split_transitions = &transitions,
        }),
    );
}

test "metadata reconciler preserves admitted split identity" {
    var manager = table_manager.TableManager.init(std.testing.allocator);
    defer manager.deinit();
    try manager.upsertTable(.{ .table_id = 10, .name = "docs" });
    try manager.upsertRange(.{
        .group_id = 101,
        .table_id = 10,
        .start_key = "doc:a",
        .end_key = "doc:m",
        .split_attempt_epoch = 2,
    });
    const desired = [_]transition_state.SplitTransitionRecord{.{
        .transition_id = 7103,
        .attempt_epoch = 2,
        .source_group_id = 101,
        .destination_group_id = 103,
        .split_key = "doc:h",
        .source_range_end = "doc:m",
        .table_contract = transitionTableContractForTest(10, "docs", 101, 101),
    }};
    try manager.syncProjectedSplitTransitions(&desired);

    const tables = try manager.listTables(std.testing.allocator);
    defer manager.freeTables(std.testing.allocator, tables);
    const ranges = try manager.listRanges(std.testing.allocator);
    defer manager.freeRanges(std.testing.allocator, ranges);
    const admitted = [_]transition_state.SplitTransitionRecord{.{
        .transition_id = 7103,
        .attempt_epoch = 1,
        .source_group_id = 101,
        .destination_group_id = 103,
        .split_key = "doc:h",
        .source_range_end = "doc:m",
        .table_contract = transitionTableContractForTest(10, "docs", 101, 101),
    }};

    var reconciler = Reconciler.init(std.testing.allocator);
    var plan = try reconciler.computePlan(&manager, &.{}, &.{}, .{
        .tables = tables,
        .ranges = ranges,
        .split_transitions = &admitted,
        .split_observations = &.{.{
            .transition_id = 7103,
            .observation = defaultSplitObservation(),
        }},
    });
    defer plan.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 0), plan.split_upserts.len);
    try std.testing.expectEqual(@as(usize, 0), plan.split_removals.len);
    try std.testing.expectEqual(@as(usize, 1), plan.split_steps.len);
    try std.testing.expectEqual(@as(u64, 1), plan.split_steps[0].record.attempt_epoch);
}

test "metadata reconciler rolls back existing split with stale doc identity namespace" {
    var manager = table_manager.TableManager.init(std.testing.allocator);
    defer manager.deinit();

    try manager.upsertTable(.{ .table_id = 11, .name = "docs" });
    try manager.upsertRange(.{
        .group_id = 1101,
        .range_id = 9001,
        .table_id = 11,
        .start_key = "doc:a",
        .end_key = "doc:z",
    });
    try manager.requestSplit(.{
        .transition_id = 7201,
        .table_id = 11,
        .source_group_id = 1101,
        .destination_group_id = 1102,
        .split_key = "doc:m",
    });

    const desired = try manager.listDesiredSplitTransitions(std.testing.allocator);
    defer manager.freeSplitTransitions(std.testing.allocator, desired);
    const tables = try manager.listTables(std.testing.allocator);
    defer manager.freeTables(std.testing.allocator, tables);
    const ranges = try manager.listRanges(std.testing.allocator);
    defer manager.freeRanges(std.testing.allocator, ranges);

    const statuses = [_]MergedGroupStatus{.{
        .group_id = 1101,
        .doc_count = 10,
        .disk_bytes = 10,
        .empty = false,
        .updated_at_millis = monotonicMillis(),
        .doc_identity = .{
            .namespace_table_id = 11,
            .namespace_shard_id = 1101,
            .namespace_range_id = 9001,
            .next_ordinal = 11,
            .allocated_ordinals = 10,
            .live_ordinals = 10,
            .rebuild_required = true,
        },
    }};

    var reconciler = Reconciler.init(std.testing.allocator);
    var plan = try reconciler.computePlan(&manager, &.{}, &.{}, .{
        .tables = tables,
        .ranges = ranges,
        .merged_group_statuses = &statuses,
        .split_transitions = desired,
        .split_observations = &.{.{
            .transition_id = 7201,
            .observation = defaultSplitObservation(),
        }},
    });
    defer plan.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 1), plan.split_upserts.len);
    try std.testing.expectEqual(@as(usize, 0), plan.split_steps.len);
    try std.testing.expectEqual(@as(u64, 7201), plan.split_upserts[0].transition_id);
    try std.testing.expectEqualStrings(doc_identity_transition_rollback_reason, plan.split_upserts[0].rollback_reason.?);
}

test "metadata reconciler does not upsert desired split with stale doc identity namespace" {
    var manager = table_manager.TableManager.init(std.testing.allocator);
    defer manager.deinit();

    try manager.upsertTable(.{ .table_id = 12, .name = "docs" });
    try manager.upsertRange(.{
        .group_id = 1201,
        .range_id = 9101,
        .table_id = 12,
        .start_key = "doc:a",
        .end_key = "doc:z",
    });
    try manager.requestSplit(.{
        .transition_id = 7301,
        .table_id = 12,
        .source_group_id = 1201,
        .destination_group_id = 1202,
        .split_key = "doc:m",
    });

    const tables = try manager.listTables(std.testing.allocator);
    defer manager.freeTables(std.testing.allocator, tables);
    const ranges = try manager.listRanges(std.testing.allocator);
    defer manager.freeRanges(std.testing.allocator, ranges);

    const statuses = [_]MergedGroupStatus{.{
        .group_id = 1201,
        .doc_count = 10,
        .disk_bytes = 10,
        .empty = false,
        .updated_at_millis = monotonicMillis(),
        .doc_identity = .{
            .namespace_table_id = 12,
            .namespace_shard_id = 1201,
            .namespace_range_id = 9101,
            .next_ordinal = 11,
            .allocated_ordinals = 10,
            .live_ordinals = 10,
            .rebuild_required = true,
        },
    }};

    var reconciler = Reconciler.init(std.testing.allocator);
    var plan = try reconciler.computePlan(&manager, &.{}, &.{}, .{
        .tables = tables,
        .ranges = ranges,
        .merged_group_statuses = &statuses,
    });
    defer plan.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 0), plan.split_upserts.len);
    try std.testing.expectEqual(@as(usize, 0), plan.split_steps.len);

    const missing_statuses = [_]MergedGroupStatus{.{
        .group_id = 1299,
        .doc_count = 10,
        .disk_bytes = 10,
        .empty = false,
        .updated_at_millis = monotonicMillis(),
        .doc_identity = .{
            .namespace_table_id = 12,
            .namespace_shard_id = 1299,
            .namespace_range_id = 9199,
            .next_ordinal = 11,
            .allocated_ordinals = 10,
            .live_ordinals = 10,
        },
    }};
    var missing_status_plan = try reconciler.computePlan(&manager, &.{}, &.{}, .{
        .tables = tables,
        .ranges = ranges,
        .merged_group_statuses = &missing_statuses,
    });
    defer missing_status_plan.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 0), missing_status_plan.split_upserts.len);
    try std.testing.expectEqual(@as(usize, 0), missing_status_plan.split_steps.len);
}

test "metadata reconciler distinguishes repair from rebalance placement changes" {
    var manager = table_manager.TableManager.init(std.testing.allocator);
    defer manager.deinit();

    try manager.upsertTable(.{ .table_id = 20, .name = "docs", .desired_replica_count = 2 });
    try manager.upsertRange(.{
        .group_id = 2001,
        .table_id = 20,
        .start_key = "doc:a",
        .end_key = "doc:z",
    });

    const current_rebalance = [_]raft_reconciler.PlacementIntent{
        .{ .record = .{ .group_id = 2001, .replica_id = 1, .local_node_id = 1, .bootstrap_mode = .persisted }, .peer_node_ids = &.{ 1, 2 } },
        .{ .record = .{ .group_id = 2001, .replica_id = 2, .local_node_id = 2, .bootstrap_mode = .persisted }, .peer_node_ids = &.{ 1, 2 } },
    };
    const candidates_rebalance = [_]@import("state.zig").CandidatePlacementInfo{
        .{ .node_id = 1, .role = "data", .failure_domain = "rack-a", .priority = 2, .status_tag = .overloaded, .available_bytes = 950, .lease_pressure = 95, .read_load = 180, .write_load = 120, .retain_current = false },
        .{ .node_id = 2, .role = "data", .failure_domain = "rack-b", .priority = 0, .status_tag = .preferred, .available_bytes = 850, .lease_pressure = 10, .read_load = 15, .write_load = 10, .retain_current = true },
        .{ .node_id = 3, .role = "data", .failure_domain = "rack-c", .priority = 0, .status_tag = .preferred, .available_bytes = 800, .lease_pressure = 12, .read_load = 18, .write_load = 10, .retain_current = true },
    };
    const rebalance_fingerprint = table_manager.voterSetFingerprint(&.{ 1, 2 }, null);
    const rebalance_status_time = monotonicMillis();
    const rebalance_stores = [_]table_manager.StoreRecord{
        .{
            .store_id = 1,
            .node_id = 1,
            .role = "data",
            .health_class = "healthy",
            .live = true,
            .group_statuses = @constCast((&[_]table_manager.GroupStatusReport{.{
                .group_id = 2001,
                .local_leader = true,
                .local_voter = true,
                .voter_count = 2,
                .voter_set_known = true,
                .voter_set_fingerprint = rebalance_fingerprint,
                .updated_at_millis = rebalance_status_time,
            }})[0..]),
        },
        .{
            .store_id = 2,
            .node_id = 2,
            .role = "data",
            .health_class = "healthy",
            .live = true,
            .group_statuses = @constCast((&[_]table_manager.GroupStatusReport{.{
                .group_id = 2001,
                .local_voter = true,
                .voter_count = 2,
                .voter_set_known = true,
                .voter_set_fingerprint = rebalance_fingerprint,
                .updated_at_millis = rebalance_status_time,
            }})[0..]),
        },
    };

    var reconciler = Reconciler.init(std.testing.allocator);
    var rebalance_plan = try reconciler.computePlan(&manager, &.{ 1, 2, 3 }, &candidates_rebalance, .{
        .placement_intents = &current_rebalance,
        .stores = &rebalance_stores,
    });
    defer rebalance_plan.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 0), rebalance_plan.repair_placement_groups);
    try std.testing.expectEqual(@as(usize, 1), rebalance_plan.rebalance_placement_groups);

    const current_repair = [_]raft_reconciler.PlacementIntent{
        .{ .record = .{ .group_id = 2001, .replica_id = 1, .local_node_id = 1, .bootstrap_mode = .persisted }, .peer_node_ids = &.{ 1, 2 } },
    };
    const candidates_repair = [_]@import("state.zig").CandidatePlacementInfo{
        .{ .node_id = 1, .role = "data", .failure_domain = "rack-a", .priority = 255, .status_tag = .excluded, .available_bytes = 0, .lease_pressure = 0, .read_load = 0, .write_load = 0, .retain_current = false },
        .{ .node_id = 2, .role = "data", .failure_domain = "rack-b", .priority = 0, .status_tag = .preferred, .available_bytes = 850, .lease_pressure = 10, .read_load = 15, .write_load = 10, .retain_current = true },
        .{ .node_id = 3, .role = "data", .failure_domain = "rack-c", .priority = 0, .status_tag = .preferred, .available_bytes = 800, .lease_pressure = 12, .read_load = 18, .write_load = 10, .retain_current = true },
    };

    var repair_plan = try reconciler.computePlan(&manager, &.{ 1, 2, 3 }, &candidates_repair, .{
        .placement_intents = &current_repair,
    });
    defer repair_plan.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 1), repair_plan.repair_placement_groups);
    try std.testing.expectEqual(@as(usize, 0), repair_plan.rebalance_placement_groups);
}

test "metadata reconciler forced reallocation can place replicas on newly added nodes" {
    var manager = table_manager.TableManager.init(std.testing.allocator);
    defer manager.deinit();

    try manager.upsertTable(.{ .table_id = 21, .name = "docs", .desired_replica_count = 3 });
    try manager.upsertRange(.{
        .group_id = 2101,
        .table_id = 21,
        .start_key = "doc:a",
        .end_key = "doc:m",
    });
    try manager.upsertRange(.{
        .group_id = 2102,
        .table_id = 21,
        .start_key = "doc:m",
        .end_key = "doc:z",
    });

    const current = [_]raft_reconciler.PlacementIntent{
        .{ .record = .{ .group_id = 2101, .replica_id = 1, .local_node_id = 1, .bootstrap_mode = .persisted }, .peer_node_ids = &.{ 1, 2, 3 } },
        .{ .record = .{ .group_id = 2101, .replica_id = 2, .local_node_id = 2, .bootstrap_mode = .persisted }, .peer_node_ids = &.{ 1, 2, 3 } },
        .{ .record = .{ .group_id = 2101, .replica_id = 3, .local_node_id = 3, .bootstrap_mode = .persisted }, .peer_node_ids = &.{ 1, 2, 3 } },
        .{ .record = .{ .group_id = 2102, .replica_id = 1, .local_node_id = 1, .bootstrap_mode = .persisted }, .peer_node_ids = &.{ 1, 2, 3 } },
        .{ .record = .{ .group_id = 2102, .replica_id = 2, .local_node_id = 2, .bootstrap_mode = .persisted }, .peer_node_ids = &.{ 1, 2, 3 } },
        .{ .record = .{ .group_id = 2102, .replica_id = 3, .local_node_id = 3, .bootstrap_mode = .persisted }, .peer_node_ids = &.{ 1, 2, 3 } },
    };
    const candidates = [_]@import("state.zig").CandidatePlacementInfo{
        .{ .node_id = 1, .role = "data", .failure_domain = "", .priority = 0, .status_tag = .preferred, .retain_current = true },
        .{ .node_id = 2, .role = "data", .failure_domain = "", .priority = 0, .status_tag = .preferred, .retain_current = true },
        .{ .node_id = 3, .role = "data", .failure_domain = "", .priority = 0, .status_tag = .preferred, .retain_current = true },
        .{ .node_id = 4, .role = "data", .failure_domain = "", .priority = 0, .status_tag = .preferred, .retain_current = true },
    };
    const stable_fingerprint = table_manager.voterSetFingerprint(&.{ 1, 2, 3 }, null);
    const stable_status_time = monotonicMillis();
    const stable_stores = [_]table_manager.StoreRecord{
        .{
            .store_id = 1,
            .node_id = 1,
            .role = "data",
            .health_class = "healthy",
            .live = true,
            .group_statuses = @constCast((&[_]table_manager.GroupStatusReport{
                .{ .group_id = 2101, .local_leader = true, .local_voter = true, .voter_count = 3, .voter_set_known = true, .voter_set_fingerprint = stable_fingerprint, .updated_at_millis = stable_status_time },
                .{ .group_id = 2102, .local_leader = true, .local_voter = true, .voter_count = 3, .voter_set_known = true, .voter_set_fingerprint = stable_fingerprint, .updated_at_millis = stable_status_time },
            })[0..]),
        },
        .{
            .store_id = 2,
            .node_id = 2,
            .role = "data",
            .health_class = "healthy",
            .live = true,
            .group_statuses = @constCast((&[_]table_manager.GroupStatusReport{
                .{ .group_id = 2101, .local_voter = true, .voter_count = 3, .voter_set_known = true, .voter_set_fingerprint = stable_fingerprint, .updated_at_millis = stable_status_time },
                .{ .group_id = 2102, .local_voter = true, .voter_count = 3, .voter_set_known = true, .voter_set_fingerprint = stable_fingerprint, .updated_at_millis = stable_status_time },
            })[0..]),
        },
        .{
            .store_id = 3,
            .node_id = 3,
            .role = "data",
            .health_class = "healthy",
            .live = true,
            .group_statuses = @constCast((&[_]table_manager.GroupStatusReport{
                .{ .group_id = 2101, .local_voter = true, .voter_count = 3, .voter_set_known = true, .voter_set_fingerprint = stable_fingerprint, .updated_at_millis = stable_status_time },
                .{ .group_id = 2102, .local_voter = true, .voter_count = 3, .voter_set_known = true, .voter_set_fingerprint = stable_fingerprint, .updated_at_millis = stable_status_time },
            })[0..]),
        },
    };

    var reconciler = Reconciler.init(std.testing.allocator);
    var stable_plan = try reconciler.computePlan(&manager, &.{ 1, 2, 3, 4 }, &candidates, .{
        .placement_intents = &current,
        .stores = &stable_stores,
    });
    defer stable_plan.deinit(std.testing.allocator);
    try std.testing.expect(findPlacementIntent(stable_plan.placement_upserts, 2101, 4) == null);
    try std.testing.expect(findPlacementIntent(stable_plan.placement_upserts, 2102, 4) == null);

    var unconverged_forced_plan = try reconciler.computePlan(&manager, &.{ 1, 2, 3, 4 }, &candidates, .{
        .placement_intents = &current,
        .reallocation_request = .{ .request_id = 1, .requested_at_ms = 1 },
    });
    defer unconverged_forced_plan.deinit(std.testing.allocator);
    try std.testing.expect(findPlacementIntent(unconverged_forced_plan.placement_upserts, 2101, 4) == null);
    try std.testing.expect(findPlacementIntent(unconverged_forced_plan.placement_upserts, 2102, 4) == null);
    try std.testing.expect(unconverged_forced_plan.clear_reallocation_request == null);

    var forced_plan = try reconciler.computePlan(&manager, &.{ 1, 2, 3, 4 }, &candidates, .{
        .placement_intents = &current,
        .stores = &stable_stores,
        .reallocation_request = .{ .request_id = 1, .requested_at_ms = 1 },
    });
    defer forced_plan.deinit(std.testing.allocator);
    try std.testing.expect(forced_plan.forced_reallocation);
    try std.testing.expectEqual(@as(?u128, 1), forced_plan.clear_reallocation_request);
    try std.testing.expect(
        findPlacementIntent(forced_plan.placement_upserts, 2101, 4) != null or
            findPlacementIntent(forced_plan.placement_upserts, 2102, 4) != null,
    );
}

test "metadata reconciler serializes forced placement movement behind split provisioning" {
    var manager = table_manager.TableManager.init(std.testing.allocator);
    defer manager.deinit();

    try manager.upsertTable(.{ .table_id = 210, .name = "docs", .desired_replica_count = 3 });
    try manager.upsertRange(.{ .group_id = 2101, .table_id = 210, .start_key = "", .end_key = null });
    try manager.requestSplit(.{
        .transition_id = 21001,
        .table_id = 210,
        .source_group_id = 2101,
        .destination_group_id = 2102,
        .split_key = "doc:m",
    });

    const current = [_]raft_reconciler.PlacementIntent{
        .{ .record = .{ .group_id = 2101, .replica_id = 1, .local_node_id = 1, .bootstrap_mode = .persisted }, .store_id = 1, .peer_node_ids = &.{ 1, 2, 3 } },
        .{ .record = .{ .group_id = 2101, .replica_id = 2, .local_node_id = 2, .bootstrap_mode = .persisted }, .store_id = 2, .peer_node_ids = &.{ 1, 2, 3 } },
        .{ .record = .{ .group_id = 2101, .replica_id = 3, .local_node_id = 3, .bootstrap_mode = .persisted }, .store_id = 3, .peer_node_ids = &.{ 1, 2, 3 } },
    };
    const ranges = [_]table_manager.RangeRecord{.{
        .group_id = 2101,
        .range_id = 2101,
        .table_id = 210,
        .start_key = "",
        .end_key = null,
    }};
    const candidates = [_]@import("state.zig").CandidatePlacementInfo{
        .{ .node_id = 1, .store_id = 1, .role = "data", .failure_domain = "rack-a", .retain_current = true },
        .{ .node_id = 2, .store_id = 2, .role = "data", .failure_domain = "rack-b", .retain_current = true },
        .{ .node_id = 3, .store_id = 3, .role = "data", .failure_domain = "rack-c", .retain_current = true },
        .{ .node_id = 4, .store_id = 4, .role = "data", .failure_domain = "rack-d", .retain_current = true },
    };

    var reconciler = Reconciler.init(std.testing.allocator);
    var plan = try reconciler.computePlan(&manager, &.{ 1, 2, 3, 4 }, &candidates, .{
        .placement_intents = &current,
        .ranges = &ranges,
        .reallocation_request = .{ .request_id = 1, .requested_at_ms = 1 },
    });
    defer plan.deinit(std.testing.allocator);

    try std.testing.expect(plan.forced_reallocation);
    try std.testing.expect(plan.clear_reallocation_request == null);
    try std.testing.expectEqual(@as(usize, 0), plan.placement_removals.len);
    try std.testing.expect(findPlacementIntent(plan.placement_upserts, 2101, 4) == null);
    var destination_placements: usize = 0;
    for (plan.placement_upserts) |intent| {
        if (intent.record.group_id == 2102) destination_placements += 1;
    }
    try std.testing.expectEqual(@as(usize, 3), destination_placements);
}

test "metadata reconciler keeps in-flight group placement sticky across repeated reallocation" {
    var manager = table_manager.TableManager.init(std.testing.allocator);
    defer manager.deinit();

    try manager.upsertTable(.{ .table_id = 211, .name = "docs", .desired_replica_count = 1 });
    try manager.upsertRange(.{ .group_id = 2111, .table_id = 211, .start_key = "", .end_key = null });

    const candidates = [_]@import("state.zig").CandidatePlacementInfo{
        .{ .node_id = 101, .store_id = 101, .role = "data", .failure_domain = "rack-a", .retain_current = true },
        .{ .node_id = 102, .store_id = 102, .role = "data", .failure_domain = "rack-b", .retain_current = true },
        .{ .node_id = 103, .store_id = 103, .role = "data", .failure_domain = "rack-c", .retain_current = true },
    };

    var reconciler = Reconciler.init(std.testing.allocator);
    var initial_plan = try reconciler.computePlan(&manager, &.{ 101, 102, 103 }, &candidates, .{
        .reallocation_request = .{ .request_id = 1, .requested_at_ms = 1 },
    });
    defer initial_plan.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 1), initial_plan.placement_upserts.len);
    const initial = initial_plan.placement_upserts[0];
    try std.testing.expectEqual(raft_reconciler.PlacementServingState.serving, initial.serving_state);

    var unready = initial;
    unready.serving_state = .bootstrapping;
    unready.relocation_doc_count_watermark = 1;
    const current = [_]raft_reconciler.PlacementIntent{unready};
    const merged_statuses = [_]MergedGroupStatus{.{
        .group_id = 2111,
        .doc_count = 1,
        .disk_bytes = 1024,
        .empty = false,
    }};
    var repeated_plan = try reconciler.computePlan(&manager, &.{ 101, 102, 103 }, &candidates, .{
        .placement_intents = &current,
        .merged_group_statuses = &merged_statuses,
        .reallocation_request = .{ .request_id = 1, .requested_at_ms = 1 },
    });
    defer repeated_plan.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 1), repeated_plan.placement_upserts.len);
    try std.testing.expectEqual(initial.record.local_node_id, repeated_plan.placement_upserts[0].record.local_node_id);
    try std.testing.expectEqual(raft_reconciler.PlacementServingState.bootstrapping, repeated_plan.placement_upserts[0].serving_state);
    try std.testing.expectEqual(@as(usize, 0), repeated_plan.placement_removals.len);
    try std.testing.expect(repeated_plan.clear_reallocation_request == null);
}

test "metadata reconciler keeps relocation source serving while target bootstraps" {
    var manager = table_manager.TableManager.init(std.testing.allocator);
    defer manager.deinit();

    try manager.upsertTable(.{ .table_id = 220, .name = "docs", .desired_replica_count = 1 });
    try manager.upsertRange(.{ .group_id = 2201, .table_id = 220, .start_key = "", .end_key = null });

    const current = [_]raft_reconciler.PlacementIntent{.{
        .record = .{
            .group_id = 2201,
            .replica_id = 1,
            .local_node_id = 1,
        },
        .store_id = 11,
        .peer_node_ids = &.{1},
        .serving_state = .serving,
    }};
    const stores = [_]table_manager.StoreRecord{.{
        .store_id = 11,
        .node_id = 1,
        .role = "data",
        .health_class = "healthy",
        .live = true,
        .group_statuses = @constCast((&[_]table_manager.GroupStatusReport{.{
            .group_id = 2201,
            .doc_count = 5,
            .disk_bytes = 2048,
            .empty = false,
            .local_leader = true,
            .local_voter = true,
            .voter_count = 1,
        }})[0..]),
    }};
    const candidates = [_]@import("state.zig").CandidatePlacementInfo{.{
        .node_id = 2,
        .store_id = 22,
        .role = "data",
        .failure_domain = "rack-b",
        .retain_current = false,
    }};

    var reconciler = Reconciler.init(std.testing.allocator);
    var plan = try reconciler.computePlan(&manager, &.{2}, &candidates, .{
        .placement_intents = &current,
        .stores = &stores,
        .reallocation_request = .{ .request_id = 1, .requested_at_ms = 1 },
    });
    defer plan.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 0), plan.placement_removals.len);
    const target = findPlacementIntent(plan.placement_upserts, 2201, 2) orelse return error.MissingRelocationTarget;
    try std.testing.expectEqual(raft_reconciler.PlacementServingState.bootstrapping, target.serving_state);
    try std.testing.expectEqual(@as(u64, 1), target.relocation_source_node_id);
    try std.testing.expectEqual(@as(u64, 5), target.relocation_doc_count_watermark);
    const source = findPlacementIntent(plan.placement_upserts, 2201, 1) orelse return error.MissingDrainingSource;
    try std.testing.expectEqual(raft_reconciler.PlacementServingState.draining, source.serving_state);
    try std.testing.expectEqualSlices(u64, &.{ 1, 2 }, source.peer_node_ids);
}

test "metadata reconciler converges overlapping relocation intents on one desired voter set" {
    var manager = table_manager.TableManager.init(std.testing.allocator);
    defer manager.deinit();

    try manager.upsertTable(.{ .table_id = 2202, .name = "docs", .desired_replica_count = 3 });
    try manager.upsertRange(.{ .group_id = 2202, .table_id = 2202, .start_key = "", .end_key = null });

    const current = [_]raft_reconciler.PlacementIntent{
        .{ .record = .{ .group_id = 2202, .replica_id = 1, .local_node_id = 1 }, .store_id = 1, .peer_node_ids = &.{ 1, 2, 3 }, .serving_state = .draining },
        .{ .record = .{ .group_id = 2202, .replica_id = 2, .local_node_id = 2 }, .store_id = 2, .peer_node_ids = &.{ 2, 3, 4 }, .serving_state = .draining },
        .{ .record = .{ .group_id = 2202, .replica_id = 3, .local_node_id = 3 }, .store_id = 3, .peer_node_ids = &.{ 1, 2, 3 }, .serving_state = .serving },
        .{ .record = .{ .group_id = 2202, .replica_id = 4, .local_node_id = 4 }, .store_id = 4, .peer_node_ids = &.{ 2, 3, 4 }, .serving_state = .serving },
    };
    const stores = [_]table_manager.StoreRecord{.{
        .store_id = 1,
        .node_id = 1,
        .role = "data",
        .health_class = "healthy",
        .live = true,
        .group_statuses = @constCast((&[_]table_manager.GroupStatusReport{.{
            .group_id = 2202,
            .doc_count = 5,
            .disk_bytes = 2048,
            .empty = false,
            .local_voter = true,
            .voter_count = 4,
        }})[0..]),
    }};
    const candidates = [_]@import("state.zig").CandidatePlacementInfo{
        .{ .node_id = 3, .store_id = 3, .role = "data", .failure_domain = "rack-c", .retain_current = true },
        .{ .node_id = 4, .store_id = 4, .role = "data", .failure_domain = "rack-d", .retain_current = true },
        .{ .node_id = 5, .store_id = 5, .role = "data", .failure_domain = "rack-e", .retain_current = true },
    };

    var reconciler = Reconciler.init(std.testing.allocator);
    var plan = try reconciler.computePlan(&manager, &.{ 3, 4, 5 }, &candidates, .{
        .placement_intents = &current,
        .stores = &stores,
        .reallocation_request = .{ .request_id = 1, .requested_at_ms = 1 },
    });
    defer plan.deinit(std.testing.allocator);

    const expected = table_manager.voterSetFingerprint(&.{ 1, 2, 3, 4, 5 }, null);
    for (plan.placement_upserts) |intent| {
        if (intent.record.group_id != 2202) continue;
        const actual = table_manager.voterSetFingerprint(intent.peer_node_ids, null);
        try std.testing.expectEqualSlices(u8, &expected, &actual);
    }
    try std.testing.expectEqual(@as(usize, 0), plan.placement_removals.len);

    const contracted_current = [_]raft_reconciler.PlacementIntent{
        .{ .record = .{ .group_id = 2202, .replica_id = 3, .local_node_id = 3 }, .store_id = 3, .peer_node_ids = &.{ 1, 2, 3, 4, 5 }, .serving_state = .serving },
        .{ .record = .{ .group_id = 2202, .replica_id = 4, .local_node_id = 4 }, .store_id = 4, .peer_node_ids = &.{ 1, 2, 3, 4, 5 }, .serving_state = .serving },
        .{ .record = .{ .group_id = 2202, .replica_id = 5, .local_node_id = 5 }, .store_id = 5, .peer_node_ids = &.{ 1, 2, 3, 4, 5 }, .serving_state = .serving },
    };
    var contract_plan = try reconciler.computePlan(&manager, &.{ 3, 4, 5 }, &candidates, .{
        .placement_intents = &contracted_current,
        .reallocation_request = .{ .request_id = 1, .requested_at_ms = 1 },
    });
    defer contract_plan.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 3), contract_plan.placement_upserts.len);
    for (contract_plan.placement_upserts) |intent| {
        try std.testing.expectEqualSlices(u64, &.{ 3, 4, 5 }, intent.peer_node_ids);
    }
}

test "metadata reconciler removes relocation source only after final voter retirement is observed" {
    var manager = table_manager.TableManager.init(std.testing.allocator);
    defer manager.deinit();

    try manager.upsertTable(.{ .table_id = 221, .name = "docs", .desired_replica_count = 1 });
    try manager.upsertRange(.{ .group_id = 2211, .table_id = 221, .start_key = "", .end_key = null });

    const current = [_]raft_reconciler.PlacementIntent{
        .{
            .record = .{
                .group_id = 2211,
                .replica_id = 1,
                .local_node_id = 1,
            },
            .store_id = 11,
            .peer_node_ids = &.{2},
            .serving_state = .retiring,
            .relocation_doc_count_watermark = 5,
            .relocation_disk_bytes_watermark = 2048,
        },
        .{
            .record = .{
                .group_id = 2211,
                .replica_id = 1,
                .local_node_id = 2,
                .bootstrap_mode = .persisted,
            },
            .store_id = 22,
            .peer_node_ids = &.{2},
            .serving_state = .serving,
            .relocation_source_node_id = 1,
            .relocation_source_store_id = 11,
            .relocation_doc_count_watermark = 5,
            .relocation_disk_bytes_watermark = 2048,
        },
    };
    const stores = [_]table_manager.StoreRecord{
        .{
            .store_id = 11,
            .node_id = 1,
            .role = "data",
            .health_class = "healthy",
            .live = true,
            .group_statuses = @constCast((&[_]table_manager.GroupStatusReport{.{
                .group_id = 2211,
                .doc_count = 5,
                .disk_bytes = 2048,
                .empty = false,
                .local_voter = false,
                .voter_count = 1,
                .voter_set_known = true,
                .voter_set_fingerprint = table_manager.voterSetFingerprint(&.{2}, null),
            }})[0..]),
        },
        .{
            .store_id = 22,
            .node_id = 2,
            .role = "data",
            .health_class = "healthy",
            .live = true,
            .group_statuses = @constCast((&[_]table_manager.GroupStatusReport{.{
                .group_id = 2211,
                .doc_count = 5,
                .disk_bytes = 2048,
                .empty = false,
                .local_leader = true,
                .local_voter = true,
                .voter_count = 1,
                .voter_set_known = true,
                .voter_set_fingerprint = table_manager.voterSetFingerprint(&.{2}, null),
                .replay_required = false,
                .replay_caught_up = true,
                .cutover_ready = true,
                .reads_ready_after_cutover = true,
            }})[0..]),
        },
    };
    const merged_statuses = [_]MergedGroupStatus{.{
        .group_id = 2211,
        .doc_count = 0,
        .disk_bytes = 2190,
        .empty = false,
        .voter_count_known = true,
        .voter_count = 1,
        .healthy_voter_reports = 2,
    }};
    const candidates = [_]@import("state.zig").CandidatePlacementInfo{.{
        .node_id = 2,
        .store_id = 22,
        .role = "data",
        .failure_domain = "rack-b",
        .retain_current = false,
    }};

    var reconciler = Reconciler.init(std.testing.allocator);
    var plan = try reconciler.computePlan(&manager, &.{2}, &candidates, .{
        .placement_intents = &current,
        .stores = &stores,
        .merged_group_statuses = &merged_statuses,
        .reallocation_request = .{ .request_id = 1, .requested_at_ms = 1 },
    });
    defer plan.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 1), plan.placement_removals.len);
    try std.testing.expectEqual(@as(u64, 2211), plan.placement_removals[0].group_id);
    try std.testing.expectEqual(@as(u64, 1), plan.placement_removals[0].local_node_id);
    try std.testing.expect(findPlacementIntent(plan.placement_upserts, 2211, 1) == null);
}

test "metadata reconciler does not gate empty relocation on storage overhead bytes" {
    var manager = table_manager.TableManager.init(std.testing.allocator);
    defer manager.deinit();

    try manager.upsertTable(.{ .table_id = 223, .name = "docs", .desired_replica_count = 1 });
    try manager.upsertRange(.{ .group_id = 2231, .table_id = 223, .start_key = "", .end_key = null });

    const current = [_]raft_reconciler.PlacementIntent{
        .{
            .record = .{
                .group_id = 2231,
                .replica_id = 1,
                .local_node_id = 1,
            },
            .store_id = 11,
            .peer_node_ids = &.{ 1, 2 },
            .serving_state = .draining,
            .relocation_disk_bytes_watermark = 2190,
        },
        .{
            .record = .{
                .group_id = 2231,
                .replica_id = 1,
                .local_node_id = 2,
                .bootstrap_mode = .persisted,
            },
            .store_id = 22,
            .peer_node_ids = &.{ 1, 2 },
            .serving_state = .replaying,
            .relocation_generation = 1,
            .relocation_source_node_id = 1,
            .relocation_source_store_id = 11,
            .relocation_disk_bytes_watermark = 2190,
        },
    };
    const empty_source_status = table_manager.GroupStatusReport{
        .group_id = 2231,
        .doc_count = 0,
        .disk_bytes = 0,
        .empty = true,
        .local_voter = true,
        .voter_count = 1,
        .voter_set_known = true,
        .voter_set_fingerprint = table_manager.voterSetFingerprint(&.{1}, null),
    };
    var empty_target_status = empty_source_status;
    empty_target_status.relocation_generation = 1;
    empty_target_status.voter_set_fingerprint = table_manager.voterSetFingerprint(&.{2}, null);
    const stores = [_]table_manager.StoreRecord{
        .{
            .store_id = 11,
            .node_id = 1,
            .role = "data",
            .health_class = "healthy",
            .live = true,
            .group_statuses = @constCast((&[_]table_manager.GroupStatusReport{empty_source_status})[0..]),
            .runtime_statuses = @constCast((&[_]table_manager.RuntimeGroupStatusReport{.{
                .table_id = 223,
                .group_id = 2231,
                .store_id = 11,
                .node_id = 1,
                .doc_count = 0,
                .disk_bytes = 2190,
            }})[0..]),
        },
        .{
            .store_id = 22,
            .node_id = 2,
            .role = "data",
            .health_class = "healthy",
            .live = true,
            .group_statuses = @constCast((&[_]table_manager.GroupStatusReport{empty_target_status})[0..]),
            .runtime_statuses = @constCast((&[_]table_manager.RuntimeGroupStatusReport{.{
                .table_id = 223,
                .group_id = 2231,
                .store_id = 22,
                .node_id = 2,
                .doc_count = 0,
                .disk_bytes = 2190,
            }})[0..]),
        },
    };
    const candidates = [_]@import("state.zig").CandidatePlacementInfo{.{
        .node_id = 2,
        .store_id = 22,
        .role = "data",
        .failure_domain = "rack-b",
        .retain_current = true,
    }};

    var reconciler = Reconciler.init(std.testing.allocator);
    var plan = try reconciler.computePlan(&manager, &.{2}, &candidates, .{
        .placement_intents = &current,
        .stores = &stores,
        .reallocation_request = .{ .request_id = 1, .requested_at_ms = 1 },
    });
    defer plan.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 0), plan.placement_removals.len);
    const target = findPlacementIntent(plan.placement_upserts, 2231, 2) orelse return error.MissingRelocationTarget;
    try std.testing.expectEqual(raft_reconciler.PlacementServingState.cutover_ready, target.serving_state);
    try std.testing.expectEqual(@as(u64, 0), target.relocation_doc_count_watermark);
    try std.testing.expectEqual(@as(u64, 0), target.relocation_disk_bytes_watermark);
    const source = findPlacementIntent(plan.placement_upserts, 2231, 1) orelse return error.MissingDrainingSource;
    try std.testing.expectEqual(raft_reconciler.PlacementServingState.draining, source.serving_state);
    try std.testing.expectEqual(@as(u64, 0), source.relocation_doc_count_watermark);
    try std.testing.expectEqual(@as(u64, 0), source.relocation_disk_bytes_watermark);
}

test "metadata reconciler preserves empty-group source voters until replacement membership is stable" {
    const current = [_]raft_reconciler.PlacementIntent{
        .{
            .record = .{ .group_id = 2232, .replica_id = 1, .local_node_id = 1 },
            .store_id = 11,
            .peer_node_ids = &.{ 1, 2, 3 },
            .serving_state = .draining,
        },
        .{
            .record = .{ .group_id = 2232, .replica_id = 2, .local_node_id = 2 },
            .store_id = 22,
            .peer_node_ids = &.{ 1, 2, 3 },
            .serving_state = .serving,
        },
        .{
            .record = .{ .group_id = 2232, .replica_id = 3, .local_node_id = 3 },
            .store_id = 33,
            .peer_node_ids = &.{ 1, 2, 3 },
            .serving_state = .serving,
            .relocation_generation = 1,
            .relocation_source_node_id = 1,
            .relocation_source_store_id = 11,
        },
    };
    const desired = [_]raft_reconciler.PlacementIntent{
        current[1],
        current[2],
    };

    const state: CurrentMetadataState = .{ .placement_intents = &current };
    var evidence = try StoreEvidenceIndex.init(std.testing.allocator, state);
    defer evidence.deinit();
    var membership_index = try MembershipTransitionIndex.init(
        std.testing.allocator,
        state,
        &desired,
        &evidence,
    );
    defer membership_index.deinit();
    try std.testing.expect(!membership_index.placementSafeToRemove(current[0]));
}

test "metadata reconciler latches final membership across planner churn" {
    const current = [_]raft_reconciler.PlacementIntent{
        .{
            .record = .{ .group_id = 2233, .replica_id = 1, .local_node_id = 101 },
            .store_id = 101,
            .peer_node_ids = &.{ 102, 103 },
            .serving_state = .retiring,
        },
        .{
            .record = .{ .group_id = 2233, .replica_id = 2, .local_node_id = 102 },
            .store_id = 102,
            .peer_node_ids = &.{ 102, 103 },
            .serving_state = .serving,
        },
        .{
            .record = .{ .group_id = 2233, .replica_id = 3, .local_node_id = 103 },
            .store_id = 103,
            .peer_node_ids = &.{ 102, 103 },
            .serving_state = .serving,
        },
    };
    const replanned = [_]raft_reconciler.PlacementIntent{
        .{
            .record = .{ .group_id = 2233, .replica_id = 2, .local_node_id = 102 },
            .store_id = 102,
            .peer_node_ids = &.{ 102, 104 },
            .serving_state = .serving,
        },
        .{
            .record = .{ .group_id = 2233, .replica_id = 4, .local_node_id = 104 },
            .store_id = 104,
            .peer_node_ids = &.{ 102, 104 },
            .serving_state = .planned,
        },
    };

    const state: CurrentMetadataState = .{ .placement_intents = &current };
    var evidence = try StoreEvidenceIndex.init(std.testing.allocator, state);
    defer evidence.deinit();
    var membership_index = try MembershipTransitionIndex.init(
        std.testing.allocator,
        state,
        &replanned,
        &evidence,
    );
    defer membership_index.deinit();

    try std.testing.expect(membership_index.contracting(2233));
    try std.testing.expectEqualSlices(u64, &.{ 102, 103 }, membership_index.latchedFinalPeers(2233).?);
    try std.testing.expect(membership_index.deferDesiredPlacement(2233, 104));
    try std.testing.expect(membership_index.preserveCurrentPlacement(2233, 103));
    try std.testing.expect(!membership_index.preserveCurrentPlacement(2233, 101));
}

test "metadata reconciler retires after exact final leader proof despite stale follower" {
    const current = [_]raft_reconciler.PlacementIntent{
        .{
            .record = .{ .group_id = 2234, .replica_id = 1, .local_node_id = 101 },
            .store_id = 101,
            .peer_node_ids = &.{ 102, 103 },
            .serving_state = .retiring,
        },
        .{
            .record = .{ .group_id = 2234, .replica_id = 2, .local_node_id = 102 },
            .store_id = 102,
            .peer_node_ids = &.{ 102, 103 },
            .serving_state = .serving,
        },
        .{
            .record = .{ .group_id = 2234, .replica_id = 3, .local_node_id = 103 },
            .store_id = 103,
            .peer_node_ids = &.{ 102, 103 },
            .serving_state = .serving,
        },
    };
    const desired = [_]raft_reconciler.PlacementIntent{ current[1], current[2] };
    const final_fingerprint = table_manager.voterSetFingerprint(&.{ 102, 103 }, null);
    const stale_fingerprint = table_manager.voterSetFingerprint(&.{ 101, 102, 103 }, null);
    const stores = [_]table_manager.StoreRecord{
        .{
            .store_id = 101,
            .node_id = 101,
            .role = "data",
            .health_class = "healthy",
            .live = true,
            .group_statuses = @constCast((&[_]table_manager.GroupStatusReport{.{
                .group_id = 2234,
                .local_voter = true,
                .voter_count = 3,
                .voter_set_known = true,
                .voter_set_fingerprint = stale_fingerprint,
            }})[0..]),
        },
        .{
            .store_id = 102,
            .node_id = 102,
            .role = "data",
            .health_class = "healthy",
            .live = true,
            .group_statuses = @constCast((&[_]table_manager.GroupStatusReport{.{
                .group_id = 2234,
                .local_leader = true,
                .local_voter = true,
                .voter_count = 2,
                .voter_set_known = true,
                .voter_set_fingerprint = final_fingerprint,
            }})[0..]),
        },
        .{
            .store_id = 103,
            .node_id = 103,
            .role = "data",
            .health_class = "healthy",
            .live = true,
            .group_statuses = @constCast((&[_]table_manager.GroupStatusReport{.{
                .group_id = 2234,
                .local_voter = true,
                .voter_count = 3,
                .voter_set_known = true,
                .voter_set_fingerprint = stale_fingerprint,
            }})[0..]),
        },
    };

    const state: CurrentMetadataState = .{ .placement_intents = &current, .stores = &stores };
    var evidence = try StoreEvidenceIndex.init(std.testing.allocator, state);
    defer evidence.deinit();
    var membership_index = try MembershipTransitionIndex.init(
        std.testing.allocator,
        state,
        &desired,
        &evidence,
    );
    defer membership_index.deinit();
    try std.testing.expect(membership_index.placementSafeToRemove(current[0]));
}

test "metadata reconciler requires logical document watermark and tolerates compacted target" {
    const intent: raft_reconciler.PlacementIntent = .{
        .record = .{ .group_id = 2241, .replica_id = 3, .local_node_id = 104 },
        .store_id = 104,
        .peer_node_ids = &.{ 101, 102, 104 },
        .serving_state = .replaying,
        .relocation_source_node_id = 105,
        .relocation_source_store_id = 105,
        .relocation_doc_count_watermark = 12,
        .relocation_disk_bytes_watermark = 10_052,
    };
    const stores = [_]table_manager.StoreRecord{.{
        .store_id = 104,
        .node_id = 104,
        .role = "data",
        .health_class = "healthy",
        .live = true,
        .group_statuses = @constCast((&[_]table_manager.GroupStatusReport{.{
            .group_id = 2241,
            .doc_count = 11,
            .disk_bytes = 8_192,
            .empty = false,
            .local_voter = true,
            .voter_count = 3,
            .voter_set_known = true,
            .voter_set_fingerprint = table_manager.voterSetFingerprint(&.{ 101, 102, 104 }, null),
        }})[0..]),
    }};

    var evidence = try StoreEvidenceIndex.init(std.testing.allocator, .{ .placement_intents = &.{intent}, .stores = &stores });
    try std.testing.expect(!relocationTargetCutoverReady(&evidence, intent));
    evidence.deinit();

    var caught_up_status = stores[0].group_statuses[0];
    caught_up_status.doc_count = intent.relocation_doc_count_watermark;
    var caught_up_store = stores[0];
    caught_up_store.group_statuses = @constCast((&[_]table_manager.GroupStatusReport{caught_up_status})[0..]);
    const caught_up_stores = [_]table_manager.StoreRecord{caught_up_store};
    evidence = try StoreEvidenceIndex.init(std.testing.allocator, .{ .placement_intents = &.{intent}, .stores = &caught_up_stores });
    defer evidence.deinit();
    try std.testing.expect(relocationTargetCutoverReady(&evidence, intent));
}

test "metadata reconciler accepts relocation evidence only from the placement store" {
    const intent: raft_reconciler.PlacementIntent = .{
        .record = .{ .group_id = 2245, .replica_id = 3, .local_node_id = 104 },
        .store_id = 1040,
        .peer_node_ids = &.{ 101, 102, 104 },
        .serving_state = .replaying,
        .relocation_generation = 17,
        .relocation_doc_count_watermark = 12,
        .relocation_target_sequence = 91,
    };
    const fingerprint = table_manager.voterSetFingerprint(&.{ 101, 102, 104 }, null);
    var owner_status: table_manager.GroupStatusReport = .{
        .group_id = 2245,
        .relocation_generation = 17,
        .raft_applied_index = 90,
        .doc_count = 11,
        .local_voter = true,
        .voter_count = 3,
        .voter_set_known = true,
        .voter_set_fingerprint = fingerprint,
    };
    const wrong_store_status: table_manager.GroupStatusReport = .{
        .group_id = 2245,
        .relocation_generation = 17,
        .raft_applied_index = 91,
        .doc_count = 12,
        .local_leader = true,
        .local_voter = true,
        .voter_count = 3,
        .voter_set_known = true,
        .voter_set_fingerprint = fingerprint,
    };
    var stores = [_]table_manager.StoreRecord{
        .{
            .store_id = 1040,
            .node_id = 104,
            .group_statuses = @constCast((&[_]table_manager.GroupStatusReport{owner_status})[0..]),
        },
        .{
            .store_id = 1041,
            .node_id = 104,
            .group_statuses = @constCast((&[_]table_manager.GroupStatusReport{wrong_store_status})[0..]),
        },
    };

    var evidence = try StoreEvidenceIndex.init(std.testing.allocator, .{ .placement_intents = &.{intent}, .stores = &stores });
    try std.testing.expect(!relocationTargetCutoverReady(&evidence, intent));
    evidence.deinit();
    owner_status.raft_applied_index = intent.relocation_target_sequence;
    owner_status.doc_count = intent.relocation_doc_count_watermark;
    stores[0].group_statuses = @constCast((&[_]table_manager.GroupStatusReport{owner_status})[0..]);
    evidence = try StoreEvidenceIndex.init(std.testing.allocator, .{ .placement_intents = &.{intent}, .stores = &stores });
    defer evidence.deinit();
    try std.testing.expect(relocationTargetCutoverReady(&evidence, intent));
}

test "metadata reconciler accepts retirement proof only from each placement store" {
    const current = [_]raft_reconciler.PlacementIntent{
        .{
            .record = .{ .group_id = 2246, .replica_id = 1, .local_node_id = 101 },
            .store_id = 1010,
            .peer_node_ids = &.{102},
            .serving_state = .retiring,
        },
        .{
            .record = .{ .group_id = 2246, .replica_id = 2, .local_node_id = 102 },
            .store_id = 1020,
            .peer_node_ids = &.{102},
            .serving_state = .serving,
        },
    };
    const desired = [_]raft_reconciler.PlacementIntent{current[1]};
    const final_fingerprint = table_manager.voterSetFingerprint(&.{102}, null);
    var owner_status: table_manager.GroupStatusReport = .{
        .group_id = 2246,
        .local_voter = true,
        .voter_count = 1,
        .voter_set_known = true,
        .voter_set_fingerprint = final_fingerprint,
    };
    const wrong_store_status: table_manager.GroupStatusReport = .{
        .group_id = 2246,
        .local_leader = true,
        .local_voter = true,
        .voter_count = 1,
        .voter_set_known = true,
        .voter_set_fingerprint = final_fingerprint,
    };
    var stores = [_]table_manager.StoreRecord{
        .{ .store_id = 1010, .node_id = 101, .group_statuses = &.{} },
        .{
            .store_id = 1020,
            .node_id = 102,
            .group_statuses = @constCast((&[_]table_manager.GroupStatusReport{owner_status})[0..]),
        },
        .{
            .store_id = 1021,
            .node_id = 102,
            .group_statuses = @constCast((&[_]table_manager.GroupStatusReport{wrong_store_status})[0..]),
        },
    };

    {
        const state: CurrentMetadataState = .{ .placement_intents = &current, .stores = &stores };
        var evidence = try StoreEvidenceIndex.init(std.testing.allocator, state);
        defer evidence.deinit();
        var index = try MembershipTransitionIndex.init(
            std.testing.allocator,
            state,
            &desired,
            &evidence,
        );
        defer index.deinit();
        try std.testing.expect(!index.placementSafeToRemove(current[0]));
    }

    owner_status.local_leader = true;
    stores[1].group_statuses = @constCast((&[_]table_manager.GroupStatusReport{owner_status})[0..]);
    const state: CurrentMetadataState = .{ .placement_intents = &current, .stores = &stores };
    var evidence = try StoreEvidenceIndex.init(std.testing.allocator, state);
    defer evidence.deinit();
    var index = try MembershipTransitionIndex.init(
        std.testing.allocator,
        state,
        &desired,
        &evidence,
    );
    defer index.deinit();
    try std.testing.expect(index.placementSafeToRemove(current[0]));
}

test "metadata reconciler requires matching relocation generation and raft apply boundary" {
    const intent: raft_reconciler.PlacementIntent = .{
        .record = .{ .group_id = 2243, .replica_id = 3, .local_node_id = 104 },
        .store_id = 104,
        .peer_node_ids = &.{ 101, 102, 104 },
        .serving_state = .replaying,
        .relocation_generation = 17,
        .relocation_doc_count_watermark = 12,
        .relocation_disk_bytes_watermark = 10_052,
        .relocation_target_sequence = 91,
    };
    var status: table_manager.GroupStatusReport = .{
        .group_id = 2243,
        .relocation_generation = 16,
        .raft_applied_index = 91,
        .doc_count = 12,
        .disk_bytes = 10_052,
        .empty = false,
        .local_voter = true,
        .voter_count = 3,
        .voter_set_known = true,
        .voter_set_fingerprint = table_manager.voterSetFingerprint(&.{ 101, 102, 104 }, null),
    };
    var store: table_manager.StoreRecord = .{
        .store_id = 104,
        .node_id = 104,
        .role = "data",
        .health_class = "healthy",
        .live = true,
        .group_statuses = @constCast((&[_]table_manager.GroupStatusReport{status})[0..]),
    };

    var state: CurrentMetadataState = .{ .placement_intents = &.{intent}, .stores = (&[_]table_manager.StoreRecord{store})[0..] };
    var evidence = try StoreEvidenceIndex.init(std.testing.allocator, state);
    try std.testing.expect(!relocationTargetCutoverReady(&evidence, intent));
    evidence.deinit();

    status.relocation_generation = intent.relocation_generation;
    status.raft_applied_index = intent.relocation_target_sequence - 1;
    store.group_statuses = @constCast((&[_]table_manager.GroupStatusReport{status})[0..]);
    state.stores = (&[_]table_manager.StoreRecord{store})[0..];
    evidence = try StoreEvidenceIndex.init(std.testing.allocator, state);
    try std.testing.expect(!relocationTargetCutoverReady(&evidence, intent));
    evidence.deinit();

    status.raft_applied_index = intent.relocation_target_sequence;
    store.group_statuses = @constCast((&[_]table_manager.GroupStatusReport{status})[0..]);
    state.stores = (&[_]table_manager.StoreRecord{store})[0..];
    evidence = try StoreEvidenceIndex.init(std.testing.allocator, state);
    defer evidence.deinit();
    try std.testing.expect(relocationTargetCutoverReady(&evidence, intent));
}

test "metadata reconciler promotes a hydrated relocation learner before serving" {
    const intent: raft_reconciler.PlacementIntent = .{
        .record = .{ .group_id = 2244, .replica_id = 3, .local_node_id = 104 },
        .store_id = 104,
        .peer_node_ids = &.{ 101, 102, 104 },
        .serving_state = .replaying,
        .relocation_generation = 17,
        .relocation_source_node_id = 105,
        .relocation_source_store_id = 105,
        .relocation_doc_count_watermark = 12,
        .relocation_disk_bytes_watermark = 10_052,
        .relocation_target_sequence = 91,
    };
    const store: table_manager.StoreRecord = .{
        .store_id = 104,
        .node_id = 104,
        .role = "data",
        .health_class = "healthy",
        .live = true,
        .group_statuses = @constCast((&[_]table_manager.GroupStatusReport{.{
            .group_id = 2244,
            .relocation_generation = 17,
            .raft_applied_index = 91,
            .doc_count = 12,
            .disk_bytes = 10_052,
            .empty = false,
            .local_voter = false,
        }})[0..]),
    };
    const current: CurrentMetadataState = .{ .placement_intents = &.{intent}, .stores = (&[_]table_manager.StoreRecord{store})[0..] };
    var evidence = try StoreEvidenceIndex.init(std.testing.allocator, current);
    defer evidence.deinit();

    try std.testing.expect(relocationTargetDataReady(&evidence, intent));
    try std.testing.expect(!relocationTargetCutoverReady(&evidence, intent));
    try std.testing.expectEqual(
        raft_reconciler.PlacementServingState.cutover_ready,
        relocationTargetServingState(&evidence, intent),
    );
    var empty_evidence = try StoreEvidenceIndex.init(std.testing.allocator, .{});
    defer empty_evidence.deinit();
    try std.testing.expectEqual(
        raft_reconciler.PlacementServingState.bootstrapping,
        relocationTargetServingState(&empty_evidence, intent),
    );
}

test "metadata reconciler never regresses a promoted relocation voter to learner" {
    const current_intent: raft_reconciler.PlacementIntent = .{
        .record = .{ .group_id = 2245, .replica_id = 3, .local_node_id = 104 },
        .store_id = 104,
        .peer_node_ids = &.{ 101, 102, 104 },
        .serving_state = .cutover_ready,
        .relocation_generation = 17,
        .relocation_source_node_id = 105,
        .relocation_source_store_id = 105,
        .relocation_doc_count_watermark = 12,
        .relocation_target_sequence = 91,
    };
    var desired = current_intent;
    desired.serving_state = .serving;

    const current: CurrentMetadataState = .{ .placement_intents = (&[_]raft_reconciler.PlacementIntent{current_intent})[0..] };
    var evidence = try StoreEvidenceIndex.init(std.testing.allocator, current);
    defer evidence.deinit();
    const effective = effectivePlacementIntent(
        current,
        &.{},
        &.{},
        &evidence,
        desired,
    );
    try std.testing.expectEqual(
        raft_reconciler.PlacementServingState.cutover_ready,
        effective.serving_state,
    );
}

test "metadata reconciler rejects stable raft membership with the wrong voter set" {
    const intent: raft_reconciler.PlacementIntent = .{
        .record = .{ .group_id = 2242, .replica_id = 3, .local_node_id = 104 },
        .peer_node_ids = &.{ 101, 102, 104 },
        .serving_state = .replaying,
        .relocation_doc_count_watermark = 12,
        .relocation_disk_bytes_watermark = 10_052,
    };
    const stores = [_]table_manager.StoreRecord{.{
        .store_id = 104,
        .node_id = 104,
        .role = "data",
        .health_class = "healthy",
        .live = true,
        .group_statuses = @constCast((&[_]table_manager.GroupStatusReport{.{
            .group_id = 2242,
            .doc_count = 12,
            .disk_bytes = 8_192,
            .empty = false,
            .local_voter = true,
            .voter_count = 3,
            .voter_set_known = true,
            .voter_set_fingerprint = table_manager.voterSetFingerprint(&.{ 101, 103, 104 }, null),
        }})[0..]),
    }};

    var evidence = try StoreEvidenceIndex.init(std.testing.allocator, .{ .placement_intents = &.{intent}, .stores = &stores });
    defer evidence.deinit();
    try std.testing.expect(!relocationTargetCutoverReady(&evidence, intent));
}

test "metadata reconciler retains a compact shrink source until a survivor leads" {
    var manager = table_manager.TableManager.init(std.testing.allocator);
    defer manager.deinit();

    try manager.upsertTable(.{ .table_id = 225, .name = "docs", .desired_replica_count = 2 });
    try manager.upsertRange(.{ .group_id = 2251, .table_id = 225, .start_key = "", .end_key = null });

    const current = [_]raft_reconciler.PlacementIntent{
        .{
            .record = .{ .group_id = 2251, .replica_id = 1, .local_node_id = 101 },
            .store_id = 101,
            .peer_node_ids = &.{ 101, 102, 103 },
            .serving_state = .draining,
        },
        .{
            .record = .{ .group_id = 2251, .replica_id = 2, .local_node_id = 102 },
            .store_id = 102,
            .peer_node_ids = &.{ 101, 102, 103 },
            .serving_state = .serving,
        },
        .{
            .record = .{ .group_id = 2251, .replica_id = 3, .local_node_id = 103 },
            .store_id = 103,
            .peer_node_ids = &.{ 101, 102, 103 },
            .serving_state = .serving,
        },
    };
    const stores = [_]table_manager.StoreRecord{
        .{
            .store_id = 101,
            .node_id = 101,
            .role = "data",
            .health_class = "healthy",
            .live = true,
            .group_statuses = @constCast((&[_]table_manager.GroupStatusReport{.{
                .group_id = 2251,
                .doc_count = 18,
                .disk_bytes = 2048,
                .empty = false,
                .local_voter = true,
                .voter_count = 3,
                .voter_set_known = true,
                .voter_set_fingerprint = table_manager.voterSetFingerprint(&.{ 101, 102, 103 }, null),
            }})[0..]),
            .runtime_statuses = @constCast((&[_]table_manager.RuntimeGroupStatusReport{.{
                .table_id = 225,
                .group_id = 2251,
                .store_id = 101,
                .node_id = 101,
                .doc_count = 18,
                .disk_bytes = 2048,
            }})[0..]),
        },
        .{
            .store_id = 102,
            .node_id = 102,
            .role = "data",
            .health_class = "healthy",
            .live = true,
            .group_statuses = @constCast((&[_]table_manager.GroupStatusReport{.{
                .group_id = 2251,
                .doc_count = 10,
                .disk_bytes = 1024,
                .empty = false,
                .local_voter = true,
                .voter_count = 3,
                .voter_set_known = true,
                .voter_set_fingerprint = table_manager.voterSetFingerprint(&.{ 101, 102, 103 }, null),
            }})[0..]),
        },
        .{
            .store_id = 103,
            .node_id = 103,
            .role = "data",
            .health_class = "healthy",
            .live = true,
            .group_statuses = @constCast((&[_]table_manager.GroupStatusReport{.{
                .group_id = 2251,
                .doc_count = 18,
                .disk_bytes = 2048,
                .empty = false,
                .local_voter = true,
                .voter_count = 3,
                .voter_set_known = true,
                .voter_set_fingerprint = table_manager.voterSetFingerprint(&.{ 101, 102, 103 }, null),
            }})[0..]),
        },
    };
    const merged_statuses = [_]MergedGroupStatus{.{
        .group_id = 2251,
        .doc_count = 18,
        .disk_bytes = 2048,
        .empty = false,
        .leader_known = true,
        .leader_store_id = 101,
        .voter_count_known = true,
        .voter_count = 3,
        .healthy_voter_reports = 3,
    }};
    const candidates = [_]@import("state.zig").CandidatePlacementInfo{
        .{ .node_id = 102, .store_id = 102, .role = "data", .failure_domain = "rack-b", .retain_current = true },
        .{ .node_id = 103, .store_id = 103, .role = "data", .failure_domain = "rack-c", .retain_current = true },
    };

    var reconciler = Reconciler.init(std.testing.allocator);
    var plan = try reconciler.computePlan(&manager, &.{ 102, 103 }, &candidates, .{
        .placement_intents = &current,
        .stores = &stores,
        .merged_group_statuses = &merged_statuses,
        .reallocation_request = .{ .request_id = 1, .requested_at_ms = 1 },
    });
    defer plan.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 0), plan.placement_removals.len);
    const source = findPlacementIntent(plan.placement_upserts, 2251, 101) orelse return error.MissingDrainingSource;
    try std.testing.expectEqual(raft_reconciler.PlacementServingState.draining, source.serving_state);
}

test "metadata reconciler compact shrink enters retirement after stable expanded membership" {
    var manager = table_manager.TableManager.init(std.testing.allocator);
    defer manager.deinit();

    try manager.upsertTable(.{ .table_id = 226, .name = "docs", .desired_replica_count = 2 });
    try manager.upsertRange(.{ .group_id = 2261, .table_id = 226, .start_key = "", .end_key = null });

    const current = [_]raft_reconciler.PlacementIntent{
        .{
            .record = .{ .group_id = 2261, .replica_id = 1, .local_node_id = 101 },
            .store_id = 101,
            .peer_node_ids = &.{ 101, 102, 103 },
            .serving_state = .draining,
            .relocation_doc_count_watermark = 18,
            .relocation_disk_bytes_watermark = 2048,
        },
        .{
            .record = .{ .group_id = 2261, .replica_id = 2, .local_node_id = 102 },
            .store_id = 102,
            .peer_node_ids = &.{ 101, 102, 103 },
            .serving_state = .serving,
        },
        .{
            .record = .{ .group_id = 2261, .replica_id = 3, .local_node_id = 103 },
            .store_id = 103,
            .peer_node_ids = &.{ 101, 102, 103 },
            .serving_state = .serving,
        },
    };
    const caught_up = table_manager.GroupStatusReport{
        .group_id = 2261,
        .doc_count = 18,
        .disk_bytes = 2048,
        .empty = false,
        .local_voter = true,
        .voter_count = 3,
        .voter_set_known = true,
        .voter_set_fingerprint = table_manager.voterSetFingerprint(&.{ 101, 102, 103 }, null),
    };
    const stores = [_]table_manager.StoreRecord{
        .{
            .store_id = 101,
            .node_id = 101,
            .role = "data",
            .health_class = "healthy",
            .live = true,
            .group_statuses = @constCast((&[_]table_manager.GroupStatusReport{.{
                .group_id = 2261,
                .doc_count = 18,
                .disk_bytes = 2048,
                .empty = false,
                .local_leader = true,
                .local_voter = true,
                .voter_count = 3,
                .voter_set_known = true,
                .voter_set_fingerprint = table_manager.voterSetFingerprint(&.{ 101, 102, 103 }, null),
            }})[0..]),
        },
        .{
            .store_id = 102,
            .node_id = 102,
            .role = "data",
            .health_class = "healthy",
            .live = true,
            // Unrelated store-level work must not block this group's stable
            // membership handoff.
            .active_backfills = 1,
            .group_statuses = @constCast((&[_]table_manager.GroupStatusReport{.{
                .group_id = 2261,
                // Physical LSM size is replica-local and is not a correctness
                // watermark for a committed Raft membership transition.
                .doc_count = 18,
                .disk_bytes = 1024,
                .empty = false,
                .local_voter = true,
                .voter_count = 3,
                .voter_set_known = true,
                .voter_set_fingerprint = table_manager.voterSetFingerprint(&.{ 101, 102, 103 }, null),
            }})[0..]),
        },
        .{
            .store_id = 103,
            .node_id = 103,
            .role = "data",
            .health_class = "healthy",
            .live = true,
            .active_backfills = 1,
            .group_statuses = @constCast((&[_]table_manager.GroupStatusReport{caught_up})[0..]),
        },
    };
    const candidates = [_]@import("state.zig").CandidatePlacementInfo{
        .{ .node_id = 102, .store_id = 102, .role = "data", .failure_domain = "rack-b", .retain_current = true },
        .{ .node_id = 103, .store_id = 103, .role = "data", .failure_domain = "rack-c", .retain_current = true },
    };

    var reconciler = Reconciler.init(std.testing.allocator);
    var plan = try reconciler.computePlan(&manager, &.{ 102, 103 }, &candidates, .{
        .placement_intents = &current,
        .stores = &stores,
        .reallocation_request = .{ .request_id = 1, .requested_at_ms = 1 },
    });
    defer plan.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 0), plan.placement_removals.len);
    const retiring = findPlacementIntent(plan.placement_upserts, 2261, 101) orelse return error.MissingRetiringSource;
    try std.testing.expectEqual(raft_reconciler.PlacementServingState.retiring, retiring.serving_state);
    try std.testing.expectEqualSlices(u64, &.{ 102, 103 }, retiring.peer_node_ids);
}

test "metadata reconciler requires preserved peers to report stable membership before retirement" {
    var manager = table_manager.TableManager.init(std.testing.allocator);
    defer manager.deinit();

    try manager.upsertTable(.{ .table_id = 222, .name = "docs", .desired_replica_count = 2 });
    try manager.upsertRange(.{ .group_id = 2221, .table_id = 222, .start_key = "", .end_key = null });

    const current = [_]raft_reconciler.PlacementIntent{
        .{
            .record = .{
                .group_id = 2221,
                .replica_id = 1,
                .local_node_id = 1,
            },
            .store_id = 11,
            .peer_node_ids = &.{ 1, 2 },
            .serving_state = .draining,
            .relocation_doc_count_watermark = 7,
            .relocation_disk_bytes_watermark = 4096,
        },
        .{
            .record = .{
                .group_id = 2221,
                .replica_id = 2,
                .local_node_id = 2,
            },
            .store_id = 22,
            .peer_node_ids = &.{ 1, 2 },
            .serving_state = .serving,
        },
        .{
            .record = .{
                .group_id = 2221,
                .replica_id = 2,
                .local_node_id = 3,
                .bootstrap_mode = .persisted,
            },
            .store_id = 33,
            .peer_node_ids = &.{ 2, 3 },
            .serving_state = .serving,
            .relocation_source_node_id = 1,
            .relocation_source_store_id = 11,
            .relocation_doc_count_watermark = 7,
            .relocation_disk_bytes_watermark = 4096,
        },
    };
    const stores = [_]table_manager.StoreRecord{
        .{
            .store_id = 11,
            .node_id = 1,
            .role = "data",
            .health_class = "healthy",
            .live = true,
            .group_statuses = @constCast((&[_]table_manager.GroupStatusReport{.{
                .group_id = 2221,
                .doc_count = 7,
                .disk_bytes = 4096,
                .empty = false,
                .local_voter = true,
                .voter_count = 2,
            }})[0..]),
        },
        .{
            .store_id = 22,
            .node_id = 2,
            .role = "data",
            .health_class = "healthy",
            .live = true,
            .group_statuses = @constCast((&[_]table_manager.GroupStatusReport{.{
                .group_id = 2221,
                .doc_count = 7,
                .disk_bytes = 4096,
                .empty = false,
                .local_voter = true,
                .voter_count = 2,
            }})[0..]),
        },
        .{
            .store_id = 33,
            .node_id = 3,
            .role = "data",
            .health_class = "healthy",
            .live = true,
            .group_statuses = @constCast((&[_]table_manager.GroupStatusReport{.{
                .group_id = 2221,
                .doc_count = 7,
                .disk_bytes = 4096,
                .empty = false,
                .local_leader = true,
                .local_voter = true,
                .voter_count = 2,
                .voter_set_known = true,
                .voter_set_fingerprint = table_manager.voterSetFingerprint(&.{ 2, 3 }, null),
                .replay_required = false,
                .replay_caught_up = true,
                .cutover_ready = true,
                .reads_ready_after_cutover = true,
            }})[0..]),
        },
    };
    const candidates = [_]@import("state.zig").CandidatePlacementInfo{
        .{
            .node_id = 2,
            .store_id = 22,
            .role = "data",
            .failure_domain = "rack-b",
            .retain_current = true,
        },
        .{
            .node_id = 3,
            .store_id = 33,
            .role = "data",
            .failure_domain = "rack-c",
            .retain_current = true,
        },
    };

    var reconciler = Reconciler.init(std.testing.allocator);
    var plan = try reconciler.computePlan(&manager, &.{ 2, 3 }, &candidates, .{
        .placement_intents = &current,
        .stores = &stores,
        .reallocation_request = .{ .request_id = 1, .requested_at_ms = 1 },
    });
    defer plan.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 0), plan.placement_removals.len);
    const source = findPlacementIntent(plan.placement_upserts, 2221, 1) orelse return error.MissingDrainingSource;
    try std.testing.expectEqual(raft_reconciler.PlacementServingState.draining, source.serving_state);
}

test "metadata reconciler waits for every final voter despite unrelated relocation identity" {
    var manager = table_manager.TableManager.init(std.testing.allocator);
    defer manager.deinit();

    try manager.upsertTable(.{ .table_id = 224, .name = "docs", .desired_replica_count = 3 });
    try manager.upsertRange(.{ .group_id = 2241, .table_id = 224, .start_key = "", .end_key = null });

    const current = [_]raft_reconciler.PlacementIntent{
        .{
            .record = .{ .group_id = 2241, .replica_id = 1, .local_node_id = 101 },
            .store_id = 101,
            .peer_node_ids = &.{ 102, 103, 104 },
            .serving_state = .draining,
            .relocation_doc_count_watermark = 3,
            .relocation_disk_bytes_watermark = 2048,
        },
        .{
            .record = .{ .group_id = 2241, .replica_id = 2, .local_node_id = 102 },
            .store_id = 102,
            .peer_node_ids = &.{ 102, 103, 104 },
            .serving_state = .serving,
        },
        .{
            .record = .{ .group_id = 2241, .replica_id = 3, .local_node_id = 103 },
            .store_id = 103,
            .peer_node_ids = &.{ 102, 103, 104 },
            .serving_state = .serving,
            .relocation_source_node_id = 101,
            .relocation_source_store_id = 101,
            .relocation_doc_count_watermark = 3,
            .relocation_disk_bytes_watermark = 2048,
        },
        .{
            .record = .{ .group_id = 2241, .replica_id = 1, .local_node_id = 104 },
            .store_id = 104,
            .peer_node_ids = &.{ 102, 103, 104 },
            .serving_state = .replaying,
            .relocation_source_node_id = 102,
            .relocation_source_store_id = 102,
            .relocation_doc_count_watermark = 3,
            .relocation_disk_bytes_watermark = 2048,
        },
    };
    const stores = [_]table_manager.StoreRecord{
        .{
            .store_id = 101,
            .node_id = 101,
            .role = "data",
            .health_class = "healthy",
            .live = true,
            .group_statuses = @constCast((&[_]table_manager.GroupStatusReport{.{
                .group_id = 2241,
                .doc_count = 3,
                .disk_bytes = 2048,
                .empty = false,
                .local_voter = true,
                .voter_count = 3,
            }})[0..]),
        },
        .{
            .store_id = 102,
            .node_id = 102,
            .role = "data",
            .health_class = "healthy",
            .live = true,
            .group_statuses = @constCast((&[_]table_manager.GroupStatusReport{.{
                .group_id = 2241,
                .doc_count = 3,
                .disk_bytes = 2048,
                .empty = false,
                .local_voter = true,
                .voter_count = 3,
            }})[0..]),
        },
        .{
            .store_id = 103,
            .node_id = 103,
            .role = "data",
            .health_class = "healthy",
            .live = true,
            .group_statuses = @constCast((&[_]table_manager.GroupStatusReport{.{
                .group_id = 2241,
                .doc_count = 3,
                .disk_bytes = 2048,
                .empty = false,
                .local_leader = true,
                .local_voter = true,
                .voter_count = 3,
                .voter_set_known = true,
                .voter_set_fingerprint = table_manager.voterSetFingerprint(&.{ 102, 103, 104 }, null),
                .replay_required = false,
                .replay_caught_up = true,
                .cutover_ready = true,
                .reads_ready_after_cutover = true,
            }})[0..]),
        },
        .{
            .store_id = 104,
            .node_id = 104,
            .role = "data",
            .health_class = "healthy",
            .live = true,
            .group_statuses = @constCast((&[_]table_manager.GroupStatusReport{.{
                .group_id = 2241,
                .doc_count = 3,
                .disk_bytes = 2048,
                .empty = false,
                .local_voter = true,
                .voter_count = 3,
                .replay_required = true,
                .replay_caught_up = false,
            }})[0..]),
        },
    };
    const candidates = [_]@import("state.zig").CandidatePlacementInfo{
        .{ .node_id = 102, .store_id = 102, .role = "data", .failure_domain = "rack-b", .retain_current = true },
        .{ .node_id = 103, .store_id = 103, .role = "data", .failure_domain = "rack-c", .retain_current = true },
        .{ .node_id = 104, .store_id = 104, .role = "data", .failure_domain = "rack-d", .retain_current = true },
    };

    var reconciler = Reconciler.init(std.testing.allocator);
    var plan = try reconciler.computePlan(&manager, &.{ 102, 103, 104 }, &candidates, .{
        .placement_intents = &current,
        .stores = &stores,
        .reallocation_request = .{ .request_id = 1, .requested_at_ms = 1 },
    });
    defer plan.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 0), plan.placement_removals.len);
    const source = findPlacementIntent(plan.placement_upserts, 2241, 101) orelse return error.MissingDrainingSource;
    try std.testing.expectEqual(raft_reconciler.PlacementServingState.draining, source.serving_state);
}

test "metadata reconciler finalizes schema migration once every hosting node reports target schema progress" {
    var manager = table_manager.TableManager.init(std.testing.allocator);
    defer manager.deinit();

    try manager.upsertTable(.{
        .table_id = 30,
        .name = "docs",
        .schema_json = "{\"version\":1}",
        .read_schema_json = "{\"version\":0}",
        .indexes_json = "{\"full_text_index_v0\":{\"type\":\"full_text\"},\"full_text_index_v1\":{\"type\":\"full_text\"}}",
    });
    try manager.upsertRange(.{
        .group_id = 3001,
        .table_id = 30,
        .start_key = "doc:a",
        .end_key = "doc:z",
    });

    const tables = try manager.listTables(std.testing.allocator);
    defer manager.freeTables(std.testing.allocator, tables);
    const ranges = try manager.listRanges(std.testing.allocator);
    defer manager.freeRanges(std.testing.allocator, ranges);

    const intents = [_]raft_reconciler.PlacementIntent{
        .{ .record = .{ .group_id = 3001, .replica_id = 1, .local_node_id = 7, .bootstrap_mode = .persisted }, .peer_node_ids = &.{ 7, 8 } },
        .{ .record = .{ .group_id = 3001, .replica_id = 2, .local_node_id = 8, .bootstrap_mode = .persisted }, .peer_node_ids = &.{ 7, 8 } },
    };
    const progress = [_]table_manager.SchemaProgressRecord{
        .{ .table_id = 30, .node_id = 7, .schema_version = 1 },
        .{ .table_id = 30, .node_id = 8, .schema_version = 1 },
    };

    var reconciler = Reconciler.init(std.testing.allocator);
    var plan = try reconciler.computePlan(&manager, &.{}, &.{}, .{
        .placement_intents = &intents,
        .tables = tables,
        .ranges = ranges,
        .schema_progresses = &progress,
    });
    defer plan.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 1), plan.table_upserts.len);
    try std.testing.expectEqualStrings("", plan.table_upserts[0].read_schema_json);
    try std.testing.expect(std.mem.indexOf(u8, plan.table_upserts[0].indexes_json, "\"full_text_index_v1\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, plan.table_upserts[0].indexes_json, "\"full_text_index_v0\"") == null);
}

test "metadata reconciler keeps schema migration open until every hosting node reports target schema progress" {
    var manager = table_manager.TableManager.init(std.testing.allocator);
    defer manager.deinit();

    try manager.upsertTable(.{
        .table_id = 31,
        .name = "docs",
        .schema_json = "{\"version\":1}",
        .read_schema_json = "{\"version\":0}",
        .indexes_json = "{\"full_text_index_v0\":{\"type\":\"full_text\"},\"full_text_index_v1\":{\"type\":\"full_text\"}}",
    });
    try manager.upsertRange(.{
        .group_id = 3101,
        .table_id = 31,
        .start_key = "doc:a",
        .end_key = "doc:z",
    });

    const tables = try manager.listTables(std.testing.allocator);
    defer manager.freeTables(std.testing.allocator, tables);
    const ranges = try manager.listRanges(std.testing.allocator);
    defer manager.freeRanges(std.testing.allocator, ranges);

    const intents = [_]raft_reconciler.PlacementIntent{
        .{ .record = .{ .group_id = 3101, .replica_id = 1, .local_node_id = 7, .bootstrap_mode = .persisted }, .peer_node_ids = &.{ 7, 8 } },
        .{ .record = .{ .group_id = 3101, .replica_id = 2, .local_node_id = 8, .bootstrap_mode = .persisted }, .peer_node_ids = &.{ 7, 8 } },
    };
    const progress = [_]table_manager.SchemaProgressRecord{
        .{ .table_id = 31, .node_id = 7, .schema_version = 1 },
    };

    var reconciler = Reconciler.init(std.testing.allocator);
    var plan = try reconciler.computePlan(&manager, &.{}, &.{}, .{
        .placement_intents = &intents,
        .tables = tables,
        .ranges = ranges,
        .schema_progresses = &progress,
    });
    defer plan.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 0), plan.table_upserts.len);
}

test "metadata reconciler plans an automatic split from fresh group status" {
    var manager = table_manager.TableManager.init(std.testing.allocator);
    defer manager.deinit();

    try manager.upsertTable(.{ .table_id = 40, .name = "docs" });
    try manager.upsertRange(.{
        .group_id = 4001,
        .table_id = 40,
        .start_key = "doc:a",
        .end_key = "doc:z",
    });

    const tables = try manager.listTables(std.testing.allocator);
    defer manager.freeTables(std.testing.allocator, tables);
    const ranges = try manager.listRanges(std.testing.allocator);
    defer manager.freeRanges(std.testing.allocator, ranges);

    const now_ms = platform_clock.Clock.real().nowRealtimeMs();
    const voter_set_fingerprint = table_manager.voterSetFingerprint(&.{1}, null);
    const placements = [_]raft_reconciler.PlacementIntent{.{
        .record = .{
            .group_id = 4001,
            .replica_id = 1,
            .local_node_id = 1,
            .bootstrap_mode = .persisted,
        },
        .store_id = 1,
        .peer_node_ids = &.{1},
    }};
    const stores = [_]table_manager.StoreRecord{
        .{
            .store_id = 1,
            .node_id = 1,
            .role = "data",
            .health_class = "healthy",
            .failure_domain = "rack-a",
            .live = true,
            .group_statuses = @constCast((&[_]table_manager.GroupStatusReport{
                .{
                    .group_id = 4001,
                    .doc_count = 200,
                    .disk_bytes = 200,
                    .empty = false,
                    .updated_at_millis = now_ms,
                    .local_leader = true,
                    .local_voter = true,
                    .voter_count = 1,
                    .voter_set_known = true,
                    .voter_set_fingerprint = voter_set_fingerprint,
                },
            })[0..]),
        },
    };

    var lookup = TestMedianKeyLookup{ .median_key = "doc:m" };
    var reconciler = Reconciler.initWithConfig(std.testing.allocator, .{
        .max_shard_size_bytes = 100,
        .max_shards_per_table = 8,
        .median_key_lookup = lookup.iface(),
    });
    var plan = try reconciler.computePlan(&manager, &.{}, &.{}, .{
        .tables = tables,
        .ranges = ranges,
        .placement_intents = &placements,
        .stores = &stores,
    });
    defer plan.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 1), plan.split_admissions.len);
    try std.testing.expectEqual(@as(u64, 4001), plan.split_admissions[0].record.source_group_id);
    try std.testing.expect(plan.split_admissions[0].record.destination_group_id != 0);
    try std.testing.expectEqualStrings("doc:m", plan.split_admissions[0].record.split_key.?);
}

test "metadata reconciler automatic split planning cleans up every allocation failure" {
    const Runner = struct {
        fn run(alloc: std.mem.Allocator) !void {
            var manager = table_manager.TableManager.init(alloc);
            defer manager.deinit();

            try manager.upsertTable(.{ .table_id = 40, .name = "docs" });
            try manager.upsertRange(.{
                .group_id = 4001,
                .table_id = 40,
                .start_key = "doc:a",
                .end_key = "doc:z",
            });

            const tables = try manager.listTables(alloc);
            defer manager.freeTables(alloc, tables);
            const ranges = try manager.listRanges(alloc);
            defer manager.freeRanges(alloc, ranges);

            const voter_set_fingerprint = table_manager.voterSetFingerprint(
                &.{1},
                null,
            );
            const placements = [_]raft_reconciler.PlacementIntent{.{
                .record = .{
                    .group_id = 4001,
                    .replica_id = 1,
                    .local_node_id = 1,
                    .bootstrap_mode = .persisted,
                },
                .store_id = 1,
                .peer_node_ids = &.{1},
            }};
            const stores = [_]table_manager.StoreRecord{.{
                .store_id = 1,
                .node_id = 1,
                .role = "data",
                .health_class = "healthy",
                .failure_domain = "rack-a",
                .live = true,
                .group_statuses = @constCast((&[_]table_manager.GroupStatusReport{.{
                    .group_id = 4001,
                    .doc_count = 200,
                    .disk_bytes = 200,
                    .empty = false,
                    .updated_at_millis = platform_clock.Clock.real().nowRealtimeMs(),
                    .local_leader = true,
                    .local_voter = true,
                    .voter_count = 1,
                    .voter_set_known = true,
                    .voter_set_fingerprint = voter_set_fingerprint,
                }})[0..]),
            }};

            var lookup = TestMedianKeyLookup{ .median_key = "doc:m" };
            var reconciler = Reconciler.initWithConfig(alloc, .{
                .max_shard_size_bytes = 100,
                .max_shards_per_table = 8,
                .median_key_lookup = lookup.iface(),
            });
            var plan = try reconciler.computePlan(&manager, &.{}, &.{}, .{
                .tables = tables,
                .ranges = ranges,
                .placement_intents = &placements,
                .stores = &stores,
            });
            defer plan.deinit(alloc);
        }
    };

    try std.testing.checkAllAllocationFailures(
        std.testing.allocator,
        Runner.run,
        .{},
    );
}

test "metadata reconciler waits for placement convergence before automatic split" {
    var manager = table_manager.TableManager.init(std.testing.allocator);
    defer manager.deinit();

    try manager.upsertTable(.{ .table_id = 401, .name = "docs" });
    try manager.upsertRange(.{
        .group_id = 4011,
        .table_id = 401,
        .start_key = "doc:a",
        .end_key = "doc:z",
    });

    const tables = try manager.listTables(std.testing.allocator);
    defer manager.freeTables(std.testing.allocator, tables);
    const ranges = try manager.listRanges(std.testing.allocator);
    defer manager.freeRanges(std.testing.allocator, ranges);

    const now_ms: u64 = @intCast(@divTrunc(platform_time.monotonicNs(), std.time.ns_per_ms));
    const statuses = @constCast((&[_]table_manager.GroupStatusReport{.{
        .group_id = 4011,
        .doc_count = 200,
        .disk_bytes = 200,
        .empty = false,
        .updated_at_millis = now_ms,
        .local_leader = true,
    }})[0..]);
    const stores = [_]table_manager.StoreRecord{.{
        .store_id = 1,
        .node_id = 1,
        .role = "data",
        .health_class = "healthy",
        .failure_domain = "rack-a",
        .live = true,
        .group_statuses = statuses,
    }};
    const intents = [_]raft_reconciler.PlacementIntent{.{
        .record = .{
            .group_id = 4011,
            .replica_id = 1,
            .local_node_id = 1,
            .bootstrap_mode = .persisted,
        },
        .peer_node_ids = &.{1},
        .serving_state = .bootstrapping,
    }};

    var lookup = TestMedianKeyLookup{ .median_key = "doc:m" };
    var reconciler = Reconciler.initWithConfig(std.testing.allocator, .{
        .max_shard_size_bytes = 100,
        .max_shards_per_table = 8,
        .median_key_lookup = lookup.iface(),
    });
    var plan = try reconciler.computePlan(&manager, &.{}, &.{}, .{
        .placement_intents = &intents,
        .tables = tables,
        .ranges = ranges,
        .stores = &stores,
    });
    defer plan.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 0), plan.split_upserts.len);
}

test "metadata reconciler plans an automatic split from disk size when doc count is stale" {
    var manager = table_manager.TableManager.init(std.testing.allocator);
    defer manager.deinit();

    try manager.upsertTable(.{ .table_id = 410, .name = "docs" });
    try manager.upsertRange(.{
        .group_id = 4101,
        .table_id = 410,
        .start_key = "doc:a",
        .end_key = "doc:z",
    });

    const tables = try manager.listTables(std.testing.allocator);
    defer manager.freeTables(std.testing.allocator, tables);
    const ranges = try manager.listRanges(std.testing.allocator);
    defer manager.freeRanges(std.testing.allocator, ranges);

    const now_ms = platform_clock.Clock.real().nowRealtimeMs();
    const voter_set_fingerprint = table_manager.voterSetFingerprint(&.{1}, null);
    const placements = [_]raft_reconciler.PlacementIntent{.{
        .record = .{
            .group_id = 4101,
            .replica_id = 1,
            .local_node_id = 1,
            .bootstrap_mode = .persisted,
        },
        .store_id = 1,
        .peer_node_ids = &.{1},
    }};
    const stores = [_]table_manager.StoreRecord{
        .{
            .store_id = 1,
            .node_id = 1,
            .role = "data",
            .health_class = "healthy",
            .failure_domain = "rack-a",
            .live = true,
            .group_statuses = @constCast((&[_]table_manager.GroupStatusReport{
                .{
                    .group_id = 4101,
                    .doc_count = 0,
                    .disk_bytes = 200,
                    .empty = false,
                    .updated_at_millis = now_ms,
                    .local_leader = true,
                    .local_voter = true,
                    .voter_count = 1,
                    .voter_set_known = true,
                    .voter_set_fingerprint = voter_set_fingerprint,
                },
            })[0..]),
        },
    };

    var lookup = TestMedianKeyLookup{ .median_key = "doc:m" };
    var reconciler = Reconciler.initWithConfig(std.testing.allocator, .{
        .max_shard_size_bytes = 100,
        .max_shards_per_table = 8,
        .median_key_lookup = lookup.iface(),
    });
    var plan = try reconciler.computePlan(&manager, &.{}, &.{}, .{
        .tables = tables,
        .ranges = ranges,
        .placement_intents = &placements,
        .stores = &stores,
    });
    defer plan.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 1), plan.split_admissions.len);
    try std.testing.expectEqual(@as(u64, 4101), plan.split_admissions[0].record.source_group_id);
    try std.testing.expect(plan.split_admissions[0].record.destination_group_id != 0);
    try std.testing.expectEqualStrings("doc:m", plan.split_admissions[0].record.split_key.?);
}

test "metadata reconciler keeps structurally valid automatic split intent across transient recompute miss" {
    var manager = table_manager.TableManager.init(std.testing.allocator);
    defer manager.deinit();

    try manager.upsertTable(.{ .table_id = 401, .name = "docs" });
    try manager.upsertRange(.{
        .group_id = 4011,
        .table_id = 401,
        .start_key = "doc:a",
        .end_key = "doc:z",
    });
    try manager.requestSplit(.{
        .transition_id = 40101,
        .table_id = 401,
        .source_group_id = 4011,
        .destination_group_id = 4012,
        .split_key = "doc:m",
        .automatic = true,
    });

    try pruneAutomaticIntents(std.testing.allocator, &manager, .{}, &.{}, &.{});
    try std.testing.expectEqual(@as(u32, 1), manager.split_intents.count());

    try manager.upsertRange(.{
        .group_id = 4011,
        .table_id = 401,
        .start_key = "doc:a",
        .end_key = "doc:b",
    });
    try pruneAutomaticIntents(std.testing.allocator, &manager, .{}, &.{}, &.{});
    try std.testing.expectEqual(@as(u32, 0), manager.split_intents.count());
}

test "metadata reconciler does not automatically split stale doc identity namespace" {
    var manager = table_manager.TableManager.init(std.testing.allocator);
    defer manager.deinit();

    try manager.upsertTable(.{ .table_id = 416, .name = "docs" });
    try manager.upsertRange(.{
        .group_id = 4161,
        .range_id = 9001,
        .table_id = 416,
        .start_key = "doc:a",
        .end_key = "doc:z",
    });

    const tables = try manager.listTables(std.testing.allocator);
    defer manager.freeTables(std.testing.allocator, tables);
    const ranges = try manager.listRanges(std.testing.allocator);
    defer manager.freeRanges(std.testing.allocator, ranges);

    const now_ms = platform_clock.Clock.real().nowRealtimeMs();
    const updated_at_ns = now_ms * std.time.ns_per_ms;
    const stores = [_]table_manager.StoreRecord{
        .{
            .store_id = 1,
            .node_id = 1,
            .role = "data",
            .health_class = "healthy",
            .failure_domain = "rack-a",
            .live = true,
            .group_statuses = @constCast((&[_]table_manager.GroupStatusReport{.{
                .group_id = 4161,
                .doc_count = 200,
                .disk_bytes = 200,
                .empty = false,
                .updated_at_millis = now_ms,
                .local_leader = true,
            }})[0..]),
            .runtime_statuses = @constCast((&[_]table_manager.RuntimeGroupStatusReport{.{
                .table_id = 416,
                .table_name = "docs",
                .group_id = 4161,
                .updated_at_ns = updated_at_ns,
                .doc_identity = .{
                    .namespace_table_id = 416,
                    .namespace_shard_id = 4161,
                    .namespace_range_id = 42,
                    .next_ordinal = 201,
                    .allocated_ordinals = 200,
                    .live_ordinals = 200,
                },
            }})[0..]),
        },
    };

    var lookup = TestMedianKeyLookup{ .median_key = "doc:m" };
    var reconciler = Reconciler.initWithConfig(std.testing.allocator, .{
        .max_shard_size_bytes = 100,
        .max_shards_per_table = 8,
        .median_key_lookup = lookup.iface(),
    });
    var plan = try reconciler.computePlan(&manager, &.{}, &.{}, .{
        .tables = tables,
        .ranges = ranges,
        .stores = &stores,
    });
    defer plan.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 0), plan.split_upserts.len);
}

test "metadata reconciler does not automatically split ordinal exhausted doc identity" {
    var manager = table_manager.TableManager.init(std.testing.allocator);
    defer manager.deinit();

    try manager.upsertTable(.{ .table_id = 417, .name = "docs" });
    try manager.upsertRange(.{
        .group_id = 4171,
        .range_id = 9001,
        .table_id = 417,
        .start_key = "doc:a",
        .end_key = "doc:z",
    });

    const tables = try manager.listTables(std.testing.allocator);
    defer manager.freeTables(std.testing.allocator, tables);
    const ranges = try manager.listRanges(std.testing.allocator);
    defer manager.freeRanges(std.testing.allocator, ranges);

    const now_ms = platform_clock.Clock.real().nowRealtimeMs();
    const updated_at_ns = now_ms * std.time.ns_per_ms;
    const stores = [_]table_manager.StoreRecord{
        .{
            .store_id = 1,
            .node_id = 1,
            .role = "data",
            .health_class = "healthy",
            .failure_domain = "rack-a",
            .live = true,
            .group_statuses = @constCast((&[_]table_manager.GroupStatusReport{.{
                .group_id = 4171,
                .doc_count = 200,
                .disk_bytes = 200,
                .empty = false,
                .updated_at_millis = now_ms,
                .local_leader = true,
            }})[0..]),
            .runtime_statuses = @constCast((&[_]table_manager.RuntimeGroupStatusReport{.{
                .table_id = 417,
                .table_name = "docs",
                .group_id = 4171,
                .updated_at_ns = updated_at_ns,
                .doc_identity = .{
                    .namespace_table_id = 417,
                    .namespace_shard_id = 4171,
                    .namespace_range_id = 9001,
                    .next_ordinal = std.math.maxInt(u32),
                    .allocated_ordinals = std.math.maxInt(u32) - 1,
                    .live_ordinals = 200,
                    .ordinal_capacity_exhausted = true,
                },
            }})[0..]),
        },
    };

    var lookup = TestMedianKeyLookup{ .median_key = "doc:m" };
    var reconciler = Reconciler.initWithConfig(std.testing.allocator, .{
        .max_shard_size_bytes = 100,
        .max_shards_per_table = 8,
        .median_key_lookup = lookup.iface(),
    });
    var plan = try reconciler.computePlan(&manager, &.{}, &.{}, .{
        .tables = tables,
        .ranges = ranges,
        .stores = &stores,
    });
    defer plan.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 0), plan.split_upserts.len);
}

test "metadata reconciler does not split when a replica is missing healthy group status" {
    var manager = table_manager.TableManager.init(std.testing.allocator);
    defer manager.deinit();

    try manager.upsertTable(.{ .table_id = 401, .name = "docs" });
    try manager.upsertRange(.{
        .group_id = 4011,
        .table_id = 401,
        .start_key = "doc:a",
        .end_key = "doc:z",
    });

    const tables = try manager.listTables(std.testing.allocator);
    defer manager.freeTables(std.testing.allocator, tables);
    const ranges = try manager.listRanges(std.testing.allocator);
    defer manager.freeRanges(std.testing.allocator, ranges);

    const now_ms = platform_clock.Clock.real().nowRealtimeMs();
    const stores = [_]table_manager.StoreRecord{
        .{
            .store_id = 1,
            .node_id = 1,
            .role = "data",
            .health_class = "healthy",
            .failure_domain = "rack-a",
            .live = true,
            .group_statuses = @constCast((&[_]table_manager.GroupStatusReport{
                .{
                    .group_id = 4011,
                    .doc_count = 200,
                    .disk_bytes = 200,
                    .empty = false,
                    .updated_at_millis = now_ms,
                    .local_leader = true,
                },
            })[0..]),
        },
    };
    const intents = [_]raft_reconciler.PlacementIntent{
        .{ .record = .{ .group_id = 4011, .replica_id = 1, .local_node_id = 1, .bootstrap_mode = .persisted }, .store_id = 1, .peer_node_ids = &.{2} },
        .{ .record = .{ .group_id = 4011, .replica_id = 2, .local_node_id = 2, .bootstrap_mode = .persisted }, .store_id = 2, .peer_node_ids = &.{1} },
    };

    var reconciler = Reconciler.initWithConfig(std.testing.allocator, .{
        .max_shard_size_bytes = 100,
        .max_shards_per_table = 8,
    });
    var plan = try reconciler.computePlan(&manager, &.{}, &.{}, .{
        .placement_intents = &intents,
        .tables = tables,
        .ranges = ranges,
        .stores = &stores,
    });
    defer plan.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 0), plan.split_upserts.len);
}

test "metadata reconciler does not split when authoritative voter reports are incomplete" {
    var manager = table_manager.TableManager.init(std.testing.allocator);
    defer manager.deinit();

    try manager.upsertTable(.{ .table_id = 402, .name = "docs" });
    try manager.upsertRange(.{
        .group_id = 4021,
        .table_id = 402,
        .start_key = "doc:a",
        .end_key = "doc:z",
    });

    const tables = try manager.listTables(std.testing.allocator);
    defer manager.freeTables(std.testing.allocator, tables);
    const ranges = try manager.listRanges(std.testing.allocator);
    defer manager.freeRanges(std.testing.allocator, ranges);

    const now_ms = platform_clock.Clock.real().nowRealtimeMs();
    const stores = [_]table_manager.StoreRecord{
        .{
            .store_id = 1,
            .node_id = 1,
            .role = "data",
            .health_class = "healthy",
            .failure_domain = "rack-a",
            .live = true,
            .group_statuses = @constCast((&[_]table_manager.GroupStatusReport{
                .{
                    .group_id = 4021,
                    .doc_count = 200,
                    .disk_bytes = 200,
                    .empty = false,
                    .updated_at_millis = now_ms,
                    .local_leader = true,
                    .local_voter = true,
                    .voter_count = 2,
                },
            })[0..]),
        },
    };

    var reconciler = Reconciler.initWithConfig(std.testing.allocator, .{
        .max_shard_size_bytes = 100,
        .max_shards_per_table = 8,
    });
    var plan = try reconciler.computePlan(&manager, &.{}, &.{}, .{
        .tables = tables,
        .ranges = ranges,
        .stores = &stores,
    });
    defer plan.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 0), plan.split_upserts.len);
}

test "metadata reconciler does not split under-replicated groups when placement intents expect more voters" {
    var manager = table_manager.TableManager.init(std.testing.allocator);
    defer manager.deinit();

    try manager.upsertTable(.{ .table_id = 4022, .name = "docs" });
    try manager.upsertRange(.{
        .group_id = 40221,
        .table_id = 4022,
        .start_key = "doc:a",
        .end_key = "doc:z",
    });

    const tables = try manager.listTables(std.testing.allocator);
    defer manager.freeTables(std.testing.allocator, tables);
    const ranges = try manager.listRanges(std.testing.allocator);
    defer manager.freeRanges(std.testing.allocator, ranges);

    const now_ms = platform_clock.Clock.real().nowRealtimeMs();
    const stores = [_]table_manager.StoreRecord{
        .{
            .store_id = 1,
            .node_id = 1,
            .role = "data",
            .health_class = "healthy",
            .failure_domain = "rack-a",
            .live = true,
            .group_statuses = @constCast((&[_]table_manager.GroupStatusReport{
                .{
                    .group_id = 40221,
                    .doc_count = 200,
                    .disk_bytes = 200,
                    .empty = false,
                    .updated_at_millis = now_ms,
                    .local_leader = true,
                    .local_voter = true,
                    .voter_count = 2,
                },
            })[0..]),
        },
        .{
            .store_id = 2,
            .node_id = 2,
            .role = "data",
            .health_class = "healthy",
            .failure_domain = "rack-b",
            .live = true,
            .group_statuses = @constCast((&[_]table_manager.GroupStatusReport{
                .{
                    .group_id = 40221,
                    .updated_at_millis = now_ms,
                    .local_voter = true,
                    .voter_count = 2,
                },
            })[0..]),
        },
    };
    const intents = [_]raft_reconciler.PlacementIntent{
        .{ .record = .{ .group_id = 40221, .replica_id = 1, .local_node_id = 1, .bootstrap_mode = .persisted }, .store_id = 1, .peer_node_ids = &.{ 2, 3 } },
        .{ .record = .{ .group_id = 40221, .replica_id = 2, .local_node_id = 2, .bootstrap_mode = .persisted }, .store_id = 2, .peer_node_ids = &.{ 1, 3 } },
        .{ .record = .{ .group_id = 40221, .replica_id = 3, .local_node_id = 3, .bootstrap_mode = .persisted }, .store_id = 3, .peer_node_ids = &.{ 1, 2 } },
    };

    var reconciler = Reconciler.initWithConfig(std.testing.allocator, .{
        .max_shard_size_bytes = 100,
        .max_shards_per_table = 8,
    });
    var plan = try reconciler.computePlan(&manager, &.{}, &.{}, .{
        .placement_intents = &intents,
        .tables = tables,
        .ranges = ranges,
        .stores = &stores,
    });
    defer plan.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 0), plan.split_upserts.len);
}

test "metadata reconciler does not split during joint consensus" {
    var manager = table_manager.TableManager.init(std.testing.allocator);
    defer manager.deinit();

    try manager.upsertTable(.{ .table_id = 403, .name = "docs" });
    try manager.upsertRange(.{
        .group_id = 4031,
        .table_id = 403,
        .start_key = "doc:a",
        .end_key = "doc:z",
    });

    const tables = try manager.listTables(std.testing.allocator);
    defer manager.freeTables(std.testing.allocator, tables);
    const ranges = try manager.listRanges(std.testing.allocator);
    defer manager.freeRanges(std.testing.allocator, ranges);

    const now_ms = platform_clock.Clock.real().nowRealtimeMs();
    const stores = [_]table_manager.StoreRecord{
        .{
            .store_id = 1,
            .node_id = 1,
            .role = "data",
            .health_class = "healthy",
            .failure_domain = "rack-a",
            .live = true,
            .group_statuses = @constCast((&[_]table_manager.GroupStatusReport{
                .{
                    .group_id = 4031,
                    .doc_count = 200,
                    .disk_bytes = 200,
                    .empty = false,
                    .updated_at_millis = now_ms,
                    .local_leader = true,
                    .local_voter = true,
                    .voter_count = 2,
                    .joint_consensus = true,
                },
                .{
                    .group_id = 4031,
                    .updated_at_millis = now_ms,
                    .local_voter = true,
                    .voter_count = 2,
                    .joint_consensus = true,
                },
            })[0..]),
        },
    };

    var reconciler = Reconciler.initWithConfig(std.testing.allocator, .{
        .max_shard_size_bytes = 100,
        .max_shards_per_table = 8,
    });
    var plan = try reconciler.computePlan(&manager, &.{}, &.{}, .{
        .tables = tables,
        .ranges = ranges,
        .stores = &stores,
    });
    defer plan.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 0), plan.split_upserts.len);
}

test "metadata reconciler plans an automatic merge from adjacent small fresh groups" {
    var manager = table_manager.TableManager.init(std.testing.allocator);
    defer manager.deinit();

    try manager.upsertTable(.{ .table_id = 41, .name = "docs", .min_ranges = 1 });
    try manager.upsertRange(.{
        .group_id = 4101,
        .range_id = 4101,
        .table_id = 41,
        .start_key = "doc:a",
        .end_key = "doc:m",
        .doc_identity_shard_id = 4101,
        .doc_identity_range_id = 4101,
    });
    try manager.upsertRange(.{
        .group_id = 4102,
        .range_id = 4102,
        .table_id = 41,
        .start_key = "doc:m",
        .end_key = "doc:z",
        .doc_identity_shard_id = 4101,
        .doc_identity_range_id = 4101,
    });

    const tables = try manager.listTables(std.testing.allocator);
    defer manager.freeTables(std.testing.allocator, tables);
    const ranges = try manager.listRanges(std.testing.allocator);
    defer manager.freeRanges(std.testing.allocator, ranges);

    const now_ms = platform_clock.Clock.real().nowRealtimeMs();
    const voter_set_fingerprint = table_manager.voterSetFingerprint(&.{1}, null);
    const placements = [_]raft_reconciler.PlacementIntent{
        .{
            .record = .{
                .group_id = 4101,
                .replica_id = 1,
                .local_node_id = 1,
                .bootstrap_mode = .persisted,
            },
            .store_id = 1,
            .peer_node_ids = &.{1},
        },
        .{
            .record = .{
                .group_id = 4102,
                .replica_id = 1,
                .local_node_id = 1,
                .bootstrap_mode = .persisted,
            },
            .store_id = 1,
            .peer_node_ids = &.{1},
        },
    };
    const stores = [_]table_manager.StoreRecord{
        .{
            .store_id = 1,
            .node_id = 1,
            .role = "data",
            .health_class = "healthy",
            .failure_domain = "rack-a",
            .live = true,
            .group_statuses = @constCast((&[_]table_manager.GroupStatusReport{
                .{
                    .group_id = 4101,
                    .doc_count = 10,
                    .disk_bytes = 20,
                    .empty = false,
                    .updated_at_millis = now_ms,
                    .local_leader = true,
                    .local_voter = true,
                    .voter_count = 1,
                    .voter_set_known = true,
                    .voter_set_fingerprint = voter_set_fingerprint,
                },
                .{
                    .group_id = 4102,
                    .doc_count = 8,
                    .disk_bytes = 15,
                    .empty = false,
                    .updated_at_millis = now_ms,
                    .local_leader = true,
                    .local_voter = true,
                    .voter_count = 1,
                    .voter_set_known = true,
                    .voter_set_fingerprint = voter_set_fingerprint,
                },
            })[0..]),
        },
    };

    var reconciler = Reconciler.initWithConfig(std.testing.allocator, .{
        .max_shard_size_bytes = 100,
        .min_shard_size_bytes = 30,
        .min_shards_per_table = 1,
        .max_shards_per_table = 8,
    });
    var plan = try reconciler.computePlan(&manager, &.{}, &.{}, .{
        .tables = tables,
        .ranges = ranges,
        .placement_intents = &placements,
        .stores = &stores,
    });
    defer plan.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 1), plan.merge_upserts.len);
    try std.testing.expectEqual(@as(u64, 4102), plan.merge_upserts[0].donor_group_id);
    try std.testing.expectEqual(@as(u64, 4101), plan.merge_upserts[0].receiver_group_id);
}

test "metadata reconciler keeps structurally valid automatic merge intent across transient recompute miss" {
    var manager = table_manager.TableManager.init(std.testing.allocator);
    defer manager.deinit();

    try manager.upsertTable(.{ .table_id = 411, .name = "docs", .min_ranges = 1 });
    try manager.upsertRange(.{
        .group_id = 4111,
        .table_id = 411,
        .start_key = "doc:a",
        .end_key = "doc:m",
    });
    try manager.upsertRange(.{
        .group_id = 4112,
        .table_id = 411,
        .start_key = "doc:m",
        .end_key = "doc:z",
    });
    try manager.requestMerge(.{
        .transition_id = 41101,
        .table_id = 411,
        .donor_group_id = 4112,
        .receiver_group_id = 4111,
        .automatic = true,
    });

    try pruneAutomaticIntents(std.testing.allocator, &manager, .{}, &.{}, &.{});
    try std.testing.expectEqual(@as(u32, 1), manager.merge_intents.count());

    try manager.upsertRange(.{
        .group_id = 4112,
        .table_id = 411,
        .start_key = "doc:n",
        .end_key = "doc:z",
    });
    try pruneAutomaticIntents(std.testing.allocator, &manager, .{}, &.{}, &.{});
    try std.testing.expectEqual(@as(u32, 0), manager.merge_intents.count());
}

test "metadata reconciler does not automatically merge incompatible doc identity namespaces" {
    var compatible_left = MergedGroupStatus{
        .group_id = 4101,
        .doc_identity = .{
            .namespace_table_id = 410,
            .namespace_shard_id = 4101,
            .namespace_range_id = 4101,
            .allocated_ordinals = 1,
        },
    };
    const compatible_right = MergedGroupStatus{
        .group_id = 4102,
        .doc_identity = .{
            .namespace_table_id = 410,
            .namespace_shard_id = 4101,
            .namespace_range_id = 4101,
            .allocated_ordinals = 1,
        },
    };
    try std.testing.expect(docIdentityNamespacesCompatibleForAutomaticMerge(compatible_left, compatible_right));
    compatible_left.doc_identity.ordinal_capacity_exhausted = true;
    try std.testing.expect(!docIdentityNamespacesCompatibleForAutomaticMerge(compatible_left, compatible_right));

    var manager = table_manager.TableManager.init(std.testing.allocator);
    defer manager.deinit();

    try manager.upsertTable(.{ .table_id = 411, .name = "docs", .min_ranges = 1 });
    try manager.upsertRange(.{
        .group_id = 4111,
        .range_id = 1001,
        .table_id = 411,
        .start_key = "doc:a",
        .end_key = "doc:m",
    });
    try manager.upsertRange(.{
        .group_id = 4112,
        .range_id = 1002,
        .table_id = 411,
        .start_key = "doc:m",
        .end_key = "doc:z",
    });

    const tables = try manager.listTables(std.testing.allocator);
    defer manager.freeTables(std.testing.allocator, tables);
    const ranges = try manager.listRanges(std.testing.allocator);
    defer manager.freeRanges(std.testing.allocator, ranges);

    const now_ms = platform_clock.Clock.real().nowRealtimeMs();
    const updated_at_ns = now_ms * std.time.ns_per_ms;
    const stores = [_]table_manager.StoreRecord{
        .{
            .store_id = 1,
            .node_id = 1,
            .role = "data",
            .health_class = "healthy",
            .failure_domain = "rack-a",
            .live = true,
            .group_statuses = @constCast((&[_]table_manager.GroupStatusReport{
                .{
                    .group_id = 4111,
                    .doc_count = 10,
                    .disk_bytes = 20,
                    .empty = false,
                    .updated_at_millis = now_ms,
                    .local_leader = true,
                },
                .{
                    .group_id = 4112,
                    .doc_count = 8,
                    .disk_bytes = 15,
                    .empty = false,
                    .updated_at_millis = now_ms,
                    .local_leader = true,
                },
            })[0..]),
            .runtime_statuses = @constCast((&[_]table_manager.RuntimeGroupStatusReport{
                .{
                    .table_id = 411,
                    .table_name = "docs",
                    .group_id = 4111,
                    .updated_at_ns = updated_at_ns,
                    .doc_identity = .{
                        .namespace_table_id = 411,
                        .namespace_shard_id = 4111,
                        .namespace_range_id = 1001,
                        .next_ordinal = 11,
                        .allocated_ordinals = 10,
                        .live_ordinals = 10,
                    },
                },
                .{
                    .table_id = 411,
                    .table_name = "docs",
                    .group_id = 4112,
                    .updated_at_ns = updated_at_ns,
                    .doc_identity = .{
                        .namespace_table_id = 411,
                        .namespace_shard_id = 4112,
                        .namespace_range_id = 1002,
                        .next_ordinal = 9,
                        .allocated_ordinals = 8,
                        .live_ordinals = 8,
                    },
                },
            })[0..]),
        },
    };

    var reconciler = Reconciler.initWithConfig(std.testing.allocator, .{
        .max_shard_size_bytes = 100,
        .min_shard_size_bytes = 30,
        .min_shards_per_table = 1,
        .max_shards_per_table = 8,
    });
    var plan = try reconciler.computePlan(&manager, &.{}, &.{}, .{
        .tables = tables,
        .ranges = ranges,
        .stores = &stores,
    });
    defer plan.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 0), plan.merge_upserts.len);
}

test "metadata reconciler does not automatically merge stale doc identity range namespace" {
    var manager = table_manager.TableManager.init(std.testing.allocator);
    defer manager.deinit();

    try manager.upsertTable(.{ .table_id = 415, .name = "docs", .min_ranges = 1 });
    try manager.upsertRange(.{
        .group_id = 4151,
        .range_id = 5001,
        .table_id = 415,
        .start_key = "doc:a",
        .end_key = "doc:m",
    });
    try manager.upsertRange(.{
        .group_id = 4152,
        .range_id = 5002,
        .table_id = 415,
        .start_key = "doc:m",
        .end_key = "doc:z",
    });

    const tables = try manager.listTables(std.testing.allocator);
    defer manager.freeTables(std.testing.allocator, tables);
    const ranges = try manager.listRanges(std.testing.allocator);
    defer manager.freeRanges(std.testing.allocator, ranges);

    const now_ms = platform_clock.Clock.real().nowRealtimeMs();
    const updated_at_ns = now_ms * std.time.ns_per_ms;
    const stores = [_]table_manager.StoreRecord{
        .{
            .store_id = 1,
            .node_id = 1,
            .role = "data",
            .health_class = "healthy",
            .failure_domain = "rack-a",
            .live = true,
            .group_statuses = @constCast((&[_]table_manager.GroupStatusReport{
                .{
                    .group_id = 4151,
                    .doc_count = 10,
                    .disk_bytes = 20,
                    .empty = false,
                    .updated_at_millis = now_ms,
                    .local_leader = true,
                },
                .{
                    .group_id = 4152,
                    .doc_count = 8,
                    .disk_bytes = 15,
                    .empty = false,
                    .updated_at_millis = now_ms,
                    .local_leader = true,
                },
            })[0..]),
            .runtime_statuses = @constCast((&[_]table_manager.RuntimeGroupStatusReport{
                .{
                    .table_id = 415,
                    .table_name = "docs",
                    .group_id = 4151,
                    .updated_at_ns = updated_at_ns,
                    .doc_identity = .{
                        .namespace_table_id = 415,
                        .namespace_shard_id = 4151,
                        .namespace_range_id = 5001,
                        .next_ordinal = 11,
                        .allocated_ordinals = 10,
                        .live_ordinals = 10,
                    },
                },
                .{
                    .table_id = 415,
                    .table_name = "docs",
                    .group_id = 4152,
                    .updated_at_ns = updated_at_ns,
                    .doc_identity = .{
                        .namespace_table_id = 415,
                        .namespace_shard_id = 4151,
                        .namespace_range_id = 5001,
                        .next_ordinal = 9,
                        .allocated_ordinals = 8,
                        .live_ordinals = 8,
                    },
                },
            })[0..]),
        },
    };

    var reconciler = Reconciler.initWithConfig(std.testing.allocator, .{
        .max_shard_size_bytes = 100,
        .min_shard_size_bytes = 30,
        .min_shards_per_table = 1,
        .max_shards_per_table = 8,
    });
    var plan = try reconciler.computePlan(&manager, &.{}, &.{}, .{
        .tables = tables,
        .ranges = ranges,
        .stores = &stores,
    });
    defer plan.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 0), plan.merge_upserts.len);
}

test "metadata reconciler allows explicit merge with doc identity reassignment opt-in" {
    var manager = table_manager.TableManager.init(std.testing.allocator);
    defer manager.deinit();

    try manager.upsertTable(.{ .table_id = 414, .name = "docs", .min_ranges = 1 });
    try manager.upsertRange(.{
        .group_id = 4141,
        .range_id = 3001,
        .table_id = 414,
        .start_key = "doc:a",
        .end_key = "doc:m",
    });
    try manager.upsertRange(.{
        .group_id = 4142,
        .range_id = 3002,
        .table_id = 414,
        .start_key = "doc:m",
        .end_key = "doc:z",
    });
    try manager.requestMerge(.{
        .transition_id = 41401,
        .table_id = 414,
        .donor_group_id = 4142,
        .receiver_group_id = 4141,
        .allow_doc_identity_reassignment = true,
    });

    const tables = try manager.listTables(std.testing.allocator);
    defer manager.freeTables(std.testing.allocator, tables);
    const ranges = try manager.listRanges(std.testing.allocator);
    defer manager.freeRanges(std.testing.allocator, ranges);

    const now_ms = platform_clock.Clock.real().nowRealtimeMs();
    const statuses = [_]MergedGroupStatus{
        .{
            .group_id = 4141,
            .doc_count = 10,
            .disk_bytes = 10,
            .empty = false,
            .updated_at_millis = now_ms,
            .doc_identity = .{
                .namespace_table_id = 414,
                .namespace_shard_id = 4141,
                .namespace_range_id = 3001,
                .next_ordinal = 11,
                .allocated_ordinals = 10,
                .live_ordinals = 10,
            },
        },
        .{
            .group_id = 4142,
            .doc_count = 9,
            .disk_bytes = 9,
            .empty = false,
            .updated_at_millis = now_ms,
            .doc_identity = .{
                .namespace_table_id = 414,
                .namespace_shard_id = 4142,
                .namespace_range_id = 3002,
                .next_ordinal = 10,
                .allocated_ordinals = 9,
                .live_ordinals = 9,
            },
        },
    };

    var reconciler = Reconciler.init(std.testing.allocator);
    var plan = try reconciler.computePlan(&manager, &.{}, &.{}, .{
        .tables = tables,
        .ranges = ranges,
        .merged_group_statuses = &statuses,
        .merge_transitions = &.{},
    });
    defer plan.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 1), plan.merge_upserts.len);
    try std.testing.expectEqual(@as(u64, 41401), plan.merge_upserts[0].transition_id);
    try std.testing.expect(plan.merge_upserts[0].allow_doc_identity_reassignment);

    var missing_status_plan = try reconciler.computePlan(&manager, &.{}, &.{}, .{
        .tables = tables,
        .ranges = ranges,
        .merged_group_statuses = statuses[0..1],
        .merge_transitions = &.{},
    });
    defer missing_status_plan.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 0), missing_status_plan.merge_upserts.len);

    var blocked_statuses = statuses;
    blocked_statuses[1].doc_identity.rebuild_required = true;
    var blocked_plan = try reconciler.computePlan(&manager, &.{}, &.{}, .{
        .tables = tables,
        .ranges = ranges,
        .merged_group_statuses = &blocked_statuses,
        .merge_transitions = &.{},
    });
    defer blocked_plan.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 0), blocked_plan.merge_upserts.len);

    var exhausted_statuses = statuses;
    exhausted_statuses[0].doc_identity.ordinal_capacity_exhausted = true;
    var exhausted_plan = try reconciler.computePlan(&manager, &.{}, &.{}, .{
        .tables = tables,
        .ranges = ranges,
        .merged_group_statuses = &exhausted_statuses,
        .merge_transitions = &.{},
    });
    defer exhausted_plan.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 0), exhausted_plan.merge_upserts.len);
}

test "metadata reconciler blocks merge replay when one side lacks doc identity status" {
    var manager = table_manager.TableManager.init(std.testing.allocator);
    defer manager.deinit();

    try manager.upsertTable(.{ .table_id = 416, .name = "docs", .min_ranges = 1 });
    try manager.upsertRange(.{
        .group_id = 4161,
        .range_id = 6001,
        .doc_identity_shard_id = 4161,
        .doc_identity_range_id = 6001,
        .table_id = 416,
        .start_key = "doc:a",
        .end_key = "doc:m",
    });
    try manager.upsertRange(.{
        .group_id = 4162,
        .range_id = 6002,
        .doc_identity_shard_id = 4161,
        .doc_identity_range_id = 6001,
        .table_id = 416,
        .start_key = "doc:m",
        .end_key = "doc:z",
    });
    try manager.requestMerge(.{
        .transition_id = 41601,
        .table_id = 416,
        .donor_group_id = 4162,
        .receiver_group_id = 4161,
    });

    const tables = try manager.listTables(std.testing.allocator);
    defer manager.freeTables(std.testing.allocator, tables);
    const ranges = try manager.listRanges(std.testing.allocator);
    defer manager.freeRanges(std.testing.allocator, ranges);
    const desired_merges = try manager.listDesiredMergeTransitions(std.testing.allocator);
    defer manager.freeMergeTransitions(std.testing.allocator, desired_merges);

    const statuses = [_]MergedGroupStatus{.{
        .group_id = 4161,
        .doc_count = 10,
        .disk_bytes = 10,
        .empty = false,
        .updated_at_millis = monotonicMillis(),
        .doc_identity = .{
            .namespace_table_id = 416,
            .namespace_shard_id = 4161,
            .namespace_range_id = 6001,
            .next_ordinal = 11,
            .allocated_ordinals = 10,
            .live_ordinals = 10,
        },
    }};

    var reconciler = Reconciler.init(std.testing.allocator);
    var plan = try reconciler.computePlan(&manager, &.{}, &.{}, .{
        .tables = tables,
        .ranges = ranges,
        .merged_group_statuses = &statuses,
        .merge_transitions = &.{},
    });
    defer plan.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 0), plan.merge_upserts.len);
    try std.testing.expectEqual(@as(usize, 0), plan.merge_steps.len);

    var replay_plan = try reconciler.computePlan(&manager, &.{}, &.{}, .{
        .tables = tables,
        .ranges = ranges,
        .merged_group_statuses = &statuses,
        .merge_transitions = desired_merges,
    });
    defer replay_plan.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 1), replay_plan.merge_upserts.len);
    try std.testing.expectEqual(@as(usize, 0), replay_plan.merge_steps.len);
    try std.testing.expectEqualStrings(doc_identity_merge_rollback_reason, replay_plan.merge_upserts[0].rollback_reason.?);
}

test "metadata reconciler rolls back existing merge with incompatible doc identity namespaces" {
    var manager = table_manager.TableManager.init(std.testing.allocator);
    defer manager.deinit();

    try manager.upsertTable(.{ .table_id = 413, .name = "docs", .min_ranges = 1 });
    try manager.upsertRange(.{
        .group_id = 4131,
        .range_id = 2001,
        .doc_identity_shard_id = 4131,
        .doc_identity_range_id = 2001,
        .table_id = 413,
        .start_key = "doc:a",
        .end_key = "doc:m",
    });
    try manager.upsertRange(.{
        .group_id = 4132,
        .range_id = 2002,
        .doc_identity_shard_id = 4131,
        .doc_identity_range_id = 2001,
        .table_id = 413,
        .start_key = "doc:m",
        .end_key = "doc:z",
    });
    try manager.requestMerge(.{
        .transition_id = 41301,
        .table_id = 413,
        .donor_group_id = 4132,
        .receiver_group_id = 4131,
    });

    const tables = try manager.listTables(std.testing.allocator);
    defer manager.freeTables(std.testing.allocator, tables);
    const ranges = try manager.listRanges(std.testing.allocator);
    defer manager.freeRanges(std.testing.allocator, ranges);
    const desired_merges = try manager.listDesiredMergeTransitions(std.testing.allocator);
    defer manager.freeMergeTransitions(std.testing.allocator, desired_merges);

    const now_ms = platform_clock.Clock.real().nowRealtimeMs();
    const statuses = [_]MergedGroupStatus{
        .{
            .group_id = 4131,
            .doc_count = 10,
            .disk_bytes = 10,
            .empty = false,
            .updated_at_millis = now_ms,
            .doc_identity = .{
                .namespace_table_id = 413,
                .namespace_shard_id = 4131,
                .namespace_range_id = 2001,
                .next_ordinal = 11,
                .allocated_ordinals = 10,
                .live_ordinals = 10,
            },
        },
        .{
            .group_id = 4132,
            .doc_count = 9,
            .disk_bytes = 9,
            .empty = false,
            .updated_at_millis = now_ms,
            .doc_identity = .{
                .namespace_table_id = 413,
                .namespace_shard_id = 4132,
                .namespace_range_id = 2002,
                .next_ordinal = 10,
                .allocated_ordinals = 9,
                .live_ordinals = 9,
            },
        },
    };

    var reconciler = Reconciler.init(std.testing.allocator);
    var plan = try reconciler.computePlan(&manager, &.{}, &.{}, .{
        .tables = tables,
        .ranges = ranges,
        .merged_group_statuses = &statuses,
        .merge_transitions = desired_merges,
    });
    defer plan.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 1), plan.merge_upserts.len);
    try std.testing.expectEqual(@as(usize, 0), plan.merge_steps.len);
    try std.testing.expectEqual(@as(u64, 41301), plan.merge_upserts[0].transition_id);
    try std.testing.expectEqualStrings(doc_identity_merge_rollback_reason, plan.merge_upserts[0].rollback_reason.?);
}

test "metadata reconciler does not merge when a replica is missing healthy group status" {
    var manager = table_manager.TableManager.init(std.testing.allocator);
    defer manager.deinit();

    try manager.upsertTable(.{ .table_id = 412, .name = "docs", .min_ranges = 1 });
    try manager.upsertRange(.{
        .group_id = 4121,
        .table_id = 412,
        .start_key = "doc:a",
        .end_key = "doc:m",
    });
    try manager.upsertRange(.{
        .group_id = 4122,
        .table_id = 412,
        .start_key = "doc:m",
        .end_key = "doc:z",
    });

    const tables = try manager.listTables(std.testing.allocator);
    defer manager.freeTables(std.testing.allocator, tables);
    const ranges = try manager.listRanges(std.testing.allocator);
    defer manager.freeRanges(std.testing.allocator, ranges);

    const now_ms = platform_clock.Clock.real().nowRealtimeMs();
    const stores = [_]table_manager.StoreRecord{
        .{
            .store_id = 1,
            .node_id = 1,
            .role = "data",
            .health_class = "healthy",
            .failure_domain = "rack-a",
            .live = true,
            .group_statuses = @constCast((&[_]table_manager.GroupStatusReport{
                .{
                    .group_id = 4121,
                    .doc_count = 10,
                    .disk_bytes = 20,
                    .empty = false,
                    .updated_at_millis = now_ms,
                    .local_leader = true,
                },
                .{
                    .group_id = 4122,
                    .doc_count = 8,
                    .disk_bytes = 15,
                    .empty = false,
                    .updated_at_millis = now_ms,
                    .local_leader = true,
                },
            })[0..]),
        },
    };
    const intents = [_]raft_reconciler.PlacementIntent{
        .{ .record = .{ .group_id = 4121, .replica_id = 1, .local_node_id = 1, .bootstrap_mode = .persisted }, .store_id = 1, .peer_node_ids = &.{2} },
        .{ .record = .{ .group_id = 4121, .replica_id = 2, .local_node_id = 2, .bootstrap_mode = .persisted }, .store_id = 2, .peer_node_ids = &.{1} },
        .{ .record = .{ .group_id = 4122, .replica_id = 1, .local_node_id = 1, .bootstrap_mode = .persisted }, .store_id = 1, .peer_node_ids = &.{2} },
        .{ .record = .{ .group_id = 4122, .replica_id = 2, .local_node_id = 2, .bootstrap_mode = .persisted }, .store_id = 2, .peer_node_ids = &.{1} },
    };

    var reconciler = Reconciler.initWithConfig(std.testing.allocator, .{
        .max_shard_size_bytes = 100,
        .min_shard_size_bytes = 30,
        .min_shards_per_table = 1,
        .max_shards_per_table = 8,
    });
    var plan = try reconciler.computePlan(&manager, &.{}, &.{}, .{
        .placement_intents = &intents,
        .tables = tables,
        .ranges = ranges,
        .stores = &stores,
    });
    defer plan.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 0), plan.merge_upserts.len);
}

test "metadata reconciler does not merge when authoritative voter reports are incomplete" {
    var manager = table_manager.TableManager.init(std.testing.allocator);
    defer manager.deinit();

    try manager.upsertTable(.{ .table_id = 413, .name = "docs", .min_ranges = 1 });
    try manager.upsertRange(.{
        .group_id = 4131,
        .table_id = 413,
        .start_key = "doc:a",
        .end_key = "doc:m",
    });
    try manager.upsertRange(.{
        .group_id = 4132,
        .table_id = 413,
        .start_key = "doc:m",
        .end_key = "doc:z",
    });

    const tables = try manager.listTables(std.testing.allocator);
    defer manager.freeTables(std.testing.allocator, tables);
    const ranges = try manager.listRanges(std.testing.allocator);
    defer manager.freeRanges(std.testing.allocator, ranges);

    const now_ms = platform_clock.Clock.real().nowRealtimeMs();
    const stores = [_]table_manager.StoreRecord{
        .{
            .store_id = 1,
            .node_id = 1,
            .role = "data",
            .health_class = "healthy",
            .failure_domain = "rack-a",
            .live = true,
            .group_statuses = @constCast((&[_]table_manager.GroupStatusReport{
                .{
                    .group_id = 4131,
                    .doc_count = 10,
                    .disk_bytes = 20,
                    .empty = false,
                    .updated_at_millis = now_ms,
                    .local_leader = true,
                    .local_voter = true,
                    .voter_count = 2,
                },
                .{
                    .group_id = 4132,
                    .doc_count = 8,
                    .disk_bytes = 15,
                    .empty = false,
                    .updated_at_millis = now_ms,
                    .local_leader = true,
                    .local_voter = true,
                    .voter_count = 2,
                },
            })[0..]),
        },
    };

    var reconciler = Reconciler.initWithConfig(std.testing.allocator, .{
        .max_shard_size_bytes = 100,
        .min_shard_size_bytes = 30,
        .min_shards_per_table = 1,
        .max_shards_per_table = 8,
    });
    var plan = try reconciler.computePlan(&manager, &.{}, &.{}, .{
        .tables = tables,
        .ranges = ranges,
        .stores = &stores,
    });
    defer plan.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 0), plan.merge_upserts.len);
}

test "metadata reconciler does not merge under-replicated groups when placement intents expect more voters" {
    var manager = table_manager.TableManager.init(std.testing.allocator);
    defer manager.deinit();

    try manager.upsertTable(.{ .table_id = 4133, .name = "docs", .min_ranges = 1 });
    try manager.upsertRange(.{
        .group_id = 41331,
        .table_id = 4133,
        .start_key = "doc:a",
        .end_key = "doc:m",
    });
    try manager.upsertRange(.{
        .group_id = 41332,
        .table_id = 4133,
        .start_key = "doc:m",
        .end_key = "doc:z",
    });

    const tables = try manager.listTables(std.testing.allocator);
    defer manager.freeTables(std.testing.allocator, tables);
    const ranges = try manager.listRanges(std.testing.allocator);
    defer manager.freeRanges(std.testing.allocator, ranges);

    const now_ms = platform_clock.Clock.real().nowRealtimeMs();
    const stores = [_]table_manager.StoreRecord{
        .{
            .store_id = 1,
            .node_id = 1,
            .role = "data",
            .health_class = "healthy",
            .failure_domain = "rack-a",
            .live = true,
            .group_statuses = @constCast((&[_]table_manager.GroupStatusReport{
                .{
                    .group_id = 41331,
                    .doc_count = 10,
                    .disk_bytes = 20,
                    .empty = false,
                    .updated_at_millis = now_ms,
                    .local_leader = true,
                    .local_voter = true,
                    .voter_count = 2,
                },
            })[0..]),
        },
        .{
            .store_id = 2,
            .node_id = 2,
            .role = "data",
            .health_class = "healthy",
            .failure_domain = "rack-b",
            .live = true,
            .group_statuses = @constCast((&[_]table_manager.GroupStatusReport{
                .{
                    .group_id = 41331,
                    .updated_at_millis = now_ms,
                    .local_voter = true,
                    .voter_count = 2,
                },
                .{
                    .group_id = 41332,
                    .doc_count = 8,
                    .disk_bytes = 15,
                    .empty = false,
                    .updated_at_millis = now_ms,
                    .local_leader = true,
                    .local_voter = true,
                    .voter_count = 2,
                },
            })[0..]),
        },
        .{
            .store_id = 3,
            .node_id = 3,
            .role = "data",
            .health_class = "healthy",
            .failure_domain = "rack-c",
            .live = true,
            .group_statuses = @constCast((&[_]table_manager.GroupStatusReport{
                .{
                    .group_id = 41332,
                    .updated_at_millis = now_ms,
                    .local_voter = true,
                    .voter_count = 2,
                },
            })[0..]),
        },
    };
    const intents = [_]raft_reconciler.PlacementIntent{
        .{ .record = .{ .group_id = 41331, .replica_id = 1, .local_node_id = 1, .bootstrap_mode = .persisted }, .store_id = 1, .peer_node_ids = &.{ 2, 3 } },
        .{ .record = .{ .group_id = 41331, .replica_id = 2, .local_node_id = 2, .bootstrap_mode = .persisted }, .store_id = 2, .peer_node_ids = &.{ 1, 3 } },
        .{ .record = .{ .group_id = 41331, .replica_id = 3, .local_node_id = 3, .bootstrap_mode = .persisted }, .store_id = 3, .peer_node_ids = &.{ 1, 2 } },
        .{ .record = .{ .group_id = 41332, .replica_id = 1, .local_node_id = 1, .bootstrap_mode = .persisted }, .store_id = 1, .peer_node_ids = &.{ 2, 3 } },
        .{ .record = .{ .group_id = 41332, .replica_id = 2, .local_node_id = 2, .bootstrap_mode = .persisted }, .store_id = 2, .peer_node_ids = &.{ 1, 3 } },
        .{ .record = .{ .group_id = 41332, .replica_id = 3, .local_node_id = 3, .bootstrap_mode = .persisted }, .store_id = 3, .peer_node_ids = &.{ 1, 2 } },
    };

    var reconciler = Reconciler.initWithConfig(std.testing.allocator, .{
        .max_shard_size_bytes = 100,
        .min_shard_size_bytes = 30,
        .min_shards_per_table = 1,
        .max_shards_per_table = 8,
    });
    var plan = try reconciler.computePlan(&manager, &.{}, &.{}, .{
        .placement_intents = &intents,
        .tables = tables,
        .ranges = ranges,
        .stores = &stores,
    });
    defer plan.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 0), plan.merge_upserts.len);
}

test "metadata reconciler does not merge shards that are younger than the merge age threshold" {
    var manager = table_manager.TableManager.init(std.testing.allocator);
    defer manager.deinit();

    try manager.upsertTable(.{ .table_id = 410, .name = "docs", .min_ranges = 1 });
    try manager.upsertRange(.{
        .group_id = 41011,
        .range_id = 41011,
        .table_id = 410,
        .start_key = "doc:a",
        .end_key = "doc:m",
        .doc_identity_shard_id = 41011,
        .doc_identity_range_id = 41011,
    });
    try manager.upsertRange(.{
        .group_id = 41012,
        .range_id = 41012,
        .table_id = 410,
        .start_key = "doc:m",
        .end_key = "doc:z",
        .doc_identity_shard_id = 41011,
        .doc_identity_range_id = 41011,
    });

    const tables = try manager.listTables(std.testing.allocator);
    defer manager.freeTables(std.testing.allocator, tables);
    const ranges = try manager.listRanges(std.testing.allocator);
    defer manager.freeRanges(std.testing.allocator, ranges);

    var manual_clock = platform_clock.ManualClock{};
    manual_clock.setRealtimeNs(10 * 60 * std.time.ns_per_s);
    const now_realtime_ms = manual_clock.clock().nowRealtimeMs();
    const voter_set_fingerprint = table_manager.voterSetFingerprint(&.{1}, null);
    const placements = [_]raft_reconciler.PlacementIntent{
        .{ .record = .{ .group_id = 41111, .replica_id = 1, .local_node_id = 1 }, .store_id = 1, .peer_node_ids = &.{1} },
        .{ .record = .{ .group_id = 41112, .replica_id = 2, .local_node_id = 1 }, .store_id = 1, .peer_node_ids = &.{1} },
    };
    const stores = [_]table_manager.StoreRecord{
        .{
            .store_id = 1,
            .node_id = 1,
            .role = "data",
            .health_class = "healthy",
            .failure_domain = "rack-a",
            .live = true,
            .group_statuses = @constCast((&[_]table_manager.GroupStatusReport{
                .{
                    .group_id = 41011,
                    .doc_count = 10,
                    .disk_bytes = 20,
                    .empty = false,
                    .created_at_millis = now_realtime_ms - 30 * std.time.ms_per_s,
                    .updated_at_millis = now_realtime_ms,
                    .local_leader = true,
                    .local_voter = true,
                    .voter_count = 1,
                    .voter_set_known = true,
                    .voter_set_fingerprint = voter_set_fingerprint,
                },
                .{
                    .group_id = 41012,
                    .doc_count = 8,
                    .disk_bytes = 15,
                    .empty = false,
                    .created_at_millis = now_realtime_ms - 30 * std.time.ms_per_s,
                    .updated_at_millis = now_realtime_ms,
                    .local_leader = true,
                    .local_voter = true,
                    .voter_count = 1,
                    .voter_set_known = true,
                    .voter_set_fingerprint = voter_set_fingerprint,
                },
            })[0..]),
        },
    };

    var reconciler = Reconciler.initWithConfig(std.testing.allocator, .{
        .max_shard_size_bytes = 100,
        .min_shard_size_bytes = 30,
        .min_shards_per_table = 1,
        .max_shards_per_table = 8,
        .min_shard_merge_age_millis = 60 * std.time.ms_per_s,
        .clock = manual_clock.clock(),
    });
    var plan = try reconciler.computePlan(&manager, &.{}, &.{}, .{
        .tables = tables,
        .ranges = ranges,
        .placement_intents = &placements,
        .stores = &stores,
    });
    defer plan.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 0), plan.merge_upserts.len);
}

test "metadata reconciler merges shards once they are older than the merge age threshold" {
    var manager = table_manager.TableManager.init(std.testing.allocator);
    defer manager.deinit();

    try manager.upsertTable(.{ .table_id = 411, .name = "docs", .min_ranges = 1 });
    try manager.upsertRange(.{
        .group_id = 41111,
        .range_id = 41111,
        .table_id = 411,
        .start_key = "doc:a",
        .end_key = "doc:m",
        .doc_identity_shard_id = 41111,
        .doc_identity_range_id = 41111,
    });
    try manager.upsertRange(.{
        .group_id = 41112,
        .range_id = 41112,
        .table_id = 411,
        .start_key = "doc:m",
        .end_key = "doc:z",
        .doc_identity_shard_id = 41111,
        .doc_identity_range_id = 41111,
    });

    const tables = try manager.listTables(std.testing.allocator);
    defer manager.freeTables(std.testing.allocator, tables);
    const ranges = try manager.listRanges(std.testing.allocator);
    defer manager.freeRanges(std.testing.allocator, ranges);

    var manual_clock = platform_clock.ManualClock{};
    manual_clock.setRealtimeNs(10 * 60 * std.time.ns_per_s);
    const now_realtime_ms = manual_clock.clock().nowRealtimeMs();
    const voter_set_fingerprint = table_manager.voterSetFingerprint(&.{1}, null);
    const placements = [_]raft_reconciler.PlacementIntent{
        .{ .record = .{ .group_id = 41111, .replica_id = 1, .local_node_id = 1 }, .store_id = 1, .peer_node_ids = &.{1} },
        .{ .record = .{ .group_id = 41112, .replica_id = 2, .local_node_id = 1 }, .store_id = 1, .peer_node_ids = &.{1} },
    };
    const stores = [_]table_manager.StoreRecord{
        .{
            .store_id = 1,
            .node_id = 1,
            .role = "data",
            .health_class = "healthy",
            .failure_domain = "rack-a",
            .live = true,
            .group_statuses = @constCast((&[_]table_manager.GroupStatusReport{
                .{
                    .group_id = 41111,
                    .doc_count = 10,
                    .disk_bytes = 20,
                    .empty = false,
                    .created_at_millis = now_realtime_ms - 2 * 60 * std.time.ms_per_s,
                    .updated_at_millis = now_realtime_ms,
                    .local_leader = true,
                    .local_voter = true,
                    .voter_count = 1,
                    .voter_set_known = true,
                    .voter_set_fingerprint = voter_set_fingerprint,
                },
                .{
                    .group_id = 41112,
                    .doc_count = 8,
                    .disk_bytes = 15,
                    .empty = false,
                    .created_at_millis = now_realtime_ms - 2 * 60 * std.time.ms_per_s,
                    .updated_at_millis = now_realtime_ms,
                    .local_leader = true,
                    .local_voter = true,
                    .voter_count = 1,
                    .voter_set_known = true,
                    .voter_set_fingerprint = voter_set_fingerprint,
                },
            })[0..]),
        },
    };

    var reconciler = Reconciler.initWithConfig(std.testing.allocator, .{
        .max_shard_size_bytes = 100,
        .min_shard_size_bytes = 30,
        .min_shards_per_table = 1,
        .max_shards_per_table = 8,
        .min_shard_merge_age_millis = 60 * std.time.ms_per_s,
        .clock = manual_clock.clock(),
    });
    var plan = try reconciler.computePlan(&manager, &.{}, &.{}, .{
        .tables = tables,
        .ranges = ranges,
        .placement_intents = &placements,
        .stores = &stores,
    });
    defer plan.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 1), plan.merge_upserts.len);
    try std.testing.expectEqual(@as(u64, 41112), plan.merge_upserts[0].donor_group_id);
    try std.testing.expectEqual(@as(u64, 41111), plan.merge_upserts[0].receiver_group_id);
}

test "metadata reconciler does not split past max shards per table" {
    var manager = table_manager.TableManager.init(std.testing.allocator);
    defer manager.deinit();

    try manager.upsertTable(.{ .table_id = 411, .name = "docs", .min_ranges = 1 });
    try manager.upsertRange(.{
        .group_id = 4111,
        .table_id = 411,
        .start_key = "doc:a",
        .end_key = "doc:z",
    });

    const tables = try manager.listTables(std.testing.allocator);
    defer manager.freeTables(std.testing.allocator, tables);
    const ranges = try manager.listRanges(std.testing.allocator);
    defer manager.freeRanges(std.testing.allocator, ranges);

    const now_ms = platform_clock.Clock.real().nowRealtimeMs();
    const stores = [_]table_manager.StoreRecord{
        .{
            .store_id = 1,
            .node_id = 1,
            .role = "data",
            .health_class = "healthy",
            .failure_domain = "rack-a",
            .live = true,
            .group_statuses = @constCast((&[_]table_manager.GroupStatusReport{
                .{
                    .group_id = 4111,
                    .doc_count = 200,
                    .disk_bytes = 200,
                    .empty = false,
                    .updated_at_millis = now_ms,
                    .local_leader = true,
                },
            })[0..]),
        },
    };

    var reconciler = Reconciler.initWithConfig(std.testing.allocator, .{
        .max_shard_size_bytes = 100,
        .max_shards_per_table = 1,
    });
    var plan = try reconciler.computePlan(&manager, &.{}, &.{}, .{
        .tables = tables,
        .ranges = ranges,
        .stores = &stores,
    });
    defer plan.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 0), plan.split_upserts.len);
}

test "metadata reconciler does not merge below min shards per table" {
    var manager = table_manager.TableManager.init(std.testing.allocator);
    defer manager.deinit();

    try manager.upsertTable(.{ .table_id = 412, .name = "docs", .min_ranges = 1 });
    try manager.upsertRange(.{
        .group_id = 4121,
        .table_id = 412,
        .start_key = "doc:a",
        .end_key = "doc:m",
    });
    try manager.upsertRange(.{
        .group_id = 4122,
        .table_id = 412,
        .start_key = "doc:m",
        .end_key = "doc:z",
    });

    const tables = try manager.listTables(std.testing.allocator);
    defer manager.freeTables(std.testing.allocator, tables);
    const ranges = try manager.listRanges(std.testing.allocator);
    defer manager.freeRanges(std.testing.allocator, ranges);

    const now_ms = platform_clock.Clock.real().nowRealtimeMs();
    const stores = [_]table_manager.StoreRecord{
        .{
            .store_id = 1,
            .node_id = 1,
            .role = "data",
            .health_class = "healthy",
            .failure_domain = "rack-a",
            .live = true,
            .group_statuses = @constCast((&[_]table_manager.GroupStatusReport{
                .{
                    .group_id = 4121,
                    .doc_count = 10,
                    .disk_bytes = 20,
                    .empty = false,
                    .updated_at_millis = now_ms,
                    .local_leader = true,
                },
                .{
                    .group_id = 4122,
                    .doc_count = 8,
                    .disk_bytes = 15,
                    .empty = false,
                    .updated_at_millis = now_ms,
                    .local_leader = true,
                },
            })[0..]),
        },
    };

    var reconciler = Reconciler.initWithConfig(std.testing.allocator, .{
        .max_shard_size_bytes = 100,
        .min_shard_size_bytes = 30,
        .min_shards_per_table = 2,
        .max_shards_per_table = 8,
    });
    var plan = try reconciler.computePlan(&manager, &.{}, &.{}, .{
        .tables = tables,
        .ranges = ranges,
        .stores = &stores,
    });
    defer plan.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 0), plan.merge_upserts.len);
}

test "metadata reconciler enforces per-table automatic transition budget" {
    var manager = table_manager.TableManager.init(std.testing.allocator);
    defer manager.deinit();

    try manager.upsertTable(.{ .table_id = 413, .name = "docs", .min_ranges = 1 });
    try manager.upsertRange(.{
        .group_id = 4131,
        .table_id = 413,
        .start_key = "doc:a",
        .end_key = "doc:m",
    });
    try manager.upsertRange(.{
        .group_id = 4132,
        .table_id = 413,
        .start_key = "doc:m",
        .end_key = "doc:z",
    });

    const tables = try manager.listTables(std.testing.allocator);
    defer manager.freeTables(std.testing.allocator, tables);
    const ranges = try manager.listRanges(std.testing.allocator);
    defer manager.freeRanges(std.testing.allocator, ranges);

    const now_ms = platform_clock.Clock.real().nowRealtimeMs();
    const voter_set_fingerprint = table_manager.voterSetFingerprint(&.{1}, null);
    const placements = [_]raft_reconciler.PlacementIntent{
        .{ .record = .{ .group_id = 4131, .replica_id = 1, .local_node_id = 1 }, .store_id = 1, .peer_node_ids = &.{1} },
        .{ .record = .{ .group_id = 4132, .replica_id = 2, .local_node_id = 1 }, .store_id = 1, .peer_node_ids = &.{1} },
    };
    const stores = [_]table_manager.StoreRecord{
        .{
            .store_id = 1,
            .node_id = 1,
            .role = "data",
            .health_class = "healthy",
            .failure_domain = "rack-a",
            .live = true,
            .group_statuses = @constCast((&[_]table_manager.GroupStatusReport{
                .{
                    .group_id = 4131,
                    .doc_count = 200,
                    .disk_bytes = 200,
                    .empty = false,
                    .updated_at_millis = now_ms,
                    .local_leader = true,
                    .local_voter = true,
                    .voter_count = 1,
                    .voter_set_known = true,
                    .voter_set_fingerprint = voter_set_fingerprint,
                },
                .{
                    .group_id = 4132,
                    .doc_count = 220,
                    .disk_bytes = 220,
                    .empty = false,
                    .updated_at_millis = now_ms,
                    .local_leader = true,
                    .local_voter = true,
                    .voter_count = 1,
                    .voter_set_known = true,
                    .voter_set_fingerprint = voter_set_fingerprint,
                },
            })[0..]),
        },
    };

    var lookup = TestMedianKeyLookup{ .median_key = "doc:g" };
    var reconciler = Reconciler.initWithConfig(std.testing.allocator, .{
        .max_shard_size_bytes = 100,
        .max_shards_per_table = 8,
        .auto_range_transition_per_table_limit = 1,
        .auto_range_transition_cluster_limit = 8,
        .median_key_lookup = lookup.iface(),
    });
    var plan = try reconciler.computePlan(&manager, &.{}, &.{}, .{
        .tables = tables,
        .ranges = ranges,
        .placement_intents = &placements,
        .stores = &stores,
    });
    defer plan.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 1), plan.split_admissions.len);
}

test "metadata reconciler enforces cluster automatic transition budget" {
    var manager = table_manager.TableManager.init(std.testing.allocator);
    defer manager.deinit();

    try manager.upsertTable(.{ .table_id = 414, .name = "docs_a", .min_ranges = 1 });
    try manager.upsertRange(.{
        .group_id = 4141,
        .table_id = 414,
        .start_key = "doc:a",
        .end_key = "doc:z",
    });
    try manager.upsertTable(.{ .table_id = 415, .name = "docs_b", .min_ranges = 1 });
    try manager.upsertRange(.{
        .group_id = 4151,
        .table_id = 415,
        .start_key = "row:a",
        .end_key = "row:z",
    });

    const tables = try manager.listTables(std.testing.allocator);
    defer manager.freeTables(std.testing.allocator, tables);
    const ranges = try manager.listRanges(std.testing.allocator);
    defer manager.freeRanges(std.testing.allocator, ranges);

    const now_ms = platform_clock.Clock.real().nowRealtimeMs();
    const voter_set_fingerprint = table_manager.voterSetFingerprint(&.{1}, null);
    const placements = [_]raft_reconciler.PlacementIntent{
        .{ .record = .{ .group_id = 4141, .replica_id = 1, .local_node_id = 1 }, .store_id = 1, .peer_node_ids = &.{1} },
        .{ .record = .{ .group_id = 4151, .replica_id = 2, .local_node_id = 1 }, .store_id = 1, .peer_node_ids = &.{1} },
    };
    const stores = [_]table_manager.StoreRecord{
        .{
            .store_id = 1,
            .node_id = 1,
            .role = "data",
            .health_class = "healthy",
            .failure_domain = "rack-a",
            .live = true,
            .group_statuses = @constCast((&[_]table_manager.GroupStatusReport{
                .{
                    .group_id = 4141,
                    .doc_count = 200,
                    .disk_bytes = 200,
                    .empty = false,
                    .updated_at_millis = now_ms,
                    .local_leader = true,
                    .local_voter = true,
                    .voter_count = 1,
                    .voter_set_known = true,
                    .voter_set_fingerprint = voter_set_fingerprint,
                },
                .{
                    .group_id = 4151,
                    .doc_count = 210,
                    .disk_bytes = 210,
                    .empty = false,
                    .updated_at_millis = now_ms,
                    .local_leader = true,
                    .local_voter = true,
                    .voter_count = 1,
                    .voter_set_known = true,
                    .voter_set_fingerprint = voter_set_fingerprint,
                },
            })[0..]),
        },
    };

    var lookup = TestMedianKeyLookup{ .median_key = "doc:m" };
    var reconciler = Reconciler.initWithConfig(std.testing.allocator, .{
        .max_shard_size_bytes = 100,
        .max_shards_per_table = 8,
        .auto_range_transition_per_table_limit = 8,
        .auto_range_transition_cluster_limit = 1,
        .median_key_lookup = lookup.iface(),
    });
    var plan = try reconciler.computePlan(&manager, &.{}, &.{}, .{
        .tables = tables,
        .ranges = ranges,
        .placement_intents = &placements,
        .stores = &stores,
    });
    defer plan.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 1), plan.split_admissions.len);
}

test "metadata reconciler respects disable shard alloc unless reallocation is requested" {
    var manager = table_manager.TableManager.init(std.testing.allocator);
    defer manager.deinit();

    try manager.upsertTable(.{ .table_id = 42, .name = "docs" });
    try manager.upsertRange(.{
        .group_id = 4201,
        .table_id = 42,
        .start_key = "doc:a",
        .end_key = "doc:z",
    });

    const tables = try manager.listTables(std.testing.allocator);
    defer manager.freeTables(std.testing.allocator, tables);
    const ranges = try manager.listRanges(std.testing.allocator);
    defer manager.freeRanges(std.testing.allocator, ranges);

    const now_ms = platform_clock.Clock.real().nowRealtimeMs();
    const voter_set_fingerprint = table_manager.voterSetFingerprint(&.{1}, null);
    const placements = [_]raft_reconciler.PlacementIntent{.{
        .record = .{ .group_id = 4201, .replica_id = 1, .local_node_id = 1 },
        .store_id = 1,
        .peer_node_ids = &.{1},
    }};
    const stores = [_]table_manager.StoreRecord{
        .{
            .store_id = 1,
            .node_id = 1,
            .role = "data",
            .health_class = "healthy",
            .failure_domain = "rack-a",
            .live = true,
            .group_statuses = @constCast((&[_]table_manager.GroupStatusReport{
                .{
                    .group_id = 4201,
                    .doc_count = 200,
                    .disk_bytes = 200,
                    .empty = false,
                    .updated_at_millis = now_ms,
                    .local_leader = true,
                    .local_voter = true,
                    .voter_count = 1,
                    .voter_set_known = true,
                    .voter_set_fingerprint = voter_set_fingerprint,
                },
            })[0..]),
        },
    };

    var lookup = TestMedianKeyLookup{ .median_key = "doc:m" };
    var reconciler = Reconciler.initWithConfig(std.testing.allocator, .{
        .max_shard_size_bytes = 100,
        .max_shards_per_table = 8,
        .disable_shard_alloc = true,
        .median_key_lookup = lookup.iface(),
    });

    var blocked_plan = try reconciler.computePlan(&manager, &.{}, &.{}, .{
        .tables = tables,
        .ranges = ranges,
        .placement_intents = &placements,
        .stores = &stores,
        .reallocation_request = null,
    });
    defer blocked_plan.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 0), blocked_plan.split_upserts.len);
    try std.testing.expect(blocked_plan.clear_reallocation_request == null);

    var forced_plan = try reconciler.computePlan(&manager, &.{}, &.{}, .{
        .tables = tables,
        .ranges = ranges,
        .placement_intents = &placements,
        .stores = &stores,
        .reallocation_request = .{ .request_id = 1, .requested_at_ms = 1 },
    });
    defer forced_plan.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 1), forced_plan.split_admissions.len);
    try std.testing.expect(forced_plan.forced_reallocation);
    // The request admitted the split, but no placement candidates were
    // available in this round. Keep it durable until a later round can run
    // the complete forced-placement plan.
    try std.testing.expect(forced_plan.clear_reallocation_request == null);
}

test "metadata reconciler places completed split groups into cooldown" {
    var manager = table_manager.TableManager.init(std.testing.allocator);
    defer manager.deinit();

    try manager.upsertTable(.{ .table_id = 43, .name = "docs" });
    try manager.upsertRange(.{
        .group_id = 4301,
        .table_id = 43,
        .start_key = "doc:a",
        .end_key = "doc:z",
    });

    const tables = try manager.listTables(std.testing.allocator);
    defer manager.freeTables(std.testing.allocator, tables);
    const ranges = try manager.listRanges(std.testing.allocator);
    defer manager.freeRanges(std.testing.allocator, ranges);

    const now_ms = platform_clock.Clock.real().nowRealtimeMs();
    const stores = [_]table_manager.StoreRecord{
        .{
            .store_id = 1,
            .node_id = 1,
            .role = "data",
            .health_class = "healthy",
            .failure_domain = "rack-a",
            .live = true,
            .group_statuses = @constCast((&[_]table_manager.GroupStatusReport{
                .{
                    .group_id = 4301,
                    .doc_count = 220,
                    .disk_bytes = 220,
                    .empty = false,
                    .updated_at_millis = now_ms,
                },
            })[0..]),
        },
    };

    var reconciler = Reconciler.initWithConfig(std.testing.allocator, .{
        .max_shard_size_bytes = 100,
        .max_shards_per_table = 8,
        .shard_cooldown_millis = 60 * std.time.ms_per_s,
    });
    defer reconciler.deinit();

    var finalize_plan = try reconciler.computePlan(&manager, &.{}, &.{}, .{
        .tables = tables,
        .ranges = ranges,
        .stores = &stores,
        .split_transitions = &[_]transition_state.SplitTransitionRecord{.{
            .transition_id = 43001,
            .attempt_epoch = 1,
            .source_group_id = 4301,
            .destination_group_id = 4302,
            .phase = .finalized,
            .split_key = "doc:m",
        }},
        .split_observations = &[_]SplitRuntimeObservation{.{
            .transition_id = 43001,
            .observation = .{
                .status = .{
                    .phase = .finalized,
                    .source_split_phase = .none,
                    .bootstrapped = true,
                    .replay_required = false,
                    .replay_caught_up = true,
                    .cutover_ready = true,
                    .destination_ready_for_reads = true,
                    .source_delta_sequence = 1,
                    .dest_delta_sequence = 1,
                },
            },
        }},
    });
    defer finalize_plan.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 1), finalize_plan.split_removals.len);
    try std.testing.expectEqual(@as(usize, 0), finalize_plan.split_upserts.len);

    var blocked_plan = try reconciler.computePlan(&manager, &.{}, &.{}, .{
        .tables = tables,
        .ranges = ranges,
        .stores = &stores,
    });
    defer blocked_plan.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 0), blocked_plan.split_upserts.len);
}

test "metadata reconciler ignores unhealthy store stats for automatic transitions" {
    var manager = table_manager.TableManager.init(std.testing.allocator);
    defer manager.deinit();

    try manager.upsertTable(.{ .table_id = 44, .name = "docs", .min_ranges = 1 });
    try manager.upsertRange(.{
        .group_id = 4401,
        .table_id = 44,
        .start_key = "doc:a",
        .end_key = "doc:m",
    });
    try manager.upsertRange(.{
        .group_id = 4402,
        .table_id = 44,
        .start_key = "doc:m",
        .end_key = "doc:z",
    });

    const tables = try manager.listTables(std.testing.allocator);
    defer manager.freeTables(std.testing.allocator, tables);
    const ranges = try manager.listRanges(std.testing.allocator);
    defer manager.freeRanges(std.testing.allocator, ranges);

    const now_ms = platform_clock.Clock.real().nowRealtimeMs();
    const stores = [_]table_manager.StoreRecord{
        .{
            .store_id = 1,
            .node_id = 1,
            .role = "data",
            .health_class = "degraded",
            .failure_domain = "rack-a",
            .live = true,
            .group_statuses = @constCast((&[_]table_manager.GroupStatusReport{
                .{
                    .group_id = 4401,
                    .doc_count = 200,
                    .disk_bytes = 200,
                    .empty = false,
                    .updated_at_millis = now_ms,
                },
                .{
                    .group_id = 4402,
                    .doc_count = 10,
                    .disk_bytes = 10,
                    .empty = false,
                    .updated_at_millis = now_ms,
                },
            })[0..]),
        },
    };

    var reconciler = Reconciler.initWithConfig(std.testing.allocator, .{
        .max_shard_size_bytes = 100,
        .min_shard_size_bytes = 30,
        .min_shards_per_table = 1,
        .max_shards_per_table = 8,
    });

    var plan = try reconciler.computePlan(&manager, &.{}, &.{}, .{
        .tables = tables,
        .ranges = ranges,
        .stores = &stores,
    });
    defer plan.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 0), plan.split_upserts.len);
    try std.testing.expectEqual(@as(usize, 0), plan.merge_upserts.len);
}

test "metadata reconciler ignores stale store stats for automatic transitions" {
    var manager = table_manager.TableManager.init(std.testing.allocator);
    defer manager.deinit();

    try manager.upsertTable(.{ .table_id = 45, .name = "docs", .min_ranges = 1 });
    try manager.upsertRange(.{
        .group_id = 4501,
        .table_id = 45,
        .start_key = "doc:a",
        .end_key = "doc:m",
    });
    try manager.upsertRange(.{
        .group_id = 4502,
        .table_id = 45,
        .start_key = "doc:m",
        .end_key = "doc:z",
    });

    const tables = try manager.listTables(std.testing.allocator);
    defer manager.freeTables(std.testing.allocator, tables);
    const ranges = try manager.listRanges(std.testing.allocator);
    defer manager.freeRanges(std.testing.allocator, ranges);

    const now_ms = platform_clock.Clock.real().nowRealtimeMs();
    const stale_ms = now_ms - 5 * std.time.ms_per_s;
    const stores = [_]table_manager.StoreRecord{
        .{
            .store_id = 1,
            .node_id = 1,
            .role = "data",
            .health_class = "healthy",
            .failure_domain = "rack-a",
            .live = true,
            .group_statuses = @constCast((&[_]table_manager.GroupStatusReport{
                .{
                    .group_id = 4501,
                    .doc_count = 220,
                    .disk_bytes = 220,
                    .empty = false,
                    .updated_at_millis = stale_ms,
                },
                .{
                    .group_id = 4502,
                    .doc_count = 8,
                    .disk_bytes = 10,
                    .empty = false,
                    .updated_at_millis = stale_ms,
                },
            })[0..]),
        },
    };

    var reconciler = Reconciler.initWithConfig(std.testing.allocator, .{
        .max_shard_size_bytes = 100,
        .min_shard_size_bytes = 30,
        .min_shards_per_table = 1,
        .max_shards_per_table = 8,
        .stats_stale_after_millis = 100,
    });

    var plan = try reconciler.computePlan(&manager, &.{}, &.{}, .{
        .tables = tables,
        .ranges = ranges,
        .stores = &stores,
    });
    defer plan.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 0), plan.split_upserts.len);
    try std.testing.expectEqual(@as(usize, 0), plan.merge_upserts.len);
}

test "metadata reconciler ignores in-flight transition groups for automatic transitions" {
    var manager = table_manager.TableManager.init(std.testing.allocator);
    defer manager.deinit();

    try manager.upsertTable(.{ .table_id = 46, .name = "docs", .min_ranges = 1 });
    try manager.upsertRange(.{
        .group_id = 4601,
        .table_id = 46,
        .start_key = "doc:a",
        .end_key = "doc:m",
    });
    try manager.upsertRange(.{
        .group_id = 4602,
        .table_id = 46,
        .start_key = "doc:m",
        .end_key = "doc:z",
    });

    const tables = try manager.listTables(std.testing.allocator);
    defer manager.freeTables(std.testing.allocator, tables);
    const ranges = try manager.listRanges(std.testing.allocator);
    defer manager.freeRanges(std.testing.allocator, ranges);

    const now_ms = platform_clock.Clock.real().nowRealtimeMs();
    const stores = [_]table_manager.StoreRecord{
        .{
            .store_id = 1,
            .node_id = 1,
            .role = "data",
            .health_class = "healthy",
            .failure_domain = "rack-a",
            .live = true,
            .group_statuses = @constCast((&[_]table_manager.GroupStatusReport{
                .{
                    .group_id = 4601,
                    .doc_count = 220,
                    .disk_bytes = 220,
                    .empty = false,
                    .updated_at_millis = now_ms,
                },
                .{
                    .group_id = 4602,
                    .doc_count = 8,
                    .disk_bytes = 10,
                    .empty = false,
                    .updated_at_millis = now_ms,
                },
            })[0..]),
        },
    };

    var reconciler = Reconciler.initWithConfig(std.testing.allocator, .{
        .max_shard_size_bytes = 100,
        .min_shard_size_bytes = 30,
        .min_shards_per_table = 1,
        .max_shards_per_table = 8,
    });

    var plan = try reconciler.computePlan(&manager, &.{}, &.{}, .{
        .tables = tables,
        .ranges = ranges,
        .stores = &stores,
        .split_transitions = &[_]transition_state.SplitTransitionRecord{.{
            .transition_id = 46001,
            .attempt_epoch = 1,
            .source_group_id = 4601,
            .destination_group_id = 4603,
            .phase = .prepare,
            .split_key = "doc:g",
            .table_contract = transitionTableContractForTest(
                46,
                "docs",
                4601,
                4601,
            ),
        }},
    });
    defer plan.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 0), plan.split_upserts.len);
    try std.testing.expectEqual(@as(usize, 0), plan.merge_upserts.len);
}

test "metadata reconciler ignores transition-marked store status for automatic transitions" {
    var manager = table_manager.TableManager.init(std.testing.allocator);
    defer manager.deinit();

    try manager.upsertTable(.{ .table_id = 461, .name = "docs", .min_ranges = 1 });
    try manager.upsertRange(.{
        .group_id = 4611,
        .table_id = 461,
        .start_key = "doc:a",
        .end_key = "doc:z",
    });

    const tables = try manager.listTables(std.testing.allocator);
    defer manager.freeTables(std.testing.allocator, tables);
    const ranges = try manager.listRanges(std.testing.allocator);
    defer manager.freeRanges(std.testing.allocator, ranges);

    const now_ms = platform_clock.Clock.real().nowRealtimeMs();
    const stores = [_]table_manager.StoreRecord{
        .{
            .store_id = 1,
            .node_id = 1,
            .role = "data",
            .health_class = "healthy",
            .failure_domain = "rack-a",
            .live = true,
            .group_statuses = @constCast((&[_]table_manager.GroupStatusReport{
                .{
                    .group_id = 4611,
                    .doc_count = 220,
                    .disk_bytes = 220,
                    .empty = false,
                    .updated_at_millis = now_ms,
                    .transition_pending = true,
                    .replay_required = true,
                },
            })[0..]),
        },
    };

    var reconciler = Reconciler.initWithConfig(std.testing.allocator, .{
        .max_shard_size_bytes = 100,
        .max_shards_per_table = 8,
    });
    defer reconciler.deinit();

    var plan = try reconciler.computePlan(&manager, &.{}, &.{}, .{
        .tables = tables,
        .ranges = ranges,
        .stores = &stores,
    });
    defer plan.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 0), plan.split_upserts.len);
}

test "metadata reconciler uses live median key lookup for split planning" {
    var manager = table_manager.TableManager.init(std.testing.allocator);
    defer manager.deinit();

    try manager.upsertTable(.{ .table_id = 47, .name = "docs" });
    try manager.upsertRange(.{
        .group_id = 4701,
        .table_id = 47,
        .start_key = "doc:a",
        .end_key = "doc:z",
    });

    const tables = try manager.listTables(std.testing.allocator);
    defer manager.freeTables(std.testing.allocator, tables);
    const ranges = try manager.listRanges(std.testing.allocator);
    defer manager.freeRanges(std.testing.allocator, ranges);

    const now_ms = platform_clock.Clock.real().nowRealtimeMs();
    const voter_set_fingerprint = table_manager.voterSetFingerprint(&.{ 1, 2 }, null);
    const placements = [_]raft_reconciler.PlacementIntent{
        .{ .record = .{ .group_id = 4701, .replica_id = 1, .local_node_id = 1 }, .store_id = 1, .peer_node_ids = &.{ 1, 2 } },
        .{ .record = .{ .group_id = 4701, .replica_id = 2, .local_node_id = 2 }, .store_id = 2, .peer_node_ids = &.{ 1, 2 } },
    };
    const stores = [_]table_manager.StoreRecord{
        .{
            .store_id = 1,
            .node_id = 1,
            .role = "data",
            .health_class = "healthy",
            .failure_domain = "rack-a",
            .live = true,
            .group_statuses = @constCast((&[_]table_manager.GroupStatusReport{
                .{
                    .group_id = 4701,
                    .doc_count = 240,
                    .disk_bytes = 240,
                    .empty = false,
                    .updated_at_millis = now_ms - 5,
                    .local_leader = true,
                    .local_voter = true,
                    .voter_count = 2,
                    .voter_set_known = true,
                    .voter_set_fingerprint = voter_set_fingerprint,
                },
            })[0..]),
        },
        .{
            .store_id = 2,
            .node_id = 2,
            .role = "data",
            .health_class = "healthy",
            .failure_domain = "rack-b",
            .live = true,
            .group_statuses = @constCast((&[_]table_manager.GroupStatusReport{
                .{
                    .group_id = 4701,
                    .doc_count = 240,
                    .disk_bytes = 240,
                    .empty = false,
                    .updated_at_millis = now_ms,
                    .local_leader = false,
                    .local_voter = true,
                    .voter_count = 2,
                    .voter_set_known = true,
                    .voter_set_fingerprint = voter_set_fingerprint,
                },
            })[0..]),
        },
    };

    var lookup = TestMedianKeyLookup{ .median_key = "doc:t" };
    var reconciler = Reconciler.initWithConfig(std.testing.allocator, .{
        .max_shard_size_bytes = 100,
        .max_shards_per_table = 8,
        .median_key_lookup = lookup.iface(),
    });
    var plan = try reconciler.computePlan(&manager, &.{}, &.{}, .{
        .tables = tables,
        .ranges = ranges,
        .placement_intents = &placements,
        .stores = &stores,
    });
    defer plan.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 1), plan.split_admissions.len);
    try std.testing.expectEqualStrings("doc:t", plan.split_admissions[0].record.split_key.?);
}

test "metadata reconciler requires leader-known group status for automatic planning" {
    var manager = table_manager.TableManager.init(std.testing.allocator);
    defer manager.deinit();

    try manager.upsertTable(.{ .table_id = 48, .name = "docs" });
    try manager.upsertRange(.{
        .group_id = 4801,
        .table_id = 48,
        .start_key = "doc:a",
        .end_key = "doc:z",
    });

    const tables = try manager.listTables(std.testing.allocator);
    defer manager.freeTables(std.testing.allocator, tables);
    const ranges = try manager.listRanges(std.testing.allocator);
    defer manager.freeRanges(std.testing.allocator, ranges);

    const now_ms = platform_clock.Clock.real().nowRealtimeMs();
    const stores = [_]table_manager.StoreRecord{
        .{
            .store_id = 1,
            .node_id = 1,
            .role = "data",
            .health_class = "healthy",
            .failure_domain = "rack-a",
            .live = true,
            .group_statuses = @constCast((&[_]table_manager.GroupStatusReport{
                .{
                    .group_id = 4801,
                    .doc_count = 200,
                    .disk_bytes = 200,
                    .empty = false,
                    .updated_at_millis = now_ms,
                    .local_leader = false,
                },
            })[0..]),
        },
    };

    var reconciler = Reconciler.initWithConfig(std.testing.allocator, .{
        .max_shard_size_bytes = 100,
        .max_shards_per_table = 8,
    });
    var plan = try reconciler.computePlan(&manager, &.{}, &.{}, .{
        .tables = tables,
        .ranges = ranges,
        .stores = &stores,
    });
    defer plan.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 0), plan.split_upserts.len);
}

test "metadata reconciler does not plan automatic split while restore is pending" {
    var manager = table_manager.TableManager.init(std.testing.allocator);
    defer manager.deinit();

    try manager.upsertTable(.{ .table_id = 482, .name = "docs" });
    try manager.upsertRange(.{
        .group_id = 4821,
        .table_id = 482,
        .start_key = "doc:a",
        .end_key = "doc:z",
    });

    const tables = try manager.listTables(std.testing.allocator);
    defer manager.freeTables(std.testing.allocator, tables);
    const ranges = try manager.listRanges(std.testing.allocator);
    defer manager.freeRanges(std.testing.allocator, ranges);

    const now_ms = platform_clock.Clock.real().nowRealtimeMs();
    const statuses = [_]MergedGroupStatus{
        .{
            .group_id = 4821,
            .doc_count = 200,
            .disk_bytes = 200,
            .empty = false,
            .updated_at_millis = now_ms,
            .leader_known = true,
            .leader_store_id = 1,
            .voter_count_known = true,
            .voter_count = 1,
            .healthy_voter_reports = 1,
            .restore_pending = true,
        },
    };

    var reconciler = Reconciler.initWithConfig(std.testing.allocator, .{
        .max_shard_size_bytes = 100,
        .max_shards_per_table = 8,
    });
    var plan = try reconciler.computePlan(&manager, &.{}, &.{}, .{
        .tables = tables,
        .ranges = ranges,
        .merged_group_statuses = &statuses,
    });
    defer plan.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 0), plan.split_upserts.len);
}

test "metadata reconciler marks restore-active placements with fetch_snapshot until progress is reported" {
    var manager = table_manager.TableManager.init(std.testing.allocator);
    defer manager.deinit();

    try manager.upsertTable(.{
        .table_id = 490,
        .name = "docs",
    });
    try manager.upsertRange(.{
        .group_id = 4901,
        .table_id = 490,
        .start_key = "",
        .end_key = null,
        .restore_backup_id = "snap1",
        .restore_location = "file:///tmp/backups",
        .restore_snapshot_path = "snap1/groups/4901",
        .restore_connection = "backups",
        .restore_artifact_sha256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
    });

    const progress = [_]table_manager.RestoreProgressRecord{
        .{
            .table_id = 490,
            .node_id = 1,
            .group_id = 4901,
            .backup_id = "snap1",
            .location = "file:///tmp/backups",
            .snapshot_path = "snap1/groups/4901",
            .artifact_sha256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            .primary_restored = true,
            .phase = "runtime_repair",
        },
    };

    var reconciler = Reconciler.init(std.testing.allocator);
    var plan = try reconciler.computePlan(&manager, &.{ 1, 2 }, &.{}, .{
        .restore_progresses = &progress,
    });
    defer plan.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 2), plan.placement_upserts.len);
    const first = findPlacementIntent(plan.placement_upserts, 4901, 1).?;
    const second = findPlacementIntent(plan.placement_upserts, 4901, 2).?;
    try std.testing.expectEqual(@import("../raft/catalog.zig").ReplicaBootstrapMode.persisted, first.record.bootstrap_mode);
    try std.testing.expectEqual(@import("../raft/catalog.zig").ReplicaBootstrapMode.fetch_snapshot, second.record.bootstrap_mode);
    try std.testing.expect(second.record.backup_restore_bootstrap != null);
    try std.testing.expectEqualStrings("snap1", second.record.backup_restore_bootstrap.?.backup_id);
    try std.testing.expectEqualStrings("file:///tmp/backups", second.record.backup_restore_bootstrap.?.location);
}

test "metadata reconciler prefers live median key lookup for automatic split" {
    var manager = table_manager.TableManager.init(std.testing.allocator);
    defer manager.deinit();

    try manager.upsertTable(.{ .table_id = 481, .name = "docs" });
    try manager.upsertRange(.{
        .group_id = 4811,
        .table_id = 481,
        .start_key = "doc:a",
        .end_key = "doc:z",
    });

    const tables = try manager.listTables(std.testing.allocator);
    defer manager.freeTables(std.testing.allocator, tables);
    const ranges = try manager.listRanges(std.testing.allocator);
    defer manager.freeRanges(std.testing.allocator, ranges);

    const now_ms = platform_clock.Clock.real().nowRealtimeMs();
    const voter_set_fingerprint = table_manager.voterSetFingerprint(&.{1}, null);
    const placements = [_]raft_reconciler.PlacementIntent{.{
        .record = .{ .group_id = 4811, .replica_id = 1, .local_node_id = 1 },
        .store_id = 1,
        .peer_node_ids = &.{1},
    }};
    const stores = [_]table_manager.StoreRecord{
        .{
            .store_id = 1,
            .node_id = 1,
            .role = "data",
            .health_class = "healthy",
            .failure_domain = "rack-a",
            .live = true,
            .group_statuses = @constCast((&[_]table_manager.GroupStatusReport{
                .{
                    .group_id = 4811,
                    .doc_count = 200,
                    .disk_bytes = 200,
                    .empty = false,
                    .updated_at_millis = now_ms,
                    .local_leader = true,
                    .local_voter = true,
                    .voter_count = 1,
                    .voter_set_known = true,
                    .voter_set_fingerprint = voter_set_fingerprint,
                },
            })[0..]),
        },
    };

    const FakeLookup = struct {
        median_key: []const u8,

        fn iface(self: *@This()) MedianKeyLookup {
            return .{
                .ptr = self,
                .vtable = &.{
                    .fetch_median_key = fetchMedianKey,
                },
            };
        }

        fn fetchMedianKey(ptr: *anyopaque, alloc: std.mem.Allocator, _: u64) !?[]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return try alloc.dupe(u8, self.median_key);
        }
    };

    var lookup = FakeLookup{ .median_key = "doc:m" };
    var reconciler = Reconciler.initWithConfig(std.testing.allocator, .{
        .max_shard_size_bytes = 100,
        .max_shards_per_table = 8,
        .median_key_lookup = lookup.iface(),
    });
    var plan = try reconciler.computePlan(&manager, &.{}, &.{}, .{
        .tables = tables,
        .ranges = ranges,
        .placement_intents = &placements,
        .stores = &stores,
    });
    defer plan.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 1), plan.split_admissions.len);
    try std.testing.expectEqualStrings("doc:m", plan.split_admissions[0].record.split_key.?);
}

test "metadata reconciler skips automatic split when live median key lookup fails" {
    var manager = table_manager.TableManager.init(std.testing.allocator);
    defer manager.deinit();

    try manager.upsertTable(.{ .table_id = 482, .name = "docs" });
    try manager.upsertRange(.{
        .group_id = 4821,
        .table_id = 482,
        .start_key = "doc:a",
        .end_key = "doc:z",
    });

    const tables = try manager.listTables(std.testing.allocator);
    defer manager.freeTables(std.testing.allocator, tables);
    const ranges = try manager.listRanges(std.testing.allocator);
    defer manager.freeRanges(std.testing.allocator, ranges);

    const now_ms = platform_clock.Clock.real().nowRealtimeMs();
    const stores = [_]table_manager.StoreRecord{
        .{
            .store_id = 1,
            .node_id = 1,
            .role = "data",
            .health_class = "healthy",
            .failure_domain = "rack-a",
            .live = true,
            .group_statuses = @constCast((&[_]table_manager.GroupStatusReport{
                .{
                    .group_id = 4821,
                    .doc_count = 200,
                    .disk_bytes = 200,
                    .empty = false,
                    .updated_at_millis = now_ms,
                    .local_leader = true,
                },
            })[0..]),
        },
    };

    const FailingLookup = struct {
        fn iface() MedianKeyLookup {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .fetch_median_key = fetchMedianKey,
                },
            };
        }

        fn fetchMedianKey(_: *anyopaque, _: std.mem.Allocator, _: u64) !?[]u8 {
            return error.UnknownGroup;
        }
    };

    var reconciler = Reconciler.initWithConfig(std.testing.allocator, .{
        .max_shard_size_bytes = 100,
        .max_shards_per_table = 8,
        .median_key_lookup = FailingLookup.iface(),
    });
    var plan = try reconciler.computePlan(&manager, &.{}, &.{}, .{
        .tables = tables,
        .ranges = ranges,
        .stores = &stores,
    });
    defer plan.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 0), plan.split_upserts.len);
}
