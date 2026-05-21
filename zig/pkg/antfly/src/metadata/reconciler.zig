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
const platform_clock = @import("../platform/clock.zig");
const platform_time = @import("../platform/time.zig");
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
    reallocate_requested: bool = false,
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
    empty: bool = true,
    created_at_millis: u64 = 0,
    updated_at_millis: u64 = 0,
    leader_known: bool = false,
    leader_store_id: u64 = 0,
    voter_count_known: bool = false,
    voter_count: u16 = 0,
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

pub const PlacementChangeKind = enum {
    stable,
    repair_required,
    rebalance,
};

pub const ReconciliationPlan = struct {
    placement_upserts: []raft_reconciler.PlacementIntent,
    table_upserts: []table_manager.TableRecord,
    range_upserts: []table_manager.RangeRecord,
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
    clear_reallocation_request: bool = false,

    pub fn empty() ReconciliationPlan {
        return .{
            .placement_upserts = &.{},
            .table_upserts = &.{},
            .range_upserts = &.{},
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
            .clear_reallocation_request = false,
        };
    }

    pub fn deinit(self: *ReconciliationPlan, alloc: std.mem.Allocator) void {
        for (self.placement_upserts) |intent| {
            var record = intent.record;
            record.deinit(alloc);
            if (intent.peer_node_ids.len > 0) alloc.free(intent.peer_node_ids);
        }
        alloc.free(self.placement_upserts);
        for (self.table_upserts) |record| table_manager.freeTable(alloc, record);
        alloc.free(self.table_upserts);
        for (self.range_upserts) |record| table_manager.freeRange(alloc, record);
        alloc.free(self.range_upserts);
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
        const desired_ranges = try manager.listRanges(self.alloc);
        defer manager.freeRanges(self.alloc, desired_ranges);
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
                .retain_current = if (current.reallocate_requested) false else candidate.retain_current,
            };
        }
        const desired_placements = if (placement_candidate_node_ids.len > 0)
            try planner.planAllIntentsWithCurrentAndDomains(manager, placement_candidate_node_ids, current.placement_intents, candidate_domains)
        else
            try self.alloc.alloc(raft_reconciler.PlacementIntent, 0);
        defer planner.freeIntents(self.alloc, desired_placements);
        try self.syncAutomaticShardIntents(manager, current, now_monotonic_ms, now_realtime_ms);
        const desired_splits = try manager.listDesiredSplitTransitions(self.alloc);
        defer manager.freeSplitTransitions(self.alloc, desired_splits);
        const desired_merges = try manager.listDesiredMergeTransitions(self.alloc);
        defer manager.freeMergeTransitions(self.alloc, desired_merges);

        var table_upserts = std.ArrayListUnmanaged(table_manager.TableRecord).empty;
        var placement_upserts = std.ArrayListUnmanaged(raft_reconciler.PlacementIntent).empty;
        errdefer {
            for (placement_upserts.items) |intent| {
                if (intent.peer_node_ids.len > 0) self.alloc.free(intent.peer_node_ids);
            }
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
            const effective = normalizeRestoreBootstrapIntent(current, desired_tables, desired_ranges, desired);
            const existing = findPlacementIntent(current.placement_intents, effective.record.group_id, effective.record.local_node_id);
            if (existing == null or !placementIntentsEqual(existing.?, effective)) {
                try placement_upserts.append(self.alloc, try clonePlacementIntent(self.alloc, effective));
            }
        }
        for (desired_tables) |*desired| {
            try maybeFinalizeSchemaMigration(self.alloc, current, desired);
            const existing = findTableRecord(current.tables, desired.table_id);
            if (existing == null or !tableRecordsEqual(existing.?, desired.*)) {
                try table_upserts.append(self.alloc, try table_manager.cloneTable(self.alloc, desired.*));
            }
        }
        for (desired_ranges) |desired| {
            const existing = findRangeRecord(current.ranges, desired.group_id);
            if (existing == null or !rangeRecordsEqual(existing.?, desired)) {
                try range_upserts.append(self.alloc, try table_manager.cloneRange(self.alloc, desired));
            }
        }
        for (desired_splits) |desired| {
            const existing = findSplitRecord(current.split_transitions, desired.transition_id);
            if (existing == null) {
                if (!splitTransitionDocIdentityCompatible(current, desired)) continue;
                try split_upserts.append(self.alloc, try cloneSplitRecord(self.alloc, desired));
                continue;
            }

            var effective_record = try cloneSplitRecord(self.alloc, desired);
            var effective_record_owned = true;
            errdefer if (effective_record_owned) table_manager.freeSplitTransitionRecord(self.alloc, effective_record);
            if (existing.?.rollback_reason) |reason| {
                if (effective_record.rollback_reason == null) {
                    effective_record.rollback_reason = try self.alloc.dupe(u8, reason);
                }
            } else if (splitTransitionCanRollback(existing.?) and !splitTransitionDocIdentityCompatible(current, existing.?)) {
                effective_record.rollback_reason = try self.alloc.dupe(u8, doc_identity_transition_rollback_reason);
            }

            if (!splitRecordsEqual(existing.?, effective_record)) {
                try split_upserts.append(self.alloc, effective_record);
                effective_record_owned = false;
                continue;
            }
            table_manager.freeSplitTransitionRecord(self.alloc, effective_record);
            effective_record_owned = false;

            const observation = findSplitObservation(current.split_observations, desired.transition_id) orelse defaultSplitObservation();
            const planned_record = try cloneSplitRecord(self.alloc, existing.?);
            errdefer table_manager.freeSplitTransitionRecord(self.alloc, planned_record);
            try split_steps.append(self.alloc, .{
                .record = planned_record,
                .execution = transition_controller.TransitionController.describeSplit(planned_record, observation),
            });
        }

        for (desired_merges) |desired| {
            const existing = findMergeRecord(current.merge_transitions, desired.transition_id);
            if (existing == null) {
                if (!mergeTransitionDocIdentityCompatible(current, desired, .disallow_active)) continue;
                try merge_upserts.append(self.alloc, try cloneMergeRecord(self.alloc, desired));
                continue;
            }

            var effective_record = try cloneMergeRecord(self.alloc, desired);
            var effective_record_owned = true;
            errdefer if (effective_record_owned) table_manager.freeMergeTransitionRecord(self.alloc, effective_record);
            if (existing.?.rollback_reason) |reason| {
                if (effective_record.rollback_reason == null) {
                    effective_record.rollback_reason = try self.alloc.dupe(u8, reason);
                }
            } else if (mergeTransitionCanRollback(existing.?) and !mergeTransitionDocIdentityCompatible(current, existing.?, .allow_existing_active)) {
                effective_record.rollback_reason = try self.alloc.dupe(u8, doc_identity_merge_rollback_reason);
            }

            if (!mergeRecordsEqual(existing.?, effective_record)) {
                try merge_upserts.append(self.alloc, effective_record);
                effective_record_owned = false;
                continue;
            }
            table_manager.freeMergeTransitionRecord(self.alloc, effective_record);
            effective_record_owned = false;

            const observation = findMergeObservation(current.merge_observations, desired.transition_id) orelse defaultMergeObservation(existing.?);
            const planned_record = try cloneMergeRecord(self.alloc, existing.?);
            errdefer table_manager.freeMergeTransitionRecord(self.alloc, planned_record);
            try merge_steps.append(self.alloc, .{
                .record = planned_record,
                .execution = transition_controller.TransitionController.describeMerge(planned_record, observation),
            });
        }

        for (current.placement_intents) |intent| {
            if (findPlacementIntent(desired_placements, intent.record.group_id, intent.record.local_node_id) == null) {
                try placement_removals.append(self.alloc, .{
                    .group_id = intent.record.group_id,
                    .local_node_id = intent.record.local_node_id,
                });
            }
        }
        for (current.tables) |record| {
            if (findTableRecord(desired_tables, record.table_id) == null) try table_removals.append(self.alloc, record.table_id);
        }
        for (current.ranges) |record| {
            if (findRangeRecord(desired_ranges, record.group_id) == null) try range_removals.append(self.alloc, record.group_id);
        }
        for (current.split_transitions) |record| {
            if (findSplitRecord(desired_splits, record.transition_id) == null) try split_removals.append(self.alloc, record.transition_id);
        }
        for (current.merge_transitions) |record| {
            if (findMergeRecord(desired_merges, record.transition_id) == null) try merge_removals.append(self.alloc, record.transition_id);
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

        return .{
            .placement_upserts = try placement_upserts.toOwnedSlice(self.alloc),
            .table_upserts = try table_upserts.toOwnedSlice(self.alloc),
            .range_upserts = try range_upserts.toOwnedSlice(self.alloc),
            .split_upserts = try split_upserts.toOwnedSlice(self.alloc),
            .merge_upserts = try merge_upserts.toOwnedSlice(self.alloc),
            .placement_removals = try placement_removals.toOwnedSlice(self.alloc),
            .table_removals = try table_removals.toOwnedSlice(self.alloc),
            .range_removals = try range_removals.toOwnedSlice(self.alloc),
            .split_removals = try split_removals.toOwnedSlice(self.alloc),
            .merge_removals = try merge_removals.toOwnedSlice(self.alloc),
            .split_steps = try split_steps.toOwnedSlice(self.alloc),
            .merge_steps = try merge_steps.toOwnedSlice(self.alloc),
            .repair_placement_groups = repair_placement_groups,
            .rebalance_placement_groups = rebalance_placement_groups,
            .forced_reallocation = current.reallocate_requested,
            .clear_reallocation_request = current.reallocate_requested,
        };
    }

    fn syncAutomaticShardIntents(
        self: *Reconciler,
        manager: *table_manager.TableManager,
        current: CurrentMetadataState,
        now_monotonic_ms: u64,
        now_realtime_ms: u64,
    ) !void {
        var auto_transitions = try self.computeAutomaticShardTransitions(current, now_monotonic_ms, now_realtime_ms);
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
        now_monotonic_ms: u64,
        now_realtime_ms: u64,
    ) !AutomaticTransitions {
        if ((self.config.disable_shard_alloc and !current.reallocate_requested) or self.config.max_shard_size_bytes == 0) {
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
            const active_table = activeRangeTransitionCountForTable(current, table.table_id);
            if (active_table >= table_budget) continue;
            table_budget -= active_table;

            var planned_shards = countRangesForTable(current.ranges, table.table_id);

            if (planned_shards > min_shards_per_table and table_budget > 0 and remaining_cluster_budget > 0) {
                var i: usize = 0;
                while (i + 1 < sorted_ranges.len and table_budget > 0 and remaining_cluster_budget > 0) : (i += 1) {
                    const left = sorted_ranges[i];
                    const right = sorted_ranges[i + 1];
                    if (left.table_id != table.table_id or right.table_id != table.table_id) continue;
                    if (!rangesAdjacent(left, right)) continue;
                    if (groupBusy(current, left.group_id) or groupBusy(current, right.group_id)) continue;
                    if (self.isShardInCooldown(left.group_id, now_monotonic_ms) or self.isShardInCooldown(right.group_id, now_monotonic_ms)) continue;
                    const left_status = mergedGroupStatus(current, left.group_id) orelse continue;
                    const right_status = mergedGroupStatus(current, right.group_id) orelse continue;
                    if (!groupHasFullHealthyPlacement(current, left.group_id, left_status) or !groupHasFullHealthyPlacement(current, right.group_id, right_status)) continue;
                    if (!groupStatusFresh(self.config, left_status, now_monotonic_ms) or !groupStatusFresh(self.config, right_status, now_monotonic_ms)) continue;
                    if (!groupStatusReadyForAutomaticPlanning(left_status) or !groupStatusReadyForAutomaticPlanning(right_status)) continue;
                    if (!self.groupOldEnoughForMerge(left_status, now_realtime_ms) or !self.groupOldEnoughForMerge(right_status, now_realtime_ms)) continue;
                    if (!docIdentityNamespacesCompatibleForAutomaticMerge(left_status, right_status)) continue;

                    const left_size = left_status.disk_bytes;
                    const right_size = right_status.disk_bytes;
                    if (!(left_size < min_shard_size_bytes or right_size < min_shard_size_bytes)) continue;
                    const combined = left_size + right_size;
                    if (combined >= self.config.max_shard_size_bytes) continue;

                    const transition_id = deriveAutomaticTransitionId("merge", left.group_id, right.group_id, null);
                    const intent: table_manager.MergeIntent = .{
                        .transition_id = transition_id,
                        .table_id = table.table_id,
                        .donor_group_id = right.group_id,
                        .receiver_group_id = left.group_id,
                        .automatic = true,
                    };
                    try merge_intents.append(self.alloc, intent);
                    planned_shards -= 1;
                    table_budget -= 1;
                    remaining_cluster_budget -= 1;
                }
            }

            if (planned_shards >= max_shards_per_table or table_budget == 0 or remaining_cluster_budget == 0) continue;

            for (sorted_ranges) |range| {
                if (range.table_id != table.table_id) continue;
                if (planned_shards >= max_shards_per_table or table_budget == 0 or remaining_cluster_budget == 0) break;
                if (groupBusy(current, range.group_id)) continue;
                if (self.isShardInCooldown(range.group_id, now_monotonic_ms)) continue;
                const status = mergedGroupStatus(current, range.group_id) orelse continue;
                if (!groupHasFullHealthyPlacement(current, range.group_id, status)) continue;
                if (!groupStatusFresh(self.config, status, now_monotonic_ms)) continue;
                if (!groupStatusReadyForAutomaticPlanning(status)) continue;
                if (!docIdentityNamespaceReadyForAutomaticSplit(status)) continue;
                if (status.disk_bytes <= self.config.max_shard_size_bytes) continue;
                const lookup = self.config.median_key_lookup orelse continue;
                const owned_split_key = (lookup.fetchMedianKey(self.alloc, range.group_id) catch continue) orelse continue;
                var split_key_consumed = false;
                defer if (!split_key_consumed) self.alloc.free(owned_split_key);
                if (owned_split_key.len == 0) continue;
                if (!keyStrictlyInsideRange(owned_split_key, range.start_key, range.end_key)) continue;

                const destination_group_id = deriveAutomaticSplitDestinationId(current, range.group_id, owned_split_key);
                if (destination_group_id == 0 or groupIdExists(current, destination_group_id)) continue;
                const transition_id = deriveAutomaticTransitionId("split", range.group_id, destination_group_id, owned_split_key);
                const intent: table_manager.SplitIntent = .{
                    .transition_id = transition_id,
                    .table_id = table.table_id,
                    .source_group_id = range.group_id,
                    .destination_group_id = destination_group_id,
                    .split_key = owned_split_key,
                    .automatic = true,
                };
                errdefer freeSplitIntentOwned(self.alloc, intent);
                try split_intents.append(self.alloc, intent);
                split_key_consumed = true;
                planned_shards += 1;
                table_budget -= 1;
                remaining_cluster_budget -= 1;
            }
        }

        return .{
            .splits = try split_intents.toOwnedSlice(self.alloc),
            .merges = try merge_intents.toOwnedSlice(self.alloc),
        };
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

    fn deinit(self: *AutomaticTransitions, alloc: std.mem.Allocator) void {
        for (self.splits) |intent| freeSplitIntentOwned(alloc, intent);
        if (self.splits.len > 0) alloc.free(self.splits);
        for (self.merges) |intent| freeMergeIntentOwned(alloc, intent);
        if (self.merges.len > 0) alloc.free(self.merges);
        self.* = undefined;
    }
};

fn clonePlacementIntent(alloc: std.mem.Allocator, intent: raft_reconciler.PlacementIntent) !raft_reconciler.PlacementIntent {
    var cloned_record = try intent.record.clone(alloc);
    errdefer cloned_record.deinit(alloc);
    return .{
        .record = cloned_record,
        .store_id = intent.store_id,
        .peer_node_ids = if (intent.peer_node_ids.len == 0) &.{} else try alloc.dupe(u64, intent.peer_node_ids),
    };
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
        std.mem.eql(u64, a.peer_node_ids, b.peer_node_ids);
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
        std.mem.eql(u8, a.?.location, b.?.location) and
        std.mem.eql(u8, a.?.snapshot_path, b.?.snapshot_path);
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
    const restore_backup_id = restoreBackupIdForRange(range, table) orelse return effective;
    if (findRestoreProgress(current.restore_progresses, table.table_id, intent.record.local_node_id, intent.record.group_id, restore_backup_id)) |progress| {
        if (!progress.primary_restored) return effective;
        effective.record.bootstrap_mode = .persisted;
        effective.record.snapshot_bootstrap = null;
        effective.record.backup_restore_bootstrap = null;
    } else {
        effective.record.bootstrap_mode = .fetch_snapshot;
        effective.record.snapshot_bootstrap = null;
        effective.record.backup_restore_bootstrap = .{
            .backup_id = restore_backup_id,
            .location = if (range.restore_location.len > 0) range.restore_location else table.restore_location,
            .snapshot_path = range.restore_snapshot_path,
        };
    }
    return effective;
}

fn findRestoreProgress(
    records: []const table_manager.RestoreProgressRecord,
    table_id: u64,
    node_id: u64,
    group_id: u64,
    backup_id: []const u8,
) ?table_manager.RestoreProgressRecord {
    for (records) |record| {
        if (record.table_id != table_id) continue;
        if (record.node_id != node_id) continue;
        if (record.group_id != group_id) continue;
        if (!std.mem.eql(u8, record.backup_id, backup_id)) continue;
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
        try merge_ids.append(alloc, entry.key_ptr.*);
    }
    for (merge_ids.items) |transition_id| _ = manager.removeMergeIntent(transition_id);
}

fn activeRangeTransitionCount(current: CurrentMetadataState) u32 {
    return @intCast(current.split_transitions.len + current.merge_transitions.len);
}

fn activeRangeTransitionCountForTable(current: CurrentMetadataState, table_id: u64) u32 {
    var total: u32 = 0;
    for (current.split_transitions) |record| {
        const range = findRangeRecord(current.ranges, record.source_group_id) orelse continue;
        if (range.table_id == table_id) total += 1;
    }
    for (current.merge_transitions) |record| {
        const range = findRangeRecord(current.ranges, record.receiver_group_id) orelse continue;
        if (range.table_id == table_id) total += 1;
    }
    return total;
}

fn mergedGroupStatus(current: CurrentMetadataState, group_id: u64) ?MergedGroupStatus {
    if (current.merged_group_statuses.len > 0) {
        for (current.merged_group_statuses) |status| {
            if (status.group_id == group_id) return mergeRuntimeGroupFacts(status, current.stores, current.placement_intents, current.ranges, group_id);
        }
        return null;
    }
    const fallback = mergeHealthyGroupStatusFallback(current.stores, group_id) orelse return null;
    return mergeRuntimeGroupFacts(fallback, current.stores, current.placement_intents, current.ranges, group_id);
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
        var store_reports_group = false;
        var store_reports_voter = false;
        var store_reports_runtime_facts = false;
        for (store.group_statuses) |status| {
            if (status.group_id != group_id) continue;
            store_reports_group = true;
            store_reports_voter = store_reports_voter or status.local_voter;
        }
        for (store.runtime_statuses) |status| {
            if (status.group_id != group_id) continue;
            store_reports_runtime_facts = store_reports_runtime_facts or
                status.doc_count > 0 or
                status.disk_bytes > 0 or
                runtimeDocIdentityHasFacts(status.doc_identity);
            if (status.doc_count > merged.doc_count) merged.doc_count = status.doc_count;
            if (status.disk_bytes > merged.disk_bytes) merged.disk_bytes = status.disk_bytes;
            if (status.doc_count > 0 or status.disk_bytes > 0) merged.empty = false;
            const updated_at_millis = @divTrunc(status.updated_at_ns, std.time.ns_per_ms);
            if (updated_at_millis > merged.updated_at_millis) merged.updated_at_millis = updated_at_millis;
            mergeRuntimeDocIdentity(&merged, status.doc_identity);
        }
        if (store_reports_voter or (!store_reports_group and store_reports_runtime_facts and storeHasPlacement(placements, group_id, store.node_id))) {
            healthy_voter_reports +|= 1;
        }
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

fn mergeTransitionCanRollback(record: transition_state.MergeTransitionRecord) bool {
    return switch (record.phase) {
        .finalized, .rolled_back => false,
        else => true,
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

fn storeHasPlacement(placements: []const raft_reconciler.PlacementIntent, group_id: u64, node_id: u64) bool {
    for (placements) |intent| {
        if (intent.record.group_id == group_id and intent.record.local_node_id == node_id) return true;
    }
    return false;
}

fn latestHealthyGroupStatus(stores: []const table_manager.StoreRecord, group_id: u64) ?table_manager.GroupStatusReport {
    var latest_leader: ?table_manager.GroupStatusReport = null;
    var latest: ?table_manager.GroupStatusReport = null;
    for (stores) |store| {
        if (!store.live) continue;
        if (!std.mem.eql(u8, store.health_class, "healthy")) continue;
        for (store.group_statuses) |group_status| {
            if (group_status.group_id != group_id) continue;
            if (group_status.local_leader) {
                if (latest_leader == null or group_status.updated_at_millis >= latest_leader.?.updated_at_millis) {
                    latest_leader = group_status;
                }
            }
            if (latest == null or group_status.updated_at_millis >= latest.?.updated_at_millis) {
                latest = group_status;
            }
        }
    }
    return latest_leader orelse latest;
}

fn mergeHealthyGroupStatusFallback(stores: []const table_manager.StoreRecord, group_id: u64) ?MergedGroupStatus {
    var latest: ?table_manager.GroupStatusReport = null;
    var latest_leader: ?table_manager.GroupStatusReport = null;
    var latest_leader_store_id: u64 = 0;
    var ambiguous_leader = false;
    var observed_voter_count: ?u16 = null;
    var ambiguous_voter_count = false;
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

        var counted_voter_for_store = false;
        for (store.group_statuses) |status| {
            if (status.group_id != group_id) continue;
            if (latest == null or status.updated_at_millis >= latest.?.updated_at_millis) {
                latest = status;
            }
            if (status.local_voter and !counted_voter_for_store) {
                healthy_voter_reports +|= 1;
                counted_voter_for_store = true;
            }
            if (status.voter_count > 0) {
                if (observed_voter_count) |existing| {
                    if (existing != status.voter_count) ambiguous_voter_count = true;
                } else {
                    observed_voter_count = status.voter_count;
                }
            }
            transition_pending = transition_pending or status.transition_pending;
            replay_required = replay_required or status.replay_required;
            replay_caught_up = replay_caught_up or status.replay_caught_up;
            cutover_ready = cutover_ready or status.cutover_ready;
            reads_ready_after_cutover = reads_ready_after_cutover or status.reads_ready_after_cutover;
            joint_consensus = joint_consensus or status.joint_consensus;
            if (status.local_leader) {
                if (latest_leader) |existing| {
                    if (status.updated_at_millis > existing.updated_at_millis) {
                        latest_leader = status;
                        latest_leader_store_id = store.store_id;
                        ambiguous_leader = false;
                    } else if (status.updated_at_millis == existing.updated_at_millis and latest_leader_store_id != store.store_id) {
                        ambiguous_leader = true;
                    }
                } else {
                    latest_leader = status;
                    latest_leader_store_id = store.store_id;
                    ambiguous_leader = false;
                }
            }
        }
    }

    const base = latest orelse return null;
    var merged: MergedGroupStatus = .{
        .group_id = base.group_id,
        .doc_count = base.doc_count,
        .disk_bytes = base.disk_bytes,
        .empty = base.empty,
        .created_at_millis = base.created_at_millis,
        .updated_at_millis = base.updated_at_millis,
        .leader_known = false,
        .leader_store_id = 0,
        .voter_count_known = observed_voter_count != null and !ambiguous_voter_count,
        .voter_count = observed_voter_count orelse 0,
        .healthy_voter_reports = healthy_voter_reports,
        .joint_consensus = joint_consensus,
        .readiness_from_leader = false,
        .transition_pending = transition_pending,
        .replay_required = replay_required,
        .replay_caught_up = replay_caught_up,
        .cutover_ready = cutover_ready,
        .reads_ready_after_cutover = reads_ready_after_cutover,
    };
    if (!ambiguous_leader) {
        if (latest_leader) |leader| {
            merged.leader_known = true;
            merged.leader_store_id = latest_leader_store_id;
            merged.readiness_from_leader = true;
            merged.transition_pending = leader.transition_pending;
            merged.replay_required = leader.replay_required;
            merged.replay_caught_up = leader.replay_caught_up;
            merged.cutover_ready = leader.cutover_ready;
            merged.reads_ready_after_cutover = leader.reads_ready_after_cutover;
        }
    }
    return merged;
}

fn groupHasFullHealthyPlacement(current: CurrentMetadataState, group_id: u64, status: MergedGroupStatus) bool {
    const expected = countPlacementIntents(current.placement_intents, group_id);
    if (status.voter_count_known) {
        if (expected > 0 and status.voter_count != expected) return false;
        return status.healthy_voter_reports >= status.voter_count;
    }
    if (expected == 0) return true;
    return countHealthyStoresReportingGroup(current.stores, group_id) >= expected;
}

fn countHealthyStoresReportingGroup(stores: []const table_manager.StoreRecord, group_id: u64) usize {
    var count: usize = 0;
    for (stores) |store| {
        if (!store.live) continue;
        if (!std.mem.eql(u8, store.health_class, "healthy")) continue;
        for (store.group_statuses) |group_status| {
            if (group_status.group_id != group_id) continue;
            count += 1;
            break;
        }
    }
    return count;
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
    if (now_ms < status.updated_at_millis) return true;
    return now_ms - status.updated_at_millis <= config.stats_stale_after_millis;
}

fn groupStatusReadyForAutomaticPlanning(status: MergedGroupStatus) bool {
    return !status.joint_consensus and
        !status.transition_pending and
        !status.replay_required and
        !status.replay_caught_up and
        !status.cutover_ready and
        !status.reads_ready_after_cutover and
        !status.restore_pending;
}

fn countRangesForTable(ranges: []const table_manager.RangeRecord, table_id: u64) u32 {
    var count: u32 = 0;
    for (ranges) |range| {
        if (range.table_id == table_id) count += 1;
    }
    return count;
}

fn groupBusy(current: CurrentMetadataState, group_id: u64) bool {
    for (current.split_transitions) |record| {
        if (record.source_group_id == group_id or record.destination_group_id == group_id) return true;
    }
    for (current.merge_transitions) |record| {
        if (record.donor_group_id == group_id or record.receiver_group_id == group_id) return true;
    }
    return false;
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
    current: CurrentMetadataState,
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
        if (!groupIdExists(current, candidate)) return candidate;
    }
    return 0;
}

fn groupIdExists(current: CurrentMetadataState, group_id: u64) bool {
    if (!group_ids.isDataGroupId(group_id)) return true;
    if (findRangeRecord(current.ranges, group_id) != null) return true;
    for (current.split_transitions) |record| {
        if (record.source_group_id == group_id or record.destination_group_id == group_id) return true;
    }
    for (current.merge_transitions) |record| {
        if (record.donor_group_id == group_id or record.receiver_group_id == group_id) return true;
    }
    return false;
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
    return .{
        .transition_id = record.transition_id,
        .source_group_id = record.source_group_id,
        .destination_group_id = record.destination_group_id,
        .phase = record.phase,
        .split_key = if (record.split_key) |value| try alloc.dupe(u8, value) else null,
        .source_range_end = if (record.source_range_end) |value| try alloc.dupe(u8, value) else null,
        .rollback_reason = if (record.rollback_reason) |value| try alloc.dupe(u8, value) else null,
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

fn restoreBackupIdForRange(
    range: table_manager.RangeRecord,
    table: table_manager.TableRecord,
) ?[]const u8 {
    if (range.restore_backup_id.len > 0) return range.restore_backup_id;
    if (table.restore_backup_id.len > 0) return table.restore_backup_id;
    return null;
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
        std.mem.eql(u8, a.restore_location, b.restore_location) and
        std.mem.eql(u8, a.restore_snapshot_path, b.restore_snapshot_path);
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

fn cloneMergeRecord(alloc: std.mem.Allocator, record: transition_state.MergeTransitionRecord) !transition_state.MergeTransitionRecord {
    return .{
        .transition_id = record.transition_id,
        .donor_group_id = record.donor_group_id,
        .receiver_group_id = record.receiver_group_id,
        .phase = record.phase,
        .rollback_reason = if (record.rollback_reason) |value| try alloc.dupe(u8, value) else null,
        .allow_doc_identity_reassignment = record.allow_doc_identity_reassignment,
    };
}

fn splitRecordsEqual(a: transition_state.SplitTransitionRecord, b: transition_state.SplitTransitionRecord) bool {
    return a.transition_id == b.transition_id and
        a.source_group_id == b.source_group_id and
        a.destination_group_id == b.destination_group_id and
        a.phase == b.phase and
        optionalBytesEqual(a.split_key, b.split_key) and
        optionalBytesEqual(a.source_range_end, b.source_range_end) and
        optionalBytesEqual(a.rollback_reason, b.rollback_reason);
}

fn mergeRecordsEqual(a: transition_state.MergeTransitionRecord, b: transition_state.MergeTransitionRecord) bool {
    return a.transition_id == b.transition_id and
        a.donor_group_id == b.donor_group_id and
        a.receiver_group_id == b.receiver_group_id and
        a.phase == b.phase and
        a.allow_doc_identity_reassignment == b.allow_doc_identity_reassignment and
        optionalBytesEqual(a.rollback_reason, b.rollback_reason);
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
    return .{
        .status = .{
            .phase = .prepare,
            .source_split_phase = .prepare,
            .bootstrapped = false,
            .replay_required = false,
            .replay_caught_up = false,
            .cutover_ready = false,
            .destination_ready_for_reads = false,
            .source_delta_sequence = 0,
            .dest_delta_sequence = 0,
        },
    };
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

test "metadata reconciler plans transition upserts before runtime steps" {
    var manager = table_manager.TableManager.init(std.testing.allocator);
    defer manager.deinit();

    try manager.upsertTable(.{ .table_id = 10, .name = "docs" });
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
    try std.testing.expectEqual(@as(usize, 1), plan.split_upserts.len);
    try std.testing.expectEqual(@as(usize, 1), plan.merge_upserts.len);
    try std.testing.expectEqual(@as(usize, 0), plan.split_steps.len);
    try std.testing.expectEqual(@as(usize, 0), plan.merge_steps.len);
}

test "metadata reconciler emits runtime steps once transitions are committed" {
    var manager = table_manager.TableManager.init(std.testing.allocator);
    defer manager.deinit();

    try manager.upsertTable(.{ .table_id = 10, .name = "docs" });
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
        .transition_id = 7101,
        .table_id = 10,
        .source_group_id = 101,
        .destination_group_id = 103,
        .split_key = "doc:h",
    });

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
    try std.testing.expectEqual(@as(usize, 0), plan.split_upserts.len);
    try std.testing.expectEqual(@as(usize, 1), plan.split_steps.len);
    try std.testing.expectEqual(transition_controller.SplitExecutionStateTag.awaiting_source_start, plan.split_steps[0].execution.tag);
    try std.testing.expect(plan.split_steps[0].execution.actionable());
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

    var reconciler = Reconciler.init(std.testing.allocator);
    var rebalance_plan = try reconciler.computePlan(&manager, &.{ 1, 2, 3 }, &candidates_rebalance, .{
        .placement_intents = &current_rebalance,
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

    var reconciler = Reconciler.init(std.testing.allocator);
    var stable_plan = try reconciler.computePlan(&manager, &.{ 1, 2, 3, 4 }, &candidates, .{
        .placement_intents = &current,
    });
    defer stable_plan.deinit(std.testing.allocator);
    try std.testing.expect(findPlacementIntent(stable_plan.placement_upserts, 2101, 4) == null);
    try std.testing.expect(findPlacementIntent(stable_plan.placement_upserts, 2102, 4) == null);

    var forced_plan = try reconciler.computePlan(&manager, &.{ 1, 2, 3, 4 }, &candidates, .{
        .placement_intents = &current,
        .reallocate_requested = true,
    });
    defer forced_plan.deinit(std.testing.allocator);
    try std.testing.expect(forced_plan.forced_reallocation);
    try std.testing.expect(forced_plan.clear_reallocation_request);
    try std.testing.expect(
        findPlacementIntent(forced_plan.placement_upserts, 2101, 4) != null or
            findPlacementIntent(forced_plan.placement_upserts, 2102, 4) != null,
    );
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

    const now_ms: u64 = @intCast(@divTrunc(platform_time.monotonicNs(), std.time.ns_per_ms));
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
        .stores = &stores,
    });
    defer plan.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 1), plan.split_upserts.len);
    try std.testing.expectEqual(@as(u64, 4001), plan.split_upserts[0].source_group_id);
    try std.testing.expect(plan.split_upserts[0].destination_group_id != 0);
    try std.testing.expectEqualStrings("doc:m", plan.split_upserts[0].split_key.?);
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

    const now_ms: u64 = @intCast(@divTrunc(platform_time.monotonicNs(), std.time.ns_per_ms));
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

    const now_ms: u64 = @intCast(@divTrunc(platform_time.monotonicNs(), std.time.ns_per_ms));
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

    const now_ms: u64 = @intCast(@divTrunc(platform_time.monotonicNs(), std.time.ns_per_ms));
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

    const now_ms: u64 = @intCast(@divTrunc(platform_time.monotonicNs(), std.time.ns_per_ms));
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

    const now_ms: u64 = @intCast(@divTrunc(platform_time.monotonicNs(), std.time.ns_per_ms));
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
        .table_id = 41,
        .start_key = "doc:a",
        .end_key = "doc:m",
    });
    try manager.upsertRange(.{
        .group_id = 4102,
        .table_id = 41,
        .start_key = "doc:m",
        .end_key = "doc:z",
    });

    const tables = try manager.listTables(std.testing.allocator);
    defer manager.freeTables(std.testing.allocator, tables);
    const ranges = try manager.listRanges(std.testing.allocator);
    defer manager.freeRanges(std.testing.allocator, ranges);

    const now_ms: u64 = @intCast(@divTrunc(platform_time.monotonicNs(), std.time.ns_per_ms));
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
                },
                .{
                    .group_id = 4102,
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
        .min_shards_per_table = 1,
        .max_shards_per_table = 8,
    });
    var plan = try reconciler.computePlan(&manager, &.{}, &.{}, .{
        .tables = tables,
        .ranges = ranges,
        .stores = &stores,
    });
    defer plan.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 1), plan.merge_upserts.len);
    try std.testing.expectEqual(@as(u64, 4102), plan.merge_upserts[0].donor_group_id);
    try std.testing.expectEqual(@as(u64, 4101), plan.merge_upserts[0].receiver_group_id);
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

    const now_ms: u64 = @intCast(@divTrunc(platform_time.monotonicNs(), std.time.ns_per_ms));
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

    const now_ms: u64 = @intCast(@divTrunc(platform_time.monotonicNs(), std.time.ns_per_ms));
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

    const now_ms = monotonicMillis();
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
        .table_id = 416,
        .start_key = "doc:a",
        .end_key = "doc:m",
    });
    try manager.upsertRange(.{
        .group_id = 4162,
        .range_id = 6002,
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
        .table_id = 413,
        .start_key = "doc:a",
        .end_key = "doc:m",
    });
    try manager.upsertRange(.{
        .group_id = 4132,
        .range_id = 2002,
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

    const now_ms = monotonicMillis();
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

    const now_ms: u64 = @intCast(@divTrunc(platform_time.monotonicNs(), std.time.ns_per_ms));
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

    const now_ms: u64 = @intCast(@divTrunc(platform_time.monotonicNs(), std.time.ns_per_ms));
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

    const now_ms: u64 = @intCast(@divTrunc(platform_time.monotonicNs(), std.time.ns_per_ms));
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
        .table_id = 410,
        .start_key = "doc:a",
        .end_key = "doc:m",
    });
    try manager.upsertRange(.{
        .group_id = 41012,
        .table_id = 410,
        .start_key = "doc:m",
        .end_key = "doc:z",
    });

    const tables = try manager.listTables(std.testing.allocator);
    defer manager.freeTables(std.testing.allocator, tables);
    const ranges = try manager.listRanges(std.testing.allocator);
    defer manager.freeRanges(std.testing.allocator, ranges);

    const now_ms = monotonicMillis();
    var manual_clock = platform_clock.ManualClock{};
    manual_clock.setRealtimeNs(10 * 60 * std.time.ns_per_s);
    const now_realtime_ms = manual_clock.clock().nowRealtimeMs();
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
                    .updated_at_millis = now_ms,
                    .local_leader = true,
                },
                .{
                    .group_id = 41012,
                    .doc_count = 8,
                    .disk_bytes = 15,
                    .empty = false,
                    .created_at_millis = now_realtime_ms - 30 * std.time.ms_per_s,
                    .updated_at_millis = now_ms,
                    .local_leader = true,
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
        .table_id = 411,
        .start_key = "doc:a",
        .end_key = "doc:m",
    });
    try manager.upsertRange(.{
        .group_id = 41112,
        .table_id = 411,
        .start_key = "doc:m",
        .end_key = "doc:z",
    });

    const tables = try manager.listTables(std.testing.allocator);
    defer manager.freeTables(std.testing.allocator, tables);
    const ranges = try manager.listRanges(std.testing.allocator);
    defer manager.freeRanges(std.testing.allocator, ranges);

    const now_ms = monotonicMillis();
    var manual_clock = platform_clock.ManualClock{};
    manual_clock.setRealtimeNs(10 * 60 * std.time.ns_per_s);
    const now_realtime_ms = manual_clock.clock().nowRealtimeMs();
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
                    .updated_at_millis = now_ms,
                    .local_leader = true,
                },
                .{
                    .group_id = 41112,
                    .doc_count = 8,
                    .disk_bytes = 15,
                    .empty = false,
                    .created_at_millis = now_realtime_ms - 2 * 60 * std.time.ms_per_s,
                    .updated_at_millis = now_ms,
                    .local_leader = true,
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

    const now_ms: u64 = @intCast(@divTrunc(platform_time.monotonicNs(), std.time.ns_per_ms));
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

    const now_ms: u64 = @intCast(@divTrunc(platform_time.monotonicNs(), std.time.ns_per_ms));
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

    const now_ms: u64 = @intCast(@divTrunc(platform_time.monotonicNs(), std.time.ns_per_ms));
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
                },
                .{
                    .group_id = 4132,
                    .doc_count = 220,
                    .disk_bytes = 220,
                    .empty = false,
                    .updated_at_millis = now_ms,
                    .local_leader = true,
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
        .stores = &stores,
    });
    defer plan.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 1), plan.split_upserts.len);
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

    const now_ms: u64 = @intCast(@divTrunc(platform_time.monotonicNs(), std.time.ns_per_ms));
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
                },
                .{
                    .group_id = 4151,
                    .doc_count = 210,
                    .disk_bytes = 210,
                    .empty = false,
                    .updated_at_millis = now_ms,
                    .local_leader = true,
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
        .stores = &stores,
    });
    defer plan.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 1), plan.split_upserts.len);
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

    const now_ms: u64 = @intCast(@divTrunc(platform_time.monotonicNs(), std.time.ns_per_ms));
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
        .stores = &stores,
        .reallocate_requested = false,
    });
    defer blocked_plan.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 0), blocked_plan.split_upserts.len);
    try std.testing.expect(!blocked_plan.clear_reallocation_request);

    var forced_plan = try reconciler.computePlan(&manager, &.{}, &.{}, .{
        .tables = tables,
        .ranges = ranges,
        .stores = &stores,
        .reallocate_requested = true,
    });
    defer forced_plan.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 1), forced_plan.split_upserts.len);
    try std.testing.expect(forced_plan.forced_reallocation);
    try std.testing.expect(forced_plan.clear_reallocation_request);
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

    const now_ms = monotonicMillis();
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

    const now_ms = monotonicMillis();
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

    const now_ms = monotonicMillis();
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

    const now_ms = monotonicMillis();
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
            .source_group_id = 4601,
            .destination_group_id = 4603,
            .phase = .prepare,
            .split_key = "doc:g",
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

    const now_ms = monotonicMillis();
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

    const now_ms = monotonicMillis();
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
        .stores = &stores,
    });
    defer plan.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 1), plan.split_upserts.len);
    try std.testing.expectEqualStrings("doc:t", plan.split_upserts[0].split_key.?);
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

    const now_ms = monotonicMillis();
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

    const now_ms = monotonicMillis();
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
        .restore_backup_id = "snap1",
        .restore_location = "file:///tmp/backups",
    });
    try manager.upsertRange(.{
        .group_id = 4901,
        .table_id = 490,
        .start_key = "",
        .end_key = null,
    });

    const progress = [_]table_manager.RestoreProgressRecord{
        .{ .table_id = 490, .node_id = 1, .group_id = 4901, .backup_id = "snap1", .primary_restored = true, .phase = "runtime_repair" },
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

    const now_ms = monotonicMillis();
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
        .stores = &stores,
    });
    defer plan.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 1), plan.split_upserts.len);
    try std.testing.expectEqualStrings("doc:m", plan.split_upserts[0].split_key.?);
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

    const now_ms = monotonicMillis();
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
