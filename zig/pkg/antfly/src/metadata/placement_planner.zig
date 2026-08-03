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
const raft_catalog = @import("../raft/catalog.zig");
const raft_reconciler = @import("../raft/reconciler.zig");
const store_observer = @import("store_observer.zig");
const table_manager = @import("table_manager.zig");

pub const PlacementPlanner = struct {
    alloc: std.mem.Allocator,

    pub fn init(alloc: std.mem.Allocator) PlacementPlanner {
        return .{ .alloc = alloc };
    }

    pub fn planLocalIntents(
        self: *const PlacementPlanner,
        manager: *table_manager.TableManager,
        local_node_id: u64,
        candidate_node_ids: []const u64,
    ) ![]raft_reconciler.PlacementIntent {
        const all = try self.planAllIntents(manager, candidate_node_ids);
        defer self.freeIntents(self.alloc, all);

        var out = std.ArrayListUnmanaged(raft_reconciler.PlacementIntent).empty;
        errdefer {
            for (out.items) |intent| freeIntent(self.alloc, intent);
            out.deinit(self.alloc);
        }
        for (all) |intent| {
            if (intent.record.local_node_id != local_node_id) continue;
            try out.append(self.alloc, .{
                .record = intent.record,
                .store_id = intent.store_id,
                .peer_node_ids = if (intent.peer_node_ids.len == 0) &.{} else try self.alloc.dupe(u64, intent.peer_node_ids),
                .serving_state = intent.serving_state,
                .relocation_generation = intent.relocation_generation,
                .relocation_source_node_id = intent.relocation_source_node_id,
                .relocation_source_store_id = intent.relocation_source_store_id,
                .relocation_doc_count_watermark = intent.relocation_doc_count_watermark,
                .relocation_disk_bytes_watermark = intent.relocation_disk_bytes_watermark,
                .relocation_target_sequence = intent.relocation_target_sequence,
                .relocation_applied_sequence = intent.relocation_applied_sequence,
            });
        }
        return try out.toOwnedSlice(self.alloc);
    }

    pub fn planAllIntents(
        self: *const PlacementPlanner,
        manager: *table_manager.TableManager,
        candidate_node_ids: []const u64,
    ) ![]raft_reconciler.PlacementIntent {
        return try self.planAllIntentsWithCurrentAndDomains(manager, candidate_node_ids, &.{}, &.{});
    }

    pub fn planAllIntentsWithCurrent(
        self: *const PlacementPlanner,
        manager: *table_manager.TableManager,
        candidate_node_ids: []const u64,
        current_intents: []const raft_reconciler.PlacementIntent,
    ) ![]raft_reconciler.PlacementIntent {
        return try self.planAllIntentsWithCurrentAndDomains(manager, candidate_node_ids, current_intents, &.{});
    }

    pub fn planAllIntentsWithCurrentAndDomains(
        self: *const PlacementPlanner,
        manager: *table_manager.TableManager,
        candidate_node_ids: []const u64,
        current_intents: []const raft_reconciler.PlacementIntent,
        candidate_domains: []const CandidateDomain,
    ) ![]raft_reconciler.PlacementIntent {
        return try self.planAllIntentsWithCurrentAndDomainsAndProvisioningRanges(
            manager,
            candidate_node_ids,
            current_intents,
            candidate_domains,
            &.{},
        );
    }

    pub fn planAllIntentsWithCurrentAndDomainsAndProvisioningRanges(
        self: *const PlacementPlanner,
        manager: *table_manager.TableManager,
        candidate_node_ids: []const u64,
        current_intents: []const raft_reconciler.PlacementIntent,
        candidate_domains: []const CandidateDomain,
        provisioning_ranges: []const table_manager.RangeRecord,
    ) ![]raft_reconciler.PlacementIntent {
        return try self.planAllIntentsWithConstraints(
            manager,
            candidate_node_ids,
            current_intents,
            candidate_domains,
            provisioning_ranges,
            &.{},
        );
    }

    pub fn planAllIntentsWithConstraints(
        self: *const PlacementPlanner,
        manager: *table_manager.TableManager,
        candidate_node_ids: []const u64,
        current_intents: []const raft_reconciler.PlacementIntent,
        candidate_domains: []const CandidateDomain,
        provisioning_ranges: []const table_manager.RangeRecord,
        protected_group_ids: []const u64,
    ) ![]raft_reconciler.PlacementIntent {
        if (candidate_node_ids.len == 0) return error.MissingCandidateNodes;

        const tables = try manager.listTables(self.alloc);
        defer manager.freeTables(self.alloc, tables);
        const owned_ranges = try manager.listRanges(self.alloc);
        var ranges = std.ArrayListUnmanaged(table_manager.RangeRecord).fromOwnedSlice(owned_ranges);
        defer {
            for (ranges.items) |range| table_manager.freeRange(self.alloc, range);
            ranges.deinit(self.alloc);
        }
        for (provisioning_ranges) |range| {
            if (containsRangeGroup(ranges.items, range.group_id)) continue;
            try ranges.append(self.alloc, try table_manager.cloneRange(self.alloc, range));
        }
        const foreign_key_ref_ranges = try manager.listForeignKeyReferenceRanges(self.alloc);
        defer manager.freeForeignKeyReferenceRanges(self.alloc, foreign_key_ref_ranges);
        const unique_constraint_ranges = try manager.listUniqueConstraintRanges(self.alloc);
        defer manager.freeUniqueConstraintRanges(self.alloc, unique_constraint_ranges);
        const secondary_index_rebuild_ranges = try manager.listSecondaryIndexRebuildRanges(self.alloc);
        defer manager.freeSecondaryIndexRebuildRanges(self.alloc, secondary_index_rebuild_ranges);
        std.mem.sort(table_manager.RangeRecord, ranges.items, current_intents, struct {
            fn lessThan(current: []const raft_reconciler.PlacementIntent, a: table_manager.RangeRecord, b: table_manager.RangeRecord) bool {
                const a_has_current = findCurrentIntent(current, a.group_id, null) != null;
                const b_has_current = findCurrentIntent(current, b.group_id, null) != null;
                if (a_has_current != b_has_current) return a_has_current;
                return a.group_id < b.group_id;
            }
        }.lessThan);
        std.mem.sort(table_manager.ForeignKeyReferenceRangeRecord, foreign_key_ref_ranges, current_intents, struct {
            fn lessThan(current: []const raft_reconciler.PlacementIntent, a: table_manager.ForeignKeyReferenceRangeRecord, b: table_manager.ForeignKeyReferenceRangeRecord) bool {
                const a_has_current = findCurrentIntent(current, a.group_id, null) != null;
                const b_has_current = findCurrentIntent(current, b.group_id, null) != null;
                if (a_has_current != b_has_current) return a_has_current;
                return a.group_id < b.group_id;
            }
        }.lessThan);
        std.mem.sort(table_manager.UniqueConstraintRangeRecord, unique_constraint_ranges, current_intents, struct {
            fn lessThan(current: []const raft_reconciler.PlacementIntent, a: table_manager.UniqueConstraintRangeRecord, b: table_manager.UniqueConstraintRangeRecord) bool {
                const a_has_current = findCurrentIntent(current, a.group_id, null) != null;
                const b_has_current = findCurrentIntent(current, b.group_id, null) != null;
                if (a_has_current != b_has_current) return a_has_current;
                return a.group_id < b.group_id;
            }
        }.lessThan);
        std.mem.sort(table_manager.SecondaryIndexRebuildRangeRecord, secondary_index_rebuild_ranges, current_intents, struct {
            fn lessThan(current: []const raft_reconciler.PlacementIntent, a: table_manager.SecondaryIndexRebuildRangeRecord, b: table_manager.SecondaryIndexRebuildRangeRecord) bool {
                const a_has_current = findCurrentIntent(current, a.group_id, null) != null;
                const b_has_current = findCurrentIntent(current, b.group_id, null) != null;
                if (a_has_current != b_has_current) return a_has_current;
                return a.group_id < b.group_id;
            }
        }.lessThan);

        var out = std.ArrayListUnmanaged(raft_reconciler.PlacementIntent).empty;
        errdefer {
            for (out.items) |intent| freeIntent(self.alloc, intent);
            out.deinit(self.alloc);
        }

        var load_by_node = std.AutoHashMapUnmanaged(u64, usize).empty;
        defer load_by_node.deinit(self.alloc);
        var pair_by_nodes = std.AutoHashMapUnmanaged(u128, usize).empty;
        defer pair_by_nodes.deinit(self.alloc);
        var placement_load_by_node = std.AutoHashMapUnmanaged(u64, usize).empty;
        defer placement_load_by_node.deinit(self.alloc);
        try seedCurrentPlacementLoads(self.alloc, &placement_load_by_node, current_intents);
        const force_reallocate = forcedReallocationRequested(candidate_domains);

        for (ranges.items) |range| {
            const table = findTable(tables, range.table_id) orelse return error.UnknownTable;
            try appendGroupPlacementIntents(
                self.alloc,
                &out,
                &load_by_node,
                &pair_by_nodes,
                &placement_load_by_node,
                current_intents,
                range.group_id,
                candidate_node_ids,
                candidate_domains,
                table.placement_role,
                table.desired_replica_count,
                protected_group_ids,
                force_reallocate,
            );
        }
        for (foreign_key_ref_ranges) |range| {
            const table = findTable(tables, range.parent_table_id) orelse return error.UnknownTable;
            try appendGroupPlacementIntents(
                self.alloc,
                &out,
                &load_by_node,
                &pair_by_nodes,
                &placement_load_by_node,
                current_intents,
                range.group_id,
                candidate_node_ids,
                candidate_domains,
                table.placement_role,
                table.desired_replica_count,
                protected_group_ids,
                force_reallocate,
            );
        }
        for (unique_constraint_ranges) |range| {
            const table = findTable(tables, range.table_id) orelse return error.UnknownTable;
            try appendGroupPlacementIntents(
                self.alloc,
                &out,
                &load_by_node,
                &pair_by_nodes,
                &placement_load_by_node,
                current_intents,
                range.group_id,
                candidate_node_ids,
                candidate_domains,
                table.placement_role,
                table.desired_replica_count,
                protected_group_ids,
                force_reallocate,
            );
        }
        for (secondary_index_rebuild_ranges) |range| {
            const table = findTable(tables, range.table_id) orelse return error.UnknownTable;
            try appendGroupPlacementIntents(
                self.alloc,
                &out,
                &load_by_node,
                &pair_by_nodes,
                &placement_load_by_node,
                current_intents,
                range.group_id,
                candidate_node_ids,
                candidate_domains,
                table.placement_role,
                table.desired_replica_count,
                protected_group_ids,
                force_reallocate,
            );
        }

        return try out.toOwnedSlice(self.alloc);
    }

    pub fn freeIntents(_: *const PlacementPlanner, alloc: std.mem.Allocator, intents: []raft_reconciler.PlacementIntent) void {
        for (intents) |intent| freeIntent(alloc, intent);
        alloc.free(intents);
    }
};

fn containsRangeGroup(ranges: []const table_manager.RangeRecord, group_id: u64) bool {
    for (ranges) |range| {
        if (range.group_id == group_id) return true;
    }
    return false;
}

pub const CandidateDomain = struct {
    node_id: u64,
    store_id: u64 = 0,
    role: []const u8,
    failure_domain: []const u8,
    priority: u8 = 0,
    status_tag: store_observer.PlacementStatusTag = .preferred,
    available_bytes: u64 = 0,
    lease_pressure: u32 = 0,
    read_load: u32 = 0,
    write_load: u32 = 0,
    retain_current: bool = true,
    force_reallocate: bool = false,
};

fn findTable(records: []const table_manager.TableRecord, table_id: u64) ?table_manager.TableRecord {
    for (records) |record| {
        if (record.table_id == table_id) return record;
    }
    return null;
}

fn appendGroupPlacementIntents(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(raft_reconciler.PlacementIntent),
    load_by_node: *std.AutoHashMapUnmanaged(u64, usize),
    pair_by_nodes: *std.AutoHashMapUnmanaged(u128, usize),
    placement_load_by_node: *std.AutoHashMapUnmanaged(u64, usize),
    current_intents: []const raft_reconciler.PlacementIntent,
    group_id: u64,
    candidate_node_ids: []const u64,
    candidate_domains: []const CandidateDomain,
    placement_role: []const u8,
    desired_replica_count: u32,
    protected_group_ids: []const u64,
    force_reallocate: bool,
) !void {
    const replica_count = @min(@as(usize, desired_replica_count), countEligibleCandidates(candidate_node_ids, candidate_domains, placement_role));
    if (replica_count == 0) return;
    const has_current_group = findCurrentIntent(current_intents, group_id, null) != null;

    var selected = std.ArrayListUnmanaged(u64).empty;
    defer selected.deinit(alloc);
    const membership_repair = groupNeedsMembershipRepair(
        current_intents,
        group_id,
        replica_count,
        candidate_node_ids,
        candidate_domains,
        placement_role,
    );
    const protect_current_members =
        std.sort.binarySearch(u64, protected_group_ids, group_id, compareNodeId) != null or
        membership_repair;
    const preserved = try collectCurrentPeers(
        alloc,
        current_intents,
        group_id,
        candidate_node_ids,
        candidate_domains,
        placement_role,
        protect_current_members,
        force_reallocate,
    );
    defer alloc.free(preserved);
    const forced_move = if (force_reallocate and
        !protect_current_members and
        !groupPlacementTransitionInFlight(current_intents, group_id) and
        preserved.len >= replica_count)
        selectBeneficialForcedMove(
            preserved,
            candidate_node_ids,
            candidate_domains,
            placement_role,
            placement_load_by_node,
        )
    else
        null;
    var selection_exclusions = std.ArrayListUnmanaged(u64).empty;
    defer selection_exclusions.deinit(alloc);
    for (preserved) |node_id| {
        if (forced_move) |move| {
            if (node_id == move.source_node_id) continue;
        }
        if (selected.items.len >= replica_count) break;
        try selected.append(alloc, node_id);
        try selection_exclusions.append(alloc, node_id);
    }
    for (current_intents) |intent| {
        if (intent.record.group_id != group_id or
            containsNode(selection_exclusions.items, intent.record.local_node_id))
            continue;
        if (replicaIdExistsOnSelectedNode(current_intents, group_id, intent.record.replica_id, selected.items))
            try selection_exclusions.append(alloc, intent.record.local_node_id);
    }
    if (forced_move) |move| try selected.append(alloc, move.target_node_id);

    const start = @as(usize, @intCast(group_id % candidate_node_ids.len));
    const ordered = if (membership_repair)
        try orderRepairCandidates(alloc, candidate_node_ids, group_id)
    else
        try orderCandidates(alloc, candidate_node_ids, candidate_domains, start, load_by_node);
    defer alloc.free(ordered);
    while (selected.items.len < replica_count) {
        const node_id = (if (membership_repair)
            chooseNextRepairCandidate(ordered, selection_exclusions.items, candidate_domains, placement_role)
        else
            chooseNextCandidate(ordered, selection_exclusions.items, pair_by_nodes, candidate_domains, placement_role)) orelse break;
        if (containsNode(selected.items, node_id)) break;
        try selected.append(alloc, node_id);
        try selection_exclusions.append(alloc, node_id);
    }
    try updateProjectedPlacementLoads(
        alloc,
        placement_load_by_node,
        current_intents,
        group_id,
        selected.items,
    );

    const dropped_sources = try collectDroppedCurrentPeers(alloc, current_intents, group_id, selected.items);
    defer alloc.free(dropped_sources);
    var dropped_source_index: usize = 0;

    const peers = try alloc.dupe(u64, selected.items);
    defer alloc.free(peers);
    var assigned_replica_ids = std.ArrayListUnmanaged(u64).empty;
    defer assigned_replica_ids.deinit(alloc);
    for (peers) |node_id| {
        const entry = try load_by_node.getOrPut(alloc, node_id);
        if (!entry.found_existing) entry.value_ptr.* = 0;
        entry.value_ptr.* += 1;
    }
    for (peers, 0..) |left, i| {
        for (peers[i + 1 ..]) |right| {
            const entry = try pair_by_nodes.getOrPut(alloc, pairKey(left, right));
            if (!entry.found_existing) entry.value_ptr.* = 0;
            entry.value_ptr.* += 1;
        }
    }

    for (peers, 0..) |node_id, replica_index| {
        const existing_intent = findCurrentIntent(current_intents, group_id, node_id);
        const bootstrap_mode: raft_catalog.ReplicaBootstrapMode = if (existing_intent) |existing|
            existing.record.bootstrap_mode
        else if (!has_current_group)
            .empty
        else
            .persisted;
        var source_intent: ?raft_reconciler.PlacementIntent = null;
        var replacement_source: ?raft_reconciler.PlacementIntent = null;
        if (existing_intent == null and has_current_group) {
            if (dropped_source_index < dropped_sources.len) {
                replacement_source = dropped_sources[dropped_source_index];
                source_intent = replacement_source;
            } else {
                source_intent = findRelocationSourceIntent(current_intents, group_id);
            }
            dropped_source_index += 1;
        }
        const serving_state: raft_reconciler.PlacementServingState = if (existing_intent) |existing|
            existing.serving_state
        else if (has_current_group)
            .bootstrapping
        else
            .serving;
        const preferred_replica_id = if (existing_intent) |existing|
            existing.record.replica_id
        else if (replacement_source) |source|
            source.record.replica_id
        else
            @as(u64, @intCast(replica_index + 1));
        const replica_id = if (!containsNode(assigned_replica_ids.items, preferred_replica_id))
            preferred_replica_id
        else
            firstUnusedReplicaId(assigned_replica_ids.items, replica_count);
        try assigned_replica_ids.append(alloc, replica_id);
        try out.append(alloc, .{
            .record = .{
                .group_id = group_id,
                .replica_id = replica_id,
                .local_node_id = node_id,
                .bootstrap_mode = bootstrap_mode,
            },
            .store_id = chooseStoreIdForNode(current_intents, candidate_domains, group_id, node_id),
            .peer_node_ids = if (peers.len == 0) &.{} else try alloc.dupe(u64, peers),
            .serving_state = serving_state,
            .relocation_generation = if (existing_intent) |existing| existing.relocation_generation else 0,
            .relocation_source_node_id = if (existing_intent) |existing| existing.relocation_source_node_id else if (source_intent) |source| source.record.local_node_id else 0,
            .relocation_source_store_id = if (existing_intent) |existing| existing.relocation_source_store_id else if (source_intent) |source| source.store_id else 0,
            .relocation_doc_count_watermark = if (existing_intent) |existing| existing.relocation_doc_count_watermark else 0,
            .relocation_disk_bytes_watermark = if (existing_intent) |existing| existing.relocation_disk_bytes_watermark else 0,
            .relocation_target_sequence = if (existing_intent) |existing| existing.relocation_target_sequence else 0,
            .relocation_applied_sequence = if (existing_intent) |existing| existing.relocation_applied_sequence else 0,
        });
    }
}

fn chooseStoreIdForNode(
    current_intents: []const raft_reconciler.PlacementIntent,
    candidate_domains: []const CandidateDomain,
    group_id: u64,
    node_id: u64,
) u64 {
    if (findCurrentIntent(current_intents, group_id, node_id)) |existing| {
        if (existing.store_id != 0 and nodeHasStoreCandidate(candidate_domains, node_id, existing.store_id)) {
            return existing.store_id;
        }
    }
    for (candidate_domains) |candidate| {
        if (candidate.node_id == node_id and candidate.store_id != 0) return candidate.store_id;
    }
    return 0;
}

fn findRelocationSourceIntent(
    current_intents: []const raft_reconciler.PlacementIntent,
    group_id: u64,
) ?raft_reconciler.PlacementIntent {
    for (current_intents) |intent| {
        if (intent.record.group_id != group_id) continue;
        if (intent.serving_state == .serving) return intent;
    }
    for (current_intents) |intent| {
        if (intent.record.group_id == group_id and intent.serving_state == .draining) return intent;
    }
    for (current_intents) |intent| {
        if (intent.record.group_id == group_id and intent.serving_state == .retiring) return intent;
    }
    for (current_intents) |intent| {
        if (intent.record.group_id == group_id) return intent;
    }
    return null;
}

fn nodeHasStoreCandidate(candidate_domains: []const CandidateDomain, node_id: u64, store_id: u64) bool {
    for (candidate_domains) |candidate| {
        if (candidate.node_id == node_id and candidate.store_id == store_id) return true;
    }
    return false;
}

fn chooseNextCandidate(
    ordered: []const u64,
    selected: []const u64,
    pair_by_nodes: *const std.AutoHashMapUnmanaged(u128, usize),
    candidate_domains: []const CandidateDomain,
    placement_role: []const u8,
) ?u64 {
    var best_node: ?u64 = null;
    var best_domain_score: usize = std.math.maxInt(usize);
    var best_pair_score: usize = std.math.maxInt(usize);
    var best_order_index: usize = std.math.maxInt(usize);
    for (ordered, 0..) |node_id, order_index| {
        if (containsNode(selected, node_id)) continue;
        if (!candidateSelectable(candidate_domains, node_id, placement_role)) continue;
        const domain_score = domainScore(selected, node_id, candidate_domains);
        const pair_score = pairScore(selected, node_id, pair_by_nodes);
        if (best_node == null or
            domain_score < best_domain_score or
            (domain_score == best_domain_score and pair_score < best_pair_score) or
            (domain_score == best_domain_score and pair_score == best_pair_score and order_index < best_order_index))
        {
            best_node = node_id;
            best_domain_score = domain_score;
            best_pair_score = pair_score;
            best_order_index = order_index;
        }
    }
    return best_node;
}

fn chooseNextRepairCandidate(
    ordered: []const u64,
    selected: []const u64,
    candidate_domains: []const CandidateDomain,
    placement_role: []const u8,
) ?u64 {
    var best_node: ?u64 = null;
    var best_domain_score: usize = std.math.maxInt(usize);
    var best_order_index: usize = std.math.maxInt(usize);
    for (ordered, 0..) |node_id, order_index| {
        if (containsNode(selected, node_id)) continue;
        if (!candidateSelectable(candidate_domains, node_id, placement_role)) continue;
        const domain_score = domainScore(selected, node_id, candidate_domains);
        if (best_node == null or
            domain_score < best_domain_score or
            (domain_score == best_domain_score and order_index < best_order_index))
        {
            best_node = node_id;
            best_domain_score = domain_score;
            best_order_index = order_index;
        }
    }
    return best_node;
}

fn pairScore(
    selected: []const u64,
    candidate_node_id: u64,
    pair_by_nodes: *const std.AutoHashMapUnmanaged(u128, usize),
) usize {
    var total: usize = 0;
    for (selected) |existing| {
        total += pair_by_nodes.get(pairKey(existing, candidate_node_id)) orelse 0;
    }
    return total;
}

fn domainScore(
    selected: []const u64,
    candidate_node_id: u64,
    candidate_domains: []const CandidateDomain,
) usize {
    const candidate_domain = findFailureDomain(candidate_domains, candidate_node_id);
    if (candidate_domain.len == 0) return 0;

    var total: usize = 0;
    for (selected) |existing| {
        if (std.mem.eql(u8, candidate_domain, findFailureDomain(candidate_domains, existing))) total += 1;
    }
    return total;
}

fn findFailureDomain(candidate_domains: []const CandidateDomain, node_id: u64) []const u8 {
    for (candidate_domains) |candidate| {
        if (candidate.node_id == node_id) return candidate.failure_domain;
    }
    return "";
}

fn countEligibleCandidates(
    candidate_node_ids: []const u64,
    candidate_domains: []const CandidateDomain,
    placement_role: []const u8,
) usize {
    var count: usize = 0;
    for (candidate_node_ids) |node_id| {
        if (candidateSelectable(candidate_domains, node_id, placement_role)) count += 1;
    }
    return count;
}

fn candidateSelectable(candidate_domains: []const CandidateDomain, node_id: u64, placement_role: []const u8) bool {
    if (placement_role.len == 0 and candidate_domains.len == 0) return true;
    for (candidate_domains) |candidate| {
        if (candidate.node_id != node_id) continue;
        if (candidate.status_tag == .excluded) return false;
        return table_manager.placementRoleCompatible(placement_role, candidate.role);
    }
    return table_manager.placementRoleCompatible(placement_role, "data");
}

fn candidateRoleMatches(candidate_domains: []const CandidateDomain, node_id: u64, placement_role: []const u8) bool {
    if (placement_role.len == 0) return true;
    for (candidate_domains) |candidate| {
        if (candidate.node_id != node_id) continue;
        return table_manager.placementRoleCompatible(placement_role, candidate.role);
    }
    return table_manager.placementRoleCompatible(placement_role, "data");
}

fn pairKey(a: u64, b: u64) u128 {
    const lo = @min(a, b);
    const hi = @max(a, b);
    return (@as(u128, hi) << 64) | @as(u128, lo);
}

fn orderCandidates(
    alloc: std.mem.Allocator,
    candidate_node_ids: []const u64,
    candidate_domains: []const CandidateDomain,
    start: usize,
    load_by_node: *const std.AutoHashMapUnmanaged(u64, usize),
) ![]u64 {
    const Candidate = struct {
        node_id: u64,
        load: usize,
        priority: u8,
        available_bytes: u64,
        lease_pressure: u32,
        load_pressure: u32,
        rotated_index: usize,
    };

    const ranked = try alloc.alloc(Candidate, candidate_node_ids.len);
    defer alloc.free(ranked);
    for (candidate_node_ids, 0..) |node_id, i| {
        ranked[i] = .{
            .node_id = node_id,
            .load = load_byNode(load_by_node, node_id),
            .priority = candidatePriority(candidate_domains, node_id),
            .available_bytes = candidateAvailableBytes(candidate_domains, node_id),
            .lease_pressure = candidateLeasePressure(candidate_domains, node_id),
            .load_pressure = candidateLoadPressure(candidate_domains, node_id),
            .rotated_index = if (candidate_node_ids.len == 0) 0 else (i + candidate_node_ids.len - start) % candidate_node_ids.len,
        };
    }
    std.mem.sort(Candidate, ranked, {}, struct {
        fn lessThan(_: void, a: Candidate, b: Candidate) bool {
            if (a.load != b.load) return a.load < b.load;
            if (a.priority != b.priority) return a.priority < b.priority;
            if (a.lease_pressure != b.lease_pressure) return a.lease_pressure < b.lease_pressure;
            if (a.load_pressure != b.load_pressure) return a.load_pressure < b.load_pressure;
            if (a.available_bytes != b.available_bytes) return a.available_bytes > b.available_bytes;
            if (a.rotated_index != b.rotated_index) return a.rotated_index < b.rotated_index;
            return a.node_id < b.node_id;
        }
    }.lessThan);

    const out = try alloc.alloc(u64, candidate_node_ids.len);
    for (ranked, 0..) |candidate, i| out[i] = candidate.node_id;
    return out;
}

fn orderRepairCandidates(
    alloc: std.mem.Allocator,
    candidate_node_ids: []const u64,
    group_id: u64,
) ![]u64 {
    const Candidate = struct {
        node_id: u64,
        repair_order: u64,
    };

    const ranked = try alloc.alloc(Candidate, candidate_node_ids.len);
    defer alloc.free(ranked);
    for (candidate_node_ids, 0..) |node_id, i| {
        ranked[i] = .{
            .node_id = node_id,
            // Membership repair must choose the same replacement from a stale
            // projection retry. Live load and capacity signals are deliberately
            // excluded; they can change between otherwise identical plans.
            .repair_order = (node_id *% 0x9e3779b97f4a7c15) ^ group_id,
        };
    }
    std.mem.sort(Candidate, ranked, {}, struct {
        fn lessThan(_: void, a: Candidate, b: Candidate) bool {
            if (a.repair_order != b.repair_order) return a.repair_order < b.repair_order;
            return a.node_id < b.node_id;
        }
    }.lessThan);

    const out = try alloc.alloc(u64, candidate_node_ids.len);
    for (ranked, 0..) |candidate, i| out[i] = candidate.node_id;
    return out;
}

fn collectCurrentPeers(
    alloc: std.mem.Allocator,
    current_intents: []const raft_reconciler.PlacementIntent,
    group_id: u64,
    candidate_node_ids: []const u64,
    candidate_domains: []const CandidateDomain,
    placement_role: []const u8,
    protect_current_members: bool,
    force_reallocate: bool,
) ![]u64 {
    const ExistingPeer = struct {
        node_id: u64,
        replica_id: u64,
        relocation_generation: u64,
        serving_state: raft_reconciler.PlacementServingState,
    };

    var peers = std.ArrayListUnmanaged(ExistingPeer).empty;
    errdefer peers.deinit(alloc);
    const transition_in_flight = groupPlacementTransitionInFlight(current_intents, group_id);
    for (current_intents) |intent| {
        if (intent.record.group_id != group_id) continue;
        if (!containsNode(candidate_node_ids, intent.record.local_node_id)) continue;
        if (!candidateRoleMatches(candidate_domains, intent.record.local_node_id, placement_role)) continue;
        // Explicit reallocation makes otherwise healthy candidates non-sticky.
        // Do not retarget a group whose previous placement change has not
        // converged yet: repeated requests would otherwise accumulate a new
        // voter on every reconciliation round. An excluded candidate remains
        // replaceable so node drain and failure repair can still make progress.
        if (!candidateRetentionAllowed(candidate_domains, intent.record.local_node_id, protect_current_members) and
            !((transition_in_flight or force_reallocate) and
                candidateEligibleForInFlightRetention(candidate_domains, intent.record.local_node_id)))
        {
            continue;
        }
        var duplicate = false;
        for (peers.items) |peer| {
            if (peer.node_id == intent.record.local_node_id) {
                duplicate = true;
                break;
            }
        }
        if (duplicate) continue;
        try peers.append(alloc, .{
            .node_id = intent.record.local_node_id,
            .replica_id = intent.record.replica_id,
            .relocation_generation = intent.relocation_generation,
            .serving_state = intent.serving_state,
        });
    }
    std.mem.sort(ExistingPeer, peers.items, {}, struct {
        fn lessThan(_: void, a: ExistingPeer, b: ExistingPeer) bool {
            if (a.replica_id != b.replica_id) return a.replica_id < b.replica_id;
            const a_rank = placementServingStateRank(a.serving_state);
            const b_rank = placementServingStateRank(b.serving_state);
            if (a_rank != b_rank) return a_rank > b_rank;
            if (a.relocation_generation != b.relocation_generation)
                return a.relocation_generation > b.relocation_generation;
            return a.node_id < b.node_id;
        }
    }.lessThan);

    var unique_count: usize = 0;
    for (peers.items) |peer| {
        if (unique_count > 0 and peers.items[unique_count - 1].replica_id == peer.replica_id) continue;
        peers.items[unique_count] = peer;
        unique_count += 1;
    }
    peers.items.len = unique_count;

    const out = try alloc.alloc(u64, peers.items.len);
    for (peers.items, 0..) |peer, i| out[i] = peer.node_id;
    peers.deinit(alloc);
    return out;
}

fn placementServingStateRank(state: raft_reconciler.PlacementServingState) u8 {
    return switch (state) {
        .serving => 6,
        .cutover_ready => 5,
        .replaying => 4,
        .bootstrapping => 3,
        .planned => 2,
        .draining => 1,
        .retiring => 0,
    };
}

fn collectDroppedCurrentPeers(
    alloc: std.mem.Allocator,
    current_intents: []const raft_reconciler.PlacementIntent,
    group_id: u64,
    selected_nodes: []const u64,
) ![]raft_reconciler.PlacementIntent {
    var dropped = std.ArrayListUnmanaged(raft_reconciler.PlacementIntent).empty;
    errdefer dropped.deinit(alloc);
    for (current_intents) |intent| {
        if (intent.record.group_id != group_id) continue;
        if (containsNode(selected_nodes, intent.record.local_node_id)) continue;
        try dropped.append(alloc, intent);
    }
    std.mem.sort(raft_reconciler.PlacementIntent, dropped.items, {}, struct {
        fn lessThan(_: void, a: raft_reconciler.PlacementIntent, b: raft_reconciler.PlacementIntent) bool {
            if (a.record.replica_id != b.record.replica_id) return a.record.replica_id < b.record.replica_id;
            return a.record.local_node_id < b.record.local_node_id;
        }
    }.lessThan);
    return try dropped.toOwnedSlice(alloc);
}

fn findCurrentIntent(
    current_intents: []const raft_reconciler.PlacementIntent,
    group_id: u64,
    maybe_node_id: ?u64,
) ?raft_reconciler.PlacementIntent {
    for (current_intents) |intent| {
        if (intent.record.group_id != group_id) continue;
        if (maybe_node_id) |node_id| {
            if (intent.record.local_node_id != node_id) continue;
        }
        return intent;
    }
    return null;
}

fn replicaIdExistsOnSelectedNode(
    current_intents: []const raft_reconciler.PlacementIntent,
    group_id: u64,
    replica_id: u64,
    selected_nodes: []const u64,
) bool {
    for (current_intents) |intent| {
        if (intent.record.group_id == group_id and
            intent.record.replica_id == replica_id and
            containsNode(selected_nodes, intent.record.local_node_id))
            return true;
    }
    return false;
}

fn firstUnusedReplicaId(used_replica_ids: []const u64, replica_count: usize) u64 {
    for (1..replica_count + 1) |candidate| {
        if (!containsNode(used_replica_ids, @intCast(candidate))) return @intCast(candidate);
    }
    unreachable;
}

fn load_byNode(load_by_node: *const std.AutoHashMapUnmanaged(u64, usize), node_id: u64) usize {
    return load_by_node.get(node_id) orelse 0;
}

fn candidatePriority(candidate_domains: []const CandidateDomain, node_id: u64) u8 {
    for (candidate_domains) |candidate| {
        if (candidate.node_id == node_id) return candidate.priority;
    }
    return 0;
}

fn candidateAvailableBytes(candidate_domains: []const CandidateDomain, node_id: u64) u64 {
    for (candidate_domains) |candidate| {
        if (candidate.node_id == node_id) return candidate.available_bytes;
    }
    return 0;
}

fn candidateLeasePressure(candidate_domains: []const CandidateDomain, node_id: u64) u32 {
    for (candidate_domains) |candidate| {
        if (candidate.node_id == node_id) return candidate.lease_pressure;
    }
    return 0;
}

fn candidateLoadPressure(candidate_domains: []const CandidateDomain, node_id: u64) u32 {
    for (candidate_domains) |candidate| {
        if (candidate.node_id == node_id) return candidate.read_load + candidate.write_load;
    }
    return 0;
}

fn compareNodeId(expected: u64, candidate: u64) std.math.Order {
    return std.math.order(expected, candidate);
}

fn candidateRetentionAllowed(candidate_domains: []const CandidateDomain, node_id: u64, protect_current_member: bool) bool {
    for (candidate_domains) |candidate| {
        if (candidate.node_id == node_id) {
            return candidate.status_tag != .excluded and (protect_current_member or candidate.retain_current);
        }
    }
    return true;
}

fn groupNeedsMembershipRepair(
    current_intents: []const raft_reconciler.PlacementIntent,
    group_id: u64,
    replica_count: usize,
    candidate_node_ids: []const u64,
    candidate_domains: []const CandidateDomain,
    placement_role: []const u8,
) bool {
    var current_count: usize = 0;
    for (current_intents, 0..) |intent, i| {
        if (intent.record.group_id != group_id) continue;
        current_count += 1;
        if (!containsNode(candidate_node_ids, intent.record.local_node_id) or
            !candidateSelectable(candidate_domains, intent.record.local_node_id, placement_role))
            return true;
        for (current_intents[i + 1 ..]) |other| {
            if (other.record.group_id == group_id and
                (other.record.replica_id == intent.record.replica_id or
                    other.record.local_node_id == intent.record.local_node_id))
                return true;
        }
    }
    return current_count != 0 and current_count != replica_count;
}

fn candidateEligibleForInFlightRetention(candidate_domains: []const CandidateDomain, node_id: u64) bool {
    for (candidate_domains) |candidate| {
        if (candidate.node_id == node_id) return candidate.status_tag != .excluded;
    }
    return true;
}

fn forcedReallocationRequested(candidate_domains: []const CandidateDomain) bool {
    for (candidate_domains) |candidate| {
        if (candidate.force_reallocate) return true;
    }
    return false;
}

const ForcedMove = struct {
    source_node_id: u64,
    target_node_id: u64,
};

fn selectBeneficialForcedMove(
    current_nodes: []const u64,
    candidate_node_ids: []const u64,
    candidate_domains: []const CandidateDomain,
    placement_role: []const u8,
    placement_load_by_node: *const std.AutoHashMapUnmanaged(u64, usize),
) ?ForcedMove {
    const current_domain_conflicts = placementDomainConflicts(current_nodes, candidate_domains);
    var best: ?ForcedMove = null;
    var best_domain_improvement: usize = 0;
    var best_load_improvement: usize = 0;
    var best_target_priority: u8 = std.math.maxInt(u8);
    var best_target_pressure: u64 = std.math.maxInt(u64);

    for (current_nodes) |source_node_id| {
        const source_load = load_byNode(placement_load_by_node, source_node_id);
        for (candidate_node_ids) |target_node_id| {
            if (containsNode(current_nodes, target_node_id)) continue;
            if (!candidateSelectable(candidate_domains, target_node_id, placement_role)) continue;

            const target_load = load_byNode(placement_load_by_node, target_node_id);
            const load_improvement = if (source_load > target_load and source_load - target_load > 1)
                source_load - target_load - 1
            else
                0;
            const domain_conflicts = placementDomainConflictsAfterMove(
                current_nodes,
                source_node_id,
                target_node_id,
                candidate_domains,
            );
            const domain_improvement = if (current_domain_conflicts > domain_conflicts)
                current_domain_conflicts - domain_conflicts
            else
                0;
            if (domain_improvement == 0 and load_improvement == 0) continue;

            const target_priority = candidatePriority(candidate_domains, target_node_id);
            const target_pressure = @as(u64, candidateLeasePressure(candidate_domains, target_node_id)) +
                @as(u64, candidateLoadPressure(candidate_domains, target_node_id));
            const better = best == null or
                domain_improvement > best_domain_improvement or
                (domain_improvement == best_domain_improvement and load_improvement > best_load_improvement) or
                (domain_improvement == best_domain_improvement and load_improvement == best_load_improvement and target_priority < best_target_priority) or
                (domain_improvement == best_domain_improvement and load_improvement == best_load_improvement and target_priority == best_target_priority and target_pressure < best_target_pressure) or
                (domain_improvement == best_domain_improvement and load_improvement == best_load_improvement and target_priority == best_target_priority and target_pressure == best_target_pressure and
                    (target_node_id < best.?.target_node_id or
                        (target_node_id == best.?.target_node_id and source_node_id > best.?.source_node_id)));
            if (!better) continue;

            best = .{
                .source_node_id = source_node_id,
                .target_node_id = target_node_id,
            };
            best_domain_improvement = domain_improvement;
            best_load_improvement = load_improvement;
            best_target_priority = target_priority;
            best_target_pressure = target_pressure;
        }
    }
    return best;
}

fn placementDomainConflicts(nodes: []const u64, candidate_domains: []const CandidateDomain) usize {
    var conflicts: usize = 0;
    for (nodes, 0..) |left, i| {
        const left_domain = findFailureDomain(candidate_domains, left);
        if (left_domain.len == 0) continue;
        for (nodes[i + 1 ..]) |right| {
            if (std.mem.eql(u8, left_domain, findFailureDomain(candidate_domains, right))) conflicts += 1;
        }
    }
    return conflicts;
}

fn placementDomainConflictsAfterMove(
    nodes: []const u64,
    source_node_id: u64,
    target_node_id: u64,
    candidate_domains: []const CandidateDomain,
) usize {
    var conflicts: usize = 0;
    for (nodes, 0..) |raw_left, i| {
        const left = if (raw_left == source_node_id) target_node_id else raw_left;
        const left_domain = findFailureDomain(candidate_domains, left);
        if (left_domain.len == 0) continue;
        for (nodes[i + 1 ..]) |raw_right| {
            const right = if (raw_right == source_node_id) target_node_id else raw_right;
            if (std.mem.eql(u8, left_domain, findFailureDomain(candidate_domains, right))) conflicts += 1;
        }
    }
    return conflicts;
}

fn seedCurrentPlacementLoads(
    alloc: std.mem.Allocator,
    placement_load_by_node: *std.AutoHashMapUnmanaged(u64, usize),
    current_intents: []const raft_reconciler.PlacementIntent,
) !void {
    for (current_intents) |intent| {
        const entry = try placement_load_by_node.getOrPut(alloc, intent.record.local_node_id);
        if (!entry.found_existing) entry.value_ptr.* = 0;
        entry.value_ptr.* += 1;
    }
}

fn updateProjectedPlacementLoads(
    alloc: std.mem.Allocator,
    placement_load_by_node: *std.AutoHashMapUnmanaged(u64, usize),
    current_intents: []const raft_reconciler.PlacementIntent,
    group_id: u64,
    selected_nodes: []const u64,
) !void {
    for (current_intents) |intent| {
        if (intent.record.group_id != group_id) continue;
        if (containsNode(selected_nodes, intent.record.local_node_id)) continue;
        if (placement_load_by_node.getPtr(intent.record.local_node_id)) |load| {
            if (load.* > 0) load.* -= 1;
        }
    }
    for (selected_nodes) |node_id| {
        if (findCurrentIntent(current_intents, group_id, node_id) != null) continue;
        const entry = try placement_load_by_node.getOrPut(alloc, node_id);
        if (!entry.found_existing) entry.value_ptr.* = 0;
        entry.value_ptr.* += 1;
    }
}

fn groupPlacementTransitionInFlight(
    current_intents: []const raft_reconciler.PlacementIntent,
    group_id: u64,
) bool {
    for (current_intents) |intent| {
        if (intent.record.group_id != group_id) continue;
        if (intent.serving_state != .serving) return true;
    }
    return false;
}

fn containsNode(nodes: []const u64, node_id: u64) bool {
    for (nodes) |existing| {
        if (existing == node_id) return true;
    }
    return false;
}

fn freeIntent(alloc: std.mem.Allocator, intent: raft_reconciler.PlacementIntent) void {
    if (intent.peer_node_ids.len > 0) alloc.free(intent.peer_node_ids);
}

test "placement planner derives stable local intents from topology" {
    var manager = table_manager.TableManager.init(std.testing.allocator);
    defer manager.deinit();

    try manager.upsertTable(.{ .table_id = 7, .name = "docs", .desired_replica_count = 3 });
    try manager.upsertRange(.{
        .group_id = 701,
        .table_id = 7,
        .start_key = "doc:a",
        .end_key = "doc:z",
    });

    var planner = PlacementPlanner.init(std.testing.allocator);
    const intents = try planner.planLocalIntents(&manager, 2, &.{ 1, 2, 3 });
    defer planner.freeIntents(std.testing.allocator, intents);

    try std.testing.expectEqual(@as(usize, 1), intents.len);
    try std.testing.expectEqual(@as(u64, 701), intents[0].record.group_id);
    try std.testing.expectEqual(@as(u64, 2), intents[0].record.local_node_id);
    try std.testing.expectEqual(@as(usize, 3), intents[0].peer_node_ids.len);
}

test "placement planner places foreign key reference owner ranges" {
    var manager = table_manager.TableManager.init(std.testing.allocator);
    defer manager.deinit();

    try manager.upsertTable(.{ .table_id = 10, .name = "orders", .desired_replica_count = 3 });
    try manager.upsertTable(.{ .table_id = 20, .name = "customers", .placement_role = "serving", .desired_replica_count = 2 });
    try manager.upsertForeignKeyReferenceRange(.{
        .group_id = 3001,
        .child_table_id = 10,
        .constraint_name = "orders_customer_id_fkey",
        .parent_table_id = 20,
        .start_parent_key = "",
        .end_parent_key = null,
    });

    const candidate_domains = [_]CandidateDomain{
        .{ .node_id = 1, .store_id = 101, .role = "data", .failure_domain = "a" },
        .{ .node_id = 2, .store_id = 102, .role = "serving", .failure_domain = "b" },
        .{ .node_id = 3, .store_id = 103, .role = "serving", .failure_domain = "c" },
    };
    var planner = PlacementPlanner.init(std.testing.allocator);
    const intents = try planner.planAllIntentsWithCurrentAndDomains(&manager, &.{ 1, 2, 3 }, &.{}, &candidate_domains);
    defer planner.freeIntents(std.testing.allocator, intents);

    var fk_owner_intents: usize = 0;
    for (intents) |intent| {
        if (intent.record.group_id != 3001) continue;
        fk_owner_intents += 1;
        try std.testing.expect(intent.record.local_node_id == 2 or intent.record.local_node_id == 3);
        try std.testing.expect(intent.store_id == 102 or intent.store_id == 103);
    }
    try std.testing.expectEqual(@as(usize, 2), fk_owner_intents);
}

test "placement planner places unique constraint owner ranges" {
    var manager = table_manager.TableManager.init(std.testing.allocator);
    defer manager.deinit();

    try manager.upsertTable(.{ .table_id = 10, .name = "users", .placement_role = "serving", .desired_replica_count = 2 });
    try manager.upsertUniqueConstraintRange(.{
        .group_id = 4001,
        .table_id = 10,
        .constraint_name = "users_email_key",
        .start_encoded_value = "",
        .end_encoded_value = null,
    });

    const candidate_domains = [_]CandidateDomain{
        .{ .node_id = 1, .store_id = 101, .role = "data", .failure_domain = "a" },
        .{ .node_id = 2, .store_id = 102, .role = "serving", .failure_domain = "b" },
        .{ .node_id = 3, .store_id = 103, .role = "serving", .failure_domain = "c" },
    };
    var planner = PlacementPlanner.init(std.testing.allocator);
    const intents = try planner.planAllIntentsWithCurrentAndDomains(&manager, &.{ 1, 2, 3 }, &.{}, &candidate_domains);
    defer planner.freeIntents(std.testing.allocator, intents);

    var unique_owner_intents: usize = 0;
    for (intents) |intent| {
        if (intent.record.group_id != 4001) continue;
        unique_owner_intents += 1;
        try std.testing.expect(intent.record.local_node_id == 2 or intent.record.local_node_id == 3);
        try std.testing.expect(intent.store_id == 102 or intent.store_id == 103);
    }
    try std.testing.expectEqual(@as(usize, 2), unique_owner_intents);
}

test "placement planner places secondary index rebuild ranges" {
    var manager = table_manager.TableManager.init(std.testing.allocator);
    defer manager.deinit();

    try manager.upsertTable(.{ .table_id = 16, .name = "users", .desired_replica_count = 2 });
    try manager.upsertSecondaryIndexRebuildRange(.{
        .table_id = 16,
        .index_name = "users_status_idx",
        .index_generation = 77,
        .start_row_key = "",
        .end_row_key = null,
        .group_id = 16001,
    });

    var planner = PlacementPlanner.init(std.testing.allocator);
    const intents = try planner.planAllIntents(&manager, &.{ 1, 2, 3 });
    defer planner.freeIntents(std.testing.allocator, intents);

    var rebuild_intents: usize = 0;
    for (intents) |intent| {
        if (intent.record.group_id == 16001) rebuild_intents += 1;
    }
    try std.testing.expectEqual(@as(usize, 2), rebuild_intents);
}

test "placement planner spreads multiple ranges across candidate nodes" {
    var manager = table_manager.TableManager.init(std.testing.allocator);
    defer manager.deinit();

    try manager.upsertTable(.{ .table_id = 8, .name = "docs", .desired_replica_count = 2 });
    try manager.upsertRange(.{
        .group_id = 801,
        .table_id = 8,
        .start_key = "doc:a",
        .end_key = "doc:m",
    });
    try manager.upsertRange(.{
        .group_id = 802,
        .table_id = 8,
        .start_key = "doc:m",
        .end_key = "doc:z",
    });

    var planner = PlacementPlanner.init(std.testing.allocator);
    const intents = try planner.planAllIntents(&manager, &.{ 1, 2, 3 });
    defer planner.freeIntents(std.testing.allocator, intents);

    var counts = [_]usize{ 0, 0, 0 };
    for (intents) |intent| {
        if (intent.record.local_node_id >= 1 and intent.record.local_node_id <= 3) {
            counts[intent.record.local_node_id - 1] += 1;
        }
    }
    try std.testing.expect(counts[0] > 0);
    try std.testing.expect(counts[1] > 0);
    try std.testing.expect(counts[2] > 0);
}

test "placement planner preserves valid current peers before moving replicas" {
    var manager = table_manager.TableManager.init(std.testing.allocator);
    defer manager.deinit();

    try manager.upsertTable(.{ .table_id = 9, .name = "docs", .desired_replica_count = 2 });
    try manager.upsertRange(.{
        .group_id = 901,
        .table_id = 9,
        .start_key = "doc:a",
        .end_key = "doc:z",
    });

    const current = [_]raft_reconciler.PlacementIntent{
        .{ .record = .{ .group_id = 901, .replica_id = 1, .local_node_id = 1 }, .peer_node_ids = &.{ 1, 2 } },
        .{ .record = .{ .group_id = 901, .replica_id = 2, .local_node_id = 2 }, .peer_node_ids = &.{ 1, 2 } },
    };

    var planner = PlacementPlanner.init(std.testing.allocator);
    const intents = try planner.planAllIntentsWithCurrent(&manager, &.{ 1, 2, 3 }, current[0..]);
    defer planner.freeIntents(std.testing.allocator, intents);

    try std.testing.expectEqual(@as(usize, 2), intents.len);
    try std.testing.expectEqual(@as(u64, 1), intents[0].record.local_node_id);
    try std.testing.expectEqual(@as(u64, 2), intents[1].record.local_node_id);
}

test "placement planner forced reallocation replaces at most one peer per group" {
    var manager = table_manager.TableManager.init(std.testing.allocator);
    defer manager.deinit();

    try manager.upsertTable(.{ .table_id = 91, .name = "docs", .desired_replica_count = 3 });
    try manager.upsertRange(.{
        .group_id = 9101,
        .table_id = 91,
        .start_key = "doc:a",
        .end_key = "doc:m",
    });
    try manager.upsertRange(.{
        .group_id = 9102,
        .table_id = 91,
        .start_key = "doc:m",
        .end_key = "doc:z",
    });

    const current = [_]raft_reconciler.PlacementIntent{
        .{ .record = .{ .group_id = 9101, .replica_id = 1, .local_node_id = 1 }, .peer_node_ids = &.{ 1, 2, 3 } },
        .{ .record = .{ .group_id = 9101, .replica_id = 2, .local_node_id = 2 }, .peer_node_ids = &.{ 1, 2, 3 } },
        .{ .record = .{ .group_id = 9101, .replica_id = 3, .local_node_id = 3 }, .peer_node_ids = &.{ 1, 2, 3 } },
        .{ .record = .{ .group_id = 9102, .replica_id = 1, .local_node_id = 1 }, .peer_node_ids = &.{ 1, 2, 3 } },
        .{ .record = .{ .group_id = 9102, .replica_id = 2, .local_node_id = 2 }, .peer_node_ids = &.{ 1, 2, 3 } },
        .{ .record = .{ .group_id = 9102, .replica_id = 3, .local_node_id = 3 }, .peer_node_ids = &.{ 1, 2, 3 } },
    };
    const candidate_domains = [_]CandidateDomain{
        .{ .node_id = 1, .role = "data", .failure_domain = "rack-a", .retain_current = false, .force_reallocate = true },
        .{ .node_id = 2, .role = "data", .failure_domain = "rack-b", .retain_current = false, .force_reallocate = true },
        .{ .node_id = 3, .role = "data", .failure_domain = "rack-c", .retain_current = false, .force_reallocate = true },
        .{ .node_id = 4, .role = "data", .failure_domain = "rack-d", .retain_current = false, .force_reallocate = true },
        .{ .node_id = 5, .role = "data", .failure_domain = "rack-e", .retain_current = false, .force_reallocate = true },
    };

    var planner = PlacementPlanner.init(std.testing.allocator);
    const intents = try planner.planAllIntentsWithCurrentAndDomains(
        &manager,
        &.{ 1, 2, 3, 4, 5 },
        &current,
        &candidate_domains,
    );
    defer planner.freeIntents(std.testing.allocator, intents);

    try std.testing.expectEqual(@as(usize, 6), intents.len);
    var retained: usize = 0;
    var replacements: usize = 0;
    var group_9101_replacements: usize = 0;
    var group_9102_replacements: usize = 0;
    for (intents) |intent| {
        if (intent.record.local_node_id <= 3) {
            retained += 1;
        } else {
            replacements += 1;
            if (intent.record.group_id == 9101) group_9101_replacements += 1;
            if (intent.record.group_id == 9102) group_9102_replacements += 1;
        }
    }
    try std.testing.expectEqual(@as(usize, 4), retained);
    try std.testing.expectEqual(@as(usize, 2), replacements);
    try std.testing.expect(group_9101_replacements <= 1);
    try std.testing.expect(group_9102_replacements <= 1);
}

test "placement planner forced reallocation avoids balanced no-op churn" {
    var manager = table_manager.TableManager.init(std.testing.allocator);
    defer manager.deinit();

    try manager.upsertTable(.{ .table_id = 92, .name = "docs", .desired_replica_count = 3 });
    try manager.upsertRange(.{
        .group_id = 9201,
        .table_id = 92,
        .start_key = "",
        .end_key = null,
    });

    const current = [_]raft_reconciler.PlacementIntent{
        .{ .record = .{ .group_id = 9201, .replica_id = 1, .local_node_id = 1 }, .peer_node_ids = &.{ 1, 2, 3 } },
        .{ .record = .{ .group_id = 9201, .replica_id = 2, .local_node_id = 2 }, .peer_node_ids = &.{ 1, 2, 3 } },
        .{ .record = .{ .group_id = 9201, .replica_id = 3, .local_node_id = 3 }, .peer_node_ids = &.{ 1, 2, 3 } },
    };
    const candidate_domains = [_]CandidateDomain{
        .{ .node_id = 1, .role = "data", .failure_domain = "rack-a", .retain_current = false, .force_reallocate = true },
        .{ .node_id = 2, .role = "data", .failure_domain = "rack-b", .retain_current = false, .force_reallocate = true },
        .{ .node_id = 3, .role = "data", .failure_domain = "rack-c", .retain_current = false, .force_reallocate = true },
        .{ .node_id = 4, .role = "data", .failure_domain = "rack-d", .retain_current = false, .force_reallocate = true },
        .{ .node_id = 5, .role = "data", .failure_domain = "rack-e", .retain_current = false, .force_reallocate = true },
    };

    var planner = PlacementPlanner.init(std.testing.allocator);
    const intents = try planner.planAllIntentsWithCurrentAndDomains(
        &manager,
        &.{ 1, 2, 3, 4, 5 },
        &current,
        &candidate_domains,
    );
    defer planner.freeIntents(std.testing.allocator, intents);

    try std.testing.expectEqual(@as(usize, 3), intents.len);
    for (intents) |intent| try std.testing.expect(intent.record.local_node_id <= 3);
}

test "placement planner anti-affinity rotates replica pairs across ranges" {
    var manager = table_manager.TableManager.init(std.testing.allocator);
    defer manager.deinit();

    try manager.upsertTable(.{ .table_id = 10, .name = "docs", .desired_replica_count = 2 });
    try manager.upsertRange(.{
        .group_id = 1001,
        .table_id = 10,
        .start_key = "doc:a",
        .end_key = "doc:g",
    });
    try manager.upsertRange(.{
        .group_id = 1002,
        .table_id = 10,
        .start_key = "doc:g",
        .end_key = "doc:n",
    });
    try manager.upsertRange(.{
        .group_id = 1003,
        .table_id = 10,
        .start_key = "doc:n",
        .end_key = "doc:z",
    });

    var planner = PlacementPlanner.init(std.testing.allocator);
    const intents = try planner.planAllIntents(&manager, &.{ 1, 2, 3 });
    defer planner.freeIntents(std.testing.allocator, intents);

    var pairs = std.AutoHashMapUnmanaged(u128, usize).empty;
    defer pairs.deinit(std.testing.allocator);
    for ([_]u64{ 1001, 1002, 1003 }) |group_id| {
        var peers = std.ArrayListUnmanaged(u64).empty;
        defer peers.deinit(std.testing.allocator);
        for (intents) |intent| {
            if (intent.record.group_id != group_id) continue;
            try peers.append(std.testing.allocator, intent.record.local_node_id);
        }
        try std.testing.expectEqual(@as(usize, 2), peers.items.len);
        const entry = try pairs.getOrPut(std.testing.allocator, pairKey(peers.items[0], peers.items[1]));
        if (!entry.found_existing) entry.value_ptr.* = 0;
        entry.value_ptr.* += 1;
    }
    try std.testing.expectEqual(@as(usize, 3), pairs.count());
}

test "placement planner prefers cross-domain peers for a range" {
    var manager = table_manager.TableManager.init(std.testing.allocator);
    defer manager.deinit();

    try manager.upsertTable(.{ .table_id = 11, .name = "docs", .desired_replica_count = 2 });
    try manager.upsertRange(.{
        .group_id = 1101,
        .table_id = 11,
        .start_key = "doc:a",
        .end_key = "doc:z",
    });

    const candidate_domains = [_]CandidateDomain{
        .{ .node_id = 1, .role = "data", .failure_domain = "rack-a" },
        .{ .node_id = 2, .role = "data", .failure_domain = "rack-a" },
        .{ .node_id = 3, .role = "data", .failure_domain = "rack-b" },
    };

    var planner = PlacementPlanner.init(std.testing.allocator);
    const intents = try planner.planAllIntentsWithCurrentAndDomains(&manager, &.{ 1, 2, 3 }, &.{}, &candidate_domains);
    defer planner.freeIntents(std.testing.allocator, intents);

    try std.testing.expectEqual(@as(usize, 2), intents.len);
    try std.testing.expect(intents[0].record.local_node_id == 1 or intents[0].record.local_node_id == 2 or intents[1].record.local_node_id == 1 or intents[1].record.local_node_id == 2);
    try std.testing.expect(intents[0].record.local_node_id == 3 or intents[1].record.local_node_id == 3);
}

test "placement planner filters candidates by table placement role" {
    var manager = table_manager.TableManager.init(std.testing.allocator);
    defer manager.deinit();

    try manager.upsertTable(.{ .table_id = 12, .name = "hot_docs", .placement_role = "hot", .desired_replica_count = 2 });
    try manager.upsertRange(.{
        .group_id = 1201,
        .table_id = 12,
        .start_key = "doc:a",
        .end_key = "doc:z",
    });

    const candidate_domains = [_]CandidateDomain{
        .{ .node_id = 1, .role = "hot", .failure_domain = "rack-a" },
        .{ .node_id = 2, .role = "cold", .failure_domain = "rack-b" },
        .{ .node_id = 3, .role = "hot", .failure_domain = "rack-c" },
    };

    var planner = PlacementPlanner.init(std.testing.allocator);
    const intents = try planner.planAllIntentsWithCurrentAndDomains(&manager, &.{ 1, 2, 3 }, &.{}, &candidate_domains);
    defer planner.freeIntents(std.testing.allocator, intents);

    try std.testing.expectEqual(@as(usize, 2), intents.len);
    for (intents) |intent| try std.testing.expect(intent.record.local_node_id != 2);
}

test "placement planner supports explicit serving bulk archive classes" {
    var manager = table_manager.TableManager.init(std.testing.allocator);
    defer manager.deinit();

    try manager.upsertTable(.{ .table_id = 13, .name = "serving_docs", .placement_role = "serving", .desired_replica_count = 2 });
    try manager.upsertRange(.{
        .group_id = 1301,
        .table_id = 13,
        .start_key = "doc:a",
        .end_key = "doc:z",
    });

    const candidate_domains = [_]CandidateDomain{
        .{ .node_id = 1, .role = "serving", .failure_domain = "rack-a" },
        .{ .node_id = 2, .role = "bulk", .failure_domain = "rack-b" },
        .{ .node_id = 3, .role = "serving", .failure_domain = "rack-c" },
        .{ .node_id = 4, .role = "archive", .failure_domain = "rack-d" },
    };

    var planner = PlacementPlanner.init(std.testing.allocator);
    const intents = try planner.planAllIntentsWithCurrentAndDomains(&manager, &.{ 1, 2, 3, 4 }, &.{}, &candidate_domains);
    defer planner.freeIntents(std.testing.allocator, intents);

    try std.testing.expectEqual(@as(usize, 2), intents.len);
    for (intents) |intent| {
        try std.testing.expect(intent.record.local_node_id == 1 or intent.record.local_node_id == 3);
    }
}

test "placement planner rebalances away from overloaded current peers" {
    var manager = table_manager.TableManager.init(std.testing.allocator);
    defer manager.deinit();

    try manager.upsertTable(.{ .table_id = 14, .name = "docs", .desired_replica_count = 2 });
    try manager.upsertRange(.{
        .group_id = 1401,
        .table_id = 14,
        .start_key = "doc:a",
        .end_key = "doc:z",
    });

    const current = [_]raft_reconciler.PlacementIntent{
        .{ .record = .{ .group_id = 1401, .replica_id = 1, .local_node_id = 1, .bootstrap_mode = .persisted }, .peer_node_ids = &.{ 1, 2 } },
        .{ .record = .{ .group_id = 1401, .replica_id = 2, .local_node_id = 2, .bootstrap_mode = .persisted }, .peer_node_ids = &.{ 1, 2 } },
    };
    const candidate_domains = [_]CandidateDomain{
        .{ .node_id = 1, .role = "data", .failure_domain = "rack-a", .priority = 2, .status_tag = .overloaded, .available_bytes = 950, .lease_pressure = 95, .read_load = 180, .write_load = 120, .retain_current = false },
        .{ .node_id = 2, .role = "data", .failure_domain = "rack-b", .priority = 0, .available_bytes = 850, .lease_pressure = 10, .read_load = 15, .write_load = 10 },
        .{ .node_id = 3, .role = "data", .failure_domain = "rack-c", .priority = 0, .available_bytes = 800, .lease_pressure = 12, .read_load = 18, .write_load = 10 },
    };

    var planner = PlacementPlanner.init(std.testing.allocator);
    const intents = try planner.planAllIntentsWithCurrentAndDomains(&manager, &.{ 1, 2, 3 }, &current, &candidate_domains);
    defer planner.freeIntents(std.testing.allocator, intents);

    try std.testing.expectEqual(@as(usize, 2), intents.len);
    var has_one = false;
    var has_two = false;
    var has_three = false;
    for (intents) |intent| {
        if (intent.record.local_node_id == 1) has_one = true;
        if (intent.record.local_node_id == 2) has_two = true;
        if (intent.record.local_node_id == 3) has_three = true;
    }
    try std.testing.expect(!has_one);
    try std.testing.expect(has_two);
    try std.testing.expect(has_three);
}

test "placement planner excludes draining stores from replacement selection" {
    var manager = table_manager.TableManager.init(std.testing.allocator);
    defer manager.deinit();

    try manager.upsertTable(.{ .table_id = 141, .name = "docs", .desired_replica_count = 1 });
    try manager.upsertRange(.{
        .group_id = 14101,
        .table_id = 141,
        .start_key = "doc:a",
        .end_key = "doc:z",
    });

    const current = [_]raft_reconciler.PlacementIntent{
        .{ .record = .{ .group_id = 14101, .replica_id = 1, .local_node_id = 1, .bootstrap_mode = .persisted }, .store_id = 1, .peer_node_ids = &.{1}, .serving_state = .serving },
    };
    const candidate_domains = [_]CandidateDomain{
        .{ .node_id = 1, .store_id = 1, .role = "data", .failure_domain = "rack-a", .priority = 255, .status_tag = .excluded, .retain_current = false },
        .{ .node_id = 2, .store_id = 2, .role = "data", .failure_domain = "rack-b" },
        .{ .node_id = 3, .store_id = 3, .role = "data", .failure_domain = "rack-c" },
    };

    var planner = PlacementPlanner.init(std.testing.allocator);
    const intents = try planner.planAllIntentsWithCurrentAndDomains(&manager, &.{ 1, 2, 3 }, &current, &candidate_domains);
    defer planner.freeIntents(std.testing.allocator, intents);

    try std.testing.expectEqual(@as(usize, 1), intents.len);
    try std.testing.expect(intents[0].record.local_node_id == 2 or intents[0].record.local_node_id == 3);
    try std.testing.expectEqual(@as(u64, 1), intents[0].relocation_source_node_id);
    try std.testing.expectEqual(@as(u64, 1), intents[0].relocation_source_store_id);
    try std.testing.expectEqual(raft_reconciler.PlacementServingState.bootstrapping, intents[0].serving_state);
}

test "placement planner tags replacement with the dropped current peer as source" {
    var manager = table_manager.TableManager.init(std.testing.allocator);
    defer manager.deinit();

    try manager.upsertTable(.{ .table_id = 15, .name = "docs", .desired_replica_count = 3 });
    try manager.upsertRange(.{
        .group_id = 1501,
        .table_id = 15,
        .start_key = "doc:a",
        .end_key = "doc:z",
    });

    const current = [_]raft_reconciler.PlacementIntent{
        .{ .record = .{ .group_id = 1501, .replica_id = 1, .local_node_id = 105, .bootstrap_mode = .persisted }, .store_id = 105, .peer_node_ids = &.{ 105, 101, 102 }, .serving_state = .serving },
        .{ .record = .{ .group_id = 1501, .replica_id = 2, .local_node_id = 101, .bootstrap_mode = .persisted }, .store_id = 101, .peer_node_ids = &.{ 105, 101, 102 }, .serving_state = .serving },
        .{ .record = .{ .group_id = 1501, .replica_id = 3, .local_node_id = 102, .bootstrap_mode = .persisted }, .store_id = 102, .peer_node_ids = &.{ 105, 101, 102 }, .serving_state = .serving },
    };
    const candidate_domains = [_]CandidateDomain{
        .{ .node_id = 101, .store_id = 101, .role = "data", .failure_domain = "rack-a", .status_tag = .excluded, .retain_current = false },
        .{ .node_id = 102, .store_id = 102, .role = "data", .failure_domain = "rack-b" },
        .{ .node_id = 103, .store_id = 103, .role = "data", .failure_domain = "rack-c" },
        .{ .node_id = 104, .store_id = 104, .role = "data", .failure_domain = "rack-d" },
        // A load rebalance request must not move this healthy survivor in the
        // same plan that replaces the excluded member.
        .{ .node_id = 105, .store_id = 105, .role = "data", .failure_domain = "rack-e", .retain_current = false },
    };

    var planner = PlacementPlanner.init(std.testing.allocator);
    const intents = try planner.planAllIntentsWithCurrentAndDomains(&manager, &.{ 101, 102, 103, 104, 105 }, &current, &candidate_domains);
    defer planner.freeIntents(std.testing.allocator, intents);

    var replacement: ?raft_reconciler.PlacementIntent = null;
    for (intents) |intent| {
        if (intent.record.local_node_id == 101) return error.DroppedPeerRetained;
        if (findCurrentIntent(&current, 1501, intent.record.local_node_id) == null) replacement = intent;
    }
    const target = replacement orelse return error.MissingReplacement;
    try std.testing.expectEqual(@as(u64, 2), target.record.replica_id);
    try std.testing.expectEqual(@as(u64, 101), target.relocation_source_node_id);
    try std.testing.expectEqual(@as(u64, 101), target.relocation_source_store_id);
    try std.testing.expectEqual(raft_reconciler.PlacementServingState.bootstrapping, target.serving_state);
    try std.testing.expectEqual(@as(u64, 1), findCurrentIntent(intents, 1501, 105).?.record.replica_id);
    try std.testing.expectEqual(@as(u64, 3), findCurrentIntent(intents, 1501, 102).?.record.replica_id);

    const changed_candidates = [_]CandidateDomain{
        .{ .node_id = 105, .store_id = 105, .role = "data", .failure_domain = "rack-e", .retain_current = false, .read_load = 999 },
        .{ .node_id = 104, .store_id = 104, .role = "data", .failure_domain = "rack-d", .available_bytes = 1 },
        .{ .node_id = 103, .store_id = 103, .role = "data", .failure_domain = "rack-c", .write_load = 999 },
        .{ .node_id = 102, .store_id = 102, .role = "data", .failure_domain = "rack-b", .available_bytes = std.math.maxInt(u64) },
        .{ .node_id = 101, .store_id = 101, .role = "data", .failure_domain = "rack-a", .status_tag = .excluded, .retain_current = false },
    };
    const retried = try planner.planAllIntentsWithCurrentAndDomains(
        &manager,
        &.{ 105, 104, 103, 102, 101 },
        &current,
        &changed_candidates,
    );
    defer planner.freeIntents(std.testing.allocator, retried);
    var retried_target: ?raft_reconciler.PlacementIntent = null;
    for (retried) |intent| {
        if (findCurrentIntent(&current, 1501, intent.record.local_node_id) == null) retried_target = intent;
    }
    try std.testing.expectEqual(target.record.local_node_id, retried_target.?.record.local_node_id);
}

test "placement planner preserves protected unconverged members during forced rebalance" {
    var manager = table_manager.TableManager.init(std.testing.allocator);
    defer manager.deinit();
    try manager.upsertTable(.{ .table_id = 151, .name = "docs", .desired_replica_count = 3 });
    try manager.upsertRange(.{
        .group_id = 15101,
        .table_id = 151,
        .start_key = "doc:a",
        .end_key = "doc:z",
    });

    const current = [_]raft_reconciler.PlacementIntent{
        .{ .record = .{ .group_id = 15101, .replica_id = 1, .local_node_id = 101 }, .peer_node_ids = &.{ 101, 102, 105 } },
        .{ .record = .{ .group_id = 15101, .replica_id = 2, .local_node_id = 102 }, .peer_node_ids = &.{ 101, 102, 105 } },
        .{ .record = .{ .group_id = 15101, .replica_id = 3, .local_node_id = 105 }, .peer_node_ids = &.{ 101, 102, 105 } },
    };
    const candidates = [_]CandidateDomain{
        .{ .node_id = 101, .role = "data", .failure_domain = "rack-a", .retain_current = false, .force_reallocate = true },
        .{ .node_id = 102, .role = "data", .failure_domain = "rack-b", .retain_current = false, .force_reallocate = true },
        .{ .node_id = 103, .role = "data", .failure_domain = "rack-c", .retain_current = false, .force_reallocate = true },
        .{ .node_id = 104, .role = "data", .failure_domain = "rack-d", .retain_current = false, .force_reallocate = true },
        .{ .node_id = 105, .role = "data", .failure_domain = "rack-e", .retain_current = false, .force_reallocate = true },
    };

    var planner = PlacementPlanner.init(std.testing.allocator);
    const intents = try planner.planAllIntentsWithConstraints(
        &manager,
        &.{ 101, 102, 103, 104, 105 },
        &current,
        &candidates,
        &.{},
        &.{15101},
    );
    defer planner.freeIntents(std.testing.allocator, intents);
    try std.testing.expectEqual(@as(usize, 3), intents.len);
    try std.testing.expect(findCurrentIntent(intents, 15101, 101) != null);
    try std.testing.expect(findCurrentIntent(intents, 15101, 102) != null);
    try std.testing.expect(findCurrentIntent(intents, 15101, 105) != null);
}

test "placement planner keeps the most advanced duplicate replacement member" {
    var manager = table_manager.TableManager.init(std.testing.allocator);
    defer manager.deinit();
    try manager.upsertTable(.{ .table_id = 152, .name = "docs", .desired_replica_count = 3 });
    try manager.upsertRange(.{
        .group_id = 15201,
        .table_id = 152,
        .start_key = "doc:a",
        .end_key = "doc:z",
    });

    // A prior reconciliation may leave more than one replacement carrying the
    // retired replica id. Protected planning must latch the replacement that
    // made the most progress instead of switching targets by node id.
    const current = [_]raft_reconciler.PlacementIntent{
        .{ .record = .{ .group_id = 15201, .replica_id = 1, .local_node_id = 101 }, .peer_node_ids = &.{ 101, 102, 103, 104, 105 }, .serving_state = .serving },
        .{ .record = .{ .group_id = 15201, .replica_id = 2, .local_node_id = 102 }, .peer_node_ids = &.{ 101, 102, 103, 104, 105 }, .serving_state = .serving },
        .{ .record = .{ .group_id = 15201, .replica_id = 3, .local_node_id = 103 }, .peer_node_ids = &.{ 101, 102, 103, 104, 105 }, .serving_state = .draining, .relocation_generation = 10 },
        .{ .record = .{ .group_id = 15201, .replica_id = 3, .local_node_id = 104 }, .peer_node_ids = &.{ 101, 102, 103, 104, 105 }, .serving_state = .serving, .relocation_generation = 11 },
        .{ .record = .{ .group_id = 15201, .replica_id = 3, .local_node_id = 105 }, .peer_node_ids = &.{ 101, 102, 103, 104, 105 }, .serving_state = .draining, .relocation_generation = 1 },
    };
    const candidates = [_]CandidateDomain{
        .{ .node_id = 101, .role = "data", .failure_domain = "", .retain_current = false },
        .{ .node_id = 102, .role = "data", .failure_domain = "", .retain_current = false },
        .{ .node_id = 103, .role = "data", .failure_domain = "", .retain_current = false },
        .{ .node_id = 104, .role = "data", .failure_domain = "", .retain_current = false },
        .{ .node_id = 105, .role = "data", .failure_domain = "", .status_tag = .excluded, .retain_current = false },
    };

    var planner = PlacementPlanner.init(std.testing.allocator);
    const intents = try planner.planAllIntentsWithConstraints(
        &manager,
        &.{ 101, 102, 103, 104, 105 },
        &current,
        &candidates,
        &.{},
        &.{15201},
    );
    defer planner.freeIntents(std.testing.allocator, intents);

    try std.testing.expectEqual(@as(usize, 3), intents.len);
    try std.testing.expect(findCurrentIntent(intents, 15201, 104) != null);
    try std.testing.expect(findCurrentIntent(intents, 15201, 103) == null);
    try std.testing.expect(findCurrentIntent(intents, 15201, 105) == null);
}

test "placement planner repairs quota-full duplicate replica ids" {
    var manager = table_manager.TableManager.init(std.testing.allocator);
    defer manager.deinit();
    try manager.upsertTable(.{ .table_id = 153, .name = "docs", .desired_replica_count = 3 });
    try manager.upsertRange(.{
        .group_id = 15301,
        .table_id = 153,
        .start_key = "doc:a",
        .end_key = "doc:z",
    });

    const current = [_]raft_reconciler.PlacementIntent{
        .{ .record = .{ .group_id = 15301, .replica_id = 1, .local_node_id = 101 }, .peer_node_ids = &.{ 101, 102, 103 }, .serving_state = .serving },
        .{ .record = .{ .group_id = 15301, .replica_id = 2, .local_node_id = 102 }, .peer_node_ids = &.{ 101, 102, 103 }, .serving_state = .planned, .relocation_generation = 10 },
        .{ .record = .{ .group_id = 15301, .replica_id = 2, .local_node_id = 103 }, .peer_node_ids = &.{ 101, 102, 103 }, .serving_state = .serving, .relocation_generation = 11 },
    };
    const candidates = [_]CandidateDomain{
        .{ .node_id = 101, .role = "data", .failure_domain = "" },
        .{ .node_id = 102, .role = "data", .failure_domain = "" },
        .{ .node_id = 103, .role = "data", .failure_domain = "" },
        .{ .node_id = 104, .role = "data", .failure_domain = "" },
    };

    var planner = PlacementPlanner.init(std.testing.allocator);
    const intents = try planner.planAllIntentsWithConstraints(
        &manager,
        &.{ 101, 102, 103, 104 },
        &current,
        &candidates,
        &.{},
        &.{15301},
    );
    defer planner.freeIntents(std.testing.allocator, intents);

    try std.testing.expectEqual(@as(usize, 3), intents.len);
    try std.testing.expect(findCurrentIntent(intents, 15301, 101) != null);
    try std.testing.expect(findCurrentIntent(intents, 15301, 103) != null);
    try std.testing.expect(findCurrentIntent(intents, 15301, 102) == null);
    var replica_ids: [3]u64 = undefined;
    for (intents, 0..) |intent, i| replica_ids[i] = intent.record.replica_id;
    std.mem.sort(u64, &replica_ids, {}, std.sort.asc(u64));
    try std.testing.expectEqualSlices(u64, &.{ 1, 2, 3 }, &replica_ids);
}

test "placement planner repairs missing replicas with a stable target" {
    var manager = table_manager.TableManager.init(std.testing.allocator);
    defer manager.deinit();
    try manager.upsertTable(.{ .table_id = 154, .name = "docs", .desired_replica_count = 3 });
    try manager.upsertRange(.{
        .group_id = 15401,
        .table_id = 154,
        .start_key = "doc:a",
        .end_key = "doc:z",
    });

    const current = [_]raft_reconciler.PlacementIntent{
        .{ .record = .{ .group_id = 15401, .replica_id = 1, .local_node_id = 101 }, .peer_node_ids = &.{ 101, 102 }, .serving_state = .serving },
        .{ .record = .{ .group_id = 15401, .replica_id = 2, .local_node_id = 102 }, .peer_node_ids = &.{ 101, 102 }, .serving_state = .serving },
    };
    const candidates = [_]CandidateDomain{
        .{ .node_id = 101, .role = "data", .failure_domain = "rack-a" },
        .{ .node_id = 102, .role = "data", .failure_domain = "rack-b" },
        .{ .node_id = 103, .role = "data", .failure_domain = "rack-c", .available_bytes = std.math.maxInt(u64) },
        .{ .node_id = 104, .role = "data", .failure_domain = "rack-d", .read_load = 500 },
        .{ .node_id = 105, .role = "data", .failure_domain = "rack-e", .write_load = 500 },
    };

    var planner = PlacementPlanner.init(std.testing.allocator);
    const first = try planner.planAllIntentsWithCurrentAndDomains(
        &manager,
        &.{ 101, 102, 103, 104, 105 },
        &current,
        &candidates,
    );
    defer planner.freeIntents(std.testing.allocator, first);

    const changed_candidates = [_]CandidateDomain{
        .{ .node_id = 105, .role = "data", .failure_domain = "rack-e", .available_bytes = std.math.maxInt(u64) },
        .{ .node_id = 104, .role = "data", .failure_domain = "rack-d", .write_load = 700 },
        .{ .node_id = 103, .role = "data", .failure_domain = "rack-c", .read_load = 700 },
        .{ .node_id = 102, .role = "data", .failure_domain = "rack-b" },
        .{ .node_id = 101, .role = "data", .failure_domain = "rack-a" },
    };
    const retried = try planner.planAllIntentsWithCurrentAndDomains(
        &manager,
        &.{ 105, 104, 103, 102, 101 },
        &current,
        &changed_candidates,
    );
    defer planner.freeIntents(std.testing.allocator, retried);

    var first_target: ?raft_reconciler.PlacementIntent = null;
    var retried_target: ?raft_reconciler.PlacementIntent = null;
    for (first) |intent| {
        if (findCurrentIntent(&current, 15401, intent.record.local_node_id) == null) first_target = intent;
    }
    for (retried) |intent| {
        if (findCurrentIntent(&current, 15401, intent.record.local_node_id) == null) retried_target = intent;
    }
    try std.testing.expectEqual(@as(usize, 3), first.len);
    try std.testing.expectEqual(first_target.?.record.local_node_id, retried_target.?.record.local_node_id);
    try std.testing.expectEqual(@as(u64, 3), first_target.?.record.replica_id);
    try std.testing.expectEqual(raft_reconciler.PlacementServingState.bootstrapping, first_target.?.serving_state);
}

test "membership repair detects under and over replication" {
    const current = [_]raft_reconciler.PlacementIntent{
        .{ .record = .{ .group_id = 15501, .replica_id = 1, .local_node_id = 101 } },
        .{ .record = .{ .group_id = 15501, .replica_id = 2, .local_node_id = 102 } },
    };
    const candidates = [_]CandidateDomain{
        .{ .node_id = 101, .role = "data", .failure_domain = "zone-a" },
        .{ .node_id = 102, .role = "data", .failure_domain = "zone-b" },
        .{ .node_id = 103, .role = "data", .failure_domain = "zone-c" },
    };

    try std.testing.expect(!groupNeedsMembershipRepair(&current, 15501, 2, &.{ 101, 102, 103 }, &candidates, "data"));
    try std.testing.expect(groupNeedsMembershipRepair(&current, 15501, 3, &.{ 101, 102, 103 }, &candidates, "data"));
    try std.testing.expect(groupNeedsMembershipRepair(&current, 15501, 1, &.{ 101, 102, 103 }, &candidates, "data"));
}
