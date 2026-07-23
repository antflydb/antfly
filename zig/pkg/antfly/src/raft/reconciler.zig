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
const raft_engine = @import("raft_engine");
const catalog = @import("catalog.zig");
const host_mod = @import("host.zig");
const peer_resolver = @import("peer_resolver.zig");

pub const PlacementIntent = struct {
    record: catalog.ReplicaRecord,
    store_id: u64 = 0,
    /// Desired voting members. Relocation learners are represented
    /// separately so they can receive state without being able to campaign.
    peer_node_ids: []const u64 = &.{},
    learner_node_ids: []const u64 = &.{},
    serving_state: PlacementServingState = .serving,
    relocation_generation: u64 = 0,
    relocation_source_node_id: u64 = 0,
    relocation_source_store_id: u64 = 0,
    relocation_doc_count_watermark: u64 = 0,
    relocation_disk_bytes_watermark: u64 = 0,
    relocation_target_sequence: u64 = 0,
    relocation_applied_sequence: u64 = 0,
};

pub const PlacementServingState = enum(u8) {
    planned,
    bootstrapping,
    replaying,
    cutover_ready,
    serving,
    draining,
    /// The replica remains hosted so it can observe and report the committed
    /// final configuration, but it is excluded from membership and routing.
    retiring,
};

pub fn placementMayServeClientReads(intent: PlacementIntent) bool {
    return switch (intent.serving_state) {
        .serving, .draining => true,
        .planned, .bootstrapping, .replaying, .cutover_ready, .retiring => false,
    };
}

/// Whether an existing placement may remain leader while a membership
/// transition converges. A cutover-ready target is already a voting member, a
/// draining source must finish expansion, and a retiring source owns orderly
/// transfer until Raft elects or confirms its successor.
pub fn placementMayLeadMembershipTransition(intent: PlacementIntent) bool {
    return switch (intent.serving_state) {
        .cutover_ready, .serving, .draining, .retiring => true,
        .planned, .bootstrapping, .replaying => false,
    };
}

pub fn placementReadableWithPeers(intents: []const PlacementIntent, intent: PlacementIntent) bool {
    switch (intent.serving_state) {
        .serving => return true,
        .draining => {
            for (intents) |peer| {
                if (peer.record.group_id == intent.record.group_id and
                    peer.record.local_node_id != intent.record.local_node_id and
                    peer.serving_state == .serving)
                {
                    return false;
                }
            }
            return true;
        },
        .planned, .bootstrapping, .replaying, .cutover_ready, .retiring => return false,
    }
}

pub const PlacementProvider = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        list_local_intents: *const fn (ptr: *anyopaque, alloc: std.mem.Allocator, local_node_id: u64) anyerror![]PlacementIntent,
    };

    pub fn listLocalIntents(self: PlacementProvider, alloc: std.mem.Allocator, local_node_id: u64) ![]PlacementIntent {
        return try self.vtable.list_local_intents(self.ptr, alloc, local_node_id);
    }
};

pub const MemoryPlacementProvider = struct {
    alloc: std.mem.Allocator,
    intents: std.ArrayListUnmanaged(PlacementIntent) = .empty,

    pub fn init(alloc: std.mem.Allocator) MemoryPlacementProvider {
        return .{ .alloc = alloc };
    }

    pub fn deinit(self: *MemoryPlacementProvider) void {
        for (self.intents.items) |intent| freeIntent(self.alloc, intent);
        self.intents.deinit(self.alloc);
        self.* = undefined;
    }

    pub fn provider(self: *MemoryPlacementProvider) PlacementProvider {
        return .{
            .ptr = self,
            .vtable = &.{
                .list_local_intents = listLocalIntents,
            },
        };
    }

    pub fn replaceAll(self: *MemoryPlacementProvider, intents: []const PlacementIntent) !void {
        var next = std.ArrayListUnmanaged(PlacementIntent).empty;
        errdefer {
            for (next.items) |intent| freeIntent(self.alloc, intent);
            next.deinit(self.alloc);
        }
        for (intents) |intent| try next.append(self.alloc, try cloneIntent(self.alloc, intent));

        for (self.intents.items) |intent| freeIntent(self.alloc, intent);
        self.intents.deinit(self.alloc);
        self.intents = next;
    }

    fn listLocalIntents(ptr: *anyopaque, alloc: std.mem.Allocator, local_node_id: u64) ![]PlacementIntent {
        const self: *MemoryPlacementProvider = @ptrCast(@alignCast(ptr));
        var out = std.ArrayListUnmanaged(PlacementIntent).empty;
        errdefer {
            for (out.items) |intent| freeIntent(alloc, intent);
            out.deinit(alloc);
        }
        for (self.intents.items) |intent| {
            if (intent.record.local_node_id != local_node_id) continue;
            try out.append(alloc, try cloneIntent(alloc, intent));
        }
        return try out.toOwnedSlice(alloc);
    }
};

pub const MetadataPlacementUpdate = union(enum) {
    upsert_intent: PlacementIntent,
    remove_group: u64,
};

pub const MetadataPlacementState = struct {
    alloc: std.mem.Allocator,
    intents: std.AutoHashMapUnmanaged(u64, PlacementIntent) = .empty,

    pub fn init(alloc: std.mem.Allocator) MetadataPlacementState {
        return .{ .alloc = alloc };
    }

    pub fn deinit(self: *MetadataPlacementState) void {
        var it = self.intents.valueIterator();
        while (it.next()) |intent| freeIntent(self.alloc, intent.*);
        self.intents.deinit(self.alloc);
        self.* = undefined;
    }

    pub fn provider(self: *MetadataPlacementState) PlacementProvider {
        return .{
            .ptr = self,
            .vtable = &.{
                .list_local_intents = listLocalIntents,
            },
        };
    }

    pub fn apply(self: *MetadataPlacementState, update: MetadataPlacementUpdate) !void {
        switch (update) {
            .upsert_intent => |intent| try self.upsertIntent(intent),
            .remove_group => |group_id| _ = try self.removeGroup(group_id),
        }
    }

    pub fn upsertIntent(self: *MetadataPlacementState, intent: PlacementIntent) !void {
        if (self.intents.getPtr(intent.record.group_id)) |existing| {
            freeIntent(self.alloc, existing.*);
            existing.* = try cloneIntent(self.alloc, intent);
            return;
        }
        try self.intents.put(self.alloc, intent.record.group_id, try cloneIntent(self.alloc, intent));
    }

    pub fn replaceAll(self: *MetadataPlacementState, intents: []const PlacementIntent) !void {
        var next = std.AutoHashMapUnmanaged(u64, PlacementIntent).empty;
        errdefer {
            var it = next.valueIterator();
            while (it.next()) |intent| freeIntent(self.alloc, intent.*);
            next.deinit(self.alloc);
        }

        for (intents) |intent| {
            try next.put(self.alloc, intent.record.group_id, try cloneIntent(self.alloc, intent));
        }

        var existing_it = self.intents.valueIterator();
        while (existing_it.next()) |intent| freeIntent(self.alloc, intent.*);
        self.intents.deinit(self.alloc);
        self.intents = next;
    }

    pub fn removeGroup(self: *MetadataPlacementState, group_id: u64) !bool {
        const removed = self.intents.fetchRemove(group_id);
        if (removed) |entry| {
            freeIntent(self.alloc, entry.value);
            return true;
        }
        return false;
    }

    fn listLocalIntents(ptr: *anyopaque, alloc: std.mem.Allocator, local_node_id: u64) ![]PlacementIntent {
        const self: *MetadataPlacementState = @ptrCast(@alignCast(ptr));
        var out = std.ArrayListUnmanaged(PlacementIntent).empty;
        errdefer {
            for (out.items) |intent| freeIntent(alloc, intent);
            out.deinit(alloc);
        }

        var it = self.intents.valueIterator();
        while (it.next()) |intent| {
            if (intent.record.local_node_id != local_node_id) continue;
            try out.append(alloc, try cloneIntent(alloc, intent.*));
        }
        return try out.toOwnedSlice(alloc);
    }
};

pub const ReconcileResult = struct {
    ensured: usize = 0,
    removed: usize = 0,
    refreshed_peers: usize = 0,
    membership_proposals: usize = 0,
};

const PreparedEnsure = struct {
    intent_index: usize,
    intent_hash: u64,
    prepare_bootstrap: bool,
    replica: ?host_mod.PreparedReplica = null,
};

/// An immutable desired-state snapshot split into a blocking durability phase
/// and a short live-runtime publication phase. The caller serializes plans and
/// executes begin/commit while holding the Raft owner's lock; prepareDurable
/// deliberately runs without that lock so consensus progress cannot be
/// blocked by restore I/O, descriptor construction, or catalog fsync.
pub const PreparedReconcile = struct {
    owner: *Reconciler,
    intents: []PlacementIntent,
    ensures: []PreparedEnsure,
    removals: []u64,
    failed_ensure_index: ?usize = null,
    durability_complete: bool = false,
    committed: bool = false,

    pub fn deinit(self: *PreparedReconcile) void {
        for (self.ensures) |*entry| {
            if (entry.replica) |*replica| replica.deinit(self.owner.alloc);
        }
        self.owner.alloc.free(self.ensures);
        self.owner.alloc.free(self.removals);
        freeIntentSlice(self.owner.alloc, self.intents);
        self.* = undefined;
    }

    pub fn beginPreparation(self: *PreparedReconcile) void {
        for (self.ensures) |entry| {
            if (!entry.prepare_bootstrap) continue;
            self.owner.host.noteReplicaBootstrapPreparing(self.intents[entry.intent_index].record);
        }
    }

    pub fn prepareDurable(self: *PreparedReconcile) !void {
        if (self.durability_complete or self.committed) return error.InvalidReconcilePhase;
        for (self.ensures, 0..) |*entry, index| {
            const record = self.intents[entry.intent_index].record;
            entry.replica = self.owner.host.prepareReplica(record, entry.prepare_bootstrap) catch |err| {
                self.failed_ensure_index = index;
                return err;
            };
        }
        for (self.removals) |group_id| try self.owner.host.prepareReplicaRemoval(group_id);
        self.durability_complete = true;
    }

    pub fn notePreparationFailure(self: *PreparedReconcile, err: anyerror) void {
        const index = self.failed_ensure_index orelse return;
        const entry = self.ensures[index];
        if (!entry.prepare_bootstrap) return;
        self.owner.host.noteReplicaBootstrapPreparationFailure(
            self.intents[entry.intent_index].record,
            err,
        );
    }

    pub fn commit(self: *PreparedReconcile) !ReconcileResult {
        if (!self.durability_complete or self.committed) return error.InvalidReconcilePhase;

        var result: ReconcileResult = .{};
        for (self.ensures) |*entry| {
            const intent = self.intents[entry.intent_index];
            const prepared = if (entry.replica) |*replica| replica else return error.InvalidReconcilePhase;
            _ = try self.owner.host.installPreparedReplica(intent.record, prepared);
            result.ensured += 1;
            result.refreshed_peers += try self.owner.refreshPeerEndpoints(intent);
            try self.owner.last_intent_hashes.put(
                self.owner.alloc,
                intent.record.group_id,
                entry.intent_hash,
            );
        }
        for (self.intents) |intent| {
            if (try self.owner.reconcileRaftMembership(intent)) result.membership_proposals += 1;
        }
        for (self.removals) |group_id| {
            try self.owner.host.removePreparedReplica(group_id);
            _ = self.owner.last_intent_hashes.remove(group_id);
            result.removed += 1;
        }
        self.owner.host.metrics.reconcile_rounds += 1;
        self.committed = true;
        return result;
    }
};

pub const Reconciler = struct {
    alloc: std.mem.Allocator,
    host: *host_mod.Host,
    provider: PlacementProvider,
    last_intent_hashes: std.AutoHashMapUnmanaged(u64, u64) = .empty,

    pub fn deinit(self: *Reconciler) void {
        self.last_intent_hashes.deinit(self.alloc);
        self.last_intent_hashes = .empty;
    }

    pub fn prepare(self: *Reconciler) !PreparedReconcile {
        const intents = try self.provider.listLocalIntents(self.alloc, self.host.cfg.local_node_id);
        errdefer freeIntentSlice(self.alloc, intents);
        const existing = try self.host.listGroupIds(self.alloc);
        defer self.alloc.free(existing);

        var desired_group_ids = std.AutoHashMapUnmanaged(u64, void).empty;
        defer desired_group_ids.deinit(self.alloc);
        var ensures = std.ArrayListUnmanaged(PreparedEnsure).empty;
        errdefer ensures.deinit(self.alloc);
        var removals = std.ArrayListUnmanaged(u64).empty;
        errdefer removals.deinit(self.alloc);

        for (intents, 0..) |intent, intent_index| {
            try desired_group_ids.put(self.alloc, intent.record.group_id, {});

            const intent_hash = hashIntent(intent);
            const hosted_status = self.host.status(intent.record.group_id);
            const stored_hash = self.last_intent_hashes.get(intent.record.group_id);
            const should_apply =
                hosted_status != .active or
                stored_hash == null or
                stored_hash.? != intent_hash;

            if (should_apply) {
                try ensures.append(self.alloc, .{
                    .intent_index = intent_index,
                    .intent_hash = intent_hash,
                    .prepare_bootstrap = !self.host.hasReplica(intent.record.group_id) and
                        intent.record.backup_restore_bootstrap != null,
                });
            }
        }
        for (existing) |group_id| {
            if (desired_group_ids.contains(group_id)) continue;
            try removals.append(self.alloc, group_id);
        }
        const owned_ensures = try ensures.toOwnedSlice(self.alloc);
        errdefer self.alloc.free(owned_ensures);
        const owned_removals = try removals.toOwnedSlice(self.alloc);
        return .{
            .owner = self,
            .intents = intents,
            .ensures = owned_ensures,
            .removals = owned_removals,
        };
    }

    pub fn reconcileOnce(self: *Reconciler) !ReconcileResult {
        var prepared = try self.prepare();
        defer prepared.deinit();
        prepared.beginPreparation();
        prepared.prepareDurable() catch |err| {
            prepared.notePreparationFailure(err);
            return err;
        };
        return try prepared.commit();
    }

    fn refreshPeerEndpoints(self: *Reconciler, intent: PlacementIntent) !usize {
        var refreshed: usize = 0;
        for (intent.peer_node_ids) |node_id| {
            if (node_id == self.host.cfg.local_node_id) continue;
            refreshed += self.host.refreshPeerEndpoints(intent.record.group_id, node_id) catch |err| switch (err) {
                error.UnknownPeer => 0,
                else => return err,
            };
        }
        for (intent.learner_node_ids) |node_id| {
            if (node_id == self.host.cfg.local_node_id or containsNodeId(intent.peer_node_ids, node_id)) continue;
            refreshed += self.host.refreshPeerEndpoints(intent.record.group_id, node_id) catch |err| switch (err) {
                error.UnknownPeer => 0,
                else => return err,
            };
        }
        return refreshed;
    }

    fn reconcileRaftMembership(self: *Reconciler, intent: PlacementIntent) !bool {
        const status = self.host.raftStatus(intent.record.group_id) orelse return false;
        if (status.soft.role != .leader or status.soft.leader_id != status.id) return false;
        if (!localNodeCanProposeMembership(status)) return false;

        // A new change cannot be proposed while joint consensus is active. Leave
        // the committed joint configuration first; the next reconcile round will
        // calculate any remaining delta from the resulting stable voter set.
        if (status.conf_state.voters_outgoing.len > 0) {
            self.host.proposeConfChangeV2(intent.record.group_id, .{}) catch |err| return switch (err) {
                error.PendingConfChange,
                error.NotInJointState,
                error.NotLeader,
                error.ProposalDropped,
                error.LeaderTransferInProgress,
                => false,
                else => err,
            };
            return true;
        }

        if (retirementLeaderTransferTarget(status, intent)) |transferee| {
            try self.host.transferLeader(intent.record.group_id, transferee);
            return true;
        }

        const changes = try allocMembershipChangesWithLocalPolicy(
            self.alloc,
            status.conf_state.voters,
            status.conf_state.learners,
            intent.record.local_node_id,
            intent.peer_node_ids,
            intent.learner_node_ids,
            intent.serving_state != .retiring,
        );
        defer self.alloc.free(changes);
        if (changes.len == 0) return false;

        self.host.proposeConfChangeV2(intent.record.group_id, .{ .changes = changes }) catch |err| return switch (err) {
            error.PendingConfChange,
            error.MustLeaveJointFirst,
            error.NotLeader,
            error.ProposalDropped,
            error.LeaderTransferInProgress,
            => false,
            else => err,
        };
        return true;
    }
};

fn retirementLeaderTransferTarget(
    status: raft_engine.core.Status,
    intent: PlacementIntent,
) ?u64 {
    if (intent.serving_state != .retiring) return null;
    if (!containsNodeId(status.conf_state.voters, status.id)) return null;
    if (containsNodeId(intent.peer_node_ids, status.id)) return null;

    var target: ?u64 = null;
    for (intent.peer_node_ids) |node_id| {
        if (!containsNodeId(status.conf_state.voters, node_id)) continue;
        if (target == null or node_id < target.?) target = node_id;
    }
    return target;
}

fn localNodeCanProposeMembership(status: raft_engine.core.Status) bool {
    return containsNodeId(status.conf_state.voters, status.id) or
        containsNodeId(status.conf_state.voters_outgoing, status.id);
}

fn allocMembershipChanges(
    alloc: std.mem.Allocator,
    current_voters: []const u64,
    current_learners: []const u64,
    local_node_id: u64,
    voter_node_ids: []const u64,
    learner_node_ids: []const u64,
) ![]raft_engine.core.ConfChangeSingle {
    return allocMembershipChangesWithLocalPolicy(
        alloc,
        current_voters,
        current_learners,
        local_node_id,
        voter_node_ids,
        learner_node_ids,
        true,
    );
}

fn allocMembershipChangesWithLocalPolicy(
    alloc: std.mem.Allocator,
    current_voters: []const u64,
    current_learners: []const u64,
    local_node_id: u64,
    voter_node_ids: []const u64,
    learner_node_ids: []const u64,
    retain_local_voter: bool,
) ![]raft_engine.core.ConfChangeSingle {
    var desired_voters = std.ArrayListUnmanaged(u64).empty;
    defer desired_voters.deinit(alloc);
    var desired_learners = std.ArrayListUnmanaged(u64).empty;
    defer desired_learners.deinit(alloc);
    for (learner_node_ids) |node_id| {
        // Raft membership is monotonic through relocation. Metadata can
        // transiently replay an older learner intent after promotion, but an
        // existing voter must never be demoted in place. Retain it as a voter;
        // the next fresh intent will either confirm promotion or remove it
        // through the ordinary contraction path.
        if (containsNodeId(current_voters, node_id)) {
            try appendUniqueNodeId(alloc, &desired_voters, node_id);
        } else {
            try appendUniqueNodeId(alloc, &desired_learners, node_id);
        }
    }
    if (retain_local_voter and !containsNodeId(desired_learners.items, local_node_id))
        try appendUniqueNodeId(alloc, &desired_voters, local_node_id);
    for (voter_node_ids) |node_id| {
        if (!containsNodeId(desired_learners.items, node_id))
            try appendUniqueNodeId(alloc, &desired_voters, node_id);
    }
    std.mem.sort(u64, desired_voters.items, {}, std.sort.asc(u64));
    std.mem.sort(u64, desired_learners.items, {}, std.sort.asc(u64));

    var changes = std.ArrayListUnmanaged(raft_engine.core.ConfChangeSingle).empty;
    errdefer changes.deinit(alloc);
    for (desired_voters.items) |node_id| {
        if (!containsNodeId(current_voters, node_id)) {
            try changes.append(alloc, .{ .change_type = .add_node, .node_id = node_id });
        }
    }
    for (desired_learners.items) |node_id| {
        if (!containsNodeId(current_learners, node_id)) {
            try changes.append(alloc, .{ .change_type = .add_learner_node, .node_id = node_id });
        }
    }
    for (current_voters) |node_id| {
        if (!containsNodeId(desired_voters.items, node_id) and
            !containsNodeId(desired_learners.items, node_id))
        {
            try changes.append(alloc, .{ .change_type = .remove_node, .node_id = node_id });
        }
    }
    for (current_learners) |node_id| {
        if (!containsNodeId(desired_voters.items, node_id) and
            !containsNodeId(desired_learners.items, node_id))
        {
            try changes.append(alloc, .{ .change_type = .remove_node, .node_id = node_id });
        }
    }
    return try changes.toOwnedSlice(alloc);
}

fn appendUniqueNodeId(alloc: std.mem.Allocator, node_ids: *std.ArrayListUnmanaged(u64), node_id: u64) !void {
    if (!containsNodeId(node_ids.items, node_id)) try node_ids.append(alloc, node_id);
}

fn containsNodeId(node_ids: []const u64, node_id: u64) bool {
    for (node_ids) |candidate| {
        if (candidate == node_id) return true;
    }
    return false;
}

fn hashIntent(intent: PlacementIntent) u64 {
    var hasher = std.hash.Wyhash.init(0);
    hashU64(&hasher, intent.record.group_id);
    hashU64(&hasher, intent.record.replica_id);
    hashU64(&hasher, intent.record.local_node_id);
    hashU64(&hasher, @as(u64, @intFromEnum(intent.record.bootstrap_mode)));
    hashU64(&hasher, intent.record.metadata_version);
    hashU64(&hasher, intent.store_id);
    hashU64(&hasher, @intFromEnum(intent.serving_state));
    hashU64(&hasher, intent.relocation_generation);
    hashU64(&hasher, intent.relocation_source_node_id);
    hashU64(&hasher, intent.relocation_source_store_id);
    hashU64(&hasher, intent.relocation_doc_count_watermark);
    hashU64(&hasher, intent.relocation_disk_bytes_watermark);
    hashU64(&hasher, intent.relocation_target_sequence);
    hashU64(&hasher, intent.relocation_applied_sequence);
    hashU64(&hasher, @intCast(intent.peer_node_ids.len));
    for (intent.peer_node_ids) |node_id| hashU64(&hasher, node_id);
    hashU64(&hasher, @intCast(intent.learner_node_ids.len));
    for (intent.learner_node_ids) |node_id| hashU64(&hasher, node_id);
    if (intent.record.snapshot_bootstrap) |snapshot| {
        hashU64(&hasher, 1);
        hashU64(&hasher, snapshot.from_node_id);
        hashU64(&hasher, snapshot.term);
        hasher.update(snapshot.snapshot_id);
        hasher.update(snapshot.uri);
    } else {
        hashU64(&hasher, 0);
    }
    if (intent.record.backup_restore_bootstrap) |backup| {
        hashU64(&hasher, 1);
        hasher.update(backup.backup_id);
        hasher.update(backup.location);
        hasher.update(backup.snapshot_path);
    } else {
        hashU64(&hasher, 0);
    }
    return hasher.final();
}

fn hashU64(hasher: *std.hash.Wyhash, value: u64) void {
    var numeric = value;
    hasher.update(std.mem.asBytes(&numeric));
}

fn cloneIntent(alloc: std.mem.Allocator, intent: PlacementIntent) !PlacementIntent {
    var cloned_record = try intent.record.clone(alloc);
    errdefer cloned_record.deinit(alloc);
    const peer_node_ids = if (intent.peer_node_ids.len == 0) &.{} else try alloc.dupe(u64, intent.peer_node_ids);
    errdefer if (peer_node_ids.len > 0) alloc.free(peer_node_ids);
    const learner_node_ids = if (intent.learner_node_ids.len == 0) &.{} else try alloc.dupe(u64, intent.learner_node_ids);
    errdefer if (learner_node_ids.len > 0) alloc.free(learner_node_ids);
    return .{
        .record = cloned_record,
        .store_id = intent.store_id,
        .peer_node_ids = peer_node_ids,
        .learner_node_ids = learner_node_ids,
        .serving_state = intent.serving_state,
        .relocation_generation = intent.relocation_generation,
        .relocation_source_node_id = intent.relocation_source_node_id,
        .relocation_source_store_id = intent.relocation_source_store_id,
        .relocation_doc_count_watermark = intent.relocation_doc_count_watermark,
        .relocation_disk_bytes_watermark = intent.relocation_disk_bytes_watermark,
        .relocation_target_sequence = intent.relocation_target_sequence,
        .relocation_applied_sequence = intent.relocation_applied_sequence,
    };
}

pub fn cloneIntentOwned(alloc: std.mem.Allocator, intent: PlacementIntent) !PlacementIntent {
    return try cloneIntent(alloc, intent);
}

fn freeIntent(alloc: std.mem.Allocator, intent: PlacementIntent) void {
    var record = intent.record;
    record.deinit(alloc);
    if (intent.peer_node_ids.len > 0) alloc.free(intent.peer_node_ids);
    if (intent.learner_node_ids.len > 0) alloc.free(intent.learner_node_ids);
}

pub fn freeIntentOwned(alloc: std.mem.Allocator, intent: PlacementIntent) void {
    freeIntent(alloc, intent);
}

fn freeIntentSlice(alloc: std.mem.Allocator, intents: []PlacementIntent) void {
    for (intents) |intent| freeIntent(alloc, intent);
    alloc.free(intents);
}

const StagedReconcileTestFactory = struct {
    alloc: std.mem.Allocator,
    stores: [2]*raft_engine.core.MemoryStorage,

    fn iface(self: *@This()) host_mod.ReplicaDescriptorFactory {
        return .{
            .ptr = self,
            .vtable = &.{
                .build_descriptor = buildDescriptor,
                .free_descriptor = freeDescriptor,
            },
        };
    }

    fn buildDescriptor(ptr: *anyopaque, record: catalog.ReplicaRecord) !raft_engine.runtime.ReplicaDescriptor {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        const store = if (record.group_id % 2 == 0) self.stores[0] else self.stores[1];
        const peers = try self.alloc.dupe(
            raft_engine.core.types.NodeId,
            &[_]raft_engine.core.types.NodeId{record.local_node_id},
        );
        return .{
            .group = .{
                .group_id = record.group_id,
                .local_node_id = record.local_node_id,
                .raft_config = .{
                    .id = record.local_node_id,
                    .group_id = record.group_id,
                    .peers = peers,
                    .election_tick = 5,
                    .heartbeat_tick = 1,
                    .pre_vote = false,
                },
                .storage = store.storage(),
            },
            .bootstrap = .persisted,
        };
    }

    fn freeDescriptor(ptr: *anyopaque, _: std.mem.Allocator, descriptor: *raft_engine.runtime.ReplicaDescriptor) void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        self.alloc.free(descriptor.group.raft_config.peers);
    }
};

test "prepared reconcile persists admission before runtime publication" {
    var store_a = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_a.deinit();
    var store_b = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_b.deinit();
    var factory = StagedReconcileTestFactory{
        .alloc = std.testing.allocator,
        .stores = .{ &store_a, &store_b },
    };
    var replica_catalog = catalog.MemoryReplicaCatalog.init(std.testing.allocator);
    defer replica_catalog.deinit();
    var host = host_mod.Host.init(std.testing.allocator, .{ .local_node_id = 1 }, .{
        .descriptor_factory = factory.iface(),
        .replica_catalog = replica_catalog.catalog(),
    });
    defer host.deinit();
    var provider = MemoryPlacementProvider.init(std.testing.allocator);
    defer provider.deinit();
    try provider.replaceAll(&.{.{
        .record = .{ .group_id = 502, .replica_id = 1, .local_node_id = 1 },
    }});
    var owner = Reconciler{
        .alloc = std.testing.allocator,
        .host = &host,
        .provider = provider.provider(),
    };
    defer owner.deinit();

    var prepared = try owner.prepare();
    defer prepared.deinit();
    prepared.beginPreparation();
    try prepared.prepareDurable();

    try std.testing.expectEqual(host_mod.HostedReplicaStatus.absent, host.status(502));
    const records = try replica_catalog.catalog().listReplicas(std.testing.allocator);
    defer catalog.freeReplicaRecords(std.testing.allocator, records);
    try std.testing.expectEqual(@as(usize, 1), records.len);
    try std.testing.expectEqual(@as(u64, 502), records[0].group_id);

    const result = try prepared.commit();
    try std.testing.expectEqual(@as(usize, 1), result.ensured);
    try std.testing.expectEqual(host_mod.HostedReplicaStatus.active, host.status(502));
}

test "prepared reconcile failure never publishes an unprepared replica" {
    var store_a = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_a.deinit();
    var store_b = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_b.deinit();
    var factory = StagedReconcileTestFactory{
        .alloc = std.testing.allocator,
        .stores = .{ &store_a, &store_b },
    };
    var host = host_mod.Host.init(std.testing.allocator, .{ .local_node_id = 1 }, .{
        .descriptor_factory = factory.iface(),
    });
    defer host.deinit();
    var provider = MemoryPlacementProvider.init(std.testing.allocator);
    defer provider.deinit();
    try provider.replaceAll(&.{.{
        .record = .{
            .group_id = 503,
            .replica_id = 1,
            .local_node_id = 1,
            .bootstrap_mode = .fetch_snapshot,
            .backup_restore_bootstrap = .{
                .backup_id = "backup-503",
                .location = "file:///unused",
                .snapshot_path = "backup-503/groups/503",
            },
        },
    }});
    var owner = Reconciler{
        .alloc = std.testing.allocator,
        .host = &host,
        .provider = provider.provider(),
    };
    defer owner.deinit();

    var prepared = try owner.prepare();
    defer prepared.deinit();
    prepared.beginPreparation();
    const failure = prepared.prepareDurable();
    try std.testing.expectError(error.MissingBackupRestoreBootstrapHandler, failure);
    prepared.notePreparationFailure(error.MissingBackupRestoreBootstrapHandler);

    try std.testing.expectEqual(host_mod.HostedReplicaStatus.failed, host.status(503));
    try std.testing.expect(!host.hasReplica(503));
    try std.testing.expectError(error.InvalidReconcilePhase, prepared.commit());
}

test "blocked reconcile preparation does not block existing raft progress" {
    if (@import("builtin").single_threaded) return error.SkipZigTest;

    const BlockingBootstrapper = struct {
        entered: std.atomic.Value(bool) = .init(false),
        release: std.atomic.Value(bool) = .init(false),

        fn iface(self: *@This()) host_mod.BackupRestoreBootstrapper {
            return .{
                .ptr = self,
                .vtable = &.{ .prepare_backup_restore = prepareBackupRestore },
            };
        }

        fn prepareBackupRestore(ptr: *anyopaque, _: catalog.ReplicaRecord) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.entered.store(true, .release);
            while (!self.release.load(.acquire)) std.Thread.yield() catch {};
        }
    };
    const PrepareThread = struct {
        prepared: *PreparedReconcile,
        failure: ?anyerror = null,

        fn run(self: *@This()) void {
            self.prepared.prepareDurable() catch |err| {
                self.failure = err;
            };
        }
    };

    var store_a = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_a.deinit();
    var store_b = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_b.deinit();
    var factory = StagedReconcileTestFactory{
        .alloc = std.testing.allocator,
        .stores = .{ &store_a, &store_b },
    };
    var bootstrapper = BlockingBootstrapper{};
    var host = host_mod.Host.init(std.testing.allocator, .{ .local_node_id = 1 }, .{
        .descriptor_factory = factory.iface(),
        .backup_restore_bootstrapper = bootstrapper.iface(),
    });
    defer host.deinit();
    _ = try host.ensureReplica(.{ .group_id = 504, .replica_id = 1, .local_node_id = 1 });

    var provider = MemoryPlacementProvider.init(std.testing.allocator);
    defer provider.deinit();
    try provider.replaceAll(&.{.{
        .record = .{
            .group_id = 505,
            .replica_id = 2,
            .local_node_id = 1,
            .bootstrap_mode = .fetch_snapshot,
            .backup_restore_bootstrap = .{
                .backup_id = "backup-505",
                .location = "file:///unused",
                .snapshot_path = "backup-505/groups/505",
            },
        },
    }});
    var owner = Reconciler{
        .alloc = std.testing.allocator,
        .host = &host,
        .provider = provider.provider(),
    };
    defer owner.deinit();
    var prepared = try owner.prepare();
    defer prepared.deinit();
    prepared.beginPreparation();

    var prepare_thread = PrepareThread{ .prepared = &prepared };
    const thread = try std.Thread.spawn(.{}, PrepareThread.run, .{&prepare_thread});
    var thread_joined = false;
    defer {
        if (!thread_joined) {
            bootstrapper.release.store(true, .release);
            thread.join();
        }
    }
    while (!bootstrapper.entered.load(.acquire)) std.Thread.yield() catch {};

    const rounds_before = host.metricsSnapshot().runtime_rounds;
    _ = try host.runRound(1, 1);
    try std.testing.expectEqual(rounds_before + 1, host.metricsSnapshot().runtime_rounds);

    bootstrapper.release.store(true, .release);
    thread.join();
    thread_joined = true;
    try std.testing.expect(prepare_thread.failure == null);
    const result = try prepared.commit();
    try std.testing.expectEqual(@as(usize, 1), result.ensured);
    try std.testing.expectEqual(@as(usize, 1), result.removed);
}

test "reconciler can ensure desired replicas and remove stale ones" {
    const Factory = struct {
        alloc: std.mem.Allocator,
        stores: [2]*raft_engine.core.MemoryStorage,

        fn iface(self: *@This()) host_mod.ReplicaDescriptorFactory {
            return .{
                .ptr = self,
                .vtable = &.{
                    .build_descriptor = buildDescriptor,
                    .free_descriptor = freeDescriptor,
                },
            };
        }

        fn buildDescriptor(ptr: *anyopaque, record: catalog.ReplicaRecord) !raft_engine.runtime.ReplicaDescriptor {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const store = if (record.group_id == 301) self.stores[0] else self.stores[1];
            const peers = try self.alloc.dupe(raft_engine.core.types.NodeId, &[_]raft_engine.core.types.NodeId{record.local_node_id});
            return .{
                .group = .{
                    .group_id = record.group_id,
                    .local_node_id = record.local_node_id,
                    .raft_config = .{
                        .id = record.local_node_id,
                        .group_id = record.group_id,
                        .peers = peers[0..],
                        .election_tick = 5,
                        .heartbeat_tick = 1,
                        .pre_vote = false,
                    },
                    .storage = store.storage(),
                },
                .bootstrap = .persisted,
            };
        }

        fn freeDescriptor(ptr: *anyopaque, alloc: std.mem.Allocator, desc: *raft_engine.runtime.ReplicaDescriptor) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = alloc;
            self.alloc.free(desc.group.raft_config.peers);
        }
    };

    const Resolver = struct {
        fn iface(_: *@This()) peer_resolver.PeerResolver {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .resolve_group_peer = resolve,
                },
            };
        }

        fn resolve(_: *anyopaque, alloc: std.mem.Allocator, group_id: u64, node_id: u64) ![]peer_resolver.PeerEndpoint {
            _ = group_id;
            return try alloc.dupe(peer_resolver.PeerEndpoint, &.{
                .{
                    .protocol = .http,
                    .address = if (node_id == 2) try alloc.dupe(u8, "http://n2") else try alloc.dupe(u8, "http://n3"),
                    .metadata = try alloc.dupe(u8, ""),
                },
            });
        }
    };

    var store_a = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_a.deinit();
    var store_b = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_b.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .stores = .{ &store_a, &store_b } };
    var resolver = Resolver{};
    var host = host_mod.Host.init(std.testing.allocator, .{ .local_node_id = 1 }, .{
        .descriptor_factory = factory.iface(),
        .peer_resolver = resolver.iface(),
    });
    defer host.deinit();

    _ = try host.ensureReplica(.{
        .group_id = 301,
        .replica_id = 1,
        .local_node_id = 1,
    });

    var provider = MemoryPlacementProvider.init(std.testing.allocator);
    defer provider.deinit();
    try provider.replaceAll(&.{
        .{
            .record = .{
                .group_id = 302,
                .replica_id = 2,
                .local_node_id = 1,
            },
            .peer_node_ids = &.{2},
            .learner_node_ids = &.{ 2, 3 },
        },
    });

    var reconciler = Reconciler{
        .alloc = std.testing.allocator,
        .host = &host,
        .provider = provider.provider(),
    };
    defer reconciler.deinit();
    const result = try reconciler.reconcileOnce();
    try std.testing.expectEqual(@as(usize, 1), result.ensured);
    try std.testing.expectEqual(@as(usize, 1), result.removed);
    try std.testing.expectEqual(@as(usize, 2), result.refreshed_peers);
    try std.testing.expectEqual(host_mod.HostedReplicaStatus.absent, host.status(301));
    try std.testing.expectEqual(host_mod.HostedReplicaStatus.active, host.status(302));
}

test "reconciler skips unchanged intents after first apply" {
    const Factory = struct {
        alloc: std.mem.Allocator,
        store: *raft_engine.core.MemoryStorage,

        fn iface(self: *@This()) host_mod.ReplicaDescriptorFactory {
            return .{
                .ptr = self,
                .vtable = &.{
                    .build_descriptor = buildDescriptor,
                    .free_descriptor = freeDescriptor,
                },
            };
        }

        fn buildDescriptor(ptr: *anyopaque, record: catalog.ReplicaRecord) !raft_engine.runtime.ReplicaDescriptor {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const peers = try self.alloc.dupe(raft_engine.core.types.NodeId, &[_]raft_engine.core.types.NodeId{record.local_node_id});
            return .{
                .group = .{
                    .group_id = record.group_id,
                    .local_node_id = record.local_node_id,
                    .raft_config = .{
                        .id = record.local_node_id,
                        .group_id = record.group_id,
                        .peers = peers,
                        .election_tick = 5,
                        .heartbeat_tick = 1,
                        .pre_vote = false,
                    },
                    .storage = self.store.storage(),
                },
                .bootstrap = .persisted,
            };
        }

        fn freeDescriptor(ptr: *anyopaque, alloc: std.mem.Allocator, desc: *raft_engine.runtime.ReplicaDescriptor) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = alloc;
            self.alloc.free(desc.group.raft_config.peers);
        }
    };

    const Resolver = struct {
        fn iface(_: *@This()) peer_resolver.PeerResolver {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .resolve_group_peer = resolve,
                },
            };
        }

        fn resolve(_: *anyopaque, alloc: std.mem.Allocator, _: u64, _: u64) ![]peer_resolver.PeerEndpoint {
            return try alloc.dupe(peer_resolver.PeerEndpoint, &.{});
        }
    };

    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .store = &store };
    var resolver = Resolver{};
    var host = host_mod.Host.init(std.testing.allocator, .{ .local_node_id = 1 }, .{
        .descriptor_factory = factory.iface(),
        .peer_resolver = resolver.iface(),
    });
    defer host.deinit();

    var provider = MemoryPlacementProvider.init(std.testing.allocator);
    defer provider.deinit();
    try provider.replaceAll(&.{
        .{
            .record = .{
                .group_id = 401,
                .replica_id = 1,
                .local_node_id = 1,
                .metadata_version = 7,
            },
            .peer_node_ids = &.{ 2, 3 },
        },
    });

    var reconciler = Reconciler{
        .alloc = std.testing.allocator,
        .host = &host,
        .provider = provider.provider(),
    };
    defer reconciler.deinit();

    const first = try reconciler.reconcileOnce();
    try std.testing.expectEqual(@as(usize, 1), first.ensured);
    try std.testing.expectEqual(@as(usize, 0), first.removed);

    const ensure_calls_after_first = host.metrics.ensure_replica_calls;
    const rounds_after_first = host.metrics.reconcile_rounds;

    const second = try reconciler.reconcileOnce();
    try std.testing.expectEqual(@as(usize, 0), second.ensured);
    try std.testing.expectEqual(@as(usize, 0), second.removed);
    try std.testing.expectEqual(ensure_calls_after_first, host.metrics.ensure_replica_calls);
    try std.testing.expectEqual(rounds_after_first + 1, host.metrics.reconcile_rounds);
}

test "membership reconciliation expands before removing obsolete voters" {
    const changes = try allocMembershipChanges(
        std.testing.allocator,
        &.{ 1, 2, 5 },
        &.{},
        1,
        &.{ 1, 2, 3, 4 },
        &.{},
    );
    defer std.testing.allocator.free(changes);

    try std.testing.expectEqualSlices(raft_engine.core.ConfChangeSingle, &.{
        .{ .change_type = .add_node, .node_id = 3 },
        .{ .change_type = .add_node, .node_id = 4 },
        .{ .change_type = .remove_node, .node_id = 5 },
    }, changes);
}

test "membership reconciliation normalizes duplicate and missing local voters" {
    const changes = try allocMembershipChanges(
        std.testing.allocator,
        &.{ 7, 8 },
        &.{},
        7,
        &.{ 8, 8 },
        &.{},
    );
    defer std.testing.allocator.free(changes);

    try std.testing.expectEqual(@as(usize, 0), changes.len);
}

test "membership reconciliation removes a hosted retiring local voter" {
    const changes = try allocMembershipChangesWithLocalPolicy(
        std.testing.allocator,
        &.{ 7, 8 },
        &.{},
        7,
        &.{8},
        &.{},
        false,
    );
    defer std.testing.allocator.free(changes);

    try std.testing.expectEqualSlices(raft_engine.core.ConfChangeSingle, &.{
        .{ .change_type = .remove_node, .node_id = 7 },
    }, changes);
}

test "membership reconciliation transfers a retiring leader to a retained voter" {
    var status: raft_engine.core.Status = .{
        .id = 7,
        .group_id = 11,
        .soft = .{ .leader_id = 7, .role = .leader },
        .hard = .{},
        .conf_state = .{ .voters = @constCast((&[_]u64{ 7, 8, 9 })[0..]) },
    };
    const retiring = PlacementIntent{
        .record = .{ .group_id = 11, .replica_id = 7, .local_node_id = 7 },
        .peer_node_ids = &.{ 9, 8 },
        .serving_state = .retiring,
    };

    try std.testing.expectEqual(@as(?u64, 8), retirementLeaderTransferTarget(status, retiring));
    status.conf_state.voters = @constCast((&[_]u64{7})[0..]);
    try std.testing.expectEqual(@as(?u64, null), retirementLeaderTransferTarget(status, retiring));
}

test "membership reconciliation hydrates learners before voter promotion" {
    const hydrate = try allocMembershipChanges(
        std.testing.allocator,
        &.{ 1, 2 },
        &.{},
        1,
        &.{ 1, 2 },
        &.{3},
    );
    defer std.testing.allocator.free(hydrate);
    try std.testing.expectEqualSlices(raft_engine.core.ConfChangeSingle, &.{
        .{ .change_type = .add_learner_node, .node_id = 3 },
    }, hydrate);

    const promote = try allocMembershipChanges(
        std.testing.allocator,
        &.{ 1, 2 },
        &.{3},
        1,
        &.{ 1, 2, 3 },
        &.{},
    );
    defer std.testing.allocator.free(promote);
    try std.testing.expectEqualSlices(raft_engine.core.ConfChangeSingle, &.{
        .{ .change_type = .add_node, .node_id = 3 },
    }, promote);
}

test "membership reconciliation never demotes a voter from a stale learner intent" {
    const changes = try allocMembershipChanges(
        std.testing.allocator,
        &.{ 1, 2, 3 },
        &.{},
        1,
        &.{ 1, 2 },
        &.{3},
    );
    defer std.testing.allocator.free(changes);

    try std.testing.expectEqual(@as(usize, 0), changes.len);
}

test "membership reconciliation requires a local voter" {
    const base: raft_engine.core.Status = .{
        .id = 7,
        .group_id = 11,
        .soft = .{ .leader_id = 7, .role = .leader },
        .hard = .{},
        .conf_state = .{},
        .last_index = 0,
        .applied_index = 0,
        .election_elapsed = 0,
        .randomized_election_timeout = 0,
        .votes_granted = 0,
        .votes_rejected = 0,
        .votes_unknown = 0,
    };

    var removed = base;
    removed.conf_state.voters = @constCast((&[_]u64{ 8, 9 })[0..]);
    try std.testing.expect(!localNodeCanProposeMembership(removed));

    var outgoing = base;
    outgoing.conf_state.voters = @constCast((&[_]u64{ 8, 9 })[0..]);
    outgoing.conf_state.voters_outgoing = @constCast((&[_]u64{ 7, 8, 9 })[0..]);
    try std.testing.expect(localNodeCanProposeMembership(outgoing));
}

test "reconciler module compiles" {
    _ = PlacementIntent;
    _ = PlacementProvider;
    _ = MemoryPlacementProvider;
    _ = MetadataPlacementUpdate;
    _ = MetadataPlacementState;
    _ = ReconcileResult;
    _ = Reconciler;
    _ = peer_resolver;
}

test "metadata placement state applies incremental updates" {
    var state = MetadataPlacementState.init(std.testing.allocator);
    defer state.deinit();

    try state.apply(.{
        .upsert_intent = .{
            .record = .{
                .group_id = 41,
                .replica_id = 2,
                .local_node_id = 7,
                .metadata_version = 1,
            },
            .peer_node_ids = &.{ 7, 8 },
        },
    });
    try state.apply(.{
        .upsert_intent = .{
            .record = .{
                .group_id = 42,
                .replica_id = 3,
                .local_node_id = 9,
                .metadata_version = 1,
            },
            .peer_node_ids = &.{9},
        },
    });

    const intents = try state.provider().listLocalIntents(std.testing.allocator, 7);
    defer {
        for (intents) |intent| freeIntent(std.testing.allocator, intent);
        std.testing.allocator.free(intents);
    }
    try std.testing.expectEqual(@as(usize, 1), intents.len);
    try std.testing.expectEqual(@as(u64, 41), intents[0].record.group_id);
    try std.testing.expectEqual(@as(usize, 2), intents[0].peer_node_ids.len);

    try std.testing.expect(try state.removeGroup(41));
    const after = try state.provider().listLocalIntents(std.testing.allocator, 7);
    defer {
        for (after) |intent| freeIntent(std.testing.allocator, intent);
        std.testing.allocator.free(after);
    }
    try std.testing.expectEqual(@as(usize, 0), after.len);
}

test "cloneIntentOwned deep clones backup restore metadata" {
    const original = PlacementIntent{
        .record = .{
            .group_id = 52,
            .replica_id = 4,
            .local_node_id = 9,
            .metadata_version = 11,
            .backup_restore_bootstrap = .{
                .backup_id = try std.testing.allocator.dupe(u8, "snap-52"),
                .location = try std.testing.allocator.dupe(u8, "file:///tmp/backups"),
                .snapshot_path = try std.testing.allocator.dupe(u8, "snap-52/groups/52"),
            },
        },
        .store_id = 21,
        .peer_node_ids = try std.testing.allocator.dupe(u64, &.{ 9, 10 }),
        .learner_node_ids = try std.testing.allocator.dupe(u64, &.{11}),
    };
    defer freeIntentOwned(std.testing.allocator, original);

    const cloned = try cloneIntentOwned(std.testing.allocator, original);
    defer freeIntentOwned(std.testing.allocator, cloned);

    try std.testing.expect(cloned.record.backup_restore_bootstrap != null);
    try std.testing.expect(cloned.record.backup_restore_bootstrap.?.backup_id.ptr != original.record.backup_restore_bootstrap.?.backup_id.ptr);
    try std.testing.expect(cloned.record.backup_restore_bootstrap.?.location.ptr != original.record.backup_restore_bootstrap.?.location.ptr);
    try std.testing.expect(cloned.record.backup_restore_bootstrap.?.snapshot_path.ptr != original.record.backup_restore_bootstrap.?.snapshot_path.ptr);
    try std.testing.expect(cloned.peer_node_ids.ptr != original.peer_node_ids.ptr);
    try std.testing.expect(cloned.learner_node_ids.ptr != original.learner_node_ids.ptr);
}
