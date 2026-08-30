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

    pub fn desiredMembership(self: PlacementIntent) DesiredMembership {
        return .{
            .voters = self.peer_node_ids,
            .learners = self.learner_node_ids,
        };
    }
};

/// Mutable topology intent. It is deliberately independent from replica
/// admission: reconciliation may revisit this value many times for one live
/// replica without rebuilding storage or rewriting runtime policy.
pub const DesiredMembership = struct {
    voters: []const u64,
    learners: []const u64,

    pub fn validate(self: DesiredMembership) !void {
        try validateNodeSet(self.voters);
        try validateNodeSet(self.learners);
    }
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

/// Optional durable policy boundary for consensus membership. Returning false
/// defers the proposal without mutating the desired intent, allowing the
/// policy owner to release the fence and resume normal reconciliation later.
pub const MembershipChangePermit = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        allows: *const fn (
            ptr: *anyopaque,
            group_id: u64,
            voter_node_ids: []const u64,
            learner_node_ids: []const u64,
        ) bool,
    };

    pub fn allows(self: MembershipChangePermit, intent: PlacementIntent) bool {
        return self.vtable.allows(
            self.ptr,
            intent.record.group_id,
            intent.peer_node_ids,
            intent.learner_node_ids,
        );
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
    admission_blocked: usize = 0,
    membership_proposals: usize = 0,
    membership_converged: usize = 0,
    membership_waiting_for_replica: usize = 0,
    membership_waiting_for_leader: usize = 0,
    membership_waiting_for_local_voter: usize = 0,
    membership_waiting_for_pending_change: usize = 0,
    membership_waiting_for_policy: usize = 0,
    membership_leader_transfers: usize = 0,

    fn recordMembership(self: *ReconcileResult, outcome: MembershipConvergence) void {
        switch (outcome) {
            .converged => self.membership_converged += 1,
            .waiting_for_replica => self.membership_waiting_for_replica += 1,
            .waiting_for_leader => self.membership_waiting_for_leader += 1,
            .waiting_for_local_voter => self.membership_waiting_for_local_voter += 1,
            .waiting_for_pending_change => self.membership_waiting_for_pending_change += 1,
            .waiting_for_policy => self.membership_waiting_for_policy += 1,
            .leader_transfer_started => self.membership_leader_transfers += 1,
            .proposal_submitted => self.membership_proposals += 1,
        }
    }
};

/// Operator-facing reason that desired membership has or has not converged.
/// These are normal lifecycle states, not generic server-round failures.
pub const MembershipConvergence = enum {
    converged,
    waiting_for_replica,
    waiting_for_leader,
    waiting_for_local_voter,
    waiting_for_pending_change,
    waiting_for_policy,
    leader_transfer_started,
    proposal_submitted,
};

const PreparedEnsure = struct {
    intent_index: usize,
    intent_hash: u64,
    prepare_bootstrap: bool,
    replica: ?host_mod.PreparedReplica = null,
    restart_policy_blocked: bool = false,
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
    catalog_upserts: []catalog.ReplicaRecord,
    catalog_revision: ?u64,
    /// Set immediately after the atomic replica-catalog batch is durable.
    /// Callers that stage sidecar cleanup intents use this to distinguish a
    /// retryable pre-commit failure from a committed removal whose live host
    /// teardown still needs recovery.
    catalog_commit_complete: bool = false,
    failed_ensure_index: ?usize = null,
    durability_complete: bool = false,
    committed: bool = false,
    aborted: bool = false,

    pub fn deinit(self: *PreparedReconcile) void {
        for (self.ensures) |*entry| {
            if (entry.replica) |*replica| replica.deinit(self.owner.alloc);
        }
        self.owner.alloc.free(self.ensures);
        self.owner.alloc.free(self.removals);
        self.owner.alloc.free(self.catalog_upserts);
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
            entry.replica = self.owner.host.prepareReplicaUnpublished(record, entry.prepare_bootstrap) catch |err| {
                if (err == error.ReplicaRuntimePolicyMismatch) {
                    // The live group remains safe and can continue membership
                    // convergence. Record desired catalog state and expose the
                    // restart-scoped conflict instead of failing the process's
                    // entire control round.
                    entry.restart_policy_blocked = true;
                    continue;
                }
                self.failed_ensure_index = index;
                return err;
            };
        }
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
        if (!self.durability_complete or self.committed or self.aborted) return error.InvalidReconcilePhase;

        if (self.catalog_upserts.len > 0 or self.removals.len > 0) {
            try self.owner.host.commitReplicaCatalog(
                self.catalog_revision,
                self.catalog_upserts,
                self.removals,
            );
        }
        self.catalog_commit_complete = true;
        var result: ReconcileResult = .{};
        for (self.ensures) |*entry| {
            const intent = self.intents[entry.intent_index];
            if (entry.restart_policy_blocked) {
                result.admission_blocked += 1;
                result.refreshed_peers += try self.owner.refreshPeerEndpoints(intent);
                // The desired record is durable and will take effect on the
                // next replica restart. Remember its fingerprint so an
                // unchanged restart-scoped policy does not fsync the catalog
                // on every control round.
                try self.owner.last_intent_hashes.put(
                    self.owner.alloc,
                    intent.record.group_id,
                    entry.intent_hash,
                );
                continue;
            }
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
            const outcome = try self.owner.reconcileRaftMembership(intent);
            self.owner.membership_convergence.putAssumeCapacity(intent.record.group_id, outcome);
            result.recordMembership(outcome);
        }
        for (self.removals) |group_id| {
            try self.owner.host.removePreparedReplica(group_id);
            _ = self.owner.last_intent_hashes.remove(group_id);
            _ = self.owner.membership_convergence.remove(group_id);
            result.removed += 1;
        }
        self.owner.host.metrics.reconcile_rounds += 1;
        self.committed = true;
        return result;
    }

    /// Discards unpublished descriptors after the caller observes a newer
    /// desired-state epoch. Durable catalog state is untouched until commit.
    pub fn abortDurable(self: *PreparedReconcile) !void {
        if (!self.durability_complete or self.committed or self.aborted)
            return error.InvalidReconcilePhase;
        for (self.ensures) |entry| {
            if (!entry.prepare_bootstrap) continue;
            self.owner.host.cancelReplicaBootstrapPreparation(self.intents[entry.intent_index].record);
        }
        self.aborted = true;
    }
};

pub const Reconciler = struct {
    alloc: std.mem.Allocator,
    host: *host_mod.Host,
    provider: PlacementProvider,
    membership_change_permit: ?MembershipChangePermit = null,
    last_intent_hashes: std.AutoHashMapUnmanaged(u64, u64) = .empty,
    membership_convergence: std.AutoHashMapUnmanaged(u64, MembershipConvergence) = .empty,

    pub fn deinit(self: *Reconciler) void {
        self.last_intent_hashes.deinit(self.alloc);
        self.last_intent_hashes = .empty;
        self.membership_convergence.deinit(self.alloc);
        self.membership_convergence = .empty;
    }

    pub fn membershipStatus(self: *const Reconciler, group_id: u64) ?MembershipConvergence {
        return self.membership_convergence.get(group_id);
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
            try intent.desiredMembership().validate();
            try desired_group_ids.put(self.alloc, intent.record.group_id, {});

            const intent_hash = hashIntent(intent);
            const hosted_status = self.host.status(intent.record.group_id);
            const stored_hash = self.last_intent_hashes.get(intent.record.group_id);
            // Quarantine is a stable operator-owned state, not an incomplete
            // ensure. Reapply only a genuinely changed intent; otherwise a
            // reconciliation loop would rewrite the catalog every round while
            // traffic correctly remains unable to auto-resume the group.
            const lifecycle_incomplete = switch (hosted_status) {
                .active, .quarantined => false,
                .absent, .starting, .quiesced, .snapshotting, .failed => true,
            };
            const should_apply =
                lifecycle_incomplete or
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
        errdefer self.alloc.free(owned_removals);
        const catalog_upserts = try self.alloc.alloc(catalog.ReplicaRecord, owned_ensures.len);
        for (owned_ensures, 0..) |entry, index| {
            catalog_upserts[index] = intents[entry.intent_index].record;
        }
        const catalog_revision = if (catalog_upserts.len > 0 or owned_removals.len > 0)
            self.host.replicaCatalogRevision()
        else
            null;
        const convergence_capacity = std.math.cast(
            u32,
            @as(usize, self.membership_convergence.count()) +| intents.len,
        ) orelse return error.TooManyPlacementIntents;
        try self.membership_convergence.ensureTotalCapacity(self.alloc, convergence_capacity);
        return .{
            .owner = self,
            .intents = intents,
            .ensures = owned_ensures,
            .removals = owned_removals,
            .catalog_upserts = catalog_upserts,
            .catalog_revision = catalog_revision,
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

    /// Advances only mutable Raft membership. It performs no descriptor
    /// construction, catalog write, restore, or filesystem work, so callers
    /// can safely run it for an unchanged metadata epoch until ConfState
    /// converges through leader changes and joint-consensus boundaries.
    pub fn reconcileMembershipOnly(
        self: *Reconciler,
        intents: []const PlacementIntent,
    ) !ReconcileResult {
        const convergence_capacity = std.math.cast(
            u32,
            @as(usize, self.membership_convergence.count()) +| intents.len,
        ) orelse return error.TooManyPlacementIntents;
        try self.membership_convergence.ensureTotalCapacity(self.alloc, convergence_capacity);
        var result: ReconcileResult = .{};
        for (intents) |intent| {
            try intent.desiredMembership().validate();
            const outcome = try self.reconcileRaftMembership(intent);
            self.membership_convergence.putAssumeCapacity(intent.record.group_id, outcome);
            result.recordMembership(outcome);
        }
        return result;
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

    fn reconcileRaftMembership(self: *Reconciler, intent: PlacementIntent) !MembershipConvergence {
        const status = self.host.raftStatus(intent.record.group_id) orelse return .waiting_for_replica;
        if (status.soft.role != .leader or status.soft.leader_id != status.id)
            return .waiting_for_leader;
        if (!localNodeCanProposeMembership(status)) return .waiting_for_local_voter;

        // A new change cannot be proposed while joint consensus is active. Leave
        // the committed joint configuration first; the next reconcile round will
        // calculate any remaining delta from the resulting stable voter set.
        if (status.conf_state.voters_outgoing.len > 0) {
            if (self.membership_change_permit) |permit| {
                if (!permit.allows(intent)) return .waiting_for_policy;
            }
            self.host.proposeConfChangeV2(intent.record.group_id, .{}) catch |err| return switch (err) {
                error.PendingConfChange,
                error.NotInJointState,
                error.ProposalDropped,
                error.LeaderTransferInProgress,
                => .waiting_for_pending_change,
                error.NotLeader => .waiting_for_leader,
                else => err,
            };
            return .proposal_submitted;
        }

        if (retirementLeaderTransferTarget(status, intent)) |transferee| {
            self.host.transferLeader(intent.record.group_id, transferee) catch |err| return switch (err) {
                error.NotLeader => .waiting_for_leader,
                error.LeaderTransferInProgress => .waiting_for_pending_change,
                else => err,
            };
            return .leader_transfer_started;
        }

        if (!membershipChangesRequiredWithLocalPolicy(
            status.conf_state.voters,
            status.conf_state.learners,
            intent.record.local_node_id,
            intent.peer_node_ids,
            intent.learner_node_ids,
            intent.serving_state != .retiring,
        )) return .converged;

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
        if (changes.len == 0) return .converged;
        if (self.membership_change_permit) |permit| {
            if (!permit.allows(intent)) return .waiting_for_policy;
        }

        self.host.proposeConfChangeV2(intent.record.group_id, .{ .changes = changes }) catch |err| return switch (err) {
            error.PendingConfChange,
            error.MustLeaveJointFirst,
            error.ProposalDropped,
            error.LeaderTransferInProgress,
            => .waiting_for_pending_change,
            error.NotLeader => .waiting_for_leader,
            else => err,
        };
        return .proposal_submitted;
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

/// Allocation-free convergence check for the common steady-state path. Its
/// classification mirrors `allocMembershipChangesWithLocalPolicy`: a stale
/// learner intent never demotes an existing voter, while an existing learner
/// named as a voter still requires promotion.
fn membershipChangesRequiredWithLocalPolicy(
    current_voters: []const u64,
    current_learners: []const u64,
    local_node_id: u64,
    voter_node_ids: []const u64,
    learner_node_ids: []const u64,
    retain_local_voter: bool,
) bool {
    for (current_voters) |node_id| {
        if (!isDesiredVoter(
            node_id,
            current_voters,
            local_node_id,
            voter_node_ids,
            learner_node_ids,
            retain_local_voter,
        )) return true;
    }
    for (current_learners) |node_id| {
        if (isDesiredVoter(
            node_id,
            current_voters,
            local_node_id,
            voter_node_ids,
            learner_node_ids,
            retain_local_voter,
        ) or !isDesiredLearner(node_id, current_voters, learner_node_ids)) return true;
    }
    for (voter_node_ids) |node_id| {
        if (isDesiredVoter(
            node_id,
            current_voters,
            local_node_id,
            voter_node_ids,
            learner_node_ids,
            retain_local_voter,
        ) and !containsNodeId(current_voters, node_id)) return true;
    }
    for (learner_node_ids) |node_id| {
        if (isDesiredLearner(node_id, current_voters, learner_node_ids) and
            !containsNodeId(current_learners, node_id)) return true;
    }
    if (retain_local_voter and isDesiredVoter(
        local_node_id,
        current_voters,
        local_node_id,
        voter_node_ids,
        learner_node_ids,
        true,
    ) and !containsNodeId(current_voters, local_node_id)) return true;
    return false;
}

fn isDesiredLearner(
    node_id: u64,
    current_voters: []const u64,
    learner_node_ids: []const u64,
) bool {
    return containsNodeId(learner_node_ids, node_id) and
        !containsNodeId(current_voters, node_id);
}

fn isDesiredVoter(
    node_id: u64,
    current_voters: []const u64,
    local_node_id: u64,
    voter_node_ids: []const u64,
    learner_node_ids: []const u64,
    retain_local_voter: bool,
) bool {
    if (isDesiredLearner(node_id, current_voters, learner_node_ids)) return false;
    return containsNodeId(voter_node_ids, node_id) or
        (retain_local_voter and node_id == local_node_id) or
        (containsNodeId(learner_node_ids, node_id) and containsNodeId(current_voters, node_id));
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

fn validateNodeSet(node_ids: []const u64) !void {
    for (node_ids, 0..) |node_id, index| {
        if (node_id == 0) return error.InvalidTopologyNodeId;
        if (containsNodeId(node_ids[0..index], node_id)) return error.DuplicateTopologyNodeId;
    }
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
    hashNodeSet(&hasher, intent.peer_node_ids);
    hashNodeSet(&hasher, intent.learner_node_ids);
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

/// Order-independent, allocation-free set fingerprint. Metadata serializers
/// may reorder equivalent topology rows; that must not trigger descriptor
/// rebuilds or catalog fsyncs.
fn hashNodeSet(hasher: *std.hash.Wyhash, node_ids: []const u64) void {
    var xor: u64 = 0;
    var sum: u64 = 0;
    for (node_ids) |node_id| {
        var numeric = node_id;
        const digest = std.hash.Wyhash.hash(0x9e3779b97f4a7c15, std.mem.asBytes(&numeric));
        xor ^= digest;
        sum +%= digest;
    }
    hashU64(hasher, @intCast(node_ids.len));
    hashU64(hasher, xor);
    hashU64(hasher, sum);
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
    election_tick: u32 = 5,

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
                    .election_tick = self.election_tick,
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

test "prepared reconcile publishes catalog admission atomically before runtime publication" {
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
    {
        const records = try replica_catalog.catalog().listReplicas(std.testing.allocator);
        defer catalog.freeReplicaRecords(std.testing.allocator, records);
        try std.testing.expectEqual(@as(usize, 0), records.len);
    }

    const result = try prepared.commit();
    try std.testing.expectEqual(@as(usize, 1), result.ensured);
    try std.testing.expectEqual(host_mod.HostedReplicaStatus.active, host.status(502));
    {
        const records = try replica_catalog.catalog().listReplicas(std.testing.allocator);
        defer catalog.freeReplicaRecords(std.testing.allocator, records);
        try std.testing.expectEqual(@as(usize, 1), records.len);
        try std.testing.expectEqual(@as(u64, 502), records[0].group_id);
    }
}

test "prepared reconcile rejects catalog races without clobbering concurrent admissions" {
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
    try replica_catalog.catalog().upsertReplica(.{
        .group_id = 501,
        .replica_id = 1,
        .local_node_id = 1,
    });
    var host = host_mod.Host.init(std.testing.allocator, .{ .local_node_id = 1 }, .{
        .descriptor_factory = factory.iface(),
        .replica_catalog = replica_catalog.catalog(),
    });
    defer host.deinit();
    _ = try host.ensureReplica(.{
        .group_id = 501,
        .replica_id = 1,
        .local_node_id = 1,
    });
    var provider = MemoryPlacementProvider.init(std.testing.allocator);
    defer provider.deinit();
    try provider.replaceAll(&.{.{
        .record = .{ .group_id = 502, .replica_id = 2, .local_node_id = 1 },
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
    {
        const staged = try replica_catalog.catalog().listReplicas(std.testing.allocator);
        defer catalog.freeReplicaRecords(std.testing.allocator, staged);
        try std.testing.expectEqual(@as(usize, 1), staged.len);
        try std.testing.expectEqual(@as(u64, 501), staged[0].group_id);
    }

    try replica_catalog.catalog().upsertReplica(.{
        .group_id = 503,
        .replica_id = 3,
        .local_node_id = 1,
    });
    try std.testing.expectError(error.ReplicaCatalogRevisionChanged, prepared.commit());
    try prepared.abortDurable();
    {
        const restored = try replica_catalog.catalog().listReplicas(std.testing.allocator);
        defer catalog.freeReplicaRecords(std.testing.allocator, restored);
        try std.testing.expectEqual(@as(usize, 2), restored.len);
        var saw_501 = false;
        var saw_503 = false;
        for (restored) |record| {
            saw_501 = saw_501 or record.group_id == 501;
            saw_503 = saw_503 or record.group_id == 503;
            try std.testing.expect(record.group_id != 502);
        }
        try std.testing.expect(saw_501);
        try std.testing.expect(saw_503);
    }
    try std.testing.expectEqual(host_mod.HostedReplicaStatus.active, host.status(501));
    try std.testing.expectEqual(host_mod.HostedReplicaStatus.absent, host.status(502));
    try std.testing.expectError(error.InvalidReconcilePhase, prepared.commit());
}

test "restart-scoped policy conflicts are isolated and durably deduplicated" {
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
    const catalog_iface = replica_catalog.catalog();
    var host = host_mod.Host.init(std.testing.allocator, .{ .local_node_id = 1 }, .{
        .descriptor_factory = factory.iface(),
        .replica_catalog = catalog_iface,
    });
    defer host.deinit();
    var provider = MemoryPlacementProvider.init(std.testing.allocator);
    defer provider.deinit();
    try provider.replaceAll(&.{.{
        .record = .{
            .group_id = 504,
            .replica_id = 1,
            .local_node_id = 1,
            .metadata_version = 1,
        },
        .peer_node_ids = &.{1},
    }});
    var owner = Reconciler{
        .alloc = std.testing.allocator,
        .host = &host,
        .provider = provider.provider(),
    };
    defer owner.deinit();

    const initial = try owner.reconcileOnce();
    try std.testing.expectEqual(@as(usize, 1), initial.ensured);

    factory.election_tick = 6;
    try provider.replaceAll(&.{.{
        .record = .{
            .group_id = 504,
            .replica_id = 1,
            .local_node_id = 1,
            .metadata_version = 2,
        },
        .peer_node_ids = &.{1},
    }});
    const blocked = try owner.reconcileOnce();
    try std.testing.expectEqual(@as(usize, 0), blocked.ensured);
    try std.testing.expectEqual(@as(usize, 1), blocked.admission_blocked);
    try std.testing.expectEqual(host_mod.HostedReplicaStatus.active, host.status(504));
    const conflict = host.replicaAdmissionConflict(504) orelse
        return error.ExpectedReplicaAdmissionConflict;
    switch (conflict) {
        .runtime_policy => |field| try std.testing.expectEqual(
            raft_engine.runtime.group.ReplicaRuntimePolicyField.election_tick,
            field,
        ),
        .local_node_id => return error.UnexpectedReplicaIdentityConflict,
    }
    {
        const records = try catalog_iface.listReplicas(std.testing.allocator);
        defer catalog.freeReplicaRecords(std.testing.allocator, records);
        try std.testing.expectEqual(@as(usize, 1), records.len);
        try std.testing.expectEqual(@as(u64, 2), records[0].metadata_version);
    }

    const durable_revision = catalog_iface.revision();
    const unchanged = try owner.reconcileOnce();
    try std.testing.expectEqual(@as(usize, 0), unchanged.ensured);
    try std.testing.expectEqual(@as(usize, 0), unchanged.admission_blocked);
    try std.testing.expectEqual(durable_revision, catalog_iface.revision());
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
                .artifact_backup_id = "backup-503",
                .location = "file:///unused",
                .snapshot_path = "backup-503/groups/503",
                .connection = "backup-store",
                .artifact_size_bytes = 1,
                .artifact_sha256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
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
                .artifact_backup_id = "backup-505",
                .location = "file:///unused",
                .snapshot_path = "backup-505/groups/505",
                .connection = "backup-store",
                .artifact_size_bytes = 1,
                .artifact_sha256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
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

test "membership convergence check preserves steady-state and monotonic promotion semantics" {
    try std.testing.expect(!membershipChangesRequiredWithLocalPolicy(
        &.{ 1, 2 },
        &.{3},
        1,
        &.{ 1, 2 },
        &.{3},
        true,
    ));
    // A replayed learner row must not demote voter 2.
    try std.testing.expect(!membershipChangesRequiredWithLocalPolicy(
        &.{ 1, 2 },
        &.{3},
        1,
        &.{1},
        &.{ 2, 3 },
        true,
    ));
    // Removing 3 from learner intent requires a change.
    try std.testing.expect(membershipChangesRequiredWithLocalPolicy(
        &.{ 1, 2 },
        &.{3},
        1,
        &.{ 1, 2 },
        &.{},
        true,
    ));
    // Naming an existing learner only as a voter requires promotion.
    try std.testing.expect(membershipChangesRequiredWithLocalPolicy(
        &.{ 1, 2 },
        &.{3},
        1,
        &.{ 1, 2, 3 },
        &.{},
        true,
    ));
    // A retiring local voter can be removed once another desired voter exists.
    try std.testing.expect(membershipChangesRequiredWithLocalPolicy(
        &.{ 1, 2 },
        &.{},
        1,
        &.{2},
        &.{},
        false,
    ));
}

test "desired membership rejects ambiguous topology" {
    try std.testing.expectError(
        error.DuplicateTopologyNodeId,
        (DesiredMembership{ .voters = &.{ 1, 1 }, .learners = &.{} }).validate(),
    );
    try std.testing.expectError(
        error.InvalidTopologyNodeId,
        (DesiredMembership{ .voters = &.{0}, .learners = &.{} }).validate(),
    );
}

test "intent fingerprint is topology order independent" {
    const lhs: PlacementIntent = .{
        .record = .{ .group_id = 51, .replica_id = 1, .local_node_id = 1 },
        .peer_node_ids = &.{ 1, 2, 3 },
        .learner_node_ids = &.{ 4, 5 },
    };
    const rhs: PlacementIntent = .{
        .record = lhs.record,
        .peer_node_ids = &.{ 3, 1, 2 },
        .learner_node_ids = &.{ 5, 4 },
    };
    try std.testing.expectEqual(hashIntent(lhs), hashIntent(rhs));
}

test "membership-only reconciliation exposes normal waiting state" {
    var host = host_mod.Host.init(std.testing.allocator, .{ .local_node_id = 1 }, .{});
    defer host.deinit();
    var provider = MemoryPlacementProvider.init(std.testing.allocator);
    defer provider.deinit();
    var reconciler = Reconciler{
        .alloc = std.testing.allocator,
        .host = &host,
        .provider = provider.provider(),
    };
    defer reconciler.deinit();

    const intents = [_]PlacementIntent{.{
        .record = .{ .group_id = 52, .replica_id = 1, .local_node_id = 1 },
        .peer_node_ids = &.{1},
    }};
    const result = try reconciler.reconcileMembershipOnly(&intents);
    try std.testing.expectEqual(@as(usize, 1), result.membership_waiting_for_replica);
    try std.testing.expectEqual(
        MembershipConvergence.waiting_for_replica,
        reconciler.membershipStatus(52).?,
    );
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
                .artifact_backup_id = try std.testing.allocator.dupe(u8, "snap-52"),
                .location = try std.testing.allocator.dupe(u8, "file:///tmp/backups"),
                .snapshot_path = try std.testing.allocator.dupe(u8, "snap-52/groups/52"),
                .connection = try std.testing.allocator.dupe(u8, "backup-store"),
                .artifact_size_bytes = 4096,
                .artifact_sha256 = try std.testing.allocator.dupe(u8, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"),
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
    try std.testing.expect(cloned.record.backup_restore_bootstrap.?.connection.ptr != original.record.backup_restore_bootstrap.?.connection.ptr);
    try std.testing.expect(cloned.record.backup_restore_bootstrap.?.artifact_sha256.ptr != original.record.backup_restore_bootstrap.?.artifact_sha256.ptr);
    try std.testing.expect(cloned.peer_node_ids.ptr != original.peer_node_ids.ptr);
    try std.testing.expect(cloned.learner_node_ids.ptr != original.learner_node_ids.ptr);
}
