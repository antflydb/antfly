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

pub const ReconcileFailurePhase = enum {
    admission_prepare,
    admission_classify,
    catalog_admission,
    live_install,
    routes,
    membership,
    catalog_retirement,
    live_retirement,
};

pub const ReconcileFailureClassification = enum {
    retryable,
    permanent,
    restart_required,
};

pub const ReconcileFailure = struct {
    group_id: u64,
    phase: ReconcileFailurePhase,
    classification: ReconcileFailureClassification,
    err: anyerror,
    attempts: u32 = 1,
    next_retry_ns: u64 = 0,
};

pub const max_reconcile_failure_details: usize = 32;

const FailureAggregate = struct {
    failure: ReconcileFailure,
    detail_index: ?u8,
};

/// Per-round exact failure accounting. Operator-facing details remain bounded,
/// while the scratch map prevents repeated failures for an omitted group from
/// inflating the unique-group metric. Capacity is reserved during planning so
/// recording a failure never allocates inside a publication phase.
const FailureAccumulator = struct {
    groups: std.AutoHashMapUnmanaged(u64, FailureAggregate) = .empty,

    fn deinit(self: *FailureAccumulator, alloc: std.mem.Allocator) void {
        self.groups.deinit(alloc);
        self.* = undefined;
    }

    fn record(
        self: *FailureAccumulator,
        result: *ReconcileResult,
        failure: ReconcileFailure,
    ) void {
        result.placement_incomplete = result.placement_incomplete or failure.phase != .routes;
        const entry = self.groups.getOrPutAssumeCapacity(failure.group_id);
        if (!entry.found_existing) {
            const detail_index: ?u8 = if (result.failure_detail_count < result.failure_details.len)
                @intCast(result.failure_detail_count)
            else
                null;
            entry.value_ptr.* = .{ .failure = failure, .detail_index = detail_index };
            result.failed_groups += 1;
            if (detail_index) |index| {
                result.failure_details[index] = failure;
                result.failure_detail_count += 1;
            }
            result.omitted_failure_details = result.failed_groups - result.failure_detail_count;
            return;
        }

        const existing_domain = failureDomainForPhase(entry.value_ptr.failure.phase);
        const new_domain = failureDomainForPhase(failure.phase);
        if (existing_domain == new_domain or
            failureDomainPriority(new_domain) < failureDomainPriority(existing_domain))
        {
            entry.value_ptr.failure = failure;
            if (entry.value_ptr.detail_index) |index| result.failure_details[index] = failure;
        }
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
    route_retrying_groups: usize = 0,
    failed_groups: usize = 0,
    placement_incomplete: bool = false,
    omitted_failure_details: usize = 0,
    failure_detail_count: usize = 0,
    failure_details: [max_reconcile_failure_details]ReconcileFailure = undefined,

    pub fn hasFailures(self: *const ReconcileResult) bool {
        return self.failed_groups != 0;
    }

    pub fn hasPlacementFailures(self: *const ReconcileResult) bool {
        return self.placement_incomplete;
    }

    pub fn failures(self: *const ReconcileResult) []const ReconcileFailure {
        return self.failure_details[0..self.failure_detail_count];
    }

    fn recordFailure(
        self: *ReconcileResult,
        accumulator: *FailureAccumulator,
        failure: ReconcileFailure,
    ) void {
        accumulator.record(self, failure);
    }

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

pub const RouteConvergence = enum {
    converged,
    retrying,
};

pub const RouteConvergenceDiagnostics = struct {
    status: RouteConvergence,
    attempts: u32 = 0,
    next_retry_ns: u64 = 0,
};

const RouteRetryState = struct {
    attempts: u32 = 0,
    next_retry_ns: u64 = 0,
};

const FailureRetryState = struct {
    intent_hash: ?u64 = null,
    phase: ReconcileFailurePhase,
    classification: ReconcileFailureClassification,
    err: anyerror,
    attempts: u32,
    next_retry_ns: u64,
};

pub const FailureDomain = enum {
    admission,
    routes,
    membership,
    retirement,
};

const FailureKey = struct {
    group_id: u64,
    domain: FailureDomain,
};

fn failureDomainForPhase(phase: ReconcileFailurePhase) FailureDomain {
    return switch (phase) {
        .admission_prepare, .admission_classify, .catalog_admission, .live_install => .admission,
        .routes => .routes,
        .membership => .membership,
        .catalog_retirement, .live_retirement => .retirement,
    };
}

fn failureDomainPriority(domain: FailureDomain) u8 {
    return switch (domain) {
        .admission => 0,
        .retirement => 1,
        .membership => 2,
        .routes => 3,
    };
}

fn failureDomainBit(domain: FailureDomain) u8 {
    return @as(u8, 1) << @intCast(@intFromEnum(domain));
}

fn failureFromRetryState(group_id: u64, state: FailureRetryState) ReconcileFailure {
    return .{
        .group_id = group_id,
        .phase = state.phase,
        .classification = state.classification,
        .err = state.err,
        .attempts = state.attempts,
        .next_retry_ns = state.next_retry_ns,
    };
}

const route_retry_initial_ns: u64 = 50 * std.time.ns_per_ms;
const route_retry_max_ns: u64 = 5 * std.time.ns_per_s;
const policy_recheck_ns: u64 = std.time.ns_per_s;
const max_live_retry_groups_per_round: usize = 64;

const PreparedEnsure = struct {
    intent_index: usize,
    intent_hash: u64,
    prepare_bootstrap: bool,
    catalog_upsert_index: ?usize = null,
    replica: ?host_mod.PreparedReplica = null,
    prepare_error: ?anyerror = null,
    failure_phase: ReconcileFailurePhase = .admission_prepare,
    failure_published: bool = false,
    restart_policy_blocked: bool = false,
};

const PreparedRoutePeer = struct {
    node_id: u64,
    prepared: ?host_mod.PreparedPeerEndpoints = null,
    prepare_error: ?anyerror = null,
};

const PreparedRouteGroup = struct {
    intent_index: usize,
    first_peer: usize,
    peer_count: usize,
};

const PreparedPolicyValidation = struct {
    intent_index: usize,
    prepared: ?host_mod.PreparedAdmissionValidation = null,
    prepare_error: ?anyerror = null,
};

/// Fallible endpoint discovery and descriptor construction are completed in
/// `prepare` without the Raft owner lock. `commit` only publishes immutable
/// results after the caller re-enters its serialized runtime phase.
pub const PreparedLiveConvergence = struct {
    owner: *Reconciler,
    intents: []const PlacementIntent,
    route_groups: []PreparedRouteGroup,
    route_peers: []PreparedRoutePeer,
    policies: []PreparedPolicyValidation,
    failure_accumulator: FailureAccumulator,
    prepared: bool = false,
    committed: bool = false,

    pub fn deinit(self: *PreparedLiveConvergence) void {
        for (self.route_peers) |*peer| {
            if (peer.prepared) |*prepared| prepared.deinit(self.owner.alloc);
        }
        for (self.policies) |*policy| {
            if (policy.prepared) |*prepared| prepared.deinit(self.owner.alloc);
        }
        self.owner.alloc.free(self.route_groups);
        self.owner.alloc.free(self.route_peers);
        self.owner.alloc.free(self.policies);
        self.failure_accumulator.deinit(self.owner.alloc);
        self.* = undefined;
    }

    pub fn prepare(self: *PreparedLiveConvergence) !void {
        if (self.prepared or self.committed) return error.InvalidReconcilePhase;
        for (self.route_groups) |group| {
            const intent = self.intents[group.intent_index];
            for (self.route_peers[group.first_peer..][0..group.peer_count]) |*peer| {
                peer.prepared = self.owner.host.preparePeerEndpoints(
                    intent.record.group_id,
                    peer.node_id,
                ) catch |err| failed: {
                    peer.prepare_error = err;
                    break :failed null;
                };
            }
        }
        for (self.policies) |*policy| {
            policy.prepared = self.owner.host.prepareReplicaAdmissionValidation(
                self.intents[policy.intent_index].record,
            ) catch |err| failed: {
                policy.prepare_error = err;
                break :failed null;
            };
        }
        self.prepared = true;
    }

    pub fn commit(self: *PreparedLiveConvergence, result: *ReconcileResult) !void {
        if (!self.prepared or self.committed) return error.InvalidReconcilePhase;
        const failures = &self.failure_accumulator;
        for (self.route_groups) |group| {
            const intent = self.intents[group.intent_index];
            // Route publication belongs to the live-runtime phase. A catalog
            // admission whose runtime install failed must remain retrying; in
            // particular, a zero-peer intent must not be reported converged
            // merely because there were no endpoints to publish.
            if (!self.owner.host.hasReplica(intent.record.group_id)) {
                self.owner.recordRouteRefreshResult(intent.record.group_id, error.UnknownGroup);
                continue;
            }
            var refreshed: usize = 0;
            var last_error: ?anyerror = null;
            for (self.route_peers[group.first_peer..][0..group.peer_count]) |*peer| {
                if (peer.prepare_error) |err| {
                    last_error = err;
                    continue;
                }
                const prepared = if (peer.prepared) |*value| value else {
                    last_error = error.InvalidReconcilePhase;
                    continue;
                };
                const endpoint_count = self.owner.host.commitPreparedPeerEndpoints(prepared) catch |err| {
                    last_error = err;
                    continue;
                };
                if (endpoint_count == 0 and last_error == null) last_error = error.NoPeerEndpoints;
                refreshed += endpoint_count;
            }
            result.refreshed_peers += refreshed;
            self.owner.recordRouteRefreshResult(intent.record.group_id, last_error);
            if (last_error) |err| {
                self.owner.recordGroupFailure(result, failures, intent.record.group_id, .routes, err);
            } else {
                self.owner.clearGroupFailure(intent.record.group_id, .routes);
            }
        }
        for (self.policies) |*policy| {
            const intent = self.intents[policy.intent_index];
            if (policy.prepare_error) |err| {
                self.owner.recordPolicyValidationResult(intent.record.group_id, null, err);
                self.owner.recordGroupFailure(result, failures, intent.record.group_id, .admission_classify, err);
                continue;
            }
            const prepared = if (policy.prepared) |*value| value else continue;
            const conflict = self.owner.host.commitReplicaAdmissionValidation(prepared) catch |err| {
                self.owner.recordPolicyValidationResult(intent.record.group_id, null, err);
                self.owner.recordGroupFailure(result, failures, intent.record.group_id, .admission_classify, err);
                continue;
            };
            self.owner.recordPolicyValidationResult(intent.record.group_id, conflict != null, null);
            self.owner.clearGroupFailure(intent.record.group_id, .admission_classify);
        }
        result.admission_blocked = self.owner.countAdmissionBlocked(self.intents);
        result.route_retrying_groups = self.owner.countRouteRetrying(self.intents);
        self.committed = true;
    }
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
    deferred_failures: []ReconcileFailure,
    catalog_upserts: []catalog.ReplicaRecord,
    catalog_upsert_hashes: []u64,
    catalog_upsert_enabled: []bool,
    catalog_upsert_count: usize,
    catalog_token: ?catalog.ReplicaCatalogToken,
    catalog_commit_token: ?catalog.ReplicaCatalogToken = null,
    prepared_retirement_catalog: ?catalog.PreparedReplicaCatalogBatch = null,
    live: PreparedLiveConvergence,
    /// Set only after the retirement half of the catalog transition is
    /// durable (or when there are no retirements). Callers that stage sidecar
    /// cleanup use this to distinguish a safely retained old owner from a
    /// committed removal whose live teardown still needs recovery.
    catalog_commit_complete: bool = false,
    durability_complete: bool = false,
    classification_complete: bool = false,
    admission_commit_complete: bool = false,
    live_commit_complete: bool = false,
    retirement_prepare_complete: bool = false,
    retirement_commit_attempted: bool = false,
    retirement_suppressed: bool = false,
    result: ReconcileResult = .{},
    committed: bool = false,
    aborted: bool = false,

    pub fn deinit(self: *PreparedReconcile) void {
        if (self.prepared_retirement_catalog) |*prepared| prepared.deinit();
        self.live.deinit();
        for (self.ensures) |*entry| {
            if (entry.replica) |*replica| replica.deinit(self.owner.alloc);
        }
        self.owner.alloc.free(self.ensures);
        self.owner.alloc.free(self.removals);
        self.owner.alloc.free(self.deferred_failures);
        self.owner.alloc.free(self.catalog_upserts);
        self.owner.alloc.free(self.catalog_upsert_hashes);
        self.owner.alloc.free(self.catalog_upsert_enabled);
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
        for (self.ensures) |*entry| {
            const record = self.intents[entry.intent_index].record;
            entry.replica = self.owner.host.prepareReplicaUnpublished(record, entry.prepare_bootstrap) catch |err| {
                entry.prepare_error = err;
                if (entry.catalog_upsert_index) |catalog_index|
                    self.catalog_upsert_enabled[catalog_index] = false;
                continue;
            };
        }
        try self.live.prepare();
        self.durability_complete = true;
    }

    fn compactCatalogUpserts(self: *PreparedReconcile) void {
        // Classification can reject additional candidates only after the
        // caller re-enters the runtime owner. Compact exactly once at commit.
        var write_index: usize = 0;
        for (self.catalog_upserts, self.catalog_upsert_hashes, self.catalog_upsert_enabled) |record, intent_hash, enabled| {
            if (!enabled) continue;
            self.catalog_upserts[write_index] = record;
            self.catalog_upsert_hashes[write_index] = intent_hash;
            write_index += 1;
        }
        self.catalog_upsert_count = write_index;
    }

    pub fn notePreparationFailure(self: *PreparedReconcile, err: anyerror) void {
        // A fatal plan-level preparation error (for example, invalid phase)
        // must not strand any bootstrap status in `preparing`. Per-replica
        // errors normally flow through commit so unrelated groups can still
        // publish; if the whole plan cannot reach commit, close every pending
        // bootstrap with its most specific error.
        for (self.ensures) |entry| {
            if (!entry.prepare_bootstrap) continue;
            self.owner.host.noteReplicaBootstrapPreparationFailure(
                self.intents[entry.intent_index].record,
                entry.prepare_error orelse err,
            );
        }
    }

    /// Revalidates prepared descriptors against live runtime state. This is a
    /// short owner-lock phase and performs no filesystem work.
    pub fn classifyAdmissions(self: *PreparedReconcile) !void {
        if (!self.durability_complete or self.classification_complete or self.committed or self.aborted)
            return error.InvalidReconcilePhase;
        // Descriptor construction is deliberately independent from live
        // runtime state. Classify it only after the caller re-enters the Raft
        // owner's serialization lock.
        for (self.ensures) |*entry| {
            if (entry.prepare_error != null) {
                continue;
            }
            const intent = self.intents[entry.intent_index];
            const prepared = if (entry.replica) |*replica| replica else return error.InvalidReconcilePhase;
            const conflict = self.owner.host.classifyPreparedReplicaAdmission(intent.record, prepared) catch |err| {
                entry.prepare_error = err;
                entry.failure_phase = .admission_classify;
                if (entry.catalog_upsert_index) |catalog_index|
                    self.catalog_upsert_enabled[catalog_index] = false;
                continue;
            };
            if (conflict) |value| switch (value) {
                .runtime_policy => entry.restart_policy_blocked = true,
                .local_node_id => {
                    entry.prepare_error = error.LocalNodeIdMismatch;
                    entry.failure_phase = .admission_classify;
                    if (entry.catalog_upsert_index) |catalog_index|
                        self.catalog_upsert_enabled[catalog_index] = false;
                },
            };
        }
        self.compactCatalogUpserts();
        self.classification_complete = true;
    }

    /// Makes admissions durable without holding the Raft owner lock. The
    /// returned catalog revision is the opaque fence for retirement; callers
    /// must never refresh it from the catalog between phases.
    pub fn commitAdmissionsDurable(self: *PreparedReconcile) !void {
        if (!self.classification_complete or self.admission_commit_complete or self.committed or self.aborted)
            return error.InvalidReconcilePhase;
        self.catalog_commit_token = self.catalog_token;
        if (self.catalog_upsert_count > 0) {
            self.catalog_commit_token = try self.owner.host.commitReplicaCatalog(
                self.catalog_token,
                self.catalog_upserts[0..self.catalog_upsert_count],
                &.{},
            );
        }
        self.admission_commit_complete = true;
    }

    /// Publishes prepared replicas and advances live convergence. This is a
    /// short owner-lock phase and performs no catalog I/O.
    pub fn publishLive(self: *PreparedReconcile) !void {
        if (!self.admission_commit_complete or self.live_commit_complete or self.committed or self.aborted)
            return error.InvalidReconcilePhase;
        for (self.deferred_failures) |failure|
            self.result.recordFailure(&self.live.failure_accumulator, failure);
        for (self.catalog_upserts[0..self.catalog_upsert_count]) |record|
            self.owner.clearGroupFailure(record.group_id, .catalog_admission);
        for (self.ensures) |*entry| {
            const intent = self.intents[entry.intent_index];
            if (entry.prepare_error != null) {
                self.publishAdmissionEntryFailure(entry);
                continue;
            }
            if (entry.restart_policy_blocked) {
                // The desired record is durable and will take effect on the
                // next replica restart. Remember its fingerprint so an
                // unchanged restart-scoped policy does not fsync the catalog
                // on every control round.
                self.owner.last_intent_hashes.putAssumeCapacity(
                    intent.record.group_id,
                    entry.intent_hash,
                );
                self.owner.clearAdmissionFailure(intent.record.group_id);
                continue;
            }
            const prepared = if (entry.replica) |*replica| replica else return error.InvalidReconcilePhase;
            _ = self.owner.host.installPreparedReplica(intent.record, prepared) catch |err| {
                self.owner.recordIntentFailure(&self.result, &self.live.failure_accumulator, intent.record.group_id, entry.intent_hash, .live_install, err);
                continue;
            };
            self.result.ensured += 1;
            self.owner.last_intent_hashes.putAssumeCapacity(
                intent.record.group_id,
                entry.intent_hash,
            );
            self.owner.clearAdmissionFailure(intent.record.group_id);
        }
        try self.live.commit(&self.result);
        for (self.intents) |intent| {
            if (!self.owner.host.hasReplica(intent.record.group_id)) continue;
            const outcome = self.owner.reconcileRaftMembership(intent) catch |err| {
                self.owner.recordGroupFailure(&self.result, &self.live.failure_accumulator, intent.record.group_id, .membership, err);
                continue;
            };
            self.owner.membership_convergence.putAssumeCapacity(intent.record.group_id, outcome);
            self.result.recordMembership(outcome);
            self.owner.clearGroupFailure(intent.record.group_id, .membership);
        }
        self.live_commit_complete = true;
    }

    /// Completes allocation, catalog cloning, serialization, and file fsync
    /// before a caller enters its placement-publication barrier. The prepared
    /// image remains fenced by the exact post-admission token until commit.
    pub fn prepareRetirementsDurable(self: *PreparedReconcile) !void {
        if (!self.live_commit_complete or self.retirement_prepare_complete or
            self.retirement_commit_attempted or self.committed or self.aborted)
            return error.InvalidReconcilePhase;
        if (self.removals.len > 0 and !self.retirement_suppressed) {
            self.prepared_retirement_catalog = try self.owner.host.prepareReplicaCatalog(
                self.catalog_commit_token,
                &.{},
                self.removals,
            );
        }
        self.retirement_prepare_complete = true;
    }

    /// Atomically publishes a prepared retirement catalog image. Production
    /// metadata callers execute this inside the same short barrier used by
    /// authoritative placement commits, immediately before live removal.
    /// Retirement uses the exact admission token, so any intervening catalog
    /// writer rejects this stale plan instead of deleting newly admitted
    /// ownership.
    ///
    /// Replacement dependencies belong to authoritative desired state: the
    /// controller retains an old placement in `retiring` until its replacement
    /// is ready. Once a group is absent from desired state, an unrelated local
    /// admission failure must not block its cleanup.
    pub fn commitRetirementsDurable(self: *PreparedReconcile) !void {
        if (!self.retirement_prepare_complete or self.retirement_commit_attempted or self.committed or self.aborted)
            return error.InvalidReconcilePhase;
        self.retirement_commit_attempted = true;
        if (self.removals.len > 0 and self.retirement_suppressed) {
            return;
        } else if (self.removals.len > 0) {
            if (self.prepared_retirement_catalog) |*prepared|
                self.catalog_commit_token = try prepared.commit();
            self.catalog_commit_complete = true;
        } else {
            self.catalog_commit_complete = true;
        }
    }

    /// Records an admission durability error while the caller owns the live
    /// runtime lock. The durable phase itself deliberately does not mutate
    /// owner diagnostics.
    pub fn noteAdmissionDurabilityFailure(self: *PreparedReconcile, err: anyerror) void {
        // Per-entry preparation and classification errors were deliberately
        // isolated from the catalog batch. Publish them first so an unrelated
        // batch failure cannot leave their retry or bootstrap state hidden.
        for (self.ensures) |*entry| self.publishAdmissionEntryFailure(entry);
        for (
            self.catalog_upserts[0..self.catalog_upsert_count],
            self.catalog_upsert_hashes[0..self.catalog_upsert_count],
        ) |record, intent_hash| {
            self.owner.recordGroupFailureImpl(
                &self.result,
                &self.live.failure_accumulator,
                record.group_id,
                intent_hash,
                .catalog_admission,
                err,
            );
            for (self.ensures) |entry| {
                if (!entry.prepare_bootstrap or
                    self.intents[entry.intent_index].record.group_id != record.group_id) continue;
                self.owner.host.noteReplicaBootstrapPreparationFailure(
                    self.intents[entry.intent_index].record,
                    err,
                );
                break;
            }
        }
    }

    fn publishAdmissionEntryFailure(
        self: *PreparedReconcile,
        entry: *PreparedEnsure,
    ) void {
        if (entry.failure_published) return;
        const err = entry.prepare_error orelse return;
        const intent = self.intents[entry.intent_index];
        if (entry.prepare_bootstrap)
            self.owner.host.noteReplicaBootstrapPreparationFailure(intent.record, err);
        self.owner.recordIntentFailure(
            &self.result,
            &self.live.failure_accumulator,
            intent.record.group_id,
            entry.intent_hash,
            entry.failure_phase,
            err,
        );
        entry.failure_published = true;
    }

    pub fn noteRetirementDurabilityFailure(self: *PreparedReconcile, err: anyerror) void {
        for (self.removals) |group_id|
            self.owner.recordGroupFailure(&self.result, &self.live.failure_accumulator, group_id, .catalog_retirement, err);
    }

    /// Prevents a stale desired-state snapshot from retiring ownership after
    /// its admissions are already durable. The next epoch will converge the
    /// admitted record, while old ownership remains available in the interim.
    pub fn suppressRetirements(self: *PreparedReconcile) void {
        self.retirement_suppressed = true;
    }

    /// Removes durably retired groups from the live runtime and publishes the
    /// final result. This is a short owner-lock phase.
    pub fn finish(self: *PreparedReconcile) !ReconcileResult {
        if (!self.retirement_commit_attempted or self.committed or self.aborted)
            return error.InvalidReconcilePhase;
        if (!self.catalog_commit_complete) {
            for (self.removals) |group_id| {
                self.owner.recordGroupFailure(
                    &self.result,
                    &self.live.failure_accumulator,
                    group_id,
                    .catalog_retirement,
                    error.RetirementPrerequisitePending,
                );
            }
            self.result.admission_blocked = self.owner.countAdmissionBlocked(self.intents);
            self.result.route_retrying_groups = self.owner.countRouteRetrying(self.intents);
            self.owner.host.metrics.reconcile_rounds += 1;
            self.owner.publishConvergenceMetrics(self.result);
            self.committed = true;
            return self.result;
        }

        for (self.removals) |group_id| {
            if (self.owner.host.hasReplica(group_id)) {
                self.owner.host.removePreparedReplica(group_id) catch |err| {
                    self.owner.recordGroupFailure(&self.result, &self.live.failure_accumulator, group_id, .live_retirement, err);
                    continue;
                };
            }
            _ = self.owner.last_intent_hashes.remove(group_id);
            _ = self.owner.membership_convergence.remove(group_id);
            _ = self.owner.route_convergence.remove(group_id);
            _ = self.owner.route_retries.remove(group_id);
            _ = self.owner.policy_retries.remove(group_id);
            self.owner.clearAllGroupFailures(group_id);
            self.result.removed += 1;
        }
        self.owner.host.metrics.reconcile_rounds += 1;
        self.owner.publishConvergenceMetrics(self.result);
        self.committed = true;
        return self.result;
    }

    /// Convenience transaction for callers that already provide their own
    /// serialization. Production runtimes use the individual stages so disk
    /// durability is never performed under the Raft owner lock.
    pub fn commit(self: *PreparedReconcile) !ReconcileResult {
        try self.classifyAdmissions();
        self.commitAdmissionsDurable() catch |err| {
            self.noteAdmissionDurabilityFailure(err);
            return err;
        };
        try self.publishLive();
        try self.prepareRetirementsDurable();
        self.commitRetirementsDurable() catch |err| {
            self.noteRetirementDurabilityFailure(err);
            return err;
        };
        return try self.finish();
    }

    /// Discards unpublished descriptors after the caller observes a newer
    /// desired-state epoch. Durable catalog state is untouched until commit.
    pub fn abortDurable(self: *PreparedReconcile) !void {
        if (!self.durability_complete or self.admission_commit_complete or self.committed or self.aborted)
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
    route_convergence: std.AutoHashMapUnmanaged(u64, RouteConvergence) = .empty,
    route_retries: std.AutoHashMapUnmanaged(u64, RouteRetryState) = .empty,
    policy_retries: std.AutoHashMapUnmanaged(u64, RouteRetryState) = .empty,
    failure_retries: std.AutoHashMapUnmanaged(FailureKey, FailureRetryState) = .empty,
    failure_group_domains: std.AutoHashMapUnmanaged(u64, u8) = .empty,
    live_retry_cursor: usize = 0,

    pub fn deinit(self: *Reconciler) void {
        self.last_intent_hashes.deinit(self.alloc);
        self.last_intent_hashes = .empty;
        self.membership_convergence.deinit(self.alloc);
        self.membership_convergence = .empty;
        self.route_convergence.deinit(self.alloc);
        self.route_convergence = .empty;
        self.route_retries.deinit(self.alloc);
        self.route_retries = .empty;
        self.policy_retries.deinit(self.alloc);
        self.policy_retries = .empty;
        self.failure_retries.deinit(self.alloc);
        self.failure_retries = .empty;
        self.failure_group_domains.deinit(self.alloc);
        self.failure_group_domains = .empty;
    }

    pub fn membershipStatus(self: *const Reconciler, group_id: u64) ?MembershipConvergence {
        return self.membership_convergence.get(group_id);
    }

    pub fn routeStatus(self: *const Reconciler, group_id: u64) ?RouteConvergence {
        return self.route_convergence.get(group_id);
    }

    pub fn routeDiagnostics(self: *const Reconciler, group_id: u64) ?RouteConvergenceDiagnostics {
        const status = self.route_convergence.get(group_id) orelse return null;
        const retry = self.route_retries.get(group_id) orelse RouteRetryState{};
        return .{
            .status = status,
            .attempts = retry.attempts,
            .next_retry_ns = retry.next_retry_ns,
        };
    }

    pub fn failureDiagnostics(self: *const Reconciler, group_id: u64) ?ReconcileFailure {
        const priority = [_]FailureDomain{ .admission, .retirement, .membership, .routes };
        for (priority) |domain| {
            const state = self.failure_retries.get(.{ .group_id = group_id, .domain = domain }) orelse continue;
            return failureFromRetryState(group_id, state);
        }
        return null;
    }

    pub fn failureDiagnosticsForDomain(
        self: *const Reconciler,
        group_id: u64,
        domain: FailureDomain,
    ) ?ReconcileFailure {
        const state = self.failure_retries.get(.{ .group_id = group_id, .domain = domain }) orelse return null;
        return failureFromRetryState(group_id, state);
    }

    pub fn prepareLiveConvergence(
        self: *Reconciler,
        intents: []const PlacementIntent,
    ) !PreparedLiveConvergence {
        try self.ensureConvergenceCapacity(intents.len);
        return try self.buildLiveConvergence(intents, null);
    }

    fn ensureConvergenceCapacity(self: *Reconciler, intent_count: usize) !void {
        const convergence_capacity = std.math.cast(
            u32,
            @as(usize, self.membership_convergence.count()) +| intent_count,
        ) orelse return error.TooManyPlacementIntents;
        try self.membership_convergence.ensureTotalCapacity(self.alloc, convergence_capacity);
        const route_capacity = std.math.cast(
            u32,
            @as(usize, self.route_convergence.count()) +| intent_count,
        ) orelse return error.TooManyPlacementIntents;
        try self.route_convergence.ensureTotalCapacity(self.alloc, route_capacity);
        const route_retry_capacity = std.math.cast(
            u32,
            @as(usize, self.route_retries.count()) +| intent_count,
        ) orelse return error.TooManyPlacementIntents;
        try self.route_retries.ensureTotalCapacity(self.alloc, route_retry_capacity);
        const policy_retry_capacity = std.math.cast(
            u32,
            @as(usize, self.policy_retries.count()) +| intent_count,
        ) orelse return error.TooManyPlacementIntents;
        try self.policy_retries.ensureTotalCapacity(self.alloc, policy_retry_capacity);
        const failure_slots = std.math.mul(usize, intent_count, @typeInfo(FailureDomain).@"enum".fields.len) catch
            return error.TooManyPlacementIntents;
        const failure_retry_capacity = std.math.cast(
            u32,
            std.math.add(usize, self.failure_retries.count(), failure_slots) catch
                return error.TooManyPlacementIntents,
        ) orelse return error.TooManyPlacementIntents;
        try self.failure_retries.ensureTotalCapacity(self.alloc, failure_retry_capacity);
        const failed_group_capacity = std.math.cast(
            u32,
            @as(usize, self.failure_group_domains.count()) +| intent_count,
        ) orelse return error.TooManyPlacementIntents;
        try self.failure_group_domains.ensureTotalCapacity(self.alloc, failed_group_capacity);
    }

    fn appendRouteGroup(
        self: *Reconciler,
        route_groups: *std.ArrayListUnmanaged(PreparedRouteGroup),
        route_peers: *std.ArrayListUnmanaged(PreparedRoutePeer),
        intents: []const PlacementIntent,
        intent_index: usize,
    ) !void {
        const intent = intents[intent_index];
        const first_peer = route_peers.items.len;
        for (intent.peer_node_ids) |node_id| {
            if (node_id == self.host.cfg.local_node_id) continue;
            try route_peers.append(self.alloc, .{ .node_id = node_id });
        }
        for (intent.learner_node_ids) |node_id| {
            if (node_id == self.host.cfg.local_node_id or containsNodeId(intent.peer_node_ids, node_id)) continue;
            try route_peers.append(self.alloc, .{ .node_id = node_id });
        }
        try route_groups.append(self.alloc, .{
            .intent_index = intent_index,
            .first_peer = first_peer,
            .peer_count = route_peers.items.len - first_peer,
        });
    }

    fn buildLiveConvergence(
        self: *Reconciler,
        intents: []const PlacementIntent,
        force_routes: ?[]const bool,
    ) !PreparedLiveConvergence {
        if (force_routes) |flags| {
            if (flags.len != intents.len) return error.InvalidReconcilePlan;
        }
        var route_groups = std.ArrayListUnmanaged(PreparedRouteGroup).empty;
        errdefer route_groups.deinit(self.alloc);
        var route_peers = std.ArrayListUnmanaged(PreparedRoutePeer).empty;
        errdefer route_peers.deinit(self.alloc);
        var policies = std.ArrayListUnmanaged(PreparedPolicyValidation).empty;
        errdefer policies.deinit(self.alloc);
        const now_ns = platform_time.monotonicNs();

        var scanned: usize = 0;
        var scheduled: usize = 0;
        var cursor = if (intents.len == 0) 0 else self.live_retry_cursor % intents.len;
        var next_cursor = cursor;
        while (scanned < intents.len) : (scanned += 1) {
            const intent_index = cursor;
            cursor = (cursor + 1) % intents.len;
            const intent = intents[intent_index];
            const forced = if (force_routes) |flags| flags[intent_index] else false;
            const route_due = !forced and
                self.route_convergence.get(intent.record.group_id) == .retrying and
                now_ns >= (self.route_retries.get(intent.record.group_id) orelse RouteRetryState{}).next_retry_ns;
            const has_policy_conflict = self.host.replicaAdmissionConflict(intent.record.group_id) != null;
            const policy_due = has_policy_conflict and
                now_ns >= (self.policy_retries.get(intent.record.group_id) orelse RouteRetryState{}).next_retry_ns;
            if (!forced and !route_due and !policy_due) continue;
            if (scheduled == max_live_retry_groups_per_round) {
                // A newly admitted group must remain visibly retrying when its
                // forced route publication is deferred by the shared budget.
                if (forced) self.deferRouteRefresh(intent.record.group_id);
                continue;
            }
            if (forced or route_due)
                try self.appendRouteGroup(&route_groups, &route_peers, intents, intent_index);
            if (policy_due) try policies.append(self.alloc, .{ .intent_index = intent_index });
            scheduled += 1;
            next_cursor = cursor;
        }
        if (intents.len > 0) self.live_retry_cursor = next_cursor;
        const owned_route_groups = try route_groups.toOwnedSlice(self.alloc);
        errdefer self.alloc.free(owned_route_groups);
        const owned_route_peers = try route_peers.toOwnedSlice(self.alloc);
        errdefer self.alloc.free(owned_route_peers);
        const owned_policies = try policies.toOwnedSlice(self.alloc);
        errdefer self.alloc.free(owned_policies);
        var failure_accumulator: FailureAccumulator = .{};
        errdefer failure_accumulator.deinit(self.alloc);
        const failure_capacity = std.math.cast(u32, intents.len) orelse
            return error.TooManyPlacementIntents;
        try failure_accumulator.groups.ensureTotalCapacity(self.alloc, failure_capacity);
        return .{
            .owner = self,
            .intents = intents,
            .route_groups = owned_route_groups,
            .route_peers = owned_route_peers,
            .policies = owned_policies,
            .failure_accumulator = failure_accumulator,
        };
    }

    /// Returns true while an unchanged admission is intentionally parked.
    /// Retryable failures honor their deadline; permanent and restart-scoped
    /// failures wake only when the intent fingerprint changes.
    fn admissionAttemptDeferred(
        self: *Reconciler,
        group_id: u64,
        intent_hash: u64,
        now_ns: u64,
    ) bool {
        const key = FailureKey{ .group_id = group_id, .domain = .admission };
        const state = self.failure_retries.get(key) orelse return false;
        if (state.intent_hash == null or state.intent_hash.? != intent_hash) {
            _ = self.removeFailure(key);
            return false;
        }
        return switch (state.classification) {
            .retryable => now_ns < state.next_retry_ns,
            .permanent, .restart_required => true,
        };
    }

    fn retirementAttemptDeferred(self: *const Reconciler, group_id: u64, now_ns: u64) bool {
        const state = self.failure_retries.get(.{ .group_id = group_id, .domain = .retirement }) orelse return false;
        return switch (state.classification) {
            .retryable => now_ns < state.next_retry_ns,
            .permanent, .restart_required => true,
        };
    }

    fn clearObsoleteRetirementFailure(self: *Reconciler, group_id: u64) void {
        _ = self.removeFailure(.{ .group_id = group_id, .domain = .retirement });
    }

    /// Desired-scoped convergence state is mark/swept every authoritative
    /// planning generation. Retirement failures remain until their durable or
    /// live owner disappears; failed never-admitted intents are removed as
    /// soon as they leave desired state.
    fn pruneConvergenceState(
        self: *Reconciler,
        desired: *const std.AutoHashMapUnmanaged(u64, void),
        removals: *const std.AutoHashMapUnmanaged(u64, void),
    ) !void {
        var stale = std.ArrayListUnmanaged(u64).empty;
        defer stale.deinit(self.alloc);

        var membership_it = self.membership_convergence.keyIterator();
        while (membership_it.next()) |group_id| {
            if (!desired.contains(group_id.*)) try stale.append(self.alloc, group_id.*);
        }
        for (stale.items) |group_id| _ = self.membership_convergence.remove(group_id);
        stale.clearRetainingCapacity();

        var route_it = self.route_convergence.keyIterator();
        while (route_it.next()) |group_id| {
            if (!desired.contains(group_id.*)) try stale.append(self.alloc, group_id.*);
        }
        for (stale.items) |group_id| {
            _ = self.route_convergence.remove(group_id);
            _ = self.route_retries.remove(group_id);
        }
        stale.clearRetainingCapacity();

        var policy_it = self.policy_retries.keyIterator();
        while (policy_it.next()) |group_id| {
            if (!desired.contains(group_id.*)) try stale.append(self.alloc, group_id.*);
        }
        for (stale.items) |group_id| _ = self.policy_retries.remove(group_id);
        stale.clearRetainingCapacity();

        var stale_failures = std.ArrayListUnmanaged(FailureKey).empty;
        defer stale_failures.deinit(self.alloc);
        var failure_it = self.failure_retries.iterator();
        while (failure_it.next()) |entry| {
            const key = entry.key_ptr.*;
            if (desired.contains(key.group_id)) continue;
            if (removals.contains(key.group_id) and key.domain == .retirement) continue;
            try stale_failures.append(self.alloc, key);
        }
        for (stale_failures.items) |key| _ = self.removeFailure(key);
    }

    pub fn prepare(self: *Reconciler) !PreparedReconcile {
        const intents = try self.provider.listLocalIntents(self.alloc, self.host.cfg.local_node_id);
        errdefer freeIntentSlice(self.alloc, intents);
        const existing = try self.host.listGroupIds(self.alloc);
        defer self.alloc.free(existing);
        var catalog_snapshot = try self.host.snapshotReplicaCatalog(self.alloc);
        defer if (catalog_snapshot) |*snapshot| snapshot.deinit(self.alloc);

        var desired_group_ids = std.AutoHashMapUnmanaged(u64, void).empty;
        defer desired_group_ids.deinit(self.alloc);
        var catalog_record_indexes = std.AutoHashMapUnmanaged(u64, usize).empty;
        defer catalog_record_indexes.deinit(self.alloc);
        if (catalog_snapshot) |snapshot| {
            try catalog_record_indexes.ensureTotalCapacity(
                self.alloc,
                std.math.cast(u32, snapshot.records.len) orelse return error.TooManyReplicaCatalogRecords,
            );
            for (snapshot.records, 0..) |record, index| {
                catalog_record_indexes.putAssumeCapacity(record.group_id, index);
            }
        }
        var ensures = std.ArrayListUnmanaged(PreparedEnsure).empty;
        errdefer ensures.deinit(self.alloc);
        var removals = std.ArrayListUnmanaged(u64).empty;
        errdefer removals.deinit(self.alloc);
        var deferred_failures = std.ArrayListUnmanaged(ReconcileFailure).empty;
        errdefer deferred_failures.deinit(self.alloc);
        var removal_group_ids = std.AutoHashMapUnmanaged(u64, void).empty;
        defer removal_group_ids.deinit(self.alloc);
        var catalog_upserts = std.ArrayListUnmanaged(catalog.ReplicaRecord).empty;
        errdefer catalog_upserts.deinit(self.alloc);
        var catalog_upsert_hashes = std.ArrayListUnmanaged(u64).empty;
        errdefer catalog_upsert_hashes.deinit(self.alloc);
        const now_ns = platform_time.monotonicNs();

        for (intents, 0..) |intent, intent_index| {
            try intent.desiredMembership().validate();
            try desired_group_ids.put(self.alloc, intent.record.group_id, {});
            self.clearObsoleteRetirementFailure(intent.record.group_id);
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
            const admission_deferred = self.admissionAttemptDeferred(
                intent.record.group_id,
                intent_hash,
                now_ns,
            );
            if (admission_deferred) {
                if (self.failureDiagnosticsForDomain(intent.record.group_id, .admission)) |failure|
                    try deferred_failures.append(self.alloc, failure);
            }
            const should_apply = !admission_deferred and (lifecycle_incomplete or
                stored_hash == null or
                stored_hash.? != intent_hash);

            // A live replica can repair catalog drift without rebuilding its
            // descriptor. Any admission whose failure is backed off waits for
            // its deadline before touching the catalog again.
            const catalog_upsert_index: ?usize = if (catalog_snapshot) |snapshot| catalog: {
                const stored_record = if (catalog_record_indexes.get(intent.record.group_id)) |index|
                    snapshot.records[index]
                else
                    null;
                if (stored_record != null and catalog.eqlReplicaRecord(stored_record.?, intent.record))
                    break :catalog null;
                if (admission_deferred) break :catalog null;
                const index = catalog_upserts.items.len;
                try catalog_upserts.append(self.alloc, intent.record);
                try catalog_upsert_hashes.append(self.alloc, intent_hash);
                break :catalog index;
            } else null;

            if (should_apply) {
                try ensures.append(self.alloc, .{
                    .intent_index = intent_index,
                    .intent_hash = intent_hash,
                    .prepare_bootstrap = !self.host.hasReplica(intent.record.group_id) and
                        intent.record.backup_restore_bootstrap != null,
                    .catalog_upsert_index = catalog_upsert_index,
                });
            }
        }
        for (existing) |group_id| {
            if (desired_group_ids.contains(group_id)) continue;
            if (try removal_group_ids.fetchPut(self.alloc, group_id, {}) != null) continue;
            if (self.retirementAttemptDeferred(group_id, now_ns)) {
                if (self.failureDiagnosticsForDomain(group_id, .retirement)) |failure|
                    try deferred_failures.append(self.alloc, failure);
                continue;
            }
            try removals.append(self.alloc, group_id);
        }
        if (catalog_snapshot) |snapshot| for (snapshot.records) |record| {
            if (desired_group_ids.contains(record.group_id)) continue;
            if (try removal_group_ids.fetchPut(self.alloc, record.group_id, {}) != null) continue;
            if (self.retirementAttemptDeferred(record.group_id, now_ns)) {
                if (self.failureDiagnosticsForDomain(record.group_id, .retirement)) |failure|
                    try deferred_failures.append(self.alloc, failure);
                continue;
            }
            try removals.append(self.alloc, record.group_id);
        };
        try self.pruneConvergenceState(&desired_group_ids, &removal_group_ids);
        const owned_ensures = try ensures.toOwnedSlice(self.alloc);
        errdefer self.alloc.free(owned_ensures);
        const owned_removals = try removals.toOwnedSlice(self.alloc);
        errdefer self.alloc.free(owned_removals);
        const owned_deferred_failures = try deferred_failures.toOwnedSlice(self.alloc);
        errdefer self.alloc.free(owned_deferred_failures);
        const owned_catalog_upserts = try catalog_upserts.toOwnedSlice(self.alloc);
        errdefer self.alloc.free(owned_catalog_upserts);
        const owned_catalog_upsert_hashes = try catalog_upsert_hashes.toOwnedSlice(self.alloc);
        errdefer self.alloc.free(owned_catalog_upsert_hashes);
        const catalog_upsert_enabled = try self.alloc.alloc(bool, owned_catalog_upserts.len);
        errdefer self.alloc.free(catalog_upsert_enabled);
        @memset(catalog_upsert_enabled, true);
        const catalog_token = if (catalog_snapshot) |snapshot| snapshot.token else null;
        try self.ensureConvergenceCapacity(intents.len +| owned_removals.len);
        const intent_hash_capacity = std.math.cast(
            u32,
            @as(usize, self.last_intent_hashes.count()) +| owned_ensures.len,
        ) orelse return error.TooManyPlacementIntents;
        try self.last_intent_hashes.ensureTotalCapacity(self.alloc, intent_hash_capacity);
        const force_routes = try self.alloc.alloc(bool, intents.len);
        defer self.alloc.free(force_routes);
        @memset(force_routes, false);
        for (owned_ensures) |entry| force_routes[entry.intent_index] = true;
        var live = try self.buildLiveConvergence(intents, force_routes);
        errdefer live.deinit();
        const failure_capacity = std.math.cast(
            u32,
            intents.len +| owned_removals.len +| owned_deferred_failures.len,
        ) orelse return error.TooManyPlacementIntents;
        try live.failure_accumulator.groups.ensureTotalCapacity(self.alloc, failure_capacity);
        return .{
            .owner = self,
            .intents = intents,
            .ensures = owned_ensures,
            .removals = owned_removals,
            .deferred_failures = owned_deferred_failures,
            .catalog_upserts = owned_catalog_upserts,
            .catalog_upsert_hashes = owned_catalog_upsert_hashes,
            .catalog_upsert_enabled = catalog_upsert_enabled,
            .catalog_upsert_count = owned_catalog_upserts.len,
            .catalog_token = catalog_token,
            .live = live,
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

    fn recordGroupFailure(
        self: *Reconciler,
        result: *ReconcileResult,
        failures: *FailureAccumulator,
        group_id: u64,
        phase: ReconcileFailurePhase,
        err: anyerror,
    ) void {
        self.recordGroupFailureImpl(result, failures, group_id, null, phase, err);
    }

    fn recordIntentFailure(
        self: *Reconciler,
        result: *ReconcileResult,
        failures: *FailureAccumulator,
        group_id: u64,
        intent_hash: u64,
        phase: ReconcileFailurePhase,
        err: anyerror,
    ) void {
        self.recordGroupFailureImpl(result, failures, group_id, intent_hash, phase, err);
    }

    fn recordGroupFailureImpl(
        self: *Reconciler,
        result: *ReconcileResult,
        failures: *FailureAccumulator,
        group_id: u64,
        intent_hash: ?u64,
        phase: ReconcileFailurePhase,
        err: anyerror,
    ) void {
        const classification = classifyReconcileFailure(phase, err);
        const key = FailureKey{ .group_id = group_id, .domain = failureDomainForPhase(phase) };
        const previous = self.failure_retries.get(key);
        const attempts: u32 = if (previous) |state|
            if (state.intent_hash == intent_hash and state.phase == phase and state.err == err)
                state.attempts +| 1
            else
                1
        else
            1;
        const next_retry_ns = if (classification == .retryable)
            platform_time.monotonicNs() +| routeRetryDelayNs(group_id ^ @intFromEnum(phase), attempts)
        else
            0;
        self.failure_retries.putAssumeCapacity(key, .{
            .intent_hash = intent_hash,
            .phase = phase,
            .classification = classification,
            .err = err,
            .attempts = attempts,
            .next_retry_ns = next_retry_ns,
        });
        const domain_entry = self.failure_group_domains.getOrPutAssumeCapacity(group_id);
        if (!domain_entry.found_existing) domain_entry.value_ptr.* = 0;
        domain_entry.value_ptr.* |= failureDomainBit(key.domain);
        result.recordFailure(failures, .{
            .group_id = group_id,
            .phase = phase,
            .classification = classification,
            .err = err,
            .attempts = attempts,
            .next_retry_ns = next_retry_ns,
        });
        if (attempts == 1 or (attempts & (attempts - 1)) == 0) {
            std.log.warn(
                "raft reconciliation deferred group_id={d} phase={s} classification={s} attempts={d} err={s}",
                .{ group_id, @tagName(phase), @tagName(classification), attempts, @errorName(err) },
            );
        }
    }

    fn clearGroupFailure(self: *Reconciler, group_id: u64, phase: ReconcileFailurePhase) void {
        const key = FailureKey{ .group_id = group_id, .domain = failureDomainForPhase(phase) };
        const state = self.failure_retries.get(key) orelse return;
        if (state.phase == phase) _ = self.removeFailure(key);
    }

    fn clearAdmissionFailure(self: *Reconciler, group_id: u64) void {
        _ = self.removeFailure(.{ .group_id = group_id, .domain = .admission });
    }

    fn clearAllGroupFailures(self: *Reconciler, group_id: u64) void {
        inline for (@typeInfo(FailureDomain).@"enum".fields) |field| {
            _ = self.removeFailure(.{
                .group_id = group_id,
                .domain = @enumFromInt(field.value),
            });
        }
    }

    fn removeFailure(self: *Reconciler, key: FailureKey) bool {
        if (!self.failure_retries.remove(key)) return false;
        const domains = self.failure_group_domains.getPtr(key.group_id) orelse unreachable;
        domains.* &= ~failureDomainBit(key.domain);
        if (domains.* == 0) _ = self.failure_group_domains.remove(key.group_id);
        return true;
    }

    /// Advances only mutable Raft membership. It performs no descriptor
    /// construction, catalog write, restore, or filesystem work, so callers
    /// can safely run it for an unchanged metadata epoch until ConfState
    /// converges through leader changes and joint-consensus boundaries.
    pub fn reconcileMembershipOnly(
        self: *Reconciler,
        intents: []const PlacementIntent,
    ) !ReconcileResult {
        var result: ReconcileResult = .{};
        try self.reconcileMembershipInto(intents, &result);
        self.finishLiveConvergence(result);
        return result;
    }

    /// Adds membership observations to an existing live-convergence result.
    /// This deliberately does not publish metrics: a runtime round can combine
    /// route, admission-policy, and membership work and expose one coherent
    /// observation after every phase has completed.
    pub fn reconcileMembershipInto(
        self: *Reconciler,
        intents: []const PlacementIntent,
        result: *ReconcileResult,
    ) !void {
        try self.ensureConvergenceCapacity(intents.len);
        for (intents) |intent| {
            try intent.desiredMembership().validate();
            const outcome = try self.reconcileRaftMembership(intent);
            self.membership_convergence.putAssumeCapacity(intent.record.group_id, outcome);
            result.recordMembership(outcome);
        }
        result.admission_blocked = self.countAdmissionBlocked(intents);
        result.route_retrying_groups = self.countRouteRetrying(intents);
    }

    /// Publishes one completed topology-stable round. Persistent retry state,
    /// rather than only work attempted in this round, owns failure gauges so a
    /// backoff interval cannot make outstanding convergence debt look healthy.
    pub fn finishLiveConvergence(
        self: *Reconciler,
        result: ReconcileResult,
    ) void {
        self.host.metrics.reconcile_rounds += 1;
        self.publishConvergenceMetrics(result);
    }

    fn recordRouteRefreshResult(self: *Reconciler, group_id: u64, last_error: ?anyerror) void {
        const status: RouteConvergence = if (last_error == null) .converged else .retrying;
        const previous = self.route_convergence.get(group_id);
        self.route_convergence.putAssumeCapacity(group_id, status);
        switch (status) {
            .converged => _ = self.route_retries.remove(group_id),
            .retrying => {
                const previous_retry = self.route_retries.get(group_id) orelse RouteRetryState{};
                const attempts = previous_retry.attempts +| 1;
                self.route_retries.putAssumeCapacity(group_id, .{
                    .attempts = attempts,
                    .next_retry_ns = platform_time.monotonicNs() +|
                        routeRetryDelayNs(group_id, attempts),
                });
            },
        }
        if (previous == null or previous.? != status) switch (status) {
            .converged => std.log.info(
                "raft peer routes converged group_id={d}",
                .{group_id},
            ),
            .retrying => std.log.warn(
                "raft peer route convergence deferred group_id={d} err={s}",
                .{ group_id, @errorName(last_error.?) },
            ),
        };
    }

    fn deferRouteRefresh(self: *Reconciler, group_id: u64) void {
        self.route_convergence.putAssumeCapacity(group_id, .retrying);
        if (!self.route_retries.contains(group_id)) {
            self.route_retries.putAssumeCapacity(group_id, .{ .next_retry_ns = 0 });
        }
    }

    fn recordPolicyValidationResult(
        self: *Reconciler,
        group_id: u64,
        conflict_remains: ?bool,
        last_error: ?anyerror,
    ) void {
        const now_ns = platform_time.monotonicNs();
        if (last_error) |err| {
            const previous = self.policy_retries.get(group_id) orelse RouteRetryState{};
            const attempts = previous.attempts +| 1;
            self.policy_retries.putAssumeCapacity(group_id, .{
                .attempts = attempts,
                .next_retry_ns = now_ns +| routeRetryDelayNs(group_id ^ 0x706f6c696379, attempts),
            });
            if (attempts == 1 or (attempts & (attempts - 1)) == 0) {
                std.log.warn(
                    "replica admission conflict revalidation deferred group_id={d} attempts={d} err={s}",
                    .{ group_id, attempts, @errorName(err) },
                );
            }
            return;
        }
        if (conflict_remains orelse true) {
            self.policy_retries.putAssumeCapacity(group_id, .{
                .next_retry_ns = now_ns +| policy_recheck_ns,
            });
        } else {
            _ = self.policy_retries.remove(group_id);
        }
    }

    fn countRouteRetrying(self: *const Reconciler, intents: []const PlacementIntent) usize {
        var count: usize = 0;
        for (intents) |intent| {
            if (self.route_convergence.get(intent.record.group_id) == .retrying) count += 1;
        }
        return count;
    }

    fn countAdmissionBlocked(self: *const Reconciler, intents: []const PlacementIntent) usize {
        var count: usize = 0;
        for (intents) |intent| {
            if (self.host.replicaAdmissionConflict(intent.record.group_id) != null) count += 1;
        }
        return count;
    }

    fn publishConvergenceMetrics(self: *Reconciler, result: ReconcileResult) void {
        self.host.metrics.membership_converged = result.membership_converged;
        self.host.metrics.membership_waiting_for_replica = result.membership_waiting_for_replica;
        self.host.metrics.membership_waiting_for_leader = result.membership_waiting_for_leader;
        self.host.metrics.membership_waiting_for_local_voter = result.membership_waiting_for_local_voter;
        self.host.metrics.membership_waiting_for_pending_change = result.membership_waiting_for_pending_change;
        self.host.metrics.membership_waiting_for_policy = result.membership_waiting_for_policy;
        self.host.metrics.route_retrying_groups = result.route_retrying_groups;
        self.host.metrics.reconcile_failed_groups = self.countFailedGroups();
    }

    fn countFailedGroups(self: *const Reconciler) usize {
        return self.failure_group_domains.count();
    }

    fn reconcileRaftMembership(self: *Reconciler, intent: PlacementIntent) !MembershipConvergence {
        const status = self.host.raftStatus(intent.record.group_id) orelse return .waiting_for_replica;
        // Convergence is an observed ConfState property and does not require
        // local leadership. Leadership is required only when an action remains.
        if (status.conf_state.voters_outgoing.len == 0 and
            !membershipChangesRequiredWithLocalPolicy(
                status.conf_state.voters,
                status.conf_state.learners,
                intent.record.local_node_id,
                intent.peer_node_ids,
                intent.learner_node_ids,
                intent.serving_state != .retiring,
            )) return .converged;
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

fn routeRetryDelayNs(group_id: u64, attempts: u32) u64 {
    const shift: u6 = @intCast(@min(attempts -| 1, 6));
    const exponential = @min(route_retry_initial_ns << shift, route_retry_max_ns);
    var seed = [2]u64{ group_id, attempts };
    const jitter_window = @max(@as(u64, 1), exponential / 4);
    const jitter = std.hash.Wyhash.hash(0x726f7574655f7274, std.mem.asBytes(&seed)) % jitter_window;
    return @min(exponential +| jitter, route_retry_max_ns);
}

fn classifyReconcileFailure(
    phase: ReconcileFailurePhase,
    err: anyerror,
) ReconcileFailureClassification {
    _ = phase;
    return switch (err) {
        error.ReplicaRuntimePolicyMismatch => .restart_required,
        error.ReplicaAdmissionRejected,
        error.LocalNodeIdMismatch,
        error.InvalidSnapshotSourceNodeId,
        error.InvalidSnapshotId,
        error.MissingReplicaDescriptorFactory,
        error.MissingBackupRestoreBootstrapHandler,
        => .permanent,
        else => .retryable,
    };
}

test "reconcile result bounds failure details while preserving failure count" {
    var accumulator: FailureAccumulator = .{};
    defer accumulator.deinit(std.testing.allocator);
    try accumulator.groups.ensureTotalCapacity(
        std.testing.allocator,
        max_reconcile_failure_details + 1,
    );
    var result: ReconcileResult = .{};
    for (0..max_reconcile_failure_details + 1) |index| {
        result.recordFailure(&accumulator, .{
            .group_id = @intCast(index + 1),
            .phase = if (index == max_reconcile_failure_details) .routes else .live_install,
            .classification = .retryable,
            .err = error.InjectedFailure,
        });
    }
    try std.testing.expectEqual(max_reconcile_failure_details + 1, result.failed_groups);
    try std.testing.expectEqual(max_reconcile_failure_details, result.failures().len);
    try std.testing.expectEqual(@as(usize, 1), result.omitted_failure_details);

    result.recordFailure(&accumulator, .{
        .group_id = 1,
        .phase = .routes,
        .classification = .retryable,
        .err = error.UpdatedFailure,
    });
    try std.testing.expectEqual(max_reconcile_failure_details + 1, result.failed_groups);
    try std.testing.expectEqual(ReconcileFailurePhase.live_install, result.failures()[0].phase);
    try std.testing.expectEqual(@as(usize, 1), result.omitted_failure_details);

    // The omitted group is tracked in the exact scratch map even though it has
    // no retained detail slot. Repeated domains must not turn the unique-group
    // metric or omitted count into event counters.
    result.recordFailure(&accumulator, .{
        .group_id = max_reconcile_failure_details + 1,
        .phase = .routes,
        .classification = .retryable,
        .err = error.UpdatedFailure,
    });
    result.recordFailure(&accumulator, .{
        .group_id = max_reconcile_failure_details + 1,
        .phase = .catalog_retirement,
        .classification = .retryable,
        .err = error.UpdatedFailure,
    });
    try std.testing.expectEqual(max_reconcile_failure_details + 1, result.failed_groups);
    try std.testing.expectEqual(@as(usize, 1), result.omitted_failure_details);
    try std.testing.expectEqual(
        ReconcileFailurePhase.catalog_retirement,
        accumulator.groups.get(max_reconcile_failure_details + 1).?.failure.phase,
    );
}

test "reconcile retry domains preserve admission backoff and primary diagnostics" {
    var host = host_mod.Host.init(std.testing.allocator, .{ .local_node_id = 1 }, .{});
    defer host.deinit();
    var provider = MemoryPlacementProvider.init(std.testing.allocator);
    defer provider.deinit();
    var owner = Reconciler{
        .alloc = std.testing.allocator,
        .host = &host,
        .provider = provider.provider(),
    };
    defer owner.deinit();
    try owner.ensureConvergenceCapacity(1);

    var accumulator: FailureAccumulator = .{};
    defer accumulator.deinit(std.testing.allocator);
    try accumulator.groups.ensureTotalCapacity(std.testing.allocator, 1);
    var result: ReconcileResult = .{};
    owner.recordIntentFailure(
        &result,
        &accumulator,
        71,
        0xabc,
        .admission_prepare,
        error.InjectedPrepareFailure,
    );
    const admission_deadline = owner.failureDiagnosticsForDomain(71, .admission).?.next_retry_ns;
    try std.testing.expect(admission_deadline != 0);
    owner.recordGroupFailure(&result, &accumulator, 71, .routes, error.NoPeerEndpoints);
    owner.recordGroupFailure(&result, &accumulator, 71, .membership, error.NotLeader);

    owner.finishLiveConvergence(result);
    try std.testing.expectEqual(@as(usize, 1), host.metrics.reconcile_failed_groups);
    // Backoff-only rounds attempt no failing operation, but the state gauge
    // must retain the outstanding unique group.
    owner.finishLiveConvergence(.{});
    try std.testing.expectEqual(@as(usize, 1), host.metrics.reconcile_failed_groups);

    try std.testing.expectEqual(
        ReconcileFailurePhase.admission_prepare,
        owner.failureDiagnostics(71).?.phase,
    );
    try std.testing.expectEqual(
        ReconcileFailurePhase.routes,
        owner.failureDiagnosticsForDomain(71, .routes).?.phase,
    );
    try std.testing.expectEqual(
        ReconcileFailurePhase.membership,
        owner.failureDiagnosticsForDomain(71, .membership).?.phase,
    );
    try std.testing.expectEqual(
        admission_deadline,
        owner.failureDiagnosticsForDomain(71, .admission).?.next_retry_ns,
    );
    try std.testing.expectEqual(ReconcileFailurePhase.admission_prepare, result.failures()[0].phase);

    owner.clearGroupFailure(71, .routes);
    try std.testing.expect(owner.failureDiagnosticsForDomain(71, .routes) == null);
    try std.testing.expect(owner.failureDiagnosticsForDomain(71, .admission) != null);

    owner.clearAdmissionFailure(71);
    owner.clearGroupFailure(71, .membership);
    owner.finishLiveConvergence(.{});
    try std.testing.expectEqual(@as(usize, 0), host.metrics.reconcile_failed_groups);
}

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
    fail_group_id: u64 = 0,

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
        if (record.group_id == self.fail_group_id) return error.InjectedDescriptorPreparationFailure;
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

test "reconciler removes catalog-only replicas absent from desired state" {
    var replica_catalog = catalog.MemoryReplicaCatalog.init(std.testing.allocator);
    defer replica_catalog.deinit();
    const catalog_iface = replica_catalog.catalog();
    try catalog_iface.upsertReplica(.{
        .group_id = 506,
        .replica_id = 1,
        .local_node_id = 1,
    });
    const admitted_revision = catalog_iface.revision();

    var host = host_mod.Host.init(std.testing.allocator, .{ .local_node_id = 1 }, .{
        .replica_catalog = catalog_iface,
    });
    defer host.deinit();
    var provider = MemoryPlacementProvider.init(std.testing.allocator);
    defer provider.deinit();
    var owner = Reconciler{
        .alloc = std.testing.allocator,
        .host = &host,
        .provider = provider.provider(),
    };
    defer owner.deinit();

    const result = try owner.reconcileOnce();
    try std.testing.expectEqual(@as(usize, 1), result.removed);
    try std.testing.expect(!catalog_iface.containsReplica(506));
    try std.testing.expectEqual(admitted_revision + 1, catalog_iface.revision());
    try std.testing.expect(!host.hasReplica(506));
}

test "reconciler repairs catalog drift independently of live admission" {
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
            .group_id = 508,
            .replica_id = 1,
            .local_node_id = 1,
            .metadata_version = 2,
        },
    }});
    var owner = Reconciler{
        .alloc = std.testing.allocator,
        .host = &host,
        .provider = provider.provider(),
    };
    defer owner.deinit();

    const admitted = try owner.reconcileOnce();
    try std.testing.expectEqual(@as(usize, 1), admitted.ensured);
    try catalog_iface.upsertReplica(.{
        .group_id = 508,
        .replica_id = 1,
        .local_node_id = 1,
        .metadata_version = 1,
    });
    const drift_revision = catalog_iface.revision();

    const repaired = try owner.reconcileOnce();
    try std.testing.expectEqual(@as(usize, 0), repaired.ensured);
    try std.testing.expectEqual(drift_revision + 1, catalog_iface.revision());
    const records = try catalog_iface.listReplicas(std.testing.allocator);
    defer catalog.freeReplicaRecords(std.testing.allocator, records);
    try std.testing.expectEqual(@as(usize, 1), records.len);
    try std.testing.expectEqual(@as(u64, 2), records[0].metadata_version);
}

test "reconcile plan preparation is allocation-failure safe" {
    const Runner = struct {
        fn run(alloc: std.mem.Allocator) !void {
            var store_a = raft_engine.core.MemoryStorage.init(alloc);
            defer store_a.deinit();
            var store_b = raft_engine.core.MemoryStorage.init(alloc);
            defer store_b.deinit();
            var factory = StagedReconcileTestFactory{
                .alloc = alloc,
                .stores = .{ &store_a, &store_b },
            };
            var replica_catalog = catalog.MemoryReplicaCatalog.init(alloc);
            defer replica_catalog.deinit();
            var host = host_mod.Host.init(alloc, .{ .local_node_id = 1 }, .{
                .descriptor_factory = factory.iface(),
                .replica_catalog = replica_catalog.catalog(),
            });
            defer host.deinit();
            var provider = MemoryPlacementProvider.init(alloc);
            defer provider.deinit();
            try provider.replaceAll(&.{.{
                .record = .{ .group_id = 509, .replica_id = 1, .local_node_id = 1 },
            }});
            var owner = Reconciler{
                .alloc = alloc,
                .host = &host,
                .provider = provider.provider(),
            };
            defer owner.deinit();

            var prepared = try owner.prepare();
            defer prepared.deinit();
        }
    };

    try std.testing.checkAllAllocationFailures(std.testing.allocator, Runner.run, .{});
}

test "reconciler isolates live install failures and retries without replaying durable admission" {
    const FailingTransport = struct {
        fail_group_id: u64,

        fn iface(self: *@This()) raft_engine.runtime.transport_iface.Transport {
            return .{
                .ptr = self,
                .vtable = &.{
                    .send_messages = sendMessages,
                    .serve_group = serveGroup,
                    .unserve_group = unserveGroup,
                },
            };
        }

        fn sendMessages(_: *anyopaque, _: u64, _: []const raft_engine.core.Message) !void {}

        fn serveGroup(
            ptr: *anyopaque,
            group_id: u64,
            _: raft_engine.runtime.transport_iface.TransportReceiver,
        ) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (group_id == self.fail_group_id) return error.InjectedInstallFailure;
        }

        fn unserveGroup(_: *anyopaque, _: u64) !void {}
    };

    var store_a = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_a.deinit();
    var store_b = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_b.deinit();
    var factory = StagedReconcileTestFactory{
        .alloc = std.testing.allocator,
        .stores = .{ &store_a, &store_b },
    };
    var transport = FailingTransport{ .fail_group_id = 511 };
    var replica_catalog = catalog.MemoryReplicaCatalog.init(std.testing.allocator);
    defer replica_catalog.deinit();
    const catalog_iface = replica_catalog.catalog();
    try catalog_iface.upsertReplica(.{
        .group_id = 512,
        .replica_id = 3,
        .local_node_id = 1,
    });
    var host = host_mod.Host.init(std.testing.allocator, .{ .local_node_id = 1 }, .{
        .descriptor_factory = factory.iface(),
        .replica_catalog = catalog_iface,
        .runtime_hooks = .{ .transport = transport.iface() },
    });
    defer host.deinit();
    var provider = MemoryPlacementProvider.init(std.testing.allocator);
    defer provider.deinit();
    try provider.replaceAll(&.{
        .{ .record = .{ .group_id = 510, .replica_id = 1, .local_node_id = 1 } },
        .{ .record = .{ .group_id = 511, .replica_id = 2, .local_node_id = 1 } },
    });
    var owner = Reconciler{
        .alloc = std.testing.allocator,
        .host = &host,
        .provider = provider.provider(),
    };
    defer owner.deinit();

    const failed = try owner.reconcileOnce();
    try std.testing.expect(failed.hasFailures());
    try std.testing.expectEqual(@as(usize, 1), failed.failed_groups);
    try std.testing.expectEqual(@as(u64, 511), failed.failures()[0].group_id);
    try std.testing.expectEqual(ReconcileFailurePhase.live_install, failed.failures()[0].phase);
    try std.testing.expectEqual(error.InjectedInstallFailure, failed.failures()[0].err);
    try std.testing.expect(host.hasReplica(510));
    try std.testing.expect(!host.hasReplica(511));
    try std.testing.expectEqual(RouteConvergence.converged, owner.routeStatus(510).?);
    try std.testing.expectEqual(RouteConvergence.retrying, owner.routeStatus(511).?);
    try std.testing.expect(catalog_iface.containsReplica(510));
    try std.testing.expect(catalog_iface.containsReplica(511));
    // Unrelated retirement follows authoritative desired state and is not
    // held hostage by this group's failed local install.
    try std.testing.expect(!catalog_iface.containsReplica(512));
    try std.testing.expect(owner.failureDiagnostics(511).?.next_retry_ns != 0);
    const durable_revision = catalog_iface.revision();

    transport.fail_group_id = 0;
    const deferred = try owner.reconcileOnce();
    try std.testing.expectEqual(@as(usize, 0), deferred.ensured);
    try std.testing.expect(!host.hasReplica(511));
    try std.testing.expectEqual(durable_revision, catalog_iface.revision());

    owner.failure_retries.getPtr(.{ .group_id = 511, .domain = .admission }).?.next_retry_ns = 0;
    const recovered = try owner.reconcileOnce();
    try std.testing.expectEqual(@as(usize, 1), recovered.ensured);
    try std.testing.expect(host.hasReplica(511));
    try std.testing.expectEqual(RouteConvergence.converged, owner.routeStatus(511).?);
    try std.testing.expectEqual(durable_revision, catalog_iface.revision());
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

test "catalog admission failure preserves isolated preparation diagnostics" {
    var store_a = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_a.deinit();
    var store_b = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_b.deinit();
    var factory = StagedReconcileTestFactory{
        .alloc = std.testing.allocator,
        .stores = .{ &store_a, &store_b },
        .fail_group_id = 541,
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
    try provider.replaceAll(&.{
        .{ .record = .{ .group_id = 541, .replica_id = 1, .local_node_id = 1 } },
        .{ .record = .{ .group_id = 542, .replica_id = 2, .local_node_id = 1 } },
    });
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
    try prepared.classifyAdmissions();
    // An external catalog writer invalidates the surviving group-542 batch.
    try catalog_iface.upsertReplica(.{ .group_id = 599, .replica_id = 9, .local_node_id = 1 });
    var commit_error: ?anyerror = null;
    prepared.commitAdmissionsDurable() catch |err| {
        commit_error = err;
    };
    try std.testing.expectEqual(error.ReplicaCatalogRevisionChanged, commit_error.?);
    prepared.noteAdmissionDurabilityFailure(commit_error.?);

    const preparation = owner.failureDiagnosticsForDomain(541, .admission).?;
    try std.testing.expectEqual(ReconcileFailurePhase.admission_prepare, preparation.phase);
    try std.testing.expectEqual(error.InjectedDescriptorPreparationFailure, preparation.err);
    const catalog_failure = owner.failureDiagnosticsForDomain(542, .admission).?;
    try std.testing.expectEqual(ReconcileFailurePhase.catalog_admission, catalog_failure.phase);
    try std.testing.expectEqual(error.ReplicaCatalogRevisionChanged, catalog_failure.err);
    try std.testing.expectEqual(@as(usize, 2), prepared.result.failed_groups);
}

test "retirement uses the exact post-admission catalog fence" {
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
    try catalog_iface.upsertReplica(.{ .group_id = 521, .replica_id = 1, .local_node_id = 1 });
    var host = host_mod.Host.init(std.testing.allocator, .{ .local_node_id = 1 }, .{
        .descriptor_factory = factory.iface(),
        .replica_catalog = catalog_iface,
    });
    defer host.deinit();
    _ = try host.ensureReplica(.{ .group_id = 521, .replica_id = 1, .local_node_id = 1 });
    var provider = MemoryPlacementProvider.init(std.testing.allocator);
    defer provider.deinit();
    try provider.replaceAll(&.{.{
        .record = .{ .group_id = 522, .replica_id = 2, .local_node_id = 1 },
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
    try prepared.classifyAdmissions();
    try prepared.commitAdmissionsDurable();
    try std.testing.expect(catalog_iface.containsReplica(522));

    try prepared.publishLive();
    try prepared.prepareRetirementsDurable();
    // This write lands after the expensive retirement image is prepared but
    // before its atomic publication. Commit must still retain the exact
    // post-admission fence rather than clobbering the concurrent admission.
    try catalog_iface.upsertReplica(.{ .group_id = 523, .replica_id = 3, .local_node_id = 1 });
    try std.testing.expectError(
        error.ReplicaCatalogRevisionChanged,
        prepared.commitRetirementsDurable(),
    );
    try std.testing.expect(catalog_iface.containsReplica(521));
    try std.testing.expect(catalog_iface.containsReplica(522));
    try std.testing.expect(catalog_iface.containsReplica(523));
    try std.testing.expect(host.hasReplica(521));
}

test "removal-only plans retain their snapshot catalog fence" {
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
    try catalog_iface.upsertReplica(.{ .group_id = 531, .replica_id = 1, .local_node_id = 1 });
    var host = host_mod.Host.init(std.testing.allocator, .{ .local_node_id = 1 }, .{
        .descriptor_factory = factory.iface(),
        .replica_catalog = catalog_iface,
    });
    defer host.deinit();
    _ = try host.ensureReplica(.{ .group_id = 531, .replica_id = 1, .local_node_id = 1 });
    var provider = MemoryPlacementProvider.init(std.testing.allocator);
    defer provider.deinit();
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
    try prepared.classifyAdmissions();
    try prepared.commitAdmissionsDurable();
    try prepared.publishLive();
    try prepared.prepareRetirementsDurable();
    try catalog_iface.upsertReplica(.{ .group_id = 532, .replica_id = 2, .local_node_id = 1 });
    try std.testing.expectError(
        error.ReplicaCatalogRevisionChanged,
        prepared.commitRetirementsDurable(),
    );
    try std.testing.expect(catalog_iface.containsReplica(531));
    try std.testing.expect(catalog_iface.containsReplica(532));
    try std.testing.expect(host.hasReplica(531));

    prepared.noteRetirementDurabilityFailure(error.ReplicaCatalogRevisionChanged);
    {
        var backed_off = try owner.prepare();
        defer backed_off.deinit();
        try std.testing.expectEqual(@as(usize, 1), backed_off.deferred_failures.len);
        try std.testing.expectEqual(@as(u64, 531), backed_off.deferred_failures[0].group_id);
        for (backed_off.removals) |group_id| try std.testing.expect(group_id != 531);
    }

    // Reintroducing a desired group invalidates obsolete retirement debt.
    try provider.replaceAll(&.{.{
        .record = .{ .group_id = 531, .replica_id = 1, .local_node_id = 1 },
    }});
    {
        var desired_again = try owner.prepare();
        defer desired_again.deinit();
    }
    try std.testing.expect(owner.failureDiagnostics(531) == null);
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
    var blocked_prepared = try owner.prepare();
    defer blocked_prepared.deinit();
    blocked_prepared.beginPreparation();
    try blocked_prepared.prepareDurable();
    // The blocking descriptor-build phase is pure: only owner-locked commit
    // may inspect or mutate live admission state.
    try std.testing.expect(host.replicaAdmissionConflict(504) == null);
    const blocked = try blocked_prepared.commit();
    try std.testing.expectEqual(@as(usize, 0), blocked.ensured);
    try std.testing.expectEqual(@as(usize, 1), blocked.admission_blocked);
    try std.testing.expectEqual(host_mod.HostedReplicaStatus.active, host.status(504));
    const conflict = host.replicaAdmissionConflict(504) orelse
        return error.ExpectedReplicaAdmissionConflict;
    switch (conflict) {
        .runtime_policy => |policy_conflict| try std.testing.expectEqual(
            raft_engine.runtime.group.ReplicaRuntimePolicyField.election_tick,
            policy_conflict.field,
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
    try std.testing.expectEqual(@as(usize, 1), unchanged.admission_blocked);
    try std.testing.expectEqual(durable_revision, catalog_iface.revision());

    // A policy rollback is independent of placement metadata. Stable rounds
    // must clear the restart requirement without rebuilding the catalog.
    factory.election_tick = 5;
    owner.policy_retries.getPtr(504).?.next_retry_ns = 0;
    const revalidated = try owner.reconcileOnce();
    try std.testing.expectEqual(@as(usize, 0), revalidated.admission_blocked);
    try std.testing.expect(host.replicaAdmissionConflict(504) == null);
    try std.testing.expectEqual(durable_revision, catalog_iface.revision());
}

test "route convergence retries without replaying durable admission" {
    const Resolver = struct {
        fail: bool = true,

        fn iface(self: *@This()) peer_resolver.PeerResolver {
            return .{
                .ptr = self,
                .vtable = &.{ .resolve_group_peer = resolve },
            };
        }

        fn resolve(
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            _: u64,
            _: u64,
        ) ![]peer_resolver.PeerEndpoint {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.fail) return error.RouteTemporarilyUnavailable;
            const endpoints = try alloc.alloc(peer_resolver.PeerEndpoint, 1);
            errdefer alloc.free(endpoints);
            const address = try alloc.dupe(u8, "http://peer-2");
            errdefer alloc.free(address);
            const metadata = try alloc.dupe(u8, "");
            endpoints[0] = .{
                .protocol = .http,
                .address = address,
                .metadata = metadata,
            };
            return endpoints;
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
    var resolver = Resolver{};
    var replica_catalog = catalog.MemoryReplicaCatalog.init(std.testing.allocator);
    defer replica_catalog.deinit();
    const catalog_iface = replica_catalog.catalog();
    var host = host_mod.Host.init(std.testing.allocator, .{ .local_node_id = 1 }, .{
        .descriptor_factory = factory.iface(),
        .peer_resolver = resolver.iface(),
        .replica_catalog = catalog_iface,
    });
    defer host.deinit();
    var provider = MemoryPlacementProvider.init(std.testing.allocator);
    defer provider.deinit();
    const intents = [_]PlacementIntent{.{
        .record = .{ .group_id = 505, .replica_id = 1, .local_node_id = 1 },
        .peer_node_ids = &.{ 1, 2 },
    }};
    try provider.replaceAll(&intents);
    var owner = Reconciler{
        .alloc = std.testing.allocator,
        .host = &host,
        .provider = provider.provider(),
    };
    defer owner.deinit();

    const admitted = try owner.reconcileOnce();
    try std.testing.expectEqual(@as(usize, 1), admitted.ensured);
    try std.testing.expectEqual(@as(usize, 1), admitted.route_retrying_groups);
    try std.testing.expectEqual(RouteConvergence.retrying, owner.routeStatus(505).?);
    const retry_diagnostics = owner.routeDiagnostics(505).?;
    try std.testing.expectEqual(@as(u32, 1), retry_diagnostics.attempts);
    try std.testing.expect(retry_diagnostics.next_retry_ns > 0);
    const durable_revision = catalog_iface.revision();

    resolver.fail = false;
    owner.route_retries.getPtr(505).?.next_retry_ns = 0;
    const converged = try owner.reconcileOnce();
    try std.testing.expectEqual(@as(usize, 1), converged.refreshed_peers);
    try std.testing.expectEqual(@as(usize, 0), converged.route_retrying_groups);
    try std.testing.expectEqual(RouteConvergence.converged, owner.routeStatus(505).?);
    try std.testing.expectEqual(durable_revision, catalog_iface.revision());
}

test "live convergence retry work is bounded and rotates fairly" {
    var host = host_mod.Host.init(std.testing.allocator, .{ .local_node_id = 1 }, .{});
    defer host.deinit();
    var provider = MemoryPlacementProvider.init(std.testing.allocator);
    defer provider.deinit();
    var owner = Reconciler{
        .alloc = std.testing.allocator,
        .host = &host,
        .provider = provider.provider(),
    };
    defer owner.deinit();

    const intent_count = max_live_retry_groups_per_round + 1;
    const intents = try std.testing.allocator.alloc(PlacementIntent, intent_count);
    defer std.testing.allocator.free(intents);
    for (intents, 0..) |*intent, index| {
        intent.* = .{
            .record = .{
                .group_id = @intCast(index + 1),
                .replica_id = 1,
                .local_node_id = 1,
            },
            .peer_node_ids = &.{ 1, 2 },
        };
    }
    try owner.ensureConvergenceCapacity(intents.len);
    for (intents) |intent| {
        owner.route_convergence.putAssumeCapacity(intent.record.group_id, .retrying);
        owner.route_retries.putAssumeCapacity(intent.record.group_id, .{});
    }

    var first = try owner.prepareLiveConvergence(intents);
    defer first.deinit();
    try std.testing.expectEqual(max_live_retry_groups_per_round, first.route_groups.len);
    try std.testing.expectEqual(@as(usize, 0), first.route_groups[0].intent_index);

    var second = try owner.prepareLiveConvergence(intents);
    defer second.deinit();
    try std.testing.expectEqual(max_live_retry_groups_per_round, second.route_groups.len);
    try std.testing.expectEqual(max_live_retry_groups_per_round, second.route_groups[0].intent_index);

    const force_routes = try std.testing.allocator.alloc(bool, intents.len);
    defer std.testing.allocator.free(force_routes);
    @memset(force_routes, true);
    owner.live_retry_cursor = 0;
    var forced_first = try owner.buildLiveConvergence(intents, force_routes);
    defer forced_first.deinit();
    try std.testing.expectEqual(max_live_retry_groups_per_round, forced_first.route_groups.len);
    try std.testing.expectEqual(RouteConvergence.retrying, owner.routeStatus(intent_count).?);

    var forced_second = try owner.buildLiveConvergence(intents, force_routes);
    defer forced_second.deinit();
    try std.testing.expectEqual(max_live_retry_groups_per_round, forced_second.route_groups.len);
    try std.testing.expectEqual(max_live_retry_groups_per_round, forced_second.route_groups[0].intent_index);
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
    try prepared.prepareDurable();
    const result = try prepared.commit();
    try std.testing.expect(result.hasFailures());
    try std.testing.expectEqual(@as(usize, 1), result.failed_groups);
    try std.testing.expectEqual(@as(u64, 503), result.failures()[0].group_id);
    try std.testing.expectEqual(ReconcileFailurePhase.admission_prepare, result.failures()[0].phase);
    try std.testing.expectEqual(
        ReconcileFailureClassification.permanent,
        result.failures()[0].classification,
    );
    try std.testing.expectEqual(error.MissingBackupRestoreBootstrapHandler, result.failures()[0].err);

    try std.testing.expectEqual(host_mod.HostedReplicaStatus.failed, host.status(503));
    try std.testing.expect(!host.hasReplica(503));
    try std.testing.expectError(error.InvalidReconcilePhase, prepared.commit());

    // An unchanged permanent failure is parked instead of rebuilding the same
    // invalid descriptor every control round.
    {
        var deferred = try owner.prepare();
        defer deferred.deinit();
        try std.testing.expectEqual(@as(usize, 0), deferred.ensures.len);
    }

    // Desired-state removal sweeps diagnostics for a group that never reached
    // either live or catalog ownership.
    try provider.replaceAll(&.{});
    {
        var pruned = try owner.prepare();
        defer pruned.deinit();
        try std.testing.expectEqual(@as(usize, 0), pruned.removals.len);
    }
    try std.testing.expect(owner.failureDiagnostics(503) == null);

    // A changed intent fingerprint wakes admission immediately.
    try provider.replaceAll(&.{.{
        .record = .{
            .group_id = 503,
            .replica_id = 1,
            .local_node_id = 1,
            .metadata_version = 2,
            .bootstrap_mode = .fetch_snapshot,
            .backup_restore_bootstrap = .{
                .backup_id = "backup-503",
                .artifact_backup_id = "backup-503",
                .location = "file:///unused",
                .snapshot_path = "backup-503/groups/503",
                .connection = "backup-store",
                .artifact_size_bytes = 1,
                .artifact_sha256 = "e3b0c44298fc1c149afbf4a8996fb92427ae41e4649b934ca495991b7852b855",
            },
        },
    }});
    {
        var changed = try owner.prepare();
        defer changed.deinit();
        try std.testing.expectEqual(@as(usize, 1), changed.ensures.len);
    }
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
