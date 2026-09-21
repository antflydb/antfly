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

//! Executable authenticated adapter for the existing durable merge scheduler.
//! Every effect is a normal replicated owner command or one bounded immutable
//! page preparation. Metadata publication is exclusively the lease-fenced CAS.
const std = @import("std");
const online = @import("online_merge.zig");
const table = @import("table_manager.zig");
const state_mod = @import("transition_state.zig");
const io_contract = @import("../storage/db/online_merge_io_contract.zig");
const types = @import("../storage/db/types.zig");
const page = @import("../storage/db/merge_page_contract.zig");
const api = @import("../api/http_client.zig");
const http = @import("../common/http/http_common.zig");
const operation = @import("../api/operation.zig");
const Allocator = std.mem.Allocator;

pub const Context = struct {
    record: state_mod.MergeTransitionRecord,
    donor: ?table.RangeRecord,
    receiver: table.RangeRecord,
    lease: @import("reconcile_lease.zig").ReconcileLeaseRecord,
};
pub const Runtime = struct {
    driver: online.Driver,
    destroy: *const fn (*anyopaque) void,
    pub fn deinit(self: *Runtime) void {
        self.destroy(self.driver.ptr);
        self.* = undefined;
    }
};

pub fn create(service: anytype, executor: http.RequestExecutor, capabilities: online.Capabilities) !Runtime {
    try capabilities.require();
    const T = Adapter(@TypeOf(service.*));
    const value = try service.alloc.create(T);
    value.* = .{ .service = service, .executor = executor, .alloc = service.alloc };
    return .{ .driver = .{ .ptr = value, .capabilities = capabilities, .observe = T.observe, .execute = T.execute, .compare_and_set = T.cas, .release_observation = T.releaseObservation, .current_state = T.current, .admit = T.admit }, .destroy = T.destroy };
}

fn validatePreparedFields(request: types.BatchRequest, comptime allowed: []const []const u8) !void {
    const empty: types.BatchRequest = .{};
    inline for (std.meta.fields(types.BatchRequest)) |field| {
        const permitted = comptime blk: {
            for (allowed) |name| if (std.mem.eql(u8, field.name, name)) break :blk true;
            break :blk false;
        };
        if (!permitted) {
            const actual = @field(request, field.name);
            if (comptime @typeInfo(field.type) == .pointer and @typeInfo(field.type).pointer.size == .slice) {
                if (actual.len != 0) return error.OnlineMergeReceiptMismatch;
            } else if (!std.meta.eql(actual, @field(empty, field.name))) return error.OnlineMergeReceiptMismatch;
        }
    }
}

fn validatePrepared(state: online.State, operation_kind: @FieldType(io_contract.Request, "operation"), result: io_contract.Prepared) !void {
    if (!std.meta.eql(result.scope, state.scope)) return error.OnlineMergeReceiptMismatch;
    const request = result.request orelse return;
    // Reject unrelated controls, including future fields by default.
    if (request.merge_checkpoint != null) {
        try validatePreparedFields(request, &.{ "merge_checkpoint", "merge_replication" });
    } else try validatePreparedFields(request, &.{ "merge_replication", "merge_page", "writes", "deletes" });
    const context = request.merge_replication orelse return error.OnlineMergeReceiptMismatch;
    if (context.transition_id != state.scope.fence.transition_id or context.donor_group_id != state.scope.fence.owner_group_id or context.receiver_group_id != state.scope.fence.peer_group_id or
        !context.identity_namespace.eql(state.scope.receiver_namespace)) return error.OnlineMergeReceiptMismatch;
    if (request.merge_checkpoint) |actual| {
        if (operation_kind != .checkpoint) return error.OnlineMergeReceiptMismatch;
        var expected = operation_kind.checkpoint;
        if (!std.meta.eql(context.copy_attempt, actual.copy_attempt)) return error.OnlineMergeReceiptMismatch;
        // Native accept reserves the tuple before a copy attempt is bound;
        // rollback may likewise close that exact initial zero-attempt tuple.
        if (actual.kind == .accept and !std.meta.eql(actual.copy_attempt, types.MergeCopyAttempt{})) return error.OnlineMergeReceiptMismatch;
        if ((actual.kind == .accept or actual.kind == .rollback) and std.meta.eql(actual.copy_attempt, types.MergeCopyAttempt{})) expected.copy_attempt = .{};
        if (!std.meta.eql(actual.copy_attempt, types.MergeCopyAttempt{}) and !std.meta.eql(actual.copy_attempt, state.scope.copy_attempt)) return error.OnlineMergeReceiptMismatch;
        inline for (std.meta.fields(types.MergeReplicationCheckpoint)) |field| {
            if (comptime field.type == []const u8) {
                if (!std.mem.eql(u8, @field(actual, field.name), @field(expected, field.name))) return error.OnlineMergeReceiptMismatch;
            } else if (!std.meta.eql(@field(actual, field.name), @field(expected, field.name))) return error.OnlineMergeReceiptMismatch;
        }
        return;
    }
    if (!std.meta.eql(context.copy_attempt, state.scope.copy_attempt)) return error.OnlineMergeReceiptMismatch;
    const receipt = switch (operation_kind) {
        .cleanup, .tail => |value| value,
        .snapshot, .integrity => |value| value.receipt,
        else => return error.OnlineMergeReceiptMismatch,
    };
    const command = request.merge_page orelse return error.OnlineMergeReceiptMismatch;
    const next_sequence = std.math.add(u64, receipt.sequence, 1) catch return error.OnlineMergeReceiptMismatch;
    if (!command.source.eql(try state.sourceIdentity()) or command.phase != receipt.phase or command.sequence != next_sequence or
        !std.mem.eql(u8, command.after, receipt.cursor)) return error.OnlineMergeReceiptMismatch;
    page.validateRequest(request) catch return error.OnlineMergeReceiptMismatch;
}

fn validateRevocation(state: online.State, result: io_contract.Prepared) !types.BatchRequest {
    if (!std.meta.eql(result.scope, state.scope)) return error.OnlineMergeReceiptMismatch;
    const batch = result.request orelse return error.OnlineMergeReceiptMismatch;
    if (batch.merge_source_transition) |command| {
        if (command.kind != .rollback or command.transition_id != state.scope.fence.transition_id or command.receiver_group_id != state.scope.fence.peer_group_id) return error.OnlineMergeReceiptMismatch;
        try validatePreparedFields(batch, &.{"merge_source_transition"});
    } else if (batch.relational_topology) |command| {
        if (command.action != .abort_transition or !command.fence.eql(state.scope.fence) or command.transfer != null) return error.OnlineMergeReceiptMismatch;
        try validatePreparedFields(batch, &.{"relational_topology"});
    } else return error.OnlineMergeReceiptMismatch;
    return batch;
}

fn artifactSlice(cursor: *usize, replicas: []const []const u8, target: []const u8, worker: anytype) !void {
    // Skip self without spending a scheduler tick. A successful bounded slice
    // keeps its peer; errors advance discovery so one absent pin cannot starve
    // a healthy replica. At most one remote candidate is contacted per call.
    for (0..replicas.len) |_| {
        const peer = replicas[cursor.* % replicas.len];
        if (std.mem.eql(u8, peer, target)) {
            cursor.* +%= 1;
            continue;
        }
        worker.run(peer) catch |err| {
            cursor.* +%= 1;
            return err;
        };
        return;
    }
    return error.OnlineSourcePinMissing;
}

fn Adapter(comptime Service: type) type {
    return struct {
        service: *Service,
        executor: http.RequestExecutor,
        alloc: Allocator,
        artifact_peer_cursor: usize = 0,
        const Self = @This();
        const Owned = struct {
            arena: *std.heap.ArenaAllocator,
            context: Context,
            request: operation.RequestContext,
            donor_uri: []const u8,
            receiver_uri: []const u8,
            donor_replica_uris: []const []const u8,
            fn uri(self: Owned, group: u64) ![]const u8 {
                if (group == self.context.record.donor_group_id) return self.donor_uri;
                if (group == self.context.record.receiver_group_id) return self.receiver_uri;
                return error.OnlineSourceScopeChanged;
            }
        };
        fn destroy(ptr: *anyopaque) void {
            const self: *Self = @ptrCast(@alignCast(ptr));
            self.alloc.destroy(self);
        }
        fn current(ptr: *anyopaque, expected: online.State) !online.Driver.Current {
            const self: *Self = @ptrCast(@alignCast(ptr));
            return @import("service.zig").currentOnlineMerge(self.service, expected);
        }
        fn admit(ptr: *anyopaque, queued: state_mod.MergeTransitionRecord) !?online.State {
            const self: *Self = @ptrCast(@alignCast(ptr));
            const arena = try self.alloc.create(std.heap.ArenaAllocator);
            arena.* = std.heap.ArenaAllocator.init(self.alloc);
            defer {
                arena.deinit();
                self.alloc.destroy(arena);
            }
            const alloc = arena.allocator();
            const request: operation.RequestContext = .{ .deadline_ns = @import("antfly_platform").time.monotonicNs() + 2 * std.time.ns_per_s };
            const context = try @import("service.zig").onlineMergeAdmissionContext(self.service, alloc, queued, request);
            if (context.record.online) |existing| {
                try existing.validateRecord(context.record);
                return existing;
            }
            if (context.record.phase != .prepare or context.record.rollback_reason != null) return null;
            // Coordinated tables additionally require the exact immutable
            // catalog/activation proof on BOTH owners below. REF3 retains
            // integrity changes in the same committed frames as primary rows.
            try context.record.table_contract.validateForMerge(context.record.allow_doc_identity_reassignment);
            const source_identity = context.record.table_contract.source_identity;
            const target_identity = context.record.table_contract.target_identity;
            // Current source admission is same-table/no identity reassignment.
            if (source_identity.shard_id != context.record.donor_group_id or target_identity.shard_id != context.record.receiver_group_id) return null;
            var initial: online.State = .{ .scope = .{
                .fence = .{ .transition_id = context.record.transition_id, .owner_group_id = context.record.donor_group_id, .peer_group_id = context.record.receiver_group_id, .namespace = .{ .table_id = context.record.table_contract.table_id, .shard_id = source_identity.shard_id, .range_id = source_identity.range_id }, .role = .merge_source, .attempt = 0, .admission_epoch = 0, .catalog_digest = @splat(0) },
                .receiver_namespace = .{ .table_id = context.record.table_contract.table_id, .shard_id = target_identity.shard_id, .range_id = target_identity.range_id },
                .consumer_epoch = 0,
                .copy_attempt = .{ .donor_term = 0, .sequence = 0 },
            } };
            const resolved = try @import("service.zig").onlineMergeRoutes(self.service, alloc, context.record.donor_group_id, context.record.receiver_group_id, request);
            const owned: Owned = .{ .arena = arena, .context = context, .request = request, .donor_uri = resolved.donor_uri, .receiver_uri = resolved.receiver_uri, .donor_replica_uris = resolved.donor_replica_uris };
            const donor = try self.fetch(io_contract.AdmissionFacts, owned, initial, .{ .admission = .donor });
            const receiver = try self.fetch(io_contract.AdmissionFacts, owned, initial, .{ .admission = .receiver });
            if (!donor.namespace.eql(initial.scope.fence.namespace) or !receiver.namespace.eql(initial.scope.receiver_namespace)) return error.OnlineMergeReceiptMismatch;
            if (!donor.eligible or !receiver.eligible) return null;
            const coordinated = context.record.table_contract.integrity_protocol != .none;
            if (coordinated != (donor.integrity != null) or coordinated != (receiver.integrity != null)) return null;
            if (coordinated) {
                if (!std.meta.eql(donor.integrity, receiver.integrity) or
                    !std.mem.eql(u8, &donor.integrity.?.catalog_digest, &donor.catalog_digest) or
                    !std.mem.eql(u8, &receiver.integrity.?.catalog_digest, &receiver.catalog_digest)) return error.OnlineMergeReceiptMismatch;
            }
            if (donor.next_topology_epoch == 0 or donor.next_consumer_epoch == 0 or donor.donor_term == 0 or donor.next_copy_sequence == 0) return error.OnlineMergeReceiptMismatch;
            initial.scope.fence.catalog_digest = donor.catalog_digest;
            initial.scope.fence.admission_epoch = donor.next_topology_epoch;
            initial.scope.fence.attempt = donor.next_consumer_epoch;
            initial.scope.consumer_epoch = donor.next_consumer_epoch;
            initial.scope.copy_attempt = .{ .donor_term = donor.donor_term, .sequence = donor.next_copy_sequence };
            try initial.validateRecord(context.record);
            // Exact immutable record + lease CAS prevents conversion after a
            // competing ordinary step/cancellation/schema or placement change.
            try @import("service.zig").admitOnlineMerge(self.service, context, initial, request);
            return initial;
        }
        fn timeout(request: operation.RequestContext) !u32 {
            try request.ensureActive();
            const remaining = (request.deadline_ns orelse return error.DeadlineExceeded) -| @import("antfly_platform").time.monotonicNs();
            if (remaining < std.time.ns_per_ms) return error.DeadlineExceeded;
            return @intCast(@min(2000, remaining / std.time.ns_per_ms));
        }
        fn releaseObservation(_: *anyopaque, observation: *online.Observation) void {
            if (observation.owned_arena) |arena| {
                const alloc = arena.child_allocator;
                arena.deinit();
                alloc.destroy(arena);
            }
            observation.owned_arena = null;
        }
        fn begin(self: *Self, state: online.State) !Owned {
            const arena = try self.alloc.create(std.heap.ArenaAllocator);
            arena.* = std.heap.ArenaAllocator.init(self.alloc);
            errdefer {
                arena.deinit();
                self.alloc.destroy(arena);
            }
            const request: operation.RequestContext = .{ .deadline_ns = @import("antfly_platform").time.monotonicNs() + 2 * std.time.ns_per_s };
            const context = try @import("service.zig").onlineMergeContext(self.service, arena.allocator(), state, request);
            const resolved = try @import("service.zig").onlineMergeRoutes(self.service, arena.allocator(), state.scope.fence.owner_group_id, state.scope.fence.peer_group_id, request);
            return .{ .arena = arena, .context = context, .request = request, .donor_uri = resolved.donor_uri, .receiver_uri = resolved.receiver_uri, .donor_replica_uris = resolved.donor_replica_uris };
        }
        fn client(self: *Self, alloc: Allocator) api.ApiHttpClient {
            var result = api.ApiHttpClient.init(alloc, self.executor);
            _ = result.withInternalServiceAuth(self.service.internal_service_secret, self.service.internal_service_issuer);
            return result;
        }
        fn fetch(self: *Self, comptime T: type, owned: Owned, state: online.State, op: @FieldType(io_contract.Request, "operation")) !T {
            const alloc = owned.arena.allocator();
            const request: io_contract.Request = .{ .scope = state.scope, .operation = op };
            var transport = self.client(alloc);
            var cancellation: http.RequestCancellation = .{ .borrowed_context = owned.request.cancellation.ptr, .borrowed_is_cancelled = owned.request.cancellation.is_cancelled_fn };
            var response = transport.fetchGroupOnlineMergeIo(try owned.uri(request.ownerGroup()), request.ownerGroup(), owned.context.record.table_contract.table_name, request, try timeout(owned.request), &cancellation) catch |err| {
                if (err == error.OnlineSourcePinMissing and op == .snapshot) {
                    try self.rehydrate(owned, state);
                    return error.OnlineMergeArtifactPending;
                }
                return err;
            };
            defer response.deinit(alloc);
            return std.json.parseFromSliceLeaky(T, alloc, response.body, .{ .allocate = .alloc_always });
        }
        const ReplicaTransport = struct {
            self: *Self,
            uri: []const u8,
            scope: @import("../storage/db/online_source_contract.zig").Scope,
            pub fn onlineSourceArtifact(replica: *@This(), alloc: Allocator, group: u64, table_name: []const u8, request: @import("../storage/db/source_artifact_transfer.zig").Request, context: operation.RequestContext) ![]u8 {
                if (group != replica.scope.fence.owner_group_id or !std.meta.eql(request.scope(), replica.scope)) return error.OnlineSourceScopeChanged;
                var transport = replica.self.client(alloc);
                var cancel: http.RequestCancellation = .{ .borrowed_context = context.cancellation.ptr, .borrowed_is_cancelled = context.cancellation.is_cancelled_fn };
                var result = try transport.fetchGroupOnlineMergeIo(replica.uri, group, table_name, .{ .scope = replica.scope, .operation = .{ .artifact = request } }, try timeout(context), &cancel);
                defer result.deinit(alloc);
                return alloc.dupe(u8, result.body);
            }
        };
        fn rehydrate(self: *Self, owned: Owned, state: online.State) !void {
            const Worker = struct {
                adapter: *Self,
                owned: Owned,
                state: online.State,
                fn run(worker: @This(), donor_uri: []const u8) !void {
                    var donor: ReplicaTransport = .{ .self = worker.adapter, .uri = donor_uri, .scope = worker.state.scope };
                    var target: ReplicaTransport = .{ .self = worker.adapter, .uri = worker.owned.donor_uri, .scope = worker.state.scope };
                    _ = try @import("online_merge_artifact.zig").step(worker.owned.arena.allocator(), .from(&donor), .from(&target), worker.owned.context.record.table_contract.table_name, worker.state, worker.owned.request);
                }
            };
            try artifactSlice(&self.artifact_peer_cursor, owned.donor_replica_uris, owned.donor_uri, Worker{ .adapter = self, .owned = owned, .state = state });
        }
        fn submit(self: *Self, owned: Owned, state: online.State, group: u64, batch: types.BatchRequest) !void {
            try owned.request.ensureActive();
            // Recheck lease/state immediately before each externally durable
            // effect; a stale scheduler never invents a new copy attempt.
            _ = try @import("service.zig").onlineMergeContext(self.service, owned.arena.allocator(), state, owned.request);
            const alloc = owned.arena.allocator();
            const body = try @import("../api/batch.zig").encodeBatchRequest(alloc, batch);
            var transport = self.client(alloc);
            var cancellation: http.RequestCancellation = .{ .borrowed_context = owned.request.cancellation.ptr, .borrowed_is_cancelled = owned.request.cancellation.is_cancelled_fn };
            const timeout_ms = try timeout(owned.request);
            var response = try transport.fetchGroupBatchWithForwarding(try owned.uri(group), group, owned.context.record.table_contract.table_name, body, timeout_ms, .{ .remaining_ms = timeout_ms, .forwards_remaining = 2, .campaign_allowed = true }, &cancellation, null);
            defer response.deinit(alloc);
        }
        fn observe(ptr: *anyopaque, state: online.State) !online.Observation {
            const self: *Self = @ptrCast(@alignCast(ptr));
            const owned = try self.begin(state);
            errdefer {
                owned.arena.deinit();
                self.alloc.destroy(owned.arena);
            }
            const donor = try self.fetch(io_contract.SourceStatus, owned, state, .{ .status = .donor });
            if (state.phase == .admit and donor.ordinary_conflict) {
                if (!std.meta.eql(donor.scope, state.scope)) return error.OnlineMergeReceiptMismatch;
                return .{ .owned_arena = owned.arena, .scope = state.scope, .source_progress = donor.progress, .ordinary_conflict = true };
            }
            const receiver = try self.fetch(io_contract.ReceiverStatus, owned, state, .{ .status = .receiver });
            if (!std.meta.eql(donor.scope, state.scope) or !std.meta.eql(receiver.scope, state.scope)) return error.OnlineMergeReceiptMismatch;
            if (!donor.row_derived_indexes and owned.context.record.rollback_reason == null and
                state.phase != .cancel_receiver and state.phase != .cancel_release and state.phase != .release and !state.terminal())
                return error.OnlineMergeArtifactTailsUnsupported;
            const finalized = if (receiver.state) |value| value.phase == .finalized and value.bootstrap_complete and value.bootstrap_applied_index == state.final_applied_index else false;
            return .{
                .owned_arena = owned.arena,
                .scope = state.scope,
                .source_progress = donor.progress,
                .certificate = donor.certificate,
                .receiver = receiver.progress,
                .retained_head = donor.retained_head,
                .retained_reclaimed = donor.retained_reclaimed,
                .retained_reclaimable = donor.retained_reclaimable,
                .frozen = donor.drained and donor.fence != null and donor.fence.?.eql(state.scope.fence),
                .cutover_prepared = state.phase == .cutover and finalized,
                .receiver_cancelled = if (receiver.state) |value| value.phase == .rolled_back else false,
                .source_admission_closed = donor.next_epoch > state.scope.fence.admission_epoch and donor.fence == null and !donor.ordinary_scope_conflict,
            };
        }
        fn checkpoint(state: online.State, context: Context, kind: types.MergeReplicationCheckpoint.Kind) !types.MergeReplicationCheckpoint {
            const receiver = context.receiver;
            const donor = context.donor orelse return error.OnlineMergeReceiptMismatch;
            const donor_first = std.mem.order(u8, donor.start_key, receiver.start_key) == .lt;
            const donor_end = donor.end_key orelse "";
            const receiver_end = receiver.end_key orelse "";
            if (!(if (donor_first) std.mem.eql(u8, donor_end, receiver.start_key) else std.mem.eql(u8, receiver_end, donor.start_key))) return error.OnlineMergeReceiptMismatch;
            // Metadata authorizes logical reconstruction into the receiver's
            // identity. No native raw-identity reassignment is requested: the
            // row-derived-only page applier assigns target-local identities.
            const source_identity = if (state.certificate != null) try state.sourceIdentity() else null;
            const bind_source = kind == .begin_copy or (kind == .accept and source_identity != null and source_identity.?.integrity != null);
            if (bind_source and source_identity == null) return error.OnlineMergeReceiptMismatch;
            return .{ .kind = kind, .transition_id = state.scope.fence.transition_id, .donor_group_id = state.scope.fence.owner_group_id, .receiver_group_id = state.scope.fence.peer_group_id, .receiver_base_start = receiver.start_key, .receiver_base_end = receiver_end, .merged_start = if (donor_first) donor.start_key else receiver.start_key, .merged_end = if (donor_first) receiver_end else donor_end, .bootstrap_applied_index = if (kind == .bootstrap_complete or kind == .finalize) state.final_applied_index else 0, .copy_attempt = state.scope.copy_attempt, .page_source = if (bind_source) source_identity else null, .page_receiver_namespace = if (bind_source) state.scope.receiver_namespace else null };
        }
        fn prepared(self: *Self, owned: Owned, state: online.State, operation_kind: @FieldType(io_contract.Request, "operation")) !void {
            const result = try self.fetch(io_contract.Prepared, owned, state, operation_kind);
            try validatePrepared(state, operation_kind, result);
            if (result.request) |request| {
                try self.submit(owned, state, state.scope.fence.peer_group_id, request);
            }
        }
        fn execute(ptr: *anyopaque, state: online.State, action: online.Action) !void {
            const self: *Self = @ptrCast(@alignCast(ptr));
            const owned = try self.begin(state);
            defer {
                owned.arena.deinit();
                self.alloc.destroy(owned.arena);
            }
            switch (action) {
                .source_command => |command| try self.submit(owned, state, state.scope.fence.owner_group_id, .{ .online_source = command }),
                .prepare_certificate => {
                    // Publication has a DB-owned lifetime: HTTP polls never
                    // restart a large immutable export at their deadline.
                    if (try self.fetch(?@import("../storage/source_snapshot.zig").Certificate, owned, state, .publication) == null)
                        return error.OnlineMergePublicationPending;
                },
                .freeze_and_drain => try self.submit(owned, state, state.scope.fence.owner_group_id, .{ .relational_topology = .{ .fence = state.scope.fence, .action = .begin } }),
                .cancel_source_admission => {
                    const prepared_revoke = try self.fetch(io_contract.Prepared, owned, state, .revoke);
                    const batch = try validateRevocation(state, prepared_revoke);
                    try self.submit(owned, state, state.scope.fence.owner_group_id, batch);
                },
                .snapshot_page, .tail_page, .cutover, .cancel_receiver => {
                    const receiver = try self.fetch(io_contract.ReceiverStatus, owned, state, .{ .status = .receiver });
                    if (!std.meta.eql(receiver.scope, state.scope)) return error.OnlineMergeReceiptMismatch;
                    if (receiver.state == null) return self.prepared(owned, state, .{ .checkpoint = try checkpoint(state, owned.context, .accept) });
                    // Even cancellation before initialization needs a durable
                    // exact-transition receipt. Accept first, then roll back
                    // on the next bounded step; absence is not cleanup proof.
                    if (action == .cancel_receiver) return self.prepared(owned, state, .{ .checkpoint = try checkpoint(state, owned.context, .rollback) });
                    if (receiver.progress == null) return self.prepared(owned, state, .{ .checkpoint = try checkpoint(state, owned.context, .begin_copy) });
                    const receipt = receiver.progress.?;
                    if (!receipt.source.eql(try state.sourceIdentity())) return error.OnlineMergeReceiptMismatch;
                    if (action == .cutover) {
                        if (receipt.phase != .complete or receipt.final_applied_index != state.final_applied_index or !std.mem.eql(u8, &receipt.final_cut_digest, &state.final_cut_digest)) return error.OnlineMergeReceiptMismatch;
                        return self.prepared(owned, state, .{ .checkpoint = try checkpoint(state, owned.context, if (receiver.state.?.bootstrap_complete) .finalize else .bootstrap_complete) });
                    }
                    switch (receipt.phase) {
                        .cleanup, .cleanup_integrity => try self.prepared(owned, state, .{ .cleanup = receipt }),
                        .rows => try self.prepared(owned, state, .{ .snapshot = .{ .receipt = receipt, .certificate = state.certificate.? } }),
                        .artifacts => if (receipt.source.integrity != null)
                            try self.prepared(owned, state, .{ .integrity = .{ .receipt = receipt, .certificate = state.certificate.? } })
                        else
                            try self.prepared(owned, state, .{ .snapshot = .{ .receipt = receipt, .certificate = state.certificate.? } }),
                        .tail => try self.prepared(owned, state, .{ .tail = receipt }),
                        .complete => {},
                    }
                },
            }
        }
        fn cas(ptr: *anyopaque, previous: online.State, next: online.State, cancel: bool) !bool {
            const self: *Self = @ptrCast(@alignCast(ptr));
            if (previous.phase == .cutover and next.phase == .release) {
                const owned = try self.begin(previous);
                defer {
                    owned.arena.deinit();
                    self.alloc.destroy(owned.arena);
                }
                const receiver = try self.fetch(io_contract.ReceiverStatus, owned, previous, .{ .status = .receiver });
                const finished = receiver.state orelse return error.OnlineMergeReceiptMismatch;
                const receipt = receiver.progress orelse return error.OnlineMergeReceiptMismatch;
                if (!std.meta.eql(receiver.scope, previous.scope) or finished.phase != .finalized or !finished.bootstrap_complete or
                    finished.bootstrap_applied_index != previous.final_applied_index or receipt.phase != .complete or receipt.final_applied_index != previous.final_applied_index or
                    !receipt.source.eql(try previous.sourceIdentity()) or !std.mem.eql(u8, &receipt.final_cut_digest, &previous.final_cut_digest)) return error.OnlineMergeReceiptMismatch;
                return @import("service.zig").compareAndSetOnlineMerge(self.service, previous, next, cancel, @import("storage/raft_apply_store.zig").OnlineMergeCutoverBounds.fromRanges(finished.receiver_base_range, finished.merged_range orelse return error.OnlineMergeReceiptMismatch));
            }
            return @import("service.zig").compareAndSetOnlineMerge(self.service, previous, next, cancel, null);
        }
    };
}

test "metadata transition driver online artifact progress retains healthy peer and advances failed candidates" {
    const Worker = struct {
        calls: usize = 0,
        transferred: usize = 0,
        healthy_available: bool = true,
        fn run(self: *@This(), peer: []const u8) !void {
            self.calls += 1;
            if (std.mem.eql(u8, peer, "missing")) return error.OnlineSourcePinMissing;
            if (std.mem.eql(u8, peer, "healthy") and !self.healthy_available) return error.GroupLeaderUnavailable;
            try std.testing.expect(std.mem.eql(u8, peer, "healthy") or std.mem.eql(u8, peer, "replacement"));
            self.transferred += 1024 * 1024;
        }
    };
    const replicas: []const []const u8 = &.{ "self", "missing", "healthy", "replacement" };
    var cursor: usize = 0;
    var worker: Worker = .{};
    try std.testing.expectError(error.OnlineSourcePinMissing, artifactSlice(&cursor, replicas, "self", &worker));
    try std.testing.expectEqual(@as(usize, 1), worker.calls);
    for (0..3) |index| {
        try artifactSlice(&cursor, replicas, "self", &worker);
        try std.testing.expectEqual(@as(usize, 2), cursor);
        try std.testing.expectEqual((index + 1) * 1024 * 1024, worker.transferred);
        try std.testing.expectEqual(index + 2, worker.calls);
    }
    worker.healthy_available = false;
    try std.testing.expectError(error.GroupLeaderUnavailable, artifactSlice(&cursor, replicas, "self", &worker));
    try std.testing.expectEqual(@as(usize, 3), cursor);
    try artifactSlice(&cursor, replicas, "self", &worker);
    try std.testing.expectEqual(@as(usize, 4 * 1024 * 1024), worker.transferred);
    const calls = worker.calls;
    try std.testing.expectError(error.OnlineSourcePinMissing, artifactSlice(&cursor, &.{"self"}, "self", &worker));
    try std.testing.expectError(error.OnlineSourcePinMissing, artifactSlice(&cursor, &.{}, "self", &worker));
    try std.testing.expectEqual(calls, worker.calls);
}

test "metadata transition driver online constructs every native checkpoint without reassignment" {
    const batch_wire = @import("../api/batch.zig");
    const alloc = std.testing.allocator;
    var state: online.State = .{ .scope = .{
        .fence = .{ .transition_id = 9, .attempt = 1, .peer_group_id = 3, .owner_group_id = 2, .role = .merge_source, .namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 4 }, .catalog_digest = @splat(7) },
        .receiver_namespace = .{ .table_id = 1, .shard_id = 3, .range_id = 5 },
        .consumer_epoch = 6,
        .copy_attempt = .{ .donor_term = 8, .sequence = 1 },
    } };
    state.certificate = .{ .cut = .{ .namespace = state.scope.fence.namespace, .applied_index = 19, .retained_start = 11 }, .objects = 1, .content_bytes = 100, .schema_manifest_digest = @splat(2), .ordered_content_digest = @splat(3) };
    state.acknowledged = 11;
    state.final_applied_index = 25;
    const context: Context = .{
        .record = .{ .transition_id = 9, .donor_group_id = 2, .receiver_group_id = 3 },
        .donor = .{ .group_id = 2, .table_id = 1, .start_key = "", .end_key = "m" },
        .receiver = .{ .group_id = 3, .table_id = 1, .start_key = "m", .end_key = null },
        .lease = .{ .owner_node_id = 1, .expires_at_ms = 100 },
    };
    inline for (.{ .accept, .begin_copy, .bootstrap_complete, .finalize, .rollback }) |kind| {
        const command = try Adapter(void).checkpoint(state, context, kind);
        try (io_contract.Request{ .scope = state.scope, .operation = .{ .checkpoint = command } }).validate();
        try std.testing.expect(command.receiver_identity_reassignment_namespace == null);
        try std.testing.expect(!command.allow_doc_identity_reassignment);
        try std.testing.expectEqualStrings("", command.merged_start);
        try std.testing.expectEqualStrings("", command.merged_end);
        try std.testing.expectEqual(kind == .begin_copy, command.page_source != null);
        try std.testing.expectEqual(kind == .begin_copy, command.page_receiver_namespace != null);
        var response: io_contract.Prepared = .{ .scope = state.scope, .request = .{ .merge_checkpoint = command, .merge_replication = .{ .transition_id = state.scope.fence.transition_id, .donor_group_id = state.scope.fence.owner_group_id, .receiver_group_id = state.scope.fence.peer_group_id, .identity_namespace = state.scope.receiver_namespace, .copy_attempt = state.scope.copy_attempt } } };
        if (kind == .accept) {
            response.request.?.merge_checkpoint.?.copy_attempt = .{};
            response.request.?.merge_replication.?.copy_attempt = .{};
        }
        const operation_kind: @FieldType(io_contract.Request, "operation") = .{ .checkpoint = command };
        try validatePrepared(state, operation_kind, response);
        const encoded = try batch_wire.encodeBatchRequest(alloc, response.request.?);
        defer alloc.free(encoded);
        var parsed = try batch_wire.parseInternalBatchRequest(alloc, encoded);
        defer parsed.deinit(alloc);
        try validatePrepared(state, operation_kind, .{ .scope = state.scope, .request = parsed.req });
        var wrong = response;
        wrong.request.?.merge_checkpoint.?.transition_id += 1;
        try std.testing.expectError(error.OnlineMergeReceiptMismatch, validatePrepared(state, operation_kind, wrong));
        wrong = response;
        wrong.request.?.merge_checkpoint.?.merged_end = "widened";
        try std.testing.expectError(error.OnlineMergeReceiptMismatch, validatePrepared(state, operation_kind, wrong));
        wrong = response;
        wrong.request.?.merge_checkpoint.?.kind = if (kind == .finalize) .rollback else .finalize;
        try std.testing.expectError(error.OnlineMergeReceiptMismatch, validatePrepared(state, operation_kind, wrong));
        wrong = response;
        wrong.request.?.writes = &.{.{ .key = "unexpected", .value = "{}" }};
        try std.testing.expectError(error.OnlineMergeReceiptMismatch, validatePrepared(state, operation_kind, wrong));
        wrong = response;
        wrong.request.?.online_source = .{ .release = state.scope };
        try std.testing.expectError(error.OnlineMergeReceiptMismatch, validatePrepared(state, operation_kind, wrong));
        wrong = response;
        wrong.request.?.merge_replication = null;
        try std.testing.expectError(error.OnlineMergeReceiptMismatch, validatePrepared(state, operation_kind, wrong));
        wrong = response;
        wrong.request.?.merge_replication.?.copy_attempt.sequence += 1;
        try std.testing.expectError(error.OnlineMergeReceiptMismatch, validatePrepared(state, operation_kind, wrong));
        if (kind == .rollback) {
            response.request.?.merge_checkpoint.?.copy_attempt = .{};
            response.request.?.merge_replication.?.copy_attempt = .{};
            try validatePrepared(state, operation_kind, response);
            const initial_encoded = try batch_wire.encodeBatchRequest(alloc, response.request.?);
            defer alloc.free(initial_encoded);
            var initial_parsed = try batch_wire.parseInternalBatchRequest(alloc, initial_encoded);
            defer initial_parsed.deinit(alloc);
            try validatePrepared(state, operation_kind, .{ .scope = state.scope, .request = initial_parsed.req });
        } else if (kind != .accept) {
            wrong = response;
            wrong.request.?.merge_checkpoint.?.copy_attempt = .{};
            wrong.request.?.merge_replication.?.copy_attempt = .{};
            try std.testing.expectError(error.OnlineMergeReceiptMismatch, validatePrepared(state, operation_kind, wrong));
        }
    }
    // The same constructor handles a receiver to the left and open-ended
    // donor range, without manufacturing null-as-a-key or widening a gap.
    var reversed = context;
    // Coordinated accept binds the authenticated source before any cleanup,
    // unlike ordinary row-only accept. Both Raft/private HTTP and standby
    // payloads must refuse a downgrade that could discard that authority.
    var coordinated = state;
    coordinated.certificate.?.integrity = .{ .catalog_digest = state.scope.fence.catalog_digest, .generation_set = @splat(8) };
    inline for (.{ .accept, .begin_copy }) |kind| {
        var checkpoint_request = try Adapter(void).checkpoint(coordinated, context, kind);
        try std.testing.expect(checkpoint_request.page_source.?.integrity != null);
        try (io_contract.Request{ .scope = coordinated.scope, .operation = .{ .checkpoint = checkpoint_request } }).validate();
        if (kind == .accept) checkpoint_request.copy_attempt = .{};
        const command: types.BatchRequest = .{ .merge_checkpoint = checkpoint_request };
        const encoded = try batch_wire.encodeBatchRequest(alloc, command);
        defer alloc.free(encoded);
        try std.testing.expect(std.mem.indexOf(u8, encoded, "page_v4_") != null);
        var decoded = try batch_wire.parseInternalBatchRequest(alloc, encoded);
        defer decoded.deinit(alloc);
        try std.testing.expectEqualDeep(command.merge_checkpoint, decoded.req.merge_checkpoint);
        const effects = @import("../storage/hot_standby/effects.zig");
        const payload = try effects.encodeBatchMutationRequestAlloc(alloc, command);
        defer alloc.free(payload);
        var standby = try std.json.parseFromSlice(effects.BatchMutationPayload, alloc, payload, .{});
        defer standby.deinit();
        try std.testing.expectEqual(@as(u32, 7), standby.value.schema_version);
    }
    reversed.donor.?.start_key = "m";
    reversed.donor.?.end_key = null;
    reversed.receiver.start_key = "";
    reversed.receiver.end_key = "m";
    try (io_contract.Request{ .scope = state.scope, .operation = .{ .checkpoint = try Adapter(void).checkpoint(state, reversed, .accept) } }).validate();
    reversed.receiver.end_key = "l";
    try std.testing.expectError(error.OnlineMergeReceiptMismatch, Adapter(void).checkpoint(state, reversed, .accept));
    var reassignment = context;
    reassignment.record.allow_doc_identity_reassignment = true;
    const reconstructed = try Adapter(void).checkpoint(state, reassignment, .accept);
    try (io_contract.Request{ .scope = state.scope, .operation = .{ .checkpoint = reconstructed } }).validate();
    try std.testing.expect(!reconstructed.allow_doc_identity_reassignment);
    try std.testing.expect(reconstructed.receiver_identity_reassignment_namespace == null);
    const receiver_context: types.MergeReplicationContext = .{ .transition_id = state.scope.fence.transition_id, .donor_group_id = state.scope.fence.owner_group_id, .receiver_group_id = state.scope.fence.peer_group_id, .identity_namespace = state.scope.receiver_namespace, .copy_attempt = state.scope.copy_attempt };
    const receipt: page.Progress = .{ .version = 4, .transition_id = receiver_context.transition_id, .donor_group_id = receiver_context.donor_group_id, .receiver_group_id = receiver_context.receiver_group_id, .receiver_namespace = receiver_context.identity_namespace, .attempt = receiver_context.copy_attempt, .source = try state.sourceIdentity(), .tail_sequence = state.certificate.?.cut.retained_start };
    var batch: types.BatchRequest = .{ .merge_replication = receiver_context, .merge_page = .{ .source = receipt.source, .sequence = 1, .phase = .cleanup, .exhausted = true, .digest = @splat(0) } };
    batch.merge_page.?.digest = page.commandDigest(batch);
    const prepared_page: io_contract.Prepared = .{ .scope = state.scope, .request = batch };
    try validatePrepared(state, .{ .cleanup = receipt }, prepared_page);
    var stale_page = prepared_page;
    stale_page.request.?.relational_schema_version = 1;
    try std.testing.expectError(error.OnlineMergeReceiptMismatch, validatePrepared(state, .{ .cleanup = receipt }, stale_page));
    stale_page = prepared_page;
    stale_page.request.?.merge_page.?.sequence = 2;
    stale_page.request.?.merge_page.?.digest = page.commandDigest(stale_page.request.?);
    try std.testing.expectError(error.OnlineMergeReceiptMismatch, validatePrepared(state, .{ .cleanup = receipt }, stale_page));
    stale_page = prepared_page;
    stale_page.request.?.merge_page = null;
    try std.testing.expectError(error.OnlineMergeReceiptMismatch, validatePrepared(state, .{ .cleanup = receipt }, stale_page));
    for ([_]types.BatchRequest{
        .{ .merge_source_transition = .{ .kind = .rollback, .transition_id = state.scope.fence.transition_id, .receiver_group_id = state.scope.fence.peer_group_id } },
        .{ .relational_topology = .{ .fence = state.scope.fence, .action = .abort_transition } },
    }) |revoke_batch| {
        const reply: io_contract.Prepared = .{ .scope = state.scope, .request = revoke_batch };
        _ = try validateRevocation(state, reply);
        var wrong = reply;
        wrong.scope.consumer_epoch += 1;
        try std.testing.expectError(error.OnlineMergeReceiptMismatch, validateRevocation(state, wrong));
        wrong = reply;
        wrong.request.?.writes = &.{.{ .key = "unexpected", .value = "{}" }};
        try std.testing.expectError(error.OnlineMergeReceiptMismatch, validateRevocation(state, wrong));
        wrong = reply;
        wrong.request.?.online_source = .{ .release = state.scope };
        try std.testing.expectError(error.OnlineMergeReceiptMismatch, validateRevocation(state, wrong));
        wrong = reply;
        if (wrong.request.?.merge_source_transition) |*control| control.receiver_group_id += 1 else wrong.request.?.relational_topology.?.fence.attempt += 1;
        try std.testing.expectError(error.OnlineMergeReceiptMismatch, validateRevocation(state, wrong));
    }
}
