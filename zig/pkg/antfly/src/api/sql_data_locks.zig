// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Native data-lock transport. Only private owner ingress can construct these
//! controls. Each accepting leader replaces caller-supplied metadata cuts with
//! a fresh linearizable observation before it appends an ordinary owner entry.
const std = @import("std");
const local = @import("antfly_local_sources");
const native = local.storage_pessimistic_lock_control;
const authority = @import("../system_catalog/lock_authority.zig");
const Server = @import("http_server.zig").ApiHttpServer;
const operation = local.api_operation;
const A = std.mem.Allocator;

/// The API kernel owns both metadata authorization and failed-command/session
/// custody. Data runtimes borrow this operation through the opaque API boundary.
pub const AuthorizationInput = struct {
    alloc: A,
    context: operation.RequestContext,
    group: u64,
    table: []const u8,
    command: native.Command,
};

pub fn authorize(server: *Server, a: A, context: operation.RequestContext, group: u64, table: []const u8, input: native.Command) !native.Command {
    try input.validate();
    if (group == 0 or input.admission.binding.namespace.group != group) return error.IdentityNamespaceMismatch;
    var result = input;
    if (input.retirement_control != null) {
        if (input.operation != .close) return error.InvalidLockScope;
        const work = try abandonedWork(server, a, context, input.admission, .{ .child = .{ .group = input.admission.source.group, .table = input.admission.binding.identity.table_id, .id = input.retirement_control.?.scope_id }, .generation = input.admission.generation });
        const canonical = work orelse return error.LockOwnerChanged;
        if (!std.mem.eql(u8, canonical.control.participant.spec.table_name, table) or canonical.control.route.route.group_id != group) return error.IdentityNamespaceMismatch;
        var supplied = input.admission;
        supplied.source.lease.expires_at_ms = canonical.control.participant.admission.source.lease.expires_at_ms;
        if (!std.meta.eql(supplied, canonical.control.participant.admission)) return error.LockOwnerChanged;
        return .{ .admission = canonical.control.participant.admission, .retirement_control = canonical.control, .operation = .close };
    }
    // Prepared commands are authorized by the receiving native frozen 2PC
    // reference. They cannot depend on a surviving connection or metadata IO.
    if (input.operation == .prepare or input.operation == .resolve) {
        // Already sealed controls are handled directly by the native owner.
        // Only a canonical ABORT may bootstrap a missing lock reference.
        if (input.native_control != null and input.operation == .resolve)
            try requireAbortedReference(server, a, context, table, input);
        result.checked = null;
        return result;
    }
    var control_context = context;
    control_context.setting_admin = true;
    control_context.sql_command_fence = null;
    control_context.sql_completion = null;
    control_context.sql_completion_submission = null;
    try control_context.ensureActive();
    const source = input.admission.source;
    const raw = try server.source.systemCatalog(a, control_context, .{ .sql_lock_read = .{
        .incarnation = source.lease.backend.incarnation,
        .standalone = source.standalone,
        .actor = .{ .session_user = .bootstrap, .credential = "" },
        .database = source.lease.database,
        .backend = source.lease.backend,
        .participant = .{ .generation = input.admission.generation },
    } });
    defer a.free(raw);
    if (raw.len > 32768) return error.InvalidSqlBackendResponse;
    const cut = try std.json.parseFromSliceLeaky(authority.Snapshot, a, raw, .{ .allocate = .alloc_always });
    if (cut.group != source.group or cut.standalone != source.standalone or !std.mem.eql(u8, &cut.incarnation, &source.lease.backend.incarnation)) return error.LockOwnerChanged;
    const enrolled = cut.participant orelse return error.LockOwnerChanged;
    if (!std.mem.eql(u8, enrolled.spec.table_name, table) or enrolled.spec.route.route.group_id != group) return error.IdentityNamespaceMismatch;
    result.checked = .{ .source = cut.scope orelse return error.LockOwnerChanged, .participant = enrolled, .admitted_at_ms = @intCast(@max(1, @divFloor(@import("antfly_platform").time.realtimeNs(), std.time.ns_per_ms))) };
    if (input.operation == .bound_control and input.operation.bound_control.failed_root) {
        const store = server.txn_sessions.durable orelse return error.SqlConnectionUnavailable;
        var session = (try store.loadInto(a, input.admission.transaction)) orelse return error.SqlTransactionNotActive;
        defer session.deinit(a);
        const metadata = session.sql orelse return error.SqlTransactionNotActive;
        if (!metadata.failed or session.current_subtransaction != 0 or session.external_preparation != null or session.commit_execution_started or
            session.connection_id == null or !std.mem.eql(u8, &session.connection_id.?, &source.connection)) return error.InvalidLockScope;
        var intent = (try @import("sql_command_producer.zig").load(store, a, source.connection)) orelse return error.SqlConnectionNotFound;
        defer intent.deinit();
        const effect = intent.value.control_effect orelse return error.InvalidLockScope;
        if (!std.meta.eql(intent.value.failure_control, @as(?[16]u8, input.admission.transaction)) or intent.value.ticket == null or
            !std.meta.eql(intent.value.ticket.?.command, input.admission.parent_command) or
            !std.mem.eql(u8, &effect.nonce, &input.operation.bound_control.nonce) or effect.action != .control or effect.action.control != .rollback or effect.action.control.rollback.first_aborted != 0) return error.InvalidLockScope;
        result.checked.?.failed_transaction = true;
    }
    try result.checked.?.validate(input.admission);
    if (input.operation != .close) try result.checked.?.requireExecuting(input.admission);
    return result;
}

pub fn submit(server: *Server, a: A, context: operation.RequestContext, table: []const u8, input: native.Command) !void {
    var attempt: u4 = 0;
    while (true) {
        try context.ensureActive();
        return submitOnce(server, a, context, table, input) catch |err| {
            // These owner-admission failures precede a proposal. Ambiguous
            // transport/proposal errors are never replayed here.
            if (!retryOwnerAdmission(err) or attempt == 8) return err;
            try pauseAdmission(server, context, attempt);
            attempt += 1;
            continue;
        };
    }
}

fn retryOwnerAdmission(err: anyerror) bool {
    return err == error.StorageBusy or err == error.StorageReadTemporarilyUnavailable;
}
fn pauseAdmission(server: *Server, context: operation.RequestContext, attempt: u4) !void {
    try context.ensureActive();
    try server.embedding_provider_runtime.io.sleep(.fromMilliseconds(@as(i64, 1) << @min(attempt, 4)), .awake);
    try context.ensureActive();
}

// Delivery follows the admitted participant, never a fresh SQL name or a
// nonexistent DataRaft leader for a native-local relation.
fn requireAbortedReference(server: *Server, a: A, context: operation.RequestContext, table: []const u8, input: native.Command) !void {
    const registry = @import("../system_catalog/prepared_transactions.zig");
    const control = input.native_control orelse return error.ExternalTransactionDecisionChanged;
    var private = controlContext(context);
    private.setting_admin = true;
    try private.ensureActive();
    const bytes = try server.source.systemCatalog(a, private, .{ .sql_prepared_read = .{ .operation = control.owner.operation_id } });
    defer a.free(bytes);
    if (bytes.len > registry.max_bytes + 4096) return error.InvalidSqlBackendResponse;
    var parsed = try std.json.parseFromSlice(registry.Snapshot, a, bytes, .{ .allocate = .alloc_always });
    defer parsed.deinit();
    if (parsed.value.records.len != 1 or !std.mem.eql(u8, &std.fmt.bytesToHex(parsed.value.incarnation, .lower), &control.owner.metadata_incarnation)) return error.ExternalTransactionDecisionChanged;
    const record = parsed.value.records[0];
    try record.validate(a);
    if (record.identity.session) |session| try session.requireRealm(parsed.value.standalone);
    const decision = record.decision orelse return error.CommitDecisionUnknown;
    if (decision.status != .aborted or !(try record.identity.owner(a)).eql(control.owner) or
        !std.mem.eql(u8, &record.identity.txn_id, &input.admission.transaction)) return error.ExternalTransactionDecisionChanged;
    for (record.participants) |part| {
        if (!std.mem.eql(u8, part.table_name, table)) continue;
        const ref = (try part.lockReference(record.identity, a)) orelse continue;
        if (!std.meta.eql(ref.admission, input.admission) or !std.meta.eql(ref.decision, control.decision)) continue;
        try @import("native_sql_lock_read.zig").requireSameRoute(part.route, control.route);
        return;
    }
    return error.ExternalTransactionDecisionChanged;
}

fn nativeRoute(server: *Server, a: A, route: local.metadata_catalog_route_contract.CatalogRouteFence) !?@import("table_router.zig").GroupRoute {
    if (route.route.execution != .native_local) return null;
    const host = route.route.execution.native_local.owner_node_id;
    const routes = @import("table_router.zig");
    var fallback = routes.CatalogBackedGroupRouter.init(server.catalogSource(), server.localSessionNodeId());
    const router = server.cfg.session_router orelse fallback.router();
    if (host == router.localNodeId()) return .local;
    return .{ .remote = .{ .node_id = host, .base_uri = (try router.nodeBaseUri(a, host)) orelse return error.GroupLeaderUnavailable } };
}
fn controlFence(checked_input: ?native.Checked, captured: ?native.NativeControl, retirement: ?native.RetirementControl) ?local.metadata_catalog_route_contract.CatalogRouteFence {
    if (retirement) |control| return control.route;
    if (captured) |control| return control.route;
    const checked = checked_input orelse return null;
    return checked.participant.spec.route;
}
fn nativeControlRoute(server: *Server, a: A, checked_input: ?native.Checked, captured: ?native.NativeControl, retirement: ?native.RetirementControl) !?@import("table_router.zig").GroupRoute {
    return nativeRoute(server, a, controlFence(checked_input, captured, retirement) orelse return null);
}

fn capturedReadSource(reads: local.api_table_read_source.TableReadSource, fence: ?local.metadata_catalog_route_contract.CatalogRouteFence, group: u64) !local.api_table_read_source.TableReadSource {
    var result = reads;
    if (fence) |value| if (value.route.execution == .native_local) {
        if (value.route.group_id != group) return error.IdentityNamespaceMismatch;
        result.route_fence = value;
    };
    return result;
}

fn submitOnce(server: *Server, a: A, context: operation.RequestContext, table: []const u8, input: native.Command) !void {
    const group = input.admission.binding.namespace.group;
    const checked = if (input.native_control != null) delivery: {
        // A serialized frozen control selects the receiving owner's private
        // probe, not an API-side ABORT adoption. Source checks the real current
        // image and stored Owner/Pin/Ref first; only a failed probe may invoke
        // authorize's canonical ABORT fallback. Ordinary sealed COMMIT must
        // reach that probe without being mistaken for missing-Ref adoption.
        try input.validate();
        break :delivery input;
    } else try authorize(server, a, context, group, table, input);
    const request: local.storage_db_types.BatchRequest = .{ .sql_lock = checked, .sync_level = .write };

    if (server.cfg.routed_raft_batch_writer) |writer| {
        const forwarding = @import("internal_batch_forwarding.zig");
        // The data leader independently authorizes this strict private shape.
        // Phase-two delivery cannot depend on a mutable catalog revision.
        _ = (try writer.write(a, .transaction, group, table, request, .{
            .remaining_ms = @min((try context.remainingTimeoutMs()) orelse forwarding.max_remaining_ms, forwarding.max_remaining_ms),
            .forwards_remaining = forwarding.max_forwards,
            .campaign_allowed = false,
        }, context)) orelse return error.GroupLeaderUnavailable;
    } else if (server.cfg.deployment_mode != .standalone) {
        // Metadata/compute ingress does not own a data Raft writer. Its
        // group-local kernel cache is not authority to mutate a replica.
        // Route the strict private control to the accepting data leader,
        // exactly as observe routes its linearizable readback.
        const routes = @import("table_router.zig");
        var fallback = routes.CatalogBackedGroupRouter.init(server.catalogSource(), server.localSessionNodeId());
        var route = (try nativeControlRoute(server, a, checked.checked, checked.native_control, checked.retirement_control)) orelse (try routes.resolveGroupRoute(a, server.catalogSource(), server.cfg.session_router orelse fallback.router(), group, .prefer_leader)) orelse return error.GroupLeaderUnavailable;
        defer route.deinit(a);
        if (route != .remote) return error.GroupLeaderUnavailable;
        const encoded = try local.api_batch.encodeBatchRequest(a, request);
        defer a.free(encoded);
        var client = @import("http_client.zig").ApiHttpClient.init(a, server.cfg.session_executor orelse return error.UnsupportedOperation);
        _ = client.withInternalServiceAuth(server.cfg.internal_service_secret, server.cfg.internal_service_issuer);
        var cancellation = @import("../raft/transport/http_common.zig").RequestCancellation.fromToken(context.cancellation);
        const forwarding = @import("internal_batch_forwarding.zig");
        const remaining = @min((try context.remainingTimeoutMs()) orelse forwarding.max_remaining_ms, forwarding.max_remaining_ms);
        var response = try client.fetchGroupBatchWithForwarding(route.remote.base_uri, group, table, encoded, remaining, .{ .remaining_ms = remaining, .forwards_remaining = forwarding.max_forwards, .campaign_allowed = false }, &cancellation, null);
        defer response.deinit(a);
    } else {
        const writes = server.table_writes orelse return error.UnsupportedOperation;
        _ = (try writes.batchGroupLocal(a, group, table, request)) orelse return error.GroupLeaderUnavailable;
    }
}

pub fn observe(server: *Server, a: A, context: operation.RequestContext, table: []const u8, query: native.Query) !native.Snapshot {
    var attempt: u4 = 0;
    while (true) {
        try context.ensureActive();
        return observeOnce(server, a, context, table, query) catch |err| {
            // Resume only the immutable native query, not SQL evaluation or
            // effect allocation. Owner publication can briefly fence a read
            // immediately after the preceding lock effect was committed.
            const operational = @import("table_reads.zig").normalizeDistributedQueryOperationalError(err);
            if ((!retryOwnerAdmission(err) and operational != error.DistributedQueryUnavailable) or attempt == 8) return err;
            try pauseAdmission(server, context, attempt);
            attempt += 1;
            continue;
        };
    }
}

fn observeOnce(server: *Server, a: A, context: operation.RequestContext, table: []const u8, query: native.Query) !native.Snapshot {
    try context.ensureActive();
    // Readback has no lock effect; the closed-scope observation uses the same
    // metadata-authorized participant directory as cleanup, without requiring
    // an executing user statement or borrowing its connection owner.
    const admitted = if (query.admission.purpose == .execution) try authorize(server, a, context, query.admission.binding.namespace.group, table, .{ .admission = query.admission, .retirement_control = query.retirement_control, .operation = .close }) else null;
    var canonical_query = query;
    if (admitted) |value| if (query.retirement_control != null) {
        canonical_query.admission = value.admission;
        canonical_query.retirement_control = value.retirement_control;
    };
    const raw = try std.json.Stringify.valueAlloc(a, canonical_query, .{});
    defer a.free(raw);
    const group = query.admission.binding.namespace.group;
    var fence = controlFence(if (admitted) |value| value.checked else null, canonical_query.native_control, canonical_query.retirement_control);
    // Fresh owning commands may carry their existing trusted process-local
    // signer. Recovery controls without a live user keep their independent
    // canonical body authority; never reconstruct an Owner from that body.
    const minter = if (context.sql_relation_owner != null and fence != null and fence.?.route.execution == .native_local)
        try @import("sql_relation_lifetimes.zig").ownerMinterForRelation(server, context, fence.?.route.execution.native_local)
    else
        null;
    if (minter) |signer| {
        fence.?.admission_sql_relation_owner = signer.owner;
        fence.?.admission_relation_minter = signer;
    }
    // Reuse the authenticated participant's exact Boot/Pin route. A native
    // owner has no DataRaft read-index; routed admission validates its actual
    // initialized image before the ordinary serialized primary read.
    const reads = try capturedReadSource(server.table_reads orelse return error.UnsupportedOperation, fence, group);
    if (server.cfg.deployment_mode != .standalone) {
        const routes = @import("table_router.zig");
        var fallback = routes.CatalogBackedGroupRouter.init(server.catalogSource(), server.localSessionNodeId());
        var route = (try nativeControlRoute(server, a, if (admitted) |value| value.checked else null, query.native_control, query.retirement_control)) orelse (try routes.resolveGroupRoute(a, server.catalogSource(), server.cfg.session_router orelse fallback.router(), group, .prefer_leader)) orelse return error.GroupLeaderUnavailable;
        defer route.deinit(a);
        if (route == .remote) {
            var client = @import("http_client.zig").ApiHttpClient.init(a, server.cfg.session_executor orelse return error.UnsupportedOperation);
            _ = client.withInternalServiceAuth(server.cfg.internal_service_secret, server.cfg.internal_service_issuer);
            if (minter != null) client.relation_owner_route = fence;
            var cancellation = @import("../raft/transport/http_common.zig").RequestCancellation.fromToken(context.cancellation);
            var response = try client.fetchGroupLookupWithOptions(route.remote.base_uri, group, table, "", null, "read_index", try context.remainingTimeoutMs(), &cancellation, .{ .sql_lock_query_json = raw, .catalog_route_fence = if (fence) |value| if (value.route.execution == .native_local) value else null else null });
            defer response.deinit(a);
            if (response.body.len > 65536) return error.InvalidSqlBackendResponse;
            return std.json.parseFromSliceLeaky(native.Snapshot, a, response.body, .{ .allocate = .alloc_always });
        }
    }
    var response = (try reads.lookupGroupLocal(a, group, table, "", .{ .relation_owner_minter = minter, .sql_lock_query_json = raw, .execution_deadline_ns = (try context.platformDeadline()).deadline_ns, .cancellation = context.cancellation }, .read_index)) orelse return error.GroupLeaderUnavailable;
    defer response.deinit(a);
    if (response.json.len > 65536) return error.InvalidSqlBackendResponse;
    return std.json.parseFromSliceLeaky(native.Snapshot, a, response.json, .{ .allocate = .alloc_always });
}

/// One native mutation or one metadata acknowledgement per pass. This is also
/// called by the existing source supervisor after a lost client; no SQL body,
/// selected row image, or in-memory participant cache is needed for recovery.
pub fn beforeMetadataMutation(server: *Server, backing: A, context: operation.RequestContext, request: authority.Request) !bool {
    var region = std.heap.ArenaAllocator.init(backing);
    defer region.deinit();
    const a = region.allocator();
    const finishing = request.operation == .finish;
    const retiring = request.operation == .release or request.operation == .release_unadmitted or request.operation == .close_step;
    const control_effect: ?authority.scopes.ledger.Effect = switch (request.operation) {
        .effect, .step => |value| if (value.effect.action == .control and (value.effect.action.control == .rollback or value.effect.action.control == .release_savepoint)) value.effect else null,
        else => null,
    };
    if (!finishing and !retiring and control_effect == null) return true;
    var private = context;
    private.setting_admin = true;
    private.sql_command_fence = null;
    private.sql_completion = null;
    private.sql_completion_submission = null;
    const query: authority.Query = .{ .incarnation = request.incarnation, .standalone = request.standalone, .actor = .{ .session_user = .bootstrap, .credential = "" }, .database = request.database, .backend = request.backend };
    const raw = try server.source.systemCatalog(a, private, .{ .sql_lock_read = query });
    defer a.free(raw);
    const root = try std.json.parseFromSliceLeaky(authority.Snapshot, a, raw, .{ .allocate = .alloc_always });
    const scope = root.scope orelse return true;
    const directory = scope.data orelse return true;
    if (control_effect) |effect| return savepointQuantum(server, a, context, request, query, directory, effect);
    if (directory.count() == 0 or (finishing and directory.open == 0)) return true;
    // Resolution first records its independently checked terminal intent in
    // metadata. Only a subsequent pass may release native transaction grants.
    if (retiring and scope.state == .active and !scope.releasing) return true;
    for (directory.slots) |generation| {
        if (generation == 0) continue;
        var selection = query;
        selection.participant = .{ .generation = generation };
        const part_raw = try server.source.systemCatalog(a, private, .{ .sql_lock_read = selection });
        defer a.free(part_raw);
        const selected = try std.json.parseFromSliceLeaky(authority.Snapshot, a, part_raw, .{ .allocate = .alloc_always });
        const part = selected.participant orelse return error.InvalidSqlBackendResponse;
        if (finishing and !part.command_open) continue;
        if (finishing and !std.meta.eql(part.admission.parent_command, request.operation.finish)) return error.LockEffectChanged;
        if (retiring and part.spec.route.route.execution == .native_local) {
            if (try @import("sql_native_recovery.zig").retiredGeneration(a, server.catalogSource(), part.spec.route, private)) |_| {
                const decision = if (scope.transaction) |transaction| transaction.decision else if (scope.released_transaction) |transaction| transaction.decision else null;
                try submitLostDirectory(server, a, private, query, selected, part.spec.route, generation, decision, false);
                return false;
            }
        }
        const state = try observe(server, a, private, part.spec.table_name, .{ .admission = part.admission });
        if (finishing) {
            if (state.scope == null) {
                try submit(server, a, private, part.spec.table_name, .{ .admission = part.admission, .operation = .begin });
                return false;
            }
            if (state.owner) |owner| if (owner.waiting) |effect| {
                try submit(server, a, private, part.spec.table_name, .{ .admission = part.admission, .operation = .{ .cancel_wait = effect } });
                return false;
            };
            if (!std.meta.eql(state.scope.?.completed, @as(?authority.scopes.Command, try part.admission.command()))) {
                try submit(server, a, private, part.spec.table_name, .{ .admission = part.admission, .operation = .finish });
                return false;
            }
        } else if (state.scope == null or state.scope.?.state != .retired) {
            try submit(server, a, private, part.spec.table_name, .{ .admission = part.admission, .operation = .close });
            return false;
        }
        var nonce: [16]u8 = undefined;
        try server.embedding_provider_runtime.io.randomSecure(&nonce);
        var ack = request;
        ack.admission = .{ .head = selected.head, .nonce = nonce };
        ack.actor = query.actor;
        ack.operation = if (finishing)
            .{ .data_ack = .{ .generation = generation, .command = part.admission.parent_command, .native = state } }
        else
            .{ .data_retire = .{ .generation = generation, .native = state } };
        const acknowledged = try server.source.systemCatalog(a, private, .{ .sql_lock_mutate = ack });
        defer a.free(acknowledged);
        const outcome = try std.json.parseFromSliceLeaky(?authority.outcomes.Outcome, a, acknowledged, .{});
        _ = try (outcome orelse return error.MetadataMutationOutcomeUnknown).result();
        return false;
    }
    return true;
}

fn controlContext(context: operation.RequestContext) operation.RequestContext {
    var result = context;
    result.setting_admin = true;
    result.sql_command_fence = null;
    result.sql_completion = null;
    result.sql_completion_submission = null;
    return result;
}
fn metadataSnapshot(server: *Server, a: A, context: operation.RequestContext, query: authority.Query) !authority.Snapshot {
    var private = controlContext(context);
    if (query.abandoned_private != null) private.schema_validation_authority = true;
    const bytes = try server.source.systemCatalog(a, private, .{ .sql_lock_read = query });
    defer a.free(bytes);
    return std.json.parseFromSliceLeaky(authority.Snapshot, a, bytes, .{ .allocate = .alloc_always });
}

/// Freeze every native lock participant into the same manifest as row intents,
/// including tables that this SQL transaction only locked. Directory state is
/// authoritative; request-local caches cannot define a recoverable cohort.
pub fn attachPreparation(server: *Server, a: A, context: operation.RequestContext, id: [16]u8, preparation: *@import("distributed_txn.zig").ExternalPreparation) !void {
    const frame = context.sql_command_fence orelse return;
    const query: authority.Query = .{ .incarnation = frame.backend.incarnation, .standalone = frame.standalone, .actor = .{ .session_user = .bootstrap, .credential = "" }, .database = frame.database, .backend = frame.backend };
    var cut = try metadataSnapshot(server, a, context, query);
    if (cut.scope) |scope| if (scope.data) |directory| if (directory.open != 0) {
        // Autocommit LockRows reaches COMMIT inside its executing command.
        // Close only the native data commands after evaluation; the metadata
        // SQL command stays active through decision admission and completion.
        var nonce: [16]u8 = undefined;
        try server.embedding_provider_runtime.io.randomSecure(&nonce);
        const finish: authority.Request = .{ .admission = .{ .head = cut.head, .nonce = nonce }, .incarnation = frame.backend.incarnation, .standalone = frame.standalone, .actor = query.actor, .database = frame.database, .backend = frame.backend, .operation = .{ .finish = frame.command } };
        var quanta: u8 = 0;
        while (!try beforeMetadataMutation(server, a, context, finish)) {
            try context.ensureActive();
            quanta += 1;
            if (quanta == 8) {
                try server.embedding_provider_runtime.io.sleep(.fromMilliseconds(1), .awake);
                quanta = 0;
            }
        }
        cut = try metadataSnapshot(server, a, context, query);
    };
    const source = cut.scope orelse return error.LockOwnerChanged;
    const directory = source.data orelse return;
    if (!std.meta.eql(source.command, @as(?authority.scopes.Command, frame.command)) or source.releasing) return error.LockOwnerChanged;
    for (directory.slots) |generation| {
        if (generation == 0) continue;
        var selection = query;
        selection.participant = .{ .generation = generation };
        const part = (try metadataSnapshot(server, a, context, selection)).participant orelse return error.InvalidSqlBackendResponse;
        if (!std.mem.eql(u8, &part.admission.transaction, &id)) continue;
        if (part.command_open) return error.LockOwnerBusy;
        var routing = (try @import("table_catalog.zig").authoritativeTableRoutingSnapshot(a, server.catalogSource(), part.spec.table_name, context.deadline_ns)) orelse return error.TableNotFound;
        defer routing.deinit(a);
        if (routing.table_id != part.spec.route.table_id or routing.topology_epoch != part.spec.route.topology_epoch) return error.CatalogGenerationChanged;
        var found = false;
        for (routing.ranges) |range| {
            if (range.group_id != part.spec.route.route.group_id) continue;
            const route = routing.resolveRouteForKey(range.start_key) orelse return error.TopologyChanged;
            if (!std.meta.eql(route, part.spec.route.route)) return error.CatalogGenerationChanged;
            var spec = part.spec;
            spec.route = routing.fenceForRoute(route);
            if (route.execution == .native_local) {
                spec.route.native_participant = part.spec.route.native_participant orelse return error.SqlRelationRecoveryIncomplete;
                const catalog = server.catalogSource();
                try catalog.pinNativeParticipant(a, &spec.route, catalog.deadlineFrom(.{ .deadline_ns = context.deadline_ns, .io = context.deadline_io }));
                spec.route.admission_relation_minter = try @import("sql_relation_lifetimes.zig").ownerMinterForRelation(server, context, route.execution.native_local);
                spec.route.admission_sql_relation_owner = context.sql_relation_owner;
            }
            var admission = part.admission;
            admission.purpose = .resolution;
            admission.serial = try std.math.add(u64, admission.serial, 1);
            admission.parent_command = frame.command;
            admission.source = source.registration;
            try preparation.attachLocks(a, spec, admission);
            found = true;
            break;
        }
        if (!found) return error.TopologyChanged;
    }
}

fn diagnoseFrozenReferenceMismatch() void {
    if (comptime @import("builtin").mode == .debug) {
        const Trace = struct {
            var reported: std.atomic.Value(bool) = .init(false);
        };
        if (!Trace.reported.swap(true, .monotonic))
            std.debug.print("SQL lock recovery rejected reason=frozen_ref_mismatch\n", .{});
    }
}

/// A pass performs at most one native WAL mutation or metadata directory
/// handoff. The immutable registry and native BEGIN retain the complete proof
/// across deadlines, source expiry, lost responses and cold recovery.
pub fn transactionQuantum(server: *Server, backing: A, context: operation.RequestContext, record: @import("../system_catalog/prepared_transactions.zig").Record, resolving: ?u64) !bool {
    var region = std.heap.ArenaAllocator.init(backing);
    defer region.deinit();
    const a = region.allocator();
    const private = controlContext(context);
    try private.ensureActive();
    const owner = try record.identity.owner(a);
    for (record.participants, 0..) |part, index| {
        if (resolving) |members| if (members & (@as(u64, 1) << @intCast(index)) == 0) continue;
        const ref = (try part.lockReference(record.identity, a)) orelse continue;
        const native_control: ?native.NativeControl = if (part.route.route.execution == .native_local) .{ .route = part.route, .owner = owner, .decision = ref.decision } else null;
        if (part.route.route.execution == .native_local) {
            if (try @import("sql_native_recovery.zig").retiredGeneration(a, server.catalogSource(), part.route, private)) |_| {
                if (resolving != null) {
                    const actual = try @import("sql_native_recovery.zig").terminalDecision(a, server.source, private, part.table_name, part.route, record.identity.txn_id, owner);
                    if (!std.meta.eql(actual, record.decision orelse return error.ExternalTransactionDecisionChanged)) return error.ExternalTransactionDecisionChanged;
                }
                const query: authority.Query = .{ .incarnation = ref.admission.source.lease.backend.incarnation, .standalone = ref.admission.source.standalone, .actor = .{ .session_user = .bootstrap, .credential = "" }, .database = ref.admission.source.lease.database, .backend = ref.admission.source.lease.backend };
                const metadata = try metadataSnapshot(server, a, private, query);
                if (metadata.scope) |source| if (source.data) |directory| if (std.mem.indexOfScalar(u64, &directory.slots, ref.admission.generation) != null) {
                    try submitLostDirectory(server, a, private, query, metadata, part.route, ref.admission.generation, ref.decision, resolving == null);
                    return false;
                };
                // An absent slot is an already-completed directory handoff;
                // canonical loss, not a fabricated native Snapshot, proves no
                // physical grant from this exact generation can remain.
                continue;
            }
        }
        const cut = observe(server, a, private, part.table_name, .{ .admission = ref.admission, .native_control = native_control }) catch |err| {
            if (err != error.ExternalTransactionDecisionChanged or native_control == null or resolving == null or record.decision == null or record.decision.?.status != .aborted) return err;
            // The receiving source rejoins this canonical ABORT before the
            // existing same-writer adoption. This is progress, not success:
            // the next quantum must observe the real sealed terminal fact.
            try submit(server, a, private, part.table_name, .{ .admission = ref.admission, .native_control = native_control, .operation = .{ .resolve = ref.decision } });
            return false;
        };
        if (resolving != null) {
            // Absence before BEGIN is handled by the source's checked abort
            // retirement after this native transaction has durably aborted.
            if (cut.frozen) |frozen| {
                if (!std.meta.eql(frozen.ref, ref)) {
                    diagnoseFrozenReferenceMismatch();
                    return error.ExternalTransactionDecisionChanged;
                }
                if (frozen.retired) {
                    // The native Ref is terminal before this metadata slot
                    // can disappear. Detach its exact retained generation
                    // before publishing native_locks_released or handoff;
                    // later source completion must not need to reopen it.
                    const query: authority.Query = .{ .incarnation = ref.admission.source.lease.backend.incarnation, .standalone = ref.admission.source.standalone, .actor = .{ .session_user = .bootstrap, .credential = "" }, .database = ref.admission.source.lease.database, .backend = ref.admission.source.lease.backend };
                    const metadata = try metadataSnapshot(server, a, private, query);
                    const source = metadata.scope orelse return error.LockOwnerChanged;
                    if (source.data) |directory| if (std.mem.indexOfScalar(u64, &directory.slots, ref.admission.generation) != null) {
                        var nonce: [16]u8 = undefined;
                        try server.embedding_provider_runtime.io.randomSecure(&nonce);
                        const request: authority.Request = .{ .admission = .{ .head = metadata.head, .nonce = nonce }, .incarnation = query.incarnation, .standalone = query.standalone, .actor = query.actor, .database = query.database, .backend = query.backend.?, .operation = .{ .data_retire = .{ .generation = ref.admission.generation, .native = cut } } };
                        const raw = try server.source.systemCatalog(a, private, .{ .sql_lock_mutate = request });
                        const result = (try std.json.parseFromSliceLeaky(?authority.outcomes.Outcome, a, raw, .{})) orelse return error.MetadataMutationOutcomeUnknown;
                        _ = try result.result();
                        return false;
                    };
                    continue;
                }
            } else if (cut.scope) |scope| {
                if (scope.state == .retired and cut.owner != null and cut.owner.?.state == .retired and cut.owner.?.resources == 0) {
                    const released = scope.released_transaction orelse return error.InvalidLockRecord;
                    if (record.decision.?.status != .aborted or !std.mem.eql(u8, &released.id, &ref.admission.transaction) or
                        (released.decision != null and !std.meta.eql(released.decision.?, ref.decision))) return error.InvalidLockRecord;
                    continue;
                }
            } else {
                // An absent native source admitted no grant. The native ABORT
                // owner still fences any delayed BEGIN through the usual 2PC
                // expiry/terminal retention rules.
                if (record.decision.?.status != .aborted) return error.InvalidLockRecord;
                continue;
            }
            try submit(server, a, private, part.table_name, .{ .admission = ref.admission, .native_control = native_control, .operation = .{ .resolve = ref.decision } });
            return false;
        }
        const frozen = cut.frozen orelse return error.ExternalTransactionDecisionChanged;
        if (!std.meta.eql(frozen.ref, ref)) {
            diagnoseFrozenReferenceMismatch();
            return error.ExternalTransactionDecisionChanged;
        }
        if (cut.prepared_owner == null or cut.prepared_owner.?.state != .prepared or cut.scope == null or cut.scope.?.transaction != null) {
            try submit(server, a, private, part.table_name, .{ .admission = ref.admission, .native_control = native_control, .operation = .{ .prepare = ref.decision } });
            return false;
        }
        // Native custody precedes directory detachment. The accepting metadata
        // writer independently joins this proof to its admitted manifest.
        const query: authority.Query = .{ .incarnation = ref.admission.source.lease.backend.incarnation, .standalone = ref.admission.source.standalone, .actor = .{ .session_user = .bootstrap, .credential = "" }, .database = ref.admission.source.lease.database, .backend = ref.admission.source.lease.backend };
        const metadata = try metadataSnapshot(server, a, private, query);
        const root = metadata.scope orelse return error.InvalidSqlBackendResponse;
        if (root.data) |directory| if (std.mem.indexOfScalar(u64, &directory.slots, ref.admission.generation) != null) {
            var nonce: [16]u8 = undefined;
            try server.embedding_provider_runtime.io.randomSecure(&nonce);
            const request: authority.Request = .{ .admission = .{ .head = metadata.head, .nonce = nonce }, .incarnation = query.incarnation, .standalone = query.standalone, .actor = query.actor, .database = query.database, .backend = query.backend.?, .operation = .{ .data_handoff = .{ .generation = ref.admission.generation, .native = cut } } };
            const bytes = try server.source.systemCatalog(a, private, .{ .sql_lock_mutate = request });
            const outcome = (try std.json.parseFromSliceLeaky(?authority.outcomes.Outcome, a, bytes, .{})) orelse return error.MetadataMutationOutcomeUnknown;
            _ = try outcome.result();
            return false;
        };
    }
    return true;
}

fn submitLostDirectory(server: *Server, a: A, context: operation.RequestContext, query: authority.Query, observed: authority.Snapshot, route: local.metadata_catalog_route_contract.CatalogRouteFence, generation: u64, decision: ?authority.scopes.DecisionRef, handoff: bool) !void {
    const pin = route.native_participant orelse return error.InvalidLockScope;
    var nonce: [16]u8 = undefined;
    try server.embedding_provider_runtime.io.randomSecure(&nonce);
    const request: authority.Request = .{ .admission = .{ .head = observed.head, .nonce = nonce }, .incarnation = query.incarnation, .standalone = query.standalone, .actor = query.actor, .database = query.database, .backend = query.backend.?, .operation = .{ .native_retire = .{ .generation = generation, .source_group_id = route.route.group_id, .query = .{ .table_id = route.table_id, .physical_pin = pin }, .decision = decision, .handoff = handoff } } };
    const raw = try server.source.systemCatalog(a, context, .{ .sql_lock_mutate = request });
    const outcome = (try std.json.parseFromSliceLeaky(?authority.outcomes.Outcome, a, raw, .{})) orelse return error.MetadataMutationOutcomeUnknown;
    _ = try outcome.result();
}

const SavepointDeliveryPhase = enum { metadata, routing, pin, enroll, observe, begin, cancel_wait, receipt, control };

fn savepointQuantum(server: *Server, a: A, context: operation.RequestContext, parent: authority.Request, query: authority.Query, directory: authority.scopes.Data.Header, effect: authority.scopes.ledger.Effect) !bool {
    var phase: SavepointDeliveryPhase = .metadata;
    return savepointQuantumChecked(server, a, context, parent, query, directory, effect, &phase) catch |err| {
        if (comptime @import("builtin").mode == .debug) {
            const Once = struct {
                var reported: std.atomic.Value(bool) = .init(false);
            };
            if (!Once.reported.swap(true, .monotonic))
                std.debug.print("native savepoint delivery phase={s} owner={} err={s}\n", .{ @tagName(phase), context.sql_relation_owner != null, @errorName(err) });
        }
        return err;
    };
}

fn savepointQuantumChecked(server: *Server, a: A, context: operation.RequestContext, parent: authority.Request, query: authority.Query, directory: authority.scopes.Data.Header, effect: authority.scopes.ledger.Effect, phase: *SavepointDeliveryPhase) !bool {
    const private = controlContext(context);
    const command = switch (parent.operation) {
        .effect, .step => |value| value.command,
        else => return error.InvalidLockEffect,
    };
    for (directory.slots) |generation| {
        if (generation == 0) continue;
        var selection = query;
        selection.participant = .{ .generation = generation };
        phase.* = .metadata;
        const metadata = try metadataSnapshot(server, a, private, selection);
        const part = metadata.participant orelse return error.InvalidSqlBackendResponse;
        if (!std.meta.eql(part.admission.parent_command, command)) {
            phase.* = .routing;
            var routing = (try @import("table_catalog.zig").authoritativeTableRoutingSnapshot(a, server.catalogSource(), part.spec.table_name, context.deadline_ns)) orelse return error.TableNotFound;
            defer routing.deinit(a);
            if (routing.table_id != part.spec.route.table_id or routing.topology_epoch != part.spec.route.topology_epoch) return error.CatalogGenerationChanged;
            var spec = part.spec;
            var found = false;
            for (routing.ranges) |range| {
                if (range.group_id != part.spec.route.route.group_id) continue;
                const route = routing.resolveRouteForKey(range.start_key) orelse return error.TopologyChanged;
                if (!std.meta.eql(route, part.spec.route.route)) return error.CatalogGenerationChanged;
                spec.route = routing.fenceForRoute(route);
                if (route.execution == .native_local) {
                    spec.route.native_participant = part.spec.route.native_participant orelse return error.SqlRelationRecoveryIncomplete;
                    const catalog = server.catalogSource();
                    phase.* = .pin;
                    try catalog.pinNativeParticipant(a, &spec.route, catalog.deadlineFrom(.{ .deadline_ns = context.deadline_ns, .io = context.deadline_io }));
                }
                found = true;
                break;
            }
            if (!found) return error.TopologyChanged;
            var request = parent;
            var nonce: [16]u8 = undefined;
            try server.embedding_provider_runtime.io.randomSecure(&nonce);
            request.admission = .{ .head = metadata.head, .nonce = nonce };
            request.operation = .{ .data_enroll = .{ .command = command, .transaction = part.admission.transaction, .spec = spec } };
            phase.* = .enroll;
            const bytes = try server.source.systemCatalog(a, private, .{ .sql_lock_mutate = request });
            const outcome = (try std.json.parseFromSliceLeaky(?authority.outcomes.Outcome, a, bytes, .{})) orelse return error.MetadataMutationOutcomeUnknown;
            _ = try outcome.result();
            return false;
        }
        phase.* = .observe;
        const cut = try observe(server, a, private, part.spec.table_name, .{ .admission = part.admission, .command_control = true });
        if (cut.scope == null or !std.meta.eql(cut.scope.?.command, @as(?authority.scopes.Command, try part.admission.command()))) {
            phase.* = .begin;
            try submit(server, a, private, part.spec.table_name, .{ .admission = part.admission, .operation = .begin });
            return false;
        }
        if (cut.owner) |owner| if (owner.waiting) |waiting| {
            phase.* = .cancel_wait;
            try submit(server, a, private, part.spec.table_name, .{ .admission = part.admission, .operation = .{ .cancel_wait = waiting } });
            return false;
        };
        if (cut.bound_control) |known| {
            if (!std.mem.eql(u8, &known.nonce, &effect.nonce) or known.action != .control or !std.meta.eql(known.action.control, effect.action.control)) return error.LockEffectChanged;
            phase.* = .receipt;
            const receipt = (try observe(server, a, private, part.spec.table_name, .{ .admission = part.admission, .effect = known })).receipt orelse return error.InvalidSqlBackendResponse;
            if (receipt.outcome == .completed) continue;
            if (receipt.outcome != .pending) return error.InvalidSqlBackendResponse;
        }
        phase.* = .control;
        try submit(server, a, private, part.spec.table_name, .{ .admission = part.admission, .operation = .{ .bound_control = .{ .nonce = effect.nonce, .control = effect.action.control, .failed_root = effect.action.control == .rollback and effect.action.control.rollback.first_aborted == 0 } } });
        return false;
    }
    return true;
}

fn abandonedWork(server: *Server, a: A, context: operation.RequestContext, admission: @FieldType(native.Command, "admission"), selection: @import("../metadata/abandoned_temporary_retirement.zig").Selection) !?@import("../metadata/abandoned_temporary_retirement.zig").Work {
    const query: authority.Query = .{ .incarnation = admission.source.lease.backend.incarnation, .standalone = admission.source.standalone, .actor = .{ .session_user = .bootstrap, .credential = "" }, .database = admission.source.lease.database, .abandoned_private = selection };
    const snapshot = try metadataSnapshot(server, a, context, query);
    return snapshot.abandoned_private;
}

/// Bound to one archived old directory participant and one actual child source
/// fence. One native mutation or scoped metadata completion per call.
pub fn abandonedPrivateQuantum(server: *Server, a: A, context: operation.RequestContext, child: @import("../metadata/table_retirement_ledger.zig").Head) !bool {
    if (child.scope.parent == null) return true;
    var incarnation: [16]u8 = undefined;
    _ = try std.fmt.hexToBytes(&incarnation, &child.incarnation);
    const first_query: authority.Query = .{ .incarnation = incarnation, .standalone = server.cfg.deployment_mode == .standalone, .actor = .{ .session_user = .bootstrap, .credential = "" }, .database = child.scope.database, .abandoned_private = .{ .child = child.lookup } };
    const cut = try metadataSnapshot(server, a, context, first_query);
    const work = cut.abandoned_private orelse return true;
    const part = work.control.participant;
    var selected = first_query.abandoned_private.?;
    selected.generation = part.admission.generation;
    const observed = try observe(server, a, context, part.spec.table_name, .{ .admission = part.admission, .retirement_control = work.control });
    if (observed.scope == null or observed.scope.?.state != .retired or observed.owner == null or observed.owner.?.state != .retired) {
        if (observed.frozen) |frozen| {
            selected.decision = frozen.ref.decision;
            const canonical = (try abandonedWork(server, a, context, part.admission, selected)) orelse return error.LockOwnerChanged;
            const captured = canonical.prepared_control orelse return error.CatalogPublicationProofPending;
            try submit(server, a, context, part.spec.table_name, .{ .admission = canonical.prepared_admission orelse return error.InvalidLockScope, .native_control = captured, .operation = .{ .resolve = captured.decision } });
        } else {
            try submit(server, a, context, part.spec.table_name, .{ .admission = part.admission, .retirement_control = work.control, .operation = .close });
        }
        return false;
    }
    var nonce: [16]u8 = undefined;
    try server.embedding_provider_runtime.io.randomSecure(&nonce);
    const request: authority.Request = .{ .abandoned_private = selected, .admission = .{ .head = cut.head, .nonce = nonce }, .incarnation = first_query.incarnation, .standalone = first_query.standalone, .database = first_query.database, .actor = first_query.actor, .backend = part.admission.source.lease.backend, .operation = .{ .data_retire = .{ .generation = part.admission.generation, .native = observed } } };
    const raw = try server.source.systemCatalog(a, controlContext(context), .{ .sql_lock_mutate = request });
    const outcome = try std.json.parseFromSliceLeaky(?authority.outcomes.Outcome, a, raw, .{});
    _ = try (outcome orelse return error.MetadataMutationOutcomeUnknown).result();
    return false;
}

test "SQL native lock observation retains captured group independently of shard without Raft fallback" {
    const reads = local.api_table_read_source;
    const Probe = struct {
        calls: usize = 0,
        fn plain(_: *anyopaque, _: A, _: u64, _: []const u8, _: []const u8, _: local.storage_db_types.LookupOptions, _: local.storage_read_consistency.ReadConsistency) !?reads.LookupResponse {
            return error.UnknownGroup;
        }
        fn routed(raw: *anyopaque, _: A, fence: local.metadata_catalog_route_contract.CatalogRouteFence, group: u64, _: []const u8, _: []const u8, options: local.storage_db_types.LookupOptions, consistency: local.storage_read_consistency.ReadConsistency) !?reads.LookupResponse {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            try std.testing.expectEqual(@as(u64, 72), group);
            try std.testing.expectEqual(@as(u64, 172), fence.route.identity_namespace.shard_id);
            try std.testing.expect(fence.route.execution == .native_local);
            try std.testing.expectEqualStrings("lock-control", options.sql_lock_query_json);
            try std.testing.expectEqual(.read_index, consistency);
            return error.TestCapturedNativeRouteReached;
        }
    };
    var probe: Probe = .{};
    const source: reads.TableReadSource = .{ .ptr = &probe, .vtable = &.{ .lookup = undefined, .scan = undefined, .query = undefined, .lookup_group_local = Probe.plain, .lookup_group_local_routed = Probe.routed } };
    const fence: local.metadata_catalog_route_contract.CatalogRouteFence = .{ .metadata_group_id = 1, .catalog_revision = 2, .table_id = 3, .topology_epoch = 4, .route = .{ .group_id = 72, .range_id = 272, .identity_namespace = .{ .table_id = 3, .shard_id = 172, .range_id = 272 }, .execution = .{ .native_local = .{ .kind = .unlogged, .owner_node_id = 7, .owner_store_id = 9, .generation_instance = @splat(3), .data_generation = 4 } } } };
    const routed = try capturedReadSource(source, fence, 72);
    try std.testing.expectError(error.TestCapturedNativeRouteReached, routed.lookupGroupLocal(std.testing.allocator, 72, "docs", "", .{ .sql_lock_query_json = "lock-control" }, .read_index));
    try std.testing.expectEqual(@as(usize, 1), probe.calls);
    try std.testing.expectError(error.IdentityNamespaceMismatch, capturedReadSource(source, fence, 172));
    const ordinary = try capturedReadSource(source, null, 72);
    try std.testing.expectError(error.UnknownGroup, ordinary.lookupGroupLocal(std.testing.allocator, 72, "docs", "", .{}, .read_index));
    try std.testing.expectEqual(@as(usize, 1), probe.calls);
}

test "SQL relation lifetime native frozen lock delivery reaches the receiving owner before ABORT fallback" {
    const P = local.common_topology_records.RelationPersistence;
    const writes = @import("table_writes.zig");
    const Probe = struct {
        deliveries: usize = 0,
        metadata_reads: usize = 0,
        expected: native.Command,
        fn batch(_: *anyopaque, _: A, _: []const u8, _: local.storage_db_types.BatchRequest) !?void {
            return error.TestUnexpectedGenericWrite;
        }
        fn status(_: *anyopaque) !@import("../metadata/api.zig").MetadataStatus {
            return error.TestUnexpectedMetadataAuthorization;
        }
        fn catalog(raw: *anyopaque, _: A, _: operation.RequestContext, _: @import("../system_catalog/server_call.zig").Call) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.metadata_reads += 1;
            return error.TestUnexpectedMetadataAuthorization;
        }
        fn write(raw: *anyopaque, _: A, group: u64, table: []const u8, request: local.storage_db_types.BatchRequest) !?void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expectEqual(@as(u64, 72), group);
            try std.testing.expectEqualStrings("rows", table);
            const command = request.sql_lock orelse return error.TestUnexpectedResult;
            if (command.native_control == null) return error.ExternalTransactionDecisionChanged;
            try std.testing.expect(std.meta.eql(self.expected, command));
            self.deliveries += 1;
            return {};
        }
    };
    const realm: [16]u8 = @splat(0xaa);
    const descriptor: P.Descriptor = .{ .kind = .unlogged, .owner_node_id = 9, .owner_store_id = 11, .generation_instance = @splat(3), .data_generation = 1 };
    const owner: local.storage_transactions.ExternalDecisionOwner = .{ .metadata_incarnation = std.fmt.bytesToHex(realm, .lower), .operation_id = @splat(14), .admission_digest = @splat(15), .expires_at_unix_ns = 1_000_000_000 };
    const admission: @FieldType(native.Command, "admission") = .{
        .purpose = .resolution,
        .binding = .{ .namespace = .{ .home = .data, .group = 72, .incarnation = realm }, .identity = .{ .table_id = 7, .shard_id = 172, .range_id = 272 } },
        .source = .{ .group = 1, .lease = .{ .backend = .{ .incarnation = realm, .id = @splat(2), .fence = 1 }, .database = 4, .node = 9, .principal = @splat(6), .expires_at_ms = 60_000 }, .connection = @splat('a'), .nonce = @splat(8), .standalone = false },
        .transaction = @splat(9),
        .generation = 1,
        .parent_command = .{ .sequence = 1, .nonce = @splat(10), .executor = @splat(11) },
        .serial = 1,
    };
    const control: native.NativeControl = .{
        .route = .{ .metadata_group_id = 1, .metadata_incarnation = owner.metadata_incarnation, .catalog_revision = 2, .table_id = 7, .topology_epoch = 7, .route = .{ .group_id = 72, .range_id = 272, .identity_namespace = .{ .table_id = 7, .shard_id = 172, .range_id = 272 }, .execution = .{ .native_local = descriptor } }, .native_participant = .{ .physical_root_incarnation = 123, .table_id = 7, .shard_id = 172, .range_id = 272, .data_generation = 1 } },
        .owner = owner,
        .decision = .{ .operation = owner.operation_id, .admission = owner.admission_digest },
    };
    var probe: Probe = .{ .expected = .{ .admission = admission, .native_control = control, .operation = .{ .resolve = control.decision } } };
    var server: Server = undefined;
    server.cfg = .{ .deployment_mode = .standalone };
    server.source = .{ .ptr = &probe, .vtable = &.{ .status = Probe.status, .system_catalog = Probe.catalog } };
    server.table_writes = writes.TableWriteSource{ .ptr = &probe, .vtable = &.{ .batch = Probe.batch, .batch_group_local = Probe.write } };
    // Real submission must defer to the receiving native boundary, which has
    // the actual sealed state; the API has no authority to guess its absence.
    try submitOnce(&server, std.testing.allocator, .{}, "rows", probe.expected);
    probe.expected.operation = .{ .prepare = control.decision };
    try submitOnce(&server, std.testing.allocator, .{}, "rows", probe.expected);
    try std.testing.expectEqual(@as(usize, 2), probe.deliveries);
    try std.testing.expectEqual(@as(usize, 0), probe.metadata_reads);
    var invalid = probe.expected;
    invalid.operation = .begin;
    try std.testing.expectError(error.InvalidLockScope, submitOnce(&server, std.testing.allocator, .{}, "rows", invalid));
    invalid = probe.expected;
    invalid.native_control.?.owner.metadata_incarnation[0] = 'b';
    try std.testing.expectError(error.ExternalTransactionDecisionChanged, submitOnce(&server, std.testing.allocator, .{}, "rows", invalid));
    // The distinct receiver fallback remains ABORT-only and still performs
    // canonical metadata authorization. It is not replaced by delivery shape.
    probe.expected.operation = .{ .resolve = control.decision };
    try std.testing.expectError(error.TestUnexpectedMetadataAuthorization, authorize(&server, std.testing.allocator, .{}, 72, "rows", probe.expected));
    try std.testing.expectEqual(@as(usize, 1), probe.metadata_reads);
    try std.testing.expectEqual(@as(usize, 2), probe.deliveries);
}
