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

pub fn authorize(server: *Server, a: A, context: operation.RequestContext, group: u64, table: []const u8, input: native.Command) !native.Command {
    try input.validate();
    if (group == 0 or input.admission.binding.namespace.group != group) return error.IdentityNamespaceMismatch;
    var result = input;
    // Prepared commands are authorized by the receiving native frozen 2PC
    // reference. They cannot depend on a surviving connection or metadata IO.
    if (input.operation == .prepare or input.operation == .resolve) {
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
    const group = input.admission.binding.namespace.group;
    const checked = try authorize(server, a, context, group, table, input);
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
        var route = (try routes.resolveGroupRoute(a, server.catalogSource(), server.cfg.session_router orelse fallback.router(), group, .prefer_leader)) orelse return error.GroupLeaderUnavailable;
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
    try context.ensureActive();
    const raw = try std.json.Stringify.valueAlloc(a, query, .{});
    defer a.free(raw);
    const reads = server.table_reads orelse return error.UnsupportedOperation;
    const group = query.admission.binding.namespace.group;
    if (server.cfg.deployment_mode != .standalone) {
        const routes = @import("table_router.zig");
        var fallback = routes.CatalogBackedGroupRouter.init(server.catalogSource(), server.localSessionNodeId());
        var route = (try routes.resolveGroupRoute(a, server.catalogSource(), server.cfg.session_router orelse fallback.router(), group, .prefer_leader)) orelse return error.GroupLeaderUnavailable;
        defer route.deinit(a);
        if (route == .remote) {
            var client = @import("http_client.zig").ApiHttpClient.init(a, server.cfg.session_executor orelse return error.UnsupportedOperation);
            _ = client.withInternalServiceAuth(server.cfg.internal_service_secret, server.cfg.internal_service_issuer);
            var cancellation = @import("../raft/transport/http_common.zig").RequestCancellation.fromToken(context.cancellation);
            var response = try client.fetchGroupLookupWithOptions(route.remote.base_uri, group, table, "", null, "read_index", try context.remainingTimeoutMs(), &cancellation, .{ .sql_lock_query_json = raw });
            defer response.deinit(a);
            if (response.body.len > 65536) return error.InvalidSqlBackendResponse;
            return std.json.parseFromSliceLeaky(native.Snapshot, a, response.body, .{ .allocate = .alloc_always });
        }
    }
    var response = (try reads.lookupGroupLocal(a, group, table, "", .{ .sql_lock_query_json = raw, .execution_deadline_ns = (try context.platformDeadline()).deadline_ns, .cancellation = context.cancellation }, .read_index)) orelse return error.GroupLeaderUnavailable;
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
    const bytes = try server.source.systemCatalog(a, controlContext(context), .{ .sql_lock_read = query });
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

/// A pass performs at most one native WAL mutation or metadata directory
/// handoff. The immutable registry and native BEGIN retain the complete proof
/// across deadlines, source expiry, lost responses and cold recovery.
pub fn transactionQuantum(server: *Server, backing: A, context: operation.RequestContext, record: @import("../system_catalog/prepared_transactions.zig").Record, resolving: ?u64) !bool {
    var region = std.heap.ArenaAllocator.init(backing);
    defer region.deinit();
    const a = region.allocator();
    const private = controlContext(context);
    try private.ensureActive();
    for (record.participants, 0..) |part, index| {
        if (resolving) |members| if (members & (@as(u64, 1) << @intCast(index)) == 0) continue;
        const ref = (try part.lockReference(record.identity, a)) orelse continue;
        const cut = try observe(server, a, private, part.table_name, .{ .admission = ref.admission });
        if (resolving != null) {
            // Absence before BEGIN is handled by the source's checked abort
            // retirement after this native transaction has durably aborted.
            if (cut.frozen) |frozen| {
                if (!std.meta.eql(frozen.ref, ref)) return error.ExternalTransactionDecisionChanged;
                if (frozen.retired) continue;
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
            try submit(server, a, private, part.table_name, .{ .admission = ref.admission, .operation = .{ .resolve = ref.decision } });
            return false;
        }
        const frozen = cut.frozen orelse return error.ExternalTransactionDecisionChanged;
        if (!std.meta.eql(frozen.ref, ref)) return error.ExternalTransactionDecisionChanged;
        if (cut.prepared_owner == null or cut.prepared_owner.?.state != .prepared or cut.scope == null or cut.scope.?.transaction != null) {
            try submit(server, a, private, part.table_name, .{ .admission = ref.admission, .operation = .{ .prepare = ref.decision } });
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

fn savepointQuantum(server: *Server, a: A, context: operation.RequestContext, parent: authority.Request, query: authority.Query, directory: authority.scopes.Data.Header, effect: authority.scopes.ledger.Effect) !bool {
    const private = controlContext(context);
    const command = switch (parent.operation) {
        .effect, .step => |value| value.command,
        else => return error.InvalidLockEffect,
    };
    for (directory.slots) |generation| {
        if (generation == 0) continue;
        var selection = query;
        selection.participant = .{ .generation = generation };
        const metadata = try metadataSnapshot(server, a, private, selection);
        const part = metadata.participant orelse return error.InvalidSqlBackendResponse;
        if (!std.meta.eql(part.admission.parent_command, command)) {
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
                found = true;
                break;
            }
            if (!found) return error.TopologyChanged;
            var request = parent;
            var nonce: [16]u8 = undefined;
            try server.embedding_provider_runtime.io.randomSecure(&nonce);
            request.admission = .{ .head = metadata.head, .nonce = nonce };
            request.operation = .{ .data_enroll = .{ .command = command, .transaction = part.admission.transaction, .spec = spec } };
            const bytes = try server.source.systemCatalog(a, private, .{ .sql_lock_mutate = request });
            const outcome = (try std.json.parseFromSliceLeaky(?authority.outcomes.Outcome, a, bytes, .{})) orelse return error.MetadataMutationOutcomeUnknown;
            _ = try outcome.result();
            return false;
        }
        const cut = try observe(server, a, private, part.spec.table_name, .{ .admission = part.admission, .command_control = true });
        if (cut.scope == null or !std.meta.eql(cut.scope.?.command, @as(?authority.scopes.Command, try part.admission.command()))) {
            try submit(server, a, private, part.spec.table_name, .{ .admission = part.admission, .operation = .begin });
            return false;
        }
        if (cut.owner) |owner| if (owner.waiting) |waiting| {
            try submit(server, a, private, part.spec.table_name, .{ .admission = part.admission, .operation = .{ .cancel_wait = waiting } });
            return false;
        };
        if (cut.bound_control) |known| {
            if (!std.mem.eql(u8, &known.nonce, &effect.nonce) or known.action != .control or !std.meta.eql(known.action.control, effect.action.control)) return error.LockEffectChanged;
            const receipt = (try observe(server, a, private, part.spec.table_name, .{ .admission = part.admission, .effect = known })).receipt orelse return error.InvalidSqlBackendResponse;
            if (receipt.outcome == .completed) continue;
            if (receipt.outcome != .pending) return error.InvalidSqlBackendResponse;
        }
        try submit(server, a, private, part.spec.table_name, .{ .admission = part.admission, .operation = .{ .bound_control = .{ .nonce = effect.nonce, .control = effect.action.control, .failed_root = effect.action.control == .rollback and effect.action.control.rollback.first_aborted == 0 } } });
        return false;
    }
    return true;
}
