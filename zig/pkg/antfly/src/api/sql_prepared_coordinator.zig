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

//! SQL session delivery and metadata-authoritative transaction coordination.
//! Native postimages are never replayed from a SQL session during recovery.
const std = @import("std");
const local = @import("antfly_local_sources");
const registry = @import("../system_catalog/prepared_transactions.zig");
const distributed = @import("distributed_txn.zig");
const transactions = @import("transactions.zig");
const http = @import("http_server.zig");
const A = std.mem.Allocator;

pub const Registry = struct {
    server: *http.ApiHttpServer,
    context: local.api_operation.RequestContext,
    actor: registry.Actor = .{ .session_user = .bootstrap, .credential = "" },
    database_id: u64,
    native_admin: bool = false,
    // A head is an admission window, not an equality precondition. Reuse it
    // across this bounded controller pass instead of adding a quorum read for
    // every phase. Durable CAS still fences the exact job revision.
    anchor: ?struct { incarnation: [16]u8, standalone: bool, head: u64 } = null,

    pub fn authority(self: *@This()) distributed.ExternalAuthority {
        return .{ .ptr = self, .read = read, .admit = admit, .advance = advance, .next_version = nextVersion };
    }

    pub fn snapshot(self: *@This(), a: A, lookup: registry.Read) !registry.Snapshot {
        var private = self.context;
        private.setting_admin = true;
        try private.ensureActive();
        const bytes = try self.server.source.systemCatalog(a, private, .{ .sql_prepared_read = lookup });
        defer a.free(bytes);
        const cut = try std.json.parseFromSliceLeaky(registry.Snapshot, a, bytes, .{ .allocate = .alloc_always });
        if (std.mem.allEqual(u8, &cut.incarnation, 0) or cut.active > registry.max_active or cut.outstanding_receipts > registry.max_receipts or cut.records.len > 8 or cut.receipts.len > 8) return error.InvalidSqlBackendResponse;
        for (cut.receipts) |receipt| {
            try receipt.validate();
            try receipt.session.requireRealm(cut.standalone);
            if (!std.mem.eql(u8, &receipt.owner.metadata_incarnation, &std.fmt.bytesToHex(cut.incarnation, .lower))) return error.CatalogGenerationChanged;
        }
        for (cut.records) |record| {
            try record.validate(a);
            if (record.identity.session) |session| try session.requireRealm(cut.standalone);
            if (!std.mem.eql(u8, &record.identity.metadata_incarnation, &std.fmt.bytesToHex(cut.incarnation, .lower))) return error.CatalogGenerationChanged;
        }
        self.anchor = .{ .incarnation = cut.incarnation, .standalone = cut.standalone, .head = cut.head };
        return cut;
    }

    fn checkRealm(cut: registry.Snapshot, identity: registry.Identity) !void {
        if (!std.mem.eql(u8, &identity.metadata_incarnation, &std.fmt.bytesToHex(cut.incarnation, .lower))) return error.CatalogGenerationChanged;
        if (identity.session) |session| try session.requireRealm(cut.standalone);
    }

    fn mutate(self: *@This(), a: A, request: registry.Mutation) !void {
        var private = self.context;
        private.setting_admin = true;
        try private.ensureActive();
        const bytes = try self.server.source.systemCatalog(a, private, .{ .sql_prepared_mutate = request });
        defer a.free(bytes);
        const outcome = (try std.json.parseFromSliceLeaky(?registry.outcomes.Outcome, a, bytes, .{})) orelse return error.MetadataMutationOutcomeUnknown;
        _ = try outcome.result();
    }

    fn read(raw: *anyopaque, a: A, operation: [16]u8) anyerror!?registry.Record {
        const self: *@This() = @ptrCast(@alignCast(raw));
        const cut = try self.snapshot(a, .{ .operation = operation });
        if (cut.records.len == 0) return null;
        if (cut.records.len != 1 or !std.mem.eql(u8, &cut.records[0].identity.operation_id, &operation)) return error.InvalidSqlBackendResponse;
        return cut.records[0];
    }

    fn admit(raw: *anyopaque, a: A, identity: registry.Identity, participants: []const registry.Participant) anyerror!registry.Record {
        const self: *@This() = @ptrCast(@alignCast(raw));
        // This is a production check, not a caller's assertion. The purpose,
        // signed plan and participant digest were committed together locally.
        const bytes = (try self.server.txn_sessions.externalPreparationSeal(self.server.alloc, identity.txn_id)) orelse return error.InvalidTransactionSessionRecord;
        defer self.server.alloc.free(bytes);
        const sealed = try std.json.parseFromSliceLeaky(transactions.ExternalPreparationSeal, a, bytes, .{ .allocate = .alloc_always });
        const owner = try identity.owner(a);
        if (!(try sealed.identity.owner(a)).eql(owner) or !std.mem.eql(u8, &identity.manifest_digest, &try registry.manifestDigest(a, participants, identity.metadata_incarnation))) return error.ExternalTransactionDecisionChanged;
        const cut = try self.snapshot(a, .{ .operation = identity.operation_id });
        try checkRealm(cut, identity);
        if (cut.records.len != 0) {
            const existing = cut.records[0];
            if (!(try existing.identity.owner(a)).eql(owner)) return error.ExternalTransactionDecisionChanged;
            return existing;
        }
        // Retirement never authorizes recreating a GID under an old plan.
        if (cut.receipts.len != 0) return error.ExternalTransactionDecisionChanged;
        try self.mutate(a, try sealed.admissionRequest());
        // The exact retained success proves this deterministic admission.
        // A concurrent decision is fenced by the native owner and the next
        // expected-revision CAS; another observation cannot eliminate that race.
        return .{ .identity = identity, .participants = participants };
    }

    fn advance(raw: *anyopaque, a: A, record: registry.Record, action: registry.Action) anyerror!registry.Record {
        const self: *@This() = @ptrCast(@alignCast(raw));
        const owner = try record.identity.owner(a);
        for (0..2) |attempt| {
            if (self.anchor == null) _ = try self.snapshot(a, .{ .operation = record.identity.operation_id });
            const anchor = self.anchor.?;
            if (!std.mem.eql(u8, &record.identity.metadata_incarnation, &std.fmt.bytesToHex(anchor.incarnation, .lower))) return error.CatalogGenerationChanged;
            var nonce: [16]u8 = undefined;
            try self.server.embedding_provider_runtime.io.randomSecure(&nonce);
            const request: registry.Mutation = .{ .admission = .{ .nonce = nonce, .head = anchor.head }, .incarnation = anchor.incarnation, .actor = if (action == .commit or action == .abort) self.actor else .{ .session_user = .bootstrap, .credential = "" }, .native_admin = self.native_admin, .standalone = anchor.standalone, .database_id = self.database_id, .operation = .{ .advance = .{ .owner = owner, .expected_revision = record.revision, .action = action } } };
            self.mutate(a, request) catch |err| {
                if (err != error.SequenceAdmissionExpired or attempt != 0) return err;
                // Refresh only a proven exhausted admission window. Reusing
                // the same job CAS prevents a lost prior reply from applying
                // this transition twice, even after its receipt was evicted.
                _ = try self.snapshot(a, .{ .operation = record.identity.operation_id });
                continue;
            };
            // Reconstruct the deterministic postimage from the exact command's
            // durable success, including retirement of its operation row.
            return registry.advance(a, record, record.revision, owner, action);
        }
        return error.MetadataMutationOutcomeUnknown;
    }

    fn nextVersion(_: *anyopaque, floor: u64) anyerror!u64 {
        return distributed.commitVersion(floor);
    }

    /// Before admission, cancellation races the exact sealed request for its
    /// original nonce. After journal eviction, its fixed old head is a durable
    /// fence; atomic job/receipt absence distinguishes an unadmitted plan from
    /// work that has already reached the native journals.
    fn cancelUnadmitted(self: *@This(), a: A, seal: transactions.ExternalPreparationSeal) !bool {
        const original = try seal.admissionRequest();
        var cancel = original;
        cancel.actor = .{ .session_user = .bootstrap, .credential = "" };
        cancel.operation = .{ .cancel_admission = .{ .original_actor = original.actor, .original = original.operation.admit } };
        var private = self.context;
        private.setting_admin = true;
        const response = self.server.source.systemCatalog(a, private, .{ .sql_prepared_mutate = cancel });
        var fenced = false;
        var expired = false;
        if (response) |bytes| {
            defer a.free(bytes);
            const outcome = (try std.json.parseFromSliceLeaky(?registry.outcomes.Outcome, a, bytes, .{})) orelse return error.MetadataMutationOutcomeUnknown;
            fenced = outcome == .rejected;
        } else |err| switch (err) {
            error.SequenceAdmissionExpired => expired = true,
            else => return err,
        }
        const cut = try self.snapshot(a, .{ .operation = seal.identity.operation_id });
        try checkRealm(cut, seal.identity);
        const head_fence = expired and cut.head >= original.admission.head and cut.head - original.admission.head >= registry.outcomes.retained;
        return (fenced or head_fence) and cut.records.len == 0 and cut.receipts.len == 0;
    }

    pub fn acknowledgeReceipt(self: *@This(), a: A, identity: registry.Identity) !void {
        const cut = try self.snapshot(a, .{ .receipt = identity.operation_id });
        try checkRealm(cut, identity);
        if (cut.receipts.len == 0) return;
        var nonce: [16]u8 = undefined;
        try self.server.embedding_provider_runtime.io.randomSecure(&nonce);
        try self.mutate(a, .{ .admission = .{ .nonce = nonce, .head = cut.head }, .incarnation = cut.incarnation, .actor = .{ .session_user = .bootstrap, .credential = "" }, .standalone = cut.standalone, .database_id = identity.database_id, .operation = .{ .acknowledge_receipt = .{ .owner = try identity.owner(a), .txn_id = identity.txn_id } } });
    }
};

/// The session proof is durable before the metadata delivery ACK. A lost ACK
/// reply is reconciled by an authoritative absence in the same incarnation.
/// Pending or absent admission remains debt; it cannot become ordinary commit.
pub fn recoverSession(server: *http.ApiHttpServer, txn_id: [16]u8, bytes: []const u8, prepared: bool, aborted: bool, committed: bool) !void {
    return recoverSessionWithContext(server, txn_id, bytes, prepared, aborted, committed, .{ .deadline_ns = @import("antfly_platform").time.monotonicNs() +| 2 * std.time.ns_per_s });
}

fn recoverSessionWithContext(server: *http.ApiHttpServer, txn_id: [16]u8, bytes: []const u8, prepared: bool, aborted: bool, committed: bool, context: local.api_operation.RequestContext) !void {
    var region = std.heap.ArenaAllocator.init(server.alloc);
    defer region.deinit();
    const a = region.allocator();
    const seal = try std.json.parseFromSliceLeaky(transactions.ExternalPreparationSeal, a, bytes, .{ .allocate = .alloc_always });
    if (!std.mem.eql(u8, &txn_id, &seal.identity.txn_id)) return error.InvalidTransactionSessionRecord;
    var source: Registry = .{ .server = server, .context = context, .database_id = seal.identity.database_id };
    const cut = try source.snapshot(a, .{ .operation = seal.identity.operation_id });
    try Registry.checkRealm(cut, seal.identity);
    if (cut.receipts.len == 0) {
        if (!prepared and !aborted and !committed) {
            if (try source.cancelUnadmitted(a, seal)) {
                _ = (try server.txn_sessions.recordExternalAdmissionRejection(server.alloc, txn_id, try seal.identity.owner(a))) orelse return error.SqlTransactionNotActive;
                return;
            }
            return error.MetadataMutationOutcomeUnknown;
        }
    } else {
        if (cut.receipts.len != 1) return error.InvalidSqlBackendResponse;
        if (cut.receipts[0].status == .pending) {
            if (cut.records.len != 1) return error.InvalidSqlBackendResponse;
            const record = cut.records[0];
            // A private commit keeps its coordinator-owned preparation alive
            // through the immutable admission deadline. The work controller
            // completes vote collection and decides it; a session recovery
            // pass must not replace that live purpose with an early abort.
            if (record.phase == .preparing and (record.identity.kind == .named or @import("antfly_platform").time.realtimeNs() >= record.identity.expires_at_unix_ns))
                _ = try Registry.advance(&source, a, record, .{ .abort = try distributed.commitVersion(record.identity.begin_timestamp) });
            return error.MetadataMutationOutcomeUnknown;
        }
        if (seal.identity.kind == .session_commit) {
            _ = (try server.txn_sessions.recordExternalCommitReceipt(server.alloc, txn_id, cut.receipts[0])) orelse return error.SqlTransactionNotActive;
        } else {
            _ = (try server.txn_sessions.recordExternalPreparationReceipt(server.alloc, txn_id, cut.receipts[0])) orelse return error.SqlTransactionNotActive;
        }
        try source.acknowledgeReceipt(a, seal.identity);
    }
    _ = (try server.txn_sessions.markExternalPreparationReceiptAcknowledged(server.alloc, txn_id)) orelse return error.SqlTransactionNotActive;
}

/// One job per supervisor pass, with a keyset cursor so indefinitely prepared
/// transactions cannot starve incomplete or decided jobs later in the index.
/// Scheduling follows the existing metadata-group leadership capability; the
/// durable CAS and native owner checks remain authoritative across elections.
pub fn advanceWork(server: *http.ApiHttpServer, after: *?[16]u8) !void {
    const group = local.common_group_ids.main_metadata_group_id;
    if (server.cfg.prepared_transaction_leadership) |leadership| {
        if (!leadership.isLocalLeader(group)) return;
    } else if (server.cfg.deployment_mode != .standalone) return;
    const writes = server.table_writes orelse return;
    var region = std.heap.ArenaAllocator.init(server.alloc);
    defer region.deinit();
    const a = region.allocator();
    const time = @import("antfly_platform").time;
    var source: Registry = .{ .server = server, .context = .{ .setting_admin = true, .deadline_ns = time.monotonicNs() +| 5 * std.time.ns_per_s }, .database_id = 1 };
    const cut = try source.snapshot(a, .{ .work = after.* });
    if (cut.records.len == 0) {
        after.* = null;
        return;
    }
    const record = cut.records[0];
    after.* = record.identity.operation_id;
    source.database_id = record.identity.database_id;
    if ((record.phase == .preparing or record.phase == .prepared) and registry.recoveryDecision(record, time.realtimeNs()) == null) return;
    const deadline = time.monotonicNs() +| distributed.external_recovery_pass_timeout_ns;
    var local_worker = distributed.LocalTableWriteParticipantWorker.init(writes);
    local_worker.recovery_deadline_ns = deadline;
    var fallback = @import("table_router.zig").CatalogBackedGroupRouter.init(server.catalogSource(), server.localSessionNodeId());
    var hosted_worker: distributed.HostedParticipantWorker = undefined;
    const worker = if (server.cfg.deployment_mode == .standalone) local_worker.worker() else hosted: {
        hosted_worker = distributed.HostedParticipantWorker.init(server.catalogSource(), server.cfg.session_router orelse fallback.router(), writes, server.cfg.session_executor orelse return error.UnsupportedSqlExecution);
        hosted_worker.recovery_deadline_ns = deadline;
        _ = hosted_worker.withInternalServiceAuth(server.cfg.internal_service_secret, server.cfg.internal_service_issuer);
        break :hosted hosted_worker.worker();
    };
    _ = try distributed.recoverExternalTransaction(a, worker, source.authority(), record.identity.operation_id, time.realtimeNs(), .{ .fanout_io = server.sharedApiIo(), .external_deadline_ns = deadline });
}
const sql_session = local.sql_session;
const ast = local.sql_ast;
const execution = @import("sql_execution.zig");

fn databaseIdentity(root: *execution.Adapter, a: A) !u64 {
    const bytes = try root.server.source.systemCatalog(a, root.context, .{ .read = .{ .kind = .database, .name = root.database } });
    defer a.free(bytes);
    const state = try std.json.parseFromSliceLeaky(local.system_catalog_domain.State, a, bytes, .{ .allocate = .alloc_always });
    for (state.resources) |resource| if (resource.kind == .database and std.mem.eql(u8, resource.name, root.database)) return resource.id;
    return error.DatabaseNotFound;
}

const ParticipantTransport = struct {
    local_worker: distributed.LocalTableWriteParticipantWorker,
    hosted_worker: distributed.HostedParticipantWorker,
    fallback: @import("table_router.zig").CatalogBackedGroupRouter,
    hosted: bool,

    fn init(self: *@This(), server: *http.ApiHttpServer) !void {
        const writes = server.table_writes orelse return error.UnsupportedSqlExecution;
        self.fallback = @import("table_router.zig").CatalogBackedGroupRouter.init(server.catalogSource(), server.localSessionNodeId());
        self.local_worker = distributed.LocalTableWriteParticipantWorker.init(writes);
        self.hosted = server.cfg.deployment_mode != .standalone;
        if (self.hosted) {
            self.hosted_worker = distributed.HostedParticipantWorker.init(server.catalogSource(), server.cfg.session_router orelse self.fallback.router(), writes, server.cfg.session_executor orelse return error.UnsupportedSqlExecution);
            _ = self.hosted_worker.withInternalServiceAuth(server.cfg.internal_service_secret, server.cfg.internal_service_issuer);
        }
    }
    fn worker(self: *@This()) distributed.ParticipantWorker {
        return if (self.hosted) self.hosted_worker.worker() else self.local_worker.worker();
    }
};

/// Session ownership, current schema/RLS, predicates and FK/UNIQUE preparation
/// use the ordinary transaction pipeline. Only the durable decision authority
/// and the terminal SQL boundary differ from COMMIT.
pub fn execute(root: *execution.Adapter, session: *sql_session.Session, a: A, command: ast.PreparedTransaction) !local.sql_runtime.Result {
    const server = root.server;
    const tag = switch (command.action) {
        .prepare => "PREPARE TRANSACTION",
        .commit => "COMMIT PREPARED",
        .rollback => "ROLLBACK PREPARED",
    };
    var result = try local.sql_runtime.Result.empty(a, tag);
    errdefer result.deinit();
    if (command.action != .prepare) {
        if (session.transaction_id != null) return error.SqlPreparedInsideTransaction;
        try registry.validateGid(command.gid);
        var region = std.heap.ArenaAllocator.init(server.alloc);
        defer region.deinit();
        const scratch = region.allocator();
        const actor = try @import("sql_authorization_recovery.zig").actor(server, root.identity.*);
        var source: Registry = .{ .server = server, .context = root.context, .actor = actor, .database_id = try databaseIdentity(root, scratch), .native_admin = try http.resourcePermissionCurrentlyAllowed(root.identity.*, .user, "*", .admin) };
        const cut = try source.snapshot(scratch, .{ .gid = command.gid });
        if (cut.records.len != 1) return error.SqlPreparedTransactionNotFound;
        var record = cut.records[0];
        if (record.identity.database_id != source.database_id) return error.SqlPreparedTransactionWrongDatabase;
        try local.api_operation.requireNativeServiceAccess(record.identity.native_service, root.context.native_service);
        if (!source.native_admin and actor.principal().kind != .bootstrap and !record.identity.principal.eql(actor.principal())) return error.Forbidden;
        if (record.phase == .preparing) return error.SqlPreparedTransactionNotFound;
        const committed = command.action == .commit;
        if (record.phase == .prepared) {
            const version = try distributed.commitVersion(record.identity.begin_timestamp);
            record = Registry.advance(&source, scratch, record, if (committed) .{ .commit = .{ .version = version, .sync_level = try local.sql_setting_catalog.mutationSyncLevel(root.setting_overlay) } } else .{ .abort = version }) catch return error.SqlTransactionOutcomeUnknown;
        }
        if (record.decision == null or (record.decision.?.status == .committed) != committed) return error.ExternalTransactionDecisionChanged;
        root.outcome_transaction_id = std.fmt.bytesToHex(record.identity.txn_id, .lower);
        var transport: ParticipantTransport = undefined;
        transport.init(server) catch return error.SqlTransactionOutcomeUnknown;
        _ = distributed.recoverExternalTransaction(scratch, transport.worker(), source.authority(), record.identity.operation_id, @import("antfly_platform").time.realtimeNs(), .{ .fanout_io = server.sharedApiIo() }) catch return error.SqlTransactionOutcomeUnknown;
        if (committed) result.output.mutation_outcome = .committed;
        return result;
    }

    const id = session.transaction_id orelse {
        result.output.command_tag = "ROLLBACK";
        if (root.notice_sink) |sink| try sink.emit(sink.ptr, .{ .code = "25P01", .severity = .warning, .message = "there is no transaction in progress" });
        return result;
    };
    const lease = server.txn_sessions.tryAcquireCommitExecution(id) orelse return error.SqlWriteCapacityUnavailable;
    defer lease.release();
    var sealed = false;
    errdefer if (!sealed) {
        if (session.owner.vtable.rollback(session.owner.ptr, session.scope, id)) |_| {
            session.transaction_id = null;
        } else |_| {
            root.outcome_transaction_id = std.fmt.bytesToHex(id, .lower);
        }
    };
    const current = try session.owner.vtable.inspect(session.owner.ptr, session.scope, id);
    if (current.state == .failed) {
        try session.owner.vtable.rollback(session.owner.ptr, session.scope, id);
        session.transaction_id = null;
        result.output.command_tag = "ROLLBACK";
        return result;
    }
    if (current.state != .active) {
        sealed = true;
        return error.SqlTransactionOutcomeUnknown;
    }
    try registry.validateGid(command.gid);
    if (server.txn_sessions.durable == null) return error.SqlPreparedDurabilityUnavailable;
    const info = server.txn_sessions.getInfo(id) orelse return error.SqlTransactionNotActive;
    var state = (try server.txn_sessions.getSqlState(server.alloc, id)) orelse return error.SqlTransactionNotActive;
    defer state.deinit(server.alloc);
    if (state.metadata.accepted_lake_reads) return error.UnsupportedSqlExecution;
    try root.validateAcceptedSerializable(server.alloc, id);
    var request = try server.txn_sessions.cloneCommitRequest(server.alloc, id, null);
    defer if (request) |*value| value.deinit(server.alloc);
    var region = std.heap.ArenaAllocator.init(server.alloc);
    defer region.deinit();
    const scratch = region.allocator();
    var sql_scope = if (request) |value| try @import("sql_authorization_runtime.zig").TransactionScope.init(server, root.identity.*, root.context, scratch, value) else null;
    defer if (sql_scope) |*authority_owner| authority_owner.deinit();
    if (request) |value| if (!try server.transactionRequestAuthorizedWithSql(root.identity.*, value, if (sql_scope) |*scope| scope else null)) return error.Forbidden;
    const commit_context = if (sql_scope) |*scope| scope.context() else root.context;
    const view = if (request) |value| try value.distributedTables(scratch) else null;
    defer if (view) |value| value.deinit();
    const tables: []const distributed.TableCommitRequest = if (view) |value| value.tables else &.{};
    try server.validateCommitTablesAgainstSchema(root.context, tables);
    if (request) |value| if (try server.validateCommitReadSet(value) != null) return error.SqlWriteConflict;
    var prepared = try server.preparePublicCommitWithIntegrity(scratch, tables, commit_context);
    defer prepared.deinit();
    const signed = try server.signCurrentRowPolicyMutationParticipants(scratch, prepared.tables, commit_context);
    var cohort = try distributed.ExternalPreparation.init(scratch, server.catalogSource(), signed);
    defer cohort.deinit(scratch);
    var transport: ParticipantTransport = undefined;
    try transport.init(server);
    const encoded = try transactions.encodeExecutionPlan(server.alloc, signed);
    defer server.alloc.free(encoded);
    var source: Registry = .{ .server = server, .context = root.context, .database_id = try databaseIdentity(root, scratch) };
    const cut = try source.snapshot(scratch, .{ .gid = command.gid });
    const actor = try @import("sql_authorization_recovery.zig").actor(server, root.identity.*);
    var operation_id: [16]u8 = undefined;
    try server.embedding_provider_runtime.io.randomSecure(&operation_id);
    const now = @import("antfly_platform").time.realtimeNs();
    const identity: registry.Identity = .{ .session = .{ .owner_node_id = server.localSessionNodeId(), .connection_id = state.connection_id, .standalone = cut.standalone }, .gid = command.gid, .database_id = source.database_id, .native_service = root.context.native_service, .principal = actor.principal(), .txn_id = id, .operation_id = operation_id, .metadata_incarnation = std.fmt.bytesToHex(cut.incarnation, .lower), .begin_timestamp = info.begin_timestamp, .expires_at_unix_ns = try std.math.add(u64, now, 5 * std.time.ns_per_min), .plan_digest = try registry.planDigest(encoded), .manifest_digest = try registry.manifestDigest(scratch, cohort.manifest, std.fmt.bytesToHex(cut.incarnation, .lower)) };
    const seal: transactions.ExternalPreparationSeal = .{ .admission_head = cut.head, .admitted_at_ns = now, .standalone = cut.standalone, .identity = identity, .actor = actor, .participants = cohort.manifest };
    var plan = (try server.txn_sessions.sealExternalEncodedExecutionPlan(server.alloc, id, seal, encoded)) orelse return error.SqlTransactionNotActive;
    defer plan.deinit();
    sealed = true;
    root.outcome_transaction_id = std.fmt.bytesToHex(id, .lower);
    _ = cohort.prepare(scratch, transport.worker(), source.authority(), identity, .{ .fanout_io = server.sharedApiIo() }) catch |err| {
        // A definite original rejection can close immediately. An admitted
        // cohort remains metadata-owned recovery debt, never ordinary abort.
        const owned_seal = (try server.txn_sessions.externalPreparationSeal(server.alloc, id)) orelse return error.SqlTransactionOutcomeUnknown;
        defer server.alloc.free(owned_seal);
        recoverSessionWithContext(server, id, owned_seal, false, false, false, root.context) catch {};
        var observed = (try server.txn_sessions.getSqlState(server.alloc, id)) orelse return error.SqlTransactionOutcomeUnknown;
        defer observed.deinit(server.alloc);
        if (observed.terminal_abort) {
            session.transaction_id = null;
            root.outcome_transaction_id = null;
            return err;
        }
        if (!observed.terminal_prepare) {
            std.log.warn("SQL transaction preparation requires reconciliation txn={s} err={s}", .{ std.fmt.bytesToHex(id, .lower), @errorName(err) });
            return error.SqlTransactionOutcomeUnknown;
        }
        session.transaction_id = null;
        return result;
    };
    const delivered = try source.snapshot(scratch, .{ .receipt = operation_id });
    if (delivered.receipts.len != 1 or delivered.receipts[0].status != .prepared) return error.SqlTransactionOutcomeUnknown;
    _ = (try server.txn_sessions.recordExternalPreparationReceipt(server.alloc, id, delivered.receipts[0])) orelse return error.SqlTransactionOutcomeUnknown;
    // SQL/HTTP connections reconcile from the durable distinct prepared state.
    // Metadata outbox ACK delivery is independently retryable maintenance.
    session.transaction_id = null;
    return result;
}
