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
        return .{ .ptr = self, .read = read, .admit = admit, .advance = advance, .next_version = nextVersion, .prepare_locks = prepareLocks, .resolve_locks = resolveLocks };
    }

    pub fn snapshot(self: *@This(), a: A, lookup: registry.Read) !registry.Snapshot {
        var private = self.context;
        private.setting_admin = true;
        // The identity embeds the command scope. Independent decision delivery
        // must keep the sealed admission hash unchanged across client death.
        if (private.sql_command_fence != null) {
            private.sql_completion = null;
            private.sql_completion_submission = null;
            private.sql_command_fence = null;
        }
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
        // The identity embeds the command scope. Independent decision delivery
        // must keep the sealed admission hash unchanged across client death.
        if (private.sql_command_fence != null) {
            private.sql_completion = null;
            private.sql_completion_submission = null;
            private.sql_command_fence = null;
        }
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

    fn prepareLocks(raw: *anyopaque, a: A, record: registry.Record) anyerror!bool {
        const self: *@This() = @ptrCast(@alignCast(raw));
        const complete = try @import("sql_data_locks.zig").transactionQuantum(self.server, a, self.context, record, null);
        self.anchor = null;
        return complete;
    }

    fn resolveLocks(raw: *anyopaque, a: A, record: registry.Record) anyerror!bool {
        const self: *@This() = @ptrCast(@alignCast(raw));
        return @import("sql_data_locks.zig").transactionQuantum(self.server, a, self.context, record, record.mask());
    }

    fn advance(raw: *anyopaque, a: A, record: registry.Record, action: registry.Action) anyerror!registry.Record {
        const self: *@This() = @ptrCast(@alignCast(raw));
        if (action == .handoff and !try self.releaseScopeQuantum(a, record.identity)) return error.MetadataMutationOutcomeUnknown;
        const owner = try record.identity.owner(a);
        for (0..2) |attempt| {
            if (self.anchor == null) _ = try self.snapshot(a, .{ .operation = record.identity.operation_id });
            const anchor = self.anchor.?;
            if (!std.mem.eql(u8, &record.identity.metadata_incarnation, &std.fmt.bytesToHex(anchor.incarnation, .lower))) return error.CatalogGenerationChanged;
            var nonce: [16]u8 = undefined;
            try self.server.embedding_provider_runtime.io.randomSecure(&nonce);
            const request: registry.Mutation = .{ .notification_control = if (record.identity.notification_plan != null) true else null, .admission = .{ .nonce = nonce, .head = anchor.head }, .incarnation = anchor.incarnation, .actor = if (action == .commit or action == .abort) self.actor else .{ .session_user = .bootstrap, .credential = "" }, .native_admin = self.native_admin, .standalone = anchor.standalone, .database_id = self.database_id, .operation = .{ .advance = .{ .owner = owner, .expected_revision = record.revision, .action = action } } };
            self.mutate(a, request) catch |err| {
                if (err != error.SequenceAdmissionExpired or attempt != 0) return err;
                // Refresh only a proven exhausted admission window. Reusing
                // the same job CAS prevents a lost prior reply from applying
                // this transition twice, even after its receipt was evicted.
                _ = try self.snapshot(a, .{ .operation = record.identity.operation_id });
                continue;
            };
            if (action == .lock_handoff or action == .lock_release) {
                // The native quantum may atomically publish its completion.
                // Its phase comes from the accepting owner, never an assumed
                // local postimage of a background job.
                const cut = try self.snapshot(a, .{ .operation = record.identity.operation_id });
                if (cut.records.len != 1 or cut.records[0].revision < record.revision or !(try cut.records[0].identity.owner(a)).eql(owner)) return error.MetadataMutationOutcomeUnknown;
                return cut.records[0];
            }
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
        if (private.sql_command_fence != null) {
            private.sql_completion = null;
            private.sql_completion_submission = null;
            private.sql_command_fence = null;
        }
        const response = self.server.source.systemCatalog(a, private, .{ .sql_prepared_mutate = cancel });
        var fenced = false;
        if (response) |bytes| {
            defer a.free(bytes);
            if (try std.json.parseFromSliceLeaky(?registry.outcomes.Outcome, a, bytes, .{})) |outcome| fenced = outcome == .rejected;
        } else |err| switch (err) {
            error.SequenceAdmissionExpired, error.MetadataMutationOutcomeUnknown => {},
            else => return err,
        }
        const cut = try self.snapshot(a, .{ .operation = seal.identity.operation_id });
        try checkRealm(cut, seal.identity);
        // The authoritative window/absence cut remains a proof even when the
        // cancellation response itself was lost or its old receipt was evicted.
        const head_fence = cut.head >= original.admission.head and cut.head - original.admission.head >= registry.outcomes.retained;
        return (fenced or head_fence) and cut.records.len == 0 and cut.receipts.len == 0;
    }

    /// Unadmitted SQL may already hold transaction locks. Native cleanup must
    /// prove the original purpose was rejected or permanently fenced, in the
    /// same writer transaction that releases a quantum of those locks.
    fn releaseUnadmittedScope(self: *@This(), a: A, seal: transactions.ExternalPreparationSeal) !bool {
        const ref = seal.identity.lock_scope orelse return true;
        const types = @import("../system_catalog/lock_authority.zig");
        const anchor = try self.snapshot(a, .{ .operation = seal.identity.operation_id });
        var context = self.context;
        context.setting_admin = true;
        context.sql_command_fence = null;
        context.sql_completion = null;
        context.sql_completion_submission = null;
        const query: types.Query = .{ .incarnation = anchor.incarnation, .standalone = anchor.standalone, .actor = .{ .session_user = .bootstrap, .credential = "" }, .database = seal.identity.database_id, .backend = ref.backend };
        const before = try std.json.parseFromSliceLeaky(types.Snapshot, a, try self.server.source.systemCatalog(a, context, .{ .sql_lock_read = query }), .{});
        const scope = before.scope orelse return error.InvalidSqlBackendResponse;
        if (scope.next_command > ref.command.sequence or scope.transaction == null) return true;
        if (!std.meta.eql(scope.transaction, @as(?types.scopes.Transaction, .{ .id = seal.identity.txn_id }))) return error.ExternalTransactionDecisionChanged;
        var nonce: [16]u8 = undefined;
        try self.server.embedding_provider_runtime.io.randomSecure(&nonce);
        const request: types.Request = .{ .admission = .{ .head = before.head, .nonce = nonce }, .incarnation = before.incarnation, .standalone = before.standalone, .actor = query.actor, .database = query.database, .backend = ref.backend, .operation = .{ .release_unadmitted = try seal.admissionRequest() } };
        if (!try @import("sql_data_locks.zig").beforeMetadataMutation(self.server, a, context, request)) return false;
        const bytes = try self.server.source.systemCatalog(a, context, .{ .sql_lock_mutate = request });
        const result = (try std.json.parseFromSliceLeaky(?types.outcomes.Outcome, a, bytes, .{})) orelse return error.MetadataMutationOutcomeUnknown;
        _ = try result.result();
        self.anchor = null;
        // One bounded cleanup quantum, then yield to the existing supervisor.
        const after = try std.json.parseFromSliceLeaky(types.Snapshot, a, try self.server.source.systemCatalog(a, context, .{ .sql_lock_read = query }), .{});
        return after.scope != null and after.scope.?.transaction == null;
    }

    /// One journal quantum per recovery pass. Scope cleanup shares the same
    /// native decision registry and retains its terminal receipt until done.
    fn releaseScopeQuantum(self: *@This(), a: A, identity: registry.Identity) !bool {
        const ref = identity.lock_scope orelse return true;
        const types = @import("../system_catalog/lock_authority.zig");
        const anchor = try self.snapshot(a, .{ .operation = identity.operation_id });
        if (identity.kind == .named) for (anchor.receipts) |receipt| {
            if (receipt.status == .prepared and receipt.owner.eql(try identity.owner(a))) return true;
        };
        var context = self.context;
        context.setting_admin = true;
        const query: types.Query = .{ .incarnation = anchor.incarnation, .standalone = anchor.standalone, .actor = .{ .session_user = .bootstrap, .credential = "" }, .database = identity.database_id, .backend = ref.backend };
        const before = try std.json.parseFromSliceLeaky(types.Snapshot, a, try self.server.source.systemCatalog(a, context, .{ .sql_lock_read = query }), .{});
        const scope = before.scope orelse return error.InvalidSqlBackendResponse;
        // A completed sealed command is a permanent cleanup witness, even if
        // later transactions have replaced the single recent release marker.
        if (scope.next_command > ref.command.sequence) return true;
        const expected: types.scopes.Transaction = .{ .id = identity.txn_id, .decision = .{ .operation = identity.operation_id, .admission = (try identity.owner(a)).admission_digest } };
        if (scope.transaction == null) {
            if (!std.meta.eql(scope.released_transaction, @as(?types.scopes.Transaction, expected))) return error.ExternalTransactionDecisionChanged;
            return true;
        }
        if (!std.meta.eql(scope.transaction, @as(?types.scopes.Transaction, expected))) return error.ExternalTransactionDecisionChanged;
        var nonce: [16]u8 = undefined;
        try self.server.embedding_provider_runtime.io.randomSecure(&nonce);
        const request: types.Request = .{ .admission = .{ .nonce = nonce, .head = before.head }, .incarnation = anchor.incarnation, .standalone = anchor.standalone, .actor = query.actor, .database = identity.database_id, .backend = ref.backend, .operation = .{ .release = expected } };
        if (!try @import("sql_data_locks.zig").beforeMetadataMutation(self.server, a, context, request)) return false;
        const result = try std.json.parseFromSliceLeaky(types.outcomes.Outcome, a, try self.server.source.systemCatalog(a, context, .{ .sql_lock_mutate = request }), .{});
        _ = try result.result();
        self.server.sql_lock_work_pending.store(true, .release);
        self.anchor = null; // Scope progress advances the shared admission head.
        const after = try std.json.parseFromSliceLeaky(types.Snapshot, a, try self.server.source.systemCatalog(a, context, .{ .sql_lock_read = query }), .{});
        return after.scope != null and after.scope.?.transaction == null and std.meta.eql(after.scope.?.released_transaction, @as(?types.scopes.Transaction, expected));
    }
    pub fn acknowledgeReceipt(self: *@This(), a: A, identity: registry.Identity) !void {
        if (!try self.releaseScopeQuantum(a, identity)) return error.MetadataMutationOutcomeUnknown;
        const cut = try self.snapshot(a, .{ .receipt = identity.operation_id });
        try checkRealm(cut, identity);
        if (cut.receipts.len == 0) return;
        var nonce: [16]u8 = undefined;
        try self.server.embedding_provider_runtime.io.randomSecure(&nonce);
        try self.mutate(a, .{ .notification_control = if (identity.notification_plan != null) true else null, .admission = .{ .nonce = nonce, .head = cut.head }, .incarnation = cut.incarnation, .actor = .{ .session_user = .bootstrap, .credential = "" }, .standalone = cut.standalone, .database_id = identity.database_id, .operation = .{ .acknowledge_receipt = .{ .owner = try identity.owner(a), .txn_id = identity.txn_id } } });
    }
};

/// The session proof is durable before the metadata delivery ACK. A lost ACK
/// reply is reconciled by an authoritative absence in the same incarnation.
/// Pending or absent admission remains debt; it cannot become ordinary commit.
pub fn recoverSession(server: *http.ApiHttpServer, txn_id: [16]u8, bytes: []const u8, prepared: bool, aborted: bool, committed: bool) !void {
    return recoverSessionWithContext(server, txn_id, bytes, prepared, aborted, committed, .{ .deadline_ns = @import("antfly_platform").time.monotonicNs() +| 2 * std.time.ns_per_s });
}

/// Drain only an already terminal local transaction's exact completion. This
/// never admits a SQL body, scans the work queue, or changes its decision. An
/// autocommit caller keeps its command loan until native scope release and ACK.
pub fn drainSessionCompletion(server: *http.ApiHttpServer, txn_id: [16]u8, context: local.api_operation.RequestContext) !void {
    const store = server.txn_sessions.durable orelse return error.SqlPreparedDurabilityUnavailable;
    var quanta: u8 = 0;
    while (true) {
        try context.ensureActive();
        var state = (try store.loadInto(server.alloc, txn_id)) orelse return error.SqlTransactionNotActive;
        defer state.deinit(server.alloc);
        if (!state.terminal_abort and state.terminal_commit == null) return error.SqlTransactionOutcomeUnknown;
        if (state.external_receipt_acknowledged) return;
        recoverSessionWithContext(server, txn_id, state.external_preparation orelse return error.InvalidTransactionSessionRecord, state.external_preparation_prepared, state.terminal_abort, state.terminal_commit != null, context) catch |err| switch (err) {
            error.MetadataMutationOutcomeUnknown => {},
            else => return err,
        };
        quanta += 1;
        if (quanta == 8) {
            try server.embedding_provider_runtime.io.sleep(.fromMilliseconds(1), .awake);
            quanta = 0;
        }
    }
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
                if (!try source.releaseUnadmittedScope(a, seal)) return error.MetadataMutationOutcomeUnknown;
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
            if (cut.receipts[0].progress == .resolving or (cut.records.len == 1 and cut.records[0].native_lock_releases != cut.records[0].nativeLockMask())) {
                // The synchronous caller owns this exact durable purpose. It
                // can advance its admitted control graph directly rather than
                // flooding metadata reads while waiting for a global worker.
                if (cut.records.len != 1) return error.InvalidSqlBackendResponse;
                var transport: ParticipantTransport = undefined;
                try transport.init(server);
                const pass_deadline = @import("antfly_platform").time.monotonicNs() +| distributed.external_recovery_pass_timeout_ns;
                const deadline = if (context.deadline_ns) |value| @min(value, pass_deadline) else pass_deadline;
                transport.local_worker.recovery_deadline_ns = deadline;
                if (transport.hosted) transport.hosted_worker.recovery_deadline_ns = deadline;
                _ = distributed.recoverExternalTransaction(a, transport.worker(), source.authority(), seal.identity.operation_id, @import("antfly_platform").time.realtimeNs(), .{ .fanout_io = server.sharedApiIo(), .external_deadline_ns = deadline }) catch |err| switch (err) {
                    // A concurrent controller can advance the same immutable
                    // purpose. Reload its record on the next control pass.
                    error.CatalogGenerationChanged => return error.MetadataMutationOutcomeUnknown,
                    else => return err,
                };
                return error.MetadataMutationOutcomeUnknown;
            }
            if (cut.receipts[0].progress == .indexing) {
                // Row intents have resolved: release transactional locks while
                // the independent index barrier remains durable recovery debt.
                _ = try source.releaseScopeQuantum(a, seal.identity);
                return error.MetadataMutationOutcomeUnknown;
            }
        } else {
            _ = (try server.txn_sessions.recordExternalPreparationReceipt(server.alloc, txn_id, cut.receipts[0])) orelse return error.SqlTransactionNotActive;
        }
        try source.acknowledgeReceipt(a, seal.identity);
    }
    _ = (try server.txn_sessions.markExternalPreparationReceiptAcknowledged(server.alloc, txn_id)) orelse return error.SqlTransactionNotActive;
}

/// The same leader-fenced supervisor retires expired/closed command scopes.
/// One indexed owner and one native quantum per call; unresolved decisions
/// remain held while the cursor advances so they cannot starve other owners.
pub fn advanceLockWork(server: *http.ApiHttpServer, after: *?@import("../system_catalog/lock_authority.zig").scopes.Work) !bool {
    const group = server.cfg.prepared_transaction_group_id;
    if (server.cfg.prepared_transaction_leadership) |leadership| {
        if (!leadership.isLocalLeader(group)) return false;
    } else if (server.cfg.deployment_mode != .standalone) return false;
    const types = @import("../system_catalog/lock_authority.zig");
    var region = std.heap.ArenaAllocator.init(server.alloc);
    defer region.deinit();
    const a = region.allocator();
    const time = @import("antfly_platform").time;
    const context: local.api_operation.RequestContext = .{ .setting_admin = true, .deadline_ns = time.monotonicNs() +| std.time.ns_per_s };
    const now_ms = time.realtimeNs() / std.time.ns_per_ms;
    const actor: registry.Actor = .{ .session_user = .bootstrap, .credential = "" };
    const query: types.Query = .{ .incarnation = @splat(0), .standalone = server.cfg.deployment_mode == .standalone, .actor = actor, .database = 0, .work = .{ .observed_at_ms = now_ms, .after = after.*, .wrap = true } };
    const cut = try std.json.parseFromSliceLeaky(types.Snapshot, a, try server.source.systemCatalog(a, context, .{ .sql_lock_read = query }), .{});
    const work = cut.next_work orelse {
        after.* = null;
        return false;
    };
    after.* = work;
    const scope = cut.scope orelse return error.InvalidSqlBackendResponse;
    var nonce: [16]u8 = undefined;
    try server.embedding_provider_runtime.io.randomSecure(&nonce);
    const operation: @FieldType(types.Request, "operation") = if (scope.releasing)
        .{ .release = scope.transaction orelse return error.InvalidSqlBackendResponse }
    else if (scope.state == .active)
        .{ .fence = .{ .expired = .{ .expected_expiry = scope.registration.lease.expires_at_ms, .observed_at_ms = now_ms } } }
    else if (scope.transaction) |transaction| .{ .release = transaction } else .close_step;
    const request: types.Request = .{ .admission = .{ .nonce = nonce, .head = cut.head }, .incarnation = cut.incarnation, .standalone = cut.standalone, .actor = actor, .database = scope.registration.lease.database, .backend = work.backend, .operation = operation };
    if (!try @import("sql_data_locks.zig").beforeMetadataMutation(server, a, context, request)) return true;
    const outcome = try std.json.parseFromSliceLeaky(types.outcomes.Outcome, a, try server.source.systemCatalog(a, context, .{ .sql_lock_mutate = request }), .{});
    _ = outcome.result() catch |err| switch (err) {
        error.CatalogGenerationChanged, error.CatalogPublicationProofPending => return true,
        else => return err,
    };
    return true;
}

/// One job per supervisor pass, with a keyset cursor so indefinitely prepared
/// transactions cannot starve incomplete or decided jobs later in the index.
/// Scheduling follows the existing metadata-group leadership capability; the
/// durable CAS and native owner checks remain authoritative across elections.
pub fn advanceWork(server: *http.ApiHttpServer, after: *?[16]u8) !void {
    const group = server.cfg.prepared_transaction_group_id;
    if (server.cfg.prepared_transaction_leadership) |leadership| {
        if (!leadership.isLocalLeader(group)) return;
    } else if (server.cfg.deployment_mode != .standalone) return;
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
    if (record.participants.len == 0) {
        _ = try recoverEmptyCohort(&source, a, record, time.realtimeNs());
        return;
    }
    const writes = server.table_writes orelse return;
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

/// Empty cohorts still have a durable decision and scope-release barrier.
/// They require no storage transport and must remain recoverable on nodes
/// without a local table writer. At most one decision and one handoff per pass.
fn recoverEmptyCohort(source: *Registry, a: A, before: registry.Record, now_ns: u64) !registry.Record {
    if (before.participants.len != 0) return error.InvalidTxnRequest;
    var record = before;
    if (registry.recoveryDecision(record, now_ns)) |decision| {
        const version = try distributed.commitVersion(record.identity.begin_timestamp);
        record = try Registry.advance(source, a, record, switch (decision) {
            .abort => .{ .abort = version },
            .commit => .{ .commit = .{ .version = version, .sync_level = record.identity.commit_sync_level } },
        });
    }
    if (record.phase == .locking) record = try Registry.advance(source, a, record, .lock_handoff);
    if (record.phase == .resolved and record.hasPreparedLocks() and !record.locks_released) record = try Registry.advance(source, a, record, .lock_release);
    if (record.phase == .resolved and (!record.hasPreparedLocks() or record.locks_released)) record = try Registry.advance(source, a, record, .handoff);
    return record;
}

/// Scoped COMMIT uses the existing external decision graph. The local seal is
/// durable before native admission; every later error is reconciliation debt,
/// never permission to run the ordinary commit coordinator under the same ID.
pub fn commitScoped(server: *http.ApiHttpServer, user: ?http.AuthenticatedIdentity, context: local.api_operation.RequestContext, id: sql_session.Id, tables: []const distributed.TableCommitRequest, abort: bool) !sql_session.CommitResult {
    const frame = context.sql_command_fence orelse return error.InvalidLockScope;
    if (abort and tables.len != 0) return error.InvalidTxnRequest;
    if (server.txn_sessions.durable == null) return error.SqlPreparedDurabilityUnavailable;
    const info = server.txn_sessions.getInfo(id) orelse return error.SqlTransactionNotActive;
    var state = (try server.txn_sessions.getSqlState(server.alloc, id)) orelse return error.SqlTransactionNotActive;
    defer state.deinit(server.alloc);
    const connection_id = state.connection_id orelse return error.SqlConnectionNotFound;
    if (state.owner_node_id != server.localSessionNodeId() or (state.metadata.accepted_lake_reads and tables.len != 0)) return error.UnsupportedSqlExecution;
    var region = std.heap.ArenaAllocator.init(server.alloc);
    defer region.deinit();
    const a = region.allocator();
    var current_user = user;
    var root: execution.Adapter = .{ .server = server, .identity = &current_user, .context = context, .database = state.metadata.database };
    var source: Registry = .{ .server = server, .context = context, .database_id = try databaseIdentity(&root, a) };
    if (source.database_id != frame.database) return error.InvalidLockScope;
    var cohort = try distributed.ExternalPreparation.init(a, server.catalogSource(), tables);
    defer cohort.deinit(a);
    try @import("sql_data_locks.zig").attachPreparation(server, a, context, id, &cohort);
    var transport: ParticipantTransport = undefined;
    if (cohort.manifest.len != 0) try transport.init(server);
    const encoded = try transactions.encodeExecutionPlan(a, tables);
    const actor = try @import("sql_authorization_recovery.zig").actor(server, user);
    source.actor = actor;
    var operation_id: [16]u8 = undefined;
    try server.embedding_provider_runtime.io.randomSecure(&operation_id);
    const cut = try source.snapshot(a, .{ .operation = operation_id });
    if (cut.standalone != frame.standalone or !std.mem.eql(u8, &cut.incarnation, &frame.backend.incarnation)) return error.CatalogGenerationChanged;
    const now = @import("antfly_platform").time.realtimeNs();
    const notification_plan: ?local.sql_ast.notifications.Plan = if (state.metadata.notifications.commands.items.len == 0) null else notification: {
        var connection = try @import("sql_connections.zig").loadWithCommand(server.txn_sessions.durable.?, a, connection_id, http.transactionPrincipal(user) orelse "", server.localSessionNodeId(), now / std.time.ns_per_ms, context.sql_command_fence);
        defer connection.deinit();
        break :notification .{ .connection = connection_id, .generation = connection.value.generation, .sender_pid = connection.value.notificationPid(), .commands = state.metadata.notifications.commands.items, .discard = abort };
    };
    const identity: registry.Identity = .{
        .notification_plan = notification_plan,
        .kind = .session_commit,
        .lock_scope = .{ .backend = frame.backend, .command = frame.command },
        .commit_sync_level = if (info.sync_level == .propose) .write else info.sync_level,
        .session = .{ .owner_node_id = state.owner_node_id, .connection_id = connection_id, .standalone = cut.standalone },
        .gid = "",
        .database_id = source.database_id,
        .native_service = context.native_service,
        .principal = actor.principal(),
        .txn_id = id,
        .operation_id = operation_id,
        .metadata_incarnation = std.fmt.bytesToHex(cut.incarnation, .lower),
        .begin_timestamp = info.begin_timestamp,
        .expires_at_unix_ns = try std.math.add(u64, now, 5 * std.time.ns_per_min),
        .plan_digest = try registry.planDigest(encoded),
        .manifest_digest = try registry.manifestDigest(a, cohort.manifest, std.fmt.bytesToHex(cut.incarnation, .lower)),
    };
    const seal: transactions.ExternalPreparationSeal = .{ .admission_head = cut.head, .admitted_at_ns = now, .standalone = cut.standalone, .identity = identity, .actor = actor, .participants = cohort.manifest };
    if (context.sql_completion) |completion| try completion.prepare_transaction(completion.ptr, id);
    var plan = (try server.txn_sessions.sealExternalEncodedExecutionPlan(server.alloc, id, seal, encoded)) orelse return error.SqlTransactionNotActive;
    defer plan.deinit();
    // A zero-row transaction can still own transaction locks. It traverses the
    // same durable decision and release barriers, without requiring a fictitious
    // table participant or constructing a transport that will never be called.
    deliver: {
        var record = if (abort or cohort.manifest.len == 0)
            Registry.admit(&source, a, identity, cohort.manifest) catch break :deliver
        else
            cohort.prepare(a, transport.worker(), source.authority(), identity, .{ .fanout_io = server.sharedApiIo(), .external_deadline_ns = context.deadline_ns }) catch break :deliver;
        if (cohort.manifest.len == 0) {
            if (abort) {
                record = Registry.advance(&source, a, record, .{ .abort = distributed.commitVersion(info.begin_timestamp) catch break :deliver }) catch break :deliver;
            } else {
                record = Registry.advance(&source, a, record, .prepare) catch break :deliver;
            }
            _ = recoverEmptyCohort(&source, a, record, now) catch break :deliver;
        } else {
            if (abort) record = Registry.advance(&source, a, record, .{ .abort = distributed.commitVersion(info.begin_timestamp) catch break :deliver }) catch break :deliver;
            _ = distributed.recoverExternalTransaction(a, transport.worker(), source.authority(), operation_id, now, .{ .fanout_io = server.sharedApiIo(), .external_deadline_ns = context.deadline_ns }) catch break :deliver;
        }
    }
    const bytes = (server.txn_sessions.externalPreparationSeal(server.alloc, id) catch return .{ .outcome = .unknown, .reconciliation_id = id }) orelse return .{ .outcome = .unknown, .reconciliation_id = id };
    defer server.alloc.free(bytes);
    // Exact native receipt adoption persists the terminal result before ACK.
    // Lost admission/reply and incomplete lock cleanup are owned by maintenance.
    recoverSessionWithContext(server, id, bytes, false, false, false, context) catch {};
    var observed = (server.txn_sessions.getSqlState(server.alloc, id) catch return .{ .outcome = .unknown, .reconciliation_id = id }) orelse return .{ .outcome = .unknown, .reconciliation_id = id };
    defer observed.deinit(server.alloc);
    // A terminal decision is durable before native lock retirement. Keep the
    // SQL command loan until its exact completion is acknowledged: otherwise a
    // successful ROLLBACK/COMMIT can leave locks visible to the next command.
    // Recovery resumes the same decision if the caller's deadline expires.
    if (observed.terminal_abort or observed.terminal != null) {
        drainSessionCompletion(server, id, context) catch return .{
            .outcome = if (observed.terminal_abort) .unknown else .committed_pending,
            .reconciliation_id = id,
        };
    }
    return .{ .outcome = if (observed.terminal_abort) .aborted else if (observed.terminal) |terminal| switch (terminal) {
        .committed => if (observed.terminal_repair_required) .committed_repair_required else .committed,
        .committed_recovery_pending, .committed_visibility_pending => .committed_pending,
    } else .unknown, .reconciliation_id = id };
}

/// Session ownership, current schema/RLS, predicates and FK/UNIQUE preparation
/// use the ordinary transaction pipeline. Only the durable decision authority
/// and the terminal SQL boundary differ from COMMIT.
pub fn execute(root: *execution.Adapter, session: *sql_session.Session, coordinator: *@import("sql_session.zig").Coordinator, a: A, command: ast.PreparedTransaction) !local.sql_runtime.Result {
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
        if (record.phase == .preparing or record.phase == .locking) return error.SqlPreparedTransactionNotFound;
        const committed = command.action == .commit;
        if (record.phase == .prepared) {
            const version = try distributed.commitVersion(record.identity.begin_timestamp);
            record = Registry.advance(&source, scratch, record, if (committed) .{ .commit = .{ .version = version, .sync_level = try local.sql_setting_catalog.mutationSyncLevel(root.setting_overlay) } } else .{ .abort = version }) catch return error.SqlTransactionOutcomeUnknown;
        }
        if (record.decision == null or (record.decision.?.status == .committed) != committed) return error.ExternalTransactionDecisionChanged;
        root.outcome_transaction_id = std.fmt.bytesToHex(record.identity.txn_id, .lower);
        var transport: ParticipantTransport = undefined;
        transport.init(server) catch return error.SqlTransactionOutcomeUnknown;
        const time = @import("antfly_platform").time;
        const deadline = root.context.deadline_ns orelse time.monotonicNs() +| 30 * std.time.ns_per_s;
        source.context.deadline_ns = deadline;
        transport.local_worker.recovery_deadline_ns = deadline;
        if (transport.hosted) transport.hosted_worker.recovery_deadline_ns = deadline;
        while (true) {
            root.context.ensureActive() catch return error.SqlTransactionOutcomeUnknown;
            if (time.monotonicNs() >= deadline) return error.SqlTransactionOutcomeUnknown;
            const finished = pass: {
                // Reclaim each pass's snapshots and transport allocations. A
                // large lock set must not accumulate one arena per quantum.
                var pass_region = std.heap.ArenaAllocator.init(server.alloc);
                defer pass_region.deinit();
                const temporary = pass_region.allocator();
                const advanced = distributed.recoverExternalTransaction(temporary, transport.worker(), source.authority(), record.identity.operation_id, time.realtimeNs(), .{ .fanout_io = server.sharedApiIo(), .external_deadline_ns = deadline }) catch |err| switch (err) {
                    // The recovery driver yields after eight durable quanta. Its
                    // incomplete-decision result is resumable for this exact
                    // admitted operation, including lock-only participants.
                    error.CommitDecisionUnknown, error.CatalogGenerationChanged, error.MetadataMutationOutcomeUnknown, error.CatalogPublicationProofPending => break :pass false,
                    error.SqlPreparedTransactionNotFound => {
                        // Another controller can finish this exact admitted
                        // decision. Only absence in the same durable realm is
                        // its retirement witness; SQL/GID lookup is not replayed.
                        const completed = try source.snapshot(temporary, .{ .operation = record.identity.operation_id });
                        try Registry.checkRealm(completed, record.identity);
                        if (completed.records.len != 0) break :pass false;
                        break :pass true;
                    },
                    else => {
                        std.log.warn("prepared SQL resolution rejected err={s}", .{@errorName(err)});
                        return error.SqlTransactionOutcomeUnknown;
                    },
                };
                break :pass advanced.phase == .retired;
            };
            if (finished) break;
            try server.embedding_provider_runtime.io.sleep(.fromMilliseconds(1), .awake);
        }
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
    const prior_execution = coordinator.borrowed_execution;
    defer coordinator.borrowed_execution = prior_execution;
    coordinator.borrowed_execution = &lease;
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
    if (state.metadata.notifications.commands.items.len != 0) return error.UnsupportedSqlExecution;
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
    try @import("sql_data_locks.zig").attachPreparation(server, scratch, root.context, id, &cohort);
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
    const identity: registry.Identity = .{ .lock_scope = if (root.context.sql_command_fence) |fence| .{ .backend = fence.backend, .command = fence.command } else null, .session = .{ .owner_node_id = server.localSessionNodeId(), .connection_id = state.connection_id, .standalone = cut.standalone }, .gid = command.gid, .database_id = source.database_id, .native_service = root.context.native_service, .principal = actor.principal(), .txn_id = id, .operation_id = operation_id, .metadata_incarnation = std.fmt.bytesToHex(cut.incarnation, .lower), .begin_timestamp = info.begin_timestamp, .expires_at_unix_ns = try std.math.add(u64, now, 5 * std.time.ns_per_min), .plan_digest = try registry.planDigest(encoded), .manifest_digest = try registry.manifestDigest(scratch, cohort.manifest, std.fmt.bytesToHex(cut.incarnation, .lower)) };
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
