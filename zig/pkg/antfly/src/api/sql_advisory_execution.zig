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

//! Native advisory execution. A SQL occurrence owns one immutable ledger
//! effect. Native control requests may resume; the SQL occurrence is never
//! replayed. Every step is one observation and at most one bounded mutation.
const std = @import("std");
const local = @import("antfly_local_sources");
const effects = local.sql_advisory_effects;
const scalar = local.sql_scalar;
const authority = @import("../system_catalog/lock_authority.zig");
const producer = @import("sql_command_producer.zig");
const transactions = @import("transactions.zig");
const A = std.mem.Allocator;
const ledger = authority.scopes.ledger;
pub const Transaction = @FieldType(authority.locks.Scope, "transaction");

pub const Frame = struct {
    source: *transactions.DurableSessionStore,
    connection: [32]u8,
    principal: []const u8,
    instance: [16]u8,
    fence: authority.scopes.CommandFence,
    actor: authority.prepared.Actor,

    pub fn requireExecuting(self: Frame, a: A, now_ms: u64) !void {
        try self.fence.validate();
        try self.actor.validate();
        var owned = (try producer.load(self.source, a, self.connection)) orelse return error.SqlConnectionNotFound;
        defer owned.deinit();
        const record = owned.value;
        if (record.phase != .executing or record.recovery or record.control_effect != null or (record.pending != null and record.pending.?.operation != .renew) or record.ticket == null or
            record.registration.lease.expires_at_ms <= now_ms or !std.mem.eql(u8, record.principal, self.principal) or
            !std.mem.eql(u8, &record.instance, &self.instance)) return error.SqlConnectionBusy;
        const expected: authority.scopes.CommandFence = .{ .group = record.registration.group, .backend = record.registration.lease.backend, .command = record.ticket.?.command, .database = record.registration.lease.database, .principal = record.registration.lease.principal, .standalone = record.registration.standalone };
        if (!std.meta.eql(expected, self.fence) or !std.meta.eql(record.actor.session_user, self.actor.session_user) or
            !std.mem.eql(u8, record.actor.credential, self.actor.credential)) return error.SqlConnectionChanged;
    }
    fn query(self: Frame, effect: ?ledger.Effect) authority.Query {
        return .{ .incarnation = self.fence.backend.incarnation, .standalone = self.fence.standalone, .actor = self.actor, .database = self.fence.database, .backend = self.fence.backend, .effect = effect };
    }
    fn observe(self: Frame, a: A, port: producer.Port, effect: ?ledger.Effect, now_ms: u64) !std.json.Parsed(authority.Snapshot) {
        const bytes = try port.call(port.ptr, a, .{ .sql_lock_read = self.query(effect) });
        defer a.free(bytes);
        if (bytes.len > 32768) return error.InvalidSqlBackendResponse;
        var owned = try std.json.parseFromSlice(authority.Snapshot, a, bytes, .{ .allocate = .alloc_always });
        errdefer owned.deinit();
        const cut = owned.value;
        if (!std.mem.eql(u8, &cut.incarnation, &self.fence.backend.incarnation) or cut.standalone != self.fence.standalone or cut.group != self.fence.group or cut.next_work != null) return error.CatalogGenerationChanged;
        const scope = cut.scope orelse return error.SqlConnectionNotFound;
        const owner = cut.owner orelse return error.InvalidSqlBackendResponse;
        const ns = self.namespace();
        try scope.validate(ns);
        try owner.validate(ns);
        if (!std.meta.eql(owner.lease, scope.registration.lease) or
            !std.mem.eql(u8, &scope.registration.connection, &self.connection) or !scope.registration.lease.backend.eql(self.fence.backend) or
            scope.registration.lease.database != self.fence.database or scope.registration.standalone != self.fence.standalone or
            !std.mem.eql(u8, &scope.registration.lease.principal, &self.fence.principal)) return error.InvalidSqlBackendResponse;
        if (scope.state != .active or scope.releasing or scope.registration.lease.expires_at_ms <= now_ms or
            !std.meta.eql(scope.command, @as(?authority.scopes.Command, self.fence.command)) or
            (scope.transaction != null and scope.transaction.?.decision != null)) return error.SqlConnectionBusy;
        if (owner.state != .active) return error.SqlConnectionBusy;
        if (owner.transaction) |id| if (scope.transaction == null or !std.mem.eql(u8, &scope.transaction.?.id, &id)) return error.InvalidSqlBackendResponse;
        return owned;
    }
    fn namespace(self: Frame) ledger.Namespace {
        return .{ .home = .metadata, .group = self.fence.group, .incarnation = self.fence.backend.incarnation };
    }
    fn request(self: Frame, head: u64, nonce: [16]u8, operation: @FieldType(authority.Request, "operation")) authority.Request {
        return .{ .admission = .{ .head = head, .nonce = nonce }, .incarnation = self.fence.backend.incarnation, .standalone = self.fence.standalone, .database = self.fence.database, .actor = self.actor, .backend = self.fence.backend, .operation = operation };
    }
};

pub const Machine = struct {
    frame: Frame,
    function: effects.Function,
    action: ledger.Action,
    nonce: [16]u8,
    transaction: ?Transaction,
    effect: ?ledger.Effect = null,
    pending: ?authority.Request = null,
    wait_resource: ?ledger.ResourceStamp = null,
    value: ?scalar.Datum = null,
    pub const Progress = enum { advanced, waiting, complete };

    pub fn init(frame: Frame, function: effects.Function, args: []const scalar.Datum, transaction: ?Transaction, nonce: [16]u8) !Machine {
        try frame.fence.validate();
        if (!function.validArity(args.len) or std.mem.allEqual(u8, &nonce, 0)) return error.InvalidSqlParameters;
        for (args) |arg| {
            try arg.validateTemporal();
            if (arg.sql_null or arg.missing or arg.numeric != null or arg.array != null or arg.extension_kind != .none or arg.value != .integer) return error.SqlTypeMismatch;
        }
        const action: ledger.Action = if (function == .pg_advisory_unlock_all)
            .{ .control = .unlock_all }
        else action: {
            const key: authority.locks.AdvisoryKey = if (args.len == 1) .{ .single = args[0].value.integer } else .{ .pair = .{
                .first = std.math.cast(i32, args[0].value.integer) orelse return error.SqlNumericOutOfRange,
                .second = std.math.cast(i32, args[1].value.integer) orelse return error.SqlNumericOutOfRange,
            } };
            const resource = try authority.locks.advisoryResource(frame.fence.backend.incarnation, frame.fence.database, key);
            const mode: authority.locks.AdvisoryMode = if (function.shared()) .shared else .exclusive;
            if (function.unlocks()) break :action .{ .unlock = .{ .resource = resource, .mode = mode } };
            const scope: authority.locks.Scope = if (function.transactional()) .{ .transaction = transaction orelse return error.SqlTransactionNotActive } else .session;
            if (scope == .transaction and std.mem.allEqual(u8, &scope.transaction.id, 0)) return error.InvalidLockScope;
            const acquisition: ledger.Acquisition = .{ .resource = resource, .mode = .{ .advisory = mode }, .scope = scope };
            break :action if (function.tries()) .{ .try_acquire = acquisition } else .{ .acquire = acquisition };
        };
        return .{ .frame = frame, .function = function, .action = action, .nonce = nonce, .transaction = transaction };
    }
    fn terminal(self: *Machine, receipt: ledger.Receipt) !bool {
        try receipt.requireFor(self.frame.namespace(), self.effect.?);
        self.value = switch (receipt.outcome) {
            .pending => return false,
            .cancelled => return error.Canceled,
            .deadlocked => return error.SqlDeadlockDetected,
            .granted => if (self.function.tries()) .json(.{ .bool = true }) else if (!self.function.booleanResult() and self.function != .pg_advisory_unlock_all) .voidValue() else return error.InvalidSqlBackendResponse,
            .contended => if (self.function.tries()) .json(.{ .bool = false }) else return error.InvalidSqlBackendResponse,
            .unlocked => if (self.function.unlocks()) .json(.{ .bool = true }) else return error.InvalidSqlBackendResponse,
            .not_held => if (self.function.unlocks()) .json(.{ .bool = false }) else return error.InvalidSqlBackendResponse,
            .completed => if (self.function == .pg_advisory_unlock_all) .voidValue() else return error.InvalidSqlBackendResponse,
        };
        return true;
    }
    fn rememberWait(self: *Machine, stamp: ?ledger.ResourceStamp) !void {
        if (self.action == .acquire) {
            const value = stamp orelse return error.InvalidSqlBackendResponse;
            try value.validate();
            if (!std.mem.eql(u8, &value.resource, &self.action.resource())) return error.InvalidSqlBackendResponse;
            self.wait_resource = value;
        } else if (stamp != null) return error.InvalidSqlBackendResponse;
    }
    pub fn step(self: *Machine, a: A, port: producer.Port, now_ms: u64, control_nonce: [16]u8) !Progress {
        // A completed machine cannot issue another effect, even if called by
        // a second observer. Reuse across SQL occurrences is forbidden.
        if (self.value != null) return .complete;
        var region = std.heap.ArenaAllocator.init(a);
        defer region.deinit();
        const scratch = region.allocator();
        try self.frame.requireExecuting(scratch, now_ms);
        var observed = try self.frame.observe(scratch, port, self.effect, now_ms);
        defer observed.deinit();
        const cut = observed.value;
        const owner = cut.owner.?;
        if (self.effect == null) {
            self.effect = .{ .backend = self.frame.fence.backend, .sequence = owner.next_effect, .nonce = self.nonce, .action = self.action };
            try self.effect.?.validate();
        } else if (cut.receipt) |receipt| {
            if (try self.terminal(receipt)) return .complete;
        }
        if (owner.next_effect != self.effect.?.sequence) return error.LockEffectExpired;
        if (owner.waiting) |waiting| {
            if (!std.meta.eql(waiting, self.effect.?)) return error.LockOwnerBusy;
            const stamp = cut.wait_resource orelse return error.InvalidSqlBackendResponse;
            try stamp.validate();
            if (!std.mem.eql(u8, &stamp.resource, &self.action.resource())) return error.InvalidSqlBackendResponse;
            // An unchanged resource cannot grant this queued acquisition.
            // Polling must not append another journal result or repeat graph
            // planning while its actual conflict frontier is unchanged.
            if (self.pending == null and std.meta.eql(self.wait_resource, @as(?ledger.ResourceStamp, stamp))) return .waiting;
        }
        if (owner.job) |job| if (!std.meta.eql(job.effect, self.effect.?)) return error.LockOwnerBusy;
        const operation: @FieldType(authority.Request, "operation") = if (self.function.transactional() and cut.scope.?.transaction == null)
            .{ .bind = .{ .command = self.frame.fence.command, .transaction = self.transaction.?.id } }
        else if (owner.job != null)
            .{ .step = .{ .command = self.frame.fence.command, .effect = self.effect.? } }
        else
            .{ .effect = .{ .command = self.frame.fence.command, .effect = self.effect.? } };
        if (self.function.transactional()) if (cut.scope.?.transaction) |transaction| if (!std.mem.eql(u8, &transaction.id, &self.transaction.?.id)) return error.InvalidLockScope;
        if (self.pending) |pending| {
            // A native transaction binding proves this exact command's bind
            // completed even when its control reply was lost.
            if (!std.meta.eql(pending.operation, operation) or (cut.head >= pending.admission.head and cut.head - pending.admission.head >= authority.outcomes.retained)) self.pending = null;
        }
        if (self.pending == null) {
            self.pending = self.frame.request(cut.head, control_nonce, operation);
            // This request has never escaped. Submit directly instead of
            // paying for a guaranteed-empty prior-outcome lookup.
            const result = port.submit(scratch, self.pending.?) catch |err| {
                if (std.meta.stringToEnum(authority.outcomes.Rejection, @errorName(err)) != null) self.pending = null;
                if (err == error.CatalogPublicationProofPending) return .waiting;
                return err;
            };
            self.pending = null;
            if (operation != .bind) {
                const issued = result.lock_effect orelse return error.InvalidSqlBackendResponse;
                if (try self.terminal(issued.receipt)) return .complete;
                try self.rememberWait(issued.wait_resource);
                return if (self.action == .acquire) .waiting else .advanced;
            }
            if (result.lock_effect != null) return error.InvalidSqlBackendResponse;
            return .advanced;
        }
        if (try port.deliver(scratch, self.pending.?)) |failure| {
            self.pending = null;
            if (failure == .CatalogPublicationProofPending) return .waiting;
            return switch (failure) {
                inline else => |tag| @field(anyerror, @tagName(tag)),
            };
        }
        self.pending = null;
        if (owner.waiting != null) try self.rememberWait(cut.wait_resource);
        return if (owner.waiting != null) .waiting else .advanced;
    }
};

/// The enclosing execution supplies cancellation/deadline checks and std.Io
/// waiting. Tests can pin time and delivery cuts without OS-thread primitives.
pub const Runtime = struct {
    ptr: *anyopaque,
    now_ms: *const fn (*anyopaque) u64,
    nonce: *const fn (*anyopaque) anyerror![16]u8,
    active: *const fn (*anyopaque) anyerror!void,
    pause: *const fn (*anyopaque, u32) anyerror!void,
};
pub const Owner = struct {
    frame: Frame,
    port: producer.Port,
    runtime: Runtime,
    transaction: ?Transaction = null,
    pub fn provider(self: *Owner) effects.Provider {
        return .{ .ptr = self, .invoke = Owner.invoke };
    }
    fn invoke(raw: *anyopaque, a: A, function: effects.Function, args: []const scalar.Datum) anyerror!scalar.Datum {
        const self: *Owner = @ptrCast(@alignCast(raw));
        try self.runtime.active(self.runtime.ptr);
        var machine = try Machine.init(self.frame, function, args, self.transaction, try self.runtime.nonce(self.runtime.ptr));
        var backoff_ms: u32 = 1;
        var quanta: u8 = 0;
        while (true) {
            try self.runtime.active(self.runtime.ptr);
            const progress = machine.step(a, self.port, self.runtime.now_ms(self.runtime.ptr), try self.runtime.nonce(self.runtime.ptr)) catch |err| switch (err) {
                // Resume the exact native frame after uncertain delivery;
                // neither the scalar occurrence nor a successful grant repeats.
                error.MetadataMutationOutcomeUnknown => .waiting,
                else => return err,
            };
            switch (progress) {
                .complete => return machine.value.?,
                .waiting => {
                    try self.runtime.pause(self.runtime.ptr, backoff_ms);
                    backoff_ms = @min(backoff_ms * 2, 32);
                    quanta = 0;
                },
                .advanced => {
                    backoff_ms = 1;
                    quanta += 1;
                    if (quanta == 8) {
                        try self.runtime.pause(self.runtime.ptr, 1);
                        quanta = 0;
                    }
                },
            }
        }
    }
};

/// Production provider boundary. The command frame is taken by ingress; neither
/// preparation nor an immutable plan can manufacture or retain that authority.
pub fn invoke(raw: *anyopaque, a: A, function: effects.Function, args: []const scalar.Datum) anyerror!scalar.Datum {
    const root: *@import("sql_execution.zig").Adapter = @ptrCast(@alignCast(raw));
    const fence = root.context.sql_command_fence orelse return error.AdvisoryProviderUnavailable;
    const connection = root.connection_id orelse return error.SqlConnectionNotFound;
    const store = root.server.txn_sessions.durable orelse return error.SqlConnectionUnavailable;
    const io = root.server.embedding_provider_runtime.io;
    const Hooks = struct {
        fn now(_: *anyopaque) u64 {
            return @import("antfly_platform").time.realtimeNs() / std.time.ns_per_ms;
        }
        fn nonce(ptr: *anyopaque) ![16]u8 {
            const adapter: *@import("sql_execution.zig").Adapter = @ptrCast(@alignCast(ptr));
            var result: [16]u8 = undefined;
            try adapter.server.embedding_provider_runtime.io.randomSecure(&result);
            return result;
        }
        fn active(ptr: *anyopaque) !void {
            const adapter: *@import("sql_execution.zig").Adapter = @ptrCast(@alignCast(ptr));
            try adapter.context.ensureActive();
        }
        fn pause(ptr: *anyopaque, ms: u32) !void {
            const adapter: *@import("sql_execution.zig").Adapter = @ptrCast(@alignCast(ptr));
            try active(ptr);
            try adapter.server.embedding_provider_runtime.io.sleep(.fromMilliseconds(ms), .awake);
            try active(ptr);
        }
    };
    // Decide from the invoked effect, after scalar STRICT/CASE/filter handling.
    // This also covers effects hidden in SQL routines, operators and casts.
    const transaction: ?Transaction = if (root.active_transaction) |id|
        .{ .id = id, .subtransaction = root.active_subtransaction }
    else if (root.implicit_advisory) |scope|
        if (function.transactional() or scope.session.transaction_id != null)
            .{ .id = try scope.transaction(), .subtransaction = 0 }
        else
            null
    else
        null;
    var port: producer.NativePort = .{ .server = root.server, .context = root.context };
    var owner: Owner = .{
        .frame = .{
            .source = store,
            .connection = connection,
            .principal = @import("http_server.zig").transactionPrincipal(root.identity.*) orelse "",
            .instance = try root.server.sql_connection_admission.executionInstance(io),
            .fence = fence.*,
            .actor = try @import("sql_authorization_recovery.zig").actor(root.server, root.identity.*),
        },
        .port = port.port(),
        .runtime = .{ .ptr = root, .now_ms = Hooks.now, .nonce = Hooks.nonce, .active = Hooks.active, .pause = Hooks.pause },
        .transaction = transaction,
    };
    const warning: ?local.sql_catalog.notices.Notice = if (function.unlocks()) .{
        .code = "01000",
        .severity = .warning,
        .message = if (function.shared()) "you don't own a lock of type ShareLock" else "you don't own a lock of type ExclusiveLock",
    } else null;
    if (warning) |notice| if (root.notice_sink) |sink| if (sink.reserve) |reserve|
        try reserve(sink.ptr, 1, notice.message.len);
    const result = try Owner.invoke(&owner, a, function, args);
    if (warning) |notice| if (!result.sql_null and result.value == .bool and !result.value.bool) {
        if (root.notice_sink) |sink| try sink.emit(sink.ptr, notice);
    };
    return result;
}
