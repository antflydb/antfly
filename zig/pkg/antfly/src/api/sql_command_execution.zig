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

//! Shared ingress command ownership. Control can resume; an execution permit
//! is taken once and never reissued by recovery or a fresh request.
const std = @import("std");
const source = @import("sql_command_producer.zig");
const authority = @import("../system_catalog/lock_authority.zig");
const admission = @import("sql_connection_admission.zig");
const A = std.mem.Allocator;
pub const Runtime = @import("sql_advisory_execution.zig").Runtime;

pub const Controller = struct {
    store: *@import("transactions.zig").DurableSessionStore,
    connection: [32]u8,
    principal: []const u8,
    actor: authority.prepared.Actor,
    instance: [16]u8,
    node: u64,
    standalone: bool,
    port: source.Port,
    runtime: Runtime,
    /// Borrowed only by a request-local Native port. Connection validation
    /// publishes its authenticated capsule before assisting earlier controls,
    /// which can precede the new SQL body's Request.attach call.
    relation_owner_context: ?*?@import("sql_relation_lifetimes.zig").Owner = null,

    fn nonce(self: Controller) ![16]u8 {
        return self.runtime.nonce(self.runtime.ptr);
    }
    fn now(self: Controller) u64 {
        return self.runtime.now_ms(self.runtime.ptr);
    }
    fn check(self: Controller) !void {
        try self.runtime.active(self.runtime.ptr);
    }
    fn load(self: Controller, a: A) !std.json.Parsed(source.Record) {
        var owned = (try source.load(self.store, a, self.connection)) orelse return error.SqlConnectionNotFound;
        errdefer owned.deinit();
        const record = owned.value;
        if (record.registration.lease.node != self.node or record.registration.standalone != self.standalone or
            !std.mem.eql(u8, record.principal, self.principal) or !std.meta.eql(record.actor.session_user, self.actor.session_user) or
            !std.mem.eql(u8, record.actor.credential, self.actor.credential) or !std.meta.eql(record.actor.current_role, self.actor.current_role)) return error.SqlConnectionChanged;
        return owned;
    }
    fn validateConnection(self: Controller, a: A, database: ?[]const u8) !?@import("sql_relation_lifetimes.zig").Owner {
        if (self.relation_owner_context) |slot| slot.* = null;
        var connection = try @import("sql_connections.zig").load(self.store, a, self.connection, self.principal, self.node, self.now());
        defer connection.deinit();
        if (database) |name| if (!std.mem.eql(u8, connection.value.database, name)) return error.SqlConnectionChanged;
        if (connection.value.state == .beginning or connection.value.state == .uncertain) return error.SqlConnectionBusy;
        // Connections predating a process-bound wire identity retain their
        // ordinary SQL behavior, but cannot lend authority for TEMP storage.
        if (connection.value.wire_epoch == null) return null;
        const owner = try @import("sql_relation_lifetimes.zig").ownerFromConnection(connection.value, self.instance, self.node);
        if (self.relation_owner_context) |slot| slot.* = owner;
        return owner;
    }
    fn advance(self: Controller, a: A, record: source.Record) !void {
        source.advance(self.store, a, self.principal, record, self.instance, self.now(), try self.nonce(), self.port) catch |err| switch (err) {
            error.MetadataMutationOutcomeUnknown, error.SqlConnectionChanged => {},
            else => return err,
        };
    }
    /// The supplied logical connection already exists. Discovery point-reads
    /// the database and capability anchor instead of scanning owners/jobs.
    pub fn enroll(self: Controller, a: A, database: []const u8) !void {
        try self.check();
        _ = try self.validateConnection(a, database);
        if (!self.store.supportsSharedMutations()) return error.SqlConnectionUnavailable;
        var existing = try source.load(self.store, a, self.connection);
        defer if (existing) |*owned| owned.deinit();
        if (existing == null) {
            const bytes = try self.port.call(self.port.ptr, a, .{ .sql_lock_read = .{ .incarnation = @splat(0), .standalone = self.standalone, .actor = self.actor, .database = 0, .discovery = database } });
            defer a.free(bytes);
            if (bytes.len > 4096) return error.InvalidSqlBackendResponse;
            var observed = try std.json.parseFromSlice(authority.Snapshot, a, bytes, .{});
            defer observed.deinit();
            const cut = observed.value;
            if (cut.group == 0 or cut.standalone != self.standalone or std.mem.allEqual(u8, &cut.incarnation, 0) or cut.database_id == null or cut.database_id.? == 0 or
                cut.scope != null or cut.owner != null or cut.next_work != null or cut.receipt != null or cut.wait_resource != null) return error.InvalidSqlBackendResponse;
            try self.check();
            const registration: authority.scopes.Registration = .{ .group = cut.group, .connection = self.connection, .nonce = try self.nonce(), .standalone = self.standalone, .lease = .{
                .backend = .{ .incarnation = cut.incarnation, .id = try self.nonce(), .fence = 1 },
                .database = cut.database_id.?,
                .node = self.node,
                .principal = try authority.principalDigest(a, self.actor.session_user),
                .expires_at_ms = self.now() +| source.execution_lease_ms,
            } };
            source.create(self.store, self.principal, registration, self.actor, self.instance, cut.head, try self.nonce(), self.now()) catch |err| switch (err) {
                error.SqlConnectionAlreadyExists => {},
                else => return err,
            };
        }
        for (0..16) |_| {
            try self.check();
            var owned = try self.load(a);
            defer owned.deinit();
            if (owned.value.recovery or owned.value.phase == .retired) return error.SqlConnectionNotFound;
            if (owned.value.phase == .idle and owned.value.pending == null) return;
            if (owned.value.phase != .registering and !(owned.value.body_skipped and (owned.value.phase == .beginning or owned.value.phase == .finishing)) and !(owned.value.phase == .idle and owned.value.pending != null and owned.value.pending.?.operation == .renew)) return error.SqlConnectionBusy;
            try self.advance(a, owned.value);
        }
        return error.SqlConnectionBusy;
    }
    /// Assist only control that cannot evaluate a body. An old ready or
    /// executing command is never adopted, even for byte-identical SQL.
    pub fn take(self: Controller, a: A, body: [32]u8) !Loan {
        try self.check();
        var relation_owner = try self.validateConnection(a, null);
        for (0..16) |_| {
            var owned = try self.load(a);
            defer owned.deinit();
            if (owned.value.recovery or owned.value.phase == .retired) return error.SqlConnectionNotFound;
            if (owned.value.phase == .idle and owned.value.pending == null) break;
            if (owned.value.phase != .finishing and !(owned.value.phase == .beginning and owned.value.body_skipped) and !(owned.value.phase == .idle and owned.value.pending != null and owned.value.pending.?.operation == .renew)) return error.SqlConnectionBusy;
            try self.check();
            try self.advance(a, owned.value);
            // Assisting an earlier DISCARD can advance the connection's
            // generation. Its successor command must pin the successor owner.
            relation_owner = try self.validateConnection(a, null);
        }
        var prior = try self.load(a);
        defer prior.deinit();
        // Completion remains publishable after request/task cancellation.
        const finish_nonce = try self.nonce();
        // Executor identifies the API process, including TEMP ownership and
        // native lock admission. Sequence and nonce identify each command.
        const command: authority.scopes.Command = .{ .sequence = prior.value.next_command, .nonce = try self.nonce(), .executor = self.instance };
        const ticket = try source.beginObserved(self.store, a, self.principal, prior.value, self.instance, command, body, try self.nonce(), self.now(), self.port);
        errdefer self.abandon(a, ticket, finish_nonce) catch {};
        for (0..16) |_| {
            try self.check();
            var owned = try self.load(a);
            defer owned.deinit();
            if (owned.value.recovery or !std.meta.eql(owned.value.ticket, @as(?source.Ticket, ticket))) return error.SqlConnectionBusy;
            if (owned.value.phase == .ready) {
                const fence = try source.take(self.store, self.principal, owned.value, self.instance, ticket, self.now());
                return .{ .controller = self, .ticket = ticket, .fence = fence, .finish_nonce = finish_nonce, .relation_owner = relation_owner };
            }
            if (owned.value.phase != .beginning) return error.SqlConnectionBusy;
            try self.advance(a, owned.value);
        }
        return error.SqlConnectionBusy;
    }
    fn abandon(self: Controller, a: A, ticket: source.Ticket, nonce_value: [16]u8) !void {
        for (0..8) |_| {
            var owned = try self.load(a);
            defer owned.deinit();
            if (!std.meta.eql(owned.value.ticket, @as(?source.Ticket, ticket)) or owned.value.recovery or owned.value.body_skipped) return;
            if (owned.value.phase != .beginning and owned.value.phase != .ready) return;
            source.skipBeforeTake(self.store, self.principal, owned.value, self.instance, ticket, nonce_value) catch |err| switch (err) {
                error.SqlConnectionChanged => continue,
                else => return err,
            };
            return;
        }
        return error.SqlConnectionBusy;
    }
    pub const RetirementProgress = enum { complete, pending };
    /// Pending denotes only unfinished retirement, never a connection guard
    /// refusal or failed child control. One slice retains the bounded quantum.
    pub fn retireSlice(self: Controller, a: A) !RetirementProgress {
        try self.check();
        var connection = try @import("sql_connections.zig").load(self.store, a, self.connection, self.principal, self.node, self.now());
        defer connection.deinit();
        if (connection.value.state != .idle) return error.SqlConnectionBusy;
        for (0..16) |_| {
            try self.check();
            var owned = try self.load(a);
            defer owned.deinit();
            if (owned.value.phase == .retired and owned.value.temporaryRetirementComplete()) return .complete;
            if (!owned.value.recovery) {
                source.requestRecovery(self.store, self.principal, owned.value) catch |err| switch (err) {
                    error.SqlConnectionChanged => continue,
                    else => return err,
                };
            } else try self.advance(a, owned.value);
        }
        return .pending;
    }
    pub fn retireAndWait(self: Controller, a: A) !void {
        if (try self.retireSlice(a) == .pending) return error.SqlConnectionBusy;
    }
    /// Native foreground use supplies an absolute request deadline. Yield
    /// between slices so workers and the metadata owner can make progress.
    pub fn awaitRetirement(self: Controller, a: A) !void {
        while (try self.retireSlice(a) == .pending) {
            try self.check();
            try self.runtime.pause(self.runtime.ptr, 2);
            try self.check();
        }
    }
    pub fn retire(self: Controller, a: A) !void {
        var owned = try self.load(a);
        defer owned.deinit();
        try source.requestRecovery(self.store, self.principal, owned.value);
    }
};
pub const Loan = struct {
    controller: Controller,
    ticket: source.Ticket,
    fence: authority.scopes.CommandFence,
    finish_nonce: [16]u8,
    relation_owner: ?@import("sql_relation_lifetimes.zig").Owner = null,
    finished: std.atomic.Value(bool) = .init(false),
    /// Bind liveness after durable take and before executing any SQL. A pool
    /// entry by itself is never proof that this particular command is live.
    pub fn bind(self: *Loan, io: std.Io, lease: *admission.Lease) !void {
        if (!std.meta.eql(lease.key, admission.Key.init(self.controller.connection, self.controller.principal))) return error.SqlConnectionChanged;
        if (self.finished.load(.acquire)) return error.SqlConnectionBusy;
        const owner = self.controller;
        try owner.check();
        var owned = try owner.load(owner.store.alloc);
        defer owned.deinit();
        const prior = owned.value;
        if (!std.mem.eql(u8, &owner.instance, &self.ticket.instance) or !prior.registration.lease.backend.eql(self.fence.backend) or
            prior.recovery or prior.phase != .executing or !std.meta.eql(prior.ticket, @as(?source.Ticket, self.ticket)) or
            prior.registration.lease.expires_at_ms <= owner.now()) return error.SqlConnectionBusy;
        try lease.bindCommand(io, self.ticket.command);
    }
    /// A heartbeat borrows the exact admission, never merely a connection ID.
    /// The source CAS serializes renewal with completion/savepoint publication.
    pub fn keepAlive(self: *const Loan, a: A, io: std.Io, pool: *admission.Pool) !u64 {
        if (self.finished.load(.acquire)) return error.SqlConnectionBusy;
        const owner = self.controller;
        try owner.check();
        if (!try pool.executing(io, .init(owner.connection, owner.principal), self.ticket.command)) return error.SqlConnectionBusy;
        var owned = try owner.load(a);
        defer owned.deinit();
        if (!std.mem.eql(u8, &owner.instance, &self.ticket.instance) or !std.meta.eql(owned.value.ticket, @as(?source.Ticket, self.ticket)) or owned.value.phase != .executing or owned.value.recovery) return error.SqlConnectionBusy;
        try source.maintain(owner.store, a, owned.value, owner.instance, owner.now(), try owner.nonce(), owner.port, .{ .command = self.ticket.command });
        var refreshed = try owner.load(a);
        defer refreshed.deinit();
        if (!std.meta.eql(refreshed.value.ticket, @as(?source.Ticket, self.ticket)) or refreshed.value.phase != .executing or refreshed.value.recovery) return error.SqlConnectionBusy;
        return refreshed.value.registration.lease.expires_at_ms;
    }
    /// Publication is local and does not inspect a cancelled request context.
    /// Independent maintenance subsequently owns all remaining control work.
    /// Retire this exact owner after failed cleanup. An old copied loan can
    /// never retire a newer command or a replacement backend.
    fn retire(self: *Loan, a: A) !bool {
        for (0..8) |_| {
            var owned = try self.controller.load(a);
            defer owned.deinit();
            const prior = owned.value;
            if (!prior.registration.lease.backend.eql(self.fence.backend)) return error.SqlConnectionChanged;
            if (prior.recovery) {
                self.finished.store(true, .release);
                return true;
            }
            if (self.finished.load(.acquire) or prior.next_command > self.ticket.command.sequence) return false;
            if (!std.meta.eql(prior.ticket, @as(?source.Ticket, self.ticket))) return error.SqlConnectionChanged;
            source.requestRecovery(self.controller.store, self.controller.principal, prior) catch |err| switch (err) {
                error.SqlConnectionChanged => continue,
                else => return err,
            };
            self.finished.store(true, .release);
            return true;
        }
        return error.SqlConnectionBusy;
    }
    pub fn finish(self: *Loan, a: A) !void {
        if (self.finished.load(.acquire)) return;
        for (0..8) |_| {
            var owned = try self.controller.load(a);
            defer owned.deinit();
            const prior = owned.value;
            if (!prior.registration.lease.backend.eql(self.fence.backend)) return error.SqlConnectionChanged;
            // Native completion can already have advanced the sequence. A
            // delayed copied loan must neither fail cleanup nor finish the
            // newer command admitted on this same logical connection.
            if (prior.next_command > self.ticket.command.sequence and !prior.recovery) {
                self.finished.store(true, .release);
                return;
            }
            if (!std.meta.eql(prior.ticket, @as(?source.Ticket, self.ticket))) return error.SqlConnectionChanged;
            if (prior.phase == .finishing) {
                self.finished.store(true, .release);
                return;
            }
            const head = if (prior.pending) |pending| pending.admission.head else 0;
            source.finish(self.controller.store, self.controller.principal, prior, self.controller.instance, self.ticket, head, self.finish_nonce) catch |err| switch (err) {
                // Heartbeats and savepoint cleanup share this source CAS.
                // Retry local publication using the same reserved nonce.
                error.SqlConnectionChanged => continue,
                else => return err,
            };
            self.finished.store(true, .release);
            return;
        }
        return error.SqlConnectionBusy;
    }
};

/// Borrowed production hooks; callers initialize this at its final address.
pub const Native = struct {
    server: *@import("http_server.zig").ApiHttpServer,
    context: @import("antfly_local_sources").api_operation.RequestContext,
    pub fn awaitRetirement(self: *Native, a: A, controller_value: Controller) !void {
        const prior_context = self.context;
        defer self.context = prior_context;
        self.context = try retirementContext(prior_context);
        try controller_value.awaitRetirement(a);
    }
    fn retirementContext(request: @import("antfly_local_sources").api_operation.RequestContext) !@import("antfly_local_sources").api_operation.RequestContext {
        var context = try request.platformDeadline();
        context.deadline_io = null;
        // Normalize once for the whole close; individual child RPCs cannot
        // renew this budget. Requests without a deadline get a bounded fallback.
        context.deadline_ns = context.deadline_ns orelse
            (@import("antfly_platform").time.monotonicNs() +|
                @as(u64, (@import("../pgwire/protocol.zig").Limits{}).statement_timeout_ms) * std.time.ns_per_ms);
        return context;
    }
    pub fn controller(self: *Native, connection: [32]u8, identity: ?@import("http_server.zig").AuthenticatedIdentity) !Controller {
        const server = self.server;
        const store = server.txn_sessions.durable orelse return error.SqlConnectionUnavailable;
        var owned = try source.load(store, server.alloc, connection);
        defer if (owned) |*value| value.deinit();
        return .{ .store = store, .connection = connection, .principal = @import("http_server.zig").transactionPrincipal(identity) orelse "", .actor = try @import("sql_authorization_recovery.zig").actor(server, identity), .instance = try server.sql_connection_admission.executionInstance(server.embedding_provider_runtime.io), .node = server.localSessionNodeId(), .standalone = if (owned) |value| value.value.registration.standalone else server.cfg.deployment_mode == .standalone, .port = .{ .ptr = self, .call = call, .retire_temporary = retireTemporary }, .runtime = .{ .ptr = self, .now_ms = now, .nonce = nonce, .active = active, .pause = pause }, .relation_owner_context = &self.context.sql_relation_owner };
    }
    fn call(raw: *anyopaque, a: A, input: @import("../system_catalog/server_call.zig").Call) anyerror![]u8 {
        const self: *Native = @ptrCast(@alignCast(raw));
        var port: source.NativePort = .{ .server = self.server, .context = try self.controlContext() };
        return port.port().call(port.port().ptr, a, input);
    }
    fn controlContext(self: *Native) !@import("antfly_local_sources").api_operation.RequestContext {
        var context = try self.context.platformDeadline();
        const deadline = @import("antfly_platform").time.monotonicNs() +| std.time.ns_per_s;
        context.deadline_ns = @min(context.deadline_ns orelse deadline, deadline);
        return context;
    }
    fn retireTemporary(raw: *anyopaque, a: A, prior: source.Record) anyerror!source.TemporaryProgress {
        const self: *Native = @ptrCast(@alignCast(raw));
        var port: source.NativePort = .{ .server = self.server, .context = try self.controlContext() };
        const control = port.port();
        return control.retire_temporary.?(control.ptr, a, prior);
    }
    fn now(_: *anyopaque) u64 {
        return @import("antfly_platform").time.realtimeNs() / std.time.ns_per_ms;
    }
    fn nonce(raw: *anyopaque) ![16]u8 {
        const self: *Native = @ptrCast(@alignCast(raw));
        var value: [16]u8 = undefined;
        try self.server.embedding_provider_runtime.io.randomSecure(&value);
        return value;
    }
    fn active(raw: *anyopaque) !void {
        const self: *Native = @ptrCast(@alignCast(raw));
        try self.context.ensureActive();
    }
    fn pause(raw: *anyopaque, ms: u32) !void {
        const self: *Native = @ptrCast(@alignCast(raw));
        try active(raw);
        try self.server.embedding_provider_runtime.io.sleep(.fromMilliseconds(ms), .awake);
        try active(raw);
    }
};

test "SQL relation lifetime foreground close retains an absolute inherited deadline and bounds missing budgets" {
    const time = @import("antfly_platform").time;
    const before = time.monotonicNs();
    const inherited = before +| std.time.ns_per_s;
    const request = try Native.retirementContext(.{ .deadline_ns = inherited });
    try std.testing.expectEqual(@as(?u64, inherited), request.deadline_ns);
    const io = std.testing.io;
    const fallback = try Native.retirementContext(.{ .deadline_io = @import("antfly_runtime_abi").io_abi.Borrow.init(&io) });
    try std.testing.expect(fallback.deadline_io == null);
    const budget = @as(u64, (@import("../pgwire/protocol.zig").Limits{}).statement_timeout_ms) * std.time.ns_per_ms;
    try std.testing.expect(fallback.deadline_ns.? >= before +| budget);
    try std.testing.expect(fallback.deadline_ns.? <= time.monotonicNs() +| budget);
}

/// One request borrows the connection's durable command authority. The pool
/// releases its signal before this stack frame or the request context expires.
pub const Request = struct {
    native: Native = undefined,
    loan: ?Loan = null,
    signal: admission.Signal = .{},

    pub fn start(self: *Request, server: *@import("http_server.zig").ApiHttpServer, context: @import("antfly_local_sources").api_operation.RequestContext, identity: ?@import("http_server.zig").AuthenticatedIdentity, lease: *admission.Lease, body: anytype) !bool {
        if (self.loan != null) return error.InvalidSqlConnection;
        const store = server.txn_sessions.durable orelse return false;
        var enrolled = try source.load(store, server.alloc, lease.key.connection);
        defer if (enrolled) |*value| value.deinit();
        if (enrolled == null and !server.source.vtable.supports_sql_command_ownership) return false;
        self.native = .{ .server = server, .context = context };
        const owner = try self.native.controller(lease.key.connection, identity);
        if (enrolled == null) {
            var connection = try @import("sql_connections.zig").load(store, server.alloc, lease.key.connection, owner.principal, server.localSessionNodeId(), owner.now());
            defer connection.deinit();
            // Publish registration before taking a body. Lost registration
            // replies may resume only native control, never execute SQL twice.
            try owner.enroll(server.alloc, connection.value.database);
        }
        try server.ensureSqlCommandUpkeep();
        var writer = std.Io.Writer.Hashing(std.crypto.hash.Blake3).initHasher(std.crypto.hash.Blake3.init(.{}), &.{});
        try writer.writer.writeAll("antfly/sql-command-request/v1\x00");
        try std.json.Stringify.value(body, .{}, &writer.writer);
        var digest: [32]u8 = undefined;
        writer.hasher.final(&digest);
        self.loan = try owner.take(server.alloc, digest);
        errdefer self.finish() catch {};
        const io = server.embedding_provider_runtime.io;
        try self.loan.?.bind(io, lease);
        var current = try owner.load(server.alloc);
        defer current.deinit();
        if (current.value.recovery or current.value.phase != .executing or !std.meta.eql(current.value.ticket, @as(?source.Ticket, self.loan.?.ticket))) return error.SqlConnectionBusy;
        self.signal.expires_at_ms.store(current.value.registration.lease.expires_at_ms, .release);
        try lease.bindSignal(io, self.loan.?.ticket.command, &self.signal);
        return true;
    }
    pub fn attach(self: *Request, context: @import("antfly_local_sources").api_operation.RequestContext) @import("antfly_local_sources").api_operation.RequestContext {
        std.debug.assert(self.loan != null);
        var result = context;
        result.sql_command_fence = &self.loan.?.fence;
        result.sql_relation_owner = self.loan.?.relation_owner;
        result.cancellation = .{ .ptr = self, .is_cancelled_fn = cancelled };
        return result;
    }
    fn cancelled(raw: *const anyopaque) bool {
        const self: *const Request = @ptrCast(@alignCast(raw));
        return self.native.context.cancellation.isCancelled() or self.signal.lost.load(.acquire) or
            @import("antfly_platform").time.realtimeNs() / std.time.ns_per_ms >= self.signal.expires_at_ms.load(.acquire);
    }
    /// Parse/bind and result-adaptation errors cross the same failure boundary
    /// as execution errors. Never run a statement to clean up its resources.
    pub fn failed(self: *Request, identity: ?@import("http_server.zig").AuthenticatedIdentity, failure: ?anyerror) !bool {
        if (failure) |err| if (err == error.SqlWriteCapacityUnavailable) return false;
        const loan = if (self.loan) |*value| value else return false;
        errdefer self.signal.retire_required.store(true, .release);
        const server = self.native.server;
        const store = loan.controller.store;
        var connection = try @import("sql_connections.zig").load(store, server.alloc, loan.controller.connection, loan.controller.principal, loan.controller.node, 0);
        defer connection.deinit();
        const id = connection.value.active_txn orelse return false;
        var session = (try store.loadInto(server.alloc, id)) orelse return false;
        defer session.deinit(server.alloc);
        if (session.commit_execution_started or session.terminal_commit != null or session.terminal_abort or session.external_preparation != null) return false;
        const metadata = session.sql orelse return false;
        if (metadata.failed) return true;
        var authenticated = identity;
        var coordinator: @import("sql_session.zig").Coordinator = .{ .server = server, .identity = &authenticated, .context = self.attach(self.native.context) };
        try @import("sql_session.zig").Coordinator.failed(&coordinator, .{ .principal = loan.controller.principal, .database = metadata.database, .namespace = metadata.namespace }, id);
        return true;
    }

    pub fn finish(self: *Request) !void {
        if (self.loan) |*loan| {
            const server = self.native.server;
            if (self.signal.retire_required.load(.acquire)) {
                if (try loan.retire(server.alloc)) {
                    // Retirement is now durable. Only an unsealed session can
                    // be detached; decided work stays in its recovery graph.
                    if (@import("sql_connections.zig").load(loan.controller.store, server.alloc, loan.controller.connection, loan.controller.principal, loan.controller.node, 0)) |value| {
                        var connection = value;
                        defer connection.deinit();
                        if (connection.value.active_txn) |id| _ = server.txn_sessions.removeBeforeExecution(server.alloc, id);
                    } else |_| {}
                }
                return error.SqlMutationOutcomeUnknown;
            }
            try loan.finish(server.alloc);
        }
    }
};
