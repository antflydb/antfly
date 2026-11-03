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
    fn validateConnection(self: Controller, a: A, database: ?[]const u8) !void {
        var connection = try @import("sql_connections.zig").load(self.store, a, self.connection, self.principal, self.node, self.now());
        defer connection.deinit();
        if (database) |name| if (!std.mem.eql(u8, connection.value.database, name)) return error.SqlConnectionChanged;
        if (connection.value.state == .beginning or connection.value.state == .uncertain) return error.SqlConnectionBusy;
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
        try self.validateConnection(a, database);
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
            if (cut.standalone != self.standalone or std.mem.allEqual(u8, &cut.incarnation, 0) or cut.database_id == null or cut.database_id.? == 0 or
                cut.scope != null or cut.owner != null or cut.next_work != null or cut.receipt != null or cut.wait_resource != null) return error.InvalidSqlBackendResponse;
            try self.check();
            const registration: authority.scopes.Registration = .{ .connection = self.connection, .nonce = try self.nonce(), .standalone = self.standalone, .lease = .{
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
        try self.validateConnection(a, null);
        for (0..16) |_| {
            var owned = try self.load(a);
            defer owned.deinit();
            if (owned.value.recovery or owned.value.phase == .retired) return error.SqlConnectionNotFound;
            if (owned.value.phase == .idle and owned.value.pending == null) break;
            if (owned.value.phase != .finishing and !(owned.value.phase == .beginning and owned.value.body_skipped) and !(owned.value.phase == .idle and owned.value.pending != null and owned.value.pending.?.operation == .renew)) return error.SqlConnectionBusy;
            try self.check();
            try self.advance(a, owned.value);
        }
        var prior = try self.load(a);
        defer prior.deinit();
        // Completion remains publishable after request/task cancellation.
        const finish_nonce = try self.nonce();
        const command: authority.scopes.Command = .{ .sequence = prior.value.next_command, .nonce = try self.nonce(), .executor = try self.nonce() };
        const ticket = try source.beginObserved(self.store, a, self.principal, prior.value, self.instance, command, body, try self.nonce(), self.now(), self.port);
        errdefer self.abandon(a, ticket, finish_nonce) catch {};
        for (0..16) |_| {
            try self.check();
            var owned = try self.load(a);
            defer owned.deinit();
            if (owned.value.recovery or !std.meta.eql(owned.value.ticket, @as(?source.Ticket, ticket))) return error.SqlConnectionBusy;
            if (owned.value.phase == .ready) {
                const fence = try source.take(self.store, self.principal, owned.value, self.instance, ticket, self.now());
                return .{ .controller = self, .ticket = ticket, .fence = fence, .finish_nonce = finish_nonce };
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
