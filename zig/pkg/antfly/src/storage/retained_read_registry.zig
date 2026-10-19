// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
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

//! Bounded owner-side lifetime authority for retained read RPC capabilities.
//! Scope must be computed from authenticated ingress, not copied from the body.
//! Capture and paging callbacks run outside the registry lock. An exclusive
//! borrow serializes cursor advancement; revocation defers destruction until
//! that borrow ends. The transport must cancel in-flight work on disconnect.
const std = @import("std");
const attempts = @import("../common/remote_attempts.zig");

pub const Scope = struct {
    principal: [32]u8,
    authorization_revision: u64,
    table_id: u64,
    group_id: u64,
    topology_revision: u64,
    schema_version: u32,
};
pub const Token = struct { incarnation: u128, sequence: u64, slot: u32 };
pub const Kind = enum { capture, snapshot, cursor };
/// Physical close, not just revocation of a borrowed capability.
pub const Retirement = enum { pending, retired };
pub const Resource = struct {
    ptr: *anyopaque,
    close: *const fn (*anyopaque) void,
    kind: Kind,
    /// Move-only worker reference acquired before native creation. The caller
    /// transfers it only on successful insert; release follows physical close.
    /// It pins the worker registry/attempt, never an ingress-stack Lease.
    retention: ?attempts.Retention = null,
    /// Optional owner-owned cancellation signal whose lifetime extends through
    /// close. Native page/capture work must use this rather than an RPC stack
    /// token. Revocation sets it even when destruction waits for a borrower.
    cancellation: ?*std.atomic.Value(bool) = null,
};

pub const Registry = struct {
    alloc: std.mem.Allocator,
    io: std.Io,
    incarnation: u128,
    slots: []Slot,
    max_per_principal: usize,
    max_lease_ns: u64,
    mutex: std.Io.Mutex = .init,
    sequence: u64 = 0,
    sweep_position: usize = 0,
    closed: bool = false,

    const Slot = struct {
        entry: ?Entry = null,
    };
    const Entry = struct {
        token: Token,
        scope: Scope,
        connection: u128,
        expires_ns: u64,
        resource: Resource,
        borrowed: bool = false,
        revoked: bool = false,
        /// Keep global and principal admission charged through physical close.
        retiring: bool = false,
    };
    pub const Borrow = struct {
        registry: ?*Registry,
        slot: usize,
        resource: Resource,

        /// Check after native work and before publishing its result. Expiry or
        /// disconnect may race a running page/capture even though destruction
        /// is safely deferred. A transport must discard that result on error.
        pub fn validate(self: *const Borrow, now_ns: u64) !void {
            const registry = self.registry orelse return error.RetainedReadExpired;
            registry.mutex.lockUncancelable(registry.io);
            defer registry.mutex.unlock(registry.io);
            const entry = registry.slots[self.slot].entry.?;
            if (entry.revoked or now_ns >= entry.expires_ns) return error.RetainedReadExpired;
        }

        pub fn deinit(self: *Borrow) void {
            const registry = self.registry orelse return;
            self.registry = null;
            registry.mutex.lockUncancelable(registry.io);
            const slot = &registry.slots[self.slot];
            const entry = &slot.entry.?;
            std.debug.assert(entry.borrowed);
            entry.borrowed = false;
            const retired = if (entry.revoked) registry.revoke(self.slot) else null;
            registry.mutex.unlock(registry.io);
            if (retired) |resource| registry.retire(self.slot, resource);
        }
    };

    /// Incarnation is a fresh process-start nonce, never reused after restart.
    /// The token is not authentication; every request must also match Scope.
    pub fn init(alloc: std.mem.Allocator, io: std.Io, incarnation: u128, capacity: usize, max_per_principal: usize, max_lease_ns: u64) !Registry {
        if (incarnation == 0 or capacity == 0 or capacity > 4096 or max_per_principal == 0 or max_per_principal > capacity or max_lease_ns == 0) return error.InvalidRetainedReadLimits;
        const slots = try alloc.alloc(Slot, capacity);
        @memset(slots, .{});
        return .{ .alloc = alloc, .io = io, .incarnation = incarnation, .slots = slots, .max_per_principal = max_per_principal, .max_lease_ns = max_lease_ns };
    }

    /// Shutdown must join all request tasks before destroying the registry.
    pub fn deinit(self: *Registry) void {
        self.mutex.lockUncancelable(self.io);
        self.closed = true;
        self.mutex.unlock(self.io);
        for (0..self.slots.len) |index| {
            self.mutex.lockUncancelable(self.io);
            const resource = if (self.slots[index].entry) |entry| value: {
                std.debug.assert(!entry.borrowed and !entry.retiring);
                break :value self.revoke(index);
            } else null;
            self.mutex.unlock(self.io);
            if (resource) |value| self.retire(index, value);
        }
        self.alloc.free(self.slots);
        self.* = undefined;
    }

    /// Ownership transfers only on success; failure leaves cleanup to caller.
    /// All times use the same owner monotonic clock, never client wall time.
    pub fn insert(self: *Registry, scope: Scope, connection: u128, now_ns: u64, expires_ns: u64, resource: Resource) !Token {
        if (expires_ns <= now_ns or expires_ns - now_ns > self.max_lease_ns) return error.InvalidRetainedReadLease;
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        if (self.closed) return error.RetainedReadAdmissionExceeded;
        var available: ?usize = null;
        var principal_count: usize = 0;
        for (self.slots, 0..) |slot, i| {
            if (slot.entry) |entry| {
                if (std.mem.eql(u8, &entry.scope.principal, &scope.principal)) principal_count += 1;
            } else if (available == null) available = i;
        }
        if (principal_count >= self.max_per_principal) return error.RetainedReadAdmissionExceeded;
        const index = available orelse return error.RetainedReadAdmissionExceeded;
        self.sequence = std.math.add(u64, self.sequence, 1) catch return error.RetainedReadTokenExhausted;
        const token = Token{ .incarnation = self.incarnation, .sequence = self.sequence, .slot = @intCast(index) };
        self.slots[index].entry = .{ .token = token, .scope = scope, .connection = connection, .expires_ns = expires_ns, .resource = resource };
        return token;
    }

    pub fn borrow(self: *Registry, token: Token, scope: Scope, kind: Kind, now_ns: u64) !Borrow {
        return self.borrowForAttempt(token, scope, kind, now_ns, null);
    }

    /// Signed request identity is supplied by authenticated ingress, never the
    /// body. Sequences may differ across capture/open/page requests of one
    /// operation; coordinator, generation and operation must remain identical.
    /// Legacy borrows cannot acquire an owned token, or vice versa. Cleanup
    /// remains scope-authenticated and idempotent even after the owner stops.
    pub fn borrowForAttempt(self: *Registry, token: Token, scope: Scope, kind: Kind, now_ns: u64, parent: ?attempts.Id) !Borrow {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        const index = self.find(token) orelse return error.RetainedReadNotFound;
        const entry = &self.slots[index].entry.?;
        if (!std.meta.eql(entry.scope, scope)) return error.RetainedReadScopeChanged;
        if (!sameOperation(if (entry.resource.retention) |retained| retained.id else null, parent)) return error.RetainedReadScopeChanged;
        if (entry.revoked or now_ns >= entry.expires_ns) return error.RetainedReadExpired;
        if (entry.resource.kind != kind) return error.InvalidRetainedReadKind;
        if (entry.borrowed) return error.RetainedReadBusy;
        entry.borrowed = true;
        return .{ .registry = self, .slot = index, .resource = entry.resource };
    }

    fn sameOperation(owned: ?attempts.Id, request: ?attempts.Id) bool {
        const original = owned orelse return request == null;
        const current = request orelse return false;
        return original.coordinator == current.coordinator and original.generation == current.generation and original.operation == current.operation;
    }

    pub fn close(self: *Registry, token: Token, scope: Scope) !void {
        self.mutex.lockUncancelable(self.io);
        const index = self.find(token) orelse {
            self.mutex.unlock(self.io);
            return; // idempotent cleanup, including response loss
        };
        if (!std.meta.eql(self.slots[index].entry.?.scope, scope)) {
            self.mutex.unlock(self.io);
            return error.RetainedReadScopeChanged;
        }
        const resource = self.revoke(index);
        self.mutex.unlock(self.io);
        if (resource) |value| self.retire(index, value);
    }

    /// A cleanup acknowledgement is authoritative only in this exact process
    /// and for an already issued token. Revocation can leave a borrower or an
    /// out-of-lock destructor alive; both keep admission charged and pending.
    /// A disappeared issued token cannot be republished: sequence never wraps.
    pub fn closeAcknowledged(self: *Registry, token: Token, scope: Scope) !Retirement {
        self.mutex.lockUncancelable(self.io);
        if (token.incarnation != self.incarnation) {
            self.mutex.unlock(self.io);
            return error.RetainedReadRestartRequired;
        }
        if (token.slot >= self.slots.len or token.sequence == 0 or token.sequence > self.sequence) {
            self.mutex.unlock(self.io);
            return error.InvalidRetainedReadToken;
        }
        const index = self.find(token) orelse {
            self.mutex.unlock(self.io);
            return .retired;
        };
        if (!std.meta.eql(self.slots[index].entry.?.scope, scope)) {
            self.mutex.unlock(self.io);
            return error.RetainedReadScopeChanged;
        }
        const resource = self.revoke(index);
        self.mutex.unlock(self.io);
        if (resource) |value| self.retire(index, value);
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        return if (self.find(token) == null) .retired else .pending;
    }

    /// Bounded maintenance: at most budget slots and callbacks per invocation.
    /// A periodic owner task must call this even when no new RPCs arrive.
    pub fn expire(self: *Registry, now_ns: u64, budget: usize) void {
        for (0..@min(budget, self.slots.len)) |_| {
            self.mutex.lockUncancelable(self.io);
            const index = self.sweep_position;
            self.sweep_position = (index + 1) % self.slots.len;
            const resource = if (self.slots[index].entry) |entry|
                if (now_ns >= entry.expires_ns) self.revoke(index) else null
            else
                null;
            self.mutex.unlock(self.io);
            if (resource) |value| self.retire(index, value);
        }
    }

    pub fn disconnect(self: *Registry, connection: u128) void {
        for (0..self.slots.len) |index| {
            self.mutex.lockUncancelable(self.io);
            const resource = if (self.slots[index].entry) |entry|
                if (entry.connection == connection) self.revoke(index) else null
            else
                null;
            self.mutex.unlock(self.io);
            if (resource) |value| self.retire(index, value);
        }
    }

    /// RPC cleanup after a response was lost before the caller learned its
    /// token. Unlike local transport shutdown this must match current scope;
    /// a caller-chosen connection identifier is never cross-principal authority.
    pub fn disconnectScoped(self: *Registry, scope: Scope, connection: u128) void {
        for (0..self.slots.len) |index| {
            self.mutex.lockUncancelable(self.io);
            const resource = if (self.slots[index].entry) |entry|
                if (entry.connection == connection and std.meta.eql(entry.scope, scope)) self.revoke(index) else null
            else
                null;
            self.mutex.unlock(self.io);
            if (resource) |value| self.retire(index, value);
        }
    }

    fn find(self: *Registry, token: Token) ?usize {
        if (token.incarnation != self.incarnation or token.slot >= self.slots.len) return null;
        if (self.slots[token.slot].entry) |entry| {
            if (entry.token.sequence == token.sequence) return token.slot;
        }
        return null;
    }
    fn revoke(self: *Registry, index: usize) ?Resource {
        const entry = &self.slots[index].entry.?;
        // The destructor may already have freed its owned cancellation signal.
        // Reentrant/concurrent revocation must not touch physical resources.
        if (entry.retiring) return null;
        entry.revoked = true;
        if (entry.resource.cancellation) |signal| signal.store(true, .release);
        if (entry.borrowed) return null;
        entry.retiring = true;
        return entry.resource;
    }
    fn retire(self: *Registry, index: usize, resource: Resource) void {
        // Mandatory cleanup neither holds the registry lock nor asks for a new
        // execution grant. The occupied slot prevents reuse until it returns.
        resource.close(resource.ptr);
        // Only now can the signed worker attempt become physically terminal.
        // Both callbacks run outside this lock while the token slot and its
        // principal quota remain occupied, including a blocked worker release.
        if (resource.retention) |parent| parent.release();
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        const entry = self.slots[index].entry.?;
        std.debug.assert(entry.retiring and entry.revoked and !entry.borrowed);
        self.slots[index].entry = null;
    }
};

test "retained read registry fences identity, bounds admission and defers close while borrowed" {
    const Counter = struct {
        closed: usize = 0,
        fn close(ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.closed += 1;
        }
    };
    var counter: Counter = .{};
    var cancelled = std.atomic.Value(bool).init(false);
    var registry = try Registry.init(std.testing.allocator, std.testing.io, 5, 2, 1, 100);
    defer registry.deinit();
    const scope = Scope{ .principal = @splat(7), .authorization_revision = 1, .table_id = 2, .group_id = 3, .topology_revision = 4, .schema_version = 5 };
    const resource = Resource{ .ptr = &counter, .close = Counter.close, .kind = .cursor, .cancellation = &cancelled };
    const token = try registry.insert(scope, 10, 1, 100, resource);
    try std.testing.expectError(error.RetainedReadAdmissionExceeded, registry.insert(scope, 10, 1, 100, resource));
    var changed = scope;
    changed.authorization_revision += 1;
    try std.testing.expectError(error.RetainedReadScopeChanged, registry.borrow(token, changed, .cursor, 2));
    var borrow = try registry.borrow(token, scope, .cursor, 2);
    try std.testing.expectError(error.RetainedReadBusy, registry.borrow(token, scope, .cursor, 2));
    registry.disconnect(10);
    try std.testing.expect(cancelled.load(.acquire));
    try std.testing.expectEqual(0, counter.closed);
    try std.testing.expectError(error.RetainedReadExpired, borrow.validate(2));
    try std.testing.expectError(error.RetainedReadExpired, registry.borrow(token, scope, .cursor, 2));
    borrow.deinit();
    borrow.deinit();
    try std.testing.expectEqual(1, counter.closed);
    try registry.close(token, scope);
    const second = try registry.insert(scope, 10, 1, 100, resource);
    try std.testing.expect(second.sequence != token.sequence);
    try std.testing.expectError(error.RetainedReadNotFound, registry.borrow(token, scope, .cursor, 2));
    registry.expire(100, 2);
    try std.testing.expectEqual(2, counter.closed);
    try std.testing.expectError(error.RetainedReadNotFound, registry.borrow(second, scope, .cursor, 100));
}

test "retained read registry lost-response cleanup cannot revoke another principal" {
    const Fixture = struct {
        closed: usize = 0,
        fn close(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.closed += 1;
        }
    };
    var registry = try Registry.init(std.testing.allocator, std.testing.io, 1, 4, 4, 100);
    defer registry.deinit();
    var first: Fixture = .{};
    var other: Fixture = .{};
    const scope = Scope{ .principal = @splat(1), .authorization_revision = 1, .table_id = 2, .group_id = 3, .topology_revision = 4, .schema_version = 5 };
    var other_scope = scope;
    other_scope.principal = @splat(2);
    _ = try registry.insert(scope, 99, 1, 100, .{ .ptr = &first, .kind = .capture, .close = Fixture.close });
    const foreign = try registry.insert(other_scope, 99, 1, 100, .{ .ptr = &other, .kind = .capture, .close = Fixture.close });
    registry.disconnectScoped(scope, 99);
    try std.testing.expectEqual(@as(usize, 1), first.closed);
    try std.testing.expectEqual(@as(usize, 0), other.closed);
    var borrow = try registry.borrow(foreign, other_scope, .capture, 2);
    borrow.deinit();
}

test "retained read registry expires in bounded slices without destroying active callbacks" {
    const Counter = struct {
        closed: usize = 0,
        fn close(ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.closed += 1;
        }
    };
    var counter: Counter = .{};
    var registry = try Registry.init(std.testing.allocator, std.testing.io, 7, 2, 2, 100);
    defer registry.deinit();
    const scope = Scope{ .principal = @splat(7), .authorization_revision = 1, .table_id = 2, .group_id = 3, .topology_revision = 4, .schema_version = 5 };
    const resource = Resource{ .ptr = &counter, .close = Counter.close, .kind = .capture };
    try std.testing.expectError(error.InvalidRetainedReadLease, registry.insert(scope, 1, 100, 100, resource));
    try std.testing.expectError(error.InvalidRetainedReadLease, registry.insert(scope, 1, 100, 201, resource));
    const first = try registry.insert(scope, 1, 100, 200, resource);
    const second = try registry.insert(scope, 2, 100, 200, resource);
    var active = try registry.borrow(first, scope, .capture, 199);
    defer active.deinit();
    registry.expire(200, 1);
    try std.testing.expectEqual(0, counter.closed);
    try std.testing.expectError(error.RetainedReadExpired, active.validate(200));
    registry.expire(200, 1);
    try std.testing.expectEqual(1, counter.closed);
    try std.testing.expectError(error.RetainedReadNotFound, registry.borrow(second, scope, .capture, 200));
    active.deinit();
    try std.testing.expectEqual(2, counter.closed);
    try std.testing.expectError(error.RetainedReadExpired, active.validate(200));
}

test "retained read registry rejects every changed scope dimension and stale process tokens" {
    const Cleanup = struct {
        fn close(_: *anyopaque) void {}
    };
    var context: u8 = 0;
    var registry = try Registry.init(std.testing.allocator, std.testing.io, 7, 1, 1, 100);
    defer registry.deinit();
    const scope = Scope{ .principal = @splat(7), .authorization_revision = 1, .table_id = 2, .group_id = 3, .topology_revision = 4, .schema_version = 5 };
    const token = try registry.insert(scope, 1, 1, 100, .{ .ptr = &context, .close = Cleanup.close, .kind = .cursor });
    inline for (comptime std.meta.fieldNames(Scope)) |reflected_name| {
        var changed = scope;
        if (comptime std.mem.eql(u8, reflected_name, "principal")) changed.principal[0] += 1 else @field(changed, reflected_name) += 1;
        try std.testing.expectError(error.RetainedReadScopeChanged, registry.borrow(token, changed, .cursor, 2));
        try std.testing.expectError(error.RetainedReadScopeChanged, registry.close(token, changed));
    }
    var previous_process = token;
    previous_process.incarnation -= 1;
    try std.testing.expectError(error.RetainedReadNotFound, registry.borrow(previous_process, scope, .cursor, 2));
    try std.testing.expectError(error.InvalidRetainedReadKind, registry.borrow(token, scope, .capture, 2));
    var live = try registry.borrow(token, scope, .cursor, 2);
    defer live.deinit();
    try live.validate(2);
}

test "retained read registry charges admission through physical destruction and tolerates reentrant revocation" {
    const Cleanup = enum { close, expire, disconnect, scoped_disconnect };
    const Fixture = struct {
        registry: *Registry,
        scope: Scope,
        token: Token = undefined,
        signal: ?*std.atomic.Value(bool),
        closed: usize = 0,
        failure: ?anyerror = null,
        fn noop(_: *anyopaque) void {}
        fn close(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.closed += 1;
            if (self.closed != 1) {
                self.failure = error.TestDuplicatePhysicalClose;
                return;
            }
            self.check() catch |err| {
                self.failure = err;
            };
        }
        fn check(self: *@This()) !void {
            try std.testing.expect(self.signal.?.load(.acquire));
            std.testing.allocator.destroy(self.signal.?);
            self.signal = null;
            // The registry still holds the now-dangling signal pointer. Every
            // reentrant revocation must skip it while physical close runs.
            try self.registry.close(self.token, self.scope);
            self.registry.expire(100, self.registry.slots.len);
            self.registry.disconnect(10);
            self.registry.disconnectScoped(self.scope, 10);
            try std.testing.expectError(error.RetainedReadExpired, self.registry.borrow(self.token, self.scope, .snapshot, 2));
            const replacement: Resource = .{ .ptr = self, .close = noop, .kind = .snapshot };
            try std.testing.expectError(error.RetainedReadAdmissionExceeded, self.registry.insert(self.scope, 10, 1, 100, replacement));
            var other = self.scope;
            other.principal = @splat(8);
            if (self.registry.slots.len == 1) {
                try std.testing.expectError(error.RetainedReadAdmissionExceeded, self.registry.insert(other, 20, 1, 100, replacement));
            } else {
                // Only this principal is full; another principal may use the
                // separate slot. No lock is held through either destructor.
                const token = try self.registry.insert(other, 20, 1, 100, replacement);
                try self.registry.close(token, other);
            }
        }
    };
    for ([_]usize{ 1, 2 }) |capacity| for ([_]bool{ false, true }) |borrowed| for (std.enums.values(Cleanup)) |cleanup| {
        var registry = try Registry.init(std.testing.allocator, std.testing.io, 5, capacity, 1, 100);
        defer registry.deinit();
        const scope: Scope = .{ .principal = @splat(7), .authorization_revision = 1, .table_id = 2, .group_id = 3, .topology_revision = 4, .schema_version = 5 };
        const signal = try std.testing.allocator.create(std.atomic.Value(bool));
        signal.* = .init(false);
        var fixture: Fixture = .{ .registry = &registry, .scope = scope, .signal = signal };
        // Physical cleanup owns the signal after insertion.
        fixture.token = try registry.insert(scope, 10, 1, 100, .{ .ptr = &fixture, .close = Fixture.close, .kind = .snapshot, .cancellation = signal });
        var active: ?Registry.Borrow = if (borrowed) try registry.borrow(fixture.token, scope, .snapshot, 2) else null;
        switch (cleanup) {
            .close => try registry.close(fixture.token, scope),
            .expire => registry.expire(100, capacity),
            .disconnect => registry.disconnect(10),
            .scoped_disconnect => registry.disconnectScoped(scope, 10),
        }
        if (active) |*value| {
            try std.testing.expectEqual(@as(usize, 0), fixture.closed);
            try std.testing.expectError(error.RetainedReadExpired, value.validate(2));
            value.deinit();
        }
        if (fixture.failure) |err| return err;
        try std.testing.expectEqual(@as(usize, 1), fixture.closed);
        try std.testing.expect(fixture.signal == null);
        try std.testing.expectError(error.RetainedReadNotFound, registry.borrow(fixture.token, scope, .snapshot, 2));
        const replacement = try registry.insert(scope, 10, 1, 100, .{ .ptr = &fixture, .close = Fixture.noop, .kind = .snapshot });
        try std.testing.expect(replacement.sequence != fixture.token.sequence);
        try registry.close(replacement, scope);
    };
}

test "retained read registry concurrent revocation cannot reuse a closing snapshot slot" {
    const Fixture = struct {
        registry: *Registry,
        token: Token,
        scope: Scope,
        entered: std.atomic.Value(bool) = .init(false),
        released: std.atomic.Value(bool) = .init(false),
        closes: std.atomic.Value(usize) = .init(0),
        fn close(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            _ = self.closes.fetchAdd(1, .monotonic);
            self.entered.store(true, .release);
            while (!self.released.load(.acquire)) std.Thread.yield() catch {};
        }
        fn run(self: *@This()) void {
            self.registry.close(self.token, self.scope) catch @panic("unexpected scope change");
        }
        fn noop(_: *anyopaque) void {}
    };
    var registry = try Registry.init(std.testing.allocator, std.testing.io, 5, 1, 1, 100);
    defer registry.deinit();
    const scope: Scope = .{ .principal = @splat(7), .authorization_revision = 1, .table_id = 2, .group_id = 3, .topology_revision = 4, .schema_version = 5 };
    var fixture: Fixture = .{ .registry = &registry, .token = undefined, .scope = scope };
    fixture.token = try registry.insert(scope, 10, 1, 100, .{ .ptr = &fixture, .close = Fixture.close, .kind = .snapshot });
    const closer = try std.Thread.spawn(.{}, Fixture.run, .{&fixture});
    var joined = false;
    defer if (!joined) {
        fixture.released.store(true, .release);
        closer.join();
    };
    while (!fixture.entered.load(.acquire)) std.Thread.yield() catch {};
    try std.testing.expectError(error.RetainedReadAdmissionExceeded, registry.insert(scope, 10, 1, 100, .{ .ptr = &fixture, .close = Fixture.noop, .kind = .snapshot }));
    try registry.close(fixture.token, scope);
    try std.testing.expectEqual(Retirement.pending, try registry.closeAcknowledged(fixture.token, scope));
    registry.expire(100, 1);
    registry.disconnect(10);
    registry.disconnectScoped(scope, 10);
    try std.testing.expectEqual(@as(usize, 1), fixture.closes.load(.acquire));
    fixture.released.store(true, .release);
    closer.join();
    joined = true;
    try std.testing.expectEqual(Retirement.retired, try registry.closeAcknowledged(fixture.token, scope));
    const replacement = try registry.insert(scope, 10, 1, 100, .{ .ptr = &fixture, .close = Fixture.noop, .kind = .snapshot });
    try registry.close(replacement, scope);
}

test "retained read registry shutdown closes admission before reentrant physical cleanup" {
    const Fixture = struct {
        registry: *Registry,
        token: Token = undefined,
        scope: Scope,
        signal: std.atomic.Value(bool) = .init(false),
        closed: usize = 0,
        failure: ?anyerror = null,
        fn noop(_: *anyopaque) void {}
        fn close(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.closed += 1;
            self.check() catch |err| {
                self.failure = err;
            };
        }
        fn check(self: *@This()) !void {
            try std.testing.expect(self.signal.load(.acquire));
            try self.registry.close(self.token, self.scope);
            var other = self.scope;
            other.principal = @splat(8);
            // A free slot exists, but shutdown must never publish a new owner.
            try std.testing.expectError(error.RetainedReadAdmissionExceeded, self.registry.insert(other, 20, 1, 100, .{ .ptr = self, .close = noop, .kind = .cursor }));
        }
    };
    var registry = try Registry.init(std.testing.allocator, std.testing.io, 5, 2, 1, 100);
    const scope: Scope = .{ .principal = @splat(7), .authorization_revision = 1, .table_id = 2, .group_id = 3, .topology_revision = 4, .schema_version = 5 };
    var fixture: Fixture = .{ .registry = &registry, .scope = scope };
    fixture.token = try registry.insert(scope, 10, 1, 100, .{ .ptr = &fixture, .close = Fixture.close, .kind = .cursor, .cancellation = &fixture.signal });
    registry.deinit();
    if (fixture.failure) |err| return err;
    try std.testing.expectEqual(@as(usize, 1), fixture.closed);
}

test "retained read registry retirement evidence excludes active borrows future tokens and process changes" {
    const Fixture = struct {
        owner: *Registry,
        scope: Scope,
        token: Token = undefined,
        closed: usize = 0,
        failure: ?anyerror = null,
        fn close(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.closed += 1;
            self.check() catch |err| {
                self.failure = err;
            };
        }
        fn check(self: *@This()) !void {
            // Reentrant cleanup runs outside the lock but cannot claim the
            // destructor finished or recycle its still-charged quota.
            try std.testing.expectEqual(Retirement.pending, try self.owner.closeAcknowledged(self.token, self.scope));
            try std.testing.expectError(error.RetainedReadAdmissionExceeded, self.owner.insert(self.scope, 11, 1, 100, .{ .ptr = self, .close = close, .kind = .cursor }));
        }
    };
    var owner = try Registry.init(std.testing.allocator, std.testing.io, 5, 1, 1, 100);
    defer owner.deinit();
    const scope: Scope = .{ .principal = @splat(7), .authorization_revision = 1, .table_id = 2, .group_id = 3, .topology_revision = 4, .schema_version = 5 };
    var fixture: Fixture = .{ .owner = &owner, .scope = scope };
    fixture.token = try owner.insert(scope, 10, 1, 100, .{ .ptr = &fixture, .close = Fixture.close, .kind = .cursor });
    var wrong = scope;
    wrong.authorization_revision += 1;
    try std.testing.expectError(error.RetainedReadScopeChanged, owner.closeAcknowledged(fixture.token, wrong));
    var future = fixture.token;
    future.sequence += 1;
    try std.testing.expectError(error.InvalidRetainedReadToken, owner.closeAcknowledged(future, scope));
    var old_process = fixture.token;
    old_process.incarnation -= 1;
    try std.testing.expectError(error.RetainedReadRestartRequired, owner.closeAcknowledged(old_process, scope));
    var borrow = try owner.borrow(fixture.token, scope, .cursor, 2);
    defer borrow.deinit();
    try std.testing.expectEqual(Retirement.pending, try owner.closeAcknowledged(fixture.token, scope));
    try std.testing.expectEqual(@as(usize, 0), fixture.closed);
    try std.testing.expectError(error.RetainedReadAdmissionExceeded, owner.insert(scope, 11, 1, 100, .{ .ptr = &fixture, .close = Fixture.close, .kind = .cursor }));
    borrow.deinit();
    if (fixture.failure) |err| return err;
    try std.testing.expectEqual(@as(usize, 1), fixture.closed);
    try std.testing.expectEqual(Retirement.retired, try owner.closeAcknowledged(fixture.token, scope));
    const retired = fixture.token;
    fixture.token = try owner.insert(scope, 11, 1, 100, .{ .ptr = &fixture, .close = Fixture.close, .kind = .cursor });
    try std.testing.expectEqual(retired.slot, fixture.token.slot);
    try std.testing.expectEqual(Retirement.retired, try owner.closeAcknowledged(retired, scope));
    var replacement = try owner.borrow(fixture.token, scope, .cursor, 2);
    replacement.deinit();
    try std.testing.expectEqual(Retirement.retired, try owner.closeAcknowledged(fixture.token, scope));
    if (fixture.failure) |err| return err;
    try std.testing.expectEqual(@as(usize, 2), fixture.closed);
}
