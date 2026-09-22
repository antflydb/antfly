// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Bounded owner-side lifetime authority for retained read RPC capabilities.
//! Scope must be computed from authenticated ingress, not copied from the body.
//! Capture and paging callbacks run outside the registry lock. An exclusive
//! borrow serializes cursor advancement; revocation defers destruction until
//! that borrow ends. The transport must cancel in-flight work on disconnect.
const std = @import("std");

pub const Scope = struct {
    principal: [32]u8,
    authorization_revision: u64,
    table_id: u64,
    group_id: u64,
    topology_revision: u64,
    schema_version: u32,
};
pub const Token = struct { incarnation: u128, sequence: u64, slot: u32 };
pub const Kind = enum { capture, cursor };
pub const Resource = struct {
    ptr: *anyopaque,
    close: *const fn (*anyopaque) void,
    kind: Kind,
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
            const retired = if (entry.revoked) entry.resource else null;
            if (retired != null) slot.entry = null;
            registry.mutex.unlock(registry.io);
            if (retired) |resource| resource.close(resource.ptr);
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
        for (self.slots) |slot| if (slot.entry) |entry| {
            std.debug.assert(!entry.borrowed);
            entry.resource.close(entry.resource.ptr);
        };
        self.alloc.free(self.slots);
        self.* = undefined;
    }

    /// Ownership transfers only on success; failure leaves cleanup to caller.
    /// All times use the same owner monotonic clock, never client wall time.
    pub fn insert(self: *Registry, scope: Scope, connection: u128, now_ns: u64, expires_ns: u64, resource: Resource) !Token {
        if (expires_ns <= now_ns or expires_ns - now_ns > self.max_lease_ns) return error.InvalidRetainedReadLease;
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
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
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        const index = self.find(token) orelse return error.RetainedReadNotFound;
        const entry = &self.slots[index].entry.?;
        if (!std.meta.eql(entry.scope, scope)) return error.RetainedReadScopeChanged;
        if (entry.revoked or now_ns >= entry.expires_ns) return error.RetainedReadExpired;
        if (entry.resource.kind != kind) return error.InvalidRetainedReadKind;
        if (entry.borrowed) return error.RetainedReadBusy;
        entry.borrowed = true;
        return .{ .registry = self, .slot = index, .resource = entry.resource };
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
        if (resource) |value| value.close(value.ptr);
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
            if (resource) |value| value.close(value.ptr);
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
            if (resource) |value| value.close(value.ptr);
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
        entry.revoked = true;
        if (entry.resource.cancellation) |signal| signal.store(true, .release);
        if (entry.borrowed) return null;
        const resource = entry.resource;
        self.slots[index].entry = null;
        return resource;
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
    inline for (std.meta.fields(Scope)) |field| {
        var changed = scope;
        if (comptime std.mem.eql(u8, field.name, "principal")) changed.principal[0] += 1 else @field(changed, field.name) += 1;
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
