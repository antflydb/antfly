// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Row payloads for native-local transactions belong to their admitted
//! process, never to the durable session log. Each publication keeps the
//! previous committed capsule until the durable reference has committed.
const std = @import("std");
const Allocator = std.mem.Allocator;
pub const Reference = struct {
    version: u8 = 1,
    digest: [32]u8,
    bytes: u64,
    generations: [32]u8,
    pub fn validate(self: @This()) !void {
        if (self.version != 1 or self.bytes == 0 or self.bytes > 128 * 1024 * 1024) return error.InvalidTransactionSessionRecord;
    }
};
const Payload = struct {
    reference: Reference,
    body: []u8,
    references: std.atomic.Value(usize) = .init(1),
};
const Versions = struct { current: ?*Payload = null, pending: ?*Payload = null, preparing: bool = false };
pub const Store = struct {
    allocator: Allocator,
    max_bytes: usize = 128 * 1024 * 1024,
    bytes: usize = 0,
    entries: std.AutoHashMapUnmanaged([16]u8, Versions) = .empty,
    mutex: std.Io.Mutex = .init,

    // This synchronous storage facade has no borrowed task Io. Match the
    // physical DocStore's non-spawning synchronization ABI, never busy wait
    // while another request is copying a large payload.
    fn syncIo() std.Io {
        return if (@import("builtin").os.tag == .freestanding) .failing else std.Io.Threaded.global_single_threaded.io();
    }
    fn lock(self: *@This()) void {
        self.mutex.lockUncancelable(syncIo());
    }
    fn unlock(self: *@This()) void {
        self.mutex.unlock(syncIo());
    }

    fn release(self: *@This(), payload: *Payload) void {
        if (payload.references.fetchSub(1, .acq_rel) != 1) return;
        const length = payload.body.len;
        // Allocation, copying and reclamation never hold the global map gate.
        self.allocator.free(payload.body);
        self.allocator.destroy(payload);
        self.lock();
        self.bytes -= length;
        self.unlock();
    }
    pub fn deinit(self: *@This()) void {
        // Like the underlying physical store, destruction requires quiesced
        // callers. A borrowed Payload is otherwise valid across publication.
        var iterator = self.entries.valueIterator();
        while (iterator.next()) |versions| {
            if (versions.current) |payload| self.release(payload);
            if (versions.pending) |payload| self.release(payload);
        }
        self.entries.deinit(self.allocator);
        self.* = .{ .allocator = self.allocator, .max_bytes = self.max_bytes };
    }
    /// Rejoin the actual durable reference before replacing a candidate left
    /// by a failed/ambiguous commit. Missing local data never authorizes replay.
    pub fn reconcile(self: *@This(), txn: [16]u8, durable: ?Reference) void {
        var retire_current: ?*Payload = null;
        var retire_pending: ?*Payload = null;
        self.lock();
        if (self.entries.getPtr(txn)) |versions| {
            if (durable != null and versions.pending != null and std.meta.eql(durable.?, versions.pending.?.reference)) {
                retire_current = versions.current;
                versions.current = versions.pending;
                versions.pending = null;
            } else {
                retire_pending = versions.pending;
                versions.pending = null;
                if (durable == null or versions.current == null or !std.meta.eql(durable.?, versions.current.?.reference)) {
                    retire_current = versions.current;
                    versions.current = null;
                }
            }
        }
        self.unlock();
        if (retire_current) |payload| self.release(payload);
        if (retire_pending) |payload| self.release(payload);
    }
    /// Always consumes body, including duplicate/error cases. The caller
    /// serializes durable writers with its existing physical-store gate.
    pub fn prepareOwned(self: *@This(), txn: [16]u8, generations: [32]u8, body: []u8) !Reference {
        var adopted = false;
        defer if (!adopted) self.allocator.free(body);
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update("antfly/native-transaction-capsule/v1\x00");
        hash.update(&txn);
        hash.update(&generations);
        hash.update(body);
        var digest: [32]u8 = undefined;
        hash.final(&digest);
        const reference: Reference = .{ .digest = digest, .bytes = body.len, .generations = generations };
        try reference.validate();
        self.lock();
        const found = self.entries.getOrPut(self.allocator, txn) catch |err| {
            self.unlock();
            return err;
        };
        if (!found.found_existing) found.value_ptr.* = .{};
        const versions = found.value_ptr;
        if (versions.current) |current| if (std.meta.eql(current.reference, reference)) {
            self.unlock();
            return reference;
        };
        if (versions.pending) |pending| {
            if (std.meta.eql(pending.reference, reference)) {
                self.unlock();
                return reference;
            }
            self.unlock();
            return error.SqlTransactionOutcomeUnknown;
        }
        if (versions.preparing) {
            self.unlock();
            return error.SqlTransactionOutcomeUnknown;
        }
        if (body.len > self.max_bytes -| self.bytes) {
            self.unlock();
            return error.TransactionTooLarge;
        }
        self.bytes += body.len;
        versions.preparing = true;
        self.unlock();
        const payload = self.allocator.create(Payload) catch |err| {
            self.lock();
            self.bytes -= body.len;
            self.entries.getPtr(txn).?.preparing = false;
            self.unlock();
            return err;
        };
        payload.* = .{ .reference = reference, .body = body };
        self.lock();
        const ready = self.entries.getPtr(txn).?;
        ready.pending = payload;
        ready.preparing = false;
        self.unlock();
        adopted = true;
        return reference;
    }
    pub fn prepare(self: *@This(), txn: [16]u8, generations: [32]u8, body: []const u8) !Reference {
        return self.prepareOwned(txn, generations, try self.allocator.dupe(u8, body));
    }
    pub fn publishPending(self: *@This(), txn: [16]u8) void {
        self.lock();
        const versions = self.entries.getPtr(txn) orelse {
            self.unlock();
            return;
        };
        const pending = versions.pending orelse {
            self.unlock();
            return;
        };
        const old = versions.current;
        versions.current = pending;
        versions.pending = null;
        self.unlock();
        if (old) |payload| self.release(payload);
    }
    pub fn published(self: *@This(), txn: [16]u8, reference: Reference) !void {
        self.lock();
        const versions = self.entries.getPtr(txn) orelse {
            self.unlock();
            return error.SqlRelationRecoveryIncomplete;
        };
        if (versions.current) |current| if (std.meta.eql(current.reference, reference)) {
            self.unlock();
            return;
        };
        const pending = versions.pending orelse {
            self.unlock();
            return error.SqlRelationRecoveryIncomplete;
        };
        if (!std.meta.eql(pending.reference, reference)) {
            self.unlock();
            return error.SqlRelationOwnerChanged;
        }
        const old = versions.current;
        versions.current = pending;
        versions.pending = null;
        self.unlock();
        if (old) |payload| self.release(payload);
    }
    pub const Borrow = struct {
        store: *Store,
        payload: *Payload,
        pub fn bytes(self: @This()) []const u8 {
            return self.payload.body;
        }
        pub fn deinit(self: @This()) void {
            self.store.release(self.payload);
        }
    };
    pub fn borrow(self: *@This(), txn: [16]u8, reference: Reference) !?Borrow {
        try reference.validate();
        self.lock();
        defer self.unlock();
        const versions = self.entries.get(txn) orelse return null;
        inline for (.{ versions.current, versions.pending }) |candidate| if (candidate) |payload| {
            if (std.meta.eql(payload.reference, reference)) {
                _ = payload.references.fetchAdd(1, .monotonic);
                return .{ .store = self, .payload = payload };
            }
        };
        return null;
    }
    pub fn load(self: *@This(), allocator: Allocator, txn: [16]u8, reference: Reference) !?[]u8 {
        const retained = (try self.borrow(txn, reference)) orelse return null;
        defer retained.deinit();
        return try allocator.dupe(u8, retained.bytes());
    }
    pub fn forget(self: *@This(), txn: [16]u8) void {
        self.lock();
        const removed = self.entries.fetchRemove(txn);
        self.unlock();
        if (removed) |entry| {
            if (entry.value.current) |payload| self.release(payload);
            if (entry.value.pending) |payload| self.release(payload);
        }
    }
};

test "volatile transaction capsule publication retains prior cut and never adopts another owner epoch" {
    const a = std.testing.allocator;
    var store: Store = .{ .allocator = a, .max_bytes = 128 };
    defer store.deinit();
    const txn: [16]u8 = @splat(1);
    const first = try store.prepare(txn, @splat(2), "first row payload");
    try store.published(txn, first);
    const pending = try store.prepare(txn, @splat(2), "second row payload with savepoints");
    const old = (try store.load(a, txn, first)).?;
    defer a.free(old);
    try std.testing.expectEqualStrings("first row payload", old);
    var wrong = pending;
    wrong.generations = @splat(3);
    try std.testing.expect((try store.load(a, txn, wrong)) == null);
    try std.testing.expectError(error.SqlRelationOwnerChanged, store.published(txn, wrong));
    try store.published(txn, pending);
    try std.testing.expect((try store.load(a, txn, first)) == null);
    store.forget(txn);
    try std.testing.expectEqual(@as(usize, 0), store.bytes);
    try std.testing.expect((try store.load(a, txn, pending)) == null);
}

test "volatile capsule admission bounds memory and preserves published data after allocation failures" {
    const a = std.testing.allocator;
    var store: Store = .{ .allocator = a, .max_bytes = 5 };
    defer store.deinit();
    const txn: [16]u8 = @splat(1);
    const first = try store.prepare(txn, @splat(2), "hello");
    try store.published(txn, first);
    try std.testing.expectError(error.TransactionTooLarge, store.prepare(txn, @splat(2), "world"));
    const body = (try store.load(a, txn, first)).?;
    defer a.free(body);
    try std.testing.expectEqualStrings("hello", body);
}

test "borrowed capsule survives publication and ambiguous writes require durable reconciliation" {
    const a = std.testing.allocator;
    var store: Store = .{ .allocator = a };
    defer store.deinit();
    const id: [16]u8 = @splat(4);
    const first = try store.prepare(id, @splat(5), "old");
    try store.published(id, first);
    const retained = (try store.borrow(id, first)).?;
    defer retained.deinit();
    const pending = try store.prepare(id, @splat(5), "maybe committed");
    try std.testing.expectError(error.SqlTransactionOutcomeUnknown, store.prepare(id, @splat(5), "another"));
    // Observing the durable pending reference proves it was published. A
    // historical reader keeps its old bytes without blocking this transition.
    store.reconcile(id, pending);
    try std.testing.expectEqualStrings("old", retained.bytes());
    const next = try store.prepare(id, @splat(5), "next");
    // A failed candidate is discarded only after a durable read proves the
    // previous reference still owns the session publication.
    store.reconcile(id, pending);
    try std.testing.expect((try store.borrow(id, next)) == null);
    const retry = try store.prepare(id, @splat(5), "retry");
    try store.published(id, retry);
    try std.testing.expectEqualStrings("old", retained.bytes());
}
