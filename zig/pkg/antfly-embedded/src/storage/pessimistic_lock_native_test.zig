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

//! Concrete LSM/WAL qualification of the transaction-local lock journal.
//! This fixture supplies authority explicitly; SQL, metadata consensus and
//! connection expiry require their own integration gates before activation.
const std = @import("std");
const ledger = @import("pessimistic_lock_ledger.zig");
const locks = @import("pessimistic_lock_contract.zig");
const DocStore = @import("docstore.zig").DocStore;
const A = std.mem.Allocator;
const a = std.testing.allocator;
const ns: ledger.Namespace = .{ .group = 21, .incarnation = @splat(1) };
const source: ledger.Lease = .{ .backend = .{ .incarnation = @splat(1), .id = @splat(2), .fence = 1 }, .database = 1, .node = 7, .principal = @splat(3), .expires_at_ms = 1000 };
const destination: ledger.Lease = .{ .backend = .{ .incarnation = @splat(1), .id = @splat(4), .fence = 1 }, .database = 1, .node = 8, .principal = @splat(5), .expires_at_ms = 1000 };
const transaction: [16]u8 = @splat(9);
const Context = struct {
    lease: ledger.Lease,
    writable: bool = true,
    pub fn requireLease(self: Context, _: anytype, value: ledger.Lease) !void {
        if (!self.writable or !std.meta.eql(self.lease, value)) return error.Forbidden;
    }
    pub fn requireEffect(self: Context, txn: anytype, value: ledger.Lease, request: ledger.Effect) !void {
        try self.requireLease(txn, value);
        if (!request.backend.eql(value.backend)) return error.Forbidden;
    }
    pub fn requireObservation(self: Context, _: anytype, value: ledger.Lease, request: ledger.Effect) !void {
        if (!std.meta.eql(self.lease, value) or !request.backend.eql(value.backend)) return error.Forbidden;
    }
    pub fn requirePrepare(self: Context, txn: anytype, from: ledger.Lease, to: ledger.Lease, id: [16]u8) !void {
        try self.requireLease(txn, from);
        if (!std.meta.eql(from, source) or !std.meta.eql(to, destination) or !std.mem.eql(u8, &transaction, &id)) return error.Forbidden;
    }
};
const ca: Context = .{ .lease = source };
const cb: Context = .{ .lease = destination };
fn effect(lease: ledger.Lease, serial: u64, resource: u64) ledger.Effect {
    var bytes: [32]u8 = @splat(0);
    std.mem.writeInt(u64, bytes[24..32], resource, .big);
    var nonce: [16]u8 = @splat(1);
    std.mem.writeInt(u64, nonce[8..16], serial, .little);
    return .{ .backend = lease.backend, .sequence = serial, .nonce = nonce, .action = .{ .try_acquire = .{ .resource = bytes, .mode = .{ .advisory = .exclusive }, .scope = .session } } };
}
fn apply(store: *DocStore, request: ledger.Effect, context: Context) !ledger.Receipt {
    var txn = try store.beginWriteTxn();
    errdefer txn.abort();
    const receipt = try ledger.apply(a, &txn, ns, request, context);
    try txn.commit();
    return receipt;
}
fn quantum(store: *DocStore, request: ledger.Effect, context: Context) !ledger.Receipt {
    var txn = try store.beginWriteTxn();
    errdefer txn.abort();
    const receipt = try ledger.step(a, &txn, ns, request, context);
    try txn.commit();
    return receipt;
}
fn register(store: *DocStore, lease: ledger.Lease, context: Context) !void {
    var txn = try store.beginWriteTxn();
    errdefer txn.abort();
    try ledger.register(a, &txn, ns, lease, context);
    try txn.commit();
}
var path_nonce: u64 = 0;
fn pathFor(label: []const u8) ![:0]u8 {
    const serial = @atomicRmw(u64, &path_nonce, .Add, 1, .monotonic);
    return std.fmt.allocPrintSentinel(a, "/tmp/antfly-lock-{s}-{d}-{d}", .{ label, @import("antfly_platform").time.monotonicNs(), serial }, 0);
}
fn cleanup(path: []const u8) void {
    std.Io.Dir.cwd().deleteTree(std.testing.io, path) catch {};
}
const FaultTxn = struct {
    txn: *DocStore.Txn,
    remaining: usize,
    pub fn get(self: *FaultTxn, address: []const u8) ![]const u8 {
        return self.txn.get(address);
    }
    pub fn openCursor(self: *FaultTxn) !DocStore.Txn.CursorAdapter {
        return self.txn.openCursor();
    }
    fn reserve(self: *FaultTxn) !void {
        if (self.remaining == 0) return error.InjectedLockWriteFailure;
        self.remaining -= 1;
    }
    pub fn put(self: *FaultTxn, address: []const u8, value: []const u8) !void {
        try self.reserve();
        try self.txn.put(address, value);
    }
    pub fn delete(self: *FaultTxn, address: []const u8) !void {
        try self.reserve();
        try self.txn.delete(address);
    }
};

test "pessimistic locks native LSM aborted transfer preserves every index and cold quantum replays cannot release prepared ownership" {
    const path = try pathFor("handoff");
    defer a.free(path);
    defer cleanup(path);
    var store = try DocStore.open(a, path, .{});
    defer store.close();
    try register(&store, source, ca);
    const resources = 16;
    for (1..resources + 1) |number| {
        var acquire = effect(source, @intCast(number), @intCast(number));
        acquire.action.try_acquire.scope = .{ .transaction = .{ .id = transaction, .subtransaction = 4 } };
        try std.testing.expectEqual(ledger.Outcome.granted, (try apply(&store, acquire, ca)).outcome);
    }
    var prepare = effect(source, resources + 1, 1);
    prepare.action = .{ .control = .{ .prepare = .{ .transaction = transaction, .destination = destination } } };
    _ = try apply(&store, prepare, ca);
    var pinned = try store.beginReadTxn();
    var owns_pinned = true;
    defer if (owns_pinned) pinned.abort();
    // Exercise every staged write boundary of the physical transfer. A native
    // abort must preserve the source grant, destination index and job cursor.
    for (0..9) |fail_after| {
        var txn = try store.beginWriteTxn();
        var fault: FaultTxn = .{ .txn = &txn, .remaining = fail_after };
        try std.testing.expectError(error.InjectedLockWriteFailure, ledger.step(a, &fault, ns, prepare, ca));
        txn.abort();
        var read = try store.beginReadTxn();
        defer read.abort();
        try std.testing.expectEqual(@as(u32, 0), (try ledger.readOwner(a, &read, ns, source.backend)).?.job.?.visited);
        try std.testing.expectEqual(@as(u32, resources), (try ledger.readOwner(a, &read, ns, source.backend)).?.resources);
        try std.testing.expectEqual(@as(u32, 0), (try ledger.readOwner(a, &read, ns, destination.backend)).?.resources);
    }
    _ = try quantum(&store, prepare, ca);
    try std.testing.expectEqual(@as(u32, 0), (try ledger.readOwner(a, &pinned, ns, source.backend)).?.job.?.visited);
    pinned.abort();
    owns_pinned = false;
    // Reopen the actual LSM/WAL at every boundary, including an unobserved
    // final receipt. Only this source's indexed resources are visited.
    var visited: u32 = 1;
    while (true) {
        store.close();
        store = try DocStore.open(a, path, .{});
        const receipt = try quantum(&store, prepare, ca);
        if (receipt.outcome == .completed) break;
        visited += 1;
        var read = try store.beginReadTxn();
        defer read.abort();
        try std.testing.expectEqual(visited, (try ledger.readOwner(a, &read, ns, source.backend)).?.job.?.visited);
    }
    try std.testing.expectEqual(@as(u32, resources), visited);
    store.close();
    store = try DocStore.open(a, path, .{});
    try std.testing.expectEqual(ledger.Outcome.completed, (try quantum(&store, prepare, ca)).outcome);
    try std.testing.expectEqual(ledger.Outcome.completed, (try apply(&store, prepare, ca)).outcome);
    try std.testing.expectEqual(ledger.Outcome.contended, (try apply(&store, effect(source, resources + 2, 1), ca)).outcome);
    var close = effect(destination, 1, 1);
    close.action = .{ .control = .close };
    _ = try apply(&store, close, cb);
    while ((try quantum(&store, close, cb)).outcome == .pending) {}
    try std.testing.expectEqual(ledger.Outcome.granted, (try apply(&store, effect(source, resources + 3, 1), ca)).outcome);
    try std.testing.expectError(error.LockOwnerBusy, apply(&store, effect(destination, 2, 1), cb));
}

test "pessimistic locks native LSM keeps exact false try receipts and session rollback across reopen" {
    const path = try pathFor("receipts");
    defer a.free(path);
    defer cleanup(path);
    var store = try DocStore.open(a, path, .{});
    defer store.close();
    try register(&store, source, ca);
    try register(&store, destination, cb);
    const acquire = effect(source, 1, 1);
    _ = try apply(&store, acquire, ca);
    _ = try apply(&store, acquire, ca);
    const refused = effect(destination, 1, 1);
    try std.testing.expectEqual(ledger.Outcome.contended, (try apply(&store, refused, cb)).outcome);
    var acquire_xact = effect(source, 2, 1);
    acquire_xact.action.try_acquire.scope = .{ .transaction = .{ .id = transaction, .subtransaction = 4 } };
    _ = try apply(&store, acquire_xact, ca);
    var rollback = effect(source, 3, 1);
    rollback.action = .{ .control = .{ .rollback = .{ .transaction = transaction, .first_aborted = 0 } } };
    _ = try apply(&store, rollback, ca);
    while ((try quantum(&store, rollback, ca)).outcome == .pending) {}
    try std.testing.expectEqual(ledger.Outcome.contended, (try apply(&store, effect(destination, 2, 1), cb)).outcome);
    var unlock = effect(source, 4, 1);
    unlock.action = .{ .unlock = .{ .resource = acquire.action.resource(), .mode = .exclusive } };
    try std.testing.expectEqual(ledger.Outcome.unlocked, (try apply(&store, unlock, ca)).outcome);
    store.close();
    store = try DocStore.open(a, path, .{});
    // Replay cannot reacquire a released lock, repeat a refcount decrement,
    // or turn a previous false result into a newly admitted true result.
    _ = try apply(&store, acquire, ca);
    _ = try apply(&store, unlock, ca);
    try std.testing.expectEqual(ledger.Outcome.contended, (try apply(&store, refused, cb)).outcome);
    try std.testing.expectEqual(ledger.Outcome.granted, (try apply(&store, effect(destination, 3, 1), cb)).outcome);
    try std.testing.expectError(error.Forbidden, apply(&store, effect(source, 5, 1), .{ .lease = source, .writable = false }));
}

fn waiting(lease: ledger.Lease, serial: u64, resource: u64, mode: locks.AdvisoryMode) ledger.Effect {
    var request = effect(lease, serial, resource);
    const acquisition: ledger.Acquisition = .{ .resource = request.action.resource(), .mode = .{ .advisory = mode }, .scope = .session };
    request.action = .{ .acquire = acquisition };
    return request;
}
fn cancel(store: *DocStore, request: ledger.Effect, context: Context) !ledger.Receipt {
    var txn = try store.beginWriteTxn();
    errdefer txn.abort();
    const receipt = try ledger.cancel(a, &txn, ns, request, context);
    try txn.commit();
    return receipt;
}
fn finish(store: *DocStore, lease: ledger.Lease, serial: u64, context: Context) !void {
    var request = effect(lease, serial, 1);
    request.action = .{ .control = .close };
    _ = try apply(store, request, context);
    var count: usize = 0;
    while ((try quantum(store, request, context)).outcome == .pending) {
        count += 1;
        if (count > ledger.max_resources_per_backend) return error.UnboundedLockCleanup;
    }
}

test "pessimistic locks native LSM queued cancellation survives all abort boundaries and fences late admission after cold reopen" {
    const path = try pathFor("wait-cancel");
    defer a.free(path);
    defer cleanup(path);
    var store = try DocStore.open(a, path, .{});
    defer store.close();
    try register(&store, source, ca);
    try register(&store, destination, cb);
    _ = try apply(&store, effect(source, 1, 1), ca);
    const queued = waiting(destination, 1, 1, .exclusive);
    try std.testing.expectEqual(ledger.Outcome.pending, (try apply(&store, queued, cb)).outcome);
    for (0..4) |failure| {
        var txn = try store.beginWriteTxn();
        var fault: FaultTxn = .{ .txn = &txn, .remaining = failure };
        try std.testing.expectError(error.InjectedLockWriteFailure, ledger.cancel(a, &fault, ns, queued, cb));
        txn.abort();
    }
    store.close();
    store = try DocStore.open(a, path, .{});
    try std.testing.expectEqual(ledger.Outcome.pending, (try apply(&store, queued, cb)).outcome);
    try std.testing.expectEqual(ledger.Outcome.cancelled, (try cancel(&store, queued, cb)).outcome);
    try finish(&store, source, 2, ca);
    store.close();
    store = try DocStore.open(a, path, .{});
    try std.testing.expectEqual(ledger.Outcome.cancelled, (try apply(&store, queued, cb)).outcome);
    const unadmitted = waiting(destination, 2, 2, .exclusive);
    try std.testing.expectEqual(ledger.Outcome.cancelled, (try cancel(&store, unadmitted, cb)).outcome);
    store.close();
    store = try DocStore.open(a, path, .{});
    try std.testing.expectEqual(ledger.Outcome.cancelled, (try apply(&store, unadmitted, cb)).outcome);
    const admitted = waiting(destination, 3, 2, .exclusive);
    try std.testing.expectEqual(ledger.Outcome.granted, (try apply(&store, admitted, cb)).outcome);
    try std.testing.expectEqual(ledger.Outcome.granted, (try cancel(&store, admitted, cb)).outcome);
    var read = try store.beginReadTxn();
    defer read.abort();
    try std.testing.expectEqual(@as(u32, 1), (try ledger.readOwner(a, &read, ns, destination.backend)).?.resources);
}

test "pessimistic locks native LSM hard and soft cycle decisions use durable current edges across cold reopen" {
    const path = try pathFor("deadlocks");
    defer a.free(path);
    defer cleanup(path);
    var store = try DocStore.open(a, path, .{});
    defer store.close();
    const third: ledger.Lease = .{ .backend = .{ .incarnation = @splat(1), .id = @splat(8), .fence = 1 }, .database = 1, .node = 9, .principal = @splat(8), .expires_at_ms = 1000 };
    const cc: Context = .{ .lease = third };
    try register(&store, source, ca);
    try register(&store, destination, cb);
    try register(&store, third, cc);
    _ = try apply(&store, waiting(source, 1, 1, .shared), ca);
    _ = try apply(&store, effect(third, 1, 2), cc);
    const b_wait = waiting(destination, 1, 1, .exclusive);
    _ = try apply(&store, b_wait, cb);
    const c_wait = waiting(third, 2, 1, .shared);
    _ = try apply(&store, c_wait, cc);
    store.close();
    store = try DocStore.open(a, path, .{});
    const a_wait = waiting(source, 2, 2, .exclusive);
    // This soft cycle has a legal queue ordering; none of its owners aborts.
    try std.testing.expectEqual(ledger.Outcome.pending, (try apply(&store, a_wait, ca)).outcome);
    store.close();
    store = try DocStore.open(a, path, .{});
    try std.testing.expectEqual(ledger.Outcome.granted, (try apply(&store, c_wait, cc)).outcome);
    try finish(&store, third, 3, cc);
    try std.testing.expectEqual(ledger.Outcome.granted, (try apply(&store, a_wait, ca)).outcome);
    // Cancel the first wait, then enter the independent hard-cycle fixture.
    _ = try cancel(&store, b_wait, cb);
    _ = try apply(&store, effect(destination, 2, 3), cb);
    const a_hard = waiting(source, 3, 3, .exclusive);
    _ = try apply(&store, a_hard, ca);
    const b_hard = waiting(destination, 3, 1, .exclusive);
    try std.testing.expectEqual(ledger.Outcome.deadlocked, (try apply(&store, b_hard, cb)).outcome);
    store.close();
    store = try DocStore.open(a, path, .{});
    try std.testing.expectEqual(ledger.Outcome.deadlocked, (try apply(&store, b_hard, cb)).outcome);
    try finish(&store, destination, 4, cb);
    try std.testing.expectEqual(ledger.Outcome.granted, (try apply(&store, a_hard, ca)).outcome);
}
