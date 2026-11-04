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
const ns: ledger.Namespace = .{ .home = .metadata, .group = 21, .incarnation = @splat(1) };
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

const scopes = @import("pessimistic_lock_authority.zig");
const scope_registration: scopes.Registration = .{ .group = ns.group, .lease = source, .connection = @splat('a'), .nonce = @splat(4), .standalone = false };
const scope_command: scopes.Command = .{ .sequence = 1, .nonce = @splat(5), .executor = @splat(6) };
const scope_decision: scopes.DecisionRef = .{ .operation = @splat(7), .admission = @splat(8) };
/// These fixtures use durable native authority rows, not source-provided flags.
/// The production receiver must join the same checks to its external registry.
const ScopeAuthority = struct {
    allowed: bool = true,
    prepared: bool = false,
    pub fn requireRegistration(self: @This(), _: anytype, _: ledger.Namespace, value: scopes.Registration) !void {
        if (!self.allowed or !std.meta.eql(value, scope_registration)) return error.Forbidden;
    }
    pub fn requireCommand(self: @This(), _: anytype, _: ledger.Namespace, value: scopes.Record, command: scopes.Command) !void {
        var registration = value.registration;
        registration.lease.expires_at_ms = source.expires_at_ms;
        if (!self.allowed or !std.meta.eql(registration, scope_registration) or !std.mem.eql(u8, &command.executor, &scope_command.executor)) return error.Forbidden;
    }
    pub fn requireTransaction(self: @This(), txn: anytype, namespace: ledger.Namespace, value: scopes.Record, id: [16]u8) !void {
        try self.requireCommand(txn, namespace, value, scope_command);
        if (!std.mem.eql(u8, &id, &transaction)) return error.Forbidden;
    }
    pub fn requireSeal(self: @This(), txn: anytype, namespace: ledger.Namespace, value: scopes.Record, decision: scopes.DecisionRef) !void {
        try self.requireCommand(txn, namespace, value, scope_command);
        if (!std.meta.eql(decision, scope_decision) or !std.mem.eql(u8, try txn.get("scope-test/admission"), &scope_decision.admission)) return error.Forbidden;
    }
    pub fn requireResolution(self: @This(), txn: anytype, namespace: ledger.Namespace, value: scopes.Record, expected: scopes.Transaction) !void {
        try self.requireCommand(txn, namespace, value, scope_command);
        if (!std.mem.eql(u8, &expected.id, &transaction) or !std.mem.eql(u8, try txn.get("scope-test/resolved"), &transaction)) return error.LockOwnerBusy;
    }
    pub fn requireRenewal(self: @This(), txn: anytype, namespace: ledger.Namespace, value: scopes.Record, _: u64, _: u64) !void {
        try self.requireCommand(txn, namespace, value, scope_command);
    }
    pub fn requireFence(self: @This(), txn: anytype, namespace: ledger.Namespace, value: scopes.Record, _: scopes.Fence) !void {
        try self.requireCommand(txn, namespace, value, scope_command);
    }
    pub fn requireRetirement(self: @This(), txn: anytype, namespace: ledger.Namespace, value: scopes.Record) !void {
        try self.requireCommand(txn, namespace, value, scope_command);
        if (value.transaction != null) return error.LockOwnerBusy;
    }
    pub fn requirePreparedHandoff(self: @This(), txn: anytype, namespace: ledger.Namespace, value: scopes.Record, from: ledger.Lease, to: ledger.Lease, id: [16]u8) !void {
        if (!self.prepared) return error.Forbidden;
        try self.requireSeal(txn, namespace, value, scope_decision);
        if (!std.mem.eql(u8, try txn.get("scope-test/preparing"), &transaction) or !std.meta.eql(from, value.registration.lease) or !std.meta.eql(to, try scopes.preparedLease(namespace, source.database, source.node, scope_decision)) or !std.mem.eql(u8, &id, &transaction)) return error.Forbidden;
    }
    pub fn requirePreparedResolution(self: @This(), txn: anytype, namespace: ledger.Namespace, lease: ledger.Lease, id: [16]u8) !void {
        if (!self.prepared or !self.allowed or !std.meta.eql(lease, try scopes.preparedLease(namespace, source.database, source.node, scope_decision)) or !std.mem.eql(u8, &id, &transaction)) return error.Forbidden;
        const proof = txn.get("scope-test/resolved") catch |err| return if (err == error.NotFound) error.LockOwnerBusy else err;
        if (!std.mem.eql(u8, proof, &transaction)) return error.LockOwnerBusy;
    }
};
const scope_authority: ScopeAuthority = .{};
fn registerScope(store: *DocStore) !void {
    var txn = try store.beginWriteTxn();
    errdefer txn.abort();
    try scopes.register(a, &txn, ns, scope_registration, scope_authority);
    try std.testing.expectEqual(scopes.Begin.admitted, try scopes.begin(a, &txn, ns, source.backend, scope_command, scope_authority));
    try txn.commit();
}
fn closeScope(store: *DocStore) !bool {
    var txn = try store.beginWriteTxn();
    errdefer txn.abort();
    const done = try scopes.closeStep(a, &txn, ns, source.backend, scope_authority);
    try txn.commit();
    return done;
}

test "pessimistic locks native LSM command scope fences late writers before bounded queued cleanup" {
    const path = try pathFor("scope-fence");
    defer a.free(path);
    defer cleanup(path);
    var store = try DocStore.open(a, path, .{});
    defer store.close();
    try registerScope(&store);
    try register(&store, destination, cb);
    _ = try apply(&store, effect(destination, 1, 1), cb);
    const request = waiting(source, 1, 1, .exclusive);
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        try std.testing.expectEqual(ledger.Outcome.pending, (try scopes.applyEffect(a, &txn, ns, source.backend, scope_command, request, scope_authority)).outcome);
        try scopes.renew(a, &txn, ns, source.backend, 1000, 2000, scope_authority);
        try txn.commit();
    }
    store.close();
    store = try DocStore.open(a, path, .{});
    {
        var txn = try store.beginWriteTxn();
        defer txn.abort();
        try std.testing.expectError(error.LockOwnerChanged, scopes.fence(a, &txn, ns, source.backend, .{ .expired = .{ .expected_expiry = 1000, .observed_at_ms = 1001 } }, scope_authority));
        try std.testing.expectError(error.Forbidden, scopes.guard(a, &txn, ns, source.backend, scope_command, ScopeAuthority{ .allowed = false }));
        try std.testing.expectEqual(@as(u64, 2000), (try ledger.readOwner(a, &txn, ns, source.backend)).?.lease.expires_at_ms);
        try std.testing.expect((try ledger.readOwner(a, &txn, ns, source.backend)).?.waiting != null);
    }
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        try scopes.fence(a, &txn, ns, source.backend, .{ .expired = .{ .expected_expiry = 2000, .observed_at_ms = 2000 } }, scope_authority);
        try txn.commit();
    }
    // A full cold reopen cannot restore an old command's write authority.
    var steps: usize = 0;
    while (true) {
        store.close();
        store = try DocStore.open(a, path, .{});
        {
            var read = try store.beginReadTxn();
            defer read.abort();
            try std.testing.expectError(error.LockOwnerChanged, scopes.guard(a, &read, ns, source.backend, scope_command, scope_authority));
        }
        steps += 1;
        if (try closeScope(&store)) break;
        try std.testing.expect(steps <= 6);
    }
    try std.testing.expect(try closeScope(&store));
    var read = try store.beginReadTxn();
    defer read.abort();
    try std.testing.expectEqual(.retired, (try scopes.read(a, &read, ns, source.backend)).?.state);
    // Cancelling an obsolete wait has not unlocked the independent holder.
    try std.testing.expectEqual(@as(u32, 1), (try ledger.readOwner(a, &read, ns, destination.backend)).?.resources);
}

test "pessimistic locks native LSM sealed scope retains grants until durable transaction resolution across cold reopen" {
    const path = try pathFor("scope-decision");
    defer a.free(path);
    defer cleanup(path);
    var store = try DocStore.open(a, path, .{});
    defer store.close();
    try registerScope(&store);
    try register(&store, destination, cb);
    var request = effect(source, 1, 1);
    request.action.try_acquire.scope = .{ .transaction = .{ .id = transaction, .subtransaction = 0 } };
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        try scopes.bindTransaction(a, &txn, ns, source.backend, scope_command, transaction, scope_authority);
        _ = try scopes.applyEffect(a, &txn, ns, source.backend, scope_command, request, scope_authority);
        try txn.put("scope-test/admission", &scope_decision.admission);
        try scopes.seal(a, &txn, ns, source.backend, scope_command, scope_decision, scope_authority);
        try scopes.seal(a, &txn, ns, source.backend, scope_command, scope_decision, scope_authority);
        try scopes.fence(a, &txn, ns, source.backend, .close, scope_authority);
        try txn.commit();
    }
    store.close();
    store = try DocStore.open(a, path, .{});
    try std.testing.expectError(error.LockOwnerBusy, closeScope(&store));
    try std.testing.expectEqual(ledger.Outcome.contended, (try apply(&store, effect(destination, 1, 1), cb)).outcome);
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        try txn.put("scope-test/resolved", &transaction);
        _ = try scopes.releaseTransaction(a, &txn, ns, source.backend, .{ .id = transaction, .decision = scope_decision }, scope_authority);
        try txn.commit();
    }
    var release_steps: usize = 0;
    while (true) {
        store.close();
        store = try DocStore.open(a, path, .{});
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        const done = try scopes.releaseTransaction(a, &txn, ns, source.backend, .{ .id = transaction, .decision = scope_decision }, scope_authority);
        try txn.commit();
        if (done) break;
        release_steps += 1;
        try std.testing.expect(release_steps <= 6);
    }
    var steps: usize = 0;
    while (true) {
        store.close();
        store = try DocStore.open(a, path, .{});
        steps += 1;
        if (try closeScope(&store)) break;
        try std.testing.expect(steps <= 6);
    }
    try std.testing.expectEqual(ledger.Outcome.granted, (try apply(&store, effect(destination, 2, 1), cb)).outcome);
}

test "pessimistic locks native LSM scope renewal and registration abort every partial native write" {
    const path = try pathFor("scope-faults");
    defer a.free(path);
    defer cleanup(path);
    var store = try DocStore.open(a, path, .{});
    defer store.close();
    // Ledger owner, scope and capacity counter share one accepting transaction.
    for (0..6) |fail_after| {
        var txn = try store.beginWriteTxn();
        var fault: FaultTxn = .{ .txn = &txn, .remaining = fail_after };
        try std.testing.expectError(error.InjectedLockWriteFailure, scopes.register(a, &fault, ns, scope_registration, scope_authority));
        txn.abort();
        var read = try store.beginReadTxn();
        defer read.abort();
        try std.testing.expectEqual(@as(?scopes.Record, null), try scopes.read(a, &read, ns, source.backend));
        try std.testing.expectEqual(@as(u32, 0), try scopes.databaseCount(&read, ns, source.database));
        try std.testing.expectEqual(@as(?ledger.Owner, null), try ledger.readOwner(a, &read, ns, source.backend));
    }
    try registerScope(&store);
    for (0..4) |fail_after| {
        var txn = try store.beginWriteTxn();
        var fault: FaultTxn = .{ .txn = &txn, .remaining = fail_after };
        try std.testing.expectError(error.InjectedLockWriteFailure, scopes.renew(a, &fault, ns, source.backend, 1000, 2000, scope_authority));
        txn.abort();
        store.close();
        store = try DocStore.open(a, path, .{});
        var read = try store.beginReadTxn();
        defer read.abort();
        try std.testing.expectEqual(@as(u64, 1000), (try scopes.read(a, &read, ns, source.backend)).?.registration.lease.expires_at_ms);
        try std.testing.expectEqual(@as(u64, 1000), (try ledger.readOwner(a, &read, ns, source.backend)).?.lease.expires_at_ms);
    }
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        try scopes.finish(a, &txn, ns, source.backend, scope_command, scope_authority);
        try scopes.finish(a, &txn, ns, source.backend, scope_command, scope_authority);
        try std.testing.expectEqual(scopes.Begin.completed, try scopes.begin(a, &txn, ns, source.backend, scope_command, scope_authority));
        try std.testing.expectError(error.LockEffectExpired, scopes.guard(a, &txn, ns, source.backend, scope_command, scope_authority));
        var next = scope_command;
        next.sequence = 2;
        try std.testing.expectEqual(scopes.Begin.admitted, try scopes.begin(a, &txn, ns, source.backend, next, scope_authority));
        var forged = next;
        forged.nonce = @splat(9);
        try std.testing.expectError(error.LockEffectChanged, scopes.begin(a, &txn, ns, source.backend, forged, scope_authority));
        try txn.commit();
    }
}

test "pessimistic locks native LSM active resolution wakes immediate work preserves session refs and invalidates old commands" {
    const path = try pathFor("scope-active-resolution");
    defer a.free(path);
    defer cleanup(path);
    var store = try DocStore.open(a, path, .{});
    defer store.close();
    try registerScope(&store);
    try register(&store, destination, cb);
    const expected: scopes.Transaction = .{ .id = transaction, .decision = scope_decision };
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        _ = try scopes.applyEffect(a, &txn, ns, source.backend, scope_command, effect(source, 1, 1), scope_authority);
        try scopes.bindTransaction(a, &txn, ns, source.backend, scope_command, transaction, scope_authority);
        var xact = effect(source, 2, 1);
        xact.action.try_acquire.scope = .{ .transaction = .{ .id = transaction, .subtransaction = 0 } };
        _ = try scopes.applyEffect(a, &txn, ns, source.backend, scope_command, xact, scope_authority);
        try txn.put("scope-test/admission", &scope_decision.admission);
        try txn.put("scope-test/resolved", &transaction);
        try scopes.seal(a, &txn, ns, source.backend, scope_command, scope_decision, scope_authority);
        // This is earlier than the backend lease expiry, not an idle timeout.
        try std.testing.expectEqual(@as(?scopes.Work, null), try scopes.nextWork(a, &txn, ns, 100, null));
        try txn.commit();
    }
    var quanta: usize = 0;
    while (true) {
        // Every failed write, including a fully staged but aborted quantum,
        // preserves the prior scope, membership counts and job cursor.
        var before = try store.beginReadTxn();
        const before_scope = (try scopes.read(a, &before, ns, source.backend)).?;
        const before_owner = (try ledger.readOwner(a, &before, ns, source.backend)).?;
        before.abort();
        for (0..16) |failure| {
            var txn = try store.beginWriteTxn();
            var fault: FaultTxn = .{ .txn = &txn, .remaining = failure };
            _ = scopes.releaseTransaction(a, &fault, ns, source.backend, expected, scope_authority) catch |err| switch (err) {
                error.InjectedLockWriteFailure => false,
                else => return err,
            };
            txn.abort();
            var read = try store.beginReadTxn();
            defer read.abort();
            try std.testing.expectEqualDeep(before_scope, (try scopes.read(a, &read, ns, source.backend)).?);
            try std.testing.expectEqualDeep(before_owner, (try ledger.readOwner(a, &read, ns, source.backend)).?);
        }
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        const done = try scopes.releaseTransaction(a, &txn, ns, source.backend, expected, scope_authority);
        try txn.commit();
        if (!done) {
            var read = try store.beginReadTxn();
            defer read.abort();
            const work = (try scopes.nextWork(a, &read, ns, 100, null)).?;
            try std.testing.expectEqual(@as(u64, 0), work.at_ms);
            try std.testing.expect(work.backend.eql(source.backend));
        }
        store.close();
        store = try DocStore.open(a, path, .{});
        if (done) break;
        quanta += 1;
        try std.testing.expect(quanta <= 6);
    }
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        try std.testing.expectEqual(@as(?[16]u8, null), (try ledger.readOwner(a, &txn, ns, source.backend)).?.transaction);
        try std.testing.expectEqual(@as(u32, 0), (try ledger.readOwner(a, &txn, ns, source.backend)).?.transaction_resources);
        try std.testing.expectEqual(@as(u32, 1), (try ledger.readOwner(a, &txn, ns, source.backend)).?.resources);
        try std.testing.expectError(error.LockEffectExpired, scopes.guard(a, &txn, ns, source.backend, scope_command, scope_authority));
        try std.testing.expectEqual(scopes.Begin.completed, try scopes.begin(a, &txn, ns, source.backend, scope_command, scope_authority));
        try std.testing.expectEqual(@as(?scopes.Work, null), try scopes.nextWork(a, &txn, ns, 100, null));
        try txn.commit();
    }
    // COMMIT releases transaction holdings, not an independent session ref.
    try std.testing.expectEqual(ledger.Outcome.contended, (try apply(&store, effect(destination, 1, 1), cb)).outcome);
}

test "pessimistic locks native LSM scoped prepared handoff and cleanup abort every partial write and retain cold independent ownership" {
    const path = try pathFor("scoped-prepare-faults");
    defer a.free(path);
    defer cleanup(path);
    const authority: ScopeAuthority = .{ .prepared = true };
    const expected: scopes.Transaction = .{ .id = transaction, .decision = scope_decision };
    const prepared_lease = try scopes.preparedLease(ns, source.database, source.node, scope_decision);
    const Helper = struct {
        fn hash(store: *DocStore) ![32]u8 {
            var txn = try store.beginReadTxn();
            defer txn.abort();
            var cursor = try txn.openCursor();
            defer cursor.close();
            var item = try cursor.seekAtOrAfter("");
            var h = std.crypto.hash.Blake3.init(.{});
            while (item) |pair| : (item = try cursor.next()) {
                var lengths: [16]u8 = undefined;
                std.mem.writeInt(u64, lengths[0..8], pair.key.len, .little);
                std.mem.writeInt(u64, lengths[8..16], pair.value.len, .little);
                h.update(&lengths);
                h.update(pair.key);
                h.update(pair.value);
            }
            var out: [32]u8 = undefined;
            h.final(&out);
            return out;
        }
    };
    var store = try DocStore.open(a, path, .{});
    defer store.close();
    try registerScope(&store);
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        try scopes.bindTransaction(a, &txn, ns, source.backend, scope_command, transaction, authority);
        for (1..4) |i| {
            var acquire = effect(source, @intCast(i), @intCast(i));
            if (i != 3) acquire.action.try_acquire.scope = .{ .transaction = .{ .id = transaction, .subtransaction = 0 } };
            _ = try scopes.applyEffect(a, &txn, ns, source.backend, scope_command, acquire, authority);
        }
        try txn.put("scope-test/admission", &scope_decision.admission);
        try txn.put("scope-test/preparing", &transaction);
        try scopes.seal(a, &txn, ns, source.backend, scope_command, scope_decision, authority);
        try txn.commit();
    }
    var transferred = false;
    for (0..8) |_| {
        const before = try Helper.hash(&store);
        var reached_end = false;
        for (0..32) |fail_after| {
            var txn = try store.beginWriteTxn();
            var fault: FaultTxn = .{ .txn = &txn, .remaining = fail_after };
            const attempt = scopes.prepareStep(a, &fault, ns, source.backend, scope_command, expected, prepared_lease, authority);
            var failure: ?anyerror = null;
            if (attempt) |_| {
                reached_end = true;
            } else |err| failure = err;
            txn.abort();
            if (failure) |err| try std.testing.expectEqual(error.InjectedLockWriteFailure, err);
            try std.testing.expectEqual(before, try Helper.hash(&store));
            if (reached_end) {
                try std.testing.expect(fail_after != 0);
                break;
            }
        }
        try std.testing.expect(reached_end);
        {
            var txn = try store.beginWriteTxn();
            errdefer txn.abort();
            transferred = try scopes.prepareStep(a, &txn, ns, source.backend, scope_command, expected, prepared_lease, authority);
            try txn.commit();
        }
        store.close();
        store = try DocStore.open(a, path, .{});
        if (transferred) break;
    }
    try std.testing.expect(transferred);
    {
        var txn = try store.beginReadTxn();
        defer txn.abort();
        const owner = (try ledger.readOwner(a, &txn, ns, prepared_lease.backend)).?;
        try std.testing.expectEqual(.prepared, owner.state);
        try std.testing.expectEqual(@as(u32, 2), owner.resources);
        try std.testing.expectEqual(@as(u32, 1), (try ledger.readOwner(a, &txn, ns, source.backend)).?.resources);
        try std.testing.expectEqual(expected, (try scopes.read(a, &txn, ns, source.backend)).?.released_transaction.?);
    }
    {
        var txn = try store.beginWriteTxn();
        defer txn.abort();
        try std.testing.expectError(error.LockOwnerBusy, scopes.releasePreparedStep(a, &txn, ns, prepared_lease, transaction, authority));
    }
    // Retiring the source closes its remaining session grant, while the
    // destination remains non-expiring and independently decision-owned.
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        try scopes.fence(a, &txn, ns, source.backend, .close, authority);
        try txn.commit();
    }
    for (0..6) |_| if (try closeScope(&store)) break;
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        try txn.put("scope-test/resolved", &transaction);
        try txn.commit();
    }
    var released = false;
    for (0..8) |_| {
        const before = try Helper.hash(&store);
        var reached_end = false;
        for (0..32) |fail_after| {
            var txn = try store.beginWriteTxn();
            var fault: FaultTxn = .{ .txn = &txn, .remaining = fail_after };
            const attempt = scopes.releasePreparedStep(a, &fault, ns, prepared_lease, transaction, authority);
            var failure: ?anyerror = null;
            if (attempt) |_| {
                reached_end = true;
            } else |err| failure = err;
            txn.abort();
            if (failure) |err| try std.testing.expectEqual(error.InjectedLockWriteFailure, err);
            try std.testing.expectEqual(before, try Helper.hash(&store));
            if (reached_end) {
                try std.testing.expect(fail_after != 0);
                break;
            }
        }
        try std.testing.expect(reached_end);
        {
            var txn = try store.beginWriteTxn();
            errdefer txn.abort();
            released = try scopes.releasePreparedStep(a, &txn, ns, prepared_lease, transaction, authority);
            try txn.commit();
        }
        store.close();
        store = try DocStore.open(a, path, .{});
        if (released) break;
    }
    try std.testing.expect(released);
    const before_reclaim = try Helper.hash(&store);
    for (0..2) |fail_after| {
        var txn = try store.beginWriteTxn();
        var fault: FaultTxn = .{ .txn = &txn, .remaining = fail_after };
        const attempt = ledger.reclaimPrepared(a, &fault, ns, prepared_lease, transaction, authority);
        txn.abort();
        try std.testing.expectError(error.InjectedLockWriteFailure, attempt);
        try std.testing.expectEqual(before_reclaim, try Helper.hash(&store));
    }
    {
        var pinned = try store.beginReadTxn();
        defer pinned.abort();
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        try ledger.reclaimPrepared(a, &txn, ns, prepared_lease, transaction, authority);
        try txn.commit();
        // Existing readers retain their immutable view across reclamation.
        try std.testing.expectEqual(.retired, (try ledger.readOwner(a, &pinned, ns, prepared_lease.backend)).?.state);
    }
    store.close();
    store = try DocStore.open(a, path, .{});
    var txn = try store.beginReadTxn();
    defer txn.abort();
    try std.testing.expectEqual(@as(?ledger.Owner, null), try ledger.readOwner(a, &txn, ns, prepared_lease.backend));
    try std.testing.expectEqual(.retired, (try scopes.read(a, &txn, ns, source.backend)).?.state);
}

const data_participant = @import("pessimistic_lock_participant.zig");
const native_transactions = @import("transactions.zig");
const DataAuthority = struct {
    approved: data_participant.Admission,
    enabled: bool = true,
    decision: ?scopes.DecisionRef = null,
    pub fn requireDataAdmission(self: @This(), _: anytype, value: data_participant.Admission) !void {
        if (!self.enabled or !std.meta.eql(self.approved, value)) return error.Forbidden;
    }
    pub fn requireDataCommand(self: @This(), txn: anytype, value: data_participant.Admission, _: @import("pessimistic_lock_authority.zig").Record) !void {
        try self.requireDataAdmission(txn, value);
    }
    pub fn requireDataResource(self: @This(), txn: anytype, value: data_participant.Admission, physical_key: ?[]const u8) !void {
        try self.requireDataAdmission(txn, value);
        if (physical_key) |key_value| if (std.mem.order(u8, key_value, "a") == .lt or std.mem.order(u8, key_value, "z") != .lt) return error.IdentityNamespaceMismatch;
    }
    pub fn requireDataEffect(self: @This(), txn: anytype, value: data_participant.Admission, _: ledger.Effect) !void {
        try self.requireDataAdmission(txn, value);
    }
    pub fn requireDataObservation(self: @This(), txn: anytype, value: data_participant.Admission, _: ledger.Effect) !void {
        try self.requireDataAdmission(txn, value);
    }
    pub fn requireDataControl(self: @This(), txn: anytype, value: data_participant.Admission, _: ledger.Effect) !void {
        try self.requireDataAdmission(txn, value);
    }
    pub fn requireDataFence(self: @This(), txn: anytype, value: data_participant.Admission) !void {
        try self.requireDataAdmission(txn, value);
    }
    fn requireNativeDecision(self: @This(), txn: anytype, selected: scopes.DecisionRef, terminal: bool) !void {
        if (!self.enabled or !std.meta.eql(self.decision, @as(?scopes.DecisionRef, selected))) return error.Forbidden;
        const state = try native_transactions.externalParticipant(txn, self.approved.transaction) orelse return error.Forbidden;
        const owner = state.owner orelse return error.Forbidden;
        if (!std.mem.eql(u8, &owner.operation_id, &selected.operation) or !std.mem.eql(u8, &owner.admission_digest, &selected.admission) or
            !std.mem.eql(u8, &owner.metadata_incarnation, &std.fmt.bytesToHex(self.approved.binding.namespace.incarnation, .lower))) return error.Forbidden;
        if ((state.status != .pending) != terminal) return error.Forbidden;
    }
    pub fn requireDataPreparedAdmission(self: @This(), txn: anytype, value: data_participant.Admission, selected: scopes.DecisionRef, _: scopes.Record) !void {
        try self.requireDataAdmission(txn, value);
        try self.requireNativeDecision(txn, selected, false);
    }
    pub fn requirePreparedDataResolution(self: @This(), txn: anytype, selected_binding: data_participant.Binding, selected: scopes.DecisionRef, id: [16]u8, _: ledger.Lease) !void {
        if (!selected_binding.eql(self.approved.binding) or !std.mem.eql(u8, &id, &self.approved.transaction)) return error.Forbidden;
        try self.requireNativeDecision(txn, selected, true);
    }
    pub fn requireFence(self: @This(), txn: anytype, selected_ns: ledger.Namespace, _: scopes.Record, _: scopes.Fence) !void {
        if (!std.meta.eql(selected_ns, self.approved.binding.namespace)) return error.Forbidden;
        try self.requireNativeDecision(txn, self.decision orelse return error.Forbidden, false);
    }
    pub fn requirePreparedHandoff(_: @This(), _: anytype, _: ledger.Namespace, _: scopes.Record, _: ledger.Lease, _: ledger.Lease, _: [16]u8) !void {
        // This authority closes only after independent prepared resolution.
        // It cannot use retirement as permission to initiate another handoff.
        return error.Forbidden;
    }
    pub fn requireRetirement(self: @This(), txn: anytype, selected_ns: ledger.Namespace, value: scopes.Record) !void {
        if (!std.meta.eql(selected_ns, self.approved.binding.namespace)) return error.Forbidden;
        const released = value.released_transaction orelse return error.Forbidden;
        const selected = released.decision orelse return error.Forbidden;
        if (!std.mem.eql(u8, &released.id, &self.approved.transaction)) return error.Forbidden;
        try self.requireNativeDecision(txn, selected, true);
        const prepared = try ledger.readOwner(a, txn, selected_ns, try scopes.preparedBackend(selected_ns, selected)) orelse return error.Forbidden;
        if (prepared.state != .retired or prepared.resources != 0) return error.Forbidden;
    }
};

test "pessimistic locks native LSM data participants preserve admission atomicity row grants and command fences across cold reopen" {
    const binding = @import("pessimistic_lock_resource.zig");
    const path = try pathFor("data-participant");
    defer a.free(path);
    defer cleanup(path);
    var store = try DocStore.open(a, path, .{});
    defer store.close();
    const admitted = dataFixture();
    const receiving = admitted.binding.identity;
    const registration = try admitted.registration();
    const authorized: DataAuthority = .{ .approved = admitted };
    const denied: DataAuthority = .{ .approved = admitted, .enabled = false };
    {
        var txn = try store.beginWriteTxn();
        defer txn.abort();
        try std.testing.expectError(error.Forbidden, data_participant.admit(a, &txn, receiving, admitted, denied));
        try std.testing.expect((try binding.Binding.load(&txn)) == null);
    }
    var committed = false;
    var aborted_boundaries: usize = 0;
    for (0..32) |fail_after| {
        var txn = try store.beginWriteTxn();
        var fault: FaultTxn = .{ .txn = &txn, .remaining = fail_after };
        const result = data_participant.admit(a, &fault, receiving, admitted, authorized) catch |err| {
            txn.abort();
            try std.testing.expectEqual(error.InjectedLockWriteFailure, err);
            aborted_boundaries += 1;
            var read = try store.beginReadTxn();
            defer read.abort();
            try std.testing.expect((try binding.Binding.load(&read)) == null);
            try std.testing.expect((try scopes.read(a, &read, admitted.binding.namespace, registration.lease.backend)) == null);
            continue;
        };
        try std.testing.expectEqual(scopes.Begin.admitted, result);
        try txn.commit();
        committed = true;
        break;
    }
    try std.testing.expect(committed and aborted_boundaries >= 8);
    const table: data_participant.Acquisition = .{ .admission = admitted, .effect = .{
        .backend = registration.lease.backend,
        .sequence = 1,
        .nonce = @splat(10),
        .action = .{ .try_acquire = .{ .resource = try admitted.binding.table(), .mode = .{ .table = .row_share }, .scope = .{ .transaction = .{ .id = admitted.transaction, .subtransaction = 4 } } } },
    } };
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        try std.testing.expectEqual(ledger.Outcome.granted, (try data_participant.acquire(a, &txn, receiving, table, authorized)).outcome);
        try txn.commit();
    }
    var row = table;
    row.physical_key = "r";
    row.effect.sequence = 2;
    row.effect.nonce = @splat(11);
    row.effect.action.try_acquire.resource = try admitted.binding.row("r");
    row.effect.action.try_acquire.mode = .{ .row = .key_share };
    var receipt: ledger.Receipt = undefined;
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        receipt = try data_participant.acquire(a, &txn, receiving, row, authorized);
        try std.testing.expectEqual(ledger.Outcome.granted, receipt.outcome);
        try data_participant.finish(a, &txn, admitted, authorized);
        try txn.commit();
    }
    store.close();
    store = try DocStore.open(a, path, .{});
    {
        var read = try store.beginReadTxn();
        defer read.abort();
        try std.testing.expectEqualDeep(receipt, (try data_participant.observe(a, &read, receiving, row, authorized)).?);
        try std.testing.expect(!try ledger.accessBlocked(a, &read, admitted.binding.namespace, try admitted.binding.row("r"), .{ .row = .no_key_update }, null));
        try std.testing.expect(try ledger.accessBlocked(a, &read, admitted.binding.namespace, try admitted.binding.row("r"), .{ .row = .update }, null));
        try std.testing.expectError(error.Forbidden, data_participant.observe(a, &read, receiving, row, denied));
        var other_identity = receiving;
        other_identity.range_id += 1;
        try std.testing.expectError(error.IdentityNamespaceMismatch, data_participant.observe(a, &read, other_identity, row, authorized));
        var altered = row;
        altered.physical_key = "s";
        try std.testing.expectError(error.InvalidLockResource, data_participant.observe(a, &read, receiving, altered, authorized));
    }
    // A retained grant receipt is historical evidence, never permission for a
    // delayed new row effect to enter a completed participant command.
    var late = row;
    late.physical_key = "s";
    late.effect.sequence = 3;
    late.effect.nonce = @splat(12);
    late.effect.action.try_acquire.resource = try admitted.binding.row("s");
    late.effect.action.try_acquire.mode = .{ .row = .update };
    {
        var txn = try store.beginWriteTxn();
        defer txn.abort();
        try std.testing.expectError(error.LockEffectExpired, data_participant.acquire(a, &txn, receiving, late, authorized));
    }
    var next = admitted;
    next.serial = 2;
    next.parent_command.sequence = 57;
    next.parent_command.nonce = @splat(13);
    next.parent_command.executor = @splat(14);
    const next_authority: DataAuthority = .{ .approved = next };
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        try std.testing.expectEqual(scopes.Begin.admitted, try data_participant.admit(a, &txn, receiving, next, next_authority));
        try txn.commit();
    }
    {
        var txn = try store.beginWriteTxn();
        defer txn.abort();
        try std.testing.expectError(error.LockEffectChanged, data_participant.acquire(a, &txn, receiving, late, authorized));
    }
    late.admission = next;
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        try std.testing.expectEqual(ledger.Outcome.granted, (try data_participant.acquire(a, &txn, receiving, late, next_authority)).outcome);
        try data_participant.finish(a, &txn, next, next_authority);
        try txn.commit();
    }
    {
        var read = try store.beginReadTxn();
        defer read.abort();
        try std.testing.expectEqual(@as(u32, 3), (try ledger.readOwner(a, &read, admitted.binding.namespace, registration.lease.backend)).?.resources);
        try std.testing.expect(try ledger.accessBlocked(a, &read, admitted.binding.namespace, try admitted.binding.row("s"), .{ .row = .key_share }, null));
    }
    // The native 2PC BEGIN is durable before custody handoff. Fencing the
    // frontend before it delivers SEAL must not orphan already admitted work.
    const selected: scopes.DecisionRef = .{ .operation = @splat(20), .admission = @splat(21) };
    const external: native_transactions.ExternalDecisionOwner = .{ .metadata_incarnation = std.fmt.bytesToHex(admitted.binding.namespace.incarnation, .lower), .operation_id = selected.operation, .admission_digest = selected.admission, .expires_at_unix_ns = 10000 };
    {
        var manager = try native_transactions.TxnManager.init(a, &store);
        defer manager.deinit();
        try manager.initTransactionWithParticipantsCreatedAtRoleAndRetentionExtraBatch(admitted.transaction, 100, 1, &.{"data:901"}, false, true, .{ .external_decision_owner = external });
    }
    var resolving = next;
    resolving.purpose = .resolution;
    resolving.serial = 3;
    resolving.parent_command.sequence = 59;
    resolving.parent_command.nonce = @splat(22);
    resolving.parent_command.executor = @splat(23);
    const resolution: DataAuthority = .{ .approved = resolving, .decision = selected };
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        try scopes.fence(a, &txn, admitted.binding.namespace, registration.lease.backend, .close, resolution);
        try txn.commit();
    }
    store.close();
    store = try DocStore.open(a, path, .{});
    {
        var txn = try store.beginWriteTxn();
        defer txn.abort();
        var changed = selected;
        changed.admission[0] += 1;
        try std.testing.expectError(error.Forbidden, data_participant.prepareStep(a, &txn, receiving, resolving, changed, resolution));
        try std.testing.expect((try scopes.read(a, &txn, admitted.binding.namespace, registration.lease.backend)).?.transaction.?.decision == null);
    }
    var handoff_done = false;
    for (0..8) |_| {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        handoff_done = try data_participant.prepareStep(a, &txn, receiving, resolving, selected, resolution);
        try txn.commit();
        if (handoff_done) break;
    }
    try std.testing.expect(handoff_done);
    {
        var txn = try store.beginWriteTxn();
        defer txn.abort();
        try std.testing.expectError(error.Forbidden, data_participant.resolvePreparedStep(a, &txn, receiving, admitted.binding, selected, admitted.transaction, resolution));
    }
    const prepared_backend = try scopes.preparedBackend(admitted.binding.namespace, selected);
    {
        var read = try store.beginReadTxn();
        defer read.abort();
        const held = (try ledger.readOwner(a, &read, admitted.binding.namespace, prepared_backend)).?;
        try std.testing.expectEqual(@as(u32, 3), held.resources);
        try std.testing.expectEqual(.prepared, held.state);
        try std.testing.expect(try ledger.accessBlocked(a, &read, admitted.binding.namespace, try admitted.binding.row("s"), .{ .row = .key_share }, null));
        try std.testing.expect(!try ledger.accessBlocked(a, &read, admitted.binding.namespace, try admitted.binding.row("s"), .{ .row = .update }, .{ .backend = prepared_backend, .transaction = admitted.transaction }));
    }
    {
        var manager = try native_transactions.TxnManager.init(a, &store);
        defer manager.deinit();
        try manager.writeIntentsExtraBatch(admitted.transaction, &.{}, &.{}, .{ .external_decision_owner = external });
        try manager.activateExternalDecision(admitted.transaction, external, .{});
        _ = try manager.resolveIntentsWithExtraBatch(admitted.transaction, .committed, 200, .{ .external_decision_owner = external });
    }
    store.close();
    store = try DocStore.open(a, path, .{});
    var released = false;
    for (0..8) |_| {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        released = try data_participant.resolvePreparedStep(a, &txn, receiving, admitted.binding, selected, admitted.transaction, resolution);
        try txn.commit();
        if (released) break;
    }
    try std.testing.expect(released);
    var retired = false;
    for (0..8) |_| {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        retired = try scopes.closeStep(a, &txn, admitted.binding.namespace, registration.lease.backend, resolution);
        try txn.commit();
        if (retired) break;
    }
    try std.testing.expect(retired);
    {
        var read = try store.beginReadTxn();
        defer read.abort();
        try std.testing.expectEqual(@as(u32, 0), try scopes.databaseCount(&read, admitted.binding.namespace, admitted.source.lease.database));
        try std.testing.expect(!try ledger.accessBlocked(a, &read, admitted.binding.namespace, try admitted.binding.row("r"), .{ .row = .update }, null));
        try std.testing.expect(!try ledger.accessBlocked(a, &read, admitted.binding.namespace, try admitted.binding.row("s"), .{ .row = .update }, null));
    }
}

fn dataFixture() data_participant.Admission {
    return .{
        .purpose = .execution,
        .binding = .{ .namespace = .{ .home = .data, .group = 901, .incarnation = @splat(1) }, .identity = .{ .table_id = 7, .shard_id = 901, .range_id = 903 } },
        .source = .{ .group = 2188, .lease = .{ .backend = .{ .incarnation = @splat(1), .id = @splat(3), .fence = 1 }, .database = 1, .node = 9, .principal = @splat(4), .expires_at_ms = 1000 }, .connection = @splat('a'), .nonce = @splat(5), .standalone = false },
        .transaction = @splat(6),
        .generation = 1,
        .parent_command = .{ .sequence = 42, .nonce = @splat(7), .executor = @splat(8) },
        .serial = 1,
    };
}

fn dataGrant(admission: data_participant.Admission, serial: u64, key_value: []const u8, mode: locks.RowMode, subtransaction: u64) !data_participant.Acquisition {
    var nonce: [16]u8 = @splat(15);
    std.mem.writeInt(u64, nonce[8..16], serial, .little);
    return .{ .admission = admission, .physical_key = key_value, .effect = .{ .backend = (try admission.registration()).lease.backend, .sequence = serial, .nonce = nonce, .action = .{ .try_acquire = .{ .resource = try admission.binding.row(key_value), .mode = .{ .row = mode }, .scope = .{ .transaction = .{ .id = admission.transaction, .subtransaction = subtransaction } } } } } };
}
fn dataControl(admission: data_participant.Admission, serial: u64, control_value: ledger.Control) !data_participant.SavepointControl {
    var nonce: [16]u8 = @splat(16);
    std.mem.writeInt(u64, nonce[8..16], serial, .little);
    return .{ .admission = admission, .effect = .{ .backend = (try admission.registration()).lease.backend, .sequence = serial, .nonce = nonce, .action = .{ .control = control_value } } };
}
fn applyDataControl(store: *DocStore, request: data_participant.SavepointControl) !void {
    for (0..8) |_| {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        const receipt = try data_participant.controlStep(a, &txn, request.admission.binding.identity, request, DataAuthority{ .approved = request.admission });
        try txn.commit();
        if (receipt.outcome == .completed) return;
        try std.testing.expectEqual(ledger.Outcome.pending, receipt.outcome);
    }
    return error.UnboundedLockCleanup;
}

test "pessimistic locks native LSM data savepoint rollback promotion queued cancellation and fence before first admission" {
    const path = try pathFor("data-controls");
    defer a.free(path);
    defer cleanup(path);
    var store = try DocStore.open(a, path, .{});
    defer store.close();
    const admitted = dataFixture();
    const authorized: DataAuthority = .{ .approved = admitted };
    const receiving = admitted.binding.identity;
    const backend = (try admitted.registration()).lease.backend;
    const parent = try dataGrant(admitted, 1, "a", .key_share, 0);
    const child = try dataGrant(admitted, 2, "b", .update, 2);
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        _ = try data_participant.admit(a, &txn, receiving, admitted, authorized);
        try std.testing.expectEqual(ledger.Outcome.granted, (try data_participant.acquire(a, &txn, receiving, parent, authorized)).outcome);
        try std.testing.expectEqual(ledger.Outcome.granted, (try data_participant.acquire(a, &txn, receiving, child, authorized)).outcome);
        try txn.commit();
    }
    const rollback = try dataControl(admitted, 3, .{ .rollback = .{ .transaction = admitted.transaction, .first_aborted = 2 } });
    try applyDataControl(&store, rollback);
    // Lost control responses observe exactly the retained completion.
    try applyDataControl(&store, rollback);
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        try std.testing.expectEqual(@as(u32, 1), (try ledger.readOwner(a, &txn, admitted.binding.namespace, backend)).?.resources);
        try std.testing.expect(!try ledger.accessBlocked(a, &txn, admitted.binding.namespace, try admitted.binding.row("b"), .{ .row = .update }, null));
        try data_participant.finish(a, &txn, admitted, authorized);
        try txn.commit();
    }
    store.close();
    store = try DocStore.open(a, path, .{});
    var next = admitted;
    next.serial += 1;
    next.parent_command.sequence += 1;
    next.parent_command.nonce = @splat(18);
    const next_authority: DataAuthority = .{ .approved = next };
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        _ = try data_participant.admit(a, &txn, receiving, next, next_authority);
        const grant = try dataGrant(next, 4, "b", .share, 3);
        try std.testing.expectEqual(ledger.Outcome.granted, (try data_participant.acquire(a, &txn, receiving, grant, next_authority)).outcome);
        try txn.commit();
    }
    try applyDataControl(&store, try dataControl(next, 5, .{ .release_savepoint = .{ .transaction = next.transaction, .first_child = 3, .parent = 0 } }));
    try applyDataControl(&store, try dataControl(next, 6, .{ .rollback = .{ .transaction = next.transaction, .first_aborted = 3 } }));
    {
        var read = try store.beginReadTxn();
        defer read.abort();
        try std.testing.expectEqual(@as(u32, 2), (try ledger.readOwner(a, &read, next.binding.namespace, backend)).?.resources);
        try std.testing.expect(try ledger.accessBlocked(a, &read, next.binding.namespace, try next.binding.row("b"), .{ .row = .no_key_update }, null));
    }
    var competing = admitted;
    competing.source.lease.backend.id = @splat(19);
    competing.transaction = @splat(20);
    const competitor: DataAuthority = .{ .approved = competing };
    var queued = try dataGrant(competing, 1, "a", .update, 0);
    const queued_request = queued.effect.action.try_acquire;
    queued.effect.action = .{ .acquire = queued_request };
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        _ = try data_participant.admit(a, &txn, receiving, competing, competitor);
        try std.testing.expectEqual(ledger.Outcome.pending, (try data_participant.acquire(a, &txn, receiving, queued, competitor)).outcome);
        try txn.commit();
    }
    store.close();
    store = try DocStore.open(a, path, .{});
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        try std.testing.expectEqual(ledger.Outcome.cancelled, (try data_participant.cancel(a, &txn, receiving, queued, competitor)).outcome);
        try data_participant.finish(a, &txn, competing, competitor);
        try txn.commit();
    }
    // An enrolled but never contacted participant must be fenced even when
    // the first execution RPC is delayed beyond connection cleanup.
    var absent = admitted;
    absent.generation = 7;
    const absence: DataAuthority = .{ .approved = absent };
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        try data_participant.fence(a, &txn, receiving, absent, absence);
        try txn.commit();
    }
    store.close();
    store = try DocStore.open(a, path, .{});
    {
        var txn = try store.beginWriteTxn();
        defer txn.abort();
        const fenced = (try scopes.read(a, &txn, absent.binding.namespace, (try absent.registration()).lease.backend)).?;
        try std.testing.expectEqual(.fenced, fenced.state);
        try std.testing.expectEqual(@as(u64, 1), fenced.next_command);
        try std.testing.expectError(error.LockOwnerBusy, data_participant.admit(a, &txn, receiving, absent, absence));
    }
}

const DirectoryAuthority = struct {
    registration: scopes.Registration,
    command: scopes.Command,
    spec: scopes.Data.Spec,
    data: *DocStore,
    pub fn requireRegistration(self: @This(), _: anytype, _: ledger.Namespace, value: scopes.Registration) !void {
        if (!std.meta.eql(value, self.registration)) return error.Forbidden;
    }
    pub fn requireCommand(self: @This(), txn: anytype, namespace: ledger.Namespace, value: scopes.Record, command: scopes.Command) !void {
        try self.requireRegistration(txn, namespace, value.registration);
        if (!std.meta.eql(command, self.command)) return error.Forbidden;
    }
    pub fn requireTransaction(self: @This(), txn: anytype, namespace: ledger.Namespace, value: scopes.Record, id: [16]u8) !void {
        try self.requireCommand(txn, namespace, value, self.command);
        if (!std.mem.eql(u8, &id, &dataFixture().transaction)) return error.Forbidden;
    }
    pub fn requireDataEnrollment(self: @This(), txn: anytype, namespace: ledger.Namespace, value: scopes.Record, spec: scopes.Data.Spec) !void {
        try self.requireCommand(txn, namespace, value, self.command);
        // The fixture's authoritative route is independent of the request.
        if (!std.meta.eql(spec.route.route, self.spec.route.route) or spec.schema_version != self.spec.schema_version or !std.mem.eql(u8, spec.table_name, self.spec.table_name)) return error.Forbidden;
    }
    pub fn requireDataCompletion(self: @This(), _: anytype, _: ledger.Namespace, _: scopes.Record, participant: scopes.Data.Participant) !void {
        var read = try self.data.beginReadTxn();
        defer read.abort();
        const registration = try participant.admission.registration();
        const native = try scopes.read(a, &read, participant.admission.binding.namespace, registration.lease.backend) orelse return error.LockOwnerBusy;
        if (!std.meta.eql(native.completed, @as(?scopes.Command, try participant.admission.command())) or native.command != null) return error.LockOwnerBusy;
    }
    pub fn requireDataRetirement(self: @This(), _: anytype, _: ledger.Namespace, _: scopes.Record, participant: scopes.Data.Participant) !void {
        var read = try self.data.beginReadTxn();
        defer read.abort();
        const registration = try participant.admission.registration();
        const native = try scopes.read(a, &read, participant.admission.binding.namespace, registration.lease.backend) orelse return error.LockOwnerBusy;
        if (native.state != .retired or native.transaction != null) return error.LockOwnerBusy;
    }
};

test "pessimistic locks native LSM metadata enrollment fences FINISH and catalog retirement until independent native completion" {
    const metadata_path = try pathFor("locking-directory");
    defer a.free(metadata_path);
    defer cleanup(metadata_path);
    const data_path = try pathFor("locking-directory-data");
    defer a.free(data_path);
    defer cleanup(data_path);
    var metadata = try DocStore.open(a, metadata_path, .{});
    defer metadata.close();
    var data = try DocStore.open(a, data_path, .{});
    defer data.close();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    var selected = dataFixture();
    selected.parent_command.sequence = 1;
    const metadata_ns: ledger.Namespace = .{ .home = .metadata, .group = selected.source.group, .incarnation = selected.binding.namespace.incarnation };
    const backend = selected.source.lease.backend;
    const spec: scopes.Data.Spec = .{ .table_name = "physical", .schema_version = 0, .route = .{
        .metadata_group_id = metadata_ns.group,
        .metadata_incarnation = std.fmt.bytesToHex(metadata_ns.incarnation, .lower),
        .catalog_revision = 10,
        .table_id = selected.binding.identity.table_id,
        .topology_epoch = 9,
        .route = .{ .group_id = selected.binding.namespace.group, .range_id = selected.binding.identity.range_id, .identity_namespace = .{ .table_id = selected.binding.identity.table_id, .shard_id = selected.binding.identity.shard_id, .range_id = selected.binding.identity.range_id } },
    } };
    var authority: DirectoryAuthority = .{ .registration = selected.source, .command = selected.parent_command, .spec = spec, .data = &data };
    var first: scopes.Data.Participant = undefined;
    {
        var txn = try metadata.beginWriteTxn();
        errdefer txn.abort();
        try scopes.register(scratch, &txn, metadata_ns, selected.source, authority);
        _ = try scopes.begin(scratch, &txn, metadata_ns, backend, selected.parent_command, authority);
        first = try scopes.Data.enroll(scratch, &txn, metadata_ns, backend, selected.parent_command, selected.transaction, spec, authority);
        try std.testing.expectEqual(@as(u64, 1), first.admission.generation);
        try std.testing.expectEqual(@as(u32, 1), try scopes.Data.tableCount(&txn, metadata_ns, spec.route.table_id));
        try std.testing.expectError(error.LockOwnerBusy, scopes.finish(scratch, &txn, metadata_ns, backend, selected.parent_command, authority));
        try std.testing.expectError(error.LockOwnerBusy, scopes.Data.acknowledge(scratch, &txn, metadata_ns, backend, 1, selected.parent_command, authority));
        try std.testing.expectError(error.LockOwnerBusy, scopes.Data.retire(scratch, &txn, metadata_ns, backend, 1, authority));
        try txn.commit();
    }
    metadata.close();
    metadata = try DocStore.open(a, metadata_path, .{});
    // Duplicate enrollment consumes neither another slot nor a generation.
    {
        var txn = try metadata.beginWriteTxn();
        errdefer txn.abort();
        const replay = try scopes.Data.enroll(scratch, &txn, metadata_ns, backend, selected.parent_command, selected.transaction, spec, authority);
        try std.testing.expectEqualDeep(first, replay);
        try std.testing.expectEqual(@as(usize, 1), (try scopes.read(scratch, &txn, metadata_ns, backend)).?.data.?.count());
        var changed = spec;
        changed.schema_version = 10;
        try std.testing.expectError(error.Forbidden, scopes.Data.enroll(scratch, &txn, metadata_ns, backend, selected.parent_command, selected.transaction, changed, authority));
        try txn.commit();
    }
    {
        var txn = try data.beginWriteTxn();
        errdefer txn.abort();
        const control = @import("pessimistic_lock_control.zig");
        var metadata_read = try metadata.beginReadTxn();
        defer metadata_read.abort();
        const checked: control.Checked = .{ .source = (try scopes.read(scratch, &metadata_read, metadata_ns, backend)).?, .participant = first, .admitted_at_ms = 1 };
        const begin_command: control.Command = .{ .admission = first.admission, .checked = checked, .operation = .begin };
        try control.apply(scratch, &txn, selected.binding.identity, selected.binding.namespace.group, .document, begin_command);
        var finish_command = begin_command;
        finish_command.operation = .finish;
        try control.apply(scratch, &txn, selected.binding.identity, selected.binding.namespace.group, .document, finish_command);
        const native_snapshot = try control.snapshot(scratch, &txn, selected.binding.identity, .{ .admission = first.admission });
        try std.testing.expectEqualDeep(try first.admission.command(), native_snapshot.scope.?.completed.?);
        try std.testing.expectEqual(@as(?scopes.Command, null), native_snapshot.scope.?.command);
        try txn.commit();
    }
    {
        var txn = try metadata.beginWriteTxn();
        errdefer txn.abort();
        try scopes.Data.acknowledge(scratch, &txn, metadata_ns, backend, 1, selected.parent_command, authority);
        // Exact repeated ACKs do not underflow the shared open counter.
        try scopes.Data.acknowledge(scratch, &txn, metadata_ns, backend, 1, selected.parent_command, authority);
        const replay = try scopes.Data.enroll(scratch, &txn, metadata_ns, backend, selected.parent_command, selected.transaction, spec, authority);
        try std.testing.expect(!replay.command_open);
        try scopes.finish(scratch, &txn, metadata_ns, backend, selected.parent_command, authority);
        try txn.commit();
    }
    selected.parent_command.sequence += 1;
    selected.parent_command.nonce = @splat(24);
    authority.command = selected.parent_command;
    {
        var txn = try metadata.beginWriteTxn();
        errdefer txn.abort();
        _ = try scopes.begin(scratch, &txn, metadata_ns, backend, selected.parent_command, authority);
        const second = try scopes.Data.enroll(scratch, &txn, metadata_ns, backend, selected.parent_command, selected.transaction, spec, authority);
        try std.testing.expectEqual(first.admission.generation, second.admission.generation);
        try std.testing.expectEqual(@as(u64, 2), second.admission.serial);
        try std.testing.expectError(error.LockEffectChanged, scopes.Data.acknowledge(scratch, &txn, metadata_ns, backend, 1, first.admission.parent_command, authority));
        try std.testing.expectError(error.LockOwnerBusy, scopes.Data.acknowledge(scratch, &txn, metadata_ns, backend, 1, selected.parent_command, authority));
        try std.testing.expectEqual(@as(u8, 1), (try scopes.read(scratch, &txn, metadata_ns, backend)).?.data.?.open);
        try std.testing.expectEqual(@as(u32, 1), try scopes.Data.tableCount(&txn, metadata_ns, spec.route.table_id));
        try txn.commit();
    }
}

const AccessCore = struct {
    store: *DocStore,
    identity_namespace: @import("db/doc_identity_namespace.zig").Namespace,
    schema: ?@import("db/schema_registry.zig").SchemaView = null,
    pub fn acquireSchemaView(self: *@This()) ?@import("db/schema_registry.zig").SchemaView {
        return if (self.schema) |view| view.clone() else null;
    }
    pub fn acquireSchemaVersionView(self: *@This(), version: u32) !?@import("db/schema_registry.zig").SchemaView {
        if (self.schema) |view| if (view.version() == version) return view.clone();
        return null;
    }
};

test "pessimistic locks native LSM mutation guard uses typed unique equality and independent native owner for self exclusion" {
    const schema_api = @import("../schema/mod.zig");
    const schema_registry = @import("db/schema_registry.zig");
    const mapper = @import("db/document_mapper.zig");
    const access = @import("db/pessimistic_access.zig");
    const path = try pathFor("typed-access");
    defer a.free(path);
    defer cleanup(path);
    var store = try DocStore.open(a, path, .{});
    defer store.close();
    const validator = try schema_api.CompiledTableValidator.init(a,
        \\{"version":1,"storage_mode":"relational","default_type":"row","unique_constraints":[{"name":"pk","columns":["id"]}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"},"value":{"type":"integer"}},"additionalProperties":false}}}}
    );
    const runtime_schema = try schema_api.deriveRuntimeTableSchema(a, validator.schema);
    const epoch = try schema_registry.Epoch.createOwnedValidated(a, runtime_schema, validator);
    var schema: schema_registry.SchemaView = .{ .epoch = epoch };
    defer schema.release();
    const admitted = dataFixture();
    const authorized: DataAuthority = .{ .approved = admitted };
    var core: AccessCore = .{ .store = &store, .identity_namespace = admitted.binding.identity, .schema = schema };
    var initial = try mapper.PreparedRelationalWrite.init(a, "r", "{\"id\":1,\"value\":10}", schema.validator(), schema.tableSchema().*, schema.physicalLayout());
    defer initial.deinit(a);
    try initial.finalizeMetadata(1);
    var unchanged_key = try mapper.PreparedRelationalWrite.init(a, "r", "{\"id\":1,\"value\":11}", schema.validator(), schema.tableSchema().*, schema.physicalLayout());
    defer unchanged_key.deinit(a);
    var changed_key = try mapper.PreparedRelationalWrite.init(a, "r", "{\"id\":2,\"value\":10}", schema.validator(), schema.tableSchema().*, schema.physicalLayout());
    defer changed_key.deinit(a);
    const row_key = try @import("db/relational_store.zig").keyAlloc(a, "r");
    defer a.free(row_key);
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        _ = try data_participant.admit(a, &txn, core.identity_namespace, admitted, authorized);
        const grant = try dataGrant(admitted, 1, "r", .key_share, 0);
        try std.testing.expectEqual(ledger.Outcome.granted, (try data_participant.acquire(a, &txn, core.identity_namespace, grant, authorized)).outcome);
        try txn.put(row_key, initial.packed_row);
        try txn.commit();
    }
    {
        var guard = try access.Guard.init(a, &core, null);
        defer guard.deinit();
        try guard.table(.row_exclusive);
        try guard.write(&core, "r", unchanged_key.packed_row);
        try std.testing.expectError(error.IntentConflict, guard.write(&core, "r", changed_key.packed_row));
        try std.testing.expectError(error.IntentConflict, guard.write(&core, "r", null));
        // A caller-supplied transaction ID without a real native owner cannot
        // exclude any grant, even when it names the lock holder's transaction.
        var unowned = try access.Guard.init(a, &core, admitted.transaction);
        defer unowned.deinit();
        try std.testing.expect(unowned.owner == null);
        try std.testing.expectError(error.IntentConflict, unowned.row("r", .update));
        try guard.write(&core, "\x00\x00internal", null);
    }
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        const registration = try admitted.registration();
        const table: data_participant.Acquisition = .{ .admission = admitted, .effect = .{ .backend = registration.lease.backend, .sequence = 2, .nonce = @splat(28), .action = .{ .try_acquire = .{ .resource = try admitted.binding.table(), .mode = .{ .table = .access_exclusive }, .scope = .{ .transaction = .{ .id = admitted.transaction, .subtransaction = 0 } } } } } };
        try std.testing.expectEqual(ledger.Outcome.granted, (try data_participant.acquire(a, &txn, core.identity_namespace, table, authorized)).outcome);
        try txn.commit();
    }
    store.close();
    store = try DocStore.open(a, path, .{});
    {
        var guard = try access.Guard.init(a, &core, null);
        defer guard.deinit();
        try std.testing.expectError(error.IntentConflict, guard.table(.row_exclusive));
        try std.testing.expectError(error.IntentConflict, guard.table(.access_share));
        var changed = core;
        changed.identity_namespace.table_id += 1;
        try std.testing.expectError(error.IdentityNamespaceMismatch, access.Guard.init(a, &changed, null));
    }
}

test "pessimistic locks native LSM compare and lock preserves false outcomes current row witnesses and bounded queued refresh across restart" {
    const row_locks = @import("pessimistic_lock_row.zig");
    const internal = @import("internal_keys.zig");
    const path = try pathFor("compare-and-lock");
    defer a.free(path);
    defer cleanup(path);
    var store = try DocStore.open(a, path, .{});
    defer store.close();
    const admitted = dataFixture();
    const receiving = admitted.binding.identity;
    const authorized: DataAuthority = .{ .approved = admitted };
    const primary = try internal.documentKeyAlloc(a, "r");
    defer a.free(primary);
    const timestamp = try internal.ttlKeyAlloc(a, "r");
    defer a.free(timestamp);
    var stamp: [8]u8 = undefined;
    std.mem.writeInt(u64, &stamp, 11, .little);
    var initial: row_locks.Witness = undefined;
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        _ = try data_participant.admit(a, &txn, receiving, admitted, authorized);
        try txn.put(primary, "{\"id\":1}");
        try txn.put(timestamp, &stamp);
        initial = (try row_locks.current(a, &txn, .document, "r")).?;
        try txn.commit();
    }
    var grant = try dataGrant(admitted, 1, "r", .key_share, 4);
    const initial_action = grant.effect.action.try_acquire;
    grant.effect.action = .{ .acquire = initial_action };
    const request: row_locks.Request = .{ .acquisition = grant, .witness = initial, .wait = .try_lock };
    var known: row_locks.Result = undefined;
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        known = try row_locks.acquire(a, &txn, receiving, .document, request, authorized);
        try std.testing.expectEqual(.matched, known.status);
        try std.testing.expectEqual(.granted, known.receipt.outcome);
        try txn.commit();
    }
    store.close();
    store = try DocStore.open(a, path, .{});
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        try std.testing.expectEqualDeep(known, (try row_locks.observe(a, &txn, receiving, request, authorized)).?);
        var changed = request;
        changed.witness.version += 1;
        try std.testing.expectError(error.LockEffectChanged, row_locks.observe(a, &txn, receiving, changed, authorized));
        std.mem.writeInt(u64, &stamp, 12, .little);
        try txn.put(primary, "{\"id\":1,\"value\":2}");
        try txn.put(timestamp, &stamp);
        // An already granted occurrence retains its exact captured image,
        // even after a compatible non-key update changes the primary bytes.
        try std.testing.expectEqualDeep(known, try row_locks.acquire(a, &txn, receiving, .document, request, authorized));
        try txn.commit();
    }
    var changed_request = request;
    changed_request.acquisition.effect.sequence = 2;
    changed_request.acquisition.effect.nonce = @splat(32);
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        const rejected = try row_locks.acquire(a, &txn, receiving, .document, changed_request, authorized);
        try std.testing.expectEqual(.changed, rejected.status);
        try std.testing.expectEqual(.cancelled, rejected.receipt.outcome);
        const owner = (try ledger.readOwner(a, &txn, admitted.binding.namespace, grant.effect.backend)).?;
        try std.testing.expectEqual(@as(u32, 1), owner.resources);
        try std.testing.expectEqual(@as(u64, 3), owner.next_effect);
        try txn.commit();
    }
    var competing = admitted;
    competing.source.lease.backend.id = @splat(33);
    competing.transaction = @splat(34);
    const competitor: DataAuthority = .{ .approved = competing };
    var next = try dataGrant(competing, 1, "r", .update, 4);
    const next_action = next.effect.action.try_acquire;
    next.effect.action = .{ .acquire = next_action };
    var contended: row_locks.Request = .{ .acquisition = next, .witness = initial, .wait = .try_lock };
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        _ = try data_participant.admit(a, &txn, receiving, competing, competitor);
        contended.witness = (try row_locks.current(a, &txn, .document, "r")).?;
        const rejected = try row_locks.acquire(a, &txn, receiving, .document, contended, competitor);
        try std.testing.expectEqual(.contended, rejected.status);
        const owner = (try ledger.readOwner(a, &txn, competing.binding.namespace, next.effect.backend)).?;
        try std.testing.expectEqual(@as(u32, 0), owner.resources);
        try std.testing.expect(owner.waiting == null);
        try txn.commit();
    }
    try applyDataControl(&store, try dataControl(admitted, 3, .{ .rollback = .{ .transaction = admitted.transaction, .first_aborted = 4 } }));
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        // Unblocking does not turn a lost NOWAIT/SKIP outcome into a grant.
        try std.testing.expectEqual(.contended, (try row_locks.acquire(a, &txn, receiving, .document, contended, competitor)).status);
        var fresh = contended;
        fresh.acquisition.effect.sequence = 2;
        fresh.acquisition.effect.nonce = @splat(35);
        try std.testing.expectEqual(.granted, (try row_locks.acquire(a, &txn, receiving, .document, fresh, competitor)).receipt.outcome);
        try txn.commit();
    }
    var absent = request;
    absent.acquisition.physical_key = "s";
    absent.acquisition.effect.sequence = 4;
    absent.acquisition.effect.nonce = @splat(36);
    absent.acquisition.effect.action.acquire.resource = try admitted.binding.row("s");
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        const missing = try row_locks.acquire(a, &txn, receiving, .document, absent, authorized);
        try std.testing.expectEqual(.absent, missing.status);
        try std.testing.expectEqual(@as(u32, 0), (try ledger.readOwner(a, &txn, admitted.binding.namespace, grant.effect.backend)).?.resources);
        try txn.commit();
    }
    var queued = request;
    queued.acquisition.effect.sequence = 5;
    queued.acquisition.effect.nonce = @splat(37);
    queued.witness = contended.witness;
    queued.wait = .block;
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        try std.testing.expectEqual(.pending, (try row_locks.acquire(a, &txn, receiving, .document, queued, authorized)).receipt.outcome);
        try txn.commit();
    }
    store.close();
    store = try DocStore.open(a, path, .{});
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        std.mem.writeInt(u64, &stamp, 13, .little);
        try txn.put(primary, "{\"id\":1,\"value\":3}");
        try txn.put(timestamp, &stamp);
        try txn.commit();
    }
    try applyDataControl(&store, try dataControl(competing, 3, .{ .rollback = .{ .transaction = competing.transaction, .first_aborted = 4 } }));
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        const rejected = try row_locks.acquire(a, &txn, receiving, .document, queued, authorized);
        try std.testing.expectEqual(.changed, rejected.status);
        try std.testing.expectEqual(.cancelled, rejected.receipt.outcome);
        try std.testing.expectEqual(@as(u32, 0), (try ledger.readOwner(a, &txn, admitted.binding.namespace, grant.effect.backend)).?.resources);
        var refreshed = queued;
        refreshed.acquisition.effect.sequence = 6;
        refreshed.acquisition.effect.nonce = @splat(38);
        refreshed.witness = (try row_locks.current(a, &txn, .document, "r")).?;
        try std.testing.expectEqual(.granted, (try row_locks.acquire(a, &txn, receiving, .document, refreshed, authorized)).receipt.outcome);
        try txn.commit();
    }
}
