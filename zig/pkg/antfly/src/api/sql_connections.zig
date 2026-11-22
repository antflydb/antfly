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

//! Durable idle HTTP SQL connections. The native store transaction is the
//! linearization point for connection generation, overlay and prepared-plan
//! retirement. Transaction IDs remain owned by SessionRegistry, not by this
//! directory.
const std = @import("std");
const transactions = @import("transactions.zig");
const record = @import("sql_connection_record.zig");
const prepared = @import("sql_prepared.zig");
const settings = @import("antfly_local_sources").sql_setting_catalog;

const directory_key = "\x00sql-connection-directory-v1";
pub const max_connections = 128;
pub const max_overlay_entries = record.max_overlay_entries;
const Entry = struct { id: [32]u8, expires_at_ms: u64 };
const Directory = struct { entries: []const Entry = &.{} };

pub const Owned = std.json.Parsed(record.Record);

pub fn load(store: *transactions.DurableSessionStore, alloc: std.mem.Allocator, id: [32]u8, principal: []const u8, owner_node_id: u64, now_ms: u64) !Owned {
    return switch (store.backend) {
        .docstore => |backend| blk: {
            var txn = try backend.beginReadTxn();
            defer txn.abort();
            break :blk try loadTxn(&txn, alloc, id, principal, owner_node_id, now_ms);
        },
        .runtime => |backend| blk: {
            var txn = try backend.beginRead();
            defer txn.abort();
            break :blk try loadTxn(&txn, alloc, id, principal, owner_node_id, now_ms);
        },
    };
}

/// Repair only outcomes that the durable transaction registry can prove. A
/// beginning ID has never escaped in an HTTP response. Retire an existing
/// unexecuted session and its beginning fence in one transaction. An uncertain commit remains fenced until a terminal
/// decision is visible; a missing record is never interpreted as an abort.
pub fn loadReconciled(
    store: *transactions.DurableSessionStore,
    registry: *transactions.SessionRegistry,
    alloc: std.mem.Allocator,
    id: [32]u8,
    principal: []const u8,
    owner_node_id: u64,
    now_ms: u64,
) !Owned {
    var owned = try load(store, alloc, id, principal, owner_node_id, now_ms);
    errdefer owned.deinit();
    if (!try reconcile(store, registry, owned.value, now_ms, null)) return owned;
    const result = try load(store, alloc, id, principal, owner_node_id, now_ms);
    owned.deinit();
    return result;
}

fn reconcile(store: *transactions.DurableSessionStore, registry: *transactions.SessionRegistry, connection: record.Record, authority_ms: u64, expiry_cutoff_ms: ?u64) !bool {
    const id = connection.id;
    const owner_node_id = connection.owner_node_id;
    const principal = connection.principal;
    if (expiry_cutoff_ms) |cutoff| if (connection.expires_at_ms > cutoff) return false;
    const txn_id = connection.active_txn orelse return false;
    const state = connection.state;
    if (state == .idle) return false;
    const session = try store.connectionLifecycle(txn_id);
    if (session) |value| {
        if (!std.meta.eql(value.connection_id, @as(?[32]u8, id))) return error.InvalidSqlConnection;
    }
    if (state == .beginning) {
        if (session) |value| {
            if ((expiry_cutoff_ms == null and value.version.owner != owner_node_id) or value.version.executing or value.terminal) return error.SqlConnectionBusy;
            registry.rollbackConnectionBeforeExecution(txn_id, id, value.version, if (expiry_cutoff_ms) |cutoff| .{ .expired = .{ .state = .beginning, .cutoff_ms = cutoff } } else .beginning) catch |err| switch (err) {
                error.SessionLeaseLost, error.SqlTransactionOutcomeUnknown => return false,
                else => return err,
            };
        } else {
            _ = mutate(store, .{ .abort_begin = .{ .id = id, .txn_id = txn_id } }, principal, owner_node_id, authority_ms) catch |err| switch (err) {
                error.SqlConnectionBusy => return false,
                else => return err,
            };
        }
    } else if (session) |value| {
        if (!value.terminal) {
            const cutoff = expiry_cutoff_ms orelse return false;
            if ((state != .active and state != .uncertain) or value.version.executing) return false;
            registry.rollbackConnectionBeforeExecution(txn_id, id, value.version, .{ .expired = .{ .state = state, .cutoff_ms = cutoff } }) catch |err| switch (err) {
                error.SessionLeaseLost, error.SqlTransactionOutcomeUnknown => return false,
                else => return err,
            };
        } else _ = mutate(store, .{ .terminal = .{ .id = id, .txn_id = txn_id } }, principal, owner_node_id, authority_ms) catch |err| switch (err) {
            error.SqlConnectionBusy => return false,
            else => return err,
        };
    } else return false;
    return true;
}

/// Admission-time bounded GC. The directory is capped at 128 entries, and
/// only expired non-idle entries need an authoritative transaction lookup.
/// `0` is an internal expiry-bypass cut: it never authorizes a public use of
/// the connection, and every mutation still matches principal, owner and ID.
pub fn reconcileExpired(
    store: *transactions.DurableSessionStore,
    registry: *transactions.SessionRegistry,
    alloc: std.mem.Allocator,
    now_ms: u64,
) !void {
    const entries = switch (store.backend) {
        .docstore => |backend| blk: {
            var txn = try backend.beginReadTxn();
            defer txn.abort();
            break :blk try copyDirectoryTxn(&txn, alloc);
        },
        .runtime => |backend| blk: {
            var txn = try backend.beginRead();
            defer txn.abort();
            break :blk try copyDirectoryTxn(&txn, alloc);
        },
    };
    defer alloc.free(entries);
    var expired: usize = 0;
    for (entries) |entry| {
        if (entry.expires_at_ms > now_ms) continue;
        entries[expired] = entry;
        expired += 1;
    }
    if (expired == 0) return;
    const batch = @min(expired, 8);
    const start = registry.connection_sweep_offset.fetchAdd(batch, .monotonic) % expired;
    for (0..batch) |offset| {
        const entry = entries[(start + offset) % expired];
        const observed: ?Owned = switch (store.backend) {
            .docstore => |backend| blk: {
                var txn = try backend.beginReadTxn();
                defer txn.abort();
                break :blk record.loadTxn(&txn, alloc, &entry.id) catch |err| switch (err) {
                    error.SqlConnectionNotFound => null,
                    else => return err,
                };
            },
            .runtime => |backend| blk: {
                var txn = try backend.beginRead();
                defer txn.abort();
                break :blk record.loadTxn(&txn, alloc, &entry.id) catch |err| switch (err) {
                    error.SqlConnectionNotFound => null,
                    else => return err,
                };
            },
        };
        var prior = observed orelse {
            _ = mutate(store, .{ .expire = .{ .id = entry.id, .cutoff_ms = now_ms } }, "", 0, 0) catch |err| switch (err) {
                error.SqlConnectionBusy, error.SqlConnectionChanged, error.SqlConnectionNotFound => continue,
                else => return err,
            };
            return;
        };
        defer prior.deinit();
        if (prior.value.expires_at_ms > now_ms) continue;
        if (prior.value.state != .idle) {
            const changed = reconcile(store, registry, prior.value, 0, now_ms) catch |err| switch (err) {
                error.SqlConnectionBusy, error.SqlConnectionChanged, error.SqlConnectionNotFound => continue,
                else => return err,
            };
            if (!changed) continue;
        }
        _ = mutate(store, .{ .expire = .{ .id = entry.id, .cutoff_ms = now_ms } }, prior.value.principal, prior.value.owner_node_id, 0) catch |err| switch (err) {
            error.SqlConnectionBusy, error.SqlConnectionChanged, error.SqlConnectionNotFound => continue,
            else => return err,
        };
        return; // One free slot is sufficient for this admission.

    }
}

fn copyDirectoryTxn(txn: anytype, alloc: std.mem.Allocator) ![]Entry {
    const raw = txn.get(directory_key) catch |err| switch (err) {
        error.NotFound => return alloc.alloc(Entry, 0),
        else => return err,
    };
    if (raw.len > 64 << 10) return error.InvalidSqlConnection;
    var parsed = try std.json.parseFromSlice(Directory, alloc, raw, .{ .allocate = .alloc_always });
    defer parsed.deinit();
    if (parsed.value.entries.len > max_connections) return error.InvalidSqlConnection;
    return alloc.dupe(Entry, parsed.value.entries);
}

fn loadTxn(txn: anytype, alloc: std.mem.Allocator, id: [32]u8, principal: []const u8, owner_node_id: u64, now_ms: u64) !Owned {
    var value = try record.loadTxn(txn, alloc, &id);
    errdefer value.deinit();
    try value.value.requireOwner(&id, principal, owner_node_id, now_ms);
    return value;
}

pub const Action = union(enum) {
    create: record.Record,
    discard: [32]u8,
    close: [32]u8,
    expire: struct { id: [32]u8, cutoff_ms: u64 },
    begin: struct { id: [32]u8, expected_revision: u64, txn_id: [16]u8 },
    abort_begin: struct { id: [32]u8, txn_id: [16]u8 },
    bind: struct { id: [32]u8, txn_id: [16]u8 },
    uncertain: struct { id: [32]u8, guard: transactions.Mutation },
    terminal: struct { id: [32]u8, txn_id: [16]u8 },
    overlay: struct { id: [32]u8, expected_revision: u64, entries: []const settings.OverlayEntry },
};

pub fn create(store: *transactions.DurableSessionStore, registry: *transactions.SessionRegistry, value: record.Record, now_ms: u64) !void {
    const action: Action = .{ .create = value };
    _ = mutate(store, action, value.principal, value.owner_node_id, now_ms) catch |err| {
        if (err != error.SqlWriteCapacityUnavailable) return err;
        try reconcileExpired(store, registry, store.alloc, now_ms);
        _ = try mutate(store, action, value.principal, value.owner_node_id, now_ms);
        return;
    };
}

pub fn mutate(store: *transactions.DurableSessionStore, action: Action, principal: []const u8, owner_node_id: u64, now_ms: u64) !usize {
    const mutation = store.mutationMutex();
    @import("antfly_platform").sync.lockYielding(mutation);
    defer mutation.unlock();
    if (store.fail_writes_for_test) return error.InjectedSessionStoreFailure;
    return switch (store.backend) {
        .docstore => |backend| blk: {
            var txn = try backend.beginWriteTxn();
            errdefer txn.abort();
            const result = try mutateTxn(store, &txn, store.alloc, action, principal, owner_node_id, now_ms);
            try txn.commit();
            break :blk result;
        },
        .runtime => |backend| blk: {
            var txn = try backend.beginWrite();
            errdefer txn.abort();
            const result = try mutateTxn(store, &txn, store.alloc, action, principal, owner_node_id, now_ms);
            try txn.commit();
            break :blk result;
        },
    };
}

fn removeDirectoryEntry(txn: anytype, alloc: std.mem.Allocator, id: [32]u8, cutoff: ?u64) !void {
    const raw = txn.get(directory_key) catch |err| switch (err) {
        error.NotFound => return alloc.alloc(Entry, 0),
        else => return err,
    };
    var directory = try std.json.parseFromSlice(Directory, alloc, raw, .{ .allocate = .alloc_always });
    defer directory.deinit();
    if (directory.value.entries.len > max_connections) return error.InvalidSqlConnection;
    var retained: [max_connections]Entry = undefined;
    var count: usize = 0;
    var found = false;
    for (directory.value.entries) |entry| {
        if (std.mem.eql(u8, &entry.id, &id)) {
            if (cutoff) |ms| if (entry.expires_at_ms > ms) return error.SqlConnectionChanged;
            found = true;
            continue;
        }
        retained[count] = entry;
        count += 1;
    }
    if (!found) {
        if (cutoff != null) return;
        return error.InvalidSqlConnection;
    }
    const encoded = try std.json.Stringify.valueAlloc(alloc, Directory{ .entries = retained[0..count] }, .{});
    defer alloc.free(encoded);
    try txn.put(directory_key, encoded);
}

fn mutateTxn(store: *transactions.DurableSessionStore, txn: anytype, alloc: std.mem.Allocator, action: Action, principal: []const u8, owner_node_id: u64, now_ms: u64) !usize {
    if (action == .create) {
        const value = action.create;
        try value.validate();
        if (value.state != .idle or value.generation != 1 or value.revision != 1 or value.active_txn != null or value.overlay.len != 0 or
            value.owner_node_id != owner_node_id or !std.mem.eql(u8, value.principal, principal) or
            value.expires_at_ms <= now_ms or value.expires_at_ms - now_ms > record.ttl_ms)
            return error.InvalidSqlConnection;
        var existing = record.loadTxn(txn, alloc, &value.id) catch |err| switch (err) {
            error.SqlConnectionNotFound => null,
            else => return err,
        };
        if (existing) |*prior| {
            prior.deinit();
            return error.SqlConnectionAlreadyExists;
        }
        const raw = txn.get(directory_key) catch |err| switch (err) {
            error.NotFound => "{}",
            else => return err,
        };
        if (raw.len > 64 << 10) return error.SqlProgramLimitExceeded;
        var directory = try std.json.parseFromSlice(Directory, alloc, raw, .{ .allocate = .alloc_always });
        defer directory.deinit();
        if (directory.value.entries.len > max_connections) return error.InvalidSqlConnection;
        const prior = directory.value.entries;
        for (prior) |entry| if (std.mem.eql(u8, &entry.id, &value.id)) return error.SqlConnectionAlreadyExists;
        if (prior.len == max_connections) return error.SqlWriteCapacityUnavailable;
        var entries: [max_connections]Entry = undefined;
        @memcpy(entries[0..prior.len], prior);
        var count = prior.len;
        entries[count] = .{ .id = value.id, .expires_at_ms = value.expires_at_ms };
        count += 1;
        const encoded = try std.json.Stringify.valueAlloc(alloc, Directory{ .entries = entries[0..count] }, .{});
        defer alloc.free(encoded);
        try txn.put(directory_key, encoded);
        try record.putTxn(txn, alloc, value);
        return 0;
    }
    const id: [32]u8 = switch (action) {
        .create => unreachable,
        .discard, .close => |value| value,
        .expire => |value| value.id,
        .begin => |value| value.id,
        .abort_begin => |value| value.id,
        .bind => |value| value.id,
        .uncertain => |value| value.id,
        .terminal => |value| value.id,
        .overlay => |value| value.id,
    };
    var owned = loadTxn(txn, alloc, id, principal, owner_node_id, now_ms) catch |err| {
        if (err != error.SqlConnectionNotFound or action != .expire) return err;
        // A missing legacy record may still occupy a directory slot. Confirm
        // absence separately because requireOwner also masks principal mismatch.
        const key = try record.key(&id);
        _ = txn.get(&key) catch |get_err| {
            if (get_err != error.NotFound) return get_err;
            try removeDirectoryEntry(txn, alloc, id, action.expire.cutoff_ms);
            return 0;
        };
        return err;
    };
    defer owned.deinit();
    var next = owned.value;
    return switch (action) {
        .create => unreachable,
        .discard, .close, .expire => blk: {
            if (action == .expire and next.expires_at_ms > action.expire.cutoff_ms) return error.SqlConnectionChanged;
            if (next.state != .idle) return error.SqlConnectionBusy;
            const removed = try prepared.discardConnectionTxn(txn, alloc, id);
            if (action == .discard) {
                if (next.generation == std.math.maxInt(u64)) return error.SqlProgramLimitExceeded;
                if (next.revision == std.math.maxInt(u64)) return error.SqlProgramLimitExceeded;
                next.generation += 1;
                next.revision += 1;
                next.overlay = &.{};
                try record.putTxn(txn, alloc, next);
            } else {
                const connection_key = try record.key(&id);
                try txn.delete(&connection_key);
                try removeDirectoryEntry(txn, alloc, id, null);
            }
            break :blk removed;
        },
        .begin => |beginning| blk: {
            // The statement overlay was captured before this transaction.
            // A concurrent SET, RESET or DISCARD must invalidate that view.
            if (next.state != .idle) return error.SqlConnectionBusy;
            if (next.revision != beginning.expected_revision) return error.SqlConnectionChanged;
            next.state = .beginning;
            next.active_txn = beginning.txn_id;
            try record.putTxn(txn, alloc, next);
            break :blk 0;
        },
        .abort_begin => |binding| blk: {
            if (next.state != .beginning or !std.mem.eql(u8, &next.active_txn.?, &binding.txn_id)) return error.SqlConnectionBusy;
            if (try store.loadSessionTxn(txn, binding.txn_id)) |present| {
                var session = present;
                defer session.deinit(store.alloc);
                return error.SqlConnectionBusy;
            }
            next.state = .idle;
            next.active_txn = null;
            try record.putTxn(txn, alloc, next);
            break :blk 0;
        },
        .bind => |binding| blk: {
            if (next.state != .beginning or !std.mem.eql(u8, &next.active_txn.?, &binding.txn_id)) return error.SqlConnectionBusy;
            next.state = .active;
            try record.putTxn(txn, alloc, next);
            break :blk 0;
        },
        .uncertain => |binding| blk: {
            if (next.state != .active or !std.mem.eql(u8, &next.active_txn.?, &binding.guard.id)) return error.SqlConnectionBusy;
            if (binding.guard.owner != owner_node_id) return error.SessionLeaseLost;
            try binding.guard.checkLeaseTxn(alloc, txn);
            next.state = .uncertain;
            try record.putTxn(txn, alloc, next);
            break :blk 0;
        },
        .terminal => |binding| blk: {
            if ((next.state != .active and next.state != .uncertain) or
                !std.mem.eql(u8, &next.active_txn.?, &binding.txn_id)) return error.SqlConnectionBusy;
            var session = (try store.loadSessionTxn(txn, binding.txn_id)) orelse return error.SqlTransactionOutcomeUnknown;
            defer session.deinit(store.alloc);
            // Recovery can transfer the session lease while the connection retains its owner.
            if (session.connection_id == null or !std.mem.eql(u8, &session.connection_id.?, &id) or
                !std.mem.eql(u8, session.principal orelse "", principal))
                return error.SqlTransactionOutcomeUnknown;
            if (session.terminal_commit == null and !session.terminal_abort) return error.SqlTransactionOutcomeUnknown;
            if (session.terminal_commit != null and session.terminal_abort) return error.InvalidTransactionSessionRecord;
            next.state = .idle;
            next.active_txn = null;
            if (next.revision == std.math.maxInt(u64)) return error.SqlProgramLimitExceeded;
            next.revision += 1;
            if (session.terminal_commit != null) next.overlay = session.setting_committed.items;
            try record.putTxn(txn, alloc, next);
            break :blk 0;
        },
        .overlay => |change| blk: {
            if (next.state != .idle or next.revision != change.expected_revision or change.entries.len > record.max_overlay_entries)
                return error.SqlConnectionChanged;
            if (next.revision == std.math.maxInt(u64)) return error.SqlProgramLimitExceeded;
            next.overlay = change.entries;
            next.revision += 1;
            try record.putTxn(txn, alloc, next);
            break :blk 0;
        },
    };
}

test "HTTP connection DISCARD fences only its prepared resources and active owner" {
    const alloc = std.testing.allocator;
    var backend = @import("antfly_local_sources").storage_mem_backend.Backend.init(alloc, .{});
    defer backend.close();
    var native = try backend.runtimeStore(alloc, .{ .name = "sql-connection-test" });
    defer native.deinit();
    var store = transactions.DurableSessionStore.initRuntime(alloc, &native);
    const alice: [32]u8 = @splat('a');
    const other: [32]u8 = @splat('b');
    const alice_plan: [32]u8 = @splat('1');
    const other_plan: [32]u8 = @splat('2');
    _ = try mutate(&store, .{ .create = .{ .id = alice, .principal = "alice", .owner_node_id = 7, .expires_at_ms = 1000, .database = "main", .namespace = "public" } }, "alice", 7, 1);
    _ = try mutate(&store, .{ .create = .{ .id = other, .principal = "alice", .owner_node_id = 7, .expires_at_ms = 1000, .database = "main", .namespace = "public" } }, "alice", 7, 1);
    try prepared.create(&store, .{ .id = alice_plan, .principal = "alice", .owner_node_id = 7, .expires_at_ms = 900, .database = "main", .namespace = "public", .statement = "SELECT 1", .connection_id = alice, .connection_generation = 1, .parameter_types = &.{}, .bindings = &.{} }, 1);
    try prepared.create(&store, .{ .id = other_plan, .principal = "alice", .owner_node_id = 7, .expires_at_ms = 900, .database = "main", .namespace = "public", .statement = "SELECT 2", .connection_id = other, .connection_generation = 1, .parameter_types = &.{}, .bindings = &.{} }, 1);
    try std.testing.expectError(error.SqlConnectionNotFound, load(&store, alloc, alice, "bob", 7, 2));
    var admitted = try prepared.loadForConnection(&store, alloc, &alice_plan, "alice", 7, 2, alice);
    defer admitted.deinit();
    _ = try mutate(&store, .{ .overlay = .{ .id = alice, .expected_revision = 1, .entries = &.{} } }, "alice", 7, 2);
    try std.testing.expectError(error.SqlConnectionChanged, mutate(&store, .{ .begin = .{ .id = alice, .expected_revision = 1, .txn_id = @splat(3) } }, "alice", 7, 2));
    _ = try mutate(&store, .{ .begin = .{ .id = alice, .expected_revision = 2, .txn_id = @splat(3) } }, "alice", 7, 2);
    try std.testing.expectError(error.SqlConnectionBusy, mutate(&store, .{ .abort_begin = .{ .id = alice, .txn_id = @splat(4) } }, "alice", 7, 2));
    try std.testing.expectError(error.SqlConnectionBusy, mutate(&store, .{ .discard = alice }, "alice", 7, 2));
    _ = try mutate(&store, .{ .bind = .{ .id = alice, .txn_id = @splat(3) } }, "alice", 7, 2);
    const attached_plan: [32]u8 = @splat('3');
    const attached_hex = std.fmt.bytesToHex(@as([16]u8, @splat(3)), .lower);
    try prepared.create(&store, .{ .id = attached_plan, .principal = "alice", .owner_node_id = 7, .expires_at_ms = 900, .database = "main", .namespace = "public", .statement = "SELECT 3", .session_id = attached_hex, .connection_id = alice, .connection_generation = 1, .parameter_types = &.{}, .bindings = &.{} }, 2);
    var attached = try prepared.loadForConnection(&store, alloc, &attached_plan, "alice", 7, 2, alice);
    defer attached.deinit();
    try attached.value.verifyExecutionSession(&attached_hex);
    try std.testing.expectError(error.SqlTransactionNotActive, attached.value.verifyExecutionSession(null));
    try std.testing.expectError(error.SqlConnectionBusy, mutate(&store, .{ .discard = alice }, "alice", 7, 2));
    _ = try mutate(&store, .{ .uncertain = .{ .id = alice, .guard = .{ .id = @splat(3), .owner = 7, .tenure = null } } }, "alice", 7, 2);
    try std.testing.expectError(error.SqlConnectionBusy, mutate(&store, .{ .discard = alice }, "alice", 7, 2));
    var aborted_session: transactions.Session = .{
        .txn_id = @splat(3),
        .owner_node_id = 7,
        .principal = try alloc.dupe(u8, "alice"),
        .connection_id = alice,
        .begin_timestamp = 1,
        .last_touched_timestamp = 1,
        .sync_level = .propose,
        .commit_body_digest = @splat(4),
        .commit_execution_started = true,
        .terminal_abort = true,
        .sql = .{ .database = try alloc.dupe(u8, "main"), .namespace = try alloc.dupe(u8, "public"), .isolation = .read_committed, .mode = .read_write },
    };
    defer aborted_session.deinit(alloc);
    try store.saveForTest(aborted_session, null);
    _ = try mutate(&store, .{ .terminal = .{ .id = alice, .txn_id = @splat(3) } }, "alice", 7, 2);
    try std.testing.expectEqual(@as(usize, 2), try mutate(&store, .{ .discard = alice }, "alice", 7, 2));
    try std.testing.expectError(error.SqlPreparedNotFound, prepared.loadForConnection(&store, alloc, &alice_plan, "alice", 7, 2, alice));
    var retained = try prepared.loadForConnection(&store, alloc, &other_plan, "alice", 7, 2, other);
    defer retained.deinit();
    try std.testing.expectEqualStrings("SELECT 1", admitted.value.statement);
    var reset = try load(&store, alloc, alice, "alice", 7, 2);
    defer reset.deinit();
    try std.testing.expectEqual(@as(u64, 2), reset.value.generation);
    try std.testing.expectEqual(record.State.idle, reset.value.state);
}

test "HTTP connection recovers a durable pre-response BEGIN fence without an escaped transaction" {
    const alloc = std.testing.allocator;
    var backend = @import("antfly_local_sources").storage_mem_backend.Backend.init(alloc, .{});
    defer backend.close();
    var native = try backend.runtimeStore(alloc, .{ .name = "sql-connection-begin-recovery-test" });
    defer native.deinit();
    var store = transactions.DurableSessionStore.initRuntime(alloc, &native);
    var registry = transactions.SessionRegistry.init(&store);
    defer registry.deinit(alloc);
    const id: [32]u8 = @splat('c');
    const txn_id: [16]u8 = @splat(9);
    _ = try mutate(&store, .{ .create = .{ .id = id, .principal = "alice", .owner_node_id = 7, .expires_at_ms = 1000, .database = "main", .namespace = "public" } }, "alice", 7, 1);
    _ = try mutate(&store, .{ .begin = .{ .id = id, .expected_revision = 1, .txn_id = txn_id } }, "alice", 7, 2);
    var fenced = try load(&store, alloc, id, "alice", 7, 2);
    defer fenced.deinit();
    try std.testing.expectEqual(record.State.beginning, fenced.value.state);
    try std.testing.expectError(error.SqlConnectionBusy, mutate(&store, .{ .discard = id }, "alice", 7, 2));
    _ = try registry.beginForPrincipalWithSettingsAndId(alloc, .{ .sql = .{ .database = "main", .namespace = "public", .isolation = .read_committed, .mode = .read_write } }, 7, "alice", &.{}, id, txn_id);
    var recovered = try loadReconciled(&store, &registry, std.heap.page_allocator, id, "alice", 7, 2);
    defer recovered.deinit();
    try std.testing.expectEqual(record.State.idle, recovered.value.state);
    try std.testing.expect(recovered.value.active_txn == null);
    try std.testing.expectEqual(@as(usize, 0), try mutate(&store, .{ .discard = id }, "alice", 7, 2));

    try std.testing.expectError(error.SqlConnectionChanged, registry.beginForPrincipalWithSettingsAndId(alloc, .{ .sql = .{ .database = "main", .namespace = "public", .isolation = .read_committed, .mode = .read_write } }, 7, "alice", &.{}, id, txn_id));
    try std.testing.expect((try registry.getSqlState(alloc, txn_id)) == null);

    // Expired non-idle roots must not occupy the bounded directory forever.
    const replacement: [32]u8 = @splat('d');
    var current = try load(&store, alloc, id, "alice", 7, 2);
    defer current.deinit();
    _ = try mutate(&store, .{ .begin = .{ .id = id, .expected_revision = current.value.revision, .txn_id = txn_id } }, "alice", 7, 2);
    _ = try registry.beginForPrincipalWithSettingsAndId(alloc, .{ .sql = .{ .database = "main", .namespace = "public", .isolation = .read_committed, .mode = .read_write } }, 7, "alice", &.{}, id, txn_id);
    try std.testing.expectError(error.SqlConnectionBusy, mutate(&store, .{ .abort_begin = .{ .id = id, .txn_id = txn_id } }, "alice", 7, 2));
    try reconcileExpired(&store, &registry, alloc, 1001);
    try std.testing.expect((try registry.getSqlState(alloc, txn_id)) == null);
    _ = try mutate(&store, .{ .create = .{ .id = replacement, .principal = "alice", .owner_node_id = 7, .expires_at_ms = 2000, .database = "main", .namespace = "public" } }, "alice", 7, 1001);
    try std.testing.expectError(error.SqlConnectionNotFound, load(&store, alloc, id, "alice", 7, 2));
}

test "HTTP connection recovers an expired slot only on capacity pressure" {
    const a = std.testing.allocator;
    var backend = @import("antfly_local_sources").storage_mem_backend.Backend.init(a, .{});
    defer backend.close();
    var native = try backend.runtimeStore(a, .{ .name = "sql-connection-pressure" });
    defer native.deinit();
    var store = transactions.DurableSessionStore.initRuntime(a, &native);
    var registry = transactions.SessionRegistry.init(&store);
    defer registry.deinit(a);
    var value: record.Record = .{ .id = @splat('0'), .principal = "alice", .owner_node_id = 7, .expires_at_ms = 2, .database = "main", .namespace = "public" };
    try create(&store, &registry, value, 1);
    const expired_id = value.id;
    const txn_id = transactions.newSessionTxnId(7);
    _ = try mutate(&store, .{ .begin = .{ .id = value.id, .expected_revision = 1, .txn_id = txn_id } }, "alice", 7, 1);
    _ = try registry.beginForPrincipalWithSettingsAndId(a, .{ .sql = .{ .database = "main", .namespace = "public", .isolation = .read_committed, .mode = .read_write } }, 7, "alice", &.{}, value.id, txn_id);
    value.expires_at_ms = 100;
    for (1..max_connections) |index| {
        value.id = std.fmt.bytesToHex(std.mem.toBytes(@as(u128, index)), .lower);
        try create(&store, &registry, value, 3);
    }
    try std.testing.expect((try store.connectionLifecycle(txn_id)) != null);
    value.id = @splat('f');
    try create(&store, &registry, value, 3);
    try std.testing.expect((try store.connectionLifecycle(txn_id)) == null);
    try std.testing.expectError(error.SqlConnectionNotFound, load(&store, a, expired_id, "alice", 7, 1));
}

test "HTTP connection keeps a referenced durable transaction through session GC" {
    const alloc = std.testing.allocator;
    var backend = @import("antfly_local_sources").storage_mem_backend.Backend.init(alloc, .{});
    defer backend.close();
    var native = try backend.runtimeStore(alloc, .{ .name = "sql-connection-gc-test" });
    defer native.deinit();
    var store = transactions.DurableSessionStore.initRuntime(alloc, &native);
    const leases = transactions.SessionLeaseStore.initFromDurable(&store);
    var registry = transactions.SessionRegistry.initWithLeaseTtl(&store, leases, 60 * std.time.ns_per_s);
    registry.durable_scope = .cluster_shared;
    defer registry.deinit(alloc);
    const id: [32]u8 = @splat('e');
    const txn_id = transactions.newSessionTxnId(7);
    _ = try mutate(&store, .{ .create = .{ .id = id, .principal = "alice", .owner_node_id = 7, .expires_at_ms = 1000, .database = "main", .namespace = "public" } }, "alice", 7, 1);
    _ = try mutate(&store, .{ .begin = .{ .id = id, .expected_revision = 1, .txn_id = txn_id } }, "alice", 7, 2);
    const begun = try registry.beginForPrincipalWithSettingsAndId(alloc, .{ .sql = .{ .database = "main", .namespace = "public", .isolation = .read_committed, .mode = .read_write } }, 7, "alice", &.{}, id, txn_id);
    _ = try mutate(&store, .{ .bind = .{ .id = id, .txn_id = txn_id } }, "alice", 7, 2);
    const original = try registry.mutation(alloc, txn_id, 7);
    const status = (try registry.getStatus(alloc, txn_id)).?;
    _ = try registry.renewOwnedLeases(7, status.lease_expires_at + std.time.ns_per_ms);
    try std.testing.expectError(error.SessionLeaseLost, registry.retireBeforeExecution(alloc, original, begun.version, true));
    try std.testing.expectError(error.SessionLeaseLost, registry.retireBeforeExecution(alloc, original, begun.version, false));
    try std.testing.expectError(error.SessionLeaseLost, mutate(&store, .{ .uncertain = .{ .id = id, .guard = original } }, "alice", 7, 2));
    store.fail_writes_for_test = true;
    try std.testing.expectError(error.InjectedSessionStoreFailure, registry.rollbackConnectionBeforeExecution(txn_id, id, begun.version, .active));
    store.fail_writes_for_test = false;
    var still_active = try load(&store, alloc, id, "alice", 7, 2);
    try std.testing.expectEqual(record.State.active, still_active.value.state);
    still_active.deinit();
    _ = try registry.cleanupExpired(alloc, std.math.maxInt(u64));
    var direct_route_details = (try registry.getDetails(alloc, txn_id)) orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(id, direct_route_details.connection_id.?);
    direct_route_details.deinit(alloc);
    var retained = (try registry.getSqlState(alloc, txn_id)) orelse return error.TestExpectedEqual;
    retained.deinit(alloc);
    try std.testing.expectError(error.SqlTransactionOutcomeUnknown, registry.rollbackConnectionBeforeExecution(txn_id, id, begun.version, .beginning));
    try reconcileExpired(&store, &registry, alloc, 999);
    var unexpired = try load(&store, alloc, id, "alice", 7, 999);
    try std.testing.expectEqual(record.State.active, unexpired.value.state);
    unexpired.deinit();
    // Reconcile from a second registry while a foreground stage holds a clone
    // of the retiring session and its own local stripe.
    const Race = struct {
        store: *transactions.DurableSessionStore,
        registry: *transactions.SessionRegistry,
        fn expire(ptr: *anyopaque, a: std.mem.Allocator, _: ?*const transactions.OwnedTransactionCommitRequest, _: *transactions.OwnedTransactionCommitRequest, _: *const transactions.OwnedTransactionCommitRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try reconcileExpired(self.store, self.registry, a, 1000);
        }
    };
    var sweeper = transactions.SessionRegistry.initWithLeaseTtl(&store, leases, 60 * std.time.ns_per_s);
    sweeper.durable_scope = .cluster_shared;
    defer sweeper.deinit(alloc);
    var race = Race{ .store = &store, .registry = &sweeper };
    try std.testing.expectError(error.SessionLeaseLost, registry.stageValidated(alloc, try registry.mutation(alloc, txn_id, 7), &.{}, .{ .ptr = &race, .validate = Race.expire }));
    try std.testing.expectEqual(@as(usize, 0), try registry.renewOwnedLeases(7, std.math.maxInt(u64)));
    try std.testing.expect((try leases.load(alloc, txn_id)) == null);
    try std.testing.expectError(error.SqlTransactionNotActive, registry.cloneSqlStaged(alloc, txn_id));
    try std.testing.expectError(error.SqlConnectionNotFound, load(&store, alloc, id, "alice", 7, 2));
    try std.testing.expect((try registry.getSqlState(alloc, txn_id)) == null);
}

test "HTTP connection reconciles expired uncertain attempts that never started execution" {
    const alloc = std.testing.allocator;
    var backend = @import("antfly_local_sources").storage_mem_backend.Backend.init(alloc, .{});
    defer backend.close();
    var native = try backend.runtimeStore(alloc, .{ .name = "sql-connection-uncertain-test" });
    defer native.deinit();
    var store = transactions.DurableSessionStore.initRuntime(alloc, &native);
    var registry = transactions.SessionRegistry.initWithLeaseTtl(&store, transactions.SessionLeaseStore.initFromDurable(&store), 60 * std.time.ns_per_s);
    registry.durable_scope = .cluster_shared;
    registry.max_sessions = 1;
    defer registry.deinit(alloc);

    try reconcileExpired(&store, &registry, alloc, 1);
    for ([_]bool{ false, true }, [_]u8{ 'a', 'b' }) |request_persisted, digit| {
        const id: [32]u8 = @splat(digit);
        const txn_id = transactions.newSessionTxnId(7);
        _ = try mutate(&store, .{ .create = .{ .id = id, .principal = "alice", .owner_node_id = 7, .expires_at_ms = 1000, .database = "main", .namespace = "public" } }, "alice", 7, 1);
        _ = try mutate(&store, .{ .begin = .{ .id = id, .expected_revision = 1, .txn_id = txn_id } }, "alice", 7, 2);
        _ = try registry.beginForPrincipalWithSettingsAndId(alloc, .{ .sql = .{ .database = "main", .namespace = "public", .isolation = .read_committed, .mode = .read_write } }, 7, "alice", &.{}, id, txn_id);
        _ = try mutate(&store, .{ .bind = .{ .id = id, .txn_id = txn_id } }, "alice", 7, 2);
        const guard = try registry.mutation(alloc, txn_id, 7);
        if (request_persisted) {
            var session = (try store.load(txn_id)).?;
            defer session.deinit(alloc);
            session.commit_body_digest = @splat(4);
            try store.saveForTest(session, null);
        }
        if (request_persisted) _ = try mutate(&store, .{ .uncertain = .{ .id = id, .guard = guard } }, "alice", 7, 2);
        try reconcileExpired(&store, &registry, alloc, 999);
        var pending = try load(&store, alloc, id, "alice", 7, 999);
        defer pending.deinit();
        try std.testing.expectEqual(if (request_persisted) record.State.uncertain else .active, pending.value.state);
        try reconcileExpired(&store, &registry, alloc, 1000);
        try std.testing.expectError(error.SqlConnectionNotFound, load(&store, alloc, id, "alice", 7, 2));
        const disposition = (try registry.getStatusForPrincipal(alloc, txn_id, "alice")).?;
        try std.testing.expectEqual(transactions.SessionDisposition.aborted, disposition.disposition);
        try std.testing.expectEqual(@as(u64, 0), disposition.lease_expires_at);
        try std.testing.expect((try registry.getStatusForPrincipal(alloc, txn_id, "bob")) == null);
        try std.testing.expect((try store.load(txn_id)) == null);
    }
    try std.testing.expectEqual(@as(usize, 2), try registry.cleanupExpired(alloc, std.math.maxInt(u64)));
}

test "HTTP connection reconciles a durable aborted coordinator decision" {
    const alloc = std.testing.allocator;
    var backend = @import("antfly_local_sources").storage_mem_backend.Backend.init(alloc, .{});
    defer backend.close();
    var native = try backend.runtimeStore(alloc, .{ .name = "sql-connection-abort-recovery-test" });
    defer native.deinit();
    var store = transactions.DurableSessionStore.initRuntime(alloc, &native);
    var registry = transactions.SessionRegistry.init(&store);
    defer registry.deinit(alloc);
    const id: [32]u8 = @splat('f');
    const txn_id = transactions.newSessionTxnId(7);
    _ = try mutate(&store, .{ .create = .{ .id = id, .principal = "alice", .owner_node_id = 7, .expires_at_ms = 1000, .database = "main", .namespace = "public" } }, "alice", 7, 1);
    _ = try mutate(&store, .{ .begin = .{ .id = id, .expected_revision = 1, .txn_id = txn_id } }, "alice", 7, 2);
    _ = try mutate(&store, .{ .bind = .{ .id = id, .txn_id = txn_id } }, "alice", 7, 2);
    var session: transactions.Session = .{
        .txn_id = txn_id,
        .owner_node_id = 7,
        .principal = try alloc.dupe(u8, "alice"),
        .connection_id = id,
        .begin_timestamp = 1,
        .last_touched_timestamp = 1,
        .sync_level = .propose,
        .commit_body_digest = @splat(4),
        .commit_execution_started = true,
        .sql = .{ .database = try alloc.dupe(u8, "main"), .namespace = try alloc.dupe(u8, "public"), .isolation = .read_committed, .mode = .read_write },
    };
    defer session.deinit(alloc);
    try store.saveForTest(session, null);
    registry.durable_scope = .cluster_shared;
    try std.testing.expect(try registry.adopt(alloc, txn_id, 7));
    var recovery = transactions.SessionRegistry.init(&store);
    defer recovery.deinit(alloc);
    recovery.durable_scope = .cluster_shared;
    try std.testing.expect(try recovery.adopt(alloc, txn_id, 8));
    try reconcileExpired(&store, &registry, alloc, 1001);
    var executing = try load(&store, alloc, id, "alice", 7, 2);
    try std.testing.expectEqual(record.State.active, executing.value.state);
    executing.deinit();
    _ = (try recovery.recordTerminalAbort(alloc, txn_id)) orelse return error.TestExpectedEqual;
    var proved = (try registry.getSqlState(alloc, txn_id)) orelse return error.TestExpectedEqual;
    try std.testing.expect(proved.terminal_abort);
    proved.deinit(alloc);
    var cold_reader = transactions.SessionRegistry.init(&store);
    defer cold_reader.deinit(alloc);
    var cold_details = (try cold_reader.getDetails(alloc, txn_id)) orelse return error.TestExpectedEqual;
    defer cold_details.deinit(alloc);
    try std.testing.expectEqual(transactions.SessionDisposition.aborted, cold_details.status.disposition);
    const status = try transactions.buildSessionStatusResponse(alloc, cold_details.status);
    defer alloc.free(status.transaction_id);
    try std.testing.expectEqualStrings("aborted", status.disposition);
    var response_arena = std.heap.ArenaAllocator.init(alloc);
    defer response_arena.deinit();
    const response_alloc = response_arena.allocator();
    const details_response = try transactions.buildSessionDetailsResponse(response_alloc, cold_details);
    const details_json = try std.json.Stringify.valueAlloc(response_alloc, details_response, .{});
    const metadata_openapi = @import("antfly_metadata_openapi");
    const generated_details = try std.json.parseFromSlice(metadata_openapi.TransactionSessionDetailsResponse, response_alloc, details_json, .{});
    try std.testing.expectEqualStrings("aborted", generated_details.value.disposition);
    const status_json = try std.json.Stringify.valueAlloc(response_alloc, status, .{});
    const generated_status = try std.json.parseFromSlice(metadata_openapi.TransactionSessionStatus, response_alloc, status_json, .{});
    try std.testing.expectEqualStrings("aborted", generated_status.value.disposition);
    var reconciled = try loadReconciled(&store, &registry, alloc, id, "alice", 7, 2);
    defer reconciled.deinit();
    try std.testing.expectEqual(record.State.idle, reconciled.value.state);
    _ = try registry.cleanupExpired(alloc, std.math.maxInt(u64));
    try std.testing.expect((try registry.getSqlState(alloc, txn_id)) == null);
}

test "HTTP connection publishes only committed nonlocal settings on terminal proof" {
    const alloc = std.testing.allocator;
    var backend = @import("antfly_local_sources").storage_mem_backend.Backend.init(alloc, .{});
    defer backend.close();
    var native = try backend.runtimeStore(alloc, .{ .name = "sql-connection-settings-test" });
    defer native.deinit();
    var store = transactions.DurableSessionStore.initRuntime(alloc, &native);
    const id: [32]u8 = @splat('d');
    const txn_id = transactions.newSessionTxnId(7);
    _ = try mutate(&store, .{ .create = .{ .id = id, .principal = "alice", .owner_node_id = 7, .expires_at_ms = 1000, .database = "main", .namespace = "public" } }, "alice", 7, 1);
    _ = try mutate(&store, .{ .begin = .{ .id = id, .expected_revision = 1, .txn_id = txn_id } }, "alice", 7, 2);
    _ = try mutate(&store, .{ .bind = .{ .id = id, .txn_id = txn_id } }, "alice", 7, 2);
    var session: transactions.Session = .{
        .txn_id = txn_id,
        .owner_node_id = 7,
        .principal = try alloc.dupe(u8, "alice"),
        .connection_id = id,
        .begin_timestamp = 1,
        .last_touched_timestamp = 1,
        .sync_level = .propose,
        .commit_body_digest = @splat(4),
        .commit_execution_started = true,
        .sql = .{ .database = try alloc.dupe(u8, "main"), .namespace = try alloc.dupe(u8, "public"), .isolation = .read_committed, .mode = .read_write },
    };
    defer session.deinit(alloc);
    try session.setting_committed.append(alloc, .{ .identity = .{ .id = 17, .generation = 1 }, .value = .{ .integer = 5 } });
    try session.setting_active.append(alloc, .{ .identity = .{ .id = 17, .generation = 1 }, .value = .{ .integer = 9 } });
    try store.saveForTest(session, null);
    try std.testing.expectError(error.SqlTransactionOutcomeUnknown, mutate(&store, .{ .terminal = .{ .id = id, .txn_id = txn_id } }, "alice", 7, 2));
    var still_active = try load(&store, alloc, id, "alice", 7, 2);
    try std.testing.expectEqual(record.State.active, still_active.value.state);
    still_active.deinit();
    session.terminal_commit = .{ .status = .committed };
    try store.saveForTest(session, null);
    _ = try mutate(&store, .{ .terminal = .{ .id = id, .txn_id = txn_id } }, "alice", 7, 2);
    var committed = try load(&store, alloc, id, "alice", 7, 2);
    defer committed.deinit();
    try std.testing.expectEqual(record.State.idle, committed.value.state);
    try std.testing.expectEqual(@as(usize, 1), committed.value.overlay.len);
    try std.testing.expectEqual(@as(i64, 5), committed.value.overlay[0].value.integer);
}

test "HTTP connection directory preserves concurrent creates and closes" {
    const alloc = std.testing.allocator;
    var backend = @import("antfly_local_sources").storage_mem_backend.Backend.init(alloc, .{});
    defer backend.close();
    var native = try backend.runtimeStore(alloc, .{ .name = "sql-connection-race-test" });
    defer native.deinit();
    var store = transactions.DurableSessionStore.initRuntime(alloc, &native);
    const Worker = struct {
        fn run(shared: *transactions.DurableSessionStore, id: [32]u8, closing: bool, failure: *?anyerror) void {
            const action: Action = if (closing) .{ .close = id } else .{ .create = .{
                .id = id,
                .principal = "alice",
                .owner_node_id = 7,
                .expires_at_ms = 1000,
                .database = "main",
                .namespace = "public",
            } };
            _ = mutate(shared, action, "alice", 7, 1) catch |err| {
                failure.* = err;
                return;
            };
        }
    };
    var ids: [16][32]u8 = undefined;
    for (&ids, 0..) |*id, index| _ = try std.fmt.bufPrint(id, "{x:0>32}", .{index + 1});
    for (0..8) |_| {
        for ([_]bool{ false, true }) |closing| {
            var failures: [16]?anyerror = @splat(null);
            var group: std.Io.Group = .init;
            defer group.cancel(std.testing.io);
            for (ids, 0..) |id, index| try group.concurrent(std.testing.io, Worker.run, .{ &store, id, closing, &failures[index] });
            try group.await(std.testing.io);
            for (failures) |failure| if (failure) |err| return err;
            var txn = try native.beginRead();
            defer txn.abort();
            const entries = try copyDirectoryTxn(&txn, alloc);
            defer alloc.free(entries);
            try std.testing.expectEqual(@as(usize, if (closing) 0 else ids.len), entries.len);
            if (!closing) for (ids) |id| {
                var connection = try load(&store, alloc, id, "alice", 7, 1);
                connection.deinit();
            };
        }
    }
}
