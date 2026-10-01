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

//! Durable idle HTTP SQL connections. The native store transaction is the
//! linearization point for connection generation, overlay and prepared-plan
//! retirement. Transaction IDs remain owned by SessionRegistry, not by this
//! directory.
const std = @import("std");
const transactions = @import("transactions.zig");
const record = @import("sql_connection_record.zig");
const prepared = @import("sql_prepared.zig");
const settings = @import("../sql/setting_catalog.zig");

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
/// beginning ID has never escaped in an HTTP response: clearing its fence
/// first makes a concurrent creator fail binding, then an unexecuted session
/// can be removed. An uncertain commit remains fenced until a terminal
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
    return loadReconciledAt(store, registry, alloc, id, principal, owner_node_id, now_ms);
}

fn loadReconciledAt(
    store: *transactions.DurableSessionStore,
    registry: *transactions.SessionRegistry,
    alloc: std.mem.Allocator,
    id: [32]u8,
    principal: []const u8,
    owner_node_id: u64,
    authority_ms: u64,
) !Owned {
    var owned = try load(store, alloc, id, principal, owner_node_id, authority_ms);
    var owns_parsed = true;
    errdefer if (owns_parsed) owned.deinit();
    const txn_id = owned.value.active_txn orelse return owned;
    const state = owned.value.state;
    if (state == .idle) return owned;
    var session = try registry.getSqlState(alloc, txn_id);
    defer if (session) |*value| value.deinit(alloc);
    if (session) |value| {
        if (!std.meta.eql(value.connection_id, @as(?[32]u8, id))) return error.InvalidSqlConnection;
    }
    if (state == .beginning) {
        if (session) |value| {
            if (value.execution_started or value.terminal != null or value.terminal_abort) return error.SqlConnectionBusy;
        }
        // A racing BEGIN must bind the exact still-beginning ID before it
        // responds. After this CAS succeeds it cannot return a session ID.
        _ = mutate(store, .{ .abort_begin = .{ .id = id, .txn_id = txn_id } }, principal, owner_node_id, authority_ms) catch |err| switch (err) {
            error.SqlConnectionBusy => return owned,
            else => return err,
        };
        _ = registry.removeBeforeExecution(alloc, txn_id);
    } else if (session) |value| {
        if (value.terminal == null and !value.terminal_abort) return owned;
        _ = mutate(store, .{ .terminal = .{ .id = id, .txn_id = txn_id } }, principal, owner_node_id, authority_ms) catch |err| switch (err) {
            error.SqlConnectionBusy => return owned,
            else => return err,
        };
    } else return owned;
    owned.deinit();
    owns_parsed = false;
    return load(store, alloc, id, principal, owner_node_id, authority_ms);
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
    for (entries) |entry| {
        if (entry.expires_at_ms > now_ms) continue;
        var prior = switch (store.backend) {
            .docstore => |backend| blk: {
                var txn = try backend.beginReadTxn();
                defer txn.abort();
                break :blk record.loadTxn(&txn, alloc, &entry.id) catch |err| switch (err) {
                    error.SqlConnectionNotFound => continue,
                    else => return err,
                };
            },
            .runtime => |backend| blk: {
                var txn = try backend.beginRead();
                defer txn.abort();
                break :blk record.loadTxn(&txn, alloc, &entry.id) catch |err| switch (err) {
                    error.SqlConnectionNotFound => continue,
                    else => return err,
                };
            },
        };
        defer prior.deinit();
        if (prior.value.state == .idle) continue;
        var reconciled = loadReconciledAt(store, registry, alloc, entry.id, prior.value.principal, prior.value.owner_node_id, 0) catch |err| switch (err) {
            error.SqlConnectionBusy, error.SqlConnectionChanged, error.SqlConnectionNotFound => continue,
            else => return err,
        };
        reconciled.deinit();
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
    begin: struct { id: [32]u8, expected_revision: u64, txn_id: [16]u8 },
    abort_begin: struct { id: [32]u8, txn_id: [16]u8 },
    bind: struct { id: [32]u8, txn_id: [16]u8 },
    uncertain: struct { id: [32]u8, txn_id: [16]u8 },
    terminal: struct { id: [32]u8, txn_id: [16]u8 },
    overlay: struct { id: [32]u8, expected_revision: u64, entries: []const settings.OverlayEntry },
};

pub fn mutate(store: *transactions.DurableSessionStore, action: Action, principal: []const u8, owner_node_id: u64, now_ms: u64) !usize {
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
        var entries: [max_connections]Entry = undefined;
        var count: usize = 0;
        for (directory.value.entries) |entry| {
            if (std.mem.eql(u8, &entry.id, &value.id)) return error.SqlConnectionAlreadyExists;
            if (entry.expires_at_ms <= now_ms) {
                const old_key = try record.key(&entry.id);
                var previous = record.loadTxn(txn, alloc, &entry.id) catch |err| switch (err) {
                    error.SqlConnectionNotFound => null,
                    else => return err,
                };
                if (previous) |*owned| {
                    defer owned.deinit();
                    // Never collect an expired connection while a transaction
                    // could still have an uncertain commit decision.
                    if (owned.value.state != .idle) {
                        entries[count] = entry;
                        count += 1;
                        continue;
                    }
                    _ = try prepared.discardConnectionTxn(txn, alloc, entry.id);
                    try txn.delete(&old_key);
                }
                continue;
            }
            entries[count] = entry;
            count += 1;
        }
        if (count == max_connections) return error.SqlWriteCapacityUnavailable;
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
        .begin => |value| value.id,
        .abort_begin => |value| value.id,
        .bind => |value| value.id,
        .uncertain => |value| value.id,
        .terminal => |value| value.id,
        .overlay => |value| value.id,
    };
    var owned = try loadTxn(txn, alloc, id, principal, owner_node_id, now_ms);
    defer owned.deinit();
    var next = owned.value;
    return switch (action) {
        .create => unreachable,
        .discard, .close => blk: {
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
                // Remove the exact directory entry as part of this commit.
                const raw = try txn.get(directory_key);
                var directory = try std.json.parseFromSlice(Directory, alloc, raw, .{ .allocate = .alloc_always });
                defer directory.deinit();
                if (directory.value.entries.len > max_connections) return error.InvalidSqlConnection;
                var retained: [max_connections]Entry = undefined;
                var count: usize = 0;
                var found = false;
                for (directory.value.entries) |entry| {
                    if (std.mem.eql(u8, &entry.id, &id)) {
                        found = true;
                        continue;
                    }
                    retained[count] = entry;
                    count += 1;
                }
                if (!found) return error.InvalidSqlConnection;
                const encoded = try std.json.Stringify.valueAlloc(alloc, Directory{ .entries = retained[0..count] }, .{});
                defer alloc.free(encoded);
                try txn.put(directory_key, encoded);
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
            if (next.state != .active or !std.mem.eql(u8, &next.active_txn.?, &binding.txn_id)) return error.SqlConnectionBusy;
            next.state = .uncertain;
            try record.putTxn(txn, alloc, next);
            break :blk 0;
        },
        .terminal => |binding| blk: {
            if ((next.state != .active and next.state != .uncertain) or
                !std.mem.eql(u8, &next.active_txn.?, &binding.txn_id)) return error.SqlConnectionBusy;
            var session = (try store.loadSessionTxn(txn, binding.txn_id)) orelse return error.SqlTransactionOutcomeUnknown;
            defer session.deinit(alloc);
            if (session.connection_id == null or !std.mem.eql(u8, &session.connection_id.?, &id) or
                session.owner_node_id != owner_node_id or !std.mem.eql(u8, session.principal orelse "", principal))
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
    var backend = @import("../storage/mem_backend.zig").Backend.init(alloc, .{});
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
    _ = try mutate(&store, .{ .uncertain = .{ .id = alice, .txn_id = @splat(3) } }, "alice", 7, 2);
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
    try store.save(aborted_session, null);
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
    var backend = @import("../storage/mem_backend.zig").Backend.init(alloc, .{});
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
    var recovered = try loadReconciled(&store, &registry, alloc, id, "alice", 7, 2);
    defer recovered.deinit();
    try std.testing.expectEqual(record.State.idle, recovered.value.state);
    try std.testing.expect(recovered.value.active_txn == null);
    try std.testing.expectEqual(@as(usize, 0), try mutate(&store, .{ .discard = id }, "alice", 7, 2));

    // Expired non-idle roots must not occupy the bounded directory forever.
    const replacement: [32]u8 = @splat('d');
    _ = try mutate(&store, .{ .begin = .{ .id = id, .expected_revision = 2, .txn_id = txn_id } }, "alice", 7, 2);
    try reconcileExpired(&store, &registry, alloc, 1001);
    _ = try mutate(&store, .{ .create = .{ .id = replacement, .principal = "alice", .owner_node_id = 7, .expires_at_ms = 2000, .database = "main", .namespace = "public" } }, "alice", 7, 1001);
    try std.testing.expectError(error.SqlConnectionNotFound, load(&store, alloc, id, "alice", 7, 2));
}

test "HTTP connection keeps a referenced durable transaction through session GC" {
    const alloc = std.testing.allocator;
    var backend = @import("../storage/mem_backend.zig").Backend.init(alloc, .{});
    defer backend.close();
    var native = try backend.runtimeStore(alloc, .{ .name = "sql-connection-gc-test" });
    defer native.deinit();
    var store = transactions.DurableSessionStore.initRuntime(alloc, &native);
    var registry = transactions.SessionRegistry.init(&store);
    defer registry.deinit(alloc);
    const id: [32]u8 = @splat('e');
    const txn_id = transactions.newSessionTxnId(7);
    _ = try mutate(&store, .{ .create = .{ .id = id, .principal = "alice", .owner_node_id = 7, .expires_at_ms = 1000, .database = "main", .namespace = "public" } }, "alice", 7, 1);
    _ = try mutate(&store, .{ .begin = .{ .id = id, .expected_revision = 1, .txn_id = txn_id } }, "alice", 7, 2);
    _ = try registry.beginForPrincipalWithSettingsAndId(alloc, .{ .sql = .{ .database = "main", .namespace = "public", .isolation = .read_committed, .mode = .read_write } }, 7, "alice", &.{}, id, txn_id);
    _ = try mutate(&store, .{ .bind = .{ .id = id, .txn_id = txn_id } }, "alice", 7, 2);
    store.fail_writes_for_test = true;
    try std.testing.expectError(error.InjectedSessionStoreFailure, registry.rollbackConnectionBeforeExecution(alloc, txn_id, id));
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
    try registry.rollbackConnectionBeforeExecution(alloc, txn_id, id);
    var idle = try load(&store, alloc, id, "alice", 7, 2);
    defer idle.deinit();
    try std.testing.expectEqual(record.State.idle, idle.value.state);
    try std.testing.expect((try registry.getSqlState(alloc, txn_id)) == null);
}

test "HTTP connection reconciles a durable aborted coordinator decision" {
    const alloc = std.testing.allocator;
    var backend = @import("../storage/mem_backend.zig").Backend.init(alloc, .{});
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
    try store.save(session, null);
    _ = (try registry.recordTerminalAbort(alloc, txn_id)) orelse return error.TestExpectedEqual;
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
    var backend = @import("../storage/mem_backend.zig").Backend.init(alloc, .{});
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
    try store.save(session, null);
    try std.testing.expectError(error.SqlTransactionOutcomeUnknown, mutate(&store, .{ .terminal = .{ .id = id, .txn_id = txn_id } }, "alice", 7, 2));
    var still_active = try load(&store, alloc, id, "alice", 7, 2);
    try std.testing.expectEqual(record.State.active, still_active.value.state);
    still_active.deinit();
    session.terminal_commit = .{ .status = .committed };
    try store.save(session, null);
    _ = try mutate(&store, .{ .terminal = .{ .id = id, .txn_id = txn_id } }, "alice", 7, 2);
    var committed = try load(&store, alloc, id, "alice", 7, 2);
    defer committed.deinit();
    try std.testing.expectEqual(record.State.idle, committed.value.state);
    try std.testing.expectEqual(@as(usize, 1), committed.value.overlay.len);
    try std.testing.expectEqual(@as(i64, 5), committed.value.overlay[0].value.integer);
}
