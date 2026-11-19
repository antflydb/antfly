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

//! Native queue WAL, retained readers and metadata snapshot installation.
const std = @import("std");
const local = @import("antfly_local_sources");
const Store = @import("raft_apply_store.zig").RaftApplyStore;
const locks = @import("../../system_catalog/lock_authority.zig");
const notifications = @import("../../system_catalog/sql_notifications.zig");
const protocol = @import("../topology_protocol.zig");
const group = local.common_group_ids.main_metadata_group_id;
const incarnation: [16]u8 = @splat(0x11);
const connection: [32]u8 = @splat('a');
const backend: locks.locks.Backend = .{ .incarnation = incarnation, .id = @splat(2), .fence = 1 };
const scope: notifications.Scope = .{ .group = group, .database = 1, .incarnation = incarnation };
fn emit(store: *Store, a: std.mem.Allocator, serial: u8, commands: []const notifications.types.Command, commit: bool) !void {
    var txn = try store.store.beginWriteTxn();
    var live = true;
    defer if (live) txn.abort();
    const plan: notifications.types.Plan = .{ .connection = connection, .generation = 1, .sender_pid = 42, .commands = commands };
    try notifications.reserve(a, &txn, scope, @splat(serial), @splat(serial + 1), plan);
    try notifications.commit(a, &txn, scope, @splat(serial), @splat(serial + 1), plan);
    try notifications.ready(a, &txn, scope, @splat(serial), @splat(serial + 1), plan);
    if (commit) {
        try txn.commit();
        live = false;
    }
}

fn rejectCorruptCheckpoint(store: *Store, a: std.mem.Allocator, fault: enum { budget, cursor, reclamation, alias }) !void {
    const N = notifications;
    var buffer: [128]u8 = undefined;
    const prefix = try N.prefixForGroup(&buffer, group);
    var txn = try store.store.beginWriteTxn();
    defer txn.abort();
    const address = switch (fault) {
        .budget => try std.fmt.allocPrint(a, "{s}budget", .{prefix}),
        .cursor => try std.fmt.allocPrint(a, "{s}listener:{s}", .{ prefix, std.fmt.bytesToHex(connection, .lower) }),
        .reclamation => try std.fmt.allocPrint(a, "{s}{s}:head", .{ prefix, std.fmt.bytesToHex(scope.id(), .lower) }),
        .alias => address: {
            var upper = std.fmt.bytesToHex(scope.id(), .lower);
            for (&upper) |*byte| byte.* = std.ascii.toUpper(byte.*);
            break :address try std.fmt.allocPrint(a, "{s}{s}:head", .{ prefix, upper });
        },
    };
    const encoded = switch (fault) {
        .alias => try std.json.Stringify.valueAlloc(a, try N.head(a, &txn, scope), .{}),
        .budget => encoded: {
            var value = try N.decodeRecord(N.Budget, a, try txn.get(address));
            value.used += 1;
            break :encoded try std.json.Stringify.valueAlloc(a, value, .{});
        },
        .cursor => encoded: {
            var value = try N.decodeRecord(N.Listener, a, try txn.get(address));
            value.offered.sequence = std.math.maxInt(u64);
            break :encoded try std.json.Stringify.valueAlloc(a, value, .{});
        },
        .reclamation => encoded: {
            var value = try N.decodeRecord(N.Head, a, try txn.get(address));
            value.gc = .{ .cut = value.next, .minimum = value.next, .revision = value.revision, .retention_revision = value.retention_revision, .after = connection, .deleting = true };
            break :encoded try std.json.Stringify.valueAlloc(a, value, .{});
        },
    };
    const raw = try a.alloc(u8, encoded.len + 32);
    @memcpy(raw[0..encoded.len], encoded);
    std.crypto.hash.Blake3.hash(encoded, raw[encoded.len..][0..32], .{});
    try txn.put(address, raw);
    const Row = struct { key: []const u8, value: []const u8 };
    var rows: std.ArrayList(Row) = .empty;
    {
        var cursor = try txn.openCursor();
        defer cursor.close();
        var entry = try cursor.seekAtOrAfter(prefix);
        while (entry) |value| {
            if (!std.mem.startsWith(u8, value.key, prefix)) break;
            try rows.append(a, .{ .key = try a.dupe(u8, value.key), .value = try a.dupe(u8, value.value) });
            entry = try cursor.next();
        }
    }
    try std.testing.expectError(error.InvalidCatalogRecord, @import("../../system_catalog/sql_notification_snapshot.zig").verify(a, group, &txn, rows.items, null));
}

test "SQL prepared transaction notification queue retains native WAL cuts escrow and offered prefixes through cold snapshot installation" {
    const a_backing = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a_backing);
    defer arena.deinit();
    const a = arena.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/notifications-source", .{tmp.sub_path});
    const replica_path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/notifications-replica", .{tmp.sub_path});
    for (0..2) |phase| {
        var store = try Store.init(a_backing, .{ .root_dir = path });
        defer store.deinit();
        if (phase == 0) {
            try store.applyStandaloneCommand(group, .{ .initialize_metadata_incarnation = @splat('1') });
            try store.applyStandaloneCommand(group, .{ .activate_topology_protocol = try std.json.Stringify.valueAlloc(a, protocol.Activation{ .version = protocol.current_version, .incarnation = @splat('1'), .member_count = 1, .membership_fingerprint = @splat(7) }, .{}) });
            const registration: locks.scopes.Registration = .{ .lease = .{ .backend = backend, .database = 1, .node = 9, .principal = try locks.principalDigest(a, .bootstrap), .expires_at_ms = 100000 }, .connection = connection, .nonce = @splat(3), .standalone = false };
            const cut = try std.json.parseFromSliceLeaky(locks.Snapshot, a, try store.sqlLockRead(a, group, .{ .backend = backend, .incarnation = incarnation, .standalone = false, .database = 1, .actor = .{ .session_user = .bootstrap, .credential = "" } }, .{}), .{});
            const request: locks.Request = .{ .admission = .{ .head = cut.head, .nonce = @splat(4) }, .incarnation = incarnation, .standalone = false, .database = 1, .actor = .{ .session_user = .bootstrap, .credential = "" }, .backend = backend, .operation = .{ .register = registration } };
            try store.applyStandaloneCommand(group, .{ .apply_sql_lock_scope = try std.json.Stringify.valueAlloc(a, request, .{}) });
            const outcome = try std.json.parseFromSliceLeaky(?locks.outcomes.Outcome, a, try store.sqlLockOutcome(a, group, request), .{});
            _ = try (outcome orelse return error.MissingRegistration).result();
            {
                var txn = try store.store.beginWriteTxn();
                errdefer txn.abort();
                try notifications.bind(a, &txn, group, .{ .backend = backend, .connection = connection, .generation = 1, .sender_pid = 42 });
                try txn.commit();
            }
            try emit(&store, a, 10, &.{.{ .listen = "events" }}, true);
            var retained = try store.store.beginReadTxn();
            defer retained.abort();
            try emit(&store, a, 12, &.{.{ .notify = .{ .channel = "events", .payload = "not-published" } }}, false);
            try std.testing.expectEqual(@as(u64, 1), (try notifications.head(a, &retained, scope)).next);
            try emit(&store, a, 14, &.{.{ .notify = .{ .channel = "events", .payload = "durable" } }}, true);
            try std.testing.expectEqual(@as(u64, 1), (try notifications.head(a, &retained, scope)).next);
            {
                var txn = try store.store.beginWriteTxn();
                errdefer txn.abort();
                const page = try notifications.poll(a, &txn, group, connection, 1, 128);
                try std.testing.expectEqual(@as(usize, 1), page.events.len);
                try std.testing.expectEqualStrings("durable", page.events[0].payload);
                try txn.commit();
            }
            try store.store.sync(true);
        } else {
            {
                var read = try store.store.beginReadTxn();
                defer read.abort();
                const page = try notifications.readPage(a, &read, group, connection, 1, 128);
                try std.testing.expectEqual(@as(usize, 1), page.events.len);
                try std.testing.expectEqualStrings("durable", page.events[0].payload);
                try std.testing.expectEqual(@as(u64, 2), page.offered.sequence);
                try std.testing.expectEqual(@as(u64, 1), page.acknowledged.sequence);
                try std.testing.expect((try notifications.head(a, &read, scope)).reserved == 0);
            }
            try rejectCorruptCheckpoint(&store, a, .budget);
            try rejectCorruptCheckpoint(&store, a, .cursor);
            try rejectCorruptCheckpoint(&store, a, .reclamation);
            try rejectCorruptCheckpoint(&store, a, .alias);
            const snapshot = try store.snapshotBuilder().buildSnapshot(a, group);
            var replica = try Store.init(a_backing, .{ .root_dir = replica_path });
            defer replica.deinit();
            const installer = replica.snapshotBuilder();
            try installer.vtable.install_snapshot.?(installer.ptr, a, group, 10, snapshot);
            var txn = try replica.store.beginWriteTxn();
            errdefer txn.abort();
            const page = try notifications.readPage(a, &txn, group, connection, 1, 128);
            try std.testing.expectEqual(@as(usize, 1), page.events.len);
            try std.testing.expectEqualStrings("durable", page.events[0].payload);
            try std.testing.expectEqual(@as(i32, 42), (try notifications.readBinding(a, &txn, group, backend)).?.sender_pid);
            var collision = backend;
            collision.id = @splat(5);
            try std.testing.expectError(error.SqlBackendPidInUse, notifications.bind(a, &txn, group, .{ .backend = collision, .connection = @splat('b'), .generation = 1, .sender_pid = 42 }));
            try notifications.acknowledge(a, &txn, group, connection, 1, page.cursor);
            for (0..4) |_| _ = try notifications.gcStep(a, &txn, scope);
            const empty = try notifications.head(a, &txn, scope);
            try std.testing.expectEqual(@as(u64, 0), empty.used);
            try std.testing.expectEqual(empty.next, empty.oldest);
            try notifications.retireBinding(a, &txn, group, backend);
            try std.testing.expect((try notifications.readBinding(a, &txn, group, backend)) == null);
            try txn.commit();
        }
    }
}
