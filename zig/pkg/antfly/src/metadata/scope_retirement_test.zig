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

//! Native transaction and cold-recovery tests of scope reservation custody.
//! These exercise the journal only, not physical child completion authority.
const std = @import("std");
const local = @import("antfly_local_sources");
const domain = local.system_catalog_domain;
const contract = domain.scope_retirement;
const ledger = @import("scope_retirement_ledger.zig");
const native = @import("storage/raft_apply_store.zig");
const topology = @import("topology_protocol.zig");
const docstore = local.storage_docstore;
const group = 21;

fn seed(store: *native.RaftApplyStore, a: std.mem.Allocator) !void {
    try store.applyStandaloneCommand(group, .{ .initialize_metadata_incarnation = @splat('1') });
    try store.applyStandaloneCommand(group, .{ .activate_topology_protocol = try std.json.Stringify.valueAlloc(a, topology.Activation{ .version = topology.current_version, .incarnation = @splat('1'), .member_count = 1, .membership_fingerprint = @splat(7) }, .{}) });
    try store.applyStandaloneCommand(group, .{ .apply_system_catalog = try std.json.Stringify.valueAlloc(a, native.SystemCatalogCommand{ .expected_revision = 0, .mutation = .{ .action = .create, .kind = .database, .name = "tenant_ops" } }, .{}) });
    for (0..2) |i| {
        const id: u64 = 101 + i;
        const name = try std.fmt.allocPrint(a, "table:{d}", .{id});
        const table: @import("domain.zig").TableRecord = .{ .table_id = id, .name = name, .min_ranges = 1 };
        const ranges = [_]@import("domain.zig").RangeRecord{.{ .table_id = id, .group_id = 301 + i, .range_id = 301 + i, .start_key = "" }};
        const command: native.SystemCatalogCommand = .{ .expected_revision = 1 + i, .mutation = .{ .action = .create, .kind = .table, .database = "tenant_ops", .name = if (i == 0) "first" else "second", .table_id = id, .storage_name = name }, .topology = .{ .create = .{ .expected_transition_generation = 0, .table = table, .ranges = &ranges } } };
        try store.applyStandaloneCommand(group, .{ .apply_system_catalog = try std.json.Stringify.valueAlloc(a, command, .{}) });
    }
}
const Fault = struct {
    txn: *docstore.DocStore.Txn,
    remaining: usize,
    pub fn get(self: *Fault, key: []const u8) ![]const u8 {
        return self.txn.get(key);
    }
    pub fn put(self: *Fault, key: []const u8, value: []const u8) !void {
        if (self.remaining == 0) return error.InjectedFailure;
        self.remaining -= 1;
        try self.txn.put(key, value);
    }
    pub fn delete(self: *Fault, key: []const u8) !void {
        if (self.remaining == 0) return error.InjectedFailure;
        self.remaining -= 1;
        try self.txn.delete(key);
    }
};

fn absent(store: *native.RaftApplyStore, lookup: ledger.Lookup) !void {
    var read = try store.store.beginReadTxn();
    defer read.abort();
    try std.testing.expect(try ledger.head(&read, lookup) == null);
    try std.testing.expect(try ledger.member(&read, group, .database, lookup.reference.database) == null);
}

test "SQL scope retirement journal atomically owns membership exact retries cold custody and terminal release" {
    const a = std.testing.allocator;
    var region: std.heap.ArenaAllocator = .init(a);
    defer region.deinit();
    const scratch = region.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(scratch, ".zig-cache/tmp/{s}/scope-retirement-journal", .{tmp.sub_path});
    var encoded: []const u8 = undefined;
    var expected: ledger.Head = undefined;
    for (0..4) |pass| {
        var store = try native.RaftApplyStore.init(a, .{ .root_dir = path });
        defer store.deinit();
        if (pass == 0) {
            try seed(&store, scratch);
            encoded = try store.prepareScopeRetirementManifestJsonWithContext(scratch, group, .{ .kind = .database, .name = "tenant_ops", .creator = .bootstrap }, .{});
        }
        var prepared = try ledger.prepare(a, group, @splat(6), encoded);
        defer prepared.deinit();
        if (pass == 0) {
            const puts = 5 + prepared.manifest.resources.len;
            for (0..puts) |fail_at| {
                var txn = try store.store.beginWriteTxn();
                var open = true;
                defer if (open) txn.abort();
                var fault: Fault = .{ .txn = &txn, .remaining = fail_at };
                try std.testing.expectError(error.InjectedFailure, ledger.stageBegin(&fault, &prepared));
                txn.abort();
                open = false;
                try absent(&store, prepared.lookup);
            }
            var txn = try store.store.beginWriteTxn();
            var committed = false;
            defer if (!committed) txn.abort();
            expected = try ledger.stageBegin(&txn, &prepared);
            try txn.commit();
            committed = true;
        } else {
            var txn = try store.store.beginWriteTxn();
            var committed = false;
            defer if (!committed) txn.abort();
            const observed = (try ledger.existing(&txn, &prepared)).?;
            try std.testing.expectEqualDeep(expected, observed);
            try std.testing.expectEqualDeep(expected, try ledger.stageBegin(&txn, &prepared));
            var other = try ledger.prepare(a, group, @splat(7), encoded);
            defer other.deinit();
            try std.testing.expectError(error.TableTransitionActive, ledger.stageBegin(&txn, &other));
            const next = switch (pass) {
                1 => try observed.progress.drained(),
                2 => try observed.progress.tablePublished(0),
                3 => try observed.progress.tablePublished(1),
                else => unreachable,
            };
            expected = try ledger.stageAdvance(&txn, &prepared, observed, next);
            try std.testing.expectError(error.CatalogGenerationChanged, ledger.stageAdvance(&txn, &prepared, observed, next));
            try txn.commit();
            committed = true;
        }
    }
    var store = try native.RaftApplyStore.init(a, .{ .root_dir = path });
    defer store.deinit();
    var prepared = try ledger.prepare(a, group, @splat(6), encoded);
    defer prepared.deinit();
    try std.testing.expectEqual(contract.Phase.publishing_catalog, expected.progress.phase);
    // The native apply owner separately verifies actual publication. Here the
    // journal only proves atomic terminal custody, not a fabricated owner ACK.
    const next = try expected.progress.published(prepared.manifest.catalog_revision + 1, prepared.manifest.catalog_revision);
    const writes = 4 + prepared.manifest.resources.len;
    for (0..writes) |fail_at| {
        var txn = try store.store.beginWriteTxn();
        var open = true;
        defer if (open) txn.abort();
        var fault: Fault = .{ .txn = &txn, .remaining = fail_at };
        try std.testing.expectError(error.InjectedFailure, ledger.stageAdvance(&fault, &prepared, expected, next));
        txn.abort();
        open = false;
        var read = try store.store.beginReadTxn();
        defer read.abort();
        try std.testing.expectEqualDeep(expected, (try ledger.existing(&read, &prepared)).?);
    }
    {
        var txn = try store.store.beginWriteTxn();
        var committed = false;
        defer if (!committed) txn.abort();
        expected = try ledger.stageAdvance(&txn, &prepared, expected, next);
        try txn.commit();
        committed = true;
    }
    var read = try store.store.beginReadTxn();
    defer read.abort();
    try std.testing.expectEqualDeep(expected, (try ledger.existing(&read, &prepared)).?);
    try std.testing.expect(try ledger.member(&read, group, .database, prepared.lookup.reference.database) == null);
    for (prepared.manifest.resources) |resource| try std.testing.expect(try ledger.member(&read, group, resource.kind, resource.id) == null);
}

test "SQL scope retirement journal rejects cross group replay and inconsistent queue counters" {
    const a = std.testing.allocator;
    var region: std.heap.ArenaAllocator = .init(a);
    defer region.deinit();
    const scratch = region.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(scratch, ".zig-cache/tmp/{s}/scope-retirement-corruption", .{tmp.sub_path});
    var store = try native.RaftApplyStore.init(a, .{ .root_dir = path });
    defer store.deinit();
    try seed(&store, scratch);
    const encoded = try store.prepareScopeRetirementManifestJsonWithContext(scratch, group, .{ .kind = .database, .name = "tenant_ops", .creator = .bootstrap }, .{});
    try std.testing.expectError(error.InvalidScopeRetirement, ledger.prepare(a, group + 1, @splat(6), encoded));
    var prepared = try ledger.prepare(a, group, @splat(6), encoded);
    defer prepared.deinit();
    var count: [8]u8 = undefined;
    std.mem.writeInt(u64, &count, 1, .little);
    const queued = try ledger.QueueRecord.init(prepared.lookup);
    var buffer: [256]u8 = undefined;
    {
        var txn = try store.store.beginWriteTxn();
        var committed = false;
        defer if (!committed) txn.abort();
        // Deliberately inconsistent recovery state: sequence 2 already owns
        // a queue row while the counter says 1. Never overwrite that work.
        try txn.put(try ledger.counterKey(&buffer, group), &count);
        try txn.put(try ledger.queueKey(&buffer, group, 2), queued.slice());
        try txn.commit();
        committed = true;
    }
    {
        var txn = try store.store.beginWriteTxn();
        defer txn.abort();
        try std.testing.expectError(error.InvalidScopeRetirement, ledger.stageBegin(&txn, &prepared));
    }
    try absent(&store, prepared.lookup);
    var read = try store.store.beginReadTxn();
    defer read.abort();
    try std.testing.expectEqualSlices(u8, &count, try read.get(try ledger.counterKey(&buffer, group)));
    try std.testing.expectEqualSlices(u8, queued.slice(), try read.get(try ledger.queueKey(&buffer, group, 2)));
}
