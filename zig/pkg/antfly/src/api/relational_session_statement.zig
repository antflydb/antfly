// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Statement validation and normalized read-your-writes staging. This is not
//! commit authority: the session seals the final guarded plan before 2PC.
const std = @import("std");
const sessions = @import("transactions.zig");
const integrity = @import("relational_integrity_commit.zig");
const contract = @import("distributed_txn_contract.zig");
const types = @import("../storage/db/types.zig");

pub fn apply(alloc: std.mem.Allocator, candidate: *sessions.OwnedTransactionCommitRequest, updates: []const contract.TableCommitRequest) !void {
    for (updates) |update| {
        if (update.writes.len == 0 and update.deletes.len == 0) continue;
        for (candidate.tables) |*table| if (std.mem.eql(u8, table.table_name, update.table_name)) {
            var writes: std.ArrayList(types.BatchWrite) = .empty;
            defer writes.deinit(alloc);
            var deletes: std.ArrayList([]const u8) = .empty;
            defer deletes.deinit(alloc);
            // Allocate both replacement containers before freeing anything.
            try writes.ensureTotalCapacity(alloc, table.batch.writes.len);
            try deletes.ensureTotalCapacity(alloc, table.batch.deletes.len);
            for (table.batch.writes) |write| {
                if (!changes(update, write.key)) writes.appendAssumeCapacity(write);
            }
            for (table.batch.deletes) |key| {
                if (!changes(update, key)) deletes.appendAssumeCapacity(key);
            }
            const next_writes = try writes.toOwnedSlice(alloc);
            var transferred = false;
            errdefer if (!transferred) alloc.free(next_writes);
            const next_deletes = try deletes.toOwnedSlice(alloc);
            for (table.batch.writes) |write| if (changes(update, write.key)) {
                alloc.free(write.key);
                alloc.free(write.value);
            };
            for (table.batch.deletes) |key| if (changes(update, key)) alloc.free(key);
            if (table.batch.writes.len != 0) alloc.free(table.batch.writes);
            if (table.batch.deletes.len != 0) alloc.free(table.batch.deletes);
            table.batch.writes = next_writes;
            table.batch.deletes = next_deletes;
            transferred = true;
            if (table.txn_writes.len != 0) alloc.free(table.txn_writes);
            table.txn_writes = &.{};
        };
        const writes = try alloc.alloc(types.BatchWrite, update.writes.len);
        defer alloc.free(writes);
        for (writes, update.writes) |*out, input| out.* = .{ .key = input.key, .value = input.value };
        var entry = [_]sessions.TableCommitRequest{.{ .table_name = @constCast(update.table_name), .batch = .{ .writes = writes, .deletes = @constCast(update.deletes) } }};
        const request: sessions.OwnedTransactionCommitRequest = .{ .tables = &entry };
        try candidate.mergeFrom(alloc, &request);
    }
}

fn changes(update: contract.TableCommitRequest, key: []const u8) bool {
    for (update.writes) |write| if (std.mem.eql(u8, write.key, key)) return true;
    for (update.deletes) |deleted| if (std.mem.eql(u8, deleted, key)) return true;
    return false;
}

pub fn validate(server: anytype, alloc: std.mem.Allocator, previous: ?*const sessions.OwnedTransactionCommitRequest, candidate: *sessions.OwnedTransactionCommitRequest, statement: *const sessions.OwnedTransactionCommitRequest, context: @import("operation.zig").RequestContext) !void {
    var original = try statement.clone(alloc);
    defer original.deinit(alloc);
    const incoming = try original.distributedTables(alloc);
    defer alloc.free(incoming);
    try server.validateCommitTablesAgainstSchema(incoming);
    var snapshot = (try server.source.adminSnapshot()) orelse {
        _ = try integrity.metadataRequiresCoordination(alloc, null, incoming);
        return;
    };
    defer server.source.freeAdminSnapshot(&snapshot);
    if (!try integrity.metadataRequiresCoordination(alloc, snapshot.tables, incoming)) return;
    var before = if (previous) |value| try value.clone(alloc) else sessions.OwnedTransactionCommitRequest{};
    defer before.deinit(alloc);
    const staged = try before.distributedTables(alloc);
    defer alloc.free(staged);
    var prepared = try integrity.prepareSessionStatement(alloc, server.table_reads orelse return error.IntegrityCatalogUnavailable, snapshot.tables, snapshot.ranges, staged, incoming, context);
    defer prepared.deinit();
    try integrity.authorizePrimaryMutations(context, server.cfg.auth_enabled, prepared.tables);
    try server.validateCommitTablesAgainstSchema(prepared.tables);
    try apply(alloc, candidate, prepared.tables);
}

test "distributed txn session statement normalization replaces old deletes and cascaded values" {
    const alloc = std.testing.allocator;
    var candidate = try sessions.parseCommitRequest(alloc, "{\"read_set\":[],\"tables\":{\"p\":{\"deletes\":[\"x\"]},\"c\":{\"inserts\":{\"y\":{\"id\":2}}}}}");
    defer candidate.deinit(alloc);
    try apply(alloc, &candidate, &.{ .{ .table_name = "p", .writes = &.{.{ .key = "x", .value = "{\"id\":3}" }} }, .{ .table_name = "c", .writes = &.{.{ .key = "y", .value = "{\"id\":3}" }} } });
    const result = try candidate.distributedTables(alloc);
    defer alloc.free(result);
    for (result) |table| {
        try std.testing.expectEqual(@as(usize, 0), table.deletes.len);
        try std.testing.expectEqual(@as(usize, 1), table.writes.len);
        try std.testing.expectEqualStrings("{\"id\":3}", table.writes[0].value);
    }
}
