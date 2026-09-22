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
        if (update.writes.len == 0 and update.deletes.len == 0 and update.predicates.len == 0) continue;
        for (candidate.tables) |*table| if (std.mem.eql(u8, candidate.physicalName(table.table_name), update.table_name)) {
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
        const label = candidate.logicalName(update.table_name);
        // New cascade participants also receive a server-authored binding;
        // never reinterpret their physical identity as a public table name.
        try candidate.bind(alloc, label, update.table_name);
        // These are observations of the live rows used to derive the staged
        // result, not observations of the staged result itself. Keep them even
        // for read-only dependencies and across subsequent statements.
        var entry = [_]sessions.TableCommitRequest{.{
            .table_name = @constCast(label),
            .relational_schema_version = update.relational_schema_version,
            .schema_version = update.schema_version,
            .batch = .{ .writes = writes, .deletes = @constCast(update.deletes) },
            .predicates = .{ .items = @constCast(update.predicates), .capacity = update.predicates.len },
        }};
        var binding = [_]sessions.CatalogBinding{.{ .logical = label, .physical = update.table_name }};
        const request: sessions.OwnedTransactionCommitRequest = .{ .tables = &entry, .catalog_bindings = .{ .items = &binding, .capacity = binding.len } };
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
    try server.validateCommitTablesAgainstSchema(context, incoming);
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
    try server.authorizeAndBindIntegrityMutations(alloc, context, prepared.tables, candidate);
    try server.validateCommitTablesAgainstSchema(context, prepared.tables);
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

test "distributed txn session normalization coalesces catalog aliases and new cascade participants" {
    const alloc = std.testing.allocator;
    var candidate = try sessions.parseCommitRequest(alloc, "{\"read_set\":[],\"tables\":{\"parent\":{\"inserts\":{\"x\":{\"id\":1}}}}}");
    defer candidate.deinit(alloc);
    try candidate.bind(alloc, "parent", "table:1");
    try apply(alloc, &candidate, &.{
        .{ .table_name = "table:1", .writes = &.{.{ .key = "x", .value = "{\"id\":2}" }} },
        .{ .table_name = "table:2", .writes = &.{.{ .key = "y", .value = "{\"id\":2}" }} },
    });
    var next = try sessions.parseCommitRequest(alloc, "{\"read_set\":[],\"tables\":{\"renamed\":{\"inserts\":{\"z\":{\"id\":3}}},\"child\":{\"inserts\":{\"w\":{\"id\":3}}}}}");
    defer next.deinit(alloc);
    try next.bind(alloc, "renamed", "table:1");
    try next.bind(alloc, "child", "table:2");
    try candidate.mergeFrom(alloc, &next);
    const result = try candidate.distributedTables(alloc);
    defer alloc.free(result);
    try std.testing.expectEqual(@as(usize, 2), result.len);
    for (result) |table| try std.testing.expectEqual(@as(usize, 2), table.writes.len);
    try std.testing.expectEqualStrings("{\"id\":2}", candidate.tables[0].batch.writes[0].value);
}

test "distributed txn session normalization retains bounded physical observations including read-only dependencies" {
    const alloc = std.testing.allocator;
    var candidate = try sessions.parseCommitRequest(alloc, "{\"read_set\":[],\"tables\":{}}");
    defer candidate.deinit(alloc);
    const observations = [_]types.TransactionVersionPredicate{.{ .key = "child", .expected_version = 42, .expected_content_digest = @splat(7) }};
    const updates = [_]contract.TableCommitRequest{.{ .table_name = "table:child", .predicates = &observations }};
    for (0..100) |_| try apply(alloc, &candidate, &updates);
    var cloned = try candidate.clone(alloc);
    defer cloned.deinit(alloc);
    const tables = try cloned.distributedTables(alloc);
    defer alloc.free(tables);
    try std.testing.expectEqual(@as(usize, 1), tables.len);
    try std.testing.expectEqual(@as(usize, 1), tables[0].predicates.len);
    try std.testing.expectEqual(@as(u64, 42), tables[0].predicates[0].expected_version);
    try std.testing.expectEqualSlices(u8, &@as([32]u8, @splat(7)), &tables[0].predicates[0].expected_content_digest.?);
    try std.testing.expectError(error.VersionConflict, apply(alloc, &candidate, &.{.{ .table_name = "table:child", .predicates = &.{.{ .key = "child", .expected_version = 43 }} }}));
    try std.testing.expectError(error.VersionConflict, apply(alloc, &candidate, &.{.{ .table_name = "table:child", .predicates = &.{.{ .key = "child", .expected_version = 42, .expected_content_digest = @splat(8) }} }}));
}
