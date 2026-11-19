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

//! Statement validation and normalized read-your-writes staging. This is not
//! commit authority: the session seals the final guarded plan before 2PC.
const std = @import("std");
const sessions = @import("transactions.zig");
const integrity = @import("antfly_local_sources").api_relational_integrity_commit;
const contract = @import("antfly_local_sources").api_distributed_txn_contract;
const types = @import("antfly_local_sources").storage_db_types;

/// Statement-only trigger effects have no row key to create a table entry.
/// Retain their exact server-resolved physical binding in the same durable
/// session candidate; the coordinator later enlists one native owner.
pub fn enlistStatementTables(alloc: std.mem.Allocator, candidate: *sessions.OwnedTransactionCommitRequest, tables: []const contract.TableCommitRequest) !void {
    for (tables) |table| {
        var entry = [_]sessions.TableCommitRequest{.{ .table_name = @constCast(table.table_name), .schema_version = table.schema_version, .relational_schema_version = table.relational_schema_version }};
        var bindings = [_]sessions.CatalogBinding{.{ .logical = table.table_name, .physical = table.table_name, .sql_persistence = table.sql_persistence }};
        const header: sessions.OwnedTransactionCommitRequest = .{ .tables = &entry, .catalog_bindings = .{ .items = &bindings, .capacity = bindings.len, .pointer_stability = .{} } };
        try candidate.mergeFrom(alloc, &header);
    }
}

/// Retroactive timing validation observes existing staged rows; it does not
/// normalize or replace them. Its result can borrow candidate row storage.
/// Retain only physical observations so validation neither recopies the batch
/// nor frees row bytes still referenced by the validation result.
pub fn retainObservations(alloc: std.mem.Allocator, candidate: *sessions.OwnedTransactionCommitRequest, updates: []const contract.TableCommitRequest) !void {
    const observations = try alloc.dupe(contract.TableCommitRequest, updates);
    defer alloc.free(observations);
    for (observations) |*table| {
        table.writes = &.{};
        table.deletes = &.{};
    }
    try apply(alloc, candidate, observations);
}

pub fn apply(alloc: std.mem.Allocator, candidate: *sessions.OwnedTransactionCommitRequest, updates: []const contract.TableCommitRequest) !void {
    for (updates) |update| {
        if (update.writes.len == 0 and update.deletes.len == 0 and update.predicates.len == 0) continue;
        const label = candidate.logicalName(update.table_name);
        // Preparation may project row effects without repeating the immutable
        // descriptor already authenticated and bound on this candidate.
        const persistence = for (candidate.catalog_bindings.items) |binding| {
            if (std.mem.eql(u8, binding.logical, label)) break binding.sql_persistence;
        } else update.sql_persistence;
        if (update.sql_persistence != null and !std.meta.eql(update.sql_persistence, persistence)) return error.CatalogGenerationChanged;
        try candidate.bindWithPersistence(alloc, label, update.table_name, persistence);
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
                for (write.json_null_fields) |field| alloc.free(field);
                if (write.json_null_fields.len != 0) alloc.free(write.json_null_fields);
            };
            for (table.batch.deletes) |key| if (changes(update, key)) alloc.free(key);
            if (table.batch.writes.len != 0) alloc.free(table.batch.writes);
            if (table.batch.deletes.len != 0) alloc.free(table.batch.deletes);
            table.batch.writes = next_writes;
            table.batch.deletes = next_deletes;
            transferred = true;
        };
        const writes = try alloc.alloc(types.BatchWrite, update.writes.len);
        defer alloc.free(writes);
        for (writes, update.writes) |*out, input| out.* = .{ .key = input.key, .value = input.value, .json_null_fields = input.json_null_fields };
        // These are observations of the live rows used to derive the staged
        // result, not observations of the staged result itself. Keep them even
        // for read-only dependencies and across subsequent statements.
        const authorization = for (candidate.tables) |table| {
            if (std.mem.eql(u8, candidate.physicalName(table.table_name), update.table_name)) break table.sql_authorization;
        } else null;
        var entry = [_]sessions.TableCommitRequest{.{
            .sql_authorization = authorization,
            .table_name = @constCast(label),
            .relational_schema_version = update.relational_schema_version,
            .schema_version = update.schema_version,
            .batch = .{ .writes = writes, .deletes = @constCast(update.deletes) },
            .predicates = .{ .items = @constCast(update.predicates), .capacity = update.predicates.len, .pointer_stability = .{} },
        }};
        var binding = [_]sessions.CatalogBinding{.{ .logical = label, .physical = update.table_name, .sql_persistence = persistence }};
        const request: sessions.OwnedTransactionCommitRequest = .{ .tables = &entry, .catalog_bindings = .{ .items = &binding, .capacity = binding.len, .pointer_stability = .{} } };
        try candidate.mergeFrom(alloc, &request);
    }
}

fn changes(update: contract.TableCommitRequest, key: []const u8) bool {
    for (update.writes) |write| if (std.mem.eql(u8, write.key, key)) return true;
    for (update.deletes) |deleted| if (std.mem.eql(u8, deleted, key)) return true;
    return false;
}

pub fn validate(server: anytype, alloc: std.mem.Allocator, previous: ?*const sessions.OwnedTransactionCommitRequest, candidate: *sessions.OwnedTransactionCommitRequest, statement: *const sessions.OwnedTransactionCommitRequest, context: @import("antfly_local_sources").api_operation.RequestContext) !void {
    return validateWithEvents(server, alloc, previous, candidate, statement, &.{}, context);
}

pub fn validateWithEvents(server: anytype, alloc: std.mem.Allocator, previous: ?*const sessions.OwnedTransactionCommitRequest, candidate: *sessions.OwnedTransactionCommitRequest, statement: *const sessions.OwnedTransactionCommitRequest, events: []const contract.TableCommitRequest, context: @import("antfly_local_sources").api_operation.RequestContext) !void {
    var original = try statement.clone(alloc);
    defer original.deinit(alloc);
    const incoming_view = try original.distributedTables(alloc);
    defer incoming_view.deinit();
    const incoming = incoming_view.tables;
    try server.validateCommitTablesAgainstSchema(context, incoming);
    const selected = (try server.selectIntegritySnapshot(alloc, incoming, context)) orelse return;
    var snapshot = selected.snapshot;
    defer server.source.freeAdminSnapshot(&snapshot);
    if (!selected.coordinated) return;
    var before = if (previous) |value| try value.clone(alloc) else sessions.OwnedTransactionCommitRequest{};
    defer before.deinit(alloc);
    const staged_view = try before.distributedTables(alloc);
    defer staged_view.deinit();
    const staged = staged_view.tables;
    var prepared = (if (events.len == 0) integrity.prepareSessionStatementWithTiming(alloc, server.table_reads orelse return error.IntegrityCatalogUnavailable, snapshot.tables, snapshot.ranges, staged, incoming, context, candidate.constraint_timing.items) else integrity.prepareSessionCommandWithTiming(alloc, server.table_reads orelse return error.IntegrityCatalogUnavailable, snapshot.tables, snapshot.ranges, staged, incoming, events, context, candidate.constraint_timing.items)) catch |err| refreshed: {
        // This is read-only preparation of an unpublished session candidate,
        // not a retry of staging or commit. Share REST's bounded authoritative
        // refresh when a data-node metadata cache trails a completed DDL.
        if (err != error.PreparedGenerationChanged or context.table_triggers != null) return err;
        const authoritative = try server.authoritativeIntegritySnapshot(context);
        server.source.freeAdminSnapshot(&snapshot);
        snapshot = authoritative;
        if (!try integrity.metadataRequiresCoordination(alloc, snapshot.tables, incoming)) return;
        break :refreshed try (if (events.len == 0) integrity.prepareSessionStatementWithTiming(alloc, server.table_reads orelse return error.IntegrityCatalogUnavailable, snapshot.tables, snapshot.ranges, staged, incoming, context, candidate.constraint_timing.items) else integrity.prepareSessionCommandWithTiming(alloc, server.table_reads orelse return error.IntegrityCatalogUnavailable, snapshot.tables, snapshot.ranges, staged, incoming, events, context, candidate.constraint_timing.items));
    };
    defer prepared.deinit();
    try server.authorizeAndBindIntegrityMutations(alloc, context, prepared.tables, candidate);
    try server.validateCommitTablesAgainstSchema(context, prepared.tables);
    try apply(alloc, candidate, prepared.tables);
}

test "distributed txn session timing observations preserve borrowed staged row ownership" {
    const alloc = std.testing.allocator;
    var candidate = try sessions.parseCommitRequest(alloc, "{\"read_set\":[],\"tables\":{\"p\":{\"inserts\":{\"x\":{\"id\":1}}}}}");
    defer candidate.deinit(alloc);
    const borrowed_view = try candidate.distributedTables(alloc);
    defer borrowed_view.deinit();
    const borrowed = borrowed_view.tables;
    const original_value = candidate.tables[0].batch.writes[0].value;
    const predicates = [_]types.TransactionVersionPredicate{.{ .key = "x", .expected_version = 7 }};
    borrowed[0].predicates = &predicates;
    try retainObservations(alloc, &candidate, borrowed);
    try std.testing.expectEqual(@as(usize, 1), candidate.tables[0].batch.writes.len);
    try std.testing.expectEqual(original_value.ptr, candidate.tables[0].batch.writes[0].value.ptr);
    try std.testing.expectEqualStrings("{\"id\":1}", candidate.tables[0].batch.writes[0].value);
    try std.testing.expectEqual(@as(usize, 1), candidate.tables[0].predicates.items.len);
    try std.testing.expectEqual(@as(u64, 7), candidate.tables[0].predicates.items[0].expected_version);
}

test "distributed txn session statement normalization replaces old deletes and cascaded values" {
    const alloc = std.testing.allocator;
    var candidate = try sessions.parseCommitRequest(alloc, "{\"read_set\":[],\"tables\":{\"p\":{\"deletes\":[\"x\"]},\"c\":{\"inserts\":{\"y\":{\"id\":2}}}}}");
    defer candidate.deinit(alloc);
    try apply(alloc, &candidate, &.{ .{ .table_name = "p", .writes = &.{.{ .key = "x", .value = "{\"id\":3}" }} }, .{ .table_name = "c", .writes = &.{.{ .key = "y", .value = "{\"id\":3}" }} } });
    const result_view = try candidate.distributedTables(alloc);
    defer result_view.deinit();
    const result = result_view.tables;
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
    const result_view = try candidate.distributedTables(alloc);
    defer result_view.deinit();
    const result = result_view.tables;
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
    const tables_view = try cloned.distributedTables(alloc);
    defer tables_view.deinit();
    const tables = tables_view.tables;
    try std.testing.expectEqual(@as(usize, 1), tables.len);
    try std.testing.expectEqual(@as(usize, 1), tables[0].predicates.len);
    try std.testing.expectEqual(@as(u64, 42), tables[0].predicates[0].expected_version);
    try std.testing.expectEqualSlices(u8, &@as([32]u8, @splat(7)), &tables[0].predicates[0].expected_content_digest.?);
    try std.testing.expectError(error.VersionConflict, apply(alloc, &candidate, &.{.{ .table_name = "table:child", .predicates = &.{.{ .key = "child", .expected_version = 43 }} }}));
    try std.testing.expectError(error.VersionConflict, apply(alloc, &candidate, &.{.{ .table_name = "table:child", .predicates = &.{.{ .key = "child", .expected_version = 42, .expected_content_digest = @splat(8) }} }}));
}

test "SQL privileges integrity normalization preserves admitted intent and discovered native participants" {
    const a = std.testing.allocator;
    const Access = @import("antfly_local_sources").system_catalog_privileges.Access;
    var candidate = try sessions.parseCommitRequest(a, "{\"read_set\":[],\"tables\":{\"docs\":{\"inserts\":{\"a\":{\"id\":1}}}}}");
    defer candidate.deinit(a);
    try candidate.bind(a, "docs", "table:9");
    candidate.tables[0].sql_authorization = .{ .row_filter_digest = @splat(0), .principal_instance = @splat(7), .table_id = 9, .required = Access.bit(.insert), .native_rights = 0 };
    const updates = [_]contract.TableCommitRequest{
        .{ .table_name = "table:9", .writes = &.{.{ .key = "a", .value = "{\"id\":1,\"default\":true}" }} },
        .{ .table_name = "other", .writes = &.{.{ .key = "b", .value = "{}" }} },
    };
    try apply(a, &candidate, &updates);
    try std.testing.expectEqual(@as(usize, 2), candidate.tables.len);
    try std.testing.expectEqual(Access.bit(.insert), candidate.tables[0].sql_authorization.?.required);
    try std.testing.expect(!candidate.tables[0].sql_authorization.?.native_write);
    try std.testing.expect(candidate.tables[1].sql_authorization == null);
}

test "SQL relation lifetime statement normalization and savepoint observations preserve native binding and reject image substitution" {
    const a = std.testing.allocator;
    const P = @import("antfly_local_sources").common_topology_records.RelationPersistence;
    var candidate = try sessions.parseCommitRequest(a, "{\"read_set\":[],\"tables\":{\"docs\":{\"inserts\":{\"a\":{\"id\":1}}}}}");
    defer candidate.deinit(a);
    const p: P.Descriptor = .{ .kind = .unlogged, .owner_node_id = 7, .owner_store_id = 8, .generation_instance = @splat(9), .data_generation = 10 };
    try candidate.bindWithPersistence(a, "docs", "table:17", p);
    var saved = try candidate.clone(a);
    defer saved.deinit(a);
    // The real preparation output may omit a repeated descriptor; the
    // authenticated candidate binding remains the identity of these effects.
    try apply(a, &candidate, &.{.{ .table_name = "table:17", .writes = &.{.{ .key = "a", .value = "{\"id\":2}" }} }});
    try std.testing.expectEqualStrings("{\"id\":2}", candidate.tables[0].batch.writes[0].value);
    const ranges = @import("antfly_local_sources").api_range_read_guards;
    const observation: ranges.OwnerRangeProof = .{ .fence = .{ .metadata_group_id = 1, .metadata_incarnation = @splat('1'), .catalog_revision = 2, .table_id = 17, .topology_epoch = 3, .route = .{ .group_id = 18, .range_id = 19, .identity_namespace = .{ .table_id = 17, .shard_id = 18, .range_id = 19 }, .execution = .{ .native_local = p } }, .native_participant = .{ .physical_root_incarnation = 20, .table_id = 17, .shard_id = 18, .range_id = 19, .data_generation = p.data_generation } }, .proofs = &.{.{ .bucket = 1, .generation = 2 }} };
    try candidate.tables[0].mergeRangeGuards(a, &.{observation});
    try saved.retainRangeGuards(a, &candidate);
    try std.testing.expectEqualStrings("{\"id\":1}", saved.tables[0].batch.writes[0].value);
    try std.testing.expectEqual(@as(usize, 1), saved.tables[0].range_guards.?.value.len);
    var cloned = try saved.clone(a);
    defer cloned.deinit(a);
    const view = try cloned.distributedTables(a);
    defer view.deinit();
    try std.testing.expect(view.tables[0].sql_persistence.?.eql(p));
    try std.testing.expect(view.tables[0].range_guards[0].fence.route.execution.native_local.eql(p));
    var changed = p;
    changed.data_generation += 1;
    try std.testing.expectError(error.CatalogGenerationChanged, apply(a, &candidate, &.{.{ .table_name = "table:17", .sql_persistence = changed, .writes = &.{.{ .key = "a", .value = "{\"id\":99}" }} }}));
    try std.testing.expectEqualStrings("{\"id\":2}", candidate.tables[0].batch.writes[0].value);
    var substituted = try candidate.clone(a);
    defer substituted.deinit(a);
    substituted.catalog_bindings.items[0].sql_persistence = changed;
    try std.testing.expectError(error.CatalogGenerationChanged, saved.retainRangeGuards(a, &substituted));
    try std.testing.expect(saved.catalog_bindings.items[0].sql_persistence.?.eql(p));
    // Physical participants coalesce across logical aliases, including a
    // rename after a savepoint. A new spelling cannot replace their image.
    try saved.bindWithPersistence(a, "renamed", "table:17", p);
    const binding_count = saved.catalog_bindings.items.len;
    try std.testing.expectError(error.CatalogGenerationChanged, saved.bindWithPersistence(a, "foreign", "table:17", changed));
    try std.testing.expectEqual(binding_count, saved.catalog_bindings.items.len);
    var renamed = try sessions.parseCommitRequest(a, "{\"read_set\":[],\"tables\":{\"renamed-after-savepoint\":{}}}");
    defer renamed.deinit(a);
    try renamed.bindWithPersistence(a, "renamed-after-savepoint", "table:17", changed);
    var changed_observation = observation;
    changed_observation.fence.route.execution.native_local = changed;
    changed_observation.fence.native_participant.?.data_generation = changed.data_generation;
    try renamed.tables[0].mergeRangeGuards(a, &.{changed_observation});
    try std.testing.expectError(error.CatalogGenerationChanged, saved.retainRangeGuards(a, &renamed));
    try std.testing.expectEqual(binding_count, saved.catalog_bindings.items.len);
    try std.testing.expectEqualStrings("{\"id\":1}", saved.tables[0].batch.writes[0].value);
    try std.testing.expectError(error.CatalogGenerationChanged, candidate.bind(a, "docs", "table:17"));
}
