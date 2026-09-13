// Copyright 2026 Antfly, Inc.
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
const std = @import("std");
const db_mod = @import("db.zig");
const integrity = @import("relational_integrity.zig");
const catalog = @import("relational_integrity_catalog.zig");
const tuples = @import("relational_index_keys.zig");

fn binding(db: *db_mod.DB, kind: catalog.Kind, name: []const u8) !integrity.Generation {
    const alloc = std.testing.allocator;
    const raw = try db.core.getStoreValue(alloc, catalog.key) orelse return error.MissingIntegrityCatalog;
    defer alloc.free(raw);
    var loaded = try catalog.decode(alloc, raw);
    defer loaded.deinit();
    return (loaded.find(kind, name) orelse return error.MissingIntegrityBinding).generation;
}

fn generationSet(db: *db_mod.DB) ![32]u8 {
    const alloc = std.testing.allocator;
    const raw = try db.core.getStoreValue(alloc, catalog.key) orelse return error.MissingIntegrityCatalog;
    defer alloc.free(raw);
    var loaded = try catalog.decode(alloc, raw);
    defer loaded.deinit();
    return @import("relational_integrity_activation.zig").generationSet(loaded);
}

test "relational integrity DB transactions atomically preserve cross-table parent dependencies" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var parent_path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const parent_path = try std.fmt.bufPrint(&parent_path_buf, ".zig-cache/tmp/{s}/parent", .{tmp.sub_path});
    var child_path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const child_path = try std.fmt.bufPrint(&child_path_buf, ".zig-cache/tmp/{s}/child", .{tmp.sub_path});
    var parent = try db_mod.DB.open(alloc, parent_path, .{ .start_optional_runtimes = false, .start_index_workers = false, .identity_namespace = .{ .table_id = 100, .shard_id = 101 }, .primary_backend = .{ .lsm = .{} } });
    defer parent.close();
    var child = try db_mod.DB.open(alloc, child_path, .{ .start_optional_runtimes = false, .start_index_workers = false, .identity_namespace = .{ .table_id = 200, .shard_id = 201 }, .primary_backend = .{ .lsm = .{} } });
    defer child.close();
    try parent.setSchemaJson(alloc,
        \\{"version":1,"storage_mode":"relational","default_type":"row","unique_constraints":[{"name":"parent_pk","columns":["id"]}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"}},"additionalProperties":false}}}}
    );
    try child.setSchemaJson(alloc,
        \\{"version":1,"storage_mode":"relational","default_type":"row","foreign_keys":[{"name":"parent_fk","child_columns":["parent_id"],"parent_table":"parents","parent_columns":["id"]}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"},"parent_id":{"type":"integer"}},"additionalProperties":false}}}}
    );
    var portable: std.ArrayList(u8) = .empty;
    defer portable.deinit(alloc);
    try std.testing.expectError(error.CoordinatedConstraintPortableBackupUnsupported, @import("../portable_backup.zig").exportPortable(alloc, parent.core.store, &portable));
    try std.testing.expectEqual(@as(usize, 0), portable.items.len);
    var view = parent.core.acquireSchemaView().?;
    defer view.release();
    var plan = try tuples.TuplePlan.init(alloc, view.tableSchema().*, view.physicalLayout(), &.{.{ .column = "id" }});
    defer plan.deinit();
    var tuple: std.ArrayList(u8) = .empty;
    defer tuple.deinit(alloc);
    _ = try plan.appendValues(alloc, &tuple, &.{.{ .integer = 1 }});
    const address = try integrity.Address.init(try binding(&parent, .unique, "parent_pk"), tuple.items);
    const claim: integrity.Claim = .{ .tuple = tuple.items, .parent_table = "parents", .parent_key = "p", .schema_version = 1 };
    const reference: integrity.Reference = .{ .child_table = "children", .child_key = "c", .constraint_name = "parent_fk", .constraint_generation = try binding(&child, .foreign_key, "parent_fk") };
    const create = try parent.beginTransactionWithId(@splat(11), 100);
    // A client schema epoch is not evidence that the internal FK planner ran.
    try std.testing.expectError(error.ForeignKeyCoordinationRequired, parent.writeTransaction(create, .{
        .relational_schema_version = 1,
        .writes = &.{.{ .key = "p", .value = "{\"id\":1}" }},
    }));
    try std.testing.expectError(error.IntegrityCatalogChanged, parent.writeTransaction(create, .{
        .relational_schema_version = 1,
        .relational_integrity_generation_set = @splat(0),
        .writes = &.{.{ .key = "p", .value = "{\"id\":1}" }},
    }));
    try parent.writeTransaction(create, .{
        .relational_schema_version = 1,
        .relational_integrity_generation_set = try generationSet(&parent),
        .writes = &.{.{ .key = "p", .value = "{\"id\":1}" }},
        .integrity_commands = &.{.{ .address = address, .operation = .{ .establish = claim } }},
    });
    // No primary row or claim is visible before terminal resolution.
    try std.testing.expect((try parent.lookup(alloc, "p", .{})) == null);
    try parent.commitTransaction(create, 200);

    const attach = try parent.beginTransactionWithId(@splat(12), 300);
    _ = try child.beginTransactionWithId(attach, 300);
    const parent_prepare: @import("types.zig").TransactionIntentRequest = .{
        .relational_schema_version = 1,
        .integrity_commands = &.{.{ .address = address, .operation = .{ .attach = reference } }},
    };
    try parent.writeTransaction(attach, parent_prepare);
    try parent.writeTransaction(attach, parent_prepare); // cumulative prepare retry is idempotent
    try child.writeTransaction(attach, .{ .relational_schema_version = 1, .relational_integrity_generation_set = try generationSet(&child), .writes = &.{.{ .key = "c", .value = "{\"id\":2,\"parent_id\":1}" }} });
    const remove = try parent.beginTransactionWithId(@splat(13), 400);
    const release: @import("types.zig").TransactionIntentRequest = .{
        .relational_schema_version = 1,
        .relational_integrity_generation_set = try generationSet(&parent),
        .deletes = &.{"p"},
        .integrity_commands = &.{.{ .address = address, .operation = .{ .release = .{ .parent_table = "parents", .parent_key = "p" } } }},
    };
    try std.testing.expectError(error.IntentConflict, parent.writeTransaction(remove, release));
    try parent.commitTransaction(attach, 500);
    try child.commitTransaction(attach, 500);
    try std.testing.expectError(error.ForeignKeyReferenced, parent.writeTransaction(remove, release));
    try parent.abortTransaction(remove, 600);

    const detach = try parent.beginTransactionWithId(@splat(14), 700);
    _ = try child.beginTransactionWithId(detach, 700);
    try parent.writeTransaction(detach, .{ .relational_schema_version = 1, .integrity_commands = &.{.{ .address = address, .operation = .{ .detach = reference } }} });
    try child.writeTransaction(detach, .{ .relational_schema_version = 1, .relational_integrity_generation_set = try generationSet(&child), .deletes = &.{"c"} });
    try child.commitTransaction(detach, 800);
    try parent.commitTransaction(detach, 800);
    const finish = try parent.beginTransactionWithId(@splat(15), 900);
    try parent.writeTransaction(finish, release);
    try parent.commitTransaction(finish, 1000);
    try std.testing.expect((try parent.lookup(alloc, "p", .{})) == null);
    try std.testing.expect((try child.lookup(alloc, "c", .{})) == null);
    var read = try parent.core.store.beginReadTxn();
    defer read.abort();
    try std.testing.expectError(error.NotFound, read.get(&address.claimKey()));
    try std.testing.expectError(error.NotFound, read.get(&(try reference.key(address))));
}

fn activateOnePage(db: *db_mod.DB, txn_byte: u8) !bool {
    const activation = @import("relational_integrity_activation.zig");
    const types = @import("types.zig");
    const alloc = std.testing.allocator;
    var page = (try activation.Page.prepare(alloc, null, db.core, &.{"id"}, .{ .rows = 1, .records = 64 })) orelse return true;
    defer page.deinit();
    const id = try binding(db, .unique, "id_unique");
    var view = db.core.acquireSchemaView().?;
    defer view.release();
    var plan = try tuples.TuplePlan.init(alloc, view.tableSchema().*, view.physicalLayout(), &.{.{ .column = "id" }});
    defer plan.deinit();
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const owned = arena.allocator();
    const commands = try owned.alloc(integrity.Command, page.rows.rows.len);
    const predicates = try owned.alloc(types.TransactionVersionPredicate, page.rows.rows.len);
    for (page.rows.rows, commands, predicates) |row, *command, *predicate| {
        var parsed = try std.json.parseFromSlice(std.json.Value, alloc, row.json, .{});
        defer parsed.deinit();
        var tuple: std.ArrayList(u8) = .empty;
        _ = try plan.appendValues(owned, &tuple, &.{.{ .integer = parsed.value.object.get("id").?.integer }});
        command.* = .{ .address = try integrity.Address.init(id, tuple.items), .operation = .{ .establish = .{ .tuple = tuple.items, .parent_table = "parents", .parent_key = row.key, .schema_version = view.version() } } };
        predicate.* = .{ .key = row.key, .expected_version = row.version };
    }
    const transaction = try db.beginTransactionWithId(@splat(txn_byte), 500);
    try db.writeTransaction(transaction, .{ .relational_schema_version = view.version(), .integrity_commands = commands, .predicates = predicates, .relational_activation = page.command });
    try db.commitTransaction(transaction, 600);
    return page.progress.state == .enforced;
}

test "relational integrity historical restore stays fenced while coherent HA seed preserves namespace" {
    const alloc = std.testing.allocator;
    const lifecycle = @import("generation_lifecycle.zig");
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const source_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/source", .{tmp.sub_path});
    defer alloc.free(source_path);
    const target_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/target", .{tmp.sub_path});
    defer alloc.free(target_path);
    const snapshot_path = try std.fmt.allocPrint(alloc, "{s}.snapshots/seed", .{source_path});
    defer alloc.free(snapshot_path);
    const namespace = @import("doc_identity.zig").Namespace{ .table_id = 400, .shard_id = 401, .range_id = 402 };
    const options: db_mod.OpenOptions = .{ .identity_namespace = namespace, .start_optional_runtimes = false, .start_index_workers = false, .primary_backend = .{ .lsm = .{} } };
    {
        var source = try db_mod.DB.open(alloc, source_path, options);
        defer source.close();
        try source.setSchemaJson(alloc,
            \\{"version":1,"storage_mode":"relational","default_type":"row","unique_constraints":[{"name":"id_unique","columns":["id"]}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"}},"additionalProperties":false}}}}
        );
        try std.testing.expectError(error.CoordinatedConstraintTopologyUnsupported, source.split(.{ .start = "", .end = "" }, "m", "", target_path, true));
        try std.testing.expectError(error.CoordinatedConstraintTopologyUnsupported, source.finalizeSplit(.{ .start = "", .end = "m" }));
        _ = try source.snapshot("seed");
    }
    var transition = try lifecycle.beginProcessExclusiveWithRuntime(target_path, null);
    defer transition.deinit();
    var staged = try transition.beginStaging();
    defer staged.deinit();
    try std.testing.expectError(error.CoordinatedConstraintRestoreRequired, db_mod.DB.restoreSnapshotToStagedGeneration(&staged, alloc, snapshot_path, staged.path(), options));
    var wrong = namespace;
    wrong.table_id += 1;
    try std.testing.expectError(error.IdentityNamespaceMismatch, db_mod.DB.restoreCoherentHASeedReplicaToStagedGeneration(&staged, alloc, snapshot_path, staged.path(), options, wrong));
    try db_mod.DB.restoreCoherentHASeedReplicaToStagedGeneration(&staged, alloc, snapshot_path, staged.path(), options, namespace);
    var staged_options = options;
    staged_options.staged_generation = &staged;
    var restored = try db_mod.DB.open(alloc, staged.path(), staged_options);
    defer restored.close();
    try std.testing.expect(restored.core.identity_namespace.eql(namespace));
    _ = try binding(&restored, .unique, "id_unique");
}

test "relational integrity DB activation backfills atomically gates writers and resumes after restart" {
    const alloc = std.testing.allocator;
    const activation = @import("relational_integrity_activation.zig");
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}/database", .{tmp.sub_path});
    const options: db_mod.OpenOptions = .{ .start_optional_runtimes = false, .start_index_workers = false, .identity_namespace = .{ .table_id = 300, .shard_id = 301 }, .primary_backend = .{ .lsm = .{} } };
    {
        var db = try db_mod.DB.open(alloc, path, options);
        defer db.close();
        try db.setSchemaJson(alloc,
            \\{"version":1,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"}},"additionalProperties":false}}}}
        );
        try db.batch(.{ .timestamp_ns = 100, .writes = &.{ .{ .key = "a", .value = "{\"id\":1}" }, .{ .key = "b", .value = "{\"id\":2}" } } });
        try db.setSchemaJson(alloc,
            \\{"version":2,"storage_mode":"relational","default_type":"row","unique_constraints":[{"name":"id_unique","columns":["id"]}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"}},"additionalProperties":false}}}}
        );
        {
            var oversized = (try activation.Page.prepare(alloc, null, db.core, &.{"id"}, .{ .output_bytes = 1 })).?;
            defer oversized.deinit();
            try std.testing.expectEqual(.invalid, oversized.progress.state);
            try std.testing.expectEqualStrings("RelationalRowResultTooLarge", oversized.progress.failure);
            try std.testing.expectEqual(@as(usize, 0), oversized.rows.rows.len);
            try std.testing.expectEqual(@as(usize, 0), oversized.progress.cursor.len);
            try std.testing.expectEqual(@as(u64, 0), oversized.progress.rows_scanned);
            const failure_txn = try db.beginTransactionWithId(@splat(50), 200);
            try db.writeTransaction(failure_txn, .{ .relational_schema_version = 2, .relational_activation = oversized.command });
            try db.abortTransaction(failure_txn, 250);
        }
        const blocked = try db.beginTransactionWithId(@splat(51), 300);
        try std.testing.expectError(error.ConstraintActivationInProgress, db.writeTransaction(blocked, .{ .relational_schema_version = 2, .relational_integrity_generation_set = try generationSet(&db), .writes = &.{.{ .key = "c", .value = "{\"id\":3}" }} }));
        try db.abortTransaction(blocked, 400);
        try std.testing.expect(!try activateOnePage(&db, 52));
    }
    {
        var db = try db_mod.DB.open(alloc, path, options);
        defer db.close();
        for (53..61) |id| if (try activateOnePage(&db, @intCast(id))) break else {} else return error.ActivationDidNotConverge;
        const raw = try db.core.getStoreValue(alloc, activation.key) orelse return error.MissingActivationProgress;
        defer alloc.free(raw);
        const progress = try activation.Progress.decode(raw);
        try std.testing.expectEqual(.enforced, progress.state);
        try std.testing.expectEqual(@as(u64, 2), progress.rows_scanned);
        var read = try db.core.store.beginReadTxn();
        defer read.abort();
        var view = db.core.acquireSchemaView().?;
        defer view.release();
        var plan = try tuples.TuplePlan.init(alloc, view.tableSchema().*, view.physicalLayout(), &.{.{ .column = "id" }});
        defer plan.deinit();
        var tuple: std.ArrayList(u8) = .empty;
        defer tuple.deinit(alloc);
        _ = try plan.appendValues(alloc, &tuple, &.{.{ .integer = 1 }});
        const address = try integrity.Address.init(try binding(&db, .unique, "id_unique"), tuple.items);
        try std.testing.expectEqualStrings("a", (try integrity.Claim.decode(&address.claimKey(), try read.get(&address.claimKey()))).parent_key);
    }
}
