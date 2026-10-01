// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
const std = @import("std");
const db_mod = @import("mod.zig");
const rows = @import("relational_rows.zig");
const mapper = @import("document_mapper.zig");
const alloc = std.testing.allocator;

test "relational index system expression CHECK activation records arithmetic failure and resumes after repair" {
    const initial =
        \\{"version":1,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"x":{"type":"integer"},"y":{"type":"integer"},"wide":{"type":"string"}},"additionalProperties":false}}}}
    ;
    const checked =
        \\{"version":2,"storage_mode":"relational","default_type":"row","checks":[{"name":"ratio","expression":{"op":"gte","args":[{"op":"divide","args":[{"op":"column","column":"x"},{"op":"column","column":"y"}]},{"op":"literal","type":"integer","value":0}]}}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"x":{"type":"integer"},"y":{"type":"integer"},"wide":{"type":"string"}},"additionalProperties":false}}}}
    ;
    var directory = try @import("../../common/test_directory.zig").TestDirectory.init("expression-check-activation");
    defer directory.cleanup();
    const options: db_mod.OpenOptions = .{ .start_optional_runtimes = false, .primary_backend = .{ .lsm = .{ .flush_threshold = 2 } } };
    var db = try db_mod.DB.open(alloc, directory.path(), options);
    defer db.close();
    try db.setSchemaJson(alloc, initial);
    try db.batch(.{ .writes = &.{ .{ .key = "bad", .value = "{\"x\":1,\"y\":0,\"wide\":\"unrelated\"}" }, .{ .key = "good", .value = "{\"x\":6,\"y\":2}" } } });
    try db.setSchemaJson(alloc, checked);
    {
        // The distributed owner page uses the same expression dependency
        // projection, while retaining a whole-row version guard for its CAS.
        var owner_directory = try @import("../../common/test_directory.zig").TestDirectory.init("expression-check-owner-projection");
        defer owner_directory.cleanup();
        var owner_options = options;
        owner_options.identity_namespace = .{ .table_id = 701, .shard_id = 702 };
        var owner = try db_mod.DB.open(alloc, owner_directory.path(), owner_options);
        defer owner.close();
        try owner.setSchemaJson(alloc, initial);
        try owner.batch(.{ .writes = &.{ .{ .key = "bad", .value = "{\"x\":1,\"y\":0,\"wide\":\"unrelated\"}" }, .{ .key = "good", .value = "{\"x\":6,\"y\":2}" } } });
        try std.testing.expectEqual(@as(u64, 1), owner.core.table_catalog.row_count);
        try owner.setSchemaJson(alloc, checked);
        const activation = @import("relational_integrity_activation.zig");
        {
            var read = try owner.core.store.beginReadTxn();
            defer read.abort();
            const catalog_mod = @import("relational_integrity_catalog.zig");
            var catalog = try catalog_mod.decode(alloc, try read.get(catalog_mod.key));
            defer catalog.deinit();
            try std.testing.expect(activation.hasActive(catalog));
            try std.testing.expectEqual(activation.State.validating, (try activation.status(&read, catalog)).state);
        }
        var page = try activation.Page.prepare(alloc, std.testing.io, owner.core, .{ .time_ns = std.time.ns_per_s }) orelse return error.ActivationPageUnavailable;
        defer page.deinit();
        try std.testing.expectEqual(.check, page.phase);
        try std.testing.expectEqual(@as(usize, 2), page.rows.rows.len);
        for (page.rows.rows) |row| {
            const json = try std.json.parseFromSlice(std.json.Value, alloc, row.json, .{});
            defer json.deinit();
            try std.testing.expectEqual(@as(u32, 2), json.value.object.count());
            try std.testing.expect(json.value.object.contains("x"));
            try std.testing.expect(json.value.object.contains("y"));
            try std.testing.expect(!json.value.object.contains("wide"));
            try std.testing.expect(row.expected_content_digest != null);
        }
    }
    try std.testing.expectError(error.RelationalExpressionDivisionByZero, db.batch(.{ .writes = &.{.{ .key = "new", .value = "{\"x\":1,\"y\":0}" }} }));
    for (0..128) |_| {
        if ((try db.constraintValidationStatus()).state == .invalid) break;
        _ = try db.validateConstraintsStep(.{ .records = 1, .time_ns = std.time.ns_per_s });
    } else return error.ValidationDidNotConverge;
    try std.testing.expectEqual(@as(?u16, 0), (try db.constraintValidationStatus()).failed_check);
    db.close();
    db = try db_mod.DB.open(alloc, directory.path(), options);
    try std.testing.expectEqual(.invalid, (try db.constraintValidationStatus()).state);
    try db.batch(.{ .writes = &.{.{ .key = "bad", .value = "{\"x\":8,\"y\":2}" }} });
    try std.testing.expect(try db.retryConstraintValidation(2));
    for (0..128) |_| {
        if ((try db.constraintValidationStatus()).state == .enforced) break;
        _ = try db.validateConstraintsStep(.{ .records = 1, .time_ns = std.time.ns_per_s });
    } else return error.ValidationDidNotConverge;
    try std.testing.expectEqual(@as(?u16, null), (try db.constraintValidationStatus()).failed_check);
}

const schema_json =
    \\{"version":1,"storage_mode":"relational","default_type":"row","column_defaults":[{"column":"b","expression":{"op":"literal","type":"integer","value":"2"}}],"generated_columns":[{"column":"doubled","expression":{"op":"multiply","args":[{"op":"column","column":"total"},{"op":"literal","type":"integer","value":"2"}]}},{"column":"total","expression":{"op":"add","args":[{"op":"column","column":"a"},{"op":"column","column":"b"}]}}],"relational_indexes":[{"name":"by_total","keys":[{"column":"total"}],"include_columns":["doubled"]}],"checks":[{"name":"positive","column":"total","op":"gt","value":"0"}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"a":{"type":"integer"},"b":{"type":["integer","null"]},"total":{"type":["integer","null"]},"doubled":{"type":["integer","null"]}},"required":["a","b","total","doubled"],"additionalProperties":false}}}}
;

test "relational index system portable restore preserves generated historical rows and resumed writes" {
    const portable = @import("../portable_backup.zig");
    const identity = @import("doc_identity.zig");
    var source_directory = try @import("../../common/test_directory.zig").TestDirectory.init("generated-portable-source");
    defer source_directory.cleanup();
    var source = try db_mod.DB.open(alloc, source_directory.path(), .{ .start_optional_runtimes = false });
    defer source.close();
    try source.setSchemaJson(alloc, schema_json);
    try source.batch(.{ .writes = &.{.{ .key = "old", .value = "{\"a\":3}" }} });
    var changed = try std.json.parseFromSlice(std.json.Value, alloc, schema_json, .{ .parse_numbers = false });
    defer changed.deinit();
    const arena = changed.arena.allocator();
    try changed.value.object.put(arena, "version", .{ .integer = 2 });
    changed.value.object.getPtr("column_defaults").?.array.items[0].object.getPtr("expression").?.object.getPtr("value").?.* = .{ .string = "7" };
    var predicate = try std.json.parseFromSlice(std.json.Value, arena, "[{\"column\":\"total\",\"op\":\"gt\",\"value\":7}]", .{});
    defer predicate.deinit();
    try changed.value.object.getPtr("relational_indexes").?.array.items[0].object.put(arena, "where", predicate.value);
    const schema_v2 = try std.json.Stringify.valueAlloc(arena, changed.value, .{});
    try source.setSchemaJson(alloc, schema_v2);
    try source.batch(.{ .writes = &.{.{ .key = "new", .value = "{\"a\":3}" }} });
    try ready(&source);
    var expected = try scan(&source, false);
    defer expected.deinit();
    try std.testing.expectEqual(@as(usize, 2), expected.rows.len);
    var archive: std.ArrayListUnmanaged(u8) = .empty;
    defer archive.deinit(alloc);
    try portable.exportPortable(alloc, source.core.store, &archive);

    inline for (.{ false, true }) |unpublished| {
        var directory = try @import("../../common/test_directory.zig").TestDirectory.init(if (unpublished) "generated-portable-staged" else "generated-portable-live");
        defer directory.cleanup();
        var restored = try db_mod.DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false });
        defer restored.close();
        if (unpublished) {
            try restored.importPortableIntoUnpublishedEmpty(alloc, archive.items, identity.default_namespace);
        } else {
            try restored.importPortableIntoEmpty(alloc, archive.items, identity.default_namespace);
        }
        // Inspect imported records before maintenance can repair a broken
        // archive rebuild. The historical total=5 row is not a member.
        const records = @import("relational_index_records.zig");
        const status = try restored.relationalIndexBuildStatus("by_total");
        const prefix = try records.forwardPrefix(.{ .generation = status.generation, .slot = status.slot });
        const keys = try restored.core.store.scanPrefixKeysPage(alloc, &prefix, null, 3);
        defer {
            for (keys) |key| alloc.free(key);
            alloc.free(keys);
        }
        try std.testing.expectEqual(@as(usize, 1), keys.len);
        try ready(&restored);
        {
            var covered_reader = try restored.beginRelationalRows(alloc, .{ .index = "by_total", .fields = &.{ "total", "doubled" }, .conditions = &.{.{ .column = "total", .op = .gt, .value = .{ .integer = 7 } }} });
            defer covered_reader.deinit();
            var covered = try covered_reader.nextPage(alloc, std.testing.io, .{ .time_ns = std.time.ns_per_s });
            defer covered.deinit();
            try std.testing.expectEqual(@as(usize, 1), covered.rows.len);
            try std.testing.expectEqual(@as(usize, 0), covered.primary_lookups);
            try std.testing.expectEqualStrings("{\"total\":10,\"doubled\":20}", covered.rows[0].json);
        }
        var actual = try scan(&restored, false);
        defer actual.deinit();
        try std.testing.expectEqual(expected.rows.len, actual.rows.len);
        for (expected.rows, actual.rows) |a, b| {
            try std.testing.expectEqualStrings(a.key, b.key);
            try std.testing.expectEqualStrings(a.json, b.json);
            try std.testing.expectEqualSlices(u8, &a.semantic_hash, &b.semantic_hash);
        }
        restored.close();
        restored = try db_mod.DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false });
        try std.testing.expectEqual(@as(u32, 2), restored.core.schema.?.version);
        try restored.batch(.{ .writes = &.{.{ .key = "after", .value = "{\"a\":4}" }} });
        const row = (try restored.get(alloc, "after")) orelse return error.TestExpectedEqual;
        defer alloc.free(row);
        var parsed = try std.json.parseFromSlice(std.json.Value, alloc, row, .{});
        defer parsed.deinit();
        try std.testing.expectEqual(@as(i64, 11), parsed.value.object.get("total").?.integer);
        try std.testing.expectEqual(@as(i64, 22), parsed.value.object.get("doubled").?.integer);
    }
}

fn ready(db: *db_mod.DB) !void {
    for (0..1024) |_| {
        if ((try db.relationalIndexBuildStatus("by_total")).state == .ready) return;
        _ = try db.runRelationalIndexMaintenancePass();
    }
    return error.IndexBuildDidNotConverge;
}

test "relational index system staged restore preserves historical absence and rejects forged generated values" {
    const staging = @import("restore_staging.zig");
    const codec = @import("algebraic/relational_row_codec.zig");
    const internal = @import("../internal_keys.zig");
    const source_namespace: @import("doc_identity.zig").Namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 2 };
    const target_namespace: @import("doc_identity.zig").Namespace = .{ .table_id = 3, .shard_id = 4, .range_id = 4 };
    var source_directory = try @import("../../common/test_directory.zig").TestDirectory.init("preserved-restore-source");
    defer source_directory.cleanup();
    var target_directory = try @import("../../common/test_directory.zig").TestDirectory.init("preserved-restore-target");
    defer target_directory.cleanup();
    var changed = try std.json.parseFromSlice(std.json.Value, alloc, schema_json, .{});
    defer changed.deinit();
    const arena = changed.arena.allocator();
    try changed.value.object.put(arena, "version", .{ .integer = 2 });
    const optional = try std.json.parseFromSlice(std.json.Value, arena, "{\"type\":[\"integer\",\"null\"]}", .{});
    try changed.value.object.getPtr("document_schemas").?.object.getPtr("row").?.object.getPtr("schema").?.object.getPtr("properties").?.object.put(arena, "later", optional.value);
    const later_default = try std.json.parseFromSlice(std.json.Value, arena, "{\"column\":\"later\",\"expression\":{\"op\":\"literal\",\"type\":\"integer\",\"value\":\"7\"}}", .{});
    try changed.value.object.getPtr("column_defaults").?.array.append(later_default.value);
    const target_schema = try std.json.Stringify.valueAlloc(arena, changed.value, .{});
    {
        var source = try db_mod.DB.open(alloc, source_directory.path(), .{ .identity_namespace = source_namespace, .start_optional_runtimes = false });
        defer source.close();
        try source.setSchemaJson(alloc, schema_json);
        try source.batch(.{ .writes = &.{.{ .key = "old", .value = "{\"a\":3}" }}, .timestamp_ns = 123 });
        try source.setSchemaJson(alloc, target_schema);
    }
    var source = try db_mod.DB.open(alloc, source_directory.path(), .{ .identity_namespace = source_namespace, .open_mode = .query_readonly, .start_optional_runtimes = false });
    defer source.close();
    var target = try db_mod.DB.open(alloc, target_directory.path(), .{ .identity_namespace = target_namespace, .start_optional_runtimes = false });
    defer target.close();
    try target.setSchemaJson(alloc, target_schema);
    const runtime = try @import("../schema.zig").serializeSchema(alloc, target.core.schema.?);
    defer alloc.free(runtime);
    const scope: staging.Scope = .{ .plan_id = @splat(1), .plan_digest = @splat(2), .source_artifact_digest = @splat(3), .source_namespace = source_namespace, .target_namespace = target_namespace, .target_schema_digest = staging.digest(runtime) };
    try target.reserveRestoreStaging(alloc, scope.plan_id, scope.plan_digest, target_namespace);
    try target.beginRestoreStaging(alloc, scope);
    var page = try target.prepareRestoreStagingPage(alloc, scope, &source, 128, .none);
    defer page.deinit();
    try std.testing.expectEqual(@as(usize, 1), page.batch.?.writes.len);
    var forged = page.batch.?;
    forged.writes = &.{.{ .key = "old", .value = "{\"a\":3,\"b\":2,\"total\":6,\"doubled\":12}" }};
    try std.testing.expectError(error.InvalidRelationalGeneratedValue, target.batchReplicatedApply(forged));
    var before = (try target.restoreStagingStatus(alloc)).?;
    defer before.deinit();
    try std.testing.expectEqual(@as(u64, 0), before.value.rows);
    try target.batchRaftReplicatedApply(page.batch.?, .{ .index = 1, .term = 1 });
    try target.batchRaftReplicatedApply(page.batch.?, .{ .index = 1, .term = 1 });
    target.close();
    target = try db_mod.DB.open(alloc, target_directory.path(), .{ .identity_namespace = target_namespace, .start_optional_runtimes = false });
    const key = try internal.relationalRowKeyAlloc(alloc, "old");
    defer alloc.free(key);
    const original = try source.core.store.get(alloc, key);
    defer alloc.free(original);
    const restored = try target.core.store.get(alloc, key);
    defer alloc.free(restored);
    try std.testing.expectEqualSlices(u8, &try codec.rowSemanticHash(original), &try codec.rowSemanticHash(restored));
    var view = target.core.acquireSchemaView().?;
    defer view.release();
    const row = try codec.ordinalRowView(restored, view.tableSchema().*, view.physicalLayout());
    try std.testing.expect((try row.findCell(row.ordinalForName("later").?)) == null);
    try std.testing.expectEqual(@as(u64, 123), row.writeTimestampNs());

    // The individually owned constructor is the serialized fallback contract.
    var fallback = try mapper.PreparedRelationalWrite.initPreserved(alloc, "old", page.batch.?.writes[0].value, view.validator(), view.tableSchema().*, view.physicalLayout());
    defer fallback.deinit(alloc);
    try std.testing.expectEqualSlices(u8, &try codec.rowSemanticHash(original), &fallback.semantic_hash);
    var mutation = try mapper.PreparedRelationalWrite.init(alloc, "new", page.batch.?.writes[0].value, view.validator(), view.tableSchema().*, view.physicalLayout());
    defer mutation.deinit(alloc);
    const mutation_row = try mutation.typedView(view.tableSchema().*, view.physicalLayout());
    try std.testing.expect((try mutation_row.findCell(mutation_row.ordinalForName("later").?)) != null);

    // A newly required column cannot be silently filled to make a restore
    // compatible. The explicit target validation must still reject it.
    try changed.value.object.getPtr("document_schemas").?.object.getPtr("row").?.object.getPtr("schema").?.object.getPtr("required").?.array.append(.{ .string = "later" });
    const strict_json = try std.json.Stringify.valueAlloc(arena, changed.value, .{});
    const schema_api = @import("../../schema/mod.zig");
    var strict = try schema_api.CompiledTableValidator.init(alloc, strict_json);
    defer strict.deinit(alloc);
    const strict_schema = try schema_api.deriveRuntimeTableSchema(alloc, strict.schema);
    defer @import("../schema.zig").freeSchema(alloc, strict_schema);
    var strict_layout = try codec.PhysicalLayout.init(alloc, strict_schema);
    defer strict_layout.deinit();
    try std.testing.expectError(error.InvalidBatchRequest, mapper.PreparedRelationalWrite.initPreserved(alloc, "old", page.batch.?.writes[0].value, strict, strict_schema, &strict_layout));
}

test "relational index system fallback consumer failure releases transferred computed effects" {
    var directory = try @import("../../common/test_directory.zig").TestDirectory.init("relational-expression-consumer-error");
    defer directory.cleanup();
    var db = try db_mod.DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false });
    defer db.close();
    try db.setSchemaJson(alloc, schema_json);
    try db.addIndex(.{ .name = "links", .kind = .graph, .config_json = "{\"edge_types\":[{\"name\":\"target\",\"field\":\"total\"}]}" });
    // Duplicate keys force the serialized fallback. Graph extraction rejects
    // the computed numeric field after takeExtracted has transferred ownership;
    // neither row/index publication nor extracted allocations may survive.
    try std.testing.expectError(error.InvalidGraphEdges, db.batch(.{ .writes = &.{
        .{ .key = "row", .value = "{\"a\":1}" },
        .{ .key = "row", .value = "{\"a\":2}" },
    } }));
    try std.testing.expect((try db.lookup(alloc, "row", .{})) == null);
}

fn scan(db: *db_mod.DB, indexed: bool) !rows.Page {
    var reader = try db.beginRelationalRows(alloc, .{ .index = if (indexed) "by_total" else null, .fields = &.{ "total", "doubled" } });
    defer reader.deinit();
    return reader.nextPage(alloc, std.testing.io, .{ .rows = 32, .records = 256, .time_ns = std.time.ns_per_s });
}

test "relational index system generated rows share defaults checks covering hash and durable replay" {
    var directory = try @import("../../common/test_directory.zig").TestDirectory.init("relational-expressions");
    defer directory.cleanup();
    var db = try db_mod.DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false });
    defer db.close();
    try db.setSchemaJson(alloc, schema_json);
    // Stored generated columns cannot be overridden, even by a supplied value
    // of the wrong type. The default applies only to the absent b column.
    try db.batch(.{ .timestamp_ns = 123, .writes = &.{.{ .key = "row", .value = "{\"a\":3,\"total\":\"ignored\"}" }} });
    try ready(&db);
    var covered = try scan(&db, true);
    defer covered.deinit();
    var primary = try scan(&db, false);
    defer primary.deinit();
    try std.testing.expectEqual(@as(usize, 1), covered.rows.len);
    try std.testing.expectEqualStrings("{\"total\":5,\"doubled\":10}", covered.rows[0].json);
    try std.testing.expectEqualStrings(primary.rows[0].json, covered.rows[0].json);
    try std.testing.expectEqualSlices(u8, &primary.rows[0].semantic_hash, &covered.rows[0].semantic_hash);
    try std.testing.expectEqual(@as(usize, 0), covered.primary_lookups);
    try std.testing.expectError(error.RelationalCheckViolation, db.batch(.{ .writes = &.{.{ .key = "row", .value = "{\"a\":-3}" }} }));
    try std.testing.expectError(error.RelationalExpressionOverflow, db.batch(.{ .writes = &.{.{ .key = "row", .value = "{\"a\":9223372036854775807}" }} }));
    try db.batch(.{ .writes = &.{
        .{ .key = "row", .value = "{\"a\":4}" },
        .{ .key = "row", .value = "{\"a\":5,\"b\":null}" },
    } });
    var nullable = try scan(&db, true);
    defer nullable.deinit();
    try std.testing.expectEqualStrings("{\"total\":null,\"doubled\":null}", nullable.rows[0].json);
    db.close();
    db = try db_mod.DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false });
    try ready(&db);
    var reopened = try scan(&db, true);
    defer reopened.deinit();
    try std.testing.expectEqualStrings(nullable.rows[0].json, reopened.rows[0].json);

    var view = db.core.acquireSchemaView().?;
    defer view.release();
    var prepared = try mapper.PreparedRelationalWrite.init(alloc, "intent", "{\"a\":3}", view.validator(), view.tableSchema().*, view.physicalLayout());
    defer prepared.deinit(alloc);
    const logical = (try prepared.extracted.logicalJson()).?;
    var logical_root = try std.json.parseFromSlice(std.json.Value, alloc, logical, .{});
    defer logical_root.deinit();
    try std.testing.expectEqual(@as(i64, 5), logical_root.value.object.get("total").?.integer);
    try std.testing.expectEqual(@as(i64, 10), logical_root.value.object.get("doubled").?.integer);
    try prepared.finalizeMetadata(999);
    // Commit consumes the canonical durable row without filling defaults into
    // its specials-only sidecar or evaluating the generated graph again.
    var replay = try mapper.PreparedRelationalWrite.initFromIntent(alloc, "intent", "{}", view.validator(), view.tableSchema().*, view.physicalLayout(), prepared.packed_row);
    defer replay.deinit(alloc);
    try std.testing.expectEqualSlices(u8, prepared.packed_row, replay.packed_row);
    try std.testing.expectEqual(@as(usize, 0), replay.parsedValue().object.count());
}

test "relational index system generated schema changes require rewrite with durable data presence" {
    var directory = try @import("../../common/test_directory.zig").TestDirectory.init("relational-generated-schema-guard");
    defer directory.cleanup();
    var db = try db_mod.DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false });
    defer db.close();
    try db.setSchemaJson(alloc, schema_json);
    try db.batch(.{ .writes = &.{.{ .key = "row", .value = "{\"a\":3}" }} });
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, schema_json, .{ .parse_numbers = false });
    defer parsed.deinit();
    const arena = parsed.arena.allocator();
    try parsed.value.object.put(arena, "version", .{ .integer = 2 });
    // Declaration order is not semantics, and changed defaults only affect
    // future writes. Neither operation needs an existing-row rewrite.
    const declarations = parsed.value.object.getPtr("generated_columns").?.array.items;
    std.mem.swap(std.json.Value, &declarations[0], &declarations[1]);
    const default_value = parsed.value.object.getPtr("column_defaults").?.array.items[0].object.getPtr("expression").?.object.getPtr("value").?;
    default_value.* = .{ .string = "7" };
    const reordered = try std.json.Stringify.valueAlloc(arena, parsed.value, .{});
    try db.setSchemaJson(alloc, reordered);
    try parsed.value.object.put(arena, "version", .{ .integer = 3 });
    const multiplier = declarations[1].object.getPtr("expression").?.object.getPtr("args").?.array.items[1].object.getPtr("value").?;
    multiplier.* = .{ .string = "3" };
    const changed = try std.json.Stringify.valueAlloc(arena, parsed.value, .{});
    try std.testing.expectError(error.GeneratedColumnRewriteRequired, db.setSchemaJson(alloc, changed));
    try std.testing.expectEqual(@as(u32, 2), db.core.schema.?.version);
    try parsed.value.object.put(arena, "generated_columns", .{ .array = std.array_list.Managed(std.json.Value).init(arena) });
    const removed = try std.json.Stringify.valueAlloc(arena, parsed.value, .{});
    try std.testing.expectError(error.GeneratedColumnRewriteRequired, db.setSchemaJson(alloc, removed));
    // Presence follows exact cardinality, remains true after a partial delete,
    // and survives restart. No schema preflight performs a user-row scan.
    try db.batch(.{ .writes = &.{.{ .key = "survivor", .value = "{\"a\":4}" }} });
    try db.batch(.{ .deletes = &.{"row"} });
    db.close();
    db = try db_mod.DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false });
    try std.testing.expectError(error.GeneratedColumnRewriteRequired, db.setSchemaJson(alloc, changed));
    try std.testing.expectEqual(@as(u32, 2), db.core.schema.?.version);
}
