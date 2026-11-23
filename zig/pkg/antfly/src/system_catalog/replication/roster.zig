// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const local = @import("antfly_local_sources");
const publisher = local.system_catalog_logical_replication_publisher;
const A = std.mem.Allocator;

/// Database publications cover durable, internally stored relational tables.
pub fn eligibleForAllTables(a: A, table: anytype, resource: local.system_catalog_domain.Resource) !bool {
    if (table.schema_json.len == 0 or table.sql_persistence != null or table.sql_materialized_view_id != null or resource.native_service != null) return false;
    var definition = try local.schema_mod.PreparedSchemaDefinition.init(a, table.schema_json);
    defer definition.deinit(a);
    return definition.runtime_schema.storage_mode == .relational and definition.runtime_schema.external_base_source == null;
}

/// Actual metadata table/schema projection used by the owning read transaction.
/// Physical read-layout changes remain pending until their publication owner
/// settles; no sampled rows or wire-supplied OIDs participate in this binding.
pub fn describeTable(a: A, table: anytype, resource: local.system_catalog_domain.Resource, namespace: local.system_catalog_domain.Resource) !publisher.RelationDescription {
    if (table.read_schema_json.len != 0 or table.relational_retirement_json.len != 0) return error.LogicalPublisherCatalogChanged;
    return describeWriterEpoch(a, table, resource, namespace);
}

/// Describe the authoritative writer layout while planning a genuine schema
/// transition. This projection grants neither capture permission nor physical
/// readiness: observation/prepare still join the actual settled owner layout.
pub fn describeWriterEpoch(a: A, table: anytype, resource: local.system_catalog_domain.Resource, namespace: local.system_catalog_domain.Resource) !publisher.RelationDescription {
    if (resource.kind != .table or resource.id != table.table_id or !std.mem.eql(u8, resource.storage_name, table.name) or
        namespace.kind != .namespace or namespace.id != resource.parent_id or table.schema_json.len == 0 or
        table.storage_migration != null or table.restore_backup_id.len != 0 or
        table.sql_persistence != null) return error.LogicalPublisherCatalogChanged;
    const logical = resource.sql_relation_id orelse return error.CatalogSourceUntracked;
    const relation_oid = std.math.cast(u32, logical) orelse return error.InvalidLogicalPublisher;
    var prepared = try local.schema_mod.PreparedSchemaDefinition.init(a, table.schema_json);
    defer prepared.deinit(a);
    const native = prepared.runtime_schema;
    if (native.storage_mode != .relational or native.external_base_source != null) return error.UnsupportedReplicationType;
    const columns = try local.sql_document_row.deriveRelationalColumnsLeaky(a, a, prepared.public_schema, native);
    if (columns.len == 0 or columns.len > publisher.max_columns) return error.InvalidLogicalPublisher;
    const result = try a.alloc(publisher.Column, columns.len);
    var keys: std.StringHashMapUnmanaged(void) = .empty;
    defer keys.deinit(a);
    if (prepared.public_schema.unique_constraints) |declarations| for (declarations.value) |constraint| {
        if (!(constraint.primary orelse false)) continue;
        for (constraint.columns orelse &.{}) |name| try keys.put(a, name, {});
    };
    for (columns, result, 0..) |column, *out, ordinal| {
        out.* = .{ .attnum = @intCast(ordinal + 1), .name = column.name, .type_oid = try typeOid(column), .type_modifier = if (column.numeric_modifier) |modifier| try modifier.postgres() else -1, .key = keys.contains(column.name), .generated = column.generated };
    }
    const description: publisher.RelationDescription = .{
        .logical_relation_id = logical,
        .physical_table_id = table.table_id,
        .database_id = namespace.parent_id,
        .namespace_id = namespace.id,
        .relation_oid = relation_oid,
        .namespace = try a.dupe(u8, namespace.name),
        .name = try a.dupe(u8, resource.name),
        .schema_version = native.version,
        .schema_digest = try @import("../../metadata/restore_staging.zig").runtimeSchemaDigestForTable(a, table.schema_json),
        .replica_identity = .default_key,
        .columns = result,
    };
    try description.validate();
    return description;
}

/// pgoutput Relation uses actual atttypid, including a domain's catalog OID.
/// The RowDescription base-type flattening rule does not apply here.
pub fn typeOid(column: local.sql_catalog.Column) !u32 {
    if (column.domain_view) |view| {
        const identity = view.identity orelse return error.InvalidLogicalPublisher;
        try identity.validate();
        return identity.oid;
    }
    if (column.enum_view) |view| {
        try view.record.identity.validate();
        return view.record.identity.oid;
    }
    if (column.temporal_kind) |kind| return kind.oid();
    if (column.type == .array) return (column.element_type orelse return error.UnsupportedReplicationType).arrayOid();
    if (column.element_type) |kind| return kind.oid();
    return switch (column.type) {
        .boolean => 16,
        .integer => 20,
        .number => 701,
        .datetime => 1184,
        .json => 3802,
        .uuid => 2950,
        .string => 25,
        .numrange => 3906,
        .daterange => 3912,
        .tsrange => 3908,
        .tstzrange => 3910,
        else => error.UnsupportedReplicationType,
    };
}

test "SQL logical replication publisher schema describes actual declared widths keys generated cells and OIDs" {
    const a0 = std.testing.allocator;
    var region = std.heap.ArenaAllocator.init(a0);
    defer region.deinit();
    const a = region.allocator();
    var compiled = try local.sql_compiler.compile(a, "CREATE TABLE usage_records (id INTEGER PRIMARY KEY, n NUMERIC(12,3), tags TEXT[], active BOOLEAN, next_id INTEGER GENERATED ALWAYS AS (id+1) STORED)", .{});
    defer compiled.deinit();
    const draft = try local.sql_ddl_runtime.createSchemaAlloc(a, compiled.statement.create_table);
    // New SQL tables use epoch zero. Use the actual issued schema instead
    // of fabricating an epoch to satisfy the publisher's validation.
    const schema = draft;
    const resource: local.system_catalog_domain.Resource = .{ .kind = .table, .id = 30000000000, .sql_relation_id = 100, .parent_id = 2, .name = "usage_records", .storage_name = "physical:usage_records" };
    const table: @import("../../metadata/table_manager.zig").TableRecord = .{ .table_id = resource.id, .name = resource.storage_name, .schema_json = schema };
    try std.testing.expect(try eligibleForAllTables(a, table, resource));
    var document_table = table;
    document_table.schema_json = "{\"version\":1}";
    try std.testing.expect(!try eligibleForAllTables(a, document_table, resource));
    document_table.schema_json = "";
    try std.testing.expect(!try eligibleForAllTables(a, document_table, resource));
    var view_table = table;
    view_table.sql_materialized_view_id = 101;
    try std.testing.expect(!try eligibleForAllTables(a, view_table, resource));
    const description = try describeTable(a, table, resource, local.system_catalog_domain.default_namespace);
    try std.testing.expectEqual(@as(u32, 100), description.relation_oid);
    try std.testing.expectEqual(@as(u32, 0), description.schema_version);
    var missing_digest = description;
    missing_digest.schema_digest = @splat(0);
    try std.testing.expectError(error.InvalidLogicalPublisher, missing_digest.validate());
    try std.testing.expectEqual(@as(u64, 30000000000), description.physical_table_id);
    try std.testing.expectEqual(@as(u32, 23), description.columns[0].type_oid);
    try std.testing.expect(description.columns[0].key);
    try std.testing.expectEqual(@as(u32, 1700), description.columns[1].type_oid);
    try std.testing.expectEqual(try (local.sql_catalog.Column{ .name = "n", .path = "n", .type = .number, .element_type = .numeric, .numeric_modifier = .{ .precision = 12, .scale = 3 } }).numeric_modifier.?.postgres(), description.columns[1].type_modifier);
    try std.testing.expectEqual(@as(u32, 1009), description.columns[2].type_oid);
    try std.testing.expectEqual(@as(u32, 16), description.columns[3].type_oid);
    try std.testing.expect(description.columns[4].generated);
    var foreign = resource;
    foreign.sql_relation_id = null;
    try std.testing.expectError(error.CatalogSourceUntracked, describeTable(a, table, foreign, local.system_catalog_domain.default_namespace));
}
