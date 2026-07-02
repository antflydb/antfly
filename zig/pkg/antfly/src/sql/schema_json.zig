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

const binder = @import("binder.zig");
const db_mod = @import("../storage/db/mod.zig");
const ddl_plan = @import("ddl_plan.zig");
const expr_type = @import("expr/type.zig");
const json_helpers = @import("../common/json_helpers.zig");
const runtime_schema = @import("../storage/schema.zig");
const schema_api = @import("../schema/mod.zig");

pub const RelationalSchemaJsonParts = struct {
    schema: *std.json.ObjectMap,
    properties: *std.json.ObjectMap,
};

pub fn runtimeSchemaFromSchemaJsonAlloc(alloc: std.mem.Allocator, schema_json: []const u8) !runtime_schema.TableSchema {
    var parsed_schema = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);
    return try schema_api.deriveRuntimeTableSchema(alloc, parsed_schema);
}

pub fn schemaJsonFromCreateTablePlanAlloc(
    alloc: std.mem.Allocator,
    plan: ddl_plan.CreateTablePlan,
) ![]u8 {
    const runtime = try ddl_plan.runtimeSchemaFromCreateTablePlanAlloc(alloc, plan);
    defer runtime_schema.freeSchema(alloc, runtime);

    var arena_impl = std.heap.ArenaAllocator.init(alloc);
    defer arena_impl.deinit();
    const arena = arena_impl.allocator();
    const root = try schemaJsonValueFromCreateTablePlanAlloc(arena, plan);
    const schema_json = try std.json.Stringify.valueAlloc(alloc, root, .{ .emit_null_optional_fields = false });
    errdefer alloc.free(schema_json);
    try validateDdlAppliedSchemaJsonAlloc(alloc, schema_json);
    return schema_json;
}

pub fn validateDdlAppliedSchemaJsonAlloc(alloc: std.mem.Allocator, schema_json: []const u8) !void {
    const runtime = try runtimeSchemaFromSchemaJsonAlloc(alloc, schema_json);
    defer runtime_schema.freeSchema(alloc, runtime);
    if (runtime.storage_mode != .relational and runtime.storage_mode != .document) return error.InvalidSqlCatalog;
}

pub fn schemaJsonFromTableClonePlanAlloc(
    alloc: std.mem.Allocator,
    source_schema_json: []const u8,
    plan: ddl_plan.TableClonePlan,
) ![]u8 {
    if (source_schema_json.len == 0) return error.InvalidSqlCatalog;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, source_schema_json);
    defer parsed.deinit(alloc);
    const source = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, source);

    var create_table = try ddl_plan.createTablePlanFromTableCloneSourceAlloc(alloc, source, plan);
    defer create_table.deinit(alloc);
    return try schemaJsonFromCreateTablePlanAlloc(alloc, create_table);
}

pub fn schemaJsonValueFromCreateTablePlanAlloc(alloc: std.mem.Allocator, plan: ddl_plan.CreateTablePlan) !std.json.Value {
    if (plan.storage_mode == .document) {
        const schema_json = try ddl_plan.documentSchemaJsonFromCreateTablePlanAlloc(alloc, plan);
        defer alloc.free(schema_json);
        return try std.json.parseFromSliceLeaky(std.json.Value, alloc, schema_json, .{ .allocate = .alloc_always });
    }

    var properties = std.json.ObjectMap.empty;
    for (plan.columns) |column| {
        try properties.put(alloc, try alloc.dupe(u8, column.name), try schemaJsonPropertyFromColumnAlloc(alloc, column));
    }

    var required = std.json.Array.init(alloc);
    for (plan.columns) |column| {
        if (!column.nullable) try required.append(.{ .string = try alloc.dupe(u8, column.name) });
    }

    var row_schema = std.json.ObjectMap.empty;
    try putJsonString(alloc, &row_schema, "type", "object");
    try row_schema.put(alloc, try alloc.dupe(u8, "properties"), .{ .object = properties });
    if (required.items.len > 0) try row_schema.put(alloc, try alloc.dupe(u8, "required"), .{ .array = required });
    try row_schema.put(alloc, try alloc.dupe(u8, "additionalProperties"), .{ .bool = false });

    var document_schema = std.json.ObjectMap.empty;
    try document_schema.put(alloc, try alloc.dupe(u8, "schema"), .{ .object = row_schema });

    var document_schemas = std.json.ObjectMap.empty;
    try document_schemas.put(alloc, try alloc.dupe(u8, "row"), .{ .object = document_schema });

    var root = std.json.ObjectMap.empty;
    try putJsonString(alloc, &root, "storage_mode", "relational");
    try putJsonString(alloc, &root, "default_type", "row");
    try root.put(alloc, try alloc.dupe(u8, "enforce_types"), .{ .bool = true });
    try root.put(alloc, try alloc.dupe(u8, "document_schemas"), .{ .object = document_schemas });
    if (plan.primary_key) |primary_key| try root.put(alloc, try alloc.dupe(u8, "primary_key"), try schemaJsonPrimaryKeyAlloc(alloc, primary_key));
    if (plan.periods.len > 0) try root.put(alloc, try alloc.dupe(u8, "periods"), try schemaJsonPeriodsAlloc(alloc, plan.periods));
    if (plan.unique_constraints.len > 0) try root.put(alloc, try alloc.dupe(u8, "unique_constraints"), try schemaJsonUniqueConstraintsAlloc(alloc, plan.unique_constraints));
    if (plan.foreign_keys.len > 0) try root.put(alloc, try alloc.dupe(u8, "foreign_keys"), try schemaJsonForeignKeysAlloc(alloc, plan.foreign_keys));
    if (plan.checks.len > 0) try root.put(alloc, try alloc.dupe(u8, "checks"), try schemaJsonRelationalChecksAlloc(alloc, plan.checks));
    if (plan.system_versioned) try root.put(alloc, try alloc.dupe(u8, "system_versioned"), .{ .bool = true });
    return .{ .object = root };
}

pub fn schemaJsonPropertyFromColumnAlloc(alloc: std.mem.Allocator, column: runtime_schema.RelationalColumn) !std.json.Value {
    var object = std.json.ObjectMap.empty;
    try putJsonString(alloc, &object, "type", ddl_plan.antflyTypeSchemaName(column.field_type));
    if (column.field_type == .array) {
        const item_type = column.array_item_type orelse return error.InvalidSqlCatalog;
        var item_object = std.json.ObjectMap.empty;
        try putJsonString(alloc, &item_object, "type", ddl_plan.antflyTypeSchemaName(item_type));
        try object.put(alloc, try alloc.dupe(u8, "items"), .{ .object = item_object });
    }
    if (!column.indexed) try object.put(alloc, try alloc.dupe(u8, "x-antfly-index"), .{ .bool = false });
    if (column.index_lifecycle != .ready) try putJsonString(alloc, &object, "x-antfly-index-lifecycle", ddl_plan.relationalIndexLifecycleName(column.index_lifecycle));
    if (column.index_generation != 0) try object.put(alloc, try alloc.dupe(u8, "x-antfly-index-generation"), .{ .integer = @intCast(column.index_generation) });
    if (column.index_name) |index_name| try putJsonString(alloc, &object, "x-antfly-index-name", index_name);
    if (column.index_include_columns.len > 0) try object.put(alloc, try alloc.dupe(u8, "x-antfly-index-include"), try schemaJsonStringArrayAlloc(alloc, column.index_include_columns));
    if (column.index_keys.len > 0) try object.put(alloc, try alloc.dupe(u8, "x-antfly-index-keys"), try schemaJsonRelationalIndexKeysAlloc(alloc, column.index_keys));
    if (column.cardinality_proof == .unique) try putJsonString(alloc, &object, "x-antfly-cardinality-proof", "unique");
    if (column.collation) |collation| try putJsonString(alloc, &object, "collation", collation);
    if (column.index_where.len > 0) try object.put(alloc, try alloc.dupe(u8, "x-antfly-index-where"), try schemaJsonUniquePredicateDefinitionAlloc(alloc, column.index_where));
    if (column.index_where_expressions.len > 0) try object.put(alloc, try alloc.dupe(u8, "x-antfly-index-where-expressions"), try schemaJsonExpressionConditionsAlloc(alloc, column.index_where_expressions));
    if (column.default_value) |value| {
        const key = if (value.kind == .literal) "default" else "x-antfly-default";
        try object.put(alloc, try alloc.dupe(u8, key), try schemaJsonDefaultValueAlloc(alloc, value, value.kind != .literal));
    }
    if (column.on_update_value) |value| try object.put(alloc, try alloc.dupe(u8, "x-antfly-on-update"), try schemaJsonDefaultValueAlloc(alloc, value, true));
    if (column.generated) |generated| try object.put(alloc, try alloc.dupe(u8, "generated"), try schemaJsonGeneratedValueAlloc(alloc, generated));
    return .{ .object = object };
}

pub fn schemaJsonDefaultValueAlloc(alloc: std.mem.Allocator, value: runtime_schema.RelationalDefaultValue, force_server_default: bool) !std.json.Value {
    if (!force_server_default and value.kind == .literal) {
        var parsed = try std.json.parseFromSlice(std.json.Value, alloc, value.value_json, .{});
        defer parsed.deinit();
        return try json_helpers.cloneJsonValue(alloc, parsed.value);
    }
    var object = std.json.ObjectMap.empty;
    try putJsonString(alloc, &object, "op", switch (value.kind) {
        .literal => return error.InvalidSqlCatalog,
        .now_ns => "now_ns",
        .current_date_ns => "current_date_ns",
        .uuid_v4 => "uuid_v4",
        .sequence_next => "sequence_next",
    });
    if (value.kind == .sequence_next) {
        var parsed = try std.json.parseFromSlice(std.json.Value, alloc, value.value_json, .{});
        defer parsed.deinit();
        if (parsed.value != .object) return error.InvalidSqlCatalog;
        const sequence = parsed.value.object.get("sequence") orelse return error.InvalidSqlCatalog;
        if (sequence != .string) return error.InvalidSqlCatalog;
        try putJsonString(alloc, &object, "sequence", sequence.string);
        if (parsed.value.object.get("database")) |database| {
            if (database != .string) return error.InvalidSqlCatalog;
            try putJsonString(alloc, &object, "database", database.string);
        }
        if (parsed.value.object.get("schema")) |schema_name| {
            if (schema_name != .string) return error.InvalidSqlCatalog;
            try putJsonString(alloc, &object, "schema", schema_name.string);
        }
    }
    return .{ .object = object };
}

pub fn schemaJsonGeneratedValueAlloc(alloc: std.mem.Allocator, generated: runtime_schema.RelationalGeneratedValue) !std.json.Value {
    var object = std.json.ObjectMap.empty;
    try putJsonString(alloc, &object, "op", switch (generated.op) {
        .lower => "lower",
        .upper => "upper",
        .md5 => "md5",
        .concat => "concat",
        .concat_ws => "concat_ws",
        .expression => "expression",
    });
    switch (generated.op) {
        .lower, .upper, .md5 => try putJsonString(alloc, &object, "field", generated.field orelse return error.InvalidSqlCatalog),
        .concat, .concat_ws => {
            try object.put(alloc, try alloc.dupe(u8, "fields"), try schemaJsonStringArrayAlloc(alloc, generated.fields));
            try putJsonString(alloc, &object, "separator", generated.separator);
        },
        .expression => try object.put(alloc, try alloc.dupe(u8, "expression"), try schemaJsonExpressionAlloc(alloc, generated.expression orelse return error.InvalidSqlCatalog)),
    }
    return .{ .object = object };
}

pub fn schemaJsonPrimaryKeyAlloc(alloc: std.mem.Allocator, primary_key: runtime_schema.PrimaryKey) !std.json.Value {
    var object = std.json.ObjectMap.empty;
    if (primary_key.name) |name| try putJsonString(alloc, &object, "name", name);
    try object.put(alloc, try alloc.dupe(u8, "columns"), try schemaJsonStringArrayAlloc(alloc, primary_key.columns));
    if (primary_key.include_columns.len > 0) try object.put(alloc, try alloc.dupe(u8, "include_columns"), try schemaJsonStringArrayAlloc(alloc, primary_key.include_columns));
    if (primary_key.without_overlaps_period) |period| try putJsonString(alloc, &object, "without_overlaps_period", period);
    if (primary_key.deferrable) try object.put(alloc, try alloc.dupe(u8, "deferrable"), .{ .bool = true });
    if (primary_key.timing == .deferred) try putJsonString(alloc, &object, "timing", "deferred");
    return .{ .object = object };
}

pub fn schemaJsonPeriodsAlloc(alloc: std.mem.Allocator, periods: []const runtime_schema.RelationalPeriod) !std.json.Value {
    var array = std.json.Array.init(alloc);
    for (periods) |period| {
        try array.append(try schemaJsonPeriodAlloc(alloc, period));
    }
    return .{ .array = array };
}

pub fn schemaJsonPeriodAlloc(alloc: std.mem.Allocator, period: runtime_schema.RelationalPeriod) !std.json.Value {
    var object = std.json.ObjectMap.empty;
    try putJsonString(alloc, &object, "name", period.name);
    try putJsonString(alloc, &object, "start_column", period.start_column);
    try putJsonString(alloc, &object, "end_column", period.end_column);
    if (period.range_type) |range_type| try putJsonString(alloc, &object, "range_type", ddl_plan.relationalPeriodRangeTypeName(range_type));
    return .{ .object = object };
}

pub fn schemaJsonUniqueConstraintsAlloc(alloc: std.mem.Allocator, constraints: []const runtime_schema.UniqueConstraint) !std.json.Value {
    var array = std.json.Array.init(alloc);
    for (constraints) |constraint| try array.append(try schemaJsonUniqueConstraintAlloc(alloc, constraint));
    return .{ .array = array };
}

pub fn schemaJsonUniqueConstraintAlloc(alloc: std.mem.Allocator, constraint: runtime_schema.UniqueConstraint) !std.json.Value {
    var object = std.json.ObjectMap.empty;
    try putJsonString(alloc, &object, "name", constraint.name);
    if (constraint.columns.len > 0) try object.put(alloc, try alloc.dupe(u8, "columns"), try schemaJsonStringArrayAlloc(alloc, constraint.columns));
    if (constraint.expressions.len > 0) try object.put(alloc, try alloc.dupe(u8, "expressions"), try schemaJsonUniqueExpressionsAlloc(alloc, constraint.expressions));
    if (constraint.include_columns.len > 0) try object.put(alloc, try alloc.dupe(u8, "include_columns"), try schemaJsonStringArrayAlloc(alloc, constraint.include_columns));
    if (constraint.index_keys.len > 0) try object.put(alloc, try alloc.dupe(u8, "index_keys"), try schemaJsonRelationalIndexKeysAlloc(alloc, constraint.index_keys));
    if (constraint.without_overlaps_period) |period| try putJsonString(alloc, &object, "without_overlaps_period", period);
    if (constraint.nulls_not_distinct) try object.put(alloc, try alloc.dupe(u8, "nulls_not_distinct"), .{ .bool = true });
    if (constraint.where.len > 0) try object.put(alloc, try alloc.dupe(u8, "where"), try schemaJsonUniquePredicateDefinitionAlloc(alloc, constraint.where));
    if (constraint.where_expressions.len > 0) try object.put(alloc, try alloc.dupe(u8, "where_expressions"), try schemaJsonExpressionConditionsAlloc(alloc, constraint.where_expressions));
    if (constraint.validation_state != .enforced) try putJsonString(alloc, &object, "validation_state", ddl_plan.uniqueConstraintValidationStateString(constraint.validation_state));
    if (constraint.deferrable) try object.put(alloc, try alloc.dupe(u8, "deferrable"), .{ .bool = true });
    if (constraint.timing == .deferred) try putJsonString(alloc, &object, "timing", "deferred");
    return .{ .object = object };
}

pub fn schemaJsonUniqueExpressionsAlloc(alloc: std.mem.Allocator, expressions: []const runtime_schema.UniqueExpression) !std.json.Value {
    var array = std.json.Array.init(alloc);
    for (expressions) |expression| {
        var object = std.json.ObjectMap.empty;
        try putJsonString(alloc, &object, "op", switch (expression.op) {
            .lower => "lower",
            .upper => "upper",
            .md5 => "md5",
            .expression => "expression",
        });
        switch (expression.op) {
            .lower, .upper, .md5 => try putJsonString(alloc, &object, "field", expression.field),
            .expression => try object.put(alloc, try alloc.dupe(u8, "expression"), try schemaJsonExpressionAlloc(alloc, expression.expression orelse return error.UnsupportedSqlShape)),
        }
        try array.append(.{ .object = object });
    }
    return .{ .array = array };
}

pub fn schemaJsonUniquePredicateDefinitionAlloc(alloc: std.mem.Allocator, predicates: []const runtime_schema.UniquePredicate) !std.json.Value {
    var object = std.json.ObjectMap.empty;
    var array = std.json.Array.init(alloc);
    for (predicates) |predicate| try array.append(try schemaJsonUniquePredicateAlloc(alloc, predicate));
    try object.put(alloc, try alloc.dupe(u8, "all"), .{ .array = array });
    return .{ .object = object };
}

pub fn schemaJsonExpressionConditionsAlloc(
    alloc: std.mem.Allocator,
    conditions: []const db_mod.types.RelationalRowsExpressionCondition,
) !std.json.Value {
    var out: std.Io.Writer.Allocating = .init(alloc);
    errdefer out.deinit();
    const writer = &out.writer;
    try writer.writeByte('[');
    for (conditions, 0..) |condition, i| {
        if (i != 0) try writer.writeByte(',');
        try expr_type.writeRowExpressionConditionJson(writer, condition);
    }
    try writer.writeByte(']');
    const json = try out.toOwnedSlice();
    defer alloc.free(json);
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, json, .{});
    defer parsed.deinit();
    return try json_helpers.cloneJsonValue(alloc, parsed.value);
}

pub fn schemaJsonExpressionAlloc(
    alloc: std.mem.Allocator,
    expression: db_mod.types.RelationalRowsExpression,
) !std.json.Value {
    var out: std.Io.Writer.Allocating = .init(alloc);
    errdefer out.deinit();
    const writer = &out.writer;
    try expr_type.writeRowExpressionJson(writer, expression);
    const json = try out.toOwnedSlice();
    defer alloc.free(json);
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, json, .{});
    defer parsed.deinit();
    return try json_helpers.cloneJsonValue(alloc, parsed.value);
}

pub fn schemaJsonUniquePredicateAlloc(alloc: std.mem.Allocator, predicate: runtime_schema.UniquePredicate) !std.json.Value {
    var object = std.json.ObjectMap.empty;
    try putJsonString(alloc, &object, "field", predicate.field);
    try putJsonString(alloc, &object, "op", expr_type.uniquePredicateOpToken(predicate.op));
    if (predicate.value_json) |value_json| {
        var parsed = try std.json.parseFromSlice(std.json.Value, alloc, value_json, .{});
        defer parsed.deinit();
        try object.put(alloc, try alloc.dupe(u8, "value"), try json_helpers.cloneJsonValue(alloc, parsed.value));
    }
    return .{ .object = object };
}

pub fn schemaJsonForeignKeysAlloc(alloc: std.mem.Allocator, foreign_keys: []const runtime_schema.ForeignKey) !std.json.Value {
    var array = std.json.Array.init(alloc);
    for (foreign_keys) |foreign_key| try array.append(try schemaJsonForeignKeyAlloc(alloc, foreign_key));
    return .{ .array = array };
}

pub fn schemaJsonForeignKeyAlloc(alloc: std.mem.Allocator, foreign_key: runtime_schema.ForeignKey) !std.json.Value {
    var reference = std.json.ObjectMap.empty;
    try putJsonString(alloc, &reference, "table", foreign_key.parent_table);
    try reference.put(alloc, try alloc.dupe(u8, "columns"), try schemaJsonStringArrayAlloc(alloc, foreign_key.parent_columns));
    if (foreign_key.parent_period) |period| try putJsonString(alloc, &reference, "period", period);

    var object = std.json.ObjectMap.empty;
    try putJsonString(alloc, &object, "name", foreign_key.name);
    try object.put(alloc, try alloc.dupe(u8, "columns"), try schemaJsonStringArrayAlloc(alloc, foreign_key.child_columns));
    if (foreign_key.child_period) |period| try putJsonString(alloc, &object, "period", period);
    try object.put(alloc, try alloc.dupe(u8, "references"), .{ .object = reference });
    try putJsonString(alloc, &object, "on_delete", ddl_plan.foreignKeyActionName(foreign_key.on_delete));
    try putJsonString(alloc, &object, "on_update", ddl_plan.foreignKeyActionName(foreign_key.on_update));
    try putJsonString(alloc, &object, "timing", ddl_plan.foreignKeyTimingName(foreign_key.timing));
    try object.put(alloc, try alloc.dupe(u8, "deferrable"), .{ .bool = foreign_key.deferrable });
    try putJsonString(alloc, &object, "match", ddl_plan.foreignKeyMatchName(foreign_key.match));
    try putJsonString(alloc, &object, "validation_state", ddl_plan.foreignKeyValidationStateName(foreign_key.validation_state));
    return .{ .object = object };
}

pub fn schemaJsonRelationalChecksAlloc(alloc: std.mem.Allocator, checks: []const runtime_schema.RelationalCheck) !std.json.Value {
    var array = std.json.Array.init(alloc);
    for (checks) |check| try array.append(try schemaJsonRelationalCheckAlloc(alloc, check));
    return .{ .array = array };
}

pub fn schemaJsonRelationalCheckAlloc(alloc: std.mem.Allocator, check: runtime_schema.RelationalCheck) !std.json.Value {
    var object = std.json.ObjectMap.empty;
    try putJsonString(alloc, &object, "name", check.name);
    if (check.validation_state != .enforced) try putJsonString(alloc, &object, "validation_state", ddl_plan.relationalCheckValidationStateName(check.validation_state));
    if (check.expression) |expression| {
        var expression_json_writer: std.Io.Writer.Allocating = .init(alloc);
        defer expression_json_writer.deinit();
        try expr_type.writeRowExpressionConditionJson(&expression_json_writer.writer, expression);
        const expression_json = try expression_json_writer.toOwnedSlice();
        defer alloc.free(expression_json);
        var parsed = try std.json.parseFromSlice(std.json.Value, alloc, expression_json, .{});
        defer parsed.deinit();
        try object.put(alloc, try alloc.dupe(u8, "expression"), try json_helpers.cloneJsonValue(alloc, parsed.value));
        return .{ .object = object };
    }
    try putJsonString(alloc, &object, "field", check.field);
    try putJsonString(alloc, &object, "op", ddl_plan.relationalCheckOpToken(check.op));
    if (check.collation) |collation| try putJsonString(alloc, &object, "collation", collation);
    if (check.value_json) |value_json| {
        var parsed = try std.json.parseFromSlice(std.json.Value, alloc, value_json, .{});
        defer parsed.deinit();
        try object.put(alloc, try alloc.dupe(u8, "value"), try json_helpers.cloneJsonValue(alloc, parsed.value));
    }
    return .{ .object = object };
}

pub fn schemaJsonStringArrayAlloc(alloc: std.mem.Allocator, values: []const []const u8) !std.json.Value {
    var array = std.json.Array.init(alloc);
    for (values) |value| try array.append(.{ .string = try alloc.dupe(u8, value) });
    return .{ .array = array };
}

pub fn schemaJsonRelationalIndexKeysAlloc(alloc: std.mem.Allocator, keys: []const runtime_schema.RelationalIndexKey) !std.json.Value {
    var array = std.json.Array.init(alloc);
    for (keys) |key| {
        var object = std.json.ObjectMap.empty;
        try putJsonString(alloc, &object, "column", key.column);
        try putJsonString(alloc, &object, "direction", switch (key.direction) {
            .asc => "asc",
            .desc => "desc",
        });
        try putJsonString(alloc, &object, "nulls", switch (key.nulls) {
            .default => "default",
            .first => "first",
            .last => "last",
        });
        try array.append(.{ .object = object });
    }
    return .{ .array = array };
}

pub fn putJsonString(alloc: std.mem.Allocator, object: *std.json.ObjectMap, key: []const u8, value: []const u8) !void {
    try object.put(alloc, try alloc.dupe(u8, key), .{ .string = try alloc.dupe(u8, value) });
}

pub fn applyCreateIndexPlanToSchemaJsonValue(
    alloc: std.mem.Allocator,
    root: *std.json.ObjectMap,
    plan: ddl_plan.CreateIndexPlan,
) !bool {
    const schema_parts = try relationalSchemaJsonParts(root);
    if (try schemaJsonIndexNameExists(schema_parts.properties, root.getPtr("unique_constraints"), plan.index_name)) {
        if (plan.if_not_exists) return false;
        return error.InvalidSqlCatalog;
    }
    if (plan.method == .gin) {
        if (plan.unique or plan.columns.len != 1 or plan.expressions.len != 0 or plan.generated_expression != null) return error.UnsupportedSqlShape;
        const property_type = try schemaJsonPropertyType(schema_parts.properties, plan.columns[0]);
        if (std.mem.eql(u8, property_type, "json")) {
            if (plan.opclass == .array_ops) return error.InvalidSqlCatalog;
        } else if (std.mem.eql(u8, property_type, "array")) {
            if (plan.opclass == .jsonb_path_ops) return error.InvalidSqlCatalog;
        } else return error.InvalidSqlCatalog;
        try validateCreateIndexIncludeColumnsForSchemaJsonProperties(schema_parts.properties, plan.columns, plan.include_columns);
    }
    if (plan.unique) {
        try validateCreateIndexIncludeColumnsForSchemaJsonProperties(schema_parts.properties, plan.columns, plan.include_columns);
        const constraint: runtime_schema.UniqueConstraint = .{
            .name = plan.index_name,
            .columns = plan.columns,
            .expressions = plan.expressions,
            .include_columns = plan.include_columns,
            .index_keys = plan.index_keys,
            .without_overlaps_period = plan.without_overlaps_period,
            .nulls_not_distinct = plan.nulls_not_distinct,
            .where = plan.where,
            .where_expressions = plan.where_expressions,
            .validation_state = .unvalidated,
        };
        var constraints = try rootArrayFieldAlloc(alloc, root, "unique_constraints");
        try constraints.append(try schemaJsonUniqueConstraintAlloc(alloc, constraint));
        return true;
    }

    const index_generation = ddl_plan.stableSecondaryIndexGeneration(plan);
    if (plan.generated_expression) |generated_expression| {
        if (plan.columns.len != 0 or plan.expressions.len != 0) return error.UnsupportedSqlShape;
        try validateCreateIndexIncludeColumnsForSchemaJsonProperties(schema_parts.properties, &.{}, plan.include_columns);
        try validateGeneratedExpressionForSchemaJsonProperties(schema_parts.properties, plan.index_name, generated_expression);
        const column: runtime_schema.RelationalColumn = .{
            .name = plan.index_name,
            .path = plan.index_name,
            .field_type = .keyword,
            .nullable = true,
            .indexed = true,
            .index_lifecycle = .building,
            .index_generation = index_generation,
            .index_name = plan.index_name,
            .index_include_columns = plan.include_columns,
            .index_keys = plan.index_keys,
            .generated = generated_expression,
            .index_where = plan.where,
            .index_where_expressions = plan.where_expressions,
        };
        try schema_parts.properties.put(alloc, try alloc.dupe(u8, plan.index_name), try schemaJsonPropertyFromColumnAlloc(alloc, column));
        return true;
    }

    if (plan.columns.len == 0 or plan.expressions.len != 0) return error.UnsupportedSqlShape;
    try validateCreateIndexIncludeColumnsForSchemaJsonProperties(schema_parts.properties, plan.columns, plan.include_columns);
    for (plan.columns) |column| {
        const property = schema_parts.properties.getPtr(column) orelse return error.InvalidSqlCatalog;
        if (property.* != .object) return error.InvalidSqlCatalog;
        if (property.object.get("x-antfly-index-name")) |existing| {
            if (existing != .string or !std.mem.eql(u8, existing.string, plan.index_name)) return error.InvalidSqlCatalog;
        }
    }
    for (plan.columns) |column| {
        const property = schema_parts.properties.getPtr(column) orelse return error.InvalidSqlCatalog;
        if (property.* != .object) return error.InvalidSqlCatalog;
        try property.object.put(alloc, try alloc.dupe(u8, "x-antfly-index"), .{ .bool = true });
        try putJsonString(alloc, &property.object, "x-antfly-index-lifecycle", "building");
        try property.object.put(alloc, try alloc.dupe(u8, "x-antfly-index-generation"), .{ .integer = @intCast(index_generation) });
        try putJsonString(alloc, &property.object, "x-antfly-index-name", plan.index_name);
        if (plan.include_columns.len > 0) try property.object.put(alloc, try alloc.dupe(u8, "x-antfly-index-include"), try schemaJsonStringArrayAlloc(alloc, plan.include_columns));
        if (plan.index_keys.len > 0) try property.object.put(alloc, try alloc.dupe(u8, "x-antfly-index-keys"), try schemaJsonRelationalIndexKeysAlloc(alloc, plan.index_keys));
        if (plan.where.len > 0) try property.object.put(alloc, try alloc.dupe(u8, "x-antfly-index-where"), try schemaJsonUniquePredicateDefinitionAlloc(alloc, plan.where));
        if (plan.where_expressions.len > 0) try property.object.put(alloc, try alloc.dupe(u8, "x-antfly-index-where-expressions"), try schemaJsonExpressionConditionsAlloc(alloc, plan.where_expressions));
    }
    return true;
}

pub fn applyDropIndexPlanToSchemaJsonValue(
    alloc: std.mem.Allocator,
    root: *std.json.ObjectMap,
    plan: ddl_plan.DropIndexPlan,
) !void {
    if (try removeNamedConstraintFromJsonArray(root.getPtr("unique_constraints"), plan.index_name)) {
        removeCommentMapEntry(root, "indexes", plan.index_name);
        return;
    }
    const schema_parts = try relationalSchemaJsonParts(root);
    const property = schemaJsonPropertyForSecondaryIndex(schema_parts.properties, plan.index_name) orelse {
        if (plan.if_exists) return;
        return error.InvalidSqlCatalog;
    };
    if (property.* != .object) return error.InvalidSqlCatalog;
    if (property.object.get("generated") != null) {
        if (!schema_parts.properties.orderedRemove(plan.index_name)) return error.InvalidSqlCatalog;
        return;
    }
    var removed = false;
    var it = schema_parts.properties.iterator();
    while (it.next()) |entry| {
        if (entry.value_ptr.* != .object) continue;
        if (!schemaJsonPropertyHasSecondaryIndexName(entry.value_ptr.*, plan.index_name)) continue;
        if (entry.value_ptr.object.get("generated") != null) return error.InvalidSqlCatalog;
        try entry.value_ptr.object.put(alloc, try alloc.dupe(u8, "x-antfly-index"), .{ .bool = false });
        _ = entry.value_ptr.object.orderedRemove("x-antfly-index-lifecycle");
        _ = entry.value_ptr.object.orderedRemove("x-antfly-index-generation");
        _ = entry.value_ptr.object.orderedRemove("x-antfly-index-name");
        _ = entry.value_ptr.object.orderedRemove("x-antfly-index-include");
        _ = entry.value_ptr.object.orderedRemove("x-antfly-index-keys");
        _ = entry.value_ptr.object.orderedRemove("x-antfly-index-where");
        _ = entry.value_ptr.object.orderedRemove("x-antfly-index-where-expressions");
        removed = true;
    }
    if (!removed) return error.InvalidSqlCatalog;
    removeCommentMapEntry(root, "indexes", plan.index_name);
}

pub fn applyCreateUpdatePolicyPlanToSchemaJsonValue(
    alloc: std.mem.Allocator,
    root: *std.json.ObjectMap,
    plan: ddl_plan.CreateUpdatePolicyPlan,
) !void {
    const schema_parts = try relationalSchemaJsonParts(root);
    const property = schema_parts.properties.getPtr(plan.column_name) orelse return error.InvalidSqlCatalog;
    if (property.* != .object) return error.InvalidSqlCatalog;
    try property.object.put(alloc, try alloc.dupe(u8, "x-antfly-on-update"), try schemaJsonDefaultValueAlloc(alloc, plan.on_update_value, true));
}

pub fn applyCommentMetadataPlanToSchemaJsonValue(
    alloc: std.mem.Allocator,
    root: *std.json.ObjectMap,
    plan: ddl_plan.CommentMetadataPlan,
) !void {
    const schema_parts = try relationalSchemaJsonParts(root);
    if (plan.object_name.len == 0) return error.InvalidSqlCatalog;

    const comments = try rootObjectFieldAlloc(alloc, root, "comments");
    switch (plan.target) {
        .table => try applyStringCommentValueToObject(alloc, comments, "table", plan.comment_json),
        .column => {
            const column_name = commentColumnName(plan.object_name);
            const property = schema_parts.properties.getPtr(column_name) orelse return error.InvalidSqlCatalog;
            if (property.* != .object) return error.InvalidSqlCatalog;
            const columns = try rootObjectFieldAlloc(alloc, comments, "columns");
            try applyStringCommentValueToObject(alloc, columns, column_name, plan.comment_json);
            removeEmptyCommentMap(comments, "columns");
        },
        .index => {
            if (!try schemaJsonIndexNameExists(schema_parts.properties, root.getPtr("unique_constraints"), plan.object_name)) return error.InvalidSqlCatalog;
            const indexes = try rootObjectFieldAlloc(alloc, comments, "indexes");
            try applyStringCommentValueToObject(alloc, indexes, plan.object_name, plan.comment_json);
            removeEmptyCommentMap(comments, "indexes");
        },
        .constraint => {
            const parent_table = plan.parent_table_name orelse return error.InvalidSqlCatalog;
            if (!try jsonConstraintNameExists(root, parent_table, plan.object_name)) return error.InvalidSqlCatalog;
            const constraints = try rootObjectFieldAlloc(alloc, comments, "constraints");
            try applyStringCommentValueToObject(alloc, constraints, plan.object_name, plan.comment_json);
            removeEmptyCommentMap(comments, "constraints");
        },
        .database => {
            const databases = try rootObjectFieldAlloc(alloc, comments, "databases");
            try applyStringCommentValueToObject(alloc, databases, plan.object_name, plan.comment_json);
            removeEmptyCommentMap(comments, "databases");
        },
        .schema => {
            const schemas = try rootObjectFieldAlloc(alloc, comments, "schemas");
            try applyStringCommentValueToObject(alloc, schemas, plan.object_name, plan.comment_json);
            removeEmptyCommentMap(comments, "schemas");
        },
        .extension => {
            const extensions = try rootObjectFieldAlloc(alloc, comments, "extensions");
            try applyStringCommentValueToObject(alloc, extensions, plan.object_name, plan.comment_json);
            removeEmptyCommentMap(comments, "extensions");
        },
        .type => {
            const types = try rootObjectFieldAlloc(alloc, comments, "types");
            try applyStringCommentValueToObject(alloc, types, plan.object_name, plan.comment_json);
            removeEmptyCommentMap(comments, "types");
        },
        .domain => {
            const domains = try rootObjectFieldAlloc(alloc, comments, "domains");
            try applyStringCommentValueToObject(alloc, domains, plan.object_name, plan.comment_json);
            removeEmptyCommentMap(comments, "domains");
        },
        .function => {
            const functions = try rootObjectFieldAlloc(alloc, comments, "functions");
            try applyStringCommentValueToObject(alloc, functions, plan.object_name, plan.comment_json);
            removeEmptyCommentMap(comments, "functions");
        },
        .procedure => {
            const procedures = try rootObjectFieldAlloc(alloc, comments, "procedures");
            try applyStringCommentValueToObject(alloc, procedures, plan.object_name, plan.comment_json);
            removeEmptyCommentMap(comments, "procedures");
        },
    }
    if (try schemaJsonCommentCountInObject(comments) == 0) _ = root.orderedRemove("comments");
}

pub fn applySecurityLabelPlanToSchemaJsonValue(
    alloc: std.mem.Allocator,
    root: *std.json.ObjectMap,
    plan: ddl_plan.SecurityLabelPlan,
) !void {
    if (plan.object_name.len == 0) return error.InvalidSqlCatalog;
    const labels = try rootObjectFieldAlloc(alloc, root, "security_labels");
    const target_map_name = switch (plan.target) {
        .table => "tables",
        .column => "columns",
        .index => "indexes",
        .constraint => return error.InvalidSqlCatalog,
        .database => "databases",
        .schema => "schemas",
        .extension => "extensions",
        .type => "types",
        .domain => "domains",
        .function => "functions",
        .procedure => "procedures",
    };
    const target_map = try rootObjectFieldAlloc(alloc, labels, target_map_name);
    const provider_key = plan.provider_name orelse "default";
    const object_labels = try rootObjectFieldAlloc(alloc, target_map, plan.object_name);
    try applyStringCommentValueToObject(alloc, object_labels, provider_key, plan.label_json);
    if (object_labels.count() == 0) removeEmptyCommentMap(target_map, plan.object_name);
    removeEmptyCommentMap(labels, target_map_name);
    if (try schemaJsonCommentCountInObject(labels) == 0) _ = root.orderedRemove("security_labels");
}

fn commentColumnName(object_name: []const u8) []const u8 {
    const dot = std.mem.lastIndexOfScalar(u8, object_name, '.') orelse return object_name;
    return object_name[dot + 1 ..];
}

fn applyStringCommentValueToObject(
    alloc: std.mem.Allocator,
    object: *std.json.ObjectMap,
    key: []const u8,
    comment_json: ?[]const u8,
) !void {
    const raw = comment_json orelse {
        _ = object.orderedRemove(key);
        return;
    };
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, raw, .{});
    defer parsed.deinit();
    const value = switch (parsed.value) {
        .string => |value| value,
        else => return error.UnsupportedSqlShape,
    };
    try putJsonString(alloc, object, key, value);
}

pub fn applyAlterTablePlanToSchemaJsonValue(
    alloc: std.mem.Allocator,
    root: *std.json.ObjectMap,
    plan: ddl_plan.AlterTablePlan,
) !void {
    const schema_parts = try relationalSchemaJsonParts(root);
    for (plan.operations) |operation| {
        switch (operation) {
            .add_column => |add_column| try addColumnOperationToSchemaJsonValue(alloc, root, schema_parts, add_column),
            .add_period => |period| {
                var periods = try rootArrayFieldAlloc(alloc, root, "periods");
                try periods.append(try schemaJsonPeriodAlloc(alloc, period));
            },
            .add_primary_key => |primary_key| {
                if (root.get("primary_key") != null) return error.InvalidSqlCatalog;
                try root.put(alloc, try alloc.dupe(u8, "primary_key"), try schemaJsonPrimaryKeyAlloc(alloc, primary_key));
            },
            .rename_column => |rename_column| try renameColumnInSchemaJsonValue(alloc, root, schema_parts, rename_column),
            .rename_constraint => |rename_constraint| try renameConstraintInSchemaJsonValue(alloc, root, plan.table_name, rename_constraint),
            .drop_column => |drop_column| try dropColumnFromSchemaJsonValue(alloc, root, schema_parts, drop_column),
            .drop_constraint => |drop_constraint| try dropConstraintFromSchemaJsonValue(root, plan.table_name, drop_constraint),
            .drop_update_policy => |drop_update_policy| try dropUpdatePolicyFromSchemaJsonValue(schema_parts, drop_update_policy),
            .alter_column_default => |alter_column_default| try alterColumnDefaultInSchemaJsonValue(alloc, schema_parts, alter_column_default),
            .alter_column_nullability => |alter_column_nullability| try alterColumnNullabilityInSchemaJsonValue(alloc, root, schema_parts, alter_column_nullability),
            .alter_column_type => |alter_column_type| try alterColumnTypeInSchemaJsonValue(alloc, schema_parts, alter_column_type),
            .add_unique_constraint => |constraint| {
                var constraints = try rootArrayFieldAlloc(alloc, root, "unique_constraints");
                try constraints.append(try schemaJsonUniqueConstraintAlloc(alloc, constraint));
            },
            .add_foreign_key => |foreign_key| {
                var foreign_keys = try rootArrayFieldAlloc(alloc, root, "foreign_keys");
                try foreign_keys.append(try schemaJsonForeignKeyAlloc(alloc, foreign_key));
            },
            .add_check => |check| {
                var checks = try rootArrayFieldAlloc(alloc, root, "checks");
                try checks.append(try schemaJsonRelationalCheckAlloc(alloc, check));
            },
            .validate_constraint => |constraint_name| try validateConstraintByNameInSchemaJson(alloc, root, plan.table_name, constraint_name),
        }
    }
    try pruneSchemaJsonCommentsForCurrentSchema(root, plan.table_name);
}

fn addColumnOperationToSchemaJsonValue(
    alloc: std.mem.Allocator,
    root: *std.json.ObjectMap,
    schema_parts: RelationalSchemaJsonParts,
    operation: ddl_plan.AddColumnOperation,
) !void {
    if (schema_parts.properties.get(operation.column.name) != null) {
        if (operation.if_not_exists) return;
        return error.InvalidSqlCatalog;
    }
    try schema_parts.properties.put(alloc, try alloc.dupe(u8, operation.column.name), try schemaJsonPropertyFromColumnAlloc(alloc, operation.column));
    if (!operation.column.nullable) {
        var required = try rootArrayFieldAlloc(alloc, schema_parts.schema, "required");
        try required.append(.{ .string = try alloc.dupe(u8, operation.column.name) });
    }
    if (operation.unique_constraints.len > 0) {
        var constraints = try rootArrayFieldAlloc(alloc, root, "unique_constraints");
        for (operation.unique_constraints) |constraint| try constraints.append(try schemaJsonUniqueConstraintAlloc(alloc, constraint));
    }
    if (operation.foreign_keys.len > 0) {
        var foreign_keys = try rootArrayFieldAlloc(alloc, root, "foreign_keys");
        for (operation.foreign_keys) |foreign_key| try foreign_keys.append(try schemaJsonForeignKeyAlloc(alloc, foreign_key));
    }
    if (operation.checks.len > 0) {
        var checks = try rootArrayFieldAlloc(alloc, root, "checks");
        for (operation.checks) |check| try checks.append(try schemaJsonRelationalCheckAlloc(alloc, check));
    }
}

fn dropUpdatePolicyFromSchemaJsonValue(
    schema_parts: RelationalSchemaJsonParts,
    operation: ddl_plan.DropUpdatePolicyOperation,
) !void {
    _ = operation.trigger_name;
    var policy_count: usize = 0;
    var it = schema_parts.properties.iterator();
    while (it.next()) |entry| {
        if (entry.value_ptr.* != .object) return error.InvalidSqlCatalog;
        if (entry.value_ptr.object.get("x-antfly-on-update") == null) continue;
        policy_count += 1;
    }
    if (policy_count == 0) {
        if (operation.if_exists) return;
        return error.InvalidSqlCatalog;
    }
    if (policy_count > 1) return error.InvalidSqlCatalog;

    var remove_it = schema_parts.properties.iterator();
    while (remove_it.next()) |entry| {
        if (entry.value_ptr.object.get("x-antfly-on-update") == null) continue;
        _ = entry.value_ptr.object.orderedRemove("x-antfly-on-update");
        return;
    }
    return error.InvalidSqlCatalog;
}

fn renameColumnInSchemaJsonValue(
    alloc: std.mem.Allocator,
    root: *std.json.ObjectMap,
    schema_parts: RelationalSchemaJsonParts,
    operation: ddl_plan.RenameColumnOperation,
) !void {
    if (std.mem.eql(u8, operation.old_name, operation.new_name)) return error.InvalidSqlCatalog;
    if (schema_parts.properties.get(operation.new_name) != null) return error.InvalidSqlCatalog;
    const property = schema_parts.properties.get(operation.old_name) orelse return error.InvalidSqlCatalog;
    _ = schema_parts.properties.orderedRemove(operation.old_name);
    try schema_parts.properties.put(alloc, try alloc.dupe(u8, operation.new_name), property);

    try renameStringInJsonArray(alloc, schema_parts.schema.getPtr("required"), operation.old_name, operation.new_name);
    if (root.getPtr("primary_key")) |primary_key| try renamePrimaryKeyJsonFields(alloc, primary_key, operation.old_name, operation.new_name);

    var property_it = schema_parts.properties.iterator();
    while (property_it.next()) |entry| {
        try renameSchemaPropertyReferences(alloc, entry.value_ptr, operation.old_name, operation.new_name);
    }

    try renameConstraintArrayFields(alloc, root.getPtr("unique_constraints"), operation.old_name, operation.new_name, .unique);
    try renameConstraintArrayFields(alloc, root.getPtr("foreign_keys"), operation.old_name, operation.new_name, .foreign_key);
    try renameConstraintArrayFields(alloc, root.getPtr("checks"), operation.old_name, operation.new_name, .check);
    try renameCommentMapEntry(alloc, root, "columns", operation.old_name, operation.new_name);
}

fn dropColumnFromSchemaJsonValue(
    alloc: std.mem.Allocator,
    root: *std.json.ObjectMap,
    schema_parts: RelationalSchemaJsonParts,
    drop_column: ddl_plan.DropColumnOperation,
) !void {
    if (schema_parts.properties.get(drop_column.name) == null) {
        if (drop_column.if_exists) return;
        return error.InvalidSqlCatalog;
    }

    var dropped = std.ArrayListUnmanaged([]const u8).empty;
    defer dropped.deinit(alloc);
    try appendUniqueBorrowedString(alloc, &dropped, drop_column.name);

    var changed = true;
    while (changed) {
        changed = false;
        var it = schema_parts.properties.iterator();
        while (it.next()) |entry| {
            if (stringSlicesContains(dropped.items, entry.key_ptr.*)) continue;
            if (propertyGeneratedReferencesAny(entry.value_ptr.*, dropped.items)) {
                try appendUniqueBorrowedString(alloc, &dropped, entry.key_ptr.*);
                changed = true;
            }
        }
    }

    try rejectPrimaryKeyDropFromSchemaJson(root, dropped.items);
    if (drop_column.dependency_mode == .restrict and try schemaJsonHasDropDependencies(root, dropped.items)) {
        return error.InvalidSqlCatalog;
    }

    for (dropped.items) |name| {
        if (!schema_parts.properties.orderedRemove(name)) return error.InvalidSqlCatalog;
    }
    try removeStringsFromJsonArray(schema_parts.schema.getPtr("required"), dropped.items);
    try removeDependentConstraintsFromJsonArray(root.getPtr("unique_constraints"), dropped.items, .unique);
    try removeDependentConstraintsFromJsonArray(root.getPtr("foreign_keys"), dropped.items, .foreign_key);
    try removeDependentConstraintsFromJsonArray(root.getPtr("checks"), dropped.items, .check);
}

fn schemaJsonHasDropDependencies(root: *std.json.ObjectMap, dropped: []const []const u8) !bool {
    if (dropped.len > 1) return true;
    if (try jsonConstraintArrayReferencesAny(root.getPtr("unique_constraints"), dropped, .unique)) return true;
    if (try jsonConstraintArrayReferencesAny(root.getPtr("foreign_keys"), dropped, .foreign_key)) return true;
    if (try jsonConstraintArrayReferencesAny(root.getPtr("checks"), dropped, .check)) return true;
    const schema_parts = try relationalSchemaJsonParts(root);
    var it = schema_parts.properties.iterator();
    while (it.next()) |entry| {
        if (schemaJsonSecondaryIndexReferencesAny(entry.value_ptr.*, dropped)) return true;
    }
    return false;
}

fn jsonConstraintArrayReferencesAny(
    value: ?*std.json.Value,
    fields: []const []const u8,
    kind: JsonConstraintKind,
) !bool {
    const array_value = value orelse return false;
    if (array_value.* != .array) return error.InvalidSqlCatalog;
    for (array_value.array.items) |item| {
        if (item != .object) return error.InvalidSqlCatalog;
        const references = switch (kind) {
            .unique => jsonUniqueConstraintReferencesAny(item, fields),
            .foreign_key => jsonStringArrayReferencesAny(item.object.get("columns") orelse return error.InvalidSqlCatalog, fields),
            .check => blk: {
                const field = item.object.get("field") orelse return error.InvalidSqlCatalog;
                break :blk field == .string and stringSlicesContains(fields, field.string);
            },
        };
        if (references) return true;
    }
    return false;
}

fn validateConstraintByNameInSchemaJson(
    alloc: std.mem.Allocator,
    root: *std.json.ObjectMap,
    table_name: []const u8,
    constraint_name: []const u8,
) !void {
    if (try schemaJsonPrimaryKeyNameEquals(root, table_name, constraint_name)) return;
    if (try setNamedConstraintValidationStateInArray(alloc, root, "unique_constraints", constraint_name, "enforced")) return;
    if (try setNamedConstraintValidationStateInArray(alloc, root, "foreign_keys", constraint_name, "enforced")) return;
    if (try setNamedConstraintValidationStateInArray(alloc, root, "checks", constraint_name, "enforced")) return;
    return error.InvalidSqlCatalog;
}

fn dropConstraintFromSchemaJsonValue(
    root: *std.json.ObjectMap,
    table_name: []const u8,
    drop_constraint: ddl_plan.DropConstraintOperation,
) !void {
    if (try dropPrimaryKeyFromSchemaJsonValue(root, table_name, drop_constraint.name)) {
        removeCommentMapEntry(root, "constraints", drop_constraint.name);
        return;
    }
    if (try removeNamedConstraintFromJsonArray(root.getPtr("unique_constraints"), drop_constraint.name)) return;
    if (try removeNamedConstraintFromJsonArray(root.getPtr("foreign_keys"), drop_constraint.name)) return;
    if (try removeNamedConstraintFromJsonArray(root.getPtr("checks"), drop_constraint.name)) return;
    if (drop_constraint.if_exists) return;
    return error.InvalidSqlCatalog;
}

fn dropPrimaryKeyFromSchemaJsonValue(
    root: *std.json.ObjectMap,
    table_name: []const u8,
    constraint_name: []const u8,
) !bool {
    if (!try schemaJsonPrimaryKeyNameEquals(root, table_name, constraint_name)) return false;
    _ = root.orderedRemove("primary_key");
    return true;
}

fn renameConstraintInSchemaJsonValue(
    alloc: std.mem.Allocator,
    root: *std.json.ObjectMap,
    table_name: []const u8,
    operation: ddl_plan.RenameConstraintOperation,
) !void {
    if (std.mem.eql(u8, operation.old_name, operation.new_name)) return error.InvalidSqlCatalog;
    if (try jsonConstraintNameExists(root, table_name, operation.new_name)) return error.InvalidSqlCatalog;
    const renamed =
        try renamePrimaryKeyConstraintInSchemaJsonValue(alloc, root, table_name, operation.old_name, operation.new_name) or
        try renameNamedConstraintInJsonArray(alloc, root.getPtr("unique_constraints"), operation.old_name, operation.new_name) or
        try renameNamedConstraintInJsonArray(alloc, root.getPtr("foreign_keys"), operation.old_name, operation.new_name) or
        try renameNamedConstraintInJsonArray(alloc, root.getPtr("checks"), operation.old_name, operation.new_name);
    if (!renamed) return error.InvalidSqlCatalog;
    try renameCommentMapEntry(alloc, root, "constraints", operation.old_name, operation.new_name);
    try renameCommentMapEntry(alloc, root, "indexes", operation.old_name, operation.new_name);
}

fn renamePrimaryKeyConstraintInSchemaJsonValue(
    alloc: std.mem.Allocator,
    root: *std.json.ObjectMap,
    table_name: []const u8,
    old_name: []const u8,
    new_name: []const u8,
) !bool {
    const primary_key = root.getPtr("primary_key") orelse return false;
    if (primary_key.* == .null) return false;
    if (primary_key.* != .object) return error.InvalidSqlCatalog;
    if (primary_key.object.get("name")) |existing| {
        if (existing != .string) return error.InvalidSqlCatalog;
        if (!std.mem.eql(u8, existing.string, old_name)) return false;
        _ = primary_key.object.orderedRemove("name");
        try putJsonString(alloc, &primary_key.object, "name", new_name);
        return true;
    }
    if (!binder.defaultPrimaryKeyNameEquals(table_name, old_name)) return false;
    _ = primary_key.object.orderedRemove("name");
    try putJsonString(alloc, &primary_key.object, "name", new_name);
    return true;
}

fn renameNamedConstraintInJsonArray(
    alloc: std.mem.Allocator,
    value: ?*std.json.Value,
    old_name: []const u8,
    new_name: []const u8,
) !bool {
    const array_value = value orelse return false;
    if (array_value.* != .array) return error.InvalidSqlCatalog;
    for (array_value.array.items) |*item| {
        if (item.* != .object) return error.InvalidSqlCatalog;
        const name = item.object.get("name") orelse return error.InvalidSqlCatalog;
        if (name != .string) return error.InvalidSqlCatalog;
        if (!std.mem.eql(u8, name.string, old_name)) continue;
        try putJsonString(alloc, &item.object, "name", new_name);
        return true;
    }
    return false;
}

fn alterColumnDefaultInSchemaJsonValue(
    alloc: std.mem.Allocator,
    schema_parts: RelationalSchemaJsonParts,
    operation: ddl_plan.AlterColumnDefaultOperation,
) !void {
    const property = schema_parts.properties.getPtr(operation.column_name) orelse return error.InvalidSqlCatalog;
    if (property.* != .object) return error.InvalidSqlCatalog;
    _ = property.object.orderedRemove("default");
    _ = property.object.orderedRemove("x-antfly-default");
    if (operation.default_value) |default_value| {
        const key = if (default_value.kind == .literal) "default" else "x-antfly-default";
        try property.object.put(alloc, try alloc.dupe(u8, key), try schemaJsonDefaultValueAlloc(alloc, default_value, default_value.kind != .literal));
    }
}

fn alterColumnNullabilityInSchemaJsonValue(
    alloc: std.mem.Allocator,
    root: *std.json.ObjectMap,
    schema_parts: RelationalSchemaJsonParts,
    operation: ddl_plan.AlterColumnNullabilityOperation,
) !void {
    if (schema_parts.properties.get(operation.column_name) == null) return error.InvalidSqlCatalog;
    if (operation.nullable) {
        try rejectPrimaryKeyDropFromSchemaJson(root, &.{operation.column_name});
        try removeStringsFromJsonArray(schema_parts.schema.getPtr("required"), &.{operation.column_name});
        return;
    }
    const required = try rootArrayFieldAlloc(alloc, schema_parts.schema, "required");
    try appendUniqueJsonString(alloc, required, operation.column_name);
}

fn alterColumnTypeInSchemaJsonValue(
    alloc: std.mem.Allocator,
    schema_parts: RelationalSchemaJsonParts,
    operation: ddl_plan.AlterColumnTypeOperation,
) !void {
    const property = schema_parts.properties.getPtr(operation.column_name) orelse return error.InvalidSqlCatalog;
    if (property.* != .object) return error.InvalidSqlCatalog;
    if (property.object.get("generated") != null) return error.UnsupportedSqlShape;
    const supports_collation = binder.relationalColumnTypeSupportsCollation(operation.field_type, operation.array_item_type);
    if (operation.collation != null and !supports_collation) return error.UnsupportedSqlShape;
    if (!supports_collation and property.object.get("collation") != null) return error.UnsupportedSqlShape;
    try putJsonString(alloc, &property.object, "type", ddl_plan.antflyTypeSchemaName(operation.field_type));
    _ = property.object.orderedRemove("items");
    if (operation.field_type == .array) {
        const item_type = operation.array_item_type orelse return error.InvalidSqlCatalog;
        var item_object = std.json.ObjectMap.empty;
        try putJsonString(alloc, &item_object, "type", ddl_plan.antflyTypeSchemaName(item_type));
        try property.object.put(alloc, try alloc.dupe(u8, "items"), .{ .object = item_object });
    }
    if (operation.collation) |collation| try putJsonString(alloc, &property.object, "collation", collation);
}

fn appendUniqueJsonString(
    alloc: std.mem.Allocator,
    array: *std.json.Array,
    value: []const u8,
) !void {
    for (array.items) |item| {
        if (item != .string) return error.InvalidSqlCatalog;
        if (std.mem.eql(u8, item.string, value)) return;
    }
    try array.append(.{ .string = try alloc.dupe(u8, value) });
}

fn renamePrimaryKeyJsonFields(
    alloc: std.mem.Allocator,
    primary_key: *std.json.Value,
    old_name: []const u8,
    new_name: []const u8,
) !void {
    if (primary_key.* != .object) return error.InvalidSqlCatalog;
    try renameStringInJsonArray(alloc, primary_key.object.getPtr("columns"), old_name, new_name);
    try renameStringInJsonArray(alloc, primary_key.object.getPtr("include_columns"), old_name, new_name);
}

fn renameSchemaPropertyReferences(
    alloc: std.mem.Allocator,
    property: *std.json.Value,
    old_name: []const u8,
    new_name: []const u8,
) !void {
    if (property.* != .object) return;
    if (property.object.getPtr("generated")) |generated| try renameGeneratedJsonFields(alloc, generated, old_name, new_name);
    try renameStringInJsonArray(alloc, property.object.getPtr("x-antfly-index-include"), old_name, new_name);
    if (property.object.getPtr("x-antfly-index-keys")) |index_keys| try renameRelationalIndexKeyJsonFields(alloc, index_keys, old_name, new_name);
    if (property.object.getPtr("x-antfly-index-where")) |where| try renameUniquePredicateDefinitionJsonFields(alloc, where, old_name, new_name);
    if (property.object.getPtr("x-antfly-index-where-expressions")) |where_expressions| try renameExpressionJsonFields(alloc, where_expressions, old_name, new_name);
}

fn renameGeneratedJsonFields(
    alloc: std.mem.Allocator,
    generated: *std.json.Value,
    old_name: []const u8,
    new_name: []const u8,
) !void {
    if (generated.* != .object) return error.InvalidSqlCatalog;
    try renameStringFieldInJsonObject(alloc, &generated.object, "field", old_name, new_name);
    try renameStringInJsonArray(alloc, generated.object.getPtr("fields"), old_name, new_name);
    if (generated.object.getPtr("expression")) |expression| try renameExpressionJsonFields(alloc, expression, old_name, new_name);
}

fn renameConstraintArrayFields(
    alloc: std.mem.Allocator,
    value: ?*std.json.Value,
    old_name: []const u8,
    new_name: []const u8,
    kind: JsonConstraintKind,
) !void {
    const array_value = value orelse return;
    if (array_value.* != .array) return error.InvalidSqlCatalog;
    for (array_value.array.items) |*item| {
        if (item.* != .object) return error.InvalidSqlCatalog;
        switch (kind) {
            .unique => {
                try renameStringInJsonArray(alloc, item.object.getPtr("columns"), old_name, new_name);
                if (item.object.getPtr("expressions")) |expressions| try renameUniqueExpressionJsonFields(alloc, expressions, old_name, new_name);
                try renameStringInJsonArray(alloc, item.object.getPtr("include_columns"), old_name, new_name);
                if (item.object.getPtr("index_keys")) |index_keys| try renameRelationalIndexKeyJsonFields(alloc, index_keys, old_name, new_name);
                if (item.object.getPtr("where")) |where| try renameUniquePredicateDefinitionJsonFields(alloc, where, old_name, new_name);
                if (item.object.getPtr("where_expressions")) |where_expressions| try renameExpressionJsonFields(alloc, where_expressions, old_name, new_name);
            },
            .foreign_key => try renameStringInJsonArray(alloc, item.object.getPtr("columns"), old_name, new_name),
            .check => {
                try renameStringFieldInJsonObject(alloc, &item.object, "field", old_name, new_name);
                if (item.object.getPtr("expression")) |expression| try renameExpressionJsonFields(alloc, expression, old_name, new_name);
            },
        }
    }
}

fn renameUniqueExpressionJsonFields(
    alloc: std.mem.Allocator,
    expressions: *std.json.Value,
    old_name: []const u8,
    new_name: []const u8,
) !void {
    if (expressions.* != .array) return error.InvalidSqlCatalog;
    for (expressions.array.items) |*expression| {
        if (expression.* != .object) return error.InvalidSqlCatalog;
        try renameStringFieldInJsonObject(alloc, &expression.object, "field", old_name, new_name);
        if (expression.object.getPtr("expression")) |row_expression| try renameExpressionJsonFields(alloc, row_expression, old_name, new_name);
    }
}

fn renameUniquePredicateDefinitionJsonFields(
    alloc: std.mem.Allocator,
    value: *std.json.Value,
    old_name: []const u8,
    new_name: []const u8,
) !void {
    if (value.* != .object) return error.InvalidSqlCatalog;
    const all = value.object.getPtr("all") orelse return;
    if (all.* != .array) return error.InvalidSqlCatalog;
    for (all.array.items) |*item| {
        if (item.* != .object) return error.InvalidSqlCatalog;
        try renameStringFieldInJsonObject(alloc, &item.object, "field", old_name, new_name);
    }
}

fn renameExpressionJsonFields(
    alloc: std.mem.Allocator,
    value: *std.json.Value,
    old_name: []const u8,
    new_name: []const u8,
) !void {
    switch (value.*) {
        .object => |*object| {
            try renameStringFieldInJsonObject(alloc, object, "field", old_name, new_name);
            var it = object.iterator();
            while (it.next()) |entry| {
                try renameExpressionJsonFields(alloc, entry.value_ptr, old_name, new_name);
            }
        },
        .array => |*array| {
            for (array.items) |*item| {
                try renameExpressionJsonFields(alloc, item, old_name, new_name);
            }
        },
        else => {},
    }
}

fn renameRelationalIndexKeyJsonFields(
    alloc: std.mem.Allocator,
    keys: *std.json.Value,
    old_name: []const u8,
    new_name: []const u8,
) !void {
    if (keys.* != .array) return error.InvalidSqlCatalog;
    for (keys.array.items) |*item| {
        if (item.* != .object) return error.InvalidSqlCatalog;
        try renameStringFieldInJsonObject(alloc, &item.object, "column", old_name, new_name);
    }
}

fn renameStringInJsonArray(
    alloc: std.mem.Allocator,
    value: ?*std.json.Value,
    old_name: []const u8,
    new_name: []const u8,
) !void {
    const array_value = value orelse return;
    if (array_value.* != .array) return error.InvalidSqlCatalog;
    for (array_value.array.items) |*item| {
        if (item.* != .string) return error.InvalidSqlCatalog;
        if (std.mem.eql(u8, item.string, old_name)) item.* = .{ .string = try alloc.dupe(u8, new_name) };
    }
}

fn renameStringFieldInJsonObject(
    alloc: std.mem.Allocator,
    object: *std.json.ObjectMap,
    field_name: []const u8,
    old_name: []const u8,
    new_name: []const u8,
) !void {
    const value = object.get(field_name) orelse return;
    if (value != .string) return error.InvalidSqlCatalog;
    if (!std.mem.eql(u8, value.string, old_name)) return;
    try putJsonString(alloc, object, field_name, new_name);
}

fn setNamedConstraintValidationStateInArray(
    alloc: std.mem.Allocator,
    root: *std.json.ObjectMap,
    field: []const u8,
    constraint_name: []const u8,
    validation_state: []const u8,
) !bool {
    const value = root.getPtr(field) orelse return false;
    if (value.* != .array) return error.InvalidSqlCatalog;
    for (value.array.items) |*item| {
        if (item.* != .object) return error.InvalidSqlCatalog;
        const name = item.object.get("name") orelse return error.InvalidSqlCatalog;
        if (name != .string) return error.InvalidSqlCatalog;
        if (!std.mem.eql(u8, name.string, constraint_name)) continue;
        try putJsonString(alloc, &item.object, "validation_state", validation_state);
        return true;
    }
    return false;
}

fn appendUniqueBorrowedString(
    alloc: std.mem.Allocator,
    values: *std.ArrayListUnmanaged([]const u8),
    value: []const u8,
) !void {
    if (stringSlicesContains(values.items, value)) return;
    try values.append(alloc, value);
}

fn propertyGeneratedReferencesAny(property: std.json.Value, fields: []const []const u8) bool {
    if (property != .object) return false;
    const generated = property.object.get("generated") orelse return false;
    if (generated != .object) return false;
    if (generated.object.get("field")) |field| {
        if (field == .string and stringSlicesContains(fields, field.string)) return true;
    }
    if (generated.object.get("fields")) |fields_value| {
        if (jsonStringArrayReferencesAny(fields_value, fields)) return true;
    }
    if (generated.object.get("expression")) |expression| {
        if (jsonExpressionReferencesAny(expression, fields)) return true;
    }
    return false;
}

fn removeStringsFromJsonArray(value: ?*std.json.Value, fields: []const []const u8) !void {
    const array_value = value orelse return;
    if (array_value.* != .array) return error.InvalidSqlCatalog;
    var i: usize = 0;
    while (i < array_value.array.items.len) {
        const item = array_value.array.items[i];
        if (item != .string) return error.InvalidSqlCatalog;
        if (stringSlicesContains(fields, item.string)) {
            _ = array_value.array.orderedRemove(i);
        } else {
            i += 1;
        }
    }
}

fn rejectPrimaryKeyDropFromSchemaJson(root: *std.json.ObjectMap, fields: []const []const u8) !void {
    const primary_key = root.get("primary_key") orelse return;
    if (primary_key == .null) return;
    if (primary_key != .object) return error.InvalidSqlCatalog;
    const columns = primary_key.object.get("columns") orelse return error.InvalidSqlCatalog;
    if (jsonStringArrayReferencesAny(columns, fields)) return error.UnsupportedSqlShape;
    if (primary_key.object.get("include_columns")) |include_columns| {
        if (jsonStringArrayReferencesAny(include_columns, fields)) return error.UnsupportedSqlShape;
    }
}

const JsonConstraintKind = enum {
    unique,
    foreign_key,
    check,
};

fn removeDependentConstraintsFromJsonArray(
    value: ?*std.json.Value,
    fields: []const []const u8,
    kind: JsonConstraintKind,
) !void {
    const array_value = value orelse return;
    if (array_value.* != .array) return error.InvalidSqlCatalog;
    var i: usize = 0;
    while (i < array_value.array.items.len) {
        const item = array_value.array.items[i];
        if (item != .object) return error.InvalidSqlCatalog;
        const remove = switch (kind) {
            .unique => jsonUniqueConstraintReferencesAny(item, fields),
            .foreign_key => jsonStringArrayReferencesAny(item.object.get("columns") orelse return error.InvalidSqlCatalog, fields),
            .check => blk: {
                const field = item.object.get("field") orelse return error.InvalidSqlCatalog;
                break :blk field == .string and stringSlicesContains(fields, field.string);
            },
        };
        if (remove) {
            _ = array_value.array.orderedRemove(i);
        } else {
            i += 1;
        }
    }
}

fn jsonUniqueConstraintReferencesAny(value: std.json.Value, fields: []const []const u8) bool {
    if (value != .object) return false;
    if (value.object.get("columns")) |columns| {
        if (jsonStringArrayReferencesAny(columns, fields)) return true;
    }
    if (value.object.get("expressions")) |expressions| {
        if (expressions == .array) {
            for (expressions.array.items) |expression| {
                if (expression != .object) continue;
                const field = expression.object.get("field") orelse continue;
                if (field == .string and stringSlicesContains(fields, field.string)) return true;
            }
        }
    }
    if (value.object.get("include_columns")) |include_columns| {
        if (jsonStringArrayReferencesAny(include_columns, fields)) return true;
    }
    if (value.object.get("index_keys")) |index_keys| {
        if (jsonRelationalIndexKeysReferenceAny(index_keys, fields)) return true;
    }
    if (value.object.get("where")) |where| {
        if (jsonUniquePredicateDefinitionReferencesAny(where, fields)) return true;
    }
    return false;
}

pub fn relationalSchemaJsonParts(root: *std.json.ObjectMap) !RelationalSchemaJsonParts {
    const storage_mode = root.get("storage_mode") orelse return error.InvalidSqlCatalog;
    if (storage_mode != .string or !std.mem.eql(u8, storage_mode.string, "relational")) return error.InvalidSqlCatalog;
    const default_type = root.get("default_type") orelse return error.InvalidSqlCatalog;
    if (default_type != .string or default_type.string.len == 0) return error.InvalidSqlCatalog;
    const document_schemas = root.getPtr("document_schemas") orelse return error.InvalidSqlCatalog;
    if (document_schemas.* != .object) return error.InvalidSqlCatalog;
    const document_schema = document_schemas.object.getPtr(default_type.string) orelse return error.InvalidSqlCatalog;
    if (document_schema.* != .object) return error.InvalidSqlCatalog;
    const schema = document_schema.object.getPtr("schema") orelse return error.InvalidSqlCatalog;
    if (schema.* != .object) return error.InvalidSqlCatalog;
    const properties = schema.object.getPtr("properties") orelse return error.InvalidSqlCatalog;
    if (properties.* != .object) return error.InvalidSqlCatalog;
    return .{ .schema = &schema.object, .properties = &properties.object };
}

pub fn rootArrayFieldAlloc(alloc: std.mem.Allocator, object: *std.json.ObjectMap, field: []const u8) !*std.json.Array {
    const entry = try object.getOrPut(alloc, field);
    if (!entry.found_existing) {
        entry.key_ptr.* = try alloc.dupe(u8, field);
        entry.value_ptr.* = .{ .array = std.json.Array.init(alloc) };
    }
    if (entry.value_ptr.* != .array) return error.InvalidSqlCatalog;
    return &entry.value_ptr.array;
}

pub fn rootObjectFieldAlloc(alloc: std.mem.Allocator, object: *std.json.ObjectMap, field: []const u8) !*std.json.ObjectMap {
    const entry = try object.getOrPut(alloc, field);
    if (!entry.found_existing) {
        entry.key_ptr.* = try alloc.dupe(u8, field);
        entry.value_ptr.* = .{ .object = std.json.ObjectMap.empty };
    }
    if (entry.value_ptr.* != .object) return error.InvalidSqlCatalog;
    return &entry.value_ptr.object;
}

pub fn schemaJsonPropertyForSecondaryIndex(properties: *std.json.ObjectMap, index_name: []const u8) ?*std.json.Value {
    var it = properties.iterator();
    while (it.next()) |entry| {
        if (schemaJsonPropertyHasSecondaryIndexName(entry.value_ptr.*, index_name)) return entry.value_ptr;
    }
    return properties.getPtr(index_name);
}

pub fn schemaJsonPropertyHasSecondaryIndexName(property: std.json.Value, index_name: []const u8) bool {
    if (property != .object) return false;
    const declared = property.object.get("x-antfly-index-name") orelse return false;
    return declared == .string and std.mem.eql(u8, declared.string, index_name);
}

pub fn validateGeneratedExpressionForSchemaJsonProperties(
    properties: *std.json.ObjectMap,
    index_name: []const u8,
    generated: runtime_schema.RelationalGeneratedValue,
) !void {
    switch (generated.op) {
        .lower, .upper, .md5 => {
            const field = generated.field orelse return error.InvalidSqlCatalog;
            try validateGeneratedExpressionSourceForSchemaJsonProperties(properties, index_name, field);
        },
        .concat, .concat_ws => {
            if (generated.fields.len == 0) return error.InvalidSqlCatalog;
            for (generated.fields) |field| {
                try validateGeneratedExpressionSourceForSchemaJsonProperties(properties, index_name, field);
            }
        },
        .expression => {
            const expression = generated.expression orelse return error.InvalidSqlCatalog;
            try validateGeneratedExpressionJsonExpressionForSchemaJsonProperties(properties, index_name, expression);
        },
    }
}

fn validateGeneratedExpressionJsonExpressionForSchemaJsonProperties(
    properties: *std.json.ObjectMap,
    index_name: []const u8,
    expression: db_mod.types.RelationalRowsExpression,
) error{InvalidSqlCatalog}!void {
    if (expression.kind == .field) try validateGeneratedExpressionFieldExistsForSchemaJsonProperties(properties, index_name, expression.field);
    for (expression.operands) |operand| try validateGeneratedExpressionJsonExpressionForSchemaJsonProperties(properties, index_name, operand);
    for (expression.case_branches) |branch| {
        try validateGeneratedExpressionJsonConditionForSchemaJsonProperties(properties, index_name, branch.when);
        try validateGeneratedExpressionJsonExpressionForSchemaJsonProperties(properties, index_name, branch.then);
    }
    for (expression.case_else) |case_else| try validateGeneratedExpressionJsonExpressionForSchemaJsonProperties(properties, index_name, case_else);
}

fn validateGeneratedExpressionJsonConditionForSchemaJsonProperties(
    properties: *std.json.ObjectMap,
    index_name: []const u8,
    condition: db_mod.types.RelationalRowsExpressionCondition,
) error{InvalidSqlCatalog}!void {
    try validateGeneratedExpressionJsonExpressionForSchemaJsonProperties(properties, index_name, condition.lhs);
    for (condition.rhs) |rhs| try validateGeneratedExpressionJsonExpressionForSchemaJsonProperties(properties, index_name, rhs);
}

fn validateGeneratedExpressionFieldExistsForSchemaJsonProperties(
    properties: *std.json.ObjectMap,
    index_name: []const u8,
    field: []const u8,
) !void {
    if (std.mem.eql(u8, field, index_name)) return error.InvalidSqlCatalog;
    const property = properties.get(field) orelse return error.InvalidSqlCatalog;
    if (property != .object) return error.InvalidSqlCatalog;
}

fn validateGeneratedExpressionSourceForSchemaJsonProperties(
    properties: *std.json.ObjectMap,
    index_name: []const u8,
    field: []const u8,
) !void {
    if (std.mem.eql(u8, field, index_name)) return error.InvalidSqlCatalog;
    const property = properties.get(field) orelse return error.InvalidSqlCatalog;
    if (property != .object) return error.InvalidSqlCatalog;
    const property_type = property.object.get("type") orelse return error.InvalidSqlCatalog;
    if (property_type != .string) return error.InvalidSqlCatalog;
    if (std.mem.eql(u8, property_type.string, "json") or std.mem.eql(u8, property_type.string, "array")) return error.InvalidSqlCatalog;
}

pub fn schemaJsonPropertyType(
    properties: *std.json.ObjectMap,
    field: []const u8,
) ![]const u8 {
    const property = properties.get(field) orelse return error.InvalidSqlCatalog;
    if (property != .object) return error.InvalidSqlCatalog;
    const property_type = property.object.get("type") orelse return error.InvalidSqlCatalog;
    if (property_type != .string) return error.InvalidSqlCatalog;
    return property_type.string;
}

pub fn validateCreateIndexIncludeColumnsForSchemaJsonProperties(
    properties: *std.json.ObjectMap,
    key_columns: []const []const u8,
    include_columns: []const []const u8,
) !void {
    for (include_columns) |column| {
        if (stringSlicesContains(key_columns, column)) return error.InvalidSqlCatalog;
        const property = properties.get(column) orelse return error.InvalidSqlCatalog;
        if (property != .object) return error.InvalidSqlCatalog;
        const property_type = property.object.get("type") orelse return error.InvalidSqlCatalog;
        if (property_type != .string) return error.InvalidSqlCatalog;
        if (std.mem.eql(u8, property_type.string, "json") or std.mem.eql(u8, property_type.string, "array")) return error.InvalidSqlCatalog;
    }
}

pub fn schemaJsonIndexNameExists(
    properties: *std.json.ObjectMap,
    unique_constraints: ?*std.json.Value,
    index_name: []const u8,
) !bool {
    if (try jsonConstraintArrayNameExists(unique_constraints, index_name)) return true;
    return schemaJsonPropertyForSecondaryIndex(properties, index_name) != null;
}

fn jsonConstraintArrayNameExists(
    value: ?*std.json.Value,
    constraint_name: []const u8,
) !bool {
    const array_value = value orelse return false;
    if (array_value.* != .array) return error.InvalidSqlCatalog;
    for (array_value.array.items) |item| {
        if (item != .object) return error.InvalidSqlCatalog;
        const name = item.object.get("name") orelse return error.InvalidSqlCatalog;
        if (name != .string) return error.InvalidSqlCatalog;
        if (std.mem.eql(u8, name.string, constraint_name)) return true;
    }
    return false;
}

pub fn schemaJsonSecondaryIndexReferencesAny(property: std.json.Value, fields: []const []const u8) bool {
    if (property != .object) return false;
    if (property.object.get("x-antfly-index-name") == null) return false;
    if (property.object.get("x-antfly-index-include")) |include| {
        if (jsonStringArrayReferencesAny(include, fields)) return true;
    }
    if (property.object.get("x-antfly-index-keys")) |index_keys| {
        if (jsonRelationalIndexKeysReferenceAny(index_keys, fields)) return true;
    }
    if (property.object.get("x-antfly-index-where")) |where| {
        if (jsonUniquePredicateDefinitionReferencesAny(where, fields)) return true;
    }
    if (property.object.get("x-antfly-index-where-expressions")) |where_expressions| {
        if (jsonExpressionReferencesAny(where_expressions, fields)) return true;
    }
    return false;
}

pub fn removeEmptyCommentMap(comments: *std.json.ObjectMap, key: []const u8) void {
    const value = comments.get(key) orelse return;
    if (value != .object or value.object.count() != 0) return;
    _ = comments.orderedRemove(key);
}

pub fn schemaJsonCommentCountInRoot(root: *std.json.ObjectMap) !usize {
    const comments = root.get("comments") orelse return 0;
    if (comments != .object) return error.InvalidSqlCatalog;
    return try schemaJsonCommentCountInObject(&comments.object);
}

pub fn schemaJsonCommentCountInObject(comments: *const std.json.ObjectMap) !usize {
    var count: usize = 0;
    if (comments.get("table")) |table| {
        if (table != .string) return error.InvalidSqlCatalog;
        count += 1;
    }
    count += try schemaJsonCommentMapCount(comments.get("columns"));
    count += try schemaJsonCommentMapCount(comments.get("indexes"));
    count += try schemaJsonCommentMapCount(comments.get("constraints"));
    return count;
}

fn schemaJsonCommentMapCount(value: ?std.json.Value) !usize {
    const object_value = value orelse return 0;
    if (object_value != .object) return error.InvalidSqlCatalog;
    var it = object_value.object.iterator();
    var count: usize = 0;
    while (it.next()) |entry| {
        if (entry.value_ptr.* != .string) return error.InvalidSqlCatalog;
        count += 1;
    }
    return count;
}

pub fn removeCommentMapEntry(root: *std.json.ObjectMap, map_name: []const u8, key: []const u8) void {
    const comments_value = root.getPtr("comments") orelse return;
    if (comments_value.* != .object) return;
    const map_value = comments_value.object.getPtr(map_name) orelse return;
    if (map_value.* != .object) return;
    _ = map_value.object.orderedRemove(key);
    removeEmptyCommentMap(&comments_value.object, map_name);
    if ((schemaJsonCommentCountInObject(&comments_value.object) catch 1) == 0) _ = root.orderedRemove("comments");
}

pub fn renameCommentMapEntry(
    alloc: std.mem.Allocator,
    root: *std.json.ObjectMap,
    map_name: []const u8,
    old_name: []const u8,
    new_name: []const u8,
) !void {
    const comments_value = root.getPtr("comments") orelse return;
    if (comments_value.* != .object) return error.InvalidSqlCatalog;
    const map_value = comments_value.object.getPtr(map_name) orelse return;
    if (map_value.* != .object) return error.InvalidSqlCatalog;
    const comment = map_value.object.get(old_name) orelse return;
    if (comment != .string) return error.InvalidSqlCatalog;
    _ = map_value.object.orderedRemove(old_name);
    try putJsonString(alloc, &map_value.object, new_name, comment.string);
}

pub fn pruneSchemaJsonCommentsForCurrentSchema(root: *std.json.ObjectMap, table_name: []const u8) !void {
    const comments_value = root.getPtr("comments") orelse return;
    if (comments_value.* != .object) return error.InvalidSqlCatalog;
    const schema_parts = try relationalSchemaJsonParts(root);

    if (comments_value.object.getPtr("columns")) |columns| {
        if (columns.* != .object) return error.InvalidSqlCatalog;
        while (true) {
            var removed = false;
            var it = columns.object.iterator();
            while (it.next()) |entry| {
                if (entry.value_ptr.* != .string) return error.InvalidSqlCatalog;
                if (schema_parts.properties.get(entry.key_ptr.*) != null) continue;
                _ = columns.object.orderedRemove(entry.key_ptr.*);
                removed = true;
                break;
            }
            if (!removed) break;
        }
        removeEmptyCommentMap(&comments_value.object, "columns");
    }
    if (comments_value.object.getPtr("indexes")) |indexes| {
        if (indexes.* != .object) return error.InvalidSqlCatalog;
        while (true) {
            var removed = false;
            var it = indexes.object.iterator();
            while (it.next()) |entry| {
                if (entry.value_ptr.* != .string) return error.InvalidSqlCatalog;
                if (try schemaJsonIndexNameExists(schema_parts.properties, root.getPtr("unique_constraints"), entry.key_ptr.*)) continue;
                _ = indexes.object.orderedRemove(entry.key_ptr.*);
                removed = true;
                break;
            }
            if (!removed) break;
        }
        removeEmptyCommentMap(&comments_value.object, "indexes");
    }
    if (comments_value.object.getPtr("constraints")) |constraints| {
        if (constraints.* != .object) return error.InvalidSqlCatalog;
        while (true) {
            var removed = false;
            var it = constraints.object.iterator();
            while (it.next()) |entry| {
                if (entry.value_ptr.* != .string) return error.InvalidSqlCatalog;
                if (try jsonConstraintNameExists(root, table_name, entry.key_ptr.*)) continue;
                _ = constraints.object.orderedRemove(entry.key_ptr.*);
                removed = true;
                break;
            }
            if (!removed) break;
        }
        removeEmptyCommentMap(&comments_value.object, "constraints");
    }
    if (try schemaJsonCommentCountInObject(&comments_value.object) == 0) _ = root.orderedRemove("comments");
}

pub fn jsonConstraintNameExists(root: *std.json.ObjectMap, table_name: []const u8, constraint_name: []const u8) !bool {
    if (try schemaJsonPrimaryKeyNameEquals(root, table_name, constraint_name)) return true;
    if (try jsonConstraintArrayNameExists(root.getPtr("unique_constraints"), constraint_name)) return true;
    if (try jsonConstraintArrayNameExists(root.getPtr("foreign_keys"), constraint_name)) return true;
    if (try jsonConstraintArrayNameExists(root.getPtr("checks"), constraint_name)) return true;
    return false;
}

pub fn schemaJsonPrimaryKeyNameEquals(root: *std.json.ObjectMap, table_name: []const u8, constraint_name: []const u8) !bool {
    const primary_key = root.get("primary_key") orelse return false;
    if (primary_key == .null) return false;
    if (primary_key != .object) return error.InvalidSqlCatalog;
    if (primary_key.object.get("name")) |name| {
        if (name != .string) return error.InvalidSqlCatalog;
        return std.mem.eql(u8, name.string, constraint_name);
    }
    return binder.defaultPrimaryKeyNameEquals(table_name, constraint_name);
}

pub fn removeNamedConstraintFromJsonArray(
    value: ?*std.json.Value,
    constraint_name: []const u8,
) !bool {
    const array_value = value orelse return false;
    if (array_value.* != .array) return error.InvalidSqlCatalog;
    var i: usize = 0;
    while (i < array_value.array.items.len) : (i += 1) {
        const item = array_value.array.items[i];
        if (item != .object) return error.InvalidSqlCatalog;
        const name = item.object.get("name") orelse return error.InvalidSqlCatalog;
        if (name != .string) return error.InvalidSqlCatalog;
        if (!std.mem.eql(u8, name.string, constraint_name)) continue;
        _ = array_value.array.orderedRemove(i);
        return true;
    }
    return false;
}

fn jsonExpressionReferencesAny(value: std.json.Value, fields: []const []const u8) bool {
    switch (value) {
        .object => |object| {
            if (object.get("field")) |field| {
                if (field == .string and stringSlicesContains(fields, field.string)) return true;
            }
            var it = object.iterator();
            while (it.next()) |entry| {
                if (jsonExpressionReferencesAny(entry.value_ptr.*, fields)) return true;
            }
        },
        .array => |array| {
            for (array.items) |item| {
                if (jsonExpressionReferencesAny(item, fields)) return true;
            }
        },
        else => {},
    }
    return false;
}

fn jsonUniquePredicateDefinitionReferencesAny(value: std.json.Value, fields: []const []const u8) bool {
    if (value != .object) return false;
    const all = value.object.get("all") orelse return false;
    if (all != .array) return false;
    for (all.array.items) |item| {
        if (item != .object) continue;
        const field = item.object.get("field") orelse continue;
        if (field == .string and stringSlicesContains(fields, field.string)) return true;
    }
    return false;
}

fn jsonStringArrayReferencesAny(value: std.json.Value, fields: []const []const u8) bool {
    if (value != .array) return false;
    for (value.array.items) |item| {
        if (item == .string and stringSlicesContains(fields, item.string)) return true;
    }
    return false;
}

fn jsonRelationalIndexKeysReferenceAny(value: std.json.Value, fields: []const []const u8) bool {
    if (value != .array) return false;
    for (value.array.items) |item| {
        if (item != .object) continue;
        const column = item.object.get("column") orelse continue;
        if (column == .string and stringSlicesContains(fields, column.string)) return true;
    }
    return false;
}

fn stringSlicesContains(values: []const []const u8, value: []const u8) bool {
    for (values) |candidate| {
        if (std.mem.eql(u8, candidate, value)) return true;
    }
    return false;
}

test "schema json emits sequence-backed relational defaults" {
    const alloc = std.testing.allocator;
    var value = try schemaJsonDefaultValueAlloc(alloc, .{
        .kind = .sequence_next,
        .value_json = "{\"sequence\":\"usage_id_seq\",\"database\":\"tenant\",\"schema\":\"billing\"}",
    }, true);
    defer json_helpers.deinitJsonValue(alloc, &value);

    const encoded = try std.json.Stringify.valueAlloc(alloc, value, .{});
    defer alloc.free(encoded);
    try std.testing.expectEqualStrings("{\"op\":\"sequence_next\",\"sequence\":\"usage_id_seq\",\"database\":\"tenant\",\"schema\":\"billing\"}", encoded);
}

test "schema json emits relational check collation" {
    const alloc = std.testing.allocator;
    var value = try schemaJsonRelationalCheckAlloc(alloc, .{
        .name = "status_case_match",
        .field = "status",
        .op = .eq,
        .value_json = "\"OPEN\"",
        .collation = "antfly.case_insensitive",
    });
    defer json_helpers.deinitJsonValue(alloc, &value);

    const encoded = try std.json.Stringify.valueAlloc(alloc, value, .{});
    defer alloc.free(encoded);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"collation\":\"antfly.case_insensitive\"") != null);
}
