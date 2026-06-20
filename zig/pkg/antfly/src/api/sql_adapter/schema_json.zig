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

const db_mod = @import("../../storage/db/mod.zig");
const ddl_plan = @import("ddl_plan.zig");
const json_helpers = @import("../json_helpers.zig");
const lower_expr = @import("lower_expr.zig");
const runtime_schema = @import("../../storage/schema.zig");

pub fn schemaJsonValueFromCreateTablePlanAlloc(alloc: std.mem.Allocator, plan: ddl_plan.CreateTablePlan) !std.json.Value {
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
    });
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
        try lower_expr.writeRowExpressionConditionJson(writer, condition);
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
    try lower_expr.writeRowExpressionJson(writer, expression);
    const json = try out.toOwnedSlice();
    defer alloc.free(json);
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, json, .{});
    defer parsed.deinit();
    return try json_helpers.cloneJsonValue(alloc, parsed.value);
}

pub fn schemaJsonUniquePredicateAlloc(alloc: std.mem.Allocator, predicate: runtime_schema.UniquePredicate) !std.json.Value {
    var object = std.json.ObjectMap.empty;
    try putJsonString(alloc, &object, "field", predicate.field);
    try putJsonString(alloc, &object, "op", lower_expr.uniquePredicateOpToken(predicate.op));
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
    try putJsonString(alloc, &object, "field", check.field);
    try putJsonString(alloc, &object, "op", ddl_plan.relationalCheckOpToken(check.op));
    if (check.validation_state != .enforced) try putJsonString(alloc, &object, "validation_state", ddl_plan.relationalCheckValidationStateName(check.validation_state));
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

pub fn putJsonString(alloc: std.mem.Allocator, object: *std.json.ObjectMap, key: []const u8, value: []const u8) !void {
    try object.put(alloc, try alloc.dupe(u8, key), .{ .string = try alloc.dupe(u8, value) });
}
