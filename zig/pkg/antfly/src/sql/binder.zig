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

const metadata_api = @import("../metadata/api.zig");
const metadata_table_manager = @import("../metadata/table_manager.zig");
const metadata_transition_state = @import("../metadata/transition_state.zig");
const raft_reconciler = @import("../raft/reconciler.zig");
const db_mod = @import("../storage/db/mod.zig");
const ddl_plan = @import("ddl.zig");
const runtime_schema = @import("../storage/schema.zig");
const schema_api = @import("../schema/mod.zig");
const catalog_resources = @import("catalog_resources.zig");
const table_catalog = @import("../api/table_catalog.zig");
const sql_statement_kind = @import("statement_kind.zig");
const grammar = @import("grammar.zig");
const lexer = @import("lexer.zig");
const lower_expr = @import("lower_expr.zig");
const plan_mod = @import("plan.zig");
const parser = @import("parser.zig");
const source_binding = @import("source_binding.zig");
const token_mod = @import("token.zig");
const tokenized = @import("tokenized.zig");

const Token = token_mod.Token;
const TokenKeyword = token_mod.TokenKeyword;

pub const ResolvedRowExpressionField = struct {
    field: []const u8,
    source: db_mod.types.RelationalRowsExpressionFieldSource,
    schema: runtime_schema.TableSchema,
};

pub fn relationalColumnForField(
    schema: runtime_schema.TableSchema,
    field: []const u8,
    expected_type: ?runtime_schema.AntflyType,
) ?runtime_schema.RelationalColumn {
    for (schema.relational_columns) |column| {
        if (!std.mem.eql(u8, column.name, field)) continue;
        if (expected_type) |field_type| {
            if (column.field_type != field_type) return null;
        }
        return column;
    }
    return null;
}

pub fn joinSideForQualifier(
    qualifier: []const u8,
    left_alias: []const u8,
    right_alias: []const u8,
) !db_mod.types.RelationalRowsJoinProjectionSide {
    if (std.mem.eql(u8, qualifier, left_alias)) return .left;
    if (std.mem.eql(u8, qualifier, right_alias)) return .right;
    return error.UnsupportedSqlShape;
}

pub fn joinColumnForSide(
    schema: runtime_schema.TableSchema,
    joined_source_schema: ?runtime_schema.TableSchema,
    side: db_mod.types.RelationalRowsJoinProjectionSide,
    field: []const u8,
) !runtime_schema.RelationalColumn {
    const source_schema = if (side == .left) schema else joined_source_schema orelse schema;
    return relationalColumnForField(source_schema, field, null) orelse error.InvalidSqlCatalog;
}

pub fn joinedMutationColumnForQualifier(
    schema: runtime_schema.TableSchema,
    joined_source_schema: ?runtime_schema.TableSchema,
    qualifier: []const u8,
    field: []const u8,
    target_alias: []const u8,
    source_alias: []const u8,
) !runtime_schema.RelationalColumn {
    if (std.mem.eql(u8, qualifier, target_alias)) {
        return relationalColumnForField(schema, field, null) orelse error.InvalidSqlCatalog;
    }
    if (std.mem.eql(u8, qualifier, source_alias)) {
        return relationalColumnForField(joined_source_schema orelse schema, field, null) orelse error.InvalidSqlCatalog;
    }
    return error.UnsupportedSqlShape;
}

pub fn joinedMutationTargetFieldMatches(
    alloc: std.mem.Allocator,
    field: []const u8,
    target_alias: []const u8,
    expected: []const u8,
) !bool {
    if (std.mem.eql(u8, field, expected)) return true;
    var dot: ?usize = std.mem.indexOfScalar(u8, field, '.');
    while (dot) |index| {
        if (index > 0 and index + 1 < field.len) {
            const qualifier = field[0..index];
            const unqualified = field[index + 1 ..];
            if (std.mem.eql(u8, unqualified, expected) and try returningQualifierMatches(alloc, qualifier, &.{target_alias})) return true;
        }
        dot = std.mem.indexOfScalarPos(u8, field, index + 1, '.');
    }
    return false;
}

pub fn validateMergeFields(
    schema: runtime_schema.TableSchema,
    joined_source_schema: ?runtime_schema.TableSchema,
    target_field: []const u8,
    source_field: []const u8,
) !void {
    const target_column = relationalColumnForField(schema, target_field, null) orelse return error.InvalidSqlCatalog;
    const source_schema = joined_source_schema orelse schema;
    const source_column = relationalColumnForField(source_schema, source_field, null) orelse return error.InvalidSqlCatalog;
    if (source_column.field_type != target_column.field_type) return error.InvalidSqlCatalog;
}

pub fn resolveJoinProjectionsAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    joined_source_schema: ?runtime_schema.TableSchema,
    raw_select: []const plan_mod.QualifiedProjection,
    left_alias: []const u8,
    right_alias: []const u8,
    select: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsJoinProjection),
) !void {
    for (raw_select) |projection| {
        const side = try joinSideForQualifier(projection.source.qualifier, left_alias, right_alias);
        _ = try joinColumnForSide(schema, joined_source_schema, side, projection.source.field);
        const output = try alloc.dupe(u8, projection.output);
        var output_transferred = false;
        errdefer if (!output_transferred) alloc.free(output);
        const field = try alloc.dupe(u8, projection.source.field);
        var field_transferred = false;
        errdefer if (!field_transferred) alloc.free(field);
        try select.append(alloc, .{ .output = output, .side = side, .field = field });
        output_transferred = true;
        field_transferred = true;
    }
}

pub fn relationalColumnForReturningField(schema: runtime_schema.TableSchema, field: []const u8) ?runtime_schema.RelationalColumn {
    if (relationalColumnForField(schema, field, null)) |column| return column;
    const dot_index = std.mem.indexOfScalar(u8, field, '.') orelse return null;
    if (dot_index == 0 or dot_index + 1 >= field.len) return null;
    return relationalColumnForField(schema, field[0..dot_index], .json);
}

pub fn returningQualifierMatches(
    alloc: std.mem.Allocator,
    qualifier: []const u8,
    returning_qualifiers: []const []const u8,
) !bool {
    for (returning_qualifiers) |expected| {
        if (std.mem.eql(u8, qualifier, expected)) return true;
    }
    const normalized = try grammar.normalizeSqlObjectIdentifierAlloc(alloc, qualifier);
    defer alloc.free(normalized);
    for (returning_qualifiers) |expected| {
        if (std.mem.eql(u8, normalized, expected)) return true;
    }
    return false;
}

pub fn matchQualifiedReturningAll(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    returning_qualifiers: []const []const u8,
) !bool {
    if (pos.* + 1 >= tokens.len or tokens[pos.*].kind != .identifier) return false;
    const token = tokens[pos.*];
    if (!std.mem.endsWith(u8, token.text, ".")) return false;
    if (tokens[pos.* + 1].kind != .star) return false;
    const qualifier = token.text[0 .. token.text.len - 1];
    if (qualifier.len == 0) return error.UnsupportedSqlShape;
    if (!try returningQualifierMatches(alloc, qualifier, returning_qualifiers)) return error.InvalidSqlCatalog;
    pos.* += 2;
    return true;
}

pub fn normalizeReturningFieldAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    field: []const u8,
    returning_qualifiers: []const []const u8,
) ![]const u8 {
    if (relationalColumnForReturningField(schema, field) != null) {
        return try alloc.dupe(u8, field);
    }

    var dot: ?usize = std.mem.indexOfScalar(u8, field, '.');
    while (dot) |index| {
        if (index > 0 and index + 1 < field.len) {
            const qualifier = field[0..index];
            const unqualified_field = field[index + 1 ..];
            if (try returningQualifierMatches(alloc, qualifier, returning_qualifiers)) {
                if (relationalColumnForReturningField(schema, unqualified_field) == null) return error.InvalidSqlCatalog;
                return try alloc.dupe(u8, unqualified_field);
            }
        }
        dot = std.mem.indexOfScalarPos(u8, field, index + 1, '.');
    }

    return error.InvalidSqlCatalog;
}

pub fn normalizeJoinedMutationReturningSourceFieldAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    joined_source_schema: ?runtime_schema.TableSchema,
    field: []const u8,
    source_alias: []const u8,
) !?[]const u8 {
    var dot: ?usize = std.mem.indexOfScalar(u8, field, '.');
    while (dot) |index| {
        if (index > 0 and index + 1 < field.len) {
            const qualifier = field[0..index];
            const unqualified_field = field[index + 1 ..];
            if (try returningQualifierMatches(alloc, qualifier, &.{source_alias})) {
                const source_schema = joined_source_schema orelse schema;
                if (relationalColumnForReturningField(source_schema, unqualified_field) == null) return error.InvalidSqlCatalog;
                return try alloc.dupe(u8, unqualified_field);
            }
        }
        dot = std.mem.indexOfScalarPos(u8, field, index + 1, '.');
    }
    return null;
}

pub fn normalizeTargetSelectorFieldAlloc(
    alloc: std.mem.Allocator,
    field: []const u8,
    target_qualifiers: []const []const u8,
) ![]u8 {
    var dot: ?usize = std.mem.indexOfScalar(u8, field, '.');
    while (dot) |index| {
        if (index > 0 and index + 1 < field.len) {
            const qualifier = field[0..index];
            const unqualified_field = field[index + 1 ..];
            if (try returningQualifierMatches(alloc, qualifier, target_qualifiers)) {
                return try alloc.dupe(u8, unqualified_field);
            }
        }
        dot = std.mem.indexOfScalarPos(u8, field, index + 1, '.');
    }
    if (std.mem.indexOfScalar(u8, field, '.') != null) return error.InvalidSqlCatalog;
    return try alloc.dupe(u8, field);
}

pub fn appendSemiJoinTargetFieldAlloc(
    alloc: std.mem.Allocator,
    fields: *std.ArrayListUnmanaged([]const u8),
    schema: runtime_schema.TableSchema,
    parsed_target_field: []const u8,
    target_qualifiers: []const []const u8,
) !void {
    const target_field = try normalizeTargetSelectorFieldAlloc(alloc, parsed_target_field, target_qualifiers);
    var target_field_transferred = false;
    errdefer if (!target_field_transferred) alloc.free(target_field);
    if (relationalColumnForField(schema, target_field, null) == null) return error.InvalidSqlCatalog;
    try fields.append(alloc, target_field);
    target_field_transferred = true;
}

pub fn normalizeRowExpressionFieldAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    field: []const u8,
    field_expression_qualifiers: []const []const u8,
    returning_expression_qualifiers: []const []const u8,
    defer_row_expression_field_validation: bool,
) ![]const u8 {
    const qualifiers = if (field_expression_qualifiers.len != 0)
        field_expression_qualifiers
    else
        returning_expression_qualifiers;
    if (defer_row_expression_field_validation) {
        if (qualifiers.len == 0) return try alloc.dupe(u8, field);
        var dot: ?usize = std.mem.indexOfScalar(u8, field, '.');
        while (dot) |index| {
            if (index > 0 and index + 1 < field.len) {
                const qualifier = field[0..index];
                const unqualified_field = field[index + 1 ..];
                if (try returningQualifierMatches(alloc, qualifier, qualifiers)) return try alloc.dupe(u8, unqualified_field);
            }
            dot = std.mem.indexOfScalarPos(u8, field, index + 1, '.');
        }
        if (std.mem.indexOfScalar(u8, field, '.') != null) return error.InvalidSqlCatalog;
        return try alloc.dupe(u8, field);
    }
    if (qualifiers.len == 0) {
        return try alloc.dupe(u8, field);
    }
    return try normalizeReturningFieldAlloc(alloc, schema, field, qualifiers);
}

pub fn normalizePeriodReferenceAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    field: []const u8,
    field_expression_qualifiers: []const []const u8,
    returning_expression_qualifiers: []const []const u8,
) !?[]const u8 {
    if (relationalPeriodForDdl(schema.periods, field) != null) {
        return try alloc.dupe(u8, field);
    }
    const qualifiers = if (field_expression_qualifiers.len != 0)
        field_expression_qualifiers
    else
        returning_expression_qualifiers;
    if (qualifiers.len == 0) return null;
    var dot: ?usize = std.mem.indexOfScalar(u8, field, '.');
    while (dot) |index| {
        if (index > 0 and index + 1 < field.len) {
            const qualifier = field[0..index];
            const unqualified_field = field[index + 1 ..];
            if (relationalPeriodForDdl(schema.periods, unqualified_field) != null and try returningQualifierMatches(alloc, qualifier, qualifiers)) {
                return try alloc.dupe(u8, unqualified_field);
            }
        }
        dot = std.mem.indexOfScalarPos(u8, field, index + 1, '.');
    }
    return null;
}

pub fn normalizeQualifiedRowExpressionFieldAlloc(
    alloc: std.mem.Allocator,
    field: []const u8,
    qualifiers: []const []const u8,
    schema: runtime_schema.TableSchema,
) !?[]const u8 {
    var dot: ?usize = std.mem.indexOfScalar(u8, field, '.');
    while (dot) |index| {
        if (index > 0 and index + 1 < field.len) {
            const qualifier = field[0..index];
            const unqualified_field = field[index + 1 ..];
            if (try returningQualifierMatches(alloc, qualifier, qualifiers)) {
                if (relationalColumnForReturningField(schema, unqualified_field) == null) return error.InvalidSqlCatalog;
                return try alloc.dupe(u8, unqualified_field);
            }
        }
        dot = std.mem.indexOfScalarPos(u8, field, index + 1, '.');
    }
    return null;
}

pub fn resolveRowExpressionFieldAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    joined_source_schema: ?runtime_schema.TableSchema,
    field: []const u8,
    field_expression_qualifiers: []const []const u8,
    returning_expression_qualifiers: []const []const u8,
    joined_source_expression_qualifiers: []const []const u8,
    joined_target_expression_qualifiers: []const []const u8,
    defer_row_expression_field_validation: bool,
    default_source: db_mod.types.RelationalRowsExpressionFieldSource,
) !ResolvedRowExpressionField {
    if (joined_source_expression_qualifiers.len != 0) {
        const source_schema = joined_source_schema orelse schema;
        if (try normalizeQualifiedRowExpressionFieldAlloc(alloc, field, joined_source_expression_qualifiers, source_schema)) |source_field| {
            return .{
                .field = source_field,
                .source = .source,
                .schema = source_schema,
            };
        }
    }
    if (joined_target_expression_qualifiers.len != 0) {
        if (try normalizeQualifiedRowExpressionFieldAlloc(alloc, field, joined_target_expression_qualifiers, schema)) |target_field| {
            return .{
                .field = target_field,
                .source = .row,
                .schema = schema,
            };
        }
    }
    return .{
        .field = try normalizeRowExpressionFieldAlloc(
            alloc,
            schema,
            field,
            field_expression_qualifiers,
            returning_expression_qualifiers,
            defer_row_expression_field_validation,
        ),
        .source = default_source,
        .schema = if (default_source == .source) joined_source_schema orelse schema else schema,
    };
}

pub fn findUniqueConstraintByName(schema: runtime_schema.TableSchema, name: []const u8) ?runtime_schema.UniqueConstraint {
    for (schema.unique_constraints) |constraint| {
        if (constraint.validation_state != .enforced) continue;
        if (std.mem.eql(u8, constraint.name, name)) return constraint;
    }
    return null;
}

pub fn findUniqueConstraintByColumns(schema: runtime_schema.TableSchema, columns: []const []const u8, require_partial: bool) ?runtime_schema.UniqueConstraint {
    for (schema.unique_constraints) |constraint| {
        if (constraint.validation_state != .enforced) continue;
        if (constraint.expressions.len != 0) continue;
        if (require_partial and constraint.where.len == 0) continue;
        if (!require_partial and constraint.where.len != 0) continue;
        if (stringSlicesEqual(constraint.columns, columns)) return constraint;
    }
    return null;
}

pub fn findUniqueConstraintByExpression(
    schema: runtime_schema.TableSchema,
    op: runtime_schema.UniqueExpressionOp,
    field: []const u8,
) ?runtime_schema.UniqueConstraint {
    for (schema.unique_constraints) |constraint| {
        if (constraint.validation_state != .enforced) continue;
        if (constraint.columns.len != 0 or constraint.expressions.len != 1) continue;
        const expression = constraint.expressions[0];
        if (expression.op == op and std.mem.eql(u8, expression.field, field)) return constraint;
    }
    return null;
}

fn stringSlicesEqual(a: []const []const u8, b: []const []const u8) bool {
    if (a.len != b.len) return false;
    for (a, b) |left, right| {
        if (!std.mem.eql(u8, left, right)) return false;
    }
    return true;
}

pub fn tableSchemaCatalogExists(current: runtime_schema.TableSchema) bool {
    return current.storage_mode == .relational or
        current.dynamic_templates.len != 0 or
        current.full_text_documents.len != 0 or
        current.relational_columns.len != 0 or
        current.primary_key != null or
        current.periods.len != 0 or
        current.foreign_keys.len != 0 or
        current.unique_constraints.len != 0 or
        current.checks.len != 0;
}

pub fn relationalIndexNameExists(schema: runtime_schema.TableSchema, index_name: []const u8) bool {
    return uniqueConstraintNameExists(schema.unique_constraints, index_name) or
        relationalColumnIndexForIndexName(schema.relational_columns, index_name) != null;
}

pub fn relationalConstraintNameExists(schema: runtime_schema.TableSchema, table_name: []const u8, name: []const u8) bool {
    return primaryKeyNameEquals(schema.primary_key, table_name, name) or
        uniqueConstraintNameExists(schema.unique_constraints, name) or
        foreignKeyNameExists(schema.foreign_keys, name) or
        relationalCheckNameExists(schema.checks, name);
}

pub fn validateCommentMetadataPlanForRuntimeSchemaAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    plan: anytype,
) !void {
    if (plan.object_name.len == 0) return error.InvalidSqlCatalog;
    try validateStringCommentJsonAlloc(alloc, plan.comment_json);
    switch (plan.target) {
        .table => {},
        .column => {
            const column_name = commentColumnName(plan.object_name);
            _ = relationalColumnForDdl(schema.relational_columns, column_name) orelse return error.InvalidSqlCatalog;
        },
        .index => {
            if (!relationalIndexNameExists(schema, plan.object_name)) return error.InvalidSqlCatalog;
        },
        .constraint => {
            const parent_table = plan.parent_table_name orelse return error.InvalidSqlCatalog;
            if (!relationalConstraintNameExists(schema, parent_table, plan.object_name)) return error.InvalidSqlCatalog;
        },
    }
}

fn validateStringCommentJsonAlloc(alloc: std.mem.Allocator, comment_json: ?[]const u8) !void {
    const raw = comment_json orelse return;
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, raw, .{});
    defer parsed.deinit();
    switch (parsed.value) {
        .string => {},
        else => return error.UnsupportedSqlShape,
    }
}

fn commentColumnName(object_name: []const u8) []const u8 {
    const dot = std.mem.lastIndexOfScalar(u8, object_name, '.') orelse return object_name;
    return object_name[dot + 1 ..];
}

pub fn primaryKeyNameEquals(primary_key: ?runtime_schema.PrimaryKey, table_name: []const u8, name: []const u8) bool {
    const key = primary_key orelse return false;
    if (key.name) |key_name| return std.mem.eql(u8, key_name, name);
    return defaultPrimaryKeyNameEquals(table_name, name);
}

pub fn defaultPrimaryKeyNameEquals(table_name: []const u8, name: []const u8) bool {
    const suffix = "_pkey";
    return name.len == table_name.len + suffix.len and
        std.mem.startsWith(u8, name, table_name) and
        std.mem.endsWith(u8, name, suffix);
}

pub fn columnsMatchPrimaryKey(primary_key: runtime_schema.PrimaryKey, columns: []const []const u8) bool {
    return stringSlicesEqual(primary_key.columns, columns);
}

pub fn primaryKeyContains(primary_key: runtime_schema.PrimaryKey, field: []const u8) bool {
    for (primary_key.columns) |column| {
        if (std.mem.eql(u8, column, field)) return true;
    }
    return false;
}

pub fn relationalColumnForDdl(columns: []const runtime_schema.RelationalColumn, name: []const u8) ?runtime_schema.RelationalColumn {
    for (columns) |column| {
        if (std.mem.eql(u8, column.name, name)) return column;
    }
    return null;
}

pub fn relationalColumnIndex(columns: []const runtime_schema.RelationalColumn, name: []const u8) ?usize {
    for (columns, 0..) |column, i| {
        if (std.mem.eql(u8, column.name, name)) return i;
    }
    return null;
}

pub fn relationalColumnIndexForIndexName(columns: []const runtime_schema.RelationalColumn, index_name: []const u8) ?usize {
    for (columns, 0..) |column, i| {
        if (relationalColumnHasIndexName(column, index_name)) return i;
    }
    return null;
}

pub fn relationalColumnHasDeclaredIndexName(column: runtime_schema.RelationalColumn, index_name: []const u8) bool {
    const declared_index_name = column.index_name orelse return false;
    return std.mem.eql(u8, declared_index_name, index_name);
}

pub fn relationalColumnHasIndexName(column: runtime_schema.RelationalColumn, index_name: []const u8) bool {
    const declared_index_name = column.index_name orelse column.name;
    return std.mem.eql(u8, declared_index_name, index_name);
}

pub fn uniqueConstraintNameExists(constraints: []const runtime_schema.UniqueConstraint, name: []const u8) bool {
    for (constraints) |constraint| {
        if (std.mem.eql(u8, constraint.name, name)) return true;
    }
    return false;
}

pub fn foreignKeyNameExists(foreign_keys: []const runtime_schema.ForeignKey, name: []const u8) bool {
    for (foreign_keys) |foreign_key| {
        if (std.mem.eql(u8, foreign_key.name, name)) return true;
    }
    return false;
}

pub fn relationalCheckNameExists(checks: []const runtime_schema.RelationalCheck, name: []const u8) bool {
    for (checks) |check| {
        if (std.mem.eql(u8, check.name, name)) return true;
    }
    return false;
}

pub fn relationalFieldTypeSupportsCollation(field_type: runtime_schema.AntflyType) bool {
    return switch (field_type) {
        .keyword, .text => true,
        else => false,
    };
}

pub fn relationalPeriodColumnType(field_type: runtime_schema.AntflyType) bool {
    return field_type == .numeric or field_type == .datetime;
}

pub fn relationalPeriodForDdl(periods: []const runtime_schema.RelationalPeriod, name: []const u8) ?runtime_schema.RelationalPeriod {
    for (periods) |period| {
        if (std.mem.eql(u8, period.name, name)) return period;
    }
    return null;
}

pub fn relationalPeriodNameExists(periods: []const runtime_schema.RelationalPeriod, name: []const u8) bool {
    return relationalPeriodForDdl(periods, name) != null;
}

pub fn validateRelationalPeriodCatalog(columns: []const runtime_schema.RelationalColumn, periods: []const runtime_schema.RelationalPeriod) !void {
    for (periods, 0..) |period, i| {
        if (relationalPeriodNameExists(periods[0..i], period.name)) return error.InvalidSqlCatalog;
        if (std.mem.eql(u8, period.start_column, period.end_column)) return error.InvalidSqlCatalog;
        const start_column = relationalColumnForDdl(columns, period.start_column) orelse return error.InvalidSqlCatalog;
        const end_column = relationalColumnForDdl(columns, period.end_column) orelse return error.InvalidSqlCatalog;
        if (!relationalPeriodColumnType(start_column.field_type) or !relationalPeriodColumnType(end_column.field_type)) return error.InvalidSqlCatalog;
        if (start_column.field_type != end_column.field_type) return error.InvalidSqlCatalog;
        if (period.range_type) |range_type| {
            switch (range_type) {
                .numrange => if (start_column.field_type != .numeric) return error.InvalidSqlCatalog,
                .daterange, .tsrange, .tstzrange => if (start_column.field_type != .datetime) return error.InvalidSqlCatalog,
            }
        }
    }
}

pub fn validatePrimaryKeyTemporalCatalog(periods: []const runtime_schema.RelationalPeriod, primary_key: runtime_schema.PrimaryKey) !void {
    if (primary_key.without_overlaps_period) |period| {
        _ = relationalPeriodForDdl(periods, period) orelse return error.InvalidSqlCatalog;
    }
}

pub fn validateForeignKeyForColumns(columns: []const runtime_schema.RelationalColumn, periods: []const runtime_schema.RelationalPeriod, foreign_key: runtime_schema.ForeignKey) !void {
    if (foreign_key.child_columns.len == 0 or foreign_key.child_columns.len != foreign_key.parent_columns.len) return error.InvalidSqlCatalog;
    if ((foreign_key.child_period == null) != (foreign_key.parent_period == null)) return error.InvalidSqlCatalog;
    if (foreign_key.child_period) |period| {
        if (!foreignKeyActionSupportsTemporalUpdate(foreign_key.on_update)) return error.InvalidSqlCatalog;
        _ = relationalPeriodForDdl(periods, period) orelse return error.InvalidSqlCatalog;
    }
    for (foreign_key.child_columns) |column| {
        const found = relationalColumnForDdl(columns, column) orelse return error.InvalidSqlCatalog;
        if (found.field_type == .json or found.field_type == .array) return error.InvalidSqlCatalog;
    }
}

pub fn foreignKeyActionIsRestrictive(action: runtime_schema.ForeignKeyAction) bool {
    return action == .restrict or action == .no_action;
}

pub fn foreignKeyActionSupportsTemporalUpdate(action: runtime_schema.ForeignKeyAction) bool {
    return foreignKeyActionIsRestrictive(action) or action == .set_null or action == .cascade;
}

pub fn runtimeSchemaForCatalogTableAlloc(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    table_name: []const u8,
) !runtime_schema.TableSchema {
    return try runtimeSchemaForCatalogTableWithSessionAlloc(alloc, catalog, table_name, catalog_resources.SqlCatalogSession.default());
}

pub fn runtimeSchemaForCatalogTableWithSessionAlloc(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    table_name: []const u8,
    session: catalog_resources.SqlCatalogSession,
) !runtime_schema.TableSchema {
    const target = try session.tableTargetFromObjectName(table_name);
    return try runtimeSchemaForQualifiedCatalogTableAlloc(
        alloc,
        catalog,
        target.database_name,
        target.namespace_name,
        target.table_name,
    );
}

fn ownedCatalogTableRefForObjectNameAlloc(
    alloc: std.mem.Allocator,
    table_name: []const u8,
    session: catalog_resources.SqlCatalogSession,
) !source_binding.CatalogTableRef {
    const target = try session.tableTargetFromObjectName(table_name);
    const database_name = try alloc.dupe(u8, target.database_name);
    errdefer alloc.free(database_name);
    const namespace_name = try alloc.dupe(u8, target.namespace_name);
    errdefer alloc.free(namespace_name);
    const owned_table_name = try alloc.dupe(u8, target.table_name);
    errdefer alloc.free(owned_table_name);
    return .{
        .database_name = database_name,
        .namespace_name = namespace_name,
        .table_name = owned_table_name,
    };
}

fn deinitCatalogTableRef(alloc: std.mem.Allocator, target: source_binding.CatalogTableRef) void {
    alloc.free(@constCast(target.database_name));
    alloc.free(@constCast(target.namespace_name));
    alloc.free(@constCast(target.table_name));
}

fn cloneCatalogTableRefAlloc(alloc: std.mem.Allocator, target: source_binding.CatalogTableRef) !source_binding.CatalogTableRef {
    const database_name = try alloc.dupe(u8, target.database_name);
    errdefer alloc.free(database_name);
    const namespace_name = try alloc.dupe(u8, target.namespace_name);
    errdefer alloc.free(namespace_name);
    const table_name = try alloc.dupe(u8, target.table_name);
    errdefer alloc.free(table_name);
    return .{
        .database_name = database_name,
        .namespace_name = namespace_name,
        .table_name = table_name,
    };
}

pub const BoundCatalogObjectRole = enum {
    target,
    source,
    insert_source,
    joined_source,
};

pub const BoundCatalogObject = struct {
    role: BoundCatalogObjectRole,
    target: source_binding.CatalogTableRef,
    family: source_binding.SqlSourceFamily,
    schema_version: u32,
    table_id: u64,
    schema_generation: u64,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        deinitCatalogTableRef(alloc, self.target);
        self.* = undefined;
    }
};

pub const BoundSqlAuthorizationPermission = enum {
    read,
    write,
    admin,
};

pub const BoundSqlAuthorizationResourceKind = enum {
    table,
    database,
    all,
};

pub const BoundSqlAuthorizationGrant = struct {
    resource_kind: BoundSqlAuthorizationResourceKind,
    resource: []const u8,
    permission: BoundSqlAuthorizationPermission,
};

pub const BoundSqlAuthorizationDecision = enum {
    not_evaluated,
    unrestricted,
    allowed,
    denied,
};

pub const BoundSqlAuthorizationOptions = struct {
    principal_name: ?[]const u8 = null,
    grants: []const BoundSqlAuthorizationGrant = &.{},
    grants_evaluated: bool = false,
    unrestricted: bool = false,
};

pub const BoundSqlAuthorizationCheck = struct {
    object_role: BoundCatalogObjectRole,
    target: source_binding.CatalogTableRef,
    family: source_binding.SqlSourceFamily,
    table_id: u64,
    schema_generation: u64,
    required_permission: BoundSqlAuthorizationPermission,
    decision: BoundSqlAuthorizationDecision,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        deinitCatalogTableRef(alloc, self.target);
        self.* = undefined;
    }
};

pub const BoundSqlAuthorization = struct {
    principal_name: ?[]const u8 = null,
    checks: []BoundSqlAuthorizationCheck = &.{},

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        if (self.principal_name) |name| alloc.free(@constCast(name));
        for (self.checks) |*check| check.deinit(alloc);
        if (self.checks.len > 0) alloc.free(self.checks);
        self.* = undefined;
    }
};

fn authorizationPermissionAllows(grant: BoundSqlAuthorizationPermission, required: BoundSqlAuthorizationPermission) bool {
    return grant == .admin or grant == required;
}

fn authorizationGrantMatchesTarget(grant: BoundSqlAuthorizationGrant, target: source_binding.CatalogTableRef, resource_name: []const u8) bool {
    return switch (grant.resource_kind) {
        .all => std.mem.eql(u8, grant.resource, "*"),
        .database => std.mem.eql(u8, grant.resource, "*") or std.mem.eql(u8, grant.resource, target.database_name),
        .table => std.mem.eql(u8, grant.resource, "*") or catalog_resources.tableResourceMatches(grant.resource, resource_name),
    };
}

fn authorizationDecisionForTarget(
    alloc: std.mem.Allocator,
    target: source_binding.CatalogTableRef,
    required_permission: BoundSqlAuthorizationPermission,
    options: BoundSqlAuthorizationOptions,
) !BoundSqlAuthorizationDecision {
    if (options.unrestricted) return .unrestricted;
    if (!options.grants_evaluated) return .not_evaluated;
    const resource_name = try catalog_resources.tableResourceNameAlloc(alloc, target.database_name, target.namespace_name, target.table_name);
    defer alloc.free(resource_name);
    for (options.grants) |grant| {
        if (!authorizationPermissionAllows(grant.permission, required_permission)) continue;
        if (authorizationGrantMatchesTarget(grant, target, resource_name)) return .allowed;
    }
    return .denied;
}

fn authorizationRequiredPermissionForBoundObject(object: BoundCatalogObject, default_permission: BoundSqlAuthorizationPermission) BoundSqlAuthorizationPermission {
    return switch (object.role) {
        .target => default_permission,
        .source, .insert_source, .joined_source => .read,
    };
}

fn boundSqlAuthorizationForObjectsAlloc(
    alloc: std.mem.Allocator,
    objects: []const BoundCatalogObject,
    options: BoundSqlAuthorizationOptions,
    default_permission: BoundSqlAuthorizationPermission,
) !BoundSqlAuthorization {
    if (options.principal_name == null and !options.grants_evaluated and !options.unrestricted) return .{};

    var out = BoundSqlAuthorization{
        .principal_name = if (options.principal_name) |name| try alloc.dupe(u8, name) else null,
    };
    errdefer out.deinit(alloc);

    const checks = try alloc.alloc(BoundSqlAuthorizationCheck, objects.len);
    var initialized: usize = 0;
    errdefer {
        for (checks[0..initialized]) |*check| check.deinit(alloc);
        alloc.free(checks);
    }
    for (objects, 0..) |object, i| {
        const target = try cloneCatalogTableRefAlloc(alloc, object.target);
        errdefer deinitCatalogTableRef(alloc, target);
        const required_permission = authorizationRequiredPermissionForBoundObject(object, default_permission);
        checks[i] = .{
            .object_role = object.role,
            .target = target,
            .family = object.family,
            .table_id = object.table_id,
            .schema_generation = object.schema_generation,
            .required_permission = required_permission,
            .decision = try authorizationDecisionForTarget(alloc, object.target, required_permission, options),
        };
        initialized += 1;
    }
    out.checks = checks;
    return out;
}

fn freeBoundCatalogObjects(alloc: std.mem.Allocator, objects: []BoundCatalogObject) void {
    deinitBoundCatalogObjects(alloc, objects);
    if (objects.len > 0) alloc.free(objects);
}

fn deinitBoundCatalogObjects(alloc: std.mem.Allocator, objects: []BoundCatalogObject) void {
    for (objects) |*object| object.deinit(alloc);
}

fn boundCatalogObjectForBindingAlloc(
    alloc: std.mem.Allocator,
    role: BoundCatalogObjectRole,
    binding: source_binding.SqlSourceBinding,
) !BoundCatalogObject {
    const target = try cloneCatalogTableRefAlloc(alloc, binding.target());
    errdefer deinitCatalogTableRef(alloc, target);
    return .{
        .role = role,
        .target = target,
        .family = binding.family(),
        .schema_version = binding.schema().version,
        .table_id = binding.tableId(),
        .schema_generation = binding.schemaGeneration(),
    };
}

fn boundCatalogObjectForCatalogTableAlloc(
    alloc: std.mem.Allocator,
    role: BoundCatalogObjectRole,
    catalog: table_catalog.CatalogSource,
    table_name: []const u8,
    session: catalog_resources.SqlCatalogSession,
) !BoundCatalogObject {
    const target = try ownedCatalogTableRefForObjectNameAlloc(alloc, table_name, session);
    errdefer deinitCatalogTableRef(alloc, target);
    var snapshot = try catalog.adminSnapshot();
    defer catalog.freeAdminSnapshot(&snapshot);
    const table = qualifiedTableRecord(&snapshot, target.database_name, target.namespace_name, target.table_name) orelse return error.TableNotFound;
    if (table.schema_json.len == 0) return error.InvalidSqlCatalog;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, table.schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);
    return .{
        .role = role,
        .target = target,
        .family = source_binding.familyForRuntimeSchema(schema),
        .schema_version = schema.version,
        .table_id = table.table_id,
        .schema_generation = metadata_table_manager.schemaRewriteGenerationForSchemaJson(table.schema_json),
    };
}

fn boundCatalogObjectForTableRecordAlloc(
    alloc: std.mem.Allocator,
    role: BoundCatalogObjectRole,
    table: metadata_table_manager.TableRecord,
) !BoundCatalogObject {
    if (table.schema_json.len == 0) return error.InvalidSqlCatalog;
    const database_name = try alloc.dupe(u8, table.database_name);
    errdefer alloc.free(database_name);
    const namespace_name = try alloc.dupe(u8, table.namespace_name);
    errdefer alloc.free(namespace_name);
    const table_name = try alloc.dupe(u8, table.name);
    errdefer alloc.free(table_name);
    var parsed = try schema_api.parseValidatedTableSchema(alloc, table.schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);
    return .{
        .role = role,
        .target = .{
            .database_name = database_name,
            .namespace_name = namespace_name,
            .table_name = table_name,
        },
        .family = source_binding.familyForRuntimeSchema(schema),
        .schema_version = schema.version,
        .table_id = table.table_id,
        .schema_generation = metadata_table_manager.schemaRewriteGenerationForSchemaJson(table.schema_json),
    };
}

fn boundCatalogObjectForCatalogIndexAlloc(
    alloc: std.mem.Allocator,
    role: BoundCatalogObjectRole,
    catalog: table_catalog.CatalogSource,
    index_name: []const u8,
    session: catalog_resources.SqlCatalogSession,
) !BoundCatalogObject {
    var snapshot = try catalog.adminSnapshot();
    defer catalog.freeAdminSnapshot(&snapshot);
    const table = try catalogTableForIndexNameAlloc(alloc, &snapshot, index_name, session);
    return try boundCatalogObjectForTableRecordAlloc(alloc, role, table);
}

fn catalogTableForIndexNameAlloc(
    alloc: std.mem.Allocator,
    snapshot: *const metadata_api.AdminSnapshot,
    index_name: []const u8,
    session: catalog_resources.SqlCatalogSession,
) !metadata_table_manager.TableRecord {
    var found: ?metadata_table_manager.TableRecord = null;
    for (snapshot.tables) |table| {
        if (!std.mem.eql(u8, table.database_name, session.currentDatabase())) continue;
        if (!sqlSessionSearchPathContainsNamespace(session, table.namespace_name)) continue;
        if (!try catalogTableRecordHasIndexNameAlloc(alloc, table, index_name)) continue;
        if (found != null) return error.InvalidSqlCatalog;
        found = table;
    }
    return found orelse error.InvalidSqlCatalog;
}

fn sqlSessionSearchPathContainsNamespace(session: catalog_resources.SqlCatalogSession, namespace_name: []const u8) bool {
    const default_search_path: []const []const u8 = &.{catalog_resources.default_namespace_name};
    const search_path = if (session.search_path.len == 0) default_search_path else session.search_path;
    for (search_path) |candidate| {
        if (std.mem.eql(u8, candidate, namespace_name)) return true;
    }
    return false;
}

fn catalogTableRecordHasIndexNameAlloc(
    alloc: std.mem.Allocator,
    table: metadata_table_manager.TableRecord,
    index_name: []const u8,
) !bool {
    if (try catalogTableSchemaHasIndexNameAlloc(alloc, table.schema_json, index_name)) return true;
    if (table.read_schema_json.len != 0 and try catalogTableSchemaHasIndexNameAlloc(alloc, table.read_schema_json, index_name)) return true;
    return try catalogIndexesJsonHasIndexNameAlloc(alloc, table.indexes_json, index_name);
}

fn catalogTableSchemaHasIndexNameAlloc(
    alloc: std.mem.Allocator,
    schema_json: []const u8,
    index_name: []const u8,
) !bool {
    if (schema_json.len == 0) return false;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);
    return relationalIndexNameExists(schema, index_name);
}

fn catalogIndexesJsonHasIndexNameAlloc(
    alloc: std.mem.Allocator,
    indexes_json: []const u8,
    index_name: []const u8,
) !bool {
    if (indexes_json.len == 0) return false;
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, indexes_json, .{});
    defer parsed.deinit();
    const root = switch (parsed.value) {
        .object => |object| object,
        else => return error.InvalidSqlCatalog,
    };
    return root.get(index_name) != null;
}

fn appendBoundCatalogObjectForBindingAlloc(
    alloc: std.mem.Allocator,
    objects: *std.ArrayListUnmanaged(BoundCatalogObject),
    role: BoundCatalogObjectRole,
    binding: source_binding.SqlSourceBinding,
) !void {
    var object = try boundCatalogObjectForBindingAlloc(alloc, role, binding);
    errdefer object.deinit(alloc);
    try objects.append(alloc, object);
}

fn appendBoundCatalogObjectForCatalogTableAlloc(
    alloc: std.mem.Allocator,
    objects: *std.ArrayListUnmanaged(BoundCatalogObject),
    role: BoundCatalogObjectRole,
    catalog: table_catalog.CatalogSource,
    table_name: []const u8,
    session: catalog_resources.SqlCatalogSession,
) !void {
    var object = try boundCatalogObjectForCatalogTableAlloc(alloc, role, catalog, table_name, session);
    errdefer object.deinit(alloc);
    try objects.append(alloc, object);
}

fn appendBoundCatalogObjectsForCatalogSchemaTablesAlloc(
    alloc: std.mem.Allocator,
    objects: *std.ArrayListUnmanaged(BoundCatalogObject),
    role: BoundCatalogObjectRole,
    catalog: table_catalog.CatalogSource,
    schema_name: []const u8,
    session: catalog_resources.SqlCatalogSession,
) !void {
    const target = try session.namespaceTargetFromSchemaName(schema_name);
    var snapshot = catalog.adminSnapshot() catch |err| switch (err) {
        error.UnsupportedOperation => return error.UnsupportedSqlShape,
        else => return err,
    };
    defer catalog.freeAdminSnapshot(&snapshot);

    var matched: usize = 0;
    for (snapshot.tables) |table| {
        if (!std.mem.eql(u8, table.database_name, target.database_name)) continue;
        if (!std.mem.eql(u8, table.namespace_name, target.namespace_name)) continue;
        var object = try boundCatalogObjectForTableRecordAlloc(alloc, role, table);
        var object_transferred = false;
        errdefer if (!object_transferred) object.deinit(alloc);
        try objects.append(alloc, object);
        object_transferred = true;
        matched += 1;
    }
    if (matched == 0) return error.TableNotFound;
}

fn appendBoundCatalogObjectsForCatalogDatabaseTablesAlloc(
    alloc: std.mem.Allocator,
    objects: *std.ArrayListUnmanaged(BoundCatalogObject),
    role: BoundCatalogObjectRole,
    catalog: table_catalog.CatalogSource,
    database_name: []const u8,
) !void {
    if (database_name.len == 0) return error.UnsupportedSqlShape;
    var snapshot = catalog.adminSnapshot() catch |err| switch (err) {
        error.UnsupportedOperation => return error.UnsupportedSqlShape,
        else => return err,
    };
    defer catalog.freeAdminSnapshot(&snapshot);

    var matched: usize = 0;
    for (snapshot.tables) |table| {
        if (!std.mem.eql(u8, table.database_name, database_name)) continue;
        var object = try boundCatalogObjectForTableRecordAlloc(alloc, role, table);
        var object_transferred = false;
        errdefer if (!object_transferred) object.deinit(alloc);
        try objects.append(alloc, object);
        object_transferred = true;
        matched += 1;
    }
    if (matched == 0) return error.TableNotFound;
}

fn appendBoundCatalogObjectForCatalogIndexAlloc(
    alloc: std.mem.Allocator,
    objects: *std.ArrayListUnmanaged(BoundCatalogObject),
    role: BoundCatalogObjectRole,
    catalog: table_catalog.CatalogSource,
    index_name: []const u8,
    session: catalog_resources.SqlCatalogSession,
) !void {
    var object = try boundCatalogObjectForCatalogIndexAlloc(alloc, role, catalog, index_name, session);
    errdefer object.deinit(alloc);
    try objects.append(alloc, object);
}

fn appendOptionalBoundCatalogObjectForCatalogIndexAlloc(
    alloc: std.mem.Allocator,
    objects: *std.ArrayListUnmanaged(BoundCatalogObject),
    role: BoundCatalogObjectRole,
    catalog: table_catalog.CatalogSource,
    index_name: []const u8,
    session: catalog_resources.SqlCatalogSession,
    missing_ok: bool,
) !void {
    appendBoundCatalogObjectForCatalogIndexAlloc(alloc, objects, role, catalog, index_name, session) catch |err| switch (err) {
        error.InvalidSqlCatalog, error.TableNotFound => if (missing_ok) return else return err,
        error.UnsupportedOperation => return,
        else => return err,
    };
}

fn sourceBindingForCatalogTableWithSessionAlloc(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    table_name: []const u8,
    session: catalog_resources.SqlCatalogSession,
) !source_binding.SqlSourceBinding {
    const target = try ownedCatalogTableRefForObjectNameAlloc(alloc, table_name, session);
    errdefer deinitCatalogTableRef(alloc, target);
    var snapshot = try catalog.adminSnapshot();
    defer catalog.freeAdminSnapshot(&snapshot);
    const table = qualifiedTableRecord(&snapshot, target.database_name, target.namespace_name, target.table_name) orelse return error.InvalidSqlCatalog;
    if (table.schema_json.len == 0) return error.InvalidSqlCatalog;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, table.schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    errdefer runtime_schema.freeSchema(alloc, schema);
    var binding = source_binding.bindingForRuntimeSchema(target, schema);
    switch (binding) {
        .relational => |*relational| {
            relational.table_id = table.table_id;
            relational.schema_generation = metadata_table_manager.schemaRewriteGenerationForSchemaJson(table.schema_json);
        },
        .document => |*document| {
            document.table_id = table.table_id;
            document.schema_generation = metadata_table_manager.schemaRewriteGenerationForSchemaJson(table.schema_json);
            document.indexes_json = try alloc.dupe(u8, table.indexes_json);
            errdefer if (document.indexes_json) |indexes_json| alloc.free(@constCast(indexes_json));
            document.capabilities = try source_binding.documentCapabilitiesForRuntimeSchemaAndIndexesJsonAlloc(alloc, schema, table.indexes_json);
            errdefer source_binding.deinitDocumentSqlCapabilities(alloc, &document.capabilities);
            document.virtual_schema = try source_binding.documentSqlSchemaForRuntimeSchemaAndIndexesJsonAlloc(alloc, schema, table.indexes_json);
        },
        .lake => |*lake| {
            lake.table_id = table.table_id;
            lake.schema_generation = metadata_table_manager.schemaRewriteGenerationForSchemaJson(table.schema_json);
        },
    }
    return binding;
}

fn deinitSqlSourceBinding(alloc: std.mem.Allocator, binding: *source_binding.SqlSourceBinding) void {
    switch (binding.*) {
        .relational => |relational| {
            deinitCatalogTableRef(alloc, relational.target);
            runtime_schema.freeSchema(alloc, relational.schema);
        },
        .document => |document| {
            deinitCatalogTableRef(alloc, document.target);
            runtime_schema.freeSchema(alloc, document.schema);
            if (document.indexes_json) |indexes_json| alloc.free(@constCast(indexes_json));
            var virtual_schema = document.virtual_schema;
            source_binding.deinitDocumentSqlSchema(alloc, &virtual_schema);
            var capabilities = document.capabilities;
            source_binding.deinitDocumentSqlCapabilities(alloc, &capabilities);
        },
        .lake => |lake| {
            deinitCatalogTableRef(alloc, lake.target);
            runtime_schema.freeSchema(alloc, lake.schema);
        },
    }
    binding.* = undefined;
}

pub fn runtimeSchemaForQualifiedCatalogTableAlloc(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    database_name: []const u8,
    namespace_name: []const u8,
    table_name: []const u8,
) !runtime_schema.TableSchema {
    var snapshot = try catalog.adminSnapshot();
    defer catalog.freeAdminSnapshot(&snapshot);
    const schema_json = qualifiedTableSchemaJson(&snapshot, database_name, namespace_name, table_name) orelse return error.InvalidSqlCatalog;
    if (schema_json.len == 0) return error.InvalidSqlCatalog;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    return try schema_api.deriveRuntimeTableSchema(alloc, parsed);
}

pub fn tableSchemaJson(snapshot: *const metadata_api.AdminSnapshot, table_name: []const u8) ?[]const u8 {
    return qualifiedTableSchemaJson(
        snapshot,
        metadata_table_manager.default_database_name,
        metadata_table_manager.default_namespace_name,
        table_name,
    );
}

pub fn qualifiedTableSchemaJson(
    snapshot: *const metadata_api.AdminSnapshot,
    database_name: []const u8,
    namespace_name: []const u8,
    table_name: []const u8,
) ?[]const u8 {
    for (snapshot.tables) |table| {
        if (!std.mem.eql(u8, table.database_name, database_name)) continue;
        if (!std.mem.eql(u8, table.namespace_name, namespace_name)) continue;
        if (std.mem.eql(u8, table.name, table_name)) return table.schema_json;
    }
    return null;
}

pub fn qualifiedTableRecord(
    snapshot: *const metadata_api.AdminSnapshot,
    database_name: []const u8,
    namespace_name: []const u8,
    table_name: []const u8,
) ?metadata_table_manager.TableRecord {
    for (snapshot.tables) |table| {
        if (!std.mem.eql(u8, table.database_name, database_name)) continue;
        if (!std.mem.eql(u8, table.namespace_name, namespace_name)) continue;
        if (std.mem.eql(u8, table.name, table_name)) return table;
    }
    return null;
}

pub const InsertSourceTableNames = struct {
    target: []const u8,
    source: []const u8,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(@constCast(self.target));
        alloc.free(@constCast(self.source));
        self.* = undefined;
    }
};

pub const ReadSourceTableNames = struct {
    left: []const u8,
    source: []const u8,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(@constCast(self.left));
        alloc.free(@constCast(self.source));
        self.* = undefined;
    }
};

pub const CatalogBoundWritePlanOptions = struct {
    options: plan_mod.LowerWritePlanOptions,
    target_binding: ?source_binding.SqlSourceBinding = null,
    owned_insert_source_schema: ?runtime_schema.TableSchema = null,
    owned_joined_source_schema: ?runtime_schema.TableSchema = null,
    bound_objects: []BoundCatalogObject = &.{},
    authorization: BoundSqlAuthorization = .{},

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        if (self.target_binding) |*binding| deinitSqlSourceBinding(alloc, binding);
        if (self.owned_insert_source_schema) |schema| runtime_schema.freeSchema(alloc, schema);
        if (self.owned_joined_source_schema) |schema| runtime_schema.freeSchema(alloc, schema);
        freeBoundCatalogObjects(alloc, self.bound_objects);
        self.authorization.deinit(alloc);
        self.* = undefined;
    }
};

pub const CatalogBoundReadPlanSourceSchema = struct {
    target_binding: ?source_binding.SqlSourceBinding = null,
    source_schema: ?runtime_schema.TableSchema = null,
    source_binding: ?source_binding.SqlSourceBinding = null,
    bound_objects: []BoundCatalogObject = &.{},
    authorization: BoundSqlAuthorization = .{},

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        if (self.target_binding) |*binding| deinitSqlSourceBinding(alloc, binding);
        if (self.source_binding == null) {
            if (self.source_schema) |schema| runtime_schema.freeSchema(alloc, schema);
        }
        if (self.source_binding) |*binding| deinitSqlSourceBinding(alloc, binding);
        freeBoundCatalogObjects(alloc, self.bound_objects);
        self.authorization.deinit(alloc);
        self.* = undefined;
    }
};

pub const CatalogBoundDdlPlanFacts = struct {
    bound_objects: []BoundCatalogObject = &.{},
    authorization: BoundSqlAuthorization = .{},
    logical_plan: ?LogicalSqlPlan = null,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        freeBoundCatalogObjects(alloc, self.bound_objects);
        self.authorization.deinit(alloc);
        if (self.logical_plan) |*plan| plan.deinit(alloc);
        self.* = undefined;
    }
};

pub const BoundSqlBinding = union(enum) {
    read_catalog: CatalogBoundReadPlanSourceSchema,
    write_catalog: CatalogBoundWritePlanOptions,
    ddl_catalog: CatalogBoundDdlPlanFacts,
    none,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        switch (self.*) {
            .read_catalog => |*read| read.deinit(alloc),
            .write_catalog => |*write| write.deinit(alloc),
            .ddl_catalog => |*ddl| ddl.deinit(alloc),
            .none => {},
        }
        self.* = undefined;
    }
};

pub const BoundSqlSession = struct {
    current_database_name: []const u8,
    search_path: []const []const u8,

    pub fn empty() BoundSqlSession {
        return .{
            .current_database_name = "",
            .search_path = &.{},
        };
    }

    pub fn fromSessionAlloc(alloc: std.mem.Allocator, source_session: catalog_resources.SqlCatalogSession) !BoundSqlSession {
        const current_database_name = try alloc.dupe(u8, source_session.currentDatabase());
        errdefer alloc.free(current_database_name);

        const default_search_path = [_][]const u8{catalog_resources.default_namespace_name};
        const source_path = if (source_session.search_path.len == 0) default_search_path[0..] else source_session.search_path;
        const search_path = try alloc.alloc([]const u8, source_path.len);
        var initialized: usize = 0;
        errdefer {
            for (search_path[0..initialized]) |name| alloc.free(@constCast(name));
            alloc.free(search_path);
        }
        for (source_path, 0..) |name, i| {
            search_path[i] = try alloc.dupe(u8, name);
            initialized += 1;
        }

        return .{
            .current_database_name = current_database_name,
            .search_path = search_path,
        };
    }

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        if (self.current_database_name.len > 0) alloc.free(@constCast(self.current_database_name));
        for (self.search_path) |name| alloc.free(@constCast(name));
        if (self.search_path.len > 0) alloc.free(self.search_path);
        self.* = undefined;
    }

    pub fn session(self: BoundSqlSession) catalog_resources.SqlCatalogSession {
        return .{
            .current_database_name = self.current_database_name,
            .search_path = self.search_path,
        };
    }
};

pub const BoundSqlStatement = struct {
    parsed_sql: ?*const tokenized.ParsedSql = null,
    statement: tokenized.ParsedStatement,
    session: BoundSqlSession,
    binding: BoundSqlBinding = .none,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        self.binding.deinit(alloc);
        self.session.deinit(alloc);
        self.* = undefined;
    }

    pub fn readCatalog(self: *BoundSqlStatement) !*CatalogBoundReadPlanSourceSchema {
        return switch (self.binding) {
            .read_catalog => |*read| read,
            else => error.UnsupportedSqlShape,
        };
    }

    pub fn writeCatalog(self: *BoundSqlStatement) !*CatalogBoundWritePlanOptions {
        return switch (self.binding) {
            .write_catalog => |*write| write,
            else => error.UnsupportedSqlShape,
        };
    }

    pub fn ddlCatalog(self: *BoundSqlStatement) !*CatalogBoundDdlPlanFacts {
        return switch (self.binding) {
            .ddl_catalog => |*ddl| ddl,
            else => error.UnsupportedSqlShape,
        };
    }

    pub fn takeDdlLogicalPlan(self: *BoundSqlStatement) !LogicalSqlPlan {
        const ddl = try self.ddlCatalog();
        if (ddl.logical_plan) |plan| {
            ddl.logical_plan = null;
            return plan;
        }
        return error.UnsupportedSqlShape;
    }

    pub fn parsedSql(self: *const BoundSqlStatement) !*const tokenized.ParsedSql {
        return self.parsed_sql orelse error.UnsupportedSqlShape;
    }
};

pub fn takeBoundSqlStatementAuthorization(bound: *BoundSqlStatement) !BoundSqlAuthorization {
    return switch (bound.binding) {
        .read_catalog => |*read| blk: {
            const authorization = read.authorization;
            read.authorization = .{};
            break :blk authorization;
        },
        .write_catalog => |*write| blk: {
            const authorization = write.authorization;
            write.authorization = .{};
            break :blk authorization;
        },
        .ddl_catalog => |*ddl| blk: {
            const authorization = ddl.authorization;
            ddl.authorization = .{};
            break :blk authorization;
        },
        .none => .{},
    };
}

pub fn enforceBoundSqlAuthorization(authorization: BoundSqlAuthorization) !void {
    for (authorization.checks) |check| {
        if (check.decision == .denied) return error.PermissionDenied;
    }
}

pub fn enforceBoundSqlStatementAuthorization(bound: *BoundSqlStatement) !void {
    switch (bound.binding) {
        .read_catalog => |read| try enforceBoundSqlAuthorization(read.authorization),
        .write_catalog => |write| try enforceBoundSqlAuthorization(write.authorization),
        .ddl_catalog => |ddl| try enforceBoundSqlAuthorization(ddl.authorization),
        .none => {},
    }
}

pub fn enforceLogicalSqlPlanAuthorization(logical: *LogicalSqlPlan) !void {
    switch (logical.*) {
        .catalog_read => |read| try enforceBoundSqlAuthorization(read.authorization),
        .catalog_write => |write| try enforceBoundSqlAuthorization(write.authorization),
        else => {},
    }
}

fn requireParsedCatalogReadStatement(statement: tokenized.ParsedStatement) !void {
    switch (statement) {
        .read => {},
        else => return error.UnsupportedSqlShape,
    }
}

fn requireParsedCatalogWriteStatement(statement: tokenized.ParsedStatement) !void {
    switch (statement) {
        .write => {},
        else => return error.UnsupportedSqlShape,
    }
}

fn requireParsedDdlStatement(statement: tokenized.ParsedStatement) !void {
    switch (statement) {
        .ddl, .session, .transaction, .prepared, .explain, .unsupported, .unknown => {},
        else => return error.UnsupportedSqlShape,
    }
}

pub const CatalogLogicalReadPlan = struct {
    statement: tokenized.ParsedStatement,
    session: BoundSqlSession,
    target_binding: ?source_binding.SqlSourceBinding = null,
    source_schema: ?runtime_schema.TableSchema = null,
    source_binding: ?source_binding.SqlSourceBinding = null,
    bound_objects: []BoundCatalogObject = &.{},
    authorization: BoundSqlAuthorization = .{},

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        if (self.target_binding) |*binding| deinitSqlSourceBinding(alloc, binding);
        if (self.source_binding == null) {
            if (self.source_schema) |schema| runtime_schema.freeSchema(alloc, schema);
        }
        if (self.source_binding) |*binding| deinitSqlSourceBinding(alloc, binding);
        freeBoundCatalogObjects(alloc, self.bound_objects);
        self.authorization.deinit(alloc);
        self.session.deinit(alloc);
        self.* = undefined;
    }
};

pub const CatalogLogicalWritePlan = struct {
    statement: tokenized.ParsedStatement,
    session: BoundSqlSession,
    options: plan_mod.LowerWritePlanOptions,
    target_binding: ?source_binding.SqlSourceBinding = null,
    owned_insert_source_schema: ?runtime_schema.TableSchema = null,
    owned_joined_source_schema: ?runtime_schema.TableSchema = null,
    bound_objects: []BoundCatalogObject = &.{},
    authorization: BoundSqlAuthorization = .{},

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        if (self.target_binding) |*binding| deinitSqlSourceBinding(alloc, binding);
        if (self.owned_insert_source_schema) |schema| runtime_schema.freeSchema(alloc, schema);
        if (self.owned_joined_source_schema) |schema| runtime_schema.freeSchema(alloc, schema);
        freeBoundCatalogObjects(alloc, self.bound_objects);
        self.authorization.deinit(alloc);
        self.session.deinit(alloc);
        self.* = undefined;
    }
};

pub const TransactionLogicalPlan = union(enum) {
    control: ddl_plan.TransactionControlPlan,
    prepared: ddl_plan.PreparedTransactionPlan,
    savepoint: ddl_plan.SavepointTransactionPlan,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        switch (self.*) {
            .control => |*plan| plan.deinit(alloc),
            .prepared => |*plan| plan.deinit(alloc),
            .savepoint => |*plan| plan.deinit(alloc),
        }
        self.* = undefined;
    }
};

pub const RoutineLogicalPlan = union(enum) {
    function_catalog: ddl_plan.FunctionCatalogPlan,
    trigger_catalog: ddl_plan.TriggerCatalogPlan,
    procedure_call: ddl_plan.ProcedureCallPlan,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        switch (self.*) {
            .function_catalog => |*plan| plan.deinit(alloc),
            .trigger_catalog => |*plan| plan.deinit(alloc),
            .procedure_call => |*plan| plan.deinit(alloc),
        }
        self.* = undefined;
    }
};

pub const AuthorizationLogicalPlan = union(enum) {
    authorization_catalog: ddl_plan.AuthorizationCatalogPlan,
    row_security_catalog: ddl_plan.RowSecurityCatalogPlan,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        switch (self.*) {
            .authorization_catalog => |*plan| plan.deinit(alloc),
            .row_security_catalog => |*plan| plan.deinit(alloc),
        }
        self.* = undefined;
    }
};

pub const TableDdlLogicalPlan = union(enum) {
    moved: void,
    create_table: ddl_plan.CreateTablePlan,
    table_clone: ddl_plan.TableClonePlan,
    view_catalog: ddl_plan.ViewCatalogPlan,
    materialized_view_catalog: ddl_plan.MaterializedViewCatalogPlan,
    relation_lifetime: ddl_plan.RelationLifetimePlan,
    table_partition_catalog: ddl_plan.TablePartitionCatalogPlan,
    create_index: ddl_plan.CreateIndexPlan,
    drop_index: ddl_plan.DropIndexPlan,
    drop_table: ddl_plan.DropTablePlan,
    alter_table: ddl_plan.AlterTablePlan,
    create_update_policy: ddl_plan.CreateUpdatePolicyPlan,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        switch (self.*) {
            .moved => {},
            .create_table => |*plan| plan.deinit(alloc),
            .table_clone => |*plan| plan.deinit(alloc),
            .view_catalog => |*plan| plan.deinit(alloc),
            .materialized_view_catalog => |*plan| plan.deinit(alloc),
            .relation_lifetime => |*plan| plan.deinit(alloc),
            .table_partition_catalog => |*plan| plan.deinit(alloc),
            .create_index => |*plan| plan.deinit(alloc),
            .drop_index => |*plan| plan.deinit(alloc),
            .drop_table => |*plan| plan.deinit(alloc),
            .alter_table => |*plan| plan.deinit(alloc),
            .create_update_policy => |*plan| plan.deinit(alloc),
        }
        self.* = undefined;
    }
};

pub const CatalogDdlLogicalPlan = union(enum) {
    moved: void,
    enum_type_catalog: ddl_plan.EnumTypeCatalogPlan,
    domain_catalog: ddl_plan.DomainCatalogPlan,
    sequence_catalog: ddl_plan.SequenceCatalogPlan,
    identity_allocator_catalog: ddl_plan.IdentityAllocatorPlan,
    schema_namespace_catalog: ddl_plan.SchemaNamespaceCatalogPlan,
    database_catalog: ddl_plan.DatabaseCatalogPlan,
    tablespace_catalog: ddl_plan.TablespaceCatalogPlan,
    logical_replication: ddl_plan.LogicalReplicationPlan,
    type_system_catalog: ddl_plan.TypeSystemCatalogPlan,
    comment_metadata: ddl_plan.CommentMetadataPlan,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        switch (self.*) {
            .moved => {},
            .enum_type_catalog => |*plan| plan.deinit(alloc),
            .domain_catalog => |*plan| plan.deinit(alloc),
            .sequence_catalog => |*plan| plan.deinit(alloc),
            .identity_allocator_catalog => |*plan| plan.deinit(alloc),
            .schema_namespace_catalog => |*plan| plan.deinit(alloc),
            .database_catalog => |*plan| plan.deinit(alloc),
            .tablespace_catalog => |*plan| plan.deinit(alloc),
            .logical_replication => |*plan| plan.deinit(alloc),
            .type_system_catalog => |*plan| plan.deinit(alloc),
            .comment_metadata => |*plan| plan.deinit(alloc),
        }
        self.* = undefined;
    }
};

pub const OtherDdlLogicalPlan = union(enum) {
    moved: void,
    adapter_noop: ddl_plan.AdapterNoopDdlPlan,

    pub fn deinit(self: *@This(), _: std.mem.Allocator) void {
        self.* = undefined;
    }
};

pub const LogicalSqlPlan = union(enum) {
    read: sql_statement_kind.SqlReadStatementKind,
    write: sql_statement_kind.SqlWriteStatementKind,
    catalog_read: CatalogLogicalReadPlan,
    catalog_write: CatalogLogicalWritePlan,
    table_ddl: TableDdlLogicalPlan,
    catalog_ddl: CatalogDdlLogicalPlan,
    other_ddl: OtherDdlLogicalPlan,
    session: ddl_plan.SessionCatalogPlan,
    transaction: TransactionLogicalPlan,
    prepared_statement: ddl_plan.PreparedStatementPlan,
    cursor: ddl_plan.CursorPortalPlan,
    notification: ddl_plan.NotificationChannelPlan,
    routine: RoutineLogicalPlan,
    auth: AuthorizationLogicalPlan,
    extension: ddl_plan.ExtensionCatalogPlan,
    maintenance: ddl_plan.MaintenanceJobPlan,
    bulk_io: ddl_plan.BulkIoPlan,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        switch (self.*) {
            .read, .write => {},
            .catalog_read => |*read| read.deinit(alloc),
            .catalog_write => |*write| write.deinit(alloc),
            .table_ddl => |*plan| plan.deinit(alloc),
            .catalog_ddl => |*plan| plan.deinit(alloc),
            .other_ddl => |*plan| plan.deinit(alloc),
            .session => |*plan| plan.deinit(alloc),
            .transaction => |*plan| plan.deinit(alloc),
            .prepared_statement => |*plan| plan.deinit(alloc),
            .cursor => |*plan| plan.deinit(alloc),
            .notification => |*plan| plan.deinit(alloc),
            .routine => |*plan| plan.deinit(alloc),
            .auth => |*plan| plan.deinit(alloc),
            .extension => |*plan| plan.deinit(alloc),
            .maintenance => |*plan| plan.deinit(alloc),
            .bulk_io => |*plan| plan.deinit(alloc),
        }
        self.* = undefined;
    }

    pub fn statementKindName(self: LogicalSqlPlan) []const u8 {
        return switch (self) {
            .read => |kind| @tagName(kind),
            .write => |kind| @tagName(kind),
            .catalog_read => "read",
            .catalog_write => "write",
            .table_ddl => "table_ddl",
            .catalog_ddl => "catalog_ddl",
            .other_ddl => "other_ddl",
            .session => "session",
            .transaction => "transaction",
            .prepared_statement => "prepared_statement",
            .cursor => "cursor",
            .notification => "notification",
            .routine => "routine",
            .auth => "auth",
            .extension => "extension",
            .maintenance => "maintenance",
            .bulk_io => "bulk_io",
        };
    }
};

pub fn logicalReadPlanFromBoundStatement(bound: *BoundSqlStatement) !LogicalSqlPlan {
    const read = try bound.readCatalog();
    const target_binding = read.target_binding;
    read.target_binding = null;
    const source_schema = read.source_schema;
    read.source_schema = null;
    const source_binding_value = read.source_binding;
    read.source_binding = null;
    const bound_objects = read.bound_objects;
    read.bound_objects = &.{};
    const authorization = read.authorization;
    read.authorization = .{};
    const session = bound.session;
    bound.session = BoundSqlSession.empty();
    return .{ .catalog_read = .{
        .statement = bound.statement,
        .session = session,
        .target_binding = target_binding,
        .source_schema = source_schema,
        .source_binding = source_binding_value,
        .bound_objects = bound_objects,
        .authorization = authorization,
    } };
}

pub fn logicalWritePlanFromBoundStatement(bound: *BoundSqlStatement) !LogicalSqlPlan {
    const write = try bound.writeCatalog();
    const target_binding = write.target_binding;
    write.target_binding = null;
    const owned_insert_source_schema = write.owned_insert_source_schema;
    const owned_joined_source_schema = write.owned_joined_source_schema;
    write.owned_insert_source_schema = null;
    write.owned_joined_source_schema = null;
    const bound_objects = write.bound_objects;
    write.bound_objects = &.{};
    const authorization = write.authorization;
    write.authorization = .{};
    const session = bound.session;
    bound.session = BoundSqlSession.empty();
    return .{ .catalog_write = .{
        .statement = bound.statement,
        .session = session,
        .options = write.options,
        .target_binding = target_binding,
        .owned_insert_source_schema = owned_insert_source_schema,
        .owned_joined_source_schema = owned_joined_source_schema,
        .bound_objects = bound_objects,
        .authorization = authorization,
    } };
}

pub const ReadPlanCatalogLoweringHooks = struct {
    ptr: *anyopaque,
    lower_document_target: *const fn (*anyopaque, source_binding.DocumentBinding) anyerror!plan_mod.LoweredReadPlan,
    lower_with_source_schema: *const fn (*anyopaque, runtime_schema.TableSchema) anyerror!plan_mod.LoweredReadPlan,
    lower_without_source_schema: *const fn (*anyopaque) anyerror!plan_mod.LoweredReadPlan,
};

pub const WritePlanCatalogLoweringHooks = struct {
    ptr: *anyopaque,
    lower_with_options: *const fn (*anyopaque, plan_mod.LowerWritePlanOptions) anyerror!plan_mod.LoweredWritePlan,
};

pub fn lowerReadPlanWithBoundStatementAlloc(
    alloc: std.mem.Allocator,
    bound: *BoundSqlStatement,
    hooks: ReadPlanCatalogLoweringHooks,
) !plan_mod.LoweredReadPlan {
    var logical = try logicalReadPlanFromBoundStatement(bound);
    defer logical.deinit(alloc);
    return try lowerReadCatalogLogicalPlan(&logical, hooks);
}

pub fn lowerWritePlanWithBoundStatementAlloc(
    alloc: std.mem.Allocator,
    bound: *BoundSqlStatement,
    hooks: WritePlanCatalogLoweringHooks,
) !plan_mod.LoweredWritePlan {
    var logical = try logicalWritePlanFromBoundStatement(bound);
    defer logical.deinit(alloc);
    return try lowerWriteCatalogLogicalPlan(&logical, hooks);
}

pub fn lowerReadCatalogLogicalPlan(logical: *LogicalSqlPlan, hooks: ReadPlanCatalogLoweringHooks) !plan_mod.LoweredReadPlan {
    return switch (logical.*) {
        .catalog_read => |*read| blk: {
            if (read.target_binding) |binding| {
                switch (binding) {
                    .document => |document| break :blk try hooks.lower_document_target(hooks.ptr, document),
                    .lake => return error.UnsupportedSqlShape,
                    .relational => {},
                }
            }
            break :blk if (read.source_schema) |source_schema|
                try hooks.lower_with_source_schema(hooks.ptr, source_schema)
            else
                try hooks.lower_without_source_schema(hooks.ptr);
        },
        else => error.UnsupportedSqlShape,
    };
}

pub fn lowerWriteCatalogLogicalPlan(logical: *LogicalSqlPlan, hooks: WritePlanCatalogLoweringHooks) !plan_mod.LoweredWritePlan {
    return switch (logical.*) {
        .catalog_write => |*write| try hooks.lower_with_options(hooks.ptr, write.options),
        else => error.UnsupportedSqlShape,
    };
}

const SelectReadTableNames = struct {
    left: []const u8,
    source: ?[]const u8 = null,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(@constCast(self.left));
        if (self.source) |source| alloc.free(@constCast(source));
        self.* = undefined;
    }
};

const CteSourceBinding = struct {
    name: []const u8,
    source: []const u8,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(@constCast(self.name));
        alloc.free(@constCast(self.source));
        self.* = undefined;
    }
};

pub fn insertSourceTableNamesFromParsedSqlAlloc(alloc: std.mem.Allocator, parsed_sql: *const tokenized.ParsedSql) !?InsertSourceTableNames {
    switch (parsed_sql.statement) {
        .write => |statement| {
            if (statement.kind != .insert_source or statement.recursive) return null;
        },
        else => return error.UnsupportedSqlShape,
    }
    return try insertSourceTableNamesFromTokensAlloc(alloc, parsed_sql.items());
}

fn insertSourceTableNamesFromTokensAlloc(alloc: std.mem.Allocator, tokens: []const Token) !?InsertSourceTableNames {
    if (tokens.len == 0 or tokens[0].kind != .identifier) return null;
    if (tokens[0].matchesKeywordTag(.with)) return try insertSourceTableNamesFromWithAlloc(alloc, tokens);
    if (!tokens[0].matchesKeywordTag(.insert)) return null;
    return try insertSourceTableNamesFromInsertAlloc(alloc, tokens, 0);
}

pub fn recursiveInsertSourceTableNamesFromParsedSqlAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const tokenized.ParsedSql,
) !?InsertSourceTableNames {
    switch (parsed_sql.statement) {
        .write => |statement| {
            if (statement.kind != .insert_source or !statement.recursive) return null;
        },
        else => return error.UnsupportedSqlShape,
    }
    return try recursiveInsertSourceTableNamesFromTokensAlloc(alloc, parsed_sql.items());
}

fn recursiveInsertSourceTableNamesFromTokensAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
) !?InsertSourceTableNames {
    if (tokens.len == 0 or tokens[0].kind != .identifier) return null;
    if (!tokens[0].matchesKeywordTag(.with)) return null;
    var index: usize = 1;
    if (!consumeKeyword(tokens, &index, .recursive)) return null;
    if (index >= tokens.len or tokens[index].kind != .identifier) return null;
    index += 1;
    if (index < tokens.len and tokens[index].kind == .lparen) {
        index = (findMatchingRParenIndex(tokens, index) orelse return null) + 1;
    }
    if (!consumeKeyword(tokens, &index, .as)) return null;
    parser.consumeCteMaterializationHint(tokens, &index) catch return null;
    if (index >= tokens.len or tokens[index].kind != .lparen) return null;

    const body_start = index + 1;
    const body_end = findMatchingRParenIndex(tokens, index) orelse return null;
    const body = tokens[body_start..body_end];
    const from_index = findTopLevelKeyword(body, .from) orelse return null;
    var source_index = body_start + from_index + 1;
    _ = consumeKeyword(tokens, &source_index, .only);
    if (source_index >= body_end or tokens[source_index].kind != .identifier) return null;
    const source = try normalizeSqlObjectIdentifierAlloc(alloc, tokens[source_index].text);
    var source_transferred = false;
    defer if (!source_transferred) alloc.free(source);

    index = body_end + 1;
    if (!consumeKeyword(tokens, &index, .insert)) return null;
    if (!consumeKeyword(tokens, &index, .into)) return null;
    _ = consumeKeyword(tokens, &index, .only);
    if (index >= tokens.len or tokens[index].kind != .identifier) return null;
    const target = try normalizeSqlObjectIdentifierAlloc(alloc, tokens[index].text);
    errdefer alloc.free(target);

    source_transferred = true;
    return .{
        .target = target,
        .source = source,
    };
}

pub fn joinedWriteSourceTableNamesFromParsedSqlAlloc(alloc: std.mem.Allocator, parsed_sql: *const tokenized.ParsedSql) !?InsertSourceTableNames {
    const tokens = parsed_sql.items();
    const raw = parsed_sql.statement.raw();
    if (raw.token_start >= raw.token_end or raw.token_end > tokens.len) return error.UnsupportedSqlShape;
    const statement_tokens = tokens[raw.token_start..raw.token_end];
    const final_statement_index = writeFinalStatementIndex(statement_tokens) orelse return error.UnsupportedSqlShape;
    const statement_kind = switch (parsed_sql.statement) {
        .write => |statement| statement.kind,
        else => writeStatementKindFromFinalStatementTokens(statement_tokens, final_statement_index) orelse return error.UnsupportedSqlShape,
    };
    switch (statement_kind) {
        .update,
        .update_source,
        .update_joined_source,
        .delete,
        .delete_source,
        .delete_joined_source,
        .merge,
        => {},
        .insert,
        .insert_source,
        .truncate,
        => return null,
    }
    return try joinedWriteSourceTableNamesFromTokensAlloc(alloc, statement_tokens);
}

pub fn writeTargetTableNameFromParsedSqlAlloc(alloc: std.mem.Allocator, parsed_sql: *const tokenized.ParsedSql) ![]const u8 {
    const tokens = parsed_sql.items();
    const raw = parsed_sql.statement.raw();
    if (raw.token_start >= raw.token_end or raw.token_end > tokens.len) return error.UnsupportedSqlShape;
    return try writeTargetTableNameFromTokensAlloc(alloc, tokens[raw.token_start..raw.token_end]);
}

fn writeTargetTableNameFromTokensAlloc(alloc: std.mem.Allocator, statement_tokens: []const Token) ![]const u8 {
    var pos = writeFinalStatementIndex(statement_tokens) orelse return error.UnsupportedSqlShape;
    const statement_kind = writeStatementKindFromFinalStatementTokens(statement_tokens, pos) orelse return error.UnsupportedSqlShape;
    switch (statement_kind) {
        .insert, .insert_source => {
            if (!consumeKeyword(statement_tokens, &pos, .insert)) return error.UnsupportedSqlShape;
            if (!consumeKeyword(statement_tokens, &pos, .into)) return error.UnsupportedSqlShape;
            _ = consumeKeyword(statement_tokens, &pos, .only);
        },
        .update, .update_source, .update_joined_source => {
            if (!consumeKeyword(statement_tokens, &pos, .update)) return error.UnsupportedSqlShape;
            _ = consumeKeyword(statement_tokens, &pos, .only);
        },
        .delete, .delete_source, .delete_joined_source => {
            if (!consumeKeyword(statement_tokens, &pos, .delete)) return error.UnsupportedSqlShape;
            if (!consumeKeyword(statement_tokens, &pos, .from)) return error.UnsupportedSqlShape;
            _ = consumeKeyword(statement_tokens, &pos, .only);
        },
        .truncate => {
            if (!consumeKeyword(statement_tokens, &pos, .truncate)) return error.UnsupportedSqlShape;
            _ = consumeKeyword(statement_tokens, &pos, .table);
            _ = consumeKeyword(statement_tokens, &pos, .only);
        },
        .merge => {
            if (!consumeKeyword(statement_tokens, &pos, .merge)) return error.UnsupportedSqlShape;
            _ = consumeKeyword(statement_tokens, &pos, .into);
            _ = consumeKeyword(statement_tokens, &pos, .only);
        },
    }
    if (pos >= statement_tokens.len or statement_tokens[pos].kind != .identifier) return error.UnsupportedSqlShape;
    return try normalizeSqlObjectIdentifierAlloc(alloc, statement_tokens[pos].text);
}

fn writeStatementKindFromFinalStatementTokens(tokens: []const Token, pos: usize) ?sql_statement_kind.SqlWriteStatementKind {
    if (pos >= tokens.len or tokens[pos].kind != .identifier) return null;
    if (tokens[pos].matchesKeywordTag(.insert)) return .insert_source;
    if (tokens[pos].matchesKeywordTag(.update)) return .update_joined_source;
    if (tokens[pos].matchesKeywordTag(.delete)) return .delete_joined_source;
    if (tokens[pos].matchesKeywordTag(.truncate)) return .truncate;
    if (tokens[pos].matchesKeywordTag(.merge)) return .merge;
    return null;
}

fn writeFinalStatementIndex(tokens: []const Token) ?usize {
    if (tokens.len == 0 or tokens[0].kind != .identifier) return null;
    if (!tokens[0].matchesKeywordTag(.with)) return 0;

    var index: usize = 1;
    _ = consumeKeyword(tokens, &index, .recursive);
    while (true) {
        if (index >= tokens.len or tokens[index].kind != .identifier) return null;
        index += 1;
        if (index < tokens.len and tokens[index].kind == .lparen) {
            index = (findMatchingRParenIndex(tokens, index) orelse return null) + 1;
        }
        if (!consumeKeyword(tokens, &index, .as)) return null;
        if (consumeKeyword(tokens, &index, .not)) {
            if (!consumeKeyword(tokens, &index, .materialized)) return null;
        } else {
            _ = consumeKeyword(tokens, &index, .materialized);
        }
        if (index >= tokens.len or tokens[index].kind != .lparen) return null;
        index = (findMatchingRParenIndex(tokens, index) orelse return null) + 1;
        if (index < tokens.len and tokens[index].kind == .comma) {
            index += 1;
            continue;
        }
        break;
    }
    if (index >= tokens.len or tokens[index].kind != .identifier) return null;
    return index;
}

fn joinedWriteSourceTableNamesFromTokensAlloc(alloc: std.mem.Allocator, tokens: []const Token) !?InsertSourceTableNames {
    if (tokens.len == 0 or tokens[0].kind != .identifier) return null;
    if (tokens[0].matchesKeywordTag(.with)) return try joinedWriteSourceTableNamesFromWithAlloc(alloc, tokens);
    return try joinedWriteSourceTableNamesFromStatementAlloc(alloc, tokens, 0);
}

pub fn bindWritePlanCatalogStatementAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const tokenized.ParsedSql,
    options: plan_mod.LowerWritePlanOptions,
    catalog: table_catalog.CatalogSource,
) !BoundSqlStatement {
    return try bindWritePlanCatalogStatementWithSessionAlloc(alloc, parsed_sql, options, catalog, catalog_resources.SqlCatalogSession.default());
}

pub fn bindWritePlanCatalogStatementWithSessionAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const tokenized.ParsedSql,
    options: plan_mod.LowerWritePlanOptions,
    catalog: table_catalog.CatalogSource,
    session: catalog_resources.SqlCatalogSession,
) !BoundSqlStatement {
    return try bindWritePlanCatalogStatementWithSessionAndAuthorizationAlloc(alloc, parsed_sql, options, catalog, session, .{});
}

pub fn bindWritePlanCatalogStatementWithSessionAndAuthorizationAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const tokenized.ParsedSql,
    options: plan_mod.LowerWritePlanOptions,
    catalog: table_catalog.CatalogSource,
    session: catalog_resources.SqlCatalogSession,
    authorization_options: BoundSqlAuthorizationOptions,
) !BoundSqlStatement {
    try requireParsedCatalogWriteStatement(parsed_sql.statement);
    var bound_session = try BoundSqlSession.fromSessionAlloc(alloc, session);
    errdefer bound_session.deinit(alloc);
    var resolved = try resolveWritePlanCatalogOptionsFromParsedSqlWithSessionAndAuthorizationAlloc(alloc, parsed_sql, options, catalog, session, authorization_options);
    errdefer resolved.deinit(alloc);
    return .{
        .parsed_sql = parsed_sql,
        .statement = parsed_sql.statement,
        .session = bound_session,
        .binding = .{ .write_catalog = resolved },
    };
}

fn resolveWritePlanCatalogOptionsParsedSqlAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const tokenized.ParsedSql,
    options: plan_mod.LowerWritePlanOptions,
    catalog: table_catalog.CatalogSource,
) !CatalogBoundWritePlanOptions {
    return try resolveWritePlanCatalogOptionsParsedSqlWithSessionAlloc(alloc, parsed_sql, options, catalog, catalog_resources.SqlCatalogSession.default());
}

fn resolveWritePlanCatalogOptionsParsedSqlWithSessionAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const tokenized.ParsedSql,
    options: plan_mod.LowerWritePlanOptions,
    catalog: table_catalog.CatalogSource,
    session: catalog_resources.SqlCatalogSession,
) !CatalogBoundWritePlanOptions {
    try requireParsedCatalogWriteStatement(parsed_sql.statement);
    return try resolveWritePlanCatalogOptionsFromParsedSqlWithSessionAlloc(alloc, parsed_sql, options, catalog, session);
}

fn resolveWritePlanCatalogOptionsFromParsedSqlWithSessionAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const tokenized.ParsedSql,
    options: plan_mod.LowerWritePlanOptions,
    catalog: table_catalog.CatalogSource,
    session: catalog_resources.SqlCatalogSession,
) !CatalogBoundWritePlanOptions {
    return try resolveWritePlanCatalogOptionsFromParsedSqlWithSessionAndAuthorizationAlloc(alloc, parsed_sql, options, catalog, session, .{});
}

fn resolveWritePlanCatalogOptionsFromParsedSqlWithSessionAndAuthorizationAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const tokenized.ParsedSql,
    options: plan_mod.LowerWritePlanOptions,
    catalog: table_catalog.CatalogSource,
    session: catalog_resources.SqlCatalogSession,
    authorization_options: BoundSqlAuthorizationOptions,
) !CatalogBoundWritePlanOptions {
    var out = CatalogBoundWritePlanOptions{
        .options = options,
    };
    errdefer out.deinit(alloc);
    var bound_objects = std.ArrayListUnmanaged(BoundCatalogObject).empty;
    var bound_objects_transferred = false;
    errdefer if (!bound_objects_transferred) {
        deinitBoundCatalogObjects(alloc, bound_objects.items);
        bound_objects.deinit(alloc);
    };

    const target_table_name = try writeTargetTableNameFromParsedSqlAlloc(alloc, parsed_sql);
    defer alloc.free(target_table_name);
    out.target_binding = sourceBindingForCatalogTableWithSessionAlloc(alloc, catalog, target_table_name, session) catch |err| switch (err) {
        error.InvalidSqlCatalog, error.TableNotFound => null,
        else => return err,
    };
    if (out.target_binding) |binding| {
        try appendBoundCatalogObjectForBindingAlloc(alloc, &bound_objects, .target, binding);
    }

    var resolved_recursive_insert_source = false;

    if (out.options.insert_source_schema == null) {
        if (try recursiveInsertSourceTableNamesFromParsedSqlAlloc(alloc, parsed_sql)) |resolved_tables| {
            var tables = resolved_tables;
            defer tables.deinit(alloc);
            resolved_recursive_insert_source = true;
            if (!std.mem.eql(u8, tables.target, tables.source)) {
                out.owned_insert_source_schema = try runtimeSchemaForCatalogTableWithSessionAlloc(alloc, catalog, tables.source, session);
                out.options.insert_source_schema = out.owned_insert_source_schema.?;
                try appendBoundCatalogObjectForCatalogTableAlloc(alloc, &bound_objects, .insert_source, catalog, tables.source, session);
            }
        } else if (try insertSourceTableNamesFromParsedSqlAlloc(alloc, parsed_sql)) |resolved_tables| {
            var tables = resolved_tables;
            defer tables.deinit(alloc);
            if (!std.mem.eql(u8, tables.target, tables.source)) {
                out.owned_insert_source_schema = try runtimeSchemaForCatalogTableWithSessionAlloc(alloc, catalog, tables.source, session);
                out.options.insert_source_schema = out.owned_insert_source_schema.?;
                try appendBoundCatalogObjectForCatalogTableAlloc(alloc, &bound_objects, .insert_source, catalog, tables.source, session);
            }
        }
    }

    if (!resolved_recursive_insert_source and out.options.joined_source_schema == null) {
        if (joinedWriteSourceTableNamesFromParsedSqlAlloc(alloc, parsed_sql)) |maybe_resolved_tables| {
            if (maybe_resolved_tables) |resolved_tables| {
                var tables = resolved_tables;
                defer tables.deinit(alloc);
                if (!std.mem.eql(u8, tables.target, tables.source)) {
                    out.owned_joined_source_schema = try runtimeSchemaForCatalogTableWithSessionAlloc(alloc, catalog, tables.source, session);
                    out.options.joined_source_schema = out.owned_joined_source_schema.?;
                    try appendBoundCatalogObjectForCatalogTableAlloc(alloc, &bound_objects, .joined_source, catalog, tables.source, session);
                }
            }
        } else |err| switch (err) {
            error.UnsupportedSqlShape => {},
            else => return err,
        }
    }

    out.bound_objects = try bound_objects.toOwnedSlice(alloc);
    bound_objects_transferred = true;
    out.authorization = try boundSqlAuthorizationForObjectsAlloc(alloc, out.bound_objects, authorization_options, .write);
    return out;
}

fn resolveReadPlanCatalogSourceSchemaParsedSqlAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const tokenized.ParsedSql,
    catalog: table_catalog.CatalogSource,
) !CatalogBoundReadPlanSourceSchema {
    return try resolveReadPlanCatalogSourceSchemaParsedSqlWithSessionAlloc(alloc, parsed_sql, catalog, catalog_resources.SqlCatalogSession.default());
}

fn resolveReadPlanCatalogSourceSchemaParsedSqlWithSessionAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const tokenized.ParsedSql,
    catalog: table_catalog.CatalogSource,
    session: catalog_resources.SqlCatalogSession,
) !CatalogBoundReadPlanSourceSchema {
    try requireParsedCatalogReadStatement(parsed_sql.statement);
    return try resolveReadPlanCatalogSourceSchemaFromParsedSqlWithSessionAlloc(alloc, parsed_sql, catalog, session);
}

fn resolveReadPlanCatalogSourceSchemaFromParsedSqlWithSessionAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const tokenized.ParsedSql,
    catalog: table_catalog.CatalogSource,
    session: catalog_resources.SqlCatalogSession,
) !CatalogBoundReadPlanSourceSchema {
    return try resolveReadPlanCatalogSourceSchemaFromParsedSqlWithSessionAndAuthorizationAlloc(alloc, parsed_sql, catalog, session, .{});
}

fn resolveReadPlanCatalogSourceSchemaFromParsedSqlWithSessionAndAuthorizationAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const tokenized.ParsedSql,
    catalog: table_catalog.CatalogSource,
    session: catalog_resources.SqlCatalogSession,
    authorization_options: BoundSqlAuthorizationOptions,
) !CatalogBoundReadPlanSourceSchema {
    var out = CatalogBoundReadPlanSourceSchema{};
    errdefer out.deinit(alloc);
    var bound_objects = std.ArrayListUnmanaged(BoundCatalogObject).empty;
    var bound_objects_transferred = false;
    errdefer if (!bound_objects_transferred) {
        deinitBoundCatalogObjects(alloc, bound_objects.items);
        bound_objects.deinit(alloc);
    };
    if (try readSourceTableNamesFromParsedSqlAlloc(alloc, parsed_sql)) |resolved_tables| {
        var tables = resolved_tables;
        defer tables.deinit(alloc);
        out.target_binding = try sourceBindingForCatalogTableWithSessionAlloc(alloc, catalog, tables.left, session);
        try appendBoundCatalogObjectForBindingAlloc(alloc, &bound_objects, .target, out.target_binding.?);
        if (!std.mem.eql(u8, tables.left, tables.source)) {
            out.source_binding = try sourceBindingForCatalogTableWithSessionAlloc(alloc, catalog, tables.source, session);
            try appendBoundCatalogObjectForBindingAlloc(alloc, &bound_objects, .source, out.source_binding.?);
            out.source_schema = switch (out.source_binding.?) {
                .relational => |binding| binding.schema,
                .document => |binding| binding.schema,
                .lake => |binding| binding.schema,
            };
        }
    }
    out.bound_objects = try bound_objects.toOwnedSlice(alloc);
    bound_objects_transferred = true;
    out.authorization = try boundSqlAuthorizationForObjectsAlloc(alloc, out.bound_objects, authorization_options, .read);
    return out;
}

fn appendOptionalBoundCatalogObjectForCatalogTableAlloc(
    alloc: std.mem.Allocator,
    objects: *std.ArrayListUnmanaged(BoundCatalogObject),
    role: BoundCatalogObjectRole,
    catalog: table_catalog.CatalogSource,
    table_name: []const u8,
    session: catalog_resources.SqlCatalogSession,
    missing_ok: bool,
) !void {
    appendBoundCatalogObjectForCatalogTableAlloc(alloc, objects, role, catalog, table_name, session) catch |err| switch (err) {
        error.InvalidSqlCatalog, error.TableNotFound => if (missing_ok) return else return err,
        error.UnsupportedOperation => return,
        else => return err,
    };
}

fn appendExistingCreateTargetIfPresentAlloc(
    alloc: std.mem.Allocator,
    objects: *std.ArrayListUnmanaged(BoundCatalogObject),
    catalog: table_catalog.CatalogSource,
    table_name: []const u8,
    session: catalog_resources.SqlCatalogSession,
) !void {
    return try appendOptionalBoundCatalogObjectForCatalogTableAlloc(alloc, objects, .target, catalog, table_name, session, true);
}

fn foreignKeyReferencesSelf(child_table_name: []const u8, foreign_key: runtime_schema.ForeignKey) bool {
    return std.mem.eql(u8, foreign_key.parent_table, child_table_name);
}

fn appendBoundCatalogObjectsForForeignKeysAlloc(
    alloc: std.mem.Allocator,
    objects: *std.ArrayListUnmanaged(BoundCatalogObject),
    catalog: table_catalog.CatalogSource,
    child_table_name: []const u8,
    foreign_keys: []const runtime_schema.ForeignKey,
    session: catalog_resources.SqlCatalogSession,
) !void {
    for (foreign_keys) |foreign_key| {
        if (foreignKeyReferencesSelf(child_table_name, foreign_key)) continue;
        try appendOptionalBoundCatalogObjectForCatalogTableAlloc(alloc, objects, .source, catalog, foreign_key.parent_table, session, false);
    }
}

fn appendBoundCatalogObjectsForAlterTableForeignKeysAlloc(
    alloc: std.mem.Allocator,
    objects: *std.ArrayListUnmanaged(BoundCatalogObject),
    catalog: table_catalog.CatalogSource,
    plan: ddl_plan.AlterTablePlan,
    session: catalog_resources.SqlCatalogSession,
) !void {
    for (plan.operations) |operation| {
        switch (operation) {
            .add_column => |add_column| try appendBoundCatalogObjectsForForeignKeysAlloc(alloc, objects, catalog, plan.table_name, add_column.foreign_keys, session),
            .add_foreign_key => |foreign_key| try appendBoundCatalogObjectsForForeignKeysAlloc(alloc, objects, catalog, plan.table_name, &.{foreign_key}, session),
            else => {},
        }
    }
}

fn collectTableDdlBoundCatalogObjectsAlloc(
    alloc: std.mem.Allocator,
    objects: *std.ArrayListUnmanaged(BoundCatalogObject),
    catalog: table_catalog.CatalogSource,
    plan: TableDdlLogicalPlan,
    session: catalog_resources.SqlCatalogSession,
) !void {
    switch (plan) {
        .moved => return,
        .create_table => |create| {
            try appendExistingCreateTargetIfPresentAlloc(alloc, objects, catalog, create.table_name, session);
            try appendBoundCatalogObjectsForForeignKeysAlloc(alloc, objects, catalog, create.table_name, create.foreign_keys, session);
        },
        .table_clone => |clone| {
            try appendExistingCreateTargetIfPresentAlloc(alloc, objects, catalog, clone.table_name, session);
            try appendOptionalBoundCatalogObjectForCatalogTableAlloc(alloc, objects, .source, catalog, clone.source_table_name, session, false);
        },
        .view_catalog => |view| switch (view) {
            .create => |create| {
                try appendExistingCreateTargetIfPresentAlloc(alloc, objects, catalog, create.view_name, session);
                try appendOptionalBoundCatalogObjectForCatalogTableAlloc(alloc, objects, .source, catalog, create.source_table_name, session, false);
            },
            .rename => |rename| try appendOptionalBoundCatalogObjectForCatalogTableAlloc(alloc, objects, .target, catalog, rename.view_name, session, false),
            .drop => |drop| try appendOptionalBoundCatalogObjectForCatalogTableAlloc(alloc, objects, .target, catalog, drop.view_name, session, drop.if_exists),
        },
        .materialized_view_catalog => |view| switch (view) {
            .create => |create| {
                try appendExistingCreateTargetIfPresentAlloc(alloc, objects, catalog, create.view_name, session);
                try appendOptionalBoundCatalogObjectForCatalogTableAlloc(alloc, objects, .source, catalog, create.source_table_name, session, false);
            },
            .refresh => |refresh| try appendOptionalBoundCatalogObjectForCatalogTableAlloc(alloc, objects, .target, catalog, refresh.view_name, session, false),
            .drop => |drop| try appendOptionalBoundCatalogObjectForCatalogTableAlloc(alloc, objects, .target, catalog, drop.view_name, session, drop.if_exists),
        },
        .relation_lifetime => |lifetime| try appendExistingCreateTargetIfPresentAlloc(alloc, objects, catalog, lifetime.create_table.table_name, session),
        .table_partition_catalog => |partition| switch (partition) {
            .create_partitioned => |create| try appendExistingCreateTargetIfPresentAlloc(alloc, objects, catalog, create.create_table.table_name, session),
            .create_partition => |create| {
                try appendExistingCreateTargetIfPresentAlloc(alloc, objects, catalog, create.table_name, session);
                try appendOptionalBoundCatalogObjectForCatalogTableAlloc(alloc, objects, .source, catalog, create.parent_table_name, session, false);
            },
            .attach => |attach| {
                try appendOptionalBoundCatalogObjectForCatalogTableAlloc(alloc, objects, .target, catalog, attach.parent_table_name, session, false);
                try appendOptionalBoundCatalogObjectForCatalogTableAlloc(alloc, objects, .source, catalog, attach.partition_table_name, session, false);
            },
            .detach => |detach| {
                try appendOptionalBoundCatalogObjectForCatalogTableAlloc(alloc, objects, .target, catalog, detach.parent_table_name, session, false);
                try appendOptionalBoundCatalogObjectForCatalogTableAlloc(alloc, objects, .source, catalog, detach.partition_table_name, session, false);
            },
        },
        .create_index => |create| if (create.table_name.len != 0) {
            try appendOptionalBoundCatalogObjectForCatalogTableAlloc(alloc, objects, .target, catalog, create.table_name, session, false);
        },
        .drop_index => |drop| try appendOptionalBoundCatalogObjectForCatalogIndexAlloc(alloc, objects, .target, catalog, drop.index_name, session, drop.if_exists),
        .drop_table => |drop| try appendOptionalBoundCatalogObjectForCatalogTableAlloc(alloc, objects, .target, catalog, drop.table_name, session, drop.if_exists),
        .alter_table => |alter| {
            try appendOptionalBoundCatalogObjectForCatalogTableAlloc(alloc, objects, .target, catalog, alter.table_name, session, alter.if_exists);
            try appendBoundCatalogObjectsForAlterTableForeignKeysAlloc(alloc, objects, catalog, alter, session);
        },
        .create_update_policy => |policy| try appendOptionalBoundCatalogObjectForCatalogTableAlloc(alloc, objects, .target, catalog, policy.table_name, session, false),
    }
}

fn collectAuthDdlBoundCatalogObjectsAlloc(
    alloc: std.mem.Allocator,
    objects: *std.ArrayListUnmanaged(BoundCatalogObject),
    catalog: table_catalog.CatalogSource,
    plan: AuthorizationLogicalPlan,
    session: catalog_resources.SqlCatalogSession,
) !void {
    switch (plan) {
        .authorization_catalog => |authorization| switch (authorization) {
            .grant_privilege => |grant| {
                if (std.ascii.eqlIgnoreCase(grant.object_kind, "table")) {
                    try appendOptionalBoundCatalogObjectForCatalogTableAlloc(alloc, objects, .target, catalog, grant.object_name, session, false);
                } else if (std.ascii.eqlIgnoreCase(grant.object_kind, "all_tables_in_schema")) {
                    try appendBoundCatalogObjectsForCatalogSchemaTablesAlloc(alloc, objects, .target, catalog, grant.object_name, session);
                }
            },
            .revoke_privilege => |revoke| {
                if (std.ascii.eqlIgnoreCase(revoke.object_kind, "table")) {
                    try appendOptionalBoundCatalogObjectForCatalogTableAlloc(alloc, objects, .target, catalog, revoke.object_name, session, false);
                } else if (std.ascii.eqlIgnoreCase(revoke.object_kind, "all_tables_in_schema")) {
                    try appendBoundCatalogObjectsForCatalogSchemaTablesAlloc(alloc, objects, .target, catalog, revoke.object_name, session);
                }
            },
            else => {},
        },
        .row_security_catalog => |row_security| switch (row_security) {
            .alter_table => |alter| try appendOptionalBoundCatalogObjectForCatalogTableAlloc(alloc, objects, .target, catalog, alter.table_name, session, false),
            .create_policy => |policy| try appendOptionalBoundCatalogObjectForCatalogTableAlloc(alloc, objects, .target, catalog, policy.table_name, session, false),
            .alter_policy => |policy| try appendOptionalBoundCatalogObjectForCatalogTableAlloc(alloc, objects, .target, catalog, policy.table_name, session, false),
            .drop_policy => |policy| try appendOptionalBoundCatalogObjectForCatalogTableAlloc(alloc, objects, .target, catalog, policy.table_name, session, policy.if_exists),
        },
    }
}

fn collectRoutineDdlBoundCatalogObjectsAlloc(
    alloc: std.mem.Allocator,
    objects: *std.ArrayListUnmanaged(BoundCatalogObject),
    catalog: table_catalog.CatalogSource,
    plan: RoutineLogicalPlan,
    session: catalog_resources.SqlCatalogSession,
) !void {
    switch (plan) {
        .function_catalog, .procedure_call => {},
        .trigger_catalog => |trigger| switch (trigger) {
            .create => |create| try appendOptionalBoundCatalogObjectForCatalogTableAlloc(alloc, objects, .target, catalog, create.table_name, session, false),
            .drop => |drop| try appendOptionalBoundCatalogObjectForCatalogTableAlloc(alloc, objects, .target, catalog, drop.table_name, session, drop.if_exists),
        },
    }
}

fn collectBulkIoDdlBoundCatalogObjectsAlloc(
    alloc: std.mem.Allocator,
    objects: *std.ArrayListUnmanaged(BoundCatalogObject),
    catalog: table_catalog.CatalogSource,
    plan: ddl_plan.BulkIoPlan,
    session: catalog_resources.SqlCatalogSession,
) !void {
    try appendOptionalBoundCatalogObjectForCatalogTableAlloc(alloc, objects, .target, catalog, plan.table_name, session, false);
}

fn collectMaintenanceDdlBoundCatalogObjectsAlloc(
    alloc: std.mem.Allocator,
    objects: *std.ArrayListUnmanaged(BoundCatalogObject),
    catalog: table_catalog.CatalogSource,
    plan: ddl_plan.MaintenanceJobPlan,
    session: catalog_resources.SqlCatalogSession,
) !void {
    switch (plan) {
        .vacuum => |vacuum| try appendOptionalBoundCatalogObjectForCatalogTableAlloc(alloc, objects, .target, catalog, vacuum.table_name, session, false),
        .analyze => |analyze| try appendOptionalBoundCatalogObjectForCatalogTableAlloc(alloc, objects, .target, catalog, analyze.table_name, session, false),
        .cluster => |cluster| {
            try appendOptionalBoundCatalogObjectForCatalogTableAlloc(alloc, objects, .target, catalog, cluster.table_name, session, false);
            try validateMaintenanceClusterIndexAlloc(alloc, catalog, cluster, session);
        },
        .reindex => |reindex| switch (reindex.target) {
            .table => try appendOptionalBoundCatalogObjectForCatalogTableAlloc(alloc, objects, .target, catalog, reindex.name, session, false),
            .index => try appendBoundCatalogObjectForCatalogIndexAlloc(alloc, objects, .target, catalog, reindex.name, session),
            .schema => try appendBoundCatalogObjectsForCatalogSchemaTablesAlloc(alloc, objects, .target, catalog, reindex.name, session),
            .database => try appendBoundCatalogObjectsForCatalogDatabaseTablesAlloc(alloc, objects, .target, catalog, reindex.name),
            .system => {},
        },
    }
}

fn validateMaintenanceClusterIndexAlloc(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    cluster: ddl_plan.ClusterMaintenancePlan,
    session: catalog_resources.SqlCatalogSession,
) !void {
    const index_name = cluster.index_name orelse return;
    const target = try ownedCatalogTableRefForObjectNameAlloc(alloc, cluster.table_name, session);
    defer deinitCatalogTableRef(alloc, target);
    var snapshot = try catalog.adminSnapshot();
    defer catalog.freeAdminSnapshot(&snapshot);
    const table = qualifiedTableRecord(&snapshot, target.database_name, target.namespace_name, target.table_name) orelse return error.InvalidSqlCatalog;
    const index_table = try catalogTableForIndexNameAlloc(alloc, &snapshot, index_name, session);
    if (table.table_id != index_table.table_id or
        !std.mem.eql(u8, table.database_name, index_table.database_name) or
        !std.mem.eql(u8, table.namespace_name, index_table.namespace_name) or
        !std.mem.eql(u8, table.name, index_table.name))
    {
        return error.InvalidSqlCatalog;
    }
}

fn collectDdlBoundCatalogObjectsAlloc(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    logical_plan: LogicalSqlPlan,
    session: catalog_resources.SqlCatalogSession,
) ![]BoundCatalogObject {
    var bound_objects = std.ArrayListUnmanaged(BoundCatalogObject).empty;
    var transferred = false;
    errdefer if (!transferred) {
        deinitBoundCatalogObjects(alloc, bound_objects.items);
        bound_objects.deinit(alloc);
    };

    switch (logical_plan) {
        .table_ddl => |plan| try collectTableDdlBoundCatalogObjectsAlloc(alloc, &bound_objects, catalog, plan, session),
        .auth => |plan| try collectAuthDdlBoundCatalogObjectsAlloc(alloc, &bound_objects, catalog, plan, session),
        .routine => |plan| try collectRoutineDdlBoundCatalogObjectsAlloc(alloc, &bound_objects, catalog, plan, session),
        .bulk_io => |plan| try collectBulkIoDdlBoundCatalogObjectsAlloc(alloc, &bound_objects, catalog, plan, session),
        .maintenance => |plan| try collectMaintenanceDdlBoundCatalogObjectsAlloc(alloc, &bound_objects, catalog, plan, session),
        else => {},
    }

    const out = try bound_objects.toOwnedSlice(alloc);
    transferred = true;
    return out;
}

fn ddlAuthorizationDefaultPermission(logical_plan: LogicalSqlPlan) BoundSqlAuthorizationPermission {
    return switch (logical_plan) {
        .bulk_io => |plan| switch (plan.direction) {
            .from => .write,
            .to => .read,
        },
        else => .admin,
    };
}

fn resolveDdlCatalogFactsFromParsedSqlWithSessionAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const tokenized.ParsedSql,
    catalog: table_catalog.CatalogSource,
    session: catalog_resources.SqlCatalogSession,
    function_bindings: lower_expr.SqlFunctionBindings,
) !CatalogBoundDdlPlanFacts {
    return try resolveDdlCatalogFactsFromParsedSqlWithSessionAndAuthorizationAlloc(alloc, parsed_sql, catalog, session, function_bindings, .{});
}

fn resolveDdlCatalogFactsFromParsedSqlWithSessionAndAuthorizationAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const tokenized.ParsedSql,
    catalog: table_catalog.CatalogSource,
    session: catalog_resources.SqlCatalogSession,
    function_bindings: lower_expr.SqlFunctionBindings,
    authorization_options: BoundSqlAuthorizationOptions,
) !CatalogBoundDdlPlanFacts {
    var logical_plan = try ddl_plan.parseLogicalDdlPlanAlloc(alloc, parsed_sql, function_bindings);
    errdefer logical_plan.deinit(alloc);
    var out = CatalogBoundDdlPlanFacts{
        .bound_objects = try collectDdlBoundCatalogObjectsAlloc(alloc, catalog, logical_plan, session),
        .logical_plan = logical_plan,
    };
    errdefer out.deinit(alloc);
    const default_permission = ddlAuthorizationDefaultPermission(logical_plan);
    logical_plan = .{ .other_ddl = .{ .moved = {} } };
    out.authorization = try boundSqlAuthorizationForObjectsAlloc(alloc, out.bound_objects, authorization_options, default_permission);
    return out;
}

pub fn bindReadPlanCatalogStatementAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const tokenized.ParsedSql,
    catalog: table_catalog.CatalogSource,
) !BoundSqlStatement {
    return try bindReadPlanCatalogStatementWithSessionAlloc(alloc, parsed_sql, catalog, catalog_resources.SqlCatalogSession.default());
}

pub fn bindReadPlanCatalogStatementWithSessionAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const tokenized.ParsedSql,
    catalog: table_catalog.CatalogSource,
    session: catalog_resources.SqlCatalogSession,
) !BoundSqlStatement {
    return try bindReadPlanCatalogStatementWithSessionAndAuthorizationAlloc(alloc, parsed_sql, catalog, session, .{});
}

pub fn bindReadPlanCatalogStatementWithSessionAndAuthorizationAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const tokenized.ParsedSql,
    catalog: table_catalog.CatalogSource,
    session: catalog_resources.SqlCatalogSession,
    authorization_options: BoundSqlAuthorizationOptions,
) !BoundSqlStatement {
    try requireParsedCatalogReadStatement(parsed_sql.statement);
    var bound_session = try BoundSqlSession.fromSessionAlloc(alloc, session);
    errdefer bound_session.deinit(alloc);
    var resolved = try resolveReadPlanCatalogSourceSchemaFromParsedSqlWithSessionAndAuthorizationAlloc(alloc, parsed_sql, catalog, session, authorization_options);
    errdefer resolved.deinit(alloc);
    return .{
        .parsed_sql = parsed_sql,
        .statement = parsed_sql.statement,
        .session = bound_session,
        .binding = .{ .read_catalog = resolved },
    };
}

pub fn bindDdlStatementWithSessionAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const tokenized.ParsedSql,
    session: catalog_resources.SqlCatalogSession,
) !BoundSqlStatement {
    try requireParsedDdlStatement(parsed_sql.statement);
    var bound_session = try BoundSqlSession.fromSessionAlloc(alloc, session);
    errdefer bound_session.deinit(alloc);
    return .{
        .parsed_sql = parsed_sql,
        .statement = parsed_sql.statement,
        .session = bound_session,
        .binding = .none,
    };
}

pub fn bindDdlStatementWithCatalogSessionAndFunctionBindingsAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const tokenized.ParsedSql,
    catalog: table_catalog.CatalogSource,
    session: catalog_resources.SqlCatalogSession,
    function_bindings: lower_expr.SqlFunctionBindings,
) !BoundSqlStatement {
    return try bindDdlStatementWithCatalogSessionFunctionBindingsAndAuthorizationAlloc(alloc, parsed_sql, catalog, session, function_bindings, .{});
}

pub fn bindDdlStatementWithCatalogSessionFunctionBindingsAndAuthorizationAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const tokenized.ParsedSql,
    catalog: table_catalog.CatalogSource,
    session: catalog_resources.SqlCatalogSession,
    function_bindings: lower_expr.SqlFunctionBindings,
    authorization_options: BoundSqlAuthorizationOptions,
) !BoundSqlStatement {
    try requireParsedDdlStatement(parsed_sql.statement);
    var bound_session = try BoundSqlSession.fromSessionAlloc(alloc, session);
    errdefer bound_session.deinit(alloc);
    var facts = try resolveDdlCatalogFactsFromParsedSqlWithSessionAndAuthorizationAlloc(alloc, parsed_sql, catalog, session, function_bindings, authorization_options);
    errdefer facts.deinit(alloc);
    return .{
        .parsed_sql = parsed_sql,
        .statement = parsed_sql.statement,
        .session = bound_session,
        .binding = .{ .ddl_catalog = facts },
    };
}

pub fn bindDdlStatementWithCatalogSessionAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const tokenized.ParsedSql,
    catalog: table_catalog.CatalogSource,
    session: catalog_resources.SqlCatalogSession,
) !BoundSqlStatement {
    return try bindDdlStatementWithCatalogSessionAndFunctionBindingsAlloc(alloc, parsed_sql, catalog, session, .{});
}

pub fn bindDdlStatementWithCatalogAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const tokenized.ParsedSql,
    catalog: table_catalog.CatalogSource,
) !BoundSqlStatement {
    return try bindDdlStatementWithCatalogSessionAlloc(alloc, parsed_sql, catalog, catalog_resources.SqlCatalogSession.default());
}

pub fn bindDdlStatementAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const tokenized.ParsedSql,
) !BoundSqlStatement {
    return try bindDdlStatementWithSessionAlloc(alloc, parsed_sql, catalog_resources.SqlCatalogSession.default());
}

fn joinedWriteSourceTableNamesFromStatementAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    statement_index: usize,
) !?InsertSourceTableNames {
    if (statement_index >= tokens.len or tokens[statement_index].kind != .identifier) return null;
    if (tokens[statement_index].matchesKeywordTag(.update)) {
        var target_index: usize = statement_index + 1;
        _ = consumeKeyword(tokens, &target_index, .only);
        if (target_index >= tokens.len or tokens[target_index].kind != .identifier) return error.UnsupportedSqlShape;
        const target = try normalizeSqlObjectIdentifierAlloc(alloc, tokens[target_index].text);
        var target_transferred = false;
        errdefer if (!target_transferred) alloc.free(target);

        const from_index = findTopLevelKeyword(tokens[target_index + 1 ..], .from) orelse {
            const where_index = findTopLevelKeyword(tokens[target_index + 1 ..], .where) orelse {
                alloc.free(target);
                return null;
            };
            const source = try joinedWriteSemiJoinSourceTableAlloc(alloc, tokens[target_index + 1 + where_index + 1 ..]) orelse {
                alloc.free(target);
                return null;
            };
            errdefer alloc.free(source);

            target_transferred = true;
            return .{ .target = target, .source = source };
        };
        var source_index = target_index + 1 + from_index + 1;
        _ = consumeKeyword(tokens, &source_index, .only);
        if (source_index >= tokens.len or tokens[source_index].kind != .identifier) return error.UnsupportedSqlShape;
        const source = try normalizeSqlObjectIdentifierAlloc(alloc, tokens[source_index].text);
        errdefer alloc.free(source);

        target_transferred = true;
        return .{ .target = target, .source = source };
    }

    if (tokens[statement_index].matchesKeywordTag(.delete)) {
        var target_index: usize = statement_index + 1;
        if (!consumeKeyword(tokens, &target_index, .from)) return null;
        _ = consumeKeyword(tokens, &target_index, .only);
        if (target_index >= tokens.len or tokens[target_index].kind != .identifier) return error.UnsupportedSqlShape;
        const target = try normalizeSqlObjectIdentifierAlloc(alloc, tokens[target_index].text);
        var target_transferred = false;
        errdefer if (!target_transferred) alloc.free(target);

        const using_index = findTopLevelKeyword(tokens[target_index + 1 ..], .using) orelse {
            const where_index = findTopLevelKeyword(tokens[target_index + 1 ..], .where) orelse {
                alloc.free(target);
                return null;
            };
            const source = try joinedWriteSemiJoinSourceTableAlloc(alloc, tokens[target_index + 1 + where_index + 1 ..]) orelse {
                alloc.free(target);
                return null;
            };
            errdefer alloc.free(source);

            target_transferred = true;
            return .{ .target = target, .source = source };
        };
        var source_index = target_index + 1 + using_index + 1;
        _ = consumeKeyword(tokens, &source_index, .only);
        if (source_index >= tokens.len or tokens[source_index].kind != .identifier) return error.UnsupportedSqlShape;
        const source = try normalizeSqlObjectIdentifierAlloc(alloc, tokens[source_index].text);
        errdefer alloc.free(source);

        target_transferred = true;
        return .{ .target = target, .source = source };
    }

    if (tokens[statement_index].matchesKeywordTag(.merge)) {
        var target_index: usize = statement_index + 1;
        if (!consumeKeyword(tokens, &target_index, .into)) return null;
        _ = consumeKeyword(tokens, &target_index, .only);
        if (target_index >= tokens.len or tokens[target_index].kind != .identifier) return error.UnsupportedSqlShape;
        const target = try normalizeSqlObjectIdentifierAlloc(alloc, tokens[target_index].text);
        var target_transferred = false;
        errdefer if (!target_transferred) alloc.free(target);

        const using_index = findTopLevelKeyword(tokens[target_index + 1 ..], .using) orelse {
            alloc.free(target);
            return null;
        };
        var source_index = target_index + 1 + using_index + 1;
        _ = consumeKeyword(tokens, &source_index, .only);
        if (source_index >= tokens.len or tokens[source_index].kind != .identifier) return error.UnsupportedSqlShape;
        const source = try normalizeSqlObjectIdentifierAlloc(alloc, tokens[source_index].text);
        errdefer alloc.free(source);

        target_transferred = true;
        return .{ .target = target, .source = source };
    }

    return null;
}

fn joinedWriteSemiJoinSourceTableAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
) !?[]const u8 {
    var index: usize = 0;
    while (index < tokens.len) : (index += 1) {
        const token = tokens[index];
        if (token.kind == .semicolon) return null;
        if (token.kind != .lparen) continue;

        const close_index = findMatchingRParenIndex(tokens, index) orelse return error.UnsupportedSqlShape;
        const is_exists = index > 0 and tokens[index - 1].matchesKeywordTag(.exists);
        const is_in = index > 0 and tokens[index - 1].matchesKeywordTag(.in);
        if (is_exists or is_in) {
            const body = tokens[index + 1 .. close_index];
            if (body.len > 0 and body[0].matchesKeywordTag(.select)) {
                const from_index = findTopLevelKeyword(body, .from) orelse return error.UnsupportedSqlShape;
                var source_index = from_index + 1;
                _ = consumeKeyword(body, &source_index, .only);
                if (source_index >= body.len or body[source_index].kind != .identifier) return error.UnsupportedSqlShape;
                return try normalizeSqlObjectIdentifierAlloc(alloc, body[source_index].text);
            }
        }

        index = close_index;
    }
    return null;
}

fn joinedWriteSourceTableNamesFromWithAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
) !?InsertSourceTableNames {
    var index: usize = 1;
    _ = consumeKeyword(tokens, &index, .recursive);

    var cte_bindings = std.ArrayListUnmanaged(CteSourceBinding).empty;
    defer {
        for (cte_bindings.items) |*binding| binding.deinit(alloc);
        cte_bindings.deinit(alloc);
    }

    while (true) {
        if (index >= tokens.len or tokens[index].kind != .identifier) return error.UnsupportedSqlShape;
        const cte_name = try normalizeSqlObjectIdentifierAlloc(alloc, tokens[index].text);
        var cte_name_transferred = false;
        errdefer if (!cte_name_transferred) alloc.free(cte_name);
        if (cteBindingIndex(cte_bindings.items, cte_name) != null) return error.UnsupportedSqlShape;
        index += 1;

        if (index < tokens.len and tokens[index].kind == .lparen) {
            index = (findMatchingRParenIndex(tokens, index) orelse return error.UnsupportedSqlShape) + 1;
        }
        if (!consumeKeyword(tokens, &index, .as)) return error.UnsupportedSqlShape;
        try parser.consumeCteMaterializationHint(tokens, &index);
        if (index >= tokens.len or tokens[index].kind != .lparen) return error.UnsupportedSqlShape;
        const close_index = findMatchingRParenIndex(tokens, index) orelse return error.UnsupportedSqlShape;
        if (index + 1 >= close_index) return error.UnsupportedSqlShape;

        const cte_source = cte_source: {
            var cte_tables = selectReadTableNamesAlloc(alloc, tokens[index + 1 .. close_index], 0) catch |err| switch (err) {
                error.UnsupportedSqlShape => null,
                else => return err,
            };
            if (cte_tables) |*tables| {
                defer tables.deinit(alloc);
                try resolveSelectReadTablesAgainstCtes(alloc, cte_bindings.items, tables);
                if (tables.source) |source| {
                    if (!std.mem.eql(u8, tables.left, source) and !std.mem.eql(u8, cte_name, source)) return error.UnsupportedSqlShape;
                }
                break :cte_source try alloc.dupe(u8, tables.left);
            }
            break :cte_source try writeTargetTableNameFromTokensAlloc(alloc, tokens[index + 1 .. close_index]);
        };
        errdefer alloc.free(cte_source);

        try cte_bindings.append(alloc, .{
            .name = cte_name,
            .source = cte_source,
        });
        cte_name_transferred = true;

        index = close_index + 1;
        if (index < tokens.len and tokens[index].kind == .comma) {
            index += 1;
            continue;
        }
        break;
    }

    var final = (try joinedWriteSourceTableNamesFromStatementAlloc(alloc, tokens, index)) orelse return null;
    errdefer final.deinit(alloc);
    final.target = try resolveTableNameAgainstCtesAlloc(alloc, cte_bindings.items, final.target);
    final.source = try resolveTableNameAgainstCtesAlloc(alloc, cte_bindings.items, final.source);
    return final;
}

pub fn readSourceTableNamesFromParsedSqlAlloc(alloc: std.mem.Allocator, parsed_sql: *const tokenized.ParsedSql) !?ReadSourceTableNames {
    switch (parsed_sql.statement) {
        .read => {},
        else => return error.UnsupportedSqlShape,
    }
    return try readSourceTableNamesFromTokensAlloc(alloc, parsed_sql.items());
}

fn readSourceTableNamesFromTokensAlloc(alloc: std.mem.Allocator, tokens: []const Token) !?ReadSourceTableNames {
    if (tokens.len == 0 or tokens[0].kind != .identifier) return null;
    if (tokens[0].matchesKeywordTag(.with)) return try readSourceTableNamesFromWithAlloc(alloc, tokens);
    if (!tokens[0].matchesKeywordTag(.select)) return null;
    return try readSourceTableNamesFromSelectAlloc(alloc, tokens, 0);
}

fn readSourceTableNamesFromSelectAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    select_index: usize,
) !?ReadSourceTableNames {
    var tables = (try selectReadTableNamesAlloc(alloc, tokens, select_index)) orelse return null;
    errdefer tables.deinit(alloc);
    const source = tables.source orelse {
        tables.deinit(alloc);
        return null;
    };
    tables.source = null;
    return .{ .left = tables.left, .source = source };
}

fn selectReadTableNamesAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    select_index: usize,
) !?SelectReadTableNames {
    if (select_index >= tokens.len or !tokens[select_index].matchesKeywordTag(.select)) return null;

    const from_index = if (findTopLevelKeyword(tokens[select_index..], .from)) |relative|
        select_index + relative
    else
        return null;
    var left_index = from_index + 1;
    _ = consumeKeyword(tokens, &left_index, .only);
    if (left_index >= tokens.len or tokens[left_index].kind != .identifier) return error.UnsupportedSqlShape;
    const left = try normalizeSqlObjectIdentifierAlloc(alloc, tokens[left_index].text);
    var left_transferred = false;
    errdefer if (!left_transferred) alloc.free(left);

    const join_index = if (findTopLevelKeyword(tokens[left_index + 1 ..], .join)) |relative|
        left_index + 1 + relative
    else {
        if (try selectSetOperationSourceTableNameAlloc(alloc, tokens, left_index + 1)) |source| {
            left_transferred = true;
            return .{ .left = left, .source = source };
        }
        const source = try alloc.dupe(u8, left);
        errdefer alloc.free(source);
        left_transferred = true;
        return .{ .left = left, .source = source };
    };
    var source_index = join_index + 1;
    if (consumeKeyword(tokens, &source_index, .lateral)) {
        if (source_index >= tokens.len or tokens[source_index].kind != .lparen) return error.UnsupportedSqlShape;
        const close_index = findMatchingRParenIndex(tokens, source_index) orelse return error.UnsupportedSqlShape;
        const inner_from = findTopLevelKeyword(tokens[source_index + 1 .. close_index], .from) orelse return error.UnsupportedSqlShape;
        source_index = source_index + 1 + inner_from + 1;
    }
    _ = consumeKeyword(tokens, &source_index, .only);
    if (source_index >= tokens.len or tokens[source_index].kind != .identifier) return error.UnsupportedSqlShape;
    const source = try normalizeSqlObjectIdentifierAlloc(alloc, tokens[source_index].text);
    errdefer alloc.free(source);

    left_transferred = true;
    return .{ .left = left, .source = source };
}

fn selectSetOperationSourceTableNameAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    start_index: usize,
) !?[]const u8 {
    var depth: usize = 0;
    var i = start_index;
    while (i < tokens.len) : (i += 1) {
        const token = tokens[i];
        switch (token.kind) {
            .lparen => depth += 1,
            .rparen => {
                if (depth == 0) return null;
                depth -= 1;
            },
            .identifier => {
                if (depth != 0 or !selectSetOperationKeyword(token)) continue;
                var select_index = i + 1;
                if (token.matchesKeywordTag(.@"union")) {
                    _ = consumeKeyword(tokens, &select_index, .all) or consumeKeyword(tokens, &select_index, .distinct);
                } else {
                    _ = consumeKeyword(tokens, &select_index, .distinct);
                }
                if (select_index >= tokens.len or !tokens[select_index].matchesKeywordTag(.select)) {
                    return error.UnsupportedSqlShape;
                }
                const from_relative = findTopLevelKeyword(tokens[select_index + 1 ..], .from) orelse return error.UnsupportedSqlShape;
                var source_index = select_index + 1 + from_relative + 1;
                _ = consumeKeyword(tokens, &source_index, .only);
                if (source_index >= tokens.len or tokens[source_index].kind != .identifier) return error.UnsupportedSqlShape;
                return try normalizeSqlObjectIdentifierAlloc(alloc, tokens[source_index].text);
            },
            else => {},
        }
    }
    return null;
}

fn selectSetOperationKeyword(token: Token) bool {
    return token.matchesKeywordTag(.@"union") or
        token.matchesKeywordTag(.intersect) or
        token.matchesKeywordTag(.except);
}

fn readSourceTableNamesFromWithAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
) !?ReadSourceTableNames {
    var index: usize = 1;
    const recursive = consumeKeyword(tokens, &index, .recursive);

    var cte_bindings = std.ArrayListUnmanaged(CteSourceBinding).empty;
    defer {
        for (cte_bindings.items) |*binding| binding.deinit(alloc);
        cte_bindings.deinit(alloc);
    }

    while (true) {
        if (index >= tokens.len or tokens[index].kind != .identifier) return error.UnsupportedSqlShape;
        const cte_name = try normalizeSqlObjectIdentifierAlloc(alloc, tokens[index].text);
        var cte_name_transferred = false;
        errdefer if (!cte_name_transferred) alloc.free(cte_name);
        if (cteBindingIndex(cte_bindings.items, cte_name) != null) return error.UnsupportedSqlShape;
        index += 1;

        if (index < tokens.len and tokens[index].kind == .lparen) {
            index = (findMatchingRParenIndex(tokens, index) orelse return error.UnsupportedSqlShape) + 1;
        }
        if (!consumeKeyword(tokens, &index, .as)) return error.UnsupportedSqlShape;
        try parser.consumeCteMaterializationHint(tokens, &index);
        if (index >= tokens.len or tokens[index].kind != .lparen) return error.UnsupportedSqlShape;
        const close_index = findMatchingRParenIndex(tokens, index) orelse return error.UnsupportedSqlShape;
        if (index + 1 >= close_index) return error.UnsupportedSqlShape;

        const cte_source = try readCteSourceTableNameAlloc(
            alloc,
            cte_bindings.items,
            cte_name,
            tokens[index + 1 .. close_index],
            recursive,
        );
        errdefer alloc.free(cte_source);

        try cte_bindings.append(alloc, .{
            .name = cte_name,
            .source = cte_source,
        });
        cte_name_transferred = true;

        index = close_index + 1;
        if (index < tokens.len and tokens[index].kind == .comma) {
            index += 1;
            continue;
        }
        break;
    }

    var final_tables = (try selectReadTableNamesAlloc(alloc, tokens, index)) orelse return null;
    errdefer final_tables.deinit(alloc);
    try resolveSelectReadTablesAgainstCtes(alloc, cte_bindings.items, &final_tables);
    const source = final_tables.source orelse {
        final_tables.deinit(alloc);
        return null;
    };
    final_tables.source = null;
    return .{ .left = final_tables.left, .source = source };
}

fn readCteSourceTableNameAlloc(
    alloc: std.mem.Allocator,
    bindings: []const CteSourceBinding,
    cte_name: []const u8,
    body_tokens: []const Token,
    recursive: bool,
) ![]const u8 {
    const source_tokens = if (recursive) recursiveReadCteAnchorTokens(body_tokens) orelse body_tokens else body_tokens;
    var cte_tables = (try selectReadTableNamesAlloc(alloc, source_tokens, 0)) orelse return error.UnsupportedSqlShape;
    defer cte_tables.deinit(alloc);
    try resolveSelectReadTablesAgainstCtes(alloc, bindings, &cte_tables);
    if (cte_tables.source) |source| {
        if (!std.mem.eql(u8, cte_tables.left, source)) return error.UnsupportedSqlShape;
    }
    if (recursive and std.mem.eql(u8, cte_tables.left, cte_name)) return error.UnsupportedSqlShape;
    return try alloc.dupe(u8, cte_tables.left);
}

fn recursiveReadCteAnchorTokens(tokens: []const Token) ?[]const Token {
    var depth: usize = 0;
    for (tokens, 0..) |token, index| {
        switch (token.kind) {
            .lparen => depth += 1,
            .rparen => {
                if (depth == 0) return null;
                depth -= 1;
            },
            .identifier => {
                if (depth == 0 and selectSetOperationKeyword(token)) {
                    if (index == 0) return null;
                    return tokens[0..index];
                }
            },
            else => {},
        }
    }
    return null;
}

fn normalizeSqlObjectIdentifierAlloc(alloc: std.mem.Allocator, identifier: []const u8) ![]const u8 {
    return try grammar.normalizeSqlObjectIdentifierAlloc(alloc, identifier);
}

fn insertSourceTableNamesFromInsertAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    insert_index: usize,
) !?InsertSourceTableNames {
    if (insert_index >= tokens.len or !tokens[insert_index].matchesKeywordTag(.insert)) return null;
    var index: usize = insert_index + 1;
    if (!consumeKeyword(tokens, &index, .into)) return null;
    _ = consumeKeyword(tokens, &index, .only);
    if (index >= tokens.len or tokens[index].kind != .identifier) return error.UnsupportedSqlShape;
    const target = try normalizeSqlObjectIdentifierAlloc(alloc, tokens[index].text);
    var target_transferred = false;
    errdefer if (!target_transferred) alloc.free(target);
    index += 1;

    const select_index = findTopLevelKeyword(tokens[index..], .select) orelse {
        alloc.free(target);
        return null;
    };
    const absolute_select = index + select_index;
    const from_index = findTopLevelKeyword(tokens[absolute_select + 1 ..], .from) orelse return error.UnsupportedSqlShape;
    var source_index = absolute_select + 1 + from_index + 1;
    _ = consumeKeyword(tokens, &source_index, .only);
    if (source_index >= tokens.len or tokens[source_index].kind != .identifier) return error.UnsupportedSqlShape;
    const source = try normalizeSqlObjectIdentifierAlloc(alloc, tokens[source_index].text);
    errdefer alloc.free(source);

    target_transferred = true;
    return .{ .target = target, .source = source };
}

fn insertSourceTableNamesFromWithAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
) !?InsertSourceTableNames {
    var index: usize = 1;
    if (consumeKeyword(tokens, &index, .recursive)) return error.UnsupportedSqlShape;

    var cte_names = std.ArrayListUnmanaged([]const u8).empty;
    defer {
        for (cte_names.items) |name| alloc.free(name);
        cte_names.deinit(alloc);
    }
    var base_source: ?[]const u8 = null;
    errdefer if (base_source) |source| alloc.free(source);

    while (true) {
        if (index >= tokens.len or tokens[index].kind != .identifier) return error.UnsupportedSqlShape;
        const cte_name = try normalizeSqlObjectIdentifierAlloc(alloc, tokens[index].text);
        var cte_name_transferred = false;
        errdefer if (!cte_name_transferred) alloc.free(cte_name);
        if (sqlStringSliceContains(cte_names.items, cte_name)) return error.UnsupportedSqlShape;
        try cte_names.append(alloc, cte_name);
        cte_name_transferred = true;
        index += 1;

        if (index < tokens.len and tokens[index].kind == .lparen) {
            index = (findMatchingRParenIndex(tokens, index) orelse return error.UnsupportedSqlShape) + 1;
        }
        if (!consumeKeyword(tokens, &index, .as)) return error.UnsupportedSqlShape;
        try parser.consumeCteMaterializationHint(tokens, &index);
        if (index >= tokens.len or tokens[index].kind != .lparen) return error.UnsupportedSqlShape;
        const close_index = findMatchingRParenIndex(tokens, index) orelse return error.UnsupportedSqlShape;
        const from_index = findTopLevelKeyword(tokens[index + 1 .. close_index], .from) orelse return error.UnsupportedSqlShape;
        var source_index = index + 1 + from_index + 1;
        _ = consumeKeyword(tokens, &source_index, .only);
        if (source_index >= close_index or tokens[source_index].kind != .identifier) return error.UnsupportedSqlShape;
        const source = try normalizeSqlObjectIdentifierAlloc(alloc, tokens[source_index].text);
        var source_transferred = false;
        errdefer if (!source_transferred) alloc.free(source);
        if (!sqlStringSliceContains(cte_names.items, source)) {
            if (base_source) |existing| {
                if (!std.mem.eql(u8, existing, source)) return error.UnsupportedSqlShape;
                alloc.free(source);
            } else {
                base_source = source;
                source_transferred = true;
            }
        } else {
            alloc.free(source);
        }

        index = close_index + 1;
        if (index < tokens.len and tokens[index].kind == .comma) {
            index += 1;
            continue;
        }
        break;
    }

    var final = (try insertSourceTableNamesFromInsertAlloc(alloc, tokens, index)) orelse {
        if (base_source) |source| alloc.free(source);
        base_source = null;
        return null;
    };
    errdefer final.deinit(alloc);
    if (sqlStringSliceContains(cte_names.items, final.source)) {
        const resolved_source = base_source orelse return error.UnsupportedSqlShape;
        const target = final.target;
        alloc.free(@constCast(final.source));
        base_source = null;
        return .{
            .target = target,
            .source = resolved_source,
        };
    }
    if (base_source) |source| alloc.free(source);
    base_source = null;
    return final;
}

fn resolveSelectReadTablesAgainstCtes(
    alloc: std.mem.Allocator,
    bindings: []const CteSourceBinding,
    tables: *SelectReadTableNames,
) !void {
    tables.left = try resolveTableNameAgainstCtesAlloc(alloc, bindings, tables.left);
    if (tables.source) |source| {
        tables.source = try resolveTableNameAgainstCtesAlloc(alloc, bindings, source);
    }
}

fn resolveTableNameAgainstCtesAlloc(
    alloc: std.mem.Allocator,
    bindings: []const CteSourceBinding,
    owned_name: []const u8,
) ![]const u8 {
    const binding_index = cteBindingIndex(bindings, owned_name) orelse return owned_name;
    const resolved = try alloc.dupe(u8, bindings[binding_index].source);
    alloc.free(@constCast(owned_name));
    return resolved;
}

fn cteBindingIndex(bindings: []const CteSourceBinding, name: []const u8) ?usize {
    for (bindings, 0..) |binding, index| {
        if (std.mem.eql(u8, binding.name, name)) return index;
    }
    return null;
}

fn consumeKeyword(tokens: []const Token, index: *usize, keyword: TokenKeyword) bool {
    return parser.matchKeywordTag(tokens, index, keyword);
}

fn findTopLevelKeyword(tokens: []const Token, keyword: TokenKeyword) ?usize {
    return parser.findTopLevelKeywordTag(tokens, keyword);
}

fn findMatchingRParenIndex(tokens: []const Token, lparen_index: usize) ?usize {
    return parser.findMatchingRParenIndex(tokens, lparen_index);
}

fn sqlStringSliceContains(items: []const []const u8, needle: []const u8) bool {
    for (items) |item| {
        if (std.mem.eql(u8, item, needle)) return true;
    }
    return false;
}

test "sql adapter binder resolves runtime schema from catalog table name" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    const tenant_schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"tenant_id":{"type":"keyword"}},"required":["tenant_id"],"additionalProperties":false}}},"primary_key":{"columns":["tenant_id"]}}
    ;
    var catalog = TestCatalog.init("usage_records", schema_json, tenant_schema_json);
    const runtime = try runtimeSchemaForCatalogTableAlloc(alloc, catalog.iface(), "usage_records");
    defer runtime_schema.freeSchema(alloc, runtime);
    try std.testing.expectEqual(runtime_schema.StorageMode.relational, runtime.storage_mode);
    try std.testing.expect(runtime.primary_key != null);
    try std.testing.expectEqual(@as(usize, 2), runtime.relational_columns.len);

    const tenant_runtime = try runtimeSchemaForQualifiedCatalogTableAlloc(alloc, catalog.iface(), "tenant_ops", "analytics", "usage_records");
    defer runtime_schema.freeSchema(alloc, tenant_runtime);
    try std.testing.expectEqual(runtime_schema.StorageMode.relational, tenant_runtime.storage_mode);
    try std.testing.expectEqual(@as(usize, 1), tenant_runtime.relational_columns.len);
    try std.testing.expectError(error.InvalidSqlCatalog, runtimeSchemaForCatalogTableAlloc(alloc, catalog.iface(), "missing_records"));
}

test "sql adapter binder resolves read source tables through non recursive ctes" {
    const alloc = std.testing.allocator;

    var single_table_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT id FROM usage_records WHERE status = 'open'",
    );
    defer single_table_sql.deinit(alloc);
    var single_table = (try readSourceTableNamesFromParsedSqlAlloc(alloc, &single_table_sql)).?;
    defer single_table.deinit(alloc);
    try std.testing.expectEqualStrings("usage_records", single_table.left);
    try std.testing.expectEqualStrings("usage_records", single_table.source);

    var joined_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "WITH open_orders AS (SELECT id, tenant, customer_id FROM usage_records), active_customers AS (SELECT id, tenant, name FROM customer_records) SELECT o.id, c.name FROM open_orders AS o LEFT JOIN active_customers AS c ON o.tenant = c.tenant",
    );
    defer joined_sql.deinit(alloc);
    var joined = (try readSourceTableNamesFromParsedSqlAlloc(alloc, &joined_sql)).?;
    defer joined.deinit(alloc);
    try std.testing.expectEqualStrings("usage_records", joined.left);
    try std.testing.expectEqualStrings("customer_records", joined.source);

    var lateral_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "WITH orgs AS (SELECT id FROM usage_records), balances AS (SELECT organization_id, amount FROM balance_records) SELECT org.id, latest.amount FROM orgs AS org LEFT JOIN LATERAL (SELECT amount FROM balances AS bal WHERE bal.organization_id = org.id LIMIT 1) AS latest ON true",
    );
    defer lateral_sql.deinit(alloc);
    var lateral = (try readSourceTableNamesFromParsedSqlAlloc(alloc, &lateral_sql)).?;
    defer lateral.deinit(alloc);
    try std.testing.expectEqualStrings("usage_records", lateral.left);
    try std.testing.expectEqualStrings("balance_records", lateral.source);

    var recursive_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "WITH RECURSIVE source_rows AS (SELECT id FROM usage_records UNION ALL SELECT child.id FROM usage_records AS child JOIN source_rows AS parent ON child.organization_id = parent.id) SELECT id FROM source_rows",
    );
    defer recursive_sql.deinit(alloc);
    var recursive = (try readSourceTableNamesFromParsedSqlAlloc(alloc, &recursive_sql)).?;
    defer recursive.deinit(alloc);
    try std.testing.expectEqualStrings("usage_records", recursive.left);
    try std.testing.expectEqualStrings("usage_records", recursive.source);
}

test "sql adapter binder resolves catalog prebind table names from shared tokens" {
    const alloc = std.testing.allocator;

    var read_tokens = try lexer.tokenizeAlloc(
        alloc,
        "WITH open_orders AS (SELECT id, tenant, customer_id FROM usage_records), active_customers AS (SELECT id, tenant, name FROM customer_records) SELECT o.id, c.name FROM open_orders AS o LEFT JOIN active_customers AS c ON o.tenant = c.tenant",
    );
    defer lexer.freeTokens(alloc, &read_tokens);
    var read = (try readSourceTableNamesFromTokensAlloc(alloc, read_tokens.items)).?;
    defer read.deinit(alloc);
    try std.testing.expectEqualStrings("usage_records", read.left);
    try std.testing.expectEqualStrings("customer_records", read.source);

    var insert_tokens = try lexer.tokenizeAlloc(
        alloc,
        "WITH source_rows AS (SELECT id, status FROM incoming_usage) INSERT INTO usage_records (id, status) SELECT id, status FROM source_rows",
    );
    defer lexer.freeTokens(alloc, &insert_tokens);
    var insert_source = (try insertSourceTableNamesFromTokensAlloc(alloc, insert_tokens.items)).?;
    defer insert_source.deinit(alloc);
    try std.testing.expectEqualStrings("usage_records", insert_source.target);
    try std.testing.expectEqualStrings("incoming_usage", insert_source.source);

    var update_tokens = try lexer.tokenizeAlloc(
        alloc,
        "UPDATE usage_records SET status = source.status FROM incoming_usage AS source WHERE source.id = usage_records.id",
    );
    defer lexer.freeTokens(alloc, &update_tokens);
    var joined_write = (try joinedWriteSourceTableNamesFromTokensAlloc(alloc, update_tokens.items)).?;
    defer joined_write.deinit(alloc);
    try std.testing.expectEqualStrings("usage_records", joined_write.target);
    try std.testing.expectEqualStrings("incoming_usage", joined_write.source);
}

test "sql adapter binder resolves write target tables from parsed statements" {
    const alloc = std.testing.allocator;

    const cases = [_]struct {
        sql: []const u8,
        target: []const u8,
    }{
        .{ .sql = "INSERT INTO usage_records (id) VALUES ('u1')", .target = "usage_records" },
        .{ .sql = "WITH source_rows AS (SELECT id FROM incoming_usage) INSERT INTO public.usage_records (id) SELECT id FROM source_rows", .target = "usage_records" },
        .{ .sql = "UPDATE ONLY usage_records SET status = 'done' WHERE id = 'u1'", .target = "usage_records" },
        .{ .sql = "DELETE FROM usage_records WHERE id = 'u1'", .target = "usage_records" },
        .{ .sql = "TRUNCATE TABLE ONLY public.usage_records", .target = "usage_records" },
        .{ .sql = "MERGE INTO public.usage_records USING incoming_usage ON usage_records.id = incoming_usage.id WHEN MATCHED THEN UPDATE SET status = incoming_usage.status", .target = "usage_records" },
    };

    for (cases) |case| {
        var parsed_sql = try tokenized.ParsedSql.initAlloc(alloc, case.sql);
        defer parsed_sql.deinit(alloc);
        const target = try writeTargetTableNameFromParsedSqlAlloc(alloc, &parsed_sql);
        defer alloc.free(target);
        try std.testing.expectEqualStrings(case.target, target);
    }

    var read_sql = try tokenized.ParsedSql.initAlloc(alloc, "SELECT id FROM usage_records");
    defer read_sql.deinit(alloc);
    try std.testing.expectError(error.UnsupportedSqlShape, writeTargetTableNameFromParsedSqlAlloc(alloc, &read_sql));
}

test "sql adapter binder source table helpers validate parsed statement family" {
    const alloc = std.testing.allocator;

    var read = try tokenized.ParsedSql.initAlloc(alloc, "SELECT id FROM usage_records");
    defer read.deinit(alloc);
    try std.testing.expectError(error.UnsupportedSqlShape, insertSourceTableNamesFromParsedSqlAlloc(alloc, &read));
    try std.testing.expectError(error.UnsupportedSqlShape, joinedWriteSourceTableNamesFromParsedSqlAlloc(alloc, &read));

    var point_insert = try tokenized.ParsedSql.initAlloc(alloc, "INSERT INTO usage_records (id) VALUES ('u1')");
    defer point_insert.deinit(alloc);
    try std.testing.expect((try insertSourceTableNamesFromParsedSqlAlloc(alloc, &point_insert)) == null);
    try std.testing.expect((try recursiveInsertSourceTableNamesFromParsedSqlAlloc(alloc, &point_insert)) == null);
    try std.testing.expectError(error.UnsupportedSqlShape, readSourceTableNamesFromParsedSqlAlloc(alloc, &point_insert));

    var insert_source = try tokenized.ParsedSql.initAlloc(alloc, "INSERT INTO usage_records (id) SELECT id FROM incoming_usage");
    defer insert_source.deinit(alloc);
    var insert_tables = (try insertSourceTableNamesFromParsedSqlAlloc(alloc, &insert_source)).?;
    defer insert_tables.deinit(alloc);
    try std.testing.expectEqualStrings("usage_records", insert_tables.target);
    try std.testing.expectEqualStrings("incoming_usage", insert_tables.source);
    try std.testing.expect((try recursiveInsertSourceTableNamesFromParsedSqlAlloc(alloc, &insert_source)) == null);

    var recursive_insert = try tokenized.ParsedSql.initAlloc(alloc, "WITH RECURSIVE source_rows AS (SELECT id FROM incoming_usage) INSERT INTO usage_records (id) SELECT id FROM source_rows");
    defer recursive_insert.deinit(alloc);
    try std.testing.expect((try insertSourceTableNamesFromParsedSqlAlloc(alloc, &recursive_insert)) == null);
    var recursive_tables = (try recursiveInsertSourceTableNamesFromParsedSqlAlloc(alloc, &recursive_insert)).?;
    defer recursive_tables.deinit(alloc);
    try std.testing.expectEqualStrings("usage_records", recursive_tables.target);
    try std.testing.expectEqualStrings("incoming_usage", recursive_tables.source);
}

const MultiTableTestCatalog = struct {
    tables: [2]metadata_table_manager.TableRecord,

    fn init(
        left_table_name: []const u8,
        left_schema_json: []const u8,
        source_table_name: []const u8,
        source_schema_json: []const u8,
    ) @This() {
        return initInNamespace(
            metadata_table_manager.default_database_name,
            metadata_table_manager.default_namespace_name,
            left_table_name,
            left_schema_json,
            source_table_name,
            source_schema_json,
        );
    }

    fn initInNamespace(
        database_name: []const u8,
        namespace_name: []const u8,
        left_table_name: []const u8,
        left_schema_json: []const u8,
        source_table_name: []const u8,
        source_schema_json: []const u8,
    ) @This() {
        return .{ .tables = .{
            .{
                .table_id = 1,
                .name = left_table_name,
                .database_name = database_name,
                .namespace_name = namespace_name,
                .placement_role = "data",
                .schema_json = left_schema_json,
            },
            .{
                .table_id = 2,
                .name = source_table_name,
                .database_name = database_name,
                .namespace_name = namespace_name,
                .placement_role = "data",
                .schema_json = source_schema_json,
            },
        } };
    }

    fn iface(self: *@This()) table_catalog.CatalogSource {
        return .{
            .ptr = self,
            .vtable = &.{
                .admin_snapshot = adminSnapshot,
                .free_admin_snapshot = freeAdminSnapshot,
            },
        };
    }

    fn adminSnapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return .{
            .status = .{ .metadata_group_id = 1, .metrics = .{} },
            .tables = self.tables[0..],
            .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{})[0..]),
            .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
            .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
            .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
            .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
        };
    }

    fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
};

test "sql adapter binder produces bound sql statements for catalog read and write" {
    const alloc = std.testing.allocator;
    const usage_schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    const incoming_schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"},"source":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    const usage_schema_generation = metadata_table_manager.schemaRewriteGenerationForSchemaJson(usage_schema_json);
    const incoming_schema_generation = metadata_table_manager.schemaRewriteGenerationForSchemaJson(incoming_schema_json);
    var catalog = MultiTableTestCatalog.init("usage_records", usage_schema_json, "incoming_usage", incoming_schema_json);
    catalog.tables[0].indexes_json = "{\"usage_status_idx\":{}}";
    catalog.tables[1].indexes_json = "{\"incoming_status_idx\":{}}";

    var parsed_read = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT usage_records.id, incoming_usage.status FROM usage_records JOIN incoming_usage ON usage_records.id = incoming_usage.id",
    );
    defer parsed_read.deinit(alloc);
    var bound_read = try bindReadPlanCatalogStatementAlloc(alloc, &parsed_read, catalog.iface());
    defer bound_read.deinit(alloc);
    switch (bound_read.statement) {
        .read => |statement| try std.testing.expectEqual(sql_statement_kind.SqlReadStatementKind.join, statement.kind),
        else => return error.TestUnexpectedResult,
    }
    const read = try bound_read.readCatalog();
    try std.testing.expect(read.target_binding != null);
    switch (read.target_binding.?) {
        .relational => |binding| {
            try std.testing.expectEqualStrings("usage_records", binding.target.table_name);
            try std.testing.expectEqual(runtime_schema.StorageMode.relational, binding.schema.storage_mode);
            try std.testing.expectEqual(@as(u64, 1), binding.table_id);
            try std.testing.expectEqual(usage_schema_generation, binding.schema_generation);
        },
        else => return error.TestUnexpectedResult,
    }
    try std.testing.expect(read.source_schema != null);
    try std.testing.expectEqual(@as(usize, 3), read.source_schema.?.relational_columns.len);
    try std.testing.expect(read.source_binding != null);
    try std.testing.expectEqual(@as(usize, 2), read.bound_objects.len);
    try std.testing.expectEqual(BoundCatalogObjectRole.target, read.bound_objects[0].role);
    try std.testing.expectEqual(source_binding.SqlSourceFamily.relational, read.bound_objects[0].family);
    try std.testing.expectEqualStrings("usage_records", read.bound_objects[0].target.table_name);
    try std.testing.expectEqual(@as(u32, 1), read.bound_objects[0].schema_version);
    try std.testing.expectEqual(@as(u64, 1), read.bound_objects[0].table_id);
    try std.testing.expectEqual(usage_schema_generation, read.bound_objects[0].schema_generation);
    try std.testing.expectEqual(BoundCatalogObjectRole.source, read.bound_objects[1].role);
    try std.testing.expectEqualStrings("incoming_usage", read.bound_objects[1].target.table_name);
    try std.testing.expectEqual(@as(u32, 1), read.bound_objects[1].schema_version);
    try std.testing.expectEqual(@as(u64, 2), read.bound_objects[1].table_id);
    try std.testing.expectEqual(incoming_schema_generation, read.bound_objects[1].schema_generation);
    var logical_read = try logicalReadPlanFromBoundStatement(&bound_read);
    defer logical_read.deinit(alloc);
    switch (logical_read) {
        .catalog_read => |logical| {
            switch (logical.statement) {
                .read => |statement| try std.testing.expectEqual(sql_statement_kind.SqlReadStatementKind.join, statement.kind),
                else => return error.TestUnexpectedResult,
            }
            try std.testing.expect(logical.target_binding != null);
            try std.testing.expect(logical.source_schema != null);
            try std.testing.expectEqual(@as(usize, 3), logical.source_schema.?.relational_columns.len);
            try std.testing.expect(logical.source_binding != null);
            try std.testing.expectEqual(@as(usize, 2), logical.bound_objects.len);
            try std.testing.expectEqual(BoundCatalogObjectRole.target, logical.bound_objects[0].role);
            try std.testing.expectEqual(BoundCatalogObjectRole.source, logical.bound_objects[1].role);
            try std.testing.expectEqual(@as(u64, 1), logical.bound_objects[0].table_id);
            try std.testing.expectEqual(@as(u64, 2), logical.bound_objects[1].table_id);
            try std.testing.expectEqual(usage_schema_generation, logical.bound_objects[0].schema_generation);
            try std.testing.expectEqual(incoming_schema_generation, logical.bound_objects[1].schema_generation);
        },
        else => return error.TestUnexpectedResult,
    }
    try std.testing.expect((try bound_read.readCatalog()).target_binding == null);
    try std.testing.expect((try bound_read.readCatalog()).source_schema == null);
    try std.testing.expect((try bound_read.readCatalog()).source_binding == null);
    try std.testing.expectEqual(@as(usize, 0), (try bound_read.readCatalog()).bound_objects.len);

    const document_schema_json =
        \\{"version":1,"default_type":"doc","document_schemas":{"doc":{"schema":{"type":"object","properties":{"title":{"type":"text"}},"additionalProperties":true}}}}
    ;
    const document_schema_generation = metadata_table_manager.schemaRewriteGenerationForSchemaJson(document_schema_json);
    var document_catalog = MultiTableTestCatalog.init("docs", document_schema_json, "incoming_usage", incoming_schema_json);
    var parsed_document_read = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT _id, title FROM docs WHERE _id = 'doc:a'",
    );
    defer parsed_document_read.deinit(alloc);
    var bound_document_read = try bindReadPlanCatalogStatementAlloc(alloc, &parsed_document_read, document_catalog.iface());
    defer bound_document_read.deinit(alloc);
    const document_read = try bound_document_read.readCatalog();
    try std.testing.expect(document_read.target_binding != null);
    try std.testing.expectEqual(@as(usize, 1), document_read.bound_objects.len);
    switch (document_read.target_binding.?) {
        .document => |binding| {
            try std.testing.expectEqualStrings("docs", binding.target.table_name);
            try std.testing.expectEqual(runtime_schema.StorageMode.document, binding.schema.storage_mode);
            try std.testing.expectEqual(@as(u64, 1), binding.table_id);
            try std.testing.expectEqual(document_schema_generation, binding.schema_generation);
            try std.testing.expectEqual(source_binding.SqlSourceFamily.document, document_read.bound_objects[0].family);
            try std.testing.expectEqual(@as(u64, 1), document_read.bound_objects[0].table_id);
            try std.testing.expectEqual(document_schema_generation, document_read.bound_objects[0].schema_generation);
        },
        else => return error.TestUnexpectedResult,
    }
    try std.testing.expect(document_read.source_schema == null);

    var parsed_write = try tokenized.ParsedSql.initAlloc(
        alloc,
        "INSERT INTO usage_records (id, status) SELECT id, status FROM incoming_usage",
    );
    defer parsed_write.deinit(alloc);
    var bound_write = try bindWritePlanCatalogStatementAlloc(alloc, &parsed_write, .{}, catalog.iface());
    defer bound_write.deinit(alloc);
    switch (bound_write.statement) {
        .write => |statement| try std.testing.expectEqual(sql_statement_kind.SqlWriteStatementKind.insert, statement.kind),
        else => return error.TestUnexpectedResult,
    }
    const write = try bound_write.writeCatalog();
    try std.testing.expect(write.options.insert_source_schema != null);
    try std.testing.expectEqual(@as(usize, 3), write.options.insert_source_schema.?.relational_columns.len);
    try std.testing.expectEqual(@as(usize, 2), write.bound_objects.len);
    try std.testing.expectEqual(BoundCatalogObjectRole.target, write.bound_objects[0].role);
    try std.testing.expectEqualStrings("usage_records", write.bound_objects[0].target.table_name);
    try std.testing.expectEqual(@as(u64, 1), write.bound_objects[0].table_id);
    try std.testing.expectEqual(usage_schema_generation, write.bound_objects[0].schema_generation);
    try std.testing.expectEqual(BoundCatalogObjectRole.insert_source, write.bound_objects[1].role);
    try std.testing.expectEqualStrings("incoming_usage", write.bound_objects[1].target.table_name);
    try std.testing.expectEqual(@as(u64, 2), write.bound_objects[1].table_id);
    try std.testing.expectEqual(incoming_schema_generation, write.bound_objects[1].schema_generation);
    var logical_write = try logicalWritePlanFromBoundStatement(&bound_write);
    defer logical_write.deinit(alloc);
    switch (logical_write) {
        .catalog_write => |logical| {
            switch (logical.statement) {
                .write => |statement| try std.testing.expectEqual(sql_statement_kind.SqlWriteStatementKind.insert, statement.kind),
                else => return error.TestUnexpectedResult,
            }
            try std.testing.expect(logical.options.insert_source_schema != null);
            try std.testing.expectEqual(@as(usize, 3), logical.options.insert_source_schema.?.relational_columns.len);
            try std.testing.expectEqual(@as(usize, 2), logical.bound_objects.len);
            try std.testing.expectEqual(BoundCatalogObjectRole.insert_source, logical.bound_objects[1].role);
            try std.testing.expectEqual(@as(u64, 1), logical.bound_objects[0].table_id);
            try std.testing.expectEqual(@as(u64, 2), logical.bound_objects[1].table_id);
            try std.testing.expectEqual(usage_schema_generation, logical.bound_objects[0].schema_generation);
            try std.testing.expectEqual(incoming_schema_generation, logical.bound_objects[1].schema_generation);
        },
        else => return error.TestUnexpectedResult,
    }
    try std.testing.expect((try bound_write.writeCatalog()).owned_insert_source_schema == null);
    try std.testing.expectEqual(@as(usize, 0), (try bound_write.writeCatalog()).bound_objects.len);

    var parsed_alter = try tokenized.ParsedSql.initAlloc(
        alloc,
        "ALTER TABLE usage_records ADD COLUMN source text",
    );
    defer parsed_alter.deinit(alloc);
    var bound_alter = try bindDdlStatementWithCatalogAlloc(alloc, &parsed_alter, catalog.iface());
    defer bound_alter.deinit(alloc);
    const alter_ddl = try bound_alter.ddlCatalog();
    try std.testing.expectEqual(@as(usize, 1), alter_ddl.bound_objects.len);
    try std.testing.expectEqual(BoundCatalogObjectRole.target, alter_ddl.bound_objects[0].role);
    try std.testing.expectEqualStrings("usage_records", alter_ddl.bound_objects[0].target.table_name);
    try std.testing.expectEqual(@as(u64, 1), alter_ddl.bound_objects[0].table_id);
    try std.testing.expectEqual(usage_schema_generation, alter_ddl.bound_objects[0].schema_generation);
    try std.testing.expect(alter_ddl.logical_plan != null);
    var alter_logical = try bound_alter.takeDdlLogicalPlan();
    defer alter_logical.deinit(alloc);
    try std.testing.expectEqualStrings("table_ddl", alter_logical.statementKindName());
    try std.testing.expect((try bound_alter.ddlCatalog()).logical_plan == null);

    var parsed_create_index = try tokenized.ParsedSql.initAlloc(
        alloc,
        "CREATE INDEX usage_status_idx ON usage_records (status)",
    );
    defer parsed_create_index.deinit(alloc);
    var bound_create_index = try bindDdlStatementWithCatalogAlloc(alloc, &parsed_create_index, catalog.iface());
    defer bound_create_index.deinit(alloc);
    const create_index_ddl = try bound_create_index.ddlCatalog();
    try std.testing.expectEqual(@as(usize, 1), create_index_ddl.bound_objects.len);
    try std.testing.expectEqual(BoundCatalogObjectRole.target, create_index_ddl.bound_objects[0].role);
    try std.testing.expectEqualStrings("usage_records", create_index_ddl.bound_objects[0].target.table_name);
    try std.testing.expectEqual(@as(u64, 1), create_index_ddl.bound_objects[0].table_id);
    try std.testing.expectEqual(usage_schema_generation, create_index_ddl.bound_objects[0].schema_generation);

    var parsed_drop_index = try tokenized.ParsedSql.initAlloc(
        alloc,
        "DROP INDEX usage_status_idx",
    );
    defer parsed_drop_index.deinit(alloc);
    var bound_drop_index = try bindDdlStatementWithCatalogAlloc(alloc, &parsed_drop_index, catalog.iface());
    defer bound_drop_index.deinit(alloc);
    const drop_index_ddl = try bound_drop_index.ddlCatalog();
    try std.testing.expectEqual(@as(usize, 1), drop_index_ddl.bound_objects.len);
    try std.testing.expectEqual(BoundCatalogObjectRole.target, drop_index_ddl.bound_objects[0].role);
    try std.testing.expectEqualStrings("usage_records", drop_index_ddl.bound_objects[0].target.table_name);
    try std.testing.expectEqual(@as(u64, 1), drop_index_ddl.bound_objects[0].table_id);
    try std.testing.expectEqual(usage_schema_generation, drop_index_ddl.bound_objects[0].schema_generation);

    var parsed_missing_drop_index = try tokenized.ParsedSql.initAlloc(
        alloc,
        "DROP INDEX missing_usage_status_idx",
    );
    defer parsed_missing_drop_index.deinit(alloc);
    try std.testing.expectError(error.InvalidSqlCatalog, bindDdlStatementWithCatalogAlloc(alloc, &parsed_missing_drop_index, catalog.iface()));

    var parsed_missing_drop_index_if_exists = try tokenized.ParsedSql.initAlloc(
        alloc,
        "DROP INDEX IF EXISTS missing_usage_status_idx",
    );
    defer parsed_missing_drop_index_if_exists.deinit(alloc);
    var bound_missing_drop_index_if_exists = try bindDdlStatementWithCatalogAlloc(alloc, &parsed_missing_drop_index_if_exists, catalog.iface());
    defer bound_missing_drop_index_if_exists.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 0), (try bound_missing_drop_index_if_exists.ddlCatalog()).bound_objects.len);

    const maintenance_cases = [_][]const u8{
        "VACUUM usage_records",
        "ANALYZE usage_records",
        "CLUSTER usage_records USING usage_status_idx",
        "REINDEX TABLE usage_records",
        "REINDEX INDEX usage_status_idx",
    };
    for (maintenance_cases) |maintenance_sql| {
        var parsed_maintenance = try tokenized.ParsedSql.initAlloc(alloc, maintenance_sql);
        defer parsed_maintenance.deinit(alloc);
        var bound_maintenance = try bindDdlStatementWithCatalogAlloc(alloc, &parsed_maintenance, catalog.iface());
        defer bound_maintenance.deinit(alloc);
        const maintenance_ddl = try bound_maintenance.ddlCatalog();
        try std.testing.expectEqual(@as(usize, 1), maintenance_ddl.bound_objects.len);
        try std.testing.expectEqual(BoundCatalogObjectRole.target, maintenance_ddl.bound_objects[0].role);
        try std.testing.expectEqualStrings("usage_records", maintenance_ddl.bound_objects[0].target.table_name);
        try std.testing.expectEqual(@as(u64, 1), maintenance_ddl.bound_objects[0].table_id);
        try std.testing.expectEqual(usage_schema_generation, maintenance_ddl.bound_objects[0].schema_generation);
    }

    const reindex_schema_sql = try std.fmt.allocPrint(alloc, "REINDEX SCHEMA {s}", .{metadata_table_manager.default_namespace_name});
    defer alloc.free(reindex_schema_sql);
    var parsed_reindex_schema = try tokenized.ParsedSql.initAlloc(
        alloc,
        reindex_schema_sql,
    );
    defer parsed_reindex_schema.deinit(alloc);
    var bound_reindex_schema = try bindDdlStatementWithCatalogAlloc(alloc, &parsed_reindex_schema, catalog.iface());
    defer bound_reindex_schema.deinit(alloc);
    const reindex_schema_ddl = try bound_reindex_schema.ddlCatalog();
    try std.testing.expectEqual(@as(usize, 2), reindex_schema_ddl.bound_objects.len);
    try std.testing.expectEqual(BoundCatalogObjectRole.target, reindex_schema_ddl.bound_objects[0].role);
    try std.testing.expectEqualStrings("usage_records", reindex_schema_ddl.bound_objects[0].target.table_name);
    try std.testing.expectEqual(@as(u64, 1), reindex_schema_ddl.bound_objects[0].table_id);
    try std.testing.expectEqual(usage_schema_generation, reindex_schema_ddl.bound_objects[0].schema_generation);
    try std.testing.expectEqual(BoundCatalogObjectRole.target, reindex_schema_ddl.bound_objects[1].role);
    try std.testing.expectEqualStrings("incoming_usage", reindex_schema_ddl.bound_objects[1].target.table_name);
    try std.testing.expectEqual(@as(u64, 2), reindex_schema_ddl.bound_objects[1].table_id);
    try std.testing.expectEqual(incoming_schema_generation, reindex_schema_ddl.bound_objects[1].schema_generation);

    const reindex_database_sql = try std.fmt.allocPrint(alloc, "REINDEX DATABASE {s}", .{metadata_table_manager.default_database_name});
    defer alloc.free(reindex_database_sql);
    var parsed_reindex_database = try tokenized.ParsedSql.initAlloc(
        alloc,
        reindex_database_sql,
    );
    defer parsed_reindex_database.deinit(alloc);
    var bound_reindex_database = try bindDdlStatementWithCatalogAlloc(alloc, &parsed_reindex_database, catalog.iface());
    defer bound_reindex_database.deinit(alloc);
    const reindex_database_ddl = try bound_reindex_database.ddlCatalog();
    try std.testing.expectEqual(@as(usize, 2), reindex_database_ddl.bound_objects.len);
    try std.testing.expectEqual(BoundCatalogObjectRole.target, reindex_database_ddl.bound_objects[0].role);
    try std.testing.expectEqualStrings("usage_records", reindex_database_ddl.bound_objects[0].target.table_name);
    try std.testing.expectEqual(@as(u64, 1), reindex_database_ddl.bound_objects[0].table_id);
    try std.testing.expectEqual(usage_schema_generation, reindex_database_ddl.bound_objects[0].schema_generation);
    try std.testing.expectEqual(BoundCatalogObjectRole.target, reindex_database_ddl.bound_objects[1].role);
    try std.testing.expectEqualStrings("incoming_usage", reindex_database_ddl.bound_objects[1].target.table_name);
    try std.testing.expectEqual(@as(u64, 2), reindex_database_ddl.bound_objects[1].table_id);
    try std.testing.expectEqual(incoming_schema_generation, reindex_database_ddl.bound_objects[1].schema_generation);

    var parsed_missing_maintenance = try tokenized.ParsedSql.initAlloc(
        alloc,
        "VACUUM missing_usage_records",
    );
    defer parsed_missing_maintenance.deinit(alloc);
    try std.testing.expectError(error.TableNotFound, bindDdlStatementWithCatalogAlloc(alloc, &parsed_missing_maintenance, catalog.iface()));

    var parsed_missing_reindex_index = try tokenized.ParsedSql.initAlloc(
        alloc,
        "REINDEX INDEX missing_usage_status_idx",
    );
    defer parsed_missing_reindex_index.deinit(alloc);
    try std.testing.expectError(error.InvalidSqlCatalog, bindDdlStatementWithCatalogAlloc(alloc, &parsed_missing_reindex_index, catalog.iface()));

    var parsed_missing_cluster_index = try tokenized.ParsedSql.initAlloc(
        alloc,
        "CLUSTER usage_records USING missing_usage_status_idx",
    );
    defer parsed_missing_cluster_index.deinit(alloc);
    try std.testing.expectError(error.InvalidSqlCatalog, bindDdlStatementWithCatalogAlloc(alloc, &parsed_missing_cluster_index, catalog.iface()));

    var parsed_wrong_table_cluster_index = try tokenized.ParsedSql.initAlloc(
        alloc,
        "CLUSTER usage_records USING incoming_status_idx",
    );
    defer parsed_wrong_table_cluster_index.deinit(alloc);
    try std.testing.expectError(error.InvalidSqlCatalog, bindDdlStatementWithCatalogAlloc(alloc, &parsed_wrong_table_cluster_index, catalog.iface()));

    var parsed_create_table = try tokenized.ParsedSql.initAlloc(
        alloc,
        "CREATE TABLE new_usage_records (id text PRIMARY KEY)",
    );
    defer parsed_create_table.deinit(alloc);
    var bound_create_table = try bindDdlStatementWithCatalogAlloc(alloc, &parsed_create_table, catalog.iface());
    defer bound_create_table.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 0), (try bound_create_table.ddlCatalog()).bound_objects.len);

    var parsed_create_fk_table = try tokenized.ParsedSql.initAlloc(
        alloc,
        "CREATE TABLE usage_events (id text PRIMARY KEY, usage_id text REFERENCES usage_records(id))",
    );
    defer parsed_create_fk_table.deinit(alloc);
    var bound_create_fk_table = try bindDdlStatementWithCatalogAlloc(alloc, &parsed_create_fk_table, catalog.iface());
    defer bound_create_fk_table.deinit(alloc);
    const create_fk_ddl = try bound_create_fk_table.ddlCatalog();
    try std.testing.expectEqual(@as(usize, 1), create_fk_ddl.bound_objects.len);
    try std.testing.expectEqual(BoundCatalogObjectRole.source, create_fk_ddl.bound_objects[0].role);
    try std.testing.expectEqualStrings("usage_records", create_fk_ddl.bound_objects[0].target.table_name);
    try std.testing.expectEqual(@as(u64, 1), create_fk_ddl.bound_objects[0].table_id);
    try std.testing.expectEqual(usage_schema_generation, create_fk_ddl.bound_objects[0].schema_generation);

    var parsed_create_self_fk_table = try tokenized.ParsedSql.initAlloc(
        alloc,
        "CREATE TABLE usage_tree (id text PRIMARY KEY, parent_id text REFERENCES usage_tree(id))",
    );
    defer parsed_create_self_fk_table.deinit(alloc);
    var bound_create_self_fk_table = try bindDdlStatementWithCatalogAlloc(alloc, &parsed_create_self_fk_table, catalog.iface());
    defer bound_create_self_fk_table.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 0), (try bound_create_self_fk_table.ddlCatalog()).bound_objects.len);

    var parsed_missing_create_fk_table = try tokenized.ParsedSql.initAlloc(
        alloc,
        "CREATE TABLE bad_usage_events (id text PRIMARY KEY, usage_id text REFERENCES missing_usage_records(id))",
    );
    defer parsed_missing_create_fk_table.deinit(alloc);
    try std.testing.expectError(error.TableNotFound, bindDdlStatementWithCatalogAlloc(alloc, &parsed_missing_create_fk_table, catalog.iface()));

    var parsed_alter_add_fk = try tokenized.ParsedSql.initAlloc(
        alloc,
        "ALTER TABLE incoming_usage ADD CONSTRAINT incoming_usage_source_fkey FOREIGN KEY (source) REFERENCES usage_records(id)",
    );
    defer parsed_alter_add_fk.deinit(alloc);
    var bound_alter_add_fk = try bindDdlStatementWithCatalogAlloc(alloc, &parsed_alter_add_fk, catalog.iface());
    defer bound_alter_add_fk.deinit(alloc);
    const alter_add_fk_ddl = try bound_alter_add_fk.ddlCatalog();
    try std.testing.expectEqual(@as(usize, 2), alter_add_fk_ddl.bound_objects.len);
    try std.testing.expectEqual(BoundCatalogObjectRole.target, alter_add_fk_ddl.bound_objects[0].role);
    try std.testing.expectEqualStrings("incoming_usage", alter_add_fk_ddl.bound_objects[0].target.table_name);
    try std.testing.expectEqual(@as(u64, 2), alter_add_fk_ddl.bound_objects[0].table_id);
    try std.testing.expectEqual(BoundCatalogObjectRole.source, alter_add_fk_ddl.bound_objects[1].role);
    try std.testing.expectEqualStrings("usage_records", alter_add_fk_ddl.bound_objects[1].target.table_name);
    try std.testing.expectEqual(@as(u64, 1), alter_add_fk_ddl.bound_objects[1].table_id);

    var parsed_alter_add_column_fk = try tokenized.ParsedSql.initAlloc(
        alloc,
        "ALTER TABLE incoming_usage ADD COLUMN usage_ref text REFERENCES usage_records(id)",
    );
    defer parsed_alter_add_column_fk.deinit(alloc);
    var bound_alter_add_column_fk = try bindDdlStatementWithCatalogAlloc(alloc, &parsed_alter_add_column_fk, catalog.iface());
    defer bound_alter_add_column_fk.deinit(alloc);
    const alter_add_column_fk_ddl = try bound_alter_add_column_fk.ddlCatalog();
    try std.testing.expectEqual(@as(usize, 2), alter_add_column_fk_ddl.bound_objects.len);
    try std.testing.expectEqualStrings("incoming_usage", alter_add_column_fk_ddl.bound_objects[0].target.table_name);
    try std.testing.expectEqualStrings("usage_records", alter_add_column_fk_ddl.bound_objects[1].target.table_name);

    var parsed_alter_add_self_fk = try tokenized.ParsedSql.initAlloc(
        alloc,
        "ALTER TABLE incoming_usage ADD CONSTRAINT incoming_usage_parent_fkey FOREIGN KEY (source) REFERENCES incoming_usage(id)",
    );
    defer parsed_alter_add_self_fk.deinit(alloc);
    var bound_alter_add_self_fk = try bindDdlStatementWithCatalogAlloc(alloc, &parsed_alter_add_self_fk, catalog.iface());
    defer bound_alter_add_self_fk.deinit(alloc);
    const alter_add_self_fk_ddl = try bound_alter_add_self_fk.ddlCatalog();
    try std.testing.expectEqual(@as(usize, 1), alter_add_self_fk_ddl.bound_objects.len);
    try std.testing.expectEqual(BoundCatalogObjectRole.target, alter_add_self_fk_ddl.bound_objects[0].role);
    try std.testing.expectEqualStrings("incoming_usage", alter_add_self_fk_ddl.bound_objects[0].target.table_name);

    try std.testing.expectError(error.UnsupportedSqlShape, bindReadPlanCatalogStatementAlloc(alloc, &parsed_write, catalog.iface()));
    try std.testing.expectError(error.UnsupportedSqlShape, bindWritePlanCatalogStatementAlloc(alloc, &parsed_read, .{}, catalog.iface()));

    const tenant_path = [_][]const u8{"analytics"};
    const tenant_session: catalog_resources.SqlCatalogSession = .{
        .current_database_name = "tenant_ops",
        .search_path = tenant_path[0..],
    };
    var tenant_catalog = MultiTableTestCatalog.initInNamespace("tenant_ops", "analytics", "usage_records", usage_schema_json, "incoming_usage", incoming_schema_json);

    var tenant_read_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT usage_records.id, incoming_usage.status FROM usage_records JOIN incoming_usage ON usage_records.id = incoming_usage.id",
    );
    defer tenant_read_sql.deinit(alloc);
    var tenant_bound_read = try bindReadPlanCatalogStatementWithSessionAlloc(alloc, &tenant_read_sql, tenant_catalog.iface(), tenant_session);
    defer tenant_bound_read.deinit(alloc);
    try std.testing.expectEqualStrings("tenant_ops", tenant_bound_read.session.current_database_name);
    try std.testing.expectEqualStrings("analytics", tenant_bound_read.session.search_path[0]);
    try std.testing.expect((try tenant_bound_read.readCatalog()).source_schema != null);

    var tenant_write_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "INSERT INTO usage_records (id, status) SELECT id, status FROM incoming_usage",
    );
    defer tenant_write_sql.deinit(alloc);
    var tenant_bound_write = try bindWritePlanCatalogStatementWithSessionAlloc(alloc, &tenant_write_sql, .{}, tenant_catalog.iface(), tenant_session);
    defer tenant_bound_write.deinit(alloc);
    try std.testing.expectEqualStrings("tenant_ops", tenant_bound_write.session.current_database_name);
    try std.testing.expectEqualStrings("analytics", tenant_bound_write.session.search_path[0]);
    try std.testing.expect((try tenant_bound_write.writeCatalog()).options.insert_source_schema != null);

    var tenant_alter_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "ALTER TABLE usage_records ADD COLUMN source text",
    );
    defer tenant_alter_sql.deinit(alloc);
    var tenant_bound_ddl = try bindDdlStatementWithCatalogSessionAlloc(alloc, &tenant_alter_sql, tenant_catalog.iface(), tenant_session);
    defer tenant_bound_ddl.deinit(alloc);
    const tenant_ddl = try tenant_bound_ddl.ddlCatalog();
    try std.testing.expectEqual(@as(usize, 1), tenant_ddl.bound_objects.len);
    try std.testing.expectEqualStrings("tenant_ops", tenant_ddl.bound_objects[0].target.database_name);
    try std.testing.expectEqualStrings("analytics", tenant_ddl.bound_objects[0].target.namespace_name);
    try std.testing.expectEqualStrings("usage_records", tenant_ddl.bound_objects[0].target.table_name);

    var parsed_grant_schema = try tokenized.ParsedSql.initAlloc(
        alloc,
        "GRANT SELECT ON ALL TABLES IN SCHEMA public TO app_writer",
    );
    defer parsed_grant_schema.deinit(alloc);
    var bound_grant_schema = try bindDdlStatementWithCatalogAlloc(alloc, &parsed_grant_schema, catalog.iface());
    defer bound_grant_schema.deinit(alloc);
    const grant_schema_ddl = try bound_grant_schema.ddlCatalog();
    try std.testing.expectEqual(@as(usize, 2), grant_schema_ddl.bound_objects.len);
    try std.testing.expectEqual(BoundCatalogObjectRole.target, grant_schema_ddl.bound_objects[0].role);
    try std.testing.expectEqualStrings("usage_records", grant_schema_ddl.bound_objects[0].target.table_name);
    try std.testing.expectEqual(@as(u64, 1), grant_schema_ddl.bound_objects[0].table_id);
    try std.testing.expectEqual(usage_schema_generation, grant_schema_ddl.bound_objects[0].schema_generation);
    try std.testing.expectEqual(BoundCatalogObjectRole.target, grant_schema_ddl.bound_objects[1].role);
    try std.testing.expectEqualStrings("incoming_usage", grant_schema_ddl.bound_objects[1].target.table_name);
    try std.testing.expectEqual(@as(u64, 2), grant_schema_ddl.bound_objects[1].table_id);
    try std.testing.expectEqual(incoming_schema_generation, grant_schema_ddl.bound_objects[1].schema_generation);

    var parsed_revoke_tenant_schema = try tokenized.ParsedSql.initAlloc(
        alloc,
        "REVOKE SELECT ON ALL TABLES IN SCHEMA analytics FROM app_writer",
    );
    defer parsed_revoke_tenant_schema.deinit(alloc);
    var bound_revoke_tenant_schema = try bindDdlStatementWithCatalogSessionAlloc(alloc, &parsed_revoke_tenant_schema, tenant_catalog.iface(), tenant_session);
    defer bound_revoke_tenant_schema.deinit(alloc);
    const revoke_tenant_schema_ddl = try bound_revoke_tenant_schema.ddlCatalog();
    try std.testing.expectEqual(@as(usize, 2), revoke_tenant_schema_ddl.bound_objects.len);
    try std.testing.expectEqualStrings("tenant_ops", revoke_tenant_schema_ddl.bound_objects[0].target.database_name);
    try std.testing.expectEqualStrings("analytics", revoke_tenant_schema_ddl.bound_objects[0].target.namespace_name);
    try std.testing.expectEqualStrings("usage_records", revoke_tenant_schema_ddl.bound_objects[0].target.table_name);
    try std.testing.expectEqualStrings("tenant_ops", revoke_tenant_schema_ddl.bound_objects[1].target.database_name);
    try std.testing.expectEqualStrings("analytics", revoke_tenant_schema_ddl.bound_objects[1].target.namespace_name);
    try std.testing.expectEqualStrings("incoming_usage", revoke_tenant_schema_ddl.bound_objects[1].target.table_name);

    var parsed_missing_grant_schema = try tokenized.ParsedSql.initAlloc(
        alloc,
        "GRANT SELECT ON ALL TABLES IN SCHEMA missing_schema TO app_writer",
    );
    defer parsed_missing_grant_schema.deinit(alloc);
    try std.testing.expectError(error.TableNotFound, bindDdlStatementWithCatalogAlloc(alloc, &parsed_missing_grant_schema, catalog.iface()));

    const read_auth_grants = [_]BoundSqlAuthorizationGrant{
        .{ .resource_kind = .table, .resource = "usage_records", .permission = .read },
        .{ .resource_kind = .table, .resource = "incoming_usage", .permission = .read },
    };
    var auth_bound_read = try bindReadPlanCatalogStatementWithSessionAndAuthorizationAlloc(alloc, &parsed_read, catalog.iface(), catalog_resources.SqlCatalogSession.default(), .{
        .principal_name = "alice",
        .grants = read_auth_grants[0..],
        .grants_evaluated = true,
    });
    defer auth_bound_read.deinit(alloc);
    const auth_read = try auth_bound_read.readCatalog();
    try std.testing.expect(auth_read.authorization.principal_name != null);
    try std.testing.expectEqualStrings("alice", auth_read.authorization.principal_name.?);
    try std.testing.expectEqual(@as(usize, 2), auth_read.authorization.checks.len);
    try std.testing.expectEqual(BoundCatalogObjectRole.target, auth_read.authorization.checks[0].object_role);
    try std.testing.expectEqual(BoundSqlAuthorizationPermission.read, auth_read.authorization.checks[0].required_permission);
    try std.testing.expectEqual(BoundSqlAuthorizationDecision.allowed, auth_read.authorization.checks[0].decision);
    try std.testing.expectEqualStrings("usage_records", auth_read.authorization.checks[0].target.table_name);
    try std.testing.expectEqual(BoundCatalogObjectRole.source, auth_read.authorization.checks[1].object_role);
    try std.testing.expectEqual(BoundSqlAuthorizationDecision.allowed, auth_read.authorization.checks[1].decision);
    var auth_logical_read = try logicalReadPlanFromBoundStatement(&auth_bound_read);
    defer auth_logical_read.deinit(alloc);
    switch (auth_logical_read) {
        .catalog_read => |logical| {
            try std.testing.expect(logical.authorization.principal_name != null);
            try std.testing.expectEqualStrings("alice", logical.authorization.principal_name.?);
            try std.testing.expectEqual(@as(usize, 2), logical.authorization.checks.len);
            try std.testing.expectEqual(BoundSqlAuthorizationDecision.allowed, logical.authorization.checks[0].decision);
        },
        else => return error.TestUnexpectedResult,
    }

    const write_auth_grants = [_]BoundSqlAuthorizationGrant{
        .{ .resource_kind = .table, .resource = "usage_records", .permission = .write },
        .{ .resource_kind = .table, .resource = "incoming_usage", .permission = .read },
    };
    var auth_bound_write = try bindWritePlanCatalogStatementWithSessionAndAuthorizationAlloc(alloc, &parsed_write, .{}, catalog.iface(), catalog_resources.SqlCatalogSession.default(), .{
        .principal_name = "writer",
        .grants = write_auth_grants[0..],
        .grants_evaluated = true,
    });
    defer auth_bound_write.deinit(alloc);
    const auth_write = try auth_bound_write.writeCatalog();
    try std.testing.expectEqual(@as(usize, 2), auth_write.authorization.checks.len);
    try std.testing.expectEqual(BoundSqlAuthorizationPermission.write, auth_write.authorization.checks[0].required_permission);
    try std.testing.expectEqual(BoundSqlAuthorizationDecision.allowed, auth_write.authorization.checks[0].decision);
    try std.testing.expectEqual(BoundSqlAuthorizationPermission.read, auth_write.authorization.checks[1].required_permission);
    try std.testing.expectEqual(BoundSqlAuthorizationDecision.allowed, auth_write.authorization.checks[1].decision);

    const denied_read_auth_grants = [_]BoundSqlAuthorizationGrant{
        .{ .resource_kind = .table, .resource = "usage_records", .permission = .read },
    };
    var denied_auth_bound_read = try bindReadPlanCatalogStatementWithSessionAndAuthorizationAlloc(alloc, &parsed_read, catalog.iface(), catalog_resources.SqlCatalogSession.default(), .{
        .principal_name = "limited_reader",
        .grants = denied_read_auth_grants[0..],
        .grants_evaluated = true,
    });
    defer denied_auth_bound_read.deinit(alloc);
    const denied_auth_read = try denied_auth_bound_read.readCatalog();
    try std.testing.expectEqual(@as(usize, 2), denied_auth_read.authorization.checks.len);
    try std.testing.expectEqual(BoundSqlAuthorizationDecision.allowed, denied_auth_read.authorization.checks[0].decision);
    try std.testing.expectEqual(BoundSqlAuthorizationDecision.denied, denied_auth_read.authorization.checks[1].decision);
    try std.testing.expectError(error.PermissionDenied, enforceBoundSqlStatementAuthorization(&denied_auth_bound_read));

    const ddl_auth_grants = [_]BoundSqlAuthorizationGrant{
        .{ .resource_kind = .database, .resource = metadata_table_manager.default_database_name, .permission = .admin },
    };
    var auth_bound_ddl = try bindDdlStatementWithCatalogSessionFunctionBindingsAndAuthorizationAlloc(alloc, &parsed_alter, catalog.iface(), catalog_resources.SqlCatalogSession.default(), .{}, .{
        .principal_name = "admin",
        .grants = ddl_auth_grants[0..],
        .grants_evaluated = true,
    });
    defer auth_bound_ddl.deinit(alloc);
    const auth_ddl = try auth_bound_ddl.ddlCatalog();
    try std.testing.expectEqual(@as(usize, 1), auth_ddl.authorization.checks.len);
    try std.testing.expectEqual(BoundSqlAuthorizationPermission.admin, auth_ddl.authorization.checks[0].required_permission);
    try std.testing.expectEqual(BoundSqlAuthorizationDecision.allowed, auth_ddl.authorization.checks[0].decision);
}

test "sql adapter binder rejects ambiguous physical cte read source tables" {
    const alloc = std.testing.allocator;
    var parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "WITH mixed AS (SELECT o.id FROM orders AS o JOIN customers AS c ON o.customer_id = c.id) SELECT mixed.id, s.id FROM mixed JOIN shipments AS s ON mixed.id = s.order_id",
    );
    defer parsed_sql.deinit(alloc);
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        readSourceTableNamesFromParsedSqlAlloc(alloc, &parsed_sql),
    );
}

test "sql adapter binder validates relational catalog lookups" {
    const columns = [_]runtime_schema.RelationalColumn{
        .{ .name = "id", .path = "id", .field_type = .keyword },
        .{ .name = "email", .path = "email", .field_type = .keyword, .index_name = "users_email_idx" },
        .{ .name = "status", .path = "status", .field_type = .keyword },
        .{ .name = "profile", .path = "profile", .field_type = .json },
    };
    const primary_key = runtime_schema.PrimaryKey{ .columns = &.{"id"} };
    const unique_expression = [_]runtime_schema.UniqueExpression{.{ .op = .lower, .field = "email" }};
    const unique_where = [_]runtime_schema.UniquePredicate{.{ .field = "status", .op = .eq, .value_json = "\"active\"" }};
    const uniques = [_]runtime_schema.UniqueConstraint{
        .{ .name = "users_email_key", .columns = &.{"email"} },
        .{ .name = "users_status_email_key", .columns = &.{ "status", "email" }, .where = &unique_where },
        .{ .name = "users_lower_email_key", .expressions = &unique_expression },
        .{ .name = "users_unvalidated_key", .columns = &.{"status"}, .validation_state = .unvalidated },
    };
    const foreign_keys = [_]runtime_schema.ForeignKey{.{ .name = "users_org_fkey", .parent_table = "orgs" }};
    const checks = [_]runtime_schema.RelationalCheck{.{ .name = "users_status_check", .field = "status" }};
    const schema = runtime_schema.TableSchema{
        .storage_mode = .relational,
        .relational_columns = &columns,
        .primary_key = primary_key,
        .unique_constraints = &uniques,
        .foreign_keys = &foreign_keys,
        .checks = &checks,
    };

    try std.testing.expect(tableSchemaCatalogExists(schema));
    try std.testing.expect(relationalColumnForDdl(&columns, "email") != null);
    try std.testing.expect(relationalColumnForReturningField(schema, "email") != null);
    try std.testing.expect(relationalColumnForReturningField(schema, "profile.city") != null);
    try std.testing.expect(relationalColumnForReturningField(schema, "email.domain") == null);
    try std.testing.expectEqual(@as(?usize, 1), relationalColumnIndexForIndexName(&columns, "users_email_idx"));
    try std.testing.expect(relationalIndexNameExists(schema, "users_email_key"));
    try std.testing.expect(relationalIndexNameExists(schema, "users_email_idx"));
    try std.testing.expect(relationalConstraintNameExists(schema, "users", "users_pkey"));
    try std.testing.expect(relationalConstraintNameExists(schema, "users", "users_status_check"));
    try std.testing.expect(!relationalConstraintNameExists(schema, "users", "missing_check"));
    try std.testing.expect(columnsMatchPrimaryKey(primary_key, &.{"id"}));
    try std.testing.expect(!columnsMatchPrimaryKey(primary_key, &.{"email"}));
    try std.testing.expect(primaryKeyContains(primary_key, "id"));
    try std.testing.expect(!primaryKeyContains(primary_key, "email"));
    try std.testing.expect(findUniqueConstraintByName(schema, "users_email_key") != null);
    try std.testing.expect(findUniqueConstraintByName(schema, "users_unvalidated_key") == null);
    try std.testing.expect(findUniqueConstraintByColumns(schema, &.{"email"}, false) != null);
    try std.testing.expect(findUniqueConstraintByColumns(schema, &.{ "status", "email" }, true) != null);
    try std.testing.expect(findUniqueConstraintByColumns(schema, &.{ "status", "email" }, false) == null);
    try std.testing.expect(findUniqueConstraintByExpression(schema, .lower, "email") != null);
    try std.testing.expect(findUniqueConstraintByExpression(schema, .upper, "email") == null);

    const period_columns = [_]runtime_schema.RelationalColumn{
        .{ .name = "id", .path = "id", .field_type = .keyword },
        .{ .name = "created_at", .path = "created_at", .field_type = .datetime },
        .{ .name = "updated_at", .path = "updated_at", .field_type = .datetime },
        .{ .name = "payload", .path = "payload", .field_type = .json },
    };
    const periods = [_]runtime_schema.RelationalPeriod{.{
        .name = "valid_at",
        .start_column = "created_at",
        .end_column = "updated_at",
        .range_type = .tstzrange,
    }};
    try validateRelationalPeriodCatalog(&period_columns, &periods);
    try std.testing.expect(relationalPeriodColumnType(.datetime));
    try std.testing.expect(relationalPeriodForDdl(&periods, "valid_at") != null);
    try std.testing.expect(relationalPeriodNameExists(&periods, "valid_at"));
    try validatePrimaryKeyTemporalCatalog(&periods, .{ .columns = &.{"id"}, .without_overlaps_period = "valid_at" });
    try validateForeignKeyForColumns(&period_columns, &periods, .{
        .name = "events_parent_fkey",
        .parent_table = "parent_events",
        .child_columns = &.{"id"},
        .parent_columns = &.{"id"},
        .child_period = "valid_at",
        .parent_period = "valid_at",
        .on_update = .set_null,
    });
    try std.testing.expect(foreignKeyActionSupportsTemporalUpdate(.restrict));
    try std.testing.expect(foreignKeyActionSupportsTemporalUpdate(.cascade));
    try std.testing.expectError(error.InvalidSqlCatalog, validateForeignKeyForColumns(&period_columns, &periods, .{
        .name = "events_payload_fkey",
        .parent_table = "parent_events",
        .child_columns = &.{"payload"},
        .parent_columns = &.{"payload"},
    }));
}

test "sql adapter binder resolves join projection bindings" {
    const alloc = std.testing.allocator;
    const left_columns = [_]runtime_schema.RelationalColumn{
        .{ .name = "id", .path = "id", .field_type = .keyword },
        .{ .name = "tenant", .path = "tenant", .field_type = .keyword },
    };
    const right_columns = [_]runtime_schema.RelationalColumn{
        .{ .name = "id", .path = "id", .field_type = .keyword },
        .{ .name = "status", .path = "status", .field_type = .keyword },
        .{ .name = "amount", .path = "amount", .field_type = .numeric },
    };
    const left_schema = runtime_schema.TableSchema{
        .storage_mode = .relational,
        .relational_columns = &left_columns,
    };
    const right_schema = runtime_schema.TableSchema{
        .storage_mode = .relational,
        .relational_columns = &right_columns,
    };

    try std.testing.expectEqual(db_mod.types.RelationalRowsJoinProjectionSide.left, try joinSideForQualifier("u", "u", "c"));
    try std.testing.expectError(error.UnsupportedSqlShape, joinSideForQualifier("x", "u", "c"));
    try std.testing.expectEqualStrings("tenant", (try joinColumnForSide(left_schema, right_schema, .left, "tenant")).name);
    try std.testing.expectEqualStrings("status", (try joinColumnForSide(left_schema, right_schema, .right, "status")).name);
    try std.testing.expectError(error.InvalidSqlCatalog, joinColumnForSide(left_schema, right_schema, .right, "tenant"));
    const resolved_source = try resolveRowExpressionFieldAlloc(
        alloc,
        left_schema,
        right_schema,
        "source.status",
        &.{},
        &.{},
        &.{"source"},
        &.{"target"},
        false,
        .row,
    );
    defer alloc.free(resolved_source.field);
    try std.testing.expectEqualStrings("status", resolved_source.field);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionFieldSource.source, resolved_source.source);
    try std.testing.expectEqualStrings("status", resolved_source.schema.relational_columns[1].name);
    const resolved_target = try resolveRowExpressionFieldAlloc(
        alloc,
        left_schema,
        right_schema,
        "target.tenant",
        &.{},
        &.{},
        &.{"source"},
        &.{"target"},
        false,
        .source,
    );
    defer alloc.free(resolved_target.field);
    try std.testing.expectEqualStrings("tenant", resolved_target.field);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionFieldSource.row, resolved_target.source);
    const resolved_default_source = try resolveRowExpressionFieldAlloc(
        alloc,
        left_schema,
        right_schema,
        "tenant",
        &.{},
        &.{},
        &.{},
        &.{},
        false,
        .source,
    );
    defer alloc.free(resolved_default_source.field);
    try std.testing.expectEqualStrings("tenant", resolved_default_source.field);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionFieldSource.source, resolved_default_source.source);
    try std.testing.expectEqualStrings("status", resolved_default_source.schema.relational_columns[1].name);
    const source_returning = (try normalizeJoinedMutationReturningSourceFieldAlloc(alloc, left_schema, right_schema, "source.status", "source")).?;
    defer alloc.free(source_returning);
    try std.testing.expectEqualStrings("status", source_returning);
    try std.testing.expect((try normalizeJoinedMutationReturningSourceFieldAlloc(alloc, left_schema, right_schema, "target.tenant", "source")) == null);
    try std.testing.expectError(error.InvalidSqlCatalog, normalizeJoinedMutationReturningSourceFieldAlloc(alloc, left_schema, right_schema, "source.tenant", "source"));
    try std.testing.expectEqualStrings("tenant", (try joinedMutationColumnForQualifier(left_schema, right_schema, "target", "tenant", "target", "source")).name);
    try std.testing.expectEqualStrings("status", (try joinedMutationColumnForQualifier(left_schema, right_schema, "source", "status", "target", "source")).name);
    try std.testing.expectError(error.UnsupportedSqlShape, joinedMutationColumnForQualifier(left_schema, right_schema, "other", "status", "target", "source"));
    try std.testing.expect(try joinedMutationTargetFieldMatches(alloc, "tenant", "target", "tenant"));
    try std.testing.expect(try joinedMutationTargetFieldMatches(alloc, "target.tenant", "target", "tenant"));
    try std.testing.expect(try joinedMutationTargetFieldMatches(alloc, "public.target.tenant", "target", "tenant"));
    try std.testing.expect(!try joinedMutationTargetFieldMatches(alloc, "source.tenant", "target", "tenant"));
    var returning_all = try lexer.tokenizeAlloc(alloc, "target.*");
    defer lexer.freeTokens(alloc, &returning_all);
    var returning_all_pos: usize = 0;
    try std.testing.expect(try matchQualifiedReturningAll(alloc, returning_all.items, &returning_all_pos, &.{ "target", "public.target" }));
    try std.testing.expectEqual(returning_all.items.len, returning_all_pos);
    var source_all = try lexer.tokenizeAlloc(alloc, "source.*");
    defer lexer.freeTokens(alloc, &source_all);
    var source_all_pos: usize = 0;
    try std.testing.expectError(error.InvalidSqlCatalog, matchQualifiedReturningAll(alloc, source_all.items, &source_all_pos, &.{"target"}));
    try validateMergeFields(left_schema, right_schema, "tenant", "status");
    try std.testing.expectError(error.InvalidSqlCatalog, validateMergeFields(left_schema, right_schema, "tenant", "amount"));
    const normalized_target = try normalizeTargetSelectorFieldAlloc(alloc, "target.tenant", &.{ "target", "public.target" });
    defer alloc.free(normalized_target);
    try std.testing.expectEqualStrings("tenant", normalized_target);
    const normalized_unqualified = try normalizeTargetSelectorFieldAlloc(alloc, "tenant", &.{"target"});
    defer alloc.free(normalized_unqualified);
    try std.testing.expectEqualStrings("tenant", normalized_unqualified);
    try std.testing.expectError(error.InvalidSqlCatalog, normalizeTargetSelectorFieldAlloc(alloc, "other.tenant", &.{"target"}));

    const raw = [_]plan_mod.QualifiedProjection{
        .{ .source = .{ .qualifier = "u", .field = "tenant" }, .output = "tenant" },
        .{ .source = .{ .qualifier = "c", .field = "status" }, .output = "customer_status" },
    };
    var select = std.ArrayListUnmanaged(db_mod.types.RelationalRowsJoinProjection).empty;
    defer {
        for (select.items) |projection| {
            alloc.free(projection.output);
            alloc.free(projection.field);
        }
        select.deinit(alloc);
    }
    try resolveJoinProjectionsAlloc(alloc, left_schema, right_schema, &raw, "u", "c", &select);
    try std.testing.expectEqual(@as(usize, 2), select.items.len);
    try std.testing.expectEqual(db_mod.types.RelationalRowsJoinProjectionSide.left, select.items[0].side);
    try std.testing.expectEqualStrings("tenant", select.items[0].field);
    try std.testing.expectEqual(db_mod.types.RelationalRowsJoinProjectionSide.right, select.items[1].side);
    try std.testing.expectEqualStrings("customer_status", select.items[1].output);
}

const TestCatalog = struct {
    tables: [2]metadata_table_manager.TableRecord,

    fn init(table_name: []const u8, schema_json: []const u8, tenant_schema_json: []const u8) @This() {
        return .{ .tables = .{
            .{
                .table_id = 2,
                .name = table_name,
                .database_name = "tenant_ops",
                .namespace_name = "analytics",
                .placement_role = "data",
                .schema_json = tenant_schema_json,
            },
            .{
                .table_id = 1,
                .name = table_name,
                .database_name = metadata_table_manager.default_database_name,
                .namespace_name = metadata_table_manager.default_namespace_name,
                .placement_role = "data",
                .schema_json = schema_json,
            },
        } };
    }

    fn iface(self: *@This()) table_catalog.CatalogSource {
        return .{
            .ptr = self,
            .vtable = &.{
                .admin_snapshot = adminSnapshot,
                .free_admin_snapshot = freeAdminSnapshot,
            },
        };
    }

    fn adminSnapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return .{
            .status = .{ .metadata_group_id = 1, .metrics = .{} },
            .tables = self.tables[0..],
            .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{})[0..]),
            .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
            .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
            .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
            .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
        };
    }

    fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
};
