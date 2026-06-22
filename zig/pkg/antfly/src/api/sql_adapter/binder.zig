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

const metadata_api = @import("../../metadata/api.zig");
const metadata_table_manager = @import("../../metadata/table_manager.zig");
const metadata_transition_state = @import("../../metadata/transition_state.zig");
const raft_reconciler = @import("../../raft/reconciler.zig");
const db_mod = @import("../../storage/db/mod.zig");
const runtime_schema = @import("../../storage/schema.zig");
const schema_api = @import("../../schema/mod.zig");
const catalog_resources = @import("../catalog_resources.zig");
const table_catalog = @import("../table_catalog.zig");
const classifier = @import("classifier.zig");
const grammar = @import("grammar.zig");
const lexer = @import("lexer.zig");
const plan_mod = @import("plan.zig");
const parser = @import("parser.zig");
const token_mod = @import("token.zig");
const tokenized = @import("tokenized.zig");

const Token = token_mod.Token;

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
    owned_insert_source_schema: ?runtime_schema.TableSchema = null,
    owned_joined_source_schema: ?runtime_schema.TableSchema = null,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        if (self.owned_insert_source_schema) |schema| runtime_schema.freeSchema(alloc, schema);
        if (self.owned_joined_source_schema) |schema| runtime_schema.freeSchema(alloc, schema);
        self.* = undefined;
    }
};

pub const CatalogBoundReadPlanSourceSchema = struct {
    source_schema: ?runtime_schema.TableSchema = null,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        if (self.source_schema) |schema| runtime_schema.freeSchema(alloc, schema);
        self.* = undefined;
    }
};

pub const BoundSqlBinding = union(enum) {
    read_catalog: CatalogBoundReadPlanSourceSchema,
    write_catalog: CatalogBoundWritePlanOptions,
    none,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        switch (self.*) {
            .read_catalog => |*read| read.deinit(alloc),
            .write_catalog => |*write| write.deinit(alloc),
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
};

pub const CatalogLogicalReadPlan = struct {
    statement: tokenized.ParsedStatement,
    session: BoundSqlSession,
    source_schema: ?runtime_schema.TableSchema = null,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        if (self.source_schema) |schema| runtime_schema.freeSchema(alloc, schema);
        self.session.deinit(alloc);
        self.* = undefined;
    }
};

pub const CatalogLogicalWritePlan = struct {
    statement: tokenized.ParsedStatement,
    session: BoundSqlSession,
    options: plan_mod.LowerWritePlanOptions,
    owned_insert_source_schema: ?runtime_schema.TableSchema = null,
    owned_joined_source_schema: ?runtime_schema.TableSchema = null,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        if (self.owned_insert_source_schema) |schema| runtime_schema.freeSchema(alloc, schema);
        if (self.owned_joined_source_schema) |schema| runtime_schema.freeSchema(alloc, schema);
        self.session.deinit(alloc);
        self.* = undefined;
    }
};

pub const LogicalSqlPlan = union(enum) {
    catalog_read: CatalogLogicalReadPlan,
    catalog_write: CatalogLogicalWritePlan,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        switch (self.*) {
            .catalog_read => |*read| read.deinit(alloc),
            .catalog_write => |*write| write.deinit(alloc),
        }
        self.* = undefined;
    }
};

pub fn logicalReadPlanFromBoundStatement(bound: *BoundSqlStatement) !LogicalSqlPlan {
    const read = try bound.readCatalog();
    const source_schema = read.source_schema;
    read.source_schema = null;
    const session = bound.session;
    bound.session = BoundSqlSession.empty();
    return .{ .catalog_read = .{
        .statement = bound.statement,
        .session = session,
        .source_schema = source_schema,
    } };
}

pub fn logicalWritePlanFromBoundStatement(bound: *BoundSqlStatement) !LogicalSqlPlan {
    const write = try bound.writeCatalog();
    const owned_insert_source_schema = write.owned_insert_source_schema;
    const owned_joined_source_schema = write.owned_joined_source_schema;
    write.owned_insert_source_schema = null;
    write.owned_joined_source_schema = null;
    const session = bound.session;
    bound.session = BoundSqlSession.empty();
    return .{ .catalog_write = .{
        .statement = bound.statement,
        .session = session,
        .options = write.options,
        .owned_insert_source_schema = owned_insert_source_schema,
        .owned_joined_source_schema = owned_joined_source_schema,
    } };
}

pub const ReadPlanCatalogLoweringHooks = struct {
    ptr: *anyopaque,
    lower_with_source_schema: *const fn (*anyopaque, runtime_schema.TableSchema) anyerror!plan_mod.LoweredReadPlan,
    lower_without_source_schema: *const fn (*anyopaque) anyerror!plan_mod.LoweredReadPlan,
};

pub const WritePlanCatalogLoweringHooks = struct {
    ptr: *anyopaque,
    lower_with_options: *const fn (*anyopaque, plan_mod.LowerWritePlanOptions) anyerror!plan_mod.LoweredWritePlan,
};

pub fn lowerReadPlanWithCatalogSourceSchemaAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    catalog: table_catalog.CatalogSource,
    hooks: ReadPlanCatalogLoweringHooks,
) !plan_mod.LoweredReadPlan {
    var parsed_sql = try tokenized.ParsedSql.initAlloc(alloc, sql);
    defer parsed_sql.deinit(alloc);
    return try lowerReadPlanWithCatalogSourceSchemaParsedSqlAlloc(alloc, &parsed_sql, catalog, hooks);
}

pub fn lowerReadPlanWithCatalogSourceSchemaParsedSqlAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const tokenized.ParsedSql,
    catalog: table_catalog.CatalogSource,
    hooks: ReadPlanCatalogLoweringHooks,
) !plan_mod.LoweredReadPlan {
    return try lowerReadPlanWithCatalogBoundStatementAlloc(alloc, parsed_sql, catalog, hooks);
}

pub fn lowerReadPlanWithCatalogSourceSchemaFromTokensAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    catalog: table_catalog.CatalogSource,
    hooks: ReadPlanCatalogLoweringHooks,
) !plan_mod.LoweredReadPlan {
    var resolved = try bindReadPlanCatalogStatementFromTokensAlloc(alloc, tokens, catalog);
    defer resolved.deinit(alloc);
    var logical = try logicalReadPlanFromBoundStatement(&resolved);
    defer logical.deinit(alloc);
    return try lowerReadCatalogLogicalPlan(&logical, hooks);
}

pub fn lowerReadPlanWithCatalogBoundStatementAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const tokenized.ParsedSql,
    catalog: table_catalog.CatalogSource,
    hooks: ReadPlanCatalogLoweringHooks,
) !plan_mod.LoweredReadPlan {
    return try lowerReadPlanWithCatalogBoundStatementWithSessionAlloc(alloc, parsed_sql, catalog, catalog_resources.SqlCatalogSession.default(), hooks);
}

pub fn lowerReadPlanWithCatalogBoundStatementWithSessionAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const tokenized.ParsedSql,
    catalog: table_catalog.CatalogSource,
    session: catalog_resources.SqlCatalogSession,
    hooks: ReadPlanCatalogLoweringHooks,
) !plan_mod.LoweredReadPlan {
    var resolved = try bindReadPlanCatalogStatementWithSessionAlloc(alloc, parsed_sql, catalog, session);
    defer resolved.deinit(alloc);
    var logical = try logicalReadPlanFromBoundStatement(&resolved);
    defer logical.deinit(alloc);
    return try lowerReadCatalogLogicalPlan(&logical, hooks);
}

pub fn lowerWritePlanWithCatalogOptionsAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    options: plan_mod.LowerWritePlanOptions,
    catalog: table_catalog.CatalogSource,
    hooks: WritePlanCatalogLoweringHooks,
) !plan_mod.LoweredWritePlan {
    var parsed_sql = try tokenized.ParsedSql.initAlloc(alloc, sql);
    defer parsed_sql.deinit(alloc);
    return try lowerWritePlanWithCatalogOptionsParsedSqlAlloc(alloc, &parsed_sql, options, catalog, hooks);
}

pub fn lowerWritePlanWithCatalogOptionsParsedSqlAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const tokenized.ParsedSql,
    options: plan_mod.LowerWritePlanOptions,
    catalog: table_catalog.CatalogSource,
    hooks: WritePlanCatalogLoweringHooks,
) !plan_mod.LoweredWritePlan {
    return try lowerWritePlanWithCatalogBoundStatementAlloc(alloc, parsed_sql, options, catalog, hooks);
}

pub fn lowerWritePlanWithCatalogOptionsFromTokensAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    options: plan_mod.LowerWritePlanOptions,
    catalog: table_catalog.CatalogSource,
    hooks: WritePlanCatalogLoweringHooks,
) !plan_mod.LoweredWritePlan {
    var resolved = try bindWritePlanCatalogStatementFromTokensAlloc(alloc, tokens, options, catalog);
    defer resolved.deinit(alloc);
    var logical = try logicalWritePlanFromBoundStatement(&resolved);
    defer logical.deinit(alloc);
    return try lowerWriteCatalogLogicalPlan(&logical, hooks);
}

pub fn lowerWritePlanWithCatalogBoundStatementAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const tokenized.ParsedSql,
    options: plan_mod.LowerWritePlanOptions,
    catalog: table_catalog.CatalogSource,
    hooks: WritePlanCatalogLoweringHooks,
) !plan_mod.LoweredWritePlan {
    return try lowerWritePlanWithCatalogBoundStatementWithSessionAlloc(alloc, parsed_sql, options, catalog, catalog_resources.SqlCatalogSession.default(), hooks);
}

pub fn lowerWritePlanWithCatalogBoundStatementWithSessionAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const tokenized.ParsedSql,
    options: plan_mod.LowerWritePlanOptions,
    catalog: table_catalog.CatalogSource,
    session: catalog_resources.SqlCatalogSession,
    hooks: WritePlanCatalogLoweringHooks,
) !plan_mod.LoweredWritePlan {
    var resolved = try bindWritePlanCatalogStatementWithSessionAlloc(alloc, parsed_sql, options, catalog, session);
    defer resolved.deinit(alloc);
    var logical = try logicalWritePlanFromBoundStatement(&resolved);
    defer logical.deinit(alloc);
    return try lowerWriteCatalogLogicalPlan(&logical, hooks);
}

pub fn lowerReadCatalogLogicalPlan(logical: *LogicalSqlPlan, hooks: ReadPlanCatalogLoweringHooks) !plan_mod.LoweredReadPlan {
    return switch (logical.*) {
        .catalog_read => |*read| if (read.source_schema) |source_schema|
            try hooks.lower_with_source_schema(hooks.ptr, source_schema)
        else
            try hooks.lower_without_source_schema(hooks.ptr),
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

pub fn insertSourceTableNamesAlloc(alloc: std.mem.Allocator, sql: []const u8) !?InsertSourceTableNames {
    var parsed_sql = try tokenized.ParsedSql.initAlloc(alloc, sql);
    defer parsed_sql.deinit(alloc);
    return try insertSourceTableNamesFromParsedSqlAlloc(alloc, &parsed_sql);
}

pub fn insertSourceTableNamesFromParsedSqlAlloc(alloc: std.mem.Allocator, parsed_sql: *const tokenized.ParsedSql) !?InsertSourceTableNames {
    return try insertSourceTableNamesFromTokensAlloc(alloc, parsed_sql.items());
}

pub fn insertSourceTableNamesFromTokensAlloc(alloc: std.mem.Allocator, tokens: []const Token) !?InsertSourceTableNames {
    if (tokens.len == 0 or tokens[0].kind != .identifier) return null;
    if (std.ascii.eqlIgnoreCase(tokens[0].text, "with")) return try insertSourceTableNamesFromWithAlloc(alloc, tokens);
    if (!std.ascii.eqlIgnoreCase(tokens[0].text, "insert")) return null;
    return try insertSourceTableNamesFromInsertAlloc(alloc, tokens, 0);
}

pub fn recursiveInsertSourceTableNamesAlloc(alloc: std.mem.Allocator, sql: []const u8) !?InsertSourceTableNames {
    var parsed_sql = try tokenized.ParsedSql.initAlloc(alloc, sql);
    defer parsed_sql.deinit(alloc);
    return try recursiveInsertSourceTableNamesFromParsedSqlAlloc(alloc, &parsed_sql);
}

pub fn recursiveInsertSourceTableNamesFromParsedSqlAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const tokenized.ParsedSql,
) !?InsertSourceTableNames {
    return try recursiveInsertSourceTableNamesFromTokensAlloc(alloc, parsed_sql.items());
}

pub fn recursiveInsertSourceTableNamesFromTokensAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
) !?InsertSourceTableNames {
    if (tokens.len == 0 or tokens[0].kind != .identifier) return null;
    if (!std.ascii.eqlIgnoreCase(tokens[0].text, "with")) return null;
    var index: usize = 1;
    if (!consumeKeyword(tokens, &index, "recursive")) return null;
    if (index >= tokens.len or tokens[index].kind != .identifier) return null;
    index += 1;
    if (index < tokens.len and tokens[index].kind == .lparen) {
        index = (findMatchingRParenIndex(tokens, index) orelse return null) + 1;
    }
    if (!consumeKeyword(tokens, &index, "as")) return null;
    parser.consumeCteMaterializationHint(tokens, &index) catch return null;
    if (index >= tokens.len or tokens[index].kind != .lparen) return null;

    const body_start = index + 1;
    const body_end = findMatchingRParenIndex(tokens, index) orelse return null;
    const body = tokens[body_start..body_end];
    const from_index = findTopLevelKeyword(body, "from") orelse return null;
    var source_index = body_start + from_index + 1;
    _ = consumeKeyword(tokens, &source_index, "only");
    if (source_index >= body_end or tokens[source_index].kind != .identifier) return null;
    const source = try normalizeSqlObjectIdentifierAlloc(alloc, tokens[source_index].text);
    var source_transferred = false;
    defer if (!source_transferred) alloc.free(source);

    index = body_end + 1;
    if (!consumeKeyword(tokens, &index, "insert")) return null;
    if (!consumeKeyword(tokens, &index, "into")) return null;
    _ = consumeKeyword(tokens, &index, "only");
    if (index >= tokens.len or tokens[index].kind != .identifier) return null;
    const target = try normalizeSqlObjectIdentifierAlloc(alloc, tokens[index].text);
    errdefer alloc.free(target);

    source_transferred = true;
    return .{
        .target = target,
        .source = source,
    };
}

pub fn joinedWriteSourceTableNamesAlloc(alloc: std.mem.Allocator, sql: []const u8) !?InsertSourceTableNames {
    var parsed_sql = try tokenized.ParsedSql.initAlloc(alloc, sql);
    defer parsed_sql.deinit(alloc);
    return try joinedWriteSourceTableNamesFromParsedSqlAlloc(alloc, &parsed_sql);
}

pub fn joinedWriteSourceTableNamesFromParsedSqlAlloc(alloc: std.mem.Allocator, parsed_sql: *const tokenized.ParsedSql) !?InsertSourceTableNames {
    return try joinedWriteSourceTableNamesFromTokensAlloc(alloc, parsed_sql.items());
}

pub fn joinedWriteSourceTableNamesFromTokensAlloc(alloc: std.mem.Allocator, tokens: []const Token) !?InsertSourceTableNames {
    if (tokens.len == 0 or tokens[0].kind != .identifier) return null;
    if (std.ascii.eqlIgnoreCase(tokens[0].text, "with")) return try joinedWriteSourceTableNamesFromWithAlloc(alloc, tokens);
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
    return try bindWritePlanCatalogStatementFromTokensWithStatementAlloc(
        alloc,
        parsed_sql.statement,
        parsed_sql.items(),
        options,
        catalog,
        session,
    );
}

pub fn bindWritePlanCatalogStatementFromTokensAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    options: plan_mod.LowerWritePlanOptions,
    catalog: table_catalog.CatalogSource,
) !BoundSqlStatement {
    return try bindWritePlanCatalogStatementFromTokensWithSessionAlloc(alloc, tokens, options, catalog, catalog_resources.SqlCatalogSession.default());
}

pub fn bindWritePlanCatalogStatementFromTokensWithSessionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    options: plan_mod.LowerWritePlanOptions,
    catalog: table_catalog.CatalogSource,
    session: catalog_resources.SqlCatalogSession,
) !BoundSqlStatement {
    const raw_statement = tokenized.RawSqlStatement{
        .family = null,
        .token_start = 0,
        .token_end = tokens.len,
        .source_span = if (tokens.len > 0) .{
            .start = tokens[0].source_start,
            .end = tokens[tokens.len - 1].source_end,
        } else .{},
    };
    const statement: tokenized.ParsedStatement = .{ .unknown = raw_statement };
    return try bindWritePlanCatalogStatementFromTokensWithStatementAlloc(alloc, statement, tokens, options, catalog, session);
}

fn bindWritePlanCatalogStatementFromTokensWithStatementAlloc(
    alloc: std.mem.Allocator,
    statement: tokenized.ParsedStatement,
    tokens: []const Token,
    options: plan_mod.LowerWritePlanOptions,
    catalog: table_catalog.CatalogSource,
    session: catalog_resources.SqlCatalogSession,
) !BoundSqlStatement {
    var bound_session = try BoundSqlSession.fromSessionAlloc(alloc, session);
    errdefer bound_session.deinit(alloc);
    var resolved = try resolveWritePlanCatalogOptionsFromTokensWithSessionAlloc(alloc, tokens, options, catalog, session);
    errdefer resolved.deinit(alloc);
    return .{
        .statement = statement,
        .session = bound_session,
        .binding = .{ .write_catalog = resolved },
    };
}

pub fn resolveWritePlanCatalogOptionsAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    options: plan_mod.LowerWritePlanOptions,
    catalog: table_catalog.CatalogSource,
) !CatalogBoundWritePlanOptions {
    var parsed_sql = try tokenized.ParsedSql.initAlloc(alloc, sql);
    defer parsed_sql.deinit(alloc);
    return try resolveWritePlanCatalogOptionsParsedSqlAlloc(alloc, &parsed_sql, options, catalog);
}

pub fn resolveWritePlanCatalogOptionsParsedSqlAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const tokenized.ParsedSql,
    options: plan_mod.LowerWritePlanOptions,
    catalog: table_catalog.CatalogSource,
) !CatalogBoundWritePlanOptions {
    return try resolveWritePlanCatalogOptionsParsedSqlWithSessionAlloc(alloc, parsed_sql, options, catalog, catalog_resources.SqlCatalogSession.default());
}

pub fn resolveWritePlanCatalogOptionsParsedSqlWithSessionAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const tokenized.ParsedSql,
    options: plan_mod.LowerWritePlanOptions,
    catalog: table_catalog.CatalogSource,
    session: catalog_resources.SqlCatalogSession,
) !CatalogBoundWritePlanOptions {
    return try resolveWritePlanCatalogOptionsFromTokensWithSessionAlloc(alloc, parsed_sql.items(), options, catalog, session);
}

pub fn resolveWritePlanCatalogOptionsFromTokensAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    options: plan_mod.LowerWritePlanOptions,
    catalog: table_catalog.CatalogSource,
) !CatalogBoundWritePlanOptions {
    return try resolveWritePlanCatalogOptionsFromTokensWithSessionAlloc(alloc, tokens, options, catalog, catalog_resources.SqlCatalogSession.default());
}

pub fn resolveWritePlanCatalogOptionsFromTokensWithSessionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    options: plan_mod.LowerWritePlanOptions,
    catalog: table_catalog.CatalogSource,
    session: catalog_resources.SqlCatalogSession,
) !CatalogBoundWritePlanOptions {
    var out = CatalogBoundWritePlanOptions{
        .options = options,
    };
    errdefer out.deinit(alloc);
    var resolved_recursive_insert_source = false;

    if (out.options.insert_source_schema == null) {
        if (try recursiveInsertSourceTableNamesFromTokensAlloc(alloc, tokens)) |resolved_tables| {
            var tables = resolved_tables;
            defer tables.deinit(alloc);
            resolved_recursive_insert_source = true;
            if (!std.mem.eql(u8, tables.target, tables.source)) {
                out.owned_insert_source_schema = try runtimeSchemaForCatalogTableWithSessionAlloc(alloc, catalog, tables.source, session);
                out.options.insert_source_schema = out.owned_insert_source_schema.?;
            }
        } else if (try insertSourceTableNamesFromTokensAlloc(alloc, tokens)) |resolved_tables| {
            var tables = resolved_tables;
            defer tables.deinit(alloc);
            if (!std.mem.eql(u8, tables.target, tables.source)) {
                out.owned_insert_source_schema = try runtimeSchemaForCatalogTableWithSessionAlloc(alloc, catalog, tables.source, session);
                out.options.insert_source_schema = out.owned_insert_source_schema.?;
            }
        }
    }

    if (!resolved_recursive_insert_source and out.options.joined_source_schema == null) {
        if (joinedWriteSourceTableNamesFromTokensAlloc(alloc, tokens)) |maybe_resolved_tables| {
            if (maybe_resolved_tables) |resolved_tables| {
                var tables = resolved_tables;
                defer tables.deinit(alloc);
                if (!std.mem.eql(u8, tables.target, tables.source)) {
                    out.owned_joined_source_schema = try runtimeSchemaForCatalogTableWithSessionAlloc(alloc, catalog, tables.source, session);
                    out.options.joined_source_schema = out.owned_joined_source_schema.?;
                }
            }
        } else |err| switch (err) {
            error.UnsupportedSqlShape => {},
            else => return err,
        }
    }

    return out;
}

pub fn resolveReadPlanCatalogSourceSchemaAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    catalog: table_catalog.CatalogSource,
) !CatalogBoundReadPlanSourceSchema {
    var parsed_sql = try tokenized.ParsedSql.initAlloc(alloc, sql);
    defer parsed_sql.deinit(alloc);
    return try resolveReadPlanCatalogSourceSchemaParsedSqlAlloc(alloc, &parsed_sql, catalog);
}

pub fn resolveReadPlanCatalogSourceSchemaParsedSqlAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const tokenized.ParsedSql,
    catalog: table_catalog.CatalogSource,
) !CatalogBoundReadPlanSourceSchema {
    return try resolveReadPlanCatalogSourceSchemaParsedSqlWithSessionAlloc(alloc, parsed_sql, catalog, catalog_resources.SqlCatalogSession.default());
}

pub fn resolveReadPlanCatalogSourceSchemaParsedSqlWithSessionAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const tokenized.ParsedSql,
    catalog: table_catalog.CatalogSource,
    session: catalog_resources.SqlCatalogSession,
) !CatalogBoundReadPlanSourceSchema {
    return try resolveReadPlanCatalogSourceSchemaFromTokensWithSessionAlloc(alloc, parsed_sql.items(), catalog, session);
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
    return try bindReadPlanCatalogStatementFromTokensWithStatementAlloc(
        alloc,
        parsed_sql.statement,
        parsed_sql.items(),
        catalog,
        session,
    );
}

pub fn bindReadPlanCatalogStatementFromTokensAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    catalog: table_catalog.CatalogSource,
) !BoundSqlStatement {
    return try bindReadPlanCatalogStatementFromTokensWithSessionAlloc(alloc, tokens, catalog, catalog_resources.SqlCatalogSession.default());
}

pub fn bindReadPlanCatalogStatementFromTokensWithSessionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    catalog: table_catalog.CatalogSource,
    session: catalog_resources.SqlCatalogSession,
) !BoundSqlStatement {
    const raw_statement = tokenized.RawSqlStatement{
        .family = null,
        .token_start = 0,
        .token_end = tokens.len,
        .source_span = if (tokens.len > 0) .{
            .start = tokens[0].source_start,
            .end = tokens[tokens.len - 1].source_end,
        } else .{},
    };
    const statement: tokenized.ParsedStatement = .{ .unknown = raw_statement };
    return try bindReadPlanCatalogStatementFromTokensWithStatementAlloc(alloc, statement, tokens, catalog, session);
}

fn bindReadPlanCatalogStatementFromTokensWithStatementAlloc(
    alloc: std.mem.Allocator,
    statement: tokenized.ParsedStatement,
    tokens: []const Token,
    catalog: table_catalog.CatalogSource,
    session: catalog_resources.SqlCatalogSession,
) !BoundSqlStatement {
    var bound_session = try BoundSqlSession.fromSessionAlloc(alloc, session);
    errdefer bound_session.deinit(alloc);
    var resolved = try resolveReadPlanCatalogSourceSchemaFromTokensWithSessionAlloc(alloc, tokens, catalog, session);
    errdefer resolved.deinit(alloc);
    return .{
        .statement = statement,
        .session = bound_session,
        .binding = .{ .read_catalog = resolved },
    };
}

pub fn resolveReadPlanCatalogSourceSchemaFromTokensAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    catalog: table_catalog.CatalogSource,
) !CatalogBoundReadPlanSourceSchema {
    return try resolveReadPlanCatalogSourceSchemaFromTokensWithSessionAlloc(alloc, tokens, catalog, catalog_resources.SqlCatalogSession.default());
}

pub fn resolveReadPlanCatalogSourceSchemaFromTokensWithSessionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    catalog: table_catalog.CatalogSource,
    session: catalog_resources.SqlCatalogSession,
) !CatalogBoundReadPlanSourceSchema {
    var out = CatalogBoundReadPlanSourceSchema{};
    errdefer out.deinit(alloc);
    if (try readSourceTableNamesFromTokensAlloc(alloc, tokens)) |resolved_tables| {
        var tables = resolved_tables;
        defer tables.deinit(alloc);
        if (!std.mem.eql(u8, tables.left, tables.source)) {
            out.source_schema = try runtimeSchemaForCatalogTableWithSessionAlloc(alloc, catalog, tables.source, session);
        }
    }
    return out;
}

fn joinedWriteSourceTableNamesFromStatementAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    statement_index: usize,
) !?InsertSourceTableNames {
    if (statement_index >= tokens.len or tokens[statement_index].kind != .identifier) return null;
    if (std.ascii.eqlIgnoreCase(tokens[statement_index].text, "update")) {
        var target_index: usize = statement_index + 1;
        _ = consumeKeyword(tokens, &target_index, "only");
        if (target_index >= tokens.len or tokens[target_index].kind != .identifier) return error.UnsupportedSqlShape;
        const target = try normalizeSqlObjectIdentifierAlloc(alloc, tokens[target_index].text);
        var target_transferred = false;
        errdefer if (!target_transferred) alloc.free(target);

        const from_index = findTopLevelKeyword(tokens[target_index + 1 ..], "from") orelse {
            const where_index = findTopLevelKeyword(tokens[target_index + 1 ..], "where") orelse {
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
        _ = consumeKeyword(tokens, &source_index, "only");
        if (source_index >= tokens.len or tokens[source_index].kind != .identifier) return error.UnsupportedSqlShape;
        const source = try normalizeSqlObjectIdentifierAlloc(alloc, tokens[source_index].text);
        errdefer alloc.free(source);

        target_transferred = true;
        return .{ .target = target, .source = source };
    }

    if (std.ascii.eqlIgnoreCase(tokens[statement_index].text, "delete")) {
        var target_index: usize = statement_index + 1;
        if (!consumeKeyword(tokens, &target_index, "from")) return null;
        _ = consumeKeyword(tokens, &target_index, "only");
        if (target_index >= tokens.len or tokens[target_index].kind != .identifier) return error.UnsupportedSqlShape;
        const target = try normalizeSqlObjectIdentifierAlloc(alloc, tokens[target_index].text);
        var target_transferred = false;
        errdefer if (!target_transferred) alloc.free(target);

        const using_index = findTopLevelKeyword(tokens[target_index + 1 ..], "using") orelse {
            const where_index = findTopLevelKeyword(tokens[target_index + 1 ..], "where") orelse {
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
        _ = consumeKeyword(tokens, &source_index, "only");
        if (source_index >= tokens.len or tokens[source_index].kind != .identifier) return error.UnsupportedSqlShape;
        const source = try normalizeSqlObjectIdentifierAlloc(alloc, tokens[source_index].text);
        errdefer alloc.free(source);

        target_transferred = true;
        return .{ .target = target, .source = source };
    }

    if (std.ascii.eqlIgnoreCase(tokens[statement_index].text, "merge")) {
        var target_index: usize = statement_index + 1;
        if (!consumeKeyword(tokens, &target_index, "into")) return null;
        _ = consumeKeyword(tokens, &target_index, "only");
        if (target_index >= tokens.len or tokens[target_index].kind != .identifier) return error.UnsupportedSqlShape;
        const target = try normalizeSqlObjectIdentifierAlloc(alloc, tokens[target_index].text);
        var target_transferred = false;
        errdefer if (!target_transferred) alloc.free(target);

        const using_index = findTopLevelKeyword(tokens[target_index + 1 ..], "using") orelse {
            alloc.free(target);
            return null;
        };
        var source_index = target_index + 1 + using_index + 1;
        _ = consumeKeyword(tokens, &source_index, "only");
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
        const is_exists = index > 0 and tokens[index - 1].kind == .identifier and std.ascii.eqlIgnoreCase(tokens[index - 1].text, "exists");
        const is_in = index > 0 and tokens[index - 1].kind == .identifier and std.ascii.eqlIgnoreCase(tokens[index - 1].text, "in");
        if (is_exists or is_in) {
            const body = tokens[index + 1 .. close_index];
            if (body.len > 0 and body[0].kind == .identifier and std.ascii.eqlIgnoreCase(body[0].text, "select")) {
                const from_index = findTopLevelKeyword(body, "from") orelse return error.UnsupportedSqlShape;
                var source_index = from_index + 1;
                _ = consumeKeyword(body, &source_index, "only");
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
    if (consumeKeyword(tokens, &index, "recursive")) return error.UnsupportedSqlShape;

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
        if (!consumeKeyword(tokens, &index, "as")) return error.UnsupportedSqlShape;
        try parser.consumeCteMaterializationHint(tokens, &index);
        if (index >= tokens.len or tokens[index].kind != .lparen) return error.UnsupportedSqlShape;
        const close_index = findMatchingRParenIndex(tokens, index) orelse return error.UnsupportedSqlShape;
        if (index + 1 >= close_index) return error.UnsupportedSqlShape;

        var cte_tables = (try selectReadTableNamesAlloc(alloc, tokens[index + 1 .. close_index], 0)) orelse return error.UnsupportedSqlShape;
        defer cte_tables.deinit(alloc);
        try resolveSelectReadTablesAgainstCtes(alloc, cte_bindings.items, &cte_tables);
        if (cte_tables.source) |source| {
            if (!std.mem.eql(u8, cte_tables.left, source)) return error.UnsupportedSqlShape;
        }

        try cte_bindings.append(alloc, .{
            .name = cte_name,
            .source = try alloc.dupe(u8, cte_tables.left),
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

pub fn readSourceTableNamesAlloc(alloc: std.mem.Allocator, sql: []const u8) !?ReadSourceTableNames {
    var parsed_sql = try tokenized.ParsedSql.initAlloc(alloc, sql);
    defer parsed_sql.deinit(alloc);
    return try readSourceTableNamesFromParsedSqlAlloc(alloc, &parsed_sql);
}

pub fn readSourceTableNamesFromParsedSqlAlloc(alloc: std.mem.Allocator, parsed_sql: *const tokenized.ParsedSql) !?ReadSourceTableNames {
    return try readSourceTableNamesFromTokensAlloc(alloc, parsed_sql.items());
}

pub fn readSourceTableNamesFromTokensAlloc(alloc: std.mem.Allocator, tokens: []const Token) !?ReadSourceTableNames {
    if (tokens.len == 0 or tokens[0].kind != .identifier) return null;
    if (std.ascii.eqlIgnoreCase(tokens[0].text, "with")) return try readSourceTableNamesFromWithAlloc(alloc, tokens);
    if (!std.ascii.eqlIgnoreCase(tokens[0].text, "select")) return null;
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
    if (select_index >= tokens.len or tokens[select_index].kind != .identifier or !std.ascii.eqlIgnoreCase(tokens[select_index].text, "select")) return null;

    const from_index = if (findTopLevelKeyword(tokens[select_index..], "from")) |relative|
        select_index + relative
    else
        return null;
    var left_index = from_index + 1;
    _ = consumeKeyword(tokens, &left_index, "only");
    if (left_index >= tokens.len or tokens[left_index].kind != .identifier) return error.UnsupportedSqlShape;
    const left = try normalizeSqlObjectIdentifierAlloc(alloc, tokens[left_index].text);
    var left_transferred = false;
    errdefer if (!left_transferred) alloc.free(left);

    const join_index = if (findTopLevelKeyword(tokens[left_index + 1 ..], "join")) |relative|
        left_index + 1 + relative
    else {
        if (try selectSetOperationSourceTableNameAlloc(alloc, tokens, left_index + 1)) |source| {
            left_transferred = true;
            return .{ .left = left, .source = source };
        }
        left_transferred = true;
        return .{ .left = left };
    };
    var source_index = join_index + 1;
    if (consumeKeyword(tokens, &source_index, "lateral")) {
        if (source_index >= tokens.len or tokens[source_index].kind != .lparen) return error.UnsupportedSqlShape;
        const close_index = findMatchingRParenIndex(tokens, source_index) orelse return error.UnsupportedSqlShape;
        const inner_from = findTopLevelKeyword(tokens[source_index + 1 .. close_index], "from") orelse return error.UnsupportedSqlShape;
        source_index = source_index + 1 + inner_from + 1;
    }
    _ = consumeKeyword(tokens, &source_index, "only");
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
                if (depth != 0 or !selectSetOperationKeyword(token.text)) continue;
                var select_index = i + 1;
                if (std.ascii.eqlIgnoreCase(token.text, "union")) {
                    _ = consumeKeyword(tokens, &select_index, "all") or consumeKeyword(tokens, &select_index, "distinct");
                } else {
                    _ = consumeKeyword(tokens, &select_index, "distinct");
                }
                if (select_index >= tokens.len or tokens[select_index].kind != .identifier or
                    !std.ascii.eqlIgnoreCase(tokens[select_index].text, "select"))
                {
                    return error.UnsupportedSqlShape;
                }
                const from_relative = findTopLevelKeyword(tokens[select_index + 1 ..], "from") orelse return error.UnsupportedSqlShape;
                var source_index = select_index + 1 + from_relative + 1;
                _ = consumeKeyword(tokens, &source_index, "only");
                if (source_index >= tokens.len or tokens[source_index].kind != .identifier) return error.UnsupportedSqlShape;
                return try normalizeSqlObjectIdentifierAlloc(alloc, tokens[source_index].text);
            },
            else => {},
        }
    }
    return null;
}

fn selectSetOperationKeyword(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "union") or
        std.ascii.eqlIgnoreCase(text, "intersect") or
        std.ascii.eqlIgnoreCase(text, "except");
}

fn readSourceTableNamesFromWithAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
) !?ReadSourceTableNames {
    var index: usize = 1;
    if (consumeKeyword(tokens, &index, "recursive")) return error.UnsupportedSqlShape;

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
        if (!consumeKeyword(tokens, &index, "as")) return error.UnsupportedSqlShape;
        try parser.consumeCteMaterializationHint(tokens, &index);
        if (index >= tokens.len or tokens[index].kind != .lparen) return error.UnsupportedSqlShape;
        const close_index = findMatchingRParenIndex(tokens, index) orelse return error.UnsupportedSqlShape;
        if (index + 1 >= close_index) return error.UnsupportedSqlShape;

        var cte_tables = (try selectReadTableNamesAlloc(alloc, tokens[index + 1 .. close_index], 0)) orelse return error.UnsupportedSqlShape;
        defer cte_tables.deinit(alloc);
        try resolveSelectReadTablesAgainstCtes(alloc, cte_bindings.items, &cte_tables);
        if (cte_tables.source) |source| {
            if (!std.mem.eql(u8, cte_tables.left, source)) return error.UnsupportedSqlShape;
        }

        try cte_bindings.append(alloc, .{
            .name = cte_name,
            .source = try alloc.dupe(u8, cte_tables.left),
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

fn normalizeSqlObjectIdentifierAlloc(alloc: std.mem.Allocator, identifier: []const u8) ![]const u8 {
    return try grammar.normalizeSqlObjectIdentifierAlloc(alloc, identifier);
}

fn insertSourceTableNamesFromInsertAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    insert_index: usize,
) !?InsertSourceTableNames {
    if (insert_index >= tokens.len or tokens[insert_index].kind != .identifier or !std.ascii.eqlIgnoreCase(tokens[insert_index].text, "insert")) return null;
    var index: usize = insert_index + 1;
    if (!consumeKeyword(tokens, &index, "into")) return null;
    _ = consumeKeyword(tokens, &index, "only");
    if (index >= tokens.len or tokens[index].kind != .identifier) return error.UnsupportedSqlShape;
    const target = try normalizeSqlObjectIdentifierAlloc(alloc, tokens[index].text);
    var target_transferred = false;
    errdefer if (!target_transferred) alloc.free(target);
    index += 1;

    const select_index = findTopLevelKeyword(tokens[index..], "select") orelse {
        alloc.free(target);
        return null;
    };
    const absolute_select = index + select_index;
    const from_index = findTopLevelKeyword(tokens[absolute_select + 1 ..], "from") orelse return error.UnsupportedSqlShape;
    var source_index = absolute_select + 1 + from_index + 1;
    _ = consumeKeyword(tokens, &source_index, "only");
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
    if (consumeKeyword(tokens, &index, "recursive")) return error.UnsupportedSqlShape;

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
        if (!consumeKeyword(tokens, &index, "as")) return error.UnsupportedSqlShape;
        try parser.consumeCteMaterializationHint(tokens, &index);
        if (index >= tokens.len or tokens[index].kind != .lparen) return error.UnsupportedSqlShape;
        const close_index = findMatchingRParenIndex(tokens, index) orelse return error.UnsupportedSqlShape;
        const from_index = findTopLevelKeyword(tokens[index + 1 .. close_index], "from") orelse return error.UnsupportedSqlShape;
        var source_index = index + 1 + from_index + 1;
        _ = consumeKeyword(tokens, &source_index, "only");
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

fn consumeKeyword(tokens: []const Token, index: *usize, keyword: []const u8) bool {
    return parser.matchKeyword(tokens, index, keyword);
}

fn findTopLevelKeyword(tokens: []const Token, keyword: []const u8) ?usize {
    return parser.findTopLevelKeyword(tokens, keyword);
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

    var joined = (try readSourceTableNamesAlloc(
        alloc,
        "WITH open_orders AS (SELECT id, tenant, customer_id FROM usage_records), active_customers AS (SELECT id, tenant, name FROM customer_records) SELECT o.id, c.name FROM open_orders AS o LEFT JOIN active_customers AS c ON o.tenant = c.tenant",
    )).?;
    defer joined.deinit(alloc);
    try std.testing.expectEqualStrings("usage_records", joined.left);
    try std.testing.expectEqualStrings("customer_records", joined.source);

    var lateral = (try readSourceTableNamesAlloc(
        alloc,
        "WITH orgs AS (SELECT id FROM usage_records), balances AS (SELECT organization_id, amount FROM balance_records) SELECT org.id, latest.amount FROM orgs AS org LEFT JOIN LATERAL (SELECT amount FROM balances AS bal WHERE bal.organization_id = org.id LIMIT 1) AS latest ON true",
    )).?;
    defer lateral.deinit(alloc);
    try std.testing.expectEqualStrings("usage_records", lateral.left);
    try std.testing.expectEqualStrings("balance_records", lateral.source);
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
    var catalog = MultiTableTestCatalog.init("usage_records", usage_schema_json, "incoming_usage", incoming_schema_json);

    var parsed_read = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT usage_records.id, incoming_usage.status FROM usage_records JOIN incoming_usage ON usage_records.id = incoming_usage.id",
    );
    defer parsed_read.deinit(alloc);
    var bound_read = try bindReadPlanCatalogStatementAlloc(alloc, &parsed_read, catalog.iface());
    defer bound_read.deinit(alloc);
    switch (bound_read.statement) {
        .read => |statement| try std.testing.expectEqual(classifier.SqlReadStatementKind.join, statement.kind),
        else => return error.TestUnexpectedResult,
    }
    const read = try bound_read.readCatalog();
    try std.testing.expect(read.source_schema != null);
    try std.testing.expectEqual(@as(usize, 3), read.source_schema.?.relational_columns.len);
    var logical_read = try logicalReadPlanFromBoundStatement(&bound_read);
    defer logical_read.deinit(alloc);
    switch (logical_read) {
        .catalog_read => |logical| {
            switch (logical.statement) {
                .read => |statement| try std.testing.expectEqual(classifier.SqlReadStatementKind.join, statement.kind),
                else => return error.TestUnexpectedResult,
            }
            try std.testing.expect(logical.source_schema != null);
            try std.testing.expectEqual(@as(usize, 3), logical.source_schema.?.relational_columns.len);
        },
        else => return error.TestUnexpectedResult,
    }
    try std.testing.expect((try bound_read.readCatalog()).source_schema == null);

    var parsed_write = try tokenized.ParsedSql.initAlloc(
        alloc,
        "INSERT INTO usage_records (id, status) SELECT id, status FROM incoming_usage",
    );
    defer parsed_write.deinit(alloc);
    var bound_write = try bindWritePlanCatalogStatementAlloc(alloc, &parsed_write, .{}, catalog.iface());
    defer bound_write.deinit(alloc);
    switch (bound_write.statement) {
        .write => |statement| try std.testing.expectEqual(classifier.SqlWriteStatementKind.insert, statement.kind),
        else => return error.TestUnexpectedResult,
    }
    const write = try bound_write.writeCatalog();
    try std.testing.expect(write.options.insert_source_schema != null);
    try std.testing.expectEqual(@as(usize, 3), write.options.insert_source_schema.?.relational_columns.len);
    var logical_write = try logicalWritePlanFromBoundStatement(&bound_write);
    defer logical_write.deinit(alloc);
    switch (logical_write) {
        .catalog_write => |logical| {
            switch (logical.statement) {
                .write => |statement| try std.testing.expectEqual(classifier.SqlWriteStatementKind.insert, statement.kind),
                else => return error.TestUnexpectedResult,
            }
            try std.testing.expect(logical.options.insert_source_schema != null);
            try std.testing.expectEqual(@as(usize, 3), logical.options.insert_source_schema.?.relational_columns.len);
        },
        else => return error.TestUnexpectedResult,
    }
    try std.testing.expect((try bound_write.writeCatalog()).owned_insert_source_schema == null);

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
}

test "sql adapter binder rejects ambiguous physical cte read source tables" {
    const alloc = std.testing.allocator;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        readSourceTableNamesAlloc(
            alloc,
            "WITH mixed AS (SELECT o.id FROM orders AS o JOIN customers AS c ON o.customer_id = c.id) SELECT mixed.id, s.id FROM mixed JOIN shipments AS s ON mixed.id = s.order_id",
        ),
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
