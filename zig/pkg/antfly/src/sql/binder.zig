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
const ddl_plan = @import("ddl_plan.zig");
const runtime_schema = @import("../storage/schema.zig");
const schema_api = @import("../schema/mod.zig");
const catalog_resources = @import("catalog_resources.zig");
const generated_parser = @import("generated_parser.zig");
const table_catalog = @import("../metadata/catalog/source.zig");
const sql_statement_kind = @import("statement_kind.zig");
const grammar = @import("grammar.zig");
const lexer = @import("lexer.zig");
const lower_expr = @import("lower_expr.zig");
const expr_row_parse = @import("expr/row_parse.zig");
const lowering_context = @import("lowering_context.zig");
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
    if (joined_target_expression_qualifiers.len != 0) {
        if (try normalizeQualifiedRowExpressionFieldAlloc(alloc, field, joined_target_expression_qualifiers, schema)) |target_field| {
            return .{
                .field = target_field,
                .source = .row,
                .schema = schema,
            };
        }
    }
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
    return relationalIndexCatalogNameExists(schema.relational_indexes, index_name) or
        uniqueConstraintNameExists(schema.unique_constraints, index_name);
}

pub fn relationalIndexCatalogNameExists(indexes: []const runtime_schema.RelationalIndex, index_name: []const u8) bool {
    for (indexes) |index| {
        if (std.mem.eql(u8, index.name, index_name)) return true;
    }
    return false;
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
        .database,
        .schema,
        .extension,
        .type,
        .domain,
        .function,
        .procedure,
        => {},
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

pub fn relationalColumnHasDeclaredIndexName(column: runtime_schema.RelationalColumn, index_name: []const u8) bool {
    const declared_index_name = column.index_name orelse return false;
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

pub fn relationalColumnTypeSupportsCollation(field_type: runtime_schema.AntflyType, array_item_type: ?runtime_schema.AntflyType) bool {
    if (relationalFieldTypeSupportsCollation(field_type)) return true;
    if (field_type != .array) return false;
    const item_type = array_item_type orelse return false;
    return relationalFieldTypeSupportsCollation(item_type);
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
    var snapshot = try catalog.adminSnapshot();
    defer catalog.freeAdminSnapshot(&snapshot);
    const resolved = try resolvedExistingCatalogTableForObjectNameAlloc(alloc, &snapshot, table_name, session) orelse return error.InvalidSqlCatalog;
    defer deinitCatalogTableRef(alloc, resolved.target);
    if (resolved.table.schema_json.len == 0) return error.InvalidSqlCatalog;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, resolved.table.schema_json);
    defer parsed.deinit(alloc);
    return try schema_api.deriveRuntimeTableSchema(alloc, parsed);
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

const ResolvedCatalogTable = struct {
    target: source_binding.CatalogTableRef,
    table: metadata_table_manager.TableRecord,
};

fn resolvedExistingCatalogTableForObjectNameAlloc(
    alloc: std.mem.Allocator,
    snapshot: *const metadata_api.AdminSnapshot,
    table_name: []const u8,
    session: catalog_resources.SqlCatalogSession,
) !?ResolvedCatalogTable {
    const parsed_target = try session.tableTargetFromObjectName(table_name);
    if (std.mem.indexOfScalar(u8, table_name, '.') != null) {
        const table = qualifiedTableRecord(snapshot, parsed_target.database_name, parsed_target.namespace_name, parsed_target.table_name) orelse return null;
        return .{
            .target = try catalogTableRefForTableRecordAlloc(alloc, table),
            .table = table,
        };
    }

    const default_search_path: []const []const u8 = &.{catalog_resources.default_namespace_name};
    const search_path = if (session.search_path.len == 0) default_search_path else session.search_path;
    for (search_path) |namespace_name| {
        const table = qualifiedTableRecord(snapshot, session.currentDatabase(), namespace_name, table_name) orelse continue;
        return .{
            .target = try catalogTableRefForTableRecordAlloc(alloc, table),
            .table = table,
        };
    }
    return null;
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
    var snapshot = try catalog.adminSnapshot();
    defer catalog.freeAdminSnapshot(&snapshot);
    const resolved = try resolvedExistingCatalogTableForObjectNameAlloc(alloc, &snapshot, table_name, session) orelse return error.TableNotFound;
    errdefer deinitCatalogTableRef(alloc, resolved.target);
    if (resolved.table.schema_json.len == 0) return error.InvalidSqlCatalog;
    const schema_info = try catalogSchemaInfoFromJsonAlloc(alloc, resolved.table.schema_json);
    return .{
        .role = role,
        .target = resolved.target,
        .family = schema_info.family,
        .schema_version = schema_info.version,
        .table_id = resolved.table.table_id,
        .schema_generation = metadata_table_manager.schemaRewriteGenerationForSchemaJson(resolved.table.schema_json),
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
    const schema_info = try catalogSchemaInfoFromJsonAlloc(alloc, table.schema_json);
    return .{
        .role = role,
        .target = .{
            .database_name = database_name,
            .namespace_name = namespace_name,
            .table_name = table_name,
        },
        .family = schema_info.family,
        .schema_version = schema_info.version,
        .table_id = table.table_id,
        .schema_generation = metadata_table_manager.schemaRewriteGenerationForSchemaJson(table.schema_json),
    };
}

const CatalogSchemaInfo = struct {
    family: source_binding.SqlSourceFamily,
    version: u32,
};

fn catalogSchemaInfoFromJsonAlloc(alloc: std.mem.Allocator, schema_json: []const u8) !CatalogSchemaInfo {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, schema_json, .{});
    defer parsed.deinit();
    const object = switch (parsed.value) {
        .object => |object| object,
        else => return error.InvalidSqlCatalog,
    };
    const version_value = switch (object.get("version") orelse return error.InvalidSqlCatalog) {
        .integer => |value| value,
        else => return error.InvalidSqlCatalog,
    };
    if (version_value < 0 or version_value > std.math.maxInt(u32)) return error.InvalidSqlCatalog;
    const family: source_binding.SqlSourceFamily = if (object.get("external_base_source") != null)
        .lake
    else blk: {
        const mode = switch (object.get("storage_mode") orelse return error.InvalidSqlCatalog) {
            .string => |value| value,
            else => return error.InvalidSqlCatalog,
        };
        if (std.mem.eql(u8, mode, "relational")) break :blk .relational;
        if (std.mem.eql(u8, mode, "document")) break :blk .document;
        return error.InvalidSqlCatalog;
    };
    return .{
        .family = family,
        .version = @intCast(version_value),
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
    var raw = try std.json.parseFromSlice(std.json.Value, alloc, schema_json, .{});
    defer raw.deinit();
    if (raw.value == .object) {
        if (raw.value.object.get("storage_mode")) |mode| {
            if (mode == .string and std.mem.eql(u8, mode.string, "relational")) {
                if (schemaJsonNamedArrayContainsName(raw.value.object.get("relational_indexes"), index_name)) return true;
                if (schemaJsonNamedArrayContainsName(raw.value.object.get("unique_constraints"), index_name)) return true;
                if (schemaJsonNamedObjectNameEquals(raw.value.object.get("primary_key"), index_name)) return true;
                return false;
            }
        }
    }
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);
    return relationalIndexNameExists(schema, index_name);
}

fn schemaJsonNamedArrayContainsName(value: ?std.json.Value, name: []const u8) bool {
    const array = switch (value orelse return false) {
        .array => |array| array,
        else => return false,
    };
    for (array.items) |item| {
        if (schemaJsonNamedObjectNameEquals(item, name)) return true;
    }
    return false;
}

fn schemaJsonNamedObjectNameEquals(value: ?std.json.Value, name: []const u8) bool {
    const object = switch (value orelse return false) {
        .object => |object| object,
        else => return false,
    };
    const object_name = switch (object.get("name") orelse return false) {
        .string => |string| string,
        else => return false,
    };
    return std.mem.eql(u8, object_name, name);
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
    allow_document_view: bool,
) !source_binding.SqlSourceBinding {
    var snapshot = try catalog.adminSnapshot();
    defer catalog.freeAdminSnapshot(&snapshot);
    if (try resolvedExistingCatalogTableForObjectNameAlloc(alloc, &snapshot, table_name, session)) |resolved| {
        errdefer deinitCatalogTableRef(alloc, resolved.target);
        return try sourceBindingForCatalogTableRecordAlloc(alloc, resolved.target, resolved.table);
    }
    if (!allow_document_view) return error.InvalidSqlCatalog;
    const source_table = try documentSqlViewSourceTableRecordForObjectNameAlloc(alloc, &snapshot, table_name, session) orelse return error.InvalidSqlCatalog;
    const source_target = try catalogTableRefForTableRecordAlloc(alloc, source_table);
    errdefer deinitCatalogTableRef(alloc, source_target);
    return try sourceBindingForCatalogTableRecordAlloc(alloc, source_target, source_table);
}

fn sourceBindingForCatalogTableRecordAlloc(
    alloc: std.mem.Allocator,
    target: source_binding.CatalogTableRef,
    table: metadata_table_manager.TableRecord,
) !source_binding.SqlSourceBinding {
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
            const schema_generation = metadata_table_manager.schemaRewriteGenerationForSchemaJson(table.schema_json);
            document.schema_generation = schema_generation;
            document.indexes_json = try alloc.dupe(u8, table.indexes_json);
            errdefer if (document.indexes_json) |indexes_json| alloc.free(@constCast(indexes_json));
            const source_schema_fingerprint = try source_binding.documentSqlSourceSchemaFingerprintAlloc(alloc, table.schema_json);
            defer alloc.free(source_schema_fingerprint);
            document.capabilities = try source_binding.documentCapabilitiesForRuntimeSchemaAndIndexesJsonWithBindingAlloc(alloc, schema, table.indexes_json, schema_generation, source_schema_fingerprint);
            errdefer source_binding.deinitDocumentSqlCapabilities(alloc, &document.capabilities);
            document.virtual_schema = try source_binding.documentSqlSchemaForRuntimeSchemaAndIndexesJsonWithBindingAlloc(alloc, schema, table.indexes_json, target.table_name, schema_generation, source_schema_fingerprint);
        },
        .lake => |*lake| {
            lake.table_id = table.table_id;
            lake.schema_generation = metadata_table_manager.schemaRewriteGenerationForSchemaJson(table.schema_json);
        },
    }
    return binding;
}

fn documentSqlViewSourceTableRecordAlloc(
    alloc: std.mem.Allocator,
    snapshot: *const metadata_api.AdminSnapshot,
    requested_target: source_binding.CatalogTableRef,
) !?metadata_table_manager.TableRecord {
    for (snapshot.tables) |table| {
        if (!std.mem.eql(u8, table.database_name, requested_target.database_name)) continue;
        if (!std.mem.eql(u8, table.namespace_name, requested_target.namespace_name)) continue;
        if (table.schema_json.len == 0) continue;
        if (!try source_binding.documentSqlIndexesJsonHasViewMappingForSourceTableAlloc(alloc, table.indexes_json, requested_target.table_name, table.name)) continue;
        return table;
    }
    return null;
}

fn documentSqlViewSourceTableRecordForObjectNameAlloc(
    alloc: std.mem.Allocator,
    snapshot: *const metadata_api.AdminSnapshot,
    table_name: []const u8,
    session: catalog_resources.SqlCatalogSession,
) !?metadata_table_manager.TableRecord {
    const parsed_target = try session.tableTargetFromObjectName(table_name);
    if (std.mem.indexOfScalar(u8, table_name, '.') != null) {
        return try documentSqlViewSourceTableRecordAlloc(alloc, snapshot, .{
            .database_name = parsed_target.database_name,
            .namespace_name = parsed_target.namespace_name,
            .table_name = parsed_target.table_name,
        });
    }

    const default_search_path: []const []const u8 = &.{catalog_resources.default_namespace_name};
    const search_path = if (session.search_path.len == 0) default_search_path else session.search_path;
    for (search_path) |namespace_name| {
        const source_table = try documentSqlViewSourceTableRecordAlloc(alloc, snapshot, .{
            .database_name = session.currentDatabase(),
            .namespace_name = namespace_name,
            .table_name = table_name,
        }) orelse continue;
        return source_table;
    }
    return null;
}

fn rejectDocumentSqlViewWriteTargetAlloc(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    table_name: []const u8,
    session: catalog_resources.SqlCatalogSession,
) !void {
    var snapshot = try catalog.adminSnapshot();
    defer catalog.freeAdminSnapshot(&snapshot);
    if (try resolvedExistingCatalogTableForObjectNameAlloc(alloc, &snapshot, table_name, session)) |resolved| {
        deinitCatalogTableRef(alloc, resolved.target);
        return;
    }
    if (try documentSqlViewSourceTableRecordForObjectNameAlloc(alloc, &snapshot, table_name, session) != null) return error.DocumentSqlWriteUnsupported;
}

fn catalogTableRefForTableRecordAlloc(alloc: std.mem.Allocator, table: metadata_table_manager.TableRecord) !source_binding.CatalogTableRef {
    const database_name = try alloc.dupe(u8, table.database_name);
    errdefer alloc.free(database_name);
    const namespace_name = try alloc.dupe(u8, table.namespace_name);
    errdefer alloc.free(namespace_name);
    const table_name = try alloc.dupe(u8, table.name);
    errdefer alloc.free(table_name);
    return .{
        .database_name = database_name,
        .namespace_name = namespace_name,
        .table_name = table_name,
    };
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
    extra_sources: []const []const u8 = &.{},

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(@constCast(self.left));
        alloc.free(@constCast(self.source));
        for (self.extra_sources) |source| alloc.free(@constCast(source));
        if (self.extra_sources.len > 0) alloc.free(self.extra_sources);
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
    source_schema_owned: bool = false,
    source_binding: ?source_binding.SqlSourceBinding = null,
    bound_objects: []BoundCatalogObject = &.{},
    authorization: BoundSqlAuthorization = .{},

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        if (self.target_binding) |*binding| deinitSqlSourceBinding(alloc, binding);
        if (self.source_schema_owned) {
            if (self.source_schema) |schema| runtime_schema.freeSchema(alloc, schema);
        } else if (self.source_binding == null) {
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

fn requireParsedCatalogReadStatementIncludingGeneratedAst(parsed_sql: *const tokenized.ParsedSql) !void {
    try requireParsedCatalogReadStatement(parsed_sql.statement);
    if (parsed_sql.generatedStatementKind() != .read) return error.UnsupportedSqlShape;
    _ = parsed_sql.generatedReadStatementKind() orelse return error.UnsupportedSqlShape;
}

fn requireParsedCatalogWriteStatement(statement: tokenized.ParsedStatement) !void {
    switch (statement) {
        .write => {},
        else => return error.UnsupportedSqlShape,
    }
}

fn requireParsedCatalogWriteStatementIncludingGeneratedAst(parsed_sql: *const tokenized.ParsedSql) !void {
    try requireParsedCatalogWriteStatement(parsed_sql.statement);
    if (parsed_sql.generatedStatementKind() != .dml) return error.UnsupportedSqlShape;
    _ = parsed_sql.writeStatementIncludingGeneratedAst() orelse return error.UnsupportedSqlShape;
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
    source_schema_owned: bool = false,
    source_binding: ?source_binding.SqlSourceBinding = null,
    bound_objects: []BoundCatalogObject = &.{},
    authorization: BoundSqlAuthorization = .{},

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        if (self.target_binding) |*binding| deinitSqlSourceBinding(alloc, binding);
        if (self.source_schema_owned) {
            if (self.source_schema) |schema| runtime_schema.freeSchema(alloc, schema);
        } else if (self.source_binding == null) {
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
    boundary: ddl_plan.TransactionBoundaryPlan,
    control: ddl_plan.TransactionControlPlan,
    prepared: ddl_plan.PreparedTransactionPlan,
    savepoint: ddl_plan.SavepointTransactionPlan,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        switch (self.*) {
            .boundary => {},
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
    security_label: ddl_plan.SecurityLabelPlan,

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
            .security_label => |*plan| plan.deinit(alloc),
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
    const source_schema_owned = read.source_schema_owned;
    read.source_schema_owned = false;
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
        .source_schema_owned = source_schema_owned,
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
    extra_sources: []const []const u8 = &.{},

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(@constCast(self.left));
        if (self.source) |source| alloc.free(@constCast(source));
        for (self.extra_sources) |source| alloc.free(@constCast(source));
        if (self.extra_sources.len > 0) alloc.free(self.extra_sources);
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
    const dml_ast = (try generatedDmlAstForParsedSql(parsed_sql)) orelse return error.UnsupportedSqlShape;
    return try insertSourceTableNamesFromGeneratedDmlAstAlloc(alloc, parsed_sql.items(), dml_ast);
}

fn generatedDmlAstForParsedSql(parsed_sql: *const tokenized.ParsedSql) !?*const generated_parser.GeneratedSqlDmlAst {
    if (parsed_sql.generatedStatementKind() != .dml) return null;
    const published = parsed_sql.writeStatementIncludingGeneratedAst() orelse return error.UnsupportedSqlShape;
    switch (parsed_sql.statement) {
        .write => |statement| {
            if (statement.kind != published.kind or statement.recursive != published.recursive) return error.UnsupportedSqlShape;
        },
        else => return error.UnsupportedSqlShape,
    }
    if (parsed_sql.generated_statement) |*generated_statement| {
        if (generated_statement.ast) |*generated_ast| {
            return switch (generated_ast.*) {
                .dml => |*dml| dml,
                else => error.UnsupportedSqlShape,
            };
        }
    }
    return error.UnsupportedSqlShape;
}

fn insertSourceTableNamesFromGeneratedDmlAstAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    dml_ast: *const generated_parser.GeneratedSqlDmlAst,
) !?InsertSourceTableNames {
    if (dml_ast.kind != .insert_select or dml_ast.cte_recursive) return null;
    if (dml_ast.cte_prefix) |cte_prefix| {
        return try insertSourceTableNamesFromGeneratedDmlCteAstAlloc(alloc, tokens, dml_ast, cte_prefix);
    }
    if (dml_ast.cte_tokens != null) return error.UnsupportedSqlShape;
    const target_tokens = dml_ast.target_table_tokens orelse return error.UnsupportedSqlShape;
    const source_read = dml_ast.source_read orelse return error.UnsupportedSqlShape;
    const source_tokens = source_read.source_tokens orelse return error.UnsupportedSqlShape;
    const source_table_tokens = source_read.source_table_tokens orelse return error.UnsupportedSqlShape;
    try validateGeneratedSimpleReadSourceTableTokens(tokens, source_tokens, source_table_tokens);

    const target = try normalizeGeneratedSingleIdentifierAlloc(alloc, tokens, target_tokens);
    errdefer alloc.free(target);
    const source = try normalizeSqlObjectIdentifierAlloc(alloc, tokens[source_table_tokens.start].text);
    errdefer alloc.free(source);
    return .{
        .target = target,
        .source = source,
    };
}

fn insertSourceTableNamesFromGeneratedDmlCteAstAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    dml_ast: *const generated_parser.GeneratedSqlDmlAst,
    cte_prefix: generated_parser.GeneratedSqlDmlCteAst,
) !InsertSourceTableNames {
    if (cte_prefix.recursive or cte_prefix.items.len == 0) return error.UnsupportedSqlShape;

    var cte_bindings = std.ArrayListUnmanaged(CteSourceBinding).empty;
    defer {
        for (cte_bindings.items) |*binding| binding.deinit(alloc);
        cte_bindings.deinit(alloc);
    }
    try appendGeneratedDmlCteReadBindingsAlloc(alloc, tokens, cte_prefix, &cte_bindings);

    const target = try writeTargetTableNameFromGeneratedDmlAstAlloc(alloc, tokens, dml_ast);
    errdefer alloc.free(target);
    const final_read = dml_ast.source_read orelse return error.UnsupportedSqlShape;
    const final_source_tokens = final_read.source_tokens orelse return error.UnsupportedSqlShape;
    const final_table_tokens = final_read.source_table_tokens orelse return error.UnsupportedSqlShape;
    try validateGeneratedSimpleReadSourceTableTokens(tokens, final_source_tokens, final_table_tokens);
    var source = try normalizeSqlObjectIdentifierAlloc(alloc, tokens[final_table_tokens.start].text);
    errdefer alloc.free(source);
    source = try resolveTableNameAgainstCtesAlloc(alloc, cte_bindings.items, source);
    return .{
        .target = target,
        .source = source,
    };
}

fn appendGeneratedDmlCteReadBindingsAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    cte_prefix: generated_parser.GeneratedSqlDmlCteAst,
    cte_bindings: *std.ArrayListUnmanaged(CteSourceBinding),
) !void {
    try validateGeneratedDmlCtePrefixItems(cte_prefix);
    for (cte_prefix.items) |item| {
        const cte_name = try normalizeGeneratedSingleIdentifierAlloc(alloc, tokens, item.name_tokens);
        var cte_name_transferred = false;
        errdefer if (!cte_name_transferred) alloc.free(cte_name);
        if (cteBindingIndex(cte_bindings.items, cte_name) != null) return error.UnsupportedSqlShape;

        var cte_source = if (item.body_read) |body_read| blk: {
            const body_source_tokens = body_read.source_tokens orelse return error.UnsupportedSqlShape;
            const body_table_tokens = body_read.source_table_tokens orelse return error.UnsupportedSqlShape;
            try validateGeneratedSimpleReadSourceTableTokens(tokens, body_source_tokens, body_table_tokens);
            break :blk try normalizeSqlObjectIdentifierAlloc(alloc, tokens[body_table_tokens.start].text);
        } else if (item.body_dml) |body_dml| blk: {
            break :blk try normalizeGeneratedDmlCteBodyTargetNameAlloc(alloc, tokens, item.body_tokens, body_dml);
        } else return error.UnsupportedSqlShape;
        errdefer alloc.free(cte_source);
        cte_source = try resolveTableNameAgainstCtesAlloc(alloc, cte_bindings.items, cte_source);
        if (cte_prefix.recursive and std.mem.eql(u8, cte_source, cte_name)) return error.UnsupportedSqlShape;

        try cte_bindings.append(alloc, .{
            .name = cte_name,
            .source = cte_source,
        });
        cte_name_transferred = true;
    }
}

fn validateGeneratedDmlCtePrefixItems(cte_prefix: generated_parser.GeneratedSqlDmlCteAst) !void {
    if (cte_prefix.count == 0 or cte_prefix.items.len == 0 or cte_prefix.count != cte_prefix.items.len) return error.UnsupportedSqlShape;
    const first = cte_prefix.items[0];
    if (!generatedTokenRangeEqual(cte_prefix.first_name_tokens, first.name_tokens) or
        !generatedTokenRangeEqual(cte_prefix.first_body_tokens, first.body_tokens))
    {
        return error.UnsupportedSqlShape;
    }
    try validateGeneratedDmlCteBodyMirror(first, cte_prefix.first_body_read, cte_prefix.first_body_dml);

    const last = cte_prefix.items[cte_prefix.items.len - 1];
    if (!generatedTokenRangeEqual(cte_prefix.last_name_tokens, last.name_tokens) or
        !generatedTokenRangeEqual(cte_prefix.last_body_tokens, last.body_tokens))
    {
        return error.UnsupportedSqlShape;
    }
    try validateGeneratedDmlCteBodyMirror(last, cte_prefix.last_body_read, cte_prefix.last_body_dml);
}

fn validateGeneratedDmlCteBodyMirror(
    item: generated_parser.GeneratedSqlDmlCteItemAst,
    mirror_read: ?generated_parser.GeneratedSqlDmlReadBodyAst,
    mirror_dml: ?*const generated_parser.GeneratedSqlDmlAst,
) !void {
    if (item.body_read) |body_read| {
        if (mirror_dml != null) return error.UnsupportedSqlShape;
        const prefix_read = mirror_read orelse return error.UnsupportedSqlShape;
        if (!generatedTokenRangeEqual(body_read.tokens, prefix_read.tokens)) return error.UnsupportedSqlShape;
        return;
    }
    if (item.body_dml) |body_dml| {
        if (mirror_read != null) return error.UnsupportedSqlShape;
        const prefix_dml = mirror_dml orelse return error.UnsupportedSqlShape;
        if (body_dml.kind != prefix_dml.kind or
            !std.meta.eql(body_dml.statement_span, prefix_dml.statement_span) or
            !std.meta.eql(body_dml.command_span, prefix_dml.command_span))
        {
            return error.UnsupportedSqlShape;
        }
        return;
    }
    if (mirror_read != null or mirror_dml != null) return error.UnsupportedSqlShape;
    return error.UnsupportedSqlShape;
}

fn generatedTokenRangeEqual(
    left: generated_parser.GeneratedSqlTokenRange,
    right: generated_parser.GeneratedSqlTokenRange,
) bool {
    return left.start == right.start and left.end == right.end;
}

fn normalizeGeneratedDmlCteBodyTargetNameAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    body_tokens: generated_parser.GeneratedSqlTokenRange,
    body_dml: *const generated_parser.GeneratedSqlDmlAst,
) ![]const u8 {
    if (body_tokens.start >= body_tokens.end or body_tokens.end > tokens.len) return error.UnsupportedSqlShape;
    const target_tokens = body_dml.target_table_tokens orelse return error.UnsupportedSqlShape;
    if (target_tokens.start >= target_tokens.end) return error.UnsupportedSqlShape;
    const rebased = generated_parser.GeneratedSqlTokenRange{
        .start = body_tokens.start + target_tokens.start,
        .end = body_tokens.start + target_tokens.end,
    };
    if (rebased.start < body_tokens.start or rebased.end > body_tokens.end) return error.UnsupportedSqlShape;
    return try normalizeGeneratedSingleIdentifierAlloc(alloc, tokens, rebased);
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
    const dml_ast = (try generatedDmlAstForParsedSql(parsed_sql)) orelse return error.UnsupportedSqlShape;
    return try recursiveInsertSourceTableNamesFromGeneratedDmlAstAlloc(alloc, parsed_sql.items(), dml_ast);
}

fn recursiveInsertSourceTableNamesFromGeneratedDmlAstAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    dml_ast: *const generated_parser.GeneratedSqlDmlAst,
) !?InsertSourceTableNames {
    if (dml_ast.kind != .insert_select or !dml_ast.cte_recursive) return null;
    const cte_prefix = dml_ast.cte_prefix orelse return error.UnsupportedSqlShape;
    if (!cte_prefix.recursive) return error.UnsupportedSqlShape;
    try validateGeneratedDmlCtePrefixItems(cte_prefix);

    var cte_bindings = std.ArrayListUnmanaged(CteSourceBinding).empty;
    defer {
        for (cte_bindings.items) |*binding| binding.deinit(alloc);
        cte_bindings.deinit(alloc);
    }

    for (cte_prefix.items) |item| {
        const cte_name = try normalizeGeneratedSingleIdentifierAlloc(alloc, tokens, item.name_tokens);
        var cte_name_transferred = false;
        errdefer if (!cte_name_transferred) alloc.free(cte_name);
        if (cteBindingIndex(cte_bindings.items, cte_name) != null) return error.UnsupportedSqlShape;

        const body_read = item.body_read orelse return error.UnsupportedSqlShape;
        const body_source_tokens = body_read.source_tokens orelse return error.UnsupportedSqlShape;
        const body_table_tokens = body_read.source_table_tokens orelse return error.UnsupportedSqlShape;
        try validateGeneratedSimpleReadSourceTableTokens(tokens, body_source_tokens, body_table_tokens);
        var cte_source = try normalizeSqlObjectIdentifierAlloc(alloc, tokens[body_table_tokens.start].text);
        errdefer alloc.free(cte_source);
        cte_source = try resolveTableNameAgainstCtesAlloc(alloc, cte_bindings.items, cte_source);
        if (std.mem.eql(u8, cte_source, cte_name)) return error.UnsupportedSqlShape;

        try cte_bindings.append(alloc, .{
            .name = cte_name,
            .source = cte_source,
        });
        cte_name_transferred = true;
    }

    const target = try writeTargetTableNameFromGeneratedDmlAstAlloc(alloc, tokens, dml_ast);
    errdefer alloc.free(target);
    const final_read = dml_ast.source_read orelse return error.UnsupportedSqlShape;
    const final_source_tokens = final_read.source_tokens orelse return error.UnsupportedSqlShape;
    const final_table_tokens = final_read.source_table_tokens orelse return error.UnsupportedSqlShape;
    try validateGeneratedSimpleReadSourceTableTokens(tokens, final_source_tokens, final_table_tokens);
    var source = try normalizeSqlObjectIdentifierAlloc(alloc, tokens[final_table_tokens.start].text);
    errdefer alloc.free(source);
    source = try resolveTableNameAgainstCtesAlloc(alloc, cte_bindings.items, source);
    return .{
        .target = target,
        .source = source,
    };
}

pub fn joinedWriteSourceTableNamesFromParsedSqlAlloc(alloc: std.mem.Allocator, parsed_sql: *const tokenized.ParsedSql) !?InsertSourceTableNames {
    const tokens = parsed_sql.items();
    const statement_kind = switch (parsed_sql.statement) {
        .write => |statement| statement.kind,
        else => return error.UnsupportedSqlShape,
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
    const dml_ast = (try generatedDmlAstForParsedSql(parsed_sql)) orelse return error.UnsupportedSqlShape;
    return try joinedWriteSourceTableNamesFromGeneratedDmlAstAlloc(alloc, tokens, dml_ast);
}

fn joinedWriteSourceTableNamesFromGeneratedDmlAstAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    dml_ast: *const generated_parser.GeneratedSqlDmlAst,
) !?InsertSourceTableNames {
    switch (dml_ast.kind) {
        .update, .delete, .merge => {},
        else => return null,
    }
    if (dml_ast.cte_prefix) |cte_prefix| {
        return try joinedWriteSourceTableNamesFromGeneratedDmlCteAstAlloc(alloc, tokens, dml_ast, cte_prefix);
    }
    if (dml_ast.cte_tokens != null or dml_ast.cte_recursive) return error.UnsupportedSqlShape;
    const source = try joinedWriteSourceNameFromGeneratedDmlAstAlloc(alloc, tokens, dml_ast) orelse return null;
    errdefer alloc.free(source);

    const target = try writeTargetTableNameFromGeneratedDmlAstAlloc(alloc, tokens, dml_ast);
    errdefer alloc.free(target);
    return .{
        .target = target,
        .source = source,
    };
}

fn joinedWriteSourceNameFromGeneratedDmlAstAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    dml_ast: *const generated_parser.GeneratedSqlDmlAst,
) !?[]const u8 {
    if (dml_ast.source_read) |source_read| {
        const source_tokens = source_read.source_tokens orelse return error.UnsupportedSqlShape;
        const source_table_tokens = source_read.source_table_tokens orelse return error.UnsupportedSqlShape;
        try validateGeneratedSimpleReadSourceTableTokens(tokens, source_tokens, source_table_tokens);
        return try normalizeSqlObjectIdentifierAlloc(alloc, tokens[source_table_tokens.start].text);
    }
    if (dml_ast.semijoin_source_table_tokens) |source_table| {
        if (source_table.end != source_table.start + 1 or source_table.end > tokens.len or tokens[source_table.start].kind != .identifier) return error.UnsupportedSqlShape;
        return try normalizeSqlObjectIdentifierAlloc(alloc, tokens[source_table.start].text);
    }
    if (dml_ast.mutation_join_source) return error.UnsupportedSqlShape;
    return null;
}

fn joinedWriteSourceTableNamesFromGeneratedDmlCteAstAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    dml_ast: *const generated_parser.GeneratedSqlDmlAst,
    cte_prefix: generated_parser.GeneratedSqlDmlCteAst,
) !?InsertSourceTableNames {
    var cte_bindings = std.ArrayListUnmanaged(CteSourceBinding).empty;
    defer {
        for (cte_bindings.items) |*binding| binding.deinit(alloc);
        cte_bindings.deinit(alloc);
    }
    try appendGeneratedDmlCteReadBindingsAlloc(alloc, tokens, cte_prefix, &cte_bindings);

    var source = try joinedWriteSourceNameFromGeneratedDmlAstAlloc(alloc, tokens, dml_ast) orelse return null;
    errdefer alloc.free(source);

    var target = try writeTargetTableNameFromGeneratedDmlAstAlloc(alloc, tokens, dml_ast);
    errdefer alloc.free(target);
    target = try resolveTableNameAgainstCtesAlloc(alloc, cte_bindings.items, target);
    source = try resolveTableNameAgainstCtesAlloc(alloc, cte_bindings.items, source);
    return .{
        .target = target,
        .source = source,
    };
}

pub fn writeTargetTableNameFromParsedSqlAlloc(alloc: std.mem.Allocator, parsed_sql: *const tokenized.ParsedSql) ![]const u8 {
    const tokens = parsed_sql.items();
    const dml_ast = (try generatedDmlAstForParsedSql(parsed_sql)) orelse return error.UnsupportedSqlShape;
    return try writeTargetTableNameFromGeneratedDmlAstAlloc(alloc, tokens, dml_ast);
}

fn writeTargetTableNameFromGeneratedDmlAstAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    dml_ast: *const generated_parser.GeneratedSqlDmlAst,
) ![]const u8 {
    const command_index = try generatedDmlCommandTokenIndex(tokens, dml_ast);
    const expected_target_start = try generatedDmlExpectedTargetStart(tokens, dml_ast, command_index);
    const target_tokens = dml_ast.target_table_tokens orelse return error.UnsupportedSqlShape;
    if (target_tokens.start != expected_target_start) return error.UnsupportedSqlShape;
    return try normalizeGeneratedSingleIdentifierAlloc(alloc, tokens, target_tokens);
}

fn generatedDmlCommandTokenIndex(tokens: []const Token, dml_ast: *const generated_parser.GeneratedSqlDmlAst) !usize {
    const command_index = if (dml_ast.cte_tokens) |cte_tokens| blk: {
        if (cte_tokens.start >= cte_tokens.end or cte_tokens.end >= tokens.len) return error.UnsupportedSqlShape;
        break :blk cte_tokens.end;
    } else 0;
    if (command_index >= tokens.len or !generatedDmlCommandTokenMatchesKind(tokens[command_index], dml_ast.kind)) {
        return error.UnsupportedSqlShape;
    }
    return command_index;
}

fn generatedDmlExpectedTargetStart(
    tokens: []const Token,
    dml_ast: *const generated_parser.GeneratedSqlDmlAst,
    command_index: usize,
) !usize {
    var index: usize = switch (dml_ast.kind) {
        .insert_values, .insert_select => blk: {
            if (command_index + 1 >= tokens.len or !tokens[command_index + 1].matchesKeywordTag(.into)) return error.UnsupportedSqlShape;
            break :blk command_index + 2;
        },
        .update => command_index + 1,
        .delete => blk: {
            if (command_index + 1 >= tokens.len or !tokens[command_index + 1].matchesKeywordTag(.from)) return error.UnsupportedSqlShape;
            break :blk command_index + 2;
        },
        .truncate => blk: {
            var target_index = command_index + 1;
            if (target_index < tokens.len and tokens[target_index].matchesKeywordTag(.table)) target_index += 1;
            break :blk target_index;
        },
        .merge => blk: {
            if (command_index + 1 >= tokens.len or !tokens[command_index + 1].matchesKeywordTag(.into)) return error.UnsupportedSqlShape;
            break :blk command_index + 2;
        },
    };
    _ = consumeKeyword(tokens, &index, .only);
    if (index >= tokens.len) return error.UnsupportedSqlShape;
    return index;
}

fn generatedDmlCommandTokenMatchesKind(token: Token, kind: generated_parser.GeneratedSqlDmlKind) bool {
    return switch (kind) {
        .insert_values, .insert_select => token.matchesKeywordTag(.insert),
        .update => token.matchesKeywordTag(.update),
        .delete => token.matchesKeywordTag(.delete),
        .truncate => token.matchesKeywordTag(.truncate),
        .merge => token.matchesKeywordTag(.merge),
    };
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
    try requireParsedCatalogWriteStatementIncludingGeneratedAst(parsed_sql);
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
    try requireParsedCatalogWriteStatementIncludingGeneratedAst(parsed_sql);
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
    try rejectDocumentSqlViewWriteTargetAlloc(alloc, catalog, target_table_name, session);
    out.target_binding = sourceBindingForCatalogTableWithSessionAlloc(alloc, catalog, target_table_name, session, false) catch |err| switch (err) {
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
            error.UnsupportedSqlShape => if (parsed_sql.generatedStatementKind() == .dml) return err,
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
    try requireParsedCatalogReadStatementIncludingGeneratedAst(parsed_sql);
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
        out.target_binding = try sourceBindingForCatalogTableWithSessionAlloc(alloc, catalog, tables.left, session, true);
        try appendBoundCatalogObjectForBindingAlloc(alloc, &bound_objects, .target, out.target_binding.?);
        if (!std.mem.eql(u8, tables.left, tables.source)) {
            out.source_binding = try sourceBindingForCatalogTableWithSessionAlloc(alloc, catalog, tables.source, session, true);
            try appendBoundCatalogObjectForBindingAlloc(alloc, &bound_objects, .source, out.source_binding.?);
            out.source_schema = switch (out.source_binding.?) {
                .relational => |binding| binding.schema,
                .document => |binding| binding.schema,
                .lake => |binding| binding.schema,
            };
        }
        if (tables.extra_sources.len != 0) {
            if (out.source_schema_owned) return error.InvalidSqlCatalog;
            var merged_schema = try mergedReadSourceSchemaForBoundTablesAlloc(
                alloc,
                out.target_binding.?,
                out.source_binding,
            );
            errdefer runtime_schema.freeSchema(alloc, merged_schema);
            for (tables.extra_sources) |extra_source| {
                if (std.mem.eql(u8, extra_source, tables.left) or std.mem.eql(u8, extra_source, tables.source)) continue;
                var extra_binding = try sourceBindingForCatalogTableWithSessionAlloc(alloc, catalog, extra_source, session, true);
                defer deinitSqlSourceBinding(alloc, &extra_binding);
                try appendBoundCatalogObjectForBindingAlloc(alloc, &bound_objects, .source, extra_binding);
                try appendReadSourceBindingColumnsAlloc(alloc, &merged_schema, extra_binding);
            }
            if (out.source_schema != null and !out.source_schema_owned) out.source_schema = null;
            out.source_schema = merged_schema;
            out.source_schema_owned = true;
        }
    }
    out.bound_objects = try bound_objects.toOwnedSlice(alloc);
    bound_objects_transferred = true;
    out.authorization = try boundSqlAuthorizationForObjectsAlloc(alloc, out.bound_objects, authorization_options, .read);
    return out;
}

fn mergedReadSourceSchemaForBoundTablesAlloc(
    alloc: std.mem.Allocator,
    target_binding: source_binding.SqlSourceBinding,
    maybe_source_binding: ?source_binding.SqlSourceBinding,
) !runtime_schema.TableSchema {
    const target_schema = switch (target_binding) {
        .relational => |relational| relational.schema,
        .document => return error.InvalidSqlCatalog,
        .lake => return error.InvalidSqlCatalog,
    };
    const default_type = try alloc.dupe(u8, target_schema.default_type);
    errdefer alloc.free(default_type);
    const ttl_field = try alloc.dupe(u8, target_schema.ttl_field);
    errdefer alloc.free(ttl_field);
    var schema = runtime_schema.TableSchema{
        .version = target_schema.version,
        .default_type = default_type,
        .ttl_duration_ns = target_schema.ttl_duration_ns,
        .ttl_field = ttl_field,
        .enforce_types = target_schema.enforce_types,
        .storage_mode = .relational,
        .relational_columns = try ddl_plan.cloneDdlRelationalColumns(alloc, target_schema.relational_columns),
        .primary_key = if (target_schema.primary_key) |primary_key|
            try cloneReadSourcePrimaryKeyAlloc(alloc, primary_key)
        else
            null,
        .system_versioned = target_schema.system_versioned,
    };
    errdefer runtime_schema.freeSchema(alloc, schema);
    if (maybe_source_binding) |source| try appendReadSourceBindingColumnsAlloc(alloc, &schema, source);
    return schema;
}

fn cloneReadSourcePrimaryKeyAlloc(
    alloc: std.mem.Allocator,
    primary_key: runtime_schema.PrimaryKey,
) !runtime_schema.PrimaryKey {
    const out = runtime_schema.PrimaryKey{
        .name = if (primary_key.name) |name| try alloc.dupe(u8, name) else null,
        .columns = try cloneStringSliceAlloc(alloc, primary_key.columns),
        .include_columns = try cloneStringSliceAlloc(alloc, primary_key.include_columns),
        .without_overlaps_period = if (primary_key.without_overlaps_period) |period| try alloc.dupe(u8, period) else null,
        .deferrable = primary_key.deferrable,
        .timing = primary_key.timing,
    };
    errdefer {
        if (out.name) |name| alloc.free(name);
        for (out.columns) |column| alloc.free(@constCast(column));
        if (out.columns.len > 0) alloc.free(out.columns);
        for (out.include_columns) |column| alloc.free(@constCast(column));
        if (out.include_columns.len > 0) alloc.free(out.include_columns);
        if (out.without_overlaps_period) |period| alloc.free(period);
    }
    return out;
}

fn cloneStringSliceAlloc(
    alloc: std.mem.Allocator,
    values: []const []const u8,
) ![]const []const u8 {
    if (values.len == 0) return &.{};
    const out = try alloc.alloc([]const u8, values.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |value| alloc.free(@constCast(value));
        alloc.free(out);
    }
    for (values, 0..) |value, index| {
        out[index] = try alloc.dupe(u8, value);
        initialized += 1;
    }
    return out;
}

fn appendReadSourceBindingColumnsAlloc(
    alloc: std.mem.Allocator,
    target: *runtime_schema.TableSchema,
    binding: source_binding.SqlSourceBinding,
) !void {
    const schema = switch (binding) {
        .relational => |relational| relational.schema,
        .document => return error.InvalidSqlCatalog,
        .lake => return error.InvalidSqlCatalog,
    };
    if (schema.storage_mode != .relational) return error.InvalidSqlCatalog;
    for (schema.relational_columns) |column| {
        if (relationalColumnIndex(target.relational_columns, column.name) != null) continue;
        const len = target.relational_columns.len;
        const out = try alloc.alloc(runtime_schema.RelationalColumn, len + 1);
        errdefer alloc.free(out);
        @memcpy(out[0..len], target.relational_columns);
        out[len] = try ddl_plan.cloneDdlRelationalColumn(alloc, column);
        if (len > 0) alloc.free(target.relational_columns);
        target.relational_columns = out;
    }
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
    var snapshot = try catalog.adminSnapshot();
    defer catalog.freeAdminSnapshot(&snapshot);
    const resolved = try resolvedExistingCatalogTableForObjectNameAlloc(alloc, &snapshot, cluster.table_name, session) orelse return error.InvalidSqlCatalog;
    defer deinitCatalogTableRef(alloc, resolved.target);
    const table = resolved.table;
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
    function_bindings: expr_row_parse.SqlFunctionBindings,
) !CatalogBoundDdlPlanFacts {
    return try resolveDdlCatalogFactsFromParsedSqlWithSessionAndAuthorizationAlloc(alloc, parsed_sql, catalog, session, function_bindings, .{});
}

fn resolveDdlCatalogFactsFromParsedSqlWithSessionAndAuthorizationAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const tokenized.ParsedSql,
    catalog: table_catalog.CatalogSource,
    session: catalog_resources.SqlCatalogSession,
    function_bindings: expr_row_parse.SqlFunctionBindings,
    authorization_options: BoundSqlAuthorizationOptions,
) !CatalogBoundDdlPlanFacts {
    var logical_plan = try ddl_plan.logicalDdlPlanParsedSqlWithFunctionBindingsAlloc(alloc, parsed_sql, function_bindings);
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
    try requireParsedCatalogReadStatementIncludingGeneratedAst(parsed_sql);
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
    function_bindings: expr_row_parse.SqlFunctionBindings,
) !BoundSqlStatement {
    return try bindDdlStatementWithCatalogSessionFunctionBindingsAndAuthorizationAlloc(alloc, parsed_sql, catalog, session, function_bindings, .{});
}

pub fn bindDdlStatementWithCatalogSessionFunctionBindingsAndAuthorizationAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const tokenized.ParsedSql,
    catalog: table_catalog.CatalogSource,
    session: catalog_resources.SqlCatalogSession,
    function_bindings: expr_row_parse.SqlFunctionBindings,
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

pub fn readSourceTableNamesFromParsedSqlAlloc(alloc: std.mem.Allocator, parsed_sql: *const tokenized.ParsedSql) !?ReadSourceTableNames {
    try requireParsedCatalogReadStatementIncludingGeneratedAst(parsed_sql);
    const read_ast = (try generatedReadAstForParsedSql(parsed_sql)) orelse return error.UnsupportedSqlShape;
    return try readSourceTableNamesFromGeneratedReadAstAlloc(alloc, parsed_sql.items(), read_ast);
}

fn generatedReadAstForParsedSql(parsed_sql: *const tokenized.ParsedSql) !?*const generated_parser.GeneratedSqlReadAst {
    if (parsed_sql.generatedStatementKind() != .read) return null;
    _ = parsed_sql.generatedReadStatementKind() orelse return error.UnsupportedSqlShape;
    if (parsed_sql.generated_statement) |*generated_statement| {
        if (generated_statement.ast) |*generated_ast| {
            return switch (generated_ast.*) {
                .read => |read| blk: {
                    try lowering_context.validateGeneratedReadAstForStatement(parsed_sql.items(), read);
                    break :blk read;
                },
                else => error.UnsupportedSqlShape,
            };
        }
    }
    return error.UnsupportedSqlShape;
}

fn readSourceTableNamesFromGeneratedReadAstAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    read_ast: *const generated_parser.GeneratedSqlReadAst,
) !?ReadSourceTableNames {
    if (read_ast.cte_tokens != null) {
        return try readSourceTableNamesFromGeneratedCteReadAstAlloc(alloc, tokens, read_ast);
    }
    if ((read_ast.source_antfly_function_items.len != 0 and read_ast.source_graph_function_items.len == 0) or
        (read_ast.join_items.len != 0 and read_ast.set_operation_tokens != null))
    {
        return null;
    }
    if (read_ast.source_graph_function_items.len != 0) {
        if (read_ast.join_items.len != 0 or read_ast.set_operation_tokens != null) return null;
        const table = try generatedGraphTableFunctionSourceNameAlloc(alloc, tokens, read_ast.source_graph_function_items);
        errdefer alloc.free(table);
        return .{ .left = table, .source = try alloc.dupe(u8, table) };
    }
    var tables = try readSourceTableNamesFromGeneratedReadBodyAlloc(
        alloc,
        tokens,
        read_ast.source_tokens,
        read_ast.source_table_tokens,
        read_ast.join_items,
        read_ast.set_operation_tokens,
        read_ast.set_operation,
    ) orelse return null;
    errdefer tables.deinit(alloc);
    const source = tables.source orelse {
        tables.deinit(alloc);
        return null;
    };
    tables.source = null;
    const extra_sources = tables.extra_sources;
    tables.extra_sources = &.{};
    return .{ .left = tables.left, .source = source, .extra_sources = extra_sources };
}

fn generatedGraphTableFunctionSourceNameAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    graph_items: []const generated_parser.GeneratedSqlGraphTableFunctionAst,
) ![]const u8 {
    if (graph_items.len != 1) return error.UnsupportedSqlShape;
    const table_name_value_tokens = graph_items[0].table_name_value_tokens orelse return error.UnsupportedSqlShape;
    return try normalizeGeneratedStringLiteralIdentifierAlloc(alloc, tokens, table_name_value_tokens);
}

fn normalizeGeneratedStringLiteralIdentifierAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    range: generated_parser.GeneratedSqlTokenRange,
) ![]const u8 {
    if (range.start >= range.end or range.end > tokens.len or range.end != range.start + 1) return error.UnsupportedSqlShape;
    const token = tokens[range.start];
    if (token.kind != .string or token.text.len == 0) return error.UnsupportedSqlShape;
    return try normalizeSqlObjectIdentifierAlloc(alloc, token.text);
}

fn readSourceTableNamesFromGeneratedReadBodyAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    maybe_source_tokens: ?generated_parser.GeneratedSqlTokenRange,
    maybe_table_tokens: ?generated_parser.GeneratedSqlTokenRange,
    join_items: []const generated_parser.GeneratedSqlJoinAst,
    maybe_set_operation_tokens: ?generated_parser.GeneratedSqlTokenRange,
    set_operation: generated_parser.GeneratedSqlSetOperationAst,
) !?SelectReadTableNames {
    if (join_items.len != 0) {
        const join = join_items[0];
        const left = try normalizeGeneratedSourceRangeTableNameAlloc(alloc, tokens, join.left_tokens);
        errdefer alloc.free(left);
        if (generatedSourceRangeStartsWithUnnest(tokens, join.right_tokens) or
            generatedSourceRangeStartsWithLateralUnnest(tokens, join.right_tokens))
        {
            const source = try alloc.dupe(u8, left);
            errdefer alloc.free(source);
            return .{ .left = left, .source = source };
        }
        const source = if (join.right_lateral_subquery_read_ast != null or join.right_lateral_subquery_tokens != null)
            try generatedLateralJoinRightSourceNameAlloc(alloc, tokens, join)
        else
            try normalizeGeneratedSourceRangeTableNameAlloc(alloc, tokens, join.right_tokens);
        errdefer alloc.free(source);
        if (join.kind == .right or generatedJoinOperatorIsRight(tokens, join)) {
            return .{ .left = source, .source = left };
        }
        return .{ .left = left, .source = source };
    }

    const source_tokens = maybe_source_tokens orelse return null;
    if (maybe_table_tokens == null and generatedSourceRangeContainsUnnest(tokens, source_tokens)) {
        const left = try normalizeGeneratedSourceRangeTableNameAlloc(alloc, tokens, source_tokens);
        errdefer alloc.free(left);
        const source = try alloc.dupe(u8, left);
        errdefer alloc.free(source);
        return .{ .left = left, .source = source };
    }
    const table_tokens = maybe_table_tokens orelse return null;
    try validateGeneratedSimpleReadSourceTableTokens(tokens, source_tokens, table_tokens);
    const left = try normalizeSqlObjectIdentifierAlloc(alloc, tokens[table_tokens.start].text);
    errdefer alloc.free(left);
    if (maybe_set_operation_tokens != null) {
        const source = try generatedSetOperationRightSourceTableNameAlloc(alloc, tokens, set_operation);
        errdefer alloc.free(source);
        const extra_sources = try generatedSetOperationRightExtraSourceTableNamesAlloc(alloc, tokens, set_operation, source);
        errdefer {
            for (extra_sources) |extra| alloc.free(@constCast(extra));
            if (extra_sources.len > 0) alloc.free(extra_sources);
        }
        return .{ .left = left, .source = source, .extra_sources = extra_sources };
    }
    const source = try alloc.dupe(u8, left);
    errdefer alloc.free(source);
    return .{ .left = left, .source = source };
}

fn generatedJoinOperatorIsRight(tokens: []const Token, join: generated_parser.GeneratedSqlJoinAst) bool {
    if (join.operator_tokens.start >= join.operator_tokens.end or join.operator_tokens.end > tokens.len) return false;
    for (tokens[join.operator_tokens.start..join.operator_tokens.end]) |token| {
        if (token.matchesKeywordTag(.right)) return true;
    }
    return false;
}

fn generatedSourceRangeStartsWithUnnest(tokens: []const Token, source_tokens: generated_parser.GeneratedSqlTokenRange) bool {
    if (source_tokens.start >= source_tokens.end or source_tokens.end > tokens.len) return false;
    const token = tokens[source_tokens.start];
    return token.kind == .identifier and std.ascii.eqlIgnoreCase(token.text, "unnest");
}

fn generatedSourceRangeStartsWithLateralUnnest(tokens: []const Token, source_tokens: generated_parser.GeneratedSqlTokenRange) bool {
    if (source_tokens.start + 2 >= source_tokens.end or source_tokens.end > tokens.len) return false;
    return tokens[source_tokens.start].matchesKeywordTag(.lateral) and
        tokens[source_tokens.start + 1].kind == .identifier and
        std.ascii.eqlIgnoreCase(tokens[source_tokens.start + 1].text, "unnest") and
        tokens[source_tokens.start + 2].kind == .lparen;
}

fn generatedSourceRangeContainsUnnest(tokens: []const Token, source_tokens: generated_parser.GeneratedSqlTokenRange) bool {
    if (source_tokens.start >= source_tokens.end or source_tokens.end > tokens.len) return false;
    for (tokens[source_tokens.start..source_tokens.end]) |token| {
        if (token.kind == .identifier and std.ascii.eqlIgnoreCase(token.text, "unnest")) return true;
    }
    return false;
}

fn normalizeGeneratedSourceRangeTableNameAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    source_tokens: generated_parser.GeneratedSqlTokenRange,
) ![]const u8 {
    if (source_tokens.start >= source_tokens.end or source_tokens.end > tokens.len) return error.UnsupportedSqlShape;
    var table_start = source_tokens.start;
    if (tokens[table_start].matchesKeywordTag(.lateral)) {
        if (table_start + 1 >= source_tokens.end or tokens[table_start + 1].kind != .lparen) return error.UnsupportedSqlShape;
        const close_index = findMatchingRParenIndex(tokens, table_start + 1) orelse return error.UnsupportedSqlShape;
        if (close_index >= source_tokens.end) return error.UnsupportedSqlShape;
        const from_index = findTopLevelKeyword(tokens[table_start + 2 .. close_index], .from) orelse return error.UnsupportedSqlShape;
        table_start = table_start + 2 + from_index + 1;
    }
    _ = consumeKeyword(tokens, &table_start, .only);
    if (table_start >= source_tokens.end or tokens[table_start].kind != .identifier) return error.UnsupportedSqlShape;
    return try normalizeSqlObjectIdentifierAlloc(alloc, tokens[table_start].text);
}

fn generatedLateralJoinRightSourceNameAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    join: generated_parser.GeneratedSqlJoinAst,
) ![]const u8 {
    const right = join.right_tokens;
    if (right.start >= right.end or right.end > tokens.len) return error.UnsupportedSqlShape;
    if (!tokens[right.start].matchesKeywordTag(.lateral)) return error.UnsupportedSqlShape;
    const subquery = join.right_lateral_subquery_tokens orelse return error.UnsupportedSqlShape;
    if (subquery.start <= right.start + 1 or subquery.end >= right.end or subquery.end > tokens.len) return error.UnsupportedSqlShape;
    if (tokens[subquery.start - 1].kind != .lparen or tokens[subquery.end].kind != .rparen) return error.UnsupportedSqlShape;
    const alias = join.right_lateral_alias_tokens orelse return error.UnsupportedSqlShape;
    const alias_name = join.right_lateral_alias_name_tokens orelse return error.UnsupportedSqlShape;
    if (alias.start <= subquery.end or alias.end != right.end or alias_name.start < alias.start or alias_name.end > alias.end) return error.UnsupportedSqlShape;

    const child = join.right_lateral_subquery_read_ast orelse return error.UnsupportedSqlShape;
    return try generatedReadBodySourceNameAlloc(
        alloc,
        tokens[subquery.start..subquery.end],
        child.source_tokens,
        child.source_table_tokens,
        child.set_operation_tokens,
        child.set_operation,
    );
}

fn ensureGeneratedSetOperationSourceMatches(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    source_name: []const u8,
    set_operation: generated_parser.GeneratedSqlSetOperationAst,
) !void {
    const right_source = try generatedSetOperationRightSourceTableNameAlloc(alloc, tokens, set_operation);
    defer alloc.free(right_source);
    if (!std.mem.eql(u8, source_name, right_source)) return error.UnsupportedSqlShape;
}

fn generatedSetOperationRightSourceTableNameAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    set_operation: generated_parser.GeneratedSqlSetOperationAst,
) anyerror![]const u8 {
    const right_source_tokens = set_operation.right_source_tokens orelse return error.UnsupportedSqlShape;
    if (set_operation.right_join_items.len != 0) {
        const root_index = set_operation.right_join_tree_root_index orelse return error.UnsupportedSqlShape;
        if (root_index != set_operation.right_join_items.len - 1 or set_operation.right_join_tree_depth != set_operation.right_join_items.len) return error.UnsupportedSqlShape;
        const first_join = set_operation.right_join_items[0];
        if (first_join.tokens.start != right_source_tokens.start) return error.UnsupportedSqlShape;
        const join = set_operation.right_join_items[set_operation.right_join_items.len - 1];
        if (join.tokens.end != right_source_tokens.end) return error.UnsupportedSqlShape;
        if (set_operation.right_join_items.len > 1) {
            for (set_operation.right_join_items) |item| {
                if (item.kind != .inner or item.condition_kind != .on) return error.UnsupportedSqlShape;
                if (item.right_lateral_subquery_read_ast != null or item.right_lateral_subquery_tokens != null) return error.UnsupportedSqlShape;
                if (item.left_child_index != null and item.left_child_index.? + 1 != item.tree_index) return error.UnsupportedSqlShape;
            }
        }
        if (join.kind == .right or generatedJoinOperatorIsRight(tokens, join)) {
            return try normalizeGeneratedSourceRangeTableNameAlloc(alloc, tokens, join.left_tokens);
        }
        return if (join.right_lateral_subquery_read_ast != null or join.right_lateral_subquery_tokens != null)
            try generatedLateralJoinRightSourceNameAlloc(alloc, tokens, join)
        else
            try normalizeGeneratedSourceRangeTableNameAlloc(alloc, tokens, join.right_tokens);
    }
    const right_source_table_tokens = set_operation.right_source_table_tokens orelse return error.UnsupportedSqlShape;
    try validateGeneratedSimpleReadSourceTableTokens(tokens, right_source_tokens, right_source_table_tokens);
    return try normalizeSqlObjectIdentifierAlloc(alloc, tokens[right_source_table_tokens.start].text);
}

fn generatedSetOperationRightExtraSourceTableNamesAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    set_operation: generated_parser.GeneratedSqlSetOperationAst,
    primary_source: []const u8,
) ![]const []const u8 {
    if (set_operation.right_join_items.len == 0) return &.{};
    const root_index = set_operation.right_join_tree_root_index orelse return error.UnsupportedSqlShape;
    if (root_index != set_operation.right_join_items.len - 1 or set_operation.right_join_tree_depth != set_operation.right_join_items.len) return error.UnsupportedSqlShape;
    for (set_operation.right_join_items) |join| {
        if (join.kind != .inner or join.condition_kind != .on) return &.{};
        if (join.right_lateral_subquery_read_ast != null or join.right_lateral_subquery_tokens != null) return &.{};
    }

    var names = std.ArrayListUnmanaged([]const u8).empty;
    errdefer {
        for (names.items) |name| alloc.free(name);
        names.deinit(alloc);
    }
    const first_join = set_operation.right_join_items[0];
    try appendGeneratedSetOperationExtraSourceIfNeededAlloc(
        alloc,
        tokens,
        &names,
        first_join.left_table_tokens orelse first_join.left_tokens,
        primary_source,
    );
    for (set_operation.right_join_items) |join| {
        if (join.left_child_index != null and join.left_child_index.? + 1 != join.tree_index) return error.UnsupportedSqlShape;
        try appendGeneratedSetOperationExtraSourceIfNeededAlloc(
            alloc,
            tokens,
            &names,
            join.right_table_tokens orelse join.right_tokens,
            primary_source,
        );
    }
    return try names.toOwnedSlice(alloc);
}

fn appendGeneratedSetOperationExtraSourceIfNeededAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    names: *std.ArrayListUnmanaged([]const u8),
    source_tokens: generated_parser.GeneratedSqlTokenRange,
    primary_source: []const u8,
) !void {
    const source = try normalizeGeneratedSourceRangeTableNameAlloc(alloc, tokens, source_tokens);
    errdefer alloc.free(source);
    if (std.mem.eql(u8, source, primary_source)) {
        alloc.free(source);
        return;
    }
    for (names.items) |existing| {
        if (std.mem.eql(u8, existing, source)) {
            alloc.free(source);
            return;
        }
    }
    try names.append(alloc, source);
}

fn generatedReadBodySourceNameAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    maybe_source_tokens: ?generated_parser.GeneratedSqlTokenRange,
    maybe_table_tokens: ?generated_parser.GeneratedSqlTokenRange,
    maybe_set_operation_tokens: ?generated_parser.GeneratedSqlTokenRange,
    set_operation: generated_parser.GeneratedSqlSetOperationAst,
) ![]const u8 {
    const source_tokens = maybe_source_tokens orelse return error.UnsupportedSqlShape;
    const table_tokens = maybe_table_tokens orelse return error.UnsupportedSqlShape;
    try validateGeneratedSimpleReadSourceTableTokens(tokens, source_tokens, table_tokens);
    const source = try normalizeSqlObjectIdentifierAlloc(alloc, tokens[table_tokens.start].text);
    errdefer alloc.free(source);
    if (maybe_set_operation_tokens != null) try ensureGeneratedSetOperationSourceMatches(alloc, tokens, source, set_operation);
    return source;
}

fn readSourceTableNamesFromGeneratedCteReadAstAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    read_ast: *const generated_parser.GeneratedSqlReadAst,
) !?ReadSourceTableNames {
    if ((read_ast.source_antfly_function_items.len != 0 and read_ast.source_graph_function_items.len == 0) or
        (read_ast.join_items.len != 0 and read_ast.set_operation_tokens != null))
    {
        return null;
    }

    var cte_bindings = std.ArrayListUnmanaged(CteSourceBinding).empty;
    defer {
        for (cte_bindings.items) |*binding| binding.deinit(alloc);
        cte_bindings.deinit(alloc);
    }

    for (read_ast.cte_items) |cte| {
        const cte_name = try normalizeGeneratedSingleIdentifierAlloc(alloc, tokens, cte.name_tokens);
        var cte_name_transferred = false;
        errdefer if (!cte_name_transferred) alloc.free(cte_name);
        if (cteBindingIndex(cte_bindings.items, cte_name) != null) return error.UnsupportedSqlShape;

        var cte_source = if (cte.body_source_graph_function_items.len != 0) blk: {
            if (cte.body_join_items.len != 0) return error.UnsupportedSqlShape;
            break :blk try generatedGraphTableFunctionSourceNameAlloc(alloc, tokens, cte.body_source_graph_function_items);
        } else blk: {
            if (cte.body_source_antfly_function_items.len != 0) return error.UnsupportedSqlShape;
            var body_tables = try readSourceTableNamesFromGeneratedReadBodyAlloc(
                alloc,
                tokens,
                cte.body_source_tokens,
                cte.body_source_table_tokens,
                cte.body_join_items,
                if (!read_ast.cte_recursive) cte.body_set_operation_tokens else null,
                cte.body_set_operation,
            ) orelse return error.UnsupportedSqlShape;
            defer body_tables.deinit(alloc);
            break :blk try alloc.dupe(u8, body_tables.left);
        };
        errdefer alloc.free(cte_source);
        cte_source = try resolveTableNameAgainstCtesAlloc(alloc, cte_bindings.items, cte_source);
        if (read_ast.cte_recursive and std.mem.eql(u8, cte_source, cte_name)) return error.UnsupportedSqlShape;

        try cte_bindings.append(alloc, .{
            .name = cte_name,
            .source = cte_source,
        });
        cte_name_transferred = true;
    }

    var final_tables = try readSourceTableNamesFromGeneratedReadBodyAlloc(
        alloc,
        tokens,
        read_ast.source_tokens,
        read_ast.source_table_tokens,
        read_ast.join_items,
        read_ast.set_operation_tokens,
        read_ast.set_operation,
    ) orelse return null;
    errdefer final_tables.deinit(alloc);
    try resolveSelectReadTablesAgainstCtes(alloc, cte_bindings.items, &final_tables);
    const source = final_tables.source orelse {
        final_tables.deinit(alloc);
        return null;
    };
    final_tables.source = null;
    const extra_sources = final_tables.extra_sources;
    final_tables.extra_sources = &.{};
    return .{ .left = final_tables.left, .source = source, .extra_sources = extra_sources };
}

fn validateGeneratedSimpleReadSourceTableTokens(
    tokens: []const Token,
    source_tokens: generated_parser.GeneratedSqlTokenRange,
    table_tokens: generated_parser.GeneratedSqlTokenRange,
) !void {
    if (source_tokens.start >= source_tokens.end or source_tokens.end > tokens.len) return error.UnsupportedSqlShape;
    if (table_tokens.start >= table_tokens.end or table_tokens.end > source_tokens.end) return error.UnsupportedSqlShape;
    if (table_tokens.end != table_tokens.start + 1) return error.UnsupportedSqlShape;
    if (table_tokens.start != source_tokens.start) {
        if (table_tokens.start != source_tokens.start + 1 or !tokens[source_tokens.start].matchesKeywordTag(.only)) {
            return error.UnsupportedSqlShape;
        }
    }
    if (tokens[table_tokens.start].kind != .identifier) return error.UnsupportedSqlShape;
}

fn normalizeGeneratedSingleIdentifierAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    range: generated_parser.GeneratedSqlTokenRange,
) ![]const u8 {
    if (range.start >= range.end or range.end > tokens.len) return error.UnsupportedSqlShape;
    if (range.end != range.start + 1) return error.UnsupportedSqlShape;
    if (tokens[range.start].kind != .identifier) return error.UnsupportedSqlShape;
    return try normalizeSqlObjectIdentifierAlloc(alloc, tokens[range.start].text);
}

fn normalizeSqlObjectIdentifierAlloc(alloc: std.mem.Allocator, identifier: []const u8) ![]const u8 {
    return try grammar.normalizeSqlObjectIdentifierAlloc(alloc, identifier);
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
    const extra_sources = @constCast(tables.extra_sources);
    for (extra_sources, 0..) |source, index| {
        extra_sources[index] = try resolveTableNameAgainstCtesAlloc(alloc, bindings, source);
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

    const fallback_search_path = [_][]const u8{ "tenant_missing", "public" };
    const fallback_runtime = try runtimeSchemaForCatalogTableWithSessionAlloc(alloc, catalog.iface(), "usage_records", .{
        .search_path = fallback_search_path[0..],
    });
    defer runtime_schema.freeSchema(alloc, fallback_runtime);
    try std.testing.expectEqual(runtime_schema.StorageMode.relational, fallback_runtime.storage_mode);
    try std.testing.expectEqual(@as(usize, 2), fallback_runtime.relational_columns.len);

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
    if (single_table_sql.generated_statement) |*generated_statement| {
        if (generated_statement.ast) |*generated_ast| {
            generated_ast.deinit(alloc);
            generated_statement.ast = null;
        } else return error.TestUnexpectedResult;
    } else return error.TestUnexpectedResult;
    try std.testing.expectError(error.UnsupportedSqlShape, readSourceTableNamesFromParsedSqlAlloc(alloc, &single_table_sql));

    var alias_source_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT u.id FROM usage_records AS u WHERE u.status = 'open'",
    );
    defer alias_source_sql.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.read, alias_source_sql.generatedStatementKind().?);
    var alias_source = (try readSourceTableNamesFromParsedSqlAlloc(alloc, &alias_source_sql)).?;
    defer alias_source.deinit(alloc);
    try std.testing.expectEqualStrings("usage_records", alias_source.left);
    try std.testing.expectEqualStrings("usage_records", alias_source.source);

    var set_operation_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT id FROM usage_records UNION SELECT id FROM usage_archive",
    );
    defer set_operation_sql.deinit(alloc);
    var set_operation = (try readSourceTableNamesFromParsedSqlAlloc(alloc, &set_operation_sql)).?;
    defer set_operation.deinit(alloc);
    try std.testing.expectEqualStrings("usage_records", set_operation.left);
    try std.testing.expectEqualStrings("usage_archive", set_operation.source);

    var set_operation_right_join_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT id FROM usage_records UNION ALL SELECT u.id FROM usage_records AS u JOIN archived_records AS a ON u.id = a.id",
    );
    defer set_operation_right_join_sql.deinit(alloc);
    var set_operation_right_join = (try readSourceTableNamesFromParsedSqlAlloc(alloc, &set_operation_right_join_sql)).?;
    defer set_operation_right_join.deinit(alloc);
    try std.testing.expectEqualStrings("usage_records", set_operation_right_join.left);
    try std.testing.expectEqualStrings("archived_records", set_operation_right_join.source);

    var malformed_set_operation_source_table = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT id FROM usage_records UNION SELECT id FROM usage_archive",
    );
    defer malformed_set_operation_source_table.deinit(alloc);
    if (malformed_set_operation_source_table.generated_statement) |*generated_statement| {
        switch (generated_statement.ast.?) {
            .read => |read| {
                if (read.set_operation.right_source_table_tokens == null) return error.TestUnexpectedResult;
                read.set_operation.right_source_table_tokens = null;
            },
            else => return error.TestUnexpectedResult,
        }
    } else return error.TestUnexpectedResult;
    try std.testing.expectError(error.UnsupportedSqlShape, readSourceTableNamesFromParsedSqlAlloc(alloc, &malformed_set_operation_source_table));

    var unnest_source_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT d._id, tag FROM docs AS d, UNNEST(d.tags) AS tag WHERE tag = 'urgent' LIMIT 10;",
    );
    defer unnest_source_sql.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.read, unnest_source_sql.generatedStatementKind().?);
    var unnest_source = (try readSourceTableNamesFromParsedSqlAlloc(alloc, &unnest_source_sql)).?;
    defer unnest_source.deinit(alloc);
    try std.testing.expectEqualStrings("docs", unnest_source.left);
    try std.testing.expectEqualStrings("docs", unnest_source.source);

    if (alias_source_sql.generated_statement) |*generated_statement| {
        switch (generated_statement.ast.?) {
            .read => |read| read.source_table_tokens = read.source_alias_name_tokens,
            else => return error.TestUnexpectedResult,
        }
    } else return error.TestUnexpectedResult;
    try std.testing.expectError(error.UnsupportedSqlShape, readSourceTableNamesFromParsedSqlAlloc(alloc, &alias_source_sql));

    var simple_cte_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "WITH open_orders AS (SELECT id, tenant FROM usage_records) SELECT id FROM open_orders WHERE tenant = 't1'",
    );
    defer simple_cte_sql.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.read, simple_cte_sql.generatedStatementKind().?);
    var simple_cte = (try readSourceTableNamesFromParsedSqlAlloc(alloc, &simple_cte_sql)).?;
    defer simple_cte.deinit(alloc);
    try std.testing.expectEqualStrings("usage_records", simple_cte.left);
    try std.testing.expectEqualStrings("usage_records", simple_cte.source);

    if (simple_cte_sql.generated_statement) |*generated_statement| {
        switch (generated_statement.ast.?) {
            .read => |read| read.cte_items[0].body_source_table_tokens = read.cte_items[0].name_tokens,
            else => return error.TestUnexpectedResult,
        }
    } else return error.TestUnexpectedResult;
    try std.testing.expectError(error.UnsupportedSqlShape, readSourceTableNamesFromParsedSqlAlloc(alloc, &simple_cte_sql));

    var joined_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "WITH open_orders AS (SELECT id, tenant, customer_id FROM usage_records), active_customers AS (SELECT id, tenant, name FROM customer_records) SELECT o.id, c.name FROM open_orders AS o LEFT JOIN active_customers AS c ON o.tenant = c.tenant",
    );
    defer joined_sql.deinit(alloc);
    var joined = (try readSourceTableNamesFromParsedSqlAlloc(alloc, &joined_sql)).?;
    defer joined.deinit(alloc);
    try std.testing.expectEqualStrings("usage_records", joined.left);
    try std.testing.expectEqualStrings("customer_records", joined.source);

    var right_joined_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT o.id AS order_id, c.name AS customer_name FROM usage_records AS o RIGHT JOIN customer_records AS c ON o.tenant_id = c.tenant_id AND o.customer_id = c.id",
    );
    defer right_joined_sql.deinit(alloc);
    var right_joined = (try readSourceTableNamesFromParsedSqlAlloc(alloc, &right_joined_sql)).?;
    defer right_joined.deinit(alloc);
    try std.testing.expectEqualStrings("customer_records", right_joined.left);
    try std.testing.expectEqualStrings("usage_records", right_joined.source);

    const lateral_query =
        "WITH orgs AS (SELECT id FROM usage_records), balances AS (SELECT organization_id, amount FROM balance_records) SELECT org.id, latest.amount FROM orgs AS org LEFT JOIN LATERAL (SELECT amount FROM balances AS bal WHERE bal.organization_id = org.id LIMIT 1) AS latest ON true";
    var lateral_sql = try tokenized.ParsedSql.initAlloc(alloc, lateral_query);
    defer lateral_sql.deinit(alloc);
    var lateral = (try readSourceTableNamesFromParsedSqlAlloc(alloc, &lateral_sql)).?;
    defer lateral.deinit(alloc);
    try std.testing.expectEqualStrings("usage_records", lateral.left);
    try std.testing.expectEqualStrings("balance_records", lateral.source);

    var malformed_lateral_child_source = try tokenized.ParsedSql.initAlloc(alloc, lateral_query);
    defer malformed_lateral_child_source.deinit(alloc);
    if (malformed_lateral_child_source.generated_statement) |*generated_statement| {
        switch (generated_statement.ast.?) {
            .read => |read| {
                try std.testing.expectEqual(@as(usize, 1), read.join_items.len);
                const child = read.join_items[0].right_lateral_subquery_read_ast orelse return error.TestUnexpectedResult;
                child.source_table_tokens = null;
            },
            else => return error.TestUnexpectedResult,
        }
    } else return error.TestUnexpectedResult;
    try std.testing.expectError(error.UnsupportedSqlShape, readSourceTableNamesFromParsedSqlAlloc(alloc, &malformed_lateral_child_source));

    var malformed_lateral_metadata = try tokenized.ParsedSql.initAlloc(alloc, lateral_query);
    defer malformed_lateral_metadata.deinit(alloc);
    if (malformed_lateral_metadata.generated_statement) |*generated_statement| {
        switch (generated_statement.ast.?) {
            .read => |read| {
                try std.testing.expectEqual(@as(usize, 1), read.join_items.len);
                if (read.join_items[0].right_lateral_subquery_tokens == null) return error.TestUnexpectedResult;
                read.join_items[0].right_lateral_subquery_tokens = null;
            },
            else => return error.TestUnexpectedResult,
        }
    } else return error.TestUnexpectedResult;
    try std.testing.expectError(error.UnsupportedSqlShape, readSourceTableNamesFromParsedSqlAlloc(alloc, &malformed_lateral_metadata));

    var recursive_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "WITH RECURSIVE source_rows AS (SELECT id FROM usage_records UNION ALL SELECT child.id FROM usage_records AS child JOIN source_rows AS parent ON child.organization_id = parent.id) SELECT id FROM source_rows",
    );
    defer recursive_sql.deinit(alloc);
    var recursive = (try readSourceTableNamesFromParsedSqlAlloc(alloc, &recursive_sql)).?;
    defer recursive.deinit(alloc);
    try std.testing.expectEqualStrings("usage_records", recursive.left);
    try std.testing.expectEqualStrings("usage_records", recursive.source);

    var graph_function_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT id FROM antfly.graph_match(table_name => 'usage_records', index => 'docs_edge_graph', start => 'doc:root', pattern => '(a)-[:cites]->(b)', return => 'b') AS gm",
    );
    defer graph_function_sql.deinit(alloc);
    var graph_function = (try readSourceTableNamesFromParsedSqlAlloc(alloc, &graph_function_sql)).?;
    defer graph_function.deinit(alloc);
    try std.testing.expectEqualStrings("usage_records", graph_function.left);
    try std.testing.expectEqualStrings("usage_records", graph_function.source);

    if (graph_function_sql.generated_statement) |*generated_statement| {
        switch (generated_statement.ast.?) {
            .read => |read| read.source_graph_function_items[0].table_name_value_tokens = null,
            else => return error.TestUnexpectedResult,
        }
    } else return error.TestUnexpectedResult;
    try std.testing.expectError(error.UnsupportedSqlShape, readSourceTableNamesFromParsedSqlAlloc(alloc, &graph_function_sql));
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

    var missing_generated_target_ast = try tokenized.ParsedSql.initAlloc(alloc, "INSERT INTO usage_records (id) VALUES ('u1')");
    defer missing_generated_target_ast.deinit(alloc);
    if (missing_generated_target_ast.generated_statement) |*generated_statement| {
        if (generated_statement.ast) |*generated_ast| {
            generated_ast.deinit(alloc);
            generated_statement.ast = null;
        } else return error.TestUnexpectedResult;
    } else return error.TestUnexpectedResult;
    try std.testing.expectError(error.UnsupportedSqlShape, writeTargetTableNameFromParsedSqlAlloc(alloc, &missing_generated_target_ast));

    var stale_published_write = try tokenized.ParsedSql.initAlloc(alloc, "INSERT INTO usage_records (id) VALUES ('u1')");
    defer stale_published_write.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.dml, stale_published_write.generatedStatementKind().?);
    stale_published_write.statement = .{ .unknown = stale_published_write.raw_statement };
    try std.testing.expectError(error.UnsupportedSqlShape, writeTargetTableNameFromParsedSqlAlloc(alloc, &stale_published_write));

    var malformed_generated_target = try tokenized.ParsedSql.initAlloc(alloc, "INSERT INTO usage_records (id) SELECT id FROM incoming_usage");
    defer malformed_generated_target.deinit(alloc);
    if (malformed_generated_target.generated_statement) |*generated_statement| {
        switch (generated_statement.ast.?) {
            .dml => |*dml| dml.target_table_tokens = dml.source_read.?.source_table_tokens,
            else => return error.TestUnexpectedResult,
        }
    } else return error.TestUnexpectedResult;
    try std.testing.expectError(error.UnsupportedSqlShape, writeTargetTableNameFromParsedSqlAlloc(alloc, &malformed_generated_target));

    var read_sql = try tokenized.ParsedSql.initAlloc(alloc, "SELECT id FROM usage_records");
    defer read_sql.deinit(alloc);
    try std.testing.expectError(error.UnsupportedSqlShape, writeTargetTableNameFromParsedSqlAlloc(alloc, &read_sql));
}

test "sql adapter binder catalog write admission requires retained generated DML metadata" {
    const alloc = std.testing.allocator;

    var parsed_sql = try tokenized.ParsedSql.initAlloc(alloc, "INSERT INTO usage_records (id) VALUES ('u1')");
    defer parsed_sql.deinit(alloc);
    try requireParsedCatalogWriteStatementIncludingGeneratedAst(&parsed_sql);

    var missing_generated = try tokenized.ParsedSql.initAlloc(alloc, "INSERT INTO usage_records (id) VALUES ('u1')");
    defer missing_generated.deinit(alloc);
    var detached_generated = missing_generated.generated_statement orelse return error.TestUnexpectedResult;
    defer detached_generated.deinit(alloc);
    missing_generated.generated_statement = null;
    try std.testing.expectError(error.UnsupportedSqlShape, requireParsedCatalogWriteStatementIncludingGeneratedAst(&missing_generated));

    var stale_list = try tokenized.ParsedSql.initAlloc(alloc, "INSERT INTO usage_records (id) VALUES ('u1')");
    defer stale_list.deinit(alloc);
    if (stale_list.generated_statement) |*generated_statement| {
        switch (generated_statement.ast orelse return error.TestUnexpectedResult) {
            .dml => |*dml| dml.insert_column_items.count += 1,
            else => return error.TestUnexpectedResult,
        }
    } else return error.TestUnexpectedResult;
    try std.testing.expectError(error.UnsupportedSqlShape, requireParsedCatalogWriteStatementIncludingGeneratedAst(&stale_list));
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

    var insert_source_cte = try tokenized.ParsedSql.initAlloc(alloc, "WITH source_rows AS (SELECT id FROM incoming_usage) INSERT INTO usage_records (id) SELECT id FROM source_rows");
    defer insert_source_cte.deinit(alloc);
    var insert_cte_tables = (try insertSourceTableNamesFromParsedSqlAlloc(alloc, &insert_source_cte)).?;
    defer insert_cte_tables.deinit(alloc);
    try std.testing.expectEqualStrings("usage_records", insert_cte_tables.target);
    try std.testing.expectEqualStrings("incoming_usage", insert_cte_tables.source);

    var stale_insert_cte_count = try tokenized.ParsedSql.initAlloc(alloc, "WITH source_rows AS (SELECT id FROM incoming_usage) INSERT INTO usage_records (id) SELECT id FROM source_rows");
    defer stale_insert_cte_count.deinit(alloc);
    if (stale_insert_cte_count.generated_statement) |*generated_statement| {
        switch (generated_statement.ast.?) {
            .dml => |*dml| {
                const cte_prefix = if (dml.cte_prefix) |*cte_prefix| cte_prefix else return error.TestUnexpectedResult;
                cte_prefix.count += 1;
            },
            else => return error.TestUnexpectedResult,
        }
    } else return error.TestUnexpectedResult;
    try std.testing.expectError(error.UnsupportedSqlShape, insertSourceTableNamesFromParsedSqlAlloc(alloc, &stale_insert_cte_count));

    var missing_insert_source_ast = try tokenized.ParsedSql.initAlloc(alloc, "INSERT INTO usage_records (id) SELECT id FROM incoming_usage");
    defer missing_insert_source_ast.deinit(alloc);
    if (missing_insert_source_ast.generated_statement) |*generated_statement| {
        if (generated_statement.ast) |*generated_ast| {
            generated_ast.deinit(alloc);
            generated_statement.ast = null;
        } else return error.TestUnexpectedResult;
    } else return error.TestUnexpectedResult;
    try std.testing.expectError(error.UnsupportedSqlShape, insertSourceTableNamesFromParsedSqlAlloc(alloc, &missing_insert_source_ast));

    if (insert_source.generated_statement) |*generated_statement| {
        switch (generated_statement.ast.?) {
            .dml => |*dml| dml.source_read.?.source_table_tokens = dml.target_table_tokens,
            else => return error.TestUnexpectedResult,
        }
    } else return error.TestUnexpectedResult;
    try std.testing.expectError(error.UnsupportedSqlShape, insertSourceTableNamesFromParsedSqlAlloc(alloc, &insert_source));

    var recursive_insert = try tokenized.ParsedSql.initAlloc(alloc, "WITH RECURSIVE source_rows AS (SELECT id FROM incoming_usage) INSERT INTO usage_records (id) SELECT id FROM source_rows");
    defer recursive_insert.deinit(alloc);
    try std.testing.expect((try insertSourceTableNamesFromParsedSqlAlloc(alloc, &recursive_insert)) == null);
    var recursive_tables = (try recursiveInsertSourceTableNamesFromParsedSqlAlloc(alloc, &recursive_insert)).?;
    defer recursive_tables.deinit(alloc);
    try std.testing.expectEqualStrings("usage_records", recursive_tables.target);
    try std.testing.expectEqualStrings("incoming_usage", recursive_tables.source);

    var missing_recursive_insert_ast = try tokenized.ParsedSql.initAlloc(alloc, "WITH RECURSIVE source_rows AS (SELECT id FROM incoming_usage) INSERT INTO usage_records (id) SELECT id FROM source_rows");
    defer missing_recursive_insert_ast.deinit(alloc);
    if (missing_recursive_insert_ast.generated_statement) |*generated_statement| {
        if (generated_statement.ast) |*generated_ast| {
            generated_ast.deinit(alloc);
            generated_statement.ast = null;
        } else return error.TestUnexpectedResult;
    } else return error.TestUnexpectedResult;
    try std.testing.expectError(error.UnsupportedSqlShape, recursiveInsertSourceTableNamesFromParsedSqlAlloc(alloc, &missing_recursive_insert_ast));

    var stale_recursive_cte_count = try tokenized.ParsedSql.initAlloc(alloc, "WITH RECURSIVE source_rows AS (SELECT id FROM incoming_usage) INSERT INTO usage_records (id) SELECT id FROM source_rows");
    defer stale_recursive_cte_count.deinit(alloc);
    if (stale_recursive_cte_count.generated_statement) |*generated_statement| {
        switch (generated_statement.ast.?) {
            .dml => |*dml| {
                const cte_prefix = if (dml.cte_prefix) |*cte_prefix| cte_prefix else return error.TestUnexpectedResult;
                cte_prefix.count += 1;
            },
            else => return error.TestUnexpectedResult,
        }
    } else return error.TestUnexpectedResult;
    try std.testing.expectError(error.UnsupportedSqlShape, recursiveInsertSourceTableNamesFromParsedSqlAlloc(alloc, &stale_recursive_cte_count));

    if (recursive_insert.generated_statement) |*generated_statement| {
        switch (generated_statement.ast.?) {
            .dml => |*dml| {
                const cte_prefix = if (dml.cte_prefix) |*cte_prefix| cte_prefix else return error.TestUnexpectedResult;
                cte_prefix.items[0].body_read.?.source_table_tokens = dml.target_table_tokens;
            },
            else => return error.TestUnexpectedResult,
        }
    } else return error.TestUnexpectedResult;
    try std.testing.expectError(error.UnsupportedSqlShape, recursiveInsertSourceTableNamesFromParsedSqlAlloc(alloc, &recursive_insert));

    var update_source = try tokenized.ParsedSql.initAlloc(alloc, "UPDATE usage_records SET status = source.status FROM incoming_usage AS source WHERE source.id = usage_records.id");
    defer update_source.deinit(alloc);
    var update_tables = (try joinedWriteSourceTableNamesFromParsedSqlAlloc(alloc, &update_source)).?;
    defer update_tables.deinit(alloc);
    try std.testing.expectEqualStrings("usage_records", update_tables.target);
    try std.testing.expectEqualStrings("incoming_usage", update_tables.source);

    var missing_update_source_ast = try tokenized.ParsedSql.initAlloc(alloc, "UPDATE usage_records SET status = source.status FROM incoming_usage AS source WHERE source.id = usage_records.id");
    defer missing_update_source_ast.deinit(alloc);
    if (missing_update_source_ast.generated_statement) |*generated_statement| {
        if (generated_statement.ast) |*generated_ast| {
            generated_ast.deinit(alloc);
            generated_statement.ast = null;
        } else return error.TestUnexpectedResult;
    } else return error.TestUnexpectedResult;
    try std.testing.expectError(error.UnsupportedSqlShape, joinedWriteSourceTableNamesFromParsedSqlAlloc(alloc, &missing_update_source_ast));

    if (update_source.generated_statement) |*generated_statement| {
        switch (generated_statement.ast.?) {
            .dml => |*dml| dml.source_read.?.source_table_tokens = dml.target_table_tokens,
            else => return error.TestUnexpectedResult,
        }
    } else return error.TestUnexpectedResult;
    try std.testing.expectError(error.UnsupportedSqlShape, joinedWriteSourceTableNamesFromParsedSqlAlloc(alloc, &update_source));

    var missing_update_source_range = try tokenized.ParsedSql.initAlloc(alloc, "UPDATE usage_records SET status = source.status FROM incoming_usage AS source WHERE source.id = usage_records.id");
    defer missing_update_source_range.deinit(alloc);
    if (missing_update_source_range.generated_statement) |*generated_statement| {
        switch (generated_statement.ast.?) {
            .dml => |*dml| dml.source_read.?.source_tokens = null,
            else => return error.TestUnexpectedResult,
        }
    } else return error.TestUnexpectedResult;
    try std.testing.expectError(error.UnsupportedSqlShape, joinedWriteSourceTableNamesFromParsedSqlAlloc(alloc, &missing_update_source_range));

    var data_cte_merge = try tokenized.ParsedSql.initAlloc(
        alloc,
        "WITH source_rows AS (UPDATE usage_records SET status = 'ready' RETURNING id, status) MERGE INTO usage_records USING source_rows ON usage_records.id = source_rows.id WHEN MATCHED THEN UPDATE SET status = source_rows.status",
    );
    defer data_cte_merge.deinit(alloc);
    const data_cte_merge_ast = switch ((data_cte_merge.generated_statement orelse return error.TestUnexpectedResult).ast orelse return error.TestUnexpectedResult) {
        .dml => |ast| ast,
        else => return error.TestUnexpectedResult,
    };
    const data_cte_prefix = data_cte_merge_ast.cte_prefix orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(generated_parser.GeneratedSqlDmlKind.update, (data_cte_prefix.first_body_dml orelse return error.TestUnexpectedResult).kind);
    var data_cte_tables = (try joinedWriteSourceTableNamesFromGeneratedDmlCteAstAlloc(alloc, data_cte_merge.items(), &data_cte_merge_ast, data_cte_prefix)).?;
    defer data_cte_tables.deinit(alloc);
    try std.testing.expectEqualStrings("usage_records", data_cte_tables.target);
    try std.testing.expectEqualStrings("usage_records", data_cte_tables.source);

    var missing_data_cte_first_body = data_cte_prefix;
    missing_data_cte_first_body.first_body_dml = null;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        joinedWriteSourceTableNamesFromGeneratedDmlCteAstAlloc(alloc, data_cte_merge.items(), &data_cte_merge_ast, missing_data_cte_first_body),
    );

    const mismatched_data_cte_last_body = data_cte_prefix;
    const last_body_dml = mismatched_data_cte_last_body.last_body_dml orelse return error.TestUnexpectedResult;
    last_body_dml.command_span = data_cte_merge_ast.command_span;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        joinedWriteSourceTableNamesFromGeneratedDmlCteAstAlloc(alloc, data_cte_merge.items(), &data_cte_merge_ast, mismatched_data_cte_last_body),
    );

    var semijoin_update = try tokenized.ParsedSql.initAlloc(
        alloc,
        "UPDATE usage_records SET status = 'archived' WHERE EXISTS (SELECT 1 FROM archived_records WHERE archived_records.organization_id = usage_records.id AND archived_records.status = 'archived') FOR UPDATE SKIP LOCKED RETURNING id",
    );
    defer semijoin_update.deinit(alloc);
    var semijoin_update_tables = (try joinedWriteSourceTableNamesFromParsedSqlAlloc(alloc, &semijoin_update)).?;
    defer semijoin_update_tables.deinit(alloc);
    try std.testing.expectEqualStrings("usage_records", semijoin_update_tables.target);
    try std.testing.expectEqualStrings("archived_records", semijoin_update_tables.source);

    var stale_semijoin_update_source = try tokenized.ParsedSql.initAlloc(
        alloc,
        "UPDATE usage_records SET status = 'archived' WHERE EXISTS (SELECT 1 FROM archived_records WHERE archived_records.organization_id = usage_records.id AND archived_records.status = 'archived') FOR UPDATE SKIP LOCKED RETURNING id",
    );
    defer stale_semijoin_update_source.deinit(alloc);
    if (stale_semijoin_update_source.generated_statement) |*generated_statement| {
        switch (generated_statement.ast.?) {
            .dml => |*dml| dml.semijoin_source_table_tokens = dml.target_table_tokens,
            else => return error.TestUnexpectedResult,
        }
    } else return error.TestUnexpectedResult;
    try std.testing.expectError(error.UnsupportedSqlShape, joinedWriteSourceTableNamesFromParsedSqlAlloc(alloc, &stale_semijoin_update_source));

    var semijoin_delete = try tokenized.ParsedSql.initAlloc(
        alloc,
        "DELETE FROM usage_records WHERE EXISTS (SELECT 1 FROM archived_records WHERE archived_records.organization_id = usage_records.id AND archived_records.status = 'archived') FOR UPDATE SKIP LOCKED RETURNING id",
    );
    defer semijoin_delete.deinit(alloc);
    var semijoin_delete_tables = (try joinedWriteSourceTableNamesFromParsedSqlAlloc(alloc, &semijoin_delete)).?;
    defer semijoin_delete_tables.deinit(alloc);
    try std.testing.expectEqualStrings("usage_records", semijoin_delete_tables.target);
    try std.testing.expectEqualStrings("archived_records", semijoin_delete_tables.source);
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

    document_catalog.tables[0].indexes_json =
        "{\"view_mappings\":{\"support_view\":{\"source_table\":\"docs\",\"fields\":[{\"name\":\"plan\",\"path\":\"metadata.plan\",\"type\":\"keyword\"}]}}}";
    var parsed_document_view_read = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT _id, plan FROM support_view WHERE plan = 'pro' LIMIT 10",
    );
    defer parsed_document_view_read.deinit(alloc);
    var bound_document_view_read = try bindReadPlanCatalogStatementAlloc(alloc, &parsed_document_view_read, document_catalog.iface());
    defer bound_document_view_read.deinit(alloc);
    const document_view_read = try bound_document_view_read.readCatalog();
    try std.testing.expect(document_view_read.target_binding != null);
    try std.testing.expectEqual(@as(usize, 1), document_view_read.bound_objects.len);
    switch (document_view_read.target_binding.?) {
        .document => |binding| {
            try std.testing.expectEqualStrings("docs", binding.target.table_name);
            try std.testing.expectEqual(@as(u64, 1), binding.table_id);
            var saw_plan_view_field = false;
            for (binding.virtual_schema.fields) |field| {
                if (std.mem.eql(u8, field.name, "plan")) {
                    saw_plan_view_field = true;
                    try std.testing.expectEqualStrings("/metadata/plan", field.path);
                    try std.testing.expectEqual(source_binding.DocumentSqlVirtualFieldSource.view_mapping, field.source);
                }
            }
            try std.testing.expect(saw_plan_view_field);
            try std.testing.expectEqual(source_binding.SqlSourceFamily.document, document_view_read.bound_objects[0].family);
            try std.testing.expectEqualStrings("docs", document_view_read.bound_objects[0].target.table_name);
        },
        else => return error.TestUnexpectedResult,
    }

    var parsed_document_unnest_read = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT d._id, tag FROM docs AS d, UNNEST(d.tags) AS tag WHERE tag = 'urgent' LIMIT 10;",
    );
    defer parsed_document_unnest_read.deinit(alloc);
    var bound_document_unnest_read = try bindReadPlanCatalogStatementAlloc(alloc, &parsed_document_unnest_read, document_catalog.iface());
    defer bound_document_unnest_read.deinit(alloc);
    const document_unnest_read = try bound_document_unnest_read.readCatalog();
    try std.testing.expect(document_unnest_read.target_binding != null);
    switch (document_unnest_read.target_binding.?) {
        .document => |binding| {
            try std.testing.expectEqualStrings("docs", binding.target.table_name);
            try std.testing.expectEqual(runtime_schema.StorageMode.document, binding.schema.storage_mode);
            try std.testing.expectEqual(source_binding.SqlSourceFamily.document, document_unnest_read.bound_objects[0].family);
        },
        else => return error.TestUnexpectedResult,
    }
    try std.testing.expect(document_unnest_read.source_schema == null);

    var parsed_document_lateral_unnest_read = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT d._id, tag FROM docs AS d JOIN LATERAL UNNEST(d.tags) AS tag ON true WHERE tag = 'urgent' LIMIT 10;",
    );
    defer parsed_document_lateral_unnest_read.deinit(alloc);
    try std.testing.expectEqual(sql_statement_kind.SqlReadStatementKind.lateral, parsed_document_lateral_unnest_read.generatedReadStatementKind().?);
    var bound_document_lateral_unnest_read = try bindReadPlanCatalogStatementAlloc(alloc, &parsed_document_lateral_unnest_read, document_catalog.iface());
    defer bound_document_lateral_unnest_read.deinit(alloc);
    const document_lateral_unnest_read = try bound_document_lateral_unnest_read.readCatalog();
    try std.testing.expect(document_lateral_unnest_read.target_binding != null);
    switch (document_lateral_unnest_read.target_binding.?) {
        .document => |binding| {
            try std.testing.expectEqualStrings("docs", binding.target.table_name);
            try std.testing.expectEqual(runtime_schema.StorageMode.document, binding.schema.storage_mode);
            try std.testing.expectEqual(source_binding.SqlSourceFamily.document, document_lateral_unnest_read.bound_objects[0].family);
        },
        else => return error.TestUnexpectedResult,
    }
    try std.testing.expect(document_lateral_unnest_read.source_schema == null);

    var parsed_document_view_write = try tokenized.ParsedSql.initAlloc(
        alloc,
        "INSERT INTO support_view (_id, plan) VALUES ('doc:a', 'pro')",
    );
    defer parsed_document_view_write.deinit(alloc);
    try std.testing.expectError(
        error.DocumentSqlWriteUnsupported,
        bindWritePlanCatalogStatementAlloc(alloc, &parsed_document_view_write, .{}, document_catalog.iface()),
    );

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

    var parsed_update_source = try tokenized.ParsedSql.initAlloc(
        alloc,
        "UPDATE usage_records SET status = source.status FROM incoming_usage AS source WHERE source.id = usage_records.id",
    );
    defer parsed_update_source.deinit(alloc);
    var bound_update_source = try bindWritePlanCatalogStatementAlloc(alloc, &parsed_update_source, .{}, catalog.iface());
    defer bound_update_source.deinit(alloc);
    const update_source = try bound_update_source.writeCatalog();
    try std.testing.expect(update_source.options.joined_source_schema != null);
    try std.testing.expectEqual(@as(usize, 3), update_source.options.joined_source_schema.?.relational_columns.len);
    try std.testing.expectEqual(@as(usize, 2), update_source.bound_objects.len);
    try std.testing.expectEqual(BoundCatalogObjectRole.target, update_source.bound_objects[0].role);
    try std.testing.expectEqual(BoundCatalogObjectRole.joined_source, update_source.bound_objects[1].role);
    try std.testing.expectEqualStrings("incoming_usage", update_source.bound_objects[1].target.table_name);

    var malformed_update_source = try tokenized.ParsedSql.initAlloc(
        alloc,
        "UPDATE usage_records SET status = source.status FROM incoming_usage AS source WHERE source.id = usage_records.id",
    );
    defer malformed_update_source.deinit(alloc);
    if (malformed_update_source.generated_statement) |*generated_statement| {
        switch (generated_statement.ast.?) {
            .dml => |*dml| dml.source_read.?.source_table_tokens = dml.target_table_tokens,
            else => return error.TestUnexpectedResult,
        }
    } else return error.TestUnexpectedResult;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        bindWritePlanCatalogStatementAlloc(alloc, &malformed_update_source, .{}, catalog.iface()),
    );

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

    const fallback_path = [_][]const u8{ "tenant_missing", "public" };
    const fallback_session: catalog_resources.SqlCatalogSession = .{
        .search_path = fallback_path[0..],
    };
    var fallback_read_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT id FROM usage_records",
    );
    defer fallback_read_sql.deinit(alloc);
    var fallback_bound_read = try bindReadPlanCatalogStatementWithSessionAlloc(alloc, &fallback_read_sql, catalog.iface(), fallback_session);
    defer fallback_bound_read.deinit(alloc);
    const fallback_read = try fallback_bound_read.readCatalog();
    try std.testing.expect(fallback_read.target_binding != null);
    switch (fallback_read.target_binding.?) {
        .relational => |binding| {
            try std.testing.expectEqualStrings("default", binding.target.database_name);
            try std.testing.expectEqualStrings("public", binding.target.namespace_name);
            try std.testing.expectEqualStrings("usage_records", binding.target.table_name);
        },
        else => return error.TestUnexpectedResult,
    }

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

test "sql adapter binder resolves self merge table qualifier as target before source alias" {
    const alloc = std.testing.allocator;
    const columns = [_]runtime_schema.RelationalColumn{
        .{ .name = "title", .path = "title", .field_type = .keyword },
        .{ .name = "status", .path = "status", .field_type = .keyword },
    };
    const schema = runtime_schema.TableSchema{
        .storage_mode = .relational,
        .relational_columns = &columns,
    };

    const target_qualifiers = [_][]const u8{ "docs", "docs" };
    const source_qualifiers = [_][]const u8{ "docs", "source" };
    const target = try resolveRowExpressionFieldAlloc(
        alloc,
        schema,
        schema,
        "docs.status",
        &.{},
        &.{},
        source_qualifiers[0..],
        target_qualifiers[0..],
        false,
        .row,
    );
    defer alloc.free(target.field);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionFieldSource.row, target.source);
    try std.testing.expectEqualStrings("status", target.field);

    const source = try resolveRowExpressionFieldAlloc(
        alloc,
        schema,
        schema,
        "source.title",
        &.{},
        &.{},
        source_qualifiers[0..],
        target_qualifiers[0..],
        false,
        .row,
    );
    defer alloc.free(source.field);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionFieldSource.source, source.source);
    try std.testing.expectEqualStrings("title", source.field);
}

test "sql adapter binder validates relational catalog lookups" {
    const columns = [_]runtime_schema.RelationalColumn{
        .{ .name = "id", .path = "id", .field_type = .keyword },
        .{ .name = "email", .path = "email", .field_type = .keyword },
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
    const relational_indexes = [_]runtime_schema.RelationalIndex{.{
        .name = "users_email_idx",
        .owner_kind = .relational_column,
        .owner_name = "email",
        .access_method = .ordered_tuple,
        .columns = &.{"email"},
        .keys = &.{.{ .column = "email" }},
        .lifecycle = .ready,
        .generation = 7,
        .schema_fingerprint = "secondary-index-v1:users_email_idx",
    }};
    const schema = runtime_schema.TableSchema{
        .storage_mode = .relational,
        .relational_columns = &columns,
        .primary_key = primary_key,
        .unique_constraints = &uniques,
        .relational_indexes = &relational_indexes,
        .foreign_keys = &foreign_keys,
        .checks = &checks,
    };

    try std.testing.expect(tableSchemaCatalogExists(schema));
    try std.testing.expect(relationalColumnForDdl(&columns, "email") != null);
    try std.testing.expect(relationalColumnForReturningField(schema, "email") != null);
    try std.testing.expect(relationalColumnForReturningField(schema, "profile.city") != null);
    try std.testing.expect(relationalColumnForReturningField(schema, "email.domain") == null);
    try std.testing.expect(relationalIndexCatalogNameExists(schema.relational_indexes, "users_email_idx"));
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
