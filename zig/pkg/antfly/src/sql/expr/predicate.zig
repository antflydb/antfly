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

const ast = @import("../ast.zig");
const binder = @import("../binder.zig");
const db_mod = @import("../../storage/db/mod.zig");
const ddl_plan = @import("../ddl_plan.zig");
const expr_limits = @import("limits.zig");
const expr_generated = @import("generated.zig");
const expr_generated_validate = @import("generated_validate.zig");
const expr_operator = @import("operator.zig");
const expr_parse = @import("parse.zig");
const expr_row_parse = @import("row_parse.zig");
const expr_token = @import("token.zig");
const expr_type = @import("type.zig");
const generated_parser = @import("../generated_parser.zig");
const parser = @import("../parser.zig");
const plan_mod = @import("../plan.zig");
const runtime_schema = @import("../../storage/schema.zig");
const token_mod = @import("../token.zig");
const value_mod = @import("../value.zig");

const Token = token_mod.Token;
const cloneExpressionAlloc = plan_mod.cloneExpressionAlloc;
const cloneQueryRelationalCheckAlloc = plan_mod.cloneQueryRelationalCheckAlloc;
const freeAccessPredicateGroup = plan_mod.freeAccessPredicateGroup;
const freeAccessPredicateGroups = plan_mod.freeAccessPredicateGroups;
const freeArrayAny = plan_mod.freeArrayAny;
const freeArrayContains = plan_mod.freeArrayContains;
const freeArrayEq = plan_mod.freeArrayEq;
const freeExpression = plan_mod.freeExpression;
const freeExpressionCondition = plan_mod.freeExpressionCondition;
const freeInPredicates = plan_mod.freeInPredicates;
const freeJsonContains = plan_mod.freeJsonContains;
const freeJsonPathEq = plan_mod.freeJsonPathEq;
const freeJsonPathExists = plan_mod.freeJsonPathExists;
const freePredicateGroup = plan_mod.freePredicateGroup;
const freePredicateGroups = plan_mod.freePredicateGroups;
const freeRelationalChecks = plan_mod.freeRelationalChecks;
const freeTextPatterns = plan_mod.freeTextPatterns;
const max_scalar_or_expanded_branches = expr_limits.max_scalar_or_expanded_branches;

pub const ScalarOrCheckBranch = std.ArrayListUnmanaged(runtime_schema.RelationalCheck);

pub const AccessPredicateBranch = struct {
    predicates: std.ArrayListUnmanaged(runtime_schema.RelationalCheck) = .empty,
    array_any: std.ArrayListUnmanaged(db_mod.types.RelationalRowsArrayAnyPredicate) = .empty,
    array_contains: std.ArrayListUnmanaged(db_mod.types.RelationalRowsArrayContainsPredicate) = .empty,
    array_eq: std.ArrayListUnmanaged(db_mod.types.RelationalRowsArrayEqPredicate) = .empty,
    in_predicates: std.ArrayListUnmanaged(db_mod.types.RelationalRowsInPredicate) = .empty,
    json_contains: std.ArrayListUnmanaged(db_mod.types.RelationalRowsJsonContainsPredicate) = .empty,
    json_path_eq: std.ArrayListUnmanaged(db_mod.types.RelationalRowsJsonPathEqPredicate) = .empty,
    json_path_exists: std.ArrayListUnmanaged(db_mod.types.RelationalRowsJsonPathExistsPredicate) = .empty,
    text_patterns: std.ArrayListUnmanaged(db_mod.types.RelationalRowsTextPatternPredicate) = .empty,
};

pub const PeriodRangeValuePair = value_mod.PeriodRangeValuePair;
pub const freePeriodRangeValuePair = value_mod.freePeriodRangeValuePair;

pub const BareBooleanWhereExpressionParserOptions = struct {
    boolean_hooks: expr_row_parse.BooleanRowExpressionParserHooks,
    generated_expression_ast: ?*const generated_parser.GeneratedSqlExpressionAst = null,
};

pub const JoinedMutationExpressionSide = union(enum) {
    single: db_mod.types.RelationalRowsJoinProjectionSide,
    mixed,
};

pub fn parseSqlPeriodRangeValuePairAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    params: []const value_mod.SqlValue,
    schema: runtime_schema.TableSchema,
    period: runtime_schema.RelationalPeriod,
    realtime_ns: u64,
) !PeriodRangeValuePair {
    const start_column = binder.relationalColumnForField(schema, period.start_column, null) orelse return error.InvalidSqlCatalog;
    const end_column = binder.relationalColumnForField(schema, period.end_column, null) orelse return error.InvalidSqlCatalog;
    if (start_column.field_type != end_column.field_type or !binder.relationalPeriodColumnType(start_column.field_type)) return error.InvalidSqlCatalog;
    if (parser.matchToken(tokens, pos, .string)) |token| {
        return try value_mod.parseSqlCanonicalRangeLiteralValuePairAlloc(alloc, token.text, start_column.field_type, period.range_type);
    }
    if (pos.* + 1 < tokens.len and tokens[pos.*].kind == .identifier and tokens[pos.* + 1].kind == .lparen) {
        return try parseSqlRangeConstructorValuePairAlloc(alloc, tokens, pos, params, start_column.field_type, realtime_ns);
    }
    return error.UnsupportedSqlShape;
}

pub fn parseSqlRangeConstructorValuePairAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    params: []const value_mod.SqlValue,
    field_type: runtime_schema.AntflyType,
    realtime_ns: u64,
) !PeriodRangeValuePair {
    const function = parser.matchToken(tokens, pos, .identifier) orelse return error.UnsupportedSqlShape;
    const constructor_type = ddl_plan.ddlRangeBoundTypeForName(function.text) orelse return error.UnsupportedSqlShape;
    if (constructor_type != field_type) return error.UnsupportedSqlShape;
    try parser.expectToken(tokens, pos, .lparen);

    var start_json = try parseSqlRangeConstructorEndpointJsonAlloc(alloc, tokens, pos, params, field_type, realtime_ns);
    var start_transferred = false;
    errdefer if (!start_transferred) alloc.free(start_json);
    try parser.expectToken(tokens, pos, .comma);
    var end_json = try parseSqlRangeConstructorEndpointJsonAlloc(alloc, tokens, pos, params, field_type, realtime_ns);
    var end_transferred = false;
    errdefer if (!end_transferred) alloc.free(end_json);

    if (parser.matchToken(tokens, pos, .comma) != null) {
        const bounds = parser.matchToken(tokens, pos, .string) orelse return error.UnsupportedSqlShape;
        if (std.mem.eql(u8, bounds.text, "[)")) {
            // Already canonical.
        } else if (std.ascii.eqlIgnoreCase(function.text, "daterange")) {
            if (bounds.text.len != 2) return error.UnsupportedSqlShape;
            const lower_bound = bounds.text[0];
            const upper_bound = bounds.text[1];
            if (lower_bound != '[' and lower_bound != '(') return error.UnsupportedSqlShape;
            if (upper_bound != ')' and upper_bound != ']') return error.UnsupportedSqlShape;
            if (lower_bound == '(' and !std.mem.eql(u8, start_json, "null")) {
                const canonical_start_json = try value_mod.canonicalizeDiscreteDateRangeFiniteBoundAlloc(alloc, start_json);
                alloc.free(start_json);
                start_json = canonical_start_json;
            }
            if (upper_bound == ']') {
                const canonical_end_json = try value_mod.canonicalizeDiscreteDateRangeFiniteBoundAlloc(alloc, end_json);
                alloc.free(end_json);
                end_json = canonical_end_json;
            }
        } else {
            return error.UnsupportedSqlShape;
        }
    }
    try parser.expectToken(tokens, pos, .rparen);

    start_transferred = true;
    end_transferred = true;
    return .{
        .start_json = start_json,
        .end_json = end_json,
    };
}

pub fn parseSqlRangeConstructorEndpointJsonAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    params: []const value_mod.SqlValue,
    field_type: runtime_schema.AntflyType,
    realtime_ns: u64,
) ![]const u8 {
    if (parser.matchKeyword(tokens, pos, "null")) return try alloc.dupe(u8, "null");
    if (field_type == .datetime) {
        if (expr_token.peekSqlNowExpressionSyntax(tokens, pos.*)) return try value_mod.parseSqlNowValueJsonAlloc(alloc, tokens, pos, realtime_ns);
        if (expr_token.peekSqlCurrentDateExpressionSyntax(tokens, pos.*)) return try value_mod.parseSqlCurrentDateValueJsonAlloc(alloc, tokens, pos, value_mod.sqlCurrentUtcDateStartNs(realtime_ns));
        if (expr_parse.peekSqlTypedDatetimeLiteral(tokens, pos.*)) return try value_mod.parseSqlTypedDatetimeLiteralValueJsonAlloc(alloc, tokens, pos);
    }
    if (parser.matchToken(tokens, pos, .placeholder)) |token| {
        const value = try value_mod.boundSqlValue(token, params);
        return switch (field_type) {
            .numeric => switch (value) {
                .integer, .float => try value.jsonAlloc(alloc),
                .string => |text| try value_mod.parseSqlRangeEndpointJsonAlloc(alloc, text, field_type),
                else => error.UnsupportedSqlShape,
            },
            .datetime => switch (value) {
                .integer => try value.jsonAlloc(alloc),
                .string => |text| try value_mod.parseSqlRangeEndpointJsonAlloc(alloc, text, field_type),
                else => error.UnsupportedSqlShape,
            },
            else => error.InvalidSqlCatalog,
        };
    }
    if (parser.matchToken(tokens, pos, .string)) |token| return try value_mod.parseSqlRangeEndpointJsonAlloc(alloc, token.text, field_type);
    if (parser.matchToken(tokens, pos, .number)) |token| return try value_mod.parseSqlRangeEndpointJsonAlloc(alloc, token.text, field_type);
    if (parser.matchToken(tokens, pos, .minus) != null) {
        const number = parser.matchToken(tokens, pos, .number) orelse return error.UnsupportedSqlShape;
        const text = try std.fmt.allocPrint(alloc, "-{s}", .{number.text});
        defer alloc.free(text);
        return try value_mod.parseSqlRangeEndpointJsonAlloc(alloc, text, field_type);
    }
    return error.UnsupportedSqlShape;
}

pub fn normalizeSqlLikePatternEscapeAlloc(alloc: std.mem.Allocator, pattern: []const u8, escape: []const u8) ![]const u8 {
    if (escape.len != 1) return error.UnsupportedSqlShape;
    const escape_char = escape[0];
    if (escape_char == '\\') return try alloc.dupe(u8, pattern);

    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    var index: usize = 0;
    while (index < pattern.len) {
        const byte = pattern[index];
        if (byte == escape_char) {
            if (index + 1 >= pattern.len) return error.UnsupportedSqlShape;
            try out.append(alloc, '\\');
            try out.append(alloc, pattern[index + 1]);
            index += 2;
            continue;
        }
        if (byte == '\\') {
            try out.append(alloc, '\\');
            try out.append(alloc, '\\');
            index += 1;
            continue;
        }
        try out.append(alloc, byte);
        index += 1;
    }
    return try out.toOwnedSlice(alloc);
}

pub fn sqlLikePrefixLiteralAlloc(alloc: std.mem.Allocator, pattern: []const u8) !?[]const u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    var index: usize = 0;
    while (index < pattern.len) {
        const byte = pattern[index];
        if (byte == '\\') {
            if (index + 1 >= pattern.len) return error.UnsupportedSqlShape;
            try out.append(alloc, pattern[index + 1]);
            index += 2;
            continue;
        }
        if (byte == '_') {
            out.deinit(alloc);
            return null;
        }
        if (byte == '%') {
            if (index + 1 != pattern.len) {
                out.deinit(alloc);
                return null;
            }
            return try out.toOwnedSlice(alloc);
        }
        try out.append(alloc, byte);
        index += 1;
    }
    out.deinit(alloc);
    return null;
}

pub fn jsonStringLiteralValueAlloc(alloc: std.mem.Allocator, value_json: []const u8) ![]const u8 {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, value_json, .{}) catch return error.UnsupportedSqlShape;
    defer parsed.deinit();
    if (parsed.value != .string) return error.UnsupportedSqlShape;
    return try alloc.dupe(u8, parsed.value.string);
}

pub fn appendExpressionValuesJsonConjunction(
    alloc: std.mem.Allocator,
    type_context: expr_type.RowExpressionTypeContext,
    expression_predicates: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionCondition),
    lhs: db_mod.types.RelationalRowsExpression,
    values_json: []const u8,
) !void {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, values_json, .{}) catch return error.UnsupportedSqlShape;
    defer parsed.deinit();
    if (parsed.value != .array or parsed.value.array.items.len == 0 or parsed.value.array.items.len > max_scalar_or_expanded_branches) return error.UnsupportedSqlShape;
    try expr_type.validateExpressionScalarMembershipValues(type_context, lhs, parsed.value);

    for (parsed.value.array.items) |value| {
        const lhs_clone = try cloneExpressionAlloc(alloc, lhs);
        var lhs_transferred = false;
        errdefer if (!lhs_transferred) freeExpression(alloc, lhs_clone);

        const value_json = try std.json.Stringify.valueAlloc(alloc, value, .{});
        var value_transferred = false;
        errdefer if (!value_transferred) alloc.free(value_json);

        const rhs = try alloc.alloc(db_mod.types.RelationalRowsExpression, 1);
        var rhs_transferred = false;
        errdefer if (!rhs_transferred) alloc.free(rhs);
        rhs[0] = .{
            .kind = .value,
            .value_json = value_json,
        };
        value_transferred = true;

        try expression_predicates.append(alloc, .{
            .lhs = lhs_clone,
            .op = .eq,
            .rhs = rhs,
        });
        lhs_transferred = true;
        rhs_transferred = true;
    }
}

pub fn appendExpressionValuesJsonOrGroups(
    alloc: std.mem.Allocator,
    type_context: expr_type.RowExpressionTypeContext,
    expression_or_predicates: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionPredicateGroup),
    lhs: db_mod.types.RelationalRowsExpression,
    values_json: []const u8,
) !void {
    try appendExpressionValuesJsonComparisonGroups(alloc, type_context, expression_or_predicates, lhs, values_json, .eq);
}

pub fn appendExpressionValuesJsonConjunctionGroup(
    alloc: std.mem.Allocator,
    type_context: expr_type.RowExpressionTypeContext,
    expression_or_predicates: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionPredicateGroup),
    lhs: db_mod.types.RelationalRowsExpression,
    values_json: []const u8,
    op: runtime_schema.RelationalCheckOp,
) !void {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, values_json, .{}) catch return error.UnsupportedSqlShape;
    defer parsed.deinit();
    if (parsed.value != .array or parsed.value.array.items.len == 0 or parsed.value.array.items.len > max_scalar_or_expanded_branches) return error.UnsupportedSqlShape;
    try expr_type.validateExpressionScalarMembershipValues(type_context, lhs, parsed.value);

    const conditions = try alloc.alloc(db_mod.types.RelationalRowsExpressionCondition, parsed.value.array.items.len);
    var condition_count: usize = 0;
    errdefer {
        for (conditions[0..condition_count]) |condition| freeExpressionCondition(alloc, condition);
        alloc.free(conditions);
    }

    for (parsed.value.array.items) |value| {
        if (op == .ne and value == .null) return error.UnsupportedSqlShape;

        const lhs_clone = try cloneExpressionAlloc(alloc, lhs);
        var lhs_transferred = false;
        errdefer if (!lhs_transferred) freeExpression(alloc, lhs_clone);

        const value_json = try std.json.Stringify.valueAlloc(alloc, value, .{});
        var value_transferred = false;
        errdefer if (!value_transferred) alloc.free(value_json);

        const rhs = try alloc.alloc(db_mod.types.RelationalRowsExpression, 1);
        var rhs_transferred = false;
        errdefer if (!rhs_transferred) alloc.free(rhs);
        rhs[0] = .{
            .kind = .value,
            .value_json = value_json,
        };
        value_transferred = true;

        conditions[condition_count] = .{
            .lhs = lhs_clone,
            .op = op,
            .rhs = rhs,
        };
        condition_count += 1;
        lhs_transferred = true;
        rhs_transferred = true;
    }

    try expression_or_predicates.append(alloc, .{ .conditions = conditions });
}

pub fn appendExpressionValuesJsonComparisonGroups(
    alloc: std.mem.Allocator,
    type_context: expr_type.RowExpressionTypeContext,
    expression_or_predicates: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionPredicateGroup),
    lhs: db_mod.types.RelationalRowsExpression,
    values_json: []const u8,
    op: runtime_schema.RelationalCheckOp,
) !void {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, values_json, .{}) catch return error.UnsupportedSqlShape;
    defer parsed.deinit();
    if (parsed.value != .array or parsed.value.array.items.len == 0 or parsed.value.array.items.len > max_scalar_or_expanded_branches) return error.UnsupportedSqlShape;
    try expr_type.validateExpressionScalarMembershipValues(type_context, lhs, parsed.value);

    for (parsed.value.array.items) |value| {
        const lhs_clone = try cloneExpressionAlloc(alloc, lhs);
        var lhs_transferred = false;
        errdefer if (!lhs_transferred) freeExpression(alloc, lhs_clone);

        const value_json = try std.json.Stringify.valueAlloc(alloc, value, .{});
        var value_transferred = false;
        errdefer if (!value_transferred) alloc.free(value_json);

        const rhs = try alloc.alloc(db_mod.types.RelationalRowsExpression, 1);
        var rhs_transferred = false;
        errdefer if (!rhs_transferred) alloc.free(rhs);
        rhs[0] = .{
            .kind = .value,
            .value_json = value_json,
        };
        value_transferred = true;

        const condition: db_mod.types.RelationalRowsExpressionCondition = .{
            .lhs = lhs_clone,
            .op = op,
            .rhs = rhs,
        };
        var condition_transferred = false;
        errdefer if (!condition_transferred) freeExpressionCondition(alloc, condition);
        lhs_transferred = true;
        rhs_transferred = true;

        const conditions = try alloc.alloc(db_mod.types.RelationalRowsExpressionCondition, 1);
        var conditions_transferred = false;
        errdefer if (!conditions_transferred) alloc.free(conditions);
        conditions[0] = condition;
        condition_transferred = true;

        try expression_or_predicates.append(alloc, .{ .conditions = conditions });
        conditions_transferred = true;
    }
}

pub fn whereHasArrayOverlapPredicateAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    schema: runtime_schema.TableSchema,
    field_expression_qualifiers: []const []const u8,
    returning_expression_qualifiers: []const []const u8,
    defer_row_expression_field_validation: bool,
) !bool {
    const saved_pos = pos.*;
    defer pos.* = saved_pos;

    var depth: usize = 0;
    var index = saved_pos;
    while (index < tokens.len) : (index += 1) {
        const token = tokens[index];
        switch (token.kind) {
            .lparen => depth += 1,
            .rparen => if (depth > 0) {
                depth -= 1;
            },
            .semicolon => if (depth == 0) return false,
            .identifier => {
                if (depth == 0 and expr_token.sqlWhereTailClauseKeywordToken(token)) return false;
                if (try arrayOverlapPredicateCanStartAtAlloc(
                    alloc,
                    tokens,
                    pos,
                    index,
                    schema,
                    field_expression_qualifiers,
                    returning_expression_qualifiers,
                    defer_row_expression_field_validation,
                )) return true;
            },
            else => {},
        }
    }
    return false;
}

fn arrayOverlapPredicateCanStartAtAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    index: usize,
    schema: runtime_schema.TableSchema,
    field_expression_qualifiers: []const []const u8,
    returning_expression_qualifiers: []const []const u8,
    defer_row_expression_field_validation: bool,
) !bool {
    const saved_pos = pos.*;
    pos.* = index;
    defer pos.* = saved_pos;

    const parsed_field = expr_generated.parseRowExpressionFieldOwnedAlloc(
        alloc,
        tokens,
        pos,
        schema,
        field_expression_qualifiers,
        returning_expression_qualifiers,
        defer_row_expression_field_validation,
    ) catch |err| return switch (err) {
        error.UnsupportedSqlShape, error.InvalidSqlCatalog => false,
        else => err,
    };
    defer alloc.free(parsed_field);
    if (pos.* >= tokens.len or tokens[pos.*].kind != .range_overlap) return false;

    const field = binder.normalizeRowExpressionFieldAlloc(
        alloc,
        schema,
        parsed_field,
        field_expression_qualifiers,
        returning_expression_qualifiers,
        defer_row_expression_field_validation,
    ) catch |err| return switch (err) {
        error.UnsupportedSqlShape, error.InvalidSqlCatalog => false,
        else => err,
    };
    defer alloc.free(field);
    return binder.relationalColumnForField(schema, field, .array) != null;
}

pub fn parseArrayOverlapIntoAccessBranchesAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    params: []const value_mod.SqlValue,
    schema: runtime_schema.TableSchema,
    field_expression_qualifiers: []const []const u8,
    returning_expression_qualifiers: []const []const u8,
    defer_row_expression_field_validation: bool,
    branches: *std.ArrayListUnmanaged(AccessPredicateBranch),
    generated_expression_ast: ?*const generated_parser.GeneratedSqlExpressionAst,
) !bool {
    const saved_pos = pos.*;
    const parsed_field = expr_generated.parseRowExpressionFieldOwnedAlloc(
        alloc,
        tokens,
        pos,
        schema,
        field_expression_qualifiers,
        returning_expression_qualifiers,
        defer_row_expression_field_validation,
    ) catch |err| {
        pos.* = saved_pos;
        return switch (err) {
            error.UnsupportedSqlShape, error.InvalidSqlCatalog => false,
            else => err,
        };
    };
    defer alloc.free(parsed_field);
    if (pos.* >= tokens.len or tokens[pos.*].kind != .range_overlap) {
        pos.* = saved_pos;
        return false;
    }

    const field = binder.normalizeRowExpressionFieldAlloc(
        alloc,
        schema,
        parsed_field,
        field_expression_qualifiers,
        returning_expression_qualifiers,
        defer_row_expression_field_validation,
    ) catch |err| {
        pos.* = saved_pos;
        return switch (err) {
            error.UnsupportedSqlShape, error.InvalidSqlCatalog => false,
            else => err,
        };
    };
    defer alloc.free(field);
    const column = binder.relationalColumnForField(schema, field, .array) orelse {
        pos.* = saved_pos;
        return false;
    };
    const operator_token_index = pos.*;
    _ = parser.matchToken(tokens, pos, .range_overlap);
    try expr_generated_validate.validateGeneratedSingleOperatorPredicateIdentity(generated_expression_ast, .overlaps, tokens, operator_token_index);
    const values_json = try value_mod.parseArrayPredicateValueAlloc(alloc, tokens, pos, params);
    defer alloc.free(values_json);
    try value_mod.validateSqlArrayValueJson(alloc, column, values_json);
    try expandArrayOverlapValuesIntoAccessBranches(alloc, branches, field, values_json);
    return true;
}

pub fn parseExpressionArrayContainsPredicateAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    params: []const value_mod.SqlValue,
    type_context: expr_type.RowExpressionTypeContext,
    hooks: expr_row_parse.FixedBinaryRowExpressionParserOptions,
) !db_mod.types.RelationalRowsExpressionArrayContainsPredicate {
    const expression = try expr_row_parse.parseStringToArrayRowExpressionAlloc(alloc, tokens, pos, type_context, hooks);
    var expression_transferred = false;
    errdefer if (!expression_transferred) freeExpression(alloc, expression);
    try parser.expectToken(tokens, pos, .at_contains);
    const value_json = if (parser.peekKeyword(tokens, pos.*, "array"))
        try value_mod.parseSqlArrayConstructorJsonAlloc(alloc, tokens, pos, params)
    else
        try value_mod.parseRequiredJsonDocumentValueAlloc(alloc, tokens, pos, params);
    var value_transferred = false;
    errdefer if (!value_transferred) alloc.free(value_json);
    try value_mod.validateJsonStringArray(alloc, value_json);

    expression_transferred = true;
    value_transferred = true;
    return .{
        .expression = expression,
        .value_json = value_json,
    };
}

pub fn parseValueEqualsAnyArrayPredicateAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    params: []const value_mod.SqlValue,
    schema: runtime_schema.TableSchema,
    field_expression_qualifiers: []const []const u8,
    returning_expression_qualifiers: []const []const u8,
    defer_row_expression_field_validation: bool,
    array_any: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsArrayAnyPredicate),
    generated_expression_ast: ?*const generated_parser.GeneratedSqlExpressionAst,
) !bool {
    if (!valueEqualsAnyArrayPredicateCanStart(tokens, pos.*)) return false;

    const value_json = try value_mod.parseJsonValueAlloc(alloc, tokens, pos, params);
    var value_transferred = false;
    errdefer if (!value_transferred) alloc.free(value_json);
    const operator_token_index = pos.*;
    try parser.expectToken(tokens, pos, .eq);
    if (!expr_token.matchAnyOrSomeKeyword(tokens, pos)) return error.UnsupportedSqlShape;
    try expr_generated_validate.validateGeneratedQuantifiedPredicateIdentity(generated_expression_ast, tokens, operator_token_index, pos.* - 1);
    try parser.expectToken(tokens, pos, .lparen);
    const parsed_field = try expr_generated.parseRowExpressionFieldOwnedAlloc(
        alloc,
        tokens,
        pos,
        schema,
        field_expression_qualifiers,
        returning_expression_qualifiers,
        defer_row_expression_field_validation,
    );
    defer alloc.free(parsed_field);
    const field = try binder.normalizeRowExpressionFieldAlloc(
        alloc,
        schema,
        parsed_field,
        field_expression_qualifiers,
        returning_expression_qualifiers,
        defer_row_expression_field_validation,
    );
    var field_transferred = false;
    errdefer if (!field_transferred) alloc.free(field);
    const column = binder.relationalColumnForField(schema, field, .array) orelse return error.InvalidSqlCatalog;
    try value_mod.validateSqlArrayElementValueJson(alloc, column, value_json);
    try parser.expectToken(tokens, pos, .rparen);
    try array_any.append(alloc, .{
        .field = field,
        .value_json = value_json,
    });
    field_transferred = true;
    value_transferred = true;
    return true;
}

pub fn expandScalarValuesJsonIntoOrBranches(
    alloc: std.mem.Allocator,
    branches: *std.ArrayListUnmanaged(ScalarOrCheckBranch),
    field: []const u8,
    values_json: []const u8,
    op: runtime_schema.RelationalCheckOp,
    collation: ?[]const u8,
) !void {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, values_json, .{}) catch return error.UnsupportedSqlShape;
    defer parsed.deinit();
    if (parsed.value != .array or parsed.value.array.items.len == 0) return error.UnsupportedSqlShape;
    if (branches.items.len == 0 or branches.items.len > max_scalar_or_expanded_branches / parsed.value.array.items.len) return error.UnsupportedSqlShape;

    var expanded = std.ArrayListUnmanaged(ScalarOrCheckBranch).empty;
    errdefer freeScalarOrCheckBranches(alloc, &expanded);
    for (branches.items) |*source_branch| {
        for (parsed.value.array.items) |value| {
            if (op == .ne and value == .null) return error.UnsupportedSqlShape;
            var expanded_branch = ScalarOrCheckBranch.empty;
            var expanded_branch_transferred = false;
            errdefer if (!expanded_branch_transferred) {
                freeRelationalChecks(alloc, expanded_branch.items);
                expanded_branch.deinit(alloc);
            };

            try cloneScalarOrBranchInto(alloc, &expanded_branch, source_branch.*);
            try appendScalarJsonValueCheckToBranch(alloc, &expanded_branch, field, value, op, collation);

            try expanded.append(alloc, expanded_branch);
            expanded_branch_transferred = true;
        }
    }

    freeScalarOrCheckBranches(alloc, branches);
    branches.* = expanded;
}

pub fn appendScalarValuesJsonToOrBranches(
    alloc: std.mem.Allocator,
    branches: *std.ArrayListUnmanaged(ScalarOrCheckBranch),
    field: []const u8,
    values_json: []const u8,
    op: runtime_schema.RelationalCheckOp,
    collation: ?[]const u8,
) !void {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, values_json, .{}) catch return error.UnsupportedSqlShape;
    defer parsed.deinit();
    if (parsed.value != .array or parsed.value.array.items.len == 0 or parsed.value.array.items.len > max_scalar_or_expanded_branches) return error.UnsupportedSqlShape;
    if (branches.items.len == 0) return error.UnsupportedSqlShape;

    for (branches.items) |*branch| {
        for (parsed.value.array.items) |value| {
            if (op == .ne and value == .null) return error.UnsupportedSqlShape;
            try appendScalarJsonValueCheckToBranch(alloc, branch, field, value, op, collation);
        }
    }
}

pub fn appendScalarJsonValueCheckToBranch(
    alloc: std.mem.Allocator,
    branch: *ScalarOrCheckBranch,
    field: []const u8,
    value: std.json.Value,
    op: runtime_schema.RelationalCheckOp,
    collation: ?[]const u8,
) !void {
    const value_json = try std.json.Stringify.valueAlloc(alloc, value, .{});
    var value_transferred = false;
    errdefer if (!value_transferred) alloc.free(value_json);
    const owned_field = try alloc.dupe(u8, field);
    var field_transferred = false;
    errdefer if (!field_transferred) alloc.free(owned_field);
    const owned_collation = if (collation) |source| try alloc.dupe(u8, source) else null;
    var collation_transferred = false;
    errdefer if (!collation_transferred) if (owned_collation) |owned| alloc.free(owned);
    try branch.append(alloc, .{
        .name = "",
        .field = owned_field,
        .op = op,
        .value_json = value_json,
        .collation = owned_collation,
    });
    value_transferred = true;
    field_transferred = true;
    collation_transferred = true;
}

pub fn initRelationalBooleanCheck(
    alloc: std.mem.Allocator,
    out: *runtime_schema.RelationalCheck,
    field: []const u8,
    op: runtime_schema.RelationalCheckOp,
    value: bool,
) !void {
    const owned_field = try alloc.dupe(u8, field);
    var field_transferred = false;
    errdefer if (!field_transferred) alloc.free(owned_field);
    const value_json = try alloc.dupe(u8, if (value) "true" else "false");
    var value_transferred = false;
    errdefer if (!value_transferred) alloc.free(value_json);
    out.* = .{
        .name = "",
        .field = owned_field,
        .op = op,
        .value_json = value_json,
    };
    field_transferred = true;
    value_transferred = true;
}

pub fn initRelationalNullCheck(
    alloc: std.mem.Allocator,
    out: *runtime_schema.RelationalCheck,
    field: []const u8,
    op: runtime_schema.RelationalCheckOp,
) !void {
    const owned_field = try alloc.dupe(u8, field);
    var field_transferred = false;
    errdefer if (!field_transferred) alloc.free(owned_field);
    out.* = .{
        .name = "",
        .field = owned_field,
        .op = op,
        .value_json = null,
    };
    field_transferred = true;
}

pub fn appendScalarBooleanCheckToBranch(
    alloc: std.mem.Allocator,
    branch: *ScalarOrCheckBranch,
    field: []const u8,
    op: runtime_schema.RelationalCheckOp,
    value: bool,
) !void {
    var check: runtime_schema.RelationalCheck = undefined;
    try initRelationalBooleanCheck(alloc, &check, field, op, value);
    var check_transferred = false;
    errdefer if (!check_transferred) plan_mod.freeRelationalCheck(alloc, check);
    try branch.append(alloc, check);
    check_transferred = true;
}

pub fn appendScalarNullCheckToBranch(
    alloc: std.mem.Allocator,
    branch: *ScalarOrCheckBranch,
    field: []const u8,
    op: runtime_schema.RelationalCheckOp,
) !void {
    var check: runtime_schema.RelationalCheck = undefined;
    try initRelationalNullCheck(alloc, &check, field, op);
    var check_transferred = false;
    errdefer if (!check_transferred) plan_mod.freeRelationalCheck(alloc, check);
    try branch.append(alloc, check);
    check_transferred = true;
}

pub fn appendRelationalCheckCloneToScalarOrBranches(
    alloc: std.mem.Allocator,
    branches: *std.ArrayListUnmanaged(ScalarOrCheckBranch),
    predicate: runtime_schema.RelationalCheck,
) !void {
    if (branches.items.len == 0) return error.UnsupportedSqlShape;
    for (branches.items) |*branch| {
        try appendRelationalCheckClone(alloc, branch, predicate);
    }
}

pub fn cloneScalarOrBranchInto(
    alloc: std.mem.Allocator,
    dest: *ScalarOrCheckBranch,
    source: ScalarOrCheckBranch,
) !void {
    for (source.items) |predicate| {
        try appendRelationalCheckClone(alloc, dest, predicate);
    }
}

pub fn appendRelationalCheckClone(
    alloc: std.mem.Allocator,
    branch: *ScalarOrCheckBranch,
    predicate: runtime_schema.RelationalCheck,
) !void {
    const cloned = try cloneQueryRelationalCheckAlloc(alloc, predicate);
    var cloned_transferred = false;
    errdefer if (!cloned_transferred) plan_mod.freeRelationalCheck(alloc, cloned);
    try branch.append(alloc, cloned);
    cloned_transferred = true;
}

pub fn freeScalarOrCheckBranches(
    alloc: std.mem.Allocator,
    branches: *std.ArrayListUnmanaged(ScalarOrCheckBranch),
) void {
    for (branches.items) |*branch| {
        freeRelationalChecks(alloc, branch.items);
        branch.deinit(alloc);
    }
    branches.deinit(alloc);
}

pub fn scalarCheckCloneAlloc(
    alloc: std.mem.Allocator,
    field: []const u8,
    op: runtime_schema.RelationalCheckOp,
    value_json: []const u8,
    collation: ?[]const u8,
) !runtime_schema.RelationalCheck {
    const owned_field = try alloc.dupe(u8, field);
    var field_transferred = false;
    errdefer if (!field_transferred) alloc.free(owned_field);
    const owned_value = try alloc.dupe(u8, value_json);
    var value_transferred = false;
    errdefer if (!value_transferred) alloc.free(owned_value);
    const owned_collation = if (collation) |source| try alloc.dupe(u8, source) else null;
    var collation_transferred = false;
    errdefer if (!collation_transferred) if (owned_collation) |owned| alloc.free(owned);

    field_transferred = true;
    value_transferred = true;
    collation_transferred = true;
    return .{
        .name = "",
        .field = owned_field,
        .op = op,
        .value_json = owned_value,
        .collation = owned_collation,
    };
}

pub fn appendBetweenScalarGroup(
    alloc: std.mem.Allocator,
    or_predicates: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsPredicateGroup),
    field: []const u8,
    first_op: runtime_schema.RelationalCheckOp,
    first_value_json: []const u8,
    second_op: runtime_schema.RelationalCheckOp,
    second_value_json: []const u8,
    collation: ?[]const u8,
) !void {
    const checks = try alloc.alloc(runtime_schema.RelationalCheck, 2);
    var checks_transferred = false;
    var first_initialized = false;
    var second_initialized = false;
    errdefer {
        if (!checks_transferred) {
            if (first_initialized) plan_mod.freeRelationalCheck(alloc, checks[0]);
            if (second_initialized) plan_mod.freeRelationalCheck(alloc, checks[1]);
            alloc.free(checks);
        }
    }

    checks[0] = try scalarCheckCloneAlloc(alloc, field, first_op, first_value_json, collation);
    first_initialized = true;
    checks[1] = try scalarCheckCloneAlloc(alloc, field, second_op, second_value_json, collation);
    second_initialized = true;

    try or_predicates.append(alloc, .{ .predicates = checks });
    checks_transferred = true;
}

pub fn appendBetweenPredicateValuesAlloc(
    alloc: std.mem.Allocator,
    predicates: *std.ArrayListUnmanaged(runtime_schema.RelationalCheck),
    or_predicates: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsPredicateGroup),
    field: []const u8,
    column: runtime_schema.RelationalColumn,
    lower_json: []const u8,
    upper_json: []const u8,
    negated: bool,
    symmetric: bool,
) !void {
    if (column.field_type == .array or column.field_type == .json) return error.InvalidSqlCatalog;

    if (symmetric) {
        if (!negated) {
            try appendBetweenScalarGroup(alloc, or_predicates, field, .gte, lower_json, .lte, upper_json, column.collation);
            try appendBetweenScalarGroup(alloc, or_predicates, field, .gte, upper_json, .lte, lower_json, column.collation);
        } else {
            try appendBetweenScalarGroup(alloc, or_predicates, field, .lt, lower_json, .lt, upper_json, column.collation);
            try appendBetweenScalarGroup(alloc, or_predicates, field, .gt, lower_json, .gt, upper_json, column.collation);
        }
        return;
    }

    if (!negated) {
        const lower = try scalarCheckCloneAlloc(alloc, field, .gte, lower_json, column.collation);
        var lower_transferred = false;
        errdefer if (!lower_transferred) plan_mod.freeRelationalCheck(alloc, lower);
        try predicates.append(alloc, lower);
        lower_transferred = true;

        const upper = try scalarCheckCloneAlloc(alloc, field, .lte, upper_json, column.collation);
        var upper_transferred = false;
        errdefer if (!upper_transferred) plan_mod.freeRelationalCheck(alloc, upper);
        try predicates.append(alloc, upper);
        upper_transferred = true;
        return;
    }

    const lower_group = try alloc.alloc(runtime_schema.RelationalCheck, 1);
    var lower_group_transferred = false;
    errdefer if (!lower_group_transferred) alloc.free(lower_group);
    lower_group[0] = try scalarCheckCloneAlloc(alloc, field, .lt, lower_json, column.collation);
    var lower_initialized = true;
    errdefer if (!lower_group_transferred and lower_initialized) plan_mod.freeRelationalCheck(alloc, lower_group[0]);

    const upper_group = try alloc.alloc(runtime_schema.RelationalCheck, 1);
    var upper_group_transferred = false;
    errdefer if (!upper_group_transferred) alloc.free(upper_group);
    upper_group[0] = try scalarCheckCloneAlloc(alloc, field, .gt, upper_json, column.collation);
    var upper_initialized = true;
    errdefer if (!upper_group_transferred and upper_initialized) plan_mod.freeRelationalCheck(alloc, upper_group[0]);

    try or_predicates.append(alloc, .{ .predicates = lower_group });
    lower_initialized = false;
    lower_group_transferred = true;
    try or_predicates.append(alloc, .{ .predicates = upper_group });
    upper_initialized = false;
    upper_group_transferred = true;
}

pub fn appendBooleanIsNotPredicateGroups(
    alloc: std.mem.Allocator,
    or_predicates: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsPredicateGroup),
    field: []const u8,
    value: bool,
) !void {
    const ne_group = try alloc.alloc(runtime_schema.RelationalCheck, 1);
    var ne_group_transferred = false;
    var ne_group_initialized = false;
    errdefer if (!ne_group_transferred) {
        if (ne_group_initialized) freeRelationalChecks(alloc, ne_group);
        alloc.free(ne_group);
    };
    try initRelationalBooleanCheck(alloc, &ne_group[0], field, .ne, value);
    ne_group_initialized = true;

    const null_group = try alloc.alloc(runtime_schema.RelationalCheck, 1);
    var null_group_transferred = false;
    var null_group_initialized = false;
    errdefer if (!null_group_transferred) {
        if (null_group_initialized) freeRelationalChecks(alloc, null_group);
        alloc.free(null_group);
    };
    try initRelationalNullCheck(alloc, &null_group[0], field, .is_null);
    null_group_initialized = true;

    try or_predicates.append(alloc, .{ .predicates = ne_group });
    ne_group_transferred = true;
    try or_predicates.append(alloc, .{ .predicates = null_group });
    null_group_transferred = true;
}

pub fn expandBooleanIsNotIntoOrBranches(
    alloc: std.mem.Allocator,
    branches: *std.ArrayListUnmanaged(ScalarOrCheckBranch),
    field: []const u8,
    value: bool,
) !void {
    if (branches.items.len == 0 or branches.items.len > max_scalar_or_expanded_branches / 2) return error.UnsupportedSqlShape;

    var expanded = std.ArrayListUnmanaged(ScalarOrCheckBranch).empty;
    errdefer freeScalarOrCheckBranches(alloc, &expanded);
    for (branches.items) |*source_branch| {
        var ne_branch = ScalarOrCheckBranch.empty;
        var ne_branch_transferred = false;
        errdefer if (!ne_branch_transferred) {
            freeRelationalChecks(alloc, ne_branch.items);
            ne_branch.deinit(alloc);
        };
        try cloneScalarOrBranchInto(alloc, &ne_branch, source_branch.*);
        try appendScalarBooleanCheckToBranch(alloc, &ne_branch, field, .ne, value);
        try expanded.append(alloc, ne_branch);
        ne_branch_transferred = true;

        var null_branch = ScalarOrCheckBranch.empty;
        var null_branch_transferred = false;
        errdefer if (!null_branch_transferred) {
            freeRelationalChecks(alloc, null_branch.items);
            null_branch.deinit(alloc);
        };
        try cloneScalarOrBranchInto(alloc, &null_branch, source_branch.*);
        try appendScalarNullCheckToBranch(alloc, &null_branch, field, .is_null);
        try expanded.append(alloc, null_branch);
        null_branch_transferred = true;
    }

    freeScalarOrCheckBranches(alloc, branches);
    branches.* = expanded;
}

pub fn appendScalarValuesJsonOrGroups(
    alloc: std.mem.Allocator,
    or_predicates: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsPredicateGroup),
    field: []const u8,
    values_json: []const u8,
    op: runtime_schema.RelationalCheckOp,
    collation: ?[]const u8,
) !void {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, values_json, .{}) catch return error.UnsupportedSqlShape;
    defer parsed.deinit();
    if (parsed.value != .array or parsed.value.array.items.len == 0 or parsed.value.array.items.len > max_scalar_or_expanded_branches) return error.UnsupportedSqlShape;

    for (parsed.value.array.items) |value| {
        const group = try alloc.alloc(runtime_schema.RelationalCheck, 1);
        var group_transferred = false;
        var group_initialized = false;
        errdefer if (!group_transferred) {
            if (group_initialized) freeRelationalChecks(alloc, group);
            alloc.free(group);
        };

        const owned_field = try alloc.dupe(u8, field);
        var field_transferred = false;
        errdefer if (!field_transferred) alloc.free(owned_field);

        const value_json = try std.json.Stringify.valueAlloc(alloc, value, .{});
        var value_transferred = false;
        errdefer if (!value_transferred) alloc.free(value_json);
        const owned_collation = if (collation) |source| try alloc.dupe(u8, source) else null;
        var collation_transferred = false;
        errdefer if (!collation_transferred) if (owned_collation) |owned| alloc.free(owned);

        group[0] = .{
            .name = "",
            .field = owned_field,
            .op = op,
            .value_json = value_json,
            .collation = owned_collation,
        };
        group_initialized = true;
        field_transferred = true;
        value_transferred = true;
        collation_transferred = true;

        try or_predicates.append(alloc, .{ .predicates = group });
        group_transferred = true;
    }
}

pub fn appendTemporalRangeContainsPredicateGroups(
    alloc: std.mem.Allocator,
    or_predicates: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsPredicateGroup),
    period: runtime_schema.RelationalPeriod,
    point_json: []const u8,
) !void {
    const start_group = try temporalRangeBoundPredicateGroupAlloc(alloc, period.start_column, .lte, point_json);
    var start_transferred = false;
    errdefer if (!start_transferred) freePredicateGroup(alloc, start_group);
    try or_predicates.append(alloc, start_group);
    start_transferred = true;

    const end_group = try temporalRangeBoundPredicateGroupAlloc(alloc, period.end_column, .gt, point_json);
    var end_transferred = false;
    errdefer if (!end_transferred) freePredicateGroup(alloc, end_group);
    try or_predicates.append(alloc, end_group);
    end_transferred = true;
}

pub fn appendTemporalRangeOverlapsPredicateGroups(
    alloc: std.mem.Allocator,
    or_predicates: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsPredicateGroup),
    period: runtime_schema.RelationalPeriod,
    range: PeriodRangeValuePair,
) !void {
    if (!std.mem.eql(u8, range.start_json, "null") and
        !std.mem.eql(u8, range.end_json, "null") and
        std.mem.eql(u8, range.start_json, range.end_json))
    {
        return error.UnsupportedSqlShape;
    }

    if (!std.mem.eql(u8, range.end_json, "null")) {
        const start_group = try temporalRangeBoundPredicateGroupAlloc(alloc, period.start_column, .lt, range.end_json);
        var start_transferred = false;
        errdefer if (!start_transferred) freePredicateGroup(alloc, start_group);
        try or_predicates.append(alloc, start_group);
        start_transferred = true;
    }

    if (!std.mem.eql(u8, range.start_json, "null")) {
        const end_group = try temporalRangeBoundPredicateGroupAlloc(alloc, period.end_column, .gt, range.start_json);
        var end_transferred = false;
        errdefer if (!end_transferred) freePredicateGroup(alloc, end_group);
        try or_predicates.append(alloc, end_group);
        end_transferred = true;
    }
}

pub fn temporalRangeBoundPredicateGroupAlloc(
    alloc: std.mem.Allocator,
    field: []const u8,
    op: runtime_schema.RelationalCheckOp,
    point_json: []const u8,
) !db_mod.types.RelationalRowsPredicateGroup {
    const predicates = try alloc.alloc(runtime_schema.RelationalCheck, 2);
    var initialized: usize = 0;
    errdefer {
        for (predicates[0..initialized]) |predicate| {
            alloc.free(predicate.field);
            if (predicate.value_json) |value| alloc.free(value);
        }
        alloc.free(predicates);
    }

    const predicate_field = try alloc.dupe(u8, field);
    var predicate_field_transferred = false;
    errdefer if (!predicate_field_transferred) alloc.free(predicate_field);

    const predicate_value = try alloc.dupe(u8, point_json);
    var predicate_value_transferred = false;
    errdefer if (!predicate_value_transferred) alloc.free(predicate_value);

    predicates[0] = .{
        .name = "",
        .field = predicate_field,
        .op = op,
        .value_json = predicate_value,
    };
    predicate_field_transferred = true;
    predicate_value_transferred = true;
    initialized = 1;

    const null_predicate_field = try alloc.dupe(u8, field);
    var null_predicate_field_transferred = false;
    errdefer if (!null_predicate_field_transferred) alloc.free(null_predicate_field);

    predicates[1] = .{
        .name = "",
        .field = null_predicate_field,
        .op = .is_null,
        .value_json = null,
    };
    null_predicate_field_transferred = true;
    initialized = 2;
    return .{ .predicates = predicates };
}

pub fn appendScalarAllEqualityPredicates(
    alloc: std.mem.Allocator,
    predicates: *std.ArrayListUnmanaged(runtime_schema.RelationalCheck),
    field: []const u8,
    values_json: []const u8,
    collation: ?[]const u8,
) !void {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, values_json, .{}) catch return error.UnsupportedSqlShape;
    defer parsed.deinit();
    if (parsed.value != .array or parsed.value.array.items.len == 0 or parsed.value.array.items.len > max_scalar_or_expanded_branches) return error.UnsupportedSqlShape;

    for (parsed.value.array.items) |value| {
        const owned_field = try alloc.dupe(u8, field);
        var field_transferred = false;
        errdefer if (!field_transferred) alloc.free(owned_field);
        const value_json = try std.json.Stringify.valueAlloc(alloc, value, .{});
        var value_transferred = false;
        errdefer if (!value_transferred) alloc.free(value_json);
        const owned_collation = if (collation) |source| try alloc.dupe(u8, source) else null;
        var collation_transferred = false;
        errdefer if (!collation_transferred) if (owned_collation) |owned| alloc.free(owned);
        try predicates.append(alloc, .{
            .name = "",
            .field = owned_field,
            .op = .eq,
            .value_json = value_json,
            .collation = owned_collation,
        });
        field_transferred = true;
        value_transferred = true;
        collation_transferred = true;
    }
}

pub fn accessPredicateGroupHasAnyPredicate(group: db_mod.types.RelationalRowsAccessPredicateGroup) bool {
    return group.predicates.len > 0 or
        group.array_any.len > 0 or
        group.array_contains.len > 0 or
        group.array_eq.len > 0 or
        group.in_predicates.len > 0 or
        group.json_contains.len > 0 or
        group.json_path_eq.len > 0 or
        group.json_path_exists.len > 0 or
        group.text_patterns.len > 0;
}

pub fn expandArrayOverlapValuesIntoAccessBranches(
    alloc: std.mem.Allocator,
    branches: *std.ArrayListUnmanaged(AccessPredicateBranch),
    field: []const u8,
    values_json: []const u8,
) !void {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, values_json, .{}) catch return error.UnsupportedSqlShape;
    defer parsed.deinit();
    if (parsed.value != .array or parsed.value.array.items.len == 0) return error.UnsupportedSqlShape;
    if (branches.items.len == 0 or branches.items.len > max_scalar_or_expanded_branches / parsed.value.array.items.len) return error.UnsupportedSqlShape;

    var expanded = std.ArrayListUnmanaged(AccessPredicateBranch).empty;
    errdefer freeAccessPredicateBranches(alloc, &expanded);
    for (branches.items) |source_branch| {
        for (parsed.value.array.items) |value| {
            var branch: AccessPredicateBranch = .{};
            var branch_transferred = false;
            errdefer if (!branch_transferred) freeAccessPredicateBranch(alloc, &branch);
            try cloneAccessPredicateBranchInto(alloc, &branch, source_branch);
            try appendArrayAnyJsonValueToAccessBranch(alloc, &branch, field, value);
            try expanded.append(alloc, branch);
            branch_transferred = true;
        }
    }

    freeAccessPredicateBranches(alloc, branches);
    branches.* = expanded;
}

pub fn appendArrayAnyJsonValueToAccessBranch(
    alloc: std.mem.Allocator,
    branch: *AccessPredicateBranch,
    field: []const u8,
    value: std.json.Value,
) !void {
    const owned_field = try alloc.dupe(u8, field);
    var field_transferred = false;
    errdefer if (!field_transferred) alloc.free(owned_field);
    const value_json = try std.json.Stringify.valueAlloc(alloc, value, .{});
    var value_transferred = false;
    errdefer if (!value_transferred) alloc.free(value_json);
    try branch.array_any.append(alloc, .{
        .field = owned_field,
        .value_json = value_json,
    });
    field_transferred = true;
    value_transferred = true;
}

pub fn appendAccessPredicateGroupToBranches(
    alloc: std.mem.Allocator,
    branches: *std.ArrayListUnmanaged(AccessPredicateBranch),
    group: db_mod.types.RelationalRowsAccessPredicateGroup,
) !void {
    if (branches.items.len == 0) return error.UnsupportedSqlShape;
    for (branches.items) |*branch| {
        try appendAccessPredicateGroupToBranch(alloc, branch, group);
    }
}

pub fn appendAccessPredicateGroupToBranch(
    alloc: std.mem.Allocator,
    branch: *AccessPredicateBranch,
    group: db_mod.types.RelationalRowsAccessPredicateGroup,
) !void {
    for (group.predicates) |predicate| try appendRelationalCheckClone(alloc, &branch.predicates, predicate);
    for (group.array_any) |predicate| try appendArrayAnyClone(alloc, &branch.array_any, predicate);
    for (group.array_contains) |predicate| try appendArrayContainsClone(alloc, &branch.array_contains, predicate);
    for (group.array_eq) |predicate| try appendArrayEqClone(alloc, &branch.array_eq, predicate);
    for (group.in_predicates) |predicate| try appendInPredicateClone(alloc, &branch.in_predicates, predicate);
    for (group.json_contains) |predicate| try appendJsonContainsClone(alloc, &branch.json_contains, predicate);
    for (group.json_path_eq) |predicate| try appendJsonPathEqClone(alloc, &branch.json_path_eq, predicate);
    for (group.json_path_exists) |predicate| try appendJsonPathExistsClone(alloc, &branch.json_path_exists, predicate);
    for (group.text_patterns) |predicate| try appendTextPatternClone(alloc, &branch.text_patterns, predicate);
}

pub fn cloneAccessPredicateBranchInto(
    alloc: std.mem.Allocator,
    dest: *AccessPredicateBranch,
    source: AccessPredicateBranch,
) !void {
    try appendAccessPredicateGroupToBranch(alloc, dest, .{
        .predicates = source.predicates.items,
        .array_any = source.array_any.items,
        .array_contains = source.array_contains.items,
        .array_eq = source.array_eq.items,
        .in_predicates = source.in_predicates.items,
        .json_contains = source.json_contains.items,
        .json_path_eq = source.json_path_eq.items,
        .json_path_exists = source.json_path_exists.items,
        .text_patterns = source.text_patterns.items,
    });
}

pub fn accessPredicateBranchToGroupAlloc(
    alloc: std.mem.Allocator,
    branch: *AccessPredicateBranch,
) !db_mod.types.RelationalRowsAccessPredicateGroup {
    var group: db_mod.types.RelationalRowsAccessPredicateGroup = .{};
    errdefer freeAccessPredicateGroup(alloc, group);
    group.predicates = try branch.predicates.toOwnedSlice(alloc);
    branch.predicates = .empty;
    group.array_any = try branch.array_any.toOwnedSlice(alloc);
    branch.array_any = .empty;
    group.array_contains = try branch.array_contains.toOwnedSlice(alloc);
    branch.array_contains = .empty;
    group.array_eq = try branch.array_eq.toOwnedSlice(alloc);
    branch.array_eq = .empty;
    group.in_predicates = try branch.in_predicates.toOwnedSlice(alloc);
    branch.in_predicates = .empty;
    group.json_contains = try branch.json_contains.toOwnedSlice(alloc);
    branch.json_contains = .empty;
    group.json_path_eq = try branch.json_path_eq.toOwnedSlice(alloc);
    branch.json_path_eq = .empty;
    group.json_path_exists = try branch.json_path_exists.toOwnedSlice(alloc);
    branch.json_path_exists = .empty;
    group.text_patterns = try branch.text_patterns.toOwnedSlice(alloc);
    branch.text_patterns = .empty;
    return group;
}

pub fn appendArrayAnyClone(
    alloc: std.mem.Allocator,
    branch: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsArrayAnyPredicate),
    predicate: db_mod.types.RelationalRowsArrayAnyPredicate,
) !void {
    const field = try alloc.dupe(u8, predicate.field);
    var field_transferred = false;
    errdefer if (!field_transferred) alloc.free(field);
    const value_json = try alloc.dupe(u8, predicate.value_json);
    var value_transferred = false;
    errdefer if (!value_transferred) alloc.free(value_json);
    try branch.append(alloc, .{ .field = field, .value_json = value_json });
    field_transferred = true;
    value_transferred = true;
}

pub fn appendArrayContainsClone(
    alloc: std.mem.Allocator,
    branch: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsArrayContainsPredicate),
    predicate: db_mod.types.RelationalRowsArrayContainsPredicate,
) !void {
    const field = try alloc.dupe(u8, predicate.field);
    var field_transferred = false;
    errdefer if (!field_transferred) alloc.free(field);
    const value_json = try alloc.dupe(u8, predicate.value_json);
    var value_transferred = false;
    errdefer if (!value_transferred) alloc.free(value_json);
    try branch.append(alloc, .{ .field = field, .value_json = value_json });
    field_transferred = true;
    value_transferred = true;
}

pub fn appendArrayEqClone(
    alloc: std.mem.Allocator,
    branch: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsArrayEqPredicate),
    predicate: db_mod.types.RelationalRowsArrayEqPredicate,
) !void {
    const field = try alloc.dupe(u8, predicate.field);
    var field_transferred = false;
    errdefer if (!field_transferred) alloc.free(field);
    const value_json = try alloc.dupe(u8, predicate.value_json);
    var value_transferred = false;
    errdefer if (!value_transferred) alloc.free(value_json);
    try branch.append(alloc, .{ .field = field, .value_json = value_json });
    field_transferred = true;
    value_transferred = true;
}

pub fn appendInPredicateClone(
    alloc: std.mem.Allocator,
    branch: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsInPredicate),
    predicate: db_mod.types.RelationalRowsInPredicate,
) !void {
    const field = try alloc.dupe(u8, predicate.field);
    var field_transferred = false;
    errdefer if (!field_transferred) alloc.free(field);
    const values_json = try alloc.dupe(u8, predicate.values_json);
    var values_transferred = false;
    errdefer if (!values_transferred) alloc.free(values_json);
    const collation = if (predicate.collation) |value| try alloc.dupe(u8, value) else null;
    var collation_transferred = false;
    errdefer if (!collation_transferred) if (collation) |value| alloc.free(value);
    try branch.append(alloc, .{ .field = field, .values_json = values_json, .negated = predicate.negated, .collation = collation });
    field_transferred = true;
    values_transferred = true;
    collation_transferred = true;
}

pub fn appendJsonContainsClone(
    alloc: std.mem.Allocator,
    branch: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsJsonContainsPredicate),
    predicate: db_mod.types.RelationalRowsJsonContainsPredicate,
) !void {
    const field = try alloc.dupe(u8, predicate.field);
    var field_transferred = false;
    errdefer if (!field_transferred) alloc.free(field);
    const value_json = try alloc.dupe(u8, predicate.value_json);
    var value_transferred = false;
    errdefer if (!value_transferred) alloc.free(value_json);
    try branch.append(alloc, .{ .field = field, .value_json = value_json });
    field_transferred = true;
    value_transferred = true;
}

pub fn appendJsonPathEqClone(
    alloc: std.mem.Allocator,
    branch: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsJsonPathEqPredicate),
    predicate: db_mod.types.RelationalRowsJsonPathEqPredicate,
) !void {
    const field = try alloc.dupe(u8, predicate.field);
    var field_transferred = false;
    errdefer if (!field_transferred) alloc.free(field);
    const path = try alloc.dupe(u8, predicate.path);
    var path_transferred = false;
    errdefer if (!path_transferred) alloc.free(path);
    const value_json = try alloc.dupe(u8, predicate.value_json);
    var value_transferred = false;
    errdefer if (!value_transferred) alloc.free(value_json);
    try branch.append(alloc, .{ .field = field, .path = path, .value_json = value_json });
    field_transferred = true;
    path_transferred = true;
    value_transferred = true;
}

pub fn appendJsonPathExistsClone(
    alloc: std.mem.Allocator,
    branch: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsJsonPathExistsPredicate),
    predicate: db_mod.types.RelationalRowsJsonPathExistsPredicate,
) !void {
    const field = try alloc.dupe(u8, predicate.field);
    var field_transferred = false;
    errdefer if (!field_transferred) alloc.free(field);
    const path = try alloc.dupe(u8, predicate.path);
    var path_transferred = false;
    errdefer if (!path_transferred) alloc.free(path);
    try branch.append(alloc, .{ .field = field, .path = path });
    field_transferred = true;
    path_transferred = true;
}

pub fn appendTextPatternClone(
    alloc: std.mem.Allocator,
    branch: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsTextPatternPredicate),
    predicate: db_mod.types.RelationalRowsTextPatternPredicate,
) !void {
    const field = try alloc.dupe(u8, predicate.field);
    var field_transferred = false;
    errdefer if (!field_transferred) alloc.free(field);
    const pattern = try alloc.dupe(u8, predicate.pattern);
    var pattern_transferred = false;
    errdefer if (!pattern_transferred) alloc.free(pattern);
    try branch.append(alloc, .{
        .field = field,
        .pattern = pattern,
        .case_insensitive = predicate.case_insensitive,
        .negated = predicate.negated,
    });
    field_transferred = true;
    pattern_transferred = true;
}

pub fn freeAccessPredicateBranches(
    alloc: std.mem.Allocator,
    branches: *std.ArrayListUnmanaged(AccessPredicateBranch),
) void {
    for (branches.items) |*branch| freeAccessPredicateBranch(alloc, branch);
    branches.deinit(alloc);
}

pub fn freeAccessPredicateBranch(alloc: std.mem.Allocator, branch: *AccessPredicateBranch) void {
    freeRelationalChecks(alloc, branch.predicates.items);
    branch.predicates.deinit(alloc);
    freeArrayAny(alloc, branch.array_any.items);
    branch.array_any.deinit(alloc);
    freeArrayContains(alloc, branch.array_contains.items);
    branch.array_contains.deinit(alloc);
    freeArrayEq(alloc, branch.array_eq.items);
    branch.array_eq.deinit(alloc);
    freeInPredicates(alloc, branch.in_predicates.items);
    branch.in_predicates.deinit(alloc);
    freeJsonContains(alloc, branch.json_contains.items);
    branch.json_contains.deinit(alloc);
    freeJsonPathEq(alloc, branch.json_path_eq.items);
    branch.json_path_eq.deinit(alloc);
    freeJsonPathExists(alloc, branch.json_path_exists.items);
    branch.json_path_exists.deinit(alloc);
    freeTextPatterns(alloc, branch.text_patterns.items);
    branch.text_patterns.deinit(alloc);
}

pub fn wrapBooleanNotExpressionAlloc(
    alloc: std.mem.Allocator,
    expression: db_mod.types.RelationalRowsExpression,
) !db_mod.types.RelationalRowsExpression {
    const operands = try alloc.alloc(db_mod.types.RelationalRowsExpression, 1);
    var operands_transferred = false;
    errdefer if (!operands_transferred) alloc.free(operands);
    operands[0] = expression;
    operands_transferred = true;
    return .{
        .kind = .bool_not,
        .operands = operands,
    };
}

pub fn expressionLikePatternExpressionAlloc(
    alloc: std.mem.Allocator,
    expression: db_mod.types.RelationalRowsExpression,
    pattern: []const u8,
    case_insensitive: bool,
) !db_mod.types.RelationalRowsExpression {
    const source_expression = try cloneExpressionAlloc(alloc, expression);
    var source_expression_transferred = false;
    errdefer if (!source_expression_transferred) freeExpression(alloc, source_expression);
    const pattern_json = try std.json.Stringify.valueAlloc(alloc, std.json.Value{ .string = pattern }, .{});
    var pattern_json_transferred = false;
    errdefer if (!pattern_json_transferred) alloc.free(pattern_json);
    const operands = try alloc.alloc(db_mod.types.RelationalRowsExpression, 2);
    var operands_transferred = false;
    errdefer if (!operands_transferred) alloc.free(operands);
    operands[0] = source_expression;
    operands[1] = .{
        .kind = .value,
        .value_json = pattern_json,
    };
    source_expression_transferred = true;
    pattern_json_transferred = true;
    operands_transferred = true;
    return .{
        .kind = if (case_insensitive) .ilike else .like,
        .operands = operands,
    };
}

pub fn expressionLikeConditionAlloc(
    alloc: std.mem.Allocator,
    type_context: expr_type.RowExpressionTypeContext,
    expression: db_mod.types.RelationalRowsExpression,
    normalized_pattern: []const u8,
    case_insensitive: bool,
    negated: bool,
) !db_mod.types.RelationalRowsExpressionCondition {
    try type_context.validateTextRowExpression(expression);
    const lhs = try expressionLikePatternExpressionAlloc(alloc, expression, normalized_pattern, case_insensitive);
    var lhs_transferred = false;
    errdefer if (!lhs_transferred) freeExpression(alloc, lhs);

    const rhs_json = try alloc.dupe(u8, value_mod.booleanJson(!negated));
    var rhs_json_transferred = false;
    errdefer if (!rhs_json_transferred) alloc.free(rhs_json);
    const rhs = try alloc.alloc(db_mod.types.RelationalRowsExpression, 1);
    var rhs_transferred = false;
    errdefer if (!rhs_transferred) {
        if (rhs_json_transferred) freeExpression(alloc, rhs[0]);
        alloc.free(rhs);
    };
    rhs[0] = .{
        .kind = .value,
        .value_json = rhs_json,
    };
    rhs_json_transferred = true;
    lhs_transferred = true;
    rhs_transferred = true;

    return .{
        .lhs = lhs,
        .op = .eq,
        .rhs = rhs,
    };
}

pub fn parseExpressionLikeConditionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    params: []const value_mod.SqlValue,
    type_context: expr_type.RowExpressionTypeContext,
    expression: db_mod.types.RelationalRowsExpression,
    case_insensitive: bool,
    negated: bool,
) !db_mod.types.RelationalRowsExpressionCondition {
    const raw_pattern = try value_mod.parseSqlStringValueAlloc(alloc, tokens, pos, params);
    defer alloc.free(raw_pattern);
    const normalized_pattern = if (parser.matchKeyword(tokens, pos, "escape")) blk: {
        const escape = try value_mod.parseSqlStringValueAlloc(alloc, tokens, pos, params);
        defer alloc.free(escape);
        break :blk try normalizeSqlLikePatternEscapeAlloc(alloc, raw_pattern, escape);
    } else try alloc.dupe(u8, raw_pattern);
    defer alloc.free(normalized_pattern);
    return try expressionLikeConditionAlloc(alloc, type_context, expression, normalized_pattern, case_insensitive, negated);
}

pub fn expressionRegexpMatchConditionAlloc(
    alloc: std.mem.Allocator,
    type_context: expr_type.RowExpressionTypeContext,
    expression: db_mod.types.RelationalRowsExpression,
    pattern_expression: db_mod.types.RelationalRowsExpression,
    case_insensitive: bool,
    negated: bool,
) !db_mod.types.RelationalRowsExpressionCondition {
    try type_context.validateTextRowExpression(expression);
    try type_context.validateTextRowExpression(pattern_expression);

    const source_expression = try cloneExpressionAlloc(alloc, expression);
    var source_expression_transferred = false;
    errdefer if (!source_expression_transferred) freeExpression(alloc, source_expression);

    const pattern_expression_clone = try cloneExpressionAlloc(alloc, pattern_expression);
    var pattern_expression_transferred = false;
    errdefer if (!pattern_expression_transferred) freeExpression(alloc, pattern_expression_clone);

    const case_json = try alloc.dupe(u8, value_mod.booleanJson(case_insensitive));
    var case_json_transferred = false;
    errdefer if (!case_json_transferred) alloc.free(case_json);
    const operands = try alloc.alloc(db_mod.types.RelationalRowsExpression, 3);
    var operands_transferred = false;
    errdefer if (!operands_transferred) {
        freeExpression(alloc, operands[0]);
        freeExpression(alloc, operands[1]);
        freeExpression(alloc, operands[2]);
        alloc.free(operands);
    };
    operands[0] = source_expression;
    operands[1] = pattern_expression_clone;
    operands[2] = .{
        .kind = .value,
        .value_json = case_json,
    };
    source_expression_transferred = true;
    pattern_expression_transferred = true;
    case_json_transferred = true;

    const rhs_json = try alloc.dupe(u8, value_mod.booleanJson(!negated));
    var rhs_json_transferred = false;
    errdefer if (!rhs_json_transferred) alloc.free(rhs_json);
    const rhs = try alloc.alloc(db_mod.types.RelationalRowsExpression, 1);
    var rhs_transferred = false;
    errdefer if (!rhs_transferred) {
        if (rhs_json_transferred) freeExpression(alloc, rhs[0]);
        alloc.free(rhs);
    };
    rhs[0] = .{
        .kind = .value,
        .value_json = rhs_json,
    };
    rhs_json_transferred = true;
    rhs_transferred = true;
    operands_transferred = true;

    return .{
        .lhs = .{
            .kind = .regexp_match,
            .operands = operands,
        },
        .op = .eq,
        .rhs = rhs,
    };
}

pub fn expressionLikeSetExpressionAlloc(
    alloc: std.mem.Allocator,
    expression: db_mod.types.RelationalRowsExpression,
    values_json: []const u8,
    case_insensitive: bool,
    negated: bool,
    quantifier: ast.SqlPatternQuantifier,
) !db_mod.types.RelationalRowsExpression {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, values_json, .{}) catch return error.UnsupportedSqlShape;
    defer parsed.deinit();
    if (parsed.value != .array or parsed.value.array.items.len == 0 or parsed.value.array.items.len > max_scalar_or_expanded_branches) return error.UnsupportedSqlShape;

    var operands = std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpression).empty;
    var operands_transferred = false;
    errdefer {
        if (!operands_transferred) {
            for (operands.items) |operand| freeExpression(alloc, operand);
            operands.deinit(alloc);
        }
    }

    for (parsed.value.array.items) |value| {
        if (value != .string) return error.UnsupportedSqlShape;
        var operand = try expressionLikePatternExpressionAlloc(alloc, expression, value.string, case_insensitive);
        var operand_transferred = false;
        errdefer if (!operand_transferred) freeExpression(alloc, operand);
        if (negated) {
            operand = try wrapBooleanNotExpressionAlloc(alloc, operand);
        }
        try operands.append(alloc, operand);
        operand_transferred = true;
    }

    if (operands.items.len == 1) {
        const out = operands.items[0];
        operands.clearRetainingCapacity();
        operands.deinit(alloc);
        operands_transferred = true;
        return out;
    }

    const owned_operands = try operands.toOwnedSlice(alloc);
    operands = .empty;
    operands_transferred = true;
    return .{
        .kind = if (quantifier == .all) .bool_and else .bool_or,
        .operands = owned_operands,
    };
}

pub fn expressionLikeSetConditionAlloc(
    alloc: std.mem.Allocator,
    type_context: expr_type.RowExpressionTypeContext,
    expression: db_mod.types.RelationalRowsExpression,
    values_json: []const u8,
    case_insensitive: bool,
    negated: bool,
    quantifier: ast.SqlPatternQuantifier,
) !db_mod.types.RelationalRowsExpressionCondition {
    try type_context.validateTextRowExpression(expression);
    const lhs = try expressionLikeSetExpressionAlloc(alloc, expression, values_json, case_insensitive, negated, quantifier);
    var lhs_transferred = false;
    errdefer if (!lhs_transferred) freeExpression(alloc, lhs);
    const rhs_json = try alloc.dupe(u8, "true");
    var rhs_json_transferred = false;
    errdefer if (!rhs_json_transferred) alloc.free(rhs_json);
    const rhs = try alloc.alloc(db_mod.types.RelationalRowsExpression, 1);
    var rhs_transferred = false;
    errdefer if (!rhs_transferred) {
        if (rhs_json_transferred) freeExpression(alloc, rhs[0]);
        alloc.free(rhs);
    };
    rhs[0] = .{
        .kind = .value,
        .value_json = rhs_json,
    };
    rhs_json_transferred = true;
    lhs_transferred = true;
    rhs_transferred = true;
    return .{
        .lhs = lhs,
        .op = .eq,
        .rhs = rhs,
    };
}

pub fn parseExpressionLikeSetConditionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    params: []const value_mod.SqlValue,
    type_context: expr_type.RowExpressionTypeContext,
    expression: db_mod.types.RelationalRowsExpression,
    case_insensitive: bool,
    negated: bool,
) !db_mod.types.RelationalRowsExpressionCondition {
    const quantifier = expr_token.matchAnySomeOrAllKeyword(tokens, pos) orelse return error.UnsupportedSqlShape;
    try parser.expectToken(tokens, pos, .lparen);
    const values_json = try value_mod.parseJsonArrayValueAlloc(alloc, tokens, pos, params);
    defer alloc.free(values_json);
    try parser.expectToken(tokens, pos, .rparen);
    return try expressionLikeSetConditionAlloc(alloc, type_context, expression, values_json, case_insensitive, negated, quantifier);
}

pub fn appendExpressionInPredicateGroupsAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    params: []const value_mod.SqlValue,
    type_context: expr_type.RowExpressionTypeContext,
    expression_groups: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionPredicateGroup),
    lhs: db_mod.types.RelationalRowsExpression,
) !void {
    const values_json = try value_mod.parseSqlInValuesJsonAlloc(alloc, tokens, pos, params);
    defer alloc.free(values_json);
    try appendExpressionValuesJsonOrGroups(alloc, type_context, expression_groups, lhs, values_json);
}

pub fn appendTextPatternPredicateAlloc(
    alloc: std.mem.Allocator,
    text_patterns: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsTextPatternPredicate),
    field: []const u8,
    column: runtime_schema.RelationalColumn,
    pattern: []const u8,
    case_insensitive: bool,
    negated: bool,
) !void {
    if (column.field_type != .keyword and column.field_type != .text and column.field_type != .link) return error.InvalidSqlCatalog;
    const predicate_field = try alloc.dupe(u8, field);
    var field_transferred = false;
    errdefer if (!field_transferred) alloc.free(predicate_field);
    const predicate_pattern = try alloc.dupe(u8, pattern);
    var pattern_transferred = false;
    errdefer if (!pattern_transferred) alloc.free(predicate_pattern);
    try text_patterns.append(alloc, .{
        .field = predicate_field,
        .pattern = predicate_pattern,
        .case_insensitive = case_insensitive,
        .negated = negated,
    });
    field_transferred = true;
    pattern_transferred = true;
}

pub fn parseAndAppendTextPatternPredicateAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    params: []const value_mod.SqlValue,
    text_patterns: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsTextPatternPredicate),
    field: []const u8,
    column: runtime_schema.RelationalColumn,
    case_insensitive: bool,
    negated: bool,
    realtime_ns: u64,
) !void {
    const pattern_json = try value_mod.parseSqlColumnValueAlloc(alloc, tokens, pos, params, column, realtime_ns);
    defer alloc.free(pattern_json);
    const parsed_pattern = try jsonStringLiteralValueAlloc(alloc, pattern_json);
    defer alloc.free(parsed_pattern);
    const pattern = if (parser.matchKeyword(tokens, pos, "escape")) blk: {
        const escape = try value_mod.parseSqlStringValueAlloc(alloc, tokens, pos, params);
        defer alloc.free(escape);
        break :blk try normalizeSqlLikePatternEscapeAlloc(alloc, parsed_pattern, escape);
    } else try alloc.dupe(u8, parsed_pattern);
    defer alloc.free(pattern);
    try appendTextPatternPredicateAlloc(alloc, text_patterns, field, column, pattern, case_insensitive, negated);
}

pub fn parenthesizedWhereHasTopLevelOr(tokens: []const Token, pos: usize) bool {
    if (pos >= tokens.len or tokens[pos].kind != .lparen) return false;
    const close_index = parser.findMatchingRParenIndex(tokens, pos) orelse return false;
    var depth: usize = 0;
    var index = pos + 1;
    while (index < close_index) : (index += 1) {
        switch (tokens[index].kind) {
            .lparen => depth += 1,
            .rparen => if (depth == 0) return false else {
                depth -= 1;
            },
            .identifier => if (depth == 0 and tokens[index].matchesKeywordTag(.@"or")) return true,
            else => {},
        }
    }
    return false;
}

pub fn whereTopLevelOrHasExpressionPredicateStart(tokens: []const Token, pos: usize) bool {
    var depth: usize = 0;
    var index = pos;
    var expect_predicate_start = true;
    while (index < tokens.len) : (index += 1) {
        const token = tokens[index];
        switch (token.kind) {
            .lparen => {
                if (depth == 0 and expect_predicate_start) {
                    const inner = parser.predicateStartIndexAfterOpenParens(tokens, index);
                    if (topLevelOrExpressionPredicateStartCanStartAt(tokens, inner)) return true;
                }
                depth += 1;
                expect_predicate_start = false;
            },
            .rparen => if (depth > 0) {
                depth -= 1;
            },
            .semicolon => if (depth == 0) return false,
            .identifier => if (depth == 0) {
                if (expr_token.sqlWhereTailClauseKeywordToken(token)) return false;
                if (token.matchesKeywordTag(.@"and") or token.matchesKeywordTag(.@"or")) {
                    expect_predicate_start = true;
                    continue;
                }
                if (expect_predicate_start and topLevelOrExpressionPredicateStartCanStartAt(tokens, index)) return true;
                expect_predicate_start = false;
            },
            .number, .string, .placeholder, .minus => if (depth == 0) {
                if (expect_predicate_start and topLevelOrExpressionPredicateStartCanStartAt(tokens, index)) return true;
                expect_predicate_start = false;
            },
            else => if (depth == 0 and expect_predicate_start) {
                expect_predicate_start = false;
            },
        }
    }
    return false;
}

fn topLevelOrExpressionPredicateStartCanStartAt(tokens: []const Token, index: usize) bool {
    return expr_parse.expressionCanStartAt(tokens, index) or
        textPatternSetPredicateCanStartAt(tokens, index) or
        expr_operator.jsonKeySetExpressionCanStartAt(tokens, index) or
        expr_operator.jsonExtractExpressionPredicateCanStartAt(tokens, index) or
        expr_operator.jsonExtractNullTestPredicateCanStartAt(tokens, index);
}

pub fn whereTopLevelOrHasAccessPredicate(tokens: []const Token, pos: usize) bool {
    var depth: usize = 0;
    var index = pos;
    while (index < tokens.len) : (index += 1) {
        const token = tokens[index];
        switch (token.kind) {
            .lparen => depth += 1,
            .rparen => if (depth > 0) {
                depth -= 1;
            },
            .semicolon => if (depth == 0) return false,
            .arrow_text, .arrow_json, .path_arrow_text, .path_arrow_json, .at_contains, .range_overlap, .question => return true,
            .identifier => {
                if (depth == 0 and expr_token.sqlWhereTailClauseKeywordToken(token)) return false;
                if (token.matchesKeywordTag(.like) or
                    token.matchesKeywordTag(.ilike))
                {
                    return true;
                }
                if (index + 2 < tokens.len and
                    (tokens[index + 1].kind == .eq or tokens[index + 1].kind == .neq) and
                    tokens[index + 2].matchesKeywordTag(.array))
                {
                    return true;
                }
            },
            else => {},
        }
    }
    return false;
}

pub fn stringToArrayPredicateIsContainment(tokens: []const Token, pos: usize) bool {
    return stringToArrayContainmentOperatorTokenIndex(tokens, pos) != null;
}

pub fn stringToArrayContainmentOperatorTokenIndex(tokens: []const Token, pos: usize) ?usize {
    if (!expr_token.peekStringToArrayFunctionCall(tokens, pos)) return null;
    var depth: usize = 0;
    var index = pos;
    while (index < tokens.len) : (index += 1) {
        const kind = tokens[index].kind;
        switch (kind) {
            .lparen => depth += 1,
            .rparen => {
                if (depth == 0) return null;
                depth -= 1;
                if (depth == 0) {
                    if (index + 1 < tokens.len and tokens[index + 1].kind == .at_contains) return index + 1;
                    return null;
                }
            },
            else => {},
        }
    }
    return null;
}

pub fn valueEqualsAnyArrayPredicateCanStart(tokens: []const Token, pos: usize) bool {
    if (pos >= tokens.len) return false;
    const value_end = switch (tokens[pos].kind) {
        .string, .number, .placeholder => pos + 1,
        .minus => blk: {
            if (pos + 1 >= tokens.len or tokens[pos + 1].kind != .number) return false;
            break :blk pos + 2;
        },
        .identifier => blk: {
            if (!tokens[pos].matchesKeywordTag(.null) and
                !tokens[pos].matchesKeywordTag(.true) and
                !tokens[pos].matchesKeywordTag(.false))
            {
                return false;
            }
            break :blk pos + 1;
        },
        else => return false,
    };
    if (value_end + 3 >= tokens.len) return false;
    return tokens[value_end].kind == .eq and
        tokens[value_end + 1].kind == .identifier and
        (tokens[value_end + 1].matchesKeywordTag(.any) or
            tokens[value_end + 1].matchesKeywordTag(.some)) and
        tokens[value_end + 2].kind == .lparen and
        tokens[value_end + 3].kind == .identifier;
}

pub fn textPatternSetPredicateCanStartAt(tokens: []const Token, index: usize) bool {
    if (index >= tokens.len) return false;
    if (tokens[index].kind != .identifier and !expr_parse.expressionCanStartAt(tokens, index)) return false;
    var depth: usize = 0;
    var i = index;
    while (i < tokens.len) : (i += 1) {
        const token = tokens[i];
        switch (token.kind) {
            .lparen => depth += 1,
            .rparen => {
                if (depth == 0) return false;
                depth -= 1;
            },
            .semicolon, .comma => if (depth == 0) return false,
            .identifier => if (depth == 0) {
                if (token.matchesKeywordTag(.like) or
                    token.matchesKeywordTag(.ilike))
                {
                    return expr_token.tokenAtIsAnySomeOrAll(tokens, i + 1);
                }
                if (token.matchesKeywordTag(.not) and i + 2 < tokens.len and
                    (tokens[i + 1].matchesKeywordTag(.like) or
                        tokens[i + 1].matchesKeywordTag(.ilike)))
                {
                    return expr_token.tokenAtIsAnySomeOrAll(tokens, i + 2);
                }
                if (expr_token.rowExpressionBoundaryKeywordToken(token)) return false;
            },
            else => {},
        }
    }
    return false;
}

pub fn peekSimpleScalarSetPredicate(tokens: []const Token, pos: usize) bool {
    if (pos + 1 >= tokens.len or tokens[pos].kind != .identifier) return false;
    if (pos + 2 < tokens.len and
        tokens[pos + 1].matchesKeywordTag(.not) and
        tokens[pos + 2].matchesKeywordTag(.in))
    {
        return true;
    }
    if (tokens[pos + 1].matchesKeywordTag(.in)) return true;
    if (pos + 2 >= tokens.len) return false;
    if (tokens[pos + 1].kind != .eq and tokens[pos + 1].kind != .neq) return false;
    return tokens[pos + 2].matchesKeywordTag(.any) or
        tokens[pos + 2].matchesKeywordTag(.some) or
        tokens[pos + 2].matchesKeywordTag(.all);
}

pub fn canParseScalarNotWhere(tokens: []const Token, pos: usize) bool {
    if (!parser.peekKeywordTag(tokens, pos, .not) or pos + 1 >= tokens.len or tokens[pos + 1].kind != .lparen) return false;
    var i = pos + 2;
    var depth: usize = 1;
    while (i < tokens.len) : (i += 1) {
        const token = tokens[i];
        switch (token.kind) {
            .lparen => return false,
            .rparen => {
                depth -= 1;
                if (depth == 0) return true;
            },
            .arrow_text, .arrow_json, .path_arrow_text, .path_arrow_json, .at_contains, .range_overlap, .question => return false,
            .identifier => {
                if (token.matchesKeywordTag(.any) or
                    token.matchesKeywordTag(.some) or
                    token.matchesKeywordTag(.between) or
                    token.matchesKeywordTag(.in) or
                    token.matchesKeywordTag(.exists))
                {
                    return false;
                }
                if (token.matchesKeywordTag(.not)) {
                    if (i == 0 or tokens[i - 1].kind != .identifier or
                        !tokens[i - 1].matchesKeywordTag(.is))
                    {
                        return false;
                    }
                }
            },
            else => {},
        }
    }
    return false;
}

pub fn canParseExpressionNotWhere(tokens: []const Token, pos: usize) bool {
    if (!parser.peekKeywordTag(tokens, pos, .not) or pos + 2 >= tokens.len or tokens[pos + 1].kind != .lparen) return false;
    const inner = parser.predicateStartIndexAfterOpenParens(tokens, pos + 1);
    return expr_parse.expressionCanStartAt(tokens, inner) or
        expr_operator.jsonKeySetExpressionCanStartAt(tokens, inner) or
        expr_operator.jsonExtractExpressionPredicateCanStartAt(tokens, inner) or
        expr_operator.jsonExtractNullTestPredicateCanStartAt(tokens, inner);
}

pub fn canParseBareBooleanWhereExpression(tokens: []const Token, pos: usize, schema: runtime_schema.TableSchema) bool {
    if (!booleanExpressionCanStartAt(tokens, pos, schema)) return false;

    var depth: usize = 0;
    var i = pos;
    var saw_boolean_syntax = false;
    var saw_token = false;
    while (i < tokens.len) : (i += 1) {
        const token = tokens[i];
        switch (token.kind) {
            .semicolon, .comma => if (depth == 0) break,
            .rparen => {
                if (depth == 0) break;
                depth -= 1;
            },
            .lparen => {
                depth += 1;
                saw_token = true;
            },
            .eq, .neq, .gt, .gte, .lt, .lte, .arrow_text, .arrow_json, .path_arrow_text, .path_arrow_json, .at_contains, .range_overlap, .question, .regex_match, .regex_imatch, .regex_not_match, .regex_not_imatch => return false,
            .identifier => {
                if (depth == 0 and expr_token.sqlWhereTailClauseKeywordToken(token)) break;
                if (expr_token.sqlKeywordStartsScalarPredicate(token.text)) return false;
                if (token.matchesKeywordTag(.@"and") or
                    token.matchesKeywordTag(.@"or") or
                    token.matchesKeywordTag(.not))
                {
                    saw_boolean_syntax = true;
                }
                saw_token = true;
            },
            else => saw_token = true,
        }
    }
    if (!saw_token) return false;
    return saw_boolean_syntax or singleBooleanExpressionCanStartAt(tokens, pos, schema);
}

pub fn booleanExpressionCanStartAt(tokens: []const Token, index: usize, schema: runtime_schema.TableSchema) bool {
    if (index >= tokens.len) return false;
    if (tokens[index].kind == .lparen) {
        const inner = parser.predicateStartIndexAfterOpenParens(tokens, index);
        return booleanExpressionCanStartAt(tokens, inner, schema);
    }
    return singleBooleanExpressionCanStartAt(tokens, index, schema);
}

fn singleBooleanExpressionCanStartAt(tokens: []const Token, index: usize, schema: runtime_schema.TableSchema) bool {
    if (index >= tokens.len) return false;
    const token = tokens[index];
    if (token.kind != .identifier) return false;
    return token.matchesKeywordTag(.not) or
        token.matchesKeywordTag(.true) or
        token.matchesKeywordTag(.false) or
        token.matchesKeywordTag(.case) or
        token.matchesKeywordTag(.cast) or
        token.matchesKeywordTag(.coalesce) or
        token.matchesKeywordTag(.nullif) or
        expr_token.sqlTokenIsRegexpMatchFunction(token) or
        expr_token.sqlTokenIsStartsWithFunction(token) or
        binder.relationalColumnForField(schema, token.text, null) != null;
}

pub fn canParseAccessNotWhere(tokens: []const Token, pos: usize) bool {
    if (!parser.peekKeywordTag(tokens, pos, .not)) return false;
    var i = pos + 1;
    while (i < tokens.len and tokens[i].kind == .lparen) : (i += 1) {}
    if (i >= tokens.len) return false;
    var depth: usize = 0;
    while (i < tokens.len) : (i += 1) {
        const token = tokens[i];
        switch (token.kind) {
            .lparen => depth += 1,
            .rparen => if (depth == 0) {
                return false;
            } else {
                depth -= 1;
            },
            .arrow_text, .arrow_json, .path_arrow_text, .path_arrow_json, .at_contains, .range_overlap, .question => return true,
            .identifier => {
                if (token.matchesKeywordTag(.like) or
                    token.matchesKeywordTag(.ilike))
                {
                    return true;
                }
                if (i + 2 < tokens.len and
                    (tokens[i + 1].kind == .eq or tokens[i + 1].kind == .neq) and
                    tokens[i + 2].matchesKeywordTag(.array))
                {
                    return true;
                }
            },
            else => {},
        }
    }
    return false;
}

pub fn canParseExpressionWhereCondition(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: usize,
    schema: runtime_schema.TableSchema,
    field_expression_qualifiers: []const []const u8,
    returning_expression_qualifiers: []const []const u8,
    defer_row_expression_field_validation: bool,
) !bool {
    if (value_mod.peekStandaloneSqlBooleanLiteral(tokens, pos) != null) return true;
    if (defer_row_expression_field_validation and parser.peekKind(tokens, pos, .identifier) and pos + 1 < tokens.len) {
        switch (tokens[pos + 1].kind) {
            .eq, .neq, .gt, .gte, .lt, .lte => {
                if (binder.relationalColumnForField(schema, tokens[pos].text, null) != null) return false;
                return true;
            },
            else => if (tokens[pos + 1].matchesKeywordTag(.is) or
                tokens[pos + 1].matchesKeywordTag(.not) or
                tokens[pos + 1].matchesKeywordTag(.in) or
                tokens[pos + 1].matchesKeywordTag(.between))
            {
                if (binder.relationalColumnForField(schema, tokens[pos].text, null) != null) return false;
                return true;
            },
        }
    }
    if (expr_parse.rowExpressionHasTopLevelPipeConcat(tokens, pos)) return true;
    if (parser.peekKind(tokens, pos, .minus)) return true;
    if (parser.peekKeyword(tokens, pos, "lower") or parser.peekKeyword(tokens, pos, "upper")) {
        return !(try expr_generated.generatedExpressionCallHasGeneratedColumn(
            alloc,
            tokens,
            pos,
            schema,
            field_expression_qualifiers,
            returning_expression_qualifiers,
            defer_row_expression_field_validation,
        ));
    }
    if (expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsInitcapFunction)) return true;
    if (expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsMd5Function)) {
        return !(try expr_generated.generatedExpressionCallHasGeneratedColumn(
            alloc,
            tokens,
            pos,
            schema,
            field_expression_qualifiers,
            returning_expression_qualifiers,
            defer_row_expression_field_validation,
        ));
    }
    if (parser.peekKeyword(tokens, pos, "concat") or parser.peekKeyword(tokens, pos, "concat_ws")) {
        return !(try expr_generated.generatedExpressionCallHasGeneratedColumn(
            alloc,
            tokens,
            pos,
            schema,
            field_expression_qualifiers,
            returning_expression_qualifiers,
            defer_row_expression_field_validation,
        ));
    }
    if (expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsJsonExtractPathFunction) or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsJsonTypeofFunction) or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsJsonArrayLengthFunction) or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsJsonBuildObjectFunction) or
        expr_token.functionCallStartsAtTokenIf(tokens, pos, expr_token.sqlTokenIsArrayLengthFunction) or
        expr_token.functionCallStartsAtTokenIf(tokens, pos, expr_token.sqlTokenIsArrayPositionFunction) or
        parser.peekKeyword(tokens, pos, "array_append") or
        parser.peekKeyword(tokens, pos, "array_prepend") or
        parser.peekKeyword(tokens, pos, "array_cat") or
        parser.peekKeyword(tokens, pos, "array_remove") or
        parser.peekKeyword(tokens, pos, "array_replace") or
        parser.peekKeyword(tokens, pos, "array_to_string") or
        parser.peekKeyword(tokens, pos, "case") or
        parser.peekKeyword(tokens, pos, "cast") or
        parser.peekKeyword(tokens, pos, "coalesce") or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsRegexpMatchFunction) or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsRegexpSubstrFunction) or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsRegexpCountFunction) or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsRegexpInstrFunction) or
        parser.peekKeyword(tokens, pos, "nullif") or
        expr_token.peekTextLengthFunctionKeyword(tokens, pos) or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsAsciiFunction) or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsChrFunction) or
        expr_token.peekSubstringFunctionKeyword(tokens, pos) or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsOverlayFunction) or
        expr_token.peekSplitPartFunctionKeyword(tokens, pos) or
        expr_token.peekStrposFunctionKeyword(tokens, pos) or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsLeftRightFunction) or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsPadFunction) or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsRepeatFunction) or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsReverseFunction) or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsStartsWithFunction) or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsEndsWithFunction) or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsDateTruncFunction) or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsDateBinFunction) or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsDatePartFunction) or
        parser.peekKeyword(tokens, pos, "position") or
        parser.peekKeyword(tokens, pos, "abs") or
        parser.peekKeyword(tokens, pos, "round") or
        expr_token.peekFunctionCallTag(tokens, pos, .trunc) or
        expr_token.peekFunctionCallTag(tokens, pos, .floor) or
        expr_token.peekFunctionCallTag(tokens, pos, .ceil) or
        expr_token.peekFunctionCallTag(tokens, pos, .sqrt) or
        expr_token.peekFunctionCallTag(tokens, pos, .sign) or
        expr_token.peekFunctionCallTag(tokens, pos, .mod) or
        expr_token.peekFunctionCallTag(tokens, pos, .power) or
        parser.peekKeyword(tokens, pos, "greatest") or
        parser.peekKeyword(tokens, pos, "least")) return true;
    if (expressionNullSafeDistinctPredicateCanStartAt(tokens, pos)) return true;
    if (textPatternSetPredicateCanStartAt(tokens, pos)) return true;
    if (parser.peekKeyword(tokens, pos, "trim")) return true;
    if (expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsTrimVariantFunction)) return true;
    if (parser.peekKeyword(tokens, pos, "replace")) return true;
    if (parser.peekKeyword(tokens, pos, "regexp_replace")) return true;
    if (expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsRegexpMatchFunction)) return true;
    if (expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsRegexpCountFunction)) return true;
    if (expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsRegexpInstrFunction)) return true;
    if (expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsTranslateFunction)) return true;
    if (parser.peekKeyword(tokens, pos, "concat_ws")) return true;
    if (expr_operator.jsonExtractNullTestPredicateCanStartAt(tokens, pos)) return true;
    if (expr_operator.jsonExtractNullSafeDistinctPredicateCanStartAt(tokens, pos)) return true;
    if (expr_operator.jsonExtractMembershipPredicateCanStartAt(tokens, pos)) return true;
    if (expr_operator.jsonKeySetExpressionCanStartAt(tokens, pos)) return true;
    if (parser.peekKind(tokens, pos, .identifier) and
        !parser.peekKeyword(tokens, pos, "null") and
        !parser.peekKeyword(tokens, pos, "true") and
        !parser.peekKeyword(tokens, pos, "false") and
        pos + 1 < tokens.len)
    {
        return switch (tokens[pos + 1].kind) {
            .plus, .minus, .star, .slash, .percent, .pipe_concat, .regex_match, .regex_imatch, .regex_not_match, .regex_not_imatch => true,
            else => false,
        };
    }
    if (parenthesizedBooleanIsPredicateCanStartAt(tokens, pos, schema)) return true;
    return false;
}

fn parenthesizedBooleanIsPredicateCanStartAt(
    tokens: []const Token,
    index: usize,
    schema: runtime_schema.TableSchema,
) bool {
    if (index >= tokens.len or tokens[index].kind != .lparen) return false;
    const inner = parser.predicateStartIndexAfterOpenParens(tokens, index);
    if (!booleanExpressionCanStartAt(tokens, inner, schema)) return false;
    const close = parser.findMatchingRParenIndex(tokens, index) orelse return false;
    if (close + 2 >= tokens.len) return false;
    if (!tokens[close + 1].matchesKeywordTag(.is)) {
        return false;
    }
    var value_index = close + 2;
    if (tokens[value_index].matchesKeywordTag(.not)) {
        value_index += 1;
    }
    if (value_index >= tokens.len or tokens[value_index].kind != .identifier) return false;
    return tokens[value_index].matchesKeywordTag(.true) or
        tokens[value_index].matchesKeywordTag(.false) or
        tokens[value_index].matchesKeywordTag(.unknown);
}

pub fn joinedMutationExpressionSideAt(
    tokens: []const Token,
    pos: usize,
    target_alias: []const u8,
    source_alias: []const u8,
    string_to_array_predicate_is_containment: bool,
) !?JoinedMutationExpressionSide {
    if (!joinedMutationExpressionCanStartAt(tokens, pos, string_to_array_predicate_is_containment)) return null;

    var side: ?db_mod.types.RelationalRowsJoinProjectionSide = null;
    var mixed = false;
    var depth: usize = 0;
    var i = pos;
    while (i < tokens.len) : (i += 1) {
        const token = tokens[i];
        switch (token.kind) {
            .lparen => depth += 1,
            .rparen => {
                if (depth == 0) break;
                depth -= 1;
            },
            .semicolon => if (depth == 0) break,
            .identifier => {
                if (depth == 0 and (token.matchesKeywordTag(.@"and") or
                    token.matchesKeywordTag(.@"or") or
                    expr_token.sqlWhereTailClauseKeywordToken(token)))
                {
                    break;
                }
                if (expr_parse.identifierContainsQualifier(token.text)) {
                    const dot = std.mem.indexOfScalar(u8, token.text, '.') orelse unreachable;
                    const next_side = try binder.joinSideForQualifier(token.text[0..dot], target_alias, source_alias);
                    if (side) |existing| {
                        if (existing != next_side) mixed = true;
                    } else {
                        side = next_side;
                    }
                }
            },
            else => {},
        }
    }
    if (mixed) return .mixed;
    if (side) |single| return .{ .single = single };
    return null;
}

pub fn joinedMutationExpressionCanStartAt(
    tokens: []const Token,
    pos: usize,
    string_to_array_predicate_is_containment: bool,
) bool {
    if (pos >= tokens.len) return false;
    if (tokens[pos].kind == .identifier and pos + 1 < tokens.len and expr_parse.identifierContainsQualifier(tokens[pos].text)) {
        return switch (tokens[pos + 1].kind) {
            .plus, .minus, .star, .slash, .percent, .arrow_json, .arrow_text, .path_arrow_json, .path_arrow_text, .pipe_concat, .question_any, .question_all => true,
            else => false,
        };
    }
    if (expressionPredicateCanStartAt(tokens, pos)) return true;
    if (canParseExpressionNotWhere(tokens, pos)) return true;
    if (parser.peekKeyword(tokens, pos, "string_to_array") and string_to_array_predicate_is_containment) return true;
    if (tokens[pos].kind == .lparen) {
        const inner = parser.predicateStartIndexAfterOpenParens(tokens, pos);
        return expressionPredicateCanStartAt(tokens, inner);
    }
    return false;
}

pub fn appendJoinOnScalarPredicateAlloc(
    alloc: std.mem.Allocator,
    source_predicates: *std.ArrayListUnmanaged(runtime_schema.RelationalCheck),
    on_expression_predicates: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionCondition),
    join_type: db_mod.types.RelationalRowsJoinType,
    side: db_mod.types.RelationalRowsJoinProjectionSide,
    field: []const u8,
    op: runtime_schema.RelationalCheckOp,
    value_json: ?[]const u8,
    collation: ?[]const u8,
) !void {
    if (join_type == .left and side == .left) {
        const lhs_field = try alloc.dupe(u8, field);
        var lhs_transferred = false;
        errdefer if (!lhs_transferred) alloc.free(lhs_field);

        const rhs = switch (op) {
            .is_null, .is_not_null => &.{},
            else => blk: {
                const json = value_json orelse return error.UnsupportedSqlShape;
                const out = try alloc.alloc(db_mod.types.RelationalRowsExpression, 1);
                var out_transferred = false;
                errdefer if (!out_transferred) alloc.free(out);
                out[0] = .{
                    .kind = .value,
                    .value_json = json,
                };
                out_transferred = true;
                break :blk out;
            },
        };
        var rhs_transferred = false;
        errdefer if (!rhs_transferred and rhs.len > 0) alloc.free(rhs);

        try on_expression_predicates.append(alloc, .{
            .lhs = .{
                .kind = .field,
                .field = lhs_field,
                .field_source = .row,
            },
            .op = op,
            .rhs = rhs,
        });
        lhs_transferred = true;
        rhs_transferred = true;
        return;
    }

    const predicate_field = try alloc.dupe(u8, field);
    var field_transferred = false;
    errdefer if (!field_transferred) alloc.free(predicate_field);
    const owned_collation = if (collation) |source| try alloc.dupe(u8, source) else null;
    var collation_transferred = false;
    errdefer if (!collation_transferred) if (owned_collation) |owned| alloc.free(owned);
    try source_predicates.append(alloc, .{
        .name = "",
        .field = predicate_field,
        .op = op,
        .value_json = value_json,
        .collation = owned_collation,
    });
    field_transferred = true;
    collation_transferred = true;
}

pub fn parseScalarWherePredicateAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    schema: runtime_schema.TableSchema,
    field_expression_qualifiers: []const []const u8,
    returning_expression_qualifiers: []const []const u8,
    defer_row_expression_field_validation: bool,
    params: []const value_mod.SqlValue,
    realtime_ns: u64,
    generated_expression_ast: ?*const generated_parser.GeneratedSqlExpressionAst,
) !runtime_schema.RelationalCheck {
    const parsed_field = try expr_generated.parseRowExpressionFieldOwnedAlloc(
        alloc,
        tokens,
        pos,
        schema,
        field_expression_qualifiers,
        returning_expression_qualifiers,
        defer_row_expression_field_validation,
    );
    defer alloc.free(parsed_field);
    const field = try binder.normalizeRowExpressionFieldAlloc(alloc, schema, parsed_field, field_expression_qualifiers, returning_expression_qualifiers, defer_row_expression_field_validation);
    var field_transferred = false;
    errdefer if (!field_transferred) alloc.free(field);
    if (expr_operator.peekJsonExtractOperator(tokens, pos.*) or parser.peekKind(tokens, pos.*, .at_contains) or parser.peekKind(tokens, pos.*, .range_overlap) or parser.peekKind(tokens, pos.*, .question)) return error.UnsupportedSqlShape;
    const column = binder.relationalColumnForField(schema, field, null) orelse return error.InvalidSqlCatalog;
    if (column.field_type == .array or column.field_type == .json) return error.UnsupportedSqlShape;

    const is_tail_token_index = pos.*;
    if (try expr_operator.parseExpressionIsTailIf(tokens, pos, .{
        .allow_boolean_unknown = true,
        .allow_boolean_literal = true,
    })) |is_tail| {
        switch (is_tail.kind) {
            .distinct_comparison => {
                try expr_generated_validate.validateGeneratedIsTailPredicateExpression(generated_expression_ast, tokens, is_tail_token_index, is_tail);
                const value_json = try value_mod.parseSqlColumnValueAlloc(alloc, tokens, pos, params, column, realtime_ns);
                var value_transferred = false;
                errdefer if (!value_transferred) alloc.free(value_json);
                field_transferred = true;
                value_transferred = true;
                return .{
                    .name = "",
                    .field = field,
                    .op = is_tail.op,
                    .value_json = value_json,
                };
            },
            .boolean_unknown => {
                try expr_generated_validate.validateGeneratedIsTailPredicateExpression(generated_expression_ast, tokens, is_tail_token_index, is_tail);
                if (column.field_type != .boolean) return error.InvalidSqlCatalog;
                field_transferred = true;
                return .{
                    .name = "",
                    .field = field,
                    .op = is_tail.op,
                    .value_json = null,
                };
            },
            .boolean_literal => {
                try expr_generated_validate.validateGeneratedIsTailPredicateExpression(generated_expression_ast, tokens, is_tail_token_index, is_tail);
                if (column.field_type != .boolean) return error.InvalidSqlCatalog;
                const value_json = try alloc.dupe(u8, value_mod.booleanJson(is_tail.boolean_value));
                var value_transferred = false;
                errdefer if (!value_transferred) alloc.free(value_json);
                field_transferred = true;
                value_transferred = true;
                return .{
                    .name = "",
                    .field = field,
                    .op = .eq,
                    .value_json = value_json,
                };
            },
            .null_test => {},
        }
        try expr_generated_validate.validateGeneratedIsTailPredicateExpression(generated_expression_ast, tokens, is_tail_token_index, is_tail);
        field_transferred = true;
        return .{
            .name = "",
            .field = field,
            .op = is_tail.op,
            .value_json = null,
        };
    }

    const postfix_null_token_index = pos.*;
    if (expr_operator.matchPostfixNullTest(tokens, pos)) |op| {
        try expr_generated_validate.validateGeneratedPostfixNullPredicateExpression(generated_expression_ast, tokens, postfix_null_token_index, op);
        field_transferred = true;
        return .{
            .name = "",
            .field = field,
            .op = op,
            .value_json = null,
        };
    }

    if (parser.peekKeywordTag(tokens, pos.*, .in) or parser.peekKeywordTag(tokens, pos.*, .not)) return error.UnsupportedSqlShape;
    if (parser.peekKeywordTag(tokens, pos.*, .between)) return error.UnsupportedSqlShape;
    const op_token_index = pos.*;
    const op = try expr_operator.parseComparisonOp(tokens, pos);
    try expr_generated_validate.validateGeneratedRelationalPredicateExpression(generated_expression_ast, tokens, op_token_index, op);
    if (parser.peekKeywordTag(tokens, pos.*, .any) or parser.peekKeywordTag(tokens, pos.*, .some)) return error.UnsupportedSqlShape;
    const value_json = try value_mod.parseSqlColumnValueAlloc(alloc, tokens, pos, params, column, realtime_ns);
    var value_transferred = false;
    errdefer if (!value_transferred) alloc.free(value_json);
    field_transferred = true;
    value_transferred = true;
    return .{
        .name = "",
        .field = field,
        .op = op,
        .value_json = value_json,
    };
}

pub fn parseScalarNotWhereAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    params: []const value_mod.SqlValue,
    schema: runtime_schema.TableSchema,
    field_expression_qualifiers: []const []const u8,
    returning_expression_qualifiers: []const []const u8,
    defer_row_expression_field_validation: bool,
    not_predicates: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsPredicateGroup),
    realtime_ns: u64,
    generated_expression_ast: ?*const generated_parser.GeneratedSqlExpressionAst,
) !void {
    if (generated_expression_ast) |expression| {
        if (expression.kind != .logical_not) return error.UnsupportedSqlShape;
    }
    try parser.expectKeyword(tokens, pos, "not");
    try parser.expectToken(tokens, pos, .lparen);
    while (true) {
        var branch = std.ArrayListUnmanaged(runtime_schema.RelationalCheck).empty;
        errdefer {
            freeRelationalChecks(alloc, branch.items);
            branch.deinit(alloc);
        }
        while (true) {
            const generated_condition_expression = try expr_generated_validate.generatedPredicateExpressionAtStart(tokens, pos.*, generated_expression_ast);
            const predicate = try parseScalarWherePredicateAlloc(
                alloc,
                tokens,
                pos,
                schema,
                field_expression_qualifiers,
                returning_expression_qualifiers,
                defer_row_expression_field_validation,
                params,
                realtime_ns,
                generated_condition_expression,
            );
            var predicate_transferred = false;
            errdefer if (!predicate_transferred) plan_mod.freeRelationalCheck(alloc, predicate);
            try branch.append(alloc, predicate);
            predicate_transferred = true;
            if (!parser.matchKeyword(tokens, pos, "and")) break;
        }
        if (branch.items.len == 0) return error.UnsupportedSqlShape;

        const predicates = try branch.toOwnedSlice(alloc);
        branch = .empty;
        var predicates_transferred = false;
        errdefer if (!predicates_transferred) {
            freeRelationalChecks(alloc, predicates);
            if (predicates.len > 0) alloc.free(predicates);
        };
        try not_predicates.append(alloc, .{ .predicates = predicates });
        predicates_transferred = true;
        branch.deinit(alloc);

        if (!parser.matchKeyword(tokens, pos, "or")) break;
    }
    try parser.expectToken(tokens, pos, .rparen);
}

pub fn parseScalarOrWhereAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    params: []const value_mod.SqlValue,
    schema: runtime_schema.TableSchema,
    field_expression_qualifiers: []const []const u8,
    returning_expression_qualifiers: []const []const u8,
    defer_row_expression_field_validation: bool,
    or_predicates: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsPredicateGroup),
    realtime_ns: u64,
    generated_expression_ast: ?*const generated_parser.GeneratedSqlExpressionAst,
) !void {
    while (true) {
        const parenthesized = parser.matchToken(tokens, pos, .lparen) != null;
        var branches = std.ArrayListUnmanaged(ScalarOrCheckBranch).empty;
        errdefer freeScalarOrCheckBranches(alloc, &branches);
        try branches.append(alloc, .empty);

        while (true) {
            const generated_condition_expression = try expr_generated_validate.generatedPredicateExpressionAtStart(tokens, pos.*, generated_expression_ast);
            if (try parseScalarBooleanIsNotIntoOrBranchesAlloc(
                alloc,
                tokens,
                pos,
                schema,
                field_expression_qualifiers,
                returning_expression_qualifiers,
                defer_row_expression_field_validation,
                &branches,
                generated_condition_expression,
            )) {
                // Expanded as native OR branches.
            } else if (!(try parseScalarWhereSetIntoOrBranchesAlloc(
                alloc,
                tokens,
                pos,
                params,
                schema,
                field_expression_qualifiers,
                returning_expression_qualifiers,
                defer_row_expression_field_validation,
                &branches,
                generated_condition_expression,
            ))) {
                const predicate = try parseScalarWherePredicateAlloc(
                    alloc,
                    tokens,
                    pos,
                    schema,
                    field_expression_qualifiers,
                    returning_expression_qualifiers,
                    defer_row_expression_field_validation,
                    params,
                    realtime_ns,
                    generated_condition_expression,
                );
                defer plan_mod.freeRelationalCheck(alloc, predicate);
                try appendRelationalCheckCloneToScalarOrBranches(alloc, &branches, predicate);
            }
            if (!parser.matchKeyword(tokens, pos, "and")) break;
        }

        if (branches.items.len == 0) return error.UnsupportedSqlShape;
        if (parenthesized) try parser.expectToken(tokens, pos, .rparen);
        for (branches.items) |*branch| {
            if (branch.items.len == 0) return error.UnsupportedSqlShape;
            const predicates = try branch.toOwnedSlice(alloc);
            branch.* = .empty;
            var predicates_transferred = false;
            errdefer if (!predicates_transferred) {
                freeRelationalChecks(alloc, predicates);
                if (predicates.len > 0) alloc.free(predicates);
            };
            try or_predicates.append(alloc, .{ .predicates = predicates });
            predicates_transferred = true;
        }
        branches.deinit(alloc);

        if (!parser.matchKeyword(tokens, pos, "or")) break;
    }
}

fn parseScalarBooleanIsNotIntoOrBranchesAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    schema: runtime_schema.TableSchema,
    field_expression_qualifiers: []const []const u8,
    returning_expression_qualifiers: []const []const u8,
    defer_row_expression_field_validation: bool,
    branches: *std.ArrayListUnmanaged(ScalarOrCheckBranch),
    generated_expression_ast: ?*const generated_parser.GeneratedSqlExpressionAst,
) !bool {
    const saved_pos = pos.*;
    const parsed_field = expr_generated.parseRowExpressionFieldOwnedAlloc(
        alloc,
        tokens,
        pos,
        schema,
        field_expression_qualifiers,
        returning_expression_qualifiers,
        defer_row_expression_field_validation,
    ) catch |err| {
        pos.* = saved_pos;
        return switch (err) {
            error.UnsupportedSqlShape => false,
            else => err,
        };
    };
    defer alloc.free(parsed_field);
    const operator_token_index = pos.*;
    if (!parser.matchKeywordTag(tokens, pos, .is)) {
        pos.* = saved_pos;
        return false;
    }
    if (!parser.matchKeywordTag(tokens, pos, .not)) {
        pos.* = saved_pos;
        return false;
    }
    if (!(parser.peekKeywordTag(tokens, pos.*, .true) or parser.peekKeywordTag(tokens, pos.*, .false))) {
        pos.* = saved_pos;
        return false;
    }

    const field = try binder.normalizeRowExpressionFieldAlloc(
        alloc,
        schema,
        parsed_field,
        field_expression_qualifiers,
        returning_expression_qualifiers,
        defer_row_expression_field_validation,
    );
    defer alloc.free(field);
    const column = binder.relationalColumnForField(schema, field, .boolean) orelse return error.InvalidSqlCatalog;
    const value = (try value_mod.parseSqlBooleanIsValue(tokens, pos, column)) orelse return error.UnsupportedSqlShape;
    const expected_kind: generated_parser.GeneratedSqlExpressionKind = if (value) .is_not_true else .is_not_false;
    try expr_generated_validate.validateGeneratedBooleanIsPredicateExpression(generated_expression_ast, tokens, operator_token_index, expected_kind);
    try expandBooleanIsNotIntoOrBranches(alloc, branches, field, value);
    return true;
}

fn parseScalarWhereSetIntoOrBranchesAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    params: []const value_mod.SqlValue,
    schema: runtime_schema.TableSchema,
    field_expression_qualifiers: []const []const u8,
    returning_expression_qualifiers: []const []const u8,
    defer_row_expression_field_validation: bool,
    branches: *std.ArrayListUnmanaged(ScalarOrCheckBranch),
    generated_expression_ast: ?*const generated_parser.GeneratedSqlExpressionAst,
) !bool {
    if (!peekSimpleScalarSetPredicate(tokens, pos.*)) return false;

    const field = try expr_generated.parseRowExpressionFieldOwnedAlloc(
        alloc,
        tokens,
        pos,
        schema,
        field_expression_qualifiers,
        returning_expression_qualifiers,
        defer_row_expression_field_validation,
    );
    defer alloc.free(field);
    const column = binder.relationalColumnForField(schema, field, null) orelse return error.InvalidSqlCatalog;
    if (column.field_type == .array or column.field_type == .json) return error.UnsupportedSqlShape;

    if (parser.matchKeyword(tokens, pos, "not")) {
        const negation_token_index = pos.* - 1;
        try parser.expectKeyword(tokens, pos, "in");
        try expr_generated_validate.validateGeneratedSetOrBetweenPredicateIdentity(generated_expression_ast, .not_in_list, tokens, pos.* - 1, negation_token_index, null);
        const values_json = try value_mod.parseSqlInValuesJsonAlloc(alloc, tokens, pos, params);
        defer alloc.free(values_json);
        try appendScalarValuesJsonToOrBranches(alloc, branches, field, values_json, .ne, column.collation);
        return true;
    }
    if (parser.matchKeyword(tokens, pos, "in")) {
        try expr_generated_validate.validateGeneratedSetOrBetweenPredicateIdentity(generated_expression_ast, .in_list, tokens, pos.* - 1, null, null);
        const values_json = try value_mod.parseSqlInValuesJsonAlloc(alloc, tokens, pos, params);
        defer alloc.free(values_json);
        try expandScalarValuesJsonIntoOrBranches(alloc, branches, field, values_json, .eq, column.collation);
        return true;
    }

    const op_token_index = pos.*;
    const op = try expr_operator.parseComparisonOp(tokens, pos);
    if (op == .eq and expr_token.matchAnyOrSomeKeyword(tokens, pos)) {
        try expr_generated_validate.validateGeneratedQuantifiedPredicateIdentity(generated_expression_ast, tokens, op_token_index, pos.* - 1);
        try parser.expectToken(tokens, pos, .lparen);
        const values_json = try value_mod.parseJsonArrayValueAlloc(alloc, tokens, pos, params);
        defer alloc.free(values_json);
        try parser.expectToken(tokens, pos, .rparen);
        try expandScalarValuesJsonIntoOrBranches(alloc, branches, field, values_json, .eq, column.collation);
        return true;
    }
    if (op == .ne and expr_token.matchAnyOrSomeKeyword(tokens, pos)) {
        try expr_generated_validate.validateGeneratedQuantifiedPredicateIdentity(generated_expression_ast, tokens, op_token_index, pos.* - 1);
        try parser.expectToken(tokens, pos, .lparen);
        const values_json = try value_mod.parseJsonArrayValueAlloc(alloc, tokens, pos, params);
        defer alloc.free(values_json);
        try parser.expectToken(tokens, pos, .rparen);
        try expandScalarValuesJsonIntoOrBranches(alloc, branches, field, values_json, .ne, column.collation);
        return true;
    }
    if (op == .eq and parser.matchKeyword(tokens, pos, "all")) {
        try expr_generated_validate.validateGeneratedQuantifiedPredicateIdentity(generated_expression_ast, tokens, op_token_index, pos.* - 1);
        try parser.expectToken(tokens, pos, .lparen);
        const values_json = try value_mod.parseJsonArrayValueAlloc(alloc, tokens, pos, params);
        defer alloc.free(values_json);
        try parser.expectToken(tokens, pos, .rparen);
        try appendScalarValuesJsonToOrBranches(alloc, branches, field, values_json, .eq, column.collation);
        return true;
    }
    if (op == .ne and parser.matchKeyword(tokens, pos, "all")) {
        try expr_generated_validate.validateGeneratedQuantifiedPredicateIdentity(generated_expression_ast, tokens, op_token_index, pos.* - 1);
        try parser.expectToken(tokens, pos, .lparen);
        const values_json = try value_mod.parseJsonArrayValueAlloc(alloc, tokens, pos, params);
        defer alloc.free(values_json);
        try parser.expectToken(tokens, pos, .rparen);
        try appendScalarValuesJsonToOrBranches(alloc, branches, field, values_json, .ne, column.collation);
        return true;
    }
    return error.UnsupportedSqlShape;
}

pub fn parseWhereAtomAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    params: []const value_mod.SqlValue,
    schema: runtime_schema.TableSchema,
    field_expression_qualifiers: []const []const u8,
    returning_expression_qualifiers: []const []const u8,
    defer_row_expression_field_validation: bool,
    predicates: *std.ArrayListUnmanaged(runtime_schema.RelationalCheck),
    json_contains: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsJsonContainsPredicate),
    json_path_eq: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsJsonPathEqPredicate),
    json_path_exists: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsJsonPathExistsPredicate),
    array_any: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsArrayAnyPredicate),
    array_contains: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsArrayContainsPredicate),
    array_eq: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsArrayEqPredicate),
    in_predicates: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsInPredicate),
    text_patterns: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsTextPatternPredicate),
    or_predicates: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsPredicateGroup),
    negated: bool,
    realtime_ns: u64,
    generated_expression_ast: ?*const generated_parser.GeneratedSqlExpressionAst,
) !void {
    try expr_generated_validate.rejectGeneratedUnsupportedSubqueryPredicateExpression(tokens, generated_expression_ast);

    if (!negated and parser.matchKeyword(tokens, pos, "not")) {
        try parser.expectToken(tokens, pos, .lparen);
        try parseWhereAtomAlloc(alloc, tokens, pos, params, schema, field_expression_qualifiers, returning_expression_qualifiers, defer_row_expression_field_validation, predicates, json_contains, json_path_eq, json_path_exists, array_any, array_contains, array_eq, in_predicates, text_patterns, or_predicates, true, realtime_ns, null);
        try parser.expectToken(tokens, pos, .rparen);
        return;
    }
    if (!negated and valueEqualsAnyArrayPredicateCanStart(tokens, pos.*)) {
        try expr_generated_validate.validateGeneratedExpressionPredicateKind(generated_expression_ast, .quantified_comparison);
    }
    if (!negated and try parseValueEqualsAnyArrayPredicateAlloc(
        alloc,
        tokens,
        pos,
        params,
        schema,
        field_expression_qualifiers,
        returning_expression_qualifiers,
        defer_row_expression_field_validation,
        array_any,
        generated_expression_ast,
    )) {
        return;
    }
    const parsed_field = try expr_generated.parseRowExpressionFieldOwnedAlloc(alloc, tokens, pos, schema, field_expression_qualifiers, returning_expression_qualifiers, defer_row_expression_field_validation);
    defer alloc.free(parsed_field);
    if (try binder.normalizePeriodReferenceAlloc(alloc, schema, parsed_field, field_expression_qualifiers, returning_expression_qualifiers)) |period_field| {
        defer alloc.free(period_field);
        if (binder.relationalPeriodForDdl(schema.periods, period_field)) |period| {
            if (parser.matchToken(tokens, pos, .at_contains) != null) {
                try expr_generated_validate.validateGeneratedSingleOperatorPredicateIdentity(generated_expression_ast, .contains, tokens, pos.* - 1);
                if (negated) return error.UnsupportedSqlShape;
                const start_column = binder.relationalColumnForField(schema, period.start_column, null) orelse return error.InvalidSqlCatalog;
                const end_column = binder.relationalColumnForField(schema, period.end_column, null) orelse return error.InvalidSqlCatalog;
                if (start_column.field_type != end_column.field_type or !binder.relationalPeriodColumnType(start_column.field_type)) return error.InvalidSqlCatalog;
                const point_json = try parseSqlRangeConstructorEndpointJsonAlloc(alloc, tokens, pos, params, start_column.field_type, realtime_ns);
                defer alloc.free(point_json);
                if (std.mem.eql(u8, point_json, "null")) return error.UnsupportedSqlShape;
                try appendTemporalRangeContainsPredicateGroups(alloc, or_predicates, period, point_json);
                return;
            }
            if (parser.matchToken(tokens, pos, .range_overlap) != null) {
                try expr_generated_validate.validateGeneratedSingleOperatorPredicateIdentity(generated_expression_ast, .overlaps, tokens, pos.* - 1);
                if (negated) return error.UnsupportedSqlShape;
                const range = try parseSqlPeriodRangeValuePairAlloc(alloc, tokens, pos, params, schema, period, realtime_ns);
                defer freePeriodRangeValuePair(alloc, range);
                try appendTemporalRangeOverlapsPredicateGroups(alloc, or_predicates, period, range);
                return;
            }
        }
    }

    const field = try binder.normalizeRowExpressionFieldAlloc(alloc, schema, parsed_field, field_expression_qualifiers, returning_expression_qualifiers, defer_row_expression_field_validation);
    var field_transferred = false;
    defer if (!field_transferred) alloc.free(field);
    const maybe_column = binder.relationalColumnForField(schema, field, null);

    if (expr_operator.matchJsonExtractOperator(tokens, pos)) |operator| {
        const as_text = expr_operator.tokenKindIsJsonExtractTextOperator(operator);
        const path = try value_mod.parseJsonExtractOperatorPathOwnedAlloc(alloc, tokens, pos, params, operator);
        var path_transferred = false;
        errdefer if (!path_transferred) alloc.free(path);
        const op_token_index = pos.*;
        try parser.expectToken(tokens, pos, .eq);
        try expr_generated_validate.validateGeneratedComparisonPredicateExpression(generated_expression_ast, tokens, op_token_index, .eq);
        const value_json = if (as_text)
            try value_mod.parseJsonValueAlloc(alloc, tokens, pos, params)
        else
            try value_mod.parseRequiredJsonDocumentValueAlloc(alloc, tokens, pos, params);
        var value_transferred = false;
        errdefer if (!value_transferred) alloc.free(value_json);
        if (binder.relationalColumnForField(schema, field, .json) == null) return error.InvalidSqlCatalog;
        const predicate_field = try alloc.dupe(u8, field);
        var predicate_field_transferred = false;
        errdefer if (!predicate_field_transferred) alloc.free(predicate_field);
        try json_path_eq.append(alloc, .{
            .field = predicate_field,
            .path = path,
            .value_json = value_json,
        });
        predicate_field_transferred = true;
        path_transferred = true;
        value_transferred = true;
        return;
    }
    if (parser.matchToken(tokens, pos, .at_contains) != null) {
        try expr_generated_validate.validateGeneratedSingleOperatorPredicateIdentity(generated_expression_ast, .contains, tokens, pos.* - 1);
        const column = maybe_column orelse return error.InvalidSqlCatalog;
        const value_json = try value_mod.parseStructuredPredicateValueAlloc(alloc, tokens, pos, params, column);
        var value_transferred = false;
        errdefer if (!value_transferred) alloc.free(value_json);
        if (column.field_type == .array) {
            try value_mod.validateSqlArrayValueJson(alloc, column, value_json);
            try array_contains.append(alloc, .{
                .field = field,
                .value_json = value_json,
            });
            field_transferred = true;
            value_transferred = true;
            return;
        }
        if (column.field_type != .json) return error.InvalidSqlCatalog;
        try json_contains.append(alloc, .{
            .field = field,
            .value_json = value_json,
        });
        field_transferred = true;
        value_transferred = true;
        return;
    }
    if (parser.matchToken(tokens, pos, .question) != null) {
        try expr_generated_validate.validateGeneratedSingleOperatorPredicateIdentity(generated_expression_ast, .json_key_exists, tokens, pos.* - 1);
        if (binder.relationalColumnForField(schema, field, .json) == null) return error.InvalidSqlCatalog;
        const path = try value_mod.parseJsonPathOwnedAlloc(alloc, tokens, pos, params);
        var path_transferred = false;
        errdefer if (!path_transferred) alloc.free(path);
        try json_path_exists.append(alloc, .{
            .field = field,
            .path = path,
        });
        field_transferred = true;
        path_transferred = true;
        return;
    }
    const column = maybe_column orelse return error.InvalidSqlCatalog;
    const is_tail_token_index = pos.*;
    if (try expr_operator.parseExpressionIsTailIf(tokens, pos, .{
        .allow_boolean_unknown = true,
        .allow_boolean_literal = true,
        .allow_boolean_literal_negation = true,
    })) |is_tail| {
        switch (is_tail.kind) {
            .distinct_comparison => {
                try expr_generated_validate.validateGeneratedIsTailPredicateExpression(generated_expression_ast, tokens, is_tail_token_index, is_tail);
                const value_json = try value_mod.parseSqlColumnValueAlloc(alloc, tokens, pos, params, column, realtime_ns);
                var value_transferred = false;
                errdefer if (!value_transferred) alloc.free(value_json);
                const collation = if (column.collation) |value| try alloc.dupe(u8, value) else null;
                var collation_transferred = false;
                errdefer if (!collation_transferred) if (collation) |value| alloc.free(value);
                try predicates.append(alloc, .{
                    .name = "",
                    .field = field,
                    .op = is_tail.op,
                    .value_json = value_json,
                    .collation = collation,
                });
                value_transferred = true;
                field_transferred = true;
                collation_transferred = true;
                return;
            },
            .boolean_unknown => {
                try expr_generated_validate.validateGeneratedIsTailPredicateExpression(generated_expression_ast, tokens, is_tail_token_index, is_tail);
                if (column.field_type != .boolean) return error.InvalidSqlCatalog;
                try predicates.append(alloc, .{
                    .name = "",
                    .field = field,
                    .op = is_tail.op,
                    .value_json = null,
                });
                field_transferred = true;
                return;
            },
            .boolean_literal => {
                try expr_generated_validate.validateGeneratedIsTailPredicateExpression(generated_expression_ast, tokens, is_tail_token_index, is_tail);
                if (column.field_type != .boolean) return error.InvalidSqlCatalog;
                if (is_tail.boolean_negated) {
                    try appendBooleanIsNotPredicateGroups(alloc, or_predicates, field, is_tail.boolean_value);
                    return;
                }
                const value_json = try alloc.dupe(u8, value_mod.booleanJson(is_tail.boolean_value));
                var value_transferred = false;
                errdefer if (!value_transferred) alloc.free(value_json);
                try predicates.append(alloc, .{
                    .name = "",
                    .field = field,
                    .op = .eq,
                    .value_json = value_json,
                });
                value_transferred = true;
                field_transferred = true;
                return;
            },
            .null_test => {},
        }
        try expr_generated_validate.validateGeneratedIsTailPredicateExpression(generated_expression_ast, tokens, is_tail_token_index, is_tail);
        try predicates.append(alloc, .{
            .name = "",
            .field = field,
            .op = is_tail.op,
            .value_json = null,
        });
        field_transferred = true;
        return;
    }
    const postfix_null_token_index = pos.*;
    if (expr_operator.matchPostfixNullTest(tokens, pos)) |op| {
        try expr_generated_validate.validateGeneratedPostfixNullPredicateExpression(generated_expression_ast, tokens, postfix_null_token_index, op);
        try predicates.append(alloc, .{
            .name = "",
            .field = field,
            .op = op,
            .value_json = null,
        });
        field_transferred = true;
        return;
    }
    if (parser.matchKeyword(tokens, pos, "like") or parser.matchKeyword(tokens, pos, "ilike")) {
        const case_insensitive = tokens[pos.* - 1].matchesKeywordTag(.ilike);
        const generated_kind: generated_parser.GeneratedSqlExpressionKind = if (case_insensitive) .ilike else .like;
        const generated_quantifier_token_index: ?usize = if (expr_token.tokenAtIsAnySomeOrAll(tokens, pos.*)) pos.* else null;
        try expr_generated_validate.validateGeneratedPatternPredicateIdentity(generated_expression_ast, generated_kind, tokens, pos.* - 1, generated_quantifier_token_index);
        try parseAndAppendTextPatternPredicateAlloc(alloc, tokens, pos, params, text_patterns, field, column, case_insensitive, negated, realtime_ns);
        return;
    }
    if (parser.matchKeyword(tokens, pos, "between")) {
        const operator_token_index = pos.* - 1;
        const modifier_token_index = expr_generated_validate.betweenModifierTokenIndex(tokens, pos.*);
        try expr_generated_validate.validateGeneratedSetOrBetweenPredicateIdentity(generated_expression_ast, .between, tokens, operator_token_index, null, modifier_token_index);
        const symmetric = expr_token.matchBetweenSymmetricMode(tokens, pos);
        const lower_json = try value_mod.parseSqlColumnValueAlloc(alloc, tokens, pos, params, column, realtime_ns);
        defer alloc.free(lower_json);
        try parser.expectKeyword(tokens, pos, "and");
        const upper_json = try value_mod.parseSqlColumnValueAlloc(alloc, tokens, pos, params, column, realtime_ns);
        defer alloc.free(upper_json);
        try appendBetweenPredicateValuesAlloc(alloc, predicates, or_predicates, field, column, lower_json, upper_json, negated, symmetric);
        return;
    }
    if (parser.matchKeyword(tokens, pos, "not")) {
        const negation_token_index = pos.* - 1;
        if (parser.matchKeyword(tokens, pos, "between")) {
            const operator_token_index = pos.* - 1;
            const modifier_token_index = expr_generated_validate.betweenModifierTokenIndex(tokens, pos.*);
            try expr_generated_validate.validateGeneratedSetOrBetweenPredicateIdentity(generated_expression_ast, .not_between, tokens, operator_token_index, negation_token_index, modifier_token_index);
            const symmetric = expr_token.matchBetweenSymmetricMode(tokens, pos);
            const lower_json = try value_mod.parseSqlColumnValueAlloc(alloc, tokens, pos, params, column, realtime_ns);
            defer alloc.free(lower_json);
            try parser.expectKeyword(tokens, pos, "and");
            const upper_json = try value_mod.parseSqlColumnValueAlloc(alloc, tokens, pos, params, column, realtime_ns);
            defer alloc.free(upper_json);
            try appendBetweenPredicateValuesAlloc(alloc, predicates, or_predicates, field, column, lower_json, upper_json, true, symmetric);
            return;
        }
        if (parser.matchKeyword(tokens, pos, "like") or parser.matchKeyword(tokens, pos, "ilike")) {
            const case_insensitive = tokens[pos.* - 1].matchesKeywordTag(.ilike);
            const generated_kind: generated_parser.GeneratedSqlExpressionKind = if (case_insensitive) .not_ilike else .not_like;
            const generated_quantifier_token_index: ?usize = if (expr_token.tokenAtIsAnySomeOrAll(tokens, pos.*)) pos.* else null;
            try expr_generated_validate.validateGeneratedPatternPredicateIdentity(generated_expression_ast, generated_kind, tokens, pos.* - 1, generated_quantifier_token_index);
            try parseAndAppendTextPatternPredicateAlloc(alloc, tokens, pos, params, text_patterns, field, column, case_insensitive, true, realtime_ns);
            return;
        }
        try parser.expectKeyword(tokens, pos, "in");
        try expr_generated_validate.validateGeneratedSetOrBetweenPredicateIdentity(generated_expression_ast, .not_in_list, tokens, pos.* - 1, negation_token_index, null);
        if (column.field_type == .array or column.field_type == .json) return error.InvalidSqlCatalog;
        const values_json = try value_mod.parseSqlInValuesJsonAlloc(alloc, tokens, pos, params);
        var values_transferred = false;
        errdefer if (!values_transferred) alloc.free(values_json);
        try value_mod.validateSqlScalarValuesJson(alloc, column, values_json);
        const collation = if (column.collation) |value| try alloc.dupe(u8, value) else null;
        var collation_transferred = false;
        errdefer if (!collation_transferred) if (collation) |value| alloc.free(value);
        try in_predicates.append(alloc, .{
            .field = field,
            .values_json = values_json,
            .negated = true,
            .collation = collation,
        });
        field_transferred = true;
        values_transferred = true;
        collation_transferred = true;
        return;
    }
    if (parser.matchKeyword(tokens, pos, "in")) {
        try expr_generated_validate.validateGeneratedSetOrBetweenPredicateIdentity(generated_expression_ast, .in_list, tokens, pos.* - 1, null, null);
        if (column.field_type == .array or column.field_type == .json) return error.InvalidSqlCatalog;
        const values_json = try value_mod.parseSqlInValuesJsonAlloc(alloc, tokens, pos, params);
        var values_transferred = false;
        errdefer if (!values_transferred) alloc.free(values_json);
        try value_mod.validateSqlScalarValuesJson(alloc, column, values_json);
        const collation = if (column.collation) |value| try alloc.dupe(u8, value) else null;
        var collation_transferred = false;
        errdefer if (!collation_transferred) if (collation) |value| alloc.free(value);
        try in_predicates.append(alloc, .{
            .field = field,
            .values_json = values_json,
            .negated = false,
            .collation = collation,
        });
        field_transferred = true;
        values_transferred = true;
        collation_transferred = true;
        return;
    }

    const op_token_index = pos.*;
    const op = try expr_operator.parseComparisonOp(tokens, pos);
    if (op == .eq and expr_token.matchAnyOrSomeKeyword(tokens, pos)) {
        try expr_generated_validate.validateGeneratedQuantifiedPredicateIdentity(generated_expression_ast, tokens, op_token_index, pos.* - 1);
        if (column.field_type == .array or column.field_type == .json) return error.InvalidSqlCatalog;
        try parser.expectToken(tokens, pos, .lparen);
        const values_json = try value_mod.parseJsonArrayValueAlloc(alloc, tokens, pos, params);
        var values_transferred = false;
        errdefer if (!values_transferred) alloc.free(values_json);
        try parser.expectToken(tokens, pos, .rparen);
        try value_mod.validateSqlScalarValuesJson(alloc, column, values_json);
        const collation = if (column.collation) |value| try alloc.dupe(u8, value) else null;
        var collation_transferred = false;
        errdefer if (!collation_transferred) if (collation) |value| alloc.free(value);
        try in_predicates.append(alloc, .{
            .field = field,
            .values_json = values_json,
            .negated = negated,
            .collation = collation,
        });
        field_transferred = true;
        values_transferred = true;
        collation_transferred = true;
        return;
    }
    if (op == .ne and expr_token.matchAnyOrSomeKeyword(tokens, pos)) {
        try expr_generated_validate.validateGeneratedQuantifiedPredicateIdentity(generated_expression_ast, tokens, op_token_index, pos.* - 1);
        if (column.field_type == .array or column.field_type == .json) return error.InvalidSqlCatalog;
        try parser.expectToken(tokens, pos, .lparen);
        const values_json = try value_mod.parseJsonArrayValueAlloc(alloc, tokens, pos, params);
        defer alloc.free(values_json);
        try parser.expectToken(tokens, pos, .rparen);
        try value_mod.validateSqlScalarValuesJson(alloc, column, values_json);
        if (negated) {
            try appendScalarAllEqualityPredicates(alloc, predicates, field, values_json, column.collation);
        } else {
            try appendScalarValuesJsonOrGroups(alloc, or_predicates, field, values_json, .ne, column.collation);
        }
        return;
    }
    if (op == .eq and parser.matchKeyword(tokens, pos, "all")) {
        try expr_generated_validate.validateGeneratedQuantifiedPredicateIdentity(generated_expression_ast, tokens, op_token_index, pos.* - 1);
        if (column.field_type == .array or column.field_type == .json) return error.InvalidSqlCatalog;
        try parser.expectToken(tokens, pos, .lparen);
        const values_json = try value_mod.parseJsonArrayValueAlloc(alloc, tokens, pos, params);
        defer alloc.free(values_json);
        try parser.expectToken(tokens, pos, .rparen);
        try value_mod.validateSqlScalarValuesJson(alloc, column, values_json);
        try appendScalarAllEqualityPredicates(alloc, predicates, field, values_json, column.collation);
        return;
    }
    if (op == .ne and parser.matchKeyword(tokens, pos, "all")) {
        try expr_generated_validate.validateGeneratedQuantifiedPredicateIdentity(generated_expression_ast, tokens, op_token_index, pos.* - 1);
        if (column.field_type == .array or column.field_type == .json) return error.InvalidSqlCatalog;
        try parser.expectToken(tokens, pos, .lparen);
        const values_json = try value_mod.parseJsonArrayValueAlloc(alloc, tokens, pos, params);
        var values_transferred = false;
        errdefer if (!values_transferred) alloc.free(values_json);
        try parser.expectToken(tokens, pos, .rparen);
        try value_mod.validateSqlScalarValuesJson(alloc, column, values_json);
        const collation = if (column.collation) |value| try alloc.dupe(u8, value) else null;
        var collation_transferred = false;
        errdefer if (!collation_transferred) if (collation) |value| alloc.free(value);
        try in_predicates.append(alloc, .{
            .field = field,
            .values_json = values_json,
            .negated = !negated,
            .collation = collation,
        });
        field_transferred = true;
        values_transferred = true;
        collation_transferred = true;
        return;
    }
    if (negated) return error.UnsupportedSqlShape;
    try expr_generated_validate.validateGeneratedRelationalPredicateExpression(generated_expression_ast, tokens, op_token_index, op);
    const value_json = if (column.field_type == .array and op == .eq)
        try value_mod.parseArrayPredicateValueAlloc(alloc, tokens, pos, params)
    else
        try value_mod.parseSqlColumnValueAlloc(alloc, tokens, pos, params, column, realtime_ns);
    var value_transferred = false;
    errdefer if (!value_transferred) alloc.free(value_json);
    if (column.field_type == .array) {
        if (op != .eq) return error.UnsupportedSqlShape;
        try value_mod.validateSqlArrayValueJson(alloc, column, value_json);
        try array_eq.append(alloc, .{
            .field = field,
            .value_json = value_json,
        });
        field_transferred = true;
        value_transferred = true;
        return;
    }
    const collation = if (column.collation) |value| try alloc.dupe(u8, value) else null;
    var collation_transferred = false;
    errdefer if (!collation_transferred) if (collation) |value| alloc.free(value);
    try predicates.append(alloc, .{
        .name = "",
        .field = field,
        .op = op,
        .value_json = value_json,
        .collation = collation,
    });
    field_transferred = true;
    value_transferred = true;
    collation_transferred = true;
}


fn parseAccessPredicateAtomGroupAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    params: []const value_mod.SqlValue,
    schema: runtime_schema.TableSchema,
    field_expression_qualifiers: []const []const u8,
    returning_expression_qualifiers: []const []const u8,
    defer_row_expression_field_validation: bool,
    realtime_ns: u64,
    generated_expression_ast: ?*const generated_parser.GeneratedSqlExpressionAst,
) !db_mod.types.RelationalRowsAccessPredicateGroup {
    var predicates = std.ArrayListUnmanaged(runtime_schema.RelationalCheck).empty;
    errdefer {
        freeRelationalChecks(alloc, predicates.items);
        predicates.deinit(alloc);
    }
    var json_path_eq = std.ArrayListUnmanaged(db_mod.types.RelationalRowsJsonPathEqPredicate).empty;
    errdefer {
        freeJsonPathEq(alloc, json_path_eq.items);
        json_path_eq.deinit(alloc);
    }
    var json_contains = std.ArrayListUnmanaged(db_mod.types.RelationalRowsJsonContainsPredicate).empty;
    errdefer {
        freeJsonContains(alloc, json_contains.items);
        json_contains.deinit(alloc);
    }
    var json_path_exists = std.ArrayListUnmanaged(db_mod.types.RelationalRowsJsonPathExistsPredicate).empty;
    errdefer {
        freeJsonPathExists(alloc, json_path_exists.items);
        json_path_exists.deinit(alloc);
    }
    var array_any = std.ArrayListUnmanaged(db_mod.types.RelationalRowsArrayAnyPredicate).empty;
    errdefer {
        freeArrayAny(alloc, array_any.items);
        array_any.deinit(alloc);
    }
    var array_contains = std.ArrayListUnmanaged(db_mod.types.RelationalRowsArrayContainsPredicate).empty;
    errdefer {
        freeArrayContains(alloc, array_contains.items);
        array_contains.deinit(alloc);
    }
    var array_eq = std.ArrayListUnmanaged(db_mod.types.RelationalRowsArrayEqPredicate).empty;
    errdefer {
        freeArrayEq(alloc, array_eq.items);
        array_eq.deinit(alloc);
    }
    var in_predicates = std.ArrayListUnmanaged(db_mod.types.RelationalRowsInPredicate).empty;
    errdefer {
        freeInPredicates(alloc, in_predicates.items);
        in_predicates.deinit(alloc);
    }
    var text_patterns = std.ArrayListUnmanaged(db_mod.types.RelationalRowsTextPatternPredicate).empty;
    errdefer {
        freeTextPatterns(alloc, text_patterns.items);
        text_patterns.deinit(alloc);
    }
    var nested_or_predicates = std.ArrayListUnmanaged(db_mod.types.RelationalRowsPredicateGroup).empty;
    defer {
        freePredicateGroups(alloc, nested_or_predicates.items);
        nested_or_predicates.deinit(alloc);
    }

    try parseWhereAtomAlloc(
        alloc,
        tokens,
        pos,
        params,
        schema,
        field_expression_qualifiers,
        returning_expression_qualifiers,
        defer_row_expression_field_validation,
        &predicates,
        &json_contains,
        &json_path_eq,
        &json_path_exists,
        &array_any,
        &array_contains,
        &array_eq,
        &in_predicates,
        &text_patterns,
        &nested_or_predicates,
        false,
        realtime_ns,
        generated_expression_ast,
    );
    if (nested_or_predicates.items.len > 0) return error.UnsupportedSqlShape;

    const group = db_mod.types.RelationalRowsAccessPredicateGroup{
        .predicates = try predicates.toOwnedSlice(alloc),
        .array_any = try array_any.toOwnedSlice(alloc),
        .array_contains = try array_contains.toOwnedSlice(alloc),
        .array_eq = try array_eq.toOwnedSlice(alloc),
        .in_predicates = try in_predicates.toOwnedSlice(alloc),
        .json_contains = try json_contains.toOwnedSlice(alloc),
        .json_path_eq = try json_path_eq.toOwnedSlice(alloc),
        .json_path_exists = try json_path_exists.toOwnedSlice(alloc),
        .text_patterns = try text_patterns.toOwnedSlice(alloc),
    };
    if (!accessPredicateGroupHasAnyPredicate(group)) {
        freeAccessPredicateGroup(alloc, group);
        return error.UnsupportedSqlShape;
    }
    return group;
}


pub fn parseAccessPredicateGroupsAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    params: []const value_mod.SqlValue,
    schema: runtime_schema.TableSchema,
    field_expression_qualifiers: []const []const u8,
    returning_expression_qualifiers: []const []const u8,
    defer_row_expression_field_validation: bool,
    realtime_ns: u64,
    generated_expression_ast: ?*const generated_parser.GeneratedSqlExpressionAst,
) ![]const db_mod.types.RelationalRowsAccessPredicateGroup {
    var branches = std.ArrayListUnmanaged(AccessPredicateBranch).empty;
    errdefer freeAccessPredicateBranches(alloc, &branches);
    try branches.append(alloc, .{});

    while (true) {
        const generated_condition_expression = try expr_generated_validate.generatedPredicateExpressionAtStart(tokens, pos.*, generated_expression_ast);
        if (!(try parseArrayOverlapIntoAccessBranchesAlloc(alloc, tokens, pos, params, schema, field_expression_qualifiers, returning_expression_qualifiers, defer_row_expression_field_validation, &branches, generated_condition_expression))) {
            const atom = try parseAccessPredicateAtomGroupAlloc(alloc, tokens, pos, params, schema, field_expression_qualifiers, returning_expression_qualifiers, defer_row_expression_field_validation, realtime_ns, generated_condition_expression);
            defer freeAccessPredicateGroup(alloc, atom);
            try appendAccessPredicateGroupToBranches(alloc, &branches, atom);
        }
        if (!parser.matchKeyword(tokens, pos, "and")) break;
    }

    if (branches.items.len == 0) return error.UnsupportedSqlShape;
    const groups = try alloc.alloc(db_mod.types.RelationalRowsAccessPredicateGroup, branches.items.len);
    var initialized: usize = 0;
    errdefer {
        freeAccessPredicateGroups(alloc, groups[0..initialized]);
        alloc.free(groups);
    }
    for (branches.items) |*branch| {
        groups[initialized] = try accessPredicateBranchToGroupAlloc(alloc, branch);
        if (!accessPredicateGroupHasAnyPredicate(groups[initialized])) return error.UnsupportedSqlShape;
        initialized += 1;
    }
    branches.deinit(alloc);
    return groups;
}

pub fn parseAccessOrWhereAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    params: []const value_mod.SqlValue,
    schema: runtime_schema.TableSchema,
    field_expression_qualifiers: []const []const u8,
    returning_expression_qualifiers: []const []const u8,
    defer_row_expression_field_validation: bool,
    access_or_predicates: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsAccessPredicateGroup),
    realtime_ns: u64,
    generated_expression_ast: ?*const generated_parser.GeneratedSqlExpressionAst,
) !void {
    while (true) {
        const parenthesized = parser.matchToken(tokens, pos, .lparen) != null;
        const groups = try parseAccessPredicateGroupsAlloc(alloc, tokens, pos, params, schema, field_expression_qualifiers, returning_expression_qualifiers, defer_row_expression_field_validation, realtime_ns, generated_expression_ast);
        var groups_transferred = false;
        errdefer if (!groups_transferred) {
            freeAccessPredicateGroups(alloc, groups);
            alloc.free(groups);
        };
        if (parenthesized) try parser.expectToken(tokens, pos, .rparen);
        try access_or_predicates.appendSlice(alloc, groups);
        groups_transferred = true;
        alloc.free(groups);
        if (!parser.matchKeyword(tokens, pos, "or")) break;
    }
}

pub fn parseAccessNotWhereAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    params: []const value_mod.SqlValue,
    schema: runtime_schema.TableSchema,
    field_expression_qualifiers: []const []const u8,
    returning_expression_qualifiers: []const []const u8,
    defer_row_expression_field_validation: bool,
    access_not_predicates: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsAccessPredicateGroup),
    realtime_ns: u64,
    generated_expression_ast: ?*const generated_parser.GeneratedSqlExpressionAst,
) !void {
    if (generated_expression_ast) |expression| {
        if (expression.kind != .logical_not) return error.UnsupportedSqlShape;
    }
    try parser.expectKeyword(tokens, pos, "not");
    try parser.expectToken(tokens, pos, .lparen);
    while (true) {
        const parenthesized = parser.matchToken(tokens, pos, .lparen) != null;
        const groups = try parseAccessPredicateGroupsAlloc(alloc, tokens, pos, params, schema, field_expression_qualifiers, returning_expression_qualifiers, defer_row_expression_field_validation, realtime_ns, generated_expression_ast);
        var groups_transferred = false;
        errdefer if (!groups_transferred) {
            freeAccessPredicateGroups(alloc, groups);
            alloc.free(groups);
        };
        if (parenthesized) try parser.expectToken(tokens, pos, .rparen);
        try access_not_predicates.appendSlice(alloc, groups);
        groups_transferred = true;
        alloc.free(groups);
        if (!parser.matchKeyword(tokens, pos, "or")) break;
    }
    try parser.expectToken(tokens, pos, .rparen);
}

pub fn parseBareBooleanWhereExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    type_context: expr_type.RowExpressionTypeContext,
    expression_predicates: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionCondition),
    options: BareBooleanWhereExpressionParserOptions,
) !void {
    const start = pos.*;
    const expression = try expr_row_parse.parseBooleanRowExpressionAlloc(alloc, tokens, pos, type_context, options.boolean_hooks);
    var expression_transferred = false;
    errdefer if (!expression_transferred) freeExpression(alloc, expression);
    try type_context.validateBooleanRowExpression(expression);
    try expr_generated_validate.validateGeneratedRowExpressionIdentityStrict(tokens, start, pos.*, expression, options.generated_expression_ast);
    if (expression.kind == .value and std.mem.eql(u8, expression.value_json, "true")) {
        freeExpression(alloc, expression);
        expression_transferred = true;
        return;
    }

    const value_json = try alloc.dupe(u8, "true");
    var value_transferred = false;
    errdefer if (!value_transferred) alloc.free(value_json);
    const rhs = try alloc.alloc(db_mod.types.RelationalRowsExpression, 1);
    var rhs_transferred = false;
    errdefer if (!rhs_transferred) alloc.free(rhs);
    rhs[0] = .{
        .kind = .value,
        .value_json = value_json,
    };

    try expression_predicates.append(alloc, .{
        .lhs = expression,
        .op = .eq,
        .rhs = rhs,
    });
    expression_transferred = true;
    value_transferred = true;
    rhs_transferred = true;
}

test "sql expr predicate peeks simple scalar set predicates" {
    const in_tokens = [_]Token{
        .{ .kind = .identifier, .text = "status" },
        .{ .kind = .identifier, .text = "in", .keyword = .in },
    };
    try std.testing.expect(peekSimpleScalarSetPredicate(&in_tokens, 0));

    const not_in_tokens = [_]Token{
        .{ .kind = .identifier, .text = "status" },
        .{ .kind = .identifier, .text = "not", .keyword = .not },
        .{ .kind = .identifier, .text = "in", .keyword = .in },
    };
    try std.testing.expect(peekSimpleScalarSetPredicate(&not_in_tokens, 0));

    const eq_any_tokens = [_]Token{
        .{ .kind = .identifier, .text = "status" },
        .{ .kind = .eq, .text = "=" },
        .{ .kind = .identifier, .text = "any", .keyword = .any },
    };
    try std.testing.expect(peekSimpleScalarSetPredicate(&eq_any_tokens, 0));
    try std.testing.expect(!peekSimpleScalarSetPredicate(&eq_any_tokens, 1));
}

test "sql expr predicate classifies negated where predicate starts" {
    const scalar_not_tokens = [_]Token{
        .{ .kind = .identifier, .text = "not", .keyword = .not },
        .{ .kind = .lparen, .text = "(" },
        .{ .kind = .identifier, .text = "status" },
        .{ .kind = .eq, .text = "=" },
        .{ .kind = .placeholder, .text = "$1" },
        .{ .kind = .rparen, .text = ")" },
    };
    try std.testing.expect(canParseScalarNotWhere(&scalar_not_tokens, 0));

    const scalar_set_tokens = [_]Token{
        .{ .kind = .identifier, .text = "not", .keyword = .not },
        .{ .kind = .lparen, .text = "(" },
        .{ .kind = .identifier, .text = "status" },
        .{ .kind = .identifier, .text = "in", .keyword = .in },
        .{ .kind = .lparen, .text = "(" },
        .{ .kind = .placeholder, .text = "$1" },
        .{ .kind = .rparen, .text = ")" },
        .{ .kind = .rparen, .text = ")" },
    };
    try std.testing.expect(!canParseScalarNotWhere(&scalar_set_tokens, 0));

    const expression_not_tokens = [_]Token{
        .{ .kind = .identifier, .text = "not", .keyword = .not },
        .{ .kind = .lparen, .text = "(" },
        .{ .kind = .identifier, .text = "lower", .keyword = .lower },
        .{ .kind = .lparen, .text = "(" },
        .{ .kind = .identifier, .text = "status" },
        .{ .kind = .rparen, .text = ")" },
        .{ .kind = .eq, .text = "=" },
        .{ .kind = .string, .text = "active" },
        .{ .kind = .rparen, .text = ")" },
    };
    try std.testing.expect(canParseExpressionNotWhere(&expression_not_tokens, 0));
}

test "sql expr predicate classifies bare boolean and access negation starts" {
    const schema = testTypeContext(std.testing.allocator).schema;

    const bare_boolean_tokens = [_]Token{
        .{ .kind = .identifier, .text = "status" },
        .{ .kind = .identifier, .text = "and", .keyword = .@"and" },
        .{ .kind = .identifier, .text = "true", .keyword = .true },
    };
    try std.testing.expect(booleanExpressionCanStartAt(&bare_boolean_tokens, 0, schema));
    try std.testing.expect(canParseBareBooleanWhereExpression(&bare_boolean_tokens, 0, schema));

    const scalar_predicate_tokens = [_]Token{
        .{ .kind = .identifier, .text = "status" },
        .{ .kind = .eq, .text = "=" },
        .{ .kind = .string, .text = "active" },
    };
    try std.testing.expect(!canParseBareBooleanWhereExpression(&scalar_predicate_tokens, 0, schema));

    const access_not_tokens = [_]Token{
        .{ .kind = .identifier, .text = "not", .keyword = .not },
        .{ .kind = .lparen, .text = "(" },
        .{ .kind = .identifier, .text = "metadata" },
        .{ .kind = .arrow_text, .text = "->>" },
        .{ .kind = .string, .text = "status" },
        .{ .kind = .eq, .text = "=" },
        .{ .kind = .string, .text = "active" },
        .{ .kind = .rparen, .text = ")" },
    };
    try std.testing.expect(canParseAccessNotWhere(&access_not_tokens, 0));
}

test "sql expr predicate classifies expression where condition starts" {
    const alloc = std.testing.allocator;
    const schema = testTypeContext(alloc).schema;

    const lower_tokens = [_]Token{
        .{ .kind = .identifier, .text = "lower", .keyword = .lower },
        .{ .kind = .lparen, .text = "(" },
        .{ .kind = .identifier, .text = "status" },
        .{ .kind = .rparen, .text = ")" },
        .{ .kind = .eq, .text = "=" },
        .{ .kind = .string, .text = "active" },
    };
    try std.testing.expect(try canParseExpressionWhereCondition(alloc, &lower_tokens, 0, schema, &.{}, &.{}, false));

    const concat_tokens = [_]Token{
        .{ .kind = .identifier, .text = "body" },
        .{ .kind = .pipe_concat, .text = "||" },
        .{ .kind = .string, .text = "!" },
        .{ .kind = .eq, .text = "=" },
        .{ .kind = .string, .text = "hello!" },
    };
    try std.testing.expect(try canParseExpressionWhereCondition(alloc, &concat_tokens, 0, schema, &.{}, &.{}, false));

    const scalar_tokens = [_]Token{
        .{ .kind = .identifier, .text = "status" },
        .{ .kind = .eq, .text = "=" },
        .{ .kind = .string, .text = "active" },
    };
    try std.testing.expect(!try canParseExpressionWhereCondition(alloc, &scalar_tokens, 0, schema, &.{}, &.{}, false));

    const deferred_tokens = [_]Token{
        .{ .kind = .identifier, .text = "unknown_field" },
        .{ .kind = .eq, .text = "=" },
        .{ .kind = .string, .text = "active" },
    };
    try std.testing.expect(try canParseExpressionWhereCondition(alloc, &deferred_tokens, 0, schema, &.{}, &.{}, true));
}

test "sql expr predicate parses scalar where predicates" {
    const alloc = std.testing.allocator;
    const schema = testTypeContext(alloc).schema;
    const tokens = [_]Token{
        .{ .kind = .identifier, .text = "status" },
        .{ .kind = .eq, .text = "=" },
        .{ .kind = .string, .text = "active" },
    };
    var pos: usize = 0;
    const check = try parseScalarWherePredicateAlloc(alloc, &tokens, &pos, schema, &.{}, &.{}, false, &.{}, 0, null);
    defer {
        alloc.free(check.field);
        if (check.value_json) |value_json| alloc.free(value_json);
    }
    try std.testing.expectEqualStrings("status", check.field);
    try std.testing.expectEqual(runtime_schema.RelationalCheckOp.eq, check.op);
    try std.testing.expectEqual(@as(usize, 3), pos);
}

test "sql expr predicate parses bare boolean where expressions" {
    const alloc = std.testing.allocator;
    const columns = [_]runtime_schema.RelationalColumn{
        .{ .name = "active", .path = "active", .field_type = .boolean },
    };
    const type_context: expr_type.RowExpressionTypeContext = .{
        .alloc = alloc,
        .schema = .{ .storage_mode = .relational, .relational_columns = &columns },
    };
    const tokens = [_]Token{.{ .kind = .identifier, .text = "active" }};
    var pos: usize = 0;
    var expression_predicates = std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionCondition).empty;
    defer {
        for (expression_predicates.items) |condition| freeExpressionCondition(alloc, condition);
        expression_predicates.deinit(alloc);
    }
    const FieldHookContext = struct {
        alloc: std.mem.Allocator,
        tokens: []const Token,
        pos: *usize,
    };
    const FieldHooks = struct {
        fn parse(ptr: *anyopaque) anyerror!db_mod.types.RelationalRowsExpression {
            const context: *FieldHookContext = @ptrCast(@alignCast(ptr));
            if (context.pos.* >= context.tokens.len) return error.UnsupportedSqlShape;
            const token = context.tokens[context.pos.*];
            if (token.kind != .identifier) return error.UnsupportedSqlShape;
            context.pos.* += 1;
            return .{
                .kind = .field,
                .field = try context.alloc.dupe(u8, token.text),
            };
        }
    };
    var hook_context = FieldHookContext{ .alloc = alloc, .tokens = &tokens, .pos = &pos };
    try parseBareBooleanWhereExpressionAlloc(alloc, &tokens, &pos, type_context, &expression_predicates, .{ .boolean_hooks = .{
        .ptr = &hook_context,
        .parse_expression = FieldHooks.parse,
        .parse_operand = FieldHooks.parse,
    } });
    try std.testing.expectEqual(@as(usize, 1), pos);
    try std.testing.expectEqual(@as(usize, 1), expression_predicates.items.len);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.field, expression_predicates.items[0].lhs.kind);
    try std.testing.expectEqual(runtime_schema.RelationalCheckOp.eq, expression_predicates.items[0].op);
}

test "sql expr predicate classifies joined mutation expression side" {
    const target_tokens = [_]Token{
        .{ .kind = .identifier, .text = "target.amount" },
        .{ .kind = .plus, .text = "+" },
        .{ .kind = .number, .text = "1" },
    };
    try std.testing.expect(joinedMutationExpressionCanStartAt(&target_tokens, 0, false));
    const target_side = (try joinedMutationExpressionSideAt(&target_tokens, 0, "target", "source", false)).?;
    try std.testing.expectEqual(db_mod.types.RelationalRowsJoinProjectionSide.left, target_side.single);

    const mixed_tokens = [_]Token{
        .{ .kind = .identifier, .text = "target.amount" },
        .{ .kind = .plus, .text = "+" },
        .{ .kind = .identifier, .text = "source.delta" },
    };
    const mixed_side = (try joinedMutationExpressionSideAt(&mixed_tokens, 0, "target", "source", false)).?;
    try std.testing.expect(mixed_side == .mixed);
}

test "sql expr predicate appends join scalar predicates" {
    const alloc = std.testing.allocator;
    var source_predicates = std.ArrayListUnmanaged(runtime_schema.RelationalCheck).empty;
    defer {
        freeRelationalChecks(alloc, source_predicates.items);
        source_predicates.deinit(alloc);
    }
    var expression_predicates = std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionCondition).empty;
    defer {
        for (expression_predicates.items) |condition| freeExpressionCondition(alloc, condition);
        expression_predicates.deinit(alloc);
    }

    try appendJoinOnScalarPredicateAlloc(alloc, &source_predicates, &expression_predicates, .inner, .right, "status", .eq, try alloc.dupe(u8, "\"open\""), null);
    try std.testing.expectEqual(@as(usize, 1), source_predicates.items.len);
    try std.testing.expectEqual(@as(usize, 0), expression_predicates.items.len);

    try appendJoinOnScalarPredicateAlloc(alloc, &source_predicates, &expression_predicates, .left, .left, "status", .eq, try alloc.dupe(u8, "\"queued\""), null);
    try std.testing.expectEqual(@as(usize, 1), source_predicates.items.len);
    try std.testing.expectEqual(@as(usize, 1), expression_predicates.items.len);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.field, expression_predicates.items[0].lhs.kind);
}

pub fn expressionNullSafeDistinctPredicateCanStartAt(tokens: []const Token, index: usize) bool {
    if (!expr_parse.expressionCanStartAt(tokens, index)) return false;
    var depth: usize = 0;
    var i = index;
    while (i < tokens.len) : (i += 1) {
        const token = tokens[i];
        switch (token.kind) {
            .lparen => depth += 1,
            .rparen => {
                if (depth == 0) return false;
                depth -= 1;
            },
            .identifier => if (depth == 0 and token.matchesKeywordTag(.is)) {
                var distinct_index = i + 1;
                if (distinct_index < tokens.len and tokens[distinct_index].matchesKeywordTag(.not)) {
                    distinct_index += 1;
                }
                return distinct_index + 1 < tokens.len and
                    tokens[distinct_index].matchesKeywordTag(.distinct) and
                    tokens[distinct_index + 1].matchesKeywordTag(.from);
            } else if (depth == 0 and expr_token.rowExpressionBoundaryKeywordToken(token)) {
                return false;
            },
            .semicolon, .comma => if (depth == 0) return false,
            else => {},
        }
    }
    return false;
}

pub fn expressionPredicateCanStartAt(tokens: []const Token, index: usize) bool {
    if (expr_parse.expressionCanStartAt(tokens, index) or
        textPatternSetPredicateCanStartAt(tokens, index) or
        expr_operator.jsonKeySetExpressionCanStartAt(tokens, index) or
        expr_operator.jsonExtractExpressionPredicateCanStartAt(tokens, index) or
        expr_operator.jsonExtractNullTestPredicateCanStartAt(tokens, index) or
        expr_operator.jsonExtractNullSafeDistinctPredicateCanStartAt(tokens, index) or
        expr_operator.jsonExtractMembershipPredicateCanStartAt(tokens, index))
    {
        return true;
    }
    if (index + 1 >= tokens.len or tokens[index].kind != .identifier) return false;
    const next = tokens[index + 1];
    return switch (next.kind) {
        .eq, .neq, .gt, .gte, .lt, .lte => true,
        .identifier => next.matchesKeywordTag(.is) or
            next.matchesKeywordTag(.in) or
            next.matchesKeywordTag(.not) or
            next.matchesKeywordTag(.between) or
            next.matchesKeywordTag(.like) or
            next.matchesKeywordTag(.ilike),
        else => false,
    };
}

fn testTypeContext(alloc: std.mem.Allocator) expr_type.RowExpressionTypeContext {
    const columns = struct {
        const values = [_]runtime_schema.RelationalColumn{
            .{ .name = "body", .path = "body", .field_type = .text },
            .{ .name = "status", .path = "status", .field_type = .keyword },
        };
    }.values;
    return .{
        .alloc = alloc,
        .schema = .{ .storage_mode = .relational, .relational_columns = &columns },
    };
}

test "sql expr_predicate builds like predicate expressions" {
    const alloc = std.testing.allocator;
    const type_context = testTypeContext(alloc);
    const body_expression: db_mod.types.RelationalRowsExpression = .{ .kind = .field, .field = "body" };

    const like_condition = try expressionLikeConditionAlloc(alloc, type_context, body_expression, "ant%", false, false);
    defer freeExpressionCondition(alloc, like_condition);
    try std.testing.expectEqual(runtime_schema.RelationalCheckOp.eq, like_condition.op);
    try std.testing.expectEqual(runtime_schema.RelationalRowsExpressionKind.like, like_condition.lhs.kind);
    try std.testing.expectEqual(@as(usize, 2), like_condition.lhs.operands.len);
    try std.testing.expectEqualStrings("body", like_condition.lhs.operands[0].field);
    try std.testing.expectEqualStrings("\"ant%\"", like_condition.lhs.operands[1].value_json);
    try std.testing.expectEqualStrings("true", like_condition.rhs[0].value_json);

    const like_any_condition = try expressionLikeSetConditionAlloc(alloc, type_context, body_expression, "[\"ant%\",\"bee%\"]", true, false, .any);
    defer freeExpressionCondition(alloc, like_any_condition);
    try std.testing.expectEqual(runtime_schema.RelationalRowsExpressionKind.bool_or, like_any_condition.lhs.kind);
    try std.testing.expectEqual(@as(usize, 2), like_any_condition.lhs.operands.len);
    try std.testing.expectEqual(runtime_schema.RelationalRowsExpressionKind.ilike, like_any_condition.lhs.operands[0].kind);
    try std.testing.expectEqualStrings("true", like_any_condition.rhs[0].value_json);
}

test "sql expr_predicate normalizes pattern escapes and json strings" {
    const alloc = std.testing.allocator;

    const normalized_like = try normalizeSqlLikePatternEscapeAlloc(alloc, "a#_%", "#");
    defer alloc.free(normalized_like);
    try std.testing.expectEqualStrings("a\\_%", normalized_like);

    const escaped_backslash_like = try normalizeSqlLikePatternEscapeAlloc(alloc, "a\\_%", "\\");
    defer alloc.free(escaped_backslash_like);
    try std.testing.expectEqualStrings("a\\_%", escaped_backslash_like);
    try std.testing.expectError(error.UnsupportedSqlShape, normalizeSqlLikePatternEscapeAlloc(alloc, "dangling#", "#"));

    const like_prefix = try sqlLikePrefixLiteralAlloc(alloc, "abc%");
    defer if (like_prefix) |owned| alloc.free(owned);
    try std.testing.expect(like_prefix != null);
    try std.testing.expectEqualStrings("abc", like_prefix.?);
    const escaped_like_prefix = try sqlLikePrefixLiteralAlloc(alloc, "ab\\_%");
    defer if (escaped_like_prefix) |owned| alloc.free(owned);
    try std.testing.expect(escaped_like_prefix != null);
    try std.testing.expectEqualStrings("ab_", escaped_like_prefix.?);
    const wildcard_like_prefix = try sqlLikePrefixLiteralAlloc(alloc, "a_c%");
    defer if (wildcard_like_prefix) |owned| alloc.free(owned);
    try std.testing.expect(wildcard_like_prefix == null);

    const json_string = try jsonStringLiteralValueAlloc(alloc, "\"hello\"");
    defer alloc.free(json_string);
    try std.testing.expectEqualStrings("hello", json_string);
    try std.testing.expectError(error.UnsupportedSqlShape, jsonStringLiteralValueAlloc(alloc, "123"));
}

test "sql expr_predicate appends scalar and text predicates" {
    const alloc = std.testing.allocator;
    const type_context = testTypeContext(alloc);

    var groups = std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionPredicateGroup).empty;
    defer {
        plan_mod.freeExpressionPredicateGroups(alloc, groups.items);
        groups.deinit(alloc);
    }
    try appendExpressionValuesJsonOrGroups(
        alloc,
        type_context,
        &groups,
        .{ .kind = .field, .field = "status" },
        "[\"active\",\"paused\"]",
    );
    try std.testing.expectEqual(@as(usize, 2), groups.items.len);
    try std.testing.expectEqual(@as(usize, 1), groups.items[0].conditions.len);
    try std.testing.expectEqual(runtime_schema.RelationalCheckOp.eq, groups.items[0].conditions[0].op);
    try std.testing.expectEqualStrings("status", groups.items[0].conditions[0].lhs.field);
    try std.testing.expectEqualStrings("\"active\"", groups.items[0].conditions[0].rhs[0].value_json);

    var text_patterns = std.ArrayListUnmanaged(db_mod.types.RelationalRowsTextPatternPredicate).empty;
    defer {
        plan_mod.freeTextPatterns(alloc, text_patterns.items);
        text_patterns.deinit(alloc);
    }
    try appendTextPatternPredicateAlloc(
        alloc,
        &text_patterns,
        "body",
        .{ .name = "body", .path = "body", .field_type = .text },
        "ant%",
        true,
        false,
    );
    try std.testing.expectEqual(@as(usize, 1), text_patterns.items.len);
    try std.testing.expectEqualStrings("body", text_patterns.items[0].field);
    try std.testing.expectEqualStrings("ant%", text_patterns.items[0].pattern);
    try std.testing.expect(text_patterns.items[0].case_insensitive);
    try std.testing.expect(!text_patterns.items[0].negated);

    var scalar_branches = std.ArrayListUnmanaged(ScalarOrCheckBranch).empty;
    defer freeScalarOrCheckBranches(alloc, &scalar_branches);
    try scalar_branches.append(alloc, .empty);
    try appendScalarBooleanCheckToBranch(alloc, &scalar_branches.items[0], "enabled", .eq, true);
    try appendScalarNullCheckToBranch(alloc, &scalar_branches.items[0], "deleted_at", .is_null);
    try appendScalarValuesJsonToOrBranches(alloc, &scalar_branches, "status", "[\"open\",\"queued\"]", .eq, null);
    try std.testing.expectEqual(@as(usize, 1), scalar_branches.items.len);
    try std.testing.expectEqual(@as(usize, 4), scalar_branches.items[0].items.len);
    try expandScalarValuesJsonIntoOrBranches(alloc, &scalar_branches, "priority", "[1,2]", .eq, null);
    try std.testing.expectEqual(@as(usize, 2), scalar_branches.items.len);
    try std.testing.expectEqual(@as(usize, 5), scalar_branches.items[0].items.len);
    try std.testing.expectEqual(@as(usize, 5), scalar_branches.items[1].items.len);
    try appendRelationalCheckCloneToScalarOrBranches(alloc, &scalar_branches, scalar_branches.items[0].items[0]);
    try std.testing.expectEqual(@as(usize, 6), scalar_branches.items[0].items.len);
    try std.testing.expectEqual(@as(usize, 6), scalar_branches.items[1].items.len);

    var scalar_predicate_groups = std.ArrayListUnmanaged(db_mod.types.RelationalRowsPredicateGroup).empty;
    defer {
        for (scalar_predicate_groups.items) |group| freePredicateGroup(alloc, group);
        scalar_predicate_groups.deinit(alloc);
    }
    var scalar_between_checks = std.ArrayListUnmanaged(runtime_schema.RelationalCheck).empty;
    defer {
        freeRelationalChecks(alloc, scalar_between_checks.items);
        scalar_between_checks.deinit(alloc);
    }
    const numeric_column: runtime_schema.RelationalColumn = .{ .name = "amount", .path = "amount", .field_type = .numeric };
    try appendBetweenPredicateValuesAlloc(alloc, &scalar_between_checks, &scalar_predicate_groups, "amount", numeric_column, "10", "20", false, false);
    try std.testing.expectEqual(@as(usize, 2), scalar_between_checks.items.len);
    try std.testing.expectEqual(runtime_schema.RelationalCheckOp.gte, scalar_between_checks.items[0].op);
    try std.testing.expectEqual(runtime_schema.RelationalCheckOp.lte, scalar_between_checks.items[1].op);
    try appendBetweenPredicateValuesAlloc(alloc, &scalar_between_checks, &scalar_predicate_groups, "amount", numeric_column, "10", "20", true, false);
    try appendBetweenScalarGroup(alloc, &scalar_predicate_groups, "amount", .gte, "10", .lte, "20", null);
    try appendBooleanIsNotPredicateGroups(alloc, &scalar_predicate_groups, "enabled", true);
    try appendScalarValuesJsonOrGroups(alloc, &scalar_predicate_groups, "status", "[\"open\",\"queued\"]", .ne, null);
    try appendTemporalRangeContainsPredicateGroups(alloc, &scalar_predicate_groups, .{
        .name = "valid_at",
        .start_column = "valid_from",
        .end_column = "valid_to",
    }, "\"2026-01-01T00:00:00Z\"");
    try appendTemporalRangeOverlapsPredicateGroups(alloc, &scalar_predicate_groups, .{
        .name = "valid_at",
        .start_column = "valid_from",
        .end_column = "valid_to",
    }, .{
        .start_json = "\"2026-01-01T00:00:00Z\"",
        .end_json = "\"2026-02-01T00:00:00Z\"",
    });
    try std.testing.expectEqual(@as(usize, 11), scalar_predicate_groups.items.len);
    try std.testing.expectEqual(@as(usize, 1), scalar_predicate_groups.items[0].predicates.len);
    try std.testing.expectEqual(@as(usize, 1), scalar_predicate_groups.items[1].predicates.len);
    try std.testing.expectEqual(@as(usize, 2), scalar_predicate_groups.items[2].predicates.len);

    var scalar_all_checks = std.ArrayListUnmanaged(runtime_schema.RelationalCheck).empty;
    defer {
        freeRelationalChecks(alloc, scalar_all_checks.items);
        scalar_all_checks.deinit(alloc);
    }
    try appendScalarAllEqualityPredicates(alloc, &scalar_all_checks, "status", "[\"open\",\"queued\"]", null);
    try std.testing.expectEqual(@as(usize, 2), scalar_all_checks.items.len);

    var access_branches = std.ArrayListUnmanaged(AccessPredicateBranch).empty;
    defer freeAccessPredicateBranches(alloc, &access_branches);
    try access_branches.append(alloc, .{});
    const access_group: db_mod.types.RelationalRowsAccessPredicateGroup = .{
        .predicates = &.{.{
            .name = "",
            .field = "status",
            .op = .eq,
            .value_json = "\"open\"",
        }},
        .text_patterns = &.{.{
            .field = "title",
            .pattern = "prefix",
        }},
    };
    try appendAccessPredicateGroupToBranches(alloc, &access_branches, access_group);
    try std.testing.expectEqual(@as(usize, 1), access_branches.items[0].predicates.items.len);
    try std.testing.expectEqual(@as(usize, 1), access_branches.items[0].text_patterns.items.len);
    try expandArrayOverlapValuesIntoAccessBranches(alloc, &access_branches, "tags", "[\"a\",\"b\"]");
    try std.testing.expectEqual(@as(usize, 2), access_branches.items.len);
    try std.testing.expectEqual(@as(usize, 1), access_branches.items[0].array_any.items.len);
    try std.testing.expectEqual(@as(usize, 1), access_branches.items[1].array_any.items.len);
    const owned_access_group = try accessPredicateBranchToGroupAlloc(alloc, &access_branches.items[0]);
    defer freeAccessPredicateGroup(alloc, owned_access_group);
    try std.testing.expect(accessPredicateGroupHasAnyPredicate(owned_access_group));
}

test "sql expr_predicate classifies where predicate token shapes" {
    const top_level_or_expression = [_]Token{
        .{ .kind = .identifier, .text = "lower", .keyword = .lower },
        .{ .kind = .lparen, .text = "(" },
        .{ .kind = .identifier, .text = "status" },
        .{ .kind = .rparen, .text = ")" },
        .{ .kind = .eq, .text = "=" },
        .{ .kind = .string, .text = "'active'" },
        .{ .kind = .identifier, .text = "OR", .keyword = .@"or" },
        .{ .kind = .identifier, .text = "status" },
        .{ .kind = .identifier, .text = "LIKE", .keyword = .like },
        .{ .kind = .identifier, .text = "ANY", .keyword = .any },
    };
    try std.testing.expect(whereTopLevelOrHasExpressionPredicateStart(top_level_or_expression[0..], 0));

    const parenthesized_or = [_]Token{
        .{ .kind = .lparen, .text = "(" },
        .{ .kind = .identifier, .text = "status" },
        .{ .kind = .eq, .text = "=" },
        .{ .kind = .string, .text = "'active'" },
        .{ .kind = .identifier, .text = "OR", .keyword = .@"or" },
        .{ .kind = .identifier, .text = "status" },
        .{ .kind = .eq, .text = "=" },
        .{ .kind = .string, .text = "'paused'" },
        .{ .kind = .rparen, .text = ")" },
    };
    try std.testing.expect(parenthesizedWhereHasTopLevelOr(parenthesized_or[0..], 0));

    const access_predicate = [_]Token{
        .{ .kind = .identifier, .text = "body" },
        .{ .kind = .arrow_text, .text = "->>" },
        .{ .kind = .string, .text = "'status'" },
    };
    try std.testing.expect(whereTopLevelOrHasAccessPredicate(access_predicate[0..], 0));

    const value_any_tokens = [_]Token{
        .{ .kind = .identifier, .text = "TRUE", .keyword = .true },
        .{ .kind = .eq, .text = "=" },
        .{ .kind = .identifier, .text = "ANY", .keyword = .any },
        .{ .kind = .lparen, .text = "(" },
        .{ .kind = .identifier, .text = "flags" },
    };
    try std.testing.expect(valueEqualsAnyArrayPredicateCanStart(value_any_tokens[0..], 0));
    const quoted_value_any_tokens = [_]Token{
        .{ .kind = .identifier, .text = "true", .source_start = 0, .source_end = 6 },
        .{ .kind = .eq, .text = "=" },
        .{ .kind = .identifier, .text = "any", .source_start = 9, .source_end = 14 },
        .{ .kind = .lparen, .text = "(" },
        .{ .kind = .identifier, .text = "flags" },
    };
    try std.testing.expect(!valueEqualsAnyArrayPredicateCanStart(quoted_value_any_tokens[0..], 0));

    const string_to_array_contains = [_]Token{
        .{ .kind = .identifier, .text = "string_to_array", .keyword = .string_to_array },
        .{ .kind = .lparen, .text = "(" },
        .{ .kind = .identifier, .text = "tags" },
        .{ .kind = .comma, .text = "," },
        .{ .kind = .string, .text = "','" },
        .{ .kind = .rparen, .text = ")" },
        .{ .kind = .at_contains, .text = "@>" },
        .{ .kind = .identifier, .text = "ARRAY", .keyword = .array },
    };
    try std.testing.expect(stringToArrayPredicateIsContainment(string_to_array_contains[0..], 0));
}
