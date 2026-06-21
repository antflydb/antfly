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

const ast = @import("ast.zig");
const binder = @import("binder.zig");
const db_mod = @import("../../storage/db/mod.zig");
const ddl_plan = @import("ddl_plan.zig");
const grammar = @import("grammar.zig");
const lexer = @import("lexer.zig");
const plan_mod = @import("plan.zig");
const parser = @import("parser.zig");
const platform_time = @import("../../platform/time.zig");
const runtime_schema = @import("../../storage/schema.zig");
const strings = @import("strings.zig");
const token_mod = @import("token.zig");
const value_mod = @import("value.zig");

pub const Token = token_mod.Token;
pub const TokenKind = token_mod.TokenKind;
pub const max_scalar_or_expanded_branches: usize = 32;

pub const QueryPlanParserHooks = struct {
    ptr: *anyopaque,
    parse_select_with_set_boundary: *const fn (
        *anyopaque,
        bool,
        []const db_mod.types.RelationalRowsCte,
    ) anyerror!plan_mod.LoweredSelect,
    parse_select_with_set_result_tail_boundary: *const fn (
        *anyopaque,
        []const db_mod.types.RelationalRowsCte,
    ) anyerror!plan_mod.LoweredSelect,
};

pub const UniquePredicateWhereExpressionParserHooks = struct {
    ptr: *anyopaque,
    parse_condition: *const fn (*anyopaque) anyerror!db_mod.types.RelationalRowsExpressionCondition,
};

pub const AggregateOutputFieldExpressionConditionParserHooks = struct {
    ptr: *anyopaque,
    parse_field: *const fn (
        *anyopaque,
        []const []const u8,
        []const db_mod.types.RelationalRowsExpressionProjection,
        []const db_mod.types.RelationalRowsAggregateSpec,
    ) anyerror![]const u8,
    parse_value_json: *const fn (*anyopaque) anyerror![]const u8,
};

pub const ReturningProjectionParserHooks = struct {
    ptr: *anyopaque,
    parse_field_expression_owned: *const fn (*anyopaque) anyerror![]const u8,
    parse_select_item: *const fn (
        *anyopaque,
        []const []const u8,
    ) anyerror!plan_mod.SelectItem,
};

pub const JoinedMutationReturningProjectionParserHooks = struct {
    ptr: *anyopaque,
    parse_field_expression_owned: *const fn (*anyopaque) anyerror![]const u8,
    parse_select_item: *const fn (
        *anyopaque,
        []const u8,
        []const []const u8,
    ) anyerror!plan_mod.SelectItem,
};

pub const AggregateOutputExpressionConditionParserHooks = struct {
    ptr: *anyopaque,
    parse_condition: *const fn (
        *anyopaque,
        runtime_schema.TableSchema,
    ) anyerror!db_mod.types.RelationalRowsExpressionCondition,
};

pub const BareBooleanAggregateHavingExpressionParserHooks = struct {
    ptr: *anyopaque,
    parse_bare_boolean: *const fn (
        *anyopaque,
        runtime_schema.TableSchema,
        *std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionCondition),
    ) anyerror!void,
};

pub const AggregateOutputOrderExpressionParserHooks = struct {
    ptr: *anyopaque,
    parse_order: *const fn (
        *anyopaque,
        runtime_schema.TableSchema,
    ) anyerror!db_mod.types.RelationalRowsQueryOrder,
};

pub const WindowOutputOrderExpressionParserHooks = struct {
    ptr: *anyopaque,
    parse_order: *const fn (
        *anyopaque,
        runtime_schema.TableSchema,
    ) anyerror!db_mod.types.RelationalRowsQueryOrder,
};

pub const JoinOutputOrderExpressionParserHooks = struct {
    ptr: *anyopaque,
    parse_order: *const fn (
        *anyopaque,
        runtime_schema.TableSchema,
    ) anyerror!db_mod.types.RelationalRowsQueryOrder,
};

const cloneExpressionConditionAlloc = plan_mod.cloneExpressionConditionAlloc;
const cloneExpressionConditionsAlloc = plan_mod.cloneExpressionConditionsAlloc;
const cloneExpressionConditionsConcatAlloc = plan_mod.cloneExpressionConditionsConcatAlloc;
const freeExpressionProjection = plan_mod.freeExpressionProjection;
const freeExpressionProjections = plan_mod.freeExpressionProjections;
const cloneExpressionPredicateGroupsAlloc = plan_mod.cloneExpressionPredicateGroupsAlloc;
const cloneInPredicatesAlloc = plan_mod.cloneInPredicatesAlloc;
const cloneInPredicatesConcatAlloc = plan_mod.cloneInPredicatesConcatAlloc;
const cloneQueryRelationalCheckAlloc = plan_mod.cloneQueryRelationalCheckAlloc;
const cloneQueryRelationalChecksAlloc = plan_mod.cloneQueryRelationalChecksAlloc;
const cloneQueryRelationalChecksConcatAlloc = plan_mod.cloneQueryRelationalChecksConcatAlloc;
const freeAccessPredicateGroup = plan_mod.freeAccessPredicateGroup;
const freeAccessPredicateGroups = plan_mod.freeAccessPredicateGroups;
const freeArrayAny = plan_mod.freeArrayAny;
const freeArrayContains = plan_mod.freeArrayContains;
const freeArrayEq = plan_mod.freeArrayEq;
const freeExpression = plan_mod.freeExpression;
const freeExpressionCondition = plan_mod.freeExpressionCondition;
const freeExpressionConditions = plan_mod.freeExpressionConditions;
const freeExpressionArrayContains = plan_mod.freeExpressionArrayContains;
const freeExpressionPredicateGroup = plan_mod.freeExpressionPredicateGroup;
const freeExpressionPredicateGroups = plan_mod.freeExpressionPredicateGroups;
const freeInPredicates = plan_mod.freeInPredicates;
const freeJsonContains = plan_mod.freeJsonContains;
const freeJsonPathEq = plan_mod.freeJsonPathEq;
const freeJsonPathExists = plan_mod.freeJsonPathExists;
const freeOrderBy = plan_mod.freeOrderBy;
const freePredicateGroup = plan_mod.freePredicateGroup;
const freePredicateGroups = plan_mod.freePredicateGroups;
const freeRelationalChecks = plan_mod.freeRelationalChecks;
const freeTextPatterns = plan_mod.freeTextPatterns;
const cloneExpressionAlloc = plan_mod.cloneExpressionAlloc;

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

pub const PeriodRangeValuePair = struct {
    start_json: []const u8,
    end_json: []const u8,
};

pub fn freePeriodRangeValuePair(alloc: std.mem.Allocator, value: PeriodRangeValuePair) void {
    alloc.free(value.start_json);
    alloc.free(value.end_json);
}

pub fn parseUniquePredicateWhereExpressionConditionsAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    hooks: UniquePredicateWhereExpressionParserHooks,
) ![]const db_mod.types.RelationalRowsExpressionCondition {
    var conditions = std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionCondition).empty;
    errdefer {
        freeExpressionConditions(alloc, conditions.items);
        conditions.deinit(alloc);
    }
    while (true) {
        var condition = try hooks.parse_condition(hooks.ptr);
        var condition_transferred = false;
        errdefer if (!condition_transferred) freeExpressionCondition(alloc, condition);
        try conditions.append(alloc, condition);
        condition = undefined;
        condition_transferred = true;
        if (!parser.matchKeyword(tokens, pos, "and")) break;
    }
    return try conditions.toOwnedSlice(alloc);
}

pub const ProjectedColumnType = struct {
    field_type: runtime_schema.AntflyType,
    array_item_type: ?runtime_schema.AntflyType = null,
};

pub const AggregateFilter = struct {
    predicates: []const runtime_schema.RelationalCheck = &.{},
    array_any: []const db_mod.types.RelationalRowsArrayAnyPredicate = &.{},
    array_contains: []const db_mod.types.RelationalRowsArrayContainsPredicate = &.{},
    array_eq: []const db_mod.types.RelationalRowsArrayEqPredicate = &.{},
    in_predicates: []const db_mod.types.RelationalRowsInPredicate = &.{},
    json_contains: []const db_mod.types.RelationalRowsJsonContainsPredicate = &.{},
    json_path_eq: []const db_mod.types.RelationalRowsJsonPathEqPredicate = &.{},
    json_path_exists: []const db_mod.types.RelationalRowsJsonPathExistsPredicate = &.{},
    text_patterns: []const db_mod.types.RelationalRowsTextPatternPredicate = &.{},
    expressions: []const db_mod.types.RelationalRowsExpressionCondition = &.{},
    expression_array_contains: []const db_mod.types.RelationalRowsExpressionArrayContainsPredicate = &.{},
    any_groups: []const db_mod.types.RelationalRowsExpressionPredicateGroup = &.{},
    not_groups: []const db_mod.types.RelationalRowsExpressionPredicateGroup = &.{},
};

pub const AggregatePercentileArgument = struct {
    percentile: ?f64 = null,
    percentiles: []const f64 = &.{},
};

pub const RegexPredicateOperator = struct {
    case_insensitive: bool,
    negated: bool,
};

pub fn matchRegexPredicateOperator(tokens: []const Token, pos: *usize) ?RegexPredicateOperator {
    if (parser.matchToken(tokens, pos, .regex_match) != null) return .{ .case_insensitive = false, .negated = false };
    if (parser.matchToken(tokens, pos, .regex_imatch) != null) return .{ .case_insensitive = true, .negated = false };
    if (parser.matchToken(tokens, pos, .regex_not_match) != null) return .{ .case_insensitive = false, .negated = true };
    if (parser.matchToken(tokens, pos, .regex_not_imatch) != null) return .{ .case_insensitive = true, .negated = true };
    return null;
}

pub const ArithmeticOperator = struct {
    token: TokenKind,
    kind: db_mod.types.RelationalRowsExpressionKind,
    precedence: u8,
};

pub const BooleanOperator = struct {
    keyword: []const u8,
    kind: db_mod.types.RelationalRowsExpressionKind,
    precedence: u8,
};

pub fn peekArithmeticOperator(tokens: []const Token, pos: usize) ?ArithmeticOperator {
    if (parser.peekKind(tokens, pos, .plus)) return .{ .token = .plus, .kind = .add, .precedence = 1 };
    if (parser.peekKind(tokens, pos, .minus)) return .{ .token = .minus, .kind = .sub, .precedence = 1 };
    if (parser.peekKind(tokens, pos, .star)) return .{ .token = .star, .kind = .mul, .precedence = 2 };
    if (parser.peekKind(tokens, pos, .slash)) return .{ .token = .slash, .kind = .div, .precedence = 2 };
    if (parser.peekKind(tokens, pos, .percent)) return .{ .token = .percent, .kind = .mod, .precedence = 2 };
    return null;
}

pub fn peekArithmeticRhsKeyword(tokens: []const Token, pos: usize, keyword: []const u8) bool {
    if (pos + 2 >= tokens.len) return false;
    const op = tokens[pos + 1].kind;
    if (op != .plus and op != .minus and op != .star and op != .slash and op != .percent) return false;
    const rhs = tokens[pos + 2];
    return rhs.kind == .identifier and std.ascii.eqlIgnoreCase(rhs.text, keyword);
}

pub fn peekBooleanOperator(tokens: []const Token, pos: usize) ?BooleanOperator {
    if (parser.peekKeyword(tokens, pos, "or")) return .{ .keyword = "or", .kind = .bool_or, .precedence = 1 };
    if (parser.peekKeyword(tokens, pos, "and")) return .{ .keyword = "and", .kind = .bool_and, .precedence = 2 };
    return null;
}

pub fn peekJsonExtractOperator(tokens: []const Token, pos: usize) bool {
    return pos < tokens.len and tokenKindIsJsonExtractOperator(tokens[pos].kind);
}

pub fn matchJsonExtractOperator(tokens: []const Token, pos: *usize) ?TokenKind {
    if (!peekJsonExtractOperator(tokens, pos.*)) return null;
    const operator = tokens[pos.*].kind;
    pos.* += 1;
    return operator;
}

pub fn matchPostfixNullTest(tokens: []const Token, pos: *usize) ?runtime_schema.RelationalCheckOp {
    if (parser.matchKeyword(tokens, pos, "isnull")) return .is_null;
    if (parser.matchKeyword(tokens, pos, "notnull")) return .is_not_null;
    return null;
}

pub const ExpressionIsTailKind = enum {
    null_test,
    distinct_comparison,
    boolean_unknown,
    boolean_literal,
};

pub const ExpressionIsTailOptions = struct {
    allow_distinct: bool = true,
    allow_boolean_unknown: bool = false,
    allow_boolean_literal: bool = false,
    allow_boolean_literal_negation: bool = false,
};

pub const ExpressionIsTail = struct {
    op: runtime_schema.RelationalCheckOp,
    kind: ExpressionIsTailKind,
    boolean_value: bool = false,
    boolean_negated: bool = false,
};

pub fn parseExpressionIsTailIf(
    tokens: []const Token,
    pos: *usize,
    options: ExpressionIsTailOptions,
) !?ExpressionIsTail {
    if (!parser.matchKeyword(tokens, pos, "is")) return null;
    const not = parser.matchKeyword(tokens, pos, "not");
    if (parser.matchKeyword(tokens, pos, "distinct")) {
        if (!options.allow_distinct) return error.UnsupportedSqlShape;
        try parser.expectKeyword(tokens, pos, "from");
        return .{
            .op = if (not) .is_not_distinct else .is_distinct,
            .kind = .distinct_comparison,
        };
    }
    if (options.allow_boolean_unknown and parser.matchKeyword(tokens, pos, "unknown")) {
        return .{
            .op = if (not) .is_not_null else .is_null,
            .kind = .boolean_unknown,
        };
    }
    if (options.allow_boolean_literal) {
        if (parser.matchKeyword(tokens, pos, "true")) {
            if (not and !options.allow_boolean_literal_negation) return error.UnsupportedSqlShape;
            return .{
                .op = .eq,
                .kind = .boolean_literal,
                .boolean_value = true,
                .boolean_negated = not,
            };
        }
        if (parser.matchKeyword(tokens, pos, "false")) {
            if (not and !options.allow_boolean_literal_negation) return error.UnsupportedSqlShape;
            return .{
                .op = .eq,
                .kind = .boolean_literal,
                .boolean_value = false,
                .boolean_negated = not,
            };
        }
    }
    try parser.expectKeyword(tokens, pos, "null");
    return .{
        .op = if (not) .is_not_null else .is_null,
        .kind = .null_test,
    };
}

pub fn parseComparisonOp(tokens: []const Token, pos: *usize) !runtime_schema.RelationalCheckOp {
    if (parser.matchToken(tokens, pos, .eq) != null) return .eq;
    if (parser.matchToken(tokens, pos, .neq) != null) return .ne;
    if (parser.matchToken(tokens, pos, .gt) != null) return .gt;
    if (parser.matchToken(tokens, pos, .gte) != null) return .gte;
    if (parser.matchToken(tokens, pos, .lt) != null) return .lt;
    if (parser.matchToken(tokens, pos, .lte) != null) return .lte;
    return error.UnsupportedSqlShape;
}

pub fn parseBulkIoWhereExpressionsAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    schema: runtime_schema.TableSchema,
    field_expression_qualifiers: []const []const u8,
    returning_expression_qualifiers: []const []const u8,
    defer_row_expression_field_validation: bool,
) ![]const db_mod.types.RelationalRowsExpressionCondition {
    const cursor = parser.Cursor.init(tokens, pos);
    if (!cursor.matchKeyword("where")) return &.{};

    var conditions = std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionCondition).empty;
    errdefer {
        plan_mod.freeExpressionConditions(alloc, conditions.items);
        conditions.deinit(alloc);
    }
    while (true) {
        const condition = try parseBulkIoWhereExpressionConditionAlloc(
            alloc,
            tokens,
            pos,
            schema,
            field_expression_qualifiers,
            returning_expression_qualifiers,
            defer_row_expression_field_validation,
        );
        var condition_transferred = false;
        errdefer if (!condition_transferred) plan_mod.freeExpressionCondition(alloc, condition);
        try conditions.append(alloc, condition);
        condition_transferred = true;
        if (!cursor.matchKeyword("and")) break;
    }
    if (cursor.matchToken(.semicolon) != null and !cursor.atEnd()) return error.UnsupportedSqlShape;
    if (!cursor.atEnd()) return error.UnsupportedSqlShape;
    return try conditions.toOwnedSlice(alloc);
}

fn parseBulkIoWhereExpressionConditionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    schema: runtime_schema.TableSchema,
    field_expression_qualifiers: []const []const u8,
    returning_expression_qualifiers: []const []const u8,
    defer_row_expression_field_validation: bool,
) !db_mod.types.RelationalRowsExpressionCondition {
    const parsed_field = try parseRowExpressionFieldOwnedAlloc(
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

    const op = try parseComparisonOp(tokens, pos);
    const value_json = try value_mod.parseSqlUntypedValueJsonAlloc(alloc, tokens, pos);
    var value_transferred = false;
    errdefer if (!value_transferred) alloc.free(value_json);

    const rhs = try alloc.alloc(db_mod.types.RelationalRowsExpression, 1);
    var rhs_transferred = false;
    errdefer if (!rhs_transferred) alloc.free(rhs);
    rhs[0] = .{
        .kind = .value,
        .value_json = value_json,
    };

    field_transferred = true;
    value_transferred = true;
    rhs_transferred = true;
    return .{
        .lhs = .{
            .kind = .field,
            .field = field,
        },
        .op = op,
        .rhs = rhs,
    };
}

pub fn parseExpressionCastType(tokens: []const Token, pos: *usize) !db_mod.types.RelationalRowsExpressionCastType {
    const token = parser.matchToken(tokens, pos, .identifier) orelse return error.UnsupportedSqlShape;
    if (std.ascii.eqlIgnoreCase(token.text, "text")) return .text;
    if (std.ascii.eqlIgnoreCase(token.text, "numeric")) return .numeric;
    if (std.ascii.eqlIgnoreCase(token.text, "bool") or std.ascii.eqlIgnoreCase(token.text, "boolean")) return .bool;
    if (std.ascii.eqlIgnoreCase(token.text, "datetime") or
        std.ascii.eqlIgnoreCase(token.text, "timestamp") or
        std.ascii.eqlIgnoreCase(token.text, "timestamptz") or
        std.ascii.eqlIgnoreCase(token.text, "date")) return .datetime;
    return error.UnsupportedSqlShape;
}

pub fn buildNowRowExpressionAlloc(alloc: std.mem.Allocator) !db_mod.types.RelationalRowsExpression {
    return .{
        .kind = .now,
        .value_json = try std.fmt.allocPrint(alloc, "{d}", .{platform_time.realtimeNs()}),
    };
}

pub fn parseSqlNowRowExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !db_mod.types.RelationalRowsExpression {
    try value_mod.parseSqlNowCall(tokens, pos);
    return try buildNowRowExpressionAlloc(alloc);
}

pub fn parseSqlCurrentDateRowExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !db_mod.types.RelationalRowsExpression {
    try value_mod.parseSqlCurrentDateKeyword(tokens, pos);
    return try buildCurrentDateExpressionAlloc(alloc);
}

pub fn parseSqlTypedDatetimeLiteralRowExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !db_mod.types.RelationalRowsExpression {
    return .{
        .kind = .value,
        .value_json = try value_mod.parseSqlTypedDatetimeLiteralValueJsonAlloc(alloc, tokens, pos),
    };
}

pub fn parseSqlUuidV4RowExpression(tokens: []const Token, pos: *usize) !db_mod.types.RelationalRowsExpression {
    try value_mod.parseSqlUuidV4Call(tokens, pos);
    return .{ .kind = .uuid_v4 };
}

pub fn parseSqlIntervalRowExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !db_mod.types.RelationalRowsExpression {
    const interval = try value_mod.parseSqlIntervalLiteral(tokens, pos);
    return try buildSingleIntervalLiteralExpressionAlloc(alloc, interval);
}

pub fn buildSingleIntervalLiteralExpressionAlloc(
    alloc: std.mem.Allocator,
    interval: value_mod.SqlIntervalLiteral,
) !db_mod.types.RelationalRowsExpression {
    const has_calendar = interval.calendar_months != 0 or (interval.saw_calendar and interval.fixed_ns == 0);
    const has_fixed = interval.fixed_ns != 0 or (interval.saw_fixed and !has_calendar);
    if (has_calendar and has_fixed) return error.UnsupportedSqlShape;
    if (has_calendar) return try buildIntervalComponentExpressionAlloc(alloc, .interval_months, interval.calendar_months);
    if (has_fixed) return try buildIntervalComponentExpressionAlloc(alloc, .interval_ns, interval.fixed_ns);
    return error.UnsupportedSqlShape;
}

pub fn buildIntervalComponentExpressionAlloc(
    alloc: std.mem.Allocator,
    kind: db_mod.types.RelationalRowsExpressionKind,
    value: u64,
) !db_mod.types.RelationalRowsExpression {
    if (kind != .interval_ns and kind != .interval_months) return error.UnsupportedSqlShape;
    const value_json = try std.fmt.allocPrint(alloc, "{d}", .{value});
    var value_transferred = false;
    errdefer if (!value_transferred) alloc.free(value_json);
    const operands = try alloc.alloc(db_mod.types.RelationalRowsExpression, 1);
    var operands_transferred = false;
    errdefer if (!operands_transferred) alloc.free(operands);
    operands[0] = .{ .kind = .value, .value_json = value_json };
    value_transferred = true;
    operands_transferred = true;
    return .{
        .kind = kind,
        .operands = operands,
    };
}

pub fn buildIntervalLiteralArithmeticAlloc(
    alloc: std.mem.Allocator,
    lhs: db_mod.types.RelationalRowsExpression,
    op_kind: db_mod.types.RelationalRowsExpressionKind,
    interval: value_mod.SqlIntervalLiteral,
) !db_mod.types.RelationalRowsExpression {
    if (op_kind != .add and op_kind != .sub) return error.UnsupportedSqlShape;

    const has_calendar = interval.calendar_months != 0;
    const has_fixed = interval.fixed_ns != 0;
    if (!has_calendar and !has_fixed) {
        const rhs = try buildSingleIntervalLiteralExpressionAlloc(alloc, interval);
        var rhs_owned = true;
        errdefer if (rhs_owned) freeExpression(alloc, rhs);
        const expression = try buildBinaryExpressionAlloc(alloc, op_kind, lhs, rhs);
        rhs_owned = false;
        return expression;
    }
    if (has_calendar and !has_fixed) {
        const rhs = try buildIntervalComponentExpressionAlloc(alloc, .interval_months, interval.calendar_months);
        var rhs_owned = true;
        errdefer if (rhs_owned) freeExpression(alloc, rhs);
        const expression = try buildBinaryExpressionAlloc(alloc, op_kind, lhs, rhs);
        rhs_owned = false;
        return expression;
    }
    if (!has_calendar and has_fixed) {
        const rhs = try buildIntervalComponentExpressionAlloc(alloc, .interval_ns, interval.fixed_ns);
        var rhs_owned = true;
        errdefer if (rhs_owned) freeExpression(alloc, rhs);
        const expression = try buildBinaryExpressionAlloc(alloc, op_kind, lhs, rhs);
        rhs_owned = false;
        return expression;
    }

    const calendar = try buildIntervalComponentExpressionAlloc(alloc, .interval_months, interval.calendar_months);
    var calendar_owned = true;
    errdefer if (calendar_owned) freeExpression(alloc, calendar);
    const fixed = try buildIntervalComponentExpressionAlloc(alloc, .interval_ns, interval.fixed_ns);
    var fixed_owned = true;
    errdefer if (fixed_owned) freeExpression(alloc, fixed);

    const inner_operands = try alloc.alloc(db_mod.types.RelationalRowsExpression, 2);
    var inner_operands_owned = true;
    errdefer if (inner_operands_owned) alloc.free(inner_operands);
    const outer_operands = try alloc.alloc(db_mod.types.RelationalRowsExpression, 2);
    var outer_operands_owned = true;
    errdefer if (outer_operands_owned) alloc.free(outer_operands);

    inner_operands[0] = lhs;
    inner_operands[1] = calendar;
    calendar_owned = false;
    inner_operands_owned = false;
    outer_operands[0] = .{
        .kind = op_kind,
        .operands = inner_operands,
    };
    outer_operands[1] = fixed;
    fixed_owned = false;
    outer_operands_owned = false;
    return .{
        .kind = op_kind,
        .operands = outer_operands,
    };
}

pub fn buildBinaryExpressionAlloc(
    alloc: std.mem.Allocator,
    kind: db_mod.types.RelationalRowsExpressionKind,
    lhs: db_mod.types.RelationalRowsExpression,
    rhs: db_mod.types.RelationalRowsExpression,
) !db_mod.types.RelationalRowsExpression {
    const operands = try alloc.alloc(db_mod.types.RelationalRowsExpression, 2);
    var operands_owned = true;
    errdefer if (operands_owned) alloc.free(operands);
    operands[0] = lhs;
    operands[1] = rhs;
    operands_owned = false;
    return .{
        .kind = kind,
        .operands = operands,
    };
}

pub const ExtensionFunctionBinding = struct {
    sql_name: []const u8,
    native_expression_kind: runtime_schema.RelationalRowsExpressionKind,
    arity: u16,
};

pub const RoutineExpressionBinding = struct {
    sql_name: []const u8,
    arity: u16,
    expression: runtime_schema.RelationalRowsExpression,
    null_input: ?ddl_plan.RoutineNullInput = null,
};

pub const SqlFunctionBindings = struct {
    extension_functions: []const ExtensionFunctionBinding = &.{},
    routine_expressions: []const RoutineExpressionBinding = &.{},
};

pub const RowExpressionParserHooks = struct {
    ptr: *anyopaque,
    parse_operand: *const fn (*anyopaque) anyerror!db_mod.types.RelationalRowsExpression,
};

pub const ExtensionFunctionRowExpressionParserHooks = struct {
    ptr: *anyopaque,
    parse_operand: *const fn (*anyopaque) anyerror!db_mod.types.RelationalRowsExpression,
};

pub const RoutineExpressionRowExpressionParserHooks = struct {
    ptr: *anyopaque,
    parse_operand: *const fn (*anyopaque) anyerror!db_mod.types.RelationalRowsExpression,
};

pub const ParenthesizedRowExpressionParserHooks = struct {
    ptr: *anyopaque,
    parse_expression: *const fn (*anyopaque) anyerror!db_mod.types.RelationalRowsExpression,
};

pub const BooleanNotRowExpressionParserHooks = struct {
    ptr: *anyopaque,
    parse_parenthesized_operand: *const fn (*anyopaque) anyerror!db_mod.types.RelationalRowsExpression,
    parse_operand: *const fn (*anyopaque) anyerror!db_mod.types.RelationalRowsExpression,
};

pub const CastRowExpressionParserHooks = struct {
    ptr: *anyopaque,
    parse_operand: *const fn (*anyopaque) anyerror!db_mod.types.RelationalRowsExpression,
};

pub const CoalesceRowExpressionParserHooks = struct {
    ptr: *anyopaque,
    parse_expression: *const fn (*anyopaque) anyerror!db_mod.types.RelationalRowsExpression,
};

pub const CoalesceProjectionParserHooks = struct {
    ptr: *anyopaque,
    parse_value_json: *const fn (*anyopaque) anyerror![]const u8,
};

pub const ExpressionProjectionParserHooks = struct {
    ptr: *anyopaque,
    parse_expression: *const fn (*anyopaque) anyerror!db_mod.types.RelationalRowsExpression,
    parse_boolean_expression: *const fn (*anyopaque) anyerror!db_mod.types.RelationalRowsExpression,
};

pub const JsonValueExpressionProjectionParserHooks = struct {
    ptr: *anyopaque,
    parse_value_json: *const fn (*anyopaque) anyerror![]const u8,
};

pub const NullifRowExpressionParserHooks = struct {
    ptr: *anyopaque,
    parse_operand: *const fn (*anyopaque) anyerror!db_mod.types.RelationalRowsExpression,
};

pub const RowExpressionInputDomain = enum {
    text,
    numeric,
    json,
    any,
};

pub const FixedUnaryRowExpressionParserHooks = struct {
    ptr: *anyopaque,
    parse_expression: *const fn (*anyopaque) anyerror!db_mod.types.RelationalRowsExpression,
};

pub const FixedBinaryRowExpressionParserHooks = struct {
    ptr: *anyopaque,
    parse_expression: *const fn (*anyopaque) anyerror!db_mod.types.RelationalRowsExpression,
};

pub const VariadicRowExpressionParserHooks = struct {
    ptr: *anyopaque,
    parse_expression: *const fn (*anyopaque) anyerror!db_mod.types.RelationalRowsExpression,
    parse_operand: *const fn (*anyopaque) anyerror!db_mod.types.RelationalRowsExpression,
};

pub const JsonBuildObjectRowExpressionParserHooks = struct {
    ptr: *anyopaque,
    parse_expression: *const fn (*anyopaque) anyerror!db_mod.types.RelationalRowsExpression,
};

pub const UnaryNegativeRowExpressionParserHooks = struct {
    ptr: *anyopaque,
    parse_operand: *const fn (*anyopaque) anyerror!db_mod.types.RelationalRowsExpression,
};

pub const ArithmeticExpressionParserHooks = struct {
    ptr: *anyopaque,
    parse_operand: *const fn (*anyopaque) anyerror!db_mod.types.RelationalRowsExpression,
    parse_parenthesized_boolean: *const fn (*anyopaque) anyerror!db_mod.types.RelationalRowsExpression,
};

pub const BooleanExpressionParserHooks = struct {
    ptr: *anyopaque,
    parse_operand: *const fn (*anyopaque) anyerror!db_mod.types.RelationalRowsExpression,
};

pub const BooleanRowExpressionParserHooks = struct {
    ptr: *anyopaque,
    parse_expression: *const fn (*anyopaque) anyerror!db_mod.types.RelationalRowsExpression,
    parse_operand: *const fn (*anyopaque) anyerror!db_mod.types.RelationalRowsExpression,
};

pub const CaseExpressionParserHooks = struct {
    ptr: *anyopaque,
    parse_expression: *const fn (*anyopaque) anyerror!db_mod.types.RelationalRowsExpression,
    parse_operand: *const fn (*anyopaque) anyerror!db_mod.types.RelationalRowsExpression,
};

pub const CaseFoldRowExpressionParserHooks = struct {
    ptr: *anyopaque,
    parse_expression: *const fn (*anyopaque) anyerror!db_mod.types.RelationalRowsExpression,
};

pub fn sqlRowClaimForClause(clause: ast.SqlRowClaimClause) db_mod.types.RowClaimRequest {
    return .{
        .mode = clause.mode,
        .wait_policy = clause.wait_policy,
        .skip_locked = clause.wait_policy == .skip_locked,
    };
}

pub fn setSqlRowClaimClause(claim: *db_mod.types.RowClaimRequest, clause: ast.SqlRowClaimClause) void {
    claim.mode = clause.mode;
    claim.wait_policy = clause.wait_policy;
    claim.skip_locked = clause.wait_policy == .skip_locked;
}

pub fn sqlRowClaimModeName(mode: db_mod.types.RowClaimMode) []const u8 {
    return switch (mode) {
        .for_update => "for_update",
        .for_no_key_update => "for_no_key_update",
        .for_share => "for_share",
        .for_key_share => "for_key_share",
    };
}

pub fn sqlRowClaimWaitPolicyName(wait_policy: db_mod.types.RowClaimWaitPolicy) []const u8 {
    return switch (wait_policy) {
        .wait => "wait",
        .nowait => "nowait",
        .skip_locked => "skip_locked",
    };
}

pub fn sqlRowClaimFingerprintName(claim: db_mod.types.RowClaimRequest) []const u8 {
    return switch (claim.mode) {
        .for_update => switch (claim.effectiveWaitPolicy()) {
            .wait => "locked",
            .nowait => "nowait",
            .skip_locked => "skip_locked",
        },
        .for_no_key_update => switch (claim.effectiveWaitPolicy()) {
            .wait => "no_key_update",
            .nowait => "no_key_update_nowait",
            .skip_locked => "no_key_update_skip_locked",
        },
        .for_share => switch (claim.effectiveWaitPolicy()) {
            .wait => "share",
            .nowait => "share_nowait",
            .skip_locked => "share_skip_locked",
        },
        .for_key_share => switch (claim.effectiveWaitPolicy()) {
            .wait => "key_share",
            .nowait => "key_share_nowait",
            .skip_locked => "key_share_skip_locked",
        },
    };
}

pub fn booleanExpressionFromPredicateGroupsAlloc(
    alloc: std.mem.Allocator,
    groups: []db_mod.types.RelationalRowsExpressionPredicateGroup,
) !db_mod.types.RelationalRowsExpression {
    if (groups.len == 0) return error.UnsupportedSqlShape;
    if (groups.len == 1) return try booleanExpressionFromPredicateGroupAlloc(alloc, &groups[0]);

    const operands = try alloc.alloc(db_mod.types.RelationalRowsExpression, groups.len);
    var initialized: usize = 0;
    errdefer {
        for (operands[0..initialized]) |operand| freeExpression(alloc, operand);
        alloc.free(operands);
    }
    for (groups) |*group| {
        operands[initialized] = try booleanExpressionFromPredicateGroupAlloc(alloc, group);
        initialized += 1;
    }
    return .{
        .kind = .bool_or,
        .operands = operands,
    };
}

pub fn booleanExpressionFromPredicateGroupAlloc(
    alloc: std.mem.Allocator,
    group: *db_mod.types.RelationalRowsExpressionPredicateGroup,
) !db_mod.types.RelationalRowsExpression {
    if (group.conditions.len == 0) return try booleanLiteralExpressionAlloc(alloc, true);
    if (group.conditions.len == 1) {
        const conditions = group.conditions;
        const condition = conditions[0];
        @constCast(conditions)[0] = emptyExpressionCondition();
        const expression = try booleanExpressionFromConditionAlloc(alloc, condition);
        group.conditions = &.{};
        alloc.free(conditions);
        return expression;
    }

    const operands = try alloc.alloc(db_mod.types.RelationalRowsExpression, group.conditions.len);
    var initialized: usize = 0;
    errdefer {
        for (operands[0..initialized]) |operand| freeExpression(alloc, operand);
        alloc.free(operands);
    }
    const conditions = group.conditions;
    for (conditions, 0..) |condition, i| {
        @constCast(conditions)[i] = emptyExpressionCondition();
        operands[initialized] = try booleanExpressionFromConditionAlloc(alloc, condition);
        initialized += 1;
    }
    group.conditions = &.{};
    alloc.free(conditions);
    return .{
        .kind = .bool_and,
        .operands = operands,
    };
}

fn emptyExpressionCondition() db_mod.types.RelationalRowsExpressionCondition {
    return .{
        .lhs = .{ .kind = .value },
        .op = .eq,
        .rhs = &.{},
    };
}

pub fn booleanNotExpressionAlloc(
    alloc: std.mem.Allocator,
    operand: db_mod.types.RelationalRowsExpression,
) !db_mod.types.RelationalRowsExpression {
    var operand_transferred = false;
    errdefer if (!operand_transferred) freeExpression(alloc, operand);
    const operands = try alloc.alloc(db_mod.types.RelationalRowsExpression, 1);
    var operands_transferred = false;
    errdefer if (!operands_transferred) alloc.free(operands);
    operands[0] = operand;
    operand_transferred = true;
    operands_transferred = true;
    return .{
        .kind = .bool_not,
        .operands = operands,
    };
}

pub fn booleanExpressionConditionAlloc(
    alloc: std.mem.Allocator,
    expression: db_mod.types.RelationalRowsExpression,
) !db_mod.types.RelationalRowsExpressionCondition {
    var expression_transferred = false;
    errdefer if (!expression_transferred) freeExpression(alloc, expression);
    const rhs = try alloc.alloc(db_mod.types.RelationalRowsExpression, 1);
    var rhs_transferred = false;
    errdefer if (!rhs_transferred) alloc.free(rhs);
    rhs[0] = try booleanLiteralExpressionAlloc(alloc, true);
    var rhs_expression_transferred = false;
    errdefer if (!rhs_expression_transferred) freeExpression(alloc, rhs[0]);
    expression_transferred = true;
    rhs_expression_transferred = true;
    rhs_transferred = true;
    return .{
        .lhs = expression,
        .op = .eq,
        .rhs = rhs,
    };
}

pub fn booleanExpressionFromConditionAlloc(
    alloc: std.mem.Allocator,
    condition: db_mod.types.RelationalRowsExpressionCondition,
) !db_mod.types.RelationalRowsExpression {
    var condition_transferred = false;
    errdefer if (!condition_transferred) freeExpressionCondition(alloc, condition);

    const branches = try alloc.alloc(db_mod.types.RelationalRowsExpressionCaseBranch, 1);
    var branches_transferred = false;
    errdefer if (!branches_transferred) alloc.free(branches);
    const then_expression = try booleanLiteralExpressionAlloc(alloc, true);
    var then_transferred = false;
    errdefer if (!then_transferred) freeExpression(alloc, then_expression);

    const fallback = try alloc.alloc(db_mod.types.RelationalRowsExpression, 1);
    var fallback_transferred = false;
    errdefer if (!fallback_transferred) alloc.free(fallback);
    fallback[0] = try booleanLiteralExpressionAlloc(alloc, false);
    var fallback_expression_transferred = false;
    errdefer if (!fallback_expression_transferred) freeExpression(alloc, fallback[0]);

    branches[0] = .{
        .when = condition,
        .then = then_expression,
    };
    condition_transferred = true;
    then_transferred = true;
    branches_transferred = true;
    fallback_expression_transferred = true;
    fallback_transferred = true;
    return .{
        .kind = .case,
        .case_branches = branches,
        .case_else = fallback,
    };
}

pub fn booleanLiteralExpressionAlloc(
    alloc: std.mem.Allocator,
    value: bool,
) !db_mod.types.RelationalRowsExpression {
    return .{
        .kind = .value,
        .value_json = try alloc.dupe(u8, if (value) "true" else "false"),
    };
}

pub fn appendExpressionConditionGroup(
    alloc: std.mem.Allocator,
    expression_groups: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionPredicateGroup),
    condition: db_mod.types.RelationalRowsExpressionCondition,
) !void {
    var condition_transferred = false;
    errdefer if (!condition_transferred) freeExpressionCondition(alloc, condition);
    const conditions = try alloc.alloc(db_mod.types.RelationalRowsExpressionCondition, 1);
    var conditions_transferred = false;
    errdefer if (!conditions_transferred) alloc.free(conditions);
    conditions[0] = condition;
    condition_transferred = true;
    try expression_groups.append(alloc, .{ .conditions = conditions });
    conditions_transferred = true;
}

pub fn andExpressionPredicateAlternatives(
    alloc: std.mem.Allocator,
    groups: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionPredicateGroup),
    alternatives: []const db_mod.types.RelationalRowsExpressionPredicateGroup,
) !void {
    if (groups.items.len == 0 or alternatives.len == 0) return error.UnsupportedSqlShape;
    var combined = std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionPredicateGroup).empty;
    var combined_transferred = false;
    errdefer {
        if (!combined_transferred) freeExpressionPredicateGroups(alloc, combined.items);
        combined.deinit(alloc);
    }

    for (groups.items) |base| {
        for (alternatives) |alternative| {
            const conditions = try cloneExpressionConditionsConcatAlloc(alloc, base.conditions, alternative.conditions);
            var conditions_transferred = false;
            errdefer if (!conditions_transferred) {
                freeExpressionConditions(alloc, conditions);
                if (conditions.len > 0) alloc.free(conditions);
            };
            try combined.append(alloc, .{ .conditions = conditions });
            conditions_transferred = true;
        }
    }

    freeExpressionPredicateGroups(alloc, groups.items);
    groups.deinit(alloc);
    groups.* = combined;
    combined_transferred = true;
}

pub fn appendExpressionBetweenSymmetricGroups(
    alloc: std.mem.Allocator,
    expression_or_predicates: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionPredicateGroup),
    lhs: db_mod.types.RelationalRowsExpression,
    lower_expression: db_mod.types.RelationalRowsExpression,
    upper_expression: db_mod.types.RelationalRowsExpression,
    negated: bool,
) !void {
    if (!negated) {
        try appendExpressionBetweenGroup(alloc, expression_or_predicates, lhs, .gte, lower_expression, .lte, upper_expression);
        try appendExpressionBetweenGroup(alloc, expression_or_predicates, lhs, .gte, upper_expression, .lte, lower_expression);
    } else {
        try appendExpressionBetweenGroup(alloc, expression_or_predicates, lhs, .lt, lower_expression, .lt, upper_expression);
        try appendExpressionBetweenGroup(alloc, expression_or_predicates, lhs, .gt, lower_expression, .gt, upper_expression);
    }
}

pub fn appendExpressionBetweenGroup(
    alloc: std.mem.Allocator,
    expression_or_predicates: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionPredicateGroup),
    lhs: db_mod.types.RelationalRowsExpression,
    first_op: runtime_schema.RelationalCheckOp,
    first_rhs: db_mod.types.RelationalRowsExpression,
    second_op: runtime_schema.RelationalCheckOp,
    second_rhs: db_mod.types.RelationalRowsExpression,
) !void {
    const conditions = try alloc.alloc(db_mod.types.RelationalRowsExpressionCondition, 2);
    var conditions_transferred = false;
    var first_initialized = false;
    var second_initialized = false;
    errdefer {
        if (!conditions_transferred) {
            if (first_initialized) freeExpressionCondition(alloc, conditions[0]);
            if (second_initialized) freeExpressionCondition(alloc, conditions[1]);
            alloc.free(conditions);
        }
    }

    conditions[0] = try expressionConditionCloneAlloc(alloc, lhs, first_op, first_rhs);
    first_initialized = true;
    conditions[1] = try expressionConditionCloneAlloc(alloc, lhs, second_op, second_rhs);
    second_initialized = true;

    try expression_or_predicates.append(alloc, .{ .conditions = conditions });
    conditions_transferred = true;
}

pub fn expressionConditionCloneAlloc(
    alloc: std.mem.Allocator,
    lhs: db_mod.types.RelationalRowsExpression,
    op: runtime_schema.RelationalCheckOp,
    rhs: db_mod.types.RelationalRowsExpression,
) !db_mod.types.RelationalRowsExpressionCondition {
    const lhs_clone = try cloneExpressionAlloc(alloc, lhs);
    var lhs_transferred = false;
    errdefer if (!lhs_transferred) freeExpression(alloc, lhs_clone);

    const rhs_slice = try alloc.alloc(db_mod.types.RelationalRowsExpression, 1);
    var rhs_slice_transferred = false;
    var rhs_initialized = false;
    errdefer {
        if (!rhs_slice_transferred) {
            if (rhs_initialized) freeExpression(alloc, rhs_slice[0]);
            alloc.free(rhs_slice);
        }
    }
    rhs_slice[0] = try cloneExpressionAlloc(alloc, rhs);
    rhs_initialized = true;

    lhs_transferred = true;
    rhs_slice_transferred = true;
    return .{
        .lhs = lhs_clone,
        .op = op,
        .rhs = rhs_slice,
    };
}

pub fn appendBooleanConstantExpressionCondition(
    alloc: std.mem.Allocator,
    expression_predicates: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionCondition),
    enabled: bool,
) !void {
    const condition = try booleanConstantExpressionConditionAlloc(alloc, enabled);
    var condition_transferred = false;
    errdefer if (!condition_transferred) freeExpressionCondition(alloc, condition);
    try expression_predicates.append(alloc, condition);
    condition_transferred = true;
}

pub fn appendBooleanConstantExpressionGroup(
    alloc: std.mem.Allocator,
    expression_groups: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionPredicateGroup),
    enabled: bool,
) !void {
    if (enabled) {
        try expression_groups.append(alloc, .{ .conditions = &.{} });
        return;
    }
    const conditions = try alloc.alloc(db_mod.types.RelationalRowsExpressionCondition, 1);
    var conditions_transferred = false;
    errdefer if (!conditions_transferred) alloc.free(conditions);
    conditions[0] = try booleanConstantExpressionConditionAlloc(alloc, false);
    var condition_transferred = false;
    errdefer if (!condition_transferred) freeExpressionCondition(alloc, conditions[0]);
    try expression_groups.append(alloc, .{ .conditions = conditions });
    condition_transferred = true;
    conditions_transferred = true;
}

pub fn booleanConstantExpressionConditionAlloc(
    alloc: std.mem.Allocator,
    enabled: bool,
) !db_mod.types.RelationalRowsExpressionCondition {
    const lhs_json = try alloc.dupe(u8, "true");
    var lhs_transferred = false;
    errdefer if (!lhs_transferred) alloc.free(lhs_json);
    const rhs_json = try alloc.dupe(u8, if (enabled) "true" else "false");
    var rhs_json_transferred = false;
    errdefer if (!rhs_json_transferred) alloc.free(rhs_json);
    const rhs = try alloc.alloc(db_mod.types.RelationalRowsExpression, 1);
    var rhs_transferred = false;
    errdefer if (!rhs_transferred) alloc.free(rhs);
    rhs[0] = .{
        .kind = .value,
        .value_json = rhs_json,
    };
    lhs_transferred = true;
    rhs_json_transferred = true;
    rhs_transferred = true;
    return .{
        .lhs = .{
            .kind = .value,
            .value_json = lhs_json,
        },
        .op = .eq,
        .rhs = rhs,
    };
}

pub fn expressionBooleanComparisonConditionAlloc(
    alloc: std.mem.Allocator,
    lhs: db_mod.types.RelationalRowsExpression,
    op: runtime_schema.RelationalCheckOp,
    value: bool,
) !db_mod.types.RelationalRowsExpressionCondition {
    const rhs_json = try alloc.dupe(u8, if (value) "true" else "false");
    var rhs_json_transferred = false;
    errdefer if (!rhs_json_transferred) alloc.free(rhs_json);
    const rhs = try alloc.alloc(db_mod.types.RelationalRowsExpression, 1);
    var rhs_transferred = false;
    errdefer if (!rhs_transferred) alloc.free(rhs);
    rhs[0] = .{
        .kind = .value,
        .value_json = rhs_json,
    };
    rhs_json_transferred = true;
    rhs_transferred = true;
    return .{
        .lhs = lhs,
        .op = op,
        .rhs = rhs,
    };
}

pub fn expressionNullTestCondition(
    lhs: db_mod.types.RelationalRowsExpression,
    op: runtime_schema.RelationalCheckOp,
) db_mod.types.RelationalRowsExpressionCondition {
    return .{
        .lhs = lhs,
        .op = op,
        .rhs = &.{},
    };
}

pub fn expressionFieldBooleanConditionAlloc(
    alloc: std.mem.Allocator,
    field: []const u8,
    op: runtime_schema.RelationalCheckOp,
    value: bool,
) !db_mod.types.RelationalRowsExpressionCondition {
    const lhs_field = try alloc.dupe(u8, field);
    var lhs_transferred = false;
    errdefer if (!lhs_transferred) alloc.free(lhs_field);
    const lhs: db_mod.types.RelationalRowsExpression = .{
        .kind = .field,
        .field = lhs_field,
    };
    const condition = try expressionBooleanComparisonConditionAlloc(alloc, lhs, op, value);
    lhs_transferred = true;
    return condition;
}

pub fn expressionFieldNullConditionAlloc(
    alloc: std.mem.Allocator,
    field: []const u8,
    op: runtime_schema.RelationalCheckOp,
) !db_mod.types.RelationalRowsExpressionCondition {
    const lhs_field = try alloc.dupe(u8, field);
    var lhs_transferred = false;
    errdefer if (!lhs_transferred) alloc.free(lhs_field);
    lhs_transferred = true;
    return expressionNullTestCondition(.{
        .kind = .field,
        .field = lhs_field,
    }, op);
}

pub fn appendBooleanIsNotExpressionGroups(
    alloc: std.mem.Allocator,
    expression_groups: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionPredicateGroup),
    field: []const u8,
    value: bool,
) !void {
    const ne_condition = try expressionFieldBooleanConditionAlloc(alloc, field, .ne, value);
    try appendExpressionConditionGroup(alloc, expression_groups, ne_condition);
    const null_condition = try expressionFieldNullConditionAlloc(alloc, field, .is_null);
    try appendExpressionConditionGroup(alloc, expression_groups, null_condition);
}

pub fn appendExpressionBooleanIsNotGroups(
    alloc: std.mem.Allocator,
    expression_groups: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionPredicateGroup),
    lhs: db_mod.types.RelationalRowsExpression,
    value: bool,
) !void {
    const ne_lhs = try cloneExpressionAlloc(alloc, lhs);
    var ne_lhs_transferred = false;
    errdefer if (!ne_lhs_transferred) freeExpression(alloc, ne_lhs);
    const null_lhs = try cloneExpressionAlloc(alloc, lhs);
    var null_lhs_transferred = false;
    errdefer if (!null_lhs_transferred) freeExpression(alloc, null_lhs);

    const ne_conditions = try alloc.alloc(db_mod.types.RelationalRowsExpressionCondition, 1);
    var ne_conditions_transferred = false;
    errdefer if (!ne_conditions_transferred) alloc.free(ne_conditions);
    ne_conditions[0] = try expressionBooleanComparisonConditionAlloc(alloc, ne_lhs, .ne, value);
    var ne_condition_transferred = false;
    errdefer if (!ne_condition_transferred) freeExpressionCondition(alloc, ne_conditions[0]);
    ne_lhs_transferred = true;

    const null_conditions = try alloc.alloc(db_mod.types.RelationalRowsExpressionCondition, 1);
    var null_conditions_transferred = false;
    errdefer if (!null_conditions_transferred) alloc.free(null_conditions);
    null_conditions[0] = expressionNullTestCondition(null_lhs, .is_null);
    var null_condition_transferred = false;
    errdefer if (!null_condition_transferred) freeExpressionCondition(alloc, null_conditions[0]);
    null_lhs_transferred = true;

    try expression_groups.append(alloc, .{ .conditions = ne_conditions });
    ne_condition_transferred = true;
    ne_conditions_transferred = true;
    try expression_groups.append(alloc, .{ .conditions = null_conditions });
    null_condition_transferred = true;
    null_conditions_transferred = true;
}

pub fn expandScalarValuesJsonIntoOrBranches(
    alloc: std.mem.Allocator,
    branches: *std.ArrayListUnmanaged(ScalarOrCheckBranch),
    field: []const u8,
    values_json: []const u8,
    op: runtime_schema.RelationalCheckOp,
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
            try appendScalarJsonValueCheckToBranch(alloc, &expanded_branch, field, value, op);

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
) !void {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, values_json, .{}) catch return error.UnsupportedSqlShape;
    defer parsed.deinit();
    if (parsed.value != .array or parsed.value.array.items.len == 0 or parsed.value.array.items.len > max_scalar_or_expanded_branches) return error.UnsupportedSqlShape;
    if (branches.items.len == 0) return error.UnsupportedSqlShape;

    for (branches.items) |*branch| {
        for (parsed.value.array.items) |value| {
            if (op == .ne and value == .null) return error.UnsupportedSqlShape;
            try appendScalarJsonValueCheckToBranch(alloc, branch, field, value, op);
        }
    }
}

pub fn appendScalarJsonValueCheckToBranch(
    alloc: std.mem.Allocator,
    branch: *ScalarOrCheckBranch,
    field: []const u8,
    value: std.json.Value,
    op: runtime_schema.RelationalCheckOp,
) !void {
    const value_json = try std.json.Stringify.valueAlloc(alloc, value, .{});
    var value_transferred = false;
    errdefer if (!value_transferred) alloc.free(value_json);
    const owned_field = try alloc.dupe(u8, field);
    var field_transferred = false;
    errdefer if (!field_transferred) alloc.free(owned_field);
    try branch.append(alloc, .{
        .name = "",
        .field = owned_field,
        .op = op,
        .value_json = value_json,
    });
    value_transferred = true;
    field_transferred = true;
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
) !runtime_schema.RelationalCheck {
    const owned_field = try alloc.dupe(u8, field);
    var field_transferred = false;
    errdefer if (!field_transferred) alloc.free(owned_field);
    const owned_value = try alloc.dupe(u8, value_json);
    var value_transferred = false;
    errdefer if (!value_transferred) alloc.free(owned_value);

    field_transferred = true;
    value_transferred = true;
    return .{
        .name = "",
        .field = owned_field,
        .op = op,
        .value_json = owned_value,
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

    checks[0] = try scalarCheckCloneAlloc(alloc, field, first_op, first_value_json);
    first_initialized = true;
    checks[1] = try scalarCheckCloneAlloc(alloc, field, second_op, second_value_json);
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
            try appendBetweenScalarGroup(alloc, or_predicates, field, .gte, lower_json, .lte, upper_json);
            try appendBetweenScalarGroup(alloc, or_predicates, field, .gte, upper_json, .lte, lower_json);
        } else {
            try appendBetweenScalarGroup(alloc, or_predicates, field, .lt, lower_json, .lt, upper_json);
            try appendBetweenScalarGroup(alloc, or_predicates, field, .gt, lower_json, .gt, upper_json);
        }
        return;
    }

    if (!negated) {
        const lower = try scalarCheckCloneAlloc(alloc, field, .gte, lower_json);
        var lower_transferred = false;
        errdefer if (!lower_transferred) plan_mod.freeRelationalCheck(alloc, lower);
        try predicates.append(alloc, lower);
        lower_transferred = true;

        const upper = try scalarCheckCloneAlloc(alloc, field, .lte, upper_json);
        var upper_transferred = false;
        errdefer if (!upper_transferred) plan_mod.freeRelationalCheck(alloc, upper);
        try predicates.append(alloc, upper);
        upper_transferred = true;
        return;
    }

    const lower_group = try alloc.alloc(runtime_schema.RelationalCheck, 1);
    var lower_group_transferred = false;
    errdefer if (!lower_group_transferred) alloc.free(lower_group);
    lower_group[0] = try scalarCheckCloneAlloc(alloc, field, .lt, lower_json);
    var lower_initialized = true;
    errdefer if (!lower_group_transferred and lower_initialized) plan_mod.freeRelationalCheck(alloc, lower_group[0]);

    const upper_group = try alloc.alloc(runtime_schema.RelationalCheck, 1);
    var upper_group_transferred = false;
    errdefer if (!upper_group_transferred) alloc.free(upper_group);
    upper_group[0] = try scalarCheckCloneAlloc(alloc, field, .gt, upper_json);
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

        group[0] = .{
            .name = "",
            .field = owned_field,
            .op = op,
            .value_json = value_json,
        };
        group_initialized = true;
        field_transferred = true;
        value_transferred = true;

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
        try predicates.append(alloc, .{
            .name = "",
            .field = owned_field,
            .op = .eq,
            .value_json = value_json,
        });
        field_transferred = true;
        value_transferred = true;
    }
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
    try source_predicates.append(alloc, .{
        .name = "",
        .field = predicate_field,
        .op = op,
        .value_json = value_json,
    });
    field_transferred = true;
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
    try branch.append(alloc, .{ .field = field, .values_json = values_json, .negated = predicate.negated });
    field_transferred = true;
    values_transferred = true;
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

pub fn sqlKeywordIsAnyOrSome(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "any") or std.ascii.eqlIgnoreCase(text, "some");
}

pub fn sqlKeywordStartsScalarPredicate(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "is") or
        std.ascii.eqlIgnoreCase(text, "isnull") or
        std.ascii.eqlIgnoreCase(text, "in") or
        std.ascii.eqlIgnoreCase(text, "between") or
        std.ascii.eqlIgnoreCase(text, "like") or
        std.ascii.eqlIgnoreCase(text, "ilike") or
        std.ascii.eqlIgnoreCase(text, "notnull") or
        sqlKeywordIsAnyOrSome(text) or
        std.ascii.eqlIgnoreCase(text, "all");
}

pub fn peekExtensionFunctionCall(tokens: []const Token, pos: usize, bindings: []const ExtensionFunctionBinding) bool {
    if (bindings.len == 0) return false;
    if (pos + 1 >= tokens.len or tokens[pos].kind != .identifier or tokens[pos + 1].kind != .lparen) return false;
    for (bindings) |binding| {
        if (std.ascii.eqlIgnoreCase(binding.sql_name, tokens[pos].text)) return true;
    }
    return false;
}

pub fn extensionFunctionBinding(bindings: []const ExtensionFunctionBinding, name: []const u8) !?ExtensionFunctionBinding {
    var found: ?ExtensionFunctionBinding = null;
    for (bindings) |binding| {
        if (!std.ascii.eqlIgnoreCase(binding.sql_name, name)) continue;
        if (found != null) return error.UnsupportedSqlShape;
        found = binding;
    }
    return found;
}

pub fn parseExtensionFunctionRowExpressionOrNullAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    bindings: []const ExtensionFunctionBinding,
    hooks: ExtensionFunctionRowExpressionParserHooks,
) !?db_mod.types.RelationalRowsExpression {
    if (!peekExtensionFunctionCall(tokens, pos.*, bindings)) return null;
    const name = tokens[pos.*].text;
    const binding = try extensionFunctionBinding(bindings, name) orelse return null;
    const kind = binding.native_expression_kind;
    _ = parser.matchToken(tokens, pos, .identifier) orelse return error.UnsupportedSqlShape;
    try parser.expectToken(tokens, pos, .lparen);
    var operands = std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpression).empty;
    errdefer {
        for (operands.items) |operand| freeExpression(alloc, operand);
        operands.deinit(alloc);
    }
    if (parser.matchToken(tokens, pos, .rparen) == null) {
        while (true) {
            const operand = try hooks.parse_operand(hooks.ptr);
            var operand_transferred = false;
            errdefer if (!operand_transferred) freeExpression(alloc, operand);
            try operands.append(alloc, operand);
            operand_transferred = true;
            if (parser.matchToken(tokens, pos, .comma) == null) break;
        }
        try parser.expectToken(tokens, pos, .rparen);
    }
    try validateExtensionFunctionArity(kind, binding.arity, operands.items.len);
    if (kind == .uuid_v4) return .{ .kind = .uuid_v4 };
    return try buildFunctionExpressionFromOperandListAlloc(alloc, kind, &operands);
}

pub fn peekRoutineExpressionCall(tokens: []const Token, pos: usize, bindings: []const RoutineExpressionBinding) bool {
    if (bindings.len == 0) return false;
    if (pos + 1 >= tokens.len or tokens[pos].kind != .identifier or tokens[pos + 1].kind != .lparen) return false;
    for (bindings) |binding| {
        if (std.ascii.eqlIgnoreCase(binding.sql_name, tokens[pos].text)) return true;
    }
    return false;
}

pub fn routineExpressionBinding(
    bindings: []const RoutineExpressionBinding,
    name: []const u8,
    arity: usize,
) !?RoutineExpressionBinding {
    var found: ?RoutineExpressionBinding = null;
    for (bindings) |binding| {
        if (!std.ascii.eqlIgnoreCase(binding.sql_name, name)) continue;
        if (@as(usize, binding.arity) != arity) continue;
        if (found != null) return error.UnsupportedSqlShape;
        found = binding;
    }
    return found;
}

pub fn parseRoutineExpressionRowExpressionOrNullAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    bindings: []const RoutineExpressionBinding,
    hooks: RoutineExpressionRowExpressionParserHooks,
) !?db_mod.types.RelationalRowsExpression {
    if (!peekRoutineExpressionCall(tokens, pos.*, bindings)) return null;
    const name = tokens[pos.*].text;
    _ = parser.matchToken(tokens, pos, .identifier) orelse return error.UnsupportedSqlShape;
    try parser.expectToken(tokens, pos, .lparen);
    var operands = std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpression).empty;
    defer {
        for (operands.items) |operand| freeExpression(alloc, operand);
        operands.deinit(alloc);
    }
    if (parser.matchToken(tokens, pos, .rparen) == null) {
        while (true) {
            const operand = try hooks.parse_operand(hooks.ptr);
            var operand_transferred = false;
            errdefer if (!operand_transferred) freeExpression(alloc, operand);
            try operands.append(alloc, operand);
            operand_transferred = true;
            if (parser.matchToken(tokens, pos, .comma) != null) continue;
            break;
        }
        try parser.expectToken(tokens, pos, .rparen);
    }
    const binding = try routineExpressionBinding(bindings, name, operands.items.len) orelse return error.UnsupportedSqlShape;
    if (binding.null_input == .returns_null) {
        for (operands.items) |operand| {
            if (routineArgumentExpressionIsNullLiteral(operand)) {
                return .{
                    .kind = .value,
                    .value_json = try alloc.dupe(u8, "null"),
                };
            }
        }
        return error.UnsupportedSqlShape;
    }

    return try cloneExpressionSubstitutingRoutineArgsAlloc(alloc, binding.expression, operands.items);
}

pub fn routineArgumentExpressionIsNullLiteral(value: runtime_schema.RelationalRowsExpression) bool {
    return value.kind == .value and std.mem.eql(u8, value.value_json, "null");
}

pub fn parseParenthesizedRowExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    hooks: ParenthesizedRowExpressionParserHooks,
) !db_mod.types.RelationalRowsExpression {
    try parser.expectToken(tokens, pos, .lparen);
    const expression = try hooks.parse_expression(hooks.ptr);
    var expression_transferred = false;
    errdefer if (!expression_transferred) freeExpression(alloc, expression);
    try parser.expectToken(tokens, pos, .rparen);
    expression_transferred = true;
    return expression;
}

pub fn parseBooleanNotRowExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    type_context: RowExpressionTypeContext,
    hooks: BooleanNotRowExpressionParserHooks,
) !db_mod.types.RelationalRowsExpression {
    try parseBooleanNotExpressionStart(tokens, pos);
    const operand = if (peekParenthesizedExpressionSyntax(tokens, pos.*))
        try hooks.parse_parenthesized_operand(hooks.ptr)
    else
        try hooks.parse_operand(hooks.ptr);
    var operand_transferred = false;
    errdefer if (!operand_transferred) freeExpression(alloc, operand);
    try type_context.validateBooleanRowExpression(operand);
    const expression = try wrapBooleanNotExpressionAlloc(alloc, operand);
    operand_transferred = true;
    return expression;
}

pub fn parseUnaryNegativeRowExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    hooks: UnaryNegativeRowExpressionParserHooks,
) !db_mod.types.RelationalRowsExpression {
    _ = parser.matchToken(tokens, pos, .minus) orelse return error.UnsupportedSqlShape;
    if (parser.matchToken(tokens, pos, .number)) |token| {
        return .{
            .kind = .value,
            .value_json = try std.fmt.allocPrint(alloc, "-{s}", .{token.text}),
        };
    }

    const operand = try hooks.parse_operand(hooks.ptr);
    var operand_transferred = false;
    errdefer if (!operand_transferred) freeExpression(alloc, operand);
    const expression = try buildUnaryNegativeExpressionAlloc(alloc, operand);
    operand_transferred = true;
    return expression;
}

pub fn parseCastRowExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    hooks: CastRowExpressionParserHooks,
) !db_mod.types.RelationalRowsExpression {
    try parseCastExpressionCallStart(tokens, pos);
    const operand = try hooks.parse_operand(hooks.ptr);
    var operand_transferred = false;
    errdefer if (!operand_transferred) freeExpression(alloc, operand);
    try parseCastExpressionAs(tokens, pos);
    const cast_type = try parseExpressionCastType(tokens, pos);
    try parser.expectToken(tokens, pos, .rparen);
    const expression = try buildCastExpressionAlloc(alloc, operand, cast_type);
    operand_transferred = true;
    return expression;
}

pub fn parseCoalesceRowExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    type_context: RowExpressionTypeContext,
    hooks: CoalesceRowExpressionParserHooks,
) !db_mod.types.RelationalRowsExpression {
    try parseCoalesceFunctionCallStart(tokens, pos);

    var operands = std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpression).empty;
    errdefer {
        for (operands.items) |operand| freeExpression(alloc, operand);
        operands.deinit(alloc);
    }
    while (true) {
        const operand = try hooks.parse_expression(hooks.ptr);
        var operand_transferred = false;
        errdefer if (!operand_transferred) freeExpression(alloc, operand);
        try operands.append(alloc, operand);
        operand_transferred = true;
        if (parser.matchToken(tokens, pos, .comma) == null) break;
    }
    if (operands.items.len == 0) return error.UnsupportedSqlShape;
    try parser.expectToken(tokens, pos, .rparen);

    const expression = try buildFunctionExpressionFromOperandListAlloc(alloc, .coalesce, &operands);
    errdefer freeExpression(alloc, expression);
    try type_context.validateExpressionOperandDomains(expression);
    return expression;
}

pub fn parseCoalesceProjectionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    schema: runtime_schema.TableSchema,
    field_expression_qualifiers: []const []const u8,
    returning_expression_qualifiers: []const []const u8,
    defer_row_expression_field_validation: bool,
    hooks: CoalesceProjectionParserHooks,
) !db_mod.types.RelationalRowsCoalesceProjection {
    try parseCoalesceFunctionCallStart(tokens, pos);
    var operands = std.ArrayListUnmanaged(db_mod.types.RelationalRowsCoalesceOperand).empty;
    errdefer {
        for (operands.items) |operand| plan_mod.freeCoalesceOperand(alloc, operand);
        operands.deinit(alloc);
    }
    while (true) {
        const operand = try parseCoalesceOperandAlloc(
            alloc,
            tokens,
            pos,
            schema,
            field_expression_qualifiers,
            returning_expression_qualifiers,
            defer_row_expression_field_validation,
            hooks,
        );
        var operand_transferred = false;
        errdefer if (!operand_transferred) plan_mod.freeCoalesceOperand(alloc, operand);
        try operands.append(alloc, operand);
        operand_transferred = true;
        if (parser.matchToken(tokens, pos, .comma) == null) break;
    }
    if (operands.items.len == 0) return error.UnsupportedSqlShape;
    try parser.expectToken(tokens, pos, .rparen);
    const output = try grammar.parseProjectionOutputOwnedAlloc(alloc, tokens, pos, "coalesce");
    var output_transferred = false;
    errdefer if (!output_transferred) alloc.free(output);
    const projection = try plan_mod.buildCoalesceProjectionFromOperandListAlloc(alloc, output, &operands);
    output_transferred = true;
    return projection;
}

pub fn parseCoalesceOperandAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    schema: runtime_schema.TableSchema,
    field_expression_qualifiers: []const []const u8,
    returning_expression_qualifiers: []const []const u8,
    defer_row_expression_field_validation: bool,
    hooks: CoalesceProjectionParserHooks,
) !db_mod.types.RelationalRowsCoalesceOperand {
    if (try parseCoalesceFieldOperandOrNullOwnedAlloc(
        alloc,
        tokens,
        pos,
        schema,
        field_expression_qualifiers,
        returning_expression_qualifiers,
        defer_row_expression_field_validation,
    )) |operand| return operand;

    const value_json = try hooks.parse_value_json(hooks.ptr);
    errdefer alloc.free(value_json);
    return .{ .kind = .value, .value_json = value_json };
}

pub fn parseParenthesizedNullTestExpressionProjectionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    schema: runtime_schema.TableSchema,
    field_expression_qualifiers: []const []const u8,
    returning_expression_qualifiers: []const []const u8,
    defer_row_expression_field_validation: bool,
    field_source: db_mod.types.RelationalRowsExpressionFieldSource,
) !db_mod.types.RelationalRowsExpressionProjection {
    try parser.expectToken(tokens, pos, .lparen);
    const field = try parseRowExpressionFieldOwnedAlloc(
        alloc,
        tokens,
        pos,
        schema,
        field_expression_qualifiers,
        returning_expression_qualifiers,
        defer_row_expression_field_validation,
    );
    var field_transferred = false;
    errdefer if (!field_transferred) alloc.free(field);
    if (binder.relationalColumnForField(schema, field, null) == null) return error.InvalidSqlCatalog;
    try parser.expectKeyword(tokens, pos, "is");
    const op: runtime_schema.RelationalCheckOp = if (parser.matchKeyword(tokens, pos, "not")) blk: {
        try parser.expectKeyword(tokens, pos, "null");
        break :blk .is_not_null;
    } else blk: {
        try parser.expectKeyword(tokens, pos, "null");
        break :blk .is_null;
    };
    try parser.expectToken(tokens, pos, .rparen);

    const output = try grammar.parseProjectionOutputOwnedAlloc(alloc, tokens, pos, if (op == .is_not_null) "is_not_null" else "is_null");
    var output_transferred = false;
    errdefer if (!output_transferred) alloc.free(output);

    const true_json = try alloc.dupe(u8, "true");
    var true_transferred = false;
    errdefer if (!true_transferred) alloc.free(true_json);
    const false_json = try alloc.dupe(u8, "false");
    var false_transferred = false;
    errdefer if (!false_transferred) alloc.free(false_json);
    const branches = try alloc.alloc(db_mod.types.RelationalRowsExpressionCaseBranch, 1);
    var branches_transferred = false;
    errdefer if (!branches_transferred) alloc.free(branches);
    const fallback = try alloc.alloc(db_mod.types.RelationalRowsExpression, 1);
    var fallback_transferred = false;
    errdefer if (!fallback_transferred) alloc.free(fallback);

    branches[0] = .{
        .when = .{
            .lhs = .{ .kind = .field, .field = field, .field_source = field_source },
            .op = op,
        },
        .then = .{ .kind = .value, .value_json = true_json },
    };
    fallback[0] = .{ .kind = .value, .value_json = false_json };
    field_transferred = true;
    true_transferred = true;
    false_transferred = true;
    branches_transferred = true;
    fallback_transferred = true;

    const expression: db_mod.types.RelationalRowsExpression = .{
        .kind = .case,
        .case_branches = branches,
        .case_else = fallback,
    };
    errdefer freeExpression(alloc, expression);
    output_transferred = true;
    return buildExpressionProjection(output, expression);
}

pub fn buildExpressionProjectionFromOwnedExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    expression: db_mod.types.RelationalRowsExpression,
    default_output: []const u8,
) !db_mod.types.RelationalRowsExpressionProjection {
    var expression_transferred = false;
    errdefer if (!expression_transferred) freeExpression(alloc, expression);
    const output = try grammar.parseProjectionOutputOwnedAlloc(alloc, tokens, pos, default_output);
    var output_transferred = false;
    errdefer if (!output_transferred) alloc.free(output);
    const projection = buildExpressionProjection(output, expression);
    expression_transferred = true;
    output_transferred = true;
    return projection;
}

pub fn buildOpExpressionProjectionFromOwnedExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    expression: db_mod.types.RelationalRowsExpression,
) !db_mod.types.RelationalRowsExpressionProjection {
    return try buildExpressionProjectionFromOwnedExpressionAlloc(alloc, tokens, pos, expression, rowExpressionOpName(expression.kind));
}

pub fn buildDefaultExpressionProjectionFromOwnedExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    expression: db_mod.types.RelationalRowsExpression,
) !db_mod.types.RelationalRowsExpressionProjection {
    return try buildExpressionProjectionFromOwnedExpressionAlloc(alloc, tokens, pos, expression, rowExpressionDefaultOutputName(expression.kind));
}

pub fn buildTextExpressionProjectionFromOwnedExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    type_context: RowExpressionTypeContext,
    expression: db_mod.types.RelationalRowsExpression,
) !db_mod.types.RelationalRowsExpressionProjection {
    var expression_transferred = false;
    errdefer if (!expression_transferred) freeExpression(alloc, expression);
    try type_context.validateTextRowExpression(expression);
    const default_output = if (expression.kind == .field and expression.field.len != 0)
        expression.field
    else
        rowExpressionOpName(expression.kind);
    expression_transferred = true;
    const projection = try buildExpressionProjectionFromOwnedExpressionAlloc(alloc, tokens, pos, expression, default_output);
    return projection;
}

pub fn buildNumericExpressionProjectionFromOwnedExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    type_context: RowExpressionTypeContext,
    expression: db_mod.types.RelationalRowsExpression,
) !db_mod.types.RelationalRowsExpressionProjection {
    var expression_transferred = false;
    errdefer if (!expression_transferred) freeExpression(alloc, expression);
    try type_context.validateNumericRowExpression(expression);
    expression_transferred = true;
    const projection = try buildOpExpressionProjectionFromOwnedExpressionAlloc(alloc, tokens, pos, expression);
    return projection;
}

pub fn buildBooleanExpressionProjectionFromOwnedExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    type_context: RowExpressionTypeContext,
    expression: db_mod.types.RelationalRowsExpression,
) !db_mod.types.RelationalRowsExpressionProjection {
    var expression_transferred = false;
    errdefer if (!expression_transferred) freeExpression(alloc, expression);
    try type_context.validateBooleanRowExpression(expression);
    expression_transferred = true;
    const projection = try buildDefaultExpressionProjectionFromOwnedExpressionAlloc(alloc, tokens, pos, expression);
    return projection;
}

pub fn parseTextExpressionProjectionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    type_context: RowExpressionTypeContext,
    hooks: ExpressionProjectionParserHooks,
) !db_mod.types.RelationalRowsExpressionProjection {
    const expression = try hooks.parse_expression(hooks.ptr);
    return try buildTextExpressionProjectionFromOwnedExpressionAlloc(alloc, tokens, pos, type_context, expression);
}

pub fn parseGenericExpressionProjectionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    type_context: RowExpressionTypeContext,
    hooks: ExpressionProjectionParserHooks,
) !db_mod.types.RelationalRowsExpressionProjection {
    const expression = try hooks.parse_expression(hooks.ptr);
    return try buildNumericExpressionProjectionFromOwnedExpressionAlloc(alloc, tokens, pos, type_context, expression);
}

pub fn parseBooleanExpressionProjectionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    type_context: RowExpressionTypeContext,
    hooks: ExpressionProjectionParserHooks,
) !db_mod.types.RelationalRowsExpressionProjection {
    const expression = try hooks.parse_boolean_expression(hooks.ptr);
    return try buildBooleanExpressionProjectionFromOwnedExpressionAlloc(alloc, tokens, pos, type_context, expression);
}

pub fn parseParenthesizedExpressionProjectionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    hooks: ExpressionProjectionParserHooks,
) !db_mod.types.RelationalRowsExpressionProjection {
    try parser.expectToken(tokens, pos, .lparen);
    const expression = try hooks.parse_boolean_expression(hooks.ptr);
    errdefer freeExpression(alloc, expression);
    try parser.expectToken(tokens, pos, .rparen);
    return try buildDefaultExpressionProjectionFromOwnedExpressionAlloc(alloc, tokens, pos, expression);
}

pub fn parseNowExpressionProjectionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !db_mod.types.RelationalRowsExpressionProjection {
    const expression = try parseSqlNowRowExpressionAlloc(alloc, tokens, pos);
    return try buildExpressionProjectionFromOwnedExpressionAlloc(alloc, tokens, pos, expression, "now");
}

pub fn parseCurrentDateExpressionProjectionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !db_mod.types.RelationalRowsExpressionProjection {
    const expression = try parseSqlCurrentDateRowExpressionAlloc(alloc, tokens, pos);
    return try buildExpressionProjectionFromOwnedExpressionAlloc(alloc, tokens, pos, expression, "current_date");
}

pub fn parseTypedDatetimeLiteralExpressionProjectionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !db_mod.types.RelationalRowsExpressionProjection {
    const expression = try parseSqlTypedDatetimeLiteralRowExpressionAlloc(alloc, tokens, pos);
    return try buildExpressionProjectionFromOwnedExpressionAlloc(alloc, tokens, pos, expression, "datetime_literal");
}

pub fn parseUuidV4ExpressionProjectionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !db_mod.types.RelationalRowsExpressionProjection {
    const expression = try parseSqlUuidV4RowExpression(tokens, pos);
    return try buildExpressionProjectionFromOwnedExpressionAlloc(alloc, tokens, pos, expression, "gen_random_uuid");
}

pub fn parseFixedOutputExpressionProjectionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    default_output: []const u8,
    hooks: ExpressionProjectionParserHooks,
) !db_mod.types.RelationalRowsExpressionProjection {
    const expression = try hooks.parse_expression(hooks.ptr);
    return try buildExpressionProjectionFromOwnedExpressionAlloc(alloc, tokens, pos, expression, default_output);
}

pub fn parseOpOutputExpressionProjectionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    hooks: ExpressionProjectionParserHooks,
) !db_mod.types.RelationalRowsExpressionProjection {
    const expression = try hooks.parse_expression(hooks.ptr);
    return try buildOpExpressionProjectionFromOwnedExpressionAlloc(alloc, tokens, pos, expression);
}

pub fn parseDefaultOutputExpressionProjectionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    hooks: ExpressionProjectionParserHooks,
) !db_mod.types.RelationalRowsExpressionProjection {
    const expression = try hooks.parse_expression(hooks.ptr);
    return try buildDefaultExpressionProjectionFromOwnedExpressionAlloc(alloc, tokens, pos, expression);
}

pub fn parseRegexpMatchExpressionProjectionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    hooks: ExpressionProjectionParserHooks,
) !db_mod.types.RelationalRowsExpressionProjection {
    const default_output = if (parser.peekKeyword(tokens, pos.*, "regexp_like")) "regexp_like" else "regexp_match";
    const expression = try hooks.parse_expression(hooks.ptr);
    return try buildExpressionProjectionFromOwnedExpressionAlloc(alloc, tokens, pos, expression, default_output);
}

pub fn parseJsonExtractPathExpressionProjectionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    hooks: ExpressionProjectionParserHooks,
) !db_mod.types.RelationalRowsExpressionProjection {
    const expression = try hooks.parse_expression(hooks.ptr);
    const default_output = if (expression.json_as_text) "json_extract_path_text" else "json_extract_path";
    return try buildExpressionProjectionFromOwnedExpressionAlloc(
        alloc,
        tokens,
        pos,
        expression,
        default_output,
    );
}

pub fn parseExtensionFunctionExpressionProjectionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    bindings: []const ExtensionFunctionBinding,
    hooks: ExtensionFunctionRowExpressionParserHooks,
) !db_mod.types.RelationalRowsExpressionProjection {
    const expression = (try parseExtensionFunctionRowExpressionOrNullAlloc(alloc, tokens, pos, bindings, hooks)) orelse return error.UnsupportedSqlShape;
    return try buildOpExpressionProjectionFromOwnedExpressionAlloc(alloc, tokens, pos, expression);
}

pub fn parseRoutineExpressionProjectionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    bindings: []const RoutineExpressionBinding,
    hooks: RoutineExpressionRowExpressionParserHooks,
) !db_mod.types.RelationalRowsExpressionProjection {
    const expression = (try parseRoutineExpressionRowExpressionOrNullAlloc(alloc, tokens, pos, bindings, hooks)) orelse return error.UnsupportedSqlShape;
    return try buildOpExpressionProjectionFromOwnedExpressionAlloc(alloc, tokens, pos, expression);
}

pub fn parseJsonValueExpressionProjectionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    default_output: []const u8,
    hooks: JsonValueExpressionProjectionParserHooks,
) !db_mod.types.RelationalRowsExpressionProjection {
    const value_json = try hooks.parse_value_json(hooks.ptr);
    const expression: db_mod.types.RelationalRowsExpression = .{
        .kind = .value,
        .value_json = value_json,
    };
    return try buildExpressionProjectionFromOwnedExpressionAlloc(alloc, tokens, pos, expression, default_output);
}

pub fn parseNullifRowExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    type_context: RowExpressionTypeContext,
    hooks: NullifRowExpressionParserHooks,
) !db_mod.types.RelationalRowsExpression {
    try parseNullifFunctionCallStart(tokens, pos);
    const lhs = try hooks.parse_operand(hooks.ptr);
    var lhs_transferred = false;
    errdefer if (!lhs_transferred) freeExpression(alloc, lhs);
    try parser.expectToken(tokens, pos, .comma);
    const rhs = try hooks.parse_operand(hooks.ptr);
    var rhs_transferred = false;
    errdefer if (!rhs_transferred) freeExpression(alloc, rhs);
    try parser.expectToken(tokens, pos, .rparen);

    const expression = try buildBinaryFunctionExpressionAlloc(alloc, .nullif, lhs, rhs);
    var expression_transferred = false;
    errdefer if (!expression_transferred) freeExpression(alloc, expression);
    lhs_transferred = true;
    rhs_transferred = true;
    try type_context.validateExpressionOperandDomains(expression);
    expression_transferred = true;
    return expression;
}

pub fn parseTextLengthRowExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    type_context: RowExpressionTypeContext,
    hooks: FixedUnaryRowExpressionParserHooks,
) !db_mod.types.RelationalRowsExpression {
    const kind = try parseTextLengthFunctionCallStart(tokens, pos);
    return try parseUnaryRowExpressionCallRestAlloc(alloc, tokens, pos, kind, type_context, .text, hooks);
}

pub fn parseFixedUnaryRowExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    kind: db_mod.types.RelationalRowsExpressionKind,
    type_context: RowExpressionTypeContext,
    input_domain: RowExpressionInputDomain,
    hooks: FixedUnaryRowExpressionParserHooks,
) !db_mod.types.RelationalRowsExpression {
    try parseFixedUnaryFunctionCallStart(tokens, pos, kind);
    return try parseUnaryRowExpressionCallRestAlloc(alloc, tokens, pos, kind, type_context, input_domain, hooks);
}

pub fn parseJsonUnaryRowExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    kind: db_mod.types.RelationalRowsExpressionKind,
    type_context: RowExpressionTypeContext,
    hooks: FixedUnaryRowExpressionParserHooks,
) !db_mod.types.RelationalRowsExpression {
    switch (kind) {
        .json_array_length => try parseJsonArrayLengthFunctionCallStart(tokens, pos),
        .json_typeof => try parseJsonTypeofFunctionCallStart(tokens, pos),
        else => return error.UnsupportedSqlShape,
    }
    return try parseUnaryRowExpressionCallRestAlloc(alloc, tokens, pos, kind, type_context, .json, hooks);
}

pub fn parseJsonExtractPathRowExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    params: []const value_mod.SqlValue,
    type_context: RowExpressionTypeContext,
    hooks: FixedUnaryRowExpressionParserHooks,
) !db_mod.types.RelationalRowsExpression {
    const keyword = matchFunctionKeywordText(tokens, pos, sqlKeywordIsJsonExtractPathFunction) orelse return error.UnsupportedSqlShape;
    const as_text = sqlJsonExtractPathFunctionAsText(keyword);
    try parser.expectToken(tokens, pos, .lparen);
    const operand = try hooks.parse_expression(hooks.ptr);
    var operand_transferred = false;
    errdefer if (!operand_transferred) freeExpression(alloc, operand);
    try type_context.validateJsonRowExpression(operand);
    const path = try value_mod.parseJsonExtractPathSegmentsAlloc(alloc, tokens, pos, params);
    var path_transferred = false;
    errdefer if (!path_transferred) alloc.free(path);
    try parser.expectToken(tokens, pos, .rparen);

    const expression = try buildJsonExtractExpressionAlloc(alloc, operand, path, as_text);
    operand_transferred = true;
    path_transferred = true;
    return expression;
}

fn parseUnaryRowExpressionCallRestAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    kind: db_mod.types.RelationalRowsExpressionKind,
    type_context: RowExpressionTypeContext,
    input_domain: RowExpressionInputDomain,
    hooks: FixedUnaryRowExpressionParserHooks,
) !db_mod.types.RelationalRowsExpression {
    const operand = try hooks.parse_expression(hooks.ptr);
    var operand_transferred = false;
    errdefer if (!operand_transferred) freeExpression(alloc, operand);
    switch (input_domain) {
        .text => try type_context.validateTextRowExpression(operand),
        .numeric => try type_context.validateNumericRowExpression(operand),
        .json => try type_context.validateJsonRowExpression(operand),
        .any => _ = try type_context.rowExpressionOutputType(operand),
    }
    try parser.expectToken(tokens, pos, .rparen);

    const expression = try buildUnaryFunctionExpressionAlloc(alloc, kind, operand);
    operand_transferred = true;
    return expression;
}

pub fn parseJsonBuildObjectRowExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    type_context: RowExpressionTypeContext,
    hooks: JsonBuildObjectRowExpressionParserHooks,
) !db_mod.types.RelationalRowsExpression {
    try parseJsonBuildObjectFunctionCallStart(tokens, pos);
    var operands = std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpression).empty;
    errdefer {
        for (operands.items) |operand| freeExpression(alloc, operand);
        operands.deinit(alloc);
    }
    if (parser.matchToken(tokens, pos, .rparen) == null) {
        while (true) {
            const key = try hooks.parse_expression(hooks.ptr);
            var key_transferred = false;
            errdefer if (!key_transferred) freeExpression(alloc, key);
            try type_context.validateTextRowExpression(key);
            try operands.append(alloc, key);
            key_transferred = true;

            try parser.expectToken(tokens, pos, .comma);
            const value = try hooks.parse_expression(hooks.ptr);
            var value_transferred = false;
            errdefer if (!value_transferred) freeExpression(alloc, value);
            _ = try type_context.rowExpressionOutputType(value);
            try operands.append(alloc, value);
            value_transferred = true;

            if (parser.matchToken(tokens, pos, .comma) == null) break;
        }
        try parser.expectToken(tokens, pos, .rparen);
    }
    const expression = try buildFunctionExpressionFromOperandListAlloc(alloc, .json_build_object, &operands);
    var expression_transferred = false;
    errdefer if (!expression_transferred) freeExpression(alloc, expression);
    try type_context.validateJsonBuildObjectExpression(expression);
    expression_transferred = true;
    return expression;
}

pub fn parseArrayPositionRowExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    type_context: RowExpressionTypeContext,
    hooks: FixedBinaryRowExpressionParserHooks,
) !db_mod.types.RelationalRowsExpression {
    const kind = try parseArrayPositionFunctionCallStart(tokens, pos);
    const array_expression = try hooks.parse_expression(hooks.ptr);
    var array_transferred = false;
    errdefer if (!array_transferred) freeExpression(alloc, array_expression);
    const array_type = try type_context.rowExpressionOutputType(array_expression);
    if (array_type != .array) return error.UnsupportedSqlShape;
    try parser.expectToken(tokens, pos, .comma);
    const needle_expression = try hooks.parse_expression(hooks.ptr);
    var needle_transferred = false;
    errdefer if (!needle_transferred) freeExpression(alloc, needle_expression);
    try parser.expectToken(tokens, pos, .rparen);

    const expression = try buildBinaryFunctionExpressionAlloc(alloc, kind, array_expression, needle_expression);
    var expression_transferred = false;
    errdefer if (!expression_transferred) freeExpression(alloc, expression);
    array_transferred = true;
    needle_transferred = true;
    try type_context.validateArrayPositionExpression(expression);

    expression_transferred = true;
    return expression;
}

pub fn parseStringToArrayRowExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    type_context: RowExpressionTypeContext,
    hooks: FixedBinaryRowExpressionParserHooks,
) !db_mod.types.RelationalRowsExpression {
    try parseStringToArrayFunctionCallStart(tokens, pos);
    const text_expression = try hooks.parse_expression(hooks.ptr);
    var text_transferred = false;
    errdefer if (!text_transferred) freeExpression(alloc, text_expression);
    try parser.expectToken(tokens, pos, .comma);
    const delimiter_expression = try hooks.parse_expression(hooks.ptr);
    var delimiter_transferred = false;
    errdefer if (!delimiter_transferred) freeExpression(alloc, delimiter_expression);
    try parser.expectToken(tokens, pos, .rparen);

    const expression = try buildBinaryFunctionExpressionAlloc(alloc, .string_to_array, text_expression, delimiter_expression);
    var expression_transferred = false;
    errdefer if (!expression_transferred) freeExpression(alloc, expression);
    text_transferred = true;
    delimiter_transferred = true;
    try type_context.validateStringToArrayExpression(expression);
    expression_transferred = true;
    return expression;
}

pub fn parseArrayToStringRowExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    type_context: RowExpressionTypeContext,
    hooks: VariadicRowExpressionParserHooks,
) !db_mod.types.RelationalRowsExpression {
    try parseArrayToStringFunctionCallStart(tokens, pos);
    var operands = std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpression).empty;
    errdefer {
        for (operands.items) |operand| freeExpression(alloc, operand);
        operands.deinit(alloc);
    }

    const array_expression = try hooks.parse_expression(hooks.ptr);
    var array_transferred = false;
    errdefer if (!array_transferred) freeExpression(alloc, array_expression);
    try operands.append(alloc, array_expression);
    array_transferred = true;

    try parser.expectToken(tokens, pos, .comma);
    const delimiter_expression = try hooks.parse_expression(hooks.ptr);
    var delimiter_transferred = false;
    errdefer if (!delimiter_transferred) freeExpression(alloc, delimiter_expression);
    try operands.append(alloc, delimiter_expression);
    delimiter_transferred = true;

    if (parser.matchToken(tokens, pos, .comma) != null) {
        const null_expression = try hooks.parse_expression(hooks.ptr);
        var null_transferred = false;
        errdefer if (!null_transferred) freeExpression(alloc, null_expression);
        try operands.append(alloc, null_expression);
        null_transferred = true;
    }
    try parser.expectToken(tokens, pos, .rparen);

    const expression = try buildFunctionExpressionFromOperandListAlloc(alloc, .array_to_string, &operands);
    var expression_transferred = false;
    errdefer if (!expression_transferred) freeExpression(alloc, expression);
    try type_context.validateArrayToStringExpression(expression);
    expression_transferred = true;
    return expression;
}

pub fn parseArrayElementTransformRowExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    type_context: RowExpressionTypeContext,
    hooks: VariadicRowExpressionParserHooks,
) !db_mod.types.RelationalRowsExpression {
    const kind = try parseArrayElementTransformFunctionCallStart(tokens, pos);
    const first_expression = try hooks.parse_expression(hooks.ptr);
    var first_transferred = false;
    errdefer if (!first_transferred) freeExpression(alloc, first_expression);
    try parser.expectToken(tokens, pos, .comma);
    const second_expression = try hooks.parse_expression(hooks.ptr);
    var second_transferred = false;
    errdefer if (!second_transferred) freeExpression(alloc, second_expression);
    var third_expression: db_mod.types.RelationalRowsExpression = undefined;
    var third_transferred = true;
    if (kind == .array_replace) {
        third_transferred = false;
        try parser.expectToken(tokens, pos, .comma);
        third_expression = try hooks.parse_expression(hooks.ptr);
        errdefer if (!third_transferred) freeExpression(alloc, third_expression);
    }
    try parser.expectToken(tokens, pos, .rparen);

    const expression = if (kind == .array_prepend) blk: {
        const out = try buildBinaryFunctionExpressionAlloc(alloc, kind, second_expression, first_expression);
        second_transferred = true;
        first_transferred = true;
        break :blk out;
    } else if (kind == .array_replace) blk: {
        const out = try buildTernaryFunctionExpressionAlloc(alloc, kind, first_expression, second_expression, third_expression);
        first_transferred = true;
        second_transferred = true;
        third_transferred = true;
        break :blk out;
    } else blk: {
        const out = try buildBinaryFunctionExpressionAlloc(alloc, kind, first_expression, second_expression);
        first_transferred = true;
        second_transferred = true;
        break :blk out;
    };
    var expression_transferred = false;
    errdefer if (!expression_transferred) freeExpression(alloc, expression);
    if (kind == .array_cat) {
        try type_context.validateArrayCatExpression(expression);
    } else if (kind == .array_replace) {
        try type_context.validateArrayReplaceExpression(expression);
    } else {
        try type_context.validateArrayElementTransformExpression(expression);
    }

    expression_transferred = true;
    return expression;
}

pub fn parseFixedNumericBinaryRowExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    kind: db_mod.types.RelationalRowsExpressionKind,
    type_context: RowExpressionTypeContext,
    hooks: FixedBinaryRowExpressionParserHooks,
) !db_mod.types.RelationalRowsExpression {
    try parseFixedBinaryFunctionCallStart(tokens, pos, kind);
    const lhs = try hooks.parse_expression(hooks.ptr);
    var lhs_transferred = false;
    errdefer if (!lhs_transferred) freeExpression(alloc, lhs);
    try type_context.validateNumericRowExpression(lhs);
    try parser.expectToken(tokens, pos, .comma);
    const rhs = try hooks.parse_expression(hooks.ptr);
    var rhs_transferred = false;
    errdefer if (!rhs_transferred) freeExpression(alloc, rhs);
    try type_context.validateNumericRowExpression(rhs);
    try parser.expectToken(tokens, pos, .rparen);
    const expression = try buildBinaryFunctionExpressionAlloc(alloc, kind, lhs, rhs);
    lhs_transferred = true;
    rhs_transferred = true;
    return expression;
}

pub fn parseTextBinaryRowExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    kind: db_mod.types.RelationalRowsExpressionKind,
    type_context: RowExpressionTypeContext,
    hooks: FixedBinaryRowExpressionParserHooks,
) !db_mod.types.RelationalRowsExpression {
    switch (kind) {
        .starts_with => try parseFunctionCallStartIf(tokens, pos, sqlKeywordIsStartsWithFunction),
        .ends_with => try parseFunctionCallStartIf(tokens, pos, sqlKeywordIsEndsWithFunction),
        .regexp_substr => try parseFunctionCallStartIf(tokens, pos, sqlKeywordIsRegexpSubstrFunction),
        .regexp_count => try parseFunctionCallStartIf(tokens, pos, sqlKeywordIsRegexpCountFunction),
        .regexp_instr => try parseFunctionCallStartIf(tokens, pos, sqlKeywordIsRegexpInstrFunction),
        else => return error.UnsupportedSqlShape,
    }

    const lhs = try hooks.parse_expression(hooks.ptr);
    var lhs_transferred = false;
    errdefer if (!lhs_transferred) freeExpression(alloc, lhs);
    try type_context.validateTextRowExpression(lhs);
    try parser.expectToken(tokens, pos, .comma);
    const rhs = try hooks.parse_expression(hooks.ptr);
    var rhs_transferred = false;
    errdefer if (!rhs_transferred) freeExpression(alloc, rhs);
    try type_context.validateTextRowExpression(rhs);
    try parser.expectToken(tokens, pos, .rparen);

    const expression = try buildBinaryFunctionExpressionAlloc(alloc, kind, lhs, rhs);
    lhs_transferred = true;
    rhs_transferred = true;
    return expression;
}

pub fn parseTextTernaryRowExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    kind: db_mod.types.RelationalRowsExpressionKind,
    type_context: RowExpressionTypeContext,
    hooks: VariadicRowExpressionParserHooks,
) !db_mod.types.RelationalRowsExpression {
    switch (kind) {
        .replace => try parseReplaceFunctionCallStart(tokens, pos),
        .translate => try parseFunctionCallStartIf(tokens, pos, sqlKeywordIsTranslateFunction),
        else => return error.UnsupportedSqlShape,
    }

    const first = try hooks.parse_expression(hooks.ptr);
    var first_transferred = false;
    errdefer if (!first_transferred) freeExpression(alloc, first);
    try type_context.validateTextRowExpression(first);
    try parser.expectToken(tokens, pos, .comma);

    const second = try hooks.parse_expression(hooks.ptr);
    var second_transferred = false;
    errdefer if (!second_transferred) freeExpression(alloc, second);
    try type_context.validateTextRowExpression(second);
    try parser.expectToken(tokens, pos, .comma);

    const third = try hooks.parse_expression(hooks.ptr);
    var third_transferred = false;
    errdefer if (!third_transferred) freeExpression(alloc, third);
    try type_context.validateTextRowExpression(third);
    try parser.expectToken(tokens, pos, .rparen);

    const expression = try buildTernaryFunctionExpressionAlloc(alloc, kind, first, second, third);
    first_transferred = true;
    second_transferred = true;
    third_transferred = true;
    return expression;
}

pub fn parseRegexpListRowExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    kind: db_mod.types.RelationalRowsExpressionKind,
    type_context: RowExpressionTypeContext,
    hooks: VariadicRowExpressionParserHooks,
) !db_mod.types.RelationalRowsExpression {
    switch (kind) {
        .regexp_replace => try parseRegexpReplaceFunctionCallStart(tokens, pos),
        .regexp_match => try parseFunctionCallStartIf(tokens, pos, sqlKeywordIsRegexpMatchFunction),
        else => return error.UnsupportedSqlShape,
    }

    var operands = std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpression).empty;
    errdefer {
        for (operands.items) |operand| freeExpression(alloc, operand);
        operands.deinit(alloc);
    }

    switch (kind) {
        .regexp_replace => {
            while (true) {
                const operand = try hooks.parse_expression(hooks.ptr);
                var operand_transferred = false;
                errdefer if (!operand_transferred) freeExpression(alloc, operand);
                try type_context.validateTextRowExpression(operand);
                try operands.append(alloc, operand);
                operand_transferred = true;
                if (parser.matchToken(tokens, pos, .comma) == null) break;
            }
            if (operands.items.len != 3 and operands.items.len != 4) return error.UnsupportedSqlShape;
        },
        .regexp_match => {
            const source = try hooks.parse_expression(hooks.ptr);
            var source_transferred = false;
            errdefer if (!source_transferred) freeExpression(alloc, source);
            try type_context.validateTextRowExpression(source);
            try operands.append(alloc, source);
            source_transferred = true;

            try parser.expectToken(tokens, pos, .comma);
            const pattern = try hooks.parse_expression(hooks.ptr);
            var pattern_transferred = false;
            errdefer if (!pattern_transferred) freeExpression(alloc, pattern);
            try type_context.validateTextRowExpression(pattern);
            try operands.append(alloc, pattern);
            pattern_transferred = true;

            if (parser.matchToken(tokens, pos, .comma) != null) {
                const case_insensitive = try hooks.parse_expression(hooks.ptr);
                var case_transferred = false;
                errdefer if (!case_transferred) freeExpression(alloc, case_insensitive);
                try type_context.validateBooleanRowExpression(case_insensitive);
                try operands.append(alloc, case_insensitive);
                case_transferred = true;
            }
        },
        else => unreachable,
    }

    try parser.expectToken(tokens, pos, .rparen);
    return try buildFunctionExpressionFromOperandListAlloc(alloc, kind, &operands);
}

pub fn parseSubstringRowExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    type_context: RowExpressionTypeContext,
    hooks: VariadicRowExpressionParserHooks,
) !db_mod.types.RelationalRowsExpression {
    try parseFunctionCallStartIf(tokens, pos, sqlKeywordIsSubstringFunction);
    var operands = std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpression).empty;
    errdefer {
        for (operands.items) |operand| freeExpression(alloc, operand);
        operands.deinit(alloc);
    }

    const text_operand = try hooks.parse_expression(hooks.ptr);
    var text_transferred = false;
    errdefer if (!text_transferred) freeExpression(alloc, text_operand);
    try type_context.validateTextRowExpression(text_operand);
    try operands.append(alloc, text_operand);
    text_transferred = true;

    if (parser.matchToken(tokens, pos, .comma) != null) {
        const start_operand = try hooks.parse_expression(hooks.ptr);
        var start_transferred = false;
        errdefer if (!start_transferred) freeExpression(alloc, start_operand);
        try type_context.validateNumericRowExpression(start_operand);
        try operands.append(alloc, start_operand);
        start_transferred = true;

        if (parser.matchToken(tokens, pos, .comma) != null) {
            const length_operand = try hooks.parse_expression(hooks.ptr);
            var length_transferred = false;
            errdefer if (!length_transferred) freeExpression(alloc, length_operand);
            try type_context.validateNumericRowExpression(length_operand);
            try operands.append(alloc, length_operand);
            length_transferred = true;
        }
    } else if (parser.matchKeyword(tokens, pos, "from")) {
        const start_operand = try hooks.parse_expression(hooks.ptr);
        var start_transferred = false;
        errdefer if (!start_transferred) freeExpression(alloc, start_operand);
        try type_context.validateNumericRowExpression(start_operand);
        try operands.append(alloc, start_operand);
        start_transferred = true;

        if (parser.matchKeyword(tokens, pos, "for")) {
            const length_operand = try hooks.parse_expression(hooks.ptr);
            var length_transferred = false;
            errdefer if (!length_transferred) freeExpression(alloc, length_operand);
            try type_context.validateNumericRowExpression(length_operand);
            try operands.append(alloc, length_operand);
            length_transferred = true;
        }
    } else return error.UnsupportedSqlShape;

    try parser.expectToken(tokens, pos, .rparen);
    return try buildFunctionExpressionFromOperandListAlloc(alloc, .substring, &operands);
}

pub fn parseOverlayRowExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    type_context: RowExpressionTypeContext,
    hooks: VariadicRowExpressionParserHooks,
) !db_mod.types.RelationalRowsExpression {
    try parseFunctionCallStartIf(tokens, pos, sqlKeywordIsOverlayFunction);
    var operands = std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpression).empty;
    errdefer {
        for (operands.items) |operand| freeExpression(alloc, operand);
        operands.deinit(alloc);
    }

    const source_operand = try hooks.parse_expression(hooks.ptr);
    var source_transferred = false;
    errdefer if (!source_transferred) freeExpression(alloc, source_operand);
    try type_context.validateTextRowExpression(source_operand);
    try operands.append(alloc, source_operand);
    source_transferred = true;

    try parser.expectKeyword(tokens, pos, "placing");
    const replacement_operand = try hooks.parse_expression(hooks.ptr);
    var replacement_transferred = false;
    errdefer if (!replacement_transferred) freeExpression(alloc, replacement_operand);
    try type_context.validateTextRowExpression(replacement_operand);
    try operands.append(alloc, replacement_operand);
    replacement_transferred = true;

    try parser.expectKeyword(tokens, pos, "from");
    const start_operand = try hooks.parse_expression(hooks.ptr);
    var start_transferred = false;
    errdefer if (!start_transferred) freeExpression(alloc, start_operand);
    try type_context.validateNumericRowExpression(start_operand);
    try operands.append(alloc, start_operand);
    start_transferred = true;

    if (parser.matchKeyword(tokens, pos, "for")) {
        const length_operand = try hooks.parse_expression(hooks.ptr);
        var length_transferred = false;
        errdefer if (!length_transferred) freeExpression(alloc, length_operand);
        try type_context.validateNumericRowExpression(length_operand);
        try operands.append(alloc, length_operand);
        length_transferred = true;
    }

    try parser.expectToken(tokens, pos, .rparen);
    return try buildFunctionExpressionFromOperandListAlloc(alloc, .overlay, &operands);
}

pub fn parseSplitPartRowExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    type_context: RowExpressionTypeContext,
    hooks: VariadicRowExpressionParserHooks,
) !db_mod.types.RelationalRowsExpression {
    try parseFunctionCallStartIf(tokens, pos, sqlKeywordIsSplitPartFunction);
    const source = try hooks.parse_expression(hooks.ptr);
    var source_transferred = false;
    errdefer if (!source_transferred) freeExpression(alloc, source);
    try type_context.validateTextRowExpression(source);
    try parser.expectToken(tokens, pos, .comma);

    const delimiter = try hooks.parse_expression(hooks.ptr);
    var delimiter_transferred = false;
    errdefer if (!delimiter_transferred) freeExpression(alloc, delimiter);
    try type_context.validateTextRowExpression(delimiter);
    try parser.expectToken(tokens, pos, .comma);

    const position = try hooks.parse_expression(hooks.ptr);
    var position_transferred = false;
    errdefer if (!position_transferred) freeExpression(alloc, position);
    try type_context.validateNumericRowExpression(position);
    try parser.expectToken(tokens, pos, .rparen);

    const expression = try buildTernaryFunctionExpressionAlloc(alloc, .split_part, source, delimiter, position);
    source_transferred = true;
    delimiter_transferred = true;
    position_transferred = true;
    return expression;
}

pub fn parseStrposRowExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    type_context: RowExpressionTypeContext,
    hooks: VariadicRowExpressionParserHooks,
) !db_mod.types.RelationalRowsExpression {
    if (matchFunctionKeyword(tokens, pos, sqlKeywordIsStrposFunction)) {
        try parser.expectToken(tokens, pos, .lparen);
        const source = try hooks.parse_expression(hooks.ptr);
        var source_transferred = false;
        errdefer if (!source_transferred) freeExpression(alloc, source);
        try type_context.validateTextRowExpression(source);
        try parser.expectToken(tokens, pos, .comma);

        const needle = try hooks.parse_expression(hooks.ptr);
        var needle_transferred = false;
        errdefer if (!needle_transferred) freeExpression(alloc, needle);
        try type_context.validateTextRowExpression(needle);
        try parser.expectToken(tokens, pos, .rparen);

        const expression = try buildBinaryFunctionExpressionAlloc(alloc, .strpos, source, needle);
        source_transferred = true;
        needle_transferred = true;
        return expression;
    } else if (parser.matchKeyword(tokens, pos, "position")) {
        try parser.expectToken(tokens, pos, .lparen);
        const needle = try hooks.parse_operand(hooks.ptr);
        var needle_transferred = false;
        errdefer if (!needle_transferred) freeExpression(alloc, needle);
        try type_context.validateTextRowExpression(needle);
        try parser.expectKeyword(tokens, pos, "in");

        const source = try hooks.parse_expression(hooks.ptr);
        var source_transferred = false;
        errdefer if (!source_transferred) freeExpression(alloc, source);
        try type_context.validateTextRowExpression(source);
        try parser.expectToken(tokens, pos, .rparen);

        const expression = try buildBinaryFunctionExpressionAlloc(alloc, .strpos, source, needle);
        source_transferred = true;
        needle_transferred = true;
        return expression;
    } else return error.UnsupportedSqlShape;
}

pub fn parsePadRowExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    type_context: RowExpressionTypeContext,
    hooks: VariadicRowExpressionParserHooks,
) !db_mod.types.RelationalRowsExpression {
    const kind = matchPadFunctionKind(tokens, pos) orelse return error.UnsupportedSqlShape;
    try parser.expectToken(tokens, pos, .lparen);
    var operands = std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpression).empty;
    errdefer {
        for (operands.items) |operand| freeExpression(alloc, operand);
        operands.deinit(alloc);
    }

    const source = try hooks.parse_expression(hooks.ptr);
    var source_transferred = false;
    errdefer if (!source_transferred) freeExpression(alloc, source);
    try type_context.validateTextRowExpression(source);
    try operands.append(alloc, source);
    source_transferred = true;
    try parser.expectToken(tokens, pos, .comma);

    const target = try hooks.parse_expression(hooks.ptr);
    var target_transferred = false;
    errdefer if (!target_transferred) freeExpression(alloc, target);
    try type_context.validateNumericRowExpression(target);
    try operands.append(alloc, target);
    target_transferred = true;

    if (parser.matchToken(tokens, pos, .comma) != null) {
        const fill = try hooks.parse_expression(hooks.ptr);
        var fill_transferred = false;
        errdefer if (!fill_transferred) freeExpression(alloc, fill);
        try type_context.validateTextRowExpression(fill);
        try operands.append(alloc, fill);
        fill_transferred = true;
    }
    try parser.expectToken(tokens, pos, .rparen);
    return try buildFunctionExpressionFromOperandListAlloc(alloc, kind, &operands);
}

pub fn parseLeftRightRowExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    type_context: RowExpressionTypeContext,
    hooks: FixedBinaryRowExpressionParserHooks,
) !db_mod.types.RelationalRowsExpression {
    const kind = matchLeftRightFunctionKind(tokens, pos) orelse return error.UnsupportedSqlShape;
    try parser.expectToken(tokens, pos, .lparen);
    return try parseTextNumericBinaryRowExpressionRestAlloc(alloc, tokens, pos, kind, type_context, hooks);
}

pub fn parseRepeatRowExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    type_context: RowExpressionTypeContext,
    hooks: FixedBinaryRowExpressionParserHooks,
) !db_mod.types.RelationalRowsExpression {
    try parseFunctionCallStartIf(tokens, pos, sqlKeywordIsRepeatFunction);
    return try parseTextNumericBinaryRowExpressionRestAlloc(alloc, tokens, pos, .repeat, type_context, hooks);
}

pub fn parseDateTruncRowExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    type_context: RowExpressionTypeContext,
    hooks: FixedBinaryRowExpressionParserHooks,
) !db_mod.types.RelationalRowsExpression {
    try parseFunctionCallStartIf(tokens, pos, sqlKeywordIsDateTruncFunction);
    const unit = try hooks.parse_expression(hooks.ptr);
    var unit_transferred = false;
    errdefer if (!unit_transferred) freeExpression(alloc, unit);
    try type_context.validateTextRowExpression(unit);
    try parser.expectToken(tokens, pos, .comma);

    const value = try hooks.parse_expression(hooks.ptr);
    var value_transferred = false;
    errdefer if (!value_transferred) freeExpression(alloc, value);
    try type_context.validateNumericOrDatetimeRowExpression(value);
    try parser.expectToken(tokens, pos, .rparen);

    const expression = try buildBinaryFunctionExpressionAlloc(alloc, .date_trunc, unit, value);
    unit_transferred = true;
    value_transferred = true;
    return expression;
}

pub fn parseDateBinRowExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    type_context: RowExpressionTypeContext,
    hooks: VariadicRowExpressionParserHooks,
) !db_mod.types.RelationalRowsExpression {
    try parseFunctionCallStartIf(tokens, pos, sqlKeywordIsDateBinFunction);
    const stride = try hooks.parse_expression(hooks.ptr);
    var stride_transferred = false;
    errdefer if (!stride_transferred) freeExpression(alloc, stride);
    try type_context.validateDateBinStrideRowExpression(stride);
    try parser.expectToken(tokens, pos, .comma);

    const source = try hooks.parse_expression(hooks.ptr);
    var source_transferred = false;
    errdefer if (!source_transferred) freeExpression(alloc, source);
    try type_context.validateNumericOrDatetimeRowExpression(source);
    try parser.expectToken(tokens, pos, .comma);

    const origin = try hooks.parse_expression(hooks.ptr);
    var origin_transferred = false;
    errdefer if (!origin_transferred) freeExpression(alloc, origin);
    try type_context.validateNumericOrDatetimeRowExpression(origin);
    try parser.expectToken(tokens, pos, .rparen);

    const expression = try buildTernaryFunctionExpressionAlloc(alloc, .date_bin, stride, source, origin);
    stride_transferred = true;
    source_transferred = true;
    origin_transferred = true;
    return expression;
}

pub fn parseDatePartRowExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    type_context: RowExpressionTypeContext,
    hooks: VariadicRowExpressionParserHooks,
) !db_mod.types.RelationalRowsExpression {
    const extract_syntax = try parseDatePartFunctionCallStart(tokens, pos);
    var unit: db_mod.types.RelationalRowsExpression = undefined;
    var unit_initialized = false;
    var unit_transferred = false;
    errdefer if (unit_initialized and !unit_transferred) freeExpression(alloc, unit);
    var value: db_mod.types.RelationalRowsExpression = undefined;
    var value_initialized = false;
    var value_transferred = false;
    errdefer if (value_initialized and !value_transferred) freeExpression(alloc, value);

    if (extract_syntax) {
        unit = try parseDatePartUnitLiteralExpressionAlloc(alloc, tokens, pos);
        unit_initialized = true;
        try parseDatePartExtractSeparator(tokens, pos);
        value = try hooks.parse_expression(hooks.ptr);
        value_initialized = true;
        try type_context.validateNumericOrDatetimeRowExpression(value);
    } else {
        unit = try hooks.parse_expression(hooks.ptr);
        unit_initialized = true;
        try type_context.validateTextRowExpression(unit);
        try parser.expectToken(tokens, pos, .comma);
        value = try hooks.parse_expression(hooks.ptr);
        value_initialized = true;
        try type_context.validateNumericOrDatetimeRowExpression(value);
    }
    try parser.expectToken(tokens, pos, .rparen);

    const expression = try buildBinaryFunctionExpressionAlloc(alloc, .date_part, unit, value);
    unit_transferred = true;
    value_transferred = true;
    return expression;
}

fn parseDatePartUnitLiteralExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !db_mod.types.RelationalRowsExpression {
    const token = if (parser.matchToken(tokens, pos, .identifier)) |token|
        token
    else if (parser.matchToken(tokens, pos, .string)) |token|
        token
    else
        return error.UnsupportedSqlShape;
    if (std.ascii.eqlIgnoreCase(token.text, "from")) return error.UnsupportedSqlShape;
    return .{
        .kind = .value,
        .value_json = try std.json.Stringify.valueAlloc(alloc, token.text, .{}),
    };
}

pub fn parseConcatRowExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    type_context: RowExpressionTypeContext,
    hooks: VariadicRowExpressionParserHooks,
) !db_mod.types.RelationalRowsExpression {
    const kind = try parseConcatFunctionCallStart(tokens, pos);

    var operands = std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpression).empty;
    errdefer {
        for (operands.items) |operand| freeExpression(alloc, operand);
        operands.deinit(alloc);
    }
    while (true) {
        const operand = try hooks.parse_operand(hooks.ptr);
        var operand_transferred = false;
        errdefer if (!operand_transferred) freeExpression(alloc, operand);
        try operands.append(alloc, operand);
        operand_transferred = true;
        if (parser.matchToken(tokens, pos, .comma) == null) break;
    }
    if ((kind == .concat and operands.items.len == 0) or (kind == .concat_ws and operands.items.len < 2)) return error.UnsupportedSqlShape;
    if (kind == .concat_ws) {
        for (operands.items) |operand| try type_context.validateTextRowExpression(operand);
    }
    try parser.expectToken(tokens, pos, .rparen);

    return try buildFunctionExpressionFromOperandListAlloc(alloc, kind, &operands);
}

pub fn parsePipeConcatExpressionRestAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    left: db_mod.types.RelationalRowsExpression,
    type_context: RowExpressionTypeContext,
    hooks: VariadicRowExpressionParserHooks,
) !db_mod.types.RelationalRowsExpression {
    var left_transferred = false;
    errdefer if (!left_transferred) freeExpression(alloc, left);
    try type_context.validateTextRowExpression(left);

    var operands = std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpression).empty;
    errdefer {
        for (operands.items) |operand| freeExpression(alloc, operand);
        operands.deinit(alloc);
    }
    try operands.append(alloc, left);
    left_transferred = true;

    while (parser.matchToken(tokens, pos, .pipe_concat) != null) {
        const rhs = try hooks.parse_operand(hooks.ptr);
        var rhs_transferred = false;
        errdefer if (!rhs_transferred) freeExpression(alloc, rhs);
        try type_context.validateTextRowExpression(rhs);
        try operands.append(alloc, rhs);
        rhs_transferred = true;
    }
    if (operands.items.len < 2) return error.UnsupportedSqlShape;
    return try buildFunctionExpressionFromOperandListAlloc(alloc, .concat, &operands);
}

pub fn parseArithmeticExpressionRestAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    lhs: db_mod.types.RelationalRowsExpression,
    min_precedence: u8,
    type_context: RowExpressionTypeContext,
    hooks: ArithmeticExpressionParserHooks,
) !db_mod.types.RelationalRowsExpression {
    var current = lhs;
    var current_owned = true;
    errdefer if (current_owned) freeExpression(alloc, current);

    while (peekArithmeticOperator(tokens, pos.*)) |op| {
        if (op.precedence < min_precedence) break;
        _ = parser.matchToken(tokens, pos, op.token) orelse unreachable;
        if (peekSqlIntervalExpressionSyntax(tokens, pos.*)) {
            try type_context.validateNumericOrDatetimeRowExpression(current);
            const interval = try value_mod.parseSqlIntervalLiteral(tokens, pos);
            const next = try buildIntervalLiteralArithmeticAlloc(alloc, current, op.kind, interval);
            current_owned = false;
            current = next;
            current_owned = true;
            continue;
        }

        var rhs = if (peekParenthesizedExpressionSyntax(tokens, pos.*))
            try hooks.parse_parenthesized_boolean(hooks.ptr)
        else
            try hooks.parse_operand(hooks.ptr);
        var rhs_owned = true;
        errdefer if (rhs_owned) freeExpression(alloc, rhs);
        try type_context.validateNumericRowExpression(rhs);

        while (peekArithmeticOperator(tokens, pos.*)) |next_op| {
            if (next_op.precedence <= op.precedence) break;
            rhs_owned = false;
            rhs = try parseArithmeticExpressionRestAlloc(alloc, tokens, pos, rhs, next_op.precedence, type_context, hooks);
            rhs_owned = true;
        }

        const expression = try buildBinaryExpressionAlloc(alloc, op.kind, current, rhs);
        current_owned = false;
        rhs_owned = false;
        current = expression;
        current_owned = true;
    }

    current_owned = false;
    return current;
}

pub fn parseBooleanExpressionRestAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    lhs: db_mod.types.RelationalRowsExpression,
    min_precedence: u8,
    type_context: RowExpressionTypeContext,
    hooks: BooleanExpressionParserHooks,
) !db_mod.types.RelationalRowsExpression {
    try type_context.validateBooleanRowExpression(lhs);
    var current = lhs;
    var current_owned = true;
    errdefer if (current_owned) freeExpression(alloc, current);

    while (peekBooleanOperator(tokens, pos.*)) |op| {
        if (op.precedence < min_precedence) break;
        if (!parser.matchKeyword(tokens, pos, op.keyword)) unreachable;
        var rhs = try hooks.parse_operand(hooks.ptr);
        var rhs_owned = true;
        errdefer if (rhs_owned) freeExpression(alloc, rhs);
        try type_context.validateBooleanRowExpression(rhs);

        while (peekBooleanOperator(tokens, pos.*)) |next_op| {
            if (next_op.precedence <= op.precedence) break;
            rhs_owned = false;
            rhs = try parseBooleanExpressionRestAlloc(alloc, tokens, pos, rhs, next_op.precedence, type_context, hooks);
            rhs_owned = true;
        }

        const expression = try buildBinaryExpressionAlloc(alloc, op.kind, current, rhs);
        current_owned = false;
        rhs_owned = false;
        current = expression;
        current_owned = true;
    }

    current_owned = false;
    return current;
}

pub fn parseRowExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    type_context: RowExpressionTypeContext,
    hooks: RowExpressionParserHooks,
    arithmetic_hooks: ArithmeticExpressionParserHooks,
    variadic_hooks: VariadicRowExpressionParserHooks,
) !db_mod.types.RelationalRowsExpression {
    var expression = try hooks.parse_operand(hooks.ptr);
    var expression_owned = true;
    errdefer if (expression_owned) freeExpression(alloc, expression);
    if (peekArithmeticOperator(tokens, pos.*)) |_| {
        try type_context.validateNumericOrDatetimeRowExpression(expression);
        expression_owned = false;
        expression = try parseArithmeticExpressionRestAlloc(alloc, tokens, pos, expression, 0, type_context, arithmetic_hooks);
        expression_owned = true;
        try type_context.validateNumericRowExpression(expression);
    }
    if (parser.peekKind(tokens, pos.*, .pipe_concat)) {
        expression_owned = false;
        expression = try parsePipeConcatExpressionRestAlloc(alloc, tokens, pos, expression, type_context, variadic_hooks);
        expression_owned = true;
    }
    expression_owned = false;
    return expression;
}

pub fn parseArithmeticExpressionProjectionFromFieldAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    lhs_field: []const u8,
    field_source: db_mod.types.RelationalRowsExpressionFieldSource,
    type_context: RowExpressionTypeContext,
    hooks: ArithmeticExpressionParserHooks,
) !db_mod.types.RelationalRowsExpressionProjection {
    var current: db_mod.types.RelationalRowsExpression = .{
        .kind = .field,
        .field = lhs_field,
        .field_source = field_source,
    };
    var current_owned = true;
    errdefer if (current_owned) freeExpression(alloc, current);

    current_owned = false;
    current = try parseArithmeticExpressionRestAlloc(alloc, tokens, pos, current, 0, type_context, hooks);
    current_owned = true;
    try type_context.validateNumericRowExpression(current);

    const default_output = switch (current.kind) {
        .add => "add",
        .sub => "sub",
        .mul => "mul",
        .div => "div",
        else => "expr",
    };
    current_owned = false;
    return try buildExpressionProjectionFromOwnedExpressionAlloc(alloc, tokens, pos, current, default_output);
}

pub fn parseBooleanExpressionProjectionFromFieldAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    lhs_field: []const u8,
    field_source: db_mod.types.RelationalRowsExpressionFieldSource,
    type_context: RowExpressionTypeContext,
    hooks: BooleanExpressionParserHooks,
) !db_mod.types.RelationalRowsExpressionProjection {
    var current: db_mod.types.RelationalRowsExpression = .{
        .kind = .field,
        .field = lhs_field,
        .field_source = field_source,
    };
    var current_owned = true;
    errdefer if (current_owned) freeExpression(alloc, current);

    current_owned = false;
    current = try parseBooleanExpressionRestAlloc(alloc, tokens, pos, current, 0, type_context, hooks);
    current_owned = true;

    current_owned = false;
    return try buildBooleanExpressionProjectionFromOwnedExpressionAlloc(alloc, tokens, pos, type_context, current);
}

pub fn parseBooleanRowExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    type_context: RowExpressionTypeContext,
    hooks: BooleanRowExpressionParserHooks,
) !db_mod.types.RelationalRowsExpression {
    var expression = try hooks.parse_expression(hooks.ptr);
    var expression_owned = true;
    errdefer if (expression_owned) freeExpression(alloc, expression);
    if (peekBooleanOperator(tokens, pos.*)) |_| {
        try type_context.validateBooleanRowExpression(expression);
        expression_owned = false;
        expression = try parseBooleanExpressionRestAlloc(alloc, tokens, pos, expression, 0, type_context, .{
            .ptr = hooks.ptr,
            .parse_operand = hooks.parse_operand,
        });
        expression_owned = true;
        try type_context.validateBooleanRowExpression(expression);
    }
    expression_owned = false;
    return expression;
}

pub fn parseCaseRowExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    type_context: RowExpressionTypeContext,
    defer_row_expression_field_validation: bool,
    hooks: CaseExpressionParserHooks,
) !db_mod.types.RelationalRowsExpression {
    try parseCaseExpressionStart(tokens, pos);

    var branches = std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionCaseBranch).empty;
    errdefer {
        for (branches.items) |branch| plan_mod.freeExpressionCaseBranch(alloc, branch);
        branches.deinit(alloc);
    }

    while (matchCaseExpressionWhen(tokens, pos)) {
        const condition = try parseCaseExpressionConditionAlloc(alloc, tokens, pos, type_context, defer_row_expression_field_validation, hooks);
        var condition_transferred = false;
        errdefer if (!condition_transferred) freeExpressionCondition(alloc, condition);
        try parseCaseExpressionThen(tokens, pos);
        const then_expression = try hooks.parse_operand(hooks.ptr);
        var then_transferred = false;
        errdefer if (!then_transferred) freeExpression(alloc, then_expression);
        try branches.append(alloc, .{ .when = condition, .then = then_expression });
        condition_transferred = true;
        then_transferred = true;
    }
    if (branches.items.len == 0) return error.UnsupportedSqlShape;

    try parseCaseExpressionElse(tokens, pos);
    const else_expression = try hooks.parse_operand(hooks.ptr);
    var else_transferred = false;
    errdefer if (!else_transferred) freeExpression(alloc, else_expression);
    try parseCaseExpressionEnd(tokens, pos);

    const owned_branches = try branches.toOwnedSlice(alloc);
    var branches_transferred = false;
    errdefer if (!branches_transferred) {
        for (owned_branches) |branch| plan_mod.freeExpressionCaseBranch(alloc, branch);
        alloc.free(owned_branches);
    };
    const fallback = try alloc.alloc(db_mod.types.RelationalRowsExpression, 1);
    var fallback_initialized = false;
    var fallback_transferred = false;
    errdefer if (!fallback_transferred) {
        if (fallback_initialized) freeExpression(alloc, fallback[0]);
        alloc.free(fallback);
    };
    fallback[0] = else_expression;
    fallback_initialized = true;
    else_transferred = true;

    _ = try type_context.caseExpressionOutputType(owned_branches, fallback);

    branches_transferred = true;
    fallback_transferred = true;
    return .{
        .kind = .case,
        .case_branches = owned_branches,
        .case_else = fallback,
    };
}

pub fn parseCaseExpressionConditionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    type_context: RowExpressionTypeContext,
    defer_row_expression_field_validation: bool,
    hooks: CaseExpressionParserHooks,
) !db_mod.types.RelationalRowsExpressionCondition {
    const lhs = try hooks.parse_expression(hooks.ptr);
    var lhs_transferred = false;
    errdefer if (!lhs_transferred) freeExpression(alloc, lhs);

    const op: runtime_schema.RelationalCheckOp = if (try parseExpressionIsTailIf(tokens, pos, .{})) |is_tail|
        is_tail.op
    else
        try parseComparisonOp(tokens, pos);

    const rhs = switch (op) {
        .is_null, .is_not_null => &.{},
        else => blk: {
            const out = try alloc.alloc(db_mod.types.RelationalRowsExpression, 1);
            var out_transferred = false;
            errdefer if (!out_transferred) alloc.free(out);
            out[0] = try hooks.parse_expression(hooks.ptr);
            out_transferred = true;
            break :blk out;
        },
    };
    var rhs_transferred = false;
    errdefer if (!rhs_transferred and rhs.len > 0) {
        for (rhs) |expression| freeExpression(alloc, expression);
        alloc.free(rhs);
    };
    try validateExpressionConditionTypes(type_context, defer_row_expression_field_validation, lhs, op, rhs);

    lhs_transferred = true;
    rhs_transferred = true;
    return .{
        .lhs = lhs,
        .op = op,
        .rhs = rhs,
    };
}

pub fn parseCaseFoldRowExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    schema: runtime_schema.TableSchema,
    field_expression_qualifiers: []const []const u8,
    returning_expression_qualifiers: []const []const u8,
    defer_row_expression_field_validation: bool,
    field_source: db_mod.types.RelationalRowsExpressionFieldSource,
    type_context: RowExpressionTypeContext,
    hooks: CaseFoldRowExpressionParserHooks,
) !db_mod.types.RelationalRowsExpression {
    const kind = try parseCaseFoldFunctionCallStart(tokens, pos);
    if (try parsePeriodBoundRowExpressionAlloc(
        alloc,
        tokens,
        pos,
        kind,
        schema,
        field_expression_qualifiers,
        returning_expression_qualifiers,
        defer_row_expression_field_validation,
        field_source,
    )) |expression| return expression;

    var operands = std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpression).empty;
    errdefer {
        for (operands.items) |operand| freeExpression(alloc, operand);
        operands.deinit(alloc);
    }

    const operand = try hooks.parse_expression(hooks.ptr);
    var operand_transferred = false;
    errdefer if (!operand_transferred) freeExpression(alloc, operand);
    try type_context.validateTextRowExpression(operand);
    try operands.append(alloc, operand);
    operand_transferred = true;

    if (kind == .trim or kind == .ltrim or kind == .rtrim) {
        if (parser.matchToken(tokens, pos, .comma) != null) {
            const trim_operand = try hooks.parse_expression(hooks.ptr);
            var trim_transferred = false;
            errdefer if (!trim_transferred) freeExpression(alloc, trim_operand);
            try type_context.validateTextRowExpression(trim_operand);
            try operands.append(alloc, trim_operand);
            trim_transferred = true;
        }
    }
    try parser.expectToken(tokens, pos, .rparen);

    return try buildFunctionExpressionFromOperandListAlloc(alloc, kind, &operands);
}

pub fn parsePeriodBoundRowExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    kind: db_mod.types.RelationalRowsExpressionKind,
    schema: runtime_schema.TableSchema,
    field_expression_qualifiers: []const []const u8,
    returning_expression_qualifiers: []const []const u8,
    defer_row_expression_field_validation: bool,
    field_source: db_mod.types.RelationalRowsExpressionFieldSource,
) !?db_mod.types.RelationalRowsExpression {
    if (kind != .lower and kind != .upper) return null;
    const start_pos = pos.*;
    const parsed_field = parseIdentifierOwnedAlloc(alloc, tokens, pos) catch |err| switch (err) {
        error.UnsupportedSqlShape => return null,
        else => return err,
    };
    defer alloc.free(parsed_field);
    const field = (try binder.normalizePeriodReferenceAlloc(
        alloc,
        schema,
        parsed_field,
        field_expression_qualifiers,
        returning_expression_qualifiers,
    )) orelse (binder.normalizeRowExpressionFieldAlloc(
        alloc,
        schema,
        parsed_field,
        field_expression_qualifiers,
        returning_expression_qualifiers,
        defer_row_expression_field_validation,
    ) catch |err| switch (err) {
        error.InvalidSqlCatalog, error.UnsupportedSqlShape => {
            pos.* = start_pos;
            return null;
        },
        else => return err,
    });
    defer alloc.free(field);
    const period = binder.relationalPeriodForDdl(schema.periods, field) orelse {
        pos.* = start_pos;
        return null;
    };
    if (!parser.Cursor.init(tokens, pos).peekKind(.rparen)) return error.UnsupportedSqlShape;
    try parser.expectToken(tokens, pos, .rparen);
    const bound_name = if (kind == .lower) period.start_column else period.end_column;
    const bound_column = binder.relationalColumnForField(schema, bound_name, null) orelse return error.InvalidSqlCatalog;
    if (!binder.relationalPeriodColumnType(bound_column.field_type)) return error.InvalidSqlCatalog;
    return .{
        .kind = .field,
        .field = try alloc.dupe(u8, bound_column.name),
        .field_source = field_source,
    };
}

fn parseTextNumericBinaryRowExpressionRestAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    kind: db_mod.types.RelationalRowsExpressionKind,
    type_context: RowExpressionTypeContext,
    hooks: FixedBinaryRowExpressionParserHooks,
) !db_mod.types.RelationalRowsExpression {
    const lhs = try hooks.parse_expression(hooks.ptr);
    var lhs_transferred = false;
    errdefer if (!lhs_transferred) freeExpression(alloc, lhs);
    try type_context.validateTextRowExpression(lhs);
    try parser.expectToken(tokens, pos, .comma);
    const rhs = try hooks.parse_expression(hooks.ptr);
    var rhs_transferred = false;
    errdefer if (!rhs_transferred) freeExpression(alloc, rhs);
    try type_context.validateNumericRowExpression(rhs);
    try parser.expectToken(tokens, pos, .rparen);

    const expression = try buildBinaryFunctionExpressionAlloc(alloc, kind, lhs, rhs);
    lhs_transferred = true;
    rhs_transferred = true;
    return expression;
}

pub fn parseGreatestLeastRowExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    type_context: RowExpressionTypeContext,
    hooks: VariadicRowExpressionParserHooks,
) !db_mod.types.RelationalRowsExpression {
    const kind = try parseGreatestLeastFunctionCallStart(tokens, pos);

    var operands = std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpression).empty;
    errdefer {
        for (operands.items) |operand| freeExpression(alloc, operand);
        operands.deinit(alloc);
    }
    while (true) {
        const operand = try hooks.parse_expression(hooks.ptr);
        var operand_transferred = false;
        errdefer if (!operand_transferred) freeExpression(alloc, operand);
        try operands.append(alloc, operand);
        operand_transferred = true;
        if (parser.matchToken(tokens, pos, .comma) == null) break;
    }
    if (operands.items.len == 0) return error.UnsupportedSqlShape;
    try parser.expectToken(tokens, pos, .rparen);

    const expression = try buildFunctionExpressionFromOperandListAlloc(alloc, kind, &operands);
    var expression_transferred = false;
    errdefer if (!expression_transferred) freeExpression(alloc, expression);
    try type_context.validateExpressionOperandDomains(expression);
    expression_transferred = true;
    return expression;
}

pub fn parseArrayLengthRowExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    params: []const value_mod.SqlValue,
    schema: runtime_schema.TableSchema,
    field_expression_qualifiers: []const []const u8,
    returning_expression_qualifiers: []const []const u8,
    defer_row_expression_field_validation: bool,
    field_source: db_mod.types.RelationalRowsExpressionFieldSource,
) !db_mod.types.RelationalRowsExpression {
    const keyword = try parseArrayLengthFunctionCallStart(tokens, pos);
    const field = try parseRowExpressionArrayFieldOwnedAlloc(
        alloc,
        tokens,
        pos,
        schema,
        field_expression_qualifiers,
        returning_expression_qualifiers,
        defer_row_expression_field_validation,
    );
    var field_transferred = false;
    errdefer if (!field_transferred) alloc.free(field);
    try value_mod.parseArrayLengthFunctionTail(tokens, pos, params, keyword);

    const field_expression: db_mod.types.RelationalRowsExpression = .{ .kind = .field, .field = field, .field_source = field_source };
    const expression = try buildUnaryFunctionExpressionAlloc(alloc, .array_length, field_expression);
    field_transferred = true;
    return expression;
}

pub fn parseArrayLengthProjectionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    params: []const value_mod.SqlValue,
    schema: runtime_schema.TableSchema,
) !db_mod.types.RelationalRowsArrayLengthProjection {
    const keyword = try parseArrayLengthFunctionCallStart(tokens, pos);
    const field = try parseArrayFieldOwnedAlloc(alloc, tokens, pos, schema);
    var field_transferred = false;
    errdefer if (!field_transferred) alloc.free(field);
    try value_mod.parseArrayLengthFunctionTail(tokens, pos, params, keyword);
    const output = try grammar.parseProjectionOutputOwnedAlloc(alloc, tokens, pos, arrayLengthDefaultOutput(keyword));
    var output_transferred = false;
    errdefer if (!output_transferred) alloc.free(output);
    field_transferred = true;
    output_transferred = true;
    return .{ .output = output, .field = field };
}

pub fn parseArrayLengthExpressionProjectionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    params: []const value_mod.SqlValue,
    schema: runtime_schema.TableSchema,
    field_expression_qualifiers: []const []const u8,
    returning_expression_qualifiers: []const []const u8,
    defer_row_expression_field_validation: bool,
    field_source: db_mod.types.RelationalRowsExpressionFieldSource,
) !db_mod.types.RelationalRowsExpressionProjection {
    const keyword = try parseArrayLengthFunctionCallStart(tokens, pos);
    const field = try parseRowExpressionArrayFieldOwnedAlloc(
        alloc,
        tokens,
        pos,
        schema,
        field_expression_qualifiers,
        returning_expression_qualifiers,
        defer_row_expression_field_validation,
    );
    var field_transferred = false;
    errdefer if (!field_transferred) alloc.free(field);
    try value_mod.parseArrayLengthFunctionTail(tokens, pos, params, keyword);
    const output = try grammar.parseProjectionOutputOwnedAlloc(alloc, tokens, pos, arrayLengthDefaultOutput(keyword));
    var output_transferred = false;
    errdefer if (!output_transferred) alloc.free(output);
    const expression = try buildUnaryFunctionExpressionAlloc(
        alloc,
        .array_length,
        .{ .kind = .field, .field = field, .field_source = field_source },
    );

    field_transferred = true;
    output_transferred = true;
    return buildExpressionProjection(output, expression);
}

pub fn isCaseFoldExpressionOp(op: runtime_schema.UniqueExpressionOp) bool {
    return switch (op) {
        .lower, .upper, .md5 => true,
        .expression => false,
    };
}

pub fn relationalGeneratedOpForUniqueExpressionOp(op: runtime_schema.UniqueExpressionOp) runtime_schema.RelationalGeneratedOp {
    return switch (op) {
        .lower => .lower,
        .upper => .upper,
        .md5 => .md5,
        .expression => unreachable,
    };
}

pub fn uniqueExpressionOpToken(op: runtime_schema.UniqueExpressionOp) []const u8 {
    return switch (op) {
        .lower => "lower",
        .upper => "upper",
        .md5 => "md5",
        .expression => "expression",
    };
}

pub fn uniquePredicateOpToken(op: runtime_schema.UniquePredicateOp) []const u8 {
    return switch (op) {
        .is_null => "is_null",
        .is_not_null => "is_not_null",
        .eq => "eq",
        .ne => "ne",
    };
}

pub fn uniquePredicateAsRelationalCheckOp(op: runtime_schema.UniquePredicateOp) runtime_schema.RelationalCheckOp {
    return switch (op) {
        .is_null => .is_null,
        .is_not_null => .is_not_null,
        .eq => .eq,
        .ne => .ne,
    };
}

pub fn relationalCheckOpFromUniquePredicateToken(token: []const u8) ?runtime_schema.RelationalCheckOp {
    if (std.mem.eql(u8, token, "is_null")) return .is_null;
    if (std.mem.eql(u8, token, "is_not_null")) return .is_not_null;
    if (std.mem.eql(u8, token, "eq")) return .eq;
    if (std.mem.eql(u8, token, "ne")) return .ne;
    return null;
}

pub fn tokenKindIsJsonExtractOperator(kind: token_mod.TokenKind) bool {
    return kind == .arrow_json or kind == .arrow_text or kind == .path_arrow_json or kind == .path_arrow_text;
}

pub fn tokenKindIsJsonExtractTextOperator(kind: token_mod.TokenKind) bool {
    return kind == .arrow_text or kind == .path_arrow_text;
}

pub fn tokenKindIsJsonExtractPathOperator(kind: token_mod.TokenKind) bool {
    return kind == .path_arrow_json or kind == .path_arrow_text;
}

pub fn jsonExtractExpressionCanStartAt(tokens: []const Token, index: usize) bool {
    if (index + 2 >= tokens.len) return false;
    if (tokens[index].kind != .identifier) return false;
    if (!tokenKindIsJsonExtractOperator(tokens[index + 1].kind)) return false;
    return tokens[index + 2].kind == .string or tokens[index + 2].kind == .placeholder;
}

pub fn jsonExtractExpressionPredicateCanStartAt(tokens: []const Token, index: usize) bool {
    if (index + 3 >= tokens.len) return false;
    if (!jsonExtractExpressionCanStartAt(tokens, index)) return false;
    return switch (tokens[index + 3].kind) {
        .eq, .neq => true,
        else => false,
    };
}

pub fn jsonExtractNullSafeDistinctPredicateCanStartAt(tokens: []const Token, index: usize) bool {
    if (index + 5 >= tokens.len) return false;
    if (!jsonExtractExpressionCanStartAt(tokens, index)) return false;
    if (tokens[index + 3].kind != .identifier or
        !std.ascii.eqlIgnoreCase(tokens[index + 3].text, "is"))
    {
        return false;
    }
    var distinct_index = index + 4;
    if (tokens[distinct_index].kind == .identifier and
        std.ascii.eqlIgnoreCase(tokens[distinct_index].text, "not"))
    {
        distinct_index += 1;
    }
    return distinct_index + 1 < tokens.len and
        tokens[distinct_index].kind == .identifier and
        std.ascii.eqlIgnoreCase(tokens[distinct_index].text, "distinct") and
        tokens[distinct_index + 1].kind == .identifier and
        std.ascii.eqlIgnoreCase(tokens[distinct_index + 1].text, "from");
}

pub fn jsonExtractNullTestPredicateCanStartAt(tokens: []const Token, index: usize) bool {
    if (index + 4 >= tokens.len) return false;
    if (!jsonExtractExpressionCanStartAt(tokens, index)) return false;
    if (tokens[index + 3].kind != .identifier or
        !std.ascii.eqlIgnoreCase(tokens[index + 3].text, "is"))
    {
        return false;
    }
    var null_index = index + 4;
    if (tokens[null_index].kind == .identifier and
        std.ascii.eqlIgnoreCase(tokens[null_index].text, "not"))
    {
        null_index += 1;
    }
    return null_index < tokens.len and
        tokens[null_index].kind == .identifier and
        std.ascii.eqlIgnoreCase(tokens[null_index].text, "null");
}

pub fn jsonExtractMembershipPredicateCanStartAt(tokens: []const Token, index: usize) bool {
    if (index + 3 >= tokens.len) return false;
    if (!jsonExtractExpressionCanStartAt(tokens, index)) return false;
    const op = tokens[index + 3];
    if (op.kind == .identifier and std.ascii.eqlIgnoreCase(op.text, "in")) return true;
    if (op.kind == .identifier and std.ascii.eqlIgnoreCase(op.text, "not")) {
        return index + 4 < tokens.len and
            tokens[index + 4].kind == .identifier and
            std.ascii.eqlIgnoreCase(tokens[index + 4].text, "in");
    }
    if (op.kind == .eq or op.kind == .neq) {
        return index + 4 < tokens.len and
            tokens[index + 4].kind == .identifier and
            (sqlKeywordIsAnyOrSome(tokens[index + 4].text) or
                std.ascii.eqlIgnoreCase(tokens[index + 4].text, "all"));
    }
    return false;
}

pub fn jsonKeySetExpressionCanStartAt(tokens: []const Token, index: usize) bool {
    return index + 1 < tokens.len and
        tokens[index].kind == .identifier and
        (tokens[index + 1].kind == .question_any or tokens[index + 1].kind == .question_all);
}

pub fn jsonPathSegmentsToDottedPathAlloc(alloc: std.mem.Allocator, segments: []const []const u8) ![]const u8 {
    if (segments.len == 0) return error.UnsupportedSqlShape;
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    for (segments, 0..) |segment, i| {
        if (segment.len == 0 or std.mem.indexOfScalar(u8, segment, '.') != null) return error.UnsupportedSqlShape;
        if (i != 0) try out.append(alloc, '.');
        try out.appendSlice(alloc, segment);
    }
    return try out.toOwnedSlice(alloc);
}

pub fn parsePostgresJsonPathTextAlloc(alloc: std.mem.Allocator, path: []const u8) ![]const []const u8 {
    if (path.len < 3 or path[0] != '{' or path[path.len - 1] != '}') return error.UnsupportedSqlShape;
    const inner = path[1 .. path.len - 1];
    var out = std.ArrayListUnmanaged([]const u8).empty;
    errdefer {
        for (out.items) |segment| alloc.free(segment);
        out.deinit(alloc);
    }
    var parts = std.mem.splitScalar(u8, inner, ',');
    while (parts.next()) |part| {
        if (part.len == 0 or std.mem.indexOfScalar(u8, part, '.') != null) return error.UnsupportedSqlShape;
        const segment = try alloc.dupe(u8, part);
        var segment_transferred = false;
        errdefer if (!segment_transferred) alloc.free(segment);
        try out.append(alloc, segment);
        segment_transferred = true;
    }
    if (out.items.len == 0) return error.UnsupportedSqlShape;
    return try out.toOwnedSlice(alloc);
}

pub fn parsePostgresJsonPathJsonArrayAlloc(alloc: std.mem.Allocator, path_json: []const u8) ![]const []const u8 {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, path_json, .{}) catch return error.UnsupportedSqlShape;
    defer parsed.deinit();
    if (parsed.value != .array or parsed.value.array.items.len == 0) return error.UnsupportedSqlShape;
    var out = std.ArrayListUnmanaged([]const u8).empty;
    errdefer {
        for (out.items) |segment| alloc.free(segment);
        out.deinit(alloc);
    }
    for (parsed.value.array.items) |item| {
        if (item != .string or item.string.len == 0 or std.mem.indexOfScalar(u8, item.string, '.') != null) return error.UnsupportedSqlShape;
        const segment = try alloc.dupe(u8, item.string);
        var segment_transferred = false;
        errdefer if (!segment_transferred) alloc.free(segment);
        try out.append(alloc, segment);
        segment_transferred = true;
    }
    return try out.toOwnedSlice(alloc);
}

pub fn generatedUnaryTextColumnForField(
    schema: runtime_schema.TableSchema,
    op: runtime_schema.RelationalGeneratedOp,
    field: []const u8,
) ?runtime_schema.RelationalColumn {
    switch (op) {
        .lower, .upper, .md5 => {},
        .concat, .concat_ws, .expression => return null,
    }
    for (schema.relational_columns) |column| {
        const generated = column.generated orelse continue;
        if (generated.op != op) continue;
        const generated_field = generated.field orelse continue;
        if (std.mem.eql(u8, generated_field, field)) return column;
    }
    return null;
}

pub fn generatedConcatColumnAt(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: usize,
    schema: runtime_schema.TableSchema,
    field_expression_qualifiers: []const []const u8,
    returning_expression_qualifiers: []const []const u8,
    defer_row_expression_field_validation: bool,
) !?runtime_schema.RelationalColumn {
    for (schema.relational_columns) |column| {
        const generated = column.generated orelse continue;
        if (generated.op != .concat and generated.op != .concat_ws) continue;
        if (try concatCallMatchesGeneratedAt(
            alloc,
            tokens,
            pos,
            schema,
            generated,
            field_expression_qualifiers,
            returning_expression_qualifiers,
            defer_row_expression_field_validation,
        )) return column;
    }
    return null;
}

pub fn parseGeneratedFieldExpressionOwnedAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    schema: runtime_schema.TableSchema,
    field_expression_qualifiers: []const []const u8,
    returning_expression_qualifiers: []const []const u8,
    defer_row_expression_field_validation: bool,
) !?[]const u8 {
    if (pos.* + 1 >= tokens.len or tokens[pos.*].kind != .identifier or tokens[pos.* + 1].kind != .lparen) return null;

    if (generatedUnaryOpKeyword(tokens[pos.*].text)) |op| {
        pos.* += 2;
        const parsed_source = try parseIdentifierOwnedAlloc(alloc, tokens, pos);
        defer alloc.free(parsed_source);
        const source = try binder.normalizeRowExpressionFieldAlloc(
            alloc,
            schema,
            parsed_source,
            field_expression_qualifiers,
            returning_expression_qualifiers,
            defer_row_expression_field_validation,
        );
        defer alloc.free(source);
        if (binder.relationalColumnForField(schema, source, null) == null) return error.InvalidSqlCatalog;
        try parser.expectToken(tokens, pos, .rparen);
        const generated = generatedUnaryTextColumnForField(schema, op, source) orelse return error.UnsupportedSqlShape;
        return try alloc.dupe(u8, generated.name);
    }

    const is_concat = std.ascii.eqlIgnoreCase(tokens[pos.*].text, "concat");
    const is_concat_ws = std.ascii.eqlIgnoreCase(tokens[pos.*].text, "concat_ws");
    if (!is_concat and !is_concat_ws) return null;

    const generated = (try generatedConcatColumnAt(
        alloc,
        tokens,
        pos.*,
        schema,
        field_expression_qualifiers,
        returning_expression_qualifiers,
        defer_row_expression_field_validation,
    )) orelse return error.UnsupportedSqlShape;
    pos.* += 2;
    const generated_value = generated.generated orelse return error.UnsupportedSqlShape;
    if (generated_value.op == .concat_ws) {
        const separator = parser.matchToken(tokens, pos, .string) orelse return error.UnsupportedSqlShape;
        if (!std.mem.eql(u8, separator.text, generated_value.separator)) return error.UnsupportedSqlShape;
        if (parser.matchToken(tokens, pos, .comma) == null) return error.UnsupportedSqlShape;
    }
    for (generated_value.fields, 0..) |field, i| {
        const parsed_source = try parseIdentifierOwnedAlloc(alloc, tokens, pos);
        defer alloc.free(parsed_source);
        const source = try binder.normalizeRowExpressionFieldAlloc(
            alloc,
            schema,
            parsed_source,
            field_expression_qualifiers,
            returning_expression_qualifiers,
            defer_row_expression_field_validation,
        );
        defer alloc.free(source);
        if (!std.mem.eql(u8, source, field)) return error.UnsupportedSqlShape;
        if (i + 1 < generated_value.fields.len) {
            if (parser.matchToken(tokens, pos, .comma) == null) return error.UnsupportedSqlShape;
            if (generated_value.op == .concat) {
                const separator = parser.matchToken(tokens, pos, .string) orelse return error.UnsupportedSqlShape;
                if (!std.mem.eql(u8, separator.text, generated_value.separator)) return error.UnsupportedSqlShape;
                if (parser.matchToken(tokens, pos, .comma) == null) return error.UnsupportedSqlShape;
            }
        }
    }
    try parser.expectToken(tokens, pos, .rparen);
    return try alloc.dupe(u8, generated.name);
}

pub fn parseGeneratedFieldExpressionOrNullOwnedAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    schema: runtime_schema.TableSchema,
    field_expression_qualifiers: []const []const u8,
    returning_expression_qualifiers: []const []const u8,
    defer_row_expression_field_validation: bool,
) !?[]const u8 {
    const start_pos = pos.*;
    return parseGeneratedFieldExpressionOwnedAlloc(
        alloc,
        tokens,
        pos,
        schema,
        field_expression_qualifiers,
        returning_expression_qualifiers,
        defer_row_expression_field_validation,
    ) catch |err| switch (err) {
        error.InvalidSqlCatalog, error.UnsupportedSqlShape => {
            pos.* = start_pos;
            return null;
        },
        else => return err,
    };
}

pub fn parseRowExpressionFieldOwnedAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    schema: runtime_schema.TableSchema,
    field_expression_qualifiers: []const []const u8,
    returning_expression_qualifiers: []const []const u8,
    defer_row_expression_field_validation: bool,
) ![]const u8 {
    if (try parseGeneratedFieldExpressionOwnedAlloc(
        alloc,
        tokens,
        pos,
        schema,
        field_expression_qualifiers,
        returning_expression_qualifiers,
        defer_row_expression_field_validation,
    )) |field| return field;
    return try parseIdentifierOwnedAlloc(alloc, tokens, pos);
}

fn generatedUnaryOpKeyword(keyword: []const u8) ?runtime_schema.RelationalGeneratedOp {
    if (std.ascii.eqlIgnoreCase(keyword, "lower")) return .lower;
    if (std.ascii.eqlIgnoreCase(keyword, "upper")) return .upper;
    if (std.ascii.eqlIgnoreCase(keyword, "md5")) return .md5;
    return null;
}

fn parseIdentifierOwnedAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) ![]const u8 {
    const token = parser.matchToken(tokens, pos, .identifier) orelse return error.UnsupportedSqlShape;
    return try alloc.dupe(u8, token.text);
}

fn concatCallMatchesGeneratedAt(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: usize,
    schema: runtime_schema.TableSchema,
    generated: runtime_schema.RelationalGeneratedValue,
    field_expression_qualifiers: []const []const u8,
    returning_expression_qualifiers: []const []const u8,
    defer_row_expression_field_validation: bool,
) !bool {
    if (generated.fields.len == 0) return false;
    if (pos + 2 >= tokens.len) return false;
    if (tokens[pos].kind != .identifier) return false;
    if (generated.op == .concat and !std.ascii.eqlIgnoreCase(tokens[pos].text, "concat")) return false;
    if (generated.op == .concat_ws and !std.ascii.eqlIgnoreCase(tokens[pos].text, "concat_ws")) return false;
    if (tokens[pos + 1].kind != .lparen) return false;
    var i: usize = pos + 2;
    if (generated.op == .concat_ws) {
        if (i >= tokens.len or tokens[i].kind != .string) return false;
        if (!std.mem.eql(u8, tokens[i].text, generated.separator)) return false;
        i += 1;
        if (i >= tokens.len or tokens[i].kind != .comma) return false;
        i += 1;
    }
    for (generated.fields, 0..) |field, field_index| {
        if (i >= tokens.len or tokens[i].kind != .identifier) return false;
        const normalized_field = try binder.normalizeRowExpressionFieldAlloc(
            alloc,
            schema,
            tokens[i].text,
            field_expression_qualifiers,
            returning_expression_qualifiers,
            defer_row_expression_field_validation,
        );
        defer alloc.free(normalized_field);
        if (!std.mem.eql(u8, normalized_field, field)) return false;
        i += 1;
        if (field_index + 1 < generated.fields.len) {
            if (i >= tokens.len or tokens[i].kind != .comma) return false;
            i += 1;
            if (generated.op == .concat) {
                if (i >= tokens.len or tokens[i].kind != .string) return false;
                if (!std.mem.eql(u8, tokens[i].text, generated.separator)) return false;
                i += 1;
                if (i >= tokens.len or tokens[i].kind != .comma) return false;
                i += 1;
            }
        }
    }
    return i < tokens.len and tokens[i].kind == .rparen;
}

fn routineArgIndex(field: []const u8) ?usize {
    if (!std.mem.startsWith(u8, field, "arg") or field.len <= 3) return null;
    var index: usize = 0;
    for (field[3..]) |ch| {
        if (ch < '0' or ch > '9') return null;
        index = index * 10 + @as(usize, ch - '0');
    }
    if (index == 0) return null;
    return index - 1;
}

pub fn cloneExpressionSubstitutingRoutineArgsAlloc(
    alloc: std.mem.Allocator,
    value: runtime_schema.RelationalRowsExpression,
    args: []const runtime_schema.RelationalRowsExpression,
) anyerror!runtime_schema.RelationalRowsExpression {
    if (value.kind == .field) {
        if (routineArgIndex(value.field)) |index| {
            if (index >= args.len) return error.UnsupportedSqlShape;
            return try runtime_schema.cloneRelationalRowsExpressionAlloc(alloc, args[index]);
        }
    }

    var cloned: runtime_schema.RelationalRowsExpression = .{
        .kind = value.kind,
        .field_source = value.field_source,
        .cast_type = value.cast_type,
        .json_as_text = value.json_as_text,
    };
    errdefer runtime_schema.freeRelationalRowsExpression(alloc, cloned);

    if (value.field.len > 0) cloned.field = try alloc.dupe(u8, value.field);
    if (value.value_json.len > 0) cloned.value_json = try alloc.dupe(u8, value.value_json);
    if (value.json_path.len > 0) cloned.json_path = try alloc.dupe(u8, value.json_path);

    if (value.operands.len > 0) {
        const operands = try alloc.alloc(runtime_schema.RelationalRowsExpression, value.operands.len);
        var initialized: usize = 0;
        errdefer {
            for (operands[0..initialized]) |operand| runtime_schema.freeRelationalRowsExpression(alloc, operand);
            alloc.free(operands);
        }
        for (value.operands, 0..) |operand, i| {
            operands[i] = try cloneExpressionSubstitutingRoutineArgsAlloc(alloc, operand, args);
            initialized += 1;
        }
        cloned.operands = operands;
    }

    if (value.case_branches.len > 0) {
        const branches = try alloc.alloc(runtime_schema.RelationalRowsExpressionCaseBranch, value.case_branches.len);
        var initialized: usize = 0;
        errdefer {
            for (branches[0..initialized]) |branch| freeRoutineExpressionCaseBranch(alloc, branch);
            alloc.free(branches);
        }
        for (value.case_branches, 0..) |branch, i| {
            branches[i] = try cloneExpressionCaseBranchSubstitutingRoutineArgsAlloc(alloc, branch, args);
            initialized += 1;
        }
        cloned.case_branches = branches;
    }

    if (value.case_else.len > 0) {
        const fallback = try alloc.alloc(runtime_schema.RelationalRowsExpression, value.case_else.len);
        var initialized: usize = 0;
        errdefer {
            for (fallback[0..initialized]) |expression| runtime_schema.freeRelationalRowsExpression(alloc, expression);
            alloc.free(fallback);
        }
        for (value.case_else, 0..) |expression, i| {
            fallback[i] = try cloneExpressionSubstitutingRoutineArgsAlloc(alloc, expression, args);
            initialized += 1;
        }
        cloned.case_else = fallback;
    }

    return cloned;
}

fn cloneExpressionCaseBranchSubstitutingRoutineArgsAlloc(
    alloc: std.mem.Allocator,
    value: runtime_schema.RelationalRowsExpressionCaseBranch,
    args: []const runtime_schema.RelationalRowsExpression,
) anyerror!runtime_schema.RelationalRowsExpressionCaseBranch {
    const when = try cloneExpressionConditionSubstitutingRoutineArgsAlloc(alloc, value.when, args);
    var when_transferred = false;
    errdefer if (!when_transferred) runtime_schema.freeRelationalRowsExpressionCondition(alloc, when);
    const then_expression = try cloneExpressionSubstitutingRoutineArgsAlloc(alloc, value.then, args);
    when_transferred = true;
    return .{
        .when = when,
        .then = then_expression,
    };
}

fn cloneExpressionConditionSubstitutingRoutineArgsAlloc(
    alloc: std.mem.Allocator,
    value: runtime_schema.RelationalRowsExpressionCondition,
    args: []const runtime_schema.RelationalRowsExpression,
) anyerror!runtime_schema.RelationalRowsExpressionCondition {
    const lhs = try cloneExpressionSubstitutingRoutineArgsAlloc(alloc, value.lhs, args);
    var lhs_transferred = false;
    errdefer if (!lhs_transferred) runtime_schema.freeRelationalRowsExpression(alloc, lhs);

    const rhs = if (value.rhs.len > 0) blk: {
        const out = try alloc.alloc(runtime_schema.RelationalRowsExpression, value.rhs.len);
        var initialized: usize = 0;
        errdefer {
            for (out[0..initialized]) |expression| runtime_schema.freeRelationalRowsExpression(alloc, expression);
            alloc.free(out);
        }
        for (value.rhs, 0..) |expression, i| {
            out[i] = try cloneExpressionSubstitutingRoutineArgsAlloc(alloc, expression, args);
            initialized += 1;
        }
        break :blk out;
    } else &.{};

    lhs_transferred = true;
    return .{
        .lhs = lhs,
        .op = value.op,
        .rhs = rhs,
    };
}

fn freeRoutineExpressionCaseBranch(
    alloc: std.mem.Allocator,
    value: runtime_schema.RelationalRowsExpressionCaseBranch,
) void {
    runtime_schema.freeRelationalRowsExpressionCondition(alloc, value.when);
    runtime_schema.freeRelationalRowsExpression(alloc, value.then);
}

pub fn relationalRowsExpressionOptionalEqual(
    lhs: ?runtime_schema.RelationalRowsExpression,
    rhs: ?runtime_schema.RelationalRowsExpression,
) bool {
    if (lhs == null or rhs == null) return lhs == null and rhs == null;
    return relationalRowsExpressionEqual(lhs.?, rhs.?);
}

pub fn relationalRowsExpressionEqual(
    lhs: runtime_schema.RelationalRowsExpression,
    rhs: runtime_schema.RelationalRowsExpression,
) bool {
    if (lhs.kind != rhs.kind or
        lhs.field_source != rhs.field_source or
        lhs.json_as_text != rhs.json_as_text or
        lhs.cast_type != rhs.cast_type or
        !std.mem.eql(u8, lhs.field, rhs.field) or
        !std.mem.eql(u8, lhs.value_json, rhs.value_json) or
        !std.mem.eql(u8, lhs.json_path, rhs.json_path) or
        lhs.operands.len != rhs.operands.len or
        lhs.case_branches.len != rhs.case_branches.len or
        lhs.case_else.len != rhs.case_else.len)
    {
        return false;
    }
    for (lhs.operands, rhs.operands) |lhs_operand, rhs_operand| {
        if (!relationalRowsExpressionEqual(lhs_operand, rhs_operand)) return false;
    }
    for (lhs.case_branches, rhs.case_branches) |lhs_branch, rhs_branch| {
        if (!relationalRowsExpressionConditionEqual(lhs_branch.when, rhs_branch.when)) return false;
        if (!relationalRowsExpressionEqual(lhs_branch.then, rhs_branch.then)) return false;
    }
    for (lhs.case_else, rhs.case_else) |lhs_else, rhs_else| {
        if (!relationalRowsExpressionEqual(lhs_else, rhs_else)) return false;
    }
    return true;
}

pub fn relationalRowsExpressionConditionsEqual(
    lhs: []const runtime_schema.RelationalRowsExpressionCondition,
    rhs: []const runtime_schema.RelationalRowsExpressionCondition,
) bool {
    if (lhs.len != rhs.len) return false;
    for (lhs, rhs) |lhs_condition, rhs_condition| {
        if (!relationalRowsExpressionConditionEqual(lhs_condition, rhs_condition)) return false;
    }
    return true;
}

pub fn relationalRowsExpressionArrayContainsEqual(
    lhs: []const runtime_schema.RelationalRowsExpressionArrayContainsPredicate,
    rhs: []const runtime_schema.RelationalRowsExpressionArrayContainsPredicate,
) bool {
    if (lhs.len != rhs.len) return false;
    for (lhs, rhs) |lhs_predicate, rhs_predicate| {
        if (!relationalRowsExpressionEqual(lhs_predicate.expression, rhs_predicate.expression) or
            !std.mem.eql(u8, lhs_predicate.value_json, rhs_predicate.value_json))
        {
            return false;
        }
    }
    return true;
}

pub fn relationalRowsExpressionPredicateGroupsEqual(
    lhs: []const runtime_schema.RelationalRowsExpressionPredicateGroup,
    rhs: []const runtime_schema.RelationalRowsExpressionPredicateGroup,
) bool {
    if (lhs.len != rhs.len) return false;
    for (lhs, rhs) |lhs_group, rhs_group| {
        if (!relationalRowsExpressionConditionsEqual(lhs_group.conditions, rhs_group.conditions)) return false;
    }
    return true;
}

pub fn relationalRowsExpressionConditionEqual(
    lhs: runtime_schema.RelationalRowsExpressionCondition,
    rhs: runtime_schema.RelationalRowsExpressionCondition,
) bool {
    if (lhs.op != rhs.op or lhs.rhs.len != rhs.rhs.len or !relationalRowsExpressionEqual(lhs.lhs, rhs.lhs)) return false;
    for (lhs.rhs, rhs.rhs) |lhs_rhs, rhs_rhs| {
        if (!relationalRowsExpressionEqual(lhs_rhs, rhs_rhs)) return false;
    }
    return true;
}

pub fn aggregateSpecsEquivalent(
    lhs: db_mod.types.RelationalRowsAggregateSpec,
    rhs: db_mod.types.RelationalRowsAggregateSpec,
) bool {
    if (lhs.op != rhs.op or lhs.distinct != rhs.distinct) return false;
    if (lhs.percentile_max_items != rhs.percentile_max_items) return false;
    if (lhs.percentile_order != rhs.percentile_order) return false;
    if (lhs.percentile == null or rhs.percentile == null) {
        if (lhs.percentile != null or rhs.percentile != null) return false;
    } else if (lhs.percentile.? != rhs.percentile.?) return false;
    if (!floatSlicesEqual(lhs.percentiles, rhs.percentiles)) return false;
    if (!optionalStringEqual(lhs.field, rhs.field)) return false;
    if (!optionalStringEqual(lhs.string_delimiter, rhs.string_delimiter)) return false;
    if (!relationalRowsExpressionOptionalEqual(lhs.expression, rhs.expression)) return false;
    if (!orderByEqual(lhs.array_order_by, rhs.array_order_by)) return false;
    if (!relationalChecksEqual(lhs.filter_predicates, rhs.filter_predicates)) return false;
    if (!structuredValuePredicatesEqual(lhs.filter_array_any, rhs.filter_array_any)) return false;
    if (!structuredValuePredicatesEqual(lhs.filter_array_contains, rhs.filter_array_contains)) return false;
    if (!structuredValuePredicatesEqual(lhs.filter_array_eq, rhs.filter_array_eq)) return false;
    if (!inPredicatesEqual(lhs.filter_in_predicates, rhs.filter_in_predicates)) return false;
    if (!jsonContainsEqual(lhs.filter_json_contains, rhs.filter_json_contains)) return false;
    if (!jsonPathEqEqual(lhs.filter_json_path_eq, rhs.filter_json_path_eq)) return false;
    if (!jsonPathExistsEqual(lhs.filter_json_path_exists, rhs.filter_json_path_exists)) return false;
    if (!textPatternsEqual(lhs.filter_text_patterns, rhs.filter_text_patterns)) return false;
    if (!relationalRowsExpressionConditionsEqual(lhs.filter_expressions, rhs.filter_expressions)) return false;
    if (!relationalRowsExpressionArrayContainsEqual(lhs.filter_expression_array_contains, rhs.filter_expression_array_contains)) return false;
    if (!relationalRowsExpressionPredicateGroupsEqual(lhs.filter_any, rhs.filter_any)) return false;
    if (!relationalRowsExpressionPredicateGroupsEqual(lhs.filter_not, rhs.filter_not)) return false;
    return true;
}

pub fn aggregateOpForName(name: []const u8) ?db_mod.types.RelationalRowsAggregateOp {
    if (std.ascii.eqlIgnoreCase(name, "count")) return .count;
    if (std.ascii.eqlIgnoreCase(name, "sum")) return .sum;
    if (std.ascii.eqlIgnoreCase(name, "min")) return .min;
    if (std.ascii.eqlIgnoreCase(name, "max")) return .max;
    if (std.ascii.eqlIgnoreCase(name, "avg")) return .avg;
    if (std.ascii.eqlIgnoreCase(name, "percentile_cont")) return .percentile_cont;
    if (std.ascii.eqlIgnoreCase(name, "percentile_disc")) return .percentile_disc;
    if (std.ascii.eqlIgnoreCase(name, "mode")) return .mode;
    if (std.ascii.eqlIgnoreCase(name, "array_agg")) return .array_agg;
    if (std.ascii.eqlIgnoreCase(name, "string_agg")) return .string_agg;
    if (std.ascii.eqlIgnoreCase(name, "bool_or")) return .bool_or;
    if (std.ascii.eqlIgnoreCase(name, "bool_and")) return .bool_and;
    return null;
}

pub fn aggregateOpName(op: db_mod.types.RelationalRowsAggregateOp) []const u8 {
    return switch (op) {
        .count => "count",
        .sum => "sum",
        .min => "min",
        .max => "max",
        .avg => "avg",
        .percentile_cont => "percentile_cont",
        .percentile_disc => "percentile_disc",
        .mode => "mode",
        .array_agg => "array_agg",
        .string_agg => "string_agg",
        .bool_or => "bool_or",
        .bool_and => "bool_and",
    };
}

pub fn validateSqlAggregatePercentile(percentile: f64) !void {
    if (!std.math.isFinite(percentile) or percentile < 0 or percentile > 1) return error.UnsupportedSqlShape;
}

pub fn aggregateAliasOrDefaultAlloc(
    alloc: std.mem.Allocator,
    explicit_alias: ?[]const u8,
    op: db_mod.types.RelationalRowsAggregateOp,
    field: ?[]const u8,
) ![]const u8 {
    if (explicit_alias) |alias| return try alloc.dupe(u8, alias);
    if (field) |field_name| return try std.fmt.allocPrint(alloc, "{s}_{s}", .{ aggregateOpName(op), field_name });
    return try alloc.dupe(u8, aggregateOpName(op));
}

pub fn isSqlPercentileAggregateOp(op: db_mod.types.RelationalRowsAggregateOp) bool {
    return op == .percentile_cont or op == .percentile_disc;
}

pub fn sqlJsonNumberAsF64(value: std.json.Value) ?f64 {
    return switch (value) {
        .integer => |number| @floatFromInt(number),
        .float => |number| number,
        .number_string => |number| std.fmt.parseFloat(f64, number) catch null,
        else => null,
    };
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

pub fn validateAggregateGroupBy(
    group_fields: []const []const u8,
    group_expressions: []const db_mod.types.RelationalRowsExpressionProjection,
    group_by: []const []const u8,
    parsed_group_expressions: []const db_mod.types.RelationalRowsExpressionProjection,
) !void {
    if (!stringSlicesEqual(group_fields, group_by)) return error.UnsupportedSqlShape;
    if (group_expressions.len != parsed_group_expressions.len) return error.UnsupportedSqlShape;
    for (group_expressions, parsed_group_expressions) |selected, parsed| {
        if (!relationalRowsExpressionEqual(selected.expression, parsed.expression)) return error.UnsupportedSqlShape;
    }
}

pub fn aggregateOutputFieldIsUnique(
    group_fields: []const []const u8,
    group_expressions: []const db_mod.types.RelationalRowsExpressionProjection,
    aggregations: []const db_mod.types.RelationalRowsAggregateSpec,
    field: []const u8,
) bool {
    var matches: usize = 0;
    for (group_fields) |group_field| {
        if (std.mem.eql(u8, group_field, field)) matches += 1;
    }
    for (group_expressions) |projection| {
        if (std.mem.eql(u8, projection.output, field)) matches += 1;
    }
    for (aggregations) |aggregation| {
        if (std.mem.eql(u8, aggregation.name, field)) matches += 1;
    }
    return matches == 1;
}

pub fn aggregateOutputColumnExists(columns: []const runtime_schema.RelationalColumn, name: []const u8) bool {
    for (columns) |column| {
        if (std.mem.eql(u8, column.name, name)) return true;
    }
    return false;
}

pub fn windowOutputFieldByOrdinalAlloc(
    alloc: std.mem.Allocator,
    select: plan_mod.WindowSelectList,
    ordinal: u32,
) ![]const u8 {
    if (ordinal == 0) return error.UnsupportedSqlShape;
    const index: usize = @intCast(ordinal - 1);
    if (index >= select.outputs.len) return error.UnsupportedSqlShape;
    const output = select.outputs[index];
    return switch (output.kind) {
        .field => try alloc.dupe(u8, select.fields[output.index]),
        .window => try alloc.dupe(u8, select.windows[output.index].output),
    };
}

pub fn windowOutputColumnsAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    type_context: RowExpressionTypeContext,
    select: plan_mod.WindowSelectList,
) ![]runtime_schema.RelationalColumn {
    const total = select.fields.len + select.windows.len;
    if (total == 0) return &.{};
    const out = try alloc.alloc(runtime_schema.RelationalColumn, total);
    var initialized: usize = 0;
    errdefer alloc.free(out);
    for (select.fields) |field| {
        if (aggregateOutputColumnExists(out[0..initialized], field)) return error.UnsupportedSqlShape;
        const column = binder.relationalColumnForField(schema, field, null) orelse return error.InvalidSqlCatalog;
        out[initialized] = .{
            .name = field,
            .path = field,
            .field_type = column.field_type,
            .array_item_type = column.array_item_type,
            .nullable = column.nullable,
        };
        initialized += 1;
    }
    for (select.windows) |window| {
        if (aggregateOutputColumnExists(out[0..initialized], window.output)) return error.UnsupportedSqlShape;
        const value_type = if (window.value_expression) |expression| try type_context.rowExpressionOutputType(expression) else null;
        out[initialized] = .{
            .name = window.output,
            .path = window.output,
            .field_type = try windowOutputType(window.function, value_type),
            .nullable = true,
        };
        initialized += 1;
    }
    return out;
}

pub fn aggregateOutputColumnForFieldAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    type_context: RowExpressionTypeContext,
    group_fields: []const []const u8,
    group_expressions: []const db_mod.types.RelationalRowsExpressionProjection,
    aggregations: []const db_mod.types.RelationalRowsAggregateSpec,
    field: []const u8,
) !runtime_schema.RelationalColumn {
    const output_columns = try aggregateOutputColumnsAlloc(alloc, schema, type_context, group_fields, group_expressions, aggregations);
    defer if (output_columns.len > 0) alloc.free(output_columns);
    for (output_columns) |column| {
        if (std.mem.eql(u8, column.name, field)) return column;
    }
    return error.UnsupportedSqlShape;
}

pub fn aggregateOutputColumnsAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    type_context: RowExpressionTypeContext,
    group_fields: []const []const u8,
    group_expressions: []const db_mod.types.RelationalRowsExpressionProjection,
    aggregations: []const db_mod.types.RelationalRowsAggregateSpec,
) ![]runtime_schema.RelationalColumn {
    const total = group_fields.len + group_expressions.len + aggregations.len;
    if (total == 0) return &.{};
    const out = try alloc.alloc(runtime_schema.RelationalColumn, total);
    var initialized: usize = 0;
    errdefer alloc.free(out);
    for (group_fields) |field| {
        if (aggregateOutputColumnExists(out[0..initialized], field)) return error.UnsupportedSqlShape;
        const column = binder.relationalColumnForField(schema, field, null) orelse return error.InvalidSqlCatalog;
        out[initialized] = .{
            .name = field,
            .path = field,
            .field_type = column.field_type,
            .array_item_type = column.array_item_type,
            .nullable = column.nullable,
        };
        initialized += 1;
    }
    for (group_expressions) |projection| {
        if (aggregateOutputColumnExists(out[0..initialized], projection.output)) return error.UnsupportedSqlShape;
        out[initialized] = .{
            .name = projection.output,
            .path = projection.output,
            .field_type = try type_context.rowExpressionOutputType(projection.expression),
            .nullable = true,
        };
        initialized += 1;
    }
    for (aggregations) |aggregation| {
        if (aggregateOutputColumnExists(out[0..initialized], aggregation.name)) return error.UnsupportedSqlShape;
        const input_type = if (aggregation.field != null or aggregation.expression != null)
            try aggregateInputType(schema, type_context, aggregation)
        else
            null;
        const projected_type = try aggregateOutputProjectedType(aggregation, input_type);
        out[initialized] = .{
            .name = aggregation.name,
            .path = aggregation.name,
            .field_type = projected_type.field_type,
            .array_item_type = projected_type.array_item_type,
            .nullable = false,
        };
        initialized += 1;
    }
    return out;
}

pub fn aggregateInputType(
    schema: runtime_schema.TableSchema,
    type_context: RowExpressionTypeContext,
    aggregation: db_mod.types.RelationalRowsAggregateSpec,
) !runtime_schema.AntflyType {
    if (aggregation.field) |field| {
        const column = binder.relationalColumnForField(schema, field, null) orelse return error.InvalidSqlCatalog;
        return column.field_type;
    }
    if (aggregation.expression) |expression| {
        return try type_context.rowExpressionOutputType(expression);
    }
    return error.UnsupportedSqlShape;
}

pub fn selectListOutputCount(
    fields: []const []const u8,
    json_extract: []const db_mod.types.RelationalRowsJsonExtractProjection,
    array_length: []const db_mod.types.RelationalRowsArrayLengthProjection,
    coalesce: []const db_mod.types.RelationalRowsCoalesceProjection,
    field_aliases: []const db_mod.types.RelationalRowsFieldAliasProjection,
    expressions: []const db_mod.types.RelationalRowsExpressionProjection,
    output: []const u8,
) usize {
    var count: usize = 0;
    for (fields) |field| {
        if (std.mem.eql(u8, field, output)) count += 1;
    }
    for (json_extract) |projection| {
        if (std.mem.eql(u8, projection.output, output)) count += 1;
    }
    for (array_length) |projection| {
        if (std.mem.eql(u8, projection.output, output)) count += 1;
    }
    for (coalesce) |projection| {
        if (std.mem.eql(u8, projection.output, output)) count += 1;
    }
    for (field_aliases) |projection| {
        if (std.mem.eql(u8, projection.output, output)) count += 1;
    }
    for (expressions) |projection| {
        if (std.mem.eql(u8, projection.output, output)) count += 1;
    }
    return count;
}

pub fn validateSelectListOutputs(
    schema: runtime_schema.TableSchema,
    select_all: bool,
    fields: []const []const u8,
    json_extract: []const db_mod.types.RelationalRowsJsonExtractProjection,
    array_length: []const db_mod.types.RelationalRowsArrayLengthProjection,
    coalesce: []const db_mod.types.RelationalRowsCoalesceProjection,
    field_aliases: []const db_mod.types.RelationalRowsFieldAliasProjection,
    expressions: []const db_mod.types.RelationalRowsExpressionProjection,
) !void {
    if (!select_all) {
        for (fields) |field| {
            if (field.len == 0) return error.UnsupportedSqlShape;
            if (selectListOutputCount(fields, json_extract, array_length, coalesce, field_aliases, expressions, field) > 1) return error.UnsupportedSqlShape;
        }
    }
    for (json_extract) |projection| try validateSelectProjectionOutput(schema, select_all, fields, json_extract, array_length, coalesce, field_aliases, expressions, projection.output);
    for (array_length) |projection| try validateSelectProjectionOutput(schema, select_all, fields, json_extract, array_length, coalesce, field_aliases, expressions, projection.output);
    for (coalesce) |projection| try validateSelectProjectionOutput(schema, select_all, fields, json_extract, array_length, coalesce, field_aliases, expressions, projection.output);
    for (field_aliases) |projection| try validateSelectProjectionOutput(schema, select_all, fields, json_extract, array_length, coalesce, field_aliases, expressions, projection.output);
    for (expressions) |projection| try validateSelectProjectionOutput(schema, select_all, fields, json_extract, array_length, coalesce, field_aliases, expressions, projection.output);
}

fn validateSelectProjectionOutput(
    schema: runtime_schema.TableSchema,
    select_all: bool,
    fields: []const []const u8,
    json_extract: []const db_mod.types.RelationalRowsJsonExtractProjection,
    array_length: []const db_mod.types.RelationalRowsArrayLengthProjection,
    coalesce: []const db_mod.types.RelationalRowsCoalesceProjection,
    field_aliases: []const db_mod.types.RelationalRowsFieldAliasProjection,
    expressions: []const db_mod.types.RelationalRowsExpressionProjection,
    output: []const u8,
) !void {
    if (output.len == 0) return error.UnsupportedSqlShape;
    if (select_all and binder.relationalColumnForField(schema, output, null) != null) return error.UnsupportedSqlShape;
    if (selectListOutputCount(fields, json_extract, array_length, coalesce, field_aliases, expressions, output) > 1) return error.UnsupportedSqlShape;
}

pub fn selectOutputColumnsAlloc(
    alloc: std.mem.Allocator,
    type_context: RowExpressionTypeContext,
    select: plan_mod.SelectList,
) ![]runtime_schema.RelationalColumn {
    if (select.select_all) return error.UnsupportedSqlShape;
    if (select.outputs.len == 0) return &.{};
    const out = try alloc.alloc(runtime_schema.RelationalColumn, select.outputs.len);
    var initialized: usize = 0;
    errdefer {
        ddl_plan.clearDdlRelationalColumns(alloc, out[0..initialized]);
        alloc.free(out);
    }
    for (select.outputs) |output| {
        const column = try selectOutputColumnAlloc(alloc, type_context, select, output);
        var column_transferred = false;
        errdefer if (!column_transferred) ddl_plan.freeDdlRelationalColumn(alloc, column);
        if (aggregateOutputColumnExists(out[0..initialized], column.name)) return error.UnsupportedSqlShape;
        out[initialized] = column;
        column_transferred = true;
        initialized += 1;
    }
    return out;
}

pub fn loweredSelectOutputColumnsAlloc(
    alloc: std.mem.Allocator,
    type_context: RowExpressionTypeContext,
    lowered: plan_mod.LoweredSelect,
) ![]runtime_schema.RelationalColumn {
    return try selectOutputColumnsAlloc(alloc, type_context, .{
        .fields = lowered.query.select,
        .json_extract = lowered.query.json_extract,
        .array_length = lowered.query.array_length,
        .coalesce = lowered.query.coalesce,
        .field_aliases = lowered.query.field_aliases,
        .expressions = lowered.query.expressions,
        .outputs = lowered.select_outputs,
        .select_all = lowered.query.select_all,
    });
}

fn selectOutputColumnAlloc(
    alloc: std.mem.Allocator,
    type_context: RowExpressionTypeContext,
    select: plan_mod.SelectList,
    output: ast.SelectOutputRef,
) !runtime_schema.RelationalColumn {
    return switch (output.kind) {
        .field => blk: {
            if (output.index >= select.fields.len) return error.UnsupportedSqlShape;
            const field = select.fields[output.index];
            const source = binder.relationalColumnForField(type_context.schema, field, null) orelse return error.InvalidSqlCatalog;
            break :blk try projectedColumnAlloc(alloc, field, source.field_type, source.array_item_type, source.nullable);
        },
        .json_extract => blk: {
            if (output.index >= select.json_extract.len) return error.UnsupportedSqlShape;
            const projection = select.json_extract[output.index];
            const field_type: runtime_schema.AntflyType = if (projection.as_text) .keyword else .json;
            break :blk try projectedColumnAlloc(alloc, projection.output, field_type, null, true);
        },
        .array_length => blk: {
            if (output.index >= select.array_length.len) return error.UnsupportedSqlShape;
            const projection = select.array_length[output.index];
            break :blk try projectedColumnAlloc(alloc, projection.output, .numeric, null, true);
        },
        .coalesce => blk: {
            if (output.index >= select.coalesce.len) return error.UnsupportedSqlShape;
            const projection = select.coalesce[output.index];
            const field_type = try coalesceOutputType(alloc, type_context.schema.relational_columns, projection);
            break :blk try projectedColumnAlloc(alloc, projection.output, field_type.field_type, field_type.array_item_type, true);
        },
        .field_alias => blk: {
            if (output.index >= select.field_aliases.len) return error.UnsupportedSqlShape;
            const projection = select.field_aliases[output.index];
            const source = binder.relationalColumnForField(type_context.schema, projection.field, null) orelse return error.InvalidSqlCatalog;
            break :blk try projectedColumnAlloc(alloc, projection.output, source.field_type, source.array_item_type, source.nullable);
        },
        .expression => blk: {
            if (output.index >= select.expressions.len) return error.UnsupportedSqlShape;
            const projection = select.expressions[output.index];
            break :blk try projectedColumnAlloc(alloc, projection.output, try type_context.rowExpressionOutputType(projection.expression), null, true);
        },
    };
}

pub fn validateWindowSelectListOutputs(
    fields: []const []const u8,
    windows: []const db_mod.types.RelationalRowsWindowSpec,
) !void {
    for (fields, 0..) |field, i| {
        for (fields[i + 1 ..]) |other| {
            if (std.mem.eql(u8, field, other)) return error.UnsupportedSqlShape;
        }
        for (windows) |window| {
            if (std.mem.eql(u8, field, window.output)) return error.UnsupportedSqlShape;
        }
    }
    for (windows, 0..) |window, i| {
        for (windows[i + 1 ..]) |other| {
            if (std.mem.eql(u8, window.output, other.output)) return error.UnsupportedSqlShape;
        }
    }
}

pub fn validateAggregateSelectListOutputs(
    group_fields: []const []const u8,
    group_expressions: []const db_mod.types.RelationalRowsExpressionProjection,
    aggregations: []const db_mod.types.RelationalRowsAggregateSpec,
) !void {
    for (group_fields, 0..) |field, i| {
        for (group_fields[i + 1 ..]) |other| {
            if (std.mem.eql(u8, field, other)) return error.UnsupportedSqlShape;
        }
        for (group_expressions) |projection| {
            if (std.mem.eql(u8, field, projection.output)) return error.UnsupportedSqlShape;
        }
        for (aggregations) |aggregation| {
            if (std.mem.eql(u8, field, aggregation.name)) return error.UnsupportedSqlShape;
        }
    }
    for (group_expressions, 0..) |projection, i| {
        for (group_expressions[i + 1 ..]) |other| {
            if (std.mem.eql(u8, projection.output, other.output)) return error.UnsupportedSqlShape;
        }
        for (aggregations) |aggregation| {
            if (std.mem.eql(u8, projection.output, aggregation.name)) return error.UnsupportedSqlShape;
        }
    }
    for (aggregations, 0..) |aggregation, i| {
        for (aggregations[i + 1 ..]) |other| {
            if (std.mem.eql(u8, aggregation.name, other.name)) return error.UnsupportedSqlShape;
        }
    }
}

pub fn validateReturningProjectionOutputs(
    schema: runtime_schema.TableSchema,
    fields: []const []const u8,
    expressions: []const db_mod.types.RelationalRowsExpressionProjection,
) !void {
    const returning_all = fields.len == 1 and std.mem.eql(u8, fields[0], "*");
    if (returning_all) {
        for (expressions) |projection| {
            if (projection.output.len == 0) return error.UnsupportedSqlShape;
            if (binder.relationalColumnForReturningField(schema, projection.output) != null) return error.UnsupportedSqlShape;
            if (returningExpressionOutputCount(expressions, projection.output) > 1) return error.UnsupportedSqlShape;
        }
        return;
    }
    for (fields) |field| {
        if (field.len == 0 or std.mem.eql(u8, field, "*")) return error.UnsupportedSqlShape;
        if (returningFieldOutputCount(fields, field) > 1) return error.UnsupportedSqlShape;
        if (returningExpressionOutputCount(expressions, field) > 0) return error.UnsupportedSqlShape;
    }
    for (expressions) |projection| {
        if (projection.output.len == 0) return error.UnsupportedSqlShape;
        if (returningExpressionOutputCount(expressions, projection.output) > 1) return error.UnsupportedSqlShape;
        if (returningFieldOutputCount(fields, projection.output) > 0) return error.UnsupportedSqlShape;
    }
}

pub fn parseReturningProjectionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    schema: runtime_schema.TableSchema,
    returning_qualifiers: []const []const u8,
    hooks: ReturningProjectionParserHooks,
) !plan_mod.ReturningProjection {
    var saw_all = false;
    var fields = std.ArrayListUnmanaged([]const u8).empty;
    errdefer {
        for (fields.items) |field| alloc.free(field);
        fields.deinit(alloc);
    }
    var expressions = std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionProjection).empty;
    errdefer {
        for (expressions.items) |projection| freeExpressionProjection(alloc, projection);
        expressions.deinit(alloc);
    }

    while (true) {
        const returning_all_item = parser.matchToken(tokens, pos, .star) != null or try binder.matchQualifiedReturningAll(alloc, tokens, pos, returning_qualifiers);
        if (returning_all_item) {
            if (saw_all or fields.items.len != 0 or expressions.items.len != 0) return error.UnsupportedSqlShape;
            const all_field = try alloc.dupe(u8, "*");
            var all_field_transferred = false;
            errdefer if (!all_field_transferred) alloc.free(all_field);
            try fields.append(alloc, all_field);
            all_field_transferred = true;
            saw_all = true;
            if (parser.matchToken(tokens, pos, .comma) == null) break;
            continue;
        }
        if (peekSimpleReturningField(tokens, pos.*)) {
            const parsed_field = try hooks.parse_field_expression_owned(hooks.ptr);
            defer alloc.free(parsed_field);
            const field = try binder.normalizeReturningFieldAlloc(alloc, schema, parsed_field, returning_qualifiers);
            var field_owned = true;
            errdefer if (field_owned) alloc.free(field);
            const alias = try grammar.parseOptionalProjectionAliasAlloc(alloc, tokens, pos);
            var alias_owned = true;
            errdefer if (alias_owned) if (alias) |owned| alloc.free(owned);
            if (alias) |output| {
                if (std.mem.eql(u8, output, field)) {
                    alloc.free(output);
                    alias_owned = false;
                    try fields.append(alloc, field);
                    field_owned = false;
                } else {
                    if (saw_all and binder.relationalColumnForReturningField(schema, output) != null) return error.UnsupportedSqlShape;
                    try expressions.append(alloc, .{
                        .output = output,
                        .expression = .{
                            .kind = .field,
                            .field = field,
                        },
                    });
                    alias_owned = false;
                    field_owned = false;
                }
            } else {
                if (saw_all) return error.UnsupportedSqlShape;
                try fields.append(alloc, field);
                field_owned = false;
            }
            if (parser.matchToken(tokens, pos, .comma) == null) break;
            continue;
        }
        const item = try hooks.parse_select_item(hooks.ptr, returning_qualifiers);
        var item_owned = true;
        errdefer if (item_owned) plan_mod.freeSelectItem(alloc, item);
        switch (item) {
            .field => |parsed_field| {
                if (saw_all) return error.UnsupportedSqlShape;
                const field = try binder.normalizeReturningFieldAlloc(alloc, schema, parsed_field, returning_qualifiers);
                var field_owned = true;
                errdefer if (field_owned) alloc.free(field);
                alloc.free(parsed_field);
                try fields.append(alloc, field);
                field_owned = false;
                item_owned = false;
            },
            .field_alias => |projection| {
                const field = try binder.normalizeReturningFieldAlloc(alloc, schema, projection.field, returning_qualifiers);
                var field_owned = true;
                errdefer if (field_owned) alloc.free(field);
                if (saw_all and binder.relationalColumnForReturningField(schema, projection.output) != null) return error.UnsupportedSqlShape;
                try expressions.append(alloc, .{
                    .output = projection.output,
                    .expression = .{
                        .kind = .field,
                        .field = field,
                    },
                });
                alloc.free(projection.field);
                field_owned = false;
                item_owned = false;
            },
            .expression => |projection| {
                if (saw_all and binder.relationalColumnForReturningField(schema, projection.output) != null) return error.UnsupportedSqlShape;
                try expressions.append(alloc, projection);
                item_owned = false;
            },
            .coalesce => |projection| {
                if (saw_all and binder.relationalColumnForReturningField(schema, projection.output) != null) return error.UnsupportedSqlShape;
                const expression_projection = try plan_mod.expressionProjectionFromCoalesceAlloc(alloc, projection);
                var expression_projection_owned = true;
                errdefer if (expression_projection_owned) freeExpressionProjection(alloc, expression_projection);
                try expressions.append(alloc, expression_projection);
                expression_projection_owned = false;
                plan_mod.freeCoalesceProjection(alloc, projection);
                item_owned = false;
            },
            else => return error.UnsupportedSqlShape,
        }
        if (parser.matchToken(tokens, pos, .comma) == null) break;
    }

    const owned_fields = try fields.toOwnedSlice(alloc);
    var fields_owned = true;
    errdefer if (fields_owned) strings.freeStringSlice(alloc, owned_fields);
    const owned_expressions = try expressions.toOwnedSlice(alloc);
    errdefer freeExpressionProjections(alloc, owned_expressions);
    try validateReturningProjectionOutputs(schema, owned_fields, owned_expressions);
    fields_owned = false;
    return .{
        .fields = owned_fields,
        .expressions = owned_expressions,
    };
}

pub fn parseJoinedMutationReturningProjectionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    schema: runtime_schema.TableSchema,
    joined_source_schema: ?runtime_schema.TableSchema,
    source_alias: []const u8,
    returning_qualifiers: []const []const u8,
    hooks: JoinedMutationReturningProjectionParserHooks,
) !plan_mod.ReturningProjection {
    var saw_all = false;
    var fields = std.ArrayListUnmanaged([]const u8).empty;
    errdefer {
        for (fields.items) |field| alloc.free(field);
        fields.deinit(alloc);
    }
    var expressions = std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionProjection).empty;
    errdefer {
        for (expressions.items) |projection| freeExpressionProjection(alloc, projection);
        expressions.deinit(alloc);
    }

    while (true) {
        const returning_all_item = parser.matchToken(tokens, pos, .star) != null or try binder.matchQualifiedReturningAll(alloc, tokens, pos, returning_qualifiers);
        if (returning_all_item) {
            if (saw_all or fields.items.len != 0 or expressions.items.len != 0) return error.UnsupportedSqlShape;
            const all_field = try alloc.dupe(u8, "*");
            var all_field_transferred = false;
            errdefer if (!all_field_transferred) alloc.free(all_field);
            try fields.append(alloc, all_field);
            all_field_transferred = true;
            saw_all = true;
            if (parser.matchToken(tokens, pos, .comma) == null) break;
            continue;
        }
        if (peekSimpleReturningField(tokens, pos.*)) {
            const parsed_field = try hooks.parse_field_expression_owned(hooks.ptr);
            defer alloc.free(parsed_field);
            if (try binder.normalizeJoinedMutationReturningSourceFieldAlloc(alloc, schema, joined_source_schema, parsed_field, source_alias)) |source_field| {
                var source_field_owned = true;
                errdefer if (source_field_owned) alloc.free(source_field);
                const alias = try grammar.parseOptionalProjectionAliasAlloc(alloc, tokens, pos);
                var alias_owned = true;
                errdefer if (alias_owned) if (alias) |owned| alloc.free(owned);
                const output = alias orelse try alloc.dupe(u8, source_field);
                alias_owned = false;
                var output_owned = true;
                errdefer if (output_owned) alloc.free(output);
                try expressions.append(alloc, .{
                    .output = output,
                    .expression = .{
                        .kind = .field,
                        .field = source_field,
                        .field_source = .source,
                    },
                });
                source_field_owned = false;
                output_owned = false;
            } else {
                const field = try binder.normalizeReturningFieldAlloc(alloc, schema, parsed_field, returning_qualifiers);
                var field_owned = true;
                errdefer if (field_owned) alloc.free(field);
                const alias = try grammar.parseOptionalProjectionAliasAlloc(alloc, tokens, pos);
                var alias_owned = true;
                errdefer if (alias_owned) if (alias) |owned| alloc.free(owned);
                if (alias) |output| {
                    if (std.mem.eql(u8, output, field)) {
                        alloc.free(output);
                        alias_owned = false;
                        try fields.append(alloc, field);
                        field_owned = false;
                    } else {
                        if (saw_all and binder.relationalColumnForReturningField(schema, output) != null) return error.UnsupportedSqlShape;
                        try expressions.append(alloc, .{
                            .output = output,
                            .expression = .{
                                .kind = .field,
                                .field = field,
                            },
                        });
                        alias_owned = false;
                        field_owned = false;
                    }
                } else {
                    if (saw_all) return error.UnsupportedSqlShape;
                    try fields.append(alloc, field);
                    field_owned = false;
                }
            }
            if (parser.matchToken(tokens, pos, .comma) == null) break;
            continue;
        }
        const item = try hooks.parse_select_item(hooks.ptr, source_alias, returning_qualifiers);
        var item_owned = true;
        errdefer if (item_owned) plan_mod.freeSelectItem(alloc, item);
        switch (item) {
            .field => |parsed_field| {
                if (saw_all) return error.UnsupportedSqlShape;
                if (try binder.normalizeJoinedMutationReturningSourceFieldAlloc(alloc, schema, joined_source_schema, parsed_field, source_alias)) |source_field| {
                    var source_field_owned = true;
                    errdefer if (source_field_owned) alloc.free(source_field);
                    const output = try alloc.dupe(u8, source_field);
                    var output_owned = true;
                    errdefer if (output_owned) alloc.free(output);
                    try expressions.append(alloc, .{
                        .output = output,
                        .expression = .{
                            .kind = .field,
                            .field = source_field,
                            .field_source = .source,
                        },
                    });
                    source_field_owned = false;
                    output_owned = false;
                } else {
                    const field = try binder.normalizeReturningFieldAlloc(alloc, schema, parsed_field, returning_qualifiers);
                    var field_owned = true;
                    errdefer if (field_owned) alloc.free(field);
                    try fields.append(alloc, field);
                    field_owned = false;
                }
                alloc.free(parsed_field);
                item_owned = false;
            },
            .field_alias => |projection| {
                if (try binder.normalizeJoinedMutationReturningSourceFieldAlloc(alloc, schema, joined_source_schema, projection.field, source_alias)) |source_field| {
                    var source_field_owned = true;
                    errdefer if (source_field_owned) alloc.free(source_field);
                    try expressions.append(alloc, .{
                        .output = projection.output,
                        .expression = .{
                            .kind = .field,
                            .field = source_field,
                            .field_source = .source,
                        },
                    });
                    alloc.free(projection.field);
                    source_field_owned = false;
                    item_owned = false;
                } else {
                    const field = try binder.normalizeReturningFieldAlloc(alloc, schema, projection.field, returning_qualifiers);
                    var field_owned = true;
                    errdefer if (field_owned) alloc.free(field);
                    if (saw_all and binder.relationalColumnForReturningField(schema, projection.output) != null) return error.UnsupportedSqlShape;
                    try expressions.append(alloc, .{
                        .output = projection.output,
                        .expression = .{
                            .kind = .field,
                            .field = field,
                        },
                    });
                    alloc.free(projection.field);
                    field_owned = false;
                    item_owned = false;
                }
            },
            .expression => |projection| {
                if (saw_all and binder.relationalColumnForReturningField(schema, projection.output) != null) return error.UnsupportedSqlShape;
                try expressions.append(alloc, projection);
                item_owned = false;
            },
            .coalesce => |projection| {
                if (saw_all and binder.relationalColumnForReturningField(schema, projection.output) != null) return error.UnsupportedSqlShape;
                const expression_projection = try plan_mod.expressionProjectionFromCoalesceAlloc(alloc, projection);
                var expression_projection_owned = true;
                errdefer if (expression_projection_owned) freeExpressionProjection(alloc, expression_projection);
                try expressions.append(alloc, expression_projection);
                expression_projection_owned = false;
                plan_mod.freeCoalesceProjection(alloc, projection);
                item_owned = false;
            },
            else => return error.UnsupportedSqlShape,
        }
        if (parser.matchToken(tokens, pos, .comma) == null) break;
    }

    const owned_fields = try fields.toOwnedSlice(alloc);
    var fields_owned = true;
    errdefer if (fields_owned) strings.freeStringSlice(alloc, owned_fields);
    const owned_expressions = try expressions.toOwnedSlice(alloc);
    errdefer freeExpressionProjections(alloc, owned_expressions);
    try validateReturningProjectionOutputs(schema, owned_fields, owned_expressions);
    fields_owned = false;
    return .{
        .fields = owned_fields,
        .expressions = owned_expressions,
    };
}

pub fn returningFieldOutputCount(fields: []const []const u8, output: []const u8) usize {
    var count: usize = 0;
    for (fields) |field| {
        if (std.mem.eql(u8, field, output)) count += 1;
    }
    return count;
}

pub fn returningExpressionOutputCount(expressions: []const db_mod.types.RelationalRowsExpressionProjection, output: []const u8) usize {
    var count: usize = 0;
    for (expressions) |projection| {
        if (std.mem.eql(u8, projection.output, output)) count += 1;
    }
    return count;
}

pub fn jsonValueProjectedType(alloc: std.mem.Allocator, value_json: []const u8) !ProjectedColumnType {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, value_json, .{}) catch return error.UnsupportedSqlShape;
    defer parsed.deinit();
    return switch (parsed.value) {
        .string => .{ .field_type = .keyword },
        .integer, .float => .{ .field_type = .numeric },
        .bool => .{ .field_type = .boolean },
        .array => .{ .field_type = .array, .array_item_type = .keyword },
        .object => .{ .field_type = .json },
        .null => error.UnsupportedSqlShape,
        else => error.UnsupportedSqlShape,
    };
}

pub fn coalesceOutputType(
    alloc: std.mem.Allocator,
    columns: []const runtime_schema.RelationalColumn,
    projection: db_mod.types.RelationalRowsCoalesceProjection,
) !ProjectedColumnType {
    for (projection.operands) |operand| {
        switch (operand.kind) {
            .field => {
                const source = relationalColumnSliceField(columns, operand.field) orelse return error.InvalidSqlCatalog;
                return .{ .field_type = source.field_type, .array_item_type = source.array_item_type };
            },
            .value => if (operand.value_json.len != 0) return try jsonValueProjectedType(alloc, operand.value_json),
        }
    }
    return error.UnsupportedSqlShape;
}

fn relationalColumnSliceField(
    columns: []const runtime_schema.RelationalColumn,
    field: []const u8,
) ?runtime_schema.RelationalColumn {
    for (columns) |column| {
        if (std.mem.eql(u8, column.name, field) or std.mem.eql(u8, column.path, field)) return column;
    }
    return null;
}

pub fn projectedColumnAlloc(
    alloc: std.mem.Allocator,
    name: []const u8,
    field_type: runtime_schema.AntflyType,
    array_item_type: ?runtime_schema.AntflyType,
    nullable: bool,
) !runtime_schema.RelationalColumn {
    const owned_name = try alloc.dupe(u8, name);
    errdefer alloc.free(owned_name);
    const owned_path = try alloc.dupe(u8, name);
    errdefer alloc.free(owned_path);
    return .{
        .name = owned_name,
        .path = owned_path,
        .field_type = field_type,
        .array_item_type = array_item_type,
        .nullable = nullable,
    };
}

pub fn aggregateOutputFieldByOrdinalAlloc(
    alloc: std.mem.Allocator,
    group_fields: []const []const u8,
    group_expressions: []const db_mod.types.RelationalRowsExpressionProjection,
    aggregations: []const db_mod.types.RelationalRowsAggregateSpec,
    ordinal: u32,
) ![]const u8 {
    if (ordinal == 0) return error.UnsupportedSqlShape;
    var index: usize = @intCast(ordinal - 1);
    if (index < group_fields.len) return try alloc.dupe(u8, group_fields[index]);
    index -= group_fields.len;
    if (index < group_expressions.len) return try alloc.dupe(u8, group_expressions[index].output);
    index -= group_expressions.len;
    if (index < aggregations.len) return try alloc.dupe(u8, aggregations[index].name);
    return error.UnsupportedSqlShape;
}

pub fn aggregateJsonPathEqFilterCanStartAt(tokens: []const Token, pos: usize) bool {
    if (pos + 4 >= tokens.len) return false;
    if (!jsonExtractExpressionCanStartAt(tokens, pos)) return false;
    if (tokens[pos + 3].kind != .eq) return false;
    const as_text = tokenKindIsJsonExtractTextOperator(tokens[pos + 1].kind);
    const rhs = tokens[pos + 4];
    return switch (rhs.kind) {
        .string, .placeholder => true,
        .number, .minus => as_text,
        .identifier => as_text and
            !sqlKeywordIsAnyOrSome(rhs.text) and
            !std.ascii.eqlIgnoreCase(rhs.text, "all") and
            (std.ascii.eqlIgnoreCase(rhs.text, "null") or
                std.ascii.eqlIgnoreCase(rhs.text, "true") or
                std.ascii.eqlIgnoreCase(rhs.text, "false")),
        else => false,
    };
}

pub fn canParseAggregateFilterNot(tokens: []const Token, pos: usize) bool {
    return parser.peekKeyword(tokens, pos, "not") and pos + 1 < tokens.len and tokens[pos + 1].kind == .lparen;
}

pub fn canParseBareBooleanAggregateHavingExpression(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: usize,
    schema: runtime_schema.TableSchema,
    type_context: RowExpressionTypeContext,
    group_fields: []const []const u8,
    group_expressions: []const db_mod.types.RelationalRowsExpressionProjection,
    aggregations: []const db_mod.types.RelationalRowsAggregateSpec,
) !bool {
    const output_columns = try aggregateOutputColumnsAlloc(alloc, schema, type_context, group_fields, group_expressions, aggregations);
    defer if (output_columns.len > 0) alloc.free(output_columns);
    const aggregate_schema: runtime_schema.TableSchema = .{
        .storage_mode = .relational,
        .relational_columns = output_columns,
    };
    return canParseBareBooleanWhereExpression(tokens, pos, aggregate_schema);
}

pub fn parseAggregateOutputFieldExpressionConditionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    schema: runtime_schema.TableSchema,
    type_context: RowExpressionTypeContext,
    group_fields: []const []const u8,
    group_expressions: []const db_mod.types.RelationalRowsExpressionProjection,
    aggregations: []const db_mod.types.RelationalRowsAggregateSpec,
    hooks: AggregateOutputFieldExpressionConditionParserHooks,
) !db_mod.types.RelationalRowsExpressionCondition {
    const field = try hooks.parse_field(hooks.ptr, group_fields, group_expressions, aggregations);
    var field_transferred = false;
    errdefer if (!field_transferred) alloc.free(field);
    if (!aggregateOutputFieldIsUnique(group_fields, group_expressions, aggregations, field)) return error.UnsupportedSqlShape;
    const column = try aggregateOutputColumnForFieldAlloc(alloc, schema, type_context, group_fields, group_expressions, aggregations, field);

    const op: runtime_schema.RelationalCheckOp = if (try parseExpressionIsTailIf(tokens, pos, .{
        .allow_boolean_unknown = true,
        .allow_boolean_literal = true,
    })) |is_tail| blk: {
        switch (is_tail.kind) {
            .distinct_comparison, .null_test => {},
            .boolean_unknown => {
                if (column.field_type != .boolean) return error.InvalidSqlCatalog;
                break :blk is_tail.op;
            },
            .boolean_literal => {
                if (column.field_type != .boolean) return error.InvalidSqlCatalog;
                const value_json = try alloc.dupe(u8, value_mod.booleanJson(is_tail.boolean_value));
                errdefer alloc.free(value_json);
                const lhs_field = field;
                field_transferred = true;
                const rhs = try alloc.alloc(db_mod.types.RelationalRowsExpression, 1);
                var rhs_transferred = false;
                errdefer if (!rhs_transferred) alloc.free(rhs);
                rhs[0] = .{
                    .kind = .value,
                    .value_json = value_json,
                };
                rhs_transferred = true;
                return .{
                    .lhs = .{
                        .kind = .field,
                        .field = lhs_field,
                    },
                    .op = .eq,
                    .rhs = rhs,
                };
            },
        }
        break :blk is_tail.op;
    } else try parseComparisonOp(tokens, pos);

    const lhs: db_mod.types.RelationalRowsExpression = .{
        .kind = .field,
        .field = field,
    };
    field_transferred = true;

    const rhs = switch (op) {
        .is_null, .is_not_null => &.{},
        else => blk: {
            const value_json = try hooks.parse_value_json(hooks.ptr);
            var value_transferred = false;
            errdefer if (!value_transferred) alloc.free(value_json);
            const out = try alloc.alloc(db_mod.types.RelationalRowsExpression, 1);
            var out_transferred = false;
            errdefer if (!out_transferred) alloc.free(out);
            out[0] = .{
                .kind = .value,
                .value_json = value_json,
            };
            value_transferred = true;
            out_transferred = true;
            break :blk out;
        },
    };

    return .{
        .lhs = lhs,
        .op = op,
        .rhs = rhs,
    };
}

pub fn parseAggregateOutputExpressionConditionAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    type_context: RowExpressionTypeContext,
    group_fields: []const []const u8,
    group_expressions: []const db_mod.types.RelationalRowsExpressionProjection,
    aggregations: []const db_mod.types.RelationalRowsAggregateSpec,
    hooks: AggregateOutputExpressionConditionParserHooks,
) !db_mod.types.RelationalRowsExpressionCondition {
    const output_columns = try aggregateOutputColumnsAlloc(alloc, schema, type_context, group_fields, group_expressions, aggregations);
    defer if (output_columns.len > 0) alloc.free(output_columns);
    const aggregate_schema: runtime_schema.TableSchema = .{
        .storage_mode = .relational,
        .relational_columns = output_columns,
    };
    return try hooks.parse_condition(hooks.ptr, aggregate_schema);
}

pub fn parseAggregateOutputOrderExpressionAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    type_context: RowExpressionTypeContext,
    group_fields: []const []const u8,
    group_expressions: []const db_mod.types.RelationalRowsExpressionProjection,
    aggregations: []const db_mod.types.RelationalRowsAggregateSpec,
    hooks: AggregateOutputOrderExpressionParserHooks,
) !db_mod.types.RelationalRowsQueryOrder {
    const output_columns = try aggregateOutputColumnsAlloc(alloc, schema, type_context, group_fields, group_expressions, aggregations);
    defer if (output_columns.len > 0) alloc.free(output_columns);
    const aggregate_schema: runtime_schema.TableSchema = .{
        .storage_mode = .relational,
        .relational_columns = output_columns,
    };
    return try hooks.parse_order(hooks.ptr, aggregate_schema);
}

pub fn parseWindowOutputOrderExpressionAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    type_context: RowExpressionTypeContext,
    select: plan_mod.WindowSelectList,
    hooks: WindowOutputOrderExpressionParserHooks,
) !db_mod.types.RelationalRowsQueryOrder {
    const output_columns = try windowOutputColumnsAlloc(alloc, schema, type_context, select);
    defer if (output_columns.len > 0) alloc.free(output_columns);
    const window_schema: runtime_schema.TableSchema = .{
        .storage_mode = .relational,
        .relational_columns = output_columns,
    };
    return try hooks.parse_order(hooks.ptr, window_schema);
}

pub fn parseJoinOutputOrderExpressionAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    select: []const db_mod.types.RelationalRowsJoinProjection,
    hooks: JoinOutputOrderExpressionParserHooks,
) !db_mod.types.RelationalRowsQueryOrder {
    const output_columns = try joinOutputColumnsAlloc(alloc, schema, select);
    defer if (output_columns.len > 0) alloc.free(output_columns);
    const join_schema: runtime_schema.TableSchema = .{
        .storage_mode = .relational,
        .relational_columns = output_columns,
    };
    return try hooks.parse_order(hooks.ptr, join_schema);
}

pub fn parseBareBooleanAggregateHavingExpression(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    type_context: RowExpressionTypeContext,
    expressions: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionCondition),
    group_fields: []const []const u8,
    group_expressions: []const db_mod.types.RelationalRowsExpressionProjection,
    aggregations: []const db_mod.types.RelationalRowsAggregateSpec,
    hooks: BareBooleanAggregateHavingExpressionParserHooks,
) !void {
    const output_columns = try aggregateOutputColumnsAlloc(alloc, schema, type_context, group_fields, group_expressions, aggregations);
    defer if (output_columns.len > 0) alloc.free(output_columns);
    const aggregate_schema: runtime_schema.TableSchema = .{
        .storage_mode = .relational,
        .relational_columns = output_columns,
    };
    try hooks.parse_bare_boolean(hooks.ptr, aggregate_schema, expressions);
}

pub fn parseAggregateHavingBooleanIsNotGroups(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    schema: runtime_schema.TableSchema,
    type_context: RowExpressionTypeContext,
    groups: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionPredicateGroup),
    group_fields: []const []const u8,
    group_expressions: []const db_mod.types.RelationalRowsExpressionProjection,
    aggregations: []const db_mod.types.RelationalRowsAggregateSpec,
    hooks: AggregateOutputFieldExpressionConditionParserHooks,
) !bool {
    const saved_pos = pos.*;
    const field = hooks.parse_field(hooks.ptr, group_fields, group_expressions, aggregations) catch |err| {
        pos.* = saved_pos;
        return switch (err) {
            error.UnsupportedSqlShape => false,
            else => err,
        };
    };
    defer alloc.free(field);
    if (!parser.matchKeyword(tokens, pos, "is")) {
        pos.* = saved_pos;
        return false;
    }
    if (!parser.matchKeyword(tokens, pos, "not")) {
        pos.* = saved_pos;
        return false;
    }
    if (!(parser.peekKeyword(tokens, pos.*, "true") or parser.peekKeyword(tokens, pos.*, "false"))) {
        pos.* = saved_pos;
        return false;
    }

    const column = try aggregateOutputColumnForFieldAlloc(alloc, schema, type_context, group_fields, group_expressions, aggregations, field);
    const value = (try value_mod.parseSqlBooleanIsValue(tokens, pos, column)) orelse return error.UnsupportedSqlShape;
    try appendBooleanIsNotExpressionGroups(alloc, groups, field, value);
    return true;
}

pub fn aggregateHavingHasBooleanIsNot(tokens: []const Token, pos: usize) bool {
    var depth: usize = 0;
    var i = pos;
    while (i + 2 < tokens.len) : (i += 1) {
        const token = tokens[i];
        switch (token.kind) {
            .lparen => depth += 1,
            .rparen => {
                if (depth == 0) return false;
                depth -= 1;
            },
            .identifier => {
                if (depth == 0 and (std.ascii.eqlIgnoreCase(token.text, "order") or
                    std.ascii.eqlIgnoreCase(token.text, "limit") or
                    std.ascii.eqlIgnoreCase(token.text, "offset") or
                    std.ascii.eqlIgnoreCase(token.text, "fetch")))
                {
                    return false;
                }
                if (std.ascii.eqlIgnoreCase(token.text, "is") and
                    tokens[i + 1].kind == .identifier and
                    std.ascii.eqlIgnoreCase(tokens[i + 1].text, "not") and
                    tokens[i + 2].kind == .identifier and
                    (std.ascii.eqlIgnoreCase(tokens[i + 2].text, "true") or
                        std.ascii.eqlIgnoreCase(tokens[i + 2].text, "false")))
                {
                    return true;
                }
            },
            else => {},
        }
    }
    return false;
}

pub fn canParseAggregateHavingNot(tokens: []const Token, pos: usize) bool {
    return parser.peekKeyword(tokens, pos, "not") and pos + 1 < tokens.len and tokens[pos + 1].kind == .lparen;
}

pub fn matchBooleanGroupOpen(tokens: []const Token, pos: *usize) bool {
    if (!parser.peekKind(tokens, pos.*, .lparen)) return false;
    if (peekParenthesizedExpressionCondition(tokens, pos.*)) return false;
    _ = parser.matchToken(tokens, pos, .lparen) orelse unreachable;
    return true;
}

pub fn peekParenthesizedExpressionCondition(tokens: []const Token, pos: usize) bool {
    if (!parser.peekKind(tokens, pos, .lparen)) return false;
    var depth: usize = 0;
    var i = pos;
    while (i < tokens.len) : (i += 1) {
        const token = tokens[i];
        switch (token.kind) {
            .lparen => depth += 1,
            .rparen => {
                if (depth == 0) return false;
                depth -= 1;
                if (depth == 0) {
                    if (i + 1 >= tokens.len) return false;
                    const next = tokens[i + 1];
                    return switch (next.kind) {
                        .eq, .neq, .gt, .gte, .lt, .lte => true,
                        .identifier => std.ascii.eqlIgnoreCase(next.text, "is"),
                        else => false,
                    };
                }
            },
            else => {},
        }
    }
    return false;
}

pub fn peekStandaloneAggregateGroupIdentifier(tokens: []const Token, pos: usize) bool {
    if (!parser.peekKind(tokens, pos, .identifier)) return false;
    if (pos + 1 >= tokens.len) return true;
    return switch (tokens[pos + 1].kind) {
        .lparen, .arrow_json, .arrow_text, .path_arrow_json, .path_arrow_text, .plus, .minus, .star, .slash, .percent, .pipe_concat => false,
        else => true,
    };
}

pub fn appendAggregateGroupByFieldOrAlias(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    group_by: *std.ArrayListUnmanaged([]const u8),
    group_expressions: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionProjection),
    select: plan_mod.AggregateSelectList,
    raw_field: []const u8,
    field_expression_qualifiers: []const []const u8,
    returning_expression_qualifiers: []const []const u8,
    defer_row_expression_field_validation: bool,
) !void {
    var raw_owned = true;
    defer if (raw_owned) alloc.free(raw_field);

    const normalized_field: ?[]const u8 = binder.normalizeRowExpressionFieldAlloc(alloc, schema, raw_field, field_expression_qualifiers, returning_expression_qualifiers, defer_row_expression_field_validation) catch |err| switch (err) {
        error.InvalidSqlCatalog => null,
        else => return err,
    };
    if (normalized_field) |field| {
        var field_owned = true;
        errdefer if (field_owned) alloc.free(field);
        if (binder.relationalColumnForField(schema, field, null) != null) {
            try group_by.append(alloc, field);
            field_owned = false;
            raw_owned = false;
            alloc.free(raw_field);
            return;
        }
        alloc.free(field);
        field_owned = false;
    }

    for (select.group_expressions) |projection| {
        if (std.mem.eql(u8, projection.output, raw_field)) {
            try group_expressions.append(alloc, try plan_mod.cloneExpressionProjection(alloc, projection));
            return;
        }
    }
    return error.InvalidSqlCatalog;
}

pub fn appendAggregateGroupByOrdinal(
    alloc: std.mem.Allocator,
    group_by: *std.ArrayListUnmanaged([]const u8),
    group_expressions: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionProjection),
    select: plan_mod.AggregateSelectList,
    ordinal: u32,
) !void {
    if (ordinal == 0) return error.UnsupportedSqlShape;
    const index: usize = @intCast(ordinal - 1);
    if (index >= select.outputs.len) return error.UnsupportedSqlShape;
    const output = select.outputs[index];
    switch (output.kind) {
        .group_field => try group_by.append(alloc, try alloc.dupe(u8, select.group_fields[output.index])),
        .group_expression => try group_expressions.append(alloc, try plan_mod.cloneExpressionProjection(alloc, select.group_expressions[output.index])),
        .aggregation => return error.UnsupportedSqlShape,
    }
}

pub fn expressionOrderCount(order_by: []const db_mod.types.RelationalRowsQueryOrder) usize {
    var count: usize = 0;
    for (order_by) |order| {
        if (order.expression != null) count += 1;
    }
    return count;
}

pub fn validateDistinctOnOrder(
    distinct_on: []const db_mod.types.RelationalRowsExpression,
    order_by: []const db_mod.types.RelationalRowsQueryOrder,
) !void {
    if (order_by.len < distinct_on.len) return error.UnsupportedSqlShape;
    for (distinct_on, 0..) |expression, i| {
        const order = order_by[i];
        if (order.expression) |order_expression| {
            if (!relationalRowsExpressionEqual(order_expression, expression)) return error.UnsupportedSqlShape;
        } else {
            if (expression.kind != .field or expression.field_source != .row or !std.mem.eql(u8, order.field, expression.field)) return error.UnsupportedSqlShape;
        }
    }
}

pub fn parseOrderModifiers(
    tokens: []const Token,
    pos: *usize,
    order: *db_mod.types.RelationalRowsQueryOrder,
) !?bool {
    order.direction = if (parser.matchKeyword(tokens, pos, "using"))
        try parseOrderUsingDirection(tokens, pos)
    else if (parser.matchKeyword(tokens, pos, "desc"))
        .desc
    else blk: {
        _ = parser.matchKeyword(tokens, pos, "asc");
        break :blk .asc;
    };
    return if (parser.matchKeyword(tokens, pos, "nulls")) blk: {
        if (parser.matchKeyword(tokens, pos, "first")) break :blk true;
        try parser.expectKeyword(tokens, pos, "last");
        break :blk false;
    } else null;
}

pub fn parseOrderUsingDirection(
    tokens: []const Token,
    pos: *usize,
) !db_mod.types.RelationalRowsQueryOrderDirection {
    if (parser.matchToken(tokens, pos, .lt) != null or parser.matchToken(tokens, pos, .lte) != null) return .asc;
    if (parser.matchToken(tokens, pos, .gt) != null or parser.matchToken(tokens, pos, .gte) != null) return .desc;
    return error.UnsupportedSqlShape;
}

pub fn appendOrderWithNullPlacement(
    alloc: std.mem.Allocator,
    order_by: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsQueryOrder),
    order: db_mod.types.RelationalRowsQueryOrder,
    explicit_nulls_first: ?bool,
) !void {
    if (explicit_nulls_first) |nulls_first| {
        if (order.null_test != null) return error.UnsupportedSqlShape;
        var null_order = try cloneOrderForNullTestAlloc(alloc, order);
        var null_order_transferred = false;
        errdefer if (!null_order_transferred) {
            if (null_order.field.len > 0) alloc.free(null_order.field);
            if (null_order.expression) |expression| freeExpression(alloc, expression);
        };
        null_order.direction = if (nulls_first) .desc else .asc;
        null_order.null_test = .is_null;
        try order_by.append(alloc, null_order);
        null_order_transferred = true;
    }
    try order_by.append(alloc, order);
}

pub fn cloneOrderForNullTestAlloc(
    alloc: std.mem.Allocator,
    order: db_mod.types.RelationalRowsQueryOrder,
) !db_mod.types.RelationalRowsQueryOrder {
    if (order.expression) |expression| {
        return .{ .expression = try cloneExpressionAlloc(alloc, expression) };
    }
    if (order.field.len == 0) return error.UnsupportedSqlShape;
    return .{ .field = try alloc.dupe(u8, order.field) };
}

pub fn selectOutputOrderByRefAlloc(
    alloc: std.mem.Allocator,
    select: plan_mod.SelectList,
    output: ast.SelectOutputRef,
) !db_mod.types.RelationalRowsQueryOrder {
    return switch (output.kind) {
        .field => .{ .field = try alloc.dupe(u8, select.fields[output.index]) },
        .json_extract => .{ .expression = try expressionFromJsonExtractProjectionAlloc(alloc, select.json_extract[output.index]) },
        .array_length => .{ .expression = try expressionFromArrayLengthProjectionAlloc(alloc, select.array_length[output.index]) },
        .coalesce => .{ .expression = try expressionFromCoalesceProjectionAlloc(alloc, select.coalesce[output.index]) },
        .field_alias => .{ .field = try alloc.dupe(u8, select.field_aliases[output.index].field) },
        .expression => blk: {
            const expression = select.expressions[output.index].expression;
            if (expression.kind == .field) break :blk .{ .field = try alloc.dupe(u8, expression.field) };
            break :blk .{ .expression = try cloneExpressionAlloc(alloc, expression) };
        },
    };
}

pub fn selectOutputOrderByOrdinalAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    select: plan_mod.SelectList,
    ordinal: u32,
) !db_mod.types.RelationalRowsQueryOrder {
    if (ordinal == 0) return error.UnsupportedSqlShape;
    var index: usize = @intCast(ordinal - 1);
    if (select.select_all) {
        if (index < schema.relational_columns.len) {
            return .{ .field = try alloc.dupe(u8, schema.relational_columns[index].name) };
        }
        index -= schema.relational_columns.len;
    }
    if (index >= select.outputs.len) return error.UnsupportedSqlShape;
    return try selectOutputOrderByRefAlloc(alloc, select, select.outputs[index]);
}

pub fn parseSelectOutputOrderByNameMaybeAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    select: plan_mod.SelectList,
) !?db_mod.types.RelationalRowsQueryOrder {
    if (!parser.peekKind(tokens, pos.*, .identifier)) return null;
    if (pos.* + 1 < tokens.len) {
        switch (tokens[pos.* + 1].kind) {
            .lparen, .arrow_json, .arrow_text, .path_arrow_json, .path_arrow_text, .plus, .minus, .star, .slash, .percent, .pipe_concat => return null,
            else => {},
        }
    }

    const name = tokens[pos.*].text;
    const output = (try plan_mod.selectOutputByName(name, select)) orelse return null;
    pos.* += 1;
    return try selectOutputOrderByRefAlloc(alloc, select, output);
}

pub fn expressionFromJsonExtractProjectionAlloc(
    alloc: std.mem.Allocator,
    projection: db_mod.types.RelationalRowsJsonExtractProjection,
) anyerror!db_mod.types.RelationalRowsExpression {
    const operands = try alloc.alloc(db_mod.types.RelationalRowsExpression, 1);
    var operands_transferred = false;
    errdefer if (!operands_transferred) alloc.free(operands);
    operands[0] = .{ .kind = .field, .field = try alloc.dupe(u8, projection.field) };
    var operand_transferred = false;
    errdefer if (!operand_transferred) freeExpression(alloc, operands[0]);
    const path = try alloc.dupe(u8, projection.path);
    var path_transferred = false;
    errdefer if (!path_transferred) alloc.free(path);
    operands_transferred = true;
    operand_transferred = true;
    path_transferred = true;
    return .{
        .kind = .json_extract,
        .json_path = path,
        .json_as_text = projection.as_text,
        .operands = operands,
    };
}

pub fn expressionFromArrayLengthProjectionAlloc(
    alloc: std.mem.Allocator,
    projection: db_mod.types.RelationalRowsArrayLengthProjection,
) !db_mod.types.RelationalRowsExpression {
    const operands = try alloc.alloc(db_mod.types.RelationalRowsExpression, 1);
    var operands_transferred = false;
    errdefer if (!operands_transferred) alloc.free(operands);
    operands[0] = .{ .kind = .field, .field = try alloc.dupe(u8, projection.field) };
    var operand_transferred = false;
    errdefer if (!operand_transferred) freeExpression(alloc, operands[0]);
    operands_transferred = true;
    operand_transferred = true;
    return .{
        .kind = .array_length,
        .operands = operands,
    };
}

pub fn expressionFromCoalesceProjectionAlloc(
    alloc: std.mem.Allocator,
    projection: db_mod.types.RelationalRowsCoalesceProjection,
) !db_mod.types.RelationalRowsExpression {
    var expression_projection = try plan_mod.expressionProjectionFromCoalesceAlloc(alloc, projection);
    alloc.free(expression_projection.output);
    expression_projection.output = "";
    return expression_projection.expression;
}

pub fn aggregateFilterExpressionCount(aggregations: []const db_mod.types.RelationalRowsAggregateSpec) usize {
    var count: usize = 0;
    for (aggregations) |aggregation| {
        count += aggregation.filter_expressions.len;
    }
    return count;
}

pub fn aggregateFilterExpressionArrayCount(aggregations: []const db_mod.types.RelationalRowsAggregateSpec) usize {
    var count: usize = 0;
    for (aggregations) |aggregation| {
        count += aggregation.filter_expression_array_contains.len;
    }
    return count;
}

pub fn aggregateFilterJsonAccessCount(aggregations: []const db_mod.types.RelationalRowsAggregateSpec) usize {
    var count: usize = 0;
    for (aggregations) |aggregation| {
        count += aggregation.filter_json_contains.len +
            aggregation.filter_json_path_eq.len +
            aggregation.filter_json_path_exists.len;
    }
    return count;
}

pub fn aggregateFilterStructuredAccessCount(aggregations: []const db_mod.types.RelationalRowsAggregateSpec) usize {
    var count: usize = 0;
    for (aggregations) |aggregation| {
        count += aggregation.filter_array_any.len +
            aggregation.filter_array_contains.len +
            aggregation.filter_array_eq.len +
            aggregation.filter_in_predicates.len +
            aggregation.filter_text_patterns.len;
    }
    return count;
}

pub fn aggregateFilterGroupCount(aggregations: []const db_mod.types.RelationalRowsAggregateSpec) usize {
    var count: usize = 0;
    for (aggregations) |aggregation| {
        count += aggregation.filter_any.len + aggregation.filter_not.len;
    }
    return count;
}

pub fn aggregateInputExpressionCount(aggregations: []const db_mod.types.RelationalRowsAggregateSpec) usize {
    var count: usize = 0;
    for (aggregations) |aggregation| {
        if (aggregation.expression != null) count += 1;
    }
    return count;
}

pub fn aggregateDescendingPercentileCount(aggregations: []const db_mod.types.RelationalRowsAggregateSpec) usize {
    var count: usize = 0;
    for (aggregations) |aggregation| {
        if (isSqlPercentileAggregateOp(aggregation.op) and aggregation.percentile_order == .desc) count += 1;
    }
    return count;
}

pub fn aggregatePercentileArrayCount(aggregations: []const db_mod.types.RelationalRowsAggregateSpec) usize {
    var count: usize = 0;
    for (aggregations) |aggregation| {
        if (isSqlPercentileAggregateOp(aggregation.op) and aggregation.percentiles.len > 0) count += 1;
    }
    return count;
}

pub fn aggregateModeCount(aggregations: []const db_mod.types.RelationalRowsAggregateSpec) usize {
    var count: usize = 0;
    for (aggregations) |aggregation| {
        if (aggregation.op == .mode) count += 1;
    }
    return count;
}

pub fn windowFunctionRequiresOrder(function: db_mod.types.RelationalRowsWindowFunction) bool {
    return switch (function) {
        .count, .sum, .avg, .min, .max, .bool_or, .bool_and => false,
        .row_number, .rank, .dense_rank, .percent_rank, .cume_dist, .ntile, .lag, .lead, .first_value, .last_value, .nth_value => true,
    };
}

pub fn windowFunctionName(function: db_mod.types.RelationalRowsWindowFunction) []const u8 {
    return switch (function) {
        .row_number => "row_number",
        .rank => "rank",
        .dense_rank => "dense_rank",
        .percent_rank => "percent_rank",
        .cume_dist => "cume_dist",
        .ntile => "ntile",
        .lag => "lag",
        .lead => "lead",
        .first_value => "first_value",
        .last_value => "last_value",
        .nth_value => "nth_value",
        .count => "count",
        .sum => "sum",
        .avg => "avg",
        .min => "min",
        .max => "max",
        .bool_or => "bool_or",
        .bool_and => "bool_and",
    };
}

pub fn windowFunctionSupportsFilter(function: db_mod.types.RelationalRowsWindowFunction) bool {
    return switch (function) {
        .count, .sum, .avg, .min, .max, .bool_or, .bool_and => true,
        else => false,
    };
}

pub fn windowOutputType(
    function: db_mod.types.RelationalRowsWindowFunction,
    value_type: ?runtime_schema.AntflyType,
) !runtime_schema.AntflyType {
    return switch (function) {
        .row_number, .rank, .dense_rank, .percent_rank, .cume_dist, .ntile, .count, .sum, .avg => .numeric,
        .lag, .lead, .first_value, .last_value, .nth_value, .min, .max => value_type orelse error.UnsupportedSqlShape,
        .bool_or, .bool_and => .boolean,
    };
}

pub fn aggregateOutputProjectedType(
    aggregation: db_mod.types.RelationalRowsAggregateSpec,
    input_type: ?runtime_schema.AntflyType,
) !ProjectedColumnType {
    return switch (aggregation.op) {
        .array_agg => .{
            .field_type = .array,
            .array_item_type = input_type orelse return error.UnsupportedSqlShape,
        },
        .string_agg => .{ .field_type = .keyword },
        .percentile_cont, .percentile_disc => .{
            .field_type = if (aggregation.percentiles.len > 0) .array else .numeric,
            .array_item_type = if (aggregation.percentiles.len > 0) .numeric else null,
        },
        .count, .sum, .avg => .{ .field_type = .numeric },
        .min, .max, .mode => .{ .field_type = input_type orelse return error.UnsupportedSqlShape },
        .bool_or, .bool_and => .{ .field_type = .boolean },
    };
}

pub fn sqlExpressionIsInterval(expression: db_mod.types.RelationalRowsExpression) bool {
    return expression.kind == .interval_ns or expression.kind == .interval_months;
}

pub fn sqlExpressionContainsInterval(expression: db_mod.types.RelationalRowsExpression) bool {
    if (sqlExpressionIsInterval(expression)) return true;
    for (expression.operands) |operand| {
        if (sqlExpressionContainsInterval(operand)) return true;
    }
    return false;
}

pub fn aggregateFilterIsEmpty(filter: AggregateFilter) bool {
    return filter.predicates.len == 0 and
        filter.array_any.len == 0 and
        filter.array_contains.len == 0 and
        filter.array_eq.len == 0 and
        filter.in_predicates.len == 0 and
        filter.json_contains.len == 0 and
        filter.json_path_eq.len == 0 and
        filter.json_path_exists.len == 0 and
        filter.text_patterns.len == 0 and
        filter.expressions.len == 0 and
        filter.expression_array_contains.len == 0 and
        filter.any_groups.len == 0 and
        filter.not_groups.len == 0;
}

pub fn freeAggregateFilter(alloc: std.mem.Allocator, filter: AggregateFilter) void {
    freeRelationalChecks(alloc, filter.predicates);
    if (filter.predicates.len > 0) alloc.free(filter.predicates);
    freeArrayAny(alloc, filter.array_any);
    if (filter.array_any.len > 0) alloc.free(filter.array_any);
    freeArrayContains(alloc, filter.array_contains);
    if (filter.array_contains.len > 0) alloc.free(filter.array_contains);
    freeArrayEq(alloc, filter.array_eq);
    if (filter.array_eq.len > 0) alloc.free(filter.array_eq);
    freeInPredicates(alloc, filter.in_predicates);
    if (filter.in_predicates.len > 0) alloc.free(filter.in_predicates);
    freeJsonContains(alloc, filter.json_contains);
    if (filter.json_contains.len > 0) alloc.free(filter.json_contains);
    freeJsonPathEq(alloc, filter.json_path_eq);
    if (filter.json_path_eq.len > 0) alloc.free(filter.json_path_eq);
    freeJsonPathExists(alloc, filter.json_path_exists);
    if (filter.json_path_exists.len > 0) alloc.free(filter.json_path_exists);
    freeTextPatterns(alloc, filter.text_patterns);
    if (filter.text_patterns.len > 0) alloc.free(filter.text_patterns);
    freeExpressionConditions(alloc, filter.expressions);
    if (filter.expressions.len > 0) alloc.free(filter.expressions);
    freeExpressionArrayContains(alloc, filter.expression_array_contains);
    if (filter.expression_array_contains.len > 0) alloc.free(filter.expression_array_contains);
    freeExpressionPredicateGroups(alloc, filter.any_groups);
    if (filter.any_groups.len > 0) alloc.free(filter.any_groups);
    freeExpressionPredicateGroups(alloc, filter.not_groups);
    if (filter.not_groups.len > 0) alloc.free(filter.not_groups);
}

pub fn validateWindowFrame(frame: db_mod.types.RelationalRowsWindowFrame) !void {
    try validateWindowFrameBoundOffset(frame.start, frame.start_offset);
    try validateWindowFrameBoundOffset(frame.end, frame.end_offset);
    if (frame.start == .unbounded_following or frame.end == .unbounded_preceding) return error.UnsupportedSqlShape;
    if (windowFrameBoundOrdinal(frame.start, frame.start_offset) > windowFrameBoundOrdinal(frame.end, frame.end_offset)) return error.UnsupportedSqlShape;
}

pub fn validateWindowFrameForOrder(
    schema: runtime_schema.TableSchema,
    type_context: RowExpressionTypeContext,
    frame: db_mod.types.RelationalRowsWindowFrame,
    order_by: []const db_mod.types.RelationalRowsQueryOrder,
) !void {
    if (frame.unit != .range) return;
    if (frame.start != .offset_preceding and frame.start != .offset_following and frame.end != .offset_preceding and frame.end != .offset_following) return;
    if (order_by.len == 0) return error.UnsupportedSqlShape;
    const order = order_by[0];
    if (order.null_test != null) return error.UnsupportedSqlShape;
    if (order.expression) |expression| {
        try type_context.validateNumericOrDatetimeRowExpression(expression);
        return;
    }
    if (order.field.len == 0) return error.UnsupportedSqlShape;
    const column = binder.relationalColumnForField(schema, order.field, null) orelse return error.InvalidSqlCatalog;
    if (column.field_type != .numeric and column.field_type != .datetime) return error.UnsupportedSqlShape;
}

pub fn validateWindowFrameBoundOffset(
    bound: db_mod.types.RelationalRowsWindowFrameBound,
    offset: u32,
) !void {
    switch (bound) {
        .offset_preceding, .offset_following => {
            if (offset == 0) return error.UnsupportedSqlShape;
        },
        else => if (offset != 0) return error.UnsupportedSqlShape,
    }
}

pub fn windowFrameBoundOrdinal(bound: db_mod.types.RelationalRowsWindowFrameBound, offset: u32) i64 {
    return switch (bound) {
        .unbounded_preceding => std.math.minInt(i64),
        .offset_preceding => -@as(i64, @intCast(offset)),
        .current_row => 0,
        .offset_following => @as(i64, @intCast(offset)),
        .unbounded_following => std.math.maxInt(i64),
    };
}

pub fn windowValueExpressionCount(windows: []const db_mod.types.RelationalRowsWindowSpec) usize {
    var count: usize = 0;
    for (windows) |window| {
        if (window.value_expression != null) count += 1;
    }
    return count;
}

pub fn windowDefaultCount(windows: []const db_mod.types.RelationalRowsWindowSpec) usize {
    var count: usize = 0;
    for (windows) |window| {
        if (window.default_json.len > 0) count += 1;
    }
    return count;
}

pub fn windowFilterPredicateCount(windows: []const db_mod.types.RelationalRowsWindowSpec) usize {
    var count: usize = 0;
    for (windows) |window| {
        count += window.filter_predicates.len;
    }
    return count;
}

pub fn windowFilterExpressionCount(windows: []const db_mod.types.RelationalRowsWindowSpec) usize {
    var count: usize = 0;
    for (windows) |window| {
        count += window.filter_expressions.len;
    }
    return count;
}

pub fn windowFilterAccessCount(windows: []const db_mod.types.RelationalRowsWindowSpec) usize {
    var count: usize = 0;
    for (windows) |window| {
        count += window.filter_array_any.len +
            window.filter_array_contains.len +
            window.filter_array_eq.len +
            window.filter_in_predicates.len +
            window.filter_json_contains.len +
            window.filter_json_path_eq.len +
            window.filter_json_path_exists.len +
            window.filter_text_patterns.len +
            window.filter_expression_array_contains.len;
    }
    return count;
}

pub fn windowFilterGroupCount(windows: []const db_mod.types.RelationalRowsWindowSpec) usize {
    var count: usize = 0;
    for (windows) |window| {
        count += window.filter_any.len + window.filter_not.len;
    }
    return count;
}

pub fn windowFrameSignature(windows: []const db_mod.types.RelationalRowsWindowSpec) u64 {
    var signature: u64 = 0;
    for (windows) |window| {
        if (window.frame) |frame| {
            signature = signature *% 131 +% 17;
            signature = signature *% 131 +% windowFrameUnitCode(frame.unit);
            signature = signature *% 131 +% windowFrameBoundCode(frame.start);
            signature = signature *% 131 +% @as(u64, @intCast(frame.start_offset));
            signature = signature *% 131 +% windowFrameBoundCode(frame.end);
            signature = signature *% 131 +% @as(u64, @intCast(frame.end_offset));
        } else {
            signature = signature *% 131;
        }
    }
    return signature;
}

fn windowFrameUnitCode(unit: db_mod.types.RelationalRowsWindowFrameUnit) u64 {
    return switch (unit) {
        .rows => 1,
        .range => 2,
    };
}

fn windowFrameBoundCode(bound: db_mod.types.RelationalRowsWindowFrameBound) u64 {
    return switch (bound) {
        .unbounded_preceding => 1,
        .offset_preceding => 2,
        .current_row => 3,
        .offset_following => 4,
        .unbounded_following => 5,
    };
}

pub fn joinProjectionOutputIsUnique(select: []const db_mod.types.RelationalRowsJoinProjection, field: []const u8) bool {
    var matches: usize = 0;
    for (select) |projection| {
        if (std.mem.eql(u8, projection.output, field)) matches += 1;
    }
    return matches == 1;
}

pub fn joinOutputFieldByOrdinalAlloc(
    alloc: std.mem.Allocator,
    select: []const db_mod.types.RelationalRowsJoinProjection,
    ordinal: u32,
) ![]const u8 {
    if (ordinal == 0) return error.UnsupportedSqlShape;
    const index: usize = @intCast(ordinal - 1);
    if (index >= select.len) return error.UnsupportedSqlShape;
    return try alloc.dupe(u8, select[index].output);
}

pub fn joinOutputColumnsAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    select: []const db_mod.types.RelationalRowsJoinProjection,
) ![]runtime_schema.RelationalColumn {
    if (select.len == 0) return &.{};
    const out = try alloc.alloc(runtime_schema.RelationalColumn, select.len);
    var initialized: usize = 0;
    errdefer alloc.free(out);
    for (select) |projection| {
        if (aggregateOutputColumnExists(out[0..initialized], projection.output)) return error.UnsupportedSqlShape;
        const column = binder.relationalColumnForField(schema, projection.field, null) orelse return error.InvalidSqlCatalog;
        out[initialized] = .{
            .name = projection.output,
            .path = projection.output,
            .field_type = column.field_type,
            .array_item_type = column.array_item_type,
            .nullable = true,
        };
        initialized += 1;
    }
    return out;
}

pub fn identifierContainsQualifier(identifier: []const u8) bool {
    const dot = std.mem.indexOfScalar(u8, identifier, '.') orelse return false;
    return dot > 0 and dot + 1 < identifier.len;
}

pub fn tailMentionsAnyQualifierBeforeClose(
    tokens: []const Token,
    pos: usize,
    qualifiers: []const []const u8,
) bool {
    var depth: usize = 0;
    var i = pos;
    while (i < tokens.len) : (i += 1) {
        const token = tokens[i];
        switch (token.kind) {
            .lparen => depth += 1,
            .rparen => {
                if (depth == 0) return false;
                depth -= 1;
            },
            .identifier => {
                const dot = std.mem.indexOfScalar(u8, token.text, '.') orelse continue;
                const qualifier = token.text[0..dot];
                for (qualifiers) |candidate| {
                    if (std.mem.eql(u8, qualifier, candidate)) return true;
                }
            },
            else => {},
        }
    }
    return false;
}

fn optionalStringEqual(lhs: ?[]const u8, rhs: ?[]const u8) bool {
    if (lhs == null or rhs == null) return lhs == null and rhs == null;
    return std.mem.eql(u8, lhs.?, rhs.?);
}

fn floatSlicesEqual(lhs: []const f64, rhs: []const f64) bool {
    if (lhs.len != rhs.len) return false;
    for (lhs, rhs) |left, right| {
        if (left != right) return false;
    }
    return true;
}

fn orderByEqual(lhs: []const db_mod.types.RelationalRowsQueryOrder, rhs: []const db_mod.types.RelationalRowsQueryOrder) bool {
    if (lhs.len != rhs.len) return false;
    for (lhs, rhs) |lhs_order, rhs_order| {
        if (lhs_order.direction != rhs_order.direction or
            lhs_order.null_test != rhs_order.null_test or
            !std.mem.eql(u8, lhs_order.field, rhs_order.field) or
            !relationalRowsExpressionOptionalEqual(lhs_order.expression, rhs_order.expression))
        {
            return false;
        }
    }
    return true;
}

fn relationalChecksEqual(lhs: []const runtime_schema.RelationalCheck, rhs: []const runtime_schema.RelationalCheck) bool {
    if (lhs.len != rhs.len) return false;
    for (lhs, rhs) |lhs_check, rhs_check| {
        if (!std.mem.eql(u8, lhs_check.name, rhs_check.name) or
            !std.mem.eql(u8, lhs_check.field, rhs_check.field) or
            lhs_check.op != rhs_check.op or
            !optionalStringEqual(lhs_check.value_json, rhs_check.value_json))
        {
            return false;
        }
    }
    return true;
}

fn structuredValuePredicatesEqual(lhs: anytype, rhs: @TypeOf(lhs)) bool {
    if (lhs.len != rhs.len) return false;
    for (lhs, rhs) |lhs_predicate, rhs_predicate| {
        if (!std.mem.eql(u8, lhs_predicate.field, rhs_predicate.field) or
            !std.mem.eql(u8, lhs_predicate.value_json, rhs_predicate.value_json))
        {
            return false;
        }
    }
    return true;
}

fn inPredicatesEqual(
    lhs: []const db_mod.types.RelationalRowsInPredicate,
    rhs: []const db_mod.types.RelationalRowsInPredicate,
) bool {
    if (lhs.len != rhs.len) return false;
    for (lhs, rhs) |lhs_predicate, rhs_predicate| {
        if (!std.mem.eql(u8, lhs_predicate.field, rhs_predicate.field) or
            !std.mem.eql(u8, lhs_predicate.values_json, rhs_predicate.values_json) or
            lhs_predicate.negated != rhs_predicate.negated)
        {
            return false;
        }
    }
    return true;
}

fn jsonContainsEqual(
    lhs: []const db_mod.types.RelationalRowsJsonContainsPredicate,
    rhs: []const db_mod.types.RelationalRowsJsonContainsPredicate,
) bool {
    if (lhs.len != rhs.len) return false;
    for (lhs, rhs) |lhs_predicate, rhs_predicate| {
        if (!std.mem.eql(u8, lhs_predicate.field, rhs_predicate.field) or
            !std.mem.eql(u8, lhs_predicate.value_json, rhs_predicate.value_json))
        {
            return false;
        }
    }
    return true;
}

fn jsonPathEqEqual(
    lhs: []const db_mod.types.RelationalRowsJsonPathEqPredicate,
    rhs: []const db_mod.types.RelationalRowsJsonPathEqPredicate,
) bool {
    if (lhs.len != rhs.len) return false;
    for (lhs, rhs) |lhs_predicate, rhs_predicate| {
        if (!std.mem.eql(u8, lhs_predicate.field, rhs_predicate.field) or
            !std.mem.eql(u8, lhs_predicate.path, rhs_predicate.path) or
            !std.mem.eql(u8, lhs_predicate.value_json, rhs_predicate.value_json))
        {
            return false;
        }
    }
    return true;
}

fn jsonPathExistsEqual(
    lhs: []const db_mod.types.RelationalRowsJsonPathExistsPredicate,
    rhs: []const db_mod.types.RelationalRowsJsonPathExistsPredicate,
) bool {
    if (lhs.len != rhs.len) return false;
    for (lhs, rhs) |lhs_predicate, rhs_predicate| {
        if (!std.mem.eql(u8, lhs_predicate.field, rhs_predicate.field) or
            !std.mem.eql(u8, lhs_predicate.path, rhs_predicate.path))
        {
            return false;
        }
    }
    return true;
}

fn textPatternsEqual(
    lhs: []const db_mod.types.RelationalRowsTextPatternPredicate,
    rhs: []const db_mod.types.RelationalRowsTextPatternPredicate,
) bool {
    if (lhs.len != rhs.len) return false;
    for (lhs, rhs) |lhs_predicate, rhs_predicate| {
        if (!std.mem.eql(u8, lhs_predicate.field, rhs_predicate.field) or
            !std.mem.eql(u8, lhs_predicate.pattern, rhs_predicate.pattern) or
            lhs_predicate.case_insensitive != rhs_predicate.case_insensitive or
            lhs_predicate.negated != rhs_predicate.negated)
        {
            return false;
        }
    }
    return true;
}

pub fn rowExpressionDeterministic(expression: runtime_schema.RelationalRowsExpression) bool {
    if (expression.field_source != .row) return false;
    if (expression.kind == .now or expression.kind == .uuid_v4) return false;
    for (expression.operands) |operand| {
        if (!rowExpressionDeterministic(operand)) return false;
    }
    for (expression.case_branches) |branch| {
        if (!rowExpressionConditionDeterministic(branch.when)) return false;
        if (!rowExpressionDeterministic(branch.then)) return false;
    }
    for (expression.case_else) |case_else| {
        if (!rowExpressionDeterministic(case_else)) return false;
    }
    return true;
}

pub fn rowExpressionConditionDeterministic(condition: runtime_schema.RelationalRowsExpressionCondition) bool {
    if (!rowExpressionDeterministic(condition.lhs)) return false;
    for (condition.rhs) |rhs| {
        if (!rowExpressionDeterministic(rhs)) return false;
    }
    return true;
}

pub fn writeRowExpressionJson(writer: *std.Io.Writer, expression: db_mod.types.RelationalRowsExpression) !void {
    switch (expression.kind) {
        .field => {
            try writer.print("{{\"field\":{f}", .{std.json.fmt(expression.field, .{})});
            if (expression.field_source != .row) {
                try writer.print(",\"source\":{f}", .{std.json.fmt(rowExpressionFieldSourceName(expression.field_source), .{})});
            }
            try writer.writeByte('}');
        },
        .value => {
            try writer.writeAll("{\"value\":");
            try writer.writeAll(expression.value_json);
            try writer.writeByte('}');
        },
        .now => {
            try writer.writeAll("{\"op\":\"now\",\"args\":[]}");
        },
        .case => {
            try writer.writeAll("{\"op\":\"case\",\"cases\":[");
            for (expression.case_branches, 0..) |branch, i| {
                if (i != 0) try writer.writeByte(',');
                try writer.writeAll("{\"when\":{\"lhs\":");
                try writeRowExpressionJson(writer, branch.when.lhs);
                try writer.print(",\"op\":{f}", .{std.json.fmt(rowExpressionConditionOpName(branch.when.op), .{})});
                if (branch.when.rhs.len == 1) {
                    try writer.writeAll(",\"rhs\":");
                    try writeRowExpressionJson(writer, branch.when.rhs[0]);
                }
                try writer.writeAll("},\"then\":");
                try writeRowExpressionJson(writer, branch.then);
                try writer.writeByte('}');
            }
            try writer.writeAll("],\"else\":");
            if (expression.case_else.len != 1) return error.UnsupportedSqlShape;
            try writeRowExpressionJson(writer, expression.case_else[0]);
            try writer.writeByte('}');
        },
        .cast => {
            if (expression.operands.len != 1) return error.UnsupportedSqlShape;
            const cast_type = expression.cast_type orelse return error.UnsupportedSqlShape;
            try writer.print("{{\"op\":\"cast\",\"to\":{f},\"args\":[", .{std.json.fmt(rowExpressionCastTypeName(cast_type), .{})});
            try writeRowExpressionJson(writer, expression.operands[0]);
            try writer.writeAll("]}");
        },
        .json_extract => {
            if (expression.operands.len != 1 or expression.json_path.len == 0) return error.UnsupportedSqlShape;
            try writer.writeAll("{\"op\":\"json_extract\",\"args\":[");
            try writeRowExpressionJson(writer, expression.operands[0]);
            try writer.print("],\"path\":{f}", .{std.json.fmt(expression.json_path, .{})});
            if (expression.json_as_text) try writer.writeAll(",\"as_text\":true");
            try writer.writeByte('}');
        },
        else => {
            try writer.print("{{\"op\":{f},\"args\":[", .{std.json.fmt(rowExpressionOpName(expression.kind), .{})});
            for (expression.operands, 0..) |operand, i| {
                if (i != 0) try writer.writeByte(',');
                try writeRowExpressionJson(writer, operand);
            }
            try writer.writeAll("]}");
        },
    }
}

pub fn writeRowExpressionConditionJson(writer: *std.Io.Writer, condition: db_mod.types.RelationalRowsExpressionCondition) !void {
    try writer.writeAll("{\"lhs\":");
    try writeRowExpressionJson(writer, condition.lhs);
    try writer.print(",\"op\":{f}", .{std.json.fmt(rowExpressionConditionOpName(condition.op), .{})});
    if (condition.rhs.len == 1) {
        try writer.writeAll(",\"rhs\":");
        try writeRowExpressionJson(writer, condition.rhs[0]);
    }
    try writer.writeByte('}');
}

pub fn rowRewriteExpressionFingerprintAlloc(
    alloc: std.mem.Allocator,
    expression: db_mod.types.RelationalRowsExpression,
) ![]u8 {
    switch (expression.kind) {
        .field => return try std.fmt.allocPrint(
            alloc,
            "field[{s}:{s}]",
            .{ @tagName(expression.field_source), expression.field },
        ),
        .value => return try std.fmt.allocPrint(
            alloc,
            "value[{x}]",
            .{expression.value_json},
        ),
        .cast => {
            if (expression.operands.len != 1) return error.UnsupportedSqlShape;
            const operand = try rowRewriteExpressionFingerprintAlloc(alloc, expression.operands[0]);
            defer alloc.free(operand);
            return try std.fmt.allocPrint(
                alloc,
                "cast[{s}|{s}]",
                .{ if (expression.cast_type) |cast_type| @tagName(cast_type) else "none", operand },
            );
        },
        .json_extract => {
            if (expression.operands.len != 1) return error.UnsupportedSqlShape;
            const operand = try rowRewriteExpressionFingerprintAlloc(alloc, expression.operands[0]);
            defer alloc.free(operand);
            return try std.fmt.allocPrint(
                alloc,
                "json_extract[{x}|text={}|{s}]",
                .{ expression.json_path, expression.json_as_text, operand },
            );
        },
        .case => {
            if (expression.case_branches.len == 0) return error.UnsupportedSqlShape;
            return try std.fmt.allocPrint(
                alloc,
                "case[branches={d}:else={d}]",
                .{ expression.case_branches.len, expression.case_else.len },
            );
        },
        else => {
            var out: std.Io.Writer.Allocating = .init(alloc);
            errdefer out.deinit();
            const writer = &out.writer;
            try writer.print("{s}[", .{@tagName(expression.kind)});
            for (expression.operands, 0..) |operand, i| {
                const operand_fingerprint = try rowRewriteExpressionFingerprintAlloc(alloc, operand);
                defer alloc.free(operand_fingerprint);
                if (i != 0) try writer.writeByte('+');
                try writer.writeAll(operand_fingerprint);
            }
            try writer.writeByte(']');
            return try out.toOwnedSlice();
        },
    }
}

pub fn rowSecurityPredicateFingerprintSuffixAlloc(
    alloc: std.mem.Allocator,
    predicate: ddl_plan.RowSecurityPolicyPredicate,
) ![]u8 {
    return switch (predicate) {
        .current_setting_equals => |current_setting| try std.fmt.allocPrint(
            alloc,
            "kind=current_setting_eq:field={s}:setting={s}",
            .{ current_setting.field, current_setting.setting_name },
        ),
        .literal_equals => |literal| blk: {
            const value_json_hex = try fingerprintStringOptionHexAlloc(alloc, literal.value_json);
            defer alloc.free(value_json_hex);
            break :blk try std.fmt.allocPrint(
                alloc,
                "kind=literal_eq:field={s}:value_json_hex={s}",
                .{ literal.field, value_json_hex },
            );
        },
        .expression => |expression| blk: {
            const expression_json = try rowSecurityExpressionConditionJsonAlloc(alloc, expression);
            defer alloc.free(expression_json);
            const expression_json_hex = try fingerprintStringOptionHexAlloc(alloc, expression_json);
            defer alloc.free(expression_json_hex);
            break :blk try std.fmt.allocPrint(alloc, "kind=expression:json_hex={s}", .{expression_json_hex});
        },
        .conjunction => |conjunction| blk: {
            var out = try std.fmt.allocPrint(alloc, "kind=and:terms={d}", .{conjunction.predicates.len});
            errdefer alloc.free(out);
            for (conjunction.predicates) |term| {
                const term_suffix = try rowSecurityPredicateFingerprintSuffixAlloc(alloc, term);
                defer alloc.free(term_suffix);
                const next = try std.fmt.allocPrint(alloc, "{s}:term={s}", .{ out, term_suffix });
                alloc.free(out);
                out = next;
            }
            break :blk out;
        },
        .disjunction => |disjunction| blk: {
            var out = try std.fmt.allocPrint(alloc, "kind=or:terms={d}", .{disjunction.predicates.len});
            errdefer alloc.free(out);
            for (disjunction.predicates) |term| {
                const term_suffix = try rowSecurityPredicateFingerprintSuffixAlloc(alloc, term);
                defer alloc.free(term_suffix);
                const next = try std.fmt.allocPrint(alloc, "{s}:term={s}", .{ out, term_suffix });
                alloc.free(out);
                out = next;
            }
            break :blk out;
        },
    };
}

fn rowSecurityExpressionConditionJsonAlloc(
    alloc: std.mem.Allocator,
    condition: db_mod.types.RelationalRowsExpressionCondition,
) ![]u8 {
    var out: std.Io.Writer.Allocating = .init(alloc);
    errdefer out.deinit();
    try writeRowExpressionConditionJson(&out.writer, condition);
    return try out.toOwnedSlice();
}

fn fingerprintStringOptionHexAlloc(alloc: std.mem.Allocator, option: ?[]const u8) ![]const u8 {
    const value = option orelse return try alloc.dupe(u8, "default");
    if (value.len == 0) return try alloc.dupe(u8, "empty");
    const out = try alloc.alloc(u8, value.len * 2);
    const hex = "0123456789abcdef";
    for (value, 0..) |byte, i| {
        out[i * 2] = hex[byte >> 4];
        out[i * 2 + 1] = hex[byte & 0x0f];
    }
    return out;
}

pub fn writeRowExpressionPredicateGroupsJson(
    writer: *std.Io.Writer,
    field_name: []const u8,
    groups: []const db_mod.types.RelationalRowsExpressionPredicateGroup,
) !void {
    try writer.print("{f}:[", .{std.json.fmt(field_name, .{})});
    for (groups, 0..) |group, group_i| {
        if (group_i != 0) try writer.writeByte(',');
        try writer.writeAll("{\"all\":[");
        for (group.conditions, 0..) |condition, condition_i| {
            if (condition_i != 0) try writer.writeByte(',');
            try writeRowExpressionConditionJson(writer, condition);
        }
        try writer.writeAll("]}");
    }
    try writer.writeByte(']');
}

pub fn rowExpressionOpName(kind: db_mod.types.RelationalRowsExpressionKind) []const u8 {
    return switch (kind) {
        .field => "field",
        .value => "value",
        .now => "now",
        .uuid_v4 => "uuid_v4",
        .coalesce => "coalesce",
        .lower => "lower",
        .upper => "upper",
        .initcap => "initcap",
        .trim => "trim",
        .ltrim => "ltrim",
        .rtrim => "rtrim",
        .replace => "replace",
        .regexp_replace => "regexp_replace",
        .regexp_substr => "regexp_substr",
        .regexp_count => "regexp_count",
        .regexp_instr => "regexp_instr",
        .translate => "translate",
        .substring => "substring",
        .overlay => "overlay",
        .split_part => "split_part",
        .strpos => "strpos",
        .ascii => "ascii",
        .left => "left",
        .right => "right",
        .lpad => "lpad",
        .rpad => "rpad",
        .repeat => "repeat",
        .reverse => "reverse",
        .md5 => "md5",
        .starts_with => "starts_with",
        .ends_with => "ends_with",
        .chr => "chr",
        .like => "like",
        .ilike => "ilike",
        .regexp_match => "regexp_match",
        .bool_and => "and",
        .bool_or => "or",
        .bool_not => "not",
        .date_trunc => "date_trunc",
        .date_bin => "date_bin",
        .date_part => "date_part",
        .concat => "concat",
        .concat_ws => "concat_ws",
        .length => "length",
        .octet_length => "octet_length",
        .bit_length => "bit_length",
        .nullif => "nullif",
        .greatest => "greatest",
        .least => "least",
        .abs => "abs",
        .round => "round",
        .trunc => "trunc",
        .floor => "floor",
        .ceil => "ceil",
        .sqrt => "sqrt",
        .sign => "sign",
        .power => "power",
        .add => "add",
        .sub => "sub",
        .mul => "mul",
        .div => "div",
        .mod => "mod",
        .case => "case",
        .cast => "cast",
        .json_extract => "json_extract",
        .json_path_exists => "json_path_exists",
        .json_typeof => "json_typeof",
        .json_array_length => "json_array_length",
        .json_build_object => "jsonb_build_object",
        .to_jsonb => "to_jsonb",
        .array_length => "array_length",
        .array_position => "array_position",
        .array_positions => "array_positions",
        .array_append => "array_append",
        .array_prepend => "array_prepend",
        .array_cat => "array_cat",
        .array_remove => "array_remove",
        .array_replace => "array_replace",
        .array_to_string => "array_to_string",
        .string_to_array => "string_to_array",
        .interval_ns => "interval_ns",
        .interval_months => "interval_months",
    };
}

pub fn rowExpressionDefaultOutputName(kind: db_mod.types.RelationalRowsExpressionKind) []const u8 {
    return switch (kind) {
        .field => "field",
        .value => "value",
        .case => "case",
        .cast => "cast",
        .json_extract => "json_extract",
        .json_typeof => "json_typeof",
        .json_array_length => "json_array_length",
        .json_build_object => "jsonb_build_object",
        .to_jsonb => "to_jsonb",
        else => rowExpressionOpName(kind),
    };
}

pub fn rowExpressionFieldSourceName(source: db_mod.types.RelationalRowsExpressionFieldSource) []const u8 {
    return switch (source) {
        .row => "row",
        .existing => "existing",
        .proposed => "proposed",
        .source => "source",
    };
}

pub fn rowExpressionCastTypeName(cast_type: db_mod.types.RelationalRowsExpressionCastType) []const u8 {
    return switch (cast_type) {
        .text => "text",
        .numeric => "numeric",
        .bool => "bool",
        .datetime => "datetime",
    };
}

pub fn rowExpressionConditionOpName(op: runtime_schema.RelationalCheckOp) []const u8 {
    return switch (op) {
        .eq => "eq",
        .ne => "ne",
        .gt => "gt",
        .gte => "gte",
        .lt => "lt",
        .lte => "lte",
        .is_null => "is_null",
        .is_not_null => "is_not_null",
        .is_distinct => "is_distinct",
        .is_not_distinct => "is_not_distinct",
    };
}

pub fn writeRelationalCheckAtomsJson(
    writer: *std.Io.Writer,
    wrote_atom: *bool,
    predicates: []const runtime_schema.RelationalCheck,
) !void {
    for (predicates) |predicate| {
        if (wrote_atom.*) try writer.writeByte(',');
        try writeRelationalCheckAtomJson(writer, predicate);
        wrote_atom.* = true;
    }
}

pub fn writeRelationalCheckAtomJson(writer: *std.Io.Writer, predicate: runtime_schema.RelationalCheck) !void {
    try writer.print("{{\"field\":{f},\"op\":{f}", .{
        std.json.fmt(predicate.field, .{}),
        std.json.fmt(ddl_plan.relationalCheckOpToken(predicate.op), .{}),
    });
    if (predicate.value_json) |value_json| {
        try writer.writeAll(",\"value\":");
        try writer.writeAll(value_json);
    }
    try writer.writeByte('}');
}

pub fn writeInPredicateAtomsJson(
    writer: *std.Io.Writer,
    wrote_atom: *bool,
    predicates: []const db_mod.types.RelationalRowsInPredicate,
) !void {
    for (predicates) |predicate| {
        if (wrote_atom.*) try writer.writeByte(',');
        try writer.print("{{\"field\":{f},\"op\":{f},\"value\":", .{
            std.json.fmt(predicate.field, .{}),
            std.json.fmt(if (predicate.negated) "not_in" else "in", .{}),
        });
        try writer.writeAll(predicate.values_json);
        try writer.writeByte('}');
        wrote_atom.* = true;
    }
}

pub fn writeTextPatternPredicateAtomsJson(
    writer: *std.Io.Writer,
    wrote_atom: *bool,
    predicates: []const db_mod.types.RelationalRowsTextPatternPredicate,
) !void {
    for (predicates) |predicate| {
        if (wrote_atom.*) try writer.writeByte(',');
        try writer.print("{{\"field\":{f},\"op\":\"text_pattern\",\"pattern\":{f}", .{
            std.json.fmt(predicate.field, .{}),
            std.json.fmt(predicate.pattern, .{}),
        });
        if (predicate.case_insensitive) try writer.writeAll(",\"case_insensitive\":true");
        if (predicate.negated) try writer.writeAll(",\"negated\":true");
        try writer.writeByte('}');
        wrote_atom.* = true;
    }
}

pub fn writeStructuredValuePredicateAtomsJson(
    writer: *std.Io.Writer,
    wrote_atom: *bool,
    op_name: []const u8,
    predicates: anytype,
) !void {
    for (predicates) |predicate| {
        if (wrote_atom.*) try writer.writeByte(',');
        try writer.print("{{\"field\":{f},\"op\":{f},\"value\":", .{
            std.json.fmt(predicate.field, .{}),
            std.json.fmt(op_name, .{}),
        });
        try writer.writeAll(predicate.value_json);
        try writer.writeByte('}');
        wrote_atom.* = true;
    }
}

pub fn writeJsonPathEqPredicateAtomsJson(
    writer: *std.Io.Writer,
    wrote_atom: *bool,
    predicates: []const db_mod.types.RelationalRowsJsonPathEqPredicate,
) !void {
    for (predicates) |predicate| {
        if (wrote_atom.*) try writer.writeByte(',');
        try writer.print("{{\"field\":{f},\"op\":\"json_path_eq\",\"path\":{f},\"value\":", .{
            std.json.fmt(predicate.field, .{}),
            std.json.fmt(predicate.path, .{}),
        });
        try writer.writeAll(predicate.value_json);
        try writer.writeByte('}');
        wrote_atom.* = true;
    }
}

pub fn writeJsonPathExistsPredicateAtomsJson(
    writer: *std.Io.Writer,
    wrote_atom: *bool,
    predicates: []const db_mod.types.RelationalRowsJsonPathExistsPredicate,
) !void {
    for (predicates) |predicate| {
        if (wrote_atom.*) try writer.writeByte(',');
        try writer.print("{{\"field\":{f},\"op\":\"json_path_exists\",\"path\":{f}}}", .{
            std.json.fmt(predicate.field, .{}),
            std.json.fmt(predicate.path, .{}),
        });
        wrote_atom.* = true;
    }
}

pub fn validateCheckExpressionConditionForColumns(
    columns: []const runtime_schema.RelationalColumn,
    condition: runtime_schema.RelationalRowsExpressionCondition,
) anyerror!void {
    switch (condition.op) {
        .is_null, .is_not_null => if (condition.rhs.len != 0) return error.InvalidSqlCatalog,
        .eq, .ne, .is_distinct, .is_not_distinct, .gt, .gte, .lt, .lte => if (condition.rhs.len != 1) return error.InvalidSqlCatalog,
    }
    const lhs_type = try checkExpressionTypeForColumns(columns, condition.lhs);
    if (condition.rhs.len == 0) return;
    const rhs_type = try checkExpressionTypeForColumns(columns, condition.rhs[0]);
    if (!checkExpressionTypesComparable(lhs_type, rhs_type)) return error.InvalidSqlCatalog;
    switch (condition.op) {
        .gt, .gte, .lt, .lte => if (!checkExpressionTypeOrderable(lhs_type) or !checkExpressionTypeOrderable(rhs_type)) return error.InvalidSqlCatalog,
        else => {},
    }
}

pub fn validateCheckExpressionForColumns(
    columns: []const runtime_schema.RelationalColumn,
    expression: runtime_schema.RelationalRowsExpression,
) anyerror!void {
    _ = try checkExpressionTypeForColumns(columns, expression);
}

pub const CheckExpressionType = union(enum) {
    type: runtime_schema.AntflyType,
    null,
};

fn checkExpressionContainsInterval(expression: runtime_schema.RelationalRowsExpression) bool {
    if (expression.kind == .interval_ns or expression.kind == .interval_months) return true;
    for (expression.operands) |operand| {
        if (checkExpressionContainsInterval(operand)) return true;
    }
    for (expression.case_branches) |branch| {
        if (checkExpressionContainsInterval(branch.then)) return true;
        if (checkExpressionContainsInterval(branch.when.lhs)) return true;
        for (branch.when.rhs) |rhs| {
            if (checkExpressionContainsInterval(rhs)) return true;
        }
    }
    for (expression.case_else) |case_else| {
        if (checkExpressionContainsInterval(case_else)) return true;
    }
    return false;
}

fn validateDateBinStrideExpressionForColumns(
    columns: []const runtime_schema.RelationalColumn,
    expression: runtime_schema.RelationalRowsExpression,
) anyerror!void {
    switch (expression.kind) {
        .interval_ns => {
            if (expression.operands.len != 1) return error.InvalidSqlCatalog;
            if (checkExpressionContainsInterval(expression.operands[0])) return error.InvalidSqlCatalog;
            if (!checkExpressionTypeEquals(try checkExpressionTypeForColumns(columns, expression.operands[0]), .numeric)) return error.InvalidSqlCatalog;
        },
        .interval_months => return error.InvalidSqlCatalog,
        else => {
            if (checkExpressionContainsInterval(expression)) return error.InvalidSqlCatalog;
            if (!checkExpressionTypeEquals(try checkExpressionTypeForColumns(columns, expression), .numeric)) return error.InvalidSqlCatalog;
        },
    }
}

pub fn checkExpressionTypeForColumns(
    columns: []const runtime_schema.RelationalColumn,
    expression: runtime_schema.RelationalRowsExpression,
) anyerror!CheckExpressionType {
    if (expression.kind == .field) {
        if (expression.field_source != .row) return error.InvalidSqlCatalog;
        const column = binder.relationalColumnForDdl(columns, expression.field) orelse return error.InvalidSqlCatalog;
        return .{ .type = column.field_type };
    }
    if (expression.kind == .value) return checkExpressionLiteralType(expression.value_json);

    for (expression.case_branches) |branch| try validateCheckExpressionConditionForColumns(columns, branch.when);

    switch (expression.kind) {
        .field, .value => unreachable,
        .now => return .{ .type = .datetime },
        .uuid_v4 => return .{ .type = .text },
        .regexp_replace, .regexp_substr => {
            if (expression.kind == .regexp_replace and expression.operands.len != 3 and expression.operands.len != 4) return error.InvalidSqlCatalog;
            if (expression.kind == .regexp_substr and expression.operands.len != 2) return error.InvalidSqlCatalog;
            for (expression.operands) |operand| {
                if (!checkExpressionTypeTextLike(try checkExpressionTypeForColumns(columns, operand))) return error.InvalidSqlCatalog;
            }
            return .{ .type = .text };
        },
        .lower, .upper, .initcap, .trim, .ltrim, .rtrim, .replace, .translate, .substring, .overlay, .split_part, .left, .right, .lpad, .rpad, .repeat, .reverse, .chr, .md5, .concat, .concat_ws => {
            for (expression.operands) |operand| {
                if (!checkExpressionTypeTextLike(try checkExpressionTypeForColumns(columns, operand))) return error.InvalidSqlCatalog;
            }
            return .{ .type = .text };
        },
        .length, .octet_length, .bit_length, .ascii, .strpos => {
            for (expression.operands) |operand| {
                if (!checkExpressionTypeTextLike(try checkExpressionTypeForColumns(columns, operand))) return error.InvalidSqlCatalog;
            }
            return .{ .type = .numeric };
        },
        .regexp_count, .regexp_instr => {
            if (expression.operands.len != 2) return error.InvalidSqlCatalog;
            for (expression.operands) |operand| {
                if (!checkExpressionTypeTextLike(try checkExpressionTypeForColumns(columns, operand))) return error.InvalidSqlCatalog;
            }
            return .{ .type = .numeric };
        },
        .starts_with, .ends_with, .like, .ilike => {
            for (expression.operands) |operand| {
                if (!checkExpressionTypeTextLike(try checkExpressionTypeForColumns(columns, operand))) return error.InvalidSqlCatalog;
            }
            return .{ .type = .boolean };
        },
        .regexp_match => {
            if (expression.operands.len != 2 and expression.operands.len != 3) return error.InvalidSqlCatalog;
            for (expression.operands[0..2]) |operand| {
                if (!checkExpressionTypeTextLike(try checkExpressionTypeForColumns(columns, operand))) return error.InvalidSqlCatalog;
            }
            if (expression.operands.len == 3 and !checkExpressionTypeEquals(try checkExpressionTypeForColumns(columns, expression.operands[2]), .boolean)) return error.InvalidSqlCatalog;
            return .{ .type = .boolean };
        },
        .bool_and, .bool_or, .bool_not => {
            for (expression.operands) |operand| {
                if (!checkExpressionTypeEquals(try checkExpressionTypeForColumns(columns, operand), .boolean)) return error.InvalidSqlCatalog;
            }
            return .{ .type = .boolean };
        },
        .abs, .round, .trunc, .floor, .ceil, .sqrt, .sign, .power, .mul, .div, .mod, .interval_ns, .interval_months => {
            for (expression.operands) |operand| {
                if (!checkExpressionTypeEquals(try checkExpressionTypeForColumns(columns, operand), .numeric)) return error.InvalidSqlCatalog;
            }
            return .{ .type = .numeric };
        },
        .add, .sub => {
            var saw_datetime = false;
            for (expression.operands) |operand| {
                const operand_type = try checkExpressionTypeForColumns(columns, operand);
                switch (operand_type) {
                    .type => |field_type| {
                        if (field_type == .datetime) saw_datetime = true else if (field_type != .numeric) return error.InvalidSqlCatalog;
                    },
                    .null => return error.InvalidSqlCatalog,
                }
            }
            return .{ .type = if (saw_datetime) .datetime else .numeric };
        },
        .date_trunc => {
            if (expression.operands.len != 2) return error.InvalidSqlCatalog;
            if (!checkExpressionTypeTextLike(try checkExpressionTypeForColumns(columns, expression.operands[0]))) return error.InvalidSqlCatalog;
            const value_type = try checkExpressionTypeForColumns(columns, expression.operands[1]);
            if (!checkExpressionTypeEquals(value_type, .datetime) and !checkExpressionTypeEquals(value_type, .numeric)) return error.InvalidSqlCatalog;
            return .{ .type = .datetime };
        },
        .date_bin => {
            if (expression.operands.len != 3) return error.InvalidSqlCatalog;
            try validateDateBinStrideExpressionForColumns(columns, expression.operands[0]);
            const source_type = try checkExpressionTypeForColumns(columns, expression.operands[1]);
            if (!checkExpressionTypeEquals(source_type, .datetime) and !checkExpressionTypeEquals(source_type, .numeric)) return error.InvalidSqlCatalog;
            const origin_type = try checkExpressionTypeForColumns(columns, expression.operands[2]);
            if (!checkExpressionTypeEquals(origin_type, .datetime) and !checkExpressionTypeEquals(origin_type, .numeric)) return error.InvalidSqlCatalog;
            return .{ .type = .datetime };
        },
        .date_part => {
            if (expression.operands.len != 2) return error.InvalidSqlCatalog;
            if (!checkExpressionTypeTextLike(try checkExpressionTypeForColumns(columns, expression.operands[0]))) return error.InvalidSqlCatalog;
            const value_type = try checkExpressionTypeForColumns(columns, expression.operands[1]);
            if (!checkExpressionTypeEquals(value_type, .datetime) and !checkExpressionTypeEquals(value_type, .numeric)) return error.InvalidSqlCatalog;
            return .{ .type = .numeric };
        },
        .cast => return .{ .type = switch (expression.cast_type orelse return error.InvalidSqlCatalog) {
            .text => .text,
            .numeric => .numeric,
            .bool => .boolean,
            .datetime => .datetime,
        } },
        .json_extract => {
            if (expression.operands.len != 1) return error.InvalidSqlCatalog;
            if (!checkExpressionTypeEquals(try checkExpressionTypeForColumns(columns, expression.operands[0]), .json)) return error.InvalidSqlCatalog;
            return .{ .type = if (expression.json_as_text) .text else .json };
        },
        .json_path_exists => {
            if (expression.operands.len != 1 or expression.json_path.len == 0) return error.InvalidSqlCatalog;
            if (!checkExpressionTypeEquals(try checkExpressionTypeForColumns(columns, expression.operands[0]), .json)) return error.InvalidSqlCatalog;
            return .{ .type = .boolean };
        },
        .json_typeof => {
            if (expression.operands.len != 1) return error.InvalidSqlCatalog;
            if (!checkExpressionTypeEquals(try checkExpressionTypeForColumns(columns, expression.operands[0]), .json)) return error.InvalidSqlCatalog;
            return .{ .type = .text };
        },
        .json_array_length => {
            if (expression.operands.len != 1) return error.InvalidSqlCatalog;
            if (!checkExpressionTypeEquals(try checkExpressionTypeForColumns(columns, expression.operands[0]), .json)) return error.InvalidSqlCatalog;
            return .{ .type = .numeric };
        },
        .json_build_object => {
            if (expression.operands.len % 2 != 0) return error.InvalidSqlCatalog;
            var index: usize = 0;
            while (index < expression.operands.len) : (index += 2) {
                if (!checkExpressionTypeTextLike(try checkExpressionTypeForColumns(columns, expression.operands[index]))) return error.InvalidSqlCatalog;
                _ = try checkExpressionTypeForColumns(columns, expression.operands[index + 1]);
            }
            return .{ .type = .json };
        },
        .to_jsonb => {
            if (expression.operands.len != 1) return error.InvalidSqlCatalog;
            _ = try checkExpressionTypeForColumns(columns, expression.operands[0]);
            return .{ .type = .json };
        },
        .array_length => {
            if (expression.operands.len != 1) return error.InvalidSqlCatalog;
            if (!checkExpressionTypeEquals(try checkExpressionTypeForColumns(columns, expression.operands[0]), .array)) return error.InvalidSqlCatalog;
            return .{ .type = .numeric };
        },
        .array_position => {
            if (expression.operands.len != 2) return error.InvalidSqlCatalog;
            if (!checkExpressionTypeEquals(try checkExpressionTypeForColumns(columns, expression.operands[0]), .array)) return error.InvalidSqlCatalog;
            _ = try checkExpressionTypeForColumns(columns, expression.operands[1]);
            return .{ .type = .numeric };
        },
        .array_positions => {
            if (expression.operands.len != 2) return error.InvalidSqlCatalog;
            if (!checkExpressionTypeEquals(try checkExpressionTypeForColumns(columns, expression.operands[0]), .array)) return error.InvalidSqlCatalog;
            _ = try checkExpressionTypeForColumns(columns, expression.operands[1]);
            return .{ .type = .array };
        },
        .array_append, .array_prepend, .array_remove => {
            if (expression.operands.len != 2) return error.InvalidSqlCatalog;
            if (!checkExpressionTypeEquals(try checkExpressionTypeForColumns(columns, expression.operands[0]), .array)) return error.InvalidSqlCatalog;
            _ = try checkExpressionTypeForColumns(columns, expression.operands[1]);
            return .{ .type = .array };
        },
        .array_replace => {
            if (expression.operands.len != 3) return error.InvalidSqlCatalog;
            if (!checkExpressionTypeEquals(try checkExpressionTypeForColumns(columns, expression.operands[0]), .array)) return error.InvalidSqlCatalog;
            _ = try checkExpressionTypeForColumns(columns, expression.operands[1]);
            _ = try checkExpressionTypeForColumns(columns, expression.operands[2]);
            return .{ .type = .array };
        },
        .array_cat => {
            if (expression.operands.len != 2) return error.InvalidSqlCatalog;
            if (!checkExpressionTypeEquals(try checkExpressionTypeForColumns(columns, expression.operands[0]), .array)) return error.InvalidSqlCatalog;
            if (!checkExpressionTypeEquals(try checkExpressionTypeForColumns(columns, expression.operands[1]), .array)) return error.InvalidSqlCatalog;
            return .{ .type = .array };
        },
        .array_to_string => {
            if (expression.operands.len != 2 and expression.operands.len != 3) return error.InvalidSqlCatalog;
            if (!checkExpressionTypeEquals(try checkExpressionTypeForColumns(columns, expression.operands[0]), .array)) return error.InvalidSqlCatalog;
            if (!checkExpressionTypeTextLike(try checkExpressionTypeForColumns(columns, expression.operands[1]))) return error.InvalidSqlCatalog;
            if (expression.operands.len == 3 and !checkExpressionTypeTextLike(try checkExpressionTypeForColumns(columns, expression.operands[2]))) return error.InvalidSqlCatalog;
            return .{ .type = .text };
        },
        .string_to_array => {
            for (expression.operands) |operand| {
                if (!checkExpressionTypeTextLike(try checkExpressionTypeForColumns(columns, operand))) return error.InvalidSqlCatalog;
            }
            return .{ .type = .array };
        },
        .coalesce, .nullif, .greatest, .least => return try checkExpressionCommonType(columns, expression.operands),
        .case => {
            if (expression.case_else.len != 1) return error.InvalidSqlCatalog;
            var candidate = try checkExpressionTypeForColumns(columns, expression.case_else[0]);
            for (expression.case_branches) |branch| {
                const branch_type = try checkExpressionTypeForColumns(columns, branch.then);
                if (!checkExpressionTypesComparable(candidate, branch_type)) return error.InvalidSqlCatalog;
                if (candidate == .null) candidate = branch_type;
            }
            return candidate;
        },
    }
}

fn checkExpressionCommonType(
    columns: []const runtime_schema.RelationalColumn,
    expressions: []const runtime_schema.RelationalRowsExpression,
) anyerror!CheckExpressionType {
    if (expressions.len == 0) return error.InvalidSqlCatalog;
    var candidate = try checkExpressionTypeForColumns(columns, expressions[0]);
    for (expressions[1..]) |expression| {
        const expression_type = try checkExpressionTypeForColumns(columns, expression);
        if (!checkExpressionTypesComparable(candidate, expression_type)) return error.InvalidSqlCatalog;
        if (candidate == .null) candidate = expression_type;
    }
    return candidate;
}

pub fn checkExpressionLiteralType(value_json: []const u8) !CheckExpressionType {
    if (std.mem.eql(u8, value_json, "null")) return .null;
    if (std.mem.eql(u8, value_json, "true") or std.mem.eql(u8, value_json, "false")) return .{ .type = .boolean };
    if (value_json.len == 0) return error.InvalidSqlCatalog;
    return switch (value_json[0]) {
        '"' => .{ .type = .text },
        '[' => .{ .type = .array },
        '{' => .{ .type = .json },
        '-', '0'...'9' => .{ .type = .numeric },
        else => error.InvalidSqlCatalog,
    };
}

pub fn checkExpressionTypesComparable(lhs: CheckExpressionType, rhs: CheckExpressionType) bool {
    if (lhs == .null or rhs == .null) return true;
    if (checkExpressionTypeTextLike(lhs) and checkExpressionTypeTextLike(rhs)) return true;
    return switch (lhs) {
        .null => true,
        .type => |lhs_type| switch (rhs) {
            .null => true,
            .type => |rhs_type| (lhs_type == .datetime and rhs_type == .numeric) or
                (lhs_type == .numeric and rhs_type == .datetime) or
                lhs_type == rhs_type,
        },
    };
}

pub fn checkExpressionTypeEquals(value: CheckExpressionType, expected: runtime_schema.AntflyType) bool {
    return switch (value) {
        .null => false,
        .type => |actual| actual == expected,
    };
}

pub fn checkExpressionTypeTextLike(value: CheckExpressionType) bool {
    return switch (value) {
        .null => false,
        .type => |field_type| switch (field_type) {
            .text, .keyword, .link, .html, .search_as_you_type, .blob, .geoshape => true,
            else => false,
        },
    };
}

pub fn checkExpressionTypeOrderable(value: CheckExpressionType) bool {
    return checkExpressionTypeTextLike(value) or
        checkExpressionTypeEquals(value, .numeric) or
        checkExpressionTypeEquals(value, .datetime) or
        checkExpressionTypeEquals(value, .boolean);
}

pub fn sqlExpressionTypesComparable(lhs: runtime_schema.AntflyType, rhs: runtime_schema.AntflyType) bool {
    if (sqlExpressionTypeIsTextLike(lhs) and sqlExpressionTypeIsTextLike(rhs)) return true;
    if ((lhs == .datetime and rhs == .numeric) or (lhs == .numeric and rhs == .datetime)) return true;
    return lhs == rhs;
}

pub fn sqlExpressionTypeIsTextLike(field_type: runtime_schema.AntflyType) bool {
    return switch (field_type) {
        .text, .keyword, .link, .html, .search_as_you_type, .blob, .geoshape => true,
        else => false,
    };
}

pub fn sqlExpressionTypeIsOrderable(field_type: runtime_schema.AntflyType) bool {
    return sqlExpressionTypeIsTextLike(field_type) or field_type == .numeric or field_type == .datetime or field_type == .boolean;
}

pub fn sqlAggregateMinMaxTypeAllowed(field_type: runtime_schema.AntflyType) bool {
    return sqlExpressionTypeIsTextLike(field_type) or field_type == .numeric or field_type == .datetime;
}

pub fn sqlAggregateModeTypeAllowed(field_type: runtime_schema.AntflyType) bool {
    return sqlExpressionTypeIsTextLike(field_type) or field_type == .numeric or field_type == .datetime or field_type == .boolean;
}

pub fn validateAggregateInputExpression(
    type_context: RowExpressionTypeContext,
    op: db_mod.types.RelationalRowsAggregateOp,
    expression: db_mod.types.RelationalRowsExpression,
) !void {
    switch (op) {
        .count, .array_agg => {},
        .string_agg => try type_context.validateTextRowExpression(expression),
        .sum, .avg, .percentile_cont, .percentile_disc => try type_context.validateNumericRowExpression(expression),
        .mode => try validateAggregateModeRowExpression(type_context, expression),
        .min, .max => try validateAggregateMinMaxRowExpression(type_context, expression),
        .bool_or, .bool_and => try type_context.validateBooleanRowExpression(expression),
    }
}

pub fn validateAggregateModeRowExpression(
    type_context: RowExpressionTypeContext,
    expression: db_mod.types.RelationalRowsExpression,
) !void {
    const output_type = try type_context.rowExpressionOutputType(expression);
    if (!sqlAggregateModeTypeAllowed(output_type)) return error.UnsupportedSqlShape;
}

pub fn validateAggregateMinMaxRowExpression(
    type_context: RowExpressionTypeContext,
    expression: db_mod.types.RelationalRowsExpression,
) !void {
    const output_type = try type_context.rowExpressionOutputType(expression);
    if (!sqlAggregateMinMaxTypeAllowed(output_type)) return error.UnsupportedSqlShape;
}

pub fn sqlExpressionTypeIsOrderKey(field_type: runtime_schema.AntflyType) bool {
    return sqlExpressionTypeIsOrderable(field_type) or field_type == .json or field_type == .array;
}

pub fn sqlExpressionResultTypesCompatible(lhs: runtime_schema.AntflyType, rhs: runtime_schema.AntflyType) bool {
    return sqlExpressionTypesComparable(lhs, rhs);
}

pub const RowExpressionTypeContext = struct {
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    joined_source_schema: ?runtime_schema.TableSchema = null,
    defer_row_expression_field_validation: bool = false,

    pub fn rowExpressionIsNullLiteral(self: @This(), expression: db_mod.types.RelationalRowsExpression) !bool {
        if (expression.kind != .value) return false;
        var parsed = std.json.parseFromSlice(std.json.Value, self.alloc, expression.value_json, .{}) catch return error.UnsupportedSqlShape;
        defer parsed.deinit();
        return parsed.value == .null;
    }

    pub fn validateNumericRowExpression(self: @This(), expression: db_mod.types.RelationalRowsExpression) anyerror!void {
        switch (expression.kind) {
            .field => {
                if (self.defer_row_expression_field_validation) return;
                const column = binder.relationalColumnForField(self.schemaForRowExpressionField(expression), expression.field, null) orelse return error.InvalidSqlCatalog;
                if (column.field_type != .numeric) return error.InvalidSqlCatalog;
            },
            .value => {
                var parsed = std.json.parseFromSlice(std.json.Value, self.alloc, expression.value_json, .{}) catch return error.UnsupportedSqlShape;
                defer parsed.deinit();
                switch (parsed.value) {
                    .null, .integer, .float, .number_string => {},
                    else => return error.UnsupportedSqlShape,
                }
            },
            .now => {},
            .date_trunc => {
                if (expression.operands.len != 2) return error.UnsupportedSqlShape;
                try self.validateTextRowExpression(expression.operands[0]);
                try self.validateNumericOrDatetimeRowExpression(expression.operands[1]);
            },
            .coalesce => {
                if (expression.operands.len == 0) return error.UnsupportedSqlShape;
                for (expression.operands) |operand| try self.validateNumericRowExpression(operand);
            },
            .nullif => {
                if (expression.operands.len != 2) return error.UnsupportedSqlShape;
                for (expression.operands) |operand| try self.validateNumericRowExpression(operand);
            },
            .length, .octet_length, .bit_length, .ascii => {
                if (expression.operands.len != 1) return error.UnsupportedSqlShape;
                try self.validateTextRowExpression(expression.operands[0]);
            },
            .strpos => {
                if (expression.operands.len != 2) return error.UnsupportedSqlShape;
                try self.validateTextRowExpression(expression.operands[0]);
                try self.validateTextRowExpression(expression.operands[1]);
            },
            .left, .right => {
                if (expression.operands.len != 2) return error.UnsupportedSqlShape;
                try self.validateTextRowExpression(expression.operands[0]);
                try self.validateNumericRowExpression(expression.operands[1]);
            },
            .greatest, .least => {
                if (expression.operands.len == 0) return error.UnsupportedSqlShape;
                for (expression.operands) |operand| try self.validateNumericRowExpression(operand);
            },
            .abs, .round, .trunc, .floor, .ceil, .sqrt, .sign => {
                if (expression.operands.len != 1) return error.UnsupportedSqlShape;
                try self.validateNumericRowExpression(expression.operands[0]);
            },
            .add => {
                if (expression.operands.len < 2) return error.UnsupportedSqlShape;
                if (sqlExpressionContainsInterval(expression)) {
                    if (expression.operands.len != 2) return error.UnsupportedSqlShape;
                    const lhs_interval = sqlExpressionIsInterval(expression.operands[0]);
                    const rhs_interval = sqlExpressionIsInterval(expression.operands[1]);
                    if (lhs_interval == rhs_interval) return error.UnsupportedSqlShape;
                    if (lhs_interval) {
                        try self.validateIntervalRowExpression(expression.operands[0]);
                        try self.validateNumericOrDatetimeRowExpression(expression.operands[1]);
                    } else {
                        try self.validateNumericOrDatetimeRowExpression(expression.operands[0]);
                        try self.validateIntervalRowExpression(expression.operands[1]);
                    }
                    return;
                }
                for (expression.operands) |operand| try self.validateNumericRowExpression(operand);
            },
            .mul => {
                if (expression.operands.len < 2) return error.UnsupportedSqlShape;
                if (sqlExpressionContainsInterval(expression)) return error.UnsupportedSqlShape;
                for (expression.operands) |operand| try self.validateNumericRowExpression(operand);
            },
            .sub => {
                if (expression.operands.len != 2) return error.UnsupportedSqlShape;
                if (sqlExpressionContainsInterval(expression)) {
                    if (sqlExpressionIsInterval(expression.operands[0]) or !sqlExpressionIsInterval(expression.operands[1])) return error.UnsupportedSqlShape;
                    try self.validateNumericOrDatetimeRowExpression(expression.operands[0]);
                    try self.validateIntervalRowExpression(expression.operands[1]);
                    return;
                }
                for (expression.operands) |operand| try self.validateNumericRowExpression(operand);
            },
            .div, .mod, .power => {
                if (expression.operands.len != 2) return error.UnsupportedSqlShape;
                if (sqlExpressionContainsInterval(expression)) return error.UnsupportedSqlShape;
                for (expression.operands) |operand| try self.validateNumericRowExpression(operand);
            },
            .case => {
                if (expression.case_branches.len == 0 or expression.case_else.len != 1) return error.UnsupportedSqlShape;
                for (expression.case_branches) |branch| try self.validateNumericRowExpression(branch.then);
                try self.validateNumericRowExpression(expression.case_else[0]);
            },
            .cast => {
                if (expression.operands.len != 1 or expression.cast_type != .numeric) return error.UnsupportedSqlShape;
            },
            .array_length => {
                if (expression.operands.len != 1) return error.UnsupportedSqlShape;
            },
            .array_position, .array_positions => try self.validateArrayPositionExpression(expression),
            .json_array_length => {
                if (expression.operands.len != 1) return error.UnsupportedSqlShape;
                try self.validateJsonRowExpression(expression.operands[0]);
            },
            .regexp_count, .regexp_instr => {
                if (expression.operands.len != 2) return error.UnsupportedSqlShape;
                try self.validateTextRowExpression(expression.operands[0]);
                try self.validateTextRowExpression(expression.operands[1]);
            },
            .interval_ns, .interval_months => try self.validateIntervalRowExpression(expression),
            .date_part => {
                if (expression.operands.len != 2) return error.UnsupportedSqlShape;
                try self.validateTextRowExpression(expression.operands[0]);
                try self.validateNumericOrDatetimeRowExpression(expression.operands[1]);
            },
            else => return error.UnsupportedSqlShape,
        }
    }

    pub fn validateBooleanRowExpression(self: @This(), expression: db_mod.types.RelationalRowsExpression) anyerror!void {
        switch (expression.kind) {
            .field => {
                if (self.defer_row_expression_field_validation) return;
                const column = binder.relationalColumnForField(self.schemaForRowExpressionField(expression), expression.field, null) orelse return error.InvalidSqlCatalog;
                if (column.field_type != .boolean) return error.InvalidSqlCatalog;
            },
            .value => {
                var parsed = std.json.parseFromSlice(std.json.Value, self.alloc, expression.value_json, .{}) catch return error.UnsupportedSqlShape;
                defer parsed.deinit();
                switch (parsed.value) {
                    .null, .bool => {},
                    else => return error.UnsupportedSqlShape,
                }
            },
            .starts_with => {
                if (expression.operands.len != 2) return error.UnsupportedSqlShape;
                try self.validateTextRowExpression(expression.operands[0]);
                try self.validateTextRowExpression(expression.operands[1]);
            },
            .ends_with => {
                if (expression.operands.len != 2) return error.UnsupportedSqlShape;
                try self.validateTextRowExpression(expression.operands[0]);
                try self.validateTextRowExpression(expression.operands[1]);
            },
            .like, .ilike => {
                if (expression.operands.len != 2) return error.UnsupportedSqlShape;
                try self.validateTextRowExpression(expression.operands[0]);
                try self.validateTextRowExpression(expression.operands[1]);
            },
            .regexp_match => {
                if (expression.operands.len != 2 and expression.operands.len != 3) return error.UnsupportedSqlShape;
                try self.validateTextRowExpression(expression.operands[0]);
                try self.validateTextRowExpression(expression.operands[1]);
                if (expression.operands.len == 3) try self.validateBooleanRowExpression(expression.operands[2]);
            },
            .json_path_exists => {
                if (expression.operands.len != 1 or expression.json_path.len == 0) return error.UnsupportedSqlShape;
                try self.validateJsonRowExpression(expression.operands[0]);
            },
            .bool_and, .bool_or => {
                if (expression.operands.len < 2) return error.UnsupportedSqlShape;
                for (expression.operands) |operand| try self.validateBooleanRowExpression(operand);
            },
            .bool_not => {
                if (expression.operands.len != 1) return error.UnsupportedSqlShape;
                try self.validateBooleanRowExpression(expression.operands[0]);
            },
            .coalesce => {
                if (expression.operands.len == 0) return error.UnsupportedSqlShape;
                for (expression.operands) |operand| try self.validateBooleanRowExpression(operand);
            },
            .nullif => {
                if (expression.operands.len != 2) return error.UnsupportedSqlShape;
                for (expression.operands) |operand| try self.validateBooleanRowExpression(operand);
            },
            .case => {
                if (expression.case_branches.len == 0 or expression.case_else.len != 1) return error.UnsupportedSqlShape;
                for (expression.case_branches) |branch| try self.validateBooleanRowExpression(branch.then);
                try self.validateBooleanRowExpression(expression.case_else[0]);
            },
            .cast => {
                if (expression.operands.len != 1 or expression.cast_type != .bool) return error.UnsupportedSqlShape;
            },
            else => return error.UnsupportedSqlShape,
        }
    }

    pub fn schemaForRowExpressionField(
        self: @This(),
        expression: db_mod.types.RelationalRowsExpression,
    ) runtime_schema.TableSchema {
        if (expression.field_source == .source) return self.joined_source_schema orelse self.schema;
        return self.schema;
    }

    pub fn validateIntervalRowExpression(self: @This(), expression: db_mod.types.RelationalRowsExpression) anyerror!void {
        if (!sqlExpressionIsInterval(expression) or expression.operands.len != 1) return error.UnsupportedSqlShape;
        if (sqlExpressionContainsInterval(expression.operands[0])) return error.UnsupportedSqlShape;
        try self.validateNumericRowExpression(expression.operands[0]);
    }

    pub fn validateDateBinStrideRowExpression(self: @This(), expression: db_mod.types.RelationalRowsExpression) anyerror!void {
        switch (expression.kind) {
            .interval_ns => try self.validateIntervalRowExpression(expression),
            .interval_months => return error.UnsupportedSqlShape,
            else => {
                if (sqlExpressionContainsInterval(expression)) return error.UnsupportedSqlShape;
                try self.validateNumericRowExpression(expression);
            },
        }
    }

    pub fn validateNumericOrDatetimeRowExpression(self: @This(), expression: db_mod.types.RelationalRowsExpression) anyerror!void {
        switch (expression.kind) {
            .field => {
                if (self.defer_row_expression_field_validation) return;
                const column = binder.relationalColumnForField(self.schemaForRowExpressionField(expression), expression.field, null) orelse return error.InvalidSqlCatalog;
                if (column.field_type != .numeric and column.field_type != .datetime) return error.InvalidSqlCatalog;
            },
            .now => {},
            .date_trunc => {
                if (expression.operands.len != 2) return error.UnsupportedSqlShape;
                try self.validateTextRowExpression(expression.operands[0]);
                try self.validateNumericOrDatetimeRowExpression(expression.operands[1]);
            },
            .date_bin => {
                if (expression.operands.len != 3) return error.UnsupportedSqlShape;
                try self.validateDateBinStrideRowExpression(expression.operands[0]);
                try self.validateNumericOrDatetimeRowExpression(expression.operands[1]);
                try self.validateNumericOrDatetimeRowExpression(expression.operands[2]);
            },
            .date_part => {
                if (expression.operands.len != 2) return error.UnsupportedSqlShape;
                try self.validateTextRowExpression(expression.operands[0]);
                try self.validateNumericOrDatetimeRowExpression(expression.operands[1]);
            },
            .cast => {
                if (expression.operands.len != 1 or expression.cast_type != .datetime) return error.UnsupportedSqlShape;
            },
            else => try self.validateNumericRowExpression(expression),
        }
    }

    pub fn validateOrderableRowExpression(self: @This(), expression: db_mod.types.RelationalRowsExpression) anyerror!void {
        const output_type = try self.rowExpressionOutputType(expression);
        if (!sqlExpressionTypeIsOrderKey(output_type)) return error.UnsupportedSqlShape;
    }

    pub fn rowExpressionOutputType(self: @This(), expression: db_mod.types.RelationalRowsExpression) !runtime_schema.AntflyType {
        switch (expression.kind) {
            .field => {
                const column = binder.relationalColumnForField(self.schemaForRowExpressionField(expression), expression.field, null) orelse {
                    if (self.defer_row_expression_field_validation) return .json;
                    return error.InvalidSqlCatalog;
                };
                return column.field_type;
            },
            .value => {
                var parsed = std.json.parseFromSlice(std.json.Value, self.alloc, expression.value_json, .{}) catch return error.UnsupportedSqlShape;
                defer parsed.deinit();
                return switch (parsed.value) {
                    .integer, .float, .number_string => .numeric,
                    .string => .keyword,
                    .bool => .boolean,
                    .array => .array,
                    .object, .null => .json,
                };
            },
            .now, .date_trunc => return .datetime,
            .date_bin => return .datetime,
            .uuid_v4, .lower, .upper, .initcap, .trim, .ltrim, .rtrim, .replace, .regexp_replace, .regexp_substr, .translate, .substring, .overlay, .split_part, .left, .right, .lpad, .rpad, .repeat, .reverse, .chr, .md5, .concat, .concat_ws, .json_typeof, .array_to_string => return .keyword,
            .starts_with, .ends_with, .like, .ilike, .regexp_match, .bool_and, .bool_or, .bool_not, .json_path_exists => return .boolean,
            .nullif => {
                if (expression.operands.len != 2) return error.UnsupportedSqlShape;
                return try self.rowExpressionOutputType(expression.operands[0]);
            },
            .length, .octet_length, .bit_length, .strpos, .ascii, .regexp_count, .regexp_instr, .abs, .round, .trunc, .floor, .ceil, .sqrt, .sign, .power, .mul, .div, .mod, .json_array_length, .array_length, .array_position, .interval_ns, .interval_months, .date_part => return .numeric,
            .add, .sub => {
                if (expression.operands.len > 0 and sqlExpressionContainsInterval(expression)) {
                    return try self.rowExpressionOutputType(expression.operands[0]);
                }
                return .numeric;
            },
            .coalesce, .greatest, .least => {
                if (expression.operands.len == 0) return error.UnsupportedSqlShape;
                return try self.expressionOperandOutputType(expression.operands);
            },
            .case => {
                return try self.caseExpressionOutputType(expression.case_branches, expression.case_else);
            },
            .cast => return switch (expression.cast_type orelse return error.UnsupportedSqlShape) {
                .text => .keyword,
                .numeric => .numeric,
                .bool => .boolean,
                .datetime => .datetime,
            },
            .json_extract => return if (expression.json_as_text) .keyword else .json,
            .json_build_object, .to_jsonb => return .json,
            .array_positions, .array_append, .array_prepend, .array_cat, .array_remove, .array_replace, .string_to_array => return .array,
        }
    }

    pub fn rowExpressionOutputArrayItemType(self: @This(), expression: db_mod.types.RelationalRowsExpression) anyerror!?runtime_schema.AntflyType {
        const output_type = try self.rowExpressionOutputType(expression);
        if (output_type != .array) return null;
        switch (expression.kind) {
            .field => {
                const column = binder.relationalColumnForField(self.schemaForRowExpressionField(expression), expression.field, .array) orelse {
                    if (self.defer_row_expression_field_validation) return .json;
                    return error.InvalidSqlCatalog;
                };
                return column.array_item_type orelse .json;
            },
            .array_positions => return .numeric,
            .array_append, .array_prepend, .array_remove, .array_replace => {
                if (expression.operands.len > 0 and (try self.rowExpressionOutputType(expression.operands[0])) == .array) {
                    return (try self.rowExpressionOutputArrayItemType(expression.operands[0])) orelse .json;
                }
                return .json;
            },
            .array_cat => {
                for (expression.operands) |operand| {
                    if ((try self.rowExpressionOutputType(operand)) == .array) {
                        return (try self.rowExpressionOutputArrayItemType(operand)) orelse .json;
                    }
                }
                return .json;
            },
            .string_to_array => return .keyword,
            .value => return .json,
            .coalesce, .greatest, .least => {
                for (expression.operands) |operand| {
                    if (try self.rowExpressionIsNullLiteral(operand)) continue;
                    if ((try self.rowExpressionOutputType(operand)) == .array) {
                        return (try self.rowExpressionOutputArrayItemType(operand)) orelse .json;
                    }
                }
                return .json;
            },
            .case => {
                for (expression.case_branches) |branch| {
                    if ((try self.rowExpressionOutputType(branch.then)) == .array) {
                        return (try self.rowExpressionOutputArrayItemType(branch.then)) orelse .json;
                    }
                }
                if (expression.case_else.len == 1 and (try self.rowExpressionOutputType(expression.case_else[0])) == .array) {
                    return (try self.rowExpressionOutputArrayItemType(expression.case_else[0])) orelse .json;
                }
                return .json;
            },
            else => return .json,
        }
    }

    pub fn expressionOperandOutputType(
        self: @This(),
        operands: []const db_mod.types.RelationalRowsExpression,
    ) anyerror!runtime_schema.AntflyType {
        if (operands.len == 0) return error.UnsupportedSqlShape;
        for (operands) |operand| {
            if (try self.rowExpressionIsNullLiteral(operand)) continue;
            return try self.rowExpressionOutputType(operand);
        }
        return .json;
    }

    pub fn caseExpressionOutputType(
        self: @This(),
        branches: []const db_mod.types.RelationalRowsExpressionCaseBranch,
        fallback: []const db_mod.types.RelationalRowsExpression,
    ) anyerror!runtime_schema.AntflyType {
        if (branches.len == 0 or fallback.len != 1) return error.UnsupportedSqlShape;
        var result_type: ?runtime_schema.AntflyType = null;
        for (branches) |branch| {
            try self.mergeCaseExpressionArmType(branch.then, &result_type);
        }
        try self.mergeCaseExpressionArmType(fallback[0], &result_type);
        return result_type orelse .json;
    }

    pub fn mergeCaseExpressionArmType(
        self: @This(),
        expression: db_mod.types.RelationalRowsExpression,
        result_type: *?runtime_schema.AntflyType,
    ) anyerror!void {
        if (try self.rowExpressionIsNullLiteral(expression)) return;
        const arm_type = try self.rowExpressionOutputType(expression);
        if (result_type.*) |existing| {
            if (!sqlExpressionResultTypesCompatible(existing, arm_type)) return error.UnsupportedSqlShape;
        } else {
            result_type.* = arm_type;
        }
    }

    pub fn validateExpressionOperandDomains(
        self: @This(),
        expression: db_mod.types.RelationalRowsExpression,
    ) anyerror!void {
        switch (expression.kind) {
            .coalesce => {
                if (expression.operands.len == 0) return error.UnsupportedSqlShape;
                try self.validateExpressionSameDomainOperands(expression.operands, false);
            },
            .nullif => {
                if (expression.operands.len != 2) return error.UnsupportedSqlShape;
                try self.validateExpressionSameDomainOperands(expression.operands, false);
            },
            .greatest, .least => {
                if (expression.operands.len == 0) return error.UnsupportedSqlShape;
                try self.validateExpressionSameDomainOperands(expression.operands, true);
            },
            else => return error.UnsupportedSqlShape,
        }
    }

    pub fn validateExpressionSameDomainOperands(
        self: @This(),
        operands: []const db_mod.types.RelationalRowsExpression,
        require_orderable: bool,
    ) anyerror!void {
        var result_type: ?runtime_schema.AntflyType = null;
        for (operands) |operand| {
            if (try self.rowExpressionIsNullLiteral(operand)) continue;
            const operand_type = try self.rowExpressionOutputType(operand);
            if (result_type) |existing| {
                if (!sqlExpressionResultTypesCompatible(existing, operand_type)) return error.UnsupportedSqlShape;
            } else {
                result_type = operand_type;
            }
        }
        if (require_orderable) {
            const operand_type = result_type orelse return;
            if (!sqlExpressionTypeIsOrderable(operand_type)) return error.UnsupportedSqlShape;
        }
    }

    pub fn validateTextRowExpression(self: @This(), expression: db_mod.types.RelationalRowsExpression) !void {
        switch (expression.kind) {
            .field => {
                if (self.defer_row_expression_field_validation) return;
                const column = binder.relationalColumnForField(self.schemaForRowExpressionField(expression), expression.field, null) orelse return error.InvalidSqlCatalog;
                if (column.field_type != .keyword and column.field_type != .text and column.field_type != .link) return error.InvalidSqlCatalog;
            },
            .value => {
                var parsed = std.json.parseFromSlice(std.json.Value, self.alloc, expression.value_json, .{}) catch return error.UnsupportedSqlShape;
                defer parsed.deinit();
                switch (parsed.value) {
                    .null, .string => {},
                    else => return error.UnsupportedSqlShape,
                }
            },
            .uuid_v4 => {
                if (expression.operands.len != 0) return error.UnsupportedSqlShape;
            },
            .coalesce => {
                if (expression.operands.len == 0) return error.UnsupportedSqlShape;
                for (expression.operands) |operand| try self.validateTextRowExpression(operand);
            },
            .lower, .upper, .initcap, .md5 => {
                if (expression.operands.len != 1) return error.UnsupportedSqlShape;
                try self.validateTextRowExpression(expression.operands[0]);
            },
            .chr => {
                if (expression.operands.len != 1) return error.UnsupportedSqlShape;
                try self.validateNumericRowExpression(expression.operands[0]);
            },
            .trim, .ltrim, .rtrim => {
                if (expression.operands.len != 1 and expression.operands.len != 2) return error.UnsupportedSqlShape;
                for (expression.operands) |operand| try self.validateTextRowExpression(operand);
            },
            .replace, .translate => {
                if (expression.operands.len != 3) return error.UnsupportedSqlShape;
                for (expression.operands) |operand| try self.validateTextRowExpression(operand);
            },
            .regexp_replace => {
                if (expression.operands.len != 3 and expression.operands.len != 4) return error.UnsupportedSqlShape;
                for (expression.operands) |operand| try self.validateTextRowExpression(operand);
            },
            .regexp_substr => {
                if (expression.operands.len != 2) return error.UnsupportedSqlShape;
                try self.validateTextRowExpression(expression.operands[0]);
                try self.validateTextRowExpression(expression.operands[1]);
            },
            .substring => {
                if (expression.operands.len != 2 and expression.operands.len != 3) return error.UnsupportedSqlShape;
                try self.validateTextRowExpression(expression.operands[0]);
                try self.validateNumericRowExpression(expression.operands[1]);
                if (expression.operands.len == 3) try self.validateNumericRowExpression(expression.operands[2]);
            },
            .overlay => {
                if (expression.operands.len != 3 and expression.operands.len != 4) return error.UnsupportedSqlShape;
                try self.validateTextRowExpression(expression.operands[0]);
                try self.validateTextRowExpression(expression.operands[1]);
                try self.validateNumericRowExpression(expression.operands[2]);
                if (expression.operands.len == 4) try self.validateNumericRowExpression(expression.operands[3]);
            },
            .split_part => {
                if (expression.operands.len != 3) return error.UnsupportedSqlShape;
                try self.validateTextRowExpression(expression.operands[0]);
                try self.validateTextRowExpression(expression.operands[1]);
                try self.validateNumericRowExpression(expression.operands[2]);
            },
            .left, .right => {
                if (expression.operands.len != 2) return error.UnsupportedSqlShape;
                try self.validateTextRowExpression(expression.operands[0]);
                try self.validateNumericRowExpression(expression.operands[1]);
            },
            .lpad, .rpad => {
                if (expression.operands.len != 2 and expression.operands.len != 3) return error.UnsupportedSqlShape;
                try self.validateTextRowExpression(expression.operands[0]);
                try self.validateNumericRowExpression(expression.operands[1]);
                if (expression.operands.len == 3) try self.validateTextRowExpression(expression.operands[2]);
            },
            .repeat => {
                if (expression.operands.len != 2) return error.UnsupportedSqlShape;
                try self.validateTextRowExpression(expression.operands[0]);
                try self.validateNumericRowExpression(expression.operands[1]);
            },
            .reverse => {
                if (expression.operands.len != 1) return error.UnsupportedSqlShape;
                try self.validateTextRowExpression(expression.operands[0]);
            },
            .starts_with, .ends_with => return error.UnsupportedSqlShape,
            .concat => {
                if (expression.operands.len == 0) return error.UnsupportedSqlShape;
            },
            .concat_ws => {
                if (expression.operands.len < 2) return error.UnsupportedSqlShape;
                for (expression.operands) |operand| try self.validateTextRowExpression(operand);
            },
            .nullif => {
                if (expression.operands.len != 2) return error.UnsupportedSqlShape;
                for (expression.operands) |operand| try self.validateTextRowExpression(operand);
            },
            .case => {
                if (expression.case_branches.len == 0 or expression.case_else.len != 1) return error.UnsupportedSqlShape;
                for (expression.case_branches) |branch| try self.validateTextRowExpression(branch.then);
                try self.validateTextRowExpression(expression.case_else[0]);
            },
            .cast => {
                if (expression.operands.len != 1 or expression.cast_type != .text) return error.UnsupportedSqlShape;
            },
            .json_extract => {
                if (expression.operands.len != 1 or !expression.json_as_text) return error.UnsupportedSqlShape;
            },
            .json_typeof => {
                if (expression.operands.len != 1) return error.UnsupportedSqlShape;
                try self.validateJsonRowExpression(expression.operands[0]);
            },
            .array_append, .array_prepend, .array_remove => try self.validateArrayElementTransformExpression(expression),
            .array_replace => try self.validateArrayReplaceExpression(expression),
            .array_cat => try self.validateArrayCatExpression(expression),
            .array_to_string => try self.validateArrayToStringExpression(expression),
            else => return error.UnsupportedSqlShape,
        }
    }

    pub fn validateJsonRowExpression(self: @This(), expression: db_mod.types.RelationalRowsExpression) anyerror!void {
        const output_type = try self.rowExpressionOutputType(expression);
        if (output_type != .json) return error.UnsupportedSqlShape;
    }

    pub fn validateJsonBuildObjectExpression(self: @This(), expression: db_mod.types.RelationalRowsExpression) anyerror!void {
        if (expression.kind != .json_build_object or expression.operands.len % 2 != 0) return error.UnsupportedSqlShape;
        var index: usize = 0;
        while (index < expression.operands.len) : (index += 2) {
            try self.validateTextRowExpression(expression.operands[index]);
            _ = try self.rowExpressionOutputType(expression.operands[index + 1]);
        }
    }

    pub fn validateStringToArrayExpression(self: @This(), expression: db_mod.types.RelationalRowsExpression) anyerror!void {
        if (expression.kind != .string_to_array or expression.operands.len != 2) return error.UnsupportedSqlShape;
        try self.validateTextRowExpression(expression.operands[0]);
        try self.validateTextRowExpression(expression.operands[1]);
        try self.validateStringToArrayDelimiterLiteral(expression.operands[1]);
    }

    pub fn validateArrayPositionExpression(self: @This(), expression: db_mod.types.RelationalRowsExpression) anyerror!void {
        if ((expression.kind != .array_position and expression.kind != .array_positions) or expression.operands.len != 2) return error.UnsupportedSqlShape;
        const array_type = try self.rowExpressionOutputType(expression.operands[0]);
        if (array_type != .array) return error.UnsupportedSqlShape;
        const item_type = (try self.rowExpressionOutputArrayItemType(expression.operands[0])) orelse return error.UnsupportedSqlShape;
        if (try self.rowExpressionIsNullLiteral(expression.operands[1])) return;
        const needle_type = try self.rowExpressionOutputType(expression.operands[1]);
        if (!sqlExpressionTypesComparable(item_type, needle_type)) return error.UnsupportedSqlShape;
    }

    pub fn validateArrayElementTransformExpression(self: @This(), expression: db_mod.types.RelationalRowsExpression) anyerror!void {
        if ((expression.kind != .array_append and expression.kind != .array_prepend and expression.kind != .array_remove) or expression.operands.len != 2) return error.UnsupportedSqlShape;
        const array_type = try self.rowExpressionOutputType(expression.operands[0]);
        if (array_type != .array) return error.UnsupportedSqlShape;
        const item_type = (try self.rowExpressionOutputArrayItemType(expression.operands[0])) orelse return error.UnsupportedSqlShape;
        if (try self.rowExpressionIsNullLiteral(expression.operands[1])) return;
        const element_type = try self.rowExpressionOutputType(expression.operands[1]);
        if (!sqlExpressionTypesComparable(item_type, element_type)) return error.UnsupportedSqlShape;
    }

    pub fn validateArrayReplaceExpression(self: @This(), expression: db_mod.types.RelationalRowsExpression) anyerror!void {
        if (expression.kind != .array_replace or expression.operands.len != 3) return error.UnsupportedSqlShape;
        const array_type = try self.rowExpressionOutputType(expression.operands[0]);
        if (array_type != .array) return error.UnsupportedSqlShape;
        const item_type = (try self.rowExpressionOutputArrayItemType(expression.operands[0])) orelse return error.UnsupportedSqlShape;
        if (!try self.rowExpressionIsNullLiteral(expression.operands[1])) {
            const old_type = try self.rowExpressionOutputType(expression.operands[1]);
            if (!sqlExpressionTypesComparable(item_type, old_type)) return error.UnsupportedSqlShape;
        }
        if (!try self.rowExpressionIsNullLiteral(expression.operands[2])) {
            const new_type = try self.rowExpressionOutputType(expression.operands[2]);
            if (!sqlExpressionTypesComparable(item_type, new_type)) return error.UnsupportedSqlShape;
        }
    }

    pub fn validateArrayCatExpression(self: @This(), expression: db_mod.types.RelationalRowsExpression) anyerror!void {
        if (expression.kind != .array_cat or expression.operands.len != 2) return error.UnsupportedSqlShape;
        const left_type = try self.rowExpressionOutputType(expression.operands[0]);
        const right_type = try self.rowExpressionOutputType(expression.operands[1]);
        if (left_type != .array or right_type != .array) return error.UnsupportedSqlShape;
        const left_item_type = (try self.rowExpressionOutputArrayItemType(expression.operands[0])) orelse return error.UnsupportedSqlShape;
        const right_item_type = (try self.rowExpressionOutputArrayItemType(expression.operands[1])) orelse return error.UnsupportedSqlShape;
        if (!sqlExpressionTypesComparable(left_item_type, right_item_type)) return error.UnsupportedSqlShape;
    }

    pub fn validateArrayToStringExpression(self: @This(), expression: db_mod.types.RelationalRowsExpression) anyerror!void {
        if (expression.kind != .array_to_string or (expression.operands.len != 2 and expression.operands.len != 3)) return error.UnsupportedSqlShape;
        const array_type = try self.rowExpressionOutputType(expression.operands[0]);
        if (array_type != .array) return error.UnsupportedSqlShape;
        try self.validateTextRowExpression(expression.operands[1]);
        if (expression.operands.len == 3) try self.validateTextRowExpression(expression.operands[2]);
    }

    pub fn validateStringToArrayDelimiterLiteral(self: @This(), expression: db_mod.types.RelationalRowsExpression) anyerror!void {
        if (expression.kind != .value) return;
        var parsed = std.json.parseFromSlice(std.json.Value, self.alloc, expression.value_json, .{}) catch return error.UnsupportedSqlShape;
        defer parsed.deinit();
        switch (parsed.value) {
            .null => {},
            .string => |delimiter| if (delimiter.len == 0) return error.UnsupportedSqlShape,
            else => return error.UnsupportedSqlShape,
        }
    }
};

pub fn validateGeneratedColumnExpressionForColumns(
    columns: []const runtime_schema.RelationalColumn,
    generated_column_name: []const u8,
    expression: runtime_schema.RelationalRowsExpression,
) error{InvalidSqlCatalog}!void {
    if (expression.kind == .field) {
        if (std.mem.eql(u8, expression.field, generated_column_name)) return error.InvalidSqlCatalog;
        _ = binder.relationalColumnForDdl(columns, expression.field) orelse return error.InvalidSqlCatalog;
    }
    for (expression.operands) |operand| try validateGeneratedColumnExpressionForColumns(columns, generated_column_name, operand);
    for (expression.case_branches) |branch| {
        try validateGeneratedColumnExpressionConditionForColumns(columns, generated_column_name, branch.when);
        try validateGeneratedColumnExpressionForColumns(columns, generated_column_name, branch.then);
    }
    for (expression.case_else) |case_else| try validateGeneratedColumnExpressionForColumns(columns, generated_column_name, case_else);
}

fn validateGeneratedColumnExpressionConditionForColumns(
    columns: []const runtime_schema.RelationalColumn,
    generated_column_name: []const u8,
    condition: runtime_schema.RelationalRowsExpressionCondition,
) error{InvalidSqlCatalog}!void {
    try validateGeneratedColumnExpressionForColumns(columns, generated_column_name, condition.lhs);
    for (condition.rhs) |rhs| try validateGeneratedColumnExpressionForColumns(columns, generated_column_name, rhs);
}

pub fn validateUniquePredicateExpressionsForColumns(
    columns: []const runtime_schema.RelationalColumn,
    conditions: []const runtime_schema.RelationalRowsExpressionCondition,
) !void {
    for (conditions) |condition| {
        try validateCheckExpressionConditionForColumns(columns, condition);
        if (!rowExpressionDeterministic(condition.lhs)) return error.InvalidSqlCatalog;
        for (condition.rhs) |rhs| {
            if (!rowExpressionDeterministic(rhs)) return error.InvalidSqlCatalog;
        }
    }
}

pub fn validateCheckForColumns(columns: []const runtime_schema.RelationalColumn, check: runtime_schema.RelationalCheck) !void {
    if (check.expression) |condition| {
        if (check.field.len != 0 or check.value_json != null) return error.InvalidSqlCatalog;
        return validateCheckExpressionConditionForColumns(columns, condition);
    }
    const column = binder.relationalColumnForDdl(columns, check.field) orelse return error.InvalidSqlCatalog;
    switch (check.op) {
        .is_null, .is_not_null => if (check.value_json != null) return error.InvalidSqlCatalog,
        .eq, .ne, .is_distinct, .is_not_distinct, .gt, .gte, .lt, .lte => {
            const value_json = check.value_json orelse return error.InvalidSqlCatalog;
            const field_type: CheckExpressionType = .{ .type = column.field_type };
            const value_type = try checkExpressionLiteralType(value_json);
            if (!checkExpressionTypesComparable(field_type, value_type)) return error.InvalidSqlCatalog;
            switch (check.op) {
                .gt, .gte, .lt, .lte => if (!checkExpressionTypeOrderable(field_type) or !checkExpressionTypeOrderable(value_type)) return error.InvalidSqlCatalog,
                else => {},
            }
        },
    }
}

pub fn validateGeneratedColumnForColumns(columns: []const runtime_schema.RelationalColumn, column: runtime_schema.RelationalColumn) !void {
    const generated = column.generated orelse return;
    switch (generated.op) {
        .lower, .upper, .md5 => {
            const field = generated.field orelse return error.InvalidSqlCatalog;
            if (std.mem.eql(u8, field, column.name)) return error.InvalidSqlCatalog;
            const source = binder.relationalColumnForDdl(columns, field) orelse return error.InvalidSqlCatalog;
            if (source.field_type == .json or source.field_type == .array) return error.InvalidSqlCatalog;
        },
        .concat => {
            if (generated.fields.len == 0) return error.InvalidSqlCatalog;
            for (generated.fields) |field| {
                if (std.mem.eql(u8, field, column.name)) return error.InvalidSqlCatalog;
                const source = binder.relationalColumnForDdl(columns, field) orelse return error.InvalidSqlCatalog;
                if (source.field_type == .json or source.field_type == .array) return error.InvalidSqlCatalog;
            }
        },
        .concat_ws => {
            if (generated.fields.len == 0) return error.InvalidSqlCatalog;
            for (generated.fields) |field| {
                if (std.mem.eql(u8, field, column.name)) return error.InvalidSqlCatalog;
                const source = binder.relationalColumnForDdl(columns, field) orelse return error.InvalidSqlCatalog;
                if (!checkExpressionTypeTextLike(.{ .type = source.field_type })) return error.InvalidSqlCatalog;
            }
        },
        .expression => {
            const expression = generated.expression orelse return error.InvalidSqlCatalog;
            try validateGeneratedColumnExpressionForColumns(columns, column.name, expression);
        },
    }
}

pub fn validateCreateIndexIncludeColumns(
    columns: []const runtime_schema.RelationalColumn,
    key_columns: []const []const u8,
    include_columns: []const []const u8,
) !void {
    for (include_columns) |column| {
        if (stringSlicesContains(key_columns, column)) return error.InvalidSqlCatalog;
        const found = binder.relationalColumnForDdl(columns, column) orelse return error.InvalidSqlCatalog;
        if (found.field_type == .json or found.field_type == .array) return error.InvalidSqlCatalog;
    }
}

pub fn validateUniquePredicatesForColumns(columns: []const runtime_schema.RelationalColumn, predicates: []const runtime_schema.UniquePredicate) !void {
    for (predicates) |predicate| {
        const found = binder.relationalColumnForDdl(columns, predicate.field) orelse return error.InvalidSqlCatalog;
        if (found.field_type == .json or found.field_type == .array) return error.InvalidSqlCatalog;
    }
}

pub fn validateUniqueConstraintForColumns(columns: []const runtime_schema.RelationalColumn, periods: []const runtime_schema.RelationalPeriod, constraint: runtime_schema.UniqueConstraint) !void {
    if (constraint.columns.len == 0 and constraint.expressions.len == 0) return error.InvalidSqlCatalog;
    if (constraint.without_overlaps_period) |period| {
        _ = binder.relationalPeriodForDdl(periods, period) orelse return error.InvalidSqlCatalog;
    }
    for (constraint.columns) |column| {
        const found = binder.relationalColumnForDdl(columns, column) orelse return error.InvalidSqlCatalog;
        if (found.field_type == .json or found.field_type == .array) return error.InvalidSqlCatalog;
    }
    for (constraint.expressions) |expression| {
        switch (expression.op) {
            .lower, .upper, .md5 => {
                const found = binder.relationalColumnForDdl(columns, expression.field) orelse return error.InvalidSqlCatalog;
                if (found.field_type == .json or found.field_type == .array) return error.InvalidSqlCatalog;
            },
            .expression => {
                const row_expression = expression.expression orelse return error.InvalidSqlCatalog;
                try validateCheckExpressionForColumns(columns, row_expression);
                if (!rowExpressionDeterministic(row_expression)) return error.InvalidSqlCatalog;
                if (!checkExpressionTypeOrderable(try checkExpressionTypeForColumns(columns, row_expression))) return error.InvalidSqlCatalog;
            },
        }
    }
    try validateCreateIndexIncludeColumns(columns, constraint.columns, constraint.include_columns);
    try validateUniquePredicatesForColumns(columns, constraint.where);
    try validateUniquePredicateExpressionsForColumns(columns, constraint.where_expressions);
}

pub fn validateRelationalColumnCatalog(columns: []const runtime_schema.RelationalColumn) !void {
    for (columns, 0..) |column, i| {
        if (binder.relationalColumnIndex(columns[0..i], column.name) != null) return error.InvalidSqlCatalog;
        if (!std.mem.eql(u8, column.name, column.path)) return error.InvalidSqlCatalog;
        if (column.collation != null and !binder.relationalFieldTypeSupportsCollation(column.field_type)) return error.InvalidSqlCatalog;
        if (column.generated) |_| try validateGeneratedColumnForColumns(columns, column);
        try validateUniquePredicatesForColumns(columns, column.index_where);
        try validateUniquePredicateExpressionsForColumns(columns, column.index_where_expressions);
        try validateRelationalColumnIndexIncludes(columns, column);
        if (column.on_update_value) |_| {
            if (column.field_type != .numeric and column.field_type != .datetime) return error.InvalidSqlCatalog;
        }
    }
}

pub fn validateRelationalColumnIndexIncludes(columns: []const runtime_schema.RelationalColumn, column: runtime_schema.RelationalColumn) !void {
    if (column.index_include_columns.len == 0) return;
    if (!column.indexed or column.index_name == null) return error.InvalidSqlCatalog;
    for (column.index_include_columns) |field| {
        _ = binder.relationalColumnForDdl(columns, field) orelse return error.InvalidSqlCatalog;
    }
    const index_name = column.index_name.?;
    for (columns) |peer| {
        if (!binder.relationalColumnHasDeclaredIndexName(peer, index_name)) continue;
        if (!stringSlicesEqual(peer.index_include_columns, column.index_include_columns)) return error.InvalidSqlCatalog;
        if (stringSlicesContains(column.index_include_columns, peer.name)) return error.InvalidSqlCatalog;
    }
}

pub fn validateUniqueConstraintCatalog(columns: []const runtime_schema.RelationalColumn, periods: []const runtime_schema.RelationalPeriod, constraints: []const runtime_schema.UniqueConstraint) !void {
    for (constraints, 0..) |constraint, i| {
        if (binder.uniqueConstraintNameExists(constraints[0..i], constraint.name)) return error.InvalidSqlCatalog;
        try validateUniqueConstraintForColumns(columns, periods, constraint);
    }
}

pub fn validateForeignKeyCatalog(columns: []const runtime_schema.RelationalColumn, periods: []const runtime_schema.RelationalPeriod, foreign_keys: []const runtime_schema.ForeignKey) !void {
    for (foreign_keys, 0..) |foreign_key, i| {
        if (binder.foreignKeyNameExists(foreign_keys[0..i], foreign_key.name)) return error.InvalidSqlCatalog;
        try binder.validateForeignKeyForColumns(columns, periods, foreign_key);
    }
}

pub fn validateRelationalCheckCatalog(columns: []const runtime_schema.RelationalColumn, checks: []const runtime_schema.RelationalCheck) !void {
    for (checks, 0..) |check, i| {
        if (binder.relationalCheckNameExists(checks[0..i], check.name)) return error.InvalidSqlCatalog;
        try validateCheckForColumns(columns, check);
    }
}

pub fn validatePrimaryKeyColumns(columns: []const runtime_schema.RelationalColumn, primary_key: runtime_schema.PrimaryKey) !void {
    if (primary_key.columns.len == 0) return error.InvalidSqlCatalog;
    for (primary_key.columns) |column| {
        const found = binder.relationalColumnForDdl(columns, column) orelse return error.InvalidSqlCatalog;
        if (found.nullable) return error.InvalidSqlCatalog;
    }
    try validateCreateIndexIncludeColumns(columns, primary_key.columns, primary_key.include_columns);
}

pub fn generatedColumnReferencesAny(column: runtime_schema.RelationalColumn, fields: []const []const u8) bool {
    const generated = column.generated orelse return false;
    if (generated.field) |field| {
        if (stringSlicesContains(fields, field)) return true;
    }
    if (generated.expression) |expression| {
        if (expressionReferencesAny(expression, fields)) return true;
    }
    return stringSlicesIntersect(generated.fields, fields);
}

pub fn expressionReferencesAny(expression: runtime_schema.RelationalRowsExpression, fields: []const []const u8) bool {
    if (expression.kind == .field and stringSlicesContains(fields, expression.field)) return true;
    for (expression.operands) |operand| {
        if (expressionReferencesAny(operand, fields)) return true;
    }
    for (expression.case_branches) |branch| {
        if (expressionConditionReferencesAny(branch.when, fields)) return true;
        if (expressionReferencesAny(branch.then, fields)) return true;
    }
    for (expression.case_else) |case_else| {
        if (expressionReferencesAny(case_else, fields)) return true;
    }
    return false;
}

pub fn expressionConditionReferencesAny(condition: runtime_schema.RelationalRowsExpressionCondition, fields: []const []const u8) bool {
    if (expressionReferencesAny(condition.lhs, fields)) return true;
    for (condition.rhs) |rhs| {
        if (expressionReferencesAny(rhs, fields)) return true;
    }
    return false;
}

pub fn expressionReferencesField(expression: runtime_schema.RelationalRowsExpression, field: []const u8) bool {
    if (expression.kind == .field and expression.field_source == .row and std.mem.eql(u8, expression.field, field)) return true;
    for (expression.operands) |operand| {
        if (expressionReferencesField(operand, field)) return true;
    }
    for (expression.case_branches) |branch| {
        if (expressionConditionReferencesField(branch.when, field)) return true;
        if (expressionReferencesField(branch.then, field)) return true;
    }
    for (expression.case_else) |fallback| {
        if (expressionReferencesField(fallback, field)) return true;
    }
    return false;
}

pub fn expressionConditionReferencesField(condition: runtime_schema.RelationalRowsExpressionCondition, field: []const u8) bool {
    if (expressionReferencesField(condition.lhs, field)) return true;
    for (condition.rhs) |rhs| {
        if (expressionReferencesField(rhs, field)) return true;
    }
    return false;
}

pub fn uniqueExpressionsEqual(a: []const runtime_schema.UniqueExpression, b: []const runtime_schema.UniqueExpression) bool {
    if (a.len != b.len) return false;
    for (a, b) |left, right| {
        if (left.op != right.op) return false;
        if (!std.mem.eql(u8, left.field, right.field)) return false;
        if (!relationalRowsExpressionOptionalEqual(left.expression, right.expression)) return false;
    }
    return true;
}

pub fn selectorExpressionValueJsonAlloc(
    alloc: std.mem.Allocator,
    expression: db_mod.types.RelationalRowsExpression,
    values: anytype,
) ![]u8 {
    return switch (expression.kind) {
        .field => blk: {
            if (expression.field_source != .row) return error.UnsupportedSqlShape;
            const value_json = fieldValueJsonFor(values, expression.field) orelse return error.UnsupportedSqlShape;
            break :blk try alloc.dupe(u8, value_json);
        },
        .value => try alloc.dupe(u8, expression.value_json),
        .lower, .upper, .initcap, .md5 => blk: {
            if (expression.operands.len != 1) return error.UnsupportedSqlShape;
            const value_json = try selectorExpressionValueJsonAlloc(alloc, expression.operands[0], values);
            defer alloc.free(value_json);
            var parsed = std.json.parseFromSlice(std.json.Value, alloc, value_json, .{}) catch return error.UnsupportedSqlShape;
            defer parsed.deinit();
            switch (parsed.value) {
                .null => break :blk try alloc.dupe(u8, "null"),
                .string => |text| {
                    const transformed = switch (expression.kind) {
                        .lower => try std.ascii.allocLowerString(alloc, text),
                        .upper => try std.ascii.allocUpperString(alloc, text),
                        .initcap => try initcapTextAlloc(alloc, text),
                        .md5 => try md5HexTextAlloc(alloc, text),
                        else => unreachable,
                    };
                    defer alloc.free(transformed);
                    break :blk try std.json.Stringify.valueAlloc(alloc, std.json.Value{ .string = transformed }, .{});
                },
                else => return error.UnsupportedSqlShape,
            }
        },
        .concat => blk: {
            if (expression.operands.len == 0) return error.UnsupportedSqlShape;
            var joined = std.ArrayListUnmanaged(u8).empty;
            defer joined.deinit(alloc);
            for (expression.operands) |operand| {
                const value_json = try selectorExpressionValueJsonAlloc(alloc, operand, values);
                defer alloc.free(value_json);
                var parsed = std.json.parseFromSlice(std.json.Value, alloc, value_json, .{}) catch return error.UnsupportedSqlShape;
                defer parsed.deinit();
                if (parsed.value == .null) continue;
                const text = try selectorScalarJsonValueTextAlloc(alloc, parsed.value);
                defer alloc.free(text);
                try joined.appendSlice(alloc, text);
            }
            break :blk try std.json.Stringify.valueAlloc(alloc, std.json.Value{ .string = joined.items }, .{});
        },
        .concat_ws => blk: {
            if (expression.operands.len < 2) return error.UnsupportedSqlShape;
            const separator_json = try selectorExpressionValueJsonAlloc(alloc, expression.operands[0], values);
            defer alloc.free(separator_json);
            var parsed_separator = std.json.parseFromSlice(std.json.Value, alloc, separator_json, .{}) catch return error.UnsupportedSqlShape;
            defer parsed_separator.deinit();
            if (parsed_separator.value == .null) break :blk try alloc.dupe(u8, "null");
            if (parsed_separator.value != .string) return error.UnsupportedSqlShape;

            var joined = std.ArrayListUnmanaged(u8).empty;
            defer joined.deinit(alloc);
            var emitted = false;
            for (expression.operands[1..]) |operand| {
                const value_json = try selectorExpressionValueJsonAlloc(alloc, operand, values);
                defer alloc.free(value_json);
                var parsed = std.json.parseFromSlice(std.json.Value, alloc, value_json, .{}) catch return error.UnsupportedSqlShape;
                defer parsed.deinit();
                if (parsed.value == .null) continue;
                if (parsed.value != .string) return error.UnsupportedSqlShape;
                if (emitted) try joined.appendSlice(alloc, parsed_separator.value.string);
                try joined.appendSlice(alloc, parsed.value.string);
                emitted = true;
            }
            break :blk try std.json.Stringify.valueAlloc(alloc, std.json.Value{ .string = joined.items }, .{});
        },
        else => error.UnsupportedSqlShape,
    };
}

pub fn fieldValueJsonFor(values: anytype, field: []const u8) ?[]const u8 {
    for (values) |value| {
        if (std.mem.eql(u8, value.field, field)) return value.value_json;
    }
    return null;
}

pub fn fieldValuesContain(values: anytype, field: []const u8) bool {
    return fieldValueJsonFor(values, field) != null;
}

pub fn fieldValuesMatchColumns(values: anytype, columns: []const []const u8) bool {
    if (values.len != columns.len) return false;
    for (columns) |column| {
        if (fieldValueJsonFor(values, column) == null) return false;
    }
    return true;
}

pub fn writeFieldValuesObjectJson(
    writer: *std.Io.Writer,
    values: anytype,
    columns: []const []const u8,
) !void {
    try writer.writeByte('{');
    for (columns, 0..) |column, i| {
        const value_json = fieldValueJsonFor(values, column) orelse return error.UnsupportedSqlShape;
        if (i != 0) try writer.writeByte(',');
        try writer.print("{f}:", .{std.json.fmt(column, .{})});
        try writer.writeAll(value_json);
    }
    try writer.writeByte('}');
}

pub fn writeAllFieldValuesObjectJson(
    writer: *std.Io.Writer,
    values: anytype,
) !void {
    try writer.writeByte('{');
    for (values, 0..) |value, i| {
        if (i != 0) try writer.writeByte(',');
        try writer.print("{f}:", .{std.json.fmt(value.field, .{})});
        try writer.writeAll(value.value_json);
    }
    try writer.writeByte('}');
}

fn selectorScalarJsonValueTextAlloc(alloc: std.mem.Allocator, value: std.json.Value) ![]u8 {
    return switch (value) {
        .string => |text| try alloc.dupe(u8, text),
        .integer => |integer| try std.fmt.allocPrint(alloc, "{d}", .{integer}),
        .float => |float| try std.fmt.allocPrint(alloc, "{d}", .{float}),
        .number_string => |text| try alloc.dupe(u8, text),
        .bool => |enabled| try alloc.dupe(u8, if (enabled) "true" else "false"),
        else => error.UnsupportedSqlShape,
    };
}

pub fn md5HexTextAlloc(alloc: std.mem.Allocator, text: []const u8) ![]u8 {
    const digest = std.crypto.hash.Md5.hashResult(text);
    const out = try alloc.alloc(u8, 32);
    for (digest, 0..) |byte, i| {
        out[i * 2] = std.fmt.digitToChar(byte >> 4, .lower);
        out[i * 2 + 1] = std.fmt.digitToChar(byte & 0x0f, .lower);
    }
    return out;
}

pub fn initcapTextAlloc(alloc: std.mem.Allocator, text: []const u8) ![]u8 {
    const out = try alloc.dupe(u8, text);
    var at_word_start = true;
    for (out) |*byte| {
        if (std.ascii.isAlphanumeric(byte.*)) {
            byte.* = if (at_word_start) std.ascii.toUpper(byte.*) else std.ascii.toLower(byte.*);
            at_word_start = false;
        } else {
            at_word_start = true;
        }
    }
    return out;
}

pub fn selectorJsonValuesEqual(a: std.json.Value, b: std.json.Value) bool {
    if (a == .null or b == .null) return a == .null and b == .null;
    if (a == .bool and b == .bool) return a.bool == b.bool;
    if (a == .string and b == .string) return std.mem.eql(u8, a.string, b.string);
    if (selectorJsonNumber(a)) |left| {
        if (selectorJsonNumber(b)) |right| return left == right;
    }
    return false;
}

pub const SelectorJsonOrder = enum { lt, eq, gt };

pub fn selectorCompareJsonScalars(a: std.json.Value, b: std.json.Value) ?SelectorJsonOrder {
    if (selectorJsonNumber(a)) |left| {
        if (selectorJsonNumber(b)) |right| {
            if (left < right) return .lt;
            if (left > right) return .gt;
            return .eq;
        }
    }
    if (a == .string and b == .string) {
        const order = std.mem.order(u8, a.string, b.string);
        return switch (order) {
            .lt => .lt,
            .eq => .eq,
            .gt => .gt,
        };
    }
    return null;
}

fn selectorJsonNumber(value: std.json.Value) ?f64 {
    return switch (value) {
        .integer => |integer| @floatFromInt(integer),
        .float => |float| float,
        .number_string => |text| std.fmt.parseFloat(f64, text) catch null,
        else => null,
    };
}

pub fn uniqueConstraintReferencesAny(
    constraint: runtime_schema.UniqueConstraint,
    fields: []const []const u8,
) bool {
    if (stringSlicesIntersect(constraint.columns, fields)) return true;
    for (constraint.expressions) |expression| {
        switch (expression.op) {
            .lower, .upper, .md5 => if (stringSlicesContains(fields, expression.field)) return true,
            .expression => if (expression.expression) |row_expression| {
                if (expressionReferencesAny(row_expression, fields)) return true;
            },
        }
    }
    for (constraint.where) |predicate| {
        if (stringSlicesContains(fields, predicate.field)) return true;
    }
    for (constraint.where_expressions) |condition| {
        if (expressionConditionReferencesAny(condition, fields)) return true;
    }
    return false;
}

pub fn windowOutputFieldIsUnique(
    fields: []const []const u8,
    windows: []const db_mod.types.RelationalRowsWindowSpec,
    field: []const u8,
) bool {
    var matches: usize = 0;
    for (fields) |candidate| {
        if (std.mem.eql(u8, candidate, field)) matches += 1;
    }
    for (windows) |window| {
        if (std.mem.eql(u8, window.output, field)) matches += 1;
    }
    return matches == 1;
}

pub fn findUniqueConstraintByColumnSet(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    values: anytype,
) ?runtime_schema.UniqueConstraint {
    for (schema.unique_constraints) |constraint| {
        if (constraint.validation_state != .enforced) continue;
        if (constraint.expressions.len != 0) continue;
        if (uniqueConstraintMatchesPointSelector(alloc, constraint, values)) return constraint;
    }
    return null;
}

fn uniqueConstraintMatchesPointSelector(
    alloc: std.mem.Allocator,
    constraint: runtime_schema.UniqueConstraint,
    values: anytype,
) bool {
    if (constraint.where.len == 0 and constraint.where_expressions.len == 0) return fieldValuesMatchColumns(values, constraint.columns);
    for (constraint.columns) |column| {
        if (fieldValueJsonFor(values, column) == null) return false;
    }
    for (values) |value| {
        if (!uniqueConstraintAllowsPointSelectorField(constraint, value.field)) return false;
    }
    for (constraint.where) |predicate| {
        if (!uniquePredicateProvenByFieldValues(predicate, values)) return false;
    }
    for (constraint.where_expressions) |condition| {
        if (!(uniqueExpressionPredicateProvenByFieldValues(alloc, condition, values) catch false)) return false;
    }
    return true;
}

fn uniqueConstraintAllowsPointSelectorField(constraint: runtime_schema.UniqueConstraint, field: []const u8) bool {
    if (stringSlicesContains(constraint.columns, field)) return true;
    for (constraint.where) |predicate| {
        if (std.mem.eql(u8, predicate.field, field)) return true;
    }
    for (constraint.where_expressions) |condition| {
        if (expressionConditionReferencesField(condition, field)) return true;
    }
    return false;
}

fn uniquePredicateProvenByFieldValues(predicate: runtime_schema.UniquePredicate, values: anytype) bool {
    const value_json = fieldValueJsonFor(values, predicate.field) orelse return false;
    return switch (predicate.op) {
        .eq => if (predicate.value_json) |expected| std.mem.eql(u8, value_json, expected) else false,
        .ne => if (predicate.value_json) |forbidden|
            !std.mem.eql(u8, value_json, "null") and !std.mem.eql(u8, value_json, forbidden)
        else
            false,
        .is_not_null => !std.mem.eql(u8, value_json, "null"),
        .is_null => std.mem.eql(u8, value_json, "null"),
    };
}

fn uniqueExpressionPredicateProvenByFieldValues(
    alloc: std.mem.Allocator,
    condition: db_mod.types.RelationalRowsExpressionCondition,
    values: anytype,
) !bool {
    const lhs_json = try selectorExpressionValueJsonAlloc(alloc, condition.lhs, values);
    defer alloc.free(lhs_json);
    var lhs = std.json.parseFromSlice(std.json.Value, alloc, lhs_json, .{}) catch return false;
    defer lhs.deinit();

    return switch (condition.op) {
        .is_null => lhs.value == .null,
        .is_not_null => lhs.value != .null,
        .eq, .ne, .is_distinct, .is_not_distinct => blk: {
            if (condition.rhs.len != 1) return false;
            const rhs_json = try selectorExpressionValueJsonAlloc(alloc, condition.rhs[0], values);
            defer alloc.free(rhs_json);
            var rhs = std.json.parseFromSlice(std.json.Value, alloc, rhs_json, .{}) catch return false;
            defer rhs.deinit();
            const equal = selectorJsonValuesEqual(lhs.value, rhs.value);
            break :blk switch (condition.op) {
                .eq, .is_not_distinct => equal,
                .ne, .is_distinct => !equal,
                else => unreachable,
            };
        },
        .gt, .gte, .lt, .lte => blk: {
            if (condition.rhs.len != 1) return false;
            const rhs_json = try selectorExpressionValueJsonAlloc(alloc, condition.rhs[0], values);
            defer alloc.free(rhs_json);
            var rhs = std.json.parseFromSlice(std.json.Value, alloc, rhs_json, .{}) catch return false;
            defer rhs.deinit();
            const comparison = selectorCompareJsonScalars(lhs.value, rhs.value) orelse return false;
            break :blk switch (condition.op) {
                .gt => comparison == .gt,
                .gte => comparison == .gt or comparison == .eq,
                .lt => comparison == .lt,
                .lte => comparison == .lt or comparison == .eq,
                else => unreachable,
            };
        },
    };
}

pub fn conflictInsertedValueForColumn(insert_columns: []const []const u8, row: []const []const u8, column: []const u8) ?[]const u8 {
    if (insert_columns.len != row.len) return null;
    for (insert_columns, row) |insert_column, value_json| {
        if (std.mem.eql(u8, insert_column, column)) return value_json;
    }
    return null;
}

pub fn findUniqueConstraintByColumnsAndExpressions(
    schema: runtime_schema.TableSchema,
    columns: []const []const u8,
    expressions: []const runtime_schema.UniqueExpression,
    require_partial: bool,
) ?runtime_schema.UniqueConstraint {
    for (schema.unique_constraints) |constraint| {
        if (constraint.validation_state != .enforced) continue;
        if (require_partial and constraint.where.len == 0) continue;
        if (!require_partial and constraint.where.len != 0) continue;
        if (!stringSlicesEqual(constraint.columns, columns)) continue;
        if (!uniqueExpressionsEqual(constraint.expressions, expressions)) continue;
        return constraint;
    }
    return null;
}

pub fn findUniqueConstraintByColumnsExpressionsAndConflictWhere(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    columns: []const []const u8,
    expressions: []const runtime_schema.UniqueExpression,
    where_json: []const u8,
    where_expressions: []const db_mod.types.RelationalRowsExpressionCondition,
) !?runtime_schema.UniqueConstraint {
    const has_field_where = where_json.len > 0;
    const has_expression_where = where_expressions.len > 0;
    if (has_field_where and has_expression_where) return error.UnsupportedSqlShape;

    for (schema.unique_constraints) |constraint| {
        if (constraint.validation_state != .enforced) continue;
        if (!stringSlicesEqual(constraint.columns, columns)) continue;
        if (!uniqueExpressionsEqual(constraint.expressions, expressions)) continue;

        if (!has_field_where and !has_expression_where) {
            if (constraint.where.len == 0 and constraint.where_expressions.len == 0) return constraint;
            continue;
        }

        if (has_field_where) {
            if (constraint.where.len != 0 and constraint.where_expressions.len == 0) {
                validateUniqueWhereJsonMatches(alloc, where_json, constraint.where) catch continue;
                return constraint;
            }
            if (constraint.where.len == 0 and constraint.where_expressions.len != 0) {
                const predicates = try relationalChecksFromUniqueWhereJsonAlloc(alloc, where_json);
                defer {
                    freeRelationalChecks(alloc, predicates);
                    if (predicates.len > 0) alloc.free(predicates);
                }
                if (try db_mod.DB.relationalRowsExpressionConditionsImpliedByEqualityPredicatesAlloc(
                    alloc,
                    predicates,
                    constraint.where_expressions,
                )) return constraint;
            }
            continue;
        }

        if (constraint.where.len != 0) continue;
        if (relationalRowsExpressionConditionsEqual(constraint.where_expressions, where_expressions)) return constraint;
    }
    return null;
}

fn relationalChecksFromUniqueWhereJsonAlloc(
    alloc: std.mem.Allocator,
    where_json: []const u8,
) ![]runtime_schema.RelationalCheck {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, where_json, .{}) catch return error.UnsupportedSqlShape;
    defer parsed.deinit();
    if (parsed.value != .object) return error.UnsupportedSqlShape;
    const all_value = parsed.value.object.get("all") orelse return error.UnsupportedSqlShape;
    if (all_value != .array) return error.UnsupportedSqlShape;
    const out = try alloc.alloc(runtime_schema.RelationalCheck, all_value.array.items.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |check| {
            alloc.free(check.field);
            if (check.value_json) |json| alloc.free(json);
        }
        alloc.free(out);
    }
    for (all_value.array.items) |item| {
        if (item != .object) return error.UnsupportedSqlShape;
        const field_value = item.object.get("field") orelse return error.UnsupportedSqlShape;
        const op_value = item.object.get("op") orelse return error.UnsupportedSqlShape;
        if (field_value != .string or op_value != .string) return error.UnsupportedSqlShape;
        const op = relationalCheckOpFromUniquePredicateToken(op_value.string) orelse return error.UnsupportedSqlShape;
        const field = try alloc.dupe(u8, field_value.string);
        var field_transferred = false;
        errdefer if (!field_transferred) alloc.free(field);
        const value_json: ?[]const u8 = if (item.object.get("value")) |value| try std.json.Stringify.valueAlloc(alloc, value, .{}) else null;
        var value_transferred = false;
        errdefer if (!value_transferred) if (value_json) |json| alloc.free(json);
        out[initialized] = .{
            .name = "",
            .field = field,
            .op = op,
            .value_json = value_json,
        };
        initialized += 1;
        field_transferred = true;
        value_transferred = true;
    }
    return out;
}

fn validateUniqueWhereJsonMatches(alloc: std.mem.Allocator, where_json: []const u8, predicates: []const runtime_schema.UniquePredicate) !void {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, where_json, .{}) catch return error.UnsupportedSqlShape;
    defer parsed.deinit();
    if (parsed.value != .object) return error.UnsupportedSqlShape;
    const all_value = parsed.value.object.get("all") orelse return error.UnsupportedSqlShape;
    if (all_value != .array or all_value.array.items.len != predicates.len) return error.UnsupportedSqlShape;
    for (all_value.array.items, predicates) |item, predicate| {
        if (item != .object) return error.UnsupportedSqlShape;
        const field_value = item.object.get("field") orelse return error.UnsupportedSqlShape;
        const op_value = item.object.get("op") orelse return error.UnsupportedSqlShape;
        if (field_value != .string or !std.mem.eql(u8, field_value.string, predicate.field)) return error.UnsupportedSqlShape;
        if (op_value != .string or !std.mem.eql(u8, op_value.string, uniquePredicateOpToken(predicate.op))) return error.UnsupportedSqlShape;
        const supplied_value = item.object.get("value");
        if (predicate.value_json) |expected_json| {
            const supplied = supplied_value orelse return error.UnsupportedSqlShape;
            const supplied_json = try std.json.Stringify.valueAlloc(alloc, supplied, .{});
            defer alloc.free(supplied_json);
            if (!std.mem.eql(u8, supplied_json, expected_json)) return error.UnsupportedSqlShape;
        } else if (supplied_value != null) {
            return error.UnsupportedSqlShape;
        }
    }
}

pub fn queryHasOnlySimpleUnionPredicateSurface(query: db_mod.types.RelationalRowsQueryRequest) bool {
    if (query.array_any.len != 0 or
        query.array_contains.len != 0 or
        query.array_eq.len != 0 or
        query.json_contains.len != 0 or
        query.json_path_eq.len != 0 or
        query.json_path_exists.len != 0 or
        query.text_patterns.len != 0 or
        query.not_predicates.len != 0 or
        query.access_or_predicates.len != 0 or
        query.access_not_predicates.len != 0 or
        query.expression_not_predicates.len != 0 or
        query.expression_array_contains.len != 0 or
        query.json_extract.len != 0 or
        query.array_length.len != 0 or
        query.coalesce.len != 0 or
        query.field_aliases.len != 0 or
        query.distinct_on.len != 0 or
        query.distinct_on_expressions.len != 0 or
        query.order_by.len != 0 or
        query.row_claim != null or
        query.doc_key_range != null or
        query.limit != null or
        query.offset != 0)
    {
        return false;
    }
    return true;
}

pub fn queryHasOnlySimpleIntersectExceptPredicateSurface(query: db_mod.types.RelationalRowsQueryRequest) bool {
    if (query.array_any.len != 0 or
        query.array_contains.len != 0 or
        query.array_eq.len != 0 or
        query.json_contains.len != 0 or
        query.json_path_eq.len != 0 or
        query.json_path_exists.len != 0 or
        query.text_patterns.len != 0 or
        query.not_predicates.len != 0 or
        query.access_or_predicates.len != 0 or
        query.access_not_predicates.len != 0 or
        query.expression_not_predicates.len != 0 or
        query.expression_array_contains.len != 0 or
        query.json_extract.len != 0 or
        query.array_length.len != 0 or
        query.coalesce.len != 0 or
        query.field_aliases.len != 0 or
        query.distinct_on.len != 0 or
        query.distinct_on_expressions.len != 0 or
        query.order_by.len != 0 or
        query.row_claim != null or
        query.doc_key_range != null or
        query.limit != null or
        query.offset != 0)
    {
        return false;
    }
    return true;
}

pub fn queryHasNoSimpleSetPredicates(query: db_mod.types.RelationalRowsQueryRequest) bool {
    return query.predicates.len == 0 and
        query.or_predicates.len == 0 and
        query.in_predicates.len == 0 and
        query.access_or_predicates.len == 0 and
        query.expression_predicates.len == 0 and
        query.expression_or_predicates.len == 0;
}

pub fn sourceQueryUsesExtendedPredicates(query: db_mod.types.RelationalRowsQueryRequest) bool {
    return query.array_any.len > 0 or
        query.in_predicates.len > 0 or
        query.json_contains.len > 0 or
        query.json_path_exists.len > 0 or
        query.array_contains.len > 0 or
        query.array_eq.len > 0 or
        query.access_or_predicates.len > 0 or
        query.access_not_predicates.len > 0 or
        query.expression_predicates.len > 0 or
        query.expression_or_predicates.len > 0 or
        query.expression_not_predicates.len > 0 or
        query.expression_array_contains.len > 0;
}

pub fn querySupportsSimpleUnionRewrite(query: db_mod.types.RelationalRowsQueryRequest) bool {
    if (!queryHasOnlySimpleUnionPredicateSurface(query)) return false;
    if (query.or_predicates.len > 0 and
        (query.predicates.len > 0 or query.in_predicates.len > 0 or query.expression_predicates.len > 0 or query.expression_or_predicates.len > 0))
    {
        return false;
    }
    if (query.in_predicates.len > 0 and query.or_predicates.len > 0) return false;
    if (query.expression_or_predicates.len != 0) {
        if (query.or_predicates.len != 0) return false;
    }
    return true;
}

pub fn querySupportsSimpleIntersectExceptRewrite(query: db_mod.types.RelationalRowsQueryRequest) bool {
    if (!queryHasOnlySimpleIntersectExceptPredicateSurface(query)) return false;
    if (query.expression_or_predicates.len != 0) {
        if (query.or_predicates.len != 0) return false;
    }
    if (query.or_predicates.len != 0 and
        (query.predicates.len != 0 or query.in_predicates.len != 0 or query.expression_predicates.len != 0))
    {
        return false;
    }
    return true;
}

pub fn validateSimpleSetOperationSelect(
    lhs: db_mod.types.RelationalRowsQueryRequest,
    rhs: db_mod.types.RelationalRowsQueryRequest,
    op: ast.SelectSetOperation,
) !void {
    if (!std.mem.eql(u8, lhs.source_cte, rhs.source_cte)) return error.UnsupportedSqlShape;
    switch (op) {
        .union_distinct, .union_all => {
            if (!querySupportsSimpleUnionRewrite(lhs)) return error.UnsupportedSqlShape;
            if (!querySupportsSimpleUnionRewrite(rhs)) return error.UnsupportedSqlShape;
        },
        .intersect, .except => {
            if (!querySupportsSimpleIntersectExceptRewrite(lhs)) return error.UnsupportedSqlShape;
            if (!querySupportsSimpleIntersectExceptRewrite(rhs)) return error.UnsupportedSqlShape;
        },
    }
}

pub fn simpleSetQueriesProvablyDisjoint(
    alloc: std.mem.Allocator,
    lhs: db_mod.types.RelationalRowsQueryRequest,
    rhs: db_mod.types.RelationalRowsQueryRequest,
) !bool {
    if (lhs.in_predicates.len > 0 or rhs.in_predicates.len > 0) {
        if (!querySupportsSimpleUnionRewrite(lhs) or !querySupportsSimpleUnionRewrite(rhs)) return false;
        if (lhs.expression_predicates.len > 0 or rhs.expression_predicates.len > 0 or
            lhs.expression_or_predicates.len > 0 or rhs.expression_or_predicates.len > 0)
        {
            if (try simpleSetQueriesExpressionBranchesProvablyDisjoint(alloc, lhs, rhs)) return true;
        }
        const lhs_branch_count = simpleAccessSetQueryBranchCount(lhs);
        const rhs_branch_count = simpleAccessSetQueryBranchCount(rhs);
        if (lhs_branch_count == 0 or rhs_branch_count == 0) return false;
        var access_branches_disjoint = true;
        for (0..lhs_branch_count) |left_index| {
            const left = simpleAccessSetQueryBranchAt(lhs, left_index) orelse return false;
            for (0..rhs_branch_count) |right_index| {
                const right = simpleAccessSetQueryBranchAt(rhs, right_index) orelse return false;
                if (!try simpleAccessSetBranchesProvablyDisjoint(alloc, left, right)) {
                    access_branches_disjoint = false;
                    break;
                }
            }
            if (!access_branches_disjoint) break;
        }
        return access_branches_disjoint;
    }

    if (lhs.or_predicates.len > 0 or rhs.or_predicates.len > 0) {
        if (!querySupportsSimpleUnionRewrite(lhs) or !querySupportsSimpleUnionRewrite(rhs)) return false;
        if (lhs.expression_predicates.len > 0 or rhs.expression_predicates.len > 0) return false;
        const lhs_branch_count = simpleScalarSetQueryBranchCount(lhs);
        const rhs_branch_count = simpleScalarSetQueryBranchCount(rhs);
        if (lhs_branch_count == 0 or rhs_branch_count == 0) return false;
        for (0..lhs_branch_count) |left_index| {
            const left = simpleScalarSetQueryBranchAt(lhs, left_index) orelse return false;
            for (0..rhs_branch_count) |right_index| {
                const right = simpleScalarSetQueryBranchAt(rhs, right_index) orelse return false;
                if (!relationalCheckBranchesProvablyDisjoint(left, right)) return false;
            }
        }
        return true;
    }

    if (lhs.expression_or_predicates.len > 0 or rhs.expression_or_predicates.len > 0) {
        if (!querySupportsSimpleUnionRewrite(lhs) or !querySupportsSimpleUnionRewrite(rhs)) return false;
        return try simpleSetQueriesExpressionBranchesProvablyDisjoint(alloc, lhs, rhs);
    }

    for (lhs.predicates) |left| {
        for (rhs.predicates) |right| {
            if (relationalChecksProvablyDisjoint(left, right)) return true;
        }
        for (rhs.expression_predicates) |right| {
            if (relationalCheckAndExpressionConditionProvablyDisjoint(left, right)) return true;
        }
    }
    for (lhs.expression_predicates) |left| {
        for (rhs.predicates) |right| {
            if (relationalCheckAndExpressionConditionProvablyDisjoint(right, left)) return true;
        }
        for (rhs.expression_predicates) |right| {
            if (expressionConditionsProvablyDisjoint(left, right)) return true;
        }
    }
    return false;
}

pub const SimpleAccessSetBranch = struct {
    predicates: []const runtime_schema.RelationalCheck = &.{},
    in_predicates: []const db_mod.types.RelationalRowsInPredicate = &.{},
};

pub fn simpleScalarSetQueryBranchCount(query: db_mod.types.RelationalRowsQueryRequest) usize {
    if (query.or_predicates.len > 0) return query.or_predicates.len;
    if (query.predicates.len > 0) return 1;
    return 0;
}

pub fn simpleScalarSetQueryBranchAt(
    query: db_mod.types.RelationalRowsQueryRequest,
    index: usize,
) ?[]const runtime_schema.RelationalCheck {
    if (query.or_predicates.len > 0) {
        if (index >= query.or_predicates.len) return null;
        return query.or_predicates[index].predicates;
    }
    if (query.predicates.len > 0 and index == 0) return query.predicates;
    return null;
}

pub fn simpleExpressionSetQueryBranchCount(query: db_mod.types.RelationalRowsQueryRequest) usize {
    if (query.expression_or_predicates.len > 0) return query.expression_or_predicates.len;
    if (query.or_predicates.len > 0) return query.or_predicates.len;
    if (query.predicates.len > 0 or query.expression_predicates.len > 0) return 1;
    return 0;
}

pub fn simpleAccessSetQueryBranchCount(query: db_mod.types.RelationalRowsQueryRequest) usize {
    if (query.or_predicates.len > 0) return query.or_predicates.len;
    if (query.predicates.len > 0 or query.in_predicates.len > 0) return 1;
    return 0;
}

pub fn simpleAccessSetQueryBranchAt(
    query: db_mod.types.RelationalRowsQueryRequest,
    index: usize,
) ?SimpleAccessSetBranch {
    if (query.or_predicates.len > 0) {
        if (index >= query.or_predicates.len) return null;
        return .{ .predicates = query.or_predicates[index].predicates };
    }
    if ((query.predicates.len > 0 or query.in_predicates.len > 0) and index == 0) {
        return .{ .predicates = query.predicates, .in_predicates = query.in_predicates };
    }
    return null;
}

fn relationalCheckBranchesProvablyDisjoint(
    lhs: []const runtime_schema.RelationalCheck,
    rhs: []const runtime_schema.RelationalCheck,
) bool {
    if (lhs.len == 0 or rhs.len == 0) return false;
    for (lhs) |left| {
        for (rhs) |right| {
            if (relationalChecksProvablyDisjoint(left, right)) return true;
        }
    }
    return false;
}

fn expressionConditionBranchesProvablyDisjoint(
    lhs: []const db_mod.types.RelationalRowsExpressionCondition,
    rhs: []const db_mod.types.RelationalRowsExpressionCondition,
) bool {
    if (lhs.len == 0 or rhs.len == 0) return false;
    for (lhs) |left| {
        for (rhs) |right| {
            if (expressionConditionsProvablyDisjoint(left, right)) return true;
        }
    }
    return false;
}

fn simpleSetQueriesExpressionBranchesProvablyDisjoint(
    alloc: std.mem.Allocator,
    lhs: db_mod.types.RelationalRowsQueryRequest,
    rhs: db_mod.types.RelationalRowsQueryRequest,
) !bool {
    if (lhs.or_predicates.len > 0 or rhs.or_predicates.len > 0) return false;

    const lhs_groups = try expressionGroupsFromSimpleUnionQueryAlloc(alloc, lhs);
    defer {
        freeExpressionPredicateGroups(alloc, lhs_groups);
        if (lhs_groups.len > 0) alloc.free(lhs_groups);
    }
    const rhs_groups = try expressionGroupsFromSimpleUnionQueryAlloc(alloc, rhs);
    defer {
        freeExpressionPredicateGroups(alloc, rhs_groups);
        if (rhs_groups.len > 0) alloc.free(rhs_groups);
    }

    if (lhs_groups.len == 0 or rhs_groups.len == 0) return false;
    for (lhs_groups) |left| {
        for (rhs_groups) |right| {
            if (!expressionConditionBranchesProvablyDisjoint(left.conditions, right.conditions)) return false;
        }
    }
    return true;
}

const InPredicateContainmentProof = enum {
    contains,
    does_not_contain,
    unknown,
};

fn simpleAccessSetBranchesProvablyDisjoint(
    alloc: std.mem.Allocator,
    lhs: SimpleAccessSetBranch,
    rhs: SimpleAccessSetBranch,
) !bool {
    if (lhs.predicates.len == 0 and lhs.in_predicates.len == 0) return false;
    if (rhs.predicates.len == 0 and rhs.in_predicates.len == 0) return false;

    for (lhs.predicates) |left| {
        for (rhs.predicates) |right| {
            if (relationalChecksProvablyDisjoint(left, right)) return true;
        }
        for (rhs.in_predicates) |right| {
            if (try inPredicateAndRelationalCheckProvablyDisjoint(alloc, right, left)) return true;
        }
    }
    for (lhs.in_predicates) |left| {
        for (rhs.predicates) |right| {
            if (try inPredicateAndRelationalCheckProvablyDisjoint(alloc, left, right)) return true;
        }
        for (rhs.in_predicates) |right| {
            if (try inPredicatesProvablyDisjoint(alloc, left, right)) return true;
        }
    }
    return false;
}

fn inPredicateAndRelationalCheckProvablyDisjoint(
    alloc: std.mem.Allocator,
    in_predicate: db_mod.types.RelationalRowsInPredicate,
    check: runtime_schema.RelationalCheck,
) !bool {
    if (check.expression != null) return false;
    if (!std.mem.eql(u8, in_predicate.field, check.field)) return false;

    switch (check.op) {
        .eq, .is_not_distinct => {
            const value_json = check.value_json orelse return false;
            const containment = try inPredicateValuesContainJsonLiteralProof(alloc, in_predicate, value_json);
            return containment == .does_not_contain;
        },
        .is_null => {
            const containment = try inPredicateValuesContainJsonLiteralProof(alloc, in_predicate, "null");
            return containment == .does_not_contain;
        },
        else => return false,
    }
}

fn inPredicatesProvablyDisjoint(
    alloc: std.mem.Allocator,
    lhs: db_mod.types.RelationalRowsInPredicate,
    rhs: db_mod.types.RelationalRowsInPredicate,
) !bool {
    if (lhs.negated or rhs.negated) return false;
    if (!std.mem.eql(u8, lhs.field, rhs.field)) return false;

    var parsed = std.json.parseFromSlice(std.json.Value, alloc, lhs.values_json, .{}) catch return false;
    defer parsed.deinit();
    if (parsed.value != .array) return false;

    for (parsed.value.array.items) |value| {
        const value_json = (try jsonValueScalarProofLiteralAlloc(alloc, value)) orelse return false;
        defer alloc.free(value_json);
        const containment = try inPredicateValuesContainJsonLiteralProof(alloc, rhs, value_json);
        switch (containment) {
            .contains, .unknown => return false,
            .does_not_contain => {},
        }
    }
    return true;
}

fn inPredicateValuesContainJsonLiteralProof(
    alloc: std.mem.Allocator,
    predicate: db_mod.types.RelationalRowsInPredicate,
    value_json: []const u8,
) !InPredicateContainmentProof {
    if (predicate.negated) return .unknown;
    if (!jsonIsSafeDisjointProofLiteral(value_json) and !jsonIsJsonNumberLiteral(value_json)) return .unknown;

    var parsed = std.json.parseFromSlice(std.json.Value, alloc, predicate.values_json, .{}) catch return .unknown;
    defer parsed.deinit();
    if (parsed.value != .array) return .unknown;

    for (parsed.value.array.items) |value| {
        const item_json = (try jsonValueScalarProofLiteralAlloc(alloc, value)) orelse return .unknown;
        defer alloc.free(item_json);
        if (std.mem.eql(u8, item_json, value_json)) return .contains;
    }
    return .does_not_contain;
}

fn jsonValueScalarProofLiteralAlloc(alloc: std.mem.Allocator, value: std.json.Value) !?[]const u8 {
    const value_json = try std.json.Stringify.valueAlloc(alloc, value, .{});
    if (jsonIsSafeDisjointProofLiteral(value_json) or jsonIsJsonNumberLiteral(value_json)) return value_json;
    alloc.free(value_json);
    return null;
}

pub fn cloneSimpleScalarSetQueryBranchesInto(
    alloc: std.mem.Allocator,
    out: []db_mod.types.RelationalRowsPredicateGroup,
    initialized: *usize,
    query: db_mod.types.RelationalRowsQueryRequest,
) !void {
    const branch_count = simpleScalarSetQueryBranchCount(query);
    if (branch_count == 0 or out.len - initialized.* < branch_count) return error.UnsupportedSqlShape;
    for (0..branch_count) |index| {
        const branch = simpleScalarSetQueryBranchAt(query, index) orelse return error.UnsupportedSqlShape;
        out[initialized.*] = .{ .predicates = try cloneQueryRelationalChecksAlloc(alloc, branch) };
        initialized.* += 1;
    }
}

pub fn cloneSimpleScalarSetQueryBranchesAlloc(
    alloc: std.mem.Allocator,
    query: db_mod.types.RelationalRowsQueryRequest,
) ![]db_mod.types.RelationalRowsPredicateGroup {
    const branch_count = simpleScalarSetQueryBranchCount(query);
    if (branch_count == 0) return error.UnsupportedSqlShape;
    const groups = try alloc.alloc(db_mod.types.RelationalRowsPredicateGroup, branch_count);
    var initialized: usize = 0;
    errdefer {
        for (groups[0..initialized]) |group| freePredicateGroup(alloc, group);
        alloc.free(groups);
    }
    try cloneSimpleScalarSetQueryBranchesInto(alloc, groups, &initialized, query);
    return groups;
}

pub fn cloneSimpleExpressionSetQueryBranchesAlloc(
    alloc: std.mem.Allocator,
    query: db_mod.types.RelationalRowsQueryRequest,
) ![]db_mod.types.RelationalRowsExpressionPredicateGroup {
    const branch_count = simpleExpressionSetQueryBranchCount(query);
    if (branch_count == 0) return error.UnsupportedSqlShape;
    const groups = try alloc.alloc(db_mod.types.RelationalRowsExpressionPredicateGroup, branch_count);
    var initialized: usize = 0;
    errdefer {
        for (groups[0..initialized]) |group| freeExpressionPredicateGroup(alloc, group);
        alloc.free(groups);
    }
    for (0..branch_count) |index| {
        groups[initialized] = .{
            .conditions = try expressionConditionsFromSimpleExpressionSetQueryBranchAlloc(alloc, query, index),
        };
        initialized += 1;
    }
    return groups;
}

pub fn expressionGroupsFromSimpleIntersectQueryAlloc(
    alloc: std.mem.Allocator,
    query: db_mod.types.RelationalRowsQueryRequest,
) ![]db_mod.types.RelationalRowsExpressionPredicateGroup {
    if (queryHasNoSimpleSetPredicates(query)) return &.{};
    if (query.in_predicates.len > 0) return try expressionGroupsFromInSetQueryAlloc(alloc, query);
    return try cloneSimpleExpressionSetQueryBranchesAlloc(alloc, query);
}

pub fn cloneSimpleAccessSetQueryBranchesInto(
    alloc: std.mem.Allocator,
    out: []db_mod.types.RelationalRowsAccessPredicateGroup,
    initialized: *usize,
    query: db_mod.types.RelationalRowsQueryRequest,
) !void {
    const branch_count = simpleAccessSetQueryBranchCount(query);
    if (branch_count == 0 or out.len - initialized.* < branch_count) return error.UnsupportedSqlShape;
    for (0..branch_count) |index| {
        const branch = simpleAccessSetQueryBranchAt(query, index) orelse return error.UnsupportedSqlShape;
        const predicates = try cloneQueryRelationalChecksAlloc(alloc, branch.predicates);
        var predicates_transferred = false;
        errdefer if (!predicates_transferred) {
            freeRelationalChecks(alloc, predicates);
            if (predicates.len > 0) alloc.free(predicates);
        };
        const in_predicates = try cloneInPredicatesAlloc(alloc, branch.in_predicates);
        var in_transferred = false;
        errdefer if (!in_transferred) {
            freeInPredicates(alloc, in_predicates);
            if (in_predicates.len > 0) alloc.free(in_predicates);
        };
        const group = db_mod.types.RelationalRowsAccessPredicateGroup{
            .predicates = predicates,
            .in_predicates = in_predicates,
        };
        var group_transferred = false;
        errdefer if (!group_transferred) freeAccessPredicateGroup(alloc, group);
        out[initialized.*] = group;
        predicates_transferred = true;
        in_transferred = true;
        group_transferred = true;
        initialized.* += 1;
    }
}

const InPredicateExpansion = struct {
    field: []const u8,
    values_json: []const []const u8,
};

fn freeInPredicateExpansions(alloc: std.mem.Allocator, expansions: []const InPredicateExpansion) void {
    for (expansions) |expansion| {
        for (expansion.values_json) |value_json| alloc.free(value_json);
        if (expansion.values_json.len > 0) alloc.free(expansion.values_json);
    }
    if (expansions.len > 0) alloc.free(expansions);
}

fn inPredicateExpansionsAlloc(
    alloc: std.mem.Allocator,
    predicates: []const db_mod.types.RelationalRowsInPredicate,
) ![]InPredicateExpansion {
    if (predicates.len == 0) return error.UnsupportedSqlShape;
    const expansions = try alloc.alloc(InPredicateExpansion, predicates.len);
    var initialized: usize = 0;
    errdefer {
        for (expansions[0..initialized]) |expansion| {
            for (expansion.values_json) |value_json| alloc.free(value_json);
            if (expansion.values_json.len > 0) alloc.free(expansion.values_json);
        }
        alloc.free(expansions);
    }

    for (predicates) |predicate| {
        if (predicate.negated) return error.UnsupportedSqlShape;

        var parsed = std.json.parseFromSlice(std.json.Value, alloc, predicate.values_json, .{}) catch return error.UnsupportedSqlShape;
        defer parsed.deinit();
        if (parsed.value != .array or parsed.value.array.items.len == 0) return error.UnsupportedSqlShape;

        const values = try alloc.alloc([]const u8, parsed.value.array.items.len);
        var values_initialized: usize = 0;
        errdefer {
            for (values[0..values_initialized]) |value_json| alloc.free(value_json);
            alloc.free(values);
        }
        for (parsed.value.array.items) |value| {
            values[values_initialized] = try std.json.Stringify.valueAlloc(alloc, value, .{});
            values_initialized += 1;
        }

        expansions[initialized] = .{
            .field = predicate.field,
            .values_json = values,
        };
        initialized += 1;
    }
    return expansions;
}

fn inPredicateExpansionBranchCount(expansions: []const InPredicateExpansion) !usize {
    var total: usize = 1;
    for (expansions) |expansion| {
        if (expansion.values_json.len == 0) return error.UnsupportedSqlShape;
        if (total > max_scalar_or_expanded_branches / expansion.values_json.len) return error.UnsupportedSqlShape;
        total *= expansion.values_json.len;
    }
    return total;
}

pub fn expressionGroupsFromInSetQueryAlloc(
    alloc: std.mem.Allocator,
    query: db_mod.types.RelationalRowsQueryRequest,
) ![]db_mod.types.RelationalRowsExpressionPredicateGroup {
    if (query.in_predicates.len == 0) return error.UnsupportedSqlShape;

    const expansions = try inPredicateExpansionsAlloc(alloc, query.in_predicates);
    defer freeInPredicateExpansions(alloc, expansions);
    const branch_count = try inPredicateExpansionBranchCount(expansions);
    if (branch_count == 0) return error.UnsupportedSqlShape;

    const base_conditions = try expressionConditionsFromSimpleSetQueryAlloc(alloc, query);
    defer {
        freeExpressionConditions(alloc, base_conditions);
        if (base_conditions.len > 0) alloc.free(base_conditions);
    }

    const groups = try alloc.alloc(db_mod.types.RelationalRowsExpressionPredicateGroup, branch_count);
    var initialized: usize = 0;
    errdefer {
        for (groups[0..initialized]) |group| freeExpressionPredicateGroup(alloc, group);
        alloc.free(groups);
    }

    for (0..branch_count) |branch_index| {
        const conditions = try alloc.alloc(db_mod.types.RelationalRowsExpressionCondition, base_conditions.len + expansions.len);
        var condition_count: usize = 0;
        errdefer {
            for (conditions[0..condition_count]) |condition| freeExpressionCondition(alloc, condition);
            alloc.free(conditions);
        }

        for (base_conditions) |condition| {
            conditions[condition_count] = try cloneExpressionConditionAlloc(alloc, condition);
            condition_count += 1;
        }

        var quotient = branch_index;
        for (expansions) |expansion| {
            const value_index = quotient % expansion.values_json.len;
            quotient /= expansion.values_json.len;
            const predicate: runtime_schema.RelationalCheck = .{
                .name = "",
                .field = expansion.field,
                .op = .eq,
                .value_json = expansion.values_json[value_index],
            };
            conditions[condition_count] = try expressionConditionFromRelationalCheckAlloc(alloc, predicate);
            condition_count += 1;
        }

        groups[initialized] = .{ .conditions = conditions };
        initialized += 1;
    }

    errdefer {
        freeExpressionPredicateGroups(alloc, groups);
        alloc.free(groups);
    }
    if (query.expression_or_predicates.len != 0) {
        if (groups.len > max_scalar_or_expanded_branches / query.expression_or_predicates.len) return error.UnsupportedSqlShape;
        const combined = try alloc.alloc(db_mod.types.RelationalRowsExpressionPredicateGroup, groups.len * query.expression_or_predicates.len);
        var combined_initialized: usize = 0;
        errdefer {
            for (combined[0..combined_initialized]) |group| freeExpressionPredicateGroup(alloc, group);
            alloc.free(combined);
        }
        for (groups) |left| {
            for (query.expression_or_predicates) |right| {
                combined[combined_initialized] = .{ .conditions = try cloneExpressionConditionsConcatAlloc(alloc, left.conditions, right.conditions) };
                combined_initialized += 1;
            }
        }
        freeExpressionPredicateGroups(alloc, groups);
        alloc.free(groups);
        return combined;
    }

    return groups;
}

pub fn expressionGroupsFromSimpleUnionQueryAlloc(
    alloc: std.mem.Allocator,
    query: db_mod.types.RelationalRowsQueryRequest,
) ![]db_mod.types.RelationalRowsExpressionPredicateGroup {
    if (query.in_predicates.len > 0) return try expressionGroupsFromInSetQueryAlloc(alloc, query);
    if (query.expression_or_predicates.len > 0) return try cloneSimpleExpressionSetQueryBranchesAlloc(alloc, query);

    const groups = try alloc.alloc(db_mod.types.RelationalRowsExpressionPredicateGroup, 1);
    var initialized: usize = 0;
    errdefer {
        for (groups[0..initialized]) |group| freeExpressionPredicateGroup(alloc, group);
        alloc.free(groups);
    }
    groups[0] = .{ .conditions = try expressionConditionsFromSimpleSetQueryAlloc(alloc, query) };
    initialized += 1;
    return groups;
}

pub fn expressionConditionFromRelationalCheckAlloc(
    alloc: std.mem.Allocator,
    value: runtime_schema.RelationalCheck,
) !db_mod.types.RelationalRowsExpressionCondition {
    if (value.expression) |expression| return try cloneExpressionConditionAlloc(alloc, expression);

    const lhs: db_mod.types.RelationalRowsExpression = .{
        .kind = .field,
        .field = try alloc.dupe(u8, value.field),
    };
    var lhs_transferred = false;
    errdefer if (!lhs_transferred) freeExpression(alloc, lhs);

    const rhs = if (value.value_json) |json| blk: {
        const out = try alloc.alloc(db_mod.types.RelationalRowsExpression, 1);
        errdefer alloc.free(out);
        out[0] = .{
            .kind = .value,
            .value_json = try alloc.dupe(u8, json),
        };
        break :blk out;
    } else &.{};

    lhs_transferred = true;
    return .{
        .lhs = lhs,
        .op = value.op,
        .rhs = rhs,
    };
}

pub fn expressionConditionsFromSimpleSetQueryAlloc(
    alloc: std.mem.Allocator,
    query: db_mod.types.RelationalRowsQueryRequest,
) ![]const db_mod.types.RelationalRowsExpressionCondition {
    const len = query.predicates.len + query.expression_predicates.len;
    if (len == 0) return &.{};

    const out = try alloc.alloc(db_mod.types.RelationalRowsExpressionCondition, len);
    var initialized: usize = 0;
    errdefer {
        freeExpressionConditions(alloc, out[0..initialized]);
        alloc.free(out);
    }
    for (query.predicates) |predicate| {
        out[initialized] = try expressionConditionFromRelationalCheckAlloc(alloc, predicate);
        initialized += 1;
    }
    for (query.expression_predicates) |predicate| {
        out[initialized] = try cloneExpressionConditionAlloc(alloc, predicate);
        initialized += 1;
    }
    return out;
}

pub fn expressionConditionsFromRelationalChecksAlloc(
    alloc: std.mem.Allocator,
    checks: []const runtime_schema.RelationalCheck,
) ![]const db_mod.types.RelationalRowsExpressionCondition {
    if (checks.len == 0) return &.{};

    const out = try alloc.alloc(db_mod.types.RelationalRowsExpressionCondition, checks.len);
    var initialized: usize = 0;
    errdefer {
        freeExpressionConditions(alloc, out[0..initialized]);
        alloc.free(out);
    }
    for (checks) |check| {
        out[initialized] = try expressionConditionFromRelationalCheckAlloc(alloc, check);
        initialized += 1;
    }
    return out;
}

pub fn expressionConditionsFromSimpleExpressionSetQueryBranchAlloc(
    alloc: std.mem.Allocator,
    query: db_mod.types.RelationalRowsQueryRequest,
    index: usize,
) ![]const db_mod.types.RelationalRowsExpressionCondition {
    if (query.expression_or_predicates.len > 0) {
        if (index >= query.expression_or_predicates.len) return error.UnsupportedSqlShape;
        const base_conditions = try expressionConditionsFromSimpleSetQueryAlloc(alloc, query);
        defer {
            freeExpressionConditions(alloc, base_conditions);
            if (base_conditions.len > 0) alloc.free(base_conditions);
        }
        return try cloneExpressionConditionsConcatAlloc(alloc, base_conditions, query.expression_or_predicates[index].conditions);
    }
    if (query.or_predicates.len > 0) {
        if (index >= query.or_predicates.len) return error.UnsupportedSqlShape;
        return try expressionConditionsFromRelationalChecksAlloc(alloc, query.or_predicates[index].predicates);
    }
    if ((query.predicates.len > 0 or query.expression_predicates.len > 0) and index == 0) {
        return try expressionConditionsFromSimpleSetQueryAlloc(alloc, query);
    }
    return error.UnsupportedSqlShape;
}

pub fn simpleSelectProjectionsEqual(
    lhs: db_mod.types.RelationalRowsQueryRequest,
    rhs: db_mod.types.RelationalRowsQueryRequest,
    lhs_outputs: []const ast.SelectOutputRef,
    rhs_outputs: []const ast.SelectOutputRef,
) bool {
    if (lhs.select_all != rhs.select_all) return false;
    if (!stringSlicesEqual(lhs.select, rhs.select)) return false;
    if (!expressionProjectionsEqual(lhs.expressions, rhs.expressions)) return false;
    if (lhs_outputs.len != rhs_outputs.len) return false;
    for (lhs_outputs, rhs_outputs) |left, right| {
        if (left.kind != right.kind or left.index != right.index) return false;
    }
    return true;
}

pub fn parseQueryPlanAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    params: []const value_mod.SqlValue,
    cte_hooks: plan_mod.CteSelectParserHooks,
    query_hooks: QueryPlanParserHooks,
    tail_hooks: plan_mod.SimpleSelectSetTailHooks,
) !plan_mod.LoweredQueryPlan {
    if (!parser.peekKeyword(tokens, pos.*, "with")) {
        var lowered = try query_hooks.parse_select_with_set_boundary(query_hooks.ptr, true, &.{});
        errdefer lowered.deinit(alloc);
        if (!parser.atEnd(tokens, pos.*)) {
            const op = try grammar.parseSelectSetOperation(tokens, pos);
            var rhs = try query_hooks.parse_select_with_set_result_tail_boundary(query_hooks.ptr, &.{});
            defer rhs.deinit(alloc);
            try applySimpleSelectSetOperationAlloc(alloc, &lowered, rhs, op);
            try plan_mod.parseSimpleSelectSetResultTailAlloc(alloc, tokens, pos, params, &lowered, tail_hooks);
        }
        const table_name = lowered.table_name;
        lowered.table_name = "";
        lowered.clearSelectOutputs(alloc);
        return .{
            .table_name = table_name,
            .plan = .{ .query = lowered.query },
        };
    }

    var base_table_name: ?[]const u8 = null;
    defer if (base_table_name) |table| alloc.free(table);
    const ctes = try plan_mod.parseCtesForPlanAlloc(alloc, tokens, pos, &base_table_name, cte_hooks);
    var ctes_transferred = false;
    errdefer if (!ctes_transferred) plan_mod.freePlanCtes(alloc, ctes);

    var final = try query_hooks.parse_select_with_set_boundary(query_hooks.ptr, true, ctes);
    errdefer final.deinit(alloc);
    try plan_mod.resolveSelectSourceForPlanAlloc(alloc, &final, ctes, &base_table_name);
    if (!parser.atEnd(tokens, pos.*)) {
        const op = try grammar.parseSelectSetOperation(tokens, pos);
        var rhs = try query_hooks.parse_select_with_set_result_tail_boundary(query_hooks.ptr, ctes);
        defer rhs.deinit(alloc);
        try plan_mod.resolveSelectSourceForPlanAlloc(alloc, &rhs, ctes, &base_table_name);
        try applySimpleSelectSetOperationAlloc(alloc, &final, rhs, op);
        try plan_mod.parseSimpleSelectSetResultTailAlloc(alloc, tokens, pos, params, &final, tail_hooks);
    }
    if (parser.matchToken(tokens, pos, .semicolon) != null and !parser.atEnd(tokens, pos.*)) return error.UnsupportedSqlShape;
    if (!parser.atEnd(tokens, pos.*)) return error.UnsupportedSqlShape;

    const table_name = base_table_name orelse return error.UnsupportedSqlShape;
    base_table_name = null;
    alloc.free(final.table_name);
    final.table_name = "";
    final.clearSelectOutputs(alloc);
    ctes_transferred = true;
    return .{
        .table_name = table_name,
        .plan = .{
            .ctes = ctes,
            .query = final.query,
        },
    };
}

pub fn applySimpleSelectSetOperationAlloc(
    alloc: std.mem.Allocator,
    lhs: *plan_mod.LoweredSelect,
    rhs: plan_mod.LoweredSelect,
    op: ast.SelectSetOperation,
) !void {
    if (!std.mem.eql(u8, lhs.table_name, rhs.table_name)) return error.UnsupportedSqlShape;
    try validateSimpleSetOperationSelect(lhs.query, rhs.query, op);
    if (!simpleSelectProjectionsEqual(lhs.query, rhs.query, lhs.select_outputs, rhs.select_outputs)) return error.UnsupportedSqlShape;

    switch (op) {
        .union_distinct => try applySimpleUnion(alloc, lhs, rhs.query),
        .union_all => try applySimpleUnionAll(alloc, lhs, rhs.query),
        .intersect => try applySimpleIntersect(alloc, lhs, rhs.query),
        .except => try applySimpleExcept(alloc, lhs, rhs.query),
    }
}

fn clearSimpleSetUnionPredicates(alloc: std.mem.Allocator, query: *db_mod.types.RelationalRowsQueryRequest) void {
    freeRelationalChecks(alloc, query.predicates);
    if (query.predicates.len > 0) alloc.free(query.predicates);
    query.predicates = &.{};
    freePredicateGroups(alloc, query.or_predicates);
    if (query.or_predicates.len > 0) alloc.free(query.or_predicates);
    query.or_predicates = &.{};
    freeInPredicates(alloc, query.in_predicates);
    if (query.in_predicates.len > 0) alloc.free(query.in_predicates);
    query.in_predicates = &.{};
    freeAccessPredicateGroups(alloc, query.access_or_predicates);
    if (query.access_or_predicates.len > 0) alloc.free(query.access_or_predicates);
    query.access_or_predicates = &.{};
    freeExpressionConditions(alloc, query.expression_predicates);
    if (query.expression_predicates.len > 0) alloc.free(query.expression_predicates);
    query.expression_predicates = &.{};
    freeExpressionPredicateGroups(alloc, query.expression_or_predicates);
    if (query.expression_or_predicates.len > 0) alloc.free(query.expression_or_predicates);
    query.expression_or_predicates = &.{};
}

fn applySimpleUnion(
    alloc: std.mem.Allocator,
    lhs: *plan_mod.LoweredSelect,
    rhs: db_mod.types.RelationalRowsQueryRequest,
) !void {
    if (queryHasNoSimpleSetPredicates(lhs.query) or queryHasNoSimpleSetPredicates(rhs)) {
        clearSimpleSetUnionPredicates(alloc, &lhs.query);
        return;
    }
    if (lhs.query.expression_or_predicates.len > 0 or rhs.expression_or_predicates.len > 0 or
        ((lhs.query.in_predicates.len > 0 or rhs.in_predicates.len > 0) and
            (lhs.query.expression_predicates.len > 0 or rhs.expression_predicates.len > 0)))
    {
        return try applySimpleExpressionBranchUnion(alloc, lhs, rhs);
    }
    if (lhs.query.in_predicates.len > 0 or rhs.in_predicates.len > 0) {
        return try applySimpleAccessBranchUnion(alloc, lhs, rhs);
    }
    if (lhs.query.or_predicates.len > 0 or rhs.or_predicates.len > 0) {
        return try applySimpleScalarBranchUnion(alloc, lhs, rhs);
    }
    if (lhs.query.expression_predicates.len > 0 or rhs.expression_predicates.len > 0) {
        return try applySimpleExpressionUnion(alloc, lhs, rhs);
    }

    const groups = try alloc.alloc(db_mod.types.RelationalRowsPredicateGroup, 2);
    var initialized: usize = 0;
    errdefer {
        for (groups[0..initialized]) |group| freePredicateGroup(alloc, group);
        alloc.free(groups);
    }
    groups[0] = .{ .predicates = try cloneQueryRelationalChecksAlloc(alloc, lhs.query.predicates) };
    initialized += 1;
    groups[1] = .{ .predicates = try cloneQueryRelationalChecksAlloc(alloc, rhs.predicates) };
    initialized += 1;

    freeRelationalChecks(alloc, lhs.query.predicates);
    if (lhs.query.predicates.len > 0) alloc.free(lhs.query.predicates);
    lhs.query.predicates = &.{};
    freePredicateGroups(alloc, lhs.query.or_predicates);
    if (lhs.query.or_predicates.len > 0) alloc.free(lhs.query.or_predicates);
    lhs.query.or_predicates = groups;
}

fn applySimpleUnionAll(
    alloc: std.mem.Allocator,
    lhs: *plan_mod.LoweredSelect,
    rhs: db_mod.types.RelationalRowsQueryRequest,
) !void {
    if (!try simpleSetQueriesProvablyDisjoint(alloc, lhs.query, rhs)) return error.UnsupportedSqlShape;
    try applySimpleUnion(alloc, lhs, rhs);
}

fn applySimpleScalarBranchUnion(
    alloc: std.mem.Allocator,
    lhs: *plan_mod.LoweredSelect,
    rhs: db_mod.types.RelationalRowsQueryRequest,
) !void {
    const lhs_branch_count = simpleScalarSetQueryBranchCount(lhs.query);
    const rhs_branch_count = simpleScalarSetQueryBranchCount(rhs);
    if (lhs_branch_count == 0 or rhs_branch_count == 0) return error.UnsupportedSqlShape;

    const groups = try alloc.alloc(db_mod.types.RelationalRowsPredicateGroup, lhs_branch_count + rhs_branch_count);
    var initialized: usize = 0;
    errdefer {
        for (groups[0..initialized]) |group| freePredicateGroup(alloc, group);
        alloc.free(groups);
    }
    try cloneSimpleScalarSetQueryBranchesInto(alloc, groups, &initialized, lhs.query);
    try cloneSimpleScalarSetQueryBranchesInto(alloc, groups, &initialized, rhs);

    freeRelationalChecks(alloc, lhs.query.predicates);
    if (lhs.query.predicates.len > 0) alloc.free(lhs.query.predicates);
    lhs.query.predicates = &.{};
    freePredicateGroups(alloc, lhs.query.or_predicates);
    if (lhs.query.or_predicates.len > 0) alloc.free(lhs.query.or_predicates);
    lhs.query.or_predicates = groups;
}

fn applySimpleAccessBranchUnion(
    alloc: std.mem.Allocator,
    lhs: *plan_mod.LoweredSelect,
    rhs: db_mod.types.RelationalRowsQueryRequest,
) !void {
    const lhs_branch_count = simpleAccessSetQueryBranchCount(lhs.query);
    const rhs_branch_count = simpleAccessSetQueryBranchCount(rhs);
    if (lhs_branch_count == 0 or rhs_branch_count == 0) return error.UnsupportedSqlShape;

    const groups = try alloc.alloc(db_mod.types.RelationalRowsAccessPredicateGroup, lhs_branch_count + rhs_branch_count);
    var initialized: usize = 0;
    errdefer {
        for (groups[0..initialized]) |group| freeAccessPredicateGroup(alloc, group);
        alloc.free(groups);
    }
    try cloneSimpleAccessSetQueryBranchesInto(alloc, groups, &initialized, lhs.query);
    try cloneSimpleAccessSetQueryBranchesInto(alloc, groups, &initialized, rhs);

    freeRelationalChecks(alloc, lhs.query.predicates);
    if (lhs.query.predicates.len > 0) alloc.free(lhs.query.predicates);
    lhs.query.predicates = &.{};
    freePredicateGroups(alloc, lhs.query.or_predicates);
    if (lhs.query.or_predicates.len > 0) alloc.free(lhs.query.or_predicates);
    lhs.query.or_predicates = &.{};
    freeInPredicates(alloc, lhs.query.in_predicates);
    if (lhs.query.in_predicates.len > 0) alloc.free(lhs.query.in_predicates);
    lhs.query.in_predicates = &.{};
    freeAccessPredicateGroups(alloc, lhs.query.access_or_predicates);
    if (lhs.query.access_or_predicates.len > 0) alloc.free(lhs.query.access_or_predicates);
    lhs.query.access_or_predicates = groups;
}

fn applySimpleExpressionUnion(
    alloc: std.mem.Allocator,
    lhs: *plan_mod.LoweredSelect,
    rhs: db_mod.types.RelationalRowsQueryRequest,
) !void {
    const groups = try alloc.alloc(db_mod.types.RelationalRowsExpressionPredicateGroup, 2);
    var initialized: usize = 0;
    errdefer {
        for (groups[0..initialized]) |group| freeExpressionPredicateGroup(alloc, group);
        alloc.free(groups);
    }
    groups[0] = .{ .conditions = try expressionConditionsFromSimpleSetQueryAlloc(alloc, lhs.query) };
    initialized += 1;
    groups[1] = .{ .conditions = try expressionConditionsFromSimpleSetQueryAlloc(alloc, rhs) };
    initialized += 1;

    freeRelationalChecks(alloc, lhs.query.predicates);
    if (lhs.query.predicates.len > 0) alloc.free(lhs.query.predicates);
    lhs.query.predicates = &.{};
    freeExpressionConditions(alloc, lhs.query.expression_predicates);
    if (lhs.query.expression_predicates.len > 0) alloc.free(lhs.query.expression_predicates);
    lhs.query.expression_predicates = &.{};
    freeExpressionPredicateGroups(alloc, lhs.query.expression_or_predicates);
    if (lhs.query.expression_or_predicates.len > 0) alloc.free(lhs.query.expression_or_predicates);
    lhs.query.expression_or_predicates = groups;
}

fn applySimpleExpressionBranchUnion(
    alloc: std.mem.Allocator,
    lhs: *plan_mod.LoweredSelect,
    rhs: db_mod.types.RelationalRowsQueryRequest,
) !void {
    const left_groups = try expressionGroupsFromSimpleUnionQueryAlloc(alloc, lhs.query);
    defer {
        freeExpressionPredicateGroups(alloc, left_groups);
        if (left_groups.len > 0) alloc.free(left_groups);
    }
    const right_groups = try expressionGroupsFromSimpleUnionQueryAlloc(alloc, rhs);
    defer {
        freeExpressionPredicateGroups(alloc, right_groups);
        if (right_groups.len > 0) alloc.free(right_groups);
    }

    const groups = try alloc.alloc(db_mod.types.RelationalRowsExpressionPredicateGroup, left_groups.len + right_groups.len);
    var initialized: usize = 0;
    errdefer {
        for (groups[0..initialized]) |group| freeExpressionPredicateGroup(alloc, group);
        alloc.free(groups);
    }
    for (left_groups) |group| {
        groups[initialized] = .{ .conditions = try cloneExpressionConditionsAlloc(alloc, group.conditions) };
        initialized += 1;
    }
    for (right_groups) |group| {
        groups[initialized] = .{ .conditions = try cloneExpressionConditionsAlloc(alloc, group.conditions) };
        initialized += 1;
    }

    replaceQueryWithExpressionOrBranches(alloc, lhs, groups);
}

fn applySimpleIntersect(
    alloc: std.mem.Allocator,
    lhs: *plan_mod.LoweredSelect,
    rhs: db_mod.types.RelationalRowsQueryRequest,
) !void {
    if (lhs.query.expression_or_predicates.len > 0 or rhs.expression_or_predicates.len > 0) {
        return try applySimpleExpressionBranchIntersect(alloc, lhs, rhs);
    }
    if (lhs.query.or_predicates.len > 0 or rhs.or_predicates.len > 0) {
        return try applySimpleScalarBranchIntersect(alloc, lhs, rhs);
    }
    if (rhs.predicates.len > 0) {
        const predicates = try cloneQueryRelationalChecksConcatAlloc(alloc, lhs.query.predicates, rhs.predicates);
        freeRelationalChecks(alloc, lhs.query.predicates);
        if (lhs.query.predicates.len > 0) alloc.free(lhs.query.predicates);
        lhs.query.predicates = predicates;
    }
    if (rhs.in_predicates.len > 0) {
        const in_predicates = try cloneInPredicatesConcatAlloc(alloc, lhs.query.in_predicates, rhs.in_predicates);
        freeInPredicates(alloc, lhs.query.in_predicates);
        if (lhs.query.in_predicates.len > 0) alloc.free(lhs.query.in_predicates);
        lhs.query.in_predicates = in_predicates;
    }
    if (rhs.expression_predicates.len > 0) {
        const expression_predicates = try cloneExpressionConditionsConcatAlloc(alloc, lhs.query.expression_predicates, rhs.expression_predicates);
        freeExpressionConditions(alloc, lhs.query.expression_predicates);
        if (lhs.query.expression_predicates.len > 0) alloc.free(lhs.query.expression_predicates);
        lhs.query.expression_predicates = expression_predicates;
    }
}

fn applySimpleScalarBranchIntersect(
    alloc: std.mem.Allocator,
    lhs: *plan_mod.LoweredSelect,
    rhs: db_mod.types.RelationalRowsQueryRequest,
) !void {
    const lhs_branch_count = simpleScalarSetQueryBranchCount(lhs.query);
    const rhs_branch_count = simpleScalarSetQueryBranchCount(rhs);
    if (rhs_branch_count == 0) return;
    if (lhs_branch_count == 0) {
        const groups = try cloneSimpleScalarSetQueryBranchesAlloc(alloc, rhs);
        freeRelationalChecks(alloc, lhs.query.predicates);
        if (lhs.query.predicates.len > 0) alloc.free(lhs.query.predicates);
        lhs.query.predicates = &.{};
        freePredicateGroups(alloc, lhs.query.or_predicates);
        if (lhs.query.or_predicates.len > 0) alloc.free(lhs.query.or_predicates);
        lhs.query.or_predicates = groups;
        return;
    }
    if (lhs_branch_count > max_scalar_or_expanded_branches / rhs_branch_count) return error.UnsupportedSqlShape;

    const groups = try alloc.alloc(db_mod.types.RelationalRowsPredicateGroup, lhs_branch_count * rhs_branch_count);
    var initialized: usize = 0;
    errdefer {
        for (groups[0..initialized]) |group| freePredicateGroup(alloc, group);
        alloc.free(groups);
    }
    for (0..lhs_branch_count) |left_index| {
        const left = simpleScalarSetQueryBranchAt(lhs.query, left_index) orelse return error.UnsupportedSqlShape;
        for (0..rhs_branch_count) |right_index| {
            const right = simpleScalarSetQueryBranchAt(rhs, right_index) orelse return error.UnsupportedSqlShape;
            groups[initialized] = .{ .predicates = try cloneQueryRelationalChecksConcatAlloc(alloc, left, right) };
            initialized += 1;
        }
    }

    freeRelationalChecks(alloc, lhs.query.predicates);
    if (lhs.query.predicates.len > 0) alloc.free(lhs.query.predicates);
    lhs.query.predicates = &.{};
    freePredicateGroups(alloc, lhs.query.or_predicates);
    if (lhs.query.or_predicates.len > 0) alloc.free(lhs.query.or_predicates);
    lhs.query.or_predicates = groups;
}

fn applySimpleExpressionBranchIntersect(
    alloc: std.mem.Allocator,
    lhs: *plan_mod.LoweredSelect,
    rhs: db_mod.types.RelationalRowsQueryRequest,
) !void {
    const rhs_groups = try expressionGroupsFromSimpleIntersectQueryAlloc(alloc, rhs);
    defer {
        freeExpressionPredicateGroups(alloc, rhs_groups);
        if (rhs_groups.len > 0) alloc.free(rhs_groups);
    }
    if (rhs_groups.len == 0) return;

    const lhs_groups = try expressionGroupsFromSimpleIntersectQueryAlloc(alloc, lhs.query);
    defer {
        freeExpressionPredicateGroups(alloc, lhs_groups);
        if (lhs_groups.len > 0) alloc.free(lhs_groups);
    }
    if (lhs_groups.len == 0) {
        const groups = try cloneExpressionPredicateGroupsAlloc(alloc, rhs_groups);
        replaceQueryWithExpressionOrBranches(alloc, lhs, groups);
        return;
    }
    if (lhs_groups.len > max_scalar_or_expanded_branches / rhs_groups.len) return error.UnsupportedSqlShape;

    const groups = try alloc.alloc(db_mod.types.RelationalRowsExpressionPredicateGroup, lhs_groups.len * rhs_groups.len);
    var initialized: usize = 0;
    errdefer {
        for (groups[0..initialized]) |group| freeExpressionPredicateGroup(alloc, group);
        alloc.free(groups);
    }
    for (lhs_groups) |left| {
        for (rhs_groups) |right| {
            groups[initialized] = .{ .conditions = try cloneExpressionConditionsConcatAlloc(alloc, left.conditions, right.conditions) };
            initialized += 1;
        }
    }

    replaceQueryWithExpressionOrBranches(alloc, lhs, groups);
}

fn replaceQueryWithExpressionOrBranches(
    alloc: std.mem.Allocator,
    lhs: *plan_mod.LoweredSelect,
    groups: []db_mod.types.RelationalRowsExpressionPredicateGroup,
) void {
    freeRelationalChecks(alloc, lhs.query.predicates);
    if (lhs.query.predicates.len > 0) alloc.free(lhs.query.predicates);
    lhs.query.predicates = &.{};
    freePredicateGroups(alloc, lhs.query.or_predicates);
    if (lhs.query.or_predicates.len > 0) alloc.free(lhs.query.or_predicates);
    lhs.query.or_predicates = &.{};
    freeInPredicates(alloc, lhs.query.in_predicates);
    if (lhs.query.in_predicates.len > 0) alloc.free(lhs.query.in_predicates);
    lhs.query.in_predicates = &.{};
    freeAccessPredicateGroups(alloc, lhs.query.access_or_predicates);
    if (lhs.query.access_or_predicates.len > 0) alloc.free(lhs.query.access_or_predicates);
    lhs.query.access_or_predicates = &.{};
    freeExpressionConditions(alloc, lhs.query.expression_predicates);
    if (lhs.query.expression_predicates.len > 0) alloc.free(lhs.query.expression_predicates);
    lhs.query.expression_predicates = &.{};
    freeExpressionPredicateGroups(alloc, lhs.query.expression_or_predicates);
    if (lhs.query.expression_or_predicates.len > 0) alloc.free(lhs.query.expression_or_predicates);
    lhs.query.expression_or_predicates = groups;
}

fn applySimpleExcept(
    alloc: std.mem.Allocator,
    lhs: *plan_mod.LoweredSelect,
    rhs: db_mod.types.RelationalRowsQueryRequest,
) !void {
    if (rhs.predicates.len == 0 and rhs.or_predicates.len == 0 and rhs.in_predicates.len == 0 and rhs.expression_predicates.len == 0 and rhs.expression_or_predicates.len == 0) return error.UnsupportedSqlShape;
    if (rhs.or_predicates.len > 0) {
        const groups = try cloneSimpleScalarSetQueryBranchesAlloc(alloc, rhs);
        freePredicateGroups(alloc, lhs.query.not_predicates);
        if (lhs.query.not_predicates.len > 0) alloc.free(lhs.query.not_predicates);
        lhs.query.not_predicates = groups;
        return;
    }
    if (rhs.in_predicates.len > 0 and rhs.expression_or_predicates.len > 0) {
        const groups = try expressionGroupsFromInSetQueryAlloc(alloc, rhs);
        freeExpressionPredicateGroups(alloc, lhs.query.expression_not_predicates);
        if (lhs.query.expression_not_predicates.len > 0) alloc.free(lhs.query.expression_not_predicates);
        lhs.query.expression_not_predicates = groups;
        return;
    }
    if (rhs.expression_or_predicates.len > 0) {
        const groups = try cloneSimpleExpressionSetQueryBranchesAlloc(alloc, rhs);
        freeExpressionPredicateGroups(alloc, lhs.query.expression_not_predicates);
        if (lhs.query.expression_not_predicates.len > 0) alloc.free(lhs.query.expression_not_predicates);
        lhs.query.expression_not_predicates = groups;
        return;
    }
    if (rhs.in_predicates.len > 0 and rhs.expression_predicates.len > 0) {
        const groups = try expressionGroupsFromInSetQueryAlloc(alloc, rhs);
        freeExpressionPredicateGroups(alloc, lhs.query.expression_not_predicates);
        if (lhs.query.expression_not_predicates.len > 0) alloc.free(lhs.query.expression_not_predicates);
        lhs.query.expression_not_predicates = groups;
        return;
    }
    if (rhs.expression_predicates.len > 0) {
        const groups = try alloc.alloc(db_mod.types.RelationalRowsExpressionPredicateGroup, 1);
        var initialized: usize = 0;
        errdefer {
            for (groups[0..initialized]) |group| freeExpressionPredicateGroup(alloc, group);
            alloc.free(groups);
        }
        groups[0] = .{ .conditions = try expressionConditionsFromSimpleSetQueryAlloc(alloc, rhs) };
        initialized += 1;
        freeExpressionPredicateGroups(alloc, lhs.query.expression_not_predicates);
        if (lhs.query.expression_not_predicates.len > 0) alloc.free(lhs.query.expression_not_predicates);
        lhs.query.expression_not_predicates = groups;
        return;
    }
    if (rhs.in_predicates.len > 0) {
        const groups = try alloc.alloc(db_mod.types.RelationalRowsAccessPredicateGroup, 1);
        var initialized: usize = 0;
        errdefer {
            for (groups[0..initialized]) |group| freeAccessPredicateGroup(alloc, group);
            alloc.free(groups);
        }
        const predicates = try cloneQueryRelationalChecksAlloc(alloc, rhs.predicates);
        var predicates_transferred = false;
        errdefer if (!predicates_transferred) {
            freeRelationalChecks(alloc, predicates);
            if (predicates.len > 0) alloc.free(predicates);
        };
        const in_predicates = try cloneInPredicatesAlloc(alloc, rhs.in_predicates);
        var in_transferred = false;
        errdefer if (!in_transferred) {
            freeInPredicates(alloc, in_predicates);
            if (in_predicates.len > 0) alloc.free(in_predicates);
        };
        groups[0] = .{ .predicates = predicates, .in_predicates = in_predicates };
        predicates_transferred = true;
        in_transferred = true;
        initialized += 1;
        freeAccessPredicateGroups(alloc, lhs.query.access_not_predicates);
        if (lhs.query.access_not_predicates.len > 0) alloc.free(lhs.query.access_not_predicates);
        lhs.query.access_not_predicates = groups;
        return;
    }
    const groups = try alloc.alloc(db_mod.types.RelationalRowsPredicateGroup, 1);
    var initialized: usize = 0;
    errdefer {
        for (groups[0..initialized]) |group| freePredicateGroup(alloc, group);
        alloc.free(groups);
    }
    groups[0] = .{ .predicates = try cloneQueryRelationalChecksAlloc(alloc, rhs.predicates) };
    initialized += 1;
    freePredicateGroups(alloc, lhs.query.not_predicates);
    if (lhs.query.not_predicates.len > 0) alloc.free(lhs.query.not_predicates);
    lhs.query.not_predicates = groups;
}

pub fn setOperationColumnsCompatible(
    lhs: []const runtime_schema.RelationalColumn,
    rhs: []const runtime_schema.RelationalColumn,
) bool {
    if (lhs.len == 0 or lhs.len != rhs.len) return false;
    for (lhs, rhs) |left, right| {
        if (!std.mem.eql(u8, left.name, right.name)) return false;
        if (left.field_type != right.field_type) return false;
        if (left.array_item_type != right.array_item_type) return false;
    }
    return true;
}

pub fn expressionProjectionsEqual(
    lhs: []const db_mod.types.RelationalRowsExpressionProjection,
    rhs: []const db_mod.types.RelationalRowsExpressionProjection,
) bool {
    if (lhs.len != rhs.len) return false;
    for (lhs, rhs) |left, right| {
        if (!std.mem.eql(u8, left.output, right.output)) return false;
        if (!relationalRowsExpressionEqual(left.expression, right.expression)) return false;
    }
    return true;
}

pub fn relationalChecksProvablyDisjoint(
    lhs: runtime_schema.RelationalCheck,
    rhs: runtime_schema.RelationalCheck,
) bool {
    if (lhs.expression) |left| {
        if (rhs.expression) |right| return expressionConditionsProvablyDisjoint(left, right);
        return relationalCheckAndExpressionConditionProvablyDisjoint(rhs, left);
    }
    if (rhs.expression) |right| return relationalCheckAndExpressionConditionProvablyDisjoint(lhs, right);
    if (!std.mem.eql(u8, lhs.field, rhs.field)) return false;
    return simplePredicateOpsProvablyDisjoint(lhs.op, lhs.value_json, rhs.op, rhs.value_json);
}

pub fn relationalCheckAndExpressionConditionProvablyDisjoint(
    check: runtime_schema.RelationalCheck,
    condition: db_mod.types.RelationalRowsExpressionCondition,
) bool {
    if (check.expression) |check_condition| return expressionConditionsProvablyDisjoint(check_condition, condition);
    if (!expressionIsRowField(condition.lhs, check.field)) return false;
    return simplePredicateOpsProvablyDisjoint(check.op, check.value_json, condition.op, expressionConditionSingleValueJson(condition));
}

pub fn expressionConditionsProvablyDisjoint(
    lhs: db_mod.types.RelationalRowsExpressionCondition,
    rhs: db_mod.types.RelationalRowsExpressionCondition,
) bool {
    if (!relationalRowsExpressionEqual(lhs.lhs, rhs.lhs)) return false;
    return simplePredicateOpsProvablyDisjoint(lhs.op, expressionConditionSingleValueJson(lhs), rhs.op, expressionConditionSingleValueJson(rhs));
}

fn expressionIsRowField(expression: db_mod.types.RelationalRowsExpression, field: []const u8) bool {
    return expression.kind == .field and
        expression.field_source == .row and
        std.mem.eql(u8, expression.field, field);
}

fn expressionConditionSingleValueJson(condition: db_mod.types.RelationalRowsExpressionCondition) ?[]const u8 {
    if (condition.rhs.len != 1) return null;
    if (condition.rhs[0].kind != .value) return null;
    return condition.rhs[0].value_json;
}

fn simplePredicateOpsProvablyDisjoint(
    lhs_op: runtime_schema.RelationalCheckOp,
    lhs_value_json: ?[]const u8,
    rhs_op: runtime_schema.RelationalCheckOp,
    rhs_value_json: ?[]const u8,
) bool {
    if (lhs_op == .eq and rhs_op == .eq) return jsonScalarLiteralsDefinitelyDistinct(lhs_value_json, rhs_value_json);
    if (jsonScalarLiteralsExactlyEqualForComplement(lhs_value_json, rhs_value_json)) {
        if ((lhs_op == .eq and rhs_op == .ne) or (lhs_op == .ne and rhs_op == .eq)) return true;
        if ((lhs_op == .eq and rhs_op == .is_distinct) or (lhs_op == .is_distinct and rhs_op == .eq)) return true;
        if ((lhs_op == .is_not_distinct and rhs_op == .is_distinct) or (lhs_op == .is_distinct and rhs_op == .is_not_distinct)) return true;
        if (jsonScalarLiteralDefinitelyNonNull(lhs_value_json) and
            ((lhs_op == .is_not_distinct and rhs_op == .ne) or (lhs_op == .ne and rhs_op == .is_not_distinct)))
        {
            return true;
        }
    }
    if (lhs_op == .is_null and rhs_op == .is_not_null) return true;
    if (lhs_op == .is_not_null and rhs_op == .is_null) return true;
    if (lhs_op == .is_null and rhs_op == .eq) return jsonScalarLiteralDefinitelyNonNull(rhs_value_json);
    if (lhs_op == .eq and rhs_op == .is_null) return jsonScalarLiteralDefinitelyNonNull(lhs_value_json);
    if (numericRangePredicatesProvablyDisjoint(lhs_op, lhs_value_json, rhs_op, rhs_value_json)) return true;
    return false;
}

const SimpleNumericBound = struct {
    value: f64,
    inclusive: bool,
};

fn numericRangePredicatesProvablyDisjoint(
    lhs_op: runtime_schema.RelationalCheckOp,
    lhs_value_json: ?[]const u8,
    rhs_op: runtime_schema.RelationalCheckOp,
    rhs_value_json: ?[]const u8,
) bool {
    if (upperNumericBound(lhs_op, lhs_value_json)) |left_upper| {
        if (lowerNumericBound(rhs_op, rhs_value_json)) |right_lower| {
            return numericUpperLowerBoundsProvablyDisjoint(left_upper, right_lower);
        }
    }
    if (lowerNumericBound(lhs_op, lhs_value_json)) |left_lower| {
        if (upperNumericBound(rhs_op, rhs_value_json)) |right_upper| {
            return numericUpperLowerBoundsProvablyDisjoint(right_upper, left_lower);
        }
    }
    return false;
}

fn upperNumericBound(op: runtime_schema.RelationalCheckOp, value_json: ?[]const u8) ?SimpleNumericBound {
    return switch (op) {
        .lt => .{ .value = jsonNumberLiteralValue(value_json) orelse return null, .inclusive = false },
        .lte => .{ .value = jsonNumberLiteralValue(value_json) orelse return null, .inclusive = true },
        else => null,
    };
}

fn lowerNumericBound(op: runtime_schema.RelationalCheckOp, value_json: ?[]const u8) ?SimpleNumericBound {
    return switch (op) {
        .gt => .{ .value = jsonNumberLiteralValue(value_json) orelse return null, .inclusive = false },
        .gte => .{ .value = jsonNumberLiteralValue(value_json) orelse return null, .inclusive = true },
        else => null,
    };
}

fn numericUpperLowerBoundsProvablyDisjoint(upper: SimpleNumericBound, lower: SimpleNumericBound) bool {
    if (lower.value > upper.value) return true;
    if (lower.value < upper.value) return false;
    return !(upper.inclusive and lower.inclusive);
}

fn jsonScalarLiteralsDefinitelyDistinct(lhs: ?[]const u8, rhs: ?[]const u8) bool {
    const left = lhs orelse return false;
    const right = rhs orelse return false;
    if (!jsonIsSafeDisjointProofLiteral(left) or !jsonIsSafeDisjointProofLiteral(right)) return false;
    return !std.mem.eql(u8, left, right);
}

fn jsonScalarLiteralsDefinitelyEqual(lhs: ?[]const u8, rhs: ?[]const u8) bool {
    const left = lhs orelse return false;
    const right = rhs orelse return false;
    if (!jsonIsSafeDisjointProofLiteral(left) or !jsonIsSafeDisjointProofLiteral(right)) return false;
    return std.mem.eql(u8, left, right);
}

fn jsonScalarLiteralsExactlyEqualForComplement(lhs: ?[]const u8, rhs: ?[]const u8) bool {
    const left = lhs orelse return false;
    const right = rhs orelse return false;
    if (!std.mem.eql(u8, left, right)) return false;
    return jsonIsSafeDisjointProofLiteral(left) or jsonIsJsonNumberLiteral(left);
}

pub fn jsonIsSafeDisjointProofLiteral(value: []const u8) bool {
    return (value.len >= 2 and value[0] == '"' and value[value.len - 1] == '"') or
        std.mem.eql(u8, value, "true") or
        std.mem.eql(u8, value, "false") or
        std.mem.eql(u8, value, "null");
}

fn jsonScalarLiteralDefinitelyNonNull(value: ?[]const u8) bool {
    const json = value orelse return false;
    return (jsonIsSafeDisjointProofLiteral(json) and !std.mem.eql(u8, json, "null")) or jsonIsJsonNumberLiteral(json);
}

pub fn jsonIsJsonNumberLiteral(value: []const u8) bool {
    if (value.len == 0) return false;
    var index: usize = 0;
    if (value[index] == '-') {
        index += 1;
        if (index == value.len) return false;
    }

    if (value[index] == '0') {
        index += 1;
    } else if (std.ascii.isDigit(value[index]) and value[index] != '0') {
        while (index < value.len and std.ascii.isDigit(value[index])) index += 1;
    } else {
        return false;
    }

    if (index < value.len and value[index] == '.') {
        index += 1;
        const start = index;
        while (index < value.len and std.ascii.isDigit(value[index])) index += 1;
        if (index == start) return false;
    }

    if (index < value.len and (value[index] == 'e' or value[index] == 'E')) {
        index += 1;
        if (index < value.len and (value[index] == '+' or value[index] == '-')) index += 1;
        const start = index;
        while (index < value.len and std.ascii.isDigit(value[index])) index += 1;
        if (index == start) return false;
    }

    return index == value.len;
}

fn jsonNumberLiteralValue(value: ?[]const u8) ?f64 {
    const json = value orelse return null;
    if (!jsonIsJsonNumberLiteral(json)) return null;
    const parsed = std.fmt.parseFloat(f64, json) catch return null;
    if (!std.math.isFinite(parsed)) return null;
    return parsed;
}

fn stringSlicesEqual(a: []const []const u8, b: []const []const u8) bool {
    if (a.len != b.len) return false;
    for (a, b) |left, right| {
        if (!std.mem.eql(u8, left, right)) return false;
    }
    return true;
}

fn stringSlicesContains(values: []const []const u8, value: []const u8) bool {
    for (values) |candidate| {
        if (std.mem.eql(u8, candidate, value)) return true;
    }
    return false;
}

fn stringSlicesIntersect(a: []const []const u8, b: []const []const u8) bool {
    for (a) |value| {
        if (stringSlicesContains(b, value)) return true;
    }
    return false;
}

pub fn validateSqlUniqueExpressionListUnique(expressions: []const runtime_schema.UniqueExpression) !void {
    for (expressions, 0..) |lhs, i| {
        for (expressions[i + 1 ..]) |rhs| {
            if (lhs.op != rhs.op) continue;
            if (lhs.op == .expression) {
                if (lhs.expression != null and rhs.expression != null and relationalRowsExpressionEqual(lhs.expression.?, rhs.expression.?)) return error.UnsupportedSqlShape;
                continue;
            }
            if (std.ascii.eqlIgnoreCase(lhs.field, rhs.field)) return error.UnsupportedSqlShape;
        }
    }
}

pub fn validateExtensionFunctionArity(
    kind: runtime_schema.RelationalRowsExpressionKind,
    declared_arity: u16,
    operand_count: usize,
) !void {
    if (operand_count != @as(usize, declared_arity)) return error.UnsupportedSqlShape;
    switch (kind) {
        .uuid_v4 => if (operand_count != 0) return error.UnsupportedSqlShape,
        .lower, .upper, .md5 => if (operand_count != 1) return error.UnsupportedSqlShape,
        .concat => if (operand_count == 0) return error.UnsupportedSqlShape,
        .concat_ws => if (operand_count < 2) return error.UnsupportedSqlShape,
        else => return error.UnsupportedSqlShape,
    }
}

pub fn sqlJoinedSourceAliasTerminator(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "where") or
        std.ascii.eqlIgnoreCase(text, "returning") or
        std.ascii.eqlIgnoreCase(text, "order") or
        std.ascii.eqlIgnoreCase(text, "limit") or
        std.ascii.eqlIgnoreCase(text, "offset") or
        std.ascii.eqlIgnoreCase(text, "for");
}

pub fn sqlAssignmentTailKeyword(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "where") or
        std.ascii.eqlIgnoreCase(text, "from") or
        std.ascii.eqlIgnoreCase(text, "returning") or
        std.ascii.eqlIgnoreCase(text, "order") or
        std.ascii.eqlIgnoreCase(text, "limit") or
        std.ascii.eqlIgnoreCase(text, "offset") or
        std.ascii.eqlIgnoreCase(text, "for");
}

pub fn sqlKeywordIsLengthFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "length") or
        std.ascii.eqlIgnoreCase(text, "char_length") or
        std.ascii.eqlIgnoreCase(text, "character_length");
}

pub fn sqlKeywordIsOctetLengthFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "octet_length");
}

pub fn sqlKeywordIsBitLengthFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "bit_length");
}

pub fn sqlKeywordIsJsonArrayLengthFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "json_array_length") or
        std.ascii.eqlIgnoreCase(text, "jsonb_array_length");
}

pub fn sqlKeywordIsCardinalityFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "cardinality");
}

pub fn sqlKeywordIsArrayLengthFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "array_length") or
        sqlKeywordIsCardinalityFunction(text);
}

pub fn sqlKeywordIsArrayPositionFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "array_position") or
        std.ascii.eqlIgnoreCase(text, "array_positions");
}

pub fn sqlKeywordIsArrayToStringFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "array_to_string");
}

pub fn arrayLengthDefaultOutput(keyword: []const u8) []const u8 {
    if (sqlKeywordIsCardinalityFunction(keyword)) return "cardinality";
    return "array_length";
}

pub fn sqlKeywordIsJsonTypeofFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "json_typeof") or
        std.ascii.eqlIgnoreCase(text, "jsonb_typeof");
}

pub fn sqlKeywordIsJsonExtractPathFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "json_extract_path") or
        std.ascii.eqlIgnoreCase(text, "json_extract_path_text") or
        std.ascii.eqlIgnoreCase(text, "jsonb_extract_path") or
        std.ascii.eqlIgnoreCase(text, "jsonb_extract_path_text");
}

pub fn sqlKeywordIsJsonBuildObjectFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "json_build_object") or
        std.ascii.eqlIgnoreCase(text, "jsonb_build_object");
}

pub fn sqlJsonExtractPathFunctionAsText(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "json_extract_path_text") or
        std.ascii.eqlIgnoreCase(text, "jsonb_extract_path_text");
}

pub fn sqlKeywordIsAsciiFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "ascii");
}

pub fn sqlKeywordIsChrFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "chr");
}

pub fn sqlKeywordIsSubstringFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "substring") or
        std.ascii.eqlIgnoreCase(text, "substr");
}

pub fn sqlKeywordIsOverlayFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "overlay");
}

pub fn sqlKeywordIsTranslateFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "translate");
}

pub fn sqlKeywordIsSplitPartFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "split_part");
}

pub fn sqlKeywordIsStrposFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "strpos");
}

pub fn sqlKeywordIsLeftRightFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "left") or
        std.ascii.eqlIgnoreCase(text, "right");
}

pub fn sqlKeywordIsPadFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "lpad") or
        std.ascii.eqlIgnoreCase(text, "rpad");
}

pub fn sqlKeywordIsRepeatFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "repeat");
}

pub fn sqlKeywordIsReverseFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "reverse");
}

pub fn sqlKeywordIsInitcapFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "initcap");
}

pub fn sqlKeywordIsMd5Function(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "md5");
}

pub fn sqlKeywordIsStartsWithFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "starts_with");
}

pub fn sqlKeywordIsEndsWithFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "ends_with");
}

pub fn sqlKeywordIsDateTruncFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "date_trunc");
}

pub fn sqlKeywordIsDateBinFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "date_bin");
}

pub fn sqlKeywordIsDatePartFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "date_part") or
        std.ascii.eqlIgnoreCase(text, "extract");
}

pub fn sqlKeywordIsTrimVariantFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "btrim") or
        std.ascii.eqlIgnoreCase(text, "ltrim") or
        std.ascii.eqlIgnoreCase(text, "rtrim");
}

pub fn sqlKeywordIsUuidV4Function(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "gen_random_uuid") or
        std.ascii.eqlIgnoreCase(text, "uuid_generate_v4");
}

pub fn sqlKeywordIsRegexpMatchFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "regexp_match") or
        std.ascii.eqlIgnoreCase(text, "regexp_like");
}

pub fn sqlKeywordIsRegexpCountFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "regexp_count");
}

pub fn sqlKeywordIsRegexpSubstrFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "regexp_substr");
}

pub fn sqlKeywordIsRegexpInstrFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "regexp_instr");
}

pub fn matchAnyOrSomeKeyword(tokens: []const Token, pos: *usize) bool {
    if (pos.* >= tokens.len) return false;
    const token = tokens[pos.*];
    if (token.kind != .identifier or !sqlKeywordIsAnyOrSome(token.text)) return false;
    pos.* += 1;
    return true;
}

pub fn tokenAtIsAnySomeOrAll(tokens: []const Token, index: usize) bool {
    if (index >= tokens.len) return false;
    const token = tokens[index];
    return token.kind == .identifier and
        (sqlKeywordIsAnyOrSome(token.text) or std.ascii.eqlIgnoreCase(token.text, "all"));
}

pub fn matchAnySomeOrAllKeyword(tokens: []const Token, pos: *usize) ?ast.SqlPatternQuantifier {
    if (pos.* >= tokens.len) return null;
    const token = tokens[pos.*];
    if (token.kind != .identifier) return null;
    if (sqlKeywordIsAnyOrSome(token.text)) {
        pos.* += 1;
        return .any;
    }
    if (std.ascii.eqlIgnoreCase(token.text, "all")) {
        pos.* += 1;
        return .all;
    }
    return null;
}

pub fn matchBetweenSymmetricMode(tokens: []const Token, pos: *usize) bool {
    const cursor = parser.Cursor.init(tokens, pos);
    if (cursor.matchKeyword("symmetric")) return true;
    _ = cursor.matchKeyword("asymmetric");
    return false;
}

pub fn matchTextLengthFunctionKind(tokens: []const Token, pos: *usize) ?db_mod.types.RelationalRowsExpressionKind {
    if (pos.* >= tokens.len) return null;
    const token = tokens[pos.*];
    if (token.kind != .identifier) return null;
    const kind: db_mod.types.RelationalRowsExpressionKind = if (sqlKeywordIsLengthFunction(token.text))
        .length
    else if (sqlKeywordIsOctetLengthFunction(token.text))
        .octet_length
    else if (sqlKeywordIsBitLengthFunction(token.text))
        .bit_length
    else
        return null;
    pos.* += 1;
    return kind;
}

pub fn parseTextLengthFunctionCallStart(tokens: []const Token, pos: *usize) !db_mod.types.RelationalRowsExpressionKind {
    const kind = matchTextLengthFunctionKind(tokens, pos) orelse return error.UnsupportedSqlShape;
    try parser.expectToken(tokens, pos, .lparen);
    return kind;
}

pub fn parseFixedUnaryFunctionCallStart(
    tokens: []const Token,
    pos: *usize,
    kind: db_mod.types.RelationalRowsExpressionKind,
) !void {
    const keyword = fixedUnaryFunctionKeyword(kind) orelse return error.UnsupportedSqlShape;
    try parser.expectKeyword(tokens, pos, keyword);
    try parser.expectToken(tokens, pos, .lparen);
}

pub fn parseFixedBinaryFunctionCallStart(
    tokens: []const Token,
    pos: *usize,
    kind: db_mod.types.RelationalRowsExpressionKind,
) !void {
    const keyword = fixedBinaryFunctionKeyword(kind) orelse return error.UnsupportedSqlShape;
    try parser.expectKeyword(tokens, pos, keyword);
    try parser.expectToken(tokens, pos, .lparen);
}

pub fn parseGreatestLeastFunctionCallStart(tokens: []const Token, pos: *usize) !db_mod.types.RelationalRowsExpressionKind {
    const cursor = parser.Cursor.init(tokens, pos);
    const kind: db_mod.types.RelationalRowsExpressionKind = if (cursor.matchKeyword("greatest"))
        .greatest
    else if (cursor.matchKeyword("least"))
        .least
    else
        return error.UnsupportedSqlShape;
    try cursor.expectToken(.lparen);
    return kind;
}

pub fn parseCaseFoldFunctionCallStart(tokens: []const Token, pos: *usize) !db_mod.types.RelationalRowsExpressionKind {
    const kind = matchCaseFoldFunctionKind(tokens, pos) orelse return error.UnsupportedSqlShape;
    try parser.expectToken(tokens, pos, .lparen);
    return kind;
}

pub fn matchCaseFoldFunctionKind(tokens: []const Token, pos: *usize) ?db_mod.types.RelationalRowsExpressionKind {
    if (pos.* >= tokens.len) return null;
    const token = tokens[pos.*];
    if (token.kind != .identifier) return null;
    const kind: db_mod.types.RelationalRowsExpressionKind = if (std.ascii.eqlIgnoreCase(token.text, "lower"))
        .lower
    else if (std.ascii.eqlIgnoreCase(token.text, "upper"))
        .upper
    else if (sqlKeywordIsInitcapFunction(token.text))
        .initcap
    else if (std.ascii.eqlIgnoreCase(token.text, "trim"))
        .trim
    else if (sqlKeywordIsTrimVariantFunction(token.text))
        if (std.ascii.eqlIgnoreCase(token.text, "ltrim")) .ltrim else .rtrim
    else
        return null;
    pos.* += 1;
    return kind;
}

pub fn parseConcatFunctionCallStart(tokens: []const Token, pos: *usize) !db_mod.types.RelationalRowsExpressionKind {
    const cursor = parser.Cursor.init(tokens, pos);
    const kind: db_mod.types.RelationalRowsExpressionKind = if (cursor.matchKeyword("concat_ws"))
        .concat_ws
    else if (cursor.matchKeyword("concat"))
        .concat
    else
        return error.UnsupportedSqlShape;
    try cursor.expectToken(.lparen);
    return kind;
}

pub fn parseCoalesceFunctionCallStart(tokens: []const Token, pos: *usize) !void {
    try parser.expectKeyword(tokens, pos, "coalesce");
    try parser.expectToken(tokens, pos, .lparen);
}

pub fn parseNullifFunctionCallStart(tokens: []const Token, pos: *usize) !void {
    try parser.expectKeyword(tokens, pos, "nullif");
    try parser.expectToken(tokens, pos, .lparen);
}

pub fn parseReplaceFunctionCallStart(tokens: []const Token, pos: *usize) !void {
    try parser.expectKeyword(tokens, pos, "replace");
    try parser.expectToken(tokens, pos, .lparen);
}

pub fn parseRegexpReplaceFunctionCallStart(tokens: []const Token, pos: *usize) !void {
    try parser.expectKeyword(tokens, pos, "regexp_replace");
    try parser.expectToken(tokens, pos, .lparen);
}

pub fn parseJsonArrayLengthFunctionCallStart(tokens: []const Token, pos: *usize) !void {
    _ = matchFunctionKeywordText(tokens, pos, sqlKeywordIsJsonArrayLengthFunction) orelse return error.UnsupportedSqlShape;
    try parser.expectToken(tokens, pos, .lparen);
}

pub fn parseJsonTypeofFunctionCallStart(tokens: []const Token, pos: *usize) !void {
    _ = matchFunctionKeywordText(tokens, pos, sqlKeywordIsJsonTypeofFunction) orelse return error.UnsupportedSqlShape;
    try parser.expectToken(tokens, pos, .lparen);
}

pub fn parseJsonBuildObjectFunctionCallStart(tokens: []const Token, pos: *usize) !void {
    _ = matchFunctionKeywordText(tokens, pos, sqlKeywordIsJsonBuildObjectFunction) orelse return error.UnsupportedSqlShape;
    try parser.expectToken(tokens, pos, .lparen);
}

pub fn parseArrayLengthFunctionCallStart(tokens: []const Token, pos: *usize) ![]const u8 {
    const keyword = matchFunctionKeywordText(tokens, pos, sqlKeywordIsArrayLengthFunction) orelse return error.UnsupportedSqlShape;
    try parser.expectToken(tokens, pos, .lparen);
    return keyword;
}

pub fn parseArrayPositionFunctionCallStart(tokens: []const Token, pos: *usize) !db_mod.types.RelationalRowsExpressionKind {
    const keyword = matchFunctionKeywordText(tokens, pos, sqlKeywordIsArrayPositionFunction) orelse return error.UnsupportedSqlShape;
    try parser.expectToken(tokens, pos, .lparen);
    return if (std.ascii.eqlIgnoreCase(keyword, "array_positions")) .array_positions else .array_position;
}

pub fn parseStringToArrayFunctionCallStart(tokens: []const Token, pos: *usize) !void {
    try parser.expectKeyword(tokens, pos, "string_to_array");
    try parser.expectToken(tokens, pos, .lparen);
}

pub fn parseArrayToStringFunctionCallStart(tokens: []const Token, pos: *usize) !void {
    try parser.expectKeyword(tokens, pos, "array_to_string");
    try parser.expectToken(tokens, pos, .lparen);
}

pub fn parseArrayElementTransformFunctionCallStart(tokens: []const Token, pos: *usize) !db_mod.types.RelationalRowsExpressionKind {
    const cursor = parser.Cursor.init(tokens, pos);
    const kind: db_mod.types.RelationalRowsExpressionKind = if (cursor.matchKeyword("array_append"))
        .array_append
    else if (cursor.matchKeyword("array_prepend"))
        .array_prepend
    else if (cursor.matchKeyword("array_cat"))
        .array_cat
    else if (cursor.matchKeyword("array_replace"))
        .array_replace
    else blk: {
        try cursor.expectKeyword("array_remove");
        break :blk .array_remove;
    };
    try cursor.expectToken(.lparen);
    return kind;
}

fn fixedUnaryFunctionKeyword(kind: db_mod.types.RelationalRowsExpressionKind) ?[]const u8 {
    return switch (kind) {
        .ascii => "ascii",
        .chr => "chr",
        .abs => "abs",
        .round => "round",
        .trunc => "trunc",
        .floor => "floor",
        .ceil => "ceil",
        .sqrt => "sqrt",
        .sign => "sign",
        .reverse => "reverse",
        .md5 => "md5",
        .to_jsonb => "to_jsonb",
        else => null,
    };
}

fn fixedBinaryFunctionKeyword(kind: db_mod.types.RelationalRowsExpressionKind) ?[]const u8 {
    return switch (kind) {
        .mod => "mod",
        .power => "power",
        else => null,
    };
}

pub fn matchLeftRightFunctionKind(tokens: []const Token, pos: *usize) ?db_mod.types.RelationalRowsExpressionKind {
    if (pos.* >= tokens.len) return null;
    const token = tokens[pos.*];
    if (token.kind != .identifier or !sqlKeywordIsLeftRightFunction(token.text)) return null;
    pos.* += 1;
    if (std.ascii.eqlIgnoreCase(token.text, "left")) return .left;
    return .right;
}

pub fn matchPadFunctionKind(tokens: []const Token, pos: *usize) ?db_mod.types.RelationalRowsExpressionKind {
    if (pos.* >= tokens.len) return null;
    const token = tokens[pos.*];
    if (token.kind != .identifier or !sqlKeywordIsPadFunction(token.text)) return null;
    pos.* += 1;
    if (std.ascii.eqlIgnoreCase(token.text, "lpad")) return .lpad;
    return .rpad;
}

pub fn matchTrimVariantFunctionKind(tokens: []const Token, pos: *usize) ?db_mod.types.RelationalRowsExpressionKind {
    if (pos.* >= tokens.len) return null;
    const token = tokens[pos.*];
    if (token.kind != .identifier or !sqlKeywordIsTrimVariantFunction(token.text)) return null;
    pos.* += 1;
    if (std.ascii.eqlIgnoreCase(token.text, "ltrim")) return .ltrim;
    if (std.ascii.eqlIgnoreCase(token.text, "rtrim")) return .rtrim;
    return .trim;
}

pub fn matchFunctionKeyword(
    tokens: []const Token,
    pos: *usize,
    comptime predicate: fn ([]const u8) bool,
) bool {
    return parser.Cursor.init(tokens, pos).matchIdentifierIf(predicate) != null;
}

pub fn matchFunctionKeywordText(
    tokens: []const Token,
    pos: *usize,
    comptime predicate: fn ([]const u8) bool,
) ?[]const u8 {
    const token = parser.Cursor.init(tokens, pos).matchIdentifierIf(predicate) orelse return null;
    return token.text;
}

pub fn parseFunctionCallStartIf(
    tokens: []const Token,
    pos: *usize,
    comptime predicate: fn ([]const u8) bool,
) !void {
    _ = matchFunctionKeywordText(tokens, pos, predicate) orelse return error.UnsupportedSqlShape;
    try parser.expectToken(tokens, pos, .lparen);
}

pub fn parseBooleanNotExpressionStart(tokens: []const Token, pos: *usize) !void {
    try parser.expectKeyword(tokens, pos, "not");
}

pub fn parseCaseExpressionStart(tokens: []const Token, pos: *usize) !void {
    try parser.expectKeyword(tokens, pos, "case");
}

pub fn matchCaseExpressionWhen(tokens: []const Token, pos: *usize) bool {
    return parser.matchKeyword(tokens, pos, "when");
}

pub fn parseCaseExpressionThen(tokens: []const Token, pos: *usize) !void {
    try parser.expectKeyword(tokens, pos, "then");
}

pub fn parseCaseExpressionElse(tokens: []const Token, pos: *usize) !void {
    try parser.expectKeyword(tokens, pos, "else");
}

pub fn parseCaseExpressionEnd(tokens: []const Token, pos: *usize) !void {
    try parser.expectKeyword(tokens, pos, "end");
}

pub fn parseCastExpressionCallStart(tokens: []const Token, pos: *usize) !void {
    try parser.expectKeyword(tokens, pos, "cast");
    try parser.expectToken(tokens, pos, .lparen);
}

pub fn parseCastExpressionAs(tokens: []const Token, pos: *usize) !void {
    try parser.expectKeyword(tokens, pos, "as");
}

pub fn parseDatePartFunctionCallStart(tokens: []const Token, pos: *usize) !bool {
    const extract_syntax = parser.matchKeyword(tokens, pos, "extract");
    if (!extract_syntax and !parser.matchKeyword(tokens, pos, "date_part")) return error.UnsupportedSqlShape;
    try parser.expectToken(tokens, pos, .lparen);
    return extract_syntax;
}

pub fn parseDatePartExtractSeparator(tokens: []const Token, pos: *usize) !void {
    try parser.expectKeyword(tokens, pos, "from");
}

pub fn functionCallStartsAt(tokens: []const Token, index: usize, keyword: []const u8) bool {
    var pos: usize = 0;
    return parser.Cursor.init(tokens, &pos).functionCallStartsAt(index, keyword);
}

pub fn functionCallStartsAtIf(
    tokens: []const Token,
    index: usize,
    comptime predicate: fn ([]const u8) bool,
) bool {
    var pos: usize = 0;
    return parser.Cursor.init(tokens, &pos).functionCallStartsAtIf(index, predicate);
}

pub fn peekFunctionCall(tokens: []const Token, pos: usize, keyword: []const u8) bool {
    return functionCallStartsAt(tokens, pos, keyword);
}

pub fn peekFunctionCallIf(
    tokens: []const Token,
    pos: usize,
    comptime predicate: fn ([]const u8) bool,
) bool {
    return functionCallStartsAtIf(tokens, pos, predicate);
}

pub fn peekCaseFoldFunctionCall(tokens: []const Token, pos: usize) bool {
    return peekFunctionCall(tokens, pos, "lower") or
        peekFunctionCall(tokens, pos, "upper") or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsInitcapFunction) or
        peekFunctionCall(tokens, pos, "trim") or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsTrimVariantFunction);
}

pub fn peekCaseExpressionSyntax(tokens: []const Token, pos: usize) bool {
    return parser.peekKeyword(tokens, pos, "case");
}

pub fn peekCastExpressionSyntax(tokens: []const Token, pos: usize) bool {
    return peekFunctionCall(tokens, pos, "cast");
}

pub fn peekBooleanNotExpressionSyntax(tokens: []const Token, pos: usize) bool {
    return parser.peekKeyword(tokens, pos, "not");
}

pub fn peekUnaryNegativeExpressionSyntax(tokens: []const Token, pos: usize) bool {
    return pos < tokens.len and tokens[pos].kind == .minus;
}

pub fn peekParenthesizedExpressionSyntax(tokens: []const Token, pos: usize) bool {
    return pos < tokens.len and tokens[pos].kind == .lparen;
}

pub fn peekReplaceFunctionCall(tokens: []const Token, pos: usize) bool {
    return peekFunctionCall(tokens, pos, "replace");
}

pub fn peekRegexpReplaceFunctionCall(tokens: []const Token, pos: usize) bool {
    return peekFunctionCall(tokens, pos, "regexp_replace");
}

pub fn peekConcatFunctionCall(tokens: []const Token, pos: usize) bool {
    return peekFunctionCall(tokens, pos, "concat") or
        peekFunctionCall(tokens, pos, "concat_ws");
}

pub fn peekCoalesceFunctionCall(tokens: []const Token, pos: usize) bool {
    return peekFunctionCall(tokens, pos, "coalesce");
}

pub fn peekNullifFunctionCall(tokens: []const Token, pos: usize) bool {
    return peekFunctionCall(tokens, pos, "nullif");
}

pub fn peekArrayElementTransformFunctionCall(tokens: []const Token, pos: usize) bool {
    return peekFunctionCall(tokens, pos, "array_append") or
        peekFunctionCall(tokens, pos, "array_prepend") or
        peekFunctionCall(tokens, pos, "array_cat") or
        peekFunctionCall(tokens, pos, "array_remove") or
        peekFunctionCall(tokens, pos, "array_replace");
}

pub fn peekArrayToStringFunctionCall(tokens: []const Token, pos: usize) bool {
    return peekFunctionCall(tokens, pos, "array_to_string");
}

pub fn peekStringToArrayFunctionCall(tokens: []const Token, pos: usize) bool {
    return peekFunctionCall(tokens, pos, "string_to_array");
}

pub fn peekFixedUnaryFunctionCall(tokens: []const Token, pos: usize, kind: db_mod.types.RelationalRowsExpressionKind) bool {
    const keyword = fixedUnaryFunctionKeyword(kind) orelse return false;
    return peekFunctionCall(tokens, pos, keyword);
}

pub fn peekFixedBinaryFunctionCall(tokens: []const Token, pos: usize, kind: db_mod.types.RelationalRowsExpressionKind) bool {
    const keyword = fixedBinaryFunctionKeyword(kind) orelse return false;
    return peekFunctionCall(tokens, pos, keyword);
}

pub fn peekGreatestLeastFunctionCall(tokens: []const Token, pos: usize) bool {
    return peekFunctionCall(tokens, pos, "greatest") or
        peekFunctionCall(tokens, pos, "least");
}

pub fn peekPositionFunctionSyntax(tokens: []const Token, pos: usize) bool {
    return peekFunctionCall(tokens, pos, "position");
}

pub fn peekSqlNowExpressionSyntax(tokens: []const Token, pos: usize) bool {
    return peekFunctionCall(tokens, pos, "now") or
        parser.peekKeyword(tokens, pos, "current_timestamp");
}

pub fn peekSqlCurrentDateExpressionSyntax(tokens: []const Token, pos: usize) bool {
    return parser.peekKeyword(tokens, pos, "current_date");
}

pub fn peekSqlIntervalExpressionSyntax(tokens: []const Token, pos: usize) bool {
    return parser.peekKeyword(tokens, pos, "interval");
}

pub fn peekTextLengthFunctionKeyword(tokens: []const Token, pos: usize) bool {
    if (pos >= tokens.len) return false;
    const token = tokens[pos];
    return token.kind == .identifier and
        (sqlKeywordIsLengthFunction(token.text) or
            sqlKeywordIsOctetLengthFunction(token.text) or
            sqlKeywordIsBitLengthFunction(token.text));
}

pub fn peekSubstringFunctionKeyword(tokens: []const Token, pos: usize) bool {
    if (pos >= tokens.len) return false;
    const token = tokens[pos];
    return token.kind == .identifier and sqlKeywordIsSubstringFunction(token.text);
}

pub fn peekSplitPartFunctionKeyword(tokens: []const Token, pos: usize) bool {
    if (pos >= tokens.len) return false;
    const token = tokens[pos];
    return token.kind == .identifier and sqlKeywordIsSplitPartFunction(token.text);
}

pub fn peekStrposFunctionKeyword(tokens: []const Token, pos: usize) bool {
    if (pos >= tokens.len) return false;
    const token = tokens[pos];
    return token.kind == .identifier and sqlKeywordIsStrposFunction(token.text);
}

pub fn peekUnsupportedSimpleFieldTail(tokens: []const Token, pos: usize) bool {
    return parser.peekKind(tokens, pos, .lparen) or peekJsonExtractOperator(tokens, pos);
}

pub fn parseSimpleReturningFieldOwnedAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    schema: runtime_schema.TableSchema,
) ![]const u8 {
    const field = try parseIdentifierOwnedAlloc(alloc, tokens, pos);
    errdefer alloc.free(field);
    if (peekUnsupportedSimpleFieldTail(tokens, pos.*)) return error.UnsupportedSqlShape;
    if (binder.relationalColumnForReturningField(schema, field) == null) return error.InvalidSqlCatalog;
    return field;
}

pub fn parseReturningListAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    schema: runtime_schema.TableSchema,
) ![]const []const u8 {
    if (parser.matchToken(tokens, pos, .star) != null) {
        const fields = try alloc.alloc([]const u8, 1);
        errdefer alloc.free(fields);
        fields[0] = try alloc.dupe(u8, "*");
        return fields;
    }

    var fields = std.ArrayListUnmanaged([]const u8).empty;
    errdefer {
        for (fields.items) |field| alloc.free(field);
        fields.deinit(alloc);
    }
    while (true) {
        const field = try parseSimpleReturningFieldOwnedAlloc(alloc, tokens, pos, schema);
        var field_transferred = false;
        errdefer if (!field_transferred) alloc.free(field);
        try fields.append(alloc, field);
        field_transferred = true;
        if (parser.matchToken(tokens, pos, .comma) == null) break;
    }
    return try fields.toOwnedSlice(alloc);
}

pub fn parseCoalesceFieldOperandOrNullOwnedAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    schema: runtime_schema.TableSchema,
    field_expression_qualifiers: []const []const u8,
    returning_expression_qualifiers: []const []const u8,
    defer_row_expression_field_validation: bool,
) !?db_mod.types.RelationalRowsCoalesceOperand {
    if (!parser.peekKind(tokens, pos.*, .identifier) or
        parser.peekKeyword(tokens, pos.*, "null") or
        parser.peekKeyword(tokens, pos.*, "true") or
        parser.peekKeyword(tokens, pos.*, "false"))
    {
        return null;
    }

    const field = try parseRowExpressionFieldOwnedAlloc(
        alloc,
        tokens,
        pos,
        schema,
        field_expression_qualifiers,
        returning_expression_qualifiers,
        defer_row_expression_field_validation,
    );
    errdefer alloc.free(field);
    if (parser.peekKind(tokens, pos.*, .lparen)) return error.UnsupportedSqlShape;
    if (binder.relationalColumnForField(schema, field, null) == null) return error.InvalidSqlCatalog;
    return .{ .kind = .field, .field = field };
}

pub fn parseArrayFieldOwnedAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    schema: runtime_schema.TableSchema,
) ![]const u8 {
    const field = try parseIdentifierOwnedAlloc(alloc, tokens, pos);
    errdefer alloc.free(field);
    _ = binder.relationalColumnForField(schema, field, .array) orelse return error.InvalidSqlCatalog;
    return field;
}

pub fn parseRowExpressionArrayFieldOwnedAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    schema: runtime_schema.TableSchema,
    field_expression_qualifiers: []const []const u8,
    returning_expression_qualifiers: []const []const u8,
    defer_row_expression_field_validation: bool,
) ![]const u8 {
    const parsed_field = try parseIdentifierOwnedAlloc(alloc, tokens, pos);
    defer alloc.free(parsed_field);
    const field = try binder.normalizeRowExpressionFieldAlloc(
        alloc,
        schema,
        parsed_field,
        field_expression_qualifiers,
        returning_expression_qualifiers,
        defer_row_expression_field_validation,
    );
    errdefer alloc.free(field);
    _ = binder.relationalColumnForField(schema, field, .array) orelse return error.InvalidSqlCatalog;
    return field;
}

pub fn peekSimpleReturningField(tokens: []const Token, pos: usize) bool {
    if (pos >= tokens.len or tokens[pos].kind != .identifier) return false;
    if (parser.peekKeyword(tokens, pos, "lower") or
        parser.peekKeyword(tokens, pos, "upper") or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsInitcapFunction) or
        parser.peekKeyword(tokens, pos, "trim") or
        parser.peekKeyword(tokens, pos, "replace") or
        parser.peekKeyword(tokens, pos, "regexp_replace") or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsRegexpSubstrFunction) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsRegexpMatchFunction) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsRegexpCountFunction) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsRegexpInstrFunction) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsTranslateFunction) or
        parser.peekKeyword(tokens, pos, "concat") or
        parser.peekKeyword(tokens, pos, "concat_ws") or
        parser.peekKeyword(tokens, pos, "coalesce") or
        parser.peekKeyword(tokens, pos, "nullif") or
        peekTextLengthFunctionKeyword(tokens, pos) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsAsciiFunction) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsChrFunction) or
        peekSubstringFunctionKeyword(tokens, pos) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsOverlayFunction) or
        peekSplitPartFunctionKeyword(tokens, pos) or
        peekStrposFunctionKeyword(tokens, pos) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsLeftRightFunction) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsPadFunction) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsRepeatFunction) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsReverseFunction) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsMd5Function) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsStartsWithFunction) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsEndsWithFunction) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsDateTruncFunction) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsDateBinFunction) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsDatePartFunction) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsTrimVariantFunction) or
        parser.peekKeyword(tokens, pos, "position") or
        parser.peekKeyword(tokens, pos, "abs") or
        parser.peekKeyword(tokens, pos, "round") or
        peekFunctionCall(tokens, pos, "trunc") or
        peekFunctionCall(tokens, pos, "floor") or
        peekFunctionCall(tokens, pos, "ceil") or
        peekFunctionCall(tokens, pos, "sqrt") or
        peekFunctionCall(tokens, pos, "sign") or
        peekFunctionCall(tokens, pos, "mod") or
        peekFunctionCall(tokens, pos, "power") or
        parser.peekKeyword(tokens, pos, "case") or
        parser.peekKeyword(tokens, pos, "cast"))
    {
        return false;
    }
    if (pos + 1 >= tokens.len) return true;
    return switch (tokens[pos + 1].kind) {
        .plus, .minus, .star, .slash, .percent, .lparen, .arrow_json, .arrow_text, .path_arrow_json, .path_arrow_text, .pipe_concat => false,
        else => true,
    };
}

pub fn peekSqlTypedDatetimeLiteral(tokens: []const Token, pos: usize) bool {
    return pos + 1 < tokens.len and
        tokens[pos].kind == .identifier and
        tokens[pos + 1].kind == .string and
        (std.ascii.eqlIgnoreCase(tokens[pos].text, "date") or
            std.ascii.eqlIgnoreCase(tokens[pos].text, "timestamp") or
            std.ascii.eqlIgnoreCase(tokens[pos].text, "timestamptz"));
}

pub fn peekParenthesizedNullTestProjection(tokens: []const Token, pos: usize) bool {
    if (pos + 2 >= tokens.len) return false;
    return tokens[pos].kind == .lparen and
        tokens[pos + 1].kind == .identifier and
        tokens[pos + 2].kind == .identifier and
        std.ascii.eqlIgnoreCase(tokens[pos + 2].text, "is");
}

pub fn rowExpressionHasTopLevelPipeConcat(tokens: []const Token, pos: usize) bool {
    var depth: usize = 0;
    var i = pos;
    while (i < tokens.len) : (i += 1) {
        const token = tokens[i];
        switch (token.kind) {
            .lparen => depth += 1,
            .rparen => {
                if (depth == 0) return false;
                depth -= 1;
            },
            .pipe_concat => if (depth == 0) return true,
            .comma, .semicolon, .eq, .neq, .lt, .lte, .gt, .gte, .at_contains, .range_overlap, .question, .arrow_json, .arrow_text, .path_arrow_json, .path_arrow_text, .regex_match, .regex_imatch, .regex_not_match, .regex_not_imatch => if (depth == 0) return false,
            .identifier => if (depth == 0 and rowExpressionBoundaryKeyword(token.text)) return false,
            else => {},
        }
    }
    return false;
}

pub fn nextIsAggregateFunction(tokens: []const Token, pos: usize) bool {
    if (pos + 1 >= tokens.len) return false;
    if (tokens[pos].kind != .identifier or tokens[pos + 1].kind != .lparen) return false;
    const name = tokens[pos].text;
    return std.ascii.eqlIgnoreCase(name, "count") or
        std.ascii.eqlIgnoreCase(name, "sum") or
        std.ascii.eqlIgnoreCase(name, "min") or
        std.ascii.eqlIgnoreCase(name, "max") or
        std.ascii.eqlIgnoreCase(name, "avg") or
        std.ascii.eqlIgnoreCase(name, "percentile_cont") or
        std.ascii.eqlIgnoreCase(name, "percentile_disc") or
        std.ascii.eqlIgnoreCase(name, "mode") or
        std.ascii.eqlIgnoreCase(name, "array_agg") or
        std.ascii.eqlIgnoreCase(name, "string_agg") or
        std.ascii.eqlIgnoreCase(name, "bool_or") or
        std.ascii.eqlIgnoreCase(name, "bool_and");
}

pub fn peekWindowFunction(tokens: []const Token, pos: usize) bool {
    return parser.peekKeyword(tokens, pos, "row_number") or
        parser.peekKeyword(tokens, pos, "rank") or
        parser.peekKeyword(tokens, pos, "dense_rank") or
        parser.peekKeyword(tokens, pos, "percent_rank") or
        parser.peekKeyword(tokens, pos, "cume_dist") or
        parser.peekKeyword(tokens, pos, "ntile") or
        parser.peekKeyword(tokens, pos, "lag") or
        parser.peekKeyword(tokens, pos, "lead") or
        parser.peekKeyword(tokens, pos, "first_value") or
        parser.peekKeyword(tokens, pos, "last_value") or
        parser.peekKeyword(tokens, pos, "nth_value") or
        parser.peekKeyword(tokens, pos, "count") or
        parser.peekKeyword(tokens, pos, "sum") or
        parser.peekKeyword(tokens, pos, "avg") or
        parser.peekKeyword(tokens, pos, "min") or
        parser.peekKeyword(tokens, pos, "max") or
        parser.peekKeyword(tokens, pos, "bool_or") or
        parser.peekKeyword(tokens, pos, "bool_and");
}

pub fn peekAggregateExpressionInput(tokens: []const Token, pos: usize) bool {
    if (pos >= tokens.len) return false;
    if (tokens[pos].kind == .lparen or tokens[pos].kind == .minus) return true;
    if (jsonExtractExpressionCanStartAt(tokens, pos)) return true;
    if (peekFunctionCallIf(tokens, pos, sqlKeywordIsJsonExtractPathFunction) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsJsonTypeofFunction) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsJsonArrayLengthFunction) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsJsonBuildObjectFunction) or
        value_mod.peekConvertFromFunctionCall(tokens, pos) or
        functionCallStartsAtIf(tokens, pos, sqlKeywordIsArrayLengthFunction) or
        functionCallStartsAtIf(tokens, pos, sqlKeywordIsArrayPositionFunction) or
        parser.peekKeyword(tokens, pos, "case") or
        parser.peekKeyword(tokens, pos, "cast") or
        peekCoalesceFunctionCall(tokens, pos) or
        peekArrayElementTransformFunctionCall(tokens, pos) or
        peekArrayToStringFunctionCall(tokens, pos) or
        peekStringToArrayFunctionCall(tokens, pos) or
        peekCaseFoldFunctionCall(tokens, pos) or
        peekConcatFunctionCall(tokens, pos) or
        peekReplaceFunctionCall(tokens, pos) or
        peekRegexpReplaceFunctionCall(tokens, pos) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsRegexpSubstrFunction) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsRegexpMatchFunction) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsRegexpCountFunction) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsRegexpInstrFunction) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsTranslateFunction) or
        peekNullifFunctionCall(tokens, pos) or
        peekTextLengthFunctionKeyword(tokens, pos) or
        peekFixedUnaryFunctionCall(tokens, pos, .ascii) or
        peekFixedUnaryFunctionCall(tokens, pos, .chr) or
        peekSubstringFunctionKeyword(tokens, pos) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsOverlayFunction) or
        peekSplitPartFunctionKeyword(tokens, pos) or
        peekStrposFunctionKeyword(tokens, pos) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsLeftRightFunction) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsPadFunction) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsRepeatFunction) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsReverseFunction) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsStartsWithFunction) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsEndsWithFunction) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsDateTruncFunction) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsDateBinFunction) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsDatePartFunction) or
        parser.peekKeyword(tokens, pos, "position") or
        peekFixedUnaryFunctionCall(tokens, pos, .abs) or
        peekFixedUnaryFunctionCall(tokens, pos, .round) or
        peekFixedUnaryFunctionCall(tokens, pos, .trunc) or
        peekFixedUnaryFunctionCall(tokens, pos, .floor) or
        peekFixedUnaryFunctionCall(tokens, pos, .ceil) or
        peekFixedUnaryFunctionCall(tokens, pos, .sqrt) or
        peekFixedUnaryFunctionCall(tokens, pos, .sign) or
        peekFixedBinaryFunctionCall(tokens, pos, .mod) or
        peekFixedBinaryFunctionCall(tokens, pos, .power) or
        peekGreatestLeastFunctionCall(tokens, pos))
    {
        return true;
    }
    if (tokens[pos].kind == .identifier and pos + 1 < tokens.len) {
        return switch (tokens[pos + 1].kind) {
            .plus, .minus, .star, .slash, .percent => true,
            else => false,
        };
    }
    return false;
}

pub fn peekAggregateExpressionFilter(tokens: []const Token, pos: usize) bool {
    if (pos >= tokens.len) return false;
    if (peekParenthesizedExpressionCondition(tokens, pos)) return true;
    if (tokens[pos].kind == .minus) return true;
    if (jsonExtractExpressionCanStartAt(tokens, pos)) return true;
    if (jsonKeySetExpressionCanStartAt(tokens, pos)) return true;
    if (rowExpressionHasTopLevelPipeConcat(tokens, pos)) return true;
    if (parser.peekKeyword(tokens, pos, "lower") or
        parser.peekKeyword(tokens, pos, "upper") or
        parser.peekKeyword(tokens, pos, "trim") or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsTrimVariantFunction) or
        parser.peekKeyword(tokens, pos, "replace") or
        parser.peekKeyword(tokens, pos, "regexp_replace") or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsRegexpSubstrFunction) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsRegexpMatchFunction) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsRegexpCountFunction) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsRegexpInstrFunction) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsTranslateFunction) or
        parser.peekKeyword(tokens, pos, "case") or
        parser.peekKeyword(tokens, pos, "cast") or
        parser.peekKeyword(tokens, pos, "coalesce") or
        parser.peekKeyword(tokens, pos, "nullif") or
        peekTextLengthFunctionKeyword(tokens, pos) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsAsciiFunction) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsChrFunction) or
        peekSubstringFunctionKeyword(tokens, pos) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsOverlayFunction) or
        peekSplitPartFunctionKeyword(tokens, pos) or
        peekStrposFunctionKeyword(tokens, pos) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsLeftRightFunction) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsPadFunction) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsRepeatFunction) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsReverseFunction) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsStartsWithFunction) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsEndsWithFunction) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsDateTruncFunction) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsDateBinFunction) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsDatePartFunction) or
        parser.peekKeyword(tokens, pos, "position") or
        parser.peekKeyword(tokens, pos, "abs") or
        parser.peekKeyword(tokens, pos, "round") or
        peekFunctionCall(tokens, pos, "trunc") or
        peekFunctionCall(tokens, pos, "floor") or
        peekFunctionCall(tokens, pos, "ceil") or
        peekFunctionCall(tokens, pos, "sqrt") or
        peekFunctionCall(tokens, pos, "sign") or
        peekFunctionCall(tokens, pos, "mod") or
        peekFunctionCall(tokens, pos, "power") or
        parser.peekKeyword(tokens, pos, "greatest") or
        parser.peekKeyword(tokens, pos, "least") or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsJsonExtractPathFunction) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsJsonTypeofFunction) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsJsonArrayLengthFunction) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsJsonBuildObjectFunction) or
        functionCallStartsAtIf(tokens, pos, sqlKeywordIsArrayLengthFunction) or
        functionCallStartsAtIf(tokens, pos, sqlKeywordIsArrayPositionFunction) or
        parser.peekKeyword(tokens, pos, "now") or
        parser.peekKeyword(tokens, pos, "current_timestamp") or
        parser.peekKeyword(tokens, pos, "current_date"))
    {
        return true;
    }
    if (tokens[pos].kind == .identifier and pos + 1 < tokens.len) {
        return switch (tokens[pos + 1].kind) {
            .plus, .minus, .star, .slash, .percent, .arrow_json, .arrow_text, .path_arrow_json, .path_arrow_text, .pipe_concat => true,
            else => false,
        };
    }
    return false;
}

pub fn peekAggregateOutputOrderExpression(tokens: []const Token, pos: usize) bool {
    if (pos >= tokens.len) return false;
    if (tokens[pos].kind == .lparen or tokens[pos].kind == .minus) return true;
    if (rowExpressionHasTopLevelPipeConcat(tokens, pos)) return true;
    if (parser.peekKeyword(tokens, pos, "lower") or
        parser.peekKeyword(tokens, pos, "upper") or
        parser.peekKeyword(tokens, pos, "trim") or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsTrimVariantFunction) or
        parser.peekKeyword(tokens, pos, "replace") or
        parser.peekKeyword(tokens, pos, "regexp_replace") or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsRegexpSubstrFunction) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsRegexpMatchFunction) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsRegexpCountFunction) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsRegexpInstrFunction) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsTranslateFunction) or
        parser.peekKeyword(tokens, pos, "concat") or
        parser.peekKeyword(tokens, pos, "concat_ws") or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsJsonExtractPathFunction) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsJsonTypeofFunction) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsJsonArrayLengthFunction) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsJsonBuildObjectFunction) or
        functionCallStartsAtIf(tokens, pos, sqlKeywordIsArrayLengthFunction) or
        functionCallStartsAtIf(tokens, pos, sqlKeywordIsArrayPositionFunction) or
        parser.peekKeyword(tokens, pos, "array_append") or
        parser.peekKeyword(tokens, pos, "array_prepend") or
        parser.peekKeyword(tokens, pos, "array_cat") or
        parser.peekKeyword(tokens, pos, "array_remove") or
        parser.peekKeyword(tokens, pos, "array_replace") or
        parser.peekKeyword(tokens, pos, "array_to_string") or
        parser.peekKeyword(tokens, pos, "string_to_array") or
        parser.peekKeyword(tokens, pos, "case") or
        parser.peekKeyword(tokens, pos, "cast") or
        parser.peekKeyword(tokens, pos, "coalesce") or
        parser.peekKeyword(tokens, pos, "nullif") or
        peekTextLengthFunctionKeyword(tokens, pos) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsAsciiFunction) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsChrFunction) or
        peekSubstringFunctionKeyword(tokens, pos) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsOverlayFunction) or
        peekSplitPartFunctionKeyword(tokens, pos) or
        peekStrposFunctionKeyword(tokens, pos) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsLeftRightFunction) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsPadFunction) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsRepeatFunction) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsReverseFunction) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsMd5Function) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsStartsWithFunction) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsEndsWithFunction) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsDateTruncFunction) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsDateBinFunction) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsDatePartFunction) or
        parser.peekKeyword(tokens, pos, "position") or
        parser.peekKeyword(tokens, pos, "abs") or
        parser.peekKeyword(tokens, pos, "round") or
        peekFunctionCall(tokens, pos, "trunc") or
        peekFunctionCall(tokens, pos, "floor") or
        peekFunctionCall(tokens, pos, "ceil") or
        peekFunctionCall(tokens, pos, "sqrt") or
        peekFunctionCall(tokens, pos, "sign") or
        peekFunctionCall(tokens, pos, "mod") or
        peekFunctionCall(tokens, pos, "power") or
        parser.peekKeyword(tokens, pos, "greatest") or
        parser.peekKeyword(tokens, pos, "least"))
    {
        return true;
    }
    if (tokens[pos].kind == .identifier and pos + 1 < tokens.len) {
        return switch (tokens[pos + 1].kind) {
            .plus, .minus, .star, .slash, .percent, .pipe_concat, .arrow_json, .arrow_text, .path_arrow_json, .path_arrow_text => true,
            else => false,
        };
    }
    return false;
}

pub fn peekGeneralOrderRowExpression(tokens: []const Token, pos: usize) bool {
    return peekFunctionCallIf(tokens, pos, sqlKeywordIsJsonExtractPathFunction) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsJsonTypeofFunction) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsJsonArrayLengthFunction) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsJsonBuildObjectFunction) or
        value_mod.peekToJsonbFunctionCall(tokens, pos) or
        functionCallStartsAtIf(tokens, pos, sqlKeywordIsArrayLengthFunction) or
        functionCallStartsAtIf(tokens, pos, sqlKeywordIsArrayPositionFunction) or
        peekArrayElementTransformFunctionCall(tokens, pos) or
        peekArrayToStringFunctionCall(tokens, pos) or
        peekCaseExpressionSyntax(tokens, pos) or
        peekCastExpressionSyntax(tokens, pos) or
        peekCoalesceFunctionCall(tokens, pos) or
        peekRegexpReplaceFunctionCall(tokens, pos) or
        peekReplaceFunctionCall(tokens, pos) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsRegexpSubstrFunction) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsRegexpMatchFunction) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsRegexpCountFunction) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsRegexpInstrFunction) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsTranslateFunction) or
        peekNullifFunctionCall(tokens, pos) or
        peekTextLengthFunctionKeyword(tokens, pos) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsAsciiFunction) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsChrFunction) or
        peekSubstringFunctionKeyword(tokens, pos) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsOverlayFunction) or
        peekSplitPartFunctionKeyword(tokens, pos) or
        peekStrposFunctionKeyword(tokens, pos) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsLeftRightFunction) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsPadFunction) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsRepeatFunction) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsReverseFunction) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsMd5Function) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsStartsWithFunction) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsEndsWithFunction) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsDateTruncFunction) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsDateBinFunction) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsDatePartFunction) or
        peekPositionFunctionSyntax(tokens, pos) or
        peekFixedUnaryFunctionCall(tokens, pos, .abs) or
        peekFixedUnaryFunctionCall(tokens, pos, .round) or
        peekFixedUnaryFunctionCall(tokens, pos, .trunc) or
        peekFixedUnaryFunctionCall(tokens, pos, .floor) or
        peekFixedUnaryFunctionCall(tokens, pos, .ceil) or
        peekFixedUnaryFunctionCall(tokens, pos, .sqrt) or
        peekFixedUnaryFunctionCall(tokens, pos, .sign) or
        peekFixedBinaryFunctionCall(tokens, pos, .mod) or
        peekFixedBinaryFunctionCall(tokens, pos, .power) or
        peekGreatestLeastFunctionCall(tokens, pos);
}

pub const OrderExpressionStart = enum {
    parenthesized_null_test,
    parenthesized,
    pipe_concat,
    json_extract_field,
    generated_or_case_fold,
    generated_or_md5,
    generated_or_concat,
    general,
    unary_negative,
    field,
};

pub fn orderExpressionStartAt(tokens: []const Token, pos: usize) OrderExpressionStart {
    if (peekParenthesizedNullTestProjection(tokens, pos)) return .parenthesized_null_test;
    if (peekParenthesizedExpressionSyntax(tokens, pos)) return .parenthesized;
    if (rowExpressionHasTopLevelPipeConcat(tokens, pos)) return .pipe_concat;
    if (pos < tokens.len and tokens[pos].kind == .identifier and pos + 1 < tokens.len and tokenKindIsJsonExtractOperator(tokens[pos + 1].kind)) return .json_extract_field;
    if (peekCaseFoldFunctionCall(tokens, pos)) return .generated_or_case_fold;
    if (peekFunctionCallIf(tokens, pos, sqlKeywordIsMd5Function)) return .generated_or_md5;
    if (peekConcatFunctionCall(tokens, pos)) return .generated_or_concat;
    if (peekGeneralOrderRowExpression(tokens, pos)) return .general;
    if (peekUnaryNegativeExpressionSyntax(tokens, pos)) return .unary_negative;
    return .field;
}

pub const SelectItemStart = enum {
    pipe_concat,
    unary_negative,
    boolean_not,
    extension_function,
    routine_expression,
    uuid_v4,
    now,
    current_date,
    typed_datetime_literal,
    json_extract_path,
    json_typeof,
    json_array_length,
    json_build_object,
    convert_from,
    to_jsonb,
    array_length,
    array_position,
    array_element_transform,
    array_to_string,
    string_to_array,
    coalesce,
    case_fold,
    replace,
    regexp_replace,
    regexp_substr,
    regexp_match,
    regexp_count,
    regexp_instr,
    translate,
    concat,
    nullif,
    text_length,
    ascii,
    chr,
    substring,
    overlay,
    split_part,
    strpos,
    left_right,
    pad,
    repeat,
    reverse,
    md5,
    starts_with,
    ends_with,
    date_trunc,
    date_bin,
    date_part,
    abs,
    round,
    trunc,
    floor,
    ceil,
    sqrt,
    sign,
    mod,
    power,
    greatest_least,
    case,
    cast,
    parenthesized,
};

pub const SelectFieldItemParserHooks = struct {
    ptr: *anyopaque,
    parse_json_array_value: *const fn (*anyopaque) anyerror![]const u8,
    parse_arithmetic_projection_from_field: *const fn (*anyopaque, []const u8) anyerror!db_mod.types.RelationalRowsExpressionProjection,
    parse_boolean_projection_from_field: *const fn (*anyopaque, []const u8) anyerror!db_mod.types.RelationalRowsExpressionProjection,
};

pub fn selectItemStartAt(tokens: []const Token, pos: usize) ?SelectItemStart {
    if (rowExpressionHasTopLevelPipeConcat(tokens, pos)) return .pipe_concat;
    if (peekUnaryNegativeExpressionSyntax(tokens, pos)) return .unary_negative;
    if (peekBooleanNotExpressionSyntax(tokens, pos)) return .boolean_not;
    if (peekFunctionCallIf(tokens, pos, sqlKeywordIsUuidV4Function)) return .uuid_v4;
    if (peekSqlNowExpressionSyntax(tokens, pos)) return .now;
    if (peekSqlCurrentDateExpressionSyntax(tokens, pos)) return .current_date;
    if (peekSqlTypedDatetimeLiteral(tokens, pos)) return .typed_datetime_literal;
    if (peekFunctionCallIf(tokens, pos, sqlKeywordIsJsonExtractPathFunction)) return .json_extract_path;
    if (peekFunctionCallIf(tokens, pos, sqlKeywordIsJsonTypeofFunction)) return .json_typeof;
    if (peekFunctionCallIf(tokens, pos, sqlKeywordIsJsonArrayLengthFunction)) return .json_array_length;
    if (peekFunctionCallIf(tokens, pos, sqlKeywordIsJsonBuildObjectFunction)) return .json_build_object;
    if (value_mod.peekConvertFromFunctionCall(tokens, pos)) return .convert_from;
    if (value_mod.peekToJsonbFunctionCall(tokens, pos)) return .to_jsonb;
    if (functionCallStartsAtIf(tokens, pos, sqlKeywordIsArrayLengthFunction)) return .array_length;
    if (functionCallStartsAtIf(tokens, pos, sqlKeywordIsArrayPositionFunction)) return .array_position;
    if (peekArrayElementTransformFunctionCall(tokens, pos)) return .array_element_transform;
    if (peekArrayToStringFunctionCall(tokens, pos)) return .array_to_string;
    if (peekStringToArrayFunctionCall(tokens, pos)) return .string_to_array;
    if (peekCoalesceFunctionCall(tokens, pos)) return .coalesce;
    if (peekCaseFoldFunctionCall(tokens, pos)) return .case_fold;
    if (peekReplaceFunctionCall(tokens, pos)) return .replace;
    if (peekRegexpReplaceFunctionCall(tokens, pos)) return .regexp_replace;
    if (peekFunctionCallIf(tokens, pos, sqlKeywordIsRegexpSubstrFunction)) return .regexp_substr;
    if (peekFunctionCallIf(tokens, pos, sqlKeywordIsRegexpMatchFunction)) return .regexp_match;
    if (peekFunctionCallIf(tokens, pos, sqlKeywordIsRegexpCountFunction)) return .regexp_count;
    if (peekFunctionCallIf(tokens, pos, sqlKeywordIsRegexpInstrFunction)) return .regexp_instr;
    if (peekFunctionCallIf(tokens, pos, sqlKeywordIsTranslateFunction)) return .translate;
    if (peekConcatFunctionCall(tokens, pos)) return .concat;
    if (peekNullifFunctionCall(tokens, pos)) return .nullif;
    if (peekTextLengthFunctionKeyword(tokens, pos)) return .text_length;
    if (peekFunctionCallIf(tokens, pos, sqlKeywordIsAsciiFunction)) return .ascii;
    if (peekFunctionCallIf(tokens, pos, sqlKeywordIsChrFunction)) return .chr;
    if (peekSubstringFunctionKeyword(tokens, pos)) return .substring;
    if (peekFunctionCallIf(tokens, pos, sqlKeywordIsOverlayFunction)) return .overlay;
    if (peekSplitPartFunctionKeyword(tokens, pos)) return .split_part;
    if (peekStrposFunctionKeyword(tokens, pos) or peekPositionFunctionSyntax(tokens, pos)) return .strpos;
    if (peekFunctionCallIf(tokens, pos, sqlKeywordIsLeftRightFunction)) return .left_right;
    if (peekFunctionCallIf(tokens, pos, sqlKeywordIsPadFunction)) return .pad;
    if (peekFunctionCallIf(tokens, pos, sqlKeywordIsRepeatFunction)) return .repeat;
    if (peekFunctionCallIf(tokens, pos, sqlKeywordIsReverseFunction)) return .reverse;
    if (peekFunctionCallIf(tokens, pos, sqlKeywordIsMd5Function)) return .md5;
    if (peekFunctionCallIf(tokens, pos, sqlKeywordIsStartsWithFunction)) return .starts_with;
    if (peekFunctionCallIf(tokens, pos, sqlKeywordIsEndsWithFunction)) return .ends_with;
    if (peekFunctionCallIf(tokens, pos, sqlKeywordIsDateTruncFunction)) return .date_trunc;
    if (peekFunctionCallIf(tokens, pos, sqlKeywordIsDateBinFunction)) return .date_bin;
    if (peekFunctionCallIf(tokens, pos, sqlKeywordIsDatePartFunction)) return .date_part;
    if (peekFixedUnaryFunctionCall(tokens, pos, .abs)) return .abs;
    if (peekFixedUnaryFunctionCall(tokens, pos, .round)) return .round;
    if (peekFixedUnaryFunctionCall(tokens, pos, .trunc)) return .trunc;
    if (peekFixedUnaryFunctionCall(tokens, pos, .floor)) return .floor;
    if (peekFixedUnaryFunctionCall(tokens, pos, .ceil)) return .ceil;
    if (peekFixedUnaryFunctionCall(tokens, pos, .sqrt)) return .sqrt;
    if (peekFixedUnaryFunctionCall(tokens, pos, .sign)) return .sign;
    if (peekFixedBinaryFunctionCall(tokens, pos, .mod)) return .mod;
    if (peekFixedBinaryFunctionCall(tokens, pos, .power)) return .power;
    if (peekGreatestLeastFunctionCall(tokens, pos)) return .greatest_least;
    if (peekCaseExpressionSyntax(tokens, pos)) return .case;
    if (peekCastExpressionSyntax(tokens, pos)) return .cast;
    if (peekParenthesizedExpressionSyntax(tokens, pos)) return .parenthesized;
    return null;
}

pub fn selectItemStartWithFunctionBindingsAt(tokens: []const Token, pos: usize, bindings: SqlFunctionBindings) ?SelectItemStart {
    if (selectItemStartAt(tokens, pos)) |start| return start;
    if (peekExtensionFunctionCall(tokens, pos, bindings.extension_functions)) return .extension_function;
    if (peekRoutineExpressionCall(tokens, pos, bindings.routine_expressions)) return .routine_expression;
    return null;
}

pub fn parseSelectFieldItemAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    schema: runtime_schema.TableSchema,
    params: []const value_mod.SqlValue,
    field_expression_qualifiers: []const []const u8,
    returning_expression_qualifiers: []const []const u8,
    defer_row_expression_field_validation: bool,
    field_source: db_mod.types.RelationalRowsExpressionFieldSource,
    hooks: SelectFieldItemParserHooks,
) !plan_mod.SelectItem {
    const parsed_field = try parseRowExpressionFieldOwnedAlloc(alloc, tokens, pos, schema, field_expression_qualifiers, returning_expression_qualifiers, defer_row_expression_field_validation);
    defer alloc.free(parsed_field);
    const field = try binder.normalizeRowExpressionFieldAlloc(alloc, schema, parsed_field, field_expression_qualifiers, returning_expression_qualifiers, defer_row_expression_field_validation);
    var field_owned = true;
    errdefer if (field_owned) alloc.free(field);
    if (parser.peekKind(tokens, pos.*, .lparen)) return error.UnsupportedSqlShape;
    if (matchJsonExtractOperator(tokens, pos)) |operator| {
        const as_text = tokenKindIsJsonExtractTextOperator(operator);
        if (binder.relationalColumnForField(schema, field, .json) == null) return error.InvalidSqlCatalog;
        const path = try value_mod.parseJsonExtractOperatorPathOwnedAlloc(alloc, tokens, pos, params, operator);
        var path_owned = true;
        errdefer if (path_owned) alloc.free(path);
        const output = try grammar.parseProjectionOutputOwnedAlloc(alloc, tokens, pos, path);
        var output_owned = true;
        errdefer if (output_owned) alloc.free(output);
        const expression = try buildJsonExtractExpressionAlloc(
            alloc,
            .{ .kind = .field, .field = field, .field_source = field_source },
            path,
            as_text,
        );
        errdefer freeExpression(alloc, expression);
        field_owned = false;
        path_owned = false;
        output_owned = false;
        return .{ .expression = buildExpressionProjection(output, expression) };
    }
    if (parser.matchToken(tokens, pos, .question) != null) {
        if (binder.relationalColumnForField(schema, field, .json) == null) return error.InvalidSqlCatalog;
        const path = try value_mod.parseJsonPathOwnedAlloc(alloc, tokens, pos, params);
        var path_owned = true;
        errdefer if (path_owned) alloc.free(path);
        const output = try grammar.parseProjectionOutputOwnedAlloc(alloc, tokens, pos, path);
        var output_owned = true;
        errdefer if (output_owned) alloc.free(output);
        const expression = try buildJsonPathExistsExpressionAlloc(
            alloc,
            .{ .kind = .field, .field = field, .field_source = field_source },
            path,
        );
        errdefer freeExpression(alloc, expression);
        field_owned = false;
        path_owned = false;
        output_owned = false;
        return .{ .expression = buildExpressionProjection(output, expression) };
    }
    if (parser.matchToken(tokens, pos, .question_any) != null or parser.matchToken(tokens, pos, .question_all) != null) {
        const match_all = tokens[pos.* - 1].kind == .question_all;
        if (binder.relationalColumnForField(schema, field, .json) == null) return error.InvalidSqlCatalog;
        const values_json = try hooks.parse_json_array_value(hooks.ptr);
        defer alloc.free(values_json);
        const expression = try buildJsonKeySetExistsExpressionAlloc(alloc, field, field_source, match_all, values_json);
        errdefer freeExpression(alloc, expression);
        const output = try grammar.parseProjectionOutputOwnedAlloc(alloc, tokens, pos, if (match_all) "json_keys_all" else "json_keys_any");
        var output_owned = true;
        errdefer if (output_owned) alloc.free(output);
        output_owned = false;
        alloc.free(field);
        field_owned = false;
        return .{ .expression = buildExpressionProjection(output, expression) };
    }
    const column = binder.relationalColumnForField(schema, field, null) orelse return error.InvalidSqlCatalog;
    if (peekArithmeticOperator(tokens, pos.*)) |_| {
        if (column.field_type != .numeric and column.field_type != .datetime) return error.InvalidSqlCatalog;
        field_owned = false;
        return .{ .expression = try hooks.parse_arithmetic_projection_from_field(hooks.ptr, field) };
    }
    if (peekBooleanOperator(tokens, pos.*)) |_| {
        if (column.field_type != .boolean) return error.InvalidSqlCatalog;
        field_owned = false;
        return .{ .expression = try hooks.parse_boolean_projection_from_field(hooks.ptr, field) };
    }
    const alias = try grammar.parseOptionalProjectionAliasAlloc(alloc, tokens, pos);
    var alias_transferred = false;
    errdefer if (!alias_transferred) if (alias) |owned| alloc.free(owned);
    if (alias) |output| {
        if (!std.mem.eql(u8, output, field)) {
            alias_transferred = true;
            field_owned = false;
            return .{ .expression = .{
                .output = output,
                .expression = .{
                    .kind = .field,
                    .field = field,
                    .field_source = field_source,
                },
            } };
        }
        alloc.free(output);
        alias_transferred = true;
    }
    field_owned = false;
    return .{ .field = field };
}

pub const RowExpressionOperandStart = enum {
    parenthesized,
    unary_negative,
    boolean_not,
    extension_function,
    routine_expression,
    cast,
    case,
    now,
    current_date,
    typed_datetime_literal,
    uuid_v4,
    interval,
    case_fold,
    replace,
    regexp_replace,
    regexp_substr,
    regexp_match,
    regexp_count,
    regexp_instr,
    translate,
    concat,
    coalesce,
    nullif,
    text_length,
    ascii,
    chr,
    substring,
    overlay,
    split_part,
    strpos,
    left_right,
    pad,
    repeat,
    reverse,
    md5,
    starts_with,
    ends_with,
    date_trunc,
    date_bin,
    date_part,
    abs,
    round,
    trunc,
    floor,
    ceil,
    sqrt,
    sign,
    mod,
    power,
    greatest_least,
    json_extract_path,
    json_typeof,
    json_array_length,
    json_build_object,
    to_jsonb,
    convert_from,
    array_length,
    array_position,
    array_element_transform,
    array_to_string,
    string_to_array,
};

pub const RowExpressionFieldOperandParserHooks = struct {
    ptr: *anyopaque,
    parse_json_array_value: *const fn (*anyopaque) anyerror![]const u8,
};

pub fn rowExpressionOperandStartAt(tokens: []const Token, pos: usize) ?RowExpressionOperandStart {
    if (peekParenthesizedExpressionSyntax(tokens, pos)) return .parenthesized;
    if (peekUnaryNegativeExpressionSyntax(tokens, pos)) return .unary_negative;
    if (peekBooleanNotExpressionSyntax(tokens, pos)) return .boolean_not;
    if (peekCastExpressionSyntax(tokens, pos)) return .cast;
    if (peekCaseExpressionSyntax(tokens, pos)) return .case;
    if (peekSqlNowExpressionSyntax(tokens, pos)) return .now;
    if (peekSqlCurrentDateExpressionSyntax(tokens, pos)) return .current_date;
    if (peekSqlTypedDatetimeLiteral(tokens, pos)) return .typed_datetime_literal;
    if (peekFunctionCallIf(tokens, pos, sqlKeywordIsUuidV4Function)) return .uuid_v4;
    if (peekSqlIntervalExpressionSyntax(tokens, pos)) return .interval;
    if (peekCaseFoldFunctionCall(tokens, pos)) return .case_fold;
    if (peekReplaceFunctionCall(tokens, pos)) return .replace;
    if (peekRegexpReplaceFunctionCall(tokens, pos)) return .regexp_replace;
    if (peekFunctionCallIf(tokens, pos, sqlKeywordIsRegexpSubstrFunction)) return .regexp_substr;
    if (peekFunctionCallIf(tokens, pos, sqlKeywordIsRegexpMatchFunction)) return .regexp_match;
    if (peekFunctionCallIf(tokens, pos, sqlKeywordIsRegexpCountFunction)) return .regexp_count;
    if (peekFunctionCallIf(tokens, pos, sqlKeywordIsRegexpInstrFunction)) return .regexp_instr;
    if (peekFunctionCallIf(tokens, pos, sqlKeywordIsTranslateFunction)) return .translate;
    if (peekConcatFunctionCall(tokens, pos)) return .concat;
    if (peekCoalesceFunctionCall(tokens, pos)) return .coalesce;
    if (peekNullifFunctionCall(tokens, pos)) return .nullif;
    if (peekTextLengthFunctionKeyword(tokens, pos)) return .text_length;
    if (peekFunctionCallIf(tokens, pos, sqlKeywordIsAsciiFunction)) return .ascii;
    if (peekFunctionCallIf(tokens, pos, sqlKeywordIsChrFunction)) return .chr;
    if (peekSubstringFunctionKeyword(tokens, pos)) return .substring;
    if (peekFunctionCallIf(tokens, pos, sqlKeywordIsOverlayFunction)) return .overlay;
    if (peekSplitPartFunctionKeyword(tokens, pos)) return .split_part;
    if (peekStrposFunctionKeyword(tokens, pos) or peekPositionFunctionSyntax(tokens, pos)) return .strpos;
    if (peekFunctionCallIf(tokens, pos, sqlKeywordIsLeftRightFunction)) return .left_right;
    if (peekFunctionCallIf(tokens, pos, sqlKeywordIsPadFunction)) return .pad;
    if (peekFunctionCallIf(tokens, pos, sqlKeywordIsRepeatFunction)) return .repeat;
    if (peekFunctionCallIf(tokens, pos, sqlKeywordIsReverseFunction)) return .reverse;
    if (peekFunctionCallIf(tokens, pos, sqlKeywordIsMd5Function)) return .md5;
    if (peekFunctionCallIf(tokens, pos, sqlKeywordIsStartsWithFunction)) return .starts_with;
    if (peekFunctionCallIf(tokens, pos, sqlKeywordIsEndsWithFunction)) return .ends_with;
    if (peekFunctionCallIf(tokens, pos, sqlKeywordIsDateTruncFunction)) return .date_trunc;
    if (peekFunctionCallIf(tokens, pos, sqlKeywordIsDateBinFunction)) return .date_bin;
    if (peekFunctionCallIf(tokens, pos, sqlKeywordIsDatePartFunction)) return .date_part;
    if (peekFixedUnaryFunctionCall(tokens, pos, .abs)) return .abs;
    if (peekFixedUnaryFunctionCall(tokens, pos, .round)) return .round;
    if (peekFixedUnaryFunctionCall(tokens, pos, .trunc)) return .trunc;
    if (peekFixedUnaryFunctionCall(tokens, pos, .floor)) return .floor;
    if (peekFixedUnaryFunctionCall(tokens, pos, .ceil)) return .ceil;
    if (peekFixedUnaryFunctionCall(tokens, pos, .sqrt)) return .sqrt;
    if (peekFixedUnaryFunctionCall(tokens, pos, .sign)) return .sign;
    if (peekFixedBinaryFunctionCall(tokens, pos, .mod)) return .mod;
    if (peekFixedBinaryFunctionCall(tokens, pos, .power)) return .power;
    if (peekGreatestLeastFunctionCall(tokens, pos)) return .greatest_least;
    if (peekFunctionCallIf(tokens, pos, sqlKeywordIsJsonExtractPathFunction)) return .json_extract_path;
    if (peekFunctionCallIf(tokens, pos, sqlKeywordIsJsonTypeofFunction)) return .json_typeof;
    if (peekFunctionCallIf(tokens, pos, sqlKeywordIsJsonArrayLengthFunction)) return .json_array_length;
    if (peekFunctionCallIf(tokens, pos, sqlKeywordIsJsonBuildObjectFunction)) return .json_build_object;
    if (value_mod.peekToJsonbFunctionCall(tokens, pos)) return .to_jsonb;
    if (value_mod.peekConvertFromFunctionCall(tokens, pos)) return .convert_from;
    if (functionCallStartsAtIf(tokens, pos, sqlKeywordIsArrayLengthFunction)) return .array_length;
    if (functionCallStartsAtIf(tokens, pos, sqlKeywordIsArrayPositionFunction)) return .array_position;
    if (peekArrayElementTransformFunctionCall(tokens, pos)) return .array_element_transform;
    if (peekArrayToStringFunctionCall(tokens, pos)) return .array_to_string;
    if (peekStringToArrayFunctionCall(tokens, pos)) return .string_to_array;
    return null;
}

pub fn rowExpressionOperandStartWithFunctionBindingsAt(tokens: []const Token, pos: usize, bindings: SqlFunctionBindings) ?RowExpressionOperandStart {
    if (rowExpressionOperandStartAt(tokens, pos)) |start| return start;
    if (peekExtensionFunctionCall(tokens, pos, bindings.extension_functions)) return .extension_function;
    if (peekRoutineExpressionCall(tokens, pos, bindings.routine_expressions)) return .routine_expression;
    return null;
}

pub fn parseRowExpressionFieldOperandOrNullAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    schema: runtime_schema.TableSchema,
    joined_source_schema: ?runtime_schema.TableSchema,
    params: []const value_mod.SqlValue,
    field_expression_qualifiers: []const []const u8,
    returning_expression_qualifiers: []const []const u8,
    joined_source_expression_qualifiers: []const []const u8,
    joined_target_expression_qualifiers: []const []const u8,
    defer_row_expression_field_validation: bool,
    field_source: db_mod.types.RelationalRowsExpressionFieldSource,
    hooks: RowExpressionFieldOperandParserHooks,
) !?db_mod.types.RelationalRowsExpression {
    if (!parser.peekKind(tokens, pos.*, .identifier) or
        parser.peekKeyword(tokens, pos.*, "null") or
        parser.peekKeyword(tokens, pos.*, "true") or
        parser.peekKeyword(tokens, pos.*, "false"))
    {
        return null;
    }
    if (pos.* + 1 < tokens.len and tokens[pos.* + 1].kind == .lparen) return error.UnsupportedSqlShape;
    const parsed_field = try parseRowExpressionFieldOwnedAlloc(
        alloc,
        tokens,
        pos,
        schema,
        field_expression_qualifiers,
        returning_expression_qualifiers,
        defer_row_expression_field_validation,
    );
    defer alloc.free(parsed_field);
    const resolved = try binder.resolveRowExpressionFieldAlloc(
        alloc,
        schema,
        joined_source_schema,
        parsed_field,
        field_expression_qualifiers,
        returning_expression_qualifiers,
        joined_source_expression_qualifiers,
        joined_target_expression_qualifiers,
        defer_row_expression_field_validation,
        field_source,
    );
    var field_transferred = false;
    errdefer if (!field_transferred) alloc.free(resolved.field);
    if (matchJsonExtractOperator(tokens, pos)) |operator| {
        const as_text = tokenKindIsJsonExtractTextOperator(operator);
        if (!defer_row_expression_field_validation and binder.relationalColumnForField(resolved.schema, resolved.field, .json) == null) return error.InvalidSqlCatalog;
        const path = try value_mod.parseJsonExtractOperatorPathOwnedAlloc(alloc, tokens, pos, params, operator);
        var path_transferred = false;
        errdefer if (!path_transferred) alloc.free(path);
        const expression = try buildJsonExtractExpressionAlloc(
            alloc,
            .{ .kind = .field, .field = resolved.field, .field_source = resolved.source },
            path,
            as_text,
        );
        field_transferred = true;
        path_transferred = true;
        return expression;
    }
    if (parser.matchToken(tokens, pos, .question) != null) {
        if (!defer_row_expression_field_validation and binder.relationalColumnForField(resolved.schema, resolved.field, .json) == null) return error.InvalidSqlCatalog;
        const path = try value_mod.parseJsonPathOwnedAlloc(alloc, tokens, pos, params);
        var path_transferred = false;
        errdefer if (!path_transferred) alloc.free(path);
        const expression = try buildJsonPathExistsExpressionAlloc(
            alloc,
            .{ .kind = .field, .field = resolved.field, .field_source = resolved.source },
            path,
        );
        field_transferred = true;
        path_transferred = true;
        return expression;
    }
    if (parser.matchToken(tokens, pos, .question_any) != null or parser.matchToken(tokens, pos, .question_all) != null) {
        const match_all = tokens[pos.* - 1].kind == .question_all;
        if (!defer_row_expression_field_validation and binder.relationalColumnForField(resolved.schema, resolved.field, .json) == null) return error.InvalidSqlCatalog;
        const values_json = try hooks.parse_json_array_value(hooks.ptr);
        defer alloc.free(values_json);
        const expression = try buildJsonKeySetExistsExpressionAlloc(alloc, resolved.field, resolved.source, match_all, values_json);
        field_transferred = true;
        alloc.free(resolved.field);
        return expression;
    }
    if (!defer_row_expression_field_validation and binder.relationalColumnForField(resolved.schema, resolved.field, null) == null) return error.InvalidSqlCatalog;
    field_transferred = true;
    return .{ .kind = .field, .field = resolved.field, .field_source = resolved.source };
}

pub fn peekWindowOutputOrderExpression(tokens: []const Token, pos: usize) bool {
    return peekAggregateOutputOrderExpression(tokens, pos);
}

pub fn peekAggregateHavingExpression(tokens: []const Token, pos: usize) bool {
    if (nextIsAggregateFunction(tokens, pos)) return false;
    if (pos >= tokens.len) return false;
    const token = tokens[pos];
    if (peekParenthesizedExpressionCondition(tokens, pos)) return true;
    if (token.kind == .identifier) {
        if (pos + 1 >= tokens.len) return false;
        const next = tokens[pos + 1];
        if (next.kind == .eq or next.kind == .neq or next.kind == .gt or next.kind == .gte or next.kind == .lt or next.kind == .lte) return false;
        if (next.kind == .identifier and std.ascii.eqlIgnoreCase(next.text, "is")) return false;
        return true;
    }
    return token.kind == .minus or
        token.kind == .number or
        parser.peekKeyword(tokens, pos, "lower") or
        parser.peekKeyword(tokens, pos, "upper") or
        parser.peekKeyword(tokens, pos, "trim") or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsTrimVariantFunction) or
        parser.peekKeyword(tokens, pos, "replace") or
        parser.peekKeyword(tokens, pos, "regexp_replace") or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsRegexpSubstrFunction) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsRegexpMatchFunction) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsRegexpCountFunction) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsRegexpInstrFunction) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsTranslateFunction) or
        parser.peekKeyword(tokens, pos, "case") or
        parser.peekKeyword(tokens, pos, "cast") or
        parser.peekKeyword(tokens, pos, "coalesce") or
        parser.peekKeyword(tokens, pos, "nullif") or
        peekTextLengthFunctionKeyword(tokens, pos) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsAsciiFunction) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsChrFunction) or
        peekSubstringFunctionKeyword(tokens, pos) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsOverlayFunction) or
        peekSplitPartFunctionKeyword(tokens, pos) or
        peekStrposFunctionKeyword(tokens, pos) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsLeftRightFunction) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsPadFunction) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsRepeatFunction) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsReverseFunction) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsMd5Function) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsStartsWithFunction) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsEndsWithFunction) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsDateTruncFunction) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsDateBinFunction) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsDatePartFunction) or
        parser.peekKeyword(tokens, pos, "position") or
        parser.peekKeyword(tokens, pos, "abs") or
        parser.peekKeyword(tokens, pos, "round") or
        peekFunctionCall(tokens, pos, "trunc") or
        peekFunctionCall(tokens, pos, "floor") or
        peekFunctionCall(tokens, pos, "ceil") or
        peekFunctionCall(tokens, pos, "sqrt") or
        peekFunctionCall(tokens, pos, "sign") or
        peekFunctionCall(tokens, pos, "mod") or
        peekFunctionCall(tokens, pos, "power") or
        parser.peekKeyword(tokens, pos, "greatest") or
        parser.peekKeyword(tokens, pos, "least");
}

pub fn appendExpressionValuesJsonConjunction(
    alloc: std.mem.Allocator,
    type_context: RowExpressionTypeContext,
    expression_predicates: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionCondition),
    lhs: db_mod.types.RelationalRowsExpression,
    values_json: []const u8,
) !void {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, values_json, .{}) catch return error.UnsupportedSqlShape;
    defer parsed.deinit();
    if (parsed.value != .array or parsed.value.array.items.len == 0 or parsed.value.array.items.len > max_scalar_or_expanded_branches) return error.UnsupportedSqlShape;
    try validateExpressionScalarMembershipValues(type_context, lhs, parsed.value);

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
    type_context: RowExpressionTypeContext,
    expression_or_predicates: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionPredicateGroup),
    lhs: db_mod.types.RelationalRowsExpression,
    values_json: []const u8,
) !void {
    try appendExpressionValuesJsonComparisonGroups(alloc, type_context, expression_or_predicates, lhs, values_json, .eq);
}

pub fn appendExpressionValuesJsonConjunctionGroup(
    alloc: std.mem.Allocator,
    type_context: RowExpressionTypeContext,
    expression_or_predicates: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionPredicateGroup),
    lhs: db_mod.types.RelationalRowsExpression,
    values_json: []const u8,
    op: runtime_schema.RelationalCheckOp,
) !void {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, values_json, .{}) catch return error.UnsupportedSqlShape;
    defer parsed.deinit();
    if (parsed.value != .array or parsed.value.array.items.len == 0 or parsed.value.array.items.len > max_scalar_or_expanded_branches) return error.UnsupportedSqlShape;
    try validateExpressionScalarMembershipValues(type_context, lhs, parsed.value);

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
    type_context: RowExpressionTypeContext,
    expression_or_predicates: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionPredicateGroup),
    lhs: db_mod.types.RelationalRowsExpression,
    values_json: []const u8,
    op: runtime_schema.RelationalCheckOp,
) !void {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, values_json, .{}) catch return error.UnsupportedSqlShape;
    defer parsed.deinit();
    if (parsed.value != .array or parsed.value.array.items.len == 0 or parsed.value.array.items.len > max_scalar_or_expanded_branches) return error.UnsupportedSqlShape;
    try validateExpressionScalarMembershipValues(type_context, lhs, parsed.value);

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

pub fn validateExpressionScalarMembershipValues(
    type_context: RowExpressionTypeContext,
    lhs: db_mod.types.RelationalRowsExpression,
    values: std.json.Value,
) !void {
    if (values != .array) return error.UnsupportedSqlShape;
    const lhs_type = try type_context.rowExpressionOutputType(lhs);
    if (lhs_type == .array or lhs_type == .json or lhs_type == .embedding) return error.UnsupportedSqlShape;
    for (values.array.items) |value| {
        if (!value_mod.sqlScalarValueMatches(lhs_type, value)) return error.UnsupportedSqlShape;
    }
}

pub fn validateExpressionConditionTypes(
    type_context: RowExpressionTypeContext,
    defer_row_expression_field_validation: bool,
    lhs: db_mod.types.RelationalRowsExpression,
    op: runtime_schema.RelationalCheckOp,
    rhs: []const db_mod.types.RelationalRowsExpression,
) !void {
    if (defer_row_expression_field_validation) {
        switch (op) {
            .is_null, .is_not_null => if (rhs.len != 0) return error.UnsupportedSqlShape,
            .eq, .ne, .is_distinct, .is_not_distinct, .gt, .gte, .lt, .lte => if (rhs.len != 1) return error.UnsupportedSqlShape,
        }
        return;
    }
    switch (op) {
        .is_null, .is_not_null => {
            if (rhs.len != 0) return error.UnsupportedSqlShape;
            return;
        },
        .eq, .ne, .is_distinct, .is_not_distinct, .gt, .gte, .lt, .lte => {
            if (rhs.len != 1) return error.UnsupportedSqlShape;
        },
    }
    const lhs_type = try type_context.rowExpressionOutputType(lhs);
    const rhs_expression = rhs[0];
    if (try type_context.rowExpressionIsNullLiteral(rhs_expression)) return;
    const rhs_type = try type_context.rowExpressionOutputType(rhs_expression);
    if (!sqlExpressionTypesComparable(lhs_type, rhs_type)) return error.UnsupportedSqlShape;
    switch (op) {
        .gt, .gte, .lt, .lte => if (!sqlExpressionTypeIsOrderable(lhs_type) or !sqlExpressionTypeIsOrderable(rhs_type)) return error.UnsupportedSqlShape,
        else => {},
    }
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

pub fn buildUnaryNegativeExpressionAlloc(
    alloc: std.mem.Allocator,
    operand: db_mod.types.RelationalRowsExpression,
) !db_mod.types.RelationalRowsExpression {
    const negative_one = try alloc.dupe(u8, "-1");
    var negative_one_transferred = false;
    errdefer if (!negative_one_transferred) alloc.free(negative_one);
    const operands = try alloc.alloc(db_mod.types.RelationalRowsExpression, 2);
    var operands_transferred = false;
    errdefer if (!operands_transferred) alloc.free(operands);
    operands[0] = .{ .kind = .value, .value_json = negative_one };
    operands[1] = operand;
    negative_one_transferred = true;
    operands_transferred = true;
    return .{
        .kind = .mul,
        .operands = operands,
    };
}

pub fn buildCurrentDateExpressionAlloc(
    alloc: std.mem.Allocator,
) !db_mod.types.RelationalRowsExpression {
    const operands = try alloc.alloc(db_mod.types.RelationalRowsExpression, 2);
    var initialized: usize = 0;
    errdefer {
        for (operands[0..initialized]) |operand| freeExpression(alloc, operand);
        alloc.free(operands);
    }

    operands[0] = .{
        .kind = .value,
        .value_json = try std.json.Stringify.valueAlloc(alloc, "day", .{}),
    };
    initialized += 1;
    operands[1] = try buildNowRowExpressionAlloc(alloc);
    initialized += 1;

    return .{
        .kind = .date_trunc,
        .operands = operands,
    };
}

pub fn buildCastExpressionAlloc(
    alloc: std.mem.Allocator,
    operand: db_mod.types.RelationalRowsExpression,
    cast_type: db_mod.types.RelationalRowsExpressionCastType,
) !db_mod.types.RelationalRowsExpression {
    const operands = try alloc.alloc(db_mod.types.RelationalRowsExpression, 1);
    var operands_transferred = false;
    errdefer if (!operands_transferred) alloc.free(operands);
    operands[0] = operand;

    operands_transferred = true;
    return .{
        .kind = .cast,
        .operands = operands,
        .cast_type = cast_type,
    };
}

pub fn buildUnaryFunctionExpressionAlloc(
    alloc: std.mem.Allocator,
    kind: db_mod.types.RelationalRowsExpressionKind,
    operand: db_mod.types.RelationalRowsExpression,
) !db_mod.types.RelationalRowsExpression {
    const operands = try alloc.alloc(db_mod.types.RelationalRowsExpression, 1);
    var operands_transferred = false;
    errdefer if (!operands_transferred) alloc.free(operands);
    operands[0] = operand;
    operands_transferred = true;
    return .{
        .kind = kind,
        .operands = operands,
    };
}

pub fn buildBinaryFunctionExpressionAlloc(
    alloc: std.mem.Allocator,
    kind: db_mod.types.RelationalRowsExpressionKind,
    lhs: db_mod.types.RelationalRowsExpression,
    rhs: db_mod.types.RelationalRowsExpression,
) !db_mod.types.RelationalRowsExpression {
    const operands = try alloc.alloc(db_mod.types.RelationalRowsExpression, 2);
    var operands_transferred = false;
    errdefer if (!operands_transferred) alloc.free(operands);
    operands[0] = lhs;
    operands[1] = rhs;
    operands_transferred = true;
    return .{
        .kind = kind,
        .operands = operands,
    };
}

pub fn buildTernaryFunctionExpressionAlloc(
    alloc: std.mem.Allocator,
    kind: db_mod.types.RelationalRowsExpressionKind,
    first: db_mod.types.RelationalRowsExpression,
    second: db_mod.types.RelationalRowsExpression,
    third: db_mod.types.RelationalRowsExpression,
) !db_mod.types.RelationalRowsExpression {
    const operands = try alloc.alloc(db_mod.types.RelationalRowsExpression, 3);
    var operands_transferred = false;
    errdefer if (!operands_transferred) alloc.free(operands);
    operands[0] = first;
    operands[1] = second;
    operands[2] = third;
    operands_transferred = true;
    return .{
        .kind = kind,
        .operands = operands,
    };
}

pub fn buildFunctionExpressionFromOperandListAlloc(
    alloc: std.mem.Allocator,
    kind: db_mod.types.RelationalRowsExpressionKind,
    operands: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpression),
) !db_mod.types.RelationalRowsExpression {
    const owned_operands = try operands.toOwnedSlice(alloc);
    operands.* = .empty;
    return .{
        .kind = kind,
        .operands = owned_operands,
    };
}

pub fn buildExpressionProjection(
    output: []const u8,
    expression: db_mod.types.RelationalRowsExpression,
) db_mod.types.RelationalRowsExpressionProjection {
    return .{
        .output = output,
        .expression = expression,
    };
}

pub fn buildJsonExtractExpressionAlloc(
    alloc: std.mem.Allocator,
    operand: db_mod.types.RelationalRowsExpression,
    path: []const u8,
    as_text: bool,
) !db_mod.types.RelationalRowsExpression {
    const operands = try alloc.alloc(db_mod.types.RelationalRowsExpression, 1);
    var operands_transferred = false;
    errdefer if (!operands_transferred) alloc.free(operands);
    operands[0] = operand;
    operands_transferred = true;
    return .{
        .kind = .json_extract,
        .json_path = path,
        .json_as_text = as_text,
        .operands = operands,
    };
}

pub fn buildJsonPathExistsExpressionAlloc(
    alloc: std.mem.Allocator,
    operand: db_mod.types.RelationalRowsExpression,
    path: []const u8,
) !db_mod.types.RelationalRowsExpression {
    const operands = try alloc.alloc(db_mod.types.RelationalRowsExpression, 1);
    var operands_transferred = false;
    errdefer if (!operands_transferred) alloc.free(operands);
    operands[0] = operand;
    operands_transferred = true;
    return .{
        .kind = .json_path_exists,
        .json_path = path,
        .operands = operands,
    };
}

pub fn buildJsonKeySetExistsExpressionAlloc(
    alloc: std.mem.Allocator,
    field: []const u8,
    source: db_mod.types.RelationalRowsExpressionFieldSource,
    match_all: bool,
    values_json: []const u8,
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
        if (value != .string or value.string.len == 0 or std.mem.indexOfScalar(u8, value.string, '.') != null) return error.UnsupportedSqlShape;
        const owned_field = try alloc.dupe(u8, field);
        var field_transferred = false;
        errdefer if (!field_transferred) alloc.free(owned_field);
        const owned_path = try alloc.dupe(u8, value.string);
        var path_transferred = false;
        errdefer if (!path_transferred) alloc.free(owned_path);
        const exists_operands = try alloc.alloc(db_mod.types.RelationalRowsExpression, 1);
        var exists_operands_transferred = false;
        errdefer if (!exists_operands_transferred) alloc.free(exists_operands);
        exists_operands[0] = .{ .kind = .field, .field = owned_field, .field_source = source };
        try operands.append(alloc, .{
            .kind = .json_path_exists,
            .json_path = owned_path,
            .operands = exists_operands,
        });
        field_transferred = true;
        path_transferred = true;
        exists_operands_transferred = true;
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
        .kind = if (match_all) .bool_and else .bool_or,
        .operands = owned_operands,
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
    type_context: RowExpressionTypeContext,
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

pub fn expressionRegexpMatchConditionAlloc(
    alloc: std.mem.Allocator,
    type_context: RowExpressionTypeContext,
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
    type_context: RowExpressionTypeContext,
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

pub fn peekConflictExistingFieldIncrement(
    tokens: []const Token,
    pos: usize,
    field: []const u8,
    column: runtime_schema.RelationalColumn,
) bool {
    if (column.field_type != .numeric) return false;
    if (pos + 1 >= tokens.len or tokens[pos].kind != .identifier) return false;
    if (!std.mem.eql(u8, tokens[pos].text, field)) return false;
    const op = tokens[pos + 1].kind;
    if (op != .plus and op != .minus) return false;
    return !peekArithmeticRhsKeyword(tokens, pos, "interval");
}

pub fn peekSimpleScalarSetPredicate(tokens: []const Token, pos: usize) bool {
    if (pos + 1 >= tokens.len or tokens[pos].kind != .identifier) return false;
    if (pos + 2 < tokens.len and
        tokens[pos + 1].kind == .identifier and
        std.ascii.eqlIgnoreCase(tokens[pos + 1].text, "not") and
        tokens[pos + 2].kind == .identifier and
        std.ascii.eqlIgnoreCase(tokens[pos + 2].text, "in"))
    {
        return true;
    }
    if (tokens[pos + 1].kind == .identifier and std.ascii.eqlIgnoreCase(tokens[pos + 1].text, "in")) return true;
    if (pos + 2 >= tokens.len) return false;
    if (tokens[pos + 1].kind != .eq and tokens[pos + 1].kind != .neq) return false;
    return tokens[pos + 2].kind == .identifier and
        (sqlKeywordIsAnyOrSome(tokens[pos + 2].text) or
            std.ascii.eqlIgnoreCase(tokens[pos + 2].text, "all"));
}

pub fn canParseScalarNotWhere(tokens: []const Token, pos: usize) bool {
    if (!parser.peekKeyword(tokens, pos, "not") or pos + 1 >= tokens.len or tokens[pos + 1].kind != .lparen) return false;
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
                if (std.ascii.eqlIgnoreCase(token.text, "any") or
                    std.ascii.eqlIgnoreCase(token.text, "some") or
                    std.ascii.eqlIgnoreCase(token.text, "between") or
                    std.ascii.eqlIgnoreCase(token.text, "in") or
                    std.ascii.eqlIgnoreCase(token.text, "exists"))
                {
                    return false;
                }
                if (std.ascii.eqlIgnoreCase(token.text, "not")) {
                    if (i == 0 or tokens[i - 1].kind != .identifier or
                        !std.ascii.eqlIgnoreCase(tokens[i - 1].text, "is"))
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

pub fn expressionCanStartAt(tokens: []const Token, index: usize) bool {
    if (index >= tokens.len) return false;
    const token = tokens[index];
    if (token.kind == .minus) return true;
    if (token.kind == .number or token.kind == .string or token.kind == .placeholder) return true;
    if (token.kind != .identifier) return false;
    if (functionCallStartsAtIf(tokens, index, sqlKeywordIsArrayLengthFunction) or
        functionCallStartsAtIf(tokens, index, sqlKeywordIsArrayPositionFunction) or
        std.ascii.eqlIgnoreCase(token.text, "case") or
        std.ascii.eqlIgnoreCase(token.text, "cast") or
        std.ascii.eqlIgnoreCase(token.text, "coalesce") or
        std.ascii.eqlIgnoreCase(token.text, "concat") or
        std.ascii.eqlIgnoreCase(token.text, "concat_ws") or
        std.ascii.eqlIgnoreCase(token.text, "convert_from") or
        std.ascii.eqlIgnoreCase(token.text, "current_date") or
        std.ascii.eqlIgnoreCase(token.text, "current_timestamp") or
        functionCallStartsAt(tokens, index, "gen_random_uuid") or
        functionCallStartsAt(tokens, index, "uuid_generate_v4") or
        std.ascii.eqlIgnoreCase(token.text, "false") or
        std.ascii.eqlIgnoreCase(token.text, "abs") or
        std.ascii.eqlIgnoreCase(token.text, "ascii") or
        functionCallStartsAt(tokens, index, "floor") or
        functionCallStartsAt(tokens, index, "ceil") or
        functionCallStartsAt(tokens, index, "sqrt") or
        functionCallStartsAt(tokens, index, "sign") or
        functionCallStartsAt(tokens, index, "mod") or
        functionCallStartsAt(tokens, index, "power") or
        std.ascii.eqlIgnoreCase(token.text, "chr") or
        std.ascii.eqlIgnoreCase(token.text, "length") or
        std.ascii.eqlIgnoreCase(token.text, "bit_length") or
        std.ascii.eqlIgnoreCase(token.text, "lower") or
        std.ascii.eqlIgnoreCase(token.text, "now") or
        std.ascii.eqlIgnoreCase(token.text, "null") or
        std.ascii.eqlIgnoreCase(token.text, "nullif") or
        std.ascii.eqlIgnoreCase(token.text, "octet_length") or
        sqlKeywordIsOverlayFunction(token.text) or
        std.ascii.eqlIgnoreCase(token.text, "round") or
        functionCallStartsAt(tokens, index, "trunc") or
        std.ascii.eqlIgnoreCase(token.text, "array_append") or
        std.ascii.eqlIgnoreCase(token.text, "array_prepend") or
        std.ascii.eqlIgnoreCase(token.text, "array_cat") or
        std.ascii.eqlIgnoreCase(token.text, "array_remove") or
        std.ascii.eqlIgnoreCase(token.text, "array_replace") or
        std.ascii.eqlIgnoreCase(token.text, "array_to_string") or
        std.ascii.eqlIgnoreCase(token.text, "string_to_array") or
        std.ascii.eqlIgnoreCase(token.text, "trim") or
        sqlKeywordIsTrimVariantFunction(token.text) or
        sqlKeywordIsPadFunction(token.text) or
        sqlKeywordIsRepeatFunction(token.text) or
        sqlKeywordIsReverseFunction(token.text) or
        sqlKeywordIsInitcapFunction(token.text) or
        sqlKeywordIsMd5Function(token.text) or
        sqlKeywordIsStartsWithFunction(token.text) or
        sqlKeywordIsEndsWithFunction(token.text) or
        sqlKeywordIsDateTruncFunction(token.text) or
        sqlKeywordIsDateBinFunction(token.text) or
        sqlKeywordIsDatePartFunction(token.text) or
        (sqlKeywordIsJsonExtractPathFunction(token.text) and index + 1 < tokens.len and tokens[index + 1].kind == .lparen) or
        (sqlKeywordIsJsonTypeofFunction(token.text) and index + 1 < tokens.len and tokens[index + 1].kind == .lparen) or
        (sqlKeywordIsJsonArrayLengthFunction(token.text) and index + 1 < tokens.len and tokens[index + 1].kind == .lparen) or
        (sqlKeywordIsJsonBuildObjectFunction(token.text) and index + 1 < tokens.len and tokens[index + 1].kind == .lparen) or
        (std.ascii.eqlIgnoreCase(token.text, "to_jsonb") and index + 1 < tokens.len and tokens[index + 1].kind == .lparen) or
        sqlKeywordIsRegexpMatchFunction(token.text) or
        sqlKeywordIsRegexpCountFunction(token.text) or
        sqlKeywordIsRegexpSubstrFunction(token.text) or
        sqlKeywordIsRegexpInstrFunction(token.text) or
        std.ascii.eqlIgnoreCase(token.text, "replace") or
        sqlKeywordIsTranslateFunction(token.text) or
        std.ascii.eqlIgnoreCase(token.text, "true") or
        std.ascii.eqlIgnoreCase(token.text, "upper"))
    {
        return true;
    }
    if (jsonExtractMembershipPredicateCanStartAt(tokens, index) or
        jsonExtractNullSafeDistinctPredicateCanStartAt(tokens, index) or
        jsonExtractNullTestPredicateCanStartAt(tokens, index))
    {
        return true;
    }
    return index + 1 < tokens.len and switch (tokens[index + 1].kind) {
        .plus, .minus, .star, .slash, .percent => true,
        else => false,
    };
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
                if (depth == 0 and sqlWhereTailClauseKeyword(token.text)) break;
                if (sqlKeywordStartsScalarPredicate(token.text)) return false;
                if (std.ascii.eqlIgnoreCase(token.text, "and") or
                    std.ascii.eqlIgnoreCase(token.text, "or") or
                    std.ascii.eqlIgnoreCase(token.text, "not"))
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
    return std.ascii.eqlIgnoreCase(token.text, "not") or
        std.ascii.eqlIgnoreCase(token.text, "true") or
        std.ascii.eqlIgnoreCase(token.text, "false") or
        std.ascii.eqlIgnoreCase(token.text, "case") or
        std.ascii.eqlIgnoreCase(token.text, "cast") or
        std.ascii.eqlIgnoreCase(token.text, "coalesce") or
        std.ascii.eqlIgnoreCase(token.text, "nullif") or
        sqlKeywordIsRegexpMatchFunction(token.text) or
        sqlKeywordIsStartsWithFunction(token.text) or
        binder.relationalColumnForField(schema, token.text, null) != null;
}

pub fn textPatternSetPredicateCanStartAt(tokens: []const Token, index: usize) bool {
    if (index >= tokens.len) return false;
    if (tokens[index].kind != .identifier and !expressionCanStartAt(tokens, index)) return false;
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
                if (std.ascii.eqlIgnoreCase(token.text, "like") or
                    std.ascii.eqlIgnoreCase(token.text, "ilike"))
                {
                    return tokenAtIsAnySomeOrAll(tokens, i + 1);
                }
                if (std.ascii.eqlIgnoreCase(token.text, "not") and i + 2 < tokens.len and
                    tokens[i + 1].kind == .identifier and
                    (std.ascii.eqlIgnoreCase(tokens[i + 1].text, "like") or
                        std.ascii.eqlIgnoreCase(tokens[i + 1].text, "ilike")))
                {
                    return tokenAtIsAnySomeOrAll(tokens, i + 2);
                }
                if (rowExpressionBoundaryKeyword(token.text)) return false;
            },
            else => {},
        }
    }
    return false;
}

pub fn expressionNullSafeDistinctPredicateCanStartAt(tokens: []const Token, index: usize) bool {
    if (!expressionCanStartAt(tokens, index)) return false;
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
            .identifier => if (depth == 0 and std.ascii.eqlIgnoreCase(token.text, "is")) {
                var distinct_index = i + 1;
                if (distinct_index < tokens.len and
                    tokens[distinct_index].kind == .identifier and
                    std.ascii.eqlIgnoreCase(tokens[distinct_index].text, "not"))
                {
                    distinct_index += 1;
                }
                return distinct_index + 1 < tokens.len and
                    tokens[distinct_index].kind == .identifier and
                    std.ascii.eqlIgnoreCase(tokens[distinct_index].text, "distinct") and
                    tokens[distinct_index + 1].kind == .identifier and
                    std.ascii.eqlIgnoreCase(tokens[distinct_index + 1].text, "from");
            } else if (depth == 0 and rowExpressionBoundaryKeyword(token.text)) {
                return false;
            },
            .semicolon, .comma => if (depth == 0) return false,
            else => {},
        }
    }
    return false;
}

pub fn canParseExpressionNotWhere(tokens: []const Token, pos: usize) bool {
    if (!parser.peekKeyword(tokens, pos, "not") or pos + 2 >= tokens.len or tokens[pos + 1].kind != .lparen) return false;
    const inner = parser.predicateStartIndexAfterOpenParens(tokens, pos + 1);
    return expressionCanStartAt(tokens, inner) or
        jsonKeySetExpressionCanStartAt(tokens, inner) or
        jsonExtractExpressionPredicateCanStartAt(tokens, inner) or
        jsonExtractNullTestPredicateCanStartAt(tokens, inner);
}

pub fn canParseAccessNotWhere(tokens: []const Token, pos: usize) bool {
    if (!parser.peekKeyword(tokens, pos, "not")) return false;
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
                if (std.ascii.eqlIgnoreCase(token.text, "like") or
                    std.ascii.eqlIgnoreCase(token.text, "ilike"))
                {
                    return true;
                }
                if (i + 2 < tokens.len and
                    (tokens[i + 1].kind == .eq or tokens[i + 1].kind == .neq) and
                    tokens[i + 2].kind == .identifier and
                    std.ascii.eqlIgnoreCase(tokens[i + 2].text, "array"))
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
            else => if (std.ascii.eqlIgnoreCase(tokens[pos + 1].text, "is") or
                std.ascii.eqlIgnoreCase(tokens[pos + 1].text, "not") or
                std.ascii.eqlIgnoreCase(tokens[pos + 1].text, "in") or
                std.ascii.eqlIgnoreCase(tokens[pos + 1].text, "between"))
            {
                if (binder.relationalColumnForField(schema, tokens[pos].text, null) != null) return false;
                return true;
            },
        }
    }
    if (rowExpressionHasTopLevelPipeConcat(tokens, pos)) return true;
    if (parser.peekKind(tokens, pos, .minus)) return true;
    if (parser.peekKeyword(tokens, pos, "lower") or parser.peekKeyword(tokens, pos, "upper")) {
        return !(try generatedExpressionCallHasGeneratedColumn(
            alloc,
            tokens,
            pos,
            schema,
            field_expression_qualifiers,
            returning_expression_qualifiers,
            defer_row_expression_field_validation,
        ));
    }
    if (peekFunctionCallIf(tokens, pos, sqlKeywordIsInitcapFunction)) return true;
    if (peekFunctionCallIf(tokens, pos, sqlKeywordIsMd5Function)) {
        return !(try generatedExpressionCallHasGeneratedColumn(
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
        return !(try generatedExpressionCallHasGeneratedColumn(
            alloc,
            tokens,
            pos,
            schema,
            field_expression_qualifiers,
            returning_expression_qualifiers,
            defer_row_expression_field_validation,
        ));
    }
    if (peekFunctionCallIf(tokens, pos, sqlKeywordIsJsonExtractPathFunction) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsJsonTypeofFunction) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsJsonArrayLengthFunction) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsJsonBuildObjectFunction) or
        functionCallStartsAtIf(tokens, pos, sqlKeywordIsArrayLengthFunction) or
        functionCallStartsAtIf(tokens, pos, sqlKeywordIsArrayPositionFunction) or
        parser.peekKeyword(tokens, pos, "array_append") or
        parser.peekKeyword(tokens, pos, "array_prepend") or
        parser.peekKeyword(tokens, pos, "array_cat") or
        parser.peekKeyword(tokens, pos, "array_remove") or
        parser.peekKeyword(tokens, pos, "array_replace") or
        parser.peekKeyword(tokens, pos, "array_to_string") or
        parser.peekKeyword(tokens, pos, "case") or
        parser.peekKeyword(tokens, pos, "cast") or
        parser.peekKeyword(tokens, pos, "coalesce") or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsRegexpMatchFunction) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsRegexpSubstrFunction) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsRegexpCountFunction) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsRegexpInstrFunction) or
        parser.peekKeyword(tokens, pos, "nullif") or
        peekTextLengthFunctionKeyword(tokens, pos) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsAsciiFunction) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsChrFunction) or
        peekSubstringFunctionKeyword(tokens, pos) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsOverlayFunction) or
        peekSplitPartFunctionKeyword(tokens, pos) or
        peekStrposFunctionKeyword(tokens, pos) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsLeftRightFunction) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsPadFunction) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsRepeatFunction) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsReverseFunction) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsStartsWithFunction) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsEndsWithFunction) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsDateTruncFunction) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsDateBinFunction) or
        peekFunctionCallIf(tokens, pos, sqlKeywordIsDatePartFunction) or
        parser.peekKeyword(tokens, pos, "position") or
        parser.peekKeyword(tokens, pos, "abs") or
        parser.peekKeyword(tokens, pos, "round") or
        peekFunctionCall(tokens, pos, "trunc") or
        peekFunctionCall(tokens, pos, "floor") or
        peekFunctionCall(tokens, pos, "ceil") or
        peekFunctionCall(tokens, pos, "sqrt") or
        peekFunctionCall(tokens, pos, "sign") or
        peekFunctionCall(tokens, pos, "mod") or
        peekFunctionCall(tokens, pos, "power") or
        parser.peekKeyword(tokens, pos, "greatest") or
        parser.peekKeyword(tokens, pos, "least")) return true;
    if (expressionNullSafeDistinctPredicateCanStartAt(tokens, pos)) return true;
    if (textPatternSetPredicateCanStartAt(tokens, pos)) return true;
    if (parser.peekKeyword(tokens, pos, "trim")) return true;
    if (peekFunctionCallIf(tokens, pos, sqlKeywordIsTrimVariantFunction)) return true;
    if (parser.peekKeyword(tokens, pos, "replace")) return true;
    if (parser.peekKeyword(tokens, pos, "regexp_replace")) return true;
    if (peekFunctionCallIf(tokens, pos, sqlKeywordIsRegexpMatchFunction)) return true;
    if (peekFunctionCallIf(tokens, pos, sqlKeywordIsRegexpCountFunction)) return true;
    if (peekFunctionCallIf(tokens, pos, sqlKeywordIsRegexpInstrFunction)) return true;
    if (peekFunctionCallIf(tokens, pos, sqlKeywordIsTranslateFunction)) return true;
    if (parser.peekKeyword(tokens, pos, "concat_ws")) return true;
    if (jsonExtractNullTestPredicateCanStartAt(tokens, pos)) return true;
    if (jsonExtractNullSafeDistinctPredicateCanStartAt(tokens, pos)) return true;
    if (jsonExtractMembershipPredicateCanStartAt(tokens, pos)) return true;
    if (jsonKeySetExpressionCanStartAt(tokens, pos)) return true;
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

fn generatedExpressionCallHasGeneratedColumn(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: usize,
    schema: runtime_schema.TableSchema,
    field_expression_qualifiers: []const []const u8,
    returning_expression_qualifiers: []const []const u8,
    defer_row_expression_field_validation: bool,
) !bool {
    if (parser.peekKeyword(tokens, pos, "concat") or parser.peekKeyword(tokens, pos, "concat_ws")) {
        return (try generatedConcatColumnAt(
            alloc,
            tokens,
            pos,
            schema,
            field_expression_qualifiers,
            returning_expression_qualifiers,
            defer_row_expression_field_validation,
        )) != null;
    }
    const op: runtime_schema.RelationalGeneratedOp = if (parser.peekKeyword(tokens, pos, "lower"))
        .lower
    else if (parser.peekKeyword(tokens, pos, "upper"))
        .upper
    else if (peekFunctionCallIf(tokens, pos, sqlKeywordIsMd5Function))
        .md5
    else
        return false;
    if (pos + 3 >= tokens.len) return false;
    if (tokens[pos + 1].kind != .lparen) return false;
    const field = tokens[pos + 2];
    if (field.kind != .identifier) return false;
    if (tokens[pos + 3].kind != .rparen) return false;
    const normalized_field = binder.normalizeRowExpressionFieldAlloc(
        alloc,
        schema,
        field.text,
        field_expression_qualifiers,
        returning_expression_qualifiers,
        defer_row_expression_field_validation,
    ) catch |err| switch (err) {
        error.InvalidSqlCatalog, error.UnsupportedSqlShape => return false,
        else => return err,
    };
    defer alloc.free(normalized_field);
    return generatedUnaryTextColumnForField(schema, op, normalized_field) != null;
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
    if (tokens[close + 1].kind != .identifier or
        !std.ascii.eqlIgnoreCase(tokens[close + 1].text, "is"))
    {
        return false;
    }
    var value_index = close + 2;
    if (tokens[value_index].kind == .identifier and
        std.ascii.eqlIgnoreCase(tokens[value_index].text, "not"))
    {
        value_index += 1;
    }
    if (value_index >= tokens.len or tokens[value_index].kind != .identifier) return false;
    return std.ascii.eqlIgnoreCase(tokens[value_index].text, "true") or
        std.ascii.eqlIgnoreCase(tokens[value_index].text, "false") or
        std.ascii.eqlIgnoreCase(tokens[value_index].text, "unknown");
}

pub fn expressionPredicateCanStartAt(tokens: []const Token, index: usize) bool {
    if (expressionCanStartAt(tokens, index) or
        textPatternSetPredicateCanStartAt(tokens, index) or
        jsonKeySetExpressionCanStartAt(tokens, index) or
        jsonExtractExpressionPredicateCanStartAt(tokens, index) or
        jsonExtractNullTestPredicateCanStartAt(tokens, index) or
        jsonExtractNullSafeDistinctPredicateCanStartAt(tokens, index) or
        jsonExtractMembershipPredicateCanStartAt(tokens, index))
    {
        return true;
    }
    if (index + 1 >= tokens.len or tokens[index].kind != .identifier) return false;
    const next = tokens[index + 1];
    return switch (next.kind) {
        .eq, .neq, .gt, .gte, .lt, .lte => true,
        .identifier => std.ascii.eqlIgnoreCase(next.text, "is") or
            std.ascii.eqlIgnoreCase(next.text, "in") or
            std.ascii.eqlIgnoreCase(next.text, "not") or
            std.ascii.eqlIgnoreCase(next.text, "between") or
            std.ascii.eqlIgnoreCase(next.text, "like") or
            std.ascii.eqlIgnoreCase(next.text, "ilike"),
        else => false,
    };
}

pub fn nextIsUnsupportedQueryKeyword(tokens: []const Token, pos: usize) bool {
    if (pos >= tokens.len or tokens[pos].kind != .identifier) return false;
    const token = tokens[pos].text;
    return std.ascii.eqlIgnoreCase(token, "join") or
        std.ascii.eqlIgnoreCase(token, "left") or
        std.ascii.eqlIgnoreCase(token, "inner") or
        std.ascii.eqlIgnoreCase(token, "group") or
        std.ascii.eqlIgnoreCase(token, "with") or
        std.ascii.eqlIgnoreCase(token, "over") or
        std.ascii.eqlIgnoreCase(token, "lateral");
}

pub fn rowExpressionBoundaryKeyword(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "as") or
        std.ascii.eqlIgnoreCase(text, "from") or
        std.ascii.eqlIgnoreCase(text, "where") or
        std.ascii.eqlIgnoreCase(text, "and") or
        std.ascii.eqlIgnoreCase(text, "or") or
        std.ascii.eqlIgnoreCase(text, "group") or
        std.ascii.eqlIgnoreCase(text, "having") or
        std.ascii.eqlIgnoreCase(text, "order") or
        std.ascii.eqlIgnoreCase(text, "limit") or
        std.ascii.eqlIgnoreCase(text, "offset") or
        std.ascii.eqlIgnoreCase(text, "fetch") or
        std.ascii.eqlIgnoreCase(text, "for") or
        std.ascii.eqlIgnoreCase(text, "asc") or
        std.ascii.eqlIgnoreCase(text, "desc") or
        std.ascii.eqlIgnoreCase(text, "nulls") or
        std.ascii.eqlIgnoreCase(text, "then") or
        std.ascii.eqlIgnoreCase(text, "else") or
        std.ascii.eqlIgnoreCase(text, "end") or
        std.ascii.eqlIgnoreCase(text, "when") or
        std.ascii.eqlIgnoreCase(text, "filter") or
        std.ascii.eqlIgnoreCase(text, "over");
}

pub fn sqlWhereTailClauseKeyword(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "order") or
        std.ascii.eqlIgnoreCase(text, "limit") or
        std.ascii.eqlIgnoreCase(text, "offset") or
        std.ascii.eqlIgnoreCase(text, "for") or
        std.ascii.eqlIgnoreCase(text, "group") or
        std.ascii.eqlIgnoreCase(text, "having") or
        std.ascii.eqlIgnoreCase(text, "returning") or
        std.ascii.eqlIgnoreCase(text, "join") or
        std.ascii.eqlIgnoreCase(text, "left") or
        std.ascii.eqlIgnoreCase(text, "inner") or
        std.ascii.eqlIgnoreCase(text, "with") or
        std.ascii.eqlIgnoreCase(text, "over") or
        std.ascii.eqlIgnoreCase(text, "lateral");
}

pub fn sqlWindowTailClauseKeyword(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "order") or
        std.ascii.eqlIgnoreCase(text, "limit") or
        std.ascii.eqlIgnoreCase(text, "offset") or
        std.ascii.eqlIgnoreCase(text, "fetch") or
        std.ascii.eqlIgnoreCase(text, "for");
}

pub fn parenthesizedPredicateGroupCanStartAt(tokens: []const Token, index: usize) bool {
    if (index >= tokens.len or tokens[index].kind != .lparen) return false;
    const close = parser.findMatchingRParenIndex(tokens, index) orelse return false;
    if (close + 1 >= tokens.len) return true;
    return !tokenContinuesScalarExpressionPredicate(tokens, close + 1);
}

fn tokenContinuesScalarExpressionPredicate(tokens: []const Token, index: usize) bool {
    const token = tokens[index];
    switch (token.kind) {
        .eq, .neq, .gt, .gte, .lt, .lte, .arrow_text, .arrow_json, .path_arrow_text, .path_arrow_json => return true,
        .identifier => {
            if (std.ascii.eqlIgnoreCase(token.text, "is") or
                std.ascii.eqlIgnoreCase(token.text, "like") or
                std.ascii.eqlIgnoreCase(token.text, "ilike") or
                std.ascii.eqlIgnoreCase(token.text, "in") or
                std.ascii.eqlIgnoreCase(token.text, "between"))
            {
                return true;
            }
            if (std.ascii.eqlIgnoreCase(token.text, "not") and index + 1 < tokens.len and tokens[index + 1].kind == .identifier) {
                const after_not = tokens[index + 1].text;
                if (std.ascii.eqlIgnoreCase(after_not, "like") or
                    std.ascii.eqlIgnoreCase(after_not, "ilike") or
                    std.ascii.eqlIgnoreCase(after_not, "in") or
                    std.ascii.eqlIgnoreCase(after_not, "between"))
                {
                    return true;
                }
            }
        },
        else => {},
    }
    return false;
}

test "sql adapter expression keyword predicates classify function and tail tokens" {
    try std.testing.expect(sqlKeywordIsAnyOrSome("SOME"));
    try std.testing.expect(sqlKeywordStartsScalarPredicate("between"));
    try std.testing.expect(sqlKeywordIsArrayLengthFunction("cardinality"));
    try std.testing.expectEqualStrings("cardinality", arrayLengthDefaultOutput("cardinality"));
    try std.testing.expect(sqlKeywordIsJsonExtractPathFunction("jsonb_extract_path_text"));
    try std.testing.expect(sqlJsonExtractPathFunctionAsText("json_extract_path_text"));
    try std.testing.expect(sqlKeywordIsRegexpMatchFunction("regexp_like"));
    try std.testing.expect(rowExpressionBoundaryKeyword("returning") == false);
    try std.testing.expect(sqlWhereTailClauseKeyword("returning"));
    try std.testing.expect(sqlWindowTailClauseKeyword("fetch"));

    const length_tokens = [_]Token{
        .{ .kind = .identifier, .text = "char_length" },
        .{ .kind = .lparen, .text = "(" },
    };
    var length_pos: usize = 0;
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.length, try parseTextLengthFunctionCallStart(length_tokens[0..], &length_pos));
    try std.testing.expectEqual(@as(usize, 2), length_pos);

    const abs_tokens = [_]Token{
        .{ .kind = .identifier, .text = "abs" },
        .{ .kind = .lparen, .text = "(" },
    };
    var abs_pos: usize = 0;
    try parseFixedUnaryFunctionCallStart(abs_tokens[0..], &abs_pos, .abs);
    try std.testing.expectEqual(@as(usize, 2), abs_pos);

    const mod_tokens = [_]Token{
        .{ .kind = .identifier, .text = "mod" },
        .{ .kind = .lparen, .text = "(" },
    };
    var mod_pos: usize = 0;
    try parseFixedBinaryFunctionCallStart(mod_tokens[0..], &mod_pos, .mod);
    try std.testing.expectEqual(@as(usize, 2), mod_pos);

    const greatest_tokens = [_]Token{
        .{ .kind = .identifier, .text = "greatest" },
        .{ .kind = .lparen, .text = "(" },
    };
    var greatest_pos: usize = 0;
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.greatest, try parseGreatestLeastFunctionCallStart(greatest_tokens[0..], &greatest_pos));
    try std.testing.expectEqual(@as(usize, 2), greatest_pos);
    try std.testing.expect(peekGreatestLeastFunctionCall(greatest_tokens[0..], 0));

    const case_fold_start_tokens = [_]Token{
        .{ .kind = .identifier, .text = "ltrim" },
        .{ .kind = .lparen, .text = "(" },
    };
    var case_fold_start_pos: usize = 0;
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.ltrim, try parseCaseFoldFunctionCallStart(case_fold_start_tokens[0..], &case_fold_start_pos));
    try std.testing.expectEqual(@as(usize, 2), case_fold_start_pos);

    const case_fold_tokens = [_]Token{
        .{ .kind = .identifier, .text = "upper" },
        .{ .kind = .lparen, .text = "(" },
    };
    try std.testing.expect(peekCaseFoldFunctionCall(case_fold_tokens[0..], 0));

    const case_tokens = [_]Token{
        .{ .kind = .identifier, .text = "case" },
        .{ .kind = .identifier, .text = "when" },
        .{ .kind = .identifier, .text = "then" },
        .{ .kind = .identifier, .text = "else" },
        .{ .kind = .identifier, .text = "end" },
    };
    try std.testing.expect(peekCaseExpressionSyntax(case_tokens[0..], 0));
    var case_pos: usize = 0;
    try parseCaseExpressionStart(case_tokens[0..], &case_pos);
    try std.testing.expectEqual(@as(usize, 1), case_pos);
    try std.testing.expect(matchCaseExpressionWhen(case_tokens[0..], &case_pos));
    try std.testing.expectEqual(@as(usize, 2), case_pos);
    try parseCaseExpressionThen(case_tokens[0..], &case_pos);
    try std.testing.expectEqual(@as(usize, 3), case_pos);
    try parseCaseExpressionElse(case_tokens[0..], &case_pos);
    try std.testing.expectEqual(@as(usize, 4), case_pos);
    try parseCaseExpressionEnd(case_tokens[0..], &case_pos);
    try std.testing.expectEqual(@as(usize, 5), case_pos);

    const cast_tokens = [_]Token{
        .{ .kind = .identifier, .text = "cast" },
        .{ .kind = .lparen, .text = "(" },
        .{ .kind = .identifier, .text = "field" },
        .{ .kind = .identifier, .text = "as" },
    };
    try std.testing.expect(peekCastExpressionSyntax(cast_tokens[0..], 0));
    var cast_pos: usize = 0;
    try parseCastExpressionCallStart(cast_tokens[0..], &cast_pos);
    try std.testing.expectEqual(@as(usize, 2), cast_pos);
    cast_pos = 3;
    try parseCastExpressionAs(cast_tokens[0..], &cast_pos);
    try std.testing.expectEqual(@as(usize, 4), cast_pos);

    const not_tokens = [_]Token{
        .{ .kind = .identifier, .text = "not" },
    };
    try std.testing.expect(peekBooleanNotExpressionSyntax(not_tokens[0..], 0));
    var not_pos: usize = 0;
    try parseBooleanNotExpressionStart(not_tokens[0..], &not_pos);
    try std.testing.expectEqual(@as(usize, 1), not_pos);

    const is_null_tokens = [_]Token{
        .{ .kind = .identifier, .text = "is" },
        .{ .kind = .identifier, .text = "not" },
        .{ .kind = .identifier, .text = "null" },
    };
    var is_null_pos: usize = 0;
    const is_null_tail = (try parseExpressionIsTailIf(is_null_tokens[0..], &is_null_pos, .{})).?;
    try std.testing.expectEqual(runtime_schema.RelationalCheckOp.is_not_null, is_null_tail.op);
    try std.testing.expectEqual(ExpressionIsTailKind.null_test, is_null_tail.kind);
    try std.testing.expectEqual(@as(usize, 3), is_null_pos);

    const is_distinct_tokens = [_]Token{
        .{ .kind = .identifier, .text = "is" },
        .{ .kind = .identifier, .text = "distinct" },
        .{ .kind = .identifier, .text = "from" },
    };
    var is_distinct_pos: usize = 0;
    const is_distinct_tail = (try parseExpressionIsTailIf(is_distinct_tokens[0..], &is_distinct_pos, .{})).?;
    try std.testing.expectEqual(runtime_schema.RelationalCheckOp.is_distinct, is_distinct_tail.op);
    try std.testing.expectEqual(ExpressionIsTailKind.distinct_comparison, is_distinct_tail.kind);
    try std.testing.expectEqual(@as(usize, 3), is_distinct_pos);
    is_distinct_pos = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseExpressionIsTailIf(is_distinct_tokens[0..], &is_distinct_pos, .{ .allow_distinct = false }));

    const is_unknown_tokens = [_]Token{
        .{ .kind = .identifier, .text = "is" },
        .{ .kind = .identifier, .text = "unknown" },
    };
    var is_unknown_pos: usize = 0;
    const is_unknown_tail = (try parseExpressionIsTailIf(is_unknown_tokens[0..], &is_unknown_pos, .{ .allow_boolean_unknown = true })).?;
    try std.testing.expectEqual(runtime_schema.RelationalCheckOp.is_null, is_unknown_tail.op);
    try std.testing.expectEqual(ExpressionIsTailKind.boolean_unknown, is_unknown_tail.kind);
    try std.testing.expectEqual(@as(usize, 2), is_unknown_pos);

    const is_true_tokens = [_]Token{
        .{ .kind = .identifier, .text = "is" },
        .{ .kind = .identifier, .text = "true" },
    };
    var is_true_pos: usize = 0;
    const is_true_tail = (try parseExpressionIsTailIf(is_true_tokens[0..], &is_true_pos, .{ .allow_boolean_literal = true })).?;
    try std.testing.expectEqual(runtime_schema.RelationalCheckOp.eq, is_true_tail.op);
    try std.testing.expectEqual(ExpressionIsTailKind.boolean_literal, is_true_tail.kind);
    try std.testing.expect(is_true_tail.boolean_value);
    try std.testing.expectEqual(@as(usize, 2), is_true_pos);

    const is_not_true_tokens = [_]Token{
        .{ .kind = .identifier, .text = "is" },
        .{ .kind = .identifier, .text = "not" },
        .{ .kind = .identifier, .text = "true" },
    };
    var is_not_true_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseExpressionIsTailIf(is_not_true_tokens[0..], &is_not_true_pos, .{ .allow_boolean_literal = true }));
    is_not_true_pos = 0;
    const is_not_true_tail = (try parseExpressionIsTailIf(is_not_true_tokens[0..], &is_not_true_pos, .{
        .allow_boolean_literal = true,
        .allow_boolean_literal_negation = true,
    })).?;
    try std.testing.expectEqual(ExpressionIsTailKind.boolean_literal, is_not_true_tail.kind);
    try std.testing.expect(is_not_true_tail.boolean_value);
    try std.testing.expect(is_not_true_tail.boolean_negated);
    try std.testing.expectEqual(@as(usize, 3), is_not_true_pos);

    const grouped_tokens = [_]Token{
        .{ .kind = .lparen, .text = "(" },
    };
    try std.testing.expect(peekParenthesizedExpressionSyntax(grouped_tokens[0..], 0));

    const negative_tokens = [_]Token{
        .{ .kind = .minus, .text = "-" },
    };
    try std.testing.expect(peekUnaryNegativeExpressionSyntax(negative_tokens[0..], 0));

    const concat_tokens = [_]Token{
        .{ .kind = .identifier, .text = "concat_ws" },
        .{ .kind = .lparen, .text = "(" },
    };
    var concat_pos: usize = 0;
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.concat_ws, try parseConcatFunctionCallStart(concat_tokens[0..], &concat_pos));
    try std.testing.expectEqual(@as(usize, 2), concat_pos);
    try std.testing.expect(peekConcatFunctionCall(concat_tokens[0..], 0));

    const coalesce_tokens = [_]Token{
        .{ .kind = .identifier, .text = "coalesce" },
        .{ .kind = .lparen, .text = "(" },
    };
    var coalesce_pos: usize = 0;
    try parseCoalesceFunctionCallStart(coalesce_tokens[0..], &coalesce_pos);
    try std.testing.expectEqual(@as(usize, 2), coalesce_pos);
    try std.testing.expect(peekCoalesceFunctionCall(coalesce_tokens[0..], 0));

    const nullif_tokens = [_]Token{
        .{ .kind = .identifier, .text = "nullif" },
        .{ .kind = .lparen, .text = "(" },
    };
    var nullif_pos: usize = 0;
    try parseNullifFunctionCallStart(nullif_tokens[0..], &nullif_pos);
    try std.testing.expectEqual(@as(usize, 2), nullif_pos);

    const replace_tokens = [_]Token{
        .{ .kind = .identifier, .text = "replace" },
        .{ .kind = .lparen, .text = "(" },
    };
    var replace_pos: usize = 0;
    try parseReplaceFunctionCallStart(replace_tokens[0..], &replace_pos);
    try std.testing.expectEqual(@as(usize, 2), replace_pos);

    const regexp_replace_tokens = [_]Token{
        .{ .kind = .identifier, .text = "regexp_replace" },
        .{ .kind = .lparen, .text = "(" },
    };
    var regexp_replace_pos: usize = 0;
    try parseRegexpReplaceFunctionCallStart(regexp_replace_tokens[0..], &regexp_replace_pos);
    try std.testing.expectEqual(@as(usize, 2), regexp_replace_pos);

    const regexp_count_tokens = [_]Token{
        .{ .kind = .identifier, .text = "regexp_count" },
        .{ .kind = .lparen, .text = "(" },
    };
    var regexp_count_pos: usize = 0;
    try parseFunctionCallStartIf(regexp_count_tokens[0..], &regexp_count_pos, sqlKeywordIsRegexpCountFunction);
    try std.testing.expectEqual(@as(usize, 2), regexp_count_pos);

    const extract_tokens = [_]Token{
        .{ .kind = .identifier, .text = "EXTRACT" },
        .{ .kind = .lparen, .text = "(" },
        .{ .kind = .identifier, .text = "dow" },
        .{ .kind = .identifier, .text = "FROM" },
    };
    var extract_pos: usize = 0;
    try std.testing.expect(try parseDatePartFunctionCallStart(extract_tokens[0..], &extract_pos));
    try std.testing.expectEqual(@as(usize, 2), extract_pos);
    extract_pos = 3;
    try parseDatePartExtractSeparator(extract_tokens[0..], &extract_pos);
    try std.testing.expectEqual(@as(usize, 4), extract_pos);

    const date_part_tokens = [_]Token{
        .{ .kind = .identifier, .text = "date_part" },
        .{ .kind = .lparen, .text = "(" },
    };
    var date_part_pos: usize = 0;
    try std.testing.expect(!try parseDatePartFunctionCallStart(date_part_tokens[0..], &date_part_pos));
    try std.testing.expectEqual(@as(usize, 2), date_part_pos);

    const array_transform_tokens = [_]Token{
        .{ .kind = .identifier, .text = "array_remove" },
        .{ .kind = .lparen, .text = "(" },
    };
    try std.testing.expect(peekArrayElementTransformFunctionCall(array_transform_tokens[0..], 0));

    const json_array_length_tokens = [_]Token{
        .{ .kind = .identifier, .text = "jsonb_array_length" },
        .{ .kind = .lparen, .text = "(" },
    };
    var json_array_length_pos: usize = 0;
    try parseJsonArrayLengthFunctionCallStart(json_array_length_tokens[0..], &json_array_length_pos);
    try std.testing.expectEqual(@as(usize, 2), json_array_length_pos);

    const json_typeof_tokens = [_]Token{
        .{ .kind = .identifier, .text = "jsonb_typeof" },
        .{ .kind = .lparen, .text = "(" },
    };
    var json_typeof_pos: usize = 0;
    try parseJsonTypeofFunctionCallStart(json_typeof_tokens[0..], &json_typeof_pos);
    try std.testing.expectEqual(@as(usize, 2), json_typeof_pos);

    const json_build_object_tokens = [_]Token{
        .{ .kind = .identifier, .text = "json_build_object" },
        .{ .kind = .lparen, .text = "(" },
    };
    var json_build_object_pos: usize = 0;
    try parseJsonBuildObjectFunctionCallStart(json_build_object_tokens[0..], &json_build_object_pos);
    try std.testing.expectEqual(@as(usize, 2), json_build_object_pos);

    const array_length_tokens = [_]Token{
        .{ .kind = .identifier, .text = "cardinality" },
        .{ .kind = .lparen, .text = "(" },
    };
    var array_length_pos: usize = 0;
    const array_length_keyword = try parseArrayLengthFunctionCallStart(array_length_tokens[0..], &array_length_pos);
    try std.testing.expectEqualStrings("cardinality", array_length_keyword);
    try std.testing.expectEqual(@as(usize, 2), array_length_pos);

    const array_position_tokens = [_]Token{
        .{ .kind = .identifier, .text = "array_positions" },
        .{ .kind = .lparen, .text = "(" },
    };
    var array_position_pos: usize = 0;
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.array_positions, try parseArrayPositionFunctionCallStart(array_position_tokens[0..], &array_position_pos));
    try std.testing.expectEqual(@as(usize, 2), array_position_pos);

    const string_to_array_tokens = [_]Token{
        .{ .kind = .identifier, .text = "string_to_array" },
        .{ .kind = .lparen, .text = "(" },
    };
    var string_to_array_pos: usize = 0;
    try parseStringToArrayFunctionCallStart(string_to_array_tokens[0..], &string_to_array_pos);
    try std.testing.expectEqual(@as(usize, 2), string_to_array_pos);

    const array_to_string_tokens = [_]Token{
        .{ .kind = .identifier, .text = "array_to_string" },
        .{ .kind = .lparen, .text = "(" },
    };
    var array_to_string_pos: usize = 0;
    try parseArrayToStringFunctionCallStart(array_to_string_tokens[0..], &array_to_string_pos);
    try std.testing.expectEqual(@as(usize, 2), array_to_string_pos);

    const array_replace_tokens = [_]Token{
        .{ .kind = .identifier, .text = "array_replace" },
        .{ .kind = .lparen, .text = "(" },
    };
    var array_replace_pos: usize = 0;
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.array_replace, try parseArrayElementTransformFunctionCallStart(array_replace_tokens[0..], &array_replace_pos));
    try std.testing.expectEqual(@as(usize, 2), array_replace_pos);

    const position_tokens = [_]Token{
        .{ .kind = .identifier, .text = "position" },
        .{ .kind = .lparen, .text = "(" },
    };
    try std.testing.expect(peekPositionFunctionSyntax(position_tokens[0..], 0));

    const now_tokens = [_]Token{
        .{ .kind = .identifier, .text = "now" },
        .{ .kind = .lparen, .text = "(" },
    };
    try std.testing.expect(peekSqlNowExpressionSyntax(now_tokens[0..], 0));

    const current_date_tokens = [_]Token{
        .{ .kind = .identifier, .text = "current_date" },
    };
    try std.testing.expect(peekSqlCurrentDateExpressionSyntax(current_date_tokens[0..], 0));

    const interval_tokens = [_]Token{
        .{ .kind = .identifier, .text = "interval" },
    };
    try std.testing.expect(peekSqlIntervalExpressionSyntax(interval_tokens[0..], 0));

    const to_jsonb_tokens = [_]Token{
        .{ .kind = .identifier, .text = "to_jsonb" },
        .{ .kind = .lparen, .text = "(" },
    };
    var to_jsonb_pos: usize = 0;
    try parseFixedUnaryFunctionCallStart(to_jsonb_tokens[0..], &to_jsonb_pos, .to_jsonb);
    try std.testing.expectEqual(@as(usize, 2), to_jsonb_pos);
}

test "sql adapter lower expr parses bulk io where expressions" {
    const alloc = std.testing.allocator;
    const schema = runtime_schema.TableSchema{
        .storage_mode = .relational,
        .relational_columns = &.{
            .{ .name = "status", .path = "status", .field_type = .keyword },
            .{ .name = "amount", .path = "amount", .field_type = .numeric },
        },
    };

    var tokens = try lexer.tokenizeAlloc(alloc, "where status = 'active' and amount > 10;");
    defer lexer.freeTokens(alloc, &tokens);
    var pos: usize = 0;
    const conditions = try parseBulkIoWhereExpressionsAlloc(alloc, tokens.items, &pos, schema, &.{}, &.{}, false);
    defer {
        plan_mod.freeExpressionConditions(alloc, conditions);
        if (conditions.len > 0) alloc.free(conditions);
    }

    try std.testing.expectEqual(tokens.items.len, pos);
    try std.testing.expectEqual(@as(usize, 2), conditions.len);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.field, conditions[0].lhs.kind);
    try std.testing.expectEqualStrings("status", conditions[0].lhs.field);
    try std.testing.expectEqual(runtime_schema.RelationalCheckOp.eq, conditions[0].op);
    try std.testing.expectEqualStrings("\"active\"", conditions[0].rhs[0].value_json);
    try std.testing.expectEqualStrings("amount", conditions[1].lhs.field);
    try std.testing.expectEqual(runtime_schema.RelationalCheckOp.gt, conditions[1].op);
    try std.testing.expectEqualStrings("10", conditions[1].rhs[0].value_json);
}

test "sql adapter expression grammar distinguishes grouped predicates from scalar expression predicates" {
    const grouped = [_]Token{
        .{ .kind = .lparen, .text = "(", .source_start = 0, .source_end = 1 },
        .{ .kind = .identifier, .text = "a", .source_start = 1, .source_end = 2 },
        .{ .kind = .eq, .text = "=", .source_start = 3, .source_end = 4 },
        .{ .kind = .number, .text = "1", .source_start = 5, .source_end = 6 },
        .{ .kind = .rparen, .text = ")", .source_start = 6, .source_end = 7 },
        .{ .kind = .identifier, .text = "and", .source_start = 8, .source_end = 11 },
    };
    try std.testing.expect(parenthesizedPredicateGroupCanStartAt(grouped[0..], 0));

    const compared_expression = [_]Token{
        .{ .kind = .lparen, .text = "(", .source_start = 0, .source_end = 1 },
        .{ .kind = .identifier, .text = "a", .source_start = 1, .source_end = 2 },
        .{ .kind = .rparen, .text = ")", .source_start = 2, .source_end = 3 },
        .{ .kind = .eq, .text = "=", .source_start = 4, .source_end = 5 },
        .{ .kind = .number, .text = "1", .source_start = 6, .source_end = 7 },
    };
    try std.testing.expect(!parenthesizedPredicateGroupCanStartAt(compared_expression[0..], 0));

    const not_like_expression = [_]Token{
        .{ .kind = .lparen, .text = "(", .source_start = 0, .source_end = 1 },
        .{ .kind = .identifier, .text = "name", .source_start = 1, .source_end = 5 },
        .{ .kind = .rparen, .text = ")", .source_start = 5, .source_end = 6 },
        .{ .kind = .identifier, .text = "not", .source_start = 7, .source_end = 10 },
        .{ .kind = .identifier, .text = "ilike", .source_start = 11, .source_end = 16 },
        .{ .kind = .string, .text = "%bot%", .source_start = 17, .source_end = 24 },
    };
    try std.testing.expect(!parenthesizedPredicateGroupCanStartAt(not_like_expression[0..], 0));
}

test "sql adapter lower expr peeks simple returning fields" {
    const alloc = std.testing.allocator;

    var field = try lexer.tokenizeAlloc(alloc, "status, total");
    defer lexer.freeTokens(alloc, &field);
    try std.testing.expect(peekSimpleReturningField(field.items, 0));

    var qualified = try lexer.tokenizeAlloc(alloc, "source.status as source_status");
    defer lexer.freeTokens(alloc, &qualified);
    try std.testing.expect(peekSimpleReturningField(qualified.items, 0));

    var arithmetic = try lexer.tokenizeAlloc(alloc, "total + tax");
    defer lexer.freeTokens(alloc, &arithmetic);
    try std.testing.expect(!peekSimpleReturningField(arithmetic.items, 0));

    var call = try lexer.tokenizeAlloc(alloc, "lower(status)");
    defer lexer.freeTokens(alloc, &call);
    try std.testing.expect(!peekSimpleReturningField(call.items, 0));

    var cast_expr = try lexer.tokenizeAlloc(alloc, "cast(total as text)");
    defer lexer.freeTokens(alloc, &cast_expr);
    try std.testing.expect(!peekSimpleReturningField(cast_expr.items, 0));
}

test "sql adapter lower expr parses coalesce field operands" {
    const alloc = std.testing.allocator;
    const schema = runtime_schema.TableSchema{
        .storage_mode = .relational,
        .relational_columns = &.{.{ .name = "status", .path = "status", .field_type = .keyword }},
    };

    var field_tokens = try lexer.tokenizeAlloc(alloc, "status, 'fallback'");
    defer lexer.freeTokens(alloc, &field_tokens);
    var field_pos: usize = 0;
    const field_operand = (try parseCoalesceFieldOperandOrNullOwnedAlloc(alloc, field_tokens.items, &field_pos, schema, &.{}, &.{}, false)) orelse return error.TestUnexpectedResult;
    defer switch (field_operand.kind) {
        .field => if (field_operand.field.len > 0) alloc.free(field_operand.field),
        .value => if (field_operand.value_json.len > 0) alloc.free(field_operand.value_json),
    };
    try std.testing.expectEqual(@as(usize, 1), field_pos);
    try std.testing.expectEqual(db_mod.types.RelationalRowsCoalesceOperandKind.field, field_operand.kind);
    try std.testing.expectEqualStrings("status", field_operand.field);

    var literal_tokens = try lexer.tokenizeAlloc(alloc, "'fallback'");
    defer lexer.freeTokens(alloc, &literal_tokens);
    var literal_pos: usize = 0;
    try std.testing.expect((try parseCoalesceFieldOperandOrNullOwnedAlloc(alloc, literal_tokens.items, &literal_pos, schema, &.{}, &.{}, false)) == null);
    try std.testing.expectEqual(@as(usize, 0), literal_pos);

    var call_tail_tokens = try lexer.tokenizeAlloc(alloc, "status()");
    defer lexer.freeTokens(alloc, &call_tail_tokens);
    var call_tail_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseCoalesceFieldOperandOrNullOwnedAlloc(alloc, call_tail_tokens.items, &call_tail_pos, schema, &.{}, &.{}, false));
}

test "sql adapter lower expr parses array field operands" {
    const alloc = std.testing.allocator;
    const schema = runtime_schema.TableSchema{
        .storage_mode = .relational,
        .relational_columns = &.{
            .{ .name = "tags", .path = "tags", .field_type = .array, .array_item_type = .keyword },
            .{ .name = "status", .path = "status", .field_type = .keyword },
        },
    };

    var raw_tokens = try lexer.tokenizeAlloc(alloc, "tags)");
    defer lexer.freeTokens(alloc, &raw_tokens);
    var raw_pos: usize = 0;
    const raw_field = try parseArrayFieldOwnedAlloc(alloc, raw_tokens.items, &raw_pos, schema);
    defer alloc.free(raw_field);
    try std.testing.expectEqual(@as(usize, 1), raw_pos);
    try std.testing.expectEqualStrings("tags", raw_field);

    var qualified_tokens = try lexer.tokenizeAlloc(alloc, "events.tags)");
    defer lexer.freeTokens(alloc, &qualified_tokens);
    var qualified_pos: usize = 0;
    const normalized_field = try parseRowExpressionArrayFieldOwnedAlloc(alloc, qualified_tokens.items, &qualified_pos, schema, &.{"events"}, &.{}, false);
    defer alloc.free(normalized_field);
    try std.testing.expectEqual(@as(usize, 1), qualified_pos);
    try std.testing.expectEqualStrings("tags", normalized_field);

    var scalar_tokens = try lexer.tokenizeAlloc(alloc, "status)");
    defer lexer.freeTokens(alloc, &scalar_tokens);
    var scalar_pos: usize = 0;
    try std.testing.expectError(error.InvalidSqlCatalog, parseArrayFieldOwnedAlloc(alloc, scalar_tokens.items, &scalar_pos, schema));
}

fn testOwnedBooleanConditionAlloc(
    alloc: std.mem.Allocator,
    lhs_value: bool,
    rhs_value: bool,
) !runtime_schema.RelationalRowsExpressionCondition {
    const lhs = try booleanLiteralExpressionAlloc(alloc, lhs_value);
    errdefer freeExpression(alloc, lhs);

    const rhs = try alloc.alloc(runtime_schema.RelationalRowsExpression, 1);
    errdefer alloc.free(rhs);
    rhs[0] = try booleanLiteralExpressionAlloc(alloc, rhs_value);

    return .{
        .lhs = lhs,
        .op = .eq,
        .rhs = rhs,
    };
}

test "sql adapter lower expr assembles boolean predicate groups" {
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

    var expression_conditions = std.ArrayListUnmanaged(runtime_schema.RelationalRowsExpressionCondition).empty;
    defer {
        freeExpressionConditions(alloc, expression_conditions.items);
        expression_conditions.deinit(alloc);
    }
    try appendBooleanConstantExpressionCondition(alloc, &expression_conditions, false);
    try std.testing.expectEqual(@as(usize, 1), expression_conditions.items.len);

    var boolean_groups = std.ArrayListUnmanaged(runtime_schema.RelationalRowsExpressionPredicateGroup).empty;
    defer {
        freeExpressionPredicateGroups(alloc, boolean_groups.items);
        boolean_groups.deinit(alloc);
    }
    try appendBooleanConstantExpressionGroup(alloc, &boolean_groups, true);
    try appendBooleanConstantExpressionGroup(alloc, &boolean_groups, false);
    const is_not_lhs = try booleanLiteralExpressionAlloc(alloc, true);
    defer freeExpression(alloc, is_not_lhs);
    try appendExpressionBooleanIsNotGroups(alloc, &boolean_groups, is_not_lhs, true);
    try appendBooleanIsNotExpressionGroups(alloc, &boolean_groups, "enabled", false);
    try std.testing.expectEqual(@as(usize, 6), boolean_groups.items.len);
    try std.testing.expectEqual(@as(usize, 0), boolean_groups.items[0].conditions.len);
    try std.testing.expectEqual(@as(usize, 1), boolean_groups.items[1].conditions.len);
    try std.testing.expectEqual(@as(usize, 1), boolean_groups.items[2].conditions.len);
    try std.testing.expectEqual(@as(usize, 1), boolean_groups.items[3].conditions.len);
    try std.testing.expectEqual(@as(usize, 1), boolean_groups.items[4].conditions.len);
    try std.testing.expectEqual(@as(usize, 1), boolean_groups.items[5].conditions.len);
    try std.testing.expectEqual(runtime_schema.RelationalCheckOp.ne, boolean_groups.items[4].conditions[0].op);
    try std.testing.expectEqual(runtime_schema.RelationalCheckOp.is_null, boolean_groups.items[5].conditions[0].op);
    try std.testing.expectEqualStrings("enabled", boolean_groups.items[4].conditions[0].lhs.field);
    try std.testing.expectEqualStrings("enabled", boolean_groups.items[5].conditions[0].lhs.field);

    var appended_groups = std.ArrayListUnmanaged(runtime_schema.RelationalRowsExpressionPredicateGroup).empty;
    defer {
        freeExpressionPredicateGroups(alloc, appended_groups.items);
        appended_groups.deinit(alloc);
    }
    const appended_condition = try testOwnedBooleanConditionAlloc(alloc, true, true);
    try appendExpressionConditionGroup(alloc, &appended_groups, appended_condition);

    const between_lhs = try booleanLiteralExpressionAlloc(alloc, true);
    defer freeExpression(alloc, between_lhs);
    const between_lower = try booleanLiteralExpressionAlloc(alloc, false);
    defer freeExpression(alloc, between_lower);
    const between_upper = try booleanLiteralExpressionAlloc(alloc, true);
    defer freeExpression(alloc, between_upper);
    try appendExpressionBetweenSymmetricGroups(alloc, &appended_groups, between_lhs, between_lower, between_upper, false);

    try std.testing.expectEqual(@as(usize, 3), appended_groups.items.len);
    try std.testing.expectEqual(@as(usize, 1), appended_groups.items[0].conditions.len);
    try std.testing.expectEqual(@as(usize, 2), appended_groups.items[1].conditions.len);
    try std.testing.expectEqual(@as(usize, 2), appended_groups.items[2].conditions.len);

    var scalar_branches = std.ArrayListUnmanaged(ScalarOrCheckBranch).empty;
    defer freeScalarOrCheckBranches(alloc, &scalar_branches);
    try scalar_branches.append(alloc, .empty);
    try appendScalarBooleanCheckToBranch(alloc, &scalar_branches.items[0], "enabled", .eq, true);
    try appendScalarNullCheckToBranch(alloc, &scalar_branches.items[0], "deleted_at", .is_null);
    try appendScalarValuesJsonToOrBranches(alloc, &scalar_branches, "status", "[\"open\",\"queued\"]", .eq);
    try std.testing.expectEqual(@as(usize, 1), scalar_branches.items.len);
    try std.testing.expectEqual(@as(usize, 4), scalar_branches.items[0].items.len);
    try expandScalarValuesJsonIntoOrBranches(alloc, &scalar_branches, "priority", "[1,2]", .eq);
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
    try appendBetweenScalarGroup(alloc, &scalar_predicate_groups, "amount", .gte, "10", .lte, "20");
    try appendBooleanIsNotPredicateGroups(alloc, &scalar_predicate_groups, "enabled", true);
    try appendScalarValuesJsonOrGroups(alloc, &scalar_predicate_groups, "status", "[\"open\",\"queued\"]", .ne);
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
    try appendScalarAllEqualityPredicates(alloc, &scalar_all_checks, "status", "[\"open\",\"queued\"]");
    try std.testing.expectEqual(@as(usize, 2), scalar_all_checks.items.len);

    var join_side_checks = std.ArrayListUnmanaged(runtime_schema.RelationalCheck).empty;
    defer {
        freeRelationalChecks(alloc, join_side_checks.items);
        join_side_checks.deinit(alloc);
    }
    var join_on_expressions = std.ArrayListUnmanaged(runtime_schema.RelationalRowsExpressionCondition).empty;
    defer {
        freeExpressionConditions(alloc, join_on_expressions.items);
        join_on_expressions.deinit(alloc);
    }
    try appendJoinOnScalarPredicateAlloc(alloc, &join_side_checks, &join_on_expressions, .inner, .right, "status", .eq, try alloc.dupe(u8, "\"open\""));
    try std.testing.expectEqual(@as(usize, 1), join_side_checks.items.len);
    try std.testing.expectEqualStrings("status", join_side_checks.items[0].field);
    try std.testing.expectEqualStrings("\"open\"", join_side_checks.items[0].value_json.?);
    try appendJoinOnScalarPredicateAlloc(alloc, &join_side_checks, &join_on_expressions, .left, .left, "status", .eq, try alloc.dupe(u8, "\"queued\""));
    try std.testing.expectEqual(@as(usize, 1), join_on_expressions.items.len);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionFieldSource.row, join_on_expressions.items[0].lhs.field_source);
    try std.testing.expectEqualStrings("status", join_on_expressions.items[0].lhs.field);
    try std.testing.expectEqualStrings("\"queued\"", join_on_expressions.items[0].rhs[0].value_json);

    var placed_orders = std.ArrayListUnmanaged(db_mod.types.RelationalRowsQueryOrder).empty;
    defer {
        freeOrderBy(alloc, placed_orders.items);
        placed_orders.deinit(alloc);
    }
    const order_field = try alloc.dupe(u8, "status");
    var order_field_transferred = false;
    errdefer if (!order_field_transferred) alloc.free(order_field);
    try appendOrderWithNullPlacement(alloc, &placed_orders, .{
        .field = order_field,
        .direction = .asc,
    }, true);
    order_field_transferred = true;
    try std.testing.expectEqual(@as(usize, 2), placed_orders.items.len);
    try std.testing.expectEqualStrings("status", placed_orders.items[0].field);
    try std.testing.expectEqual(db_mod.types.RelationalRowsQueryOrderDirection.desc, placed_orders.items[0].direction);
    try std.testing.expectEqual(db_mod.types.RelationalRowsQueryOrderNullTest.is_null, placed_orders.items[0].null_test.?);
    try std.testing.expectEqualStrings("status", placed_orders.items[1].field);
    try std.testing.expect(placed_orders.items[1].null_test == null);

    var expression_orders = std.ArrayListUnmanaged(db_mod.types.RelationalRowsQueryOrder).empty;
    defer {
        freeOrderBy(alloc, expression_orders.items);
        expression_orders.deinit(alloc);
    }
    const order_expression: db_mod.types.RelationalRowsExpression = .{ .kind = .value, .value_json = try alloc.dupe(u8, "1") };
    var order_expression_transferred = false;
    errdefer if (!order_expression_transferred) freeExpression(alloc, order_expression);
    try appendOrderWithNullPlacement(alloc, &expression_orders, .{
        .expression = order_expression,
        .direction = .desc,
    }, false);
    order_expression_transferred = true;
    try std.testing.expectEqual(@as(usize, 2), expression_orders.items.len);
    try std.testing.expect(expression_orders.items[0].expression != null);
    try std.testing.expectEqual(db_mod.types.RelationalRowsQueryOrderDirection.asc, expression_orders.items[0].direction);
    try std.testing.expectEqual(db_mod.types.RelationalRowsQueryOrderNullTest.is_null, expression_orders.items[0].null_test.?);
    try std.testing.expect(expression_orders.items[1].expression != null);
    try std.testing.expectEqual(db_mod.types.RelationalRowsQueryOrderDirection.desc, expression_orders.items[1].direction);

    var desc_order: db_mod.types.RelationalRowsQueryOrder = .{ .field = "status" };
    var desc_pos: usize = 0;
    const desc_tokens = [_]Token{
        .{ .kind = .identifier, .text = "desc", .source_start = 0, .source_end = 4 },
        .{ .kind = .identifier, .text = "nulls", .source_start = 5, .source_end = 10 },
        .{ .kind = .identifier, .text = "last", .source_start = 11, .source_end = 15 },
    };
    try std.testing.expectEqual(false, (try parseOrderModifiers(desc_tokens[0..], &desc_pos, &desc_order)).?);
    try std.testing.expectEqual(@as(usize, 3), desc_pos);
    try std.testing.expectEqual(db_mod.types.RelationalRowsQueryOrderDirection.desc, desc_order.direction);

    var asc_order: db_mod.types.RelationalRowsQueryOrder = .{ .field = "status" };
    var asc_pos: usize = 0;
    const asc_tokens = [_]Token{
        .{ .kind = .identifier, .text = "asc", .source_start = 0, .source_end = 3 },
        .{ .kind = .identifier, .text = "nulls", .source_start = 4, .source_end = 9 },
        .{ .kind = .identifier, .text = "first", .source_start = 10, .source_end = 15 },
    };
    try std.testing.expectEqual(true, (try parseOrderModifiers(asc_tokens[0..], &asc_pos, &asc_order)).?);
    try std.testing.expectEqual(@as(usize, 3), asc_pos);
    try std.testing.expectEqual(db_mod.types.RelationalRowsQueryOrderDirection.asc, asc_order.direction);

    var using_order: db_mod.types.RelationalRowsQueryOrder = .{ .field = "status" };
    var using_pos: usize = 0;
    const using_tokens = [_]Token{
        .{ .kind = .identifier, .text = "using", .source_start = 0, .source_end = 5 },
        .{ .kind = .gte, .text = ">=", .source_start = 6, .source_end = 8 },
    };
    try std.testing.expect((try parseOrderModifiers(using_tokens[0..], &using_pos, &using_order)) == null);
    try std.testing.expectEqual(@as(usize, 2), using_pos);
    try std.testing.expectEqual(db_mod.types.RelationalRowsQueryOrderDirection.desc, using_order.direction);

    const json_extract_expression = try expressionFromJsonExtractProjectionAlloc(alloc, .{
        .output = "metadata_source",
        .field = "metadata",
        .path = "source",
        .as_text = true,
    });
    defer freeExpression(alloc, json_extract_expression);
    try std.testing.expectEqual(runtime_schema.RelationalRowsExpressionKind.json_extract, json_extract_expression.kind);
    try std.testing.expectEqualStrings("source", json_extract_expression.json_path);
    try std.testing.expect(json_extract_expression.json_as_text);
    try std.testing.expectEqualStrings("metadata", json_extract_expression.operands[0].field);

    const array_length_expression = try expressionFromArrayLengthProjectionAlloc(alloc, .{
        .output = "tag_count",
        .field = "tags",
    });
    defer freeExpression(alloc, array_length_expression);
    try std.testing.expectEqual(runtime_schema.RelationalRowsExpressionKind.array_length, array_length_expression.kind);
    try std.testing.expectEqualStrings("tags", array_length_expression.operands[0].field);

    const coalesce_expression = try expressionFromCoalesceProjectionAlloc(alloc, .{
        .output = "status_coalesced",
        .operands = &.{
            .{ .kind = .field, .field = "status" },
            .{ .kind = .value, .value_json = "\"unknown\"" },
        },
    });
    defer freeExpression(alloc, coalesce_expression);
    try std.testing.expectEqual(runtime_schema.RelationalRowsExpressionKind.coalesce, coalesce_expression.kind);
    try std.testing.expectEqualStrings("status", coalesce_expression.operands[0].field);
    try std.testing.expectEqualStrings("\"unknown\"", coalesce_expression.operands[1].value_json);

    const order_schema: runtime_schema.TableSchema = .{
        .storage_mode = .relational,
        .relational_columns = &.{
            .{ .name = "id", .path = "id", .field_type = .keyword },
            .{ .name = "status", .path = "status", .field_type = .keyword },
        },
    };
    const order_select: plan_mod.SelectList = .{
        .fields = &.{"projected_status"},
        .json_extract = &.{.{ .output = "tier", .field = "payload", .path = "$.tier", .as_text = true }},
        .field_aliases = &.{.{ .field = "tenant_id", .output = "tenant" }},
        .expressions = &.{.{ .output = "always_true", .expression = .{ .kind = .value, .value_json = "true" } }},
        .outputs = &.{
            .{ .kind = .field, .index = 0 },
            .{ .kind = .json_extract, .index = 0 },
            .{ .kind = .field_alias, .index = 0 },
            .{ .kind = .expression, .index = 0 },
        },
        .select_all = true,
    };
    var ordinal_base_order = try selectOutputOrderByOrdinalAlloc(alloc, order_schema, order_select, 1);
    defer freeOrderBy(alloc, (&ordinal_base_order)[0..1]);
    try std.testing.expectEqualStrings("id", ordinal_base_order.field);
    var ordinal_projected_order = try selectOutputOrderByOrdinalAlloc(alloc, order_schema, order_select, 3);
    defer freeOrderBy(alloc, (&ordinal_projected_order)[0..1]);
    try std.testing.expectEqualStrings("projected_status", ordinal_projected_order.field);
    var alias_pos: usize = 0;
    const alias_tokens = [_]Token{.{ .kind = .identifier, .text = "tenant", .source_start = 0, .source_end = 6 }};
    var alias_order = (try parseSelectOutputOrderByNameMaybeAlloc(alloc, alias_tokens[0..], &alias_pos, order_select)).?;
    defer freeOrderBy(alloc, (&alias_order)[0..1]);
    try std.testing.expectEqual(@as(usize, 1), alias_pos);
    try std.testing.expectEqualStrings("tenant_id", alias_order.field);
    var expression_pos: usize = 0;
    const expression_tokens = [_]Token{.{ .kind = .identifier, .text = "always_true", .source_start = 0, .source_end = 11 }};
    var expression_order = (try parseSelectOutputOrderByNameMaybeAlloc(alloc, expression_tokens[0..], &expression_pos, order_select)).?;
    defer freeOrderBy(alloc, (&expression_order)[0..1]);
    try std.testing.expectEqual(@as(usize, 1), expression_pos);
    try std.testing.expect(expression_order.expression != null);
    try std.testing.expectEqualStrings("true", expression_order.expression.?.value_json);
    var operator_pos: usize = 0;
    const operator_tokens = [_]Token{
        .{ .kind = .identifier, .text = "tenant", .source_start = 0, .source_end = 6 },
        .{ .kind = .plus, .text = "+", .source_start = 7, .source_end = 8 },
    };
    try std.testing.expect((try parseSelectOutputOrderByNameMaybeAlloc(alloc, operator_tokens[0..], &operator_pos, order_select)) == null);
    try std.testing.expectEqual(@as(usize, 0), operator_pos);

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

    var left_conditions = try alloc.alloc(runtime_schema.RelationalRowsExpressionCondition, 2);
    left_conditions[0] = try testOwnedBooleanConditionAlloc(alloc, true, true);
    left_conditions[1] = try testOwnedBooleanConditionAlloc(alloc, true, false);

    var right_conditions = try alloc.alloc(runtime_schema.RelationalRowsExpressionCondition, 1);
    right_conditions[0] = try testOwnedBooleanConditionAlloc(alloc, false, false);

    var groups = [_]runtime_schema.RelationalRowsExpressionPredicateGroup{
        .{ .conditions = left_conditions },
        .{ .conditions = right_conditions },
    };
    const expression = try booleanExpressionFromPredicateGroupsAlloc(alloc, &groups);
    defer freeExpression(alloc, expression);

    try std.testing.expectEqual(runtime_schema.RelationalRowsExpressionKind.bool_or, expression.kind);
    try std.testing.expectEqual(@as(usize, 2), expression.operands.len);
    try std.testing.expectEqual(runtime_schema.RelationalRowsExpressionKind.bool_and, expression.operands[0].kind);
    try std.testing.expectEqual(@as(usize, 2), expression.operands[0].operands.len);
    try std.testing.expectEqual(runtime_schema.RelationalRowsExpressionKind.case, expression.operands[1].kind);
    try std.testing.expectEqual(@as(usize, 0), groups[0].conditions.len);
    try std.testing.expectEqual(@as(usize, 0), groups[1].conditions.len);
}

test "sql adapter lower expr names every row expression kind" {
    inline for (std.meta.fields(runtime_schema.RelationalRowsExpressionKind)) |field| {
        const kind: runtime_schema.RelationalRowsExpressionKind = @field(runtime_schema.RelationalRowsExpressionKind, field.name);
        try std.testing.expect(rowExpressionOpName(kind).len > 0);
        try std.testing.expect(rowExpressionDefaultOutputName(kind).len > 0);
    }
}

test "sql adapter lower expr compares row expressions" {
    const lower_status: runtime_schema.RelationalRowsExpression = .{
        .kind = .lower,
        .operands = &.{.{ .kind = .field, .field = "status" }},
    };
    const same_lower_status: runtime_schema.RelationalRowsExpression = .{
        .kind = .lower,
        .operands = &.{.{ .kind = .field, .field = "status" }},
    };
    const upper_status: runtime_schema.RelationalRowsExpression = .{
        .kind = .upper,
        .operands = &.{.{ .kind = .field, .field = "status" }},
    };

    try std.testing.expect(relationalRowsExpressionEqual(lower_status, same_lower_status));
    try std.testing.expect(!relationalRowsExpressionEqual(lower_status, upper_status));
    try std.testing.expect(relationalRowsExpressionOptionalEqual(lower_status, same_lower_status));
    try std.testing.expect(!relationalRowsExpressionOptionalEqual(lower_status, null));
    try validateDistinctOnOrder(&.{.{ .kind = .field, .field = "status" }}, &.{.{ .field = "status" }});
    try validateDistinctOnOrder(&.{lower_status}, &.{.{ .expression = same_lower_status }});
    try std.testing.expectError(error.UnsupportedSqlShape, validateDistinctOnOrder(&.{lower_status}, &.{.{ .expression = upper_status }}));
    try std.testing.expectError(error.UnsupportedSqlShape, validateDistinctOnOrder(
        &.{ .{ .kind = .field, .field = "tenant_id" }, .{ .kind = .field, .field = "status" } },
        &.{.{ .field = "tenant_id" }},
    ));
}

test "sql adapter lower expr compares aggregate specs" {
    const alloc = std.testing.allocator;

    const lower_status: runtime_schema.RelationalRowsExpression = .{
        .kind = .lower,
        .operands = &.{.{ .kind = .field, .field = "status" }},
    };
    const order_by = [_]db_mod.types.RelationalRowsQueryOrder{.{
        .field = "status",
        .expression = lower_status,
        .direction = .asc,
    }};
    const filters = [_]runtime_schema.RelationalCheck{.{
        .name = "status_check",
        .field = "status",
        .op = .eq,
        .value_json = "\"open\"",
    }};
    const json_filters = [_]db_mod.types.RelationalRowsJsonContainsPredicate{.{
        .field = "metadata",
        .value_json = "{\"source\":\"sql\"}",
    }};

    const lhs: db_mod.types.RelationalRowsAggregateSpec = .{
        .name = "statuses",
        .op = .array_agg,
        .field = "status",
        .array_order_by = &order_by,
        .filter_predicates = &filters,
        .filter_json_contains = &json_filters,
    };
    const same: db_mod.types.RelationalRowsAggregateSpec = .{
        .name = "different_output_name",
        .op = .array_agg,
        .field = "status",
        .array_order_by = &order_by,
        .filter_predicates = &filters,
        .filter_json_contains = &json_filters,
    };
    const different: db_mod.types.RelationalRowsAggregateSpec = .{
        .name = "statuses",
        .op = .array_agg,
        .field = "status",
        .array_order_by = &.{.{
            .field = "status",
            .expression = lower_status,
            .direction = .desc,
        }},
        .filter_predicates = &filters,
        .filter_json_contains = &json_filters,
    };

    try std.testing.expect(aggregateSpecsEquivalent(lhs, same));
    try std.testing.expect(!aggregateSpecsEquivalent(lhs, different));
    try std.testing.expectEqual(db_mod.types.RelationalRowsAggregateOp.percentile_cont, aggregateOpForName("PERCENTILE_CONT").?);
    try std.testing.expectEqualStrings("array_agg", aggregateOpName(.array_agg));
    try validateSqlAggregatePercentile(0);
    try validateSqlAggregatePercentile(1);
    try std.testing.expectError(error.UnsupportedSqlShape, validateSqlAggregatePercentile(-0.01));
    try std.testing.expectError(error.UnsupportedSqlShape, validateSqlAggregatePercentile(1.01));
    try std.testing.expectError(error.UnsupportedSqlShape, validateSqlAggregatePercentile(std.math.inf(f64)));
    const explicit_alias = try aggregateAliasOrDefaultAlloc(alloc, "total_amount", .sum, "amount");
    defer alloc.free(explicit_alias);
    try std.testing.expectEqualStrings("total_amount", explicit_alias);
    const field_alias = try aggregateAliasOrDefaultAlloc(alloc, null, .sum, "amount");
    defer alloc.free(field_alias);
    try std.testing.expectEqualStrings("sum_amount", field_alias);
    const op_alias = try aggregateAliasOrDefaultAlloc(alloc, null, .count, null);
    defer alloc.free(op_alias);
    try std.testing.expectEqualStrings("count", op_alias);
    try std.testing.expect(isSqlPercentileAggregateOp(.percentile_disc));
    try std.testing.expect(!isSqlPercentileAggregateOp(.array_agg));
    try std.testing.expectEqual(@as(?f64, 1.5), sqlJsonNumberAsF64(.{ .number_string = "1.5" }));
    try std.testing.expect(sqlJsonNumberAsF64(.{ .string = "1.5" }) == null);

    const group_projection: db_mod.types.RelationalRowsExpressionProjection = .{
        .output = "status_lower",
        .expression = lower_status,
    };
    const field_alias_projection: db_mod.types.RelationalRowsFieldAliasProjection = .{
        .output = "status",
        .field = "status",
    };
    try std.testing.expectEqual(runtime_schema.AntflyType.keyword, (try jsonValueProjectedType(alloc, "\"open\"")).field_type);
    try std.testing.expectEqual(runtime_schema.AntflyType.numeric, (try jsonValueProjectedType(alloc, "42")).field_type);
    try std.testing.expectEqual(runtime_schema.AntflyType.boolean, (try jsonValueProjectedType(alloc, "true")).field_type);
    try std.testing.expectError(error.UnsupportedSqlShape, jsonValueProjectedType(alloc, "null"));
    const coalesce_columns = [_]runtime_schema.RelationalColumn{.{
        .name = "status",
        .path = "status",
        .field_type = .keyword,
        .array_item_type = null,
    }};
    try std.testing.expectEqual(runtime_schema.AntflyType.keyword, (try coalesceOutputType(alloc, &coalesce_columns, .{
        .output = "status_coalesced",
        .operands = &.{.{ .kind = .field, .field = "status" }},
    })).field_type);
    const projected_column = try projectedColumnAlloc(alloc, "status", .keyword, null, true);
    defer {
        alloc.free(projected_column.name);
        alloc.free(projected_column.path);
    }
    try std.testing.expectEqualStrings("status", projected_column.name);
    try std.testing.expectEqualStrings("status", projected_column.path);
    try std.testing.expectEqual(runtime_schema.AntflyType.keyword, projected_column.field_type);
    try std.testing.expect(projected_column.nullable);
    try std.testing.expectEqual(@as(usize, 1), selectListOutputCount(&.{"status"}, &.{}, &.{}, &.{}, &.{}, &.{}, "status"));
    try std.testing.expectEqual(@as(usize, 2), selectListOutputCount(&.{"status"}, &.{}, &.{}, &.{}, &.{field_alias_projection}, &.{}, "status"));
    const select_schema = runtime_schema.TableSchema{
        .storage_mode = .relational,
        .relational_columns = &.{.{ .name = "status", .path = "status", .field_type = .keyword }},
    };
    try validateSelectListOutputs(select_schema, false, &.{"status"}, &.{}, &.{}, &.{}, &.{}, &.{});
    try validateSelectListOutputs(select_schema, false, &.{}, &.{.{
        .output = "status_json",
        .field = "payload",
        .path = "$.status",
        .as_text = true,
    }}, &.{}, &.{}, &.{}, &.{});
    try std.testing.expectError(error.UnsupportedSqlShape, validateSelectListOutputs(select_schema, false, &.{"status"}, &.{}, &.{}, &.{}, &.{field_alias_projection}, &.{}));
    try std.testing.expectError(error.UnsupportedSqlShape, validateSelectListOutputs(select_schema, true, &.{}, &.{.{
        .output = "status",
        .field = "payload",
        .path = "$.status",
        .as_text = true,
    }}, &.{}, &.{}, &.{}, &.{}));
    const select_columns = try selectOutputColumnsAlloc(alloc, .{ .alloc = alloc, .schema = select_schema }, .{
        .fields = &.{"status"},
        .json_extract = &.{.{ .output = "status_json", .field = "payload", .path = "$.status", .as_text = true }},
        .array_length = &.{.{ .output = "tag_count", .field = "tags" }},
        .expressions = &.{.{ .output = "enabled", .expression = .{ .kind = .value, .value_json = "true" } }},
        .outputs = &.{
            .{ .kind = .field, .index = 0 },
            .{ .kind = .json_extract, .index = 0 },
            .{ .kind = .array_length, .index = 0 },
            .{ .kind = .expression, .index = 0 },
        },
    });
    defer ddl_plan.freeDdlRelationalColumns(alloc, select_columns);
    try std.testing.expectEqual(@as(usize, 4), select_columns.len);
    try std.testing.expectEqualStrings("status", select_columns[0].name);
    try std.testing.expectEqual(runtime_schema.AntflyType.keyword, select_columns[0].field_type);
    try std.testing.expectEqualStrings("status_json", select_columns[1].name);
    try std.testing.expectEqual(runtime_schema.AntflyType.keyword, select_columns[1].field_type);
    try std.testing.expectEqualStrings("tag_count", select_columns[2].name);
    try std.testing.expectEqual(runtime_schema.AntflyType.numeric, select_columns[2].field_type);
    try std.testing.expectEqualStrings("enabled", select_columns[3].name);
    try std.testing.expectEqual(runtime_schema.AntflyType.boolean, select_columns[3].field_type);
    try std.testing.expectError(error.UnsupportedSqlShape, selectOutputColumnsAlloc(alloc, .{ .alloc = alloc, .schema = select_schema }, .{
        .fields = &.{"status"},
        .outputs = &.{.{ .kind = .field, .index = 1 }},
    }));
    try validateAggregateGroupBy(&.{"status"}, &.{group_projection}, &.{"status"}, &.{group_projection});
    try std.testing.expectError(error.UnsupportedSqlShape, validateAggregateGroupBy(&.{"status"}, &.{}, &.{"tenant_id"}, &.{}));

    const aggregate_specs = [_]db_mod.types.RelationalRowsAggregateSpec{lhs};
    const duplicate_aggregate_specs = [_]db_mod.types.RelationalRowsAggregateSpec{.{
        .name = "status",
        .op = .count,
    }};
    try std.testing.expect(aggregateOutputFieldIsUnique(&.{"status"}, &.{group_projection}, &aggregate_specs, "statuses"));
    try std.testing.expect(!aggregateOutputFieldIsUnique(&.{"status"}, &.{group_projection}, &duplicate_aggregate_specs, "status"));
    const output_columns = [_]runtime_schema.RelationalColumn{.{ .name = "statuses", .path = "statuses", .field_type = .array }};
    try std.testing.expect(aggregateOutputColumnExists(&output_columns, "statuses"));
    try std.testing.expect(!aggregateOutputColumnExists(&output_columns, "missing"));
    try validateAggregateSelectListOutputs(&.{"status"}, &.{group_projection}, &aggregate_specs);
    try std.testing.expectError(error.UnsupportedSqlShape, validateAggregateSelectListOutputs(&.{"status"}, &.{group_projection}, &.{.{
        .name = "status_lower",
        .op = .count,
    }}));
    const ordinal_group_field = try aggregateOutputFieldByOrdinalAlloc(alloc, &.{"status"}, &.{group_projection}, &aggregate_specs, 1);
    defer alloc.free(ordinal_group_field);
    try std.testing.expectEqualStrings("status", ordinal_group_field);
    const ordinal_group_expression = try aggregateOutputFieldByOrdinalAlloc(alloc, &.{"status"}, &.{group_projection}, &aggregate_specs, 2);
    defer alloc.free(ordinal_group_expression);
    try std.testing.expectEqualStrings("status_lower", ordinal_group_expression);
    const ordinal_aggregate = try aggregateOutputFieldByOrdinalAlloc(alloc, &.{"status"}, &.{group_projection}, &aggregate_specs, 3);
    defer alloc.free(ordinal_aggregate);
    try std.testing.expectEqualStrings("statuses", ordinal_aggregate);
    try std.testing.expectError(error.UnsupportedSqlShape, aggregateOutputFieldByOrdinalAlloc(alloc, &.{"status"}, &.{group_projection}, &aggregate_specs, 0));
    try std.testing.expectError(error.UnsupportedSqlShape, aggregateOutputFieldByOrdinalAlloc(alloc, &.{"status"}, &.{group_projection}, &aggregate_specs, 4));
    const group_identifier_tokens = [_]Token{
        .{ .kind = .identifier, .text = "status" },
        .{ .kind = .comma, .text = "," },
    };
    const function_identifier_tokens = [_]Token{
        .{ .kind = .identifier, .text = "lower" },
        .{ .kind = .lparen, .text = "(" },
    };
    try std.testing.expect(peekStandaloneAggregateGroupIdentifier(&group_identifier_tokens, 0));
    try std.testing.expect(!peekStandaloneAggregateGroupIdentifier(&function_identifier_tokens, 0));
    const aggregate_select = plan_mod.AggregateSelectList{
        .group_fields = &.{"status"},
        .group_expressions = &.{group_projection},
        .aggregations = &aggregate_specs,
        .outputs = &.{
            .{ .kind = .group_field, .index = 0 },
            .{ .kind = .group_expression, .index = 0 },
            .{ .kind = .aggregation, .index = 0 },
        },
    };
    var bound_group_fields = std.ArrayListUnmanaged([]const u8).empty;
    defer {
        for (bound_group_fields.items) |field| alloc.free(field);
        bound_group_fields.deinit(alloc);
    }
    var bound_group_expressions = std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionProjection).empty;
    defer {
        for (bound_group_expressions.items) |projection| {
            alloc.free(projection.output);
            freeExpression(alloc, projection.expression);
        }
        bound_group_expressions.deinit(alloc);
    }
    try appendAggregateGroupByOrdinal(alloc, &bound_group_fields, &bound_group_expressions, aggregate_select, 1);
    try std.testing.expectEqualStrings("status", bound_group_fields.items[0]);
    try appendAggregateGroupByOrdinal(alloc, &bound_group_fields, &bound_group_expressions, aggregate_select, 2);
    try std.testing.expectEqualStrings("status_lower", bound_group_expressions.items[0].output);
    try std.testing.expectError(error.UnsupportedSqlShape, appendAggregateGroupByOrdinal(alloc, &bound_group_fields, &bound_group_expressions, aggregate_select, 3));
    try appendAggregateGroupByFieldOrAlias(alloc, select_schema, &bound_group_fields, &bound_group_expressions, aggregate_select, try alloc.dupe(u8, "status_lower"), &.{}, &.{}, false);
    try std.testing.expectEqualStrings("status_lower", bound_group_expressions.items[1].output);
    try appendAggregateGroupByFieldOrAlias(alloc, select_schema, &bound_group_fields, &bound_group_expressions, aggregate_select, try alloc.dupe(u8, "status"), &.{}, &.{}, false);
    try std.testing.expectEqualStrings("status", bound_group_fields.items[1]);
    try std.testing.expectEqual(runtime_schema.AntflyType.keyword, try aggregateInputType(select_schema, .{ .alloc = alloc, .schema = select_schema }, lhs));
    const aggregate_output_columns = try aggregateOutputColumnsAlloc(alloc, select_schema, .{ .alloc = alloc, .schema = select_schema }, &.{"status"}, &.{group_projection}, &aggregate_specs);
    defer alloc.free(aggregate_output_columns);
    try std.testing.expectEqual(@as(usize, 3), aggregate_output_columns.len);
    try std.testing.expectEqualStrings("status", aggregate_output_columns[0].name);
    try std.testing.expectEqual(runtime_schema.AntflyType.keyword, aggregate_output_columns[0].field_type);
    try std.testing.expectEqualStrings("status_lower", aggregate_output_columns[1].name);
    try std.testing.expectEqual(runtime_schema.AntflyType.keyword, aggregate_output_columns[1].field_type);
    try std.testing.expectEqualStrings("statuses", aggregate_output_columns[2].name);
    try std.testing.expectEqual(runtime_schema.AntflyType.array, aggregate_output_columns[2].field_type);
    try std.testing.expectEqual(runtime_schema.AntflyType.keyword, aggregate_output_columns[2].array_item_type.?);
    const aggregate_status_lower = try aggregateOutputColumnForFieldAlloc(alloc, select_schema, .{ .alloc = alloc, .schema = select_schema }, &.{"status"}, &.{group_projection}, &aggregate_specs, "status_lower");
    try std.testing.expectEqual(runtime_schema.AntflyType.keyword, aggregate_status_lower.field_type);
    try std.testing.expectError(error.UnsupportedSqlShape, aggregateOutputColumnForFieldAlloc(alloc, select_schema, .{ .alloc = alloc, .schema = select_schema }, &.{"status"}, &.{group_projection}, &aggregate_specs, "missing"));
    const returning_schema = runtime_schema.TableSchema{
        .storage_mode = .relational,
        .relational_columns = &coalesce_columns,
    };
    const returning_expression: db_mod.types.RelationalRowsExpressionProjection = .{
        .output = "status_lower",
        .expression = lower_status,
    };
    try std.testing.expectEqual(@as(usize, 1), returningFieldOutputCount(&.{"status"}, "status"));
    try std.testing.expectEqual(@as(usize, 1), returningExpressionOutputCount(&.{returning_expression}, "status_lower"));
    try validateReturningProjectionOutputs(returning_schema, &.{"status"}, &.{returning_expression});
    try std.testing.expectError(error.UnsupportedSqlShape, validateReturningProjectionOutputs(returning_schema, &.{ "status", "status" }, &.{}));
    try std.testing.expectError(error.UnsupportedSqlShape, validateReturningProjectionOutputs(returning_schema, &.{"status"}, &.{.{
        .output = "status",
        .expression = lower_status,
    }}));
    try std.testing.expectError(error.UnsupportedSqlShape, validateReturningProjectionOutputs(returning_schema, &.{"*"}, &.{.{
        .output = "status",
        .expression = lower_status,
    }}));
    try std.testing.expectEqual(@as(usize, 1), expressionOrderCount(&order_by));
    try std.testing.expectEqual(@as(usize, 0), aggregateFilterGroupCount(&aggregate_specs));
    try std.testing.expectEqual(@as(usize, 0), aggregateFilterExpressionCount(&aggregate_specs));
    try std.testing.expectEqual(@as(usize, 0), aggregateFilterExpressionArrayCount(&aggregate_specs));
    try std.testing.expectEqual(@as(usize, 1), aggregateFilterJsonAccessCount(&aggregate_specs));
    try std.testing.expectEqual(@as(usize, 0), aggregateFilterStructuredAccessCount(&aggregate_specs));
    try std.testing.expectEqual(@as(usize, 0), aggregateInputExpressionCount(&aggregate_specs));

    const percentile_specs = [_]db_mod.types.RelationalRowsAggregateSpec{
        .{ .name = "p", .op = .percentile_cont, .field = "amount", .percentile_order = .desc, .percentiles = &.{ 0.5, 0.9 } },
        .{ .name = "m", .op = .mode, .field = "status" },
    };
    try std.testing.expectEqual(@as(usize, 1), aggregateDescendingPercentileCount(&percentile_specs));
    try std.testing.expectEqual(@as(usize, 1), aggregatePercentileArrayCount(&percentile_specs));
    try std.testing.expectEqual(@as(usize, 1), aggregateModeCount(&percentile_specs));
    const aggregate_schema = runtime_schema.TableSchema{
        .storage_mode = .relational,
        .relational_columns = &.{
            .{ .name = "status", .path = "status", .field_type = .keyword },
            .{ .name = "amount", .path = "amount", .field_type = .numeric },
            .{ .name = "enabled", .path = "enabled", .field_type = .boolean },
        },
    };
    const aggregate_type_context = RowExpressionTypeContext{ .alloc = alloc, .schema = aggregate_schema };
    try validateAggregateInputExpression(aggregate_type_context, .sum, .{ .kind = .field, .field = "amount" });
    try std.testing.expectError(error.InvalidSqlCatalog, validateAggregateInputExpression(aggregate_type_context, .sum, .{ .kind = .field, .field = "status" }));
    try validateAggregateInputExpression(aggregate_type_context, .mode, .{ .kind = .field, .field = "enabled" });
    try validateAggregateMinMaxRowExpression(aggregate_type_context, .{ .kind = .field, .field = "status" });
    try std.testing.expectError(error.UnsupportedSqlShape, validateAggregateMinMaxRowExpression(aggregate_type_context, .{ .kind = .field, .field = "enabled" }));

    const window_specs = [_]db_mod.types.RelationalRowsWindowSpec{.{
        .output = "ranked",
        .function = .lag,
        .value_expression = lower_status,
        .default_json = "\"unknown\"",
        .filter_predicates = &filters,
        .filter_json_contains = &json_filters,
        .filter_expressions = &.{.{
            .lhs = lower_status,
            .op = .eq,
            .rhs = &.{.{ .kind = .value, .value_json = "\"open\"" }},
        }},
        .frame = .{
            .unit = .rows,
            .start = .offset_preceding,
            .start_offset = 1,
            .end = .current_row,
        },
    }};
    try std.testing.expectEqual(@as(usize, 1), windowValueExpressionCount(&window_specs));
    try std.testing.expectEqual(@as(usize, 1), windowDefaultCount(&window_specs));
    try std.testing.expectEqual(@as(usize, 1), windowFilterPredicateCount(&window_specs));
    try std.testing.expectEqual(@as(usize, 1), windowFilterExpressionCount(&window_specs));
    try std.testing.expectEqual(@as(usize, 1), windowFilterAccessCount(&window_specs));
    try std.testing.expectEqual(@as(usize, 0), windowFilterGroupCount(&window_specs));
    try std.testing.expect(windowFrameSignature(&window_specs) != 0);
    try validateWindowSelectListOutputs(&.{"status"}, &window_specs);
    try std.testing.expectError(error.UnsupportedSqlShape, validateWindowSelectListOutputs(&.{"ranked"}, &window_specs));
    const window_select = plan_mod.WindowSelectList{
        .fields = &.{"status"},
        .windows = &window_specs,
        .outputs = &.{
            .{ .kind = .field, .index = 0 },
            .{ .kind = .window, .index = 0 },
        },
    };
    const window_ordinal_field = try windowOutputFieldByOrdinalAlloc(alloc, window_select, 2);
    defer alloc.free(window_ordinal_field);
    try std.testing.expectEqualStrings("ranked", window_ordinal_field);
    const window_output_columns = try windowOutputColumnsAlloc(alloc, select_schema, .{ .alloc = alloc, .schema = select_schema }, window_select);
    defer alloc.free(window_output_columns);
    try std.testing.expectEqual(@as(usize, 2), window_output_columns.len);
    try std.testing.expectEqualStrings("status", window_output_columns[0].name);
    try std.testing.expectEqual(runtime_schema.AntflyType.keyword, window_output_columns[0].field_type);
    try std.testing.expectEqualStrings("ranked", window_output_columns[1].name);
    try std.testing.expectEqual(runtime_schema.AntflyType.keyword, window_output_columns[1].field_type);
    try std.testing.expect(windowFunctionRequiresOrder(.lag));
    try std.testing.expect(!windowFunctionRequiresOrder(.count));
    try std.testing.expectEqualStrings("row_number", windowFunctionName(.row_number));
    try std.testing.expectEqual(runtime_schema.AntflyType.numeric, try windowOutputType(.row_number, null));
    try std.testing.expectEqual(runtime_schema.AntflyType.keyword, try windowOutputType(.lag, .keyword));
    try std.testing.expectError(error.UnsupportedSqlShape, windowOutputType(.lag, null));
    try validateWindowFrame(.{
        .unit = .rows,
        .start = .offset_preceding,
        .start_offset = 1,
        .end = .current_row,
    });
    try std.testing.expectError(error.UnsupportedSqlShape, validateWindowFrame(.{
        .unit = .rows,
        .start = .offset_following,
        .start_offset = 1,
        .end = .offset_preceding,
        .end_offset = 1,
    }));
    const range_frame = db_mod.types.RelationalRowsWindowFrame{
        .unit = .range,
        .start = .offset_preceding,
        .start_offset = 1,
        .end = .current_row,
    };
    try validateWindowFrameForOrder(aggregate_schema, aggregate_type_context, range_frame, &.{.{ .field = "amount" }});
    try std.testing.expectError(error.UnsupportedSqlShape, validateWindowFrameForOrder(aggregate_schema, aggregate_type_context, range_frame, &.{.{ .field = "status" }}));
    try std.testing.expectError(error.UnsupportedSqlShape, validateWindowFrameForOrder(aggregate_schema, aggregate_type_context, range_frame, &.{.{ .field = "amount", .null_test = .is_null }}));
    try std.testing.expectError(error.UnsupportedSqlShape, validateWindowFrameBoundOffset(.current_row, 1));
    try std.testing.expect(windowFrameBoundOrdinal(.unbounded_preceding, 0) < windowFrameBoundOrdinal(.current_row, 0));
    try std.testing.expect(windowFunctionSupportsFilter(.count));
    try std.testing.expect(!windowFunctionSupportsFilter(.lag));
    try std.testing.expectEqual(runtime_schema.AntflyType.array, (try aggregateOutputProjectedType(.{
        .name = "values",
        .op = .array_agg,
    }, .keyword)).field_type);
    try std.testing.expectEqual(runtime_schema.AntflyType.keyword, (try aggregateOutputProjectedType(.{
        .name = "values",
        .op = .array_agg,
    }, .keyword)).array_item_type.?);
    try std.testing.expectEqual(runtime_schema.AntflyType.numeric, (try aggregateOutputProjectedType(.{
        .name = "p95",
        .op = .percentile_cont,
    }, null)).field_type);
    try std.testing.expectEqual(runtime_schema.AntflyType.array, (try aggregateOutputProjectedType(.{
        .name = "percentiles",
        .op = .percentile_disc,
        .percentiles = &.{ 0.5, 0.95 },
    }, null)).field_type);
    try std.testing.expectEqual(runtime_schema.AntflyType.numeric, (try aggregateOutputProjectedType(.{
        .name = "percentiles",
        .op = .percentile_disc,
        .percentiles = &.{ 0.5, 0.95 },
    }, null)).array_item_type.?);
    try std.testing.expectEqual(runtime_schema.AntflyType.datetime, (try aggregateOutputProjectedType(.{
        .name = "latest",
        .op = .max,
    }, .datetime)).field_type);
    try std.testing.expectError(error.UnsupportedSqlShape, aggregateOutputProjectedType(.{
        .name = "missing",
        .op = .array_agg,
    }, null));
    const interval_expression: runtime_schema.RelationalRowsExpression = .{
        .kind = .interval_ns,
        .operands = &.{.{ .kind = .value, .value_json = "1000" }},
    };
    const nested_interval_expression: runtime_schema.RelationalRowsExpression = .{
        .kind = .add,
        .operands = &.{
            .{ .kind = .field, .field = "created_at" },
            interval_expression,
        },
    };
    try std.testing.expect(sqlExpressionIsInterval(interval_expression));
    try std.testing.expect(sqlExpressionContainsInterval(nested_interval_expression));
    try std.testing.expect(!sqlExpressionContainsInterval(.{ .kind = .field, .field = "created_at" }));
    try std.testing.expect(aggregateFilterIsEmpty(.{}));
    const owned_filter_predicates = try alloc.alloc(runtime_schema.RelationalCheck, 1);
    const owned_filter_field = try alloc.dupe(u8, "status");
    const owned_filter_value = try alloc.dupe(u8, "\"open\"");
    owned_filter_predicates[0] = .{
        .name = "",
        .field = owned_filter_field,
        .op = .eq,
        .value_json = owned_filter_value,
    };
    const owned_filter: AggregateFilter = .{ .predicates = owned_filter_predicates };
    defer freeAggregateFilter(alloc, owned_filter);
    try std.testing.expect(!aggregateFilterIsEmpty(owned_filter));

    try std.testing.expect(identifierContainsQualifier("left.status"));
    try std.testing.expect(!identifierContainsQualifier("status"));
    const qualified_tail_tokens = [_]Token{
        .{ .kind = .identifier, .text = "source.status", .source_start = 0, .source_end = 13 },
        .{ .kind = .identifier, .text = "and", .source_start = 14, .source_end = 17 },
        .{ .kind = .lparen, .text = "(", .source_start = 18, .source_end = 19 },
        .{ .kind = .identifier, .text = "target.id", .source_start = 19, .source_end = 28 },
        .{ .kind = .rparen, .text = ")", .source_start = 28, .source_end = 29 },
    };
    try std.testing.expect(tailMentionsAnyQualifierBeforeClose(qualified_tail_tokens[0..], 0, &.{"target"}));
    try std.testing.expect(!tailMentionsAnyQualifierBeforeClose(qualified_tail_tokens[0..], 0, &.{"missing"}));
    const close_tail_tokens = [_]Token{
        .{ .kind = .rparen, .text = ")", .source_start = 0, .source_end = 1 },
        .{ .kind = .identifier, .text = "target.id", .source_start = 2, .source_end = 11 },
    };
    try std.testing.expect(!tailMentionsAnyQualifierBeforeClose(close_tail_tokens[0..], 0, &.{"target"}));
    try std.testing.expectEqual(db_mod.types.RelationalRowsJoinProjectionSide.left, try binder.joinSideForQualifier("l", "l", "r"));
    try std.testing.expectError(error.UnsupportedSqlShape, binder.joinSideForQualifier("x", "l", "r"));

    const join_projections = [_]db_mod.types.RelationalRowsJoinProjection{
        .{ .output = "id", .field = "id", .side = .left },
        .{ .output = "status", .field = "status", .side = .right },
    };
    const duplicate_join_projections = [_]db_mod.types.RelationalRowsJoinProjection{
        .{ .output = "id", .field = "left_id", .side = .left },
        .{ .output = "id", .field = "right_id", .side = .right },
    };
    try std.testing.expect(joinProjectionOutputIsUnique(&join_projections, "status"));
    try std.testing.expect(!joinProjectionOutputIsUnique(&duplicate_join_projections, "id"));
}

test "sql adapter lower expr detects catalog expression references" {
    const status_field: runtime_schema.RelationalRowsExpression = .{ .kind = .field, .field = "status" };
    const tenant_field: runtime_schema.RelationalRowsExpression = .{ .kind = .field, .field = "tenant_id" };
    const literal: runtime_schema.RelationalRowsExpression = .{ .kind = .value, .value_json = "\"active\"" };
    const condition: runtime_schema.RelationalRowsExpressionCondition = .{
        .lhs = status_field,
        .op = .eq,
        .rhs = &.{literal},
    };
    const generated_column: runtime_schema.RelationalColumn = .{
        .name = "tenant_status",
        .path = "tenant_status",
        .field_type = .keyword,
        .generated = .{ .op = .concat_ws, .fields = &.{ "tenant_id", "status" }, .separator = ":" },
    };
    const expression_generated: runtime_schema.RelationalColumn = .{
        .name = "status_lower",
        .path = "status_lower",
        .field_type = .keyword,
        .generated = .{ .op = .expression, .expression = .{ .kind = .lower, .operands = &.{status_field} } },
    };
    const unique_constraint: runtime_schema.UniqueConstraint = .{
        .name = "tenant_status_key",
        .columns = &.{"tenant_id"},
        .expressions = &.{.{ .op = .expression, .expression = .{ .kind = .concat, .operands = &.{ tenant_field, status_field } } }},
        .where_expressions = &.{condition},
    };

    try std.testing.expect(generatedColumnReferencesAny(generated_column, &.{"tenant_id"}));
    try std.testing.expect(generatedColumnReferencesAny(expression_generated, &.{"status"}));
    try std.testing.expect(expressionConditionReferencesAny(condition, &.{"status"}));
    try std.testing.expect(expressionConditionReferencesField(condition, "status"));
    try std.testing.expect(!expressionConditionReferencesField(condition, "tenant_id"));
    try std.testing.expect(uniqueConstraintReferencesAny(unique_constraint, &.{"status"}));
    try std.testing.expect(!uniqueConstraintReferencesAny(unique_constraint, &.{"missing"}));

    const lower_status: runtime_schema.RelationalRowsExpression = .{
        .kind = .lower,
        .operands = &.{status_field},
    };
    const same_unique_expressions = [_]runtime_schema.UniqueExpression{.{ .op = .expression, .expression = lower_status }};
    const equal_unique_expressions = [_]runtime_schema.UniqueExpression{.{ .op = .expression, .expression = lower_status }};
    const different_unique_expressions = [_]runtime_schema.UniqueExpression{.{ .op = .lower, .field = "tenant_id" }};
    try std.testing.expect(uniqueExpressionsEqual(&same_unique_expressions, &equal_unique_expressions));
    try std.testing.expect(!uniqueExpressionsEqual(&same_unique_expressions, &different_unique_expressions));

    const SelectorValue = struct {
        field: []const u8,
        value_json: []const u8,
    };
    const selector_values = [_]SelectorValue{
        .{ .field = "status", .value_json = "\"Active User\"" },
        .{ .field = "tenant_id", .value_json = "\"tenant-a\"" },
        .{ .field = "amount", .value_json = "42" },
    };
    const lower_selector: runtime_schema.RelationalRowsExpression = .{
        .kind = .lower,
        .operands = &.{.{ .kind = .field, .field = "status", .field_source = .row }},
    };
    const lower_json = try selectorExpressionValueJsonAlloc(std.testing.allocator, lower_selector, &selector_values);
    defer std.testing.allocator.free(lower_json);
    try std.testing.expectEqualStrings("\"active user\"", lower_json);

    const concat_selector: runtime_schema.RelationalRowsExpression = .{
        .kind = .concat_ws,
        .operands = &.{
            .{ .kind = .value, .value_json = "\"/\"" },
            .{ .kind = .field, .field = "tenant_id", .field_source = .row },
            .{ .kind = .field, .field = "status", .field_source = .row },
        },
    };
    const concat_json = try selectorExpressionValueJsonAlloc(std.testing.allocator, concat_selector, &selector_values);
    defer std.testing.allocator.free(concat_json);
    try std.testing.expectEqualStrings("\"tenant-a/Active User\"", concat_json);
    try std.testing.expectEqualStrings("\"Active User\"", fieldValueJsonFor(&selector_values, "status").?);
    try std.testing.expect(fieldValuesContain(&selector_values, "tenant_id"));
    try std.testing.expect(!fieldValuesContain(&selector_values, "missing"));
    try std.testing.expect(fieldValuesMatchColumns(&selector_values, &.{ "status", "tenant_id", "amount" }));
    try std.testing.expect(!fieldValuesMatchColumns(&selector_values, &.{ "status", "tenant_id" }));

    var selected_fields: std.Io.Writer.Allocating = .init(std.testing.allocator);
    errdefer selected_fields.deinit();
    try writeFieldValuesObjectJson(&selected_fields.writer, &selector_values, &.{ "tenant_id", "status" });
    const selected_json = try selected_fields.toOwnedSlice();
    defer std.testing.allocator.free(selected_json);
    try std.testing.expectEqualStrings("{\"tenant_id\":\"tenant-a\",\"status\":\"Active User\"}", selected_json);

    var all_fields: std.Io.Writer.Allocating = .init(std.testing.allocator);
    errdefer all_fields.deinit();
    try writeAllFieldValuesObjectJson(&all_fields.writer, &selector_values);
    const all_json = try all_fields.toOwnedSlice();
    defer std.testing.allocator.free(all_json);
    try std.testing.expectEqualStrings("{\"status\":\"Active User\",\"tenant_id\":\"tenant-a\",\"amount\":42}", all_json);

    var left_number = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, "42", .{});
    defer left_number.deinit();
    var right_number = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, "43", .{});
    defer right_number.deinit();
    try std.testing.expectEqual(SelectorJsonOrder.lt, selectorCompareJsonScalars(left_number.value, right_number.value) orelse return error.TestUnexpectedResult);
    try std.testing.expect(selectorJsonValuesEqual(left_number.value, left_number.value));

    try std.testing.expect(isCaseFoldExpressionOp(.lower));
    try std.testing.expect(isCaseFoldExpressionOp(.upper));
    try std.testing.expect(isCaseFoldExpressionOp(.md5));
    try std.testing.expect(!isCaseFoldExpressionOp(.expression));
    const md5_hex = try md5HexTextAlloc(std.testing.allocator, "hello");
    defer std.testing.allocator.free(md5_hex);
    try std.testing.expectEqualStrings("5d41402abc4b2a76b9719d911017c592", md5_hex);
    const initcap = try initcapTextAlloc(std.testing.allocator, "hello SQL-world");
    defer std.testing.allocator.free(initcap);
    try std.testing.expectEqualStrings("Hello Sql-World", initcap);
    try std.testing.expectEqual(runtime_schema.RelationalGeneratedOp.lower, relationalGeneratedOpForUniqueExpressionOp(.lower));
    try std.testing.expectEqual(runtime_schema.RelationalGeneratedOp.upper, relationalGeneratedOpForUniqueExpressionOp(.upper));
    try std.testing.expectEqual(runtime_schema.RelationalGeneratedOp.md5, relationalGeneratedOpForUniqueExpressionOp(.md5));
    try std.testing.expectEqualStrings("expression", uniqueExpressionOpToken(.expression));
    try std.testing.expectEqualStrings("eq", uniquePredicateOpToken(.eq));
    try std.testing.expectEqualStrings("is_not_null", uniquePredicateOpToken(.is_not_null));
    try std.testing.expectEqual(runtime_schema.RelationalCheckOp.ne, uniquePredicateAsRelationalCheckOp(.ne));
    try std.testing.expectEqual(runtime_schema.RelationalCheckOp.is_null, uniquePredicateAsRelationalCheckOp(.is_null));
    try std.testing.expectEqual(runtime_schema.RelationalCheckOp.eq, relationalCheckOpFromUniquePredicateToken("eq").?);
    try std.testing.expect(relationalCheckOpFromUniquePredicateToken("missing") == null);
    const arithmetic_tokens = [_]Token{
        .{ .kind = .identifier, .text = "amount" },
        .{ .kind = .plus, .text = "+" },
        .{ .kind = .identifier, .text = "interval" },
    };
    try std.testing.expectEqual(TokenKind.plus, peekArithmeticOperator(&arithmetic_tokens, 1).?.token);
    try std.testing.expect(peekArithmeticRhsKeyword(&arithmetic_tokens, 0, "interval"));
    try std.testing.expect(!peekArithmeticRhsKeyword(&arithmetic_tokens, 1, "interval"));
    var comparison_pos: usize = 0;
    const comparison_tokens = [_]Token{.{ .kind = .gte, .text = ">=" }};
    try std.testing.expectEqual(runtime_schema.RelationalCheckOp.gte, try parseComparisonOp(&comparison_tokens, &comparison_pos));
    try std.testing.expectEqual(@as(usize, 1), comparison_pos);
    var postfix_pos: usize = 0;
    const postfix_tokens = [_]Token{.{ .kind = .identifier, .text = "notnull" }};
    try std.testing.expectEqual(runtime_schema.RelationalCheckOp.is_not_null, matchPostfixNullTest(&postfix_tokens, &postfix_pos).?);
    try std.testing.expectEqual(@as(usize, 1), postfix_pos);
    try std.testing.expect(tokenKindIsJsonExtractOperator(.arrow_json));
    try std.testing.expect(tokenKindIsJsonExtractOperator(.path_arrow_text));
    try std.testing.expect(tokenKindIsJsonExtractTextOperator(.arrow_text));
    try std.testing.expect(!tokenKindIsJsonExtractTextOperator(.arrow_json));
    try std.testing.expect(tokenKindIsJsonExtractPathOperator(.path_arrow_json));
    try std.testing.expect(!tokenKindIsJsonExtractPathOperator(.arrow_json));
    const json_extract_tokens = [_]Token{
        .{ .kind = .identifier, .text = "metadata" },
        .{ .kind = .arrow_text, .text = "->>" },
        .{ .kind = .string, .text = "status" },
        .{ .kind = .identifier, .text = "is" },
        .{ .kind = .identifier, .text = "not" },
        .{ .kind = .identifier, .text = "distinct" },
        .{ .kind = .identifier, .text = "from" },
        .{ .kind = .string, .text = "active" },
    };
    try std.testing.expect(jsonExtractExpressionCanStartAt(&json_extract_tokens, 0));
    try std.testing.expect(!jsonExtractExpressionPredicateCanStartAt(&json_extract_tokens, 0));
    try std.testing.expect(jsonExtractNullSafeDistinctPredicateCanStartAt(&json_extract_tokens, 0));
    const aggregate_json_eq_tokens = [_]Token{
        .{ .kind = .identifier, .text = "metadata" },
        .{ .kind = .arrow_text, .text = "->>" },
        .{ .kind = .string, .text = "status" },
        .{ .kind = .eq, .text = "=" },
        .{ .kind = .identifier, .text = "true" },
    };
    try std.testing.expect(aggregateJsonPathEqFilterCanStartAt(&aggregate_json_eq_tokens, 0));
    const json_null_tokens = [_]Token{
        .{ .kind = .identifier, .text = "metadata" },
        .{ .kind = .arrow_json, .text = "->" },
        .{ .kind = .placeholder, .text = "$1" },
        .{ .kind = .identifier, .text = "is" },
        .{ .kind = .identifier, .text = "null" },
    };
    try std.testing.expect(jsonExtractNullTestPredicateCanStartAt(&json_null_tokens, 0));
    const json_membership_tokens = [_]Token{
        .{ .kind = .identifier, .text = "metadata" },
        .{ .kind = .arrow_text, .text = "->>" },
        .{ .kind = .string, .text = "status" },
        .{ .kind = .identifier, .text = "not" },
        .{ .kind = .identifier, .text = "in" },
    };
    try std.testing.expect(jsonExtractMembershipPredicateCanStartAt(&json_membership_tokens, 0));
    const not_group_tokens = [_]Token{
        .{ .kind = .identifier, .text = "not" },
        .{ .kind = .lparen, .text = "(" },
    };
    try std.testing.expect(canParseAggregateFilterNot(&not_group_tokens, 0));
    const parenthesized_condition_tokens = [_]Token{
        .{ .kind = .lparen, .text = "(" },
        .{ .kind = .identifier, .text = "amount" },
        .{ .kind = .rparen, .text = ")" },
        .{ .kind = .gte, .text = ">=" },
    };
    try std.testing.expect(peekParenthesizedExpressionCondition(&parenthesized_condition_tokens, 0));
    var group_pos: usize = 0;
    const parenthesized_group_tokens = [_]Token{
        .{ .kind = .lparen, .text = "(" },
        .{ .kind = .identifier, .text = "status" },
        .{ .kind = .eq, .text = "=" },
        .{ .kind = .string, .text = "active" },
        .{ .kind = .rparen, .text = ")" },
    };
    try std.testing.expect(matchBooleanGroupOpen(&parenthesized_group_tokens, &group_pos));
    try std.testing.expectEqual(@as(usize, 1), group_pos);
    var regex_pos: usize = 0;
    const regex_tokens = [_]Token{.{ .kind = .regex_not_imatch, .text = "!~*" }};
    const regex_operator = matchRegexPredicateOperator(&regex_tokens, &regex_pos) orelse return error.TestUnexpectedResult;
    try std.testing.expect(regex_operator.case_insensitive);
    try std.testing.expect(regex_operator.negated);
    try std.testing.expectEqual(@as(usize, 1), regex_pos);
    const json_key_set_tokens = [_]Token{
        .{ .kind = .identifier, .text = "metadata" },
        .{ .kind = .question_any, .text = "?|" },
    };
    try std.testing.expect(jsonKeySetExpressionCanStartAt(&json_key_set_tokens, 0));

    const generated_columns = [_]runtime_schema.RelationalColumn{
        .{
            .name = "status_lower",
            .path = "status_lower",
            .field_type = .keyword,
            .generated = .{ .op = .lower, .field = "status" },
        },
    };
    const generated_schema: runtime_schema.TableSchema = .{ .relational_columns = &generated_columns };
    try std.testing.expectEqualStrings("status_lower", generatedUnaryTextColumnForField(generated_schema, .lower, "status").?.name);
    try std.testing.expect(generatedUnaryTextColumnForField(generated_schema, .upper, "status") == null);
    try std.testing.expect(generatedUnaryTextColumnForField(generated_schema, .concat, "status") == null);
}

test "sql adapter lower expr compares query projection and set operation surfaces" {
    const status_field: runtime_schema.RelationalRowsExpression = .{ .kind = .field, .field = "status" };
    const lower_status: runtime_schema.RelationalRowsExpression = .{
        .kind = .lower,
        .operands = &.{status_field},
    };
    const projection: db_mod.types.RelationalRowsExpressionProjection = .{
        .output = "status_lower",
        .expression = lower_status,
    };
    const simple_query: db_mod.types.RelationalRowsQueryRequest = .{
        .select = &.{"status"},
        .expressions = &.{projection},
    };
    const same_query: db_mod.types.RelationalRowsQueryRequest = .{
        .select = &.{"status"},
        .expressions = &.{projection},
    };
    const limited_query: db_mod.types.RelationalRowsQueryRequest = .{
        .select = &.{"status"},
        .expressions = &.{projection},
        .limit = 10,
    };
    const status_predicate: runtime_schema.RelationalCheck = .{
        .name = "status_active",
        .field = "status",
        .op = .eq,
        .value_json = "\"active\"",
    };
    const grouped_predicate: db_mod.types.RelationalRowsPredicateGroup = .{
        .predicates = &.{status_predicate},
    };
    const scalar_or_query: db_mod.types.RelationalRowsQueryRequest = .{
        .or_predicates = &.{grouped_predicate},
    };
    const mixed_or_query: db_mod.types.RelationalRowsQueryRequest = .{
        .predicates = &.{status_predicate},
        .or_predicates = &.{grouped_predicate},
    };
    const extended_query: db_mod.types.RelationalRowsQueryRequest = .{
        .json_contains = &.{.{ .field = "metadata", .value_json = "{\"tier\":\"gold\"}" }},
    };
    const left_outputs = [_]ast.SelectOutputRef{.{ .kind = .field, .index = 0 }};
    const right_outputs = [_]ast.SelectOutputRef{.{ .kind = .field, .index = 0 }};
    const mismatched_outputs = [_]ast.SelectOutputRef{.{ .kind = .expression, .index = 0 }};
    const left_columns = [_]runtime_schema.RelationalColumn{.{ .name = "status", .path = "status", .field_type = .keyword }};
    const right_columns = [_]runtime_schema.RelationalColumn{.{ .name = "status", .path = "status", .field_type = .keyword }};
    const incompatible_columns = [_]runtime_schema.RelationalColumn{.{ .name = "status", .path = "status", .field_type = .numeric }};

    try std.testing.expect(queryHasOnlySimpleUnionPredicateSurface(simple_query));
    try std.testing.expect(queryHasOnlySimpleIntersectExceptPredicateSurface(simple_query));
    try std.testing.expect(!queryHasOnlySimpleUnionPredicateSurface(limited_query));
    try std.testing.expect(!queryHasOnlySimpleIntersectExceptPredicateSurface(limited_query));
    try std.testing.expect(queryHasNoSimpleSetPredicates(simple_query));
    try std.testing.expect(!queryHasNoSimpleSetPredicates(scalar_or_query));
    try std.testing.expect(querySupportsSimpleUnionRewrite(scalar_or_query));
    try std.testing.expect(querySupportsSimpleIntersectExceptRewrite(scalar_or_query));
    try std.testing.expect(!querySupportsSimpleUnionRewrite(mixed_or_query));
    try std.testing.expect(!querySupportsSimpleIntersectExceptRewrite(mixed_or_query));
    try std.testing.expect(!sourceQueryUsesExtendedPredicates(simple_query));
    try std.testing.expect(sourceQueryUsesExtendedPredicates(extended_query));
    try std.testing.expect(expressionProjectionsEqual(&.{projection}, &.{projection}));
    try std.testing.expect(simpleSelectProjectionsEqual(simple_query, same_query, &left_outputs, &right_outputs));
    try std.testing.expect(!simpleSelectProjectionsEqual(simple_query, same_query, &left_outputs, &mismatched_outputs));
    try std.testing.expect(setOperationColumnsCompatible(&left_columns, &right_columns));
    try std.testing.expect(!setOperationColumnsCompatible(&left_columns, &incompatible_columns));
}

test "sql adapter lower expr proves simple predicate disjointness" {
    const status_field: runtime_schema.RelationalRowsExpression = .{ .kind = .field, .field = "status" };
    const active_literal: runtime_schema.RelationalRowsExpression = .{ .kind = .value, .value_json = "\"active\"" };
    const inactive_literal: runtime_schema.RelationalRowsExpression = .{ .kind = .value, .value_json = "\"inactive\"" };
    const active_condition: runtime_schema.RelationalRowsExpressionCondition = .{
        .lhs = status_field,
        .op = .eq,
        .rhs = &.{active_literal},
    };
    const inactive_condition: runtime_schema.RelationalRowsExpressionCondition = .{
        .lhs = status_field,
        .op = .eq,
        .rhs = &.{inactive_literal},
    };

    try std.testing.expect(relationalChecksProvablyDisjoint(
        .{ .name = "", .field = "status", .op = .eq, .value_json = "\"active\"" },
        .{ .name = "", .field = "status", .op = .eq, .value_json = "\"inactive\"" },
    ));
    try std.testing.expect(relationalCheckAndExpressionConditionProvablyDisjoint(
        .{ .name = "", .field = "status", .op = .eq, .value_json = "\"active\"" },
        inactive_condition,
    ));
    try std.testing.expect(expressionConditionsProvablyDisjoint(active_condition, inactive_condition));
    try std.testing.expect(relationalChecksProvablyDisjoint(
        .{ .name = "", .field = "amount", .op = .lt, .value_json = "10" },
        .{ .name = "", .field = "amount", .op = .gte, .value_json = "10" },
    ));
    try std.testing.expect(jsonIsSafeDisjointProofLiteral("\"active\""));
    try std.testing.expect(jsonIsJsonNumberLiteral("-10.5e2"));
    try std.testing.expect(!jsonIsJsonNumberLiteral("01"));
}

test "sql adapter lower expr validates unique expression lists" {
    try validateSqlUniqueExpressionListUnique(&.{
        .{ .op = .lower, .field = "status" },
        .{ .op = .upper, .field = "status" },
    });
    try std.testing.expectError(error.UnsupportedSqlShape, validateSqlUniqueExpressionListUnique(&.{
        .{ .op = .lower, .field = "status" },
        .{ .op = .lower, .field = "STATUS" },
    }));

    const lower_status: runtime_schema.RelationalRowsExpression = .{
        .kind = .lower,
        .operands = &.{.{ .kind = .field, .field = "status" }},
    };
    try std.testing.expectError(error.UnsupportedSqlShape, validateSqlUniqueExpressionListUnique(&.{
        .{ .op = .expression, .expression = lower_status },
        .{ .op = .expression, .expression = lower_status },
    }));
}

test "sql adapter lower expr detects deterministic row expressions" {
    const status_field: runtime_schema.RelationalRowsExpression = .{ .kind = .field, .field = "status" };
    const literal: runtime_schema.RelationalRowsExpression = .{ .kind = .value, .value_json = "\"open\"" };
    const source_field: runtime_schema.RelationalRowsExpression = .{ .kind = .field, .field = "status", .field_source = .source };
    const now_expression: runtime_schema.RelationalRowsExpression = .{ .kind = .now };
    const condition: runtime_schema.RelationalRowsExpressionCondition = .{
        .lhs = status_field,
        .op = .eq,
        .rhs = &.{literal},
    };
    const nondeterministic_condition: runtime_schema.RelationalRowsExpressionCondition = .{
        .lhs = status_field,
        .op = .eq,
        .rhs = &.{now_expression},
    };

    try std.testing.expect(rowExpressionDeterministic(status_field));
    try std.testing.expect(!rowExpressionDeterministic(source_field));
    try std.testing.expect(!rowExpressionDeterministic(now_expression));
    try std.testing.expect(rowExpressionConditionDeterministic(condition));
    try std.testing.expect(!rowExpressionConditionDeterministic(nondeterministic_condition));
}

test "sql adapter lower expr validates catalog check expression types" {
    const columns = [_]runtime_schema.RelationalColumn{
        .{ .name = "status", .path = "status", .field_type = .keyword },
        .{ .name = "amount", .path = "amount", .field_type = .numeric },
        .{ .name = "metadata", .path = "metadata", .field_type = .json },
    };
    const status_field: runtime_schema.RelationalRowsExpression = .{ .kind = .field, .field = "status" };
    const amount_field: runtime_schema.RelationalRowsExpression = .{ .kind = .field, .field = "amount" };
    const metadata_field: runtime_schema.RelationalRowsExpression = .{ .kind = .field, .field = "metadata" };
    const numeric_literal: runtime_schema.RelationalRowsExpression = .{ .kind = .value, .value_json = "10" };
    const now_expression: runtime_schema.RelationalRowsExpression = .{ .kind = .now };
    const lower_status: runtime_schema.RelationalRowsExpression = .{
        .kind = .lower,
        .operands = &.{status_field},
    };
    const valid_condition: runtime_schema.RelationalRowsExpressionCondition = .{
        .lhs = amount_field,
        .op = .gt,
        .rhs = &.{numeric_literal},
    };
    const incomparable_condition: runtime_schema.RelationalRowsExpressionCondition = .{
        .lhs = status_field,
        .op = .eq,
        .rhs = &.{metadata_field},
    };
    const nondeterministic_condition: runtime_schema.RelationalRowsExpressionCondition = .{
        .lhs = amount_field,
        .op = .eq,
        .rhs = &.{now_expression},
    };

    try validateCheckExpressionForColumns(&columns, lower_status);
    try validateCheckExpressionConditionForColumns(&columns, valid_condition);
    try std.testing.expectError(error.InvalidSqlCatalog, validateCheckExpressionConditionForColumns(&columns, incomparable_condition));
    try validateGeneratedColumnExpressionForColumns(&columns, "status_lower", lower_status);
    try std.testing.expectError(error.InvalidSqlCatalog, validateGeneratedColumnExpressionForColumns(&columns, "status", status_field));
    try std.testing.expectError(error.InvalidSqlCatalog, validateUniquePredicateExpressionsForColumns(&columns, &.{nondeterministic_condition}));

    try std.testing.expect(sqlExpressionTypeIsTextLike(.keyword));
    try std.testing.expect(!sqlExpressionTypeIsTextLike(.json));
    try std.testing.expect(sqlExpressionTypesComparable(.keyword, .text));
    try std.testing.expect(sqlExpressionTypesComparable(.datetime, .numeric));
    try std.testing.expect(!sqlExpressionTypesComparable(.json, .text));
    try std.testing.expect(sqlExpressionTypeIsOrderable(.boolean));
    try std.testing.expect(!sqlExpressionTypeIsOrderable(.json));
    try std.testing.expect(sqlAggregateMinMaxTypeAllowed(.datetime));
    try std.testing.expect(!sqlAggregateMinMaxTypeAllowed(.boolean));
    try std.testing.expect(sqlAggregateModeTypeAllowed(.boolean));
    try std.testing.expect(sqlExpressionTypeIsOrderKey(.json));
    try std.testing.expect(sqlExpressionResultTypesCompatible(.keyword, .text));
}

test "sql adapter lower expr validates DDL expression catalog constraints" {
    const columns = [_]runtime_schema.RelationalColumn{
        .{ .name = "status", .path = "status", .field_type = .keyword },
        .{ .name = "amount", .path = "amount", .field_type = .numeric },
        .{ .name = "created_at", .path = "created_at", .field_type = .datetime },
        .{ .name = "updated_at", .path = "updated_at", .field_type = .datetime },
        .{ .name = "metadata", .path = "metadata", .field_type = .json },
    };
    const pk_columns = [_]runtime_schema.RelationalColumn{
        .{ .name = "id", .path = "id", .field_type = .keyword, .nullable = false },
        .{ .name = "status", .path = "status", .field_type = .keyword },
    };
    const periods = [_]runtime_schema.RelationalPeriod{.{ .name = "valid_at", .start_column = "created_at", .end_column = "updated_at" }};
    const lower_status: runtime_schema.RelationalRowsExpression = .{
        .kind = .lower,
        .operands = &.{.{ .kind = .field, .field = "status" }},
    };

    try validateRelationalColumnCatalog(&columns);
    try std.testing.expectError(error.InvalidSqlCatalog, validateRelationalColumnCatalog(&.{.{
        .name = "amount",
        .path = "amount",
        .field_type = .numeric,
        .collation = "C",
    }}));
    try validatePrimaryKeyColumns(&pk_columns, .{ .columns = &.{"id"}, .include_columns = &.{"status"} });
    try std.testing.expectError(error.InvalidSqlCatalog, validatePrimaryKeyColumns(&columns, .{ .columns = &.{"status"} }));

    try validateCheckForColumns(&columns, .{ .name = "amount_positive", .field = "amount", .op = .gt, .value_json = "0" });
    try std.testing.expectError(error.InvalidSqlCatalog, validateCheckForColumns(&columns, .{ .name = "bad_json_order", .field = "metadata", .op = .gt, .value_json = "{}" }));
    try validateRelationalCheckCatalog(&columns, &.{.{ .name = "amount_positive", .field = "amount", .op = .gt, .value_json = "0" }});
    try std.testing.expectError(error.InvalidSqlCatalog, validateRelationalCheckCatalog(&columns, &.{
        .{ .name = "dup_check", .field = "amount", .op = .gt, .value_json = "0" },
        .{ .name = "dup_check", .field = "amount", .op = .lt, .value_json = "10" },
    }));

    try validateGeneratedColumnForColumns(&columns, .{
        .name = "status_lower",
        .path = "status_lower",
        .field_type = .keyword,
        .generated = .{ .op = .expression, .expression = lower_status },
    });
    try std.testing.expectError(error.InvalidSqlCatalog, validateGeneratedColumnForColumns(&columns, .{
        .name = "status",
        .path = "status",
        .field_type = .keyword,
        .generated = .{ .op = .lower, .field = "status" },
    }));

    try validateCreateIndexIncludeColumns(&columns, &.{"status"}, &.{"amount"});
    try std.testing.expectError(error.InvalidSqlCatalog, validateCreateIndexIncludeColumns(&columns, &.{"status"}, &.{"status"}));
    try std.testing.expectError(error.InvalidSqlCatalog, validateCreateIndexIncludeColumns(&columns, &.{"status"}, &.{"metadata"}));

    try validateUniquePredicatesForColumns(&columns, &.{.{ .field = "status", .op = .eq, .value_json = "\"open\"" }});
    try std.testing.expectError(error.InvalidSqlCatalog, validateUniquePredicatesForColumns(&columns, &.{.{ .field = "metadata", .op = .is_not_null }}));

    try validateUniqueConstraintForColumns(&columns, &periods, .{
        .name = "status_key",
        .columns = &.{"status"},
        .expressions = &.{.{ .op = .expression, .expression = lower_status }},
        .include_columns = &.{"amount"},
        .without_overlaps_period = "valid_at",
    });
    try std.testing.expectError(error.InvalidSqlCatalog, validateUniqueConstraintForColumns(&columns, &periods, .{
        .name = "metadata_key",
        .columns = &.{"metadata"},
    }));
    try std.testing.expectError(error.InvalidSqlCatalog, validateUniqueConstraintForColumns(&columns, &periods, .{
        .name = "empty_key",
    }));
    try validateUniqueConstraintCatalog(&columns, &periods, &.{.{ .name = "status_key", .columns = &.{"status"} }});
    try std.testing.expectError(error.InvalidSqlCatalog, validateUniqueConstraintCatalog(&columns, &periods, &.{
        .{ .name = "dup_key", .columns = &.{"status"} },
        .{ .name = "dup_key", .columns = &.{"amount"} },
    }));

    try validateForeignKeyCatalog(&columns, &periods, &.{.{
        .name = "status_parent_fkey",
        .parent_table = "parent_statuses",
        .child_columns = &.{"status"},
        .parent_columns = &.{"status"},
    }});
    try std.testing.expectError(error.InvalidSqlCatalog, validateForeignKeyCatalog(&columns, &periods, &.{
        .{ .name = "dup_fkey", .parent_table = "parent_statuses", .child_columns = &.{"status"}, .parent_columns = &.{"status"} },
        .{ .name = "dup_fkey", .parent_table = "parent_statuses", .child_columns = &.{"amount"}, .parent_columns = &.{"amount"} },
    }));
}
