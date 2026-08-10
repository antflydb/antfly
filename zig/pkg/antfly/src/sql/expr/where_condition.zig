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

const db_mod = struct {
    pub const types = @import("../../storage/db/types.zig");
};
const expr_condition = @import("condition.zig");
const expr_generated_validate = @import("generated_validate.zig");
const expr_operator = @import("operator.zig");
const expr_predicate = @import("predicate.zig");
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
const freeExpression = plan_mod.freeExpression;
const freeExpressionCondition = plan_mod.freeExpressionCondition;
const freeExpressionPredicateGroups = plan_mod.freeExpressionPredicateGroups;

pub const UniquePredicateWhereExpressionParserOptions = struct {
    type_context: expr_type.RowExpressionTypeContext,
    defer_row_expression_field_validation: bool = false,
    case_expression_hooks: expr_row_parse.CaseExpressionParserHooks,
    generated_expression_ast: ?*const generated_parser.GeneratedSqlExpressionAst = null,
    require_exact_generated_expression: bool = false,
};

pub const ExpressionWhereConditionRowParserOptions = struct {
    params: []const value_mod.SqlValue = &.{},
    select_context_hooks: ?expr_row_parse.SelectParserContextHooks = null,
    joined_context_hooks: ?expr_row_parse.JoinedExpressionParserContextHooks = null,
    generated_expression_ast: ?*const generated_parser.GeneratedSqlExpressionAst = null,
    require_exact_generated_expression: bool = false,
    row_expression_hooks: expr_row_parse.RowExpressionParserHooks,
    arithmetic_hooks: expr_row_parse.ArithmeticExpressionParserHooks,
    variadic_hooks: expr_row_parse.VariadicRowExpressionParserHooks,
};

pub const ExpressionWhereConditionsParserOptions = ExpressionWhereConditionRowParserOptions;
pub const ExpressionWhereConditionAlternativesParserOptions = ExpressionWhereConditionRowParserOptions;

fn parseExpressionWhereConditionRowExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    type_context: expr_type.RowExpressionTypeContext,
    options: ExpressionWhereConditionRowParserOptions,
) !db_mod.types.RelationalRowsExpression {
    const start = pos.*;
    const expression = try expr_row_parse.parseRowExpressionAlloc(
        alloc,
        tokens,
        pos,
        type_context,
        options.row_expression_hooks,
        options.arithmetic_hooks,
        options.variadic_hooks,
    );
    errdefer freeExpression(alloc, expression);
    if (options.require_exact_generated_expression) {
        try expr_generated_validate.validateGeneratedRowExpressionIdentityStrictWithContext(
            .{ .alloc = alloc, .params = options.params },
            tokens,
            start,
            pos.*,
            expression,
            options.generated_expression_ast,
        );
    } else {
        try expr_generated_validate.validateGeneratedRowExpressionIdentity(
            tokens,
            start,
            pos.*,
            expression,
            options.generated_expression_ast,
        );
    }
    return expression;
}

pub fn parseExpressionRegexpMatchConditionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    type_context: expr_type.RowExpressionTypeContext,
    expression: db_mod.types.RelationalRowsExpression,
    case_insensitive: bool,
    negated: bool,
    options: ExpressionWhereConditionRowParserOptions,
) !db_mod.types.RelationalRowsExpressionCondition {
    const pattern_expression = try parseExpressionWhereConditionRowExpressionAlloc(alloc, tokens, pos, type_context, options);
    defer freeExpression(alloc, pattern_expression);
    return try expr_predicate.expressionRegexpMatchConditionAlloc(alloc, type_context, expression, pattern_expression, case_insensitive, negated);
}

pub fn parseCaseExpressionConditionWithSelectSchemaAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    schema: runtime_schema.TableSchema,
    context_hooks: expr_row_parse.SelectParserContextHooks,
    hooks: expr_row_parse.CaseExpressionParserHooks,
) !db_mod.types.RelationalRowsExpressionCondition {
    return try parseCaseExpressionConditionWithSelectSchemaAndOperatorAlloc(
        alloc,
        tokens,
        pos,
        schema,
        context_hooks,
        hooks,
        null,
    );
}

pub fn parseCaseExpressionConditionWithSelectSchemaAndOperatorAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    schema: runtime_schema.TableSchema,
    context_hooks: expr_row_parse.SelectParserContextHooks,
    hooks: expr_row_parse.CaseExpressionParserHooks,
    operator_token_index: ?*usize,
) !db_mod.types.RelationalRowsExpressionCondition {
    const previous_context = context_hooks.get_context(context_hooks.ptr);
    var context = previous_context;
    context.schema = schema;
    context_hooks.set_context(context_hooks.ptr, context);
    defer context_hooks.set_context(context_hooks.ptr, previous_context);

    return try expr_row_parse.parseCaseExpressionConditionWithOperatorAlloc(
        alloc,
        tokens,
        pos,
        context_hooks.row_expression_type_context(context_hooks.ptr),
        context.defer_row_expression_field_validation,
        hooks,
        operator_token_index,
    );
}

pub fn parseBareBooleanWhereExpressionWithSelectSchemaAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    schema: runtime_schema.TableSchema,
    expression_predicates: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionCondition),
    context_hooks: expr_row_parse.SelectParserContextHooks,
    options: expr_predicate.BareBooleanWhereExpressionParserOptions,
) !void {
    const previous_context = context_hooks.get_context(context_hooks.ptr);
    var context = previous_context;
    context.schema = schema;
    context_hooks.set_context(context_hooks.ptr, context);
    defer context_hooks.set_context(context_hooks.ptr, previous_context);

    return try expr_predicate.parseBareBooleanWhereExpressionAlloc(
        alloc,
        tokens,
        pos,
        context_hooks.row_expression_type_context(context_hooks.ptr),
        expression_predicates,
        options,
    );
}

pub fn parseExpressionNotWhereAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    params: []const value_mod.SqlValue,
    type_context: expr_type.RowExpressionTypeContext,
    defer_row_expression_field_validation: bool,
    expression_not_predicates: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionPredicateGroup),
    hooks: ExpressionWhereConditionAlternativesParserOptions,
) !void {
    return try parseExpressionNotWhereWithGeneratedAlloc(
        alloc,
        tokens,
        pos,
        params,
        type_context,
        defer_row_expression_field_validation,
        expression_not_predicates,
        hooks,
        null,
    );
}

pub fn parseExpressionNotWhereWithGeneratedAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    params: []const value_mod.SqlValue,
    type_context: expr_type.RowExpressionTypeContext,
    defer_row_expression_field_validation: bool,
    expression_not_predicates: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionPredicateGroup),
    hooks: ExpressionWhereConditionAlternativesParserOptions,
    generated_expression_ast: ?*const generated_parser.GeneratedSqlExpressionAst,
) !void {
    if (generated_expression_ast) |expression| {
        if (expression.kind != .logical_not) return error.UnsupportedSqlShape;
    }
    try parser.expectKeyword(tokens, pos, "not");
    try parser.expectToken(tokens, pos, .lparen);
    while (true) {
        var groups = std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionPredicateGroup).empty;
        var groups_transferred = false;
        defer groups.deinit(alloc);
        errdefer {
            if (!groups_transferred) freeExpressionPredicateGroups(alloc, groups.items);
        }
        try groups.append(alloc, .{ .conditions = &.{} });

        while (true) {
            var alternatives = std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionPredicateGroup).empty;
            defer alternatives.deinit(alloc);
            errdefer freeExpressionPredicateGroups(alloc, alternatives.items);
            var hooks_with_generated = hooks;
            if (generated_expression_ast != null) {
                hooks_with_generated.generated_expression_ast = try expr_generated_validate.generatedPredicateExpressionAtStart(tokens, pos.*, generated_expression_ast);
                hooks_with_generated.require_exact_generated_expression = true;
            }
            try parseExpressionWhereConditionAlternativesAlloc(
                alloc,
                tokens,
                pos,
                params,
                type_context,
                defer_row_expression_field_validation,
                &alternatives,
                hooks_with_generated,
            );
            try expr_condition.andExpressionPredicateAlternatives(alloc, &groups, alternatives.items);
            freeExpressionPredicateGroups(alloc, alternatives.items);
            if (!parser.matchKeyword(tokens, pos, "and")) break;
        }
        if (groups.items.len == 0) return error.UnsupportedSqlShape;
        try expression_not_predicates.appendSlice(alloc, groups.items);
        groups_transferred = true;

        if (!parser.matchKeyword(tokens, pos, "or")) break;
    }
    try parser.expectToken(tokens, pos, .rparen);
}

pub fn parseUniquePredicateWhereExpressionConditionsAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    options: UniquePredicateWhereExpressionParserOptions,
) ![]const db_mod.types.RelationalRowsExpressionCondition {
    var conditions = std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionCondition).empty;
    errdefer {
        plan_mod.freeExpressionConditions(alloc, conditions.items);
        conditions.deinit(alloc);
    }
    while (true) {
        const condition_start = pos.*;
        var condition = try expr_row_parse.parseCaseExpressionConditionAlloc(
            alloc,
            tokens,
            pos,
            options.type_context,
            options.defer_row_expression_field_validation,
            options.case_expression_hooks,
        );
        var condition_transferred = false;
        errdefer if (!condition_transferred) freeExpressionCondition(alloc, condition);
        if (options.require_exact_generated_expression) {
            try expr_generated_validate.validateGeneratedExpressionConditionIdentityStrict(tokens, condition_start, pos.*, condition, options.generated_expression_ast);
        } else {
            try expr_generated_validate.validateGeneratedExpressionConditionIdentity(tokens, condition_start, pos.*, condition, options.generated_expression_ast);
        }
        try conditions.append(alloc, condition);
        condition = undefined;
        condition_transferred = true;
        if (!parser.matchKeyword(tokens, pos, "and")) break;
    }
    return try conditions.toOwnedSlice(alloc);
}

pub const DeferredUniquePredicateWhereExpressionConditionsParserOptions = struct {
    context_hooks: expr_row_parse.SelectParserContextHooks,
    case_expression_hooks: expr_row_parse.CaseExpressionParserHooks,
};

pub fn parseUniquePredicateWhereExpressionConditionsWithDeferredFieldValidationAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    options: DeferredUniquePredicateWhereExpressionConditionsParserOptions,
) ![]const db_mod.types.RelationalRowsExpressionCondition {
    const previous_context = expr_row_parse.setDeferredRowExpressionFieldValidation(options.context_hooks);
    defer options.context_hooks.set_context(options.context_hooks.ptr, previous_context);
    const context = options.context_hooks.get_context(options.context_hooks.ptr);

    return try parseUniquePredicateWhereExpressionConditionsAlloc(
        alloc,
        tokens,
        pos,
        .{
            .type_context = options.context_hooks.row_expression_type_context(options.context_hooks.ptr),
            .defer_row_expression_field_validation = context.defer_row_expression_field_validation,
            .case_expression_hooks = options.case_expression_hooks,
        },
    );
}

pub const DeferredExpressionWhereConditionAlternativesParserOptions = struct {
    params: []const value_mod.SqlValue = &.{},
    context_hooks: expr_row_parse.SelectParserContextHooks,
    alternatives_hooks: ExpressionWhereConditionAlternativesParserOptions,
};

pub fn parseExpressionWhereConditionAlternativesWithDeferredFieldValidationAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    alternatives: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionPredicateGroup),
    options: DeferredExpressionWhereConditionAlternativesParserOptions,
) !void {
    const previous_context = expr_row_parse.setDeferredRowExpressionFieldValidation(options.context_hooks);
    defer options.context_hooks.set_context(options.context_hooks.ptr, previous_context);

    const context = options.context_hooks.get_context(options.context_hooks.ptr);
    return try parseExpressionWhereConditionAlternativesAlloc(
        alloc,
        tokens,
        pos,
        options.params,
        options.context_hooks.row_expression_type_context(options.context_hooks.ptr),
        context.defer_row_expression_field_validation,
        alternatives,
        options.alternatives_hooks,
    );
}

pub const JoinedExpressionNotWhereParserOptions = struct {
    params: []const value_mod.SqlValue = &.{},
    context_hooks: expr_row_parse.JoinedExpressionParserContextHooks,
    alternatives_hooks: ExpressionWhereConditionAlternativesParserOptions,
    generated_expression_ast: ?*const generated_parser.GeneratedSqlExpressionAst = null,
};

pub fn parseExpressionNotWhereWithTableQualifiersAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    target_table: plan_mod.TableAlias,
    source_table: plan_mod.TableAlias,
    expression_not_predicates: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionPredicateGroup),
    options: JoinedExpressionNotWhereParserOptions,
) !void {
    const target_qualifiers = [_][]const u8{ target_table.name, target_table.alias };
    const source_qualifiers = [_][]const u8{ source_table.name, source_table.alias };
    const previous_context = expr_row_parse.setJoinedExpressionContextForTables(options.context_hooks, target_qualifiers[0..], source_qualifiers[0..]);
    defer options.context_hooks.set_context(options.context_hooks.ptr, previous_context);

    return try parseExpressionNotWhereWithGeneratedAlloc(
        alloc,
        tokens,
        pos,
        options.params,
        options.context_hooks.row_expression_type_context(options.context_hooks.ptr),
        options.context_hooks.get_context(options.context_hooks.ptr).defer_row_expression_field_validation,
        expression_not_predicates,
        options.alternatives_hooks,
        options.generated_expression_ast,
    );
}

pub const JoinedExpressionOrWhereParserOptions = struct {
    params: []const value_mod.SqlValue = &.{},
    context_hooks: expr_row_parse.JoinedExpressionParserContextHooks,
    alternatives_hooks: ExpressionWhereConditionAlternativesParserOptions,
    generated_expression_ast: ?*const generated_parser.GeneratedSqlExpressionAst = null,
};

pub fn parseExpressionOrWhereWithTableQualifiersAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    target_table: plan_mod.TableAlias,
    source_table: plan_mod.TableAlias,
    expression_or_predicates: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionPredicateGroup),
    options: JoinedExpressionOrWhereParserOptions,
) !void {
    const target_qualifiers = [_][]const u8{ target_table.name, target_table.alias };
    const source_qualifiers = [_][]const u8{ source_table.name, source_table.alias };
    const previous_context = expr_row_parse.setJoinedExpressionContextForTables(options.context_hooks, target_qualifiers[0..], source_qualifiers[0..]);
    defer options.context_hooks.set_context(options.context_hooks.ptr, previous_context);

    return try parseExpressionOrWhereWithGeneratedAlloc(
        alloc,
        tokens,
        pos,
        options.params,
        options.context_hooks.row_expression_type_context(options.context_hooks.ptr),
        options.context_hooks.get_context(options.context_hooks.ptr).defer_row_expression_field_validation,
        expression_or_predicates,
        options.alternatives_hooks,
        options.generated_expression_ast,
    );
}

pub const JoinedExpressionWhereConditionsParserOptions = struct {
    params: []const value_mod.SqlValue = &.{},
    context_hooks: expr_row_parse.JoinedExpressionParserContextHooks,
    condition_hooks: ExpressionWhereConditionsParserOptions,
    generated_expression_ast: ?*const generated_parser.GeneratedSqlExpressionAst = null,
};

pub fn parseExpressionWhereConditionsWithTableQualifiersAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    target_table: plan_mod.TableAlias,
    source_table: plan_mod.TableAlias,
    expression_predicates: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionCondition),
    expression_or_predicates: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionPredicateGroup),
    expression_not_predicates: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionPredicateGroup),
    options: JoinedExpressionWhereConditionsParserOptions,
) !void {
    const target_qualifiers = [_][]const u8{ target_table.name, target_table.alias };
    const source_qualifiers = [_][]const u8{ source_table.name, source_table.alias };
    const previous_context = expr_row_parse.setJoinedExpressionContextForTables(options.context_hooks, target_qualifiers[0..], source_qualifiers[0..]);
    defer options.context_hooks.set_context(options.context_hooks.ptr, previous_context);

    const context = options.context_hooks.get_context(options.context_hooks.ptr);
    var condition_hooks = options.condition_hooks;
    condition_hooks.generated_expression_ast = options.generated_expression_ast;
    condition_hooks.require_exact_generated_expression = options.generated_expression_ast != null;
    return try parseExpressionWhereConditionsAlloc(
        alloc,
        tokens,
        pos,
        options.params,
        options.context_hooks.row_expression_type_context(options.context_hooks.ptr),
        context.defer_row_expression_field_validation,
        expression_predicates,
        expression_or_predicates,
        expression_not_predicates,
        condition_hooks,
    );
}

pub fn parseExpressionOrWhereAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    params: []const value_mod.SqlValue,
    type_context: expr_type.RowExpressionTypeContext,
    defer_row_expression_field_validation: bool,
    expression_or_predicates: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionPredicateGroup),
    hooks: ExpressionWhereConditionAlternativesParserOptions,
) !void {
    return try parseExpressionOrWhereWithGeneratedAlloc(
        alloc,
        tokens,
        pos,
        params,
        type_context,
        defer_row_expression_field_validation,
        expression_or_predicates,
        hooks,
        null,
    );
}

pub fn parseExpressionOrWhereWithGeneratedAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    params: []const value_mod.SqlValue,
    type_context: expr_type.RowExpressionTypeContext,
    defer_row_expression_field_validation: bool,
    expression_or_predicates: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionPredicateGroup),
    hooks: ExpressionWhereConditionAlternativesParserOptions,
    generated_expression_ast: ?*const generated_parser.GeneratedSqlExpressionAst,
) !void {
    while (true) {
        const parenthesized = parser.matchToken(tokens, pos, .lparen) != null;
        while (true) {
            var groups = std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionPredicateGroup).empty;
            var groups_transferred = false;
            defer groups.deinit(alloc);
            errdefer {
                if (!groups_transferred) freeExpressionPredicateGroups(alloc, groups.items);
            }
            try groups.append(alloc, .{ .conditions = &.{} });

            while (true) {
                var alternatives = std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionPredicateGroup).empty;
                defer alternatives.deinit(alloc);
                errdefer freeExpressionPredicateGroups(alloc, alternatives.items);
                var hooks_with_generated = hooks;
                if (generated_expression_ast != null) {
                    hooks_with_generated.generated_expression_ast = try expr_generated_validate.generatedPredicateExpressionAtStart(tokens, pos.*, generated_expression_ast);
                    hooks_with_generated.require_exact_generated_expression = true;
                }
                try parseExpressionWhereConditionAlternativesAlloc(
                    alloc,
                    tokens,
                    pos,
                    params,
                    type_context,
                    defer_row_expression_field_validation,
                    &alternatives,
                    hooks_with_generated,
                );
                try expr_condition.andExpressionPredicateAlternatives(alloc, &groups, alternatives.items);
                freeExpressionPredicateGroups(alloc, alternatives.items);
                if (!parser.matchKeyword(tokens, pos, "and")) break;
            }
            if (groups.items.len == 0) return error.UnsupportedSqlShape;
            try expression_or_predicates.appendSlice(alloc, groups.items);
            groups_transferred = true;

            if (!parenthesized or !parser.matchKeyword(tokens, pos, "or")) break;
        }
        if (parenthesized) try parser.expectToken(tokens, pos, .rparen);

        if (!parser.matchKeyword(tokens, pos, "or")) break;
    }
}

pub fn parseExpressionWhereConditionAlternativesAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    params: []const value_mod.SqlValue,
    type_context: expr_type.RowExpressionTypeContext,
    defer_row_expression_field_validation: bool,
    alternatives: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionPredicateGroup),
    options: ExpressionWhereConditionAlternativesParserOptions,
) !void {
    if (value_mod.matchStandaloneSqlBooleanLiteral(tokens, pos)) |enabled| {
        try expr_condition.appendBooleanConstantExpressionGroup(alloc, alternatives, enabled);
        return;
    }

    const lhs = try parseExpressionWhereConditionRowExpressionAlloc(
        alloc,
        tokens,
        pos,
        type_context,
        options,
    );
    var lhs_transferred = false;
    errdefer if (!lhs_transferred) freeExpression(alloc, lhs);

    if (expr_operator.matchRegexPredicateOperator(tokens, pos)) |regex_operator| {
        try expr_generated_validate.validateGeneratedRegexPredicateExpression(
            options.generated_expression_ast,
            tokens,
            pos.* - 1,
            regex_operator.case_insensitive,
            regex_operator.negated,
        );
        const condition = try parseExpressionRegexpMatchConditionAlloc(
            alloc,
            tokens,
            pos,
            type_context,
            lhs,
            regex_operator.case_insensitive,
            regex_operator.negated,
            options,
        );
        try expr_condition.appendExpressionConditionGroup(alloc, alternatives, condition);
        freeExpression(alloc, lhs);
        lhs_transferred = true;
        return;
    }

    if (parser.matchKeyword(tokens, pos, "like") or parser.matchKeyword(tokens, pos, "ilike")) {
        const case_insensitive = tokens[pos.* - 1].matchesKeywordTag(.ilike);
        const generated_kind: generated_parser.GeneratedSqlExpressionKind = if (case_insensitive) .ilike else .like;
        const generated_quantifier_token_index: ?usize = if (expr_token.tokenAtIsAnySomeOrAll(tokens, pos.*)) pos.* else null;
        try expr_generated_validate.validateGeneratedPatternPredicateIdentity(options.generated_expression_ast, generated_kind, tokens, pos.* - 1, generated_quantifier_token_index);
        const condition = if (generated_quantifier_token_index != null)
            try expr_predicate.parseExpressionLikeSetConditionAlloc(alloc, tokens, pos, params, type_context, lhs, case_insensitive, false)
        else
            try expr_predicate.parseExpressionLikeConditionAlloc(alloc, tokens, pos, params, type_context, lhs, case_insensitive, false);
        try expr_condition.appendExpressionConditionGroup(alloc, alternatives, condition);
        freeExpression(alloc, lhs);
        lhs_transferred = true;
        return;
    }

    if (parser.matchKeyword(tokens, pos, "not")) {
        if (parser.matchKeyword(tokens, pos, "like") or parser.matchKeyword(tokens, pos, "ilike")) {
            const case_insensitive = tokens[pos.* - 1].matchesKeywordTag(.ilike);
            const generated_kind: generated_parser.GeneratedSqlExpressionKind = if (case_insensitive) .not_ilike else .not_like;
            const generated_quantifier_token_index: ?usize = if (expr_token.tokenAtIsAnySomeOrAll(tokens, pos.*)) pos.* else null;
            try expr_generated_validate.validateGeneratedPatternPredicateIdentity(options.generated_expression_ast, generated_kind, tokens, pos.* - 1, generated_quantifier_token_index);
            const condition = if (generated_quantifier_token_index != null)
                try expr_predicate.parseExpressionLikeSetConditionAlloc(alloc, tokens, pos, params, type_context, lhs, case_insensitive, true)
            else
                try expr_predicate.parseExpressionLikeConditionAlloc(alloc, tokens, pos, params, type_context, lhs, case_insensitive, true);
            try expr_condition.appendExpressionConditionGroup(alloc, alternatives, condition);
            freeExpression(alloc, lhs);
            lhs_transferred = true;
            return;
        }
        return error.UnsupportedSqlShape;
    }

    if (parser.matchKeyword(tokens, pos, "in")) {
        try expr_generated_validate.validateGeneratedSetOrBetweenPredicateIdentity(options.generated_expression_ast, .in_list, tokens, pos.* - 1, null, null);
        try expr_predicate.appendExpressionInPredicateGroupsAlloc(alloc, tokens, pos, params, type_context, alternatives, lhs);
        freeExpression(alloc, lhs);
        lhs_transferred = true;
        return;
    }

    if (parser.matchKeyword(tokens, pos, "between")) {
        const operator_token_index = pos.* - 1;
        const modifier_token_index = expr_generated_validate.betweenModifierTokenIndex(tokens, pos.*);
        try expr_generated_validate.validateGeneratedSetOrBetweenPredicateIdentity(options.generated_expression_ast, .between, tokens, operator_token_index, null, modifier_token_index);
        const symmetric = expr_token.matchBetweenSymmetricMode(tokens, pos);
        const lower_expression = try expr_row_parse.parseRowExpressionAlloc(
            alloc,
            tokens,
            pos,
            type_context,
            options.row_expression_hooks,
            options.arithmetic_hooks,
            options.variadic_hooks,
        );
        var lower_transferred = false;
        errdefer if (!lower_transferred) freeExpression(alloc, lower_expression);
        try parser.expectKeyword(tokens, pos, "and");
        const upper_expression = try expr_row_parse.parseRowExpressionAlloc(
            alloc,
            tokens,
            pos,
            type_context,
            options.row_expression_hooks,
            options.arithmetic_hooks,
            options.variadic_hooks,
        );
        var upper_transferred = false;
        errdefer if (!upper_transferred) freeExpression(alloc, upper_expression);

        if (symmetric) {
            try expr_condition.appendExpressionBetweenSymmetricGroups(alloc, alternatives, lhs, lower_expression, upper_expression, false);
            freeExpression(alloc, lhs);
            freeExpression(alloc, lower_expression);
            freeExpression(alloc, upper_expression);
            lhs_transferred = true;
            lower_transferred = true;
            upper_transferred = true;
            return;
        }

        const upper_lhs = try cloneExpressionAlloc(alloc, lhs);
        var upper_lhs_transferred = false;
        errdefer if (!upper_lhs_transferred) freeExpression(alloc, upper_lhs);

        const lower_rhs = try alloc.alloc(db_mod.types.RelationalRowsExpression, 1);
        var lower_rhs_transferred = false;
        errdefer if (!lower_rhs_transferred) alloc.free(lower_rhs);
        lower_rhs[0] = lower_expression;

        const upper_rhs = try alloc.alloc(db_mod.types.RelationalRowsExpression, 1);
        var upper_rhs_transferred = false;
        errdefer if (!upper_rhs_transferred) alloc.free(upper_rhs);
        upper_rhs[0] = upper_expression;

        const conditions = try alloc.alloc(db_mod.types.RelationalRowsExpressionCondition, 2);
        var conditions_transferred = false;
        errdefer if (!conditions_transferred) alloc.free(conditions);

        conditions[0] = .{
            .lhs = lhs,
            .op = .gte,
            .rhs = lower_rhs,
        };
        lhs_transferred = true;
        lower_transferred = true;
        lower_rhs_transferred = true;

        conditions[1] = .{
            .lhs = upper_lhs,
            .op = .lte,
            .rhs = upper_rhs,
        };
        upper_lhs_transferred = true;
        upper_transferred = true;
        upper_rhs_transferred = true;

        try alternatives.append(alloc, .{ .conditions = conditions });
        conditions_transferred = true;
        return;
    }

    const op_token_index = pos.*;
    const op: runtime_schema.RelationalCheckOp = if (try expr_operator.parseExpressionIsTailIf(tokens, pos, .{
        .allow_boolean_unknown = true,
        .allow_boolean_literal = true,
        .allow_boolean_literal_negation = true,
    })) |is_tail| blk: {
        switch (is_tail.kind) {
            .distinct_comparison, .null_test => try expr_generated_validate.validateGeneratedIsTailPredicateExpression(options.generated_expression_ast, tokens, op_token_index, is_tail),
            .boolean_unknown => {
                try expr_generated_validate.validateGeneratedIsTailPredicateExpression(options.generated_expression_ast, tokens, op_token_index, is_tail);
                try type_context.validateBooleanRowExpression(lhs);
                const condition = expr_condition.expressionNullTestCondition(lhs, is_tail.op);
                lhs_transferred = true;
                try expr_condition.appendExpressionConditionGroup(alloc, alternatives, condition);
                return;
            },
            .boolean_literal => {
                try expr_generated_validate.validateGeneratedIsTailPredicateExpression(options.generated_expression_ast, tokens, op_token_index, is_tail);
                try type_context.validateBooleanRowExpression(lhs);
                if (is_tail.boolean_negated) {
                    try expr_condition.appendExpressionBooleanIsNotGroups(alloc, alternatives, lhs, is_tail.boolean_value);
                    freeExpression(alloc, lhs);
                    lhs_transferred = true;
                    return;
                }
                const condition = try expr_condition.expressionBooleanComparisonConditionAlloc(alloc, lhs, is_tail.op, is_tail.boolean_value);
                lhs_transferred = true;
                try expr_condition.appendExpressionConditionGroup(alloc, alternatives, condition);
                return;
            },
        }
        break :blk is_tail.op;
    } else if (expr_operator.matchPostfixNullTest(tokens, pos)) |postfix_null_test| blk: {
        try expr_generated_validate.validateGeneratedPostfixNullPredicateExpression(options.generated_expression_ast, tokens, op_token_index, postfix_null_test);
        break :blk postfix_null_test;
    } else try expr_operator.parseComparisonOp(tokens, pos);

    if (op == .eq and expr_token.matchAnyOrSomeKeyword(tokens, pos)) {
        try expr_generated_validate.validateGeneratedQuantifiedPredicateIdentity(options.generated_expression_ast, tokens, op_token_index, pos.* - 1);
        try parser.expectToken(tokens, pos, .lparen);
        const values_json = try value_mod.parseJsonArrayValueAlloc(alloc, tokens, pos, params);
        defer alloc.free(values_json);
        try parser.expectToken(tokens, pos, .rparen);
        try expr_predicate.appendExpressionValuesJsonOrGroups(alloc, type_context, alternatives, lhs, values_json);
        freeExpression(alloc, lhs);
        lhs_transferred = true;
        return;
    }
    if (op == .ne and expr_token.matchAnyOrSomeKeyword(tokens, pos)) {
        try expr_generated_validate.validateGeneratedQuantifiedPredicateIdentity(options.generated_expression_ast, tokens, op_token_index, pos.* - 1);
        try parser.expectToken(tokens, pos, .lparen);
        const values_json = try value_mod.parseJsonArrayValueAlloc(alloc, tokens, pos, params);
        defer alloc.free(values_json);
        try parser.expectToken(tokens, pos, .rparen);
        try expr_predicate.appendExpressionValuesJsonComparisonGroups(alloc, type_context, alternatives, lhs, values_json, .ne);
        freeExpression(alloc, lhs);
        lhs_transferred = true;
        return;
    }
    if (op == .eq and parser.matchKeyword(tokens, pos, "all")) {
        try expr_generated_validate.validateGeneratedQuantifiedPredicateIdentity(options.generated_expression_ast, tokens, op_token_index, pos.* - 1);
        try parser.expectToken(tokens, pos, .lparen);
        const values_json = try value_mod.parseJsonArrayValueAlloc(alloc, tokens, pos, params);
        defer alloc.free(values_json);
        try parser.expectToken(tokens, pos, .rparen);
        try expr_predicate.appendExpressionValuesJsonConjunctionGroup(alloc, type_context, alternatives, lhs, values_json, .eq);
        freeExpression(alloc, lhs);
        lhs_transferred = true;
        return;
    }
    if (op == .ne and parser.matchKeyword(tokens, pos, "all")) {
        try expr_generated_validate.validateGeneratedQuantifiedPredicateIdentity(options.generated_expression_ast, tokens, op_token_index, pos.* - 1);
        try parser.expectToken(tokens, pos, .lparen);
        const values_json = try value_mod.parseJsonArrayValueAlloc(alloc, tokens, pos, params);
        defer alloc.free(values_json);
        try parser.expectToken(tokens, pos, .rparen);
        try expr_predicate.appendExpressionValuesJsonConjunctionGroup(alloc, type_context, alternatives, lhs, values_json, .ne);
        freeExpression(alloc, lhs);
        lhs_transferred = true;
        return;
    }

    const rhs = switch (op) {
        .is_null, .is_not_null => &.{},
        else => blk: {
            const out = try alloc.alloc(db_mod.types.RelationalRowsExpression, 1);
            var out_transferred = false;
            errdefer if (!out_transferred) alloc.free(out);
            out[0] = try expr_row_parse.parseRowExpressionAlloc(
                alloc,
                tokens,
                pos,
                type_context,
                options.row_expression_hooks,
                options.arithmetic_hooks,
                options.variadic_hooks,
            );
            out_transferred = true;
            break :blk out;
        },
    };
    var rhs_transferred = false;
    errdefer if (!rhs_transferred and rhs.len > 0) {
        for (rhs) |expression| freeExpression(alloc, expression);
        alloc.free(rhs);
    };
    try expr_generated_validate.validateGeneratedRelationalPredicateExpression(options.generated_expression_ast, tokens, op_token_index, op);
    try expr_type.validateExpressionConditionTypes(type_context, defer_row_expression_field_validation, lhs, op, rhs);

    const conditions = try alloc.alloc(db_mod.types.RelationalRowsExpressionCondition, 1);
    var conditions_transferred = false;
    errdefer if (!conditions_transferred) alloc.free(conditions);
    conditions[0] = .{
        .lhs = lhs,
        .op = op,
        .rhs = rhs,
    };
    lhs_transferred = true;
    rhs_transferred = true;
    try alternatives.append(alloc, .{ .conditions = conditions });
    conditions_transferred = true;
}

pub fn parseExpressionWhereConditionsAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    params: []const value_mod.SqlValue,
    type_context: expr_type.RowExpressionTypeContext,
    defer_row_expression_field_validation: bool,
    expression_predicates: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionCondition),
    expression_or_predicates: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionPredicateGroup),
    expression_not_predicates: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionPredicateGroup),
    options: ExpressionWhereConditionsParserOptions,
) !void {
    if (value_mod.matchStandaloneSqlBooleanLiteral(tokens, pos)) |enabled| {
        if (!enabled) try expr_condition.appendBooleanConstantExpressionCondition(alloc, expression_predicates, false);
        return;
    }

    const lhs = try parseExpressionWhereConditionRowExpressionAlloc(alloc, tokens, pos, type_context, options);
    var lhs_transferred = false;
    errdefer if (!lhs_transferred) freeExpression(alloc, lhs);

    if (expr_operator.matchRegexPredicateOperator(tokens, pos)) |regex_operator| {
        try expr_generated_validate.validateGeneratedRegexPredicateExpression(
            options.generated_expression_ast,
            tokens,
            pos.* - 1,
            regex_operator.case_insensitive,
            regex_operator.negated,
        );
        const condition = try parseExpressionRegexpMatchConditionAlloc(
            alloc,
            tokens,
            pos,
            type_context,
            lhs,
            regex_operator.case_insensitive,
            regex_operator.negated,
            options,
        );
        var condition_transferred = false;
        errdefer if (!condition_transferred) freeExpressionCondition(alloc, condition);
        try expression_predicates.append(alloc, condition);
        condition_transferred = true;
        freeExpression(alloc, lhs);
        lhs_transferred = true;
        return;
    }

    if (parser.matchKeyword(tokens, pos, "like") or parser.matchKeyword(tokens, pos, "ilike")) {
        const case_insensitive = tokens[pos.* - 1].matchesKeywordTag(.ilike);
        const generated_kind: generated_parser.GeneratedSqlExpressionKind = if (case_insensitive) .ilike else .like;
        const generated_quantifier_token_index: ?usize = if (expr_token.tokenAtIsAnySomeOrAll(tokens, pos.*)) pos.* else null;
        try expr_generated_validate.validateGeneratedPatternPredicateIdentity(options.generated_expression_ast, generated_kind, tokens, pos.* - 1, generated_quantifier_token_index);
        const condition = if (generated_quantifier_token_index != null)
            try expr_predicate.parseExpressionLikeSetConditionAlloc(alloc, tokens, pos, params, type_context, lhs, case_insensitive, false)
        else
            try expr_predicate.parseExpressionLikeConditionAlloc(alloc, tokens, pos, params, type_context, lhs, case_insensitive, false);
        var condition_transferred = false;
        errdefer if (!condition_transferred) freeExpressionCondition(alloc, condition);
        try expression_predicates.append(alloc, condition);
        condition_transferred = true;
        freeExpression(alloc, lhs);
        lhs_transferred = true;
        return;
    }

    if (parser.matchKeyword(tokens, pos, "in")) {
        try expr_generated_validate.validateGeneratedSetOrBetweenPredicateIdentity(options.generated_expression_ast, .in_list, tokens, pos.* - 1, null, null);
        try expr_predicate.appendExpressionInPredicateGroupsAlloc(alloc, tokens, pos, params, type_context, expression_or_predicates, lhs);
        freeExpression(alloc, lhs);
        lhs_transferred = true;
        return;
    }

    if (parser.matchKeyword(tokens, pos, "not")) {
        const negation_token_index = pos.* - 1;
        if (parser.matchKeyword(tokens, pos, "like") or parser.matchKeyword(tokens, pos, "ilike")) {
            const case_insensitive = tokens[pos.* - 1].matchesKeywordTag(.ilike);
            const generated_kind: generated_parser.GeneratedSqlExpressionKind = if (case_insensitive) .not_ilike else .not_like;
            const generated_quantifier_token_index: ?usize = if (expr_token.tokenAtIsAnySomeOrAll(tokens, pos.*)) pos.* else null;
            try expr_generated_validate.validateGeneratedPatternPredicateIdentity(options.generated_expression_ast, generated_kind, tokens, pos.* - 1, generated_quantifier_token_index);
            const condition = if (generated_quantifier_token_index != null)
                try expr_predicate.parseExpressionLikeSetConditionAlloc(alloc, tokens, pos, params, type_context, lhs, case_insensitive, true)
            else
                try expr_predicate.parseExpressionLikeConditionAlloc(alloc, tokens, pos, params, type_context, lhs, case_insensitive, true);
            var condition_transferred = false;
            errdefer if (!condition_transferred) freeExpressionCondition(alloc, condition);
            try expression_predicates.append(alloc, condition);
            condition_transferred = true;
            freeExpression(alloc, lhs);
            lhs_transferred = true;
            return;
        }
        if (parser.matchKeyword(tokens, pos, "in")) {
            try expr_generated_validate.validateGeneratedSetOrBetweenPredicateIdentity(options.generated_expression_ast, .not_in_list, tokens, pos.* - 1, negation_token_index, null);
            try expr_predicate.appendExpressionInPredicateGroupsAlloc(alloc, tokens, pos, params, type_context, expression_not_predicates, lhs);
            freeExpression(alloc, lhs);
            lhs_transferred = true;
            return;
        }
        try parser.expectKeyword(tokens, pos, "between");
        const operator_token_index = pos.* - 1;
        const modifier_token_index = expr_generated_validate.betweenModifierTokenIndex(tokens, pos.*);
        try expr_generated_validate.validateGeneratedSetOrBetweenPredicateIdentity(options.generated_expression_ast, .not_between, tokens, operator_token_index, negation_token_index, modifier_token_index);
        const symmetric = expr_token.matchBetweenSymmetricMode(tokens, pos);
        const lower_expression = try parseExpressionWhereConditionRowExpressionAlloc(alloc, tokens, pos, type_context, options);
        var lower_transferred = false;
        errdefer if (!lower_transferred) freeExpression(alloc, lower_expression);
        try parser.expectKeyword(tokens, pos, "and");
        const upper_expression = try parseExpressionWhereConditionRowExpressionAlloc(alloc, tokens, pos, type_context, options);
        var upper_transferred = false;
        errdefer if (!upper_transferred) freeExpression(alloc, upper_expression);

        if (symmetric) {
            try expr_condition.appendExpressionBetweenSymmetricGroups(alloc, expression_or_predicates, lhs, lower_expression, upper_expression, true);
            freeExpression(alloc, lhs);
            freeExpression(alloc, lower_expression);
            freeExpression(alloc, upper_expression);
            lhs_transferred = true;
            lower_transferred = true;
            upper_transferred = true;
            return;
        }

        const lower_lhs = try cloneExpressionAlloc(alloc, lhs);
        var lower_lhs_transferred = false;
        errdefer if (!lower_lhs_transferred) freeExpression(alloc, lower_lhs);
        const upper_lhs = try cloneExpressionAlloc(alloc, lhs);
        var upper_lhs_transferred = false;
        errdefer if (!upper_lhs_transferred) freeExpression(alloc, upper_lhs);

        const lower_rhs = try alloc.alloc(db_mod.types.RelationalRowsExpression, 1);
        var lower_rhs_transferred = false;
        errdefer if (!lower_rhs_transferred) alloc.free(lower_rhs);
        lower_rhs[0] = lower_expression;

        const upper_rhs = try alloc.alloc(db_mod.types.RelationalRowsExpression, 1);
        var upper_rhs_transferred = false;
        errdefer if (!upper_rhs_transferred) alloc.free(upper_rhs);
        upper_rhs[0] = upper_expression;

        const lower_conditions = try alloc.alloc(db_mod.types.RelationalRowsExpressionCondition, 1);
        var lower_conditions_transferred = false;
        errdefer if (!lower_conditions_transferred) alloc.free(lower_conditions);
        lower_conditions[0] = .{
            .lhs = lower_lhs,
            .op = .lt,
            .rhs = lower_rhs,
        };
        lower_lhs_transferred = true;
        lower_transferred = true;
        lower_rhs_transferred = true;

        const upper_conditions = try alloc.alloc(db_mod.types.RelationalRowsExpressionCondition, 1);
        var upper_conditions_transferred = false;
        errdefer if (!upper_conditions_transferred) alloc.free(upper_conditions);
        upper_conditions[0] = .{
            .lhs = upper_lhs,
            .op = .gt,
            .rhs = upper_rhs,
        };
        upper_lhs_transferred = true;
        upper_transferred = true;
        upper_rhs_transferred = true;

        try expression_or_predicates.append(alloc, .{ .conditions = lower_conditions });
        lower_conditions_transferred = true;
        try expression_or_predicates.append(alloc, .{ .conditions = upper_conditions });
        upper_conditions_transferred = true;
        freeExpression(alloc, lhs);
        lhs_transferred = true;
        return;
    }

    if (parser.matchKeyword(tokens, pos, "between")) {
        const operator_token_index = pos.* - 1;
        const modifier_token_index = expr_generated_validate.betweenModifierTokenIndex(tokens, pos.*);
        try expr_generated_validate.validateGeneratedSetOrBetweenPredicateIdentity(options.generated_expression_ast, .between, tokens, operator_token_index, null, modifier_token_index);
        const symmetric = expr_token.matchBetweenSymmetricMode(tokens, pos);
        const upper_lhs = try cloneExpressionAlloc(alloc, lhs);
        var upper_lhs_transferred = false;
        errdefer if (!upper_lhs_transferred) freeExpression(alloc, upper_lhs);

        const lower_expression = try parseExpressionWhereConditionRowExpressionAlloc(alloc, tokens, pos, type_context, options);
        var lower_transferred = false;
        errdefer if (!lower_transferred) freeExpression(alloc, lower_expression);
        try parser.expectKeyword(tokens, pos, "and");
        const upper_expression = try parseExpressionWhereConditionRowExpressionAlloc(alloc, tokens, pos, type_context, options);
        var upper_transferred = false;
        errdefer if (!upper_transferred) freeExpression(alloc, upper_expression);

        if (symmetric) {
            freeExpression(alloc, upper_lhs);
            upper_lhs_transferred = true;
            try expr_condition.appendExpressionBetweenSymmetricGroups(alloc, expression_or_predicates, lhs, lower_expression, upper_expression, false);
            freeExpression(alloc, lhs);
            freeExpression(alloc, lower_expression);
            freeExpression(alloc, upper_expression);
            lhs_transferred = true;
            lower_transferred = true;
            upper_transferred = true;
            return;
        }

        const lower_rhs = try alloc.alloc(db_mod.types.RelationalRowsExpression, 1);
        var lower_rhs_transferred = false;
        errdefer if (!lower_rhs_transferred) alloc.free(lower_rhs);
        lower_rhs[0] = lower_expression;

        const upper_rhs = try alloc.alloc(db_mod.types.RelationalRowsExpression, 1);
        var upper_rhs_transferred = false;
        errdefer if (!upper_rhs_transferred) alloc.free(upper_rhs);
        upper_rhs[0] = upper_expression;

        try expression_predicates.append(alloc, .{
            .lhs = lhs,
            .op = .gte,
            .rhs = lower_rhs,
        });
        lhs_transferred = true;
        lower_transferred = true;
        lower_rhs_transferred = true;

        try expression_predicates.append(alloc, .{
            .lhs = upper_lhs,
            .op = .lte,
            .rhs = upper_rhs,
        });
        upper_lhs_transferred = true;
        upper_transferred = true;
        upper_rhs_transferred = true;
        return;
    }

    const op_token_index = pos.*;
    const op: runtime_schema.RelationalCheckOp = if (try expr_operator.parseExpressionIsTailIf(tokens, pos, .{
        .allow_boolean_unknown = true,
        .allow_boolean_literal = true,
        .allow_boolean_literal_negation = true,
    })) |is_tail| blk: {
        switch (is_tail.kind) {
            .distinct_comparison, .null_test => try expr_generated_validate.validateGeneratedIsTailPredicateExpression(options.generated_expression_ast, tokens, op_token_index, is_tail),
            .boolean_unknown => {
                try expr_generated_validate.validateGeneratedIsTailPredicateExpression(options.generated_expression_ast, tokens, op_token_index, is_tail);
                try type_context.validateBooleanRowExpression(lhs);
                try expression_predicates.append(alloc, expr_condition.expressionNullTestCondition(lhs, is_tail.op));
                lhs_transferred = true;
                return;
            },
            .boolean_literal => {
                try expr_generated_validate.validateGeneratedIsTailPredicateExpression(options.generated_expression_ast, tokens, op_token_index, is_tail);
                try type_context.validateBooleanRowExpression(lhs);
                if (is_tail.boolean_negated) {
                    try expr_condition.appendExpressionBooleanIsNotGroups(alloc, expression_or_predicates, lhs, is_tail.boolean_value);
                    freeExpression(alloc, lhs);
                    lhs_transferred = true;
                    return;
                }
                const condition = try expr_condition.expressionBooleanComparisonConditionAlloc(alloc, lhs, is_tail.op, is_tail.boolean_value);
                var condition_transferred = false;
                errdefer if (!condition_transferred) freeExpressionCondition(alloc, condition);
                try expression_predicates.append(alloc, condition);
                condition_transferred = true;
                lhs_transferred = true;
                return;
            },
        }
        break :blk is_tail.op;
    } else if (expr_operator.matchPostfixNullTest(tokens, pos)) |postfix_null_test| blk: {
        try expr_generated_validate.validateGeneratedPostfixNullPredicateExpression(options.generated_expression_ast, tokens, op_token_index, postfix_null_test);
        break :blk postfix_null_test;
    } else try expr_operator.parseComparisonOp(tokens, pos);

    if (op == .eq and expr_token.matchAnyOrSomeKeyword(tokens, pos)) {
        try expr_generated_validate.validateGeneratedQuantifiedPredicateIdentity(options.generated_expression_ast, tokens, op_token_index, pos.* - 1);
        try parser.expectToken(tokens, pos, .lparen);
        const values_json = try value_mod.parseJsonArrayValueAlloc(alloc, tokens, pos, params);
        defer alloc.free(values_json);
        try parser.expectToken(tokens, pos, .rparen);
        try expr_predicate.appendExpressionValuesJsonOrGroups(alloc, type_context, expression_or_predicates, lhs, values_json);
        freeExpression(alloc, lhs);
        lhs_transferred = true;
        return;
    }
    if (op == .ne and expr_token.matchAnyOrSomeKeyword(tokens, pos)) {
        try expr_generated_validate.validateGeneratedQuantifiedPredicateIdentity(options.generated_expression_ast, tokens, op_token_index, pos.* - 1);
        try parser.expectToken(tokens, pos, .lparen);
        const values_json = try value_mod.parseJsonArrayValueAlloc(alloc, tokens, pos, params);
        defer alloc.free(values_json);
        try parser.expectToken(tokens, pos, .rparen);
        try expr_predicate.appendExpressionValuesJsonComparisonGroups(alloc, type_context, expression_or_predicates, lhs, values_json, .ne);
        freeExpression(alloc, lhs);
        lhs_transferred = true;
        return;
    }
    if (op == .eq and parser.matchKeyword(tokens, pos, "all")) {
        try expr_generated_validate.validateGeneratedQuantifiedPredicateIdentity(options.generated_expression_ast, tokens, op_token_index, pos.* - 1);
        try parser.expectToken(tokens, pos, .lparen);
        const values_json = try value_mod.parseJsonArrayValueAlloc(alloc, tokens, pos, params);
        defer alloc.free(values_json);
        try parser.expectToken(tokens, pos, .rparen);
        try expr_predicate.appendExpressionValuesJsonConjunction(alloc, type_context, expression_predicates, lhs, values_json);
        freeExpression(alloc, lhs);
        lhs_transferred = true;
        return;
    }
    if (op == .ne and parser.matchKeyword(tokens, pos, "all")) {
        try expr_generated_validate.validateGeneratedQuantifiedPredicateIdentity(options.generated_expression_ast, tokens, op_token_index, pos.* - 1);
        try parser.expectToken(tokens, pos, .lparen);
        const values_json = try value_mod.parseJsonArrayValueAlloc(alloc, tokens, pos, params);
        defer alloc.free(values_json);
        try parser.expectToken(tokens, pos, .rparen);
        try expr_predicate.appendExpressionValuesJsonOrGroups(alloc, type_context, expression_not_predicates, lhs, values_json);
        freeExpression(alloc, lhs);
        lhs_transferred = true;
        return;
    }

    const rhs = switch (op) {
        .is_null, .is_not_null => &.{},
        else => blk: {
            const out = try alloc.alloc(db_mod.types.RelationalRowsExpression, 1);
            var out_transferred = false;
            errdefer if (!out_transferred) alloc.free(out);
            out[0] = try parseExpressionWhereConditionRowExpressionAlloc(alloc, tokens, pos, type_context, options);
            out_transferred = true;
            break :blk out;
        },
    };
    var rhs_transferred = false;
    errdefer if (!rhs_transferred and rhs.len > 0) {
        for (rhs) |expression| freeExpression(alloc, expression);
        alloc.free(rhs);
    };
    try expr_generated_validate.validateGeneratedRelationalPredicateExpression(options.generated_expression_ast, tokens, op_token_index, op);
    try expr_type.validateExpressionConditionTypes(type_context, defer_row_expression_field_validation, lhs, op, rhs);

    try expression_predicates.append(alloc, .{
        .lhs = lhs,
        .op = op,
        .rhs = rhs,
    });
    lhs_transferred = true;
    rhs_transferred = true;
}
