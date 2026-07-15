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

const binder = @import("../binder.zig");
const db_mod = @import("../../storage/db/mod.zig");
const ddl_plan = @import("../ddl_plan.zig");
const expr_aggregate = @import("aggregate.zig");
const expr_equal = @import("equal.zig");
const expr_generated = @import("generated.zig");
const expr_generated_validate = @import("generated_validate.zig");
const expr_operator = @import("operator.zig");
const expr_parse = @import("parse.zig");
const expr_projection = @import("projection.zig");
const expr_row_parse = @import("row_parse.zig");
const expr_token = @import("token.zig");
const expr_type = @import("type.zig");
const expr_window = @import("window.zig");
const generated_parser = @import("../generated_parser.zig");
const grammar = @import("../grammar.zig");
const parser = @import("../parser.zig");
const plan_mod = @import("../plan.zig");
const runtime_schema = @import("../../storage/schema.zig");
const token_mod = @import("../token.zig");
const value_mod = @import("../value.zig");

pub const Token = token_mod.Token;

const freeExpression = plan_mod.freeExpression;
const freeExpressionSlice = plan_mod.freeExpressionSlice;

pub const OrderExpressionParserOptions = struct {
    params: []const value_mod.SqlValue = &.{},
    field_source: db_mod.types.RelationalRowsExpressionFieldSource = .row,
    row_expression_hooks: expr_row_parse.RowExpressionParserHooks,
    arithmetic_hooks: expr_row_parse.ArithmeticExpressionParserHooks,
    variadic_hooks: expr_row_parse.VariadicRowExpressionParserHooks,
    parenthesized: expr_row_parse.ParenthesizedRowExpressionParserOptions,
    case_fold_hooks: expr_row_parse.CaseFoldRowExpressionParserOptions,
    fixed_unary: expr_row_parse.FixedUnaryRowExpressionParserOptions,
    generated_expression_ast: ?*const generated_parser.GeneratedSqlExpressionAst = null,
};

pub const OutputExpressionParserOptions = struct {
    function_bindings: expr_row_parse.SqlFunctionBindings = .{},
    context_hooks: expr_row_parse.SelectParserContextHooks,
    order_expression_hooks: OrderExpressionParserOptions,
    generated_read_ast: ?*const generated_parser.GeneratedSqlReadAst = null,
};

pub const ByParserOptions = struct {
    schema: runtime_schema.TableSchema,
    function_bindings: expr_row_parse.SqlFunctionBindings = .{},
    field_expression_qualifiers: []const []const u8 = &.{},
    returning_expression_qualifiers: []const []const u8 = &.{},
    defer_row_expression_field_validation: bool = false,
    type_context: expr_type.RowExpressionTypeContext,
    order_expression_hooks: OrderExpressionParserOptions,
    generated_read_ast: ?*const generated_parser.GeneratedSqlReadAst = null,
    generated_order_items: ?*const generated_parser.GeneratedSqlListAst = null,
};

pub const ExpressionStart = enum {
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

pub const peekParenthesizedNullTestProjection = expr_projection.peekParenthesizedNullTestProjection;

pub fn peekGeneralRowExpression(tokens: []const Token, pos: usize) bool {
    return expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsJsonExtractPathFunction) or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsJsonTypeofFunction) or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsJsonArrayLengthFunction) or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsJsonBuildObjectFunction) or
        value_mod.peekToJsonbFunctionCall(tokens, pos) or
        expr_token.functionCallStartsAtTokenIf(tokens, pos, expr_token.sqlTokenIsArrayLengthFunction) or
        expr_token.functionCallStartsAtTokenIf(tokens, pos, expr_token.sqlTokenIsArrayPositionFunction) or
        expr_token.peekArrayElementTransformFunctionCall(tokens, pos) or
        expr_token.peekArrayToStringFunctionCall(tokens, pos) or
        expr_token.peekCaseExpressionSyntax(tokens, pos) or
        expr_token.peekCastExpressionSyntax(tokens, pos) or
        expr_token.peekCoalesceFunctionCall(tokens, pos) or
        expr_token.peekRegexpReplaceFunctionCall(tokens, pos) or
        expr_token.peekReplaceFunctionCall(tokens, pos) or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsRegexpSubstrFunction) or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsRegexpMatchFunction) or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsRegexpCountFunction) or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsRegexpInstrFunction) or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsTranslateFunction) or
        expr_token.peekNullifFunctionCall(tokens, pos) or
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
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsMd5Function) or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsStartsWithFunction) or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsEndsWithFunction) or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsDateTruncFunction) or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsDateBinFunction) or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsDatePartFunction) or
        expr_token.peekPositionFunctionSyntax(tokens, pos) or
        expr_token.peekFixedUnaryFunctionCall(tokens, pos, .abs) or
        expr_token.peekFixedUnaryFunctionCall(tokens, pos, .round) or
        expr_token.peekFixedUnaryFunctionCall(tokens, pos, .trunc) or
        expr_token.peekFixedUnaryFunctionCall(tokens, pos, .floor) or
        expr_token.peekFixedUnaryFunctionCall(tokens, pos, .ceil) or
        expr_token.peekFixedUnaryFunctionCall(tokens, pos, .sqrt) or
        expr_token.peekFixedUnaryFunctionCall(tokens, pos, .sign) or
        expr_token.peekFixedBinaryFunctionCall(tokens, pos, .mod) or
        expr_token.peekFixedBinaryFunctionCall(tokens, pos, .power) or
        expr_token.peekGreatestLeastFunctionCall(tokens, pos);
}

fn validateGeneratedOrderExpressionIdentityStrict(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    start: usize,
    end: usize,
    expression: db_mod.types.RelationalRowsExpression,
    options: OrderExpressionParserOptions,
) !void {
    try expr_generated_validate.validateGeneratedRowExpressionIdentityStrictWithContext(
        .{ .alloc = alloc, .params = options.params },
        tokens,
        start,
        end,
        expression,
        options.generated_expression_ast,
    );
}

pub fn expressionStartAt(tokens: []const Token, pos: usize) ExpressionStart {
    if (peekParenthesizedNullTestProjection(tokens, pos)) return .parenthesized_null_test;
    if (expr_token.peekParenthesizedExpressionSyntax(tokens, pos)) return .parenthesized;
    if (expr_parse.rowExpressionHasTopLevelPipeConcat(tokens, pos)) return .pipe_concat;
    if (pos < tokens.len and tokens[pos].kind == .identifier and pos + 1 < tokens.len and expr_operator.tokenKindIsJsonExtractOperator(tokens[pos + 1].kind)) return .json_extract_field;
    if (expr_token.peekCaseFoldFunctionCall(tokens, pos)) return .generated_or_case_fold;
    if (expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsMd5Function)) return .generated_or_md5;
    if (expr_token.peekConcatFunctionCall(tokens, pos)) return .generated_or_concat;
    if (peekGeneralRowExpression(tokens, pos)) return .general;
    if (expr_token.peekUnaryNegativeExpressionSyntax(tokens, pos)) return .unary_negative;
    return .field;
}

fn generatedOrderExpressionStartAllowsExpressionKind(
    start: ExpressionStart,
    kind: generated_parser.GeneratedSqlExpressionKind,
) bool {
    return switch (start) {
        .parenthesized_null_test,
        .parenthesized,
        => kind == .grouped,
        .pipe_concat => kind == .string_concat,
        .json_extract_field => kind == .json_access or kind == .json_text_access or kind == .json_path_access or kind == .json_path_text_access,
        .generated_or_case_fold,
        .generated_or_md5,
        .generated_or_concat,
        => kind == .function_call,
        .unary_negative => kind == .unary_negative,
        .general,
        .field,
        => true,
    };
}

fn validateGeneratedOrderExpressionStartForExpression(
    tokens: []const Token,
    start: ExpressionStart,
    generated_expression: ?*const generated_parser.GeneratedSqlExpressionAst,
) !void {
    const expression = generated_expression orelse return;
    try expr_generated_validate.validateGeneratedExpressionPayloads(tokens, expression.*);
    if (generatedOrderExpressionContainsSubquery(expression.*)) return error.UnsupportedSqlShape;
    if (!generatedOrderExpressionStartAllowsExpressionKind(start, expression.kind)) return error.UnsupportedSqlShape;
    switch (start) {
        .generated_or_case_fold => {
            const token = try expr_generated_validate.generatedExpressionFunctionNameToken(tokens, expression.*);
            if (!token.matchesKeywordTag(.lower) and
                !token.matchesKeywordTag(.upper) and
                !expr_token.sqlTokenIsInitcapFunction(token) and
                !token.matchesKeywordTag(.trim) and
                !expr_token.sqlTokenIsTrimVariantFunction(token))
            {
                return error.UnsupportedSqlShape;
            }
        },
        .generated_or_md5 => {
            const token = try expr_generated_validate.generatedExpressionFunctionNameToken(tokens, expression.*);
            if (!expr_token.sqlTokenIsMd5Function(token)) return error.UnsupportedSqlShape;
        },
        .generated_or_concat => {
            const token = try expr_generated_validate.generatedExpressionFunctionNameToken(tokens, expression.*);
            if (!token.matchesKeywordTag(.concat) and !token.matchesKeywordTag(.concat_ws)) return error.UnsupportedSqlShape;
        },
        else => {},
    }
}

fn generatedOrderExpressionContainsSubquery(expression: generated_parser.GeneratedSqlExpressionAst) bool {
    if (expression.kind == .subquery) return true;
    if (expression.inner_expression) |inner| {
        if (generatedOrderExpressionContainsSubquery(inner.*)) return true;
    }
    if (expression.left_expression) |left| {
        if (generatedOrderExpressionContainsSubquery(left.*)) return true;
    }
    if (expression.right_expression) |right| {
        if (generatedOrderExpressionContainsSubquery(right.*)) return true;
    }
    if (expression.between_lower_expression) |lower| {
        if (generatedOrderExpressionContainsSubquery(lower.*)) return true;
    }
    if (expression.between_upper_expression) |upper| {
        if (generatedOrderExpressionContainsSubquery(upper.*)) return true;
    }
    if (expression.escape_expression) |escape| {
        if (generatedOrderExpressionContainsSubquery(escape.*)) return true;
    }
    if (expression.cast_expression) |cast_expression| {
        if (generatedOrderExpressionContainsSubquery(cast_expression.*)) return true;
    }
    if (expression.filter_expression) |filter_expression| {
        if (generatedOrderExpressionContainsSubquery(filter_expression.*)) return true;
    }
    if (expression.extract_source_expression) |source_expression| {
        if (generatedOrderExpressionContainsSubquery(source_expression.*)) return true;
    }
    for (expression.argument_items.expressions) |argument_expression| {
        if (generatedOrderExpressionContainsSubquery(argument_expression)) return true;
    }
    for (expression.array_items.expressions) |array_expression| {
        if (generatedOrderExpressionContainsSubquery(array_expression)) return true;
    }
    for (expression.case_condition_items.expressions) |condition_expression| {
        if (generatedOrderExpressionContainsSubquery(condition_expression)) return true;
    }
    for (expression.case_result_items.expressions) |result_expression| {
        if (generatedOrderExpressionContainsSubquery(result_expression)) return true;
    }
    if (expression.case_else_expression) |else_expression| {
        if (generatedOrderExpressionContainsSubquery(else_expression.*)) return true;
    }
    return false;
}

pub fn validateGeneratedSimpleOrderExpression(
    tokens: []const Token,
    generated_expression: ?*const generated_parser.GeneratedSqlExpressionAst,
) !void {
    const expression = generated_expression orelse return;
    try expr_generated_validate.validateGeneratedExpressionPayloads(tokens, expression.*);
    try validateGeneratedSimpleOrderChildPayloads(expression.*);
    switch (expression.kind) {
        .token_range, .function_call => {},
        else => return error.UnsupportedSqlShape,
    }
}

fn validateGeneratedSimpleOrderChildPayloads(expression: generated_parser.GeneratedSqlExpressionAst) !void {
    try validateGeneratedOptionalChildPayload(expression.inner_tokens, expression.inner_expression_kind, expression.inner_expression);
    try validateGeneratedOptionalChildPayload(expression.left_tokens, expression.left_expression_kind, expression.left_expression);
    try validateGeneratedOptionalChildPayload(expression.right_tokens, expression.right_expression_kind, expression.right_expression);
    try validateGeneratedOptionalChildPayload(expression.cast_expression_tokens, expression.cast_expression_kind, expression.cast_expression);
    try validateGeneratedOptionalChildPayload(expression.filter_predicate_tokens, expression.filter_expression_kind, expression.filter_expression);
    try validateGeneratedOptionalChildPayload(expression.over_frame_start_expression_tokens, expression.over_frame_start_expression_kind, expression.over_frame_start_expression);
    try validateGeneratedOptionalChildPayload(expression.over_frame_end_expression_tokens, expression.over_frame_end_expression_kind, expression.over_frame_end_expression);
    try validateGeneratedOptionalChildPayload(expression.between_lower_tokens, expression.between_lower_expression_kind, expression.between_lower_expression);
    try validateGeneratedOptionalChildPayload(expression.between_upper_tokens, expression.between_upper_expression_kind, expression.between_upper_expression);
    try validateGeneratedOptionalChildPayload(expression.escape_tokens, expression.escape_expression_kind, expression.escape_expression);
    try validateGeneratedOptionalChildPayload(expression.case_first_condition_tokens, expression.case_first_condition_kind, expression.case_first_condition);
    try validateGeneratedOptionalChildPayload(expression.case_first_result_tokens, expression.case_first_result_kind, expression.case_first_result);
    try validateGeneratedOptionalChildPayload(expression.case_else_expression_tokens, expression.case_else_expression_kind, expression.case_else_expression);
    try validateGeneratedOptionalChildPayload(expression.boolean_first_condition_tokens, expression.boolean_first_condition_kind, expression.boolean_first_condition);
    try validateGeneratedOptionalChildPayload(expression.boolean_last_condition_tokens, expression.boolean_last_condition_kind, expression.boolean_last_condition);
    try validateGeneratedOptionalChildPayload(expression.extract_source_tokens, expression.extract_source_expression_kind, expression.extract_source_expression);
}

fn validateGeneratedOptionalChildPayload(
    tokens: ?generated_parser.GeneratedSqlTokenRange,
    kind: ?generated_parser.GeneratedSqlExpressionKind,
    expression: ?*generated_parser.GeneratedSqlExpressionAst,
) !void {
    if (tokens == null and kind == null and expression == null) return;
    if (tokens == null or kind == null or expression == null) return error.UnsupportedSqlShape;
}

pub fn validateDistinctOnOrder(
    distinct_on: []const db_mod.types.RelationalRowsExpression,
    order_by: []const db_mod.types.RelationalRowsQueryOrder,
) !void {
    if (order_by.len < distinct_on.len) return error.UnsupportedSqlShape;
    for (distinct_on, 0..) |expression, i| {
        const order = order_by[i];
        if (order.expression) |order_expression| {
            if (!expr_equal.relationalRowsExpressionEqual(order_expression, expression)) return error.UnsupportedSqlShape;
        } else {
            if (expression.kind != .field or expression.field_source != .row or !std.mem.eql(u8, order.field, expression.field)) return error.UnsupportedSqlShape;
        }
    }
}

pub fn parseExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    schema: runtime_schema.TableSchema,
    function_bindings: expr_row_parse.SqlFunctionBindings,
    field_expression_qualifiers: []const []const u8,
    returning_expression_qualifiers: []const []const u8,
    defer_row_expression_field_validation: bool,
    type_context: expr_type.RowExpressionTypeContext,
    options: OrderExpressionParserOptions,
) !db_mod.types.RelationalRowsQueryOrder {
    if (expr_row_parse.peekExtensionFunctionCall(tokens, pos.*, function_bindings.extension_functions) or
        expr_row_parse.peekRoutineExpressionCall(tokens, pos.*, function_bindings.routine_expressions))
    {
        if (options.generated_expression_ast) |generated_expression| {
            if (generated_expression.kind != .function_call) return error.UnsupportedSqlShape;
        }
        const expression_start = pos.*;
        const expression = try expr_row_parse.parseRowExpressionAlloc(alloc, tokens, pos, type_context, options.row_expression_hooks, options.arithmetic_hooks, options.variadic_hooks);
        var expression_transferred = false;
        errdefer if (!expression_transferred) freeExpression(alloc, expression);
        try validateGeneratedOrderExpressionIdentityStrict(
            alloc,
            tokens,
            expression_start,
            pos.*,
            expression,
            options,
        );
        try type_context.validateOrderableRowExpression(expression);
        expression_transferred = true;
        return .{ .expression = expression };
    }

    const start = expressionStartAt(tokens, pos.*);
    try validateGeneratedOrderExpressionStartForExpression(tokens, start, options.generated_expression_ast);
    switch (start) {
        .parenthesized_null_test => {
            _ = parser.matchToken(tokens, pos, .lparen) orelse unreachable;
            const parsed_field = try expr_generated.parseRowExpressionFieldOwnedAlloc(alloc, tokens, pos, schema, field_expression_qualifiers, returning_expression_qualifiers, defer_row_expression_field_validation);
            defer alloc.free(parsed_field);
            const field = try binder.normalizeRowExpressionFieldAlloc(alloc, schema, parsed_field, field_expression_qualifiers, returning_expression_qualifiers, defer_row_expression_field_validation);
            var field_transferred = false;
            errdefer if (!field_transferred) alloc.free(field);
            if (binder.relationalColumnForField(schema, field, null) == null) return error.InvalidSqlCatalog;
            try parser.expectKeyword(tokens, pos, "is");
            const null_test: db_mod.types.RelationalRowsQueryOrderNullTest = if (parser.matchKeyword(tokens, pos, "not")) blk: {
                try parser.expectKeyword(tokens, pos, "null");
                break :blk .is_not_null;
            } else blk: {
                try parser.expectKeyword(tokens, pos, "null");
                break :blk .is_null;
            };
            try parser.expectToken(tokens, pos, .rparen);
            field_transferred = true;
            var order = try orderForOwnedFieldAlloc(alloc, schema, field);
            order.null_test = null_test;
            return order;
        },
        .parenthesized => {
            const expression_start = pos.*;
            const expression = try expr_row_parse.parseParenthesizedRowExpressionAlloc(alloc, tokens, pos, options.parenthesized);
            var expression_transferred = false;
            errdefer if (!expression_transferred) freeExpression(alloc, expression);
            try validateGeneratedOrderExpressionIdentityStrict(
                alloc,
                tokens,
                expression_start,
                pos.*,
                expression,
                options,
            );
            try type_context.validateOrderableRowExpression(expression);
            expression_transferred = true;
            return .{ .expression = expression };
        },
        .pipe_concat => {
            const expression_start = pos.*;
            const expression = try expr_row_parse.parseRowExpressionAlloc(alloc, tokens, pos, type_context, options.row_expression_hooks, options.arithmetic_hooks, options.variadic_hooks);
            var expression_transferred = false;
            errdefer if (!expression_transferred) freeExpression(alloc, expression);
            try validateGeneratedOrderExpressionIdentityStrict(
                alloc,
                tokens,
                expression_start,
                pos.*,
                expression,
                options,
            );
            try type_context.validateTextRowExpression(expression);
            expression_transferred = true;
            return .{ .expression = expression };
        },
        .json_extract_field, .generated_or_concat, .general => {
            if (try expr_generated.parseGeneratedFieldExpressionOrNullOwnedAlloc(
                alloc,
                tokens,
                pos,
                schema,
                field_expression_qualifiers,
                returning_expression_qualifiers,
                defer_row_expression_field_validation,
            )) |field| return try orderForOwnedFieldAlloc(alloc, schema, field);
            const expression_start = pos.*;
            const expression = try expr_row_parse.parseRowExpressionAlloc(alloc, tokens, pos, type_context, options.row_expression_hooks, options.arithmetic_hooks, options.variadic_hooks);
            var expression_transferred = false;
            errdefer if (!expression_transferred) freeExpression(alloc, expression);
            try validateGeneratedOrderExpressionIdentityStrict(
                alloc,
                tokens,
                expression_start,
                pos.*,
                expression,
                options,
            );
            try type_context.validateOrderableRowExpression(expression);
            expression_transferred = true;
            return .{ .expression = expression };
        },
        .generated_or_case_fold => {
            if (try expr_generated.parseGeneratedFieldExpressionOrNullOwnedAlloc(
                alloc,
                tokens,
                pos,
                schema,
                field_expression_qualifiers,
                returning_expression_qualifiers,
                defer_row_expression_field_validation,
            )) |field| return try orderForOwnedFieldAlloc(alloc, schema, field);
            const expression_start = pos.*;
            const expression = try expr_row_parse.parseCaseFoldRowExpressionAlloc(
                alloc,
                tokens,
                pos,
                schema,
                field_expression_qualifiers,
                returning_expression_qualifiers,
                defer_row_expression_field_validation,
                options.field_source,
                type_context,
                options.case_fold_hooks,
            );
            var expression_transferred = false;
            errdefer if (!expression_transferred) freeExpression(alloc, expression);
            try validateGeneratedOrderExpressionIdentityStrict(
                alloc,
                tokens,
                expression_start,
                pos.*,
                expression,
                options,
            );
            if (expression.kind == .field and expression.field.len != 0) {
                expression_transferred = true;
                return try orderForOwnedFieldAlloc(alloc, schema, expression.field);
            }
            expression_transferred = true;
            return .{ .expression = expression };
        },
        .generated_or_md5 => {
            if (try expr_generated.parseGeneratedFieldExpressionOrNullOwnedAlloc(
                alloc,
                tokens,
                pos,
                schema,
                field_expression_qualifiers,
                returning_expression_qualifiers,
                defer_row_expression_field_validation,
            )) |field| return .{ .field = field };
            const expression_start = pos.*;
            const expression = try expr_row_parse.parseFixedUnaryRowExpressionAlloc(alloc, tokens, pos, .md5, type_context, .text, options.fixed_unary);
            var expression_transferred = false;
            errdefer if (!expression_transferred) freeExpression(alloc, expression);
            try validateGeneratedOrderExpressionIdentityStrict(
                alloc,
                tokens,
                expression_start,
                pos.*,
                expression,
                options,
            );
            expression_transferred = true;
            return .{ .expression = expression };
        },
        .unary_negative => {
            const expression_start = pos.*;
            const expression = try expr_row_parse.parseRowExpressionAlloc(alloc, tokens, pos, type_context, options.row_expression_hooks, options.arithmetic_hooks, options.variadic_hooks);
            var expression_transferred = false;
            errdefer if (!expression_transferred) freeExpression(alloc, expression);
            try validateGeneratedOrderExpressionIdentityStrict(
                alloc,
                tokens,
                expression_start,
                pos.*,
                expression,
                options,
            );
            try type_context.validateNumericRowExpression(expression);
            expression_transferred = true;
            return .{ .expression = expression };
        },
        .field => {},
    }

    const parsed_field = try expr_generated.parseRowExpressionFieldOwnedAlloc(alloc, tokens, pos, schema, field_expression_qualifiers, returning_expression_qualifiers, defer_row_expression_field_validation);
    defer alloc.free(parsed_field);
    const field = try binder.normalizeRowExpressionFieldAlloc(alloc, schema, parsed_field, field_expression_qualifiers, returning_expression_qualifiers, defer_row_expression_field_validation);
    var field_transferred = false;
    errdefer if (!field_transferred) alloc.free(field);
    const column = binder.relationalColumnForField(schema, field, null) orelse return error.InvalidSqlCatalog;
    if (expr_operator.peekArithmeticOperator(tokens, pos.*)) |_| {
        if (column.field_type != .numeric and column.field_type != .datetime) return error.InvalidSqlCatalog;
        field_transferred = true;
        const expression_start = pos.* - 1;
        const expression = try expr_row_parse.parseArithmeticExpressionRestAlloc(alloc, tokens, pos, .{ .kind = .field, .field = field }, 0, type_context, options.arithmetic_hooks);
        var expression_transferred = false;
        errdefer if (!expression_transferred) freeExpression(alloc, expression);
        try validateGeneratedOrderExpressionIdentityStrict(
            alloc,
            tokens,
            expression_start,
            pos.*,
            expression,
            options,
        );
        try type_context.validateNumericRowExpression(expression);
        expression_transferred = true;
        return .{ .expression = expression };
    }
    field_transferred = true;
    return try orderForOwnedFieldAlloc(alloc, schema, field);
}

fn orderForOwnedFieldAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    field: []const u8,
) !db_mod.types.RelationalRowsQueryOrder {
    errdefer alloc.free(field);
    const column = binder.relationalColumnForField(schema, field, null) orelse return error.InvalidSqlCatalog;
    const collation = if (column.collation) |value| try alloc.dupe(u8, value) else null;
    errdefer if (collation) |value| alloc.free(value);
    return .{
        .field = field,
        .collation = collation,
    };
}

pub fn parseExpressionWithSelectSchemaAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    schema: runtime_schema.TableSchema,
    function_bindings: expr_row_parse.SqlFunctionBindings,
    context_hooks: expr_row_parse.SelectParserContextHooks,
    hooks: OrderExpressionParserOptions,
) !db_mod.types.RelationalRowsQueryOrder {
    const previous_context = context_hooks.get_context(context_hooks.ptr);
    var context = previous_context;
    context.schema = schema;
    context_hooks.set_context(context_hooks.ptr, context);
    defer context_hooks.set_context(context_hooks.ptr, previous_context);

    return try parseExpressionAlloc(
        alloc,
        tokens,
        pos,
        context.schema,
        function_bindings,
        context.field_expression_qualifiers,
        context.returning_expression_qualifiers,
        context.defer_row_expression_field_validation,
        context_hooks.row_expression_type_context(context_hooks.ptr),
        hooks,
    );
}

pub fn parseByAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    order_by: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsQueryOrder),
    options: ByParserOptions,
) !void {
    return try parseByWithExpressionHooksAlloc(
        alloc,
        tokens,
        pos,
        order_by,
        options.schema,
        options.function_bindings,
        options.field_expression_qualifiers,
        options.returning_expression_qualifiers,
        options.defer_row_expression_field_validation,
        options.type_context,
        options.order_expression_hooks,
        options.generated_read_ast,
        options.generated_order_items,
    );
}

pub fn parseByWithExpressionHooksAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    order_by: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsQueryOrder),
    schema: runtime_schema.TableSchema,
    function_bindings: expr_row_parse.SqlFunctionBindings,
    field_expression_qualifiers: []const []const u8,
    returning_expression_qualifiers: []const []const u8,
    defer_row_expression_field_validation: bool,
    type_context: expr_type.RowExpressionTypeContext,
    hooks: OrderExpressionParserOptions,
    generated_read_ast: ?*const generated_parser.GeneratedSqlReadAst,
    generated_order_items: ?*const generated_parser.GeneratedSqlListAst,
) !void {
    while (true) {
        const item_start = pos.*;
        const generated_item = if (generated_order_items) |items|
            try expr_generated_validate.generatedExpressionListItemAtStart(tokens, item_start, items)
        else
            try expr_generated_validate.generatedOrderItemAtStart(tokens, item_start, generated_read_ast);
        if (generated_item == null and (generated_read_ast != null or generated_order_items != null)) return error.UnsupportedSqlShape;
        const generated_expression = if (generated_item) |item| item.expression else null;
        var item_hooks = hooks;
        item_hooks.generated_expression_ast = generated_expression;
        var order = try parseExpressionAlloc(
            alloc,
            tokens,
            pos,
            schema,
            function_bindings,
            field_expression_qualifiers,
            returning_expression_qualifiers,
            defer_row_expression_field_validation,
            type_context,
            item_hooks,
        );
        var order_transferred = false;
        errdefer if (!order_transferred) {
            if (order.field.len > 0) alloc.free(order.field);
            if (order.expression) |expression| freeExpression(alloc, expression);
            if (order.collation) |collation| alloc.free(collation);
        };
        const explicit_nulls_first = try parseModifiers(tokens, pos, &order);
        try expr_generated_validate.validateGeneratedExpressionItemEnd(generated_item, pos.*);
        try appendWithNullPlacement(alloc, order_by, order, explicit_nulls_first);
        order_transferred = true;
        if (parser.matchToken(tokens, pos, .comma) == null) break;
    }
}

pub fn parseSelectOutputByAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    order_by: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsQueryOrder),
    select: plan_mod.SelectList,
    options: ByParserOptions,
) !void {
    while (true) {
        const item_start = pos.*;
        const generated_item = try expr_generated_validate.generatedOrderItemAtStart(tokens, item_start, options.generated_read_ast);
        if (generated_item == null and options.generated_read_ast != null) return error.UnsupportedSqlShape;
        const generated_expression = if (generated_item) |item| item.expression else null;
        var order_expression_hooks = options.order_expression_hooks;
        order_expression_hooks.generated_expression_ast = generated_expression;
        var order = if (parser.matchToken(tokens, pos, .number)) |token| blk: {
            try validateGeneratedSimpleOrderExpression(tokens, generated_expression);
            const ordinal = std.fmt.parseInt(u32, token.text, 10) catch return error.UnsupportedSqlShape;
            break :blk try expr_projection.selectOutputOrderByOrdinalAlloc(alloc, options.schema, select, ordinal);
        } else if (try expr_projection.parseSelectOutputOrderByNameMaybeAlloc(alloc, tokens, pos, options.schema, select)) |named_order| blk: {
            const order_candidate = named_order;
            var order_candidate_transferred = false;
            errdefer if (!order_candidate_transferred) {
                if (order_candidate.field.len > 0) alloc.free(order_candidate.field);
                if (order_candidate.expression) |expression| freeExpression(alloc, expression);
                if (order_candidate.collation) |collation| alloc.free(collation);
            };
            try validateGeneratedSimpleOrderExpression(tokens, generated_expression);
            order_candidate_transferred = true;
            break :blk order_candidate;
        } else if (try parseGeneratedScalarSubqueryExpressionOutputOrderAlloc(alloc, tokens, pos, select, generated_expression)) |named_order| blk: {
            break :blk named_order;
        } else try parseExpressionAlloc(
            alloc,
            tokens,
            pos,
            options.schema,
            options.function_bindings,
            options.field_expression_qualifiers,
            options.returning_expression_qualifiers,
            options.defer_row_expression_field_validation,
            options.type_context,
            order_expression_hooks,
        );
        var order_transferred = false;
        errdefer if (!order_transferred) {
            if (order.field.len > 0) alloc.free(order.field);
            if (order.expression) |expression| freeExpression(alloc, expression);
            if (order.collation) |collation| alloc.free(collation);
        };
        const explicit_nulls_first = try parseModifiers(tokens, pos, &order);
        try expr_generated_validate.validateGeneratedExpressionItemEnd(generated_item, pos.*);
        try appendWithNullPlacement(alloc, order_by, order, explicit_nulls_first);
        order_transferred = true;
        if (parser.matchToken(tokens, pos, .comma) == null) break;
    }
}

fn parseGeneratedScalarSubqueryExpressionOutputOrderAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    select: plan_mod.SelectList,
    generated_expression: ?*const generated_parser.GeneratedSqlExpressionAst,
) !?db_mod.types.RelationalRowsQueryOrder {
    if (generated_expression == null or select.scalar_subqueries.len == 0) return null;
    if (!parser.peekKind(tokens, pos.*, .identifier)) return null;
    const name = tokens[pos.*].text;
    var found = false;
    for (select.expressions) |projection| {
        if (!std.ascii.eqlIgnoreCase(projection.output, name)) continue;
        if (found) return error.UnsupportedSqlShape;
        found = true;
    }
    if (!found) return null;
    try validateGeneratedSimpleOrderExpression(tokens, generated_expression);
    pos.* += 1;
    return .{ .field = try alloc.dupe(u8, name) };
}

pub fn parseTargetByAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    schema: runtime_schema.TableSchema,
    order_by: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsQueryOrder),
    target_alias: []const u8,
) !void {
    while (true) {
        const field = if (parser.peekKind(tokens, pos.*, .identifier) and expr_parse.identifierContainsQualifier(tokens[pos.*].text)) blk: {
            const source = try plan_mod.parseQualifiedFieldAlloc(alloc, tokens, pos);
            defer plan_mod.freeQualifiedField(alloc, source);
            if (!std.mem.eql(u8, source.qualifier, target_alias)) return error.UnsupportedSqlShape;
            break :blk try alloc.dupe(u8, source.field);
        } else try grammar.parseIdentifierOwnedAlloc(alloc, tokens, pos);
        var field_transferred = false;
        errdefer if (!field_transferred) alloc.free(field);
        if (binder.relationalColumnForField(schema, field, null) == null) return error.InvalidSqlCatalog;
        var order: db_mod.types.RelationalRowsQueryOrder = .{ .field = field };
        const explicit_nulls_first = try parseModifiers(tokens, pos, &order);
        try appendWithNullPlacement(alloc, order_by, order, explicit_nulls_first);
        field_transferred = true;
        if (parser.matchToken(tokens, pos, .comma) == null) break;
    }
}

pub fn parseAggregateOutputExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    schema: runtime_schema.TableSchema,
    type_context: expr_type.RowExpressionTypeContext,
    group_fields: []const []const u8,
    group_expressions: []const db_mod.types.RelationalRowsExpressionProjection,
    aggregations: []const db_mod.types.RelationalRowsAggregateSpec,
    options: OutputExpressionParserOptions,
) !db_mod.types.RelationalRowsQueryOrder {
    const output_columns = try expr_aggregate.outputColumnsAlloc(alloc, schema, type_context, group_fields, group_expressions, aggregations);
    defer ddl_plan.freeDdlRelationalColumns(alloc, output_columns);
    const aggregate_schema: runtime_schema.TableSchema = .{
        .storage_mode = .relational,
        .relational_columns = output_columns,
    };
    return try parseExpressionWithSelectSchemaAlloc(
        alloc,
        tokens,
        pos,
        aggregate_schema,
        options.function_bindings,
        options.context_hooks,
        options.order_expression_hooks,
    );
}

fn parseAggregateGroupedExpressionOrderAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    schema: runtime_schema.TableSchema,
    type_context: expr_type.RowExpressionTypeContext,
    group_expressions: []const db_mod.types.RelationalRowsExpressionProjection,
    options: OutputExpressionParserOptions,
) !?db_mod.types.RelationalRowsQueryOrder {
    if (group_expressions.len == 0) return null;
    const start = pos.*;

    const previous_context = options.context_hooks.get_context(options.context_hooks.ptr);
    var context = previous_context;
    context.schema = schema;
    options.context_hooks.set_context(options.context_hooks.ptr, context);
    defer options.context_hooks.set_context(options.context_hooks.ptr, previous_context);

    const order = parseExpressionAlloc(
        alloc,
        tokens,
        pos,
        context.schema,
        options.function_bindings,
        context.field_expression_qualifiers,
        context.returning_expression_qualifiers,
        context.defer_row_expression_field_validation,
        type_context,
        options.order_expression_hooks,
    ) catch |err| switch (err) {
        error.InvalidSqlCatalog, error.UnsupportedSqlShape => {
            pos.* = start;
            return null;
        },
        else => return err,
    };
    var order_transferred = false;
    errdefer if (!order_transferred) plan_mod.freeOrderBy(alloc, &.{order});

    const expression = order.expression orelse {
        plan_mod.freeOrderBy(alloc, &.{order});
        pos.* = start;
        return null;
    };
    for (group_expressions) |projection| {
        if (expr_equal.relationalRowsExpressionEqual(expression, projection.expression)) {
            order_transferred = true;
            return order;
        }
    }

    plan_mod.freeOrderBy(alloc, &.{order});
    pos.* = start;
    return null;
}

pub fn parseWindowOutputExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    schema: runtime_schema.TableSchema,
    type_context: expr_type.RowExpressionTypeContext,
    select: plan_mod.WindowSelectList,
    options: OutputExpressionParserOptions,
) !db_mod.types.RelationalRowsQueryOrder {
    const output_columns = try expr_window.outputColumnsAlloc(alloc, schema, type_context, select);
    defer ddl_plan.freeDdlRelationalColumns(alloc, output_columns);
    const window_schema: runtime_schema.TableSchema = .{
        .storage_mode = .relational,
        .relational_columns = output_columns,
    };
    return try parseExpressionWithSelectSchemaAlloc(
        alloc,
        tokens,
        pos,
        window_schema,
        options.function_bindings,
        options.context_hooks,
        options.order_expression_hooks,
    );
}

pub fn parseJoinOutputExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    schema: runtime_schema.TableSchema,
    select: []const db_mod.types.RelationalRowsJoinProjection,
    options: OutputExpressionParserOptions,
) !db_mod.types.RelationalRowsQueryOrder {
    const output_columns = try expr_projection.joinOutputColumnsAlloc(alloc, schema, select);
    defer ddl_plan.freeDdlRelationalColumns(alloc, output_columns);
    const join_schema: runtime_schema.TableSchema = .{
        .storage_mode = .relational,
        .relational_columns = output_columns,
    };
    return try parseExpressionWithSelectSchemaAlloc(
        alloc,
        tokens,
        pos,
        join_schema,
        options.function_bindings,
        options.context_hooks,
        options.order_expression_hooks,
    );
}

pub fn parseAggregateByAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    schema: runtime_schema.TableSchema,
    type_context: expr_type.RowExpressionTypeContext,
    order_by: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsQueryOrder),
    group_fields: []const []const u8,
    group_expressions: []const db_mod.types.RelationalRowsExpressionProjection,
    aggregations: []const db_mod.types.RelationalRowsAggregateSpec,
    field_options: expr_aggregate.OutputFieldParserOptions,
    order_expression_options: OutputExpressionParserOptions,
) !void {
    while (true) {
        const item_start = pos.*;
        const generated_item = try expr_generated_validate.generatedOrderItemAtStart(tokens, item_start, order_expression_options.generated_read_ast);
        if (generated_item == null and order_expression_options.generated_read_ast != null) return error.UnsupportedSqlShape;
        const generated_expression = if (generated_item) |item| item.expression else null;
        var item_order_expression_options = order_expression_options;
        item_order_expression_options.order_expression_hooks.generated_expression_ast = generated_expression;
        var order = if (parser.matchToken(tokens, pos, .number)) |token| blk: {
            try validateGeneratedSimpleOrderExpression(tokens, generated_expression);
            const ordinal = std.fmt.parseInt(u32, token.text, 10) catch return error.UnsupportedSqlShape;
            break :blk db_mod.types.RelationalRowsQueryOrder{ .field = try expr_aggregate.outputFieldByOrdinalAlloc(alloc, group_fields, group_expressions, aggregations, ordinal) };
        } else if (expr_aggregate.peekOutputOrderExpression(tokens, pos.*)) blk: {
            if (try parseAggregateGroupedExpressionOrderAlloc(alloc, tokens, pos, schema, type_context, group_expressions, item_order_expression_options)) |grouped_order| {
                break :blk grouped_order;
            }
            break :blk try parseAggregateOutputExpressionAlloc(alloc, tokens, pos, schema, type_context, group_fields, group_expressions, aggregations, item_order_expression_options);
        } else blk: {
            try validateGeneratedSimpleOrderExpression(tokens, generated_expression);
            const field = try expr_aggregate.parseOutputFieldAlloc(alloc, tokens, pos, group_fields, group_expressions, aggregations, field_options);
            if (!expr_aggregate.outputFieldIsUnique(group_fields, group_expressions, aggregations, field)) {
                alloc.free(field);
                return error.UnsupportedSqlShape;
            }
            break :blk db_mod.types.RelationalRowsQueryOrder{ .field = field };
        };
        var order_transferred = false;
        errdefer if (!order_transferred) {
            if (order.field.len > 0) alloc.free(order.field);
            if (order.expression) |expression| freeExpression(alloc, expression);
            if (order.collation) |collation| alloc.free(collation);
        };
        const explicit_nulls_first = try parseModifiers(tokens, pos, &order);
        try expr_generated_validate.validateGeneratedExpressionItemEnd(generated_item, pos.*);
        try appendWithNullPlacement(alloc, order_by, order, explicit_nulls_first);
        order_transferred = true;
        if (parser.matchToken(tokens, pos, .comma) == null) break;
    }
}

pub fn parseWindowOutputByAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    schema: runtime_schema.TableSchema,
    type_context: expr_type.RowExpressionTypeContext,
    order_by: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsQueryOrder),
    select: plan_mod.WindowSelectList,
    options: OutputExpressionParserOptions,
) !void {
    while (true) {
        const item_start = pos.*;
        const generated_item = try expr_generated_validate.generatedOrderItemAtStart(tokens, item_start, options.generated_read_ast);
        if (generated_item == null and options.generated_read_ast != null) return error.UnsupportedSqlShape;
        const generated_expression = if (generated_item) |item| item.expression else null;
        var item_options = options;
        item_options.order_expression_hooks.generated_expression_ast = generated_expression;
        var order = if (parser.matchToken(tokens, pos, .number)) |token| blk: {
            try validateGeneratedSimpleOrderExpression(tokens, generated_expression);
            const ordinal = std.fmt.parseInt(u32, token.text, 10) catch return error.UnsupportedSqlShape;
            const field = try expr_window.outputFieldByOrdinalAlloc(alloc, select, ordinal);
            if (!expr_window.outputFieldIsUnique(select.fields, select.windows, field)) {
                alloc.free(field);
                return error.UnsupportedSqlShape;
            }
            break :blk db_mod.types.RelationalRowsQueryOrder{ .field = field };
        } else if (expr_window.peekOutputOrderExpression(tokens, pos.*)) blk: {
            break :blk try parseWindowOutputExpressionAlloc(alloc, tokens, pos, schema, type_context, select, item_options);
        } else blk: {
            try validateGeneratedSimpleOrderExpression(tokens, generated_expression);
            const field = try grammar.parseIdentifierOwnedAlloc(alloc, tokens, pos);
            if (!expr_window.outputFieldIsUnique(select.fields, select.windows, field)) {
                alloc.free(field);
                return error.UnsupportedSqlShape;
            }
            break :blk db_mod.types.RelationalRowsQueryOrder{ .field = field };
        };
        var order_transferred = false;
        errdefer if (!order_transferred) {
            if (order.field.len > 0) alloc.free(order.field);
            if (order.expression) |expression| freeExpression(alloc, expression);
            if (order.collation) |collation| alloc.free(collation);
        };
        const explicit_nulls_first = try parseModifiers(tokens, pos, &order);
        try expr_generated_validate.validateGeneratedExpressionItemEnd(generated_item, pos.*);
        try appendWithNullPlacement(alloc, order_by, order, explicit_nulls_first);
        order_transferred = true;
        if (parser.matchToken(tokens, pos, .comma) == null) break;
    }
}

pub fn parseJoinByAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    schema: runtime_schema.TableSchema,
    order_by: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsQueryOrder),
    select: []const db_mod.types.RelationalRowsJoinProjection,
    options: OutputExpressionParserOptions,
) !void {
    while (true) {
        const item_start = pos.*;
        const generated_item = try expr_generated_validate.generatedOrderItemAtStart(tokens, item_start, options.generated_read_ast);
        if (generated_item == null and options.generated_read_ast != null) return error.UnsupportedSqlShape;
        const generated_expression = if (generated_item) |item| item.expression else null;
        var item_options = options;
        item_options.order_expression_hooks.generated_expression_ast = generated_expression;
        var order = if (parser.matchToken(tokens, pos, .number)) |token| blk: {
            try validateGeneratedSimpleOrderExpression(tokens, generated_expression);
            const ordinal = std.fmt.parseInt(u32, token.text, 10) catch return error.UnsupportedSqlShape;
            const field = try expr_projection.joinOutputFieldByOrdinalAlloc(alloc, select, ordinal);
            if (!expr_projection.joinOutputIsUnique(select, field)) {
                alloc.free(field);
                return error.UnsupportedSqlShape;
            }
            break :blk db_mod.types.RelationalRowsQueryOrder{ .field = field };
        } else if (expr_aggregate.peekOutputOrderExpression(tokens, pos.*)) blk: {
            break :blk try parseJoinOutputExpressionAlloc(alloc, tokens, pos, schema, select, item_options);
        } else blk: {
            try validateGeneratedSimpleOrderExpression(tokens, generated_expression);
            const field = try grammar.parseIdentifierOwnedAlloc(alloc, tokens, pos);
            if (!expr_projection.joinOutputIsUnique(select, field)) {
                alloc.free(field);
                return error.UnsupportedSqlShape;
            }
            break :blk db_mod.types.RelationalRowsQueryOrder{ .field = field };
        };
        var order_transferred = false;
        errdefer if (!order_transferred) {
            if (order.field.len > 0) alloc.free(order.field);
            if (order.expression) |expression| freeExpression(alloc, expression);
            if (order.collation) |collation| alloc.free(collation);
        };
        const explicit_nulls_first = try parseModifiers(tokens, pos, &order);
        try expr_generated_validate.validateGeneratedExpressionItemEnd(generated_item, pos.*);
        try appendWithNullPlacement(alloc, order_by, order, explicit_nulls_first);
        order_transferred = true;
        if (parser.matchToken(tokens, pos, .comma) == null) break;
    }
}

pub fn parseOptionalDistinctOnAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    type_context: expr_type.RowExpressionTypeContext,
    row_expression_hooks: expr_row_parse.RowExpressionParserHooks,
    arithmetic_hooks: expr_row_parse.ArithmeticExpressionParserHooks,
    variadic_hooks: expr_row_parse.VariadicRowExpressionParserHooks,
) ![]const db_mod.types.RelationalRowsExpression {
    if (!parser.matchKeyword(tokens, pos, "distinct")) return &.{};
    if (!parser.matchKeyword(tokens, pos, "on")) return error.UnsupportedSqlShape;
    try parser.expectToken(tokens, pos, .lparen);
    var expressions = std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpression).empty;
    errdefer {
        freeExpressionSlice(alloc, expressions.items);
        expressions.deinit(alloc);
    }
    while (true) {
        const expression = try expr_row_parse.parseRowExpressionAlloc(
            alloc,
            tokens,
            pos,
            type_context,
            row_expression_hooks,
            arithmetic_hooks,
            variadic_hooks,
        );
        var expression_transferred = false;
        errdefer if (!expression_transferred) freeExpression(alloc, expression);
        _ = try type_context.rowExpressionOutputType(expression);
        try expressions.append(alloc, expression);
        expression_transferred = true;
        if (parser.matchToken(tokens, pos, .comma) == null) break;
    }
    try parser.expectToken(tokens, pos, .rparen);
    if (expressions.items.len == 0) return error.UnsupportedSqlShape;
    return try expressions.toOwnedSlice(alloc);
}

pub fn parseModifiers(
    tokens: []const Token,
    pos: *usize,
    order: *db_mod.types.RelationalRowsQueryOrder,
) !?bool {
    order.direction = if (parser.matchKeyword(tokens, pos, "using"))
        try parseUsingDirection(tokens, pos)
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

pub fn parseUsingDirection(
    tokens: []const Token,
    pos: *usize,
) !db_mod.types.RelationalRowsQueryOrderDirection {
    if (parser.matchToken(tokens, pos, .lt) != null or parser.matchToken(tokens, pos, .lte) != null) return .asc;
    if (parser.matchToken(tokens, pos, .gt) != null or parser.matchToken(tokens, pos, .gte) != null) return .desc;
    return error.UnsupportedSqlShape;
}

pub fn appendWithNullPlacement(
    alloc: std.mem.Allocator,
    order_by: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsQueryOrder),
    order: db_mod.types.RelationalRowsQueryOrder,
    explicit_nulls_first: ?bool,
) !void {
    if (explicit_nulls_first) |nulls_first| {
        if (order.null_test != null) return error.UnsupportedSqlShape;
        var null_order = try cloneForNullTestAlloc(alloc, order);
        var null_order_transferred = false;
        errdefer if (!null_order_transferred) {
            if (null_order.field.len > 0) alloc.free(null_order.field);
            if (null_order.expression) |expression| freeExpression(alloc, expression);
            if (null_order.collation) |collation| alloc.free(collation);
        };
        null_order.direction = if (nulls_first) .desc else .asc;
        null_order.null_test = .is_null;
        try order_by.append(alloc, null_order);
        null_order_transferred = true;
    }
    try order_by.append(alloc, order);
}

pub fn cloneForNullTestAlloc(
    alloc: std.mem.Allocator,
    order: db_mod.types.RelationalRowsQueryOrder,
) !db_mod.types.RelationalRowsQueryOrder {
    if (order.expression) |expression| {
        return .{ .expression = try plan_mod.cloneExpressionAlloc(alloc, expression) };
    }
    if (order.field.len == 0) return error.UnsupportedSqlShape;
    const field = try alloc.dupe(u8, order.field);
    errdefer alloc.free(field);
    const collation = if (order.collation) |value| try alloc.dupe(u8, value) else null;
    errdefer if (collation) |value| alloc.free(value);
    return .{
        .field = field,
        .collation = collation,
    };
}

pub fn testOrderNullPlacementAndModifiers() !void {
    const alloc = std.testing.allocator;

    var placed_orders = std.ArrayListUnmanaged(db_mod.types.RelationalRowsQueryOrder).empty;
    defer {
        plan_mod.freeOrderBy(alloc, placed_orders.items);
        placed_orders.deinit(alloc);
    }
    const order_field = try alloc.dupe(u8, "status");
    var order_field_transferred = false;
    errdefer if (!order_field_transferred) alloc.free(order_field);
    try appendWithNullPlacement(alloc, &placed_orders, .{
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
        plan_mod.freeOrderBy(alloc, expression_orders.items);
        expression_orders.deinit(alloc);
    }
    const order_expression: db_mod.types.RelationalRowsExpression = .{ .kind = .value, .value_json = try alloc.dupe(u8, "1") };
    var order_expression_transferred = false;
    errdefer if (!order_expression_transferred) freeExpression(alloc, order_expression);
    try appendWithNullPlacement(alloc, &expression_orders, .{
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
        .{ .kind = .identifier, .text = "desc", .source_start = 0, .source_end = 4, .keyword = .desc },
        .{ .kind = .identifier, .text = "nulls", .source_start = 5, .source_end = 10, .keyword = .nulls },
        .{ .kind = .identifier, .text = "last", .source_start = 11, .source_end = 15, .keyword = .last },
    };
    try std.testing.expectEqual(false, (try parseModifiers(desc_tokens[0..], &desc_pos, &desc_order)).?);
    try std.testing.expectEqual(@as(usize, 3), desc_pos);
    try std.testing.expectEqual(db_mod.types.RelationalRowsQueryOrderDirection.desc, desc_order.direction);

    var asc_order: db_mod.types.RelationalRowsQueryOrder = .{ .field = "status" };
    var asc_pos: usize = 0;
    const asc_tokens = [_]Token{
        .{ .kind = .identifier, .text = "asc", .source_start = 0, .source_end = 3, .keyword = .asc },
        .{ .kind = .identifier, .text = "nulls", .source_start = 4, .source_end = 9, .keyword = .nulls },
        .{ .kind = .identifier, .text = "first", .source_start = 10, .source_end = 15, .keyword = .first },
    };
    try std.testing.expectEqual(true, (try parseModifiers(asc_tokens[0..], &asc_pos, &asc_order)).?);
    try std.testing.expectEqual(@as(usize, 3), asc_pos);
    try std.testing.expectEqual(db_mod.types.RelationalRowsQueryOrderDirection.asc, asc_order.direction);

    var using_order: db_mod.types.RelationalRowsQueryOrder = .{ .field = "status" };
    var using_pos: usize = 0;
    const using_tokens = [_]Token{
        .{ .kind = .identifier, .text = "using", .source_start = 0, .source_end = 5, .keyword = .using },
        .{ .kind = .gte, .text = ">=", .source_start = 6, .source_end = 8 },
    };
    try std.testing.expect((try parseModifiers(using_tokens[0..], &using_pos, &using_order)) == null);
    try std.testing.expectEqual(@as(usize, 2), using_pos);
    try std.testing.expectEqual(db_mod.types.RelationalRowsQueryOrderDirection.desc, using_order.direction);
}

pub fn testDistinctOnOrderValidation() !void {
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

    try validateDistinctOnOrder(&.{.{ .kind = .field, .field = "status" }}, &.{.{ .field = "status" }});
    try validateDistinctOnOrder(&.{lower_status}, &.{.{ .expression = same_lower_status }});
    try std.testing.expectError(error.UnsupportedSqlShape, validateDistinctOnOrder(&.{lower_status}, &.{.{ .expression = upper_status }}));
    try std.testing.expectError(error.UnsupportedSqlShape, validateDistinctOnOrder(
        &.{ .{ .kind = .field, .field = "tenant_id" }, .{ .kind = .field, .field = "status" } },
        &.{.{ .field = "tenant_id" }},
    ));
}

test "sql expr order handles null placement and modifiers" {
    try testOrderNullPlacementAndModifiers();
}

test "sql expr order validates distinct on order expressions" {
    try testDistinctOnOrderValidation();
}
