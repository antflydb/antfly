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
const db_mod = struct {
    pub const types = @import("../../storage/db/types.zig");
};
const ddl_plan = @import("../ddl_plan.zig");
const expr_build = @import("build.zig");
const expr_generated = @import("generated.zig");
const expr_generated_validate = @import("generated_validate.zig");
const strings = @import("../strings.zig");
const expr_operator = @import("operator.zig");
const expr_parse = @import("parse.zig");
const expr_row_parse = @import("row_parse.zig");
const expr_token = @import("token.zig");
const expr_type = @import("type.zig");
const generated_parser = @import("../generated_parser.zig");
const grammar = @import("../grammar.zig");
const parser = @import("../parser.zig");
const plan_mod = @import("../plan.zig");
const runtime_schema = @import("../../storage/schema.zig");
const token_mod = @import("../token.zig");
const value_mod = @import("../value.zig");

pub const Token = token_mod.Token;
const GeneratedExpressionItem = expr_generated_validate.GeneratedExpressionItem;

const cloneExpressionAlloc = plan_mod.cloneExpressionAlloc;
const freeExpression = plan_mod.freeExpression;
const freeExpressionProjection = plan_mod.freeExpressionProjection;
const freeExpressionProjections = plan_mod.freeExpressionProjections;

pub const SelectItemStart = enum {
    pipe_concat,
    unary_positive,
    unary_negative,
    boolean_not,
    extension_function,
    routine_expression,
    uuid_v4,
    now,
    current_date,
    typed_datetime_literal,
    array_constructor,
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
    soundex,
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

pub fn selectItemStartAt(tokens: []const Token, pos: usize) ?SelectItemStart {
    if (expr_parse.rowExpressionHasTopLevelPipeConcat(tokens, pos)) return .pipe_concat;
    if (parser.peekKind(tokens, pos, .plus)) return .unary_positive;
    if (expr_token.peekUnaryNegativeExpressionSyntax(tokens, pos)) return .unary_negative;
    if (expr_token.peekBooleanNotExpressionSyntax(tokens, pos)) return .boolean_not;
    if (expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsUuidV4Function)) return .uuid_v4;
    if (expr_token.peekSqlNowExpressionSyntax(tokens, pos)) return .now;
    if (expr_token.peekSqlCurrentDateExpressionSyntax(tokens, pos)) return .current_date;
    if (expr_parse.peekSqlTypedDatetimeLiteral(tokens, pos)) return .typed_datetime_literal;
    if (parser.peekKeywordTag(tokens, pos, .array)) return .array_constructor;
    if (expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsJsonExtractPathFunction)) return .json_extract_path;
    if (expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsJsonTypeofFunction)) return .json_typeof;
    if (expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsJsonArrayLengthFunction)) return .json_array_length;
    if (expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsJsonBuildObjectFunction)) return .json_build_object;
    if (value_mod.peekConvertFromFunctionCall(tokens, pos)) return .convert_from;
    if (value_mod.peekToJsonbFunctionCall(tokens, pos)) return .to_jsonb;
    if (expr_token.functionCallStartsAtTokenIf(tokens, pos, expr_token.sqlTokenIsArrayLengthFunction)) return .array_length;
    if (expr_token.functionCallStartsAtTokenIf(tokens, pos, expr_token.sqlTokenIsArrayPositionFunction)) return .array_position;
    if (expr_token.peekArrayElementTransformFunctionCall(tokens, pos)) return .array_element_transform;
    if (expr_token.peekArrayToStringFunctionCall(tokens, pos)) return .array_to_string;
    if (expr_token.peekStringToArrayFunctionCall(tokens, pos)) return .string_to_array;
    if (expr_token.peekCoalesceFunctionCall(tokens, pos)) return .coalesce;
    if (expr_token.peekCaseFoldFunctionCall(tokens, pos)) return .case_fold;
    if (expr_token.peekReplaceFunctionCall(tokens, pos)) return .replace;
    if (expr_token.peekRegexpReplaceFunctionCall(tokens, pos)) return .regexp_replace;
    if (expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsRegexpSubstrFunction)) return .regexp_substr;
    if (expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsRegexpMatchFunction)) return .regexp_match;
    if (expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsRegexpCountFunction)) return .regexp_count;
    if (expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsRegexpInstrFunction)) return .regexp_instr;
    if (expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsTranslateFunction)) return .translate;
    if (expr_token.peekConcatFunctionCall(tokens, pos)) return .concat;
    if (expr_token.peekNullifFunctionCall(tokens, pos)) return .nullif;
    if (expr_token.peekTextLengthFunctionKeyword(tokens, pos)) return .text_length;
    if (expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsAsciiFunction)) return .ascii;
    if (expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsChrFunction)) return .chr;
    if (expr_token.peekSubstringFunctionKeyword(tokens, pos)) return .substring;
    if (expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsOverlayFunction)) return .overlay;
    if (expr_token.peekSplitPartFunctionKeyword(tokens, pos)) return .split_part;
    if (expr_token.peekStrposFunctionKeyword(tokens, pos) or expr_token.peekPositionFunctionSyntax(tokens, pos)) return .strpos;
    if (expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsLeftRightFunction)) return .left_right;
    if (expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsPadFunction)) return .pad;
    if (expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsRepeatFunction)) return .repeat;
    if (expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsReverseFunction)) return .reverse;
    if (expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsMd5Function)) return .md5;
    if (expr_token.peekFixedUnaryFunctionCall(tokens, pos, .soundex)) return .soundex;
    if (expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsStartsWithFunction)) return .starts_with;
    if (expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsEndsWithFunction)) return .ends_with;
    if (expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsDateTruncFunction)) return .date_trunc;
    if (expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsDateBinFunction)) return .date_bin;
    if (expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsDatePartFunction)) return .date_part;
    if (expr_token.peekFixedUnaryFunctionCall(tokens, pos, .abs)) return .abs;
    if (expr_token.peekFixedUnaryFunctionCall(tokens, pos, .round)) return .round;
    if (expr_token.peekFixedUnaryFunctionCall(tokens, pos, .trunc)) return .trunc;
    if (expr_token.peekFixedUnaryFunctionCall(tokens, pos, .floor)) return .floor;
    if (expr_token.peekFixedUnaryFunctionCall(tokens, pos, .ceil)) return .ceil;
    if (expr_token.peekFixedUnaryFunctionCall(tokens, pos, .sqrt)) return .sqrt;
    if (expr_token.peekFixedUnaryFunctionCall(tokens, pos, .sign)) return .sign;
    if (expr_token.peekFixedBinaryFunctionCall(tokens, pos, .mod)) return .mod;
    if (expr_token.peekFixedBinaryFunctionCall(tokens, pos, .power)) return .power;
    if (expr_token.peekGreatestLeastFunctionCall(tokens, pos)) return .greatest_least;
    if (expr_token.peekCaseExpressionSyntax(tokens, pos)) return .case;
    if (expr_token.peekCastExpressionSyntax(tokens, pos)) return .cast;
    if (expr_token.peekParenthesizedExpressionSyntax(tokens, pos)) return .parenthesized;
    return null;
}

pub fn peekParenthesizedNullTestProjection(tokens: []const Token, pos: usize) bool {
    if (pos + 2 >= tokens.len) return false;
    return tokens[pos].kind == .lparen and
        tokens[pos + 1].kind == .identifier and
        tokens[pos + 2].matchesKeywordTag(.is);
}

pub fn selectItemStartWithFunctionBindingsAt(tokens: []const Token, pos: usize, bindings: expr_row_parse.SqlFunctionBindings) ?SelectItemStart {
    if (selectItemStartAt(tokens, pos)) |start| return start;
    if (expr_row_parse.peekExtensionFunctionCall(tokens, pos, bindings.extension_functions)) return .extension_function;
    if (expr_row_parse.peekRoutineExpressionCall(tokens, pos, bindings.routine_expressions)) return .routine_expression;
    return null;
}

pub const ProjectedColumnType = struct {
    field_type: runtime_schema.AntflyType,
    array_item_type: ?runtime_schema.AntflyType = null,
};

pub const ExpressionProjectionParserOptions = struct {
    type_context: expr_type.RowExpressionTypeContext,
    row_expression_hooks: expr_row_parse.RowExpressionParserHooks,
    arithmetic_hooks: expr_row_parse.ArithmeticExpressionParserHooks,
    variadic_hooks: expr_row_parse.VariadicRowExpressionParserHooks,
    boolean_hooks: expr_row_parse.BooleanRowExpressionParserHooks,
    generated_expression_ast: ?*const generated_parser.GeneratedSqlExpressionAst = null,
    require_exact_generated_expression: bool = false,
};

pub const JsonValueExpressionProjectionParserOptions = struct {
    params: []const value_mod.SqlValue = &.{},
};

pub fn peekUnsupportedSimpleFieldTail(tokens: []const Token, pos: usize) bool {
    return parser.peekKind(tokens, pos, .lparen) or expr_operator.peekJsonExtractOperator(tokens, pos);
}

pub fn peekSimpleReturningField(tokens: []const Token, pos: usize) bool {
    if (pos >= tokens.len or tokens[pos].kind != .identifier) return false;
    if (parser.peekKeyword(tokens, pos, "lower") or
        parser.peekKeyword(tokens, pos, "upper") or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsInitcapFunction) or
        parser.peekKeyword(tokens, pos, "trim") or
        parser.peekKeyword(tokens, pos, "replace") or
        parser.peekKeyword(tokens, pos, "regexp_replace") or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsRegexpSubstrFunction) or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsRegexpMatchFunction) or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsRegexpCountFunction) or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsRegexpInstrFunction) or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsTranslateFunction) or
        parser.peekKeyword(tokens, pos, "concat") or
        parser.peekKeyword(tokens, pos, "concat_ws") or
        parser.peekKeyword(tokens, pos, "coalesce") or
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
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsMd5Function) or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsStartsWithFunction) or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsEndsWithFunction) or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsDateTruncFunction) or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsDateBinFunction) or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsDatePartFunction) or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsTrimVariantFunction) or
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
        parser.peekKeyword(tokens, pos, "case") or
        parser.peekKeyword(tokens, pos, "cast"))
    {
        return false;
    }
    if (pos + 1 >= tokens.len) return true;
    if (expr_operator.peekBooleanOperator(tokens, pos + 1) != null) return false;
    return switch (tokens[pos + 1].kind) {
        .plus, .minus, .star, .slash, .percent, .lparen, .arrow_json, .arrow_text, .path_arrow_json, .path_arrow_text, .pipe_concat => false,
        else => true,
    };
}

pub fn parseGroupByAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    schema: runtime_schema.TableSchema,
    field_expression_qualifiers: []const []const u8,
    returning_expression_qualifiers: []const []const u8,
    defer_row_expression_field_validation: bool,
    group_by: *std.ArrayListUnmanaged([]const u8),
) !void {
    while (true) {
        const field = try expr_generated.parseRowExpressionFieldOwnedAlloc(
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
        if (peekUnsupportedSimpleFieldTail(tokens, pos.*)) return error.UnsupportedSqlShape;
        if (binder.relationalColumnForField(schema, field, null) == null) return error.InvalidSqlCatalog;
        try group_by.append(alloc, field);
        field_transferred = true;
        if (parser.matchToken(tokens, pos, .comma) == null) break;
    }
}

fn parseCoalesceFieldOperandOrNullOwnedAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    schema: runtime_schema.TableSchema,
    field_expression_qualifiers: []const []const u8,
    returning_expression_qualifiers: []const []const u8,
    defer_row_expression_field_validation: bool,
) !?db_mod.types.RelationalRowsCoalesceOperand {
    if (!parser.peekKind(tokens, pos.*, .identifier) or
        parser.peekKeywordTag(tokens, pos.*, .null) or
        parser.peekKeywordTag(tokens, pos.*, .true) or
        parser.peekKeywordTag(tokens, pos.*, .false))
    {
        return null;
    }

    const field = try expr_generated.parseRowExpressionFieldOwnedAlloc(
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

pub const CoalesceProjectionParserOptions = struct {
    params: []const value_mod.SqlValue = &.{},
};

pub fn parseCoalesceProjectionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    schema: runtime_schema.TableSchema,
    field_expression_qualifiers: []const []const u8,
    returning_expression_qualifiers: []const []const u8,
    defer_row_expression_field_validation: bool,
    hooks: CoalesceProjectionParserOptions,
) !db_mod.types.RelationalRowsCoalesceProjection {
    try expr_token.parseCoalesceFunctionCallStart(tokens, pos);
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
    hooks: CoalesceProjectionParserOptions,
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

    const value_json = try value_mod.parseJsonValueAlloc(alloc, tokens, pos, hooks.params);
    errdefer alloc.free(value_json);
    return .{ .kind = .value, .value_json = value_json };
}

fn parseArrayFieldOwnedAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    schema: runtime_schema.TableSchema,
) ![]const u8 {
    const field = try grammar.parseIdentifierOwnedAlloc(alloc, tokens, pos);
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
    const parsed_field = try grammar.parseIdentifierOwnedAlloc(alloc, tokens, pos);
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
    const keyword = try expr_token.parseArrayLengthFunctionCallStart(tokens, pos);
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
    const expression = try expr_build.buildUnaryFunctionExpressionAlloc(alloc, .array_length, field_expression);
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
    const keyword = try expr_token.parseArrayLengthFunctionCallStart(tokens, pos);
    const field = try parseArrayFieldOwnedAlloc(alloc, tokens, pos, schema);
    var field_transferred = false;
    errdefer if (!field_transferred) alloc.free(field);
    try value_mod.parseArrayLengthFunctionTail(tokens, pos, params, keyword);
    const output = try grammar.parseProjectionOutputOwnedAlloc(alloc, tokens, pos, expr_token.arrayLengthDefaultOutput(keyword));
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
    const keyword = try expr_token.parseArrayLengthFunctionCallStart(tokens, pos);
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
    const output = try grammar.parseProjectionOutputOwnedAlloc(alloc, tokens, pos, expr_token.arrayLengthDefaultOutput(keyword));
    var output_transferred = false;
    errdefer if (!output_transferred) alloc.free(output);
    const expression = try expr_build.buildUnaryFunctionExpressionAlloc(
        alloc,
        .array_length,
        .{ .kind = .field, .field = field, .field_source = field_source },
    );

    field_transferred = true;
    output_transferred = true;
    return expr_build.buildExpressionProjection(output, expression);
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
    generated_expression_ast: ?*const generated_parser.GeneratedSqlExpressionAst,
) !db_mod.types.RelationalRowsExpressionProjection {
    try parser.expectToken(tokens, pos, .lparen);
    const field = try expr_generated.parseRowExpressionFieldOwnedAlloc(
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
    const op_token_index = pos.*;
    try parser.expectKeyword(tokens, pos, "is");
    const op: runtime_schema.RelationalCheckOp = if (parser.matchKeyword(tokens, pos, "not")) blk: {
        try parser.expectKeyword(tokens, pos, "null");
        break :blk .is_not_null;
    } else blk: {
        try parser.expectKeyword(tokens, pos, "null");
        break :blk .is_null;
    };
    try expr_generated_validate.validateGeneratedIsTailPredicateIdentity(generated_expression_ast, tokens, op_token_index, .{
        .kind = .null_test,
        .op = op,
    });
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
    return expr_build.buildExpressionProjection(output, expression);
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
    const projection = expr_build.buildExpressionProjection(output, expression);
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
    return try buildExpressionProjectionFromOwnedExpressionAlloc(alloc, tokens, pos, expression, expr_type.rowExpressionOpName(expression.kind));
}

pub fn buildDefaultExpressionProjectionFromOwnedExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    expression: db_mod.types.RelationalRowsExpression,
) !db_mod.types.RelationalRowsExpressionProjection {
    return try buildExpressionProjectionFromOwnedExpressionAlloc(alloc, tokens, pos, expression, expr_type.rowExpressionDefaultOutputName(expression.kind));
}

pub fn buildTextExpressionProjectionFromOwnedExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    type_context: expr_type.RowExpressionTypeContext,
    expression: db_mod.types.RelationalRowsExpression,
) !db_mod.types.RelationalRowsExpressionProjection {
    var expression_transferred = false;
    errdefer if (!expression_transferred) freeExpression(alloc, expression);
    try type_context.validateTextRowExpression(expression);
    const default_output = if (expression.kind == .field and expression.field.len != 0)
        expression.field
    else
        expr_type.rowExpressionOpName(expression.kind);
    expression_transferred = true;
    const projection = try buildExpressionProjectionFromOwnedExpressionAlloc(alloc, tokens, pos, expression, default_output);
    return projection;
}

pub fn buildNumericExpressionProjectionFromOwnedExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    type_context: expr_type.RowExpressionTypeContext,
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
    type_context: expr_type.RowExpressionTypeContext,
    expression: db_mod.types.RelationalRowsExpression,
) !db_mod.types.RelationalRowsExpressionProjection {
    var expression_transferred = false;
    errdefer if (!expression_transferred) freeExpression(alloc, expression);
    try type_context.validateBooleanRowExpression(expression);
    expression_transferred = true;
    const projection = try buildDefaultExpressionProjectionFromOwnedExpressionAlloc(alloc, tokens, pos, expression);
    return projection;
}

fn validateGeneratedExpressionProjectionIdentity(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    start: usize,
    end: usize,
    expression: db_mod.types.RelationalRowsExpression,
    options: ExpressionProjectionParserOptions,
) !void {
    if (options.require_exact_generated_expression) {
        try expr_generated_validate.validateGeneratedRowExpressionIdentityStrictWithContext(
            .{ .alloc = alloc },
            tokens,
            start,
            end,
            expression,
            options.generated_expression_ast,
        );
    } else {
        try expr_generated_validate.validateGeneratedRowExpressionIdentity(tokens, start, end, expression, options.generated_expression_ast);
    }
}

pub fn parseTextExpressionProjectionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    type_context: expr_type.RowExpressionTypeContext,
    options: ExpressionProjectionParserOptions,
) !db_mod.types.RelationalRowsExpressionProjection {
    const start = pos.*;
    const expression = try expr_row_parse.parseRowExpressionAlloc(alloc, tokens, pos, options.type_context, options.row_expression_hooks, options.arithmetic_hooks, options.variadic_hooks);
    errdefer freeExpression(alloc, expression);
    try validateGeneratedExpressionProjectionIdentity(alloc, tokens, start, pos.*, expression, options);
    return try buildTextExpressionProjectionFromOwnedExpressionAlloc(alloc, tokens, pos, type_context, expression);
}

pub fn parseGenericExpressionProjectionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    type_context: expr_type.RowExpressionTypeContext,
    options: ExpressionProjectionParserOptions,
) !db_mod.types.RelationalRowsExpressionProjection {
    const start = pos.*;
    const expression = try expr_row_parse.parseRowExpressionAlloc(alloc, tokens, pos, options.type_context, options.row_expression_hooks, options.arithmetic_hooks, options.variadic_hooks);
    errdefer freeExpression(alloc, expression);
    try validateGeneratedExpressionProjectionIdentity(alloc, tokens, start, pos.*, expression, options);
    return try buildNumericExpressionProjectionFromOwnedExpressionAlloc(alloc, tokens, pos, type_context, expression);
}

pub fn parseUnaryPositiveExpressionProjectionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    type_context: expr_type.RowExpressionTypeContext,
    options: ExpressionProjectionParserOptions,
) !db_mod.types.RelationalRowsExpressionProjection {
    const start = pos.*;
    try parser.expectToken(tokens, pos, .plus);
    const expression = try expr_row_parse.parseRowExpressionAlloc(alloc, tokens, pos, options.type_context, options.row_expression_hooks, options.arithmetic_hooks, options.variadic_hooks);
    errdefer freeExpression(alloc, expression);
    try validateGeneratedExpressionProjectionIdentity(alloc, tokens, start, pos.*, expression, options);
    return try buildNumericExpressionProjectionFromOwnedExpressionAlloc(alloc, tokens, pos, type_context, expression);
}

pub fn parseBooleanExpressionProjectionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    type_context: expr_type.RowExpressionTypeContext,
    options: ExpressionProjectionParserOptions,
) !db_mod.types.RelationalRowsExpressionProjection {
    const start = pos.*;
    const expression = try expr_row_parse.parseBooleanRowExpressionAlloc(alloc, tokens, pos, options.type_context, options.boolean_hooks);
    errdefer freeExpression(alloc, expression);
    try validateGeneratedExpressionProjectionIdentity(alloc, tokens, start, pos.*, expression, options);
    return try buildBooleanExpressionProjectionFromOwnedExpressionAlloc(alloc, tokens, pos, type_context, expression);
}

pub fn parseParenthesizedExpressionProjectionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    options: ExpressionProjectionParserOptions,
) !db_mod.types.RelationalRowsExpressionProjection {
    try parser.expectToken(tokens, pos, .lparen);
    const expression = try expr_row_parse.parseBooleanRowExpressionAlloc(alloc, tokens, pos, options.type_context, options.boolean_hooks);
    errdefer freeExpression(alloc, expression);
    try parser.expectToken(tokens, pos, .rparen);
    return try buildDefaultExpressionProjectionFromOwnedExpressionAlloc(alloc, tokens, pos, expression);
}

pub fn parseNowExpressionProjectionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !db_mod.types.RelationalRowsExpressionProjection {
    const expression = try expr_build.parseSqlNowRowExpressionAlloc(alloc, tokens, pos);
    return try buildExpressionProjectionFromOwnedExpressionAlloc(alloc, tokens, pos, expression, "now");
}

pub fn parseCurrentDateExpressionProjectionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !db_mod.types.RelationalRowsExpressionProjection {
    const expression = try expr_build.parseSqlCurrentDateRowExpressionAlloc(alloc, tokens, pos);
    return try buildExpressionProjectionFromOwnedExpressionAlloc(alloc, tokens, pos, expression, "current_date");
}

pub fn parseTypedDatetimeLiteralExpressionProjectionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !db_mod.types.RelationalRowsExpressionProjection {
    const expression = try expr_build.parseSqlTypedDatetimeLiteralRowExpressionAlloc(alloc, tokens, pos);
    return try buildExpressionProjectionFromOwnedExpressionAlloc(alloc, tokens, pos, expression, "datetime_literal");
}

pub fn parseUuidV4ExpressionProjectionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !db_mod.types.RelationalRowsExpressionProjection {
    const expression = try expr_build.parseSqlUuidV4RowExpression(tokens, pos);
    return try buildExpressionProjectionFromOwnedExpressionAlloc(alloc, tokens, pos, expression, "gen_random_uuid");
}

pub fn parseFixedOutputExpressionProjectionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    default_output: []const u8,
    options: ExpressionProjectionParserOptions,
) !db_mod.types.RelationalRowsExpressionProjection {
    const start = pos.*;
    const expression = try expr_row_parse.parseRowExpressionAlloc(alloc, tokens, pos, options.type_context, options.row_expression_hooks, options.arithmetic_hooks, options.variadic_hooks);
    errdefer freeExpression(alloc, expression);
    try validateGeneratedExpressionProjectionIdentity(alloc, tokens, start, pos.*, expression, options);
    return try buildExpressionProjectionFromOwnedExpressionAlloc(alloc, tokens, pos, expression, default_output);
}

pub fn parseOpOutputExpressionProjectionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    options: ExpressionProjectionParserOptions,
) !db_mod.types.RelationalRowsExpressionProjection {
    const start = pos.*;
    const expression = try expr_row_parse.parseRowExpressionAlloc(alloc, tokens, pos, options.type_context, options.row_expression_hooks, options.arithmetic_hooks, options.variadic_hooks);
    errdefer freeExpression(alloc, expression);
    try validateGeneratedExpressionProjectionIdentity(alloc, tokens, start, pos.*, expression, options);
    return try buildOpExpressionProjectionFromOwnedExpressionAlloc(alloc, tokens, pos, expression);
}

pub fn parseDefaultOutputExpressionProjectionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    options: ExpressionProjectionParserOptions,
) !db_mod.types.RelationalRowsExpressionProjection {
    const start = pos.*;
    const expression = try expr_row_parse.parseRowExpressionAlloc(alloc, tokens, pos, options.type_context, options.row_expression_hooks, options.arithmetic_hooks, options.variadic_hooks);
    errdefer freeExpression(alloc, expression);
    try validateGeneratedExpressionProjectionIdentity(alloc, tokens, start, pos.*, expression, options);
    return try buildDefaultExpressionProjectionFromOwnedExpressionAlloc(alloc, tokens, pos, expression);
}

pub fn parseRegexpMatchExpressionProjectionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    options: ExpressionProjectionParserOptions,
) !db_mod.types.RelationalRowsExpressionProjection {
    const default_output = if (parser.peekKeyword(tokens, pos.*, "regexp_like")) "regexp_like" else "regexp_match";
    const start = pos.*;
    const expression = try expr_row_parse.parseRowExpressionAlloc(alloc, tokens, pos, options.type_context, options.row_expression_hooks, options.arithmetic_hooks, options.variadic_hooks);
    errdefer freeExpression(alloc, expression);
    try validateGeneratedExpressionProjectionIdentity(alloc, tokens, start, pos.*, expression, options);
    return try buildExpressionProjectionFromOwnedExpressionAlloc(alloc, tokens, pos, expression, default_output);
}

pub fn parseJsonExtractPathExpressionProjectionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    options: ExpressionProjectionParserOptions,
) !db_mod.types.RelationalRowsExpressionProjection {
    const start = pos.*;
    const expression = try expr_row_parse.parseRowExpressionAlloc(alloc, tokens, pos, options.type_context, options.row_expression_hooks, options.arithmetic_hooks, options.variadic_hooks);
    errdefer freeExpression(alloc, expression);
    try validateGeneratedExpressionProjectionIdentity(alloc, tokens, start, pos.*, expression, options);
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
    bindings: []const expr_row_parse.ExtensionFunctionBinding,
    options: expr_row_parse.ExtensionFunctionRowExpressionParserOptions,
) !db_mod.types.RelationalRowsExpressionProjection {
    const expression = (try expr_row_parse.parseExtensionFunctionRowExpressionOrNullAlloc(alloc, tokens, pos, bindings, options)) orelse return error.UnsupportedSqlShape;
    return try buildOpExpressionProjectionFromOwnedExpressionAlloc(alloc, tokens, pos, expression);
}

pub fn parseRoutineExpressionProjectionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    bindings: []const expr_row_parse.RoutineExpressionBinding,
    options: expr_row_parse.RoutineExpressionRowExpressionParserOptions,
) !db_mod.types.RelationalRowsExpressionProjection {
    const expression = (try expr_row_parse.parseRoutineExpressionRowExpressionOrNullAlloc(alloc, tokens, pos, bindings, options)) orelse return error.UnsupportedSqlShape;
    return try buildOpExpressionProjectionFromOwnedExpressionAlloc(alloc, tokens, pos, expression);
}

pub fn parseJsonValueExpressionProjectionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    default_output: []const u8,
    hooks: JsonValueExpressionProjectionParserOptions,
) !db_mod.types.RelationalRowsExpressionProjection {
    const value_json = try value_mod.parseJsonValueAlloc(alloc, tokens, pos, hooks.params);
    const expression: db_mod.types.RelationalRowsExpression = .{
        .kind = .value,
        .value_json = value_json,
    };
    return try buildExpressionProjectionFromOwnedExpressionAlloc(alloc, tokens, pos, expression, default_output);
}

pub fn parseArrayConstructorExpressionProjectionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    params: []const value_mod.SqlValue,
    options: ExpressionProjectionParserOptions,
) !db_mod.types.RelationalRowsExpressionProjection {
    const start = pos.*;
    const value_json = try value_mod.parseSqlArrayConstructorJsonAlloc(alloc, tokens, pos, params);
    const expression: db_mod.types.RelationalRowsExpression = .{
        .kind = .value,
        .value_json = value_json,
    };
    errdefer freeExpression(alloc, expression);
    try validateGeneratedExpressionProjectionIdentity(alloc, tokens, start, pos.*, expression, options);
    return try buildExpressionProjectionFromOwnedExpressionAlloc(alloc, tokens, pos, expression, "array");
}

pub fn parseArithmeticExpressionProjectionFromFieldAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    expression_start: usize,
    lhs_field: []const u8,
    field_source: db_mod.types.RelationalRowsExpressionFieldSource,
    type_context: expr_type.RowExpressionTypeContext,
    hooks: expr_row_parse.ArithmeticExpressionParserHooks,
    generated_expression_ast: ?*const generated_parser.GeneratedSqlExpressionAst,
    require_exact_generated_expression: bool,
) !db_mod.types.RelationalRowsExpressionProjection {
    var current: db_mod.types.RelationalRowsExpression = .{
        .kind = .field,
        .field = lhs_field,
        .field_source = field_source,
    };
    var current_owned = true;
    errdefer if (current_owned) freeExpression(alloc, current);

    current_owned = false;
    current = try expr_row_parse.parseArithmeticExpressionRestAlloc(alloc, tokens, pos, current, 0, type_context, hooks);
    current_owned = true;
    if (require_exact_generated_expression) {
        try expr_generated_validate.validateGeneratedRowExpressionIdentityStrict(tokens, expression_start, pos.*, current, generated_expression_ast);
    } else {
        try expr_generated_validate.validateGeneratedRowExpressionIdentity(tokens, expression_start, pos.*, current, generated_expression_ast);
    }
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
    expression_start: usize,
    lhs_field: []const u8,
    field_source: db_mod.types.RelationalRowsExpressionFieldSource,
    type_context: expr_type.RowExpressionTypeContext,
    hooks: expr_row_parse.BooleanExpressionParserHooks,
    generated_expression_ast: ?*const generated_parser.GeneratedSqlExpressionAst,
    require_exact_generated_expression: bool,
) !db_mod.types.RelationalRowsExpressionProjection {
    var current: db_mod.types.RelationalRowsExpression = .{
        .kind = .field,
        .field = lhs_field,
        .field_source = field_source,
    };
    var current_owned = true;
    errdefer if (current_owned) freeExpression(alloc, current);

    current_owned = false;
    current = try expr_row_parse.parseBooleanExpressionRestAlloc(alloc, tokens, pos, current, 0, type_context, hooks);
    current_owned = true;
    if (require_exact_generated_expression) {
        try expr_generated_validate.validateGeneratedRowExpressionIdentityStrict(tokens, expression_start, pos.*, current, generated_expression_ast);
    } else {
        try expr_generated_validate.validateGeneratedRowExpressionIdentity(tokens, expression_start, pos.*, current, generated_expression_ast);
    }

    current_owned = false;
    return try buildBooleanExpressionProjectionFromOwnedExpressionAlloc(alloc, tokens, pos, type_context, current);
}

pub fn selectListOutputCount(
    fields: []const []const u8,
    json_extract: []const db_mod.types.RelationalRowsJsonExtractProjection,
    array_length: []const db_mod.types.RelationalRowsArrayLengthProjection,
    coalesce: []const db_mod.types.RelationalRowsCoalesceProjection,
    field_aliases: []const db_mod.types.RelationalRowsFieldAliasProjection,
    expressions: []const db_mod.types.RelationalRowsExpressionProjection,
    scalar_subqueries: []const db_mod.types.RelationalRowsScalarSubqueryProjection,
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
    for (scalar_subqueries) |projection| {
        if (std.mem.eql(u8, projection.output, output)) count += 1;
    }
    return count;
}

fn selectProjectionOutputCollision(
    schema: runtime_schema.TableSchema,
    select_all: bool,
    fields: []const []const u8,
    json_extract: []const db_mod.types.RelationalRowsJsonExtractProjection,
    array_length: []const db_mod.types.RelationalRowsArrayLengthProjection,
    coalesce: []const db_mod.types.RelationalRowsCoalesceProjection,
    field_aliases: []const db_mod.types.RelationalRowsFieldAliasProjection,
    expressions: []const db_mod.types.RelationalRowsExpressionProjection,
    scalar_subqueries: []const db_mod.types.RelationalRowsScalarSubqueryProjection,
    output: []const u8,
) bool {
    if (select_all and binder.relationalColumnForField(schema, output, null) != null) return true;
    return selectListOutputCount(fields, json_extract, array_length, coalesce, field_aliases, expressions, scalar_subqueries, output) > 0;
}

fn allocateDisambiguatedSelectProjectionOutputAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    select_all: bool,
    fields: []const []const u8,
    json_extract: []const db_mod.types.RelationalRowsJsonExtractProjection,
    array_length: []const db_mod.types.RelationalRowsArrayLengthProjection,
    coalesce: []const db_mod.types.RelationalRowsCoalesceProjection,
    field_aliases: []const db_mod.types.RelationalRowsFieldAliasProjection,
    expressions: []const db_mod.types.RelationalRowsExpressionProjection,
    scalar_subqueries: []const db_mod.types.RelationalRowsScalarSubqueryProjection,
    output: []const u8,
) !?[]const u8 {
    if (!select_all) return null;
    if (!selectProjectionOutputCollision(schema, select_all, fields, json_extract, array_length, coalesce, field_aliases, expressions, scalar_subqueries, output)) return null;

    var suffix: usize = 2;
    while (true) : (suffix += 1) {
        const candidate = try std.fmt.allocPrint(alloc, "{s}_{d}", .{ output, suffix });
        if (!selectProjectionOutputCollision(schema, select_all, fields, json_extract, array_length, coalesce, field_aliases, expressions, scalar_subqueries, candidate)) return candidate;
        alloc.free(candidate);
    }
}

pub fn disambiguateSelectProjectionOutputIfNeededAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    select_all: bool,
    fields: []const []const u8,
    json_extract: []const db_mod.types.RelationalRowsJsonExtractProjection,
    array_length: []const db_mod.types.RelationalRowsArrayLengthProjection,
    coalesce: []const db_mod.types.RelationalRowsCoalesceProjection,
    field_aliases: []const db_mod.types.RelationalRowsFieldAliasProjection,
    expressions: []const db_mod.types.RelationalRowsExpressionProjection,
    scalar_subqueries: []const db_mod.types.RelationalRowsScalarSubqueryProjection,
    output: *[]const u8,
) !void {
    const disambiguated = try allocateDisambiguatedSelectProjectionOutputAlloc(
        alloc,
        schema,
        select_all,
        fields,
        json_extract,
        array_length,
        coalesce,
        field_aliases,
        expressions,
        scalar_subqueries,
        output.*,
    ) orelse return;
    alloc.free(output.*);
    output.* = disambiguated;
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
    scalar_subqueries: []const db_mod.types.RelationalRowsScalarSubqueryProjection,
) !void {
    if (!select_all) {
        for (fields) |field| {
            if (field.len == 0) return error.UnsupportedSqlShape;
            if (selectListOutputCount(fields, json_extract, array_length, coalesce, field_aliases, expressions, scalar_subqueries, field) > 1) return error.UnsupportedSqlShape;
        }
    }
    for (json_extract) |projection| try validateSelectProjectionOutput(schema, select_all, fields, json_extract, array_length, coalesce, field_aliases, expressions, scalar_subqueries, projection.output);
    for (array_length) |projection| try validateSelectProjectionOutput(schema, select_all, fields, json_extract, array_length, coalesce, field_aliases, expressions, scalar_subqueries, projection.output);
    for (coalesce) |projection| try validateSelectProjectionOutput(schema, select_all, fields, json_extract, array_length, coalesce, field_aliases, expressions, scalar_subqueries, projection.output);
    for (field_aliases) |projection| try validateSelectProjectionOutput(schema, select_all, fields, json_extract, array_length, coalesce, field_aliases, expressions, scalar_subqueries, projection.output);
    for (expressions) |projection| try validateSelectProjectionOutput(schema, select_all, fields, json_extract, array_length, coalesce, field_aliases, expressions, scalar_subqueries, projection.output);
    for (scalar_subqueries) |projection| try validateSelectProjectionOutput(schema, select_all, fields, json_extract, array_length, coalesce, field_aliases, expressions, scalar_subqueries, projection.output);
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
    scalar_subqueries: []const db_mod.types.RelationalRowsScalarSubqueryProjection,
    output: []const u8,
) !void {
    if (output.len == 0) return error.UnsupportedSqlShape;
    if (select_all and binder.relationalColumnForField(schema, output, null) != null) return error.UnsupportedSqlShape;
    if (selectListOutputCount(fields, json_extract, array_length, coalesce, field_aliases, expressions, scalar_subqueries, output) > 1) return error.UnsupportedSqlShape;
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

pub fn projectedSourceColumnAlloc(
    alloc: std.mem.Allocator,
    output_name: []const u8,
    source: runtime_schema.RelationalColumn,
) !runtime_schema.RelationalColumn {
    const owned_name = try alloc.dupe(u8, output_name);
    errdefer alloc.free(owned_name);
    const owned_path = try alloc.dupe(u8, output_name);
    errdefer alloc.free(owned_path);
    const collation = if (source.collation) |value| try alloc.dupe(u8, value) else null;
    errdefer if (collation) |value| alloc.free(value);
    return .{
        .name = owned_name,
        .path = owned_path,
        .field_type = source.field_type,
        .array_item_type = source.array_item_type,
        .nullable = source.nullable,
        .collation = collation,
    };
}

pub fn outputColumnExists(columns: []const runtime_schema.RelationalColumn, name: []const u8) bool {
    for (columns) |column| {
        if (std.mem.eql(u8, column.name, name)) return true;
    }
    return false;
}

fn projectedExpressionColumnAlloc(
    alloc: std.mem.Allocator,
    type_context: expr_type.RowExpressionTypeContext,
    output_name: []const u8,
    expression: db_mod.types.RelationalRowsExpression,
    scalar_subqueries: []const db_mod.types.RelationalRowsScalarSubqueryProjection,
) !runtime_schema.RelationalColumn {
    var type_context_with_hidden = type_context;
    var synthetic_columns: []runtime_schema.RelationalColumn = &.{};
    defer alloc.free(synthetic_columns);
    var hidden_count: usize = 0;
    for (scalar_subqueries) |projection| {
        if (projection.hidden) hidden_count += 1;
    }
    if (hidden_count > 0) {
        synthetic_columns = try alloc.alloc(runtime_schema.RelationalColumn, type_context.schema.relational_columns.len + hidden_count);
        @memcpy(synthetic_columns[0..type_context.schema.relational_columns.len], type_context.schema.relational_columns);
        var index = type_context.schema.relational_columns.len;
        for (scalar_subqueries) |projection| {
            if (!projection.hidden) continue;
            const source = binder.relationalColumnForField(type_context.schema, projection.output_field, null);
            synthetic_columns[index] = .{
                .name = projection.output,
                .path = projection.output,
                .field_type = if (source) |column| column.field_type else .json,
                .array_item_type = if (source) |column| column.array_item_type else null,
            };
            index += 1;
        }
        type_context_with_hidden.schema.relational_columns = synthetic_columns;
    }

    if (expression.kind == .field) {
        if (binder.relationalColumnForField(type_context_with_hidden.schemaForRowExpressionField(expression), expression.field, null)) |source| {
            return try projectedSourceColumnAlloc(alloc, output_name, source);
        }
        if (!type_context_with_hidden.defer_row_expression_field_validation) return error.InvalidSqlCatalog;
    }
    const field_type = try type_context_with_hidden.rowExpressionOutputType(expression);
    const array_item_type = try type_context_with_hidden.rowExpressionOutputArrayItemType(expression);
    return try projectedColumnAlloc(alloc, output_name, field_type, array_item_type, true);
}

pub fn selectOutputColumnsAlloc(
    alloc: std.mem.Allocator,
    type_context: expr_type.RowExpressionTypeContext,
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
        if (outputColumnExists(out[0..initialized], column.name)) return error.UnsupportedSqlShape;
        out[initialized] = column;
        column_transferred = true;
        initialized += 1;
    }
    return out;
}

pub fn loweredSelectOutputColumnsAlloc(
    alloc: std.mem.Allocator,
    type_context: expr_type.RowExpressionTypeContext,
    lowered: plan_mod.LoweredSelect,
) ![]runtime_schema.RelationalColumn {
    return try selectOutputColumnsAlloc(alloc, type_context, .{
        .fields = lowered.query.select,
        .json_extract = lowered.query.json_extract,
        .array_length = lowered.query.array_length,
        .coalesce = lowered.query.coalesce,
        .field_aliases = lowered.query.field_aliases,
        .expressions = lowered.query.expressions,
        .scalar_subqueries = lowered.query.scalar_subqueries,
        .outputs = lowered.select_outputs,
        .select_all = lowered.query.select_all,
    });
}

fn selectOutputColumnAlloc(
    alloc: std.mem.Allocator,
    type_context: expr_type.RowExpressionTypeContext,
    select: plan_mod.SelectList,
    output: ast.SelectOutputRef,
) !runtime_schema.RelationalColumn {
    return switch (output.kind) {
        .field => blk: {
            if (output.index >= select.fields.len) return error.UnsupportedSqlShape;
            const field = select.fields[output.index];
            const source = binder.relationalColumnForField(type_context.schema, field, null) orelse return error.InvalidSqlCatalog;
            break :blk try projectedSourceColumnAlloc(alloc, field, source);
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
            break :blk try projectedSourceColumnAlloc(alloc, projection.output, source);
        },
        .expression => blk: {
            if (output.index >= select.expressions.len) return error.UnsupportedSqlShape;
            const projection = select.expressions[output.index];
            break :blk try projectedExpressionColumnAlloc(alloc, type_context, projection.output, projection.expression, select.scalar_subqueries);
        },
        .scalar_subquery => blk: {
            if (output.index >= select.scalar_subqueries.len) return error.UnsupportedSqlShape;
            const projection = select.scalar_subqueries[output.index];
            const source = binder.relationalColumnForField(type_context.schema, projection.output_field, null);
            if (source) |column| break :blk try projectedColumnAlloc(alloc, projection.output, column.field_type, column.array_item_type, true);
            break :blk try projectedColumnAlloc(alloc, projection.output, .json, null, true);
        },
    };
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

fn cloneReturningOutputColumnAlloc(
    alloc: std.mem.Allocator,
    output_name: []const u8,
    source: runtime_schema.RelationalColumn,
) !runtime_schema.RelationalColumn {
    const name = try alloc.dupe(u8, output_name);
    errdefer alloc.free(name);
    const path = try alloc.dupe(u8, output_name);
    errdefer alloc.free(path);
    const collation = if (source.collation) |value| try alloc.dupe(u8, value) else null;
    errdefer if (collation) |value| alloc.free(value);
    return .{
        .name = name,
        .path = path,
        .field_type = source.field_type,
        .array_item_type = source.array_item_type,
        .nullable = source.nullable,
        .collation = collation,
    };
}

pub fn returningOutputColumnsAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    type_context: expr_type.RowExpressionTypeContext,
    returning: plan_mod.ReturningProjection,
) ![]const runtime_schema.RelationalColumn {
    if (!returning.hasProjection()) return &.{};
    const returning_all = returning.returnsAll();
    const field_count = if (returning_all) schema.relational_columns.len else returning.fields.len;
    const columns = try alloc.alloc(runtime_schema.RelationalColumn, field_count + returning.expressions.len);
    var initialized: usize = 0;
    errdefer {
        for (columns[0..initialized]) |column| {
            alloc.free(column.name);
            alloc.free(column.path);
            if (column.collation) |collation| alloc.free(collation);
        }
        alloc.free(columns);
    }

    if (returning_all) {
        for (schema.relational_columns) |source| {
            columns[initialized] = try cloneReturningOutputColumnAlloc(alloc, source.name, source);
            initialized += 1;
        }
    } else {
        for (returning.fields) |field| {
            const source = binder.relationalColumnForReturningField(schema, field) orelse return error.InvalidSqlCatalog;
            columns[initialized] = try cloneReturningOutputColumnAlloc(alloc, field, source);
            initialized += 1;
        }
    }

    for (returning.expressions) |projection| {
        const field_type = try type_context.rowExpressionOutputType(projection.expression);
        const array_item_type = try type_context.rowExpressionOutputArrayItemType(projection.expression);
        columns[initialized] = try projectedColumnAlloc(alloc, projection.output, field_type, array_item_type, true);
        initialized += 1;
    }
    return columns;
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

pub fn expressionOrderCount(order_by: []const db_mod.types.RelationalRowsQueryOrder) usize {
    var count: usize = 0;
    for (order_by) |order| {
        if (order.expression != null) count += 1;
    }
    return count;
}

pub fn joinOutputIsUnique(select: []const db_mod.types.RelationalRowsJoinProjection, field: []const u8) bool {
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
    errdefer {
        ddl_plan.clearDdlRelationalColumns(alloc, out[0..initialized]);
        alloc.free(out);
    }
    for (select) |projection| {
        if (outputColumnExists(out[0..initialized], projection.output)) return error.UnsupportedSqlShape;
        const column = binder.relationalColumnForField(schema, projection.field, null) orelse return error.InvalidSqlCatalog;
        out[initialized] = try projectedSourceColumnAlloc(alloc, projection.output, .{
            .name = column.name,
            .path = column.path,
            .field_type = column.field_type,
            .array_item_type = column.array_item_type,
            .nullable = true,
            .collation = column.collation,
        });
        initialized += 1;
    }
    return out;
}

fn orderForOwnedOutputFieldAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    field: []const u8,
) !db_mod.types.RelationalRowsQueryOrder {
    errdefer alloc.free(field);
    const column = binder.relationalColumnForField(schema, field, null);
    const collation = if (column) |resolved| if (resolved.collation) |value| try alloc.dupe(u8, value) else null else null;
    errdefer if (collation) |value| alloc.free(value);
    return .{
        .field = field,
        .collation = collation,
    };
}

pub fn selectOutputOrderByRefAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    select: plan_mod.SelectList,
    output: ast.SelectOutputRef,
) !db_mod.types.RelationalRowsQueryOrder {
    return switch (output.kind) {
        .field => try orderForOwnedOutputFieldAlloc(alloc, schema, try alloc.dupe(u8, select.fields[output.index])),
        .json_extract => .{ .expression = try expressionFromJsonExtractProjectionAlloc(alloc, select.json_extract[output.index]) },
        .array_length => .{ .expression = try expressionFromArrayLengthProjectionAlloc(alloc, select.array_length[output.index]) },
        .coalesce => .{ .expression = try expressionFromCoalesceProjectionAlloc(alloc, select.coalesce[output.index]) },
        .field_alias => try orderForOwnedOutputFieldAlloc(alloc, schema, try alloc.dupe(u8, select.field_aliases[output.index].field)),
        .expression => blk: {
            if (select.scalar_subqueries.len != 0) break :blk try orderForOwnedOutputFieldAlloc(alloc, schema, try alloc.dupe(u8, select.expressions[output.index].output));
            const expression = select.expressions[output.index].expression;
            if (expression.kind == .field) break :blk try orderForOwnedOutputFieldAlloc(alloc, schema, try alloc.dupe(u8, expression.field));
            break :blk .{ .expression = try cloneExpressionAlloc(alloc, expression) };
        },
        .scalar_subquery => blk: {
            const projection = select.scalar_subqueries[output.index];
            break :blk try orderForOwnedOutputFieldAlloc(alloc, schema, try alloc.dupe(u8, projection.output));
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
            return try orderForOwnedOutputFieldAlloc(alloc, schema, try alloc.dupe(u8, schema.relational_columns[index].name));
        }
        index -= schema.relational_columns.len;
    }
    if (index >= select.outputs.len) return error.UnsupportedSqlShape;
    return try selectOutputOrderByRefAlloc(alloc, schema, select, select.outputs[index]);
}

pub fn parseSelectOutputOrderByNameMaybeAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    schema: runtime_schema.TableSchema,
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
    return try selectOutputOrderByRefAlloc(alloc, schema, select, output);
}

pub fn testProjectionBuildsOwnedDefaultOutputs() !void {
    const alloc = std.testing.allocator;
    const tokens = [_]Token{};
    var pos: usize = 0;

    const field = try alloc.dupe(u8, "status");
    const projection = try buildExpressionProjectionFromOwnedExpressionAlloc(
        alloc,
        &tokens,
        &pos,
        .{ .kind = .field, .field = field },
        "status_text",
    );
    defer plan_mod.freeExpressionProjection(alloc, projection);
    try std.testing.expectEqual(@as(usize, 0), pos);
    try std.testing.expectEqualStrings("status_text", projection.output);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.field, projection.expression.kind);
    try std.testing.expectEqualStrings("status", projection.expression.field);

    const uuid_projection = try buildDefaultExpressionProjectionFromOwnedExpressionAlloc(
        alloc,
        &tokens,
        &pos,
        .{ .kind = .uuid_v4 },
    );
    defer plan_mod.freeExpressionProjection(alloc, uuid_projection);
    try std.testing.expectEqualStrings("uuid_v4", uuid_projection.output);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.uuid_v4, uuid_projection.expression.kind);
}

pub fn testProjectionParsesFieldHelpers() !void {
    const alloc = std.testing.allocator;
    const coalesce_schema = runtime_schema.TableSchema{
        .storage_mode = .relational,
        .relational_columns = &.{.{ .name = "status", .path = "status", .field_type = .keyword }},
    };

    const field_tokens = [_]Token{
        .{ .kind = .identifier, .text = "status" },
        .{ .kind = .comma, .text = "," },
        .{ .kind = .string, .text = "'fallback'" },
    };
    var field_pos: usize = 0;
    const field_operand = (try parseCoalesceFieldOperandOrNullOwnedAlloc(alloc, field_tokens[0..], &field_pos, coalesce_schema, &.{}, &.{}, false)) orelse return error.TestUnexpectedResult;
    defer switch (field_operand.kind) {
        .field => if (field_operand.field.len > 0) alloc.free(field_operand.field),
        .value => if (field_operand.value_json.len > 0) alloc.free(field_operand.value_json),
    };
    try std.testing.expectEqual(@as(usize, 1), field_pos);
    try std.testing.expectEqual(db_mod.types.RelationalRowsCoalesceOperandKind.field, field_operand.kind);
    try std.testing.expectEqualStrings("status", field_operand.field);

    const literal_tokens = [_]Token{.{ .kind = .string, .text = "'fallback'" }};
    var literal_pos: usize = 0;
    try std.testing.expect((try parseCoalesceFieldOperandOrNullOwnedAlloc(alloc, literal_tokens[0..], &literal_pos, coalesce_schema, &.{}, &.{}, false)) == null);
    try std.testing.expectEqual(@as(usize, 0), literal_pos);

    const call_tail_tokens = [_]Token{
        .{ .kind = .identifier, .text = "status" },
        .{ .kind = .lparen, .text = "(" },
    };
    var call_tail_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseCoalesceFieldOperandOrNullOwnedAlloc(alloc, call_tail_tokens[0..], &call_tail_pos, coalesce_schema, &.{}, &.{}, false));

    const array_schema = runtime_schema.TableSchema{
        .storage_mode = .relational,
        .relational_columns = &.{
            .{ .name = "tags", .path = "tags", .field_type = .array, .array_item_type = .keyword },
            .{ .name = "status", .path = "status", .field_type = .keyword },
        },
    };

    const raw_tokens = [_]Token{
        .{ .kind = .identifier, .text = "tags" },
        .{ .kind = .rparen, .text = ")" },
    };
    var raw_pos: usize = 0;
    const raw_field = try parseArrayFieldOwnedAlloc(alloc, raw_tokens[0..], &raw_pos, array_schema);
    defer alloc.free(raw_field);
    try std.testing.expectEqual(@as(usize, 1), raw_pos);
    try std.testing.expectEqualStrings("tags", raw_field);

    const qualified_tokens = [_]Token{
        .{ .kind = .identifier, .text = "events.tags" },
        .{ .kind = .rparen, .text = ")" },
    };
    var qualified_pos: usize = 0;
    const normalized_field = try parseRowExpressionArrayFieldOwnedAlloc(alloc, qualified_tokens[0..], &qualified_pos, array_schema, &.{"events"}, &.{}, false);
    defer alloc.free(normalized_field);
    try std.testing.expectEqual(@as(usize, 1), qualified_pos);
    try std.testing.expectEqualStrings("tags", normalized_field);

    const scalar_tokens = [_]Token{
        .{ .kind = .identifier, .text = "status" },
        .{ .kind = .rparen, .text = ")" },
    };
    var scalar_pos: usize = 0;
    try std.testing.expectError(error.InvalidSqlCatalog, parseArrayFieldOwnedAlloc(alloc, scalar_tokens[0..], &scalar_pos, array_schema));
}

pub fn testProjectionPeeksSimpleReturningFields() !void {
    const field = [_]Token{
        .{ .kind = .identifier, .text = "status" },
        .{ .kind = .comma, .text = "," },
    };
    try std.testing.expect(peekSimpleReturningField(field[0..], 0));

    const qualified = [_]Token{
        .{ .kind = .identifier, .text = "source.status" },
        .{ .kind = .identifier, .text = "as", .keyword = .as },
    };
    try std.testing.expect(peekSimpleReturningField(qualified[0..], 0));

    const arithmetic = [_]Token{
        .{ .kind = .identifier, .text = "total" },
        .{ .kind = .plus, .text = "+" },
    };
    try std.testing.expect(!peekSimpleReturningField(arithmetic[0..], 0));

    const boolean = [_]Token{
        .{ .kind = .identifier, .text = "enabled" },
        .{ .kind = .identifier, .text = "AND", .keyword = .@"and" },
    };
    try std.testing.expect(!peekSimpleReturningField(boolean[0..], 0));

    const call = [_]Token{
        .{ .kind = .identifier, .text = "lower", .keyword = .lower },
        .{ .kind = .lparen, .text = "(" },
    };
    try std.testing.expect(!peekSimpleReturningField(call[0..], 0));

    const cast_expr = [_]Token{
        .{ .kind = .identifier, .text = "cast", .keyword = .cast },
        .{ .kind = .lparen, .text = "(" },
    };
    try std.testing.expect(!peekSimpleReturningField(cast_expr[0..], 0));
}

pub fn testProjectionBuildsJoinOutputs() !void {
    const alloc = std.testing.allocator;
    const schema = runtime_schema.TableSchema{
        .storage_mode = .relational,
        .relational_columns = &.{.{ .name = "status", .path = "status", .field_type = .keyword, .collation = "status_collation" }},
    };
    const join_output_columns = try joinOutputColumnsAlloc(alloc, schema, &.{.{
        .output = "joined_status",
        .side = .left,
        .field = "status",
    }});
    defer ddl_plan.freeDdlRelationalColumns(alloc, join_output_columns);
    try std.testing.expectEqual(@as(usize, 1), join_output_columns.len);
    try std.testing.expectEqualStrings("joined_status", join_output_columns[0].name);
    try std.testing.expectEqual(runtime_schema.AntflyType.keyword, join_output_columns[0].field_type);
    try std.testing.expectEqualStrings("status_collation", join_output_columns[0].collation.?);

    const join_projections = [_]db_mod.types.RelationalRowsJoinProjection{
        .{ .output = "id", .field = "id", .side = .left },
        .{ .output = "status", .field = "status", .side = .right },
    };
    const duplicate_join_projections = [_]db_mod.types.RelationalRowsJoinProjection{
        .{ .output = "id", .field = "left_id", .side = .left },
        .{ .output = "id", .field = "right_id", .side = .right },
    };
    try std.testing.expect(joinOutputIsUnique(&join_projections, "status"));
    try std.testing.expect(!joinOutputIsUnique(&duplicate_join_projections, "id"));

    const ordinal_field = try joinOutputFieldByOrdinalAlloc(alloc, &join_projections, 2);
    defer alloc.free(ordinal_field);
    try std.testing.expectEqualStrings("status", ordinal_field);
    try std.testing.expectError(error.UnsupportedSqlShape, joinOutputFieldByOrdinalAlloc(alloc, &join_projections, 0));
}

fn generatedSetOperationRightProjectionSideEndedBefore(
    read: *const generated_parser.GeneratedSqlReadAst,
    pos: usize,
) !bool {
    const right_query = read.set_operation.right_query_tokens orelse return error.UnsupportedSqlShape;
    return right_query.end <= pos;
}

fn generatedProjectionItemAtStart(
    tokens: []const Token,
    pos: usize,
    generated_read_ast: ?*const generated_parser.GeneratedSqlReadAst,
) !?GeneratedExpressionItem {
    const read = generated_read_ast orelse return null;
    if (read.projection_items.items.len != read.projection_items.count or
        read.projection_items.expressions.len != read.projection_items.count or
        read.projection_items.alias_items.len != read.projection_items.count or
        read.projection_items.alias_name_items.len != read.projection_items.count)
    {
        return error.UnsupportedSqlShape;
    }
    for (read.projection_items.items, 0..) |item, index| {
        if (item.start == pos) {
            try expr_generated_validate.validateGeneratedExpressionPayloads(tokens, read.projection_items.expressions[index]);
            return .{
                .tokens = item,
                .expression = &read.projection_items.expressions[index],
                .alias_tokens = read.projection_items.alias_items[index],
                .alias_name_tokens = read.projection_items.alias_name_items[index],
            };
        }
    }
    if (read.set_operation_tokens != null) {
        if (read.set_operation.right_projection_items.items.len != read.set_operation.right_projection_items.count or
            read.set_operation.right_projection_items.expressions.len != read.set_operation.right_projection_items.count or
            read.set_operation.right_projection_items.alias_items.len != read.set_operation.right_projection_items.count or
            read.set_operation.right_projection_items.alias_name_items.len != read.set_operation.right_projection_items.count)
        {
            return error.UnsupportedSqlShape;
        }
        for (read.set_operation.right_projection_items.items, 0..) |item, index| {
            if (item.start == pos) {
                try expr_generated_validate.validateGeneratedExpressionPayloads(tokens, read.set_operation.right_projection_items.expressions[index]);
                return .{
                    .tokens = item,
                    .expression = &read.set_operation.right_projection_items.expressions[index],
                    .alias_tokens = read.set_operation.right_projection_items.alias_items[index],
                    .alias_name_tokens = read.set_operation.right_projection_items.alias_name_items[index],
                };
            }
        }
    }
    return null;
}

fn validateGeneratedExpressionItemEnd(generated_item: ?GeneratedExpressionItem, pos: usize) !void {
    if (generated_item) |item| {
        if (pos != item.tokens.end) return error.UnsupportedSqlShape;
    }
}

fn consumeGeneratedProjectionAliasAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    generated_item: ?GeneratedExpressionItem,
    item: *plan_mod.SelectItem,
) !void {
    const generated = generated_item orelse return;
    const alias = generated.alias_tokens orelse return;
    const alias_name = generated.alias_name_tokens orelse return error.UnsupportedSqlShape;
    if (alias.end > tokens.len) return error.UnsupportedSqlShape;
    if (alias.start < pos.*) {
        if (pos.* != alias.end) return error.UnsupportedSqlShape;
        return;
    }
    if (alias.start != pos.*) return error.UnsupportedSqlShape;
    if (alias_name.start < alias.start or alias_name.end != alias.end or alias_name.start >= alias_name.end) return error.UnsupportedSqlShape;
    if (tokens[alias.start].matchesKeywordTag(.as)) {
        if (alias_name.start != alias.start + 1) return error.UnsupportedSqlShape;
    } else if (alias_name.start != alias.start) {
        return error.UnsupportedSqlShape;
    }
    if (alias_name.end != alias_name.start + 1) return error.UnsupportedSqlShape;

    const output = try alloc.dupe(u8, tokens[alias_name.start].text);
    var output_owned = true;
    errdefer if (output_owned) alloc.free(output);

    switch (item.*) {
        .field => |field| {
            if (std.mem.eql(u8, output, field)) {
                alloc.free(output);
                output_owned = false;
            } else {
                item.* = .{ .expression = .{
                    .output = output,
                    .expression = .{
                        .kind = .field,
                        .field = field,
                    },
                } };
                output_owned = false;
            }
        },
        .json_extract => |*projection| {
            alloc.free(projection.output);
            projection.output = output;
            output_owned = false;
        },
        .array_length => |*projection| {
            alloc.free(projection.output);
            projection.output = output;
            output_owned = false;
        },
        .coalesce => |*projection| {
            alloc.free(projection.output);
            projection.output = output;
            output_owned = false;
        },
        .expression => |*projection| {
            alloc.free(projection.output);
            projection.output = output;
            output_owned = false;
        },
        .field_alias => |*projection| {
            alloc.free(projection.output);
            projection.output = output;
            output_owned = false;
        },
        .scalar_subquery => |*projection| {
            alloc.free(projection.output);
            projection.output = output;
            output_owned = false;
        },
    }
    pos.* = alias.end;
}

fn generatedSelectItemStartAllowsExpressionKind(
    start: SelectItemStart,
    kind: generated_parser.GeneratedSqlExpressionKind,
) bool {
    return switch (start) {
        .pipe_concat => kind == .string_concat,
        .unary_positive => kind == .unary_positive,
        .unary_negative => kind == .unary_negative,
        .boolean_not => kind == .logical_not,
        .extension_function,
        .routine_expression,
        .uuid_v4,
        .json_extract_path,
        .json_typeof,
        .json_array_length,
        .json_build_object,
        .convert_from,
        .to_jsonb,
        .array_length,
        .array_position,
        .array_element_transform,
        .array_to_string,
        .string_to_array,
        .coalesce,
        .case_fold,
        .replace,
        .regexp_replace,
        .regexp_substr,
        .regexp_match,
        .regexp_count,
        .regexp_instr,
        .translate,
        .concat,
        .nullif,
        .text_length,
        .ascii,
        .chr,
        .substring,
        .overlay,
        .split_part,
        .strpos,
        .left_right,
        .pad,
        .repeat,
        .reverse,
        .md5,
        .soundex,
        .starts_with,
        .ends_with,
        .date_trunc,
        .date_bin,
        .abs,
        .round,
        .trunc,
        .floor,
        .ceil,
        .sqrt,
        .sign,
        .mod,
        .power,
        .greatest_least,
        => kind == .function_call,
        .date_part => kind == .function_call or kind == .extract_expression,
        .now => kind == .function_call or kind == .current_timestamp,
        .current_date => kind == .current_date,
        .typed_datetime_literal => kind == .timestamp_literal,
        .array_constructor => kind == .array_constructor,
        .case => kind == .case_expression,
        .cast => kind == .cast,
        .parenthesized => kind == .grouped,
    };
}

fn validateGeneratedSelectItemStartFunctionName(
    tokens: []const Token,
    start: SelectItemStart,
    expression: generated_parser.GeneratedSqlExpressionAst,
) !void {
    switch (start) {
        .uuid_v4,
        .json_extract_path,
        .json_typeof,
        .json_array_length,
        .json_build_object,
        .convert_from,
        .to_jsonb,
        .array_length,
        .array_position,
        .array_to_string,
        .string_to_array,
        .coalesce,
        .replace,
        .regexp_replace,
        .regexp_substr,
        .regexp_match,
        .regexp_count,
        .regexp_instr,
        .translate,
        .nullif,
        .ascii,
        .chr,
        .overlay,
        .repeat,
        .reverse,
        .md5,
        .soundex,
        .starts_with,
        .ends_with,
        .date_trunc,
        .date_bin,
        .abs,
        .round,
        .mod,
        .power,
        => {},
        else => return,
    }

    const token = try expr_generated_validate.generatedExpressionFunctionNameToken(tokens, expression);
    const valid = switch (start) {
        .uuid_v4 => expr_token.sqlTokenIsUuidV4Function(token),
        .json_extract_path => expr_token.sqlTokenIsJsonExtractPathFunction(token),
        .json_typeof => expr_token.sqlTokenIsJsonTypeofFunction(token),
        .json_array_length => expr_token.sqlTokenIsJsonArrayLengthFunction(token),
        .json_build_object => expr_token.sqlTokenIsJsonBuildObjectFunction(token),
        .convert_from => token.matchesKeywordTag(.convert_from),
        .to_jsonb => token.matchesKeywordTag(.to_jsonb),
        .array_length => expr_token.sqlTokenIsArrayLengthFunction(token),
        .array_position => expr_token.sqlTokenIsArrayPositionFunction(token),
        .array_to_string => expr_token.sqlTokenIsArrayToStringFunction(token),
        .string_to_array => token.matchesKeywordTag(.string_to_array),
        .coalesce => token.matchesKeywordTag(.coalesce),
        .replace => token.matchesKeywordTag(.replace),
        .regexp_replace => token.matchesKeywordTag(.regexp_replace),
        .regexp_substr => expr_token.sqlTokenIsRegexpSubstrFunction(token),
        .regexp_match => expr_token.sqlTokenIsRegexpMatchFunction(token),
        .regexp_count => expr_token.sqlTokenIsRegexpCountFunction(token),
        .regexp_instr => expr_token.sqlTokenIsRegexpInstrFunction(token),
        .translate => expr_token.sqlTokenIsTranslateFunction(token),
        .nullif => token.matchesKeywordTag(.nullif),
        .ascii => expr_token.sqlTokenIsAsciiFunction(token),
        .chr => expr_token.sqlTokenIsChrFunction(token),
        .overlay => expr_token.sqlTokenIsOverlayFunction(token),
        .repeat => expr_token.sqlTokenIsRepeatFunction(token),
        .reverse => expr_token.sqlTokenIsReverseFunction(token),
        .md5 => expr_token.sqlTokenIsMd5Function(token),
        .soundex => std.ascii.eqlIgnoreCase(token.text, "soundex"),
        .starts_with => expr_token.sqlTokenIsStartsWithFunction(token),
        .ends_with => expr_token.sqlTokenIsEndsWithFunction(token),
        .date_trunc => expr_token.sqlTokenIsDateTruncFunction(token),
        .date_bin => expr_token.sqlTokenIsDateBinFunction(token),
        .abs => token.matchesKeywordTag(.abs),
        .round => token.matchesKeywordTag(.round),
        .mod => token.matchesKeywordTag(.mod),
        .power => token.matchesKeywordTag(.power),
        else => unreachable,
    };
    if (!valid) return error.UnsupportedSqlShape;
}

fn validateGeneratedSelectItemStartForExpression(
    tokens: []const Token,
    start: SelectItemStart,
    generated_expression: ?*const generated_parser.GeneratedSqlExpressionAst,
) !void {
    const expression = generated_expression orelse return;
    try expr_generated_validate.validateGeneratedExpressionPayloads(tokens, expression.*);
    if (!generatedSelectItemStartAllowsExpressionKind(start, expression.kind)) return error.UnsupportedSqlShape;
    try validateGeneratedSelectItemStartFunctionName(tokens, start, expression.*);
}

pub const ReturningProjectionParserOptions = struct {
    params: []const value_mod.SqlValue = &.{},
    context_hooks: expr_row_parse.SelectParserContextHooks,
    select_item_options: SelectItemParserOptions,
    generated_returning_items: ?*const generated_parser.GeneratedSqlListAst = null,
};

pub const JoinedMutationReturningProjectionParserOptions = struct {
    params: []const value_mod.SqlValue = &.{},
    select_context_hooks: expr_row_parse.SelectParserContextHooks,
    joined_context_hooks: expr_row_parse.JoinedExpressionParserContextHooks,
    select_item_options: SelectItemParserOptions,
    generated_returning_items: ?*const generated_parser.GeneratedSqlListAst = null,
};

pub const SelectFieldItemParserOptions = struct {
    type_context: expr_type.RowExpressionTypeContext,
    arithmetic_hooks: expr_row_parse.ArithmeticExpressionParserHooks,
    boolean_hooks: expr_row_parse.BooleanExpressionParserHooks,
    generated_expression_ast: ?*const generated_parser.GeneratedSqlExpressionAst = null,
    require_exact_generated_expression: bool = false,
};

pub const SelectItemParserOptions = struct {
    expression: ExpressionProjectionParserOptions,
    json_value_expression: JsonValueExpressionProjectionParserOptions,
    select_field: SelectFieldItemParserOptions,
    extension_function: expr_row_parse.ExtensionFunctionRowExpressionParserOptions,
    routine_expression: expr_row_parse.RoutineExpressionRowExpressionParserOptions,
    generated_expression_ast: ?*const generated_parser.GeneratedSqlExpressionAst = null,
    require_exact_generated_expression: bool = false,
};

pub const SelectListParserOptions = struct {
    params: []const value_mod.SqlValue = &.{},
    output_schema: runtime_schema.TableSchema,
    select_item_schema: runtime_schema.TableSchema,
    function_bindings: expr_row_parse.SqlFunctionBindings = .{},
    type_context: expr_type.RowExpressionTypeContext,
    field_expression_qualifiers: []const []const u8 = &.{},
    returning_expression_qualifiers: []const []const u8 = &.{},
    defer_row_expression_field_validation: bool = false,
    field_source: db_mod.types.RelationalRowsExpressionFieldSource = .row,
    select_item_options: SelectItemParserOptions,
    generated_read_ast: ?*const generated_parser.GeneratedSqlReadAst = null,
    allow_set_operation_right_projection_side: bool = false,
};

pub fn parseSelectListAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    options: SelectListParserOptions,
) !plan_mod.SelectList {
    var select_all = false;
    var fields = std.ArrayListUnmanaged([]const u8).empty;
    errdefer {
        for (fields.items) |field| alloc.free(field);
        fields.deinit(alloc);
    }
    var json_extract = std.ArrayListUnmanaged(db_mod.types.RelationalRowsJsonExtractProjection).empty;
    errdefer {
        for (json_extract.items) |projection| {
            alloc.free(projection.output);
            alloc.free(projection.field);
            alloc.free(projection.path);
        }
        json_extract.deinit(alloc);
    }
    var array_length = std.ArrayListUnmanaged(db_mod.types.RelationalRowsArrayLengthProjection).empty;
    errdefer {
        for (array_length.items) |projection| {
            alloc.free(projection.output);
            alloc.free(projection.field);
        }
        array_length.deinit(alloc);
    }
    var coalesce = std.ArrayListUnmanaged(db_mod.types.RelationalRowsCoalesceProjection).empty;
    errdefer {
        for (coalesce.items) |projection| plan_mod.freeCoalesceProjection(alloc, projection);
        coalesce.deinit(alloc);
    }
    var expressions = std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionProjection).empty;
    errdefer {
        for (expressions.items) |projection| freeExpressionProjection(alloc, projection);
        expressions.deinit(alloc);
    }
    var scalar_subqueries = std.ArrayListUnmanaged(db_mod.types.RelationalRowsScalarSubqueryProjection).empty;
    errdefer {
        for (scalar_subqueries.items) |projection| plan_mod.freeScalarSubqueryProjection(alloc, projection);
        scalar_subqueries.deinit(alloc);
    }
    var field_aliases = std.ArrayListUnmanaged(db_mod.types.RelationalRowsFieldAliasProjection).empty;
    errdefer {
        for (field_aliases.items) |projection| {
            alloc.free(projection.output);
            alloc.free(projection.field);
        }
        field_aliases.deinit(alloc);
    }
    var outputs = std.ArrayListUnmanaged(ast.SelectOutputRef).empty;
    errdefer outputs.deinit(alloc);

    while (true) {
        if (parser.matchToken(tokens, pos, .star) != null) {
            if (select_all or
                fields.items.len != 0 or
                json_extract.items.len != 0 or
                array_length.items.len != 0 or
                coalesce.items.len != 0 or
                field_aliases.items.len != 0 or
                expressions.items.len != 0 or
                scalar_subqueries.items.len != 0) return error.UnsupportedSqlShape;
            select_all = true;
            if (parser.matchToken(tokens, pos, .comma) == null) break;
            continue;
        }

        const item_start = pos.*;
        const generated_item = try generatedProjectionItemAtStart(tokens, item_start, options.generated_read_ast);
        if (generated_item == null and options.generated_read_ast != null) {
            if (!options.allow_set_operation_right_projection_side or !try generatedSetOperationRightProjectionSideEndedBefore(options.generated_read_ast.?, item_start)) return error.UnsupportedSqlShape;
        }
        const generated_expression = if (generated_item) |item| item.expression else null;
        var select_item_options = options.select_item_options;
        select_item_options.generated_expression_ast = generated_expression;
        select_item_options.require_exact_generated_expression = generated_item != null;
        var item = try parseSelectItemAlloc(
            alloc,
            tokens,
            pos,
            options.params,
            options.select_item_schema,
            options.function_bindings,
            options.field_expression_qualifiers,
            options.returning_expression_qualifiers,
            options.defer_row_expression_field_validation,
            options.field_source,
            options.type_context,
            select_item_options,
        );
        try consumeGeneratedProjectionAliasAlloc(alloc, tokens, pos, generated_item, &item);
        var item_transferred = false;
        errdefer if (!item_transferred) plan_mod.freeSelectItem(alloc, item);
        switch (item) {
            .field => |field| {
                item_transferred = true;
                var field_transferred = false;
                errdefer if (!field_transferred) alloc.free(field);
                if (select_all) {
                    var output: []const u8 = try alloc.dupe(u8, field);
                    var output_transferred = false;
                    errdefer if (!output_transferred) alloc.free(output);
                    try disambiguateSelectProjectionOutputIfNeededAlloc(
                        alloc,
                        options.output_schema,
                        select_all,
                        fields.items,
                        json_extract.items,
                        array_length.items,
                        coalesce.items,
                        field_aliases.items,
                        expressions.items,
                        scalar_subqueries.items,
                        &output,
                    );
                    try outputs.append(alloc, .{ .kind = .field_alias, .index = field_aliases.items.len });
                    try field_aliases.append(alloc, .{ .field = field, .output = output });
                    field_transferred = true;
                    output_transferred = true;
                } else {
                    try outputs.append(alloc, .{ .kind = .field, .index = fields.items.len });
                    try fields.append(alloc, field);
                    field_transferred = true;
                }
            },
            .json_extract => |projection_value| {
                item_transferred = true;
                var projection = projection_value;
                var projection_transferred = false;
                errdefer if (!projection_transferred) {
                    alloc.free(projection.output);
                    alloc.free(projection.field);
                    alloc.free(projection.path);
                };
                try disambiguateSelectProjectionOutputIfNeededAlloc(
                    alloc,
                    options.output_schema,
                    select_all,
                    fields.items,
                    json_extract.items,
                    array_length.items,
                    coalesce.items,
                    field_aliases.items,
                    expressions.items,
                    scalar_subqueries.items,
                    &projection.output,
                );
                try outputs.append(alloc, .{ .kind = .json_extract, .index = json_extract.items.len });
                try json_extract.append(alloc, projection);
                projection_transferred = true;
            },
            .array_length => |projection_value| {
                item_transferred = true;
                var projection = projection_value;
                var projection_transferred = false;
                errdefer if (!projection_transferred) {
                    alloc.free(projection.output);
                    alloc.free(projection.field);
                };
                try disambiguateSelectProjectionOutputIfNeededAlloc(
                    alloc,
                    options.output_schema,
                    select_all,
                    fields.items,
                    json_extract.items,
                    array_length.items,
                    coalesce.items,
                    field_aliases.items,
                    expressions.items,
                    scalar_subqueries.items,
                    &projection.output,
                );
                try outputs.append(alloc, .{ .kind = .array_length, .index = array_length.items.len });
                try array_length.append(alloc, projection);
                projection_transferred = true;
            },
            .coalesce => |projection_value| {
                item_transferred = true;
                var projection = projection_value;
                var projection_transferred = false;
                errdefer if (!projection_transferred) plan_mod.freeCoalesceProjection(alloc, projection);
                try disambiguateSelectProjectionOutputIfNeededAlloc(
                    alloc,
                    options.output_schema,
                    select_all,
                    fields.items,
                    json_extract.items,
                    array_length.items,
                    coalesce.items,
                    field_aliases.items,
                    expressions.items,
                    scalar_subqueries.items,
                    &projection.output,
                );
                const expression_projection = try plan_mod.expressionProjectionFromCoalesceAlloc(alloc, projection);
                var expression_projection_transferred = false;
                errdefer if (!expression_projection_transferred) freeExpressionProjection(alloc, expression_projection);
                try outputs.append(alloc, .{ .kind = .coalesce, .index = coalesce.items.len });
                try expressions.append(alloc, expression_projection);
                expression_projection_transferred = true;
                try coalesce.append(alloc, projection);
                projection_transferred = true;
            },
            .expression => |projection_value| {
                item_transferred = true;
                var projection = projection_value;
                var projection_transferred = false;
                errdefer if (!projection_transferred) freeExpressionProjection(alloc, projection);
                try disambiguateSelectProjectionOutputIfNeededAlloc(
                    alloc,
                    options.output_schema,
                    select_all,
                    fields.items,
                    json_extract.items,
                    array_length.items,
                    coalesce.items,
                    field_aliases.items,
                    expressions.items,
                    scalar_subqueries.items,
                    &projection.output,
                );
                try outputs.append(alloc, .{ .kind = .expression, .index = expressions.items.len });
                try expressions.append(alloc, projection);
                projection_transferred = true;
            },
            .field_alias => |projection_value| {
                item_transferred = true;
                var projection = projection_value;
                var projection_transferred = false;
                errdefer if (!projection_transferred) {
                    alloc.free(projection.output);
                    alloc.free(projection.field);
                };
                try disambiguateSelectProjectionOutputIfNeededAlloc(
                    alloc,
                    options.output_schema,
                    select_all,
                    fields.items,
                    json_extract.items,
                    array_length.items,
                    coalesce.items,
                    field_aliases.items,
                    expressions.items,
                    scalar_subqueries.items,
                    &projection.output,
                );
                try outputs.append(alloc, .{ .kind = .field_alias, .index = field_aliases.items.len });
                try field_aliases.append(alloc, projection);
                projection_transferred = true;
            },
            .scalar_subquery => |projection_value| {
                item_transferred = true;
                var projection = projection_value;
                var projection_transferred = false;
                errdefer if (!projection_transferred) plan_mod.freeScalarSubqueryProjection(alloc, projection);
                try disambiguateSelectProjectionOutputIfNeededAlloc(
                    alloc,
                    options.output_schema,
                    select_all,
                    fields.items,
                    json_extract.items,
                    array_length.items,
                    coalesce.items,
                    field_aliases.items,
                    expressions.items,
                    scalar_subqueries.items,
                    &projection.output,
                );
                try outputs.append(alloc, .{ .kind = .scalar_subquery, .index = scalar_subqueries.items.len });
                try scalar_subqueries.append(alloc, projection);
                projection_transferred = true;
            },
        }
        item_transferred = true;
        try expr_generated_validate.validateGeneratedExpressionItemEnd(generated_item, pos.*);
        if (parser.matchToken(tokens, pos, .comma) == null) break;
    }

    try validateSelectListOutputs(
        options.output_schema,
        select_all,
        fields.items,
        json_extract.items,
        array_length.items,
        coalesce.items,
        field_aliases.items,
        expressions.items,
        scalar_subqueries.items,
    );

    const owned_fields = try fields.toOwnedSlice(alloc);
    var fields_transferred = false;
    errdefer if (!fields_transferred) strings.freeStringSlice(alloc, owned_fields);
    const owned_json_extract = try json_extract.toOwnedSlice(alloc);
    var json_extract_transferred = false;
    errdefer if (!json_extract_transferred) plan_mod.freeJsonExtract(alloc, owned_json_extract);
    const owned_array_length = try array_length.toOwnedSlice(alloc);
    var array_length_transferred = false;
    errdefer if (!array_length_transferred) plan_mod.freeArrayLengthProjections(alloc, owned_array_length);
    const owned_coalesce = try coalesce.toOwnedSlice(alloc);
    var coalesce_transferred = false;
    errdefer if (!coalesce_transferred) plan_mod.freeCoalesceProjections(alloc, owned_coalesce);
    const owned_field_aliases = try field_aliases.toOwnedSlice(alloc);
    var field_aliases_transferred = false;
    errdefer if (!field_aliases_transferred) plan_mod.freeFieldAliasProjections(alloc, owned_field_aliases);
    const owned_expressions = try expressions.toOwnedSlice(alloc);
    var expressions_transferred = false;
    errdefer if (!expressions_transferred) freeExpressionProjections(alloc, owned_expressions);
    const owned_scalar_subqueries = try scalar_subqueries.toOwnedSlice(alloc);
    var scalar_subqueries_transferred = false;
    errdefer if (!scalar_subqueries_transferred) plan_mod.freeScalarSubqueryProjections(alloc, owned_scalar_subqueries);
    const owned_outputs = try outputs.toOwnedSlice(alloc);

    fields_transferred = true;
    json_extract_transferred = true;
    array_length_transferred = true;
    coalesce_transferred = true;
    field_aliases_transferred = true;
    expressions_transferred = true;
    scalar_subqueries_transferred = true;
    return .{
        .fields = owned_fields,
        .json_extract = owned_json_extract,
        .array_length = owned_array_length,
        .coalesce = owned_coalesce,
        .field_aliases = owned_field_aliases,
        .expressions = owned_expressions,
        .scalar_subqueries = owned_scalar_subqueries,
        .outputs = owned_outputs,
        .select_all = select_all,
    };
}

pub fn parseSelectItemAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    params: []const value_mod.SqlValue,
    schema: runtime_schema.TableSchema,
    function_bindings: expr_row_parse.SqlFunctionBindings,
    field_expression_qualifiers: []const []const u8,
    returning_expression_qualifiers: []const []const u8,
    defer_row_expression_field_validation: bool,
    field_source: db_mod.types.RelationalRowsExpressionFieldSource,
    type_context: expr_type.RowExpressionTypeContext,
    base_options: SelectItemParserOptions,
) !plan_mod.SelectItem {
    var options = base_options;
    options.expression.generated_expression_ast = options.generated_expression_ast;
    options.expression.require_exact_generated_expression = options.require_exact_generated_expression;

    if (selectItemStartWithFunctionBindingsAt(tokens, pos.*, function_bindings)) |start| {
        try validateGeneratedSelectItemStartForExpression(tokens, start, options.generated_expression_ast);
        switch (start) {
            .pipe_concat => return .{ .expression = try parseTextExpressionProjectionAlloc(alloc, tokens, pos, type_context, options.expression) },
            .unary_positive => return .{ .expression = try parseUnaryPositiveExpressionProjectionAlloc(alloc, tokens, pos, type_context, options.expression) },
            .unary_negative => return .{ .expression = try parseGenericExpressionProjectionAlloc(alloc, tokens, pos, type_context, options.expression) },
            .boolean_not => return .{ .expression = try parseBooleanExpressionProjectionAlloc(alloc, tokens, pos, type_context, options.expression) },
            .extension_function => return .{ .expression = try parseExtensionFunctionExpressionProjectionAlloc(alloc, tokens, pos, function_bindings.extension_functions, options.extension_function) },
            .routine_expression => return .{ .expression = try parseRoutineExpressionProjectionAlloc(alloc, tokens, pos, function_bindings.routine_expressions, options.routine_expression) },
            .uuid_v4 => return .{ .expression = try parseUuidV4ExpressionProjectionAlloc(alloc, tokens, pos) },
            .now => return .{ .expression = try parseNowExpressionProjectionAlloc(alloc, tokens, pos) },
            .current_date => return .{ .expression = try parseCurrentDateExpressionProjectionAlloc(alloc, tokens, pos) },
            .typed_datetime_literal => return .{ .expression = try parseTypedDatetimeLiteralExpressionProjectionAlloc(alloc, tokens, pos) },
            .array_constructor => return .{ .expression = try parseArrayConstructorExpressionProjectionAlloc(alloc, tokens, pos, params, options.expression) },
            .json_extract_path => return .{ .expression = try parseJsonExtractPathExpressionProjectionAlloc(alloc, tokens, pos, options.expression) },
            .json_typeof => return .{ .expression = try parseFixedOutputExpressionProjectionAlloc(alloc, tokens, pos, "json_typeof", options.expression) },
            .json_array_length => return .{ .expression = try parseFixedOutputExpressionProjectionAlloc(alloc, tokens, pos, "json_array_length", options.expression) },
            .json_build_object => return .{ .expression = try parseFixedOutputExpressionProjectionAlloc(alloc, tokens, pos, "jsonb_build_object", options.expression) },
            .convert_from => return .{ .expression = try parseJsonValueExpressionProjectionAlloc(alloc, tokens, pos, "convert_from", options.json_value_expression) },
            .to_jsonb => return .{ .expression = try parseFixedOutputExpressionProjectionAlloc(alloc, tokens, pos, "to_jsonb", options.expression) },
            .array_length => return .{ .expression = try parseArrayLengthExpressionProjectionAlloc(alloc, tokens, pos, params, schema, field_expression_qualifiers, returning_expression_qualifiers, defer_row_expression_field_validation, field_source) },
            .array_position => return .{ .expression = try parseOpOutputExpressionProjectionAlloc(alloc, tokens, pos, options.expression) },
            .array_element_transform => return .{ .expression = try parseOpOutputExpressionProjectionAlloc(alloc, tokens, pos, options.expression) },
            .array_to_string => return .{ .expression = try parseFixedOutputExpressionProjectionAlloc(alloc, tokens, pos, "array_to_string", options.expression) },
            .string_to_array => return .{ .expression = try parseFixedOutputExpressionProjectionAlloc(alloc, tokens, pos, "string_to_array", options.expression) },
            .coalesce => return .{ .expression = try parseFixedOutputExpressionProjectionAlloc(alloc, tokens, pos, "coalesce", options.expression) },
            .case_fold => return .{ .expression = try parseDefaultOutputExpressionProjectionAlloc(alloc, tokens, pos, options.expression) },
            .replace => return .{ .expression = try parseFixedOutputExpressionProjectionAlloc(alloc, tokens, pos, "replace", options.expression) },
            .regexp_replace => return .{ .expression = try parseFixedOutputExpressionProjectionAlloc(alloc, tokens, pos, "regexp_replace", options.expression) },
            .regexp_substr => return .{ .expression = try parseFixedOutputExpressionProjectionAlloc(alloc, tokens, pos, "regexp_substr", options.expression) },
            .regexp_match => return .{ .expression = try parseRegexpMatchExpressionProjectionAlloc(alloc, tokens, pos, options.expression) },
            .regexp_count => return .{ .expression = try parseFixedOutputExpressionProjectionAlloc(alloc, tokens, pos, "regexp_count", options.expression) },
            .regexp_instr => return .{ .expression = try parseFixedOutputExpressionProjectionAlloc(alloc, tokens, pos, "regexp_instr", options.expression) },
            .translate => return .{ .expression = try parseFixedOutputExpressionProjectionAlloc(alloc, tokens, pos, "translate", options.expression) },
            .concat => return .{ .expression = try parseOpOutputExpressionProjectionAlloc(alloc, tokens, pos, options.expression) },
            .nullif => return .{ .expression = try parseFixedOutputExpressionProjectionAlloc(alloc, tokens, pos, "nullif", options.expression) },
            .text_length => return .{ .expression = try parseFixedOutputExpressionProjectionAlloc(alloc, tokens, pos, "length", options.expression) },
            .ascii => return .{ .expression = try parseOpOutputExpressionProjectionAlloc(alloc, tokens, pos, options.expression) },
            .chr => return .{ .expression = try parseOpOutputExpressionProjectionAlloc(alloc, tokens, pos, options.expression) },
            .substring => return .{ .expression = try parseOpOutputExpressionProjectionAlloc(alloc, tokens, pos, options.expression) },
            .overlay => return .{ .expression = try parseOpOutputExpressionProjectionAlloc(alloc, tokens, pos, options.expression) },
            .split_part => return .{ .expression = try parseOpOutputExpressionProjectionAlloc(alloc, tokens, pos, options.expression) },
            .strpos => return .{ .expression = try parseOpOutputExpressionProjectionAlloc(alloc, tokens, pos, options.expression) },
            .left_right => return .{ .expression = try parseOpOutputExpressionProjectionAlloc(alloc, tokens, pos, options.expression) },
            .pad => return .{ .expression = try parseOpOutputExpressionProjectionAlloc(alloc, tokens, pos, options.expression) },
            .repeat => return .{ .expression = try parseOpOutputExpressionProjectionAlloc(alloc, tokens, pos, options.expression) },
            .reverse => return .{ .expression = try parseOpOutputExpressionProjectionAlloc(alloc, tokens, pos, options.expression) },
            .md5 => return .{ .expression = try parseOpOutputExpressionProjectionAlloc(alloc, tokens, pos, options.expression) },
            .soundex => return .{ .expression = try parseOpOutputExpressionProjectionAlloc(alloc, tokens, pos, options.expression) },
            .starts_with => return .{ .expression = try parseOpOutputExpressionProjectionAlloc(alloc, tokens, pos, options.expression) },
            .ends_with => return .{ .expression = try parseOpOutputExpressionProjectionAlloc(alloc, tokens, pos, options.expression) },
            .date_trunc => return .{ .expression = try parseOpOutputExpressionProjectionAlloc(alloc, tokens, pos, options.expression) },
            .date_bin => return .{ .expression = try parseOpOutputExpressionProjectionAlloc(alloc, tokens, pos, options.expression) },
            .date_part => return .{ .expression = try parseOpOutputExpressionProjectionAlloc(alloc, tokens, pos, options.expression) },
            .abs => return .{ .expression = try parseFixedOutputExpressionProjectionAlloc(alloc, tokens, pos, "abs", options.expression) },
            .round => return .{ .expression = try parseFixedOutputExpressionProjectionAlloc(alloc, tokens, pos, "round", options.expression) },
            .trunc, .floor, .ceil, .sqrt, .sign => return .{ .expression = try parseOpOutputExpressionProjectionAlloc(alloc, tokens, pos, options.expression) },
            .mod => return .{ .expression = try parseFixedOutputExpressionProjectionAlloc(alloc, tokens, pos, "mod", options.expression) },
            .power => return .{ .expression = try parseFixedOutputExpressionProjectionAlloc(alloc, tokens, pos, "power", options.expression) },
            .greatest_least => return .{ .expression = try parseOpOutputExpressionProjectionAlloc(alloc, tokens, pos, options.expression) },
            .case => return .{ .expression = try parseFixedOutputExpressionProjectionAlloc(alloc, tokens, pos, "case", options.expression) },
            .cast => return .{ .expression = try parseFixedOutputExpressionProjectionAlloc(alloc, tokens, pos, "cast", options.expression) },
            .parenthesized => {
                if (peekParenthesizedNullTestProjection(tokens, pos.*)) {
                    return .{ .expression = try parseParenthesizedNullTestExpressionProjectionAlloc(alloc, tokens, pos, schema, field_expression_qualifiers, returning_expression_qualifiers, defer_row_expression_field_validation, field_source, options.generated_expression_ast) };
                }
                return .{ .expression = try parseParenthesizedExpressionProjectionAlloc(alloc, tokens, pos, options.expression) };
            },
        }
    }

    var select_field_options = options.select_field;
    select_field_options.generated_expression_ast = options.generated_expression_ast;
    select_field_options.require_exact_generated_expression = options.require_exact_generated_expression;
    return try parseSelectFieldItemAlloc(
        alloc,
        tokens,
        pos,
        schema,
        params,
        field_expression_qualifiers,
        returning_expression_qualifiers,
        defer_row_expression_field_validation,
        field_source,
        select_field_options,
    );
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
    options: SelectFieldItemParserOptions,
) !plan_mod.SelectItem {
    const expression_start = pos.*;
    const parsed_field = try expr_generated.parseRowExpressionFieldOwnedAlloc(alloc, tokens, pos, schema, field_expression_qualifiers, returning_expression_qualifiers, defer_row_expression_field_validation);
    defer alloc.free(parsed_field);
    const field = try binder.normalizeRowExpressionFieldAlloc(alloc, schema, parsed_field, field_expression_qualifiers, returning_expression_qualifiers, defer_row_expression_field_validation);
    var field_owned = true;
    errdefer if (field_owned) alloc.free(field);
    if (parser.peekKind(tokens, pos.*, .lparen)) return error.UnsupportedSqlShape;
    if (expr_operator.matchJsonExtractOperator(tokens, pos)) |operator| {
        const as_text = expr_operator.tokenKindIsJsonExtractTextOperator(operator);
        if (binder.relationalColumnForField(schema, field, .json) == null) return error.InvalidSqlCatalog;
        const path = try value_mod.parseJsonExtractOperatorPathOwnedAlloc(alloc, tokens, pos, params, operator);
        var path_owned = true;
        errdefer if (path_owned) alloc.free(path);
        const output = try grammar.parseProjectionOutputOwnedAlloc(alloc, tokens, pos, path);
        var output_owned = true;
        errdefer if (output_owned) alloc.free(output);
        const expression = try expr_build.buildJsonExtractExpressionAlloc(
            alloc,
            .{ .kind = .field, .field = field, .field_source = field_source },
            path,
            as_text,
        );
        errdefer freeExpression(alloc, expression);
        field_owned = false;
        path_owned = false;
        output_owned = false;
        return .{ .expression = expr_build.buildExpressionProjection(output, expression) };
    }
    if (parser.matchToken(tokens, pos, .question) != null) {
        if (binder.relationalColumnForField(schema, field, .json) == null) return error.InvalidSqlCatalog;
        const path = try value_mod.parseJsonPathOwnedAlloc(alloc, tokens, pos, params);
        var path_owned = true;
        errdefer if (path_owned) alloc.free(path);
        const output = try grammar.parseProjectionOutputOwnedAlloc(alloc, tokens, pos, path);
        var output_owned = true;
        errdefer if (output_owned) alloc.free(output);
        const expression = try expr_build.buildJsonPathExistsExpressionAlloc(
            alloc,
            .{ .kind = .field, .field = field, .field_source = field_source },
            path,
        );
        errdefer freeExpression(alloc, expression);
        field_owned = false;
        path_owned = false;
        output_owned = false;
        return .{ .expression = expr_build.buildExpressionProjection(output, expression) };
    }
    if (parser.matchToken(tokens, pos, .question_any) != null or parser.matchToken(tokens, pos, .question_all) != null) {
        const match_all = tokens[pos.* - 1].kind == .question_all;
        if (binder.relationalColumnForField(schema, field, .json) == null) return error.InvalidSqlCatalog;
        const values_json = try value_mod.parseJsonArrayValueAlloc(alloc, tokens, pos, params);
        defer alloc.free(values_json);
        const expression = try expr_build.buildJsonKeySetExistsExpressionAlloc(alloc, field, field_source, match_all, values_json);
        errdefer freeExpression(alloc, expression);
        const output = try grammar.parseProjectionOutputOwnedAlloc(alloc, tokens, pos, if (match_all) "json_keys_all" else "json_keys_any");
        var output_owned = true;
        errdefer if (output_owned) alloc.free(output);
        output_owned = false;
        alloc.free(field);
        field_owned = false;
        return .{ .expression = expr_build.buildExpressionProjection(output, expression) };
    }
    const column = binder.relationalColumnForField(schema, field, null) orelse return error.InvalidSqlCatalog;
    if (expr_operator.peekArithmeticOperator(tokens, pos.*)) |_| {
        if (column.field_type != .numeric and column.field_type != .datetime) return error.InvalidSqlCatalog;
        field_owned = false;
        return .{ .expression = try parseArithmeticExpressionProjectionFromFieldAlloc(alloc, tokens, pos, expression_start, field, field_source, options.type_context, options.arithmetic_hooks, options.generated_expression_ast, options.require_exact_generated_expression) };
    }
    if (expr_operator.peekBooleanOperator(tokens, pos.*)) |_| {
        if (column.field_type != .boolean) return error.InvalidSqlCatalog;
        field_owned = false;
        return .{ .expression = try parseBooleanExpressionProjectionFromFieldAlloc(alloc, tokens, pos, expression_start, field, field_source, options.type_context, options.boolean_hooks, options.generated_expression_ast, options.require_exact_generated_expression) };
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

test "sql expr projection builds owned default outputs" {
    try testProjectionBuildsOwnedDefaultOutputs();
}

test "sql expr projection parses field helpers" {
    try testProjectionParsesFieldHelpers();
}

test "sql expr projection peeks simple returning fields" {
    try testProjectionPeeksSimpleReturningFields();
}

test "sql expr projection builds join outputs" {
    try testProjectionBuildsJoinOutputs();
}

test "sql expr projection classifies select item starts" {
    const concat_tokens = [_]Token{
        .{ .kind = .identifier, .text = "concat" },
        .{ .kind = .lparen, .text = "(" },
        .{ .kind = .identifier, .text = "first_name" },
        .{ .kind = .comma, .text = "," },
        .{ .kind = .identifier, .text = "last_name" },
        .{ .kind = .rparen, .text = ")" },
    };
    try std.testing.expectEqual(SelectItemStart.concat, selectItemStartAt(&concat_tokens, 0).?);

    const typed_date_tokens = [_]Token{
        .{ .kind = .identifier, .text = "DATE" },
        .{ .kind = .string, .text = "'2026-01-01'" },
    };
    try std.testing.expectEqual(SelectItemStart.typed_datetime_literal, selectItemStartAt(&typed_date_tokens, 0).?);

    const binding_tokens = [_]Token{
        .{ .kind = .identifier, .text = "safe_slug" },
        .{ .kind = .lparen, .text = "(" },
        .{ .kind = .identifier, .text = "status" },
        .{ .kind = .rparen, .text = ")" },
    };
    try std.testing.expectEqual(SelectItemStart.extension_function, selectItemStartWithFunctionBindingsAt(&binding_tokens, 0, .{
        .extension_functions = &.{.{
            .sql_name = "safe_slug",
            .native_expression_kind = .lower,
            .arity = 1,
        }},
    }).?);
    try std.testing.expectEqual(SelectItemStart.routine_expression, selectItemStartWithFunctionBindingsAt(&binding_tokens, 0, .{
        .routine_expressions = &.{.{
            .sql_name = "safe_slug",
            .arity = 1,
            .expression = .{ .kind = .field, .field = "status" },
        }},
    }).?);
}
