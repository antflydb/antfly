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
const expr_build = @import("build.zig");
const expr_generated = @import("generated.zig");
const expr_generated_validate = @import("generated_validate.zig");
const expr_row_parse = @import("row_parse.zig");
const expr_type = @import("type.zig");
const generated_parser = @import("../generated_parser.zig");
const grammar = @import("../grammar.zig");
const parser = @import("../parser.zig");
const plan_mod = @import("../plan.zig");
const runtime_schema = @import("../../storage/schema.zig");
const token_mod = @import("../token.zig");
const value_mod = @import("../value.zig");

pub const Token = token_mod.Token;

const cloneExpressionAlloc = plan_mod.cloneExpressionAlloc;
const freeExpression = plan_mod.freeExpression;

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
    tokens: []const Token,
    start: usize,
    end: usize,
    expression: db_mod.types.RelationalRowsExpression,
    options: ExpressionProjectionParserOptions,
) !void {
    if (options.require_exact_generated_expression) {
        try expr_generated_validate.validateGeneratedRowExpressionIdentityStrict(tokens, start, end, expression, options.generated_expression_ast);
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
    try validateGeneratedExpressionProjectionIdentity(tokens, start, pos.*, expression, options);
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
    try validateGeneratedExpressionProjectionIdentity(tokens, start, pos.*, expression, options);
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
    try validateGeneratedExpressionProjectionIdentity(tokens, start, pos.*, expression, options);
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
    try validateGeneratedExpressionProjectionIdentity(tokens, start, pos.*, expression, options);
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
    try validateGeneratedExpressionProjectionIdentity(tokens, start, pos.*, expression, options);
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
    try validateGeneratedExpressionProjectionIdentity(tokens, start, pos.*, expression, options);
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
    try validateGeneratedExpressionProjectionIdentity(tokens, start, pos.*, expression, options);
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
    try validateGeneratedExpressionProjectionIdentity(tokens, start, pos.*, expression, options);
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

fn selectProjectionOutputCollision(
    schema: runtime_schema.TableSchema,
    select_all: bool,
    fields: []const []const u8,
    json_extract: []const db_mod.types.RelationalRowsJsonExtractProjection,
    array_length: []const db_mod.types.RelationalRowsArrayLengthProjection,
    coalesce: []const db_mod.types.RelationalRowsCoalesceProjection,
    field_aliases: []const db_mod.types.RelationalRowsFieldAliasProjection,
    expressions: []const db_mod.types.RelationalRowsExpressionProjection,
    output: []const u8,
) bool {
    if (select_all and binder.relationalColumnForField(schema, output, null) != null) return true;
    return selectListOutputCount(fields, json_extract, array_length, coalesce, field_aliases, expressions, output) > 0;
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
    output: []const u8,
) !?[]const u8 {
    if (!select_all) return null;
    if (!selectProjectionOutputCollision(schema, select_all, fields, json_extract, array_length, coalesce, field_aliases, expressions, output)) return null;

    var suffix: usize = 2;
    while (true) : (suffix += 1) {
        const candidate = try std.fmt.allocPrint(alloc, "{s}_{d}", .{ output, suffix });
        if (!selectProjectionOutputCollision(schema, select_all, fields, json_extract, array_length, coalesce, field_aliases, expressions, candidate)) return candidate;
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
) !runtime_schema.RelationalColumn {
    if (expression.kind == .field) {
        if (binder.relationalColumnForField(type_context.schemaForRowExpressionField(expression), expression.field, null)) |source| {
            return try projectedSourceColumnAlloc(alloc, output_name, source);
        }
        if (!type_context.defer_row_expression_field_validation) return error.InvalidSqlCatalog;
    }
    const field_type = try type_context.rowExpressionOutputType(expression);
    const array_item_type = try type_context.rowExpressionOutputArrayItemType(expression);
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
            break :blk try projectedExpressionColumnAlloc(alloc, type_context, projection.output, projection.expression);
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
            const expression = select.expressions[output.index].expression;
            if (expression.kind == .field) break :blk try orderForOwnedOutputFieldAlloc(alloc, schema, try alloc.dupe(u8, expression.field));
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

test "sql expr projection builds owned default outputs" {
    try testProjectionBuildsOwnedDefaultOutputs();
}
