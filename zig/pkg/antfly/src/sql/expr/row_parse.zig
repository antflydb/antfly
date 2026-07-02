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
const expr_build = @import("build.zig");
const expr_generated = @import("generated.zig");
const expr_generated_validate = @import("generated_validate.zig");
const expr_operator = @import("operator.zig");
const expr_parse = @import("parse.zig");
const expr_token = @import("token.zig");
const expr_type = @import("type.zig");
const generated_parser = @import("../generated_parser.zig");
const parser = @import("../parser.zig");
const plan_mod = @import("../plan.zig");
const runtime_schema = @import("../../storage/schema.zig");
const token_mod = @import("../token.zig");
const value_mod = @import("../value.zig");

pub const Token = token_mod.Token;

const freeExpression = plan_mod.freeExpression;
const freeExpressionCondition = plan_mod.freeExpressionCondition;

fn wrapBooleanNotExpressionAlloc(
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

fn parseIdentifierOwnedAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) ![]const u8 {
    const token = parser.matchToken(tokens, pos, .identifier) orelse return error.UnsupportedSqlShape;
    return try alloc.dupe(u8, token.text);
}

fn parseRowExpressionArrayFieldOwnedAlloc(
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

fn parseArrayLengthRowExpressionAlloc(
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

pub const SelectParserContext = struct {
    schema: runtime_schema.TableSchema,
    joined_source_schema: ?runtime_schema.TableSchema = null,
    field_expression_qualifiers: []const []const u8 = &.{},
    returning_expression_qualifiers: []const []const u8 = &.{},
    defer_row_expression_field_validation: bool = false,
};

pub const SelectParserContextHooks = struct {
    ptr: *anyopaque,
    get_context: *const fn (*anyopaque) SelectParserContext,
    set_context: *const fn (*anyopaque, SelectParserContext) void,
    row_expression_type_context: *const fn (*anyopaque) expr_type.RowExpressionTypeContext,
};

pub const JoinedExpressionParserContext = struct {
    joined_target_expression_qualifiers: []const []const u8 = &.{},
    joined_source_expression_qualifiers: []const []const u8 = &.{},
    defer_row_expression_field_validation: bool = false,
};

pub const JoinedExpressionParserContextHooks = struct {
    ptr: *anyopaque,
    get_context: *const fn (*anyopaque) JoinedExpressionParserContext,
    set_context: *const fn (*anyopaque, JoinedExpressionParserContext) void,
    row_expression_type_context: *const fn (*anyopaque) expr_type.RowExpressionTypeContext,
};

pub const RowExpressionParserHooks = struct {
    ptr: *anyopaque,
    parse_operand: *const fn (*anyopaque) anyerror!db_mod.types.RelationalRowsExpression,
};

pub fn parseRowExpressionFromContextAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    context_hooks: SelectParserContextHooks,
    row_expression_hooks: RowExpressionParserHooks,
    arithmetic_hooks: ArithmeticExpressionParserHooks,
    variadic_hooks: VariadicRowExpressionParserHooks,
) !db_mod.types.RelationalRowsExpression {
    return try parseRowExpressionAlloc(
        alloc,
        tokens,
        pos,
        context_hooks.row_expression_type_context(context_hooks.ptr),
        row_expression_hooks,
        arithmetic_hooks,
        variadic_hooks,
    );
}

pub fn setJoinedExpressionContextForTables(
    context_hooks: JoinedExpressionParserContextHooks,
    target_qualifiers: []const []const u8,
    source_qualifiers: []const []const u8,
) JoinedExpressionParserContext {
    const previous_context = context_hooks.get_context(context_hooks.ptr);
    var context = previous_context;
    context.joined_target_expression_qualifiers = target_qualifiers;
    context.joined_source_expression_qualifiers = source_qualifiers;
    context_hooks.set_context(context_hooks.ptr, context);
    return previous_context;
}

pub const JoinedRowExpressionParserOptions = struct {
    context_hooks: JoinedExpressionParserContextHooks,
    row_expression_hooks: RowExpressionParserHooks,
    arithmetic_hooks: ArithmeticExpressionParserHooks,
    variadic_hooks: VariadicRowExpressionParserHooks,
    generated_expression_ast: ?*const generated_parser.GeneratedSqlExpressionAst = null,
    require_exact_generated_expression: bool = false,
};

pub fn parseJoinedRowExpressionWithTableQualifiersAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    target_table: plan_mod.TableAlias,
    source_table: plan_mod.TableAlias,
    options: JoinedRowExpressionParserOptions,
) !db_mod.types.RelationalRowsExpression {
    const target_qualifiers = [_][]const u8{ target_table.name, target_table.alias };
    const source_qualifiers = [_][]const u8{ source_table.name, source_table.alias };
    const previous_context = setJoinedExpressionContextForTables(options.context_hooks, target_qualifiers[0..], source_qualifiers[0..]);
    defer options.context_hooks.set_context(options.context_hooks.ptr, previous_context);

    const start = pos.*;
    const expression = try parseRowExpressionAlloc(
        alloc,
        tokens,
        pos,
        options.context_hooks.row_expression_type_context(options.context_hooks.ptr),
        options.row_expression_hooks,
        options.arithmetic_hooks,
        options.variadic_hooks,
    );
    errdefer freeExpression(alloc, expression);
    if (options.require_exact_generated_expression) {
        try expr_generated_validate.validateGeneratedRowExpressionIdentityStrict(tokens, start, pos.*, expression, options.generated_expression_ast);
    } else {
        try expr_generated_validate.validateGeneratedRowExpressionIdentity(tokens, start, pos.*, expression, options.generated_expression_ast);
    }
    return expression;
}

pub fn parseJoinedRowExpressionWithQualifiersAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    target_qualifiers: []const []const u8,
    source_qualifiers: []const []const u8,
    options: JoinedRowExpressionParserOptions,
) !db_mod.types.RelationalRowsExpression {
    const previous_context = setJoinedExpressionContextForTables(options.context_hooks, target_qualifiers, source_qualifiers);
    defer options.context_hooks.set_context(options.context_hooks.ptr, previous_context);

    const start = pos.*;
    const expression = try parseRowExpressionAlloc(
        alloc,
        tokens,
        pos,
        options.context_hooks.row_expression_type_context(options.context_hooks.ptr),
        options.row_expression_hooks,
        options.arithmetic_hooks,
        options.variadic_hooks,
    );
    errdefer freeExpression(alloc, expression);
    if (options.require_exact_generated_expression) {
        try expr_generated_validate.validateGeneratedRowExpressionIdentityStrict(tokens, start, pos.*, expression, options.generated_expression_ast);
    } else {
        try expr_generated_validate.validateGeneratedRowExpressionIdentity(tokens, start, pos.*, expression, options.generated_expression_ast);
    }
    return expression;
}

pub const JoinedBooleanRowExpressionParserOptions = struct {
    context_hooks: JoinedExpressionParserContextHooks,
    boolean_hooks: BooleanRowExpressionParserHooks,
    generated_expression_ast: ?*const generated_parser.GeneratedSqlExpressionAst = null,
    require_exact_generated_expression: bool = false,
};

pub fn parseJoinedBooleanRowExpressionWithQualifiersAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    target_qualifiers: []const []const u8,
    source_qualifiers: []const []const u8,
    options: JoinedBooleanRowExpressionParserOptions,
) !db_mod.types.RelationalRowsExpression {
    const previous_context = setJoinedExpressionContextForTables(options.context_hooks, target_qualifiers, source_qualifiers);
    defer options.context_hooks.set_context(options.context_hooks.ptr, previous_context);

    const start = pos.*;
    const expression = try parseBooleanRowExpressionAlloc(
        alloc,
        tokens,
        pos,
        options.context_hooks.row_expression_type_context(options.context_hooks.ptr),
        options.boolean_hooks,
    );
    errdefer freeExpression(alloc, expression);
    if (options.require_exact_generated_expression) {
        try expr_generated_validate.validateGeneratedRowExpressionIdentityStrict(tokens, start, pos.*, expression, options.generated_expression_ast);
    } else {
        try expr_generated_validate.validateGeneratedRowExpressionIdentity(tokens, start, pos.*, expression, options.generated_expression_ast);
    }
    return expression;
}

pub fn setDeferredRowExpressionFieldValidation(context_hooks: SelectParserContextHooks) SelectParserContext {
    const previous_context = context_hooks.get_context(context_hooks.ptr);
    var context = previous_context;
    context.defer_row_expression_field_validation = true;
    context_hooks.set_context(context_hooks.ptr, context);
    return previous_context;
}

pub const DeferredRowExpressionParserOptions = struct {
    function_bindings: SqlFunctionBindings = .{},
    require_routine_expression_binding: bool = false,
    context_hooks: SelectParserContextHooks,
    row_expression_hooks: RowExpressionParserHooks,
    arithmetic_hooks: ArithmeticExpressionParserHooks,
    variadic_hooks: VariadicRowExpressionParserHooks,
};

pub fn parseRowExpressionWithDeferredFieldValidationAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    options: DeferredRowExpressionParserOptions,
) !db_mod.types.RelationalRowsExpression {
    if (options.require_routine_expression_binding and options.function_bindings.routine_expressions.len == 0) return error.UnsupportedSqlShape;
    const previous_context = setDeferredRowExpressionFieldValidation(options.context_hooks);
    defer options.context_hooks.set_context(options.context_hooks.ptr, previous_context);

    return try parseRowExpressionAlloc(
        alloc,
        tokens,
        pos,
        options.context_hooks.row_expression_type_context(options.context_hooks.ptr),
        options.row_expression_hooks,
        options.arithmetic_hooks,
        options.variadic_hooks,
    );
}

pub const DeferredBooleanRowExpressionParserOptions = struct {
    function_bindings: SqlFunctionBindings = .{},
    require_routine_expression_binding: bool = false,
    context_hooks: SelectParserContextHooks,
    boolean_hooks: BooleanRowExpressionParserHooks,
};

pub fn parseBooleanRowExpressionWithDeferredFieldValidationAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    options: DeferredBooleanRowExpressionParserOptions,
) !db_mod.types.RelationalRowsExpression {
    if (options.require_routine_expression_binding and options.function_bindings.routine_expressions.len == 0) return error.UnsupportedSqlShape;
    const previous_context = setDeferredRowExpressionFieldValidation(options.context_hooks);
    defer options.context_hooks.set_context(options.context_hooks.ptr, previous_context);

    return try parseBooleanRowExpressionAlloc(
        alloc,
        tokens,
        pos,
        options.context_hooks.row_expression_type_context(options.context_hooks.ptr),
        options.boolean_hooks,
    );
}

pub const ExtensionFunctionRowExpressionParserOptions = struct {
    type_context: expr_type.RowExpressionTypeContext,
    row_expression_hooks: RowExpressionParserHooks,
    arithmetic_hooks: ArithmeticExpressionParserHooks,
    variadic_hooks: VariadicRowExpressionParserHooks,
};

pub const RoutineExpressionRowExpressionParserOptions = struct {
    type_context: expr_type.RowExpressionTypeContext,
    boolean_hooks: BooleanRowExpressionParserHooks,
};

pub const ParenthesizedRowExpressionParserOptions = struct {
    type_context: expr_type.RowExpressionTypeContext,
    boolean_hooks: BooleanRowExpressionParserHooks,
};

pub const CastRowExpressionParserOptions = struct {
    type_context: expr_type.RowExpressionTypeContext,
    row_expression_hooks: RowExpressionParserHooks,
    arithmetic_hooks: ArithmeticExpressionParserHooks,
    variadic_hooks: VariadicRowExpressionParserHooks,
};

pub const CoalesceRowExpressionParserOptions = struct {
    type_context: expr_type.RowExpressionTypeContext,
    row_expression_hooks: RowExpressionParserHooks,
    arithmetic_hooks: ArithmeticExpressionParserHooks,
    variadic_hooks: VariadicRowExpressionParserHooks,
};

pub const RowExpressionInputDomain = enum {
    text,
    numeric,
    json,
    any,
};

pub const FixedUnaryRowExpressionParserOptions = struct {
    row_expression_hooks: RowExpressionParserHooks,
    arithmetic_hooks: ArithmeticExpressionParserHooks,
    variadic_hooks: VariadicRowExpressionParserHooks,
};

pub const FixedBinaryRowExpressionParserOptions = struct {
    row_expression_hooks: RowExpressionParserHooks,
    arithmetic_hooks: ArithmeticExpressionParserHooks,
    variadic_hooks: VariadicRowExpressionParserHooks,
};

pub const VariadicRowExpressionParserHooks = struct {
    ptr: *anyopaque,
    parse_expression: *const fn (*anyopaque) anyerror!db_mod.types.RelationalRowsExpression,
    parse_operand: *const fn (*anyopaque) anyerror!db_mod.types.RelationalRowsExpression,
};

pub const JsonBuildObjectRowExpressionParserOptions = struct {
    row_expression_hooks: RowExpressionParserHooks,
    arithmetic_hooks: ArithmeticExpressionParserHooks,
    variadic_hooks: VariadicRowExpressionParserHooks,
};

pub const ArithmeticExpressionParserHooks = struct {
    ptr: *anyopaque,
    parse_operand: *const fn (*anyopaque) anyerror!db_mod.types.RelationalRowsExpression,
    parenthesized: ParenthesizedRowExpressionParserOptions,
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

pub const BooleanNotRowExpressionParserOptions = struct {
    operand_hooks: RowExpressionParserHooks,
    parenthesized: ParenthesizedRowExpressionParserOptions,
};

pub const CaseExpressionParserHooks = struct {
    ptr: *anyopaque,
    parse_expression: *const fn (*anyopaque) anyerror!db_mod.types.RelationalRowsExpression,
    parse_operand: *const fn (*anyopaque) anyerror!db_mod.types.RelationalRowsExpression,
};

pub const CaseFoldRowExpressionParserOptions = struct {
    row_expression_hooks: RowExpressionParserHooks,
    arithmetic_hooks: ArithmeticExpressionParserHooks,
    variadic_hooks: VariadicRowExpressionParserHooks,
};

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
    options: ExtensionFunctionRowExpressionParserOptions,
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
            const operand = try parseRowExpressionAlloc(
                alloc,
                tokens,
                pos,
                options.type_context,
                options.row_expression_hooks,
                options.arithmetic_hooks,
                options.variadic_hooks,
            );
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
    return try expr_build.buildFunctionExpressionFromOperandListAlloc(alloc, kind, &operands);
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
    options: RoutineExpressionRowExpressionParserOptions,
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
            const operand = try parseBooleanRowExpressionAlloc(
                alloc,
                tokens,
                pos,
                options.type_context,
                options.boolean_hooks,
            );
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
    options: ParenthesizedRowExpressionParserOptions,
) !db_mod.types.RelationalRowsExpression {
    try parser.expectToken(tokens, pos, .lparen);
    const expression = try parseBooleanRowExpressionAlloc(alloc, tokens, pos, options.type_context, options.boolean_hooks);
    var expression_transferred = false;
    errdefer if (!expression_transferred) freeExpression(alloc, expression);
    try parser.expectToken(tokens, pos, .rparen);
    expression_transferred = true;
    return expression;
}

pub fn parseCastRowExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    options: CastRowExpressionParserOptions,
) !db_mod.types.RelationalRowsExpression {
    try expr_token.parseCastExpressionCallStart(tokens, pos);
    const operand = try parseRowExpressionAlloc(alloc, tokens, pos, options.type_context, options.row_expression_hooks, options.arithmetic_hooks, options.variadic_hooks);
    var operand_transferred = false;
    errdefer if (!operand_transferred) freeExpression(alloc, operand);
    try expr_token.parseCastExpressionAs(tokens, pos);
    const cast_type = try expr_operator.parseExpressionCastType(tokens, pos);
    try parser.expectToken(tokens, pos, .rparen);
    const expression = try expr_build.buildCastExpressionAlloc(alloc, operand, cast_type);
    operand_transferred = true;
    return expression;
}

pub fn parseCoalesceRowExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    type_context: expr_type.RowExpressionTypeContext,
    options: CoalesceRowExpressionParserOptions,
) !db_mod.types.RelationalRowsExpression {
    try expr_token.parseCoalesceFunctionCallStart(tokens, pos);

    var operands = std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpression).empty;
    errdefer {
        for (operands.items) |operand| freeExpression(alloc, operand);
        operands.deinit(alloc);
    }
    while (true) {
        const operand = try parseRowExpressionAlloc(alloc, tokens, pos, options.type_context, options.row_expression_hooks, options.arithmetic_hooks, options.variadic_hooks);
        var operand_transferred = false;
        errdefer if (!operand_transferred) freeExpression(alloc, operand);
        try operands.append(alloc, operand);
        operand_transferred = true;
        if (parser.matchToken(tokens, pos, .comma) == null) break;
    }
    if (operands.items.len == 0) return error.UnsupportedSqlShape;
    try parser.expectToken(tokens, pos, .rparen);

    const expression = try expr_build.buildFunctionExpressionFromOperandListAlloc(alloc, .coalesce, &operands);
    errdefer freeExpression(alloc, expression);
    try type_context.validateExpressionOperandDomains(expression);
    return expression;
}

pub fn parseTextLengthRowExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    type_context: expr_type.RowExpressionTypeContext,
    options: FixedUnaryRowExpressionParserOptions,
) !db_mod.types.RelationalRowsExpression {
    const kind = try expr_token.parseTextLengthFunctionCallStart(tokens, pos);
    return try parseUnaryRowExpressionCallRestAlloc(alloc, tokens, pos, kind, type_context, .text, options);
}

pub fn parseFixedUnaryRowExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    kind: db_mod.types.RelationalRowsExpressionKind,
    type_context: expr_type.RowExpressionTypeContext,
    input_domain: RowExpressionInputDomain,
    options: FixedUnaryRowExpressionParserOptions,
) !db_mod.types.RelationalRowsExpression {
    try expr_token.parseFixedUnaryFunctionCallStart(tokens, pos, kind);
    return try parseUnaryRowExpressionCallRestAlloc(alloc, tokens, pos, kind, type_context, input_domain, options);
}

pub fn parseJsonUnaryRowExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    kind: db_mod.types.RelationalRowsExpressionKind,
    type_context: expr_type.RowExpressionTypeContext,
    options: FixedUnaryRowExpressionParserOptions,
) !db_mod.types.RelationalRowsExpression {
    switch (kind) {
        .json_array_length => try expr_token.parseJsonArrayLengthFunctionCallStart(tokens, pos),
        .json_typeof => try expr_token.parseJsonTypeofFunctionCallStart(tokens, pos),
        else => return error.UnsupportedSqlShape,
    }
    return try parseUnaryRowExpressionCallRestAlloc(alloc, tokens, pos, kind, type_context, .json, options);
}

pub fn parseJsonExtractPathRowExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    params: []const value_mod.SqlValue,
    type_context: expr_type.RowExpressionTypeContext,
    options: FixedUnaryRowExpressionParserOptions,
) !db_mod.types.RelationalRowsExpression {
    const function_token = expr_token.matchFunctionKeywordToken(tokens, pos, expr_token.sqlTokenIsJsonExtractPathFunction) orelse return error.UnsupportedSqlShape;
    const as_text = expr_token.sqlJsonExtractPathTokenAsText(function_token);
    try parser.expectToken(tokens, pos, .lparen);
    const operand = try parseRowExpressionAlloc(alloc, tokens, pos, type_context, options.row_expression_hooks, options.arithmetic_hooks, options.variadic_hooks);
    var operand_transferred = false;
    errdefer if (!operand_transferred) freeExpression(alloc, operand);
    try type_context.validateJsonRowExpression(operand);
    const path = try value_mod.parseJsonExtractPathSegmentsAlloc(alloc, tokens, pos, params);
    var path_transferred = false;
    errdefer if (!path_transferred) alloc.free(path);
    try parser.expectToken(tokens, pos, .rparen);

    const expression = try expr_build.buildJsonExtractExpressionAlloc(alloc, operand, path, as_text);
    operand_transferred = true;
    path_transferred = true;
    return expression;
}

fn parseUnaryRowExpressionCallRestAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    kind: db_mod.types.RelationalRowsExpressionKind,
    type_context: expr_type.RowExpressionTypeContext,
    input_domain: RowExpressionInputDomain,
    options: FixedUnaryRowExpressionParserOptions,
) !db_mod.types.RelationalRowsExpression {
    const operand = try parseRowExpressionAlloc(alloc, tokens, pos, type_context, options.row_expression_hooks, options.arithmetic_hooks, options.variadic_hooks);
    var operand_transferred = false;
    errdefer if (!operand_transferred) freeExpression(alloc, operand);
    switch (input_domain) {
        .text => try type_context.validateTextRowExpression(operand),
        .numeric => try type_context.validateNumericRowExpression(operand),
        .json => try type_context.validateJsonRowExpression(operand),
        .any => _ = try type_context.rowExpressionOutputType(operand),
    }
    try parser.expectToken(tokens, pos, .rparen);

    const expression = try expr_build.buildUnaryFunctionExpressionAlloc(alloc, kind, operand);
    operand_transferred = true;
    return expression;
}

pub fn parseJsonBuildObjectRowExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    type_context: expr_type.RowExpressionTypeContext,
    options: JsonBuildObjectRowExpressionParserOptions,
) !db_mod.types.RelationalRowsExpression {
    try expr_token.parseJsonBuildObjectFunctionCallStart(tokens, pos);
    var operands = std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpression).empty;
    errdefer {
        for (operands.items) |operand| freeExpression(alloc, operand);
        operands.deinit(alloc);
    }
    if (parser.matchToken(tokens, pos, .rparen) == null) {
        while (true) {
            const key = try parseRowExpressionAlloc(alloc, tokens, pos, type_context, options.row_expression_hooks, options.arithmetic_hooks, options.variadic_hooks);
            var key_transferred = false;
            errdefer if (!key_transferred) freeExpression(alloc, key);
            try type_context.validateTextRowExpression(key);
            try operands.append(alloc, key);
            key_transferred = true;

            try parser.expectToken(tokens, pos, .comma);
            const value = try parseRowExpressionAlloc(alloc, tokens, pos, type_context, options.row_expression_hooks, options.arithmetic_hooks, options.variadic_hooks);
            var value_transferred = false;
            errdefer if (!value_transferred) freeExpression(alloc, value);
            _ = try type_context.rowExpressionOutputType(value);
            try operands.append(alloc, value);
            value_transferred = true;

            if (parser.matchToken(tokens, pos, .comma) == null) break;
        }
        try parser.expectToken(tokens, pos, .rparen);
    }
    const expression = try expr_build.buildFunctionExpressionFromOperandListAlloc(alloc, .json_build_object, &operands);
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
    type_context: expr_type.RowExpressionTypeContext,
    hooks: FixedBinaryRowExpressionParserOptions,
) !db_mod.types.RelationalRowsExpression {
    const kind = try expr_token.parseArrayPositionFunctionCallStart(tokens, pos);
    const array_expression = try parseRowExpressionAlloc(alloc, tokens, pos, type_context, hooks.row_expression_hooks, hooks.arithmetic_hooks, hooks.variadic_hooks);
    var array_transferred = false;
    errdefer if (!array_transferred) freeExpression(alloc, array_expression);
    const array_type = try type_context.rowExpressionOutputType(array_expression);
    if (array_type != .array) return error.UnsupportedSqlShape;
    try parser.expectToken(tokens, pos, .comma);
    const needle_expression = try parseRowExpressionAlloc(alloc, tokens, pos, type_context, hooks.row_expression_hooks, hooks.arithmetic_hooks, hooks.variadic_hooks);
    var needle_transferred = false;
    errdefer if (!needle_transferred) freeExpression(alloc, needle_expression);
    try parser.expectToken(tokens, pos, .rparen);

    const expression = try expr_build.buildBinaryFunctionExpressionAlloc(alloc, kind, array_expression, needle_expression);
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
    type_context: expr_type.RowExpressionTypeContext,
    hooks: FixedBinaryRowExpressionParserOptions,
) !db_mod.types.RelationalRowsExpression {
    try expr_token.parseStringToArrayFunctionCallStart(tokens, pos);
    const text_expression = try parseRowExpressionAlloc(alloc, tokens, pos, type_context, hooks.row_expression_hooks, hooks.arithmetic_hooks, hooks.variadic_hooks);
    var text_transferred = false;
    errdefer if (!text_transferred) freeExpression(alloc, text_expression);
    try parser.expectToken(tokens, pos, .comma);
    const delimiter_expression = try parseRowExpressionAlloc(alloc, tokens, pos, type_context, hooks.row_expression_hooks, hooks.arithmetic_hooks, hooks.variadic_hooks);
    var delimiter_transferred = false;
    errdefer if (!delimiter_transferred) freeExpression(alloc, delimiter_expression);
    try parser.expectToken(tokens, pos, .rparen);

    const expression = try expr_build.buildBinaryFunctionExpressionAlloc(alloc, .string_to_array, text_expression, delimiter_expression);
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
    type_context: expr_type.RowExpressionTypeContext,
    hooks: VariadicRowExpressionParserHooks,
) !db_mod.types.RelationalRowsExpression {
    try expr_token.parseArrayToStringFunctionCallStart(tokens, pos);
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

    const expression = try expr_build.buildFunctionExpressionFromOperandListAlloc(alloc, .array_to_string, &operands);
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
    type_context: expr_type.RowExpressionTypeContext,
    hooks: VariadicRowExpressionParserHooks,
) !db_mod.types.RelationalRowsExpression {
    const kind = try expr_token.parseArrayElementTransformFunctionCallStart(tokens, pos);
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
        const out = try expr_build.buildBinaryFunctionExpressionAlloc(alloc, kind, second_expression, first_expression);
        second_transferred = true;
        first_transferred = true;
        break :blk out;
    } else if (kind == .array_replace) blk: {
        const out = try expr_build.buildTernaryFunctionExpressionAlloc(alloc, kind, first_expression, second_expression, third_expression);
        first_transferred = true;
        second_transferred = true;
        third_transferred = true;
        break :blk out;
    } else blk: {
        const out = try expr_build.buildBinaryFunctionExpressionAlloc(alloc, kind, first_expression, second_expression);
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
    type_context: expr_type.RowExpressionTypeContext,
    hooks: FixedBinaryRowExpressionParserOptions,
) !db_mod.types.RelationalRowsExpression {
    try expr_token.parseFixedBinaryFunctionCallStart(tokens, pos, kind);
    const lhs = try parseRowExpressionAlloc(alloc, tokens, pos, type_context, hooks.row_expression_hooks, hooks.arithmetic_hooks, hooks.variadic_hooks);
    var lhs_transferred = false;
    errdefer if (!lhs_transferred) freeExpression(alloc, lhs);
    try type_context.validateNumericRowExpression(lhs);
    try parser.expectToken(tokens, pos, .comma);
    const rhs = try parseRowExpressionAlloc(alloc, tokens, pos, type_context, hooks.row_expression_hooks, hooks.arithmetic_hooks, hooks.variadic_hooks);
    var rhs_transferred = false;
    errdefer if (!rhs_transferred) freeExpression(alloc, rhs);
    try type_context.validateNumericRowExpression(rhs);
    try parser.expectToken(tokens, pos, .rparen);
    const expression = try expr_build.buildBinaryFunctionExpressionAlloc(alloc, kind, lhs, rhs);
    lhs_transferred = true;
    rhs_transferred = true;
    return expression;
}

pub fn parseTextBinaryRowExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    kind: db_mod.types.RelationalRowsExpressionKind,
    type_context: expr_type.RowExpressionTypeContext,
    hooks: FixedBinaryRowExpressionParserOptions,
) !db_mod.types.RelationalRowsExpression {
    switch (kind) {
        .starts_with => try expr_token.parseFunctionCallStartTokenIf(tokens, pos, expr_token.sqlTokenIsStartsWithFunction),
        .ends_with => try expr_token.parseFunctionCallStartTokenIf(tokens, pos, expr_token.sqlTokenIsEndsWithFunction),
        .regexp_substr => try expr_token.parseFunctionCallStartTokenIf(tokens, pos, expr_token.sqlTokenIsRegexpSubstrFunction),
        .regexp_count => try expr_token.parseFunctionCallStartTokenIf(tokens, pos, expr_token.sqlTokenIsRegexpCountFunction),
        .regexp_instr => try expr_token.parseFunctionCallStartTokenIf(tokens, pos, expr_token.sqlTokenIsRegexpInstrFunction),
        else => return error.UnsupportedSqlShape,
    }

    const lhs = try parseRowExpressionAlloc(alloc, tokens, pos, type_context, hooks.row_expression_hooks, hooks.arithmetic_hooks, hooks.variadic_hooks);
    var lhs_transferred = false;
    errdefer if (!lhs_transferred) freeExpression(alloc, lhs);
    try type_context.validateTextRowExpression(lhs);
    try parser.expectToken(tokens, pos, .comma);
    const rhs = try parseRowExpressionAlloc(alloc, tokens, pos, type_context, hooks.row_expression_hooks, hooks.arithmetic_hooks, hooks.variadic_hooks);
    var rhs_transferred = false;
    errdefer if (!rhs_transferred) freeExpression(alloc, rhs);
    try type_context.validateTextRowExpression(rhs);
    try parser.expectToken(tokens, pos, .rparen);

    const expression = try expr_build.buildBinaryFunctionExpressionAlloc(alloc, kind, lhs, rhs);
    lhs_transferred = true;
    rhs_transferred = true;
    return expression;
}

pub fn parseTextTernaryRowExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    kind: db_mod.types.RelationalRowsExpressionKind,
    type_context: expr_type.RowExpressionTypeContext,
    hooks: VariadicRowExpressionParserHooks,
) !db_mod.types.RelationalRowsExpression {
    switch (kind) {
        .replace => try expr_token.parseReplaceFunctionCallStart(tokens, pos),
        .translate => try expr_token.parseFunctionCallStartTokenIf(tokens, pos, expr_token.sqlTokenIsTranslateFunction),
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

    const expression = try expr_build.buildTernaryFunctionExpressionAlloc(alloc, kind, first, second, third);
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
    type_context: expr_type.RowExpressionTypeContext,
    hooks: VariadicRowExpressionParserHooks,
) !db_mod.types.RelationalRowsExpression {
    switch (kind) {
        .regexp_replace => try expr_token.parseRegexpReplaceFunctionCallStart(tokens, pos),
        .regexp_match => try expr_token.parseFunctionCallStartTokenIf(tokens, pos, expr_token.sqlTokenIsRegexpMatchFunction),
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
    return try expr_build.buildFunctionExpressionFromOperandListAlloc(alloc, kind, &operands);
}

pub fn parseSubstringRowExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    type_context: expr_type.RowExpressionTypeContext,
    hooks: VariadicRowExpressionParserHooks,
) !db_mod.types.RelationalRowsExpression {
    try expr_token.parseFunctionCallStartTokenIf(tokens, pos, expr_token.sqlTokenIsSubstringFunction);
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
    return try expr_build.buildFunctionExpressionFromOperandListAlloc(alloc, .substring, &operands);
}

pub fn parseOverlayRowExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    type_context: expr_type.RowExpressionTypeContext,
    hooks: VariadicRowExpressionParserHooks,
) !db_mod.types.RelationalRowsExpression {
    try expr_token.parseFunctionCallStartTokenIf(tokens, pos, expr_token.sqlTokenIsOverlayFunction);
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
    return try expr_build.buildFunctionExpressionFromOperandListAlloc(alloc, .overlay, &operands);
}

pub fn parseSplitPartRowExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    type_context: expr_type.RowExpressionTypeContext,
    hooks: VariadicRowExpressionParserHooks,
) !db_mod.types.RelationalRowsExpression {
    try expr_token.parseFunctionCallStartTokenIf(tokens, pos, expr_token.sqlTokenIsSplitPartFunction);
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

    const expression = try expr_build.buildTernaryFunctionExpressionAlloc(alloc, .split_part, source, delimiter, position);
    source_transferred = true;
    delimiter_transferred = true;
    position_transferred = true;
    return expression;
}

pub fn parseStrposRowExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    type_context: expr_type.RowExpressionTypeContext,
    hooks: VariadicRowExpressionParserHooks,
) !db_mod.types.RelationalRowsExpression {
    if (expr_token.matchFunctionKeywordToken(tokens, pos, expr_token.sqlTokenIsStrposFunction) != null) {
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

        const expression = try expr_build.buildBinaryFunctionExpressionAlloc(alloc, .strpos, source, needle);
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

        const expression = try expr_build.buildBinaryFunctionExpressionAlloc(alloc, .strpos, source, needle);
        source_transferred = true;
        needle_transferred = true;
        return expression;
    } else return error.UnsupportedSqlShape;
}

pub fn parsePadRowExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    type_context: expr_type.RowExpressionTypeContext,
    hooks: VariadicRowExpressionParserHooks,
) !db_mod.types.RelationalRowsExpression {
    const kind = expr_token.matchPadFunctionKind(tokens, pos) orelse return error.UnsupportedSqlShape;
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
    return try expr_build.buildFunctionExpressionFromOperandListAlloc(alloc, kind, &operands);
}

pub fn parseLeftRightRowExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    type_context: expr_type.RowExpressionTypeContext,
    hooks: FixedBinaryRowExpressionParserOptions,
) !db_mod.types.RelationalRowsExpression {
    const kind = expr_token.matchLeftRightFunctionKind(tokens, pos) orelse return error.UnsupportedSqlShape;
    try parser.expectToken(tokens, pos, .lparen);
    return try parseTextNumericBinaryRowExpressionRestAlloc(alloc, tokens, pos, kind, type_context, hooks);
}

pub fn parseRepeatRowExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    type_context: expr_type.RowExpressionTypeContext,
    hooks: FixedBinaryRowExpressionParserOptions,
) !db_mod.types.RelationalRowsExpression {
    try expr_token.parseFunctionCallStartTokenIf(tokens, pos, expr_token.sqlTokenIsRepeatFunction);
    return try parseTextNumericBinaryRowExpressionRestAlloc(alloc, tokens, pos, .repeat, type_context, hooks);
}

pub fn parseDateTruncRowExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    type_context: expr_type.RowExpressionTypeContext,
    hooks: FixedBinaryRowExpressionParserOptions,
) !db_mod.types.RelationalRowsExpression {
    try expr_token.parseFunctionCallStartTokenIf(tokens, pos, expr_token.sqlTokenIsDateTruncFunction);
    const unit = try parseRowExpressionAlloc(alloc, tokens, pos, type_context, hooks.row_expression_hooks, hooks.arithmetic_hooks, hooks.variadic_hooks);
    var unit_transferred = false;
    errdefer if (!unit_transferred) freeExpression(alloc, unit);
    try type_context.validateTextRowExpression(unit);
    try parser.expectToken(tokens, pos, .comma);

    const value = try parseRowExpressionAlloc(alloc, tokens, pos, type_context, hooks.row_expression_hooks, hooks.arithmetic_hooks, hooks.variadic_hooks);
    var value_transferred = false;
    errdefer if (!value_transferred) freeExpression(alloc, value);
    try type_context.validateNumericOrDatetimeRowExpression(value);
    try parser.expectToken(tokens, pos, .rparen);

    const expression = try expr_build.buildBinaryFunctionExpressionAlloc(alloc, .date_trunc, unit, value);
    unit_transferred = true;
    value_transferred = true;
    return expression;
}

pub fn parseDateBinRowExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    type_context: expr_type.RowExpressionTypeContext,
    hooks: VariadicRowExpressionParserHooks,
) !db_mod.types.RelationalRowsExpression {
    try expr_token.parseFunctionCallStartTokenIf(tokens, pos, expr_token.sqlTokenIsDateBinFunction);
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

    const expression = try expr_build.buildTernaryFunctionExpressionAlloc(alloc, .date_bin, stride, source, origin);
    stride_transferred = true;
    source_transferred = true;
    origin_transferred = true;
    return expression;
}

pub fn parseDatePartRowExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    type_context: expr_type.RowExpressionTypeContext,
    hooks: VariadicRowExpressionParserHooks,
) !db_mod.types.RelationalRowsExpression {
    const extract_syntax = try expr_token.parseDatePartFunctionCallStart(tokens, pos);
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
        try expr_token.parseDatePartExtractSeparator(tokens, pos);
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

    const expression = try expr_build.buildBinaryFunctionExpressionAlloc(alloc, .date_part, unit, value);
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
    type_context: expr_type.RowExpressionTypeContext,
    hooks: VariadicRowExpressionParserHooks,
) !db_mod.types.RelationalRowsExpression {
    const kind = try expr_token.parseConcatFunctionCallStart(tokens, pos);

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

    return try expr_build.buildFunctionExpressionFromOperandListAlloc(alloc, kind, &operands);
}

pub fn parsePipeConcatExpressionRestAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    left: db_mod.types.RelationalRowsExpression,
    type_context: expr_type.RowExpressionTypeContext,
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
    return try expr_build.buildFunctionExpressionFromOperandListAlloc(alloc, .concat, &operands);
}

pub fn parseArithmeticExpressionRestAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    lhs: db_mod.types.RelationalRowsExpression,
    min_precedence: u8,
    type_context: expr_type.RowExpressionTypeContext,
    hooks: ArithmeticExpressionParserHooks,
) !db_mod.types.RelationalRowsExpression {
    var current = lhs;
    var current_owned = true;
    errdefer if (current_owned) freeExpression(alloc, current);

    while (expr_operator.peekArithmeticOperator(tokens, pos.*)) |op| {
        if (op.precedence < min_precedence) break;
        _ = parser.matchToken(tokens, pos, op.token) orelse unreachable;
        if (expr_token.peekSqlIntervalExpressionSyntax(tokens, pos.*)) {
            try type_context.validateNumericOrDatetimeRowExpression(current);
            const interval = try value_mod.parseSqlIntervalLiteral(tokens, pos);
            const next = try expr_build.buildIntervalLiteralArithmeticAlloc(alloc, current, op.kind, interval);
            current_owned = false;
            current = next;
            current_owned = true;
            continue;
        }

        var rhs = if (expr_token.peekParenthesizedExpressionSyntax(tokens, pos.*))
            try parseParenthesizedRowExpressionAlloc(alloc, tokens, pos, hooks.parenthesized)
        else
            try hooks.parse_operand(hooks.ptr);
        var rhs_owned = true;
        errdefer if (rhs_owned) freeExpression(alloc, rhs);
        try type_context.validateNumericRowExpression(rhs);

        while (expr_operator.peekArithmeticOperator(tokens, pos.*)) |next_op| {
            if (next_op.precedence <= op.precedence) break;
            rhs_owned = false;
            rhs = try parseArithmeticExpressionRestAlloc(alloc, tokens, pos, rhs, next_op.precedence, type_context, hooks);
            rhs_owned = true;
        }

        const expression = try expr_build.buildBinaryExpressionAlloc(alloc, op.kind, current, rhs);
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
    type_context: expr_type.RowExpressionTypeContext,
    hooks: BooleanExpressionParserHooks,
) !db_mod.types.RelationalRowsExpression {
    try type_context.validateBooleanRowExpression(lhs);
    var current = lhs;
    var current_owned = true;
    errdefer if (current_owned) freeExpression(alloc, current);

    while (expr_operator.peekBooleanOperator(tokens, pos.*)) |op| {
        if (op.precedence < min_precedence) break;
        if (!parser.matchKeyword(tokens, pos, op.keyword)) unreachable;
        var rhs = try hooks.parse_operand(hooks.ptr);
        var rhs_owned = true;
        errdefer if (rhs_owned) freeExpression(alloc, rhs);
        try type_context.validateBooleanRowExpression(rhs);

        while (expr_operator.peekBooleanOperator(tokens, pos.*)) |next_op| {
            if (next_op.precedence <= op.precedence) break;
            rhs_owned = false;
            rhs = try parseBooleanExpressionRestAlloc(alloc, tokens, pos, rhs, next_op.precedence, type_context, hooks);
            rhs_owned = true;
        }

        const expression = try expr_build.buildBinaryExpressionAlloc(alloc, op.kind, current, rhs);
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
    type_context: expr_type.RowExpressionTypeContext,
    hooks: RowExpressionParserHooks,
    arithmetic_hooks: ArithmeticExpressionParserHooks,
    variadic_hooks: VariadicRowExpressionParserHooks,
) !db_mod.types.RelationalRowsExpression {
    var expression = try hooks.parse_operand(hooks.ptr);
    var expression_owned = true;
    errdefer if (expression_owned) freeExpression(alloc, expression);
    if (expr_operator.peekArithmeticOperator(tokens, pos.*)) |_| {
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

pub fn parseBooleanRowExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    type_context: expr_type.RowExpressionTypeContext,
    hooks: BooleanRowExpressionParserHooks,
) !db_mod.types.RelationalRowsExpression {
    var expression = try hooks.parse_expression(hooks.ptr);
    var expression_owned = true;
    errdefer if (expression_owned) freeExpression(alloc, expression);
    if (expr_operator.peekBooleanOperator(tokens, pos.*)) |_| {
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

pub fn parseBooleanNotRowExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    type_context: expr_type.RowExpressionTypeContext,
    options: BooleanNotRowExpressionParserOptions,
) anyerror!db_mod.types.RelationalRowsExpression {
    try expr_token.parseBooleanNotExpressionStart(tokens, pos);
    const operand = if (expr_token.peekParenthesizedExpressionSyntax(tokens, pos.*))
        try parseParenthesizedRowExpressionAlloc(alloc, tokens, pos, .{
            .type_context = type_context,
            .boolean_hooks = options.parenthesized.boolean_hooks,
        })
    else
        try options.operand_hooks.parse_operand(options.operand_hooks.ptr);
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
    hooks: RowExpressionParserHooks,
) anyerror!db_mod.types.RelationalRowsExpression {
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
    const expression = try expr_build.buildUnaryNegativeExpressionAlloc(alloc, operand);
    operand_transferred = true;
    return expression;
}

pub fn parseNullifRowExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    type_context: expr_type.RowExpressionTypeContext,
    hooks: RowExpressionParserHooks,
) anyerror!db_mod.types.RelationalRowsExpression {
    try expr_token.parseNullifFunctionCallStart(tokens, pos);
    const lhs = try hooks.parse_operand(hooks.ptr);
    var lhs_transferred = false;
    errdefer if (!lhs_transferred) freeExpression(alloc, lhs);
    try parser.expectToken(tokens, pos, .comma);
    const rhs = try hooks.parse_operand(hooks.ptr);
    var rhs_transferred = false;
    errdefer if (!rhs_transferred) freeExpression(alloc, rhs);
    try parser.expectToken(tokens, pos, .rparen);

    const expression = try expr_build.buildBinaryFunctionExpressionAlloc(alloc, .nullif, lhs, rhs);
    var expression_transferred = false;
    errdefer if (!expression_transferred) freeExpression(alloc, expression);
    lhs_transferred = true;
    rhs_transferred = true;
    try type_context.validateExpressionOperandDomains(expression);
    expression_transferred = true;
    return expression;
}

pub fn parseCaseRowExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    type_context: expr_type.RowExpressionTypeContext,
    defer_row_expression_field_validation: bool,
    hooks: CaseExpressionParserHooks,
) !db_mod.types.RelationalRowsExpression {
    try expr_token.parseCaseExpressionStart(tokens, pos);

    var branches = std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionCaseBranch).empty;
    errdefer {
        for (branches.items) |branch| plan_mod.freeExpressionCaseBranch(alloc, branch);
        branches.deinit(alloc);
    }

    while (expr_token.matchCaseExpressionWhen(tokens, pos)) {
        const condition = try parseCaseExpressionConditionAlloc(alloc, tokens, pos, type_context, defer_row_expression_field_validation, hooks);
        var condition_transferred = false;
        errdefer if (!condition_transferred) freeExpressionCondition(alloc, condition);
        try expr_token.parseCaseExpressionThen(tokens, pos);
        const then_expression = try hooks.parse_operand(hooks.ptr);
        var then_transferred = false;
        errdefer if (!then_transferred) freeExpression(alloc, then_expression);
        try branches.append(alloc, .{ .when = condition, .then = then_expression });
        condition_transferred = true;
        then_transferred = true;
    }
    if (branches.items.len == 0) return error.UnsupportedSqlShape;

    try expr_token.parseCaseExpressionElse(tokens, pos);
    const else_expression = try hooks.parse_operand(hooks.ptr);
    var else_transferred = false;
    errdefer if (!else_transferred) freeExpression(alloc, else_expression);
    try expr_token.parseCaseExpressionEnd(tokens, pos);

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
    type_context: expr_type.RowExpressionTypeContext,
    defer_row_expression_field_validation: bool,
    hooks: CaseExpressionParserHooks,
) !db_mod.types.RelationalRowsExpressionCondition {
    return try parseCaseExpressionConditionWithOperatorAlloc(
        alloc,
        tokens,
        pos,
        type_context,
        defer_row_expression_field_validation,
        hooks,
        null,
    );
}

pub fn parseCaseExpressionConditionWithOperatorAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    type_context: expr_type.RowExpressionTypeContext,
    defer_row_expression_field_validation: bool,
    hooks: CaseExpressionParserHooks,
    operator_token_index: ?*usize,
) !db_mod.types.RelationalRowsExpressionCondition {
    const lhs = try hooks.parse_expression(hooks.ptr);
    var lhs_transferred = false;
    errdefer if (!lhs_transferred) freeExpression(alloc, lhs);

    const parsed_operator_token_index = pos.*;
    const op: runtime_schema.RelationalCheckOp = if (try expr_operator.parseExpressionIsTailIf(tokens, pos, .{})) |is_tail|
        is_tail.op
    else
        try expr_operator.parseComparisonOp(tokens, pos);
    if (operator_token_index) |out| out.* = parsed_operator_token_index;

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
    try expr_type.validateExpressionConditionTypes(type_context, defer_row_expression_field_validation, lhs, op, rhs);

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
    type_context: expr_type.RowExpressionTypeContext,
    options: CaseFoldRowExpressionParserOptions,
) !db_mod.types.RelationalRowsExpression {
    const kind = try expr_token.parseCaseFoldFunctionCallStart(tokens, pos);
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

    var effective_type_context = type_context;
    effective_type_context.defer_row_expression_field_validation =
        effective_type_context.defer_row_expression_field_validation or
        defer_row_expression_field_validation or
        effective_type_context.joined_source_schema != null;

    const operand = parseRowExpressionAlloc(alloc, tokens, pos, effective_type_context, options.row_expression_hooks, options.arithmetic_hooks, options.variadic_hooks) catch |err| {
        const token_text = if (pos.* < tokens.len) tokens[pos.*].text else "<eof>";
        std.log.err("case fold operand parse failed token={s} pos={} err={}", .{ token_text, pos.*, err });
        return err;
    };
    var operand_transferred = false;
    errdefer if (!operand_transferred) freeExpression(alloc, operand);
    effective_type_context.validateTextRowExpression(operand) catch |err| {
        const field = if (operand.kind == .field) operand.field else "";
        std.log.err("case fold operand text validation failed kind={} field={s} err={}", .{ operand.kind, field, err });
        return err;
    };
    try operands.append(alloc, operand);
    operand_transferred = true;

    if (kind == .trim or kind == .ltrim or kind == .rtrim) {
        if (parser.matchToken(tokens, pos, .comma) != null) {
            const trim_operand = try parseRowExpressionAlloc(alloc, tokens, pos, effective_type_context, options.row_expression_hooks, options.arithmetic_hooks, options.variadic_hooks);
            var trim_transferred = false;
            errdefer if (!trim_transferred) freeExpression(alloc, trim_operand);
            try effective_type_context.validateTextRowExpression(trim_operand);
            try operands.append(alloc, trim_operand);
            trim_transferred = true;
        }
    }
    try parser.expectToken(tokens, pos, .rparen);

    return try expr_build.buildFunctionExpressionFromOperandListAlloc(alloc, kind, &operands);
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
    type_context: expr_type.RowExpressionTypeContext,
    hooks: FixedBinaryRowExpressionParserOptions,
) !db_mod.types.RelationalRowsExpression {
    const lhs = try parseRowExpressionAlloc(alloc, tokens, pos, type_context, hooks.row_expression_hooks, hooks.arithmetic_hooks, hooks.variadic_hooks);
    var lhs_transferred = false;
    errdefer if (!lhs_transferred) freeExpression(alloc, lhs);
    try type_context.validateTextRowExpression(lhs);
    try parser.expectToken(tokens, pos, .comma);
    const rhs = try parseRowExpressionAlloc(alloc, tokens, pos, type_context, hooks.row_expression_hooks, hooks.arithmetic_hooks, hooks.variadic_hooks);
    var rhs_transferred = false;
    errdefer if (!rhs_transferred) freeExpression(alloc, rhs);
    try type_context.validateNumericRowExpression(rhs);
    try parser.expectToken(tokens, pos, .rparen);

    const expression = try expr_build.buildBinaryFunctionExpressionAlloc(alloc, kind, lhs, rhs);
    lhs_transferred = true;
    rhs_transferred = true;
    return expression;
}

pub fn parseGreatestLeastRowExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    type_context: expr_type.RowExpressionTypeContext,
    hooks: VariadicRowExpressionParserHooks,
) !db_mod.types.RelationalRowsExpression {
    const kind = try expr_token.parseGreatestLeastFunctionCallStart(tokens, pos);

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

    const expression = try expr_build.buildFunctionExpressionFromOperandListAlloc(alloc, kind, &operands);
    var expression_transferred = false;
    errdefer if (!expression_transferred) freeExpression(alloc, expression);
    try type_context.validateExpressionOperandDomains(expression);
    expression_transferred = true;
    return expression;
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

pub fn testRowParseResolvesFunctionBindings() !void {
    const tokens = [_]Token{
        .{ .kind = .identifier, .text = "normalize_status" },
        .{ .kind = .lparen, .text = "(" },
    };
    const extension_bindings = [_]ExtensionFunctionBinding{.{
        .sql_name = "normalize_status",
        .native_expression_kind = .lower,
        .arity = 1,
    }};
    try std.testing.expect(peekExtensionFunctionCall(&tokens, 0, &extension_bindings));
    try std.testing.expect(!peekExtensionFunctionCall(&tokens, 1, &extension_bindings));
    const extension_binding = (try extensionFunctionBinding(&extension_bindings, "NORMALIZE_STATUS")).?;
    try std.testing.expectEqual(runtime_schema.RelationalRowsExpressionKind.lower, extension_binding.native_expression_kind);
    try validateExtensionFunctionArity(extension_binding.native_expression_kind, extension_binding.arity, 1);
    try std.testing.expectError(error.UnsupportedSqlShape, validateExtensionFunctionArity(.lower, 1, 2));
    const duplicate_extension_bindings = [_]ExtensionFunctionBinding{
        .{ .sql_name = "normalize_status", .native_expression_kind = .lower, .arity = 1 },
        .{ .sql_name = "normalize_status", .native_expression_kind = .upper, .arity = 1 },
    };
    try std.testing.expectError(error.UnsupportedSqlShape, extensionFunctionBinding(&duplicate_extension_bindings, "normalize_status"));

    const routine_bindings = [_]RoutineExpressionBinding{
        .{
            .sql_name = "status_label",
            .arity = 1,
            .expression = .{ .kind = .lower },
        },
        .{
            .sql_name = "status_label",
            .arity = 2,
            .expression = .{ .kind = .concat_ws },
        },
    };
    const routine_tokens = [_]Token{
        .{ .kind = .identifier, .text = "status_label" },
        .{ .kind = .lparen, .text = "(" },
    };
    try std.testing.expect(peekRoutineExpressionCall(&routine_tokens, 0, &routine_bindings));
    try std.testing.expectEqual(
        runtime_schema.RelationalRowsExpressionKind.concat_ws,
        (try routineExpressionBinding(&routine_bindings, "STATUS_LABEL", 2)).?.expression.kind,
    );
    try std.testing.expect((try routineExpressionBinding(&routine_bindings, "status_label", 3)) == null);
    const duplicate_routine_bindings = [_]RoutineExpressionBinding{
        .{ .sql_name = "status_label", .arity = 1, .expression = .{ .kind = .lower } },
        .{ .sql_name = "status_label", .arity = 1, .expression = .{ .kind = .upper } },
    };
    try std.testing.expectError(error.UnsupportedSqlShape, routineExpressionBinding(&duplicate_routine_bindings, "status_label", 1));
    try std.testing.expect(routineArgumentExpressionIsNullLiteral(.{ .kind = .value, .value_json = "null" }));
    try std.testing.expect(!routineArgumentExpressionIsNullLiteral(.{ .kind = .value, .value_json = "\"null\"" }));
}

pub const RowExpressionOperandParserOptions = struct {
    extension_function: ExtensionFunctionRowExpressionParserOptions,
    routine_expression: RoutineExpressionRowExpressionParserOptions,
    parenthesized: ParenthesizedRowExpressionParserOptions,
    cast: CastRowExpressionParserOptions,
    case_expression: CaseExpressionParserHooks,
    case_fold: CaseFoldRowExpressionParserOptions,
    coalesce: CoalesceRowExpressionParserOptions,
    fixed_unary: FixedUnaryRowExpressionParserOptions,
    fixed_binary: FixedBinaryRowExpressionParserOptions,
    variadic: VariadicRowExpressionParserHooks,
    json_build_object: JsonBuildObjectRowExpressionParserOptions,
};

pub fn rowExpressionOperandStartWithFunctionBindingsAt(tokens: []const Token, pos: usize, bindings: SqlFunctionBindings) ?expr_parse.RowExpressionOperandStart {
    if (expr_parse.rowExpressionOperandStartAt(tokens, pos)) |start| return start;
    if (peekExtensionFunctionCall(tokens, pos, bindings.extension_functions)) return .extension_function;
    if (peekRoutineExpressionCall(tokens, pos, bindings.routine_expressions)) return .routine_expression;
    return null;
}

pub fn parseRowExpressionOperandAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    params: []const value_mod.SqlValue,
    schema: runtime_schema.TableSchema,
    joined_source_schema: ?runtime_schema.TableSchema,
    function_bindings: SqlFunctionBindings,
    field_expression_qualifiers: []const []const u8,
    returning_expression_qualifiers: []const []const u8,
    joined_source_expression_qualifiers: []const []const u8,
    joined_target_expression_qualifiers: []const []const u8,
    defer_row_expression_field_validation: bool,
    field_source: db_mod.types.RelationalRowsExpressionFieldSource,
    type_context: expr_type.RowExpressionTypeContext,
    hooks: RowExpressionOperandParserOptions,
) !db_mod.types.RelationalRowsExpression {
    if (rowExpressionOperandStartWithFunctionBindingsAt(tokens, pos.*, function_bindings)) |start| {
        switch (start) {
            .parenthesized => return try parseParenthesizedRowExpressionAlloc(alloc, tokens, pos, hooks.parenthesized),
            .unary_negative => {
                _ = parser.matchToken(tokens, pos, .minus) orelse return error.UnsupportedSqlShape;
                if (parser.matchToken(tokens, pos, .number)) |token| {
                    return .{
                        .kind = .value,
                        .value_json = try std.fmt.allocPrint(alloc, "-{s}", .{token.text}),
                    };
                }
                const operand = try parseRowExpressionOperandAlloc(
                    alloc,
                    tokens,
                    pos,
                    params,
                    schema,
                    joined_source_schema,
                    function_bindings,
                    field_expression_qualifiers,
                    returning_expression_qualifiers,
                    joined_source_expression_qualifiers,
                    joined_target_expression_qualifiers,
                    defer_row_expression_field_validation,
                    field_source,
                    type_context,
                    hooks,
                );
                var operand_transferred = false;
                errdefer if (!operand_transferred) freeExpression(alloc, operand);
                const expression = try expr_build.buildUnaryNegativeExpressionAlloc(alloc, operand);
                operand_transferred = true;
                return expression;
            },
            .boolean_not => {
                try expr_token.parseBooleanNotExpressionStart(tokens, pos);
                const operand = if (expr_token.peekParenthesizedExpressionSyntax(tokens, pos.*))
                    try parseParenthesizedRowExpressionAlloc(alloc, tokens, pos, .{
                        .type_context = type_context,
                        .boolean_hooks = hooks.parenthesized.boolean_hooks,
                    })
                else
                    try parseRowExpressionOperandAlloc(
                        alloc,
                        tokens,
                        pos,
                        params,
                        schema,
                        joined_source_schema,
                        function_bindings,
                        field_expression_qualifiers,
                        returning_expression_qualifiers,
                        joined_source_expression_qualifiers,
                        joined_target_expression_qualifiers,
                        defer_row_expression_field_validation,
                        field_source,
                        type_context,
                        hooks,
                    );
                var operand_transferred = false;
                errdefer if (!operand_transferred) freeExpression(alloc, operand);
                try type_context.validateBooleanRowExpression(operand);
                const expression = try wrapBooleanNotExpressionAlloc(alloc, operand);
                operand_transferred = true;
                return expression;
            },
            .extension_function => return (try parseExtensionFunctionRowExpressionOrNullAlloc(alloc, tokens, pos, function_bindings.extension_functions, hooks.extension_function)) orelse unreachable,
            .routine_expression => return (try parseRoutineExpressionRowExpressionOrNullAlloc(alloc, tokens, pos, function_bindings.routine_expressions, hooks.routine_expression)) orelse unreachable,
            .cast => return try parseCastRowExpressionAlloc(alloc, tokens, pos, hooks.cast),
            .case => return try parseCaseRowExpressionAlloc(alloc, tokens, pos, type_context, defer_row_expression_field_validation, hooks.case_expression),
            .now => return try expr_build.parseSqlNowRowExpressionAlloc(alloc, tokens, pos),
            .current_date => return try expr_build.parseSqlCurrentDateRowExpressionAlloc(alloc, tokens, pos),
            .typed_datetime_literal => return try expr_build.parseSqlTypedDatetimeLiteralRowExpressionAlloc(alloc, tokens, pos),
            .uuid_v4 => return try expr_build.parseSqlUuidV4RowExpression(tokens, pos),
            .interval => return try expr_build.parseSqlIntervalRowExpressionAlloc(alloc, tokens, pos),
            .case_fold => return try parseCaseFoldRowExpressionAlloc(alloc, tokens, pos, schema, field_expression_qualifiers, returning_expression_qualifiers, defer_row_expression_field_validation, field_source, type_context, hooks.case_fold),
            .replace => return try parseTextTernaryRowExpressionAlloc(alloc, tokens, pos, .replace, type_context, hooks.variadic),
            .regexp_replace => return try parseRegexpListRowExpressionAlloc(alloc, tokens, pos, .regexp_replace, type_context, hooks.variadic),
            .regexp_substr => return try parseTextBinaryRowExpressionAlloc(alloc, tokens, pos, .regexp_substr, type_context, hooks.fixed_binary),
            .regexp_match => return try parseRegexpListRowExpressionAlloc(alloc, tokens, pos, .regexp_match, type_context, hooks.variadic),
            .regexp_count => return try parseTextBinaryRowExpressionAlloc(alloc, tokens, pos, .regexp_count, type_context, hooks.fixed_binary),
            .regexp_instr => return try parseTextBinaryRowExpressionAlloc(alloc, tokens, pos, .regexp_instr, type_context, hooks.fixed_binary),
            .translate => return try parseTextTernaryRowExpressionAlloc(alloc, tokens, pos, .translate, type_context, hooks.variadic),
            .concat => return try parseConcatRowExpressionAlloc(alloc, tokens, pos, type_context, hooks.variadic),
            .coalesce => return try parseCoalesceRowExpressionAlloc(alloc, tokens, pos, type_context, hooks.coalesce),
            .nullif => {
                try expr_token.parseNullifFunctionCallStart(tokens, pos);
                const lhs = try parseRowExpressionOperandAlloc(
                    alloc,
                    tokens,
                    pos,
                    params,
                    schema,
                    joined_source_schema,
                    function_bindings,
                    field_expression_qualifiers,
                    returning_expression_qualifiers,
                    joined_source_expression_qualifiers,
                    joined_target_expression_qualifiers,
                    defer_row_expression_field_validation,
                    field_source,
                    type_context,
                    hooks,
                );
                var lhs_transferred = false;
                errdefer if (!lhs_transferred) freeExpression(alloc, lhs);
                try parser.expectToken(tokens, pos, .comma);
                const rhs = try parseRowExpressionOperandAlloc(
                    alloc,
                    tokens,
                    pos,
                    params,
                    schema,
                    joined_source_schema,
                    function_bindings,
                    field_expression_qualifiers,
                    returning_expression_qualifiers,
                    joined_source_expression_qualifiers,
                    joined_target_expression_qualifiers,
                    defer_row_expression_field_validation,
                    field_source,
                    type_context,
                    hooks,
                );
                var rhs_transferred = false;
                errdefer if (!rhs_transferred) freeExpression(alloc, rhs);
                try parser.expectToken(tokens, pos, .rparen);
                const expression = try expr_build.buildBinaryFunctionExpressionAlloc(alloc, .nullif, lhs, rhs);
                var expression_transferred = false;
                errdefer if (!expression_transferred) freeExpression(alloc, expression);
                lhs_transferred = true;
                rhs_transferred = true;
                try type_context.validateExpressionOperandDomains(expression);
                expression_transferred = true;
                return expression;
            },
            .text_length => return try parseTextLengthRowExpressionAlloc(alloc, tokens, pos, type_context, hooks.fixed_unary),
            .ascii => return try parseFixedUnaryRowExpressionAlloc(alloc, tokens, pos, .ascii, type_context, .text, hooks.fixed_unary),
            .chr => return try parseFixedUnaryRowExpressionAlloc(alloc, tokens, pos, .chr, type_context, .numeric, hooks.fixed_unary),
            .substring => return try parseSubstringRowExpressionAlloc(alloc, tokens, pos, type_context, hooks.variadic),
            .overlay => return try parseOverlayRowExpressionAlloc(alloc, tokens, pos, type_context, hooks.variadic),
            .split_part => return try parseSplitPartRowExpressionAlloc(alloc, tokens, pos, type_context, hooks.variadic),
            .strpos => return try parseStrposRowExpressionAlloc(alloc, tokens, pos, type_context, hooks.variadic),
            .left_right => return try parseLeftRightRowExpressionAlloc(alloc, tokens, pos, type_context, hooks.fixed_binary),
            .pad => return try parsePadRowExpressionAlloc(alloc, tokens, pos, type_context, hooks.variadic),
            .repeat => return try parseRepeatRowExpressionAlloc(alloc, tokens, pos, type_context, hooks.fixed_binary),
            .reverse => return try parseFixedUnaryRowExpressionAlloc(alloc, tokens, pos, .reverse, type_context, .text, hooks.fixed_unary),
            .md5 => return try parseFixedUnaryRowExpressionAlloc(alloc, tokens, pos, .md5, type_context, .text, hooks.fixed_unary),
            .starts_with => return try parseTextBinaryRowExpressionAlloc(alloc, tokens, pos, .starts_with, type_context, hooks.fixed_binary),
            .ends_with => return try parseTextBinaryRowExpressionAlloc(alloc, tokens, pos, .ends_with, type_context, hooks.fixed_binary),
            .date_trunc => return try parseDateTruncRowExpressionAlloc(alloc, tokens, pos, type_context, hooks.fixed_binary),
            .date_bin => return try parseDateBinRowExpressionAlloc(alloc, tokens, pos, type_context, hooks.variadic),
            .date_part => return try parseDatePartRowExpressionAlloc(alloc, tokens, pos, type_context, hooks.variadic),
            .abs => return try parseFixedUnaryRowExpressionAlloc(alloc, tokens, pos, .abs, type_context, .numeric, hooks.fixed_unary),
            .round => return try parseFixedUnaryRowExpressionAlloc(alloc, tokens, pos, .round, type_context, .numeric, hooks.fixed_unary),
            .trunc => return try parseFixedUnaryRowExpressionAlloc(alloc, tokens, pos, .trunc, type_context, .numeric, hooks.fixed_unary),
            .floor => return try parseFixedUnaryRowExpressionAlloc(alloc, tokens, pos, .floor, type_context, .numeric, hooks.fixed_unary),
            .ceil => return try parseFixedUnaryRowExpressionAlloc(alloc, tokens, pos, .ceil, type_context, .numeric, hooks.fixed_unary),
            .sqrt => return try parseFixedUnaryRowExpressionAlloc(alloc, tokens, pos, .sqrt, type_context, .numeric, hooks.fixed_unary),
            .sign => return try parseFixedUnaryRowExpressionAlloc(alloc, tokens, pos, .sign, type_context, .numeric, hooks.fixed_unary),
            .mod => return try parseFixedNumericBinaryRowExpressionAlloc(alloc, tokens, pos, .mod, type_context, hooks.fixed_binary),
            .power => return try parseFixedNumericBinaryRowExpressionAlloc(alloc, tokens, pos, .power, type_context, hooks.fixed_binary),
            .greatest_least => return try parseGreatestLeastRowExpressionAlloc(alloc, tokens, pos, type_context, hooks.variadic),
            .json_extract_path => return try parseJsonExtractPathRowExpressionAlloc(alloc, tokens, pos, params, type_context, hooks.fixed_unary),
            .json_typeof => return try parseJsonUnaryRowExpressionAlloc(alloc, tokens, pos, .json_typeof, type_context, hooks.fixed_unary),
            .json_array_length => return try parseJsonUnaryRowExpressionAlloc(alloc, tokens, pos, .json_array_length, type_context, hooks.fixed_unary),
            .json_build_object => return try parseJsonBuildObjectRowExpressionAlloc(alloc, tokens, pos, type_context, hooks.json_build_object),
            .to_jsonb => return try parseFixedUnaryRowExpressionAlloc(alloc, tokens, pos, .to_jsonb, type_context, .any, hooks.fixed_unary),
            .convert_from => {
                const value_json = try value_mod.parseJsonValueAlloc(alloc, tokens, pos, params);
                errdefer alloc.free(value_json);
                return .{ .kind = .value, .value_json = value_json };
            },
            .array_length => return try parseArrayLengthRowExpressionAlloc(alloc, tokens, pos, params, schema, field_expression_qualifiers, returning_expression_qualifiers, defer_row_expression_field_validation, field_source),
            .array_position => return try parseArrayPositionRowExpressionAlloc(alloc, tokens, pos, type_context, hooks.fixed_binary),
            .array_element_transform => return try parseArrayElementTransformRowExpressionAlloc(alloc, tokens, pos, type_context, hooks.variadic),
            .array_to_string => return try parseArrayToStringRowExpressionAlloc(alloc, tokens, pos, type_context, hooks.variadic),
            .string_to_array => return try parseStringToArrayRowExpressionAlloc(alloc, tokens, pos, type_context, hooks.fixed_binary),
        }
    }
    if (try parseRowExpressionFieldOperandOrNullAlloc(
        alloc,
        tokens,
        pos,
        schema,
        joined_source_schema,
        params,
        field_expression_qualifiers,
        returning_expression_qualifiers,
        joined_source_expression_qualifiers,
        joined_target_expression_qualifiers,
        defer_row_expression_field_validation,
        field_source,
    )) |expression| return expression;

    const value_json = try value_mod.parseJsonValueAlloc(alloc, tokens, pos, params);
    errdefer alloc.free(value_json);
    return .{ .kind = .value, .value_json = value_json };
}

pub fn parseRowExpressionOperandFromContextAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    params: []const value_mod.SqlValue,
    joined_source_schema: ?runtime_schema.TableSchema,
    function_bindings: SqlFunctionBindings,
    field_source: db_mod.types.RelationalRowsExpressionFieldSource,
    select_context_hooks: SelectParserContextHooks,
    joined_context_hooks: JoinedExpressionParserContextHooks,
    options: RowExpressionOperandParserOptions,
) !db_mod.types.RelationalRowsExpression {
    const select_context = select_context_hooks.get_context(select_context_hooks.ptr);
    const joined_context = joined_context_hooks.get_context(joined_context_hooks.ptr);
    const defer_field_validation =
        select_context.defer_row_expression_field_validation or
        joined_context.defer_row_expression_field_validation or
        joined_context.joined_source_expression_qualifiers.len != 0 or
        joined_context.joined_target_expression_qualifiers.len != 0;
    var type_context = select_context_hooks.row_expression_type_context(select_context_hooks.ptr);
    type_context.defer_row_expression_field_validation = defer_field_validation;
    return try parseRowExpressionOperandAlloc(
        alloc,
        tokens,
        pos,
        params,
        select_context.schema,
        joined_source_schema,
        function_bindings,
        select_context.field_expression_qualifiers,
        select_context.returning_expression_qualifiers,
        joined_context.joined_source_expression_qualifiers,
        joined_context.joined_target_expression_qualifiers,
        defer_field_validation,
        field_source,
        type_context,
        options,
    );
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
) !?db_mod.types.RelationalRowsExpression {
    if (!parser.peekKind(tokens, pos.*, .identifier) or
        parser.peekKeywordTag(tokens, pos.*, .null) or
        parser.peekKeywordTag(tokens, pos.*, .true) or
        parser.peekKeywordTag(tokens, pos.*, .false))
    {
        return null;
    }
    if (pos.* + 1 < tokens.len and tokens[pos.* + 1].kind == .lparen) return error.UnsupportedSqlShape;
    const parsed_field = if (joined_source_expression_qualifiers.len != 0 or joined_target_expression_qualifiers.len != 0)
        try parseIdentifierOwnedAlloc(alloc, tokens, pos)
    else
        try expr_generated.parseRowExpressionFieldOwnedAlloc(
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
    if (expr_operator.matchJsonExtractOperator(tokens, pos)) |operator| {
        const as_text = expr_operator.tokenKindIsJsonExtractTextOperator(operator);
        if (!defer_row_expression_field_validation and binder.relationalColumnForField(resolved.schema, resolved.field, .json) == null) return error.InvalidSqlCatalog;
        const path = try value_mod.parseJsonExtractOperatorPathOwnedAlloc(alloc, tokens, pos, params, operator);
        var path_transferred = false;
        errdefer if (!path_transferred) alloc.free(path);
        const expression = try expr_build.buildJsonExtractExpressionAlloc(
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
        const expression = try expr_build.buildJsonPathExistsExpressionAlloc(
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
        const values_json = try value_mod.parseJsonArrayValueAlloc(alloc, tokens, pos, params);
        defer alloc.free(values_json);
        const expression = try expr_build.buildJsonKeySetExistsExpressionAlloc(alloc, resolved.field, resolved.source, match_all, values_json);
        field_transferred = true;
        alloc.free(resolved.field);
        return expression;
    }
    if (!defer_row_expression_field_validation and binder.relationalColumnForField(resolved.schema, resolved.field, null) == null) return error.InvalidSqlCatalog;
    field_transferred = true;
    return .{ .kind = .field, .field = resolved.field, .field_source = resolved.source };
}

test "sql expr row parse resolves function bindings" {
    try testRowParseResolvesFunctionBindings();
}
