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
const expr_token = @import("token.zig");
const generated_parser = @import("../generated_parser.zig");
const parser = @import("../parser.zig");
const runtime_schema = @import("../../storage/schema.zig");
const token_mod = @import("../token.zig");

const Token = token_mod.Token;
const TokenKind = token_mod.TokenKind;

pub fn generatedTokenRangeEqual(
    a: generated_parser.GeneratedSqlTokenRange,
    b: generated_parser.GeneratedSqlTokenRange,
) bool {
    return a.start == b.start and a.end == b.end;
}

pub fn generatedExpressionKindUsesOperatorPayload(kind: generated_parser.GeneratedSqlExpressionKind) bool {
    return switch (kind) {
        .comparison,
        .like,
        .ilike,
        .in_list,
        .between,
        .not_like,
        .not_ilike,
        .not_in_list,
        .not_between,
        .quantified_comparison,
        .exists_subquery,
        .not_exists_subquery,
        .is_null,
        .is_not_null,
        .is_true,
        .is_false,
        .is_unknown,
        .is_not_true,
        .is_not_false,
        .is_not_unknown,
        .is_distinct_from,
        .is_not_distinct_from,
        .logical_or,
        .logical_and,
        .logical_not,
        .unary_positive,
        .unary_negative,
        .additive,
        .subtractive,
        .multiplicative,
        .divisive,
        .modulo,
        .contains,
        .overlaps,
        .json_key_exists,
        .json_key_any,
        .json_key_all,
        .regex_match,
        .regex_imatch,
        .regex_not_match,
        .regex_not_imatch,
        .string_concat,
        .json_access,
        .json_text_access,
        .json_path_access,
        .json_path_text_access,
        => true,
        else => false,
    };
}

pub fn generatedExpressionKindAllowsEscapePayload(kind: generated_parser.GeneratedSqlExpressionKind) bool {
    return switch (kind) {
        .like, .ilike, .not_like, .not_ilike => true,
        else => false,
    };
}

pub fn generatedTokenKindIsComparisonOperator(kind: TokenKind) bool {
    return kind == .eq or kind == .neq or kind == .lt or kind == .lte or kind == .gt or kind == .gte;
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
        if (generated.op == op) {
            const generated_field = generated.field orelse continue;
            if (std.mem.eql(u8, generated_field, field)) return column;
        } else if (generated.op == .expression and generatedUnaryExpressionMatchesField(generated.expression orelse continue, op, field)) {
            return column;
        }
    }
    return null;
}

fn generatedUnaryExpressionMatchesField(
    expression: runtime_schema.RelationalRowsExpression,
    op: runtime_schema.RelationalGeneratedOp,
    field: []const u8,
) bool {
    const kind: runtime_schema.RelationalRowsExpressionKind = switch (op) {
        .lower => .lower,
        .upper => .upper,
        .md5 => .md5,
        .concat, .concat_ws, .expression => return false,
    };
    if (expression.kind != kind or expression.operands.len != 1) return false;
    const operand = expression.operands[0];
    return operand.kind == .field and std.mem.eql(u8, operand.field, field);
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
        switch (generated.op) {
            .concat, .concat_ws => {
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
            },
            .expression => {
                const expression = generated.expression orelse continue;
                if (try concatCallMatchesGeneratedExpressionAt(
                    alloc,
                    tokens,
                    pos,
                    schema,
                    expression,
                    field_expression_qualifiers,
                    returning_expression_qualifiers,
                    defer_row_expression_field_validation,
                )) return column;
            },
            else => {},
        }
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

    if (generatedUnaryOpToken(tokens[pos.*])) |op| {
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

    const is_concat = tokens[pos.*].matchesKeywordTag(.concat);
    const is_concat_ws = tokens[pos.*].matchesKeywordTag(.concat_ws);
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
    if (generated_value.op == .expression) {
        pos.* -= 2;
        const expression = generated_value.expression orelse return error.UnsupportedSqlShape;
        try consumeConcatCallMatchingGeneratedExpression(
            alloc,
            tokens,
            pos,
            schema,
            expression,
            field_expression_qualifiers,
            returning_expression_qualifiers,
            defer_row_expression_field_validation,
        );
        return try alloc.dupe(u8, generated.name);
    }
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

fn consumeConcatCallMatchingGeneratedExpression(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    schema: runtime_schema.TableSchema,
    expression: runtime_schema.RelationalRowsExpression,
    field_expression_qualifiers: []const []const u8,
    returning_expression_qualifiers: []const []const u8,
    defer_row_expression_field_validation: bool,
) !void {
    if (expression.kind != .concat and expression.kind != .concat_ws) return error.UnsupportedSqlShape;
    if (tokens[pos.*].kind != .identifier) return error.UnsupportedSqlShape;
    if (expression.kind == .concat and !tokens[pos.*].matchesKeywordTag(.concat)) return error.UnsupportedSqlShape;
    if (expression.kind == .concat_ws and !tokens[pos.*].matchesKeywordTag(.concat_ws)) return error.UnsupportedSqlShape;
    pos.* += 1;
    try parser.expectToken(tokens, pos, .lparen);

    var operand_index: usize = 0;
    if (expression.kind == .concat_ws) {
        const separator = parser.matchToken(tokens, pos, .string) orelse return error.UnsupportedSqlShape;
        if (expression.operands[0].kind != .value) return error.UnsupportedSqlShape;
        if (!(try valueExpressionStringEquals(alloc, expression.operands[0], separator.text))) return error.UnsupportedSqlShape;
        if (parser.matchToken(tokens, pos, .comma) == null) return error.UnsupportedSqlShape;
        operand_index = 1;
    }

    while (operand_index < expression.operands.len) : (operand_index += 1) {
        const operand = expression.operands[operand_index];
        if (operand.kind == .value and expression.kind == .concat) {
            const separator = parser.matchToken(tokens, pos, .string) orelse return error.UnsupportedSqlShape;
            if (!(try valueExpressionStringEquals(alloc, operand, separator.text))) return error.UnsupportedSqlShape;
        } else if (operand.kind == .field) {
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
            if (!std.mem.eql(u8, source, operand.field)) return error.UnsupportedSqlShape;
        } else {
            return error.UnsupportedSqlShape;
        }
        if (operand_index + 1 < expression.operands.len and parser.matchToken(tokens, pos, .comma) == null) return error.UnsupportedSqlShape;
    }
    try parser.expectToken(tokens, pos, .rparen);
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
    if (try generatedExpressionCallHasGeneratedColumn(
        alloc,
        tokens,
        pos.*,
        schema,
        field_expression_qualifiers,
        returning_expression_qualifiers,
        defer_row_expression_field_validation,
    )) if (try parseGeneratedFieldExpressionOwnedAlloc(
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

pub fn generatedExpressionCallHasGeneratedColumn(
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
    else if (expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsMd5Function))
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

fn generatedUnaryOpToken(token: Token) ?runtime_schema.RelationalGeneratedOp {
    if (token.matchesKeywordTag(.lower)) return .lower;
    if (token.matchesKeywordTag(.upper)) return .upper;
    if (token.matchesKeywordTag(.md5)) return .md5;
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
    if (generated.op == .concat and !tokens[pos].matchesKeywordTag(.concat)) return false;
    if (generated.op == .concat_ws and !tokens[pos].matchesKeywordTag(.concat_ws)) return false;
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

fn concatCallMatchesGeneratedExpressionAt(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: usize,
    schema: runtime_schema.TableSchema,
    expression: runtime_schema.RelationalRowsExpression,
    field_expression_qualifiers: []const []const u8,
    returning_expression_qualifiers: []const []const u8,
    defer_row_expression_field_validation: bool,
) !bool {
    if (expression.kind != .concat and expression.kind != .concat_ws) return false;
    if (expression.operands.len == 0) return false;
    if (pos + 2 >= tokens.len or tokens[pos].kind != .identifier or tokens[pos + 1].kind != .lparen) return false;
    if (expression.kind == .concat and !tokens[pos].matchesKeywordTag(.concat)) return false;
    if (expression.kind == .concat_ws and !tokens[pos].matchesKeywordTag(.concat_ws)) return false;

    var token_i: usize = pos + 2;
    var operand_i: usize = 0;
    if (expression.kind == .concat_ws) {
        if (token_i >= tokens.len or tokens[token_i].kind != .string) return false;
        if (expression.operands[0].kind != .value) return false;
        if (!(try valueExpressionStringEquals(alloc, expression.operands[0], tokens[token_i].text))) return false;
        token_i += 1;
        if (token_i >= tokens.len or tokens[token_i].kind != .comma) return false;
        token_i += 1;
        operand_i = 1;
    }

    while (operand_i < expression.operands.len) : (operand_i += 1) {
        const operand = expression.operands[operand_i];
        if (operand.kind == .value and expression.kind == .concat) {
            if (token_i >= tokens.len or tokens[token_i].kind != .string) return false;
            if (!(try valueExpressionStringEquals(alloc, operand, tokens[token_i].text))) return false;
            token_i += 1;
        } else if (operand.kind == .field) {
            if (token_i >= tokens.len or tokens[token_i].kind != .identifier) return false;
            const normalized_field = try binder.normalizeRowExpressionFieldAlloc(
                alloc,
                schema,
                tokens[token_i].text,
                field_expression_qualifiers,
                returning_expression_qualifiers,
                defer_row_expression_field_validation,
            );
            defer alloc.free(normalized_field);
            if (!std.mem.eql(u8, normalized_field, operand.field)) return false;
            token_i += 1;
        } else {
            return false;
        }
        if (operand_i + 1 < expression.operands.len) {
            if (token_i >= tokens.len or tokens[token_i].kind != .comma) return false;
            token_i += 1;
        }
    }

    return token_i < tokens.len and tokens[token_i].kind == .rparen;
}

fn valueExpressionStringEquals(
    alloc: std.mem.Allocator,
    expression: runtime_schema.RelationalRowsExpression,
    expected: []const u8,
) !bool {
    if (expression.kind != .value) return false;
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, expression.value_json, .{}) catch return false;
    defer parsed.deinit();
    return parsed.value == .string and std.mem.eql(u8, parsed.value.string, expected);
}

test "sql expr_generated resolves generated field expressions" {
    const alloc = std.testing.allocator;
    const quoted_generated_name = Token{ .kind = .identifier, .text = "md5", .source_start = 0, .source_end = 5 };
    try std.testing.expect(generatedUnaryOpToken(quoted_generated_name) == null);

    const generated_columns = [_]runtime_schema.RelationalColumn{
        .{ .name = "tenant_id", .path = "tenant_id", .field_type = .keyword },
        .{ .name = "status", .path = "status", .field_type = .keyword },
        .{
            .name = "status_lower",
            .path = "status_lower",
            .field_type = .keyword,
            .generated = .{
                .op = .expression,
                .expression = .{
                    .kind = .lower,
                    .operands = &.{.{ .kind = .field, .field = "status" }},
                },
            },
        },
        .{
            .name = "tenant_status",
            .path = "tenant_status",
            .field_type = .keyword,
            .generated = .{
                .op = .expression,
                .expression = .{
                    .kind = .concat_ws,
                    .operands = &.{
                        .{ .kind = .value, .value_json = "\":\"" },
                        .{ .kind = .field, .field = "tenant_id" },
                        .{ .kind = .field, .field = "status" },
                    },
                },
            },
        },
    };
    const schema: runtime_schema.TableSchema = .{ .relational_columns = &generated_columns };
    try std.testing.expectEqualStrings("status_lower", generatedUnaryTextColumnForField(schema, .lower, "status").?.name);
    try std.testing.expect(generatedUnaryTextColumnForField(schema, .upper, "status") == null);
    try std.testing.expect(generatedUnaryTextColumnForField(schema, .concat, "status") == null);

    const lower_tokens = [_]Token{
        .{ .kind = .identifier, .text = "lower" },
        .{ .kind = .lparen, .text = "(" },
        .{ .kind = .identifier, .text = "status" },
        .{ .kind = .rparen, .text = ")" },
    };
    var lower_pos: usize = 0;
    const lower_field = try parseGeneratedFieldExpressionOwnedAlloc(alloc, &lower_tokens, &lower_pos, schema, &.{}, &.{}, false) orelse return error.TestUnexpectedResult;
    defer alloc.free(lower_field);
    try std.testing.expectEqualStrings("status_lower", lower_field);
    try std.testing.expectEqual(@as(usize, lower_tokens.len), lower_pos);

    const concat_tokens = [_]Token{
        .{ .kind = .identifier, .text = "concat_ws" },
        .{ .kind = .lparen, .text = "(" },
        .{ .kind = .string, .text = ":" },
        .{ .kind = .comma, .text = "," },
        .{ .kind = .identifier, .text = "tenant_id" },
        .{ .kind = .comma, .text = "," },
        .{ .kind = .identifier, .text = "status" },
        .{ .kind = .rparen, .text = ")" },
    };
    var concat_pos: usize = 0;
    const concat_field = try parseRowExpressionFieldOwnedAlloc(alloc, &concat_tokens, &concat_pos, schema, &.{}, &.{}, false);
    defer alloc.free(concat_field);
    try std.testing.expectEqualStrings("tenant_status", concat_field);
    try std.testing.expectEqual(@as(usize, concat_tokens.len), concat_pos);
}
