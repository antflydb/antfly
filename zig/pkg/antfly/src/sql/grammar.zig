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
const sql_statement_kind = @import("statement_kind.zig");
const db_mod = @import("../storage/db/mod.zig");
const ddl_plan = @import("ddl_plan.zig");
const expr_type = @import("expr/type.zig");
const generated_parser = @import("generated_parser.zig");
const expr_token = @import("expr/token.zig");
const lexer = @import("lexer.zig");
const parser = @import("parser.zig");
const runtime_schema = @import("../storage/schema.zig");
const token_mod = @import("token.zig");
const tokenized = @import("tokenized.zig");
const sql_value = @import("value.zig");

pub const Token = token_mod.Token;
const TokenKind = token_mod.TokenKind;

pub const AdapterNoopTransactionBoundaryTail = struct {
    work: bool = false,
    transaction: bool = false,
};

fn skipSqlWhitespace(sql: []const u8, start: usize) usize {
    var index = start;
    while (index < sql.len and std.ascii.isWhitespace(sql[index])) : (index += 1) {}
    return index;
}

fn consumeSqlKeyword(sql: []const u8, index: *usize, keyword: []const u8) bool {
    const start = index.*;
    const end = start + keyword.len;
    if (end > sql.len) return false;
    if (!std.ascii.eqlIgnoreCase(sql[start..end], keyword)) return false;
    if (end < sql.len and isSqlIdentifierByte(sql[end])) return false;
    if (start > 0 and isSqlIdentifierByte(sql[start - 1])) return false;
    index.* = end;
    return true;
}

fn isSqlIdentifierByte(byte: u8) bool {
    return std.ascii.isAlphanumeric(byte) or byte == '_';
}

pub fn parseRowSecurityPolicyPredicateAlloc(
    alloc: std.mem.Allocator,
    cursor: parser.Cursor,
    tokens: []const Token,
    pos: *usize,
) !ddl_plan.RowSecurityPolicyPredicate {
    try cursor.expectToken(.lparen);
    var predicate = try parseRowSecurityPolicyOrPredicateAlloc(alloc, cursor, tokens, pos);
    errdefer predicate.deinit(alloc);
    try cursor.expectToken(.rparen);
    return predicate;
}

fn parseRowSecurityPolicyOrPredicateAlloc(
    alloc: std.mem.Allocator,
    cursor: parser.Cursor,
    tokens: []const Token,
    pos: *usize,
) !ddl_plan.RowSecurityPolicyPredicate {
    var predicates = std.ArrayList(ddl_plan.RowSecurityPolicyPredicate).empty;
    var predicates_transferred = false;
    errdefer if (!predicates_transferred) freeRowSecurityPolicyPredicateList(alloc, &predicates);

    var first = try parseRowSecurityPolicyAndPredicateAlloc(alloc, cursor, tokens, pos);
    var first_transferred = false;
    errdefer if (!first_transferred) first.deinit(alloc);
    try predicates.append(alloc, first);
    first_transferred = true;
    while (true) {
        if (!cursor.matchKeyword("or")) break;
        var term = try parseRowSecurityPolicyAndPredicateAlloc(alloc, cursor, tokens, pos);
        var term_transferred = false;
        errdefer if (!term_transferred) term.deinit(alloc);
        try predicates.append(alloc, term);
        term_transferred = true;
    }

    if (predicates.items.len == 1) {
        const single = predicates.items[0];
        predicates_transferred = true;
        predicates.deinit(alloc);
        return single;
    }

    const owned = try predicates.toOwnedSlice(alloc);
    predicates_transferred = true;
    return .{ .disjunction = .{ .predicates = owned } };
}

fn parseRowSecurityPolicyAndPredicateAlloc(
    alloc: std.mem.Allocator,
    cursor: parser.Cursor,
    tokens: []const Token,
    pos: *usize,
) !ddl_plan.RowSecurityPolicyPredicate {
    var predicates = std.ArrayList(ddl_plan.RowSecurityPolicyPredicate).empty;
    var predicates_transferred = false;
    errdefer if (!predicates_transferred) freeRowSecurityPolicyPredicateList(alloc, &predicates);

    var first = try parseRowSecurityPolicyPredicateAtomAlloc(alloc, cursor, tokens, pos);
    var first_transferred = false;
    errdefer if (!first_transferred) first.deinit(alloc);
    try predicates.append(alloc, first);
    first_transferred = true;
    while (true) {
        if (!cursor.matchKeyword("and")) break;
        var term = try parseRowSecurityPolicyPredicateAtomAlloc(alloc, cursor, tokens, pos);
        var term_transferred = false;
        errdefer if (!term_transferred) term.deinit(alloc);
        try predicates.append(alloc, term);
        term_transferred = true;
    }

    if (predicates.items.len == 1) {
        const single = predicates.items[0];
        predicates_transferred = true;
        predicates.deinit(alloc);
        return single;
    }

    const owned = try predicates.toOwnedSlice(alloc);
    predicates_transferred = true;
    return .{ .conjunction = .{ .predicates = owned } };
}

fn freeRowSecurityPolicyPredicateList(
    alloc: std.mem.Allocator,
    predicates: *std.ArrayList(ddl_plan.RowSecurityPolicyPredicate),
) void {
    for (predicates.items) |*predicate| predicate.deinit(alloc);
    predicates.deinit(alloc);
}

fn parseRowSecurityPolicyPredicateAtomAlloc(
    alloc: std.mem.Allocator,
    cursor: parser.Cursor,
    tokens: []const Token,
    pos: *usize,
) !ddl_plan.RowSecurityPolicyPredicate {
    const start = pos.*;
    if (parseRowSecurityPolicySimplePredicateAtomAlloc(alloc, cursor, tokens, pos)) |predicate| {
        return predicate;
    } else |err| {
        switch (err) {
            error.UnsupportedSqlShape => pos.* = start,
            else => return err,
        }
    }
    return try parseRowSecurityPolicyExpressionPredicateAtomAlloc(alloc, cursor, tokens, pos);
}

fn parseRowSecurityPolicySimplePredicateAtomAlloc(
    alloc: std.mem.Allocator,
    cursor: parser.Cursor,
    tokens: []const Token,
    pos: *usize,
) !ddl_plan.RowSecurityPolicyPredicate {
    const field = try parseSqlObjectIdentifierOwnedAlloc(alloc, tokens, pos);
    var field_transferred = false;
    errdefer if (!field_transferred) alloc.free(field);
    try cursor.expectToken(.eq);
    if (cursor.matchKeyword("current_setting")) {
        try cursor.expectToken(.lparen);
        const setting_token = cursor.matchToken(.string) orelse return error.UnsupportedSqlShape;
        if (setting_token.text.len == 0) return error.UnsupportedSqlShape;
        const setting_name = try alloc.dupe(u8, setting_token.text);
        var setting_transferred = false;
        errdefer if (!setting_transferred) alloc.free(setting_name);
        try cursor.expectToken(.rparen);

        field_transferred = true;
        setting_transferred = true;
        return .{ .current_setting_equals = .{
            .field = field,
            .setting_name = setting_name,
        } };
    }

    const value_json = try sql_value.parseSqlUntypedValueJsonAlloc(alloc, tokens, pos);
    var value_transferred = false;
    errdefer if (!value_transferred) alloc.free(value_json);

    field_transferred = true;
    value_transferred = true;
    return .{ .literal_equals = .{
        .field = field,
        .value_json = value_json,
    } };
}

fn parseRowSecurityPolicyExpressionPredicateAtomAlloc(
    alloc: std.mem.Allocator,
    cursor: parser.Cursor,
    tokens: []const Token,
    pos: *usize,
) !ddl_plan.RowSecurityPolicyPredicate {
    _ = pos;
    _ = tokens;
    const lhs = try parseDdlGeneratedRowExpressionAlloc(alloc, cursor);
    var lhs_transferred = false;
    errdefer if (!lhs_transferred) runtime_schema.freeRelationalRowsExpression(alloc, lhs);

    const op = try parseRowSecurityExpressionComparisonOp(cursor);

    const rhs = try alloc.alloc(db_mod.types.RelationalRowsExpression, 1);
    var rhs_transferred = false;
    errdefer {
        if (!rhs_transferred) alloc.free(rhs);
    }
    rhs[0] = try parseDdlGeneratedRowExpressionAlloc(alloc, cursor);
    errdefer if (!rhs_transferred) runtime_schema.freeRelationalRowsExpression(alloc, rhs[0]);

    lhs_transferred = true;
    rhs_transferred = true;
    return .{ .expression = .{
        .lhs = lhs,
        .op = op,
        .rhs = rhs,
    } };
}

fn parseRowSecurityExpressionComparisonOp(cursor: parser.Cursor) !runtime_schema.RelationalCheckOp {
    if (cursor.matchToken(.eq) != null) return .eq;
    if (cursor.matchToken(.neq) != null) return .ne;
    if (cursor.matchToken(.gt) != null) return .gt;
    if (cursor.matchToken(.gte) != null) return .gte;
    if (cursor.matchToken(.lt) != null) return .lt;
    if (cursor.matchToken(.lte) != null) return .lte;
    return error.UnsupportedSqlShape;
}

fn isSupportedUpdatedAtTriggerFunction(name: []const u8) bool {
    const dot = std.mem.lastIndexOfScalar(u8, name, '.');
    const base = if (dot) |idx| name[idx + 1 ..] else name;
    return std.ascii.eqlIgnoreCase(base, "touch_updated_at") or
        std.ascii.eqlIgnoreCase(base, "set_updated_at") or
        std.ascii.eqlIgnoreCase(base, "update_updated_at") or
        std.ascii.eqlIgnoreCase(base, "antfly_on_update_now") or
        std.ascii.eqlIgnoreCase(base, "antfly_touch_updated_at");
}

pub fn matchAdapterNoopTransactionBoundaryTail(
    tokens: []const Token,
    pos: *usize,
    options: AdapterNoopTransactionBoundaryTail,
) !bool {
    var cursor = parser.Cursor.init(tokens, pos);
    const checkpoint = cursor.checkpoint();
    if (try matchAdapterNoopStatementEnd(cursor)) return true;
    if ((options.work and cursor.matchKeyword("work")) or
        (options.transaction and cursor.matchKeyword("transaction")))
    {
        if (try matchAdapterNoopStatementEnd(cursor)) return true;
    }
    cursor.restore(checkpoint);
    return false;
}

const RowClaimParse = struct {
    clause: ast.SqlRowClaimClause,
    targets: []const []const u8 = &.{},

    fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        for (self.targets) |target| alloc.free(@constCast(target));
        if (self.targets.len > 0) alloc.free(self.targets);
        self.* = undefined;
    }
};

fn parseForRowClaimClauseAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !RowClaimParse {
    var cursor = parser.Cursor.init(tokens, pos);
    const mode: db_mod.types.RowClaimMode = if (cursor.matchKeyword("no")) blk: {
        try cursor.expectKeyword("key");
        try cursor.expectKeyword("update");
        break :blk .for_no_key_update;
    } else if (cursor.matchKeyword("key")) blk: {
        try cursor.expectKeyword("share");
        break :blk .for_key_share;
    } else if (cursor.matchKeyword("share")) blk: {
        break :blk .for_share;
    } else blk: {
        try cursor.expectKeyword("update");
        break :blk .for_update;
    };

    var targets = std.ArrayListUnmanaged([]const u8).empty;
    errdefer {
        for (targets.items) |target| alloc.free(@constCast(target));
        targets.deinit(alloc);
    }
    if (cursor.matchKeyword("of")) {
        while (true) {
            _ = cursor.matchKeyword("only");
            const target = cursor.matchToken(.identifier) orelse return error.UnsupportedSqlShape;
            try targets.append(alloc, try alloc.dupe(u8, target.text));
            if (cursor.matchToken(.comma) == null) break;
        }
    }

    const wait_policy: db_mod.types.RowClaimWaitPolicy = if (cursor.matchKeyword("skip")) blk: {
        try cursor.expectKeyword("locked");
        break :blk .skip_locked;
    } else if (cursor.matchKeyword("nowait"))
        .nowait
    else
        .wait;

    return .{
        .clause = .{ .mode = mode, .wait_policy = wait_policy },
        .targets = try targets.toOwnedSlice(alloc),
    };
}

pub fn rowClaimTargetAllowed(alloc: std.mem.Allocator, target: []const u8, allowed_targets: []const []const u8) bool {
    for (allowed_targets) |allowed| {
        if (allowed.len > 0 and std.mem.eql(u8, target, allowed)) return true;
    }
    const normalized = normalizeSqlObjectIdentifierAlloc(alloc, target) catch return false;
    defer alloc.free(normalized);
    for (allowed_targets) |allowed| {
        if (allowed.len > 0 and std.mem.eql(u8, normalized, allowed)) return true;
    }
    return false;
}

pub fn parseCheckedForRowClaimClauseAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    allowed_targets: []const []const u8,
) !ast.SqlRowClaimClause {
    var syntax = try parseForRowClaimClauseAlloc(alloc, tokens, pos);
    defer syntax.deinit(alloc);
    for (syntax.targets) |target| {
        if (!rowClaimTargetAllowed(alloc, target, allowed_targets)) return error.UnsupportedSqlShape;
    }
    return syntax.clause;
}

pub fn parseExclusiveForRowClaimClauseAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    allowed_targets: []const []const u8,
) !ast.SqlRowClaimClause {
    const clause = try parseCheckedForRowClaimClauseAlloc(alloc, tokens, pos, allowed_targets);
    if (!clause.mode.isExclusiveWriteClaim()) return error.UnsupportedSqlShape;
    return clause;
}

pub fn consumeOptionalDdlNotValid(tokens: []const Token, pos: *usize) bool {
    if (!peekDdlNotValid(tokens, pos.*)) return false;
    pos.* += 2;
    return true;
}

pub fn parseOptionalDdlUniqueNullsDistinct(tokens: []const Token, pos: *usize) !?bool {
    const cursor = parser.Cursor.init(tokens, pos);
    if (!cursor.matchKeyword("nulls")) return null;
    const not_distinct = cursor.matchKeyword("not");
    try cursor.expectKeyword("distinct");
    return not_distinct;
}

pub fn peekDdlNotValid(tokens: []const Token, pos: usize) bool {
    if (pos + 1 >= tokens.len) return false;
    const not_token = tokens[pos];
    const valid_token = tokens[pos + 1];
    return not_token.kind == .identifier and valid_token.kind == .identifier and
        std.ascii.eqlIgnoreCase(not_token.text, "not") and
        std.ascii.eqlIgnoreCase(valid_token.text, "valid");
}

pub fn peekDdlIndexElementExpression(
    tokens: []const Token,
    pos: usize,
    include_generated_expression: bool,
) bool {
    var scan = pos;
    while (scan < tokens.len and tokens[scan].kind == .lparen) : (scan += 1) {}
    if (scan >= tokens.len or tokens[scan].kind != .identifier) return false;
    if (scan + 1 >= tokens.len or tokens[scan + 1].kind != .lparen) return false;
    const token = tokens[scan];
    if (token.matchesKeywordTag(.lower) or
        token.matchesKeywordTag(.upper) or
        expr_token.sqlTokenIsMd5Function(token))
    {
        return true;
    }
    return include_generated_expression;
}

pub fn consumeDdlIndexExpressionWrappers(tokens: []const Token, pos: *usize) usize {
    var count: usize = 0;
    while (pos.* < tokens.len and tokens[pos.*].kind == .lparen) {
        pos.* += 1;
        count += 1;
    }
    return count;
}

pub fn closeDdlIndexExpressionWrappers(tokens: []const Token, pos: *usize, count: usize) !void {
    const cursor = parser.Cursor.init(tokens, pos);
    var remaining = count;
    while (remaining > 0) : (remaining -= 1) {
        try cursor.expectToken(.rparen);
    }
}

pub fn parseDdlUniqueExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !runtime_schema.UniqueExpression {
    const cursor = parser.Cursor.init(tokens, pos);
    const start = cursor.checkpoint();
    const op: ?runtime_schema.UniqueExpressionOp = if (cursor.matchKeyword("lower"))
        .lower
    else if (cursor.matchKeyword("upper"))
        .upper
    else blk: {
        if (cursor.matchIdentifierTokenIf(expr_token.sqlTokenIsMd5Function) == null) break :blk null;
        break :blk .md5;
    };
    if (op) |simple_op| {
        if (cursor.matchToken(.lparen) != null) {
            const field = parseIdentifierOwnedAlloc(alloc, tokens, pos) catch |err| {
                cursor.restore(start);
                if (err == error.OutOfMemory) return err;
                const expression = try parseDdlGeneratedRowExpressionAlloc(alloc, cursor);
                errdefer runtime_schema.freeRelationalRowsExpression(alloc, expression);
                return .{ .op = .expression, .expression = expression };
            };
            var field_transferred = false;
            errdefer if (!field_transferred) alloc.free(field);
            if (cursor.matchToken(.rparen) != null) {
                field_transferred = true;
                return .{ .op = simple_op, .field = field };
            }
            alloc.free(field);
            field_transferred = true;
        }
        cursor.restore(start);
    }

    const expression = try parseDdlGeneratedRowExpressionAlloc(alloc, cursor);
    errdefer runtime_schema.freeRelationalRowsExpression(alloc, expression);
    return .{ .op = .expression, .expression = expression };
}

pub fn parseDdlGeneratedExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !runtime_schema.RelationalGeneratedValue {
    const cursor = parser.Cursor.init(tokens, pos);
    const start = cursor.checkpoint();
    if (cursor.peekKeywordTag(.lower) or cursor.peekKeywordTag(.upper) or cursor.peekFunctionCallTokenIf(expr_token.sqlTokenIsMd5Function)) {
        const op: runtime_schema.RelationalGeneratedOp = if (cursor.matchKeywordTag(.lower))
            .lower
        else if (cursor.matchKeywordTag(.upper))
            .upper
        else blk: {
            if (cursor.matchIdentifierTokenIf(expr_token.sqlTokenIsMd5Function) == null) return error.UnsupportedSqlShape;
            break :blk .md5;
        };
        try cursor.expectToken(.lparen);
        const field = try parseIdentifierOwnedAlloc(alloc, tokens, pos);
        var field_transferred = false;
        errdefer if (!field_transferred) alloc.free(field);
        try cursor.expectToken(.rparen);
        const separator = try alloc.dupe(u8, "");
        var separator_transferred = false;
        errdefer if (!separator_transferred) alloc.free(separator);
        field_transferred = true;
        separator_transferred = true;
        return .{ .op = op, .field = field, .separator = separator };
    }
    if (cursor.matchKeyword("concat")) return parseDdlGeneratedConcatExpressionAlloc(alloc, cursor, tokens, pos, .concat) catch |err| {
        cursor.restore(start);
        if (err != error.UnsupportedSqlShape) return err;
        return try parseDdlGeneratedRowExpressionGeneratedValueAlloc(alloc, cursor);
    };
    if (cursor.matchKeyword("concat_ws")) return parseDdlGeneratedConcatExpressionAlloc(alloc, cursor, tokens, pos, .concat_ws) catch |err| {
        cursor.restore(start);
        if (err != error.UnsupportedSqlShape) return err;
        return try parseDdlGeneratedRowExpressionGeneratedValueAlloc(alloc, cursor);
    };
    cursor.restore(start);
    return try parseDdlGeneratedRowExpressionGeneratedValueAlloc(alloc, cursor);
}

fn parseDdlGeneratedRowExpressionGeneratedValueAlloc(
    alloc: std.mem.Allocator,
    cursor: parser.Cursor,
) !runtime_schema.RelationalGeneratedValue {
    const expression = try parseDdlGeneratedRowExpressionAlloc(alloc, cursor);
    var expression_transferred = false;
    errdefer if (!expression_transferred) runtime_schema.freeRelationalRowsExpression(alloc, expression);
    const separator = try alloc.dupe(u8, "");
    var separator_transferred = false;
    errdefer if (!separator_transferred) alloc.free(separator);
    expression_transferred = true;
    separator_transferred = true;
    return .{
        .op = .expression,
        .separator = separator,
        .expression = expression,
    };
}

pub fn parseDdlGeneratedRowExpressionAlloc(
    alloc: std.mem.Allocator,
    cursor: parser.Cursor,
) anyerror!runtime_schema.RelationalRowsExpression {
    return try parseDdlGeneratedConcatPipeExpressionAlloc(alloc, cursor);
}

fn parseDdlGeneratedConcatPipeExpressionAlloc(
    alloc: std.mem.Allocator,
    cursor: parser.Cursor,
) anyerror!runtime_schema.RelationalRowsExpression {
    var expression = try parseDdlGeneratedAdditiveExpressionAlloc(alloc, cursor);
    var expression_transferred = false;
    errdefer if (!expression_transferred) runtime_schema.freeRelationalRowsExpression(alloc, expression);
    while (cursor.matchToken(.pipe_concat) != null) {
        const rhs = try parseDdlGeneratedAdditiveExpressionAlloc(alloc, cursor);
        var rhs_transferred = false;
        errdefer if (!rhs_transferred) runtime_schema.freeRelationalRowsExpression(alloc, rhs);
        const operands = try alloc.alloc(runtime_schema.RelationalRowsExpression, 2);
        operands[0] = expression;
        operands[1] = rhs;
        expression_transferred = true;
        rhs_transferred = true;
        expression = try ddlGeneratedOperationExpressionAlloc(alloc, .concat, operands);
        expression_transferred = false;
    }
    expression_transferred = true;
    return expression;
}

fn parseDdlGeneratedAdditiveExpressionAlloc(
    alloc: std.mem.Allocator,
    cursor: parser.Cursor,
) anyerror!runtime_schema.RelationalRowsExpression {
    var expression = try parseDdlGeneratedMultiplicativeExpressionAlloc(alloc, cursor);
    var expression_transferred = false;
    errdefer if (!expression_transferred) runtime_schema.freeRelationalRowsExpression(alloc, expression);
    while (true) {
        const kind: runtime_schema.RelationalRowsExpressionKind = if (cursor.matchToken(.plus) != null)
            .add
        else if (cursor.matchToken(.minus) != null)
            .sub
        else
            break;
        const rhs = try parseDdlGeneratedMultiplicativeExpressionAlloc(alloc, cursor);
        var rhs_transferred = false;
        errdefer if (!rhs_transferred) runtime_schema.freeRelationalRowsExpression(alloc, rhs);
        const operands = try alloc.alloc(runtime_schema.RelationalRowsExpression, 2);
        operands[0] = expression;
        operands[1] = rhs;
        expression_transferred = true;
        rhs_transferred = true;
        expression = try ddlGeneratedOperationExpressionAlloc(alloc, kind, operands);
        expression_transferred = false;
    }
    expression_transferred = true;
    return expression;
}

fn parseDdlGeneratedMultiplicativeExpressionAlloc(
    alloc: std.mem.Allocator,
    cursor: parser.Cursor,
) anyerror!runtime_schema.RelationalRowsExpression {
    var expression = try parseDdlGeneratedPrimaryExpressionAlloc(alloc, cursor);
    var expression_transferred = false;
    errdefer if (!expression_transferred) runtime_schema.freeRelationalRowsExpression(alloc, expression);
    while (true) {
        const kind: runtime_schema.RelationalRowsExpressionKind = if (cursor.matchToken(.star) != null)
            .mul
        else if (cursor.matchToken(.slash) != null)
            .div
        else if (cursor.matchToken(.percent) != null)
            .mod
        else
            break;
        const rhs = try parseDdlGeneratedPrimaryExpressionAlloc(alloc, cursor);
        var rhs_transferred = false;
        errdefer if (!rhs_transferred) runtime_schema.freeRelationalRowsExpression(alloc, rhs);
        const operands = try alloc.alloc(runtime_schema.RelationalRowsExpression, 2);
        operands[0] = expression;
        operands[1] = rhs;
        expression_transferred = true;
        rhs_transferred = true;
        expression = try ddlGeneratedOperationExpressionAlloc(alloc, kind, operands);
        expression_transferred = false;
    }
    expression_transferred = true;
    return expression;
}

fn parseDdlGeneratedPrimaryExpressionAlloc(
    alloc: std.mem.Allocator,
    cursor: parser.Cursor,
) anyerror!runtime_schema.RelationalRowsExpression {
    if (cursor.matchToken(.lparen) != null) {
        const expression = try parseDdlGeneratedRowExpressionAlloc(alloc, cursor);
        var expression_transferred = false;
        errdefer if (!expression_transferred) runtime_schema.freeRelationalRowsExpression(alloc, expression);
        try cursor.expectToken(.rparen);
        expression_transferred = true;
        return expression;
    }
    if (cursor.matchToken(.minus) != null) {
        const number = cursor.matchToken(.number) orelse return error.UnsupportedSqlShape;
        const value_json = try std.fmt.allocPrint(alloc, "-{s}", .{number.text});
        return try ddlGeneratedValueExpressionWithOwnedJsonAlloc(alloc, value_json);
    }
    if (cursor.peekFunctionCallTag(.cast)) return try parseDdlGeneratedCastExpressionAlloc(alloc, cursor);
    if (cursor.peekFunctionCallIf(expr_token.sqlKeywordIsJsonExtractPathFunction)) return try parseDdlGeneratedJsonExtractPathExpressionAlloc(alloc, cursor);
    if (cursor.peekKind(.identifier) and cursor.pos.* + 1 < cursor.tokens.len and cursor.tokens[cursor.pos.* + 1].kind == .lparen) {
        return try parseDdlGeneratedFunctionExpressionAlloc(alloc, cursor);
    }
    const literal_start = cursor.checkpoint();
    if (sql_value.parseSqlUntypedValueJsonAlloc(alloc, cursor.tokens, cursor.pos)) |value_json| {
        return try ddlGeneratedValueExpressionWithOwnedJsonAlloc(alloc, value_json);
    } else |_| {
        cursor.restore(literal_start);
    }
    const field = cursor.matchToken(.identifier) orelse return error.UnsupportedSqlShape;
    return try ddlGeneratedFieldExpressionAlloc(alloc, field.text);
}

fn parseDdlGeneratedFunctionExpressionAlloc(
    alloc: std.mem.Allocator,
    cursor: parser.Cursor,
) !runtime_schema.RelationalRowsExpression {
    const name = cursor.matchToken(.identifier) orelse return error.UnsupportedSqlShape;
    const kind = ddlGeneratedFunctionExpressionKind(name.text) orelse return error.UnsupportedSqlShape;
    try cursor.expectToken(.lparen);
    var operands = std.ArrayListUnmanaged(runtime_schema.RelationalRowsExpression).empty;
    errdefer freeDdlGeneratedExpressionList(alloc, &operands);
    if (cursor.matchToken(.rparen) == null) {
        while (true) {
            const operand = try parseDdlGeneratedRowExpressionAlloc(alloc, cursor);
            try operands.append(alloc, operand);
            if (cursor.matchToken(.comma) == null) break;
        }
        try cursor.expectToken(.rparen);
    }
    if (operands.items.len == 0) return error.UnsupportedSqlShape;
    const owned_operands = try operands.toOwnedSlice(alloc);
    return try ddlGeneratedOperationExpressionAlloc(alloc, kind, owned_operands);
}

fn parseDdlGeneratedCastExpressionAlloc(
    alloc: std.mem.Allocator,
    cursor: parser.Cursor,
) !runtime_schema.RelationalRowsExpression {
    try cursor.expectKeyword("cast");
    try cursor.expectToken(.lparen);
    const operand = try parseDdlGeneratedRowExpressionAlloc(alloc, cursor);
    var operand_transferred = false;
    errdefer if (!operand_transferred) runtime_schema.freeRelationalRowsExpression(alloc, operand);
    try cursor.expectKeyword("as");
    const type_token = cursor.matchToken(.identifier) orelse return error.UnsupportedSqlShape;
    const cast_type = ddlGeneratedCastType(type_token.text) orelse return error.UnsupportedSqlShape;
    try cursor.expectToken(.rparen);
    const operands = try alloc.alloc(runtime_schema.RelationalRowsExpression, 1);
    operands[0] = operand;
    operand_transferred = true;
    return try ddlGeneratedCastExpressionAlloc(alloc, operands, cast_type);
}

fn parseDdlGeneratedJsonExtractPathExpressionAlloc(
    alloc: std.mem.Allocator,
    cursor: parser.Cursor,
) !runtime_schema.RelationalRowsExpression {
    const function_name = cursor.matchToken(.identifier) orelse return error.UnsupportedSqlShape;
    const as_text = std.ascii.eqlIgnoreCase(function_name.text, "json_extract_path_text") or
        std.ascii.eqlIgnoreCase(function_name.text, "jsonb_extract_path_text");
    try cursor.expectToken(.lparen);
    const root = try parseDdlGeneratedRowExpressionAlloc(alloc, cursor);
    var root_transferred = false;
    errdefer if (!root_transferred) runtime_schema.freeRelationalRowsExpression(alloc, root);
    try cursor.expectToken(.comma);
    var path = std.ArrayListUnmanaged(u8).empty;
    errdefer path.deinit(alloc);
    while (true) {
        const path_part = cursor.matchToken(.string) orelse return error.UnsupportedSqlShape;
        if (path.items.len != 0) try path.append(alloc, '.');
        try path.appendSlice(alloc, path_part.text);
        if (cursor.matchToken(.comma) == null) break;
    }
    try cursor.expectToken(.rparen);
    const operands = try alloc.alloc(runtime_schema.RelationalRowsExpression, 1);
    operands[0] = root;
    root_transferred = true;
    const owned_path = try path.toOwnedSlice(alloc);
    return try ddlGeneratedJsonExtractExpressionAlloc(alloc, operands, owned_path, as_text);
}

fn ddlGeneratedFunctionExpressionKind(name: []const u8) ?runtime_schema.RelationalRowsExpressionKind {
    if (std.ascii.eqlIgnoreCase(name, "lower")) return .lower;
    if (std.ascii.eqlIgnoreCase(name, "upper")) return .upper;
    if (std.ascii.eqlIgnoreCase(name, "initcap")) return .initcap;
    if (std.ascii.eqlIgnoreCase(name, "trim") or std.ascii.eqlIgnoreCase(name, "btrim")) return .trim;
    if (std.ascii.eqlIgnoreCase(name, "ltrim")) return .ltrim;
    if (std.ascii.eqlIgnoreCase(name, "rtrim")) return .rtrim;
    if (std.ascii.eqlIgnoreCase(name, "replace")) return .replace;
    if (std.ascii.eqlIgnoreCase(name, "regexp_replace")) return .regexp_replace;
    if (std.ascii.eqlIgnoreCase(name, "translate")) return .translate;
    if (std.ascii.eqlIgnoreCase(name, "substring") or std.ascii.eqlIgnoreCase(name, "substr")) return .substring;
    if (std.ascii.eqlIgnoreCase(name, "overlay")) return .overlay;
    if (std.ascii.eqlIgnoreCase(name, "split_part")) return .split_part;
    if (std.ascii.eqlIgnoreCase(name, "strpos")) return .strpos;
    if (std.ascii.eqlIgnoreCase(name, "left")) return .left;
    if (std.ascii.eqlIgnoreCase(name, "right")) return .right;
    if (std.ascii.eqlIgnoreCase(name, "lpad")) return .lpad;
    if (std.ascii.eqlIgnoreCase(name, "rpad")) return .rpad;
    if (std.ascii.eqlIgnoreCase(name, "repeat")) return .repeat;
    if (std.ascii.eqlIgnoreCase(name, "reverse")) return .reverse;
    if (std.ascii.eqlIgnoreCase(name, "starts_with")) return .starts_with;
    if (std.ascii.eqlIgnoreCase(name, "ends_with")) return .ends_with;
    if (std.ascii.eqlIgnoreCase(name, "ascii")) return .ascii;
    if (std.ascii.eqlIgnoreCase(name, "chr")) return .chr;
    if (std.ascii.eqlIgnoreCase(name, "md5")) return .md5;
    if (std.ascii.eqlIgnoreCase(name, "soundex")) return .soundex;
    if (std.ascii.eqlIgnoreCase(name, "concat")) return .concat;
    if (std.ascii.eqlIgnoreCase(name, "concat_ws")) return .concat_ws;
    if (std.ascii.eqlIgnoreCase(name, "length") or std.ascii.eqlIgnoreCase(name, "char_length") or std.ascii.eqlIgnoreCase(name, "character_length")) return .length;
    if (std.ascii.eqlIgnoreCase(name, "octet_length")) return .octet_length;
    if (std.ascii.eqlIgnoreCase(name, "bit_length")) return .bit_length;
    if (std.ascii.eqlIgnoreCase(name, "coalesce")) return .coalesce;
    if (std.ascii.eqlIgnoreCase(name, "nullif")) return .nullif;
    if (std.ascii.eqlIgnoreCase(name, "greatest")) return .greatest;
    if (std.ascii.eqlIgnoreCase(name, "least")) return .least;
    if (std.ascii.eqlIgnoreCase(name, "abs")) return .abs;
    if (std.ascii.eqlIgnoreCase(name, "round")) return .round;
    if (std.ascii.eqlIgnoreCase(name, "trunc")) return .trunc;
    if (std.ascii.eqlIgnoreCase(name, "floor")) return .floor;
    if (std.ascii.eqlIgnoreCase(name, "ceil")) return .ceil;
    if (std.ascii.eqlIgnoreCase(name, "sqrt")) return .sqrt;
    if (std.ascii.eqlIgnoreCase(name, "sign")) return .sign;
    if (std.ascii.eqlIgnoreCase(name, "power")) return .power;
    if (std.ascii.eqlIgnoreCase(name, "mod")) return .mod;
    if (std.ascii.eqlIgnoreCase(name, "date_trunc")) return .date_trunc;
    if (std.ascii.eqlIgnoreCase(name, "date_bin")) return .date_bin;
    if (std.ascii.eqlIgnoreCase(name, "date_part")) return .date_part;
    if (std.ascii.eqlIgnoreCase(name, "json_typeof") or std.ascii.eqlIgnoreCase(name, "jsonb_typeof")) return .json_typeof;
    if (std.ascii.eqlIgnoreCase(name, "json_array_length") or std.ascii.eqlIgnoreCase(name, "jsonb_array_length")) return .json_array_length;
    if (std.ascii.eqlIgnoreCase(name, "json_build_object") or std.ascii.eqlIgnoreCase(name, "jsonb_build_object")) return .json_build_object;
    if (std.ascii.eqlIgnoreCase(name, "to_jsonb")) return .to_jsonb;
    if (std.ascii.eqlIgnoreCase(name, "array_length") or std.ascii.eqlIgnoreCase(name, "cardinality")) return .array_length;
    if (std.ascii.eqlIgnoreCase(name, "array_position")) return .array_position;
    if (std.ascii.eqlIgnoreCase(name, "array_positions")) return .array_positions;
    if (std.ascii.eqlIgnoreCase(name, "array_append")) return .array_append;
    if (std.ascii.eqlIgnoreCase(name, "array_prepend")) return .array_prepend;
    if (std.ascii.eqlIgnoreCase(name, "array_cat")) return .array_cat;
    if (std.ascii.eqlIgnoreCase(name, "array_remove")) return .array_remove;
    if (std.ascii.eqlIgnoreCase(name, "array_replace")) return .array_replace;
    if (std.ascii.eqlIgnoreCase(name, "array_to_string")) return .array_to_string;
    if (std.ascii.eqlIgnoreCase(name, "string_to_array")) return .string_to_array;
    return null;
}

fn ddlGeneratedCastType(name: []const u8) ?runtime_schema.RelationalRowsExpressionCastType {
    if (std.ascii.eqlIgnoreCase(name, "text") or std.ascii.eqlIgnoreCase(name, "varchar") or std.ascii.eqlIgnoreCase(name, "uuid")) return .text;
    if (std.ascii.eqlIgnoreCase(name, "numeric") or std.ascii.eqlIgnoreCase(name, "integer") or std.ascii.eqlIgnoreCase(name, "int") or std.ascii.eqlIgnoreCase(name, "bigint")) return .numeric;
    if (std.ascii.eqlIgnoreCase(name, "boolean") or std.ascii.eqlIgnoreCase(name, "bool")) return .bool;
    if (std.ascii.eqlIgnoreCase(name, "timestamp") or std.ascii.eqlIgnoreCase(name, "timestamptz") or std.ascii.eqlIgnoreCase(name, "datetime")) return .datetime;
    return null;
}

fn ddlGeneratedFieldExpressionAlloc(alloc: std.mem.Allocator, field: []const u8) !runtime_schema.RelationalRowsExpression {
    return .{
        .kind = .field,
        .field = try alloc.dupe(u8, field),
        .value_json = try alloc.dupe(u8, ""),
        .json_path = try alloc.dupe(u8, ""),
    };
}

fn ddlGeneratedValueExpressionWithOwnedJsonAlloc(
    alloc: std.mem.Allocator,
    value_json: []const u8,
) !runtime_schema.RelationalRowsExpression {
    errdefer alloc.free(@constCast(value_json));
    return .{
        .kind = .value,
        .field = try alloc.dupe(u8, ""),
        .value_json = value_json,
        .json_path = try alloc.dupe(u8, ""),
    };
}

fn ddlGeneratedOperationExpressionAlloc(
    alloc: std.mem.Allocator,
    kind: runtime_schema.RelationalRowsExpressionKind,
    operands: []const runtime_schema.RelationalRowsExpression,
) !runtime_schema.RelationalRowsExpression {
    errdefer {
        for (operands) |operand| runtime_schema.freeRelationalRowsExpression(alloc, operand);
        alloc.free(@constCast(operands));
    }
    return .{
        .kind = kind,
        .field = try alloc.dupe(u8, ""),
        .value_json = try alloc.dupe(u8, ""),
        .json_path = try alloc.dupe(u8, ""),
        .operands = operands,
    };
}

fn ddlGeneratedCastExpressionAlloc(
    alloc: std.mem.Allocator,
    operands: []const runtime_schema.RelationalRowsExpression,
    cast_type: runtime_schema.RelationalRowsExpressionCastType,
) !runtime_schema.RelationalRowsExpression {
    var expression = try ddlGeneratedOperationExpressionAlloc(alloc, .cast, operands);
    expression.cast_type = cast_type;
    return expression;
}

fn ddlGeneratedJsonExtractExpressionAlloc(
    alloc: std.mem.Allocator,
    operands: []const runtime_schema.RelationalRowsExpression,
    json_path: []const u8,
    as_text: bool,
) !runtime_schema.RelationalRowsExpression {
    errdefer {
        for (operands) |operand| runtime_schema.freeRelationalRowsExpression(alloc, operand);
        alloc.free(@constCast(operands));
        alloc.free(@constCast(json_path));
    }
    return .{
        .kind = .json_extract,
        .field = try alloc.dupe(u8, ""),
        .value_json = try alloc.dupe(u8, ""),
        .json_path = json_path,
        .json_as_text = as_text,
        .operands = operands,
    };
}

fn freeDdlGeneratedExpressionList(
    alloc: std.mem.Allocator,
    list: *std.ArrayListUnmanaged(runtime_schema.RelationalRowsExpression),
) void {
    for (list.items) |expression| runtime_schema.freeRelationalRowsExpression(alloc, expression);
    list.deinit(alloc);
}

fn parseDdlGeneratedConcatExpressionAlloc(
    alloc: std.mem.Allocator,
    cursor: parser.Cursor,
    tokens: []const Token,
    pos: *usize,
    op: runtime_schema.RelationalGeneratedOp,
) !runtime_schema.RelationalGeneratedValue {
    try cursor.expectToken(.lparen);
    var fields = std.ArrayListUnmanaged([]const u8).empty;
    errdefer freeStringList(alloc, &fields);

    var separator: ?[]const u8 = null;
    errdefer if (separator) |value| alloc.free(@constCast(value));
    if (op == .concat_ws) {
        const separator_token = cursor.matchToken(.string) orelse return error.UnsupportedSqlShape;
        separator = try alloc.dupe(u8, separator_token.text);
        try cursor.expectToken(.comma);
    }

    const first = try parseIdentifierOwnedAlloc(alloc, tokens, pos);
    var first_transferred = false;
    errdefer if (!first_transferred) alloc.free(first);
    try fields.append(alloc, first);
    first_transferred = true;

    while (cursor.matchToken(.comma) != null) {
        if (op == .concat) {
            if (cursor.matchToken(.string)) |token| {
                if (separator) |existing| {
                    if (!std.mem.eql(u8, existing, token.text)) return error.UnsupportedSqlShape;
                } else {
                    separator = try alloc.dupe(u8, token.text);
                }
                try cursor.expectToken(.comma);
            }
        } else if (separator == null) {
            separator = try alloc.dupe(u8, "");
        }
        const field = try parseIdentifierOwnedAlloc(alloc, tokens, pos);
        var field_transferred = false;
        errdefer if (!field_transferred) alloc.free(field);
        try fields.append(alloc, field);
        field_transferred = true;
    }
    try cursor.expectToken(.rparen);
    if (fields.items.len < 2) return error.UnsupportedSqlShape;

    const owned_fields = try fields.toOwnedSlice(alloc);
    var fields_transferred = false;
    errdefer if (!fields_transferred) freeStringSlice(alloc, owned_fields);
    const owned_separator = separator orelse try alloc.dupe(u8, "");
    separator = null;
    fields_transferred = true;
    return .{ .op = op, .fields = owned_fields, .separator = owned_separator };
}

pub fn parseOptionalDdlCollationAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !?[]const u8 {
    const cursor = parser.Cursor.init(tokens, pos);
    if (!cursor.matchKeyword("collate")) return null;
    const first = cursor.matchToken(.identifier) orelse return error.UnsupportedSqlShape;
    if (std.mem.endsWith(u8, first.text, ".")) {
        const second = cursor.matchToken(.identifier) orelse return error.UnsupportedSqlShape;
        return try std.fmt.allocPrint(alloc, "{s}{s}", .{ first.text, second.text });
    }
    return try alloc.dupe(u8, first.text);
}

fn parseOptionalIfNotExists(cursor: parser.Cursor) !bool {
    if (!cursor.matchKeyword("if")) return false;
    try cursor.expectKeyword("not");
    try cursor.expectKeyword("exists");
    return true;
}

pub fn isRoleAliasKeywordToken(token: Token) bool {
    return token.matchesKeywordTag(.role) or
        token.matchesKeyword("user") or
        token.matchesKeywordTag(.group);
}

pub fn parseSelectSetOperation(tokens: []const Token, pos: *usize) !ast.SelectSetOperation {
    const cursor = parser.Cursor.init(tokens, pos);
    const op: ast.SelectSetOperation = if (cursor.matchKeyword("union")) blk: {
        if (cursor.matchKeyword("all")) break :blk .union_all;
        break :blk .union_distinct;
    } else if (cursor.matchKeyword("intersect"))
        .intersect
    else if (cursor.matchKeyword("except"))
        .except
    else
        return error.UnsupportedSqlShape;
    _ = cursor.matchKeyword("distinct");
    return op;
}

pub fn nextIsSelectSetOperationKeyword(tokens: []const Token, pos: usize) bool {
    return parser.peekKeywordTag(tokens, pos, .@"union") or
        parser.peekKeywordTag(tokens, pos, .intersect) or
        parser.peekKeywordTag(tokens, pos, .except);
}

pub fn peekUpdateJoinedMutationSourceAlias(tokens: []const Token, pos: usize) ?[]const u8 {
    var depth: usize = 0;
    var i = pos;
    while (i < tokens.len) : (i += 1) {
        const token = tokens[i];
        switch (token.kind) {
            .lparen => depth += 1,
            .rparen => {
                if (depth == 0) return null;
                depth -= 1;
            },
            .identifier => {
                if (depth != 0 or !token.matchesKeywordTag(.from)) continue;
                var j = i + 1;
                if (j >= tokens.len or tokens[j].kind != .identifier) return null;
                const table_name = tokens[j].text;
                j += 1;
                if (j < tokens.len and tokens[j].matchesKeywordTag(.as)) {
                    j += 1;
                }
                if (j < tokens.len and tokens[j].kind == .identifier and !expr_token.sqlJoinedSourceAliasTerminatorToken(tokens[j])) {
                    return tokens[j].text;
                }
                return table_name;
            },
            else => {},
        }
    }
    return null;
}

pub fn peekStaticToJsonbValue(tokens: []const Token, pos: usize) bool {
    if (parser.peekKeyword(tokens, pos, "null") or
        parser.peekKeyword(tokens, pos, "true") or
        parser.peekKeyword(tokens, pos, "false"))
    {
        return true;
    }
    if (pos >= tokens.len) return false;
    if (tokens[pos].kind == .placeholder or tokens[pos].kind == .string or tokens[pos].kind == .number) return true;
    return tokens[pos].kind == .minus and pos + 1 < tokens.len and tokens[pos + 1].kind == .number;
}

pub fn peekArrayTransformSelfAssignment(tokens: []const Token, pos: usize, field: []const u8) bool {
    if (!(parser.peekKeyword(tokens, pos, "array_append") or parser.peekKeyword(tokens, pos, "array_remove"))) return false;
    if (pos + 3 >= tokens.len) return false;
    return tokens[pos + 1].kind == .lparen and
        tokens[pos + 2].kind == .identifier and
        std.mem.eql(u8, tokens[pos + 2].text, field) and
        tokens[pos + 3].kind == .comma;
}

pub fn matchArrayTransformUpdateOp(tokens: []const Token, pos: *usize) ?db_mod.types.TransformOpType {
    if (parser.matchKeyword(tokens, pos, "array_append")) return .push;
    if (parser.matchKeyword(tokens, pos, "array_remove")) return .pull;
    return null;
}

pub fn peekDdlRangeColumnDefinition(tokens: []const Token, pos: usize) bool {
    if (pos + 1 >= tokens.len) return false;
    return tokens[pos].kind == .identifier and
        tokens[pos + 1].kind == .identifier and
        ddl_plan.ddlRangeBoundTypeForName(tokens[pos + 1].text) != null;
}

pub fn nextIsSelectSetResultTailKeyword(tokens: []const Token, pos: usize) bool {
    return parser.peekKeyword(tokens, pos, "order") or
        parser.peekKeyword(tokens, pos, "limit") or
        parser.peekKeyword(tokens, pos, "offset") or
        parser.peekKeyword(tokens, pos, "fetch") or
        (pos < tokens.len and tokens[pos].kind == .semicolon);
}

pub fn normalizeSqlObjectIdentifierAlloc(alloc: std.mem.Allocator, identifier: []const u8) ![]const u8 {
    const dot = std.mem.indexOfScalar(u8, identifier, '.') orelse return try alloc.dupe(u8, identifier);
    if (dot == 0) return error.UnsupportedSqlShape;
    const object_name = identifier[dot + 1 ..];
    if (object_name.len == 0 or std.mem.indexOfScalar(u8, object_name, '.') != null) return error.UnsupportedSqlShape;
    if (!std.ascii.eqlIgnoreCase(identifier[0..dot], "public")) return try alloc.dupe(u8, identifier);
    return try alloc.dupe(u8, object_name);
}

pub fn normalizeSqlSchemaIdentifierAlloc(alloc: std.mem.Allocator, identifier: []const u8) ![]const u8 {
    if (identifier.len == 0) return error.UnsupportedSqlShape;
    const dot = std.mem.indexOfScalar(u8, identifier, '.') orelse return try alloc.dupe(u8, identifier);
    if (dot == 0) return error.UnsupportedSqlShape;
    const schema_name = identifier[dot + 1 ..];
    if (schema_name.len == 0 or std.mem.indexOfScalar(u8, schema_name, '.') != null) return error.UnsupportedSqlShape;
    return try alloc.dupe(u8, identifier);
}

pub fn parseIdentifierOwnedAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) ![]const u8 {
    const token = parser.matchToken(tokens, pos, .identifier) orelse return error.UnsupportedSqlShape;
    return try alloc.dupe(u8, token.text);
}

pub fn parseOptionalProjectionAliasAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !?[]const u8 {
    if (!parser.matchKeyword(tokens, pos, "as")) return null;
    return try parseIdentifierOwnedAlloc(alloc, tokens, pos);
}

pub fn parseProjectionOutputOwnedAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    default_output: []const u8,
) ![]const u8 {
    return (try parseOptionalProjectionAliasAlloc(alloc, tokens, pos)) orelse
        try alloc.dupe(u8, default_output);
}

pub fn parseRequiredAliasAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) ![]const u8 {
    _ = parser.matchKeyword(tokens, pos, "as");
    return try parseIdentifierOwnedAlloc(alloc, tokens, pos);
}

pub fn consumeProjectionAlias(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    field: []const u8,
) !void {
    const alias = (try parseOptionalProjectionAliasAlloc(alloc, tokens, pos)) orelse return;
    defer alloc.free(alias);
    if (!std.mem.eql(u8, alias, field)) return error.UnsupportedSqlShape;
}

pub fn parseSqlStringLiteralValueAlloc(alloc: std.mem.Allocator, cursor: parser.Cursor) ![]const u8 {
    const token = cursor.matchToken(.string) orelse return error.UnsupportedSqlShape;
    return try alloc.dupe(u8, token.text);
}

pub fn parseIdentifierListAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) ![]const []const u8 {
    var out = std.ArrayListUnmanaged([]const u8).empty;
    errdefer {
        for (out.items) |item| alloc.free(item);
        out.deinit(alloc);
    }
    while (true) {
        try out.append(alloc, try parseIdentifierOwnedAlloc(alloc, tokens, pos));
        if (parser.matchToken(tokens, pos, .comma) == null) break;
    }
    return try out.toOwnedSlice(alloc);
}

pub fn validateSqlIdentifierListUnique(columns: []const []const u8) !void {
    for (columns, 0..) |lhs, i| {
        for (columns[i + 1 ..]) |rhs| {
            if (std.ascii.eqlIgnoreCase(lhs, rhs)) return error.UnsupportedSqlShape;
        }
    }
}

pub fn validateSqlIdentifierListsDisjoint(left: []const []const u8, right: []const []const u8) !void {
    for (left) |lhs| {
        for (right) |rhs| {
            if (std.ascii.eqlIgnoreCase(lhs, rhs)) return error.UnsupportedSqlShape;
        }
    }
}

pub fn parseOptionalCteColumnAliasesAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) ![]const []const u8 {
    if (parser.matchToken(tokens, pos, .lparen) == null) return &.{};

    var aliases = std.ArrayListUnmanaged([]const u8).empty;
    errdefer freeStringList(alloc, &aliases);
    while (true) {
        const alias = try parseIdentifierOwnedAlloc(alloc, tokens, pos);
        var alias_transferred = false;
        errdefer if (!alias_transferred) alloc.free(@constCast(alias));
        for (aliases.items) |existing| {
            if (std.ascii.eqlIgnoreCase(existing, alias)) return error.UnsupportedSqlShape;
        }
        try aliases.append(alloc, alias);
        alias_transferred = true;
        if (parser.matchToken(tokens, pos, .comma) == null) break;
    }

    try parser.expectToken(tokens, pos, .rparen);
    return try aliases.toOwnedSlice(alloc);
}

fn parseParenthesizedIdentifierListAlloc(
    alloc: std.mem.Allocator,
    cursor: parser.Cursor,
    tokens: []const Token,
    pos: *usize,
) ![]const []const u8 {
    try cursor.expectToken(.lparen);
    const values = try parseIdentifierListAlloc(alloc, tokens, pos);
    errdefer freeStringSlice(alloc, values);
    try cursor.expectToken(.rparen);
    return values;
}

pub fn parseSqlObjectIdentifierOwnedAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) ![]const u8 {
    const token = parser.matchToken(tokens, pos, .identifier) orelse return error.UnsupportedSqlShape;
    return try normalizeSqlObjectIdentifierAlloc(alloc, token.text);
}

pub fn parseSqlObjectIdentifierListAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) ![]const []const u8 {
    var out = std.ArrayListUnmanaged([]const u8).empty;
    errdefer {
        for (out.items) |item| alloc.free(item);
        out.deinit(alloc);
    }
    while (true) {
        try out.append(alloc, try parseSqlObjectIdentifierOwnedAlloc(alloc, tokens, pos));
        if (parser.matchToken(tokens, pos, .comma) == null) break;
    }
    return try out.toOwnedSlice(alloc);
}

pub fn parseDdlUniquePredicatesAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) ![]const runtime_schema.UniquePredicate {
    var predicates = std.ArrayListUnmanaged(runtime_schema.UniquePredicate).empty;
    errdefer {
        for (predicates.items) |predicate| freeDdlUniquePredicate(alloc, predicate);
        predicates.deinit(alloc);
    }
    while (true) {
        const atom_start = pos.*;
        var depth: usize = 0;
        while (pos.* < tokens.len) {
            const token = tokens[pos.*];
            if (depth == 0 and token.matchesKeywordTag(.@"and")) break;
            if (depth == 0 and token.kind == .semicolon) break;
            switch (token.kind) {
                .lparen => depth += 1,
                .rparen => {
                    if (depth == 0) return error.UnsupportedSqlShape;
                    depth -= 1;
                },
                else => {},
            }
            pos.* += 1;
        }
        if (depth != 0 or atom_start == pos.*) return error.UnsupportedSqlShape;

        const predicate = try parseDdlUniquePredicateAtomAlloc(alloc, tokens[atom_start..pos.*]);
        var predicate_transferred = false;
        errdefer if (!predicate_transferred) freeDdlUniquePredicate(alloc, predicate);
        try predicates.append(alloc, predicate);
        predicate_transferred = true;
        if (!parser.matchKeywordTag(tokens, pos, .@"and")) break;
    }
    return try predicates.toOwnedSlice(alloc);
}

pub fn uniquePredicateWhereJsonAlloc(
    alloc: std.mem.Allocator,
    predicates: []const runtime_schema.UniquePredicate,
) ![]const u8 {
    if (predicates.len == 0) return "";
    var out: std.Io.Writer.Allocating = .init(alloc);
    errdefer out.deinit();
    const writer = &out.writer;
    try writer.writeAll("{\"all\":[");
    for (predicates, 0..) |predicate, i| {
        if (i != 0) try writer.writeByte(',');
        try writer.print("{{\"field\":{f},\"op\":{f}", .{
            std.json.fmt(predicate.field, .{}),
            std.json.fmt(expr_type.uniquePredicateOpToken(predicate.op), .{}),
        });
        if (predicate.value_json) |value_json| {
            try writer.writeAll(",\"value\":");
            try writer.writeAll(value_json);
        }
        try writer.writeByte('}');
    }
    try writer.writeAll("]}");
    return try out.toOwnedSlice();
}

pub fn parseDdlUniquePredicateWhereJsonAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    columns: []const runtime_schema.RelationalColumn,
) ![]const u8 {
    var predicates = std.ArrayListUnmanaged(runtime_schema.UniquePredicate).empty;
    defer {
        for (predicates.items) |predicate| freeDdlUniquePredicate(alloc, predicate);
        predicates.deinit(alloc);
    }
    while (true) {
        const atom_start = pos.*;
        var depth: usize = 0;
        while (pos.* < tokens.len) {
            const token = tokens[pos.*];
            if (depth == 0 and (token.matchesKeywordTag(.@"and") or token.matchesKeywordTag(.do))) break;
            if (depth == 0 and token.kind == .semicolon) break;
            switch (token.kind) {
                .lparen => depth += 1,
                .rparen => {
                    if (depth == 0) return error.UnsupportedSqlShape;
                    depth -= 1;
                },
                else => {},
            }
            pos.* += 1;
        }
        if (depth != 0 or atom_start == pos.*) return error.UnsupportedSqlShape;

        const predicate = try parseDdlUniquePredicateAtomAlloc(alloc, tokens[atom_start..pos.*]);
        var predicate_transferred = false;
        errdefer if (!predicate_transferred) freeDdlUniquePredicate(alloc, predicate);
        try predicates.append(alloc, predicate);
        predicate_transferred = true;
        if (!parser.matchKeywordTag(tokens, pos, .@"and")) break;
    }
    try expr_type.validateUniquePredicatesForColumns(columns, predicates.items);
    return try uniquePredicateWhereJsonAlloc(alloc, predicates.items);
}

fn parseDdlUniquePredicateAtomAlloc(
    alloc: std.mem.Allocator,
    raw_tokens: []const Token,
) !runtime_schema.UniquePredicate {
    const tokens = parser.stripBalancedOuterParens(raw_tokens);
    if (tokens.len == 0) return error.UnsupportedSqlShape;

    var idx: usize = 0;
    const field_token = try parser.parseWrappedIdentifierOperand(tokens, &idx);
    const field = try alloc.dupe(u8, field_token.text);
    var field_transferred = false;
    errdefer if (!field_transferred) alloc.free(field);

    if (idx >= tokens.len) return error.UnsupportedSqlShape;
    if (tokens[idx].matchesKeywordTag(.is)) {
        idx += 1;
        const op: runtime_schema.UniquePredicateOp = if (idx < tokens.len and tokens[idx].matchesKeywordTag(.not)) blk: {
            idx += 1;
            if (idx >= tokens.len or !tokens[idx].matchesKeywordTag(.null)) return error.UnsupportedSqlShape;
            idx += 1;
            break :blk .is_not_null;
        } else blk: {
            if (idx >= tokens.len or !tokens[idx].matchesKeywordTag(.null)) return error.UnsupportedSqlShape;
            idx += 1;
            break :blk .is_null;
        };
        if (idx != tokens.len) return error.UnsupportedSqlShape;
        field_transferred = true;
        return .{ .field = field, .op = op };
    }

    const op: runtime_schema.UniquePredicateOp = if (tokens[idx].kind == .eq) .eq else if (tokens[idx].kind == .neq) .ne else return error.UnsupportedSqlShape;
    idx += 1;
    if (idx >= tokens.len) return error.UnsupportedSqlShape;
    const value_json = try sql_value.parseSqlUntypedValueJsonAlloc(alloc, tokens, &idx);
    var value_transferred = false;
    errdefer if (!value_transferred) alloc.free(value_json);
    if (idx != tokens.len) return error.UnsupportedSqlShape;

    field_transferred = true;
    value_transferred = true;
    return .{ .field = field, .op = op, .value_json = value_json };
}

fn freeDdlUniquePredicate(alloc: std.mem.Allocator, predicate: runtime_schema.UniquePredicate) void {
    alloc.free(predicate.field);
    if (predicate.value_json) |value| alloc.free(value);
}

pub fn parseSqlTableReferenceIdentifierOwnedAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) ![]const u8 {
    _ = parser.matchKeyword(tokens, pos, "only");
    return try parseSqlObjectIdentifierOwnedAlloc(alloc, tokens, pos);
}

pub fn parseAdapterNoopStatementEnd(tokens: []const Token, pos: *usize) !void {
    try adapterNoopStatementEnd(parser.Cursor.init(tokens, pos));
}

fn adapterNoopStatementEnd(cursor: parser.Cursor) !void {
    if (!cursor.atEnd() and !cursor.peekKind(.semicolon)) return error.UnsupportedSqlShape;
    if (cursor.matchToken(.semicolon) != null and !cursor.atEnd()) return error.UnsupportedSqlShape;
    if (!cursor.atEnd()) return error.UnsupportedSqlShape;
}

fn matchAdapterNoopStatementEnd(cursor: parser.Cursor) !bool {
    if (cursor.matchToken(.semicolon) != null) {
        if (!cursor.atEnd()) return error.UnsupportedSqlShape;
        return true;
    }
    return cursor.atEnd();
}

fn countParenthesizedUntypedValues(cursor: parser.Cursor) !usize {
    if (cursor.matchToken(.lparen) == null) return 0;
    if (cursor.matchToken(.rparen) != null) return 0;
    var count: usize = 0;
    while (true) {
        try parseUntypedValue(cursor);
        count += 1;
        if (cursor.matchToken(.comma) == null) break;
    }
    try cursor.expectToken(.rparen);
    return count;
}

fn parseUntypedValue(cursor: parser.Cursor) !void {
    if (cursor.matchKeyword("true")) return;
    if (cursor.matchKeyword("false")) return;
    if (cursor.matchKeyword("null")) return;
    if (cursor.matchToken(.string) != null) return;
    if (cursor.matchToken(.number) != null) return;
    if (cursor.matchToken(.minus) != null) {
        try cursor.expectToken(.number);
        return;
    }
    return error.UnsupportedSqlShape;
}

fn freeStringList(alloc: std.mem.Allocator, list: *std.ArrayListUnmanaged([]const u8)) void {
    for (list.items) |value| alloc.free(@constCast(value));
    list.deinit(alloc);
}

fn freeStringSlice(alloc: std.mem.Allocator, values: []const []const u8) void {
    for (values) |value| alloc.free(@constCast(value));
    if (values.len > 0) alloc.free(values);
}

fn findMatchingParen(tokens: []const Token, open_index: usize, end: usize) ?usize {
    if (open_index >= end or tokens[open_index].kind != .lparen) return null;
    var depth: usize = 1;
    var index = open_index + 1;
    while (index < end) : (index += 1) {
        switch (tokens[index].kind) {
            .lparen => depth += 1,
            .rparen => {
                depth -= 1;
                if (depth == 0) return index;
            },
            else => {},
        }
    }
    return null;
}

pub fn tokenRangeSqlTextAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    start: usize,
    end: usize,
) ![]const u8 {
    if (start >= end or end > tokens.len) return error.UnsupportedSqlShape;
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    for (tokens[start..end], 0..) |token, index| {
        if (index != 0) try out.append(alloc, ' ');
        try appendSqlTokenText(alloc, &out, token);
    }
    return try out.toOwnedSlice(alloc);
}

fn appendSqlTokenText(alloc: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), token: Token) !void {
    if (token.kind != .string) {
        try out.appendSlice(alloc, token.text);
        return;
    }
    try out.append(alloc, '\'');
    for (token.text) |ch| {
        if (ch == '\'') try out.append(alloc, '\'');
        try out.append(alloc, ch);
    }
    try out.append(alloc, '\'');
}

fn freeDdlGeneratedValue(alloc: std.mem.Allocator, generated: runtime_schema.RelationalGeneratedValue) void {
    if (generated.field) |field| alloc.free(@constCast(field));
    freeStringSlice(alloc, generated.fields);
    alloc.free(@constCast(generated.separator));
    if (generated.expression) |expression| runtime_schema.freeRelationalRowsExpression(alloc, expression);
}

pub fn sessionSettingKindForName(setting: []const u8) ?ddl_plan.SessionSettingKind {
    if (std.mem.startsWith(u8, setting, "app.") and setting.len > "app.".len) return .app;
    if (std.ascii.eqlIgnoreCase(setting, "antfly.sync_level")) return .antfly;
    if (std.ascii.eqlIgnoreCase(setting, "statement_timeout") or
        std.ascii.eqlIgnoreCase(setting, "timezone") or
        std.ascii.eqlIgnoreCase(setting, "default_transaction_read_only") or
        std.ascii.eqlIgnoreCase(setting, "transaction_read_only"))
    {
        return .runtime;
    }
    return null;
}

pub fn validateSetSessionSettingValue(setting: []const u8, kind: ddl_plan.SessionSettingKind, value: []const u8) !void {
    switch (kind) {
        .app => {
            if (value.len == 0) return error.UnsupportedSqlShape;
        },
        .antfly => {
            if (!std.ascii.eqlIgnoreCase(setting, "antfly.sync_level")) return error.UnsupportedSqlShape;
            _ = db_mod.types.parsePublicSyncLevelText(value) orelse return error.UnsupportedSqlShape;
        },
        .runtime => {},
    }
}

pub fn sqlSessionNamespaceNameValid(name: []const u8) bool {
    if (name.len == 0) return false;
    const first = name[0];
    if (!(std.ascii.isAlphabetic(first) or first == '_')) return false;
    for (name[1..]) |ch| {
        if (!(std.ascii.isAlphanumeric(ch) or ch == '_')) return false;
    }
    return true;
}

test "sql adapter grammar matches transaction boundary noops" {
    const alloc = std.testing.allocator;

    var bare_tokens = try lexer.tokenizeAlloc(alloc, ";");
    defer lexer.freeTokens(alloc, &bare_tokens);
    var bare_pos: usize = 0;
    try std.testing.expect(try matchAdapterNoopTransactionBoundaryTail(bare_tokens.items, &bare_pos, .{}));
    try std.testing.expectEqual(bare_tokens.items.len, bare_pos);

    var work_tokens = try lexer.tokenizeAlloc(alloc, "WORK;");
    defer lexer.freeTokens(alloc, &work_tokens);
    var work_pos: usize = 0;
    try std.testing.expect(try matchAdapterNoopTransactionBoundaryTail(work_tokens.items, &work_pos, .{ .work = true }));
    try std.testing.expectEqual(work_tokens.items.len, work_pos);

    var transaction_tokens = try lexer.tokenizeAlloc(alloc, "TRANSACTION;");
    defer lexer.freeTokens(alloc, &transaction_tokens);
    var transaction_pos: usize = 0;
    try std.testing.expect(try matchAdapterNoopTransactionBoundaryTail(transaction_tokens.items, &transaction_pos, .{ .transaction = true }));
    try std.testing.expectEqual(transaction_tokens.items.len, transaction_pos);

    var prepared_tokens = try lexer.tokenizeAlloc(alloc, "PREPARED 'x';");
    defer lexer.freeTokens(alloc, &prepared_tokens);
    var prepared_pos: usize = 0;
    try std.testing.expect(!try matchAdapterNoopTransactionBoundaryTail(prepared_tokens.items, &prepared_pos, .{ .work = true, .transaction = true }));
    try std.testing.expectEqual(@as(usize, 0), prepared_pos);

    var extra_tokens = try lexer.tokenizeAlloc(alloc, "; SELECT 1");
    defer lexer.freeTokens(alloc, &extra_tokens);
    var extra_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, matchAdapterNoopTransactionBoundaryTail(extra_tokens.items, &extra_pos, .{}));
}

test "sql adapter grammar parses ddl constraint suffixes" {
    const alloc = std.testing.allocator;

    var not_valid_tokens = try lexer.tokenizeAlloc(alloc, "NOT VALID,");
    defer lexer.freeTokens(alloc, &not_valid_tokens);
    var not_valid_pos: usize = 0;
    try std.testing.expect(peekDdlNotValid(not_valid_tokens.items, not_valid_pos));
    try std.testing.expect(consumeOptionalDdlNotValid(not_valid_tokens.items, &not_valid_pos));
    try std.testing.expect(not_valid_tokens.items[not_valid_pos].kind == .comma);

    var distinct_tokens = try lexer.tokenizeAlloc(alloc, "NULLS DISTINCT (tenant_id)");
    defer lexer.freeTokens(alloc, &distinct_tokens);
    var distinct_pos: usize = 0;
    try std.testing.expectEqual(false, (try parseOptionalDdlUniqueNullsDistinct(distinct_tokens.items, &distinct_pos)).?);
    try std.testing.expect(distinct_tokens.items[distinct_pos].kind == .lparen);

    var not_distinct_tokens = try lexer.tokenizeAlloc(alloc, "NULLS NOT DISTINCT (tenant_id)");
    defer lexer.freeTokens(alloc, &not_distinct_tokens);
    var not_distinct_pos: usize = 0;
    try std.testing.expectEqual(true, (try parseOptionalDdlUniqueNullsDistinct(not_distinct_tokens.items, &not_distinct_pos)).?);
    try std.testing.expect(not_distinct_tokens.items[not_distinct_pos].kind == .lparen);

    var missing_distinct_tokens = try lexer.tokenizeAlloc(alloc, "NULLS NOT (tenant_id)");
    defer lexer.freeTokens(alloc, &missing_distinct_tokens);
    var missing_distinct_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseOptionalDdlUniqueNullsDistinct(missing_distinct_tokens.items, &missing_distinct_pos));
}

test "sql adapter grammar validates identifier lists" {
    const alloc = std.testing.allocator;

    try validateSqlIdentifierListUnique(&.{ "tenant_id", "usage_id" });
    try std.testing.expectError(error.UnsupportedSqlShape, validateSqlIdentifierListUnique(&.{ "tenant_id", "TENANT_ID" }));

    try validateSqlIdentifierListsDisjoint(&.{ "tenant_id", "usage_id" }, &.{"status"});
    try std.testing.expectError(error.UnsupportedSqlShape, validateSqlIdentifierListsDisjoint(&.{ "tenant_id", "usage_id" }, &.{"USAGE_ID"}));

    var alias_tokens = try lexer.tokenizeAlloc(alloc, "AS projected_name");
    defer lexer.freeTokens(alloc, &alias_tokens);
    var alias_pos: usize = 0;
    const alias = (try parseOptionalProjectionAliasAlloc(alloc, alias_tokens.items, &alias_pos)) orelse return error.TestUnexpectedResult;
    defer alloc.free(alias);
    try std.testing.expectEqualStrings("projected_name", alias);
    try std.testing.expectEqual(alias_tokens.items.len, alias_pos);

    var consume_tokens = try lexer.tokenizeAlloc(alloc, "AS status");
    defer lexer.freeTokens(alloc, &consume_tokens);
    var consume_pos: usize = 0;
    try consumeProjectionAlias(alloc, consume_tokens.items, &consume_pos, "status");
    try std.testing.expectEqual(consume_tokens.items.len, consume_pos);
}

test "sql adapter grammar parses ddl generated expressions" {
    const alloc = std.testing.allocator;

    var lower_tokens = try lexer.tokenizeAlloc(alloc, "lower(email)) STORED");
    defer lexer.freeTokens(alloc, &lower_tokens);
    var lower_pos: usize = 0;
    const lower = try parseDdlGeneratedExpressionAlloc(alloc, lower_tokens.items, &lower_pos);
    defer freeDdlGeneratedValue(alloc, lower);
    try std.testing.expectEqual(runtime_schema.RelationalGeneratedOp.lower, lower.op);
    try std.testing.expectEqualStrings("email", lower.field.?);
    try std.testing.expect(lower_tokens.items[lower_pos].kind == .rparen);

    var upper_tokens = try lexer.tokenizeAlloc(alloc, "upper(status)");
    defer lexer.freeTokens(alloc, &upper_tokens);
    var upper_pos: usize = 0;
    const upper = try parseDdlGeneratedExpressionAlloc(alloc, upper_tokens.items, &upper_pos);
    defer freeDdlGeneratedValue(alloc, upper);
    try std.testing.expectEqual(runtime_schema.RelationalGeneratedOp.upper, upper.op);
    try std.testing.expectEqualStrings("status", upper.field.?);
    try std.testing.expectEqual(upper_tokens.items.len, upper_pos);

    var md5_tokens = try lexer.tokenizeAlloc(alloc, "md5(request_id)");
    defer lexer.freeTokens(alloc, &md5_tokens);
    var md5_pos: usize = 0;
    const md5 = try parseDdlGeneratedExpressionAlloc(alloc, md5_tokens.items, &md5_pos);
    defer freeDdlGeneratedValue(alloc, md5);
    try std.testing.expectEqual(runtime_schema.RelationalGeneratedOp.md5, md5.op);
    try std.testing.expectEqualStrings("request_id", md5.field.?);

    var concat_tokens = try lexer.tokenizeAlloc(alloc, "concat(tenant_id, ':', status)");
    defer lexer.freeTokens(alloc, &concat_tokens);
    var concat_pos: usize = 0;
    const concat = try parseDdlGeneratedExpressionAlloc(alloc, concat_tokens.items, &concat_pos);
    defer freeDdlGeneratedValue(alloc, concat);
    try std.testing.expectEqual(runtime_schema.RelationalGeneratedOp.concat, concat.op);
    try std.testing.expectEqual(@as(usize, 2), concat.fields.len);
    try std.testing.expectEqualStrings("tenant_id", concat.fields[0]);
    try std.testing.expectEqualStrings("status", concat.fields[1]);
    try std.testing.expectEqualStrings(":", concat.separator);

    var concat_ws_tokens = try lexer.tokenizeAlloc(alloc, "concat_ws(':', tenant_id, status)");
    defer lexer.freeTokens(alloc, &concat_ws_tokens);
    var concat_ws_pos: usize = 0;
    const concat_ws = try parseDdlGeneratedExpressionAlloc(alloc, concat_ws_tokens.items, &concat_ws_pos);
    defer freeDdlGeneratedValue(alloc, concat_ws);
    try std.testing.expectEqual(runtime_schema.RelationalGeneratedOp.concat_ws, concat_ws.op);
    try std.testing.expectEqual(@as(usize, 2), concat_ws.fields.len);
    try std.testing.expectEqualStrings(":", concat_ws.separator);

    var single_concat_tokens = try lexer.tokenizeAlloc(alloc, "concat(status)");
    defer lexer.freeTokens(alloc, &single_concat_tokens);
    var single_concat_pos: usize = 0;
    const single_concat = try parseDdlGeneratedExpressionAlloc(alloc, single_concat_tokens.items, &single_concat_pos);
    defer freeDdlGeneratedValue(alloc, single_concat);
    try std.testing.expectEqual(runtime_schema.RelationalGeneratedOp.expression, single_concat.op);
    try std.testing.expectEqual(runtime_schema.RelationalRowsExpressionKind.concat, single_concat.expression.?.kind);

    var mismatched_separator_tokens = try lexer.tokenizeAlloc(alloc, "concat(tenant_id, ':', status, '-', id)");
    defer lexer.freeTokens(alloc, &mismatched_separator_tokens);
    var mismatched_separator_pos: usize = 0;
    const mismatched_separator = try parseDdlGeneratedExpressionAlloc(alloc, mismatched_separator_tokens.items, &mismatched_separator_pos);
    defer freeDdlGeneratedValue(alloc, mismatched_separator);
    try std.testing.expectEqual(runtime_schema.RelationalGeneratedOp.expression, mismatched_separator.op);
    try std.testing.expectEqual(runtime_schema.RelationalRowsExpressionKind.concat, mismatched_separator.expression.?.kind);

    var replace_tokens = try lexer.tokenizeAlloc(alloc, "replace(status, 'a', 'b')");
    defer lexer.freeTokens(alloc, &replace_tokens);
    var replace_pos: usize = 0;
    const replace = try parseDdlGeneratedExpressionAlloc(alloc, replace_tokens.items, &replace_pos);
    defer freeDdlGeneratedValue(alloc, replace);
    try std.testing.expectEqual(runtime_schema.RelationalGeneratedOp.expression, replace.op);
    try std.testing.expectEqual(runtime_schema.RelationalRowsExpressionKind.replace, replace.expression.?.kind);
    try std.testing.expectEqual(@as(usize, 3), replace.expression.?.operands.len);

    var arithmetic_tokens = try lexer.tokenizeAlloc(alloc, "round((amount + fee) * 100)");
    defer lexer.freeTokens(alloc, &arithmetic_tokens);
    var arithmetic_pos: usize = 0;
    const arithmetic = try parseDdlGeneratedExpressionAlloc(alloc, arithmetic_tokens.items, &arithmetic_pos);
    defer freeDdlGeneratedValue(alloc, arithmetic);
    try std.testing.expectEqual(runtime_schema.RelationalGeneratedOp.expression, arithmetic.op);
    try std.testing.expectEqual(runtime_schema.RelationalRowsExpressionKind.round, arithmetic.expression.?.kind);
    try std.testing.expectEqual(runtime_schema.RelationalRowsExpressionKind.mul, arithmetic.expression.?.operands[0].kind);

    var json_extract_tokens = try lexer.tokenizeAlloc(alloc, "jsonb_extract_path_text(metadata, 'source')");
    defer lexer.freeTokens(alloc, &json_extract_tokens);
    var json_extract_pos: usize = 0;
    const json_extract = try parseDdlGeneratedExpressionAlloc(alloc, json_extract_tokens.items, &json_extract_pos);
    defer freeDdlGeneratedValue(alloc, json_extract);
    try std.testing.expectEqual(runtime_schema.RelationalGeneratedOp.expression, json_extract.op);
    try std.testing.expectEqual(runtime_schema.RelationalRowsExpressionKind.json_extract, json_extract.expression.?.kind);
    try std.testing.expect(json_extract.expression.?.json_as_text);
    try std.testing.expectEqualStrings("source", json_extract.expression.?.json_path);

    var volatile_tokens = try lexer.tokenizeAlloc(alloc, "now()");
    defer lexer.freeTokens(alloc, &volatile_tokens);
    var volatile_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseDdlGeneratedExpressionAlloc(alloc, volatile_tokens.items, &volatile_pos));
}

test "sql adapter grammar parses conflict-target unique predicate JSON" {
    const alloc = std.testing.allocator;
    const columns = [_]runtime_schema.RelationalColumn{
        .{ .name = "status", .path = "status", .field_type = .keyword },
        .{ .name = "tenant_id", .path = "tenant_id", .field_type = .keyword },
    };

    var tokens = try lexer.tokenizeAlloc(alloc, "status = 'active' AND tenant_id IS NOT NULL DO UPDATE");
    defer lexer.freeTokens(alloc, &tokens);
    var pos: usize = 0;
    const where_json = try parseDdlUniquePredicateWhereJsonAlloc(alloc, tokens.items, &pos, &columns);
    defer alloc.free(where_json);
    try std.testing.expectEqualStrings("{\"all\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"active\"},{\"field\":\"tenant_id\",\"op\":\"is_not_null\"}]}", where_json);
    try std.testing.expect(pos < tokens.items.len);
    try std.testing.expect(std.ascii.eqlIgnoreCase(tokens.items[pos].text, "do"));

    var invalid_tokens = try lexer.tokenizeAlloc(alloc, "missing = 'active' DO UPDATE");
    defer lexer.freeTokens(alloc, &invalid_tokens);
    var invalid_pos: usize = 0;
    try std.testing.expectError(error.InvalidSqlCatalog, parseDdlUniquePredicateWhereJsonAlloc(alloc, invalid_tokens.items, &invalid_pos, &columns));
}

test "sql adapter grammar parses ddl unique expressions" {
    const alloc = std.testing.allocator;

    var lower_tokens = try lexer.tokenizeAlloc(alloc, "lower(email) WHERE");
    defer lexer.freeTokens(alloc, &lower_tokens);
    var lower_pos: usize = 0;
    const lower = try parseDdlUniqueExpressionAlloc(alloc, lower_tokens.items, &lower_pos);
    defer alloc.free(lower.field);
    try std.testing.expectEqual(runtime_schema.UniqueExpressionOp.lower, lower.op);
    try std.testing.expectEqualStrings("email", lower.field);
    try std.testing.expect(std.ascii.eqlIgnoreCase(lower_tokens.items[lower_pos].text, "where"));

    var upper_tokens = try lexer.tokenizeAlloc(alloc, "upper(status)");
    defer lexer.freeTokens(alloc, &upper_tokens);
    var upper_pos: usize = 0;
    const upper = try parseDdlUniqueExpressionAlloc(alloc, upper_tokens.items, &upper_pos);
    defer alloc.free(upper.field);
    try std.testing.expectEqual(runtime_schema.UniqueExpressionOp.upper, upper.op);
    try std.testing.expectEqualStrings("status", upper.field);
    try std.testing.expectEqual(upper_tokens.items.len, upper_pos);

    var md5_tokens = try lexer.tokenizeAlloc(alloc, "md5(request_id)");
    defer lexer.freeTokens(alloc, &md5_tokens);
    var md5_pos: usize = 0;
    const md5 = try parseDdlUniqueExpressionAlloc(alloc, md5_tokens.items, &md5_pos);
    defer alloc.free(md5.field);
    try std.testing.expectEqual(runtime_schema.UniqueExpressionOp.md5, md5.op);
    try std.testing.expectEqualStrings("request_id", md5.field);

    var concat_tokens = try lexer.tokenizeAlloc(alloc, "concat(tenant_id, ':', status)");
    defer lexer.freeTokens(alloc, &concat_tokens);
    var concat_pos: usize = 0;
    const concat = try parseDdlUniqueExpressionAlloc(alloc, concat_tokens.items, &concat_pos);
    defer runtime_schema.freeRelationalRowsExpression(alloc, concat.expression.?);
    try std.testing.expectEqual(runtime_schema.UniqueExpressionOp.expression, concat.op);
    try std.testing.expectEqual(runtime_schema.RelationalRowsExpressionKind.concat, concat.expression.?.kind);
    try std.testing.expectEqual(@as(usize, 3), concat.expression.?.operands.len);
    try std.testing.expectEqual(concat_tokens.items.len, concat_pos);
}

test "sql adapter grammar parses ddl index expression wrappers" {
    const alloc = std.testing.allocator;

    var wrapped_tokens = try lexer.tokenizeAlloc(alloc, "((lower(email))) ASC");
    defer lexer.freeTokens(alloc, &wrapped_tokens);
    try std.testing.expect(peekDdlIndexElementExpression(wrapped_tokens.items, 0, false));
    var wrapped_pos: usize = 0;
    const wrappers = consumeDdlIndexExpressionWrappers(wrapped_tokens.items, &wrapped_pos);
    try std.testing.expectEqual(@as(usize, 2), wrappers);
    const expression = try parseDdlUniqueExpressionAlloc(alloc, wrapped_tokens.items, &wrapped_pos);
    defer alloc.free(expression.field);
    try std.testing.expectEqual(runtime_schema.UniqueExpressionOp.lower, expression.op);
    try std.testing.expectEqualStrings("email", expression.field);
    try closeDdlIndexExpressionWrappers(wrapped_tokens.items, &wrapped_pos, wrappers);
    try std.testing.expect(std.ascii.eqlIgnoreCase(wrapped_tokens.items[wrapped_pos].text, "asc"));

    var concat_tokens = try lexer.tokenizeAlloc(alloc, "(concat(tenant_id, ':', status))");
    defer lexer.freeTokens(alloc, &concat_tokens);
    try std.testing.expect(!peekDdlIndexElementExpression(concat_tokens.items, 0, false));
    try std.testing.expect(peekDdlIndexElementExpression(concat_tokens.items, 0, true));

    var replace_tokens = try lexer.tokenizeAlloc(alloc, "(replace(status, 'a', 'b'))");
    defer lexer.freeTokens(alloc, &replace_tokens);
    try std.testing.expect(!peekDdlIndexElementExpression(replace_tokens.items, 0, false));
    try std.testing.expect(peekDdlIndexElementExpression(replace_tokens.items, 0, true));

    var column_tokens = try lexer.tokenizeAlloc(alloc, "(email)");
    defer lexer.freeTokens(alloc, &column_tokens);
    try std.testing.expect(!peekDdlIndexElementExpression(column_tokens.items, 0, true));

    var function_name_column_tokens = try lexer.tokenizeAlloc(alloc, "(lower)");
    defer lexer.freeTokens(alloc, &function_name_column_tokens);
    try std.testing.expect(!peekDdlIndexElementExpression(function_name_column_tokens.items, 0, true));
}

test "sql adapter grammar parses ddl collations" {
    const alloc = std.testing.allocator;

    var simple_tokens = try lexer.tokenizeAlloc(alloc, "COLLATE \"C\" NOT NULL");
    defer lexer.freeTokens(alloc, &simple_tokens);
    var simple_pos: usize = 0;
    const simple = (try parseOptionalDdlCollationAlloc(alloc, simple_tokens.items, &simple_pos)).?;
    defer alloc.free(simple);
    try std.testing.expectEqualStrings("C", simple);
    try std.testing.expect(std.ascii.eqlIgnoreCase(simple_tokens.items[simple_pos].text, "not"));

    var qualified_tokens = try lexer.tokenizeAlloc(alloc, "COLLATE public.en_US DEFAULT 'active'");
    defer lexer.freeTokens(alloc, &qualified_tokens);
    var qualified_pos: usize = 0;
    const qualified = (try parseOptionalDdlCollationAlloc(alloc, qualified_tokens.items, &qualified_pos)).?;
    defer alloc.free(qualified);
    try std.testing.expectEqualStrings("public.en_US", qualified);
    try std.testing.expect(std.ascii.eqlIgnoreCase(qualified_tokens.items[qualified_pos].text, "default"));

    var absent_tokens = try lexer.tokenizeAlloc(alloc, "NOT NULL");
    defer lexer.freeTokens(alloc, &absent_tokens);
    var absent_pos: usize = 0;
    try std.testing.expect((try parseOptionalDdlCollationAlloc(alloc, absent_tokens.items, &absent_pos)) == null);
    try std.testing.expectEqual(@as(usize, 0), absent_pos);

    var missing_name_tokens = try lexer.tokenizeAlloc(alloc, "COLLATE");
    defer lexer.freeTokens(alloc, &missing_name_tokens);
    var missing_name_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseOptionalDdlCollationAlloc(alloc, missing_name_tokens.items, &missing_name_pos));
}

test "sql adapter grammar parses select set operation tokens" {
    const alloc = std.testing.allocator;

    var union_all_tokens = try lexer.tokenizeAlloc(alloc, "UNION ALL SELECT id FROM usage_archive");
    defer lexer.freeTokens(alloc, &union_all_tokens);
    var union_all_pos: usize = 0;
    try std.testing.expectEqual(ast.SelectSetOperation.union_all, try parseSelectSetOperation(union_all_tokens.items, &union_all_pos));
    try std.testing.expect(std.ascii.eqlIgnoreCase(union_all_tokens.items[union_all_pos].text, "select"));

    var union_distinct_tokens = try lexer.tokenizeAlloc(alloc, "UNION DISTINCT SELECT id FROM usage_archive");
    defer lexer.freeTokens(alloc, &union_distinct_tokens);
    var union_distinct_pos: usize = 0;
    try std.testing.expectEqual(ast.SelectSetOperation.union_distinct, try parseSelectSetOperation(union_distinct_tokens.items, &union_distinct_pos));
    try std.testing.expect(std.ascii.eqlIgnoreCase(union_distinct_tokens.items[union_distinct_pos].text, "select"));

    var intersect_tokens = try lexer.tokenizeAlloc(alloc, "INTERSECT SELECT id FROM usage_archive");
    defer lexer.freeTokens(alloc, &intersect_tokens);
    var intersect_pos: usize = 0;
    try std.testing.expectEqual(ast.SelectSetOperation.intersect, try parseSelectSetOperation(intersect_tokens.items, &intersect_pos));

    var except_tokens = try lexer.tokenizeAlloc(alloc, "EXCEPT SELECT id FROM usage_archive");
    defer lexer.freeTokens(alloc, &except_tokens);
    var except_pos: usize = 0;
    try std.testing.expectEqual(ast.SelectSetOperation.except, try parseSelectSetOperation(except_tokens.items, &except_pos));

    try std.testing.expect(nextIsSelectSetOperationKeyword(union_all_tokens.items, 0));
    try std.testing.expect(!nextIsSelectSetOperationKeyword(union_all_tokens.items, union_all_pos));

    var tail_tokens = try lexer.tokenizeAlloc(alloc, "ORDER BY id LIMIT 10 OFFSET 1 FETCH NEXT ROW ONLY ;");
    defer lexer.freeTokens(alloc, &tail_tokens);
    try std.testing.expect(nextIsSelectSetResultTailKeyword(tail_tokens.items, 0));
    try std.testing.expect(nextIsSelectSetResultTailKeyword(tail_tokens.items, 3));
    try std.testing.expect(nextIsSelectSetResultTailKeyword(tail_tokens.items, 5));
    try std.testing.expect(nextIsSelectSetResultTailKeyword(tail_tokens.items, 7));
    try std.testing.expect(nextIsSelectSetResultTailKeyword(tail_tokens.items, tail_tokens.items.len - 1));

    var invalid_tokens = try lexer.tokenizeAlloc(alloc, "SELECT id FROM usage_records");
    defer lexer.freeTokens(alloc, &invalid_tokens);
    var invalid_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseSelectSetOperation(invalid_tokens.items, &invalid_pos));
}

test "sql adapter grammar parses row claim clauses" {
    const alloc = std.testing.allocator;

    var skip_tokens = try lexer.tokenizeAlloc(alloc, "NO KEY UPDATE OF usage_records, public.jobs SKIP LOCKED");
    defer lexer.freeTokens(alloc, &skip_tokens);
    var skip_pos: usize = 0;
    var skip_clause = try parseForRowClaimClauseAlloc(alloc, skip_tokens.items, &skip_pos);
    defer skip_clause.deinit(alloc);
    try std.testing.expectEqual(skip_tokens.items.len, skip_pos);
    try std.testing.expectEqual(db_mod.types.RowClaimMode.for_no_key_update, skip_clause.clause.mode);
    try std.testing.expectEqual(db_mod.types.RowClaimWaitPolicy.skip_locked, skip_clause.clause.wait_policy);
    try std.testing.expectEqual(@as(usize, 2), skip_clause.targets.len);
    try std.testing.expectEqualStrings("usage_records", skip_clause.targets[0]);
    try std.testing.expectEqualStrings("public.jobs", skip_clause.targets[1]);

    var share_tokens = try lexer.tokenizeAlloc(alloc, "KEY SHARE OF ONLY usage_records NOWAIT");
    defer lexer.freeTokens(alloc, &share_tokens);
    var share_pos: usize = 0;
    var share_clause = try parseForRowClaimClauseAlloc(alloc, share_tokens.items, &share_pos);
    defer share_clause.deinit(alloc);
    try std.testing.expectEqual(share_tokens.items.len, share_pos);
    try std.testing.expectEqual(db_mod.types.RowClaimMode.for_key_share, share_clause.clause.mode);
    try std.testing.expectEqual(db_mod.types.RowClaimWaitPolicy.nowait, share_clause.clause.wait_policy);
    try std.testing.expectEqual(@as(usize, 1), share_clause.targets.len);
    try std.testing.expectEqualStrings("usage_records", share_clause.targets[0]);

    var default_tokens = try lexer.tokenizeAlloc(alloc, "UPDATE");
    defer lexer.freeTokens(alloc, &default_tokens);
    var default_pos: usize = 0;
    var default_clause = try parseForRowClaimClauseAlloc(alloc, default_tokens.items, &default_pos);
    defer default_clause.deinit(alloc);
    try std.testing.expectEqual(default_tokens.items.len, default_pos);
    try std.testing.expectEqual(db_mod.types.RowClaimMode.for_update, default_clause.clause.mode);
    try std.testing.expectEqual(db_mod.types.RowClaimWaitPolicy.wait, default_clause.clause.wait_policy);

    var invalid_tokens = try lexer.tokenizeAlloc(alloc, "SHARE SKIP");
    defer lexer.freeTokens(alloc, &invalid_tokens);
    var invalid_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseForRowClaimClauseAlloc(alloc, invalid_tokens.items, &invalid_pos));

    try std.testing.expect(rowClaimTargetAllowed(alloc, "public.jobs", &.{"jobs"}));
    try std.testing.expect(rowClaimTargetAllowed(alloc, "usage_records", &.{ "usage_records", "u" }));
    try std.testing.expect(!rowClaimTargetAllowed(alloc, "tenant.jobs", &.{"jobs"}));

    var checked_tokens = try lexer.tokenizeAlloc(alloc, "UPDATE OF public.jobs NOWAIT");
    defer lexer.freeTokens(alloc, &checked_tokens);
    var checked_pos: usize = 0;
    const checked_clause = try parseCheckedForRowClaimClauseAlloc(alloc, checked_tokens.items, &checked_pos, &.{"jobs"});
    try std.testing.expectEqual(checked_tokens.items.len, checked_pos);
    try std.testing.expectEqual(db_mod.types.RowClaimMode.for_update, checked_clause.mode);
    try std.testing.expectEqual(db_mod.types.RowClaimWaitPolicy.nowait, checked_clause.wait_policy);

    var rejected_tokens = try lexer.tokenizeAlloc(alloc, "UPDATE OF tenant.jobs");
    defer lexer.freeTokens(alloc, &rejected_tokens);
    var rejected_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseCheckedForRowClaimClauseAlloc(alloc, rejected_tokens.items, &rejected_pos, &.{"jobs"}));

    var exclusive_tokens = try lexer.tokenizeAlloc(alloc, "NO KEY UPDATE OF jobs");
    defer lexer.freeTokens(alloc, &exclusive_tokens);
    var exclusive_pos: usize = 0;
    const exclusive_clause = try parseExclusiveForRowClaimClauseAlloc(alloc, exclusive_tokens.items, &exclusive_pos, &.{"jobs"});
    try std.testing.expectEqual(exclusive_tokens.items.len, exclusive_pos);
    try std.testing.expectEqual(db_mod.types.RowClaimMode.for_no_key_update, exclusive_clause.mode);

    var shared_tokens = try lexer.tokenizeAlloc(alloc, "SHARE OF jobs");
    defer lexer.freeTokens(alloc, &shared_tokens);
    var shared_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseExclusiveForRowClaimClauseAlloc(alloc, shared_tokens.items, &shared_pos, &.{"jobs"}));
}

test "sql adapter grammar normalizes public object identifiers" {
    const alloc = std.testing.allocator;

    const bare = try normalizeSqlObjectIdentifierAlloc(alloc, "usage_records");
    defer alloc.free(bare);
    try std.testing.expectEqualStrings("usage_records", bare);

    const public_qualified = try normalizeSqlObjectIdentifierAlloc(alloc, "public.usage_records");
    defer alloc.free(public_qualified);
    try std.testing.expectEqualStrings("usage_records", public_qualified);

    const other_schema = try normalizeSqlObjectIdentifierAlloc(alloc, "tenant_1.usage_records");
    defer alloc.free(other_schema);
    try std.testing.expectEqualStrings("tenant_1.usage_records", other_schema);

    try std.testing.expectError(error.UnsupportedSqlShape, normalizeSqlObjectIdentifierAlloc(alloc, ".usage_records"));
    try std.testing.expectError(error.UnsupportedSqlShape, normalizeSqlObjectIdentifierAlloc(alloc, "public."));
    try std.testing.expectError(error.UnsupportedSqlShape, normalizeSqlObjectIdentifierAlloc(alloc, "public.analytics.usage_records"));
}

test "sql adapter grammar preserves database-qualified schema identifiers" {
    const alloc = std.testing.allocator;

    const bare = try normalizeSqlSchemaIdentifierAlloc(alloc, "analytics");
    defer alloc.free(bare);
    try std.testing.expectEqualStrings("analytics", bare);

    const qualified = try normalizeSqlSchemaIdentifierAlloc(alloc, "tenant_ops.analytics");
    defer alloc.free(qualified);
    try std.testing.expectEqualStrings("tenant_ops.analytics", qualified);

    const public_qualified = try normalizeSqlSchemaIdentifierAlloc(alloc, "public.analytics");
    defer alloc.free(public_qualified);
    try std.testing.expectEqualStrings("public.analytics", public_qualified);

    try std.testing.expectError(error.UnsupportedSqlShape, normalizeSqlSchemaIdentifierAlloc(alloc, ".analytics"));
    try std.testing.expectError(error.UnsupportedSqlShape, normalizeSqlSchemaIdentifierAlloc(alloc, "tenant_ops."));
    try std.testing.expectError(error.UnsupportedSqlShape, normalizeSqlSchemaIdentifierAlloc(alloc, "tenant_ops.analytics.events"));
}

test "sql adapter grammar peeks joined mutation and assignment syntax" {
    const alloc = std.testing.allocator;

    var joined_alias = try lexer.tokenizeAlloc(alloc, "set total = src.total from usage_delta as src where src.id = usage.id");
    defer lexer.freeTokens(alloc, &joined_alias);
    try std.testing.expectEqualStrings("src", peekUpdateJoinedMutationSourceAlias(joined_alias.items, 0).?);

    var joined_table = try lexer.tokenizeAlloc(alloc, "set total = usage_delta.total from usage_delta where usage_delta.id = usage.id");
    defer lexer.freeTokens(alloc, &joined_table);
    try std.testing.expectEqualStrings("usage_delta", peekUpdateJoinedMutationSourceAlias(joined_table.items, 0).?);

    var nested_from = try lexer.tokenizeAlloc(alloc, "set payload = jsonb_set(payload, '{x}', to_jsonb(select from nested))");
    defer lexer.freeTokens(alloc, &nested_from);
    try std.testing.expect(peekUpdateJoinedMutationSourceAlias(nested_from.items, 0) == null);

    var static_value = try lexer.tokenizeAlloc(alloc, "-42");
    defer lexer.freeTokens(alloc, &static_value);
    try std.testing.expect(peekStaticToJsonbValue(static_value.items, 0));

    var expression_value = try lexer.tokenizeAlloc(alloc, "source.total + 1");
    defer lexer.freeTokens(alloc, &expression_value);
    try std.testing.expect(!peekStaticToJsonbValue(expression_value.items, 0));

    var array_append = try lexer.tokenizeAlloc(alloc, "array_append(tags, 'urgent')");
    defer lexer.freeTokens(alloc, &array_append);
    try std.testing.expect(peekArrayTransformSelfAssignment(array_append.items, 0, "tags"));
    try std.testing.expect(!peekArrayTransformSelfAssignment(array_append.items, 0, "labels"));
    var array_append_pos: usize = 0;
    try std.testing.expectEqual(db_mod.types.TransformOpType.push, matchArrayTransformUpdateOp(array_append.items, &array_append_pos).?);
    try std.testing.expectEqual(@as(usize, 1), array_append_pos);

    var array_remove = try lexer.tokenizeAlloc(alloc, "array_remove(tags, 'stale')");
    defer lexer.freeTokens(alloc, &array_remove);
    var array_remove_pos: usize = 0;
    try std.testing.expectEqual(db_mod.types.TransformOpType.pull, matchArrayTransformUpdateOp(array_remove.items, &array_remove_pos).?);
    try std.testing.expectEqual(@as(usize, 1), array_remove_pos);
}

test "sql adapter grammar parses owned identifiers and normalized object lists" {
    const alloc = std.testing.allocator;

    var identifiers = try lexer.tokenizeAlloc(alloc, "tenant_id, order_id, status");
    defer lexer.freeTokens(alloc, &identifiers);
    var identifier_pos: usize = 0;
    const identifier_list = try parseIdentifierListAlloc(alloc, identifiers.items, &identifier_pos);
    defer {
        for (identifier_list) |item| alloc.free(item);
        alloc.free(identifier_list);
    }
    try std.testing.expectEqual(identifiers.items.len, identifier_pos);
    try std.testing.expectEqual(@as(usize, 3), identifier_list.len);
    try std.testing.expectEqualStrings("tenant_id", identifier_list[0]);
    try std.testing.expectEqualStrings("order_id", identifier_list[1]);
    try std.testing.expectEqualStrings("status", identifier_list[2]);

    var objects = try lexer.tokenizeAlloc(alloc, "public.usage_records, tenant_1.audit_records");
    defer lexer.freeTokens(alloc, &objects);
    var object_pos: usize = 0;
    const object_list = try parseSqlObjectIdentifierListAlloc(alloc, objects.items, &object_pos);
    defer {
        for (object_list) |item| alloc.free(item);
        alloc.free(object_list);
    }
    try std.testing.expectEqual(objects.items.len, object_pos);
    try std.testing.expectEqual(@as(usize, 2), object_list.len);
    try std.testing.expectEqualStrings("usage_records", object_list[0]);
    try std.testing.expectEqualStrings("tenant_1.audit_records", object_list[1]);

    var table_ref = try lexer.tokenizeAlloc(alloc, "ONLY public.usage_records");
    defer lexer.freeTokens(alloc, &table_ref);
    var table_pos: usize = 0;
    const table_name = try parseSqlTableReferenceIdentifierOwnedAlloc(alloc, table_ref.items, &table_pos);
    defer alloc.free(table_name);
    try std.testing.expectEqual(table_ref.items.len, table_pos);
    try std.testing.expectEqualStrings("usage_records", table_name);

    var invalid = try lexer.tokenizeAlloc(alloc, "usage_records,");
    defer lexer.freeTokens(alloc, &invalid);
    var invalid_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseIdentifierListAlloc(alloc, invalid.items, &invalid_pos));
}

test "sql adapter grammar parses optional CTE column aliases" {
    const alloc = std.testing.allocator;

    var none = try lexer.tokenizeAlloc(alloc, "as");
    defer lexer.freeTokens(alloc, &none);
    var none_pos: usize = 0;
    const no_aliases = try parseOptionalCteColumnAliasesAlloc(alloc, none.items, &none_pos);
    try std.testing.expectEqual(@as(usize, 0), no_aliases.len);
    try std.testing.expectEqual(@as(usize, 0), none_pos);

    var aliases = try lexer.tokenizeAlloc(alloc, "(tenant_id, order_id) as");
    defer lexer.freeTokens(alloc, &aliases);
    var alias_pos: usize = 0;
    const parsed = try parseOptionalCteColumnAliasesAlloc(alloc, aliases.items, &alias_pos);
    defer freeStringSlice(alloc, parsed);
    try std.testing.expectEqual(@as(usize, 5), alias_pos);
    try std.testing.expectEqual(@as(usize, 2), parsed.len);
    try std.testing.expectEqualStrings("tenant_id", parsed[0]);
    try std.testing.expectEqualStrings("order_id", parsed[1]);

    var duplicate = try lexer.tokenizeAlloc(alloc, "(tenant_id, TENANT_ID)");
    defer lexer.freeTokens(alloc, &duplicate);
    var duplicate_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseOptionalCteColumnAliasesAlloc(alloc, duplicate.items, &duplicate_pos));

    var trailing = try lexer.tokenizeAlloc(alloc, "(tenant_id,)");
    defer lexer.freeTokens(alloc, &trailing);
    var trailing_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseOptionalCteColumnAliasesAlloc(alloc, trailing.items, &trailing_pos));
}
