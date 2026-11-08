// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

const std = @import("std");
pub const ast = @import("ast.zig");
pub const lexer = @import("lexer.zig");
const generated = @import("grammar/generated/root.zig");
pub const Diagnostic = lexer.Diagnostic;
pub const Limits = struct { max_bytes: usize = 1 << 20, max_tokens: usize = 16384, max_nodes: usize = 8192, max_depth: usize = 64 };
pub const Error = lexer.Error || error{ InvalidCypherNumber, UnsupportedCypherFeature };
pub const Compiled = struct {
    arena: std.heap.ArenaAllocator,
    query: ast.Query,
    parameters: []const []const u8,
    pub fn deinit(self: *@This()) void {
        self.arena.deinit();
        self.* = undefined;
    }
};

pub const Parsed = struct { query: ast.Query, parameters: []const []const u8 };
pub fn compile(a: std.mem.Allocator, source: []const u8, limits: Limits, diagnostic: *Diagnostic) Error!Compiled {
    var arena = std.heap.ArenaAllocator.init(a);
    errdefer arena.deinit();
    const parsed = try compileInto(arena.allocator(), source, limits, diagnostic);
    return .{ .arena = arena, .query = parsed.query, .parameters = parsed.parameters };
}
/// SQL and other compiler hosts own one arena for all language fragments.
/// This entry point builds shared graph IR directly in that owner arena.
pub fn compileInto(a: std.mem.Allocator, source: []const u8, limits: Limits, diagnostic: *Diagnostic) Error!Parsed {
    diagnostic.* = .{};
    if (source.len > limits.max_bytes) {
        diagnostic.message = "Cypher statement byte budget exceeded";
        return error.CypherLimitExceeded;
    }
    var scratch = std.heap.ArenaAllocator.init(a);
    defer scratch.deinit();
    const tokens = try lexer.scan(scratch.allocator(), source, limits.max_tokens, diagnostic);
    const ids = try scratch.allocator().alloc(u16, tokens.len - 1);
    const fallbacks = try scratch.allocator().alloc(u16, ids.len);
    for (tokens[0 .. tokens.len - 1], ids, fallbacks, 0..) |token, *id, *fallback, index| {
        id.* = if (patternBracket(tokens, index)) generated.tokenId(.PATTERN_LBRACKET) else tokenId(token);
        fallback.* = if (token.kind == .word) generated.tokenId(.IDENT) else id.*;
    }
    // Generated syntax is the public boundary; semantic construction below
    // cannot accept a statement rejected by the owned grammar.
    var failure = generated.parseDiagnosticWithFallback(scratch.allocator(), ids, fallbacks) catch |err| {
        if (err == error.OutOfMemory) return error.OutOfMemory;
        diagnostic.message = "Cypher parser budget exceeded";
        return error.CypherLimitExceeded;
    };
    if (failure) |*item| {
        defer item.deinit(scratch.allocator());
        diagnostic.* = .{ .span = tokens[@min(item.token_index, tokens.len - 1)].span, .message = "unexpected Cypher token" };
        return error.InvalidCypherSyntax;
    }
    var p: Parser = .{ .a = a, .tokens = tokens, .limits = limits, .diagnostic = diagnostic };
    const explain = p.word("EXPLAIN");
    const profile = if (!explain) p.word("PROFILE") else false;
    const clauses = try p.clauses();
    var unions: std.ArrayList(ast.UnionArm) = .empty;
    while (p.word("UNION")) {
        const all = p.word("ALL");
        try unions.append(p.a, .{ .all = all, .clauses = try p.clauses() });
    }
    _ = p.take(.semicolon);
    try p.expect(.eof);
    return .{ .query = .{ .source = try a.dupe(u8, source), .clauses = clauses, .unions = try unions.toOwnedSlice(p.a), .explain = explain, .profile = profile }, .parameters = try p.parameters.toOwnedSlice(p.a) };
}

/// Distinguish a pattern expression from a parenthesized list item before LR
/// recognition. Require a complete relationship prefix and its target node;
/// subtraction and list indexing retain their ordinary expression tokens.
fn patternBracket(tokens: []const lexer.Token, index: usize) bool {
    if (tokens[index].kind != .lbracket) return false;
    var pos = index + 1;
    if (pos + 1 < tokens.len and (tokens[pos].kind == .word or tokens[pos].kind == .quoted_name) and tokens[pos + 1].kind == .eq) pos += 2;
    if (pos >= tokens.len or tokens[pos].kind != .lparen) return false;
    var depth: usize = 0;
    while (pos < tokens.len) : (pos += 1) {
        if (tokens[pos].kind == .lparen) depth += 1;
        if (tokens[pos].kind == .rparen) {
            depth -= 1;
            if (depth == 0) {
                pos += 1;
                break;
            }
        }
    }
    if (pos < tokens.len and tokens[pos].kind == .lt) pos += 1;
    if (pos >= tokens.len or tokens[pos].kind != .minus) return false;
    pos += 1;
    if (pos < tokens.len and tokens[pos].kind == .lbracket) {
        depth = 0;
        while (pos < tokens.len) : (pos += 1) {
            if (tokens[pos].kind == .lbracket) depth += 1;
            if (tokens[pos].kind == .rbracket) {
                depth -= 1;
                if (depth == 0) {
                    pos += 1;
                    break;
                }
            }
        }
    }
    if (pos >= tokens.len or tokens[pos].kind != .minus) return false;
    pos += 1;
    if (pos < tokens.len and tokens[pos].kind == .gt) pos += 1;
    return pos < tokens.len and tokens[pos].kind == .lparen;
}

fn tokenId(t: lexer.Token) u16 {
    if (t.kind == .word) {
        inline for (@typeInfo(generated.Token).@"enum".field_names, @typeInfo(generated.Token).@"enum".field_values) |name, value| {
            if (std.ascii.eqlIgnoreCase(t.text, name)) return @intCast(value + 1);
        }
        return generated.tokenId(.IDENT);
    }
    return switch (t.kind) {
        .quoted_name => generated.tokenId(.IDENT),
        .string => generated.tokenId(.STRING),
        .integer => generated.tokenId(.INTEGER),
        .number => generated.tokenId(.NUMBER),
        .parameter => generated.tokenId(.PARAMETER),
        .lparen => generated.tokenId(.LPAREN),
        .rparen => generated.tokenId(.RPAREN),
        .lbracket => generated.tokenId(.LBRACKET),
        .rbracket => generated.tokenId(.RBRACKET),
        .lbrace => generated.tokenId(.LBRACE),
        .rbrace => generated.tokenId(.RBRACE),
        .comma => generated.tokenId(.COMMA),
        .colon => generated.tokenId(.COLON),
        .dot => generated.tokenId(.DOT),
        .range => generated.tokenId(.RANGE),
        .plus => generated.tokenId(.PLUS),
        .minus => generated.tokenId(.MINUS),
        .star => generated.tokenId(.STAR),
        .slash => generated.tokenId(.SLASH),
        .percent => generated.tokenId(.PERCENT),
        .caret => generated.tokenId(.CARET),
        .eq => generated.tokenId(.EQ),
        .neq => generated.tokenId(.NEQ),
        .lt => generated.tokenId(.LT),
        .lte => generated.tokenId(.LTE),
        .gt => generated.tokenId(.GT),
        .gte => generated.tokenId(.GTE),
        .regex => generated.tokenId(.REGEX),
        .pipe => generated.tokenId(.PIPE),
        .semicolon => generated.tokenId(.SEMICOLON),
        .eof => 0,
        .word => unreachable,
    };
}

const Parser = struct {
    a: std.mem.Allocator,
    tokens: []const lexer.Token,
    pos: usize = 0,
    nodes: usize = 0,
    depth: usize = 0,
    limits: Limits,
    diagnostic: *Diagnostic,
    parameters: std.ArrayList([]const u8) = .empty,
    fn peek(self: *Parser) lexer.Token {
        return self.tokens[self.pos];
    }
    fn take(self: *Parser, kind: lexer.Kind) bool {
        if (self.peek().kind != kind) return false;
        self.pos += 1;
        return true;
    }
    fn isWord(self: *Parser, word_value: []const u8) bool {
        return self.peek().kind == .word and std.ascii.eqlIgnoreCase(self.peek().text, word_value);
    }
    fn word(self: *Parser, word_value: []const u8) bool {
        if (!self.isWord(word_value)) return false;
        self.pos += 1;
        return true;
    }
    fn expect(self: *Parser, kind: lexer.Kind) Error!void {
        if (!self.take(kind)) return self.fail("unexpected token");
    }
    fn expectWord(self: *Parser, text: []const u8) Error!void {
        if (!self.word(text)) return self.fail("expected Cypher keyword");
    }
    fn fail(self: *Parser, message: []const u8) Error {
        self.diagnostic.* = .{ .span = self.peek().span, .message = message };
        return error.InvalidCypherSyntax;
    }
    fn own(self: *Parser, s: []const u8) Error![]const u8 {
        return self.a.dupe(u8, s);
    }
    fn name(self: *Parser) Error![]const u8 {
        const t = self.peek();
        if (t.kind != .word and t.kind != .quoted_name) return self.fail("expected identifier");
        self.pos += 1;
        return self.own(t.text);
    }
    fn span(self: *Parser, start: usize) ast.Span {
        return .{ .start = start, .end = self.tokens[self.pos - 1].span.end };
    }
    fn enter(self: *Parser) Error!void {
        self.depth += 1;
        if (self.depth > self.limits.max_depth) return error.CypherLimitExceeded;
    }
    fn node(self: *Parser, start: usize, value: @TypeOf(@as(ast.Expr, undefined).value)) Error!*const ast.Expr {
        self.nodes += 1;
        if (self.nodes > self.limits.max_nodes) return error.CypherLimitExceeded;
        const depth = 1 + childDepth(value);
        if (depth > self.limits.max_depth) {
            self.diagnostic.* = .{ .span = self.span(start), .message = "Cypher expression tree depth budget exceeded" };
            return error.CypherLimitExceeded;
        }
        const result = try self.a.create(ast.Expr);
        result.* = .{ .span = self.span(start), .depth = depth, .value = value };
        return result;
    }
    fn clauses(self: *Parser) Error![]const ast.Clause {
        try self.enter();
        defer self.depth -= 1;
        var result: std.ArrayList(ast.Clause) = .empty;
        while (self.peek().kind != .eof and self.peek().kind != .semicolon and self.peek().kind != .rbrace and !self.isWord("UNION")) {
            const start = self.peek().span.start;
            const operation: @TypeOf(@as(ast.Clause, undefined).operation) = blk: {
                const optional = self.word("OPTIONAL");
                if (optional or self.isWord("MATCH")) {
                    try self.expectWord("MATCH");
                    var mode: ast.PathMode = .relationship_simple;
                    if (self.word("REPEATABLE")) {
                        try self.expectWord("ELEMENTS");
                        mode = .repeatable;
                    } else if (self.word("DIFFERENT")) try self.expectWord("RELATIONSHIPS");
                    const path_values = try self.paths();
                    const predicate = if (self.word("WHERE")) try self.expr(0) else null;
                    break :blk .{ .match = .{ .paths = path_values, .optional = optional, .predicate = predicate, .mode = mode } };
                }
                if (self.word("RETURN")) break :blk .{ .project = .{ .value = try self.projection(), .final = true } };
                if (self.word("WITH")) break :blk .{ .project = .{ .value = try self.projection(), .final = false } };
                if (self.word("UNWIND")) {
                    const expression = try self.expr(0);
                    try self.expectWord("AS");
                    break :blk .{ .unwind = .{ .expression = expression, .alias = try self.name() } };
                }
                if (self.word("CALL")) {
                    if (self.take(.lbrace)) {
                        const inner = try self.clauses();
                        try self.expect(.rbrace);
                        break :blk .{ .subquery = inner };
                    }
                    var qualified = try self.name();
                    while (self.take(.dot)) qualified = try std.fmt.allocPrint(self.a, "{s}.{s}", .{ qualified, try self.name() });
                    try self.expect(.lparen);
                    const args = try self.expressions(.rparen);
                    try self.expect(.rparen);
                    var yields: std.ArrayList(ast.Yield) = .empty;
                    if (self.word("YIELD")) while (true) {
                        const field = try self.name();
                        const alias = if (self.word("AS")) try self.name() else field;
                        try yields.append(self.a, .{ .name = field, .alias = alias });
                        if (!self.take(.comma)) break;
                    };
                    break :blk .{ .call = .{ .name = qualified, .args = args, .yields = try yields.toOwnedSlice(self.a), .predicate = if (self.word("WHERE")) try self.expr(0) else null } };
                }
                if (self.word("CREATE")) break :blk .{ .create = try self.paths() };
                if (self.word("MERGE")) {
                    const path_value = try self.path();
                    var on_create: []const ast.Assignment = &.{};
                    var on_match: []const ast.Assignment = &.{};
                    while (self.word("ON")) {
                        const create = self.word("CREATE");
                        if (!create) try self.expectWord("MATCH");
                        try self.expectWord("SET");
                        if (create) on_create = try self.assignments() else on_match = try self.assignments();
                    }
                    break :blk .{ .merge = .{ .path = path_value, .on_create = on_create, .on_match = on_match } };
                }
                if (self.word("SET")) break :blk .{ .set = try self.assignments() };
                if (self.word("REMOVE")) break :blk .{ .remove = try self.expressions(.eof) };
                const detach = self.word("DETACH");
                if (detach or self.isWord("DELETE")) {
                    try self.expectWord("DELETE");
                    break :blk .{ .delete = .{ .detach = detach, .expressions = try self.expressions(.eof) } };
                }
                return self.fail("expected graph clause");
            };
            try result.append(self.a, .{ .span = self.span(start), .operation = operation });
        }
        if (result.items.len == 0) return self.fail("expected graph query");
        return result.toOwnedSlice(self.a);
    }
    fn paths(self: *Parser) Error![]const ast.Path {
        var result: std.ArrayList(ast.Path) = .empty;
        while (true) {
            try result.append(self.a, try self.path());
            if (!self.take(.comma)) break;
        }
        return result.toOwnedSlice(self.a);
    }
    fn path(self: *Parser) Error!ast.Path {
        const start = self.peek().span.start;
        var alias: ?[]const u8 = null;
        if ((self.peek().kind == .word or self.peek().kind == .quoted_name) and self.tokens[self.pos + 1].kind == .eq) {
            alias = try self.name();
            try self.expect(.eq);
        }
        var selector: @TypeOf(@as(ast.Path, undefined).selector) = .all;
        if (self.isWord("shortestPath") or self.isWord("allShortestPaths")) {
            selector = if (self.word("shortestPath")) .shortest else blk: {
                _ = self.word("allShortestPaths");
                break :blk .all_shortest;
            };
            try self.expect(.lparen);
        }
        var nodes: std.ArrayList(ast.Node) = .empty;
        var edges: std.ArrayList(ast.Edge) = .empty;
        try nodes.append(self.a, try self.patternNode());
        while (self.peek().kind == .minus or self.peek().kind == .lt) {
            try edges.append(self.a, try self.edge());
            try nodes.append(self.a, try self.patternNode());
        }
        if (selector != .all) try self.expect(.rparen);
        return .{ .span = self.span(start), .alias = alias, .nodes = try nodes.toOwnedSlice(self.a), .edges = try edges.toOwnedSlice(self.a), .selector = selector };
    }
    fn patternNode(self: *Parser) Error!ast.Node {
        const start = self.peek().span.start;
        try self.expect(.lparen);
        const alias = if (self.peek().kind == .word or self.peek().kind == .quoted_name) try self.name() else null;
        var labels: std.ArrayList([]const u8) = .empty;
        while (self.take(.colon)) try labels.append(self.a, try self.name());
        const properties = if (self.peek().kind == .lbrace) try self.map() else &.{};
        try self.expect(.rparen);
        return .{ .span = self.span(start), .alias = alias, .labels = try labels.toOwnedSlice(self.a), .properties = properties };
    }
    fn hops(self: *Parser) Error!u32 {
        const t = self.peek();
        try self.expect(.integer);
        return std.fmt.parseInt(u32, t.text, 10) catch error.InvalidCypherNumber;
    }
    fn edge(self: *Parser) Error!ast.Edge {
        const start = self.peek().span.start;
        const incoming = self.take(.lt);
        try self.expect(.minus);
        var result: ast.Edge = .{ .span = .{}, .direction = if (incoming) .in else .both };
        if (self.take(.lbracket)) {
            if (self.peek().kind == .word or self.peek().kind == .quoted_name) result.alias = try self.name();
            var types: std.ArrayList([]const u8) = .empty;
            if (self.take(.colon)) while (true) {
                try types.append(self.a, try self.name());
                if (!self.take(.pipe)) break;
                _ = self.take(.colon);
            };
            result.types = try types.toOwnedSlice(self.a);
            if (self.take(.star)) {
                result.variable_length = true;
                result.max_hops = null;
                if (self.peek().kind == .integer) {
                    result.min_hops = try self.hops();
                    result.max_hops = result.min_hops;
                }
                if (self.take(.range)) result.max_hops = if (self.peek().kind == .integer) try self.hops() else null;
                if (result.max_hops) |maximum| if (maximum < result.min_hops) return self.fail("invalid path bounds");
            }
            if (self.peek().kind == .lbrace) result.properties = try self.map();
            try self.expect(.rbracket);
        }
        try self.expect(.minus);
        if (!incoming and self.take(.gt)) result.direction = .out;
        result.span = self.span(start);
        return result;
    }
    fn projection(self: *Parser) Error!ast.Projection {
        var result: ast.Projection = .{ .items = &.{}, .distinct = self.word("DISTINCT") };
        var items: std.ArrayList(ast.Item) = .empty;
        result.wildcard = self.take(.star);
        if (!result.wildcard or self.take(.comma)) while (true) {
            const expression = try self.expr(0);
            try items.append(self.a, .{ .expression = expression, .alias = if (self.word("AS")) try self.name() else null });
            if (!self.take(.comma)) break;
        };
        result.items = try items.toOwnedSlice(self.a);
        var orders: std.ArrayList(ast.Order) = .empty;
        if (self.word("ORDER")) {
            try self.expectWord("BY");
            while (true) {
                const expression = try self.expr(0);
                const descending = self.word("DESC");
                if (!descending) _ = self.word("ASC");
                try orders.append(self.a, .{ .expression = expression, .descending = descending });
                if (!self.take(.comma)) break;
            }
        }
        result.order = try orders.toOwnedSlice(self.a);
        if (self.word("SKIP")) result.skip = try self.expr(0);
        if (self.word("LIMIT")) result.limit = try self.expr(0);
        if (self.word("WHERE")) result.predicate = try self.expr(0);
        return result;
    }
    fn assignments(self: *Parser) Error![]const ast.Assignment {
        var result: std.ArrayList(ast.Assignment) = .empty;
        while (true) {
            const target = try self.expr(6);
            const merge = self.take(.plus);
            try self.expect(.eq);
            try result.append(self.a, .{ .target = target, .value = try self.expr(0), .merge = merge });
            if (!self.take(.comma)) break;
        }
        return result.toOwnedSlice(self.a);
    }
    fn expressions(self: *Parser, end: lexer.Kind) Error![]const *const ast.Expr {
        var result: std.ArrayList(*const ast.Expr) = .empty;
        if (self.peek().kind == end) return &.{};
        while (true) {
            try result.append(self.a, try self.expr(0));
            if (!self.take(.comma)) break;
        }
        return result.toOwnedSlice(self.a);
    }
    fn map(self: *Parser) Error![]const ast.Entry {
        try self.expect(.lbrace);
        var result: std.ArrayList(ast.Entry) = .empty;
        if (!self.take(.rbrace)) {
            while (true) {
                const name_value = if (self.peek().kind == .string) blk: {
                    const t = self.peek();
                    self.pos += 1;
                    break :blk try self.own(t.text);
                } else try self.name();
                for (result.items) |entry| if (std.mem.eql(u8, entry.name, name_value)) return self.fail("duplicate Cypher map key");
                try self.expect(.colon);
                try result.append(self.a, .{ .name = name_value, .value = try self.expr(0) });
                if (!self.take(.comma)) break;
            }
            try self.expect(.rbrace);
        }
        return result.toOwnedSlice(self.a);
    }
    fn expr(self: *Parser, min: u8) Error!*const ast.Expr {
        try self.enter();
        defer self.depth -= 1;
        const start = self.peek().span.start;
        var left = try self.primary();
        var comparison_chain = false;
        var comparison_operands: std.ArrayList(*const ast.Expr) = .empty;
        var comparison_operators: std.ArrayList(ast.Binary) = .empty;
        while (true) {
            if (self.take(.lparen)) {
                comparison_chain = false;
                const name_value = try self.callName(left);
                const distinct = self.word("DISTINCT");
                const star = self.take(.star);
                const args = if (star) &.{} else try self.expressions(.rparen);
                try self.expect(.rparen);
                left = try self.node(start, .{ .call = .{ .name = name_value, .args = args, .distinct = distinct, .star = star } });
                continue;
            }
            if (self.take(.lbrace)) {
                comparison_chain = false;
                var items: std.ArrayList(ast.MapSelector) = .empty;
                if (self.peek().kind != .rbrace) while (true) {
                    const item: ast.MapSelector = if (self.take(.dot))
                        (if (self.take(.star)) .all else .{ .property = try self.name() })
                    else blk: {
                        const name_value = try self.name();
                        break :blk if (self.take(.colon)) .{ .entry = .{ .name = name_value, .value = try self.expr(0) } } else .{ .variable = name_value };
                    };
                    try items.append(self.a, item);
                    if (!self.take(.comma)) break;
                };
                try self.expect(.rbrace);
                left = try self.node(start, .{ .map_projection = .{ .base = left, .items = try items.toOwnedSlice(self.a) } });
                continue;
            }
            if (self.take(.dot)) {
                comparison_chain = false;
                left = try self.node(start, .{ .property = .{ .base = left, .name = try self.name() } });
                continue;
            }
            if (self.take(.lbracket)) {
                comparison_chain = false;
                const index = if (self.peek().kind == .range) null else try self.expr(0);
                if (self.take(.range)) {
                    const end = if (self.peek().kind == .rbracket) null else try self.expr(0);
                    if (index == null and end == null) return self.fail("a list slice requires at least one bound");
                    try self.expect(.rbracket);
                    left = try self.node(start, .{ .slice = .{ .base = left, .start = index, .end = end } });
                    continue;
                }
                try self.expect(.rbracket);
                left = try self.node(start, .{ .index = .{ .base = left, .index = index.? } });
                continue;
            }
            if (min <= 4 and self.word("IS")) {
                comparison_chain = false;
                const negated = self.word("NOT");
                try self.expectWord("NULL");
                left = try self.node(start, .{ .unary = .{ .op = if (negated) .is_not_null else .is_null, .operand = left } });
                continue;
            }
            const op_value = self.operator() orelse break;
            if (op_value.precedence < min) break;
            self.pos += 1;
            if (op_value.op == .starts_with or op_value.op == .ends_with) try self.expectWord("WITH");
            const right = try self.expr(op_value.precedence + @as(u8, if (op_value.op == .power) 0 else 1));
            const comparison = switch (op_value.op) {
                .eq, .neq, .lt, .lte, .gt, .gte => true,
                else => false,
            };
            if (comparison and comparison_chain) {
                if (left.value != .comparison) {
                    comparison_operands = .empty;
                    comparison_operators = .empty;
                    try comparison_operands.appendSlice(self.a, &.{ left.value.binary.left, left.value.binary.right });
                    try comparison_operators.append(self.a, left.value.binary.op);
                }
                try comparison_operands.append(self.a, right);
                try comparison_operators.append(self.a, op_value.op);
                left = try self.node(start, .{ .comparison = .{ .operands = comparison_operands.items, .operators = comparison_operators.items } });
            } else left = try self.node(start, .{ .binary = .{ .op = op_value.op, .left = left, .right = right } });
            comparison_chain = comparison;
        }
        return left;
    }
    fn operator(self: *Parser) ?struct { op: ast.Binary, precedence: u8 } {
        const t = self.peek();
        const op: ast.Binary = switch (t.kind) {
            .plus => .add,
            .minus => .subtract,
            .star => .multiply,
            .slash => .divide,
            .percent => .modulo,
            .caret => .power,
            .eq => .eq,
            .neq => .neq,
            .lt => .lt,
            .lte => .lte,
            .gt => .gt,
            .gte => .gte,
            .regex => .regex,
            .word => if (self.isWord("OR")) .@"or" else if (self.isWord("XOR")) .xor else if (self.isWord("AND")) .@"and" else if (self.isWord("IN")) .in else if (self.isWord("STARTS")) .starts_with else if (self.isWord("ENDS")) .ends_with else if (self.isWord("CONTAINS")) .contains else return null,
            else => return null,
        };
        return .{ .op = op, .precedence = switch (op) {
            .@"or" => 1,
            .xor => 2,
            .@"and" => 3,
            .add, .subtract => 5,
            .multiply, .divide, .modulo => 6,
            .power => 7,
            else => 4,
        } };
    }
    fn callName(self: *Parser, expression: *const ast.Expr) Error![]const u8 {
        return switch (expression.value) {
            .variable => |name_value| name_value,
            .property => |p| try std.fmt.allocPrint(self.a, "{s}.{s}", .{ try self.callName(p.base), p.name }),
            else => self.fail("expected a qualified scalar function name"),
        };
    }
    fn primary(self: *Parser) Error!*const ast.Expr {
        const t = self.peek();
        const start = t.span.start;
        if (self.word("NOT")) return self.node(start, .{ .unary = .{ .op = .not, .operand = try self.expr(4) } });
        if (self.take(.plus)) return self.node(start, .{ .unary = .{ .op = .positive, .operand = try self.expr(8) } });
        if (self.take(.minus)) {
            // The magnitude of the smallest INTEGER cannot be represented as
            // a positive i64 operand. Keep the signed literal in the AST.
            if (self.peek().kind == .integer and (std.fmt.parseInt(u64, self.peek().text, 10) catch 0) == @as(u64, 1) << 63) {
                self.pos += 1;
                return self.node(start, .{ .literal = .{ .integer = std.math.minInt(i64) } });
            }
            return self.node(start, .{ .unary = .{ .op = .negative, .operand = try self.expr(8) } });
        }
        if (self.word("NULL")) return self.node(start, .{ .literal = .null });
        if (self.word("TRUE")) return self.node(start, .{ .literal = .{ .boolean = true } });
        if (self.word("FALSE")) return self.node(start, .{ .literal = .{ .boolean = false } });
        if (self.take(.parameter)) {
            const name_value = try self.own(t.text);
            for (self.parameters.items) |existing| if (std.mem.eql(u8, existing, name_value)) return self.node(start, .{ .parameter = existing });
            try self.parameters.append(self.a, name_value);
            return self.node(start, .{ .parameter = name_value });
        }
        if (self.take(.string)) return self.node(start, .{ .literal = .{ .string = try self.own(t.text) } });
        if (self.take(.integer)) return self.node(start, .{ .literal = .{ .integer = std.fmt.parseInt(i64, t.text, 10) catch return error.InvalidCypherNumber } });
        if (self.take(.number)) {
            const number = std.fmt.parseFloat(f64, t.text) catch return error.InvalidCypherNumber;
            if (!std.math.isFinite(number)) return error.InvalidCypherNumber;
            return self.node(start, .{ .literal = .{ .number = number } });
        }
        if (self.take(.lparen)) {
            const result = try self.expr(0);
            try self.expect(.rparen);
            return result;
        }
        if (self.take(.lbracket)) {
            if (patternBracket(self.tokens, self.pos - 1)) {
                const pattern = try self.path();
                const predicate = if (self.word("WHERE")) try self.expr(0) else null;
                try self.expect(.pipe);
                const projection_value = try self.expr(0);
                try self.expect(.rbracket);
                return self.node(start, .{ .pattern_comprehension = .{ .path = pattern, .predicate = predicate, .projection = projection_value } });
            }
            if ((self.peek().kind == .word or self.peek().kind == .quoted_name) and self.tokens[self.pos + 1].kind == .word and std.ascii.eqlIgnoreCase(self.tokens[self.pos + 1].text, "IN")) {
                const alias = try self.name();
                try self.expectWord("IN");
                const input = try self.expr(0);
                const predicate = if (self.word("WHERE")) try self.expr(0) else null;
                const projection_value = if (self.take(.pipe)) try self.expr(0) else null;
                try self.expect(.rbracket);
                return self.node(start, .{ .comprehension = .{ .alias = alias, .input = input, .predicate = predicate, .projection = projection_value } });
            }
            const items = try self.expressions(.rbracket);
            try self.expect(.rbracket);
            return self.node(start, .{ .list = items });
        }
        if (self.peek().kind == .lbrace) return self.node(start, .{ .map = try self.map() });
        if (self.word("EXISTS")) {
            try self.expect(.lbrace);
            const clauses_value = try self.clauses();
            try self.expect(.rbrace);
            return self.node(start, .{ .exists = clauses_value });
        }
        if (self.word("CASE")) {
            const operand = if (!self.isWord("WHEN")) try self.expr(0) else null;
            var branches: std.ArrayList(ast.Branch) = .empty;
            while (self.word("WHEN")) {
                const condition = try self.expr(0);
                try self.expectWord("THEN");
                try branches.append(self.a, .{ .condition = condition, .value = try self.expr(0) });
            }
            const otherwise = if (self.word("ELSE")) try self.expr(0) else null;
            try self.expectWord("END");
            return self.node(start, .{ .case_when = .{ .operand = operand, .branches = try branches.toOwnedSlice(self.a), .otherwise = otherwise } });
        }
        if (t.kind == .word or t.kind == .quoted_name) {
            const name_value = try self.name();
            if (self.take(.lparen)) {
                if (std.ascii.eqlIgnoreCase(name_value, "all") or std.ascii.eqlIgnoreCase(name_value, "any") or std.ascii.eqlIgnoreCase(name_value, "none") or std.ascii.eqlIgnoreCase(name_value, "single")) {
                    const alias = try self.name();
                    try self.expectWord("IN");
                    const input = try self.expr(0);
                    try self.expectWord("WHERE");
                    const predicate = try self.expr(0);
                    try self.expect(.rparen);
                    const kind: @FieldType(@FieldType(ast.Expr, "value"), "list_predicate") = .{ .kind = if (std.ascii.eqlIgnoreCase(name_value, "all")) .all else if (std.ascii.eqlIgnoreCase(name_value, "any")) .any else if (std.ascii.eqlIgnoreCase(name_value, "none")) .none else .single, .alias = alias, .input = input, .predicate = predicate };
                    return self.node(start, .{ .list_predicate = kind });
                }
                if (std.ascii.eqlIgnoreCase(name_value, "reduce")) {
                    const accumulator = try self.name();
                    try self.expect(.eq);
                    const initial = try self.expr(0);
                    try self.expect(.comma);
                    const alias = try self.name();
                    if (std.mem.eql(u8, accumulator, alias)) return self.fail("reduce bindings must have different names");
                    try self.expectWord("IN");
                    const input = try self.expr(0);
                    try self.expect(.pipe);
                    const projection_value = try self.expr(0);
                    try self.expect(.rparen);
                    return self.node(start, .{ .reduce = .{ .accumulator = accumulator, .initial = initial, .alias = alias, .input = input, .projection = projection_value } });
                }
                const distinct = self.word("DISTINCT");
                const star = self.take(.star);
                const args = if (star) &.{} else try self.expressions(.rparen);
                try self.expect(.rparen);
                return self.node(start, .{ .call = .{ .name = name_value, .args = args, .distinct = distinct, .star = star } });
            }
            return self.node(start, .{ .variable = name_value });
        }
        return self.fail("expected graph expression");
    }
};

/// Walk only containers between expression pointers. Child expressions cache
/// their depth, so postfix/left-associative chains are checked in linear work.
/// EXISTS includes its clause bodies; nested CALL clauses add a scope level.
fn childDepth(value: anytype) usize {
    const T = @TypeOf(value);
    if (T == ast.Clause) return 1 + childDepth(value.operation);
    return switch (@typeInfo(T)) {
        .pointer => |p| switch (p.size) {
            .one => if (p.child == ast.Expr) value.depth else 0,
            .slice => blk: {
                if (p.child == u8) break :blk 0;
                var depth: usize = 0;
                for (value) |item| depth = @max(depth, childDepth(item));
                break :blk depth;
            },
            else => 0,
        },
        .optional => if (value) |child| childDepth(child) else 0,
        .@"union" => switch (value) {
            inline else => |child| childDepth(child),
        },
        .@"struct" => |s| blk: {
            var depth: usize = 0;
            inline for (s.field_names) |name| depth = @max(depth, childDepth(@field(value, name)));
            break :blk depth;
        },
        else => 0,
    };
}
