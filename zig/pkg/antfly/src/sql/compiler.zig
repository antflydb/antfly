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
const sql_parser = @import("sql_parser");
const lexer = sql_parser.lexer;
const token = sql_parser.token;
pub const ast = @import("ast.zig");
pub const Diagnostic = @import("diagnostics.zig").Diagnostic;

pub const Limits = struct {
    max_bytes: usize = 1 << 20,
    max_tokens: usize = 16_384,
    max_nodes: usize = 8_192,
    max_depth: usize = 64,
    max_parameters: u32 = 1_024,
    max_insert_rows: usize = 1_000,
};

pub const Error = std.mem.Allocator.Error || error{
    InvalidSqlSyntax,
    UnsupportedSqlShape,
    SqlLimitExceeded,
    InvalidSqlParameter,
    InvalidSqlNumber,
    DuplicateSqlColumn,
};

/// Immutable and schema independent: safely share a compiled statement between
/// readers, binding each execution against its own catalog snapshot and values.
/// Move-only owner; every string, literal, and node lives in this one arena.
pub const Compiled = struct {
    arena: std.heap.ArenaAllocator,
    statement: ast.Statement,
    parameter_count: u32,

    pub fn deinit(self: *Compiled) void {
        self.arena.deinit();
        self.* = undefined;
    }
};

pub fn compile(allocator: std.mem.Allocator, sql: []const u8, limits: Limits) Error!Compiled {
    var diagnostic: Diagnostic = .{};
    return compileDiagnostic(allocator, sql, limits, &diagnostic);
}

pub const CompiledScalar = struct {
    arena: std.heap.ArenaAllocator,
    expression: *const ast.Scalar,
    parameter_count: u32,

    pub fn deinit(self: *CompiledScalar) void {
        self.arena.deinit();
        self.* = undefined;
    }
};

pub fn compileScalar(allocator: std.mem.Allocator, sql: []const u8, limits: Limits) Error!CompiledScalar {
    if (sql.len > limits.max_bytes) return error.SqlLimitExceeded;
    var arena = std.heap.ArenaAllocator.init(allocator);
    errdefer arena.deinit();
    var scratch = std.heap.ArenaAllocator.init(allocator);
    defer scratch.deinit();
    const lexed = lexer.tokenizeBoundedDiagnosticAlloc(scratch.allocator(), sql, limits.max_tokens) catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        error.SqlTokenLimitExceeded => return error.SqlLimitExceeded,
        else => return error.InvalidSqlSyntax,
    };
    const tokens = switch (lexed) {
        .tokens => |value| value,
        .diagnostic => return error.InvalidSqlSyntax,
    };
    var diagnostic: Diagnostic = .{};
    var parser: Parser = .{ .alloc = arena.allocator(), .tokens = tokens.items, .source = sql, .limits = limits, .diagnostic = &diagnostic };
    const expression = try parser.scalar(0, 0);
    if (parser.pos != tokens.items.len) return error.InvalidSqlSyntax;
    try parser.checkScalarDepth(expression, 0);
    return .{ .arena = arena, .expression = expression, .parameter_count = parser.parameter_count };
}

pub fn compileDiagnostic(allocator: std.mem.Allocator, sql: []const u8, limits: Limits, diagnostic: *Diagnostic) Error!Compiled {
    diagnostic.* = .{};
    if (sql.len > limits.max_bytes) {
        diagnostic.message = "SQL statement byte budget exceeded";
        return error.SqlLimitExceeded;
    }
    var arena = std.heap.ArenaAllocator.init(allocator);
    errdefer arena.deinit();
    const alloc = arena.allocator();
    // Token buffers and decoded lexer strings are preparation scratch. Cached
    // plans retain only semantic data, not comments/source text/token capacity.
    var scratch = std.heap.ArenaAllocator.init(allocator);
    defer scratch.deinit();
    const result = lexer.tokenizeBoundedDiagnosticAlloc(scratch.allocator(), sql, limits.max_tokens) catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        error.SqlTokenLimitExceeded => {
            diagnostic.message = "SQL token budget exceeded";
            return error.SqlLimitExceeded;
        },
        else => return error.InvalidSqlSyntax,
    };
    const tokens = switch (result) {
        .tokens => |tokens| tokens,
        .diagnostic => |failure| {
            diagnostic.* = .{ .start = failure.source_start, .end = failure.source_end, .message = failure.message() };
            return error.InvalidSqlSyntax;
        },
    };
    if (tokens.items.len > limits.max_tokens) return error.SqlLimitExceeded;
    var parser: Parser = .{ .alloc = alloc, .tokens = tokens.items, .source = sql, .limits = limits, .diagnostic = diagnostic };
    const statement = try parser.statement();
    _ = parser.take(.semicolon);
    if (parser.pos != tokens.items.len) return parser.fail(error.UnsupportedSqlShape, "unexpected trailing SQL; only one supported statement is allowed");
    return .{ .arena = arena, .statement = statement, .parameter_count = parser.parameter_count };
}

const Parser = struct {
    alloc: std.mem.Allocator,
    tokens: []const token.Token,
    source: []const u8,
    limits: Limits,
    diagnostic: *Diagnostic,
    pos: usize = 0,
    node_count: usize = 0,
    parameter_count: u32 = 0,
    relation_depth: usize = 0,

    fn fail(self: *Parser, err: Error, message: []const u8) Error {
        const current = if (self.pos < self.tokens.len) self.tokens[self.pos] else null;
        self.diagnostic.* = .{
            .start = if (current) |t| t.source_start else self.source.len,
            .end = if (current) |t| t.source_end else self.source.len,
            .message = message,
        };
        return err;
    }

    fn node(self: *Parser) Error!void {
        if (self.node_count >= self.limits.max_nodes) return self.fail(error.SqlLimitExceeded, "SQL AST node budget exceeded");
        self.node_count += 1;
    }

    fn peek(self: *Parser, kind: token.TokenKind) bool {
        return self.pos < self.tokens.len and self.tokens[self.pos].kind == kind;
    }

    fn take(self: *Parser, kind: token.TokenKind) bool {
        if (!self.peek(kind)) return false;
        self.pos += 1;
        return true;
    }

    fn keyword(self: *Parser, word: token.TokenKeyword) bool {
        if (self.pos >= self.tokens.len or !self.tokens[self.pos].isKeyword(word)) return false;
        self.pos += 1;
        return true;
    }

    fn expect(self: *Parser, kind: token.TokenKind) Error!void {
        if (!self.take(kind)) return self.fail(error.InvalidSqlSyntax, "expected SQL punctuation");
    }

    fn expectKeyword(self: *Parser, word: token.TokenKeyword) Error!void {
        if (!self.keyword(word)) return self.fail(error.InvalidSqlSyntax, "expected SQL keyword");
    }

    fn identifier(self: *Parser) Error![]const u8 {
        if (!self.peek(.identifier)) return self.fail(error.InvalidSqlSyntax, "expected SQL identifier");
        const t = self.tokens[self.pos];
        if (std.mem.indexOfScalar(u8, t.text, 0) != null) return self.fail(error.InvalidSqlSyntax, "SQL identifiers cannot contain NUL bytes");
        if (t.keyword) |word| {
            if (token.keywordClass(word) == .reserved) return self.fail(error.InvalidSqlSyntax, "reserved keyword must be quoted when used as an identifier");
        }
        self.pos += 1;
        // Quoted identifiers preserve case; ordinary identifiers follow SQL's
        // ASCII folding rule. The lexer owns already-unescaped quoted strings.
        if (t.owned) return try self.alloc.dupe(u8, t.text);
        const folded = try self.alloc.dupe(u8, t.text);
        _ = std.ascii.lowerString(folded, folded);
        return folded;
    }

    fn name(self: *Parser) Error!ast.Name {
        const first = try self.identifier();
        if (!self.take(.dot)) return .{ .table = first };
        const second = try self.identifier();
        if (!self.take(.dot)) return .{ .namespace = first, .table = second };
        return .{ .database = first, .namespace = second, .table = try self.identifier() };
    }

    fn field(self: *Parser) Error![]const u8 {
        const result = try self.identifier();
        // Internal separator preserves the distinction between t.column and
        // the single quoted identifier "t.column" without reparsing names.
        if (self.take(.dot)) return std.fmt.allocPrint(self.alloc, "{s}\x00{s}", .{ result, try self.identifier() });
        return result;
    }

    fn value(self: *Parser) Error!ast.Value {
        try self.node();
        if (self.keyword(.null)) return .null;
        if (self.keyword(.true)) return .{ .boolean = true };
        if (self.keyword(.false)) return .{ .boolean = false };
        if (self.peek(.placeholder)) {
            const t = self.tokens[self.pos];
            self.pos += 1;
            const index = std.fmt.parseInt(u32, t.text[1..], 10) catch return self.fail(error.InvalidSqlParameter, "invalid positional parameter");
            if (index == 0 or index > self.limits.max_parameters) return self.fail(error.InvalidSqlParameter, "parameter index exceeds configured bound");
            self.parameter_count = @max(self.parameter_count, index);
            return .{ .parameter = index };
        }
        if (self.peek(.string)) {
            const t = self.tokens[self.pos];
            self.pos += 1;
            return .{ .string = try self.alloc.dupe(u8, t.text) };
        }
        const negative = self.take(.minus);
        if (!negative) _ = self.take(.plus);
        if (!self.peek(.number)) return self.fail(error.UnsupportedSqlShape, "expected a literal or positional parameter; expression is not supported");
        const t = self.tokens[self.pos];
        self.pos += 1;
        if (std.mem.indexOfAny(u8, t.text, ".eE") != null) {
            const parsed = std.fmt.parseFloat(f64, t.text) catch return self.fail(error.InvalidSqlNumber, "invalid numeric literal");
            if (!std.math.isFinite(parsed)) return self.fail(error.InvalidSqlNumber, "numeric literal must be finite");
            return .{ .number = if (negative) -parsed else parsed };
        }
        const magnitude = std.fmt.parseInt(u64, t.text, 10) catch return self.fail(error.InvalidSqlNumber, "integer literal exceeds 64-bit range");
        if (negative) {
            if (magnitude > @as(u64, std.math.maxInt(i64)) + 1) return self.fail(error.InvalidSqlNumber, "integer literal exceeds 64-bit range");
            if (magnitude == @as(u64, std.math.maxInt(i64)) + 1) return .{ .integer = std.math.minInt(i64) };
            return .{ .integer = -@as(i64, @intCast(magnitude)) };
        }
        if (magnitude > std.math.maxInt(i64)) return self.fail(error.InvalidSqlNumber, "integer literal exceeds 64-bit range");
        return .{ .integer = @intCast(magnitude) };
    }

    fn scalarNode(self: *Parser, expression: ast.Scalar) Error!*const ast.Scalar {
        try self.node();
        const node_value = try self.alloc.create(ast.Scalar);
        node_value.* = expression;
        return node_value;
    }

    fn scalar(self: *Parser, depth: usize, minimum: u8) Error!*const ast.Scalar {
        if (depth >= self.limits.max_depth) return self.fail(error.SqlLimitExceeded, "SQL scalar nesting budget exceeded");
        var left: *const ast.Scalar = undefined;
        if (self.keyword(.not)) {
            left = try self.scalarNode(.{ .unary = .{ .op = .not, .operand = try self.scalar(depth + 1, 3) } });
        } else if (self.take(.lparen)) {
            left = try self.scalar(depth + 1, 0);
            try self.expect(.rparen);
        } else if (self.keyword(.cast)) {
            try self.expect(.lparen);
            const operand = try self.scalar(depth + 1, 0);
            try self.expectKeyword(.as);
            const kind = try self.columnType();
            try self.expect(.rparen);
            left = try self.scalarNode(.{ .cast = .{ .operand = operand, .type = kind } });
        } else if (self.keyword(.case)) {
            const base = if (self.pos < self.tokens.len and self.tokens[self.pos].isKeyword(.when)) null else try self.scalar(depth + 1, 0);
            var branches: std.ArrayList(ast.Scalar.Branch) = .empty;
            while (self.keyword(.when)) {
                var condition = try self.scalar(depth + 1, 0);
                if (base) |base_expression| condition = try self.scalarNode(.{ .binary = .{ .op = .eq, .left = base_expression, .right = condition } });
                try self.expectKeyword(.then);
                try branches.append(self.alloc, .{ .condition = condition, .value = try self.scalar(depth + 1, 0) });
            }
            if (branches.items.len == 0) return self.fail(error.InvalidSqlSyntax, "CASE requires WHEN");
            const otherwise = if (self.keyword(.@"else")) try self.scalar(depth + 1, 0) else null;
            try self.expectKeyword(.end);
            left = try self.scalarNode(.{ .case_when = .{ .branches = try branches.toOwnedSlice(self.alloc), .otherwise = otherwise } });
        } else if ((self.peek(.minus) or self.peek(.plus)) and !(self.pos + 1 < self.tokens.len and self.tokens[self.pos + 1].kind == .number)) {
            const negative = self.take(.minus);
            if (!negative) try self.expect(.plus);
            left = try self.scalarNode(.{ .unary = .{ .op = if (negative) .negative else .positive, .operand = try self.scalar(depth + 1, 7) } });
        } else if (self.peek(.identifier) and self.pos + 1 < self.tokens.len and self.tokens[self.pos + 1].kind == .lparen) {
            const function = self.tokens[self.pos];
            self.pos += 2;
            const name_value = try self.alloc.dupe(u8, function.text);
            if (!function.owned) _ = std.ascii.lowerString(name_value, name_value);
            var args: std.ArrayList(*const ast.Scalar) = .empty;
            const distinct = self.keyword(.distinct);
            const star = self.take(.star);
            if (star) {
                if (!std.mem.eql(u8, name_value, "count")) return self.fail(error.InvalidSqlSyntax, "only COUNT accepts a star argument");
                try self.expect(.rparen);
            } else if (!self.take(.rparen)) {
                while (true) {
                    try args.append(self.alloc, try self.scalar(depth + 1, 0));
                    if (!self.take(.comma)) break;
                }
                try self.expect(.rparen);
            }
            var filter: ?*const ast.Scalar = null;
            if (self.keyword(.filter)) {
                try self.expect(.lparen);
                try self.expectKeyword(.where);
                filter = try self.scalar(depth + 1, 0);
                try self.expect(.rparen);
            }
            left = try self.scalarNode(.{ .call = .{ .name = name_value, .args = try args.toOwnedSlice(self.alloc), .star = star, .distinct = distinct, .filter = filter } });
        } else if (self.peek(.identifier) and !self.tokens[self.pos].isKeyword(.null) and !self.tokens[self.pos].isKeyword(.true) and !self.tokens[self.pos].isKeyword(.false)) {
            left = try self.scalarNode(.{ .column = try self.field() });
        } else left = try self.scalarNode(.{ .literal = try self.value() });

        while (self.pos < self.tokens.len) {
            if (minimum <= 3) {
                const negated = self.tokens[self.pos].isKeyword(.not) and self.pos + 1 < self.tokens.len and (self.tokens[self.pos + 1].isKeyword(.in) or self.tokens[self.pos + 1].isKeyword(.between) or self.tokens[self.pos + 1].isKeyword(.like) or self.tokens[self.pos + 1].isKeyword(.ilike));
                if (negated) self.pos += 1;
                if (self.keyword(.between)) {
                    const low = try self.scalar(depth + 1, 4);
                    try self.expectKeyword(.@"and");
                    const high = try self.scalar(depth + 1, 4);
                    left = try self.scalarNode(.{ .binary = .{ .op = .@"and", .left = try self.scalarNode(.{ .binary = .{ .op = .gte, .left = left, .right = low } }), .right = try self.scalarNode(.{ .binary = .{ .op = .lte, .left = left, .right = high } }) } });
                    if (negated) left = try self.scalarNode(.{ .unary = .{ .op = .not, .operand = left } });
                    continue;
                }
                if (self.keyword(.in)) {
                    try self.expect(.lparen);
                    var values: std.ArrayList(*const ast.Scalar) = .empty;
                    while (true) {
                        try values.append(self.alloc, try self.scalar(depth + 1, 0));
                        if (!self.take(.comma)) break;
                    }
                    try self.expect(.rparen);
                    left = try self.scalarNode(.{ .in_list = .{ .operand = left, .values = try values.toOwnedSlice(self.alloc), .negated = negated } });
                    continue;
                }
                if (negated) {
                    const insensitive = self.keyword(.ilike);
                    if (!insensitive) try self.expectKeyword(.like);
                    left = try self.scalarNode(.{ .unary = .{ .op = .not, .operand = try self.scalarNode(.{ .binary = .{ .op = if (insensitive) .ilike else .like, .left = left, .right = try self.scalar(depth + 1, 4) } }) } });
                    continue;
                }
            }
            if (self.peek(.colon_colon)) {
                if (minimum > 8) break;
                self.pos += 1;
                left = try self.scalarNode(.{ .cast = .{ .operand = left, .type = try self.columnType() } });
                continue;
            }
            if (minimum <= 3 and self.keyword(.is)) {
                const negated = self.keyword(.not);
                if (self.keyword(.distinct)) {
                    try self.expectKeyword(.from);
                    left = try self.scalarNode(.{ .binary = .{ .op = if (negated) .is_not_distinct else .is_distinct, .left = left, .right = try self.scalar(depth + 1, 4) } });
                } else {
                    const op: ast.Scalar.Unary = if (self.keyword(.null)) (if (negated) .is_not_null else .is_null) else if (self.keyword(.true)) (if (negated) .is_not_true else .is_true) else if (self.keyword(.false)) (if (negated) .is_not_false else .is_false) else return self.fail(error.InvalidSqlSyntax, "expected NULL, TRUE, FALSE or DISTINCT");
                    left = try self.scalarNode(.{ .unary = .{ .op = op, .operand = left } });
                }
                continue;
            }
            const current = self.tokens[self.pos];
            const op: ast.Scalar.Binary = switch (current.kind) {
                .plus => .add,
                .minus => .subtract,
                .star => .multiply,
                .slash => .divide,
                .percent => .modulo,
                .pipe_concat => .concat,
                .arrow_json => .json_get,
                .arrow_text => .json_text,
                .eq => .eq,
                .neq => .neq,
                .lt => .lt,
                .lte => .lte,
                .gt => .gt,
                .gte => .gte,
                else => if (current.isKeyword(.@"and")) .@"and" else if (current.isKeyword(.@"or")) .@"or" else if (current.isKeyword(.like)) .like else if (current.isKeyword(.ilike)) .ilike else break,
            };
            const precedence: u8 = switch (op) {
                .@"or" => 1,
                .@"and" => 2,
                .eq, .neq, .lt, .lte, .gt, .gte, .like, .ilike, .is_distinct, .is_not_distinct => 3,
                .concat => 4,
                .add, .subtract => 5,
                .multiply, .divide, .modulo => 6,
                .json_get, .json_text => 7,
            };
            if (precedence < minimum) break;
            self.pos += 1;
            left = try self.scalarNode(.{ .binary = .{ .op = op, .left = left, .right = try self.scalar(depth + 1, precedence + 1) } });
        }
        return left;
    }

    fn checkScalarDepth(self: *Parser, expression: *const ast.Scalar, depth: usize) Error!void {
        if (depth >= self.limits.max_depth) return self.fail(error.SqlLimitExceeded, "SQL scalar tree depth budget exceeded");
        switch (expression.*) {
            .unary => |part| try self.checkScalarDepth(part.operand, depth + 1),
            .binary => |part| {
                try self.checkScalarDepth(part.left, depth + 1);
                try self.checkScalarDepth(part.right, depth + 1);
            },
            .cast => |part| try self.checkScalarDepth(part.operand, depth + 1),
            .call => |part| {
                for (part.args) |arg| try self.checkScalarDepth(arg, depth + 1);
                if (part.filter) |filter| try self.checkScalarDepth(filter, depth + 1);
            },
            .case_when => |part| {
                for (part.branches) |branch| {
                    try self.checkScalarDepth(branch.condition, depth + 1);
                    try self.checkScalarDepth(branch.value, depth + 1);
                }
                if (part.otherwise) |other| try self.checkScalarDepth(other, depth + 1);
            },
            .in_list => |part| {
                try self.checkScalarDepth(part.operand, depth + 1);
                for (part.values) |item| try self.checkScalarDepth(item, depth + 1);
            },
            .literal, .column => {},
        }
    }

    fn predicateNode(self: *Parser, expression: ast.Predicate) Error!*const ast.Predicate {
        try self.node();
        const result = try self.alloc.create(ast.Predicate);
        result.* = expression;
        return result;
    }

    fn predicate(self: *Parser, depth: usize, minimum: u8) Error!*const ast.Predicate {
        if (depth >= self.limits.max_depth) return self.fail(error.SqlLimitExceeded, "SQL expression nesting budget exceeded");
        if (minimum <= 2) {
            const conjunction = minimum == 2;
            const next: u8 = if (conjunction) 3 else 2;
            const first = try self.predicate(depth, next);
            const operator: token.TokenKeyword = if (conjunction) .@"and" else .@"or";
            if (!self.keyword(operator)) return first;
            // Preserve precedence while balancing associative boolean chains.
            // This keeps downstream binding/evaluation logarithmic in stack
            // depth even for thousands of AND/OR terms.
            var operands = std.ArrayList(*const ast.Predicate).empty;
            try operands.append(self.alloc, first);
            while (true) {
                try operands.append(self.alloc, try self.predicate(depth, next));
                if (!self.keyword(operator)) break;
            }
            return self.balancedPredicate(operands.items, conjunction);
        }
        var left: *const ast.Predicate = undefined;
        if (self.keyword(.not)) {
            left = try self.predicateNode(.{ .negation = try self.predicate(depth + 1, 3) });
        } else if (self.take(.lparen)) {
            left = try self.predicate(depth + 1, 0);
            try self.expect(.rparen);
        } else {
            const column = try self.field();
            if (self.keyword(.is)) {
                const negated = self.keyword(.not);
                try self.expectKeyword(.null);
                left = try self.predicateNode(.{ .is_null = .{ .field = column, .negated = negated } });
            } else {
                if (self.pos >= self.tokens.len) return self.fail(error.InvalidSqlSyntax, "expected comparison operator");
                const op: ast.Comparison = switch (self.tokens[self.pos].kind) {
                    .eq => .eq,
                    .neq => .neq,
                    .lt => .lt,
                    .lte => .lte,
                    .gt => .gt,
                    .gte => .gte,
                    else => return self.fail(error.UnsupportedSqlShape, "unsupported SQL predicate operator"),
                };
                self.pos += 1;
                left = try self.predicateNode(.{ .comparison = .{ .field = column, .op = op, .value = try self.value() } });
            }
        }
        return left;
    }

    fn balancedPredicate(self: *Parser, operands: []const *const ast.Predicate, conjunction: bool) Error!*const ast.Predicate {
        if (operands.len == 1) return operands[0];
        const middle = operands.len / 2;
        const left = try self.balancedPredicate(operands[0..middle], conjunction);
        const right = try self.balancedPredicate(operands[middle..], conjunction);
        return self.predicateNode(if (conjunction) .{ .conjunction = .{ .left = left, .right = right } } else .{ .disjunction = .{ .left = left, .right = right } });
    }

    fn checkPredicateDepth(self: *Parser, expression: *const ast.Predicate, depth: usize) Error!void {
        if (depth >= self.limits.max_depth) return self.fail(error.SqlLimitExceeded, "SQL expression tree depth budget exceeded");
        switch (expression.*) {
            .conjunction, .disjunction => |binary| {
                try self.checkPredicateDepth(binary.left, depth + 1);
                try self.checkPredicateDepth(binary.right, depth + 1);
            },
            .negation => |operand| try self.checkPredicateDepth(operand, depth + 1),
            .scalar => |expression_node| try self.checkScalarDepth(expression_node, depth + 1),
            .comparison, .is_null => {},
        }
    }

    fn where(self: *Parser) Error!?*const ast.Predicate {
        if (!self.keyword(.where)) return null;
        const start = self.pos;
        const result = self.predicate(0, 0) catch |err| switch (err) {
            error.InvalidSqlSyntax, error.UnsupportedSqlShape => {
                self.pos = start;
                const expression = try self.scalar(0, 0);
                try self.checkScalarDepth(expression, 0);
                return self.predicateNode(.{ .scalar = expression });
            },
            else => return err,
        };
        if (self.pos < self.tokens.len and self.tokens[self.pos].kind != .semicolon and !self.tokens[self.pos].isKeyword(.order) and !self.tokens[self.pos].isKeyword(.limit) and !self.tokens[self.pos].isKeyword(.offset) and !self.tokens[self.pos].isKeyword(.returning)) {
            self.pos = start;
            const expression = try self.scalar(0, 0);
            try self.checkScalarDepth(expression, 0);
            return self.predicateNode(.{ .scalar = expression });
        }
        try self.checkPredicateDepth(result, 0);
        return result;
    }

    fn select(self: *Parser) Error!ast.Select {
        return self.selectFinish(try self.selectCore());
    }

    fn selectFinish(self: *Parser, first: ast.Select) Error!ast.Select {
        var result = try self.setTail(first, 1, 0);
        result.order_by = try self.selectOrder();
        result.limit = if (self.keyword(.limit)) try self.rowBound() else null;
        result.offset = if (self.keyword(.offset)) try self.rowBound() else null;
        if (result.columns.len == 1 and result.group_by.len == 0 and result.having == null and result.order_by.len == 0) {
            if (result.columns[0].expression) |expression| if (expression.* == .call and expression.call.star and !expression.call.distinct and expression.call.filter == null and std.mem.eql(u8, expression.call.name, "count")) {
                result.count_all = true;
                result.count_alias = result.columns[0].alias;
                result.columns = &.{};
            };
        }
        return result;
    }

    fn setTail(self: *Parser, first: ast.Select, minimum: u8, depth: usize) Error!ast.Select {
        if (depth >= self.limits.max_depth) return self.fail(error.SqlLimitExceeded, "SQL set nesting limit exceeded");
        var left = first;
        while (self.pos < self.tokens.len) {
            const next_token = self.tokens[self.pos];
            const kind: ast.SetKind = if (next_token.isKeyword(.@"union")) .@"union" else if (next_token.isKeyword(.intersect)) .intersect else if (next_token.isKeyword(.except)) .except else break;
            const precedence: u8 = if (kind == .intersect) 2 else 1;
            if (precedence < minimum) break;
            self.pos += 1;
            const all = self.keyword(.all);
            if (!all) _ = self.keyword(.distinct);
            var right: ast.Select = undefined;
            if (self.take(.lparen)) {
                self.relation_depth += 1;
                defer self.relation_depth -= 1;
                if (self.relation_depth >= self.limits.max_depth) return self.fail(error.SqlLimitExceeded, "SQL set nesting limit exceeded");
                const statement_value = try self.statement();
                if (statement_value != .select) return self.fail(error.InvalidSqlSyntax, "set operands require SELECT");
                right = statement_value.select;
                try self.expect(.rparen);
            } else {
                try self.expectKeyword(.select);
                right = try self.selectCore();
            }
            right = try self.setTail(right, precedence + 1, depth + 1);
            try self.node();
            const left_ptr = try self.alloc.create(ast.Select);
            left_ptr.* = left;
            const right_ptr = try self.alloc.create(ast.Select);
            right_ptr.* = right;
            left = .{ .set_operation = .{ .kind = kind, .all = all, .left = left_ptr, .right = right_ptr } };
        }
        return left;
    }

    fn selectCore(self: *Parser) Error!ast.Select {
        var columns = std.ArrayList(ast.Projection).empty;
        if (self.take(.star)) {
            // Wildcard must stand alone in this execution shape.
        } else {
            while (true) {
                try self.node();
                const expression = try self.scalar(0, 0);
                try self.checkScalarDepth(expression, 0);
                const alias = if (self.keyword(.as)) try self.identifier() else null;
                try columns.append(self.alloc, if (expression.* == .column) .{ .field = expression.column, .alias = alias } else .{ .expression = expression, .alias = alias });
                if (!self.take(.comma)) break;
            }
        }
        const source = if (self.keyword(.from)) try self.relation() else null;
        const simple = source != null and source.?.* == .table and source.?.table.alias == null;
        const table = if (simple) source.?.table.name else null;
        const filter = try self.where();
        var group_by: std.ArrayList(*const ast.Scalar) = .empty;
        if (self.keyword(.group)) {
            try self.expectKeyword(.by);
            while (true) {
                try group_by.append(self.alloc, try self.scalar(0, 0));
                if (!self.take(.comma)) break;
            }
        }
        const having = if (self.keyword(.having)) try self.scalar(0, 0) else null;
        return .{ .table = table, .source = if (simple) null else source, .columns = try columns.toOwnedSlice(self.alloc), .predicate = filter, .group_by = try group_by.toOwnedSlice(self.alloc), .having = having };
    }

    fn selectOrder(self: *Parser) Error![]const ast.Order {
        var order_by = std.ArrayList(ast.Order).empty;
        if (self.keyword(.order)) {
            try self.expectKeyword(.by);
            while (true) {
                try self.node();
                const expression = try self.scalar(0, 0);
                try self.checkScalarDepth(expression, 0);
                const descending = self.keyword(.desc);
                if (!descending) _ = self.keyword(.asc);
                var order: ast.Order = .{ .descending = descending };
                if (expression.* == .column) order.field = expression.column else if (expression.* == .literal and expression.literal == .integer) {
                    order.position = std.math.cast(u32, expression.literal.integer) orelse return self.fail(error.InvalidSqlSyntax, "ORDER BY position must be a positive integer");
                    if (order.position.? == 0) return self.fail(error.InvalidSqlSyntax, "ORDER BY position must be a positive integer");
                } else order.expression = expression;
                if (self.keyword(.nulls)) {
                    if (self.keyword(.first)) order.nulls_first = true else {
                        try self.expectKeyword(.last);
                        order.nulls_first = false;
                    }
                }
                try order_by.append(self.alloc, order);
                if (!self.take(.comma)) break;
            }
        }
        return order_by.toOwnedSlice(self.alloc);
    }

    fn relationNode(self: *Parser, relation_value: ast.Relation) Error!*const ast.Relation {
        try self.node();
        const result = try self.alloc.create(ast.Relation);
        result.* = relation_value;
        return result;
    }

    fn sourceAlias(self: *Parser) Error!?[]const u8 {
        if (self.keyword(.as)) return try self.identifier();
        if (self.peek(.identifier) and (self.tokens[self.pos].keyword == null or self.tokens[self.pos].owned)) return try self.identifier();
        return null;
    }

    fn relationAtom(self: *Parser) Error!*const ast.Relation {
        if (self.take(.lparen)) {
            self.relation_depth += 1;
            defer self.relation_depth -= 1;
            if (self.relation_depth >= self.limits.max_depth) return self.fail(error.SqlLimitExceeded, "SQL relation nesting limit exceeded");
            const statement_value = try self.statement();
            if (statement_value != .select) return self.fail(error.InvalidSqlSyntax, "derived relation requires SELECT");
            try self.expect(.rparen);
            const alias = try self.sourceAlias() orelse return self.fail(error.InvalidSqlSyntax, "derived relation requires an alias");
            const query = try self.alloc.create(ast.Select);
            query.* = statement_value.select;
            return self.relationNode(.{ .derived = .{ .query = query, .alias = alias } });
        }
        const name_value = try self.name();
        return self.relationNode(.{ .table = .{ .name = name_value, .alias = try self.sourceAlias() } });
    }

    fn relation(self: *Parser) Error!*const ast.Relation {
        var left = try self.relationAtom();
        var depth: usize = 1;
        while (true) {
            var kind: ast.JoinKind = .inner;
            if (self.take(.comma)) kind = .cross else if (self.keyword(.join)) {} else {
                if (self.keyword(.inner)) kind = .inner else if (self.keyword(.left)) kind = .left else if (self.keyword(.right)) kind = .right else if (self.keyword(.full)) kind = .full else if (self.keyword(.cross)) kind = .cross else break;
                _ = self.keyword(.outer);
                try self.expectKeyword(.join);
            }
            depth += 1;
            if (depth >= self.limits.max_depth) return self.fail(error.SqlLimitExceeded, "SQL join nesting limit exceeded");
            const right = try self.relationAtom();
            const condition = if (kind == .cross) null else blk: {
                try self.expectKeyword(.on);
                break :blk try self.scalar(0, 0);
            };
            left = try self.relationNode(.{ .join = .{ .kind = kind, .left = left, .right = right, .condition = condition } });
        }
        return left;
    }

    fn rowBound(self: *Parser) Error!ast.Value {
        const result = try self.value();
        switch (result) {
            .integer => |v| if (v < 0) return self.fail(error.InvalidSqlSyntax, "row bound must be nonnegative"),
            .parameter => {},
            else => return self.fail(error.InvalidSqlSyntax, "row bound must be an integer or parameter"),
        }
        return result;
    }

    fn insert(self: *Parser) Error!ast.Insert {
        try self.expectKeyword(.into);
        const table = try self.name();
        try self.expect(.lparen);
        var columns = std.ArrayList([]const u8).empty;
        var seen: std.StringHashMapUnmanaged(void) = .empty;
        while (true) {
            try self.node();
            const column = try self.field();
            const entry = try seen.getOrPut(self.alloc, column);
            if (entry.found_existing) return self.fail(error.DuplicateSqlColumn, "duplicate INSERT column");
            try columns.append(self.alloc, column);
            if (!self.take(.comma)) break;
        }
        try self.expect(.rparen);
        if (self.pos < self.tokens.len and (self.tokens[self.pos].isKeyword(.select) or self.tokens[self.pos].isKeyword(.with))) {
            const source = try self.alloc.create(ast.Select);
            const statement_ = try self.statement();
            if (statement_ != .select) return self.fail(error.InvalidSqlSyntax, "INSERT source must be SELECT");
            source.* = statement_.select;
            return .{ .table = table, .columns = try columns.toOwnedSlice(self.alloc), .source = source, .returning = try self.returning() };
        }
        try self.expectKeyword(.values);
        var rows = std.ArrayList([]const ast.Value).empty;
        var expressions = std.ArrayList([]const ?*const ast.Scalar).empty;
        while (true) {
            if (rows.items.len >= self.limits.max_insert_rows) return self.fail(error.SqlLimitExceeded, "INSERT row budget exceeded");
            try self.expect(.lparen);
            var row = std.ArrayList(ast.Value).empty;
            var row_expressions = std.ArrayList(?*const ast.Scalar).empty;
            while (true) {
                const expression = try self.scalar(0, 0);
                try self.checkScalarDepth(expression, 0);
                try row.append(self.alloc, if (expression.* == .literal) expression.literal else .null);
                try row_expressions.append(self.alloc, if (expression.* == .literal) null else expression);
                if (!self.take(.comma)) break;
            }
            try self.expect(.rparen);
            if (row.items.len != columns.items.len) return self.fail(error.InvalidSqlSyntax, "INSERT values count does not match columns");
            try rows.append(self.alloc, try row.toOwnedSlice(self.alloc));
            try expressions.append(self.alloc, try row_expressions.toOwnedSlice(self.alloc));
            if (!self.take(.comma)) break;
        }
        return .{ .table = table, .columns = try columns.toOwnedSlice(self.alloc), .rows = try rows.toOwnedSlice(self.alloc), .expressions = try expressions.toOwnedSlice(self.alloc), .returning = try self.returning() };
    }

    fn update(self: *Parser) Error!ast.Update {
        const table = try self.name();
        try self.expectKeyword(.set);
        var assignments = std.ArrayList(ast.Assignment).empty;
        var seen: std.StringHashMapUnmanaged(void) = .empty;
        while (true) {
            try self.node();
            const column = try self.field();
            const entry = try seen.getOrPut(self.alloc, column);
            if (entry.found_existing) return self.fail(error.DuplicateSqlColumn, "duplicate UPDATE assignment");
            try self.expect(.eq);
            const expression = try self.scalar(0, 0);
            try self.checkScalarDepth(expression, 0);
            try assignments.append(self.alloc, if (expression.* == .literal) .{ .field = column, .value = expression.literal } else .{ .field = column, .expression = expression });
            if (!self.take(.comma)) break;
        }
        return .{ .table = table, .assignments = try assignments.toOwnedSlice(self.alloc), .predicate = try self.where(), .returning = try self.returning() };
    }

    fn returning(self: *Parser) Error!?[]const ast.Projection {
        if (!self.keyword(.returning)) return null;
        if (self.take(.star)) return &.{};
        var columns: std.ArrayList(ast.Projection) = .empty;
        while (true) {
            try self.node();
            const expression = try self.scalar(0, 0);
            try self.checkScalarDepth(expression, 0);
            const alias = if (self.keyword(.as)) try self.identifier() else null;
            try columns.append(self.alloc, if (expression.* == .column) .{ .field = expression.column, .alias = alias } else .{ .expression = expression, .alias = alias });
            if (!self.take(.comma)) break;
        }
        return try columns.toOwnedSlice(self.alloc);
    }

    fn columnType(self: *Parser) Error!ast.ColumnType {
        if (!self.peek(.identifier)) return self.fail(error.InvalidSqlSyntax, "expected SQL column type");
        const t = self.tokens[self.pos];
        self.pos += 1;
        // Quoted type names denote user-defined types, which require catalog
        // lookup; never silently reinterpret them as builtins.
        if (t.owned) return self.fail(error.UnsupportedSqlShape, "user-defined column types are not supported");
        const pairs = .{
            .{ "text", ast.ColumnType.string },       .{ "string", ast.ColumnType.string },
            .{ "bigint", ast.ColumnType.integer },    .{ "int8", ast.ColumnType.integer },
            .{ "integer", ast.ColumnType.integer },   .{ "int", ast.ColumnType.integer },
            .{ "float8", ast.ColumnType.number },     .{ "number", ast.ColumnType.number },
            .{ "boolean", ast.ColumnType.boolean },   .{ "bool", ast.ColumnType.boolean },
            .{ "datetime", ast.ColumnType.datetime }, .{ "timestamptz", ast.ColumnType.datetime },
            .{ "json", ast.ColumnType.json },         .{ "jsonb", ast.ColumnType.json },
        };
        inline for (pairs) |pair| if (std.ascii.eqlIgnoreCase(t.text, pair[0])) return pair[1];
        if (std.ascii.eqlIgnoreCase(t.text, "double")) {
            if (!self.peek(.identifier) or self.tokens[self.pos].owned or !std.ascii.eqlIgnoreCase(self.tokens[self.pos].text, "precision")) return self.fail(error.InvalidSqlSyntax, "expected DOUBLE PRECISION");
            self.pos += 1;
            return .number;
        }
        return self.fail(error.UnsupportedSqlShape, "unsupported SQL column type");
    }

    fn createTable(self: *Parser) Error!ast.CreateTable {
        const if_not_exists = self.keyword(.@"if");
        if (if_not_exists) {
            try self.expectKeyword(.not);
            try self.expectKeyword(.exists);
        }
        const table = try self.name();
        try self.expect(.lparen);
        var columns = std.ArrayList(ast.Column).empty;
        var constraints = std.ArrayList(ast.SchemaChange).empty;
        var seen: std.StringHashMapUnmanaged(void) = .empty;
        while (true) {
            try self.node();
            if (self.keyword(.constraint)) {
                try constraints.append(self.alloc, try self.tableConstraint());
                if (!self.take(.comma)) break;
                continue;
            }
            if (self.tokens[self.pos].isKeyword(.primary) or self.tokens[self.pos].isKeyword(.unique) or self.tokens[self.pos].isKeyword(.check) or self.tokens[self.pos].isKeyword(.foreign)) {
                const constraint_name = try std.fmt.allocPrint(self.alloc, "sql_constraint_{d}", .{constraints.items.len});
                try constraints.append(self.alloc, try self.constraintDefinition(constraint_name));
                if (!self.take(.comma)) break;
                continue;
            }
            const column = try self.identifier();
            if (std.mem.eql(u8, column, "_id")) return self.fail(error.UnsupportedSqlShape, "_id is reserved for row identity");
            const entry = try seen.getOrPut(self.alloc, column);
            if (entry.found_existing) return self.fail(error.DuplicateSqlColumn, "duplicate CREATE TABLE column");
            var definition: ast.Column = .{ .name = column, .type = try self.columnType() };
            var null_seen = false;
            var default_seen = false;
            while (true) {
                if (self.keyword(.not)) {
                    if (null_seen) return self.fail(error.InvalidSqlSyntax, "duplicate column nullability");
                    try self.expectKeyword(.null);
                    definition.nullable = false;
                    null_seen = true;
                } else if (self.keyword(.null)) {
                    if (null_seen) return self.fail(error.InvalidSqlSyntax, "duplicate column nullability");
                    null_seen = true;
                } else if (self.keyword(.default)) {
                    if (default_seen) return self.fail(error.InvalidSqlSyntax, "duplicate column default");
                    definition.default_value = try self.value();
                    if (definition.default_value.? == .parameter) return self.fail(error.UnsupportedSqlShape, "schema defaults cannot contain execution parameters");
                    default_seen = true;
                } else if (self.keyword(.primary)) {
                    try self.expectKeyword(.key);
                    definition.nullable = false;
                    try constraints.append(self.alloc, .{ .add_unique = .{ .name = try std.fmt.allocPrint(self.alloc, "sql_primary_{d}", .{constraints.items.len}), .columns = try self.alloc.dupe([]const u8, &.{column}), .primary = true } });
                } else if (self.keyword(.unique)) {
                    try constraints.append(self.alloc, .{ .add_unique = .{ .name = try std.fmt.allocPrint(self.alloc, "sql_unique_{d}", .{constraints.items.len}), .columns = try self.alloc.dupe([]const u8, &.{column}) } });
                } else if (self.keyword(.constraint)) {
                    const constraint_name = try self.identifier();
                    try constraints.append(self.alloc, try self.inlineConstraint(constraint_name, column));
                } else if (self.tokens[self.pos].isKeyword(.check) or std.ascii.eqlIgnoreCase(self.tokens[self.pos].text, "REFERENCES")) {
                    const constraint_name = try std.fmt.allocPrint(self.alloc, "sql_inline_{d}", .{constraints.items.len});
                    try constraints.append(self.alloc, try self.inlineConstraint(constraint_name, column));
                } else break;
            }
            try columns.append(self.alloc, definition);
            if (!self.take(.comma)) break;
        }
        try self.expect(.rparen);
        const tablespace = if (self.keyword(.tablespace)) try self.identifier() else null;
        var has_primary = false;
        for (constraints.items) |constraint| if (constraint == .add_unique and constraint.add_unique.primary) {
            if (has_primary) return self.fail(error.InvalidSqlSyntax, "a table can have only one primary key");
            has_primary = true;
            for (constraint.add_unique.columns) |key| {
                for (columns.items) |*definition| {
                    if (std.mem.eql(u8, key, definition.name)) {
                        definition.nullable = false;
                        break;
                    }
                } else return self.fail(error.InvalidSqlSyntax, "primary key references an unknown column");
            }
        };
        return .{ .table = table, .columns = try columns.toOwnedSlice(self.alloc), .constraints = try constraints.toOwnedSlice(self.alloc), .if_not_exists = if_not_exists, .tablespace = tablespace };
    }

    fn catalogDdl(self: *Parser, action: @FieldType(ast.CatalogDdl, "action")) Error!ast.CatalogDdl {
        const kind: @FieldType(ast.CatalogDdl, "kind") = if (self.keyword(.database)) .database else if (self.keyword(.schema)) .namespace else if (self.keyword(.tablespace)) .tablespace else if (self.keyword(.table)) .table else return self.fail(error.UnsupportedSqlShape, "expected a table, database, schema, or tablespace");
        var conditional = false;
        if (action == .create or action == .drop) {
            conditional = self.keyword(.@"if");
            if (conditional) {
                if (action == .create) try self.expectKeyword(.not);
                try self.expectKeyword(.exists);
            }
        }
        var target_name: ast.Name = if (kind == .table) try self.name() else .{ .table = try self.identifier() };
        if (kind == .namespace and self.take(.dot)) {
            target_name.database = target_name.table;
            target_name.table = try self.identifier();
        }
        var ddl: ast.CatalogDdl = .{ .kind = kind, .action = action, .name = target_name, .conditional = conditional };
        if (action == .rename) {
            if (kind == .table and self.keyword(.add)) {
                if (self.keyword(.constraint)) {
                    ddl.action = .alter_schema;
                    ddl.schema_change = try self.tableConstraint();
                    return ddl;
                }
                _ = self.keyword(.column);
                const column_name = try self.identifier();
                var column: ast.Column = .{ .name = column_name, .type = try self.columnType() };
                if (self.keyword(.not)) {
                    try self.expectKeyword(.null);
                    column.nullable = false;
                }
                if (self.keyword(.default)) column.default_value = try self.value();
                ddl.action = .alter_schema;
                ddl.schema_change = .{ .add_column = column };
            } else if (kind == .table and self.keyword(.drop)) {
                if (self.keyword(.constraint)) {
                    ddl.action = .alter_schema;
                    ddl.schema_change = .{ .drop_constraint = try self.identifier() };
                    return ddl;
                }
                _ = self.keyword(.column);
                ddl.action = .alter_schema;
                ddl.schema_change = .{ .drop_column = try self.identifier() };
            } else if (kind == .table and self.keyword(.alter)) {
                _ = self.keyword(.column);
                const column_name = try self.identifier();
                ddl.action = .alter_schema;
                if (self.keyword(.set)) {
                    try self.expectKeyword(.default);
                    ddl.schema_change = .{ .set_default = .{ .column = column_name, .value = try self.value() } };
                } else {
                    try self.expectKeyword(.drop);
                    try self.expectKeyword(.default);
                    ddl.schema_change = .{ .drop_default = column_name };
                }
            } else if (kind == .table and self.ddlWord("VALIDATE")) {
                try self.expectKeyword(.constraint);
                ddl.action = .alter_schema;
                ddl.schema_change = .{ .validate_constraint = try self.identifier() };
            } else if (self.keyword(.rename)) {
                try self.expectKeyword(.to);
                ddl.new_name = try self.identifier();
            } else if (self.keyword(.set)) {
                try self.expectKeyword(.tablespace);
                ddl.action = .set_tablespace;
                ddl.tablespace = try self.identifier();
            } else return self.fail(error.UnsupportedSqlShape, "unsupported catalog ALTER operation");
        } else if (action == .create and kind == .tablespace) {
            if (self.keyword(.location)) {
                const location = try self.value();
                if (location != .string) return self.fail(error.InvalidSqlSyntax, "tablespace location must be a string literal");
                ddl.location = location.string;
            }
        } else if (action == .create and self.keyword(.tablespace)) ddl.tablespace = try self.identifier();
        return ddl;
    }

    fn indexDdl(self: *Parser, create: bool, unique: bool) Error!ast.CatalogDdl {
        const conditional = self.keyword(.@"if");
        if (conditional) {
            if (create) try self.expectKeyword(.not);
            try self.expectKeyword(.exists);
        }
        const index_name = try self.identifier();
        try self.expectKeyword(.on);
        const table_name = try self.name();
        if (!create) return .{ .kind = .table, .action = .alter_schema, .name = table_name, .conditional = conditional, .schema_change = .{ .drop_index = index_name } };
        try self.expect(.lparen);
        var keys = std.ArrayList(ast.Order).empty;
        while (true) {
            try self.node();
            const expression = try self.scalar(0, 0);
            try self.checkScalarDepth(expression, 0);
            const descending = self.keyword(.desc);
            if (!descending) _ = self.keyword(.asc);
            var nulls_first: ?bool = null;
            if (self.keyword(.nulls)) nulls_first = if (self.keyword(.first)) true else if (self.keyword(.last)) false else return self.fail(error.InvalidSqlSyntax, "expected NULLS FIRST or LAST");
            try keys.append(self.alloc, .{ .field = if (expression.* == .column) expression.column else "", .expression = if (expression.* == .column) null else expression, .descending = descending, .nulls_first = nulls_first });
            if (!self.take(.comma)) break;
        }
        try self.expect(.rparen);
        var included = std.ArrayList([]const u8).empty;
        if (self.keyword(.include)) {
            try self.expect(.lparen);
            while (true) {
                try self.node();
                try included.append(self.alloc, try self.identifier());
                if (!self.take(.comma)) break;
            }
            try self.expect(.rparen);
        }
        const partial_predicate = if (self.keyword(.where)) try self.scalar(0, 0) else null;
        if (partial_predicate) |expression| try self.checkScalarDepth(expression, 0);
        return .{ .kind = .table, .action = .alter_schema, .name = table_name, .conditional = conditional, .schema_change = .{ .create_index = .{ .name = index_name, .keys = try keys.toOwnedSlice(self.alloc), .include_columns = try included.toOwnedSlice(self.alloc), .unique = unique, .predicate = partial_predicate } } };
    }

    fn ddlWord(self: *Parser, word: []const u8) bool {
        if (self.pos >= self.tokens.len or self.tokens[self.pos].owned or !std.ascii.eqlIgnoreCase(self.tokens[self.pos].text, word)) return false;
        self.pos += 1;
        return true;
    }

    fn ddlColumnList(self: *Parser) Error![]const []const u8 {
        try self.expect(.lparen);
        var columns = std.ArrayList([]const u8).empty;
        while (true) {
            try self.node();
            try columns.append(self.alloc, try self.identifier());
            if (!self.take(.comma)) break;
        }
        try self.expect(.rparen);
        return columns.toOwnedSlice(self.alloc);
    }

    fn foreignKeyAction(self: *Parser) Error![]const u8 {
        if (self.keyword(.cascade)) return "cascade";
        if (self.keyword(.restrict)) return "restrict";
        if (self.keyword(.set)) {
            try self.expectKeyword(.null);
            return "set_null";
        }
        if (self.ddlWord("NO") and self.ddlWord("ACTION")) return "no_action";
        return self.fail(error.UnsupportedSqlShape, "unsupported foreign key action");
    }

    fn tableConstraint(self: *Parser) Error!ast.SchemaChange {
        const constraint_name = try self.identifier();
        return self.constraintDefinition(constraint_name);
    }

    fn inlineConstraint(self: *Parser, constraint_name: []const u8, column: []const u8) Error!ast.SchemaChange {
        const columns = try self.alloc.dupe([]const u8, &.{column});
        if (self.keyword(.primary)) {
            try self.expectKeyword(.key);
            return .{ .add_unique = .{ .name = constraint_name, .columns = columns, .primary = true } };
        }
        if (self.keyword(.unique)) return .{ .add_unique = .{ .name = constraint_name, .columns = columns } };
        if (self.tokens[self.pos].isKeyword(.check)) return self.constraintDefinition(constraint_name);
        return self.foreignKeyDefinition(constraint_name, columns);
    }

    fn constraintDefinition(self: *Parser, constraint_name: []const u8) Error!ast.SchemaChange {
        if (self.keyword(.primary)) {
            try self.expectKeyword(.key);
            return .{ .add_unique = .{ .name = constraint_name, .columns = try self.ddlColumnList(), .primary = true } };
        }
        if (self.keyword(.unique)) return .{ .add_unique = .{ .name = constraint_name, .columns = try self.ddlColumnList() } };
        if (self.keyword(.check)) {
            try self.expect(.lparen);
            const expression = try self.scalar(0, 0);
            try self.checkScalarDepth(expression, 0);
            try self.expect(.rparen);
            return .{ .add_check = .{ .name = constraint_name, .expression = expression } };
        }
        try self.expectKeyword(.foreign);
        try self.expectKeyword(.key);
        const columns = try self.ddlColumnList();
        return self.foreignKeyDefinition(constraint_name, columns);
    }

    fn foreignKeyDefinition(self: *Parser, constraint_name: []const u8, columns: []const []const u8) Error!ast.SchemaChange {
        if (!self.ddlWord("REFERENCES")) return self.fail(error.InvalidSqlSyntax, "expected REFERENCES");
        const parent = try self.identifier();
        var foreign: @FieldType(ast.SchemaChange, "add_foreign_key") = .{ .name = constraint_name, .columns = columns, .parent = parent, .parent_columns = try self.ddlColumnList() };
        var on_delete = false;
        var on_update = false;
        var match_seen = false;
        var deferrable_seen = false;
        var timing_seen = false;
        while (true) {
            if (self.keyword(.on)) {
                if (self.keyword(.delete)) {
                    if (on_delete) return self.fail(error.InvalidSqlSyntax, "duplicate ON DELETE action");
                    on_delete = true;
                    foreign.on_delete = try self.foreignKeyAction();
                } else {
                    try self.expectKeyword(.update);
                    if (on_update) return self.fail(error.InvalidSqlSyntax, "duplicate ON UPDATE action");
                    on_update = true;
                    foreign.on_update = try self.foreignKeyAction();
                }
            } else if (self.ddlWord("MATCH")) {
                if (match_seen) return self.fail(error.InvalidSqlSyntax, "duplicate MATCH clause");
                match_seen = true;
                foreign.match = if (self.ddlWord("SIMPLE")) "simple" else if (self.ddlWord("FULL")) "full" else if (self.ddlWord("PARTIAL")) "partial" else return self.fail(error.InvalidSqlSyntax, "invalid MATCH clause");
            } else if (self.keyword(.deferrable)) {
                if (deferrable_seen) return self.fail(error.InvalidSqlSyntax, "duplicate DEFERRABLE clause");
                deferrable_seen = true;
                foreign.deferrable = true;
            } else if (self.ddlWord("INITIALLY")) {
                if (timing_seen) return self.fail(error.InvalidSqlSyntax, "duplicate INITIALLY clause");
                timing_seen = true;
                foreign.timing = if (self.keyword(.deferred)) "deferred" else if (self.keyword(.immediate)) "immediate" else return self.fail(error.InvalidSqlSyntax, "invalid constraint timing");
            } else break;
        }
        if (std.mem.eql(u8, foreign.timing, "deferred") and !foreign.deferrable) return self.fail(error.InvalidSqlSyntax, "INITIALLY DEFERRED requires DEFERRABLE");
        return .{ .add_foreign_key = foreign };
    }

    fn statement(self: *Parser) Error!ast.Statement {
        try self.node();
        if (self.keyword(.with)) {
            if (self.keyword(.recursive)) return self.fail(error.UnsupportedSqlShape, "recursive CTE execution is not available");
            self.relation_depth += 1;
            defer self.relation_depth -= 1;
            if (self.relation_depth >= self.limits.max_depth) return self.fail(error.SqlLimitExceeded, "SQL CTE nesting limit exceeded");
            var ctes: std.ArrayList(ast.Cte) = .empty;
            while (true) {
                const cte_name = try self.identifier();
                var column_names: std.ArrayList([]const u8) = .empty;
                if (self.take(.lparen)) {
                    while (true) {
                        try column_names.append(self.alloc, try self.identifier());
                        if (!self.take(.comma)) break;
                    }
                    try self.expect(.rparen);
                }
                try self.expectKeyword(.as);
                try self.expect(.lparen);
                const cte_statement = try self.statement();
                if (cte_statement != .select) return self.fail(error.UnsupportedSqlShape, "CTEs require SELECT queries");
                try self.expect(.rparen);
                const query = try self.alloc.create(ast.Select);
                query.* = cte_statement.select;
                try ctes.append(self.alloc, .{ .name = cte_name, .columns = try column_names.toOwnedSlice(self.alloc), .query = query });
                if (!self.take(.comma)) break;
            }
            try self.expectKeyword(.select);
            var result = try self.select();
            result.ctes = try ctes.toOwnedSlice(self.alloc);
            return .{ .select = result };
        }
        if (self.take(.lparen)) {
            self.relation_depth += 1;
            defer self.relation_depth -= 1;
            if (self.relation_depth >= self.limits.max_depth) return self.fail(error.SqlLimitExceeded, "SQL query nesting limit exceeded");
            const nested = try self.statement();
            if (nested != .select) return self.fail(error.InvalidSqlSyntax, "parenthesized queries require SELECT");
            try self.expect(.rparen);
            const next_kind = if (self.pos < self.tokens.len) self.tokens[self.pos] else null;
            if (next_kind != null and (next_kind.?.isKeyword(.@"union") or next_kind.?.isKeyword(.intersect) or next_kind.?.isKeyword(.except)))
                return .{ .select = try self.selectFinish(nested.select) };
            if (next_kind == null or (!next_kind.?.isKeyword(.order) and !next_kind.?.isKeyword(.limit) and !next_kind.?.isKeyword(.offset))) return nested;
            // Preserve inner ORDER/LIMIT: outer clauses apply to a derived
            // relation, never overwrite the parenthesized query's clauses.
            const query = try self.alloc.create(ast.Select);
            query.* = nested.select;
            const source = try self.alloc.create(ast.Relation);
            source.* = .{ .derived = .{ .query = query, .alias = "$parenthesized" } };
            return .{ .select = try self.selectFinish(.{ .source = source }) };
        }
        if (self.keyword(.select)) return .{ .select = try self.select() };
        if (self.keyword(.insert)) return .{ .insert = try self.insert() };
        if (self.keyword(.update)) return .{ .update = try self.update() };
        if (self.keyword(.delete)) {
            try self.expectKeyword(.from);
            return .{ .delete = .{ .table = try self.name(), .predicate = try self.where(), .returning = try self.returning() } };
        }
        if (self.keyword(.create)) {
            if (self.keyword(.table)) return .{ .create_table = try self.createTable() };
            const unique = self.keyword(.unique);
            if (self.keyword(.index)) return .{ .catalog_ddl = try self.indexDdl(true, unique) };
            if (unique) return self.fail(error.InvalidSqlSyntax, "UNIQUE requires INDEX");
            return .{ .catalog_ddl = try self.catalogDdl(.create) };
        }
        if (self.keyword(.drop)) {
            if (self.keyword(.index)) return .{ .catalog_ddl = try self.indexDdl(false, false) };
            if (!self.keyword(.table)) return .{ .catalog_ddl = try self.catalogDdl(.drop) };
            const if_exists = self.keyword(.@"if");
            if (if_exists) try self.expectKeyword(.exists);
            return .{ .drop_table = .{ .table = try self.name(), .if_exists = if_exists } };
        }
        if (self.keyword(.alter)) return .{ .catalog_ddl = try self.catalogDdl(.rename) };
        if (self.keyword(.begin)) {
            _ = self.keyword(.transaction) or self.keyword(.work);
            return .{ .begin = try self.transactionOptions() };
        }
        if (self.keyword(.start)) {
            try self.expectKeyword(.transaction);
            return .{ .begin = try self.transactionOptions() };
        }
        if (self.keyword(.commit)) {
            _ = self.keyword(.transaction) or self.keyword(.work);
            return .commit;
        }
        if (self.keyword(.rollback)) {
            _ = self.keyword(.transaction) or self.keyword(.work);
            if (self.keyword(.to)) {
                _ = self.keyword(.savepoint);
                return .{ .rollback_to_savepoint = try self.identifier() };
            }
            return .rollback;
        }
        if (self.keyword(.savepoint)) return .{ .savepoint = try self.identifier() };
        if (self.keyword(.release)) {
            _ = self.keyword(.savepoint);
            return .{ .release_savepoint = try self.identifier() };
        }
        return self.fail(error.UnsupportedSqlShape, "unsupported SQL statement");
    }

    fn transactionOptions(self: *Parser) Error!@import("session.zig").Begin {
        var options: @import("session.zig").Begin = .{};
        var isolation_seen = false;
        var mode_seen = false;
        while (true) {
            if (self.keyword(.isolation)) {
                if (isolation_seen) return self.fail(error.InvalidSqlSyntax, "duplicate transaction isolation mode");
                isolation_seen = true;
                try self.expectKeyword(.level);
                if (self.keyword(.serializable)) options.isolation = .serializable else if (self.keyword(.repeatable)) {
                    try self.expectKeyword(.read);
                    options.isolation = .repeatable_read;
                } else {
                    try self.expectKeyword(.read);
                    try self.expectKeyword(.committed);
                    options.isolation = .read_committed;
                }
            } else if (self.keyword(.read)) {
                if (mode_seen) return self.fail(error.InvalidSqlSyntax, "duplicate transaction access mode");
                mode_seen = true;
                if (self.keyword(.only)) options.mode = .read_only else {
                    try self.expectKeyword(.write);
                    options.mode = .read_write;
                }
            } else break;
            if (self.take(.comma) and (self.pos >= self.tokens.len or self.tokens[self.pos].kind == .semicolon)) return self.fail(error.InvalidSqlSyntax, "transaction mode required after comma");
        }
        return options;
    }
};

test "compiler transaction modes and quoted savepoints are explicit" {
    var begin = try compile(std.testing.allocator, "START TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY", .{});
    defer begin.deinit();
    try std.testing.expectEqual(@import("session.zig").Isolation.repeatable_read, begin.statement.begin.isolation);
    try std.testing.expectEqual(@import("session.zig").ReadMode.read_only, begin.statement.begin.mode);
    var rollback = try compile(std.testing.allocator, "ROLLBACK TO SAVEPOINT \"Before Update\"", .{});
    defer rollback.deinit();
    try std.testing.expectEqualStrings("Before Update", rollback.statement.rollback_to_savepoint);
    var release = try compile(std.testing.allocator, "RELEASE before_update", .{});
    defer release.deinit();
    try std.testing.expectEqualStrings("before_update", release.statement.release_savepoint);
    try std.testing.expectError(error.InvalidSqlSyntax, compile(std.testing.allocator, "BEGIN READ ONLY READ WRITE", .{}));
}

test "compiler owns folded and quoted identifiers and exact literals" {
    const source = try std.testing.allocator.dupe(u8, "INSERT INTO db.ns.Accounts (_id, \"UserName\", balance) VALUES ('1', 'Ada', -9223372036854775808), ('2', $1, 9223372036854775807);");
    var compiled = try compile(std.testing.allocator, source, .{});
    std.testing.allocator.free(source);
    defer compiled.deinit();
    const insert = compiled.statement.insert;
    try std.testing.expectEqualStrings("accounts", insert.table.table);
    try std.testing.expectEqualStrings("UserName", insert.columns[1]);
    try std.testing.expectEqualStrings("Ada", insert.rows[0][1].string);
    try std.testing.expectEqual(std.math.minInt(i64), insert.rows[0][2].integer);
    try std.testing.expectEqual(std.math.maxInt(i64), insert.rows[1][2].integer);
    try std.testing.expectEqual(@as(u32, 1), compiled.parameter_count);
}

test "compiler keeps typed parameters and boolean precedence" {
    var compiled = try compile(std.testing.allocator, "SELECT _id, name AS display FROM public.users WHERE active = true OR age >= $1 AND NOT name IS NULL ORDER BY age DESC LIMIT $2 OFFSET 4", .{});
    defer compiled.deinit();
    const select = compiled.statement.select;
    try std.testing.expectEqualStrings("public", select.table.?.namespace.?);
    try std.testing.expectEqualStrings("display", select.columns[1].alias.?);
    try std.testing.expectEqual(@as(u32, 1), select.predicate.?.disjunction.right.conjunction.left.comparison.value.parameter);
    try std.testing.expect(select.predicate.?.disjunction.right.conjunction.right.* == .negation);
    try std.testing.expect(select.order_by[0].descending);
    try std.testing.expectEqual(@as(u32, 2), select.limit.?.parameter);
}

test "compiler rejects unsupported clauses and additional statements atomically" {
    const cases = [_][]const u8{
        "SELECT * FROM t; DELETE FROM t",
        "SELECT * FROM t JOIN u USING (x)",
        "INSERT INTO t (x) VALUES (1) ON CONFLICT DO NOTHING",
        "DELETE FROM t RETURNING *; INSERT INTO t (x) VALUES (1)",
    };
    for (cases) |source| {
        const result = compile(std.testing.allocator, source, .{});
        if (result) |value_| {
            var value = value_;
            value.deinit();
            return error.TestUnexpectedResult;
        } else |_| {}
    }
}

test "compiler bounds resources and validates parameters and numbers" {
    try std.testing.expectError(error.SqlLimitExceeded, compile(std.testing.allocator, "SELECT * FROM t", .{ .max_bytes = 3 }));
    try std.testing.expectError(error.SqlLimitExceeded, compile(std.testing.allocator, "SELECT * FROM t", .{ .max_tokens = 2 }));
    try std.testing.expectError(error.SqlLimitExceeded, compile(std.testing.allocator, "SELECT a,b FROM t", .{ .max_nodes = 1 }));
    try std.testing.expectError(error.SqlLimitExceeded, compile(std.testing.allocator, "SELECT * FROM t WHERE (((a=1)))", .{ .max_depth = 2 }));
    try std.testing.expectError(error.InvalidSqlParameter, compile(std.testing.allocator, "DELETE FROM t WHERE x=$0", .{}));
    try std.testing.expectError(error.InvalidSqlParameter, compile(std.testing.allocator, "DELETE FROM t WHERE x=$1025", .{}));
    try std.testing.expectError(error.InvalidSqlNumber, compile(std.testing.allocator, "INSERT INTO t(x) VALUES (9223372036854775808)", .{}));
    try std.testing.expectError(error.InvalidSqlNumber, compile(std.testing.allocator, "INSERT INTO t(x) VALUES (1e9999)", .{}));
    try std.testing.expectError(error.DuplicateSqlColumn, compile(std.testing.allocator, "UPDATE t SET x=1,X=2", .{}));
}

test "compiler DDL literal defaults and count" {
    var ddl = try compile(std.testing.allocator, "CREATE TABLE IF NOT EXISTS t (name text NOT NULL, age bigint DEFAULT 0, enabled boolean DEFAULT true)", .{});
    defer ddl.deinit();
    try std.testing.expect(ddl.statement.create_table.if_not_exists);
    try std.testing.expect(!ddl.statement.create_table.columns[0].nullable);
    try std.testing.expectEqual(@as(i64, 0), ddl.statement.create_table.columns[1].default_value.?.integer);
    var count = try compile(std.testing.allocator, "SELECT count(*) AS total FROM t", .{});
    defer count.deinit();
    try std.testing.expect(count.statement.select.count_all);
    try std.testing.expectEqualStrings("total", count.statement.select.count_alias.?);
}

test "compiler balances long boolean chains rather than retaining linear tree depth" {
    var source = std.ArrayList(u8).empty;
    defer source.deinit(std.testing.allocator);
    try source.appendSlice(std.testing.allocator, "SELECT * FROM t WHERE x=1");
    for (0..1023) |_| try source.appendSlice(std.testing.allocator, " AND x=1");
    var compiled = try compile(std.testing.allocator, source.items, .{ .max_depth = 16 });
    defer compiled.deinit();
    try std.testing.expect(compiled.statement.select.predicate.?.* == .conjunction);
}

test "compiler cached plan excludes comments and lexer scratch buffers" {
    var source = std.ArrayList(u8).empty;
    defer source.deinit(std.testing.allocator);
    try source.appendSlice(std.testing.allocator, "SELECT name FROM users /*");
    try source.appendNTimes(std.testing.allocator, 'x', 100_000);
    try source.appendSlice(std.testing.allocator, "*/ WHERE _id='1'");
    var compiled = try compile(std.testing.allocator, source.items, .{});
    defer compiled.deinit();
    try std.testing.expect(compiled.arena.queryCapacity() < 4096);
}

fn allocationFailureCase(allocator: std.mem.Allocator) !void {
    var compiled = try compile(allocator, "INSERT INTO \"People\" (_id,name,age) VALUES ('a', 'Ada', $1), ('b', 'Bob', 42)", .{});
    defer compiled.deinit();
    try std.testing.expectEqualStrings("People", compiled.statement.insert.table.table);
}

test "compiler releases all owned allocations on every allocation failure" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, allocationFailureCase, .{});
}

test "compiler diagnostic identifies rejected trailing syntax without retained source" {
    var diagnostic: Diagnostic = .{};
    try std.testing.expectError(error.UnsupportedSqlShape, compileDiagnostic(std.testing.allocator, "SELECT * FROM t FOR UPDATE", .{}, &diagnostic));
    try std.testing.expectEqual(@as(usize, 16), diagnostic.start);
    try std.testing.expectEqual(@as(usize, 19), diagnostic.end);
    try std.testing.expectEqualStrings("unexpected trailing SQL; only one supported statement is allowed", diagnostic.message);
}

test "compiler preserves keyword-named columns and quoted SQL-looking values" {
    var columns = try compile(std.testing.allocator, "SELECT count FROM t WHERE \"select\" = 'x''; DELETE FROM t; --'", .{});
    defer columns.deinit();
    try std.testing.expect(!columns.statement.select.count_all);
    try std.testing.expectEqualStrings("count", columns.statement.select.columns[0].field);
    try std.testing.expectEqualStrings("select", columns.statement.select.predicate.?.comparison.field);
    try std.testing.expectEqualStrings("x'; DELETE FROM t; --", columns.statement.select.predicate.?.comparison.value.string);
}
