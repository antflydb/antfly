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
    uses_current_setting: bool = false,

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
    return .{ .arena = arena, .statement = statement, .parameter_count = parser.parameter_count, .uses_current_setting = parser.uses_current_setting };
}

test "setting authority capture follows parsed calls, not SQL text" {
    var literal = try compile(std.testing.allocator, "SELECT 'current_setting(' AS note /* current_setting('app.tenant') */", .{});
    defer literal.deinit();
    try std.testing.expect(!literal.uses_current_setting);
    var call = try compile(std.testing.allocator, "SELECT current_setting('app.tenant')", .{});
    defer call.deinit();
    try std.testing.expect(call.uses_current_setting);
}

test "SQL MERGE compiles ordered matched and unmatched mutation arms" {
    // sql-0579, sql-0581, sql-0583, sql-0589: syntax/AST coverage only;
    // these cases remain unresolved until native atomic execution is wired.
    const sql = "MERGE INTO usage_records AS target USING source_records AS source ON target.id = source.id " ++
        "WHEN MATCHED AND target.status = 'locked' THEN DO NOTHING " ++
        "WHEN MATCHED AND lower(source.status) != lower(target.status) THEN UPDATE SET status = lower(source.status) " ++
        "WHEN MATCHED AND source.status = 'deleted' THEN DELETE " ++
        "WHEN NOT MATCHED AND source.status = 'ready' THEN INSERT (id, status) VALUES (source.id, source.status) " ++
        "WHEN NOT MATCHED THEN DO NOTHING RETURNING target.id, target.status";
    var compiled = try compile(std.testing.allocator, sql, .{});
    defer compiled.deinit();
    try std.testing.expect(compiled.statement == .merge);
    const merge_statement = compiled.statement.merge;
    try std.testing.expectEqualStrings("usage_records", merge_statement.table.table);
    try std.testing.expectEqualStrings("target", merge_statement.alias.?);
    try std.testing.expect(merge_statement.source.* == .table);
    try std.testing.expectEqualStrings("source", merge_statement.source.table.alias.?);
    try std.testing.expectEqual(@as(usize, 5), merge_statement.arms.len);
    try std.testing.expect(merge_statement.arms[0].matched and merge_statement.arms[0].action == .nothing);
    try std.testing.expectEqualStrings("status", merge_statement.arms[1].action.update[0].field);
    try std.testing.expect(merge_statement.arms[2].action == .delete);
    try std.testing.expect(!merge_statement.arms[3].matched);
    try std.testing.expectEqual(@as(usize, 2), merge_statement.arms[3].action.insert.columns.len);
    try std.testing.expect(merge_statement.arms[4].action == .nothing);
    try std.testing.expectEqual(@as(usize, 2), merge_statement.returning.?.len);
}

test "SQL MERGE rejects invalid arm/action and insert shapes before execution" {
    const alloc = std.testing.allocator;
    for ([_][]const u8{
        "MERGE INTO t USING s ON t.id = s.id",
        "MERGE INTO t USING s ON t.id = s.id WHEN NOT MATCHED THEN DELETE",
        "MERGE INTO t USING s ON t.id = s.id WHEN MATCHED THEN INSERT (id) VALUES (s.id)",
        "MERGE INTO t USING s ON t.id = s.id WHEN MATCHED THEN UPDATE SET id = s.id, id = 3",
        "MERGE INTO t USING s ON t.id = s.id WHEN NOT MATCHED THEN INSERT (id, name) VALUES (s.id)",
        "MERGE INTO t USING s ON t.id = s.id WHEN NOT MATCHED THEN INSERT (id) VALUES (s.id, s.id)",
    }) |sql| {
        if (compile(alloc, sql, .{})) |result| {
            var compiled = result;
            compiled.deinit();
            return error.TestUnexpectedResult;
        } else |err| try std.testing.expect(err == error.InvalidSqlSyntax or err == error.DuplicateSqlColumn or err == error.UnsupportedSqlShape);
    }
}

test "SQL MERGE preserves explicit DEFAULT cells for native preparation" {
    var compiled = try compile(std.testing.allocator, "MERGE INTO t USING s ON t.id=s.id WHEN MATCHED THEN UPDATE SET status=DEFAULT WHEN NOT MATCHED THEN INSERT (id,status) VALUES (s.id,DEFAULT)", .{});
    defer compiled.deinit();
    try std.testing.expect(compiled.statement.merge.arms[0].action.update[0].use_default);
    try std.testing.expect(compiled.statement.merge.arms[1].action.insert.values[1] == null);
}

test "SQL MERGE preserves recursive CTE and derived source structure" {
    // sql-0014: syntax/ownership only; mutation execution is still guarded.
    const sql = "WITH RECURSIVE source_rows AS (SELECT id, status FROM usage_records " ++
        "UNION ALL SELECT child.id, child.status FROM usage_records AS child " ++
        "JOIN source_rows AS parent ON child.organization_id = parent.id) " ++
        "MERGE INTO usage_records USING source_rows ON usage_records.id = source_rows.id " ++
        "WHEN MATCHED THEN UPDATE SET status = source_rows.status";
    var compiled = try compile(std.testing.allocator, sql, .{});
    defer compiled.deinit();
    try std.testing.expect(compiled.statement == .merge);
    try std.testing.expectEqual(@as(usize, 1), compiled.statement.merge.ctes.len);
    try std.testing.expect(compiled.statement.merge.ctes[0].recursive);
    try std.testing.expect(compiled.statement.merge.source.* == .table);
    try std.testing.expectEqualStrings("source_rows", compiled.statement.merge.source.table.name.table);

    var derived = try compile(std.testing.allocator, "MERGE INTO t USING (SELECT id FROM s) AS source ON t.id = source.id WHEN MATCHED THEN DELETE", .{});
    defer derived.deinit();
    try std.testing.expect(derived.statement.merge.source.* == .derived);
}

test "SQL INSERT VALUES subquery source is flat at row budget" {
    const alloc = std.testing.allocator;
    var sql: std.ArrayList(u8) = .empty;
    defer sql.deinit(alloc);
    try sql.appendSlice(alloc, "INSERT INTO things (_id,n) VALUES ");
    for (0..1000) |index| {
        const row = try std.fmt.allocPrint(alloc, "{s}('r{d}',{s})", .{ if (index == 0) "" else ",", index, if (index == 0) "(SELECT 7)" else "1" });
        defer alloc.free(row);
        try sql.appendSlice(alloc, row);
    }
    var compiled = try compile(alloc, sql.items, .{});
    defer compiled.deinit();
    try std.testing.expect(compiled.statement == .insert);
    const source = compiled.statement.insert.source orelse return error.TestUnexpectedResult;
    try std.testing.expect(source.generated_values);
    try std.testing.expect(source.set_operation == null);
    try std.testing.expectEqual(@as(usize, 1000), source.values_arms.len);
    for (source.values_arms) |arm| try std.testing.expectEqual(@as(usize, 2), arm.columns.len);
}

test "compiler treats TIMESTAMPTZ quoted literals as typed datetime casts" {
    var compiled = try compile(std.testing.allocator, "INSERT INTO usage_records (id, created_at_ns) VALUES ('u_typed_time', TIMESTAMPTZ '2025-01-01T01:30:00+01:30') RETURNING created_at_ns", .{});
    defer compiled.deinit();
    try std.testing.expect(compiled.statement == .insert);
    const expression = compiled.statement.insert.expressions[0][1] orelse return error.TestUnexpectedResult;
    try std.testing.expect(expression.* == .cast);
    try std.testing.expectEqual(ast.ColumnType.datetime, expression.cast.type);
    try std.testing.expectEqualStrings("2025-01-01T01:30:00+01:30", expression.cast.operand.literal.string);
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
    uses_current_setting: bool = false,
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

    fn tableReferenceName(self: *Parser) Error!ast.Name {
        // Inheritance is not a catalog capability: every table reference is
        // already limited to its exact table. Accept ONLY at that boundary,
        // not in generic object names such as a new table declaration.
        _ = self.keyword(.only);
        return self.name();
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

    fn singleColumnSubquery(self: *Parser, query: ast.Select) Error!void {
        // Explicit projection arity is known before catalog binding. Reject
        // invalid scalar/IN/quantified subqueries even inside mutations, whose
        // source relation must not be opened just to discover this error.
        // SELECT * remains a binder check because its width is schema-bound.
        if (query.set_operation) |set| {
            try self.singleColumnSubquery(set.left.*);
            try self.singleColumnSubquery(set.right.*);
            return;
        }
        if (query.columns.len > 1) return self.fail(error.InvalidSqlSyntax, "subquery must return one column in this expression");
    }

    fn scalar(self: *Parser, depth: usize, minimum: u8) Error!*const ast.Scalar {
        if (depth >= self.limits.max_depth) return self.fail(error.SqlLimitExceeded, "SQL scalar nesting budget exceeded");
        var left: *const ast.Scalar = undefined;
        if (self.keyword(.not)) {
            left = try self.scalarNode(.{ .unary = .{ .op = .not, .operand = try self.scalar(depth + 1, 3) } });
        } else if (self.keyword(.exists)) {
            self.relation_depth += 1;
            defer self.relation_depth -= 1;
            if (self.relation_depth >= self.limits.max_depth) return self.fail(error.SqlLimitExceeded, "SQL subquery nesting limit exceeded");
            try self.expect(.lparen);
            const query = try self.alloc.create(ast.Select);
            const nested = try self.statement();
            if (nested != .select) return self.fail(error.InvalidSqlSyntax, "subquery requires SELECT");
            query.* = nested.select;
            try self.expect(.rparen);
            left = try self.scalarNode(.{ .call = .{ .name = "$exists", .args = &.{}, .subquery = query } });
        } else if (self.take(.lparen)) {
            if (self.pos < self.tokens.len and (self.tokens[self.pos].isKeyword(.select) or self.tokens[self.pos].isKeyword(.with))) {
                self.relation_depth += 1;
                defer self.relation_depth -= 1;
                if (self.relation_depth >= self.limits.max_depth) return self.fail(error.SqlLimitExceeded, "SQL subquery nesting limit exceeded");
                const query = try self.alloc.create(ast.Select);
                const nested = try self.statement();
                if (nested != .select) return self.fail(error.InvalidSqlSyntax, "subquery requires SELECT");
                query.* = nested.select;
                try self.singleColumnSubquery(query.*);
                left = try self.scalarNode(.{ .call = .{ .name = "$scalar", .args = &.{}, .subquery = query } });
            } else left = try self.scalar(depth + 1, 0);
            try self.expect(.rparen);
        } else if (self.keyword(.cast)) {
            try self.expect(.lparen);
            const operand = try self.scalar(depth + 1, 0);
            try self.expectKeyword(.as);
            const kind = try self.columnType();
            try self.expect(.rparen);
            left = try self.scalarNode(.{ .cast = .{ .operand = operand, .type = kind } });
        } else if (self.peek(.identifier) and !self.tokens[self.pos].owned and std.ascii.eqlIgnoreCase(self.tokens[self.pos].text, "timestamptz") and self.pos + 1 < self.tokens.len and self.tokens[self.pos + 1].kind == .string) {
            // Typed literals use the same validating/canonicalizing cast as
            // CAST(text AS timestamptz), including offset normalization.
            self.pos += 1;
            const literal = try self.scalarNode(.{ .literal = try self.value() });
            left = try self.scalarNode(.{ .cast = .{ .operand = literal, .type = .datetime } });
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
            if (std.mem.eql(u8, name_value, "current_setting")) self.uses_current_setting = true;
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
            const window_spec = if (self.keyword(.over)) try self.window(depth + 1) else null;
            left = try self.scalarNode(.{ .call = .{ .name = name_value, .args = try args.toOwnedSlice(self.alloc), .star = star, .distinct = distinct, .filter = filter, .window = window_spec } });
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
                    if (self.pos < self.tokens.len and (self.tokens[self.pos].isKeyword(.select) or self.tokens[self.pos].isKeyword(.with))) {
                        if (self.relation_depth >= self.limits.max_depth) return self.fail(error.SqlLimitExceeded, "SQL subquery nesting limit exceeded");
                        self.relation_depth += 1;
                        const query = try self.alloc.create(ast.Select);
                        const nested = try self.statement();
                        if (nested != .select) return self.fail(error.InvalidSqlSyntax, "subquery requires SELECT");
                        query.* = nested.select;
                        try self.singleColumnSubquery(query.*);
                        self.relation_depth -= 1;
                        try self.expect(.rparen);
                        left = try self.scalarNode(.{ .call = .{ .name = "$in_subquery", .args = try self.alloc.dupe(*const ast.Scalar, &.{left}), .subquery = query } });
                        if (negated) left = try self.scalarNode(.{ .unary = .{ .op = .not, .operand = left } });
                        continue;
                    }
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
                    const every = self.keyword(.all);
                    if (every or self.keyword(.any) or self.keyword(.some)) {
                        try self.expect(.lparen);
                        if (self.relation_depth >= self.limits.max_depth) return self.fail(error.SqlLimitExceeded, "SQL subquery nesting limit exceeded");
                        self.relation_depth += 1;
                        const query = try self.alloc.create(ast.Select);
                        const nested = try self.statement();
                        if (nested != .select) return self.fail(error.InvalidSqlSyntax, "subquery requires SELECT");
                        query.* = nested.select;
                        try self.singleColumnSubquery(query.*);
                        self.relation_depth -= 1;
                        try self.expect(.rparen);
                        left = try self.scalarNode(.{ .call = .{
                            .name = if (every) (if (insensitive) "$all_not_ilike" else "$all_not_like") else (if (insensitive) "$any_not_ilike" else "$any_not_like"),
                            .args = try self.alloc.dupe(*const ast.Scalar, &.{left}),
                            .subquery = query,
                        } });
                        continue;
                    }
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
            if (op == .eq or op == .neq or op == .lt or op == .lte or op == .gt or op == .gte or op == .like or op == .ilike) {
                const every = self.keyword(.all);
                if (every or self.keyword(.any) or self.keyword(.some)) {
                    try self.expect(.lparen);
                    if (self.relation_depth >= self.limits.max_depth) return self.fail(error.SqlLimitExceeded, "SQL subquery nesting limit exceeded");
                    self.relation_depth += 1;
                    const query = try self.alloc.create(ast.Select);
                    const nested = try self.statement();
                    if (nested != .select) return self.fail(error.InvalidSqlSyntax, "subquery requires SELECT");
                    query.* = nested.select;
                    try self.singleColumnSubquery(query.*);
                    self.relation_depth -= 1;
                    try self.expect(.rparen);
                    left = try self.scalarNode(.{ .call = .{
                        .name = try std.fmt.allocPrint(self.alloc, "${s}_{s}", .{ if (every) "all" else "any", @tagName(op) }),
                        .args = try self.alloc.dupe(*const ast.Scalar, &.{left}),
                        .subquery = query,
                    } });
                    continue;
                }
            }
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
                if (part.window) |spec| {
                    for (spec.partition) |item| try self.checkScalarDepth(item, depth + 1);
                    for (spec.order) |item| if (item.expression) |expression_| try self.checkScalarDepth(expression_, depth + 1);
                }
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
        try @import("window_names.zig").resolveOrder(self.alloc, result, self.limits.max_depth);
        result.limit = if (self.keyword(.limit)) try self.rowBound() else null;
        result.offset = if (self.keyword(.offset)) try self.rowBound() else null;
        if (result.columns.len == 1 and result.group_by.len == 0 and result.having == null and result.order_by.len == 0) {
            if (result.columns[0].expression) |expression| if (expression.* == .call and expression.call.window == null and expression.call.star and !expression.call.distinct and expression.call.filter == null and std.mem.eql(u8, expression.call.name, "count")) {
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
                const alias = if (self.keyword(.as))
                    try self.identifier()
                else if (self.peek(.identifier) and (self.tokens[self.pos].owned or self.tokens[self.pos].keyword == null or token.keywordClass(self.tokens[self.pos].keyword.?) == .unreserved))
                    try self.identifier()
                else
                    null;
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
        var windows: std.ArrayList(ast.NamedWindow) = .empty;
        if (self.keyword(.window)) while (true) {
            try self.node();
            const window_name = try self.identifier();
            try self.expectKeyword(.as);
            if (!self.peek(.lparen)) return self.fail(error.InvalidSqlSyntax, "window definition requires parentheses");
            try windows.append(self.alloc, .{ .name = window_name, .window = try self.window(0) });
            if (!self.take(.comma)) break;
        };
        var result = ast.Select{ .table = table, .source = if (simple) null else source, .columns = try columns.toOwnedSlice(self.alloc), .predicate = filter, .group_by = try group_by.toOwnedSlice(self.alloc), .having = having, .windows = try windows.toOwnedSlice(self.alloc) };
        try @import("window_names.zig").resolveCore(self.alloc, &result, self.limits.max_depth);
        return result;
    }

    fn window(self: *Parser, depth: usize) Error!ast.Window {
        if (!self.peek(.lparen)) return .{ .reference = try self.identifier() };
        try self.expect(.lparen);
        const reference = if (self.peek(.identifier) and !self.tokens[self.pos].isKeyword(.partition) and !self.tokens[self.pos].isKeyword(.order) and !self.tokens[self.pos].isKeyword(.rows) and !self.tokens[self.pos].isKeyword(.range) and (self.tokens[self.pos].owned or !std.ascii.eqlIgnoreCase(self.tokens[self.pos].text, "groups"))) try self.identifier() else null;
        var partitions: std.ArrayList(*const ast.Scalar) = .empty;
        if (self.keyword(.partition)) {
            try self.expectKeyword(.by);
            while (true) {
                try partitions.append(self.alloc, try self.scalar(depth, 0));
                if (!self.take(.comma)) break;
            }
        }
        const orders = try self.orderExpressions(false);
        var frame: ?ast.Window.Frame = null;
        const mode: ?@FieldType(ast.Window.Frame, "mode") = if (self.keyword(.rows)) .rows else if (self.keyword(.range)) .range else if (self.ddlWord("groups")) .groups else null;
        if (mode) |kind| {
            const between = self.keyword(.between);
            const first = try self.windowBound();
            const last = if (between) blk: {
                try self.expectKeyword(.@"and");
                break :blk try self.windowBound();
            } else ast.Window.Bound.current;
            if (first == .unbounded_following or last == .unbounded_preceding) return self.fail(error.InvalidSqlSyntax, "invalid window frame boundary");
            if ((first == .following and (last == .current or last == .preceding)) or (first == .current and last == .preceding)) return self.fail(error.InvalidSqlSyntax, "window frame end cannot precede its start category");
            frame = .{ .mode = kind, .start = first, .end = last };
            if (self.ddlWord("exclude")) {
                frame.?.exclusion = if (self.keyword(.current)) blk: {
                    try self.expectKeyword(.row);
                    break :blk .current;
                } else if (self.keyword(.group)) .group else if (self.ddlWord("ties")) .ties else blk: {
                    if (!self.ddlWord("no") or !self.ddlWord("others")) return self.fail(error.InvalidSqlSyntax, "invalid window exclusion");
                    break :blk .no_others;
                };
            }
        }
        try self.expect(.rparen);
        return .{ .reference = reference, .copy_reference = reference != null, .partition = try partitions.toOwnedSlice(self.alloc), .order = orders, .frame = frame };
    }

    fn windowBound(self: *Parser) Error!ast.Window.Bound {
        if (self.keyword(.unbounded)) {
            if (self.keyword(.preceding)) return .unbounded_preceding;
            try self.expectKeyword(.following);
            return .unbounded_following;
        }
        if (self.keyword(.current)) {
            try self.expectKeyword(.row);
            return .current;
        }
        const offset = try self.value();
        if (offset != .parameter and (offset != .integer or offset.integer < 0)) return self.fail(error.InvalidSqlSyntax, "window frame offset must be a nonnegative integer or parameter");
        if (self.keyword(.preceding)) return .{ .preceding = offset };
        try self.expectKeyword(.following);
        return .{ .following = offset };
    }

    fn selectOrder(self: *Parser) Error![]const ast.Order {
        return self.orderExpressions(true);
    }

    fn orderExpressions(self: *Parser, positions: bool) Error![]const ast.Order {
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
                if (expression.* == .column) order.field = expression.column else if (positions and expression.* == .literal and expression.literal == .integer) {
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
            var names: std.ArrayList([]const u8) = .empty;
            if (self.take(.lparen)) {
                while (true) {
                    try self.node();
                    try names.append(self.alloc, try self.identifier());
                    if (!self.take(.comma)) break;
                }
                try self.expect(.rparen);
            }
            const query = try self.alloc.create(ast.Select);
            query.* = statement_value.select;
            return self.relationNode(.{ .derived = .{ .query = query, .alias = alias, .columns = try names.toOwnedSlice(self.alloc) } });
        }
        const name_value = try self.tableReferenceName();
        return self.relationNode(.{ .table = .{ .name = name_value, .alias = try self.sourceAlias() } });
    }

    fn relation(self: *Parser) Error!*const ast.Relation {
        return self.relationTail(try self.relationAtom());
    }

    fn relationTail(self: *Parser, first: *const ast.Relation) Error!*const ast.Relation {
        var left = first;
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
        const table = try self.tableReferenceName();
        if (self.keyword(.default)) {
            try self.expectKeyword(.values);
            const rows = try self.alloc.alloc([]const ast.Value, 1);
            rows[0] = &.{};
            return .{ .table = table, .columns = &.{}, .rows = rows, .conflict = try self.conflict(), .returning = try self.returning() };
        }
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
            var conflict_clause = try self.conflict();
            if (conflict_clause) |*clause| {
                const captures = try self.conflictCaptures(clause);
                if (captures.len != 0) {
                    if (source.columns.len == 0 or source.set_operation != null or source.values_arms.len != 0) return self.fail(error.UnsupportedSqlShape, "conflict assignment subquery requires an explicit INSERT source projection");
                    const projected = try self.alloc.alloc(ast.Projection, source.columns.len + captures.len);
                    @memcpy(projected[0..source.columns.len], source.columns);
                    for (captures, projected[source.columns.len..], 0..) |expression, *projection, ordinal| {
                        projection.* = .{ .alias = try std.fmt.allocPrint(self.alloc, "$conflict_capture_{d}", .{ordinal}), .expression = expression };
                    }
                    source.columns = projected;
                }
            }
            return .{ .table = table, .columns = try columns.toOwnedSlice(self.alloc), .source = source, .conflict = conflict_clause, .returning = try self.returning() };
        }
        try self.expectKeyword(.values);
        var rows = std.ArrayList([]const ast.Value).empty;
        var expressions = std.ArrayList([]const ?*const ast.Scalar).empty;
        var defaults = std.ArrayList([]const bool).empty;
        while (true) {
            if (rows.items.len >= self.limits.max_insert_rows) return self.fail(error.SqlLimitExceeded, "INSERT row budget exceeded");
            try self.expect(.lparen);
            var row = std.ArrayList(ast.Value).empty;
            var row_expressions = std.ArrayList(?*const ast.Scalar).empty;
            var row_defaults = std.ArrayList(bool).empty;
            while (true) {
                if (self.keyword(.default)) {
                    try row.append(self.alloc, .null);
                    try row_expressions.append(self.alloc, null);
                    try row_defaults.append(self.alloc, true);
                } else {
                    const expression = try self.scalar(0, 0);
                    try self.checkScalarDepth(expression, 0);
                    try row.append(self.alloc, if (expression.* == .literal) expression.literal else .null);
                    try row_expressions.append(self.alloc, if (expression.* == .literal) null else expression);
                    try row_defaults.append(self.alloc, false);
                }
                if (!self.take(.comma)) break;
            }
            try self.expect(.rparen);
            if (row.items.len != columns.items.len) return self.fail(error.InvalidSqlSyntax, "INSERT values count does not match columns");
            try rows.append(self.alloc, try row.toOwnedSlice(self.alloc));
            try expressions.append(self.alloc, try row_expressions.toOwnedSlice(self.alloc));
            try defaults.append(self.alloc, try row_defaults.toOwnedSlice(self.alloc));
            if (!self.take(.comma)) break;
        }
        const names = try columns.toOwnedSlice(self.alloc);
        const values = try rows.toOwnedSlice(self.alloc);
        const cells = try expressions.toOwnedSlice(self.alloc);
        const default_cells = try defaults.toOwnedSlice(self.alloc);
        var conflict_clause = try self.conflict();
        const returning_columns = try self.returning();
        const captures = if (conflict_clause) |*clause| try self.conflictCaptures(clause) else &.{};
        var contains_subquery = false;
        for (cells) |row| for (row) |cell| if (cell) |expression| {
            contains_subquery = contains_subquery or @import("subquery_lowering.zig").has(expression);
        };
        if (!contains_subquery and captures.len == 0) return .{ .table = table, .columns = names, .rows = values, .expressions = cells, .defaults = default_cells, .conflict = conflict_clause, .returning = returning_columns };

        // VALUES with scalar subqueries is one source relation, not a collection
        // of independent expression evaluations. Keep source arms flat and in
        // input order; binding infers their shared output types as one unit.
        // INSERT ... SELECT then captures every source before preparing any
        // target image, including when the source reads the target itself.
        var arms: std.ArrayList(*const ast.Select) = .empty;
        for (values, cells) |row, row_cells| {
            const projections = try self.alloc.alloc(ast.Projection, names.len + captures.len);
            for (names, row, row_cells, projections[0..names.len]) |column_name, literal_value, expression, *projection| {
                const scalar_value = expression orelse blk: {
                    const literal = try self.scalarNode(.{ .literal = literal_value });
                    break :blk literal;
                };
                projection.* = .{ .alias = column_name, .expression = scalar_value };
            }
            for (captures, projections[names.len..], 0..) |expression, *projection, ordinal| {
                projection.* = .{ .alias = try std.fmt.allocPrint(self.alloc, "$conflict_capture_{d}", .{ordinal}), .expression = expression };
            }
            const leaf = try self.alloc.create(ast.Select);
            leaf.* = .{ .columns = projections, .generated_values = true };
            try arms.append(self.alloc, leaf);
        }
        const source = try self.alloc.create(ast.Select);
        source.* = .{ .values_arms = try arms.toOwnedSlice(self.alloc), .generated_values = true };
        return .{ .table = table, .columns = names, .source = source, .values_source_rows = values, .defaults = default_cells, .conflict = conflict_clause, .returning = returning_columns };
    }

    fn conflictCaptures(self: *Parser, clause: *ast.Conflict) Error![]const *const ast.Scalar {
        var captures: std.ArrayList(*const ast.Scalar) = .empty;
        const assignments = try self.alloc.dupe(ast.Assignment, clause.assignments);
        for (assignments) |*assignment| {
            const expression = assignment.expression orelse continue;
            if (!hasScalarSubquery(expression)) continue;
            // A conflict-row reference outside the subquery must be evaluated
            // after owner arbitration. It cannot be moved into the INSERT
            // source without changing which row the reference denotes.
            assignment.capture_ordinal = captures.items.len;
            if (hasOuterColumn(expression)) {
                assignment.capture_expression = try self.rewriteConflictHoles(expression, &captures);
                assignment.capture_span = captures.items.len - assignment.capture_ordinal.?;
            } else {
                try captures.append(self.alloc, expression);
                assignment.capture_span = 1;
            }
        }
        clause.assignments = assignments;
        clause.capture_count = captures.items.len;
        return captures.toOwnedSlice(self.alloc);
    }

    fn rewriteConflictHoles(self: *Parser, expression: *const ast.Scalar, captures: *std.ArrayList(*const ast.Scalar)) Error!*const ast.Scalar {
        if (expression.* == .call and expression.call.subquery != null) {
            if (!std.mem.eql(u8, expression.call.name, "$scalar")) return self.fail(error.UnsupportedSqlShape, "conflict assignment subquery must be scalar");
            const ordinal = captures.items.len;
            try captures.append(self.alloc, expression);
            const hole = try self.alloc.create(ast.Scalar);
            hole.* = .{ .column = try std.fmt.allocPrint(self.alloc, "$conflict_capture_{d}", .{ordinal}) };
            return hole;
        }
        if (expression.* == .case_when or (expression.* == .call and std.mem.eql(u8, expression.call.name, "coalesce"))) return self.fail(error.UnsupportedSqlShape, "lazy conflict expression requires conditional subquery Apply");
        const rewritten = try self.alloc.create(ast.Scalar);
        rewritten.* = switch (expression.*) {
            .literal, .column => expression.*,
            .unary => |part| .{ .unary = .{ .op = part.op, .operand = try self.rewriteConflictHoles(part.operand, captures) } },
            .binary => |part| .{ .binary = .{ .op = part.op, .left = try self.rewriteConflictHoles(part.left, captures), .right = try self.rewriteConflictHoles(part.right, captures) } },
            .cast => |part| .{ .cast = .{ .operand = try self.rewriteConflictHoles(part.operand, captures), .type = part.type } },
            .in_list => |part| blk: {
                const values = try self.alloc.alloc(*const ast.Scalar, part.values.len);
                for (part.values, values) |value_, *out| out.* = try self.rewriteConflictHoles(value_, captures);
                break :blk .{ .in_list = .{ .operand = try self.rewriteConflictHoles(part.operand, captures), .values = values, .negated = part.negated } };
            },
            .call => |part| blk: {
                const args = try self.alloc.alloc(*const ast.Scalar, part.args.len);
                for (part.args, args) |arg, *out| out.* = try self.rewriteConflictHoles(arg, captures);
                var copy = part;
                copy.args = args;
                copy.filter = if (part.filter) |filter| try self.rewriteConflictHoles(filter, captures) else null;
                break :blk .{ .call = copy };
            },
            .case_when => unreachable,
        };
        return rewritten;
    }

    fn hasScalarSubquery(expression: *const ast.Scalar) bool {
        return switch (expression.*) {
            .literal, .column => false,
            .unary => |item| hasScalarSubquery(item.operand),
            .binary => |item| hasScalarSubquery(item.left) or hasScalarSubquery(item.right),
            .cast => |item| hasScalarSubquery(item.operand),
            .case_when => |item| blk: {
                for (item.branches) |branch| if (hasScalarSubquery(branch.condition) or hasScalarSubquery(branch.value)) break :blk true;
                break :blk if (item.otherwise) |otherwise| hasScalarSubquery(otherwise) else false;
            },
            .in_list => |item| blk: {
                if (hasScalarSubquery(item.operand)) break :blk true;
                for (item.values) |candidate| if (hasScalarSubquery(candidate)) break :blk true;
                break :blk false;
            },
            .call => |item| blk: {
                if (item.subquery != null and std.mem.eql(u8, item.name, "$scalar")) break :blk true;
                for (item.args) |argument| if (hasScalarSubquery(argument)) break :blk true;
                break :blk if (item.filter) |filter| hasScalarSubquery(filter) else false;
            },
        };
    }

    fn hasOuterColumn(expression: *const ast.Scalar) bool {
        return switch (expression.*) {
            .literal => false,
            .column => true,
            .unary => |item| hasOuterColumn(item.operand),
            .binary => |item| hasOuterColumn(item.left) or hasOuterColumn(item.right),
            .cast => |item| hasOuterColumn(item.operand),
            .case_when => |item| blk: {
                for (item.branches) |branch| if (hasOuterColumn(branch.condition) or hasOuterColumn(branch.value)) break :blk true;
                break :blk if (item.otherwise) |otherwise| hasOuterColumn(otherwise) else false;
            },
            .in_list => |item| blk: {
                if (hasOuterColumn(item.operand)) break :blk true;
                for (item.values) |candidate| if (hasOuterColumn(candidate)) break :blk true;
                break :blk false;
            },
            .call => |item| blk: {
                // A scalar subquery has its own column scope.
                if (item.subquery != null and std.mem.eql(u8, item.name, "$scalar")) break :blk false;
                for (item.args) |argument| if (hasOuterColumn(argument)) break :blk true;
                break :blk if (item.filter) |filter| hasOuterColumn(filter) else false;
            },
        };
    }

    fn conflict(self: *Parser) Error!?ast.Conflict {
        if (!self.keyword(.on)) return null;
        try self.expectKeyword(.conflict);
        var columns: std.ArrayList([]const u8) = .empty;
        var expressions: std.ArrayList(*const ast.Scalar) = .empty;
        if (self.take(.lparen)) {
            while (true) {
                try self.node();
                const expression = try self.scalar(0, 0);
                if (expression.* == .column) try columns.append(self.alloc, expression.column) else try expressions.append(self.alloc, expression);
                if (!self.take(.comma)) break;
            }
            try self.expect(.rparen);
        }
        const arbiter_predicate = if (self.keyword(.where)) try self.scalar(0, 0) else null;
        if (arbiter_predicate != null and columns.items.len + expressions.items.len == 0) return self.fail(error.UnsupportedSqlShape, "partial conflict inference requires an explicit target");
        try self.expectKeyword(.do);
        if (self.keyword(.nothing)) return .{ .columns = try columns.toOwnedSlice(self.alloc), .expressions = try expressions.toOwnedSlice(self.alloc), .arbiter_predicate = arbiter_predicate };
        if (columns.items.len + expressions.items.len == 0) return self.fail(error.UnsupportedSqlShape, "ON CONFLICT DO UPDATE requires an explicit conflict target");
        try self.expectKeyword(.update);
        try self.expectKeyword(.set);
        var assignments: std.ArrayList(ast.Assignment) = .empty;
        while (true) {
            try self.node();
            const column_name = try self.identifier();
            for (assignments.items) |prior| if (std.mem.eql(u8, prior.field, column_name)) return self.fail(error.DuplicateSqlColumn, "duplicate conflict assignment");
            try self.expect(.eq);
            const expression = try self.scalar(0, 0);
            try self.checkScalarDepth(expression, 0);
            try assignments.append(self.alloc, .{ .field = column_name, .expression = expression });
            if (!self.take(.comma)) break;
        }
        const filter = if (self.keyword(.where)) try self.scalar(0, 0) else null;
        if (filter) |expression| try self.checkScalarDepth(expression, 0);
        return .{ .columns = try columns.toOwnedSlice(self.alloc), .expressions = try expressions.toOwnedSlice(self.alloc), .arbiter_predicate = arbiter_predicate, .assignments = try assignments.toOwnedSlice(self.alloc), .predicate = filter };
    }

    fn update(self: *Parser) Error!ast.Update {
        const table = try self.tableReferenceName();
        const alias = try self.sourceAlias();
        const target = try self.relationNode(.{ .table = .{ .name = table, .alias = alias, .mutation_target = true, .mutation_document = true, .mutation_presence = true } });
        var source = try self.relationTail(target);
        try self.expectKeyword(.set);
        var assignments = std.ArrayList(ast.Assignment).empty;
        var seen: std.StringHashMapUnmanaged(void) = .empty;
        while (true) {
            const tuple = self.take(.lparen);
            var targets: std.ArrayList([]const u8) = .empty;
            while (true) {
                try self.node();
                const field_name = try self.field();
                const column = if (std.mem.indexOfScalar(u8, field_name, 0)) |separator| blk: {
                    if (!std.mem.eql(u8, field_name[0..separator], alias orelse table.table)) return self.fail(error.InvalidSqlSyntax, "assignment must name the mutation target");
                    break :blk field_name[separator + 1 ..];
                } else field_name;
                const entry = try seen.getOrPut(self.alloc, column);
                if (entry.found_existing) return self.fail(error.DuplicateSqlColumn, "duplicate UPDATE assignment");
                try targets.append(self.alloc, column);
                if (!tuple or !self.take(.comma)) break;
            }
            if (tuple) try self.expect(.rparen);
            try self.expect(.eq);
            if (tuple) {
                _ = self.keyword(.row);
                try self.expect(.lparen);
                for (targets.items, 0..) |column, i| {
                    if (i != 0) try self.expect(.comma);
                    if (self.keyword(.default)) {
                        try assignments.append(self.alloc, .{ .field = column, .use_default = true });
                        continue;
                    }
                    const expression = try self.scalar(0, 0);
                    try self.checkScalarDepth(expression, 0);
                    try assignments.append(self.alloc, if (expression.* == .literal) .{ .field = column, .value = expression.literal } else .{ .field = column, .expression = expression });
                }
                if (!self.take(.rparen)) return self.fail(error.InvalidSqlSyntax, "row assignment value count does not match targets");
            } else {
                if (self.keyword(.default)) {
                    try assignments.append(self.alloc, .{ .field = targets.items[0], .use_default = true });
                } else {
                    const expression = try self.scalar(0, 0);
                    try self.checkScalarDepth(expression, 0);
                    try assignments.append(self.alloc, if (expression.* == .literal) .{ .field = targets.items[0], .value = expression.literal } else .{ .field = targets.items[0], .expression = expression });
                }
            }
            if (!self.take(.comma)) break;
        }
        if (self.keyword(.from)) {
            if (source != target) return self.fail(error.InvalidSqlSyntax, "use either joined UPDATE or UPDATE FROM");
            source = try self.relationNode(.{ .join = .{ .kind = .cross, .left = target, .right = try self.relation() } });
        }
        return .{ .table = table, .alias = alias, .source = if (alias != null or source != target) source else null, .assignments = try assignments.toOwnedSlice(self.alloc), .predicate = try self.where(), .returning = try self.returning() };
    }

    fn delete(self: *Parser) Error!ast.Delete {
        try self.expectKeyword(.from);
        const table = try self.tableReferenceName();
        const alias = try self.sourceAlias();
        const target = try self.relationNode(.{ .table = .{ .name = table, .alias = alias, .mutation_target = true, .mutation_presence = true } });
        var source = try self.relationTail(target);
        if (self.keyword(.using)) {
            if (source != target) return self.fail(error.InvalidSqlSyntax, "use either joined DELETE or DELETE USING");
            source = try self.relationNode(.{ .join = .{ .kind = .cross, .left = target, .right = try self.relation() } });
        }
        return .{ .table = table, .alias = alias, .source = if (alias != null or source != target) source else null, .predicate = try self.where(), .returning = try self.returning() };
    }

    fn merge(self: *Parser) Error!ast.Merge {
        try self.expectKeyword(.into);
        const table = try self.tableReferenceName();
        const alias = try self.sourceAlias();
        try self.expectKeyword(.using);
        const source = try self.relation();
        try self.expectKeyword(.on);
        const condition = try self.scalar(0, 0);
        try self.checkScalarDepth(condition, 0);
        var arms: std.ArrayList(ast.Merge.Arm) = .empty;
        while (self.keyword(.when)) {
            if (arms.items.len >= 128) return self.fail(error.SqlLimitExceeded, "MERGE arm limit exceeded");
            const matched = !self.keyword(.not);
            try self.expectKeyword(.matched);
            const arm_filter = if (self.keyword(.@"and")) try self.scalar(0, 0) else null;
            if (arm_filter) |filter_expr| try self.checkScalarDepth(filter_expr, 0);
            try self.expectKeyword(.then);
            var action: ast.Merge.Arm.Action = undefined;
            if (self.keyword(.update)) {
                if (!matched) return self.fail(error.InvalidSqlSyntax, "NOT MATCHED cannot UPDATE");
                try self.expectKeyword(.set);
                var assignments: std.ArrayList(ast.Assignment) = .empty;
                var seen: std.StringHashMapUnmanaged(void) = .empty;
                while (true) {
                    if (assignments.items.len >= 256) return self.fail(error.SqlLimitExceeded, "MERGE assignment limit exceeded");
                    try self.node();
                    const field_name = try self.field();
                    const column = if (std.mem.indexOfScalar(u8, field_name, 0)) |separator| blk: {
                        if (!std.mem.eql(u8, field_name[0..separator], alias orelse table.table)) return self.fail(error.InvalidSqlSyntax, "assignment must name MERGE target");
                        break :blk field_name[separator + 1 ..];
                    } else field_name;
                    const entry = try seen.getOrPut(self.alloc, column);
                    if (entry.found_existing) return self.fail(error.DuplicateSqlColumn, "duplicate MERGE assignment");
                    try self.expect(.eq);
                    if (self.keyword(.default)) {
                        try assignments.append(self.alloc, .{ .field = column, .use_default = true });
                        if (!self.take(.comma)) break;
                        continue;
                    }
                    const expression = try self.scalar(0, 0);
                    try self.checkScalarDepth(expression, 0);
                    try assignments.append(self.alloc, if (expression.* == .literal) .{ .field = column, .value = expression.literal } else .{ .field = column, .expression = expression });
                    if (!self.take(.comma)) break;
                }
                action = .{ .update = try assignments.toOwnedSlice(self.alloc) };
            } else if (self.keyword(.delete)) {
                if (!matched) return self.fail(error.InvalidSqlSyntax, "NOT MATCHED cannot DELETE");
                action = .delete;
            } else if (self.keyword(.insert)) {
                if (matched) return self.fail(error.InvalidSqlSyntax, "MATCHED cannot INSERT");
                try self.expect(.lparen);
                var columns: std.ArrayList([]const u8) = .empty;
                var seen: std.StringHashMapUnmanaged(void) = .empty;
                while (true) {
                    if (columns.items.len >= 256) return self.fail(error.SqlLimitExceeded, "MERGE insert column limit exceeded");
                    const column_name = try self.identifier();
                    const entry = try seen.getOrPut(self.alloc, column_name);
                    if (entry.found_existing) return self.fail(error.DuplicateSqlColumn, "duplicate MERGE insert column");
                    try columns.append(self.alloc, column_name);
                    if (!self.take(.comma)) break;
                }
                try self.expect(.rparen);
                try self.expectKeyword(.values);
                try self.expect(.lparen);
                var values: std.ArrayList(?*const ast.Scalar) = .empty;
                while (true) {
                    if (values.items.len >= columns.items.len) return self.fail(error.InvalidSqlSyntax, "MERGE insert value count exceeds columns");
                    const expression = if (self.keyword(.default)) null else try self.scalar(0, 0);
                    if (expression) |expr| try self.checkScalarDepth(expr, 0);
                    try values.append(self.alloc, expression);
                    if (!self.take(.comma)) break;
                }
                try self.expect(.rparen);
                if (values.items.len != columns.items.len) return self.fail(error.InvalidSqlSyntax, "MERGE insert value count differs from columns");
                action = .{ .insert = .{ .columns = try columns.toOwnedSlice(self.alloc), .values = try values.toOwnedSlice(self.alloc) } };
            } else if (self.keyword(.do)) {
                try self.expectKeyword(.nothing);
                action = .nothing;
            } else return self.fail(error.InvalidSqlSyntax, "expected MERGE action");
            try arms.append(self.alloc, .{ .matched = matched, .predicate = arm_filter, .action = action });
        }
        if (arms.items.len == 0) return self.fail(error.InvalidSqlSyntax, "MERGE requires a WHEN arm");
        return .{ .table = table, .alias = alias, .source = source, .condition = condition, .arms = try arms.toOwnedSlice(self.alloc), .returning = try self.returning() };
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
                    try constraints.append(self.alloc, try self.uniqueTiming(.{ .add_unique = .{ .name = try std.fmt.allocPrint(self.alloc, "sql_primary_{d}", .{constraints.items.len}), .columns = try self.alloc.dupe([]const u8, &.{column}), .primary = true } }));
                } else if (self.keyword(.unique)) {
                    try constraints.append(self.alloc, try self.uniqueTiming(.{ .add_unique = .{ .name = try std.fmt.allocPrint(self.alloc, "sql_unique_{d}", .{constraints.items.len}), .columns = try self.alloc.dupe([]const u8, &.{column}) } }));
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
        var target_name: ast.Name = if (kind == .table) (if (action == .rename) try self.tableReferenceName() else try self.name()) else .{ .table = try self.identifier() };
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
        const table_name = try self.tableReferenceName();
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

    fn uniqueTiming(self: *Parser, change: ast.SchemaChange) Error!ast.SchemaChange {
        var result = change;
        var timing_seen = false;
        var deferrable_seen = false;
        while (true) {
            if (self.keyword(.deferrable)) {
                if (deferrable_seen) return self.fail(error.InvalidSqlSyntax, "duplicate DEFERRABLE clause");
                deferrable_seen = true;
                result.add_unique.deferrable = true;
            } else if (self.pos + 1 < self.tokens.len and self.tokens[self.pos].isKeyword(.not) and self.tokens[self.pos + 1].isKeyword(.deferrable)) {
                if (deferrable_seen) return self.fail(error.InvalidSqlSyntax, "duplicate DEFERRABLE clause");
                deferrable_seen = true;
                self.pos += 2;
            } else if (self.ddlWord("INITIALLY")) {
                if (timing_seen) return self.fail(error.InvalidSqlSyntax, "duplicate INITIALLY clause");
                timing_seen = true;
                result.add_unique.timing = if (self.keyword(.deferred)) "deferred" else if (self.keyword(.immediate)) "immediate" else return self.fail(error.InvalidSqlSyntax, "invalid constraint timing");
            } else break;
        }
        if (std.mem.eql(u8, result.add_unique.timing, "deferred") and !result.add_unique.deferrable) return self.fail(error.InvalidSqlSyntax, "INITIALLY DEFERRED requires DEFERRABLE");
        return result;
    }

    fn inlineConstraint(self: *Parser, constraint_name: []const u8, column: []const u8) Error!ast.SchemaChange {
        const columns = try self.alloc.dupe([]const u8, &.{column});
        if (self.keyword(.primary)) {
            try self.expectKeyword(.key);
            return self.uniqueTiming(.{ .add_unique = .{ .name = constraint_name, .columns = columns, .primary = true } });
        }
        if (self.keyword(.unique)) return self.uniqueTiming(.{ .add_unique = .{ .name = constraint_name, .columns = columns } });
        if (self.tokens[self.pos].isKeyword(.check)) return self.constraintDefinition(constraint_name);
        return self.foreignKeyDefinition(constraint_name, columns);
    }

    fn constraintDefinition(self: *Parser, constraint_name: []const u8) Error!ast.SchemaChange {
        if (self.keyword(.primary)) {
            try self.expectKeyword(.key);
            return self.uniqueTiming(.{ .add_unique = .{ .name = constraint_name, .columns = try self.ddlColumnList(), .primary = true } });
        }
        if (self.keyword(.unique)) return self.uniqueTiming(.{ .add_unique = .{ .name = constraint_name, .columns = try self.ddlColumnList() } });
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
        if (self.keyword(.explain)) {
            var format: @FieldType(@FieldType(ast.Statement, "explain"), "format") = .text;
            var verbose = false;
            if (self.take(.lparen)) {
                while (true) {
                    if (self.keyword(.format)) {
                        if (!self.keyword(.json)) return self.fail(error.UnsupportedSqlShape, "EXPLAIN supports FORMAT JSON or the default text format");
                        format = .json;
                    } else if (self.keyword(.verbose)) {
                        if (self.keyword(.off)) return self.fail(error.UnsupportedSqlShape, "EXPLAIN VERBOSE OFF is not supported");
                        _ = self.keyword(.on);
                        verbose = true;
                    } else if (self.keyword(.costs)) {
                        if (!self.keyword(.off)) return self.fail(error.UnsupportedSqlShape, "EXPLAIN has no cost model; use COSTS OFF");
                    } else return self.fail(error.UnsupportedSqlShape, "unsupported EXPLAIN option");
                    if (!self.take(.comma)) break;
                }
                try self.expect(.rparen);
            }
            if (self.keyword(.analyze)) return self.fail(error.UnsupportedSqlShape, "EXPLAIN ANALYZE requires instrumented execution");
            const inner = try self.alloc.create(ast.Statement);
            inner.* = try self.statement();
            switch (inner.*) {
                .select, .insert, .update, .delete, .merge => {},
                else => return self.fail(error.UnsupportedSqlShape, "EXPLAIN requires a supported query or mutation"),
            }
            return .{ .explain = .{ .statement = inner, .format = format, .verbose = verbose } };
        }
        if (self.keyword(.truncate)) {
            _ = self.keyword(.table);
            var tables: std.ArrayList(ast.Name) = .empty;
            while (true) {
                try tables.append(self.alloc, try self.tableReferenceName());
                if (tables.items.len > 128) return self.fail(error.SqlLimitExceeded, "TRUNCATE table limit exceeded");
                if (!self.take(.comma)) break;
            }
            var restart = false;
            if (self.keyword(.restart)) {
                try self.expectKeyword(.identity);
                restart = true;
            } else if (self.keyword(.@"continue")) try self.expectKeyword(.identity);
            const cascade = self.keyword(.cascade);
            if (!cascade) _ = self.keyword(.restrict);
            return .{ .catalog_ddl = .{ .kind = .table, .action = .truncate, .name = tables.items[0], .truncate_tables = try tables.toOwnedSlice(self.alloc), .restart_identity = restart, .cascade = cascade } };
        }
        if (self.keyword(.with)) {
            const recursive = self.keyword(.recursive);
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
                const materialization: ast.Cte.Materialization = if (self.keyword(.materialized)) .materialized else if (self.keyword(.not)) blk: {
                    try self.expectKeyword(.materialized);
                    break :blk .not_materialized;
                } else .automatic;
                try self.expect(.lparen);
                const cte_statement = try self.statement();
                if (cte_statement != .select) return self.fail(error.UnsupportedSqlShape, "CTEs require SELECT queries");
                try self.expect(.rparen);
                const query = try self.alloc.create(ast.Select);
                query.* = cte_statement.select;
                try ctes.append(self.alloc, .{ .name = cte_name, .columns = try column_names.toOwnedSlice(self.alloc), .query = query, .recursive = recursive, .materialization = materialization });
                if (!self.take(.comma)) break;
            }
            var result = try self.statement();
            switch (result) {
                .update => |*mutation| {
                    mutation.ctes = try ctes.toOwnedSlice(self.alloc);
                    if (mutation.source == null) mutation.source = try self.relationNode(.{ .table = .{ .name = mutation.table, .alias = mutation.alias, .mutation_target = true, .mutation_document = true, .mutation_presence = true } });
                    return result;
                },
                .delete => |*mutation| {
                    mutation.ctes = try ctes.toOwnedSlice(self.alloc);
                    if (mutation.source == null) mutation.source = try self.relationNode(.{ .table = .{ .name = mutation.table, .alias = mutation.alias, .mutation_target = true, .mutation_presence = true } });
                    return result;
                },
                .merge => |*mutation| {
                    mutation.ctes = try ctes.toOwnedSlice(self.alloc);
                    return result;
                },
                else => {},
            }
            const source: *ast.Select = switch (result) {
                .select => &result.select,
                .insert => blk: {
                    const original = result.insert.source orelse return self.fail(error.UnsupportedSqlShape, "WITH INSERT requires a SELECT source");
                    const copy = try self.alloc.create(ast.Select);
                    copy.* = original.*;
                    result.insert.source = copy;
                    break :blk copy;
                },
                else => return self.fail(error.UnsupportedSqlShape, "WITH requires a query or mutation"),
            };
            try ctes.appendSlice(self.alloc, source.ctes);
            source.ctes = try ctes.toOwnedSlice(self.alloc);
            return result;
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
            return .{ .delete = try self.delete() };
        }
        if (self.keyword(.merge)) return .{ .merge = try self.merge() };
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
        if (self.keyword(.set)) {
            if (!self.ddlWord("CONSTRAINTS")) return self.fail(error.UnsupportedSqlShape, "SET requires CONSTRAINTS");
            var names: std.ArrayList([]const u8) = .empty;
            if (!self.keyword(.all)) while (true) {
                if (names.items.len >= 256) return error.SqlLimitExceeded;
                try names.append(self.alloc, try self.identifier());
                if (!self.take(.comma)) break;
            };
            const deferred = self.ddlWord("DEFERRED");
            if (!deferred and !self.ddlWord("IMMEDIATE")) return self.fail(error.InvalidSqlSyntax, "SET CONSTRAINTS requires DEFERRED or IMMEDIATE");
            return .{ .set_constraints = .{ .names = try names.toOwnedSlice(self.alloc), .deferred = deferred } };
        }
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

test "compiler deferred uniqueness and constraint timing are explicit" {
    var declaration = try compile(std.testing.allocator, "ALTER TABLE rows ADD CONSTRAINT u UNIQUE (id) DEFERRABLE INITIALLY DEFERRED", .{});
    defer declaration.deinit();
    try std.testing.expect(declaration.statement.catalog_ddl.schema_change.?.add_unique.deferrable);
    try std.testing.expectEqualStrings("deferred", declaration.statement.catalog_ddl.schema_change.?.add_unique.timing);
    var immediate = try compile(std.testing.allocator, "ALTER TABLE rows ADD CONSTRAINT u UNIQUE (id) NOT DEFERRABLE", .{});
    defer immediate.deinit();
    try std.testing.expect(!immediate.statement.catalog_ddl.schema_change.?.add_unique.deferrable);
    try std.testing.expectError(error.InvalidSqlSyntax, compile(std.testing.allocator, "ALTER TABLE rows ADD CONSTRAINT u UNIQUE (id) INITIALLY DEFERRED", .{}));
    var all = try compile(std.testing.allocator, "SET CONSTRAINTS ALL DEFERRED", .{});
    defer all.deinit();
    try std.testing.expect(all.statement.set_constraints.deferred);
    try std.testing.expectEqual(@as(usize, 0), all.statement.set_constraints.names.len);
    var named = try compile(std.testing.allocator, "SET CONSTRAINTS u, \"Other\" IMMEDIATE", .{});
    defer named.deinit();
    try std.testing.expect(!named.statement.set_constraints.deferred);
    try std.testing.expectEqualStrings("Other", named.statement.set_constraints.names[1]);
}

test "compiler ONLY scopes exact table references across read and write statements" {
    for ([_][]const u8{
        "SELECT id FROM ONLY public.rows",
        "INSERT INTO ONLY public.rows (id) VALUES ('a') RETURNING id",
        "UPDATE ONLY public.rows SET id='a' FROM ONLY public.source AS s WHERE rows.id=s.id",
        "DELETE FROM ONLY public.rows USING ONLY public.source AS s WHERE rows.id=s.id",
        "MERGE INTO ONLY public.rows AS r USING ONLY public.source AS s ON r.id=s.id WHEN MATCHED THEN DELETE",
        "TRUNCATE ONLY public.rows",
        "ALTER TABLE ONLY public.rows VALIDATE CONSTRAINT c",
        "CREATE INDEX idx ON ONLY public.rows (id)",
    }) |sql| {
        var compiled = try compile(std.testing.allocator, sql, .{});
        defer compiled.deinit();
    }
    var inserted = try compile(std.testing.allocator, "INSERT INTO ONLY public.rows (id) VALUES ('a')", .{});
    defer inserted.deinit();
    try std.testing.expectEqualStrings("public", inserted.statement.insert.table.namespace.?);
    try std.testing.expectEqualStrings("rows", inserted.statement.insert.table.table);
    var quoted = try compile(std.testing.allocator, "SELECT id FROM \"only\"", .{});
    defer quoted.deinit();
    try std.testing.expectEqualStrings("only", quoted.statement.select.table.?.table);
}

test "compiler UPDATE row assignments flatten simultaneous typed expressions" {
    var compiled = try compile(std.testing.allocator, "UPDATE rows SET (quantity, status) = ROW((SELECT quantity FROM rows WHERE id = 'source'), 'copied') WHERE id = 'target' RETURNING id", .{});
    defer compiled.deinit();
    const assignments = compiled.statement.update.assignments;
    try std.testing.expectEqual(@as(usize, 2), assignments.len);
    try std.testing.expectEqualStrings("quantity", assignments[0].field);
    try std.testing.expect(assignments[0].expression != null);
    try std.testing.expectEqualStrings("status", assignments[1].field);
    try std.testing.expectEqualStrings("copied", assignments[1].value.string);
    var bare = try compile(std.testing.allocator, "UPDATE rows SET (quantity, status) = (7, 'ready')", .{});
    defer bare.deinit();
    try std.testing.expectEqual(@as(i64, 7), bare.statement.update.assignments[0].value.integer);
    var defaults = try compile(std.testing.allocator, "UPDATE rows SET (quantity, status) = ROW(DEFAULT, 'ready'), other=DEFAULT", .{});
    defer defaults.deinit();
    try std.testing.expect(defaults.statement.update.assignments[0].use_default);
    try std.testing.expect(!defaults.statement.update.assignments[1].use_default);
    try std.testing.expect(defaults.statement.update.assignments[2].use_default);
    try std.testing.expectError(error.DuplicateSqlColumn, compile(std.testing.allocator, "UPDATE rows SET (quantity, quantity) = ROW(1, 2)", .{}));
    try std.testing.expectError(error.DuplicateSqlColumn, compile(std.testing.allocator, "UPDATE rows SET (quantity, status) = ROW(1, 'x'), status='y'", .{}));
    try std.testing.expectError(error.InvalidSqlSyntax, compile(std.testing.allocator, "UPDATE rows SET (quantity, status) = ROW(1)", .{}));
    try std.testing.expectError(error.InvalidSqlSyntax, compile(std.testing.allocator, "UPDATE rows SET (quantity, status) = ROW(1, 'x', 3)", .{}));
}

test "compiler INSERT defaults retain per-row omission beside scalar sources" {
    var values = try compile(std.testing.allocator, "INSERT INTO rows (id,status,n) VALUES ('a',DEFAULT,1),('b','set',DEFAULT)", .{});
    defer values.deinit();
    try std.testing.expectEqual(@as(usize, 2), values.statement.insert.rows.len);
    try std.testing.expect(values.statement.insert.isDefault(0, 1));
    try std.testing.expect(values.statement.insert.isDefault(1, 2));
    try std.testing.expect(!values.statement.insert.isDefault(0, 2));
    var sourced = try compile(std.testing.allocator, "INSERT INTO rows (id,status,n) VALUES ('a',DEFAULT,(SELECT 1)),('b',(SELECT 'set'),DEFAULT)", .{});
    defer sourced.deinit();
    try std.testing.expect(sourced.statement.insert.source != null);
    try std.testing.expect(sourced.statement.insert.isDefault(0, 1));
    try std.testing.expect(sourced.statement.insert.isDefault(1, 2));
    var all_default = try compile(std.testing.allocator, "INSERT INTO rows DEFAULT VALUES", .{});
    defer all_default.deinit();
    try std.testing.expectEqual(@as(usize, 1), all_default.statement.insert.rows.len);
    try std.testing.expectEqual(@as(usize, 0), all_default.statement.insert.columns.len);
}

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

test "compiler keeps ON CONFLICT arbiter predicate separate from update predicate" {
    var compiled = try compile(std.testing.allocator, "INSERT INTO users (email, active) VALUES ('a', true) ON CONFLICT (email) WHERE active = true DO UPDATE SET active = false WHERE users.active = true", .{});
    defer compiled.deinit();
    const conflict = compiled.statement.insert.conflict.?;
    try std.testing.expect(conflict.arbiter_predicate != null);
    try std.testing.expect(conflict.predicate != null);
    try std.testing.expect(conflict.arbiter_predicate.?.* == .binary);
    try std.testing.expectEqual(ast.Scalar.Binary.eq, conflict.arbiter_predicate.?.binary.op);
    try std.testing.expectEqual(ast.Scalar.Binary.eq, conflict.predicate.?.binary.op);
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
        "INSERT INTO t (x) VALUES (1) ON CONFLICT DO UPDATE SET x=2",
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

test "SQL original row-lock clauses on non-locking SQL surfaces reject before backend access" {
    // sql-0437, sql-0577, sql-0578, sql-0618: the original corpus marks
    // these row-lock forms invalid. This compiler has no row-lock syntax, so
    // reject the complete statement instead of executing its unlocked prefix.
    for ([_][]const u8{
        "SELECT u.id FROM usage_records AS u WHERE u.status = 'queued' FOR UPDATE OF archived_records",
        "UPDATE usage_records SET status = 'processing' WHERE status = 'queued' FOR SHARE RETURNING id",
        "UPDATE usage_records AS u SET status = 'processing' WHERE u.status = 'queued' FOR UPDATE OF archived_records RETURNING u.id",
        "UPDATE usage_records AS target SET status = source.status FROM source_records AS source WHERE target.id = source.id FOR UPDATE OF source RETURNING target.id",
    }) |sql| {
        try std.testing.expectError(error.UnsupportedSqlShape, compile(std.testing.allocator, sql, .{}));
    }
}

test "SQL original duplicate point update target rejects before backend access" {
    // sql-0576
    try std.testing.expectError(error.DuplicateSqlColumn, compile(std.testing.allocator, "UPDATE usage_records SET status = 'active', status = lower(status) WHERE id = 'u1'", .{}));
}

test "SQL original named conflict target rejects before backend access" {
    // sql-1487: named constraints are not an admitted ON CONFLICT arbiter.
    try std.testing.expectError(error.InvalidSqlSyntax, compile(std.testing.allocator, "INSERT INTO usage_records (id, status) VALUES ('u_bad_named', 'pending') ON CONFLICT ON CONSTRAINT usage_records_id_key DO NOTHING", .{}));
}

test "SQL original multi-output mutation selector rejects before backend access" {
    // sql-0616, sql-0617: IN requires one projected value even when the
    // enclosing mutation would otherwise need a target table binding.
    for ([_][]const u8{
        "UPDATE usage_records SET status = 'archived' WHERE id IN (SELECT organization_id, status FROM archived_records)",
        "DELETE FROM usage_records WHERE id IN (SELECT organization_id, status FROM archived_records)",
    }) |sql| {
        try std.testing.expectError(error.InvalidSqlSyntax, compile(std.testing.allocator, sql, .{}));
    }
    for ([_][]const u8{
        "SELECT (SELECT 1, 2)",
        "SELECT 1 = ANY (SELECT 1, 2)",
        "SELECT 1 IN (SELECT 1 UNION ALL SELECT 2, 3)",
    }) |sql| {
        try std.testing.expectError(error.InvalidSqlSyntax, compile(std.testing.allocator, sql, .{}));
    }
}

test "SQL original unsupported catalog object commands reject before backend access" {
    // Event triggers, rules, transforms, ordinary triggers, foreign wrappers
    // and mappings, operator classes and families, and text-search objects.
    // sql-1141, sql-1142, sql-1143, sql-1144, sql-1145, sql-1146, sql-1147.
    // sql-1148, sql-1149, sql-1150, sql-1151, sql-1152, sql-1153, sql-1154.
    // sql-1155, sql-1156, sql-1157, sql-1158, sql-1159, sql-1160, sql-1161.
    // sql-1162, sql-1163, sql-1164, sql-1165, sql-1166, sql-1167, sql-1168.
    // sql-1169, sql-1170, sql-1171, sql-1172, sql-1173, sql-1174, sql-1175.
    // sql-1176, sql-1177, sql-1178, sql-1179, sql-1180, sql-1181, sql-1182.
    // sql-1185, sql-1186, sql-1187, sql-1188, sql-1189, sql-1190, sql-1191, sql-1192.
    const parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, @embedFile("fixtures/sql_parity_inventory.json"), .{});
    defer parsed.deinit();
    var covered: usize = 0;
    for (parsed.value.object.get("entries").?.array.items) |entry| {
        const id = entry.object.get("id").?.string;
        const ordinal = if (std.mem.startsWith(u8, id, "sql-")) std.fmt.parseInt(usize, id[4..], 10) catch continue else continue;
        if (ordinal < 1141 or ordinal > 1192 or ordinal == 1183 or ordinal == 1184) continue;
        try std.testing.expectEqualStrings("rejection", entry.object.get("source_expectation").?.string);
        var diagnostic: Diagnostic = .{};
        if (compileDiagnostic(std.testing.allocator, entry.object.get("sql").?.string, .{}, &diagnostic)) |compiled| {
            var unexpected = compiled;
            unexpected.deinit();
            std.debug.print("unexpectedly compiled {s}\n", .{id});
            return error.TestUnexpectedResult;
        } else |err| {
            try std.testing.expect(err == error.InvalidSqlSyntax or err == error.UnsupportedSqlShape);
            try std.testing.expect(diagnostic.message.len > 0);
        }
        covered += 1;
    }
    try std.testing.expectEqual(@as(usize, 50), covered);
}

test "compiler implicit projection aliases preserve clause and expression boundaries" {
    var implicit = try compile(std.testing.allocator, "SELECT id match FROM usage_records", .{});
    defer implicit.deinit();
    try std.testing.expectEqualStrings("match", implicit.statement.select.columns[0].alias.?);
    var quoted = try compile(std.testing.allocator, "SELECT id \"MixedCase\" FROM usage_records", .{});
    defer quoted.deinit();
    try std.testing.expectEqualStrings("MixedCase", quoted.statement.select.columns[0].alias.?);
    var explicit = try compile(std.testing.allocator, "SELECT id AS match FROM usage_records", .{});
    defer explicit.deinit();
    try std.testing.expectEqualStrings("match", explicit.statement.select.columns[0].alias.?);
    for ([_][]const u8{
        "SELECT id FROM usage_records WHERE id = 1",
        "SELECT id FROM usage_records GROUP BY id",
        "SELECT id FROM usage_records ORDER BY id",
        "SELECT id FROM usage_records LIMIT 2",
        "SELECT id, name FROM usage_records",
    }) |sql| {
        var statement = try compile(std.testing.allocator, sql, .{});
        defer statement.deinit();
        try std.testing.expect(statement.statement.select.columns[0].alias == null);
    }
    try std.testing.expectError(error.UnsupportedSqlShape, compile(std.testing.allocator, "SELECT id name extra FROM usage_records", .{}));
    try std.testing.expectError(error.UnsupportedSqlShape, compile(std.testing.allocator, "SELECT id 42 FROM usage_records", .{}));
}

test "compiler preserves keyword-named columns and quoted SQL-looking values" {
    var columns = try compile(std.testing.allocator, "SELECT count FROM t WHERE \"select\" = 'x''; DELETE FROM t; --'", .{});
    defer columns.deinit();
    try std.testing.expect(!columns.statement.select.count_all);
    try std.testing.expectEqualStrings("count", columns.statement.select.columns[0].field);
    try std.testing.expectEqualStrings("select", columns.statement.select.predicate.?.comparison.field);
    try std.testing.expectEqualStrings("x'; DELETE FROM t; --", columns.statement.select.predicate.?.comparison.value.string);
}
