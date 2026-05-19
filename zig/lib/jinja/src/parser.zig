// Copyright 2026 Antfly, Inc.
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

// Jinja2 parser — tokens → AST.
//
// Parses the HuggingFace chat template subset of Jinja2:
// if/elif/else, for loops, set statements, expressions with operators,
// subscript/dot access, filters, inline if ternary.

const std = @import("std");
const ast = @import("ast.zig");
const lexer_mod = @import("lexer.zig");
const Lexer = lexer_mod.Lexer;
const TokenKind = lexer_mod.TokenKind;
const Token = lexer_mod.Token;
const Expr = ast.Expr;
const Node = ast.Node;

pub const ParseError = error{
    UnexpectedToken,
    UnexpectedEndOfTemplate,
    UnterminatedBlock,
    InvalidSyntax,
    OutOfMemory,
};

pub const Parser = struct {
    lexer: Lexer,
    arena: std.mem.Allocator,
    /// Buffered token for look-ahead.
    peeked: ?Token = null,
    /// Strip flag from the most recently consumed stmt_open token.
    last_stmt_open_strip: bool = false,

    pub fn init(source: []const u8, arena: std.mem.Allocator) Parser {
        return .{
            .lexer = Lexer.init(source),
            .arena = arena,
        };
    }

    pub fn parse(source: []const u8, arena: std.mem.Allocator) ParseError![]const *Node {
        var p = Parser.init(source, arena);
        return p.parseBody(null);
    }

    // --- Token helpers ---

    fn nextToken(self: *Parser) Token {
        if (self.peeked) |tok| {
            self.peeked = null;
            return tok;
        }
        return self.lexer.next();
    }

    fn peekToken(self: *Parser) Token {
        if (self.peeked) |tok| return tok;
        self.peeked = self.lexer.next();
        return self.peeked.?;
    }

    fn expectIdent(self: *Parser, name: []const u8) ParseError!void {
        const tok = self.nextToken();
        if (tok.kind != .ident or !std.mem.eql(u8, tok.value, name)) {
            return error.UnexpectedToken;
        }
    }

    fn expect(self: *Parser, kind: TokenKind) ParseError!Token {
        const tok = self.nextToken();
        if (tok.kind != kind) return error.UnexpectedToken;
        return tok;
    }

    // --- Template body parsing ---

    /// Parse nodes until a stop keyword is hit. stop_keywords = null means parse to EOF.
    fn parseBody(self: *Parser, stop_keywords: ?[]const []const u8) ParseError![]const *Node {
        var nodes = std.ArrayListUnmanaged(*Node).empty;

        while (true) {
            const tok = self.peekToken();

            switch (tok.kind) {
                .eof => {
                    if (stop_keywords != null) return error.UnterminatedBlock;
                    break;
                },
                .text => {
                    _ = self.nextToken();
                    const node = try self.allocNode();
                    node.* = .{ .text = tok.value };
                    try nodes.append(self.arena, node);
                },
                .expr_open => {
                    const node = try self.parseOutput();
                    try nodes.append(self.arena, node);
                },
                .stmt_open => {
                    // Peek at the keyword after {%
                    const open_tok = self.nextToken(); // consume stmt_open
                    self.last_stmt_open_strip = open_tok.strip;
                    const kw = self.peekToken();

                    if (kw.kind == .ident) {
                        // Check if this is a stop keyword
                        if (stop_keywords) |stops| {
                            for (stops) |stop| {
                                if (std.mem.eql(u8, kw.value, stop)) {
                                    // Don't consume — caller handles it
                                    return try nodes.toOwnedSlice(self.arena);
                                }
                            }
                        }

                        if (std.mem.eql(u8, kw.value, "if")) {
                            const node = try self.parseIf();
                            try nodes.append(self.arena, node);
                        } else if (std.mem.eql(u8, kw.value, "for")) {
                            const node = try self.parseFor();
                            try nodes.append(self.arena, node);
                        } else if (std.mem.eql(u8, kw.value, "set")) {
                            const node = try self.parseSet();
                            try nodes.append(self.arena, node);
                        } else if (std.mem.eql(u8, kw.value, "macro")) {
                            const node = try self.parseMacro();
                            try nodes.append(self.arena, node);
                        } else {
                            return error.UnexpectedToken;
                        }
                    } else {
                        return error.UnexpectedToken;
                    }
                },
                else => {
                    // Skip unexpected tokens
                    _ = self.nextToken();
                },
            }
        }

        return try nodes.toOwnedSlice(self.arena);
    }

    /// Parse {{ expr }}
    fn parseOutput(self: *Parser) ParseError!*Node {
        const open = self.nextToken(); // consume expr_open
        const strip_left = open.strip;
        const expr = try self.parseExpr();
        const close = try self.expect(.expr_close);
        const strip_right = close.strip;

        const node = try self.allocNode();
        node.* = .{ .output = .{
            .expr = expr,
            .strip_left = strip_left,
            .strip_right = strip_right,
        } };
        return node;
    }

    /// Parse {% if COND %}...{% elif COND %}...{% else %}...{% endif %}
    /// Assumes stmt_open already consumed, "if" keyword is next.
    fn parseIf(self: *Parser) ParseError!*Node {
        var branches = std.ArrayListUnmanaged(ast.IfBranch).empty;

        // First branch: if
        const if_open_strip = self.last_stmt_open_strip;
        try self.expectIdent("if");
        const cond = try self.parseExpr();
        const if_close = try self.expect(.stmt_close);

        const body = try self.parseBody(&.{ "elif", "else", "endif" });
        // After parseBody returns, last_stmt_open_strip has the strip from the next tag's {%
        try branches.append(self.arena, .{
            .condition = cond,
            .body = body,
            .open_strip_left = if_open_strip,
            .open_strip_right = if_close.strip,
            .close_strip_left = self.last_stmt_open_strip,
        });

        // elif / else branches
        while (true) {
            const kw = self.peekToken();
            if (kw.kind != .ident) return error.UnexpectedToken;

            if (std.mem.eql(u8, kw.value, "elif")) {
                const elif_open_strip = self.last_stmt_open_strip;
                _ = self.nextToken(); // consume "elif"
                const elif_cond = try self.parseExpr();
                const elif_close = try self.expect(.stmt_close);
                // Set previous branch's close_strip_right
                branches.items[branches.items.len - 1].close_strip_right = elif_close.strip;
                const elif_body = try self.parseBody(&.{ "elif", "else", "endif" });
                try branches.append(self.arena, .{
                    .condition = elif_cond,
                    .body = elif_body,
                    .open_strip_left = elif_open_strip,
                    .open_strip_right = elif_close.strip,
                    .close_strip_left = self.last_stmt_open_strip,
                });
            } else if (std.mem.eql(u8, kw.value, "else")) {
                const else_open_strip = self.last_stmt_open_strip;
                _ = self.nextToken(); // consume "else"
                const else_close = try self.expect(.stmt_close);
                // Set previous branch's close_strip_right
                branches.items[branches.items.len - 1].close_strip_right = else_close.strip;
                const else_body = try self.parseBody(&.{"endif"});
                try branches.append(self.arena, .{
                    .condition = null,
                    .body = else_body,
                    .open_strip_left = else_open_strip,
                    .open_strip_right = else_close.strip,
                    .close_strip_left = self.last_stmt_open_strip,
                });
                // Must be followed by endif
                try self.expectIdent("endif");
                const endif_close = try self.expect(.stmt_close);
                branches.items[branches.items.len - 1].close_strip_right = endif_close.strip;
                break;
            } else if (std.mem.eql(u8, kw.value, "endif")) {
                _ = self.nextToken(); // consume "endif"
                const endif_close = try self.expect(.stmt_close);
                branches.items[branches.items.len - 1].close_strip_right = endif_close.strip;
                break;
            } else {
                return error.UnexpectedToken;
            }
        }

        const node = try self.allocNode();
        node.* = .{ .if_stmt = .{
            .branches = try branches.toOwnedSlice(self.arena),
        } };
        return node;
    }

    /// Parse {% for VAR in EXPR %} or {% for KEY, VAL in EXPR %}...{% endfor %}
    /// Assumes stmt_open already consumed, "for" keyword is next.
    fn parseFor(self: *Parser) ParseError!*Node {
        const open_strip_left = self.last_stmt_open_strip;
        try self.expectIdent("for");
        const var_tok = try self.expect(.ident);

        // Check for tuple unpacking: for key, value in ...
        var value_name: ?[]const u8 = null;
        if (self.peekToken().kind == .comma) {
            _ = self.nextToken(); // consume ,
            const val_tok = try self.expect(.ident);
            value_name = val_tok.value;
        }

        try self.expectIdent("in");
        const iterable = try self.parseExpr();
        const open_close = try self.expect(.stmt_close);

        const body = try self.parseBody(&.{ "else", "endfor" });

        var else_body: []const *Node = &.{};
        const kw = self.peekToken();
        if (kw.kind == .ident and std.mem.eql(u8, kw.value, "else")) {
            _ = self.nextToken();
            _ = try self.expect(.stmt_close);
            else_body = try self.parseBody(&.{"endfor"});
        }

        const endfor_open_strip = self.last_stmt_open_strip;
        try self.expectIdent("endfor");
        const endfor_close = try self.expect(.stmt_close);

        const node = try self.allocNode();
        node.* = .{ .for_stmt = .{
            .var_name = var_tok.value,
            .value_name = value_name,
            .iterable = iterable,
            .body = body,
            .else_body = else_body,
            .open_strip_left = open_strip_left,
            .open_strip_right = open_close.strip,
            .close_strip_left = endfor_open_strip,
            .close_strip_right = endfor_close.strip,
        } };
        return node;
    }

    /// Parse {% set NAME = EXPR %} or {% set NAME.ATTR = EXPR %}
    /// Assumes stmt_open already consumed, "set" keyword is next.
    fn parseSet(self: *Parser) ParseError!*Node {
        const strip_left = self.last_stmt_open_strip;
        try self.expectIdent("set");
        const name = try self.expect(.ident);

        // Check for dotted assignment: set ns.attr = value
        var attr: ?[]const u8 = null;
        if (self.peekToken().kind == .dot) {
            _ = self.nextToken(); // consume .
            const attr_tok = try self.expect(.ident);
            attr = attr_tok.value;
        }

        _ = try self.expect(.assign);
        const value = try self.parseExpr();
        const close_tok = try self.expect(.stmt_close);

        const node = try self.allocNode();
        node.* = .{ .set_stmt = .{
            .name = name.value,
            .attr = attr,
            .value = value,
            .strip_left = strip_left,
            .strip_right = close_tok.strip,
        } };
        return node;
    }

    /// Parse {% macro NAME(params) %} ... {% endmacro %}
    /// Assumes stmt_open already consumed, "macro" keyword is next.
    fn parseMacro(self: *Parser) ParseError!*Node {
        const strip_left = self.last_stmt_open_strip;
        try self.expectIdent("macro");
        const name_tok = try self.expect(.ident);
        _ = try self.expect(.lparen);

        // Parse parameter list
        var params = std.ArrayListUnmanaged(ast.MacroParam).empty;
        while (self.peekToken().kind != .rparen) {
            if (params.items.len > 0) _ = try self.expect(.comma);
            const param_name = try self.expect(.ident);
            var default_val: ?*Expr = null;
            if (self.peekToken().kind == .assign) {
                _ = self.nextToken(); // consume =
                default_val = try self.parseExpr();
            }
            try params.append(self.arena, .{ .name = param_name.value, .default = default_val });
        }
        _ = try self.expect(.rparen);
        const open_close = try self.expect(.stmt_close);

        const body = try self.parseBody(&.{"endmacro"});
        const close_strip_left = self.last_stmt_open_strip;
        try self.expectIdent("endmacro");
        const close_tok = try self.expect(.stmt_close);

        const node = try self.allocNode();
        node.* = .{ .macro_stmt = .{
            .name = name_tok.value,
            .params = try params.toOwnedSlice(self.arena),
            .body = body,
            .strip_left = strip_left,
            .strip_right = open_close.strip,
            .close_strip_left = close_strip_left,
            .close_strip_right = close_tok.strip,
        } };
        return node;
    }

    // --- Expression parsing (precedence climbing) ---

    /// Parse a full expression (lowest precedence).
    pub fn parseExpr(self: *Parser) ParseError!*Expr {
        return self.parseInlineIf();
    }

    /// Inline if: true_expr if condition else false_expr
    fn parseInlineIf(self: *Parser) ParseError!*Expr {
        const expr = try self.parseOr();

        const tok = self.peekToken();
        if (tok.kind == .ident and std.mem.eql(u8, tok.value, "if")) {
            _ = self.nextToken(); // consume "if"
            const condition = try self.parseOr();

            var false_expr: ?*Expr = null;
            const tok2 = self.peekToken();
            if (tok2.kind == .ident and std.mem.eql(u8, tok2.value, "else")) {
                _ = self.nextToken(); // consume "else"
                false_expr = try self.parseOr();
            }

            const result = try self.allocExpr();
            result.* = .{ .inline_if = .{
                .true_expr = expr,
                .condition = condition,
                .false_expr = false_expr,
            } };
            return result;
        }

        return expr;
    }

    /// or
    fn parseOr(self: *Parser) ParseError!*Expr {
        var left = try self.parseAnd();
        while (true) {
            const tok = self.peekToken();
            if (tok.kind == .ident and std.mem.eql(u8, tok.value, "or")) {
                _ = self.nextToken();
                const right = try self.parseAnd();
                const node = try self.allocExpr();
                node.* = .{ .bin_op = .{ .op = .@"or", .left = left, .right = right } };
                left = node;
            } else break;
        }
        return left;
    }

    /// and
    fn parseAnd(self: *Parser) ParseError!*Expr {
        var left = try self.parseNot();
        while (true) {
            const tok = self.peekToken();
            if (tok.kind == .ident and std.mem.eql(u8, tok.value, "and")) {
                _ = self.nextToken();
                const right = try self.parseNot();
                const node = try self.allocExpr();
                node.* = .{ .bin_op = .{ .op = .@"and", .left = left, .right = right } };
                left = node;
            } else break;
        }
        return left;
    }

    /// not
    fn parseNot(self: *Parser) ParseError!*Expr {
        const tok = self.peekToken();
        if (tok.kind == .ident and std.mem.eql(u8, tok.value, "not")) {
            _ = self.nextToken();
            const operand = try self.parseNot();
            const node = try self.allocExpr();
            node.* = .{ .unary_op = .{ .op = .not, .operand = operand } };
            return node;
        }
        return self.parseCompare();
    }

    /// ==, !=, <, >, <=, >=, in, not in, is
    fn parseCompare(self: *Parser) ParseError!*Expr {
        var left = try self.parseAddSub();
        while (true) {
            const tok = self.peekToken();

            // "is" tests
            if (tok.kind == .ident and std.mem.eql(u8, tok.value, "is")) {
                _ = self.nextToken();
                var negated = false;
                const maybe_not = self.peekToken();
                if (maybe_not.kind == .ident and std.mem.eql(u8, maybe_not.value, "not")) {
                    _ = self.nextToken();
                    negated = true;
                }
                const test_tok = try self.expect(.ident);
                const unary_op: ast.UnaryOp = if (std.mem.eql(u8, test_tok.value, "defined"))
                    if (negated) .is_not_defined else .is_defined
                else if (std.mem.eql(u8, test_tok.value, "string"))
                    .is_string
                else if (std.mem.eql(u8, test_tok.value, "iterable"))
                    .is_iterable
                else if (std.mem.eql(u8, test_tok.value, "mapping"))
                    .is_mapping
                else if (std.mem.eql(u8, test_tok.value, "sequence"))
                    .is_sequence
                else if (std.mem.eql(u8, test_tok.value, "none"))
                    if (negated) .is_not_none else .is_none
                else if (std.mem.eql(u8, test_tok.value, "boolean"))
                    .is_boolean
                else
                    return error.UnexpectedToken;

                const node = try self.allocExpr();
                // For negated tests other than defined/none, wrap in not
                if (negated and unary_op != .is_not_defined and unary_op != .is_not_none) {
                    const inner = try self.allocExpr();
                    inner.* = .{ .unary_op = .{ .op = unary_op, .operand = left } };
                    node.* = .{ .unary_op = .{ .op = .not, .operand = inner } };
                } else {
                    node.* = .{ .unary_op = .{ .op = unary_op, .operand = left } };
                }
                left = node;
                continue;
            }

            // "in" operator
            if (tok.kind == .ident and std.mem.eql(u8, tok.value, "in")) {
                _ = self.nextToken();
                const right = try self.parseAddSub();
                const node = try self.allocExpr();
                node.* = .{ .bin_op = .{ .op = .in_op, .left = left, .right = right } };
                left = node;
                continue;
            }

            // "not in" operator
            if (tok.kind == .ident and std.mem.eql(u8, tok.value, "not")) {
                // Save state in case this is not "not in"
                const save_lexer_pos = self.lexer.pos;
                const save_lexer_in_tag = self.lexer.in_tag;
                const save_peeked = self.peeked;
                _ = self.nextToken(); // consume "not"
                const after_not = self.peekToken();
                if (after_not.kind == .ident and std.mem.eql(u8, after_not.value, "in")) {
                    _ = self.nextToken(); // consume "in"
                    const right = try self.parseAddSub();
                    const node = try self.allocExpr();
                    node.* = .{ .bin_op = .{ .op = .not_in, .left = left, .right = right } };
                    left = node;
                    continue;
                }
                // Not "not in" — restore parser state
                self.lexer.pos = save_lexer_pos;
                self.lexer.in_tag = save_lexer_in_tag;
                self.peeked = save_peeked;
                break;
            }

            const op: ?ast.BinOp = switch (tok.kind) {
                .eq => .eq,
                .ne => .ne,
                .lt => .lt,
                .gt => .gt,
                .le => .le,
                .ge => .ge,
                else => null,
            };
            if (op) |o| {
                _ = self.nextToken();
                const right = try self.parseAddSub();
                const node = try self.allocExpr();
                node.* = .{ .bin_op = .{ .op = o, .left = left, .right = right } };
                left = node;
            } else break;
        }
        return left;
    }

    /// +, -, ~
    fn parseAddSub(self: *Parser) ParseError!*Expr {
        var left = try self.parseMulDiv();
        while (true) {
            const tok = self.peekToken();
            const op: ?ast.BinOp = switch (tok.kind) {
                .plus => .add,
                .minus => .sub,
                .tilde => .concat,
                else => null,
            };
            if (op) |o| {
                _ = self.nextToken();
                const right = try self.parseMulDiv();
                const node = try self.allocExpr();
                node.* = .{ .bin_op = .{ .op = o, .left = left, .right = right } };
                left = node;
            } else break;
        }
        return left;
    }

    /// *, /, %
    fn parseMulDiv(self: *Parser) ParseError!*Expr {
        var left = try self.parseUnary();
        while (true) {
            const tok = self.peekToken();
            const op: ?ast.BinOp = switch (tok.kind) {
                .star => .mul,
                .slash => .div,
                .percent => .mod,
                else => null,
            };
            if (op) |o| {
                _ = self.nextToken();
                const right = try self.parseUnary();
                const node = try self.allocExpr();
                node.* = .{ .bin_op = .{ .op = o, .left = left, .right = right } };
                left = node;
            } else break;
        }
        return left;
    }

    /// Unary -
    fn parseUnary(self: *Parser) ParseError!*Expr {
        const tok = self.peekToken();
        if (tok.kind == .minus) {
            _ = self.nextToken();
            const operand = try self.parseUnary();
            const node = try self.allocExpr();
            node.* = .{ .unary_op = .{ .op = .neg, .operand = operand } };
            return node;
        }
        return self.parseFilter();
    }

    /// expr | filter | filter(args)
    fn parseFilter(self: *Parser) ParseError!*Expr {
        var expr = try self.parsePostfix();
        while (self.peekToken().kind == .pipe) {
            _ = self.nextToken(); // consume |
            const name_tok = try self.expect(.ident);
            var args = std.ArrayListUnmanaged(*Expr).empty;

            if (self.peekToken().kind == .lparen) {
                _ = self.nextToken(); // consume (
                while (self.peekToken().kind != .rparen) {
                    if (args.items.len > 0) _ = try self.expect(.comma);
                    const arg = try self.parseExpr();
                    try args.append(self.arena, arg);
                }
                _ = try self.expect(.rparen);
            }

            const node = try self.allocExpr();
            node.* = .{ .filter = .{
                .expr = expr,
                .name = name_tok.value,
                .args = try args.toOwnedSlice(self.arena),
            } };
            expr = node;
        }
        return expr;
    }

    /// Postfix: dot access, subscript, function call
    fn parsePostfix(self: *Parser) ParseError!*Expr {
        var expr = try self.parsePrimary();

        while (true) {
            const tok = self.peekToken();
            switch (tok.kind) {
                .dot => {
                    _ = self.nextToken(); // consume .
                    const attr = try self.expect(.ident);
                    const node = try self.allocExpr();
                    node.* = .{ .get_attr = .{ .obj = expr, .attr = attr.value } };
                    expr = node;
                },
                .lbracket => {
                    _ = self.nextToken(); // consume [

                    // Check for slice: [start:stop]
                    const next = self.peekToken();
                    if (next.kind == .colon) {
                        // [:stop]
                        _ = self.nextToken(); // consume :
                        var stop: ?*Expr = null;
                        if (self.peekToken().kind != .rbracket) {
                            stop = try self.parseExpr();
                        }
                        _ = try self.expect(.rbracket);
                        const node = try self.allocExpr();
                        node.* = .{ .slice = .{ .obj = expr, .start = null, .stop = stop } };
                        expr = node;
                    } else {
                        const key = try self.parseExpr();
                        if (self.peekToken().kind == .colon) {
                            // [start:stop]
                            _ = self.nextToken(); // consume :
                            var stop: ?*Expr = null;
                            if (self.peekToken().kind != .rbracket) {
                                stop = try self.parseExpr();
                            }
                            _ = try self.expect(.rbracket);
                            const node = try self.allocExpr();
                            node.* = .{ .slice = .{ .obj = expr, .start = key, .stop = stop } };
                            expr = node;
                        } else {
                            _ = try self.expect(.rbracket);
                            const node = try self.allocExpr();
                            node.* = .{ .get_item = .{ .obj = expr, .key = key } };
                            expr = node;
                        }
                    }
                },
                .lparen => {
                    _ = self.nextToken(); // consume (
                    var args = std.ArrayListUnmanaged(*Expr).empty;
                    var kwargs = std.ArrayListUnmanaged(ast.KwArg).empty;

                    while (self.peekToken().kind != .rparen) {
                        if (args.items.len > 0 or kwargs.items.len > 0)
                            _ = try self.expect(.comma);

                        // Check for kwarg: name=value
                        const arg = try self.parseExpr();
                        if (self.peekToken().kind == .assign) {
                            _ = self.nextToken();
                            const name = switch (arg.*) {
                                .name => |n| n,
                                else => return error.InvalidSyntax,
                            };
                            const val = try self.parseExpr();
                            try kwargs.append(self.arena, .{ .key = name, .value = val });
                        } else {
                            try args.append(self.arena, arg);
                        }
                    }
                    _ = try self.expect(.rparen);
                    const node = try self.allocExpr();
                    node.* = .{ .call = .{
                        .func = expr,
                        .args = try args.toOwnedSlice(self.arena),
                        .kwargs = try kwargs.toOwnedSlice(self.arena),
                    } };
                    expr = node;
                },
                else => break,
            }
        }

        return expr;
    }

    /// Primary: literals, identifiers, parenthesized expressions, list literals
    fn parsePrimary(self: *Parser) ParseError!*Expr {
        const tok = self.nextToken();

        switch (tok.kind) {
            .string => {
                const node = try self.allocExpr();
                node.* = .{ .literal = .{ .string = tok.value } };
                return node;
            },
            .integer => {
                const val = std.fmt.parseInt(i64, tok.value, 10) catch return error.InvalidSyntax;
                const node = try self.allocExpr();
                node.* = .{ .literal = .{ .integer = val } };
                return node;
            },
            .float_lit => {
                const val = std.fmt.parseFloat(f64, tok.value) catch return error.InvalidSyntax;
                const node = try self.allocExpr();
                node.* = .{ .literal = .{ .float = val } };
                return node;
            },
            .ident => {
                if (std.mem.eql(u8, tok.value, "true") or std.mem.eql(u8, tok.value, "True")) {
                    const node = try self.allocExpr();
                    node.* = .{ .literal = .{ .boolean = true } };
                    return node;
                }
                if (std.mem.eql(u8, tok.value, "false") or std.mem.eql(u8, tok.value, "False")) {
                    const node = try self.allocExpr();
                    node.* = .{ .literal = .{ .boolean = false } };
                    return node;
                }
                if (std.mem.eql(u8, tok.value, "none") or std.mem.eql(u8, tok.value, "None")) {
                    const node = try self.allocExpr();
                    node.* = .{ .literal = .none };
                    return node;
                }
                if (std.mem.eql(u8, tok.value, "namespace")) {
                    // namespace() call — treat as dict constructor with optional kwargs
                    if (self.peekToken().kind == .lparen) {
                        _ = self.nextToken(); // (
                        var pairs = std.ArrayListUnmanaged(ast.DictPair).empty;
                        while (self.peekToken().kind != .rparen) {
                            if (pairs.items.len > 0) _ = try self.expect(.comma);
                            const key_tok = try self.expect(.ident);
                            _ = try self.expect(.assign);
                            const val = try self.parseExpr();
                            const key_expr = try self.allocExpr();
                            key_expr.* = .{ .literal = .{ .string = key_tok.value } };
                            try pairs.append(self.arena, .{ .key = key_expr, .value = val });
                        }
                        _ = try self.expect(.rparen);
                        const node = try self.allocExpr();
                        node.* = .{ .dict = try pairs.toOwnedSlice(self.arena) };
                        return node;
                    }
                }
                const node = try self.allocExpr();
                node.* = .{ .name = tok.value };
                return node;
            },
            .lparen => {
                const expr = try self.parseExpr();
                _ = try self.expect(.rparen);
                return expr;
            },
            .lbracket => {
                var items = std.ArrayListUnmanaged(*Expr).empty;
                while (self.peekToken().kind != .rbracket) {
                    if (items.items.len > 0) _ = try self.expect(.comma);
                    const item = try self.parseExpr();
                    try items.append(self.arena, item);
                }
                _ = try self.expect(.rbracket);
                const node = try self.allocExpr();
                node.* = .{ .list = try items.toOwnedSlice(self.arena) };
                return node;
            },
            else => return error.UnexpectedToken,
        }
    }

    // --- Allocation helpers ---

    fn allocNode(self: *Parser) ParseError!*Node {
        return self.arena.create(Node) catch return error.OutOfMemory;
    }

    fn allocExpr(self: *Parser) ParseError!*Expr {
        return self.arena.create(Expr) catch return error.OutOfMemory;
    }
};

// --- Tests ---

test "parse simple output" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    const nodes = try Parser.parse("Hello {{ name }}!", arena.allocator());
    try std.testing.expectEqual(@as(usize, 3), nodes.len);
    try std.testing.expectEqualStrings("Hello ", nodes[0].text);
    try std.testing.expectEqual(.name, std.meta.activeTag(nodes[1].output.expr.*));
    try std.testing.expectEqualStrings("!", nodes[2].text);
}

test "parse if/else/endif" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    const nodes = try Parser.parse("{% if x %}yes{% else %}no{% endif %}", arena.allocator());
    try std.testing.expectEqual(@as(usize, 1), nodes.len);
    const if_stmt = nodes[0].if_stmt;
    try std.testing.expectEqual(@as(usize, 2), if_stmt.branches.len);
    try std.testing.expect(if_stmt.branches[0].condition != null); // if
    try std.testing.expect(if_stmt.branches[1].condition == null); // else
}

test "parse for loop" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    const nodes = try Parser.parse("{% for msg in messages %}{{ msg }}{% endfor %}", arena.allocator());
    try std.testing.expectEqual(@as(usize, 1), nodes.len);
    const for_stmt = nodes[0].for_stmt;
    try std.testing.expectEqualStrings("msg", for_stmt.var_name);
    try std.testing.expectEqual(@as(usize, 1), for_stmt.body.len);
}

test "parse set statement" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    const nodes = try Parser.parse("{% set x = 42 %}", arena.allocator());
    try std.testing.expectEqual(@as(usize, 1), nodes.len);
    const set = nodes[0].set_stmt;
    try std.testing.expectEqualStrings("x", set.name);
    try std.testing.expectEqual(@as(i64, 42), set.value.literal.integer);
}

test "parse dot access" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    const nodes = try Parser.parse("{{ loop.first }}", arena.allocator());
    const expr = nodes[0].output.expr;
    try std.testing.expectEqual(.get_attr, std.meta.activeTag(expr.*));
    try std.testing.expectEqualStrings("first", expr.get_attr.attr);
}

test "parse subscript" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    const nodes = try Parser.parse("{{ messages[0] }}", arena.allocator());
    const expr = nodes[0].output.expr;
    try std.testing.expectEqual(.get_item, std.meta.activeTag(expr.*));
}

test "parse slice" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    const nodes = try Parser.parse("{{ messages[1:] }}", arena.allocator());
    const expr = nodes[0].output.expr;
    try std.testing.expectEqual(.slice, std.meta.activeTag(expr.*));
    try std.testing.expect(expr.slice.start != null);
    try std.testing.expect(expr.slice.stop == null);
}

test "parse string concatenation" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    const nodes = try Parser.parse("{{ 'hello' + ' ' + 'world' }}", arena.allocator());
    const expr = nodes[0].output.expr;
    try std.testing.expectEqual(.bin_op, std.meta.activeTag(expr.*));
    try std.testing.expectEqual(ast.BinOp.add, expr.bin_op.op);
}

test "parse filter" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    const nodes = try Parser.parse("{{ name | trim }}", arena.allocator());
    const expr = nodes[0].output.expr;
    try std.testing.expectEqual(.filter, std.meta.activeTag(expr.*));
    try std.testing.expectEqualStrings("trim", expr.filter.name);
}

test "parse comparison" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    const nodes = try Parser.parse("{% if role == 'user' %}yes{% endif %}", arena.allocator());
    const if_stmt = nodes[0].if_stmt;
    const cond = if_stmt.branches[0].condition.?;
    try std.testing.expectEqual(.bin_op, std.meta.activeTag(cond.*));
    try std.testing.expectEqual(ast.BinOp.eq, cond.bin_op.op);
}
