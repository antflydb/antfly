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

const std = @import("std");
const ast = @import("ast.zig");
const lexer_mod = @import("lexer.zig");
const Token = lexer_mod.Token;
const TokenKind = lexer_mod.TokenKind;
const Lexer = lexer_mod.Lexer;
const Allocator = std.mem.Allocator;

pub const ParseError = error{
    UnexpectedToken,
    UnexpectedEof,
    UnclosedBlock,
    MismatchedBlock,
    InvalidPath,
    OutOfMemory,
};

pub const Parser = struct {
    lexer: Lexer,
    arena: Allocator,
    buf: [4]Token = undefined,
    buf_len: u8 = 0,

    pub fn parse(source: []const u8, arena: Allocator) ParseError!*ast.Node {
        var p = Parser{ .lexer = Lexer.init(source), .arena = arena };
        return p.parseProgram(false);
    }

    // ---------------------------------------------------------------
    // Token access
    // ---------------------------------------------------------------

    fn peek(self: *Parser) Token {
        self.ensure(1);
        return self.buf[0];
    }

    fn peekAt(self: *Parser, idx: u8) Token {
        self.ensure(idx + 1);
        return self.buf[idx];
    }

    fn consume(self: *Parser) Token {
        self.ensure(1);
        const tok = self.buf[0];
        if (self.buf_len > 1) {
            std.mem.copyForwards(Token, self.buf[0 .. self.buf_len - 1], self.buf[1..self.buf_len]);
        }
        self.buf_len -= 1;
        return tok;
    }

    fn expect(self: *Parser, kind: TokenKind) ParseError!Token {
        const tok = self.consume();
        if (tok.kind != kind) return error.UnexpectedToken;
        return tok;
    }

    fn ensure(self: *Parser, n: u8) void {
        while (self.buf_len < n) {
            self.buf[self.buf_len] = self.lexer.next();
            self.buf_len += 1;
        }
    }

    fn isElse(self: *Parser) bool {
        self.ensure(2);
        return self.buf[0].kind == .open and
            self.buf[1].kind == .id and
            std.mem.eql(u8, self.buf[1].value, "else");
    }

    // ---------------------------------------------------------------
    // Grammar
    // ---------------------------------------------------------------

    fn parseProgram(self: *Parser, stop_at_inverse: bool) ParseError!*ast.Node {
        var body: std.ArrayListUnmanaged(*ast.Node) = .empty;

        while (true) {
            const tok = self.peek();
            switch (tok.kind) {
                .eof, .open_end_block => break,
                .open_inverse => {
                    if (stop_at_inverse) break;
                    // Standalone inverse block like {{^name}}...
                    try body.append(self.arena, try self.parseStatement());
                },
                .open => {
                    if (stop_at_inverse and self.isElse()) break;
                    try body.append(self.arena, try self.parseStatement());
                },
                else => try body.append(self.arena, try self.parseStatement()),
            }
        }

        // Post-process: detect standalone partials and extract indent
        self.extractStandalonePartialIndent(&body);

        const node = try self.arena.create(ast.Node);
        node.* = .{ .program = .{ .body = try self.arena.dupe(*ast.Node, body.items) } };
        return node;
    }

    /// Detect standalone partial lines: whitespace-only content before a partial,
    /// followed by nothing or newline. Extract the whitespace as indent on the partial.
    fn extractStandalonePartialIndent(self: *Parser, body: *std.ArrayListUnmanaged(*ast.Node)) void {
        _ = self;
        var i: usize = 0;
        while (i < body.items.len) : (i += 1) {
            const node = body.items[i];
            const is_partial = node.* == .partial or node.* == .partial_block;
            if (!is_partial) continue;

            // Check preceding node: must be content that is empty or ends with newline + whitespace only
            var indent: []const u8 = "";
            var has_preceding_indent = false;
            if (i == 0) {
                // At start of template — standalone if nothing before
                has_preceding_indent = true;
            } else if (body.items[i - 1].* == .content) {
                const prev = body.items[i - 1].content.value;
                // Find last newline in preceding content
                if (std.mem.lastIndexOfScalar(u8, prev, '\n')) |nl_pos| {
                    const after_nl = prev[nl_pos + 1 ..];
                    if (isOnlyWhitespace(after_nl)) {
                        indent = after_nl;
                        has_preceding_indent = true;
                    }
                } else {
                    // No newline — only standalone if it's the first content and all whitespace
                    if (i == 1 and isOnlyWhitespace(prev)) {
                        indent = prev;
                        has_preceding_indent = true;
                    }
                }
            }

            if (!has_preceding_indent) continue;

            // Check following node: must be nothing, or content starting with newline
            var is_standalone = false;
            if (i + 1 >= body.items.len) {
                is_standalone = true;
            } else if (body.items[i + 1].* == .content) {
                const next_val = body.items[i + 1].content.value;
                if (next_val.len > 0 and next_val[0] == '\n') {
                    is_standalone = true;
                } else if (next_val.len > 1 and next_val[0] == '\r' and next_val[1] == '\n') {
                    is_standalone = true;
                }
            }

            if (!is_standalone) continue;

            // Apply indent to partial
            switch (node.*) {
                .partial => |*p| p.indent = indent,
                .partial_block => {}, // partial blocks don't indent
                else => {},
            }

            // Trim the indent from preceding content
            if (indent.len > 0 and i > 0 and body.items[i - 1].* == .content) {
                const prev = body.items[i - 1].content.value;
                if (std.mem.lastIndexOfScalar(u8, prev, '\n')) |nl_pos| {
                    body.items[i - 1].content.value = prev[0 .. nl_pos + 1];
                } else {
                    // Was at start of template — remove entirely
                    body.items[i - 1].content.value = "";
                }
            }

            // Note: we do NOT strip the trailing newline from the following content.
            // The standalone partial consumes the indent but preserves the structure.
        }
    }

    fn isOnlyWhitespace(s: []const u8) bool {
        for (s) |c| {
            if (c != ' ' and c != '\t') return false;
        }
        return true;
    }

    fn parseStatement(self: *Parser) ParseError!*ast.Node {
        const tok = self.peek();
        return switch (tok.kind) {
            .content => self.parseContent(),
            .comment => self.parseComment(),
            .open => self.parseMustache(),
            .open_unescaped => self.parseMustacheUnescaped(),
            .open_block => self.parseBlock(),
            .open_inverse => self.parseInverseBlock(),
            .open_partial => self.parsePartial(),
            .open_partial_block => self.parsePartialBlock(),
            .open_inline => self.parseInlinePartial(),
            else => error.UnexpectedToken,
        };
    }

    fn parseContent(self: *Parser) ParseError!*ast.Node {
        const tok = self.consume();
        const node = try self.arena.create(ast.Node);
        node.* = .{ .content = .{ .value = tok.value, .loc = .{ .line = tok.line, .col = tok.col } } };
        return node;
    }

    fn parseComment(self: *Parser) ParseError!*ast.Node {
        const tok = self.consume();
        const node = try self.arena.create(ast.Node);
        node.* = .{ .comment = .{ .value = tok.value, .loc = .{ .line = tok.line, .col = tok.col } } };
        return node;
    }

    fn parseMustache(self: *Parser) ParseError!*ast.Node {
        const open = self.consume(); // OPEN
        const unescaped = std.mem.eql(u8, open.value, "{{&");
        const expr = try self.parseExpression();
        const close = try self.expect(.close);

        const node = try self.arena.create(ast.Node);
        node.* = .{ .mustache = .{
            .expression = expr,
            .unescaped = unescaped,
            .strip = .{ .open = isOpenStrip(open), .close = isCloseStrip(close) },
            .loc = .{ .line = open.line, .col = open.col },
        } };
        return node;
    }

    fn parseMustacheUnescaped(self: *Parser) ParseError!*ast.Node {
        const open = self.consume(); // OPEN_UNESCAPED
        const expr = try self.parseExpression();
        const close = try self.expect(.close_unescaped);

        const node = try self.arena.create(ast.Node);
        node.* = .{ .mustache = .{
            .expression = expr,
            .unescaped = true,
            .strip = .{ .open = isOpenStrip(open), .close = isCloseStrip(close) },
            .loc = .{ .line = open.line, .col = open.col },
        } };
        return node;
    }

    fn parseBlock(self: *Parser) ParseError!*ast.Node {
        const open = self.consume(); // OPEN_BLOCK
        const expr = try self.parseExpression();

        // Optional block params: as |x y|
        var block_params: []const []const u8 = &.{};
        if (self.peek().kind == .open_block_params) {
            block_params = try self.parseBlockParams();
        }

        const open_close = try self.expect(.close);

        // Parse main body
        const program = try self.parseProgram(true);
        if (block_params.len > 0) {
            program.program.block_params = block_params;
        }

        // Check for inverse ({{else}}, {{else if ...}}, or {{^}})
        var inverse: ?*ast.Node = null;
        var inv_strip: ast.Strip = .{};
        const next = self.peek();
        if (next.kind == .open and self.isElse()) {
            // Consume {{else
            const else_open = self.consume(); // open
            _ = self.consume(); // "else"
            inv_strip.open = isOpenStrip(else_open);
            if (self.peek().kind != .close) {
                // Chained: {{else if ...}} → nested block
                inverse = try self.parseChainedBlock();
            } else {
                const else_close = try self.expect(.close);
                inv_strip.close = isCloseStrip(else_close);
                inverse = try self.parseProgram(false);
            }
        } else if (next.kind == .open_inverse) {
            _ = self.consume(); // {{^
            _ = try self.expect(.close);
            inverse = try self.parseProgram(false);
        }

        // Close block: {{/name}}
        const close_open = try self.expect(.open_end_block);
        const close_name = self.consume(); // block name
        if (close_name.kind != .id) return error.UnexpectedToken;
        const close_close = try self.expect(.close);

        // Verify matching block name
        const open_name = self.getExpressionName(expr) orelse return error.MismatchedBlock;
        if (!std.mem.eql(u8, open_name, close_name.value)) {
            return error.MismatchedBlock;
        }

        const node = try self.arena.create(ast.Node);
        node.* = .{ .block = .{
            .expression = expr,
            .program = program,
            .inverse = inverse,
            .open_strip = .{ .open = isOpenStrip(open), .close = isCloseStrip(open_close) },
            .close_strip = .{ .open = isOpenStrip(close_open), .close = isCloseStrip(close_close) },
            .inverse_strip = inv_strip,
            .loc = .{ .line = open.line, .col = open.col },
        } };
        return node;
    }

    fn parseInverseBlock(self: *Parser) ParseError!*ast.Node {
        const open = self.consume(); // OPEN_INVERSE

        // {{^name}}...{{/name}} is equivalent to {{#name}}{{else}}...{{/name}}
        if (self.peek().kind == .close) {
            // Standalone {{^}} - should have been handled by block parser
            return error.UnexpectedToken;
        }

        const expr = try self.parseExpression();
        _ = try self.expect(.close);

        // Parse inverse body (this IS the inverse)
        const inverse_body = try self.parseProgram(true);

        // Check for else → becomes main body
        var main_body: ?*ast.Node = null;
        if (self.peek().kind == .open and self.isElse()) {
            _ = self.consume();
            _ = self.consume();
            if (self.peek().kind != .close) {
                main_body = try self.parseChainedBlock();
            } else {
                _ = try self.expect(.close);
                main_body = try self.parseProgram(false);
            }
        }

        _ = try self.expect(.open_end_block);
        const close_name = self.consume();
        if (close_name.kind != .id) return error.UnexpectedToken;
        _ = try self.expect(.close);

        // Verify name match
        const open_name = self.getExpressionName(expr) orelse return error.MismatchedBlock;
        if (!std.mem.eql(u8, open_name, close_name.value)) return error.MismatchedBlock;

        // {{^name}} inverts: inverse is the main block, main is the else
        const empty_prog = try self.arena.create(ast.Node);
        empty_prog.* = .{ .program = .{ .body = &.{} } };

        const node = try self.arena.create(ast.Node);
        node.* = .{ .block = .{
            .expression = expr,
            .program = main_body orelse empty_prog,
            .inverse = inverse_body,
            .loc = .{ .line = open.line, .col = open.col },
        } };
        return node;
    }

    fn parseChainedBlock(self: *Parser) ParseError!*ast.Node {
        // After consuming {{ and "else", tokens are: helper params... }}
        const expr = try self.parseExpression();

        var block_params: []const []const u8 = &.{};
        if (self.peek().kind == .open_block_params) {
            block_params = try self.parseBlockParams();
        }

        _ = try self.expect(.close);

        const program = try self.parseProgram(true);
        if (block_params.len > 0) {
            program.program.block_params = block_params;
        }

        // Check for further chaining
        var inverse: ?*ast.Node = null;
        if (self.peek().kind == .open and self.isElse()) {
            _ = self.consume(); // {{
            _ = self.consume(); // else
            if (self.peek().kind != .close) {
                inverse = try self.parseChainedBlock();
            } else {
                _ = try self.expect(.close);
                inverse = try self.parseProgram(false);
            }
        }

        // Create block node wrapped in program
        const block_node = try self.arena.create(ast.Node);
        block_node.* = .{ .block = .{
            .expression = expr,
            .program = program,
            .inverse = inverse,
        } };

        const body = try self.arena.alloc(*ast.Node, 1);
        body[0] = block_node;

        const prog_node = try self.arena.create(ast.Node);
        prog_node.* = .{ .program = .{ .body = body } };
        return prog_node;
    }

    fn parsePartial(self: *Parser) ParseError!*ast.Node {
        const open = self.consume(); // OPEN_PARTIAL

        // Name can be a path or sub-expression (dynamic partial)
        const name = if (self.peek().kind == .open_sexpr)
            try self.parseSubExpression()
        else
            try self.parsePath();

        var params: std.ArrayListUnmanaged(*ast.Node) = .empty;
        var hash_node: ?*ast.Node = null;

        while (self.peek().kind != .close and self.peek().kind != .eof) {
            if (self.isHashStart()) {
                hash_node = try self.parseHash();
                break;
            }
            try params.append(self.arena, try self.parseParam());
        }
        const close = try self.expect(.close);

        const node = try self.arena.create(ast.Node);
        node.* = .{ .partial = .{
            .name = name,
            .params = try self.arena.dupe(*ast.Node, params.items),
            .hash = hash_node,
            .strip = .{ .open = isOpenStrip(open), .close = isCloseStrip(close) },
            .loc = .{ .line = open.line, .col = open.col },
        } };
        return node;
    }

    /// {{#> name}}default content{{/name}}
    fn parsePartialBlock(self: *Parser) ParseError!*ast.Node {
        const open = self.consume(); // OPEN_PARTIAL_BLOCK

        const name = if (self.peek().kind == .open_sexpr)
            try self.parseSubExpression()
        else
            try self.parsePath();

        var params: std.ArrayListUnmanaged(*ast.Node) = .empty;
        var hash_node: ?*ast.Node = null;

        while (self.peek().kind != .close and self.peek().kind != .eof) {
            if (self.isHashStart()) {
                hash_node = try self.parseHash();
                break;
            }
            try params.append(self.arena, try self.parseParam());
        }
        const open_close = try self.expect(.close);

        // Parse default body
        const program = try self.parseProgram(false);

        // Close: {{/name}}
        const close_open = try self.expect(.open_end_block);
        const close_name = self.consume();
        if (close_name.kind != .id) return error.UnexpectedToken;
        const close_close = try self.expect(.close);

        // Verify name
        const open_name = switch (name.*) {
            .path => |p| if (p.parts.len > 0) p.parts[0] else return error.InvalidPath,
            else => return error.InvalidPath,
        };
        if (!std.mem.eql(u8, open_name, close_name.value)) return error.MismatchedBlock;

        const node = try self.arena.create(ast.Node);
        node.* = .{ .partial_block = .{
            .name = name,
            .params = try self.arena.dupe(*ast.Node, params.items),
            .hash = hash_node,
            .program = program,
            .open_strip = .{ .open = isOpenStrip(open), .close = isCloseStrip(open_close) },
            .close_strip = .{ .open = isOpenStrip(close_open), .close = isCloseStrip(close_close) },
            .loc = .{ .line = open.line, .col = open.col },
        } };
        return node;
    }

    /// {{#*inline "name"}}content{{/inline}}
    fn parseInlinePartial(self: *Parser) ParseError!*ast.Node {
        const open = self.consume(); // OPEN_INLINE

        // Expect "inline" keyword
        const kw = self.consume();
        if (kw.kind != .id or !std.mem.eql(u8, kw.value, "inline")) return error.UnexpectedToken;

        // Name is a string literal
        const name_tok = self.consume();
        if (name_tok.kind != .string) return error.UnexpectedToken;
        const name = try self.processEscapes(name_tok.value);

        _ = try self.expect(.close);

        // Parse body
        const program = try self.parseProgram(false);

        // Close: {{/inline}}
        _ = try self.expect(.open_end_block);
        const close_name = self.consume();
        if (close_name.kind != .id or !std.mem.eql(u8, close_name.value, "inline")) return error.MismatchedBlock;
        _ = try self.expect(.close);

        const node = try self.arena.create(ast.Node);
        node.* = .{ .inline_partial = .{
            .name = name,
            .program = program,
            .loc = .{ .line = open.line, .col = open.col },
        } };
        return node;
    }

    fn parseExpression(self: *Parser) ParseError!*ast.Node {
        const loc = self.peek();
        const path = try self.parseExprValue();

        var params: std.ArrayListUnmanaged(*ast.Node) = .empty;
        var hash_node: ?*ast.Node = null;

        // Parse params and hash until we hit a closing delimiter
        while (true) {
            const next = self.peek();
            if (next.kind == .close or next.kind == .close_unescaped or
                next.kind == .close_sexpr or next.kind == .eof or
                next.kind == .open_block_params)
            {
                break;
            }
            if (self.isHashStart()) {
                hash_node = try self.parseHash();
                break;
            }
            try params.append(self.arena, try self.parseParam());
        }

        const node = try self.arena.create(ast.Node);
        node.* = .{ .expression = .{
            .path = path,
            .params = try self.arena.dupe(*ast.Node, params.items),
            .hash = hash_node,
            .loc = .{ .line = loc.line, .col = loc.col },
        } };
        return node;
    }

    fn parseExprValue(self: *Parser) ParseError!*ast.Node {
        const tok = self.peek();
        return switch (tok.kind) {
            .string => self.parseStringLiteral(),
            .number => self.parseNumberLiteral(),
            .boolean => self.parseBooleanLiteral(),
            .open_sexpr => self.parseSubExpression(),
            else => self.parsePath(),
        };
    }

    fn parseParam(self: *Parser) ParseError!*ast.Node {
        const tok = self.peek();
        return switch (tok.kind) {
            .string => self.parseStringLiteral(),
            .number => self.parseNumberLiteral(),
            .boolean => self.parseBooleanLiteral(),
            .open_sexpr => self.parseSubExpression(),
            else => self.parsePath(),
        };
    }

    fn parsePath(self: *Parser) ParseError!*ast.Node {
        const loc = self.peek();
        var parts: std.ArrayListUnmanaged([]const u8) = .empty;
        var depth: u32 = 0;
        var data = false;
        var scoped = false;

        // Check for @ data prefix
        if (self.peek().kind == .data) {
            _ = self.consume();
            data = true;
        }

        // First part
        const first = self.consume();
        if (first.kind != .id) return error.InvalidPath;

        if (std.mem.eql(u8, first.value, "..")) {
            depth += 1;
            scoped = true;
        } else if (std.mem.eql(u8, first.value, ".") or std.mem.eql(u8, first.value, "this")) {
            scoped = true;
        } else {
            try parts.append(self.arena, first.value);
        }

        // Subsequent parts separated by SEP
        while (self.peek().kind == .sep) {
            _ = self.consume(); // separator
            const part = self.consume();
            if (part.kind == .id) {
                if (std.mem.eql(u8, part.value, "..")) {
                    depth += 1;
                    scoped = true;
                } else {
                    try parts.append(self.arena, part.value);
                }
            } else if (part.kind == .number) {
                // Array index: items.0, items.1, etc.
                try parts.append(self.arena, part.value);
            } else {
                return error.InvalidPath;
            }
        }

        // Build original string
        var orig_buf: std.ArrayListUnmanaged(u8) = .empty;
        if (data) try orig_buf.append(self.arena, '@');
        for (0..depth) |i| {
            if (i > 0 or parts.items.len > 0 or scoped) try orig_buf.appendSlice(self.arena, "/");
            try orig_buf.appendSlice(self.arena, "..");
        }
        if (scoped and depth > 0 and parts.items.len > 0) {
            try orig_buf.appendSlice(self.arena, "/");
        }
        for (parts.items, 0..) |part, i| {
            if (i > 0) try orig_buf.append(self.arena, '.');
            try orig_buf.appendSlice(self.arena, part);
        }

        const node = try self.arena.create(ast.Node);
        node.* = .{ .path = .{
            .original = orig_buf.items,
            .parts = try self.arena.dupe([]const u8, parts.items),
            .depth = depth,
            .data = data,
            .scoped = scoped,
            .loc = .{ .line = loc.line, .col = loc.col },
        } };
        return node;
    }

    fn parseSubExpression(self: *Parser) ParseError!*ast.Node {
        const open = self.consume(); // (
        const expr = try self.parseExpression();
        _ = try self.expect(.close_sexpr);

        const node = try self.arena.create(ast.Node);
        node.* = .{ .sub_expression = .{
            .expression = expr,
            .loc = .{ .line = open.line, .col = open.col },
        } };
        return node;
    }

    fn parseStringLiteral(self: *Parser) ParseError!*ast.Node {
        const tok = self.consume();
        const value = try self.processEscapes(tok.value);

        const node = try self.arena.create(ast.Node);
        node.* = .{ .string_literal = .{
            .value = value,
            .loc = .{ .line = tok.line, .col = tok.col },
        } };
        return node;
    }

    fn parseNumberLiteral(self: *Parser) ParseError!*ast.Node {
        const tok = self.consume();
        const is_int = std.mem.indexOfScalar(u8, tok.value, '.') == null;
        const value = std.fmt.parseFloat(f64, tok.value) catch 0;

        const node = try self.arena.create(ast.Node);
        node.* = .{ .number_literal = .{
            .value = value,
            .is_int = is_int,
            .original = tok.value,
            .loc = .{ .line = tok.line, .col = tok.col },
        } };
        return node;
    }

    fn parseBooleanLiteral(self: *Parser) ParseError!*ast.Node {
        const tok = self.consume();
        const node = try self.arena.create(ast.Node);
        node.* = .{ .boolean_literal = .{
            .value = std.mem.eql(u8, tok.value, "true"),
            .loc = .{ .line = tok.line, .col = tok.col },
        } };
        return node;
    }

    fn parseHash(self: *Parser) ParseError!*ast.Node {
        var pairs: std.ArrayListUnmanaged(*ast.Node) = .empty;

        while (self.isHashStart()) {
            const key = self.consume(); // ID
            _ = try self.expect(.equals);
            const value = try self.parseParam();

            const pair = try self.arena.create(ast.Node);
            pair.* = .{ .hash_pair = .{
                .key = key.value,
                .value = value,
                .loc = .{ .line = key.line, .col = key.col },
            } };
            try pairs.append(self.arena, pair);
        }

        const node = try self.arena.create(ast.Node);
        node.* = .{ .hash = .{
            .pairs = try self.arena.dupe(*ast.Node, pairs.items),
        } };
        return node;
    }

    fn parseBlockParams(self: *Parser) ParseError![]const []const u8 {
        _ = self.consume(); // "as |"
        var params: std.ArrayListUnmanaged([]const u8) = .empty;

        while (self.peek().kind == .id) {
            try params.append(self.arena, self.consume().value);
        }
        _ = try self.expect(.close_block_params);

        return try self.arena.dupe([]const u8, params.items);
    }

    // ---------------------------------------------------------------
    // Strip detection
    // ---------------------------------------------------------------

    fn isOpenStrip(tok: Token) bool {
        return std.mem.indexOf(u8, tok.value, "~") != null;
    }

    fn isCloseStrip(tok: Token) bool {
        return std.mem.startsWith(u8, tok.value, "~");
    }

    // ---------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------

    fn isHashStart(self: *Parser) bool {
        self.ensure(2);
        return self.buf[0].kind == .id and self.buf[1].kind == .equals;
    }

    fn getExpressionName(self: *Parser, expr: *ast.Node) ?[]const u8 {
        _ = self;
        const e = switch (expr.*) {
            .expression => |ex| ex,
            else => return null,
        };
        return switch (e.path.*) {
            .path => |p| if (p.parts.len > 0) p.parts[0] else null,
            else => null,
        };
    }

    fn processEscapes(self: *Parser, input: []const u8) ParseError![]const u8 {
        // Check if any escapes present
        if (std.mem.indexOfScalar(u8, input, '\\') == null) return input;

        var buf: std.ArrayListUnmanaged(u8) = .empty;
        var i: usize = 0;
        while (i < input.len) {
            if (input[i] == '\\' and i + 1 < input.len) {
                switch (input[i + 1]) {
                    'n' => try buf.append(self.arena, '\n'),
                    't' => try buf.append(self.arena, '\t'),
                    'r' => try buf.append(self.arena, '\r'),
                    '\\' => try buf.append(self.arena, '\\'),
                    '"' => try buf.append(self.arena, '"'),
                    '\'' => try buf.append(self.arena, '\''),
                    else => {
                        try buf.append(self.arena, '\\');
                        try buf.append(self.arena, input[i + 1]);
                    },
                }
                i += 2;
            } else {
                try buf.append(self.arena, input[i]);
                i += 1;
            }
        }
        return buf.items;
    }
};

// -----------------------------------------------------------------------
// Tests
// -----------------------------------------------------------------------

test "parse simple variable" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const node = try Parser.parse("Hello, {{name}}!", arena);
    const prog = node.program;
    try std.testing.expectEqual(3, prog.body.len);
    try std.testing.expectEqual(.content, std.meta.activeTag(prog.body[0].*));
    try std.testing.expectEqual(.mustache, std.meta.activeTag(prog.body[1].*));
    try std.testing.expectEqual(.content, std.meta.activeTag(prog.body[2].*));

    try std.testing.expectEqualStrings("Hello, ", prog.body[0].content.value);
    try std.testing.expectEqualStrings("!", prog.body[2].content.value);

    // Check mustache expression path
    const mustache = prog.body[1].mustache;
    const expr = mustache.expression.expression;
    const path = expr.path.path;
    try std.testing.expectEqual(1, path.parts.len);
    try std.testing.expectEqualStrings("name", path.parts[0]);
}

test "parse dotted path" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const node = try Parser.parse("{{user.email}}", arena);
    const mustache = node.program.body[0].mustache;
    const path = mustache.expression.expression.path.path;
    try std.testing.expectEqual(2, path.parts.len);
    try std.testing.expectEqualStrings("user", path.parts[0]);
    try std.testing.expectEqualStrings("email", path.parts[1]);
}

test "parse block with else" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const node = try Parser.parse("{{#if x}}yes{{else}}no{{/if}}", arena);
    const block = node.program.body[0].block;
    const name = block.expression.expression.path.path;
    try std.testing.expectEqualStrings("if", name.parts[0]);

    try std.testing.expectEqual(1, block.program.program.body.len);
    try std.testing.expectEqualStrings("yes", block.program.program.body[0].content.value);

    try std.testing.expect(block.inverse != null);
    try std.testing.expectEqual(1, block.inverse.?.program.body.len);
    try std.testing.expectEqualStrings("no", block.inverse.?.program.body[0].content.value);
}

test "parse each with index" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const node = try Parser.parse("{{#each items}}{{@index}}: {{this}}{{/each}}", arena);
    const block = node.program.body[0].block;
    try std.testing.expectEqualStrings("each", block.expression.expression.path.path.parts[0]);

    // Params: "items"
    const params = block.expression.expression.params;
    try std.testing.expectEqual(1, params.len);
    try std.testing.expectEqualStrings("items", params[0].path.parts[0]);
}

test "parse sub-expression" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const node = try Parser.parse("{{#if (eq a \"b\")}}yes{{/if}}", arena);
    const block = node.program.body[0].block;
    const if_params = block.expression.expression.params;
    try std.testing.expectEqual(1, if_params.len);

    // Sub-expression
    const sub = if_params[0].sub_expression;
    const sub_expr = sub.expression.expression;
    try std.testing.expectEqualStrings("eq", sub_expr.path.path.parts[0]);
    try std.testing.expectEqual(2, sub_expr.params.len);
}

test "parse hash params" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const node = try Parser.parse("{{helper key=\"val\" num=42}}", arena);
    const expr = node.program.body[0].mustache.expression.expression;
    try std.testing.expectEqualStrings("helper", expr.path.path.parts[0]);
    try std.testing.expect(expr.hash != null);

    const hash = expr.hash.?.hash;
    try std.testing.expectEqual(2, hash.pairs.len);
    try std.testing.expectEqualStrings("key", hash.pairs[0].hash_pair.key);
    try std.testing.expectEqualStrings("val", hash.pairs[0].hash_pair.value.string_literal.value);
    try std.testing.expectEqualStrings("num", hash.pairs[1].hash_pair.key);
}

test "parse comment" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const node = try Parser.parse("before{{! comment }}after", arena);
    try std.testing.expectEqual(3, node.program.body.len);
    try std.testing.expectEqual(.content, std.meta.activeTag(node.program.body[0].*));
    try std.testing.expectEqual(.comment, std.meta.activeTag(node.program.body[1].*));
    try std.testing.expectEqual(.content, std.meta.activeTag(node.program.body[2].*));
}

test "parse unescaped mustache" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const node = try Parser.parse("{{{html}}}", arena);
    const mustache = node.program.body[0].mustache;
    try std.testing.expect(mustache.unescaped);
}

test "parse else if chaining" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const node = try Parser.parse("{{#if a}}A{{else if b}}B{{else}}C{{/if}}", arena);
    const block = node.program.body[0].block;
    try std.testing.expectEqualStrings("if", block.expression.expression.path.path.parts[0]);

    // Main body: "A"
    try std.testing.expectEqual(1, block.program.program.body.len);
    try std.testing.expectEqualStrings("A", block.program.program.body[0].content.value);

    // Inverse: Program containing a nested Block(if b)
    try std.testing.expect(block.inverse != null);
    const inv_prog = block.inverse.?.program;
    try std.testing.expectEqual(1, inv_prog.body.len);
    const nested_block = inv_prog.body[0].block;
    try std.testing.expectEqualStrings("if", nested_block.expression.expression.path.path.parts[0]);

    // Nested block program: "B"
    try std.testing.expectEqual(1, nested_block.program.program.body.len);
    try std.testing.expectEqualStrings("B", nested_block.program.program.body[0].content.value);

    // Nested block inverse: "C"
    try std.testing.expect(nested_block.inverse != null);
    try std.testing.expectEqual(1, nested_block.inverse.?.program.body.len);
    try std.testing.expectEqualStrings("C", nested_block.inverse.?.program.body[0].content.value);
}

test "parse ampersand unescaped" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const node = try Parser.parse("{{&html}}", arena);
    const mustache = node.program.body[0].mustache;
    try std.testing.expect(mustache.unescaped);
    try std.testing.expectEqualStrings("html", mustache.expression.expression.path.path.parts[0]);
}

test "parse error - mismatched block" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const result = Parser.parse("{{#if x}}...{{/unless}}", arena);
    try std.testing.expectError(error.MismatchedBlock, result);
}

test "parse error - unclosed block" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const result = Parser.parse("{{#if x}}no end", arena);
    try std.testing.expectError(error.UnexpectedToken, result);
}

test "parse parent path" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const node = try Parser.parse("{{../name}}", arena);
    const path = node.program.body[0].mustache.expression.expression.path.path;
    try std.testing.expectEqual(1, path.depth);
    try std.testing.expectEqual(1, path.parts.len);
    try std.testing.expectEqualStrings("name", path.parts[0]);
    try std.testing.expect(path.scoped);
}

test "parse data reference" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const node = try Parser.parse("{{@root.name}}", arena);
    const path = node.program.body[0].mustache.expression.expression.path.path;
    try std.testing.expect(path.data);
    try std.testing.expectEqual(2, path.parts.len);
    try std.testing.expectEqualStrings("root", path.parts[0]);
    try std.testing.expectEqualStrings("name", path.parts[1]);
}
