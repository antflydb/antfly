// Vendored Yuku 0.20.1; upstream revision e68066ff291d2fe43c727054f03636f4a633596b.
// SPDX-License-Identifier: MIT
//
// MIT License
//
// Copyright (c) 2026 Yuku
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

const ast = @import("../../../ast.zig");
const Parser = @import("../../../parser.zig").Parser;
const Error = @import("../../../parser.zig").Error;

const literals = @import("../../literals.zig");
const core = @import("core.zig");

// 42   "foo"   true   -1
// ^^   ^^^^^   ^^^^   ^^
pub fn parseLiteralType(parser: *Parser) Error!?ast.NodeIndex {
    const token = parser.current_token;
    const start = token.span.start;

    const literal: ast.NodeIndex = switch (token.tag) {
        .true, .false => try literals.parseBooleanLiteral(parser) orelse return null,
        .string_literal => try literals.parseStringLiteral(parser) orelse return null,
        .numeric_literal,
        .hex_literal,
        .octal_literal,
        .binary_literal,
        .bigint_literal,
        => try literals.parseNumericLiteral(parser) orelse return null,
        .no_substitution_template => try literals.parseNoSubstitutionTemplate(parser, false) orelse
            return null,
        .minus, .plus => try parseSignedNumericLiteralType(parser) orelse return null,
        else => unreachable,
    };

    return try parser.tree.addNode(
        .{ .ts_literal_type = .{ .literal = literal } },
        .{ .start = start, .end = parser.tree.span(literal).end },
    );
}

fn parseSignedNumericLiteralType(parser: *Parser) Error!?ast.NodeIndex {
    const sign_token = parser.current_token;
    const next = parser.peekAhead();
    if (!next.tag.isNumericLiteral()) return null;

    try parser.advance() orelse return null;
    const arg = try literals.parseNumericLiteral(parser) orelse return null;

    return try parser.tree.addNode(
        .{ .unary_expression = .{
            .argument = arg,
            .operator = if (sign_token.tag == .minus) .negate else .positive,
        } },
        .{ .start = sign_token.span.start, .end = parser.tree.span(arg).end },
    );
}

pub fn parseTemplateLiteralType(parser: *Parser) Error!?ast.NodeIndex {
    const start = parser.current_token.span.start;
    const parts = try literals.parseTemplateParts(
        parser,
        false,
        "type interpolation",
        core.parseType,
    ) orelse return null;

    return try parser.tree.addNode(
        .{ .ts_template_literal_type = .{ .quasis = parts.quasis, .types = parts.substitutions } },
        .{ .start = start, .end = parts.end },
    );
}
