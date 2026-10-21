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

const std = @import("std");
const Parser = @import("../parser.zig").Parser;
const Error = @import("../parser.zig").Error;
const ast = @import("../ast.zig");
const Precedence = @import("../token.zig").Precedence;

const array = @import("array.zig");
const object = @import("object.zig");
const literals = @import("literals.zig");
const expressions = @import("expressions.zig");
const ts = @import("ts/types.zig");

pub inline fn parseBindingPattern(parser: *Parser) Error!?ast.NodeIndex {
    if (parser.current_token.tag.isIdentifierLike()) {
        return literals.parseBindingIdentifier(parser);
    }

    return switch (parser.current_token.tag) {
        .left_bracket => parseArrayPattern(parser),
        .left_brace => parseObjectPattern(parser),
        else => {
            try parser.report(
                parser.current_token.span,
                try parser.fmt(
                    "Unexpected token '{s}' in binding pattern",
                    .{parser.describeToken(parser.current_token)},
                ),
                .{ .help = "Expected an identifier, array pattern ([a, b])," ++
                    " or object pattern ({a, b})." },
            );
            return null;
        },
    };
}

fn parseArrayPattern(parser: *Parser) Error!?ast.NodeIndex {
    std.debug.assert(parser.current_token.tag == .left_bracket);
    const cover = try array.parseCover(parser) orelse return null;
    return try array.coverToPattern(parser, cover, .binding);
}

fn parseObjectPattern(parser: *Parser) Error!?ast.NodeIndex {
    std.debug.assert(parser.current_token.tag == .left_brace);
    const cover = try object.parseCover(parser) orelse return null;
    return try object.coverToPattern(parser, cover, .binding);
}

pub fn parseAssignmentPattern(parser: *Parser, left: ast.NodeIndex) Error!?ast.NodeIndex {
    std.debug.assert(left != .null);
    if (parser.current_token.tag != .assign) return left;

    try parser.advance() orelse return null;

    const right = try expressions.parseExpression(parser, Precedence.Assignment, .{}) orelse
        return null;

    return try parser.tree.addNode(
        .{ .assignment_pattern = .{ .left = left, .right = right } },
        .{ .start = parser.tree.span(left).start, .end = parser.tree.span(right).end },
    );
}

pub fn parseBindingRestElement(parser: *Parser) Error!?ast.NodeIndex {
    std.debug.assert(parser.current_token.tag == .spread);
    const start = parser.current_token.span.start;
    try parser.advance() orelse return null; // consume ...

    const argument = try parseBindingPattern(parser) orelse return null;
    var end = parser.tree.span(argument).end;

    var type_annotation: ast.NodeIndex = .null;
    if (parser.tree.isTs() and parser.current_token.tag == .colon) {
        type_annotation = try ts.parseTypeAnnotation(parser) orelse return null;
        end = parser.tree.span(type_annotation).end;
    }

    return try parser.tree.addNode(
        .{ .binding_rest_element = .{ .argument = argument, .type_annotation = type_annotation } },
        .{ .start = start, .end = end },
    );
}

pub fn isDestructuringPattern(parser: *Parser, index: ast.NodeIndex) bool {
    return switch (parser.tree.data(index)) {
        .array_pattern, .object_pattern => true,
        .assignment_pattern => |pattern| isDestructuringPattern(parser, pattern.left),
        else => false,
    };
}
