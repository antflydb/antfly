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
const ast = @import("../ast.zig");
const Parser = @import("../parser.zig").Parser;
const Error = @import("../parser.zig").Error;

const expressions = @import("expressions.zig");

/// Parses a possibly empty run of `@expression` decorators.
pub fn parseDecorators(parser: *Parser) Error!?ast.IndexRange {
    return parseDecoratorsAfter(parser, ast.IndexRange.empty);
}

/// Parses a possibly empty run of decorators continuing `leading`, as one list.
pub fn parseDecoratorsAfter(parser: *Parser, leading: ast.IndexRange) Error!?ast.IndexRange {
    const checkpoint = parser.scratch_decorators.begin();
    defer parser.scratch_decorators.reset(checkpoint);

    for (parser.tree.extra(leading)) |decorator| {
        try parser.scratch_decorators.append(parser.allocator(), decorator);
    }

    while (parser.current_token.tag == .at) {
        const decorator = try parseDecorator(parser) orelse return null;
        try parser.scratch_decorators.append(parser.allocator(), decorator);
    }

    return try parser.flushToExtras(&parser.scratch_decorators, checkpoint);
}

fn parseDecorator(parser: *Parser) Error!?ast.NodeIndex {
    std.debug.assert(parser.current_token.tag == .at);
    const start = parser.current_token.span.start;
    try parser.advance() orelse return null; // consume '@'

    const expression = try expressions.parseLeftHandSideExpression(parser, .decorator) orelse
        return null;

    return try parser.tree.addNode(.{
        .decorator = .{ .expression = expression },
    }, .{ .start = start, .end = parser.tree.span(expression).end });
}
