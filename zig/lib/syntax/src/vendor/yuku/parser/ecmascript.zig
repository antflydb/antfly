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
const ast = @import("ast.zig");

pub const PropName = struct {
    name: []const u8,
    span: ast.Span,
    is_string_literal: bool,

    pub fn eql(self: PropName, expected: []const u8) bool {
        return std.mem.eql(u8, self.name, expected);
    }
};

/// https://tc39.es/ecma262/#sec-static-semantics-propname
pub fn propName(tree: *const ast.Tree, key: ast.NodeIndex) ?PropName {
    switch (tree.data(key)) {
        .identifier_name => |id| return .{
            .name = tree.string(id.name),
            .span = tree.span(key),
            .is_string_literal = false,
        },
        .string_literal => |str| {
            const name = tree.string(str.value);
            if (name.len == 0) return null;
            return .{
                .name = name,
                .span = tree.span(key),
                .is_string_literal = true,
            };
        },
        else => return null,
    }
}

/// https://tc39.es/ecma262/#sec-static-semantics-containsexpression
///
/// The first default initializer or computed key in a parameter list.
pub fn findParameterExpression(tree: *const ast.Tree, params: ast.FormalParameters) ?ast.NodeIndex {
    for (tree.extra(params.items)) |param_idx| {
        const pattern = switch (tree.data(param_idx)) {
            .formal_parameter => |fp| fp.pattern,
            .ts_parameter_property => |pp| pp.parameter,
            else => param_idx,
        };
        if (findPatternExpression(tree, pattern)) |found| return found;
    }
    if (params.rest != .null) return findPatternExpression(tree, params.rest);
    return null;
}

/// https://tc39.es/ecma262/#sec-static-semantics-containsexpression
///
/// The first expression contained in one binding pattern. Safe to call with `.null`.
pub fn findPatternExpression(tree: *const ast.Tree, pattern: ast.NodeIndex) ?ast.NodeIndex {
    if (pattern == .null) return null;
    switch (tree.data(pattern)) {
        .assignment_pattern => return pattern,
        .binding_rest_element => |rest| return findPatternExpression(tree, rest.argument),
        .array_pattern => |arr| {
            for (tree.extra(arr.elements)) |element| {
                if (findPatternExpression(tree, element)) |found| return found;
            }
            return findPatternExpression(tree, arr.rest);
        },
        .object_pattern => |obj| {
            for (tree.extra(obj.properties)) |prop_idx| {
                switch (tree.data(prop_idx)) {
                    .binding_property => |prop| {
                        if (prop.computed) return prop.key;
                        if (findPatternExpression(tree, prop.value)) |found| return found;
                    },
                    else => {},
                }
            }
            // a BindingRestProperty is always `...BindingIdentifier`
            return null;
        },
        else => return null,
    }
}

/// The expression inside any number of parentheses.
pub fn unwrapParens(tree: *const ast.Tree, node: ast.NodeIndex) ast.NodeIndex {
    std.debug.assert(node != .null);
    var current = node;
    // at most one paren node per syntactic level, so nesting depth bounds the chain
    var depth: u32 = 0;
    while (true) : (depth += 1) {
        std.debug.assert(depth < 1_000_000);
        switch (tree.data(current)) {
            .parenthesized_expression => |p| current = p.expression,
            else => return current,
        }
    }
}

/// https://tc39.es/ecma262/#sec-initializers-in-forin-statement-heads
pub fn isAnnexBForInHead(
    tree: *const ast.Tree,
    kind: ast.VariableKind,
    declarator_id: ast.NodeIndex,
    is_for_in: bool,
) bool {
    if (!is_for_in) return false;
    if (kind != .@"var") return false;
    if (tree.isTs()) return false;
    if (tree.isModule()) return false;
    return tree.data(declarator_id) == .binding_identifier;
}

/// https://tc39.es/ecma262/#sec-static-semantics-issimpleparameterlist
pub fn findNonSimpleParameter(tree: *const ast.Tree, params: ast.FormalParameters) ?ast.NodeIndex {
    if (params.rest != .null) return params.rest;

    for (tree.extra(params.items)) |param_idx| {
        const pattern = switch (tree.data(param_idx)) {
            .formal_parameter => |fp| fp.pattern,
            .ts_parameter_property => |pp| pp.parameter,
            else => return param_idx,
        };
        switch (tree.data(pattern)) {
            // `this: T` is erased at emit, so it keeps the list simple
            .binding_identifier, .ts_this_parameter => {},
            else => return pattern,
        }
    }

    return null;
}
