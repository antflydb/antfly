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

// Jinja2 AST node types.
//
// Covers the HuggingFace chat template subset: if/elif/else, for loops,
// set statements, expressions with operators, subscript/dot access, filters.

const std = @import("std");

pub const BinOp = enum {
    add, // +
    sub, // -
    mul, // *
    div, // /
    mod, // %
    eq, // ==
    ne, // !=
    lt, // <
    gt, // >
    le, // <=
    ge, // >=
    @"and",
    @"or",
    concat, // ~ (string concat)
    in_op, // in (membership/containment)
    not_in, // not in
};

pub const UnaryOp = enum {
    not,
    neg, // unary -
    is_defined,
    is_not_defined,
    is_string,
    is_iterable,
    is_mapping,
    is_sequence,
    is_none,
    is_not_none,
    is_boolean,
};

pub const Expr = union(enum) {
    literal: Literal,
    name: []const u8,
    get_attr: GetAttr,
    get_item: GetItem,
    slice: Slice,
    bin_op: BinOpNode,
    unary_op: UnaryOpNode,
    call: Call,
    filter: Filter,
    list: []const *Expr,
    dict: []const DictPair,
    // Jinja2 ternary: (true_expr if condition else false_expr)
    inline_if: InlineIf,
};

pub const Literal = union(enum) {
    string: []const u8,
    integer: i64,
    float: f64,
    boolean: bool,
    none,
};

pub const GetAttr = struct {
    obj: *Expr,
    attr: []const u8,
};

pub const GetItem = struct {
    obj: *Expr,
    key: *Expr,
};

pub const Slice = struct {
    obj: *Expr,
    start: ?*Expr,
    stop: ?*Expr,
};

pub const BinOpNode = struct {
    op: BinOp,
    left: *Expr,
    right: *Expr,
};

pub const UnaryOpNode = struct {
    op: UnaryOp,
    operand: *Expr,
};

pub const Call = struct {
    func: *Expr,
    args: []const *Expr,
    kwargs: []const KwArg,
};

pub const KwArg = struct {
    key: []const u8,
    value: *Expr,
};

pub const Filter = struct {
    expr: *Expr,
    name: []const u8,
    args: []const *Expr,
};

pub const DictPair = struct {
    key: *Expr,
    value: *Expr,
};

pub const InlineIf = struct {
    true_expr: *Expr,
    condition: *Expr,
    false_expr: ?*Expr,
};

// --- Statement / Template nodes ---

pub const Node = union(enum) {
    text: []const u8,
    output: Output,
    if_stmt: IfStmt,
    for_stmt: ForStmt,
    set_stmt: SetStmt,
    macro_stmt: MacroStmt,
};

pub const Output = struct {
    expr: *Expr,
    strip_left: bool = false,
    strip_right: bool = false,
};

pub const IfBranch = struct {
    condition: ?*Expr, // null for else
    body: []const *Node,
    // Strip flags from the tag that opens this branch ({%- if/elif/else -%})
    open_strip_left: bool = false,
    open_strip_right: bool = false,
    // Strip flags from the tag that closes this branch ({%- elif/else/endif -%})
    close_strip_left: bool = false,
    close_strip_right: bool = false,
};

pub const IfStmt = struct {
    branches: []const IfBranch,
};

pub const ForStmt = struct {
    var_name: []const u8,
    value_name: ?[]const u8 = null, // For tuple unpacking: {% for key, value in ... %}
    iterable: *Expr,
    body: []const *Node,
    else_body: []const *Node,
    open_strip_left: bool = false,
    open_strip_right: bool = false,
    close_strip_left: bool = false,
    close_strip_right: bool = false,
};

pub const SetStmt = struct {
    name: []const u8,
    attr: ?[]const u8 = null, // For dotted assignment: {% set name.attr = value %}
    value: *Expr,
    strip_left: bool = false,
    strip_right: bool = false,
};

pub const MacroStmt = struct {
    name: []const u8,
    params: []const MacroParam,
    body: []const *Node,
    strip_left: bool = false,
    strip_right: bool = false,
    close_strip_left: bool = false,
    close_strip_right: bool = false,
};

pub const MacroParam = struct {
    name: []const u8,
    default: ?*Expr = null,
};
