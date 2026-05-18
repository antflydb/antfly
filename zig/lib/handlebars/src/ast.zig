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

pub const Loc = struct {
    line: u32 = 0,
    col: u32 = 0,
};

pub const Strip = struct {
    open: bool = false, // {{~ — strip whitespace before
    close: bool = false, // ~}} — strip whitespace after
};

pub const Node = union(Tag) {
    program: Program,
    mustache: Mustache,
    block: Block,
    partial: Partial,
    partial_block: PartialBlock,
    inline_partial: InlinePartial,
    content: Content,
    comment: Comment,
    expression: Expression,
    sub_expression: SubExpression,
    path: Path,
    string_literal: StringLiteral,
    boolean_literal: BooleanLiteral,
    number_literal: NumberLiteral,
    hash: Hash,
    hash_pair: HashPair,

    pub const Tag = enum {
        program,
        mustache,
        block,
        partial,
        partial_block,
        inline_partial,
        content,
        comment,
        expression,
        sub_expression,
        path,
        string_literal,
        boolean_literal,
        number_literal,
        hash,
        hash_pair,
    };
};

pub const Program = struct {
    body: []const *Node,
    block_params: []const []const u8 = &.{},
    loc: Loc = .{},
};

pub const Mustache = struct {
    expression: *Node, // Expression
    unescaped: bool = false,
    strip: Strip = .{},
    loc: Loc = .{},
};

pub const Block = struct {
    expression: *Node, // Expression
    program: *Node, // Program (main block body)
    inverse: ?*Node = null, // Program (else block body)
    open_strip: Strip = .{}, // {{#block~ or {{~#block
    close_strip: Strip = .{}, // {{/block~ or {{~/block
    inverse_strip: Strip = .{}, // {{~else~ or {{else~
    loc: Loc = .{},
};

pub const Partial = struct {
    name: *Node, // Path or SubExpression
    params: []const *Node = &.{},
    hash: ?*Node = null,
    indent: []const u8 = "",
    strip: Strip = .{},
    loc: Loc = .{},
};

/// {{#> name}}default content{{/name}} — partial block (template inheritance)
pub const PartialBlock = struct {
    name: *Node, // Path or SubExpression
    params: []const *Node = &.{},
    hash: ?*Node = null,
    program: *Node, // Program (default content, becomes @partial-block)
    open_strip: Strip = .{},
    close_strip: Strip = .{},
    loc: Loc = .{},
};

/// {{#*inline "name"}}content{{/inline}} — inline partial definition
pub const InlinePartial = struct {
    name: []const u8,
    program: *Node, // Program (partial content)
    loc: Loc = .{},
};

pub const Content = struct {
    value: []const u8,
    loc: Loc = .{},
};

pub const Comment = struct {
    value: []const u8,
    strip: Strip = .{},
    loc: Loc = .{},
};

pub const Expression = struct {
    path: *Node, // Path, StringLiteral, BooleanLiteral, NumberLiteral
    params: []const *Node = &.{},
    hash: ?*Node = null, // Hash
    loc: Loc = .{},
};

pub const SubExpression = struct {
    expression: *Node, // Expression
    loc: Loc = .{},
};

pub const Path = struct {
    original: []const u8,
    parts: []const []const u8,
    depth: u32 = 0, // number of ".." parent refs
    data: bool = false, // @data reference
    scoped: bool = false, // contains "this", ".", or ".."
    loc: Loc = .{},
};

pub const StringLiteral = struct {
    value: []const u8,
    loc: Loc = .{},
};

pub const BooleanLiteral = struct {
    value: bool,
    loc: Loc = .{},
};

pub const NumberLiteral = struct {
    value: f64,
    is_int: bool = false,
    original: []const u8,
    loc: Loc = .{},
};

pub const Hash = struct {
    pairs: []const *Node, // HashPair nodes
    loc: Loc = .{},
};

pub const HashPair = struct {
    key: []const u8,
    value: *Node,
    loc: Loc = .{},
};
