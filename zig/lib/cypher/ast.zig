// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
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

//! Catalog-free, source-spanned graph IR. All allocations belong to Compiled.
pub const Span = struct { start: usize = 0, end: usize = 0 };
pub const Direction = enum { out, in, both };
pub const PathMode = enum { relationship_simple, node_simple, repeatable };
pub const Value = union(enum) { null, boolean: bool, integer: i64, number: f64, string: []const u8 };
pub const Expr = struct {
    span: Span,
    /// Cached tree depth bounds subsequent binding/evaluation recursion,
    /// including postfix and associative expressions parsed with loops.
    depth: usize = 1,
    value: union(enum) {
        literal: Value,
        variable: []const u8,
        parameter: []const u8,
        property: struct { base: *const Expr, name: []const u8 },
        index: struct { base: *const Expr, index: *const Expr },
        slice: struct { base: *const Expr, start: ?*const Expr, end: ?*const Expr },
        unary: struct { op: Unary, operand: *const Expr },
        binary: struct { op: Binary, left: *const Expr, right: *const Expr },
        comparison: struct { operands: []const *const Expr, operators: []const Binary },
        call: Function,
        list: []const *const Expr,
        comprehension: struct { alias: []const u8, input: *const Expr, predicate: ?*const Expr = null, projection: ?*const Expr = null },
        pattern_comprehension: struct { path: Path, predicate: ?*const Expr = null, projection: *const Expr },
        list_predicate: struct { kind: enum { all, any, none, single }, alias: []const u8, input: *const Expr, predicate: *const Expr },
        reduce: struct { accumulator: []const u8, initial: *const Expr, alias: []const u8, input: *const Expr, projection: *const Expr },
        map: []const Entry,
        map_projection: struct { base: *const Expr, items: []const MapSelector },
        case_when: struct { operand: ?*const Expr, branches: []const Branch, otherwise: ?*const Expr },
        exists: []const Clause,
    },
};
pub const Unary = enum { positive, negative, not, is_null, is_not_null };
pub const Binary = enum { add, subtract, multiply, divide, modulo, power, eq, neq, lt, lte, gt, gte, @"and", @"or", xor, in, starts_with, ends_with, contains, regex };
pub const Entry = struct { name: []const u8, value: *const Expr };
pub const MapSelector = union(enum) { property: []const u8, variable: []const u8, entry: Entry, all };
pub const Function = struct {
    name: []const u8,
    args: []const *const Expr,
    distinct: bool = false,
    star: bool = false,
    /// Physical retention demand; the parser never sets this field.
    collect_limit: ?usize = null,
};
pub const Procedure = struct { name: []const u8, args: []const *const Expr, yields: []const Yield, predicate: ?*const Expr = null };
pub const Branch = struct { condition: *const Expr, value: *const Expr };
pub const Node = struct { span: Span, alias: ?[]const u8, labels: []const []const u8, properties: []const Entry };
pub const Edge = struct {
    execution_length: ?[]const u8 = null,
    span: Span,
    alias: ?[]const u8 = null,
    types: []const []const u8 = &.{},
    properties: []const Entry = &.{},
    direction: Direction,
    min_hops: u32 = 1,
    max_hops: ?u32 = 1,
    variable_length: bool = false,
};
pub const PathSummary = struct { presence: ?[]const u8 = null, length: ?[]const u8 = null, first: ?[]const u8 = null, last: ?[]const u8 = null };
pub const Path = struct { execution_summary: PathSummary = .{}, span: Span, alias: ?[]const u8, nodes: []const Node, edges: []const Edge, selector: enum { all, shortest, all_shortest } = .all, execution_reversed: bool = false, execution_prefix: bool = false };
pub const Match = struct { paths: []const Path, optional: bool = false, predicate: ?*const Expr = null, mode: PathMode = .relationship_simple };
pub const Item = struct { expression: *const Expr, alias: ?[]const u8 = null };
pub const Order = struct { expression: *const Expr, descending: bool = false };
pub const Projection = struct { items: []const Item, wildcard: bool = false, distinct: bool = false, predicate: ?*const Expr = null, order: []const Order = &.{}, skip: ?*const Expr = null, limit: ?*const Expr = null };
pub const Assignment = struct { target: *const Expr, value: *const Expr, merge: bool = false };
pub const Yield = struct { name: []const u8, alias: []const u8 };
pub const UnionArm = struct { all: bool, clauses: []const Clause };
pub const Clause = struct {
    span: Span,
    operation: union(enum) {
        match: Match,
        project: struct { value: Projection, final: bool },
        unwind: struct { expression: *const Expr, alias: []const u8 },
        call: Procedure,
        subquery: []const Clause,
        create: []const Path,
        merge: struct { path: Path, on_create: []const Assignment = &.{}, on_match: []const Assignment = &.{} },
        set: []const Assignment,
        remove: []const *const Expr,
        delete: struct { expressions: []const *const Expr, detach: bool },
    },
};
pub const Query = struct { source: []const u8 = "", clauses: []const Clause, unions: []const UnionArm = &.{}, explain: bool = false, profile: bool = false };
