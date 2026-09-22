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

// Immutable, schema-independent SQL IR. Every slice/pointer belongs to Compiled.
pub const Scalar = union(enum) {
    literal: Value,
    column: []const u8,
    unary: struct { op: Unary, operand: *const Scalar },
    binary: struct { op: Binary, left: *const Scalar, right: *const Scalar },
    call: struct { name: []const u8, args: []const *const Scalar, star: bool = false, distinct: bool = false, filter: ?*const Scalar = null, window: ?Window = null, subquery: ?*const Select = null },
    cast: struct { operand: *const Scalar, type: ColumnType },
    case_when: struct { branches: []const Branch, otherwise: ?*const Scalar = null },
    in_list: struct { operand: *const Scalar, values: []const *const Scalar, negated: bool = false },

    pub const Unary = enum { positive, negative, not, is_null, is_not_null, is_true, is_not_true, is_false, is_not_false };
    pub const Binary = enum { add, subtract, multiply, divide, modulo, concat, eq, neq, lt, lte, gt, gte, @"and", @"or", is_distinct, is_not_distinct, like, ilike, json_get, json_text };
    pub const Branch = struct { condition: *const Scalar, value: *const Scalar };
};

pub const Name = struct {
    database: ?[]const u8 = null,
    namespace: ?[]const u8 = null,
    table: []const u8,
};

pub const Value = union(enum) {
    null,
    boolean: bool,
    integer: i64,
    number: f64,
    string: []const u8,
    /// One-based positional parameter. Binding never mutates the compiled IR.
    parameter: u32,
};

pub const Comparison = enum { eq, neq, lt, lte, gt, gte };
pub const BinaryPredicate = struct { left: *const Predicate, right: *const Predicate };
pub const Predicate = union(enum) {
    scalar: *const Scalar,
    comparison: struct { field: []const u8, op: Comparison, value: Value },
    is_null: struct { field: []const u8, negated: bool = false },
    conjunction: BinaryPredicate,
    disjunction: BinaryPredicate,
    negation: *const Predicate,
};

pub const Projection = struct { field: []const u8 = "", alias: ?[]const u8 = null, expression: ?*const Scalar = null };
pub const Order = struct {
    field: []const u8 = "",
    expression: ?*const Scalar = null,
    position: ?u32 = null,
    descending: bool = false,
    nulls_first: ?bool = null,
};
pub const Window = struct {
    reference: ?[]const u8 = null,
    copy_reference: bool = false,
    partition: []const *const Scalar = &.{},
    order: []const Order = &.{},
    frame: ?Frame = null,
    pub const Bound = union(enum) { unbounded_preceding, preceding: Value, current, following: Value, unbounded_following };
    pub const Exclusion = enum { no_others, current, group, ties };
    pub const Frame = struct { mode: enum { rows, range, groups }, start: Bound, end: Bound = .current, exclusion: Exclusion = .no_others };
};
pub const Select = struct {
    windows: []const NamedWindow = &.{},
    set_operation: ?struct { kind: SetKind, all: bool, left: *const Select, right: *const Select } = null,
    table: ?Name = null,
    source: ?*const Relation = null,
    ctes: []const Cte = &.{},
    /// Empty means all visible columns. count_all is mutually exclusive.
    columns: []const Projection = &.{},
    count_all: bool = false,
    count_alias: ?[]const u8 = null,
    predicate: ?*const Predicate = null,
    group_by: []const *const Scalar = &.{},
    having: ?*const Scalar = null,
    order_by: []const Order = &.{},
    limit: ?Value = null,
    offset: ?Value = null,
};
pub const NamedWindow = struct { name: []const u8, window: Window };
pub const SetKind = enum { @"union", intersect, except };
pub const Cte = struct { name: []const u8, columns: []const []const u8 = &.{}, query: *const Select };
pub const Relation = union(enum) {
    table: struct { name: Name, alias: ?[]const u8 = null },
    derived: struct { query: *const Select, alias: []const u8, hidden: bool = false },
    join: struct { kind: JoinKind, left: *const Relation, right: *const Relation, condition: ?*const Scalar = null },
};
pub const JoinKind = enum { inner, left, right, full, cross };
pub const Insert = struct {
    conflict: ?Conflict = null,
    returning: ?[]const Projection = null,
    table: Name,
    columns: []const []const u8,
    rows: []const []const Value = &.{},
    source: ?*const Select = null,
    /// Aligned with rows/cells. Literal cells keep the direct binding path.
    expressions: []const []const ?*const Scalar = &.{},
};
pub const Conflict = struct {
    columns: []const []const u8,
    assignments: []const Assignment = &.{},
    predicate: ?*const Scalar = null,
};
pub const Assignment = struct { field: []const u8, value: Value = .null, expression: ?*const Scalar = null };
pub const Update = struct { table: Name, assignments: []const Assignment, predicate: ?*const Predicate = null, returning: ?[]const Projection = null };
pub const Delete = struct { table: Name, predicate: ?*const Predicate = null, returning: ?[]const Projection = null };
pub const ColumnType = enum { string, integer, number, boolean, datetime, json };
pub const Column = struct { name: []const u8, type: ColumnType, nullable: bool = true, default_value: ?Value = null };
pub const CreateTable = struct { table: Name, columns: []const Column, constraints: []const SchemaChange = &.{}, if_not_exists: bool = false, tablespace: ?[]const u8 = null };
pub const DropTable = struct { table: Name, if_exists: bool = false };
pub const CatalogDdl = struct {
    kind: enum { database, namespace, tablespace, table },
    action: enum { create, drop, rename, set_tablespace, alter_schema },
    name: Name,
    new_name: ?[]const u8 = null,
    tablespace: ?[]const u8 = null,
    location: ?[]const u8 = null,
    conditional: bool = false,
    schema_change: ?SchemaChange = null,
};
pub const SchemaChange = union(enum) {
    drop_constraint: []const u8,
    validate_constraint: []const u8,
    add_unique: struct { name: []const u8, columns: []const []const u8, primary: bool = false },
    add_check: struct { name: []const u8, expression: *const Scalar },
    add_foreign_key: struct {
        name: []const u8,
        columns: []const []const u8,
        parent: []const u8,
        parent_columns: []const []const u8,
        on_delete: []const u8 = "no_action",
        on_update: []const u8 = "no_action",
        match: []const u8 = "simple",
        deferrable: bool = false,
        timing: []const u8 = "immediate",
    },
    create_index: struct { name: []const u8, keys: []const Order, include_columns: []const []const u8 = &.{}, unique: bool = false, predicate: ?*const Scalar = null },
    drop_index: []const u8,
    add_column: Column,
    drop_column: []const u8,
    set_default: struct { column: []const u8, value: Value },
    drop_default: []const u8,
};
pub const Statement = union(enum) {
    select: Select,
    insert: Insert,
    update: Update,
    delete: Delete,
    create_table: CreateTable,
    drop_table: DropTable,
    catalog_ddl: CatalogDdl,
    begin: @import("session.zig").Begin,
    commit,
    rollback,
    savepoint: []const u8,
    rollback_to_savepoint: []const u8,
    release_savepoint: []const u8,
};
