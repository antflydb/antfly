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

// Immutable, schema-independent SQL IR. Every slice/pointer belongs to Compiled.
pub const Scalar = union(enum) {
    literal: Value,
    column: []const u8,
    unary: struct { op: Unary, operand: *const Scalar },
    binary: struct { op: Binary, left: *const Scalar, right: *const Scalar },
    call: struct {
        name: []const u8,
        args: []const *const Scalar,
        star: bool = false,
        distinct: bool = false,
        filter: ?*const Scalar = null,
        window: ?Window = null,
        subquery: ?*const Select = null,
        /// Ordered inputs are the last within_group.orders.len arguments. Keeping
        /// expressions in args makes ordinary dependency, authorization and
        /// parameter visitors see every input without a second hidden tree.
        /// Earlier arguments belong to the grouped direct-argument domain.
        within_group: ?*const WithinGroup = null,
    },
    cast: struct { operand: *const Scalar, type: ColumnType, element_type: ?@import("array_value.zig").ElementType = null },
    case_when: struct { branches: []const Branch, otherwise: ?*const Scalar = null },
    in_list: struct { operand: *const Scalar, values: []const *const Scalar, negated: bool = false },

    pub const Unary = enum { positive, negative, not, is_null, is_not_null, is_true, is_not_true, is_false, is_not_false };
    pub const Binary = enum { add, subtract, multiply, divide, modulo, concat, eq, neq, lt, lte, gt, gte, @"and", @"or", is_distinct, is_not_distinct, like, ilike, json_get, json_text };
    pub const Branch = struct { condition: *const Scalar, value: *const Scalar };
    pub const Ordering = struct { descending: bool = false, nulls_first: ?bool = null };
    /// Cold metadata is indirect: ordinary scalar nodes must not retain a
    /// slice/optional discriminator for a rarely used aggregate clause.
    pub const WithinGroup = struct { orders: []const Ordering };
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

pub const Projection = struct {
    field: []const u8 = "",
    alias: ?[]const u8 = null,
    expression: ?*const Scalar = null,
    /// SELECT wildcard, expanded against the pinned visible source layout.
    /// field is empty for * or the source qualifier for qualifier.*.
    wildcard: bool = false,
    /// Transient wildcard-expansion ordinal in the caller's pinned layout.
    /// Consumed during lowering, never resolved by a user-visible column name.
    bound_column: ?usize = null,
};
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
    /// Compiler-owned INSERT VALUES source, bounded by max_insert_rows. Its
    /// additional relation nodes must not raise the budget for user SQL.
    generated_values: bool = false,
    values_arms: []const *const Select = &.{},
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
    // Internal binding-domain marker: lowered window orders must not expand
    // output aliases a second time when entering their virtual-table binder.
    order_aliases_expanded: bool = false,
    limit: ?Value = null,
    offset: ?Value = null,
    /// Compiler-only scalar-query demand, applied after validating the user's
    /// LIMIT and after the child's ordering/OFFSET/grouping semantics.
    scalar_cardinality_limit: bool = false,
    /// A compiler-owned transparent selection frame already carries the
    /// enclosing query's outer bindings; do not append them a second time.
    selection_staged: bool = false,
    /// Compiler-only ordinal forwarding; its width is not a user projection
    /// or a physical storage-field request. Retained-memory limits still apply.
    internal_projection: bool = false,
    /// Bound outer-frame columns are invocation constants, not grouping keys.
    /// In particular they survive a global aggregate's empty input.
    invocation_constants: []const []const u8 = &.{},

    pub fn capRows(self: Select, requested: usize) usize {
        return if (self.scalar_cardinality_limit) @min(requested, 2) else requested;
    }
};
pub const NamedWindow = struct { name: []const u8, window: Window };
pub const SetKind = enum { @"union", intersect, except };
pub const Cte = struct {
    pub const Materialization = enum { automatic, materialized, not_materialized };
    name: []const u8,
    columns: []const []const u8 = &.{},
    query: *const Select,
    recursive: bool = false,
    materialization: Materialization = .automatic,
};
pub const Relation = union(enum) {
    table: struct {
        name: Name,
        alias: ?[]const u8 = null,
        mutation_target: bool = false,
        mutation_document: bool = false,
        mutation_presence: bool = false,
        /// Compiler-owned RETURNING input, never a physical table scan.
        prepared_rows: bool = false,
    },
    derived: struct {
        query: *const Select,
        alias: []const u8,
        columns: []const []const u8 = &.{},
        hidden: bool = false,
        lateral: bool = false,
        /// Internal selection boundary, not a user-visible derived scope.
        preserve_scope: bool = false,
    },
    join: struct {
        kind: JoinKind,
        left: *const Relation,
        right: *const Relation,
        condition: ?*const Scalar = null,
        /// Compiler-only left-domain demand. A masked LEFT LATERAL producer
        /// is not opened or evaluated for an undemanded row.
        demand: ?*const Scalar = null,
    },
};
pub const JoinKind = enum { inner, left, right, full, cross };
pub const Insert = struct {
    alias: ?[]const u8 = null,
    conflict: ?Conflict = null,
    returning: ?[]const Projection = null,
    table: Name,
    columns: []const []const u8,
    rows: []const []const Value = &.{},
    /// Per-cell native DEFAULT markers for VALUES, including generated VALUES
    /// sources. Omitted cells are normalized with the rest of the row image.
    defaults: []const []const bool = &.{},
    source: ?*const Select = null,
    /// Original literal cells retained only when VALUES subqueries are lowered
    /// through the bounded INSERT-source path. Target binding applies the
    /// same assignment coercion as ordinary VALUES before set type inference.
    values_source_rows: []const []const Value = &.{},
    /// Aligned with rows/cells. Literal cells keep the direct binding path.
    expressions: []const []const ?*const Scalar = &.{},

    pub fn isDefault(self: Insert, row: usize, cell: usize) bool {
        return self.defaults.len != 0 and self.defaults[row][cell];
    }
};
pub const Conflict = struct {
    columns: []const []const u8,
    expressions: []const *const Scalar = &.{},
    /// Optional predicate used to infer partial unique arbiters. It is distinct
    /// from the DO UPDATE predicate, which runs only after an arbiter conflict.
    arbiter_predicate: ?*const Scalar = null,
    assignments: []const Assignment = &.{},
    predicate: ?*const Scalar = null,
    /// Hidden INSERT-source outputs for scalar assignment subqueries. Each
    /// output is captured before conflict-owner reads and aligned by input row.
    capture_count: usize = 0,
    /// A direct scalar subquery evaluated only for an owner-selected update.
    deferred_count: usize = 0,
};
pub const Assignment = struct { field: []const u8, value: Value = .null, expression: ?*const Scalar = null, use_default: bool = false, capture_ordinal: ?usize = null, capture_span: usize = 0, capture_expression: ?*const Scalar = null, deferred_scalar: bool = false };
pub const Update = struct { table: Name, alias: ?[]const u8 = null, source: ?*const Relation = null, ctes: []const Cte = &.{}, assignments: []const Assignment, predicate: ?*const Predicate = null, returning: ?[]const Projection = null };
pub const Delete = struct { table: Name, alias: ?[]const u8 = null, source: ?*const Relation = null, ctes: []const Cte = &.{}, predicate: ?*const Predicate = null, returning: ?[]const Projection = null };
pub const Merge = struct {
    table: Name,
    alias: ?[]const u8 = null,
    source: *const Relation,
    ctes: []const Cte = &.{},
    condition: *const Scalar,
    arms: []const Arm,
    returning: ?[]const Projection = null,
    pub const Arm = struct {
        matched: bool,
        predicate: ?*const Scalar = null,
        action: Action,
        pub const Action = union(enum) {
            update: []const Assignment,
            delete,
            insert: struct { columns: []const []const u8, values: []const ?*const Scalar },
            nothing,
        };
    };
};
/// Array element identity belongs to the accompanying binding descriptor;
/// an array is never represented as a JSON column or an unknown NULL type.
pub const ColumnType = enum { string, uuid, integer, number, boolean, datetime, json, array };
pub const Column = struct { name: []const u8, type: ColumnType, nullable: bool = true, default_value: ?Value = null };
pub const CreateTable = struct { table: Name, columns: []const Column, constraints: []const SchemaChange = &.{}, if_not_exists: bool = false, tablespace: ?[]const u8 = null };
pub const DropTable = struct { table: Name, if_exists: bool = false };
pub const CatalogDdl = struct {
    kind: enum { database, namespace, tablespace, table },
    action: enum { create, drop, rename, set_tablespace, alter_schema, truncate },
    name: Name,
    new_name: ?[]const u8 = null,
    tablespace: ?[]const u8 = null,
    location: ?[]const u8 = null,
    conditional: bool = false,
    schema_change: ?SchemaChange = null,
    truncate_tables: []const Name = &.{},
    restart_identity: bool = false,
    cascade: bool = false,
};
pub const SchemaChange = union(enum) {
    drop_constraint: []const u8,
    validate_constraint: []const u8,
    add_unique: struct { name: []const u8, columns: []const []const u8, primary: bool = false, deferrable: bool = false, timing: []const u8 = "immediate" },
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
pub const PolicyDdl = struct {
    action: enum { create, alter, drop, enable, disable },
    name: []const u8,
    table: Name,
    if_exists: bool = false,
    command: enum { all, select, insert, update, delete } = .all,
    roles: []const []const u8 = &.{},
    roles_specified: bool = false,
    permissive: bool = true,
    using: ?*const Scalar = null,
    using_specified: bool = false,
    with_check: ?*const Scalar = null,
    check_specified: bool = false,
};
pub const Statement = union(enum) {
    explain: struct { statement: *const Statement, format: enum { text, json } = .text, verbose: bool = false },
    select: Select,
    insert: Insert,
    update: Update,
    delete: Delete,
    merge: Merge,
    create_table: CreateTable,
    drop_table: DropTable,
    catalog_ddl: CatalogDdl,
    policy_ddl: PolicyDdl,
    begin: @import("session.zig").Begin,
    commit,
    rollback,
    savepoint: []const u8,
    rollback_to_savepoint: []const u8,
    release_savepoint: []const u8,
    set_constraints: struct { names: []const []const u8, deferred: bool },
};
