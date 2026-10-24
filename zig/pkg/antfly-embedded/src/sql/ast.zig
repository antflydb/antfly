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
    cast: struct {
        operand: *const Scalar,
        type: ColumnType,
        temporal_kind: ?@import("temporal_value.zig").Kind = null,
        element_type: ?@import("array_value.zig").ElementType = null,
        numeric_modifier: ?@import("../common/sql_builtin_type.zig").NumericModifier = null,
        /// Coercion origin controls binding diagnostics, not execution identity.
        coercion: enum { explicit, function } = .explicit,
        pub fn withOperand(self: @This(), operand: *const Scalar) @This() {
            var result = self;
            result.operand = operand;
            return result;
        }
    },
    case_when: struct { branches: []const Branch, otherwise: ?*const Scalar = null },
    in_list: struct { operand: *const Scalar, values: []const *const Scalar, negated: bool = false },

    pub const Unary = enum { positive, negative, not, is_null, is_not_null, is_true, is_not_true, is_false, is_not_false, is_unknown, is_not_unknown };
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
    /// Exact SQL numeric spelling, owned by the compiled statement. Decimal,
    /// scientific and oversized integer literals never cross floating point.
    numeric: []const u8,
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
    /// null is a bag; an empty list is DISTINCT over output slots; otherwise
    /// the expressions identify PostgreSQL DISTINCT ON's first-row groups.
    distinct: ?[]const *const Scalar = null,
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
    /// Compiler-owned row-assignment boundary. Wildcards are checked only
    /// after expansion against the pinned source layout, before any capture.
    required_output_columns: ?usize = null,
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
    /// Internal sorted prefix includes OFFSET rows for post-sort projection.
    /// The enclosing consumer applies OFFSET only after those values exist.
    selection_prefix: bool = false,
    /// Compiler-only ordinal forwarding; its width is not a user projection
    /// or a physical storage-field request. Retained-memory limits still apply.
    internal_projection: bool = false,
    /// Compiler-owned grouping/window input; output reads live above it.
    phase_inputs: bool = false,
    /// Bound outer-frame columns are invocation constants, not grouping keys.
    /// In particular they survive a global aggregate's empty input.
    invocation_constants: []const []const u8 = &.{},

    pub fn capRows(self: Select, requested: usize) usize {
        return if (self.scalar_cardinality_limit) @min(requested, 2) else requested;
    }
};
pub const NamedWindow = struct { name: []const u8, window: Window };

pub const PhaseScope = struct {
    /// Forwarded source identities; null denotes a hidden computed result.
    columns: []const ?[]const u8,
    grouped: bool,
};
pub const SetKind = enum { @"union", intersect, except };
pub const Cte = struct {
    pub const Materialization = enum { automatic, materialized, not_materialized };
    name: []const u8,
    columns: []const []const u8 = &.{},
    query: *const Select,
    recursive: bool = false,
    materialization: Materialization = .automatic,
};
pub const Search = struct { request: Value, limit: ?Value = null };
pub const Relation = union(enum) {
    /// FROM UNNEST expands typed arrays in storage order, zipping multiple
    /// arguments with SQL NULL padding. Function inputs are implicitly lateral.
    unnest: struct {
        args: []const *const Scalar,
        alias: ?[]const u8 = null,
        columns: []const []const u8 = &.{},
        ordinality: bool = false,
        qualified: bool = false,
    },
    table: struct {
        name: Name,
        search: ?*const Search = null,
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
        /// Typed aggregate/window boundary retaining its lexical source names.
        phase_scope: ?*const PhaseScope = null,
    },
    join: struct {
        kind: JoinKind,
        left: *const Relation,
        right: *const Relation,
        condition: ?*const Scalar = null,
        /// Schema-bound keys, lowered to equality predicates and a merged
        /// projection by relation binding. Never combined with an ON clause.
        keys: union(enum) { none, using: []const []const u8, natural } = .none,
        using_alias: ?[]const u8 = null,
        /// Compiler-only left-domain demand. A masked LEFT LATERAL producer
        /// is not opened or evaluated for an undemanded row.
        demand: ?*const Scalar = null,
        /// Compiler-owned row assignment: zero rows produce SQL NULL cells;
        /// a second row is an error before any output from this producer.
        single_row: bool = false,
        /// Compiler-owned NULL-aware membership, not a SQL join condition.
        membership: ?struct { probes: []const *const Scalar, correlations: usize, alias: []const u8 } = null,
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
    constraint_name: ?[]const u8 = null,
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
pub const ForeignKey = struct {
    not_valid: bool = false,
    name: []const u8,
    name_generated: bool = false,
    columns: []const []const u8,
    parent: []const u8,
    parent_columns: []const []const u8,
    on_delete: []const u8 = "no_action",
    on_update: []const u8 = "no_action",
    match: []const u8 = "simple",
    deferrable: bool = false,
    timing: []const u8 = "immediate",
};
pub const Column = struct {
    name: []const u8,
    type: ColumnType,
    /// Exact declared builtin identity, including an array's element width.
    /// Declared array dimensions are not PostgreSQL type identity and are not
    /// retained here. Durable schema admission must not infer this from rows.
    element_type: ?@import("array_value.zig").ElementType = null,
    numeric_modifier: ?@import("../common/sql_builtin_type.zig").NumericModifier = null,
    nullable: bool = true,
    default_expression: ?*const Scalar = null,
    generated_expression: ?*const Scalar = null,
    references: ?ForeignKey = null,
};
/// Native document schema declarations are deliberately separate from SQL row
/// columns: neither representation may silently discard the other's contract.
pub const DocumentTable = struct {
    default_type: ?[]const u8 = null,
    enforce_types: bool = false,
    schemas: []const struct { name: []const u8, schema_json: []const u8 },
};
pub const CreateTable = struct { table: Name, columns: []const Column, constraints: []const SchemaChange = &.{}, if_not_exists: bool = false, tablespace: ?[]const u8 = null, document: ?*const DocumentTable = null };
pub const DropTable = struct { table: Name, if_exists: bool = false };
/// Immutable SQL intent, not a catalog identity. OWNED BY names must bind to
/// stable relation/attribute IDs in the authoritative catalog transaction.
pub const SequenceOptions = struct {
    values: @import("../system_catalog/sequences.zig").Options = .{},
    data_type: ?Name = null,
    /// Quoted "integer" is a type name, not the INTEGER grammar alias.
    data_type_quoted: bool = false,
    cache: ?i64 = null,
    owned_by: union(enum) {
        unchanged,
        none,
        column: struct { table: Name, column: []const u8 },
    } = .unchanged,

    /// Only the transaction-pinned catalog resolver may supply owner IDs.
    /// Neither a name nor a column ordinal is an allocation credential.
    pub fn bind(self: SequenceOptions, resolved: struct {
        integer: ?@import("../system_catalog/sequences.zig").Integer = null,
        owner: ?@import("../system_catalog/sequences.zig").OwnedBy = null,
    }) !@import("../system_catalog/sequences.zig").Options {
        if (self.values.owned_by != .unchanged or self.values.integer != null or self.values.cache != null) return error.InvalidSequenceDefinition;
        if ((self.data_type != null) != (resolved.integer != null) or (self.data_type_quoted and self.data_type == null)) return error.InvalidSequenceDefinition;
        var options = self.values;
        options.integer = resolved.integer;
        // Value validation belongs after object lookup, not in the parser:
        // ALTER of a missing sequence must not become a CACHE-value error.
        if (self.cache) |count| options.cache = @import("std").math.cast(u64, count) orelse return error.InvalidSequenceDefinition;
        options.owned_by = switch (self.owned_by) {
            .unchanged => if (resolved.owner == null) .unchanged else return error.InvalidSequenceDefinition,
            .none => if (resolved.owner == null) .none else return error.InvalidSequenceDefinition,
            .column => .{ .column = resolved.owner orelse return error.InvalidSequenceDefinition },
        };
        if (resolved.owner) |owner| try owner.validate();
        return options;
    }
};
pub const CatalogDdl = struct {
    kind: enum { database, namespace, tablespace, table, index, view, role, sequence },
    action: enum { create, drop, rename, set_tablespace, alter_schema, truncate, role_default, comment, alter_sequence },
    name: Name,
    new_name: ?[]const u8 = null,
    tablespace: ?[]const u8 = null,
    location: ?[]const u8 = null,
    conditional: bool = false,
    schema_change: ?SchemaChange = null,
    truncate_tables: []const Name = &.{},
    restart_identity: bool = false,
    cascade: bool = false,
    /// Index names are namespace relations, not names of their owning table.
    index_table: ?Name = null,
    index_targets: []const Name = &.{},
    concurrently: bool = false,
    view_definition: ?ViewDefinition = null,
    role_setting: ?struct { name: []const u8, database: ?[]const u8, value: ?Value } = null,
    comment_value: ?[]const u8 = null,
    comment_column: ?[]const u8 = null,
    sequence_options: ?SequenceOptions = null,
    sequence_targets: []const Name = &.{},
};
/// Compiler-owned view intent. The parsed query is reused for semantic
/// preparation; retained text is for durable definition/rebinding, not execution.
pub const ViewDefinition = struct {
    query: *const Select,
    query_sql: []const u8,
    columns: []const []const u8 = &.{},
    replace: bool = false,
    security_invoker: bool = false,
    security_barrier: bool = false,
};
pub const SchemaChange = union(enum) {
    drop_constraint: []const u8,
    rename_constraint: struct { name: []const u8, new_name: []const u8 },
    validate_constraint: []const u8,
    add_unique: struct { name: []const u8, columns: []const []const u8, include_columns: []const []const u8 = &.{}, name_generated: bool = false, primary: bool = false, deferrable: bool = false, timing: []const u8 = "immediate", nulls_not_distinct: bool = false },
    add_check: struct { name: []const u8, expression: *const Scalar, name_generated: bool = false, not_valid: bool = false },
    add_foreign_key: ForeignKey,
    create_index: struct { name: []const u8, keys: []const Order, include_columns: []const []const u8 = &.{}, unique: bool = false, predicate: ?*const Scalar = null, nulls_not_distinct: bool = false },
    drop_index: []const u8,
    add_column: Column,
    drop_column: []const u8,
    rename_column: struct { name: []const u8, new_name: []const u8 },
    set_default: struct { column: []const u8, expression: *const Scalar },
    drop_default: []const u8,
    set_nullability: struct { column: []const u8, nullable: bool },
    set_type: struct { column: Column, using: *const Scalar },

    /// Provenance is internal compiler data, never inferred from the spelling
    /// of an identifier or persisted as an editable public-schema attribute.
    pub fn generatedConstraintName(self: SchemaChange) ?[]const u8 {
        return switch (self) {
            .add_unique => |rule| if (rule.name_generated) rule.name else null,
            .add_check => |rule| if (rule.name_generated) rule.name else null,
            .add_foreign_key => |rule| if (rule.name_generated) rule.name else null,
            .add_column => |column| if (column.references) |rule| (if (rule.name_generated) rule.name else null) else null,
            else => null,
        };
    }
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
pub const CopyOptions = struct {
    format: enum { text, csv, binary } = .text,
    delimiter: ?u8 = null,
    null_string: ?[]const u8 = null,
    quote: ?u8 = null,
    escape: ?u8 = null,
    header: bool = false,
    force_quote_all: bool = false,
    force_quote_columns: []const []const u8 = &.{},

    /// Leaky clone into the caller's description/portal arena. Plan-cache
    /// leases need not outlive Parse/Describe results.
    pub fn clone(self: @This(), allocator: @import("std").mem.Allocator) !@This() {
        var result = self;
        if (self.null_string) |text| result.null_string = try allocator.dupe(u8, text);
        const columns = try allocator.alloc([]const u8, self.force_quote_columns.len);
        for (columns, self.force_quote_columns) |*out, column| out.* = try allocator.dupe(u8, column);
        result.force_quote_columns = columns;
        return result;
    }

    pub fn delimiterByte(self: @This()) u8 {
        return self.delimiter orelse if (self.format == .csv) ',' else '\t';
    }
    pub fn nullBytes(self: @This()) []const u8 {
        return self.null_string orelse if (self.format == .csv) "" else "\\N";
    }
    pub fn quoteByte(self: @This()) u8 {
        return self.quote orelse '"';
    }
    pub fn escapeByte(self: @This()) u8 {
        return self.escape orelse self.quoteByte();
    }
    pub fn validate(self: @This()) !void {
        const std = @import("std");
        if (self.format == .binary and (self.delimiter != null or self.null_string != null or self.header or self.quote != null or self.escape != null or self.force_quote_all or self.force_quote_columns.len != 0)) return error.SqlInvalidParameterValue;
        if (self.format != .csv and (self.quote != null or self.escape != null or self.force_quote_all or self.force_quote_columns.len != 0)) return error.SqlInvalidParameterValue;
        const delimiter = self.delimiterByte();
        if (delimiter == 0 or delimiter >= 128 or delimiter == '\n' or delimiter == '\r') return error.SqlInvalidParameterValue;
        if (self.format == .text and (delimiter == '\\' or delimiter == '.' or std.ascii.isDigit(delimiter) or std.ascii.isLower(delimiter))) return error.SqlInvalidParameterValue;
        if (self.quoteByte() == 0 or self.quoteByte() >= 128 or self.escapeByte() == 0 or self.escapeByte() >= 128 or (self.format == .csv and delimiter == self.quoteByte())) return error.SqlInvalidParameterValue;
        const nulls = self.nullBytes();
        if (nulls.len > 1024 or std.mem.indexOfAny(u8, nulls, "\x00\n\r") != null or !std.unicode.utf8ValidateSlice(nulls)) return error.SqlInvalidParameterValue;
        if (std.mem.indexOfScalar(u8, nulls, delimiter) != null) return error.SqlInvalidParameterValue;
        if (self.format == .csv and std.mem.indexOfScalar(u8, nulls, self.quoteByte()) != null) return error.SqlInvalidParameterValue;
    }
};

pub const Begin = struct {
    isolation: @import("session.zig").Isolation = .serializable,
    mode: @import("session.zig").ReadMode = .read_write,
    isolation_specified: bool = false,
    mode_specified: bool = false,
    deferrable: ?bool = null,
    syntax: enum { begin, start_transaction } = .begin,

    pub fn options(self: Begin) @import("session.zig").Begin {
        return .{ .isolation = self.isolation, .mode = self.mode, .deferrable = self.deferrable orelse false };
    }

    pub fn commandTag(self: Begin) []const u8 {
        return switch (self.syntax) {
            .begin => "BEGIN",
            .start_transaction => "START TRANSACTION",
        };
    }
};

pub const Statement = union(enum) {
    /// Read plan plus output contract. Ordinary execute must never silently
    /// expose this as SELECT; the COPY-capable stream owns protocol framing.
    copy_to: struct { query: *const Select, options: CopyOptions, implicit_columns: bool = false },
    explain: struct { statement: *const Statement, format: enum { text, json } = .text, verbose: bool = false, analyze: bool = false, costs: bool = false },
    select: Select,
    insert: Insert,
    update: Update,
    delete: Delete,
    merge: Merge,
    create_table: CreateTable,
    drop_table: DropTable,
    catalog_ddl: CatalogDdl,
    policy_ddl: PolicyDdl,
    begin: Begin,
    commit,
    rollback,
    savepoint: []const u8,
    rollback_to_savepoint: []const u8,
    release_savepoint: []const u8,
    set_constraints: struct { names: []const []const u8, deferred: bool },
    set_transaction: @import("session.zig").Characteristics,
};

/// Only target postimages need a reserved write stamp. DELETE retains its
/// captured preimage; SELECT and literal strings never start a transaction.
pub fn returningWriteVersion(statement: Statement) bool {
    const projections = switch (statement) {
        .insert => |v| v.returning,
        .update => |v| v.returning,
        .merge => |v| v.returning,
        else => return false,
    } orelse return false;
    for (projections) |projection| {
        if (projection.expression) |expression| {
            if (versionExpression(expression)) return true;
        } else if (!projection.wildcard and versionName(projection.field)) return true;
    }
    return false;
}

fn versionName(name: []const u8) bool {
    const std = @import("std");
    return std.mem.eql(u8, name, "_version") or std.mem.endsWith(u8, name, "\x00_version");
}

fn versionExpression(expression: *const Scalar) bool {
    return switch (expression.*) {
        .literal => false,
        .column => |name| versionName(name),
        .cast => |v| versionExpression(v.operand),
        .unary => |v| versionExpression(v.operand),
        .binary => |v| versionExpression(v.left) or versionExpression(v.right),
        .call => |v| blk: {
            for (v.args) |arg| if (versionExpression(arg)) break :blk true;
            if (v.filter) |filter| if (versionExpression(filter)) break :blk true;
            // Scalar subqueries own captured read versions, not target images.
            break :blk false;
        },
        .case_when => |v| blk: {
            for (v.branches) |branch| if (versionExpression(branch.condition) or versionExpression(branch.value)) break :blk true;
            break :blk if (v.otherwise) |other| versionExpression(other) else false;
        },
        .in_list => |v| blk: {
            if (versionExpression(v.operand)) break :blk true;
            for (v.values) |item| if (versionExpression(item)) break :blk true;
            break :blk false;
        },
    };
}
