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

//! Request-owned SQL binding contract. Implementations resolve only referenced
//! names, authorize their current logical resources, and pin physical identity
//! and schema together. This is not another durable catalog or storage engine.
const std = @import("std");
const ast = @import("ast.zig");

pub const Column = struct {
    name: []const u8,
    path: []const u8,
    type: ast.ColumnType,
    element_type: ?@import("array_value.zig").ElementType = null,
    numeric_modifier: ?@import("../common/sql_builtin_type.zig").NumericModifier = null,
    nullable: bool = true,
    /// Native stored generated columns are readable but never SQL assignable.
    generated: bool = false,
    /// Native row preparation fills absent DEFAULT cells after SQL evaluation.
    defaulted: bool = false,
};

pub fn hasColumnDefault(schema: anytype, name: []const u8) bool {
    const entries = schema.column_defaults orelse return false;
    if (entries.value != .array) return false;
    for (entries.value.array.items) |entry| {
        if (entry != .object) continue;
        const column = entry.object.get("column") orelse continue;
        if (column == .string and std.mem.eql(u8, column.string, name)) return true;
    }
    return false;
}

pub const Index = struct {
    name: []const u8,
    /// Only direct ordered-tuple keys are exposed to SQL access planning.
    /// Expression and partial indexes require a separate implication proof.
    columns: []const []const u8,
};

/// Pinned declarative constraints, not similarly named access indexes. Native
/// execution independently rebinds the name to its guarded durable generation.
pub const Constraint = struct {
    name: []const u8,
    kind: enum { unique, check, foreign_key },
    deferrable: bool = false,

    fn namedConstraint(definition: anytype) bool {
        if (comptime @hasField(@TypeOf(definition), "origin")) return (definition.origin orelse .constraint) != .index;
        return true;
    }

    pub fn derive(alloc: std.mem.Allocator, schema: anytype) ![]const Constraint {
        var count: usize = 0;
        inline for (.{ "unique_constraints", "checks", "foreign_keys" }) |field| {
            if (@field(schema, field)) |values| for (values.value) |definition| {
                if (namedConstraint(definition)) count = try std.math.add(usize, count, 1);
            };
        }
        const output = try alloc.alloc(Constraint, count);
        errdefer alloc.free(output);
        var initialized: usize = 0;
        errdefer for (output[0..initialized]) |value| alloc.free(value.name);
        inline for (.{ "unique_constraints", "checks", "foreign_keys" }, .{ .unique, .check, .foreign_key }) |field, kind| {
            if (@field(schema, field)) |values| for (values.value) |definition| {
                // Native uniqueness ownership and SQL constraint identity are
                // distinct. Classify once in this immutable schema view, not
                // by scanning access indexes or mutable labels per request.
                if (!namedConstraint(definition)) continue;
                output[initialized] = .{ .name = try alloc.dupe(u8, definition.name), .kind = kind, .deferrable = if (@hasField(@TypeOf(definition), "deferrable")) definition.deferrable orelse false else false };
                initialized += 1;
            };
        }
        return output;
    }
};

pub const Table = struct {
    pub const ExternalIndexes = struct {
        /// Fresh query-definition metadata, allocated for this execution. A
        /// schema/prepared-plan cache must never retain a publication pointer.
        catalog_json: []const u8,
        object_generation: u64 = 0,
        indexes_json: []const u8,
        schema_json: []const u8 = "",
        desired: [32]u8,
    };
    pub const Scope = struct {
        database: []const u8,
        namespace: []const u8,
        name: []const u8,
        revision: u64,

        pub fn matchesQualifier(self: Scope, qualifier: []const u8) bool {
            var parts = std.mem.splitScalar(u8, qualifier, 0);
            const first = parts.next().?;
            const second = parts.next() orelse return std.mem.eql(u8, first, self.name);
            const third = parts.next() orelse return std.mem.eql(u8, first, self.namespace) and std.mem.eql(u8, second, self.name);
            return parts.next() == null and std.mem.eql(u8, first, self.database) and std.mem.eql(u8, second, self.namespace) and std.mem.eql(u8, third, self.name);
        }
    };
    id: u64,
    physical_name: []const u8,
    schema_version: u32,
    storage_mode: enum { relational, document } = .relational,
    /// External bindings are read-only and pinned by the serving cursor.
    external_base_source: ?@import("../serverless/external_source/schema_binding.zig").OwnedExternalTableBinding = null,
    external_indexes: ?ExternalIndexes = null,
    columns: []const Column,
    indexes: []const Index = &.{},
    constraints: []const Constraint = &.{},
    /// Request-owned logical authority. Never use a mutable adapter's last
    /// resolved name to validate an earlier table in a join or subquery.
    scope: ?Scope = null,

    pub fn column(self: Table, name: []const u8) !Column {
        if (std.mem.eql(u8, name, "_id")) return .{ .name = "_id", .path = "_id", .type = .string, .nullable = false };
        for (self.columns) |value| if (std.mem.eql(u8, value.name, name)) return value;
        // Point predicate planning must use the same pinned logical authority
        // as relation binding; never strip a namespace before validating it.
        const separator = std.mem.lastIndexOfScalar(u8, name, 0) orelse return error.UndefinedColumn;
        const scope = self.scope orelse return error.UndefinedColumn;
        if (!scope.matchesQualifier(name[0..separator])) return error.UndefinedColumn;
        const column_name = name[separator + 1 ..];
        if (std.mem.eql(u8, column_name, "_id")) return .{ .name = "_id", .path = "_id", .type = .string, .nullable = false };
        for (self.columns) |value| if (std.mem.eql(u8, value.name, column_name)) return value;
        return error.UndefinedColumn;
    }

    pub fn columnAliases(self: Table, alloc: std.mem.Allocator, name: []const u8) ![]const []const u8 {
        const scope = self.scope orelse return &.{};
        const aliases = try alloc.alloc([]const u8, 3);
        aliases[0] = try std.fmt.allocPrint(alloc, "{s}\x00{s}", .{ scope.name, name });
        aliases[1] = try std.fmt.allocPrint(alloc, "{s}\x00{s}\x00{s}", .{ scope.namespace, scope.name, name });
        aliases[2] = try std.fmt.allocPrint(alloc, "{s}\x00{s}\x00{s}\x00{s}", .{ scope.database, scope.namespace, scope.name, name });
        return aliases;
    }
};

pub const Action = enum { read, write, read_write, admin };
pub const Condition = struct {
    pub const Op = enum { eq, neq, lt, lte, gt, gte, is_null, is_not_null };
    /// Native relational three-valued comparison; only TRUE rows match.
    /// Equality with NULL is UNKNOWN, distinct from the explicit null tests.
    column: []const u8,
    op: Op,
    value: std.json.Value = .null,
    /// Exact statement-owned operand. `value` is its lossless compatibility
    /// encoding for tuple bounds; typed consumers must retain this identity.
    operand: ?@import("scalar.zig").Datum = null,
    pub fn datum(self: Condition) @import("scalar.zig").Datum {
        return self.operand orelse @import("scalar.zig").Datum.fromJson(self.value);
    }
};
pub const Scan = struct {
    pub const Order = struct { column: []const u8, descending: bool = false, nulls_first: bool = false };
    pub const IndexEquality = struct {
        name: []const u8,
        values: []const std.json.Value,
    };
    pub const IndexRange = struct {
        pub const Bound = struct { values: []const std.json.Value, inclusive: bool = true };
        name: []const u8,
        lower: ?Bound = null,
        upper: ?Bound = null,
        after: ?[]const u8 = null,
    };
    pub const Search = struct { expression: ast.Search, request_text: ?[]const u8 = null, limit: ?u32 = null };
    search: ?*const Search = null,
    /// A request, never proof. Providers explicitly attest the entire order.
    order: []const Order = &.{},
    /// Advisory SQL OFFSET + LIMIT for costing only. Never a scan stop bound.
    /// Absent when residual selectivity is unknown. Page size remains limit.
    row_goal: ?u64 = null,
    index_range: ?IndexRange = null,
    fields: []const []const u8,
    /// Mutation-only full document preimage; ordinary reads remain projected.
    include_document: bool = false,
    include_primary_digest: bool = false,
    /// Ordered SQL must not be satisfied by an unrelated secondary-key order.
    primary_order: bool = false,
    /// Exact physical identity, not a schema predicate. Point pages exhaust
    /// after their matching row (or absence), without a continuation probe.
    primary_key: ?[]const u8 = null,
    /// Snapshot-bound remote candidates. null means a full scan; an empty
    /// slice means no rows. Consumers retain ranking separately from physical
    /// hydration order. The lake cursor owns and validates the selection.
    physical_selection: ?@import("../serverless/query/lake_row_selection.zig").Selection = null,
    row_refs: ?[]const @import("../storage/rowsource/types.zig").RowRef = null,
    /// Require a READY schema-bound index. Unlike auto_index this must fail
    /// closed; the coordinated owner read returns one exact-span proof.
    index_equality: ?IndexEquality = null,
    conditions: []const Condition = &.{},
    after: ?[]const u8 = null,
    /// External scan upper bound, validated against the opened snapshot.
    before: ?[]const u8 = null,
    limit: u32,
};
pub const Row = struct {
    id: []const u8,
    version: u64,
    value: std.json.Value,
    index_cursor: ?[]const u8 = null,
    /// Digest of the exact primary bytes in this snapshot. Timestamps alone
    /// are not a version fence when a custom TTL field is unchanged.
    expected_content_digest: ?[32]u8 = null,
    document: ?std.json.Value = null,
    /// Authoritative native null flags aligned with value.object insertion
    /// order. Null means a legacy JSON-only backend without that distinction.
    sql_nulls: ?[]const bool = null,
    pattern_sources: ?[]const ?*@import("scalar.zig").PatternSet = null,
    /// Internal relation rows retain complete, owned Datums and share a single
    /// page-local name directory, without constructing per-row JSON objects.
    /// Legacy backend rows continue to use JSON and the null/pattern channels.
    typed_cells: ?TypedCells = null,

    pub const TypedCells = struct {
        layout: *const TypedLayout,
        values: []const Cell = &.{},
        presence: ?[]const bool = null,
        /// Shared immutable column blocks, owned by the enclosing page lease.
        columnar: ?struct { blocks: []const @import("spill.zig").ColumnarBlock, row: usize } = null,
        pub fn count(self: TypedCells) usize {
            if (self.columnar != null) return self.layout.names.len;
            return self.values.len;
        }
        pub fn frozenJson(self: TypedCells, ordinal: usize) !?@import("execution_batch.zig").FrozenJson {
            if (ordinal >= self.count()) return error.InvalidSqlBackendResponse;
            const source = self.columnar orelse return null;
            if (self.values.len != 0) return error.InvalidSqlBackendResponse;
            if (self.presence) |flags| if (!flags[ordinal]) return null;
            var first: usize = 0;
            for (source.blocks) |block| {
                if (ordinal - first < block.values.len) return block.frozenJson(source.row, ordinal - first, false);
                first += block.values.len;
            }
            return error.InvalidSqlBackendResponse;
        }
        pub fn encodedJson(self: TypedCells, ordinal: usize) !?[]const u8 {
            if (ordinal >= self.count()) return error.InvalidSqlBackendResponse;
            const source = self.columnar orelse return null;
            if (self.values.len != 0) return error.InvalidSqlBackendResponse;
            if (self.presence) |flags| if (!flags[ordinal]) return null;
            var first: usize = 0;
            for (source.blocks) |block| {
                if (ordinal - first < block.values.len) return block.encodedJson(source.row, ordinal - first, false);
                first += block.values.len;
            }
            return error.InvalidSqlBackendResponse;
        }
        pub fn at(self: TypedCells, ordinal: usize) !Cell {
            if (self.columnar) |source| {
                if (self.values.len != 0 or ordinal >= self.layout.names.len) return error.InvalidSqlBackendResponse;
                var first: usize = 0;
                for (source.blocks) |block| {
                    if (ordinal - first < block.values.len) return block.cell(source.row, ordinal - first);
                    first += block.values.len;
                }
                return error.InvalidSqlBackendResponse;
            }
            if (ordinal >= self.values.len) return error.InvalidSqlBackendResponse;
            return self.values[ordinal];
        }
    };

    pub const Cell = @import("scalar.zig").Datum;
    pub const TypedLayout = struct {
        names: []const []const u8 = &.{},
        ordinals: std.StringHashMapUnmanaged(usize) = .empty,

        /// Names borrow the immutable relation binding; entries belong to the
        /// page arena. Duplicate names must not silently overwrite an ordinal.
        pub fn init(arena: std.mem.Allocator, names: []const []const u8) !*const TypedLayout {
            const result = try arena.create(TypedLayout);
            result.* = .{ .names = names };
            for (names, 0..) |name, index| {
                const entry = try result.ordinals.getOrPut(arena, name);
                if (entry.found_existing) return error.InvalidSqlBackendResponse;
                entry.value_ptr.* = index;
            }
            return result;
        }

        /// Clone once per emitted page, not once per row. No names or directory
        /// entries may borrow a cursor that can close before the page consumer.
        pub fn clone(self: *const TypedLayout, alloc: std.mem.Allocator) !*const TypedLayout {
            const names = try alloc.alloc([]const u8, self.names.len);
            for (self.names, names) |name, *out| out.* = try alloc.dupe(u8, name);
            return init(alloc, names);
        }
    };

    /// Clone once at the page ownership boundary; consumers borrow the typed
    /// cells until that page is released. Layout and allocations must belong
    /// to the same page owner (or a longer-lived immutable binding).
    pub fn fromDatums(alloc: std.mem.Allocator, id: []const u8, layout: *const TypedLayout, values: []const Cell) !Row {
        if (layout.ordinals.count() != values.len) return error.InvalidSqlBackendResponse;
        const owned = try alloc.alloc(Cell, values.len);
        for (values, owned) |value, *out| out.* = try @import("operators.zig").cloneDatum(alloc, value);
        return .{ .id = try alloc.dupe(u8, id), .version = 0, .value = .null, .typed_cells = .{ .layout = layout, .values = owned } };
    }

    /// Decoding is explicit about the SQL/JSON null boundary. The native
    /// projection's field names are literal names, never dotted JSON paths.
    pub fn frozenJson(self: Row, name: []const u8) !?@import("execution_batch.zig").FrozenJson {
        if (std.mem.eql(u8, name, "_id")) return null;
        const cells = self.typed_cells orelse return null;
        try self.validateTyped();
        const ordinal = cells.layout.ordinals.get(name) orelse return null;
        return cells.frozenJson(ordinal);
    }
    pub fn encodedJson(self: Row, name: []const u8) !?[]const u8 {
        if (std.mem.eql(u8, name, "_id")) return null;
        const cells = self.typed_cells orelse return null;
        try self.validateTyped();
        const ordinal = cells.layout.ordinals.get(name) orelse return null;
        return cells.encodedJson(ordinal);
    }
    pub fn cell(self: Row, name: []const u8) !Cell {
        if (std.mem.eql(u8, name, "_id")) return .{ .value = .{ .string = self.id }, .sql_null = false };
        if (self.typed_cells) |cells| {
            try self.validateTyped();
            const index = cells.layout.ordinals.get(name) orelse return .{};
            if (index >= cells.count()) return error.InvalidSqlBackendResponse;
            const result = try cells.at(index);
            if (cells.presence) |present| if (!present[index] and !result.sql_null) return error.InvalidSqlBackendResponse;
            if (result.array != null and (result.sql_null or result.value != .null or result.patterns != null)) return error.InvalidSqlBackendResponse;
            if (result.sql_null and result.value != .null) return error.InvalidSqlBackendResponse;
            return result;
        }
        if (self.value != .object) return error.InvalidSqlBackendResponse;
        if (self.sql_nulls) |flags| if (flags.len != self.value.object.count()) return error.InvalidSqlBackendResponse;
        const index = self.value.object.getIndex(name) orelse return .{ .value = .null, .sql_null = true };
        const value = self.value.object.values()[index];
        const sql_null = if (self.sql_nulls) |flags| flags[index] else value == .null;
        if (sql_null and value != .null) return error.InvalidSqlBackendResponse;
        return .{ .value = value, .sql_null = sql_null, .patterns = if (self.pattern_sources) |sources| if (index < sources.len) sources[index] else return error.InvalidSqlBackendResponse else null };
    }

    fn validateTyped(self: Row) !void {
        const cells = self.typed_cells orelse return error.InvalidSqlBackendResponse;
        if (cells.count() != cells.layout.ordinals.count() or cells.count() != cells.layout.names.len or
            self.value != .null or self.sql_nulls != null or self.pattern_sources != null)
            return error.InvalidSqlBackendResponse;
        if (cells.presence) |present| if (present.len != cells.count()) return error.InvalidSqlBackendResponse;
    }

    pub fn fieldNames(self: Row) ![]const []const u8 {
        if (self.typed_cells) |cells| {
            try self.validateTyped();
            return cells.layout.names;
        }
        if (self.value != .object) return error.InvalidSqlBackendResponse;
        return self.value.object.keys();
    }

    pub fn hasField(self: Row, name: []const u8) !bool {
        if (self.typed_cells) |cells| {
            try self.validateTyped();
            const index = cells.layout.ordinals.get(name) orelse return false;
            if (index >= cells.count()) return error.InvalidSqlBackendResponse;
            return if (cells.presence) |present| present[index] else true;
        }
        if (self.value != .object) return error.InvalidSqlBackendResponse;
        return self.value.object.contains(name);
    }

    pub fn cloneOwned(self: Row, alloc: std.mem.Allocator) !Row {
        return self.cloneWithLayout(alloc, if (self.typed_cells) |cells| try cells.layout.clone(alloc) else null);
    }

    /// Payload ownership transfer with a page-shared, owned name directory.
    /// Compiled pattern objects retain their existing statement-owner lifetime.
    pub fn cloneWithLayout(self: Row, alloc: std.mem.Allocator, layout: ?*const TypedLayout) !Row {
        var result = self;
        result.id = try alloc.dupe(u8, self.id);
        if (self.index_cursor) |cursor| result.index_cursor = try alloc.dupe(u8, cursor);
        if (self.document) |document| result.document = try @import("../storage/typed_json.zig").clone(alloc, document);
        if (self.typed_cells) |cells| {
            try self.validateTyped();
            const owned_layout = layout orelse return error.InvalidSqlBackendResponse;
            if (owned_layout.names.len != cells.count()) return error.InvalidSqlBackendResponse;
            const values = try alloc.alloc(Cell, cells.count());
            for (values, cells.layout.names, owned_layout.names, 0..) |*out, old_name, new_name, ordinal| {
                const value = try cells.at(ordinal);
                if (!std.mem.eql(u8, old_name, new_name)) return error.InvalidSqlBackendResponse;
                _ = try self.cell(old_name);
                out.* = try @import("operators.zig").cloneDatum(alloc, value);
            }
            result.typed_cells = .{ .layout = owned_layout, .values = values, .presence = if (cells.presence) |present| try alloc.dupe(bool, present) else null };
        } else {
            result.value = try @import("../storage/typed_json.zig").clone(alloc, self.value);
            if (self.sql_nulls) |flags| result.sql_nulls = try alloc.dupe(bool, flags);
            if (self.pattern_sources) |sources| result.pattern_sources = try alloc.dupe(?*@import("scalar.zig").PatternSet, sources);
        }
        return result;
    }
};
pub const Page = struct {
    rows: []const Row,
    owned_arena: ?std.heap.ArenaAllocator = null,
    storage_page: ?@import("../storage/relational_read_view.zig").View.Page = null,
    /// Exclusive native continuation; null proves exhaustion. Implementations
    /// must not report a short filtered page as exhaustion without that proof.
    after: ?[]const u8 = null,

    pub fn deinit(self: Page) void {
        if (self.storage_page) |owned| {
            var page = owned;
            page.deinit();
        }
        if (self.owned_arena) |owned| {
            var arena = owned;
            arena.deinit();
        }
    }
};

/// Borrowed typed vectors plus a page-owned selection. Vectors remain valid
/// until the cursor's next pull or close; consumers retain only their results.
/// SQL names are literal, and null bitmaps distinguish SQL NULL from JSON null.
pub const ColumnPage = struct {
    batch: @import("../storage/rowsource/types.zig").ColumnBatch = .{ .snapshot = .{ .table_id = "sql-relation", .snapshot_id = "statement" }, .row_refs = &.{}, .columns = &.{} },
    /// Borrowed operator columns, preserving JSON and retained typed storage.
    /// The pointer avoids embedding mutually recursive Batch/ColumnPage values.
    native: ?struct { values: *const @import("execution_batch.zig").Batch, names: []const []const u8 } = null,
    /// Optional producer capability; call before the next pull. A successful
    /// lease owns vector payloads, independently of page/cursor allocators.
    retain_columns: ?struct {
        ptr: *anyopaque,
        retain_fn: *const fn (*anyopaque, std.mem.Allocator) anyerror!?@import("../storage/rowsource/types.zig").ColumnOwner,
    } = null,
    selection: []const usize,
    after: ?[]const u8 = null,
    pub fn validate(self: ColumnPage) !void {
        if (self.native) |native| {
            if (native.values.len() != 0 and native.names.len != native.values.width()) return error.InvalidSqlBackendResponse;
            for (self.selection) |index| if (index >= native.values.len()) return error.InvalidSqlBackendResponse;
        } else {
            try self.batch.validate();
            for (self.selection) |index| if (index >= self.batch.rowCount()) return error.InvalidSqlBackendResponse;
        }
    }
    /// Declared NUMERIC reads fixed coefficients directly. Raw callers retain
    /// the established exact string contract for detected decimal columns.
    pub fn typedCell(self: ColumnPage, alloc: std.mem.Allocator, row: usize, name: []const u8, numeric: bool) !Row.Cell {
        return self.typedCellControlled(alloc, row, name, numeric, null);
    }
    pub fn typedCellControlled(self: ColumnPage, alloc: std.mem.Allocator, row: usize, name: []const u8, numeric: bool, control: ?@import("operator_work.zig").Control) !Row.Cell {
        try @import("operator_work.zig").poll(control);
        if (row >= self.selection.len) return error.InvalidSqlBackendResponse;
        if (numeric and self.native == null) if (self.batch.findColumn(name)) |column| {
            const index = self.selection[row];
            if (index >= self.batch.rowCount() or index >= column.rowCount()) return error.InvalidSqlBackendResponse;
            if (column.values == .decimal128 and !column.nulls.isNull(index)) {
                const decimal = column.values.decimal128;
                @import("../common/decimal128.zig").validateCoefficient(decimal.values[index], decimal.precision, decimal.scale) catch return error.InvalidSqlBackendResponse;
                var context: @import("numeric_value.zig").Context = .{ .alloc = alloc };
                var number = try @import("numeric_value.zig").fromScaledInteger(&context, decimal.values[index], @intCast(decimal.scale));
                errdefer number.deinit();
                const value = try alloc.create(@import("numeric_value.zig").Value);
                value.* = number.value;
                return @import("scalar.zig").Datum.typedNumeric(value);
            }
        };
        return self.cellControlled(alloc, row, name, control);
    }
    pub fn frozenJson(self: ColumnPage, row: usize, name: []const u8) !?@import("execution_batch.zig").FrozenJson {
        if (row >= self.selection.len) return error.InvalidSqlBackendResponse;
        if (self.native) |native| {
            for (native.names, 0..) |column, ordinal| if (std.mem.eql(u8, column, name)) return native.values.frozenJson(self.selection[row], ordinal);
        }
        return null;
    }
    pub fn encodedJson(self: ColumnPage, row: usize, name: []const u8) !?[]const u8 {
        if (row >= self.selection.len) return error.InvalidSqlBackendResponse;
        if (self.native) |native| {
            for (native.names, 0..) |column, ordinal| if (std.mem.eql(u8, column, name)) return native.values.encodedJson(self.selection[row], ordinal);
        }
        return null;
    }
    pub fn cell(self: ColumnPage, alloc: std.mem.Allocator, row: usize, name: []const u8) !Row.Cell {
        return self.cellControlled(alloc, row, name, null);
    }
    pub fn cellControlled(self: ColumnPage, alloc: std.mem.Allocator, row: usize, name: []const u8, control: ?@import("operator_work.zig").Control) !Row.Cell {
        try @import("operator_work.zig").poll(control);
        if (row >= self.selection.len) return error.InvalidSqlBackendResponse;
        const index = self.selection[row];
        if (self.native) |native| {
            if (index >= native.values.len() or native.names.len != native.values.width()) return error.InvalidSqlBackendResponse;
            for (native.names, 0..) |column, ordinal| if (std.mem.eql(u8, column, name)) {
                const value = try native.values.cellControlled(alloc, index, ordinal, control);
                return value;
            };
            return .{ .value = .null, .sql_null = true };
        }
        if (index >= self.batch.rowCount()) return error.InvalidSqlBackendResponse;
        if (std.mem.eql(u8, name, "_id")) return .{ .value = .{ .string = try @import("../storage/rowsource/identity.zig").allocId(alloc, self.batch.rowRef(index)) }, .sql_null = false };
        const column = self.batch.findColumn(name) orelse return .{ .value = .null, .sql_null = true };
        if (column.nulls.isNull(index)) return .{ .value = .null, .sql_null = true };
        const value: std.json.Value = switch (column.values) {
            .encoded => |values| try values.at(index),
            .dictionary_i64 => |values| .{ .integer = values.at(index) },
            .dictionary_f64 => |values| .{ .float = values.at(index) },
            .i64 => |values| .{ .integer = values[index] },
            .f64 => |values| .{ .float = values[index] },
            .bool => |values| .{ .bool = values[index] },
            .bytes => |values| .{ .string = values[index] },
            .dictionary_bytes => |values| .{ .string = values.at(index) },
            .json => |values| try @import("controlled_json_text.zig").parseLeaky(alloc, values[index], control),
            .decimal128 => |values| .{ .string = try values.text(alloc, index) },
            .vector_f32 => return error.UnsupportedSqlExecution,
        };
        return .{ .value = value, .sql_null = false };
    }
};

/// Owned statement read view. Opening pins data, not just routing metadata.
/// Page values belong to the next() allocator; the cursor lives until close().
/// Borrowed snapshot-local costing evidence. Providers never perform I/O for
/// optimizer statistics; absent fields remain unknown. Frequencies and hashes
/// describe semantic equality keys, and are advisory rather than pruning proof.
pub const ColumnStatistics = @import("../serverless/external_source/types.zig").ColumnStatistics;
/// Mergeable, advisory snapshot statistics. Fixed binary aggregates fit ordered
/// page summaries; no sketch or frequency estimate can authorize pruning.
pub const StatisticsSketch = struct {
    pub const encoded_bytes = 224;
    rows: u64 = 0,
    non_null: u64 = 0,
    bytes: u64 = 0,
    registers: [64]u8 = @splat(0),
    bounded: bool = true,
    minimum: i64 = std.math.maxInt(i64),
    maximum: i64 = std.math.minInt(i64),
    hot: [4]ColumnStatistics.Frequency = @splat(.{ .hash = 0, .rows = 0 }),
    fn frequency(self: *@This(), hash: u64, count: u64) void {
        if (count == 0) return;
        var least: usize = 0;
        for (&self.hot, 0..) |*value, i| {
            if (value.rows != 0 and value.hash == hash) {
                value.rows +|= count;
                return;
            }
            if (value.rows < self.hot[least].rows) least = i;
        }
        // Weighted Misra-Gries cancellation keeps counts as lower bounds.
        if (self.hot[least].rows == 0) {
            self.hot[least] = .{ .hash = hash, .rows = count };
            return;
        }
        const removed = @min(count, self.hot[least].rows);
        for (&self.hot) |*value| value.rows -= removed;
        if (count > removed) self.frequency(hash, count - removed);
    }
    pub fn add(self: *@This(), value: @import("scalar.zig").Datum, width: usize) !void {
        self.rows +|= 1;
        self.bytes +|= 1;
        if (value.sql_null) return;
        self.non_null +|= 1;
        self.bytes +|= width;
        const digest = try @import("scalar.zig").semanticHashDatum(value);
        const bucket: usize = @intCast(digest & 63);
        self.registers[bucket] = @max(self.registers[bucket], @as(u8, @intCast(@min(59, @ctz(digest >> 6) + 1))));
        self.frequency(digest, 1);
        const integer: ?i64 = if (value.numeric != null) null else switch (value.value) {
            .integer => |v| v,
            .bool => |v| @intFromBool(v),
            else => null,
        };
        if (integer) |v| {
            self.minimum = @min(self.minimum, v);
            self.maximum = @max(self.maximum, v);
        } else self.bounded = false;
    }
    pub fn merge(self: *@This(), other: @This()) void {
        self.rows +|= other.rows;
        self.non_null +|= other.non_null;
        self.bytes +|= other.bytes;
        for (&self.registers, other.registers) |*r, value| r.* = @max(r.*, value);
        self.bounded = self.bounded and other.bounded;
        self.minimum = @min(self.minimum, other.minimum);
        self.maximum = @max(self.maximum, other.maximum);
        for (other.hot) |value| self.frequency(value.hash, value.rows);
    }
    pub fn estimate(self: @This(), a: std.mem.Allocator, name: []const u8) !ColumnStatistics {
        var inverse: f64 = 0;
        var empty: f64 = 0;
        for (self.registers) |rank| {
            inverse += std.math.pow(f64, 2, -@as(f64, @floatFromInt(rank)));
            if (rank == 0) empty += 1;
        }
        const raw = if (empty != 0) 64 * @log(64 / empty) else 0.709 * 64 * 64 / inverse;
        const count: u64 = @intFromFloat(@min(@as(f64, @floatFromInt(self.non_null)), @max(0, @min(@as(f64, @floatFromInt(std.math.maxInt(u64) - 2048)), @round(raw)))));
        var frequencies: std.ArrayList(ColumnStatistics.Frequency) = .empty;
        errdefer frequencies.deinit(a);
        for (self.hot) |value| if (value.rows != 0) try frequencies.append(a, value);
        return .{ .name = name, .population_rows = self.rows, .non_null_rows = self.non_null, .distinct_count = count, .average_bytes = @intCast(@min(std.math.maxInt(u32), @max(1, self.bytes / @max(1, self.rows)))), .min_integer = if (self.bounded and self.non_null != 0) self.minimum else null, .max_integer = if (self.bounded and self.non_null != 0) self.maximum else null, .frequencies = try frequencies.toOwnedSlice(a) };
    }
    pub fn encode(self: @This()) [encoded_bytes]u8 {
        var bytes: [encoded_bytes]u8 = @splat(0);
        bytes[0] = 1;
        bytes[1] = @intFromBool(self.bounded);
        std.mem.writeInt(u64, bytes[8..16], self.rows, .little);
        std.mem.writeInt(u64, bytes[16..24], self.non_null, .little);
        std.mem.writeInt(u64, bytes[24..32], self.bytes, .little);
        std.mem.writeInt(i64, bytes[32..40], self.minimum, .little);
        std.mem.writeInt(i64, bytes[40..48], self.maximum, .little);
        bytes[48..112].* = self.registers;
        for (self.hot, 0..) |value, i| {
            const offset = 112 + i * 16;
            std.mem.writeInt(u64, bytes[offset..][0..8], value.hash, .little);
            std.mem.writeInt(u64, bytes[offset + 8 ..][0..8], value.rows, .little);
        }
        return bytes;
    }
    pub fn decode(bytes: []const u8) !@This() {
        if (bytes.len != encoded_bytes or bytes[0] != 1 or bytes[1] > 1) return error.InvalidSqlBackendResponse;
        var result: @This() = .{ .rows = std.mem.readInt(u64, bytes[8..16], .little), .non_null = std.mem.readInt(u64, bytes[16..24], .little), .bytes = std.mem.readInt(u64, bytes[24..32], .little), .minimum = std.mem.readInt(i64, bytes[32..40], .little), .maximum = std.mem.readInt(i64, bytes[40..48], .little), .bounded = bytes[1] != 0, .registers = bytes[48..112].* };
        if (result.non_null > result.rows or (result.bounded and result.non_null != 0 and result.minimum > result.maximum)) return error.InvalidSqlBackendResponse;
        for (result.registers) |rank| if (rank > 59) return error.InvalidSqlBackendResponse;
        for (&result.hot, 0..) |*value, i| {
            const offset = 112 + i * 16;
            value.* = .{ .hash = std.mem.readInt(u64, bytes[offset..][0..8], .little), .rows = std.mem.readInt(u64, bytes[offset + 8 ..][0..8], .little) };
            if (value.rows > result.non_null) return error.InvalidSqlBackendResponse;
        }
        return result;
    }
};
pub const Cursor = struct {
    /// True only when this cursor preserves every requested Scan.order key.
    order_satisfied: bool = false,
    /// Snapshot-local optimizer estimates; absent means unknown, never zero.
    /// Optional estimate after the captured Scan.conditions, never an execution cap.
    column_statistics: []const ColumnStatistics = &.{},
    estimated_filtered_rows: ?u64 = null,
    estimated_rows: ?u64 = null,
    estimated_bytes: ?u64 = null,
    ptr: *anyopaque,
    next: *const fn (*anyopaque, std.mem.Allocator, u32) anyerror!Page,
    close: *const fn (*anyopaque) void,
    /// Optional native column path; consumers may fall back to next().
    next_columns: ?*const fn (*anyopaque, std.mem.Allocator, u32) anyerror!ColumnPage = null,
    /// Borrow immutable filter evidence before any pull; false declines it.
    set_dynamic_filter: ?*const fn (*anyopaque, *const @import("dynamic_filter.zig").Filter) anyerror!bool = null,
    /// Negotiate complete key spooling before building the filter. Absent
    /// means Bloom/domain evidence suffices; do not spill unused exact tuples.
    /// Planning may also inspect this capability for identity join keys.
    /// Read captured metadata only; do not start scans or fetch source objects.
    wants_exact_filter: ?*const fn (*anyopaque, []const @import("dynamic_filter.zig").Column) anyerror!bool = null,
    /// Split an unopened pinned scan into disjoint work for exact associative
    /// reducers. Providers may distribute row groups dynamically; child order
    /// is not source order. Children share the snapshot and close before their parent.
    split_scan: ?*const fn (*anyopaque, std.mem.Allocator, usize) anyerror!?[]Cursor = null,
    /// Release dynamically claimed work when a reducer completes or stops
    /// early. This does not close the cursor; its owner still calls close().
    finish_partition: ?*const fn (*anyopaque) void = null,
    /// Ordered, disjoint ranges whose concatenation preserves source order.
    split_ordered: ?*const fn (*anyopaque, std.mem.Allocator, usize) anyerror!?[]Cursor = null,
    /// Rolling ordered ranges. Consume and close every child before asking for
    /// the next bounded batch; null is end of source. Later file/footer errors
    /// are reported only when the caller reaches that batch.
    split_ordered_batch: ?*const fn (*anyopaque, std.mem.Allocator, usize) anyerror!?[]Cursor = null,
    /// Provider estimate of retained metadata per ordered child. Planning
    /// limits fan-out before cloning a large immutable inventory.
    ordered_split_bytes: usize = 0,
    /// Explicit capability for an exclusively owned scan child. The provider
    /// replaces mutable parent callbacks with this worker's independent CPU/IO
    /// receiver and closes the cursor before that receiver physically retires.
    bind_execution: ?*const fn (*anyopaque, *@import("../common/runnable_scheduler.zig").Execution) anyerror!void = null,
    /// Temporary coordinator attachment. The provider owns independent mutable
    /// receiver state, joins speculative work on release, and never retains a
    /// request execution between pulls (a named portal may outlive that call).
    coordinator_execution: ?struct {
        bind: *const fn (*anyopaque, *@import("../common/runnable_scheduler.zig").Execution) anyerror!void,
        release: *const fn (*anyopaque) void,
    } = null,
    /// Exact snapshot count; null means the retained cursor must be scanned.
    /// Providers may use metadata only after accounting for filters/deletes.
    count_rows: ?*const fn (*anyopaque) anyerror!?u64 = null,
    fn attach(self: Cursor, execution: ?*@import("../common/runnable_scheduler.zig").Execution) !bool {
        const owner = execution orelse return false;
        const capability = self.coordinator_execution orelse return false;
        try capability.bind(self.ptr, owner);
        return true;
    }
    fn detach(self: Cursor, attached: bool) void {
        if (attached) self.coordinator_execution.?.release(self.ptr);
    }
    pub fn nextFor(self: Cursor, execution: ?*@import("../common/runnable_scheduler.zig").Execution, alloc: std.mem.Allocator, limit: u32) !Page {
        const attached = try self.attach(execution);
        const result = self.next(self.ptr, alloc, limit);
        self.detach(attached);
        if (attached) execution.?.checkpoint() catch |err| {
            if (result) |page| page.deinit() else |_| {}
            return err;
        };
        return result;
    }
    pub fn columnsFor(self: Cursor, execution: ?*@import("../common/runnable_scheduler.zig").Execution, alloc: std.mem.Allocator, limit: u32) !ColumnPage {
        const attached = try self.attach(execution);
        const result = self.next_columns.?(self.ptr, alloc, limit);
        self.detach(attached);
        if (attached) try execution.?.checkpoint();
        return result;
    }
    pub fn splitFor(self: Cursor, execution: ?*@import("../common/runnable_scheduler.zig").Execution, alloc: std.mem.Allocator, maximum: usize, comptime ordered: bool) !?[]Cursor {
        return self.splitWith(execution, alloc, maximum, if (ordered) self.split_ordered.? else self.split_scan.?);
    }
    pub fn splitOrderedFor(self: Cursor, execution: ?*@import("../common/runnable_scheduler.zig").Execution, alloc: std.mem.Allocator, maximum: usize, batch: bool) !?[]Cursor {
        return self.splitWith(execution, alloc, maximum, if (batch) self.split_ordered_batch.? else self.split_ordered.?);
    }
    fn splitWith(self: Cursor, execution: ?*@import("../common/runnable_scheduler.zig").Execution, alloc: std.mem.Allocator, maximum: usize, split: *const fn (*anyopaque, std.mem.Allocator, usize) anyerror!?[]Cursor) !?[]Cursor {
        const attached = try self.attach(execution);
        const result = split(self.ptr, alloc, maximum);
        self.detach(attached);
        if (attached) execution.?.checkpoint() catch |err| {
            if (result) |cursors| {
                if (cursors) |owned| {
                    for (owned) |cursor| cursor.close(cursor.ptr);
                    alloc.free(owned);
                }
            } else |_| {}
            return err;
        };
        return result;
    }
    pub fn countFor(self: Cursor, execution: ?*@import("../common/runnable_scheduler.zig").Execution) !?u64 {
        const attached = try self.attach(execution);
        const result = self.count_rows.?(self.ptr);
        self.detach(attached);
        if (attached) try execution.?.checkpoint();
        return result;
    }
};

/// Planning-only metadata. Estimates may be stale and never establish an
/// execution snapshot, exact count, visibility proof or row-admission bound.
pub const ScanEstimate = struct {
    estimated_rows: ?u64 = null,
    estimated_filtered_rows: ?u64 = null,
    estimated_bytes: ?u64 = null,
};
pub const StatementScan = struct { table: Table, request: Scan };
/// Every physical scan instance is pinned to one coordinated visibility cut.
/// Cursor handles and their storage belong to this owner; callers invoke only
/// next(), never cursor.close(). close() releases every cursor exactly once.
pub const StatementRead = struct {
    ptr: *anyopaque,
    cursors: []const Cursor,
    close: *const fn (*anyopaque) void,
};
pub const Mutation = struct {
    /// Owned native authority envelope. The SQL engine carries but never
    /// interprets native claim keys, generation fences or commands.
    conflict_guard: ?*const anyopaque = null,
    /// Read-set fence only; never a delete, write or affected result row.
    predicate_only: bool = false,
    key: []const u8,
    expected_version: u64,
    expected_content_digest: ?[32]u8 = null,
    /// A server-authored INSERT identity constraint, rather than an observed read.
    unique_absence: bool = false,
    row: ?std.json.Value,
    json_null_fields: []const []const u8 = &.{},
    /// Owned preimage retained only for DELETE RETURNING. The observed version
    /// predicate makes it authoritative only after a successful commit.
    previous: ?*const Row = null,
};
pub const ConflictExpression = struct { json: []const u8, result_type: ast.ColumnType };
pub const ConflictTarget = struct {
    columns: []const []const u8 = &.{},
    expressions: []const ConflictExpression = &.{},
    conditions: []const Condition = &.{},
    constraint_name: ?[]const u8 = null,
};

pub const ConflictOwner = struct {
    key: ?[]const u8,
    identity: ?[]const u8,
    identities: []const []const u8 = &.{},
    guard: ?*const anyopaque,
    /// The bound schema declares no native unique arbiters. The ordinary
    /// schema-version and primary predicates protect this negative proof.
    primary_only: bool = false,
};
pub const MutationOutcome = enum {
    committed,
    committed_pending,
    committed_repair_required,
    committed_graph_metric_materialization_rejected,
};

pub const Ddl = union(enum) {
    create_table: struct {
        name: ast.Name,
        schema_json: []const u8,
        if_not_exists: bool,
        tablespace: ?[]const u8 = null,
        /// Only compiler-generated backing index names may be disambiguated.
        /// Explicit SQL names remain exact catalog declarations.
        generated_constraint_names: []const []const u8 = &.{},
    },
    drop_table: ast.DropTable,
    catalog_ddl: ast.CatalogDdl,
    policy_ddl: ast.PolicyDdl,
};
pub const DdlReceipt = struct {
    pub const State = enum { ready, pending, invalid, admission_unknown };

    database: []const u8,
    namespace: []const u8,
    table: []const u8,
    table_id: []const u8,
    schema_version: u32,
    state: State,
    diagnostic: ?[]const u8 = null,
    restore_job_id: ?[]const u8 = null,
    idempotency_key: ?[]const u8 = null,
    fk_generation_publication_id: ?[]const u8 = null,
};
/// Scratch must reclaim individual allocations; result may be an arena. DDL
/// implementations reserve durable receipts in result before admission and
/// release parsing/validation/plan memory through scratch before returning.
pub const DdlAllocators = struct { result: std.mem.Allocator, scratch: std.mem.Allocator };

pub const DdlOutcome = struct { mutation_outcome: ?MutationOutcome = .committed, receipt: ?DdlReceipt = null };

pub const ScanQuota = @import("native_scan_quota.zig").Quota;

/// Complete, authorized aggregate states for one pinned table and recipe.
/// Keys and versioned aggregate cells borrow the supplied page allocator until the next pull.
/// A provider must return null before opening if it cannot prove equivalence;
/// errors after selection abort execution rather than mixing source snapshots.
pub const GroupSelection = struct {
    spill: ?*@import("spill.zig").Manager = null,
    memory_bytes: usize = 8 * 1024 * 1024,
    row_limit: usize = std.math.maxInt(usize),
    page_limit: usize = std.math.maxInt(usize),
    conditions: []const Condition = &.{},
    order: []const Scan.Order = &.{},
    /// The caller accepts any source prefix (SQL has no ORDER BY).
    unordered_prefix: bool = false,
    /// Proven upper bound on the consumed prefix, including OFFSET.
    /// Absent when SQL still needs residual filtering or an unbounded result.
    maximum_groups: ?usize = null,
    batch_rows: u32 = 4096,
};
pub const FinalGroupRequest = struct {
    page_limit: usize,
    row_limit: usize = std.math.maxInt(usize),
    selection: GroupSelection = .{},
    /// Input batch size is independent of the requested result fragment.
    batch_rows: u32 = 4096,
    /// Complete, exactly typed grouping key for native hash/range pruning.
    point: ?[]const @import("scalar.zig").Datum = null,
    predicate: ?struct {
        ptr: *anyopaque,
        matches: *const fn (*anyopaque, std.mem.Allocator, []const @import("scalar.zig").Datum) anyerror!bool,
    } = null,
};

pub const AggregatePartialCursor = struct {
    /// Pages are borrowed from this cursor or allocated in the caller's page
    /// arena. Failed authority never publishes them; the caller unwinds its arena.
    coordinator_execution: ?struct {
        bind: *const fn (*anyopaque, *@import("../common/runnable_scheduler.zig").Execution) anyerror!void,
        release: *const fn (*anyopaque) void,
    } = null,
    /// Providers may prove a unique complete cohort before the first pull.
    /// Slots refer to the requested recipe, independent of physical order.
    final_group_proof: ?struct { count: u64, slots: []const u16 } = null,
    select_keys: ?*const fn (*anyopaque, GroupSelection) anyerror!bool = null,
    select_point: ?*const fn (*anyopaque, []const @import("scalar.zig").Datum) anyerror!void = null,
    /// Transfer the cursor's ownership into an empty result operator. False
    /// preserves the ordinary partial path; a successful transfer is streamed.
    adopt_final_groups: ?*const fn (*anyopaque, *@import("operators.zig").Grouped, FinalGroupRequest) anyerror!bool = null,
    /// Optional ownership transfer before the first pull. The provider proves
    /// one state per final recipe key, with the exact recipe slots and limits.
    /// A successful transfer leaves close responsible only for cursor resources.
    /// Upper key predicates and FILTER adjustments must be absent for adoption.
    take_final_groups: ?*const fn (*anyopaque) anyerror!?*@import("operators.zig").Grouped = null,
    ptr: *anyopaque,
    next: *const fn (*anyopaque, std.mem.Allocator, u32) anyerror!?[]const @import("operators.zig").GroupResult,
    close: *const fn (*anyopaque) void,
    pub fn nextFor(self: AggregatePartialCursor, execution: ?*@import("../common/runnable_scheduler.zig").Execution, alloc: std.mem.Allocator, maximum: u32) !?[]const @import("operators.zig").GroupResult {
        const capability = self.coordinator_execution;
        const attached = execution != null and capability != null;
        if (attached) try capability.?.bind(self.ptr, execution.?);
        const result = self.next(self.ptr, alloc, maximum);
        if (attached) capability.?.release(self.ptr);
        if (attached) try execution.?.checkpoint();
        return result;
    }
};

pub const Backend = struct {
    error_context: ?*@import("errors.zig").Context = null,
    /// Execution-local only: never store this owner in an immutable plan.
    regex_execution: ?*@import("regex_execution.zig") = null,
    /// Native scalar callbacks cannot suspend/yield or perform provider I/O.
    /// Carry an explicit context so lowering can change Backend.ptr safely.
    scalar_control: ?struct { ptr: ?*anyopaque, checkpoint: *const fn (?*anyopaque) anyerror!void } = null,
    supports_search_relations: bool = false,
    execution_io: ?std.Io = null,
    /// Independent scheduler children for pure kernels over immutable columns.
    /// This does not enable concurrent access to this backend's mutable state.
    cpu_scope: ?@import("parallel_scheduler.zig").ExecutionScope = null,
    /// Inline coordinator kernels may checkpoint without allowing child tasks.
    /// Keep serial/session-affine frontends independent of fan-out capability.
    coordinator_scope: ?@import("parallel_scheduler.zig").CoordinatorScope = null,
    /// Serial spill I/O is independent of parallel compute ownership. A
    /// scheduled single-worker adapter can retain spilling without launching
    /// children that share its mutable execution capsule.
    spill_io: ?std.Io = null,
    /// Single-worker execution handoff around private spill file I/O.
    spill_wait: ?@import("execution_wait.zig").Boundary = null,
    spill_manager: ?*@import("spill.zig").Manager = null,
    decision_provider: ?@import("../functions/decisions.zig").DecisionProvider = null,
    ptr: *anyopaque,
    vtable: *const VTable,
    /// Runtime captures a fresh owner-authorized view for each statement.
    /// Overlay entries carry durable setting identities, never raw names.
    setting_capture: ?struct {
        owner: @import("setting_catalog.zig").Owner,
        scope: @import("setting_catalog.zig").Scope,
        overlay: []const @import("setting_catalog.zig").OverlayEntry = &.{},
    } = null,
    /// Authorized, immutable settings captured for this exact statement.
    /// Providers must never populate this from pgwire-local string settings.
    settings_view: ?*const @import("setting_catalog.zig").View = null,
    /// Request-owned transport types fill only unconstrained parameters after
    /// SQL inference. They never replace explicit or schema-derived types.
    parameter_fallback_types: []const ?ast.ColumnType = &.{},
    /// Authoritative Parse-time descriptors, including primitive widths and
    /// array element identity. Unlike fallbacks these constrain SQL binding.
    parameter_descriptor_hints: []const @import("scalar.zig").Type = &.{},
    /// Shared precise parameter constraints and execution frame for this
    /// statement, including its lowered/derived/mutation child plans.
    parameter_invocation: ?*@import("parameter_binding.zig").Invocation = null,
    /// Only set when every page belongs to the same retained statement read
    /// view. Catalog revisions and per-page read_index are not such a view.
    pinned_statement_snapshot: bool = false,
    /// Validate execution policy against the complete binding before choosing
    /// a cursor or fallback. Child plans inherit the already validated policy.
    execution_binding_validator: ?struct {
        ptr: *anyopaque,
        validate: *const fn (*anyopaque, std.mem.Allocator, @import("describe.zig").BoundStatement) anyerror!void,
    } = null,
    /// mutate retains predicate-only entries in the same atomic commit.
    predicate_only_mutations: bool = false,
    /// Statement capture range proofs are retained with the subsequent
    /// mutation in one durable transaction. MERGE absence decisions may not
    /// execute through a plain autocommit batch endpoint.
    atomic_statement_read_set: bool = false,
    /// Multiple bounded statement captures share one transaction read set;
    /// each point absence proof is validated with the eventual mutation.
    coordinated_point_reads: bool = false,
    /// Exact secondary-index probes also share that read set, and cannot be
    /// selected while a staged-session overlay requires primary ordering.
    coordinated_index_reads: bool = false,
    /// Every later scan can fork the same owner-issued cut and retain its
    /// route/range proof in the mutation's atomic commit read set.
    dynamic_statement_read_set: bool = false,

    pub const VTable = struct {
        /// Native opaque identity, generated once before mutation admission.
        /// Providers without this capability require an explicit _id.
        generate_row_id: ?*const fn (*anyopaque, std.mem.Allocator) anyerror![]const u8 = null,
        resolve_conflict_owners: ?*const fn (*anyopaque, std.mem.Allocator, std.mem.Allocator, Table, ConflictTarget, []const Mutation) anyerror![]const ConflictOwner = null,
        // All returned data belongs to the supplied allocator. Scans receive
        // a short-lived page arena, not the retained statement result arena.
        resolve: *const fn (*anyopaque, std.mem.Allocator, ast.Name, Action) anyerror!Table,
        scan: *const fn (*anyopaque, std.mem.Allocator, Table, Scan) anyerror!Page,
        /// null means this provider cannot retain a statement snapshot. Never
        /// substitute a collection of independently refreshed shard pages.
        /// Allows order negotiation before rows are pulled. Ordinary backends
        /// need not open speculative scans merely to decline an ordering.
        supports_scan_order: bool = false,
        open_scan: ?*const fn (*anyopaque, std.mem.Allocator, Table, Scan) anyerror!?Cursor = null,
        /// Fresh authority/source/coverage proofs belong to the provider. The
        /// SQL engine binds a strict recipe and retains projection, HAVING,
        /// ordering, limits and exact native reducer semantics.
        aggregate_partials: ?*const fn (*anyopaque, std.mem.Allocator, Table, @import("aggregate_materialization.zig").Recipe) anyerror!?AggregatePartialCursor = null,
        open_statement: ?*const fn (*anyopaque, std.mem.Allocator, []const StatementScan) anyerror!StatementRead = null,
        /// Metadata-only planning port: never open/pull cursors, execute search,
        /// activate tracking, publish state or sample rows to obtain estimates.
        /// Return one entry per input; absent metadata stays null.
        estimate_scans: ?*const fn (*anyopaque, std.mem.Allocator, []const StatementScan) anyerror![]const ScanEstimate = null,
        // The mutation allocator is a call-scoped arena. Providers must copy
        // any data retained after return into their own durable/session owner.
        // Exactly one atomic commit, retaining all schema/row-version fences.
        // An ambiguous outcome is propagated, never replayed by SQL.
        /// First allocator owns commit-scoped wire buffers; the second is a
        /// reclaiming, budgeted backing allocator for nested temporary regions.
        /// Borrowed scratch must never escape the mutation call.
        mutate: *const fn (*anyopaque, std.mem.Allocator, std.mem.Allocator, Table, []const Mutation) anyerror!MutationOutcome,
        /// Commit the exact images returned by prepare_mutations without
        /// applying defaults/generated values a second time. Required when
        /// SQL exposes a prepared postimage through RETURNING.
        mutate_prepared: ?*const fn (*anyopaque, std.mem.Allocator, std.mem.Allocator, Table, []const Mutation) anyerror!MutationOutcome = null,
        /// Native deterministic defaults/generated/check preparation under the
        /// bound schema epoch. No writes occur. Mutation consumes these exact
        /// normalized values; SQL must never guess postimages or read them back
        /// after commit. Returned rows preserve key/version/order/deletion.
        prepare_mutations: ?*const fn (*anyopaque, std.mem.Allocator, Table, []const Mutation) anyerror![]const Mutation = null,
        checkpoint: *const fn (*anyopaque) anyerror!void,
        /// Native authority owns atomic existence checks and durable catalog
        /// publication. SQL must never emulate DDL with read-then-write.
        ddl: ?*const fn (*anyopaque, DdlAllocators, Ddl) anyerror!DdlOutcome = null,
    };
};

test "SQL row cells distinguish absent SQL NULL and JSON null and reject invalid flags" {
    const alloc = std.testing.allocator;
    var object: std.json.ObjectMap = .empty;
    defer object.deinit(alloc);
    try object.put(alloc, "j", .null);
    try object.put(alloc, "n", .null);
    try object.put(alloc, "i", .{ .integer = 7 });
    var row: Row = .{ .id = "identity", .version = 1, .value = .{ .object = object }, .sql_nulls = &.{ false, true, false } };
    try std.testing.expect(!(try row.cell("j")).sql_null);
    try std.testing.expect((try row.cell("n")).sql_null);
    try std.testing.expect((try row.cell("missing")).sql_null);
    try std.testing.expectEqualStrings("identity", (try row.cell("_id")).value.string);
    row.sql_nulls = &.{true};
    try std.testing.expectError(error.InvalidSqlBackendResponse, row.cell("j"));
    row.sql_nulls = &.{ false, true, true };
    try std.testing.expectError(error.InvalidSqlBackendResponse, row.cell("i"));
}

test "SQL typed rows own arrays without conflating JSON null or array null elements" {
    const scalar = @import("scalar.zig");
    const arrays = @import("array_value.zig");
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var dimensions = [_]arrays.Dimension{.{ .length = 2, .lower = -3 }};
    var elements = [_]scalar.Datum{ scalar.Datum.json(.{ .integer = 9007199254740993 }), .{} };
    var array = try arrays.Value.init(.int64, &dimensions, &elements, .{});
    const layout = try Row.TypedLayout.init(a, &.{ "array", "json", "null" });
    var row = try Row.fromDatums(a, "row", layout, &.{ scalar.Datum.typedArray(&array), scalar.Datum.json(.null), .{} });
    dimensions[0].lower = 1;
    elements[0].value = .{ .integer = 0 };
    const cell = try row.cell("array");
    try std.testing.expect(!cell.sql_null);
    try std.testing.expectEqual(@as(i32, -3), cell.array.?.dimensions[0].lower);
    try std.testing.expectEqual(@as(i64, 9007199254740993), cell.array.?.flatCell(0).value.integer);
    try std.testing.expect(cell.array.?.flatCell(1).sql_null);
    try std.testing.expect(!(try row.cell("json")).sql_null);
    try std.testing.expect((try row.cell("null")).sql_null);
    try std.testing.expect((try row.cell("missing")).sql_null);
    try std.testing.expectError(error.InvalidSqlBackendResponse, Row.TypedLayout.init(a, &.{ "x", "x" }));
    try std.testing.expectError(error.InvalidSqlBackendResponse, Row.fromDatums(a, "", layout, &.{}));
    row.sql_nulls = &.{ false, false, true };
    try std.testing.expectError(error.InvalidSqlBackendResponse, row.cell("array"));
    row.sql_nulls = null;
    row.typed_cells.?.values = &.{};
    try std.testing.expectError(error.InvalidSqlBackendResponse, row.cell("array"));
}

test "SQL native column pages preserve selection JSON null and validate schema width" {
    const Datum = @import("scalar.zig").Datum;
    const values = [_]Datum{ Datum.json(.{ .integer = 9007199254740993 }), .{}, Datum.json(.null) };
    const vectors = [_][]const Datum{&values};
    const batch: @import("execution_batch.zig").Batch = .{ .vectors = .{ .values = &vectors, .count = 3 } };
    var page: ColumnPage = .{ .native = .{ .values = &batch, .names = &.{"literal.name"} }, .selection = &.{ 2, 0, 1 } };
    try page.validate();
    const json_null = try page.cell(std.testing.allocator, 0, "literal.name");
    try std.testing.expect(!json_null.sql_null and json_null.value == .null);
    try std.testing.expectEqual(@as(i64, 9007199254740993), (try page.cell(std.testing.allocator, 1, "literal.name")).value.integer);
    try std.testing.expect((try page.cell(std.testing.allocator, 2, "literal.name")).sql_null);
    page.selection = &.{3};
    try std.testing.expectError(error.InvalidSqlBackendResponse, page.validate());
    page.selection = &.{0};
    page.native.?.names = &.{};
    try std.testing.expectError(error.InvalidSqlBackendResponse, page.validate());
}

test "SQL typed array column adapters preserve dimensions and owned index continuation" {
    const scalar = @import("scalar.zig");
    const arrays = @import("array_value.zig");
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const dimensions = [_]arrays.Dimension{.{ .length = 2, .lower = -3 }};
    const elements = [_]scalar.Datum{ scalar.Datum.json(.{ .integer = 9007199254740993 }), .{} };
    var array = try arrays.Value.init(.int64, &dimensions, &elements, .{});
    const cells = [_]scalar.Datum{scalar.Datum.typedArray(&array)};
    const rows = [_][]const scalar.Datum{&cells};
    const source: @import("execution_batch.zig").Batch = .{ .rows = &rows };
    const page: ColumnPage = .{ .native = .{ .values = &source, .names = &.{"a"} }, .selection = &.{0} };
    const definitions = [_]scalar.Column{.{ .name = "a", .type = .array, .element_type = .int64 }};
    const adapter: @import("execution_batch.zig").Batch = .{ .columns = .{ .page = page, .definitions = &definitions } };
    const cell = try adapter.cell(a, 0, 0);
    try std.testing.expectEqual(@as(i32, -3), cell.array.?.dimensions[0].lower);
    try std.testing.expectEqual(@as(i64, 9007199254740993), cell.array.?.flatCell(0).value.integer);
    try std.testing.expect(cell.array.?.flatCell(1).sql_null);
    var cursor = [_]u8{ 'n', 'e', 'x', 't' };
    var row = try Row.fromDatums(a, "row", try Row.TypedLayout.init(a, &.{"a"}), &cells);
    row.index_cursor = &cursor;
    const owned = try row.cloneOwned(a);
    @memset(&cursor, 'x');
    try std.testing.expectEqualStrings("next", owned.index_cursor.?);
    try std.testing.expectEqual(@as(i32, -3), (try owned.cell("a")).array.?.dimensions[0].lower);
}

test "lake SQL decimal columns distinguish declared NUMERIC from exact string reads" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const rowsource = @import("../storage/rowsource/types.zig");
    const page: ColumnPage = .{ .batch = .{ .snapshot = .{ .table_id = "t", .snapshot_id = "s" }, .row_refs = &.{.{ .relational_key = "a" }}, .columns = &.{rowsource.ColumnVector{ .name = "n", .values = .{ .decimal128 = .{ .values = &.{9007199254740993125}, .precision = 22, .scale = 3 } } }} }, .selection = &.{0} };
    const raw = try page.cell(a, 0, "n");
    try std.testing.expectEqualStrings("9007199254740993.125", raw.value.string);
    const native = try page.typedCell(a, 0, "n", true);
    try std.testing.expect(native.numeric != null);
    var context: @import("numeric_value.zig").Context = .{ .alloc = a };
    try std.testing.expectEqualStrings(raw.value.string, try @import("numeric_value.zig").format(&context, native.numeric.?.*));
}

test "SQL durable planner sketches merge duplicate keys nulls widths and bounded heavy frequencies" {
    const a = std.testing.allocator;
    var full: StatisticsSketch = .{};
    var left: StatisticsSketch = .{};
    var right: StatisticsSketch = .{};
    for (0..20000) |i| {
        const value = @import("scalar.zig").Datum.fromJson(if (i % 5 == 0) .null else .{ .integer = if (i < 10000) 7 else @intCast(i) });
        try full.add(value, 8);
        if (i < 10000) try left.add(value, 8) else try right.add(value, 8);
    }
    left.merge(right);
    try std.testing.expectEqualDeep(full.registers, left.registers);
    try std.testing.expectEqual(full.rows, left.rows);
    const roundtrip = try StatisticsSketch.decode(&left.encode());
    try std.testing.expectEqualDeep(left, roundtrip);
    const estimate = try left.estimate(a, "key");
    defer a.free(estimate.frequencies);
    try std.testing.expectEqual(@as(?u64, 20000), estimate.population_rows);
    try std.testing.expectEqual(@as(?u64, 16000), estimate.non_null_rows);
    try std.testing.expect(estimate.distinct_count.? > 5000 and estimate.distinct_count.? < 12000);
    try std.testing.expectEqual(@as(?u32, 7), estimate.average_bytes);
    try std.testing.expectEqual(@as(?i64, 7), estimate.min_integer);
    try std.testing.expectEqual(@as(?i64, 19999), estimate.max_integer);
    for (estimate.frequencies) |frequency| try std.testing.expect(frequency.rows <= 8000);
    var malformed = left.encode();
    malformed[48] = 60;
    try std.testing.expectError(error.InvalidSqlBackendResponse, StatisticsSketch.decode(&malformed));
}
