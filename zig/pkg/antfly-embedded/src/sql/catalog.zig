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
    nullable: bool = true,
    /// Native stored generated columns are readable but never SQL assignable.
    generated: bool = false,
};

pub const Index = struct {
    name: []const u8,
    /// Only direct ordered-tuple keys are exposed to SQL access planning.
    /// Expression and partial indexes require a separate implication proof.
    columns: []const []const u8,
};

pub const Table = struct {
    pub const ExternalIndexes = struct {
        /// Fresh query-definition metadata, allocated for this execution. A
        /// schema/prepared-plan cache must never retain a publication pointer.
        catalog_json: []const u8,
        indexes_json: []const u8,
        schema_json: []const u8 = "",
        desired: [32]u8,
    };
    pub const Scope = struct {
        database: []const u8,
        namespace: []const u8,
        name: []const u8,
        revision: u64,
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
    /// Request-owned logical authority. Never use a mutable adapter's last
    /// resolved name to validate an earlier table in a join or subquery.
    scope: ?Scope = null,

    pub fn column(self: Table, name: []const u8) !Column {
        if (std.mem.eql(u8, name, "_id")) return .{ .name = "_id", .path = "_id", .type = .string, .nullable = false };
        for (self.columns) |value| if (std.mem.eql(u8, value.name, name)) return value;
        return error.UndefinedColumn;
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

    pub const Cell = struct { value: std.json.Value, sql_null: bool, patterns: ?*@import("scalar.zig").PatternSet = null };

    /// Decoding is explicit about the SQL/JSON null boundary. The native
    /// projection's field names are literal names, never dotted JSON paths.
    pub fn cell(self: Row, name: []const u8) !Cell {
        if (std.mem.eql(u8, name, "_id")) return .{ .value = .{ .string = self.id }, .sql_null = false };
        if (self.value != .object) return error.InvalidSqlBackendResponse;
        if (self.sql_nulls) |flags| if (flags.len != self.value.object.count()) return error.InvalidSqlBackendResponse;
        const index = self.value.object.getIndex(name) orelse return .{ .value = .null, .sql_null = true };
        const value = self.value.object.values()[index];
        const sql_null = if (self.sql_nulls) |flags| flags[index] else value == .null;
        if (sql_null and value != .null) return error.InvalidSqlBackendResponse;
        return .{ .value = value, .sql_null = sql_null, .patterns = if (self.pattern_sources) |sources| if (index < sources.len) sources[index] else return error.InvalidSqlBackendResponse else null };
    }
};
pub const Page = struct {
    rows: []const Row,
    owned_arena: ?std.heap.ArenaAllocator = null,
    /// Exclusive native continuation; null proves exhaustion. Implementations
    /// must not report a short filtered page as exhaustion without that proof.
    after: ?[]const u8 = null,

    pub fn deinit(self: Page) void {
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
    pub fn cell(self: ColumnPage, alloc: std.mem.Allocator, row: usize, name: []const u8) !Row.Cell {
        if (row >= self.selection.len) return error.InvalidSqlBackendResponse;
        const index = self.selection[row];
        if (self.native) |native| {
            if (index >= native.values.len() or native.names.len != native.values.width()) return error.InvalidSqlBackendResponse;
            for (native.names, 0..) |column, ordinal| if (std.mem.eql(u8, column, name)) {
                const value = try native.values.cell(alloc, index, ordinal);
                return .{ .value = value.value, .sql_null = value.sql_null, .patterns = value.patterns };
            };
            return .{ .value = .null, .sql_null = true };
        }
        if (index >= self.batch.rowCount()) return error.InvalidSqlBackendResponse;
        if (std.mem.eql(u8, name, "_id")) return .{ .value = .{ .string = try @import("../storage/rowsource/identity.zig").allocId(alloc, self.batch.row_refs[index]) }, .sql_null = false };
        const column = self.batch.findColumn(name) orelse return .{ .value = .null, .sql_null = true };
        if (column.nulls.isNull(index)) return .{ .value = .null, .sql_null = true };
        const value: std.json.Value = switch (column.values) {
            .dictionary_i64 => |values| .{ .integer = values.at(index) },
            .dictionary_f64 => |values| .{ .float = values.at(index) },
            .i64 => |values| .{ .integer = values[index] },
            .f64 => |values| .{ .float = values[index] },
            .bool => |values| .{ .bool = values[index] },
            .bytes => |values| .{ .string = values[index] },
            .dictionary_bytes => |values| .{ .string = values.at(index) },
            .json => |values| try std.json.parseFromSliceLeaky(std.json.Value, alloc, values[index], .{ .allocate = .alloc_always, .parse_numbers = false }),
            .vector_f32 => return error.UnsupportedSqlExecution,
        };
        return .{ .value = value, .sql_null = false };
    }
};

/// Owned statement read view. Opening pins data, not just routing metadata.
/// Page values belong to the next() allocator; the cursor lives until close().
pub const Cursor = struct {
    /// True only when this cursor preserves every requested Scan.order key.
    order_satisfied: bool = false,
    /// Snapshot-local optimizer estimates; absent means unknown, never zero.
    estimated_rows: ?u64 = null,
    estimated_bytes: ?u64 = null,
    ptr: *anyopaque,
    next: *const fn (*anyopaque, std.mem.Allocator, u32) anyerror!Page,
    close: *const fn (*anyopaque) void,
    /// Optional native column path; consumers may fall back to next().
    next_columns: ?*const fn (*anyopaque, std.mem.Allocator, u32) anyerror!ColumnPage = null,
    /// Borrow immutable filter evidence before any pull; false declines it.
    set_dynamic_filter: ?*const fn (*anyopaque, *const @import("dynamic_filter.zig").Filter) anyerror!bool = null,
    /// Split an unopened pinned scan into disjoint work for exact associative
    /// reducers. Providers may distribute row groups dynamically; child order
    /// is not source order. Children share the snapshot and close before their parent.
    split_scan: ?*const fn (*anyopaque, std.mem.Allocator, usize) anyerror!?[]Cursor = null,
    /// Ordered, disjoint ranges whose concatenation preserves source order.
    split_ordered: ?*const fn (*anyopaque, std.mem.Allocator, usize) anyerror!?[]Cursor = null,
    /// Provider estimate of retained metadata per ordered child. Planning
    /// limits fan-out before cloning a large immutable inventory.
    ordered_split_bytes: usize = 0,
    /// Exact snapshot count; null means the retained cursor must be scanned.
    /// Providers may use metadata only after accounting for filters/deletes.
    count_rows: ?*const fn (*anyopaque) anyerror!?u64 = null,
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
    create_table: struct { name: ast.Name, schema_json: []const u8, if_not_exists: bool, tablespace: ?[]const u8 = null },
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
pub const DdlOutcome = struct { mutation_outcome: ?MutationOutcome = .committed, receipt: ?DdlReceipt = null };

/// Complete, authorized aggregate states for one pinned table and recipe.
/// Keys and AGS1 cells borrow the supplied page allocator until the next pull.
/// A provider must return null before opening if it cannot prove equivalence;
/// errors after selection abort execution rather than mixing source snapshots.
pub const AggregatePartialCursor = struct {
    ptr: *anyopaque,
    next: *const fn (*anyopaque, std.mem.Allocator, u32) anyerror!?[]const @import("operators.zig").GroupResult,
    close: *const fn (*anyopaque) void,
};

pub const Backend = struct {
    supports_search_relations: bool = false,
    execution_io: ?std.Io = null,
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
    /// Only set when every page belongs to the same retained statement read
    /// view. Catalog revisions and per-page read_index are not such a view.
    pinned_statement_snapshot: bool = false,
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
        resolve_conflict_owners: ?*const fn (*anyopaque, std.mem.Allocator, Table, []const []const u8, []const ConflictExpression, []const Condition, []const Mutation) anyerror![]const ConflictOwner = null,
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
        // The mutation allocator is a call-scoped arena. Providers must copy
        // any data retained after return into their own durable/session owner.
        // Exactly one atomic commit, retaining all schema/row-version fences.
        // An ambiguous outcome is propagated, never replayed by SQL.
        mutate: *const fn (*anyopaque, std.mem.Allocator, Table, []const Mutation) anyerror!MutationOutcome,
        /// Commit the exact images returned by prepare_mutations without
        /// applying defaults/generated values a second time. Required when
        /// SQL exposes a prepared postimage through RETURNING.
        mutate_prepared: ?*const fn (*anyopaque, std.mem.Allocator, Table, []const Mutation) anyerror!MutationOutcome = null,
        /// Native deterministic defaults/generated/check preparation under the
        /// bound schema epoch. No writes occur. Mutation consumes these exact
        /// normalized values; SQL must never guess postimages or read them back
        /// after commit. Returned rows preserve key/version/order/deletion.
        prepare_mutations: ?*const fn (*anyopaque, std.mem.Allocator, Table, []const Mutation) anyerror![]const Mutation = null,
        checkpoint: *const fn (*anyopaque) anyerror!void,
        /// Native authority owns atomic existence checks and durable catalog
        /// publication. SQL must never emulate DDL with read-then-write.
        ddl: ?*const fn (*anyopaque, std.mem.Allocator, Ddl) anyerror!DdlOutcome = null,
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
