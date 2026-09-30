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
    pub const IndexEquality = struct {
        name: []const u8,
        values: []const std.json.Value,
    };
    fields: []const []const u8,
    /// Mutation-only full document preimage; ordinary reads remain projected.
    include_document: bool = false,
    include_primary_digest: bool = false,
    /// Ordered SQL must not be satisfied by an unrelated secondary-key order.
    primary_order: bool = false,
    /// Exact physical identity, not a schema predicate. Point pages exhaust
    /// after their matching row (or absence), without a continuation probe.
    primary_key: ?[]const u8 = null,
    /// Require a READY schema-bound index. Unlike auto_index this must fail
    /// closed; the coordinated owner read returns one exact-span proof.
    index_equality: ?IndexEquality = null,
    conditions: []const Condition = &.{},
    after: ?[]const u8 = null,
    limit: u32,
};
pub const Row = struct {
    id: []const u8,
    version: u64,
    value: std.json.Value,
    /// Digest of the exact primary bytes in this snapshot. Timestamps alone
    /// are not a version fence when a custom TTL field is unchanged.
    expected_content_digest: ?[32]u8 = null,
    document: ?std.json.Value = null,
    /// Authoritative native null flags aligned with value.object insertion
    /// order. Null means a legacy JSON-only backend without that distinction.
    sql_nulls: ?[]const bool = null,

    pub const Cell = struct { value: std.json.Value, sql_null: bool };

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
        return .{ .value = value, .sql_null = sql_null };
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

/// Owned statement read view. Opening pins data, not just routing metadata.
/// Page values belong to the next() allocator; the cursor lives until close().
pub const Cursor = struct {
    ptr: *anyopaque,
    next: *const fn (*anyopaque, std.mem.Allocator, u32) anyerror!Page,
    close: *const fn (*anyopaque) void,
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
    database: []const u8,
    namespace: []const u8,
    table: []const u8,
    table_id: []const u8,
    schema_version: u32,
    state: enum { ready, pending, invalid, admission_unknown },
    diagnostic: ?[]const u8 = null,
    restore_job_id: ?[]const u8 = null,
    idempotency_key: ?[]const u8 = null,
    fk_generation_publication_id: ?[]const u8 = null,
};
pub const DdlOutcome = struct { mutation_outcome: ?MutationOutcome = .committed, receipt: ?DdlReceipt = null };

pub const Backend = struct {
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
        open_scan: ?*const fn (*anyopaque, std.mem.Allocator, Table, Scan) anyerror!?Cursor = null,
        open_statement: ?*const fn (*anyopaque, std.mem.Allocator, []const StatementScan) anyerror!StatementRead = null,
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
