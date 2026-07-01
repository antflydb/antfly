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

const std = @import("std");
const platform_time = @import("antfly_platform").time;
const catalog_resources = @import("../catalog_resources.zig");
const db_mod = @import("../../storage/db/mod.zig");
const metadata_api = @import("../../metadata/api.zig");
const metadata_table_manager = @import("../../metadata/table_manager.zig");
const metadata_transition_state = @import("../../metadata/transition_state.zig");
const query_api = @import("../query.zig");
const raft_mod = @import("../../raft/mod.zig");
const raft_reconciler = @import("../../raft/reconciler.zig");
const relational_rows_api = @import("../../sql/relational_rows.zig");
const sql_adapter = @import("../../sql/mod.zig");
const storage_schema = @import("../../storage/schema.zig");
const table_catalog = @import("../../metadata/catalog/routing.zig");
const tables_api = @import("../../metadata/catalog/table_ddl.zig");
const table_read_core = @import("../table_reads/core.zig");
const table_read_relational_rows = @import("../table_reads/relational_rows.zig");
const table_write_core = @import("core.zig");

const TableReadSource = table_read_core.TableReadSource;
const TableWriteSource = table_write_core.TableWriteSource;
const normalizeRelationalConstraintError = table_write_core.normalizeRelationalConstraintError;
const nextTxnTimestamp = table_write_core.nextTxnTimestamp;

fn recursiveDmlFullRowSourceQuery(req: db_mod.types.RelationalRowsQueryRequest) db_mod.types.RelationalRowsQueryRequest {
    var query = req;
    query.source_cte = "";
    query.select = &.{};
    query.json_extract = &.{};
    query.array_length = &.{};
    query.coalesce = &.{};
    query.field_aliases = &.{};
    query.expressions = &.{};
    query.select_all = true;
    query.row_claim = null;
    return query;
}

fn requireAutocommitMutationClaim(claim: db_mod.types.RowClaimRequest) !db_mod.types.TxnId {
    if (!claim.mode.isExclusiveWriteClaim()) return error.InvalidQueryRequest;
    const txn_id = claim.txn_id orelse return error.InvalidQueryRequest;
    if (claim.owner_id.len == 0 or claim.lease_ms == 0) return error.InvalidQueryRequest;
    return txn_id;
}

pub fn mutateRowsJoinedFromSourceRowsOnDb(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    target_schema: storage_schema.TableSchema,
    source_schema: storage_schema.TableSchema,
    req: db_mod.types.RelationalRowsJoinedMutationSourceRequest,
    source_rows: []const []const u8,
) !db_mod.types.RelationalRowsMutationSourceResult {
    var target_candidates = try db.collectRelationalRowsJoinedMutationTargetCandidatesForTargetRangeAlloc(alloc, target_schema, req, null);
    errdefer {
        for (target_candidates) |*candidate| candidate.deinit(alloc);
        if (target_candidates.len > 0) alloc.free(target_candidates);
    }

    var candidates = try db_mod.DB.buildRelationalRowsJoinedMutationSourceCandidatesFromCollectedRowsAlloc(alloc, req, &target_candidates, source_rows);
    errdefer {
        for (candidates) |*candidate| candidate.deinit(alloc);
        if (candidates.len > 0) alloc.free(candidates);
    }

    var plan = try db_mod.DB.selectPlannedRelationalRowsJoinedMutationSourceCandidatesAlloc(alloc, req, &candidates);
    defer plan.deinit(alloc);

    return try db.stagePlannedRelationalRowsJoinedMutationSourceWithSourceSchemaAlloc(alloc, target_schema, source_schema, req, plan.matched, plan.candidates);
}

pub fn mutateRowsFromSourceAutocommitOnDb(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    schema: storage_schema.TableSchema,
    req: db_mod.types.RelationalRowsMutationSourceRequest,
) !db_mod.types.RelationalRowsMutationSourceResult {
    const claim = req.source.row_claim orelse return error.InvalidQueryRequest;
    const txn_id = try requireAutocommitMutationClaim(claim);

    const begin_timestamp = nextTxnTimestamp();
    const commit_version = begin_timestamp + 1;
    _ = try db.beginTransactionWithIdAndParticipants(txn_id, begin_timestamp, &.{});
    var result = db.mutateRelationalRowsFromSource(alloc, schema, req) catch |err| {
        db.resolveTransactionIntents(txn_id, .aborted, commit_version) catch {};
        return normalizeRelationalConstraintError(err);
    };
    errdefer result.deinit(alloc);
    db.resolveTransactionIntents(txn_id, .committed, commit_version) catch |err| {
        return normalizeRelationalConstraintError(err);
    };
    return result;
}

fn joinedMutationSourceTargetClaim(req: db_mod.types.RelationalRowsJoinedMutationSourceRequest) ?db_mod.types.RowClaimRequest {
    return switch (req.target_side) {
        .left => req.join.left.row_claim,
        .right => req.join.right.row_claim,
    };
}

fn rejectMaterializedCteSourceControls(query: db_mod.types.RelationalRowsQueryRequest) !void {
    if (query.source_cte.len == 0) return;
    if (query.row_claim != null) return error.UnsupportedRowsQuery;
    if (query.doc_key_range != null) return error.InvalidRowsRequest;
}

pub fn mutateRowsJoinedFromSourceRowsAutocommitOnDb(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    target_schema: storage_schema.TableSchema,
    source_schema: storage_schema.TableSchema,
    req: db_mod.types.RelationalRowsJoinedMutationSourceRequest,
    source_rows: []const []const u8,
) !db_mod.types.RelationalRowsMutationSourceResult {
    const claim = joinedMutationSourceTargetClaim(req) orelse return error.InvalidQueryRequest;
    const txn_id = try requireAutocommitMutationClaim(claim);

    const begin_timestamp = nextTxnTimestamp();
    const commit_version = begin_timestamp + 1;
    _ = try db.beginTransactionWithIdAndParticipants(txn_id, begin_timestamp, &.{});
    var result = mutateRowsJoinedFromSourceRowsOnDb(alloc, db, target_schema, source_schema, req, source_rows) catch |err| {
        db.resolveTransactionIntents(txn_id, .aborted, commit_version) catch {};
        return normalizeRelationalConstraintError(err);
    };
    errdefer result.deinit(alloc);
    db.resolveTransactionIntents(txn_id, .committed, commit_version) catch |err| {
        return normalizeRelationalConstraintError(err);
    };
    return result;
}

pub fn mergeRowsFromSourceRowsOnDb(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    target_schema: storage_schema.TableSchema,
    source_schema: storage_schema.TableSchema,
    plan: sql_adapter.LoweredMergeMutationPlan,
    source_rows: []const []const u8,
    comptime normalize_error: fn (anyerror) anyerror,
) !relational_rows_api.OwnedRowsBatchRequest {
    const target_preimages = try db.collectRelationalRowsPreimagesAlloc(alloc, target_schema, .{});
    defer db_mod.types.freeRelationalRowsCollectedRows(alloc, target_preimages);

    const target_rows = try alloc.alloc(sql_adapter.MergeExecutionTargetRow, target_preimages.len);
    defer alloc.free(target_rows);
    for (target_preimages, 0..) |row, i| {
        target_rows[i] = .{
            .key = row.key,
            .json = row.json,
            .version = row.version,
        };
    }

    var batch_req = try sql_adapter.buildMergeMutationBatchAlloc(alloc, target_schema, source_schema, plan, target_rows, source_rows, .{});
    errdefer batch_req.deinit(alloc);
    db.batch(batch_req.req) catch |err| return normalize_error(err);
    return batch_req;
}

pub fn mutateRowsJoinedFromRecursiveCtePlanAlloc(
    alloc: std.mem.Allocator,
    read_source: TableReadSource,
    write_source: TableWriteSource,
    catalog: table_catalog.CatalogSource,
    default_table_name: []const u8,
    recursive_source_schema: storage_schema.TableSchema,
    target_schema: storage_schema.TableSchema,
    lowered: sql_adapter.LoweredRecursiveJoinedMutationSource,
    consistency: raft_mod.ReadConsistency,
) !?db_mod.types.RelationalRowsMutationSourceResult {
    return try mutateRowsJoinedFromRecursiveCtePlanWithSessionAlloc(
        alloc,
        read_source,
        write_source,
        catalog,
        catalog_resources.SqlCatalogSession.default(),
        default_table_name,
        recursive_source_schema,
        target_schema,
        lowered,
        consistency,
    );
}

pub fn mutateRowsJoinedFromRecursiveCtePlanWithSessionAlloc(
    alloc: std.mem.Allocator,
    read_source: TableReadSource,
    write_source: TableWriteSource,
    catalog: table_catalog.CatalogSource,
    session: catalog_resources.SqlCatalogSession,
    default_table_name: []const u8,
    recursive_source_schema: storage_schema.TableSchema,
    target_schema: storage_schema.TableSchema,
    lowered: sql_adapter.LoweredRecursiveJoinedMutationSource,
    consistency: raft_mod.ReadConsistency,
) !?db_mod.types.RelationalRowsMutationSourceResult {
    const source_query = recursiveJoinedMutationSourceQuery(lowered.mutation.mutation.req);
    if (!std.mem.eql(u8, source_query.source_cte, lowered.recursive.cte_name)) return error.InvalidRowsRequest;
    try rejectMaterializedCteSourceControls(source_query);

    var materialized = (try table_read_relational_rows.materializeLoweredRecursiveCteRowsWithSessionAlloc(
        alloc,
        read_source,
        catalog,
        session,
        default_table_name,
        recursive_source_schema,
        lowered.recursive,
        consistency,
    )) orelse return null;
    defer materialized.deinit(alloc);

    var cte_schema = recursive_source_schema;
    cte_schema.relational_columns = lowered.recursive.output_columns;
    const synthetic_primary_key_columns = [_][]const u8{lowered.recursive.output_columns[0].name};
    cte_schema.primary_key = .{ .columns = synthetic_primary_key_columns[0..] };

    const filtered_source_query = recursiveDmlFullRowSourceQuery(source_query);
    var source_rows = try relational_rows_api.executeRowsQueryOnJsonRowsAlloc(alloc, cte_schema, filtered_source_query, materialized.rows);
    defer source_rows.deinit(alloc);

    return try write_source.mutateRowsJoinedFromSourceRows(
        alloc,
        lowered.mutation.target_table_name,
        target_schema,
        recursive_source_schema,
        lowered.mutation.mutation.req,
        source_rows.rows,
    );
}

pub fn mutateRowsJoinedFromRecursiveCtePlanAutocommitAlloc(
    alloc: std.mem.Allocator,
    read_source: TableReadSource,
    write_source: TableWriteSource,
    catalog: table_catalog.CatalogSource,
    default_table_name: []const u8,
    recursive_source_schema: storage_schema.TableSchema,
    target_schema: storage_schema.TableSchema,
    lowered: sql_adapter.LoweredRecursiveJoinedMutationSource,
    consistency: raft_mod.ReadConsistency,
) !?db_mod.types.RelationalRowsMutationSourceResult {
    return try mutateRowsJoinedFromRecursiveCtePlanAutocommitWithSessionAlloc(
        alloc,
        read_source,
        write_source,
        catalog,
        catalog_resources.SqlCatalogSession.default(),
        default_table_name,
        recursive_source_schema,
        target_schema,
        lowered,
        consistency,
    );
}

pub fn mutateRowsJoinedFromRecursiveCtePlanAutocommitWithSessionAlloc(
    alloc: std.mem.Allocator,
    read_source: TableReadSource,
    write_source: TableWriteSource,
    catalog: table_catalog.CatalogSource,
    session: catalog_resources.SqlCatalogSession,
    default_table_name: []const u8,
    recursive_source_schema: storage_schema.TableSchema,
    target_schema: storage_schema.TableSchema,
    lowered: sql_adapter.LoweredRecursiveJoinedMutationSource,
    consistency: raft_mod.ReadConsistency,
) !?db_mod.types.RelationalRowsMutationSourceResult {
    const source_query = recursiveJoinedMutationSourceQuery(lowered.mutation.mutation.req);
    if (!std.mem.eql(u8, source_query.source_cte, lowered.recursive.cte_name)) return error.InvalidRowsRequest;
    try rejectMaterializedCteSourceControls(source_query);

    var materialized = (try table_read_relational_rows.materializeLoweredRecursiveCteRowsWithSessionAlloc(
        alloc,
        read_source,
        catalog,
        session,
        default_table_name,
        recursive_source_schema,
        lowered.recursive,
        consistency,
    )) orelse return null;
    defer materialized.deinit(alloc);

    var cte_schema = recursive_source_schema;
    cte_schema.relational_columns = lowered.recursive.output_columns;
    const synthetic_primary_key_columns = [_][]const u8{lowered.recursive.output_columns[0].name};
    cte_schema.primary_key = .{ .columns = synthetic_primary_key_columns[0..] };

    const filtered_source_query = recursiveDmlFullRowSourceQuery(source_query);
    var source_rows = try relational_rows_api.executeRowsQueryOnJsonRowsAlloc(alloc, cte_schema, filtered_source_query, materialized.rows);
    defer source_rows.deinit(alloc);

    return try write_source.mutateRowsJoinedFromSourceRowsAutocommit(
        alloc,
        lowered.mutation.target_table_name,
        target_schema,
        recursive_source_schema,
        lowered.mutation.mutation.req,
        source_rows.rows,
        lowered.mutation.sync_level,
    );
}

pub fn mergeRowsFromRecursiveCtePlanAlloc(
    alloc: std.mem.Allocator,
    read_source: TableReadSource,
    write_source: TableWriteSource,
    catalog: table_catalog.CatalogSource,
    default_table_name: []const u8,
    recursive_source_schema: storage_schema.TableSchema,
    target_schema: storage_schema.TableSchema,
    lowered: sql_adapter.LoweredRecursiveMergeMutation,
    consistency: raft_mod.ReadConsistency,
) !?relational_rows_api.OwnedRowsBatchRequest {
    return try mergeRowsFromRecursiveCtePlanWithSessionAlloc(
        alloc,
        read_source,
        write_source,
        catalog,
        catalog_resources.SqlCatalogSession.default(),
        default_table_name,
        recursive_source_schema,
        target_schema,
        lowered,
        consistency,
    );
}

pub fn mergeRowsFromRecursiveCtePlanWithSessionAlloc(
    alloc: std.mem.Allocator,
    read_source: TableReadSource,
    write_source: TableWriteSource,
    catalog: table_catalog.CatalogSource,
    session: catalog_resources.SqlCatalogSession,
    default_table_name: []const u8,
    recursive_source_schema: storage_schema.TableSchema,
    target_schema: storage_schema.TableSchema,
    lowered: sql_adapter.LoweredRecursiveMergeMutation,
    consistency: raft_mod.ReadConsistency,
) !?relational_rows_api.OwnedRowsBatchRequest {
    if (!std.mem.eql(u8, lowered.merge.source.source_cte, lowered.recursive.cte_name)) return error.InvalidRowsRequest;
    try rejectMaterializedCteSourceControls(lowered.merge.source);

    var materialized = (try table_read_relational_rows.materializeLoweredRecursiveCteRowsWithSessionAlloc(
        alloc,
        read_source,
        catalog,
        session,
        default_table_name,
        recursive_source_schema,
        lowered.recursive,
        consistency,
    )) orelse return null;
    defer materialized.deinit(alloc);

    var cte_schema = recursive_source_schema;
    cte_schema.relational_columns = lowered.recursive.output_columns;
    const synthetic_primary_key_columns = [_][]const u8{lowered.recursive.output_columns[0].name};
    cte_schema.primary_key = .{ .columns = synthetic_primary_key_columns[0..] };

    const source_query = recursiveDmlFullRowSourceQuery(lowered.merge.source);
    var source_rows = try relational_rows_api.executeRowsQueryOnJsonRowsAlloc(alloc, cte_schema, source_query, materialized.rows);
    defer source_rows.deinit(alloc);

    var merge_plan = lowered.merge;
    merge_plan.source.source_cte = "";
    merge_plan.ctes = &.{};
    return try write_source.mergeRowsFromSourceRows(
        alloc,
        lowered.merge.target_table_name,
        target_schema,
        cte_schema,
        merge_plan,
        source_rows.rows,
    );
}

fn recursiveJoinedMutationSourceQuery(req: db_mod.types.RelationalRowsJoinedMutationSourceRequest) db_mod.types.RelationalRowsQueryRequest {
    return switch (req.target_side) {
        .left => req.join.right,
        .right => req.join.left,
    };
}

fn parseTestRuntimeSchema(alloc: std.mem.Allocator, schema_json: []const u8) !storage_schema.TableSchema {
    var parsed = try tables_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    return try tables_api.deriveRuntimeTableSchema(alloc, parsed);
}

fn applyRowsBatchJsonAt(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    schema: storage_schema.TableSchema,
    body: []const u8,
    timestamp_ns: u64,
) !void {
    var batch = try relational_rows_api.parseRowsBatchRequest(alloc, body, schema);
    defer batch.deinit(alloc);
    batch.req.timestamp_ns = timestamp_ns;
    try db.batch(batch.req);
}

const EmptyCatalogSource = struct {
    fn iface(self: *@This()) table_catalog.CatalogSource {
        return .{
            .ptr = self,
            .vtable = &.{
                .admin_snapshot = adminSnapshot,
                .free_admin_snapshot = freeAdminSnapshot,
            },
        };
    }

    fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
        return .{
            .status = .{ .metadata_group_id = 1, .metrics = .{} },
            .tables = @constCast((&[_]metadata_table_manager.TableRecord{})[0..]),
            .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{})[0..]),
            .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
            .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
            .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
            .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
        };
    }

    fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
};

const RecursiveMutationReadSource = struct {
    db: *db_mod.DB,
    calls: usize = 0,

    fn source(self: *@This()) TableReadSource {
        return .{
            .ptr = self,
            .vtable = &.{
                .lookup = lookup,
                .scan = scan,
                .query = query,
                .rows_query_plan_catalog = rowsQueryPlanCatalog,
            },
        };
    }

    fn lookup(
        _: *anyopaque,
        _: std.mem.Allocator,
        _: []const u8,
        _: []const u8,
        _: db_mod.types.LookupOptions,
        _: raft_mod.ReadConsistency,
    ) !?table_read_core.LookupResponse {
        return error.UnsupportedOperation;
    }

    fn scan(
        _: *anyopaque,
        _: std.mem.Allocator,
        _: []const u8,
        _: []const u8,
        _: []const u8,
        _: db_mod.types.ScanOptions,
        _: raft_mod.ReadConsistency,
    ) !?table_read_core.ScanResponse {
        return error.UnsupportedOperation;
    }

    fn query(
        _: *anyopaque,
        _: std.mem.Allocator,
        _: []const u8,
        _: db_mod.types.SearchRequest,
        _: raft_mod.ReadConsistency,
    ) !?query_api.QueryResponse {
        return error.UnsupportedOperation;
    }

    fn rowsQueryPlanCatalog(
        ptr: *anyopaque,
        allocator: std.mem.Allocator,
        target: catalog_resources.TableTarget,
        runtime_schema: storage_schema.TableSchema,
        plan: db_mod.types.RelationalRowsQueryPlan,
        consistency: raft_mod.ReadConsistency,
    ) !?db_mod.types.RelationalRowsQueryResult {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        try std.testing.expectEqual(raft_mod.ReadConsistency.read_index, consistency);
        try std.testing.expectEqualStrings(catalog_resources.default_database_name, target.database_name);
        try std.testing.expectEqualStrings(catalog_resources.default_namespace_name, target.namespace_name);
        try std.testing.expectEqualStrings("usage_records", target.table_name);
        self.calls += 1;
        return try self.db.queryRelationalRowsPlan(allocator, runtime_schema, plan);
    }
};

const LocalRelationalMutationWriteSource = struct {
    table_name: []const u8,
    db: *db_mod.DB,

    fn source(self: *@This()) TableWriteSource {
        return .{
            .ptr = self,
            .vtable = &.{
                .batch = batch,
                .mutate_rows_from_source = mutateRowsFromSource,
                .mutate_rows_joined_from_source_rows = mutateRowsJoinedFromSourceRows,
                .merge_rows_from_source_rows = mergeRowsFromSourceRows,
            },
        };
    }

    fn batch(
        _: *anyopaque,
        _: std.mem.Allocator,
        _: []const u8,
        _: db_mod.types.BatchRequest,
    ) !?void {
        return error.UnsupportedOperation;
    }

    fn mutateRowsFromSource(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        schema: storage_schema.TableSchema,
        req: db_mod.types.RelationalRowsMutationSourceRequest,
    ) !?db_mod.types.RelationalRowsMutationSourceResult {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, table_name, self.table_name)) return null;
        return try self.db.mutateRelationalRowsFromSource(alloc, schema, req);
    }

    fn mutateRowsJoinedFromSourceRows(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        target_schema: storage_schema.TableSchema,
        source_schema: storage_schema.TableSchema,
        req: db_mod.types.RelationalRowsJoinedMutationSourceRequest,
        source_rows: []const []const u8,
    ) !?db_mod.types.RelationalRowsMutationSourceResult {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, table_name, self.table_name)) return null;
        return try mutateRowsJoinedFromSourceRowsOnDb(alloc, self.db, target_schema, source_schema, req, source_rows);
    }

    fn mergeRowsFromSourceRows(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        target_schema: storage_schema.TableSchema,
        source_schema: storage_schema.TableSchema,
        plan: sql_adapter.LoweredMergeMutationPlan,
        source_rows: []const []const u8,
    ) !?relational_rows_api.OwnedRowsBatchRequest {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, table_name, self.table_name)) return null;
        return try mergeRowsFromSourceRowsOnDb(alloc, self.db, target_schema, source_schema, plan, source_rows, normalizeRelationalConstraintError);
    }
};

test "mutation source autocommit rejects non-exclusive claims before opening transaction" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-mutation-source-autocommit-claim-admission";
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer db.close();

    const target_schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"target_rows","enforce_types":true,"document_schemas":{"target_rows":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"source_id":{"type":"keyword"},"status":{"type":"keyword"},"quantity":{"type":"numeric"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    const target_schema = try parseTestRuntimeSchema(alloc, target_schema_json);
    defer storage_schema.freeSchema(alloc, target_schema);
    try db.applyTableSchemaJson(alloc, target_schema_json, .{});

    const source_schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"source_rows","enforce_types":true,"document_schemas":{"source_rows":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"state":{"type":"keyword"},"source_quantity":{"type":"numeric"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    const source_schema = try parseTestRuntimeSchema(alloc, source_schema_json);
    defer storage_schema.freeSchema(alloc, source_schema);

    const single_txn_id: db_mod.types.TxnId = .{ 0xa0, 0xa1, 0xa2, 0xa3, 0xa4, 0xa5, 0xa6, 0xa7, 0xa8, 0xa9, 0xaa, 0xab, 0xac, 0xad, 0xae, 0xaf };
    try std.testing.expectError(error.InvalidQueryRequest, mutateRowsFromSourceAutocommitOnDb(alloc, &db, target_schema, .{
        .kind = .update,
        .source = .{
            .row_claim = .{
                .mode = .for_share,
                .owner_id = "session:invalid-single-autocommit",
                .txn_id = single_txn_id,
            },
        },
        .operations = &.{.{
            .op = .set,
            .path = "status",
            .value_json = "\"done\"",
        }},
    }));
    try std.testing.expectError(error.TxnNotFound, db.getTransactionStatus(single_txn_id));

    const joined_txn_id: db_mod.types.TxnId = .{ 0xb0, 0xb1, 0xb2, 0xb3, 0xb4, 0xb5, 0xb6, 0xb7, 0xb8, 0xb9, 0xba, 0xbb, 0xbc, 0xbd, 0xbe, 0xbf };
    const on = [_]db_mod.types.RelationalRowsJoinOn{.{
        .left_field = "source_id",
        .right_field = "id",
    }};
    const source_assignments = [_]db_mod.types.RelationalRowsJoinedMutationFieldAssignment{.{
        .field = "quantity",
        .source_side = .right,
        .source_field = "source_quantity",
    }};
    const source_rows = [_][]const u8{
        "{\"id\":\"s1\",\"state\":\"source\",\"source_quantity\":42}",
    };
    try std.testing.expectError(error.InvalidQueryRequest, mutateRowsJoinedFromSourceRowsAutocommitOnDb(alloc, &db, target_schema, source_schema, .{
        .kind = .update,
        .target_side = .left,
        .join = .{
            .left = .{
                .row_claim = .{
                    .mode = .for_key_share,
                    .owner_id = "session:invalid-joined-autocommit",
                    .txn_id = joined_txn_id,
                },
            },
            .right = .{},
            .on = on[0..],
        },
        .source_assignments = source_assignments[0..],
    }, source_rows[0..]));
    try std.testing.expectError(error.TxnNotFound, db.getTransactionStatus(joined_txn_id));
}

test "local mutation source staged claims recover after reopen before commit" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-local-mutation-source-reopen-staged-claim";
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    const schema = try parseTestRuntimeSchema(alloc, schema_json);
    defer storage_schema.freeSchema(alloc, schema);

    const stale_txn = blk: {
        var db = try db_mod.DB.open(alloc, path, .{});
        defer db.close();
        try db.setSchema(schema);

        try db.batch(.{
            .writes = &.{.{ .key = "row:a", .value = "{\"id\":\"a\",\"status\":\"ready\"}" }},
            .timestamp_ns = 1_000,
        });

        const txn_id = try db.beginTransaction(2_000);
        var write_source = LocalRelationalMutationWriteSource{ .table_name = "usage_records", .db = &db };
        const predicates = [_]storage_schema.RelationalCheck{.{
            .name = "",
            .field = "status",
            .op = .eq,
            .value_json = "\"ready\"",
        }};
        const operations = [_]db_mod.types.TransformOp{.{
            .op = .set,
            .path = "status",
            .value_json = "\"claimed\"",
        }};
        const returning = [_][]const u8{ "id", "status" };
        var staged = (try write_source.source().mutateRowsFromSource(alloc, "usage_records", schema, .{
            .kind = .update,
            .source = .{
                .predicates = predicates[0..],
                .row_claim = .{
                    .mode = .for_update,
                    .owner_id = "session:stale-api",
                    .lease_ms = 1,
                    .txn_id = txn_id,
                },
            },
            .operations = operations[0..],
            .returning = returning[0..],
        })) orelse return error.TestUnexpectedResult;
        defer staged.deinit(alloc);

        try std.testing.expectEqual(@as(u32, 1), staged.matched);
        try std.testing.expectEqual(@as(u32, 1), staged.staged);
        try std.testing.expectEqual(@as(usize, 1), staged.returning_rows.len);
        try std.testing.expectEqualStrings("{\"id\":\"a\",\"status\":\"claimed\"}", staged.returning_rows[0]);
        break :blk txn_id;
    };

    platform_time.sleepNs(10 * std.time.ns_per_ms);

    {
        var db = try db_mod.DB.open(alloc, path, .{});
        defer db.close();
        try db.setSchema(schema);

        try std.testing.expectEqual(db_mod.types.TxnStatus.pending, try db.getTransactionStatus(stale_txn));
        var visible_before = (try db.lookup(alloc, "row:a", .{})) orelse return error.TestUnexpectedResult;
        defer visible_before.deinit(alloc);
        try std.testing.expect(std.mem.indexOf(u8, visible_before.json, "\"status\":\"ready\"") != null);
        try std.testing.expect(std.mem.indexOf(u8, visible_before.json, "\"status\":\"claimed\"") == null);

        const next_txn = try db.beginTransaction(3_000);
        var write_source = LocalRelationalMutationWriteSource{ .table_name = "usage_records", .db = &db };
        const predicates = [_]storage_schema.RelationalCheck{.{
            .name = "",
            .field = "status",
            .op = .eq,
            .value_json = "\"ready\"",
        }};
        const operations = [_]db_mod.types.TransformOp{.{
            .op = .set,
            .path = "status",
            .value_json = "\"done\"",
        }};
        const returning = [_][]const u8{ "id", "status" };
        var staged = (try write_source.source().mutateRowsFromSource(alloc, "usage_records", schema, .{
            .kind = .update,
            .source = .{
                .predicates = predicates[0..],
                .row_claim = .{
                    .mode = .for_update,
                    .owner_id = "session:next-api",
                    .txn_id = next_txn,
                },
            },
            .operations = operations[0..],
            .returning = returning[0..],
        })) orelse return error.TestUnexpectedResult;
        defer staged.deinit(alloc);

        try std.testing.expectEqual(@as(u32, 1), staged.matched);
        try std.testing.expectEqual(@as(u32, 1), staged.staged);
        try std.testing.expectEqualStrings("{\"id\":\"a\",\"status\":\"done\"}", staged.returning_rows[0]);
        try std.testing.expectEqual(db_mod.types.TxnStatus.aborted, try db.getTransactionStatus(stale_txn));
        try std.testing.expectError(error.DecisionConflict, db.writeTransaction(stale_txn, .{
            .writes = &.{.{ .key = "row:a", .value = "{\"id\":\"a\",\"status\":\"stale\"}" }},
        }));

        try std.testing.expectError(error.IntentConflict, db.batch(.{
            .writes = &.{.{ .key = "row:a", .value = "{\"id\":\"a\",\"status\":\"blocked\"}" }},
            .timestamp_ns = 3_001,
        }));

        try db.commitTransaction(next_txn, 3_010);
    }

    var final_db = try db_mod.DB.open(alloc, path, .{});
    defer final_db.close();
    try final_db.setSchema(schema);

    var final_row = (try final_db.lookup(alloc, "row:a", .{})) orelse return error.TestUnexpectedResult;
    defer final_row.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, final_row.json, "\"status\":\"done\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, final_row.json, "\"status\":\"claimed\"") == null);
    try std.testing.expect(std.mem.indexOf(u8, final_row.json, "\"status\":\"stale\"") == null);
}

test "bound table write source stages joined mutation from materialized source rows" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-bound-joined-mutation-source-rows";
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer db.close();

    const target_schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"target_rows","enforce_types":true,"document_schemas":{"target_rows":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"source_id":{"type":"keyword"},"status":{"type":"keyword"},"quantity":{"type":"numeric"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    const target_schema = try parseTestRuntimeSchema(alloc, target_schema_json);
    defer storage_schema.freeSchema(alloc, target_schema);
    try db.applyTableSchemaJson(alloc, target_schema_json, .{});

    const source_schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"source_rows","enforce_types":true,"document_schemas":{"source_rows":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"state":{"type":"keyword"},"source_quantity":{"type":"numeric"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    const source_schema = try parseTestRuntimeSchema(alloc, source_schema_json);
    defer storage_schema.freeSchema(alloc, source_schema);

    try db.batch(.{
        .writes = &.{.{
            .key = "target:t1",
            .value = "{\"id\":\"t1\",\"source_id\":\"s1\",\"status\":\"ready\",\"quantity\":1}",
        }},
        .sync_level = .write,
    });

    const txn_id = try db.beginTransaction(10_000);
    const target_predicates = [_]storage_schema.RelationalCheck{.{
        .name = "",
        .field = "status",
        .op = .eq,
        .value_json = "\"ready\"",
    }};
    const source_predicates = [_]storage_schema.RelationalCheck{.{
        .name = "",
        .field = "state",
        .op = .eq,
        .value_json = "\"source\"",
    }};
    const on = [_]db_mod.types.RelationalRowsJoinOn{.{
        .left_field = "source_id",
        .right_field = "id",
    }};
    const source_assignments = [_]db_mod.types.RelationalRowsJoinedMutationFieldAssignment{.{
        .field = "quantity",
        .source_side = .right,
        .source_field = "source_quantity",
    }};
    const returning = [_][]const u8{ "id", "quantity", "status" };
    const req = db_mod.types.RelationalRowsJoinedMutationSourceRequest{
        .kind = .update,
        .source_table = "recursive_source",
        .target_side = .left,
        .join = .{
            .left = .{
                .predicates = target_predicates[0..],
                .row_claim = .{
                    .mode = .for_update,
                    .owner_id = "session:joined-source-rows",
                    .txn_id = txn_id,
                },
            },
            .right = .{ .predicates = source_predicates[0..] },
            .on = on[0..],
        },
        .source_assignments = source_assignments[0..],
        .returning = returning[0..],
    };

    const source_rows = [_][]const u8{
        "{\"id\":\"s1\",\"state\":\"source\",\"source_quantity\":42}",
    };
    var write_source = LocalRelationalMutationWriteSource{ .table_name = "orders", .db = &db };
    var result = (try write_source.source().mutateRowsJoinedFromSourceRows(
        alloc,
        "orders",
        target_schema,
        source_schema,
        req,
        source_rows[0..],
    )) orelse return error.TestUnexpectedResult;
    defer result.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 1), result.matched);
    try std.testing.expectEqual(@as(u32, 1), result.staged);
    try std.testing.expectEqual(@as(usize, 1), result.returning_rows.len);
    try std.testing.expectEqualStrings("{\"id\":\"t1\",\"quantity\":42,\"status\":\"ready\"}", result.returning_rows[0]);

    try db.commitTransaction(txn_id, 10_001);

    var updated = (try db.lookup(alloc, "target:t1", .{})) orelse return error.TestUnexpectedResult;
    defer updated.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, updated.json, "\"quantity\":42") != null);

    try std.testing.expect((try write_source.source().mutateRowsJoinedFromSourceRows(
        alloc,
        "other_table",
        target_schema,
        source_schema,
        req,
        source_rows[0..],
    )) == null);
}

test "recursive cte joined mutation source executes through typed read materialization and write staging" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-recursive-joined-mutation-source";
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer db.close();

    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"organization_id":{"type":"keyword"},"status":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    const schema = try parseTestRuntimeSchema(alloc, schema_json);
    defer storage_schema.freeSchema(alloc, schema);
    try db.applyTableSchemaJson(alloc, schema_json, .{});
    try applyRowsBatchJsonAt(alloc, &db, schema,
        \\{"operations":[{"op":"insert","row":{"id":"a","organization_id":"root","status":"open"}},{"op":"insert","row":{"id":"b","organization_id":"a","status":"open"}},{"op":"insert","row":{"id":"c","organization_id":"other","status":"open"}}],"sync_level":"write"}
    , 1);

    const txn_id = try db.beginTransaction(10_000);
    var txn_open = true;
    defer if (txn_open) db.abortTransaction(txn_id, 10_999) catch {};
    var lowered = try sql_adapter.lowerWritePlanAlloc(
        alloc,
        "WITH RECURSIVE source_rows AS (SELECT id FROM usage_records WHERE organization_id = 'root' UNION ALL SELECT child.id FROM usage_records AS child JOIN source_rows AS parent ON child.organization_id = parent.id) UPDATE usage_records SET status = 'done' WHERE id IN (SELECT id FROM source_rows)",
        schema,
        &.{},
        .{ .row_claim = .{
            .mode = .for_update,
            .owner_id = "session:recursive-joined-mutation",
            .txn_id = txn_id,
        } },
    );
    defer lowered.deinit(alloc);

    var catalog = EmptyCatalogSource{};
    var read_source = RecursiveMutationReadSource{ .db = &db };
    var write_source = LocalRelationalMutationWriteSource{ .table_name = "usage_records", .db = &db };

    switch (lowered) {
        .recursive_update_joined_source => |recursive| {
            var claimed_cte_source = recursive;
            claimed_cte_source.mutation.mutation.req.join.right.row_claim = .{
                .mode = .for_update,
                .owner_id = "session:illegal-cte-source-claim",
                .txn_id = txn_id,
            };
            const calls_before = read_source.calls;
            try std.testing.expectError(error.UnsupportedRowsQuery, mutateRowsJoinedFromRecursiveCtePlanAlloc(
                alloc,
                read_source.source(),
                write_source.source(),
                catalog.iface(),
                "usage_records",
                schema,
                schema,
                claimed_cte_source,
                .read_index,
            ));
            try std.testing.expectEqual(calls_before, read_source.calls);

            var ranged_cte_source = recursive;
            ranged_cte_source.mutation.mutation.req.join.right.doc_key_range = .{ .start = "row:a", .end = "row:z" };
            try std.testing.expectError(error.InvalidRowsRequest, mutateRowsJoinedFromRecursiveCtePlanAlloc(
                alloc,
                read_source.source(),
                write_source.source(),
                catalog.iface(),
                "usage_records",
                schema,
                schema,
                ranged_cte_source,
                .read_index,
            ));
            try std.testing.expectEqual(calls_before, read_source.calls);
        },
        else => return error.TestUnexpectedResult,
    }

    var result = switch (lowered) {
        .recursive_update_joined_source => |recursive| (try mutateRowsJoinedFromRecursiveCtePlanAlloc(
            alloc,
            read_source.source(),
            write_source.source(),
            catalog.iface(),
            "usage_records",
            schema,
            schema,
            recursive,
            .read_index,
        )) orelse return error.TestUnexpectedResult,
        else => return error.TestUnexpectedResult,
    };
    defer result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 2), read_source.calls);
    try std.testing.expectEqual(@as(u32, 2), result.matched);
    try std.testing.expectEqual(@as(u32, 2), result.staged);

    try db.commitTransaction(txn_id, 10_001);
    txn_open = false;

    var rows = try db.queryRelationalRows(alloc, schema, .{ .select_all = true, .order_by = &.{.{ .field = "id" }} });
    defer rows.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 3), rows.rows.len);
    try std.testing.expectEqualStrings("{\"id\":\"a\",\"organization_id\":\"root\",\"status\":\"done\"}", rows.rows[0]);
    try std.testing.expectEqualStrings("{\"id\":\"b\",\"organization_id\":\"a\",\"status\":\"done\"}", rows.rows[1]);
    try std.testing.expectEqualStrings("{\"id\":\"c\",\"organization_id\":\"other\",\"status\":\"open\"}", rows.rows[2]);
}

test "recursive cte merge mutation executes through typed read materialization and merge batch staging" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-recursive-merge-mutation";
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer db.close();

    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"organization_id":{"type":"keyword"},"status":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    const schema = try parseTestRuntimeSchema(alloc, schema_json);
    defer storage_schema.freeSchema(alloc, schema);
    try db.applyTableSchemaJson(alloc, schema_json, .{});
    try applyRowsBatchJsonAt(alloc, &db, schema,
        \\{"operations":[{"op":"insert","row":{"id":"a","organization_id":"root","status":"open"}},{"op":"insert","row":{"id":"b","organization_id":"a","status":"open"}},{"op":"insert","row":{"id":"c","organization_id":"other","status":"open"}}],"sync_level":"write"}
    , 1);

    var lowered = try sql_adapter.lowerWritePlanAlloc(
        alloc,
        "WITH RECURSIVE source_rows AS (SELECT id FROM usage_records WHERE organization_id = 'root' UNION ALL SELECT child.id FROM usage_records AS child JOIN source_rows AS parent ON child.organization_id = parent.id) MERGE INTO usage_records USING source_rows ON usage_records.id = source_rows.id WHEN MATCHED THEN UPDATE SET status = 'done'",
        schema,
        &.{},
        .{},
    );
    defer lowered.deinit(alloc);

    var catalog = EmptyCatalogSource{};
    var read_source = RecursiveMutationReadSource{ .db = &db };
    var write_source = LocalRelationalMutationWriteSource{ .table_name = "usage_records", .db = &db };

    switch (lowered) {
        .recursive_merge_mutation => |recursive| {
            var claimed_cte_source = recursive;
            claimed_cte_source.merge.source.row_claim = .{
                .mode = .for_update,
                .owner_id = "session:illegal-merge-cte-source-claim",
                .txn_id = [_]u8{ 0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x39, 0x3a, 0x3b, 0x3c, 0x3d, 0x3e, 0x3f },
            };
            const calls_before = read_source.calls;
            try std.testing.expectError(error.UnsupportedRowsQuery, mergeRowsFromRecursiveCtePlanAlloc(
                alloc,
                read_source.source(),
                write_source.source(),
                catalog.iface(),
                "usage_records",
                schema,
                schema,
                claimed_cte_source,
                .read_index,
            ));
            try std.testing.expectEqual(calls_before, read_source.calls);

            var ranged_cte_source = recursive;
            ranged_cte_source.merge.source.doc_key_range = .{ .start = "row:a", .end = "row:z" };
            try std.testing.expectError(error.InvalidRowsRequest, mergeRowsFromRecursiveCtePlanAlloc(
                alloc,
                read_source.source(),
                write_source.source(),
                catalog.iface(),
                "usage_records",
                schema,
                schema,
                ranged_cte_source,
                .read_index,
            ));
            try std.testing.expectEqual(calls_before, read_source.calls);
        },
        else => return error.TestUnexpectedResult,
    }

    var batch_result = switch (lowered) {
        .recursive_merge_mutation => |recursive| blk: {
            const source_order = [_]db_mod.types.RelationalRowsQueryOrder{.{
                .field = "id",
                .direction = .desc,
            }};
            const source_projection = [_]db_mod.types.RelationalRowsExpressionProjection{.{
                .output = "id",
                .expression = .{ .kind = .value, .value_json = "\"shadowed\"" },
            }};
            var projected_recursive = recursive;
            projected_recursive.merge.source.order_by = source_order[0..];
            projected_recursive.merge.source.limit = 1;
            projected_recursive.merge.source.expressions = source_projection[0..];

            break :blk (try mergeRowsFromRecursiveCtePlanAlloc(
                alloc,
                read_source.source(),
                write_source.source(),
                catalog.iface(),
                "usage_records",
                schema,
                schema,
                projected_recursive,
                .read_index,
            )) orelse return error.TestUnexpectedResult;
        },
        else => return error.TestUnexpectedResult,
    };
    defer batch_result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 2), read_source.calls);
    try std.testing.expectEqual(@as(u32, 1), batch_result.transformed);

    var rows = try db.queryRelationalRows(alloc, schema, .{ .select_all = true, .order_by = &.{.{ .field = "id" }} });
    defer rows.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 3), rows.rows.len);
    try std.testing.expectEqualStrings("{\"id\":\"a\",\"organization_id\":\"root\",\"status\":\"open\"}", rows.rows[0]);
    try std.testing.expectEqualStrings("{\"id\":\"b\",\"organization_id\":\"a\",\"status\":\"done\"}", rows.rows[1]);
    try std.testing.expectEqualStrings("{\"id\":\"c\",\"organization_id\":\"other\",\"status\":\"open\"}", rows.rows[2]);
}
