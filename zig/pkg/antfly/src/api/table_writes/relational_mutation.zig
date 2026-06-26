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
const catalog_resources = @import("../catalog_resources.zig");
const db_mod = @import("../../storage/db/mod.zig");
const raft_mod = @import("../../raft/mod.zig");
const relational_rows_api = @import("../relational_rows.zig");
const sql_adapter = @import("../../sql/mod.zig");
const storage_schema = @import("../../storage/schema.zig");
const table_catalog = @import("../table_catalog.zig");
const table_reads = @import("../table_reads.zig");
const table_write_core = @import("core.zig");

const TableWriteSource = table_write_core.TableWriteSource;

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

    var batch_req = try sql_adapter.buildMergeMutationBatchAlloc(alloc, target_schema, source_schema, plan, target_rows, source_rows);
    errdefer batch_req.deinit(alloc);
    db.batch(batch_req.req) catch |err| return normalize_error(err);
    return batch_req;
}

pub fn mutateRowsJoinedFromRecursiveCtePlanAlloc(
    alloc: std.mem.Allocator,
    read_source: table_reads.TableReadSource,
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
    read_source: table_reads.TableReadSource,
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

    var materialized = (try table_reads.materializeLoweredSqlRecursiveCteRowsWithSessionAlloc(
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

    var filtered_source_query = source_query;
    filtered_source_query.source_cte = "";
    filtered_source_query.select = &.{};
    filtered_source_query.select_all = true;
    filtered_source_query.row_claim = null;
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
    read_source: table_reads.TableReadSource,
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
    read_source: table_reads.TableReadSource,
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

    var materialized = (try table_reads.materializeLoweredSqlRecursiveCteRowsWithSessionAlloc(
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

    var filtered_source_query = source_query;
    filtered_source_query.source_cte = "";
    filtered_source_query.select = &.{};
    filtered_source_query.select_all = true;
    filtered_source_query.row_claim = null;
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
    read_source: table_reads.TableReadSource,
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
    read_source: table_reads.TableReadSource,
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

    var materialized = (try table_reads.materializeLoweredSqlRecursiveCteRowsWithSessionAlloc(
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

    var source_query = lowered.merge.source;
    source_query.source_cte = "";
    source_query.select = &.{};
    source_query.select_all = true;
    source_query.row_claim = null;
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
