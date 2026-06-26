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

const db_mod = @import("../../storage/db/mod.zig");
const storage_schema = @import("../../storage/schema.zig");
const raft_mod = @import("../../raft/mod.zig");
const query_api = @import("../query.zig");
const relational_rows_api = @import("../relational_rows.zig");
const external_binding_api = @import("../../serverless/external_source/catalog_binding.zig");
const external_source_api = @import("../../serverless/external_source/mod.zig");
const artifact_ref_api = @import("../../serverless/manifest/artifact_ref.zig");
const sidecar_manifest_api = @import("../../serverless/segment/sidecar_manifest.zig");
const source_binding_api = @import("../../serverless/segment/source_binding.zig");
const serverless_algebraic_segment = @import("../../serverless/algebraic_segment/mod.zig");
const serverless_query = @import("../../serverless/query/mod.zig");
const rowsource_api = @import("../../storage/rowsource/types.zig");
const object_storage_api = @import("../../storage/object_storage.zig");
const table_read_core = @import("core.zig");

const TableReadSource = table_read_core.TableReadSource;
const LookupResponse = table_read_core.LookupResponse;
const ScanResponse = table_read_core.ScanResponse;

pub const PinnedExternalLakeRowsScanner = struct {
    inventory: external_source_api.Inventory,
    reader: serverless_query.LakeParquetObjectRangeReader,
    cache: ?*serverless_query.LakeParquetObjectRangeCache = null,
    coalesce_options: serverless_query.LakeRangeCoalesceOptions = .{},
    sidecar_context: PinnedExternalLakeSidecarContext = .{},

    pub fn scanAlloc(
        self: PinnedExternalLakeRowsScanner,
        alloc: std.mem.Allocator,
        runtime_schema: storage_schema.TableSchema,
        request: serverless_query.LakeRowsScanRequest,
    ) !serverless_query.LakeRowsScanResult {
        return try executePinnedExternalLakeRowsScanAlloc(
            alloc,
            runtime_schema,
            self.inventory,
            self.reader,
            self.cache,
            self.coalesce_options,
            self.sidecar_context,
            request,
        );
    }
};

pub const PinnedExternalLakeSidecarContext = struct {
    sidecars: []const sidecar_manifest_api.DeclaredArtifact = &.{},
    desired_sidecars: []const serverless_query.LakeSidecarDesired = &.{},
    sidecar_policy: serverless_query.LakeSidecarSelectionPolicy = .{},
    candidates: []const serverless_query.LakeRowsSidecarCandidateSet = &.{},
};

pub const PinnedExternalObjectStorageLakeRowsScanner = struct {
    inventory: external_source_api.Inventory,
    object_reader: serverless_query.LakeObjectStorageRangeReader,
    cache: ?*serverless_query.LakeParquetObjectRangeCache = null,
    coalesce_options: serverless_query.LakeRangeCoalesceOptions = .{},
    sidecar_context: PinnedExternalLakeSidecarContext = .{},
    iceberg_delete_plan: ?serverless_query.LakeIcebergDeletePlan = null,

    pub fn init(
        inventory: external_source_api.Inventory,
        client: object_storage_api.ObjectStorage,
    ) PinnedExternalObjectStorageLakeRowsScanner {
        return .{
            .inventory = inventory,
            .object_reader = serverless_query.LakeObjectStorageRangeReader.init(client),
        };
    }

    pub fn parquetScanner(self: *@This()) PinnedExternalLakeRowsScanner {
        return .{
            .inventory = self.inventory,
            .reader = self.object_reader.parquetReader(),
            .cache = self.cache,
            .coalesce_options = self.coalesce_options,
            .sidecar_context = self.sidecar_context,
        };
    }

    pub fn scanAlloc(
        self: *@This(),
        alloc: std.mem.Allocator,
        runtime_schema: storage_schema.TableSchema,
        request: serverless_query.LakeRowsScanRequest,
    ) !serverless_query.LakeRowsScanResult {
        if (inventoryHasRowGroupMetadata(self.inventory)) {
            return try self.scanInventoryAlloc(alloc, runtime_schema, self.inventory, request);
        }

        const external_base_source = runtime_schema.external_base_source orelse return error.InvalidRowsRequest;
        const binding = external_binding_api.bindingFromRuntimeExternalBaseSource(external_base_source);
        var validation = try serverless_query.planProjectedLakeScanAlloc(alloc, .{
            .binding = binding,
            .inventory = self.inventory,
            .projected_columns = request.projected_columns,
        });
        defer validation.deinit(alloc);

        var discovered = serverless_query.discoverLakeParquetSupportedI64ObjectRangeRowGroupsFromFootersAlloc(
            alloc,
            self.object_reader.parquetReader(),
            self.inventory,
            request.projected_columns,
            64 * 1024,
        ) catch |err| return normalizedFooterDiscoveryError(err);
        defer discovered.deinit(alloc);

        return try self.scanInventoryAlloc(alloc, runtime_schema, discovered.inventory, request);
    }

    pub fn expressionAggregatesAlloc(
        self: *@This(),
        alloc: std.mem.Allocator,
        runtime_schema: storage_schema.TableSchema,
        request: serverless_query.LakeRowsExpressionAggregateRequest,
    ) !serverless_query.LakeRowsExpressionAggregateResult {
        if (inventoryHasRowGroupMetadata(self.inventory)) {
            return try self.expressionAggregatesInventoryAlloc(alloc, runtime_schema, self.inventory, request);
        }

        const external_base_source = runtime_schema.external_base_source orelse return error.InvalidRowsRequest;
        const binding = external_binding_api.bindingFromRuntimeExternalBaseSource(external_base_source);
        const projected_columns = try lakeExpressionAggregateProjectedColumnsAlloc(alloc, request.expressions);
        defer alloc.free(projected_columns);
        if (projected_columns.len == 0) return error.UnsupportedLakeRowsExpressionAggregate;

        var validation = try serverless_query.planProjectedLakeScanAlloc(alloc, .{
            .binding = binding,
            .inventory = self.inventory,
            .projected_columns = projected_columns,
        });
        defer validation.deinit(alloc);

        var discovered = serverless_query.discoverLakeParquetSupportedI64ObjectRangeRowGroupsFromFootersAlloc(
            alloc,
            self.object_reader.parquetReader(),
            self.inventory,
            projected_columns,
            64 * 1024,
        ) catch |err| return normalizedFooterDiscoveryError(err);
        defer discovered.deinit(alloc);

        return try self.expressionAggregatesInventoryAlloc(alloc, runtime_schema, discovered.inventory, request);
    }

    fn scanInventoryAlloc(
        self: *@This(),
        alloc: std.mem.Allocator,
        runtime_schema: storage_schema.TableSchema,
        inventory: external_source_api.Inventory,
        request: serverless_query.LakeRowsScanRequest,
    ) !serverless_query.LakeRowsScanResult {
        var local_request = request;
        var iceberg_deleted_refs: []rowsource_api.RowRef = &.{};
        defer if (iceberg_deleted_refs.len > 0) alloc.free(iceberg_deleted_refs);
        var combined_deleted_refs: []rowsource_api.RowRef = &.{};
        defer if (combined_deleted_refs.len > 0) alloc.free(combined_deleted_refs);

        if (self.iceberg_delete_plan) |delete_plan| {
            iceberg_deleted_refs = try serverless_query.readLakeIcebergDeleteRowRefsAlloc(alloc, .{
                .reader = self.object_reader.parquetReader(),
                .client = self.object_reader.client,
                .cache = self.cache,
                .data_inventory = inventory,
                .delete_plan = delete_plan,
                .coalesce_options = self.coalesce_options,
            });
            if (request.deleted_row_refs.len == 0) {
                local_request.deleted_row_refs = iceberg_deleted_refs;
            } else if (iceberg_deleted_refs.len != 0) {
                combined_deleted_refs = try alloc.alloc(rowsource_api.RowRef, request.deleted_row_refs.len + iceberg_deleted_refs.len);
                @memcpy(combined_deleted_refs[0..request.deleted_row_refs.len], request.deleted_row_refs);
                @memcpy(combined_deleted_refs[request.deleted_row_refs.len..], iceberg_deleted_refs);
                local_request.deleted_row_refs = combined_deleted_refs;
            }
        }

        const scanner = PinnedExternalLakeRowsScanner{
            .inventory = inventory,
            .reader = self.object_reader.parquetReader(),
            .cache = self.cache,
            .coalesce_options = self.coalesce_options,
            .sidecar_context = self.sidecar_context,
        };
        return try scanner.scanAlloc(alloc, runtime_schema, local_request);
    }

    fn expressionAggregatesInventoryAlloc(
        self: *@This(),
        alloc: std.mem.Allocator,
        runtime_schema: storage_schema.TableSchema,
        inventory: external_source_api.Inventory,
        request: serverless_query.LakeRowsExpressionAggregateRequest,
    ) !serverless_query.LakeRowsExpressionAggregateResult {
        if (runtime_schema.storage_mode != .relational) return error.InvalidRowsRequest;
        const external_base_source = runtime_schema.external_base_source orelse return error.InvalidRowsRequest;
        const binding = external_binding_api.bindingFromRuntimeExternalBaseSource(external_base_source);
        if (binding.format != .parquet and binding.format != .iceberg) return error.UnsupportedRowsQuery;

        var local_request = request;
        var iceberg_deleted_refs: []rowsource_api.RowRef = &.{};
        defer if (iceberg_deleted_refs.len > 0) alloc.free(iceberg_deleted_refs);
        var combined_deleted_refs: []rowsource_api.RowRef = &.{};
        defer if (combined_deleted_refs.len > 0) alloc.free(combined_deleted_refs);

        if (self.iceberg_delete_plan) |delete_plan| {
            iceberg_deleted_refs = try serverless_query.readLakeIcebergDeleteRowRefsAlloc(alloc, .{
                .reader = self.object_reader.parquetReader(),
                .client = self.object_reader.client,
                .cache = self.cache,
                .data_inventory = inventory,
                .delete_plan = delete_plan,
                .coalesce_options = self.coalesce_options,
            });
            if (request.deleted_row_refs.len == 0) {
                local_request.deleted_row_refs = iceberg_deleted_refs;
            } else if (iceberg_deleted_refs.len != 0) {
                combined_deleted_refs = try alloc.alloc(rowsource_api.RowRef, request.deleted_row_refs.len + iceberg_deleted_refs.len);
                @memcpy(combined_deleted_refs[0..request.deleted_row_refs.len], request.deleted_row_refs);
                @memcpy(combined_deleted_refs[request.deleted_row_refs.len..], iceberg_deleted_refs);
                local_request.deleted_row_refs = combined_deleted_refs;
            }
        }

        local_request.materialized_source = .{
            .kind = switch (binding.format) {
                .parquet => .external_parquet,
                .iceberg => .external_iceberg,
                .lance => .external_lance,
            },
            .source_id = inventory.source_id,
            .snapshot_id = inventory.snapshot_id,
            .schema_fingerprint = inventory.schema_fingerprint,
        };

        return try serverless_query.executeLakeParquetSupportedI64ObjectRangeExpressionAggregatesAlloc(alloc, .{
            .binding = binding,
            .reader = self.object_reader.parquetReader(),
            .cache = self.cache,
            .inventory = inventory,
            .aggregate = local_request,
            .coalesce_options = self.coalesce_options,
        });
    }

    fn inventoryHasRowGroupMetadata(inventory: external_source_api.Inventory) bool {
        for (inventory.files) |file| {
            if (file.row_groups.len != 0) return true;
        }
        return false;
    }

    fn normalizedFooterDiscoveryError(err: anyerror) anyerror {
        return normalizeLakeFooterDiscoveryError(err);
    }
};

pub fn normalizeLakeFooterDiscoveryError(err: anyerror) anyerror {
    return switch (err) {
        error.FileNotFound => error.ExternalLakeSnapshotMismatch,
        error.InvalidParquetFooter,
        error.InvalidParquetFooterMagic,
        error.InvalidParquetMetadata,
        error.ParquetInventoryFileNotFound,
        => error.InvalidParquetRowGroupBatch,
        else => err,
    };
}

fn lakeExpressionAggregateProjectedColumnsAlloc(
    alloc: std.mem.Allocator,
    expressions: []const serverless_algebraic_segment.ExpressionSpec,
) ![]const []const u8 {
    var columns = std.ArrayListUnmanaged([]const u8).empty;
    errdefer columns.deinit(alloc);
    for (expressions) |expression| {
        if (expression.op == .count) continue;
        if (expression.value_column.len == 0) return error.InvalidLakeRowsQuery;
        if (!stringSliceContains(columns.items, expression.value_column)) {
            try columns.append(alloc, expression.value_column);
        }
    }
    return try columns.toOwnedSlice(alloc);
}

fn stringSliceContains(values: []const []const u8, needle: []const u8) bool {
    for (values) |value| {
        if (std.mem.eql(u8, value, needle)) return true;
    }
    return false;
}

pub const PinnedExternalObjectStorageLakeRowsSource = struct {
    scanner: PinnedExternalObjectStorageLakeRowsScanner,

    pub fn init(
        inventory: external_source_api.Inventory,
        client: object_storage_api.ObjectStorage,
    ) PinnedExternalObjectStorageLakeRowsSource {
        return .{
            .scanner = PinnedExternalObjectStorageLakeRowsScanner.init(inventory, client),
        };
    }

    pub fn source(self: *@This()) TableReadSource {
        return .{
            .ptr = self,
            .vtable = &.{
                .lookup = lookup,
                .scan = scan,
                .query = query,
                .rows_query_plan = rowsQueryPlan,
                .rows_aggregate_plan = rowsAggregatePlan,
                .rows_window_plan = rowsWindowPlan,
                .rows_join_plan = rowsJoinPlan,
                .rows_lateral_plan = rowsLateralPlan,
                .lake_rows_scan = lakeRowsScan,
                .lake_rows_expression_aggregates = lakeRowsExpressionAggregates,
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
    ) !?LookupResponse {
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
    ) !?ScanResponse {
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

    fn rowsQueryPlan(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        runtime_schema: storage_schema.TableSchema,
        plan: db_mod.types.RelationalRowsQueryPlan,
        consistency: raft_mod.ReadConsistency,
    ) !?db_mod.types.RelationalRowsQueryResult {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return try rowsQueryPlanFromLakeScanAlloc(alloc, self.source(), table_name, runtime_schema, plan, consistency);
    }

    fn rowsAggregatePlan(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        runtime_schema: storage_schema.TableSchema,
        plan: db_mod.types.RelationalRowsAggregatePlan,
        consistency: raft_mod.ReadConsistency,
    ) !?db_mod.types.RelationalRowsAggregateResult {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return try rowsAggregatePlanFromLakeScanAlloc(alloc, self.source(), table_name, runtime_schema, plan, consistency);
    }

    fn rowsWindowPlan(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        runtime_schema: storage_schema.TableSchema,
        plan: db_mod.types.RelationalRowsWindowPlan,
        consistency: raft_mod.ReadConsistency,
    ) !?db_mod.types.RelationalRowsWindowResult {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return try rowsWindowPlanFromLakeScanAlloc(alloc, self.source(), table_name, runtime_schema, plan, consistency);
    }

    fn rowsJoinPlan(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        runtime_schema: storage_schema.TableSchema,
        plan: db_mod.types.RelationalRowsJoinPlan,
        consistency: raft_mod.ReadConsistency,
    ) !?db_mod.types.RelationalRowsJoinResult {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return try rowsJoinPlanFromLakeScanAlloc(alloc, self.source(), table_name, runtime_schema, plan, consistency);
    }

    fn rowsLateralPlan(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        runtime_schema: storage_schema.TableSchema,
        plan: db_mod.types.RelationalRowsLateralPlan,
        consistency: raft_mod.ReadConsistency,
    ) !?db_mod.types.RelationalRowsJoinResult {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return try rowsLateralPlanFromLakeScanAlloc(alloc, self.source(), table_name, runtime_schema, plan, consistency);
    }

    fn lakeRowsScan(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        _: []const u8,
        runtime_schema: storage_schema.TableSchema,
        request: serverless_query.LakeRowsScanRequest,
        _: raft_mod.ReadConsistency,
    ) !?serverless_query.LakeRowsScanResult {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return try self.scanner.scanAlloc(alloc, runtime_schema, request);
    }

    fn lakeRowsExpressionAggregates(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        _: []const u8,
        runtime_schema: storage_schema.TableSchema,
        request: serverless_query.LakeRowsExpressionAggregateRequest,
        _: raft_mod.ReadConsistency,
    ) !?serverless_query.LakeRowsExpressionAggregateResult {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return try self.scanner.expressionAggregatesAlloc(alloc, runtime_schema, request);
    }
};

pub fn executePinnedExternalLakeRowsScanAlloc(
    alloc: std.mem.Allocator,
    runtime_schema: storage_schema.TableSchema,
    inventory: external_source_api.Inventory,
    reader: serverless_query.LakeParquetObjectRangeReader,
    cache: ?*serverless_query.LakeParquetObjectRangeCache,
    coalesce_options: serverless_query.LakeRangeCoalesceOptions,
    sidecar_context: PinnedExternalLakeSidecarContext,
    request: serverless_query.LakeRowsScanRequest,
) !serverless_query.LakeRowsScanResult {
    if (runtime_schema.storage_mode != .relational) return error.InvalidRowsRequest;
    const external_base_source = runtime_schema.external_base_source orelse return error.InvalidRowsRequest;
    const binding = external_binding_api.bindingFromRuntimeExternalBaseSource(external_base_source);
    if (binding.format != .parquet and binding.format != .iceberg) return error.UnsupportedRowsQuery;

    return try serverless_query.queryLakeParquetSupportedI64ObjectRangeRowsAlloc(alloc, .{
        .binding = binding,
        .reader = reader,
        .cache = cache,
        .inventory = inventory,
        .projected_columns = request.projected_columns,
        .predicate = request.predicate,
        .limit = request.limit,
        .deleted_row_refs = request.deleted_row_refs,
        .coalesce_options = coalesce_options,
        .sidecars = sidecar_context.sidecars,
        .desired_sidecars = sidecar_context.desired_sidecars,
        .sidecar_policy = sidecar_context.sidecar_policy,
        .candidates = sidecar_context.candidates,
    });
}

pub fn rowsQueryPlanFromLakeScanAlloc(
    alloc: std.mem.Allocator,
    source: TableReadSource,
    table_name: []const u8,
    runtime_schema: storage_schema.TableSchema,
    plan: db_mod.types.RelationalRowsQueryPlan,
    consistency: raft_mod.ReadConsistency,
) !?db_mod.types.RelationalRowsQueryResult {
    if (plan.ctes.len != 0 or plan.ranges.len != 0) {
        var base_rows = (try fullRowsFromLakeScanAlloc(alloc, source, table_name, runtime_schema, consistency)) orelse return null;
        defer base_rows.deinit(alloc);
        return try relational_rows_api.executeRowsQueryPlanOnJsonRowsAlloc(alloc, runtime_schema, plan, base_rows.rows);
    }
    var lake_request = try relational_rows_api.buildLakeRowsScanRequestForRowsQueryAlloc(alloc, runtime_schema, plan.query);
    defer lake_request.deinit(alloc);

    var scan_result = (try source.lakeRowsScan(alloc, table_name, runtime_schema, lake_request.request, consistency)) orelse return null;
    defer scan_result.deinit(alloc);
    return try relational_rows_api.buildRowsQueryResultFromLakeRowsWithSchemaAlloc(alloc, runtime_schema, plan.query, scan_result);
}

pub fn rowsAggregatePlanFromLakeScanAlloc(
    alloc: std.mem.Allocator,
    source: TableReadSource,
    table_name: []const u8,
    runtime_schema: storage_schema.TableSchema,
    plan: db_mod.types.RelationalRowsAggregatePlan,
    consistency: raft_mod.ReadConsistency,
) !?db_mod.types.RelationalRowsAggregateResult {
    if (plan.ctes.len != 0 or plan.ranges.len != 0) {
        var base_rows = (try fullRowsFromLakeScanAlloc(alloc, source, table_name, runtime_schema, consistency)) orelse return null;
        defer base_rows.deinit(alloc);
        return try relational_rows_api.executeRowsAggregatePlanOnJsonRowsAlloc(alloc, runtime_schema, plan, base_rows.rows);
    }
    var maybe_expression_request = try relational_rows_api.buildLakeRowsExpressionAggregateRequestForRowsAggregateAlloc(alloc, plan.aggregate);
    if (maybe_expression_request) |*expression_request| {
        defer expression_request.deinit(alloc);
        const expression_result = source.lakeRowsExpressionAggregates(alloc, table_name, runtime_schema, expression_request.request, consistency) catch |err| switch (err) {
            error.UnsupportedOperation,
            error.UnsupportedRowsQuery,
            error.UnsupportedLakeRowsExpressionAggregate,
            error.EmptyLakeRowsExpressionAggregate,
            => null,
            else => return err,
        };
        if (expression_result) |value| {
            var owned_value = value;
            defer owned_value.deinit(alloc);
            return try relational_rows_api.buildRowsAggregateResultFromLakeRowsExpressionAggregatesAlloc(alloc, plan.aggregate, owned_value);
        }
    }

    var lake_request = try relational_rows_api.buildLakeRowsScanRequestForRowsAggregateAlloc(alloc, runtime_schema, plan.aggregate);
    defer lake_request.deinit(alloc);

    var scan_result = (try source.lakeRowsScan(alloc, table_name, runtime_schema, lake_request.request, consistency)) orelse return null;
    defer scan_result.deinit(alloc);
    return try relational_rows_api.buildRowsAggregateResultFromLakeRowsAlloc(alloc, plan.aggregate, scan_result);
}

pub fn rowsWindowPlanFromLakeScanAlloc(
    alloc: std.mem.Allocator,
    source: TableReadSource,
    table_name: []const u8,
    runtime_schema: storage_schema.TableSchema,
    plan: db_mod.types.RelationalRowsWindowPlan,
    consistency: raft_mod.ReadConsistency,
) !?db_mod.types.RelationalRowsWindowResult {
    if (plan.ctes.len != 0 or plan.ranges.len != 0) return error.UnsupportedRowsQuery;
    var base_rows = (try fullRowsFromLakeScanAlloc(alloc, source, table_name, runtime_schema, consistency)) orelse return null;
    defer base_rows.deinit(alloc);
    return try relational_rows_api.executeRowsWindowPlanOnJsonRowsAlloc(alloc, runtime_schema, plan, base_rows.rows);
}

pub fn rowsJoinPlanFromLakeScanAlloc(
    alloc: std.mem.Allocator,
    source: TableReadSource,
    table_name: []const u8,
    runtime_schema: storage_schema.TableSchema,
    plan: db_mod.types.RelationalRowsJoinPlan,
    consistency: raft_mod.ReadConsistency,
) !?db_mod.types.RelationalRowsJoinResult {
    if (!relationalRowsPlanUsesDefaultSideTables(table_name, plan.left_table, plan.right_table)) return error.UnsupportedRowsQuery;
    var base_rows = (try fullRowsFromLakeScanAlloc(alloc, source, table_name, runtime_schema, consistency)) orelse return null;
    defer base_rows.deinit(alloc);
    return try relational_rows_api.executeRowsJoinPlanOnJsonRowsWithSchemasAlloc(
        alloc,
        runtime_schema,
        runtime_schema,
        runtime_schema,
        plan,
        base_rows.rows,
        base_rows.rows,
        base_rows.rows,
    );
}

pub fn rowsLateralPlanFromLakeScanAlloc(
    alloc: std.mem.Allocator,
    source: TableReadSource,
    table_name: []const u8,
    runtime_schema: storage_schema.TableSchema,
    plan: db_mod.types.RelationalRowsLateralPlan,
    consistency: raft_mod.ReadConsistency,
) !?db_mod.types.RelationalRowsJoinResult {
    if (!relationalRowsPlanUsesDefaultSideTables(table_name, plan.left_table, plan.right_table)) return error.UnsupportedRowsQuery;
    var base_rows = (try fullRowsFromLakeScanAlloc(alloc, source, table_name, runtime_schema, consistency)) orelse return null;
    defer base_rows.deinit(alloc);
    return try relational_rows_api.executeRowsLateralPlanOnJsonRowsWithSchemasAlloc(
        alloc,
        runtime_schema,
        runtime_schema,
        runtime_schema,
        plan,
        base_rows.rows,
        base_rows.rows,
        base_rows.rows,
    );
}

pub fn fullRowsFromLakeScanAlloc(
    alloc: std.mem.Allocator,
    source: TableReadSource,
    table_name: []const u8,
    runtime_schema: storage_schema.TableSchema,
    consistency: raft_mod.ReadConsistency,
) !?db_mod.types.RelationalRowsQueryResult {
    const full_source = db_mod.types.RelationalRowsQueryRequest{ .select_all = true };
    var lake_request = try relational_rows_api.buildLakeRowsScanRequestForRowsQueryAlloc(alloc, runtime_schema, full_source);
    defer lake_request.deinit(alloc);

    var scan_result = (try source.lakeRowsScan(alloc, table_name, runtime_schema, lake_request.request, consistency)) orelse return null;
    defer scan_result.deinit(alloc);
    return try relational_rows_api.buildRowsQueryResultFromLakeRowsWithSchemaAlloc(alloc, runtime_schema, full_source, scan_result);
}

pub fn relationalRowsPlanUsesDefaultSideTables(default_table_name: []const u8, left_table_name: []const u8, right_table_name: []const u8) bool {
    return (left_table_name.len == 0 or std.mem.eql(u8, left_table_name, default_table_name)) and
        (right_table_name.len == 0 or std.mem.eql(u8, right_table_name, default_table_name));
}

test "external lake row plan helpers execute through lake source hooks" {
    const alloc = std.testing.allocator;
    var columns = [_]storage_schema.RelationalColumn{
        .{ .name = "id", .path = "id", .field_type = .keyword, .nullable = false },
        .{ .name = "amount", .path = "amount", .field_type = .numeric, .nullable = false },
    };
    const schema = storage_schema.TableSchema{
        .storage_mode = .relational,
        .relational_columns = columns[0..],
        .primary_key = .{ .columns = &.{"id"} },
        .external_base_source = .{
            .table_id = "events",
            .format = .parquet,
            .source_uri = "s3://bucket/events",
            .snapshot_mode = .{ .object_version_digest = "digest-1" },
            .schema_fingerprint = "schema-v1",
        },
    };

    const FakeLakeSource = struct {
        scan_calls: usize = 0,
        expression_aggregate_calls: usize = 0,

        fn source(self: *@This()) TableReadSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .lookup = lookup,
                    .scan = scan,
                    .query = query,
                    .lake_rows_scan = lakeRowsScan,
                    .lake_rows_expression_aggregates = lakeRowsExpressionAggregates,
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
            return error.UnexpectedLookupForLakeRowsTest;
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
            return error.UnexpectedDocumentScanForLakeRowsTest;
        }

        fn query(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: db_mod.types.SearchRequest,
            _: raft_mod.ReadConsistency,
        ) !?query_api.QueryResponse {
            return error.UnexpectedSearchForLakeRowsTest;
        }

        fn lakeRowsScan(
            ptr: *anyopaque,
            scan_alloc: std.mem.Allocator,
            table_name: []const u8,
            runtime_schema: storage_schema.TableSchema,
            request: serverless_query.LakeRowsScanRequest,
            consistency: raft_mod.ReadConsistency,
        ) !?serverless_query.LakeRowsScanResult {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqualStrings("events", table_name);
            try std.testing.expect(runtime_schema.external_base_source != null);
            try std.testing.expectEqual(raft_mod.ReadConsistency.read_index, consistency);
            self.scan_calls += 1;

            const rows = try scan_alloc.alloc(serverless_query.lake_rows.ProjectedRow, 2);
            errdefer scan_alloc.free(rows);
            rows[0] = try buildRow(scan_alloc, "row:1", request.projected_columns, 20);
            errdefer rows[0].deinit(scan_alloc);
            rows[1] = try buildRow(scan_alloc, "row:2", request.projected_columns, 30);
            return .{ .rows = rows, .total = 2 };
        }

        fn lakeRowsExpressionAggregates(
            ptr: *anyopaque,
            expression_alloc: std.mem.Allocator,
            table_name: []const u8,
            runtime_schema: storage_schema.TableSchema,
            request: serverless_query.LakeRowsExpressionAggregateRequest,
            consistency: raft_mod.ReadConsistency,
        ) !?serverless_query.LakeRowsExpressionAggregateResult {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqualStrings("events", table_name);
            try std.testing.expect(runtime_schema.external_base_source != null);
            try std.testing.expectEqual(raft_mod.ReadConsistency.read_index, consistency);
            self.expression_aggregate_calls += 1;

            const expressions = try expression_alloc.alloc(serverless_query.lake_rows.ExpressionResult, request.expressions.len);
            errdefer expression_alloc.free(expressions);
            var initialized: usize = 0;
            errdefer {
                for (expressions[0..initialized]) |*expression| expression.deinit(expression_alloc);
            }
            for (request.expressions, expressions) |spec, *out| {
                out.* = .{
                    .name = try expression_alloc.dupe(u8, spec.name),
                    .value = switch (spec.op) {
                        .count => .{ .count = 2 },
                        .sum_i64 => .{ .sum_i64 = 50 },
                        .min_i64 => .{ .min_i64 = 20 },
                        .max_i64 => .{ .max_i64 = 30 },
                        .avg_i64 => .{ .avg_i64 = .{ .sum_i64 = 50, .count = 2 } },
                    },
                };
                initialized += 1;
            }
            return .{ .expressions = expressions, .source = .rowsource_scan };
        }

        fn buildRow(
            row_alloc: std.mem.Allocator,
            row_key: []const u8,
            projected_columns: []const []const u8,
            amount: i64,
        ) !serverless_query.lake_rows.ProjectedRow {
            const cells = try row_alloc.alloc(serverless_query.lake_rows.ProjectedCell, projected_columns.len);
            errdefer row_alloc.free(cells);
            var initialized: usize = 0;
            errdefer {
                for (cells[0..initialized]) |*cell| cell.deinit(row_alloc);
            }
            for (projected_columns, cells) |column, *cell| {
                cell.* = .{
                    .name = try row_alloc.dupe(u8, column),
                    .value = if (std.mem.eql(u8, column, "id"))
                        .{ .bytes = try row_alloc.dupe(u8, if (amount == 20) "a" else "b") }
                    else if (std.mem.eql(u8, column, "amount"))
                        .{ .i64 = amount }
                    else
                        return error.UnexpectedLakeProjection,
                };
                initialized += 1;
            }
            return .{
                .row_ref = .{ .relational_key = row_key },
                .cells = cells,
            };
        }
    };

    var fake = FakeLakeSource{};
    const source = fake.source();
    const select = [_][]const u8{"amount"};
    var query_result = (try rowsQueryPlanFromLakeScanAlloc(alloc, source, "events", schema, .{
        .query = .{
            .select = select[0..],
            .select_all = false,
            .limit = 2,
        },
    }, .read_index)).?;
    defer query_result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), fake.scan_calls);
    try std.testing.expectEqual(@as(u32, 2), query_result.total);
    try std.testing.expectEqualStrings("{\"amount\":20}", query_result.rows[0]);
    try std.testing.expectEqualStrings("{\"amount\":30}", query_result.rows[1]);

    const aggregations = [_]db_mod.types.RelationalRowsAggregateSpec{
        .{ .name = "count_all", .op = .count },
        .{ .name = "sum_amount", .op = .sum, .field = "amount" },
    };
    var aggregate_result = (try rowsAggregatePlanFromLakeScanAlloc(alloc, source, "events", schema, .{
        .aggregate = .{
            .aggregations = aggregations[0..],
        },
    }, .read_index)).?;
    defer aggregate_result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), fake.scan_calls);
    try std.testing.expectEqual(@as(usize, 1), fake.expression_aggregate_calls);
    try std.testing.expectEqual(@as(u32, 1), aggregate_result.total_groups);
    try std.testing.expectEqualStrings("{\"count_all\":2,\"sum_amount\":50}", aggregate_result.rows[0]);
}

test "pinned external lake rows scanner validates schema binding against inventory" {
    const alloc = std.testing.allocator;
    var columns = [_]storage_schema.RelationalColumn{
        .{ .name = "amount", .path = "amount", .field_type = .numeric, .nullable = false },
    };
    const schema = storage_schema.TableSchema{
        .storage_mode = .relational,
        .relational_columns = columns[0..],
        .external_base_source = .{
            .table_id = "events",
            .format = .parquet,
            .source_uri = "s3://bucket/events",
            .snapshot_mode = .{ .object_version_digest = "sha256:expected" },
            .schema_fingerprint = "schema-v1",
        },
    };

    var inventory = external_source_api.Inventory{
        .format = .parquet,
        .source_id = try alloc.dupe(u8, "events"),
        .source_uri = try alloc.dupe(u8, "s3://bucket/events"),
        .snapshot_id = try alloc.dupe(u8, "sha256:stale"),
        .schema_fingerprint = try alloc.dupe(u8, "schema-v1"),
        .files = try alloc.alloc(external_source_api.types.FileEntry, 1),
    };
    defer inventory.deinit(alloc);
    inventory.files[0] = .{
        .file_id = try alloc.dupe(u8, "part-a.parquet"),
        .object_uri = try alloc.dupe(u8, "s3://bucket/events/part-a.parquet"),
        .etag = try alloc.dupe(u8, "etag-a"),
        .byte_len = 128,
        .row_count = 0,
        .row_groups = &.{},
    };

    const NeverRead = struct {
        fn readRangeAlloc(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: []const u8,
            _: u64,
            _: usize,
        ) ![]u8 {
            return error.UnexpectedLakeRangeRead;
        }
    };
    var ctx: u8 = 0;
    const reader = serverless_query.LakeParquetObjectRangeReader{
        .ctx = &ctx,
        .read_range_alloc = NeverRead.readRangeAlloc,
    };
    const projection = [_][]const u8{"amount"};

    try std.testing.expectError(
        error.ExternalLakeSnapshotMismatch,
        executePinnedExternalLakeRowsScanAlloc(
            alloc,
            schema,
            inventory,
            reader,
            null,
            .{},
            .{},
            .{ .projected_columns = projection[0..] },
        ),
    );
}

test "pinned external lake rows scanner forwards sidecar freshness policy" {
    const alloc = std.testing.allocator;
    var columns = [_]storage_schema.RelationalColumn{
        .{ .name = "amount", .path = "amount", .field_type = .numeric, .nullable = false },
    };
    const schema = storage_schema.TableSchema{
        .storage_mode = .relational,
        .relational_columns = columns[0..],
        .external_base_source = .{
            .table_id = "events",
            .format = .parquet,
            .source_uri = "s3://bucket/events",
            .snapshot_mode = .{ .object_version_digest = "sha256:expected" },
            .schema_fingerprint = "schema-v1",
        },
    };

    var inventory = external_source_api.Inventory{
        .format = .parquet,
        .source_id = try alloc.dupe(u8, "events"),
        .source_uri = try alloc.dupe(u8, "s3://bucket/events"),
        .snapshot_id = try alloc.dupe(u8, "sha256:expected"),
        .schema_fingerprint = try alloc.dupe(u8, "schema-v1"),
        .files = try alloc.alloc(external_source_api.types.FileEntry, 1),
    };
    defer inventory.deinit(alloc);
    inventory.files[0] = .{
        .file_id = try alloc.dupe(u8, "part-a.parquet"),
        .object_uri = try alloc.dupe(u8, "s3://bucket/events/part-a.parquet"),
        .etag = try alloc.dupe(u8, "etag-a"),
        .byte_len = 128,
        .row_count = 1,
        .row_groups = try alloc.dupe(external_source_api.types.RowGroup, &[_]external_source_api.types.RowGroup{.{
            .ordinal = 0,
            .row_count = 1,
            .file_offset = 0,
            .total_byte_len = 1,
            .column_chunks = try alloc.dupe(external_source_api.types.ColumnChunk, &[_]external_source_api.types.ColumnChunk{.{
                .column_id = try alloc.dupe(u8, "amount"),
                .file_offset = 0,
                .compressed_len = 1,
                .uncompressed_len = 1,
                .encoding = try alloc.dupe(u8, "plain"),
                .physical_type = try alloc.dupe(u8, "int64"),
            }}),
        }}),
    };

    const NeverRead = struct {
        fn readRangeAlloc(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: []const u8,
            _: u64,
            _: usize,
        ) ![]u8 {
            return error.UnexpectedLakeRangeRead;
        }
    };
    var ctx: u8 = 0;
    const reader = serverless_query.LakeParquetObjectRangeReader{
        .ctx = &ctx,
        .read_range_alloc = NeverRead.readRangeAlloc,
    };

    const stale_sidecars = [_]sidecar_manifest_api.DeclaredArtifact{.{
        .name = "events.amount.vector",
        .binding = .{
            .sidecar_kind = .vector,
            .source_kind = .external_parquet,
            .row_ref_kind = .external,
            .source_id = "events",
            .snapshot_id = "sha256:old",
            .schema_fingerprint = "schema-v1",
            .column_bindings = &[_][]const u8{"amount"},
            .index_config_hash = "sha256:vector",
        },
        .artifact = .{
            .kind = artifact_ref_api.ArtifactKind.vector_segment,
            .name = "events.amount.vector",
            .artifact_id = "artifact:vector:old",
            .byte_len = 1,
            .checksum = "sha256:artifact",
        },
    }};
    const desired = [_]serverless_query.LakeSidecarDesired{.{ .kind = source_binding_api.SidecarKind.vector }};
    const projection = [_][]const u8{"amount"};

    try std.testing.expectError(
        error.StaleLakeSidecar,
        executePinnedExternalLakeRowsScanAlloc(
            alloc,
            schema,
            inventory,
            reader,
            null,
            .{},
            .{
                .sidecars = stale_sidecars[0..],
                .desired_sidecars = desired[0..],
            },
            .{ .projected_columns = projection[0..] },
        ),
    );
}
