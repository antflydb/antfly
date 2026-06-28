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
const catalog_resources = @import("../catalog_resources.zig");
const document_sql_runtime = @import("document_sql_runtime.zig");
const distributed_graph = @import("../distributed_graph.zig");
const runtime_status = @import("../runtime_status.zig");
const query_api = @import("../query.zig");
const relational_rows_api = @import("../../sql/relational_rows.zig");
const common_secrets = @import("../../common/secrets.zig");
const common_config = @import("../../common/config.zig");
const external_binding_api = @import("../../serverless/external_source/catalog_binding.zig");
const external_source_api = @import("../../serverless/external_source/mod.zig");
const artifact_ref_api = @import("../../serverless/manifest/artifact_ref.zig");
const sidecar_manifest_api = @import("../../serverless/segment/sidecar_manifest.zig");
const source_binding_api = @import("../../serverless/segment/source_binding.zig");
const object_store_support = @import("../../serverless/object_store_support.zig");
const configured_object_store_support = @import("../../serverless/configured_object_store_support.zig");
const serverless_algebraic_segment = @import("../../serverless/algebraic_segment/mod.zig");
const serverless_query = @import("../../serverless/query/mod.zig");
const rowsource_api = @import("../../storage/rowsource/types.zig");
const object_storage_api = @import("../../storage/object_storage.zig");
const table_read_core = @import("core.zig");

const TableReadSource = table_read_core.TableReadSource;
const LookupResponse = table_read_core.LookupResponse;
const ScanResponse = table_read_core.ScanResponse;
const LsmStorageStats = table_read_core.LsmStorageStats;
const table_read_relational_rows = @import("relational_rows.zig");

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

pub const ExternalObjectStorageLakeRowsSourceOptions = struct {
    cache: ?*serverless_query.LakeParquetObjectRangeCache = null,
    serving_cache_max_bytes: ?usize = null,
    persistent_cache_root_dir: ?[]const u8 = null,
    coalesce_options: serverless_query.LakeRangeCoalesceOptions = .{},
    sidecar_context: PinnedExternalLakeSidecarContext = .{},
    file_bucket: []const u8 = "external-lake",
    object_uri_base: ?[]const u8 = null,
};

pub const ExternalLakePinnedSourceState = struct {
    format: external_source_api.Format,
    source_uri: []const u8,
    snapshot_id: []const u8,
    schema_fingerprint: []const u8,
    file_count: usize,
    row_group_count: usize,
    row_count: u64,
    byte_len: u64,

    pub fn fromInventory(inventory: external_source_api.Inventory) ExternalLakePinnedSourceState {
        var row_group_count: usize = 0;
        var row_count: u64 = 0;
        var byte_len: u64 = 0;
        for (inventory.files) |file| {
            byte_len +|= file.byte_len;
            row_count +|= file.row_count;
            row_group_count +|= file.row_groups.len;
        }
        return .{
            .format = inventory.format,
            .source_uri = inventory.source_uri,
            .snapshot_id = inventory.snapshot_id,
            .schema_fingerprint = inventory.schema_fingerprint,
            .file_count = inventory.files.len,
            .row_group_count = row_group_count,
            .row_count = row_count,
            .byte_len = byte_len,
        };
    }
};

pub const ExternalLakePhysicalScanSummary = struct {
    logical_read_count: usize = 0,
    physical_read_count: usize = 0,
    logical_read_bytes: u64 = 0,
    physical_read_bytes: u64 = 0,
    footer_read_count: usize = 0,
    column_chunk_read_count: usize = 0,
};

pub const OwnedExternalObjectStorageLakeRowsSource = struct {
    alloc: std.mem.Allocator,
    inventory: external_source_api.Inventory,
    pinned_source: PinnedExternalObjectStorageLakeRowsSource,
    owned_cache: ?*serverless_query.LakeParquetObjectRangeCache = null,
    owned_persistent_cache: ?*serverless_query.LakeParquetPersistentObjectRangeCache = null,

    pub fn initExternalLakeAlloc(
        alloc: std.mem.Allocator,
        runtime_schema: storage_schema.TableSchema,
        client: object_storage_api.ObjectStorage,
        bucket: []const u8,
        prefix: []const u8,
        options: ExternalObjectStorageLakeRowsSourceOptions,
    ) !OwnedExternalObjectStorageLakeRowsSource {
        if (runtime_schema.storage_mode != .relational) return error.InvalidRowsRequest;
        const external_base_source = runtime_schema.external_base_source orelse return error.InvalidRowsRequest;
        const binding = external_binding_api.bindingFromRuntimeExternalBaseSource(external_base_source);
        var source_options = options;
        const owned_cache = try initOwnedServingCacheAlloc(alloc, source_options);
        errdefer if (owned_cache) |cache| {
            cache.deinit(alloc);
            alloc.destroy(cache);
        };
        const owned_persistent_cache = try initOwnedPersistentRangeCacheAlloc(alloc, source_options);
        errdefer if (owned_persistent_cache) |persistent_cache| alloc.destroy(persistent_cache);
        if (owned_cache) |cache| {
            if (owned_persistent_cache) |persistent_cache| cache.persistent = persistent_cache;
        }
        if (source_options.cache == null) source_options.cache = owned_cache;

        var iceberg_delete_plan: ?serverless_query.LakeIcebergDeletePlan = null;
        errdefer if (iceberg_delete_plan) |*delete_plan| delete_plan.deinit(alloc);

        var inventory = switch (binding.format) {
            .parquet => try external_source_api.planParquetPrefixInventoryFromObjectStorageAlloc(alloc, .{
                .client = client,
                .bucket = bucket,
                .prefix = prefix,
                .source_id = binding.table_id,
                .source_uri = binding.source_uri,
                .object_uri_base = source_options.object_uri_base,
                .schema_fingerprint = binding.schema_fingerprint,
            }),
            .iceberg => blk: {
                const metadata_uri = try icebergMetadataUriForOpenedStoreAlloc(alloc, client, bucket, prefix, binding.source_uri, source_options.object_uri_base);
                defer alloc.free(metadata_uri);
                var snapshot = try serverless_query.readLakeIcebergSnapshotInventoryAndDeletePlanAlloc(alloc, .{
                    .client = client,
                    .source_id = binding.table_id,
                    .metadata_uri = metadata_uri,
                    .requested_snapshot_id = binding.snapshot_mode.pinnedSnapshotId(),
                    .cache = source_options.cache,
                });
                errdefer snapshot.inventory.deinit(alloc);
                iceberg_delete_plan = snapshot.delete_plan;
                try serverless_query.pinLakeIcebergInventoryDataFileObjectVersionsAlloc(alloc, client, &snapshot.inventory);
                break :blk snapshot.inventory;
            },
            .lance => return error.UnsupportedRowsQuery,
        };
        errdefer inventory.deinit(alloc);
        try serverless_query.validateLakeBindingInventory(binding, inventory);

        var pinned_source = PinnedExternalObjectStorageLakeRowsSource.init(inventory, client);
        pinned_source.scanner.cache = source_options.cache;
        pinned_source.scanner.coalesce_options = source_options.coalesce_options;
        pinned_source.scanner.sidecar_context = source_options.sidecar_context;
        pinned_source.scanner.iceberg_delete_plan = iceberg_delete_plan;
        return .{
            .alloc = alloc,
            .inventory = inventory,
            .pinned_source = pinned_source,
            .owned_cache = owned_cache,
            .owned_persistent_cache = owned_persistent_cache,
        };
    }

    pub fn initParquetPrefixAlloc(
        alloc: std.mem.Allocator,
        runtime_schema: storage_schema.TableSchema,
        client: object_storage_api.ObjectStorage,
        bucket: []const u8,
        prefix: []const u8,
        options: ExternalObjectStorageLakeRowsSourceOptions,
    ) !OwnedExternalObjectStorageLakeRowsSource {
        if (runtime_schema.storage_mode != .relational) return error.InvalidRowsRequest;
        const external_base_source = runtime_schema.external_base_source orelse return error.InvalidRowsRequest;
        const binding = external_binding_api.bindingFromRuntimeExternalBaseSource(external_base_source);
        if (binding.format != .parquet) return error.UnsupportedRowsQuery;
        return try initExternalLakeAlloc(alloc, runtime_schema, client, bucket, prefix, options);
    }

    pub fn source(self: *@This()) TableReadSource {
        return self.pinned_source.source();
    }

    pub fn pinnedState(self: *const @This()) ExternalLakePinnedSourceState {
        return ExternalLakePinnedSourceState.fromInventory(self.inventory);
    }

    pub fn rangeCacheStats(self: *const @This()) ?serverless_query.LakeParquetObjectRangeCacheStats {
        const cache = self.owned_cache orelse return null;
        return cache.statsSnapshot();
    }

    pub fn sidecarContext(self: *const @This()) PinnedExternalLakeSidecarContext {
        return self.pinned_source.scanner.sidecar_context;
    }

    pub fn planProjectedScanAlloc(
        self: *const @This(),
        alloc: std.mem.Allocator,
        runtime_schema: storage_schema.TableSchema,
        projected_columns: []const []const u8,
    ) !serverless_query.LakeScanPlan {
        const external_base_source = runtime_schema.external_base_source orelse return error.InvalidRowsRequest;
        const binding = external_binding_api.bindingFromRuntimeExternalBaseSource(external_base_source);
        return try serverless_query.planProjectedLakeScanAlloc(alloc, .{
            .binding = binding,
            .inventory = self.inventory,
            .projected_columns = projected_columns,
            .include_footer_reads = !inventoryHasAnyRowGroupMetadata(self.inventory),
            .coalesce_options = self.pinned_source.scanner.coalesce_options,
        });
    }

    pub fn explainProjectedScanSummaryAlloc(
        self: *@This(),
        alloc: std.mem.Allocator,
        runtime_schema: storage_schema.TableSchema,
        physical_columns: []const []const u8,
    ) !ExternalLakePhysicalScanSummary {
        const external_base_source = runtime_schema.external_base_source orelse return error.InvalidRowsRequest;
        const binding = external_binding_api.bindingFromRuntimeExternalBaseSource(external_base_source);

        var summary = ExternalLakePhysicalScanSummary{};
        if (inventoryHasAnyRowGroupMetadata(self.inventory) or physical_columns.len == 0) {
            var scan_plan = try serverless_query.planProjectedLakeScanAlloc(alloc, .{
                .binding = binding,
                .inventory = self.inventory,
                .projected_columns = physical_columns,
                .include_footer_reads = !inventoryHasAnyRowGroupMetadata(self.inventory),
                .coalesce_options = self.pinned_source.scanner.coalesce_options,
            });
            defer scan_plan.deinit(alloc);
            addLakeScanPlanToSummary(&summary, scan_plan);
            return summary;
        }

        var discovery_plan = try serverless_query.planProjectedLakeScanAlloc(alloc, .{
            .binding = binding,
            .inventory = self.inventory,
            .projected_columns = physical_columns,
            .include_footer_reads = true,
            .coalesce_options = self.pinned_source.scanner.coalesce_options,
        });
        defer discovery_plan.deinit(alloc);
        addLakeScanPlanToSummary(&summary, discovery_plan);

        var discovered = serverless_query.discoverLakeParquetSupportedI64ObjectRangeRowGroupsFromFootersAlloc(
            alloc,
            self.pinned_source.scanner.object_reader.parquetReader(),
            self.inventory,
            physical_columns,
            64 * 1024,
        ) catch |err| return normalizeLakeFooterDiscoveryError(err);
        defer discovered.deinit(alloc);

        var data_plan = try serverless_query.planProjectedLakeScanAlloc(alloc, .{
            .binding = binding,
            .inventory = discovered.inventory,
            .projected_columns = physical_columns,
            .coalesce_options = self.pinned_source.scanner.coalesce_options,
        });
        defer data_plan.deinit(alloc);
        addLakeScanPlanToSummary(&summary, data_plan);
        return summary;
    }

    pub fn deinit(self: *@This()) void {
        if (self.pinned_source.scanner.iceberg_delete_plan) |*delete_plan| {
            delete_plan.deinit(self.alloc);
        }
        if (self.owned_cache) |cache| {
            cache.deinit(self.alloc);
            self.alloc.destroy(cache);
        }
        if (self.owned_persistent_cache) |persistent_cache| self.alloc.destroy(persistent_cache);
        self.inventory.deinit(self.alloc);
        self.* = undefined;
    }

    fn initOwnedServingCacheAlloc(
        alloc: std.mem.Allocator,
        options: ExternalObjectStorageLakeRowsSourceOptions,
    ) !?*serverless_query.LakeParquetObjectRangeCache {
        if (options.cache != null and options.serving_cache_max_bytes != null) return error.InvalidRowsRequest;
        if (options.cache != null and options.persistent_cache_root_dir != null) return error.InvalidRowsRequest;
        const max_bytes = options.serving_cache_max_bytes orelse return null;
        if (max_bytes == 0) return error.InvalidRowsRequest;
        const cache = try alloc.create(serverless_query.LakeParquetObjectRangeCache);
        cache.* = serverless_query.initLakeParquetServingObjectRangeCache(max_bytes);
        return cache;
    }

    fn initOwnedPersistentRangeCacheAlloc(
        alloc: std.mem.Allocator,
        options: ExternalObjectStorageLakeRowsSourceOptions,
    ) !?*serverless_query.LakeParquetPersistentObjectRangeCache {
        const root_dir = options.persistent_cache_root_dir orelse return null;
        if (root_dir.len == 0) return error.InvalidRowsRequest;
        if (options.cache != null or options.serving_cache_max_bytes == null) return error.InvalidRowsRequest;
        const cache = try alloc.create(serverless_query.LakeParquetPersistentObjectRangeCache);
        cache.* = serverless_query.LakeParquetPersistentObjectRangeCache.init(root_dir);
        return cache;
    }

    fn icebergMetadataUriForOpenedStoreAlloc(
        alloc: std.mem.Allocator,
        client: object_storage_api.ObjectStorage,
        bucket: []const u8,
        prefix: []const u8,
        source_uri: []const u8,
        object_uri_base: ?[]const u8,
    ) ![]u8 {
        if (std.mem.endsWith(u8, source_uri, ".metadata.json")) return try alloc.dupe(u8, source_uri);

        const metadata_prefix = try icebergMetadataListPrefixAlloc(alloc, prefix);
        defer alloc.free(metadata_prefix);
        var storage_client = client;
        storage_client.allocator = alloc;

        if (try icebergMetadataUriFromVersionHintAlloc(alloc, &storage_client, bucket, prefix, metadata_prefix, source_uri, object_uri_base)) |metadata_uri| {
            return metadata_uri;
        }

        var best_key: ?[]u8 = null;
        defer if (best_key) |key| alloc.free(key);
        var next_token: ?[]u8 = null;
        defer if (next_token) |token| alloc.free(token);
        while (true) {
            var page = try storage_client.listObjects(bucket, .{
                .prefix = metadata_prefix,
                .recursive = true,
                .continuation_token = next_token,
                .max_keys = 1000,
            });
            defer page.deinit(alloc);

            for (page.entries) |entry| {
                if (!std.mem.endsWith(u8, entry.key, ".metadata.json")) continue;
                if (best_key == null or std.mem.order(u8, best_key.?, entry.key) == .lt) {
                    const next_best = try alloc.dupe(u8, entry.key);
                    if (best_key) |old| alloc.free(old);
                    best_key = next_best;
                }
            }

            if (page.next_continuation_token) |token| {
                const owned_next = try alloc.dupe(u8, token);
                if (next_token) |old| alloc.free(old);
                next_token = owned_next;
            } else break;
        }

        const key = best_key orelse return error.ExternalLakeSnapshotMismatch;
        const relative_key = relativeObjectKeyForPrefix(prefix, key);
        const base_uri = object_uri_base orelse source_uri;
        return try objectUriForRelativeKeyAlloc(alloc, base_uri, relative_key);
    }

    fn icebergMetadataUriFromVersionHintAlloc(
        alloc: std.mem.Allocator,
        client: *object_storage_api.ObjectStorage,
        bucket: []const u8,
        prefix: []const u8,
        metadata_prefix: []const u8,
        source_uri: []const u8,
        object_uri_base: ?[]const u8,
    ) !?[]u8 {
        const hint_key = try std.fmt.allocPrint(alloc, "{s}version-hint.text", .{metadata_prefix});
        defer alloc.free(hint_key);
        var hint = client.getObject(bucket, hint_key, .{}) catch |err| switch (err) {
            error.FileNotFound => return null,
            else => return err,
        };
        defer hint.deinit(alloc);

        const trimmed = std.mem.trim(u8, hint.body, " \t\r\n");
        if (trimmed.len == 0) return null;
        const version = std.fmt.parseUnsigned(u64, trimmed, 10) catch return null;
        const metadata_key = try std.fmt.allocPrint(alloc, "{s}v{d}.metadata.json", .{ metadata_prefix, version });
        defer alloc.free(metadata_key);
        var metadata_stat = client.statObject(bucket, metadata_key) catch |err| switch (err) {
            error.FileNotFound => return null,
            else => return err,
        };
        defer metadata_stat.deinit(alloc);

        const relative_key = relativeObjectKeyForPrefix(prefix, metadata_key);
        const base_uri = object_uri_base orelse source_uri;
        return try objectUriForRelativeKeyAlloc(alloc, base_uri, relative_key);
    }

    fn icebergMetadataListPrefixAlloc(alloc: std.mem.Allocator, prefix: []const u8) ![]u8 {
        if (prefix.len == 0) return try alloc.dupe(u8, "metadata/");
        if (std.mem.endsWith(u8, prefix, "/")) return try std.fmt.allocPrint(alloc, "{s}metadata/", .{prefix});
        return try std.fmt.allocPrint(alloc, "{s}/metadata/", .{prefix});
    }

    fn relativeObjectKeyForPrefix(prefix: []const u8, key: []const u8) []const u8 {
        if (prefix.len == 0) return key;
        if (std.mem.startsWith(u8, key, prefix)) {
            var rest = key[prefix.len..];
            if (std.mem.startsWith(u8, rest, "/")) rest = rest[1..];
            return rest;
        }
        return key;
    }

    fn objectUriForRelativeKeyAlloc(alloc: std.mem.Allocator, base_uri: []const u8, relative_key: []const u8) ![]u8 {
        if (std.mem.endsWith(u8, base_uri, "/")) return try std.fmt.allocPrint(alloc, "{s}{s}", .{ base_uri, relative_key });
        return try std.fmt.allocPrint(alloc, "{s}/{s}", .{ base_uri, relative_key });
    }

    fn inventoryHasAnyRowGroupMetadata(inventory: external_source_api.Inventory) bool {
        for (inventory.files) |file| {
            if (file.row_groups.len != 0) return true;
        }
        return false;
    }

    fn addLakeScanPlanToSummary(summary: *ExternalLakePhysicalScanSummary, scan_plan: serverless_query.LakeScanPlan) void {
        summary.logical_read_count += scan_plan.logical_reads.len;
        summary.physical_read_count += scan_plan.physical_reads.len;
        for (scan_plan.logical_reads) |read| {
            summary.logical_read_bytes +|= read.range.len;
            switch (read.purpose) {
                .parquet_footer => summary.footer_read_count += 1,
                .parquet_column_chunk => summary.column_chunk_read_count += 1,
                else => {},
            }
        }
        for (scan_plan.physical_reads) |read| {
            summary.physical_read_bytes +|= read.range.len;
        }
    }
};

pub const OpenedExternalObjectStorageLakeRowsSource = struct {
    opened_store: object_store_support.OpenedObjectStore,
    owned_source: OwnedExternalObjectStorageLakeRowsSource,

    pub fn initRemoteUriAlloc(
        alloc: std.mem.Allocator,
        runtime_schema: storage_schema.TableSchema,
        options: ExternalObjectStorageLakeRowsSourceOptions,
    ) !OpenedExternalObjectStorageLakeRowsSource {
        if (runtime_schema.storage_mode != .relational) return error.InvalidRowsRequest;
        const external_base_source = runtime_schema.external_base_source orelse return error.InvalidRowsRequest;
        const binding = external_binding_api.bindingFromRuntimeExternalBaseSource(external_base_source);
        if (binding.format != .parquet and binding.format != .iceberg) return error.UnsupportedRowsQuery;

        const opened_store = try object_store_support.OpenedObjectStore.initRemoteUri(
            alloc,
            binding.source_uri,
            options.file_bucket,
        );
        return try initWithOpenedStoreAlloc(alloc, runtime_schema, opened_store, options);
    }

    pub fn initWithOpenedStoreAlloc(
        alloc: std.mem.Allocator,
        runtime_schema: storage_schema.TableSchema,
        opened_store: object_store_support.OpenedObjectStore,
        options: ExternalObjectStorageLakeRowsSourceOptions,
    ) !OpenedExternalObjectStorageLakeRowsSource {
        var store = opened_store;
        errdefer store.deinit();

        var source_options = options;
        var owned_object_uri_base: ?[]u8 = null;
        defer if (owned_object_uri_base) |value| alloc.free(value);
        if (store.fs_client != null and source_options.object_uri_base == null) {
            owned_object_uri_base = try objectStoreUriBaseAlloc(alloc, store.bucket, store.prefix);
            source_options.object_uri_base = owned_object_uri_base.?;
        }

        var owned_source = try OwnedExternalObjectStorageLakeRowsSource.initExternalLakeAlloc(
            alloc,
            runtime_schema,
            store.client,
            store.bucket,
            store.prefix,
            source_options,
        );
        errdefer owned_source.deinit();

        return .{
            .opened_store = store,
            .owned_source = owned_source,
        };
    }

    pub fn source(self: *@This()) TableReadSource {
        return self.owned_source.source();
    }

    pub fn pinnedState(self: *const @This()) ExternalLakePinnedSourceState {
        return self.owned_source.pinnedState();
    }

    pub fn rangeCacheStats(self: *const @This()) ?serverless_query.LakeParquetObjectRangeCacheStats {
        return self.owned_source.rangeCacheStats();
    }

    pub fn sidecarContext(self: *const @This()) PinnedExternalLakeSidecarContext {
        return self.owned_source.sidecarContext();
    }

    pub fn planProjectedScanAlloc(
        self: *const @This(),
        alloc: std.mem.Allocator,
        runtime_schema: storage_schema.TableSchema,
        projected_columns: []const []const u8,
    ) !serverless_query.LakeScanPlan {
        return try self.owned_source.planProjectedScanAlloc(alloc, runtime_schema, projected_columns);
    }

    pub fn explainProjectedScanSummaryAlloc(
        self: *@This(),
        alloc: std.mem.Allocator,
        runtime_schema: storage_schema.TableSchema,
        physical_columns: []const []const u8,
    ) !ExternalLakePhysicalScanSummary {
        return try self.owned_source.explainProjectedScanSummaryAlloc(alloc, runtime_schema, physical_columns);
    }

    pub fn deinit(self: *@This()) void {
        self.owned_source.deinit();
        self.opened_store.deinit();
        self.* = undefined;
    }

    fn objectStoreUriBaseAlloc(alloc: std.mem.Allocator, bucket: []const u8, prefix: []const u8) ![]u8 {
        if (prefix.len == 0) return try std.fmt.allocPrint(alloc, "object://{s}", .{bucket});
        return try std.fmt.allocPrint(alloc, "object://{s}/{s}", .{ bucket, prefix });
    }
};

pub const ExternalLakeObjectStoreResolver = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        open_parquet_prefix: *const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            binding: external_binding_api.Binding,
            options: ExternalObjectStorageLakeRowsSourceOptions,
        ) anyerror!object_store_support.OpenedObjectStore,
    };

    pub fn openParquetPrefixAlloc(
        self: ExternalLakeObjectStoreResolver,
        alloc: std.mem.Allocator,
        binding: external_binding_api.Binding,
        options: ExternalObjectStorageLakeRowsSourceOptions,
    ) !object_store_support.OpenedObjectStore {
        return try self.vtable.open_parquet_prefix(self.ptr, alloc, binding, options);
    }
};

pub const RemoteUriExternalLakeObjectStoreResolver = struct {
    pub fn resolver(self: *@This()) ExternalLakeObjectStoreResolver {
        return .{
            .ptr = self,
            .vtable = &.{
                .open_parquet_prefix = openParquetPrefix,
            },
        };
    }

    fn openParquetPrefix(
        _: *anyopaque,
        alloc: std.mem.Allocator,
        binding: external_binding_api.Binding,
        options: ExternalObjectStorageLakeRowsSourceOptions,
    ) !object_store_support.OpenedObjectStore {
        if (binding.format != .parquet and binding.format != .iceberg) return error.UnsupportedRowsQuery;
        if (binding.credential_ref != null) return error.UnsupportedExternalLakeCredentialRef;
        return try object_store_support.OpenedObjectStore.initRemoteUri(
            alloc,
            binding.source_uri,
            options.file_bucket,
        );
    }
};

pub const ConfiguredExternalLakeObjectStoreResolver = struct {
    node_config: ?*const common_config.Config = null,
    secret_store: ?*common_secrets.FileStore = null,

    pub fn resolver(self: *@This()) ExternalLakeObjectStoreResolver {
        return .{
            .ptr = self,
            .vtable = &.{
                .open_parquet_prefix = openParquetPrefix,
            },
        };
    }

    pub fn configure(self: *@This(), node_config: ?*const common_config.Config, secret_store: ?*common_secrets.FileStore) void {
        self.node_config = node_config;
        self.secret_store = secret_store;
    }

    fn openParquetPrefix(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        binding: external_binding_api.Binding,
        options: ExternalObjectStorageLakeRowsSourceOptions,
    ) !object_store_support.OpenedObjectStore {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        if (binding.format != .parquet and binding.format != .iceberg) return error.UnsupportedRowsQuery;
        return try configured_object_store_support.openBindingObjectStoreAlloc(alloc, binding, .{
            .file_bucket = options.file_bucket,
            .node_config = self.node_config,
            .secret_store = self.secret_store,
        });
    }
};

pub const ExternalLakeRoutingTableReadSource = struct {
    base: TableReadSource,
    resolver: ExternalLakeObjectStoreResolver,
    options: ExternalObjectStorageLakeRowsSourceOptions = .{},

    pub fn init(
        base: TableReadSource,
        resolver: ExternalLakeObjectStoreResolver,
        options: ExternalObjectStorageLakeRowsSourceOptions,
    ) ExternalLakeRoutingTableReadSource {
        return .{
            .base = base,
            .resolver = resolver,
            .options = options,
        };
    }

    pub fn source(self: *@This()) TableReadSource {
        return .{
            .ptr = self,
            .vtable = &.{
                .lookup = lookup,
                .lookup_catalog = lookupCatalog,
                .scan = scan,
                .scan_catalog = scanCatalog,
                .query = query,
                .query_catalog = queryCatalog,
                .document_algebraic_aggregate = documentAlgebraicAggregate,
                .document_algebraic_aggregate_catalog = documentAlgebraicAggregateCatalog,
                .document_algebraic_aggregate_group_local = documentAlgebraicAggregateGroupLocal,
                .preflight_query = preflightQuery,
                .preflight_query_group_local = preflightQueryGroupLocal,
                .lookup_group_local = lookupGroupLocal,
                .relational_unique_owner_lookup = relationalUniqueOwnerLookup,
                .relational_temporal_unique_owner_lookup = relationalTemporalUniqueOwnerLookup,
                .relational_temporal_unique_overlap_owner_lookup = relationalTemporalUniqueOverlapOwnerLookup,
                .relational_temporal_unique_owner_lookup_group_local = relationalTemporalUniqueOwnerLookupGroupLocal,
                .relational_temporal_unique_overlap_owner_lookup_group_local = relationalTemporalUniqueOverlapOwnerLookupGroupLocal,
                .rows_query_plan = rowsQueryPlan,
                .rows_query_plan_catalog = rowsQueryPlanCatalog,
                .rows_set_operation_plan = rowsSetOperationPlan,
                .rows_set_operation_plan_catalog = rowsSetOperationPlanCatalog,
                .rows_aggregate_plan = rowsAggregatePlan,
                .lake_rows_scan = lakeRowsScan,
                .lake_rows_expression_aggregates = lakeRowsExpressionAggregates,
                .rows_window_plan = rowsWindowPlan,
                .rows_join_plan = rowsJoinPlan,
                .rows_lateral_plan = rowsLateralPlan,
                .relational_rows_source_group_local = relationalRowsSourceGroupLocal,
                .scan_group_local = scanGroupLocal,
                .query_group_local = queryGroupLocal,
                .search_result_group_local = searchResultGroupLocal,
                .text_stats_group_local = textStatsGroupLocal,
                .algebraic_partials_group_local = algebraicPartialsGroupLocal,
                .join_partition_group_local = joinPartitionGroupLocal,
                .join_rows_group_local = joinRowsGroupLocal,
                .join_unmatched_group_local = joinUnmatchedGroupLocal,
                .join_finalize_group_local = joinFinalizeGroupLocal,
                .join_job_state_group_local = joinJobStateGroupLocal,
                .graph_expand_group_local = graphExpandGroupLocal,
                .graph_hydrate_group_local = graphHydrateGroupLocal,
                .graph_edges_group_local = graphEdgesGroupLocal,
                .local_runtime_statuses = localRuntimeStatuses,
                .lsm_storage_stats = lsmStorageStats,
                .document_artifact_manifest = documentArtifactManifest,
                .document_artifact_manifests = documentArtifactManifests,
                .document_artifact_manifest_group_local = documentArtifactManifestGroupLocal,
                .document_artifact_manifests_group_local = documentArtifactManifestsGroupLocal,
            },
        };
    }

    fn openedLakeSourceAlloc(
        self: *@This(),
        alloc: std.mem.Allocator,
        runtime_schema: storage_schema.TableSchema,
    ) !OpenedExternalObjectStorageLakeRowsSource {
        if (runtime_schema.storage_mode != .relational) return error.InvalidRowsRequest;
        const external_base_source = runtime_schema.external_base_source orelse return error.InvalidRowsRequest;
        const binding = external_binding_api.bindingFromRuntimeExternalBaseSource(external_base_source);
        if (binding.format != .parquet and binding.format != .iceberg) return error.UnsupportedRowsQuery;
        const opened_store = try self.resolver.openParquetPrefixAlloc(alloc, binding, self.options);
        return try OpenedExternalObjectStorageLakeRowsSource.initWithOpenedStoreAlloc(
            alloc,
            runtime_schema,
            opened_store,
            self.options,
        );
    }

    fn lookup(ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, key: []const u8, opts: db_mod.types.LookupOptions, consistency: raft_mod.ReadConsistency) !?LookupResponse {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return try self.base.lookup(alloc, table_name, key, opts, consistency);
    }

    fn lookupCatalog(ptr: *anyopaque, alloc: std.mem.Allocator, target: catalog_resources.TableTarget, key: []const u8, opts: db_mod.types.LookupOptions, consistency: raft_mod.ReadConsistency) !?LookupResponse {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return try self.base.lookupCatalog(alloc, target, key, opts, consistency);
    }

    fn scan(ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, from_key: []const u8, to_key: []const u8, opts: db_mod.types.ScanOptions, consistency: raft_mod.ReadConsistency) !?ScanResponse {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return try self.base.scan(alloc, table_name, from_key, to_key, opts, consistency);
    }

    fn scanCatalog(ptr: *anyopaque, alloc: std.mem.Allocator, target: catalog_resources.TableTarget, from_key: []const u8, to_key: []const u8, opts: db_mod.types.ScanOptions, consistency: raft_mod.ReadConsistency) !?ScanResponse {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return try self.base.scanCatalog(alloc, target, from_key, to_key, opts, consistency);
    }

    fn query(ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, req: db_mod.types.SearchRequest, consistency: raft_mod.ReadConsistency) !?query_api.QueryResponse {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return try self.base.query(alloc, table_name, req, consistency);
    }

    fn queryCatalog(ptr: *anyopaque, alloc: std.mem.Allocator, target: catalog_resources.TableTarget, req: db_mod.types.SearchRequest, consistency: raft_mod.ReadConsistency) !?query_api.QueryResponse {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return try self.base.queryCatalog(alloc, target, req, consistency);
    }

    fn documentAlgebraicAggregate(ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, req: document_sql_runtime.AlgebraicAggregateRequest, consistency: raft_mod.ReadConsistency) !?document_sql_runtime.AlgebraicAggregateResponse {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return try self.base.documentAlgebraicAggregate(alloc, table_name, req, consistency);
    }

    fn documentAlgebraicAggregateCatalog(ptr: *anyopaque, alloc: std.mem.Allocator, target: catalog_resources.TableTarget, req: document_sql_runtime.AlgebraicAggregateRequest, consistency: raft_mod.ReadConsistency) !?document_sql_runtime.AlgebraicAggregateResponse {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return try self.base.documentAlgebraicAggregateCatalog(alloc, target, req, consistency);
    }

    fn documentAlgebraicAggregateGroupLocal(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, req: document_sql_runtime.AlgebraicAggregateRequest, consistency: raft_mod.ReadConsistency) !?document_sql_runtime.AlgebraicAggregateResponse {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return try self.base.documentAlgebraicAggregateGroupLocal(alloc, group_id, table_name, req, consistency);
    }

    fn preflightQuery(ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, req: db_mod.types.SearchRequest, consistency: raft_mod.ReadConsistency, max_work: u32) !?db_mod.RuntimePreflightSummary {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return try self.base.preflightQuery(alloc, table_name, req, consistency, max_work);
    }

    fn preflightQueryGroupLocal(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, req: db_mod.types.SearchRequest, consistency: raft_mod.ReadConsistency, max_work: u32) !?db_mod.RuntimePreflightSummary {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return try self.base.preflightQueryGroupLocal(alloc, group_id, table_name, req, consistency, max_work);
    }

    fn lookupGroupLocal(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, key: []const u8, opts: db_mod.types.LookupOptions, consistency: raft_mod.ReadConsistency) !?LookupResponse {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return try self.base.lookupGroupLocal(alloc, group_id, table_name, key, opts, consistency);
    }

    fn relationalUniqueOwnerLookup(ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, constraint_name: []const u8, encoded_value: []const u8, consistency: raft_mod.ReadConsistency) !?[]u8 {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return try self.base.relationalUniqueOwnerLookup(alloc, table_name, constraint_name, encoded_value, consistency);
    }

    fn relationalTemporalUniqueOwnerLookup(ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, constraint_name: []const u8, encoded_value: []const u8, encoded_point: []const u8, consistency: raft_mod.ReadConsistency) !?[]u8 {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return try self.base.relationalTemporalUniqueOwnerLookup(alloc, table_name, constraint_name, encoded_value, encoded_point, consistency);
    }

    fn relationalTemporalUniqueOverlapOwnerLookup(ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, constraint_name: []const u8, encoded_value: []const u8, encoded_start: []const u8, encoded_end: []const u8, consistency: raft_mod.ReadConsistency) !?[]u8 {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return try self.base.relationalTemporalUniqueOverlapOwnerLookup(alloc, table_name, constraint_name, encoded_value, encoded_start, encoded_end, consistency);
    }

    fn relationalTemporalUniqueOwnerLookupGroupLocal(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, constraint_name: []const u8, encoded_value: []const u8, encoded_point: []const u8, consistency: raft_mod.ReadConsistency) !?[]u8 {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return try self.base.relationalTemporalUniqueOwnerLookupGroupLocal(alloc, group_id, table_name, constraint_name, encoded_value, encoded_point, consistency);
    }

    fn relationalTemporalUniqueOverlapOwnerLookupGroupLocal(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, constraint_name: []const u8, encoded_value: []const u8, encoded_start: []const u8, encoded_end: []const u8, consistency: raft_mod.ReadConsistency) !?[]u8 {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return try self.base.relationalTemporalUniqueOverlapOwnerLookupGroupLocal(alloc, group_id, table_name, constraint_name, encoded_value, encoded_start, encoded_end, consistency);
    }

    fn rowsQueryPlan(ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, runtime_schema: storage_schema.TableSchema, plan: db_mod.types.RelationalRowsQueryPlan, consistency: raft_mod.ReadConsistency) !?db_mod.types.RelationalRowsQueryResult {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        if (runtime_schema.external_base_source == null) return try self.base.rowsQueryPlan(alloc, table_name, runtime_schema, plan, consistency);
        var lake_source = try self.openedLakeSourceAlloc(alloc, runtime_schema);
        defer lake_source.deinit();
        return try lake_source.source().rowsQueryPlan(alloc, table_name, runtime_schema, plan, consistency);
    }

    fn rowsQueryPlanCatalog(ptr: *anyopaque, alloc: std.mem.Allocator, target: catalog_resources.TableTarget, runtime_schema: storage_schema.TableSchema, plan: db_mod.types.RelationalRowsQueryPlan, consistency: raft_mod.ReadConsistency) !?db_mod.types.RelationalRowsQueryResult {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        if (runtime_schema.external_base_source == null) return try self.base.rowsQueryPlanCatalog(alloc, target, runtime_schema, plan, consistency);
        var lake_source = try self.openedLakeSourceAlloc(alloc, runtime_schema);
        defer lake_source.deinit();
        return try lake_source.source().rowsQueryPlan(alloc, target.table_name, runtime_schema, plan, consistency);
    }

    fn rowsSetOperationPlan(ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, runtime_schema: storage_schema.TableSchema, plan: db_mod.types.RelationalRowsSetOperationPlan, consistency: raft_mod.ReadConsistency) !?db_mod.types.RelationalRowsQueryResult {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        if (runtime_schema.external_base_source == null) return try self.base.rowsSetOperationPlan(alloc, table_name, runtime_schema, plan, consistency);
        var lake_source = try self.openedLakeSourceAlloc(alloc, runtime_schema);
        defer lake_source.deinit();

        var left = (try lake_source.source().rowsQueryPlan(alloc, table_name, runtime_schema, plan.left, consistency)) orelse return null;
        defer left.deinit(alloc);
        var right = (try lake_source.source().rowsQueryPlan(alloc, table_name, runtime_schema, plan.right, consistency)) orelse return null;
        defer right.deinit(alloc);
        return try table_read_relational_rows.executeSetOperationOnQueryResultsAlloc(alloc, plan, left.rows, right.rows);
    }

    fn rowsSetOperationPlanCatalog(ptr: *anyopaque, alloc: std.mem.Allocator, target: catalog_resources.TableTarget, runtime_schema: storage_schema.TableSchema, plan: db_mod.types.RelationalRowsSetOperationPlan, consistency: raft_mod.ReadConsistency) !?db_mod.types.RelationalRowsQueryResult {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        if (runtime_schema.external_base_source == null) return try self.base.rowsSetOperationPlanCatalog(alloc, target, runtime_schema, plan, consistency);
        var lake_source = try self.openedLakeSourceAlloc(alloc, runtime_schema);
        defer lake_source.deinit();

        var left = (try lake_source.source().rowsQueryPlan(alloc, target.table_name, runtime_schema, plan.left, consistency)) orelse return null;
        defer left.deinit(alloc);
        var right = (try lake_source.source().rowsQueryPlan(alloc, target.table_name, runtime_schema, plan.right, consistency)) orelse return null;
        defer right.deinit(alloc);
        return try table_read_relational_rows.executeSetOperationOnQueryResultsAlloc(alloc, plan, left.rows, right.rows);
    }

    fn rowsAggregatePlan(ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, runtime_schema: storage_schema.TableSchema, plan: db_mod.types.RelationalRowsAggregatePlan, consistency: raft_mod.ReadConsistency) !?db_mod.types.RelationalRowsAggregateResult {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        if (runtime_schema.external_base_source == null) return try self.base.rowsAggregatePlan(alloc, table_name, runtime_schema, plan, consistency);
        var lake_source = try self.openedLakeSourceAlloc(alloc, runtime_schema);
        defer lake_source.deinit();
        return try lake_source.source().rowsAggregatePlan(alloc, table_name, runtime_schema, plan, consistency);
    }

    fn lakeRowsScan(ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, runtime_schema: storage_schema.TableSchema, request: serverless_query.LakeRowsScanRequest, consistency: raft_mod.ReadConsistency) !?serverless_query.LakeRowsScanResult {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        if (runtime_schema.external_base_source == null) return try self.base.lakeRowsScan(alloc, table_name, runtime_schema, request, consistency);
        var lake_source = try self.openedLakeSourceAlloc(alloc, runtime_schema);
        defer lake_source.deinit();
        return try lake_source.source().lakeRowsScan(alloc, table_name, runtime_schema, request, consistency);
    }

    fn lakeRowsExpressionAggregates(ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, runtime_schema: storage_schema.TableSchema, request: serverless_query.LakeRowsExpressionAggregateRequest, consistency: raft_mod.ReadConsistency) !?serverless_query.LakeRowsExpressionAggregateResult {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        if (runtime_schema.external_base_source == null) return try self.base.lakeRowsExpressionAggregates(alloc, table_name, runtime_schema, request, consistency);
        var lake_source = try self.openedLakeSourceAlloc(alloc, runtime_schema);
        defer lake_source.deinit();
        return try lake_source.source().lakeRowsExpressionAggregates(alloc, table_name, runtime_schema, request, consistency);
    }

    fn rowsWindowPlan(ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, runtime_schema: storage_schema.TableSchema, plan: db_mod.types.RelationalRowsWindowPlan, consistency: raft_mod.ReadConsistency) !?db_mod.types.RelationalRowsWindowResult {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        if (runtime_schema.external_base_source == null) return try self.base.rowsWindowPlan(alloc, table_name, runtime_schema, plan, consistency);
        var lake_source = try self.openedLakeSourceAlloc(alloc, runtime_schema);
        defer lake_source.deinit();
        return try lake_source.source().rowsWindowPlan(alloc, table_name, runtime_schema, plan, consistency);
    }

    fn rowsJoinPlan(ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, runtime_schema: storage_schema.TableSchema, plan: db_mod.types.RelationalRowsJoinPlan, consistency: raft_mod.ReadConsistency) !?db_mod.types.RelationalRowsJoinResult {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        if (runtime_schema.external_base_source == null) return try self.base.rowsJoinPlan(alloc, table_name, runtime_schema, plan, consistency);
        var lake_source = try self.openedLakeSourceAlloc(alloc, runtime_schema);
        defer lake_source.deinit();
        return try lake_source.source().rowsJoinPlan(alloc, table_name, runtime_schema, plan, consistency);
    }

    fn rowsLateralPlan(ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, runtime_schema: storage_schema.TableSchema, plan: db_mod.types.RelationalRowsLateralPlan, consistency: raft_mod.ReadConsistency) !?db_mod.types.RelationalRowsJoinResult {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        if (runtime_schema.external_base_source == null) return try self.base.rowsLateralPlan(alloc, table_name, runtime_schema, plan, consistency);
        var lake_source = try self.openedLakeSourceAlloc(alloc, runtime_schema);
        defer lake_source.deinit();
        return try lake_source.source().rowsLateralPlan(alloc, table_name, runtime_schema, plan, consistency);
    }

    fn relationalRowsSourceGroupLocal(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, schema_json: []const u8, topology_epoch: u64, req: db_mod.types.RelationalRowsQueryRequest, doc_key_range: db_mod.types.RelationalRowsDocKeyRange, consistency: raft_mod.ReadConsistency) !?db_mod.types.RelationalRowsQueryResult {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return try self.base.relationalRowsSourceGroupLocal(alloc, group_id, table_name, schema_json, topology_epoch, req, doc_key_range, consistency);
    }

    fn scanGroupLocal(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, from_key: []const u8, to_key: []const u8, opts: db_mod.types.ScanOptions, consistency: raft_mod.ReadConsistency) !?ScanResponse {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return try self.base.scanGroupLocal(alloc, group_id, table_name, from_key, to_key, opts, consistency);
    }

    fn queryGroupLocal(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, req: db_mod.types.SearchRequest, consistency: raft_mod.ReadConsistency) !?query_api.QueryResponse {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return try self.base.queryGroupLocal(alloc, group_id, table_name, req, consistency);
    }

    fn searchResultGroupLocal(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, req: db_mod.types.SearchRequest, consistency: raft_mod.ReadConsistency) !?db_mod.types.SearchResult {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return try self.base.searchResultGroupLocal(alloc, group_id, table_name, req, consistency);
    }

    fn textStatsGroupLocal(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, body: []const u8) !?query_api.QueryResponse {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return try self.base.textStatsGroupLocal(alloc, group_id, table_name, body);
    }

    fn algebraicPartialsGroupLocal(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, body: []const u8) !?query_api.QueryResponse {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return try self.base.algebraicPartialsGroupLocal(alloc, group_id, table_name, body);
    }

    fn joinPartitionGroupLocal(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, body: []const u8) !?query_api.QueryResponse {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return try self.base.joinPartitionGroupLocal(alloc, group_id, table_name, body);
    }

    fn joinRowsGroupLocal(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, body: []const u8) !?query_api.QueryResponse {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return try self.base.joinRowsGroupLocal(alloc, group_id, table_name, body);
    }

    fn joinUnmatchedGroupLocal(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, body: []const u8) !?query_api.QueryResponse {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return try self.base.joinUnmatchedGroupLocal(alloc, group_id, table_name, body);
    }

    fn joinFinalizeGroupLocal(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, body: []const u8) !?query_api.QueryResponse {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return try self.base.joinFinalizeGroupLocal(alloc, group_id, table_name, body);
    }

    fn joinJobStateGroupLocal(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, body: []const u8) !?query_api.QueryResponse {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return try self.base.joinJobStateGroupLocal(alloc, group_id, table_name, body);
    }

    fn graphExpandGroupLocal(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, req: distributed_graph.GraphExpandRequest, consistency: raft_mod.ReadConsistency) !?distributed_graph.GraphExpandResponse {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return try self.base.graphExpandGroupLocal(alloc, group_id, table_name, req, consistency);
    }

    fn graphHydrateGroupLocal(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, req: distributed_graph.GraphHydrateRequest, consistency: raft_mod.ReadConsistency) !?distributed_graph.GraphHydrateResponse {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return try self.base.graphHydrateGroupLocal(alloc, group_id, table_name, req, consistency);
    }

    fn graphEdgesGroupLocal(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, req: distributed_graph.GraphEdgesRequest, consistency: raft_mod.ReadConsistency) !?distributed_graph.GraphEdgesResponse {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return try self.base.graphEdgesGroupLocal(alloc, group_id, table_name, req, consistency);
    }

    fn localRuntimeStatuses(ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8) !?runtime_status.LocalTableRuntimeStatuses {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return try self.base.localRuntimeStatuses(alloc, table_name);
    }

    fn lsmStorageStats(ptr: *anyopaque, table_name: []const u8) !?LsmStorageStats {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return try self.base.lsmStorageStats(table_name);
    }

    fn documentArtifactManifest(ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, doc_key: []const u8, artifact_name: []const u8, consistency: raft_mod.ReadConsistency) !?db_mod.types.DocumentArtifactManifest {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return try self.base.documentArtifactManifest(alloc, table_name, doc_key, artifact_name, consistency);
    }

    fn documentArtifactManifests(ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, doc_key: []const u8, consistency: raft_mod.ReadConsistency) !?db_mod.types.DocumentArtifactManifestList {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return try self.base.documentArtifactManifests(alloc, table_name, doc_key, consistency);
    }

    fn documentArtifactManifestGroupLocal(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, doc_key: []const u8, artifact_name: []const u8, consistency: raft_mod.ReadConsistency) !?db_mod.types.DocumentArtifactManifest {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return try self.base.documentArtifactManifestGroupLocal(alloc, group_id, table_name, doc_key, artifact_name, consistency);
    }

    fn documentArtifactManifestsGroupLocal(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, doc_key: []const u8, consistency: raft_mod.ReadConsistency) !?db_mod.types.DocumentArtifactManifestList {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return try self.base.documentArtifactManifestsGroupLocal(alloc, group_id, table_name, doc_key, consistency);
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

pub fn rowsSetOperationPlanFromLakeScanAlloc(
    alloc: std.mem.Allocator,
    source: TableReadSource,
    table_name: []const u8,
    runtime_schema: storage_schema.TableSchema,
    plan: db_mod.types.RelationalRowsSetOperationPlan,
    consistency: raft_mod.ReadConsistency,
) !?db_mod.types.RelationalRowsQueryResult {
    var left = (try rowsQueryPlanFromLakeScanAlloc(alloc, source, table_name, runtime_schema, plan.left, consistency)) orelse return null;
    defer left.deinit(alloc);
    var right = (try rowsQueryPlanFromLakeScanAlloc(alloc, source, table_name, runtime_schema, plan.right, consistency)) orelse return null;
    defer right.deinit(alloc);
    return try table_read_relational_rows.executeSetOperationOnQueryResultsAlloc(alloc, plan, left.rows, right.rows);
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

test "external lake row plans route broad relational operators through lake scan hook" {
    const alloc = std.testing.allocator;

    var columns = [_]storage_schema.RelationalColumn{
        .{ .name = "id", .path = "id", .field_type = .keyword, .nullable = false },
        .{ .name = "tenant", .path = "tenant", .field_type = .keyword, .nullable = false },
        .{ .name = "amount", .path = "amount", .field_type = .numeric, .nullable = false },
        .{ .name = "attrs", .path = "attrs", .field_type = .json, .nullable = false },
        .{ .name = "tags", .path = "tags", .field_type = .array, .nullable = false },
        .{ .name = "nickname", .path = "nickname", .field_type = .keyword, .nullable = true },
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
        lake_scan_calls: usize = 0,
        lake_expression_aggregate_calls: usize = 0,
        routed_scan_calls: usize = 0,
        last_scan_limit: ?usize = null,

        fn source(self: *@This()) TableReadSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .lookup = lookup,
                    .scan = scan,
                    .query = query,
                    .rows_query_plan = rowsQueryPlan,
                    .rows_set_operation_plan = rowsSetOperationPlan,
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
            return null;
        }

        fn query(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: db_mod.types.SearchRequest,
            _: raft_mod.ReadConsistency,
        ) !?query_api.QueryResponse {
            return null;
        }

        fn scan(
            ptr: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: []const u8,
            _: []const u8,
            _: db_mod.types.ScanOptions,
            _: raft_mod.ReadConsistency,
        ) !?ScanResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.routed_scan_calls += 1;
            return error.UnexpectedRoutedScanForLakeTable;
        }

        fn rowsQueryPlan(
            ptr: *anyopaque,
            plan_alloc: std.mem.Allocator,
            table_name: []const u8,
            runtime_schema: storage_schema.TableSchema,
            plan: db_mod.types.RelationalRowsQueryPlan,
            consistency: raft_mod.ReadConsistency,
        ) !?db_mod.types.RelationalRowsQueryResult {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return try rowsQueryPlanFromLakeScanAlloc(plan_alloc, self.source(), table_name, runtime_schema, plan, consistency);
        }

        fn rowsSetOperationPlan(
            ptr: *anyopaque,
            plan_alloc: std.mem.Allocator,
            table_name: []const u8,
            runtime_schema: storage_schema.TableSchema,
            plan: db_mod.types.RelationalRowsSetOperationPlan,
            consistency: raft_mod.ReadConsistency,
        ) !?db_mod.types.RelationalRowsQueryResult {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return try rowsSetOperationPlanFromLakeScanAlloc(plan_alloc, self.source(), table_name, runtime_schema, plan, consistency);
        }

        fn rowsAggregatePlan(
            ptr: *anyopaque,
            plan_alloc: std.mem.Allocator,
            table_name: []const u8,
            runtime_schema: storage_schema.TableSchema,
            plan: db_mod.types.RelationalRowsAggregatePlan,
            consistency: raft_mod.ReadConsistency,
        ) !?db_mod.types.RelationalRowsAggregateResult {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return try rowsAggregatePlanFromLakeScanAlloc(plan_alloc, self.source(), table_name, runtime_schema, plan, consistency);
        }

        fn rowsWindowPlan(
            ptr: *anyopaque,
            plan_alloc: std.mem.Allocator,
            table_name: []const u8,
            runtime_schema: storage_schema.TableSchema,
            plan: db_mod.types.RelationalRowsWindowPlan,
            consistency: raft_mod.ReadConsistency,
        ) !?db_mod.types.RelationalRowsWindowResult {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return try rowsWindowPlanFromLakeScanAlloc(plan_alloc, self.source(), table_name, runtime_schema, plan, consistency);
        }

        fn rowsJoinPlan(
            ptr: *anyopaque,
            plan_alloc: std.mem.Allocator,
            table_name: []const u8,
            runtime_schema: storage_schema.TableSchema,
            plan: db_mod.types.RelationalRowsJoinPlan,
            consistency: raft_mod.ReadConsistency,
        ) !?db_mod.types.RelationalRowsJoinResult {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return try rowsJoinPlanFromLakeScanAlloc(plan_alloc, self.source(), table_name, runtime_schema, plan, consistency);
        }

        fn rowsLateralPlan(
            ptr: *anyopaque,
            plan_alloc: std.mem.Allocator,
            table_name: []const u8,
            runtime_schema: storage_schema.TableSchema,
            plan: db_mod.types.RelationalRowsLateralPlan,
            consistency: raft_mod.ReadConsistency,
        ) !?db_mod.types.RelationalRowsJoinResult {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return try rowsLateralPlanFromLakeScanAlloc(plan_alloc, self.source(), table_name, runtime_schema, plan, consistency);
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
            if (request.predicate) |predicate| {
                if (std.mem.eql(u8, predicate.column, "tenant")) {
                    try std.testing.expectEqual(serverless_query.LakeRowsPredicateOp.eq_bytes, predicate.op);
                    try std.testing.expectEqualStrings("t2", predicate.bytes_value);
                } else if (std.mem.eql(u8, predicate.column, "amount")) {
                    try std.testing.expectEqual(serverless_query.LakeRowsPredicateOp.eq_f64, predicate.op);
                    try std.testing.expectEqual(@as(f64, 20.5), predicate.f64_value);
                } else return error.UnexpectedLakePredicate;
            }
            self.lake_scan_calls += 1;
            self.last_scan_limit = request.limit;
            return try buildRows(scan_alloc, request.projected_columns);
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
            self.lake_expression_aggregate_calls += 1;

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

        fn buildRows(
            row_alloc: std.mem.Allocator,
            projected_columns: []const []const u8,
        ) !serverless_query.LakeRowsScanResult {
            if (projected_columns.len == 0) return error.InvalidLakeRowsQuery;
            const rows = try row_alloc.alloc(serverless_query.lake_rows.ProjectedRow, 2);
            errdefer row_alloc.free(rows);
            var initialized: usize = 0;
            errdefer {
                for (rows[0..initialized]) |*row| row.deinit(row_alloc);
            }
            rows[0] = try buildRow(row_alloc, "row:1", projected_columns, 20);
            initialized += 1;
            rows[1] = try buildRow(row_alloc, "row:2", projected_columns, 30);
            initialized += 1;
            return .{ .rows = rows, .total = 2 };
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
                    else if (std.mem.eql(u8, column, "tenant"))
                        .{ .bytes = try row_alloc.dupe(u8, "t2") }
                    else if (std.mem.eql(u8, column, "attrs"))
                        .{ .json = try row_alloc.dupe(u8, if (amount == 20) "{\"tier\":\"gold\",\"flags\":{\"vip\":true}}" else "{\"tier\":\"silver\",\"flags\":{\"vip\":false}}") }
                    else if (std.mem.eql(u8, column, "tags"))
                        .{ .json = try row_alloc.dupe(u8, if (amount == 20) "[\"hot\",\"vip\"]" else "[\"cold\"]") }
                    else if (std.mem.eql(u8, column, "nickname"))
                        if (amount == 20) null else .{ .bytes = try row_alloc.dupe(u8, "nick30") }
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
    var source = fake.source();
    const select = [_][]const u8{"amount"};
    const predicates = [_]storage_schema.RelationalCheck{.{
        .name = "",
        .field = "tenant",
        .value_json = "\"t2\"",
    }};

    var query_result = (try source.rowsQueryPlan(alloc, "events", schema, .{
        .query = .{
            .select = select[0..],
            .select_all = false,
            .predicates = predicates[0..],
            .limit = 5,
        },
    }, .read_index)).?;
    defer query_result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), fake.lake_scan_calls);
    try std.testing.expectEqual(@as(usize, 0), fake.routed_scan_calls);
    try std.testing.expectEqual(@as(u32, 2), query_result.total);
    try std.testing.expectEqualStrings("{\"amount\":20}", query_result.rows[0]);
    try std.testing.expectEqualStrings("{\"amount\":30}", query_result.rows[1]);

    const float_predicates = [_]storage_schema.RelationalCheck{.{
        .name = "",
        .field = "amount",
        .value_json = "20.5",
    }};
    var float_query_result = (try source.rowsQueryPlan(alloc, "events", schema, .{
        .query = .{
            .select = select[0..],
            .select_all = false,
            .predicates = float_predicates[0..],
            .limit = 5,
        },
    }, .read_index)).?;
    defer float_query_result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 2), fake.lake_scan_calls);
    try std.testing.expectEqual(@as(usize, 0), fake.routed_scan_calls);

    const residual_predicates = [_]storage_schema.RelationalCheck{.{
        .name = "",
        .field = "amount",
        .op = .gt,
        .value_json = "20",
    }};
    var residual_query_result = (try source.rowsQueryPlan(alloc, "events", schema, .{
        .query = .{
            .select = select[0..],
            .select_all = false,
            .predicates = residual_predicates[0..],
            .limit = 5,
        },
    }, .read_index)).?;
    defer residual_query_result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 3), fake.lake_scan_calls);
    try std.testing.expectEqual(@as(usize, 0), fake.routed_scan_calls);
    try std.testing.expectEqual(@as(u32, 1), residual_query_result.total);
    try std.testing.expectEqual(@as(usize, 1), residual_query_result.rows.len);
    try std.testing.expectEqualStrings("{\"amount\":30}", residual_query_result.rows[0]);

    const or_amount_ten = [_]storage_schema.RelationalCheck{.{
        .name = "",
        .field = "amount",
        .value_json = "10",
    }};
    const or_amount_thirty = [_]storage_schema.RelationalCheck{.{
        .name = "",
        .field = "amount",
        .value_json = "30",
    }};
    const not_amount_twenty = [_]storage_schema.RelationalCheck{.{
        .name = "",
        .field = "amount",
        .value_json = "20",
    }};
    const or_groups = [_]db_mod.types.RelationalRowsPredicateGroup{
        .{ .predicates = or_amount_ten[0..] },
        .{ .predicates = or_amount_thirty[0..] },
    };
    const not_groups = [_]db_mod.types.RelationalRowsPredicateGroup{.{
        .predicates = not_amount_twenty[0..],
    }};
    var group_query_result = (try source.rowsQueryPlan(alloc, "events", schema, .{
        .query = .{
            .select = select[0..],
            .select_all = false,
            .or_predicates = or_groups[0..],
            .not_predicates = not_groups[0..],
            .limit = 5,
        },
    }, .read_index)).?;
    defer group_query_result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 4), fake.lake_scan_calls);
    try std.testing.expectEqual(@as(usize, 0), fake.routed_scan_calls);
    try std.testing.expectEqual(@as(u32, 1), group_query_result.total);
    try std.testing.expectEqual(@as(usize, 1), group_query_result.rows.len);
    try std.testing.expectEqualStrings("{\"amount\":30}", group_query_result.rows[0]);

    const in_predicates = [_]db_mod.types.RelationalRowsInPredicate{.{
        .field = "amount",
        .values_json = "[30]",
    }};
    var in_query_result = (try source.rowsQueryPlan(alloc, "events", schema, .{
        .query = .{
            .select = select[0..],
            .select_all = false,
            .in_predicates = in_predicates[0..],
            .limit = 5,
        },
    }, .read_index)).?;
    defer in_query_result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 5), fake.lake_scan_calls);
    try std.testing.expectEqual(@as(usize, 0), fake.routed_scan_calls);
    try std.testing.expectEqual(@as(u32, 1), in_query_result.total);
    try std.testing.expectEqual(@as(usize, 1), in_query_result.rows.len);
    try std.testing.expectEqualStrings("{\"amount\":30}", in_query_result.rows[0]);

    const text_patterns = [_]db_mod.types.RelationalRowsTextPatternPredicate{.{
        .field = "tenant",
        .pattern = "T_",
        .case_insensitive = true,
    }};
    var text_pattern_query_result = (try source.rowsQueryPlan(alloc, "events", schema, .{
        .query = .{
            .select = select[0..],
            .select_all = false,
            .text_patterns = text_patterns[0..],
            .limit = 5,
        },
    }, .read_index)).?;
    defer text_pattern_query_result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 6), fake.lake_scan_calls);
    try std.testing.expectEqual(@as(usize, 0), fake.routed_scan_calls);
    try std.testing.expectEqual(@as(u32, 2), text_pattern_query_result.total);
    try std.testing.expectEqual(@as(usize, 2), text_pattern_query_result.rows.len);
    try std.testing.expectEqualStrings("{\"amount\":20}", text_pattern_query_result.rows[0]);
    try std.testing.expectEqualStrings("{\"amount\":30}", text_pattern_query_result.rows[1]);

    const json_contains = [_]db_mod.types.RelationalRowsJsonContainsPredicate{.{
        .field = "attrs",
        .value_json = "{\"tier\":\"silver\"}",
    }};
    var json_contains_result = (try source.rowsQueryPlan(alloc, "events", schema, .{
        .query = .{
            .select = select[0..],
            .select_all = false,
            .json_contains = json_contains[0..],
            .limit = 5,
        },
    }, .read_index)).?;
    defer json_contains_result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 7), fake.lake_scan_calls);
    try std.testing.expectEqual(@as(usize, 0), fake.routed_scan_calls);
    try std.testing.expectEqual(@as(u32, 1), json_contains_result.total);
    try std.testing.expectEqual(@as(usize, 1), json_contains_result.rows.len);
    try std.testing.expectEqualStrings("{\"amount\":30}", json_contains_result.rows[0]);

    const json_path_eq = [_]db_mod.types.RelationalRowsJsonPathEqPredicate{.{
        .field = "attrs",
        .path = "flags.vip",
        .value_json = "true",
    }};
    var json_path_eq_result = (try source.rowsQueryPlan(alloc, "events", schema, .{
        .query = .{
            .select = select[0..],
            .select_all = false,
            .json_path_eq = json_path_eq[0..],
            .limit = 5,
        },
    }, .read_index)).?;
    defer json_path_eq_result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 8), fake.lake_scan_calls);
    try std.testing.expectEqual(@as(usize, 0), fake.routed_scan_calls);
    try std.testing.expectEqual(@as(u32, 1), json_path_eq_result.total);
    try std.testing.expectEqual(@as(usize, 1), json_path_eq_result.rows.len);
    try std.testing.expectEqualStrings("{\"amount\":20}", json_path_eq_result.rows[0]);

    const json_path_exists = [_]db_mod.types.RelationalRowsJsonPathExistsPredicate{.{
        .field = "attrs",
        .path = "flags.vip",
    }};
    var json_path_exists_result = (try source.rowsQueryPlan(alloc, "events", schema, .{
        .query = .{
            .select = select[0..],
            .select_all = false,
            .json_path_exists = json_path_exists[0..],
            .limit = 5,
        },
    }, .read_index)).?;
    defer json_path_exists_result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 9), fake.lake_scan_calls);
    try std.testing.expectEqual(@as(usize, 0), fake.routed_scan_calls);
    try std.testing.expectEqual(@as(u32, 2), json_path_exists_result.total);
    try std.testing.expectEqual(@as(usize, 2), json_path_exists_result.rows.len);
    try std.testing.expectEqualStrings("{\"amount\":20}", json_path_exists_result.rows[0]);
    try std.testing.expectEqualStrings("{\"amount\":30}", json_path_exists_result.rows[1]);

    const json_extract = [_]db_mod.types.RelationalRowsJsonExtractProjection{
        .{ .output = "tier", .field = "attrs", .path = "tier", .as_text = true },
        .{ .output = "flags", .field = "attrs", .path = "flags" },
    };
    var json_extract_result = (try source.rowsQueryPlan(alloc, "events", schema, .{
        .query = .{
            .select_all = false,
            .json_extract = json_extract[0..],
            .limit = 5,
        },
    }, .read_index)).?;
    defer json_extract_result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 10), fake.lake_scan_calls);
    try std.testing.expectEqual(@as(usize, 0), fake.routed_scan_calls);
    try std.testing.expectEqual(@as(u32, 2), json_extract_result.total);
    try std.testing.expectEqual(@as(usize, 2), json_extract_result.rows.len);
    try std.testing.expectEqualStrings("{\"tier\":\"gold\",\"flags\":{\"vip\":true}}", json_extract_result.rows[0]);
    try std.testing.expectEqualStrings("{\"tier\":\"silver\",\"flags\":{\"vip\":false}}", json_extract_result.rows[1]);

    const array_length = [_]db_mod.types.RelationalRowsArrayLengthProjection{.{
        .output = "tag_count",
        .field = "tags",
    }};
    const field_aliases = [_]db_mod.types.RelationalRowsFieldAliasProjection{.{
        .output = "tenant_alias",
        .field = "tenant",
    }};
    const coalesce = [_]db_mod.types.RelationalRowsCoalesceProjection{.{
        .output = "display_name",
        .operands = &.{
            .{ .kind = .field, .field = "nickname" },
            .{ .kind = .value, .value_json = "\"fallback\"" },
        },
    }};
    var shaped_projection_result = (try source.rowsQueryPlan(alloc, "events", schema, .{
        .query = .{
            .select_all = false,
            .array_length = array_length[0..],
            .coalesce = coalesce[0..],
            .field_aliases = field_aliases[0..],
            .limit = 5,
        },
    }, .read_index)).?;
    defer shaped_projection_result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 11), fake.lake_scan_calls);
    try std.testing.expectEqual(@as(usize, 0), fake.routed_scan_calls);
    try std.testing.expectEqual(@as(u32, 2), shaped_projection_result.total);
    try std.testing.expectEqual(@as(usize, 2), shaped_projection_result.rows.len);
    try std.testing.expectEqualStrings("{\"tag_count\":2,\"display_name\":\"fallback\",\"tenant_alias\":\"t2\"}", shaped_projection_result.rows[0]);
    try std.testing.expectEqualStrings("{\"tag_count\":1,\"display_name\":\"nick30\",\"tenant_alias\":\"t2\"}", shaped_projection_result.rows[1]);

    var offset_query_result = (try source.rowsQueryPlan(alloc, "events", schema, .{
        .query = .{
            .select = select[0..],
            .select_all = false,
            .offset = 1,
            .limit = 1,
        },
    }, .read_index)).?;
    defer offset_query_result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 12), fake.lake_scan_calls);
    try std.testing.expectEqual(@as(?usize, 2), fake.last_scan_limit);
    try std.testing.expectEqual(@as(usize, 0), fake.routed_scan_calls);
    try std.testing.expectEqual(@as(u32, 2), offset_query_result.total);
    try std.testing.expectEqual(@as(usize, 1), offset_query_result.rows.len);
    try std.testing.expectEqualStrings("{\"amount\":30}", offset_query_result.rows[0]);

    const doc_range_start = try relational_rows_api.physicalPrimaryKeyFromRowJsonAlloc(alloc, schema, "{\"id\":\"b\"}");
    defer alloc.free(doc_range_start);
    const doc_range_end = try relational_rows_api.physicalPrimaryKeyFromRowJsonAlloc(alloc, schema, "{\"id\":\"c\"}");
    defer alloc.free(doc_range_end);
    var doc_key_range_result = (try source.rowsQueryPlan(alloc, "events", schema, .{
        .query = .{
            .select = select[0..],
            .select_all = false,
            .doc_key_range = .{ .start = doc_range_start, .end = doc_range_end },
            .limit = 1,
        },
    }, .read_index)).?;
    defer doc_key_range_result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 13), fake.lake_scan_calls);
    try std.testing.expectEqual(@as(?usize, null), fake.last_scan_limit);
    try std.testing.expectEqual(@as(usize, 0), fake.routed_scan_calls);
    try std.testing.expectEqual(@as(u32, 1), doc_key_range_result.total);
    try std.testing.expectEqual(@as(usize, 1), doc_key_range_result.rows.len);
    try std.testing.expectEqualStrings("{\"amount\":30}", doc_key_range_result.rows[0]);

    const distinct_on = [_][]const u8{"tenant"};
    var unordered_distinct_query_result = (try source.rowsQueryPlan(alloc, "events", schema, .{
        .query = .{
            .select = select[0..],
            .select_all = false,
            .distinct_on = distinct_on[0..],
            .limit = 1,
        },
    }, .read_index)).?;
    defer unordered_distinct_query_result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 14), fake.lake_scan_calls);
    try std.testing.expectEqual(@as(?usize, null), fake.last_scan_limit);
    try std.testing.expectEqual(@as(usize, 0), fake.routed_scan_calls);
    try std.testing.expectEqual(@as(u32, 1), unordered_distinct_query_result.total);
    try std.testing.expectEqual(@as(usize, 1), unordered_distinct_query_result.rows.len);
    try std.testing.expectEqualStrings("{\"amount\":20}", unordered_distinct_query_result.rows[0]);

    const distinct_order_by = [_]db_mod.types.RelationalRowsQueryOrder{
        .{ .field = "tenant" },
        .{ .field = "amount", .direction = .desc },
    };
    var distinct_query_result = (try source.rowsQueryPlan(alloc, "events", schema, .{
        .query = .{
            .select = select[0..],
            .select_all = false,
            .distinct_on = distinct_on[0..],
            .order_by = distinct_order_by[0..],
            .limit = 5,
        },
    }, .read_index)).?;
    defer distinct_query_result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 15), fake.lake_scan_calls);
    try std.testing.expectEqual(@as(?usize, null), fake.last_scan_limit);
    try std.testing.expectEqual(@as(usize, 0), fake.routed_scan_calls);
    try std.testing.expectEqual(@as(u32, 1), distinct_query_result.total);
    try std.testing.expectEqual(@as(usize, 1), distinct_query_result.rows.len);
    try std.testing.expectEqualStrings("{\"amount\":30}", distinct_query_result.rows[0]);

    const lower_tenant_operands = [_]db_mod.types.RelationalRowsExpression{.{
        .kind = .field,
        .field = "tenant",
    }};
    const lower_tenant_expression = db_mod.types.RelationalRowsExpression{
        .kind = .lower,
        .operands = lower_tenant_operands[0..],
    };
    const distinct_on_expressions = [_]db_mod.types.RelationalRowsExpression{lower_tenant_expression};
    const distinct_expression_order_by = [_]db_mod.types.RelationalRowsQueryOrder{
        .{ .expression = lower_tenant_expression },
        .{ .field = "amount", .direction = .desc },
    };
    var distinct_expression_query_result = (try source.rowsQueryPlan(alloc, "events", schema, .{
        .query = .{
            .select = select[0..],
            .select_all = false,
            .distinct_on_expressions = distinct_on_expressions[0..],
            .order_by = distinct_expression_order_by[0..],
            .limit = 5,
        },
    }, .read_index)).?;
    defer distinct_expression_query_result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 16), fake.lake_scan_calls);
    try std.testing.expectEqual(@as(?usize, null), fake.last_scan_limit);
    try std.testing.expectEqual(@as(usize, 0), fake.routed_scan_calls);
    try std.testing.expectEqual(@as(u32, 1), distinct_expression_query_result.total);
    try std.testing.expectEqual(@as(usize, 1), distinct_expression_query_result.rows.len);
    try std.testing.expectEqualStrings("{\"amount\":30}", distinct_expression_query_result.rows[0]);

    const fast_aggregations = [_]db_mod.types.RelationalRowsAggregateSpec{
        .{ .name = "count_all", .op = .count },
        .{ .name = "sum_amount", .op = .sum, .field = "amount" },
        .{ .name = "max_amount", .op = .max, .field = "amount" },
    };
    var fast_aggregate_result = (try source.rowsAggregatePlan(alloc, "events", schema, .{
        .aggregate = .{
            .aggregations = fast_aggregations[0..],
        },
    }, .read_index)).?;
    defer fast_aggregate_result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 16), fake.lake_scan_calls);
    try std.testing.expectEqual(@as(usize, 1), fake.lake_expression_aggregate_calls);
    try std.testing.expectEqual(@as(usize, 0), fake.routed_scan_calls);
    try std.testing.expectEqual(@as(u32, 1), fast_aggregate_result.total_groups);
    try std.testing.expectEqualStrings("{\"count_all\":2,\"sum_amount\":50,\"max_amount\":30}", fast_aggregate_result.rows[0]);

    const aggregations = [_]db_mod.types.RelationalRowsAggregateSpec{
        .{ .name = "count_all", .op = .count },
        .{ .name = "sum_amount", .op = .sum, .field = "amount" },
    };
    var aggregate_result = (try source.rowsAggregatePlan(alloc, "events", schema, .{
        .aggregate = .{
            .source = .{ .predicates = predicates[0..] },
            .aggregations = aggregations[0..],
        },
    }, .read_index)).?;
    defer aggregate_result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 17), fake.lake_scan_calls);
    try std.testing.expectEqual(@as(usize, 1), fake.lake_expression_aggregate_calls);
    try std.testing.expectEqual(@as(usize, 0), fake.routed_scan_calls);
    try std.testing.expectEqual(@as(u32, 1), aggregate_result.total_groups);
    try std.testing.expectEqualStrings("{\"count_all\":2,\"sum_amount\":50}", aggregate_result.rows[0]);

    const set_left = db_mod.types.RelationalRowsQueryPlan{ .query = .{
        .select = select[0..],
        .select_all = false,
    } };
    const set_right = db_mod.types.RelationalRowsQueryPlan{ .query = .{
        .select = select[0..],
        .select_all = false,
        .predicates = residual_predicates[0..],
    } };
    var except_result = (try source.rowsSetOperationPlan(alloc, "events", schema, .{
        .operation = .except,
        .left = set_left,
        .right = set_right,
    }, .read_index)).?;
    defer except_result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 19), fake.lake_scan_calls);
    try std.testing.expectEqual(@as(usize, 1), fake.lake_expression_aggregate_calls);
    try std.testing.expectEqual(@as(usize, 0), fake.routed_scan_calls);
    try std.testing.expectEqual(@as(u32, 1), except_result.total);
    try std.testing.expectEqual(@as(usize, 1), except_result.rows.len);
    try std.testing.expectEqualStrings("{\"amount\":20}", except_result.rows[0]);

    const set_order = [_]db_mod.types.RelationalRowsQueryOrder{.{
        .field = "amount",
        .direction = .desc,
    }};
    var union_all_result = (try source.rowsSetOperationPlan(alloc, "events", schema, .{
        .operation = .union_all,
        .left = set_left,
        .right = set_right,
        .order_by = set_order[0..],
        .limit = 2,
    }, .read_index)).?;
    defer union_all_result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 21), fake.lake_scan_calls);
    try std.testing.expectEqual(@as(usize, 1), fake.lake_expression_aggregate_calls);
    try std.testing.expectEqual(@as(usize, 0), fake.routed_scan_calls);
    try std.testing.expectEqual(@as(u32, 3), union_all_result.total);
    try std.testing.expectEqual(@as(usize, 2), union_all_result.rows.len);
    try std.testing.expectEqualStrings("{\"amount\":30}", union_all_result.rows[0]);
    try std.testing.expectEqualStrings("{\"amount\":30}", union_all_result.rows[1]);

    try std.testing.expectError(error.RelationalRowsCteMaterializationRejected, source.rowsSetOperationPlan(alloc, "events", schema, .{
        .operation = .union_all,
        .left = set_left,
        .right = set_right,
        .max_rows = 2,
    }, .read_index));
    try std.testing.expectEqual(@as(usize, 23), fake.lake_scan_calls);
    try std.testing.expectEqual(@as(usize, 0), fake.routed_scan_calls);

    const window_partition = [_][]const u8{"tenant"};
    const window_order = [_]db_mod.types.RelationalRowsQueryOrder{.{
        .field = "amount",
        .direction = .desc,
    }};
    const windows = [_]db_mod.types.RelationalRowsWindowSpec{.{
        .output = "rn",
        .function = .row_number,
        .partition_by = window_partition[0..],
        .order_by = window_order[0..],
    }};
    var window_result = (try source.rowsWindowPlan(alloc, "events", schema, .{
        .window = .{
            .source = .{ .select_all = true },
            .windows = windows[0..],
            .select = select[0..],
            .order_by = window_order[0..],
        },
    }, .read_index)).?;
    defer window_result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 24), fake.lake_scan_calls);
    try std.testing.expectEqual(@as(usize, 0), fake.routed_scan_calls);
    try std.testing.expectEqual(@as(u32, 2), window_result.total_rows);
    try std.testing.expectEqual(@as(usize, 2), window_result.rows.len);
    try std.testing.expectEqualStrings("{\"amount\":30,\"rn\":1}", window_result.rows[0]);
    try std.testing.expectEqualStrings("{\"amount\":20,\"rn\":2}", window_result.rows[1]);

    const left_amount_twenty = [_]storage_schema.RelationalCheck{.{
        .name = "",
        .field = "amount",
        .value_json = "20",
    }};
    const right_amount_thirty = [_]storage_schema.RelationalCheck{.{
        .name = "",
        .field = "amount",
        .value_json = "30",
    }};
    const join_on = [_]db_mod.types.RelationalRowsJoinOn{.{
        .left_field = "tenant",
        .right_field = "tenant",
    }};
    const join_select = [_]db_mod.types.RelationalRowsJoinProjection{
        .{ .output = "left_amount", .side = .left, .field = "amount" },
        .{ .output = "right_amount", .side = .right, .field = "amount" },
    };
    var join_result = (try source.rowsJoinPlan(alloc, "events", schema, .{
        .join = .{
            .left = .{ .predicates = left_amount_twenty[0..] },
            .right = .{ .predicates = right_amount_thirty[0..] },
            .on = join_on[0..],
            .select = join_select[0..],
        },
    }, .read_index)).?;
    defer join_result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 25), fake.lake_scan_calls);
    try std.testing.expectEqual(@as(usize, 0), fake.routed_scan_calls);
    try std.testing.expectEqual(@as(u32, 1), join_result.total_rows);
    try std.testing.expectEqual(@as(usize, 1), join_result.rows.len);
    try std.testing.expectEqualStrings("{\"left_amount\":20,\"right_amount\":30}", join_result.rows[0]);

    const lateral_correlations = [_]db_mod.types.RelationalRowsLateralCorrelation{.{
        .left_field = "tenant",
        .right_field = "tenant",
    }};
    const lateral_select = [_]db_mod.types.RelationalRowsJoinProjection{
        .{ .output = "left_amount", .side = .left, .field = "amount" },
        .{ .output = "latest_amount", .side = .right, .field = "amount" },
    };
    var lateral_result = (try source.rowsLateralPlan(alloc, "events", schema, .{
        .lateral = .{
            .left = .{ .order_by = &.{.{ .field = "amount", .direction = .asc }} },
            .right = .{ .order_by = &.{.{ .field = "amount", .direction = .desc }}, .limit = 1 },
            .correlations = lateral_correlations[0..],
            .select = lateral_select[0..],
            .order_by = &.{.{ .field = "left_amount", .direction = .asc }},
        },
    }, .read_index)).?;
    defer lateral_result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 26), fake.lake_scan_calls);
    try std.testing.expectEqual(@as(usize, 0), fake.routed_scan_calls);
    try std.testing.expectEqual(@as(u32, 2), lateral_result.total_rows);
    try std.testing.expectEqual(@as(usize, 2), lateral_result.rows.len);
    try std.testing.expectEqualStrings("{\"left_amount\":20,\"latest_amount\":30}", lateral_result.rows[0]);
    try std.testing.expectEqualStrings("{\"left_amount\":30,\"latest_amount\":30}", lateral_result.rows[1]);

    const lake_range = [_]db_mod.types.RelationalRowsDocKeyRange{.{
        .start = doc_range_start,
        .end = doc_range_end,
    }};
    var ranged_plan_result = (try source.rowsQueryPlan(alloc, "events", schema, .{
        .ranges = lake_range[0..],
        .query = .{
            .select = select[0..],
            .select_all = false,
        },
    }, .read_index)).?;
    defer ranged_plan_result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 27), fake.lake_scan_calls);
    try std.testing.expectEqual(@as(usize, 0), fake.routed_scan_calls);
    try std.testing.expectEqual(@as(u32, 1), ranged_plan_result.total);
    try std.testing.expectEqual(@as(usize, 1), ranged_plan_result.rows.len);
    try std.testing.expectEqualStrings("{\"amount\":30}", ranged_plan_result.rows[0]);

    const cte_select = [_][]const u8{ "amount", "tenant" };
    const lake_ctes = [_]db_mod.types.RelationalRowsCte{.{
        .name = "high_amounts",
        .query = .{
            .predicates = residual_predicates[0..],
            .select = cte_select[0..],
            .select_all = false,
        },
    }};
    var cte_query_result = (try source.rowsQueryPlan(alloc, "events", schema, .{
        .ctes = lake_ctes[0..],
        .query = .{
            .source_cte = "high_amounts",
            .select = select[0..],
            .select_all = false,
        },
    }, .read_index)).?;
    defer cte_query_result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 28), fake.lake_scan_calls);
    try std.testing.expectEqual(@as(usize, 0), fake.routed_scan_calls);
    try std.testing.expectEqual(@as(u32, 1), cte_query_result.total);
    try std.testing.expectEqual(@as(usize, 1), cte_query_result.rows.len);
    try std.testing.expectEqualStrings("{\"amount\":30}", cte_query_result.rows[0]);

    var cte_aggregate_result = (try source.rowsAggregatePlan(alloc, "events", schema, .{
        .ctes = lake_ctes[0..],
        .aggregate = .{
            .source = .{ .source_cte = "high_amounts" },
            .aggregations = aggregations[0..],
        },
    }, .read_index)).?;
    defer cte_aggregate_result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 29), fake.lake_scan_calls);
    try std.testing.expectEqual(@as(usize, 1), fake.lake_expression_aggregate_calls);
    try std.testing.expectEqual(@as(usize, 0), fake.routed_scan_calls);
    try std.testing.expectEqual(@as(u32, 1), cte_aggregate_result.total_groups);
    try std.testing.expectEqualStrings("{\"count_all\":1,\"sum_amount\":30}", cte_aggregate_result.rows[0]);
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

test "object storage pinned external lake source routes row plans through scanner" {
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

    var memory = object_storage_api.MemoryObjectStorage.init(alloc);
    defer memory.deinit();
    var client = memory.client();
    try client.makeBucket("bucket");

    var lake_source = PinnedExternalObjectStorageLakeRowsSource.init(inventory, client);
    var source = lake_source.source();
    const projection = [_][]const u8{"amount"};
    try std.testing.expectError(
        error.ExternalLakeSnapshotMismatch,
        source.rowsQueryPlan(alloc, "events", schema, .{
            .query = .{
                .select = projection[0..],
                .select_all = false,
            },
        }, .read_index),
    );

    const aggregations = [_]db_mod.types.RelationalRowsAggregateSpec{
        .{ .name = "count_all", .op = .count },
    };
    try std.testing.expectError(
        error.ExternalLakeSnapshotMismatch,
        source.rowsAggregatePlan(alloc, "events", schema, .{
            .aggregate = .{
                .aggregations = aggregations[0..],
            },
        }, .read_index),
    );
}

test "owned object storage lake source discovers and pins parquet prefix inventory" {
    const alloc = std.testing.allocator;
    var columns = [_]storage_schema.RelationalColumn{
        .{ .name = "amount", .path = "amount", .field_type = .numeric, .nullable = false },
    };
    const current_schema = storage_schema.TableSchema{
        .storage_mode = .relational,
        .relational_columns = columns[0..],
        .external_base_source = .{
            .table_id = "events",
            .format = .parquet,
            .source_uri = "s3://bucket/events",
            .snapshot_mode = .current,
            .schema_fingerprint = "schema-v1",
        },
    };

    var memory = object_storage_api.MemoryObjectStorage.init(alloc);
    defer memory.deinit();
    var client = memory.client();
    try client.makeBucket("bucket");
    var put_a = try client.putObject("bucket", "events/part-a.parquet", "not-a-real-parquet-file", .{});
    defer put_a.deinit(alloc);
    var put_b = try client.putObject("bucket", "events/_SUCCESS", "ok", .{});
    defer put_b.deinit(alloc);

    var owned_source = try OwnedExternalObjectStorageLakeRowsSource.initParquetPrefixAlloc(
        alloc,
        current_schema,
        client,
        "bucket",
        "events",
        .{},
    );
    defer owned_source.deinit();
    try std.testing.expectEqualStrings("events", owned_source.inventory.source_id);
    try std.testing.expectEqualStrings("s3://bucket/events", owned_source.inventory.source_uri);
    try std.testing.expect(std.mem.startsWith(u8, owned_source.inventory.snapshot_id, "sha256:"));
    try std.testing.expectEqual(@as(usize, 1), owned_source.inventory.files.len);
    try std.testing.expectEqualStrings("part-a.parquet", owned_source.inventory.files[0].file_id);
    try std.testing.expectEqualStrings("s3://bucket/events/part-a.parquet", owned_source.inventory.files[0].object_uri);
    const pinned_state = owned_source.pinnedState();
    try std.testing.expectEqual(external_source_api.Format.parquet, pinned_state.format);
    try std.testing.expectEqualStrings("s3://bucket/events", pinned_state.source_uri);
    try std.testing.expectEqualStrings(owned_source.inventory.snapshot_id, pinned_state.snapshot_id);
    try std.testing.expectEqualStrings("schema-v1", pinned_state.schema_fingerprint);
    try std.testing.expectEqual(@as(usize, 1), pinned_state.file_count);
    try std.testing.expectEqual(@as(usize, 0), pinned_state.row_group_count);
    try std.testing.expectEqual(@as(u64, 0), pinned_state.row_count);
    try std.testing.expectEqual(@as(u64, "not-a-real-parquet-file".len), pinned_state.byte_len);
    _ = owned_source.source();

    const stale_schema = storage_schema.TableSchema{
        .storage_mode = .relational,
        .relational_columns = columns[0..],
        .external_base_source = .{
            .table_id = "events",
            .format = .parquet,
            .source_uri = "s3://bucket/events",
            .snapshot_mode = .{ .object_version_digest = "sha256:stale" },
            .schema_fingerprint = "schema-v1",
        },
    };
    try std.testing.expectError(
        error.ExternalLakeSnapshotMismatch,
        OwnedExternalObjectStorageLakeRowsSource.initParquetPrefixAlloc(
            alloc,
            stale_schema,
            client,
            "bucket",
            "events",
            .{},
        ),
    );
}

test "opened object storage lake source owns store and pins parquet prefix inventory" {
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
            .snapshot_mode = .current,
            .schema_fingerprint = "schema-v1",
        },
    };

    var memory = object_storage_api.MemoryObjectStorage.init(alloc);
    defer memory.deinit();
    var client = memory.client();
    try client.makeBucket("bucket");
    var put_a = try client.putObject("bucket", "events/part-a.parquet", "not-a-real-parquet-file", .{});
    defer put_a.deinit(alloc);
    var put_b = try client.putObject("bucket", "events/part-b.parquet", "not-a-real-parquet-file", .{});
    defer put_b.deinit(alloc);

    const opened_store = try object_store_support.OpenedObjectStore.initWithClient(
        alloc,
        client,
        "bucket",
        "events",
    );
    var opened_source = try OpenedExternalObjectStorageLakeRowsSource.initWithOpenedStoreAlloc(
        alloc,
        schema,
        opened_store,
        .{},
    );
    defer opened_source.deinit();

    try std.testing.expectEqualStrings("bucket", opened_source.opened_store.bucket);
    try std.testing.expectEqualStrings("events", opened_source.opened_store.prefix);
    try std.testing.expectEqualStrings("events", opened_source.owned_source.inventory.source_id);
    try std.testing.expectEqualStrings("s3://bucket/events", opened_source.owned_source.inventory.source_uri);
    try std.testing.expect(std.mem.startsWith(u8, opened_source.owned_source.inventory.snapshot_id, "sha256:"));
    try std.testing.expectEqual(@as(usize, 2), opened_source.owned_source.inventory.files.len);
    const pinned_state = opened_source.pinnedState();
    try std.testing.expectEqual(external_source_api.Format.parquet, pinned_state.format);
    try std.testing.expectEqualStrings("s3://bucket/events", pinned_state.source_uri);
    try std.testing.expectEqualStrings(opened_source.owned_source.inventory.snapshot_id, pinned_state.snapshot_id);
    try std.testing.expectEqualStrings("schema-v1", pinned_state.schema_fingerprint);
    try std.testing.expectEqual(@as(usize, 2), pinned_state.file_count);
    try std.testing.expectEqual(@as(usize, 0), pinned_state.row_group_count);
    try std.testing.expectEqual(@as(u64, 0), pinned_state.row_count);
    try std.testing.expectEqual(@as(u64, "not-a-real-parquet-file".len * 2), pinned_state.byte_len);
    _ = opened_source.source();
}

test "opened object storage lake source can own serving object range cache" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const persistent_cache_root = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/lake-range-cache", .{tmp.sub_path});
    defer alloc.free(persistent_cache_root);

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
            .snapshot_mode = .current,
            .schema_fingerprint = "schema-v1",
        },
    };

    var memory = object_storage_api.MemoryObjectStorage.init(alloc);
    defer memory.deinit();
    var client = memory.client();
    try client.makeBucket("bucket");
    var put_a = try client.putObject("bucket", "events/part-a.parquet", "not-a-real-parquet-file", .{});
    defer put_a.deinit(alloc);

    const opened_store = try object_store_support.OpenedObjectStore.initWithClient(
        alloc,
        client,
        "bucket",
        "events",
    );
    const sidecars = [_]sidecar_manifest_api.DeclaredArtifact{.{
        .name = "events.amount.vector",
        .binding = .{
            .sidecar_kind = .vector,
            .source_kind = .external_parquet,
            .row_ref_kind = .external,
            .source_id = "events",
            .snapshot_id = "sha256:expected",
            .schema_fingerprint = "schema-v1",
            .column_bindings = &[_][]const u8{"amount"},
            .index_config_hash = "sha256:vector",
        },
        .artifact = .{
            .kind = artifact_ref_api.ArtifactKind.vector_segment,
            .name = "events.amount.vector",
            .artifact_id = "artifact:vector:expected",
            .byte_len = 1,
            .checksum = "sha256:artifact",
        },
    }};
    const desired = [_]serverless_query.LakeSidecarDesired{.{ .kind = source_binding_api.SidecarKind.vector }};
    var opened_source = try OpenedExternalObjectStorageLakeRowsSource.initWithOpenedStoreAlloc(
        alloc,
        schema,
        opened_store,
        .{
            .serving_cache_max_bytes = 1024,
            .persistent_cache_root_dir = persistent_cache_root,
            .sidecar_context = .{
                .sidecars = sidecars[0..],
                .desired_sidecars = desired[0..],
            },
        },
    );
    defer opened_source.deinit();

    const owned_cache = opened_source.owned_source.owned_cache orelse return error.TestExpectedEqual;
    const owned_persistent_cache = opened_source.owned_source.owned_persistent_cache orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(owned_cache, opened_source.owned_source.pinned_source.scanner.cache.?);
    try std.testing.expectEqual(owned_persistent_cache, owned_cache.persistent.?);
    try std.testing.expectEqualStrings(persistent_cache_root, owned_persistent_cache.root_dir);
    try std.testing.expectEqual(@as(?usize, 1024), owned_cache.policy.max_total_bytes);
    try std.testing.expect(owned_cache.policy.isProtected(.metadata));
    try std.testing.expect(owned_cache.policy.isProtected(.serving_sidecar));
    try std.testing.expectEqual(@as(?usize, 128), owned_cache.policy.laneLimit(.broad_scan_scratch));
    const sidecar_context = opened_source.sidecarContext();
    try std.testing.expectEqual(@as(usize, 1), sidecar_context.sidecars.len);
    try std.testing.expectEqual(@as(usize, 1), sidecar_context.desired_sidecars.len);
    try std.testing.expectEqualStrings("events.amount.vector", sidecar_context.sidecars[0].name);

    var external_cache = serverless_query.initLakeParquetServingObjectRangeCache(2048);
    defer external_cache.deinit(alloc);
    const invalid_store = try object_store_support.OpenedObjectStore.initWithClient(
        alloc,
        client,
        "bucket",
        "events",
    );
    try std.testing.expectError(
        error.InvalidRowsRequest,
        OpenedExternalObjectStorageLakeRowsSource.initWithOpenedStoreAlloc(
            alloc,
            schema,
            invalid_store,
            .{
                .cache = &external_cache,
                .serving_cache_max_bytes = 1024,
            },
        ),
    );
    const invalid_persistent_store = try object_store_support.OpenedObjectStore.initWithClient(
        alloc,
        client,
        "bucket",
        "events",
    );
    try std.testing.expectError(
        error.InvalidRowsRequest,
        OpenedExternalObjectStorageLakeRowsSource.initWithOpenedStoreAlloc(
            alloc,
            schema,
            invalid_persistent_store,
            .{
                .cache = &external_cache,
                .persistent_cache_root_dir = persistent_cache_root,
            },
        ),
    );
}

test "opened object storage lake source resolves iceberg table root inventory" {
    const alloc = std.testing.allocator;
    var columns = [_]storage_schema.RelationalColumn{
        .{ .name = "amount", .path = "amount", .field_type = .numeric, .nullable = false },
    };
    const schema = storage_schema.TableSchema{
        .storage_mode = .relational,
        .relational_columns = columns[0..],
        .external_base_source = .{
            .table_id = "events",
            .format = .iceberg,
            .source_uri = "s3://bucket/events",
            .snapshot_mode = .current,
            .schema_fingerprint = "iceberg-schema:7",
        },
    };

    var memory = object_storage_api.MemoryObjectStorage.init(alloc);
    defer memory.deinit();
    var client = memory.client();
    try client.makeBucket("bucket");
    var version_hint = try client.putObject("bucket", "events/metadata/version-hint.text", "1\n", .{});
    defer version_hint.deinit(alloc);
    var metadata_file = try client.putObject("bucket", "events/metadata/v1.metadata.json", icebergRoutingMetadataJson(), .{});
    defer metadata_file.deinit(alloc);
    var data_manifest = try buildIcebergRoutingDataManifestFixture(alloc);
    defer data_manifest.deinit(alloc);
    var manifest_list = try buildIcebergRoutingManifestListFixture(alloc, data_manifest.items.len);
    defer manifest_list.deinit(alloc);
    var manifest_list_put = try client.putObject("bucket", "events/metadata/snap-12.avro", manifest_list.items, .{});
    defer manifest_list_put.deinit(alloc);
    var data_manifest_put = try client.putObject("bucket", "events/metadata/m-a.avro", data_manifest.items, .{});
    defer data_manifest_put.deinit(alloc);
    const data_a = try alloc.alloc(u8, 4096);
    defer alloc.free(data_a);
    @memset(data_a, 0);
    var data_a_put = try client.putObject("bucket", "events/data/a.parquet", data_a, .{});
    defer data_a_put.deinit(alloc);
    const data_b = try alloc.alloc(u8, 2048);
    defer alloc.free(data_b);
    @memset(data_b, 0);
    var data_b_put = try client.putObject("bucket", "events/data/b.parquet", data_b, .{});
    defer data_b_put.deinit(alloc);

    const opened_store = try object_store_support.OpenedObjectStore.initWithClient(
        alloc,
        client,
        "bucket",
        "events",
    );
    var opened_source = try OpenedExternalObjectStorageLakeRowsSource.initWithOpenedStoreAlloc(
        alloc,
        schema,
        opened_store,
        .{},
    );
    defer opened_source.deinit();

    try std.testing.expectEqual(external_source_api.Format.iceberg, opened_source.owned_source.inventory.format);
    try std.testing.expectEqualStrings("events", opened_source.owned_source.inventory.source_id);
    try std.testing.expectEqualStrings("s3://bucket/events", opened_source.owned_source.inventory.source_uri);
    try std.testing.expectEqualStrings("12", opened_source.owned_source.inventory.snapshot_id);
    try std.testing.expectEqual(@as(usize, 2), opened_source.owned_source.inventory.files.len);
    try std.testing.expectEqualStrings("s3://bucket/events/data/a.parquet", opened_source.owned_source.inventory.files[0].object_uri);
    try std.testing.expect(opened_source.owned_source.inventory.files[0].etag.len != 0);
    try std.testing.expect(opened_source.owned_source.inventory.files[0].version_id.len == 0);
    try std.testing.expectEqual(@as(?i64, 42), opened_source.owned_source.inventory.files[0].data_sequence_number);
    const pinned_state = opened_source.pinnedState();
    try std.testing.expectEqual(external_source_api.Format.iceberg, pinned_state.format);
    try std.testing.expectEqual(@as(usize, 2), pinned_state.file_count);
    try std.testing.expectEqual(@as(u64, 6144), pinned_state.byte_len);
}

test "opened object storage iceberg source applies position delete files" {
    const alloc = std.testing.allocator;
    var columns = [_]storage_schema.RelationalColumn{
        .{ .name = "amount", .path = "amount", .field_type = .numeric, .nullable = false },
    };
    const schema = storage_schema.TableSchema{
        .storage_mode = .relational,
        .relational_columns = columns[0..],
        .external_base_source = .{
            .table_id = "events",
            .format = .iceberg,
            .source_uri = "s3://bucket/events",
            .snapshot_mode = .current,
            .schema_fingerprint = "iceberg-schema:7",
        },
    };

    var memory = object_storage_api.MemoryObjectStorage.init(alloc);
    defer memory.deinit();
    var client = memory.client();
    try client.makeBucket("bucket");
    var version_hint = try client.putObject("bucket", "events/metadata/version-hint.text", "1\n", .{});
    defer version_hint.deinit(alloc);
    var metadata_file = try client.putObject("bucket", "events/metadata/v1.metadata.json", icebergRoutingMetadataJson(), .{});
    defer metadata_file.deinit(alloc);

    const data_file_path = "s3://bucket/events/data/a.parquet";
    const data_columns = [_]serverless_query.LakeParquetTestPlainI64Column{.{
        .column_id = "amount",
        .values = &[_]i64{ 10, 20, 30 },
        .field_id = 1,
    }};
    const data_object = try serverless_query.buildLakeParquetTestPlainI64AndByteArrayObjectAlloc(alloc, &data_columns, &.{});
    defer alloc.free(data_object);
    var data_put = try client.putObject("bucket", "events/data/a.parquet", data_object, .{});
    defer data_put.deinit(alloc);

    const delete_file_path = "s3://bucket/events/deletes/pos-a.parquet";
    const pos_columns = [_]serverless_query.LakeParquetTestPlainI64Column{.{
        .column_id = "pos",
        .values = &[_]i64{1},
    }};
    const file_path_columns = [_]serverless_query.LakeParquetTestPlainByteArrayColumn{.{
        .column_id = "file_path",
        .values = &[_][]const u8{data_file_path},
    }};
    const delete_object = try serverless_query.buildLakeParquetTestPlainI64AndByteArrayObjectAlloc(
        alloc,
        &pos_columns,
        &file_path_columns,
    );
    defer alloc.free(delete_object);
    var delete_put = try client.putObject("bucket", "events/deletes/pos-a.parquet", delete_object, .{});
    defer delete_put.deinit(alloc);

    var data_manifest = try buildIcebergRoutingOneDataManifestFixture(alloc, data_file_path, 3, data_object.len);
    defer data_manifest.deinit(alloc);
    var delete_manifest = try buildIcebergRoutingDeleteManifestFixture(alloc, delete_file_path, 1, delete_object.len);
    defer delete_manifest.deinit(alloc);
    var manifest_list = try buildIcebergRoutingDataAndDeleteManifestListFixture(
        alloc,
        data_manifest.items.len,
        delete_manifest.items.len,
    );
    defer manifest_list.deinit(alloc);
    var manifest_list_put = try client.putObject("bucket", "events/metadata/snap-12.avro", manifest_list.items, .{});
    defer manifest_list_put.deinit(alloc);
    var data_manifest_put = try client.putObject("bucket", "events/metadata/m-a.avro", data_manifest.items, .{});
    defer data_manifest_put.deinit(alloc);
    var delete_manifest_put = try client.putObject("bucket", "events/metadata/d-a.avro", delete_manifest.items, .{});
    defer delete_manifest_put.deinit(alloc);

    const opened_store = try object_store_support.OpenedObjectStore.initWithClient(
        alloc,
        client,
        "bucket",
        "events",
    );
    var opened_source = try OpenedExternalObjectStorageLakeRowsSource.initWithOpenedStoreAlloc(
        alloc,
        schema,
        opened_store,
        .{},
    );
    defer opened_source.deinit();

    const projection = [_][]const u8{"amount"};
    var result = try opened_source.owned_source.pinned_source.scanner.scanAlloc(alloc, schema, .{
        .projected_columns = &projection,
    });
    defer result.deinit(alloc);

    try std.testing.expectEqual(@as(u32, 2), result.total);
    try std.testing.expectEqual(@as(usize, 2), result.rows.len);
    try std.testing.expectEqual(@as(i64, 10), result.rows[0].find("amount").?.value.?.i64);
    try std.testing.expectEqual(@as(i64, 30), result.rows[1].find("amount").?.value.?.i64);
}

test "external lake routing source resolves object store for external row plans" {
    const alloc = std.testing.allocator;
    var columns = [_]storage_schema.RelationalColumn{
        .{ .name = "amount", .path = "amount", .field_type = .numeric, .nullable = false },
    };
    const external_schema = storage_schema.TableSchema{
        .storage_mode = .relational,
        .relational_columns = columns[0..],
        .external_base_source = .{
            .table_id = "events",
            .format = .parquet,
            .source_uri = "s3://bucket/events",
            .snapshot_mode = .current,
            .schema_fingerprint = "schema-v1",
        },
    };
    const local_schema = storage_schema.TableSchema{
        .storage_mode = .relational,
        .relational_columns = columns[0..],
    };

    const FakeBase = struct {
        rows_query_count: u32 = 0,
        document_algebraic_aggregate_count: u32 = 0,

        fn source(self: *@This()) TableReadSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .lookup = lookup,
                    .scan = scan,
                    .query = query,
                    .document_algebraic_aggregate = documentAlgebraicAggregate,
                    .rows_query_plan = rowsQueryPlan,
                },
            };
        }

        fn lookup(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: db_mod.types.LookupOptions, _: raft_mod.ReadConsistency) !?LookupResponse {
            return error.UnexpectedBaseLookup;
        }

        fn scan(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: []const u8, _: db_mod.types.ScanOptions, _: raft_mod.ReadConsistency) !?ScanResponse {
            return error.UnexpectedBaseScan;
        }

        fn query(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: db_mod.types.SearchRequest, _: raft_mod.ReadConsistency) !?query_api.QueryResponse {
            return error.UnexpectedBaseQuery;
        }

        fn rowsQueryPlan(ptr: *anyopaque, _: std.mem.Allocator, _: []const u8, _: storage_schema.TableSchema, _: db_mod.types.RelationalRowsQueryPlan, _: raft_mod.ReadConsistency) !?db_mod.types.RelationalRowsQueryResult {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.rows_query_count += 1;
            return error.BaseRowsQueryReached;
        }

        fn documentAlgebraicAggregate(
            ptr: *anyopaque,
            aggregate_alloc: std.mem.Allocator,
            table_name: []const u8,
            req: document_sql_runtime.AlgebraicAggregateRequest,
            consistency: raft_mod.ReadConsistency,
        ) !?document_sql_runtime.AlgebraicAggregateResponse {
            _ = consistency;
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.document_algebraic_aggregate_count += 1;
            try std.testing.expectEqualStrings("docs", table_name);
            try std.testing.expectEqualStrings("amount_alg", req.index_name);
            try std.testing.expectEqualStrings("avg_by_status", req.materialization_name);
            var rows = try aggregate_alloc.alloc(document_sql_runtime.AlgebraicAggregateRow, 1);
            errdefer aggregate_alloc.free(rows);
            rows[0] = .{
                .value_json = try aggregate_alloc.dupe(u8, "11"),
                .raw_value = try db_mod.algebraic.algebra.encodeAvgAlloc(aggregate_alloc, .{ .sum = 11, .count = 1 }),
            };
            return .{ .rows = rows, .total_groups = 1 };
        }
    };

    const FakeResolver = struct {
        memory: *object_storage_api.MemoryObjectStorage,
        open_count: u32 = 0,

        fn resolver(self: *@This()) ExternalLakeObjectStoreResolver {
            return .{
                .ptr = self,
                .vtable = &.{
                    .open_parquet_prefix = openParquetPrefix,
                },
            };
        }

        fn openParquetPrefix(
            ptr: *anyopaque,
            inner_alloc: std.mem.Allocator,
            binding: external_binding_api.Binding,
            _: ExternalObjectStorageLakeRowsSourceOptions,
        ) !object_store_support.OpenedObjectStore {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.open_count += 1;
            try std.testing.expectEqual(external_source_api.Format.parquet, binding.format);
            try std.testing.expectEqualStrings("s3://bucket/events", binding.source_uri);
            return try object_store_support.OpenedObjectStore.initWithClient(
                inner_alloc,
                self.memory.client(),
                "bucket",
                "events",
            );
        }
    };

    var memory = object_storage_api.MemoryObjectStorage.init(alloc);
    defer memory.deinit();
    var client = memory.client();
    try client.makeBucket("bucket");
    var put = try client.putObject("bucket", "events/part-a.parquet", "not-a-real-parquet-file", .{});
    defer put.deinit(alloc);

    var base = FakeBase{};
    var resolver = FakeResolver{ .memory = &memory };
    var routed = ExternalLakeRoutingTableReadSource.init(base.source(), resolver.resolver(), .{});
    var source = routed.source();
    const projection = [_][]const u8{"amount"};
    const plan = db_mod.types.RelationalRowsQueryPlan{
        .query = .{
            .select = projection[0..],
            .select_all = false,
        },
    };

    try std.testing.expectError(error.InvalidParquetRowGroupBatch, source.rowsQueryPlan(alloc, "events", external_schema, plan, .read_index));
    try std.testing.expectEqual(@as(u32, 1), resolver.open_count);
    try std.testing.expectEqual(@as(u32, 0), base.rows_query_count);

    try std.testing.expectError(error.BaseRowsQueryReached, source.rowsQueryPlan(alloc, "events", local_schema, plan, .read_index));
    try std.testing.expectEqual(@as(u32, 1), resolver.open_count);
    try std.testing.expectEqual(@as(u32, 1), base.rows_query_count);

    var aggregate = (try source.documentAlgebraicAggregate(alloc, "docs", .{
        .index_name = "amount_alg",
        .materialization_name = "avg_by_status",
        .aggregate_op = .avg,
        .group_by = null,
        .limit = null,
    }, .read_index)).?;
    defer aggregate.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 1), base.document_algebraic_aggregate_count);
    try std.testing.expectEqual(@as(u32, 1), aggregate.total_groups);
    try std.testing.expectEqual(@as(usize, 1), aggregate.rows.len);
    try std.testing.expect(aggregate.rows[0].group_json == null);
    try std.testing.expectEqualStrings("11", aggregate.rows[0].value_json);
}

fn icebergRoutingMetadataJson() []const u8 {
    return
    \\{
    \\  "format-version": 2,
    \\  "table-uuid": "uuid-events",
    \\  "location": "s3://bucket/events",
    \\  "current-schema-id": 7,
    \\  "current-snapshot-id": 12,
    \\  "snapshots": [
    \\    {
    \\      "snapshot-id": 12,
    \\      "sequence-number": 42,
    \\      "timestamp-ms": 1700000000000,
    \\      "manifest-list": "s3://bucket/events/metadata/snap-12.avro"
    \\    }
    \\  ]
    \\}
    ;
}

fn buildIcebergRoutingManifestListFixture(alloc: std.mem.Allocator, data_manifest_len: usize) !std.ArrayListUnmanaged(u8) {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try appendIcebergRoutingAvroHeader(alloc, &out, icebergRoutingManifestListSchema(), "0123456789abcdef");

    var block = std.ArrayListUnmanaged(u8).empty;
    defer block.deinit(alloc);
    try appendIcebergRoutingManifestListRecord(alloc, &block, "s3://bucket/events/metadata/m-a.avro", data_manifest_len, 0, .{});

    try appendIcebergRoutingAvroBlock(alloc, &out, block.items, 1, "0123456789abcdef");
    return out;
}

fn buildIcebergRoutingDataAndDeleteManifestListFixture(
    alloc: std.mem.Allocator,
    data_manifest_len: usize,
    delete_manifest_len: usize,
) !std.ArrayListUnmanaged(u8) {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try appendIcebergRoutingAvroHeader(alloc, &out, icebergRoutingManifestListSchema(), "0123456789abcdef");

    var block = std.ArrayListUnmanaged(u8).empty;
    defer block.deinit(alloc);
    try appendIcebergRoutingManifestListRecord(alloc, &block, "s3://bucket/events/metadata/m-a.avro", data_manifest_len, 0, .{
        .added_files = 1,
        .added_rows = 3,
    });
    try appendIcebergRoutingManifestListRecord(alloc, &block, "s3://bucket/events/metadata/d-a.avro", delete_manifest_len, 1, .{
        .added_files = 1,
        .added_rows = 1,
    });

    try appendIcebergRoutingAvroBlock(alloc, &out, block.items, 2, "0123456789abcdef");
    return out;
}

const IcebergRoutingManifestSummary = struct {
    added_files: i64 = 0,
    existing_files: i64 = 0,
    deleted_files: i64 = 0,
    added_rows: i64 = 0,
    existing_rows: i64 = 0,
    deleted_rows: i64 = 0,
};

fn appendIcebergRoutingManifestListRecord(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    manifest_path: []const u8,
    manifest_len: usize,
    content: i64,
    summary: IcebergRoutingManifestSummary,
) !void {
    try appendIcebergRoutingString(alloc, out, manifest_path);
    try appendIcebergRoutingLong(alloc, out, @intCast(manifest_len));
    try appendIcebergRoutingLong(alloc, out, 0);
    try appendIcebergRoutingLong(alloc, out, content);
    try appendIcebergRoutingLong(alloc, out, 42);
    try appendIcebergRoutingLong(alloc, out, summary.added_files);
    try appendIcebergRoutingLong(alloc, out, summary.existing_files);
    try appendIcebergRoutingLong(alloc, out, summary.deleted_files);
    try appendIcebergRoutingLong(alloc, out, summary.added_rows);
    try appendIcebergRoutingLong(alloc, out, summary.existing_rows);
    try appendIcebergRoutingLong(alloc, out, summary.deleted_rows);
}

fn buildIcebergRoutingDataManifestFixture(alloc: std.mem.Allocator) !std.ArrayListUnmanaged(u8) {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try appendIcebergRoutingAvroHeader(alloc, &out, icebergRoutingDataManifestSchema(), "fedcba9876543210");

    var block = std.ArrayListUnmanaged(u8).empty;
    defer block.deinit(alloc);
    try appendIcebergRoutingDataManifestRecord(alloc, &block, 1, "s3://bucket/events/data/a.parquet", 3, 4096);
    try appendIcebergRoutingDataManifestRecord(alloc, &block, 0, "s3://bucket/events/data/b.parquet", 2, 2048);

    try appendIcebergRoutingAvroBlock(alloc, &out, block.items, 2, "fedcba9876543210");
    return out;
}

fn buildIcebergRoutingOneDataManifestFixture(
    alloc: std.mem.Allocator,
    file_path: []const u8,
    record_count: i64,
    file_size_in_bytes: usize,
) !std.ArrayListUnmanaged(u8) {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try appendIcebergRoutingAvroHeader(alloc, &out, icebergRoutingDataManifestSchema(), "fedcba9876543210");

    var block = std.ArrayListUnmanaged(u8).empty;
    defer block.deinit(alloc);
    try appendIcebergRoutingDataManifestRecordWithContent(alloc, &block, 1, 0, file_path, record_count, @intCast(file_size_in_bytes));

    try appendIcebergRoutingAvroBlock(alloc, &out, block.items, 1, "fedcba9876543210");
    return out;
}

fn buildIcebergRoutingDeleteManifestFixture(
    alloc: std.mem.Allocator,
    file_path: []const u8,
    record_count: i64,
    file_size_in_bytes: usize,
) !std.ArrayListUnmanaged(u8) {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try appendIcebergRoutingAvroHeader(alloc, &out, icebergRoutingDataManifestSchema(), "fedcba9876543210");

    var block = std.ArrayListUnmanaged(u8).empty;
    defer block.deinit(alloc);
    try appendIcebergRoutingDataManifestRecordWithContent(alloc, &block, 1, 1, file_path, record_count, @intCast(file_size_in_bytes));

    try appendIcebergRoutingAvroBlock(alloc, &out, block.items, 1, "fedcba9876543210");
    return out;
}

fn icebergRoutingManifestListSchema() []const u8 {
    return
    \\{"type":"record","name":"manifest_file","fields":[
    \\{"name":"manifest_path","type":"string"},
    \\{"name":"manifest_length","type":"long"},
    \\{"name":"partition_spec_id","type":"int"},
    \\{"name":"content","type":"int"},
    \\{"name":"sequence_number","type":"long"},
    \\{"name":"added_files_count","type":"int"},
    \\{"name":"existing_files_count","type":"int"},
    \\{"name":"deleted_files_count","type":"int"},
    \\{"name":"added_rows_count","type":"long"},
    \\{"name":"existing_rows_count","type":"long"},
    \\{"name":"deleted_rows_count","type":"long"}]}
    ;
}

fn icebergRoutingDataManifestSchema() []const u8 {
    return
    \\{"type":"record","name":"manifest_entry","fields":[
    \\{"name":"status","type":"int"},
    \\{"name":"snapshot_id","type":["null","long"]},
    \\{"name":"data_sequence_number","type":["null","long"]},
    \\{"name":"file_sequence_number","type":["null","long"]},
    \\{"name":"data_file","type":{"type":"record","name":"data_file","fields":[
    \\{"name":"content","type":"int"},
    \\{"name":"file_path","type":"string"},
    \\{"name":"file_format","type":"string"},
    \\{"name":"record_count","type":"long"},
    \\{"name":"file_size_in_bytes","type":"long"}]}}]}
    ;
}

fn appendIcebergRoutingDataManifestRecord(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    status: i64,
    file_path: []const u8,
    record_count: i64,
    file_size_in_bytes: i64,
) !void {
    return try appendIcebergRoutingDataManifestRecordWithContent(
        alloc,
        out,
        status,
        0,
        file_path,
        record_count,
        file_size_in_bytes,
    );
}

fn appendIcebergRoutingDataManifestRecordWithContent(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    status: i64,
    content: i64,
    file_path: []const u8,
    record_count: i64,
    file_size_in_bytes: i64,
) !void {
    try appendIcebergRoutingLong(alloc, out, status);
    try appendIcebergRoutingLong(alloc, out, 1);
    try appendIcebergRoutingLong(alloc, out, 12);
    try appendIcebergRoutingLong(alloc, out, 1);
    try appendIcebergRoutingLong(alloc, out, 42);
    try appendIcebergRoutingLong(alloc, out, 1);
    try appendIcebergRoutingLong(alloc, out, 43);
    try appendIcebergRoutingLong(alloc, out, content);
    try appendIcebergRoutingString(alloc, out, file_path);
    try appendIcebergRoutingString(alloc, out, "PARQUET");
    try appendIcebergRoutingLong(alloc, out, record_count);
    try appendIcebergRoutingLong(alloc, out, file_size_in_bytes);
}

fn appendIcebergRoutingAvroHeader(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    schema: []const u8,
    sync: []const u8,
) !void {
    try out.appendSlice(alloc, "Obj\x01");
    try appendIcebergRoutingLong(alloc, out, 2);
    try appendIcebergRoutingString(alloc, out, "avro.schema");
    try appendIcebergRoutingBytes(alloc, out, schema);
    try appendIcebergRoutingString(alloc, out, "avro.codec");
    try appendIcebergRoutingBytes(alloc, out, "null");
    try appendIcebergRoutingLong(alloc, out, 0);
    try out.appendSlice(alloc, sync);
}

fn appendIcebergRoutingAvroBlock(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    block: []const u8,
    count: i64,
    sync: []const u8,
) !void {
    try appendIcebergRoutingLong(alloc, out, count);
    try appendIcebergRoutingLong(alloc, out, @intCast(block.len));
    try out.appendSlice(alloc, block);
    try out.appendSlice(alloc, sync);
}

fn appendIcebergRoutingBytes(alloc: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), bytes: []const u8) !void {
    try appendIcebergRoutingLong(alloc, out, @intCast(bytes.len));
    try out.appendSlice(alloc, bytes);
}

fn appendIcebergRoutingString(alloc: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), text: []const u8) !void {
    try appendIcebergRoutingBytes(alloc, out, text);
}

fn appendIcebergRoutingLong(alloc: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), value: i64) !void {
    var remaining = encodeIcebergRoutingZigzag(value);
    while (remaining >= 0x80) {
        try out.append(alloc, @as(u8, @intCast(remaining & 0x7f)) | 0x80);
        remaining >>= 7;
    }
    try out.append(alloc, @intCast(remaining));
}

fn encodeIcebergRoutingZigzag(value: i64) u64 {
    if (value >= 0) return @as(u64, @intCast(value)) << 1;
    const magnitude: u64 = @intCast(-(value + 1));
    return (magnitude << 1) | 1;
}

test "configured external lake resolver opens credentialed filesystem connection" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const allowed_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/allowed/events", .{tmp.sub_path});
    defer alloc.free(allowed_path);
    const denied_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/denied/events", .{tmp.sub_path});
    defer alloc.free(denied_path);

    const cfg_json = try std.fmt.allocPrint(alloc,
        \\{{
        \\  "connections": {{
        \\    "prod-lake-read": {{
        \\      "kind": "external_io",
        \\      "capabilities": ["lake_read"],
        \\      "external_io": {{
        \\        "protocol": "filesystem",
        \\        "prefix": ".zig-cache/tmp/{s}/allowed"
        \\      }}
        \\    }}
        \\  }}
        \\}}
    , .{tmp.sub_path});
    defer alloc.free(cfg_json);
    var cfg = try common_config.Config.parseFromSlice(alloc, cfg_json);
    defer cfg.deinit();

    var resolver = ConfiguredExternalLakeObjectStoreResolver{};
    resolver.configure(&cfg, null);
    const source = resolver.resolver();

    const allowed_uri = try std.fmt.allocPrint(alloc, "file://{s}", .{allowed_path});
    defer alloc.free(allowed_uri);
    var opened = try source.openParquetPrefixAlloc(alloc, .{
        .table_id = "events",
        .format = .parquet,
        .source_uri = allowed_uri,
        .credential_ref = .{ .ref_id = "prod-lake-read", .scope = "events" },
        .schema_fingerprint = "schema-v1",
    }, .{});
    defer opened.deinit();
    try std.testing.expectEqualStrings("external-lake", opened.bucket);

    const denied_uri = try std.fmt.allocPrint(alloc, "file://{s}", .{denied_path});
    defer alloc.free(denied_uri);
    try std.testing.expectError(error.ExternalLakeCredentialScopeMismatch, source.openParquetPrefixAlloc(alloc, .{
        .table_id = "events",
        .format = .parquet,
        .source_uri = denied_uri,
        .credential_ref = .{ .ref_id = "prod-lake-read", .scope = "events" },
        .schema_fingerprint = "schema-v1",
    }, .{}));

    const no_lake_read_cfg_json = try std.fmt.allocPrint(alloc,
        \\{{
        \\  "connections": {{
        \\    "prod-lake-read": {{
        \\      "kind": "external_io",
        \\      "capabilities": ["lake_write"],
        \\      "external_io": {{
        \\        "protocol": "filesystem",
        \\        "prefix": ".zig-cache/tmp/{s}/allowed"
        \\      }}
        \\    }}
        \\  }}
        \\}}
    , .{tmp.sub_path});
    defer alloc.free(no_lake_read_cfg_json);
    var no_lake_read_cfg = try common_config.Config.parseFromSlice(alloc, no_lake_read_cfg_json);
    defer no_lake_read_cfg.deinit();
    resolver.configure(&no_lake_read_cfg, null);
    try std.testing.expectError(error.UnsupportedExternalLakeCredentialRef, source.openParquetPrefixAlloc(alloc, .{
        .table_id = "events",
        .format = .parquet,
        .source_uri = allowed_uri,
        .credential_ref = .{ .ref_id = "prod-lake-read", .scope = "events" },
        .schema_fingerprint = "schema-v1",
    }, .{}));
}
