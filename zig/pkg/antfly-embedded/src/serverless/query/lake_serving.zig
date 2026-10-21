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

//! Serving adapters over the maintained lake engine. Each owner pins inventory,
//! object versions and Iceberg deletes before any row is published.
const std = @import("std");
const storage_schema = @import("../../storage/schema.zig");
const external_binding_api = @import("../external_source/catalog_binding.zig");
const external_source_api = @import("../external_source/mod.zig");
const sidecar_manifest_api = @import("../segment/sidecar_manifest.zig");
const serverless_query = @import("lake_api.zig");
const serverless_algebraic_segment = @import("../algebraic_segment/mod.zig");
const rowsource_api = @import("../../storage/rowsource/types.zig");
const object_storage_api = @import("../../storage/object_storage.zig");
const object_store_support = @import("../object_store_support.zig");
const host_store = @import("../lake_host.zig");
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
    shared_reader: ?*@import("lake_serving_cache.zig").Reader = null,

    pub fn init(
        inventory: external_source_api.Inventory,
        client: object_storage_api.ObjectStorage,
    ) PinnedExternalObjectStorageLakeRowsScanner {
        return .{
            .inventory = inventory,
            .object_reader = serverless_query.LakeObjectStorageRangeReader.init(client),
        };
    }

    pub fn reader(self: *@This()) serverless_query.LakeParquetObjectRangeReader {
        if (self.shared_reader) |cached| return cached.reader();
        return self.object_reader.parquetReader();
    }

    pub fn parquetScanner(self: *@This()) PinnedExternalLakeRowsScanner {
        return .{
            .inventory = self.inventory,
            .reader = self.reader(),
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
        const binding = external_base_source.binding;
        var validation = try serverless_query.planProjectedLakeScanAlloc(alloc, .{
            .binding = binding,
            .inventory = self.inventory,
            .projected_columns = request.projected_columns,
        });
        defer validation.deinit(alloc);

        var discovered = serverless_query.discoverLakeParquetSupportedI64ObjectRangeRowGroupsFromFootersAlloc(
            alloc,
            self.reader(),
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
        const binding = external_base_source.binding;
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
            self.reader(),
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
                .reader = self.reader(),
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
            .reader = self.reader(),
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
        const binding = external_base_source.binding;
        if (binding.format != .parquet and binding.format != .iceberg) return error.UnsupportedRowsQuery;

        var local_request = request;
        var iceberg_deleted_refs: []rowsource_api.RowRef = &.{};
        defer if (iceberg_deleted_refs.len > 0) alloc.free(iceberg_deleted_refs);
        var combined_deleted_refs: []rowsource_api.RowRef = &.{};
        defer if (combined_deleted_refs.len > 0) alloc.free(combined_deleted_refs);

        if (self.iceberg_delete_plan) |delete_plan| {
            iceberg_deleted_refs = try serverless_query.readLakeIcebergDeleteRowRefsAlloc(alloc, .{
                .reader = self.reader(),
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
            .reader = self.reader(),
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
    const binding = external_base_source.binding;
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
        .scan_limits = request.limits,
        .coalesce_options = coalesce_options,
        .sidecars = sidecar_context.sidecars,
        .desired_sidecars = sidecar_context.desired_sidecars,
        .sidecar_policy = sidecar_context.sidecar_policy,
        .candidates = sidecar_context.candidates,
    });
}

pub const DeleteQualification = @import("lake_delete_qualification.zig").Qualification;

pub const ServingSource = struct {
    /// Authorized publication navigation is query-owned. Lookups return fully
    /// owned entries; full scans can rediscover the frozen metadata normally.
    pub const FileIterator = struct {
        ptr: *anyopaque,
        next: *const fn (*anyopaque, std.mem.Allocator, @import("lake_read_context.zig").Context) anyerror!?external_source_api.FileEntry,
        split: ?*const fn (*anyopaque, std.mem.Allocator, usize, usize, @import("lake_read_context.zig").Context) anyerror!FileIterator = null,
        has_more: ?*const fn (*anyopaque, @import("lake_read_context.zig").Context) anyerror!bool = null,
        close: *const fn (*anyopaque) void,
    };
    pub const Estimates = struct { rows: u64, bytes: u64 };
    pub const FileNavigation = struct {
        ptr: *anyopaque,
        statistics: []const external_source_api.types.ColumnStatistics = &.{},
        count: usize,
        plan: [32]u8,
        lookup: *const fn (*anyopaque, std.mem.Allocator, []const u8, @import("lake_read_context.zig").Context) anyerror!external_source_api.FileEntry,
        close: *const fn (*anyopaque) void,
        delete_proof: ?@import("lake_delete_qualification.zig").Qualification.Proof = null,
        enumerate: ?*const fn (*anyopaque, std.mem.Allocator, @import("lake_read_context.zig").Context) anyerror!FileIterator = null,
        estimates: ?Estimates = null,
    };
    pub const SnapshotPlans = struct { requested: [32]u8, current: [32]u8, selected: [32]u8 };
    pub const SnapshotNavigation = struct {
        ptr: *anyopaque,
        resolve: *const fn (*anyopaque, std.mem.Allocator, SnapshotPlans, external_source_api.Inventory, @import("lake_read_context.zig").Context) anyerror!?FileNavigation,
    };
    const DeferredSnapshot = struct {
        metadata: []u8,
        uri: []u8,
        snapshot: ?[]u8,
        cache: *@import("lake_serving_cache.zig").Cache,
        key: [32]u8,
        scope: [32]u8,
        fn deinit(self: @This(), a: std.mem.Allocator) void {
            a.free(self.metadata);
            a.free(self.uri);
            if (self.snapshot) |id| a.free(id);
        }
    };

    /// Query-owned translation for one verified, publication-bound index cut.
    /// Parallel views borrow it until they have joined their parent source.
    pub const RowResolver = struct {
        ptr: *anyopaque,
        source: []const u8,
        snapshot: []const u8,
        current_source: []const u8,
        current_snapshot: []const u8,
        rows: u64,
        from_index: *const fn (*anyopaque, std.mem.Allocator, @import("../../storage/rowsource/types.zig").RowRef) anyerror!@import("../../storage/rowsource/types.zig").RowRef,
        to_index: *const fn (*anyopaque, std.mem.Allocator, @import("../../storage/rowsource/types.zig").RowRef) anyerror!@import("../../storage/rowsource/types.zig").RowRef,
        file: *const fn (*anyopaque, std.mem.Allocator, []const u8) anyerror!?[]const u8,
        digest: *const fn (*anyopaque, std.mem.Allocator, []const u8) anyerror![]const u8,
        tie_rank: *const fn (*anyopaque, std.mem.Allocator, @import("../../storage/rowsource/types.zig").RowRef) anyerror!u64,
        close: *const fn (*anyopaque) void,
    };
    pub const FileVersions = struct { etag: []u8, version_id: []u8 };
    /// Query-owned authenticated selector pages retain independent native
    /// reader authority until source teardown. Caches contain data only.
    pub const VerifiedSelector = struct {
        ptr: *anyopaque,
        read: *const fn (*anyopaque, std.mem.Allocator, external_source_api.FileEntry, @import("lake_read_context.zig").Context) anyerror!FileVersions,
        close: *const fn (*anyopaque) void,
    };
    file_navigation: ?FileNavigation = null,
    row_resolver: ?RowResolver = null,
    planner_statistics: []const external_source_api.types.ColumnStatistics = &.{},
    statistics_owner: ?struct { ptr: *anyopaque, close: *const fn (*anyopaque) void } = null,
    deferred_snapshot: ?DeferredSnapshot = null,
    catalog_files_owned: bool = false,
    alloc: std.mem.Allocator,
    snapshot_pin: ?host_store.SnapshotPin = null,
    store: object_store_support.OpenedObjectStore,
    inventory: external_source_api.Inventory,
    scanner: PinnedExternalObjectStorageLakeRowsScanner,
    iceberg_schema: ?@import("lake_schema.zig").Detected = null,
    partition_rules: ?@import("lake_partition_pruning.zig").Rules = null,
    plan_identity: ?[32]u8 = null,
    plan_lease: ?@import("lake_decoded_cache.zig").Lease = null,
    delete_lease: ?@import("lake_decoded_cache.zig").Lease = null,
    local_plan: ?PreparedPlan = null,
    inventory_owned: bool = true,
    catalog_files: bool = false,
    selected_catalog_view: bool = false,
    attachment_uri: ?[]u8 = null,
    // Optional shared-plan admission cannot impose a new statement limit.
    // Private overflow plans use the caller allocator and pin their chunks.
    private_snapshot: ?*@import("lake_iceberg_snapshot.zig").SnapshotWithDeletePlan = null,
    private_snapshot_owned: bool = false,
    local_plan_owned: bool = true,
    // Exact immutable metadata used to construct this inventory, independent
    // of the attachment URI and subsequent catalog pointer changes.
    catalog_metadata: ?@import("../external_source/lake_catalog/types.zig").Table = null,
    // Version evidence is query-local; shared manifest bytes are never mutated.
    versions: std.AutoHashMapUnmanaged(usize, external_source_api.FileEntry) = .empty,
    version_parent: ?*const ServingSource = null,
    lazy_versions: bool = false,
    immutable_objects: bool = false,
    pinned_files: []bool = &.{},
    pinned_files_owned: bool = true,
    // Only installed after a full coverage check against this immutable plan.
    verified_files: ?*const std.StringHashMapUnmanaged(*const external_source_api.FileEntry) = null,
    verified_inventory_lease: ?@import("lake_decoded_cache.zig").Lease = null,
    verified_selector: ?VerifiedSelector = null,
    context_store: ?*@import("lake_read_context.zig").Store = null,
    prepared_deletes: ?*@import("lake_prepared_deletes.zig").Prepared = null,
    delete_qualification: ?*@import("lake_delete_qualification.zig").Qualification = null,
    delete_qualification_owned: bool = false,

    pub fn acceptsIndexCut(self: *const ServingSource, source: []const u8, snapshot: []const u8) bool {
        const expected_source = if (self.row_resolver) |resolver| resolver.source else self.inventory.source_id;
        const expected_snapshot = if (self.row_resolver) |resolver| resolver.snapshot else self.inventory.snapshot_id;
        return std.mem.eql(u8, source, expected_source) and std.mem.eql(u8, snapshot, expected_snapshot);
    }
    pub fn fromIndex(self: *const ServingSource, a: std.mem.Allocator, ref: @import("../../storage/rowsource/types.zig").RowRef) !@import("../../storage/rowsource/types.zig").RowRef {
        if (self.row_resolver) |resolver| return resolver.from_index(resolver.ptr, a, ref);
        return ref;
    }
    pub fn toIndex(self: *const ServingSource, a: std.mem.Allocator, ref: @import("../../storage/rowsource/types.zig").RowRef) !@import("../../storage/rowsource/types.zig").RowRef {
        if (self.row_resolver) |resolver| return resolver.to_index(resolver.ptr, a, ref);
        return ref;
    }

    /// Parent remains pinned and quiescent until every view joins. Immutable
    /// inventory/deletes/selector authority are borrowed; mutable version maps
    /// and range-reader flights are private to the lane.
    pub fn parallelView(self: *ServingSource, a: std.mem.Allocator) !ServingSource {
        try self.ensureDeleteQualification();
        var view = self.*;
        view.alloc = a;
        view.delete_qualification_owned = false;
        view.inventory_owned = false;
        view.private_snapshot_owned = false;
        view.local_plan_owned = false;
        view.catalog_files = false;
        view.pinned_files_owned = false;
        view.attachment_uri = null;
        view.versions = .empty;
        view.delete_lease = if (self.delete_lease) |lease| lease.retain() else null;
        view.scanner.shared_reader = null;
        errdefer view.closeParallelView(self);
        view.version_parent = self;
        if (self.scanner.shared_reader) |reader| {
            const owned = try a.create(@import("lake_serving_cache.zig").Reader);
            owned.* = .{ .base = reader.base, .cache = reader.cache, .scope = reader.scope, .context = reader.context };
            view.scanner.shared_reader = owned;
        }
        return view;
    }
    pub fn closeParallelView(self: *ServingSource, parent: *ServingSource) void {
        if (self.delete_qualification_owned) if (self.delete_qualification) |qualification| qualification.destroy();
        if (self.scanner.shared_reader) |reader| {
            reader.drain(true);
            self.alloc.destroy(reader);
        }
        if (self.delete_lease) |lease| lease.release() else if (self.prepared_deletes != parent.prepared_deletes) if (self.prepared_deletes) |prepared| prepared.destroy(self.alloc);
        self.clearVersions();
        self.freeCatalogFiles();
        if (self.inventory_owned) self.inventory.deinit(self.alloc);
        if (self.pinned_files_owned) self.alloc.free(self.pinned_files);
        if (self.attachment_uri) |uri| self.alloc.free(uri);
        self.clearLocalPlan();
        if (self.private_snapshot_owned) if (self.private_snapshot) |snapshot| {
            snapshot.deinit(self.alloc);
            self.alloc.destroy(snapshot);
        };
        if (self.plan_lease) |lease| if (parent.plan_lease == null or lease.item != parent.plan_lease.?.item) lease.release();
    }

    fn ensureDeleteQualification(self: *ServingSource) !void {
        if (self.delete_qualification != null) return;
        const plan = self.scanner.iceberg_delete_plan orelse return;
        if (plan.files.len == 0) return;
        self.delete_qualification = try @import("lake_delete_qualification.zig").Qualification.create(self.alloc);
        self.delete_qualification_owned = true;
    }
    pub fn qualifiedDeletes(self: *ServingSource, context: @import("lake_read_context.zig").Context) !?*const @import("lake_delete_qualification.zig").Qualification.Data {
        const checked = self.protectContext(context);
        try checked.ensureActive();
        if (self.context_store) |store| try store.ensureActive();
        const plan = self.scanner.iceberg_delete_plan orelse return null;
        try self.ensureDeleteQualification();
        const qualification = self.delete_qualification orelse return null;
        const proof = if (self.immutable_objects and self.snapshot_pin != null and self.file_navigation != null) self.file_navigation.?.delete_proof else null;
        return qualification.acquireProven(self.scanner.object_reader.client, plan, checked, proof);
    }

    pub fn open(alloc: std.mem.Allocator, schema: storage_schema.TableSchema, options: host_store.OpenOptions) !ServingSource {
        return openWithContext(alloc, schema, options, .{});
    }

    pub fn openWithContext(alloc: std.mem.Allocator, schema: storage_schema.TableSchema, options: host_store.OpenOptions, context: @import("lake_read_context.zig").Context) !ServingSource {
        return openCached(alloc, schema, options, context, null);
    }
    pub fn openCached(alloc: std.mem.Allocator, schema: storage_schema.TableSchema, options: host_store.OpenOptions, context: @import("lake_read_context.zig").Context, cache: ?*@import("lake_serving_cache.zig").Cache) !ServingSource {
        return openCachedMode(alloc, schema, options, context, cache, false, null);
    }
    pub fn openLazyCached(alloc: std.mem.Allocator, schema: storage_schema.TableSchema, options: host_store.OpenOptions, context: @import("lake_read_context.zig").Context, cache: ?*@import("lake_serving_cache.zig").Cache) !ServingSource {
        return openCachedMode(alloc, schema, options, context, cache, true, null);
    }
    pub fn openLazyNavigated(alloc: std.mem.Allocator, schema: storage_schema.TableSchema, options: host_store.OpenOptions, context: @import("lake_read_context.zig").Context, cache: *@import("lake_serving_cache.zig").Cache, navigation: SnapshotNavigation) !ServingSource {
        return openCachedMode(alloc, schema, options, context, cache, true, navigation);
    }
    fn openCachedMode(alloc: std.mem.Allocator, schema: storage_schema.TableSchema, options: host_store.OpenOptions, context: @import("lake_read_context.zig").Context, cache: ?*@import("lake_serving_cache.zig").Cache, lazy: bool, navigation: ?SnapshotNavigation) !ServingSource {
        try context.ensureActive();
        const binding = (schema.external_base_source orelse return error.InvalidExternalTableBinding).binding;
        const identity_version = options.row_identity_version orelse binding.row_identity_version;
        var store = try options.open(alloc, binding);
        errdefer store.deinit();
        const context_store = try alloc.create(@import("lake_read_context.zig").Store);
        errdefer alloc.destroy(context_store);
        context_store.* = .{ .base = store.client, .context = context };
        var client = context_store.client(alloc);
        const base = if (store.fs_client != null) try std.fmt.allocPrint(alloc, "object://{s}/{s}", .{ store.bucket, store.prefix }) else null;
        defer if (base) |value| alloc.free(value);
        var catalog_uuid: ?[]u8 = null;
        defer if (catalog_uuid) |uuid| alloc.free(uuid);
        var iceberg_schema: ?@import("lake_schema.zig").Detected = null;
        errdefer if (iceberg_schema) |*value| value.deinit();
        var partition_rules: ?@import("lake_partition_pruning.zig").Rules = null;
        errdefer if (partition_rules) |*rules| rules.deinit();
        var retained_metadata: ?@import("../external_source/lake_catalog/types.zig").Table = null;
        errdefer if (retained_metadata) |*table| table.deinit(alloc);
        var attachment_uri: ?[]u8 = null;
        errdefer if (attachment_uri) |uri| alloc.free(uri);
        var file_navigation: ?FileNavigation = null;
        errdefer if (file_navigation) |value| value.close(value.ptr);
        var deferred_snapshot: ?DeferredSnapshot = null;
        errdefer if (deferred_snapshot) |value| value.deinit(alloc);
        var plan_identity: ?[32]u8 = null;
        var plan_lease: ?@import("lake_decoded_cache.zig").Lease = null;
        var deletes: ?serverless_query.LakeIcebergDeletePlan = null;
        errdefer if (plan_lease == null) if (deletes) |*value| value.deinit(alloc);
        errdefer if (plan_lease) |lease| lease.release();
        var inventory = switch (binding.format) {
            .parquet => blk: {
                if (!@import("lake_parquet_manifest.zig").isManifest(binding.source_uri)) break :blk try external_source_api.planParquetPrefixInventoryFromObjectStorageAlloc(alloc, .{
                    .client = client,
                    .bucket = store.bucket,
                    .prefix = store.prefix,
                    .source_id = binding.table_id,
                    .source_uri = binding.source_uri,
                    .object_uri_base = base,
                    .schema_fingerprint = binding.schema_fingerprint,
                });
                const manifest_api = @import("lake_parquet_manifest.zig");
                const manifest_uri = base orelse binding.source_uri;
                const bytes = try @import("lake_iceberg_snapshot.zig").readFullObjectAlloc(alloc, &client, null, manifest_uri, .iceberg_metadata, null, 1024 * 1024);
                defer alloc.free(bytes);
                var snapshot: @import("lake_iceberg_snapshot.zig").SnapshotWithDeletePlan = undefined;
                if (cache) |shared| {
                    const scope = try cacheScope(alloc, store, binding);
                    const key = snapshotPlanKey(scope, manifest_uri, bytes, binding.snapshot_mode.pinnedSnapshotId());
                    plan_identity = key;
                    var loader: SnapshotPlanLoader = .{ .identity_version = identity_version, .client = client, .source = binding.table_id, .uri = manifest_uri, .attachment = binding.source_uri, .schema = binding.schema_fingerprint, .snapshot = binding.snapshot_mode.pinnedSnapshotId(), .metadata = bytes, .chunks = &shared.inventory_chunks, .catalogs = &shared.inventory_catalogs, .scope = scope, .context = context };
                    if (navigation != null and binding.object_mutability == .immutable) {
                        var header = try manifest_api.header(alloc, binding.table_id, binding.source_uri, binding.schema_fingerprint, bytes);
                        header.row_identity_version = identity_version;
                        var adopted = false;
                        defer if (!adopted) header.deinit(alloc);
                        file_navigation = try navigation.?.resolve(navigation.?.ptr, alloc, .{ .requested = key, .current = key, .selected = key }, header, context);
                        if (file_navigation != null) {
                            const metadata_copy = try alloc.dupe(u8, bytes);
                            errdefer alloc.free(metadata_copy);
                            const uri_copy = try alloc.dupe(u8, manifest_uri);
                            errdefer alloc.free(uri_copy);
                            const snapshot_copy = if (binding.snapshot_mode.pinnedSnapshotId()) |value| try alloc.dupe(u8, value) else null;
                            deferred_snapshot = .{ .metadata = metadata_copy, .uri = uri_copy, .snapshot = snapshot_copy, .cache = shared, .key = key, .scope = scope };
                            snapshot = .{ .inventory = header };
                            adopted = true;
                        }
                    }
                    if (file_navigation == null) {
                        var identity_hash = std.crypto.hash.Blake3.init(.{});
                        identity_hash.update(&key);
                        identity_hash.update(&.{identity_version});
                        var cache_key: [32]u8 = undefined;
                        identity_hash.final(&cache_key);
                        const lease = try shared.decoded.acquire(cache_key, 64 * 1024 * 1024, context, .{ .ptr = &loader, .load = SnapshotPlanLoader.load });
                        plan_lease = lease;
                        snapshot = lease.item.payload.snapshot;
                    }
                } else snapshot = try manifest_api.plan(alloc, .{ .client = client, .source = binding.table_id, .uri = binding.source_uri, .schema = binding.schema_fingerprint, .context = context }, bytes);
                break :blk snapshot.inventory;
            },
            .iceberg => blk: {
                var catalog_table = if (binding.catalog != null) try options.resolveCatalog(alloc, binding, store, context) else null;
                defer if (catalog_table) |*table| table.deinit(alloc);
                const uri = if (catalog_table) |table| try alloc.dupe(u8, table.metadata_location) else try icebergMetadataUriForOpenedStoreAlloc(alloc, client, store.bucket, store.prefix, binding.source_uri, base);
                defer alloc.free(uri);
                const metadata_bytes = if (catalog_table) |table| try alloc.dupe(u8, table.metadata_json) else try @import("lake_iceberg_snapshot.zig").readFullObjectAlloc(alloc, &client, null, uri, .iceberg_metadata, null, 16 * 1024 * 1024);
                defer alloc.free(metadata_bytes);
                if (binding.catalog != null) {
                    const location = try alloc.dupe(u8, uri);
                    errdefer alloc.free(location);
                    retained_metadata = .{ .metadata_location = location, .metadata_json = try alloc.dupe(u8, metadata_bytes) };
                }
                if (binding.catalog != null) {
                    var metadata = try std.json.parseFromSlice(std.json.Value, alloc, metadata_bytes, .{});
                    defer metadata.deinit();
                    const uuid = metadata.value.object.get("table-uuid") orelse return error.InvalidLakeMetadata;
                    if (uuid != .string) return error.InvalidLakeMetadata;
                    catalog_uuid = try alloc.dupe(u8, uuid.string);
                }
                var snapshot: @import("lake_iceberg_snapshot.zig").SnapshotWithDeletePlan = undefined;
                if (cache) |shared| {
                    // Revalidate the current metadata pointer on every open.
                    // Only its immutable content and credential scope are reused.
                    const scope = try cacheScope(alloc, store, binding);
                    var hash = std.crypto.hash.sha2.Sha256.init(.{});
                    hash.update(&scope);
                    hash.update("iceberg-plan-v1");
                    @import("lake_prepared_deletes.zig").Prepared.hashObjectVersion(&hash, uri, metadata_bytes, binding.snapshot_mode.pinnedSnapshotId() orelse "");
                    const key = hash.finalResult();
                    plan_identity = key;
                    var loader: SnapshotPlanLoader = .{ .identity_version = identity_version, .client = client, .source = binding.table_id, .uri = uri, .snapshot = binding.snapshot_mode.pinnedSnapshotId(), .metadata = metadata_bytes, .manifests = if (binding.object_mutability == .immutable) &shared.manifests else null, .chunks = if (binding.object_mutability == .immutable) &shared.inventory_chunks else null, .catalogs = if (binding.object_mutability == .immutable) &shared.inventory_catalogs else null, .scope = scope, .context = context };
                    // Coalesce cold manifest discovery as well as warm residency.
                    // Waiters retain their own authority, cancellation and lease;
                    // the cache owns only the immutable inventory/delete plan.
                    if (navigation != null and binding.object_mutability == .immutable) {
                        var header = try @import("lake_iceberg_snapshot.zig").snapshotHeaderAlloc(alloc, .{ .client = client, .source_id = binding.table_id, .metadata_uri = uri, .requested_snapshot_id = binding.snapshot_mode.pinnedSnapshotId() }, metadata_bytes);
                        header.row_identity_version = identity_version;
                        var adopted = false;
                        defer if (!adopted) header.deinit(alloc);
                        file_navigation = try navigation.?.resolve(navigation.?.ptr, alloc, .{ .requested = key, .current = snapshotPlanKey(scope, uri, metadata_bytes, null), .selected = snapshotPlanKey(scope, uri, metadata_bytes, header.snapshot_id) }, header, context);
                        if (file_navigation != null) {
                            const owned_metadata = try alloc.dupe(u8, metadata_bytes);
                            errdefer alloc.free(owned_metadata);
                            const owned_uri = try alloc.dupe(u8, uri);
                            errdefer alloc.free(owned_uri);
                            const owned_snapshot = if (binding.snapshot_mode.pinnedSnapshotId()) |id| try alloc.dupe(u8, id) else null;
                            errdefer if (owned_snapshot) |id| alloc.free(id);
                            snapshot = .{ .inventory = header, .delete_plan = try @import("lake_iceberg_snapshot.zig").deletePlanFromMetadataAlloc(alloc, .{ .client = client, .source_id = binding.table_id, .metadata_uri = uri, .requested_snapshot_id = binding.snapshot_mode.pinnedSnapshotId(), .manifests = loader.manifests, .manifest_scope = scope, .context = context }, metadata_bytes) };
                            adopted = true;
                            plan_identity = file_navigation.?.plan;
                            deferred_snapshot = .{ .metadata = owned_metadata, .uri = owned_uri, .snapshot = owned_snapshot, .cache = shared, .key = key, .scope = scope };
                        }
                    }
                    if (file_navigation == null) {
                        var identity_hash = std.crypto.hash.Blake3.init(.{});
                        identity_hash.update(&key);
                        identity_hash.update(&.{identity_version});
                        var cache_key: [32]u8 = undefined;
                        identity_hash.final(&cache_key);
                        const lease = try shared.decoded.acquire(cache_key, 64 * 1024 * 1024, context, .{ .ptr = &loader, .load = SnapshotPlanLoader.load });
                        plan_lease = lease;
                        snapshot = lease.item.payload.snapshot;
                    }
                } else {
                    const snapshot_owned = try @import("lake_iceberg_snapshot.zig").planSnapshotInventoryAndDeletePlanFromMetadataAlloc(alloc, .{
                        .client = client,
                        .source_id = binding.table_id,
                        .metadata_uri = uri,
                        .requested_snapshot_id = binding.snapshot_mode.pinnedSnapshotId(),
                    }, metadata_bytes);
                    snapshot = snapshot_owned;
                }
                deletes = snapshot.delete_plan;
                errdefer if (plan_lease == null) snapshot.inventory.deinit(alloc);
                if (base) |object_base| {
                    if (!std.mem.eql(u8, std.mem.trimEnd(u8, snapshot.inventory.source_uri, "/"), std.mem.trimEnd(u8, object_base, "/")) and
                        !(binding.catalog != null and std.mem.eql(u8, std.mem.trimEnd(u8, snapshot.inventory.source_uri, "/"), std.mem.trimEnd(u8, binding.source_uri, "/")))) return error.ExternalLakeSnapshotMismatch;
                    if (plan_lease == null) {
                        const uri_copy = try alloc.dupe(u8, binding.source_uri);
                        alloc.free(@constCast(snapshot.inventory.source_uri));
                        snapshot.inventory.source_uri = uri_copy;
                    } else {
                        attachment_uri = try alloc.dupe(u8, binding.source_uri);
                        snapshot.inventory.source_uri = attachment_uri.?;
                    }
                } else if (std.mem.endsWith(u8, binding.source_uri, ".metadata.json")) {
                    // Explicit object-store metadata locations are attachment
                    // identities; the metadata's location names the data root.
                    if (plan_lease == null) {
                        const uri_copy = try alloc.dupe(u8, binding.source_uri);
                        alloc.free(@constCast(snapshot.inventory.source_uri));
                        snapshot.inventory.source_uri = uri_copy;
                    } else {
                        attachment_uri = try alloc.dupe(u8, binding.source_uri);
                        snapshot.inventory.source_uri = attachment_uri.?;
                    }
                }
                {
                    iceberg_schema = try @import("lake_schema.zig").icebergSchema(alloc, metadata_bytes, binding.snapshot_mode.pinnedSnapshotId());
                    if (!std.mem.eql(u8, iceberg_schema.?.fingerprint, snapshot.inventory.schema_fingerprint)) return error.ExternalLakeSnapshotMismatch;
                    if ((if (file_navigation) |value| value.count else if (snapshot.catalog) |index| index.count() else snapshot.inventory.files.len) != 0) partition_rules = try @import("lake_partition_pruning.zig").parseAlloc(alloc, uri, metadata_bytes, snapshot.inventory.snapshot_id, binding.snapshot_mode.pinnedSnapshotId(), snapshot.inventory.schema_fingerprint);
                }
                if (cache == null) try serverless_query.pinLakeIcebergInventoryDataFileObjectVersionsAlloc(alloc, client, &snapshot.inventory);
                break :blk snapshot.inventory;
            },
            .lance => return error.UnsupportedRowsQuery,
        };
        errdefer if (plan_lease == null) inventory.deinit(alloc);
        if (plan_lease != null or file_navigation != null) try @import("lake_scan_plan.zig").validateBindingInventoryIdentity(binding, inventory) else try serverless_query.validateLakeBindingInventory(binding, inventory);
        inventory.row_identity_version = identity_version;
        var scanner = PinnedExternalObjectStorageLakeRowsScanner.init(inventory, client);
        scanner.iceberg_delete_plan = deletes;
        const lazy_versions = cache != null and binding.format == .iceberg;
        const pinned_files: []bool = if (plan_lease == null and (lazy_versions or binding.object_mutability == .immutable)) try alloc.alloc(bool, inventory.files.len) else &.{};
        errdefer alloc.free(pinned_files);
        @memset(pinned_files, false);
        var snapshot_pin: ?host_store.SnapshotPin = null;
        if (options.snapshot_pin_resolver) |resolver| if (binding.catalog != null) {
            snapshot_pin = try resolver.acquire(resolver.ptr, alloc, binding, inventory.snapshot_id, catalog_uuid orelse return error.InvalidLakeMetadata, context);
        };
        errdefer if (snapshot_pin) |pin| pin.deinit(pin.ptr);
        if (snapshot_pin) |pin| context_store.extra_checkpoint = .{ .ptr = pin.ptr, .check = pin.check };
        const local_plan = if (plan_lease == null and file_navigation == null) try PreparedPlan.init(alloc, inventory) else null;
        var result: ServingSource = .{ .file_navigation = file_navigation, .deferred_snapshot = deferred_snapshot, .alloc = alloc, .snapshot_pin = snapshot_pin, .store = store, .inventory = inventory, .scanner = scanner, .context_store = context_store, .partition_rules = partition_rules, .iceberg_schema = iceberg_schema, .plan_identity = plan_identity, .plan_lease = plan_lease, .local_plan = local_plan, .inventory_owned = plan_lease == null, .attachment_uri = attachment_uri, .catalog_metadata = retained_metadata, .lazy_versions = lazy_versions, .immutable_objects = binding.object_mutability == .immutable, .pinned_files = pinned_files };
        errdefer result.freeCatalogFiles();
        try result.ensureDeleteQualification();
        errdefer if (result.delete_qualification_owned) if (result.delete_qualification) |qualification| qualification.destroy();
        if (!lazy) {
            try result.ensureInventory(context);
            try result.ensureCanonicalPlan(context);
        }
        return result;
    }

    fn snapshotPlanKey(scope: [32]u8, uri: []const u8, metadata: []const u8, snapshot: ?[]const u8) [32]u8 {
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        hash.update(&scope);
        hash.update("iceberg-plan-v1");
        @import("lake_prepared_deletes.zig").Prepared.hashObjectVersion(&hash, uri, metadata, snapshot orelse "");
        return hash.finalResult();
    }
    const SnapshotPlanLoader = struct {
        identity_version: u8 = 1,
        client: @import("../../storage/object_storage.zig").ObjectStorage,
        source: []const u8,
        uri: []const u8,
        attachment: ?[]const u8 = null,
        schema: []const u8 = "auto",
        snapshot: ?[]const u8,
        metadata: []const u8,
        manifests: ?*@import("lake_decoded_cache.zig").Cache = null,
        chunks: ?*@import("lake_decoded_cache.zig").Cache = null,
        catalogs: ?*@import("lake_inventory_catalog.zig").Cache = null,
        scope: [32]u8 = @splat(0),
        context: @import("lake_read_context.zig").Context = .{},
        fn load(raw: *anyopaque, item: *@import("lake_decoded_cache.zig").Item) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const decoded = if (@import("lake_parquet_manifest.zig").isManifest(self.uri))
                try @import("lake_parquet_manifest.zig").plan(item.budget.allocator(), .{ .client = self.client, .source = self.source, .uri = self.attachment orelse self.uri, .schema = self.schema, .chunks = self.chunks, .catalogs = self.catalogs, .scope = self.scope, .context = self.context }, self.metadata)
            else
                try @import("lake_iceberg_snapshot.zig").planSnapshotInventoryAndDeletePlanFromMetadataAlloc(item.budget.allocator(), .{ .client = self.client, .source_id = self.source, .metadata_uri = self.uri, .requested_snapshot_id = self.snapshot, .manifests = self.manifests, .inventory_chunks = self.chunks, .catalogs = self.catalogs, .manifest_scope = self.scope, .context = self.context }, self.metadata);
            item.payload = .{ .snapshot = decoded };
            item.payload.snapshot.inventory.row_identity_version = self.identity_version;
            if (item.payload.snapshot.catalog) |index| item.foreign_bytes +|= index.footprint();
            for (item.payload.snapshot.file_leases) |lease| item.foreign_bytes +|= lease.item.budget.live;
            try prepareCachedPlan(item);
        }
    };

    pub fn validateBinding(self: *const ServingSource, binding: @import("../external_source/catalog_binding.zig").Binding) !void {
        if (self.catalog() != null or self.file_navigation != null or self.canonicalOrder() != null)
            try @import("lake_scan_plan.zig").validateBindingInventoryIdentity(binding, self.inventory)
        else
            try serverless_query.validateLakeBindingInventory(binding, self.inventory);
    }
    pub fn catalog(self: *const ServingSource) ?*@import("lake_inventory_catalog.zig").Index {
        if (self.selected_catalog_view) return null;
        return if (self.plan_lease) |lease| switch (lease.item.payload) {
            .snapshot => |snapshot| snapshot.catalog,
            else => null,
        } else null;
    }
    pub fn fileCount(self: *const ServingSource) usize {
        return if (self.selected_catalog_view) self.inventory.files.len else if (self.file_navigation) |value| value.count else if (self.catalog()) |index| index.count() else self.inventory.files.len;
    }
    pub fn canEnumerate(self: *const ServingSource) bool {
        return self.hasLazyInventory() and self.file_navigation != null and self.file_navigation.?.enumerate != null;
    }
    pub fn hasLazyInventory(self: *const ServingSource) bool {
        if (self.deferred_snapshot != null and (self.plan_lease != null or self.private_snapshot != null)) return false;
        return !self.selected_catalog_view and (self.catalog() != null or self.file_navigation != null) and !self.catalog_files and self.inventory.files.len == 0;
    }
    fn freeCatalogFiles(self: *ServingSource) void {
        if (!self.catalog_files) return;
        for (self.inventory.files) |file_| {
            if (self.catalog_files_owned) {
                var file = file_;
                file.deinit(self.alloc);
            } else self.alloc.free(file_.version_id);
        }
        self.alloc.free(self.inventory.files);
        self.inventory.files = &.{};
        self.catalog_files = false;
    }
    pub fn ensureInventory(self: *ServingSource, context: @import("lake_read_context.zig").Context) !void {
        if (!self.hasLazyInventory()) return;
        if (self.deferred_snapshot) |frozen| {
            // Full scans share the flat immutable view, version strings and
            // canonical ordering too. This separate key cannot collide with
            // the lazy persistent-catalog plan for the same frozen metadata.
            var hash = std.crypto.hash.sha2.Sha256.init(.{});
            hash.update("iceberg-full-scan-plan-v1");
            hash.update(&frozen.key);
            hash.update(&.{self.inventory.row_identity_version});
            var loader: SnapshotPlanLoader = .{ .identity_version = self.inventory.row_identity_version, .client = self.scanner.object_reader.client, .source = self.inventory.source_id, .uri = frozen.uri, .attachment = self.inventory.source_uri, .schema = self.inventory.schema_fingerprint, .snapshot = frozen.snapshot, .metadata = frozen.metadata, .manifests = &frozen.cache.manifests, .chunks = &frozen.cache.inventory_chunks, .scope = frozen.scope, .context = context };
            const lease = frozen.cache.decoded.acquire(hash.finalResult(), 64 * 1024 * 1024, context, .{ .ptr = &loader, .load = SnapshotPlanLoader.load }) catch |err| switch (err) {
                error.OutOfMemory, error.SqlMemoryLimitExceeded => return self.privateFrozenInventory(frozen, context),
                else => return err,
            };
            errdefer lease.release();
            const uri = try self.alloc.dupe(u8, self.inventory.source_uri);
            if (self.delete_lease) |held| held.release() else if (self.inventory_owned) if (self.prepared_deletes) |prepared| prepared.destroy(self.alloc);
            self.delete_lease = null;
            self.prepared_deletes = null;
            if (self.inventory_owned) {
                self.inventory.deinit(self.alloc);
                if (self.scanner.iceberg_delete_plan) |*plan| plan.deinit(self.alloc);
            }
            if (self.attachment_uri) |old| self.alloc.free(old);
            self.attachment_uri = uri;
            self.plan_lease = lease;
            self.inventory = lease.item.payload.snapshot.inventory;
            self.inventory.source_uri = uri;
            self.inventory_owned = false;
            self.scanner.iceberg_delete_plan = lease.item.payload.snapshot.delete_plan;
            // No flat files belong to this query; version overrides remain in
            // its private map and the parent/worker read contexts stay private.
            self.scanner.inventory = self.inventory;
            return;
        } else self.inventory.files = try self.catalog().?.materialize(self.alloc, self.inventory.snapshot_id, context);
        self.catalog_files = true;
        self.scanner.inventory = self.inventory;
    }
    fn privateFrozenInventory(self: *ServingSource, frozen: DeferredSnapshot, context: @import("lake_read_context.zig").Context) !void {
        try context.ensureActive();
        const identity_version = self.inventory.row_identity_version;
        const snapshot = try self.alloc.create(@import("lake_iceberg_snapshot.zig").SnapshotWithDeletePlan);
        errdefer self.alloc.destroy(snapshot);
        snapshot.* = if (@import("lake_parquet_manifest.zig").isManifest(frozen.uri))
            try @import("lake_parquet_manifest.zig").plan(self.alloc, .{ .client = self.scanner.object_reader.client, .source = self.inventory.source_id, .uri = self.inventory.source_uri, .schema = self.inventory.schema_fingerprint, .chunks = &frozen.cache.inventory_chunks, .scope = frozen.scope, .context = context }, frozen.metadata)
        else
            try @import("lake_iceberg_snapshot.zig").planSnapshotInventoryAndDeletePlanFromMetadataAlloc(self.alloc, .{ .client = self.scanner.object_reader.client, .source_id = self.inventory.source_id, .metadata_uri = frozen.uri, .requested_snapshot_id = frozen.snapshot, .manifests = &frozen.cache.manifests, .inventory_chunks = &frozen.cache.inventory_chunks, .manifest_scope = frozen.scope, .context = context }, frozen.metadata);
        errdefer snapshot.deinit(self.alloc);
        const uri = try self.alloc.dupe(u8, self.inventory.source_uri);
        errdefer self.alloc.free(uri);
        if (self.delete_lease) |held| held.release() else if (self.inventory_owned) if (self.prepared_deletes) |prepared| prepared.destroy(self.alloc);
        self.delete_lease = null;
        self.prepared_deletes = null;
        if (self.inventory_owned) {
            self.inventory.deinit(self.alloc);
            if (self.scanner.iceberg_delete_plan) |*plan| plan.deinit(self.alloc);
        }
        if (self.attachment_uri) |old| self.alloc.free(old);
        self.attachment_uri = uri;
        self.private_snapshot = snapshot;
        self.private_snapshot_owned = true;
        snapshot.inventory.row_identity_version = identity_version;
        self.inventory = snapshot.inventory;
        self.inventory.source_uri = uri;
        self.inventory_owned = false;
        self.scanner.inventory = self.inventory;
        self.scanner.iceberg_delete_plan = snapshot.delete_plan;
        // Preserve ordinary request admission; no private unbounded cache.
        // The flat descriptor vector is necessary for existing scan cursors,
        // while payloads remain borrowed from individually bounded chunks.
    }
    /// Borrowers that keep the store and inventory elsewhere still release
    /// request-owned preparation through the same lifecycle as full sources.
    pub fn clearLocalPlan(self: *ServingSource) void {
        if (self.local_plan_owned) if (self.local_plan) |*plan| plan.deinit(self.alloc);
        self.local_plan = null;
        self.local_plan_owned = true;
    }
    pub fn ensureCanonicalPlan(self: *ServingSource, context: @import("lake_read_context.zig").Context) !void {
        if (self.catalog()) |index| {
            _ = index.ensureOrderingVersion(self.inventory.source_id, self.inventory.snapshot_id, self.inventory.row_identity_version, context) catch |err| switch (err) {
                error.OutOfMemory => return, // ordinary scan-local ordering remains valid
                else => return err,
            };
            return;
        }
        if (self.selected_catalog_view or self.canonicalOrder() != null) return;
        // Publication selects its identity version before lending parallel
        // views. Legacy flat callers rebuild once, never once per file cursor.
        const plan = PreparedPlan.init(self.alloc, self.inventory) catch |err| switch (err) {
            error.OutOfMemory => return,
        };
        self.clearLocalPlan();
        self.local_plan = plan;
        self.local_plan_owned = true;
    }
    /// A bounded selection owns just its flat view/version strings. The parent
    /// pins immutable chunk pages, provider selectors and reader authority.
    pub fn selectedView(self: *ServingSource, a: std.mem.Allocator, refs: []const @import("../../storage/rowsource/types.zig").RowRef, context: @import("lake_read_context.zig").Context) !ServingSource {
        var selected: std.StringHashMapUnmanaged(void) = .empty;
        defer selected.deinit(a);
        for (refs) |ref| {
            try context.ensureActive();
            if (ref != .external or !std.mem.eql(u8, ref.external.source_id, self.inventory.source_id) or !std.mem.eql(u8, ref.external.snapshot_id, self.inventory.snapshot_id)) return error.ExternalLakeSnapshotMismatch;
            try selected.put(a, ref.external.file_id, {});
        }
        return self.selectedFileMapView(a, &selected, context);
    }
    /// Physical predicate blocks already carry file identities. Resolve those
    /// identities directly, without inventing row coordinates or flattening
    /// the complete inventory before a residual scan.
    pub fn selectedFilesView(self: *ServingSource, a: std.mem.Allocator, ids: []const []const u8, context: @import("lake_read_context.zig").Context) !ServingSource {
        var selected: std.StringHashMapUnmanaged(void) = .empty;
        defer selected.deinit(a);
        for (ids) |id| {
            try context.ensureActive();
            try selected.put(a, id, {});
        }
        return self.selectedFileMapView(a, &selected, context);
    }
    fn selectedFileMapView(self: *ServingSource, a: std.mem.Allocator, selected: *const std.StringHashMapUnmanaged(void), context: @import("lake_read_context.zig").Context) !ServingSource {
        const index = self.catalog();
        const navigation_ = if (self.selected_catalog_view) null else self.file_navigation;
        if (index == null and navigation_ == null) return error.InvalidExternalSourceInventory;
        var view = try self.parallelView(a);
        errdefer view.closeParallelView(self);
        view.version_parent = null;
        view.pinned_files = &.{};
        view.local_plan = null;
        view.selected_catalog_view = true;
        view.plan_identity = null; // delete plans bind this exact bounded file view
        if (view.delete_lease) |lease| lease.release();
        view.delete_lease = null;
        view.prepared_deletes = null;
        const files = try a.alloc(external_source_api.FileEntry, selected.count());
        var initialized: usize = 0;
        errdefer {
            for (files[0..initialized]) |file_| {
                if (navigation_ != null) {
                    var file = file_;
                    file.deinit(a);
                } else a.free(file_.version_id);
            }
            a.free(files);
        }
        var ids = selected.keyIterator();
        for (files) |*file| {
            const id = ids.next().?.*;
            file.* = if (navigation_) |value| try value.lookup(value.ptr, a, id, context) else blk: {
                const input = index.?.find(id) orelse return error.ExternalLakeSnapshotMismatch;
                break :blk try @import("lake_inventory_catalog.zig").Index.qualify(a, input.*, self.inventory.snapshot_id);
            };
            initialized += 1;
        }
        std.mem.sort(external_source_api.FileEntry, files, {}, struct {
            fn less(_: void, l: external_source_api.FileEntry, r: external_source_api.FileEntry) bool {
                return std.mem.lessThan(u8, l.file_id, r.file_id);
            }
        }.less);
        view.inventory.files = files;
        view.scanner.inventory = view.inventory;
        view.catalog_files = true;
        view.catalog_files_owned = navigation_ != null;
        return view;
    }
    pub fn isFilePinned(self: *const ServingSource, index: usize) bool {
        return self.pinnedVersion(index) != null or (self.pinned_files.len != 0 and self.pinned_files[index]);
    }
    pub fn pinnedVersion(self: *const ServingSource, index: usize) ?external_source_api.FileEntry {
        return self.versions.get(index) orelse if (self.version_parent) |parent| parent.pinnedVersion(index) else null;
    }
    pub fn fileAt(self: *const ServingSource, index: usize) external_source_api.FileEntry {
        return self.pinnedVersion(index) orelse self.inventory.files[index];
    }
    pub fn clearVersions(self: *ServingSource) void {
        var it = self.versions.valueIterator();
        while (it.next()) |file| {
            self.alloc.free(file.etag);
            self.alloc.free(file.version_id);
        }
        self.versions.deinit(self.alloc);
        self.versions = .empty;
        self.version_parent = null;
    }
    // Full source proofs and builds explicitly materialize mutable evidence.
    // Warm selective readers retain only the files they actually touch.
    pub fn ownInventory(self: *ServingSource) !void {
        if (self.inventory_owned) return;
        try self.ensureInventory(self.protectContext(.{}));
        const bytes = try @import("../external_source/codec.zig").encodeAlloc(self.alloc, self.inventory);
        defer self.alloc.free(bytes);
        var inventory = try @import("../external_source/codec.zig").decodeAlloc(self.alloc, bytes);
        errdefer inventory.deinit(self.alloc);
        const pinned = try self.alloc.alloc(bool, inventory.files.len);
        errdefer self.alloc.free(pinned);
        @memset(pinned, false);
        for (inventory.files, 0..) |*file, index| if (self.pinnedVersion(index)) |evidence| {
            const etag = try self.alloc.dupe(u8, evidence.etag);
            errdefer self.alloc.free(etag);
            const version = try self.alloc.dupe(u8, evidence.version_id);
            self.alloc.free(file.etag);
            self.alloc.free(file.version_id);
            file.etag = etag;
            file.version_id = version;
            pinned[index] = true;
        };
        self.clearVersions();
        self.freeCatalogFiles();
        if (self.pinned_files_owned) self.alloc.free(self.pinned_files);
        self.pinned_files = pinned;
        self.pinned_files_owned = true;
        if (self.attachment_uri) |uri| self.alloc.free(uri);
        self.attachment_uri = null;
        self.inventory = inventory;
        self.scanner.inventory = inventory;
        self.inventory_owned = true;
    }
    const PreparedPlan = struct {
        identity_version: u8,
        file_order: []usize,
        file_rank: []usize,
        file_by_id: std.StringHashMapUnmanaged(usize) = .empty,
        estimated_rows: u64 = 0,
        estimated_bytes: u64 = 0,
        fn init(a: std.mem.Allocator, inventory: external_source_api.Inventory) !PreparedPlan {
            const order = try canonicalFileOrder(a, inventory);
            errdefer a.free(order);
            const rank = try a.alloc(usize, inventory.files.len);
            errdefer a.free(rank);
            var plan: PreparedPlan = .{ .identity_version = inventory.row_identity_version, .file_order = order, .file_rank = rank };
            errdefer plan.file_by_id.deinit(a);
            try plan.file_by_id.ensureTotalCapacity(a, @intCast(inventory.files.len));
            for (inventory.files, 0..) |file, index| {
                plan.file_by_id.putAssumeCapacity(file.file_id, index);
                plan.estimated_rows +|= file.row_count;
                plan.estimated_bytes +|= file.byte_len;
            }
            for (order, 0..) |file, position| rank[file] = position;
            return plan;
        }
        fn deinit(self: *PreparedPlan, a: std.mem.Allocator) void {
            a.free(self.file_order);
            a.free(self.file_rank);
            self.file_by_id.deinit(a);
        }
    };
    pub fn canonicalOrder(self: *const ServingSource) ?[]usize {
        if (self.selected_catalog_view) return null;
        if (self.catalog()) |index| if (index.orderingFor(self.inventory.row_identity_version).load(.acquire)) |order| return order.order;
        if (self.plan_lease) |lease| if (lease.item.file_order_identity_version == self.inventory.row_identity_version) if (lease.item.file_order) |order| return order;
        return if (self.local_plan) |plan| if (plan.identity_version == self.inventory.row_identity_version) plan.file_order else null else null;
    }
    pub fn fileRanks(self: *const ServingSource) ?[]usize {
        if (self.selected_catalog_view) return null;
        if (self.catalog()) |index| if (index.orderingFor(self.inventory.row_identity_version).load(.acquire)) |order| return order.ranks;
        if (self.plan_lease) |lease| if (lease.item.file_order_identity_version == self.inventory.row_identity_version) if (lease.item.file_rank) |rank| return rank;
        return if (self.local_plan) |plan| if (plan.identity_version == self.inventory.row_identity_version) plan.file_rank else null else null;
    }
    pub fn fileMap(self: *const ServingSource) ?*const std.StringHashMapUnmanaged(usize) {
        if (self.selected_catalog_view) return null;
        if (self.catalog()) |index| if (index.orderingFor(self.inventory.row_identity_version).load(.acquire)) |order| return &order.by_id;
        if (self.plan_lease) |lease| if (lease.item.file_rank != null) return &lease.item.file_by_id;
        return if (self.local_plan) |*plan| &plan.file_by_id else null;
    }
    pub fn estimates(self: *const ServingSource) ?Estimates {
        if (self.file_navigation) |navigation| if (navigation.estimates) |value| return value;
        if (self.catalog()) |index| return .{ .rows = index.rows(), .bytes = index.bytes() };
        if (self.plan_lease) |lease| if (lease.item.file_order != null) return .{ .rows = lease.item.estimated_rows, .bytes = lease.item.estimated_bytes };
        return if (self.local_plan) |plan| .{ .rows = plan.estimated_rows, .bytes = plan.estimated_bytes } else null;
    }
    pub fn prepareCachedPlan(item: *@import("lake_decoded_cache.zig").Item) !void {
        if (item.payload.snapshot.catalog != null) return;
        const plan = try PreparedPlan.init(item.budget.allocator(), item.payload.snapshot.inventory);
        item.file_order = plan.file_order;
        item.file_order_identity_version = plan.identity_version;
        item.file_rank = plan.file_rank;
        item.file_by_id = plan.file_by_id;
        item.estimated_rows = plan.estimated_rows;
        item.estimated_bytes = plan.estimated_bytes;
    }
    pub fn canonicalFileOrder(alloc: std.mem.Allocator, inventory: external_source_api.Inventory) ![]usize {
        const identities = @import("../../storage/rowsource/identity.zig");
        const digests = try alloc.alloc([32]u8, inventory.files.len);
        defer alloc.free(digests);
        const order = try alloc.alloc(usize, inventory.files.len);
        for (inventory.files, digests, order, 0..) |file, *digest, *index, i| {
            digest.* = identities.navigationDigest(inventory.row_identity_version, inventory.source_id, inventory.snapshot_id, file.file_id);
            index.* = i;
        }
        std.mem.sort(usize, order, digests, struct {
            fn less(keys: []const [32]u8, left: usize, right: usize) bool {
                return std.mem.order(u8, &keys[left], &keys[right]) == .lt;
            }
        }.less);
        return order;
    }

    fn cacheScope(alloc: std.mem.Allocator, store: object_store_support.OpenedObjectStore, binding: @import("../external_source/catalog_binding.zig").Binding) ![32]u8 {
        const scope_bytes = try std.json.Stringify.valueAlloc(alloc, .{
            .credentials = binding.credential_ref,
            .source_id = binding.table_id,
            .s3_credentials = if (store.s3_client) |s3| s3.cfg.credentials else null,
            .source_uri = binding.source_uri,
            .filesystem_root = if (store.fs_client) |fs| @as(?[]const u8, fs.root_dir) else null,
            .s3_endpoint = if (store.s3_client) |s3| @as(?[]const u8, s3.cfg.credentials.endpoint) else null,
            .s3_region = if (store.s3_client) |s3| @as(?[]const u8, s3.cfg.credentials.region) else null,
            .s3_ssl = if (store.s3_client) |s3| @as(?bool, s3.cfg.credentials.use_ssl) else null,
            .gcs_bearer = if (store.gcs_client) |gcs| switch (gcs.cfg.auth) {
                .bearer_token => |token| @as(?[]const u8, token),
                else => null,
            } else null,
            .gcs_credentials = if (store.gcs_client) |gcs| switch (gcs.cfg.auth) {
                .google_token_source => |source| @as(?@TypeOf(source.cfg), source.cfg),
                else => null,
            } else null,
            .gcs_endpoint = if (store.gcs_client) |gcs| @as(?[]const u8, gcs.cfg.endpoint) else null,
        }, .{});
        defer alloc.free(scope_bytes);
        var scope: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(scope_bytes, &scope, .{});
        return scope;
    }
    /// Credential identity is available only after opening an authorized source.
    /// Persist the digest, never the credential material used to compute it.
    pub fn protectContext(self: *ServingSource, parent: @import("lake_read_context.zig").Context) @import("lake_read_context.zig").Context {
        if (self.snapshot_pin) |pin| {
            var context = parent;
            context.additional_checkpoint = .{ .ptr = pin.ptr, .check = pin.check };
            // The pin owner also retains the original context (including any
            // publication lease), so adding protection never drops authority.
            return context;
        }
        return parent;
    }
    pub fn credentialIdentity(self: *ServingSource, binding: @import("../external_source/catalog_binding.zig").Binding) ![32]u8 {
        try self.validateBinding(binding);
        return cacheScope(self.alloc, self.store, binding);
    }
    pub fn attachCache(self: *ServingSource, cache: *@import("lake_serving_cache.zig").Cache, binding: @import("../external_source/catalog_binding.zig").Binding, context: @import("lake_read_context.zig").Context) !void {
        const reader = try self.alloc.create(@import("lake_serving_cache.zig").Reader);
        errdefer self.alloc.destroy(reader);
        const scope = try cacheScope(self.alloc, self.store, binding);
        reader.* = .{ .base = self.scanner.object_reader, .cache = cache, .scope = scope, .context = self.protectContext(context) };
        self.scanner.shared_reader = reader;
    }

    pub fn deinit(self: *ServingSource) void {
        const snapshot_pin = self.snapshot_pin;
        defer if (snapshot_pin) |pin| pin.deinit(pin.ptr);
        if (self.verified_inventory_lease) |lease| lease.release();
        if (self.delete_lease) |lease| lease.release() else if (self.prepared_deletes) |prepared| prepared.destroy(self.alloc);
        if (self.delete_qualification_owned) if (self.delete_qualification) |qualification| qualification.destroy();
        if (self.scanner.shared_reader) |reader| {
            reader.drain(true);
            self.alloc.destroy(reader);
        }
        if (self.verified_selector) |selector| selector.close(selector.ptr);
        if (self.row_resolver) |resolver| resolver.close(resolver.ptr);
        if (self.plan_lease) |lease| lease.release() else if (self.private_snapshot == null) if (self.scanner.iceberg_delete_plan) |*value| value.deinit(self.alloc);
        if (self.partition_rules) |*rules| rules.deinit();
        if (self.iceberg_schema) |*value| value.deinit();
        self.clearVersions();
        self.clearLocalPlan();
        if (self.pinned_files_owned) self.alloc.free(self.pinned_files);
        self.freeCatalogFiles();
        if (self.inventory_owned) self.inventory.deinit(self.alloc);
        if (self.attachment_uri) |uri| self.alloc.free(uri);
        if (self.catalog_metadata) |*table| table.deinit(self.alloc);
        if (self.file_navigation) |value| value.close(value.ptr);
        if (self.statistics_owner) |owner| owner.close(owner.ptr);
        if (self.deferred_snapshot) |value| value.deinit(self.alloc);
        if (self.private_snapshot_owned) if (self.private_snapshot) |snapshot| {
            snapshot.deinit(self.alloc);
            self.alloc.destroy(snapshot);
        };
        self.store.deinit();
        if (self.context_store) |store| self.alloc.destroy(store);
        self.* = undefined;
    }
    pub fn icebergMetadataUriForOpenedStoreAlloc(
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

        // A directory can contain uncommitted metadata from concurrent writers.
        // Only an explicit metadata URI or the supported filesystem commit
        // pointer may select a snapshot; listing order is never authority.
        return error.ExternalLakeSnapshotMismatch;
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
            error.FileNotFound, error.ObjectNotFound => return null,
            else => return err,
        };
        defer hint.deinit(alloc);

        const trimmed = std.mem.trim(u8, hint.body, " \t\r\n");
        if (trimmed.len == 0) return null;
        const version = std.fmt.parseUnsigned(u64, trimmed, 10) catch return null;
        const metadata_key = try std.fmt.allocPrint(alloc, "{s}v{d}.metadata.json", .{ metadata_prefix, version });
        defer alloc.free(metadata_key);
        var metadata_stat = client.statObject(bucket, metadata_key) catch |err| switch (err) {
            error.FileNotFound, error.ObjectNotFound => return null,
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
};

test "external lake metadata navigation coalesces cold immutable snapshot discovery" {
    const io = std.testing.io;
    const cache_mod = @import("lake_decoded_cache.zig");
    var cache: cache_mod.Cache = .{ .a = std.testing.allocator };
    defer cache.deinit();
    var memory = object_storage_api.MemoryObjectStorage.init(std.testing.allocator);
    defer memory.deinit();
    const metadata =
        \\{"format-version":2,"table-uuid":"test","location":"s3://bucket/table","current-schema-id":0,"schemas":[{"schema-id":0,"type":"struct","fields":[{"id":1,"name":"id","required":true,"type":"long"}]}],"snapshots":[],"current-snapshot-id":-1}
    ;
    const State = struct {
        loader: ServingSource.SnapshotPlanLoader,
        started: std.Io.Event = .unset,
        gate: std.Io.Event = .unset,
        calls: std.atomic.Value(usize) = .init(0),
        fn load(raw: *anyopaque, item: *cache_mod.Item) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            _ = self.calls.fetchAdd(1, .monotonic);
            self.started.set(std.testing.io);
            try self.gate.wait(std.testing.io);
            try ServingSource.SnapshotPlanLoader.load(&self.loader, item);
        }
        fn read(self: *@This(), shared: *cache_mod.Cache) anyerror!cache_mod.Lease {
            return shared.acquire(@splat(8), 64 * 1024 * 1024, .{ .io = std.testing.io }, .{ .ptr = self, .load = load });
        }
    };
    var state: State = .{ .loader = .{ .client = memory.client(), .source = "source", .uri = "s3://bucket/table/metadata/v1.json", .snapshot = null, .metadata = metadata } };
    var first = try io.concurrent(State.read, .{ &state, &cache });
    defer if (first.any_future != null) if (first.cancel(io)) |lease| lease.release() else |_| {};
    try state.started.wait(io);
    var second = try io.concurrent(State.read, .{ &state, &cache });
    defer if (second.any_future != null) if (second.cancel(io)) |lease| lease.release() else |_| {};
    // Hold the leader until a second reader has actually joined its flight.
    // A merely warm cache hit would not exercise construction coalescing.
    while (true) {
        while (!cache.mutex.tryLock()) try io.sleep(.fromMilliseconds(1), .awake);
        const joined = if (cache.flights.get(@splat(8))) |flight| flight.refs > 1 else false;
        cache.mutex.unlock();
        if (joined) break;
        try io.sleep(.fromMilliseconds(1), .awake);
    }
    state.gate.set(io);
    const one = try first.await(io);
    defer one.release();
    const two = try second.await(io);
    defer two.release();
    try std.testing.expectEqual(one.item, two.item);
    try std.testing.expectEqual(@as(usize, 1), state.calls.load(.acquire));
    try std.testing.expect(one.item.file_order != null);
    try std.testing.expectEqualStrings("source", one.item.payload.snapshot.inventory.source_id);
    var canceled = std.atomic.Value(bool).init(true);
    try std.testing.expectError(error.Canceled, cache.acquire(@splat(8), 64 * 1024 * 1024, .{ .io = io, .cancellation = object_storage_api.CancellationToken.fromAtomic(&canceled) }, .{ .ptr = &state, .load = State.load }));

    // A failed constructor must leave the item's previous empty payload intact.
    // Otherwise cleanup interprets uninitialized fields as snapshot resources.
    const missing =
        \\{"format-version":2,"table-uuid":"test","location":"s3://bucket/table","current-schema-id":0,"schemas":[{"schema-id":0,"type":"struct","fields":[{"id":1,"name":"id","required":true,"type":"long"}]}],"snapshots":[{"snapshot-id":1,"sequence-number":1,"timestamp-ms":1,"manifest-list":"s3://bucket/table/missing.avro"}],"current-snapshot-id":1}
    ;
    var broken = state.loader;
    broken.metadata = missing;
    for (0..2) |_| try std.testing.expectError(error.FileNotFound, cache.acquire(@splat(9), 64 * 1024 * 1024, .{ .io = io }, .{ .ptr = &broken, .load = ServingSource.SnapshotPlanLoader.load }));
    try std.testing.expectEqualStrings("source", one.item.payload.snapshot.inventory.source_id);
}

test "external lake metadata navigation parallel source views own version mutations and adopted inventories" {
    const a = std.testing.allocator;
    var memory = object_storage_api.MemoryObjectStorage.init(a);
    defer memory.deinit();
    var files = [_]external_source_api.FileEntry{.{ .file_id = @constCast("file"), .object_uri = @constCast("s3://bucket/data.parquet"), .etag = @constCast("base"), .version_id = @constCast("v0"), .byte_len = 100, .row_count = 1, .row_groups = &.{} }};
    const inventory: external_source_api.Inventory = .{ .format = .parquet, .source_id = @constCast("table"), .source_uri = @constCast("s3://bucket"), .snapshot_id = @constCast("snapshot"), .schema_fingerprint = @constCast("schema"), .files = &files };
    const pinned = try a.alloc(bool, 1);
    defer a.free(pinned);
    pinned[0] = true;
    var parent: ServingSource = .{ .alloc = a, .store = undefined, .inventory = inventory, .scanner = PinnedExternalObjectStorageLakeRowsScanner.init(inventory, memory.client()), .inventory_owned = false, .pinned_files = pinned };
    defer parent.clearVersions();
    var file = files[0];
    file.etag = try a.dupe(u8, "original");
    file.version_id = try a.dupe(u8, "v1");
    try parent.versions.put(a, 0, file);
    var view = try parent.parallelView(a);
    defer view.closeParallelView(&parent);
    var selected_file = view.pinnedVersion(0).?;
    selected_file.etag = try a.dupe(u8, selected_file.etag);
    selected_file.version_id = try a.dupe(u8, selected_file.version_id);
    try view.versions.put(a, 0, selected_file);
    const selected = view.versions.getPtr(0).?;
    a.free(selected.etag);
    selected.etag = try a.dupe(u8, "changed");
    try view.ownInventory();
    try std.testing.expectEqualStrings("changed", view.inventory.files[0].etag);
    try std.testing.expectEqualStrings("original", parent.versions.get(0).?.etag);
    try std.testing.expectEqualStrings("base", parent.inventory.files[0].etag);
    try std.testing.expect(parent.pinned_files[0]);
    try std.testing.expect(view.pinned_files.ptr != parent.pinned_files.ptr);
}

test "external lake metadata navigation full scans share frozen plans and empty inventory ownership" {
    const a = std.testing.allocator;
    var memory = object_storage_api.MemoryObjectStorage.init(a);
    defer memory.deinit();
    var cache = @import("lake_serving_cache.zig").Cache.init(a);
    defer cache.deinit();
    const metadata =
        \\{"format-version":2,"table-uuid":"test","location":"s3://bucket/table","current-schema-id":0,"schemas":[{"schema-id":0,"type":"struct","fields":[{"id":1,"name":"id","required":true,"type":"long"}]}],"snapshots":[],"current-snapshot-id":-1}
    ;
    const Fixture = struct {
        fn lookup(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: @import("lake_read_context.zig").Context) !external_source_api.FileEntry {
            return error.ExternalSourceFileNotFound;
        }
        fn close(_: *anyopaque) void {}
        fn source(alloc: std.mem.Allocator, client: object_storage_api.ObjectStorage, shared: *@import("lake_serving_cache.zig").Cache, bytes: []const u8) !ServingSource {
            var inventory = try @import("lake_iceberg_snapshot.zig").snapshotHeaderAlloc(alloc, .{ .client = client, .source_id = "source", .metadata_uri = "s3://bucket/table/metadata/v1.json" }, bytes);
            errdefer inventory.deinit(alloc);
            const frozen_bytes = try alloc.dupe(u8, bytes);
            errdefer alloc.free(frozen_bytes);
            const uri = try alloc.dupe(u8, "s3://bucket/table/metadata/v1.json");
            return .{ .alloc = alloc, .store = undefined, .inventory = inventory, .scanner = PinnedExternalObjectStorageLakeRowsScanner.init(inventory, client), .file_navigation = .{ .ptr = shared, .count = 0, .plan = @splat(7), .lookup = lookup, .close = close }, .deferred_snapshot = .{ .metadata = frozen_bytes, .uri = uri, .snapshot = null, .cache = shared, .key = @splat(8), .scope = @splat(9) } };
        }
        fn cleanup(source_: *ServingSource) void {
            if (source_.inventory_owned) source_.inventory.deinit(source_.alloc);
            if (source_.plan_lease) |lease| lease.release();
            if (source_.private_snapshot_owned) if (source_.private_snapshot) |snapshot| {
                snapshot.deinit(source_.alloc);
                source_.alloc.destroy(snapshot);
            };
            if (source_.attachment_uri) |uri| source_.alloc.free(uri);
            source_.deferred_snapshot.?.deinit(source_.alloc);
        }
        fn checked(alloc: std.mem.Allocator, client: object_storage_api.ObjectStorage, shared: *@import("lake_serving_cache.zig").Cache, bytes: []const u8) !void {
            var first = try source(alloc, client, shared, bytes);
            defer cleanup(&first);
            try first.ensureInventory(.{});
            const item = if (first.plan_lease) |lease| @intFromPtr(lease.item) else @intFromPtr(first.private_snapshot.?);
            try first.ensureInventory(.{});
            try std.testing.expectEqual(item, if (first.plan_lease) |lease| @intFromPtr(lease.item) else @intFromPtr(first.private_snapshot.?));
            try first.ownInventory();
            try std.testing.expect(first.inventory_owned);
        }
    };
    var first = try Fixture.source(a, memory.client(), &cache, metadata);
    defer Fixture.cleanup(&first);
    var second = try Fixture.source(a, memory.client(), &cache, metadata);
    defer Fixture.cleanup(&second);
    try first.ensureInventory(.{});
    try second.ensureInventory(.{});
    try std.testing.expectEqual(first.plan_lease.?.item, second.plan_lease.?.item);
    try std.testing.expect(first.canonicalOrder().?.ptr == second.canonicalOrder().?.ptr);
    try std.testing.expect(!first.hasLazyInventory());
    try std.testing.checkAllAllocationFailures(a, Fixture.checked, .{ memory.client(), &cache, metadata });
    var constrained = @import("lake_serving_cache.zig").Cache.init(a);
    defer constrained.deinit();
    constrained.decoded.max_loading_bytes = 32 * 1024 * 1024;
    var private = try Fixture.source(a, memory.client(), &constrained, metadata);
    defer Fixture.cleanup(&private);
    try private.ensureInventory(.{});
    try std.testing.expect(private.private_snapshot != null);
    try private.ensureInventory(.{});
    try private.ownInventory();
    try std.testing.checkAllAllocationFailures(a, Fixture.checked, .{ memory.client(), &constrained, metadata });
}
