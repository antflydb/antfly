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

pub const ServingSource = struct {
    alloc: std.mem.Allocator,
    store: object_store_support.OpenedObjectStore,
    inventory: external_source_api.Inventory,
    scanner: PinnedExternalObjectStorageLakeRowsScanner,
    iceberg_schema: ?@import("lake_schema.zig").Detected = null,
    partition_rules: ?@import("lake_partition_pruning.zig").Rules = null,
    plan_identity: ?[32]u8 = null,
    plan_lease: ?@import("lake_decoded_cache.zig").Lease = null,
    delete_lease: ?@import("lake_decoded_cache.zig").Lease = null,
    lazy_versions: bool = false,
    pinned_files: []bool = &.{},
    context_store: ?*@import("lake_read_context.zig").Store = null,
    prepared_deletes: ?*@import("lake_prepared_deletes.zig").Prepared = null,

    pub fn open(alloc: std.mem.Allocator, schema: storage_schema.TableSchema, options: host_store.OpenOptions) !ServingSource {
        return openWithContext(alloc, schema, options, .{});
    }

    pub fn openWithContext(alloc: std.mem.Allocator, schema: storage_schema.TableSchema, options: host_store.OpenOptions, context: @import("lake_read_context.zig").Context) !ServingSource {
        return openCached(alloc, schema, options, context, null);
    }
    pub fn openCached(alloc: std.mem.Allocator, schema: storage_schema.TableSchema, options: host_store.OpenOptions, context: @import("lake_read_context.zig").Context, cache: ?*@import("lake_serving_cache.zig").Cache) !ServingSource {
        try context.ensureActive();
        const binding = (schema.external_base_source orelse return error.InvalidExternalTableBinding).binding;
        var store = try options.open(alloc, binding);
        errdefer store.deinit();
        const context_store = try alloc.create(@import("lake_read_context.zig").Store);
        errdefer alloc.destroy(context_store);
        context_store.* = .{ .base = store.client, .context = context };
        var client = context_store.client(alloc);
        const base = if (store.fs_client != null) try std.fmt.allocPrint(alloc, "object://{s}/{s}", .{ store.bucket, store.prefix }) else null;
        defer if (base) |value| alloc.free(value);
        var iceberg_schema: ?@import("lake_schema.zig").Detected = null;
        errdefer if (iceberg_schema) |*value| value.deinit();
        var partition_rules: ?@import("lake_partition_pruning.zig").Rules = null;
        errdefer if (partition_rules) |*rules| rules.deinit();
        var plan_identity: ?[32]u8 = null;
        var plan_lease: ?@import("lake_decoded_cache.zig").Lease = null;
        var deletes: ?serverless_query.LakeIcebergDeletePlan = null;
        errdefer if (plan_lease == null) if (deletes) |*value| value.deinit(alloc);
        errdefer if (plan_lease) |lease| lease.release();
        var inventory = switch (binding.format) {
            .parquet => try external_source_api.planParquetPrefixInventoryFromObjectStorageAlloc(alloc, .{
                .client = client,
                .bucket = store.bucket,
                .prefix = store.prefix,
                .source_id = binding.table_id,
                .source_uri = binding.source_uri,
                .object_uri_base = base,
                .schema_fingerprint = binding.schema_fingerprint,
            }),
            .iceberg => blk: {
                const uri = try icebergMetadataUriForOpenedStoreAlloc(alloc, client, store.bucket, store.prefix, binding.source_uri, base);
                defer alloc.free(uri);
                const metadata_bytes = try @import("lake_iceberg_snapshot.zig").readFullObjectAlloc(alloc, &client, null, uri, .iceberg_metadata, null, 16 * 1024 * 1024);
                defer alloc.free(metadata_bytes);
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
                    const lease = shared.decoded.lookup(key) orelse blk_lease: {
                        const owned = try shared.decoded.create(64 * 1024 * 1024);
                        errdefer owned.release();
                        owned.item.payload = .{ .snapshot = try @import("lake_iceberg_snapshot.zig").planSnapshotInventoryAndDeletePlanFromMetadataAlloc(owned.item.budget.allocator(), .{ .client = client, .source_id = binding.table_id, .metadata_uri = uri, .requested_snapshot_id = binding.snapshot_mode.pinnedSnapshotId() }, metadata_bytes) };
                        try context.ensureActive();
                        shared.decoded.publish(key, owned);
                        break :blk_lease owned;
                    };
                    plan_lease = lease;
                    const bytes = try @import("../external_source/codec.zig").encodeAlloc(alloc, lease.item.payload.snapshot.inventory);
                    defer alloc.free(bytes);
                    snapshot = .{ .inventory = try @import("../external_source/codec.zig").decodeAlloc(alloc, bytes), .delete_plan = lease.item.payload.snapshot.delete_plan };
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
                errdefer snapshot.inventory.deinit(alloc);
                if (base) |object_base| {
                    if (!std.mem.eql(u8, std.mem.trimEnd(u8, snapshot.inventory.source_uri, "/"), std.mem.trimEnd(u8, object_base, "/"))) return error.ExternalLakeSnapshotMismatch;
                    const uri_copy = try alloc.dupe(u8, binding.source_uri);
                    alloc.free(@constCast(snapshot.inventory.source_uri));
                    snapshot.inventory.source_uri = uri_copy;
                } else if (std.mem.endsWith(u8, binding.source_uri, ".metadata.json")) {
                    // Explicit object-store metadata locations are attachment
                    // identities; the metadata's location names the data root.
                    const uri_copy = try alloc.dupe(u8, binding.source_uri);
                    alloc.free(@constCast(snapshot.inventory.source_uri));
                    snapshot.inventory.source_uri = uri_copy;
                }
                {
                    iceberg_schema = try @import("lake_schema.zig").icebergSchema(alloc, metadata_bytes, binding.snapshot_mode.pinnedSnapshotId());
                    if (!std.mem.eql(u8, iceberg_schema.?.fingerprint, snapshot.inventory.schema_fingerprint)) return error.ExternalLakeSnapshotMismatch;
                    if (snapshot.inventory.files.len != 0) partition_rules = try @import("lake_partition_pruning.zig").parseAlloc(alloc, uri, metadata_bytes, snapshot.inventory.snapshot_id, binding.snapshot_mode.pinnedSnapshotId(), snapshot.inventory.schema_fingerprint);
                }
                if (cache == null) try serverless_query.pinLakeIcebergInventoryDataFileObjectVersionsAlloc(alloc, client, &snapshot.inventory);
                break :blk snapshot.inventory;
            },
            .lance => return error.UnsupportedRowsQuery,
        };
        errdefer inventory.deinit(alloc);
        try serverless_query.validateLakeBindingInventory(binding, inventory);
        var scanner = PinnedExternalObjectStorageLakeRowsScanner.init(inventory, client);
        scanner.iceberg_delete_plan = deletes;
        const lazy_versions = cache != null and binding.format == .iceberg;
        const pinned_files: []bool = if (lazy_versions) try alloc.alloc(bool, inventory.files.len) else &.{};
        @memset(pinned_files, false);
        return .{ .alloc = alloc, .store = store, .inventory = inventory, .scanner = scanner, .context_store = context_store, .partition_rules = partition_rules, .iceberg_schema = iceberg_schema, .plan_identity = plan_identity, .plan_lease = plan_lease, .lazy_versions = lazy_versions, .pinned_files = pinned_files };
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
    pub fn attachCache(self: *ServingSource, cache: *@import("lake_serving_cache.zig").Cache, binding: @import("../external_source/catalog_binding.zig").Binding, context: @import("lake_read_context.zig").Context) !void {
        const reader = try self.alloc.create(@import("lake_serving_cache.zig").Reader);
        errdefer self.alloc.destroy(reader);
        const scope = try cacheScope(self.alloc, self.store, binding);
        reader.* = .{ .base = self.scanner.object_reader, .cache = cache, .scope = scope, .context = context };
        self.scanner.shared_reader = reader;
    }

    pub fn deinit(self: *ServingSource) void {
        if (self.delete_lease) |lease| lease.release() else if (self.prepared_deletes) |prepared| prepared.destroy(self.alloc);
        if (self.scanner.shared_reader) |reader| {
            reader.drain(true);
            self.alloc.destroy(reader);
        }
        if (self.plan_lease) |lease| lease.release() else if (self.scanner.iceberg_delete_plan) |*value| value.deinit(self.alloc);
        if (self.partition_rules) |*rules| rules.deinit();
        if (self.iceberg_schema) |*value| value.deinit();
        self.alloc.free(self.pinned_files);
        self.inventory.deinit(self.alloc);
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
