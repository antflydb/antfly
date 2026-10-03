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

//! Serving adapters over the maintained lake engine. Each owner pins inventory,
//! object versions and Iceberg deletes before any row is published.
const std = @import("std");
const storage_schema = @import("../../storage/schema.zig");
const external_binding_api = @import("../external_source/catalog_binding.zig");
const external_source_api = @import("../external_source/mod.zig");
const sidecar_manifest_api = @import("../segment/sidecar_manifest.zig");
const serverless_query = @import("mod.zig");
const serverless_algebraic_segment = @import("../algebraic_segment/mod.zig");
const rowsource_api = @import("../../storage/rowsource/types.zig");
const object_storage_api = @import("../../storage/object_storage.zig");
const object_store_support = @import("../object_store_support.zig");
const configured_store = @import("../configured_object_store_support.zig");
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
        const binding = external_base_source.binding;
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
        const binding = external_base_source.binding;
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
    context_store: ?*@import("lake_read_context.zig").Store = null,

    pub fn open(alloc: std.mem.Allocator, schema: storage_schema.TableSchema, options: configured_store.BindingObjectStoreOpenOptions) !ServingSource {
        return openWithContext(alloc, schema, options, .{});
    }

    pub fn openWithContext(alloc: std.mem.Allocator, schema: storage_schema.TableSchema, options: configured_store.BindingObjectStoreOpenOptions, context: @import("lake_read_context.zig").Context) !ServingSource {
        try context.ensureActive();
        const binding = (schema.external_base_source orelse return error.InvalidExternalTableBinding).binding;
        var store = try configured_store.openBindingObjectStoreAlloc(alloc, binding, options);
        errdefer store.deinit();
        const context_store = try alloc.create(@import("lake_read_context.zig").Store);
        errdefer alloc.destroy(context_store);
        context_store.* = .{ .base = store.client, .context = context };
        const client = context_store.client(alloc);
        const base = if (store.fs_client != null) try std.fmt.allocPrint(alloc, "object://{s}/{s}", .{ store.bucket, store.prefix }) else null;
        defer if (base) |value| alloc.free(value);
        var deletes: ?serverless_query.LakeIcebergDeletePlan = null;
        errdefer if (deletes) |*value| value.deinit(alloc);
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
                var snapshot = try serverless_query.readLakeIcebergSnapshotInventoryAndDeletePlanAlloc(alloc, .{
                    .client = client,
                    .source_id = binding.table_id,
                    .metadata_uri = uri,
                    .requested_snapshot_id = binding.snapshot_mode.pinnedSnapshotId(),
                });
                deletes = snapshot.delete_plan;
                errdefer snapshot.inventory.deinit(alloc);
                if (base) |object_base| {
                    if (!std.mem.eql(u8, std.mem.trimEnd(u8, snapshot.inventory.source_uri, "/"), std.mem.trimEnd(u8, object_base, "/"))) return error.ExternalLakeSnapshotMismatch;
                    const uri_copy = try alloc.dupe(u8, binding.source_uri);
                    alloc.free(@constCast(snapshot.inventory.source_uri));
                    snapshot.inventory.source_uri = uri_copy;
                }
                try serverless_query.pinLakeIcebergInventoryDataFileObjectVersionsAlloc(alloc, client, &snapshot.inventory);
                break :blk snapshot.inventory;
            },
            .lance => return error.UnsupportedRowsQuery,
        };
        errdefer inventory.deinit(alloc);
        try serverless_query.validateLakeBindingInventory(binding, inventory);
        var scanner = PinnedExternalObjectStorageLakeRowsScanner.init(inventory, client);
        scanner.iceberg_delete_plan = deletes;
        return .{ .alloc = alloc, .store = store, .inventory = inventory, .scanner = scanner, .context_store = context_store };
    }

    pub fn deinit(self: *ServingSource) void {
        if (self.scanner.iceberg_delete_plan) |*value| value.deinit(self.alloc);
        self.inventory.deinit(self.alloc);
        self.store.deinit();
        if (self.context_store) |store| self.alloc.destroy(store);
        self.* = undefined;
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
};
