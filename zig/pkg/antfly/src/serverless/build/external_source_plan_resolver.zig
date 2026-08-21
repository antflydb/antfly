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

//! Production resolver for catalog external-source publication plans.
//!
//! Catalog publication owns the decision to pin a `.current` external binding.
//! This module resolves the user-owned object storage source, discovers a
//! deterministic Parquet/Iceberg inventory, stores that inventory in Antfly's
//! artifact layer, and returns the manifest plan consumed by the builder.

const std = @import("std");
const Allocator = std.mem.Allocator;
const artifacts_mod = @import("../artifacts/mod.zig");
const catalog_binding = @import("../external_source/catalog_binding.zig");
const external_source = @import("../external_source/mod.zig");
const external_source_manifest = @import("external_source_manifest.zig");
const external_source_publish = @import("external_source_publish.zig");
const object_store_support = @import("../object_store_support.zig");
const object_storage = @import("../../storage/object_storage.zig");
const resolver_api = @import("external_source_plan_resolver_api.zig");
const lake_iceberg_snapshot = @import("../query/lake_iceberg_snapshot.zig");
const lake_object_reader = @import("../query/lake_object_reader.zig");
const lake_parquet_footer = @import("../query/lake_parquet_footer.zig");
const lake_parquet_metadata = @import("../query/lake_parquet_metadata.zig");
const lake_parquet_rowgroup = @import("../query/lake_parquet_rowgroup.zig");
const lake_range_io = @import("../query/lake_range_io.zig");

pub const OpenedObjectStoreResolver = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        open: *const fn (
            ptr: *anyopaque,
            alloc: Allocator,
            binding: catalog_binding.Binding,
            options: OpenOptions,
        ) anyerror!object_store_support.OpenedObjectStore,
    };

    pub const OpenOptions = struct {
        file_bucket: []const u8 = "antfly",
    };

    pub fn openAlloc(
        self: OpenedObjectStoreResolver,
        alloc: Allocator,
        binding: catalog_binding.Binding,
        options: OpenOptions,
    ) !object_store_support.OpenedObjectStore {
        return try self.vtable.open(self.ptr, alloc, binding, options);
    }
};

pub const RemoteUriObjectStoreResolver = struct {
    pub fn resolver(self: *@This()) OpenedObjectStoreResolver {
        return .{
            .ptr = self,
            .vtable = &.{
                .open = open,
            },
        };
    }

    fn open(
        _: *anyopaque,
        alloc: Allocator,
        binding: catalog_binding.Binding,
        options: OpenedObjectStoreResolver.OpenOptions,
    ) !object_store_support.OpenedObjectStore {
        if (binding.credential_ref != null) return error.UnsupportedExternalLakeCredentialRef;
        return switch (binding.format) {
            .parquet, .iceberg => try object_store_support.OpenedObjectStore.initRemoteUri(
                alloc,
                binding.source_uri,
                options.file_bucket,
            ),
            .lance => error.UnsupportedRowsQuery,
        };
    }
};

pub const ResolverOptions = struct {
    file_bucket: []const u8 = "antfly",
    object_uri_base: ?[]const u8 = null,
    footer_probe_bytes: u64 = 64 * 1024,
};

pub const Resolver = struct {
    artifacts: *artifacts_mod.ArtifactStore,
    object_store_resolver: OpenedObjectStoreResolver,
    options: ResolverOptions = .{},

    pub fn init(
        artifacts: *artifacts_mod.ArtifactStore,
        object_store_resolver: OpenedObjectStoreResolver,
        options: ResolverOptions,
    ) Resolver {
        return .{
            .artifacts = artifacts,
            .object_store_resolver = object_store_resolver,
            .options = options,
        };
    }

    pub fn planResolver(self: *@This()) resolver_api.Resolver {
        return .{
            .ptr = self,
            .vtable = &.{
                .resolve = resolve,
            },
        };
    }

    fn resolve(
        ptr: *anyopaque,
        alloc: Allocator,
        request: resolver_api.ResolveRequest,
    ) !?external_source_manifest.Plan {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        const binding = request.binding;
        try binding.validateReadOnlyMvp();

        var opened_store = try self.object_store_resolver.openAlloc(alloc, binding, .{
            .file_bucket = self.options.file_bucket,
        });
        defer opened_store.deinit();

        var inventory = try inventoryForBindingAlloc(alloc, binding, opened_store, self.options);
        defer inventory.deinit(alloc);

        const artifact_name = try std.fmt.allocPrint(alloc, "{s}.external-files", .{request.table_name});
        defer alloc.free(artifact_name);
        const published = try external_source_publish.publishInventoryAlloc(
            alloc,
            self.artifacts,
            binding,
            inventory,
            .{ .artifact_name = artifact_name },
        );
        return published.plan;
    }
};

fn inventoryForBindingAlloc(
    alloc: Allocator,
    binding: catalog_binding.Binding,
    opened_store: object_store_support.OpenedObjectStore,
    options: ResolverOptions,
) !external_source.Inventory {
    var source_options = options;
    var owned_object_uri_base: ?[]u8 = null;
    defer if (owned_object_uri_base) |value| alloc.free(value);
    if (opened_store.fs_client != null and source_options.object_uri_base == null) {
        owned_object_uri_base = try objectStoreUriBaseAlloc(alloc, opened_store.bucket, opened_store.prefix);
        source_options.object_uri_base = owned_object_uri_base.?;
    }

    return switch (binding.format) {
        .parquet => blk: {
            var inventory = try external_source.planParquetPrefixInventoryFromObjectStorageAlloc(alloc, .{
                .client = opened_store.client,
                .bucket = opened_store.bucket,
                .prefix = opened_store.prefix,
                .source_id = binding.table_id,
                .source_uri = binding.source_uri,
                .object_uri_base = source_options.object_uri_base,
                .schema_fingerprint = binding.schema_fingerprint,
            });
            errdefer inventory.deinit(alloc);
            const enriched = try enrichInventoryWithObjectFootersAlloc(
                alloc,
                opened_store.client,
                inventory,
                source_options.footer_probe_bytes,
            );
            inventory.deinit(alloc);
            break :blk enriched;
        },
        .iceberg => blk: {
            const metadata_uri = try icebergMetadataUriForOpenedStoreAlloc(
                alloc,
                opened_store.client,
                opened_store.bucket,
                opened_store.prefix,
                binding.source_uri,
                source_options.object_uri_base,
            );
            defer alloc.free(metadata_uri);
            var inventory = try lake_iceberg_snapshot.readSnapshotInventoryAlloc(alloc, .{
                .client = opened_store.client,
                .source_id = binding.table_id,
                .metadata_uri = metadata_uri,
                .requested_snapshot_id = binding.snapshot_mode.pinnedSnapshotId(),
            });
            errdefer inventory.deinit(alloc);
            try lake_iceberg_snapshot.pinInventoryDataFileObjectVersions(
                alloc,
                opened_store.client,
                &inventory,
            );
            const enriched = try enrichInventoryWithObjectFootersAlloc(
                alloc,
                opened_store.client,
                inventory,
                source_options.footer_probe_bytes,
            );
            inventory.deinit(alloc);
            break :blk enriched;
        },
        .lance => error.UnsupportedRowsQuery,
    };
}

fn enrichInventoryWithObjectFootersAlloc(
    alloc: Allocator,
    source_client: object_storage.ObjectStorage,
    inventory: external_source.Inventory,
    footer_probe_bytes: u64,
) !external_source.Inventory {
    try inventory.validate();
    if (inventory.format != .parquet and inventory.format != .iceberg) return error.InvalidParquetMetadata;
    if (footer_probe_bytes == 0) return error.InvalidLakeRangeRead;

    var object_range_reader = lake_object_reader.ObjectStorageRangeReader.init(source_client);
    const range_reader = object_range_reader.parquetReader();
    const footers = try alloc.alloc(lake_parquet_metadata.FileFooter, inventory.files.len);
    errdefer alloc.free(footers);
    var initialized: usize = 0;
    errdefer {
        for (footers[0..initialized]) |*entry| entry.footer.deinit(alloc);
    }

    for (inventory.files, 0..) |file, idx| {
        const object = try lake_range_io.objectRefForExternalFileUri(file);
        const tail_read = try lake_range_io.planParquetFooterRead(object, footer_probe_bytes);
        const tail = try range_reader.readPlannedAlloc(alloc, tail_read);
        defer alloc.free(tail);

        const preflight = try lake_parquet_footer.parseFooterPreflight(object.byte_len, tail_read.range.offset, tail);
        var owned_metadata: ?[]u8 = null;
        defer if (owned_metadata) |bytes| alloc.free(bytes);
        const metadata_bytes = preflight.metadataSlice(tail) orelse blk: {
            const metadata_read = try lake_parquet_footer.planFooterMetadataRead(object, tail_read.range.offset, tail);
            owned_metadata = try range_reader.readPlannedAlloc(alloc, metadata_read);
            break :blk owned_metadata.?;
        };

        footers[idx] = .{
            .file_id = file.file_id,
            .footer = try lake_parquet_metadata.parseFooterMetadataAlloc(alloc, metadata_bytes, file.byte_len),
        };
        initialized += 1;
    }

    var enriched = try lake_parquet_metadata.enrichInventoryFilesWithFootersAlloc(alloc, inventory, footers);
    errdefer enriched.deinit(alloc);
    for (footers[0..initialized]) |*entry| entry.footer.deinit(alloc);
    alloc.free(footers);
    return enriched;
}

fn icebergMetadataUriForOpenedStoreAlloc(
    alloc: Allocator,
    client: object_storage.ObjectStorage,
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

        if (!try external_source.advanceObjectListingContinuationTokenAlloc(
            alloc,
            &next_token,
            page.next_continuation_token,
        )) break;
    }

    const key = best_key orelse return error.ExternalLakeSnapshotMismatch;
    const relative_key = relativeObjectKeyForPrefix(prefix, key);
    const base_uri = object_uri_base orelse source_uri;
    return try objectUriForRelativeKeyAlloc(alloc, base_uri, relative_key);
}

fn icebergMetadataUriFromVersionHintAlloc(
    alloc: Allocator,
    client: *object_storage.ObjectStorage,
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

fn icebergMetadataListPrefixAlloc(alloc: Allocator, prefix: []const u8) ![]u8 {
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

fn objectUriForRelativeKeyAlloc(alloc: Allocator, base_uri: []const u8, relative_key: []const u8) ![]u8 {
    if (std.mem.endsWith(u8, base_uri, "/")) return try std.fmt.allocPrint(alloc, "{s}{s}", .{ base_uri, relative_key });
    return try std.fmt.allocPrint(alloc, "{s}/{s}", .{ base_uri, relative_key });
}

fn objectStoreUriBaseAlloc(alloc: Allocator, bucket: []const u8, prefix: []const u8) ![]u8 {
    if (prefix.len == 0) return try std.fmt.allocPrint(alloc, "object://{s}", .{bucket});
    return try std.fmt.allocPrint(alloc, "object://{s}/{s}", .{ bucket, prefix });
}

test "remote uri publication resolver pins parquet inventory into artifact store" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const fs_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/resolver-parquet", .{tmp.sub_path});
    defer alloc.free(fs_path);

    var fs = try object_storage.FilesystemObjectStorage.init(alloc, fs_path);
    defer fs.deinit();
    var client = fs.client();
    if (!(try client.bucketExists("antfly"))) try client.makeBucket("antfly");
    const parquet_object = try lake_parquet_rowgroup.buildTestSingleColumnPlainI64ParquetObjectAlloc(
        alloc,
        "amount",
        &[_]i64{ 10, 20, 30 },
    );
    defer alloc.free(parquet_object);
    var put = try client.putObject("antfly", "events/data-0001.parquet", parquet_object, .{});
    defer put.deinit(alloc);

    var opened_store = try object_store_support.OpenedObjectStore.initWithClient(alloc, client, "antfly", "events");
    defer opened_store.deinit();
    var object_resolver = StaticObjectStoreResolver{ .opened_store = opened_store };

    var artifact_impl = MemoryArtifactStore.init(alloc);
    defer artifact_impl.deinit();
    var artifacts = artifact_impl.artifactStore();
    defer artifacts.deinit();

    var publication = Resolver.init(&artifacts, object_resolver.resolver(), .{
        .object_uri_base = "object://antfly/events",
    });
    var plan = (try publication.planResolver().resolveAlloc(alloc, .{
        .namespace = "events",
        .table_name = "events",
        .binding = .{
            .table_id = "events",
            .format = .parquet,
            .source_uri = "file:///tmp/events",
            .snapshot_mode = .current,
            .schema_fingerprint = "schema-v1",
            .write_policy = .read_only,
        },
    })).?;
    defer plan.deinit(alloc);

    try std.testing.expectEqualStrings("events.external-files", plan.artifacts[0].name);
    try std.testing.expectEqualStrings(plan.artifacts[0].artifact_id, plan.base_source.external_parquet.file_inventory_artifact.?);
    try std.testing.expectEqualStrings("schema-v1", plan.base_source.external_parquet.schema_fingerprint);

    const artifact_bytes = artifact_impl.bytes orelse return error.ArtifactNotFound;
    var decoded = try external_source.decodeInventoryAlloc(alloc, artifact_bytes);
    defer decoded.deinit(alloc);
    try std.testing.expectEqual(external_source.Format.parquet, decoded.format);
    try std.testing.expectEqual(@as(usize, 1), decoded.files.len);
    try std.testing.expectEqual(@as(u64, 3), decoded.files[0].row_count);
    try std.testing.expectEqual(@as(usize, 1), decoded.files[0].row_groups.len);
    try std.testing.expectEqualStrings("amount", decoded.files[0].row_groups[0].column_chunks[0].column_id);
}

test "remote uri publication resolver rejects invalid parquet before publishing inventory artifact" {
    const alloc = std.testing.allocator;
    var memory = object_storage.MemoryObjectStorage.init(alloc);
    defer memory.deinit();
    var client = memory.client();
    try client.makeBucket("antfly");
    var put = try client.putObject("antfly", "events/data-0001.parquet", "not-parquet", .{});
    defer put.deinit(alloc);

    var opened_store = try object_store_support.OpenedObjectStore.initWithClient(alloc, client, "antfly", "events");
    defer opened_store.deinit();
    var object_resolver = StaticObjectStoreResolver{ .opened_store = opened_store };

    var artifact_impl = MemoryArtifactStore.init(alloc);
    defer artifact_impl.deinit();
    var artifacts = artifact_impl.artifactStore();
    defer artifacts.deinit();

    var publication = Resolver.init(&artifacts, object_resolver.resolver(), .{
        .object_uri_base = "object://antfly/events",
    });
    try std.testing.expectError(error.InvalidParquetFooterMagic, publication.planResolver().resolveAlloc(alloc, .{
        .namespace = "events",
        .table_name = "events",
        .binding = .{
            .table_id = "events",
            .format = .parquet,
            .source_uri = "file:///tmp/events",
            .snapshot_mode = .current,
            .schema_fingerprint = "schema-v1",
            .write_policy = .read_only,
        },
    }));
    try std.testing.expect(artifact_impl.bytes == null);
}

test "remote uri publication resolver pins iceberg data object identity into artifact store" {
    const alloc = std.testing.allocator;
    var memory = object_storage.MemoryObjectStorage.init(alloc);
    defer memory.deinit();
    var client = memory.client();
    try client.makeBucket("bucket");

    var version_hint = try client.putObject("bucket", "events/metadata/version-hint.text", "1\n", .{});
    defer version_hint.deinit(alloc);
    var metadata_file = try client.putObject("bucket", "events/metadata/v1.metadata.json", resolverIcebergMetadataJson(), .{});
    defer metadata_file.deinit(alloc);
    const data_a = try lake_parquet_rowgroup.buildTestPlainI64ParquetObjectAlloc(
        alloc,
        &[_]lake_parquet_rowgroup.TestPlainI64Column{.{
            .column_id = "amount",
            .values = &[_]i64{ 10, 20, 30 },
            .field_id = 7,
        }},
    );
    defer alloc.free(data_a);
    var data_manifest = try buildResolverIcebergDataManifestFixture(alloc, data_a.len);
    defer data_manifest.deinit(alloc);
    var manifest_list = try buildResolverIcebergManifestListFixture(alloc, data_manifest.items.len);
    defer manifest_list.deinit(alloc);
    var manifest_list_put = try client.putObject("bucket", "events/metadata/snap-12.avro", manifest_list.items, .{});
    defer manifest_list_put.deinit(alloc);
    var data_manifest_put = try client.putObject("bucket", "events/metadata/m-a.avro", data_manifest.items, .{});
    defer data_manifest_put.deinit(alloc);
    var data_a_put = try client.putObject("bucket", "events/data/a.parquet", data_a, .{});
    defer data_a_put.deinit(alloc);

    var opened_store = try object_store_support.OpenedObjectStore.initWithClient(alloc, client, "bucket", "events");
    defer opened_store.deinit();
    var object_resolver = StaticObjectStoreResolver{ .opened_store = opened_store };

    var artifact_impl = MemoryArtifactStore.init(alloc);
    defer artifact_impl.deinit();
    var artifacts = artifact_impl.artifactStore();
    defer artifacts.deinit();

    var publication = Resolver.init(&artifacts, object_resolver.resolver(), .{});
    var plan = (try publication.planResolver().resolveAlloc(alloc, .{
        .namespace = "events",
        .table_name = "events",
        .binding = .{
            .table_id = "events",
            .format = .iceberg,
            .source_uri = "s3://bucket/events",
            .snapshot_mode = .current,
            .schema_fingerprint = "iceberg-schema:7",
            .write_policy = .read_only,
        },
    })).?;
    defer plan.deinit(alloc);

    const artifact_bytes = artifact_impl.bytes orelse return error.ArtifactNotFound;
    var decoded = try external_source.decodeInventoryAlloc(alloc, artifact_bytes);
    defer decoded.deinit(alloc);

    try std.testing.expectEqualStrings("events.external-files", plan.artifacts[0].name);
    try std.testing.expectEqualStrings(plan.artifacts[0].artifact_id, plan.base_source.external_iceberg.file_inventory_artifact.?);
    try std.testing.expectEqual(external_source.Format.iceberg, decoded.format);
    try std.testing.expectEqual(@as(usize, 1), decoded.files.len);
    try std.testing.expectEqualStrings("s3://bucket/events/data/a.parquet", decoded.files[0].object_uri);
    try std.testing.expect(decoded.files[0].etag.len != 0);
    try std.testing.expectEqual(@as(usize, 0), decoded.files[0].version_id.len);
    try std.testing.expectEqual(@as(?i64, 42), decoded.files[0].data_sequence_number);
    try std.testing.expectEqual(@as(u64, 3), decoded.files[0].row_count);
    try std.testing.expectEqual(@as(usize, 1), decoded.files[0].row_groups.len);
    try std.testing.expectEqualStrings("amount", decoded.files[0].row_groups[0].column_chunks[0].column_id);
    try std.testing.expectEqual(@as(?i32, 7), decoded.files[0].row_groups[0].column_chunks[0].field_id);
}

fn resolverIcebergMetadataJson() []const u8 {
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

fn buildResolverIcebergManifestListFixture(alloc: Allocator, manifest_length: usize) !std.ArrayListUnmanaged(u8) {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try appendResolverIcebergAvroHeader(alloc, &out, resolverIcebergManifestListSchema(), "0123456789abcdef");

    var block = std.ArrayListUnmanaged(u8).empty;
    defer block.deinit(alloc);
    try appendResolverIcebergString(alloc, &block, "s3://bucket/events/metadata/m-a.avro");
    try appendResolverIcebergLong(alloc, &block, @intCast(manifest_length));
    try appendResolverIcebergLong(alloc, &block, 0);
    try appendResolverIcebergLong(alloc, &block, 0);
    try appendResolverIcebergLong(alloc, &block, 42);
    try appendResolverIcebergLong(alloc, &block, 1);
    try appendResolverIcebergLong(alloc, &block, 0);
    try appendResolverIcebergLong(alloc, &block, 0);
    try appendResolverIcebergLong(alloc, &block, 3);
    try appendResolverIcebergLong(alloc, &block, 0);
    try appendResolverIcebergLong(alloc, &block, 0);

    try appendResolverIcebergAvroBlock(alloc, &out, block.items, 1, "0123456789abcdef");
    return out;
}

fn buildResolverIcebergDataManifestFixture(alloc: Allocator, file_size_in_bytes: usize) !std.ArrayListUnmanaged(u8) {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try appendResolverIcebergAvroHeader(alloc, &out, resolverIcebergDataManifestSchema(), "fedcba9876543210");

    var block = std.ArrayListUnmanaged(u8).empty;
    defer block.deinit(alloc);
    try appendResolverIcebergLong(alloc, &block, 1);
    try appendResolverIcebergLong(alloc, &block, 1);
    try appendResolverIcebergLong(alloc, &block, 12);
    try appendResolverIcebergLong(alloc, &block, 1);
    try appendResolverIcebergLong(alloc, &block, 42);
    try appendResolverIcebergLong(alloc, &block, 1);
    try appendResolverIcebergLong(alloc, &block, 43);
    try appendResolverIcebergLong(alloc, &block, 0);
    try appendResolverIcebergString(alloc, &block, "s3://bucket/events/data/a.parquet");
    try appendResolverIcebergString(alloc, &block, "PARQUET");
    try appendResolverIcebergLong(alloc, &block, 3);
    try appendResolverIcebergLong(alloc, &block, @intCast(file_size_in_bytes));

    try appendResolverIcebergAvroBlock(alloc, &out, block.items, 1, "fedcba9876543210");
    return out;
}

fn resolverIcebergManifestListSchema() []const u8 {
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

fn resolverIcebergDataManifestSchema() []const u8 {
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

fn appendResolverIcebergAvroHeader(
    alloc: Allocator,
    out: *std.ArrayListUnmanaged(u8),
    schema: []const u8,
    sync: []const u8,
) !void {
    try out.appendSlice(alloc, "Obj\x01");
    try appendResolverIcebergLong(alloc, out, 2);
    try appendResolverIcebergString(alloc, out, "avro.schema");
    try appendResolverIcebergBytes(alloc, out, schema);
    try appendResolverIcebergString(alloc, out, "avro.codec");
    try appendResolverIcebergBytes(alloc, out, "null");
    try appendResolverIcebergLong(alloc, out, 0);
    try out.appendSlice(alloc, sync);
}

fn appendResolverIcebergAvroBlock(
    alloc: Allocator,
    out: *std.ArrayListUnmanaged(u8),
    block: []const u8,
    count: i64,
    sync: []const u8,
) !void {
    try appendResolverIcebergLong(alloc, out, count);
    try appendResolverIcebergLong(alloc, out, @intCast(block.len));
    try out.appendSlice(alloc, block);
    try out.appendSlice(alloc, sync);
}

fn appendResolverIcebergString(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), text: []const u8) !void {
    try appendResolverIcebergBytes(alloc, out, text);
}

fn appendResolverIcebergBytes(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), bytes: []const u8) !void {
    try appendResolverIcebergLong(alloc, out, @intCast(bytes.len));
    try out.appendSlice(alloc, bytes);
}

fn appendResolverIcebergLong(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), value: i64) !void {
    var remaining = resolverIcebergZigzag(value);
    while (remaining >= 0x80) {
        try out.append(alloc, @as(u8, @intCast(remaining & 0x7f)) | 0x80);
        remaining >>= 7;
    }
    try out.append(alloc, @intCast(remaining));
}

fn resolverIcebergZigzag(value: i64) u64 {
    if (value >= 0) return @as(u64, @intCast(value)) << 1;
    const magnitude: u64 = @intCast(-(value + 1));
    return (magnitude << 1) | 1;
}

const StaticObjectStoreResolver = struct {
    opened_store: object_store_support.OpenedObjectStore,

    fn resolver(self: *@This()) OpenedObjectStoreResolver {
        return .{
            .ptr = self,
            .vtable = &.{
                .open = open,
            },
        };
    }

    fn open(
        ptr: *anyopaque,
        alloc: Allocator,
        _: catalog_binding.Binding,
        _: OpenedObjectStoreResolver.OpenOptions,
    ) !object_store_support.OpenedObjectStore {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return try object_store_support.OpenedObjectStore.initWithClient(
            alloc,
            self.opened_store.client,
            self.opened_store.bucket,
            self.opened_store.prefix,
        );
    }
};

const MemoryArtifactStore = struct {
    alloc: Allocator,
    bytes: ?[]u8 = null,

    fn init(alloc: Allocator) MemoryArtifactStore {
        return .{ .alloc = alloc };
    }

    fn deinit(self: *MemoryArtifactStore) void {
        if (self.bytes) |bytes| self.alloc.free(bytes);
        self.* = undefined;
    }

    fn artifactStore(self: *MemoryArtifactStore) artifacts_mod.ArtifactStore {
        return .{
            .allocator = self.alloc,
            .ptr = self,
            .vtable = &vtable,
        };
    }

    fn put(self: *MemoryArtifactStore, alloc: Allocator, contents: []const u8) !artifacts_mod.ArtifactMetadata {
        if (self.bytes) |bytes| self.alloc.free(bytes);
        self.bytes = try self.alloc.dupe(u8, contents);
        return .{
            .artifact_id = try alloc.dupe(u8, "mem:external-files"),
            .byte_len = @intCast(contents.len),
            .checksum = try std.fmt.allocPrint(alloc, "len:{d}", .{contents.len}),
        };
    }

    fn getAlloc(self: *MemoryArtifactStore, alloc: Allocator, artifact_id: []const u8) ![]u8 {
        if (!std.mem.eql(u8, artifact_id, "mem:external-files")) return error.ArtifactNotFound;
        const bytes = self.bytes orelse return error.ArtifactNotFound;
        return try alloc.dupe(u8, bytes);
    }

    fn getRangeAlloc(self: *MemoryArtifactStore, alloc: Allocator, artifact_id: []const u8, offset: u64, len: usize) ![]u8 {
        const bytes = try self.getAlloc(alloc, artifact_id);
        defer alloc.free(bytes);
        if (offset > bytes.len) return error.InvalidRange;
        const start: usize = @intCast(offset);
        const end = @min(bytes.len, start + len);
        return try alloc.dupe(u8, bytes[start..end]);
    }

    fn stat(self: *MemoryArtifactStore, alloc: Allocator, artifact_id: []const u8) !artifacts_mod.ArtifactMetadata {
        const bytes = try self.getAlloc(alloc, artifact_id);
        defer alloc.free(bytes);
        return .{
            .artifact_id = try alloc.dupe(u8, "mem:external-files"),
            .byte_len = @intCast(bytes.len),
            .checksum = try std.fmt.allocPrint(alloc, "len:{d}", .{bytes.len}),
        };
    }

    fn delete(self: *MemoryArtifactStore, artifact_id: []const u8) !void {
        if (!std.mem.eql(u8, artifact_id, "mem:external-files")) return error.ArtifactNotFound;
        if (self.bytes) |bytes| self.alloc.free(bytes);
        self.bytes = null;
    }

    fn ifaceDeinit(alloc: Allocator, ptr: *anyopaque) void {
        _ = alloc;
        _ = ptr;
    }

    const vtable: artifacts_mod.ArtifactStore.VTable = .{
        .deinit = ifaceDeinit,
        .put = putErased,
        .get_alloc = getAllocErased,
        .get_range_alloc = getRangeAllocErased,
        .stat = statErased,
        .delete = deleteErased,
    };

    fn putErased(ptr: *anyopaque, alloc: Allocator, contents: []const u8) !artifacts_mod.ArtifactMetadata {
        const self: *MemoryArtifactStore = @ptrCast(@alignCast(ptr));
        return try self.put(alloc, contents);
    }

    fn getAllocErased(ptr: *anyopaque, alloc: Allocator, artifact_id: []const u8) ![]u8 {
        const self: *MemoryArtifactStore = @ptrCast(@alignCast(ptr));
        return try self.getAlloc(alloc, artifact_id);
    }

    fn getRangeAllocErased(ptr: *anyopaque, alloc: Allocator, artifact_id: []const u8, offset: u64, len: usize) ![]u8 {
        const self: *MemoryArtifactStore = @ptrCast(@alignCast(ptr));
        return try self.getRangeAlloc(alloc, artifact_id, offset, len);
    }

    fn statErased(ptr: *anyopaque, alloc: Allocator, artifact_id: []const u8) !artifacts_mod.ArtifactMetadata {
        const self: *MemoryArtifactStore = @ptrCast(@alignCast(ptr));
        return try self.stat(alloc, artifact_id);
    }

    fn deleteErased(ptr: *anyopaque, artifact_id: []const u8) !void {
        const self: *MemoryArtifactStore = @ptrCast(@alignCast(ptr));
        try self.delete(artifact_id);
    }
};
