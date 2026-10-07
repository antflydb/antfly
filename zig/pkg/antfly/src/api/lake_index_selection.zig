// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//! Authorized native publication selection. Absence or stale coverage is an
//! automatic fallback; explicitly requested indexes fail closed.
const std = @import("std");
const local = @import("antfly_local_sources");
const catalog = local.metadata_lake_index_catalog;
const sql = local.sql_catalog;
const Store = @import("lake_index_store.zig").Store;
const coverage = @import("lake_index_coverage.zig");
const Context = local.serverless_query_lake_read_context.Context;
const A = std.mem.Allocator;

pub const Policy = enum { automatic, required };
pub const Selected = struct {
    state: std.json.Parsed(catalog.State),
    delete_objects: catalog.Digest,
    inventory_lease: ?@import("lake_index_decoded_metadata.zig").Owned(InventoryMetadata) = null,
    directory_lease: ?@import("lake_index_decoded_metadata.zig").Owned(@import("lake_index_directory.zig").Document) = null,
    pub fn publication(self: Selected) catalog.Publication {
        return self.state.value.published.?;
    }
    pub fn deinit(self: *Selected) void {
        if (self.inventory_lease) |owned| owned.release();
        if (self.directory_lease) |owned| owned.release();
        self.state.deinit();
        self.* = undefined;
    }
};

pub fn select(a: A, table: sql.Table, source: *local.serverless_query_lake_serving.ServingSource, store: *Store, context: Context, policy: Policy) !?Selected {
    return selectCached(a, table, source, store, context, policy, null);
}
pub fn selectCached(a: A, table: sql.Table, source: *local.serverless_query_lake_serving.ServingSource, store: *Store, context: Context, policy: Policy, cache: ?*local.serverless_query_lake_serving_cache.Cache) !?Selected {
    try context.ensureActive();
    return selectVerified(a, table, source, store, context, cache) catch |err| {
        try context.ensureActive();
        if (err == error.OutOfMemory) return err;
        if (policy == .required) return err;
        return null;
    };
}
fn selectVerified(a: A, table: sql.Table, source: *local.serverless_query_lake_serving.ServingSource, store: *Store, context: Context, cache: ?*local.serverless_query_lake_serving_cache.Cache) !Selected {
    const definitions = table.external_indexes orelse return error.ExternalLakeIndexUnavailable;
    var state = try catalog.parse(a, definitions.catalog_json);
    errdefer state.deinit();
    const published = state.value.published orelse return error.ExternalLakeIndexUnavailable;
    if (published.inventory.byte_len > (local.serverless_external_source_mod.codec.DecodeLimits{}).max_artifact_bytes) return error.ExternalSourceInventoryTooLarge;
    if (!std.mem.eql(u8, &published.signature.desired, &definitions.desired)) return error.ExternalLakeIndexDefinitionChanged;
    if (!std.mem.eql(u8, &published.signature.store, &store.identity)) return error.ExternalLakeIndexStoreChanged;
    const binding = (table.external_base_source orelse return error.InvalidExternalTableBinding).binding;
    if (!std.mem.eql(u8, &published.signature.credentials, &try source.credentialIdentity(binding))) return error.ExternalLakeIndexCredentialsChanged;
    var artifacts = store.artifactStore();
    artifacts.allocator = a;
    const normalized: @import("antfly_cancellation").CancellationToken = if (context.cancellation) |token| .{ .ptr = token.ptr, .is_cancelled_fn = token.is_cancelled_fn } else .none;
    const cached: ?@import("lake_index_aggregate_artifact.zig").CachedRead = if (cache) |shared| .{ .cache = shared, .scope = store.identity, .context = context } else null;
    var inventory_lease: ?@import("lake_index_decoded_metadata.zig").Owned(InventoryMetadata) = null;
    errdefer if (inventory_lease) |owned| owned.release();
    var owned_inventory: ?InventoryMetadata = null;
    defer if (owned_inventory) |*owned| {
        owned.by_id.deinit(a);
        owned.inventory.deinit(a);
    };
    if (cached) |shared| inventory_lease = try @import("lake_index_decoded_metadata.zig").acquire(InventoryMetadata, shared, artifacts, published.inventory, normalized, InventoryMetadata.load) else owned_inventory = try InventoryMetadata.load(a, artifacts, published.inventory, normalized, null);
    const metadata = if (inventory_lease) |owned| owned.value else &owned_inventory.?;
    const inventory = metadata.inventory;
    try local.serverless_query_lake_scan_plan.validateBindingInventoryIdentity(binding, inventory);
    // The plan key is derived from freshly read metadata bytes and credential
    // scope, never a snapshot label or TTL. Mutable sources always revalidate.
    const proof_key = if (source.immutable_objects and source.plan_lease != null and source.plan_identity != null and cache != null) key: {
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update("native-lake-verified-coverage-v1");
        hash.update(&source.plan_identity.?);
        hash.update(&published.signature.desired);
        hash.update(&published.signature.source);
        hash.update(&published.signature.credentials);
        hash.update(&published.signature.store);
        hash.update(published.inventory.artifact_id);
        hash.update(published.inventory.checksum);
        var result: [32]u8 = undefined;
        hash.final(&result);
        break :key result;
    } else null;
    const proof_lease = if (proof_key) |key| cache.?.decoded.lookup(key) else null;
    defer if (proof_lease) |lease| lease.release();
    const data = if (proof_lease) |lease|
        @as(*const coverage.DataProof, @ptrCast(@alignCast(lease.item.payload.extension))).*
    else
        try coverage.pinData(source, context, inventory, &metadata.by_id);
    const pinned = try coverage.finish(source, context, data);
    if (!std.mem.eql(u8, &published.signature.source, &pinned.source)) return error.ExternalLakeIndexSourceChanged;
    if (proof_lease == null) {
        if (inventory.files.len != source.inventory.files.len) return error.InvalidExternalLakeIndexCoverage;
        for (source.inventory.files) |current| {
            const stored = metadata.by_id.get(current.file_id) orelse return error.InvalidExternalLakeIndexCoverage;
            if (!std.mem.eql(u8, stored.object_uri, current.object_uri) or
                !std.mem.eql(u8, stored.etag, current.etag) or !std.mem.eql(u8, stored.version_id, current.version_id) or
                stored.byte_len != current.byte_len or stored.row_count != current.row_count) return error.InvalidExternalLakeIndexCoverage;
        }
        if (proof_key) |key| {
            const lease = try cache.?.decoded.create(4096);
            defer lease.release();
            const proof = try lease.item.arena.allocator().create(coverage.DataProof);
            proof.* = data;
            lease.item.payload = .{ .extension = proof };
            cache.?.decoded.publish(key, lease);
        }
    } else if (inventory_lease) |owned| {
        // Resolve provider versions only for files that survive pruning. The
        // source retains its own lease, independent of Selected's lifetime.
        if (source.verified_inventory_lease) |lease| lease.release();
        source.verified_inventory_lease = owned.lease.retain();
        source.verified_files = &owned.value.by_id;
    }
    try context.ensureActive();
    var directory_lease: ?@import("lake_index_decoded_metadata.zig").Owned(@import("lake_index_directory.zig").Document) = null;
    errdefer if (directory_lease) |owned| owned.release();
    if (cached) |shared| {
        if (published.directory) |directory| {
            directory_lease = try @import("lake_index_decoded_metadata.zig").acquire(@import("lake_index_directory.zig").Document, shared, artifacts, .{ .kind = .external_base_source, .artifact_id = directory.artifact_id, .checksum = directory.checksum, .byte_len = directory.byte_len }, normalized, @import("lake_index_directory.zig").loadDocument);
            const document = directory_lease.?.value;
            if (document.declarations.len != directory.count) return error.InvalidLakeIndexCatalog;
            state.value.published.?.declarations = document.declarations;
            state.value.published.?.file_contributions = document.file_contributions;
            try state.value.published.?.validate();
        }
    } else try @import("lake_index_directory.zig").hydrate(state.arena.allocator(), artifacts, &state.value.published.?, normalized, cached);
    return .{ .state = state, .delete_objects = pinned.delete_objects, .inventory_lease = inventory_lease, .directory_lease = directory_lease };
}

const InventoryMetadata = struct {
    inventory: local.serverless_external_source_types.Inventory,
    by_id: std.StringHashMapUnmanaged(*const local.serverless_external_source_types.FileEntry) = .empty,
    fn load(a: A, store: @import("../serverless/artifacts/store.zig").ArtifactStore, ref: local.serverless_manifest_artifact_ref.ArtifactRef, cancellation: @import("antfly_cancellation").CancellationToken, cached: ?@import("lake_index_aggregate_artifact.zig").CachedRead) !InventoryMetadata {
        const bytes = try @import("lake_index_aggregate_artifact.zig").readArtifact(a, store, .{ .artifact_id = ref.artifact_id, .checksum = ref.checksum, .byte_len = ref.byte_len }, cancellation, cached);
        defer a.free(bytes);
        var inventory = try local.serverless_external_source_mod.decodeInventoryAlloc(a, bytes);
        errdefer inventory.deinit(a);
        var result: InventoryMetadata = .{ .inventory = inventory };
        errdefer result.by_id.deinit(a);
        try result.by_id.ensureTotalCapacity(a, @intCast(inventory.files.len));
        for (inventory.files) |*file| result.by_id.putAssumeCapacity(file.file_id, file);
        return result;
    }
};

test "external lake coverage proofs bind immutable plans publications and fresh request authority" {
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("coverage-proof-selection");
    defer directory.cleanup();
    var fs = try local.storage_object_storage.FilesystemObjectStorage.init(a, directory.path());
    defer fs.deinit();
    var client = fs.client();
    try client.makeBucket("antfly");
    const data = try local.serverless_query_lake_parquet_rowgroup.buildTestPlainI64AndByteArrayParquetObjectAlloc(a, &.{}, &.{.{ .column_id = "body", .converted_type = 0, .values = &.{"needle"} }});
    defer a.free(data);
    var upload = try client.putObject("antfly", "part.parquet", data, .{});
    upload.deinit(a);
    const schema_json = try std.fmt.allocPrint(a, "{{\"version\":1,\"storage_mode\":\"relational\",\"default_type\":\"row\",\"base_source\":{{\"kind\":\"external\",\"table_id\":\"lake\",\"format\":\"parquet\",\"object_mutability\":\"immutable\",\"uri\":\"file://{s}\",\"schema_fingerprint\":\"schema\"}},\"document_schemas\":{{\"row\":{{\"schema\":{{\"type\":\"object\",\"properties\":{{\"body\":{{\"type\":\"string\"}}}}}}}}}}}}", .{directory.path()});
    defer a.free(schema_json);
    var binding = (try local.serverless_external_source_schema_binding.externalBindingFromSchemaJsonAlloc(a, schema_json)).?;
    defer binding.deinit(a);
    var source = try local.serverless_query_lake_serving.ServingSource.open(a, .{ .storage_mode = .relational, .external_base_source = binding }, .{});
    defer source.deinit();
    var store = try Store.openNative(a, null, null, false, .standalone, directory.path());
    defer store.deinit();
    var artifact_store = store.artifactStore();
    var table: local.common_topology_records.TableRecord = .{ .table_id = 9, .name = "lake", .schema_json = schema_json, .indexes_json = "{\"body_text\":{\"type\":\"full_text\",\"field\":\"body\"}}" };
    const publication = @import("lake_index_publication.zig");
    const pending = try publication.begin(a, std.testing.io, table, &source, store.identity, .{}, 100, 20);
    defer a.free(pending);
    table.lake_index_catalog_json = pending;
    const Clock = struct {
        fn now(_: *const anyopaque) !u64 {
            return 101;
        }
    };
    var dummy: u8 = 0;
    const published = try publication.build(a, &artifact_store, table, &source, store.identity, .{}, .none, .{ .ptr = &dummy, .now_ms = Clock.now });
    defer a.free(published);
    var parsed = try catalog.parse(a, published);
    defer parsed.deinit();
    const sql_table: sql.Table = .{ .id = 9, .physical_name = "lake", .schema_version = 1, .columns = &.{}, .external_base_source = binding, .external_indexes = .{ .catalog_json = published, .indexes_json = table.indexes_json, .schema_json = schema_json, .desired = parsed.value.published.?.signature.desired } };
    var cache = local.serverless_query_lake_serving_cache.Cache.init(a);
    defer cache.deinit();
    for (0..4) |phase| {
        var current = try local.serverless_query_lake_serving.ServingSource.open(a, .{ .storage_mode = .relational, .external_base_source = binding }, .{});
        defer current.deinit();
        // A small deterministic plan-owner fixture isolates proof admission
        // from manifest parsing. Production derives this key from fresh
        // Iceberg metadata bytes and the authorized credential scope.
        current.plan_identity = @splat(if (phase == 2) @as(u8, 2) else 1);
        current.plan_lease = try cache.decoded.create(64);
        current.lazy_versions = true;
        if (phase == 3) current.immutable_objects = false;
        var selected = (try selectCached(a, sql_table, &current, &store, .{}, .required, &cache)).?;
        defer selected.deinit();
        try std.testing.expectEqual(phase == 1, current.verified_files != null);
        if (phase == 1) {
            try std.testing.expect(!current.pinned_files[0]);
            selected.deinit();
            // The source holds its own inventory lease after selection closes.
            try std.testing.expect(current.verified_files.?.get(current.inventory.files[0].file_id) != null);
            selected = (try selectCached(a, sql_table, &current, &store, .{}, .required, &cache)).?;
        }
        try std.testing.expectError(error.DeadlineExceeded, selectCached(a, sql_table, &current, &store, .{ .deadline_ns = 1 }, .required, &cache));
    }
}
