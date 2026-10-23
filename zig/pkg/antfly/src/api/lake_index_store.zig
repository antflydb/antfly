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

//! Durable native index storage, separate from the evictable lake read cache.
const std = @import("std");
const local = @import("antfly_local_sources");
const configured = @import("../serverless/configured_object_store_support.zig");
const artifacts = @import("../serverless/artifacts/object_store.zig");
const stores = @import("../serverless/artifacts/store.zig");
const A = std.mem.Allocator;

pub const Store = struct {
    const Reopen = struct { config: ?*const local.common_config.Config, secrets: ?*local.common_secrets.FileStore, deployment: local.common_config.DeploymentMode, local_base_dir: ?[]const u8 };
    opened: local.serverless_object_store_support.OpenedObjectStore,
    implementation: artifacts.ObjectStore,
    identity: local.metadata_lake_index_catalog.Digest,
    locator: local.metadata_lake_index_catalog.StoreLocator,
    cloud_binding: ?@import("lake_cloud_publication.zig").Binding = null,
    reopen_options: ?Reopen = null,

    pub fn open(a: A, config: *const local.common_config.Config, secrets: ?*local.common_secrets.FileStore, read_only: bool) !Store {
        return openNative(a, config, secrets, read_only, config.deployment_mode, null);
    }
    pub fn openNative(a: A, config: ?*const local.common_config.Config, secrets: ?*local.common_secrets.FileStore, read_only: bool, deployment: local.common_config.DeploymentMode, local_base_dir: ?[]const u8) !Store {
        return openNativeFor(a, config, secrets, read_only, deployment, local_base_dir, .{});
    }
    pub fn openNativeFor(a: A, config: ?*const local.common_config.Config, secrets: ?*local.common_secrets.FileStore, read_only: bool, deployment: local.common_config.DeploymentMode, local_base_dir: ?[]const u8, context: local.serverless_query_lake_read_context.Context) !Store {
        try context.ensureActive();
        const storage = if (config) |value| value.storage else local.common_config.Config.StorageConfig{};
        var opened = if (storage.artifacts.connection != null)
            try configured.openNativeArtifactObjectStoreFor(a, config.?, secrets, read_only, context)
        else fallback: {
            if (deployment != .standalone and deployment != .embedded) return error.NativeArtifactStorageRequired;
            const base = local_base_dir orelse storage.local_base_dir orelse
                (if (storage.lite_path) |path| std.fs.path.dirname(path) orelse "." else return error.NativeArtifactStorageRequired);
            const root = try std.fs.path.join(a, &.{ base, "artifacts" });
            defer a.free(root);
            const uri = try std.fmt.allocPrint(a, "file://{s}", .{root});
            defer a.free(uri);
            break :fallback try local.serverless_object_store_support.OpenedObjectStore.initFileUriWithOptions(a, uri, "native-lake-indexes", .{ .ensure_bucket = !read_only, .context = context });
        };
        errdefer opened.deinit();
        // The configured opener already enforces create-if-missing policy.
        // This wrapper must never add bucket provisioning authority.
        var implementation = try artifacts.ObjectStore.initWithClientOptions(a, opened.client, opened.bucket, opened.prefix, .{ .read_only = read_only, .filesystem = opened.fs_client, .context = context });
        errdefer implementation.deinit();
        implementation.cloud_retention_qualified = if (config) |value| value.lake_indexes.cloud_generations.qualified_retention else false;
        implementation.upload_context = context;
        const encoded = try std.json.Stringify.valueAlloc(a, .{
            .domain = "native-lake-artifact-store-v1",
            .connection = storage.artifacts.connection,
            .bucket = opened.bucket,
            .prefix = opened.prefix,
            .filesystem_root = if (opened.fs_client) |fs| @as(?[]const u8, fs.root_dir) else null,
            .s3_credentials = if (opened.s3_client) |s3| s3.cfg.credentials else null,
            .gcs_endpoint = if (opened.gcs_client) |gcs| @as(?[]const u8, gcs.cfg.endpoint) else null,
            .gcs_bearer = if (opened.gcs_client) |gcs| switch (gcs.cfg.auth) {
                .bearer_token => |token| @as(?[]const u8, token),
                else => null,
            } else null,
            .gcs_credentials = if (opened.gcs_client) |gcs| switch (gcs.cfg.auth) {
                .google_token_source => |source| @as(?@TypeOf(source.cfg), source.cfg),
                else => null,
            } else null,
        }, .{});
        defer a.free(encoded);
        var identity: local.metadata_lake_index_catalog.Digest = undefined;
        std.crypto.hash.sha2.Sha256.hash(encoded, &identity, .{});
        implementation.cloud_cache_scope = identity;
        const locator: local.metadata_lake_index_catalog.StoreLocator = .{
            .protocol = if (opened.fs_client != null) .filesystem else if (opened.s3_client != null) .s3 else .gcs,
            .connection = storage.artifacts.connection,
            .bucket = opened.bucket,
            .prefix = opened.prefix,
            .root = if (opened.fs_client) |fs| fs.root_dir else if (opened.s3_client) |s3| s3.cfg.credentials.endpoint else if (opened.gcs_client) |gcs| gcs.cfg.endpoint else return error.NativeArtifactStorageRequired,
            .tls = if (opened.s3_client) |s3| s3.cfg.credentials.use_ssl else true,
        };
        try locator.validate();
        return .{ .opened = opened, .implementation = implementation, .identity = identity, .locator = locator, .reopen_options = .{ .config = config, .secrets = secrets, .deployment = deployment, .local_base_dir = local_base_dir } };
    }
    /// Reopen a retained namespace with currently authorized credentials. A
    /// connection can rotate keys, but cannot silently redirect old collection
    /// work into a different physical endpoint/root. Missing old connections
    /// leave their publications retained until operators restore the mapping.
    pub fn openRetained(a: A, config: *const local.common_config.Config, secrets: ?*local.common_secrets.FileStore, locator: local.metadata_lake_index_catalog.StoreLocator, read_only: bool) !Store {
        return openRetainedNative(a, config, secrets, locator, read_only, config.deployment_mode, null);
    }
    pub fn openRetainedNative(a: A, config: *const local.common_config.Config, secrets: ?*local.common_secrets.FileStore, locator: local.metadata_lake_index_catalog.StoreLocator, read_only: bool, deployment: local.common_config.DeploymentMode, local_base_dir: ?[]const u8) !Store {
        try locator.validate();
        var retained = config.*;
        retained.storage.artifacts = .{ .connection = if (locator.connection) |connection| @constCast(connection) else null, .bucket = @constCast(locator.bucket), .prefix = @constCast(locator.prefix) };
        var opened = try openNative(a, &retained, secrets, read_only, deployment, local_base_dir);
        errdefer opened.deinit();
        // The temporary configuration override must not escape.
        opened.reopen_options = .{ .config = config, .secrets = secrets, .deployment = deployment, .local_base_dir = local_base_dir };
        if (opened.locator.protocol != locator.protocol or opened.locator.tls != locator.tls or !std.mem.eql(u8, opened.locator.root, locator.root) or !std.mem.eql(u8, opened.locator.bucket, locator.bucket) or !std.mem.eql(u8, opened.locator.prefix, locator.prefix)) return error.NativeArtifactStoreLocationChanged;
        return opened;
    }
    /// Reopen independent client ownership for a query-local lazy selector.
    /// Both physical location and current credential scope must still match.
    pub fn reopenReadOnly(self: *const Store, a: A) !Store {
        const options = self.reopen_options orelse return error.NativeArtifactStorageRequired;
        var result = if (options.config) |config|
            try openRetainedNative(a, config, options.secrets, self.locator, true, options.deployment, options.local_base_dir)
        else
            try openNative(a, null, options.secrets, true, options.deployment, options.local_base_dir);
        errdefer result.deinit();
        if (!std.mem.eql(u8, &result.identity, &self.identity)) return error.ExternalLakeIndexStoreChanged;
        return result;
    }
    pub fn bindPublication(self: *Store, publication: local.metadata_lake_index_catalog.Publication, token: @import("antfly_cancellation").CancellationToken) !void {
        if (self.cloud_binding) |*held| {
            const expected = publication.cloud_generations orelse return error.CatalogGenerationChanged;
            if (!std.mem.eql(u8, held.reader.root.?.artifact_id, expected.root.artifact_id) or !std.mem.eql(u8, held.reader.root.?.generation, expected.root.generation)) return error.CatalogGenerationChanged;
            return;
        }
        self.cloud_binding = try @import("lake_cloud_publication.zig").Binding.init(self.implementation.alloc, &self.implementation, publication, token);
    }
    pub fn bindGenerationMap(self: *Store, cloud: ?local.metadata_lake_index_catalog.CloudGenerations, token: @import("antfly_cancellation").CancellationToken) !void {
        const selected = cloud orelse return;
        if (self.cloud_binding != null) return error.CatalogGenerationChanged;
        self.cloud_binding = try @import("lake_cloud_publication.zig").Binding.initMap(self.implementation.alloc, &self.implementation, selected, token);
    }
    /// The Store's address must remain stable while this handle is borrowed.
    pub fn artifactStore(self: *Store) stores.ArtifactStore {
        return self.implementation.artifactStore();
    }
    pub fn deinit(self: *Store) void {
        if (self.cloud_binding) |*binding| binding.deinit();
        self.implementation.deinit();
        self.opened.deinit();
        self.* = undefined;
    }
};

test "external lake native artifact storage survives reopen and excludes read cache" {
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("lake-native-store");
    defer directory.cleanup();
    const json = try std.json.Stringify.valueAlloc(a, .{ .deployment_mode = "standalone", .storage = .{ .engine = "local", .local = .{ .base_dir = directory.path() } } }, .{});
    defer a.free(json);
    var config = try local.common_config.Config.parseFromSlice(a, json);
    defer config.deinit();
    var writer = try Store.open(a, &config, null, false);
    const identity = writer.identity;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const encoded_locator = try std.json.Stringify.valueAlloc(arena.allocator(), writer.locator, .{});
    const retained_locator = try std.json.parseFromSliceLeaky(local.metadata_lake_index_catalog.StoreLocator, arena.allocator(), encoded_locator, .{ .allocate = .alloc_always });
    var handle = writer.artifactStore();
    var artifact = try handle.put("persistent native artifact");
    defer artifact.deinit(a);
    writer.deinit();
    var reader = try Store.open(a, &config, null, true);
    defer reader.deinit();
    try std.testing.expectEqual(identity, reader.identity);
    var reads = reader.artifactStore();
    const bytes = try reads.getVerifiedAllocWithCancellation(artifact.artifact_id, artifact.byte_len, artifact.checksum, .none);
    defer a.free(bytes);
    try std.testing.expectEqualStrings("persistent native artifact", bytes);
    var retained = try Store.openRetained(a, &config, null, retained_locator, true);
    defer retained.deinit();
    try std.testing.expectEqual(identity, retained.identity);
    var wrong = retained_locator;
    wrong.root = "a different physical root";
    try std.testing.expectError(error.NativeArtifactStoreLocationChanged, Store.openRetained(a, &config, null, wrong, true));
    config.deployment_mode = .distributed;
    try std.testing.expectError(error.NativeArtifactStorageRequired, Store.open(a, &config, null, false));
}
