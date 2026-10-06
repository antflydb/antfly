// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//! Durable native index storage, separate from the evictable lake read cache.
const std = @import("std");
const local = @import("antfly_local_sources");
const configured = @import("../serverless/configured_object_store_support.zig");
const artifacts = @import("../serverless/artifacts/object_store.zig");
const stores = @import("../serverless/artifacts/store.zig");
const A = std.mem.Allocator;

pub const Store = struct {
    opened: local.serverless_object_store_support.OpenedObjectStore,
    implementation: artifacts.ObjectStore,
    identity: local.metadata_lake_index_catalog.Digest,

    pub fn open(a: A, config: *const local.common_config.Config, secrets: ?*local.common_secrets.FileStore, read_only: bool) !Store {
        return openNative(a, config, secrets, read_only, config.deployment_mode, null);
    }
    pub fn openNative(a: A, config: ?*const local.common_config.Config, secrets: ?*local.common_secrets.FileStore, read_only: bool, deployment: local.common_config.DeploymentMode, local_base_dir: ?[]const u8) !Store {
        const storage = if (config) |value| value.storage else local.common_config.Config.StorageConfig{};
        var opened = if (storage.artifacts.connection != null)
            try configured.openNativeArtifactObjectStoreAlloc(a, config.?, secrets, read_only)
        else fallback: {
            if (deployment != .standalone and deployment != .embedded) return error.NativeArtifactStorageRequired;
            const base = local_base_dir orelse storage.local_base_dir orelse
                (if (storage.lite_path) |path| std.fs.path.dirname(path) orelse "." else return error.NativeArtifactStorageRequired);
            const root = try std.fs.path.join(a, &.{ base, "artifacts" });
            defer a.free(root);
            const uri = try std.fmt.allocPrint(a, "file://{s}", .{root});
            defer a.free(uri);
            break :fallback try local.serverless_object_store_support.OpenedObjectStore.initFileUriWithOptions(a, uri, "native-lake-indexes", .{ .ensure_bucket = !read_only });
        };
        errdefer opened.deinit();
        // The configured opener already enforces create-if-missing policy.
        // This wrapper must never add bucket provisioning authority.
        var implementation = try artifacts.ObjectStore.initWithClientOptions(a, opened.client, opened.bucket, opened.prefix, .{ .read_only = read_only });
        errdefer implementation.deinit();
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
        return .{ .opened = opened, .implementation = implementation, .identity = identity };
    }
    /// The Store's address must remain stable while this handle is borrowed.
    pub fn artifactStore(self: *Store) stores.ArtifactStore {
        return self.implementation.artifactStore();
    }
    pub fn deinit(self: *Store) void {
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
    config.deployment_mode = .distributed;
    try std.testing.expectError(error.NativeArtifactStorageRequired, Store.open(a, &config, null, false));
}
