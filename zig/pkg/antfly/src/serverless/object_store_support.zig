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
const object_storage = @import("../storage/object_storage.zig");
const bedrock = @import("../inference/bedrock.zig");
const remote_uri = @import("remote_uri.zig");

const Allocator = std.mem.Allocator;

pub const S3Options = struct {
    endpoint: ?[]const u8 = null,
    region: ?[]const u8 = null,
    access_key_id: ?[]const u8 = null,
    secret_access_key: ?[]const u8 = null,
    session_token: ?[]const u8 = null,
    credential_source: bedrock.CredentialSource = .default,
    use_ssl: bool = true,
    addressing_style: object_storage.S3.AddressingStyle = .path,
    create_bucket: bool = false,
};

pub fn s3ConfigAlloc(alloc: Allocator, options: ?S3Options) !object_storage.S3.Config {
    const opts = options orelse S3Options{};
    return try object_storage.S3.fromEnvAlloc(
        alloc,
        opts.endpoint,
        opts.use_ssl,
        opts.access_key_id,
        opts.secret_access_key,
        opts.session_token,
        opts.region,
        opts.addressing_style,
    );
}

pub const OpenedObjectStore = struct {
    alloc: Allocator,
    client: object_storage.ObjectStorage,
    fs_client: ?*object_storage.FilesystemObjectStorage = null,
    gcs_client: ?*object_storage.Gcs.JsonApiClient = null,
    s3_client: ?*object_storage.S3.Client = null,
    owns_client: bool = true,
    bucket: []u8,
    prefix: []u8,

    pub fn initRemoteUri(alloc: Allocator, uri: []const u8, file_bucket: []const u8) !OpenedObjectStore {
        return try initRemoteUriWithS3Options(alloc, uri, file_bucket, null);
    }

    pub fn initRemoteUriWithS3Options(
        alloc: Allocator,
        uri: []const u8,
        file_bucket: []const u8,
        s3_options: ?S3Options,
    ) !OpenedObjectStore {
        var parsed = try remote_uri.parseAlloc(alloc, uri);
        defer switch (parsed) {
            .file => |value| alloc.free(value),
            .gcs => |*value| value.deinit(alloc),
            .s3 => |*value| value.deinit(alloc),
        };

        return switch (parsed) {
            .file => |path| blk: {
                const file_uri = try std.fmt.allocPrint(alloc, "file://{s}", .{path});
                defer alloc.free(file_uri);
                break :blk try initFileUri(alloc, file_uri, file_bucket);
            },
            .gcs => |value| try initGcsUri(alloc, value.bucket, value.prefix),
            .s3 => |value| try initS3UriWithOptions(alloc, value.bucket, value.prefix, s3_options),
        };
    }

    pub fn initFileUri(alloc: Allocator, uri: []const u8, bucket: []const u8) !OpenedObjectStore {
        const path = try remote_uri.filePathFromUriAlloc(alloc, uri);
        defer alloc.free(path);
        const fs = try alloc.create(object_storage.FilesystemObjectStorage);
        errdefer alloc.destroy(fs);
        fs.* = try object_storage.FilesystemObjectStorage.init(alloc, path);

        var owned_client = fs.client();
        if (!(try owned_client.bucketExists(bucket))) try owned_client.makeBucket(bucket);
        return .{
            .alloc = alloc,
            .client = owned_client,
            .fs_client = fs,
            .bucket = try alloc.dupe(u8, bucket),
            .prefix = try alloc.dupe(u8, ""),
        };
    }

    pub fn initGcsUri(alloc: Allocator, bucket: []const u8, prefix: []const u8) !OpenedObjectStore {
        const gcs = try alloc.create(object_storage.Gcs.JsonApiClient);
        errdefer alloc.destroy(gcs);
        const cfg = try object_storage.Gcs.jsonApiClientConfigFromEnvAlloc(alloc);
        gcs.* = try object_storage.Gcs.JsonApiClient.init(alloc, cfg);

        var owned_client = gcs.client();
        if (!(try owned_client.bucketExists(bucket))) try owned_client.makeBucket(bucket);
        return .{
            .alloc = alloc,
            .client = owned_client,
            .gcs_client = gcs,
            .bucket = try alloc.dupe(u8, bucket),
            .prefix = try alloc.dupe(u8, prefix),
        };
    }

    pub fn initS3Uri(alloc: Allocator, bucket: []const u8, prefix: []const u8) !OpenedObjectStore {
        return try initS3UriWithOptions(alloc, bucket, prefix, null);
    }

    pub fn initS3UriWithOptions(
        alloc: Allocator,
        bucket: []const u8,
        prefix: []const u8,
        options: ?S3Options,
    ) !OpenedObjectStore {
        const s3 = try alloc.create(object_storage.S3.Client);
        errdefer alloc.destroy(s3);
        const cfg = try s3ConfigAlloc(alloc, options);
        s3.* = try object_storage.S3.Client.init(alloc, cfg);

        var owned_client = s3.client();
        if (!(try owned_client.bucketExists(bucket))) try owned_client.makeBucket(bucket);
        return .{
            .alloc = alloc,
            .client = owned_client,
            .s3_client = s3,
            .bucket = try alloc.dupe(u8, bucket),
            .prefix = try alloc.dupe(u8, prefix),
        };
    }

    pub fn initWithClient(alloc: Allocator, client: object_storage.ObjectStorage, bucket: []const u8, prefix: []const u8) !OpenedObjectStore {
        var owned_client = client;
        if (!(try owned_client.bucketExists(bucket))) try owned_client.makeBucket(bucket);
        return .{
            .alloc = alloc,
            .client = owned_client,
            .owns_client = false,
            .bucket = try alloc.dupe(u8, bucket),
            .prefix = try alloc.dupe(u8, prefix),
        };
    }

    pub fn deinit(self: *OpenedObjectStore) void {
        if (self.owns_client) self.client.deinit();
        if (self.fs_client) |fs| self.alloc.destroy(fs);
        if (self.gcs_client) |gcs| self.alloc.destroy(gcs);
        if (self.s3_client) |s3| self.alloc.destroy(s3);
        self.alloc.free(self.bucket);
        self.alloc.free(self.prefix);
        self.* = undefined;
    }
};

test "storage.ha cleanup object store does not create a missing bucket while opening deletion authority" {
    const alloc = std.testing.allocator;
    var memory = object_storage.MemoryObjectStorage.init(alloc);
    defer memory.deinit();
    var client = memory.client();
    try std.testing.expect(!(try client.bucketExists("missing-ha-bucket")));

    var opened = try OpenedObjectStore.initWithExistingClient(
        alloc,
        client,
        "missing-ha-bucket",
        "instances/instance-a/ha-seeds/",
    );
    defer opened.deinit();
    try std.testing.expect(!(try client.bucketExists("missing-ha-bucket")));
}
