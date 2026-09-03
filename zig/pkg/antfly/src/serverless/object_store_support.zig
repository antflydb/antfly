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
const google_auth = @import("antfly_google").auth;
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

pub const GcsCredentialSource = enum { default, bearer_token, service_account };

pub const GcsOptions = struct {
    endpoint: ?[]const u8 = null,
    upload_endpoint: ?[]const u8 = null,
    project_id: ?[]const u8 = null,
    credential_source: GcsCredentialSource = .default,
    bearer_token: ?[]const u8 = null,
    service_account_json: ?[]const u8 = null,
    credentials_path: ?[]const u8 = null,
    scope: ?[]const u8 = null,
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

pub fn gcsConfigAlloc(alloc: Allocator, maybe_options: ?GcsOptions) !object_storage.Gcs.JsonApiConfig {
    const options = maybe_options orelse GcsOptions{};
    var cfg = switch (options.credential_source) {
        .default => try object_storage.Gcs.jsonApiClientConfigFromEnvAlloc(alloc),
        .bearer_token => try object_storage.Gcs.jsonApiClientConfigWithBearerTokenAlloc(
            alloc,
            options.bearer_token orelse return error.MissingGcsBearerToken,
            options.project_id,
        ),
        .service_account => blk: {
            var service_account = if (options.service_account_json) |raw|
                try google_auth.parseServiceAccountJsonAlloc(alloc, raw)
            else if (options.credentials_path) |path|
                try google_auth.serviceAccountFromFileAlloc(alloc, path)
            else
                return error.MissingServiceAccount;
            var service_account_owned = true;
            errdefer if (service_account_owned) service_account.deinit(alloc);
            const discovered_project = service_account.project_id;
            var auth_cfg = try google_auth.configFromServiceAccountAlloc(
                alloc,
                service_account,
                options.scope orelse google_auth.default_scope,
            );
            service_account_owned = false;
            var auth_cfg_owned = true;
            errdefer if (auth_cfg_owned) auth_cfg.deinit(alloc);
            const source = try alloc.create(google_auth.CachedTokenSource);
            errdefer alloc.destroy(source);
            source.* = try google_auth.CachedTokenSource.init(alloc, auth_cfg);
            auth_cfg_owned = false;
            var source_owned = true;
            errdefer if (source_owned) {
                source.deinit();
                alloc.destroy(source);
            };
            var result = try object_storage.Gcs.jsonApiClientConfigAlloc(alloc);
            errdefer result.deinit(alloc);
            result.auth = .{ .google_token_source = source };
            source_owned = false;
            if (options.project_id orelse discovered_project) |project_id| {
                result.project_id = try alloc.dupe(u8, project_id);
            }
            break :blk result;
        },
    };
    errdefer cfg.deinit(alloc);
    if (options.endpoint) |endpoint| {
        alloc.free(cfg.endpoint);
        cfg.endpoint = try alloc.dupe(u8, endpoint);
    }
    if (options.upload_endpoint) |upload_endpoint| {
        alloc.free(cfg.upload_endpoint);
        cfg.upload_endpoint = try alloc.dupe(u8, upload_endpoint);
    }
    if (options.project_id) |project_id| {
        if (cfg.project_id) |owned| alloc.free(owned);
        cfg.project_id = try alloc.dupe(u8, project_id);
    }
    return cfg;
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

    pub const OpenOptions = struct {
        ensure_bucket: bool = true,
    };

    pub fn initRemoteUri(alloc: Allocator, uri: []const u8, file_bucket: []const u8) !OpenedObjectStore {
        return try initRemoteUriWithS3AndOpenOptions(alloc, uri, file_bucket, null, .{});
    }

    pub fn initRemoteUriWithOptions(
        alloc: Allocator,
        uri: []const u8,
        file_bucket: []const u8,
        options: OpenOptions,
    ) !OpenedObjectStore {
        return try initRemoteUriWithS3AndOpenOptions(alloc, uri, file_bucket, null, options);
    }

    /// Opens an existing S3 namespace without creating a missing bucket. This
    /// is the only safe constructor for destructive cleanup: a missing bucket
    /// must be observed as an error, not manufactured into an empty success.
    pub fn initExistingS3RemoteUri(alloc: Allocator, uri: []const u8) !OpenedObjectStore {
        var value = try remote_uri.bucketPathFromS3UriAlloc(alloc, uri);
        defer value.deinit(alloc);
        return try initExistingS3UriWithOptions(alloc, value.bucket, value.prefix, null);
    }

    pub fn initRemoteUriWithS3Options(
        alloc: Allocator,
        uri: []const u8,
        file_bucket: []const u8,
        s3_options: ?S3Options,
    ) !OpenedObjectStore {
        return try initRemoteUriWithS3AndOpenOptions(alloc, uri, file_bucket, s3_options, .{});
    }

    pub fn initRemoteUriWithS3AndOpenOptions(
        alloc: Allocator,
        uri: []const u8,
        file_bucket: []const u8,
        s3_options: ?S3Options,
        open_options: OpenOptions,
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
                break :blk try initFileUriWithOptions(alloc, file_uri, file_bucket, open_options);
            },
            .gcs => |value| try initGcsUriWithOptions(alloc, value.bucket, value.prefix, open_options),
            .s3 => |value| try initS3UriWithS3AndOpenOptions(alloc, value.bucket, value.prefix, s3_options, open_options),
        };
    }

    pub fn initFileUri(alloc: Allocator, uri: []const u8, bucket: []const u8) !OpenedObjectStore {
        return try initFileUriWithOptions(alloc, uri, bucket, .{});
    }

    pub fn initFileUriWithOptions(
        alloc: Allocator,
        uri: []const u8,
        bucket: []const u8,
        options: OpenOptions,
    ) !OpenedObjectStore {
        const path = try remote_uri.filePathFromUriAlloc(alloc, uri);
        defer alloc.free(path);
        const fs = try alloc.create(object_storage.FilesystemObjectStorage);
        errdefer alloc.destroy(fs);
        fs.* = try object_storage.FilesystemObjectStorage.init(alloc, path);

        var owned_client = fs.client();
        if (options.ensure_bucket and !(try owned_client.bucketExists(bucket))) try owned_client.makeBucket(bucket);
        return .{
            .alloc = alloc,
            .client = owned_client,
            .fs_client = fs,
            .bucket = try alloc.dupe(u8, bucket),
            .prefix = try alloc.dupe(u8, ""),
        };
    }

    pub fn initGcsUri(alloc: Allocator, bucket: []const u8, prefix: []const u8) !OpenedObjectStore {
        return try initGcsUriWithOptions(alloc, bucket, prefix, .{});
    }

    pub fn initGcsUriWithOptions(
        alloc: Allocator,
        bucket: []const u8,
        prefix: []const u8,
        options: OpenOptions,
    ) !OpenedObjectStore {
        return try initGcsUriWithBearerTokenAndOptions(alloc, bucket, prefix, null, options);
    }

    pub fn initGcsUriWithBearerTokenAndOptions(
        alloc: Allocator,
        bucket: []const u8,
        prefix: []const u8,
        bearer_token: ?[]const u8,
        options: OpenOptions,
    ) !OpenedObjectStore {
        const gcs = try alloc.create(object_storage.Gcs.JsonApiClient);
        errdefer alloc.destroy(gcs);
        var cfg = if (bearer_token) |token|
            try object_storage.Gcs.jsonApiClientConfigWithBearerTokenAlloc(alloc, token, null)
        else
            try object_storage.Gcs.jsonApiClientConfigFromEnvAlloc(alloc);
        errdefer cfg.deinit(alloc);
        gcs.* = try object_storage.Gcs.JsonApiClient.init(alloc, cfg);

        var owned_client = gcs.client();
        if (options.ensure_bucket and !(try owned_client.bucketExists(bucket))) try owned_client.makeBucket(bucket);
        return .{
            .alloc = alloc,
            .client = owned_client,
            .gcs_client = gcs,
            .bucket = try alloc.dupe(u8, bucket),
            .prefix = try alloc.dupe(u8, prefix),
        };
    }

    pub fn initS3Uri(alloc: Allocator, bucket: []const u8, prefix: []const u8) !OpenedObjectStore {
        return try initS3UriWithS3AndOpenOptions(alloc, bucket, prefix, null, .{});
    }

    pub fn initS3UriWithOptions(
        alloc: Allocator,
        bucket: []const u8,
        prefix: []const u8,
        options: ?S3Options,
    ) !OpenedObjectStore {
        return try initS3UriWithS3AndOpenOptions(alloc, bucket, prefix, options, .{});
    }

    fn initExistingS3UriWithOptions(
        alloc: Allocator,
        bucket: []const u8,
        prefix: []const u8,
        options: ?S3Options,
    ) !OpenedObjectStore {
        return try initS3UriWithS3AndOpenOptions(alloc, bucket, prefix, options, .{ .ensure_bucket = false });
    }

    pub fn initS3UriWithOpenOptions(
        alloc: Allocator,
        bucket: []const u8,
        prefix: []const u8,
        options: OpenOptions,
    ) !OpenedObjectStore {
        return try initS3UriWithS3AndOpenOptions(alloc, bucket, prefix, null, options);
    }

    pub const S3Overrides = struct {
        endpoint: ?[]const u8 = null,
        use_ssl: bool = true,
        access_key_id: ?[]const u8 = null,
        secret_access_key: ?[]const u8 = null,
        session_token: ?[]const u8 = null,
        region: ?[]const u8 = null,
        addressing_style: object_storage.S3.AddressingStyle = .path,
    };

    pub fn initS3UriWithOverrides(alloc: Allocator, bucket: []const u8, prefix: []const u8, overrides: S3Overrides) !OpenedObjectStore {
        return try initS3UriWithOverridesAndOptions(alloc, bucket, prefix, overrides, .{});
    }

    pub fn initS3UriWithOverridesAndOptions(
        alloc: Allocator,
        bucket: []const u8,
        prefix: []const u8,
        overrides: S3Overrides,
        options: OpenOptions,
    ) !OpenedObjectStore {
        return try initS3UriWithS3AndOpenOptions(alloc, bucket, prefix, .{
            .endpoint = overrides.endpoint,
            .region = overrides.region,
            .access_key_id = overrides.access_key_id,
            .secret_access_key = overrides.secret_access_key,
            .session_token = overrides.session_token,
            .use_ssl = overrides.use_ssl,
            .addressing_style = overrides.addressing_style,
        }, options);
    }

    pub fn initS3UriWithS3AndOpenOptions(
        alloc: Allocator,
        bucket: []const u8,
        prefix: []const u8,
        s3_options: ?S3Options,
        open_options: OpenOptions,
    ) !OpenedObjectStore {
        const s3 = try alloc.create(object_storage.S3.Client);
        errdefer alloc.destroy(s3);
        const cfg = try s3ConfigAlloc(alloc, s3_options);
        s3.* = try object_storage.S3.Client.init(alloc, cfg);

        var owned_client = s3.client();
        if (open_options.ensure_bucket and !(try owned_client.bucketExists(bucket))) try owned_client.makeBucket(bucket);
        return .{
            .alloc = alloc,
            .client = owned_client,
            .s3_client = s3,
            .bucket = try alloc.dupe(u8, bucket),
            .prefix = try alloc.dupe(u8, prefix),
        };
    }

    pub fn initWithClient(alloc: Allocator, client: object_storage.ObjectStorage, bucket: []const u8, prefix: []const u8) !OpenedObjectStore {
        return try initWithClientOptions(alloc, client, bucket, prefix, .{});
    }

    pub fn initWithClientOptions(
        alloc: Allocator,
        client: object_storage.ObjectStorage,
        bucket: []const u8,
        prefix: []const u8,
        options: OpenOptions,
    ) !OpenedObjectStore {
        var owned_client = client;
        if (options.ensure_bucket and !(try owned_client.bucketExists(bucket))) try owned_client.makeBucket(bucket);
        return .{
            .alloc = alloc,
            .client = owned_client,
            .owns_client = false,
            .bucket = try alloc.dupe(u8, bucket),
            .prefix = try alloc.dupe(u8, prefix),
        };
    }

    pub fn initWithExistingClient(alloc: Allocator, client: object_storage.ObjectStorage, bucket: []const u8, prefix: []const u8) !OpenedObjectStore {
        const owned_bucket = try alloc.dupe(u8, bucket);
        errdefer alloc.free(owned_bucket);
        return .{
            .alloc = alloc,
            .client = client,
            .owns_client = false,
            .bucket = owned_bucket,
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
