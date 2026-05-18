// Copyright 2026 Antfly, Inc.
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

const std = @import("std");
const platform = @import("antfly_platform");
const httpx = @import("httpx");
const client_mod = @import("client.zig");
const test_support = @import("test_support.zig");
const types = @import("types.zig");
const s3 = @import("s3.zig");
const google_auth = @import("google_auth.zig");

const Allocator = std.mem.Allocator;

pub const Transport = enum {
    s3_compatible,
    json_api,
    grpc,
};

pub const Auth = union(enum) {
    none,
    bearer_token: []u8,
    google_token_source: *google_auth.CachedTokenSource,

    pub fn deinit(self: *Auth, alloc: Allocator) void {
        switch (self.*) {
            .none => {},
            .bearer_token => |value| alloc.free(value),
            .google_token_source => |source| {
                source.deinit();
                alloc.destroy(source);
            },
        }
        self.* = undefined;
    }

    fn authorizationValueAlloc(self: *Auth, alloc: Allocator) !?[]u8 {
        return switch (self.*) {
            .none => null,
            .bearer_token => |token| try std.fmt.allocPrint(alloc, "Bearer {s}", .{token}),
            .google_token_source => |source| try source.authorizationValueAlloc(alloc),
        };
    }
};

pub const JsonApiConfig = struct {
    endpoint: []u8,
    upload_endpoint: []u8,
    project_id: ?[]u8 = null,
    auth: Auth = .none,

    pub fn deinit(self: *JsonApiConfig, alloc: Allocator) void {
        alloc.free(self.endpoint);
        alloc.free(self.upload_endpoint);
        if (self.project_id) |value| alloc.free(value);
        self.auth.deinit(alloc);
        self.* = undefined;
    }
};

pub const Config = struct {
    transport: Transport = .s3_compatible,
    bucket: []u8,
    s3_config: ?s3.Config = null,
    json_api: ?JsonApiConfig = null,

    pub fn deinit(self: *Config, alloc: Allocator) void {
        alloc.free(self.bucket);
        if (self.s3_config) |*cfg| cfg.deinit(alloc);
        if (self.json_api) |*cfg| cfg.deinit(alloc);
        self.* = undefined;
    }
};

pub const HttpMethod = enum {
    GET,
    POST,
    DELETE,

    fn toHttpx(self: HttpMethod) httpx.Method {
        return switch (self) {
            .GET => .GET,
            .POST => .POST,
            .DELETE => .DELETE,
        };
    }
};

pub const HeaderPair = [2][]const u8;

pub const TransportResponse = struct {
    status: u16,
    body: []u8,
    etag: ?[]u8 = null,
    content_type: ?[]u8 = null,

    pub fn deinit(self: *TransportResponse, alloc: Allocator) void {
        alloc.free(self.body);
        if (self.etag) |value| alloc.free(value);
        if (self.content_type) |value| alloc.free(value);
        self.* = undefined;
    }
};

const RequestFn = *const fn (?*anyopaque, Allocator, HttpMethod, []const u8, []const HeaderPair, ?[]const u8, ?[]const u8) anyerror!TransportResponse;

const HttpxTransport = struct {
    alloc: Allocator,
    io_impl: std.Io.Threaded,
    client: httpx.Client,

    fn init(alloc: Allocator) HttpxTransport {
        var io_impl = std.Io.Threaded.init(alloc, .{});
        errdefer io_impl.deinit();
        return .{
            .alloc = alloc,
            .io_impl = io_impl,
            .client = httpx.Client.init(alloc, io_impl.io()),
        };
    }

    fn deinit(self: *HttpxTransport) void {
        self.client.deinit();
        self.io_impl.deinit();
        self.* = undefined;
    }

    fn request(
        ctx: ?*anyopaque,
        alloc: Allocator,
        method: HttpMethod,
        url: []const u8,
        headers: []const HeaderPair,
        body: ?[]const u8,
        content_type: ?[]const u8,
    ) !TransportResponse {
        const self: *HttpxTransport = @ptrCast(@alignCast(ctx.?));

        var request_headers = std.ArrayListUnmanaged(HeaderPair).empty;
        defer request_headers.deinit(alloc);
        try request_headers.appendSlice(alloc, headers);
        if (content_type) |value| {
            try request_headers.append(alloc, .{ "Content-Type", value });
        }

        var response = try self.client.request(method.toHttpx(), url, .{
            .headers = request_headers.items,
            .body = body,
        });
        defer response.deinit();

        return .{
            .status = response.status.code,
            .body = if (response.body) |value| try alloc.dupe(u8, value) else try alloc.alloc(u8, 0),
            .etag = if (response.headers.get("ETag")) |value| try alloc.dupe(u8, value) else null,
            .content_type = if (response.headers.get("Content-Type")) |value| try alloc.dupe(u8, value) else null,
        };
    }
};

pub const JsonApiClient = struct {
    alloc: Allocator,
    cfg: JsonApiConfig,
    request_ctx: ?*anyopaque,
    request_fn: RequestFn,
    owned_httpx: ?*HttpxTransport,

    pub fn init(alloc: Allocator, cfg: JsonApiConfig) !JsonApiClient {
        const transport = try alloc.create(HttpxTransport);
        errdefer alloc.destroy(transport);
        transport.* = HttpxTransport.init(alloc);
        return .{
            .alloc = alloc,
            .cfg = cfg,
            .request_ctx = transport,
            .request_fn = HttpxTransport.request,
            .owned_httpx = transport,
        };
    }

    pub fn initWithRequestFn(
        alloc: Allocator,
        cfg: JsonApiConfig,
        request_ctx: ?*anyopaque,
        request_fn: RequestFn,
    ) JsonApiClient {
        return .{
            .alloc = alloc,
            .cfg = cfg,
            .request_ctx = request_ctx,
            .request_fn = request_fn,
            .owned_httpx = null,
        };
    }

    pub fn deinit(self: *JsonApiClient) void {
        if (self.owned_httpx) |transport| {
            transport.deinit();
            self.alloc.destroy(transport);
        }
        self.cfg.deinit(self.alloc);
        self.* = undefined;
    }

    pub fn client(self: *JsonApiClient) client_mod.Client {
        return .{
            .allocator = self.alloc,
            .ptr = self,
            .vtable = &vtable,
        };
    }

    fn bucketExists(self: *JsonApiClient, bucket: []const u8) !bool {
        const url = try bucketMetadataUrlAlloc(self.alloc, self.cfg, bucket);
        defer self.alloc.free(url);

        var response = try self.perform(.GET, url, &.{}, null, null);
        defer response.deinit(self.alloc);
        return switch (response.status) {
            200 => true,
            404 => false,
            else => mapUnexpectedStatus(response.status),
        };
    }

    fn makeBucket(self: *JsonApiClient, bucket: []const u8) !void {
        const project_id = self.cfg.project_id orelse return error.MissingProjectId;
        const url = try bucketCreateUrlAlloc(self.alloc, self.cfg, project_id);
        defer self.alloc.free(url);

        const payload = try httpx.json.Json.stringify(self.alloc, .{ .name = bucket });
        defer self.alloc.free(payload);

        var response = try self.perform(.POST, url, &.{}, payload, "application/json");
        defer response.deinit(self.alloc);
        switch (response.status) {
            200, 201 => return,
            409 => return,
            else => return mapUnexpectedStatus(response.status),
        }
    }

    fn putObject(
        self: *JsonApiClient,
        alloc: Allocator,
        bucket: []const u8,
        key: []const u8,
        body: []const u8,
        opts: types.PutOptions,
    ) !types.PutResult {
        const url = try uploadMediaUrlAlloc(alloc, self.cfg, bucket, key, opts);
        defer alloc.free(url);

        var headers = std.ArrayListUnmanaged(HeaderPair).empty;
        defer headers.deinit(alloc);
        try appendConditionalHeaders(alloc, &headers, opts.if_match_etag, opts.if_none_match);

        var response = try self.perform(.POST, url, headers.items, body, opts.content_type orelse "application/octet-stream");
        defer response.deinit(alloc);

        switch (response.status) {
            200, 201 => {},
            304, 412 => return error.PreconditionFailed,
            404 => return error.FileNotFound,
            else => return mapUnexpectedStatus(response.status),
        }

        var metadata = try parseObjectMetadataResponse(alloc, bucket, response.body);
        defer metadata.deinit(alloc);
        return .{
            .etag = if (metadata.etag) |value| try alloc.dupe(u8, value) else null,
        };
    }

    fn getObject(
        self: *JsonApiClient,
        alloc: Allocator,
        bucket: []const u8,
        key: []const u8,
        opts: types.GetOptions,
    ) !types.GetResult {
        if (opts.part_number) |part_number| {
            if (part_number != 1) return error.InvalidPartNumber;
        }

        var meta = try self.statObject(alloc, bucket, key);
        errdefer meta.deinit(alloc);

        const url = try objectMediaUrlWithGenerationAlloc(alloc, self.cfg, bucket, key, opts.version_id);
        defer alloc.free(url);

        var headers = std.ArrayListUnmanaged(HeaderPair).empty;
        defer headers.deinit(alloc);
        try appendConditionalHeaders(alloc, &headers, opts.if_match_etag, false);
        if (opts.range) |range| {
            const value = try byteRangeHeaderAlloc(alloc, range);
            errdefer alloc.free(value);
            try headers.append(alloc, .{ "Range", value });
        }
        defer {
            for (headers.items) |pair| {
                if (std.mem.eql(u8, pair[0], "Range")) alloc.free(pair[1]);
            }
        }

        var response = try self.perform(.GET, url, headers.items, null, null);
        errdefer response.deinit(alloc);

        switch (response.status) {
            200, 206 => {},
            304, 412 => return error.PreconditionFailed,
            404 => return error.FileNotFound,
            416 => return error.InvalidRange,
            else => return mapUnexpectedStatus(response.status),
        }

        meta.content_length = @intCast(response.body.len);
        if (response.content_type) |value| {
            if (meta.content_type) |current| alloc.free(current);
            meta.content_type = try alloc.dupe(u8, value);
        }

        const body = response.body;
        response.body = &.{};
        response.deinit(alloc);

        return .{
            .body = body,
            .metadata = meta,
        };
    }

    fn getObjectAttributes(self: *JsonApiClient, alloc: Allocator, bucket: []const u8, key: []const u8) !types.ObjectAttributes {
        var meta = try self.statObject(alloc, bucket, key);
        defer meta.deinit(alloc);

        const parts = try alloc.alloc(types.ObjectPart, 1);
        errdefer alloc.free(parts);
        parts[0] = .{
            .part_number = 1,
            .size = meta.content_length,
            .etag = if (meta.etag) |value| try alloc.dupe(u8, value) else null,
        };

        return .{
            .etag = if (meta.etag) |value| try alloc.dupe(u8, value) else null,
            .version_id = if (meta.version_id) |value| try alloc.dupe(u8, value) else null,
            .content_length = meta.content_length,
            .content_type = if (meta.content_type) |value| try alloc.dupe(u8, value) else null,
            .parts = parts,
        };
    }

    fn statObject(self: *JsonApiClient, alloc: Allocator, bucket: []const u8, key: []const u8) !types.ObjectMetadata {
        const url = try objectMetadataUrlWithGenerationAlloc(alloc, self.cfg, bucket, key, null);
        defer alloc.free(url);

        var response = try self.perform(.GET, url, &.{}, null, null);
        defer response.deinit(alloc);

        switch (response.status) {
            200 => return try parseObjectMetadataResponse(alloc, bucket, response.body),
            404 => return error.FileNotFound,
            else => return mapUnexpectedStatus(response.status),
        }
    }

    fn deleteObject(self: *JsonApiClient, bucket: []const u8, key: []const u8, opts: types.DeleteOptions) !void {
        const url = try objectMetadataUrlWithGenerationAlloc(self.alloc, self.cfg, bucket, key, opts.version_id);
        defer self.alloc.free(url);

        var headers = std.ArrayListUnmanaged(HeaderPair).empty;
        defer headers.deinit(self.alloc);
        try appendConditionalHeaders(self.alloc, &headers, opts.if_match_etag, false);

        var response = try self.perform(.DELETE, url, headers.items, null, null);
        defer response.deinit(self.alloc);

        switch (response.status) {
            200, 204 => return,
            404 => return error.FileNotFound,
            304, 412 => return error.PreconditionFailed,
            else => return mapUnexpectedStatus(response.status),
        }
    }

    fn listObjects(self: *JsonApiClient, alloc: Allocator, bucket: []const u8, opts: types.ListOptions) !types.ListResult {
        const url = try objectListUrlAlloc(alloc, self.cfg, bucket, opts);
        defer alloc.free(url);

        var response = try self.perform(.GET, url, &.{}, null, null);
        defer response.deinit(alloc);

        switch (response.status) {
            200 => return try parseListResponse(alloc, response.body),
            404 => return .{
                .entries = try alloc.alloc(types.ListEntry, 0),
                .common_prefixes = try alloc.alloc([]u8, 0),
            },
            else => return mapUnexpectedStatus(response.status),
        }
    }

    fn perform(
        self: *JsonApiClient,
        method: HttpMethod,
        url: []const u8,
        headers: []const HeaderPair,
        body: ?[]const u8,
        content_type: ?[]const u8,
    ) !TransportResponse {
        var merged = std.ArrayListUnmanaged(HeaderPair).empty;
        defer merged.deinit(self.alloc);
        try merged.appendSlice(self.alloc, headers);

        const auth_value = try self.cfg.auth.authorizationValueAlloc(self.alloc);
        defer if (auth_value) |value| self.alloc.free(value);
        if (auth_value) |value| try merged.append(self.alloc, .{ "Authorization", value });

        return try self.request_fn(self.request_ctx, self.alloc, method, url, merged.items, body, content_type);
    }

    const vtable: client_mod.Client.VTable = .{
        .deinit = erasedDeinit,
        .bucket_exists = erasedBucketExists,
        .make_bucket = erasedMakeBucket,
        .put_object = erasedPutObject,
        .get_object = erasedGetObject,
        .get_object_attributes = erasedGetObjectAttributes,
        .stat_object = erasedStatObject,
        .delete_object = erasedDeleteObject,
        .list_objects = erasedListObjects,
    };

    fn erasedDeinit(_: Allocator, ptr: *anyopaque) void {
        const self: *JsonApiClient = @ptrCast(@alignCast(ptr));
        self.deinit();
    }

    fn erasedBucketExists(ptr: *anyopaque, bucket: []const u8) !bool {
        const self: *JsonApiClient = @ptrCast(@alignCast(ptr));
        return try self.bucketExists(bucket);
    }

    fn erasedMakeBucket(ptr: *anyopaque, bucket: []const u8) !void {
        const self: *JsonApiClient = @ptrCast(@alignCast(ptr));
        try self.makeBucket(bucket);
    }

    fn erasedPutObject(ptr: *anyopaque, alloc: Allocator, bucket: []const u8, key: []const u8, body: []const u8, opts: types.PutOptions) !types.PutResult {
        const self: *JsonApiClient = @ptrCast(@alignCast(ptr));
        return try self.putObject(alloc, bucket, key, body, opts);
    }

    fn erasedGetObject(ptr: *anyopaque, alloc: Allocator, bucket: []const u8, key: []const u8, opts: types.GetOptions) !types.GetResult {
        const self: *JsonApiClient = @ptrCast(@alignCast(ptr));
        return try self.getObject(alloc, bucket, key, opts);
    }

    fn erasedGetObjectAttributes(ptr: *anyopaque, alloc: Allocator, bucket: []const u8, key: []const u8) !types.ObjectAttributes {
        const self: *JsonApiClient = @ptrCast(@alignCast(ptr));
        return try self.getObjectAttributes(alloc, bucket, key);
    }

    fn erasedStatObject(ptr: *anyopaque, alloc: Allocator, bucket: []const u8, key: []const u8) !types.ObjectMetadata {
        const self: *JsonApiClient = @ptrCast(@alignCast(ptr));
        return try self.statObject(alloc, bucket, key);
    }

    fn erasedDeleteObject(ptr: *anyopaque, bucket: []const u8, key: []const u8, opts: types.DeleteOptions) !void {
        const self: *JsonApiClient = @ptrCast(@alignCast(ptr));
        try self.deleteObject(bucket, key, opts);
    }

    fn erasedListObjects(ptr: *anyopaque, alloc: Allocator, bucket: []const u8, opts: types.ListOptions) !types.ListResult {
        const self: *JsonApiClient = @ptrCast(@alignCast(ptr));
        return try self.listObjects(alloc, bucket, opts);
    }
};

pub fn s3CompatibleConfigAlloc(
    alloc: Allocator,
    bucket: []const u8,
    access_key_id: []const u8,
    secret_access_key: []const u8,
    session_token: ?[]const u8,
) !Config {
    return .{
        .transport = .s3_compatible,
        .bucket = try alloc.dupe(u8, bucket),
        .s3_config = .{
            .credentials = .{
                .endpoint = try alloc.dupe(u8, "storage.googleapis.com"),
                .use_ssl = true,
                .access_key_id = try alloc.dupe(u8, access_key_id),
                .secret_access_key = try alloc.dupe(u8, secret_access_key),
                .session_token = if (session_token) |value| try alloc.dupe(u8, value) else null,
                .region = try alloc.dupe(u8, "auto"),
            },
            .addressing_style = .path,
        },
    };
}

pub fn jsonApiConfigAlloc(alloc: Allocator, bucket: []const u8) !Config {
    return .{
        .transport = .json_api,
        .bucket = try alloc.dupe(u8, bucket),
        .json_api = .{
            .endpoint = try alloc.dupe(u8, "https://storage.googleapis.com/storage/v1"),
            .upload_endpoint = try alloc.dupe(u8, "https://storage.googleapis.com/upload/storage/v1"),
            .auth = .none,
        },
    };
}

pub fn jsonApiClientConfigAlloc(alloc: Allocator) !JsonApiConfig {
    return .{
        .endpoint = try alloc.dupe(u8, "https://storage.googleapis.com/storage/v1"),
        .upload_endpoint = try alloc.dupe(u8, "https://storage.googleapis.com/upload/storage/v1"),
        .auth = .none,
    };
}

pub fn jsonApiClientConfigWithBearerTokenAlloc(
    alloc: Allocator,
    bearer_token: []const u8,
    project_id: ?[]const u8,
) !JsonApiConfig {
    var cfg = try jsonApiClientConfigAlloc(alloc);
    errdefer cfg.deinit(alloc);
    cfg.auth = .{ .bearer_token = try alloc.dupe(u8, bearer_token) };
    if (project_id) |value| cfg.project_id = try alloc.dupe(u8, value);
    return cfg;
}

pub fn jsonApiClientConfigFromEnvAlloc(alloc: Allocator) !JsonApiConfig {
    var cfg = try jsonApiClientConfigAlloc(alloc);
    errdefer cfg.deinit(alloc);

    if ((try envOwned(alloc, "GCS_BEARER_TOKEN")) orelse (try envOwned(alloc, "GOOGLE_OAUTH_ACCESS_TOKEN"))) |token| {
        defer alloc.free(token);
        cfg.auth = .{ .bearer_token = try alloc.dupe(u8, token) };
    } else {
        const scope = (try envOwned(alloc, "GCS_OAUTH_SCOPE")) orelse try alloc.dupe(u8, google_auth.default_scope);
        defer alloc.free(scope);
        cfg.auth = .{ .google_token_source = try google_auth.tokenSourceFromEnvAlloc(alloc, scope) };
    }

    const explicit_project_id = (try envOwned(alloc, "GOOGLE_CLOUD_PROJECT")) orelse try envOwned(alloc, "GCLOUD_PROJECT");
    defer if (explicit_project_id) |value| alloc.free(value);
    if (explicit_project_id) |value| {
        cfg.project_id = try alloc.dupe(u8, value);
    } else if (switch (cfg.auth) {
        .google_token_source => |source| source.cfg.service_account.project_id,
        else => null,
    }) |value| {
        cfg.project_id = try alloc.dupe(u8, value);
    }

    if (try envOwned(alloc, "GCS_JSON_API_ENDPOINT")) |value| {
        alloc.free(cfg.endpoint);
        cfg.endpoint = value;
    }
    if (try envOwned(alloc, "GCS_JSON_API_UPLOAD_ENDPOINT")) |value| {
        alloc.free(cfg.upload_endpoint);
        cfg.upload_endpoint = value;
    }

    return cfg;
}

pub fn jsonApiConfigWithBearerTokenAlloc(
    alloc: Allocator,
    bucket: []const u8,
    bearer_token: []const u8,
    project_id: ?[]const u8,
) !Config {
    var cfg = try jsonApiConfigAlloc(alloc, bucket);
    errdefer cfg.deinit(alloc);
    cfg.json_api.?.auth = .{ .bearer_token = try alloc.dupe(u8, bearer_token) };
    if (project_id) |value| cfg.json_api.?.project_id = try alloc.dupe(u8, value);
    return cfg;
}

pub fn objectMediaUrlAlloc(alloc: Allocator, cfg: Config, key: []const u8) ![]u8 {
    const bucket = cfg.bucket;
    return switch (cfg.transport) {
        .s3_compatible => blk: {
            const s3_cfg = cfg.s3_config orelse return error.MissingS3Config;
            var shape = try s3.objectUriAlloc(alloc, s3_cfg, bucket, key);
            defer shape.deinit(alloc);
            break :blk try alloc.dupe(u8, shape.uri);
        },
        .json_api, .grpc => try objectMediaUrlWithGenerationAlloc(alloc, cfg.json_api orelse return error.MissingJsonApiConfig, bucket, key, null),
    };
}

pub fn localGrpcReferencePathAlloc(alloc: Allocator) !?[]u8 {
    const candidates = try moduleCacheCandidatesAlloc(alloc);
    defer {
        for (candidates) |candidate| alloc.free(candidate);
        alloc.free(candidates);
    }

    for (candidates) |root| {
        var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
        defer io_impl.deinit();
        const io = io_impl.io();

        var dir = std.Io.Dir.openDirAbsolute(io, root, .{ .iterate = true }) catch continue;
        defer dir.close(io);

        var walker = try dir.walk(alloc);
        defer walker.deinit();

        while (try walker.next(io)) |entry| {
            if (entry.kind != .file) continue;
            if (!std.mem.eql(u8, std.fs.path.basename(entry.path), "storage.pb.go")) continue;
            if (!std.mem.endsWith(u8, entry.path, "googleapis/storage/v2/storage.pb.go")) continue;
            return try std.fs.path.join(alloc, &.{ root, entry.path });
        }
    }

    return null;
}

pub fn localGrpcReferenceExists() bool {
    const alloc = std.heap.page_allocator;
    const path = localGrpcReferencePathAlloc(alloc) catch return false;
    if (path) |value| {
        alloc.free(value);
        return true;
    }
    return false;
}

fn bucketMetadataUrlAlloc(alloc: Allocator, cfg: JsonApiConfig, bucket: []const u8) ![]u8 {
    const escaped_bucket = try percentEncodeAlloc(alloc, bucket);
    defer alloc.free(escaped_bucket);
    return try std.fmt.allocPrint(alloc, "{s}/b/{s}", .{ cfg.endpoint, escaped_bucket });
}

fn bucketCreateUrlAlloc(alloc: Allocator, cfg: JsonApiConfig, project_id: []const u8) ![]u8 {
    const escaped_project = try percentEncodeAlloc(alloc, project_id);
    defer alloc.free(escaped_project);
    return try std.fmt.allocPrint(alloc, "{s}/b?project={s}", .{ cfg.endpoint, escaped_project });
}

fn objectMetadataUrlWithGenerationAlloc(
    alloc: Allocator,
    cfg: JsonApiConfig,
    bucket: []const u8,
    key: []const u8,
    generation: ?[]const u8,
) ![]u8 {
    const escaped_bucket = try percentEncodeAlloc(alloc, bucket);
    defer alloc.free(escaped_bucket);
    const escaped_key = try percentEncodeAlloc(alloc, key);
    defer alloc.free(escaped_key);

    if (generation) |value| {
        const escaped_generation = try percentEncodeAlloc(alloc, value);
        defer alloc.free(escaped_generation);
        return try std.fmt.allocPrint(alloc, "{s}/b/{s}/o/{s}?generation={s}", .{ cfg.endpoint, escaped_bucket, escaped_key, escaped_generation });
    }

    return try std.fmt.allocPrint(alloc, "{s}/b/{s}/o/{s}", .{ cfg.endpoint, escaped_bucket, escaped_key });
}

fn objectMediaUrlWithGenerationAlloc(
    alloc: Allocator,
    cfg: JsonApiConfig,
    bucket: []const u8,
    key: []const u8,
    generation: ?[]const u8,
) ![]u8 {
    const metadata_url = try objectMetadataUrlWithGenerationAlloc(alloc, cfg, bucket, key, generation);
    defer alloc.free(metadata_url);
    return try std.fmt.allocPrint(alloc, "{s}{s}alt=media", .{
        metadata_url,
        if (std.mem.indexOfScalar(u8, metadata_url, '?') == null) "?" else "&",
    });
}

fn uploadMediaUrlAlloc(
    alloc: Allocator,
    cfg: JsonApiConfig,
    bucket: []const u8,
    key: []const u8,
    opts: types.PutOptions,
) ![]u8 {
    const escaped_bucket = try percentEncodeAlloc(alloc, bucket);
    defer alloc.free(escaped_bucket);
    const escaped_key = try percentEncodeAlloc(alloc, key);
    defer alloc.free(escaped_key);

    var url = try std.fmt.allocPrint(alloc, "{s}/b/{s}/o?uploadType=media&name={s}", .{ cfg.upload_endpoint, escaped_bucket, escaped_key });
    errdefer alloc.free(url);

    if (opts.if_none_match) {
        const next = try std.fmt.allocPrint(alloc, "{s}&ifGenerationMatch=0", .{url});
        alloc.free(url);
        url = next;
    }

    return url;
}

fn objectListUrlAlloc(alloc: Allocator, cfg: JsonApiConfig, bucket: []const u8, opts: types.ListOptions) ![]u8 {
    const escaped_bucket = try percentEncodeAlloc(alloc, bucket);
    defer alloc.free(escaped_bucket);

    var parts = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (parts.items) |part| alloc.free(part);
        parts.deinit(alloc);
    }

    const escaped_prefix = try percentEncodeAlloc(alloc, opts.prefix);
    defer alloc.free(escaped_prefix);
    try parts.append(alloc, try std.fmt.allocPrint(alloc, "prefix={s}", .{escaped_prefix}));
    if (!opts.recursive and opts.delimiter.len > 0) {
        const escaped_delimiter = try percentEncodeAlloc(alloc, opts.delimiter);
        defer alloc.free(escaped_delimiter);
        try parts.append(alloc, try std.fmt.allocPrint(alloc, "delimiter={s}", .{escaped_delimiter}));
    }
    if (opts.start_after) |value| {
        const escaped = try percentEncodeAlloc(alloc, value);
        defer alloc.free(escaped);
        try parts.append(alloc, try std.fmt.allocPrint(alloc, "startOffset={s}", .{escaped}));
    }
    if (opts.continuation_token) |value| {
        const escaped = try percentEncodeAlloc(alloc, value);
        defer alloc.free(escaped);
        try parts.append(alloc, try std.fmt.allocPrint(alloc, "pageToken={s}", .{escaped}));
    }
    try parts.append(alloc, try std.fmt.allocPrint(alloc, "maxResults={d}", .{opts.max_keys}));

    var query = std.ArrayListUnmanaged(u8).empty;
    defer query.deinit(alloc);
    for (parts.items, 0..) |part, idx| {
        if (idx > 0) try query.append(alloc, '&');
        try query.appendSlice(alloc, part);
    }

    return try std.fmt.allocPrint(alloc, "{s}/b/{s}/o?{s}", .{ cfg.endpoint, escaped_bucket, query.items });
}

fn parseObjectMetadataResponse(alloc: Allocator, bucket: []const u8, body: []const u8) !types.ObjectMetadata {
    const Parsed = struct {
        bucket: ?[]const u8 = null,
        name: []const u8,
        etag: ?[]const u8 = null,
        generation: ?[]const u8 = null,
        size: ?[]const u8 = null,
        contentType: ?[]const u8 = null,
    };

    var parsed = try std.json.parseFromSlice(Parsed, alloc, body, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();

    return .{
        .bucket = try alloc.dupe(u8, parsed.value.bucket orelse bucket),
        .key = try alloc.dupe(u8, parsed.value.name),
        .etag = if (parsed.value.etag) |value| try alloc.dupe(u8, value) else null,
        .version_id = if (parsed.value.generation) |value| try alloc.dupe(u8, value) else null,
        .content_length = if (parsed.value.size) |value| try std.fmt.parseUnsigned(u64, value, 10) else 0,
        .content_type = if (parsed.value.contentType) |value| try alloc.dupe(u8, value) else null,
        .last_modified_unix_ms = null,
    };
}

fn parseListResponse(alloc: Allocator, body: []const u8) !types.ListResult {
    const Parsed = struct {
        items: ?[]const struct {
            name: []const u8,
            etag: ?[]const u8 = null,
            size: ?[]const u8 = null,
        } = null,
        prefixes: ?[]const []const u8 = null,
        nextPageToken: ?[]const u8 = null,
    };

    var parsed = try std.json.parseFromSlice(Parsed, alloc, body, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();

    const items = parsed.value.items orelse &.{};
    var entries = try alloc.alloc(types.ListEntry, items.len);
    errdefer {
        for (entries) |*entry| entry.deinit(alloc);
        alloc.free(entries);
    }
    for (items, 0..) |item, idx| {
        entries[idx] = .{
            .key = try alloc.dupe(u8, item.name),
            .etag = if (item.etag) |value| try alloc.dupe(u8, value) else null,
            .size = if (item.size) |value| try std.fmt.parseUnsigned(u64, value, 10) else 0,
            .last_modified_unix_ms = null,
        };
    }

    const prefixes_in = parsed.value.prefixes orelse &.{};
    var prefixes = try alloc.alloc([]u8, prefixes_in.len);
    errdefer {
        for (prefixes) |prefix| alloc.free(prefix);
        alloc.free(prefixes);
    }
    for (prefixes_in, 0..) |prefix, idx| {
        prefixes[idx] = try alloc.dupe(u8, prefix);
    }

    return .{
        .entries = entries,
        .common_prefixes = prefixes,
        .next_continuation_token = if (parsed.value.nextPageToken) |value| try alloc.dupe(u8, value) else null,
    };
}

fn appendConditionalHeaders(alloc: Allocator, headers: *std.ArrayListUnmanaged(HeaderPair), if_match_etag: ?[]const u8, if_none_match: bool) !void {
    if (if_match_etag) |value| try headers.append(alloc, .{ "If-Match", value });
    if (if_none_match) try headers.append(alloc, .{ "If-None-Match", "*" });
}

fn byteRangeHeaderAlloc(alloc: Allocator, range: types.ByteRange) ![]u8 {
    const start = range.offset;
    if (range.length) |length| {
        if (length == 0) return error.InvalidRange;
        return try std.fmt.allocPrint(alloc, "bytes={d}-{d}", .{ start, start + length - 1 });
    }
    return try std.fmt.allocPrint(alloc, "bytes={d}-", .{start});
}

fn mapUnexpectedStatus(status: u16) anyerror {
    return switch (status) {
        400 => error.InvalidRequest,
        401 => error.Unauthorized,
        403 => error.AccessDenied,
        404 => error.FileNotFound,
        409 => error.Conflict,
        412 => error.PreconditionFailed,
        429 => error.RateLimited,
        500, 502, 503, 504 => error.RemoteUnavailable,
        else => error.UnexpectedHttpStatus,
    };
}

fn percentEncodeAlloc(alloc: Allocator, value: []const u8) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    for (value) |byte| {
        if (std.ascii.isAlphanumeric(byte) or byte == '-' or byte == '_' or byte == '.' or byte == '~') {
            try out.append(alloc, byte);
            continue;
        }
        var encoded: [3]u8 = undefined;
        encoded[0] = '%';
        encoded[1] = std.fmt.digitToChar(byte >> 4, .upper);
        encoded[2] = std.fmt.digitToChar(byte & 0x0f, .upper);
        try out.appendSlice(alloc, &encoded);
    }
    return try out.toOwnedSlice(alloc);
}

fn moduleCacheCandidatesAlloc(alloc: Allocator) ![][]u8 {
    var out = std.ArrayListUnmanaged([]u8).empty;
    errdefer {
        for (out.items) |item| alloc.free(item);
        out.deinit(alloc);
    }

    const modcache = try envOwned(alloc, "GOMODCACHE");
    defer if (modcache) |value| alloc.free(value);
    if (modcache) |value| {
        try out.append(alloc, try alloc.dupe(u8, value));
    }

    const gopath = try envOwned(alloc, "GOPATH");
    defer if (gopath) |value| alloc.free(value);
    if (gopath) |value| {
        var iter = std.mem.splitScalar(u8, value, std.fs.path.delimiter);
        while (iter.next()) |entry| {
            if (entry.len == 0) continue;
            try out.append(alloc, try std.fs.path.join(alloc, &.{ entry, "pkg", "mod" }));
        }
    }

    if (out.items.len == 0) {
        const home = try envOwned(alloc, "HOME");
        defer if (home) |value| alloc.free(value);
        if (home) |value| {
            try out.append(alloc, try std.fs.path.join(alloc, &.{ value, "go", "pkg", "mod" }));
        }
    }

    return try out.toOwnedSlice(alloc);
}

fn envOwned(alloc: Allocator, comptime name: []const u8) !?[]u8 {
    const value = platform.env.getenv(name ++ "\x00") orelse return null;
    return try alloc.dupe(u8, value);
}

test "gcs s3-compatible config defaults to storage endpoint" {
    const alloc = std.testing.allocator;
    var cfg = try s3CompatibleConfigAlloc(alloc, "bucket", "key", "secret", null);
    defer cfg.deinit(alloc);
    try std.testing.expectEqual(Transport.s3_compatible, cfg.transport);
    try std.testing.expectEqualStrings("storage.googleapis.com", cfg.s3_config.?.credentials.endpoint);
    try std.testing.expectEqual(s3.AddressingStyle.path, cfg.s3_config.?.addressing_style);
}

test "gcs json api media url encodes object path as one segment" {
    const alloc = std.testing.allocator;
    var cfg = try jsonApiConfigAlloc(alloc, "bucket");
    defer cfg.deinit(alloc);
    const url = try objectMediaUrlAlloc(alloc, cfg, "a b/c.txt");
    defer alloc.free(url);
    try std.testing.expectEqualStrings("https://storage.googleapis.com/storage/v1/b/bucket/o/a%20b%2Fc.txt?alt=media", url);
}

test "gcs local grpc reference path can be discovered when present" {
    const alloc = std.testing.allocator;
    const path = try localGrpcReferencePathAlloc(alloc);
    defer if (path) |value| alloc.free(value);
    if (path) |value| {
        try std.testing.expect(std.mem.endsWith(u8, value, "googleapis/storage/v2/storage.pb.go"));
    }
}

test "json api client get object uses metadata then media with auth and range" {
    const alloc = std.testing.allocator;
    const State = struct {
        calls: usize = 0,

        fn request(
            ptr: ?*anyopaque,
            request_alloc: Allocator,
            method: HttpMethod,
            url: []const u8,
            headers: []const HeaderPair,
            body: ?[]const u8,
            content_type: ?[]const u8,
        ) !TransportResponse {
            _ = body;
            _ = content_type;
            const self: *@This() = @ptrCast(@alignCast(ptr.?));
            defer self.calls += 1;

            switch (self.calls) {
                0 => {
                    try std.testing.expectEqual(HttpMethod.GET, method);
                    try std.testing.expectEqualStrings("https://storage.googleapis.com/storage/v1/b/bucket/o/folder%2Fdoc.txt", url);
                    try expectHeader(headers, "Authorization", "Bearer token-123");
                    return .{
                        .status = 200,
                        .body = try request_alloc.dupe(u8, "{\"bucket\":\"bucket\",\"name\":\"folder/doc.txt\",\"etag\":\"etag-1\",\"generation\":\"42\",\"size\":\"11\",\"contentType\":\"text/plain\"}"),
                    };
                },
                1 => {
                    try std.testing.expectEqual(HttpMethod.GET, method);
                    try std.testing.expectEqualStrings("https://storage.googleapis.com/storage/v1/b/bucket/o/folder%2Fdoc.txt?generation=42&alt=media", url);
                    try expectHeader(headers, "Authorization", "Bearer token-123");
                    try expectHeader(headers, "If-Match", "etag-1");
                    try expectHeader(headers, "Range", "bytes=2-5");
                    return .{
                        .status = 206,
                        .body = try request_alloc.dupe(u8, "cdef"),
                        .content_type = try request_alloc.dupe(u8, "text/plain"),
                    };
                },
                else => return error.UnexpectedCall,
            }
        }
    };

    const cfg = try jsonApiClientConfigWithBearerTokenAlloc(alloc, "token-123", null);
    var state = State{};
    var json_client = JsonApiClient.initWithRequestFn(alloc, cfg, &state, State.request);

    var client = json_client.client();
    defer client.deinit();

    var result = try client.getObject("bucket", "folder/doc.txt", .{
        .version_id = "42",
        .range = .{ .offset = 2, .length = 4 },
        .if_match_etag = "etag-1",
    });
    defer result.deinit(alloc);

    try std.testing.expectEqualStrings("cdef", result.body);
    try std.testing.expectEqualStrings("bucket", result.metadata.bucket);
    try std.testing.expectEqualStrings("folder/doc.txt", result.metadata.key);
    try std.testing.expectEqualStrings("etag-1", result.metadata.etag.?);
    try std.testing.expectEqualStrings("42", result.metadata.version_id.?);
    try std.testing.expectEqualStrings("text/plain", result.metadata.content_type.?);
}

test "json api client put object encodes upload url and returns etag" {
    const alloc = std.testing.allocator;
    const State = struct {
        fn request(
            _: ?*anyopaque,
            request_alloc: Allocator,
            method: HttpMethod,
            url: []const u8,
            headers: []const HeaderPair,
            body: ?[]const u8,
            content_type: ?[]const u8,
        ) !TransportResponse {
            try std.testing.expectEqual(HttpMethod.POST, method);
            try std.testing.expectEqualStrings("https://storage.googleapis.com/upload/storage/v1/b/bucket/o?uploadType=media&name=folder%2Fdoc.txt&ifGenerationMatch=0", url);
            try expectHeader(headers, "If-None-Match", "*");
            try std.testing.expectEqualStrings("hello", body.?);
            try std.testing.expectEqualStrings("text/plain", content_type.?);
            return .{
                .status = 200,
                .body = try request_alloc.dupe(u8, "{\"bucket\":\"bucket\",\"name\":\"folder/doc.txt\",\"etag\":\"etag-2\",\"size\":\"5\",\"contentType\":\"text/plain\"}"),
            };
        }
    };

    const cfg = try jsonApiClientConfigAlloc(alloc);
    var json_client = JsonApiClient.initWithRequestFn(alloc, cfg, null, State.request);

    var client = json_client.client();
    defer client.deinit();

    var put = try client.putObject("bucket", "folder/doc.txt", "hello", .{
        .content_type = "text/plain",
        .if_none_match = true,
    });
    defer put.deinit(alloc);

    try std.testing.expectEqualStrings("etag-2", put.etag.?);
}

test "json api client lists objects and prefixes" {
    const alloc = std.testing.allocator;
    const State = struct {
        fn request(
            _: ?*anyopaque,
            request_alloc: Allocator,
            method: HttpMethod,
            url: []const u8,
            headers: []const HeaderPair,
            body: ?[]const u8,
            content_type: ?[]const u8,
        ) !TransportResponse {
            _ = headers;
            _ = body;
            _ = content_type;
            try std.testing.expectEqual(HttpMethod.GET, method);
            try std.testing.expectEqualStrings("https://storage.googleapis.com/storage/v1/b/bucket/o?prefix=docs%2F&delimiter=%2F&startOffset=docs%2Fa&maxResults=10", url);
            return .{
                .status = 200,
                .body = try request_alloc.dupe(u8,
                    \\{"items":[{"name":"docs/a.txt","etag":"e1","size":"5"}],"prefixes":["docs/sub/"],"nextPageToken":"next-1"}
                ),
            };
        }
    };

    const cfg = try jsonApiClientConfigAlloc(alloc);
    var json_client = JsonApiClient.initWithRequestFn(alloc, cfg, null, State.request);

    var client = json_client.client();
    defer client.deinit();

    var listed = try client.listObjects("bucket", .{
        .prefix = "docs/",
        .recursive = false,
        .start_after = "docs/a",
        .max_keys = 10,
    });
    defer listed.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), listed.entries.len);
    try std.testing.expectEqual(@as(usize, 1), listed.common_prefixes.len);
    try std.testing.expectEqualStrings("docs/a.txt", listed.entries[0].key);
    try std.testing.expectEqualStrings("docs/sub/", listed.common_prefixes[0]);
    try std.testing.expectEqualStrings("next-1", listed.next_continuation_token.?);
}

test "json api client make bucket requires project id" {
    const alloc = std.testing.allocator;
    const cfg = try jsonApiClientConfigAlloc(alloc);
    var json_client = JsonApiClient.initWithRequestFn(alloc, cfg, null, struct {
        fn request(
            _: ?*anyopaque,
            _: Allocator,
            _: HttpMethod,
            _: []const u8,
            _: []const HeaderPair,
            _: ?[]const u8,
            _: ?[]const u8,
        ) !TransportResponse {
            return error.Unreachable;
        }
    }.request);
    defer json_client.deinit();

    try std.testing.expectError(error.MissingProjectId, json_client.makeBucket("bucket"));
}

test "gcs unexpected status mapping covers auth and availability failures" {
    try std.testing.expectEqual(error.Unauthorized, mapUnexpectedStatus(401));
    try std.testing.expectEqual(error.AccessDenied, mapUnexpectedStatus(403));
    try std.testing.expectEqual(error.RemoteUnavailable, mapUnexpectedStatus(503));
}

test "json api client round-trips against env-configured endpoint" {
    const alloc = std.testing.allocator;
    try test_support.requireIntegrationEnabled("OBJECTSTORE_GCS_INTEGRATION");

    const bucket = try test_support.requiredOwned(alloc, "OBJECTSTORE_GCS_TEST_BUCKET");
    defer alloc.free(bucket);

    const cfg = jsonApiClientConfigFromEnvAlloc(alloc) catch return error.SkipZigTest;
    var json_client = try JsonApiClient.init(alloc, cfg);
    var client = json_client.client();
    defer client.deinit();

    if (!(try client.bucketExists(bucket))) {
        client.makeBucket(bucket) catch return error.SkipZigTest;
    }

    const key = try std.fmt.allocPrint(alloc, "zig-objectstore-gcs/{d}.txt", .{test_support.integrationNonce()});
    defer alloc.free(key);

    var put = try client.putObject(bucket, key, "hello-gcs", .{
        .content_type = "text/plain",
    });
    defer put.deinit(alloc);

    var meta = try client.statObject(bucket, key);
    defer meta.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 9), meta.content_length);
    try std.testing.expectEqualStrings("text/plain", meta.content_type.?);

    var get = try client.getObject(bucket, key, .{});
    defer get.deinit(alloc);
    try std.testing.expectEqualStrings("hello-gcs", get.body);

    var listed = try client.listObjects(bucket, .{
        .prefix = "zig-objectstore-gcs/",
    });
    defer listed.deinit(alloc);
    var found = false;
    for (listed.entries) |entry| {
        if (std.mem.eql(u8, entry.key, key)) {
            found = true;
            break;
        }
    }
    try std.testing.expect(found);

    try client.deleteObject(bucket, key, .{});
}

fn expectHeader(headers: []const HeaderPair, name: []const u8, value: []const u8) !void {
    for (headers) |header| {
        if (std.ascii.eqlIgnoreCase(header[0], name)) {
            try std.testing.expectEqualStrings(value, header[1]);
            return;
        }
    }
    return error.MissingHeader;
}
