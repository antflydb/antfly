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
const google_auth = @import("antfly_google").auth;

const Allocator = std.mem.Allocator;
const resumable_upload_threshold: u64 = 64 * 1024 * 1024;
const resumable_upload_min_chunk_bytes: u64 = 16 * 1024 * 1024;
const resumable_upload_max_chunk_bytes: u64 = 512 * 1024 * 1024;
const resumable_upload_chunk_alignment: u64 = 256 * 1024;
const resumable_upload_target_chunks: u64 = 10_000;

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
    /// Optional end-to-end HTTP deadline. Object transfers leave this unset;
    /// control-plane probes set a bounded deadline explicitly.
    request_timeout_ms: ?u64 = null,
    /// Optional borrowed application runtime. When absent, the client owns a
    /// threaded fallback for standalone library use.
    io: ?std.Io = null,

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
    PUT,
    DELETE,

    fn toHttpx(self: HttpMethod) httpx.Method {
        return switch (self) {
            .GET => .GET,
            .POST => .POST,
            .PUT => .PUT,
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
    location: ?[]u8 = null,

    pub fn deinit(self: *TransportResponse, alloc: Allocator) void {
        alloc.free(self.body);
        if (self.etag) |value| alloc.free(value);
        if (self.content_type) |value| alloc.free(value);
        if (self.location) |value| alloc.free(value);
        self.* = undefined;
    }
};

const RequestFn = *const fn (?*anyopaque, Allocator, HttpMethod, []const u8, []const HeaderPair, ?[]const u8, ?[]const u8, ?usize, ?types.CancellationToken) anyerror!TransportResponse;

const HttpxTransport = struct {
    alloc: Allocator,
    io_impl: ?*std.Io.Threaded,
    client: httpx.Client,

    fn init(alloc: Allocator, request_timeout_ms: ?u64, shared_io: ?std.Io) !HttpxTransport {
        const io_impl: ?*std.Io.Threaded = if (shared_io == null) blk: {
            const owned = try alloc.create(std.Io.Threaded);
            owned.* = std.Io.Threaded.init(alloc, .{});
            break :blk owned;
        } else null;
        errdefer if (io_impl) |owned| {
            owned.deinit();
            alloc.destroy(owned);
        };
        return .{
            .alloc = alloc,
            .io_impl = io_impl,
            .client = if (request_timeout_ms) |timeout_ms|
                httpx.Client.initWithConfig(alloc, shared_io orelse io_impl.?.io(), .{ .timeouts = .{
                    .connect_ms = timeout_ms,
                    .read_ms = timeout_ms,
                    .write_ms = timeout_ms,
                    .request_ms = timeout_ms,
                } })
            else
                httpx.Client.init(alloc, shared_io orelse io_impl.?.io()),
        };
    }

    fn deinit(self: *HttpxTransport) void {
        self.client.deinit();
        if (self.io_impl) |io_impl| {
            io_impl.deinit();
            self.alloc.destroy(io_impl);
        }
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
        max_response_size: ?usize,
        cancellation: ?types.CancellationToken,
    ) !TransportResponse {
        const self: *HttpxTransport = @ptrCast(@alignCast(ctx.?));

        var request_headers = std.ArrayListUnmanaged(HeaderPair).empty;
        defer request_headers.deinit(alloc);
        try request_headers.appendSlice(alloc, headers);
        if (content_type) |value| {
            try request_headers.append(alloc, .{ "Content-Type", value });
        }

        var response = self.client.request(method.toHttpx(), url, .{
            .headers = request_headers.items,
            .body = body,
            .max_response_size = max_response_size,
            .cancellation = if (cancellation) |token|
                httpx.CancellationToken.fromCallback(token.ptr, token.is_cancelled_fn)
            else
                null,
        }) catch |err| return if (err == error.Cancelled) error.Canceled else err;
        defer response.deinit();

        return try transportResponseAlloc(
            alloc,
            response.status.code,
            response.body orelse "",
            response.headers.get("ETag"),
            response.headers.get("Content-Type"),
            response.headers.get("Location"),
        );
    }
};

fn transportResponseAlloc(
    alloc: Allocator,
    status: u16,
    body: []const u8,
    etag: ?[]const u8,
    content_type: ?[]const u8,
    location: ?[]const u8,
) !TransportResponse {
    var result = TransportResponse{
        .status = status,
        .body = try alloc.dupe(u8, body),
    };
    errdefer result.deinit(alloc);
    if (etag) |value| result.etag = try alloc.dupe(u8, value);
    if (content_type) |value| result.content_type = try alloc.dupe(u8, value);
    if (location) |value| result.location = try alloc.dupe(u8, value);
    return result;
}

test "gcs http transport borrows a shared io runtime" {
    const alloc = std.testing.allocator;
    var shared = std.Io.Threaded.init(alloc, .{});
    defer shared.deinit();
    var transport = try HttpxTransport.init(alloc, null, shared.io());
    defer transport.deinit();
    try std.testing.expect(transport.io_impl == null);

    var fallback = try HttpxTransport.init(alloc, null, null);
    defer fallback.deinit();
    try std.testing.expect(fallback.io_impl != null);
    try std.testing.expect(fallback.client.io.userdata == @as(?*anyopaque, @ptrCast(fallback.io_impl.?)));
}

pub const JsonApiClient = struct {
    alloc: Allocator,
    cfg: JsonApiConfig,
    request_ctx: ?*anyopaque,
    request_fn: RequestFn,
    owned_httpx: ?*HttpxTransport,

    pub fn init(alloc: Allocator, cfg: JsonApiConfig) !JsonApiClient {
        const transport = try alloc.create(HttpxTransport);
        errdefer alloc.destroy(transport);
        transport.* = try HttpxTransport.init(alloc, cfg.request_timeout_ms, cfg.io);
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

    fn putFile(
        self: *JsonApiClient,
        alloc: Allocator,
        io: std.Io,
        bucket: []const u8,
        key: []const u8,
        src_path: []const u8,
        opts: types.PutOptions,
    ) !types.PutResult {
        return try self.putFileWithThreshold(alloc, io, bucket, key, src_path, opts, resumable_upload_threshold);
    }

    fn putFileWithThreshold(
        self: *JsonApiClient,
        alloc: Allocator,
        io: std.Io,
        bucket: []const u8,
        key: []const u8,
        src_path: []const u8,
        opts: types.PutOptions,
        resumable_threshold: u64,
    ) !types.PutResult {
        const source = try openFilePath(io, src_path);
        defer source.close(io);
        const stat = try source.stat(io);
        if (stat.size <= resumable_threshold) {
            const body = try alloc.alloc(u8, @intCast(stat.size));
            defer alloc.free(body);
            if (try source.readPositionalAll(io, body, 0) != body.len) return error.SourceFileChanged;
            var extra: [1]u8 = undefined;
            if (try source.readPositionalAll(io, &extra, stat.size) != 0) return error.SourceFileChanged;
            const current_stat = try source.stat(io);
            if (current_stat.size != stat.size or !std.meta.eql(current_stat.mtime, stat.mtime)) return error.SourceFileChanged;
            return try self.putObject(alloc, bucket, key, body, opts);
        }
        if (opts.if_match_etag != null) return error.ConditionalResumableUploadUnsupported;
        const required_chunk_bytes = ((stat.size - 1) / resumable_upload_target_chunks) + 1;
        const aligned_chunk_bytes = std.mem.alignForward(u64, required_chunk_bytes, resumable_upload_chunk_alignment);
        const chunk_bytes = @max(resumable_upload_min_chunk_bytes, aligned_chunk_bytes);
        if (chunk_bytes > resumable_upload_max_chunk_bytes) return error.ObjectTooLarge;

        const initiate_url = try uploadResumableUrlAlloc(alloc, self.cfg, bucket, key, opts);
        defer alloc.free(initiate_url);
        const size_text = try std.fmt.allocPrint(alloc, "{d}", .{stat.size});
        defer alloc.free(size_text);
        const upload_type = opts.content_type orelse "application/octet-stream";
        const initiate_headers = [_]HeaderPair{
            .{ "X-Upload-Content-Length", size_text },
            .{ "X-Upload-Content-Type", upload_type },
        };
        var initiated = try self.perform(.POST, initiate_url, &initiate_headers, "{}", "application/json");
        defer initiated.deinit(alloc);
        if (initiated.status != 200 and initiated.status != 201) return mapUnexpectedStatus(initiated.status);
        const session_url = initiated.location orelse return error.MissingResumableUploadLocation;

        var completed = false;
        defer if (!completed) self.cancelResumableUpload(session_url) catch {};
        const buffer = try alloc.alloc(u8, @intCast(chunk_bytes));
        defer alloc.free(buffer);
        var offset: u64 = 0;
        var final_response: ?TransportResponse = null;
        defer if (final_response) |*response| response.deinit(alloc);
        while (offset < stat.size) {
            const wanted: usize = @intCast(@min(stat.size - offset, buffer.len));
            if (try source.readPositionalAll(io, buffer[0..wanted], offset) != wanted) return error.SourceFileChanged;
            const current_stat = try source.stat(io);
            if (current_stat.size != stat.size or !std.meta.eql(current_stat.mtime, stat.mtime)) return error.SourceFileChanged;
            const last = offset + wanted - 1;
            const content_range = try std.fmt.allocPrint(alloc, "bytes {d}-{d}/{d}", .{ offset, last, stat.size });
            defer alloc.free(content_range);
            const headers = [_]HeaderPair{.{ "Content-Range", content_range }};
            var response = try self.perform(.PUT, session_url, &headers, buffer[0..wanted], upload_type);
            if (offset + wanted < stat.size) {
                defer response.deinit(alloc);
                if (response.status != 308) return mapUnexpectedStatus(response.status);
            } else {
                if (response.status != 200 and response.status != 201) {
                    defer response.deinit(alloc);
                    return mapUnexpectedStatus(response.status);
                }
                final_response = response;
            }
            offset += wanted;
        }
        var extra: [1]u8 = undefined;
        if (try source.readPositionalAll(io, &extra, offset) != 0) return error.SourceFileChanged;
        const response = &final_response.?;
        var metadata = try parseObjectMetadataResponse(alloc, bucket, response.body);
        defer metadata.deinit(alloc);
        completed = true;
        return .{ .etag = if (metadata.etag) |value| try alloc.dupe(u8, value) else null };
    }

    fn cancelResumableUpload(self: *JsonApiClient, session_url: []const u8) !void {
        var response = try self.perform(.DELETE, session_url, &.{}, null, null);
        defer response.deinit(self.alloc);
        if (response.status != 200 and response.status != 204 and response.status != 404 and response.status != 499)
            return mapUnexpectedStatus(response.status);
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

        var meta = if (opts.skip_metadata_probe) blk: {
            const owned_bucket = try alloc.dupe(u8, bucket);
            errdefer alloc.free(owned_bucket);
            const owned_key = try alloc.dupe(u8, key);
            break :blk types.ObjectMetadata{
                .bucket = owned_bucket,
                .key = owned_key,
                .content_length = 0,
            };
        } else try self.statObjectVersion(alloc, bucket, key, opts.version_id, opts.cancellation);
        errdefer meta.deinit(alloc);

        const effective_generation: ?[]const u8 = if (opts.version_id) |value|
            value
        else if (!opts.skip_metadata_probe)
            meta.version_id
        else
            null;
        const url = try objectMediaUrlWithGenerationAlloc(alloc, self.cfg, bucket, key, effective_generation);
        defer alloc.free(url);

        var headers = std.ArrayListUnmanaged(HeaderPair).empty;
        defer headers.deinit(alloc);
        const effective_if_match: ?[]const u8 = if (opts.if_match_etag) |value|
            value
        else if (!opts.skip_metadata_probe and effective_generation == null)
            meta.etag
        else
            null;
        try appendConditionalHeaders(alloc, &headers, effective_if_match, false);
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

        var response = try self.performWithResponseLimitAndCancellation(
            .GET,
            url,
            headers.items,
            null,
            null,
            opts.max_response_bytes,
            opts.cancellation,
        );
        errdefer response.deinit(alloc);

        switch (response.status) {
            200, 206 => {},
            304, 412 => return error.PreconditionFailed,
            404 => return error.FileNotFound,
            416 => return error.InvalidRange,
            else => return mapUnexpectedStatus(response.status),
        }

        meta.content_length = @intCast(response.body.len);
        if (opts.skip_metadata_probe) {
            if (response.etag) |value| meta.etag = try alloc.dupe(u8, value);
        }
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
        return self.statObjectVersion(alloc, bucket, key, null, null);
    }

    fn statObjectWithOptions(self: *JsonApiClient, alloc: Allocator, bucket: []const u8, key: []const u8, opts: types.StatOptions) !types.ObjectMetadata {
        return self.statObjectVersion(alloc, bucket, key, null, opts.cancellation);
    }

    fn statObjectVersion(
        self: *JsonApiClient,
        alloc: Allocator,
        bucket: []const u8,
        key: []const u8,
        generation: ?[]const u8,
        cancellation: ?types.CancellationToken,
    ) !types.ObjectMetadata {
        const url = try objectMetadataUrlWithGenerationAlloc(alloc, self.cfg, bucket, key, generation);
        defer alloc.free(url);

        var response = try self.performWithResponseLimitAndCancellation(.GET, url, &.{}, null, null, null, cancellation);
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
            200 => {
                var result = try parseListResponse(alloc, response.body);
                errdefer result.deinit(alloc);
                // GCS JSON API's `startOffset` is inclusive, while the shared
                // object-store contract deliberately models S3's exclusive
                // `start-after`. Normalize the first GCS page here so callers
                // cannot repeat a cursor forever. Page tokens are already
                // exclusive and must not be filtered against start_after.
                if (opts.start_after != null and opts.continuation_token == null) {
                    try enforceExclusiveStartAfter(alloc, &result, opts.start_after.?);
                }
                return result;
            },
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
        return try self.performWithResponseLimit(method, url, headers, body, content_type, null);
    }

    fn performWithResponseLimit(
        self: *JsonApiClient,
        method: HttpMethod,
        url: []const u8,
        headers: []const HeaderPair,
        body: ?[]const u8,
        content_type: ?[]const u8,
        max_response_size: ?usize,
    ) !TransportResponse {
        return try self.performWithResponseLimitAndCancellation(method, url, headers, body, content_type, max_response_size, null);
    }

    fn performWithResponseLimitAndCancellation(
        self: *JsonApiClient,
        method: HttpMethod,
        url: []const u8,
        headers: []const HeaderPair,
        body: ?[]const u8,
        content_type: ?[]const u8,
        max_response_size: ?usize,
        cancellation: ?types.CancellationToken,
    ) !TransportResponse {
        if (cancellation) |token| try token.check();
        var merged = std.ArrayListUnmanaged(HeaderPair).empty;
        defer merged.deinit(self.alloc);
        try merged.appendSlice(self.alloc, headers);

        const auth_value = try self.cfg.auth.authorizationValueAlloc(self.alloc);
        defer if (auth_value) |value| self.alloc.free(value);
        if (auth_value) |value| try merged.append(self.alloc, .{ "Authorization", value });

        return try self.request_fn(
            self.request_ctx,
            self.alloc,
            method,
            url,
            merged.items,
            body,
            content_type,
            max_response_size,
            cancellation,
        );
    }

    const vtable: client_mod.Client.VTable = .{
        .deinit = erasedDeinit,
        .bucket_exists = erasedBucketExists,
        .make_bucket = erasedMakeBucket,
        .put_object = erasedPutObject,
        .put_file = erasedPutFile,
        .get_object = erasedGetObject,
        .get_object_attributes = erasedGetObjectAttributes,
        .stat_object = erasedStatObject,
        .stat_object_with_options = erasedStatObjectWithOptions,
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

    fn erasedPutFile(ptr: *anyopaque, alloc: Allocator, io: std.Io, bucket: []const u8, key: []const u8, src_path: []const u8, opts: types.PutOptions) !types.PutResult {
        const self: *JsonApiClient = @ptrCast(@alignCast(ptr));
        return try self.putFile(alloc, io, bucket, key, src_path, opts);
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

    fn erasedStatObjectWithOptions(ptr: *anyopaque, alloc: Allocator, bucket: []const u8, key: []const u8, opts: types.StatOptions) !types.ObjectMetadata {
        const self: *JsonApiClient = @ptrCast(@alignCast(ptr));
        return try self.statObjectWithOptions(alloc, bucket, key, opts);
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

fn enforceExclusiveStartAfter(alloc: Allocator, result: *types.ListResult, start_after: []const u8) !void {
    var entry_count: usize = 0;
    for (result.entries) |entry| {
        if (std.mem.order(u8, entry.key, start_after) == .gt) entry_count += 1;
    }
    var prefix_count: usize = 0;
    for (result.common_prefixes) |prefix| {
        if (std.mem.order(u8, prefix, start_after) == .gt) prefix_count += 1;
    }

    const entries = try alloc.alloc(types.ListEntry, entry_count);
    errdefer alloc.free(entries);
    const prefixes = try alloc.alloc([]u8, prefix_count);
    errdefer alloc.free(prefixes);

    var entry_index: usize = 0;
    for (result.entries) |*entry| {
        if (std.mem.order(u8, entry.key, start_after) == .gt) {
            entries[entry_index] = entry.*;
            entry_index += 1;
        } else {
            entry.deinit(alloc);
        }
    }
    alloc.free(result.entries);
    result.entries = entries;

    var prefix_index: usize = 0;
    for (result.common_prefixes) |prefix| {
        if (std.mem.order(u8, prefix, start_after) == .gt) {
            prefixes[prefix_index] = prefix;
            prefix_index += 1;
        } else {
            alloc.free(prefix);
        }
    }
    alloc.free(result.common_prefixes);
    result.common_prefixes = prefixes;
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

fn uploadResumableUrlAlloc(
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
    var url = try std.fmt.allocPrint(alloc, "{s}/b/{s}/o?uploadType=resumable&name={s}", .{ cfg.upload_endpoint, escaped_bucket, escaped_key });
    errdefer alloc.free(url);
    if (opts.if_none_match) {
        const next = try std.fmt.allocPrint(alloc, "{s}&ifGenerationMatch=0", .{url});
        alloc.free(url);
        url = next;
    }
    return url;
}

fn openFilePath(io: std.Io, path: []const u8) !std.Io.File {
    return if (std.fs.path.isAbsolute(path))
        try std.Io.Dir.openFileAbsolute(io, path, .{})
    else
        try std.Io.Dir.cwd().openFile(io, path, .{});
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
        md5Hash: ?[]const u8 = null,
        crc32c: ?[]const u8 = null,
    };

    var parsed = try std.json.parseFromSlice(Parsed, alloc, body, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();

    const content_length = if (parsed.value.size) |value| try std.fmt.parseUnsigned(u64, value, 10) else 0;
    const owned_bucket = try alloc.dupe(u8, parsed.value.bucket orelse bucket);
    errdefer alloc.free(owned_bucket);
    const key = try alloc.dupe(u8, parsed.value.name);
    errdefer alloc.free(key);
    const etag = if (parsed.value.etag) |value| try alloc.dupe(u8, value) else null;
    errdefer if (etag) |value| alloc.free(value);
    const version_id = if (parsed.value.generation) |value| try alloc.dupe(u8, value) else null;
    errdefer if (version_id) |value| alloc.free(value);
    const content_type = if (parsed.value.contentType) |value| try alloc.dupe(u8, value) else null;
    errdefer if (content_type) |value| alloc.free(value);
    var checksum: ?types.ObjectChecksum = if (parsed.value.md5Hash) |value| .{
        .algorithm = .md5_base64,
        .value = try alloc.dupe(u8, value),
    } else if (parsed.value.crc32c) |value| .{
        .algorithm = .crc32c_base64,
        .value = try alloc.dupe(u8, value),
    } else null;
    errdefer if (checksum) |*value| value.deinit(alloc);
    return .{
        .bucket = owned_bucket,
        .key = key,
        .etag = etag,
        .version_id = version_id,
        .checksum = checksum,
        .content_length = content_length,
        .content_type = content_type,
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
    const entries = try alloc.alloc(types.ListEntry, items.len);
    var entries_initialized: usize = 0;
    errdefer {
        for (entries[0..entries_initialized]) |*entry| entry.deinit(alloc);
        alloc.free(entries);
    }
    for (items, 0..) |item, idx| {
        const size = if (item.size) |value| try std.fmt.parseUnsigned(u64, value, 10) else 0;
        const key = try alloc.dupe(u8, item.name);
        errdefer alloc.free(key);
        const etag = if (item.etag) |value| try alloc.dupe(u8, value) else null;
        entries[idx] = .{
            .key = key,
            .etag = etag,
            .size = size,
            .last_modified_unix_ms = null,
        };
        entries_initialized += 1;
    }

    const prefixes_in = parsed.value.prefixes orelse &.{};
    const prefixes = try alloc.alloc([]u8, prefixes_in.len);
    var prefixes_initialized: usize = 0;
    errdefer {
        for (prefixes[0..prefixes_initialized]) |prefix| alloc.free(prefix);
        alloc.free(prefixes);
    }
    for (prefixes_in, 0..) |prefix, idx| {
        prefixes[idx] = try alloc.dupe(u8, prefix);
        prefixes_initialized += 1;
    }

    const next_token = if (parsed.value.nextPageToken) |value| try alloc.dupe(u8, value) else null;
    return .{
        .entries = entries,
        .common_prefixes = prefixes,
        .next_continuation_token = next_token,
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

test "gcs resumable upload url preserves create-only semantics" {
    const alloc = std.testing.allocator;
    var cfg = try jsonApiConfigAlloc(alloc, "bucket");
    defer cfg.deinit(alloc);
    const url = try uploadResumableUrlAlloc(alloc, cfg.json_api.?, "bucket", "folder/doc.txt", .{ .if_none_match = true });
    defer alloc.free(url);
    try std.testing.expectEqualStrings(
        "https://storage.googleapis.com/upload/storage/v1/b/bucket/o?uploadType=resumable&name=folder%2Fdoc.txt&ifGenerationMatch=0",
        url,
    );
}

test "gcs response parsers clean up every allocation failure" {
    const Runner = struct {
        fn run(alloc: Allocator) !void {
            var metadata = try parseObjectMetadataResponse(
                alloc,
                "fallback",
                "{\"bucket\":\"bucket\",\"name\":\"backup/a\",\"etag\":\"etag\",\"generation\":\"7\",\"size\":\"42\",\"contentType\":\"application/octet-stream\"}",
            );
            defer metadata.deinit(alloc);
            var listed = try parseListResponse(
                alloc,
                "{\"items\":[{\"name\":\"backup/a\",\"etag\":\"a\",\"size\":\"1\"},{\"name\":\"backup/b\",\"etag\":\"b\",\"size\":\"2\"}],\"prefixes\":[\"backup/nested/\"],\"nextPageToken\":\"next\"}",
            );
            defer listed.deinit(alloc);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Runner.run, .{});
}

test "gcs transport response construction cleans up every allocation failure" {
    const Runner = struct {
        fn run(alloc: Allocator) !void {
            var response = try transportResponseAlloc(
                alloc,
                200,
                "payload",
                "etag",
                "application/octet-stream",
                "https://storage.googleapis.com/session",
            );
            defer response.deinit(alloc);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Runner.run, .{});
}

test "gcs file upload completes a resumable lifecycle with bounded chunks" {
    const alloc = std.testing.allocator;
    const io = std.testing.io;
    const source_path = try std.fmt.allocPrint(alloc, "/tmp/antfly-gcs-resumable-{d}", .{test_support.integrationNonce()});
    defer alloc.free(source_path);
    defer std.Io.Dir.deleteFileAbsolute(io, source_path) catch {};
    {
        var source = try std.Io.Dir.createFileAbsolute(io, source_path, .{ .truncate = true });
        defer source.close(io);
        try source.writePositionalAll(io, "resumable-payload", 0);
        try source.sync(io);
    }

    const State = struct {
        calls: usize = 0,

        fn request(ctx: ?*anyopaque, request_alloc: Allocator, method: HttpMethod, url: []const u8, _: []const HeaderPair, body: ?[]const u8, _: ?[]const u8, _: ?usize, _: ?types.CancellationToken) !TransportResponse {
            const self: *@This() = @ptrCast(@alignCast(ctx.?));
            defer self.calls += 1;
            return switch (self.calls) {
                0 => blk: {
                    try std.testing.expectEqual(HttpMethod.POST, method);
                    try std.testing.expect(std.mem.indexOf(u8, url, "uploadType=resumable") != null);
                    break :blk .{
                        .status = 200,
                        .body = try request_alloc.alloc(u8, 0),
                        .location = try request_alloc.dupe(u8, "https://upload.example/session-1"),
                    };
                },
                1 => blk: {
                    try std.testing.expectEqual(HttpMethod.PUT, method);
                    try std.testing.expectEqualStrings("https://upload.example/session-1", url);
                    try std.testing.expectEqualStrings("resumable-payload", body.?);
                    break :blk .{
                        .status = 200,
                        .body = try request_alloc.dupe(u8, "{\"bucket\":\"bucket\",\"name\":\"backup/segment\",\"etag\":\"final-etag\",\"generation\":\"7\",\"size\":\"17\"}"),
                    };
                },
                else => error.UnexpectedCall,
            };
        }
    };
    const cfg = try jsonApiClientConfigWithBearerTokenAlloc(alloc, "token", null);
    var state = State{};
    var gcs_client = JsonApiClient.initWithRequestFn(alloc, cfg, &state, State.request);
    var client = gcs_client.client();
    defer client.deinit();
    var result = try gcs_client.putFileWithThreshold(alloc, io, "bucket", "backup/segment", source_path, .{}, 0);
    defer result.deinit(alloc);
    try std.testing.expectEqualStrings("final-etag", result.etag.?);
    try std.testing.expectEqual(@as(usize, 2), state.calls);
}

test "gcs local grpc reference path can be discovered when present" {
    const alloc = std.testing.allocator;
    const path = try localGrpcReferencePathAlloc(alloc);
    defer if (path) |value| alloc.free(value);
    if (path) |value| {
        try std.testing.expect(std.mem.endsWith(u8, value, "googleapis/storage/v2/storage.pb.go"));
    }
}

test "json api client pins media reads to the generation returned by metadata" {
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
            max_response_size: ?usize,
            _: ?types.CancellationToken,
        ) !TransportResponse {
            _ = body;
            _ = content_type;
            const self: *@This() = @ptrCast(@alignCast(ptr.?));
            defer self.calls += 1;

            switch (self.calls) {
                0 => {
                    try std.testing.expectEqual(@as(?usize, null), max_response_size);
                    try std.testing.expectEqual(HttpMethod.GET, method);
                    try std.testing.expectEqualStrings("https://storage.googleapis.com/storage/v1/b/bucket/o/folder%2Fdoc.txt", url);
                    try expectHeader(headers, "Authorization", "Bearer token-123");
                    return .{
                        .status = 200,
                        .body = try request_alloc.dupe(u8, "{\"bucket\":\"bucket\",\"name\":\"folder/doc.txt\",\"etag\":\"etag-1\",\"generation\":\"42\",\"size\":\"11\",\"contentType\":\"text/plain\",\"md5Hash\":\"md5-body\"}"),
                    };
                },
                1 => {
                    try std.testing.expectEqual(@as(?usize, null), max_response_size);
                    try std.testing.expectEqual(HttpMethod.GET, method);
                    try std.testing.expectEqualStrings("https://storage.googleapis.com/storage/v1/b/bucket/o/folder%2Fdoc.txt?generation=42&alt=media", url);
                    try expectHeader(headers, "Authorization", "Bearer token-123");
                    try expectHeader(headers, "Range", "bytes=2-5");
                    return .{
                        .status = 206,
                        .body = try request_alloc.dupe(u8, "cdef"),
                        .content_type = try request_alloc.dupe(u8, "text/plain"),
                    };
                },
                2 => {
                    try std.testing.expectEqual(@as(?usize, 4), max_response_size);
                    try std.testing.expectEqual(HttpMethod.GET, method);
                    try std.testing.expectEqualStrings("https://storage.googleapis.com/storage/v1/b/bucket/o/folder%2Fdoc.txt?generation=42&alt=media", url);
                    try expectHeader(headers, "Authorization", "Bearer token-123");
                    try expectHeader(headers, "If-Match", "etag-1");
                    try expectHeader(headers, "Range", "bytes=2-5");
                    return .{
                        .status = 206,
                        .body = try request_alloc.dupe(u8, "cdef"),
                        .etag = try request_alloc.dupe(u8, "etag-media"),
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
        .range = .{ .offset = 2, .length = 4 },
    });
    defer result.deinit(alloc);

    try std.testing.expectEqualStrings("cdef", result.body);
    try std.testing.expectEqualStrings("bucket", result.metadata.bucket);
    try std.testing.expectEqualStrings("folder/doc.txt", result.metadata.key);
    try std.testing.expectEqualStrings("etag-1", result.metadata.etag.?);
    try std.testing.expectEqualStrings("42", result.metadata.version_id.?);
    try std.testing.expectEqual(types.ObjectChecksumAlgorithm.md5_base64, result.metadata.checksum.?.algorithm);
    try std.testing.expectEqualStrings("md5-body", result.metadata.checksum.?.value);
    try std.testing.expectEqual(types.ObjectChecksumType.full_object, result.metadata.checksum.?.checksum_type);
    try std.testing.expectEqualStrings("text/plain", result.metadata.content_type.?);

    var direct = try client.getObject("bucket", "folder/doc.txt", .{
        .version_id = "42",
        .range = .{ .offset = 2, .length = 4 },
        .if_match_etag = "etag-1",
        .skip_metadata_probe = true,
        .max_response_bytes = 4,
    });
    defer direct.deinit(alloc);
    try std.testing.expectEqualStrings("cdef", direct.body);
    try std.testing.expectEqualStrings("bucket", direct.metadata.bucket);
    try std.testing.expectEqualStrings("folder/doc.txt", direct.metadata.key);
    try std.testing.expectEqualStrings("etag-media", direct.metadata.etag.?);
    try std.testing.expectEqualStrings("text/plain", direct.metadata.content_type.?);
    try std.testing.expectEqual(@as(usize, 3), state.calls);
}

test "json api metadata falls back to the always-available crc32c checksum" {
    const alloc = std.testing.allocator;
    var meta = try parseObjectMetadataResponse(
        alloc,
        "bucket",
        "{\"name\":\"composite\",\"generation\":\"9\",\"size\":\"4\",\"crc32c\":\"crc-body\"}",
    );
    defer meta.deinit(alloc);

    try std.testing.expectEqual(types.ObjectChecksumAlgorithm.crc32c_base64, meta.checksum.?.algorithm);
    try std.testing.expectEqualStrings("crc-body", meta.checksum.?.value);
    try std.testing.expectEqual(types.ObjectChecksumType.full_object, meta.checksum.?.checksum_type);
}

test "gcs read cancellation reaches active media and metadata requests" {
    const alloc = std.testing.allocator;
    const State = struct {
        signal: *std.atomic.Value(bool),
        calls: usize = 0,

        fn request(
            ptr: ?*anyopaque,
            _: Allocator,
            method: HttpMethod,
            _: []const u8,
            _: []const HeaderPair,
            _: ?[]const u8,
            _: ?[]const u8,
            _: ?usize,
            cancellation: ?types.CancellationToken,
        ) !TransportResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr.?));
            self.calls += 1;
            try std.testing.expectEqual(HttpMethod.GET, method);
            self.signal.store(true, .release);
            try cancellation.?.check();
            return error.TestExpectedCancellation;
        }
    };

    var signal = std.atomic.Value(bool).init(false);
    var state = State{ .signal = &signal };
    const cfg = try jsonApiClientConfigAlloc(alloc);
    var json_client = JsonApiClient.initWithRequestFn(alloc, cfg, &state, State.request);
    var client = json_client.client();
    defer client.deinit();

    try std.testing.expectError(
        error.Canceled,
        client.getObject("bucket", "object", .{
            .range = .{ .offset = 0, .length = 1 },
            .skip_metadata_probe = true,
            .max_response_bytes = 1,
            .cancellation = types.CancellationToken.fromAtomic(&signal),
        }),
    );
    try std.testing.expectEqual(@as(usize, 1), state.calls);

    signal.store(false, .release);
    try std.testing.expectError(
        error.Canceled,
        client.statObjectWithOptions("bucket", "object", .{
            .cancellation = types.CancellationToken.fromAtomic(&signal),
        }),
    );
    try std.testing.expectEqual(@as(usize, 2), state.calls);
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
            _: ?usize,
            _: ?types.CancellationToken,
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
            _: ?usize,
            _: ?types.CancellationToken,
        ) !TransportResponse {
            _ = headers;
            _ = body;
            _ = content_type;
            try std.testing.expectEqual(HttpMethod.GET, method);
            try std.testing.expectEqualStrings("https://storage.googleapis.com/storage/v1/b/bucket/o?prefix=docs%2F&delimiter=%2F&startOffset=docs%2Fa&maxResults=10", url);
            return .{
                .status = 200,
                .body = try request_alloc.dupe(u8,
                    \\{"items":[{"name":"docs/a","etag":"cursor","size":"1"},{"name":"docs/a.txt","etag":"e1","size":"5"}],"prefixes":["docs/a","docs/sub/"],"nextPageToken":"next-1"}
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
            _: ?usize,
            _: ?types.CancellationToken,
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
