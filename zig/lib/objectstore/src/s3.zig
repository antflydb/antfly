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
const httpx = @import("httpx");
const client_mod = @import("client.zig");
const s3_compat = @import("s3_compat.zig");
const test_support = @import("test_support.zig");
const types = @import("types.zig");

const Allocator = std.mem.Allocator;

pub const Scheme = s3_compat.Scheme;
pub const AddressingStyle = s3_compat.AddressingStyle;
pub const Credentials = s3_compat.Credentials;
pub const EndpointResolution = s3_compat.EndpointResolution;
pub const RequestShape = s3_compat.RequestShape;
pub const S3Path = s3_compat.S3Path;

pub const Config = struct {
    credentials: Credentials,
    addressing_style: AddressingStyle = .virtual_hosted,

    pub fn deinit(self: *Config, alloc: Allocator) void {
        self.credentials.deinit(alloc);
        self.* = undefined;
    }

    pub fn compat(self: Config) s3_compat.Config {
        return .{
            .endpoint = self.credentials.endpoint,
            .region = self.credentials.region,
            .access_key_id = self.credentials.access_key_id,
            .secret_access_key = self.credentials.secret_access_key,
            .session_token = self.credentials.session_token,
            .scheme = if (self.credentials.use_ssl) .https else .http,
            .addressing_style = self.addressing_style,
        };
    }
};

pub const HeaderPair = [2][]const u8;

pub const HttpMethod = enum {
    GET,
    PUT,
    DELETE,
    HEAD,

    fn toHttpx(self: HttpMethod) httpx.Method {
        return switch (self) {
            .GET => .GET,
            .PUT => .PUT,
            .DELETE => .DELETE,
            .HEAD => .HEAD,
        };
    }

    fn asBytes(self: HttpMethod) []const u8 {
        return switch (self) {
            .GET => "GET",
            .PUT => "PUT",
            .DELETE => "DELETE",
            .HEAD => "HEAD",
        };
    }
};

pub const TransportResponse = struct {
    status: u16,
    body: []u8,
    etag: ?[]u8 = null,
    content_type: ?[]u8 = null,
    content_length: ?u64 = null,
    version_id: ?[]u8 = null,
    last_modified: ?[]u8 = null,

    pub fn deinit(self: *TransportResponse, alloc: Allocator) void {
        alloc.free(self.body);
        if (self.etag) |value| alloc.free(value);
        if (self.content_type) |value| alloc.free(value);
        if (self.version_id) |value| alloc.free(value);
        if (self.last_modified) |value| alloc.free(value);
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
            .content_length = if (response.headers.get("Content-Length")) |value| std.fmt.parseInt(u64, value, 10) catch null else null,
            .version_id = if (response.headers.get("x-amz-version-id")) |value| try alloc.dupe(u8, value) else null,
            .last_modified = if (response.headers.get("Last-Modified")) |value| try alloc.dupe(u8, value) else null,
        };
    }
};

pub const Client = struct {
    alloc: Allocator,
    cfg: Config,
    request_ctx: ?*anyopaque,
    request_fn: RequestFn,
    owned_httpx: ?*HttpxTransport,

    pub fn init(alloc: Allocator, cfg: Config) !Client {
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
        cfg: Config,
        request_ctx: ?*anyopaque,
        request_fn: RequestFn,
    ) Client {
        return .{
            .alloc = alloc,
            .cfg = cfg,
            .request_ctx = request_ctx,
            .request_fn = request_fn,
            .owned_httpx = null,
        };
    }

    pub fn deinit(self: *Client) void {
        if (self.owned_httpx) |transport| {
            transport.deinit();
            self.alloc.destroy(transport);
        }
        self.cfg.deinit(self.alloc);
        self.* = undefined;
    }

    pub fn client(self: *Client) client_mod.Client {
        return .{
            .allocator = self.alloc,
            .ptr = self,
            .vtable = &vtable,
        };
    }

    fn bucketExists(self: *Client, bucket: []const u8) !bool {
        var target = try bucketTargetAlloc(self.alloc, self.cfg, bucket);
        defer target.deinit(self.alloc);

        var response = try self.perform(.HEAD, target, &.{}, null, null);
        defer response.deinit(self.alloc);
        return switch (response.status) {
            200, 204, 301, 403 => true,
            404 => false,
            else => return unexpectedStatusError(response.status),
        };
    }

    fn makeBucket(self: *Client, bucket: []const u8) !void {
        var target = try bucketTargetAlloc(self.alloc, self.cfg, bucket);
        defer target.deinit(self.alloc);

        var response = try self.perform(.PUT, target, &.{}, "", null);
        defer response.deinit(self.alloc);
        switch (response.status) {
            200, 201 => return,
            409 => return,
            else => return unexpectedStatusError(response.status),
        }
    }

    fn putObject(
        self: *Client,
        alloc: Allocator,
        bucket: []const u8,
        key: []const u8,
        body: []const u8,
        opts: types.PutOptions,
    ) !types.PutResult {
        var target = try objectTargetAlloc(alloc, self.cfg, bucket, key);
        defer target.deinit(alloc);

        var headers = std.ArrayListUnmanaged(HeaderPair).empty;
        defer headers.deinit(alloc);
        try appendConditionalHeaders(alloc, &headers, opts.if_match_etag, opts.if_none_match);

        var response = try self.perform(.PUT, target, headers.items, body, opts.content_type);
        defer response.deinit(alloc);

        switch (response.status) {
            200, 201 => {},
            304, 412 => return error.PreconditionFailed,
            404 => return error.FileNotFound,
            else => return unexpectedStatusError(response.status),
        }
        return .{
            .etag = if (response.etag) |value| try alloc.dupe(u8, stripQuotes(value)) else null,
            .version_id = if (response.version_id) |value| try alloc.dupe(u8, value) else null,
        };
    }

    fn getObject(
        self: *Client,
        alloc: Allocator,
        bucket: []const u8,
        key: []const u8,
        opts: types.GetOptions,
    ) !types.GetResult {
        var meta = try self.statObject(alloc, bucket, key);
        errdefer meta.deinit(alloc);

        const query = try buildObjectQueryAlloc(alloc, opts.version_id, opts.part_number);
        defer freeQueryPairs(alloc, query);
        var target = try objectTargetAllocWithQuery(alloc, self.cfg, bucket, key, query);
        defer target.deinit(alloc);

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

        var response = try self.perform(.GET, target, headers.items, null, null);
        errdefer response.deinit(alloc);
        switch (response.status) {
            200, 206 => {},
            304, 412 => return error.PreconditionFailed,
            404 => return error.FileNotFound,
            416 => return error.InvalidRange,
            else => return unexpectedStatusError(response.status),
        }

        meta.content_length = @intCast(response.body.len);
        if (response.content_type) |value| {
            if (meta.content_type) |current| alloc.free(current);
            meta.content_type = try alloc.dupe(u8, value);
        }
        if (response.version_id) |value| {
            if (meta.version_id) |current| alloc.free(current);
            meta.version_id = try alloc.dupe(u8, value);
        }

        const out_body = response.body;
        response.body = &.{};
        response.deinit(alloc);
        return .{
            .body = out_body,
            .metadata = meta,
        };
    }

    fn getObjectAttributes(self: *Client, alloc: Allocator, bucket: []const u8, key: []const u8) !types.ObjectAttributes {
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

    fn statObject(self: *Client, alloc: Allocator, bucket: []const u8, key: []const u8) !types.ObjectMetadata {
        var target = try objectTargetAlloc(alloc, self.cfg, bucket, key);
        defer target.deinit(alloc);

        var response = try self.perform(.HEAD, target, &.{}, null, null);
        defer response.deinit(alloc);
        switch (response.status) {
            200 => {},
            404 => return error.FileNotFound,
            304, 412 => return error.PreconditionFailed,
            else => return unexpectedStatusError(response.status),
        }

        return .{
            .bucket = try alloc.dupe(u8, bucket),
            .key = try alloc.dupe(u8, key),
            .etag = if (response.etag) |value| try alloc.dupe(u8, stripQuotes(value)) else null,
            .version_id = if (response.version_id) |value| try alloc.dupe(u8, value) else null,
            .content_length = response.content_length orelse 0,
            .content_type = if (response.content_type) |value| try alloc.dupe(u8, value) else null,
            .last_modified_unix_ms = null,
        };
    }

    fn deleteObject(self: *Client, bucket: []const u8, key: []const u8, opts: types.DeleteOptions) !void {
        const query = try buildDeleteQueryAlloc(self.alloc, opts.version_id);
        defer freeQueryPairs(self.alloc, query);
        var target = try objectTargetAllocWithQuery(self.alloc, self.cfg, bucket, key, query);
        defer target.deinit(self.alloc);

        var headers = std.ArrayListUnmanaged(HeaderPair).empty;
        defer headers.deinit(self.alloc);
        try appendConditionalHeaders(self.alloc, &headers, opts.if_match_etag, false);

        var response = try self.perform(.DELETE, target, headers.items, null, null);
        defer response.deinit(self.alloc);
        switch (response.status) {
            200, 204 => return,
            404 => return error.FileNotFound,
            304, 412 => return error.PreconditionFailed,
            else => return unexpectedStatusError(response.status),
        }
    }

    fn listObjects(self: *Client, alloc: Allocator, bucket: []const u8, opts: types.ListOptions) !types.ListResult {
        const query = try buildListQueryAlloc(alloc, opts);
        defer freeQueryPairs(alloc, query);
        var target = try bucketTargetAllocWithQuery(alloc, self.cfg, bucket, query);
        defer target.deinit(alloc);

        var response = try self.perform(.GET, target, &.{}, null, null);
        defer response.deinit(alloc);
        switch (response.status) {
            200 => return try parseListResponse(alloc, response.body),
            404 => return .{
                .entries = try alloc.alloc(types.ListEntry, 0),
                .common_prefixes = try alloc.alloc([]u8, 0),
            },
            else => return unexpectedStatusError(response.status),
        }
    }

    fn perform(
        self: *Client,
        method: HttpMethod,
        target: RequestTarget,
        headers: []const HeaderPair,
        body: ?[]const u8,
        content_type: ?[]const u8,
    ) !TransportResponse {
        const timestamp = currentUnixSeconds();
        const payload_hash = try sha256HexAlloc(self.alloc, body orelse "");
        defer self.alloc.free(payload_hash);

        const amz_date = try formatAmzDateAlloc(self.alloc, timestamp);
        defer self.alloc.free(amz_date);
        const scope_date = try formatScopeDateAlloc(self.alloc, timestamp);
        defer self.alloc.free(scope_date);

        const signed = try signHeadersAlloc(
            self.alloc,
            self.cfg,
            method,
            target.host,
            target.canonical_uri,
            target.query_pairs,
            headers,
            payload_hash,
            amz_date,
            scope_date,
            content_type,
        );
        defer freeHeaderPairs(self.alloc, signed);

        return try self.request_fn(self.request_ctx, self.alloc, method, target.url, signed, body, content_type);
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
        const self: *Client = @ptrCast(@alignCast(ptr));
        self.deinit();
    }

    fn erasedBucketExists(ptr: *anyopaque, bucket: []const u8) !bool {
        const self: *Client = @ptrCast(@alignCast(ptr));
        return try self.bucketExists(bucket);
    }

    fn erasedMakeBucket(ptr: *anyopaque, bucket: []const u8) !void {
        const self: *Client = @ptrCast(@alignCast(ptr));
        try self.makeBucket(bucket);
    }

    fn erasedPutObject(ptr: *anyopaque, alloc: Allocator, bucket: []const u8, key: []const u8, body: []const u8, opts: types.PutOptions) !types.PutResult {
        const self: *Client = @ptrCast(@alignCast(ptr));
        return try self.putObject(alloc, bucket, key, body, opts);
    }

    fn erasedGetObject(ptr: *anyopaque, alloc: Allocator, bucket: []const u8, key: []const u8, opts: types.GetOptions) !types.GetResult {
        const self: *Client = @ptrCast(@alignCast(ptr));
        return try self.getObject(alloc, bucket, key, opts);
    }

    fn erasedGetObjectAttributes(ptr: *anyopaque, alloc: Allocator, bucket: []const u8, key: []const u8) !types.ObjectAttributes {
        const self: *Client = @ptrCast(@alignCast(ptr));
        return try self.getObjectAttributes(alloc, bucket, key);
    }

    fn erasedStatObject(ptr: *anyopaque, alloc: Allocator, bucket: []const u8, key: []const u8) !types.ObjectMetadata {
        const self: *Client = @ptrCast(@alignCast(ptr));
        return try self.statObject(alloc, bucket, key);
    }

    fn erasedDeleteObject(ptr: *anyopaque, bucket: []const u8, key: []const u8, opts: types.DeleteOptions) !void {
        const self: *Client = @ptrCast(@alignCast(ptr));
        try self.deleteObject(bucket, key, opts);
    }

    fn erasedListObjects(ptr: *anyopaque, alloc: Allocator, bucket: []const u8, opts: types.ListOptions) !types.ListResult {
        const self: *Client = @ptrCast(@alignCast(ptr));
        return try self.listObjects(alloc, bucket, opts);
    }
};

const QueryPair = struct {
    name: []u8,
    value: []u8,
};

const RequestTarget = struct {
    url: []u8,
    host: []u8,
    canonical_uri: []u8,
    query_pairs: []QueryPair,

    fn deinit(self: *RequestTarget, alloc: Allocator) void {
        alloc.free(self.url);
        alloc.free(self.host);
        alloc.free(self.canonical_uri);
        freeQueryPairs(alloc, self.query_pairs);
        self.* = undefined;
    }
};

pub fn fromEnvAlloc(
    alloc: Allocator,
    endpoint_override: ?[]const u8,
    use_ssl: bool,
    access_key_id_override: ?[]const u8,
    secret_access_key_override: ?[]const u8,
    session_token_override: ?[]const u8,
    region_override: ?[]const u8,
    addressing_style: AddressingStyle,
) !Config {
    return .{
        .credentials = try s3_compat.credentialsFromEnvAlloc(
            alloc,
            endpoint_override,
            use_ssl,
            access_key_id_override,
            secret_access_key_override,
            session_token_override,
            region_override,
        ),
        .addressing_style = addressing_style,
    };
}

pub fn resolveEndpointAlloc(alloc: Allocator, endpoint: []const u8, use_ssl: bool) !EndpointResolution {
    return try s3_compat.resolveEndpointAlloc(alloc, endpoint, use_ssl);
}

pub fn parseUrlAlloc(alloc: Allocator, location: []const u8) !S3Path {
    return try s3_compat.parseCanonicalS3UrlAlloc(alloc, location);
}

pub fn extractBucketFromUrlAlloc(alloc: Allocator, location: []const u8) ![]u8 {
    return try s3_compat.extractBucketFromUrlAlloc(alloc, location);
}

pub fn objectUriAlloc(alloc: Allocator, cfg: Config, bucket: []const u8, key: []const u8) !RequestShape {
    return try s3_compat.objectUriAlloc(alloc, cfg.compat(), bucket, key);
}

pub fn putObjectShapeAlloc(alloc: Allocator, cfg: Config, bucket: []const u8, key: []const u8, opts: types.PutOptions) !RequestShape {
    return try s3_compat.putObjectShapeAlloc(alloc, cfg.compat(), bucket, key, opts);
}

fn bucketTargetAlloc(alloc: Allocator, cfg: Config, bucket: []const u8) !RequestTarget {
    return try bucketTargetAllocWithQuery(alloc, cfg, bucket, &.{});
}

fn bucketTargetAllocWithQuery(alloc: Allocator, cfg: Config, bucket: []const u8, query_pairs: []const QueryPair) !RequestTarget {
    const scheme = if (cfg.credentials.use_ssl) "https" else "http";
    const encoded_bucket = try encodeUriComponentAlloc(alloc, bucket, false);
    defer alloc.free(encoded_bucket);

    const host = switch (cfg.addressing_style) {
        .virtual_hosted => try std.fmt.allocPrint(alloc, "{s}.{s}", .{ bucket, cfg.credentials.endpoint }),
        .path => try alloc.dupe(u8, cfg.credentials.endpoint),
    };
    errdefer alloc.free(host);

    const canonical_uri = switch (cfg.addressing_style) {
        .virtual_hosted => try alloc.dupe(u8, "/"),
        .path => try std.fmt.allocPrint(alloc, "/{s}", .{encoded_bucket}),
    };
    errdefer alloc.free(canonical_uri);

    const owned_query = try cloneQueryPairsAlloc(alloc, query_pairs);
    errdefer freeQueryPairs(alloc, owned_query);
    const canonical_query = try canonicalQueryStringAlloc(alloc, owned_query);
    defer alloc.free(canonical_query);

    const url = if (canonical_query.len == 0)
        switch (cfg.addressing_style) {
            .virtual_hosted => try std.fmt.allocPrint(alloc, "{s}://{s}", .{ scheme, host }),
            .path => try std.fmt.allocPrint(alloc, "{s}://{s}/{s}", .{ scheme, host, encoded_bucket }),
        }
    else switch (cfg.addressing_style) {
        .virtual_hosted => try std.fmt.allocPrint(alloc, "{s}://{s}?{s}", .{ scheme, host, canonical_query }),
        .path => try std.fmt.allocPrint(alloc, "{s}://{s}/{s}?{s}", .{ scheme, host, encoded_bucket, canonical_query }),
    };
    return .{
        .url = url,
        .host = host,
        .canonical_uri = canonical_uri,
        .query_pairs = owned_query,
    };
}

fn objectTargetAlloc(alloc: Allocator, cfg: Config, bucket: []const u8, key: []const u8) !RequestTarget {
    return try objectTargetAllocWithQuery(alloc, cfg, bucket, key, &.{});
}

fn objectTargetAllocWithQuery(alloc: Allocator, cfg: Config, bucket: []const u8, key: []const u8, query_pairs: []const QueryPair) !RequestTarget {
    var shape = try s3_compat.objectUriAlloc(alloc, cfg.compat(), bucket, key);
    defer shape.deinit(alloc);

    const parsed = try std.Uri.parse(shape.uri);
    const canonical_uri = try alloc.dupe(u8, parsed.path.percent_encoded);
    errdefer alloc.free(canonical_uri);
    const owned_query = try cloneQueryPairsAlloc(alloc, query_pairs);
    errdefer freeQueryPairs(alloc, owned_query);
    const canonical_query = try canonicalQueryStringAlloc(alloc, owned_query);
    defer alloc.free(canonical_query);

    const url = if (canonical_query.len == 0)
        try alloc.dupe(u8, shape.uri)
    else
        try std.fmt.allocPrint(alloc, "{s}?{s}", .{ shape.uri, canonical_query });
    return .{
        .url = url,
        .host = try alloc.dupe(u8, shape.host),
        .canonical_uri = canonical_uri,
        .query_pairs = owned_query,
    };
}

fn buildObjectQueryAlloc(alloc: Allocator, version_id: ?[]const u8, part_number: ?u32) ![]QueryPair {
    var query = std.ArrayListUnmanaged(QueryPair).empty;
    errdefer freeQueryPairs(alloc, query.items);

    if (version_id) |value| try appendQueryPair(alloc, &query, "versionId", value);
    if (part_number) |value| {
        const encoded = try std.fmt.allocPrint(alloc, "{d}", .{value});
        errdefer alloc.free(encoded);
        try query.append(alloc, .{ .name = try alloc.dupe(u8, "partNumber"), .value = encoded });
    }
    return try query.toOwnedSlice(alloc);
}

fn buildDeleteQueryAlloc(alloc: Allocator, version_id: ?[]const u8) ![]QueryPair {
    var query = std.ArrayListUnmanaged(QueryPair).empty;
    errdefer freeQueryPairs(alloc, query.items);
    if (version_id) |value| try appendQueryPair(alloc, &query, "versionId", value);
    return try query.toOwnedSlice(alloc);
}

fn buildListQueryAlloc(alloc: Allocator, opts: types.ListOptions) ![]QueryPair {
    var query = std.ArrayListUnmanaged(QueryPair).empty;
    errdefer freeQueryPairs(alloc, query.items);

    try appendQueryPair(alloc, &query, "list-type", "2");
    if (opts.prefix.len > 0) try appendQueryPair(alloc, &query, "prefix", opts.prefix);
    if (!opts.recursive and opts.delimiter.len > 0) try appendQueryPair(alloc, &query, "delimiter", opts.delimiter);
    if (opts.start_after) |value| try appendQueryPair(alloc, &query, "start-after", value);
    if (opts.continuation_token) |value| try appendQueryPair(alloc, &query, "continuation-token", value);
    if (opts.max_keys != 1000) {
        const value = try std.fmt.allocPrint(alloc, "{d}", .{opts.max_keys});
        errdefer alloc.free(value);
        try query.append(alloc, .{ .name = try alloc.dupe(u8, "max-keys"), .value = value });
    }
    return try query.toOwnedSlice(alloc);
}

fn appendQueryPair(alloc: Allocator, list: *std.ArrayListUnmanaged(QueryPair), name: []const u8, value: []const u8) !void {
    try list.append(alloc, .{
        .name = try alloc.dupe(u8, name),
        .value = try alloc.dupe(u8, value),
    });
}

fn cloneQueryPairsAlloc(alloc: Allocator, pairs: []const QueryPair) ![]QueryPair {
    const out = try alloc.alloc(QueryPair, pairs.len);
    errdefer {
        for (out[0..@min(out.len, pairs.len)]) |pair| {
            alloc.free(pair.name);
            alloc.free(pair.value);
        }
        alloc.free(out);
    }
    for (pairs, 0..) |pair, idx| {
        out[idx] = .{
            .name = try alloc.dupe(u8, pair.name),
            .value = try alloc.dupe(u8, pair.value),
        };
    }
    return out;
}

fn freeQueryPairs(alloc: Allocator, pairs: []const QueryPair) void {
    for (pairs) |pair| {
        alloc.free(pair.name);
        alloc.free(pair.value);
    }
    alloc.free(pairs);
}

fn signHeadersAlloc(
    alloc: Allocator,
    cfg: Config,
    method: HttpMethod,
    host: []const u8,
    canonical_uri: []const u8,
    query_pairs: []const QueryPair,
    extra_headers: []const HeaderPair,
    payload_hash: []const u8,
    amz_date: []const u8,
    scope_date: []const u8,
    content_type: ?[]const u8,
) ![]HeaderPair {
    var headers = std.ArrayListUnmanaged(HeaderPair).empty;
    errdefer freeHeaderPairs(alloc, headers.items);

    try headers.append(alloc, .{ try alloc.dupe(u8, "Host"), try alloc.dupe(u8, host) });
    try headers.append(alloc, .{ try alloc.dupe(u8, "x-amz-date"), try alloc.dupe(u8, amz_date) });
    try headers.append(alloc, .{ try alloc.dupe(u8, "x-amz-content-sha256"), try alloc.dupe(u8, payload_hash) });
    if (cfg.credentials.session_token) |token| {
        try headers.append(alloc, .{ try alloc.dupe(u8, "x-amz-security-token"), try alloc.dupe(u8, token) });
    }
    if (content_type) |value| {
        try headers.append(alloc, .{ try alloc.dupe(u8, "Content-Type"), try alloc.dupe(u8, value) });
    }
    for (extra_headers) |pair| {
        try headers.append(alloc, .{
            try alloc.dupe(u8, pair[0]),
            try alloc.dupe(u8, pair[1]),
        });
    }

    const signature = try authorizationValueAlloc(
        alloc,
        cfg,
        method,
        canonical_uri,
        query_pairs,
        headers.items,
        payload_hash,
        amz_date,
        scope_date,
    );
    errdefer alloc.free(signature);
    try headers.append(alloc, .{ try alloc.dupe(u8, "Authorization"), signature });
    return try headers.toOwnedSlice(alloc);
}

fn authorizationValueAlloc(
    alloc: Allocator,
    cfg: Config,
    method: HttpMethod,
    canonical_uri: []const u8,
    query_pairs: []const QueryPair,
    headers: []const HeaderPair,
    payload_hash: []const u8,
    amz_date: []const u8,
    scope_date: []const u8,
) ![]u8 {
    var canonical_headers = try canonicalHeadersAlloc(alloc, headers);
    defer canonical_headers.deinit(alloc);

    const canonical_query = try canonicalQueryStringAlloc(alloc, query_pairs);
    defer alloc.free(canonical_query);

    const canonical_request = try std.fmt.allocPrint(
        alloc,
        "{s}\n{s}\n{s}\n{s}\n{s}\n{s}",
        .{
            method.asBytes(),
            canonical_uri,
            canonical_query,
            canonical_headers.header_block,
            canonical_headers.signed_headers,
            payload_hash,
        },
    );
    defer alloc.free(canonical_request);

    const canonical_request_hash = try sha256HexAlloc(alloc, canonical_request);
    defer alloc.free(canonical_request_hash);

    const scope = try std.fmt.allocPrint(alloc, "{s}/{s}/s3/aws4_request", .{ scope_date, cfg.credentials.region });
    defer alloc.free(scope);
    const string_to_sign = try std.fmt.allocPrint(
        alloc,
        "AWS4-HMAC-SHA256\n{s}\n{s}\n{s}",
        .{ amz_date, scope, canonical_request_hash },
    );
    defer alloc.free(string_to_sign);

    const signing_key = try signingKeyAlloc(alloc, cfg.credentials.secret_access_key, scope_date, cfg.credentials.region);
    defer alloc.free(signing_key);
    const signature = try hmacSha256HexAlloc(alloc, signing_key, string_to_sign);
    defer alloc.free(signature);

    return try std.fmt.allocPrint(
        alloc,
        "AWS4-HMAC-SHA256 Credential={s}/{s}, SignedHeaders={s}, Signature={s}",
        .{
            cfg.credentials.access_key_id,
            scope,
            canonical_headers.signed_headers,
            signature,
        },
    );
}

const CanonicalHeaders = struct {
    entries: []CanonicalHeader,
    header_block: []u8,
    signed_headers: []u8,

    fn deinit(self: *CanonicalHeaders, alloc: Allocator) void {
        for (self.entries) |*entry| entry.deinit(alloc);
        alloc.free(self.entries);
        alloc.free(self.header_block);
        alloc.free(self.signed_headers);
        self.* = undefined;
    }
};

const CanonicalHeader = struct {
    name: []u8,
    value: []u8,

    fn deinit(self: *CanonicalHeader, alloc: Allocator) void {
        alloc.free(self.name);
        alloc.free(self.value);
        self.* = undefined;
    }
};

fn canonicalHeadersAlloc(alloc: Allocator, headers: []const HeaderPair) !CanonicalHeaders {
    const entries = try alloc.alloc(CanonicalHeader, headers.len);
    errdefer {
        for (entries[0..headers.len]) |entry| {
            alloc.free(entry.name);
            alloc.free(entry.value);
        }
        alloc.free(entries);
    }

    for (headers, 0..) |pair, idx| {
        entries[idx] = .{
            .name = try asciiLowerAlloc(alloc, std.mem.trim(u8, pair[0], " ")),
            .value = try alloc.dupe(u8, std.mem.trim(u8, pair[1], " ")),
        };
    }
    std.mem.sort(CanonicalHeader, entries, {}, lessCanonicalHeader);

    var block = std.ArrayListUnmanaged(u8).empty;
    errdefer block.deinit(alloc);
    var signed = std.ArrayListUnmanaged(u8).empty;
    errdefer signed.deinit(alloc);

    for (entries, 0..) |entry, idx| {
        try block.appendSlice(alloc, entry.name);
        try block.append(alloc, ':');
        try block.appendSlice(alloc, entry.value);
        try block.append(alloc, '\n');

        if (idx > 0) try signed.append(alloc, ';');
        try signed.appendSlice(alloc, entry.name);
    }

    return .{
        .entries = entries,
        .header_block = try block.toOwnedSlice(alloc),
        .signed_headers = try signed.toOwnedSlice(alloc),
    };
}

fn canonicalQueryStringAlloc(alloc: Allocator, pairs: []const QueryPair) ![]u8 {
    const encoded = try alloc.alloc(QueryPair, pairs.len);
    errdefer {
        for (encoded[0..pairs.len]) |pair| {
            alloc.free(pair.name);
            alloc.free(pair.value);
        }
        alloc.free(encoded);
    }
    for (pairs, 0..) |pair, idx| {
        encoded[idx] = .{
            .name = try encodeUriComponentAlloc(alloc, pair.name, true),
            .value = try encodeUriComponentAlloc(alloc, pair.value, true),
        };
    }
    defer {
        for (encoded) |pair| {
            alloc.free(pair.name);
            alloc.free(pair.value);
        }
        alloc.free(encoded);
    }

    std.mem.sort(QueryPair, encoded, {}, lessQueryPair);
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    for (encoded, 0..) |pair, idx| {
        if (idx > 0) try out.append(alloc, '&');
        try out.appendSlice(alloc, pair.name);
        try out.append(alloc, '=');
        try out.appendSlice(alloc, pair.value);
    }
    return try out.toOwnedSlice(alloc);
}

fn signingKeyAlloc(alloc: Allocator, secret: []const u8, scope_date: []const u8, region: []const u8) ![]u8 {
    const k_secret = try std.fmt.allocPrint(alloc, "AWS4{s}", .{secret});
    defer alloc.free(k_secret);

    const k_date = try hmacSha256Alloc(alloc, k_secret, scope_date);
    defer alloc.free(k_date);
    const k_region = try hmacSha256Alloc(alloc, k_date, region);
    defer alloc.free(k_region);
    const k_service = try hmacSha256Alloc(alloc, k_region, "s3");
    defer alloc.free(k_service);
    return try hmacSha256Alloc(alloc, k_service, "aws4_request");
}

fn hmacSha256Alloc(alloc: Allocator, key: []const u8, data: []const u8) ![]u8 {
    var mac: [std.crypto.auth.hmac.sha2.HmacSha256.mac_length]u8 = undefined;
    std.crypto.auth.hmac.sha2.HmacSha256.create(mac[0..], data, key);
    return try alloc.dupe(u8, mac[0..]);
}

fn hmacSha256HexAlloc(alloc: Allocator, key: []const u8, data: []const u8) ![]u8 {
    var mac: [std.crypto.auth.hmac.sha2.HmacSha256.mac_length]u8 = undefined;
    std.crypto.auth.hmac.sha2.HmacSha256.create(mac[0..], data, key);
    return try bytesToHexAlloc(alloc, mac[0..]);
}

fn sha256HexAlloc(alloc: Allocator, body: []const u8) ![]u8 {
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(body, &digest, .{});
    return try bytesToHexAlloc(alloc, digest[0..]);
}

fn bytesToHexAlloc(alloc: Allocator, bytes: []const u8) ![]u8 {
    const out = try alloc.alloc(u8, bytes.len * 2);
    for (bytes, 0..) |byte, idx| {
        out[idx * 2] = std.fmt.digitToChar(byte >> 4, .lower);
        out[idx * 2 + 1] = std.fmt.digitToChar(byte & 0x0f, .lower);
    }
    return out;
}

fn stripQuotes(value: []const u8) []const u8 {
    return std.mem.trim(u8, value, "\"");
}

fn lessCanonicalHeader(_: void, lhs: CanonicalHeader, rhs: CanonicalHeader) bool {
    return std.mem.order(u8, lhs.name, rhs.name) == .lt;
}

fn lessQueryPair(_: void, lhs: QueryPair, rhs: QueryPair) bool {
    const by_name = std.mem.order(u8, lhs.name, rhs.name);
    if (by_name != .eq) return by_name == .lt;
    return std.mem.order(u8, lhs.value, rhs.value) == .lt;
}

fn appendConditionalHeaders(
    alloc: Allocator,
    headers: *std.ArrayListUnmanaged(HeaderPair),
    if_match_etag: ?[]const u8,
    if_none_match: bool,
) !void {
    if (if_match_etag) |value| {
        try headers.append(alloc, .{ "If-Match", value });
    }
    if (if_none_match) {
        try headers.append(alloc, .{ "If-None-Match", "*" });
    }
}

fn byteRangeHeaderAlloc(alloc: Allocator, range: types.ByteRange) ![]u8 {
    if (range.length) |len| {
        if (len == 0) return error.InvalidRange;
        return try std.fmt.allocPrint(alloc, "bytes={d}-{d}", .{ range.offset, range.offset + len - 1 });
    }
    return try std.fmt.allocPrint(alloc, "bytes={d}-", .{range.offset});
}

fn currentUnixSeconds() u64 {
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    const now = std.Io.Timestamp.now(io_impl.io(), .awake);
    const ns: u64 = @intCast(now.toNanoseconds());
    return ns / std.time.ns_per_s;
}

fn formatAmzDateAlloc(alloc: Allocator, unix_seconds: u64) ![]u8 {
    const epoch_seconds = std.time.epoch.EpochSeconds{ .secs = unix_seconds };
    const year_day = epoch_seconds.getEpochDay().calculateYearDay();
    const month_day = year_day.calculateMonthDay();
    const day_seconds = epoch_seconds.getDaySeconds();
    return try std.fmt.allocPrint(
        alloc,
        "{d:0>4}{d:0>2}{d:0>2}T{d:0>2}{d:0>2}{d:0>2}Z",
        .{
            year_day.year,
            @intFromEnum(month_day.month) + 1,
            month_day.day_index + 1,
            day_seconds.getHoursIntoDay(),
            day_seconds.getMinutesIntoHour(),
            day_seconds.getSecondsIntoMinute(),
        },
    );
}

fn formatScopeDateAlloc(alloc: Allocator, unix_seconds: u64) ![]u8 {
    const epoch_seconds = std.time.epoch.EpochSeconds{ .secs = unix_seconds };
    const year_day = epoch_seconds.getEpochDay().calculateYearDay();
    const month_day = year_day.calculateMonthDay();
    return try std.fmt.allocPrint(
        alloc,
        "{d:0>4}{d:0>2}{d:0>2}",
        .{
            year_day.year,
            @intFromEnum(month_day.month) + 1,
            month_day.day_index + 1,
        },
    );
}

fn asciiLowerAlloc(alloc: Allocator, input: []const u8) ![]u8 {
    const out = try alloc.dupe(u8, input);
    _ = std.ascii.lowerString(out, out);
    return out;
}

fn encodeUriComponentAlloc(alloc: Allocator, input: []const u8, encode_slash: bool) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    for (input) |byte| {
        if (!encode_slash and byte == '/') {
            try out.append(alloc, '/');
            continue;
        }
        if (isUnreserved(byte)) {
            try out.append(alloc, byte);
            continue;
        }
        const encoded = try std.fmt.allocPrint(alloc, "%{X:0>2}", .{byte});
        defer alloc.free(encoded);
        try out.appendSlice(alloc, encoded);
    }
    return try out.toOwnedSlice(alloc);
}

fn isUnreserved(byte: u8) bool {
    return switch (byte) {
        'A'...'Z', 'a'...'z', '0'...'9', '-', '.', '_', '~' => true,
        else => false,
    };
}

fn freeHeaderPairs(alloc: Allocator, headers: []const HeaderPair) void {
    for (headers) |pair| {
        alloc.free(pair[0]);
        alloc.free(pair[1]);
    }
    alloc.free(headers);
}

fn unexpectedStatusError(status: u16) anyerror {
    return switch (status) {
        400 => error.InvalidRequest,
        401, 403 => error.AccessDenied,
        404 => error.FileNotFound,
        409 => error.Conflict,
        412 => error.PreconditionFailed,
        else => error.UnexpectedHttpStatus,
    };
}

fn parseListResponse(alloc: Allocator, xml: []const u8) !types.ListResult {
    var entries = std.ArrayListUnmanaged(types.ListEntry).empty;
    errdefer {
        for (entries.items) |*entry| entry.deinit(alloc);
        entries.deinit(alloc);
    }

    var prefixes = std.ArrayListUnmanaged([]u8).empty;
    errdefer {
        for (prefixes.items) |prefix| alloc.free(prefix);
        prefixes.deinit(alloc);
    }

    var search_from: usize = 0;
    while (findBlock(xml, "Contents", search_from)) |block| {
        search_from = block.end;
        const key = try decodeXmlAlloc(alloc, block.inner, "Key");
        errdefer alloc.free(key);
        const etag_raw = try optionalTagAlloc(alloc, block.inner, "ETag");
        errdefer if (etag_raw) |value| alloc.free(value);
        const size_raw = try requiredTagAlloc(alloc, block.inner, "Size");
        defer alloc.free(size_raw);
        try entries.append(alloc, .{
            .key = key,
            .etag = if (etag_raw) |value| try alloc.dupe(u8, stripQuotes(value)) else null,
            .size = try std.fmt.parseInt(u64, size_raw, 10),
            .last_modified_unix_ms = null,
        });
        if (etag_raw) |value| alloc.free(value);
    }

    search_from = 0;
    while (findBlock(xml, "CommonPrefixes", search_from)) |block| {
        search_from = block.end;
        try prefixes.append(alloc, try decodeXmlAlloc(alloc, block.inner, "Prefix"));
    }

    return .{
        .entries = try entries.toOwnedSlice(alloc),
        .common_prefixes = try prefixes.toOwnedSlice(alloc),
        .next_continuation_token = if (try optionalTagAlloc(alloc, xml, "NextContinuationToken")) |value| value else null,
    };
}

const XmlBlock = struct {
    inner: []const u8,
    end: usize,
};

fn findBlock(xml: []const u8, tag: []const u8, start: usize) ?XmlBlock {
    var open_buf: [64]u8 = undefined;
    var close_buf: [64]u8 = undefined;
    const open_tag = std.fmt.bufPrint(&open_buf, "<{s}>", .{tag}) catch return null;
    const close_tag = std.fmt.bufPrint(&close_buf, "</{s}>", .{tag}) catch return null;
    const open_index = std.mem.indexOfPos(u8, xml, start, open_tag) orelse return null;
    const inner_start = open_index + open_tag.len;
    const close_index = std.mem.indexOfPos(u8, xml, inner_start, close_tag) orelse return null;
    return .{
        .inner = xml[inner_start..close_index],
        .end = close_index + close_tag.len,
    };
}

fn requiredTagAlloc(alloc: Allocator, xml: []const u8, tag: []const u8) ![]u8 {
    const value = try optionalTagAlloc(alloc, xml, tag);
    return value orelse error.MissingXmlTag;
}

fn optionalTagAlloc(alloc: Allocator, xml: []const u8, tag: []const u8) !?[]u8 {
    const block = findBlock(xml, tag, 0) orelse return null;
    return try alloc.dupe(u8, block.inner);
}

fn decodeXmlAlloc(alloc: Allocator, xml: []const u8, tag: []const u8) ![]u8 {
    const raw = try requiredTagAlloc(alloc, xml, tag);
    defer alloc.free(raw);

    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);

    var idx: usize = 0;
    while (idx < raw.len) {
        if (raw[idx] != '&') {
            try out.append(alloc, raw[idx]);
            idx += 1;
            continue;
        }
        if (std.mem.startsWith(u8, raw[idx..], "&amp;")) {
            try out.append(alloc, '&');
            idx += 5;
        } else if (std.mem.startsWith(u8, raw[idx..], "&lt;")) {
            try out.append(alloc, '<');
            idx += 4;
        } else if (std.mem.startsWith(u8, raw[idx..], "&gt;")) {
            try out.append(alloc, '>');
            idx += 4;
        } else if (std.mem.startsWith(u8, raw[idx..], "&quot;")) {
            try out.append(alloc, '"');
            idx += 6;
        } else if (std.mem.startsWith(u8, raw[idx..], "&apos;")) {
            try out.append(alloc, '\'');
            idx += 6;
        } else {
            try out.append(alloc, raw[idx]);
            idx += 1;
        }
    }
    return try out.toOwnedSlice(alloc);
}

test "s3 config builds object request shape" {
    const alloc = std.testing.allocator;
    var cfg = Config{
        .credentials = .{
            .endpoint = try alloc.dupe(u8, "127.0.0.1:9000"),
            .use_ssl = false,
            .access_key_id = try alloc.dupe(u8, "key"),
            .secret_access_key = try alloc.dupe(u8, "secret"),
            .region = try alloc.dupe(u8, "us-east-1"),
        },
        .addressing_style = .path,
    };
    defer cfg.deinit(alloc);

    var shape = try objectUriAlloc(alloc, cfg, "bucket", "a/b.txt");
    defer shape.deinit(alloc);
    try std.testing.expectEqualStrings("http://127.0.0.1:9000/bucket/a/b.txt", shape.uri);
}

test "s3 authorization uses sigv4 format" {
    const alloc = std.testing.allocator;
    var cfg = Config{
        .credentials = .{
            .endpoint = try alloc.dupe(u8, "127.0.0.1:9000"),
            .use_ssl = false,
            .access_key_id = try alloc.dupe(u8, "minioadmin"),
            .secret_access_key = try alloc.dupe(u8, "miniosecret"),
            .region = try alloc.dupe(u8, "us-east-1"),
        },
        .addressing_style = .path,
    };
    defer cfg.deinit(alloc);

    const query = [_]QueryPair{};
    const headers = [_]HeaderPair{
        .{ "Content-Type", "text/plain" },
    };
    const auth = try authorizationValueAlloc(
        alloc,
        cfg,
        .PUT,
        "/bucket/a.txt",
        &query,
        &headers,
        "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824",
        "20260327T120000Z",
        "20260327",
    );
    defer alloc.free(auth);

    try std.testing.expect(std.mem.startsWith(u8, auth, "AWS4-HMAC-SHA256 Credential=minioadmin/20260327/us-east-1/s3/aws4_request"));
    try std.testing.expect(std.mem.indexOf(u8, auth, "SignedHeaders=content-type") != null);
    try std.testing.expect(std.mem.indexOf(u8, auth, "Signature=") != null);
}

test "s3 list parser extracts entries and prefixes" {
    const alloc = std.testing.allocator;
    const xml =
        \\<?xml version="1.0" encoding="UTF-8"?>
        \\<ListBucketResult>
        \\  <IsTruncated>false</IsTruncated>
        \\  <Contents>
        \\    <Key>a%2Fb.txt</Key>
        \\    <ETag>"etag-a"</ETag>
        \\    <Size>5</Size>
        \\  </Contents>
        \\  <CommonPrefixes>
        \\    <Prefix>nested/</Prefix>
        \\  </CommonPrefixes>
        \\</ListBucketResult>
    ;

    var parsed = try parseListResponse(alloc, xml);
    defer parsed.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), parsed.entries.len);
    try std.testing.expectEqualStrings("a%2Fb.txt", parsed.entries[0].key);
    try std.testing.expectEqualStrings("etag-a", parsed.entries[0].etag.?);
    try std.testing.expectEqual(@as(u64, 5), parsed.entries[0].size);
    try std.testing.expectEqual(@as(usize, 1), parsed.common_prefixes.len);
    try std.testing.expectEqualStrings("nested/", parsed.common_prefixes[0]);
}

test "s3 object query includes version and part selectors" {
    const alloc = std.testing.allocator;
    const query = try buildObjectQueryAlloc(alloc, "v123", 7);
    defer freeQueryPairs(alloc, query);
    const rendered = try canonicalQueryStringAlloc(alloc, query);
    defer alloc.free(rendered);
    try std.testing.expectEqualStrings("partNumber=7&versionId=v123", rendered);
}

test "s3 client signs and issues object operations through request fn" {
    const alloc = std.testing.allocator;

    const Step = struct {
        method: HttpMethod,
        url_contains: []const u8,
        status: u16,
        body: []const u8 = "",
        etag: ?[]const u8 = null,
        content_type: ?[]const u8 = null,
        content_length: ?u64 = null,
        version_id: ?[]const u8 = null,
        expect_body: ?[]const u8 = null,
    };

    const Fake = struct {
        steps: []const Step,
        index: usize = 0,

        fn request(
            ctx: ?*anyopaque,
            req_alloc: Allocator,
            method: HttpMethod,
            url: []const u8,
            headers: []const HeaderPair,
            body: ?[]const u8,
            _: ?[]const u8,
        ) !TransportResponse {
            const self: *@This() = @ptrCast(@alignCast(ctx.?));
            defer self.index += 1;
            const step = self.steps[self.index];
            try std.testing.expectEqual(step.method, method);
            try std.testing.expect(std.mem.indexOf(u8, url, step.url_contains) != null);
            try expectHeader(headers, "Authorization");
            try expectHeader(headers, "x-amz-date");
            try expectHeader(headers, "x-amz-content-sha256");
            if (step.expect_body) |expected| {
                try std.testing.expectEqualStrings(expected, body orelse "");
            }
            return .{
                .status = step.status,
                .body = try req_alloc.dupe(u8, step.body),
                .etag = if (step.etag) |value| try req_alloc.dupe(u8, value) else null,
                .content_type = if (step.content_type) |value| try req_alloc.dupe(u8, value) else null,
                .content_length = step.content_length,
                .version_id = if (step.version_id) |value| try req_alloc.dupe(u8, value) else null,
            };
        }

        fn expectHeader(headers: []const HeaderPair, name: []const u8) !void {
            for (headers) |pair| {
                if (std.ascii.eqlIgnoreCase(pair[0], name)) return;
            }
            return error.MissingHeader;
        }
    };

    const steps = [_]Step{
        .{ .method = .HEAD, .url_contains = "/bucket", .status = 404 },
        .{ .method = .PUT, .url_contains = "/bucket", .status = 200 },
        .{ .method = .PUT, .url_contains = "/bucket/docs/a.txt", .status = 200, .etag = "\"etag-put\"", .expect_body = "hello" },
        .{ .method = .HEAD, .url_contains = "/bucket/docs/a.txt", .status = 200, .etag = "\"etag-head\"", .content_type = "text/plain", .content_length = 5 },
        .{ .method = .GET, .url_contains = "/bucket/docs/a.txt", .status = 200, .body = "hello", .etag = "\"etag-head\"", .content_type = "text/plain", .content_length = 5 },
        .{ .method = .HEAD, .url_contains = "/bucket/docs/a.txt", .status = 200, .etag = "\"etag-head\"", .content_type = "text/plain", .content_length = 5 },
        .{ .method = .GET, .url_contains = "list-type=2", .status = 200, .body = "<ListBucketResult><Contents><Key>docs/a.txt</Key><ETag>\"etag-head\"</ETag><Size>5</Size></Contents></ListBucketResult>" },
        .{ .method = .DELETE, .url_contains = "/bucket/docs/a.txt", .status = 204 },
    };
    var fake = Fake{ .steps = &steps };

    const cfg = Config{
        .credentials = .{
            .endpoint = try alloc.dupe(u8, "127.0.0.1:9000"),
            .use_ssl = false,
            .access_key_id = try alloc.dupe(u8, "minioadmin"),
            .secret_access_key = try alloc.dupe(u8, "miniosecret"),
            .region = try alloc.dupe(u8, "us-east-1"),
        },
        .addressing_style = .path,
    };
    var s3_client = Client.initWithRequestFn(alloc, cfg, &fake, Fake.request);
    var client = s3_client.client();
    defer client.deinit();

    try std.testing.expect(!(try client.bucketExists("bucket")));
    try client.makeBucket("bucket");

    var put = try client.putObject("bucket", "docs/a.txt", "hello", .{ .content_type = "text/plain" });
    defer put.deinit(alloc);
    try std.testing.expectEqualStrings("etag-put", put.etag.?);

    var get = try client.getObject("bucket", "docs/a.txt", .{});
    defer get.deinit(alloc);
    try std.testing.expectEqualStrings("hello", get.body);
    try std.testing.expectEqualStrings("etag-head", get.metadata.etag.?);

    var meta = try client.statObject("bucket", "docs/a.txt");
    defer meta.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 5), meta.content_length);

    var listed = try client.listObjects("bucket", .{ .prefix = "docs/" });
    defer listed.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), listed.entries.len);
    try std.testing.expectEqualStrings("docs/a.txt", listed.entries[0].key);

    try client.deleteObject("bucket", "docs/a.txt", .{});
    try std.testing.expectEqual(steps.len, fake.index);
}

test "s3 client round-trips against env-configured endpoint" {
    const alloc = std.testing.allocator;
    try test_support.requireIntegrationEnabled("OBJECTSTORE_S3_INTEGRATION");

    const bucket = try test_support.requiredOwned(alloc, "OBJECTSTORE_S3_TEST_BUCKET");
    defer alloc.free(bucket);

    const cfg = fromEnvAlloc(alloc, null, true, null, null, null, null, .path) catch return error.SkipZigTest;
    var s3_client = try Client.init(alloc, cfg);
    var client = s3_client.client();
    defer client.deinit();

    if (!(try client.bucketExists(bucket))) try client.makeBucket(bucket);

    const key = try std.fmt.allocPrint(alloc, "zig-objectstore/{d}.txt", .{test_support.integrationNonce()});
    defer alloc.free(key);

    var put = try client.putObject(bucket, key, "hello-minio", .{ .content_type = "text/plain" });
    defer put.deinit(alloc);

    var meta = try client.statObject(bucket, key);
    defer meta.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 11), meta.content_length);
    try std.testing.expectEqualStrings("text/plain", meta.content_type.?);

    var get = try client.getObject(bucket, key, .{});
    defer get.deinit(alloc);
    try std.testing.expectEqualStrings("hello-minio", get.body);

    var listed = try client.listObjects(bucket, .{
        .prefix = "zig-objectstore/",
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
