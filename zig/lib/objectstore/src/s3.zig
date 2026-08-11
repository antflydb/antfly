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
const multipart_upload_threshold: u64 = 64 * 1024 * 1024;
const multipart_upload_min_part_bytes: u64 = 16 * 1024 * 1024;
const multipart_upload_max_part_bytes: u64 = 512 * 1024 * 1024;
const multipart_upload_part_alignment: u64 = 1024 * 1024;
const max_multipart_parts: u64 = 10_000;

pub const Scheme = s3_compat.Scheme;
pub const AddressingStyle = s3_compat.AddressingStyle;
pub const Credentials = s3_compat.Credentials;
pub const EndpointResolution = s3_compat.EndpointResolution;
pub const RequestShape = s3_compat.RequestShape;
pub const S3Path = s3_compat.S3Path;

pub const Config = struct {
    credentials: Credentials,
    addressing_style: AddressingStyle = .virtual_hosted,
    credential_provider: ?CredentialProvider = null,
    /// Optional hard ceiling for one object-store HTTP request. Production
    /// clients normally use the transport defaults; health probes set this so
    /// an unreachable endpoint cannot monopolize an API worker.
    request_timeout_ms: ?u64 = null,
    /// Optional admission hook for the exact resolved endpoint address. This
    /// is evaluated immediately before connect and therefore remains safe
    /// against DNS rebinding.
    resolved_address_validator: ?httpx.ResolvedAddressValidator = null,
    resolved_address_validator_context: ?*anyopaque = null,
    /// Optional borrowed application runtime. When absent, the client owns a
    /// threaded fallback for standalone library use.
    io: ?std.Io = null,

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

pub const DynamicCredentials = struct {
    pub const Borrowed = struct {
        ctx: *anyopaque,
        release: *const fn (*anyopaque) void,
    };

    pub const Ownership = union(enum) {
        owned,
        borrowed: Borrowed,
    };

    access_key_id: []u8,
    secret_access_key: []u8,
    session_token: ?[]u8 = null,
    ownership: Ownership = .owned,

    pub fn deinit(self: *DynamicCredentials, alloc: Allocator) void {
        switch (self.ownership) {
            .owned => {
                alloc.free(self.access_key_id);
                alloc.free(self.secret_access_key);
                if (self.session_token) |value| alloc.free(value);
            },
            .borrowed => |borrowed| borrowed.release(borrowed.ctx),
        }
        self.* = undefined;
    }
};

pub const CredentialProvider = struct {
    ptr: *anyopaque,
    get_fn: *const fn (*anyopaque, Allocator) anyerror!DynamicCredentials,

    pub fn get(self: CredentialProvider, alloc: Allocator) !DynamicCredentials {
        return try self.get_fn(self.ptr, alloc);
    }
};

pub const HeaderPair = [2][]const u8;

pub const HttpMethod = enum {
    GET,
    POST,
    PUT,
    DELETE,
    HEAD,

    fn toHttpx(self: HttpMethod) httpx.Method {
        return switch (self) {
            .GET => .GET,
            .POST => .POST,
            .PUT => .PUT,
            .DELETE => .DELETE,
            .HEAD => .HEAD,
        };
    }

    fn asBytes(self: HttpMethod) []const u8 {
        return switch (self) {
            .GET => "GET",
            .POST => "POST",
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

const RequestFn = *const fn (?*anyopaque, Allocator, HttpMethod, []const u8, []const HeaderPair, ?[]const u8, ?[]const u8, ?usize) anyerror!TransportResponse;

const HttpxTransport = struct {
    alloc: Allocator,
    io_impl: ?*std.Io.Threaded,
    client: httpx.Client,

    fn init(
        alloc: Allocator,
        request_timeout_ms: ?u64,
        shared_io: ?std.Io,
        resolved_address_validator: ?httpx.ResolvedAddressValidator,
        resolved_address_validator_context: ?*anyopaque,
    ) !HttpxTransport {
        const io_impl: ?*std.Io.Threaded = if (shared_io == null) blk: {
            const owned = try alloc.create(std.Io.Threaded);
            owned.* = std.Io.Threaded.init(alloc, .{});
            break :blk owned;
        } else null;
        errdefer if (io_impl) |owned| {
            owned.deinit();
            alloc.destroy(owned);
        };
        var client_config: httpx.ClientConfig = if (request_timeout_ms) |timeout_ms| .{
            .timeouts = .{
                .connect_ms = timeout_ms,
                .read_ms = timeout_ms,
                .write_ms = timeout_ms,
                .keep_alive_ms = timeout_ms,
                .idle_ms = timeout_ms,
                .request_ms = timeout_ms,
            },
        } else .{};
        // S3 authorization is signed for one exact authority and path. Never
        // replay it across an HTTP redirect: doing so would bypass the
        // caller's endpoint admission policy and expose the signed request (and
        // a temporary-session token, when present) to the redirect target.
        client_config.redirect_policy = httpx.RedirectPolicy.noFollow();
        client_config.cookies_enabled = false;
        client_config.resolved_address_validator = resolved_address_validator;
        client_config.resolved_address_validator_context = resolved_address_validator_context;
        return .{
            .alloc = alloc,
            .io_impl = io_impl,
            .client = httpx.Client.initWithConfig(alloc, shared_io orelse io_impl.?.io(), client_config),
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
            .max_response_size = max_response_size,
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

test "s3 http transport borrows a shared io runtime" {
    const alloc = std.testing.allocator;
    var shared = std.Io.Threaded.init(alloc, .{});
    defer shared.deinit();
    var transport = try HttpxTransport.init(alloc, null, shared.io(), null, null);
    defer transport.deinit();
    try std.testing.expect(transport.io_impl == null);
    try std.testing.expect(!transport.client.config.redirect_policy.follow_redirects);
    try std.testing.expect(!transport.client.config.cookies_enabled);

    var fallback = try HttpxTransport.init(alloc, null, null, null, null);
    defer fallback.deinit();
    try std.testing.expect(fallback.io_impl != null);
    try std.testing.expect(fallback.client.io.userdata == @as(?*anyopaque, @ptrCast(fallback.io_impl.?)));
}

pub const Client = struct {
    alloc: Allocator,
    cfg: Config,
    request_ctx: ?*anyopaque,
    request_fn: RequestFn,
    owned_httpx: ?*HttpxTransport,

    pub fn init(alloc: Allocator, cfg: Config) !Client {
        const transport = try alloc.create(HttpxTransport);
        errdefer alloc.destroy(transport);
        transport.* = try HttpxTransport.init(
            alloc,
            cfg.request_timeout_ms,
            cfg.io,
            cfg.resolved_address_validator,
            cfg.resolved_address_validator_context,
        );
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
            200, 204 => true,
            301 => error.BucketRegionMismatch,
            401, 403 => error.AccessDenied,
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

    fn putFile(
        self: *Client,
        alloc: Allocator,
        io: std.Io,
        bucket: []const u8,
        key: []const u8,
        src_path: []const u8,
        opts: types.PutOptions,
    ) !types.PutResult {
        return try self.putFileWithThreshold(alloc, io, bucket, key, src_path, opts, multipart_upload_threshold);
    }

    fn putFileWithThreshold(
        self: *Client,
        alloc: Allocator,
        io: std.Io,
        bucket: []const u8,
        key: []const u8,
        src_path: []const u8,
        opts: types.PutOptions,
        multipart_threshold: u64,
    ) !types.PutResult {
        const source = try openFilePath(io, src_path);
        defer source.close(io);
        const stat = try source.stat(io);
        if (stat.size <= multipart_threshold) {
            const body = try alloc.alloc(u8, @intCast(stat.size));
            defer alloc.free(body);
            if (try source.readPositionalAll(io, body, 0) != body.len) return error.SourceFileChanged;
            var extra: [1]u8 = undefined;
            if (try source.readPositionalAll(io, &extra, stat.size) != 0) return error.SourceFileChanged;
            const current_stat = try source.stat(io);
            if (current_stat.size != stat.size or !std.meta.eql(current_stat.mtime, stat.mtime)) return error.SourceFileChanged;
            return try self.putObject(alloc, bucket, key, body, opts);
        }
        if (opts.if_match_etag != null or opts.if_none_match) return error.ConditionalMultipartUnsupported;
        const required_part_bytes = ((stat.size - 1) / max_multipart_parts) + 1;
        const aligned_part_bytes = std.mem.alignForward(u64, required_part_bytes, multipart_upload_part_alignment);
        const part_bytes = @max(multipart_upload_min_part_bytes, aligned_part_bytes);
        if (part_bytes > multipart_upload_max_part_bytes) return error.ObjectTooLarge;
        const part_count = ((stat.size - 1) / part_bytes) + 1;
        if (part_count > max_multipart_parts) return error.ObjectTooLarge;

        var initiate_query = std.ArrayListUnmanaged(QueryPair).empty;
        errdefer deinitQueryList(alloc, &initiate_query);
        try appendQueryPair(alloc, &initiate_query, "uploads", "");
        const initiate_pairs = try initiate_query.toOwnedSlice(alloc);
        defer freeQueryPairs(alloc, initiate_pairs);
        var initiate_target = try objectTargetAllocWithQuery(alloc, self.cfg, bucket, key, initiate_pairs);
        defer initiate_target.deinit(alloc);
        var initiated = try self.perform(.POST, initiate_target, &.{}, null, opts.content_type);
        defer initiated.deinit(alloc);
        if (initiated.status != 200) return unexpectedStatusError(initiated.status);
        const upload_id = try requiredTagAlloc(alloc, initiated.body, "UploadId");
        defer alloc.free(upload_id);

        var completed = false;
        defer if (!completed) self.abortMultipartUpload(bucket, key, upload_id) catch {};
        var etags = std.ArrayListUnmanaged([]u8).empty;
        defer {
            for (etags.items) |etag| alloc.free(etag);
            etags.deinit(alloc);
        }
        const buffer = try alloc.alloc(u8, @intCast(part_bytes));
        defer alloc.free(buffer);
        var offset: u64 = 0;
        var part_number: u32 = 1;
        while (offset < stat.size) : (part_number += 1) {
            const wanted: usize = @intCast(@min(stat.size - offset, buffer.len));
            if (try source.readPositionalAll(io, buffer[0..wanted], offset) != wanted) return error.SourceFileChanged;
            const current_stat = try source.stat(io);
            if (current_stat.size != stat.size or !std.meta.eql(current_stat.mtime, stat.mtime)) return error.SourceFileChanged;
            const part_number_text = try std.fmt.allocPrint(alloc, "{d}", .{part_number});
            defer alloc.free(part_number_text);
            var query = std.ArrayListUnmanaged(QueryPair).empty;
            errdefer deinitQueryList(alloc, &query);
            try appendQueryPair(alloc, &query, "partNumber", part_number_text);
            try appendQueryPair(alloc, &query, "uploadId", upload_id);
            const query_pairs = try query.toOwnedSlice(alloc);
            defer freeQueryPairs(alloc, query_pairs);
            var target = try objectTargetAllocWithQuery(alloc, self.cfg, bucket, key, query_pairs);
            defer target.deinit(alloc);
            var response = try self.perform(.PUT, target, &.{}, buffer[0..wanted], null);
            defer response.deinit(alloc);
            if (response.status != 200) return unexpectedStatusError(response.status);
            const etag = response.etag orelse return error.MissingMultipartEtag;
            try etags.append(alloc, try alloc.dupe(u8, etag));
            offset += wanted;
        }
        var extra: [1]u8 = undefined;
        if (try source.readPositionalAll(io, &extra, offset) != 0) return error.SourceFileChanged;

        const completion_xml = try completeMultipartXmlAlloc(alloc, etags.items);
        defer alloc.free(completion_xml);
        var complete_query = std.ArrayListUnmanaged(QueryPair).empty;
        errdefer deinitQueryList(alloc, &complete_query);
        try appendQueryPair(alloc, &complete_query, "uploadId", upload_id);
        const complete_pairs = try complete_query.toOwnedSlice(alloc);
        defer freeQueryPairs(alloc, complete_pairs);
        var complete_target = try objectTargetAllocWithQuery(alloc, self.cfg, bucket, key, complete_pairs);
        defer complete_target.deinit(alloc);
        var response = try self.perform(.POST, complete_target, &.{}, completion_xml, "application/xml");
        defer response.deinit(alloc);
        if (response.status != 200) return unexpectedStatusError(response.status);
        if (findBlock(response.body, "Error", 0) != null) return error.MultipartCompletionFailed;
        completed = true;
        const result_etag = if (response.etag) |value|
            try alloc.dupe(u8, stripQuotes(value))
        else if (try optionalTagAlloc(alloc, response.body, "ETag")) |value| blk: {
            defer alloc.free(value);
            break :blk try alloc.dupe(u8, stripQuotes(value));
        } else null;
        return .{
            .etag = result_etag,
            .version_id = if (response.version_id) |value| try alloc.dupe(u8, value) else null,
        };
    }

    fn abortMultipartUpload(self: *Client, bucket: []const u8, key: []const u8, upload_id: []const u8) !void {
        var query = std.ArrayListUnmanaged(QueryPair).empty;
        errdefer deinitQueryList(self.alloc, &query);
        try appendQueryPair(self.alloc, &query, "uploadId", upload_id);
        const query_pairs = try query.toOwnedSlice(self.alloc);
        defer freeQueryPairs(self.alloc, query_pairs);
        var target = try objectTargetAllocWithQuery(self.alloc, self.cfg, bucket, key, query_pairs);
        defer target.deinit(self.alloc);
        var response = try self.perform(.DELETE, target, &.{}, null, null);
        defer response.deinit(self.alloc);
        if (response.status != 200 and response.status != 204 and response.status != 404)
            return unexpectedStatusError(response.status);
    }

    fn getObject(
        self: *Client,
        alloc: Allocator,
        bucket: []const u8,
        key: []const u8,
        opts: types.GetOptions,
    ) !types.GetResult {
        var meta = if (opts.skip_metadata_probe) blk: {
            const owned_bucket = try alloc.dupe(u8, bucket);
            errdefer alloc.free(owned_bucket);
            const owned_key = try alloc.dupe(u8, key);
            break :blk types.ObjectMetadata{
                .bucket = owned_bucket,
                .key = owned_key,
                .content_length = 0,
            };
        } else try self.statObject(alloc, bucket, key);
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

        var response = try self.performWithResponseLimit(
            .GET,
            target,
            headers.items,
            null,
            null,
            opts.max_response_bytes,
        );
        errdefer response.deinit(alloc);
        switch (response.status) {
            200, 206 => {},
            304, 412 => return error.PreconditionFailed,
            404 => return error.FileNotFound,
            416 => return error.InvalidRange,
            else => return unexpectedStatusError(response.status),
        }

        meta.content_length = @intCast(response.body.len);
        if (opts.skip_metadata_probe) {
            if (response.etag) |value| meta.etag = try alloc.dupe(u8, stripQuotes(value));
        }
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
        return try self.performWithResponseLimit(method, target, headers, body, content_type, null);
    }

    fn performWithResponseLimit(
        self: *Client,
        method: HttpMethod,
        target: RequestTarget,
        headers: []const HeaderPair,
        body: ?[]const u8,
        content_type: ?[]const u8,
        max_response_size: ?usize,
    ) !TransportResponse {
        var dynamic_credentials = if (self.cfg.credential_provider) |provider| try provider.get(self.alloc) else null;
        defer if (dynamic_credentials) |*credentials| credentials.deinit(self.alloc);
        var signing_config = self.cfg;
        if (dynamic_credentials) |credentials| {
            signing_config.credentials.access_key_id = credentials.access_key_id;
            signing_config.credentials.secret_access_key = credentials.secret_access_key;
            signing_config.credentials.session_token = credentials.session_token;
        }
        const timestamp = currentUnixSeconds();
        const payload_hash = try sha256HexAlloc(self.alloc, body orelse "");
        defer self.alloc.free(payload_hash);

        const amz_date = try formatAmzDateAlloc(self.alloc, timestamp);
        defer self.alloc.free(amz_date);
        const scope_date = try formatScopeDateAlloc(self.alloc, timestamp);
        defer self.alloc.free(scope_date);

        const signed = try signHeadersAlloc(
            self.alloc,
            signing_config,
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

        return try self.request_fn(
            self.request_ctx,
            self.alloc,
            method,
            target.url,
            signed,
            body,
            content_type,
            max_response_size,
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

    fn erasedPutFile(ptr: *anyopaque, alloc: Allocator, io: std.Io, bucket: []const u8, key: []const u8, src_path: []const u8, opts: types.PutOptions) !types.PutResult {
        const self: *Client = @ptrCast(@alignCast(ptr));
        return try self.putFile(alloc, io, bucket, key, src_path, opts);
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
    errdefer deinitQueryList(alloc, &query);

    if (version_id) |value| try appendQueryPair(alloc, &query, "versionId", value);
    if (part_number) |value| {
        const encoded = try std.fmt.allocPrint(alloc, "{d}", .{value});
        defer alloc.free(encoded);
        try appendQueryPair(alloc, &query, "partNumber", encoded);
    }
    return try query.toOwnedSlice(alloc);
}

fn buildDeleteQueryAlloc(alloc: Allocator, version_id: ?[]const u8) ![]QueryPair {
    var query = std.ArrayListUnmanaged(QueryPair).empty;
    errdefer deinitQueryList(alloc, &query);
    if (version_id) |value| try appendQueryPair(alloc, &query, "versionId", value);
    return try query.toOwnedSlice(alloc);
}

fn buildListQueryAlloc(alloc: Allocator, opts: types.ListOptions) ![]QueryPair {
    var query = std.ArrayListUnmanaged(QueryPair).empty;
    errdefer deinitQueryList(alloc, &query);

    try appendQueryPair(alloc, &query, "list-type", "2");
    if (opts.prefix.len > 0) try appendQueryPair(alloc, &query, "prefix", opts.prefix);
    if (!opts.recursive and opts.delimiter.len > 0) try appendQueryPair(alloc, &query, "delimiter", opts.delimiter);
    if (opts.start_after) |value| try appendQueryPair(alloc, &query, "start-after", value);
    if (opts.continuation_token) |value| try appendQueryPair(alloc, &query, "continuation-token", value);
    if (opts.max_keys != 1000) {
        const value = try std.fmt.allocPrint(alloc, "{d}", .{opts.max_keys});
        defer alloc.free(value);
        try appendQueryPair(alloc, &query, "max-keys", value);
    }
    return try query.toOwnedSlice(alloc);
}

fn appendQueryPair(alloc: Allocator, list: *std.ArrayListUnmanaged(QueryPair), name: []const u8, value: []const u8) !void {
    const owned_name = try alloc.dupe(u8, name);
    errdefer alloc.free(owned_name);
    const owned_value = try alloc.dupe(u8, value);
    errdefer alloc.free(owned_value);
    try list.append(alloc, .{ .name = owned_name, .value = owned_value });
}

fn cloneQueryPairsAlloc(alloc: Allocator, pairs: []const QueryPair) ![]QueryPair {
    const out = try alloc.alloc(QueryPair, pairs.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |pair| {
            alloc.free(pair.name);
            alloc.free(pair.value);
        }
        alloc.free(out);
    }
    for (pairs, 0..) |pair, idx| {
        const name = try alloc.dupe(u8, pair.name);
        errdefer alloc.free(name);
        const value = try alloc.dupe(u8, pair.value);
        out[idx] = .{ .name = name, .value = value };
        initialized += 1;
    }
    return out;
}

fn deinitQueryList(alloc: Allocator, list: *std.ArrayListUnmanaged(QueryPair)) void {
    for (list.items) |pair| {
        alloc.free(pair.name);
        alloc.free(pair.value);
    }
    list.deinit(alloc);
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
    errdefer deinitHeaderList(alloc, &headers);

    try appendHeaderCopy(alloc, &headers, "Host", host);
    try appendHeaderCopy(alloc, &headers, "x-amz-date", amz_date);
    try appendHeaderCopy(alloc, &headers, "x-amz-content-sha256", payload_hash);
    if (cfg.credentials.session_token) |token| {
        try appendHeaderCopy(alloc, &headers, "x-amz-security-token", token);
    }
    if (content_type) |value| {
        try appendHeaderCopy(alloc, &headers, "Content-Type", value);
    }
    for (extra_headers) |pair| {
        try appendHeaderCopy(alloc, &headers, pair[0], pair[1]);
    }

    {
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
        const authorization_name = try alloc.dupe(u8, "Authorization");
        errdefer alloc.free(authorization_name);
        try headers.append(alloc, .{ authorization_name, signature });
    }
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
    var initialized: usize = 0;
    errdefer {
        for (entries[0..initialized]) |entry| {
            alloc.free(entry.name);
            alloc.free(entry.value);
        }
        alloc.free(entries);
    }

    for (headers, 0..) |pair, idx| {
        const name = try asciiLowerAlloc(alloc, std.mem.trim(u8, pair[0], " "));
        errdefer alloc.free(name);
        const value = try alloc.dupe(u8, std.mem.trim(u8, pair[1], " "));
        entries[idx] = .{ .name = name, .value = value };
        initialized += 1;
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

    const header_block = try block.toOwnedSlice(alloc);
    errdefer alloc.free(header_block);
    const signed_headers = try signed.toOwnedSlice(alloc);
    return .{
        .entries = entries,
        .header_block = header_block,
        .signed_headers = signed_headers,
    };
}

fn canonicalQueryStringAlloc(alloc: Allocator, pairs: []const QueryPair) ![]u8 {
    const encoded = try alloc.alloc(QueryPair, pairs.len);
    var initialized: usize = 0;
    errdefer {
        for (encoded[0..initialized]) |pair| {
            alloc.free(pair.name);
            alloc.free(pair.value);
        }
        alloc.free(encoded);
    }
    for (pairs, 0..) |pair, idx| {
        const name = try encodeUriComponentAlloc(alloc, pair.name, true);
        errdefer alloc.free(name);
        const value = try encodeUriComponentAlloc(alloc, pair.value, true);
        encoded[idx] = .{ .name = name, .value = value };
        initialized += 1;
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
    const result = try out.toOwnedSlice(alloc);
    for (encoded) |pair| {
        alloc.free(pair.name);
        alloc.free(pair.value);
    }
    alloc.free(encoded);
    return result;
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
    const now = std.Io.Timestamp.now(io_impl.io(), .real);
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
            @intFromEnum(month_day.month),
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
            @intFromEnum(month_day.month),
            month_day.day_index + 1,
        },
    );
}

fn asciiLowerAlloc(alloc: Allocator, input: []const u8) ![]u8 {
    const out = try alloc.dupe(u8, input);
    _ = std.ascii.lowerString(out, out);
    return out;
}

test "s3 signing timestamp uses Unix wall clock" {
    var io_impl = std.Io.Threaded.init(std.testing.allocator, .{});
    defer io_impl.deinit();

    const before: u64 = @intCast(std.Io.Timestamp.now(io_impl.io(), .real).toSeconds());
    const actual = currentUnixSeconds();
    const after: u64 = @intCast(std.Io.Timestamp.now(io_impl.io(), .real).toSeconds());

    try std.testing.expect(actual >= before);
    try std.testing.expect(actual <= after);
}

test "s3 signing dates use calendar month numbers" {
    const alloc = std.testing.allocator;
    const amz_date = try formatAmzDateAlloc(alloc, 0);
    defer alloc.free(amz_date);
    const scope_date = try formatScopeDateAlloc(alloc, 0);
    defer alloc.free(scope_date);

    try std.testing.expectEqualStrings("19700101T000000Z", amz_date);
    try std.testing.expectEqualStrings("19700101", scope_date);
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

fn deinitHeaderList(alloc: Allocator, headers: *std.ArrayListUnmanaged(HeaderPair)) void {
    for (headers.items) |pair| {
        alloc.free(pair[0]);
        alloc.free(pair[1]);
    }
    headers.deinit(alloc);
}

fn appendHeaderCopy(alloc: Allocator, headers: *std.ArrayListUnmanaged(HeaderPair), name: []const u8, value: []const u8) !void {
    const owned_name = try alloc.dupe(u8, name);
    errdefer alloc.free(owned_name);
    const owned_value = try alloc.dupe(u8, value);
    errdefer alloc.free(owned_value);
    try headers.append(alloc, .{ owned_name, owned_value });
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
        const size_raw = try requiredTagAlloc(alloc, block.inner, "Size");
        defer alloc.free(size_raw);
        const size = try std.fmt.parseInt(u64, size_raw, 10);
        const key = try decodeXmlAlloc(alloc, block.inner, "Key");
        const etag_raw = optionalTagAlloc(alloc, block.inner, "ETag") catch |err| {
            alloc.free(key);
            return err;
        };
        defer if (etag_raw) |value| alloc.free(value);
        const etag = if (etag_raw) |value| alloc.dupe(u8, stripQuotes(value)) catch |err| {
            alloc.free(key);
            return err;
        } else null;
        var entry = types.ListEntry{
            .key = key,
            .etag = etag,
            .size = size,
            .last_modified_unix_ms = null,
        };
        entries.append(alloc, entry) catch |err| {
            entry.deinit(alloc);
            return err;
        };
    }

    search_from = 0;
    while (findBlock(xml, "CommonPrefixes", search_from)) |block| {
        search_from = block.end;
        const prefix = try decodeXmlAlloc(alloc, block.inner, "Prefix");
        prefixes.append(alloc, prefix) catch |err| {
            alloc.free(prefix);
            return err;
        };
    }

    const owned_entries = try entries.toOwnedSlice(alloc);
    errdefer {
        for (owned_entries) |*entry| entry.deinit(alloc);
        alloc.free(owned_entries);
    }
    const owned_prefixes = try prefixes.toOwnedSlice(alloc);
    errdefer {
        for (owned_prefixes) |prefix| alloc.free(prefix);
        alloc.free(owned_prefixes);
    }
    const next_token = try optionalTagAlloc(alloc, xml, "NextContinuationToken");
    return .{
        .entries = owned_entries,
        .common_prefixes = owned_prefixes,
        .next_continuation_token = next_token,
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

fn completeMultipartXmlAlloc(alloc: Allocator, etags: []const []u8) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try out.appendSlice(alloc, "<CompleteMultipartUpload>");
    for (etags, 1..) |etag, part_number| {
        try out.appendSlice(alloc, "<Part><PartNumber>");
        var number_buf: [16]u8 = undefined;
        const number = try std.fmt.bufPrint(&number_buf, "{d}", .{part_number});
        try out.appendSlice(alloc, number);
        try out.appendSlice(alloc, "</PartNumber><ETag>");
        try appendXmlEscaped(alloc, &out, etag);
        try out.appendSlice(alloc, "</ETag></Part>");
    }
    try out.appendSlice(alloc, "</CompleteMultipartUpload>");
    return try out.toOwnedSlice(alloc);
}

fn appendXmlEscaped(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), value: []const u8) !void {
    for (value) |byte| switch (byte) {
        '&' => try out.appendSlice(alloc, "&amp;"),
        '<' => try out.appendSlice(alloc, "&lt;"),
        '>' => try out.appendSlice(alloc, "&gt;"),
        '"' => try out.appendSlice(alloc, "&quot;"),
        '\'' => try out.appendSlice(alloc, "&apos;"),
        else => try out.append(alloc, byte),
    };
}

fn openFilePath(io: std.Io, path: []const u8) !std.Io.File {
    return if (std.fs.path.isAbsolute(path))
        try std.Io.Dir.openFileAbsolute(io, path, .{})
    else
        try std.Io.Dir.cwd().openFile(io, path, .{});
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

test "s3 multipart completion preserves ordered quoted etags" {
    const alloc = std.testing.allocator;
    const etags = [_][]u8{ @constCast("\"etag-one\""), @constCast("\"etag-two\"") };
    const xml = try completeMultipartXmlAlloc(alloc, &etags);
    defer alloc.free(xml);
    try std.testing.expectEqualStrings(
        "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>&quot;etag-one&quot;</ETag></Part><Part><PartNumber>2</PartNumber><ETag>&quot;etag-two&quot;</ETag></Part></CompleteMultipartUpload>",
        xml,
    );
}

test "s3 query and signing builders clean up every allocation failure" {
    const Runner = struct {
        fn run(alloc: Allocator) !void {
            const query = try buildListQueryAlloc(alloc, .{
                .prefix = "backup/",
                .continuation_token = "cursor",
                .max_keys = 17,
            });
            defer freeQueryPairs(alloc, query);
            const cfg = Config{
                .credentials = .{
                    .endpoint = @constCast("example.invalid"),
                    .use_ssl = true,
                    .access_key_id = @constCast("key"),
                    .secret_access_key = @constCast("secret"),
                    .session_token = @constCast("token"),
                    .region = @constCast("us-east-1"),
                },
                .addressing_style = .path,
            };
            const headers = try signHeadersAlloc(
                alloc,
                cfg,
                .GET,
                "example.invalid",
                "/bucket/key",
                query,
                &.{.{ "If-Match", "etag" }},
                "00",
                "20260712T000000Z",
                "20260712",
                "application/octet-stream",
            );
            defer freeHeaderPairs(alloc, headers);
            var listed = try parseListResponse(
                alloc,
                "<ListBucketResult><Contents><Key>backup/a</Key><ETag>\"a\"</ETag><Size>1</Size></Contents><Contents><Key>backup/b</Key><ETag>\"b\"</ETag><Size>2</Size></Contents><CommonPrefixes><Prefix>backup/nested/</Prefix></CommonPrefixes><NextContinuationToken>next</NextContinuationToken></ListBucketResult>",
            );
            defer listed.deinit(alloc);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Runner.run, .{});
}

test "s3 file upload completes a multipart lifecycle with bounded parts" {
    const alloc = std.testing.allocator;
    const io = std.testing.io;
    const source_path = try std.fmt.allocPrint(alloc, "/tmp/antfly-s3-multipart-{d}", .{test_support.integrationNonce()});
    defer alloc.free(source_path);
    defer std.Io.Dir.deleteFileAbsolute(io, source_path) catch {};
    {
        var source = try std.Io.Dir.createFileAbsolute(io, source_path, .{ .truncate = true });
        defer source.close(io);
        try source.writePositionalAll(io, "multipart-payload", 0);
        try source.sync(io);
    }

    const State = struct {
        calls: usize = 0,

        fn request(ctx: ?*anyopaque, request_alloc: Allocator, method: HttpMethod, url: []const u8, _: []const HeaderPair, body: ?[]const u8, _: ?[]const u8, _: ?usize) !TransportResponse {
            const self: *@This() = @ptrCast(@alignCast(ctx.?));
            defer self.calls += 1;
            return switch (self.calls) {
                0 => blk: {
                    try std.testing.expectEqual(HttpMethod.POST, method);
                    try std.testing.expect(std.mem.endsWith(u8, url, "?uploads="));
                    break :blk .{ .status = 200, .body = try request_alloc.dupe(u8, "<InitiateMultipartUploadResult><UploadId>upload-1</UploadId></InitiateMultipartUploadResult>") };
                },
                1 => blk: {
                    try std.testing.expectEqual(HttpMethod.PUT, method);
                    try std.testing.expect(std.mem.indexOf(u8, url, "partNumber=1&uploadId=upload-1") != null);
                    try std.testing.expectEqualStrings("multipart-payload", body.?);
                    break :blk .{ .status = 200, .body = try request_alloc.alloc(u8, 0), .etag = try request_alloc.dupe(u8, "\"part-1\"") };
                },
                2 => blk: {
                    try std.testing.expectEqual(HttpMethod.POST, method);
                    try std.testing.expect(std.mem.endsWith(u8, url, "?uploadId=upload-1"));
                    try std.testing.expect(std.mem.indexOf(u8, body.?, "<PartNumber>1</PartNumber>") != null);
                    break :blk .{ .status = 200, .body = try request_alloc.dupe(u8, "<CompleteMultipartUploadResult><ETag>\"final-etag\"</ETag></CompleteMultipartUploadResult>") };
                },
                else => error.UnexpectedCall,
            };
        }
    };
    const cfg = Config{
        .credentials = .{
            .endpoint = try alloc.dupe(u8, "127.0.0.1:9000"),
            .use_ssl = false,
            .access_key_id = try alloc.dupe(u8, "key"),
            .secret_access_key = try alloc.dupe(u8, "secret"),
            .region = try alloc.dupe(u8, "us-east-1"),
        },
        .addressing_style = .path,
    };
    var state = State{};
    var s3_client = Client.initWithRequestFn(alloc, cfg, &state, State.request);
    var client = s3_client.client();
    defer client.deinit();
    var result = try s3_client.putFileWithThreshold(alloc, io, "bucket", "backup/segment", source_path, .{}, 0);
    defer result.deinit(alloc);
    try std.testing.expectEqualStrings("final-etag", result.etag.?);
    try std.testing.expectEqual(@as(usize, 3), state.calls);
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
        expect_max_response_size: ?usize = null,
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
            max_response_size: ?usize,
        ) !TransportResponse {
            const self: *@This() = @ptrCast(@alignCast(ctx.?));
            defer self.index += 1;
            const step = self.steps[self.index];
            try std.testing.expectEqual(step.method, method);
            try std.testing.expect(std.mem.indexOf(u8, url, step.url_contains) != null);
            try expectHeader(headers, "Authorization");
            try expectHeader(headers, "x-amz-date");
            try expectHeader(headers, "x-amz-content-sha256");
            try std.testing.expectEqual(step.expect_max_response_size, max_response_size);
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
        .{ .method = .GET, .url_contains = "/bucket/docs/a.txt", .status = 206, .body = "hell", .etag = "\"etag-direct\"", .content_type = "text/plain", .content_length = 4, .expect_max_response_size = 4 },
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

    var direct = try client.getObject("bucket", "docs/a.txt", .{
        .range = .{ .offset = 0, .length = 4 },
        .skip_metadata_probe = true,
        .max_response_bytes = 4,
    });
    defer direct.deinit(alloc);
    try std.testing.expectEqualStrings("hell", direct.body);
    try std.testing.expectEqualStrings("etag-direct", direct.metadata.etag.?);

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

test "s3 client refreshes dynamic credentials for every signed request" {
    const alloc = std.testing.allocator;

    const Provider = struct {
        calls: usize = 0,

        fn get(ptr: *anyopaque, request_alloc: Allocator) !DynamicCredentials {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            const access_key = try std.fmt.allocPrint(request_alloc, "rotating-key-{d}", .{self.calls});
            errdefer request_alloc.free(access_key);
            return .{
                .access_key_id = access_key,
                .secret_access_key = try request_alloc.dupe(u8, "rotating-secret"),
                .session_token = try request_alloc.dupe(u8, "rotating-session"),
            };
        }
    };
    const Fake = struct {
        requests: usize = 0,

        fn request(
            ptr: ?*anyopaque,
            request_alloc: Allocator,
            _: HttpMethod,
            _: []const u8,
            headers: []const HeaderPair,
            _: ?[]const u8,
            _: ?[]const u8,
            _: ?usize,
        ) !TransportResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr.?));
            self.requests += 1;
            const expected = try std.fmt.allocPrint(request_alloc, "Credential=rotating-key-{d}/", .{self.requests});
            defer request_alloc.free(expected);
            var found = false;
            for (headers) |header| {
                if (std.ascii.eqlIgnoreCase(header[0], "Authorization") and std.mem.indexOf(u8, header[1], expected) != null) {
                    found = true;
                    break;
                }
            }
            try std.testing.expect(found);
            return .{ .status = 200, .body = try request_alloc.alloc(u8, 0) };
        }
    };

    var provider = Provider{};
    var fake = Fake{};
    const cfg = Config{
        .credentials = .{
            .endpoint = try alloc.dupe(u8, "s3.us-west-2.amazonaws.com"),
            .use_ssl = true,
            .access_key_id = try alloc.dupe(u8, "placeholder"),
            .secret_access_key = try alloc.dupe(u8, "placeholder"),
            .region = try alloc.dupe(u8, "us-west-2"),
        },
        .credential_provider = .{ .ptr = &provider, .get_fn = Provider.get },
    };
    var s3_client = Client.initWithRequestFn(alloc, cfg, &fake, Fake.request);
    var client = s3_client.client();
    defer client.deinit();

    try std.testing.expect(try client.bucketExists("bucket"));
    try std.testing.expect(try client.bucketExists("bucket"));
    try std.testing.expectEqual(@as(usize, 2), provider.calls);
    try std.testing.expectEqual(@as(usize, 2), fake.requests);
}

test "s3 bucket existence fails closed on access denied" {
    const alloc = std.testing.allocator;
    const Fake = struct {
        fn request(
            _: ?*anyopaque,
            request_alloc: Allocator,
            method: HttpMethod,
            _: []const u8,
            _: []const HeaderPair,
            _: ?[]const u8,
            _: ?[]const u8,
            _: ?usize,
        ) !TransportResponse {
            try std.testing.expectEqual(HttpMethod.HEAD, method);
            return .{ .status = 403, .body = try request_alloc.alloc(u8, 0) };
        }
    };
    const cfg = Config{
        .credentials = .{
            .endpoint = try alloc.dupe(u8, "s3.example.invalid"),
            .use_ssl = true,
            .access_key_id = try alloc.dupe(u8, "denied"),
            .secret_access_key = try alloc.dupe(u8, "denied"),
            .region = try alloc.dupe(u8, "us-east-1"),
        },
    };
    var s3_client = Client.initWithRequestFn(alloc, cfg, null, Fake.request);
    var client = s3_client.client();
    defer client.deinit();
    try std.testing.expectError(error.AccessDenied, client.bucketExists("private-bucket"));
}

test "borrowed dynamic credentials release through tagged ownership" {
    const alloc = std.testing.allocator;
    const Release = struct {
        fn call(ptr: *anyopaque) void {
            const released: *bool = @ptrCast(@alignCast(ptr));
            released.* = true;
        }
    };
    var released = false;
    var credentials = DynamicCredentials{
        .access_key_id = @constCast("borrowed-key"),
        .secret_access_key = @constCast("borrowed-secret"),
        .ownership = .{ .borrowed = .{ .ctx = &released, .release = Release.call } },
    };
    credentials.deinit(alloc);
    try std.testing.expect(released);
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
