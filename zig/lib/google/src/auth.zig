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

const Allocator = std.mem.Allocator;
const asn1_der = std.crypto.Certificate.der;

pub const default_scope = "https://www.googleapis.com/auth/devstorage.full_control";
pub const default_token_uri = "https://oauth2.googleapis.com/token";

pub const HttpMethod = enum {
    GET,
    POST,

    fn toHttpx(self: HttpMethod) httpx.Method {
        return switch (self) {
            .GET => .GET,
            .POST => .POST,
        };
    }
};

pub const HeaderPair = [2][]const u8;

pub const TransportResponse = struct {
    status: u16,
    body: []u8,
    content_type: ?[]u8 = null,

    pub fn deinit(self: *TransportResponse, alloc: Allocator) void {
        alloc.free(self.body);
        if (self.content_type) |value| alloc.free(value);
        self.* = undefined;
    }
};

const RequestFn = *const fn (?*anyopaque, Allocator, HttpMethod, []const u8, []const HeaderPair, ?[]const u8, ?[]const u8) anyerror!TransportResponse;

const HttpxTransport = struct {
    alloc: Allocator,
    io_impl: ?*std.Io.Threaded,
    client: httpx.Client,

    fn init(alloc: Allocator, shared_io: ?std.Io) !HttpxTransport {
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
            .client = httpx.Client.init(alloc, shared_io orelse io_impl.?.io()),
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
    ) !TransportResponse {
        const self: *HttpxTransport = @ptrCast(@alignCast(ctx.?));

        var request_headers = std.ArrayListUnmanaged(HeaderPair).empty;
        defer request_headers.deinit(alloc);
        try request_headers.appendSlice(alloc, headers);
        if (content_type) |value| try request_headers.append(alloc, .{ "Content-Type", value });

        var response = try self.client.request(method.toHttpx(), url, .{
            .headers = request_headers.items,
            .body = body,
        });
        defer response.deinit();

        return .{
            .status = response.status.code,
            .body = if (response.body) |value| try alloc.dupe(u8, value) else try alloc.alloc(u8, 0),
            .content_type = if (response.headers.get("Content-Type")) |value| try alloc.dupe(u8, value) else null,
        };
    }
};

test "google auth transport borrows a shared io runtime" {
    const alloc = std.testing.allocator;
    var shared = std.Io.Threaded.init(alloc, .{});
    defer shared.deinit();
    var transport = try HttpxTransport.init(alloc, shared.io());
    defer transport.deinit();
    try std.testing.expect(transport.io_impl == null);

    var fallback = try HttpxTransport.init(alloc, null);
    defer fallback.deinit();
    try std.testing.expect(fallback.io_impl != null);
    try std.testing.expect(fallback.client.io.userdata == @as(?*anyopaque, @ptrCast(fallback.io_impl.?)));
}

pub const ServiceAccount = struct {
    project_id: ?[]u8 = null,
    private_key_id: ?[]u8 = null,
    private_key_pem: []u8,
    client_email: []u8,
    token_uri: []u8,

    pub fn deinit(self: *ServiceAccount, alloc: Allocator) void {
        if (self.project_id) |value| alloc.free(value);
        if (self.private_key_id) |value| alloc.free(value);
        alloc.free(self.private_key_pem);
        alloc.free(self.client_email);
        alloc.free(self.token_uri);
        self.* = undefined;
    }
};

pub const AuthorizedUser = struct {
    client_id: []u8,
    client_secret: []u8,
    refresh_token: []u8,
    token_uri: []u8,
    quota_project_id: ?[]u8 = null,

    fn deinit(self: *AuthorizedUser, alloc: Allocator) void {
        alloc.free(self.client_id);
        alloc.free(self.client_secret);
        alloc.free(self.refresh_token);
        alloc.free(self.token_uri);
        if (self.quota_project_id) |value| alloc.free(value);
        self.* = undefined;
    }
};

pub const ExternalAccount = struct {
    audience: []u8,
    subject_token_type: []u8,
    token_url: []u8,
    subject_token_file: ?[]u8 = null,
    subject_token_url: ?[]u8 = null,
    subject_token_headers: []HeaderPair = &.{},
    subject_token_field: ?[]u8 = null,
    service_account_impersonation_url: ?[]u8 = null,
    quota_project_id: ?[]u8 = null,

    fn deinit(self: *ExternalAccount, alloc: Allocator) void {
        alloc.free(self.audience);
        alloc.free(self.subject_token_type);
        alloc.free(self.token_url);
        if (self.subject_token_file) |value| alloc.free(value);
        if (self.subject_token_url) |value| alloc.free(value);
        for (self.subject_token_headers) |header| {
            alloc.free(header[0]);
            alloc.free(header[1]);
        }
        if (self.subject_token_headers.len > 0) alloc.free(self.subject_token_headers);
        if (self.subject_token_field) |value| alloc.free(value);
        if (self.service_account_impersonation_url) |value| alloc.free(value);
        if (self.quota_project_id) |value| alloc.free(value);
        self.* = undefined;
    }
};

pub const MetadataCredentials = struct {
    token_url: []u8,
    project_id_url: []u8,

    fn deinit(self: *MetadataCredentials, alloc: Allocator) void {
        alloc.free(self.token_url);
        alloc.free(self.project_id_url);
        self.* = undefined;
    }
};

pub const Credentials = union(enum) {
    service_account: ServiceAccount,
    authorized_user: AuthorizedUser,
    external_account: ExternalAccount,
    metadata: MetadataCredentials,

    fn deinit(self: *Credentials, alloc: Allocator) void {
        switch (self.*) {
            inline else => |*value| value.deinit(alloc),
        }
        self.* = undefined;
    }
};

pub const Config = struct {
    credentials: Credentials,
    scope: []u8,

    pub fn deinit(self: *Config, alloc: Allocator) void {
        self.credentials.deinit(alloc);
        alloc.free(self.scope);
        self.* = undefined;
    }

    pub fn projectId(self: Config) ?[]const u8 {
        return switch (self.credentials) {
            .service_account => |account| account.project_id,
            .authorized_user, .external_account, .metadata => null,
        };
    }
};

pub const CachedTokenSource = struct {
    alloc: Allocator,
    cfg: Config,
    request_ctx: ?*anyopaque,
    request_fn: RequestFn,
    owned_httpx: ?*HttpxTransport,
    io: ?std.Io,
    mutex: std.Io.Mutex = .init,
    cached_token: ?AccessToken = null,

    pub fn init(alloc: Allocator, cfg: Config) !CachedTokenSource {
        return try initWithIo(alloc, cfg, null);
    }

    pub fn initWithIo(alloc: Allocator, cfg: Config, shared_io: ?std.Io) !CachedTokenSource {
        const transport = try alloc.create(HttpxTransport);
        errdefer alloc.destroy(transport);
        transport.* = try HttpxTransport.init(alloc, shared_io);
        return .{
            .alloc = alloc,
            .cfg = cfg,
            .request_ctx = transport,
            .request_fn = HttpxTransport.request,
            .owned_httpx = transport,
            .io = transport.client.io,
        };
    }

    pub fn initWithRequestFn(
        alloc: Allocator,
        cfg: Config,
        request_ctx: ?*anyopaque,
        request_fn: RequestFn,
    ) CachedTokenSource {
        return .{
            .alloc = alloc,
            .cfg = cfg,
            .request_ctx = request_ctx,
            .request_fn = request_fn,
            .owned_httpx = null,
            .io = null,
        };
    }

    pub fn deinit(self: *CachedTokenSource) void {
        if (self.owned_httpx) |transport| {
            transport.deinit();
            self.alloc.destroy(transport);
        }
        if (self.cached_token) |*token| token.deinit(self.alloc);
        self.cfg.deinit(self.alloc);
        self.* = undefined;
    }

    pub fn authorizationValueAlloc(self: *CachedTokenSource, alloc: Allocator) ![]u8 {
        const token = try self.accessTokenAlloc(alloc);
        defer alloc.free(token);
        return try std.fmt.allocPrint(alloc, "Bearer {s}", .{token});
    }

    pub fn accessTokenAlloc(self: *CachedTokenSource, alloc: Allocator) ![]u8 {
        const io = self.io;
        if (io) |runtime_io| self.mutex.lockUncancelable(runtime_io);
        defer if (io) |runtime_io| self.mutex.unlock(runtime_io);
        return try self.accessTokenAllocLocked(alloc);
    }

    fn accessTokenAllocLocked(self: *CachedTokenSource, alloc: Allocator) ![]u8 {
        const now = nowSeconds();
        var unexpired_fallback: ?[]const u8 = null;
        if (self.cached_token) |token| {
            if (token.expires_at_s > now + 30) return try alloc.dupe(u8, token.value);
            if (token.expires_at_s > now) unexpired_fallback = token.value;
        }

        var minted = self.mintTokenAlloc(self.alloc, now) catch |err| {
            // A proactive refresh failure must not turn a still-valid token
            // into an outage. Once expired, fail closed and surface the auth
            // provider error.
            if (unexpired_fallback) |token| return try alloc.dupe(u8, token);
            return err;
        };
        errdefer minted.deinit(self.alloc);

        if (self.cached_token) |*existing| existing.deinit(self.alloc);
        self.cached_token = minted;
        return try alloc.dupe(u8, self.cached_token.?.value);
    }

    fn mintTokenAlloc(self: *CachedTokenSource, alloc: Allocator, now_s: u64) !AccessToken {
        return switch (self.cfg.credentials) {
            .service_account => |account| try self.mintServiceAccountTokenAlloc(alloc, account, now_s),
            .authorized_user => |user| try self.mintAuthorizedUserTokenAlloc(alloc, user, now_s),
            .external_account => |account| try self.mintExternalAccountTokenAlloc(alloc, account, now_s),
            .metadata => |metadata| try self.mintMetadataTokenAlloc(alloc, metadata, now_s),
        };
    }

    fn mintServiceAccountTokenAlloc(self: *CachedTokenSource, alloc: Allocator, account: ServiceAccount, now_s: u64) !AccessToken {
        const assertion = try signedJwtAssertionAlloc(alloc, account, self.cfg.scope, now_s);
        defer alloc.free(assertion);

        const grant_type = try httpx.uri.encode(alloc, "urn:ietf:params:oauth:grant-type:jwt-bearer");
        defer alloc.free(grant_type);
        const assertion_encoded = try httpx.uri.encode(alloc, assertion);
        defer alloc.free(assertion_encoded);
        const body = try std.fmt.allocPrint(alloc, "grant_type={s}&assertion={s}", .{ grant_type, assertion_encoded });
        defer alloc.free(body);

        const headers = [_]HeaderPair{
            .{ "Accept", "application/json" },
        };
        var response = try self.request_fn(
            self.request_ctx,
            alloc,
            .POST,
            account.token_uri,
            &headers,
            body,
            "application/x-www-form-urlencoded",
        );
        defer response.deinit(alloc);

        switch (response.status) {
            200 => {},
            else => return error.UnexpectedHttpStatus,
        }

        const parsed = try std.json.parseFromSlice(TokenResponseBody, alloc, response.body, .{});
        defer parsed.deinit();

        return .{
            .value = try alloc.dupe(u8, parsed.value.access_token),
            .expires_at_s = now_s + parsed.value.expires_in -| 30,
        };
    }

    fn mintAuthorizedUserTokenAlloc(self: *CachedTokenSource, alloc: Allocator, user: AuthorizedUser, now_s: u64) !AccessToken {
        const client_id = try httpx.uri.encode(alloc, user.client_id);
        defer alloc.free(client_id);
        const client_secret = try httpx.uri.encode(alloc, user.client_secret);
        defer alloc.free(client_secret);
        const refresh_token = try httpx.uri.encode(alloc, user.refresh_token);
        defer alloc.free(refresh_token);
        const body = try std.fmt.allocPrint(
            alloc,
            "grant_type=refresh_token&client_id={s}&client_secret={s}&refresh_token={s}",
            .{ client_id, client_secret, refresh_token },
        );
        defer alloc.free(body);
        return try self.exchangeTokenRequestAlloc(alloc, user.token_uri, &.{.{ "Accept", "application/json" }}, body, now_s);
    }

    fn mintMetadataTokenAlloc(self: *CachedTokenSource, alloc: Allocator, metadata: MetadataCredentials, now_s: u64) !AccessToken {
        var response = try self.request_fn(
            self.request_ctx,
            alloc,
            .GET,
            metadata.token_url,
            &.{.{ "Metadata-Flavor", "Google" }},
            null,
            null,
        );
        defer response.deinit(alloc);
        if (response.status != 200) return error.UnexpectedHttpStatus;
        return try parseAccessTokenResponseAlloc(alloc, response.body, now_s);
    }

    fn mintExternalAccountTokenAlloc(self: *CachedTokenSource, alloc: Allocator, account: ExternalAccount, now_s: u64) !AccessToken {
        const subject_token = try self.externalSubjectTokenAlloc(alloc, account);
        defer alloc.free(subject_token);
        const audience = try httpx.uri.encode(alloc, account.audience);
        defer alloc.free(audience);
        const subject_token_type = try httpx.uri.encode(alloc, account.subject_token_type);
        defer alloc.free(subject_token_type);
        const subject = try httpx.uri.encode(alloc, subject_token);
        defer alloc.free(subject);
        const scope = try httpx.uri.encode(alloc, self.cfg.scope);
        defer alloc.free(scope);
        const body = try std.fmt.allocPrint(
            alloc,
            "grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Atoken-exchange&audience={s}&requested_token_type=urn%3Aietf%3Aparams%3Aoauth%3Atoken-type%3Aaccess_token&subject_token={s}&subject_token_type={s}&scope={s}",
            .{ audience, subject, subject_token_type, scope },
        );
        defer alloc.free(body);
        var federated = try self.exchangeTokenRequestAlloc(alloc, account.token_url, &.{.{ "Accept", "application/json" }}, body, now_s);
        if (account.service_account_impersonation_url == null) return federated;
        defer federated.deinit(alloc);

        const authorization = try std.fmt.allocPrint(alloc, "Bearer {s}", .{federated.value});
        defer alloc.free(authorization);
        const impersonation_body = try std.json.Stringify.valueAlloc(alloc, .{
            .scope = &.{self.cfg.scope},
            .lifetime = "3600s",
        }, .{});
        defer alloc.free(impersonation_body);
        var response = try self.request_fn(
            self.request_ctx,
            alloc,
            .POST,
            account.service_account_impersonation_url.?,
            &.{ .{ "Accept", "application/json" }, .{ "Authorization", authorization } },
            impersonation_body,
            "application/json",
        );
        defer response.deinit(alloc);
        if (response.status != 200) return error.UnexpectedHttpStatus;
        const ImpersonationResponse = struct { accessToken: []const u8 };
        const parsed = try std.json.parseFromSlice(ImpersonationResponse, alloc, response.body, .{ .ignore_unknown_fields = true });
        defer parsed.deinit();
        return .{ .value = try alloc.dupe(u8, parsed.value.accessToken), .expires_at_s = now_s + 3300 };
    }

    fn exchangeTokenRequestAlloc(
        self: *CachedTokenSource,
        alloc: Allocator,
        url: []const u8,
        headers: []const HeaderPair,
        body: []const u8,
        now_s: u64,
    ) !AccessToken {
        var response = try self.request_fn(self.request_ctx, alloc, .POST, url, headers, body, "application/x-www-form-urlencoded");
        defer response.deinit(alloc);
        if (response.status != 200) return error.UnexpectedHttpStatus;
        return try parseAccessTokenResponseAlloc(alloc, response.body, now_s);
    }

    fn externalSubjectTokenAlloc(self: *CachedTokenSource, alloc: Allocator, account: ExternalAccount) ![]u8 {
        const raw = if (account.subject_token_file) |path| blk: {
            const io = self.io orelse return error.MissingIoRuntime;
            break :blk try std.Io.Dir.cwd().readFileAlloc(io, path, alloc, .limited(1024 * 1024));
        } else if (account.subject_token_url) |url| blk: {
            var response = try self.request_fn(
                self.request_ctx,
                alloc,
                .GET,
                url,
                account.subject_token_headers,
                null,
                null,
            );
            defer response.deinit(alloc);
            if (response.status != 200) return error.UnexpectedHttpStatus;
            break :blk try alloc.dupe(u8, response.body);
        } else return error.MissingSubjectTokenSource;
        defer alloc.free(raw);
        if (account.subject_token_field) |field| {
            var parsed = try std.json.parseFromSlice(std.json.Value, alloc, raw, .{});
            defer parsed.deinit();
            if (parsed.value != .object) return error.InvalidSubjectToken;
            const value = parsed.value.object.get(field) orelse return error.InvalidSubjectToken;
            if (value != .string) return error.InvalidSubjectToken;
            return try alloc.dupe(u8, value.string);
        }
        return try alloc.dupe(u8, std.mem.trim(u8, raw, &std.ascii.whitespace));
    }
};

/// Runtime-owned cache for ADC token sources. A source is keyed by credential
/// identity and scope; each source already serializes refreshes and retains a
/// still-valid token when a proactive refresh transiently fails.
pub const CredentialManager = struct {
    alloc: Allocator,
    io: std.Io,
    mutex: std.Io.Mutex = .init,
    sources: std.StringHashMapUnmanaged(*CachedTokenSource) = .empty,

    pub fn init(alloc: Allocator, io: std.Io) CredentialManager {
        return .{ .alloc = alloc, .io = io };
    }

    pub fn deinit(self: *CredentialManager) void {
        self.mutex.lockUncancelable(self.io);
        var it = self.sources.iterator();
        while (it.next()) |entry| {
            entry.value_ptr.*.deinit();
            self.alloc.destroy(entry.value_ptr.*);
            self.alloc.free(entry.key_ptr.*);
        }
        self.sources.deinit(self.alloc);
        self.mutex.unlock(self.io);
        self.* = undefined;
    }

    pub fn tokenSource(
        self: *CredentialManager,
        credentials_path: ?[]const u8,
        scope: []const u8,
    ) !*CachedTokenSource {
        const identity = credentials_path orelse "<default-adc>";
        const key = try std.fmt.allocPrint(self.alloc, "{s}\x00{s}", .{ identity, scope });
        errdefer self.alloc.free(key);

        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        if (self.sources.get(key)) |source| {
            self.alloc.free(key);
            return source;
        }

        var cfg = if (credentials_path) |path|
            configFromFileAlloc(self.alloc, path, scope) catch return error.MissingGoogleCredentials
        else
            configFromEnvAlloc(self.alloc, scope) catch return error.MissingGoogleCredentials;
        errdefer cfg.deinit(self.alloc);
        const source = try self.alloc.create(CachedTokenSource);
        errdefer self.alloc.destroy(source);
        source.* = try CachedTokenSource.initWithIo(self.alloc, cfg, self.io);
        try self.sources.put(self.alloc, key, source);
        return source;
    }
};

test "google credential manager releases its mutex before invalidation" {
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    var manager = CredentialManager.init(std.testing.allocator, io_impl.io());
    manager.deinit();
}

const AccessToken = struct {
    value: []u8,
    expires_at_s: u64,

    fn deinit(self: *AccessToken, alloc: Allocator) void {
        alloc.free(self.value);
        self.* = undefined;
    }
};

const ServiceAccountJson = struct {
    type: ?[]const u8 = null,
    project_id: ?[]const u8 = null,
    private_key_id: ?[]const u8 = null,
    private_key: []const u8,
    client_email: []const u8,
    token_uri: ?[]const u8 = null,
};

const AuthorizedUserJson = struct {
    client_id: []const u8,
    client_secret: []const u8,
    refresh_token: []const u8,
    token_uri: ?[]const u8 = null,
    quota_project_id: ?[]const u8 = null,
};

const ExternalAccountJson = struct {
    audience: []const u8,
    subject_token_type: []const u8,
    token_url: []const u8,
    service_account_impersonation_url: ?[]const u8 = null,
    quota_project_id: ?[]const u8 = null,
    credential_source: struct {
        file: ?[]const u8 = null,
        url: ?[]const u8 = null,
        headers: ?std.json.Value = null,
        format: ?struct {
            type: ?[]const u8 = null,
            subject_token_field_name: ?[]const u8 = null,
        } = null,
    },
};

const TokenResponseBody = struct {
    access_token: []const u8,
    expires_in: u64,
    token_type: ?[]const u8 = null,
};

fn parseAccessTokenResponseAlloc(alloc: Allocator, body: []const u8, now_s: u64) !AccessToken {
    const parsed = try std.json.parseFromSlice(TokenResponseBody, alloc, body, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    return .{
        .value = try alloc.dupe(u8, parsed.value.access_token),
        .expires_at_s = now_s + parsed.value.expires_in -| 30,
    };
}

pub fn configFromServiceAccountAlloc(alloc: Allocator, service_account: ServiceAccount, scope: []const u8) !Config {
    return .{
        .credentials = .{ .service_account = service_account },
        .scope = try alloc.dupe(u8, scope),
    };
}

fn configFromAuthorizedUserJsonAlloc(alloc: Allocator, raw: []const u8, scope: []const u8) !Config {
    var parsed = try std.json.parseFromSlice(AuthorizedUserJson, alloc, raw, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    const client_id = try alloc.dupe(u8, parsed.value.client_id);
    errdefer alloc.free(client_id);
    const client_secret = try alloc.dupe(u8, parsed.value.client_secret);
    errdefer alloc.free(client_secret);
    const refresh_token = try alloc.dupe(u8, parsed.value.refresh_token);
    errdefer alloc.free(refresh_token);
    const token_uri = try alloc.dupe(u8, parsed.value.token_uri orelse default_token_uri);
    errdefer alloc.free(token_uri);
    const quota_project_id = if (parsed.value.quota_project_id) |value| try alloc.dupe(u8, value) else null;
    errdefer if (quota_project_id) |value| alloc.free(value);
    const owned_scope = try alloc.dupe(u8, scope);
    errdefer alloc.free(owned_scope);
    return .{
        .credentials = .{ .authorized_user = .{
            .client_id = client_id,
            .client_secret = client_secret,
            .refresh_token = refresh_token,
            .token_uri = token_uri,
            .quota_project_id = quota_project_id,
        } },
        .scope = owned_scope,
    };
}

fn configFromExternalAccountJsonAlloc(alloc: Allocator, raw: []const u8, scope: []const u8) !Config {
    var parsed = try std.json.parseFromSlice(ExternalAccountJson, alloc, raw, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    const source = parsed.value.credential_source;
    if ((source.file == null) == (source.url == null)) return error.InvalidExternalAccount;
    if (source.format) |format| {
        if (format.type) |format_type| {
            if (!std.mem.eql(u8, format_type, "text") and !std.mem.eql(u8, format_type, "json")) return error.InvalidExternalAccount;
        }
    }
    const audience = try alloc.dupe(u8, parsed.value.audience);
    errdefer alloc.free(audience);
    const subject_token_type = try alloc.dupe(u8, parsed.value.subject_token_type);
    errdefer alloc.free(subject_token_type);
    const token_url = try alloc.dupe(u8, parsed.value.token_url);
    errdefer alloc.free(token_url);
    const subject_token_file = if (source.file) |value| try alloc.dupe(u8, value) else null;
    errdefer if (subject_token_file) |value| alloc.free(value);
    const subject_token_url = if (source.url) |value| try alloc.dupe(u8, value) else null;
    errdefer if (subject_token_url) |value| alloc.free(value);
    const subject_token_headers = try externalAccountHeadersAlloc(alloc, source.headers);
    errdefer freeHeaderPairs(alloc, subject_token_headers);
    const subject_token_field = if (source.format) |format| if (format.subject_token_field_name) |value| try alloc.dupe(u8, value) else null else null;
    errdefer if (subject_token_field) |value| alloc.free(value);
    const impersonation_url = if (parsed.value.service_account_impersonation_url) |value| try alloc.dupe(u8, value) else null;
    errdefer if (impersonation_url) |value| alloc.free(value);
    const quota_project_id = if (parsed.value.quota_project_id) |value| try alloc.dupe(u8, value) else null;
    errdefer if (quota_project_id) |value| alloc.free(value);
    const owned_scope = try alloc.dupe(u8, scope);
    errdefer alloc.free(owned_scope);
    return .{
        .credentials = .{ .external_account = .{
            .audience = audience,
            .subject_token_type = subject_token_type,
            .token_url = token_url,
            .subject_token_file = subject_token_file,
            .subject_token_url = subject_token_url,
            .subject_token_headers = subject_token_headers,
            .subject_token_field = subject_token_field,
            .service_account_impersonation_url = impersonation_url,
            .quota_project_id = quota_project_id,
        } },
        .scope = owned_scope,
    };
}

fn externalAccountHeadersAlloc(alloc: Allocator, maybe_value: ?std.json.Value) ![]HeaderPair {
    const value = maybe_value orelse return &.{};
    if (value != .object) return error.InvalidExternalAccount;
    if (value.object.count() == 0) return &.{};
    const headers = try alloc.alloc(HeaderPair, value.object.count());
    errdefer alloc.free(headers);
    var initialized: usize = 0;
    errdefer for (headers[0..initialized]) |header| {
        alloc.free(header[0]);
        alloc.free(header[1]);
    };
    var it = value.object.iterator();
    while (it.next()) |entry| {
        if (entry.value_ptr.* != .string) return error.InvalidExternalAccount;
        headers[initialized] = try dupeHeaderPairAlloc(alloc, entry.key_ptr.*, entry.value_ptr.string);
        initialized += 1;
    }
    return headers;
}

fn dupeHeaderPairAlloc(alloc: Allocator, name: []const u8, value: []const u8) !HeaderPair {
    const owned_name = try alloc.dupe(u8, name);
    errdefer alloc.free(owned_name);
    return .{ owned_name, try alloc.dupe(u8, value) };
}

fn freeHeaderPairs(alloc: Allocator, headers: []HeaderPair) void {
    for (headers) |header| {
        alloc.free(header[0]);
        alloc.free(header[1]);
    }
    if (headers.len > 0) alloc.free(headers);
}

fn metadataConfigAlloc(alloc: Allocator, scope: []const u8) !Config {
    const host = (try envOwned(alloc, "GCE_METADATA_HOST")) orelse try alloc.dupe(u8, "metadata.google.internal");
    defer alloc.free(host);
    const token_url = try std.fmt.allocPrint(alloc, "http://{s}/computeMetadata/v1/instance/service-accounts/default/token", .{host});
    errdefer alloc.free(token_url);
    const project_id_url = try std.fmt.allocPrint(alloc, "http://{s}/computeMetadata/v1/project/project-id", .{host});
    errdefer alloc.free(project_id_url);
    const owned_scope = try alloc.dupe(u8, scope);
    errdefer alloc.free(owned_scope);
    return .{
        .credentials = .{ .metadata = .{
            .token_url = token_url,
            .project_id_url = project_id_url,
        } },
        .scope = owned_scope,
    };
}

pub fn parseServiceAccountJsonAlloc(alloc: Allocator, raw: []const u8) !ServiceAccount {
    var parsed = try std.json.parseFromSlice(ServiceAccountJson, alloc, raw, .{});
    defer parsed.deinit();
    const project_id = if (parsed.value.project_id) |value| try alloc.dupe(u8, value) else null;
    errdefer if (project_id) |value| alloc.free(value);
    const private_key_id = if (parsed.value.private_key_id) |value| try alloc.dupe(u8, value) else null;
    errdefer if (private_key_id) |value| alloc.free(value);
    const private_key_pem = try alloc.dupe(u8, parsed.value.private_key);
    errdefer alloc.free(private_key_pem);
    const client_email = try alloc.dupe(u8, parsed.value.client_email);
    errdefer alloc.free(client_email);
    const token_uri = try alloc.dupe(u8, parsed.value.token_uri orelse default_token_uri);
    return .{
        .project_id = project_id,
        .private_key_id = private_key_id,
        .private_key_pem = private_key_pem,
        .client_email = client_email,
        .token_uri = token_uri,
    };
}

pub fn serviceAccountFromFileAlloc(alloc: Allocator, path: []const u8) !ServiceAccount {
    return try serviceAccountFromFileAllocWithIo(alloc, path, null);
}

pub fn serviceAccountFromFileAllocWithIo(alloc: Allocator, path: []const u8, shared_io: ?std.Io) !ServiceAccount {
    var io_impl: ?std.Io.Threaded = if (shared_io == null) std.Io.Threaded.init(std.heap.page_allocator, .{}) else null;
    defer if (io_impl) |*owned| owned.deinit();
    const io = shared_io orelse io_impl.?.io();
    const raw = try std.Io.Dir.cwd().readFileAlloc(io, path, alloc, .limited(std.math.maxInt(usize)));
    defer alloc.free(raw);
    return try parseServiceAccountJsonAlloc(alloc, raw);
}

pub fn configFromFileAlloc(alloc: Allocator, path: []const u8, scope: []const u8) !Config {
    return try configFromFileAllocWithIo(alloc, path, scope, null);
}

pub fn configFromFileAllocWithIo(alloc: Allocator, path: []const u8, scope: []const u8, shared_io: ?std.Io) !Config {
    var io_impl: ?std.Io.Threaded = if (shared_io == null) std.Io.Threaded.init(std.heap.page_allocator, .{}) else null;
    defer if (io_impl) |*owned| owned.deinit();
    const io = shared_io orelse io_impl.?.io();
    const raw = try std.Io.Dir.cwd().readFileAlloc(io, path, alloc, .limited(4 * 1024 * 1024));
    defer alloc.free(raw);
    return try configFromJsonAlloc(alloc, raw, scope);
}

fn configFromJsonAlloc(alloc: Allocator, raw: []const u8, scope: []const u8) !Config {
    var kind = try std.json.parseFromSlice(struct { type: ?[]const u8 = null }, alloc, raw, .{ .ignore_unknown_fields = true });
    defer kind.deinit();
    if (kind.value.type) |credential_type| {
        if (std.mem.eql(u8, credential_type, "authorized_user")) return try configFromAuthorizedUserJsonAlloc(alloc, raw, scope);
        if (std.mem.eql(u8, credential_type, "external_account")) return try configFromExternalAccountJsonAlloc(alloc, raw, scope);
        if (!std.mem.eql(u8, credential_type, "service_account")) return error.UnsupportedGoogleCredentialType;
    }
    var account = try parseServiceAccountJsonAlloc(alloc, raw);
    errdefer account.deinit(alloc);
    return try configFromServiceAccountAlloc(alloc, account, scope);
}

fn defaultAdcPathAlloc(alloc: Allocator) !?[]u8 {
    if (try envOwned(alloc, "CLOUDSDK_CONFIG")) |config_dir| {
        defer alloc.free(config_dir);
        return try std.fs.path.join(alloc, &.{ config_dir, "application_default_credentials.json" });
    }
    if (try envOwned(alloc, "HOME")) |home_dir| {
        defer alloc.free(home_dir);
        return try std.fs.path.join(alloc, &.{ home_dir, ".config", "gcloud", "application_default_credentials.json" });
    }
    if (try envOwned(alloc, "APPDATA")) |app_data| {
        defer alloc.free(app_data);
        return try std.fs.path.join(alloc, &.{ app_data, "gcloud", "application_default_credentials.json" });
    }
    return null;
}

pub fn tokenSourceFromEnvAlloc(alloc: Allocator, scope: []const u8) !*CachedTokenSource {
    var cfg = try configFromEnvAlloc(alloc, scope);
    errdefer cfg.deinit(alloc);
    const source = try alloc.create(CachedTokenSource);
    errdefer alloc.destroy(source);
    source.* = try CachedTokenSource.init(alloc, cfg);
    return source;
}

pub fn configFromEnvAlloc(alloc: Allocator, scope: []const u8) !Config {
    if (try envOwned(alloc, "GOOGLE_SERVICE_ACCOUNT_JSON")) |json| {
        defer alloc.free(json);
        const account = try parseServiceAccountJsonAlloc(alloc, json);
        return try configFromServiceAccountAlloc(alloc, account, scope);
    }
    if (try envOwned(alloc, "GOOGLE_APPLICATION_CREDENTIALS")) |path| {
        defer alloc.free(path);
        return try configFromFileAlloc(alloc, path, scope);
    }
    if (try defaultAdcPathAlloc(alloc)) |path| {
        defer alloc.free(path);
        if (configFromFileAlloc(alloc, path, scope)) |cfg| {
            return cfg;
        } else |err| switch (err) {
            error.FileNotFound => {},
            else => return err,
        }
    }

    return try metadataConfigAlloc(alloc, scope);
}

pub fn projectIdFromDefaultCredentialsAlloc(alloc: Allocator) !?[]u8 {
    if ((try envOwned(alloc, "GOOGLE_CLOUD_PROJECT")) orelse (try envOwned(alloc, "GCLOUD_PROJECT"))) |value| return value;
    var cfg = configFromEnvAlloc(alloc, default_scope) catch return null;
    defer cfg.deinit(alloc);
    return switch (cfg.credentials) {
        .service_account => if (cfg.projectId()) |value| try alloc.dupe(u8, value) else null,
        .authorized_user, .external_account => null,
        .metadata => |metadata| blk: {
            var transport = HttpxTransport.init(alloc, null) catch break :blk null;
            defer transport.deinit();
            var response = HttpxTransport.request(&transport, alloc, .GET, metadata.project_id_url, &.{.{ "Metadata-Flavor", "Google" }}, null, null) catch break :blk null;
            defer response.deinit(alloc);
            if (response.status != 200) break :blk null;
            const project = std.mem.trim(u8, response.body, &std.ascii.whitespace);
            break :blk if (project.len > 0) try alloc.dupe(u8, project) else null;
        },
    };
}

pub fn projectIdFromFileAlloc(alloc: Allocator, path: []const u8) !?[]u8 {
    var cfg = try configFromFileAlloc(alloc, path, default_scope);
    defer cfg.deinit(alloc);
    return if (cfg.projectId()) |value| try alloc.dupe(u8, value) else null;
}

/// Compatibility alias. New code should use projectIdFromDefaultCredentialsAlloc.
pub fn serviceAccountEnvProjectIdAlloc(alloc: Allocator) !?[]u8 {
    return try projectIdFromDefaultCredentialsAlloc(alloc);
}

pub fn signedJwtAssertionAlloc(alloc: Allocator, service_account: ServiceAccount, scope: []const u8, now_s: u64) ![]u8 {
    const header_json = try std.json.Stringify.valueAlloc(alloc, .{
        .alg = "RS256",
        .typ = "JWT",
    }, .{});
    defer alloc.free(header_json);

    const claims_json = try std.json.Stringify.valueAlloc(alloc, .{
        .iss = service_account.client_email,
        .scope = scope,
        .aud = service_account.token_uri,
        .iat = now_s,
        .exp = now_s + 3600,
    }, .{});
    defer alloc.free(claims_json);

    const header_b64 = try base64UrlEncodeAlloc(alloc, header_json);
    defer alloc.free(header_b64);
    const claims_b64 = try base64UrlEncodeAlloc(alloc, claims_json);
    defer alloc.free(claims_b64);
    const signing_input = try std.fmt.allocPrint(alloc, "{s}.{s}", .{ header_b64, claims_b64 });
    defer alloc.free(signing_input);

    const signature = try rsaPkcs1v15Sha256SignAlloc(alloc, service_account.private_key_pem, signing_input);
    defer alloc.free(signature);
    const signature_b64 = try base64UrlEncodeAlloc(alloc, signature);
    defer alloc.free(signature_b64);

    return try std.fmt.allocPrint(alloc, "{s}.{s}", .{ signing_input, signature_b64 });
}

fn rsaPkcs1v15Sha256SignAlloc(alloc: Allocator, private_key_pem: []const u8, message: []const u8) ![]u8 {
    const pkcs8 = try decodePemPrivateKeyAlloc(alloc, private_key_pem);
    defer alloc.free(pkcs8);
    const parsed = try parsePkcs8PrivateKey(pkcs8);

    switch (parsed.modulus.len) {
        inline 128, 256, 384, 512 => |modulus_len| {
            var digest: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
            std.crypto.hash.sha2.Sha256.hash(message, &digest, .{});

            const encoded = try emsaPkcs1v15Sha256Encode(modulus_len, digest);
            const Modulus = std.crypto.ff.Modulus(4096);
            const n = try Modulus.fromBytes(parsed.modulus, .big);
            const m = try Modulus.Fe.fromBytes(n, &encoded, .big);
            const d = try Modulus.Fe.fromBytes(n, parsed.private_exponent, .big);
            const sig = try n.powPublic(m, d);
            var out: [modulus_len]u8 = undefined;
            try sig.toBytes(&out, .big);
            return try alloc.dupe(u8, &out);
        },
        else => return error.UnsupportedKeySize,
    }
}

fn emsaPkcs1v15Sha256Encode(comptime modulus_len: usize, digest: [std.crypto.hash.sha2.Sha256.digest_length]u8) ![modulus_len]u8 {
    const digest_info_prefix = [_]u8{
        0x30, 0x31, 0x30, 0x0d, 0x06, 0x09, 0x60, 0x86,
        0x48, 0x01, 0x65, 0x03, 0x04, 0x02, 0x01, 0x05,
        0x00, 0x04, 0x20,
    };
    const t_len = digest_info_prefix.len + digest.len;
    if (modulus_len < t_len + 11) return error.MessageTooLong;

    var out: [modulus_len]u8 = undefined;
    out[0] = 0x00;
    out[1] = 0x01;
    const ps_end = modulus_len - t_len - 1;
    @memset(out[2..ps_end], 0xff);
    out[ps_end] = 0x00;
    @memcpy(out[(ps_end + 1)..][0..digest_info_prefix.len], &digest_info_prefix);
    @memcpy(out[(ps_end + 1 + digest_info_prefix.len)..], &digest);
    return out;
}

const ParsedPrivateKey = struct {
    modulus: []const u8,
    private_exponent: []const u8,
};

fn parsePkcs8PrivateKey(pkcs8: []const u8) !ParsedPrivateKey {
    const root = try asn1_der.Element.parse(pkcs8, 0);
    if (root.identifier.tag != .sequence) return error.InvalidPrivateKey;
    const version = try asn1_der.Element.parse(pkcs8, root.slice.start);
    if (version.identifier.tag != .integer) return error.InvalidPrivateKey;
    const algorithm = try asn1_der.Element.parse(pkcs8, version.slice.end);
    if (algorithm.identifier.tag != .sequence) return error.InvalidPrivateKey;
    const private_key = try asn1_der.Element.parse(pkcs8, algorithm.slice.end);
    if (private_key.identifier.tag != .octetstring) return error.InvalidPrivateKey;

    const rsa = pkcs8[private_key.slice.start..private_key.slice.end];
    const seq = try asn1_der.Element.parse(rsa, 0);
    if (seq.identifier.tag != .sequence) return error.InvalidPrivateKey;
    const rsa_version = try asn1_der.Element.parse(rsa, seq.slice.start);
    if (rsa_version.identifier.tag != .integer) return error.InvalidPrivateKey;
    const modulus_elem = try asn1_der.Element.parse(rsa, rsa_version.slice.end);
    if (modulus_elem.identifier.tag != .integer) return error.InvalidPrivateKey;
    const public_exponent_elem = try asn1_der.Element.parse(rsa, modulus_elem.slice.end);
    if (public_exponent_elem.identifier.tag != .integer) return error.InvalidPrivateKey;
    const private_exponent_elem = try asn1_der.Element.parse(rsa, public_exponent_elem.slice.end);
    if (private_exponent_elem.identifier.tag != .integer) return error.InvalidPrivateKey;

    return .{
        .modulus = trimLeadingZeroes(rsa[modulus_elem.slice.start..modulus_elem.slice.end]),
        .private_exponent = trimLeadingZeroes(rsa[private_exponent_elem.slice.start..private_exponent_elem.slice.end]),
    };
}

fn trimLeadingZeroes(bytes: []const u8) []const u8 {
    const offset = for (bytes, 0..) |byte, idx| {
        if (byte != 0) break idx;
    } else bytes.len;
    return bytes[offset..];
}

fn decodePemPrivateKeyAlloc(alloc: Allocator, pem: []const u8) ![]u8 {
    const begin_marker = "-----BEGIN PRIVATE KEY-----";
    const end_marker = "-----END PRIVATE KEY-----";
    const begin = std.mem.indexOf(u8, pem, begin_marker) orelse return error.InvalidPem;
    const end = std.mem.indexOf(u8, pem, end_marker) orelse return error.InvalidPem;
    const body = pem[(begin + begin_marker.len)..end];
    const trimmed = std.mem.trim(u8, body, &std.ascii.whitespace);
    const decoder = std.base64.standard.decoderWithIgnore("\n\r\t ");
    const out = try alloc.alloc(u8, decoder.calcSizeUpperBound(trimmed.len));
    errdefer alloc.free(out);
    const out_len = try decoder.decode(out, trimmed);
    return try alloc.realloc(out, out_len);
}

fn base64UrlEncodeAlloc(alloc: Allocator, raw: []const u8) ![]u8 {
    const encoder = std.base64.url_safe_no_pad.Encoder;
    const out = try alloc.alloc(u8, encoder.calcSize(raw.len));
    errdefer alloc.free(out);
    _ = encoder.encode(out, raw);
    return out;
}

fn envOwned(alloc: Allocator, comptime name: []const u8) !?[]u8 {
    const value = platform.env.getenv(name ++ "\x00") orelse return null;
    return try alloc.dupe(u8, value);
}

fn nowSeconds() u64 {
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    const now = std.Io.Timestamp.now(io_impl.io(), .real);
    return @intCast(now.toSeconds());
}

test "google auth parses service account json" {
    const alloc = std.testing.allocator;
    const json =
        \\{
        \\  "project_id": "proj-1",
        \\  "private_key_id": "kid-1",
        \\  "private_key": "-----BEGIN PRIVATE KEY-----\nABC\n-----END PRIVATE KEY-----\n",
        \\  "client_email": "svc@example.iam.gserviceaccount.com",
        \\  "token_uri": "https://oauth2.googleapis.com/token"
        \\}
    ;
    var parsed = try parseServiceAccountJsonAlloc(alloc, json);
    defer parsed.deinit(alloc);
    try std.testing.expectEqualStrings("proj-1", parsed.project_id.?);
    try std.testing.expectEqualStrings("kid-1", parsed.private_key_id.?);
    try std.testing.expectEqualStrings("svc@example.iam.gserviceaccount.com", parsed.client_email);
}

test "google auth token source exchanges and caches access token" {
    const alloc = std.testing.allocator;
    const pem =
        \\-----BEGIN PRIVATE KEY-----
        \\MIICeAIBADANBgkqhkiG9w0BAQEFAASCAmIwggJeAgEAAoGBAOXaLd9jk03zcJ95
        \\CfwKjyqHiZAaf0KC4rwRWd+TSvrqdiZUHneOXchF4FtwAJ6m+qi5KsTyazOWv4S0
        \\FRLd49XFNv8op9e8x+gnItgt4QoQ2UT+QU7qG+wyavU25+m61G2CFB8+I9wXzH3x
        \\HMfUuOWgqfy+szxUFNRf3sEfGW8DAgMBAAECgYEAmR1LG5mQggfeCU2vGgfKsRES
        \\0Tzlc2APPCruzKGo/Bb917CHjyr2TDhIKYEl2InxRj37QLEgOoB8WiFAPI41e2mZ
        \\r/sshHAB74N7OOCG6G4Jin1qsnQKgSwloBctDxtvUydD1ApmjfKQB1vENL6h4jKU
        \\VMBm/65DU/4iWJkWgBECQQD4oRPl63IemtUsRTnz+j8tEC5MsH7CNvwNj5os2ptm
        \\X3/rAge3BKYMWlN237K6yapZMHfiLj3K3fv8Kkbn7VwpAkEA7KqY97XZaLr4sI3a
        \\9EHgbB2GjzJAsnzXSfn7OXLuc812rDpK/+6mcXFSbe1OmQTbzPIOJIARcIz3fqXI
        \\uAHXSwJAOlA1RYjKVElGVELMS9/Wr3ALG+uNX2ncBiY3J+wB5Knja7AnNRK/C0io
        \\KMpgthSUgqSuiXsE/S7BaixUQxNVuQJBAJC8hHB5tkxmjFDtcEqRPz7fj7tjcE24
        \\K7ICP7ISp+IKddk+jT+YJBKcy1yPFNJgNkxQfHW2HPRIQdQib26ZMaECQQCcW21U
        \\jsnUTXZp0WrOnzoqkJtQmmey1Bb9ZxBym/IoaQdDefgbdlyeFQTz2tWKDwqAlEsl
        \\8peeQ6Fmi8Vuw9qK
        \\-----END PRIVATE KEY-----
    ;
    const service_account = ServiceAccount{
        .project_id = try alloc.dupe(u8, "proj-1"),
        .private_key_id = null,
        .private_key_pem = try alloc.dupe(u8, pem),
        .client_email = try alloc.dupe(u8, "svc@example.iam.gserviceaccount.com"),
        .token_uri = try alloc.dupe(u8, "https://oauth2.googleapis.com/token"),
    };
    const cfg = try configFromServiceAccountAlloc(alloc, service_account, default_scope);

    const State = struct {
        calls: usize = 0,
        fail: bool = false,

        fn request(
            ptr: ?*anyopaque,
            request_alloc: Allocator,
            method: HttpMethod,
            url: []const u8,
            headers: []const HeaderPair,
            body: ?[]const u8,
            content_type: ?[]const u8,
        ) !TransportResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr.?));
            self.calls += 1;
            if (self.fail) return error.TokenEndpointUnavailable;
            try std.testing.expectEqual(HttpMethod.POST, method);
            try std.testing.expectEqualStrings("https://oauth2.googleapis.com/token", url);
            try std.testing.expectEqualStrings("application/x-www-form-urlencoded", content_type.?);
            try expectHeader(headers, "Accept", "application/json");
            try std.testing.expect(std.mem.indexOf(u8, body.?, "grant_type=") != null);
            try std.testing.expect(std.mem.indexOf(u8, body.?, "assertion=") != null);
            return .{
                .status = 200,
                .body = try request_alloc.dupe(u8, "{\"access_token\":\"token-123\",\"expires_in\":3600,\"token_type\":\"Bearer\"}"),
                .content_type = try request_alloc.dupe(u8, "application/json"),
            };
        }
    };

    var state = State{};
    var source = CachedTokenSource.initWithRequestFn(alloc, cfg, &state, State.request);
    defer source.deinit();
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    source.io = io_impl.io();

    const Job = struct {
        source: *CachedTokenSource,
        alloc: Allocator,
        value: ?[]u8 = null,
        err: ?anyerror = null,

        fn run(job: *@This()) std.Io.Cancelable!void {
            job.value = job.source.authorizationValueAlloc(job.alloc) catch |err| {
                job.err = err;
                return;
            };
        }
    };
    var jobs: [8]Job = undefined;
    var group: std.Io.Group = .init;
    for (&jobs) |*job| {
        job.* = .{ .source = &source, .alloc = alloc };
        try group.concurrent(io_impl.io(), Job.run, .{job});
    }
    try group.await(io_impl.io());
    for (&jobs) |*job| {
        if (job.err) |err| return err;
        const value = job.value.?;
        defer alloc.free(value);
        try std.testing.expectEqualStrings("Bearer token-123", value);
    }
    try std.testing.expectEqual(@as(usize, 1), state.calls);

    source.cached_token.?.expires_at_s = nowSeconds() + 10;
    state.fail = true;
    const fallback = try source.authorizationValueAlloc(alloc);
    defer alloc.free(fallback);
    try std.testing.expectEqualStrings("Bearer token-123", fallback);
    try std.testing.expectEqual(@as(usize, 2), state.calls);

    source.cached_token.?.expires_at_s = 0;
    try std.testing.expectError(error.TokenEndpointUnavailable, source.authorizationValueAlloc(alloc));
    try std.testing.expectEqual(@as(usize, 3), state.calls);
}

test "google auth metadata credentials mint and cache a token" {
    const alloc = std.testing.allocator;
    const cfg = Config{
        .credentials = .{ .metadata = .{
            .token_url = try alloc.dupe(u8, "http://metadata.test/token"),
            .project_id_url = try alloc.dupe(u8, "http://metadata.test/project"),
        } },
        .scope = try alloc.dupe(u8, default_scope),
    };
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
            const self: *@This() = @ptrCast(@alignCast(ptr.?));
            self.calls += 1;
            try std.testing.expectEqual(HttpMethod.GET, method);
            try std.testing.expectEqualStrings("http://metadata.test/token", url);
            try expectHeader(headers, "Metadata-Flavor", "Google");
            try std.testing.expect(body == null);
            try std.testing.expect(content_type == null);
            return .{
                .status = 200,
                .body = try request_alloc.dupe(u8, "{\"access_token\":\"metadata-token\",\"expires_in\":3600,\"token_type\":\"Bearer\"}"),
            };
        }
    };
    var state = State{};
    var source = CachedTokenSource.initWithRequestFn(alloc, cfg, &state, State.request);
    defer source.deinit();
    const first = try source.authorizationValueAlloc(alloc);
    defer alloc.free(first);
    const second = try source.authorizationValueAlloc(alloc);
    defer alloc.free(second);
    try std.testing.expectEqualStrings("Bearer metadata-token", first);
    try std.testing.expectEqualStrings(first, second);
    try std.testing.expectEqual(@as(usize, 1), state.calls);
}

test "google auth authorized user credentials refresh and cache a token" {
    const alloc = std.testing.allocator;
    const cfg = try configFromJsonAlloc(
        alloc,
        "{\"type\":\"authorized_user\",\"client_id\":\"client\",\"client_secret\":\"secret\",\"refresh_token\":\"refresh\",\"token_uri\":\"https://oauth.test/token\"}",
        default_scope,
    );
    const State = struct {
        calls: usize = 0,
        fn request(
            ptr: ?*anyopaque,
            request_alloc: Allocator,
            method: HttpMethod,
            url: []const u8,
            _: []const HeaderPair,
            body: ?[]const u8,
            content_type: ?[]const u8,
        ) !TransportResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr.?));
            self.calls += 1;
            try std.testing.expectEqual(HttpMethod.POST, method);
            try std.testing.expectEqualStrings("https://oauth.test/token", url);
            try std.testing.expectEqualStrings("application/x-www-form-urlencoded", content_type.?);
            try std.testing.expect(std.mem.indexOf(u8, body.?, "grant_type=refresh_token") != null);
            try std.testing.expect(std.mem.indexOf(u8, body.?, "refresh_token=refresh") != null);
            return .{
                .status = 200,
                .body = try request_alloc.dupe(u8, "{\"access_token\":\"user-token\",\"expires_in\":3600}"),
            };
        }
    };
    var state = State{};
    var source = CachedTokenSource.initWithRequestFn(alloc, cfg, &state, State.request);
    defer source.deinit();
    const first = try source.accessTokenAlloc(alloc);
    defer alloc.free(first);
    const second = try source.accessTokenAlloc(alloc);
    defer alloc.free(second);
    try std.testing.expectEqualStrings("user-token", first);
    try std.testing.expectEqualStrings(first, second);
    try std.testing.expectEqual(@as(usize, 1), state.calls);
}

test "google auth external account credentials honor subject token headers" {
    const alloc = std.testing.allocator;
    const cfg = try configFromJsonAlloc(
        alloc,
        "{\"type\":\"external_account\",\"audience\":\"//iam.googleapis.com/pool/provider\",\"subject_token_type\":\"urn:ietf:params:oauth:token-type:jwt\",\"token_url\":\"https://sts.test/token\",\"credential_source\":{\"url\":\"https://identity.test/token\",\"headers\":{\"Metadata-Flavor\":\"Google\"},\"format\":{\"type\":\"json\",\"subject_token_field_name\":\"token\"}}}",
        default_scope,
    );
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
            const self: *@This() = @ptrCast(@alignCast(ptr.?));
            self.calls += 1;
            if (self.calls == 1) {
                try std.testing.expectEqual(HttpMethod.GET, method);
                try std.testing.expectEqualStrings("https://identity.test/token", url);
                try expectHeader(headers, "Metadata-Flavor", "Google");
                return .{ .status = 200, .body = try request_alloc.dupe(u8, "{\"token\":\"subject-jwt\"}") };
            }
            try std.testing.expectEqual(HttpMethod.POST, method);
            try std.testing.expectEqualStrings("https://sts.test/token", url);
            try std.testing.expectEqualStrings("application/x-www-form-urlencoded", content_type.?);
            try std.testing.expect(std.mem.indexOf(u8, body.?, "subject_token=subject-jwt") != null);
            return .{ .status = 200, .body = try request_alloc.dupe(u8, "{\"access_token\":\"federated-token\",\"expires_in\":3600}") };
        }
    };
    var state = State{};
    var source = CachedTokenSource.initWithRequestFn(alloc, cfg, &state, State.request);
    defer source.deinit();
    const token = try source.accessTokenAlloc(alloc);
    defer alloc.free(token);
    try std.testing.expectEqualStrings("federated-token", token);
    try std.testing.expectEqual(@as(usize, 2), state.calls);
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
