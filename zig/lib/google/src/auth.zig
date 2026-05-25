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
    POST,

    fn toHttpx(self: HttpMethod) httpx.Method {
        return switch (self) {
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

pub const Config = struct {
    service_account: ServiceAccount,
    scope: []u8,

    pub fn deinit(self: *Config, alloc: Allocator) void {
        self.service_account.deinit(alloc);
        alloc.free(self.scope);
        self.* = undefined;
    }
};

pub const CachedTokenSource = struct {
    alloc: Allocator,
    cfg: Config,
    request_ctx: ?*anyopaque,
    request_fn: RequestFn,
    owned_httpx: ?*HttpxTransport,
    cached_token: ?AccessToken = null,

    pub fn init(alloc: Allocator, cfg: Config) !CachedTokenSource {
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
    ) CachedTokenSource {
        return .{
            .alloc = alloc,
            .cfg = cfg,
            .request_ctx = request_ctx,
            .request_fn = request_fn,
            .owned_httpx = null,
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
        const now = nowSeconds();
        if (self.cached_token) |token| {
            if (token.expires_at_s > now + 30) return try alloc.dupe(u8, token.value);
        }

        var minted = try self.mintTokenAlloc(self.alloc, now);
        errdefer minted.deinit(self.alloc);

        if (self.cached_token) |*existing| existing.deinit(self.alloc);
        self.cached_token = minted;
        return try alloc.dupe(u8, self.cached_token.?.value);
    }

    fn mintTokenAlloc(self: *CachedTokenSource, alloc: Allocator, now_s: u64) !AccessToken {
        const assertion = try signedJwtAssertionAlloc(alloc, self.cfg.service_account, self.cfg.scope, now_s);
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
            self.cfg.service_account.token_uri,
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
};

const AccessToken = struct {
    value: []u8,
    expires_at_s: u64,

    fn deinit(self: *AccessToken, alloc: Allocator) void {
        alloc.free(self.value);
        self.* = undefined;
    }
};

const ServiceAccountJson = struct {
    project_id: ?[]const u8 = null,
    private_key_id: ?[]const u8 = null,
    private_key: []const u8,
    client_email: []const u8,
    token_uri: ?[]const u8 = null,
};

const TokenResponseBody = struct {
    access_token: []const u8,
    expires_in: u64,
    token_type: ?[]const u8 = null,
};

pub fn configFromServiceAccountAlloc(alloc: Allocator, service_account: ServiceAccount, scope: []const u8) !Config {
    return .{
        .service_account = service_account,
        .scope = try alloc.dupe(u8, scope),
    };
}

pub fn parseServiceAccountJsonAlloc(alloc: Allocator, raw: []const u8) !ServiceAccount {
    var parsed = try std.json.parseFromSlice(ServiceAccountJson, alloc, raw, .{});
    defer parsed.deinit();
    return .{
        .project_id = if (parsed.value.project_id) |value| try alloc.dupe(u8, value) else null,
        .private_key_id = if (parsed.value.private_key_id) |value| try alloc.dupe(u8, value) else null,
        .private_key_pem = try alloc.dupe(u8, parsed.value.private_key),
        .client_email = try alloc.dupe(u8, parsed.value.client_email),
        .token_uri = try alloc.dupe(u8, parsed.value.token_uri orelse default_token_uri),
    };
}

pub fn serviceAccountFromFileAlloc(alloc: Allocator, path: []const u8) !ServiceAccount {
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    const raw = try std.Io.Dir.cwd().readFileAlloc(io_impl.io(), path, alloc, .limited(std.math.maxInt(usize)));
    defer alloc.free(raw);
    return try parseServiceAccountJsonAlloc(alloc, raw);
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
    const service_account = if (try envOwned(alloc, "GOOGLE_SERVICE_ACCOUNT_JSON")) |json| blk: {
        defer alloc.free(json);
        break :blk try parseServiceAccountJsonAlloc(alloc, json);
    } else if (try envOwned(alloc, "GOOGLE_APPLICATION_CREDENTIALS")) |path| blk: {
        defer alloc.free(path);
        break :blk try serviceAccountFromFileAlloc(alloc, path);
    } else return error.MissingServiceAccount;

    return try configFromServiceAccountAlloc(alloc, service_account, scope);
}

pub fn serviceAccountEnvProjectIdAlloc(alloc: Allocator) !?[]u8 {
    var service_account = configFromEnvAlloc(alloc, default_scope) catch return null;
    defer service_account.deinit(alloc);
    return if (service_account.service_account.project_id) |value| try alloc.dupe(u8, value) else null;
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

    const auth_value_1 = try source.authorizationValueAlloc(alloc);
    defer alloc.free(auth_value_1);
    try std.testing.expectEqualStrings("Bearer token-123", auth_value_1);

    const auth_value_2 = try source.authorizationValueAlloc(alloc);
    defer alloc.free(auth_value_2);
    try std.testing.expectEqualStrings("Bearer token-123", auth_value_2);
    try std.testing.expectEqual(@as(usize, 1), state.calls);
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
