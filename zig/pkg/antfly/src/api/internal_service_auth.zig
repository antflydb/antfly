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

//! Short-lived, audience-bound credentials for node-to-node API calls.

const std = @import("std");
const http_common = @import("antfly_local_sources").common_http_http_common;
const platform_time = @import("antfly_platform").time;

pub const audience = "antfly-internal-v1";
pub const principal_kind = "service";
pub const default_subject = "antfly-node";
pub const token_ttl_seconds: i64 = 60;
pub const header_name = "X-Antfly-Trusted-Principal";
pub const minimum_secret_bytes: usize = 32;
pub const maximum_issuer_bytes: usize = 256;

/// `migration` is an explicit two-phase rolling-upgrade escape hatch. New
/// nodes still sign every outbound internal request, but temporarily accept
/// unsigned requests from old peers. Operators must return to `enforce` after
/// every peer has been upgraded. The secure default never accepts legacy RPC.
pub const RolloutMode = enum {
    enforce,
    migration,
};

pub fn capability(mode: RolloutMode, has_additional_verifier: bool) []const u8 {
    return switch (mode) {
        .enforce => if (has_additional_verifier)
            "v1; mode=enforce; verification=additional"
        else
            "v1; mode=enforce",
        .migration => if (has_additional_verifier)
            "v1; mode=migration; verification=additional"
        else
            "v1; mode=migration",
    };
}

pub const Config = struct {
    secret: []const u8,
    issuer: []const u8,
    subject: []const u8 = default_subject,
};

pub fn requestTargetsInternalApi(uri: []const u8) bool {
    const path_start = if (std.mem.indexOf(u8, uri, "://")) |scheme_end| blk: {
        const authority_start = scheme_end + 3;
        break :blk std.mem.indexOfScalarPos(u8, uri, authority_start, '/') orelse return false;
    } else 0;
    const target = uri[path_start..];
    const path_end = std.mem.indexOfAny(u8, target, "?#") orelse target.len;
    const path = target[0..path_end];
    return std.mem.eql(u8, path, "/internal/v1") or
        std.mem.startsWith(u8, path, "/internal/v1/");
}

/// Execute a request with a short-lived service credential only when its
/// resolved target is inside the internal API namespace. Keeping signing here
/// gives every handwritten node client the same non-leakage and duplicate-
/// header guarantees as generated API forwarding.
pub fn executeRequest(
    alloc: std.mem.Allocator,
    executor: http_common.RequestExecutor,
    request: http_common.HttpRequest,
    config: ?Config,
) !http_common.HttpResponse {
    if (request.delivery_tracker) |tracker| tracker.markNotSent();
    const signing = config orelse return executor.execute(alloc, request);
    if (!requestTargetsInternalApi(request.uri)) return executor.execute(alloc, request);

    const realtime_ns = executor.realtimeNs() orelse platform_time.realtimeNs();
    const token = try tokenAlloc(alloc, signing, @intCast(@divFloor(realtime_ns, std.time.ns_per_s)));
    defer alloc.free(token);

    const headers = try alloc.alloc(http_common.RequestHeader, request.headers.len + 1);
    defer alloc.free(headers);
    var header_count: usize = 0;
    for (request.headers) |header| {
        if (std.ascii.eqlIgnoreCase(header.name, header_name)) continue;
        headers[header_count] = header;
        header_count += 1;
    }
    headers[header_count] = .{ .name = header_name, .value = token };
    header_count += 1;
    var authenticated = request;
    authenticated.headers = headers[0..header_count];
    return executor.execute(alloc, authenticated);
}

pub fn executeRequestStream(
    alloc: std.mem.Allocator,
    executor: http_common.RequestExecutor,
    request: http_common.HttpRequest,
    writer: http_common.StreamWriter,
    config: ?Config,
) !?bool {
    const signing = config orelse return try executor.executeStream(alloc, request, writer);
    if (!requestTargetsInternalApi(request.uri))
        return try executor.executeStream(alloc, request, writer);

    const token = try tokenAlloc(
        alloc,
        signing,
        @intCast(@divFloor(platform_time.realtimeNs(), std.time.ns_per_s)),
    );
    defer alloc.free(token);
    const headers = try alloc.alloc(http_common.RequestHeader, request.headers.len + 1);
    defer alloc.free(headers);
    var header_count: usize = 0;
    for (request.headers) |header| {
        if (std.ascii.eqlIgnoreCase(header.name, header_name)) continue;
        headers[header_count] = header;
        header_count += 1;
    }
    headers[header_count] = .{ .name = header_name, .value = token };
    header_count += 1;
    var authenticated = request;
    authenticated.headers = headers[0..header_count];
    return try executor.executeStream(alloc, authenticated, writer);
}

pub fn parseRolloutMode(raw: ?[]const u8) !RolloutMode {
    const value = raw orelse return .enforce;
    if (std.mem.eql(u8, value, "enforce")) return .enforce;
    if (std.mem.eql(u8, value, "migration")) return .migration;
    return error.InvalidInternalServiceRolloutMode;
}

pub fn validateRuntimeConfig(secret: ?[]const u8, verification_secret: ?[]const u8, issuer: ?[]const u8) !void {
    const signing_secret = secret orelse return error.InternalServiceSecretMissing;
    if (signing_secret.len < minimum_secret_bytes)
        return error.InternalServiceSecretTooShort;
    if (verification_secret) |additional| {
        if (additional.len < minimum_secret_bytes)
            return error.InternalServiceVerificationSecretTooShort;
        if (std.mem.eql(u8, signing_secret, additional))
            return error.InternalServiceVerificationSecretReused;
    }
    const signing_issuer = issuer orelse return error.InternalServiceIssuerMissing;
    if (signing_issuer.len == 0 or signing_issuer.len > maximum_issuer_bytes)
        return error.InvalidInternalServiceIssuer;
    for (signing_issuer) |byte| {
        if (byte < 0x21 or byte > 0x7e)
            return error.InvalidInternalServiceIssuer;
    }
}

pub fn validateCredentialIsolation(
    internal_service_secret: ?[]const u8,
    trusted_principal_secret: ?[]const u8,
) !void {
    const internal = internal_service_secret orelse return;
    const trusted = trusted_principal_secret orelse return;
    if (std.mem.eql(u8, internal, trusted))
        return error.InternalServiceSecretReused;
}

pub fn tokenAlloc(alloc: std.mem.Allocator, config: Config, now_seconds: i64) ![]u8 {
    const header_json = "{\"alg\":\"HS256\",\"typ\":\"JWT\"}";
    const payload_json = try std.json.Stringify.valueAlloc(alloc, .{
        .iss = config.issuer,
        .sub = config.subject,
        .aud = audience,
        .principal_kind = principal_kind,
        .admin = true,
        .iat = now_seconds,
        .exp = now_seconds + token_ttl_seconds,
    }, .{});
    defer alloc.free(payload_json);

    const header_b64 = try base64UrlEncodeAlloc(alloc, header_json);
    defer alloc.free(header_b64);
    const payload_b64 = try base64UrlEncodeAlloc(alloc, payload_json);
    defer alloc.free(payload_b64);
    const signing_input = try std.fmt.allocPrint(alloc, "{s}.{s}", .{ header_b64, payload_b64 });
    defer alloc.free(signing_input);

    var mac: [std.crypto.auth.hmac.sha2.HmacSha256.mac_length]u8 = undefined;
    std.crypto.auth.hmac.sha2.HmacSha256.create(mac[0..], signing_input, config.secret);
    const signature_b64 = try base64UrlEncodeAlloc(alloc, &mac);
    defer alloc.free(signature_b64);
    return try std.fmt.allocPrint(alloc, "{s}.{s}", .{ signing_input, signature_b64 });
}

fn base64UrlEncodeAlloc(alloc: std.mem.Allocator, value: []const u8) ![]u8 {
    const size = std.base64.url_safe_no_pad.Encoder.calcSize(value.len);
    const encoded = try alloc.alloc(u8, size);
    _ = std.base64.url_safe_no_pad.Encoder.encode(encoded, value);
    return encoded;
}

test "internal service tokens carry a distinct bounded identity" {
    const alloc = std.testing.allocator;
    const token = try tokenAlloc(alloc, .{
        .secret = "secret",
        .issuer = "cluster",
        .subject = "node:7",
    }, 100);
    defer alloc.free(token);
    try std.testing.expectEqual(@as(usize, 2), std.mem.count(u8, token, "."));
}

test "runtime config requires an isolated production-strength credential" {
    try std.testing.expectError(
        error.InternalServiceSecretMissing,
        validateRuntimeConfig(null, null, "cluster-a"),
    );
    try std.testing.expectError(
        error.InternalServiceSecretTooShort,
        validateRuntimeConfig("short", null, "cluster-a"),
    );
    try std.testing.expectError(
        error.InternalServiceIssuerMissing,
        validateRuntimeConfig("0123456789abcdef0123456789abcdef", null, null),
    );
    try validateRuntimeConfig("0123456789abcdef0123456789abcdef", null, "cluster-a");
    try validateRuntimeConfig(
        "0123456789abcdef0123456789abcdef",
        "abcdef0123456789abcdef0123456789",
        "cluster-a",
    );
    try std.testing.expectError(
        error.InternalServiceVerificationSecretReused,
        validateRuntimeConfig(
            "0123456789abcdef0123456789abcdef",
            "0123456789abcdef0123456789abcdef",
            "cluster-a",
        ),
    );
    try std.testing.expectError(
        error.InternalServiceSecretReused,
        validateCredentialIsolation(
            "0123456789abcdef0123456789abcdef",
            "0123456789abcdef0123456789abcdef",
        ),
    );
    try validateCredentialIsolation(
        "0123456789abcdef0123456789abcdef",
        "fedcba9876543210fedcba9876543210",
    );
    try std.testing.expectEqual(RolloutMode.enforce, try parseRolloutMode(null));
    try std.testing.expectEqual(RolloutMode.migration, try parseRolloutMode("migration"));
    try std.testing.expectError(
        error.InvalidInternalServiceRolloutMode,
        parseRolloutMode("disabled"),
    );
}

/// Strict service-only verifier for runtimes without the public principal
/// parser. It deliberately accepts neither SQL-policy nor legacy credentials.
pub fn verifyToken(alloc: std.mem.Allocator, token: []const u8, config: Config, now_seconds: i64) !void {
    if (token.len > 4096 or config.secret.len < minimum_secret_bytes or config.issuer.len == 0) return error.Unauthorized;
    var parts = std.mem.splitScalar(u8, token, '.');
    const header = parts.next() orelse return error.Unauthorized;
    const payload = parts.next() orelse return error.Unauthorized;
    const signature = parts.next() orelse return error.Unauthorized;
    if (parts.next() != null or header.len == 0 or payload.len == 0) return error.Unauthorized;
    var mac: [32]u8 = undefined;
    std.crypto.auth.hmac.sha2.HmacSha256.create(&mac, token[0 .. header.len + 1 + payload.len], config.secret);
    var actual: [32]u8 = undefined;
    const decoder = std.base64.url_safe_no_pad.Decoder;
    if ((decoder.calcSizeForSlice(signature) catch return error.Unauthorized) != actual.len) return error.Unauthorized;
    decoder.decode(&actual, signature) catch return error.Unauthorized;
    if (!std.crypto.timing_safe.eql([32]u8, mac, actual)) return error.Unauthorized;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    const h = try a.alloc(u8, decoder.calcSizeForSlice(header) catch return error.Unauthorized);
    decoder.decode(h, header) catch return error.Unauthorized;
    const Header = struct { alg: []const u8, typ: []const u8 };
    const parsed_header = std.json.parseFromSliceLeaky(Header, a, h, .{}) catch return error.Unauthorized;
    if (!std.mem.eql(u8, parsed_header.alg, "HS256") or !std.mem.eql(u8, parsed_header.typ, "JWT")) return error.Unauthorized;
    const p = try a.alloc(u8, decoder.calcSizeForSlice(payload) catch return error.Unauthorized);
    decoder.decode(p, payload) catch return error.Unauthorized;
    const Payload = struct { iss: []const u8, sub: []const u8, aud: []const u8, principal_kind: []const u8, admin: bool, iat: i64, exp: i64 };
    const parsed = std.json.parseFromSliceLeaky(Payload, a, p, .{}) catch return error.Unauthorized;
    if (!std.mem.eql(u8, parsed.iss, config.issuer) or !std.mem.eql(u8, parsed.aud, audience) or !std.mem.eql(u8, parsed.principal_kind, principal_kind) or parsed.sub.len == 0 or !parsed.admin or parsed.exp < now_seconds or parsed.iat > now_seconds +| 60 or parsed.exp < parsed.iat or parsed.exp -| parsed.iat > token_ttl_seconds) return error.Unauthorized;
}

test "internal service strict verifier rejects expired wrong issuer and modified credentials" {
    const a = std.testing.allocator;
    const config: Config = .{ .secret = "0123456789abcdef0123456789abcdef", .issuer = "workers" };
    const token = try tokenAlloc(a, config, 1000);
    defer a.free(token);
    try verifyToken(a, token, config, 1020);
    try std.testing.expectError(error.Unauthorized, verifyToken(a, token, config, 1061));
    try std.testing.expectError(error.Unauthorized, verifyToken(a, token, .{ .secret = config.secret, .issuer = "other" }, 1020));
    token[token.len - 5] = if (token[token.len - 5] == 'a') 'b' else 'a';
    try std.testing.expectError(error.Unauthorized, verifyToken(a, token, config, 1020));
}
