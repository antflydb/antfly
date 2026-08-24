// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at https://www.antfly.io/licensing/ELv2-license

//! Short-lived, audience-bound credentials for node-to-node API calls.

const std = @import("std");

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

pub const Config = struct {
    secret: []const u8,
    issuer: []const u8,
    subject: []const u8 = default_subject,
};

pub fn parseRolloutMode(raw: ?[]const u8) !RolloutMode {
    const value = raw orelse return .enforce;
    if (std.mem.eql(u8, value, "enforce")) return .enforce;
    if (std.mem.eql(u8, value, "migration")) return .migration;
    return error.InvalidInternalServiceRolloutMode;
}

pub fn validateRuntimeConfig(secret: ?[]const u8, issuer: ?[]const u8) !void {
    const signing_secret = secret orelse return error.InternalServiceSecretMissing;
    if (signing_secret.len < minimum_secret_bytes)
        return error.InternalServiceSecretTooShort;
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
        validateRuntimeConfig(null, "cluster-a"),
    );
    try std.testing.expectError(
        error.InternalServiceSecretTooShort,
        validateRuntimeConfig("short", "cluster-a"),
    );
    try std.testing.expectError(
        error.InternalServiceIssuerMissing,
        validateRuntimeConfig("0123456789abcdef0123456789abcdef", null),
    );
    try validateRuntimeConfig("0123456789abcdef0123456789abcdef", "cluster-a");
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
