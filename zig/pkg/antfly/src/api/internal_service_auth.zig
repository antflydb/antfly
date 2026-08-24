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

pub const Config = struct {
    secret: []const u8,
    issuer: []const u8,
    subject: []const u8 = default_subject,
};

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
