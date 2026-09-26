// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Short-lived, body-bound grants minted only after public API authorization.
//! Derive a dedicated key from the trusted-principal credential; never use
//! the generic node-to-node service key or the raw gateway-token key directly.
//! Metadata requires both this grant and authenticated internal transport.
//! Rotation is fail-closed: publish the new trusted-principal key to gateway
//! and metadata together; in-flight grants expire within 60 seconds.
const std = @import("std");

pub const header_name = "x-antfly-setting-grant";
pub const ttl_seconds: i64 = 60;
pub const Purpose = enum { read, admin };

pub fn sign(alloc: std.mem.Allocator, secret: []const u8, issuer: []const u8, purpose: Purpose, body: []const u8, now_seconds: i64) ![]u8 {
    if (secret.len == 0 or issuer.len == 0) return error.SettingAuthorityUnavailable;
    const expires = try std.math.add(i64, now_seconds, ttl_seconds);
    const mac = digest(secret, issuer, purpose, body, expires);
    const encoded = std.fmt.bytesToHex(mac, .lower);
    return std.fmt.allocPrint(alloc, "v1:{d}:{s}", .{ expires, &encoded });
}

pub fn verify(secret: []const u8, issuer: []const u8, purpose: Purpose, body: []const u8, now_seconds: i64, grant: []const u8) !void {
    if (secret.len == 0 or issuer.len == 0) return error.SettingAuthorityUnavailable;
    var parts = std.mem.splitScalar(u8, grant, ':');
    if (!std.mem.eql(u8, parts.next() orelse "", "v1")) return error.Forbidden;
    const expires = std.fmt.parseInt(i64, parts.next() orelse return error.Forbidden, 10) catch return error.Forbidden;
    const encoded = parts.next() orelse return error.Forbidden;
    if (parts.next() != null or encoded.len != 64 or expires < now_seconds or expires > now_seconds +| ttl_seconds) return error.Forbidden;
    const expected = digest(secret, issuer, purpose, body, expires);
    var provided: [32]u8 = undefined;
    _ = std.fmt.hexToBytes(&provided, encoded) catch return error.Forbidden;
    if (!std.crypto.timing_safe.eql([32]u8, expected, provided)) return error.Forbidden;
}

fn digest(secret: []const u8, issuer: []const u8, purpose: Purpose, body: []const u8, expires: i64) [32]u8 {
    var body_hash: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(body, &body_hash, .{});
    var expires_bytes: [8]u8 = undefined;
    std.mem.writeInt(i64, &expires_bytes, expires, .little);
    var derived_key: [32]u8 = undefined;
    std.crypto.auth.hmac.sha2.HmacSha256.create(&derived_key, "antfly/sql-setting-authority/key/v1", secret);
    var mac: [32]u8 = undefined;
    var hmac = std.crypto.auth.hmac.sha2.HmacSha256.init(&derived_key);
    hmac.update("sql-setting-catalog-v1");
    hmac.update(issuer);
    hmac.update(&.{0});
    hmac.update(@tagName(purpose));
    hmac.update(&expires_bytes);
    hmac.update(&body_hash);
    hmac.final(&mac);
    return mac;
}

test "setting grants bind purpose body key and expiry" {
    const alloc = std.testing.allocator;
    const grant = try sign(alloc, "separate-setting-authority-secret", "cluster-a", .read, "alice", 100);
    defer alloc.free(grant);
    try verify("separate-setting-authority-secret", "cluster-a", .read, "alice", 100, grant);
    try std.testing.expectError(error.Forbidden, verify("separate-setting-authority-secret", "cluster-a", .read, "bob", 100, grant));
    try std.testing.expectError(error.Forbidden, verify("separate-setting-authority-secret", "cluster-a", .admin, "alice", 100, grant));
    try std.testing.expectError(error.Forbidden, verify("separate-setting-authority-secret", "cluster-b", .read, "alice", 100, grant));
    try std.testing.expectError(error.Forbidden, verify("other-key", "cluster-a", .read, "alice", 100, grant));
    try std.testing.expectError(error.Forbidden, verify("separate-setting-authority-secret", "cluster-a", .read, "alice", 161, grant));
    try std.testing.expectError(error.SettingAuthorityUnavailable, sign(alloc, "", "cluster-a", .read, "alice", 100));
    try std.testing.expectError(error.SettingAuthorityUnavailable, verify("", "cluster-a", .read, "alice", 100, grant));
}
