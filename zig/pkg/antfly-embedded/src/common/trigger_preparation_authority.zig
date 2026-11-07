// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! User-authorized, body-bound trigger preparation. Node transport credentials
//! and a self-supplied processed-row checksum never grant executable authority.
const std = @import("std");
pub const maximum_token_bytes: usize = 4096;
pub const ttl_seconds: i64 = 30;
pub const header = "X-Antfly-SQL-Trigger-Preparation";
pub const command_digest_header = "X-Antfly-SQL-Trigger-Command-Digest";
pub const Purpose = enum { batch, prepare, native_batch, native_prepare };
pub const Scope = struct {
    purpose: Purpose,
    table_id: u64,
    group_id: u64,
    table_fingerprint: [32]u8,
    receipts_digest: [32]u8,
    body_digest: [32]u8,
    command_fence_digest: ?[32]u8 = null,

    fn validate(self: Scope) !void {
        if (self.table_id == 0 or self.group_id == 0 or std.mem.allEqual(u8, &self.table_fingerprint, 0) or std.mem.allEqual(u8, &self.receipts_digest, 0) or std.mem.allEqual(u8, &self.body_digest, 0)) return error.Forbidden;
        if (self.command_fence_digest) |digest| if (std.mem.allEqual(u8, &digest, 0)) return error.Forbidden;
    }
};
/// An API-owned callback plus immutable preparation identity. It deliberately
/// contains no request pointer or secret and has no serializable representation.
pub const Minter = struct {
    ptr: *anyopaque,
    sign_fn: *const fn (*anyopaque, std.mem.Allocator, Scope) anyerror![]u8,
    table_id: u64,
    table_fingerprint: [32]u8,
    receipts_digest: [32]u8,
    command_fence_digest: ?[32]u8 = null,

    pub fn jsonStringify(_: @This(), stream: anytype) @TypeOf(stream.*).Error!void {
        return stream.write(null);
    }
    pub fn jsonParse(_: std.mem.Allocator, _: anytype, _: std.json.ParseOptions) !@This() {
        return error.UnexpectedToken;
    }
    pub fn jsonParseFromValue(_: std.mem.Allocator, _: std.json.Value, _: std.json.ParseOptions) !@This() {
        return error.UnexpectedToken;
    }
    pub fn mint(self: Minter, a: std.mem.Allocator, input: Scope) ![]u8 {
        var scope = input;
        if (scope.table_id != self.table_id or !std.mem.eql(u8, &scope.table_fingerprint, &self.table_fingerprint) or !std.mem.eql(u8, &scope.receipts_digest, &self.receipts_digest)) return error.Forbidden;
        scope.command_fence_digest = self.command_fence_digest;
        try scope.validate();
        return self.sign_fn(self.ptr, a, scope);
    }
};
const Payload = struct { version: u8 = 1, scope: Scope, issued_at: i64, expires: i64 };
pub const Admission = struct { issued_at: i64, admitted_at: i64, expires: i64 };

pub fn bodyDigest(body: []const u8) [32]u8 {
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("antfly/sql-trigger-preparation/body/v1\x00");
    hash.update(body);
    var result: [32]u8 = undefined;
    hash.final(&result);
    return result;
}
fn mac(secret: []const u8, issuer: []const u8, encoded: []const u8) [32]u8 {
    var key: [32]u8 = undefined;
    std.crypto.auth.hmac.sha2.HmacSha256.create(&key, "antfly/sql-trigger-preparation/key/v1", secret);
    defer @memset(&key, 0);
    var hmac = std.crypto.auth.hmac.sha2.HmacSha256.init(&key);
    hmac.update("antfly/sql-trigger-preparation/v1\x00");
    hmac.update(issuer);
    hmac.update(&.{0});
    hmac.update(encoded);
    var result: [32]u8 = undefined;
    hmac.final(&result);
    return result;
}
fn requireProvider(secret: []const u8, issuer: []const u8, now: i64) !void {
    if (secret.len == 0 or issuer.len == 0 or issuer.len > 256 or std.mem.indexOfScalar(u8, issuer, 0) != null or !std.unicode.utf8ValidateSlice(issuer) or now < 0) return error.Forbidden;
}
pub fn sign(a: std.mem.Allocator, secret: []const u8, issuer: []const u8, scope: Scope, now: i64) ![]u8 {
    try requireProvider(secret, issuer, now);
    try scope.validate();
    const payload = try std.json.Stringify.valueAlloc(a, Payload{ .scope = scope, .issued_at = now, .expires = try std.math.add(i64, now, ttl_seconds) }, .{});
    defer a.free(payload);
    const size = std.base64.url_safe_no_pad.Encoder.calcSize(payload.len);
    if (size + 68 > maximum_token_bytes) return error.Forbidden;
    const encoded = try a.alloc(u8, size);
    defer a.free(encoded);
    _ = std.base64.url_safe_no_pad.Encoder.encode(encoded, payload);
    return std.fmt.allocPrint(a, "v1:{s}:{s}", .{ encoded, std.fmt.bytesToHex(mac(secret, issuer, encoded), .lower) });
}
/// Native ordered replay may use its durably recorded admission instant after
/// proving replay provenance. Live callers must always supply the receiver's
/// actual clock, never an instant reconstructed from their request payload.
pub fn verify(a: std.mem.Allocator, secret: []const u8, issuer: []const u8, expected: Scope, now: i64, token: []const u8) !Admission {
    try requireProvider(secret, issuer, now);
    try expected.validate();
    if (token.len > maximum_token_bytes) return error.Forbidden;
    var parts = std.mem.splitScalar(u8, token, ':');
    if (!std.mem.eql(u8, parts.next() orelse "", "v1")) return error.Forbidden;
    const encoded = parts.next() orelse return error.Forbidden;
    const signature = parts.next() orelse return error.Forbidden;
    if (parts.next() != null or signature.len != 64) return error.Forbidden;
    var supplied: [32]u8 = undefined;
    _ = std.fmt.hexToBytes(&supplied, signature) catch return error.Forbidden;
    if (!std.mem.eql(u8, &std.fmt.bytesToHex(supplied, .lower), signature) or !std.crypto.timing_safe.eql([32]u8, supplied, mac(secret, issuer, encoded))) return error.Forbidden;
    const size = std.base64.url_safe_no_pad.Decoder.calcSizeForSlice(encoded) catch return error.Forbidden;
    if (size > maximum_token_bytes) return error.Forbidden;
    const bytes = try a.alloc(u8, size);
    defer a.free(bytes);
    std.base64.url_safe_no_pad.Decoder.decode(bytes, encoded) catch return error.Forbidden;
    var parsed = std.json.parseFromSlice(Payload, a, bytes, .{}) catch |err| return if (err == error.OutOfMemory) err else error.Forbidden;
    defer parsed.deinit();
    const value = parsed.value;
    if (value.version != 1 or value.issued_at < 0 or value.issued_at > now or value.expires <= now or value.expires <= value.issued_at or value.expires - value.issued_at != ttl_seconds or !std.meta.eql(value.scope, expected)) return error.Forbidden;
    const canonical = try std.json.Stringify.valueAlloc(a, value, .{});
    defer a.free(canonical);
    if (!std.mem.eql(u8, bytes, canonical)) return error.Forbidden;
    return .{ .issued_at = value.issued_at, .admitted_at = now, .expires = value.expires };
}

test "trigger preparation authority binds distinct purpose native route program rows clock and issuer" {
    const a = std.testing.allocator;
    const scope: Scope = .{ .purpose = .prepare, .table_id = 7, .group_id = 11, .table_fingerprint = @splat(1), .receipts_digest = @splat(2), .body_digest = bodyDigest("exact private request body"), .command_fence_digest = @splat(3) };
    const secret = "user-authority-key-not-node-transport-key";
    const token = try sign(a, secret, "issuer", scope, 100);
    defer a.free(token);
    const admission = try verify(a, secret, "issuer", scope, 101, token);
    try std.testing.expectEqual(@as(i64, 101), admission.admitted_at);
    try std.testing.expectError(error.Forbidden, verify(a, "node-transport-secret", "issuer", scope, 101, token));
    try std.testing.expectError(error.Forbidden, verify(a, secret, "different-issuer", scope, 101, token));
    try std.testing.expectError(error.Forbidden, verify(a, secret, "issuer", scope, 99, token));
    try std.testing.expectError(error.Forbidden, verify(a, secret, "issuer", scope, 130, token));
    var changed = scope;
    changed.purpose = .batch;
    try std.testing.expectError(error.Forbidden, verify(a, secret, "issuer", changed, 101, token));
    changed = scope;
    changed.group_id += 1;
    try std.testing.expectError(error.Forbidden, verify(a, secret, "issuer", changed, 101, token));
    changed = scope;
    changed.table_fingerprint[0] ^= 1;
    try std.testing.expectError(error.Forbidden, verify(a, secret, "issuer", changed, 101, token));
    changed = scope;
    changed.receipts_digest[0] ^= 1;
    try std.testing.expectError(error.Forbidden, verify(a, secret, "issuer", changed, 101, token));
    changed = scope;
    changed.body_digest = bodyDigest("altered private request body");
    try std.testing.expectError(error.Forbidden, verify(a, secret, "issuer", changed, 101, token));
    changed = scope;
    changed.command_fence_digest = null;
    try std.testing.expectError(error.Forbidden, verify(a, secret, "issuer", changed, 101, token));
}
