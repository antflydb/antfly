// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! A user-authorized TEMP owner capability, separate from node transport auth.
//! The receiver supplies exact prepared body/route identity and its own clock.
const std = @import("std");
const persistence = @import("relation_persistence.zig");
pub const header = "X-Antfly-SQL-Relation-Owner";
pub const maximum_token_bytes: usize = 4096;
pub const ttl_seconds: i64 = 30;
pub const Purpose = enum { read, write, prepare, resolve };
pub const Scope = struct {
    purpose: Purpose,
    table_id: u64,
    group_id: u64,
    descriptor_fingerprint: [32]u8,
    body_digest: [32]u8,
    command_fence_digest: ?[32]u8 = null,

    fn validate(self: Scope) !void {
        if (self.table_id == 0 or self.group_id == 0 or std.mem.allEqual(u8, &self.descriptor_fingerprint, 0) or std.mem.allEqual(u8, &self.body_digest, 0)) return error.Forbidden;
    }
};
/// Process-long authenticated API callback. Routes/clients copy only this
/// callback capability and immutable owner/fence identity, never config keys
/// or borrowed request contexts. This type has no wire representation.
pub const Minter = struct {
    ptr: *anyopaque,
    sign_fn: *const fn (*anyopaque, std.mem.Allocator, persistence.Owner, Scope) anyerror![]u8,
    owner: persistence.Owner,
    command_fence_digest: ?[32]u8 = null,

    pub fn mint(self: Minter, a: std.mem.Allocator, scope: Scope) ![]u8 {
        var bound = scope;
        bound.command_fence_digest = self.command_fence_digest;
        return self.sign_fn(self.ptr, a, self.owner, bound);
    }
    /// A runtime authority is never deserializable. Containers omit this
    /// field; a direct optional serialization can only lose authority to null,
    /// never disclose or reconstruct process pointers/callbacks.
    pub fn jsonStringify(_: @This(), stream: anytype) @TypeOf(stream.*).Error!void {
        return stream.write(null);
    }
    pub fn jsonParse(_: std.mem.Allocator, _: anytype, _: std.json.ParseOptions) !@This() {
        return error.UnexpectedToken;
    }
    pub fn jsonParseFromValue(_: std.mem.Allocator, _: std.json.Value, _: std.json.ParseOptions) !@This() {
        return error.UnexpectedToken;
    }
};
pub const command_digest_header = "X-Antfly-SQL-Relation-Command-Digest";

const Payload = struct { version: u8 = 1, owner: persistence.Owner, scope: Scope, issued_at: i64, expires: i64 };

pub fn bodyDigest(body: []const u8) [32]u8 {
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("antfly/sql-relation-owner/body/v1\x00");
    hash.update(body);
    var result: [32]u8 = undefined;
    hash.final(&result);
    return result;
}
fn mac(secret: []const u8, issuer: []const u8, encoded: []const u8) [32]u8 {
    var key: [32]u8 = undefined;
    std.crypto.auth.hmac.sha2.HmacSha256.create(&key, "antfly/sql-relation-owner/key/v1", secret);
    var hmac = std.crypto.auth.hmac.sha2.HmacSha256.init(&key);
    hmac.update("antfly/sql-relation-owner/v1\x00");
    hmac.update(issuer);
    hmac.update(&.{0});
    hmac.update(encoded);
    var result: [32]u8 = undefined;
    hmac.final(&result);
    return result;
}
pub fn sign(a: std.mem.Allocator, secret: []const u8, issuer: []const u8, owner: persistence.Owner, scope: Scope, now_seconds: i64) ![]u8 {
    if (secret.len == 0 or issuer.len == 0 or issuer.len > 256 or now_seconds < 0) return error.Forbidden;
    try owner.validate();
    try scope.validate();
    const payload = try std.json.Stringify.valueAlloc(a, Payload{ .owner = owner, .scope = scope, .issued_at = now_seconds, .expires = try std.math.add(i64, now_seconds, ttl_seconds) }, .{});
    defer a.free(payload);
    const size = std.base64.url_safe_no_pad.Encoder.calcSize(payload.len);
    if (size + 68 > maximum_token_bytes) return error.Forbidden;
    const encoded = try a.alloc(u8, size);
    defer a.free(encoded);
    _ = std.base64.url_safe_no_pad.Encoder.encode(encoded, payload);
    return std.fmt.allocPrint(a, "v1:{s}:{s}", .{ encoded, std.fmt.bytesToHex(mac(secret, issuer, encoded), .lower) });
}
/// Returns an owned-by-value identity only after authentication and exact
/// operation binding. The caller must also match the current Descriptor owner.
pub fn verify(a: std.mem.Allocator, secret: []const u8, issuer: []const u8, scope: Scope, descriptor: persistence.Descriptor, now_seconds: i64, token: []const u8) !persistence.Owner {
    if (secret.len == 0 or issuer.len == 0 or issuer.len > 256 or token.len > maximum_token_bytes or now_seconds < 0) return error.Forbidden;
    try scope.validate();
    var parts = std.mem.splitScalar(u8, token, ':');
    if (!std.mem.eql(u8, parts.next() orelse "", "v1")) return error.Forbidden;
    const encoded = parts.next() orelse return error.Forbidden;
    const signature = parts.next() orelse return error.Forbidden;
    if (parts.next() != null or signature.len != 64) return error.Forbidden;
    var supplied: [32]u8 = undefined;
    _ = std.fmt.hexToBytes(&supplied, signature) catch return error.Forbidden;
    if (!std.crypto.timing_safe.eql([32]u8, supplied, mac(secret, issuer, encoded))) return error.Forbidden;
    const size = std.base64.url_safe_no_pad.Decoder.calcSizeForSlice(encoded) catch return error.Forbidden;
    if (size > maximum_token_bytes) return error.Forbidden;
    const bytes = try a.alloc(u8, size);
    defer a.free(bytes);
    std.base64.url_safe_no_pad.Decoder.decode(bytes, encoded) catch return error.Forbidden;
    var parsed = std.json.parseFromSlice(Payload, a, bytes, .{}) catch |err| return if (err == error.OutOfMemory) err else error.Forbidden;
    defer parsed.deinit();
    const value = parsed.value;
    if (value.version != 1 or value.issued_at > now_seconds or value.expires <= now_seconds or value.expires <= value.issued_at or value.expires - value.issued_at > ttl_seconds or
        !std.meta.eql(value.scope, scope) or !std.mem.eql(u8, &scope.descriptor_fingerprint, &try descriptor.fingerprint())) return error.Forbidden;
    try value.owner.validate();
    try descriptor.requireConnectionOwner(value.owner);
    return value.owner;
}

test "native relation owner grant binds route body purpose owner signer expiry and command" {
    const a = std.testing.allocator;
    const owner: persistence.Owner = .{ .issuer_node_id = 1, .connection_id = @splat(1), .wire_epoch = @splat(2), .execution_instance = @splat(3), .connection_generation = 4, .principal_digest = @splat(5) };
    const descriptor: persistence.Descriptor = .{ .kind = .temporary, .owner_node_id = 2, .owner_store_id = 3, .generation_instance = @splat(8), .data_generation = 9, .owner = owner };
    const scope: Scope = .{ .purpose = .read, .table_id = 11, .group_id = 12, .descriptor_fingerprint = try descriptor.fingerprint(), .body_digest = bodyDigest("prepared exact body"), .command_fence_digest = @splat(7) };
    const key = "native-relation-user-authority-secret-v1";
    const token = try sign(a, key, "issuer-a", owner, scope, 100);
    defer a.free(token);
    try std.testing.expect(owner.eql(try verify(a, key, "issuer-a", scope, descriptor, 110, token)));
    try std.testing.expectError(error.Forbidden, verify(a, key, "issuer-a", scope, descriptor, 99, token));
    try std.testing.expectError(error.Forbidden, verify(a, key, "issuer-a", scope, descriptor, 130, token));
    try std.testing.expectError(error.Forbidden, verify(a, key, "issuer-a", scope, descriptor, 131, token));
    try std.testing.expectError(error.Forbidden, verify(a, key, "issuer-b", scope, descriptor, 110, token));
    try std.testing.expectError(error.Forbidden, verify(a, "another-secret-key-with-32-bytes-v1", "issuer-a", scope, descriptor, 110, token));
    inline for (.{ "purpose", "table_id", "group_id", "descriptor_fingerprint", "body_digest", "command_fence_digest" }) |field| {
        var changed = scope;
        if (comptime std.mem.eql(u8, field, "purpose")) changed.purpose = .write else if (comptime std.mem.eql(u8, field, "table_id")) changed.table_id += 1 else if (comptime std.mem.eql(u8, field, "group_id")) changed.group_id += 1 else if (comptime std.mem.eql(u8, field, "command_fence_digest")) changed.command_fence_digest = @as([32]u8, @splat(9)) else @field(changed, field) = @splat(9);
        try std.testing.expectError(error.Forbidden, verify(a, key, "issuer-a", changed, descriptor, 110, token));
    }
    var other = descriptor;
    other.owner.?.wire_epoch = @splat(99);
    try std.testing.expectError(error.Forbidden, verify(a, key, "issuer-a", scope, other, 110, token));
    const configured_short = try sign(a, "configured-user-key", "issuer-a", owner, scope, 100);
    defer a.free(configured_short);
    try std.testing.expect(owner.eql(try verify(a, "configured-user-key", "issuer-a", scope, descriptor, 110, configured_short)));
    const forged = try a.dupe(u8, token);
    defer a.free(forged);
    forged[forged.len - 1] = if (forged[forged.len - 1] == '0') '1' else '0';
    try std.testing.expectError(error.Forbidden, verify(a, key, "issuer-a", scope, descriptor, 110, forged));
}
