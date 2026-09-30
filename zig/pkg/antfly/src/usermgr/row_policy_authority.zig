// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Statement-scoped role proof. Only an authenticated user manager may mint
//! this after pinning the effective role graph under its mutation mutex. The
//! owner verifies it against the exact table and policy epoch before admitting
//! a bounded statement; wire-supplied role lists have no authority.
const std = @import("std");

pub const ttl_seconds: i64 = 30;
pub const maximum_token_bytes: usize = 16 * 1024;
pub const maximum_roles: usize = 128;
pub const maximum_name_bytes: usize = 256;

pub const Access = enum { read, write };

pub const Scope = struct {
    table_id: u64,
    table: []const u8,
    database: []const u8,
    policy_generation: u64,
    catalog_epoch: u64,
    access: Access = .read,
};

pub const PinnedRoles = struct {
    principal: []const u8,
    roles: []const []const u8,
    auth_revision: u64,
};

pub const Payload = struct {
    principal: []const u8,
    roles: []const []const u8,
    auth_revision: u64,
    table_id: u64,
    table: []const u8,
    database: []const u8,
    policy_generation: u64,
    catalog_epoch: u64,
    access: Access,
    expires: i64,
};

pub fn sign(
    alloc: std.mem.Allocator,
    secret: []const u8,
    issuer: []const u8,
    pinned: PinnedRoles,
    scope: Scope,
    now_seconds: i64,
) ![]u8 {
    if (secret.len < 32 or issuer.len == 0) return error.RowPolicyAuthorityUnavailable;
    if (!validName(pinned.principal) or !validName(scope.table) or !validName(scope.database) or
        pinned.roles.len > maximum_roles or pinned.auth_revision == 0 or scope.table_id == 0 or
        scope.policy_generation == 0 or scope.catalog_epoch == 0)
        return error.InvalidRowPolicyPrincipal;
    for (pinned.roles) |role| if (!validName(role)) return error.InvalidRowPolicyPrincipal;
    const expires = try std.math.add(i64, now_seconds, ttl_seconds);
    const payload = try std.json.Stringify.valueAlloc(alloc, Payload{
        .principal = pinned.principal,
        .roles = pinned.roles,
        .auth_revision = pinned.auth_revision,
        .table_id = scope.table_id,
        .table = scope.table,
        .database = scope.database,
        .policy_generation = scope.policy_generation,
        .catalog_epoch = scope.catalog_epoch,
        .access = scope.access,
        .expires = expires,
    }, .{});
    defer alloc.free(payload);
    const payload_size = std.base64.url_safe_no_pad.Encoder.calcSize(payload.len);
    if (payload_size + 68 > maximum_token_bytes) return error.InvalidRowPolicyPrincipal;
    const encoded = try alloc.alloc(u8, payload_size);
    defer alloc.free(encoded);
    _ = std.base64.url_safe_no_pad.Encoder.encode(encoded, payload);
    const mac = digest(secret, issuer, encoded);
    const hex = std.fmt.bytesToHex(mac, .lower);
    return std.fmt.allocPrint(alloc, "v1:{s}:{s}", .{ encoded, &hex });
}

/// The returned parsed value owns the verified principal and role slices.
pub fn verify(
    alloc: std.mem.Allocator,
    secret: []const u8,
    issuer: []const u8,
    scope: Scope,
    now_seconds: i64,
    token: []const u8,
) !std.json.Parsed(Payload) {
    if (secret.len < 32 or issuer.len == 0) return error.RowPolicyAuthorityUnavailable;
    if (token.len > maximum_token_bytes) return error.Forbidden;
    var parts = std.mem.splitScalar(u8, token, ':');
    if (!std.mem.eql(u8, parts.next() orelse "", "v1")) return error.Forbidden;
    const encoded = parts.next() orelse return error.Forbidden;
    const signature = parts.next() orelse return error.Forbidden;
    if (parts.next() != null or signature.len != 64) return error.Forbidden;
    var provided: [32]u8 = undefined;
    _ = std.fmt.hexToBytes(&provided, signature) catch return error.Forbidden;
    const expected = digest(secret, issuer, encoded);
    if (!std.crypto.timing_safe.eql([32]u8, expected, provided)) return error.Forbidden;

    const payload_size = std.base64.url_safe_no_pad.Decoder.calcSizeForSlice(encoded) catch return error.Forbidden;
    const payload = try alloc.alloc(u8, payload_size);
    defer alloc.free(payload);
    std.base64.url_safe_no_pad.Decoder.decode(payload, encoded) catch return error.Forbidden;
    var parsed = std.json.parseFromSlice(Payload, alloc, payload, .{ .allocate = .alloc_always, .ignore_unknown_fields = false }) catch return error.Forbidden;
    errdefer parsed.deinit();
    const value = parsed.value;
    if (!validName(value.principal) or !validName(value.table) or !validName(value.database) or
        value.roles.len > maximum_roles or value.auth_revision == 0 or
        value.table_id != scope.table_id or
        value.policy_generation != scope.policy_generation or
        value.catalog_epoch != scope.catalog_epoch or
        value.access != scope.access or
        !std.mem.eql(u8, value.table, scope.table) or !std.mem.eql(u8, value.database, scope.database) or
        value.expires < now_seconds or value.expires > now_seconds +| ttl_seconds)
        return error.Forbidden;
    for (value.roles) |role| if (!validName(role)) return error.Forbidden;
    return parsed;
}

fn validName(name: []const u8) bool {
    return name.len > 0 and name.len <= maximum_name_bytes and
        std.mem.indexOfScalar(u8, name, 0) == null;
}

fn digest(secret: []const u8, issuer: []const u8, payload: []const u8) [32]u8 {
    var key: [32]u8 = undefined;
    std.crypto.auth.hmac.sha2.HmacSha256.create(&key, "antfly/row-policy-principal/key/v1", secret);
    var hmac = std.crypto.auth.hmac.sha2.HmacSha256.init(&key);
    hmac.update("antfly/row-policy-principal/v1");
    hmac.update(issuer);
    hmac.update(&.{0});
    hmac.update(payload);
    var mac: [32]u8 = undefined;
    hmac.final(&mac);
    return mac;
}

test "role proof binds principal, table, policy epoch, signer and expiry" {
    const alloc = std.testing.allocator;
    const key = "1234567890abcdef1234567890abcdef";
    const scope: Scope = .{ .table_id = 41, .table = "orders", .database = "main", .policy_generation = 8, .catalog_epoch = 31 };
    const token = try sign(alloc, key, "cluster-a", .{
        .principal = "alice",
        .roles = &.{ "reader", "tenant-7" },
        .auth_revision = 5,
    }, scope, 100);
    defer alloc.free(token);
    var verified = try verify(alloc, key, "cluster-a", scope, 100, token);
    defer verified.deinit();
    try std.testing.expectEqualStrings("alice", verified.value.principal);
    try std.testing.expectEqual(@as(usize, 2), verified.value.roles.len);
    try std.testing.expectError(error.Forbidden, verify(alloc, key, "cluster-a", .{ .table_id = 41, .table = "other", .database = "main", .policy_generation = 8, .catalog_epoch = 31 }, 100, token));
    try std.testing.expectError(error.Forbidden, verify(alloc, key, "cluster-a", .{ .table_id = 42, .table = "orders", .database = "main", .policy_generation = 8, .catalog_epoch = 31 }, 100, token));
    try std.testing.expectError(error.Forbidden, verify(alloc, key, "cluster-a", .{ .table_id = 41, .table = "orders", .database = "other", .policy_generation = 8, .catalog_epoch = 31 }, 100, token));
    try std.testing.expectError(error.Forbidden, verify(alloc, key, "cluster-a", .{ .table_id = 41, .table = "orders", .database = "main", .policy_generation = 9, .catalog_epoch = 31 }, 100, token));
    try std.testing.expectError(error.Forbidden, verify(alloc, key, "cluster-a", .{ .table_id = 41, .table = "orders", .database = "main", .policy_generation = 8, .catalog_epoch = 31, .access = .write }, 100, token));
    try std.testing.expectError(error.Forbidden, verify(alloc, key, "cluster-b", scope, 100, token));
    try std.testing.expectError(error.Forbidden, verify(alloc, key, "cluster-a", scope, 131, token));
}
