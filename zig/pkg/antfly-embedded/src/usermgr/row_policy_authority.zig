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

/// Per-row command provenance carried by the private native predicate/intent
/// envelope, covered by durable sealing and authenticated native transport.
/// Ordinary REST mutations retain their existing native action semantics.
pub const MutationRequirement = enum(u8) {
    native,
    insert,
    insert_select,
    update,
    update_select,
    delete,
    delete_select,

    pub fn requiresSelect(self: MutationRequirement) bool {
        return self == .insert_select or self == .update_select or self == .delete_select;
    }
    pub fn isInsert(self: MutationRequirement) bool {
        return self == .insert or self == .insert_select;
    }
    pub fn isUpdate(self: MutationRequirement) bool {
        return self == .update or self == .update_select;
    }
    pub fn isDelete(self: MutationRequirement) bool {
        return self == .delete or self == .delete_select;
    }
};

/// Authenticated SQL execution intent, never a public request field. Mutation
/// scans use their own USING policy; SELECT is an additional requirement only
/// when the statement actually reads target columns or returns their values.
pub const Purpose = enum {
    ordinary,
    update,
    update_select,
    delete,
    delete_select,
    normalize,
    normalize_select,
    integrity,
    sql_write,

    pub fn requiresWritePermission(self: Purpose) bool {
        return self != .ordinary;
    }
    pub fn normalizationOnly(self: Purpose) bool {
        return self == .normalize or self == .normalize_select;
    }
    pub fn requiresSelectPermission(self: Purpose) bool {
        return self == .update_select or self == .delete_select or self == .normalize_select;
    }
};

pub const Scope = struct {
    table_id: u64,
    table: []const u8,
    database: []const u8,
    policy_generation: u64,
    catalog_epoch: u64,
    access: Access = .read,
    purpose: Purpose = .ordinary,
};

pub const PinnedRoles = struct {
    principal: []const u8,
    /// Read-only view delegation selects policies as this principal while SQL
    /// expressions/settings continue to observe the authenticated caller.
    policy_principal: ?[]const u8 = null,
    setting_principal: ?[]const u8 = null,
    roles: []const []const u8,
    auth_revision: u64,
};

pub const Payload = struct {
    principal: []const u8,
    policy_principal: ?[]const u8 = null,
    setting_principal: ?[]const u8 = null,
    roles: []const []const u8,
    auth_revision: u64,
    table_id: u64,
    table: []const u8,
    database: []const u8,
    policy_generation: u64,
    catalog_epoch: u64,
    access: Access,
    expires: i64,
    purpose: Purpose = .ordinary,

    pub fn jsonStringify(self: Payload, writer: anytype) !void {
        if (self.purpose != .ordinary) return writer.write(.{
            .principal = self.principal,
            .policy_principal = self.policy_principal,
            .setting_principal = self.setting_principal,
            .roles = self.roles,
            .auth_revision = self.auth_revision,
            .table_id = self.table_id,
            .table = self.table,
            .database = self.database,
            .policy_generation = self.policy_generation,
            .catalog_epoch = self.catalog_epoch,
            .access = self.access,
            .expires = self.expires,
            .purpose = self.purpose,
        });
        // Ordinary proofs retain their original shape for strict older
        // decoders. View extensions require the coordinated view capability.
        if (self.policy_principal == null and self.setting_principal == null) return writer.write(.{
            .principal = self.principal,
            .roles = self.roles,
            .auth_revision = self.auth_revision,
            .table_id = self.table_id,
            .table = self.table,
            .database = self.database,
            .policy_generation = self.policy_generation,
            .catalog_epoch = self.catalog_epoch,
            .access = self.access,
            .expires = self.expires,
        });
        return writer.write(.{
            .principal = self.principal,
            .policy_principal = self.policy_principal,
            .setting_principal = self.setting_principal,
            .roles = self.roles,
            .auth_revision = self.auth_revision,
            .table_id = self.table_id,
            .table = self.table,
            .database = self.database,
            .policy_generation = self.policy_generation,
            .catalog_epoch = self.catalog_epoch,
            .access = self.access,
            .expires = self.expires,
        });
    }
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
    if ((scope.purpose == .sql_write and scope.access != .write) or
        (scope.purpose != .ordinary and scope.purpose != .sql_write and scope.access != .read)) return error.InvalidRowPolicyPrincipal;
    if (!validName(pinned.principal) or !validName(scope.table) or !validName(scope.database) or
        pinned.roles.len > maximum_roles or pinned.auth_revision == 0 or scope.table_id == 0 or
        scope.policy_generation == 0 or scope.catalog_epoch == 0)
        return error.InvalidRowPolicyPrincipal;
    for (pinned.roles) |role| if (!validName(role)) return error.InvalidRowPolicyPrincipal;
    if (pinned.policy_principal) |principal| {
        if (!validName(principal) or scope.access != .read) return error.InvalidRowPolicyPrincipal;
    }
    if (pinned.setting_principal) |principal| if (!validName(principal)) return error.InvalidRowPolicyPrincipal;
    const expires = try std.math.add(i64, now_seconds, ttl_seconds);
    const payload = try std.json.Stringify.valueAlloc(alloc, Payload{
        .principal = pinned.principal,
        .policy_principal = pinned.policy_principal,
        .setting_principal = pinned.setting_principal,
        .roles = pinned.roles,
        .auth_revision = pinned.auth_revision,
        .table_id = scope.table_id,
        .table = scope.table,
        .database = scope.database,
        .policy_generation = scope.policy_generation,
        .catalog_epoch = scope.catalog_epoch,
        .access = scope.access,
        .expires = expires,
        .purpose = scope.purpose,
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
    var parsed = std.json.parseFromSlice(Payload, alloc, payload, .{ .allocate = .alloc_always, .ignore_unknown_fields = false }) catch |err| {
        if (err == error.OutOfMemory) return err;
        return error.Forbidden;
    };
    errdefer parsed.deinit();
    const value = parsed.value;
    if ((value.purpose == .sql_write and value.access != .write) or
        (value.purpose != .ordinary and value.purpose != .sql_write and value.access != .read) or
        (scope.purpose != .ordinary and value.purpose != scope.purpose)) return error.Forbidden;
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
    if (value.policy_principal) |principal| {
        if (!validName(principal) or value.access != .read) return error.Forbidden;
    }
    if (value.setting_principal) |principal| if (!validName(principal)) return error.Forbidden;
    return parsed;
}

fn validName(name: []const u8) bool {
    return name.len > 0 and name.len <= maximum_name_bytes and
        std.mem.indexOfScalar(u8, name, 0) == null and std.unicode.utf8ValidateSlice(name);
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
    const legacy = try std.json.Stringify.valueAlloc(alloc, verified.value, .{});
    defer alloc.free(legacy);
    try std.testing.expect(std.mem.indexOf(u8, legacy, "policy_principal") == null);
    try std.testing.expect(std.mem.indexOf(u8, legacy, "setting_principal") == null);
    try std.testing.expectError(error.Forbidden, verify(alloc, key, "cluster-a", .{ .table_id = 41, .table = "other", .database = "main", .policy_generation = 8, .catalog_epoch = 31 }, 100, token));
    try std.testing.expectError(error.Forbidden, verify(alloc, key, "cluster-a", .{ .table_id = 42, .table = "orders", .database = "main", .policy_generation = 8, .catalog_epoch = 31 }, 100, token));
    try std.testing.expectError(error.Forbidden, verify(alloc, key, "cluster-a", .{ .table_id = 41, .table = "orders", .database = "other", .policy_generation = 8, .catalog_epoch = 31 }, 100, token));
    try std.testing.expectError(error.Forbidden, verify(alloc, key, "cluster-a", .{ .table_id = 41, .table = "orders", .database = "main", .policy_generation = 9, .catalog_epoch = 31 }, 100, token));
    try std.testing.expectError(error.Forbidden, verify(alloc, key, "cluster-a", .{ .table_id = 41, .table = "orders", .database = "main", .policy_generation = 8, .catalog_epoch = 31, .access = .write }, 100, token));
    try std.testing.expectError(error.Forbidden, verify(alloc, key, "cluster-b", scope, 100, token));
    try std.testing.expectError(error.Forbidden, verify(alloc, key, "cluster-a", scope, 131, token));
}

test "view role proof authenticates policy selection separately and cannot authorize writes" {
    const a = std.testing.allocator;
    const key = "1234567890abcdef1234567890abcdef";
    const scope: Scope = .{ .table_id = 7, .table = "rows", .database = "main", .policy_generation = 2, .catalog_epoch = 3 };
    const roles: PinnedRoles = .{ .principal = "caller", .policy_principal = "owner", .setting_principal = "api-key:caller", .roles = &.{"owner_reader"}, .auth_revision = 9 };
    const token = try sign(a, key, "cluster", roles, scope, 100);
    defer a.free(token);
    var verified = try verify(a, key, "cluster", scope, 100, token);
    defer verified.deinit();
    try std.testing.expectEqualStrings("caller", verified.value.principal);
    try std.testing.expectEqualStrings("owner", verified.value.policy_principal.?);
    try std.testing.expectEqualStrings("api-key:caller", verified.value.setting_principal.?);
    try std.testing.expectEqualStrings("owner_reader", verified.value.roles[0]);
    var write = scope;
    write.access = .write;
    try std.testing.expectError(error.InvalidRowPolicyPrincipal, sign(a, key, "cluster", roles, write, 100));
    try std.testing.expectError(error.Forbidden, verify(a, key, "cluster", write, 100, token));
    var invalid = roles;
    invalid.policy_principal = "";
    try std.testing.expectError(error.InvalidRowPolicyPrincipal, sign(a, key, "cluster", invalid, scope, 100));
}
