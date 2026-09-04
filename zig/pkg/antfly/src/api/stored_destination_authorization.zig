// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License at https://www.antfly.io/licensing/ELv2-license

//! Durable authorization envelopes for catalog-stored write destinations.
//!
//! Admission code first checks the caller's live permissions, then seals the
//! normalized catalog JSON with the exact destination set. Background workers
//! validate that server-authored set whenever work starts or resumes. Public
//! admission always overwrites this reserved field, so a client cannot mint a
//! grant by including it in its request.

const std = @import("std");
const usermgr = @import("../usermgr/mod.zig");

pub const grant_field = "_antfly_destination_authorization_v1";
pub const catalog_service_principal = "service:catalog";
pub const auth_disabled_principal = "service:auth-disabled";
const auth_disabled_signature = "auth-disabled";
const grant_domain = "antfly.destination-authorization.v1";

pub const Authorizer = struct {
    manager: ?*const usermgr.UserManager = null,
    auth_enabled: bool = false,

    fn signatureAlloc(
        self: Authorizer,
        alloc: std.mem.Allocator,
        principal: []const u8,
        payload: []const u8,
    ) ![]u8 {
        if (std.mem.eql(u8, principal, auth_disabled_principal)) {
            if (!self.auth_enabled) return try alloc.dupe(u8, auth_disabled_signature);
            return error.StoredDestinationAuthorizationRevoked;
        }
        if (std.mem.eql(u8, principal, catalog_service_principal))
            return error.StoredDestinationCredentialUnsupported;
        if (!std.mem.startsWith(u8, principal, "basic:") and
            !std.mem.startsWith(u8, principal, "api-key:"))
            return error.StoredDestinationCredentialUnsupported;
        const manager = self.manager orelse return error.StoredDestinationAuthorizationRevoked;
        const mac = manager.destinationGrantMac(principal, payload) catch |err| switch (err) {
            error.InvalidDestinationGrantPrincipal => return error.StoredDestinationCredentialUnsupported,
            else => return error.StoredDestinationAuthorizationRevoked,
        };
        const hex = std.fmt.bytesToHex(mac, .lower);
        return try alloc.dupe(u8, &hex);
    }

    pub fn authorize(
        self: Authorizer,
        alloc: std.mem.Allocator,
        principal: []const u8,
        destinations: []const []const u8,
        payload: []const u8,
        signature: []const u8,
    ) !void {
        const expected = try self.signatureAlloc(alloc, principal, payload);
        defer alloc.free(expected);
        if (!constantTimeEql(expected, signature))
            return error.StoredDestinationAuthorizationInvalid;
        if (std.mem.eql(u8, principal, auth_disabled_principal)) return;
        const manager = self.manager orelse return error.StoredDestinationAuthorizationRevoked;
        const permissions = if (std.mem.startsWith(u8, principal, "basic:"))
            manager.getPermissionsForUser(principal["basic:".len..]) catch |err| switch (err) {
                error.OutOfMemory => return err,
                else => return error.StoredDestinationAuthorizationRevoked,
            }
        else if (std.mem.startsWith(u8, principal, "api-key:"))
            manager.effectiveApiKeyPermissions(principal["api-key:".len..]) catch |err| switch (err) {
                error.OutOfMemory => return err,
                else => return error.StoredDestinationAuthorizationRevoked,
            }
        else
            return error.StoredDestinationAuthorizationRevoked;
        defer {
            for (permissions) |*permission| permission.deinit(manager.alloc);
            manager.alloc.free(permissions);
        }
        for (destinations) |destination| {
            if (!permissionsAllowWrite(permissions, destination))
                return error.StoredDestinationAuthorizationRevoked;
        }
    }
};

fn constantTimeEql(left: []const u8, right: []const u8) bool {
    if (left.len != right.len) return false;
    var diff: u8 = 0;
    for (left, right) |a, b| diff |= a ^ b;
    return diff == 0;
}

fn permissionsAllowWrite(permissions: []const usermgr.Permission, table_name: []const u8) bool {
    for (permissions) |permission| {
        const type_match = permission.resource_type == .@"*" or permission.resource_type == .table;
        const resource_match = std.mem.eql(u8, permission.resource, "*") or
            std.mem.eql(u8, permission.resource, table_name);
        if (type_match and resource_match and (permission.type == .write or permission.type == .admin))
            return true;
    }
    return false;
}

pub fn destinationConfigFingerprintAlloc(
    alloc: std.mem.Allocator,
    replication_sources_json: []const u8,
    indexes_json: []const u8,
) ![]u8 {
    var hasher = std.crypto.hash.sha2.Sha256.init(.{});
    var length_bytes: [8]u8 = undefined;
    std.mem.writeInt(u64, &length_bytes, @intCast(replication_sources_json.len), .little);
    hasher.update(&length_bytes);
    hasher.update(replication_sources_json);
    std.mem.writeInt(u64, &length_bytes, @intCast(indexes_json.len), .little);
    hasher.update(&length_bytes);
    hasher.update(indexes_json);
    var digest: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
    hasher.final(&digest);
    const hex = std.fmt.bytesToHex(digest, .lower);
    return try alloc.dupe(u8, &hex);
}

pub fn destinationConfigFingerprintMatches(
    replication_sources_json: []const u8,
    indexes_json: []const u8,
    expected: []const u8,
) bool {
    if (expected.len != std.crypto.hash.sha2.Sha256.digest_length * 2) return false;
    var hasher = std.crypto.hash.sha2.Sha256.init(.{});
    var length_bytes: [8]u8 = undefined;
    std.mem.writeInt(u64, &length_bytes, @intCast(replication_sources_json.len), .little);
    hasher.update(&length_bytes);
    hasher.update(replication_sources_json);
    std.mem.writeInt(u64, &length_bytes, @intCast(indexes_json.len), .little);
    hasher.update(&length_bytes);
    hasher.update(indexes_json);
    var digest: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
    hasher.final(&digest);
    const hex = std.fmt.bytesToHex(digest, .lower);
    return std.mem.eql(u8, &hex, expected);
}

/// Legacy restore jobs predate durable destination grants. They may resume
/// automatically only when the backed-up definition cannot write to another
/// table. This parser is intentionally shared with sealing so schema drift
/// fails closed instead of silently classifying a new sink form as harmless.
pub fn configurationHasDestinations(
    alloc: std.mem.Allocator,
    replication_sources_json: []const u8,
    indexes_json: []const u8,
) !bool {
    var parsed_sources = try std.json.parseFromSlice(std.json.Value, alloc, replication_sources_json, .{});
    defer parsed_sources.deinit();
    if (parsed_sources.value != .array) return error.InvalidStoredDestinationConfig;
    for (parsed_sources.value.array.items) |source| {
        if (source != .object) return error.InvalidStoredDestinationConfig;
        var destinations = std.ArrayListUnmanaged([]const u8).empty;
        collectRouteDestinations(alloc, source, &destinations) catch |err| {
            destinations.deinit(alloc);
            return err;
        };
        const has_destinations = destinations.items.len > 0;
        destinations.deinit(alloc);
        if (has_destinations) return true;
    }

    var parsed_indexes = try std.json.parseFromSlice(std.json.Value, alloc, indexes_json, .{});
    defer parsed_indexes.deinit();
    if (parsed_indexes.value != .object) return error.InvalidStoredDestinationConfig;
    return try valueHasResolverDestination(parsed_indexes.value);
}

fn valueHasResolverDestination(value: std.json.Value) !bool {
    switch (value) {
        .object => |object| {
            if (object.get("resolvers")) |resolvers| {
                if (resolvers != .array) return error.InvalidStoredDestinationConfig;
                for (resolvers.array.items) |resolver| {
                    if (resolver == .string) continue;
                    if (resolver != .object) return error.InvalidStoredDestinationConfig;
                    const table = resolver.object.get("table") orelse
                        return error.InvalidStoredDestinationConfig;
                    if (table != .string or table.string.len == 0)
                        return error.InvalidStoredDestinationConfig;
                    return true;
                }
            }
            var it = object.iterator();
            while (it.next()) |entry| {
                if (std.mem.eql(u8, entry.key_ptr.*, "resolvers")) continue;
                if (try valueHasResolverDestination(entry.value_ptr.*)) return true;
            }
        },
        .array => |array| for (array.items) |item| {
            if (try valueHasResolverDestination(item)) return true;
        },
        else => {},
    }
    return false;
}

pub fn sealReplicationSourcesJsonForPrincipalAlloc(
    alloc: std.mem.Allocator,
    json: []const u8,
    source_table: []const u8,
    principal: []const u8,
    authorizer: ?Authorizer,
) ![]u8 {
    if (principal.len == 0) return error.InvalidStoredDestinationConfig;
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, json, .{});
    defer parsed.deinit();
    if (parsed.value != .array) return error.InvalidStoredDestinationConfig;

    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    try out.append(alloc, '[');
    for (parsed.value.array.items, 0..) |source, index| {
        if (index > 0) try out.append(alloc, ',');
        if (source != .object) return error.InvalidStoredDestinationConfig;
        var destinations = std.ArrayListUnmanaged([]const u8).empty;
        defer destinations.deinit(alloc);
        try collectRouteDestinations(alloc, source, &destinations);
        try appendSealedObject(alloc, &out, source.object, source_table, destinations.items, principal, authorizer);
    }
    try out.append(alloc, ']');
    return try out.toOwnedSlice(alloc);
}

pub fn sealIndexesJsonForPrincipalAlloc(
    alloc: std.mem.Allocator,
    json: []const u8,
    source_table: []const u8,
    principal: []const u8,
    authorizer: ?Authorizer,
) ![]u8 {
    if (principal.len == 0) return error.InvalidStoredDestinationConfig;
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, json, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidStoredDestinationConfig;

    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    try appendValueSealingResolvers(alloc, &out, parsed.value, source_table, principal, authorizer);
    return try out.toOwnedSlice(alloc);
}

pub fn sealIndexJsonForPrincipalAlloc(
    alloc: std.mem.Allocator,
    json: []const u8,
    source_table: []const u8,
    principal: []const u8,
    authorizer: ?Authorizer,
) ![]u8 {
    if (principal.len == 0) return error.InvalidStoredDestinationConfig;
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, json, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidStoredDestinationConfig;
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    try appendValueSealingResolvers(alloc, &out, parsed.value, source_table, principal, authorizer);
    return try out.toOwnedSlice(alloc);
}

pub fn validateReplicationSourceValue(alloc: std.mem.Allocator, source: std.json.Value) !void {
    return try authorizeReplicationSourceValue(alloc, source, "", null);
}

pub fn authorizeReplicationSourcesJson(
    alloc: std.mem.Allocator,
    json: []const u8,
    source_table: []const u8,
    authorizer: ?Authorizer,
) !void {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, json, .{});
    defer parsed.deinit();
    if (parsed.value != .array) return error.InvalidStoredDestinationConfig;
    for (parsed.value.array.items) |source|
        try authorizeReplicationSourceValue(alloc, source, source_table, authorizer);
}

pub fn authorizeReplicationSourceValue(
    alloc: std.mem.Allocator,
    source: std.json.Value,
    source_table: []const u8,
    authorizer: ?Authorizer,
) !void {
    if (source != .object) return error.InvalidStoredDestinationConfig;
    var destinations = std.ArrayListUnmanaged([]const u8).empty;
    defer destinations.deinit(alloc);
    try collectRouteDestinations(alloc, source, &destinations);
    const grant = try validateGrant(source.object, destinations.items);
    // The envelope authorizes writes into other Antfly tables. A source with
    // no routes has no such destination and therefore needs neither a user
    // grant nor a catalog-service exception. External source credentials are
    // governed by the secret store and connection policy instead.
    if (destinations.items.len == 0) return;
    if (authorizer) |live| {
        const payload = try grantSigningPayloadAlloc(
            alloc,
            source_table,
            grant.principal,
            destinations.items,
            source.object,
        );
        defer alloc.free(payload);
        try live.authorize(alloc, grant.principal, destinations.items, payload, grant.signature);
    }
}

pub fn validateIndexesJson(alloc: std.mem.Allocator, json: []const u8) !void {
    return try authorizeIndexesJson(alloc, json, "", null);
}

pub fn authorizeIndexesJson(
    alloc: std.mem.Allocator,
    json: []const u8,
    source_table: []const u8,
    authorizer: ?Authorizer,
) !void {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, json, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidStoredDestinationConfig;
    try validateResolverGrants(alloc, parsed.value, source_table, authorizer);
}

fn validateResolverGrants(
    alloc: std.mem.Allocator,
    value: std.json.Value,
    source_table: []const u8,
    authorizer: ?Authorizer,
) !void {
    switch (value) {
        .object => |object| {
            if (object.get("resolvers")) |resolvers| {
                if (resolvers != .array) return error.InvalidStoredDestinationConfig;
                for (resolvers.array.items) |resolver| {
                    if (resolver == .string) continue;
                    if (resolver != .object) return error.InvalidStoredDestinationConfig;
                    const table = resolver.object.get("table") orelse return error.InvalidStoredDestinationConfig;
                    if (table != .string or table.string.len == 0) return error.InvalidStoredDestinationConfig;
                    const destinations = &.{table.string};
                    const grant = try validateGrant(resolver.object, destinations);
                    if (authorizer) |live| {
                        const payload = try grantSigningPayloadAlloc(
                            alloc,
                            source_table,
                            grant.principal,
                            destinations,
                            resolver.object,
                        );
                        defer alloc.free(payload);
                        try live.authorize(alloc, grant.principal, destinations, payload, grant.signature);
                    }
                }
            }
            var it = object.iterator();
            while (it.next()) |entry| {
                if (std.mem.eql(u8, entry.key_ptr.*, "resolvers") or
                    std.mem.eql(u8, entry.key_ptr.*, grant_field)) continue;
                try validateResolverGrants(alloc, entry.value_ptr.*, source_table, authorizer);
            }
        },
        .array => |array| for (array.items) |item| try validateResolverGrants(alloc, item, source_table, authorizer),
        else => {},
    }
}

const ValidatedGrant = struct {
    principal: []const u8,
    signature: []const u8,
};

fn validateGrant(object: std.json.ObjectMap, destinations: []const []const u8) !ValidatedGrant {
    if (destinations.len == 0) return .{ .principal = catalog_service_principal, .signature = "" };
    const grant = object.get(grant_field) orelse return error.StoredDestinationAuthorizationMissing;
    if (grant != .object) return error.StoredDestinationAuthorizationInvalid;
    const principal_value = grant.object.get("principal") orelse return error.StoredDestinationAuthorizationInvalid;
    if (principal_value != .string or principal_value.string.len == 0)
        return error.StoredDestinationAuthorizationInvalid;
    const signature_value = grant.object.get("signature") orelse return error.StoredDestinationAuthorizationInvalid;
    if (signature_value != .string or signature_value.string.len == 0 or signature_value.string.len > 128)
        return error.StoredDestinationAuthorizationInvalid;
    const granted_destinations = grant.object.get("destinations") orelse return error.StoredDestinationAuthorizationInvalid;
    if (granted_destinations != .array or granted_destinations.array.items.len != destinations.len)
        return error.StoredDestinationAuthorizationInvalid;
    for (granted_destinations.array.items) |item| if (item != .string)
        return error.StoredDestinationAuthorizationInvalid;
    for (destinations) |destination| {
        var found = false;
        for (granted_destinations.array.items) |item| {
            if (item == .string and std.mem.eql(u8, item.string, destination)) {
                found = true;
                break;
            }
        }
        if (!found) return error.StoredDestinationAuthorizationInvalid;
    }
    return .{ .principal = principal_value.string, .signature = signature_value.string };
}

fn collectRouteDestinations(
    alloc: std.mem.Allocator,
    source: std.json.Value,
    out: *std.ArrayListUnmanaged([]const u8),
) !void {
    const routes = source.object.get("routes") orelse return;
    if (routes == .null) return;
    if (routes != .array) return error.InvalidStoredDestinationConfig;
    for (routes.array.items) |route| {
        if (route != .object) return error.InvalidStoredDestinationConfig;
        const target = route.object.get("target_table") orelse return error.InvalidStoredDestinationConfig;
        if (target != .string or target.string.len == 0) return error.InvalidStoredDestinationConfig;
        try appendUnique(alloc, out, target.string);
    }
}

fn appendValueSealingResolvers(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    value: std.json.Value,
    source_table: []const u8,
    principal: []const u8,
    authorizer: ?Authorizer,
) !void {
    switch (value) {
        .object => |object| {
            try out.append(alloc, '{');
            var first = true;
            var it = object.iterator();
            while (it.next()) |entry| {
                if (std.mem.eql(u8, entry.key_ptr.*, grant_field)) continue;
                if (!first) try out.append(alloc, ',');
                first = false;
                try appendJsonValue(alloc, out, .{ .string = entry.key_ptr.* });
                try out.append(alloc, ':');
                if (std.mem.eql(u8, entry.key_ptr.*, "resolvers")) {
                    const resolvers = entry.value_ptr.*;
                    if (resolvers != .array) return error.InvalidStoredDestinationConfig;
                    try out.append(alloc, '[');
                    for (resolvers.array.items, 0..) |resolver, index| {
                        if (index > 0) try out.append(alloc, ',');
                        if (resolver == .string) {
                            try appendJsonValue(alloc, out, resolver);
                            continue;
                        }
                        if (resolver != .object) return error.InvalidStoredDestinationConfig;
                        const table = resolver.object.get("table") orelse return error.InvalidStoredDestinationConfig;
                        if (table != .string or table.string.len == 0) return error.InvalidStoredDestinationConfig;
                        try appendSealedObject(
                            alloc,
                            out,
                            resolver.object,
                            source_table,
                            &.{table.string},
                            principal,
                            authorizer,
                        );
                    }
                    try out.append(alloc, ']');
                } else {
                    try appendValueSealingResolvers(alloc, out, entry.value_ptr.*, source_table, principal, authorizer);
                }
            }
            try out.append(alloc, '}');
        },
        .array => |array| {
            try out.append(alloc, '[');
            for (array.items, 0..) |item, index| {
                if (index > 0) try out.append(alloc, ',');
                try appendValueSealingResolvers(alloc, out, item, source_table, principal, authorizer);
            }
            try out.append(alloc, ']');
        },
        else => try appendJsonValue(alloc, out, value),
    }
}

fn appendUnique(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged([]const u8),
    value: []const u8,
) !void {
    for (out.items) |existing| if (std.mem.eql(u8, existing, value)) return;
    try out.append(alloc, value);
}

fn appendSealedObject(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    object: std.json.ObjectMap,
    source_table: []const u8,
    destinations: []const []const u8,
    principal: []const u8,
    authorizer: ?Authorizer,
) !void {
    try out.append(alloc, '{');
    var first = true;
    var it = object.iterator();
    while (it.next()) |entry| {
        if (std.mem.eql(u8, entry.key_ptr.*, grant_field)) continue;
        if (!first) try out.append(alloc, ',');
        first = false;
        try appendJsonValue(alloc, out, .{ .string = entry.key_ptr.* });
        try out.append(alloc, ':');
        try appendJsonValue(alloc, out, entry.value_ptr.*);
    }
    if (destinations.len > 0) {
        const live = authorizer orelse return error.StoredDestinationAuthorizationUnavailable;
        const payload = try grantSigningPayloadAlloc(alloc, source_table, principal, destinations, object);
        defer alloc.free(payload);
        const signature = try live.signatureAlloc(alloc, principal, payload);
        defer alloc.free(signature);
        if (!first) try out.append(alloc, ',');
        try appendJsonValue(alloc, out, .{ .string = grant_field });
        try out.appendSlice(alloc, ":{\"principal\":");
        try appendJsonValue(alloc, out, .{ .string = principal });
        try out.appendSlice(alloc, ",\"signature\":");
        try appendJsonValue(alloc, out, .{ .string = signature });
        try out.appendSlice(alloc, ",\"destinations\":[");
        for (destinations, 0..) |destination, index| {
            if (index > 0) try out.append(alloc, ',');
            try appendJsonValue(alloc, out, .{ .string = destination });
        }
        try out.appendSlice(alloc, "]}");
    }
    try out.append(alloc, '}');
}

fn grantSigningPayloadAlloc(
    alloc: std.mem.Allocator,
    source_table: []const u8,
    principal: []const u8,
    destinations: []const []const u8,
    object: std.json.ObjectMap,
) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try out.appendSlice(alloc, "{\"domain\":");
    try appendJsonValue(alloc, &out, .{ .string = grant_domain });
    try out.appendSlice(alloc, ",\"source_table\":");
    try appendJsonValue(alloc, &out, .{ .string = source_table });
    try out.appendSlice(alloc, ",\"principal\":");
    try appendJsonValue(alloc, &out, .{ .string = principal });
    try out.appendSlice(alloc, ",\"destinations\":[");
    for (destinations, 0..) |destination, index| {
        if (index > 0) try out.append(alloc, ',');
        try appendJsonValue(alloc, &out, .{ .string = destination });
    }
    try out.appendSlice(alloc, "],\"configuration\":{");
    var first = true;
    var it = object.iterator();
    while (it.next()) |entry| {
        if (std.mem.eql(u8, entry.key_ptr.*, grant_field)) continue;
        if (!first) try out.append(alloc, ',');
        first = false;
        try appendJsonValue(alloc, &out, .{ .string = entry.key_ptr.* });
        try out.append(alloc, ':');
        try appendJsonValue(alloc, &out, entry.value_ptr.*);
    }
    try out.appendSlice(alloc, "}}");
    return try out.toOwnedSlice(alloc);
}

fn appendJsonValue(alloc: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), value: std.json.Value) !void {
    const encoded = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(value, .{})});
    defer alloc.free(encoded);
    try out.appendSlice(alloc, encoded);
}

test "stored destination envelopes cannot be forged and validate on resume" {
    const alloc = std.testing.allocator;
    const disabled_authorizer: Authorizer = .{ .auth_enabled = false };
    var source_only = try std.json.parseFromSlice(
        std.json.Value,
        alloc,
        "{\"type\":\"postgres\",\"dsn\":\"postgres://source\"}",
        .{},
    );
    defer source_only.deinit();
    // Authentication state is irrelevant when the source has no Antfly write
    // destinations. In particular, the catalog service is not asked to mint
    // or validate an empty grant.
    try authorizeReplicationSourceValue(alloc, source_only.value, "source", .{ .auth_enabled = true });

    const raw =
        \\[{"type":"postgres","routes":[{"target_table":"protected"}],"_antfly_destination_authorization_v1":["decoy"]}]
    ;
    const sealed = try sealReplicationSourcesJsonForPrincipalAlloc(
        alloc,
        raw,
        "source",
        auth_disabled_principal,
        disabled_authorizer,
    );
    defer alloc.free(sealed);
    try std.testing.expect(std.mem.indexOf(u8, sealed, "protected") != null);
    try std.testing.expect(std.mem.indexOf(u8, sealed, auth_disabled_principal) != null);
    try std.testing.expect(std.mem.indexOf(u8, sealed, "decoy") == null);
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, sealed, .{});
    defer parsed.deinit();
    try authorizeReplicationSourceValue(alloc, parsed.value.array.items[0], "source", disabled_authorizer);
    try std.testing.expectError(error.StoredDestinationAuthorizationMissing, blk: {
        var unsealed = try std.json.parseFromSlice(std.json.Value, alloc, raw, .{});
        defer unsealed.deinit();
        _ = unsealed.value.array.items[0].object.swapRemove(grant_field);
        break :blk validateReplicationSourceValue(alloc, unsealed.value.array.items[0]);
    });

    const raw_indexes =
        \\{"graph":{"type":"graph"},"resolvers":[{"name":"entity","table":"protected","_antfly_destination_authorization_v1":["decoy"]}]}
    ;
    const sealed_indexes = try sealIndexesJsonForPrincipalAlloc(
        alloc,
        raw_indexes,
        "source",
        auth_disabled_principal,
        disabled_authorizer,
    );
    defer alloc.free(sealed_indexes);
    try validateIndexesJson(alloc, sealed_indexes);
    try std.testing.expect(std.mem.indexOf(u8, sealed_indexes, "protected") != null);
    try std.testing.expect(std.mem.indexOf(u8, sealed_indexes, "decoy") == null);
    try std.testing.expectError(error.StoredDestinationAuthorizationInvalid, validateIndexesJson(
        alloc,
        "{\"graph\":{\"type\":\"graph\"},\"resolvers\":[{\"table\":\"protected\",\"_antfly_destination_authorization_v1\":[\"decoy\"]}]}",
    ));

    try std.testing.expectError(
        error.StoredDestinationCredentialUnsupported,
        sealReplicationSourcesJsonForPrincipalAlloc(
            alloc,
            raw,
            "source",
            "trusted:issuer:user",
            .{ .auth_enabled = true },
        ),
    );
    const unauthenticated_sealed = try sealReplicationSourcesJsonForPrincipalAlloc(
        alloc,
        raw,
        "source",
        auth_disabled_principal,
        disabled_authorizer,
    );
    defer alloc.free(unauthenticated_sealed);
    try authorizeReplicationSourcesJson(alloc, unauthenticated_sealed, "source", disabled_authorizer);
    try std.testing.expectError(
        error.StoredDestinationAuthorizationRevoked,
        authorizeReplicationSourcesJson(alloc, unauthenticated_sealed, "source", .{ .auth_enabled = true }),
    );
    // With authentication disabled, every caller already has universal table
    // authority; the fixed marker is accepted only while that mode remains
    // active and is rejected immediately if authentication is enabled.
    try authorizeReplicationSourcesJson(alloc, unauthenticated_sealed, "different-source", disabled_authorizer);

    const forged =
        \\[{"type":"postgres","routes":[{"target_table":"protected"}],"_antfly_destination_authorization_v1":{"principal":"service:catalog","signature":"forged","destinations":["protected"]}}]
    ;
    try std.testing.expectError(
        error.StoredDestinationCredentialUnsupported,
        authorizeReplicationSourcesJson(alloc, forged, "source", .{ .auth_enabled = true }),
    );

    const fingerprint = try destinationConfigFingerprintAlloc(alloc, raw, raw_indexes);
    defer alloc.free(fingerprint);
    try std.testing.expect(destinationConfigFingerprintMatches(raw, raw_indexes, fingerprint));
    try std.testing.expect(!destinationConfigFingerprintMatches(raw, "{}", fingerprint));
}
