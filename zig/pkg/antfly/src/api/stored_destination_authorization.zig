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

pub const grant_field = "_antfly_destination_authorization_v1";

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

pub fn sealReplicationSourcesJsonAlloc(alloc: std.mem.Allocator, json: []const u8) ![]u8 {
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
        try appendSealedObject(alloc, &out, source.object, destinations.items);
    }
    try out.append(alloc, ']');
    return try out.toOwnedSlice(alloc);
}

pub fn sealIndexesJsonAlloc(alloc: std.mem.Allocator, json: []const u8) ![]u8 {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, json, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidStoredDestinationConfig;

    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    try appendValueSealingResolvers(alloc, &out, parsed.value);
    return try out.toOwnedSlice(alloc);
}

pub fn sealIndexJsonAlloc(alloc: std.mem.Allocator, json: []const u8) ![]u8 {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, json, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidStoredDestinationConfig;
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    try appendValueSealingResolvers(alloc, &out, parsed.value);
    return try out.toOwnedSlice(alloc);
}

pub fn validateReplicationSourceValue(alloc: std.mem.Allocator, source: std.json.Value) !void {
    if (source != .object) return error.InvalidStoredDestinationConfig;
    var destinations = std.ArrayListUnmanaged([]const u8).empty;
    defer destinations.deinit(alloc);
    try collectRouteDestinations(alloc, source, &destinations);
    try validateGrant(source.object, destinations.items);
}

pub fn validateIndexesJson(alloc: std.mem.Allocator, json: []const u8) !void {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, json, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidStoredDestinationConfig;
    try validateResolverGrants(alloc, parsed.value);
}

fn validateResolverGrants(alloc: std.mem.Allocator, value: std.json.Value) !void {
    switch (value) {
        .object => |object| {
            if (object.get("resolvers")) |resolvers| {
                if (resolvers != .array) return error.InvalidStoredDestinationConfig;
                for (resolvers.array.items) |resolver| {
                    if (resolver == .string) continue;
                    if (resolver != .object) return error.InvalidStoredDestinationConfig;
                    const table = resolver.object.get("table") orelse return error.InvalidStoredDestinationConfig;
                    if (table != .string or table.string.len == 0) return error.InvalidStoredDestinationConfig;
                    try validateGrant(resolver.object, &.{table.string});
                }
            }
            var it = object.iterator();
            while (it.next()) |entry| {
                if (std.mem.eql(u8, entry.key_ptr.*, "resolvers") or
                    std.mem.eql(u8, entry.key_ptr.*, grant_field)) continue;
                try validateResolverGrants(alloc, entry.value_ptr.*);
            }
        },
        .array => |array| for (array.items) |item| try validateResolverGrants(alloc, item),
        else => {},
    }
}

fn validateGrant(object: std.json.ObjectMap, destinations: []const []const u8) !void {
    if (destinations.len == 0) return;
    const grant = object.get(grant_field) orelse return error.StoredDestinationAuthorizationMissing;
    if (grant != .array) return error.StoredDestinationAuthorizationInvalid;
    if (grant.array.items.len != destinations.len) return error.StoredDestinationAuthorizationInvalid;
    for (grant.array.items) |item| if (item != .string)
        return error.StoredDestinationAuthorizationInvalid;
    for (destinations) |destination| {
        var found = false;
        for (grant.array.items) |item| {
            if (item == .string and std.mem.eql(u8, item.string, destination)) {
                found = true;
                break;
            }
        }
        if (!found) return error.StoredDestinationAuthorizationInvalid;
    }
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
                        try appendSealedObject(alloc, out, resolver.object, &.{table.string});
                    }
                    try out.append(alloc, ']');
                } else {
                    try appendValueSealingResolvers(alloc, out, entry.value_ptr.*);
                }
            }
            try out.append(alloc, '}');
        },
        .array => |array| {
            try out.append(alloc, '[');
            for (array.items, 0..) |item, index| {
                if (index > 0) try out.append(alloc, ',');
                try appendValueSealingResolvers(alloc, out, item);
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
    destinations: []const []const u8,
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
        if (!first) try out.append(alloc, ',');
        try appendJsonValue(alloc, out, .{ .string = grant_field });
        try out.appendSlice(alloc, ":[");
        for (destinations, 0..) |destination, index| {
            if (index > 0) try out.append(alloc, ',');
            try appendJsonValue(alloc, out, .{ .string = destination });
        }
        try out.append(alloc, ']');
    }
    try out.append(alloc, '}');
}

fn appendJsonValue(alloc: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), value: std.json.Value) !void {
    const encoded = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(value, .{})});
    defer alloc.free(encoded);
    try out.appendSlice(alloc, encoded);
}

test "stored destination envelopes cannot be forged and validate on resume" {
    const alloc = std.testing.allocator;
    const raw =
        \\[{"type":"postgres","routes":[{"target_table":"protected"}],"_antfly_destination_authorization_v1":["decoy"]}]
    ;
    const sealed = try sealReplicationSourcesJsonAlloc(alloc, raw);
    defer alloc.free(sealed);
    try std.testing.expect(std.mem.indexOf(u8, sealed, "protected") != null);
    try std.testing.expect(std.mem.indexOf(u8, sealed, "decoy") == null);
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, sealed, .{});
    defer parsed.deinit();
    try validateReplicationSourceValue(alloc, parsed.value.array.items[0]);
    try std.testing.expectError(error.StoredDestinationAuthorizationMissing, blk: {
        var unsealed = try std.json.parseFromSlice(std.json.Value, alloc, raw, .{});
        defer unsealed.deinit();
        _ = unsealed.value.array.items[0].object.swapRemove(grant_field);
        break :blk validateReplicationSourceValue(alloc, unsealed.value.array.items[0]);
    });

    const raw_indexes =
        \\{"graph":{"type":"graph"},"resolvers":[{"name":"entity","table":"protected","_antfly_destination_authorization_v1":["decoy"]}]}
    ;
    const sealed_indexes = try sealIndexesJsonAlloc(alloc, raw_indexes);
    defer alloc.free(sealed_indexes);
    try validateIndexesJson(alloc, sealed_indexes);
    try std.testing.expect(std.mem.indexOf(u8, sealed_indexes, "protected") != null);
    try std.testing.expect(std.mem.indexOf(u8, sealed_indexes, "decoy") == null);
    try std.testing.expectError(error.StoredDestinationAuthorizationInvalid, validateIndexesJson(
        alloc,
        "{\"graph\":{\"type\":\"graph\"},\"resolvers\":[{\"table\":\"protected\",\"_antfly_destination_authorization_v1\":[\"decoy\"]}]}",
    ));

    const fingerprint = try destinationConfigFingerprintAlloc(alloc, raw, raw_indexes);
    defer alloc.free(fingerprint);
    try std.testing.expect(destinationConfigFingerprintMatches(raw, raw_indexes, fingerprint));
    try std.testing.expect(!destinationConfigFingerprintMatches(raw, "{}", fingerprint));
}
