// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Secondary ownership keys for ordinary namespace catalog rows, including
//! empty TEMP namespaces. The primary resource and these keys share one writer.
const std = @import("std");
const local = @import("antfly_local_sources");
const domain = local.system_catalog_domain;
const Owner = local.common_topology_records.RelationPersistence.Owner;
const A = std.mem.Allocator;
pub const max_page = 128;
pub const Selection = union(enum) { connection: Owner, issuer: u64 };
pub const Reference = struct {
    database_id: u64,
    namespace_id: u64,
    owner: Owner,
    pub fn validate(self: Reference) !void {
        try self.owner.validate();
        if (self.database_id == 0 or self.namespace_id == 0) return error.InvalidSqlTemporaryNamespace;
    }
};
pub const Page = struct { entries: []const Reference, next: ?u64 = null };
pub const Target = struct { reference: Reference, database: []const u8 };
pub const NamedPage = struct { entries: []const Target, next: ?u64 = null };

fn selectedPrefix(buffer: []u8, group: u64, selection: Selection) ![]const u8 {
    if (group == 0) return error.InvalidSqlTemporaryNamespace;
    return switch (selection) {
        .connection => |owner| blk: {
            var name: [72]u8 = undefined;
            break :blk std.fmt.bufPrint(buffer, "\x00\x00__metadata__:system_catalog:{d}:temporary_namespace:connection:{s}:", .{ group, try owner.namespace(&name) });
        },
        .issuer => |node| std.fmt.bufPrint(buffer, "\x00\x00__metadata__:system_catalog:{d}:temporary_namespace:issuer:{x:0>16}:", .{ group, node }),
    };
}
fn indexKey(buffer: []u8, group: u64, selection: Selection, id: u64) ![]const u8 {
    var prefix: [256]u8 = undefined;
    return std.fmt.bufPrint(buffer, "{s}{x:0>16}", .{ try selectedPrefix(&prefix, group, selection), id });
}
pub fn reference(resource: domain.Resource) !?Reference {
    const owner = resource.sql_temporary_owner orelse return null;
    var name: [72]u8 = undefined;
    if (resource.kind != .namespace or resource.id == domain.default_namespace_id or resource.native_service != null or resource.sql_owner != null or
        !std.mem.eql(u8, resource.name, try owner.namespace(&name))) return error.InvalidSqlTemporaryNamespace;
    const value: Reference = .{ .database_id = resource.parent_id, .namespace_id = resource.id, .owner = owner };
    try value.validate();
    return value;
}

pub fn stage(a: A, txn: anytype, group: u64, resource: domain.Resource, add: bool) !void {
    const value = (try reference(resource)) orelse return;
    const bytes = try std.json.Stringify.valueAlloc(a, value, .{});
    defer a.free(bytes);
    for ([_]Selection{ .{ .connection = value.owner }, .{ .issuer = value.owner.issuer_node_id } }) |selection| {
        var buffer: [288]u8 = undefined;
        const key = try indexKey(&buffer, group, selection, value.namespace_id);
        const before = txn.get(key) catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        };
        if (before) |stored| if (!std.mem.eql(u8, stored, bytes)) return error.InvalidSqlTemporaryNamespace;
        if (add) {
            try txn.put(key, bytes);
        } else {
            if (before == null) return error.InvalidSqlTemporaryNamespace;
            try txn.delete(key);
        }
    }
}

/// Keyset pagination is bounded by entries, not by the number of catalog
/// tables. Every result rejoins its authoritative namespace in the same view.
pub fn list(a: A, txn: anytype, group: u64, selection: Selection, after: ?u64, limit: usize, view: anytype) !Page {
    if (limit == 0 or limit > max_page) return error.InvalidSqlTemporaryNamespace;
    var prefix_buffer: [256]u8 = undefined;
    const prefix = try selectedPrefix(&prefix_buffer, group, selection);
    var seek_buffer: [288]u8 = undefined;
    const seek = if (after) |id| try indexKey(&seek_buffer, group, selection, id) else prefix;
    var cursor = try txn.openCursor();
    defer cursor.close();
    var entries: std.ArrayList(Reference) = .empty;
    errdefer entries.deinit(a);
    var row = try cursor.seekAtOrAfter(seek);
    while (row) |item| : (row = try cursor.next()) {
        if (!std.mem.startsWith(u8, item.key, prefix)) break;
        if (item.key.len != prefix.len + 16 or item.value.len > 2048) return error.InvalidSqlTemporaryNamespace;
        const id = std.fmt.parseInt(u64, item.key[prefix.len..], 16) catch return error.InvalidSqlTemporaryNamespace;
        if (after != null and id <= after.?) continue;
        if (entries.items.len == limit) {
            const next = entries.items[entries.items.len - 1].namespace_id;
            return .{ .entries = try entries.toOwnedSlice(a), .next = next };
        }
        var parsed = try std.json.parseFromSlice(Reference, a, item.value, .{});
        defer parsed.deinit();
        const value = parsed.value;
        try value.validate();
        if (value.namespace_id != id) return error.InvalidSqlTemporaryNamespace;
        switch (selection) {
            .connection => |owner| if (!owner.eql(value.owner)) return error.InvalidSqlTemporaryNamespace,
            .issuer => |node| if (node != value.owner.issuer_node_id) return error.InvalidSqlTemporaryNamespace,
        }
        try view.requireNamespace(value);
        try entries.append(a, value);
    }
    return .{ .entries = try entries.toOwnedSlice(a) };
}

/// Validate the forward keys without rebuilding or silently repairing them.
pub fn requireResource(a: A, reader: anytype, group: u64, resource: domain.Resource) !void {
    const value = (try reference(resource)) orelse return;
    const bytes = try std.json.Stringify.valueAlloc(a, value, .{});
    defer a.free(bytes);
    for ([_]Selection{ .{ .connection = value.owner }, .{ .issuer = value.owner.issuer_node_id } }) |selection| {
        var buffer: [288]u8 = undefined;
        const actual = reader.get(try indexKey(&buffer, group, selection, value.namespace_id)) catch |err| return if (err == error.NotFound) error.InvalidSqlTemporaryNamespace else err;
        if (!std.mem.eql(u8, actual, bytes)) return error.InvalidSqlTemporaryNamespace;
    }
}

/// Streaming reverse validation detects orphan or forged secondary keys on
/// restore. Primary lookups own short-lived parsed records; no full page lives
/// across iterations and an unknown index kind is corruption, not a hint.
pub fn validate(a: A, reader: anytype, group: u64, view: anytype) !void {
    var buffer: [256]u8 = undefined;
    const prefix = try std.fmt.bufPrint(&buffer, "\x00\x00__metadata__:system_catalog:{d}:temporary_namespace:", .{group});
    var cursor = try reader.openCursor();
    defer cursor.close();
    var row = try cursor.seekAtOrAfter(prefix);
    while (row) |item| : (row = try cursor.next()) {
        if (!std.mem.startsWith(u8, item.key, prefix)) break;
        if (item.key.len > 288 or item.value.len > 2048) return error.InvalidSqlTemporaryNamespace;
        var saved: [288]u8 = undefined;
        @memcpy(saved[0..item.key.len], item.key);
        const stored_key = saved[0..item.key.len];
        var parsed = try std.json.parseFromSlice(Reference, a, item.value, .{});
        defer parsed.deinit();
        const value = parsed.value;
        try value.validate();
        var expected: [288]u8 = undefined;
        const selection: Selection = if (std.mem.startsWith(u8, stored_key[prefix.len..], "connection:")) .{ .connection = value.owner } else if (std.mem.startsWith(u8, stored_key[prefix.len..], "issuer:")) .{ .issuer = value.owner.issuer_node_id } else return error.InvalidSqlTemporaryNamespace;
        if (!std.mem.eql(u8, stored_key, try indexKey(&expected, group, selection, value.namespace_id))) return error.InvalidSqlTemporaryNamespace;
        try view.requireNamespace(value);
    }
}
