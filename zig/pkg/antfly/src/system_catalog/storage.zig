// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! Transactional catalog persistence. Records and name indexes are separate so
//! routing resolves a qualified name with point reads. Derived indexes are rebuilt
//! from authoritative records at projection initialization and snapshot install.
const std = @import("std");
const docstore = @import("../storage/docstore.zig");
const domain = @import("domain.zig");

pub const Meta = struct {
    version: u16 = 1,
    revision: u64 = 0,
    next_id: u64 = 3,
    last_command: [32]u8 = @splat(0),
};

pub const OwnedState = struct {
    arena: std.heap.ArenaAllocator,
    meta: Meta,
    value: domain.State,

    pub fn deinit(self: *@This()) void {
        self.arena.deinit();
        self.* = undefined;
    }
};

pub fn prefixForGroup(buf: []u8, group_id: u64) ![]const u8 {
    return std.fmt.bufPrint(buf, "\x00\x00__metadata__:system_catalog:{d}:", .{group_id});
}

fn keyAlloc(alloc: std.mem.Allocator, group_id: u64, suffix: []const u8) ![]u8 {
    return std.fmt.allocPrint(alloc, "\x00\x00__metadata__:system_catalog:{d}:{s}", .{ group_id, suffix });
}

fn recordKeyAlloc(alloc: std.mem.Allocator, group_id: u64, kind: domain.Kind, id: u64) ![]u8 {
    return std.fmt.allocPrint(alloc, "\x00\x00__metadata__:system_catalog:{d}:record:{s}:{d}", .{ group_id, @tagName(kind), id });
}

pub fn nameKeyAlloc(alloc: std.mem.Allocator, group_id: u64, kind: domain.Kind, parent: u64, name: []const u8) ![]u8 {
    try domain.validateResourceName(kind, name);
    return std.fmt.allocPrint(alloc, "\x00\x00__metadata_derived__:system_catalog_name:{d}:{s}:{d}:{s}", .{ group_id, @tagName(kind), parent, name });
}

pub fn readMeta(alloc: std.mem.Allocator, txn: *docstore.DocStore.Txn, group_id: u64) !Meta {
    const key = try keyAlloc(alloc, group_id, "meta");
    defer alloc.free(key);
    const bytes = txn.get(key) catch |err| switch (err) {
        error.NotFound => return .{},
        else => return err,
    };
    var parsed = try std.json.parseFromSlice(Meta, alloc, bytes, .{});
    defer parsed.deinit();
    if (parsed.value.version != 1 or parsed.value.next_id < 3) return error.InvalidCatalogRecord;
    return parsed.value;
}

pub fn loadState(alloc: std.mem.Allocator, txn: *docstore.DocStore.Txn, group_id: u64) !OwnedState {
    var arena = std.heap.ArenaAllocator.init(alloc);
    errdefer arena.deinit();
    const a = arena.allocator();
    const meta = try readMeta(a, txn, group_id);
    const prefix = try keyAlloc(a, group_id, "record:");
    const kvs = try docstore.DocStore.scanPrefixTxn(a, txn, prefix);
    const resources = try a.alloc(domain.Resource, kvs.len);
    for (kvs, resources) |kv, *resource| {
        resource.* = try std.json.parseFromSliceLeaky(domain.Resource, a, kv.value, .{ .allocate = .alloc_always });
        try domain.validateResourceName(resource.kind, resource.name);
        if (resource.id == 0) return error.InvalidCatalogRecord;
        const expected_key = try recordKeyAlloc(a, group_id, resource.kind, resource.id);
        if (!std.mem.eql(u8, expected_key, kv.key)) return error.InvalidCatalogRecord;
    }
    return .{ .arena = arena, .meta = meta, .value = .{ .revision = meta.revision, .next_id = meta.next_id, .resources = resources } };
}

pub fn getById(alloc: std.mem.Allocator, txn: *docstore.DocStore.Txn, group_id: u64, kind: domain.Kind, id: u64) !?std.json.Parsed(domain.Resource) {
    const key = try recordKeyAlloc(alloc, group_id, kind, id);
    defer alloc.free(key);
    const bytes = txn.get(key) catch |err| switch (err) {
        error.NotFound => return null,
        else => return err,
    };
    var parsed = try std.json.parseFromSlice(domain.Resource, alloc, bytes, .{ .allocate = .alloc_always });
    errdefer parsed.deinit();
    if (parsed.value.kind != kind or parsed.value.id != id) return error.InvalidCatalogRecord;
    return parsed;
}

pub fn find(alloc: std.mem.Allocator, txn: *docstore.DocStore.Txn, group_id: u64, kind: domain.Kind, parent: u64, name: []const u8) !?std.json.Parsed(domain.Resource) {
    const key = try nameKeyAlloc(alloc, group_id, kind, parent, name);
    defer alloc.free(key);
    const bytes = txn.get(key) catch |err| switch (err) {
        error.NotFound => return null,
        else => return err,
    };
    if (bytes.len != 8) return error.InvalidCatalogRecord;
    const id = std.mem.readInt(u64, bytes[0..8], .little);
    var resource = (try getById(alloc, txn, group_id, kind, id)) orelse return error.InvalidCatalogRecord;
    errdefer resource.deinit();
    if (resource.value.parent_id != parent or !std.mem.eql(u8, resource.value.name, name)) return error.InvalidCatalogRecord;
    return resource;
}

pub fn namePrefixAlloc(alloc: std.mem.Allocator, group_id: u64) ![]u8 {
    return std.fmt.allocPrint(alloc, "\x00\x00__metadata_derived__:system_catalog_name:{d}:", .{group_id});
}

/// Rebuild only derived rows. Duplicate authoritative names/IDs fail closed.
pub fn rebuildNameIndex(alloc: std.mem.Allocator, txn: *docstore.DocStore.Txn, group_id: u64) !void {
    var state = try loadState(alloc, txn, group_id);
    defer state.deinit();
    var index = try domain.StateIndex.init(alloc, state.value);
    defer index.deinit(alloc);
    const prefix = try namePrefixAlloc(alloc, group_id);
    defer alloc.free(prefix);
    const legacy_prefix = try keyAlloc(alloc, group_id, "name:");
    defer alloc.free(legacy_prefix);
    for ([_][]const u8{ prefix, legacy_prefix }) |p| {
        const rows = try docstore.DocStore.scanPrefixTxn(alloc, txn, p);
        defer {
            for (rows) |row| {
                alloc.free(row.key);
                alloc.free(row.value);
            }
            alloc.free(rows);
        }
        for (rows) |row| try txn.delete(row.key);
    }
    for (state.value.resources) |resource| {
        const key = try nameKeyAlloc(alloc, group_id, resource.kind, resource.parent_id, resource.name);
        defer alloc.free(key);
        var id: [8]u8 = undefined;
        std.mem.writeInt(u64, &id, resource.id, .little);
        try txn.put(key, &id);
    }
}

pub fn validateNameIndex(alloc: std.mem.Allocator, txn: *docstore.DocStore.Txn, group_id: u64) !void {
    var state = try loadState(alloc, txn, group_id);
    defer state.deinit();
    var index = try domain.StateIndex.init(alloc, state.value);
    defer index.deinit(alloc);
    for (state.value.resources) |resource| {
        var found = (try find(alloc, txn, group_id, resource.kind, resource.parent_id, resource.name)) orelse return error.InvalidCatalogRecord;
        defer found.deinit();
        if (found.value.id != resource.id) return error.InvalidCatalogRecord;
    }
}

pub fn writeResource(alloc: std.mem.Allocator, txn: *docstore.DocStore.Txn, group_id: u64, resource: domain.Resource) !void {
    if (try getById(alloc, txn, group_id, resource.kind, resource.id)) |old_value| {
        var old = old_value;
        defer old.deinit();
        const old_name_key = try nameKeyAlloc(alloc, group_id, old.value.kind, old.value.parent_id, old.value.name);
        defer alloc.free(old_name_key);
        txn.delete(old_name_key) catch |err| switch (err) {
            error.NotFound => {},
            else => return err,
        };
    }
    const record_key = try recordKeyAlloc(alloc, group_id, resource.kind, resource.id);
    defer alloc.free(record_key);
    const name_key = try nameKeyAlloc(alloc, group_id, resource.kind, resource.parent_id, resource.name);
    defer alloc.free(name_key);
    const json = try std.json.Stringify.valueAlloc(alloc, resource, .{});
    defer alloc.free(json);
    var encoded_id: [8]u8 = undefined;
    std.mem.writeInt(u64, &encoded_id, resource.id, .little);
    try txn.put(record_key, json);
    try txn.put(name_key, &encoded_id);
}

pub fn removeResource(alloc: std.mem.Allocator, txn: *docstore.DocStore.Txn, group_id: u64, resource: domain.Resource) !void {
    const record_key = try recordKeyAlloc(alloc, group_id, resource.kind, resource.id);
    defer alloc.free(record_key);
    const name_key = try nameKeyAlloc(alloc, group_id, resource.kind, resource.parent_id, resource.name);
    defer alloc.free(name_key);
    txn.delete(record_key) catch |err| switch (err) {
        error.NotFound => {},
        else => return err,
    };
    txn.delete(name_key) catch |err| switch (err) {
        error.NotFound => {},
        else => return err,
    };
}

pub fn applyDelta(alloc: std.mem.Allocator, txn: *docstore.DocStore.Txn, group_id: u64, delta: domain.Delta, previous: Meta, command_hash: [32]u8) !void {
    if (previous.revision == 0) {
        try writeResource(alloc, txn, group_id, domain.default_database);
        try writeResource(alloc, txn, group_id, domain.default_namespace);
    }
    for (delta.removes) |resource| try removeResource(alloc, txn, group_id, resource);
    for (delta.upserts) |resource| try writeResource(alloc, txn, group_id, resource);
    const meta: Meta = .{ .revision = try std.math.add(u64, previous.revision, 1), .next_id = delta.next_id, .last_command = command_hash };
    const key = try keyAlloc(alloc, group_id, "meta");
    defer alloc.free(key);
    const bytes = try std.json.Stringify.valueAlloc(alloc, meta, .{});
    defer alloc.free(bytes);
    try txn.put(key, bytes);
}
