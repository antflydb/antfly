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

/// All returned records borrow this view's arena. Its transaction pins one
/// catalog revision for both planning and response projection.
pub const View = struct {
    alloc: std.mem.Allocator,
    txn: *docstore.DocStore.Txn,
    group_id: u64,
    meta: Meta,

    pub fn byId(self: View, kind: domain.Kind, id: u64) !?domain.Resource {
        if (try getById(self.alloc, self.txn, self.group_id, kind, id)) |record| return record.value;
        return (domain.State{}).byId(kind, id);
    }
    pub fn lookup(self: View, kind: domain.Kind, parent: u64, name: []const u8) !?domain.Resource {
        if (try find(self.alloc, self.txn, self.group_id, kind, parent, name)) |record| return record.value;
        return (domain.State{}).find(kind, parent, name);
    }
    pub fn children(self: View, kind: domain.Kind, parent: u64, limit: usize) ![]const domain.Resource {
        return self.childrenPage(kind, parent, limit, "", null);
    }
    pub fn childrenPage(self: View, kind: domain.Kind, parent: u64, limit: usize, name_prefix: []const u8, after: ?[]const u8) ![]const domain.Resource {
        const base = try std.fmt.allocPrint(self.alloc, "\x00\x00__metadata_derived__:system_catalog_name:{d}:children:{s}:{d}:", .{ self.group_id, @tagName(kind), parent });
        const prefix = try std.mem.concat(self.alloc, u8, &.{ base, name_prefix });
        const seek = if (after) |name| try std.mem.concat(self.alloc, u8, &.{ base, name }) else prefix;
        var cursor = try self.txn.openCursor();
        defer cursor.close();
        var out: std.ArrayListUnmanaged(domain.Resource) = .empty;
        var entry = try cursor.seekAtOrAfter(if (std.mem.lessThan(u8, seek, prefix)) prefix else seek);
        while (entry) |kv| : (entry = try cursor.next()) {
            if (!std.mem.startsWith(u8, kv.key, prefix)) break;
            const record = try std.json.parseFromSliceLeaky(domain.Resource, self.alloc, kv.value, .{ .allocate = .alloc_always });
            if (record.kind != kind or record.parent_id != parent or record.id == 0 or !std.mem.eql(u8, kv.key, try childKey(self.alloc, self.group_id, record))) return error.InvalidCatalogRecord;
            try domain.validateResourceName(record.kind, record.name);
            if (after) |name| if (!std.mem.lessThan(u8, name, record.name)) continue;
            try out.append(self.alloc, record);
            if (limit != 0 and out.items.len >= limit) break;
        }
        if (out.items.len == 0 and self.meta.revision == 0) {
            if (kind == .database and parent == 0) try out.append(self.alloc, domain.default_database);
            if (kind == .namespace and parent == domain.default_database_id) try out.append(self.alloc, domain.default_namespace);
        }
        return out.items;
    }
    pub fn bindingForStorage(self: View, name: []const u8) !?domain.Resource {
        const key = try storageBindingKey(self.alloc, self.group_id, name);
        const bytes = self.txn.get(key) catch |err| switch (err) {
            error.NotFound => return null,
            else => return err,
        };
        if (bytes.len != 8) return error.InvalidCatalogRecord;
        const record = (try self.byId(.table, std.mem.readInt(u64, bytes[0..8], .little))) orelse return error.InvalidCatalogRecord;
        if (!std.mem.eql(u8, record.storage_name, name)) return error.InvalidCatalogRecord;
        return record;
    }
    pub fn tablespaceInUse(self: View, id: u64) !bool {
        const prefix = try std.fmt.allocPrint(self.alloc, "\x00\x00__metadata_derived__:system_catalog_name:{d}:uses:{d}:", .{ self.group_id, id });
        var cursor = try self.txn.openCursor();
        defer cursor.close();
        const kv = (try cursor.seekAtOrAfter(prefix)) orelse return false;
        if (!std.mem.startsWith(u8, kv.key, prefix)) return false;
        const suffix = kv.key[prefix.len..];
        const colon = std.mem.indexOfScalar(u8, suffix, ':') orelse return error.InvalidCatalogRecord;
        const kind = std.meta.stringToEnum(domain.Kind, suffix[0..colon]) orelse return error.InvalidCatalogRecord;
        const resource_id = std.fmt.parseInt(u64, suffix[colon + 1 ..], 10) catch return error.InvalidCatalogRecord;
        const record = (try self.byId(kind, resource_id)) orelse return error.InvalidCatalogRecord;
        if (record.tablespace_id != id) return error.InvalidCatalogRecord;
        return true;
    }
    pub fn namespaceFor(self: View, database: []const u8, namespace: []const u8) !domain.Resource {
        const db = (try self.lookup(.database, 0, database)) orelse return error.DatabaseNotFound;
        return (try self.lookup(.namespace, db.id, namespace)) orelse error.NamespaceNotFound;
    }
    pub fn effectiveTablespace(self: View, namespace: domain.Resource, explicit: u64) !?domain.Resource {
        const db = (try self.byId(.database, namespace.parent_id)) orelse return error.DatabaseNotFound;
        const id = if (explicit != 0) explicit else if (namespace.tablespace_id != 0) namespace.tablespace_id else db.tablespace_id;
        return if (id == 0) null else (try self.byId(.tablespace, id)) orelse return error.TablespaceNotFound;
    }
    pub fn read(self: View, request: domain.Read) !domain.State {
        if (request.kind == .table) return error.InvalidCatalogMutation;
        const parent = if (request.kind == .namespace) ((try self.lookup(.database, 0, request.database)) orelse return error.DatabaseNotFound).id else 0;
        var out: std.ArrayListUnmanaged(domain.Resource) = .empty;
        if (request.name) |name| {
            try out.append(self.alloc, (try self.lookup(request.kind, parent, name)) orelse return error.CatalogNotFound);
        } else try out.appendSlice(self.alloc, try self.children(request.kind, parent, 0));
        var related: std.AutoHashMapUnmanaged(u64, void) = .empty;
        const count = out.items.len;
        for (0..count) |i| {
            const id = out.items[i].tablespace_id;
            if (id == 0 or related.contains(id)) continue;
            try related.put(self.alloc, id, {});
            try out.append(self.alloc, (try self.byId(.tablespace, id)) orelse return error.InvalidCatalogRecord);
        }
        if (request.kind == .namespace) try out.append(self.alloc, (try self.byId(.database, parent)) orelse return error.InvalidCatalogRecord);
        return .{ .revision = self.meta.revision, .next_id = self.meta.next_id, .resources = out.items };
    }
};

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
        try writeReferences(alloc, txn, group_id, resource);
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
        const child_key = try childKey(alloc, group_id, resource);
        defer alloc.free(child_key);
        const child_json = try std.json.Stringify.valueAlloc(alloc, resource, .{});
        defer alloc.free(child_json);
        if (!std.mem.eql(u8, txn.get(child_key) catch return error.InvalidCatalogRecord, child_json)) return error.InvalidCatalogRecord;
        if (resource.kind == .table) {
            const key = try storageBindingKey(alloc, group_id, resource.storage_name);
            defer alloc.free(key);
            const bytes = txn.get(key) catch return error.InvalidCatalogRecord;
            if (bytes.len != 8 or std.mem.readInt(u64, bytes[0..8], .little) != resource.id) return error.InvalidCatalogRecord;
        }
        if (resource.tablespace_id != 0) {
            const key = try tablespaceUseKey(alloc, group_id, resource);
            defer alloc.free(key);
            _ = txn.get(key) catch return error.InvalidCatalogRecord;
        }
    }
}

fn storageBindingKey(alloc: std.mem.Allocator, group_id: u64, name: []const u8) ![]u8 {
    return std.fmt.allocPrint(alloc, "\x00\x00__metadata_derived__:system_catalog_name:{d}:storage:{s}", .{ group_id, name });
}
fn tablespaceUseKey(alloc: std.mem.Allocator, group_id: u64, r: domain.Resource) ![]u8 {
    return std.fmt.allocPrint(alloc, "\x00\x00__metadata_derived__:system_catalog_name:{d}:uses:{d}:{s}:{d}", .{ group_id, r.tablespace_id, @tagName(r.kind), r.id });
}
// Covering parent rows keep list reads sequential. These values are derived,
// written atomically with the primary record, and rebuilt on index migration.
fn childKey(alloc: std.mem.Allocator, group_id: u64, r: domain.Resource) ![]u8 {
    return std.fmt.allocPrint(alloc, "\x00\x00__metadata_derived__:system_catalog_name:{d}:children:{s}:{d}:{s}", .{ group_id, @tagName(r.kind), r.parent_id, r.name });
}
fn writeReferences(alloc: std.mem.Allocator, txn: *docstore.DocStore.Txn, group_id: u64, r: domain.Resource) !void {
    const child_key = try childKey(alloc, group_id, r);
    defer alloc.free(child_key);
    const child_json = try std.json.Stringify.valueAlloc(alloc, r, .{});
    defer alloc.free(child_json);
    try txn.put(child_key, child_json);
    if (r.kind == .table) {
        const key = try storageBindingKey(alloc, group_id, r.storage_name);
        defer alloc.free(key);
        var id: [8]u8 = undefined;
        std.mem.writeInt(u64, &id, r.id, .little);
        const previous = txn.get(key) catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        };
        if (previous) |value| if (!std.mem.eql(u8, value, &id)) return error.InvalidCatalogRecord;
        try txn.put(key, &id);
    }
    if (r.tablespace_id != 0) {
        const key = try tablespaceUseKey(alloc, group_id, r);
        defer alloc.free(key);
        try txn.put(key, "");
    }
}
fn removeReferences(alloc: std.mem.Allocator, txn: *docstore.DocStore.Txn, group_id: u64, r: domain.Resource) !void {
    const child_key = try childKey(alloc, group_id, r);
    defer alloc.free(child_key);
    txn.delete(child_key) catch |err| switch (err) {
        error.NotFound => {},
        else => return err,
    };
    if (r.kind == .table) {
        const key = try storageBindingKey(alloc, group_id, r.storage_name);
        defer alloc.free(key);
        txn.delete(key) catch |err| switch (err) {
            error.NotFound => {},
            else => return err,
        };
    }
    if (r.tablespace_id != 0) {
        const key = try tablespaceUseKey(alloc, group_id, r);
        defer alloc.free(key);
        txn.delete(key) catch |err| switch (err) {
            error.NotFound => {},
            else => return err,
        };
    }
}

pub fn writeResource(alloc: std.mem.Allocator, txn: *docstore.DocStore.Txn, group_id: u64, resource: domain.Resource) !void {
    if (try getById(alloc, txn, group_id, resource.kind, resource.id)) |old_value| {
        var old = old_value;
        defer old.deinit();
        try removeReferences(alloc, txn, group_id, old.value);
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
    try writeReferences(alloc, txn, group_id, resource);
}

pub fn removeResource(alloc: std.mem.Allocator, txn: *docstore.DocStore.Txn, group_id: u64, resource: domain.Resource) !void {
    try removeReferences(alloc, txn, group_id, resource);
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
