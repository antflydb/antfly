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

//! Connection-owned graph catalog for C, Lite, and WASM databases. Definitions
//! pin native table and index incarnations exactly like hosted graph records.
const std = @import("std");
const cypher = @import("cypher");
const A = std.mem.Allocator;
const key = "\x00antfly/embedded/graphs/v1";
const format_key = "\x00antfly/embedded/graphs/format";
pub const Record = struct {
    name: []const u8,
    incarnation: u64,
    revision: u64,
    definition_json: []const u8,
    tables: []const u64,
    indexes: []const cypher.catalog.IndexIdentity,
};
pub const Catalog = struct { version: u8 = 1, next_id: u64 = 1, records: []const Record = &.{} };
// v1 is retained as the manifest key. Older binaries reject version 2 rather
// than treating a migrated catalog as empty. Records publish in the same txn.
const Manifest = struct { version: u8 = 2, next_id: u64 = 1, names: []const []const u8 = &.{} };
const Decoded = struct { manifest: Manifest, legacy: ?Catalog = null };
fn recordKey(a: A, name: []const u8) ![]const u8 {
    return std.fmt.allocPrint(a, "\x00antfly/embedded/graph-record/v2/{s}", .{name});
}
fn decode(a: A, bytes: ?[]const u8) !Decoded {
    const input = bytes orelse return .{ .manifest = .{} };
    const header = try std.json.parseFromSliceLeaky(struct { version: u8 = 1 }, a, input, .{ .ignore_unknown_fields = true });
    if (header.version == 1) {
        const legacy = try std.json.parseFromSliceLeaky(Catalog, a, input, .{ .allocate = .alloc_always });
        if (legacy.next_id == 0 or legacy.records.len > 1024) return error.InvalidGraphDefinition;
        const names = try a.alloc([]const u8, legacy.records.len);
        for (legacy.records, names) |record, *name| name.* = record.name;
        return .{ .manifest = .{ .next_id = legacy.next_id, .names = names }, .legacy = legacy };
    }
    if (header.version != 2) return error.InvalidGraphDefinition;
    const manifest = try std.json.parseFromSliceLeaky(Manifest, a, input, .{ .allocate = .alloc_always });
    if (manifest.next_id == 0 or manifest.names.len > 1024) return error.InvalidGraphDefinition;
    return .{ .manifest = manifest };
}
fn readFormat(txn: anytype) !?[]const u8 {
    const format = txn.get(format_key) catch |err| switch (err) {
        error.NotFound => return null,
        else => return err,
    };
    if (!std.mem.eql(u8, format, "2")) return error.InvalidGraphDefinition;
    return format;
}
fn validateFormat(format: ?[]const u8, bytes: ?[]const u8, decoded: Decoded) !void {
    if (format != null) {
        if (bytes == null or decoded.legacy != null) return error.InvalidGraphDefinition;
    } else if (bytes != null and decoded.legacy == null) return error.InvalidGraphDefinition;
}
fn readRecord(a: A, bytes: []const u8, name: []const u8) !Record {
    const record = try std.json.parseFromSliceLeaky(Record, a, bytes, .{ .allocate = .alloc_always });
    if (!std.mem.eql(u8, record.name, name) or record.incarnation == 0 or record.revision == 0) return error.InvalidGraphDefinition;
    return record;
}
pub fn load(a: A, context: anytype) !Catalog {
    var txn = try context.root.core.store.beginReadTxn();
    defer txn.abort();
    const format = try readFormat(&txn);
    const bytes = txn.get(key) catch |err| switch (err) {
        error.NotFound => null,
        else => return err,
    };
    const decoded = try decode(a, bytes);
    try validateFormat(format, bytes, decoded);
    if (bytes == null) return .{};
    if (decoded.legacy) |legacy| return legacy;
    const records = try a.alloc(Record, decoded.manifest.names.len);
    for (decoded.manifest.names, records) |name, *record| {
        const value = txn.get(try recordKey(a, name)) catch |err| switch (err) {
            error.NotFound => return error.InvalidGraphDefinition,
            else => return err,
        };
        record.* = try readRecord(a, value, name);
    }
    return .{ .version = 2, .next_id = decoded.manifest.next_id, .records = records };
}
pub fn get(a: A, context: anytype, name: []const u8) !Record {
    var txn = try context.root.core.store.beginReadTxn();
    defer txn.abort();
    if (try readFormat(&txn)) |_| {
        const bytes = txn.get(try recordKey(a, name)) catch |err| switch (err) {
            error.NotFound => return error.CatalogNotFound,
            else => return err,
        };
        return readRecord(a, bytes, name);
    }
    const bytes = txn.get(key) catch |err| switch (err) {
        error.NotFound => return error.CatalogNotFound,
        else => return err,
    };
    const decoded = try decode(a, bytes);
    try validateFormat(null, bytes, decoded);
    if (decoded.legacy) |legacy| for (legacy.records) |record| if (std.mem.eql(u8, name, record.name)) return record;
    return error.CatalogNotFound;
}
pub fn graph(a: A, record: Record) !cypher.catalog.Graph {
    const result = try cypher.catalog.parseDefinition(a, record.name, record.incarnation, record.revision, record.definition_json);
    if (result.labels.len != record.tables.len or result.relationships.len != record.indexes.len) return error.InvalidGraphDefinition;
    for (@constCast(result.labels), record.tables) |*label, id| label.table_id = id;
    for (@constCast(result.relationships), record.indexes) |*mapping, identity| mapping.identity = identity;
    return result;
}
pub fn validateOwners(a: A, context: anytype, selected: cypher.catalog.Graph) !void {
    for (selected.labels) |label| {
        if (!std.mem.eql(u8, label.database, "default") or !std.mem.eql(u8, label.namespace, "public")) return error.UndefinedGraphTable;
        const db = try context.get(a, label.table);
        if (label.table_id != db.core.identity_namespace.table_id) return error.CatalogGenerationChanged;
    }
    for (selected.relationships) |mapping| {
        const db = try context.get(a, selected.label(mapping.source_label).?.table);
        const current = (try db.cypherGraphIndexIdentity(a, mapping.index)) orelse return error.IndexNotFound;
        if (!std.meta.eql(current, mapping.identity.?)) return error.CatalogGenerationChanged;
    }
}
fn publicRecord(a: A, record: Record) !std.json.Value {
    return std.json.parseFromSliceLeaky(std.json.Value, a, try std.json.Stringify.valueAlloc(a, .{
        .name = record.name,
        .incarnation = try std.fmt.allocPrint(a, "{d}", .{record.incarnation}),
        .revision = try std.fmt.allocPrint(a, "{d}", .{record.revision}),
        .definition = try std.json.parseFromSliceLeaky(std.json.Value, a, record.definition_json, .{}),
    }, .{}), .{});
}
pub fn execute(context: anytype, result_allocator: A, json: []const u8) ![]u8 {
    if (json.len > 1 << 20) return error.CypherRequestTooLarge;
    var arena = std.heap.ArenaAllocator.init(context.alloc);
    defer arena.deinit();
    const a = arena.allocator();
    const Input = struct { action: enum { get, list, put, drop }, name: ?[]const u8 = null, definition: ?std.json.Value = null, expected_revision: ?[]const u8 = null };
    const input = try std.json.parseFromSliceLeaky(Input, a, json, .{ .allocate = .alloc_always });
    if (input.action == .list) {
        if (input.name != null or input.definition != null or input.expected_revision != null) return error.InvalidGraphDefinition;
        const catalog = try load(a, context);
        const records = try a.alloc(std.json.Value, catalog.records.len);
        for (catalog.records, records) |record, *value| value.* = try publicRecord(a, record);
        return cypher.bounded_json.encode(result_allocator, records, 32 << 20, null);
    }
    const name = input.name orelse return error.InvalidGraphDefinition;
    if (name.len == 0 or name.len > 256 or std.mem.indexOfScalar(u8, name, 0) != null) return error.InvalidGraphDefinition;
    if (input.action == .get) {
        if (input.definition != null or input.expected_revision != null) return error.InvalidGraphDefinition;
        return cypher.bounded_json.encode(result_allocator, try publicRecord(a, try get(a, context, name)), 32 << 20, null);
    }
    if (!context.writable) return error.ReadOnly;
    if (context.uncertain) return error.SqlMutationOutcomeUnknown;
    const expected = if (input.expected_revision) |value| blk: {
        if (value.len == 0 or value.len > 20) return error.InvalidGraphDefinition;
        for (value) |byte| if (!std.ascii.isDigit(byte)) return error.InvalidGraphDefinition;
        break :blk std.fmt.parseInt(u64, value, 10) catch return error.InvalidGraphDefinition;
    } else null;
    var read = try context.root.core.store.beginReadTxn();
    var read_held = true;
    defer if (read_held) read.abort();
    const predecessor_format = if (try readFormat(&read)) |format| try a.dupe(u8, format) else null;
    const predecessor = if (read.get(key)) |bytes| try a.dupe(u8, bytes) else |err| if (err == error.NotFound) null else return err;
    const decoded = try decode(a, predecessor);
    try validateFormat(predecessor_format, predecessor, decoded);
    var manifest = decoded.manifest;
    var names: std.ArrayList([]const u8) = .empty;
    try names.appendSlice(a, manifest.names);
    var index: ?usize = null;
    for (names.items, 0..) |entry, i| if (std.mem.eql(u8, entry, name)) {
        index = i;
        break;
    };
    const previous: ?Record = if (index) |i| if (decoded.legacy) |legacy| legacy.records[i] else try readRecord(a, try read.get(try recordKey(a, name)), name) else null;
    read.abort();
    read_held = false;
    if (previous) |record| {
        if (expected == null or expected.? != record.revision) return error.CatalogGenerationChanged;
    } else if (expected != null) return error.CatalogGenerationChanged;
    var replacement: ?Record = null;
    var response: std.json.Value = .null;
    if (input.action == .drop) {
        if (input.definition != null) return error.InvalidGraphDefinition;
        _ = names.orderedRemove(index orelse return error.CatalogNotFound);
    } else {
        const definition = input.definition orelse return error.InvalidGraphDefinition;
        const bytes = try std.json.Stringify.valueAlloc(a, definition, .{});
        const id = if (previous) |record| record.incarnation else manifest.next_id;
        const revision = if (previous) |record| try std.math.add(u64, record.revision, 1) else 1;
        const selected = try cypher.catalog.parseDefinition(a, name, id, revision, bytes);
        const table_ids = try a.alloc(u64, selected.labels.len);
        for (selected.labels, table_ids) |label, *table_id| {
            if (!std.mem.eql(u8, label.database, "default") or !std.mem.eql(u8, label.namespace, "public")) return error.UndefinedGraphTable;
            const db = try context.get(a, label.table);
            table_id.* = db.core.identity_namespace.table_id;
        }
        const identities = try a.alloc(cypher.catalog.IndexIdentity, selected.relationships.len);
        for (selected.relationships, identities) |mapping, *identity| {
            const db = try context.get(a, selected.label(mapping.source_label).?.table);
            identity.* = (try db.cypherGraphIndexIdentity(a, mapping.index)) orelse return error.IndexNotFound;
        }
        const record: Record = .{ .name = name, .incarnation = id, .revision = revision, .definition_json = bytes, .tables = table_ids, .indexes = identities };
        replacement = record;
        if (index == null) {
            if (names.items.len == 1024) return error.CypherStateBudgetExceeded;
            manifest.next_id = try std.math.add(u64, manifest.next_id, 1);
            try names.append(a, name);
        }
        response = try publicRecord(a, record);
    }
    manifest.names = names.items;
    const bytes = try cypher.bounded_json.encode(a, manifest, 32 << 20, null);
    // Encode the response before committing the durable catalog mutation.
    const result = try cypher.bounded_json.encode(result_allocator, response, 32 << 20, null);
    errdefer result_allocator.free(result);
    var txn = try context.root.core.store.beginWriteTxn();
    errdefer txn.abort();
    const current_format = try readFormat(&txn);
    if ((current_format == null) != (predecessor_format == null)) return error.CatalogGenerationChanged;
    const current = txn.get(key) catch |err| switch (err) {
        error.NotFound => null,
        else => return err,
    };
    if ((current == null) != (predecessor == null) or (current != null and !std.mem.eql(u8, current.?, predecessor.?))) return error.CatalogGenerationChanged;
    if (decoded.legacy) |legacy| for (legacy.records) |record| try txn.put(try recordKey(a, record.name), try cypher.bounded_json.encode(a, record, 32 << 20, null));
    const selected_key = try recordKey(a, name);
    if (replacement) |record| try txn.put(selected_key, try cypher.bounded_json.encode(a, record, 32 << 20, null)) else try txn.delete(selected_key);
    try txn.put(key, bytes);
    try txn.put(format_key, "2");
    try txn.commit();
    return result;
}

test "capi embedded graph catalog and query migrate indexed records without stale writes" {
    const Store = struct {
        const Self = @This();
        a: A,
        values: std.StringHashMapUnmanaged([]const u8) = .empty,
        reads: usize = 0,
        race: bool = false,
        const Txn = struct {
            store: *Self,
            pub fn get(self: *@This(), k: []const u8) anyerror![]const u8 {
                self.store.reads += 1;
                return self.store.values.get(k) orelse error.NotFound;
            }
            pub fn put(self: *@This(), k: []const u8, value: []const u8) !void {
                const entry = try self.store.values.getOrPut(self.store.a, k);
                if (!entry.found_existing) entry.key_ptr.* = try self.store.a.dupe(u8, k);
                entry.value_ptr.* = try self.store.a.dupe(u8, value);
            }
            pub fn delete(self: *@This(), k: []const u8) !void {
                if (!self.store.values.remove(k)) return error.NotFound;
            }
            pub fn commit(_: *@This()) !void {}
            pub fn abort(_: *@This()) void {}
        };
        pub fn beginReadTxn(self: *@This()) !Txn {
            return .{ .store = self };
        }
        pub fn beginWriteTxn(self: *@This()) !Txn {
            var txn: Txn = .{ .store = self };
            if (self.race) {
                self.race = false;
                try txn.put(key, try std.fmt.allocPrint(self.a, "{s} ", .{self.values.get(key).?}));
            }
            return txn;
        }
    };
    const DB = struct {
        core: struct { store: *Store, identity_namespace: struct { table_id: u64 = 7 } = .{} },
        pub fn cypherGraphIndexIdentity(_: *@This(), _: A, _: []const u8) !?cypher.catalog.IndexIdentity {
            return null;
        }
    };
    const Context = struct {
        root: *DB,
        alloc: A,
        writable: bool = true,
        uncertain: bool = false,
        pub fn get(self: @This(), _: A, name: []const u8) !*DB {
            if (!std.mem.eql(u8, name, "docs")) return error.UndefinedGraphTable;
            return self.root;
        }
    };
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var store: Store = .{ .a = a };
    var db: DB = .{ .core = .{ .store = &store } };
    const context: Context = .{ .root = &db, .alloc = a };
    const definition = "{\"labels\":[{\"name\":\"D\",\"table\":\"docs\"}],\"relationships\":[]}";
    const legacy: Catalog = .{ .next_id = 3, .records = &.{
        .{ .name = "g", .incarnation = 1, .revision = 1, .definition_json = definition, .tables = &.{7}, .indexes = &.{} },
        .{ .name = "other", .incarnation = 2, .revision = 1, .definition_json = definition, .tables = &.{7}, .indexes = &.{} },
    } };
    var initial = try store.beginWriteTxn();
    try initial.put(key, try std.json.Stringify.valueAlloc(a, legacy, .{}));
    try std.testing.expectEqual(@as(u64, 1), (try get(a, context, "g")).revision);
    _ = try execute(context, a, try std.fmt.allocPrint(a, "{{\"action\":\"put\",\"name\":\"g\",\"expected_revision\":\"1\",\"definition\":{s}}}", .{definition}));
    const migrated = try load(a, context);
    try std.testing.expectEqual(@as(u8, 2), migrated.version);
    try std.testing.expectEqual(@as(usize, 2), migrated.records.len);
    store.reads = 0;
    try std.testing.expectEqual(@as(u64, 2), (try get(a, context, "g")).revision);
    try std.testing.expectEqual(@as(usize, 2), store.reads);
    try std.testing.expectEqual(@as(u64, 2), (try get(a, context, "other")).incarnation);
    _ = try execute(context, a, "{\"action\":\"drop\",\"name\":\"g\",\"expected_revision\":\"2\"}");
    try std.testing.expectError(error.CatalogNotFound, get(a, context, "g"));
    _ = try execute(context, a, try std.fmt.allocPrint(a, "{{\"action\":\"put\",\"name\":\"g\",\"definition\":{s}}}", .{definition}));
    try std.testing.expectEqual(@as(u64, 3), (try get(a, context, "g")).incarnation);
    store.race = true;
    try std.testing.expectError(error.CatalogGenerationChanged, execute(context, a, try std.fmt.allocPrint(a, "{{\"action\":\"put\",\"name\":\"g\",\"expected_revision\":\"1\",\"definition\":{s}}}", .{definition})));
    try std.testing.expectEqual(@as(u64, 1), (try get(a, context, "g")).revision);
    var newer = try store.beginWriteTxn();
    try newer.put(format_key, "3");
    try std.testing.expectError(error.InvalidGraphDefinition, get(a, context, "g"));
    try std.testing.expectError(error.InvalidGraphDefinition, load(a, context));
    try std.testing.expectError(error.InvalidGraphDefinition, execute(context, a, "{\"action\":\"drop\",\"name\":\"g\",\"expected_revision\":\"1\"}"));
    try std.testing.expectEqualStrings("3", store.values.get(format_key).?);
}
