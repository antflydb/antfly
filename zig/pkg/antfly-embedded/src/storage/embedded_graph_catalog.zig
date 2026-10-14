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
pub const Record = struct {
    name: []const u8,
    incarnation: u64,
    revision: u64,
    definition_json: []const u8,
    tables: []const u64,
    indexes: []const cypher.catalog.IndexIdentity,
};
pub const Catalog = struct { version: u8 = 1, next_id: u64 = 1, records: []const Record = &.{} };
pub fn load(a: A, context: anytype) !Catalog {
    var txn = try context.root.core.store.beginReadTxn();
    defer txn.abort();
    const bytes = txn.get(key) catch |err| switch (err) {
        error.NotFound => return .{},
        else => return err,
    };
    const result = try std.json.parseFromSliceLeaky(Catalog, a, bytes, .{ .allocate = .alloc_always });
    if (result.version != 1 or result.next_id == 0 or result.records.len > 1024) return error.InvalidGraphDefinition;
    return result;
}
pub fn get(a: A, context: anytype, name: []const u8) !Record {
    for ((try load(a, context)).records) |record| if (std.mem.eql(u8, name, record.name)) return record;
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
    var catalog = try load(a, context);
    if (input.action == .list) {
        if (input.name != null or input.definition != null or input.expected_revision != null) return error.InvalidGraphDefinition;
        const records = try a.alloc(std.json.Value, catalog.records.len);
        for (catalog.records, records) |record, *value| value.* = try publicRecord(a, record);
        return cypher.bounded_json.encode(result_allocator, records, 32 << 20, null);
    }
    const name = input.name orelse return error.InvalidGraphDefinition;
    if (name.len == 0 or name.len > 256 or std.mem.indexOfScalar(u8, name, 0) != null) return error.InvalidGraphDefinition;
    var index: ?usize = null;
    for (catalog.records, 0..) |record, i| if (std.mem.eql(u8, record.name, name)) {
        index = i;
        break;
    };
    if (input.action == .get) {
        if (input.definition != null or input.expected_revision != null) return error.InvalidGraphDefinition;
        return cypher.bounded_json.encode(result_allocator, try publicRecord(a, catalog.records[index orelse return error.CatalogNotFound]), 32 << 20, null);
    }
    if (!context.writable) return error.ReadOnly;
    if (context.uncertain) return error.SqlMutationOutcomeUnknown;
    const expected = if (input.expected_revision) |value| blk: {
        if (value.len == 0 or value.len > 20) return error.InvalidGraphDefinition;
        for (value) |byte| if (!std.ascii.isDigit(byte)) return error.InvalidGraphDefinition;
        break :blk std.fmt.parseInt(u64, value, 10) catch return error.InvalidGraphDefinition;
    } else null;
    if (index) |i| {
        if (expected == null or expected.? != catalog.records[i].revision) return error.CatalogGenerationChanged;
    } else if (expected != null) return error.CatalogGenerationChanged;
    var records: std.ArrayList(Record) = .empty;
    try records.appendSlice(a, catalog.records);
    var response: std.json.Value = .null;
    if (input.action == .drop) {
        if (input.definition != null) return error.InvalidGraphDefinition;
        _ = records.orderedRemove(index orelse return error.CatalogNotFound);
    } else {
        const definition = input.definition orelse return error.InvalidGraphDefinition;
        const bytes = try std.json.Stringify.valueAlloc(a, definition, .{});
        const id = if (index) |i| catalog.records[i].incarnation else catalog.next_id;
        const revision = if (index) |i| try std.math.add(u64, catalog.records[i].revision, 1) else 1;
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
        if (index) |i| records.items[i] = record else {
            if (records.items.len == 1024) return error.CypherStateBudgetExceeded;
            catalog.next_id = try std.math.add(u64, catalog.next_id, 1);
            try records.append(a, record);
        }
        response = try publicRecord(a, record);
    }
    catalog.records = records.items;
    const bytes = try cypher.bounded_json.encode(a, catalog, 32 << 20, null);
    // Encode the response before committing the durable catalog mutation.
    const result = try cypher.bounded_json.encode(result_allocator, response, 32 << 20, null);
    errdefer result_allocator.free(result);
    var txn = try context.root.core.store.beginWriteTxn();
    errdefer txn.abort();
    try txn.put(key, bytes);
    try txn.commit();
    return result;
}
