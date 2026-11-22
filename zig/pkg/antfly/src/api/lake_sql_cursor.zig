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

//! Server credential policy adapter for the shared embedded SQL cursor.
const std = @import("std");
const local = @import("antfly_local_sources").sql_lake_cursor;
const catalog = @import("antfly_local_sources").sql_catalog;
const operation = @import("antfly_local_sources").api_operation;
const configured = @import("../serverless/configured_object_store_support.zig");
pub const openPinned = local.openPinned;
pub fn open(alloc: std.mem.Allocator, table: catalog.Table, request: catalog.Scan, context: operation.RequestContext, options: configured.BindingObjectStoreOpenOptions) !catalog.Cursor {
    return local.open(alloc, table, request, context, options.lakeOptions());
}
pub fn openWithCache(alloc: std.mem.Allocator, table: catalog.Table, request: catalog.Scan, context: operation.RequestContext, options: configured.BindingObjectStoreOpenOptions, cache: ?*@import("antfly_local_sources").serverless_query_lake_serving_cache.Cache, io: ?std.Io) !catalog.Cursor {
    return local.openWithCache(alloc, table, request, context, options.lakeOptions(), cache, io);
}

/// Opens the exact catalog snapshot through a read-only binding.
pub fn openSnapshot(a: std.mem.Allocator, table: catalog.Table, snapshot_id: i64, metadata: @FieldType(configured.BindingObjectStoreOpenOptions, "retained_catalog_metadata"), scan: catalog.Scan, context: operation.RequestContext, options: configured.BindingObjectStoreOpenOptions) !catalog.Cursor {
    if (metadata == null or snapshot_id <= 0 or table.external_base_source == null) return error.InvalidExternalTableBinding;
    var pinned = table;
    const snapshot = try std.fmt.allocPrint(a, "{d}", .{snapshot_id});
    pinned.external_base_source.?.binding.snapshot_mode = .{ .snapshot_id = snapshot };
    pinned.external_base_source.?.binding.write_policy = .read_only;
    var settings = options;
    settings.retained_catalog_metadata = metadata;
    settings.read_only = true;
    return openWithCache(a, pinned, scan, context, settings, null, settings.io);
}

/// Read a unique key from a particular committed snapshot using Iceberg's data
/// and delete plans. The caller supplies catalog-owned metadata and a table
/// schema valid at the requested snapshot.
pub fn lookupSnapshotRow(
    a: std.mem.Allocator,
    table: catalog.Table,
    snapshot_id: i64,
    metadata: @FieldType(configured.BindingObjectStoreOpenOptions, "retained_catalog_metadata"),
    keys: []const []const u8,
    key: std.json.Value,
    context: operation.RequestContext,
    options: configured.BindingObjectStoreOpenOptions,
) !?std.json.Value {
    if (metadata == null or snapshot_id <= 0 or keys.len == 0 or key != .object) return error.InvalidLakeKey;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    const fields = try scratch.alloc([]const u8, table.columns.len);
    for (table.columns, fields) |column, *field| field.* = column.name;
    const conditions = try scratch.alloc(catalog.Condition, keys.len);
    for (keys, conditions) |name, *condition| {
        const value = key.object.get(name) orelse return error.InvalidLakeKey;
        if (value == .null) return error.InvalidLakeKey;
        condition.* = .{ .column = name, .op = .eq, .value = value };
    }
    const cursor = try openSnapshot(a, table, snapshot_id, metadata, .{ .fields = fields, .conditions = conditions, .limit = 2 }, context, options);
    defer cursor.close(cursor.ptr);
    const clone = @import("antfly_local_sources").api_json_helpers.cloneJsonValue;
    var image: ?std.json.Value = null;
    while (true) {
        const page = try cursor.next(cursor.ptr, a, 2);
        defer page.deinit();
        for (page.rows) |row| {
            if (image != null) return error.InvalidLakeKey;
            image = try clone(scratch, row.value);
        }
        if (page.after == null) break;
    }
    return if (image) |value| try clone(a, value) else null;
}

pub const LookupKey = struct { identity: []const u8, row: std.json.Value };

/// Open one scan for a window of scalar CDC keys. The shared dynamic filter
/// prunes matching key domains before materializing the complete row images.
pub fn lookupSnapshotRows(
    a: std.mem.Allocator,
    table: catalog.Table,
    snapshot_id: i64,
    metadata: @FieldType(configured.BindingObjectStoreOpenOptions, "retained_catalog_metadata"),
    keys: []const []const u8,
    requested: []const LookupKey,
    context: operation.RequestContext,
    options: configured.BindingObjectStoreOpenOptions,
) !?[]?std.json.Value {
    const sources = @import("antfly_local_sources");
    const Datum = sources.sql_scalar.Datum;
    const dynamic = sources.sql_dynamic_filter;
    const result = try a.alloc(?std.json.Value, requested.len);
    @memset(result, null);
    if (requested.len == 0) return result;

    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    const columns = try scratch.alloc(dynamic.Column, keys.len);
    for (keys, columns) |name, *column| {
        const physical = try table.column(name);
        switch (physical.type) {
            .integer, .string, .boolean => {},
            else => return null, // Use exact typed point comparisons for other key types.
        }
        column.* = .{ .name = name, .type = physical.type, .element_type = physical.element_type };
    }

    const filter = try dynamic.Filter.create(scratch, columns, 4096);
    defer filter.close();
    const cells = try scratch.alloc(Datum, keys.len);
    var indexes: std.StringHashMap(usize) = .init(scratch);
    const keyIdentity = @import("../serverless/lake_ingestion.zig").rowId;
    for (requested, 0..) |item, index| {
        if (item.row != .object) return error.InvalidLakeKey;
        for (columns, cells) |column, *cell| {
            const value = item.row.object.get(column.name) orelse return error.InvalidLakeKey;
            if (value == .null) return error.InvalidLakeKey;
            cell.* = try column.datum(scratch, Datum.fromJson(value));
        }
        try filter.add(cells);
        try indexes.put(item.identity, index);
    }
    if (!filter.exact_complete) return null;
    filter.sealed = true;

    const fields = try scratch.alloc([]const u8, table.columns.len);
    for (table.columns, fields) |column, *field| field.* = column.name;
    const cursor = try openSnapshot(scratch, table, snapshot_id, metadata, .{ .fields = fields, .limit = 4 }, context, options);
    defer cursor.close(cursor.ptr);
    const apply = cursor.set_dynamic_filter orelse return null;
    if (!try apply(cursor.ptr, filter)) return null;
    const json = @import("../metadata/changefeed_json.zig");
    var retained_bytes: u64 = 0;
    while (true) {
        try context.ensureActive();
        const page = try cursor.next(cursor.ptr, a, 4);
        defer page.deinit();
        for (page.rows) |row| {
            const identity = try keyIdentity(scratch, keys, row.value);
            const index = indexes.get(identity) orelse return null;
            if (result[index] != null) return error.InvalidLakeKey;
            retained_bytes +|= try json.size(row.value);
            if (retained_bytes > @import("../metadata/changefeed_delivery.zig").max_page_bytes) return null;
            result[index] = try sources.api_json_helpers.cloneJsonValue(a, row.value);
        }
        if (page.after == null) break;
    }
    return result;
}
