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
