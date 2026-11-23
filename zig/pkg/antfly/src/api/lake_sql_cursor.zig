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

// Iceberg WAL timestamps carry epoch microseconds. SQL cursor predicates
// and stored cursor cells use signed epoch nanoseconds.
fn walKeyOperand(a: std.mem.Allocator, column: catalog.Column, raw: std.json.Value) !std.json.Value {
    if (column.type != .datetime) return raw;
    const micros: i64 = switch (raw) {
        .integer => |value| value,
        .number_string => |value| std.fmt.parseInt(i64, value, 10) catch return error.InvalidLakeKey,
        else => return error.InvalidLakeKey,
    };
    const nanos = @as(i128, micros) * std.time.ns_per_us;
    return if (std.math.cast(i64, nanos)) |value| .{ .integer = value } else .{ .number_string = try std.fmt.allocPrint(a, "{d}", .{nanos}) };
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
    keys_only: bool,
    context: operation.RequestContext,
    options: configured.BindingObjectStoreOpenOptions,
) !?std.json.Value {
    if (metadata == null or snapshot_id <= 0 or keys.len == 0 or key != .object) return error.InvalidLakeKey;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    const fields = if (keys_only) keys else all: {
        const names = try scratch.alloc([]const u8, table.columns.len);
        for (table.columns, names) |column, *field| field.* = column.name;
        break :all names;
    };
    const conditions = try scratch.alloc(catalog.Condition, keys.len);
    for (keys, conditions) |name, *condition| {
        const value = key.object.get(name) orelse return error.InvalidLakeKey;
        if (value == .null) return error.InvalidLakeKey;
        condition.* = .{ .column = name, .op = .eq, .value = try walKeyOperand(scratch, try table.column(name), value) };
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
    keys_only: bool,
    context: operation.RequestContext,
    options: configured.BindingObjectStoreOpenOptions,
) !?[]?std.json.Value {
    return (try lookupDynamicRows(a, table, snapshot_id, metadata, keys, requested, keys_only, context, options)) orelse
        try lookupTypedRows(a, table, snapshot_id, metadata, keys, requested, keys_only, context, options);
}

fn freeLookupRows(a: std.mem.Allocator, rows: []?std.json.Value) void {
    const deinit = @import("antfly_local_sources").api_json_helpers.deinitJsonValue;
    for (rows) |*row| {
        if (row.*) |*image| deinit(a, image);
    }
    a.free(rows);
}

fn lookupDynamicRows(
    a: std.mem.Allocator,
    table: catalog.Table,
    snapshot_id: i64,
    metadata: @FieldType(configured.BindingObjectStoreOpenOptions, "retained_catalog_metadata"),
    keys: []const []const u8,
    requested: []const LookupKey,
    keys_only: bool,
    context: operation.RequestContext,
    options: configured.BindingObjectStoreOpenOptions,
) !?[]?std.json.Value {
    const sources = @import("antfly_local_sources");
    const Datum = sources.sql_scalar.Datum;
    const dynamic = sources.sql_dynamic_filter;
    if (requested.len == 0) return try a.alloc(?std.json.Value, 0);
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

    const fields = if (keys_only) keys else columns: {
        const all = try scratch.alloc([]const u8, table.columns.len);
        for (table.columns, all) |column, *field| field.* = column.name;
        break :columns all;
    };
    const cursor = try openSnapshot(scratch, table, snapshot_id, metadata, .{ .fields = fields, .limit = 4 }, context, options);
    defer cursor.close(cursor.ptr);
    const apply = cursor.set_dynamic_filter orelse return null;
    if (!try apply(cursor.ptr, filter)) return null;
    const result = try a.alloc(?std.json.Value, requested.len);
    @memset(result, null);
    var complete = false;
    defer if (!complete) freeLookupRows(a, result);
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
            if (retained_bytes > @import("../metadata/changefeed_delivery.zig").max_page_bytes) return error.IcebergChangefeedImageBudgetExceeded;
            result[index] = try sources.api_json_helpers.cloneJsonValue(a, row.value);
        }
        if (page.after == null) break;
    }
    complete = true;
    return result;
}

// Resolve typed SQL equality in one pinned scan. Each key column contributes
// a conservative range, while tuple hashing and equality reject cross-key matches.
fn lookupTypedRows(
    a: std.mem.Allocator,
    table: catalog.Table,
    snapshot_id: i64,
    metadata: @FieldType(configured.BindingObjectStoreOpenOptions, "retained_catalog_metadata"),
    keys: []const []const u8,
    requested: []const LookupKey,
    keys_only: bool,
    context: operation.RequestContext,
    options: configured.BindingObjectStoreOpenOptions,
) ![]?std.json.Value {
    const sources = @import("antfly_local_sources");
    const scalar = sources.sql_scalar;
    const values = sources.sql_lake_values;
    const hashKey = sources.sql_operators.HashJoin.keyHash;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    const columns = try scratch.alloc(catalog.Column, keys.len);
    const ranges = try scratch.alloc(catalog.Condition, keys.len * 2);
    const entries = try scratch.alloc([]scalar.Datum, requested.len);
    const links = try scratch.alloc(?usize, requested.len);
    var heads: std.AutoHashMap(u64, usize) = .init(scratch);
    for (keys, columns) |key, *column| column.* = try table.column(key);
    for (requested, entries, links, 0..) |item, *entry, *link, index| {
        if (item.row != .object) return error.InvalidLakeKey;
        entry.* = try scratch.alloc(scalar.Datum, keys.len);
        for (columns, entry.*, 0..) |column, *datum, col| {
            const raw = item.row.object.get(column.name) orelse return error.InvalidLakeKey;
            if (raw == .null) return error.InvalidLakeKey;
            const value = try values.conditionValue(scratch, try walKeyOperand(scratch, column, raw), column);
            datum.* = try values.comparisonDatum(scratch, scalar.Datum.fromJson(value), column);
            const lower = &ranges[col * 2];
            const upper = &ranges[col * 2 + 1];
            if (index == 0 or try scalar.compareDatums(datum.*, lower.operand.?) == .lt)
                lower.* = .{ .column = column.name, .op = .gte, .value = value, .operand = datum.* };
            if (index == 0 or try scalar.compareDatums(datum.*, upper.operand.?) == .gt)
                upper.* = .{ .column = column.name, .op = .lte, .value = value, .operand = datum.* };
        }
        const hash = (try hashKey(entry.*)) orelse return error.InvalidLakeKey;
        const bucket = try heads.getOrPut(hash);
        link.* = if (bucket.found_existing) bucket.value_ptr.* else null;
        bucket.value_ptr.* = index;
    }
    const result = try a.alloc(?std.json.Value, requested.len);
    @memset(result, null);
    errdefer freeLookupRows(a, result);
    if (requested.len == 0) return result;
    const fields = if (keys_only) keys else all: {
        const names = try scratch.alloc([]const u8, table.columns.len);
        for (table.columns, names) |column, *name| name.* = column.name;
        break :all names;
    };
    const cursor = try openSnapshot(scratch, table, snapshot_id, metadata, .{ .fields = fields, .conditions = ranges, .limit = 4 }, context, options);
    defer cursor.close(cursor.ptr);
    var row_arena = std.heap.ArenaAllocator.init(a);
    defer row_arena.deinit();
    const tuple = try scratch.alloc(scalar.Datum, keys.len);
    const json = @import("../metadata/changefeed_json.zig");
    var retained_bytes: u64 = 0;
    while (true) {
        try context.ensureActive();
        const page = try cursor.next(cursor.ptr, a, 4);
        defer page.deinit();
        for (page.rows) |row| {
            _ = row_arena.reset(.retain_capacity);
            const row_alloc = row_arena.allocator();
            for (columns, tuple) |column, *datum| {
                const cell = try row.cell(column.name);
                datum.* = try values.storedComparisonDatum(row_alloc, .{ .value = cell.value, .sql_null = cell.sql_null }, column);
            }
            const hash = (try hashKey(tuple)) orelse continue;
            var candidate = heads.get(hash);
            while (candidate) |index| : (candidate = links[index]) {
                for (tuple, entries[index]) |left, right| {
                    if (try scalar.compareDatums(left, right) != .eq) break;
                } else {
                    if (result[index] != null) return error.InvalidLakeKey;
                    retained_bytes +|= try json.size(row.value);
                    if (retained_bytes > @import("../metadata/changefeed_delivery.zig").max_page_bytes)
                        return error.IcebergChangefeedImageBudgetExceeded;
                    result[index] = try sources.api_json_helpers.cloneJsonValue(a, row.value);
                }
            }
        }
        if (page.after == null) return result;
    }
}
