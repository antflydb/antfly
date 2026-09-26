// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//! Exact descriptor transformation after every owner has acknowledged the new
//! schema. Reconciliation and the metadata FK-lock admission check must use
//! the same transformation; neither may retire the old read layout early.
const std = @import("std");
const records = @import("../common/topology_records.zig");
const tables = @import("../api/tables.zig");

/// `table` owns its strings and remains owned by the caller after mutation.
pub fn apply(alloc: std.mem.Allocator, table: *records.TableRecord) !void {
    if (table.read_schema_json.len == 0) return error.SchemaMigrationNotActive;
    const target_version = try schemaVersion(alloc, table.schema_json);
    const read_version = try schemaVersion(alloc, table.read_schema_json);
    const empty_read_schema = try alloc.dupe(u8, "");
    errdefer alloc.free(empty_read_schema);
    if (read_version != target_version) {
        const next_indexes_json = try dropFullTextIndexForVersion(alloc, table.indexes_json, read_version);
        alloc.free(table.indexes_json);
        table.indexes_json = next_indexes_json;
    }
    alloc.free(table.read_schema_json);
    table.read_schema_json = empty_read_schema;
}

fn schemaVersion(alloc: std.mem.Allocator, schema_json: []const u8) !u32 {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, schema_json, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidTableSchema;
    const value = parsed.value.object.get("version") orelse return 0;
    if (value != .integer or value.integer < 0) return error.InvalidTableSchema;
    return std.math.cast(u32, value.integer) orelse error.InvalidTableSchema;
}

fn dropFullTextIndexForVersion(alloc: std.mem.Allocator, indexes_json: []const u8, version: u32) ![]u8 {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, indexes_json, .{});
    defer parsed.deinit();
    const object = switch (parsed.value) {
        .object => |*value| value,
        else => return error.InvalidTableIndexMetadata,
    };
    var name_buf: [64]u8 = undefined;
    const stale_name = if (version == 0)
        tables.default_full_text_index_name
    else
        try std.fmt.bufPrint(&name_buf, "full_text_index_v{d}", .{version});
    _ = object.swapRemove(stale_name);
    return std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(parsed.value, .{})});
}
