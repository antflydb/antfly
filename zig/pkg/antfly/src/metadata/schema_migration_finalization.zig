// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
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

//! Exact descriptor transformation after every owner has acknowledged the new
//! schema. Reconciliation and the metadata FK-lock admission check must use
//! the same transformation; neither may retire the old read layout early.
const std = @import("std");
const records = @import("antfly_local_sources").common_topology_records;
const tables = @import("../api/tables.zig");

/// Publication can be followed immediately by its deterministic read-layout
/// retirement. Acknowledgement must accept that exact logical successor, not
/// demand that asynchronous maintenance leave the transient image untouched.
/// Every other field still compares exactly; a later DDL, stale predecessor,
/// changed owner or arbitrary index mutation is not publication evidence.
pub fn matchesPublishedDefinition(alloc: std.mem.Allocator, projected: records.TableRecord, replacement: records.TableRecord) !bool {
    const metadata_tables = @import("table_manager.zig");
    if (metadata_tables.tableDefinitionsEqual(projected, replacement)) return true;
    if (replacement.read_schema_json.len == 0) return false;
    // Reject unrelated publications before allocating a candidate descriptor.
    if (projected.table_id != replacement.table_id or projected.read_schema_json.len != 0 or
        !std.mem.eql(u8, projected.name, replacement.name) or
        !std.mem.eql(u8, projected.schema_json, replacement.schema_json)) return false;
    var finalized = try metadata_tables.cloneTable(alloc, replacement);
    defer metadata_tables.freeTable(alloc, finalized);
    try apply(alloc, &finalized);
    return metadata_tables.tableDefinitionsEqual(projected, finalized);
}

test "schema publication acknowledgement accepts only exact deterministic finalization" {
    const a = std.testing.allocator;
    const before: records.TableRecord = .{
        .table_id = 7,
        .name = "rows",
        .schema_json = "{\"version\":2}",
        .read_schema_json = "{\"version\":1}",
        .indexes_json = "{\"full_text_index_v1\":{\"type\":\"full_text\"},\"full_text_index_v2\":{\"type\":\"full_text\"},\"named\":{\"type\":\"full_text\"}}",
    };
    const metadata_tables = @import("table_manager.zig");
    var finalized = try metadata_tables.cloneTable(a, before);
    defer metadata_tables.freeTable(a, finalized);
    try apply(a, &finalized);
    try std.testing.expect(try matchesPublishedDefinition(a, before, before));
    try std.testing.expect(try matchesPublishedDefinition(a, finalized, before));
    var changed = finalized;
    changed.table_id = 8;
    try std.testing.expect(!try matchesPublishedDefinition(a, changed, before));
    changed = finalized;
    changed.schema_json = "{\"version\":3}";
    try std.testing.expect(!try matchesPublishedDefinition(a, changed, before));
    changed.schema_json = "{\"version\":2,\"column_defaults\":[]}";
    try std.testing.expect(!try matchesPublishedDefinition(a, changed, before));
    changed = finalized;
    changed.indexes_json = "{\"full_text_index_v2\":{\"type\":\"full_text\"}}";
    try std.testing.expect(!try matchesPublishedDefinition(a, changed, before));
    changed = before;
    changed.schema_json = "{\"version\":1}";
    try std.testing.expect(!try matchesPublishedDefinition(a, changed, before));
    try std.testing.expect(!try matchesPublishedDefinition(a, before, finalized));
    const Faults = struct {
        fn run(backing: std.mem.Allocator, current: records.TableRecord, candidate: records.TableRecord) !void {
            try std.testing.expect(try matchesPublishedDefinition(backing, current, candidate));
        }
    };
    try std.testing.checkAllAllocationFailures(a, Faults.run, .{ finalized, before });
}

/// `table` owns its strings and remains owned by the caller after mutation.
pub fn apply(alloc: std.mem.Allocator, table: *records.TableRecord) !void {
    if (table.read_schema_json.len == 0) return error.SchemaMigrationNotActive;
    const target_version = try schemaVersion(alloc, table.schema_json);
    const read_version = try schemaVersion(alloc, table.read_schema_json);
    // This immutable definition stops admitting the retired read layout.
    // Derive before mutating any owned field; the durable cluster floor is
    // separate and remains monotonic over every historical layout.
    const next_requirements = if (table.schema_reader_requirements != null)
        try @import("antfly_local_sources").schema_mod.readerRequirementsForDefinitions(alloc, table.schema_json, "")
    else
        null;
    const empty_read_schema = try alloc.dupe(u8, "");
    errdefer alloc.free(empty_read_schema);
    const next_indexes = if (table.full_text_read_index) |name|
        if (@import("antfly_local_sources").api_full_text_indexes.managedGenerationVersion(name) != null)
            try dropFullTextIndexNamed(alloc, table.indexes_json, name)
        else
            null
    else if (read_version != target_version)
        try dropFullTextIndexForVersion(alloc, table.indexes_json, read_version)
    else
        null;
    if (next_indexes) |next_indexes_json| {
        alloc.free(table.indexes_json);
        table.indexes_json = next_indexes_json;
    }
    alloc.free(table.read_schema_json);
    table.read_schema_json = empty_read_schema;
    table.schema_reader_requirements = next_requirements;
    if (table.full_text_read_index) |name| alloc.free(name);
    table.full_text_read_index = null;
}

test "schema validation publication subsequent migration retires physical text epoch not logical predecessor" {
    const a = std.testing.allocator;
    const table: records.TableRecord = .{ .table_id = 7, .name = "rows", .schema_json = "{\"version\":4}", .read_schema_json = "{\"version\":3}", .full_text_read_index = "full_text_index_v1", .indexes_json = "{\"full_text_index_v1\":{\"type\":\"full_text\"},\"full_text_index_v4\":{\"type\":\"full_text\"},\"named\":{\"type\":\"full_text\"}}" };
    var updated = try @import("table_manager.zig").cloneTable(a, table);
    defer @import("table_manager.zig").freeTable(a, updated);
    try apply(a, &updated);
    try std.testing.expect(updated.full_text_read_index == null);
    try std.testing.expectEqual(@as(usize, 0), updated.read_schema_json.len);
    const parsed = try std.json.parseFromSlice(std.json.Value, a, updated.indexes_json, .{});
    defer parsed.deinit();
    try std.testing.expect(!parsed.value.object.contains("full_text_index_v1"));
    try std.testing.expect(parsed.value.object.contains("full_text_index_v4"));
    try std.testing.expect(parsed.value.object.contains("named"));
    try std.testing.expect(try matchesPublishedDefinition(a, updated, table));
}

test "schema validation publication later migration never retires a user named primary index" {
    const a = std.testing.allocator;
    const table: records.TableRecord = .{ .table_id = 7, .name = "rows", .schema_json = "{\"version\":4}", .read_schema_json = "{\"version\":3}", .full_text_read_index = "named", .indexes_json = "{\"named\":{\"type\":\"full_text\"},\"full_text_index_v4\":{\"type\":\"full_text\"}}" };
    var updated = try @import("table_manager.zig").cloneTable(a, table);
    defer @import("table_manager.zig").freeTable(a, updated);
    try apply(a, &updated);
    try std.testing.expect(updated.full_text_read_index == null);
    try std.testing.expectEqualStrings(table.indexes_json, updated.indexes_json);
    try std.testing.expect(try matchesPublishedDefinition(a, updated, table));
    const generated = @import("antfly_local_sources").api_full_text_indexes.managedGenerationVersion;
    for ([_][]const u8{ "named", "full_text_index_v01", "full_text_index_v+1", "full_text_index_v4294967296", "full_text_index_v" }) |name|
        try std.testing.expect(generated(name) == null);
    try std.testing.expectEqual(@as(u32, 0), generated("full_text_index_v0").?);
    try std.testing.expectEqual(@as(u32, 4), generated("full_text_index_v4").?);
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
    var name_buf: [64]u8 = undefined;
    const name = if (version == 0)
        tables.default_full_text_index_name
    else
        try std.fmt.bufPrint(&name_buf, "full_text_index_v{d}", .{version});
    return dropFullTextIndexNamed(alloc, indexes_json, name);
}

fn dropFullTextIndexNamed(alloc: std.mem.Allocator, indexes_json: []const u8, stale_name: []const u8) ![]u8 {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, indexes_json, .{});
    defer parsed.deinit();
    const object = switch (parsed.value) {
        .object => |*value| value,
        else => return error.InvalidTableIndexMetadata,
    };
    _ = object.swapRemove(stale_name);
    return std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(parsed.value, .{})});
}
