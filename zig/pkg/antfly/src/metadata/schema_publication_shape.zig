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

//! Independent shape checks for immutable, schema-only publication plans.
const std = @import("std");
const records = @import("antfly_local_sources").common_topology_records;
const tables = @import("table_manager.zig");
const Value = std.json.Value;

/// Object order is irrelevant; array order and numerical representation are
/// not silently rewritten. Depth is bounded independently of parser settings.
pub const equal = @import("antfly_local_sources").schema_json_shape.equal;

fn equalExcept(a: Value, b: Value, allowed: []const []const u8) bool {
    if (a != .object or b != .object) return false;
    for ([_]bool{ false, true }) |reverse| {
        const left = if (reverse) b else a;
        const right = if (reverse) a else b;
        var it = left.object.iterator();
        while (it.next()) |entry| {
            const skip = for (allowed) |key| {
                if (std.mem.eql(u8, key, entry.key_ptr.*)) break true;
            } else false;
            if (skip) continue;
            const other = right.object.get(entry.key_ptr.*) orelse return false;
            if (!equal(entry.value_ptr.*, other, 0)) return false;
        }
    }
    return true;
}

pub fn requireSchemaDelta(alloc: std.mem.Allocator, before: []const u8, after: []const u8, allowed: []const []const u8) !void {
    var left = try std.json.parseFromSlice(Value, alloc, before, .{});
    defer left.deinit();
    var right = try std.json.parseFromSlice(Value, alloc, after, .{});
    defer right.deinit();
    if (!equalExcept(left.value, right.value, allowed)) return error.InvalidGenerationPublication;
}

/// FK publication may also introduce one nullable column whose historical
/// value is NULL. Everything else remains exact: no defaults, generated
/// expressions, changed old columns, required fields or unrelated constraints.
/// The same owner cohort fences the column and its FK as one schema change.
pub fn requireForeignKeySchemaDelta(alloc: std.mem.Allocator, before: []const u8, after: []const u8) !void {
    var left = try std.json.parseFromSlice(Value, alloc, before, .{});
    defer left.deinit();
    var right = try std.json.parseFromSlice(Value, alloc, after, .{});
    defer right.deinit();
    const old = left.value;
    const new = right.value;
    if (equalExcept(old, new, &.{ "version", "foreign_keys" })) return;
    if (!equalExcept(old, new, &.{ "version", "foreign_keys", "document_schemas" })) return error.InvalidGenerationPublication;
    const name = old.object.get("default_type") orelse return error.InvalidGenerationPublication;
    if (name != .string) return error.InvalidGenerationPublication;
    const old_docs = old.object.get("document_schemas") orelse return error.InvalidGenerationPublication;
    const new_docs = new.object.get("document_schemas") orelse return error.InvalidGenerationPublication;
    if (!equalExcept(old_docs, new_docs, &.{name.string})) return error.InvalidGenerationPublication;
    const old_doc = old_docs.object.get(name.string) orelse return error.InvalidGenerationPublication;
    const new_doc = new_docs.object.get(name.string) orelse return error.InvalidGenerationPublication;
    if (!equalExcept(old_doc, new_doc, &.{"schema"})) return error.InvalidGenerationPublication;
    const old_row = old_doc.object.get("schema") orelse return error.InvalidGenerationPublication;
    const new_row = new_doc.object.get("schema") orelse return error.InvalidGenerationPublication;
    if (!equalExcept(old_row, new_row, &.{"properties"})) return error.InvalidGenerationPublication;
    const old_props = old_row.object.get("properties") orelse return error.InvalidGenerationPublication;
    const new_props = new_row.object.get("properties") orelse return error.InvalidGenerationPublication;
    if (old_props != .object or new_props != .object or new_props.object.count() != old_props.object.count() + 1)
        return error.InvalidGenerationPublication;
    var prior = old_props.object.iterator();
    while (prior.next()) |entry| {
        const current = new_props.object.get(entry.key_ptr.*) orelse return error.InvalidGenerationPublication;
        if (!equal(entry.value_ptr.*, current, 0)) return error.InvalidGenerationPublication;
    }
    var added = new_props.object.iterator();
    const column = while (added.next()) |entry| {
        if (old_props.object.contains(entry.key_ptr.*)) continue;
        if (entry.value_ptr.* != .object) return error.InvalidGenerationPublication;
        const nullable = entry.value_ptr.object.get("nullable") orelse return error.InvalidGenerationPublication;
        if (nullable != .bool or !nullable.bool or std.mem.eql(u8, entry.key_ptr.*, "_id")) return error.InvalidGenerationPublication;
        // Defaults and generated expressions belong in the top-level schema;
        // accepting property-local variants would undermine the NULL proof.
        for ([_][]const u8{ "default", "generated", "const" }) |key|
            if (entry.value_ptr.object.contains(key)) return error.InvalidGenerationPublication;
        break entry.key_ptr.*;
    } else return error.InvalidGenerationPublication;
    const old_foreign: []const Value = if (old.object.get("foreign_keys")) |rules| blk: {
        if (rules != .array) return error.InvalidGenerationPublication;
        break :blk rules.array.items;
    } else &.{};
    const new_foreign = new.object.get("foreign_keys") orelse return error.InvalidGenerationPublication;
    if (new_foreign != .array or new_foreign.array.items.len != old_foreign.len + 1) return error.InvalidGenerationPublication;
    for (old_foreign, new_foreign.array.items[0..old_foreign.len]) |a, b|
        if (!equal(a, b, 0)) return error.InvalidGenerationPublication;
    const rule = new_foreign.array.items[old_foreign.len];
    if (rule != .object) return error.InvalidGenerationPublication;
    const keys = rule.object.get("child_columns") orelse return error.InvalidGenerationPublication;
    if (keys != .array or keys.array.items.len != 1 or keys.array.items[0] != .string or
        !std.mem.eql(u8, keys.array.items[0].string, column)) return error.InvalidGenerationPublication;
}

const fk_column_before =
    \\{"version":1,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword","nullable":false}},"required":["id"],"additionalProperties":false}}}}
;
const fk_column_after =
    \\{"version":2,"storage_mode":"relational","default_type":"row","foreign_keys":[{"name":"rows_parent_fkey","child_columns":["parent_id"],"parent_table":"parents","parent_columns":["id"]}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword","nullable":false},"parent_id":{"type":"keyword","nullable":true}},"required":["id"],"additionalProperties":false}}}}
;

test "system catalog FK column publication rejects unrelated edits and non-NULL historical values" {
    const a = std.testing.allocator;
    try requireForeignKeySchemaDelta(a, fk_column_before, fk_column_after);
    const Probe = struct { from: []const u8, to: []const u8 };
    for ([_]Probe{
        .{ .from = "\"nullable\":true", .to = "\"nullable\":false" },
        .{ .from = "\"nullable\":true", .to = "\"nullable\":true,\"default\":\"missing\"" },
        .{ .from = "\"child_columns\":[\"parent_id\"]", .to = "\"child_columns\":[\"id\"]" },
        .{ .from = "\"required\":[\"id\"]", .to = "\"required\":[\"id\",\"parent_id\"]" },
        .{ .from = "\"id\":{\"type\":\"keyword\"", .to = "\"id\":{\"type\":\"integer\"" },
        .{ .from = "\"foreign_keys\"", .to = "\"column_defaults\":[{\"column\":\"parent_id\",\"expression\":{\"op\":\"literal\",\"value\":\"missing\"}}],\"foreign_keys\"" },
        .{ .from = "\"foreign_keys\"", .to = "\"checks\":[],\"foreign_keys\"" },
        .{ .from = "\"parent_id\":{", .to = "\"other\":{\"type\":\"keyword\",\"nullable\":true},\"parent_id\":{" },
    }) |probe| {
        const changed = try std.mem.replaceOwned(u8, a, fk_column_after, probe.from, probe.to);
        defer a.free(changed);
        try std.testing.expect(!std.mem.eql(u8, changed, fk_column_after));
        try std.testing.expectError(error.InvalidGenerationPublication, requireForeignKeySchemaDelta(a, fk_column_before, changed));
    }
    const Check = struct {
        fn run(alloc: std.mem.Allocator) !void {
            try requireForeignKeySchemaDelta(alloc, fk_column_before, fk_column_after);
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(a, Check.run, .{});
}

fn version(schema: Value) !i64 {
    if (schema != .object) return error.InvalidGenerationPublication;
    const v = schema.object.get("version") orelse return 0;
    if (v != .integer or v.integer < 0 or v.integer > std.math.maxInt(u32)) return error.InvalidGenerationPublication;
    return v.integer;
}

fn requireDerivedReaders(alloc: std.mem.Allocator, table: records.TableRecord, comptime prospective: bool) !void {
    const claimed = table.schema_reader_requirements orelse return;
    if (!claimed.valid()) return error.InvalidGenerationPublication;
    const derived = @import("antfly_local_sources").schema_mod.readerRequirementsForDefinitions(alloc, table.schema_json, table.read_schema_json) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => return error.InvalidGenerationPublication,
    };
    // JSON proves logical requirements, not an existing physical frame's
    // encoding. The predecessor's encoding stays bound by its immutable table
    // guard and owner receipts. Only the compiler's prospective frame must
    // match its emitted encoding exactly.
    if (claimed.semantic_version != derived.semantic_version or
        (prospective and claimed.encoding_version != derived.encoding_version)) return error.InvalidGenerationPublication;
}

/// Schema/public-index fields and their server-derived reader requirements can
/// change together. Introduction of the marker still requires durable decoder
/// activation at admission; this shape check is not publication authority.
/// A marked identity cannot silently become unmarked. The predecessor read
/// layout remains pinned; publication cannot retire it or substitute the new
/// write layout before migration readiness. All other table settings are fixed.
pub fn requireTableImages(alloc: std.mem.Allocator, before: records.TableRecord, after: records.TableRecord) !void {
    var expected = before;
    expected.schema_json = after.schema_json;
    expected.read_schema_json = after.read_schema_json;
    expected.indexes_json = after.indexes_json;
    if (before.schema_reader_requirements != null and after.schema_reader_requirements == null)
        return error.InvalidGenerationPublication;
    try requireDerivedReaders(alloc, before, false);
    try requireDerivedReaders(alloc, after, true);
    expected.schema_reader_requirements = after.schema_reader_requirements;
    if (!tables.tableDefinitionsEqual(expected, after)) return error.InvalidGenerationPublication;
    const read_before = if (before.read_schema_json.len == 0) before.schema_json else before.read_schema_json;
    if (after.read_schema_json.len == 0) return error.InvalidGenerationPublication;
    var left = try std.json.parseFromSlice(Value, alloc, read_before, .{});
    defer left.deinit();
    var right = try std.json.parseFromSlice(Value, alloc, after.read_schema_json, .{});
    defer right.deinit();
    if (try version(left.value) != try version(right.value) or !equalExcept(left.value, right.value, &.{"version"}))
        return error.InvalidGenerationPublication;
}

const reader_test_before =
    \\{"version":1,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"}},"additionalProperties":false}}}}
;
const reader_test_after =
    \\{"version":2,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"},"values":{"type":"sql_array","x-antfly-sql-type":"jsonb","nullable":true}},"additionalProperties":false}}}}
;

test "system catalog schema reader table publication shapes bind exact active and retained requirements" {
    const a = std.testing.allocator;
    var before: records.TableRecord = .{ .table_id = 7, .name = "rows", .schema_json = reader_test_before };
    var after = before;
    after.schema_json = reader_test_after;
    after.read_schema_json = reader_test_before;
    after.schema_reader_requirements = .{ .semantic_version = 31, .encoding_version = @import("antfly_local_sources").common_schema_readers.current_encoding_version };
    // Shape can introduce the server-derived marker; actual admission must
    // separately persist decoder activation before retaining this candidate.
    try requireTableImages(a, before, after);
    before.schema_reader_requirements = .{ .semantic_version = 14, .encoding_version = 31 };
    try requireTableImages(a, before, after);
    var stored_frame = before;
    stored_frame.schema_reader_requirements.?.encoding_version = 17;
    try requireTableImages(a, stored_frame, after);
    for ([_]?@import("antfly_local_sources").common_schema_readers.ReaderRequirements{
        null,
        .{ .semantic_version = 30, .encoding_version = @import("antfly_local_sources").common_schema_readers.current_encoding_version },
        .{ .semantic_version = 31, .encoding_version = 17 },
        .{ .semantic_version = 31, .encoding_version = @import("antfly_local_sources").common_schema_readers.current_encoding_version - 1 },
        .{ .semantic_version = 32, .encoding_version = @import("antfly_local_sources").common_schema_readers.current_encoding_version },
    }) |invalid| {
        var bad = after;
        bad.schema_reader_requirements = invalid;
        try std.testing.expectError(error.InvalidGenerationPublication, requireTableImages(a, before, bad));
    }
    var understated_before = before;
    understated_before.schema_reader_requirements.?.semantic_version = 13;
    try std.testing.expectError(error.InvalidGenerationPublication, requireTableImages(a, understated_before, after));
    var unrelated = after;
    unrelated.description = "unrelated change";
    try std.testing.expectError(error.InvalidGenerationPublication, requireTableImages(a, before, unrelated));
    var inflated = before;
    inflated.read_schema_json = reader_test_before;
    inflated.schema_reader_requirements = .{ .semantic_version = 31, .encoding_version = 31 };
    try std.testing.expectError(error.InvalidGenerationPublication, requireTableImages(a, before, inflated));
}

test "system catalog schema reader table publication shapes release compiled requirements on allocation failure" {
    const Check = struct {
        fn run(a: std.mem.Allocator) !void {
            const before: records.TableRecord = .{ .table_id = 7, .name = "rows", .schema_json = reader_test_before, .schema_reader_requirements = .{ .semantic_version = 14, .encoding_version = 31 } };
            var after = before;
            after.schema_json = reader_test_after;
            after.read_schema_json = reader_test_before;
            after.schema_reader_requirements = .{ .semantic_version = 31, .encoding_version = @import("antfly_local_sources").common_schema_readers.current_encoding_version };
            try requireTableImages(a, before, after);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Check.run, .{});
}
