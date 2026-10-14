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
    after.schema_reader_requirements = .{ .semantic_version = 31, .encoding_version = 31 };
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
        .{ .semantic_version = 30, .encoding_version = 31 },
        .{ .semantic_version = 31, .encoding_version = 17 },
        .{ .semantic_version = 32, .encoding_version = 31 },
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
            after.schema_reader_requirements = .{ .semantic_version = 31, .encoding_version = 31 };
            try requireTableImages(a, before, after);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Check.run, .{});
}
