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

/// Only the three schema/public-index fields can change. The predecessor read
/// layout remains pinned; publication cannot retire it or substitute the new
/// write layout before migration readiness. All other table settings are fixed.
pub fn requireTableImages(alloc: std.mem.Allocator, before: records.TableRecord, after: records.TableRecord) !void {
    var expected = before;
    expected.schema_json = after.schema_json;
    expected.read_schema_json = after.read_schema_json;
    expected.indexes_json = after.indexes_json;
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
