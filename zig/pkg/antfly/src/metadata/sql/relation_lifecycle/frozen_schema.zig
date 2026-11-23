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

//! A frozen multiset retains the logical column/layout vocabulary while its
//! physical indexes admit repeated historical primary/unique keys. The live
//! schema and its integrity witnesses remain independent and unchanged.
const std = @import("std");
const A = std.mem.Allocator;
const Json = std.json.Value;

/// Allocate into the generation preparation arena. Input is the immutable,
/// already validated logical declaration; values/defaults/generated columns
/// have crossed SQL preparation before entering this read-only population.
pub fn derive(a: A, logical: []const u8) ![]const u8 {
    var value = try std.json.parseFromSliceLeaky(Json, a, logical, .{ .allocate = .alloc_always });
    if (value != .object) return error.InvalidTransactionSessionRecord;
    inline for (.{ "external_base_source", "sql_partition" }) |name| if (value.object.get(name)) |field| {
        if (field != .null) return error.UnsupportedSqlExecution;
    };
    var indexes: std.array_list.Managed(Json) = .init(a);
    var names: std.StringHashMapUnmanaged(void) = .empty;
    if (value.object.get("relational_indexes")) |declared| {
        if (declared != .null) {
            if (declared != .array) return error.InvalidTransactionSessionRecord;
            for (declared.array.items) |index| {
                try indexName(a, &names, index);
                try indexes.append(index);
            }
        }
    }
    if (value.object.get("unique_constraints")) |constraints| {
        if (constraints != .null) {
            if (constraints != .array) return error.InvalidTransactionSessionRecord;
            for (constraints.array.items) |constraint| {
                try indexName(a, &names, constraint);
                var index: std.json.ObjectMap = .empty;
                try index.put(a, "name", constraint.object.get("name").?);
                var keys: std.array_list.Managed(Json) = .init(a);
                if (constraint.object.get("keys")) |expressions| {
                    if (expressions != .null) {
                        if (expressions != .array) return error.InvalidTransactionSessionRecord;
                        try keys.appendSlice(expressions.array.items);
                    }
                }
                if (keys.items.len == 0) {
                    const columns = constraint.object.get("columns") orelse return error.InvalidTransactionSessionRecord;
                    if (columns != .array or columns.array.items.len == 0) return error.InvalidTransactionSessionRecord;
                    for (columns.array.items) |column| {
                        if (column != .string) return error.InvalidTransactionSessionRecord;
                        var key: std.json.ObjectMap = .empty;
                        try key.put(a, "column", column);
                        try keys.append(.{ .object = key });
                    }
                }
                // WITHOUT OVERLAPS already includes its final range column
                // in columns. Keep that key exactly once in the read index.
                try index.put(a, "keys", .{ .array = keys });
                inline for (.{ "where", "include_columns" }) |field| if (constraint.object.get(field)) |definition| {
                    if (definition != .null) try index.put(a, field, definition);
                };
                try indexes.append(.{ .object = index });
            }
        }
    }
    if (indexes.items.len != 0) try value.object.put(a, "relational_indexes", .{ .array = indexes });
    // These are mutation/retirement policies, never read-layout definitions.
    // In particular a historical multiset must not rebuild current UNIQUE/FK
    // witnesses or expire its initial rows through live-table TTL maintenance.
    inline for (.{ "unique_constraints", "foreign_keys", "checks", "sql_triggers", "ttl", "ttl_field" }) |field| _ = value.object.swapRemove(field);
    return std.json.Stringify.valueAlloc(a, value, .{});
}
pub fn sourceDefinition(manifest: [32]u8, declaration: []const u8) [32]u8 {
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("antfly/sql-frozen-layout/v1\x00");
    hash.update(&manifest);
    hash.update(declaration);
    var result: [32]u8 = undefined;
    hash.final(&result);
    return result;
}

fn indexName(a: A, names: *std.StringHashMapUnmanaged(void), value: Json) !void {
    if (value != .object) return error.InvalidTransactionSessionRecord;
    const name = value.object.get("name") orelse return error.InvalidTransactionSessionRecord;
    if (name != .string or name.string.len == 0 or (try names.getOrPut(a, name.string)).found_existing) return error.InvalidTransactionSessionRecord;
}

test "SQL generation intents frozen schema retains nonunique composite expression partial and covering access" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const logical =
        \\{"version":7,"storage_mode":"relational","unique_constraints":[{"name":"temporal","columns":["tenant","period"],"without_overlaps_column":"period"},{"name":"pk","columns":["tenant","id"],"primary":true},{"name":"expr","keys":[{"expression":{"op":"column","column":"body"},"result_type":"string"}],"include_columns":["id"],"where":[{"column":"id","op":"gt","value":0}]}],"relational_indexes":[{"name":"ordered","keys":[{"column":"id","direction":"desc"}]}],"checks":[],"foreign_keys":[],"column_defaults":[{"column":"id","expression":{"op":"literal","value":7}}],"generated_columns":[],"ttl":"1s","ttl_field":"at"}
    ;
    const derived = try derive(a, logical);
    const frozen = try std.json.parseFromSliceLeaky(Json, a, derived, .{});
    const indexes = frozen.object.get("relational_indexes").?.array.items;
    try std.testing.expectEqual(@as(usize, 4), indexes.len);
    try std.testing.expectEqual(@as(i64, 7), frozen.object.get("version").?.integer);
    try std.testing.expectEqual(@as(usize, 2), indexes[1].object.get("keys").?.array.items.len);
    try std.testing.expectEqual(@as(usize, 2), indexes[2].object.get("keys").?.array.items.len);
    try std.testing.expect(indexes[3].object.contains("where") and indexes[3].object.contains("include_columns"));
    try std.testing.expect(frozen.object.contains("column_defaults") and frozen.object.contains("generated_columns"));
    inline for (.{ "unique_constraints", "foreign_keys", "checks", "ttl", "ttl_field" }) |field| try std.testing.expect(!frozen.object.contains(field));
    const original = try std.json.parseFromSliceLeaky(Json, a, logical, .{});
    try std.testing.expect(original.object.contains("unique_constraints"));
    try std.testing.expectError(error.UnsupportedSqlExecution, derive(a, "{\"version\":1,\"sql_partition\":{}}"));
}
