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

//! Independent typed batch ownership for bounded producer/consumer overlap.
const std = @import("std");
const rows = @import("antfly_local_sources").storage_rowsource_types;
const A = std.mem.Allocator;
fn strings(a: A, values: []const []const u8, nulls: rows.NullBitmap) ![]const []const u8 {
    const result = try a.alloc([]const u8, values.len);
    var total: usize = 0;
    for (0..values.len) |row| if (!nulls.isNull(row)) {
        total = std.math.add(usize, total, values[row].len) catch return error.OutOfMemory;
    };
    const payload = try a.alloc(u8, total);
    var offset: usize = 0;
    for (result, 0..) |*out, row| {
        if (nulls.isNull(row)) {
            out.* = "";
            continue;
        }
        const value = values[row];
        @memcpy(payload[offset..][0..value.len], value);
        out.* = payload[offset..][0..value.len];
        offset += value.len;
    }
    return result;
}
fn external(a: A, value: rows.ExternalRowRef) !rows.ExternalRowRef {
    var result = value;
    result.source_id = try a.dupe(u8, value.source_id);
    result.snapshot_id = try a.dupe(u8, value.snapshot_id);
    result.file_id = try a.dupe(u8, value.file_id);
    return result;
}
/// All allocations belong to an enclosing bounded arena, including error paths.
pub fn copy(a: A, input: rows.ColumnBatch) !rows.ColumnBatch {
    var result = input;
    result.snapshot.table_id = try a.dupe(u8, input.snapshot.table_id);
    result.snapshot.snapshot_id = try a.dupe(u8, input.snapshot.snapshot_id);
    if (input.external_span) |span| {
        result.external_span = .{ .first = try external(a, span.first), .count = span.count };
    } else {
        const refs = try a.dupe(rows.RowRef, input.row_refs);
        for (refs) |*ref| switch (ref.*) {
            .external => |value| ref.* = .{ .external = try external(a, value) },
            .relational_key => |value| ref.* = .{ .relational_key = try a.dupe(u8, value) },
            .serverless => |value| {
                var owned = value;
                owned.fragment_id = try a.dupe(u8, value.fragment_id);
                ref.* = .{ .serverless = owned };
            },
        };
        result.row_refs = refs;
    }
    const columns = try a.dupe(rows.ColumnVector, input.columns);
    for (columns) |*column| {
        column.name = try a.dupe(u8, column.name);
        column.nulls = .{ .bytes = try column.nulls.dupeBytes(a, input.rowCount()) };
        column.values = switch (column.values) {
            .i64 => |v| .{ .i64 = try a.dupe(i64, v) },
            .f64 => |v| .{ .f64 = try a.dupe(f64, v) },
            .bool => |v| .{ .bool = try a.dupe(bool, v) },
            .bytes => |v| .{ .bytes = try strings(a, v, column.nulls) },
            .json => |v| .{ .json = try strings(a, v, column.nulls) },
            .decimal128 => |v| .{ .decimal128 = .{ .values = try a.dupe(i128, v.values), .precision = v.precision, .scale = v.scale } },
            .dictionary_i64 => |v| .{ .dictionary_i64 = .{ .values = try a.dupe(i64, v.values), .indices = try a.dupe(u32, v.indices) } },
            .dictionary_f64 => |v| .{ .dictionary_f64 = .{ .values = try a.dupe(f64, v.values), .indices = try a.dupe(u32, v.indices) } },
            .dictionary_bytes => |v| .{ .dictionary_bytes = .{ .values = try strings(a, v.values, .{}), .indices = try a.dupe(u32, v.indices) } },
            .vector_f32 => |v| value: {
                const vectors = try a.alloc([]const f32, v.len);
                for (vectors, 0..) |*out, row| out.* = if (column.nulls.isNull(row)) &.{} else try a.dupe(f32, v[row]);
                break :value .{ .vector_f32 = vectors };
            },
            .encoded => |v| value: {
                const n = input.rowCount();
                const output: rows.ColumnValues = switch (v.kind) {
                    .i64 => .{ .i64 = try a.alloc(i64, n) },
                    .f64 => .{ .f64 = try a.alloc(f64, n) },
                    .bool => .{ .bool = try a.alloc(bool, n) },
                    .bytes => .{ .bytes = try a.alloc([]const u8, n) },
                    .json => .{ .json = try a.alloc([]const u8, n) },
                    else => return error.RowSourceColumnKindMismatch,
                };
                for (0..n) |row| {
                    const is_null = column.nulls.isNull(row);
                    const cell = if (is_null) @as(std.json.Value, .null) else try v.at(row);
                    switch (output) {
                        .i64 => |lanes| @constCast(lanes)[row] = if (is_null) 0 else if (cell == .integer) cell.integer else return error.RowSourceColumnKindMismatch,
                        .f64 => |lanes| @constCast(lanes)[row] = if (is_null) 0 else if (cell == .float) cell.float else return error.RowSourceColumnKindMismatch,
                        .bool => |lanes| @constCast(lanes)[row] = !is_null and (if (cell == .bool) cell.bool else return error.RowSourceColumnKindMismatch),
                        .bytes => |lanes| @constCast(lanes)[row] = if (is_null) "" else if (cell == .string) try a.dupe(u8, cell.string) else return error.RowSourceColumnKindMismatch,
                        .json => |lanes| @constCast(lanes)[row] = if (is_null) "" else try std.json.Stringify.valueAlloc(a, cell, .{}),
                        else => unreachable,
                    }
                }
                break :value output;
            },
        };
    }
    result.columns = columns;
    return result;
}

fn copyScenario(a: A) !void {
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    var numbers = [_]i64{ 9007199254740993, -7 };
    var coefficients = [_]i128{ -12345, 67890 };
    var dictionary = [_][]const u8{ "alpha", "beta" };
    var ids = [_]u32{ 1, 0 };
    const optional = [_][]const u8{ "present", undefined };
    const input: rows.ColumnBatch = .{ .snapshot = .{ .table_id = "t", .snapshot_id = "s" }, .external_span = .{ .first = .{ .source_id = "t", .snapshot_id = "s", .file_id = "f", .row_group_ordinal = 2, .row_ordinal = 9 }, .count = 2 }, .columns = &.{
        .{ .name = "n", .values = .{ .i64 = &numbers } },
        .{ .name = "exact", .values = .{ .decimal128 = .{ .values = &coefficients, .precision = 12, .scale = 2 } } },
        .{ .name = "optional", .values = .{ .bytes = &optional }, .nulls = .{ .bytes = &.{ 0, 1 } } },
        .{ .name = "label", .values = .{ .dictionary_bytes = .{ .values = &dictionary, .indices = &ids } }, .nulls = .{ .bits = .{ .words = &.{2}, .offset = 0 } } },
    } };
    const result = try copy(arena.allocator(), input);
    numbers[0] = 0;
    coefficients[0] = 0;
    dictionary[0] = "changed";
    ids[0] = 0;
    try result.validate();
    try std.testing.expectEqual(@as(i64, 9007199254740993), result.columns[0].values.i64[0]);
    try std.testing.expectEqual(@as(i128, -12345), result.columns[1].values.decimal128.values[0]);
    try std.testing.expectEqualStrings("beta", result.columns[3].values.dictionary_bytes.at(0));
    try std.testing.expect(result.columns[3].nulls.isNull(1));
    try std.testing.expectEqual(@as(u64, 10), result.rowRef(1).external.row_ordinal);
}
test "external lake owned streaming batches preserve typed dictionaries decimals identities and allocation cleanup" {
    try copyScenario(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, copyScenario, .{});
}
