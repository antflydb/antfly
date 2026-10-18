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

//! Residual lake predicates dispatch once per native column. Pruning remains
//! advisory; this exact mask preserves NULL and PostgreSQL comparison rules.
const std = @import("std");
const A = std.mem.Allocator;
const catalog = @import("catalog.zig");
const types = @import("../storage/rowsource/types.zig");
const Op = catalog.Condition.Op;
fn accepts(op: Op, order: std.math.Order) bool {
    return switch (op) {
        .eq => order == .eq,
        .neq => order != .eq,
        .lt => order == .lt,
        .lte => order != .gt,
        .gt => order == .gt,
        .gte => order != .lt,
        else => unreachable,
    };
}
fn orderValue(comptime T: type, left: T, right: T) std.math.Order {
    if (T == []const u8) return std.mem.order(u8, left, right);
    if (T == bool) return std.math.order(@intFromBool(left), @intFromBool(right));
    if (T == f64) {
        if (std.math.isNan(left)) return if (std.math.isNan(right)) .eq else .gt;
        if (std.math.isNan(right)) return .lt;
    }
    return std.math.order(left, right);
}
fn flat(comptime T: type, values: []const T, nulls: types.NullBitmap, keep: []bool, literal: T, op: Op) void {
    var row: usize = 0;
    while (row < keep.len) {
        if (T == i64 or T == f64) {
            const dense = row + 4 <= keep.len and for (keep[row..][0..4], row..) |active, index| {
                if (!active or nulls.isNull(index)) break false;
                if (T == f64) if (!std.math.isFinite(values[index]) or !std.math.isFinite(literal)) break false;
            } else true;
            if (dense) {
                const l: @Vector(4, T) = values[row..][0..4].*;
                const r: @Vector(4, T) = @splat(literal);
                keep[row..][0..4].* = switch (op) {
                    .eq => l == r,
                    .neq => l != r,
                    .lt => l < r,
                    .lte => l <= r,
                    .gt => l > r,
                    .gte => l >= r,
                    else => unreachable,
                };
                row += 4;
                continue;
            }
        }
        if (keep[row]) keep[row] = !nulls.isNull(row) and accepts(op, orderValue(T, values[row], literal));
        row += 1;
    }
}
fn dictionary(comptime T: type, values: []const T, indices: []const u32, nulls: types.NullBitmap, keep: []bool, literal: T, op: Op) !void {
    // Page dictionaries may be much larger than the selected cohort. Evaluate
    // referenced values only, with a fixed bounded memo and no allocation.
    var memo: [8192]u8 = @splat(0);
    var identities: [8192]u32 = undefined;
    var cached: usize = 0;
    for (keep, 0..) |*active, row| {
        if (!active.*) continue;
        if (nulls.isNull(row)) {
            active.* = false;
            continue;
        }
        const id = indices[row];
        if (id >= values.len) return error.InvalidSqlBackendResponse;
        var slot: usize = (id *% 2654435761) & (memo.len - 1);
        while (memo[slot] != 0 and identities[slot] != id) slot = (slot + 1) & (memo.len - 1);
        if (memo[slot] != 0) {
            active.* = memo[slot] == 2;
            continue;
        }
        active.* = accepts(op, orderValue(T, values[id], literal));
        if (cached < 4096) {
            identities[slot] = id;
            memo[slot] = if (active.*) 2 else 1;
            cached += 1;
        }
    }
}
fn encoded(values: types.EncodedValues, nulls: types.NullBitmap, keep: []bool, literal: std.json.Value, op: Op) !void {
    var memo: [8192]u8 = @splat(0);
    var identities: [8192]u32 = undefined;
    var cached: usize = 0;
    for (keep, 0..) |*active, row| {
        if (!active.*) continue;
        if (nulls.isNull(row)) {
            active.* = false;
            continue;
        }
        const id = if (values.dictionary_id) |identify| identify(values.ptr, values.begin + row) else null;
        var slot: usize = 0;
        if (id) |value| {
            slot = (value *% 2654435761) & (memo.len - 1);
            while (memo[slot] != 0 and identities[slot] != value) slot = (slot + 1) & (memo.len - 1);
            if (memo[slot] != 0) {
                active.* = memo[slot] == 2;
                continue;
            }
        }
        const value = try values.at(row);
        const order = switch (value) {
            .integer => if (literal == .integer) orderValue(i64, value.integer, literal.integer) else return error.InvalidSqlBackendResponse,
            .float => if (literal == .float) orderValue(f64, value.float, literal.float) else return error.InvalidSqlBackendResponse,
            .string => if (literal == .string) orderValue([]const u8, value.string, literal.string) else return error.InvalidSqlBackendResponse,
            .bool => if (literal == .bool) orderValue(bool, value.bool, literal.bool) else return error.InvalidSqlBackendResponse,
            else => return error.InvalidSqlBackendResponse,
        };
        active.* = accepts(op, order);
        if (id != null and cached < 4096) {
            identities[slot] = id.?;
            memo[slot] = if (active.*) 2 else 1;
            cached += 1;
        }
    }
}
pub fn apply(a: A, batch: types.ColumnBatch, table: catalog.Table, condition: catalog.Condition, keep: []bool) !bool {
    if (keep.len != batch.rowCount()) return error.InvalidSqlBackendResponse;
    const vector = batch.findColumn(condition.column) orelse return false;
    if (condition.op == .is_null or condition.op == .is_not_null) {
        for (keep, 0..) |*active, row| if (active.*) {
            active.* = vector.nulls.isNull(row) == (condition.op == .is_null);
        };
        return true;
    }
    const definition = try table.column(condition.column);
    // Physical callbacks do not carry the SQL width. Use the typed cell
    // boundary for REAL, including lazy encoded/dictionary lanes.
    if (definition.element_type == .float32) return false;
    if (definition.element_type == .numeric) {
        if (vector.values != .decimal128 or definition.numeric_modifier != null) return false;
        if (condition.datum().sql_null) {
            @memset(keep, false);
            return true;
        }
        var nonnull = false;
        for (keep, 0..) |active, row| if (active and !vector.nulls.isNull(row)) {
            nonnull = true;
            break;
        };
        if (!nonnull) {
            @memset(keep, false);
            return true;
        }
        const literal = try @import("lake_values.zig").comparisonDatum(a, condition.datum(), definition);
        const number = literal.numeric orelse return false;
        const decimal = vector.values.decimal128;
        const numeric = @import("numeric_value.zig");
        const coefficient = numeric.coefficientAtScale(number.*, @intCast(decimal.scale));
        var context: numeric.Context = .{ .alloc = a };
        for (keep, 0..) |*active, row| {
            if (!active.*) continue;
            if (vector.nulls.isNull(row)) {
                active.* = false;
                continue;
            }
            try @import("../common/decimal128.zig").validateCoefficient(decimal.values[row], decimal.precision, decimal.scale);
            const order = if (coefficient) |value| std.math.order(decimal.values[row], value) else fallback: {
                const fixed = try numeric.ScaledInteger.init(decimal.values[row], @intCast(decimal.scale));
                break :fallback try numeric.order(&context, fixed.view(), number.*);
            };
            active.* = accepts(condition.op, order);
        }
        return true;
    }

    const supported = switch (vector.values) {
        .encoded => |values| switch (values.kind) {
            .i64 => definition.type == .integer or definition.type == .datetime,
            .f64 => definition.type == .number,
            .bool => definition.type == .boolean,
            .bytes => definition.type == .string,
            else => false,
        },

        .i64, .dictionary_i64 => definition.type == .integer or definition.type == .datetime,
        .f64, .dictionary_f64 => definition.type == .number,
        .bytes, .dictionary_bytes => definition.type == .string,
        .bool => definition.type == .boolean,
        else => false,
    };
    if (!supported) return false;
    if (for (keep) |active| {
        if (active) break false;
    } else true) return true;
    // Literal coercion stays lazy when every surviving lane is SQL NULL.
    const nonnull = for (keep, 0..) |active, row| {
        if (active and !vector.nulls.isNull(row)) break true;
    } else false;
    if (!nonnull) {
        @memset(keep, false);
        return true;
    }
    // Untyped lake number coercion rejects nonfinite cells. Decline before
    // changing any mask so the established scalar boundary reports its error.
    switch (vector.values) {
        .encoded => |values| if (values.kind == .f64 and values.mask == null) {
            for (keep, 0..) |active, row| {
                if (active and !vector.nulls.isNull(row) and !std.math.isFinite((try values.at(row)).float)) return false;
            }
        },
        .f64 => |values| for (keep, 0..) |active, row| {
            if (active and !vector.nulls.isNull(row) and !std.math.isFinite(values[row])) return false;
        },
        .dictionary_f64 => |values| for (keep, 0..) |active, row| {
            if (!active or vector.nulls.isNull(row)) continue;
            const id = values.indices[row];
            if (id >= values.values.len) return error.InvalidSqlBackendResponse;
            if (!std.math.isFinite(values.values[id])) return false;
        },
        else => {},
    }
    if (condition.value == .null) {
        @memset(keep, false);
        return true;
    }
    const literal = (try @import("lake_values.zig").comparisonDatum(a, condition.datum(), definition)).value;
    switch (vector.values) {
        .encoded => |values| {
            if (values.mask) |mask| return mask(values.ptr, values.begin, keep, literal, switch (condition.op) {
                .eq => .eq,
                .neq => .neq,
                .lt => .lt,
                .lte => .lte,
                .gt => .gt,
                .gte => .gte,
                else => unreachable,
            });
            try encoded(values, vector.nulls, keep, literal, condition.op);
        },

        inline .i64, .dictionary_i64 => |values, tag| {
            if (literal != .integer) return false;
            if (tag == .i64) flat(i64, values, vector.nulls, keep, literal.integer, condition.op) else try dictionary(i64, values.values, values.indices, vector.nulls, keep, literal.integer, condition.op);
        },
        inline .f64, .dictionary_f64 => |values, tag| {
            // The literal has already crossed the shared FLOAT coercion
            // boundary, including integral bounds and its rounding contract.
            if (literal != .float) return false;
            if (tag == .f64) flat(f64, values, vector.nulls, keep, literal.float, condition.op) else try dictionary(f64, values.values, values.indices, vector.nulls, keep, literal.float, condition.op);
        },
        inline .bytes, .dictionary_bytes => |values, tag| {
            if (literal != .string) return false;
            if (tag == .bytes) flat([]const u8, values, vector.nulls, keep, literal.string, condition.op) else try dictionary([]const u8, values.values, values.indices, vector.nulls, keep, literal.string, condition.op);
        },
        .bool => |values| {
            if (literal != .bool) return false;
            flat(bool, values, vector.nulls, keep, literal.bool, condition.op);
        },
        else => unreachable,
    }
    return true;
}

test "lake SQL native predicate masks match scalar null dictionary timestamp and NaN rules" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const alloc = arena.allocator();
    const integer = [_]i64{ -7, 0, 9007199254740993, 19, 2, 7, 9, 20 };
    const numbers = [_]f64{ -0.0, 0, std.math.nan(f64), std.math.inf(f64), -std.math.inf(f64), 3, 4, 5 };
    const ids = [_]u32{ 0, 1, 2, 0, 1, 2, 0, 1 };
    const vectors = [_]types.ColumnVector{
        .{ .name = "k", .values = .{ .i64 = &integer }, .nulls = .{ .bytes = &.{ 0, 1, 0, 0, 0, 0, 0, 0 } } },
        .{ .name = "k", .values = .{ .dictionary_i64 = .{ .values = &.{ -7, 9007199254740993, 9 }, .indices = &ids } } },
        .{ .name = "k", .values = .{ .f64 = &numbers } },
        .{ .name = "k", .values = .{ .dictionary_bytes = .{ .values = &.{ "alpha", "beta", "omega" }, .indices = &ids } } },
        .{ .name = "k", .values = .{ .i64 = &integer } },
    };
    const kinds = [_]@import("ast.zig").ColumnType{ .integer, .integer, .number, .string, .datetime };
    const literals = [_]std.json.Value{ .{ .integer = 9 }, .{ .integer = 9007199254740993 }, .{ .float = 0 }, .{ .string = "beta" }, .{ .integer = 0 } };
    for (vectors, kinds, literals) |vector, kind, literal| {
        const table: catalog.Table = .{ .id = 1, .physical_name = "t", .schema_version = 1, .columns = &.{.{ .name = "k", .path = "k", .type = kind }} };
        const batch: types.ColumnBatch = .{ .snapshot = .{ .table_id = "t", .snapshot_id = "s" }, .external_span = .{ .first = .{ .source_id = "t", .snapshot_id = "s", .file_id = "f", .row_group_ordinal = 0, .row_ordinal = 0 }, .count = integer.len }, .columns = &.{vector} };
        for ([_]Op{ .eq, .neq, .lt, .lte, .gt, .gte, .is_null, .is_not_null }) |op| {
            const condition: catalog.Condition = .{ .column = "k", .op = op, .value = literal };
            var keep: [8]bool = @splat(true);
            if (kind == .number) @memset(keep[2..5], false);
            try std.testing.expect(try apply(alloc, batch, table, condition, &keep));
            for (keep, 0..) |actual, row| {
                const page: catalog.ColumnPage = .{ .batch = batch, .selection = &.{row} };
                const cell = try page.cell(alloc, 0, "k");
                const excluded = kind == .number and row >= 2 and row < 5;
                const expected = if (excluded) false else if (op == .is_null) cell.sql_null else if (op == .is_not_null) !cell.sql_null else if (cell.sql_null) false else blk: {
                    const stored = try @import("lake_values.zig").comparisonDatum(alloc, cell, table.columns[0]);
                    const bound = try @import("lake_values.zig").comparisonDatum(alloc, condition.datum(), table.columns[0]);
                    break :blk @import("scalar.zig").comparison(switch (op) {
                        .eq => .eq,
                        .neq => .neq,
                        .lt => .lt,
                        .lte => .lte,
                        .gt => .gt,
                        .gte => .gte,
                        else => unreachable,
                    }, try @import("scalar.zig").compareDatums(stored, bound)).bool;
                };
                try std.testing.expectEqual(expected, actual);
            }
        }
    }
    const floats: types.ColumnBatch = .{ .snapshot = .{ .table_id = "t", .snapshot_id = "s" }, .row_refs = &.{.{ .relational_key = "a" }}, .columns = &.{.{ .name = "k", .values = .{ .f64 = &.{std.math.nan(f64)} } }} };
    const float_table: catalog.Table = .{ .id = 1, .physical_name = "t", .schema_version = 1, .columns = &.{.{ .name = "k", .path = "k", .type = .number }} };
    var active = [_]bool{true};
    try std.testing.expect(!try apply(alloc, floats, float_table, .{ .column = "k", .op = .eq, .value = .{ .float = 0 } }, &active));
    try std.testing.expect(active[0]);
    const finite: types.ColumnBatch = .{ .snapshot = floats.snapshot, .row_refs = floats.row_refs, .columns = &.{.{ .name = "k", .values = .{ .f64 = &.{9007199254740992} } }} };
    try std.testing.expect(try apply(alloc, finite, float_table, .{ .column = "k", .op = .gt, .value = .{ .integer = 0 } }, &active));
    active[0] = true;
    try std.testing.expect(try apply(alloc, finite, float_table, .{ .column = "k", .op = .eq, .value = .{ .integer = 9007199254740993 } }, &active));
    try std.testing.expect(active[0]);
    // A malformed unselected dictionary entry must not be evaluated.
    const batch: types.ColumnBatch = .{ .snapshot = .{ .table_id = "t", .snapshot_id = "s" }, .row_refs = &.{ .{ .relational_key = "a" }, .{ .relational_key = "b" } }, .columns = &.{.{ .name = "k", .values = .{ .dictionary_i64 = .{ .values = &.{7}, .indices = &.{ 0, 99 } } } }} };
    const table: catalog.Table = .{ .id = 1, .physical_name = "t", .schema_version = 1, .columns = &.{.{ .name = "k", .path = "k", .type = .integer }} };
    var keep = [_]bool{ true, false };
    try std.testing.expect(try apply(alloc, batch, table, .{ .column = "k", .op = .eq, .value = .{ .integer = 7 } }, &keep));
    try std.testing.expect(keep[0] and !keep[1]);
}

test "lake SQL encoded predicates memoize dictionary values without materializing lanes" {
    const Harness = struct {
        ids: [8]u32 = .{ 0, 1, 0, 1, 0, 1, 0, 1 },
        reads: usize = 0,
        fn read(raw: *const anyopaque, row: usize) !std.json.Value {
            const self: *@This() = @ptrCast(@alignCast(@constCast(raw)));
            self.reads += 1;
            return .{ .integer = if (self.ids[row] == 0) 7 else 99 };
        }
        fn id(raw: *const anyopaque, row: usize) ?u32 {
            const self: *const @This() = @ptrCast(@alignCast(raw));
            return self.ids[row];
        }
    };
    var harness: Harness = .{};
    const table: catalog.Table = .{ .id = 1, .physical_name = "t", .schema_version = 1, .columns = &.{.{ .name = "k", .path = "k", .type = .integer }} };
    const vector: types.ColumnVector = .{ .name = "k", .values = .{ .encoded = .{ .ptr = &harness, .begin = 1, .count = 6, .kind = .i64, .read = Harness.read, .dictionary_id = Harness.id } }, .nulls = .{ .bytes = &.{ 0, 0, 0, 0, 1, 0 } } };
    const batch: types.ColumnBatch = .{ .snapshot = .{ .table_id = "t", .snapshot_id = "s" }, .external_span = .{ .first = .{ .source_id = "t", .snapshot_id = "s", .file_id = "f", .row_group_ordinal = 0, .row_ordinal = 0 }, .count = 6 }, .columns = &.{vector} };
    var keep = [_]bool{ true, false, true, true, true, true };
    try std.testing.expect(try apply(std.testing.allocator, batch, table, .{ .column = "k", .op = .eq, .value = .{ .integer = 7 } }, &keep));
    try std.testing.expectEqualSlices(bool, &.{ false, false, false, true, false, true }, &keep);
    try std.testing.expectEqual(@as(usize, 2), harness.reads);
}
