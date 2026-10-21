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

//! Typed lake comparison operands. Timestamp comparisons use exact signed
//! nanoseconds independently of the timestamp's wire or SQL representation.
const std = @import("std");
const Json = std.json.Value;
pub fn comparisonValue(a: std.mem.Allocator, raw: Json, kind: @import("ast.zig").ColumnType) !Json {
    if (raw == .null) return raw;
    if (kind != .datetime) return @import("describe.zig").coerceAlloc(a, raw, kind);
    const ns: i128 = switch (raw) {
        .integer => |value| value,
        .number_string => |text| std.fmt.parseInt(i128, text, 10) catch return error.InvalidSqlDateTime,
        .string => |text| @import("../datetime.zig").rangeNanoseconds(raw) orelse @import("../datetime.zig").parseDateTimeToSignedNs(text) orelse return error.InvalidSqlDateTime,
        else => return error.SqlTypeMismatch,
    };
    return if (std.math.cast(i64, ns)) |value| .{ .integer = value } else .{ .number_string = try std.fmt.allocPrint(a, "{d}", .{ns}) };
}

test "lake SQL timestamp comparisons normalize signed values offsets and distant literals" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const epoch = try comparisonValue(a, .{ .string = "1970-01-01T01:00:00+01:00" }, .datetime);
    try std.testing.expectEqual(@as(i64, 0), epoch.integer);
    const negative = try comparisonValue(a, .{ .string = "1969-12-31T23:59:59.999999999Z" }, .datetime);
    try std.testing.expectEqual(@as(i64, -1), negative.integer);
    const far = try comparisonValue(a, .{ .string = "2500-01-01" }, .datetime);
    try std.testing.expectEqual(std.math.Order.gt, try @import("scalar.zig").compare(far, .{ .integer = std.math.maxInt(i64) }));
    try std.testing.expectError(error.InvalidSqlDateTime, comparisonValue(a, .{ .string = "not-a-date" }, .datetime));
    try std.testing.expectEqual(Json.null, try comparisonValue(a, .null, .datetime));
}

/// Preserve exact NUMERIC lexemes in untyped condition envelopes; ordinary
/// columns retain their existing normalization and signed timestamp contract.
pub fn conditionValue(a: std.mem.Allocator, raw: Json, column: @import("catalog.zig").Column) !Json {
    if (column.element_type == .numeric) return raw;
    return comparisonValue(a, raw, column.type);
}
pub fn comparisonDatum(a: std.mem.Allocator, raw: @import("scalar.zig").Datum, column: @import("catalog.zig").Column) !@import("scalar.zig").Datum {
    if (column.element_type == .numeric) return @import("describe.zig").coerceDatum(a, raw, column.type, column.element_type);
    return .{ .value = try comparisonValue(a, raw.value, column.type), .sql_null = raw.sql_null };
}

test "external lake NUMERIC predicates preserve exact conditions and native cells" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const column: @import("catalog.zig").Column = .{ .name = "k", .path = "k", .type = .number, .element_type = .numeric };
    const scalar = @import("scalar.zig");
    const large = try comparisonDatum(a, scalar.Datum.json(try conditionValue(a, .{ .string = "9007199254740993.125" }, column)), column);
    const adjacent = try comparisonDatum(a, scalar.Datum.json(.{ .string = "9007199254740993.124" }), column);
    try std.testing.expectEqual(std.math.Order.gt, try scalar.compareDatums(large, adjacent));
    try std.testing.expect((try comparisonDatum(a, large, column)).numeric == large.numeric);
    const integer = try comparisonDatum(a, scalar.Datum.json(.{ .integer = 9007199254740993 }), column);
    const before = try comparisonDatum(a, scalar.Datum.json(.{ .integer = 9007199254740992 }), column);
    try std.testing.expectEqual(std.math.Order.gt, try scalar.compareDatums(integer, before));
}

/// Normalize stored cells at their declared SQL width. Comparison operands keep
/// their own domain; narrowing a float8 literal would change mixed comparisons.
pub fn storedComparisonDatum(a: std.mem.Allocator, raw: @import("scalar.zig").Datum, column: @import("catalog.zig").Column) !@import("scalar.zig").Datum {
    if (column.type == .number and column.element_type != null and @import("builtin_cast.zig").floating(column.element_type.?))
        return @import("describe.zig").coerceDatum(a, raw, column.type, column.element_type);
    return comparisonDatum(a, raw, column);
}
