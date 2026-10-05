// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//! Typed lake comparison operands. Timestamp comparisons use exact signed
//! nanoseconds independently of the timestamp's wire or SQL representation.
const std = @import("std");
const Json = std.json.Value;
pub fn comparisonValue(a: std.mem.Allocator, raw: Json, kind: @import("antfly_local_sources").sql_ast.ColumnType) !Json {
    if (raw == .null) return raw;
    if (kind != .datetime) return @import("antfly_local_sources").sql_describe.coerceAlloc(a, raw, kind);
    const ns: i128 = switch (raw) {
        .integer => |value| value,
        .number_string => |text| std.fmt.parseInt(i128, text, 10) catch return error.InvalidSqlDateTime,
        .string => |text| @import("antfly_local_sources").datetime.parseDateTimeToSignedNs(text) orelse return error.InvalidSqlDateTime,
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
    try std.testing.expectEqual(std.math.Order.gt, try @import("antfly_local_sources").sql_scalar.compare(far, .{ .integer = std.math.maxInt(i64) }));
    try std.testing.expectError(error.InvalidSqlDateTime, comparisonValue(a, .{ .string = "not-a-date" }, .datetime));
    try std.testing.expectEqual(Json.null, try comparisonValue(a, .null, .datetime));
}
