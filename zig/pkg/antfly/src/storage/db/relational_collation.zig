// Copyright 2026 Antfly, Inc.
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

const std = @import("std");

pub const TextComparison = enum { lt, eq, gt };

pub fn isCaseInsensitive(name: []const u8) bool {
    return std.ascii.eqlIgnoreCase(name, "antfly.case_insensitive") or
        std.ascii.eqlIgnoreCase(name, "case_insensitive") or
        std.ascii.eqlIgnoreCase(name, "ci");
}

pub fn isBinary(name: []const u8) bool {
    return std.ascii.eqlIgnoreCase(name, "C") or
        std.ascii.eqlIgnoreCase(name, "POSIX") or
        std.ascii.eqlIgnoreCase(name, "binary");
}

pub fn isSupported(name: []const u8) bool {
    return isBinary(name) or isCaseInsensitive(name);
}

pub fn textEqual(lhs: []const u8, rhs: []const u8, collation: ?[]const u8) bool {
    if (collation) |name| {
        if (isCaseInsensitive(name)) return std.ascii.eqlIgnoreCase(lhs, rhs);
    }
    return std.mem.eql(u8, lhs, rhs);
}

pub fn compareTextForScalar(lhs: []const u8, rhs: []const u8, collation: ?[]const u8) TextComparison {
    if (collation) |name| {
        if (isCaseInsensitive(name)) {
            const folded = std.ascii.orderIgnoreCase(lhs, rhs);
            if (folded == .eq) return .eq;
            return textComparisonFromOrder(folded);
        }
    }
    return textComparisonFromOrder(std.mem.order(u8, lhs, rhs));
}

pub fn compareTextForOrdering(lhs: []const u8, rhs: []const u8, collation: ?[]const u8) TextComparison {
    if (collation) |name| {
        if (isCaseInsensitive(name)) {
            const folded = std.ascii.orderIgnoreCase(lhs, rhs);
            if (folded != .eq) return textComparisonFromOrder(folded);
        }
    }
    return textComparisonFromOrder(std.mem.order(u8, lhs, rhs));
}

pub fn jsonValuesNotDistinctWithCollation(lhs: std.json.Value, rhs: std.json.Value, collation: ?[]const u8) ?bool {
    if (jsonNumberAsF64(lhs)) |left| {
        const right = jsonNumberAsF64(rhs) orelse return null;
        return left == right;
    }
    if (lhs == .string and rhs == .string) return textEqual(lhs.string, rhs.string, collation);
    if (lhs == .bool and rhs == .bool) return lhs.bool == rhs.bool;
    if (lhs == .null and rhs == .null) return true;
    return null;
}

fn jsonNumberAsF64(value: std.json.Value) ?f64 {
    return switch (value) {
        .integer => |integer| @floatFromInt(integer),
        .float => |float| float,
        .number_string => |number| std.fmt.parseFloat(f64, number) catch null,
        else => null,
    };
}

fn textComparisonFromOrder(order: std.math.Order) TextComparison {
    return switch (order) {
        .lt => .lt,
        .eq => .eq,
        .gt => .gt,
    };
}

test "relational collation centralizes scalar equality and stable ordering" {
    try std.testing.expect(isSupported("C"));
    try std.testing.expect(isSupported("binary"));
    try std.testing.expect(isCaseInsensitive("antfly.case_insensitive"));
    try std.testing.expect(isCaseInsensitive("CI"));
    try std.testing.expect(!isSupported("und-x-icu"));
    try std.testing.expect(textEqual("Alpha", "alpha", "ci"));
    try std.testing.expect(!textEqual("Alpha", "alpha", "C"));
    try std.testing.expectEqual(TextComparison.eq, compareTextForScalar("Alpha", "alpha", "ci"));
    try std.testing.expectEqual(TextComparison.lt, compareTextForOrdering("Alpha", "alpha", "ci"));
    try std.testing.expectEqual(true, jsonValuesNotDistinctWithCollation(.{ .string = "Alpha" }, .{ .string = "alpha" }, "ci"));
    try std.testing.expectEqual(true, jsonValuesNotDistinctWithCollation(.{ .integer = 42 }, .{ .float = 42.0 }, null));
    try std.testing.expectEqual(null, jsonValuesNotDistinctWithCollation(.{ .integer = 42 }, .{ .string = "42" }, null));
}
