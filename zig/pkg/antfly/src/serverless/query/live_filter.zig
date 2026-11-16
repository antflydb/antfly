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

const std = @import("std");
const A = std.mem.Allocator;
pub const Clause = struct { field: []const u8, eq: std.json.Value };
pub const Filter = struct {
    alloc: A,
    clauses: []const Clause,
    pub fn validate(self: Filter) !void {
        if (self.clauses.len > 16) return error.FilterBudgetExceeded;
        for (self.clauses) |clause| {
            if (clause.field.len == 0 or clause.field.len > 256) return error.InvalidLiveFilter;
            switch (clause.eq) {
                .null, .bool, .integer, .float => {},
                .string => |value| if (value.len > 4096) return error.FilterBudgetExceeded,
                else => return error.InvalidLiveFilter,
            }
        }
    }
    pub fn matches(self: Filter, document: anytype) !bool {
        const parsed = try std.json.parseFromSlice(std.json.Value, self.alloc, document.body, .{});
        defer parsed.deinit();
        for (self.clauses) |clause| {
            if (std.mem.eql(u8, clause.field, "$id")) {
                if (clause.eq != .string or !std.mem.eql(u8, clause.eq.string, document.doc_id)) return false;
                continue;
            }
            var value = parsed.value;
            var path = std.mem.splitScalar(u8, clause.field, '.');
            while (path.next()) |component| {
                if (value != .object) return false;
                value = value.object.get(component) orelse return false;
            }
            if (!equal(value, clause.eq)) return false;
        }
        return true;
    }
    fn integerFloat(integer: i64, float: f64) bool {
        return float >= -0x1p63 and float < 0x1p63 and @trunc(float) == float and @as(i64, @intFromFloat(float)) == integer;
    }
    fn equal(a: std.json.Value, b: std.json.Value) bool {
        return switch (a) {
            .null => b == .null,
            .bool => |value| b == .bool and b.bool == value,
            .integer => |value| if (b == .integer) b.integer == value else b == .float and integerFloat(value, b.float),
            .float => |value| if (b == .float) b.float == value else b == .integer and integerFloat(b.integer, value),
            .string => |value| b == .string and std.mem.eql(u8, b.string, value),
            else => false,
        };
    }
};
test "serverless live scalar filters inspect current nested metadata without numeric rounding" {
    const filter = Filter{ .alloc = std.testing.allocator, .clauses = &.{ .{ .field = "state.read", .eq = .{ .bool = true } }, .{ .field = "version", .eq = .{ .integer = 9007199254740993 } } } };
    try filter.validate();
    try std.testing.expect(try filter.matches(.{ .doc_id = "a", .body = "{\"state\":{\"read\":true},\"version\":9007199254740993}" }));
    try std.testing.expect(!try filter.matches(.{ .doc_id = "a", .body = "{\"state\":{\"read\":true},\"version\":9007199254740992.0}" }));
}
