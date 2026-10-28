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

//! Native catalog declarations and expression widths share one SQL identity.
const std = @import("std");
const ast = @import("ast.zig");
const catalog = @import("catalog.zig");
const compiler = @import("compiler.zig");
const describe = @import("describe.zig");

const Fixture = struct {
    columns: [1]catalog.Column = .{.{ .name = "v", .path = "v", .type = .integer }},
    fn resolve(raw: *anyopaque, _: std.mem.Allocator, name: ast.Name, action: catalog.Action) !catalog.Table {
        const self: *@This() = @ptrCast(@alignCast(raw));
        try std.testing.expect(action == .read);
        try std.testing.expectEqualStrings("t", name.table);
        return .{ .id = 7, .physical_name = "t", .schema_version = 1, .columns = &self.columns };
    }
    fn checkpoint(_: *anyopaque) !void {}
    fn backend(self: *@This()) catalog.Backend {
        return .{ .ptr = self, .vtable = &.{ .resolve = resolve, .scan = undefined, .mutate = undefined, .checkpoint = checkpoint } };
    }
};

test "SQL scalar input set binding canonicalizes native default widths" {
    for ([_]ast.ColumnType{ .integer, .number }) |kind| {
        var fixture: Fixture = .{};
        fixture.columns[0].type = kind;
        for ([_][]const u8{
            "SELECT v AS x FROM t UNION ALL SELECT v + 1 FROM t",
            "SELECT v + 1 AS x FROM t UNION ALL SELECT v FROM t",
            "SELECT v AS x FROM t WHERE v=$1 UNION ALL SELECT v+1 FROM t WHERE v=$1",
            "SELECT x FROM (SELECT v AS x FROM t UNION ALL SELECT v + 1 FROM t) s UNION ALL SELECT v FROM t",
        }) |sql| {
            var compiled = try compiler.compile(std.testing.allocator, sql, .{});
            defer compiled.deinit();
            const hints: []const ?ast.ColumnType = if (compiled.parameter_count == 0) &.{} else &.{kind};
            var result = try describe.describe(std.testing.allocator, fixture.backend(), &compiled, hints);
            defer result.deinit();
            try std.testing.expectEqual(@as(usize, 1), result.binding.columns.len);
            try std.testing.expectEqual(kind, result.binding.columns[0].type);
            try std.testing.expectEqual(if (kind == .integer) @import("array_value.zig").ElementType.int64 else .float64, result.binding.columns[0].element_type.?);
        }
    }
}

test "SQL scalar input set binding still rejects incompatible native types" {
    var fixture: Fixture = .{};
    fixture.columns[0].type = .string;
    var compiled = try compiler.compile(std.testing.allocator, "SELECT v FROM t UNION ALL SELECT CAST(1 AS BIGINT)", .{});
    defer compiled.deinit();
    try std.testing.expectError(error.SqlTypeMismatch, describe.describe(std.testing.allocator, fixture.backend(), &compiled, &.{}));
}
