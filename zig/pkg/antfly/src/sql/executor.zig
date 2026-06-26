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

const classifier = @import("classifier.zig");
const ddl_plan = @import("ddl_plan.zig");
const lower_expr = @import("lower_expr.zig");
const tokenized = @import("tokenized.zig");

pub const SqlExecutionPlan = union(enum) {
    read: ReadStatement,
    write: WriteStatement,
    ddl: ddl_plan.LoweredDdlPlan,

    pub const ReadStatement = struct {
        kind: classifier.SqlReadStatementKind,
    };

    pub const WriteStatement = struct {
        kind: classifier.SqlWriteStatementKind,
    };

    pub fn deinit(self: *SqlExecutionPlan, alloc: std.mem.Allocator) void {
        switch (self.*) {
            .ddl => |*plan| plan.deinit(alloc),
            .read, .write => {},
        }
        self.* = undefined;
    }

    pub fn statementKindName(self: SqlExecutionPlan) []const u8 {
        return switch (self) {
            .read => |statement| @tagName(statement.kind),
            .write => |statement| @tagName(statement.kind),
            .ddl => "ddl",
        };
    }
};

pub fn classifyParsedSql(parsed_sql: *const tokenized.ParsedSql) ?SqlExecutionPlan {
    if (parsed_sql.writeStatementKind()) |kind| {
        return .{ .write = .{ .kind = kind } };
    }
    if (parsed_sql.readStatementKind()) |kind| {
        return .{ .read = .{ .kind = kind } };
    }
    return null;
}

pub fn lowerDdlLogicalPlanParsedSqlWithFunctionBindingsAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const tokenized.ParsedSql,
    function_bindings: lower_expr.SqlFunctionBindings,
) !SqlExecutionPlan {
    return .{ .ddl = try ddl_plan.lowerDdlPlanParsedSqlWithFunctionBindingsAlloc(alloc, parsed_sql, function_bindings) };
}

test "sql executor classifies statement families and owns ddl plans" {
    const alloc = std.testing.allocator;

    var read_sql = try tokenized.ParsedSql.initAlloc(alloc, "SELECT id FROM usage_records");
    defer read_sql.deinit(alloc);
    const read_plan = classifyParsedSql(&read_sql) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(classifier.SqlReadStatementKind.query, read_plan.read.kind);

    var write_sql = try tokenized.ParsedSql.initAlloc(alloc, "INSERT INTO usage_records (id) VALUES ('evt-1')");
    defer write_sql.deinit(alloc);
    const write_plan = classifyParsedSql(&write_sql) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(classifier.SqlWriteStatementKind.insert, write_plan.write.kind);

    var ddl_sql = try tokenized.ParsedSql.initAlloc(alloc, "CREATE TABLE usage_records (id text PRIMARY KEY)");
    defer ddl_sql.deinit(alloc);
    var ddl_logical = try lowerDdlLogicalPlanParsedSqlWithFunctionBindingsAlloc(alloc, &ddl_sql, .{});
    defer ddl_logical.deinit(alloc);
    try std.testing.expectEqualStrings("ddl", ddl_logical.statementKindName());
}
