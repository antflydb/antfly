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

const binder = @import("binder.zig");
const catalog_resources = @import("catalog_resources.zig");
const ddl_plan = @import("ddl.zig");
const lower_expr = @import("lower_expr.zig");
const table_catalog = @import("../api/table_catalog.zig");
const tokenized = @import("tokenized.zig");

pub fn planLogicalDdlPlanParsedSqlWithFunctionBindingsAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const tokenized.ParsedSql,
    function_bindings: lower_expr.SqlFunctionBindings,
) !binder.LogicalSqlPlan {
    return try ddl_plan.parseLogicalDdlPlanAlloc(alloc, parsed_sql, function_bindings);
}

pub fn planLogicalDdlPlanBoundStatementWithFunctionBindingsAlloc(
    alloc: std.mem.Allocator,
    bound: *binder.BoundSqlStatement,
    function_bindings: lower_expr.SqlFunctionBindings,
) !binder.LogicalSqlPlan {
    _ = alloc;
    _ = function_bindings;
    return try bound.takeDdlLogicalPlan();
}

pub const parseLogicalDdlPlanAlloc = ddl_plan.parseLogicalDdlPlanAlloc;
pub const planGeneratedLogicalDdlAstAlloc = ddl_plan.planGeneratedLogicalDdlAstAlloc;

test "bound ddl planner consumes binder-owned logical plan without parsed fallback" {
    const alloc = std.testing.allocator;
    var parsed = try tokenized.ParsedSql.initAlloc(alloc, "CREATE TABLE usage_records (id text PRIMARY KEY)");
    defer parsed.deinit(alloc);

    var bound = try binder.bindDdlStatementWithCatalogSessionAlloc(
        alloc,
        &parsed,
        table_catalog.unavailableCatalogSource(),
        catalog_resources.SqlCatalogSession.default(),
    );
    defer bound.deinit(alloc);

    var logical = try planLogicalDdlPlanBoundStatementWithFunctionBindingsAlloc(alloc, &bound, .{});
    defer logical.deinit(alloc);
    try std.testing.expectEqualStrings("table_ddl", logical.statementKindName());
    try std.testing.expectError(error.UnsupportedSqlShape, planLogicalDdlPlanBoundStatementWithFunctionBindingsAlloc(alloc, &bound, .{}));
}
