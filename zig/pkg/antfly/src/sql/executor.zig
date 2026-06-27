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

const catalog_resources = @import("../api/catalog_resources.zig");
const table_catalog = @import("../api/table_catalog.zig");
const binder = @import("binder.zig");
const sql_statement_kind = @import("statement_kind.zig");
const logical_ddl_plan = @import("logical_ddl_plan.zig");
const lower_expr = @import("lower_expr.zig");
const plan_mod = @import("plan.zig");
const tokenized = @import("tokenized.zig");

pub const SqlExecutionPlan = binder.LogicalSqlPlan;

pub const PlanParsedSqlOptions = struct {
    catalog: table_catalog.CatalogSource,
    session: catalog_resources.SqlCatalogSession = catalog_resources.SqlCatalogSession.default(),
    write_options: plan_mod.LowerWritePlanOptions = .{},
    function_bindings: lower_expr.SqlFunctionBindings = .{},
    authorization: binder.BoundSqlAuthorizationOptions = .{},
};

pub const PlannedSqlStatement = struct {
    logical_plan: binder.LogicalSqlPlan,
    authorization: binder.BoundSqlAuthorization = .{},

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        self.logical_plan.deinit(alloc);
        self.authorization.deinit(alloc);
        self.* = undefined;
    }
};

pub fn classifyParsedSql(parsed_sql: *const tokenized.ParsedSql) ?SqlExecutionPlan {
    return switch (parsed_sql.statement) {
        .write => |statement| .{ .write = statement.kind },
        .read => |statement| .{ .read = statement.kind },
        else => null,
    };
}

pub fn planParsedSqlWithSessionAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const tokenized.ParsedSql,
    options: PlanParsedSqlOptions,
) !SqlExecutionPlan {
    switch (parsed_sql.statement) {
        .write => {
            var bound = try binder.bindWritePlanCatalogStatementWithSessionAndAuthorizationAlloc(
                alloc,
                parsed_sql,
                options.write_options,
                options.catalog,
                options.session,
                options.authorization,
            );
            defer bound.deinit(alloc);
            try binder.enforceBoundSqlStatementAuthorization(&bound);
            return try binder.logicalWritePlanFromBoundStatement(&bound);
        },
        .read => {
            var bound = try binder.bindReadPlanCatalogStatementWithSessionAndAuthorizationAlloc(alloc, parsed_sql, options.catalog, options.session, options.authorization);
            defer bound.deinit(alloc);
            try binder.enforceBoundSqlStatementAuthorization(&bound);
            return try binder.logicalReadPlanFromBoundStatement(&bound);
        },
        .ddl, .explain, .transaction, .prepared, .session, .unsupported, .unknown => {
            var bound = try binder.bindDdlStatementWithCatalogSessionFunctionBindingsAndAuthorizationAlloc(
                alloc,
                parsed_sql,
                options.catalog,
                options.session,
                options.function_bindings,
                options.authorization,
            );
            defer bound.deinit(alloc);
            try binder.enforceBoundSqlStatementAuthorization(&bound);
            return try logical_ddl_plan.planLogicalDdlPlanBoundStatementWithFunctionBindingsAlloc(alloc, &bound, options.function_bindings);
        },
    }
}

pub fn planParsedDdlSqlWithSessionAuthorizationEvidenceAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const tokenized.ParsedSql,
    options: PlanParsedSqlOptions,
) !PlannedSqlStatement {
    var bound = try binder.bindDdlStatementWithCatalogSessionFunctionBindingsAndAuthorizationAlloc(
        alloc,
        parsed_sql,
        options.catalog,
        options.session,
        options.function_bindings,
        options.authorization,
    );
    defer bound.deinit(alloc);

    var logical_plan = try logical_ddl_plan.planLogicalDdlPlanBoundStatementWithFunctionBindingsAlloc(alloc, &bound, options.function_bindings);
    errdefer logical_plan.deinit(alloc);
    var authorization = try binder.takeBoundSqlStatementAuthorization(&bound);
    errdefer authorization.deinit(alloc);
    return .{
        .logical_plan = logical_plan,
        .authorization = authorization,
    };
}

test "sql executor classifies statement families and owns ddl plans" {
    const alloc = std.testing.allocator;

    var read_sql = try tokenized.ParsedSql.initAlloc(alloc, "SELECT id FROM usage_records");
    defer read_sql.deinit(alloc);
    const read_plan = classifyParsedSql(&read_sql) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(sql_statement_kind.SqlReadStatementKind.query, read_plan.read);

    var write_sql = try tokenized.ParsedSql.initAlloc(alloc, "INSERT INTO usage_records (id) VALUES ('evt-1')");
    defer write_sql.deinit(alloc);
    const write_plan = classifyParsedSql(&write_sql) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(sql_statement_kind.SqlWriteStatementKind.insert, write_plan.write);

    var ddl_sql = try tokenized.ParsedSql.initAlloc(alloc, "CREATE TABLE usage_records (id text PRIMARY KEY)");
    defer ddl_sql.deinit(alloc);
    var ddl_logical = try planParsedSqlWithSessionAlloc(alloc, &ddl_sql, .{
        .catalog = table_catalog.unavailableCatalogSource(),
        .function_bindings = .{},
    });
    defer ddl_logical.deinit(alloc);
    try std.testing.expectEqualStrings("table_ddl", ddl_logical.statementKindName());

    var bound_ddl = try binder.bindDdlStatementWithCatalogSessionAlloc(
        alloc,
        &ddl_sql,
        table_catalog.unavailableCatalogSource(),
        catalog_resources.SqlCatalogSession.default(),
    );
    defer bound_ddl.deinit(alloc);
    try std.testing.expectEqual(@as(std.meta.Tag(tokenized.ParsedStatement), .ddl), std.meta.activeTag(bound_ddl.statement));
    try std.testing.expect(bound_ddl.parsed_sql != null);
    var bound_ddl_logical = try logical_ddl_plan.planLogicalDdlPlanBoundStatementWithFunctionBindingsAlloc(alloc, &bound_ddl, .{});
    defer bound_ddl_logical.deinit(alloc);
    try std.testing.expectEqualStrings("table_ddl", bound_ddl_logical.statementKindName());
}
