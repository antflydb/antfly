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
const ddl_plan = @import("ddl.zig");
const executor = @import("executor.zig");
const logical_ddl_plan = @import("logical_ddl_plan.zig");
const lower_expr = @import("lower_expr.zig");
const tokenized = @import("tokenized.zig");

pub const DurableSqlPlan = union(enum) {
    table_ddl: binder.TableDdlLogicalPlan,
    catalog_ddl: binder.CatalogDdlLogicalPlan,
    extension: ddl_plan.ExtensionCatalogPlan,
    auth: binder.AuthorizationLogicalPlan,
    routine: binder.RoutineLogicalPlan,
    maintenance: ddl_plan.MaintenanceJobPlan,
    bulk_io: ddl_plan.BulkIoPlan,

    pub fn deinit(self: *@This(), alloc: @import("std").mem.Allocator) void {
        switch (self.*) {
            .table_ddl => |*plan| plan.deinit(alloc),
            .catalog_ddl => |*plan| plan.deinit(alloc),
            .extension => |*plan| plan.deinit(alloc),
            .auth => |*plan| plan.deinit(alloc),
            .routine => |*plan| plan.deinit(alloc),
            .maintenance => |*plan| plan.deinit(alloc),
            .bulk_io => |*plan| plan.deinit(alloc),
        }
        self.* = undefined;
    }

    fn fromLogical(logical: *binder.LogicalSqlPlan) !DurableSqlPlan {
        return switch (logical.*) {
            .table_ddl => |plan| moveLogical(logical, .{ .table_ddl = plan }),
            .catalog_ddl => |plan| moveLogical(logical, .{ .catalog_ddl = plan }),
            .extension => |plan| moveLogical(logical, .{ .extension = plan }),
            .auth => |plan| moveLogical(logical, .{ .auth = plan }),
            .routine => |plan| switch (plan) {
                .function_catalog, .trigger_catalog => moveLogical(logical, .{ .routine = plan }),
                .procedure_call => error.UnsupportedSqlShape,
            },
            .maintenance => |plan| moveLogical(logical, .{ .maintenance = plan }),
            .bulk_io => |plan| moveLogical(logical, .{ .bulk_io = plan }),
            else => error.UnsupportedSqlShape,
        };
    }

    fn moveLogical(logical: *binder.LogicalSqlPlan, durable: DurableSqlPlan) DurableSqlPlan {
        logical.* = .{ .other_ddl = .{ .moved = {} } };
        return durable;
    }
};

pub fn takeDurableSqlPlanFromLogical(logical: *binder.LogicalSqlPlan) !DurableSqlPlan {
    return try DurableSqlPlan.fromLogical(logical);
}

pub const DurableSqlPlanOrAdapterNoop = union(enum) {
    durable: DurableSqlPlan,
    adapter_noop: ddl_plan.AdapterNoopDdlPlan,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        switch (self.*) {
            .durable => |*plan| plan.deinit(alloc),
            .adapter_noop => {},
        }
        self.* = undefined;
    }

    fn fromLogical(logical: *binder.LogicalSqlPlan) !DurableSqlPlanOrAdapterNoop {
        return switch (logical.*) {
            .other_ddl => |plan| switch (plan) {
                .adapter_noop => |noop| blk: {
                    logical.* = .{ .other_ddl = .{ .moved = {} } };
                    break :blk .{ .adapter_noop = noop };
                },
                .moved => error.UnsupportedSqlShape,
            },
            else => .{ .durable = try DurableSqlPlan.fromLogical(logical) },
        };
    }
};

pub fn planDurableSqlPlanOrAdapterNoopParsedSqlWithFunctionBindingsAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const tokenized.ParsedSql,
    function_bindings: lower_expr.SqlFunctionBindings,
) !DurableSqlPlanOrAdapterNoop {
    var logical_plan = try executor.planParsedSqlWithSessionAlloc(alloc, parsed_sql, .{
        .catalog = table_catalog.unavailableCatalogSource(),
        .function_bindings = function_bindings,
    });
    errdefer logical_plan.deinit(alloc);
    return try DurableSqlPlanOrAdapterNoop.fromLogical(&logical_plan);
}

pub fn planDurableSqlPlanBoundStatementWithFunctionBindingsAlloc(
    alloc: std.mem.Allocator,
    bound: *binder.BoundSqlStatement,
    function_bindings: lower_expr.SqlFunctionBindings,
) !DurableSqlPlan {
    var logical_plan = try logical_ddl_plan.planLogicalDdlPlanBoundStatementWithFunctionBindingsAlloc(alloc, bound, function_bindings);
    errdefer logical_plan.deinit(alloc);
    return try DurableSqlPlan.fromLogical(&logical_plan);
}

pub const DurablePlannedSqlStatement = struct {
    durable_plan: DurableSqlPlan,
    authorization: binder.BoundSqlAuthorization = .{},

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        self.durable_plan.deinit(alloc);
        self.authorization.deinit(alloc);
        self.* = undefined;
    }
};

pub fn planDurableDdlSqlWithSessionAuthorizationEvidenceAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const tokenized.ParsedSql,
    options: executor.PlanParsedSqlOptions,
) !DurablePlannedSqlStatement {
    var bound = try binder.bindDdlStatementWithCatalogSessionFunctionBindingsAndAuthorizationAlloc(
        alloc,
        parsed_sql,
        options.catalog,
        options.session,
        options.function_bindings,
        options.authorization,
    );
    defer bound.deinit(alloc);

    var durable_plan = try planDurableSqlPlanBoundStatementWithFunctionBindingsAlloc(alloc, &bound, options.function_bindings);
    errdefer durable_plan.deinit(alloc);
    var authorization = try binder.takeBoundSqlStatementAuthorization(&bound);
    errdefer authorization.deinit(alloc);
    return .{
        .durable_plan = durable_plan,
        .authorization = authorization,
    };
}

pub fn planDurableSqlPlanParsedSqlWithFunctionBindingsAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const tokenized.ParsedSql,
    function_bindings: lower_expr.SqlFunctionBindings,
) !DurableSqlPlan {
    var logical_plan = try executor.planParsedSqlWithSessionAlloc(alloc, parsed_sql, .{
        .catalog = table_catalog.unavailableCatalogSource(),
        .function_bindings = function_bindings,
    });
    errdefer logical_plan.deinit(alloc);
    return try DurableSqlPlan.fromLogical(&logical_plan);
}

pub fn planDurableSqlPlanParsedSqlWithCatalogSessionFunctionBindingsAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const tokenized.ParsedSql,
    catalog: table_catalog.CatalogSource,
    session: catalog_resources.SqlCatalogSession,
    function_bindings: lower_expr.SqlFunctionBindings,
) !DurableSqlPlan {
    var logical_plan = try executor.planParsedSqlWithSessionAlloc(alloc, parsed_sql, .{
        .catalog = catalog,
        .session = session,
        .function_bindings = function_bindings,
    });
    errdefer logical_plan.deinit(alloc);
    return try DurableSqlPlan.fromLogical(&logical_plan);
}

pub fn durablePlanForDropUpdatePolicyTriggerAlloc(
    alloc: std.mem.Allocator,
    drop: ddl_plan.DropRoutineTriggerPlan,
) !DurableSqlPlan {
    const operations = try alloc.alloc(ddl_plan.AlterTableOperation, 1);
    var operations_transferred = false;
    errdefer if (!operations_transferred) alloc.free(operations);
    operations[0] = .{ .drop_update_policy = .{
        .trigger_name = try alloc.dupe(u8, drop.trigger_name),
        .if_exists = drop.if_exists,
    } };
    var operation_transferred = false;
    errdefer if (!operation_transferred) ddl_plan.freeAlterTableOperation(alloc, operations[0]);
    const table_name = try alloc.dupe(u8, drop.table_name);
    errdefer alloc.free(table_name);

    operations_transferred = true;
    operation_transferred = true;
    return .{ .table_ddl = .{ .alter_table = .{
        .table_name = table_name,
        .operations = operations,
    } } };
}

test "durable sql plan builds table ddl for dropped update-policy trigger" {
    const alloc = std.testing.allocator;
    const drop = ddl_plan.DropRoutineTriggerPlan{
        .trigger_name = "set_updated_at",
        .table_name = "usage_records",
        .if_exists = true,
    };
    var durable = try durablePlanForDropUpdatePolicyTriggerAlloc(alloc, drop);
    defer durable.deinit(alloc);
    switch (durable) {
        .table_ddl => |table_plan| switch (table_plan) {
            .alter_table => |alter| {
                try std.testing.expectEqualStrings("usage_records", alter.table_name);
                try std.testing.expectEqual(@as(usize, 1), alter.operations.len);
                switch (alter.operations[0]) {
                    .drop_update_policy => |operation| {
                        try std.testing.expectEqualStrings("set_updated_at", operation.trigger_name);
                        try std.testing.expect(operation.if_exists);
                    },
                    else => return error.TestUnexpectedResult,
                }
            },
            else => return error.TestUnexpectedResult,
        },
        else => return error.TestUnexpectedResult,
    }
}

test "durable sql planner returns typed table ddl plan from parsed sql" {
    const alloc = std.testing.allocator;
    var parsed = try tokenized.ParsedSql.initAlloc(alloc, "CREATE TABLE usage_records (id text PRIMARY KEY)");
    defer parsed.deinit(alloc);

    var durable = try planDurableSqlPlanParsedSqlWithFunctionBindingsAlloc(alloc, &parsed, .{});
    defer durable.deinit(alloc);

    switch (durable) {
        .table_ddl => |table_plan| switch (table_plan) {
            .create_table => |create| try std.testing.expectEqualStrings("usage_records", create.table_name),
            else => return error.TestUnexpectedResult,
        },
        else => return error.TestUnexpectedResult,
    }
}

test "durable sql planner returns typed auth plan from parsed sql" {
    const alloc = std.testing.allocator;
    var parsed = try tokenized.ParsedSql.initAlloc(alloc, "CREATE ROLE app_writer");
    defer parsed.deinit(alloc);

    var durable = try planDurableSqlPlanParsedSqlWithFunctionBindingsAlloc(alloc, &parsed, .{});
    defer durable.deinit(alloc);

    switch (durable) {
        .auth => |auth_plan| switch (auth_plan) {
            .authorization_catalog => |authorization| switch (authorization) {
                .create_role => |create| try std.testing.expectEqualStrings("app_writer", create.role_name),
                else => return error.TestUnexpectedResult,
            },
            else => return error.TestUnexpectedResult,
        },
        else => return error.TestUnexpectedResult,
    }
}

test "durable sql planner returns typed routine catalog plan from parsed sql" {
    const alloc = std.testing.allocator;
    var parsed = try tokenized.ParsedSql.initAlloc(alloc, "CREATE FUNCTION normalize_status(text) RETURNS text LANGUAGE sql AS 'SELECT lower($1)'");
    defer parsed.deinit(alloc);

    var durable = try planDurableSqlPlanParsedSqlWithFunctionBindingsAlloc(alloc, &parsed, .{});
    defer durable.deinit(alloc);

    switch (durable) {
        .routine => |routine_plan| switch (routine_plan) {
            .function_catalog => |function_catalog| switch (function_catalog) {
                .create => |create| try std.testing.expectEqualStrings("normalize_status", create.function_name),
                else => return error.TestUnexpectedResult,
            },
            else => return error.TestUnexpectedResult,
        },
        else => return error.TestUnexpectedResult,
    }
}

test "durable sql planner or adapter noop preserves extension compatibility no-op" {
    const alloc = std.testing.allocator;
    var parsed = try tokenized.ParsedSql.initAlloc(alloc, "CREATE EXTENSION IF NOT EXISTS pgcrypto");
    defer parsed.deinit(alloc);

    var planned = try planDurableSqlPlanOrAdapterNoopParsedSqlWithFunctionBindingsAlloc(alloc, &parsed, .{});
    defer planned.deinit(alloc);

    switch (planned) {
        .adapter_noop => |noop| try std.testing.expectEqual(ddl_plan.AdapterNoopDdlReason.extension, noop.reason),
        else => return error.TestUnexpectedResult,
    }
}

test "durable sql planner rejects procedure calls as non-durable" {
    const alloc = std.testing.allocator;
    var parsed = try tokenized.ParsedSql.initAlloc(alloc, "CALL refresh_rollups()");
    defer parsed.deinit(alloc);

    try std.testing.expectError(
        error.UnsupportedSqlShape,
        planDurableSqlPlanParsedSqlWithFunctionBindingsAlloc(alloc, &parsed, .{}),
    );
}

test "durable sql planner returns typed maintenance plan from parsed sql" {
    const alloc = std.testing.allocator;
    var parsed = try tokenized.ParsedSql.initAlloc(alloc, "VACUUM (VERBOSE) usage_records");
    defer parsed.deinit(alloc);

    var durable = try planDurableSqlPlanParsedSqlWithFunctionBindingsAlloc(alloc, &parsed, .{});
    defer durable.deinit(alloc);

    switch (durable) {
        .maintenance => |maintenance_plan| switch (maintenance_plan) {
            .vacuum => |vacuum| {
                try std.testing.expect(vacuum.verbose);
                try std.testing.expectEqualStrings("usage_records", vacuum.table_name.?);
            },
            else => return error.TestUnexpectedResult,
        },
        else => return error.TestUnexpectedResult,
    }
}

test "durable sql planner returns typed bulk io plan from parsed sql" {
    const alloc = std.testing.allocator;
    var parsed = try tokenized.ParsedSql.initAlloc(alloc, "COPY usage_records (id, status) FROM STDIN WITH (FORMAT csv)");
    defer parsed.deinit(alloc);

    var durable = try planDurableSqlPlanParsedSqlWithFunctionBindingsAlloc(alloc, &parsed, .{});
    defer durable.deinit(alloc);

    switch (durable) {
        .bulk_io => |bulk_io| {
            try std.testing.expectEqual(ddl_plan.BulkIoDirection.from, bulk_io.direction);
            try std.testing.expectEqual(ddl_plan.BulkIoEndpointKind.stream, bulk_io.endpoint_kind);
            try std.testing.expectEqualStrings("usage_records", bulk_io.table_name);
            try std.testing.expectEqual(@as(usize, 2), bulk_io.columns.len);
            try std.testing.expectEqualStrings("id", bulk_io.columns[0]);
            try std.testing.expectEqualStrings("status", bulk_io.columns[1]);
            try std.testing.expectEqualStrings("csv", bulk_io.format.?);
        },
        else => return error.TestUnexpectedResult,
    }
}
